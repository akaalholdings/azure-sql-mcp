"""Elastic pool pressure, read from inside a pooled database.

``sys.dm_elastic_pool_resource_stats`` returns the pool that holds the current
database: a row every few seconds, about 40 minutes retained, each percentage
measured against the pool limit. It needs VIEW SERVER STATE on every service
objective, so no database-level grant opens it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .azure_tier import dmv_permission_hint
from .azure_tier import read_dmv
from .azure_tier import read_tier_context
from .connection import AzureSqlExecutor
from .incident_log import note_exception
from .observability import sanitize_error_message
from .resource_governance import MEMORY_NOTE
from .resource_governance import summarize_resource_metric
from .result_status import ResultStatus
from .result_status import status_payload

POOL_DMV = "sys.dm_elastic_pool_resource_stats"
POOL_RETENTION_MINUTES = 40

POOL_WINDOW_SQL = """
SELECT
    end_time,
    avg_cpu_percent,
    avg_data_io_percent,
    avg_log_write_percent,
    max_worker_percent,
    max_session_percent,
    avg_edtu_percent,
    avg_instance_cpu_percent,
    avg_instance_memory_percent,
    instance_vcores,
    used_storage_mb,
    allocated_storage_mb,
    storage_limit_mb,
    used_storage_percent,
    allocated_storage_percent
FROM sys.dm_elastic_pool_resource_stats
WHERE end_time >= DATEADD(MINUTE, -?, GETUTCDATE())
ORDER BY end_time DESC
"""

# Dimension -> the column measured against the pool limit.
GOVERNED_METRICS = {
    "cpu": "avg_cpu_percent",
    "data_io": "avg_data_io_percent",
    "log_write": "avg_log_write_percent",
    "workers": "max_worker_percent",
    "sessions": "max_session_percent",
    "edtu": "avg_edtu_percent",
}
INSTANCE_METRICS = ("avg_instance_cpu_percent", "avg_instance_memory_percent")
STORAGE_FIELDS = (
    "used_storage_mb",
    "allocated_storage_mb",
    "storage_limit_mb",
    "used_storage_percent",
    "allocated_storage_percent",
)
# The same bars as the single-database resource check.
WARNING_P95_PERCENT = 80.0
CRITICAL_P95_PERCENT = 95.0
CRITICAL_MINUTES_ABOVE_95 = 5.0
STORAGE_WARNING_PERCENT = 80.0
STORAGE_CRITICAL_PERCENT = 90.0


def _timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _sample_seconds(rows: list[dict[str, Any]]) -> int | None:
    """The observed sample length; the page says only "every few seconds"."""

    newest, oldest = _timestamp(rows[0].get("end_time")), _timestamp(rows[-1].get("end_time"))
    if newest is None or oldest is None or len(rows) < 2:
        return None
    seconds = (newest - oldest).total_seconds() / (len(rows) - 1)
    return round(seconds) if seconds > 0 else None


def _iso(value: Any) -> str | None:
    stamp = _timestamp(value)
    return stamp.isoformat() if stamp else (str(value) if value is not None else None)


class ElasticPoolService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_pool_pressure(self, database_name: str, window_minutes: int = 30) -> dict[str, Any]:
        """CPU, IO, log, workers, sessions and storage of the pool against the pool limits."""

        window_minutes = max(1, min(int(window_minutes), POOL_RETENTION_MINUTES))
        payload: dict[str, Any] = {
            "database_name": database_name,
            "elastic_pool_name": None,
            "source_dmv": POOL_DMV,
            "permission_path": dmv_permission_hint(POOL_DMV),
            "collection_window": {"mode": "history", "window_minutes": window_minutes, "sample_count": 0},
            "pressure": [],
        }
        try:
            tier = (await read_tier_context(self.executor, database_name)).tier
        except Exception as exc:
            note_exception(exc, "elastic_pool.tier")
            payload.update(
                status_payload(
                    ResultStatus.UNAVAILABLE,
                    "The service objective could not be read, so pool membership is unknown: "
                    + sanitize_error_message(str(exc)),
                )
            )
            return payload
        payload["elastic_pool_name"] = tier.elastic_pool_name
        if not tier.pooled:
            payload.update(
                status_payload(
                    ResultStatus.NOT_SUPPORTED,
                    f"{database_name} is not in an elastic pool ({tier.service_objective}). Its own "
                    "limits are in get_resource_limits and get_resource_stats_history.",
                )
            )
            return payload

        rows, gap = await read_dmv(
            self.executor, database_name, POOL_DMV, POOL_WINDOW_SQL, [window_minutes], site="elastic_pool.usage"
        )
        if gap is not None:
            payload.update(gap)
            return payload
        if not rows:
            payload.update(
                status_payload(
                    ResultStatus.EMPTY,
                    f"{POOL_DMV} held no sample in the last {window_minutes} minutes.",
                )
            )
            return payload

        sample_seconds = _sample_seconds(rows)

        def summary(metric: str) -> dict[str, Any] | None:
            values = [float(row[metric]) for row in rows if row.get(metric) is not None]
            return summarize_resource_metric(values, sample_seconds=sample_seconds) if values else None

        usage = {metric: summary(metric) for metric in GOVERNED_METRICS.values()}
        latest = rows[0]
        payload["collection_window"] = {
            "mode": "history",
            "window_minutes": window_minutes,
            "sample_count": len(rows),
            "sample_seconds": sample_seconds,
            "start_utc": _iso(rows[-1].get("end_time")),
            "end_utc": _iso(latest.get("end_time")),
            "retention_minutes": POOL_RETENTION_MINUTES,
        }
        payload["limits"] = {
            "instance_vcores": latest.get("instance_vcores"),
            "storage_limit_mb": latest.get("storage_limit_mb"),
        }
        payload["usage_percent_of_pool_limit"] = {name: value for name, value in usage.items() if value}
        payload["instance"] = {
            **{metric: summary(metric) for metric in INSTANCE_METRICS},
            "memory_note": "Information only. " + MEMORY_NOTE,
        }
        payload["storage"] = {field: latest.get(field) for field in STORAGE_FIELDS}
        payload["pressure"] = _pressure(usage, payload["storage"])
        payload.update(
            status_payload(
                ResultStatus.OK,
                f"Read {len(rows)} pool sample(s); {len(payload['pressure'])} dimension(s) under pressure.",
            )
        )
        return payload


def _pressure(usage: dict[str, dict[str, Any] | None], storage: dict[str, Any]) -> list[dict[str, Any]]:
    pressure: list[dict[str, Any]] = []
    for dimension, metric in GOVERNED_METRICS.items():
        info = usage.get(metric)
        if not info:
            continue
        minutes_above_95 = info.get("minutes_above_95")
        if info["p95"] >= CRITICAL_P95_PERCENT or (minutes_above_95 or 0) >= CRITICAL_MINUTES_ABOVE_95:
            level = "critical"
        elif info["p95"] >= WARNING_P95_PERCENT:
            level = "warning"
        else:
            continue
        pressure.append(
            {
                "dimension": dimension,
                "metric": metric,
                "level": level,
                "p95_percent": info["p95"],
                "max_percent": info["max"],
                "minutes_above_95": minutes_above_95,
            }
        )
    used = storage.get("used_storage_percent")
    if used is not None and float(used) >= STORAGE_WARNING_PERCENT:
        pressure.append(
            {
                "dimension": "storage",
                "metric": "used_storage_percent",
                "level": "critical" if float(used) >= STORAGE_CRITICAL_PERCENT else "warning",
                "used_storage_percent": float(used),
                "storage_limit_mb": storage.get("storage_limit_mb"),
            }
        )
    return pressure
