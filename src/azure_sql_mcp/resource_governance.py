from __future__ import annotations

import math
from typing import Any

from .azure_tier import TIER_SQL
from .azure_tier import classify_service_tier
from .azure_tier import dmv_permission_hint
from .connection import AzureSqlExecutor
from .incident_log import note_exception
from .observability import sanitize_error_message
from .result_status import ResultStatus
from .result_status import status_payload


MAX_RESOURCE_HISTORY_MINUTES = 14 * 24 * 60
RECENT_RETENTION_MINUTES = 60

RECENT_RESOURCE_SQL = """
SELECT
    end_time,
    CAST(avg_cpu_percent AS DECIMAL(5, 2)) AS avg_cpu_percent,
    CAST(avg_data_io_percent AS DECIMAL(5, 2)) AS avg_data_io_percent,
    CAST(avg_log_write_percent AS DECIMAL(5, 2)) AS avg_log_write_percent,
    CAST(avg_memory_usage_percent AS DECIMAL(5, 2)) AS avg_memory_usage_percent,
    CAST(avg_instance_memory_percent AS DECIMAL(5, 2)) AS avg_instance_memory_percent,
    CAST(max_worker_percent AS DECIMAL(5, 2)) AS max_worker_percent,
    CAST(max_session_percent AS DECIMAL(5, 2)) AS max_session_percent,
    CAST(xtp_storage_percent AS DECIMAL(5, 2)) AS xtp_storage_percent,
    CAST(avg_instance_cpu_percent AS DECIMAL(5, 2)) AS avg_instance_cpu_percent,
    dtu_limit,
    cpu_limit
FROM sys.dm_db_resource_stats
WHERE end_time >= DATEADD(MINUTE, -{window_minutes}, GETUTCDATE())
ORDER BY end_time DESC
"""

LONG_TERM_RESOURCE_SQL = """
SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, end_time), 0) AS hour_start_utc,
    COUNT(*) AS samples,
    CAST(AVG(avg_cpu_percent) AS DECIMAL(5, 2)) AS avg_cpu_percent,
    CAST(MAX(avg_cpu_percent) AS DECIMAL(5, 2)) AS max_cpu_percent,
    CAST(AVG(avg_data_io_percent) AS DECIMAL(5, 2)) AS avg_data_io_percent,
    CAST(MAX(avg_data_io_percent) AS DECIMAL(5, 2)) AS max_data_io_percent,
    CAST(AVG(avg_log_write_percent) AS DECIMAL(5, 2)) AS avg_log_write_percent,
    CAST(MAX(avg_log_write_percent) AS DECIMAL(5, 2)) AS max_log_write_percent,
    CAST(MAX(max_worker_percent) AS DECIMAL(5, 2)) AS max_worker_percent,
    CAST(MAX(max_session_percent) AS DECIMAL(5, 2)) AS max_session_percent,
    MAX(storage_in_megabytes) AS storage_in_megabytes,
    MAX(sku) AS sku
FROM sys.resource_stats
WHERE database_name = ?
  AND end_time >= DATEADD(MINUTE, -?, GETUTCDATE())
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, end_time), 0)
ORDER BY hour_start_utc DESC
"""

GOVERNANCE_DMV = "sys.dm_user_db_resource_governance"
GOVERNANCE_SQL = """
SELECT TOP (1) *
FROM sys.dm_user_db_resource_governance
WHERE database_id = DB_ID()
"""

RECENT_SAMPLE_SECONDS = 15
SUMMARY_METRICS = (
    "avg_cpu_percent",
    "avg_data_io_percent",
    "avg_log_write_percent",
    "avg_memory_usage_percent",
    "avg_instance_memory_percent",
    "max_worker_percent",
    "max_session_percent",
    "xtp_storage_percent",
)
MEMORY_METRICS = ("avg_memory_usage_percent", "avg_instance_memory_percent")
MEMORY_NOTE = (
    "Memory near 100% after ramp-up is expected on Azure SQL Database: the engine caches data "
    "by design, and Microsoft documents that reaching the memory limit does not slow queries or "
    "cause errors. Memory pressure shows as RESOURCE_SEMAPHORE waits, pending memory grants or "
    "out-of-memory events."
)
_BYTES_PER_MB = 1024 * 1024


def log_rate_caps(governance: dict[str, Any]) -> dict[str, float | None]:
    """Log-rate caps in MB/s from a sys.dm_user_db_resource_governance row (bytes per second).

    ``effective_log_rate_cap_mb_per_sec`` is the smallest known cap: the database
    (workload group), the elastic pool or user resource pool, and the instance.
    """

    caps: dict[str, float | None] = {}
    for column, key in (
        ("primary_max_log_rate", "max_log_rate_mb_per_sec"),
        ("pool_max_log_rate", "pool_max_log_rate_mb_per_sec"),
        ("instance_max_log_rate", "instance_max_log_rate_mb_per_sec"),
    ):
        value = governance.get(column)
        caps[key] = round(float(value) / _BYTES_PER_MB, 2) if value else None
    known = [value for value in caps.values() if value]
    caps["effective_log_rate_cap_mb_per_sec"] = min(known) if known else None
    return caps


def summarize_resource_metric(values: list[float], *, sample_seconds: int | None) -> dict[str, Any]:
    """Average, extremes, nearest-rank p95 and time above 80% and 95% of the limit.

    ``sample_seconds`` is the sample length (15 for sys.dm_db_resource_stats); the
    minutes are None for aggregated buckets, where samples are not time slices.
    """

    ordered = sorted(values)
    above_80 = sum(1 for value in values if value > 80)
    above_95 = sum(1 for value in values if value > 95)

    def minutes(count: int) -> float | None:
        return round(count * sample_seconds / 60, 2) if sample_seconds else None

    return {
        "avg": round(sum(values) / len(values), 2),
        "max": round(ordered[-1], 2),
        "min": round(ordered[0], 2),
        "p95": round(ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)], 2),
        "samples_above_80_pct": above_80,
        "total_samples": len(values),
        "minutes_above_80": minutes(above_80),
        "minutes_above_95": minutes(above_95),
    }


class ResourceGovernanceService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_io_stats(
        self,
        database_name: str,
    ) -> dict[str, Any]:
        """Per-file I/O stats: latency, throughput, pending I/O."""
        # Azure SQL DB user databases cannot read sys.master_files (server-scoped
        # catalog only visible from master). Use sys.database_files which is
        # available from each user database and joins to the per-file IO DMV.
        query = """
        SELECT
            df.name AS file_name,
            df.type_desc AS file_type,
            df.physical_name,
            CAST(df.size * 8.0 / 1024 AS DECIMAL(18, 2)) AS file_size_mb,
            fs.num_of_reads,
            fs.num_of_writes,
            fs.num_of_bytes_read / (1024.0 * 1024.0) AS read_mb,
            fs.num_of_bytes_written / (1024.0 * 1024.0) AS write_mb,
            CASE WHEN fs.num_of_reads > 0
                 THEN CAST(fs.io_stall_read_ms * 1.0 / fs.num_of_reads AS DECIMAL(18, 2))
                 ELSE 0
            END AS avg_read_latency_ms,
            CASE WHEN fs.num_of_writes > 0
                 THEN CAST(fs.io_stall_write_ms * 1.0 / fs.num_of_writes AS DECIMAL(18, 2))
                 ELSE 0
            END AS avg_write_latency_ms,
            fs.io_stall_read_ms,
            fs.io_stall_write_ms,
            fs.io_stall AS total_io_stall_ms
        FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) AS fs
        INNER JOIN sys.database_files AS df
            ON fs.file_id = df.file_id
        ORDER BY fs.io_stall DESC
        """
        rows = await self.executor.fetch_all(database_name, query)

        warnings: list[dict[str, Any]] = []
        for row in rows:
            avg_read = row.get("avg_read_latency_ms", 0) or 0
            avg_write = row.get("avg_write_latency_ms", 0) or 0
            if avg_read > 20:
                warnings.append(
                    {
                        "type": "high_read_latency",
                        "file": row.get("file_name"),
                        "latency_ms": avg_read,
                        "message": f"Average read latency {avg_read}ms exceeds 20ms threshold",
                    }
                )
            if avg_write > 20:
                warnings.append(
                    {
                        "type": "high_write_latency",
                        "file": row.get("file_name"),
                        "latency_ms": avg_write,
                        "message": f"Average write latency {avg_write}ms exceeds 20ms threshold",
                    }
                )

        return {
            "database_name": database_name,
            "files": rows,
            "warnings": warnings,
        }

    async def get_resource_limits(
        self,
        database_name: str,
    ) -> dict[str, Any]:
        """Azure resource governance limits and current service objective."""
        # Column availability in sys.dm_user_db_resource_governance varies
        # significantly by service tier (GeneralPurpose vs BusinessCritical
        # vs Hyperscale) and across Azure SQL versions.  SELECT * returns
        # whatever the current tier exposes, and we expose it as-is.
        # In an elastic pool the view returns a row per pool database, so the
        # current database must be selected explicitly.
        warnings: list[dict[str, Any]] = []
        governance_error: str | None = None
        try:
            governance_rows = await self.executor.fetch_all(database_name, GOVERNANCE_SQL)
        except Exception as exc:
            note_exception(exc, "resource_governance.limits")
            governance_rows = []
            governance_error = sanitize_error_message(str(exc))
        try:
            slo_rows = await self.executor.fetch_all(database_name, TIER_SQL)
        except Exception as exc:
            slo_rows = []
            warnings.append(
                {
                    "type": "service_objective_unavailable",
                    "message": "The edition and service objective could not be read: "
                    + sanitize_error_message(str(exc)),
                }
            )
        if governance_error is None and not governance_rows:
            warnings.append(
                {
                    "type": "governance_row_missing",
                    "message": "sys.dm_user_db_resource_governance returned no row for this database.",
                }
            )

        governance = dict(governance_rows[0]) if governance_rows else {}
        slo = slo_rows[0] if slo_rows else {}
        if governance:
            governance.update(log_rate_caps(governance))

        payload: dict[str, Any] = {
            "database_name": database_name,
            "service_objective": slo,
            "governance_limits": governance,
            "warnings": warnings,
        }
        if governance_error is not None:
            tier = (
                classify_service_tier(slo.get("edition"), slo.get("service_objective"), slo.get("elastic_pool_name"))
                if slo
                else None
            )
            # available=False is what evidence collectors read as a gap; without it a
            # case that lost its limits could still be recorded as healthy.
            payload["available"] = False
            payload.update(
                status_payload(
                    ResultStatus.UNAVAILABLE,
                    f"{GOVERNANCE_DMV} could not be read: {governance_error}. "
                    + dmv_permission_hint(GOVERNANCE_DMV, tier),
                )
            )
        return payload

    async def get_resource_stats_history(
        self,
        database_name: str,
        window_minutes: int = 60,
        *,
        source: str = "auto",
        master_available: bool = False,
    ) -> dict[str, Any]:
        """Resource utilization history against the database's governance limits.

        ``recent`` reads sys.dm_db_resource_stats (15-second samples, about one
        hour retained). ``long_term`` reads sys.resource_stats in master (5-minute
        samples, about 14 days retained) and returns hourly average and maximum
        buckets so long windows stay compact. ``auto`` picks recent for windows of
        60 minutes or less and long_term beyond that.
        """
        window_minutes = max(1, min(int(window_minutes), MAX_RESOURCE_HISTORY_MINUTES))
        if source not in {"auto", "recent", "long_term"}:
            raise ValueError("source must be auto, recent, or long_term")
        resolved = source
        if source == "auto":
            resolved = "recent" if window_minutes <= RECENT_RETENTION_MINUTES else "long_term"
        status: dict[str, Any] = {}
        if resolved == "long_term" and not master_available:
            status = status_payload(
                ResultStatus.PRECONDITION,
                f"A {window_minutes}-minute window needs sys.resource_stats, which is read "
                "from master. Returned the most recent hour from sys.dm_db_resource_stats "
                "instead.",
                remediation=(
                    "Add master to AZURE_SQL_ALLOWED_DATABASES; the login needs access to master."
                ),
            )
            resolved = "recent"
        if resolved == "long_term":
            rows = await self.executor.fetch_all(
                "master", LONG_TERM_RESOURCE_SQL, params=[database_name, window_minutes]
            )
            granularity = "hourly_buckets_from_5_minute_samples"
        else:
            query = RECENT_RESOURCE_SQL.format(window_minutes=window_minutes)
            rows = await self.executor.fetch_all(database_name, query)
            granularity = "15_second_samples"

        # Compute summary stats
        sample_seconds = RECENT_SAMPLE_SECONDS if resolved == "recent" else None
        summary: dict[str, Any] = {}
        if rows:
            for metric in SUMMARY_METRICS:
                if not any(metric in r for r in rows):
                    continue
                values = [float(r.get(metric, 0) or 0) for r in rows]
                summary[metric] = summarize_resource_metric(values, sample_seconds=sample_seconds)

        # Generate warnings for sustained pressure; memory is information only.
        warnings: list[dict[str, Any]] = []
        informational: list[dict[str, Any]] = []
        for metric_name, info in summary.items():
            if info.get("samples_above_80_pct", 0) > len(rows) * 0.3:
                friendly = metric_name.replace("avg_", "").replace("_", " ").title()
                message = (
                    f"{friendly} was above 80% for "
                    f"{info['samples_above_80_pct']} of {info['total_samples']} samples "
                    f"(>{30}% of window)"
                )
                if metric_name in MEMORY_METRICS:
                    informational.append(
                        {
                            "type": f"memory_{metric_name}",
                            "message": f"{message}. {MEMORY_NOTE}",
                            "max": info["max"],
                            "avg": info["avg"],
                        }
                    )
                    continue
                warnings.append(
                    {
                        "type": f"sustained_{metric_name}",
                        "message": message,
                        "max": info["max"],
                        "avg": info["avg"],
                    }
                )

        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "source": resolved,
            "granularity": granularity,
            "sample_count": len(rows),
            "summary": summary,
            "warnings": warnings,
            "informational": informational,
            "history": rows,
            **status,
        }
