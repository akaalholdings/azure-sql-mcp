"""Query Store over time: trends and regressions with explicit windows.

Query Store keeps runtime statistics per interval and survives failovers,
restarts, and plan-cache eviction, so it is the right source for "when did this
start" and "what changed". Both reads take a window length and an optional
``as_of_utc`` end.
"""

from __future__ import annotations

import math
from datetime import datetime
from datetime import timedelta
from typing import Any

from .connection import AzureSqlExecutor
from .incident_log import note_exception
from .observability import sanitize_error_message
from .result_status import ResultStatus
from .result_status import status_payload
from .time_windows import iso_z
from .time_windows import sql_utc
from .time_windows import window_bounds

MAX_TREND_BUCKETS = 500
MAX_WINDOW_MINUTES = 30 * 24 * 60
REGRESSION_METRICS = {
    "duration": ("total_duration_us", "avg_duration_ms", 1000.0),
    "cpu": ("total_cpu_us", "avg_cpu_ms", 1000.0),
    "logical_reads": ("total_logical_reads", "avg_logical_reads", 1.0),
}

QUERY_STORE_STATE_SQL = """
SELECT actual_state_desc, query_capture_mode_desc, readonly_reason
FROM sys.database_query_store_options
"""

_BUCKET = (
    "DATEADD(MINUTE, (DATEDIFF(MINUTE, '20000101', "
    "CAST(SWITCHOFFSET(rsi.start_time, '+00:00') AS datetime2)) / {bucket}) * {bucket}, "
    "CAST('20000101' AS datetime2))"
)

_TREND_SQL_TEMPLATE = """
SELECT
    {bucket_expr} AS bucket_start_utc,
    COUNT(DISTINCT rs.plan_id) AS plan_count,
    SUM(rs.count_executions) AS executions,
    SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS total_cpu_ms,
    SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS total_duration_ms,
    SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_reads,
    SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_reads
FROM sys.query_store_runtime_stats AS rs
INNER JOIN sys.query_store_runtime_stats_interval AS rsi
    ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
INNER JOIN sys.query_store_plan AS p
    ON p.plan_id = rs.plan_id
WHERE rsi.start_time < TODATETIMEOFFSET(CAST(? AS datetime2), 0)
  AND rsi.end_time > TODATETIMEOFFSET(CAST(? AS datetime2), 0)
  {query_filter}
GROUP BY {bucket_expr}
ORDER BY bucket_start_utc
"""

_PLAN_BREAKDOWN_SQL = """
SELECT
    rs.plan_id,
    MAX(CAST(p.is_forced_plan AS int)) AS is_forced_plan,
    SUM(rs.count_executions) AS executions,
    SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS total_cpu_ms,
    SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS total_duration_ms,
    SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_reads,
    CONVERT(varchar(33), MIN(SWITCHOFFSET(rsi.start_time, '+00:00')), 127) AS first_seen_utc,
    CONVERT(varchar(33), MAX(SWITCHOFFSET(rsi.end_time, '+00:00')), 127) AS last_seen_utc
FROM sys.query_store_runtime_stats AS rs
INNER JOIN sys.query_store_runtime_stats_interval AS rsi
    ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
INNER JOIN sys.query_store_plan AS p
    ON p.plan_id = rs.plan_id
WHERE rsi.start_time < TODATETIMEOFFSET(CAST(? AS datetime2), 0)
  AND rsi.end_time > TODATETIMEOFFSET(CAST(? AS datetime2), 0)
  AND p.query_id = ?
GROUP BY rs.plan_id
ORDER BY SUM(rs.avg_duration * rs.count_executions) DESC
"""

_REGRESSION_SQL = """
WITH runtime AS (
    SELECT
        p.query_id,
        rs.plan_id,
        CASE
            WHEN rsi.start_time >= TODATETIMEOFFSET(CAST(? AS datetime2), 0) THEN 'recent'
            ELSE 'baseline'
        END AS period,
        rs.count_executions,
        rs.avg_duration,
        rs.avg_cpu_time,
        rs.avg_logical_io_reads
    FROM sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi
        ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
    INNER JOIN sys.query_store_plan AS p
        ON p.plan_id = rs.plan_id
    WHERE rsi.start_time < TODATETIMEOFFSET(CAST(? AS datetime2), 0)
      AND rsi.end_time > TODATETIMEOFFSET(CAST(? AS datetime2), 0)
),
plan_period AS (
    SELECT
        query_id,
        period,
        plan_id,
        SUM(CAST(count_executions AS float)) AS executions,
        SUM(avg_duration * count_executions) AS total_duration_us,
        SUM(avg_cpu_time * count_executions) AS total_cpu_us,
        SUM(avg_logical_io_reads * count_executions) AS total_logical_reads
    FROM runtime
    GROUP BY query_id, period, plan_id
),
query_period AS (
    SELECT
        query_id,
        period,
        SUM(executions) AS executions,
        SUM(total_duration_us) AS total_duration_us,
        SUM(total_cpu_us) AS total_cpu_us,
        SUM(total_logical_reads) AS total_logical_reads,
        STRING_AGG(CAST(plan_id AS varchar(20)), ',') AS plan_ids
    FROM plan_period
    GROUP BY query_id, period
)
SELECT TOP (2000)
    r.query_id,
    r.executions AS recent_executions,
    r.total_duration_us AS recent_total_duration_us,
    r.total_cpu_us AS recent_total_cpu_us,
    r.total_logical_reads AS recent_total_logical_reads,
    r.plan_ids AS recent_plan_ids,
    b.executions AS baseline_executions,
    b.total_duration_us AS baseline_total_duration_us,
    b.total_cpu_us AS baseline_total_cpu_us,
    b.total_logical_reads AS baseline_total_logical_reads,
    b.plan_ids AS baseline_plan_ids,
    OBJECT_SCHEMA_NAME(q.object_id) AS object_schema,
    OBJECT_NAME(q.object_id) AS object_name,
    LEFT(qt.query_sql_text, 300) AS query_text_preview
FROM query_period AS r
INNER JOIN query_period AS b
    ON b.query_id = r.query_id
   AND b.period = 'baseline'
INNER JOIN sys.query_store_query AS q
    ON q.query_id = r.query_id
INNER JOIN sys.query_store_query_text AS qt
    ON qt.query_text_id = q.query_text_id
WHERE r.period = 'recent'
  AND r.executions >= ?
  AND b.executions >= ?
ORDER BY r.total_duration_us DESC
"""

_BASELINE_COVERAGE_SQL = """
SELECT
    CONVERT(varchar(33), MIN(SWITCHOFFSET(start_time, '+00:00')), 127) AS first_interval_utc,
    COUNT(*) AS intervals_in_baseline
FROM sys.query_store_runtime_stats_interval
WHERE start_time < TODATETIMEOFFSET(CAST(? AS datetime2), 0)
  AND end_time > TODATETIMEOFFSET(CAST(? AS datetime2), 0)
"""


class QueryStoreTrendService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def trend(
        self,
        database_name: str,
        *,
        query_id: int | None = None,
        window_minutes: int = 1440,
        bucket_minutes: int = 60,
        as_of_utc: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        window_minutes = max(5, min(int(window_minutes), MAX_WINDOW_MINUTES))
        bucket_minutes = max(5, min(int(bucket_minutes), 1440))
        adjusted = False
        if window_minutes / bucket_minutes > MAX_TREND_BUCKETS:
            bucket_minutes = int(math.ceil(window_minutes / MAX_TREND_BUCKETS / 5.0) * 5)
            adjusted = True
        start, end = window_bounds(window_minutes, as_of_utc, now=now)
        state = await self._query_store_state(database_name)
        if state is not None:
            return {
                "database_name": database_name,
                "query_id": query_id,
                "buckets": [],
                **state,
            }
        bucket_expr = _BUCKET.format(bucket=bucket_minutes)
        query_filter = "AND p.query_id = ?" if query_id is not None else ""
        params: list[Any] = [sql_utc(end), sql_utc(start)]
        if query_id is not None:
            params.append(int(query_id))
        rows = await self.executor.fetch_all(
            database_name,
            _TREND_SQL_TEMPLATE.format(bucket_expr=bucket_expr, query_filter=query_filter),
            params=params,
        )
        buckets = [_trend_bucket(row) for row in rows]
        payload: dict[str, Any] = {
            "database_name": database_name,
            "query_id": query_id,
            "window": {"start_utc": iso_z(start), "end_utc": iso_z(end), "minutes": window_minutes},
            "bucket_minutes": bucket_minutes,
            "bucket_size_adjusted": adjusted,
            "buckets": buckets,
            "totals": _totals(buckets),
        }
        if query_id is not None:
            plans = await self.executor.fetch_all(
                database_name,
                _PLAN_BREAKDOWN_SQL,
                params=[sql_utc(end), sql_utc(start), int(query_id)],
            )
            payload["plans"] = [_plan_row(row) for row in plans]
            payload["plan_changes_in_window"] = max(0, len(plans) - 1)
        return payload

    async def regressions(
        self,
        database_name: str,
        *,
        recent_minutes: int = 60,
        baseline_minutes: int = 7 * 24 * 60,
        as_of_utc: str | None = None,
        metric: str = "duration",
        min_executions: int = 10,
        min_regression_pct: float = 25.0,
        top: int = 20,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if metric not in REGRESSION_METRICS:
            raise ValueError(f"metric must be one of {sorted(REGRESSION_METRICS)}")
        recent_minutes = max(5, min(int(recent_minutes), MAX_WINDOW_MINUTES))
        baseline_minutes = max(15, min(int(baseline_minutes), MAX_WINDOW_MINUTES))
        recent_start, end = window_bounds(recent_minutes, as_of_utc, now=now)
        baseline_start = recent_start - timedelta(minutes=baseline_minutes)
        window = {
            "baseline_start_utc": iso_z(baseline_start),
            "recent_start_utc": iso_z(recent_start),
            "end_utc": iso_z(end),
            "recent_minutes": recent_minutes,
            "baseline_minutes": baseline_minutes,
        }
        state = await self._query_store_state(database_name)
        if state is not None:
            return {"database_name": database_name, "window": window, "regressions": [], **state}
        coverage = await self.executor.fetch_all(
            database_name,
            _BASELINE_COVERAGE_SQL,
            params=[sql_utc(recent_start), sql_utc(baseline_start)],
        )
        baseline_intervals = int((coverage[0].get("intervals_in_baseline") if coverage else 0) or 0)
        if baseline_intervals == 0:
            return {
                "database_name": database_name,
                "window": window,
                "regressions": [],
                **status_payload(
                    ResultStatus.UNAVAILABLE,
                    "Query Store holds no runtime intervals in the baseline window, so no "
                    "regression can be detected. This is not a clean bill of health; use a "
                    "shorter baseline or wait for history to accumulate.",
                ),
            }
        rows = await self.executor.fetch_all(
            database_name,
            _REGRESSION_SQL,
            params=[
                sql_utc(recent_start),
                sql_utc(end),
                sql_utc(baseline_start),
                max(1, int(min_executions)),
                max(1, int(min_executions)),
            ],
        )
        total_key, avg_name, divisor = REGRESSION_METRICS[metric]
        regressions = []
        for row in rows:
            item = _regression_row(row, total_key, avg_name, divisor)
            if item is None or item["regression_pct"] < min_regression_pct:
                continue
            regressions.append(item)
        regressions.sort(key=lambda item: -item["extra_cost"])
        first_interval = coverage[0].get("first_interval_utc") if coverage else None
        return {
            "database_name": database_name,
            "metric": metric,
            "window": window,
            "baseline_first_interval_utc": first_interval,
            "min_executions": int(min_executions),
            "min_regression_pct": float(min_regression_pct),
            "queries_compared": len(rows),
            "regressions": regressions[: max(1, int(top))],
        }

    async def _query_store_state(self, database_name: str) -> dict[str, Any] | None:
        try:
            rows = await self.executor.fetch_all(database_name, QUERY_STORE_STATE_SQL)
        except Exception as exc:
            note_exception(exc, "query_store_trends.query_store_state")
            return status_payload(
                ResultStatus.UNAVAILABLE,
                "Query Store options could not be read: " + sanitize_error_message(str(exc)),
            )
        state = str((rows[0].get("actual_state_desc") if rows else "") or "").upper()
        if state in {"READ_WRITE", "READ_ONLY"}:
            return None
        return status_payload(
            ResultStatus.PRECONDITION,
            f"Query Store is {state or 'not available'}; it holds the runtime history these reads need.",
            remediation="ALTER DATABASE CURRENT SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE);",
        )


def _trend_bucket(row: dict[str, Any]) -> dict[str, Any]:
    executions = _float(row.get("executions"))
    bucket = row.get("bucket_start_utc")
    return {
        "bucket_start_utc": bucket.isoformat() + "Z" if isinstance(bucket, datetime) else bucket,
        "executions": executions,
        "plan_count": int(_float(row.get("plan_count"))),
        "total_cpu_ms": round(_float(row.get("total_cpu_ms")), 3),
        "total_duration_ms": round(_float(row.get("total_duration_ms")), 3),
        "total_logical_reads": round(_float(row.get("total_logical_reads")), 1),
        "total_physical_reads": round(_float(row.get("total_physical_reads")), 1),
        "avg_cpu_ms": round(_float(row.get("total_cpu_ms")) / executions, 3) if executions else None,
        "avg_duration_ms": round(_float(row.get("total_duration_ms")) / executions, 3) if executions else None,
        "avg_logical_reads": round(_float(row.get("total_logical_reads")) / executions, 1) if executions else None,
    }


def _totals(buckets: list[dict[str, Any]]) -> dict[str, Any]:
    executions = sum(bucket["executions"] for bucket in buckets)
    duration = sum(bucket["total_duration_ms"] for bucket in buckets)
    cpu = sum(bucket["total_cpu_ms"] for bucket in buckets)
    peak = max(buckets, key=lambda bucket: bucket["total_duration_ms"], default=None)
    return {
        "executions": executions,
        "total_cpu_ms": round(cpu, 3),
        "total_duration_ms": round(duration, 3),
        "avg_duration_ms": round(duration / executions, 3) if executions else None,
        "peak_bucket_utc": peak["bucket_start_utc"] if peak else None,
    }


def _plan_row(row: dict[str, Any]) -> dict[str, Any]:
    executions = _float(row.get("executions"))
    return {
        "plan_id": int(_float(row.get("plan_id"))),
        "is_forced_plan": bool(row.get("is_forced_plan")),
        "executions": executions,
        "avg_cpu_ms": round(_float(row.get("total_cpu_ms")) / executions, 3) if executions else None,
        "avg_duration_ms": round(_float(row.get("total_duration_ms")) / executions, 3) if executions else None,
        "avg_logical_reads": round(_float(row.get("total_logical_reads")) / executions, 1) if executions else None,
        "first_seen_utc": row.get("first_seen_utc"),
        "last_seen_utc": row.get("last_seen_utc"),
    }


def _regression_row(
    row: dict[str, Any], total_key: str, avg_name: str, divisor: float
) -> dict[str, Any] | None:
    recent_executions = _float(row.get("recent_executions"))
    baseline_executions = _float(row.get("baseline_executions"))
    if recent_executions <= 0 or baseline_executions <= 0:
        return None
    recent_avg = _float(row.get(f"recent_{total_key}")) / recent_executions / divisor
    baseline_avg = _float(row.get(f"baseline_{total_key}")) / baseline_executions / divisor
    if baseline_avg <= 0:
        return None
    recent_plans = _plan_ids(row.get("recent_plan_ids"))
    baseline_plans = _plan_ids(row.get("baseline_plan_ids"))
    object_name = row.get("object_name")
    object_schema = row.get("object_schema")
    return {
        "query_id": int(_float(row.get("query_id"))),
        "object_name": f"{object_schema}.{object_name}" if object_name and object_schema else object_name,
        f"recent_{avg_name}": round(recent_avg, 3),
        f"baseline_{avg_name}": round(baseline_avg, 3),
        "regression_pct": round(100.0 * (recent_avg - baseline_avg) / baseline_avg, 1),
        "recent_executions": recent_executions,
        "baseline_executions": baseline_executions,
        "extra_cost": round((recent_avg - baseline_avg) * recent_executions, 3),
        "extra_cost_unit": avg_name.removeprefix("avg_") + " (recent executions x per-execution increase)",
        "recent_plan_ids": sorted(recent_plans),
        "baseline_plan_ids": sorted(baseline_plans),
        "new_plan_ids": sorted(recent_plans - baseline_plans),
        "plan_changed": bool(recent_plans - baseline_plans),
        "query_text_preview": row.get("query_text_preview"),
    }


def _plan_ids(value: Any) -> set[int]:
    if not value:
        return set()
    return {int(part) for part in str(value).split(",") if part.strip().isdigit()}


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
