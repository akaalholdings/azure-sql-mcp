"""Query Store over time: trends and regressions with explicit windows.

Query Store keeps runtime statistics per interval and survives failovers,
restarts, and plan-cache eviction, so it is the right source for "when did this
start" and "what changed". Both reads take a window length and an optional
``as_of_utc`` end.
"""

from __future__ import annotations

import math
import xml.etree.ElementTree as ET
from collections.abc import Mapping
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

from .connection import AzureSqlExecutor
from .incident_log import note_exception
from .observability import sanitize_error_message
from .query_store import QueryStoreReplicaScope
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
REGRESSION_CANDIDATE_CAP = 2000
# A candidate is not a regression when it returns this many times the rows per
# execution while the cost per row grew by less than the second factor.
ROW_GROWTH_FACTOR = 10.0
COST_PER_ROW_GROWTH_FACTOR = 2.0
MAX_COMPARED_PLAN_XML_CHARS = 2_000_000
# Aborted and Exception runs in the recent window against the baseline rate:
# flagged at 2x with at least 3 runs, high at 5x.
FAILURE_FLAG_RATIO = 2.0
FAILURE_HIGH_RATIO = 5.0
FAILURE_MIN_EXECUTIONS = 3
FAILURE_CANDIDATE_CAP = 500
_SHOWPLAN_NS = "{http://schemas.microsoft.com/sqlserver/2004/07/showplan}"

QUERY_STORE_STATE_SQL = """
SELECT actual_state_desc, query_capture_mode_desc, readonly_reason
FROM sys.database_query_store_options
"""

_BUCKET = (
    "DATEADD(MINUTE, (DATEDIFF(MINUTE, '20000101', "
    "CAST(SWITCHOFFSET(rsi.start_time, '+00:00') AS datetime2)) / {bucket}) * {bucket}, "
    "CAST('20000101' AS datetime2))"
)

# execution_type: 0 Regular, 3 Aborted (timeout or cancel), 4 Exception. The
# executions and avg_* fields count every run; these columns show the failed runs
# and give the Regular-only average that compare_query_plans reports.
_EXECUTION_OUTCOME_COLUMNS = """
    SUM(CASE WHEN rs.execution_type = 0 THEN rs.count_executions ELSE 0 END) AS regular_executions,
    SUM(CASE WHEN rs.execution_type = 0 THEN rs.avg_duration * rs.count_executions END) / 1000.0
        AS regular_total_duration_ms,
    SUM(CASE WHEN rs.execution_type = 3 THEN rs.count_executions ELSE 0 END) AS aborted_executions,
    SUM(CASE WHEN rs.execution_type = 4 THEN rs.count_executions ELSE 0 END) AS exception_executions,
"""

_TREND_SQL_TEMPLATE = """
SELECT
    {bucket_expr} AS bucket_start_utc,
    COUNT(DISTINCT rs.plan_id) AS plan_count,
    SUM(rs.count_executions) AS executions,
    SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS total_cpu_ms,
    SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS total_duration_ms,
    SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_reads,
    {outcome_columns}
    SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_reads
FROM sys.query_store_runtime_stats AS rs
INNER JOIN sys.query_store_runtime_stats_interval AS rsi
    ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
INNER JOIN sys.query_store_plan AS p
    ON p.plan_id = rs.plan_id
WHERE rsi.start_time < TODATETIMEOFFSET(CAST(? AS datetime2), 0)
  AND rsi.end_time > TODATETIMEOFFSET(CAST(? AS datetime2), 0)
  {replica_filter}
  {query_filter}
GROUP BY {bucket_expr}
ORDER BY bucket_start_utc
"""

_PLAN_BREAKDOWN_SQL_TEMPLATE = f"""
SELECT
    rs.plan_id,
    MAX(CAST(p.is_forced_plan AS int)) AS is_forced_plan,
    SUM(rs.count_executions) AS executions,
    {_EXECUTION_OUTCOME_COLUMNS}
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
  {{replica_filter}}
  AND p.query_id = ?
GROUP BY rs.plan_id
ORDER BY SUM(rs.avg_duration * rs.count_executions) DESC
"""

# Candidates are the queries whose per-execution {total} grew, ranked by the
# extra {total} the recent runs cost, so the cut follows the requested metric.
# plan_ids is 'plan_id:executions,...': the dominant plan of each period shows a
# flip between two known plans.
_REGRESSION_SQL_TEMPLATE = """
WITH runtime AS (
    SELECT
        p.query_id,
        rs.plan_id,
        CASE
            WHEN rsi.start_time >= TODATETIMEOFFSET(CAST(? AS datetime2), 0) THEN 'recent'
            ELSE 'baseline'
        END AS period,
        rs.execution_type,
        rs.count_executions,
        rs.avg_duration,
        rs.avg_cpu_time,
        rs.avg_logical_io_reads,
        rs.avg_rowcount
    FROM sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi
        ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
    INNER JOIN sys.query_store_plan AS p
        ON p.plan_id = rs.plan_id
    WHERE rsi.start_time < TODATETIMEOFFSET(CAST(? AS datetime2), 0)
      AND rsi.end_time > TODATETIMEOFFSET(CAST(? AS datetime2), 0)
      {replica_filter}
),
plan_period AS (
    SELECT
        query_id,
        period,
        plan_id,
        SUM(CAST(count_executions AS float)) AS executions,
        SUM(avg_duration * count_executions) AS total_duration_us,
        SUM(avg_cpu_time * count_executions) AS total_cpu_us,
        SUM(avg_logical_io_reads * count_executions) AS total_logical_reads,
        SUM(avg_rowcount * count_executions) AS total_rows,
        -- execution_type: 0 Regular, 3 Aborted (timeout or cancel), 4 Exception.
        SUM(CASE WHEN execution_type = 0 THEN CAST(count_executions AS float) ELSE 0 END)
            AS regular_executions,
        SUM(CASE WHEN execution_type = 0 THEN avg_duration * count_executions ELSE 0 END)
            AS regular_total_duration_us,
        SUM(CASE WHEN execution_type = 3 THEN CAST(count_executions AS float) ELSE 0 END)
            AS aborted_executions,
        SUM(CASE WHEN execution_type = 4 THEN CAST(count_executions AS float) ELSE 0 END)
            AS exception_executions
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
        SUM(total_rows) AS total_rows,
        SUM(regular_executions) AS regular_executions,
        SUM(regular_total_duration_us) AS regular_total_duration_us,
        SUM(aborted_executions) AS aborted_executions,
        SUM(exception_executions) AS exception_executions,
        STRING_AGG(
            CONCAT(CAST(plan_id AS varchar(max)), ':', CAST(CAST(executions AS bigint) AS varchar(20))),
            ','
        ) AS plan_ids
    FROM plan_period
    GROUP BY query_id, period
)
SELECT TOP ({cap})
    r.query_id,
    r.executions AS recent_executions,
    r.total_duration_us AS recent_total_duration_us,
    r.total_cpu_us AS recent_total_cpu_us,
    r.total_logical_reads AS recent_total_logical_reads,
    r.total_rows AS recent_total_rows,
    r.plan_ids AS recent_plan_ids,
    r.regular_executions AS recent_regular_executions,
    r.regular_total_duration_us AS recent_regular_total_duration_us,
    r.aborted_executions AS recent_aborted_executions,
    r.exception_executions AS recent_exception_executions,
    b.executions AS baseline_executions,
    b.total_duration_us AS baseline_total_duration_us,
    b.total_cpu_us AS baseline_total_cpu_us,
    b.total_logical_reads AS baseline_total_logical_reads,
    b.total_rows AS baseline_total_rows,
    b.plan_ids AS baseline_plan_ids,
    b.regular_executions AS baseline_regular_executions,
    b.regular_total_duration_us AS baseline_regular_total_duration_us,
    b.aborted_executions AS baseline_aborted_executions,
    b.exception_executions AS baseline_exception_executions,
    OBJECT_SCHEMA_NAME(q.object_id) AS object_schema,
    OBJECT_NAME(q.object_id) AS object_name,
    CASE
        WHEN qt.query_sql_text COLLATE Latin1_General_CI_AS LIKE N'%OPTION%(%RECOMPILE%'
        THEN 1 ELSE 0
    END AS has_option_recompile,
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
  AND r.{total} / r.executions > b.{total} / b.executions
ORDER BY (r.{total} / r.executions - b.{total} / b.executions) * r.executions DESC
"""

_PLAN_HASH_SQL = """
SELECT p.plan_id, CONVERT(varchar(18), p.query_plan_hash, 1) AS query_plan_hash
FROM sys.query_store_plan AS p
WHERE p.plan_id IN ({placeholders})
"""

_PLAN_XML_SQL = """
SELECT p.plan_id, CAST(p.query_plan AS nvarchar(max)) AS query_plan_xml
FROM sys.query_store_plan AS p
WHERE p.plan_id IN ({placeholders})
"""

# One row per query that failed in the recent window, with its baseline, and
# the database totals over every query (a window over the per-query rows).
_FAILED_EXECUTIONS_SQL_TEMPLATE = """
WITH runtime AS (
    SELECT
        p.query_id,
        CASE
            WHEN rsi.start_time >= TODATETIMEOFFSET(CAST(? AS datetime2), 0) THEN 1 ELSE 0
        END AS is_recent,
        rs.execution_type,
        CAST(rs.count_executions AS float) AS executions
    FROM sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi
        ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
    INNER JOIN sys.query_store_plan AS p
        ON p.plan_id = rs.plan_id
    WHERE rsi.start_time < TODATETIMEOFFSET(CAST(? AS datetime2), 0)
      AND rsi.end_time > TODATETIMEOFFSET(CAST(? AS datetime2), 0)
      {replica_filter}
),
per_query AS (
    -- execution_type: 3 Aborted (timeout or cancel), 4 Exception.
    SELECT
        query_id,
        SUM(CASE WHEN is_recent = 1 THEN executions ELSE 0 END) AS recent_executions,
        SUM(CASE WHEN is_recent = 1 AND execution_type = 3 THEN executions ELSE 0 END)
            AS recent_aborted_executions,
        SUM(CASE WHEN is_recent = 1 AND execution_type = 4 THEN executions ELSE 0 END)
            AS recent_exception_executions,
        SUM(CASE WHEN is_recent = 0 THEN executions ELSE 0 END) AS baseline_executions,
        SUM(CASE WHEN is_recent = 0 AND execution_type = 3 THEN executions ELSE 0 END)
            AS baseline_aborted_executions,
        SUM(CASE WHEN is_recent = 0 AND execution_type = 4 THEN executions ELSE 0 END)
            AS baseline_exception_executions
    FROM runtime
    GROUP BY query_id
),
with_totals AS (
    SELECT
        per_query.*,
        SUM(recent_executions) OVER () AS database_recent_executions,
        SUM(recent_aborted_executions) OVER () AS database_recent_aborted_executions,
        SUM(recent_exception_executions) OVER () AS database_recent_exception_executions,
        SUM(baseline_executions) OVER () AS database_baseline_executions,
        SUM(baseline_aborted_executions) OVER () AS database_baseline_aborted_executions,
        SUM(baseline_exception_executions) OVER () AS database_baseline_exception_executions
    FROM per_query
)
SELECT TOP ({cap})
    t.*,
    CONVERT(varchar(18), q.query_hash, 1) AS query_hash,
    OBJECT_SCHEMA_NAME(q.object_id) AS object_schema,
    OBJECT_NAME(q.object_id) AS object_name,
    LEFT(qt.query_sql_text, 300) AS query_text_preview
FROM with_totals AS t
INNER JOIN sys.query_store_query AS q
    ON q.query_id = t.query_id
INNER JOIN sys.query_store_query_text AS qt
    ON qt.query_text_id = q.query_text_id
WHERE t.recent_aborted_executions + t.recent_exception_executions > 0
ORDER BY t.recent_aborted_executions + t.recent_exception_executions DESC, t.query_id
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
        self.replicas = QueryStoreReplicaScope(executor)

    async def trend(
        self,
        database_name: str,
        *,
        query_id: int | None = None,
        window_minutes: int = 1440,
        bucket_minutes: int = 60,
        as_of_utc: str | None = None,
        replica_role: str | None = None,
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
        scope = await self.replicas.scope(database_name, replica_role)
        if scope.unsupported_reason is not None:
            return {
                "database_name": database_name,
                "query_id": query_id,
                "buckets": [],
                **_replica_unsupported(scope),
            }
        bucket_expr = _BUCKET.format(bucket=bucket_minutes)
        query_filter = "AND p.query_id = ?" if query_id is not None else ""
        params: list[Any] = [sql_utc(end), sql_utc(start), *scope.params]
        if query_id is not None:
            params.append(int(query_id))
        rows = await self.executor.fetch_all(
            database_name,
            _TREND_SQL_TEMPLATE.format(
                bucket_expr=bucket_expr,
                query_filter=query_filter,
                outcome_columns=_EXECUTION_OUTCOME_COLUMNS,
                replica_filter=scope.predicate,
            ),
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
            "totals": _totals(buckets, rows),
            **_replica_scope_field(scope),
        }
        if query_id is not None:
            plans = await self.executor.fetch_all(
                database_name,
                _PLAN_BREAKDOWN_SQL_TEMPLATE.format(replica_filter=scope.predicate),
                params=[sql_utc(end), sql_utc(start), *scope.params, int(query_id)],
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
        replica_role: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Queries whose per-execution cost grew, most extra cost first.

        Three kinds of candidate are not plan regressions. They go to
        ``filtered_candidates`` with the reason, never silently away:
        an OPTION (RECOMPILE) statement, a query that returns many more rows at
        a similar cost per row, and a plan change between plans that differ
        only in their compiled parameter values.
        """
        if metric not in REGRESSION_METRICS:
            raise ValueError(f"metric must be one of {sorted(REGRESSION_METRICS)}")
        recent_minutes = max(5, min(int(recent_minutes), MAX_WINDOW_MINUTES))
        baseline_minutes = max(15, min(int(baseline_minutes), MAX_WINDOW_MINUTES))
        top = max(1, int(top))
        recent_start, end = window_bounds(recent_minutes, as_of_utc, now=now)
        baseline_start = recent_start - timedelta(minutes=baseline_minutes)
        window = {
            "baseline_start_utc": iso_z(baseline_start),
            "recent_start_utc": iso_z(recent_start),
            "end_utc": iso_z(end),
            "recent_minutes": recent_minutes,
            "baseline_minutes": baseline_minutes,
        }
        empty = {"database_name": database_name, "window": window, "regressions": []}
        state = await self._query_store_state(database_name)
        if state is not None:
            return {**empty, **state}
        scope = await self.replicas.scope(database_name, replica_role)
        if scope.unsupported_reason is not None:
            return {**empty, **_replica_unsupported(scope)}
        coverage = await self._baseline_coverage(database_name, recent_start, baseline_start)
        if coverage["intervals"] == 0:
            return {
                **empty,
                **status_payload(
                    ResultStatus.UNAVAILABLE,
                    "Query Store holds no runtime intervals in the baseline window, so no "
                    "regression can be detected. This is not a clean bill of health; use a "
                    "shorter baseline or wait for history to accumulate.",
                ),
            }
        total_key, avg_name, divisor = REGRESSION_METRICS[metric]
        rows = await self.executor.fetch_all(
            database_name,
            _REGRESSION_SQL_TEMPLATE.format(
                replica_filter=scope.predicate,
                cap=int(REGRESSION_CANDIDATE_CAP),
                total=total_key,
            ),
            params=[
                sql_utc(recent_start),
                sql_utc(end),
                sql_utc(baseline_start),
                *scope.params,
                max(1, int(min_executions)),
                max(1, int(min_executions)),
            ],
        )
        candidates = []
        for row in rows:
            item = _regression_row(row, total_key, avg_name, divisor)
            if item is None or item["regression_pct"] < min_regression_pct:
                continue
            candidates.append(item)
        candidates.sort(key=lambda item: -item["extra_cost"])
        regressions: list[dict[str, Any]] = []
        filtered: list[dict[str, Any]] = []
        for item in candidates:
            (filtered if "filter_reason" in item else regressions).append(item)
        # The plan comparison reads plan XML, so it covers only the rows that
        # can still reach the answer.
        shortlist = regressions[: 2 * top]
        await self._mark_compiled_value_only_changes(database_name, shortlist)
        filtered.extend(item for item in shortlist if "filter_reason" in item)
        regressions = [item for item in regressions if "filter_reason" not in item]
        filtered.sort(key=lambda item: -item["extra_cost"])
        return {
            "database_name": database_name,
            "metric": metric,
            "window": window,
            "baseline_first_interval_utc": coverage["first_interval_utc"],
            "min_executions": int(min_executions),
            "min_regression_pct": float(min_regression_pct),
            "queries_compared": len(rows),
            "candidates_capped": len(rows) >= int(REGRESSION_CANDIDATE_CAP),
            "regressions": regressions[:top],
            "filtered_candidate_count": len(filtered),
            "filtered_candidates": filtered[:top],
            **_replica_scope_field(scope),
        }

    async def failed_executions(
        self,
        database_name: str,
        *,
        recent_minutes: int = 24 * 60,
        baseline_minutes: int = 14 * 24 * 60,
        as_of_utc: str | None = None,
        top: int = 20,
        replica_role: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Aborted and Exception runs in the recent window against the baseline rate.

        ``rising`` holds the queries whose failed runs are at least 2x the
        baseline rate with at least 3 runs (5x is high). ``top_failing_queries``
        ranks the queries that fail most, flagged or not. The baseline rate uses
        only the baseline time Query Store still holds, and the expected count
        is never below one run, so a first few failures are not an infinite rise.
        """
        recent_minutes = max(5, min(int(recent_minutes), MAX_WINDOW_MINUTES))
        baseline_minutes = max(15, min(int(baseline_minutes), MAX_WINDOW_MINUTES))
        top = max(1, int(top))
        recent_start, end = window_bounds(recent_minutes, as_of_utc, now=now)
        baseline_start = recent_start - timedelta(minutes=baseline_minutes)
        window = {
            "baseline_start_utc": iso_z(baseline_start),
            "recent_start_utc": iso_z(recent_start),
            "end_utc": iso_z(end),
            "recent_minutes": recent_minutes,
            "baseline_minutes": baseline_minutes,
        }
        empty = {
            "database_name": database_name,
            "window": window,
            "rising": [],
            "top_failing_queries": [],
        }
        state = await self._query_store_state(database_name)
        if state is not None:
            return {**empty, **state}
        scope = await self.replicas.scope(database_name, replica_role)
        if scope.unsupported_reason is not None:
            return {**empty, **_replica_unsupported(scope)}
        coverage = await self._baseline_coverage(database_name, recent_start, baseline_start)
        if coverage["intervals"] == 0:
            return {
                **empty,
                **status_payload(
                    ResultStatus.UNAVAILABLE,
                    "Query Store holds no runtime intervals in the baseline window, so a rise "
                    "in failed executions cannot be judged. This is not a clean bill of "
                    "health; use a shorter baseline or wait for history to accumulate.",
                ),
            }
        covered_start = max(baseline_start, coverage["first_interval"] or baseline_start)
        covered_minutes = max((recent_start - covered_start).total_seconds() / 60.0, 1.0)
        scale = recent_minutes / covered_minutes
        rows = await self.executor.fetch_all(
            database_name,
            _FAILED_EXECUTIONS_SQL_TEMPLATE.format(
                replica_filter=scope.predicate, cap=int(FAILURE_CANDIDATE_CAP)
            ),
            params=[sql_utc(recent_start), sql_utc(end), sql_utc(baseline_start), *scope.params],
        )
        queries = [_failure_row(row, "", scale) for row in rows]
        rising = [item for item in queries if item["severity"] is not None]
        rising.sort(key=lambda item: (item["severity"] != "high", -item["rise_ratio"]))
        summary = (
            _failure_row(rows[0], "database_", scale)
            if rows
            else _failure_row({}, "database_", scale)
        )
        return {
            "database_name": database_name,
            "window": window,
            "baseline_first_interval_utc": coverage["first_interval_utc"],
            "baseline_covered_minutes": round(covered_minutes, 1),
            "thresholds": {
                "flag_ratio": FAILURE_FLAG_RATIO,
                "high_ratio": FAILURE_HIGH_RATIO,
                "min_failed_executions": FAILURE_MIN_EXECUTIONS,
            },
            "summary": summary,
            "rising": rising[:top],
            "top_failing_queries": queries[:top],
            "candidates_capped": len(rows) >= int(FAILURE_CANDIDATE_CAP),
            **_replica_scope_field(scope),
        }

    async def _baseline_coverage(
        self, database_name: str, recent_start: datetime, baseline_start: datetime
    ) -> dict[str, Any]:
        rows = await self.executor.fetch_all(
            database_name,
            _BASELINE_COVERAGE_SQL,
            params=[sql_utc(recent_start), sql_utc(baseline_start)],
        )
        row = rows[0] if rows else {}
        first_interval_utc = row.get("first_interval_utc")
        return {
            "intervals": int(row.get("intervals_in_baseline") or 0),
            "first_interval_utc": first_interval_utc,
            "first_interval": _parse_utc(first_interval_utc),
        }

    async def _mark_compiled_value_only_changes(
        self, database_name: str, items: list[dict[str, Any]]
    ) -> None:
        """Mark a plan change between two plans that differ only in compiled values."""

        pairs = {
            item["query_id"]: (item["dominant_baseline_plan_id"], item["dominant_recent_plan_id"])
            for item in items
            if item["dominant_baseline_plan_id"] is not None
            and item["dominant_recent_plan_id"] is not None
            and item["dominant_baseline_plan_id"] != item["dominant_recent_plan_id"]
        }
        if not pairs:
            return
        hashes = await self._plan_column(
            database_name, _PLAN_HASH_SQL, {i for pair in pairs.values() for i in pair},
            "query_plan_hash",
        )
        same_shape = {
            query_id: pair
            for query_id, pair in pairs.items()
            if hashes.get(pair[0]) is not None and hashes.get(pair[0]) == hashes.get(pair[1])
        }
        if not same_shape:
            return
        plans = await self._plan_column(
            database_name, _PLAN_XML_SQL, {i for pair in same_shape.values() for i in pair},
            "query_plan_xml",
        )
        for item in items:
            pair = same_shape.get(item["query_id"])
            if pair is None:
                continue
            baseline = plan_compile_inputs(plans.get(pair[0]))
            recent = plan_compile_inputs(plans.get(pair[1]))
            if (
                baseline is None
                or recent is None
                or not baseline["statistics"]
                or baseline["statistics"] != recent["statistics"]
                or baseline["ce_version"] is None
                or baseline["ce_version"] != recent["ce_version"]
                or baseline["compiled_values"] == recent["compiled_values"]
            ):
                continue
            item["filter_reason"] = "plans_differ_only_in_compiled_values"
            item["filter_explanation"] = (
                f"Plans {pair[0]} and {pair[1]} have the same shape, the same statistics and "
                "the same cardinality estimator version; only the compiled parameter values "
                "differ. The cost follows the parameter values, so forcing the older plan "
                "changes nothing."
            )

    async def _plan_column(
        self, database_name: str, query: str, plan_ids: set[int], column: str
    ) -> dict[int, Any]:
        ordered = sorted(plan_ids)
        rows = await self.executor.fetch_all(
            database_name,
            query.format(placeholders=", ".join("?" for _ in ordered)),
            params=ordered,
        )
        return {int(_float(row.get("plan_id"))): row.get(column) for row in rows}

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
        **_execution_outcomes(row),
    }


def _execution_outcomes(row: dict[str, Any]) -> dict[str, Any]:
    regular = _float(row.get("regular_executions"))
    return {
        "aborted_executions": _float(row.get("aborted_executions")),
        "exception_executions": _float(row.get("exception_executions")),
        "regular_avg_duration_ms": (
            round(_float(row.get("regular_total_duration_ms")) / regular, 3) if regular else None
        ),
    }


def _totals(buckets: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, Any]:
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
        **_execution_outcomes(
            {
                key: sum(_float(row.get(key)) for row in rows)
                for key in (
                    "regular_executions",
                    "regular_total_duration_ms",
                    "aborted_executions",
                    "exception_executions",
                )
            }
        ),
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
        **_execution_outcomes(row),
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
    recent_plans = _plan_executions(row.get("recent_plan_ids"))
    baseline_plans = _plan_executions(row.get("baseline_plan_ids"))
    dominant_recent = _dominant_plan(recent_plans)
    dominant_baseline = _dominant_plan(baseline_plans)
    new_plans = sorted(set(recent_plans) - set(baseline_plans))
    object_name = row.get("object_name")
    object_schema = row.get("object_schema")
    item = {
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
        "new_plan_ids": new_plans,
        "dominant_recent_plan_id": dominant_recent,
        "dominant_baseline_plan_id": dominant_baseline,
        # A flip between two known plans is a plan change too.
        "plan_changed": bool(new_plans)
        or (
            dominant_recent is not None
            and dominant_baseline is not None
            and dominant_recent != dominant_baseline
        ),
        "query_text_preview": row.get("query_text_preview"),
        **_period_outcomes(row, "recent"),
        **_period_outcomes(row, "baseline"),
    }
    _mark_false_positive(item, row, recent_avg / baseline_avg)
    return item


def _mark_false_positive(item: dict[str, Any], row: Mapping[str, Any], cost_ratio: float) -> None:
    """Set filter_reason for a candidate that is not a plan regression."""

    if row.get("recent_total_rows") is not None and row.get("baseline_total_rows") is not None:
        recent_rows = _float(row.get("recent_total_rows")) / item["recent_executions"]
        baseline_rows = _float(row.get("baseline_total_rows")) / item["baseline_executions"]
        item["recent_rows_per_execution"] = round(recent_rows, 1)
        item["baseline_rows_per_execution"] = round(baseline_rows, 1)
        # Under one row per execution the per-row cost is noise.
        row_ratio = max(recent_rows, 1.0) / max(baseline_rows, 1.0)
    else:
        row_ratio = 1.0
    if row.get("has_option_recompile"):
        item["filter_reason"] = "statement_has_option_recompile"
        item["filter_explanation"] = (
            "The statement has OPTION (RECOMPILE): each run compiles for its own values, "
            "so the cost follows the parameter mix and no cached plan regressed."
        )
    elif (
        row_ratio >= ROW_GROWTH_FACTOR
        and cost_ratio / row_ratio < COST_PER_ROW_GROWTH_FACTOR
    ):
        item["filter_reason"] = "row_count_growth"
        item["rows_per_execution_ratio"] = round(row_ratio, 1)
        item["filter_explanation"] = (
            f"Each execution returns {row_ratio:.0f}x the rows and the cost per row grew "
            f"{cost_ratio / row_ratio:.1f}x: the query does more work, the plan did not get worse."
        )


def plan_compile_inputs(plan_xml: Any) -> dict[str, Any] | None:
    """Return what a plan was compiled from: estimator, statistics and values."""

    if not isinstance(plan_xml, str) or not plan_xml or len(plan_xml) > MAX_COMPARED_PLAN_XML_CHARS:
        return None
    try:
        root = ET.fromstring(plan_xml)
    except ET.ParseError:
        return None
    statement = next(root.iter(f"{_SHOWPLAN_NS}StmtSimple"), None)
    return {
        "ce_version": (
            statement.get("CardinalityEstimationModelVersion") if statement is not None else None
        ),
        # ModificationCount moves with every write; LastUpdate names the statistics version.
        "statistics": frozenset(
            (info.get("Schema"), info.get("Table"), info.get("Statistics"),
             info.get("LastUpdate"), info.get("SamplingPercent"))
            for info in root.iter(f"{_SHOWPLAN_NS}StatisticsInfo")
        ),
        "compiled_values": tuple(
            sorted(
                (str(column.get("Column")), str(column.get("ParameterCompiledValue")))
                for parameters in root.iter(f"{_SHOWPLAN_NS}ParameterList")
                for column in parameters.iter(f"{_SHOWPLAN_NS}ColumnReference")
            )
        ),
    }


def _failure_row(row: Mapping[str, Any], prefix: str, scale: float) -> dict[str, Any]:
    """Grade the recent failed runs against the baseline rate for one row."""

    recent_aborted = _float(row.get(f"{prefix}recent_aborted_executions"))
    recent_exception = _float(row.get(f"{prefix}recent_exception_executions"))
    recent_failed = recent_aborted + recent_exception
    recent_executions = _float(row.get(f"{prefix}recent_executions"))
    baseline_executions = _float(row.get(f"{prefix}baseline_executions"))
    baseline_failed = _float(row.get(f"{prefix}baseline_aborted_executions")) + _float(
        row.get(f"{prefix}baseline_exception_executions")
    )
    expected = baseline_failed * scale
    ratio = recent_failed / max(expected, 1.0)
    severity = None
    if recent_failed >= FAILURE_MIN_EXECUTIONS and ratio >= FAILURE_HIGH_RATIO:
        severity = "high"
    elif recent_failed >= FAILURE_MIN_EXECUTIONS and ratio >= FAILURE_FLAG_RATIO:
        severity = "medium"
    item: dict[str, Any] = {}
    if not prefix:
        object_name = row.get("object_name")
        object_schema = row.get("object_schema")
        item = {
            "query_id": int(_float(row.get("query_id"))),
            "query_hash": row.get("query_hash"),
            "object_name": (
                f"{object_schema}.{object_name}" if object_name and object_schema else object_name
            ),
            "query_text_preview": row.get("query_text_preview"),
        }
    return {
        **item,
        "severity": severity,
        "rise_ratio": round(ratio, 2),
        "recent_failed_executions": recent_failed,
        "recent_aborted_executions": recent_aborted,
        "recent_exception_executions": recent_exception,
        "recent_executions": recent_executions,
        "recent_failure_pct": (
            round(100.0 * recent_failed / recent_executions, 2) if recent_executions else None
        ),
        "baseline_failed_executions": baseline_failed,
        "baseline_executions": baseline_executions,
        "baseline_failure_pct": (
            round(100.0 * baseline_failed / baseline_executions, 2) if baseline_executions else None
        ),
        # The baseline failures scaled to the length of the recent window.
        "expected_failed_executions": round(expected, 2),
    }


def _replica_scope_field(scope: Any) -> dict[str, Any]:
    return {"replica_scope": scope.info} if scope.info is not None else {}


def _replica_unsupported(scope: Any) -> dict[str, Any]:
    return {
        **status_payload(ResultStatus.NOT_SUPPORTED, str(scope.unsupported_reason)),
        **_replica_scope_field(scope),
    }


def _parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _period_outcomes(row: dict[str, Any], period: str) -> dict[str, Any]:
    # The regression compares averages over every run; these fields show the
    # failed runs and the Regular-only duration so a timeout storm reads as one.
    regular = _float(row.get(f"{period}_regular_executions"))
    total = _float(row.get(f"{period}_regular_total_duration_us"))
    return {
        f"{period}_aborted_executions": _float(row.get(f"{period}_aborted_executions")),
        f"{period}_exception_executions": _float(row.get(f"{period}_exception_executions")),
        f"{period}_regular_avg_duration_ms": round(total / regular / 1000.0, 3) if regular else None,
    }


def _plan_executions(value: Any) -> dict[int, float]:
    """Parse 'plan_id:executions,...' (or bare plan ids) into executions per plan."""

    plans: dict[int, float] = {}
    for part in str(value or "").split(","):
        plan_id, _, executions = part.strip().partition(":")
        if plan_id.isdigit():
            plans[int(plan_id)] = _float(executions)
    return plans


def _dominant_plan(plans: Mapping[int, float]) -> int | None:
    """The plan with the most executions; None when no execution counts exist."""

    if not plans or not any(plans.values()):
        return None
    return max(sorted(plans), key=lambda plan_id: plans[plan_id])


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
