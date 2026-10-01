"""Where a plan comes from on Azure SQL Database, and what each source can show.

- Query Store keeps the compiled (estimated) plan of every captured statement,
  plus measured runtime and waits per plan. The plan alone cannot say what was
  slow; the runtime rows can say how slow, at statement level.
- ``sys.dm_exec_query_plan_stats`` returns the last actual plan of a cached plan
  without re-running the query, once ``LAST_QUERY_PLAN_STATS`` is ON.
- ``sys.dm_exec_query_statistics_xml`` returns the in-flight plan of a running
  request; lightweight profiling is on by default in Azure SQL Database.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any

from .connection import AzureSqlExecutor
from .observability import sanitize_error_message
from .plan_tree import PlanParseError
from .plan_tree import parse_showplan
from .plan_tree import statements
from .result_status import ResultStatus
from .result_status import status_payload

PLAN_BY_ID_SQL = """
SELECT
    p.plan_id,
    p.query_id,
    p.is_forced_plan,
    CAST(p.query_plan AS nvarchar(max)) AS query_plan
FROM sys.query_store_plan AS p
WHERE p.plan_id = ?
"""

DOMINANT_PLAN_FOR_QUERY_SQL = """
WITH ranked AS (
    SELECT
        p.plan_id,
        SUM(rs.avg_duration * rs.count_executions) AS total_duration_us,
        MAX(p.last_execution_time) AS last_execution_time
    FROM sys.query_store_plan AS p
    LEFT JOIN sys.query_store_runtime_stats AS rs
        ON rs.plan_id = p.plan_id
    WHERE p.query_id = ?
    GROUP BY p.plan_id
)
SELECT TOP (1)
    p.plan_id,
    p.query_id,
    p.is_forced_plan,
    CAST(p.query_plan AS nvarchar(max)) AS query_plan
FROM ranked AS r
INNER JOIN sys.query_store_plan AS p
    ON p.plan_id = r.plan_id
ORDER BY r.total_duration_us DESC, r.last_execution_time DESC
"""

LAST_QUERY_PLAN_STATS_SQL = """
SELECT CAST(value AS nvarchar(20)) AS value
FROM sys.database_scoped_configurations
WHERE name = N'LAST_QUERY_PLAN_STATS'
"""

_LAST_ACTUAL_PLAN_SQL = """
SELECT TOP (1)
    p.plan_id,
    p.query_id,
    CONVERT(varchar(18), p.query_plan_hash, 1) AS query_plan_hash,
    qs.last_execution_time,
    CAST(ps.query_plan AS nvarchar(max)) AS query_plan
FROM sys.query_store_plan AS p
INNER JOIN sys.dm_exec_query_stats AS qs
    ON qs.query_plan_hash = p.query_plan_hash
CROSS APPLY sys.dm_exec_query_plan_stats(qs.plan_handle) AS ps
WHERE {predicate}
  AND ps.query_plan IS NOT NULL
ORDER BY qs.last_execution_time DESC
"""
LAST_ACTUAL_BY_PLAN_SQL = _LAST_ACTUAL_PLAN_SQL.format(predicate="p.plan_id = ?")
LAST_ACTUAL_BY_QUERY_SQL = _LAST_ACTUAL_PLAN_SQL.format(predicate="p.query_id = ?")

LIVE_PLAN_SQL = """
SELECT
    r.session_id,
    r.status,
    r.command,
    r.start_time,
    r.total_elapsed_time,
    CAST(qp.query_plan AS nvarchar(max)) AS query_plan
FROM sys.dm_exec_requests AS r
CROSS APPLY sys.dm_exec_query_statistics_xml(r.session_id) AS qp
WHERE r.session_id = ?
"""

PLAN_RUNTIME_SQL = """
SELECT
    SUM(rs.count_executions) AS executions,
    SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) / 1000.0 AS avg_duration_ms,
    SUM(rs.avg_cpu_time * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) / 1000.0 AS avg_cpu_ms,
    SUM(rs.avg_logical_io_reads * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) AS avg_logical_reads,
    SUM(rs.avg_rowcount * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) AS avg_rowcount,
    SUM(rs.avg_query_max_used_memory * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) * 8.0
        AS avg_max_used_memory_kb,
    MAX(rs.max_dop) AS max_dop,
    MIN(rs.first_execution_time) AS first_execution_time,
    MAX(rs.last_execution_time) AS last_execution_time
FROM sys.query_store_runtime_stats AS rs
WHERE rs.plan_id = ?
"""

PLAN_WAITS_SQL = """
SELECT TOP (5)
    ws.wait_category_desc,
    SUM(ws.total_query_wait_time_ms) AS wait_ms
FROM sys.query_store_wait_stats AS ws
WHERE ws.plan_id = ?
GROUP BY ws.wait_category_desc
ORDER BY wait_ms DESC
"""

ENABLE_LAST_ACTUAL_PLANS = "ALTER DATABASE SCOPED CONFIGURATION SET LAST_QUERY_PLAN_STATS = ON;"


@dataclass
class PlanSource:
    """A plan to analyse, or the status that explains why there is none."""

    xml: str | None
    source: dict[str, Any] = field(default_factory=dict)
    status: dict[str, Any] = field(default_factory=dict)


class PlanSourceService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def query_store(self, database_name: str, *, plan_id: int | None, query_id: int | None) -> PlanSource:
        query = PLAN_BY_ID_SQL if plan_id is not None else DOMINANT_PLAN_FOR_QUERY_SQL
        rows = await self.executor.fetch_all(database_name, query, params=[int(plan_id or query_id or 0)])
        if not rows:
            return PlanSource(
                None,
                {"plan_id": plan_id, "query_id": query_id},
                status_payload(
                    ResultStatus.EMPTY,
                    "No Query Store plan matched; check the id and the Query Store retention window.",
                ),
            )
        row = rows[0]
        return PlanSource(
            str(row.get("query_plan") or ""),
            {
                "source": "query_store",
                "plan_id": row.get("plan_id"),
                "query_id": row.get("query_id"),
                "is_forced_plan": bool(row.get("is_forced_plan")),
            },
        )

    async def last_actual(self, database_name: str, *, plan_id: int | None, query_id: int | None) -> PlanSource:
        source: dict[str, Any] = {"source": "last_actual_plan", "plan_id": plan_id, "query_id": query_id}
        try:
            config = await self.executor.fetch_all(database_name, LAST_QUERY_PLAN_STATS_SQL)
        except Exception as exc:
            return PlanSource(None, source, _unreadable("sys.database_scoped_configurations", exc))
        value = str((config[0].get("value") if config else "") or "").strip().upper()
        if value not in {"1", "ON", "TRUE"}:
            return PlanSource(
                None,
                source,
                status_payload(
                    ResultStatus.PRECONDITION,
                    "Last actual plans need LAST_QUERY_PLAN_STATS = ON. It enables lightweight profiling "
                    "for every query in the database, so turn it on deliberately.",
                    remediation=ENABLE_LAST_ACTUAL_PLANS,
                ),
            )
        query = LAST_ACTUAL_BY_PLAN_SQL if plan_id is not None else LAST_ACTUAL_BY_QUERY_SQL
        try:
            rows = await self.executor.fetch_all(database_name, query, params=[int(plan_id or query_id or 0)])
        except Exception as exc:
            return PlanSource(None, source, _unreadable("sys.dm_exec_query_plan_stats", exc))
        if not rows or not rows[0].get("query_plan"):
            return PlanSource(
                None,
                source,
                status_payload(
                    ResultStatus.EMPTY,
                    "No cached last actual plan: the plan may have left the cache, or the query has not "
                    "run since LAST_QUERY_PLAN_STATS was turned on.",
                ),
            )
        row = rows[0]
        source.update(
            {
                "plan_id": row.get("plan_id"),
                "query_id": row.get("query_id"),
                "query_plan_hash": row.get("query_plan_hash"),
                "last_execution_time": _text(row.get("last_execution_time")),
                "source_note": (
                    "The plan of the last execution of the whole cached batch; find this statement by "
                    "query_plan_hash. Row counts are actual."
                ),
            }
        )
        return PlanSource(str(row["query_plan"]), source)

    async def live(self, database_name: str, session_id: int) -> PlanSource:
        source: dict[str, Any] = {"source": "live_session", "session_id": session_id}
        try:
            rows = await self.executor.fetch_all(database_name, LIVE_PLAN_SQL, params=[int(session_id)])
        except Exception as exc:
            return PlanSource(None, source, _unreadable("sys.dm_exec_query_statistics_xml", exc))
        if not rows or not rows[0].get("query_plan"):
            return PlanSource(
                None,
                source,
                status_payload(
                    ResultStatus.EMPTY,
                    f"Session {session_id} is not running a query with a readable plan in this database.",
                ),
            )
        row = rows[0]
        source.update(
            {
                "request_status": row.get("status"),
                "command": row.get("command"),
                "start_time": _text(row.get("start_time")),
                "elapsed_ms": row.get("total_elapsed_time"),
                "source_note": "In-flight plan: counts are partial and still growing.",
            }
        )
        return PlanSource(str(row["query_plan"]), source)

    async def query_store_runtime(self, database_name: str, plan_id: int, plan_xml: str) -> dict[str, Any]:
        """Measured statement-level runtime and waits for a Query Store plan, over its retention."""

        try:
            rows = await self.executor.fetch_all(database_name, PLAN_RUNTIME_SQL, params=[int(plan_id)])
            waits = await self.executor.fetch_all(database_name, PLAN_WAITS_SQL, params=[int(plan_id)])
        except Exception as exc:
            return _unreadable("Query Store runtime statistics", exc)
        row = rows[0] if rows else {}
        if not row or not row.get("executions"):
            return status_payload(ResultStatus.EMPTY, "Query Store holds no runtime rows for this plan.")
        runtime: dict[str, Any] = {
            key: row.get(key)
            for key in (
                "executions",
                "avg_duration_ms",
                "avg_cpu_ms",
                "avg_logical_reads",
                "avg_rowcount",
                "avg_max_used_memory_kb",
                "max_dop",
            )
        }
        runtime["first_execution_time"] = _text(row.get("first_execution_time"))
        runtime["last_execution_time"] = _text(row.get("last_execution_time"))
        runtime["wait_categories"] = [
            {"category": item.get("wait_category_desc"), "wait_ms": item.get("wait_ms")} for item in waits
        ]
        estimated = _statement_estimated_rows(plan_xml)
        average = _float(row.get("avg_rowcount"))
        if estimated is not None and average is not None:
            high, low = max(estimated, average), max(1.0, min(estimated, average))
            runtime["rows_estimate_check"] = {
                "estimated_rows": estimated,
                "average_rows": round(average, 3),
                "factor": round(high / low, 1),
                "direction": "under" if average > estimated else "over",
                "note": "Root estimate against the average rows Query Store measured per execution.",
            }
        runtime["note"] = "Measured by Query Store over its retention window; the plan itself is an estimate."
        return runtime


def _statement_estimated_rows(plan_xml: str) -> float | None:
    try:
        root = parse_showplan(plan_xml)
    except PlanParseError:
        return None
    for statement, _ in statements(root):
        value = _float(statement.get("StatementEstRows"))
        if value is not None:
            return value
    return None


def _unreadable(what: str, exc: Exception) -> dict[str, Any]:
    return status_payload(
        ResultStatus.UNAVAILABLE,
        f"{what} could not be read: {sanitize_error_message(str(exc))}. VIEW DATABASE STATE is required.",
    )


def _float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)
