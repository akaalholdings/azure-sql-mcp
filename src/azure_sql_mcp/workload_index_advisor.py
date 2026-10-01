"""Collect Query Store workload and catalog evidence, then run the index advisor.

All reads are read-only and need only ``VIEW DATABASE STATE`` and
``VIEW DEFINITION`` (plus ``SELECT`` on tables for statistics histograms). No
history tables, policy file, or installation step is required. Optional
evidence (operational stats, histograms, foreign keys, Query Store reference
checks) degrades to a reported gap instead of failing the review.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

from .connection import AzureSqlExecutor
from .index_advisor import OBJECTIVES
from .index_advisor import AdvisorInputs
from .index_advisor import AdvisorSettings
from .index_advisor import ColumnInfo
from .index_advisor import ColumnSelectivity
from .index_advisor import ForeignKeyInfo
from .index_advisor import TableInfo
from .index_advisor import WorkloadQuery
from .index_advisor import build_index_advice
from .index_metadata import ExistingIndex
from .index_metadata import collect_existing_indexes
from .observability import sanitize_error_message
from .result_status import ResultStatus
from .result_status import status_payload
from .showplan_access import parse_plan_access

MAX_LOOKBACK_DAYS = 90
MAX_TOP_QUERIES = 500
MAX_PLANS_PER_QUERY = 3
MAX_DETAIL_TABLES = 200
MAX_REFERENCE_CHECKS = 40
QUERY_TEXT_PREVIEW_CHARS = 300

QUERY_STORE_OPTIONS_SQL = """
SELECT
    actual_state_desc,
    desired_state_desc,
    readonly_reason,
    query_capture_mode_desc,
    interval_length_minutes,
    stale_query_threshold_days,
    max_storage_size_mb,
    current_storage_size_mb
FROM sys.database_query_store_options
"""

_WORKLOAD_SQL_TEMPLATE = """
WITH plan_stats AS (
    SELECT
        p.query_id,
        rs.plan_id,
        SUM(CAST(rs.count_executions AS float)) AS executions,
        SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu_us,
        SUM(rs.avg_duration * rs.count_executions) AS total_duration_us,
        SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_reads,
        SUM(rs.avg_logical_io_writes * rs.count_executions) AS total_logical_writes,
        SUM(rs.avg_rowcount * rs.count_executions) AS total_rowcount,
        COUNT(DISTINCT CONVERT(date, rsi.start_time)) AS active_days
    FROM sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi
        ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
    INNER JOIN sys.query_store_plan AS p
        ON p.plan_id = rs.plan_id
    WHERE rsi.start_time < TODATETIMEOFFSET(CAST(? AS datetime2), 0)
      AND rsi.end_time > TODATETIMEOFFSET(CAST(? AS datetime2), 0)
    GROUP BY p.query_id, rs.plan_id
),
query_stats AS (
    SELECT
        query_id,
        SUM(executions) AS executions,
        SUM(total_cpu_us) AS total_cpu_us,
        SUM(total_duration_us) AS total_duration_us,
        SUM(total_logical_reads) AS total_logical_reads,
        SUM(total_logical_writes) AS total_logical_writes,
        SUM(total_rowcount) AS total_rowcount,
        MAX(active_days) AS active_days
    FROM plan_stats
    GROUP BY query_id
),
totals AS (
    SELECT
        COUNT_BIG(*) AS workload_query_count,
        SUM(executions) AS workload_executions,
        SUM(total_cpu_us) AS workload_cpu_us,
        SUM(total_duration_us) AS workload_duration_us,
        SUM(total_logical_reads) AS workload_logical_reads
    FROM query_stats
),
ranked AS (
    SELECT TOP (?)
        qs.*,
        ROW_NUMBER() OVER (ORDER BY qs.{objective} DESC, qs.query_id) AS workload_rank
    FROM query_stats AS qs
    ORDER BY qs.{objective} DESC, qs.query_id
),
dominant AS (
    SELECT
        ps.*,
        ROW_NUMBER() OVER (
            PARTITION BY ps.query_id ORDER BY ps.{objective} DESC, ps.plan_id
        ) AS plan_rank
    FROM plan_stats AS ps
    WHERE ps.query_id IN (SELECT query_id FROM ranked)
)
SELECT
    r.query_id,
    d.plan_id,
    d.plan_rank,
    r.workload_rank,
    d.executions,
    d.total_cpu_us,
    d.total_duration_us,
    d.total_logical_reads,
    d.total_logical_writes,
    d.total_rowcount,
    d.active_days,
    t.workload_query_count,
    t.workload_executions,
    t.workload_cpu_us,
    t.workload_duration_us,
    t.workload_logical_reads,
    OBJECT_SCHEMA_NAME(q.object_id) AS object_schema,
    OBJECT_NAME(q.object_id) AS object_name,
    LEFT(qt.query_sql_text, {preview}) AS query_text_preview,
    p.is_forced_plan,
    CAST(p.query_plan AS nvarchar(max)) AS query_plan
FROM ranked AS r
CROSS JOIN totals AS t
INNER JOIN dominant AS d
    ON d.query_id = r.query_id
   AND d.plan_rank <= ?
INNER JOIN sys.query_store_query AS q
    ON q.query_id = r.query_id
INNER JOIN sys.query_store_query_text AS qt
    ON qt.query_text_id = q.query_text_id
INNER JOIN sys.query_store_plan AS p
    ON p.plan_id = d.plan_id
ORDER BY r.workload_rank, d.plan_rank
"""

_OBJECTIVE_SORT_COLUMNS = {
    "cpu": "total_cpu_us",
    "duration": "total_duration_us",
    "logical_reads": "total_logical_reads",
    "executions": "executions",
}

TABLES_SQL = """
SELECT TOP (20000)
    t.object_id,
    s.name AS schema_name,
    t.name AS table_name,
    t.is_memory_optimized,
    SUM(CASE WHEN ps.index_id IN (0, 1) THEN ps.row_count ELSE 0 END) AS row_count,
    SUM(CASE WHEN ps.index_id IN (0, 1) THEN ps.used_page_count ELSE 0 END) AS base_used_pages,
    SUM(ps.used_page_count) AS all_used_pages,
    MAX(CASE WHEN ps.index_id = 0 THEN 1 ELSE 0 END) AS is_heap
FROM sys.tables AS t
INNER JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
LEFT JOIN sys.dm_db_partition_stats AS ps
    ON ps.object_id = t.object_id
WHERE t.is_ms_shipped = 0
GROUP BY t.object_id, s.name, t.name, t.is_memory_optimized
ORDER BY t.object_id
"""

_COLUMNS_SQL_TEMPLATE = """
SELECT
    c.object_id,
    c.name AS column_name,
    CASE WHEN ty.is_assembly_type = 1 THEN ty.name ELSE TYPE_NAME(c.system_type_id) END AS type_name,
    c.max_length,
    c.is_nullable,
    c.is_computed
FROM sys.columns AS c
INNER JOIN sys.types AS ty
    ON ty.user_type_id = c.user_type_id
WHERE c.object_id IN ({ids})
"""

_OPERATIONAL_SQL_TEMPLATE = """
SELECT
    os.object_id,
    SUM(os.forwarded_fetch_count) AS forwarded_fetches
FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) AS os
WHERE os.index_id = 0
  AND os.object_id IN ({ids})
GROUP BY os.object_id
"""

_SELECTIVITY_SQL_TEMPLATE = """
SELECT
    st.object_id,
    c.name AS column_name,
    st.name AS stats_name,
    sp.rows,
    sp.rows_sampled,
    sp.modification_counter,
    CONVERT(varchar(33), sp.last_updated, 127) AS last_updated_utc,
    h.distinct_estimate
FROM sys.stats AS st
INNER JOIN sys.stats_columns AS sc
    ON sc.object_id = st.object_id
   AND sc.stats_id = st.stats_id
   AND sc.stats_column_id = 1
INNER JOIN sys.columns AS c
    ON c.object_id = sc.object_id
   AND c.column_id = sc.column_id
CROSS APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) AS sp
OUTER APPLY (
    SELECT SUM(CAST(hg.distinct_range_rows AS float)) + COUNT_BIG(*) AS distinct_estimate
    FROM sys.dm_db_stats_histogram(st.object_id, st.stats_id) AS hg
) AS h
WHERE st.object_id IN ({ids})
"""

FOREIGN_KEYS_SQL = """
SELECT TOP (5000)
    fk.object_id AS foreign_key_id,
    fk.name AS foreign_key_name,
    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS schema_name,
    OBJECT_NAME(fk.parent_object_id) AS table_name,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
    OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
    fkc.constraint_column_id,
    c.name AS column_name,
    fk.delete_referential_action_desc,
    fk.is_disabled
FROM sys.foreign_keys AS fk
INNER JOIN sys.foreign_key_columns AS fkc
    ON fkc.constraint_object_id = fk.object_id
INNER JOIN sys.columns AS c
    ON c.object_id = fkc.parent_object_id
   AND c.column_id = fkc.parent_column_id
ORDER BY fk.object_id, fkc.constraint_column_id
"""

_INDEX_REFERENCE_SQL_TEMPLATE = """
SELECT
    v.ref_id,
    COUNT(p.plan_id) AS plan_references
FROM (VALUES {values}) AS v(ref_id, pattern)
LEFT JOIN sys.query_store_plan AS p
    ON p.last_execution_time > TODATETIMEOFFSET(CAST(? AS datetime2), 0)
   AND CHARINDEX(v.pattern, CAST(p.query_plan AS nvarchar(max))) > 0
GROUP BY v.ref_id
"""


def workload_sql(objective: str) -> str:
    column = _OBJECTIVE_SORT_COLUMNS[objective]
    return _WORKLOAD_SQL_TEMPLATE.format(objective=column, preview=QUERY_TEXT_PREVIEW_CHARS)


def resolve_window(lookback_days: int, as_of_utc: str | None, *, now: datetime | None = None) -> tuple[datetime, datetime]:
    current = now or datetime.now(timezone.utc)
    if as_of_utc:
        text = as_of_utc.strip()
        if len(text) == 10:
            text += "T00:00:00"
        try:
            end = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                "as_of_utc must be an ISO-8601 UTC instant such as 2026-09-30T05:00:00Z"
            ) from exc
        end = end.astimezone(timezone.utc) if end.tzinfo else end.replace(tzinfo=timezone.utc)
        if end > current + timedelta(minutes=1):
            raise ValueError("as_of_utc must not be in the future")
    else:
        end = current
    return end - timedelta(days=lookback_days), end


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


def _norm(value: str) -> str:
    return value.casefold()


class WorkloadIndexAdvisor:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def review(
        self,
        database_name: str,
        *,
        schema_name: str | None = None,
        table_names: list[str] | None = None,
        lookback_days: int = 7,
        as_of_utc: str | None = None,
        objective: str = "cpu",
        top_queries: int = 100,
        plans_per_query: int = 1,
        min_table_rows: int = 10_000,
        max_recommendations_per_table: int = 5,
        include_existing_index_review: bool = True,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if objective not in OBJECTIVES:
            raise ValueError(f"objective must be one of {sorted(OBJECTIVES)}")
        lookback_days = max(1, min(int(lookback_days), MAX_LOOKBACK_DAYS))
        top_queries = max(1, min(int(top_queries), MAX_TOP_QUERIES))
        plans_per_query = max(1, min(int(plans_per_query), MAX_PLANS_PER_QUERY))
        if table_names and not schema_name:
            raise ValueError("table_names requires schema_name")
        start, end = resolve_window(lookback_days, as_of_utc, now=now)
        gaps: list[str] = []

        query_store, query_store_readable = await self._query_store_options(database_name, gaps)
        queries: list[WorkloadQuery] = []
        totals: dict[str, float] = {}
        if query_store_readable:
            try:
                queries, totals = await self._workload(
                    database_name, objective, start, end, top_queries, plans_per_query
                )
            except Exception as exc:
                gaps.append(
                    "Query Store workload could not be read: "
                    + sanitize_error_message(str(exc))
                )
                query_store_readable = False

        try:
            existing = await collect_existing_indexes(self.executor, database_name)
        except Exception as exc:
            return {
                "database_name": database_name,
                "recommend_only": True,
                "recommendations": [],
                "gaps": gaps,
                **status_payload(
                    ResultStatus.UNAVAILABLE,
                    "Existing index metadata could not be read, so no index advice is safe: "
                    + sanitize_error_message(str(exc))
                    + ". VIEW DEFINITION and VIEW DATABASE STATE are required.",
                ),
            }
        tables = await self._tables(database_name, gaps)
        by_id: dict[int, tuple[str, str]] = {
            object_id: key
            for key, info in tables.items()
            if (object_id := info.object_id) is not None
        }

        focus_tables: frozenset[tuple[str, str]] | None = None
        if table_names and schema_name:
            focus_tables = frozenset((_norm(schema_name), _norm(name)) for name in table_names)
        settings = AdvisorSettings(
            objective=objective,
            lookback_days=lookback_days,
            min_table_rows=max(0, int(min_table_rows)),
            max_recommendations_per_table=max(1, int(max_recommendations_per_table)),
            include_existing_index_review=include_existing_index_review,
            focus_tables=focus_tables,
            focus_schema=schema_name if not table_names else None,
        )

        detail_keys = self._detail_tables(queries, tables, focus_tables, schema_name)
        detail_ids = [
            object_id
            for key in detail_keys
            if key in tables and (object_id := tables[key].object_id) is not None
        ][:MAX_DETAIL_TABLES]
        heap_ids = [
            object_id
            for info in tables.values()
            if info.is_heap and (object_id := info.object_id) is not None
        ][:MAX_DETAIL_TABLES]

        columns = await self._columns(database_name, detail_ids, by_id, gaps)
        await self._forwarded_fetches(database_name, heap_ids, tables, by_id, gaps)
        selectivity = await self._selectivity(database_name, detail_ids, by_id, gaps)
        foreign_keys = await self._foreign_keys(database_name, gaps)
        references = (
            await self._index_references(database_name, existing, start, gaps)
            if include_existing_index_review and query_store_readable
            else {}
        )
        if include_existing_index_review and not query_store_readable:
            gaps.append(
                "Query Store plan references could not be checked; unused-index findings stay low confidence"
            )

        engine_start = next(
            (
                index.usage_context.get("engine_start_time_utc")
                for index in existing
                if index.usage_context.get("engine_start_time_utc")
            ),
            None,
        )
        inputs = AdvisorInputs(
            database_name=database_name,
            window_start_utc=_iso(start) + "Z",
            window_end_utc=_iso(end) + "Z",
            settings=settings,
            queries=queries,
            workload_totals=totals,
            tables=tables,
            columns=columns,
            existing_indexes=existing,
            selectivity=selectivity,
            foreign_keys=foreign_keys,
            index_plan_references=references,
            engine_start_time_utc=engine_start,
            query_store=query_store,
            gaps=gaps,
            observed_at_utc=(now or datetime.now(timezone.utc)).isoformat(),
        )
        report = build_index_advice(inputs)
        report.update(self._overall_status(report, query_store, query_store_readable, queries))
        return report

    # ------------------------------------------------------------------

    async def _query_store_options(
        self, database_name: str, gaps: list[str]
    ) -> tuple[dict[str, Any], bool]:
        try:
            rows = await self.executor.fetch_all(database_name, QUERY_STORE_OPTIONS_SQL)
        except Exception as exc:
            gaps.append("Query Store options could not be read: " + sanitize_error_message(str(exc)))
            return {"actual_state": None}, False
        if not rows:
            return {"actual_state": None}, False
        row = rows[0]
        state = str(row.get("actual_state_desc") or "").upper()
        options = {
            "actual_state": state or None,
            "desired_state": row.get("desired_state_desc"),
            "readonly_reason": row.get("readonly_reason"),
            "query_capture_mode": row.get("query_capture_mode_desc"),
            "interval_length_minutes": row.get("interval_length_minutes"),
            "stale_query_threshold_days": row.get("stale_query_threshold_days"),
            "max_storage_size_mb": row.get("max_storage_size_mb"),
            "current_storage_size_mb": row.get("current_storage_size_mb"),
        }
        readable = state in {"READ_WRITE", "READ_ONLY"}
        if state == "READ_ONLY":
            gaps.append(
                "Query Store is READ_ONLY"
                + (f" (reason {options['readonly_reason']})" if options["readonly_reason"] else "")
                + "; recent workload may be missing"
            )
        return options, readable

    async def _workload(
        self,
        database_name: str,
        objective: str,
        start: datetime,
        end: datetime,
        top_queries: int,
        plans_per_query: int,
    ) -> tuple[list[WorkloadQuery], dict[str, float]]:
        rows = await self.executor.fetch_all(
            database_name,
            workload_sql(objective),
            params=[_iso(end), _iso(start), top_queries, plans_per_query],
        )
        totals: dict[str, float] = {}
        queries: list[WorkloadQuery] = []
        for row in rows:
            if not totals:
                totals = {
                    "workload_query_count": _float(row.get("workload_query_count")),
                    "workload_executions": _float(row.get("workload_executions")),
                    "total_cpu_us": _float(row.get("workload_cpu_us")),
                    "total_duration_us": _float(row.get("workload_duration_us")),
                    "total_logical_reads": _float(row.get("workload_logical_reads")),
                    "executions": _float(row.get("workload_executions")),
                }
            object_name = row.get("object_name")
            object_schema = row.get("object_schema")
            queries.append(
                WorkloadQuery(
                    query_id=int(row.get("query_id") or 0),
                    plan_id=int(row.get("plan_id") or 0),
                    plan=parse_plan_access(str(row.get("query_plan") or "")),
                    executions=_float(row.get("executions")),
                    total_cpu_us=_float(row.get("total_cpu_us")),
                    total_duration_us=_float(row.get("total_duration_us")),
                    total_logical_reads=_float(row.get("total_logical_reads")),
                    total_logical_writes=_float(row.get("total_logical_writes")),
                    total_rowcount=_float(row.get("total_rowcount")),
                    active_days=int(_float(row.get("active_days"))),
                    object_name=(
                        f"{object_schema}.{object_name}" if object_name and object_schema else object_name
                    ),
                    query_text_preview=row.get("query_text_preview"),
                    is_forced_plan=bool(row.get("is_forced_plan")),
                )
            )
        return queries, totals

    async def _tables(self, database_name: str, gaps: list[str]) -> dict[tuple[str, str], TableInfo]:
        try:
            rows = await self.executor.fetch_all(database_name, TABLES_SQL)
        except Exception as exc:
            gaps.append("Table sizes could not be read: " + sanitize_error_message(str(exc)))
            return {}
        tables: dict[tuple[str, str], TableInfo] = {}
        for row in rows:
            schema = str(row.get("schema_name") or "")
            table = str(row.get("table_name") or "")
            if not schema or not table:
                continue
            info = TableInfo(
                schema=schema,
                table=table,
                object_id=_int_or_none(row.get("object_id")),
                row_count=_int_or_none(row.get("row_count")),
                base_used_pages=_int_or_none(row.get("base_used_pages")),
                all_used_pages=_int_or_none(row.get("all_used_pages")),
                is_heap=bool(row.get("is_heap")),
                is_memory_optimized=bool(row.get("is_memory_optimized")),
            )
            tables[info.key] = info
        return tables

    @staticmethod
    def _detail_tables(
        queries: list[WorkloadQuery],
        tables: dict[tuple[str, str], TableInfo],
        focus_tables: frozenset[tuple[str, str]] | None,
        schema_name: str | None,
    ) -> list[tuple[str, str]]:
        keys: dict[tuple[str, str], None] = {}
        for query in queries:
            for access in query.plan.accesses:
                key = (_norm(access.schema), _norm(access.table))
                if key in tables:
                    keys[key] = None
        if focus_tables:
            keys = {key: None for key in keys if key in focus_tables}
            for key in focus_tables:
                if key in tables:
                    keys[key] = None
        elif schema_name:
            keys = {key: None for key in keys if key[0] == _norm(schema_name)}
        return list(keys)

    async def _columns(
        self,
        database_name: str,
        object_ids: list[int],
        by_id: dict[int, tuple[str, str]],
        gaps: list[str],
    ) -> dict[tuple[str, str], dict[str, ColumnInfo]]:
        ids = _id_list(object_ids)
        if not ids:
            return {}
        try:
            rows = await self.executor.fetch_all(database_name, _COLUMNS_SQL_TEMPLATE.format(ids=ids))
        except Exception as exc:
            gaps.append("Column metadata could not be read: " + sanitize_error_message(str(exc)))
            return {}
        columns: dict[tuple[str, str], dict[str, ColumnInfo]] = {}
        for row in rows:
            key = _lookup(by_id, row.get("object_id"))
            name = row.get("column_name")
            if key is None or not name:
                continue
            columns.setdefault(key, {})[_norm(str(name))] = ColumnInfo(
                name=str(name),
                type_name=str(row.get("type_name") or ""),
                max_length=int(_float(row.get("max_length"))),
                is_nullable=bool(row.get("is_nullable")),
                is_computed=bool(row.get("is_computed")),
            )
        return columns

    async def _forwarded_fetches(
        self,
        database_name: str,
        object_ids: list[int],
        tables: dict[tuple[str, str], TableInfo],
        by_id: dict[int, tuple[str, str]],
        gaps: list[str],
    ) -> None:
        ids = _id_list(object_ids)
        if not ids:
            return
        try:
            rows = await self.executor.fetch_all(
                database_name, _OPERATIONAL_SQL_TEMPLATE.format(ids=ids)
            )
        except Exception as exc:
            gaps.append("Heap forwarded-record counters could not be read: " + sanitize_error_message(str(exc)))
            return
        for row in rows:
            key = _lookup(by_id, row.get("object_id"))
            if key is not None and key in tables:
                tables[key].forwarded_fetches = _int_or_none(row.get("forwarded_fetches"))

    async def _selectivity(
        self,
        database_name: str,
        object_ids: list[int],
        by_id: dict[int, tuple[str, str]],
        gaps: list[str],
    ) -> dict[tuple[str, str, str], ColumnSelectivity]:
        ids = _id_list(object_ids)
        if not ids:
            return {}
        try:
            rows = await self.executor.fetch_all(
                database_name, _SELECTIVITY_SQL_TEMPLATE.format(ids=ids)
            )
        except Exception as exc:
            gaps.append("Statistics histograms could not be read: " + sanitize_error_message(str(exc)))
            return {}
        best: dict[tuple[str, str, str], tuple[float, str, ColumnSelectivity]] = {}
        for row in rows:
            key = _lookup(by_id, row.get("object_id"))
            column = row.get("column_name")
            if key is None or not column:
                continue
            item_key = (key[0], key[1], _norm(str(column)))
            sampled = _float(row.get("rows_sampled"))
            updated = str(row.get("last_updated_utc") or "")
            value = ColumnSelectivity(
                distinct_estimate=_float_or_none(row.get("distinct_estimate")),
                rows=_float_or_none(row.get("rows")),
                modification_counter=_int_or_none(row.get("modification_counter")),
                stats_name=row.get("stats_name"),
                last_updated_utc=updated or None,
            )
            current = best.get(item_key)
            if current is None or (sampled, updated) > (current[0], current[1]):
                best[item_key] = (sampled, updated, value)
        return {key: value for key, (_, _, value) in best.items()}

    async def _foreign_keys(self, database_name: str, gaps: list[str]) -> list[ForeignKeyInfo]:
        try:
            rows = await self.executor.fetch_all(database_name, FOREIGN_KEYS_SQL)
        except Exception as exc:
            gaps.append("Foreign keys could not be read: " + sanitize_error_message(str(exc)))
            return []
        grouped: dict[Any, dict[str, Any]] = {}
        for row in rows:
            fk_id = row.get("foreign_key_id")
            entry = grouped.setdefault(
                fk_id,
                {
                    "name": str(row.get("foreign_key_name") or ""),
                    "schema": str(row.get("schema_name") or ""),
                    "table": str(row.get("table_name") or ""),
                    "referenced_schema": str(row.get("referenced_schema") or ""),
                    "referenced_table": str(row.get("referenced_table") or ""),
                    "delete_action": row.get("delete_referential_action_desc"),
                    "is_disabled": bool(row.get("is_disabled")),
                    "columns": [],
                },
            )
            if row.get("column_name"):
                entry["columns"].append(str(row["column_name"]))
        return [
            ForeignKeyInfo(
                name=entry["name"],
                schema=entry["schema"],
                table=entry["table"],
                columns=tuple(entry["columns"]),
                referenced_schema=entry["referenced_schema"],
                referenced_table=entry["referenced_table"],
                delete_action=entry["delete_action"],
                is_disabled=entry["is_disabled"],
            )
            for entry in grouped.values()
            if entry["schema"] and entry["table"] and entry["columns"]
        ]

    async def _index_references(
        self,
        database_name: str,
        existing: list[ExistingIndex],
        start: datetime,
        gaps: list[str],
    ) -> dict[tuple[str, str, str], int | None]:
        unused = [
            index
            for index in existing
            if index.index_type_code == 2
            and not index.is_disabled
            and not index.is_hypothetical
            and sum(int(index.usage.get(m) or 0) for m in ("user_seeks", "user_scans", "user_lookups")) == 0
        ]
        if not unused:
            return {}
        checked = unused[:MAX_REFERENCE_CHECKS]
        if len(unused) > len(checked):
            gaps.append(
                f"Query Store references were checked for {len(checked)} of {len(unused)} unused indexes"
            )
        values = ", ".join("(?, ?)" for _ in checked)
        params: list[Any] = []
        for position, index in enumerate(checked):
            params.extend([position, 'Index="[' + index.name.replace("]", "]]") + ']"'])
        params.append(_iso(start))
        try:
            rows = await self.executor.fetch_all(
                database_name, _INDEX_REFERENCE_SQL_TEMPLATE.format(values=values), params=params
            )
        except Exception as exc:
            gaps.append(
                "Query Store index references could not be checked: " + sanitize_error_message(str(exc))
            )
            return {(_norm(i.schema), _norm(i.table), _norm(i.name)): None for i in checked}
        counts = {int(_float(row.get("ref_id"))): int(_float(row.get("plan_references"))) for row in rows}
        return {
            (_norm(index.schema), _norm(index.table), _norm(index.name)): counts.get(position)
            for position, index in enumerate(checked)
        }

    @staticmethod
    def _overall_status(
        report: dict[str, Any],
        query_store: dict[str, Any],
        query_store_readable: bool,
        queries: list[WorkloadQuery],
    ) -> dict[str, Any]:
        recommendations = report.get("recommendations") or []
        if not query_store_readable:
            state = query_store.get("actual_state")
            remediation = (
                "ALTER DATABASE CURRENT SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE);"
                if state in {None, "OFF", "ERROR"}
                else None
            )
            return status_payload(
                ResultStatus.PRECONDITION,
                "Query Store is not readable"
                + (f" (state {state})" if state else "")
                + ", so no workload-driven index advice was produced. Existing-index "
                "findings below come from catalog and usage counters only.",
                remediation=remediation,
            )
        if not queries and not recommendations:
            return status_payload(
                ResultStatus.EMPTY,
                "Query Store held no workload in the window and no existing-index finding applied.",
            )
        return status_payload(
            ResultStatus.OK,
            f"Analysed {len(queries)} Query Store plan(s); {len(recommendations)} recommendation(s).",
        )


def _id_list(object_ids: list[int]) -> str:
    return ", ".join(str(int(value)) for value in dict.fromkeys(object_ids))


def _lookup(by_id: dict[int, tuple[str, str]], value: Any) -> tuple[str, str] | None:
    object_id = _int_or_none(value)
    return by_id.get(object_id) if object_id is not None else None


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
