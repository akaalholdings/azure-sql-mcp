"""Collect Query Store workload and catalog evidence, then run the index advisor.

All reads are read-only and need only ``VIEW DATABASE STATE`` and
``VIEW DEFINITION`` (plus ``SELECT`` on tables for statistics histograms; on
Basic, S0, S1 and elastic-pool databases the index usage counters need
``##MS_ServerStateReader##`` or an admin, see ``azure_tier``). No
history tables, policy file, or installation step is required. Optional
evidence (operational stats, histograms, foreign keys, Query Store reference
checks) degrades to a reported gap instead of failing the review.
"""

from __future__ import annotations

import asyncio
import copy
import threading
from collections.abc import Awaitable
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import TypeVar
from typing import cast

from .azure_tier import dmv_permission_hint
from .azure_tier import read_dmv
from .columnstore_health import ROW_GROUP_DMV
from .columnstore_health import ROW_GROUP_SQL
from .columnstore_health import summarize_columnstore_indexes
from .connection import AzureSqlExecutor
from .index_advisor import COLUMNSTORE_TYPE_CODES
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
from .index_metadata import collect_readable_replica_status
from .incident_log import note_exception
from .index_review import _reference_is_read
from .observability import sanitize_error_message
from .plans import parse_showplan_index_evidence
from .query_store import QueryStoreService
from .result_status import RESULT_STATUS_REASON_KEY
from .result_status import ResultStatus
from .result_status import status_payload
from .showplan_access import parse_plan_access
from .time_windows import sql_utc
from .time_windows import window_bounds

MAX_LOOKBACK_DAYS = 90
MAX_TOP_QUERIES = 500
MAX_PLANS_PER_QUERY = 3
MAX_DETAIL_TABLES = 200
MAX_REFERENCE_CHECKS = 40
# Query ids a caller may pin into the analysed set, whatever their rank.
MAX_QUERY_IDS = 50
# Reads one review runs at once. The connection pool default is 5; a review
# that took every connection would make other tool calls wait.
MAX_CONCURRENT_READS = 4
QUERY_TEXT_PREVIEW_CHARS = 300
# Forced-plan dependency read: plan count, bytes per plan XML, bytes in total.
MAX_FORCED_PLANS = 500
MAX_FORCED_PLAN_XML_BYTES = 4_000_000
MAX_FORCED_PLAN_TOTAL_BYTES = 64_000_000
# Plan XML the unused-index reference read returns in total. It runs beside the
# forced-plan read, so it gets a smaller share; plans past it stay use. Plans
# take turns per index, smallest first, so one index cannot use all of it.
MAX_INDEX_REFERENCE_PLAN_TOTAL_BYTES = 16_000_000
# Rows read per hint source (query text, Query Store hints, plan guides, modules).
MAX_HINT_SOURCE_ROWS = 2_000
# Each optional dependency read runs concurrently and gives up after the
# executor's configured query timeout, or this when it has none.
OPTIONAL_READ_TIMEOUT_SECONDS = 30.0
# Hint-scan blockers for a hint that matches no single index; it may name any
# index advised for removal, so removals get no DDL.
_UNRESOLVED_HINT_BLOCKERS = frozenset(
    {"unresolved_or_ambiguous_index_hint", "unresolved_or_ambiguous_numeric_index_hint"}
)
# The hint matched no index of the database, so no index here depends on it.
_HINT_NAMES_NO_INDEX = "index_hint_names_no_index"
_HINT_SOURCE_NAMES = {
    "query_store_text": "Query Store query text",
    "query_store_query_hints": "Query Store hints",
    "plan_guides": "plan guides",
    "module_definitions": "module definitions",
}
_REMOVALS_CAPPED = "unused and redundant index removals are capped at medium confidence"

QUERY_STORE_OPTIONS_SQL = """
SELECT
    actual_state_desc,
    desired_state_desc,
    readonly_reason,
    query_capture_mode_desc,
    interval_length_minutes,
    stale_query_threshold_days,
    max_storage_size_mb,
    current_storage_size_mb,
    size_based_cleanup_mode_desc
FROM sys.database_query_store_options
"""

# The part of the window that Query Store still holds, plus its oldest interval.
QUERY_STORE_COVERAGE_SQL = """
SELECT
    CONVERT(datetime2(0), SWITCHOFFSET(MIN(rsi.start_time), '+00:00')) AS effective_start_utc,
    CONVERT(datetime2(0), SWITCHOFFSET(MAX(rsi.end_time), '+00:00')) AS effective_end_utc,
    COUNT_BIG(*) AS interval_count,
    (
        SELECT CONVERT(datetime2(0), SWITCHOFFSET(MIN(oldest.start_time), '+00:00'))
        FROM sys.query_store_runtime_stats_interval AS oldest
    ) AS oldest_interval_start_utc
FROM sys.query_store_runtime_stats_interval AS rsi
WHERE rsi.start_time < TODATETIMEOFFSET(CAST(? AS datetime2), 0)
  AND rsi.end_time > TODATETIMEOFFSET(CAST(? AS datetime2), 0)
"""

# Every forced plan, not only the analysed top queries. Plan XML over the
# per-plan or running-total byte cap is withheld and reported as unread.
FORCED_PLANS_SQL = """
SELECT TOP (?)
    p.plan_id,
    p.query_id,
    DATALENGTH(p.query_plan) AS plan_bytes,
    CASE
        WHEN DATALENGTH(p.query_plan) <= ?
         AND SUM(CASE WHEN DATALENGTH(p.query_plan) <= ? THEN DATALENGTH(p.query_plan) ELSE 0 END)
             OVER (ORDER BY p.plan_id ROWS UNBOUNDED PRECEDING) <= ?
        THEN CAST(p.query_plan AS nvarchar(max))
    END AS query_plan
FROM sys.query_store_plan AS p
WHERE p.is_forced_plan = 1
ORDER BY p.plan_id
"""

_T = TypeVar("_T")

# One pass over the runtime aggregate: SQL Server inlines a CTE at each
# reference, so plan_stats and every CTE built on it are referenced once.
# Per-query totals, plan rank, workload totals and workload rank are window
# functions over that one result.
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
windowed AS (
    SELECT
        ps.*,
        SUM(ps.{objective}) OVER (PARTITION BY ps.query_id) AS query_objective,
        ROW_NUMBER() OVER (
            PARTITION BY ps.query_id ORDER BY ps.{objective} DESC, ps.plan_id
        ) AS plan_rank,
        SUM(ps.executions) OVER () AS workload_executions,
        SUM(ps.total_cpu_us) OVER () AS workload_cpu_us,
        SUM(ps.total_duration_us) OVER () AS workload_duration_us,
        SUM(ps.total_logical_reads) OVER () AS workload_logical_reads{in_scope}
    FROM plan_stats AS ps{scope_join}
),
ranked AS (
    SELECT
        w.*,
        SUM(CASE WHEN w.plan_rank = 1 THEN CAST(1 AS bigint) ELSE 0 END) OVER () AS workload_query_count,
        DENSE_RANK() OVER (
            ORDER BY {scope_first}w.query_objective DESC, w.query_id
        ) AS workload_rank
    FROM windowed AS w
)
SELECT
    r.query_id,
    r.plan_id,
    r.plan_rank,
    r.workload_rank,
    r.executions,
    r.total_cpu_us,
    r.total_duration_us,
    r.total_logical_reads,
    r.total_logical_writes,
    r.total_rowcount,
    r.active_days,
    r.workload_query_count,
    r.workload_executions,
    r.workload_cpu_us,
    r.workload_duration_us,
    r.workload_logical_reads,
    OBJECT_SCHEMA_NAME(q.object_id) AS object_schema,
    OBJECT_NAME(q.object_id) AS object_name,
    LEFT(qt.query_sql_text, {preview}) AS query_text_preview,
    p.is_forced_plan,
    CAST(p.query_plan AS nvarchar(max)) AS query_plan
FROM ranked AS r
INNER JOIN sys.query_store_query AS q
    ON q.query_id = r.query_id
INNER JOIN sys.query_store_query_text AS qt
    ON qt.query_text_id = q.query_text_id
INNER JOIN sys.query_store_plan AS p
    ON p.plan_id = r.plan_id
WHERE ({top_queries}{pinned})
  AND r.plan_rank <= ?
ORDER BY r.workload_rank, r.plan_rank
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
    c.is_computed,
    COLUMNPROPERTY(c.object_id, c.name, 'IsIndexable') AS is_indexable,
    COLUMNPROPERTY(c.object_id, c.name, 'IsDeterministic') AS is_deterministic
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

_LEAF_ACTIVITY_SQL_TEMPLATE = """
SELECT
    os.object_id,
    i.name AS index_name,
    SUM(os.leaf_insert_count) AS leaf_insert_count,
    SUM(os.leaf_allocation_count) AS leaf_allocation_count
FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) AS os
INNER JOIN sys.indexes AS i
    ON i.object_id = os.object_id
   AND i.index_id = os.index_id
WHERE os.index_id > 0
  AND os.object_id IN ({ids})
GROUP BY os.object_id, i.name
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

# One row per foreign key column; one more than the cap is read to detect it.
MAX_FOREIGN_KEY_ROWS = 5_000
FOREIGN_KEYS_SQL = """
SELECT TOP (?)
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

# One row per (index, plan) match in the window: index_match when the plan
# names the index (Index="[name]"), stats_match when its OptimizerStatsUsage
# read the index statistics. The plans are the outer side, so each plan XML is
# read once and every pattern is applied to it; the comparison is binary
# because showplan writes exact catalog names. Each index_match plan's XML
# comes back once (on its first row) within the byte budget, so Python can tell
# a read of the index from a write plan that only maintains it. The budget is
# spent round-robin: each index's smallest plan, then each index's next
# smallest, and so on. Internal queries (ALTER INDEX REBUILD, UPDATE
# STATISTICS) are maintenance, not use.
_INDEX_REFERENCE_SQL_TEMPLATE = """
WITH refs AS (
    SELECT v.ref_id, v.index_pattern, v.stats_pattern
    FROM (VALUES {values}) AS v(ref_id, index_pattern, stats_pattern)
),
matches AS (
    SELECT
        hit.ref_id,
        x.plan_id,
        x.query_id,
        x.is_forced_plan,
        x.plan_bytes,
        CASE WHEN hit.index_at > 0 THEN 1 ELSE 0 END AS index_match,
        CASE WHEN hit.stats_at > 0 THEN 1 ELSE 0 END AS stats_match
    FROM (
        SELECT
            p.plan_id,
            p.query_id,
            CAST(p.is_forced_plan AS int) AS is_forced_plan,
            DATALENGTH(p.query_plan) AS plan_bytes,
            CAST(p.query_plan AS nvarchar(max)) AS plan_xml
        FROM sys.query_store_plan AS p
        INNER JOIN sys.query_store_query AS q
            ON q.query_id = p.query_id
        WHERE p.last_execution_time > TODATETIMEOFFSET(CAST(? AS datetime2), 0)
          AND q.is_internal_query = 0
    ) AS x
    CROSS APPLY (
        SELECT
            v.ref_id,
            CHARINDEX(v.index_pattern COLLATE Latin1_General_100_BIN2, x.plan_xml COLLATE Latin1_General_100_BIN2) AS index_at,
            CHARINDEX(v.stats_pattern COLLATE Latin1_General_100_BIN2, x.plan_xml COLLATE Latin1_General_100_BIN2) AS stats_at
        FROM refs AS v
    ) AS hit
    WHERE hit.index_at > 0
       OR hit.stats_at > 0
),
ranked AS (
    SELECT
        m.*,
        ROW_NUMBER() OVER (PARTITION BY m.plan_id ORDER BY m.index_match DESC, m.ref_id) AS plan_row
    FROM matches AS m
),
turns AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY r.ref_id, r.plan_row, r.index_match ORDER BY r.plan_bytes, r.plan_id
        ) AS plan_turn
    FROM ranked AS r
),
budgeted AS (
    SELECT
        t.*,
        SUM(CASE WHEN t.plan_row = 1 AND t.index_match = 1 AND t.plan_bytes <= ? THEN t.plan_bytes ELSE 0 END)
            OVER (ORDER BY t.plan_turn, t.plan_bytes, t.plan_id, t.plan_row ROWS UNBOUNDED PRECEDING)
            AS running_bytes
    FROM turns AS t
)
SELECT
    b.ref_id,
    b.plan_id,
    b.query_id,
    b.is_forced_plan,
    b.index_match,
    b.stats_match,
    b.plan_bytes,
    CASE
        WHEN b.plan_bytes <= ? AND b.running_bytes <= ?
        THEN CAST(p.query_plan AS nvarchar(max))
    END AS query_plan
FROM budgeted AS b
LEFT JOIN sys.query_store_plan AS p
    ON p.plan_id = b.plan_id
   AND b.plan_row = 1
   AND b.index_match = 1
ORDER BY b.plan_id, b.plan_row
"""


def workload_sql(objective: str, scope_pattern_count: int = 0, pinned_query_count: int = 0) -> str:
    """Parameters, in order: window end, window start, scope patterns, top
    queries, pinned query ids, plans per query."""

    column = _OBJECTIVE_SORT_COLUMNS[objective]
    # An unscoped review has no in_scope column: a constant there would be a
    # constant window ORDER BY expression, which SQL Server refuses (5309).
    in_scope = scope_join = scope_first = ""
    top_queries = "r.workload_rank <= ?"
    if scope_pattern_count:
        # A scoped review ranks only queries whose plans touch the scope, before
        # the top-queries cap; else a busy database pushes the scope's queries
        # out of it. Totals stay database-wide.
        matches = " OR ".join(
            ["CHARINDEX(?, CAST(sp.query_plan AS nvarchar(max))) > 0"] * scope_pattern_count
        )
        in_scope = (
            f",\n        MAX(CASE WHEN {matches} THEN 1 ELSE 0 END)"
            " OVER (PARTITION BY ps.query_id) AS in_scope"
        )
        scope_join = "\n    INNER JOIN sys.query_store_plan AS sp\n        ON sp.plan_id = ps.plan_id"
        scope_first = "w.in_scope DESC, "
        top_queries = "(r.in_scope = 1 AND r.workload_rank <= ?)"
    pinned = ""
    if pinned_query_count:
        # Given query ids are analysed whatever their rank or scope.
        pinned = " OR r.query_id IN (" + ", ".join(["?"] * pinned_query_count) + ")"
    return _WORKLOAD_SQL_TEMPLATE.format(
        objective=column,
        preview=QUERY_TEXT_PREVIEW_CHARS,
        in_scope=in_scope,
        scope_join=scope_join,
        scope_first=scope_first,
        top_queries=top_queries,
        pinned=pinned,
    )


def _showplan_name(name: str) -> str:
    # Showplan writes object names bracket-quoted inside XML attribute values.
    quoted = "[" + name.replace("]", "]]") + "]"
    return (
        quoted.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def _scope_plan_patterns(schema_name: str | None, table_names: list[str] | None) -> list[str]:
    """Plan XML Object attributes that mark a plan as touching the review scope."""

    if schema_name is None:
        return []
    schema = f'Schema="{_showplan_name(schema_name)}"'
    if not table_names:
        return [schema]
    return [f'{schema} Table="{_showplan_name(table)}"' for table in table_names]


def resolve_window(lookback_days: int, as_of_utc: str | None, *, now: datetime | None = None) -> tuple[datetime, datetime]:
    return window_bounds(lookback_days * 24 * 60, as_of_utc, now=now)


def _iso(value: datetime) -> str:
    return sql_utc(value)


def _norm(value: str) -> str:
    return value.casefold()


class WorkloadIndexAdvisor:
    def __init__(
        self,
        executor: AzureSqlExecutor,
        *,
        optional_read_timeout_seconds: float | None = None,
    ):
        self.executor = executor
        if optional_read_timeout_seconds is None:
            config = getattr(executor, "config", None)
            optional_read_timeout_seconds = float(
                getattr(config, "query_timeout_seconds", None) or OPTIONAL_READ_TIMEOUT_SECONDS
            )
        self.optional_read_timeout_seconds = optional_read_timeout_seconds

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
        query_ids: list[int] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if objective not in OBJECTIVES:
            raise ValueError(f"objective must be one of {sorted(OBJECTIVES)}")
        lookback_days = max(1, min(int(lookback_days), MAX_LOOKBACK_DAYS))
        top_queries = max(1, min(int(top_queries), MAX_TOP_QUERIES))
        plans_per_query = max(1, min(int(plans_per_query), MAX_PLANS_PER_QUERY))
        if table_names and not schema_name:
            raise ValueError("table_names requires schema_name")
        pinned_ids = list(dict.fromkeys(int(query_id) for query_id in query_ids or ()))
        if any(query_id <= 0 for query_id in pinned_ids):
            raise ValueError("query_ids must be positive Query Store query ids")
        if len(pinned_ids) > MAX_QUERY_IDS:
            raise ValueError(f"query_ids accepts at most {MAX_QUERY_IDS} ids")
        start, end = resolve_window(lookback_days, as_of_utc, now=now)

        # Every read of this review, nested ones too, goes through one limiter.
        reader = copy.copy(self)
        reader.executor = cast(AzureSqlExecutor, _LimitedReads(self.executor, MAX_CONCURRENT_READS))

        # Stage 1: reads that need nothing from each other. Gaps are kept per
        # read and joined in a fixed order, so the report does not depend on
        # which read ends first.
        workload_gaps: list[str] = []
        table_gaps: list[str] = []
        foreign_key_gaps: list[str] = []

        async def read_workload() -> tuple[dict[str, Any], bool, list[WorkloadQuery], dict[str, float]]:
            # The workload read needs the Query Store state, so the two stay in order.
            query_store, readable = await reader._query_store_options(database_name, workload_gaps)
            queries: list[WorkloadQuery] = []
            totals: dict[str, float] = {}
            if readable:
                try:
                    queries, totals = await reader._workload(
                        database_name,
                        objective,
                        start,
                        end,
                        top_queries,
                        plans_per_query,
                        _scope_plan_patterns(schema_name, table_names),
                        pinned_ids,
                    )
                except Exception as exc:
                    note_exception(exc, "workload_index_advisor.workload")
                    workload_gaps.append(
                        "Query Store workload could not be read: "
                        + sanitize_error_message(str(exc))
                    )
                    readable = False
                else:
                    analysed = {query.query_id for query in queries}
                    missing = [str(query_id) for query_id in pinned_ids if query_id not in analysed]
                    if missing:
                        workload_gaps.append(
                            f"Query id(s) {', '.join(missing)} have no Query Store runtime statistics "
                            "in the window, so they were not analysed"
                        )
            return query_store, readable, queries, totals

        async def read_existing() -> list[ExistingIndex] | Exception:
            try:
                return await collect_existing_indexes(reader.executor, database_name)
            except Exception as exc:
                note_exception(exc, "workload_index_advisor.existing_indexes")
                return exc

        (
            (query_store, query_store_readable, queries, totals),
            existing,
            tables,
            foreign_keys,
            readable_replicas,
        ) = await asyncio.gather(
            read_workload(),
            read_existing(),
            reader._tables(database_name, table_gaps),
            reader._foreign_keys(database_name, foreign_key_gaps),
            collect_readable_replica_status(
                reader.executor, database_name, site="workload_index_advisor"
            )
            if include_existing_index_review
            else _value(None),
        )
        gaps: list[str] = list(workload_gaps)
        if isinstance(existing, Exception):
            return {
                "database_name": database_name,
                "recommend_only": True,
                "recommendations": [],
                "gaps": gaps,
                **status_payload(
                    ResultStatus.UNAVAILABLE,
                    "Existing index metadata could not be read, so no index advice is safe: "
                    + sanitize_error_message(str(existing))
                    + ". The index catalog views need VIEW DEFINITION. "
                    + dmv_permission_hint("sys.dm_db_index_usage_stats"),
                ),
            }
        gaps.extend(table_gaps)
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

        detail_keys = [
            key
            for key in self._detail_tables(queries, tables, focus_tables, schema_name)
            if key in tables and tables[key].object_id is not None
        ]
        if len(detail_keys) > MAX_DETAIL_TABLES:
            skipped = [f"{tables[key].schema}.{tables[key].table}" for key in detail_keys[MAX_DETAIL_TABLES:]]
            shown = ", ".join(skipped[:10]) + (f" and {len(skipped) - 10} more" if len(skipped) > 10 else "")
            gaps.append(
                f"{len(skipped)} workload table(s) are past the {MAX_DETAIL_TABLES}-table detail limit "
                f"({shown}); their column metadata and statistics were not read, so advice for them "
                "did not check computed columns, LOB columns or key width, and key order uses "
                "workload frequency only"
            )
        detail_ids = [
            object_id
            for key in detail_keys[:MAX_DETAIL_TABLES]
            if (object_id := tables[key].object_id) is not None
        ]
        heap_ids = [
            object_id
            for info in tables.values()
            if info.is_heap and (object_id := info.object_id) is not None
        ][:MAX_DETAIL_TABLES]

        # Stage 2: reads that need the workload's tables.
        column_gaps: list[str] = []
        forwarded_gaps: list[str] = []
        selectivity_gaps: list[str] = []
        leaf_gaps: list[str] = []
        columns, _, selectivity, leaf_activity = await asyncio.gather(
            reader._columns(database_name, detail_ids, by_id, column_gaps),
            reader._forwarded_fetches(database_name, heap_ids, tables, by_id, forwarded_gaps),
            reader._selectivity(database_name, detail_ids, by_id, selectivity_gaps),
            reader._leaf_activity(database_name, detail_ids, by_id, leaf_gaps)
            if include_existing_index_review
            else _value(None),
        )
        gaps.extend(column_gaps)
        gaps.extend(_unusable_computed_column_gaps(columns, tables))
        gaps.extend(forwarded_gaps + selectivity_gaps + foreign_key_gaps + leaf_gaps)

        # Stage 3: optional dependency reads, each bounded; a timeout or failure
        # becomes a gap. They get a stage of their own so that no read waits
        # for a slot inside its timeout.
        reference_gaps: list[str] = []
        forced_gaps: list[str] = []
        hint_gaps: list[str] = []
        coverage_gaps: list[str] = []
        columnstore_gaps: list[str] = []
        pin_reads = include_existing_index_review and query_store_readable
        has_columnstore = any(index.index_type_code in COLUMNSTORE_TYPE_CODES for index in existing)
        # The reference read costs (index names) x (plan bytes), so check only
        # the indexes this review can report. The hint read keeps every index:
        # a hint that names an out-of-scope index must still resolve.
        scoped_existing = [
            index
            for index in existing
            if (schema_name is None or _norm(index.schema) == _norm(schema_name))
            and (focus_tables is None or (_norm(index.schema), _norm(index.table)) in focus_tables)
        ]
        (
            (references, statistics_references),
            (forced_accesses, forced_coverage),
            (hint_references, hint_coverage),
            coverage,
            columnstore_health,
        ) = await asyncio.gather(
            self._bounded(
                reader._index_references(database_name, scoped_existing, start, reference_gaps)
                if pin_reads
                else _value(({}, {})),
                fallback=({}, {}),
                gaps=reference_gaps,
                what="Query Store plan reference",
                consequence="unused-index findings stay low confidence",
                site="workload_index_advisor.index_references",
            ),
            self._bounded(
                reader._forced_plans(database_name, forced_gaps)
                if pin_reads
                else _value(([], "not_checked")),
                fallback=([], "incomplete"),
                gaps=forced_gaps,
                what="forced Query Store plan",
                consequence=_REMOVALS_CAPPED,
                site="workload_index_advisor.forced_plans",
            ),
            self._bounded(
                reader._index_hints(database_name, existing, hint_gaps)
                if include_existing_index_review
                else _value(({}, "not_checked")),
                fallback=({}, "incomplete"),
                gaps=hint_gaps,
                what="index hint",
                consequence=_REMOVALS_CAPPED,
                site="workload_index_advisor.index_hints",
            ),
            self._bounded(
                reader._query_store_coverage(database_name, start, end, coverage_gaps)
                if query_store_readable
                else _value(None),
                fallback=None,
                gaps=coverage_gaps,
                what="Query Store window coverage",
                consequence="per-day rates use the requested window and removals are capped at medium confidence",
                site="workload_index_advisor.query_store_coverage",
            ),
            self._bounded(
                reader._columnstore_health(database_name, columnstore_gaps)
                if include_existing_index_review and has_columnstore
                else _value(None),
                fallback=None,
                gaps=columnstore_gaps,
                what="columnstore row-group health",
                consequence="columnstore maintenance leads are missing",
                site="workload_index_advisor.columnstore_health",
            ),
        )
        gaps.extend(reference_gaps + forced_gaps + hint_gaps + coverage_gaps + columnstore_gaps)
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
            index_statistics_references=statistics_references,
            query_store_coverage=coverage,
            forced_plan_accesses=forced_accesses,
            index_hint_references=hint_references,
            forced_plan_coverage=forced_coverage,
            hint_coverage=hint_coverage,
            readable_replicas=readable_replicas,
            columnstore_health=columnstore_health,
            index_leaf_activity=leaf_activity,
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
            note_exception(exc, "workload_index_advisor.query_store_options")
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
            "size_based_cleanup_mode": row.get("size_based_cleanup_mode_desc"),
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
        scope_patterns: list[str] | None = None,
        query_ids: list[int] | None = None,
    ) -> tuple[list[WorkloadQuery], dict[str, float]]:
        patterns = list(scope_patterns or ())
        pinned = list(query_ids or ())
        rows = await self.executor.fetch_all(
            database_name,
            workload_sql(objective, len(patterns), len(pinned)),
            params=[_iso(end), _iso(start), *patterns, top_queries, *pinned, plans_per_query],
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
            note_exception(exc, "workload_index_advisor.tables")
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
            note_exception(exc, "workload_index_advisor.columns")
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
                is_indexable=_bool_or_none(row.get("is_indexable")),
                is_deterministic=_bool_or_none(row.get("is_deterministic")),
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
            note_exception(exc, "workload_index_advisor.operational_stats")
            gaps.append("Heap forwarded-record counters could not be read: " + sanitize_error_message(str(exc)))
            return
        for row in rows:
            key = _lookup(by_id, row.get("object_id"))
            if key is not None and key in tables:
                tables[key].forwarded_fetches = _int_or_none(row.get("forwarded_fetches"))

    async def _leaf_activity(
        self,
        database_name: str,
        object_ids: list[int],
        by_id: dict[int, tuple[str, str]],
        gaps: list[str],
    ) -> dict[tuple[str, str, str], dict[str, int]] | None:
        """Leaf inserts and leaf page allocations per index: the page-split evidence."""

        ids = _id_list(object_ids)
        if not ids:
            return {}
        try:
            rows = await self.executor.fetch_all(
                database_name, _LEAF_ACTIVITY_SQL_TEMPLATE.format(ids=ids)
            )
        except Exception as exc:
            note_exception(exc, "workload_index_advisor.leaf_activity")
            gaps.append(
                "Leaf page counters could not be read: "
                + sanitize_error_message(str(exc))
                + "; fill factor was not reviewed"
            )
            return None
        activity: dict[tuple[str, str, str], dict[str, int]] = {}
        for row in rows:
            key = _lookup(by_id, row.get("object_id"))
            name = row.get("index_name")
            if key is None or not name:
                continue
            activity[(key[0], key[1], _norm(str(name)))] = {
                "leaf_insert_count": int(_float(row.get("leaf_insert_count"))),
                "leaf_allocation_count": int(_float(row.get("leaf_allocation_count"))),
            }
        return activity

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
            note_exception(exc, "workload_index_advisor.selectivity")
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
            rows = await self.executor.fetch_all(
                database_name, FOREIGN_KEYS_SQL, params=[MAX_FOREIGN_KEY_ROWS + 1]
            )
        except Exception as exc:
            note_exception(exc, "workload_index_advisor.foreign_keys")
            gaps.append("Foreign keys could not be read: " + sanitize_error_message(str(exc)))
            return []
        if len(rows) > MAX_FOREIGN_KEY_ROWS:
            # The key that continues past the cap was read only in part.
            cut = rows[MAX_FOREIGN_KEY_ROWS].get("foreign_key_id")
            rows = [row for row in rows[:MAX_FOREIGN_KEY_ROWS] if row.get("foreign_key_id") != cut]
            gaps.append(
                f"More than {MAX_FOREIGN_KEY_ROWS} foreign key column rows exist; foreign keys past "
                "that cap were not checked for a supporting index"
            )
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

    async def _bounded(
        self,
        read: Awaitable[_T],
        *,
        fallback: _T,
        gaps: list[str],
        what: str,
        consequence: str,
        site: str,
    ) -> _T:
        try:
            return await asyncio.wait_for(read, timeout=self.optional_read_timeout_seconds)
        except asyncio.TimeoutError as exc:
            note_exception(exc, site)
            gaps.append(
                f"The {what} read timed out after {self.optional_read_timeout_seconds:g} s; {consequence}"
            )
            return fallback

    async def _index_references(
        self,
        database_name: str,
        existing: list[ExistingIndex],
        start: datetime,
        gaps: list[str],
    ) -> tuple[dict[tuple[str, str, str], int | None], dict[tuple[str, str, str], int | None]]:
        """Query Store plans in the window that read (or force a plan on), or read the statistics of, each unused index."""

        unused = [
            index
            for index in existing
            if index.index_type_code == 2
            and not index.is_disabled
            and not index.is_hypothetical
            and sum(int(index.usage.get(m) or 0) for m in ("user_seeks", "user_scans", "user_lookups")) == 0
        ]
        if not unused:
            return {}, {}
        checked = unused[:MAX_REFERENCE_CHECKS]
        if len(unused) > len(checked):
            gaps.append(
                f"Query Store references were checked for {len(checked)} of {len(unused)} unused indexes"
            )
        values = ", ".join("(?, ?, ?)" for _ in checked)
        params: list[Any] = []
        for position, index in enumerate(checked):
            quoted = "[" + index.name.replace("]", "]]") + ']"'
            params.extend([position, 'Index="' + quoted, 'Statistics="' + quoted])
        params.extend(
            [_iso(start), MAX_FORCED_PLAN_XML_BYTES, MAX_FORCED_PLAN_XML_BYTES, MAX_INDEX_REFERENCE_PLAN_TOTAL_BYTES]
        )
        keys = [(_norm(i.schema), _norm(i.table), _norm(i.name)) for i in checked]
        try:
            rows = await self.executor.fetch_all(
                database_name, _INDEX_REFERENCE_SQL_TEMPLATE.format(values=values), params=params
            )
        except Exception as exc:
            note_exception(exc, "workload_index_advisor.index_references")
            gaps.append(
                "Query Store index references could not be checked: " + sanitize_error_message(str(exc))
            )
            return dict.fromkeys(keys), dict.fromkeys(keys)
        index_plans: dict[int, set[int]] = {}
        statistics: dict[int, int] = {}
        plan_xml: dict[int, str] = {}
        query_ids: dict[int, int] = {}
        forced: set[int] = set()
        within_plan_cap: set[int] = set()
        for row in rows:
            position = int(_float(row.get("ref_id")))
            plan_id = int(_float(row.get("plan_id")))
            query_ids[plan_id] = int(_float(row.get("query_id")))
            if row.get("query_plan"):
                plan_xml[plan_id] = str(row["query_plan"])
            if row.get("plan_bytes") is not None and _float(row["plan_bytes"]) <= MAX_FORCED_PLAN_XML_BYTES:
                within_plan_cap.add(plan_id)
            if _float(row.get("is_forced_plan")):
                forced.add(plan_id)
            if _float(row.get("index_match")):
                index_plans.setdefault(position, set()).add(plan_id)
            if _float(row.get("stats_match")):
                statistics[position] = statistics.get(position, 0) + 1
        # Same rule as review_index_portfolio: a plan is use when it reads the
        # index, or is forced (a drop silently stops the forcing). A plan whose
        # XML was not returned or not fully parsed stays use for every index it
        # does not name in full. Parsing runs off the event loop and stops when
        # the bounded read is cancelled.
        stop = threading.Event()
        try:
            plan_reads = await asyncio.to_thread(
                _plan_reads, sorted(set().union(*index_plans.values())), plan_xml, query_ids, stop
            )
        finally:
            stop.set()
        unread: set[int] = set()
        plans: dict[int, int] = {}
        for position, plan_ids in index_plans.items():
            if position >= len(keys):
                continue
            for plan_id in plan_ids:
                entry = plan_reads[plan_id]
                unknown = entry is None or (entry[1] is not None and keys[position] not in entry[1])
                if unknown and plan_id not in forced:
                    unread.add(plan_id)
                if plan_id in forced or unknown or (entry is not None and keys[position] in entry[0]):
                    plans[position] = plans.get(position, 0) + 1
        if unread:
            gap = (
                f"{len(unread)} Query Store plan(s) that name an unused index could not be read "
                "(too large or not fully parsed); they count as references, so those indexes stay kept"
            )
            # A plan under the per-plan cap with no XML was cut by the total budget.
            if any(plan_id in within_plan_cap and plan_id not in plan_xml for plan_id in unread):
                gap += (
                    f"; the {MAX_INDEX_REFERENCE_PLAN_TOTAL_BYTES // 1_000_000} MB plan read budget "
                    "was reached, so later plans were not read"
                )
            gaps.append(gap)
        return (
            {key: plans.get(position, 0) for position, key in enumerate(keys)},
            {key: statistics.get(position, 0) for position, key in enumerate(keys)},
        )

    async def _forced_plans(
        self, database_name: str, gaps: list[str]
    ) -> tuple[list[tuple[int, int, str, str, str]], str]:
        """Every index each forced Query Store plan reads, and whether all plans were read."""

        try:
            rows = await self.executor.fetch_all(
                database_name,
                FORCED_PLANS_SQL,
                params=[
                    MAX_FORCED_PLANS + 1,
                    MAX_FORCED_PLAN_XML_BYTES,
                    MAX_FORCED_PLAN_XML_BYTES,
                    MAX_FORCED_PLAN_TOTAL_BYTES,
                ],
            )
        except Exception as exc:
            note_exception(exc, "workload_index_advisor.forced_plans")
            gaps.append(
                "Forced Query Store plans could not be read: "
                + sanitize_error_message(str(exc))
                + f"; {_REMOVALS_CAPPED}"
            )
            return [], "incomplete"
        coverage = "complete"
        if len(rows) > MAX_FORCED_PLANS:
            rows = rows[:MAX_FORCED_PLANS]
            coverage = "incomplete"
            gaps.append(
                f"More than {MAX_FORCED_PLANS} forced Query Store plans exist and only "
                f"{MAX_FORCED_PLANS} were checked for index dependencies; {_REMOVALS_CAPPED}"
            )
        accesses: list[tuple[int, int, str, str, str]] = []
        unread = 0
        for row in rows:
            plan_xml = row.get("query_plan")
            plan = parse_plan_access(str(plan_xml)) if plan_xml else None
            if plan is None or plan.parse_error:
                unread += 1
                continue
            query_id = int(_float(row.get("query_id")))
            plan_id = int(_float(row.get("plan_id")))
            accesses.extend(
                (query_id, plan_id, access.schema, access.table, access.index_name)
                for access in plan.accesses
                if access.index_name
            )
        if unread:
            coverage = "incomplete"
            gaps.append(
                f"{unread} forced Query Store plan(s) were too large to read or could not be "
                f"parsed; {_REMOVALS_CAPPED}"
            )
        return accesses, coverage

    async def _index_hints(
        self, database_name: str, existing: list[ExistingIndex], gaps: list[str]
    ) -> tuple[dict[tuple[str, str, str], list[str]], str]:
        """INDEX and FORCESEEK hints in query text, Query Store hints, plan guides and modules."""

        identities = [
            {
                "object_id": index.object_id,
                "index_id": index.index_id,
                "schema": index.schema,
                "table": index.table,
                "index_name": index.name,
            }
            for index in existing
            # A heap (index_id 0) cannot be named in an index hint.
            if index.index_id > 0 and not index.is_hypothetical
        ]
        try:
            result = await QueryStoreService(self.executor).get_index_hint_coverage(
                database_name, index_identities=identities, limit=MAX_HINT_SOURCE_ROWS
            )
        except Exception as exc:
            note_exception(exc, "workload_index_advisor.index_hints")
            gaps.append(
                "Index hints could not be checked: " + sanitize_error_message(str(exc)) + f"; {_REMOVALS_CAPPED}"
            )
            return {}, "incomplete"
        references: dict[tuple[str, str, str], list[str]] = {}
        for evidence in result.get("evidence") or []:
            place = _hint_place(str(evidence.get("source") or ""), evidence.get("source_id") or {})
            for resolved in evidence.get("resolved_indexes") or []:
                key = (
                    _norm(str(resolved.get("schema") or "")),
                    _norm(str(resolved.get("table") or "")),
                    _norm(str(resolved.get("index_name") or "")),
                )
                places = references.setdefault(key, [])
                if place not in places:
                    places.append(place)
        coverage = result.get("coverage") or {}
        unresolved = [
            _HINT_SOURCE_NAMES.get(str(source), str(source))
            for source, source_coverage in (coverage.get("sources") or {}).items()
            if _UNRESOLVED_HINT_BLOCKERS & set(source_coverage.get("blockers") or [])
        ]
        if unresolved:
            gaps.append(
                f"Index hints in {', '.join(unresolved)} match no single index here: the name "
                "differs in case from the index name, is on more than one table without a "
                "TABLE HINT target, or names no index in this database. Any of them may name an "
                "index advised for removal, so unused and redundant index removals have no DDL"
            )
        if coverage.get("status") == "complete":
            return references, "complete"
        all_blockers = [str(blocker) for blocker in coverage.get("blockers") or []]
        blockers = [
            blocker
            for blocker in all_blockers
            if blocker not in _UNRESOLVED_HINT_BLOCKERS and blocker != _HINT_NAMES_NO_INDEX
        ]
        if _HINT_NAMES_NO_INDEX in all_blockers:
            gaps.append(
                "Some index hints name no index in this database (a temp table, another database, "
                "or an index that no longer exists). Removing an index here cannot affect them, "
                "so they do not limit this review"
            )
            if not blockers and not unresolved:
                return references, "complete"
        if blockers or not unresolved:
            gaps.append(
                "Index hints in query text, Query Store hints, plan guides and modules were not fully checked"
                + (f" ({', '.join(blockers[:5])})" if blockers else "")
                + f"; {_REMOVALS_CAPPED}"
            )
        return references, "unresolved" if unresolved else "incomplete"

    async def _query_store_coverage(
        self, database_name: str, start: datetime, end: datetime, gaps: list[str]
    ) -> dict[str, Any] | None:
        try:
            rows = await self.executor.fetch_all(
                database_name, QUERY_STORE_COVERAGE_SQL, params=[_iso(end), _iso(start)]
            )
        except Exception as exc:
            note_exception(exc, "workload_index_advisor.query_store_coverage")
            gaps.append(
                "Query Store window coverage could not be read: "
                + sanitize_error_message(str(exc))
                + "; per-day rates use the requested window and removals are capped at medium confidence"
            )
            return None
        row = rows[0] if rows else {}
        return {
            "effective_start_utc": row.get("effective_start_utc"),
            "effective_end_utc": row.get("effective_end_utc"),
            "interval_count": int(_float(row.get("interval_count"))),
            "oldest_interval_start_utc": row.get("oldest_interval_start_utc"),
        }

    async def _columnstore_health(
        self, database_name: str, gaps: list[str]
    ) -> list[dict[str, Any]] | None:
        """Row-group health of every columnstore index, or None with a gap."""

        rows, blocked = await read_dmv(
            self.executor,
            database_name,
            ROW_GROUP_DMV,
            ROW_GROUP_SQL.format(object_filter=""),
            site="workload_index_advisor.columnstore_health",
        )
        if blocked is not None:
            gaps.append(
                "columnstore row-group health could not be read ("
                + str(blocked.get(RESULT_STATUS_REASON_KEY) or "unavailable")
                + "); columnstore maintenance leads are missing"
            )
            return None
        return summarize_columnstore_indexes(rows)

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


_IndexKey = tuple[str, str, str]


def _plan_reads(
    plan_ids: list[int], plan_xml: dict[int, str], query_ids: dict[int, int], stop: threading.Event
) -> dict[int, tuple[set[_IndexKey], set[_IndexKey] | None] | None]:
    """Per plan: the indexes it reads, and the indexes it names in full when an
    Object had no table identity (a table variable); None when not parsed or
    past the parser's reference cap."""

    plan_reads: dict[int, tuple[set[_IndexKey], set[_IndexKey] | None] | None] = {}
    for plan_id in plan_ids:
        if stop.is_set():
            break
        xml = plan_xml.get(plan_id)
        evidence = (
            parse_showplan_index_evidence(xml, query_id=max(query_ids[plan_id], 1), plan_id=max(plan_id, 1))
            if xml
            else None
        )
        if (
            evidence is None
            or not evidence["coverage"]["scanned"]
            or "index_reference_cap_reached" in evidence["coverage"]["blockers"]
        ):
            plan_reads[plan_id] = None
            continue
        keyed = [
            ((_norm(ref["schema_name"]), _norm(ref["object_name"]), _norm(ref["index_name"])), ref)
            for ref in evidence["index_references"]
        ]
        plan_reads[plan_id] = (
            {key for key, ref in keyed if _reference_is_read(ref)},
            {key for key, _ in keyed} if "index_reference_identity_incomplete" in evidence["coverage"]["blockers"] else None,
        )
    return plan_reads


async def _value(value: _T) -> _T:
    return value


class _LimitedReads:
    """An executor view whose reads share ``limit`` slots."""

    def __init__(self, executor: AzureSqlExecutor, limit: int) -> None:
        self._executor = executor
        self._slots = asyncio.Semaphore(limit)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._executor, name)

    async def fetch_all(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        async with self._slots:
            return await self._executor.fetch_all(*args, **kwargs)


def _hint_place(source: str, source_id: dict[str, Any]) -> str:
    if source == "query_store_text":
        return f"the text of query {source_id.get('query_id')}"
    if source == "query_store_query_hints":
        return f"the Query Store hint on query {source_id.get('query_id')}"
    if source == "plan_guides":
        return f"plan guide {source_id.get('plan_guide_id')}"
    if source == "module_definitions":
        return f"module object_id {source_id.get('object_id')}"
    return source or "an unknown hint source"


def _unusable_computed_column_gaps(
    columns: dict[tuple[str, str], dict[str, ColumnInfo]],
    tables: dict[tuple[str, str], TableInfo],
) -> list[str]:
    """Computed columns that COLUMNPROPERTY rules out; unread flags are the advisor's own gap."""

    unusable: list[str] = []
    for key, table_columns in columns.items():
        info = tables.get(key)
        prefix = f"{info.schema}.{info.table}" if info else ".".join(key)
        for column in table_columns.values():
            if not column.is_computed or column.is_indexable is None or column.is_deterministic is None:
                continue
            if not column.is_deterministic:
                unusable.append(f"{prefix}.{column.name} (not deterministic)")
            elif not column.is_indexable:
                unusable.append(f"{prefix}.{column.name} (not indexable)")
    if not unusable:
        return []
    unusable.sort()
    shown = ", ".join(unusable[:10]) + (f" and {len(unusable) - 10} more" if len(unusable) > 10 else "")
    return [
        f"Computed column(s) {shown} cannot be index keys, and non-deterministic ones cannot be "
        "included columns; index advice that needs them in those roles is withheld"
    ]


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


def _bool_or_none(value: Any) -> bool | None:
    return None if value is None else bool(value)
