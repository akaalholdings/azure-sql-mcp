from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from azure_sql_mcp.diagnostics import DiagnosticQueryService
from tests.azure_dmv_contract import StrictDmvExecutor


class RecordingExecutor:
    def __init__(self, errors: Sequence[tuple[str, Exception]] = ()) -> None:
        self.errors = list(errors)
        self.calls: list[tuple[str, str, tuple[Any, ...] | None]] = []

    async def fetch_all(
        self,
        database_name: str,
        query: str,
        params: Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((database_name, query, tuple(params) if params is not None else None))
        for needle, error in self.errors:
            if needle in query:
                raise error
        return []


def _projected_names(select: Any) -> list[str]:
    return [projection.alias_or_name for projection in select.expressions]


def _assert_plan_built_only_for_top_rows(query: str, *, limit: int, filter_sql: str) -> Any:
    """TOP, the database filter and the sort sit in a CTE; the plan TVF is outside it."""

    statement = sqlglot.parse_one(query, read="tsql")
    (cte,) = statement.find_all(exp.CTE)
    staged = cte.this
    staged_sql = staged.sql(dialect="tsql")
    assert staged.args["limit"].expression.sql(dialect="tsql") == str(limit)
    assert f"SELECT TOP ({limit})" in " ".join(query.split())
    assert "dm_exec_query_plan" not in staged_sql
    assert "query_plan" not in _projected_names(staged)
    assert filter_sql in staged.args["where"].sql(dialect="tsql")
    assert staged.args.get("order") is not None
    # The outer statement adds no second cut, builds the plan, and sorts again:
    # a CTE's ORDER BY does not order the final result.
    assert statement.args.get("limit") is None
    assert "OUTER APPLY sys.dm_exec_query_plan(" in " ".join(query.split())
    assert statement.args["order"].sql(dialect="tsql").endswith("sort_key DESC")
    return statement


CACHED_QUERY_COLUMNS = [
    "query_hash",
    "query_plan_hash",
    "execution_count",
    "total_worker_time",
    "avg_worker_time",
    "total_elapsed_time",
    "avg_elapsed_time",
    "total_logical_reads",
    "avg_logical_reads",
    "total_physical_reads",
    "total_logical_writes",
    "creation_time",
    "last_execution_time",
    "has_missing_index",
    "query_text_preview",
]
PROCEDURE_COLUMNS = [
    "schema_name",
    "routine_name",
    "routine_type",
    "execution_count",
    "total_worker_time",
    "avg_worker_time",
    "total_elapsed_time",
    "avg_elapsed_time",
    "total_logical_reads",
    "avg_logical_reads",
    "total_physical_reads",
    "total_logical_writes",
    "calls_per_minute",
    "cached_time",
    "last_execution_time",
    "has_missing_index",
]
FUNCTION_COLUMNS = [column for column in PROCEDURE_COLUMNS if column != "calls_per_minute"]


@pytest.mark.parametrize(
    ("sort_by", "sort_sql"),
    [
        ("total_worker_time", "qs.total_worker_time"),
        ("avg_elapsed_time", "qs.total_elapsed_time / NULLIF(qs.execution_count, 0)"),
    ],
)
@pytest.mark.asyncio
async def test_top_cached_queries_build_plan_xml_for_the_top_rows_only(
    sort_by: str, sort_sql: str
) -> None:
    # An ad-hoc OLTP plan cache holds tens of thousands of statements. With
    # TOP only in the outer SELECT, dm_exec_query_plan built showplan XML for
    # every one of them to return 25 rows, on a database already under load.
    executor = RecordingExecutor()

    await DiagnosticQueryService(executor).get_top_cached_queries(  # type: ignore[arg-type]
        "appdb", sort_by=sort_by, limit=25
    )

    query = executor.calls[0][1]
    statement = _assert_plan_built_only_for_top_rows(
        query, limit=25, filter_sql="st.dbid IS NULL OR st.dbid = DB_ID()"
    )
    (cte,) = statement.find_all(exp.CTE)
    assert "CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st" in " ".join(query.split())
    assert cte.this.args["order"].sql(dialect="tsql") == f"ORDER BY {sort_sql} DESC"
    assert f"{sort_sql} AS sort_key" in " ".join(query.split())
    assert _projected_names(statement) == CACHED_QUERY_COLUMNS


@pytest.mark.asyncio
async def test_cached_routine_stats_build_plan_xml_for_the_top_rows_only() -> None:
    executor = RecordingExecutor()

    await DiagnosticQueryService(executor).get_cached_routine_stats(  # type: ignore[arg-type]
        "appdb", routine_type="all", sort_by="avg_worker_time", limit=10
    )

    procedures, functions = (call[1] for call in executor.calls)
    statement = _assert_plan_built_only_for_top_rows(
        procedures, limit=10, filter_sql="qs.database_id = DB_ID()"
    )
    assert "sys.dm_exec_procedure_stats" in procedures
    assert _projected_names(statement) == PROCEDURE_COLUMNS
    statement = _assert_plan_built_only_for_top_rows(
        functions, limit=10, filter_sql="fs.database_id = DB_ID()"
    )
    assert "sys.dm_exec_function_stats" in functions
    assert _projected_names(statement) == FUNCTION_COLUMNS


@pytest.mark.asyncio
async def test_cached_query_reads_are_valid_tsql_on_documented_columns() -> None:
    executor = StrictDmvExecutor()
    service = DiagnosticQueryService(executor)  # type: ignore[arg-type]

    queries = await service.get_top_cached_queries("appdb")
    routines = await service.get_cached_routine_stats("appdb")

    assert executor.violations == []
    assert queries["cached_queries"]["available"] is True
    assert routines["coverage"]["status"] == "complete"


@pytest.mark.asyncio
async def test_object_index_diagnostics_skip_the_buffer_pool_walk_by_default() -> None:
    # sys.dm_os_buffer_descriptors has no predicate pushdown: one table's
    # diagnostics walked every buffer of the instance, tens of millions of rows
    # on large tiers, and could time the section out during an incident.
    executor = RecordingExecutor(
        errors=[("sys.dm_os_buffer_descriptors", AssertionError("buffer pool was read"))]
    )

    result = await DiagnosticQueryService(executor).get_object_index_diagnostics(  # type: ignore[arg-type]
        "appdb", schema_name="dbo", table_name="Orders"
    )

    assert all("dm_os_buffer_descriptors" not in call[1] for call in executor.calls)
    assert len(executor.calls) == 8
    buffer_usage = result["buffer_usage"]
    assert buffer_usage["available"] is False
    assert buffer_usage["skipped"] is True
    assert "include_buffer_usage" in buffer_usage["reason"]
    assert "error" not in buffer_usage
    # A skipped section is not a failed one, and not a complete read either.
    assert result["coverage"]["skipped_sections"] == ["buffer_usage"]
    assert result["coverage"]["unavailable_sections"] == []
    assert result["coverage"]["status"] == "partial"


@pytest.mark.asyncio
async def test_object_index_diagnostics_read_buffer_usage_on_request() -> None:
    executor = RecordingExecutor()

    result = await DiagnosticQueryService(executor).get_object_index_diagnostics(  # type: ignore[arg-type]
        "appdb", schema_name="dbo", table_name="Orders", include_buffer_usage=True
    )

    (buffer_query,) = [call for call in executor.calls if "sys.dm_os_buffer_descriptors" in call[1]]
    assert "COUNT(*) * 8.0 / 1024 AS buffer_mb" in buffer_query[1]
    assert "bd.database_id = DB_ID()" in buffer_query[1]
    assert buffer_query[2] == ("dbo", "dbo", "Orders", "Orders")
    assert len(executor.calls) == 9
    assert result["buffer_usage"]["available"] is True
    assert result["coverage"]["status"] == "complete"
