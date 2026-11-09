"""Server cost of review_workload_indexes: one-pass workload SQL, binary plan
reference check, staged concurrent reads, and pinned query ids."""

from __future__ import annotations

from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from azure_sql_mcp import workload_index_advisor as module
from azure_sql_mcp.workload_index_advisor import _INDEX_REFERENCE_SQL_TEMPLATE
from azure_sql_mcp.workload_index_advisor import WorkloadIndexAdvisor
from azure_sql_mcp.workload_index_advisor import workload_sql
from tests.unit.test_workload_index_advisor import NOW
from tests.unit.test_workload_index_advisor import RoutingExecutor
from tests.unit.test_workload_index_advisor import _workload_row
from tests.unit.test_workload_index_advisor import existing  # noqa: F401  (fixture)

WORKLOAD_COLUMNS = [
    "query_id",
    "plan_id",
    "plan_rank",
    "workload_rank",
    "executions",
    "total_cpu_us",
    "total_duration_us",
    "total_logical_reads",
    "total_logical_writes",
    "total_rowcount",
    "active_days",
    "workload_query_count",
    "workload_executions",
    "workload_cpu_us",
    "workload_duration_us",
    "workload_logical_reads",
    "object_schema",
    "object_name",
    "query_text_preview",
    "is_forced_plan",
    "query_plan",
]


def _cte_references(sql: str) -> dict[str, int]:
    tree = sqlglot.parse_one(sql, read="tsql")
    counts = {cte.alias: 0 for cte in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        if table.name in counts and not table.db:
            counts[table.name] += 1
    return counts


# ---------------------------------------------------------------- #63


@pytest.mark.parametrize("objective", ["cpu", "duration", "logical_reads", "executions"])
@pytest.mark.parametrize(("scope_patterns", "pinned"), [(0, 0), (2, 0), (0, 3), (2, 3)])
def test_workload_sql_reads_the_runtime_aggregate_once(objective: str, scope_patterns: int, pinned: int) -> None:
    # SQL Server inlines a CTE at every reference. The Query Store runtime
    # aggregate is the heaviest part of the read, so each CTE built on it is
    # referenced once; a second reference runs the aggregate again.
    counts = _cte_references(workload_sql(objective, scope_patterns, pinned))

    assert counts["plan_stats"] == 1
    assert all(count == 1 for count in counts.values()), counts


def test_workload_sql_keeps_its_result_columns_and_order() -> None:
    sql = workload_sql("cpu", 1, 2)
    tree = sqlglot.parse_one(sql, read="tsql")

    assert [projection.alias_or_name for projection in tree.selects] == WORKLOAD_COLUMNS
    assert sql.rstrip().endswith("ORDER BY r.workload_rank, r.plan_rank")
    # Ties rank the same way as before: objective, then query id; plans by
    # objective, then plan id.
    assert "ORDER BY w.in_scope DESC, w.query_objective DESC, w.query_id" in sql
    assert "PARTITION BY ps.query_id ORDER BY ps.total_cpu_us DESC, ps.plan_id" in sql


def test_workload_totals_cover_every_query_not_only_the_ranked_ones() -> None:
    # analyzed_share_pct divides the analysed queries by these totals.
    sql = workload_sql("cpu", 2)

    for total in ("executions", "total_cpu_us", "total_duration_us", "total_logical_reads"):
        assert f"SUM(ps.{total}) OVER ()" in sql
    assert (
        "SUM(CASE WHEN w.plan_rank = 1 THEN CAST(1 AS bigint) ELSE 0 END) OVER () AS workload_query_count" in sql
    )


def test_unscoped_workload_sql_has_no_constant_in_a_window_order() -> None:
    # SQL Server refuses a constant ORDER BY expression in a window (5309), so
    # the scope column exists only when a scope is given.
    unscoped = workload_sql("cpu", 0, 2)

    assert "in_scope" not in unscoped
    assert "ORDER BY w.query_objective DESC, w.query_id" in unscoped
    assert "WHERE (r.workload_rank <= ? OR r.query_id IN (?, ?))" in unscoped
    assert "WHERE (r.workload_rank <= ?)\n  AND r.plan_rank <= ?" in workload_sql("cpu")


# ---------------------------------------------------------------- #64


def test_reference_sql_scans_each_plan_once_with_a_binary_comparison() -> None:
    sql = _INDEX_REFERENCE_SQL_TEMPLATE.format(values="(?, ?, ?), (?, ?, ?)")

    # One CHARINDEX per pattern kind. The old shape ran each one twice (join
    # predicate, then the CASE), over the non-binary column collation.
    assert sql.count("CHARINDEX(") == 2
    binary = "COLLATE Latin1_General_100_BIN2"
    assert f"CHARINDEX(v.index_pattern {binary}, x.plan_xml {binary})" in sql
    assert f"CHARINDEX(v.stats_pattern {binary}, x.plan_xml {binary})" in sql
    # The patterns are applied to each plan row, so the plan is the outer side.
    matches = sql[sql.index("matches AS (") : sql.index("ranked AS (")]
    assert matches.index("FROM sys.query_store_plan AS p") < matches.index("CROSS APPLY (")
    assert matches.index("CROSS APPLY (") < matches.index("FROM refs AS v")
    assert _cte_references(sql) == {"refs": 1, "matches": 1, "ranked": 1, "turns": 1, "budgeted": 1}
    # The pattern parameters stay first, then the window start and the byte caps.
    assert sql.index("FROM (VALUES") < sql.index("TODATETIMEOFFSET(CAST(? AS datetime2), 0)")


def test_reference_sql_keeps_its_result_columns() -> None:
    tree = sqlglot.parse_one(_INDEX_REFERENCE_SQL_TEMPLATE.format(values="(?, ?, ?)"), read="tsql")

    assert [projection.alias_or_name for projection in tree.selects] == [
        "ref_id",
        "plan_id",
        "query_id",
        "is_forced_plan",
        "index_match",
        "stats_match",
        "plan_bytes",
        "query_plan",
    ]


# ---------------------------------------------------------------- #62


def _span(executor: RoutingExecutor, key: str) -> tuple[float, float]:
    return next((started, ended) for name, started, ended in executor.spans if name == key)


def _overlap(executor: RoutingExecutor, first: str, second: str) -> bool:
    first_start, first_end = _span(executor, first)
    second_start, second_end = _span(executor, second)
    return first_start < second_end and second_start < first_end


@pytest.mark.asyncio
async def test_independent_reads_overlap_and_dependent_reads_wait() -> None:
    # Wall clock was the sum of about ten reads. Reads that need nothing from
    # each other now share a stage; a read that needs an earlier result waits.
    executor = RoutingExecutor()
    executor.delays = {
        "query_store": 0.06,
        "workload": 0.06,
        "tables": 0.02,
        "foreign_keys": 0.02,
        "index_metadata": 0.06,
        "columns": 0.04,
        "selectivity": 0.04,
    }

    report = await WorkloadIndexAdvisor(executor).review("appdb", lookback_days=7, now=NOW)

    assert report["result_status"] == "ok"
    assert _overlap(executor, "tables", "query_store")
    assert _overlap(executor, "foreign_keys", "query_store")
    assert _overlap(executor, "index_metadata", "query_store")
    assert _overlap(executor, "columns", "selectivity")
    # The workload read needs the Query Store state.
    assert _span(executor, "workload")[0] >= _span(executor, "query_store")[1]
    # Column and statistics reads need the tables the workload touches.
    for dependent in ("columns", "selectivity"):
        assert _span(executor, dependent)[0] >= _span(executor, "workload")[1]
        assert _span(executor, dependent)[0] >= _span(executor, "tables")[1]
    # Pin reads need to know that the workload read worked.
    assert _span(executor, "forced_plans")[0] >= _span(executor, "workload")[1]


@pytest.mark.asyncio
async def test_review_runs_at_most_four_reads_at_once() -> None:
    # The pool default is 5 connections. Seven reads are ready in the first
    # stage (the index metadata read is three); a review must not take them all.
    executor = RoutingExecutor()
    executor.delays = dict.fromkeys(
        ("query_store", "tables", "foreign_keys", "index_metadata", "engine_start", "protection", "edition"), 0.03
    )

    await WorkloadIndexAdvisor(executor).review("appdb", lookback_days=7, now=NOW)

    assert executor.peak_in_flight == module.MAX_CONCURRENT_READS == 4


@pytest.mark.asyncio
async def test_failed_optional_reads_keep_their_gaps_and_gap_order(existing) -> None:  # noqa: F811
    # Each read is slower the earlier its gap is listed, so the gaps are
    # appended out of order unless they are kept per read.
    executor = RoutingExecutor(
        tables=PermissionError("tables denied"),
        foreign_keys=PermissionError("fk denied"),
        forced_plans=PermissionError("forced denied"),
        coverage=PermissionError("coverage denied"),
    )
    executor.delays = {"tables": 0.04, "foreign_keys": 0.001, "forced_plans": 0.04, "coverage": 0.001}

    report = await WorkloadIndexAdvisor(executor).review("appdb", lookback_days=7, now=NOW)

    assert report["result_status"] == "ok"
    starts = [gap.split(":")[0] for gap in report["gaps"]]
    wanted = [
        "Table sizes could not be read",
        "Foreign keys could not be read",
        "Forced Query Store plans could not be read",
        "Query Store window coverage could not be read",
    ]
    assert [start for start in starts if start in wanted] == wanted


@pytest.mark.asyncio
async def test_unavailable_index_metadata_returns_the_same_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    # Before the reads were staged, the table read had not run when the index
    # metadata read failed. Its gap must not appear in the early return.
    async def failing_collect(executor: Any, database_name: str, **kwargs: Any) -> Any:
        raise PermissionError("VIEW DEFINITION permission denied")

    monkeypatch.setattr(module, "collect_existing_indexes", failing_collect)
    executor = RoutingExecutor(
        tables=PermissionError("tables denied"),
        workload=TimeoutError("query timeout"),
    )

    report = await WorkloadIndexAdvisor(executor).review("appdb", now=NOW)

    assert set(report) == {
        "database_name",
        "recommend_only",
        "recommendations",
        "gaps",
        "result_status",
        "result_status_reason",
    }
    assert report["database_name"] == "appdb"
    assert report["recommend_only"] is True
    assert report["recommendations"] == []
    assert report["gaps"] == ["Query Store workload could not be read: query timeout"]
    assert report["result_status"] == "unavailable"
    assert report["result_status_reason"].startswith(
        "Existing index metadata could not be read, so no index advice is safe: "
        "VIEW DEFINITION permission denied. The index catalog views need VIEW DEFINITION."
    )


# ---------------------------------------------------------------- #65


def _workload_call(executor: RoutingExecutor) -> tuple[str, list[Any]]:
    _key, sql, params = next(call for call in executor.calls if call[0] == "workload")
    assert params is not None
    return sql, params


@pytest.mark.asyncio
async def test_given_query_ids_are_analysed_whatever_their_rank(existing) -> None:  # noqa: F811
    # The optimizer tunes one query. When it ranks below top_queries (or the
    # review ranks by another measure), its plan was never read and the advice
    # was for other queries.
    executor = RoutingExecutor()

    await WorkloadIndexAdvisor(executor).review("appdb", lookback_days=7, now=NOW, top_queries=5, query_ids=[140, 7, 140])

    sql, params = _workload_call(executor)
    assert "WHERE (r.workload_rank <= ? OR r.query_id IN (?, ?))" in sql
    assert params == ["2026-10-01T12:00:00", "2026-09-24T12:00:00", 5, 140, 7, 1]


@pytest.mark.asyncio
async def test_given_query_ids_pass_the_table_scope_filter(existing) -> None:  # noqa: F811
    executor = RoutingExecutor()

    await WorkloadIndexAdvisor(executor).review(
        "appdb", lookback_days=7, now=NOW, schema_name="Sales", table_names=["Orders"], query_ids=[140]
    )

    sql, params = _workload_call(executor)
    # The scope decides in_scope before the plan XML is returned; a given id is
    # returned even when its plan is outside the scope.
    assert "WHERE ((r.in_scope = 1 AND r.workload_rank <= ?) OR r.query_id IN (?))" in sql
    assert params == [
        "2026-10-01T12:00:00",
        "2026-09-24T12:00:00",
        'Schema="[Sales]" Table="[Orders]"',
        100,
        140,
        1,
    ]


@pytest.mark.asyncio
async def test_review_without_query_ids_sends_no_id_filter(existing) -> None:  # noqa: F811
    executor = RoutingExecutor()

    await WorkloadIndexAdvisor(executor).review("appdb", lookback_days=7, now=NOW)

    sql, params = _workload_call(executor)
    assert "r.query_id IN" not in sql
    assert params == ["2026-10-01T12:00:00", "2026-09-24T12:00:00", 100, 1]


@pytest.mark.asyncio
async def test_given_query_id_with_no_runtime_stats_in_the_window_is_a_gap(existing) -> None:  # noqa: F811
    executor = RoutingExecutor(workload=[_workload_row(7, "seek_residual_lookup_sort.xml", 900_000)])

    report = await WorkloadIndexAdvisor(executor).review("appdb", lookback_days=7, now=NOW, query_ids=[7, 140, 141])

    assert (
        "Query id(s) 140, 141 have no Query Store runtime statistics in the window, so they were not analysed"
        in report["gaps"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("query_ids", [[0], [-3], list(range(1, 52))])
async def test_query_ids_are_validated(existing, query_ids: list[int]) -> None:  # noqa: F811
    assert module.MAX_QUERY_IDS == 50
    with pytest.raises(ValueError, match="query_ids"):
        await WorkloadIndexAdvisor(RoutingExecutor()).review("appdb", now=NOW, query_ids=query_ids)
