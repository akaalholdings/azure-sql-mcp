"""Forced-plan averages and age (finding 82) and recompile detection (finding 36)."""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp import query_regression
from azure_sql_mcp.plan_cache import PlanCacheService
from azure_sql_mcp.query_regression import QueryRegressionService
from tests.unit.test_query_regression import QueryStoreSqlite
from tests.unit.test_query_regression import _hourly_intervals


class ForcedPlanSqlite(QueryStoreSqlite):
    def __init__(self) -> None:
        super().__init__()
        self.conn.execute("ALTER TABLE sys.query_store_plan ADD COLUMN plan_forcing_type_desc TEXT")
        self.conn.execute("ALTER TABLE sys.query_store_plan ADD COLUMN last_execution_time TEXT")
        self.conn.execute("ALTER TABLE sys.query_store_runtime_stats ADD COLUMN max_duration REAL")

    def force(self, plan_id: int, *, last_execution_time: str | None = None) -> None:
        self.conn.execute(
            "UPDATE sys.query_store_plan SET is_forced_plan = 1, plan_forcing_type_desc = 'MANUAL', "
            "last_execution_time = ? WHERE plan_id = ?",
            [last_execution_time, plan_id],
        )

    async def stats(self) -> list[dict[str, Any]]:
        """Run the service's own forced-plan aggregation."""

        return await self.fetch_all(
            "db",
            query_regression._FORCED_PLAN_STATS_CTE + "SELECT * FROM ForcedPlanStats",
            params=[1440],
        )


@pytest.mark.asyncio
async def test_forced_plan_average_is_not_the_worst_interval() -> None:
    # 10,000 runs at 3 ms and one 4 s timeout. The worst interval's average
    # read as avg_duration_ms tells the reviewer to unforce a good plan.
    store = ForcedPlanSqlite()
    _hourly_intervals(store, 3)
    store.add_query(42, {7: "Index Seek"})
    store.force(7)
    store.add_runtime(7, 1, 6000, 3_000)
    store.add_runtime(7, 2, 4000, 3_000)
    store.add_runtime(7, 3, 1, 4_000_000, execution_type=3)
    store.conn.execute("UPDATE sys.query_store_runtime_stats SET max_duration = avg_duration")

    [row] = await store.stats()

    assert row["avg_duration_ms"] == pytest.approx(3.0)
    assert row["max_duration_ms"] == pytest.approx(4000.0)
    assert row["aborted_executions"] == 1
    assert row["count_executions"] == 10_001


@pytest.mark.asyncio
async def test_forced_plan_keeps_its_last_execution_after_runtime_stats_age_out() -> None:
    # Query Store removed the plan's runtime statistics (retention). The plan
    # row still knows when it last ran; without it the plan looked fresh.
    store = ForcedPlanSqlite()
    _hourly_intervals(store, 2)
    store.add_query(42, {7: "Index Seek"})
    store.force(7, last_execution_time="2026-08-01 00:00:00")

    [row] = await store.stats()

    assert row["count_executions"] is None
    assert row["last_execution_time"] == "2026-08-01 00:00:00"


class RowsExecutor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.query = ""

    async def fetch_all(self, database_name: str, query: str, params: Any = None) -> list[dict[str, Any]]:
        self.query = query
        return self.rows


def _forced_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "plan_id": 7, "query_id": 42, "is_forced_plan": True, "force_failure_count": 0,
        "count_executions": 100, "recent_execution_count": 5, "days_since_last_exec": 0,
    }
    row.update(overrides)
    return row


@pytest.mark.asyncio
async def test_forced_plan_with_no_execution_evidence_is_not_fresh() -> None:
    # No runtime statistics and no last execution time: the age is unknown.
    # Unknown is not zero days; the plan is listed as stale for review.
    rows = [
        _forced_row(),
        _forced_row(plan_id=9, count_executions=None, recent_execution_count=None,
                    days_since_last_exec=None),
    ]

    result = await QueryRegressionService(RowsExecutor(rows)).get_forced_plans("db")

    assert result["stale_count"] == 1
    [warning] = result["warnings"]
    assert warning["type"] == "stale_forced_plans"
    assert warning["plan_ids"] == [9]
    assert result["forced_plans"][1]["last_execution_unknown"] is True
    assert result["forced_plans"][0]["last_execution_unknown"] is False


def _compile_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "query_hash": b"\x01", "recompile_count": 2, "execution_count": 100,
        "minutes_since_last_compile": 5000, "sql_text_preview": "SELECT 1",
    }
    row.update(overrides)
    return row


@pytest.mark.asyncio
async def test_statement_that_recompiles_on_every_call_is_flagged() -> None:
    # execution_count restarts at each compile, so a statement that recompiles
    # on every call shows 1 execution and 5,000 plan generations. The old
    # "more than 10 executions" gate hid exactly this statement.
    executor = RowsExecutor(
        [
            _compile_row(recompile_count=5000, execution_count=1, minutes_since_last_compile=0),
            # Recompiled 20 times months ago, stable since: not churn.
            _compile_row(query_hash=b"\x02", recompile_count=20, execution_count=11,
                         minutes_since_last_compile=130_000),
        ]
    )

    result = await PlanCacheService(executor).get_query_compilation_stats("db")

    assert result["excessive_recompile_count"] == 1
    assert [row["recompiling_now"] for row in result["queries"]] == [True, False]
    [warning] = result["warnings"]
    # plan_generation_num and execution_count cover different periods, so no
    # share of executions can be claimed.
    assert "%" not in warning["message"]
    assert "recompile_ratio" not in executor.query
    assert "minutes_since_last_compile" in executor.query
