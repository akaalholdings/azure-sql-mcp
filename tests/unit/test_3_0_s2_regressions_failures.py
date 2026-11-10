"""Regression false positives, plan flips and rising timeouts (Query Store).

The regression and failure SQL runs on SQLite, so the grain, the candidate cut
and the flags come from rows, not from a mocked result.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

import pytest

from azure_sql_mcp import query_store_trends
from azure_sql_mcp.query_store import QueryStoreService
from azure_sql_mcp.query_store_trends import QueryStoreTrendService
from tests.unit.test_query_regression import QueryStoreSqlite
from tests.unit.test_query_regression import _hourly_intervals
from tests.unit.test_query_regression import _qs_ts

PLAN_XML = (
    "<ShowPlanXML xmlns='http://schemas.microsoft.com/sqlserver/2004/07/showplan'>"
    "<BatchSequence><Batch><Statements>"
    "<StmtSimple CardinalityEstimationModelVersion='{ce}'><QueryPlan>"
    "<OptimizerStatsUsage><StatisticsInfo Database='[db]' Schema='[dbo]' Table='[Orders]' "
    "Statistics='[IX_Orders_Customer]' ModificationCount='{mods}' SamplingPercent='100' "
    "LastUpdate='{stats_updated}'/></OptimizerStatsUsage>"
    "<RelOp PhysicalOp='Index Seek'/>"
    "<ParameterList><ColumnReference Column='@c' ParameterDataType='int' "
    "ParameterCompiledValue='({value})'/></ParameterList>"
    "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"
)


class TrendSqlite(QueryStoreSqlite):
    """QueryStoreSqlite plus the T-SQL the trend reads use."""

    def __init__(self, hours: int) -> None:
        super().__init__()
        self.conn.execute("ALTER TABLE sys.query_store_runtime_stats ADD COLUMN replica_group_id INT")
        self.conn.execute(
            "CREATE TABLE sys.query_store_replicas (replica_group_id INT, role_type INT)"
        )
        self.replica_dimension = False
        self.first_interval: datetime | None = None
        top = _hourly_intervals(self, hours)
        # The newest interval ends one hour after the top of the current hour.
        self.now = (top + timedelta(hours=1)).replace(tzinfo=timezone.utc)

    def add_plan(self, query_id: int, plan_id: int, *, text: str | None = None, **plan: Any) -> None:
        exists = self.conn.execute(
            "SELECT 1 FROM sys.query_store_query WHERE query_id = ?", [query_id]
        ).fetchone()
        if not exists:
            self.insert(
                "query_store_query_text",
                query_text_id=query_id,
                query_sql_text=text or f"SELECT {query_id}",
            )
            self.insert(
                "query_store_query",
                query_id=query_id,
                query_text_id=query_id,
                query_hash=f"0x{query_id:016X}",
            )
        self.insert(
            "query_store_plan",
            plan_id=plan_id,
            query_id=query_id,
            is_forced_plan=0,
            force_failure_count=0,
            **{"query_plan": "", **plan},
        )

    def run(
        self,
        plan_id: int,
        interval_id: int,
        count: int,
        duration_us: float,
        *,
        cpu_us: float | None = None,
        rows: float = 10.0,
        execution_type: int = 0,
        replica_group_id: int = 1,
    ) -> None:
        self.insert(
            "query_store_runtime_stats",
            plan_id=plan_id,
            runtime_stats_interval_id=interval_id,
            execution_type=execution_type,
            count_executions=count,
            avg_duration=duration_us,
            avg_cpu_time=duration_us if cpu_us is None else cpu_us,
            avg_logical_io_reads=10.0,
            avg_rowcount=rows,
            replica_group_id=replica_group_id,
        )

    async def fetch_all(self, database_name: str, query: str, params: list | None = None) -> list[dict]:
        if "intervals_in_baseline" in query:
            row = self.conn.execute(
                "SELECT MIN(start_time), COUNT(*) FROM sys.query_store_runtime_stats_interval"
            ).fetchone()
            first = self.first_interval or datetime.fromisoformat(row[0])
            return [{"first_interval_utc": _qs_ts(first).replace(" ", "T") + "Z",
                     "intervals_in_baseline": row[1]}]
        if "database_query_store_options" in query:
            return [{"actual_state_desc": "READ_WRITE"}]
        if "sys.all_columns" in query:
            flag = int(self.replica_dimension)
            return [{"runtime_stats_has_replica_group_id": flag, "has_query_store_replicas": flag}]
        return await super().fetch_all(database_name, query, params)


def _service(store: TrendSqlite) -> QueryStoreTrendService:
    return QueryStoreTrendService(store)


async def _regressions(store: TrendSqlite, **kwargs: Any) -> dict[str, Any]:
    return await _service(store).regressions(
        "db", recent_minutes=60, baseline_minutes=47 * 60, now=store.now, **kwargs
    )


@pytest.mark.asyncio
async def test_flip_to_a_plan_that_ran_in_the_baseline_is_a_plan_change() -> None:
    # Plan 7 ran 990 times at 2 ms and plan 9 ran 10 times at 400 ms. Now only
    # plan 9 runs. This is the classic plan flip; "no plan change" sends the
    # reader to data and statistics, away from the one-call fix.
    store = TrendSqlite(48)
    store.add_plan(1, 7)
    store.add_plan(1, 9)
    store.run(7, 10, 990, 2_000)
    store.run(9, 11, 10, 400_000)
    store.run(9, 48, 100, 400_000)

    [item] = (await _regressions(store))["regressions"]

    assert item["new_plan_ids"] == []
    assert item["dominant_baseline_plan_id"] == 7
    assert item["dominant_recent_plan_id"] == 9
    assert item["plan_changed"] is True


@pytest.mark.asyncio
async def test_candidates_are_cut_on_the_requested_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    # Query 1 dominates duration (blocked, no extra CPU). Query 2 is short but
    # burns 50x the CPU. A cut on duration never compares query 2 for CPU.
    monkeypatch.setattr(query_store_trends, "REGRESSION_CANDIDATE_CAP", 1)
    store = TrendSqlite(48)
    store.add_plan(1, 10)
    store.run(10, 10, 100, 1_000_000, cpu_us=1_000)
    store.run(10, 48, 100, 9_000_000, cpu_us=1_000)
    store.add_plan(2, 20)
    store.run(20, 10, 100, 500, cpu_us=100)
    store.run(20, 48, 100, 900, cpu_us=5_000)

    result = await _regressions(store, metric="cpu")

    assert [item["query_id"] for item in result["regressions"]] == [2]
    assert result["candidates_capped"] is True


@pytest.mark.asyncio
async def test_option_recompile_statement_is_not_a_regression() -> None:
    # Each run compiles for its own values: the cost follows the parameter mix
    # and there is no cached plan that regressed, so forcing fixes nothing.
    store = TrendSqlite(48)
    store.add_plan(1, 10, text="SELECT * FROM dbo.Orders WHERE c = @c OPTION (MAXDOP 1, RECOMPILE)")
    store.run(10, 10, 100, 1_000)
    store.run(10, 48, 100, 5_000)
    store.add_plan(2, 20, text="SELECT * FROM dbo.Lines WHERE c = @c")
    store.run(20, 10, 100, 1_000)
    store.run(20, 48, 100, 5_000)

    result = await _regressions(store)

    assert [item["query_id"] for item in result["regressions"]] == [2]
    [filtered] = result["filtered_candidates"]
    assert (filtered["query_id"], filtered["filter_reason"]) == (1, "statement_has_option_recompile")
    assert filtered["regression_pct"] == 400.0


@pytest.mark.asyncio
async def test_more_rows_at_the_same_cost_per_row_is_not_a_regression() -> None:
    # Query 1 returns 50x the rows and takes 20x the time: cheaper per row.
    # Query 2 returns 50x the rows and takes 200x the time: 4x per row.
    store = TrendSqlite(48)
    store.add_plan(1, 10)
    store.run(10, 10, 100, 1_000, rows=10)
    store.run(10, 48, 100, 20_000, rows=500)
    store.add_plan(2, 20)
    store.run(20, 10, 100, 1_000, rows=10)
    store.run(20, 48, 100, 200_000, rows=500)

    result = await _regressions(store)

    assert [item["query_id"] for item in result["regressions"]] == [2]
    [filtered] = result["filtered_candidates"]
    assert (filtered["query_id"], filtered["filter_reason"]) == (1, "row_count_growth")
    assert filtered["rows_per_execution_ratio"] == 50.0


@pytest.mark.asyncio
async def test_plans_that_differ_only_in_compiled_values_are_not_a_regression() -> None:
    # Query 1: same shape, same statistics, same estimator; only the compiled
    # value differs, so the older plan is the same plan. Query 2: the
    # statistics were updated between the compiles, which can change the plan's
    # estimates: it stays a regression.
    store = TrendSqlite(48)
    same = {"ce": 160, "mods": 0, "stats_updated": "2026-10-01T00:00:00.00"}
    store.add_plan(1, 10, query_plan=PLAN_XML.format(value=5, **same), query_plan_hash="0xAA")
    store.add_plan(1, 11, query_plan=PLAN_XML.format(value=900, **same), query_plan_hash="0xAA")
    store.run(10, 10, 100, 1_000)
    store.run(11, 48, 100, 5_000)
    store.add_plan(2, 20, query_plan=PLAN_XML.format(value=5, **same), query_plan_hash="0xBB")
    store.add_plan(
        2, 21, query_plan_hash="0xBB",
        query_plan=PLAN_XML.format(value=900, ce=160, mods=0, stats_updated="2026-10-09T00:00:00.00"),
    )
    store.run(20, 10, 100, 1_000)
    store.run(21, 48, 100, 5_000)

    result = await _regressions(store)

    assert [item["query_id"] for item in result["regressions"]] == [2]
    [filtered] = result["filtered_candidates"]
    assert filtered["query_id"] == 1
    assert filtered["filter_reason"] == "plans_differ_only_in_compiled_values"


def _failure_store() -> TrendSqlite:
    """15 days of hourly intervals: 14 days of baseline and the last 24 hours."""

    store = TrendSqlite(15 * 24)
    for query_id in (1, 2, 3, 4):
        store.add_plan(query_id, query_id * 10)
        store.run(query_id * 10, 100, 1_000, 1_000)
        store.run(query_id * 10, 350, 1_000, 1_000)
    return store


RECENT = 350  # an interval inside the last 24 hours
BASELINE = 100  # an interval inside the 14 days before


async def _failures(store: TrendSqlite, **kwargs: Any) -> dict[str, Any]:
    return await _service(store).failed_executions("db", now=store.now, **kwargs)


@pytest.mark.asyncio
async def test_rising_timeouts_are_flagged_against_the_baseline_rate() -> None:
    store = _failure_store()
    store.run(10, BASELINE, 1, 30_000_000, execution_type=3)
    store.run(10, RECENT, 12, 30_000_000, execution_type=3)  # 12 against 1 in 14 days
    store.run(20, BASELINE, 28, 30_000_000, execution_type=3)
    store.run(20, RECENT, 4, 30_000_000, execution_type=3)  # 4 against 2 a day
    store.run(30, BASELINE, 28, 1_000, execution_type=4)
    store.run(30, RECENT, 3, 1_000, execution_type=4)  # 3 against 2 a day: no rise
    store.run(40, RECENT, 2, 1_000, execution_type=4)  # under 3 runs

    result = await _failures(store)

    assert [(item["query_id"], item["severity"]) for item in result["rising"]] == [
        (1, "high"),
        (2, "medium"),
    ]
    assert result["rising"][0]["recent_aborted_executions"] == 12
    assert result["rising"][1]["rise_ratio"] == 2.0
    # The ranking shows who fails most, flagged or not.
    assert [item["query_id"] for item in result["top_failing_queries"]] == [1, 2, 3, 4]
    summary = result["summary"]
    assert summary["recent_failed_executions"] == 21
    assert summary["baseline_failed_executions"] == 57
    assert summary["expected_failed_executions"] == pytest.approx(57 / 14, abs=0.01)
    assert summary["severity"] == "high"


@pytest.mark.asyncio
async def test_baseline_rate_uses_the_days_query_store_really_holds() -> None:
    # Retention kept 7 of the 14 baseline days: 35 failures are 5 a day, so 13
    # recent failures are a 2.6x rise. Spread over 14 days they read as 5.2x.
    store = _failure_store()
    store.first_interval = (store.now - timedelta(days=8)).replace(tzinfo=None)
    store.run(10, 300, 35, 30_000_000, execution_type=3)
    store.run(10, RECENT, 13, 30_000_000, execution_type=3)

    [item] = (await _failures(store))["rising"]

    assert item["expected_failed_executions"] == pytest.approx(5.0)
    assert item["severity"] == "medium"


@pytest.mark.asyncio
async def test_no_failed_runs_is_an_empty_result() -> None:
    result = await _failures(_failure_store())

    assert result["rising"] == []
    assert result["top_failing_queries"] == []
    assert result["summary"]["recent_failed_executions"] == 0
    assert "result_status" not in result


@pytest.mark.asyncio
async def test_failures_and_regressions_read_one_replica_role() -> None:
    # Group 1 is the primary and group 2 a readable secondary. The secondary's
    # timeouts must not be counted as primary timeouts.
    store = _failure_store()
    store.replica_dimension = True
    store.insert("query_store_replicas", replica_group_id=1, role_type=1)
    store.insert("query_store_replicas", replica_group_id=2, role_type=2)
    store.run(10, RECENT, 4, 30_000_000, execution_type=3)
    store.run(10, RECENT, 40, 30_000_000, execution_type=3, replica_group_id=2)

    blended = await _failures(store)
    primary = await _failures(store, replica_role="primary")
    secondary = await _failures(store, replica_role="secondary")

    assert blended["summary"]["recent_failed_executions"] == 44
    assert primary["summary"]["recent_failed_executions"] == 4
    assert secondary["summary"]["recent_failed_executions"] == 40
    assert primary["replica_scope"] == {"replica_role": "primary", "replica_filter": "replica_group_id"}


@pytest.mark.asyncio
async def test_replica_role_on_a_database_without_the_dimension() -> None:
    # No replica_group_id in the catalog: every run is a primary run, so
    # "primary" reads unfiltered and "secondary" is not supported. No statement
    # may name the missing column.
    store = _failure_store()
    store.run(10, RECENT, 4, 30_000_000, execution_type=3)

    primary = await _failures(store, replica_role="primary")
    secondary = await _failures(store, replica_role="secondary")

    assert primary["summary"]["recent_failed_executions"] == 4
    assert primary["replica_scope"]["replica_filter"] == "not_available"
    assert secondary["result_status"] == "not_supported"
    assert not any("replica_group_id" in query and "all_columns" not in query for query, _ in store.calls)


def _replica_workload() -> TrendSqlite:
    """Query 1 runs on the primary; query 2 is a heavy report on a secondary."""

    store = TrendSqlite(4)
    store.replica_dimension = True
    store.add_plan(1, 10)
    store.run(10, 4, 100, 1_000)
    store.add_plan(2, 20)
    store.run(20, 4, 100, 900_000, replica_group_id=2)
    return store


@pytest.mark.asyncio
async def test_top_queries_for_the_primary_leave_out_secondary_runs() -> None:
    # A report that runs on a readable secondary uses no primary CPU. Ranked
    # with the primary's queries it is blamed for primary pressure.
    store = _replica_workload()
    store.insert("query_store_replicas", replica_group_id=1, role_type=1)
    store.insert("query_store_replicas", replica_group_id=2, role_type=2)
    service = QueryStoreService(store)

    blended = await service.get_top_queries("db", "cpu", 1440, 5)
    primary = await service.get_top_queries("db", "cpu", 1440, 5, replica_role="primary")
    secondary = await service.get_top_queries("db", "cpu", 1440, 5, replica_role="secondary")

    assert [row["query_id"] for row in blended] == [2, 1]
    assert [row["query_id"] for row in primary] == [1]
    assert [row["query_id"] for row in secondary] == [2]


@pytest.mark.asyncio
async def test_primary_reads_everything_when_no_replica_is_registered() -> None:
    # The catalog has the dimension but sys.query_store_replicas is empty:
    # secondaries are not captured, so every stored run is a primary run.
    store = _replica_workload()

    primary = await QueryStoreService(store).get_top_queries(
        "db", "cpu", 1440, 5, replica_role="primary"
    )

    assert [row["query_id"] for row in primary] == [2, 1]


@pytest.mark.asyncio
async def test_secondary_top_queries_are_refused_without_the_dimension() -> None:
    store = _replica_workload()
    store.replica_dimension = False

    with pytest.raises(ValueError, match="no replica dimension"):
        await QueryStoreService(store).get_top_queries(
            "db", "cpu", 1440, 5, replica_role="secondary"
        )
    with pytest.raises(ValueError, match="replica_role must be one of"):
        await QueryStoreService(store).get_top_queries("db", "cpu", 1440, 5, replica_role="standby")


@pytest.mark.asyncio
async def test_replica_dimension_lists_the_replica_sets_with_their_roles() -> None:
    store = _replica_workload()
    store.conn.execute("ALTER TABLE sys.query_store_replicas ADD COLUMN replica_name TEXT")
    store.insert("query_store_replicas", replica_group_id=1, role_type=1, replica_name="p")
    store.insert("query_store_replicas", replica_group_id=2, role_type=2, replica_name="s")

    result = await QueryStoreService(store).get_replica_dimension("db")

    assert result["available"] is True
    assert [(row["replica_group_id"], row["role"]) for row in result["replicas"]] == [
        (1, "primary"),
        (2, "secondary"),
    ]
