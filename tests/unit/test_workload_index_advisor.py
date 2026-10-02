from __future__ import annotations

import asyncio
from datetime import datetime
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from azure_sql_mcp import workload_index_advisor as module
from azure_sql_mcp.azure_tier import SERVER_STATE_GATED_DMVS
from azure_sql_mcp.workload_index_advisor import WorkloadIndexAdvisor
from azure_sql_mcp.workload_index_advisor import resolve_window
from azure_sql_mcp.workload_index_advisor import workload_sql
from tests.unit.test_index_advisor import _index
from tests.unit.test_index_advisor import _orders_indexes

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "showplans"
NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def _plan_text(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _workload_row(query_id: int, fixture: str, cpu_us: float) -> dict[str, Any]:
    return {
        "query_id": query_id,
        "plan_id": query_id * 10,
        "plan_rank": 1,
        "workload_rank": query_id,
        "executions": 5000,
        "total_cpu_us": cpu_us,
        "total_duration_us": cpu_us * 2,
        "total_logical_reads": cpu_us / 10,
        "total_logical_writes": 0,
        "total_rowcount": 100,
        "active_days": 6,
        "workload_query_count": 40,
        "workload_executions": 90_000,
        "workload_cpu_us": 3_000_000,
        "workload_duration_us": 6_000_000,
        "workload_logical_reads": 300_000,
        "object_schema": None,
        "object_name": None,
        "query_text_preview": "SELECT ...",
        "is_forced_plan": False,
        "query_plan": _plan_text(fixture),
    }


class RoutingExecutor:
    def __init__(self, **overrides: Any) -> None:
        self.responses: dict[str, Any] = {
            "query_store": [
                {
                    "actual_state_desc": "READ_WRITE",
                    "desired_state_desc": "READ_WRITE",
                    "readonly_reason": 0,
                    "query_capture_mode_desc": "ALL",
                    "interval_length_minutes": 60,
                    "stale_query_threshold_days": 30,
                    "max_storage_size_mb": 1000,
                    "current_storage_size_mb": 120,
                }
            ],
            "workload": [
                _workload_row(1, "seek_residual_lookup_sort.xml", 900_000),
                _workload_row(2, "clustered_scan_residuals.xml", 2_000_000),
            ],
            "tables": [
                {"object_id": 1, "schema_name": "Sales", "table_name": "Orders", "row_count": 73_595, "base_used_pages": 3000, "all_used_pages": 4000, "is_heap": 0, "is_memory_optimized": 0},
                {"object_id": 2, "schema_name": "Sales", "table_name": "OrderLines", "row_count": 231_412, "base_used_pages": 8000, "all_used_pages": 9000, "is_heap": 0, "is_memory_optimized": 0},
            ],
            "columns": [
                {"object_id": 1, "column_name": name, "type_name": type_name, "max_length": length, "is_nullable": 1, "is_computed": 0}
                for name, type_name, length in (
                    ("OrderID", "int", 4),
                    ("CustomerID", "int", 4),
                    ("OrderDate", "date", 3),
                    ("Status", "tinyint", 1),
                    ("Comments", "nvarchar", 400),
                )
            ]
            + [
                {"object_id": 2, "column_name": name, "type_name": type_name, "max_length": length, "is_nullable": 1, "is_computed": 0}
                for name, type_name, length in (
                    ("OrderLineID", "int", 4),
                    ("StockItemID", "int", 4),
                    ("Description", "nvarchar", 200),
                    ("PackageTypeID", "int", 4),
                    ("Quantity", "int", 4),
                    ("UnitPrice", "decimal", 9),
                    ("PickingCompletedWhen", "datetime2", 8),
                )
            ],
            "selectivity": [
                {"object_id": 1, "column_name": "CustomerID", "stats_name": "s1", "rows": 73_595, "rows_sampled": 73_595, "modification_counter": 0, "last_updated_utc": "2026-09-30T00:00:00", "distinct_estimate": 663},
                {"object_id": 1, "column_name": "Status", "stats_name": "s2", "rows": 73_595, "rows_sampled": 73_595, "modification_counter": 0, "last_updated_utc": "2026-09-30T00:00:00", "distinct_estimate": 3},
            ],
            "foreign_keys": [],
            "operational": [],
            "references": [],
            "forced_plans": [],
            # Query Store holds exactly the requested window unless a test says otherwise.
            "coverage": lambda params: [
                {
                    "effective_start_utc": params[1],
                    "effective_end_utc": params[0],
                    "interval_count": 24 * 7,
                    "oldest_interval_start_utc": params[1],
                }
            ],
            "hint_query_text": [],
            "hint_query_hints": [],
            "hint_plan_guides": [],
            "hint_modules": [],
        }
        self.responses.update(overrides)
        self.delays: dict[str, float] = {}
        self.calls: list[tuple[str, str, list[Any] | None]] = []
        self.in_flight = 0
        self.peak_in_flight = 0

    async def fetch_all(self, database_name: str, query: str, params=None, **kwargs: Any):
        if "sys.database_query_store_options" in query:
            key = "query_store"
        elif "WITH plan_stats AS" in query:
            key = "workload"
        elif "FROM sys.tables AS t" in query:
            key = "tables"
        elif "dm_db_stats_histogram" in query:
            key = "selectivity"
        elif "INNER JOIN sys.types AS ty" in query:
            key = "columns"
        elif "dm_db_index_operational_stats" in query:
            key = "operational"
        elif "FROM sys.foreign_keys AS fk" in query:
            key = "foreign_keys"
        elif "FROM (VALUES" in query:
            key = "references"
        elif "p.is_forced_plan = 1" in query:
            key = "forced_plans"
        elif "AS oldest_interval_start_utc" in query:
            key = "coverage"
        elif "AS retained_query_text" in query:
            key = "hint_query_text"
        elif "FROM sys.query_store_query_hints" in query:
            key = "hint_query_hints"
        elif "FROM sys.plan_guides" in query:
            key = "hint_plan_guides"
        elif "FROM sys.sql_modules" in query:
            key = "hint_modules"
        else:
            raise AssertionError(f"unexpected query: {query[:80]}")
        self.calls.append((key, query, list(params) if params is not None else None))
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self.delays.get(key, 0.001))
        finally:
            self.in_flight -= 1
        result = self.responses[key]
        if callable(result):
            result = result(params)
        if isinstance(result, Exception):
            raise result
        return result


@pytest.fixture
def existing(monkeypatch: pytest.MonkeyPatch):
    indexes = _orders_indexes() + [
        _index("Sales", "OrderLines", "PK_Sales_OrderLines", ("OrderLineID",), index_id=1, type_code=1, primary_key=True),
        _index("Sales", "Orders", "IX_Odd]Name", ("OrderDate",), index_id=7, reads=0, updates=900),
    ]
    for index in indexes:
        index.usage_context["engine_start_time_utc"] = "2026-08-01T00:00:00Z"

    async def fake_collect(executor, database_name, **kwargs):
        return indexes

    monkeypatch.setattr(module, "collect_existing_indexes", fake_collect)
    return indexes


def test_window_resolution_rejects_the_future_and_accepts_dates() -> None:
    start, end = resolve_window(7, None, now=NOW)
    assert (end - start).days == 7 and end == NOW

    start, end = resolve_window(1, "2026-09-30", now=NOW)
    assert end == datetime(2026, 9, 30, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="future"):
        resolve_window(1, "2026-12-01T00:00:00Z", now=NOW)
    with pytest.raises(ValueError, match="ISO-8601"):
        resolve_window(1, "last tuesday", now=NOW)


def test_workload_sql_orders_by_a_whitelisted_objective_column() -> None:
    assert "ORDER BY qs.total_cpu_us DESC" in workload_sql("cpu")
    assert "ORDER BY qs.total_logical_reads DESC" in workload_sql("logical_reads")
    with pytest.raises(KeyError):
        workload_sql("drop table")


@pytest.mark.asyncio
async def test_full_review_uses_query_store_plans_and_returns_ranked_advice(existing) -> None:
    executor = RoutingExecutor()

    report = await WorkloadIndexAdvisor(executor).review("appdb", lookback_days=7, now=NOW)

    assert report["result_status"] == "ok"
    actions = {(rec["action"], rec["table"]) for rec in report["recommendations"]}
    assert ("widen_index", "Orders") in actions
    assert ("create_index", "OrderLines") in actions
    workload_call = next(call for call in executor.calls if call[0] == "workload")
    assert workload_call[2] == ["2026-10-01T12:00:00", "2026-09-24T12:00:00", 100, 1]
    assert report["window"] == {
        "start_utc": "2026-09-24T12:00:00Z",
        "end_utc": "2026-10-01T12:00:00Z",
        "lookback_days": 7,
        "requested_days": 7,
        "query_store_effective_start_utc": "2026-09-24T12:00:00Z",
        "query_store_effective_end_utc": "2026-10-01T12:00:00Z",
        "query_store_effective_days": 7.0,
        "query_store_interval_count": 168,
        "query_store_oldest_interval_utc": "2026-09-24T12:00:00Z",
    }
    assert report["workload"]["queries_in_window"] == 40
    assert report["workload"]["analyzed_share_pct"] == pytest.approx(96.667, rel=1e-3)
    assert report["usage_counters"]["engine_start_time_utc"] == "2026-08-01T00:00:00Z"


@pytest.mark.asyncio
async def test_reference_check_patterns_escape_closing_brackets(existing) -> None:
    executor = RoutingExecutor(references=[{"ref_id": 0, "plan_references": 2, "statistics_references": 0}])

    report = await WorkloadIndexAdvisor(executor).review("appdb", now=NOW)

    reference_call = next(call for call in executor.calls if call[0] == "references")
    assert reference_call[2][:3] == [0, 'Index="[IX_Odd]]Name]"', 'Statistics="[IX_Odd]]Name]"']
    review = [r for r in report["recommendations"] if r["index_name"] == "IX_Odd]Name"]
    assert review[0]["reason_codes"] == ["unused_by_dmv_but_in_query_store_plans"]


def _unused_drop(report: dict[str, Any]) -> dict[str, Any]:
    [rec] = [r for r in report["recommendations"] if r["index_name"] == "IX_Odd]Name"]
    assert rec["action"] == "drop_index"
    return rec


# Usage counters (61 days) and a 40-day Query Store window both cover the
# 35-day removal window, so IX_Odd]Name is a high-confidence drop by default.
LONG = {"lookback_days": 40, "now": NOW}
NO_REFERENCES = [{"ref_id": 0, "plan_references": 0, "statistics_references": 0}]


@pytest.mark.asyncio
async def test_unused_index_is_a_high_confidence_drop_when_every_dependency_check_is_complete(existing) -> None:
    executor = RoutingExecutor(references=NO_REFERENCES)

    rec = _unused_drop(await WorkloadIndexAdvisor(executor).review("appdb", **LONG))

    assert rec["confidence"] == "high"
    assert rec["blockers"] == []
    assert rec["ddl"] == "DROP INDEX [IX_Odd]]Name] ON [Sales].[Orders];"


@pytest.mark.asyncio
async def test_a_heap_elsewhere_does_not_make_the_hint_check_incomplete(existing) -> None:
    heap = _index("dbo", "Staging", "", (), index_id=0, type_code=0, reads=None)
    heap.usage_context["engine_start_time_utc"] = "2026-08-01T00:00:00Z"
    existing.append(heap)
    executor = RoutingExecutor(references=NO_REFERENCES)

    rec = _unused_drop(await WorkloadIndexAdvisor(executor).review("appdb", **LONG))

    assert "hint_reference_check_incomplete" not in rec["blockers"]
    assert rec["confidence"] == "high"


@pytest.mark.asyncio
async def test_reference_check_counts_statistics_usage_separately(existing) -> None:
    executor = RoutingExecutor(references=[{"ref_id": 0, "plan_references": 0, "statistics_references": 2}])

    rec = _unused_drop(await WorkloadIndexAdvisor(executor).review("appdb", **LONG))

    reference_call = next(call for call in executor.calls if call[0] == "references")
    assert "statistics_references" in reference_call[1]
    assert "index_statistics_used_by_optimizer" in rec["reason_codes"]
    assert rec["ddl"].startswith("CREATE STATISTICS [st_IX_Odd]]Name] ON [Sales].[Orders] ([OrderDate])")
    assert rec["confidence"] == "medium"


@pytest.mark.asyncio
async def test_forced_plans_are_parsed_into_index_pins(existing) -> None:
    plan = _plan_text("seek_forced_index_hint.xml").replace(
        'Index="[IX_Orders_Cust_Hinted]"', 'Index="[IX_Odd]]Name]"'
    )
    executor = RoutingExecutor(
        references=NO_REFERENCES,
        forced_plans=[{"plan_id": 770, "query_id": 77, "plan_bytes": len(plan) * 2, "query_plan": plan}],
    )

    rec = _unused_drop(await WorkloadIndexAdvisor(executor).review("appdb", **LONG))

    forced_call = next(call for call in executor.calls if call[0] == "forced_plans")
    assert forced_call[2][0] == module.MAX_FORCED_PLANS + 1
    assert "index_named_by_hint_or_forced_plan" in rec["blockers"]
    assert rec["ddl"] is None and rec["rollback_ddl"] is None
    assert any("plan 770" in step and "query 77" in step for step in rec["prerequisites"])


@pytest.mark.asyncio
async def test_forced_plan_read_failure_is_a_gap_and_caps_cleanup_confidence(existing) -> None:
    executor = RoutingExecutor(references=NO_REFERENCES, forced_plans=PermissionError("denied"))

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    rec = _unused_drop(report)
    assert rec["confidence"] == "medium"
    assert "forced_plan_dependency_check_incomplete" in rec["blockers"]
    assert rec["ddl"] is not None
    assert any(gap.startswith("Forced Query Store plans could not be read") for gap in report["gaps"])
    assert report["result_status"] == "ok"


@pytest.mark.asyncio
async def test_forced_plans_over_the_count_or_size_cap_leave_pin_checks_incomplete(existing) -> None:
    plan = _plan_text("seek_residual_lookup_sort.xml")
    too_many = [
        {"plan_id": n, "query_id": n, "plan_bytes": len(plan) * 2, "query_plan": plan}
        for n in range(module.MAX_FORCED_PLANS + 1)
    ]
    too_big = [{"plan_id": 1, "query_id": 1, "plan_bytes": module.MAX_FORCED_PLAN_XML_BYTES + 2, "query_plan": None}]

    for rows, phrase in ((too_many, "forced Query Store plans exist"), (too_big, "too large")):
        report = await WorkloadIndexAdvisor(
            RoutingExecutor(references=NO_REFERENCES, forced_plans=rows)
        ).review("appdb", **LONG)

        assert "forced_plan_dependency_check_incomplete" in _unused_drop(report)["blockers"]
        assert any(phrase in gap for gap in report["gaps"])


@pytest.mark.asyncio
async def test_index_hint_in_a_module_pins_the_index(existing) -> None:
    executor = RoutingExecutor(
        references=NO_REFERENCES,
        hint_modules=[
            {
                "object_id": 1234,
                "module_definition": "CREATE PROCEDURE dbo.p AS SELECT OrderID FROM Sales.Orders WITH (INDEX([IX_Odd]]Name]));",
            }
        ],
    )

    rec = _unused_drop(await WorkloadIndexAdvisor(executor).review("appdb", **LONG))

    assert "index_named_by_hint_or_forced_plan" in rec["blockers"]
    assert rec["ddl"] is None
    assert any("module object_id 1234" in step for step in rec["prerequisites"])


@pytest.mark.parametrize(
    ("hint", "same_name_elsewhere"),
    [
        # Default Azure SQL collation is case-insensitive, so this hint runs today.
        ("INDEX([ix_odd]]name])", False),
        # The same index name on two tables, and no TABLE HINT to say which.
        ("INDEX([IX_Odd]]Name])", True),
    ],
)
@pytest.mark.asyncio
async def test_hint_that_matches_no_single_index_withholds_drop_ddl(existing, hint, same_name_elsewhere) -> None:
    if same_name_elsewhere:
        existing.append(_index("Sales", "OrderLines", "IX_Odd]Name", ("Quantity",), index_id=4, reads=500))
    executor = RoutingExecutor(
        references=NO_REFERENCES,
        hint_modules=[
            {
                "object_id": 1234,
                "module_definition": f"CREATE PROCEDURE dbo.p AS SELECT OrderID FROM Sales.Orders o WITH ({hint});",
            }
        ],
    )

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    rec = _unused_drop(report)
    assert "unresolved_index_hint" in rec["blockers"]
    assert rec["confidence"] == "low"
    assert rec["ddl"] is None and rec["rollback_ddl"] is None
    assert any("module definitions match no single index" in gap for gap in report["gaps"])


@pytest.mark.asyncio
async def test_unreadable_hint_source_caps_cleanup_confidence(existing) -> None:
    executor = RoutingExecutor(references=NO_REFERENCES, hint_modules=PermissionError("denied"))

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    rec = _unused_drop(report)
    assert rec["confidence"] == "medium"
    assert "hint_reference_check_incomplete" in rec["blockers"]
    assert any("module_definitions_permission_or_version_unavailable" in gap for gap in report["gaps"])


@pytest.mark.asyncio
async def test_query_store_coverage_is_read_and_its_failure_is_a_gap(existing) -> None:
    held = RoutingExecutor(
        coverage=[
            {
                "effective_start_utc": datetime(2026, 9, 28, 12, 0),
                "effective_end_utc": datetime(2026, 10, 1, 12, 0),
                "interval_count": 72,
                "oldest_interval_start_utc": datetime(2026, 9, 28, 12, 0),
            }
        ]
    )
    failed = RoutingExecutor(coverage=PermissionError("denied"))

    short = await WorkloadIndexAdvisor(held).review("appdb", lookback_days=30, now=NOW)
    unknown = await WorkloadIndexAdvisor(failed).review("appdb", lookback_days=30, now=NOW)

    coverage_call = next(call for call in held.calls if call[0] == "coverage")
    assert coverage_call[2] == ["2026-10-01T12:00:00", "2026-09-01T12:00:00"]
    assert short["window"]["query_store_effective_days"] == pytest.approx(3.0)
    assert short["window"]["query_store_oldest_interval_utc"] == "2026-09-28T12:00:00Z"
    assert any("Query Store holds 3.0 of the requested 30 day(s)" in gap for gap in short["gaps"])
    assert unknown["window"]["query_store_effective_days"] is None
    assert any(gap.startswith("Query Store window coverage could not be read") for gap in unknown["gaps"])
    assert unknown["result_status"] == "ok"


@pytest.mark.asyncio
async def test_query_store_options_include_the_size_based_cleanup_mode(existing) -> None:
    options = dict(RoutingExecutor().responses["query_store"][0], size_based_cleanup_mode_desc="AUTO")

    report = await WorkloadIndexAdvisor(RoutingExecutor(query_store=[options])).review("appdb", now=NOW)

    assert "size_based_cleanup_mode_desc" in module.QUERY_STORE_OPTIONS_SQL
    assert report["query_store"]["size_based_cleanup_mode"] == "AUTO"


@pytest.mark.asyncio
async def test_optional_dependency_reads_run_concurrently(existing) -> None:
    executor = RoutingExecutor(references=NO_REFERENCES)
    executor.delays = {"references": 0.05, "forced_plans": 0.05, "coverage": 0.05, "hint_query_text": 0.05}

    await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    assert executor.peak_in_flight == 4


@pytest.mark.asyncio
async def test_slow_optional_read_becomes_a_gap_and_caps_cleanup_confidence(existing) -> None:
    executor = RoutingExecutor(references=NO_REFERENCES)
    executor.delays = {"forced_plans": 5.0}

    report = await asyncio.wait_for(
        WorkloadIndexAdvisor(executor, optional_read_timeout_seconds=0.05).review("appdb", **LONG),
        timeout=2.0,
    )

    rec = _unused_drop(report)
    assert rec["confidence"] == "medium"
    assert "forced_plan_dependency_check_incomplete" in rec["blockers"]
    assert any("forced Query Store plan read timed out after 0.05 s" in gap for gap in report["gaps"])
    assert report["result_status"] == "ok"


@pytest.mark.asyncio
async def test_optional_reads_are_bounded_by_the_configured_query_timeout(existing) -> None:
    # An operator who sets AZURE_SQL_QUERY_TIMEOUT_SECONDS for a large Query Store
    # gives these reads the same budget; a fixed bound would drop all drop advice.
    executor = RoutingExecutor(references=NO_REFERENCES)
    executor.config = SimpleNamespace(query_timeout_seconds=0.05)
    executor.delays = {"forced_plans": 5.0}

    report = await asyncio.wait_for(WorkloadIndexAdvisor(executor).review("appdb", **LONG), timeout=2.0)

    assert any("forced Query Store plan read timed out after 0.05 s" in gap for gap in report["gaps"])


@pytest.mark.asyncio
async def test_query_store_off_is_a_precondition_with_the_enabling_statement(existing) -> None:
    executor = RoutingExecutor(
        query_store=[{"actual_state_desc": "OFF", "desired_state_desc": "OFF", "query_capture_mode_desc": "ALL"}]
    )

    report = await WorkloadIndexAdvisor(executor).review("appdb", now=NOW)

    assert report["result_status"] == "precondition"
    assert report["remediation"] == (
        "ALTER DATABASE CURRENT SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE);"
    )
    assert all(call[0] != "workload" for call in executor.calls)
    assert all(call[0] != "references" for call in executor.calls)


@pytest.mark.asyncio
async def test_optional_evidence_failure_becomes_a_gap_not_a_failure(existing) -> None:
    executor = RoutingExecutor(selectivity=PermissionError("SELECT permission denied"))

    report = await WorkloadIndexAdvisor(executor).review("appdb", now=NOW)

    assert report["result_status"] == "ok"
    assert any(gap.startswith("Statistics histograms could not be read") for gap in report["gaps"])


@pytest.mark.xfail(
    "sys.dm_db_index_usage_stats" not in SERVER_STATE_GATED_DMVS,
    reason="azure_tier.SERVER_STATE_GATED_DMVS does not list sys.dm_db_index_usage_stats yet",
    strict=True,
)
@pytest.mark.asyncio
async def test_existing_index_metadata_failure_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    async def failing_collect(executor, database_name, **kwargs):
        raise PermissionError("VIEW DEFINITION permission denied")

    monkeypatch.setattr(module, "collect_existing_indexes", failing_collect)

    report = await WorkloadIndexAdvisor(RoutingExecutor()).review("appdb", now=NOW)

    assert report["result_status"] == "unavailable"
    assert report["recommendations"] == []
    # Usage counters need more than VIEW DATABASE STATE on Basic, S0, S1 and in
    # elastic pools (Microsoft Learn, sys.dm_db_index_usage_stats Permissions).
    reason = report["result_status_reason"]
    assert "VIEW DEFINITION" in reason
    assert "##MS_ServerStateReader##" in reason
    assert "Basic, S0, S1 and elastic-pool" in reason


@pytest.mark.asyncio
async def test_table_filter_requires_a_schema(existing) -> None:
    with pytest.raises(ValueError, match="schema_name"):
        await WorkloadIndexAdvisor(RoutingExecutor()).review("appdb", table_names=["Orders"], now=NOW)


@pytest.mark.asyncio
async def test_table_filter_narrows_detail_reads_and_output(existing) -> None:
    executor = RoutingExecutor()

    report = await WorkloadIndexAdvisor(executor).review(
        "appdb", schema_name="Sales", table_names=["OrderLines"], now=NOW
    )

    assert {t["table"] for t in report["tables"]} == {"OrderLines"}
    column_call = next(call for call in executor.calls if call[0] == "columns")
    assert "IN (2)" in column_call[1]
