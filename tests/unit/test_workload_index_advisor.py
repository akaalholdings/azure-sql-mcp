from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from azure_sql_mcp import workload_index_advisor as module
from azure_sql_mcp.workload_index_advisor import WorkloadIndexAdvisor
from azure_sql_mcp.workload_index_advisor import resolve_window
from azure_sql_mcp.workload_index_advisor import workload_sql
from tests.unit.test_index_advisor import _index
from tests.unit.test_index_advisor import _orders_clustered
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
            "edition": [{"edition": "GeneralPurpose"}],
            "geo_links": [{"geo_replication_link_count": 0}],
            # Read only when a test leaves collect_existing_indexes unpatched.
            "index_metadata": [],
            "engine_start": [{"engine_start_time_utc": "2026-08-01T00:00:00", "engine_identity": "srv"}],
            "protection": [],
        }
        self.responses.update(overrides)
        self.delays: dict[str, float] = {}
        self.calls: list[tuple[str, str, list[Any] | None]] = []
        # (key, started, ended) per read, to assert which reads overlap.
        self.spans: list[tuple[str, float, float]] = []
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
        elif "DATABASEPROPERTYEX(DB_NAME(), 'Edition')" in query:
            key = "edition"
        elif "FROM sys.dm_geo_replication_link_status" in query:
            key = "geo_links"
        elif "sys.geo_replication_links" in query:
            # A master catalog view: absent from the user database MCP reads.
            raise RuntimeError("Invalid object name 'sys.geo_replication_links'. (208)")
        elif "FROM sys.dm_os_sys_info" in query:
            key = "engine_start"
        elif "child_foreign_key_ids" in query:
            key = "protection"
        elif "FROM sys.indexes AS i" in query:
            key = "index_metadata"
        else:
            raise AssertionError(f"unexpected query: {query[:80]}")
        self.calls.append((key, query, list(params) if params is not None else None))
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        started = time.perf_counter()
        try:
            await asyncio.sleep(self.delays.get(key, 0.001))
        finally:
            self.in_flight -= 1
            self.spans.append((key, started, time.perf_counter()))
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
    assert "SUM(ps.total_cpu_us) OVER (PARTITION BY ps.query_id) AS query_objective" in workload_sql("cpu")
    assert "SUM(ps.total_logical_reads) OVER (PARTITION BY ps.query_id) AS query_objective" in workload_sql(
        "logical_reads"
    )
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


_ORDERS = 'Database="[appdb]" Schema="[Sales]" Table="[Orders]"'


def _showplan(relop: str) -> str:
    return (
        '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">'
        "<BatchSequence><Batch><Statements><StmtSimple><QueryPlan>"
        f"{relop}</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"
    )


# Azure narrow INSERT plan: the Clustered Index Insert lists every NC index it
# maintains in its Object list. IX_Odd]Name is written, never read.
_INSERT_PLAN = _showplan(
    '<RelOp NodeId="0" PhysicalOp="Clustered Index Insert" LogicalOp="Insert"><Update>'
    f'<Object {_ORDERS} Index="[PK_Orders]" IndexKind="Clustered"/>'
    f'<Object {_ORDERS} Index="[IX_Odd]]Name]" IndexKind="NonClustered"/>'
    "</Update></RelOp>"
)
_SEEK_PLAN = _showplan(
    '<RelOp NodeId="0" PhysicalOp="Index Seek" LogicalOp="Index Seek"><IndexScan>'
    f'<Object {_ORDERS} Index="[IX_Odd]]Name]" IndexKind="NonClustered"/>'
    "</IndexScan></RelOp>"
)


def _plan_match(plan_id: int, plan: str | None, *, forced: bool = False, stats: bool = False) -> dict[str, Any]:
    return {
        "ref_id": 0,
        "plan_id": plan_id,
        "query_id": plan_id // 10,
        "is_forced_plan": int(forced),
        "index_match": 1,
        "stats_match": int(stats),
        "query_plan": plan,
    }


@pytest.mark.asyncio
async def test_write_maintenance_plan_is_not_a_query_store_reference(existing) -> None:
    # Live Azure: an index only INSERT/UPDATE plans maintain got
    # "in Query Store plans ... Keep it" here while the portfolio said
    # observe/drop. Both tools now count only plans that read the index.
    executor = RoutingExecutor(references=[_plan_match(70, _INSERT_PLAN), _plan_match(80, _INSERT_PLAN)])

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    rec = _unused_drop(report)
    assert "unused_by_dmv_but_in_query_store_plans" not in rec["reason_codes"]
    assert "no_query_store_plan_reference_in_window" in rec["reason_codes"]
    assert _odd_name_plan_references(report) == 0


@pytest.mark.asyncio
async def test_read_plan_is_a_query_store_reference(existing) -> None:
    executor = RoutingExecutor(references=[_plan_match(70, _INSERT_PLAN), _plan_match(80, _SEEK_PLAN)])

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    [rec] = [r for r in report["recommendations"] if r["index_name"] == "IX_Odd]Name"]
    assert rec["reason_codes"] == ["unused_by_dmv_but_in_query_store_plans"]
    assert _odd_name_plan_references(report) == 1


@pytest.mark.asyncio
async def test_forced_plan_that_only_maintains_the_index_keeps_it(existing) -> None:
    # A drop silently stops the forcing (no force failure is recorded).
    executor = RoutingExecutor(references=[_plan_match(70, _INSERT_PLAN, forced=True)])

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    [rec] = [r for r in report["recommendations"] if r["index_name"] == "IX_Odd]Name"]
    assert rec["action"] != "drop_index"
    assert rec["reason_codes"] == ["unused_by_dmv_but_in_query_store_plans"]


@pytest.mark.asyncio
async def test_unread_plan_counts_as_a_reference_and_is_a_gap(existing) -> None:
    # Plan XML over the byte budget: its operators are unknown, so it stays use.
    executor = RoutingExecutor(references=[_plan_match(70, None)])

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    assert _odd_name_plan_references(report) == 1
    assert any("could not be read" in gap and "count as references" in gap for gap in report["gaps"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("plan_bytes", "budget_named"),
    [(2_000, True), (module.MAX_FORCED_PLAN_XML_BYTES + 2, False)],
)
async def test_unread_plan_gap_says_when_the_total_plan_budget_was_reached(
    existing, plan_bytes: int, budget_named: bool
) -> None:
    # A small plan comes back without XML only when the total read budget ran
    # out: the reader must see that the keep is a budget limit, not plan size.
    executor = RoutingExecutor(references=[{**_plan_match(70, None), "plan_bytes": plan_bytes}])

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    assert _odd_name_plan_references(report) == 1
    [gap] = [gap for gap in report["gaps"] if "count as references" in gap]
    assert ("plan read budget" in gap) is budget_named


def _seek(object_attributes: str) -> str:
    return (
        '<RelOp PhysicalOp="Index Seek" LogicalOp="Index Seek"><IndexScan>'
        f"<Object {object_attributes}/></IndexScan></RelOp>"
    )


@pytest.mark.asyncio
async def test_plan_past_the_parser_reference_cap_counts_as_a_reference(existing) -> None:
    # The parser keeps the first 500 index references; a seek of the unused
    # index after them is dropped, so the plan was not fully read.
    others = "".join(
        _seek(f'Database="[appdb]" Schema="[dbo]" Table="[t{n}]" Index="[ix_{n}]"') for n in range(600)
    )
    target = _seek(f'{_ORDERS} Index="[IX_Odd]]Name]"')
    plan = _showplan(f'<RelOp PhysicalOp="Concatenation" LogicalOp="Concatenation">{others}{target}</RelOp>')
    executor = RoutingExecutor(references=[_plan_match(70, plan)])

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    assert _odd_name_plan_references(report) == 1
    assert any("could not be read" in gap and "count as references" in gap for gap in report["gaps"])


@pytest.mark.asyncio
async def test_plan_with_an_index_object_of_unknown_table_counts_as_a_reference(existing) -> None:
    # The parser drops an Object with an Index but no table identity, so the
    # plan may seek the unused index without the advisor seeing it.
    plan = _showplan(_seek('Index="[IX_Odd]]Name]"'))
    executor = RoutingExecutor(references=[_plan_match(70, plan)])

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    assert _odd_name_plan_references(report) == 1


@pytest.mark.asyncio
async def test_write_plan_from_an_indexed_table_variable_is_not_a_reference(existing) -> None:
    # Live Azure: INSERT ... SELECT FROM @t names @t's PK with no Database or
    # Schema. The unused index is fully identified as written, never read.
    plan = _showplan(
        '<RelOp PhysicalOp="Clustered Index Insert" LogicalOp="Insert"><Update>'
        f'<Object {_ORDERS} Index="[PK_Orders]" IndexKind="Clustered"/>'
        f'<Object {_ORDERS} Index="[IX_Odd]]Name]" IndexKind="NonClustered"/>'
        '<RelOp PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan"><IndexScan>'
        '<Object Table="[@t]" Index="[PK__#A52DA50__3213E83F]" Alias="[t]"/>'
        "</IndexScan></RelOp></Update></RelOp>"
    )
    executor = RoutingExecutor(references=[_plan_match(70, plan)])

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    assert _odd_name_plan_references(report) == 0
    assert not any("could not be read" in gap for gap in report["gaps"])


@pytest.mark.asyncio
async def test_reference_plans_are_parsed_off_the_event_loop(existing, monkeypatch) -> None:
    threads: list[int] = []
    parse = module.parse_showplan_index_evidence

    def recording_parse(*args: Any, **kwargs: Any) -> dict[str, Any]:
        threads.append(threading.get_ident())
        return parse(*args, **kwargs)

    monkeypatch.setattr(module, "parse_showplan_index_evidence", recording_parse)
    executor = RoutingExecutor(references=[_plan_match(80, _SEEK_PLAN)])

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    assert threads and threading.get_ident() not in threads
    assert _odd_name_plan_references(report) == 1


@pytest.mark.asyncio
async def test_slow_reference_plan_parsing_is_bounded_by_the_read_timeout(existing, monkeypatch) -> None:
    # Large plans parse for seconds; the parse must not hold the event loop
    # past optional_read_timeout_seconds, and it stops once the read is dropped.
    parsed: list[int] = []
    parse = module.parse_showplan_index_evidence

    def slow_parse(*args: Any, **kwargs: Any) -> dict[str, Any]:
        parsed.append(1)
        time.sleep(0.05)
        return parse(*args, **kwargs)

    monkeypatch.setattr(module, "parse_showplan_index_evidence", slow_parse)
    executor = RoutingExecutor(references=[_plan_match(n * 10, _SEEK_PLAN) for n in range(1, 41)])

    started = time.monotonic()
    report = await WorkloadIndexAdvisor(executor, optional_read_timeout_seconds=0.1).review("appdb", **LONG)
    elapsed = time.monotonic() - started
    await asyncio.sleep(0.2)

    assert elapsed < 1.0
    assert any("Query Store plan reference read timed out after 0.1 s" in gap for gap in report["gaps"])
    assert len(parsed) < 20


@pytest.mark.asyncio
async def test_reference_read_skips_internal_queries_and_caps_plan_bytes(existing) -> None:
    # ALTER INDEX REBUILD and UPDATE STATISTICS are internal queries whose
    # plans scan the index; they are maintenance, not use (same as portfolio).
    executor = RoutingExecutor(references=NO_REFERENCES)

    await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    reference_call = next(call for call in executor.calls if call[0] == "references")
    assert "q.is_internal_query = 0" in reference_call[1]
    assert reference_call[2][-3:] == [
        module.MAX_FORCED_PLAN_XML_BYTES,
        module.MAX_FORCED_PLAN_XML_BYTES,
        module.MAX_INDEX_REFERENCE_PLAN_TOTAL_BYTES,
    ]
    # This read holds its plan XML while the forced-plan read holds its own.
    assert module.MAX_INDEX_REFERENCE_PLAN_TOTAL_BYTES < module.MAX_FORCED_PLAN_TOTAL_BYTES


@pytest.mark.asyncio
async def test_reference_check_patterns_escape_closing_brackets(existing) -> None:
    executor = RoutingExecutor(references=[_plan_match(70, _SEEK_PLAN), _plan_match(80, _SEEK_PLAN)])

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
NO_REFERENCES: list[dict[str, Any]] = []


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
    stats_only = {**_plan_match(70, None, stats=True), "index_match": 0}
    executor = RoutingExecutor(references=[stats_only, {**stats_only, "plan_id": 80}])

    rec = _unused_drop(await WorkloadIndexAdvisor(executor).review("appdb", **LONG))

    reference_call = next(call for call in executor.calls if call[0] == "references")
    assert "stats_match" in reference_call[1]
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
async def test_hint_differing_in_case_or_naming_two_indexes_pins_the_index(existing, hint, same_name_elsewhere) -> None:
    # Dropping an index such a hint names fails the query with Msg 308, so the
    # hint pins every index it can name: no executable DROP.
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
    assert "index_named_by_hint_or_forced_plan" in rec["blockers"]
    assert rec["confidence"] == "low"
    assert rec["ddl"] is None and rec["rollback_ddl"] is None
    assert any("module object_id 1234" in step for step in rec["prerequisites"])


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


@pytest.fixture
def clustered_only(monkeypatch: pytest.MonkeyPatch):
    # Only the clustered keys exist, so a covered lookup is a create_index.
    indexes = [
        _orders_clustered(),
        _index("Sales", "OrderLines", "PK_Sales_OrderLines", ("OrderLineID",), index_id=1, type_code=1, primary_key=True),
    ]
    for index in indexes:
        index.usage_context["engine_start_time_utc"] = "2026-08-01T00:00:00Z"

    async def fake_collect(executor, database_name, **kwargs):
        return indexes

    monkeypatch.setattr(module, "collect_existing_indexes", fake_collect)
    return indexes


def _computed(column: str, is_indexable: int, is_deterministic: int) -> RoutingExecutor:
    """One lookup query on Sales.Orders, where ``column`` is a computed column."""

    rows = [
        dict(row, is_computed=1, is_indexable=is_indexable, is_deterministic=is_deterministic)
        if row["column_name"] == column
        else row
        for row in RoutingExecutor().responses["columns"]
    ]
    return RoutingExecutor(columns=rows, workload=[_workload_row(1, "seek_residual_lookup_sort.xml", 900_000)])


def _orders_create(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [r for r in report["recommendations"] if r["action"] == "create_index" and r["table"] == "Orders"]


@pytest.mark.asyncio
async def test_columns_read_computed_column_indexability_from_columnproperty(clustered_only) -> None:
    executor = _computed("Comments", 1, 1)

    await WorkloadIndexAdvisor(executor).review("appdb", now=NOW)

    column_sql = next(call[1] for call in executor.calls if call[0] == "columns")
    assert "COLUMNPROPERTY(c.object_id, c.name, 'IsIndexable') AS is_indexable" in column_sql
    assert "COLUMNPROPERTY(c.object_id, c.name, 'IsDeterministic') AS is_deterministic" in column_sql


def _used_columns(rec: dict[str, Any]) -> list[str]:
    return [key["name"] for key in rec["key_columns"]] + list(rec["include_columns"])


# Comments is a column the lookup must INCLUDE; CustomerID is its leading key.
COMPUTED_ROLES = pytest.mark.parametrize("column", ["Comments", "CustomerID"])


@COMPUTED_ROLES
@pytest.mark.asyncio
async def test_deterministic_indexable_computed_column_is_used_in_advice(clustered_only, column) -> None:
    # Computed columns fail closed without COLUMNPROPERTY; once it says the column
    # can be indexed, the covering index for the lookup comes back.
    report = await WorkloadIndexAdvisor(_computed(column, 1, 1)).review("appdb", now=NOW)

    [rec] = _orders_create(report)
    assert column in _used_columns(rec)
    assert not any(column in gap for gap in report["gaps"])


@COMPUTED_ROLES
@pytest.mark.asyncio
async def test_nondeterministic_computed_column_is_never_used_and_is_reported(clustered_only, column) -> None:
    # SQL rejects a non-deterministic computed column as a key or INCLUDE column.
    report = await WorkloadIndexAdvisor(_computed(column, 0, 0)).review("appdb", now=NOW)

    assert not any(column in _used_columns(rec) for rec in _orders_create(report))
    [gap] = [gap for gap in report["gaps"] if f"Sales.Orders.{column}" in gap]
    assert f"{column} (not deterministic)" in gap
    assert "withheld" in gap


@pytest.mark.asyncio
async def test_deterministic_unindexable_computed_column_is_still_included(clustered_only) -> None:
    # A deterministic computed column that IsIndexable rejects (an imprecise,
    # non-persisted expression) cannot be a key but can be INCLUDEd, so the
    # lookup is still covered and the gap must not say that advice is withheld.
    report = await WorkloadIndexAdvisor(_computed("Comments", 0, 1)).review("appdb", now=NOW)

    [rec] = _orders_create(report)
    assert rec["include_columns"] == ["Comments"]
    [gap] = [gap for gap in report["gaps"] if "Sales.Orders.Comments" in gap]
    assert "Comments (not indexable)" in gap
    assert "key lookup" not in gap


@pytest.mark.parametrize(("column", "is_indexable"), [("Comments", 1), ("CustomerID", 1), ("Comments", 0)])
@pytest.mark.asyncio
async def test_computed_column_advice_requires_writer_set_options(clustered_only, column, is_indexable) -> None:
    # A key or INCLUDE computed column makes every writer of the table need the
    # indexed-view SET options, or its INSERT/UPDATE/DELETE fails with Msg 1934.
    report = await WorkloadIndexAdvisor(_computed(column, is_indexable, 1)).review("appdb", now=NOW)
    plain = await WorkloadIndexAdvisor(
        RoutingExecutor(workload=[_workload_row(1, "seek_residual_lookup_sort.xml", 900_000)])
    ).review("appdb", now=NOW)

    [rec] = _orders_create(report)
    assert column in _used_columns(rec)
    [step] = rec["prerequisites"]
    assert f"Sales.Orders.{column}" in step
    assert "QUOTED_IDENTIFIER" in step and "NUMERIC_ROUNDABORT OFF" in step and "Msg 1934" in step
    [plain_rec] = _orders_create(plain)
    assert plain_rec["prerequisites"] == []


@pytest.mark.parametrize("columns", [RuntimeError("timeout"), []], ids=["read_failed", "no_rows"])
@pytest.mark.asyncio
async def test_unread_column_metadata_is_never_reported_as_no_computed_column(clustered_only, columns) -> None:
    # Without sys.columns rows the advisor cannot know whether an INCLUDE is
    # computed; prerequisites=[] would look checked while writers may hit Msg 1934.
    report = await WorkloadIndexAdvisor(
        RoutingExecutor(columns=columns, workload=[_workload_row(1, "seek_residual_lookup_sort.xml", 900_000)])
    ).review("appdb", now=NOW)

    [rec] = _orders_create(report)
    assert "Comments" in rec["include_columns"]
    [step] = rec["prerequisites"]
    assert "Sales.Orders.Comments" in step and "not read" in step and "Msg 1934" in step


@pytest.mark.asyncio
async def test_tables_past_the_detail_cap_are_reported_as_a_gap(clustered_only, monkeypatch) -> None:
    # Past the cap no column, computed-column or selectivity read is issued, so
    # the skipped tables must show as a gap and their advice must not look checked.
    monkeypatch.setattr(module, "MAX_DETAIL_TABLES", 0)
    executor = _computed("Comments", 1, 1)

    report = await WorkloadIndexAdvisor(executor).review("appdb", now=NOW)

    assert not any(call[0] == "columns" for call in executor.calls)
    [gap] = [gap for gap in report["gaps"] if "detail limit" in gap]
    assert "1 workload table(s)" in gap and "Sales.Orders" in gap
    [rec] = _orders_create(report)
    [step] = rec["prerequisites"]
    assert "Sales.Orders.Comments" in step and "not read" in step


def _foreign_key_rows(count: int, columns_per_key: int) -> list[dict[str, Any]]:
    columns = ("StockItemID", "PackageTypeID", "Quantity")[:columns_per_key]
    return [
        {
            "foreign_key_id": 100 + n // columns_per_key,
            "foreign_key_name": f"FK_{n // columns_per_key}",
            "schema_name": "Sales",
            "table_name": "OrderLines",
            "referenced_schema": "Sales",
            "referenced_table": "Orders",
            "constraint_column_id": n % columns_per_key + 1,
            "column_name": columns[n % columns_per_key],
            "delete_referential_action_desc": "NO_ACTION",
            "is_disabled": False,
        }
        for n in range(count)
    ]


@pytest.mark.asyncio
async def test_foreign_key_list_at_cap_is_reported_as_gap() -> None:
    cap = module.MAX_FOREIGN_KEY_ROWS
    # Three columns per key: the row past the cap is the last column of a key
    # whose first two columns were read.
    executor = RoutingExecutor(foreign_keys=_foreign_key_rows(cap + 1, 3))
    gaps: list[str] = []

    foreign_keys = await WorkloadIndexAdvisor(executor)._foreign_keys("appdb", gaps)

    fk_call = next(call for call in executor.calls if call[0] == "foreign_keys")
    assert fk_call[2] == [cap + 1]
    assert any(f"{cap} foreign key column rows" in gap and "not checked" in gap for gap in gaps)
    # A key judged on part of its columns gives a wrong supporting-index finding.
    assert all(len(fk.columns) == 3 for fk in foreign_keys)
    assert len(foreign_keys) == cap // 3

    complete = RoutingExecutor(foreign_keys=_foreign_key_rows(cap, 2))
    complete_gaps: list[str] = []
    assert len(await WorkloadIndexAdvisor(complete)._foreign_keys("appdb", complete_gaps)) == cap // 2
    assert complete_gaps == []


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "blocker"),
    [
        ({"edition": [{"edition": "BusinessCritical"}]}, "readable_replica_usage_unobserved"),
        ({"edition": [{"edition": "Hyperscale"}]}, "readable_replica_usage_unobserved"),
        ({"geo_links": [{"geo_replication_link_count": 1}]}, "readable_replica_usage_unobserved"),
        ({"geo_links": PermissionError("denied")}, "readable_replica_check_unavailable"),
    ],
)
async def test_readable_replica_caps_unused_drop_in_workload_review(
    existing, overrides: dict[str, Any], blocker: str
) -> None:
    # Read scale-out is on by default for Premium and Business Critical, and
    # Hyperscale and geo links add readable replicas. Their reads never reach
    # the primary's usage counters or Query Store.
    executor = RoutingExecutor(references=NO_REFERENCES, **overrides)

    rec = _unused_drop(await WorkloadIndexAdvisor(executor).review("appdb", **LONG))

    assert rec["confidence"] == "medium"
    assert blocker in rec["blockers"]


@pytest.mark.asyncio
async def test_general_purpose_without_geo_link_keeps_a_high_confidence_drop(existing) -> None:
    # General Purpose has no readable secondary unless a geo link exists, and
    # the per-database link DMV says there is none. The replica caveat must
    # not cap every unused drop on the most common tier.
    executor = RoutingExecutor(references=NO_REFERENCES)

    rec = _unused_drop(await WorkloadIndexAdvisor(executor).review("appdb", **LONG))

    assert rec["confidence"] == "high"
    assert not {"readable_replica_usage_unobserved", "readable_replica_check_unavailable"} & set(rec["blockers"])
    assert "geo_links" in [call[0] for call in executor.calls]


def _add_out_of_scope_unused(existing: list, count: int) -> None:
    for position in range(count):
        index = _index("Archive", "Big", f"IX_Archive_{position}", ("OrderID",), index_id=10 + position, reads=0)
        index.usage_context["engine_start_time_utc"] = "2026-08-01T00:00:00Z"
        existing.append(index)


def _odd_name_plan_references(report: dict[str, Any]) -> Any:
    [summary] = [
        index
        for table in report["tables"]
        for index in table["existing_indexes"]
        if index["name"] == "IX_Odd]Name"
    ]
    return summary["query_store_plan_references"]


def _reference_names(executor: RoutingExecutor) -> list[str]:
    reference_call = next(call for call in executor.calls if call[0] == "references")
    return [param for param in reference_call[2] if str(param).startswith('Index="')]


@pytest.mark.asyncio
async def test_schema_scoped_review_checks_query_store_references_for_its_own_indexes_only(existing) -> None:
    # The reference read costs (index names) x (plan bytes in the window). A
    # review of one schema must not pay for, or be capped by, every other
    # schema's unused indexes: 45 of them pushed IX_Odd]Name past the 40-name cap.
    _add_out_of_scope_unused(existing, 45)
    executor = RoutingExecutor(references=NO_REFERENCES)

    report = await WorkloadIndexAdvisor(executor).review("appdb", schema_name="Sales", **LONG)

    names = _reference_names(executor)
    assert 'Index="[IX_Odd]]Name]"' in names
    assert not any("IX_Archive_" in name for name in names)
    assert not any("checked for 40 of" in gap for gap in report["gaps"])
    assert _odd_name_plan_references(report) == 0


@pytest.mark.asyncio
async def test_table_scoped_review_checks_query_store_references_for_that_table_only(existing) -> None:
    _add_out_of_scope_unused(existing, 3)
    existing.append(_index("Sales", "OrderLines", "IX_Lines_Unused", ("StockItemID",), index_id=9, reads=0))
    executor = RoutingExecutor(references=NO_REFERENCES)

    await WorkloadIndexAdvisor(executor).review("appdb", schema_name="Sales", table_names=["Orders"], **LONG)

    names = _reference_names(executor)
    assert 'Index="[IX_Odd]]Name]"' in names
    assert not any("IX_Archive_" in name or "IX_Lines_Unused" in name for name in names)


class _PerNameCostExecutor(RoutingExecutor):
    # Live Azure: the reference read grew with the number of names (2 names
    # 7.6 s, 23 names 81.7 s) and timed out for a one-schema review.
    async def fetch_all(self, database_name: str, query: str, params=None, **kwargs: Any):
        if "FROM (VALUES" in query:
            await asyncio.sleep(0.01 * (len(params) - 1) / 3)
        return await super().fetch_all(database_name, query, params, **kwargs)


@pytest.mark.asyncio
async def test_out_of_scope_indexes_do_not_time_out_the_scoped_reference_read(existing) -> None:
    _add_out_of_scope_unused(existing, 30)
    executor = _PerNameCostExecutor(references=NO_REFERENCES)

    report = await WorkloadIndexAdvisor(executor, optional_read_timeout_seconds=0.1).review(
        "appdb", schema_name="Sales", **LONG
    )

    assert not any("plan reference read timed out" in gap for gap in report["gaps"])
    assert _odd_name_plan_references(report) == 0


def test_reference_budget_takes_plans_in_turns_per_index() -> None:
    # One write-heavy index with many plans must not use up the whole plan-XML
    # budget and leave every other index "unread" (a false keep). Each index
    # gets its next-smallest plan in turn: the turn number is per index, and
    # the running budget sum is ordered by that turn first.
    import sqlglot
    from sqlglot import exp

    from azure_sql_mcp.workload_index_advisor import _INDEX_REFERENCE_SQL_TEMPLATE

    tree = sqlglot.parse_one(
        _INDEX_REFERENCE_SQL_TEMPLATE.format(values="(1, N'a', N'b')"), read="tsql"
    )
    ctes = {cte.alias: cte for cte in tree.find_all(exp.CTE)}
    turn_window = next(
        window
        for window in ctes["turns"].find_all(exp.Window)
        if window.parent is not None and window.parent.alias == "plan_turn"
    )
    partitions = [column.name for column in turn_window.args["partition_by"]]
    assert partitions[0] == "ref_id"
    turn_order = [item.this.name for item in turn_window.args["order"].expressions]
    assert turn_order[0] == "plan_bytes"
    budget_window = next(ctes["budgeted"].find_all(exp.Window))
    budget_order = [item.this.name for item in budget_window.args["order"].expressions]
    assert budget_order[0] == "plan_turn"


@pytest.mark.asyncio
async def test_scoped_review_ranks_the_scope_before_the_top_query_cap(existing) -> None:
    # Live (2026-11-04): a schema-scoped review took the database-wide top 100
    # queries and only then applied the scope. On a busy database the scope's
    # own queries fell outside the cap, and its index advice silently vanished.
    executor = RoutingExecutor()

    await WorkloadIndexAdvisor(executor).review(
        "appdb", lookback_days=7, now=NOW, schema_name="sales", table_names=["Orders", "Order]Lines"]
    )

    sql, params = next((query, params) for _db, query, params in executor.calls if "WITH plan_stats AS" in query)
    # in_scope is decided per query before the rank, and the rank puts the
    # scope's queries first, so the cap counts only them.
    windowed = sql[sql.index("windowed AS (") : sql.index("ranked AS (")]
    assert "CHARINDEX(?, CAST(sp.query_plan AS nvarchar(max)))" in windowed
    assert "OVER (PARTITION BY ps.query_id) AS in_scope" in windowed
    assert "ORDER BY w.in_scope DESC, w.query_objective DESC, w.query_id" in sql
    assert "r.in_scope = 1 AND r.workload_rank <= ?" in sql
    assert params[2:4] == ['Schema="[sales]" Table="[Orders]"', 'Schema="[sales]" Table="[Order]]Lines]"']
    assert params[4:] == [100, 1]  # top_queries, then plans_per_query last
    # Workload totals stay database-wide: they are windows over every plan row.
    assert "SUM(ps.total_cpu_us) OVER () AS workload_cpu_us" in windowed


@pytest.mark.asyncio
async def test_unscoped_review_reads_no_plan_xml_to_rank_queries(existing) -> None:
    executor = RoutingExecutor()

    await WorkloadIndexAdvisor(executor).review("appdb", lookback_days=7, now=NOW)

    sql, params = next((query, params) for _db, query, params in executor.calls if "WITH plan_stats AS" in query)
    assert "CHARINDEX" not in sql
    assert len(params) == 4


def test_scope_pattern_escapes_names_the_way_showplan_writes_them() -> None:
    from azure_sql_mcp.workload_index_advisor import _scope_plan_patterns

    assert _scope_plan_patterns("s&p", None) == ['Schema="[s&amp;p]"']
    assert _scope_plan_patterns("dbo", ['a"b<c']) == ['Schema="[dbo]" Table="[a&quot;b&lt;c]"']
    assert _scope_plan_patterns(None, None) == []


# A seek on Orders.CustomerID with a residual LIKE N'%late%' on Comments: the seek
# limits the rows, so the wildcard is no rewrite opportunity. Live Azure listed it.
_SEEK_WILDCARD_PLAN = _showplan(
    '<RelOp NodeId="0" PhysicalOp="Index Seek" LogicalOp="Index Seek" EstimatedTotalSubtreeCost="1"><IndexScan>'
    f'<Object {_ORDERS} Index="[FK_Sales_Orders_CustomerID]" IndexKind="NonClustered"/>'
    '<SeekPredicates><SeekPredicateNew><SeekKeys><Prefix ScanType="EQ"><RangeColumns>'
    f'<ColumnReference {_ORDERS} Column="CustomerID"/></RangeColumns><RangeExpressions><ScalarOperator>'
    '<Identifier><ColumnReference Column="@id"/></Identifier></ScalarOperator></RangeExpressions></Prefix>'
    "</SeekKeys></SeekPredicateNew></SeekPredicates>"
    '<Predicate><ScalarOperator><Intrinsic FunctionName="like"><ScalarOperator><Identifier>'
    f'<ColumnReference {_ORDERS} Column="Comments"/></Identifier></ScalarOperator>'
    "<ScalarOperator><Const ConstValue=\"N'%late%'\"/></ScalarOperator></Intrinsic></ScalarOperator></Predicate>"
    "</IndexScan></RelOp>"
)


@pytest.mark.asyncio
async def test_rewrite_opportunities_list_wildcards_only_where_the_access_scans(existing) -> None:
    seek_row = {**_workload_row(3, "seek_residual_lookup_sort.xml", 500_000), "query_plan": _SEEK_WILDCARD_PLAN}
    executor = RoutingExecutor(
        workload=[_workload_row(2, "clustered_scan_residuals.xml", 2_000_000), seek_row]
    )

    report = await WorkloadIndexAdvisor(executor).review("appdb", lookback_days=7, now=NOW)

    found = [(item["table"], item["column"], item["pattern"], item["query_ids"]) for item in report["rewrite_opportunities"]]
    assert found == [("OrderLines", "Description", "leading_wildcard", [2])]
