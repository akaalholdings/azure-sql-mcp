from __future__ import annotations

from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp import workload_index_advisor as module
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
        }
        self.responses.update(overrides)
        self.calls: list[tuple[str, str, list[Any] | None]] = []

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
        else:
            raise AssertionError(f"unexpected query: {query[:80]}")
        self.calls.append((key, query, list(params) if params is not None else None))
        result = self.responses[key]
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
    }
    assert report["workload"]["queries_in_window"] == 40
    assert report["workload"]["analyzed_share_pct"] == pytest.approx(96.667, rel=1e-3)
    assert report["usage_counters"]["engine_start_time_utc"] == "2026-08-01T00:00:00Z"


@pytest.mark.asyncio
async def test_reference_check_patterns_escape_closing_brackets(existing) -> None:
    executor = RoutingExecutor(references=[{"ref_id": 0, "plan_references": 2}])

    report = await WorkloadIndexAdvisor(executor).review("appdb", now=NOW)

    reference_call = next(call for call in executor.calls if call[0] == "references")
    assert reference_call[2][:2] == [0, 'Index="[IX_Odd]]Name]"']
    review = [r for r in report["recommendations"] if r["index_name"] == "IX_Odd]Name"]
    assert review[0]["reason_codes"] == ["unused_by_dmv_but_in_query_store_plans"]


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
