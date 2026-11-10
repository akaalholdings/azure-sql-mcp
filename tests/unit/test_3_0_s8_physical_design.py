"""Physical design leads: compression, a unique index for a constraint plus its twin, fill factor.

Each is a lead with inert statements. The tool never runs an estimate or a rebuild.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from azure_sql_mcp import workload_index_advisor as workload_module
from azure_sql_mcp.index_advisor import build_index_advice
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.workload_index_advisor import WorkloadIndexAdvisor
from tests.unit.test_index_advisor import _index
from tests.unit.test_index_advisor import _inputs
from tests.unit.test_index_advisor import _orders_indexes
from tests.unit.test_workload_index_advisor import NOW
from tests.unit.test_workload_index_advisor import RoutingExecutor

GB_PAGES = 131_072


def _leads(report: dict[str, Any], reason: str) -> list[dict[str, Any]]:
    return [
        rec
        for rec in report["recommendations"]
        if rec["action"] == "physical_design_lead" and reason in rec["reason_codes"]
    ]


def _sized(index: ExistingIndex, pages: int, *, rows: int = 10_000_000, compression: str = "NONE") -> ExistingIndex:
    return replace(
        index,
        partition_page_counts=((1, pages),),
        partition_row_counts=((1, rows),),
        partition_compression=((1, compression),),
    )


# --- compression --------------------------------------------------------------


def test_large_uncompressed_index_gets_the_estimate_statement_and_never_a_rebuild() -> None:
    big = _sized(_index("Sales", "Orders", "IX_Orders_Big", ("OrderDate",), index_id=5), 3 * GB_PAGES)
    small = _sized(_index("Sales", "Orders", "IX_Orders_Small", ("Status",), index_id=6), 1000)
    report = build_index_advice(_inputs([], [*_orders_indexes(), big, small]))

    (lead,) = _leads(report, "large_uncompressed_rowstore_index")
    assert lead["ddl"] is None and lead["rollback_ddl"] is None
    assert [item["name"] for item in lead["indexes"]] == ["IX_Orders_Big"]
    assert lead["indexes"][0]["size_mb"] == 3072.0
    assert lead["estimate_statements"] == [
        "EXEC sys.sp_estimate_data_compression_savings @schema_name = N'Sales', @object_name = N'Orders', "
        "@index_id = 5, @partition_number = NULL, @data_compression = N'PAGE';",
        "EXEC sys.sp_estimate_data_compression_savings @schema_name = N'Sales', @object_name = N'Orders', "
        "@index_id = 5, @partition_number = NULL, @data_compression = N'ROW';",
    ]
    assert "tempdb" in lead["cost"] and "never runs it" in lead["cost"]
    assert "25%" in lead["rationale"]


def test_compressed_or_small_indexes_get_no_compression_lead() -> None:
    compressed = _sized(
        _index("Sales", "Orders", "IX_Orders_Big", ("OrderDate",), index_id=5), 3 * GB_PAGES, compression="PAGE"
    )
    report = build_index_advice(_inputs([], [*_orders_indexes(), compressed]))
    assert _leads(report, "large_uncompressed_rowstore_index") == []
    # The same index uncompressed does get it: the lead depends on the compression state.
    plain = _sized(_index("Sales", "Orders", "IX_Orders_Big", ("OrderDate",), index_id=5), 3 * GB_PAGES)
    assert len(_leads(build_index_advice(_inputs([], [*_orders_indexes(), plain])), "large_uncompressed_rowstore_index")) == 1


# --- unique constraint plus covering twin ---------------------------------------


def _constraint(referenced: bool = False) -> ExistingIndex:
    base = _index(
        "Sales", "Orders", "UQ_Orders_OrderDate", ("OrderDate",), index_id=5, unique=True, reads=40,
        protection={"coverage": "complete", **({"referenced_foreign_keys": [7]} if referenced else {})},
    )
    return replace(base, is_unique_constraint=True, constraint_name="UQ_Orders_OrderDate", constraint_type="UNIQUE_CONSTRAINT")


def _twin() -> ExistingIndex:
    return _index("Sales", "Orders", "IX_Orders_OrderDate_Cover", ("OrderDate",), index_id=6, includes=("Status",), reads=900)


def test_unique_constraint_and_its_covering_twin_become_one_unique_index_lead() -> None:
    """Two indexes on the same key are maintained on every write; one unique index with INCLUDE does both jobs."""

    report = build_index_advice(_inputs([], [*_orders_indexes(), _constraint(), _twin()]))

    (lead,) = _leads(report, "unique_index_can_replace_constraint_and_twin")
    assert lead["index_name"] == "IX_Orders_OrderDate_Cover"
    assert lead["ddl"] is None
    first, second = lead["design_statements"]
    # Uniqueness is never unenforced: the index becomes unique before the constraint goes.
    assert "CREATE UNIQUE NONCLUSTERED INDEX [IX_Orders_OrderDate_Cover]" in first
    assert "DROP_EXISTING = ON, ONLINE = ON" in first and "INCLUDE ([Status])" in first
    assert second == "ALTER TABLE [Sales].[Orders] DROP CONSTRAINT [UQ_Orders_OrderDate];"
    assert lead["rollback_statements"][0] == (
        "ALTER TABLE [Sales].[Orders] ADD CONSTRAINT [UQ_Orders_OrderDate] UNIQUE NONCLUSTERED ([OrderDate] ASC) "
        "WITH (ONLINE = ON);"
    )
    assert lead["blockers"] == ["design_lead_requires_change_review"]
    # Neither index is advised away by the ordinary cleanup.
    assert [rec for rec in report["recommendations"] if rec["action"] in {"drop_index", "consolidate_index"}] == []


def test_constraint_referenced_by_a_foreign_key_gets_no_statements() -> None:
    report = build_index_advice(_inputs([], [*_orders_indexes(), _constraint(referenced=True), _twin()]))
    (lead,) = _leads(report, "unique_index_can_replace_constraint_and_twin")
    assert "foreign_key_references_the_constraint" in lead["blockers"]
    assert lead["design_statements"] == []


# --- fill factor ----------------------------------------------------------------


def _fill(fill_factor: int) -> ExistingIndex:
    # 10,000,000 rows on 100,000 pages: 100 rows per page.
    index = _sized(_index("Sales", "Orders", "IX_Orders_Customer", ("CustomerID", "OrderDate"), index_id=5), 100_000)
    return replace(index, fill_factor=fill_factor)


def _activity(inserts: int, allocations: int) -> dict[tuple[str, str, str], dict[str, int]]:
    return {("sales", "orders", "ix_orders_customer"): {"leaf_insert_count": inserts, "leaf_allocation_count": allocations}}


def _fill_report(fill_factor: int, activity: dict[tuple[str, str, str], dict[str, int]] | None) -> dict[str, Any]:
    inputs = _inputs([], [*_orders_indexes(), _fill(fill_factor)])
    inputs.index_leaf_activity = activity
    return build_index_advice(inputs)


def test_page_splits_on_a_full_index_raise_a_fill_factor_lead_with_the_evidence() -> None:
    """1,000,000 inserts need 10,000 new pages at 100 rows per page; 60,000 allocations means 50,000 splits."""

    (lead,) = _leads(_fill_report(0, _activity(1_000_000, 60_000)), "page_splits_measured")

    assert lead["index_name"] == "IX_Orders_Customer"
    assert lead["evidence"]["estimated_page_splits"] == 50_000
    assert lead["ddl"] == (
        "ALTER INDEX [IX_Orders_Customer] ON [Sales].[Orders] REBUILD WITH (FILLFACTOR = 90, ONLINE = ON);"
    )
    assert lead["rollback_ddl"] == (
        "ALTER INDEX [IX_Orders_Customer] ON [Sales].[Orders] REBUILD WITH (FILLFACTOR = 100, ONLINE = ON);"
    )

    # No advice without the evidence. Appends only: allocations match what the inserts need.
    assert _leads(_fill_report(0, _activity(1_000_000, 10_100)), "page_splits_measured") == []
    # Counters not read: nothing is said about fill factor.
    assert _leads(_fill_report(0, None), "page_splits_measured") == []


def test_low_fill_factor_with_no_page_splits_is_reported_as_reserved_space() -> None:
    (lead,) = _leads(_fill_report(70, _activity(1_000_000, 10_100)), "fill_factor_without_page_split_evidence")

    assert lead["ddl"].endswith("REBUILD WITH (FILLFACTOR = 100, ONLINE = ON);")
    assert lead["rollback_ddl"].endswith("REBUILD WITH (FILLFACTOR = 70, ONLINE = ON);")
    assert lead["evidence"]["reserved_mb"] == 234.4

    # With splits measured the low fill factor is doing its job: no lead either way.
    busy = _fill_report(70, _activity(1_000_000, 60_000))
    assert _leads(busy, "fill_factor_without_page_split_evidence") == []
    assert _leads(busy, "page_splits_measured") == []
    # Unread counters: a gap, never a guess.
    unread = _fill_report(70, None)
    assert _leads(unread, "fill_factor_without_page_split_evidence") == []
    assert any("fill factor" in gap for gap in unread["gaps"])


# --- the live review reads the leaf counters ----------------------------------


@pytest.mark.asyncio
async def test_review_reads_leaf_activity_once_and_reports_the_fill_factor_lead(monkeypatch: pytest.MonkeyPatch) -> None:
    indexes = [*_orders_indexes(), _fill(70)]
    for index in indexes:
        index.usage_context["engine_start_time_utc"] = "2026-08-01T00:00:00Z"

    async def fake_collect(executor, database_name, **kwargs):
        return indexes

    monkeypatch.setattr(workload_module, "collect_existing_indexes", fake_collect)
    executor = RoutingExecutor(
        operational=[
            {"object_id": 1, "index_name": "IX_Orders_Customer", "leaf_insert_count": 1_000_000, "leaf_allocation_count": 10_100}
        ]
    )

    report = await WorkloadIndexAdvisor(executor).review("appdb", now=NOW)

    (lead,) = _leads(report, "fill_factor_without_page_split_evidence")
    assert lead["evidence"]["leaf_insert_count"] == 1_000_000
    leaf_reads = [call for call in executor.calls if "leaf_allocation_count" in call[1]]
    assert len(leaf_reads) == 1

    # A failed read is a gap, and no fill factor lead is given.
    failing = RoutingExecutor(operational=RuntimeError("permission denied"))
    report = await WorkloadIndexAdvisor(failing).review("appdb", now=NOW)
    assert _leads(report, "fill_factor_without_page_split_evidence") == []
    assert any("Leaf page counters" in gap for gap in report["gaps"])
