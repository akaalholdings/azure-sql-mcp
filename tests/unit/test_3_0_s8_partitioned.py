"""Partitioned tables: aligned advice for new indexes, and no silent exclusion.

Every index on a partition scheme is protected from an executable removal,
because partition switching needs matching indexes on both tables. That
protection stays. What changes: the index now appears as a lead with the
reason, instead of vanishing from the report.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from azure_sql_mcp.index_advisor import build_index_advice
from azure_sql_mcp.index_metadata import ExistingIndex
from tests.unit.test_index_advisor import _index
from tests.unit.test_index_advisor import _inputs
from tests.unit.test_index_advisor import _query

PARTITIONS = 48
PARTITION_ONLY: dict[str, Any] = {"coverage": "complete", "partition_switch_dependency": True}


def _partitioned(index: ExistingIndex, *, column: str = "OrderDate") -> ExistingIndex:
    return replace(
        index,
        data_space_name="PS_Orders",
        data_space_type="PARTITION_SCHEME",
        partition_scheme_name="PS_Orders",
        partition_function_name="PF_Orders",
        partition_columns=(column,),
        partition_compression=tuple((number, "NONE") for number in range(1, PARTITIONS + 1)),
        xml_compression=tuple((number, "OFF") for number in range(1, PARTITIONS + 1)),
        partition_page_counts=tuple((number, 8) for number in range(1, PARTITIONS + 1)),
        protection_evidence={**(index.protection_evidence or {}), "partition_switch_dependency": True},
    )


def _clustered(*, column: str = "OrderDate") -> ExistingIndex:
    return _partitioned(
        _index("Sales", "Orders", "PK_Sales_Orders", ("OrderID",), index_id=1, type_code=1, primary_key=True, reads=5000),
        column=column,
    )


def _leads(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        rec["index_name"]: rec
        for rec in report["recommendations"]
        if "partitioned_index_removal_needs_switch_review" in rec["reason_codes"]
    }


def _removals(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [rec for rec in report["recommendations"] if rec["action"] in {"drop_index", "consolidate_index"}]


def test_unused_index_on_a_partition_scheme_is_reported_with_its_reason_and_no_drop_statement() -> None:
    unused = _partitioned(
        _index("Sales", "Orders", "IX_Orders_Status", ("Status",), index_id=5, reads=0, updates=90_000, protection=PARTITION_ONLY)
    )
    references = {("sales", "orders", "ix_orders_status"): 0}
    report = build_index_advice(_inputs([], [_clustered(), unused], references=references, window_days=40))

    assert _removals(report) == []
    lead = _leads(report)["IX_Orders_Status"]
    assert lead["action"] == "review_index"
    assert lead["ddl"] is None and lead["rollback_ddl"] is None
    assert "partition_switch_dependency" in lead["blockers"]
    assert lead["removal_lead"] == {"kind": "drop", "merge_into": None, "evidence_confidence": "high"}
    assert "no_reads_since_counter_reset" in lead["reason_codes"]
    assert "PS_Orders" in lead["rationale"] and "SWITCH" in lead["rationale"]


def test_unused_partitioned_index_with_recent_counters_still_reports_the_usual_reason() -> None:
    """The evidence checks run first: a lead never claims non-use that the counters cannot show."""

    unused = _partitioned(
        _index("Sales", "Orders", "IX_Orders_Status", ("Status",), index_id=5, reads=0, protection=PARTITION_ONLY)
    )
    report = build_index_advice(
        _inputs([], [_clustered(), unused], references={("sales", "orders", "ix_orders_status"): 0}, engine_start="2026-09-28T00:00:00Z")
    )
    assert _leads(report) == {}
    assert "unused_but_counters_recent" in {code for rec in report["recommendations"] for code in rec["reason_codes"]}


def test_duplicate_and_left_prefix_indexes_on_a_partition_scheme_become_leads() -> None:
    wide = _partitioned(
        _index("Sales", "Orders", "IX_Orders_Customer_Date", ("CustomerID", "OrderDate"), index_id=5, reads=900, protection=PARTITION_ONLY)
    )
    twin = _partitioned(
        _index("Sales", "Orders", "IX_Orders_Customer_Date_2", ("CustomerID", "OrderDate"), index_id=6, reads=3, protection=PARTITION_ONLY)
    )
    prefix = _partitioned(
        _index("Sales", "Orders", "IX_Orders_Customer", ("CustomerID",), index_id=7, reads=40, protection=PARTITION_ONLY)
    )
    report = build_index_advice(_inputs([], [_clustered(), wide, twin, prefix]))

    assert _removals(report) == []
    leads = _leads(report)
    assert set(leads) == {"IX_Orders_Customer_Date_2", "IX_Orders_Customer"}
    assert leads["IX_Orders_Customer_Date_2"]["removal_lead"]["kind"] == "consolidate"
    assert leads["IX_Orders_Customer_Date_2"]["removal_lead"]["merge_into"] == "IX_Orders_Customer_Date"
    assert "exact_duplicate_keys" in leads["IX_Orders_Customer_Date_2"]["reason_codes"]
    assert leads["IX_Orders_Customer"]["removal_lead"]["merge_into"] == "IX_Orders_Customer_Date"
    assert "left_prefix_of_wider_index" in leads["IX_Orders_Customer"]["reason_codes"]
    for lead in leads.values():
        assert lead["ddl"] is None


def test_unique_index_on_a_partition_scheme_stays_a_constraint_and_gets_no_removal_lead() -> None:
    unique = _partitioned(
        _index("Sales", "Orders", "UQ_Orders_Ref", ("Comments", "OrderDate"), index_id=5, reads=0, unique=True, protection=PARTITION_ONLY)
    )
    report = build_index_advice(
        _inputs([], [_clustered(), unique], references={("sales", "orders", "uq_orders_ref"): 0}, window_days=40)
    )
    assert _leads(report) == {} and _removals(report) == []


def _create(report: dict[str, Any]) -> dict[str, Any]:
    creates = [rec for rec in report["recommendations"] if rec["action"] == "create_index"]
    assert len(creates) == 1
    return creates[0]


def test_new_index_on_a_partitioned_table_is_aligned_and_names_the_non_aligned_trade() -> None:
    """Sales.Orders is partitioned by ExpectedDeliveryDate; the query filters on OrderDate only."""

    query = _query(1, "hash_join_stream_aggregate.xml", cpu_us=9_000_000)
    report = build_index_advice(_inputs([query], [_clustered(column="ExpectedDeliveryDate")]))

    rec = _create(report)
    part = rec["partitioning"]
    assert part["alignment"] == "aligned"
    assert part["partition_scheme"] == "PS_Orders"
    assert part["partition_columns"] == ["ExpectedDeliveryDate"]
    assert part["partition_count"] == PARTITIONS
    # The executable statement has no ON clause: SQL Server aligns it with the table.
    assert " ON [PRIMARY]" not in rec["ddl"] and "PS_Orders" not in rec["ddl"]
    alternative = part["non_aligned_alternative"]
    assert alternative["ddl"].endswith("WITH (ONLINE = ON) ON [PRIMARY];")
    assert "48 partitions" in alternative["when_better"]
    assert "SWITCH" in alternative["breaks"] and "7733" in alternative["breaks"]
    assert any("every partition" in risk for risk in rec["risks"])


def test_new_index_keeps_only_the_aligned_form_when_queries_filter_on_the_partitioning_column() -> None:
    query = _query(1, "hash_join_stream_aggregate.xml", cpu_us=9_000_000)
    report = build_index_advice(_inputs([query], [_clustered(column="OrderDate")]))

    part = _create(report)["partitioning"]
    assert part["alignment"] == "aligned"
    assert part["non_aligned_alternative"] is None


def test_table_summary_reports_partitioning() -> None:
    query = _query(1, "hash_join_stream_aggregate.xml", cpu_us=9_000_000)
    report = build_index_advice(_inputs([query], [_clustered()]))
    table = next(item for item in report["tables"] if item["table"] == "Orders")
    assert table["partitioning"] == {
        "partition_scheme": "PS_Orders",
        "partition_columns": ["OrderDate"],
        "partition_count": PARTITIONS,
    }
