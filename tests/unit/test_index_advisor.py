from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp.index_advisor import AdvisorInputs
from azure_sql_mcp.index_advisor import AdvisorSettings
from azure_sql_mcp.index_advisor import ColumnInfo
from azure_sql_mcp.index_advisor import ColumnSelectivity
from azure_sql_mcp.index_advisor import ForeignKeyInfo
from azure_sql_mcp.index_advisor import TableInfo
from azure_sql_mcp.index_advisor import WorkloadQuery
from azure_sql_mcp.index_advisor import build_index_advice
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import IndexKeyColumn
from azure_sql_mcp.showplan_access import parse_plan_access

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "showplans"
OBSERVED_AT = "2026-10-01T00:00:00+00:00"


def _plan(name: str):
    return parse_plan_access((FIXTURES / name).read_text(encoding="utf-8"))


def _query(
    query_id: int,
    fixture: str,
    *,
    cpu_us: float,
    executions: float = 1000,
    active_days: int = 5,
    rowcount: float = 0.0,
) -> WorkloadQuery:
    return WorkloadQuery(
        query_id=query_id,
        plan_id=query_id * 10,
        plan=_plan(fixture),
        executions=executions,
        total_cpu_us=cpu_us,
        total_duration_us=cpu_us * 2,
        total_logical_reads=cpu_us / 10,
        total_rowcount=rowcount,
        active_days=active_days,
    )


def _index(
    schema: str,
    table: str,
    name: str,
    keys: tuple[str, ...],
    *,
    index_id: int,
    includes: tuple[str, ...] = (),
    type_code: int = 2,
    unique: bool = False,
    primary_key: bool = False,
    reads: int | None = 100,
    updates: int = 10,
    pages: int = 128,
    protection: dict[str, Any] | None = None,
    filter_definition: str | None = None,
) -> ExistingIndex:
    usage: dict[str, int | None] = {
        "user_seeks": reads,
        "user_scans": 0 if reads is not None else None,
        "user_lookups": 0 if reads is not None else None,
        "user_updates": updates,
    }
    return ExistingIndex(
        schema=schema,
        table=table,
        index_id=index_id,
        name=name,
        index_type="CLUSTERED" if type_code == 1 else "NONCLUSTERED",
        key_columns=tuple(IndexKeyColumn(column, "ASC") for column in keys),
        include_columns=includes,
        filter_definition=filter_definition,
        is_unique=unique or primary_key,
        is_primary_key=primary_key,
        is_unique_constraint=False,
        constraint_name=f"{name}" if primary_key else None,
        constraint_type="PRIMARY_KEY_CONSTRAINT" if primary_key else None,
        is_disabled=False,
        fill_factor=0,
        data_space_name="PRIMARY",
        data_space_type="ROWS_FILEGROUP",
        partition_compression=((1, "NONE"),),
        xml_compression=((1, "OFF"),),
        usage=usage,
        usage_context={"availability": "available", "coverage": "covered"},
        object_id=hash((schema, table)) % 100_000,
        parent_object_type="USER_TABLE",
        parent_object_type_code="U",
        index_type_code=type_code,
        is_hypothetical=False,
        is_auto_created=False,
        has_filter=filter_definition is not None,
        is_padded=False,
        ignore_dup_key=False,
        allow_row_locks=True,
        allow_page_locks=True,
        optimize_for_sequential_key=False,
        suppress_dup_key_messages=False,
        statistics_no_recompute=False,
        statistics_incremental=False,
        partition_page_counts=((1, pages),),
        protection_evidence=protection or {},
    )


def _columns(*specs: tuple[str, str, int]) -> dict[str, ColumnInfo]:
    return {name.casefold(): ColumnInfo(name=name, type_name=type_name, max_length=length) for name, type_name, length in specs}


ORDERS_COLUMNS = _columns(
    ("OrderID", "int", 4),
    ("CustomerID", "int", 4),
    ("SalespersonPersonID", "int", 4),
    ("OrderDate", "date", 3),
    ("ExpectedDeliveryDate", "date", 3),
    ("Status", "tinyint", 1),
    ("Comments", "nvarchar", 400),
    ("InternalNotes", "nvarchar", -1),
)
ORDERLINES_COLUMNS = _columns(
    ("OrderLineID", "int", 4),
    ("OrderID", "int", 4),
    ("StockItemID", "int", 4),
    ("Description", "nvarchar", 200),
    ("PackageTypeID", "int", 4),
    ("Quantity", "int", 4),
    ("UnitPrice", "decimal", 9),
    ("PickingCompletedWhen", "datetime2", 8),
)
EVENTLOG_COLUMNS = _columns(
    ("EventID", "bigint", 8),
    ("UserID", "int", 4),
    ("CreatedAt", "datetime2", 8),
    ("AccountNumber", "varchar", 20),
    ("Severity", "tinyint", 1),
    ("Payload", "nvarchar", -1),
)


def _tables() -> dict[tuple[str, str], TableInfo]:
    return {
        ("sales", "orders"): TableInfo("Sales", "Orders", object_id=1, row_count=73_595, all_used_pages=4_000),
        ("sales", "orderlines"): TableInfo("Sales", "OrderLines", object_id=2, row_count=231_412, all_used_pages=9_000),
        ("dbo", "eventlog"): TableInfo(
            "dbo", "EventLog", object_id=3, row_count=500_000, all_used_pages=60_000, is_heap=True, forwarded_fetches=0
        ),
        ("sales", "customers"): TableInfo("Sales", "Customers", object_id=4, row_count=663, all_used_pages=40),
    }


def _orders_indexes() -> list[ExistingIndex]:
    return [
        _index("Sales", "Orders", "PK_Sales_Orders", ("OrderID",), index_id=1, type_code=1, primary_key=True, reads=5000),
        _index(
            "Sales",
            "Orders",
            "FK_Sales_Orders_CustomerID",
            ("CustomerID",),
            index_id=2,
            reads=4000,
            protection={"child_foreign_key_support": [{"foreign_key_id": 9, "leading_key_supported": True}]},
        ),
    ]


def _inputs(
    queries: list[WorkloadQuery],
    existing: list[ExistingIndex],
    *,
    settings: AdvisorSettings | None = None,
    selectivity: dict[tuple[str, str, str], ColumnSelectivity] | None = None,
    references: dict[tuple[str, str, str], int | None] | None = None,
    engine_start: str | None = "2026-08-01T00:00:00Z",
    capture_mode: str = "ALL",
    foreign_keys: list[ForeignKeyInfo] | None = None,
    total_cpu: float | None = None,
) -> AdvisorInputs:
    total = total_cpu if total_cpu is not None else sum(q.total_cpu_us for q in queries) or 1.0
    return AdvisorInputs(
        database_name="appdb",
        window_start_utc="2026-09-24T00:00:00Z",
        window_end_utc="2026-10-01T00:00:00Z",
        settings=settings or AdvisorSettings(),
        queries=queries,
        workload_totals={"total_cpu_us": total, "workload_query_count": len(queries), "workload_executions": 1.0},
        tables=_tables(),
        columns={
            ("sales", "orders"): ORDERS_COLUMNS,
            ("sales", "orderlines"): ORDERLINES_COLUMNS,
            ("dbo", "eventlog"): EVENTLOG_COLUMNS,
        },
        existing_indexes=existing,
        selectivity=selectivity or {},
        foreign_keys=foreign_keys or [],
        index_plan_references=references or {},
        engine_start_time_utc=engine_start,
        query_store={"actual_state": "READ_WRITE", "query_capture_mode": capture_mode},
        observed_at_utc=OBSERVED_AT,
    )


def _recs(report: dict[str, Any], action: str, table: str | None = None) -> list[dict[str, Any]]:
    return [
        rec
        for rec in report["recommendations"]
        if rec["action"] == action and (table is None or rec["table"] == table)
    ]


def _keys(rec: dict[str, Any]) -> list[str]:
    return [column["name"] for column in rec["key_columns"]]


ORDERS_SELECTIVITY = {
    ("sales", "orders", "customerid"): ColumnSelectivity(distinct_estimate=663, rows=73_595),
    ("sales", "orders", "status"): ColumnSelectivity(distinct_estimate=3, rows=73_595),
    ("sales", "orders", "orderdate"): ColumnSelectivity(distinct_estimate=1_400, rows=73_595),
}


def test_seek_plus_lookup_widens_the_feeding_index_into_a_covering_index() -> None:
    report = build_index_advice(
        _inputs(
            [_query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000)],
            _orders_indexes(),
            selectivity=ORDERS_SELECTIVITY,
        )
    )

    widen = _recs(report, "widen_index", "Orders")
    assert len(widen) == 1
    rec = widen[0]
    assert rec["target_index"] == "FK_Sales_Orders_CustomerID"
    # Equality keys first (more selective first within one query), then the range
    # column that also satisfies the ORDER BY; the lookup's output becomes an include.
    assert _keys(rec) == ["CustomerID", "Status", "OrderDate"]
    assert rec["include_columns"] == ["Comments"]
    assert "optimizer_missing_index_hint_agrees" in rec["reason_codes"]
    assert "cover_lookup" in rec["reason_codes"]
    assert "DROP_EXISTING = ON" in rec["ddl"]
    assert "ONLINE = ON" in rec["ddl"]
    assert "DATA_COMPRESSION = NONE" in rec["ddl"]
    # One rebuild, not a DROP_EXISTING followed by a compression rebuild.
    assert "ALTER INDEX" not in rec["ddl"]
    # Rollback restores the original single-key definition with DROP_EXISTING.
    assert "([CustomerID] ASC)" in rec["rollback_ddl"]
    assert "DROP_EXISTING = ON" in rec["rollback_ddl"]
    assert rec["supporting_queries"][0]["query_id"] == 1
    assert rec["estimated_max_benefit_pct"] > 50


def test_scan_with_residual_filters_creates_a_seekable_index() -> None:
    selectivity = {
        ("sales", "orderlines", "stockitemid"): ColumnSelectivity(distinct_estimate=227, rows=231_412),
        ("sales", "orderlines", "packagetypeid"): ColumnSelectivity(distinct_estimate=4, rows=231_412),
        ("sales", "orderlines", "pickingcompletedwhen"): ColumnSelectivity(distinct_estimate=1_000, rows=231_412),
    }
    existing = [
        _index("Sales", "OrderLines", "PK_Sales_OrderLines", ("OrderLineID",), index_id=1, type_code=1, primary_key=True)
    ]

    report = build_index_advice(
        _inputs([_query(2, "clustered_scan_residuals.xml", cpu_us=2_000_000)], existing, selectivity=selectivity)
    )

    create = _recs(report, "create_index", "OrderLines")
    assert len(create) == 1
    rec = create[0]
    assert _keys(rec) == ["PickingCompletedWhen", "StockItemID", "PackageTypeID"]
    # The clustered key (OrderLineID) is implicit in every nonclustered index.
    assert rec["include_columns"] == ["Quantity", "Description", "UnitPrice"]
    assert rec["ddl"] == (
        "CREATE NONCLUSTERED INDEX [IX_OrderLines_PickingCompletedWhen_StockItemID_PackageTypeID] "
        "ON [Sales].[OrderLines] ([PickingCompletedWhen] ASC, [StockItemID] ASC, [PackageTypeID] ASC) "
        "INCLUDE ([Quantity], [Description], [UnitPrice]) WITH (ONLINE = ON);"
    )
    assert rec["rollback_ddl"] == (
        "DROP INDEX [IX_OrderLines_PickingCompletedWhen_StockItemID_PackageTypeID] ON [Sales].[OrderLines];"
    )
    assert rec["confidence"] in {"high", "medium"}
    # Upper bound from declared widths: 231,412 rows of 8+4+4+4+200+9 bytes plus overhead.
    assert 50 < rec["estimated_max_size_mb"] < 80


def test_non_sargable_predicates_route_to_the_optimizer_not_to_an_index() -> None:
    report = build_index_advice(
        _inputs([_query(3, "heap_scan_nonsargable.xml", cpu_us=4_000_000)], [], total_cpu=4_000_000)
    )

    assert _recs(report, "create_index", "EventLog") == []
    patterns = {(item["column"], item["pattern"]) for item in report["rewrite_opportunities"]}
    assert patterns == {("CreatedAt", "convert"), ("AccountNumber", "convert_implicit")}
    assert all(item["owner"] == "sql-optimizer" for item in report["rewrite_opportunities"])
    heap = _recs(report, "create_clustered_index", "EventLog")
    assert heap and "heap_scans_in_workload" in heap[0]["reason_codes"]


def test_lookup_that_needs_a_lob_column_is_not_covered_and_heap_is_flagged() -> None:
    existing = [_index("dbo", "EventLog", "IX_EventLog_UserID", ("UserID",), index_id=2)]

    report = build_index_advice(
        _inputs([_query(4, "heap_seek_rid_lookup.xml", cpu_us=600_000)], existing)
    )

    # Payload is nvarchar(max): no include can remove the RID lookup.
    assert _recs(report, "extend_index", "EventLog") == []
    assert _recs(report, "create_index", "EventLog") == []
    heap = _recs(report, "create_clustered_index", "EventLog")
    assert heap and "rid_lookups_in_workload" in heap[0]["reason_codes"]


def test_existing_index_that_already_covers_is_flagged_for_plan_investigation() -> None:
    existing = [
        _index("Sales", "OrderLines", "PK_Sales_OrderLines", ("OrderLineID",), index_id=1, type_code=1, primary_key=True),
        _index(
            "Sales",
            "OrderLines",
            "IX_Covering",
            ("PickingCompletedWhen", "StockItemID", "PackageTypeID"),
            index_id=2,
            includes=("Quantity", "Description", "UnitPrice"),
        ),
    ]
    selectivity = {
        ("sales", "orderlines", "stockitemid"): ColumnSelectivity(distinct_estimate=227, rows=231_412),
        ("sales", "orderlines", "packagetypeid"): ColumnSelectivity(distinct_estimate=4, rows=231_412),
        ("sales", "orderlines", "pickingcompletedwhen"): ColumnSelectivity(distinct_estimate=1_000, rows=231_412),
    }

    report = build_index_advice(
        _inputs([_query(2, "clustered_scan_residuals.xml", cpu_us=2_000_000)], existing, selectivity=selectivity)
    )

    assert _recs(report, "create_index", "OrderLines") == []
    review = [r for r in _recs(report, "review_index", "OrderLines") if r["index_name"] == "IX_Covering"]
    assert review and review[0]["reason_codes"] == ["existing_index_already_covers_access"]


def test_exact_duplicate_index_is_consolidated_into_the_wider_twin() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Salesperson", ("SalespersonPersonID",), index_id=3, reads=10),
        _index(
            "Sales",
            "Orders",
            "IX_Salesperson_Dates",
            ("SalespersonPersonID",),
            index_id=4,
            includes=("OrderDate",),
            reads=900,
        ),
    ]

    report = build_index_advice(_inputs([], existing))

    consolidate = _recs(report, "consolidate_index", "Orders")
    assert len(consolidate) == 1
    rec = consolidate[0]
    assert rec["target_index"] == "IX_Salesperson"
    assert rec["merge_into"] == "IX_Salesperson_Dates"
    assert rec["reason_codes"] == ["exact_duplicate_keys"]
    assert rec["ddl"] == "DROP INDEX [IX_Salesperson] ON [Sales].[Orders];"
    assert "CREATE NONCLUSTERED INDEX [IX_Salesperson]" in rec["rollback_ddl"]
    assert rec["confidence"] == "high"


def test_left_prefix_index_merges_its_includes_into_the_wider_index() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Date", ("OrderDate",), index_id=3, includes=("Comments",), reads=50),
        _index("Sales", "Orders", "IX_Date_Status", ("OrderDate", "Status"), index_id=4, reads=700),
    ]

    report = build_index_advice(_inputs([], existing))

    rec = _recs(report, "consolidate_index", "Orders")[0]
    assert rec["target_index"] == "IX_Date"
    assert rec["merge_into"] == "IX_Date_Status"
    assert rec["include_columns"] == ["Comments"]
    # The survivor gains the include before the redundant index is dropped.
    assert rec["ddl"].index("CREATE NONCLUSTERED INDEX [IX_Date_Status]") < rec["ddl"].index("DROP INDEX [IX_Date]")


def test_unused_index_with_long_uptime_and_no_query_store_reference_is_a_drop_candidate() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Unused", ("ExpectedDeliveryDate",), index_id=5, reads=0, updates=50_000)
    ]

    report = build_index_advice(
        _inputs([], existing, references={("sales", "orders", "ix_unused"): 0})
    )

    drop = _recs(report, "drop_index", "Orders")
    assert len(drop) == 1
    rec = drop[0]
    assert rec["target_index"] == "IX_Unused"
    assert rec["confidence"] == "high"
    assert rec["ddl"] == "DROP INDEX [IX_Unused] ON [Sales].[Orders];"
    assert "CREATE NONCLUSTERED INDEX [IX_Unused]" in rec["rollback_ddl"]
    assert "no_query_store_plan_reference_in_window" in rec["reason_codes"]


def test_unused_by_counters_but_present_in_query_store_plans_is_kept() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Unused", ("ExpectedDeliveryDate",), index_id=5, reads=0, updates=50_000)
    ]

    report = build_index_advice(
        _inputs([], existing, references={("sales", "orders", "ix_unused"): 3})
    )

    assert _recs(report, "drop_index", "Orders") == []
    review = [r for r in _recs(report, "review_index", "Orders") if r["index_name"] == "IX_Unused"]
    assert review[0]["reason_codes"] == ["unused_by_dmv_but_in_query_store_plans"]


def test_unused_index_after_a_recent_counter_reset_is_only_observed() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Unused", ("ExpectedDeliveryDate",), index_id=5, reads=0, updates=50_000)
    ]

    report = build_index_advice(
        _inputs(
            [],
            existing,
            references={("sales", "orders", "ix_unused"): 0},
            engine_start="2026-09-29T00:00:00Z",
        )
    )

    assert _recs(report, "drop_index", "Orders") == []
    review = [r for r in _recs(report, "review_index", "Orders") if r["index_name"] == "IX_Unused"]
    assert review[0]["reason_codes"] == ["unused_but_counters_recent"]


def test_unique_and_constraint_indexes_are_never_drop_candidates() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "UX_Unused", ("ExpectedDeliveryDate",), index_id=5, unique=True, reads=0, updates=50_000)
    ]

    report = build_index_advice(
        _inputs([], existing, references={("sales", "orders", "ux_unused"): 0})
    )

    assert _recs(report, "drop_index", "Orders") == []


def test_auto_capture_mode_lowers_drop_confidence() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Unused", ("ExpectedDeliveryDate",), index_id=5, reads=0, updates=50_000)
    ]

    report = build_index_advice(
        _inputs([], existing, references={("sales", "orders", "ix_unused"): 0}, capture_mode="AUTO")
    )

    rec = _recs(report, "drop_index", "Orders")[0]
    assert rec["confidence"] == "medium"
    assert "query_store_capture_mode_auto_may_miss_rare_queries" in rec["blockers"]
    assert any("AUTO" in gap for gap in report["gaps"])


def test_write_heavy_table_carries_a_write_penalty() -> None:
    read = _query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000)
    quiet = build_index_advice(_inputs([read], _orders_indexes(), selectivity=ORDERS_SELECTIVITY))
    # 200,000 rows updated over a 7-day window on a 73,595-row table.
    update = _query(9, "update_scan_dml.xml", cpu_us=50_000, rowcount=200_000)
    busy = build_index_advice(
        _inputs([read, update], _orders_indexes(), selectivity=ORDERS_SELECTIVITY)
    )

    quiet_rec = _recs(quiet, "widen_index", "Orders")[0]
    busy_rec = _recs(busy, "widen_index", "Orders")[0]
    assert quiet_rec["write_impact"]["penalty"] == 0.0
    assert busy_rec["write_impact"]["penalty"] > 0.3
    assert busy_rec["score"] < busy_rec["estimated_max_benefit_pct"]
    orders = next(t for t in busy["tables"] if t["table"] == "Orders")
    assert orders["write_activity"]["dml_statements"] == 1
    assert orders["write_activity"]["dml_rows_in_window"] == 200_000


def test_focus_tables_limit_the_review() -> None:
    queries = [
        _query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000),
        _query(2, "clustered_scan_residuals.xml", cpu_us=2_000_000),
    ]

    report = build_index_advice(
        _inputs(
            queries,
            _orders_indexes(),
            settings=AdvisorSettings(focus_tables=frozenset({("sales", "orderlines")})),
        )
    )

    assert {t["table"] for t in report["tables"]} == {"OrderLines"}
    assert {r["table"] for r in report["recommendations"]} <= {"OrderLines"}


def test_small_tables_are_skipped_with_a_note() -> None:
    report = build_index_advice(
        _inputs(
            [_query(2, "clustered_scan_residuals.xml", cpu_us=2_000_000)],
            [],
            settings=AdvisorSettings(min_table_rows=1_000_000),
        )
    )

    table = next(t for t in report["tables"] if t["table"] == "OrderLines")
    assert "table_below_min_rows" in table["notes"]
    assert _recs(report, "create_index") == []


def test_low_coverage_of_the_workload_is_reported_as_a_gap() -> None:
    report = build_index_advice(
        _inputs([_query(2, "clustered_scan_residuals.xml", cpu_us=100)], [], total_cpu=10_000)
    )

    assert report["workload"]["analyzed_share_pct"] == pytest.approx(1.0)
    assert any("cover 1.0% of the workload" in gap for gap in report["gaps"])


def test_unindexed_foreign_key_is_reported() -> None:
    fk = ForeignKeyInfo(
        name="FK_OrderLines_Orders",
        schema="Sales",
        table="OrderLines",
        columns=("OrderID",),
        referenced_schema="Sales",
        referenced_table="Orders",
    )
    existing = [
        _index("Sales", "OrderLines", "PK_Sales_OrderLines", ("OrderLineID",), index_id=1, type_code=1, primary_key=True)
    ]

    report = build_index_advice(_inputs([], existing, foreign_keys=[fk]))

    review = _recs(report, "review_index", "OrderLines")
    assert review and review[0]["reason_codes"] == ["foreign_key_without_supporting_index"]


def test_report_shape_is_recommend_only_with_stable_ids() -> None:
    report = build_index_advice(
        _inputs(
            [
                _query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000),
                _query(2, "clustered_scan_residuals.xml", cpu_us=2_000_000),
            ],
            _orders_indexes(),
            selectivity=ORDERS_SELECTIVITY,
        )
    )

    assert report["contract"] == "workload_index_advice_v1"
    assert report["recommend_only"] is True
    ids = [rec["id"] for rec in report["recommendations"]]
    assert ids == [f"R{position}" for position in range(1, len(ids) + 1)]
    for table in report["tables"]:
        assert set(table["recommendation_ids"]) <= set(ids)
    assert all("_supports" not in rec for rec in report["recommendations"])
    assert report["summary"]["top_improvement_ids"]
    assert any("change control" in step for step in report["next_steps"])


def test_eager_index_spool_becomes_the_index_it_was_building() -> None:
    existing = [
        _index("Sales", "OrderLines", "PK_Sales_OrderLines", ("OrderLineID",), index_id=1, type_code=1, primary_key=True)
    ]

    report = build_index_advice(
        _inputs([_query(7, "eager_index_spool.xml", cpu_us=3_000_000)], existing)
    )

    create = _recs(report, "create_index", "OrderLines")
    assert len(create) == 1
    rec = create[0]
    assert _keys(rec) == ["OrderID"]
    assert set(rec["include_columns"]) == {"Quantity", "UnitPrice"}
    assert "replace_eager_spool" in rec["reason_codes"]
    assert "eager index spool" in rec["rationale"]
