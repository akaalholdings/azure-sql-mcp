from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
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
        # Complete protection metadata unless a test states otherwise.
        protection_evidence=protection if protection is not None else {"coverage": "complete"},
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


FK9_SUPPORT: dict[str, Any] = {
    "coverage": "complete",
    "child_foreign_key_support": [
        {"foreign_key_id": 9, "child_object_id": 1, "leading_key_supported": True}
    ],
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
            protection=FK9_SUPPORT,
        ),
    ]


WINDOW_END = datetime(2026, 10, 1, tzinfo=timezone.utc)


def _utc(days_before_end: float) -> str:
    return (WINDOW_END - timedelta(days=days_before_end)).strftime("%Y-%m-%dT%H:%M:%SZ")


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
    window_days: int = 7,
    query_store_days: float | None = None,
    coverage_known: bool = True,
    query_store_options: dict[str, Any] | None = None,
    statistics: dict[tuple[str, str, str], int | None] | None = None,
    forced_plan_accesses: list[tuple[int, int, str, str, str]] | None = None,
    hint_references: dict[tuple[str, str, str], list[str]] | None = None,
    pin_coverage: str = "complete",
    hint_coverage: str | None = None,
) -> AdvisorInputs:
    """Inputs ending at OBSERVED_AT. Query Store holds the whole window unless
    query_store_days says otherwise; every pin check is complete by default."""

    total = total_cpu if total_cpu is not None else sum(q.total_cpu_us for q in queries) or 1.0
    held = window_days if query_store_days is None else query_store_days
    return AdvisorInputs(
        database_name="appdb",
        window_start_utc=_utc(window_days),
        window_end_utc=_utc(0),
        settings=settings or AdvisorSettings(lookback_days=window_days),
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
        query_store={"actual_state": "READ_WRITE", "query_capture_mode": capture_mode, **(query_store_options or {})},
        observed_at_utc=OBSERVED_AT,
        index_statistics_references=(
            statistics if statistics is not None else {key: 0 for key in (references or {})}
        ),
        query_store_coverage=(
            {
                "effective_start_utc": _utc(held),
                "effective_end_utc": _utc(0),
                "interval_count": int(held * 24),
                "oldest_interval_start_utc": _utc(held),
            }
            if coverage_known
            else None
        ),
        forced_plan_accesses=forced_plan_accesses or [],
        index_hint_references=hint_references or {},
        forced_plan_coverage=pin_coverage,
        hint_coverage=hint_coverage or pin_coverage,
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

    # High confidence needs a Query Store reference check over the 35-day removal window.
    report = build_index_advice(
        _inputs([], existing, references={("sales", "orders", "ix_unused"): 0}, window_days=40)
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


def _orders_clustered() -> ExistingIndex:
    return _index("Sales", "Orders", "PK_Sales_Orders", ("OrderID",), index_id=1, type_code=1, primary_key=True, reads=5000)


INCOMPLETE_PROTECTION: dict[str, Any] = {
    "coverage": "incomplete",
    "blockers": ["protection_metadata_cap_reached"],
}


def test_drop_is_withheld_when_protection_metadata_is_incomplete() -> None:
    fk = ForeignKeyInfo(
        name="FK_Orders_Customers",
        schema="Sales",
        table="Orders",
        columns=("CustomerID",),
        referenced_schema="Sales",
        referenced_table="Customers",
    )
    existing = [
        _orders_clustered(),
        _index(
            "Sales",
            "Orders",
            "IX_Orders_CustomerID",
            ("CustomerID",),
            index_id=2,
            reads=0,
            updates=50_000,
            protection=INCOMPLETE_PROTECTION,
        ),
    ]

    report = build_index_advice(
        _inputs(
            [],
            existing,
            references={("sales", "orders", "ix_orders_customerid"): 0},
            foreign_keys=[fk],
        )
    )

    # Without complete constraint and FK evidence the lead stays visible, but no
    # executable DROP is offered: the index may be the only FK support.
    [rec] = _recs(report, "drop_index", "Orders")
    assert rec["index_name"] == "IX_Orders_CustomerID"
    assert "protection_metadata_incomplete" in rec["blockers"]
    assert rec["confidence"] == "low"
    assert rec["ddl"] is None
    assert rec["rollback_ddl"] is None


def test_consolidation_is_withheld_when_protection_metadata_is_incomplete() -> None:
    existing = [
        _orders_clustered(),
        _index(
            "Sales",
            "Orders",
            "IX_Salesperson",
            ("SalespersonPersonID",),
            index_id=3,
            reads=10,
            protection=INCOMPLETE_PROTECTION,
        ),
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

    [rec] = _recs(report, "consolidate_index", "Orders")
    assert rec["index_name"] == "IX_Salesperson"
    assert "protection_metadata_incomplete" in rec["blockers"]
    assert rec["confidence"] == "low"
    assert rec["ddl"] is None
    assert rec["rollback_ddl"] is None


def test_sole_fk_index_is_kept_but_true_duplicate_fk_index_consolidates() -> None:
    duplicate_pair = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_Cust", ("CustomerID",), index_id=2, reads=0, updates=50_000, protection=FK9_SUPPORT),
        _index(
            "Sales",
            "Orders",
            "IX_Cust_Dates",
            ("CustomerID",),
            index_id=3,
            includes=("OrderDate",),
            reads=900,
            protection=FK9_SUPPORT,
        ),
    ]
    sole = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_Cust", ("CustomerID",), index_id=2, reads=0, updates=50_000, protection=FK9_SUPPORT),
    ]
    references = {("sales", "orders", "ix_cust"): 0}

    duplicates = build_index_advice(_inputs([], duplicate_pair, references=references))
    alone = build_index_advice(_inputs([], sole, references=references))

    # The twin still supports FK 9, so the redundant copy can go.
    [rec] = _recs(duplicates, "consolidate_index", "Orders")
    assert rec["index_name"] == "IX_Cust"
    assert rec["merge_into"] == "IX_Cust_Dates"
    assert rec["ddl"] == "DROP INDEX [IX_Cust] ON [Sales].[Orders];"
    # The last index that supports FK 9 is never a drop candidate.
    assert _recs(alone, "drop_index", "Orders") == []
    review = [r for r in _recs(alone, "review_index", "Orders") if r["index_name"] == "IX_Cust"]
    assert review and review[0]["reason_codes"] == ["sole_foreign_key_support"]


def test_two_unused_fk_indexes_keep_one_supporter() -> None:
    existing = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_Cust_A", ("CustomerID", "Status"), index_id=2, reads=0, updates=50_000, protection=FK9_SUPPORT),
        _index("Sales", "Orders", "IX_Cust_B", ("CustomerID", "OrderDate"), index_id=3, reads=0, updates=50_000, protection=FK9_SUPPORT),
    ]
    references = {("sales", "orders", "ix_cust_a"): 0, ("sales", "orders", "ix_cust_b"): 0}

    report = build_index_advice(_inputs([], existing, references=references))

    drops = _recs(report, "drop_index", "Orders")
    assert [rec["index_name"] for rec in drops] == ["IX_Cust_A"]
    review = [r for r in _recs(report, "review_index", "Orders") if r["index_name"] == "IX_Cust_B"]
    assert review and review[0]["reason_codes"] == ["sole_foreign_key_support"]


def test_filtered_index_is_not_alternative_fk_support() -> None:
    existing = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_Cust", ("CustomerID",), index_id=2, reads=0, updates=50_000, protection=FK9_SUPPORT),
        # The protection query reports any index that leads with the FK columns,
        # filtered or not. The RI check (CustomerID = @parent) cannot use a
        # Status = 1 filter, so this index does not replace IX_Cust.
        _index(
            "Sales",
            "Orders",
            "IX_Cust_Open",
            ("CustomerID",),
            index_id=3,
            reads=900,
            filter_definition="([Status]=(1))",
            protection=FK9_SUPPORT,
        ),
    ]
    references = {("sales", "orders", "ix_cust"): 0}

    report = build_index_advice(_inputs([], existing, references=references))

    assert _recs(report, "drop_index", "Orders") == []
    review = [r for r in _recs(report, "review_index", "Orders") if r["index_name"] == "IX_Cust"]
    assert review and review[0]["reason_codes"] == ["sole_foreign_key_support"]


def test_foreign_key_with_only_filtered_index_support_is_reported() -> None:
    fk = ForeignKeyInfo(
        name="FK_Orders_Customers",
        schema="Sales",
        table="Orders",
        columns=("CustomerID",),
        referenced_schema="Sales",
        referenced_table="Customers",
    )
    existing = [
        _orders_clustered(),
        _index(
            "Sales",
            "Orders",
            "IX_Cust_Open",
            ("CustomerID",),
            index_id=3,
            reads=900,
            filter_definition="([Status]=(1))",
            protection=FK9_SUPPORT,
        ),
    ]

    report = build_index_advice(_inputs([], existing, foreign_keys=[fk]))

    review = [r for r in _recs(report, "review_index", "Orders") if r["index_name"] is None]
    assert review and review[0]["reason_codes"] == ["foreign_key_without_supporting_index"]


def _rebuilds_of(report: dict[str, Any], index_name: str) -> int:
    marker = f"CREATE NONCLUSTERED INDEX [{index_name}]"
    return sum((rec["ddl"] or "").count(marker) for rec in report["recommendations"])


def test_constraint_survivor_never_drops_its_covering_twin() -> None:
    existing = [
        _index("Sales", "Orders", "PK_Orders", ("OrderID",), index_id=2, primary_key=True, reads=10),
        _index(
            "Sales",
            "Orders",
            "IX_Cover",
            ("OrderID",),
            index_id=3,
            includes=("CustomerID", "OrderDate", "Status"),
            reads=90_000,
        ),
    ]

    report = build_index_advice(_inputs([], existing))

    # A constraint index cannot take INCLUDE columns, so the twin is the only
    # covering index for its seeks and must stay.
    assert _recs(report, "consolidate_index", "Orders") == []
    assert not any("DROP INDEX [IX_Cover]" in (rec["ddl"] or "") for rec in report["recommendations"])
    review = [r for r in _recs(report, "review_index", "Orders") if r["index_name"] == "IX_Cover"]
    assert review and review[0]["reason_codes"] == ["covering_twin_of_constraint_index"]
    assert review[0]["ddl"] is None


def test_left_prefix_into_nonclustered_pk_keeps_narrow_index_with_includes() -> None:
    existing = [
        _index("Sales", "OrderLines", "PK_OrderLines", ("OrderID", "OrderLineID"), index_id=2, primary_key=True),
        _index("Sales", "OrderLines", "IX_Order", ("OrderID",), index_id=3, includes=("Quantity",), reads=500),
    ]

    report = build_index_advice(_inputs([], existing))

    assert _recs(report, "consolidate_index", "OrderLines") == []
    review = [r for r in _recs(report, "review_index", "OrderLines") if r["index_name"] == "IX_Order"]
    assert review and review[0]["reason_codes"] == ["covering_twin_of_constraint_index"]


def test_left_prefix_into_pk_without_extra_columns_is_still_consolidated() -> None:
    existing = [
        _index("Sales", "OrderLines", "PK_OrderLines", ("OrderID", "OrderLineID"), index_id=2, primary_key=True),
        _index("Sales", "OrderLines", "IX_Order", ("OrderID",), index_id=3, reads=500),
    ]

    report = build_index_advice(_inputs([], existing))

    [rec] = _recs(report, "consolidate_index", "OrderLines")
    assert rec["merge_into"] == "PK_OrderLines"
    assert rec["ddl"] == "DROP INDEX [IX_Order] ON [Sales].[OrderLines];"


def test_three_way_duplicate_rebuilds_survivor_once() -> None:
    existing = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_A", ("CustomerID",), index_id=3, includes=("Status",), reads=10),
        _index("Sales", "Orders", "IX_B", ("CustomerID",), index_id=4, includes=("OrderDate",), reads=10),
        _index("Sales", "Orders", "IX_C", ("CustomerID",), index_id=5, includes=("Comments", "SalespersonPersonID"), reads=10),
    ]

    report = build_index_advice(_inputs([], existing))

    # Two rebuilds of IX_C from its catalogued definition would undo each
    # other, so only one consolidation rebuilds it.
    assert _rebuilds_of(report, "IX_C") == 1
    [rec] = _recs(report, "consolidate_index", "Orders")
    assert rec["index_name"] == "IX_A"
    assert rec["merge_into"] == "IX_C"
    assert "INCLUDE ([Comments], [SalespersonPersonID], [Status])" in rec["ddl"]
    review = [r for r in _recs(report, "review_index", "Orders") if r["index_name"] == "IX_B"]
    assert review and review[0]["reason_codes"] == ["survivor_rebuild_already_recommended"]


def test_duplicates_that_add_nothing_all_consolidate_into_one_survivor() -> None:
    existing = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_A", ("CustomerID",), index_id=3, reads=10),
        _index("Sales", "Orders", "IX_B", ("CustomerID",), index_id=4, reads=10),
        _index("Sales", "Orders", "IX_C", ("CustomerID",), index_id=5, includes=("Comments",), reads=10),
    ]

    report = build_index_advice(_inputs([], existing))

    recs = _recs(report, "consolidate_index", "Orders")
    assert sorted(rec["index_name"] for rec in recs) == ["IX_A", "IX_B"]
    assert all(rec["ddl"].startswith("DROP INDEX ") for rec in recs)
    assert _rebuilds_of(report, "IX_C") == 0


def test_prefix_chain_never_consolidates_into_an_index_being_dropped() -> None:
    existing = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_A", ("CustomerID",), index_id=3, includes=("Status",), reads=10),
        _index("Sales", "Orders", "IX_B", ("CustomerID", "OrderDate"), index_id=4, includes=("Comments",), reads=10),
        _index("Sales", "Orders", "IX_C", ("CustomerID", "OrderDate", "SalespersonPersonID"), index_id=5, reads=10),
    ]

    report = build_index_advice(_inputs([], existing))

    recs = _recs(report, "consolidate_index", "Orders")
    removed = {rec["index_name"] for rec in recs}
    assert all(rec["merge_into"] not in removed for rec in recs)
    assert _rebuilds_of(report, "IX_B") == 0
    assert _rebuilds_of(report, "IX_C") == 1
    [rec] = recs
    assert rec["index_name"] == "IX_A"
    assert rec["merge_into"] == "IX_C"
    assert "INCLUDE ([Status])" in rec["ddl"]
    review = [r for r in _recs(report, "review_index", "Orders") if r["index_name"] == "IX_B"]
    assert review and review[0]["reason_codes"] == ["survivor_rebuild_already_recommended"]


def test_survivor_that_cannot_absorb_includes_keeps_the_redundant_index() -> None:
    existing = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_Date", ("OrderDate",), index_id=3, includes=("Comments", "Status"), reads=50),
        _index("Sales", "Orders", "IX_Date_Cust", ("OrderDate", "CustomerID"), index_id=4, includes=("SalespersonPersonID",), reads=700),
    ]

    report = build_index_advice(
        _inputs([], existing, settings=AdvisorSettings(max_include_columns=2))
    )

    assert _recs(report, "consolidate_index", "Orders") == []
    assert _rebuilds_of(report, "IX_Date_Cust") == 0
    review = [r for r in _recs(report, "review_index", "Orders") if r["index_name"] == "IX_Date"]
    assert review and review[0]["reason_codes"] == ["survivor_cannot_absorb_includes"]


def test_consolidation_survivor_is_never_also_a_drop_candidate() -> None:
    existing = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_Narrow", ("CustomerID",), index_id=3, includes=("Status",), reads=1_000),
        _index("Sales", "Orders", "IX_Wide", ("CustomerID", "OrderDate"), index_id=4, reads=0, updates=50_000),
    ]

    report = build_index_advice(
        _inputs([], existing, references={("sales", "orders", "ix_wide"): 0})
    )

    # Applying both would remove every index that serves IX_Narrow's seeks.
    [rec] = _recs(report, "consolidate_index", "Orders")
    assert rec["merge_into"] == "IX_Wide"
    assert [r["index_name"] for r in _recs(report, "drop_index", "Orders")] == []


def test_filtered_index_is_not_extended_for_queries_outside_its_filter() -> None:
    existing = [
        _orders_clustered(),
        _index(
            "Sales",
            "Orders",
            "IX_Open",
            ("CustomerID", "Status", "OrderDate"),
            index_id=2,
            filter_definition="([SalespersonPersonID]=(1))",
        ),
    ]

    report = build_index_advice(
        _inputs(
            [_query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000)],
            existing,
            selectivity=ORDERS_SELECTIVITY,
        )
    )

    # The plan keeps no constants, so the query cannot be proven to match the
    # filter; rebuilding the filtered index would change nothing for it.
    assert _recs(report, "extend_index", "Orders") == []
    [create] = _recs(report, "create_index", "Orders")
    assert _keys(create) == ["CustomerID", "Status", "OrderDate"]
    assert "WHERE" not in create["ddl"]


ORDERLINES_SELECTIVITY = {
    ("sales", "orderlines", "stockitemid"): ColumnSelectivity(distinct_estimate=227, rows=231_412),
    ("sales", "orderlines", "packagetypeid"): ColumnSelectivity(distinct_estimate=4, rows=231_412),
    ("sales", "orderlines", "pickingcompletedwhen"): ColumnSelectivity(distinct_estimate=1_000, rows=231_412),
}


def _orderlines_report(columns: dict[str, ColumnInfo]) -> dict[str, Any]:
    existing = [
        _index("Sales", "OrderLines", "PK_Sales_OrderLines", ("OrderLineID",), index_id=1, type_code=1, primary_key=True)
    ]
    inputs = _inputs(
        [_query(2, "clustered_scan_residuals.xml", cpu_us=2_000_000)],
        existing,
        selectivity=ORDERLINES_SELECTIVITY,
    )
    inputs.columns[("sales", "orderlines")] = columns
    return build_index_advice(inputs)


def test_key_wider_than_1700_bytes_is_never_emitted() -> None:
    columns = dict(ORDERLINES_COLUMNS)
    # nvarchar(1000): 2,000 bytes declared, over the 1,700-byte key limit alone.
    columns["pickingcompletedwhen"] = ColumnInfo("PickingCompletedWhen", "nvarchar", 2000)

    report = _orderlines_report(columns)

    [rec] = _recs(report, "create_index", "OrderLines")
    assert _keys(rec)[0] == "PickingCompletedWhen"
    assert "key_width_exceeds_1700_bytes" in rec["blockers"]
    assert rec["confidence"] == "low"
    assert rec["ddl"] is None
    assert rec["rollback_ddl"] is None
    assert any("1946" in risk for risk in rec["risks"])


def test_trailing_wide_key_moves_to_include() -> None:
    columns = dict(ORDERS_COLUMNS)
    columns["status"] = ColumnInfo("Status", "nvarchar", 1800)
    inputs = _inputs(
        [_query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000)],
        [_orders_clustered()],
        selectivity=ORDERS_SELECTIVITY,
    )
    inputs.columns[("sales", "orders")] = columns

    report = build_index_advice(inputs)

    [rec] = _recs(report, "create_index", "Orders")
    # The seekable leading key stays; wide trailing keys move to INCLUDE.
    assert _keys(rec) == ["CustomerID"]
    assert {"Status", "OrderDate", "Comments"} <= set(rec["include_columns"])
    assert "key_columns_moved_to_include_for_1700_byte_limit" in rec["reason_codes"]
    assert rec["ddl"].startswith(
        "CREATE NONCLUSTERED INDEX [IX_Orders_CustomerID] ON [Sales].[Orders] ([CustomerID] ASC) INCLUDE ("
    )
    assert rec["blockers"] == []


def test_non_indexable_computed_column_is_never_a_key() -> None:
    def with_computed(is_indexable: bool | None) -> dict[str, ColumnInfo]:
        columns = dict(ORDERLINES_COLUMNS)
        columns["pickingcompletedwhen"] = ColumnInfo(
            "PickingCompletedWhen",
            "datetime2",
            8,
            is_computed=True,
            is_indexable=is_indexable,
            is_deterministic=is_indexable,
        )
        return columns

    unknown = _orderlines_report(with_computed(None))
    indexable = _orderlines_report(with_computed(True))

    [unknown_rec] = _recs(unknown, "create_index", "OrderLines")
    assert "PickingCompletedWhen" not in _keys(unknown_rec)
    assert "PickingCompletedWhen" not in unknown_rec["include_columns"]
    [indexable_rec] = _recs(indexable, "create_index", "OrderLines")
    assert _keys(indexable_rec)[0] == "PickingCompletedWhen"


def test_nondeterministic_computed_column_is_never_included() -> None:
    columns = dict(ORDERLINES_COLUMNS)
    columns["description"] = ColumnInfo(
        "Description", "nvarchar", 200, is_computed=True, is_indexable=False, is_deterministic=False
    )

    report = _orderlines_report(columns)

    [rec] = _recs(report, "create_index", "OrderLines")
    assert "Description" not in rec["include_columns"]
    assert "computed_columns_not_includable" in rec["reason_codes"]


def test_computed_columns_without_metadata_are_reported_as_a_gap() -> None:
    def report_for(flags: bool | None) -> dict[str, Any]:
        columns = dict(ORDERS_COLUMNS)
        columns["comments"] = ColumnInfo(
            "Comments", "nvarchar", 400, is_computed=True, is_indexable=flags, is_deterministic=flags
        )
        inputs = _inputs(
            [_query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000)],
            [_orders_clustered()],
            selectivity=ORDERS_SELECTIVITY,
        )
        inputs.columns[("sales", "orders")] = columns
        return build_index_advice(inputs)

    unread = report_for(None)
    read = report_for(True)

    # Without IsIndexable/IsDeterministic the lookup cannot be covered. The lost
    # advice must show as a gap, not vanish silently.
    assert _recs(unread, "create_index", "Orders") == []
    assert any("Sales.Orders.Comments" in gap for gap in unread["gaps"])
    [rec] = _recs(read, "create_index", "Orders")
    assert "Comments" in rec["include_columns"]
    assert not any("Comments" in gap for gap in read["gaps"])


def test_fitted_key_with_only_low_selectivity_columns_is_capped_at_medium() -> None:
    columns = dict(ORDERS_COLUMNS)
    columns["status"] = ColumnInfo("Status", "nvarchar", 1800)
    selectivity = dict(ORDERS_SELECTIVITY)
    selectivity[("sales", "orders", "customerid")] = ColumnSelectivity(distinct_estimate=5, rows=73_595)
    inputs = _inputs(
        [_query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000)],
        [_orders_clustered()],
        selectivity=selectivity,
    )
    inputs.columns[("sales", "orders")] = columns

    report = build_index_advice(inputs)

    [rec] = _recs(report, "create_index", "Orders")
    # OrderDate made the designed key selective; after the 1,700-byte fit only
    # the 5-value CustomerID is left, so the optimizer may still prefer a scan.
    assert _keys(rec) == ["CustomerID"]
    assert "all_key_columns_low_selectivity" in rec["reason_codes"]
    assert rec["confidence"] == "medium"


def test_partitioned_page_index_widen_is_one_online_rebuild_keeping_compression() -> None:
    partitioned = replace(
        _orders_indexes()[1],
        partition_columns=("OrderDate",),
        data_space_name="ps_Date",
        data_space_type="PARTITION_SCHEME",
        partition_scheme_name="ps_Date",
        partition_compression=((1, "PAGE"), (2, "PAGE"), (3, "PAGE")),
        xml_compression=((1, "OFF"), (2, "OFF"), (3, "OFF")),
        partition_page_counts=((1, 100), (2, 100), (3, 100)),
    )
    existing = [_orders_indexes()[0], partitioned]

    report = build_index_advice(
        _inputs(
            [_query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000)],
            existing,
            selectivity=ORDERS_SELECTIVITY,
        )
    )

    [rec] = _recs(report, "widen_index", "Orders")
    for ddl in (rec["ddl"], rec["rollback_ddl"]):
        # Offline per-partition rebuilds would take Sch-M locks and leave an
        # uncompressed copy in between.
        assert "ALTER INDEX" not in ddl
        assert ddl.count("CREATE NONCLUSTERED INDEX") == 1
        assert "DROP_EXISTING = ON, ONLINE = ON" in ddl
        assert "DATA_COMPRESSION = PAGE ON PARTITIONS (1, 2, 3)" in ddl
        assert "ON [ps_Date] ([OrderDate]);" in ddl


# --- W11b: hint and forced-plan pins, Query Store window, proof sets, statistics ---


def _forced_index_query(query_id: int, forced_index: str = "1", *, is_forced_plan: bool = False) -> WorkloadQuery:
    text = (FIXTURES / "seek_forced_index_hint.xml").read_text(encoding="utf-8")
    text = text.replace('ForcedIndex="1"', f'ForcedIndex="{forced_index}"')
    query = _query(query_id, "seek_forced_index_hint.xml", cpu_us=900_000)
    return replace(query, plan=parse_plan_access(text), is_forced_plan=is_forced_plan)


NO_DESIGN = AdvisorSettings(min_table_rows=1_000_000)


@pytest.mark.parametrize("forced_index", ["1", "true"])
def test_exact_duplicate_keeps_the_index_named_by_a_forced_index_hint(forced_index: str) -> None:
    existing = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_Orders_Cust_A", ("CustomerID",), index_id=3, includes=("OrderDate", "Status"), reads=900),
        _index("Sales", "Orders", "IX_Orders_Cust_Hinted", ("CustomerID",), index_id=4, reads=50),
    ]

    report = build_index_advice(_inputs([_forced_index_query(42, forced_index)], existing, settings=NO_DESIGN))

    # Dropping the hinted index fails query 42 with Msg 308, so the hinted twin
    # survives and absorbs the other twin's includes.
    [rec] = _recs(report, "consolidate_index", "Orders")
    assert rec["index_name"] == "IX_Orders_Cust_A"
    assert rec["merge_into"] == "IX_Orders_Cust_Hinted"
    assert rec["ddl"].endswith("DROP INDEX [IX_Orders_Cust_A] ON [Sales].[Orders];")
    assert "CREATE NONCLUSTERED INDEX [IX_Orders_Cust_Hinted]" in rec["ddl"]
    assert not any(
        "DROP INDEX [IX_Orders_Cust_Hinted]" in (r["ddl"] or "") for r in report["recommendations"]
    )


def test_left_prefix_index_used_by_a_forced_plan_has_no_drop_ddl() -> None:
    existing = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_Orders_Cust_Hinted", ("CustomerID",), index_id=3, reads=50),
        _index("Sales", "Orders", "IX_Cust_Date", ("CustomerID", "OrderDate"), index_id=4, reads=900),
    ]
    forced = _forced_index_query(42, "0", is_forced_plan=True)

    report = build_index_advice(_inputs([forced], existing, settings=NO_DESIGN))

    # The action stays visible, but change control gets no executable outage:
    # the forced plan would fail with NO_INDEX once the index is gone.
    [rec] = _recs(report, "consolidate_index", "Orders")
    assert rec["index_name"] == "IX_Orders_Cust_Hinted"
    assert rec["merge_into"] == "IX_Cust_Date"
    assert "index_named_by_hint_or_forced_plan" in rec["blockers"]
    assert "forced_query_store_plan" in rec["reason_codes"]
    assert rec["confidence"] == "low"
    assert rec["ddl"] is None
    assert rec["rollback_ddl"] is None
    assert any("query 42" in step and "plan 420" in step for step in rec["prerequisites"])


def test_unused_index_referenced_only_by_a_forced_plan_outside_the_top_queries_is_not_dropped() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Unused", ("ExpectedDeliveryDate",), index_id=5, reads=0, updates=50_000)
    ]

    report = build_index_advice(
        _inputs(
            [],
            existing,
            references={("sales", "orders", "ix_unused"): 0},
            window_days=40,
            forced_plan_accesses=[(77, 770, "Sales", "Orders", "IX_Unused")],
        )
    )

    [rec] = _recs(report, "drop_index", "Orders")
    assert rec["index_name"] == "IX_Unused"
    assert "index_named_by_hint_or_forced_plan" in rec["blockers"]
    assert rec["confidence"] == "low"
    assert rec["ddl"] is None and rec["rollback_ddl"] is None
    assert any("query 77" in step and "plan 770" in step for step in rec["prerequisites"])


def test_index_named_by_a_module_hint_has_no_drop_ddl() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Unused", ("ExpectedDeliveryDate",), index_id=5, reads=0, updates=50_000)
    ]

    report = build_index_advice(
        _inputs(
            [],
            existing,
            references={("sales", "orders", "ix_unused"): 0},
            window_days=40,
            hint_references={("sales", "orders", "ix_unused"): ["module object_id 1234"]},
        )
    )

    [rec] = _recs(report, "drop_index", "Orders")
    assert "index_named_by_hint_or_forced_plan" in rec["blockers"]
    assert "index_named_in_hint_plan_guide_or_module" in rec["reason_codes"]
    assert rec["ddl"] is None
    assert any("module object_id 1234" in step for step in rec["prerequisites"])


def test_two_pinned_exact_duplicates_are_both_kept() -> None:
    existing = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_A", ("SalespersonPersonID",), index_id=3, includes=("OrderDate",), reads=10),
        _index("Sales", "Orders", "IX_B", ("SalespersonPersonID",), index_id=4, reads=10),
    ]
    hints = {
        ("sales", "orders", "ix_a"): ["plan guide 7"],
        ("sales", "orders", "ix_b"): ["the Query Store hint on query 12"],
    }

    report = build_index_advice(_inputs([], existing, hint_references=hints))

    assert _recs(report, "consolidate_index", "Orders") == []
    assert not any("DROP INDEX" in (r["ddl"] or "") for r in report["recommendations"])
    [review] = [
        r for r in _recs(report, "review_index", "Orders")
        if r["reason_codes"] == ["duplicate_pinned_by_hint_or_forced_plan"]
    ]
    assert review["index_name"] in {"IX_A", "IX_B"}


def test_unpinned_twin_of_a_pinned_index_keeps_its_executable_drop() -> None:
    existing = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_A", ("SalespersonPersonID",), index_id=3, reads=10),
        _index("Sales", "Orders", "IX_B", ("SalespersonPersonID",), index_id=4, includes=("OrderDate",), reads=900),
    ]

    report = build_index_advice(
        _inputs([], existing, hint_references={("sales", "orders", "ix_a"): ["plan guide 7"]})
    )

    [rec] = _recs(report, "consolidate_index", "Orders")
    assert (rec["index_name"], rec["merge_into"]) == ("IX_B", "IX_A")
    assert rec["ddl"].endswith("DROP INDEX [IX_B] ON [Sales].[Orders];")
    assert rec["blockers"] == []
    assert rec["confidence"] == "high"


def test_incomplete_pin_checks_cap_cleanup_confidence_at_medium() -> None:
    duplicates = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Salesperson", ("SalespersonPersonID",), index_id=3, reads=10),
        _index("Sales", "Orders", "IX_Salesperson_Dates", ("SalespersonPersonID",), index_id=4, includes=("OrderDate",), reads=900),
        _index("Sales", "Orders", "IX_Unused", ("ExpectedDeliveryDate",), index_id=5, reads=0, updates=50_000),
    ]

    report = build_index_advice(
        _inputs(
            [],
            duplicates,
            references={("sales", "orders", "ix_unused"): 0},
            window_days=40,
            pin_coverage="incomplete",
        )
    )

    for rec in _recs(report, "consolidate_index", "Orders") + _recs(report, "drop_index", "Orders"):
        assert rec["confidence"] == "medium"
        assert {"forced_plan_dependency_check_incomplete", "hint_reference_check_incomplete"} <= set(rec["blockers"])
        assert rec["ddl"] is not None
    assert len(_recs(report, "drop_index", "Orders")) == 1


def test_unused_drop_needs_35_days_of_usage_counters_for_high_confidence() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Unused", ("ExpectedDeliveryDate",), index_id=5, reads=0, updates=50_000)
    ]

    def drop_after(days: int) -> dict[str, Any]:
        report = build_index_advice(
            _inputs(
                [],
                existing,
                references={("sales", "orders", "ix_unused"): 0},
                window_days=40,
                engine_start=_utc(days),
            )
        )
        [rec] = _recs(report, "drop_index", "Orders")
        return rec

    # 35 days always holds one full month-end plus a buffer (owner decision 2026-10-02).
    assert drop_after(34)["confidence"] == "medium"
    assert drop_after(36)["confidence"] == "high"


def test_query_store_retention_shorter_than_the_removal_window_is_a_gap() -> None:
    def gaps(stale_days: int) -> str:
        report = build_index_advice(
            _inputs([], _orders_indexes(), query_store_options={"stale_query_threshold_days": stale_days})
        )
        return " | ".join(report["gaps"])

    assert "35-day index removal window" in gaps(30)
    assert "stale_query_threshold_days" not in gaps(60)


def test_write_rates_and_coverage_use_the_effective_query_store_window() -> None:
    def report_for(lookback: int) -> dict[str, Any]:
        read = _query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000)
        update = _query(9, "update_scan_dml.xml", cpu_us=50_000, rowcount=200_000)
        return build_index_advice(
            _inputs(
                [read, update],
                _orders_indexes(),
                selectivity=ORDERS_SELECTIVITY,
                window_days=lookback,
                query_store_days=3,
                query_store_options={
                    "stale_query_threshold_days": 3,
                    "current_storage_size_mb": 100,
                    "max_storage_size_mb": 100,
                    "size_based_cleanup_mode": "AUTO",
                },
            )
        )

    reports = {lookback: report_for(lookback) for lookback in (7, 30, 90)}

    # The 200,000 rows were written in the 3 days Query Store holds, whatever window was asked for.
    impacts = [_recs(report, "widen_index", "Orders")[0]["write_impact"] for report in reports.values()]
    assert impacts[0] == impacts[1] == impacts[2]
    assert impacts[0]["dml_rows_per_day"] == pytest.approx(200_000 / 3, abs=0.1)
    for lookback, report in reports.items():
        window = report["window"]
        assert window["lookback_days"] == window["requested_days"] == lookback
        assert window["query_store_effective_days"] == pytest.approx(3.0)
        assert window["query_store_interval_count"] == 72
        orders = next(t for t in report["tables"] if t["table"] == "Orders")
        assert orders["write_activity"]["dml_rows_per_day"] == pytest.approx(200_000 / 3, abs=0.1)
        gaps = " | ".join(report["gaps"])
        assert f"Query Store holds 3.0 of the requested {lookback} day(s)" in gaps
        assert "stale_query_threshold_days" in gaps
        assert "100% of max_storage_size_mb" in gaps


def test_short_query_store_window_caps_unused_drop_at_medium() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Unused", ("ExpectedDeliveryDate",), index_id=5, reads=0, updates=50_000)
    ]
    references = {("sales", "orders", "ix_unused"): 0}

    short = build_index_advice(_inputs([], existing, references=references, window_days=40, query_store_days=5))
    unknown = build_index_advice(_inputs([], existing, references=references, window_days=40, coverage_known=False))

    # 61 days of counters, but plan references were only checked over the 5 days Query Store holds.
    [rec] = _recs(short, "drop_index", "Orders")
    assert rec["confidence"] == "medium"
    assert "query_store_reference_window_shorter_than_usage_window" in rec["blockers"]
    assert rec["reference_window_days"] == pytest.approx(5.0)
    assert "no_query_store_plan_reference_in_window" not in rec["reason_codes"]
    assert rec["ddl"] == "DROP INDEX [IX_Unused] ON [Sales].[Orders];"
    [rec] = _recs(unknown, "drop_index", "Orders")
    assert rec["confidence"] == "medium"
    assert "query_store_window_coverage_unknown" in rec["blockers"]


def test_sandbox_validation_names_the_most_expensive_supporting_query() -> None:
    queries = [
        _query(5, "seek_residual_lookup_sort.xml", cpu_us=1_000),
        _query(999, "seek_residual_lookup_sort.xml", cpu_us=899_000),
    ]

    report = build_index_advice(_inputs(queries, _orders_indexes(), selectivity=ORDERS_SELECTIVITY))

    [rec] = _recs(report, "widen_index", "Orders")
    validation = rec["validation"]
    # Query 999 carries 99.9% of the cost; proving the index on query 5 proves nothing.
    assert [item["query_id"] for item in validation["proof_set"]] == [999]
    assert validation["proof_set"][0]["plan_id"] == 9990
    assert validation["proof_set"][0]["share_of_recommendation_pct"] == pytest.approx(99.889, abs=0.001)
    assert "query 999" in validation["sandbox"]
    assert "query 5 " not in validation["sandbox"]


def test_proof_set_covers_80_percent_capped_at_three() -> None:
    def proof_ids(costs: list[float]) -> list[int]:
        queries = [
            _query(position + 1, "seek_residual_lookup_sort.xml", cpu_us=cost)
            for position, cost in enumerate(costs)
        ]
        report = build_index_advice(_inputs(queries, _orders_indexes(), selectivity=ORDERS_SELECTIVITY))
        [rec] = _recs(report, "widen_index", "Orders")
        return [item["query_id"] for item in rec["validation"]["proof_set"]]

    assert proof_ids([600_000, 250_000, 100_000, 50_000]) == [1, 2]
    assert proof_ids([200_000] * 5) == [1, 2, 3]


def test_widen_lists_other_users_of_the_target_index_as_regression_set() -> None:
    other_text = (FIXTURES / "hash_join_stream_aggregate.xml").read_text(encoding="utf-8").replace(
        'Index="[PK_Sales_Orders]" Alias="[o]" IndexKind="Clustered"',
        'Index="[FK_Sales_Orders_CustomerID]" Alias="[o]" IndexKind="NonClustered"',
    )
    other = replace(
        _query(2, "hash_join_stream_aggregate.xml", cpu_us=300_000), plan=parse_plan_access(other_text)
    )
    queries = [_query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000), other]

    report = build_index_advice(_inputs(queries, _orders_indexes(), selectivity=ORDERS_SELECTIVITY))

    # Query 2 reads the index that DROP_EXISTING rebuilds with new keys.
    [rec] = _recs(report, "widen_index", "Orders")
    validation = rec["validation"]
    assert [item["query_id"] for item in validation["proof_set"]] == [1]
    assert [item["query_id"] for item in validation["regression_set"]] == [2]
    assert "regression_set" in validation["after_change"]
    assert "20%" in validation["after_change"]


def test_consolidation_lists_the_redundant_index_users_as_regression_set() -> None:
    existing = [
        _orders_clustered(),
        _index("Sales", "Orders", "IX_Orders_Cust_Hinted", ("CustomerID",), index_id=3, reads=50),
        _index("Sales", "Orders", "IX_Cust_Date", ("CustomerID", "OrderDate"), index_id=4, reads=900),
    ]

    report = build_index_advice(_inputs([_forced_index_query(42, "0")], existing, settings=NO_DESIGN))

    [rec] = _recs(report, "consolidate_index", "Orders")
    assert [item["query_id"] for item in rec["validation"]["regression_set"]] == [42]


def test_drop_of_index_whose_statistics_plans_use_creates_replacement_statistics_first() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Unused", ("ExpectedDeliveryDate", "Status"), index_id=5, reads=0, updates=50_000)
    ]
    key = ("sales", "orders", "ix_unused")

    report = build_index_advice(
        _inputs([], existing, references={key: 0}, statistics={key: 2}, window_days=40)
    )

    # Plans read the index's multi-column density and full-scan histogram even
    # though nothing seeks it; auto-created statistics would not replace them.
    [rec] = _recs(report, "drop_index", "Orders")
    assert rec["ddl"] == (
        "CREATE STATISTICS [st_IX_Unused] ON [Sales].[Orders] ([ExpectedDeliveryDate], [Status]) "
        "WITH FULLSCAN, PERSIST_SAMPLE_PERCENT = ON;\n"
        "DROP INDEX [IX_Unused] ON [Sales].[Orders];"
    )
    # Rollback re-creates the index first, then removes the stand-in statistics.
    assert "CREATE NONCLUSTERED INDEX [IX_Unused]" in rec["rollback_ddl"]
    assert rec["rollback_ddl"].endswith("\nDROP STATISTICS [Sales].[Orders].[st_IX_Unused];")
    assert "index_statistics_used_by_optimizer" in rec["reason_codes"]
    assert rec["confidence"] == "medium"
    assert any("FULLSCAN" in risk and "SAMPLE" in risk for risk in rec["risks"])


def test_replacement_statistics_name_stays_a_valid_identifier() -> None:
    name = "IX_" + "x" * 125
    existing = _orders_indexes() + [
        _index("Sales", "Orders", name, ("ExpectedDeliveryDate",), index_id=5, reads=0, updates=50_000)
    ]
    key = ("sales", "orders", name.casefold())

    report = build_index_advice(_inputs([], existing, references={key: 0}, statistics={key: 1}, window_days=40))

    [rec] = _recs(report, "drop_index", "Orders")
    statistics_name = rec["ddl"].split("[", 1)[1].split("]", 1)[0]
    assert statistics_name.startswith("st_IX_") and len(statistics_name) <= 128


def test_unknown_statistics_usage_caps_drop_at_medium() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Unused", ("ExpectedDeliveryDate",), index_id=5, reads=0, updates=50_000)
    ]
    key = ("sales", "orders", "ix_unused")

    report = build_index_advice(
        _inputs([], existing, references={key: 0}, statistics={key: None}, window_days=40)
    )

    [rec] = _recs(report, "drop_index", "Orders")
    assert "statistics_reference_check_unavailable" in rec["blockers"]
    assert rec["confidence"] == "medium"
    assert rec["ddl"] == "DROP INDEX [IX_Unused] ON [Sales].[Orders];"


def test_consolidation_needs_no_replacement_statistics() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Salesperson", ("SalespersonPersonID",), index_id=3, reads=10),
        _index("Sales", "Orders", "IX_Salesperson_Dates", ("SalespersonPersonID",), index_id=4, includes=("OrderDate",), reads=900),
    ]
    key = ("sales", "orders", "ix_salesperson")

    report = build_index_advice(_inputs([], existing, statistics={key: 3}))

    # The survivor has the same leading keys, so its statistics carry the same histogram and density.
    [rec] = _recs(report, "consolidate_index", "Orders")
    assert rec["ddl"] == "DROP INDEX [IX_Salesperson] ON [Sales].[Orders];"
    assert "CREATE STATISTICS" not in rec["ddl"]


def test_thin_query_store_history_never_makes_a_one_day_query_recurring() -> None:
    def widen(window_days: int, query_store_days: float | None) -> dict[str, Any]:
        query = _query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000, active_days=1)
        report = build_index_advice(
            _inputs(
                [query],
                _orders_indexes(),
                selectivity=ORDERS_SELECTIVITY,
                window_days=window_days,
                query_store_days=query_store_days,
            )
        )
        [rec] = _recs(report, "widen_index", "Orders")
        return rec

    # A 30-day review where Query Store was purged to 1.5 days has less evidence
    # than a full 30 days, so it must not earn a high-confidence rebuild.
    assert widen(30, None)["confidence"] == "medium"
    assert widen(30, 1.5)["confidence"] == "medium"
    # A one-day review cannot show a second active day; that is the operator's choice.
    assert widen(1, None)["confidence"] == "high"


def test_unresolved_index_hint_withholds_every_removal_ddl() -> None:
    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_Salesperson", ("SalespersonPersonID",), index_id=3, reads=10),
        _index("Sales", "Orders", "IX_Salesperson_Dates", ("SalespersonPersonID",), index_id=4, includes=("OrderDate",), reads=900),
        _index("Sales", "Orders", "IX_Unused", ("ExpectedDeliveryDate",), index_id=5, reads=0, updates=50_000),
    ]

    report = build_index_advice(
        _inputs(
            [],
            existing,
            references={("sales", "orders", "ix_unused"): 0},
            window_days=40,
            hint_coverage="unresolved",
        )
    )

    # A hint the scan could not tie to exactly one index (other case, or a name on
    # two tables) may name any of these; dropping it fails that query with Msg 308.
    removals = _recs(report, "consolidate_index", "Orders") + _recs(report, "drop_index", "Orders")
    assert {rec["index_name"] for rec in removals} == {"IX_Salesperson", "IX_Unused"}
    for rec in removals:
        assert "unresolved_index_hint" in rec["blockers"]
        assert rec["confidence"] == "low"
        assert rec["ddl"] is None and rec["rollback_ddl"] is None
        assert any(rec["index_name"] in step for step in rec["prerequisites"])
