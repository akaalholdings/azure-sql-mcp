"""Columnstore advice: protection, B-tree for lookups, design leads, row-group maintenance.

A fact table with a clustered columnstore index used to get no advice and no
explanation: the advisor skipped every columnstore access.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from azure_sql_mcp import workload_index_advisor as workload_module
from azure_sql_mcp.columnstore_health import summarize_columnstore_indexes
from azure_sql_mcp.index_advisor import AdvisorInputs
from azure_sql_mcp.index_advisor import AdvisorSettings
from azure_sql_mcp.index_advisor import ColumnInfo
from azure_sql_mcp.index_advisor import TableInfo
from azure_sql_mcp.index_advisor import WorkloadQuery
from azure_sql_mcp.index_advisor import build_index_advice
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.showplan_access import DmlTarget
from azure_sql_mcp.showplan_access import PlanAccessSummary
from azure_sql_mcp.showplan_access import StatementAccess
from azure_sql_mcp.showplan_access import TableAccess
from azure_sql_mcp.workload_index_advisor import WorkloadIndexAdvisor
from tests.unit.test_index_advisor import _index
from tests.unit.test_index_advisor import _inputs
from tests.unit.test_index_advisor import _query
from tests.unit.test_workload_index_advisor import NOW as WORKLOAD_NOW
from tests.unit.test_workload_index_advisor import RoutingExecutor

FACT_ROWS = 50_331_648
FACT_COLUMNS = {
    name.casefold(): ColumnInfo(name=name, type_name=type_name, max_length=length)
    for name, type_name, length in (
        ("SaleId", "bigint", 8),
        ("StoreId", "int", 4),
        ("SaleDate", "date", 3),
        ("Amount", "decimal", 9),
        ("Region", "varchar", 20),
        ("Notes", "nvarchar", -1),
    )
}


def _columnstore(name: str, *, clustered: bool, reads: int | None = 0) -> ExistingIndex:
    base = _index("dbo", "FactSales", name, ("SaleId",), index_id=1 if clustered else 5, reads=reads)
    return replace(
        base,
        key_columns=(),
        index_type="CLUSTERED COLUMNSTORE" if clustered else "NONCLUSTERED COLUMNSTORE",
        index_type_code=5 if clustered else 6,
        partition_compression=((1, "COLUMNSTORE"),),
    )


def _scan(
    *,
    table: str = "FactSales",
    index: str = "CCI_FactSales",
    storage: str = "ColumnStore",
    residual: dict[str, str] | None = None,
    output: tuple[str, ...] = ("Amount",),
    estimated_rows: float,
    cardinality: float = FACT_ROWS,
) -> PlanAccessSummary:
    access = TableAccess(
        node_id=2,
        physical_op="Clustered Index Scan",
        logical_op="Clustered Index Scan",
        schema="dbo",
        table=table,
        alias=None,
        index_name=index,
        index_kind="Clustered",
        storage=storage,
        operation="scan",
        residual=residual or {},
        output_columns=output,
        estimated_rows=estimated_rows,
        table_cardinality=cardinality,
        own_cost=10.0,
        cost_share=1.0,
    )
    return PlanAccessSummary(
        statements=[
            StatementAccess(
                statement_type="SELECT", statement_cost=10.0, query_hash=None, plan_hash=None, accesses=[access]
            )
        ]
    )


def _workload_query(query_id: int, plan: PlanAccessSummary, *, cpu_us: float, rowcount: float = 1000.0) -> WorkloadQuery:
    return WorkloadQuery(
        query_id=query_id,
        plan_id=query_id * 10,
        plan=plan,
        executions=1000,
        total_cpu_us=cpu_us,
        total_duration_us=cpu_us * 2,
        total_logical_reads=cpu_us / 10,
        total_rowcount=rowcount,
        active_days=5,
    )


def _fact_inputs(
    queries: list[WorkloadQuery],
    existing: list[ExistingIndex],
    *,
    rows: int = FACT_ROWS,
    health: list[dict[str, Any]] | None = None,
    **kwargs: Any,
) -> AdvisorInputs:
    inputs = _inputs(queries, existing, **kwargs)
    inputs.tables[("dbo", "factsales")] = TableInfo(
        "dbo", "FactSales", object_id=9, row_count=rows, all_used_pages=400_000
    )
    inputs.columns[("dbo", "factsales")] = FACT_COLUMNS
    inputs.columnstore_health = health
    return inputs


def _by_action(report: dict[str, Any], action: str) -> list[dict[str, Any]]:
    return [rec for rec in report["recommendations"] if rec["action"] == action]


def _reasons(report: dict[str, Any]) -> set[str]:
    return {code for rec in report["recommendations"] for code in rec["reason_codes"]}


# --- (a) protection ---------------------------------------------------------


def test_unused_columnstore_indexes_are_never_removal_candidates_and_the_report_says_why() -> None:
    """Zero reads for 60 days would drop a rowstore index; a columnstore index stays, with the reason."""

    existing = [
        _columnstore("CCI_FactSales", clustered=True, reads=0),
        _index("dbo", "FactSales", "IX_FactSales_StoreId", ("StoreId",), index_id=3, reads=0),
    ]
    ncci = replace(_columnstore("NCCI_FactSales", clustered=False, reads=0), table="FactSales")
    references = {("dbo", "factsales", "ix_factsales_storeid"): 0}
    report = build_index_advice(_fact_inputs([], [*existing, ncci], references=references, window_days=40))

    removed = {rec["index_name"] for rec in report["recommendations"] if rec["action"] in {"drop_index", "consolidate_index"}}
    assert removed == {"IX_FactSales_StoreId"}
    protected = {
        rec["index_name"]: rec
        for rec in _by_action(report, "review_index")
        if "columnstore_index_protected" in rec["reason_codes"]
    }
    assert set(protected) == {"CCI_FactSales", "NCCI_FactSales"}
    for rec in protected.values():
        assert rec["ddl"] is None and rec["rollback_ddl"] is None
        assert "never proposes dropping, disabling or replacing" in rec["rationale"]
    assert "table's storage" in protected["CCI_FactSales"]["rationale"]


# --- (b) B-tree for selective lookups ---------------------------------------


def test_selective_lookup_on_a_columnstore_table_gets_a_btree_candidate_with_its_load_cost() -> None:
    """A point lookup scans the whole columnstore; a B-tree on the lookup column is the fix."""

    plan = _scan(residual={"SaleId": "eq"}, output=("Amount", "StoreId"), estimated_rows=3)
    report = build_index_advice(
        _fact_inputs([_workload_query(1, plan, cpu_us=9_000_000)], [_columnstore("CCI_FactSales", clustered=True)])
    )

    creates = _by_action(report, "create_index")
    assert len(creates) == 1
    rec = creates[0]
    assert [key["name"] for key in rec["key_columns"]] == ["SaleId"]
    assert "columnstore_selective_lookup" in rec["reason_codes"]
    assert rec["ddl"].startswith("CREATE NONCLUSTERED INDEX")
    assert "COLUMNSTORE" not in rec["ddl"]
    # The cost model of a columnstore scan is a rough guide and the load cost is unmeasured.
    assert rec["confidence"] != "high"
    load_risk = " ".join(rec["risks"])
    assert "bulk load" in load_risk and "row by row" in load_risk


def test_analytic_columnstore_scan_gets_an_explanation_instead_of_a_btree() -> None:
    """A scan that returns 3% of a 50M-row fact table is what columnstore is for."""

    query = _query(1, "columnstore_scan_no_segment_elimination.xml", cpu_us=9_000_000)
    report = build_index_advice(_fact_inputs([query], [_columnstore("CCI_FactSales", clustered=True)]))

    assert _by_action(report, "create_index") == []
    explained = [
        rec for rec in _by_action(report, "review_index") if "columnstore_scan_not_a_btree_candidate" in rec["reason_codes"]
    ]
    assert len(explained) == 1
    assert explained[0]["index_name"] == "CCI_FactSales"
    assert [item["query_id"] for item in explained[0]["supporting_queries"]] == [1]
    assert "columnstore_no_segment_elimination" in explained[0]["rationale"]


# --- (c) nonclustered columnstore design lead -------------------------------


def _bare_scan(*, rows: float = 5_000_000) -> PlanAccessSummary:
    return _scan(
        table="FactSales",
        index="PK_FactSales",
        storage="RowStore",
        output=("StoreId", "Amount", "Notes"),
        estimated_rows=rows,
        cardinality=rows,
    )


def _rowstore_pk() -> ExistingIndex:
    return _index("dbo", "FactSales", "PK_FactSales", ("SaleId",), index_id=1, type_code=1, primary_key=True)


def test_bare_scan_of_a_large_rowstore_table_raises_a_columnstore_design_lead_without_ddl() -> None:
    """Scan-and-aggregate over 5M rows: no B-tree helps, columnstore may. A lead, never a statement."""

    query = _workload_query(1, _bare_scan(), cpu_us=9_000_000, rowcount=40_000)
    report = build_index_advice(_fact_inputs([query], [_rowstore_pk()], rows=5_000_000))

    leads = _by_action(report, "consider_columnstore_index")
    assert len(leads) == 1
    lead = leads[0]
    assert lead["ddl"] is None and lead["rollback_ddl"] is None
    assert "design_lead_requires_sandbox_proof" in lead["blockers"]
    assert lead["confidence"] in {"low", "medium"}
    assert lead["estimated_max_benefit_pct"] is None
    # LOB columns cannot be in a nonclustered columnstore index.
    assert lead["candidate_columns"] == ["StoreId", "Amount"]
    conditions = " ".join(lead["conditions"])
    assert "5,000,000 rows" in conditions
    assert "S3" in conditions
    assert [item["query_id"] for item in lead["validation"]["proof_set"]] == [1]
    assert _by_action(report, "create_index") == []


def test_columnstore_lead_needs_a_large_table_light_writes_and_a_reducing_scan() -> None:
    """Each gate is shown against the same workload that raises the lead when the gate holds."""

    def leads(queries: list[WorkloadQuery], existing: list[ExistingIndex], rows: int) -> list[dict[str, Any]]:
        report = build_index_advice(_fact_inputs(queries, existing, rows=rows))
        return _by_action(report, "consider_columnstore_index")

    def scan_query(rows: int, returned_per_execution: float = 40) -> WorkloadQuery:
        return _workload_query(1, _bare_scan(rows=rows), cpu_us=9_000_000, rowcount=returned_per_execution * 1000)

    assert len(leads([scan_query(5_000_000)], [_rowstore_pk()], 5_000_000)) == 1

    # Under about one row group columnstore rarely helps.
    assert leads([scan_query(400_000)], [_rowstore_pk()], 400_000) == []

    # 2,000,000 rows written per day is 40% of the table.
    writer = PlanAccessSummary(
        statements=[
            StatementAccess(
                statement_type="UPDATE",
                statement_cost=1.0,
                query_hash=None,
                plan_hash=None,
                dml_targets=[DmlTarget("dbo", "FactSales", "Update", ())],
            )
        ]
    )
    churn = _workload_query(2, writer, cpu_us=1.0, rowcount=14_000_000)
    assert leads([scan_query(5_000_000), churn], [_rowstore_pk()], 5_000_000) == []

    # An export that returns every row it scans does not aggregate: batch mode gains little.
    assert leads([scan_query(5_000_000, returned_per_execution=5_000_000)], [_rowstore_pk()], 5_000_000) == []

    # The table already has a columnstore index: its scans belong to that index.
    with_ncci = [_rowstore_pk(), _columnstore("NCCI_FactSales", clustered=False, reads=5)]
    assert leads([scan_query(5_000_000)], with_ncci, 5_000_000) == []


def test_key_lookup_into_a_columnstore_table_is_covered_by_the_btree_that_feeds_it() -> None:
    """Lookups into a clustered columnstore index are costly per row; the feeding B-tree should cover."""

    seek = TableAccess(
        node_id=3, physical_op="Index Seek", logical_op="Index Seek", schema="dbo", table="FactSales",
        alias=None, index_name="IX_FactSales_StoreId", index_kind="NonClustered", storage="RowStore",
        operation="seek", seek_eq_columns=("StoreId",), output_columns=("StoreId",), estimated_rows=200,
        table_cardinality=FACT_ROWS, own_cost=1.0, cost_share=0.1,
    )
    lookup = TableAccess(
        node_id=4, physical_op="Key Lookup", logical_op="Key Lookup", schema="dbo", table="FactSales",
        alias=None, index_name="CCI_FactSales", index_kind="Clustered", storage="ColumnStore",
        operation="lookup", output_columns=("Amount",), estimated_rows=200, table_cardinality=FACT_ROWS,
        own_cost=9.0, cost_share=0.9, paired_index="IX_FactSales_StoreId", paired_node_id=3,
    )
    plan = PlanAccessSummary(
        statements=[
            StatementAccess(
                statement_type="SELECT", statement_cost=10.0, query_hash=None, plan_hash=None, accesses=[seek, lookup]
            )
        ]
    )
    existing = [
        _columnstore("CCI_FactSales", clustered=True, reads=9),
        _index("dbo", "FactSales", "IX_FactSales_StoreId", ("StoreId",), index_id=3, reads=900),
    ]
    report = build_index_advice(_fact_inputs([_workload_query(1, plan, cpu_us=9_000_000)], existing))

    extends = _by_action(report, "extend_index")
    assert len(extends) == 1
    assert extends[0]["target_index"] == "IX_FactSales_StoreId"
    assert extends[0]["include_columns"] == ["Amount"]


# --- (d) row-group health ---------------------------------------------------


def _row_groups(state: str, count: int, total_rows: int, deleted: int = 0, trim: str | None = "NO_TRIM") -> dict[str, Any]:
    return {
        "object_id": 9,
        "index_id": 1,
        "schema_name": "dbo",
        "table_name": "FactSales",
        "index_name": "CCI_FactSales",
        "index_type": "CLUSTERED COLUMNSTORE",
        "state_desc": state,
        "trim_reason_desc": trim,
        "row_group_count": count,
        "total_rows": total_rows,
        "deleted_rows": deleted,
        "size_in_bytes": 512 * 1024 * 1024,
    }


def _maintenance(report: dict[str, Any]) -> list[dict[str, Any]]:
    return _by_action(report, "columnstore_maintenance")


def test_deleted_rows_lead_gives_reorganize_and_the_rebuild_cost() -> None:
    health = summarize_columnstore_indexes([_row_groups("COMPRESSED", 48, 48_000_000, deleted=14_400_000)])
    report = build_index_advice(_fact_inputs([], [_columnstore("CCI_FactSales", clustered=True, reads=9)], health=health))

    leads = _maintenance(report)
    assert len(leads) == 1
    lead = leads[0]
    assert lead["index_name"] == "CCI_FactSales"
    assert "deleted_rows_20_percent_or_more" in lead["reason_codes"]
    assert lead["ddl"] == "ALTER INDEX [CCI_FactSales] ON [dbo].[FactSales] REORGANIZE;"
    assert lead["rollback_ddl"] is None
    assert lead["alternative_ddl"] == "ALTER INDEX [CCI_FactSales] ON [dbo].[FactSales] REBUILD WITH (ONLINE = ON);"
    assert "30.0%" in lead["rationale"]
    assert "512.0 MB" in lead["cost"]


def test_open_delta_stores_lead_compresses_all_row_groups() -> None:
    health = summarize_columnstore_indexes(
        [_row_groups("COMPRESSED", 40, 40_000_000), _row_groups("OPEN", 6, 3_000_000, trim=None)]
    )
    report = build_index_advice(_fact_inputs([], [_columnstore("CCI_FactSales", clustered=True, reads=9)], health=health))

    lead = _maintenance(report)[0]
    assert "delta_store_rows_at_or_above_one_row_group" in lead["reason_codes"]
    assert lead["ddl"] == (
        "ALTER INDEX [CCI_FactSales] ON [dbo].[FactSales] REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);"
    )
    assert "3,000,000" in lead["rationale"]


def test_undersized_row_groups_from_dictionary_pressure_are_not_sent_to_reorganize() -> None:
    """REORGANIZE cannot merge row groups that a dictionary cut short; say so instead of a futile statement."""

    health = summarize_columnstore_indexes([_row_groups("COMPRESSED", 400, 20_000_000, trim="DICTIONARY_SIZE")])
    report = build_index_advice(_fact_inputs([], [_columnstore("CCI_FactSales", clustered=True, reads=9)], health=health))

    lead = _maintenance(report)[0]
    assert {"undersized_row_groups", "dictionary_size_trims"} <= set(lead["reason_codes"])
    assert lead["ddl"] is None
    assert "dictionary" in lead["rationale"]


def test_healthy_columnstore_index_gets_no_maintenance_lead_and_unread_health_is_a_gap() -> None:
    health = summarize_columnstore_indexes([_row_groups("COMPRESSED", 48, 50_000_000, deleted=1_000)])
    existing = [_columnstore("CCI_FactSales", clustered=True, reads=9)]
    assert _maintenance(build_index_advice(_fact_inputs([], existing, health=health))) == []

    unread = build_index_advice(_fact_inputs([], existing, health=None))
    assert _maintenance(unread) == []
    assert any("row-group health was not read" in gap for gap in unread["gaps"])


def test_leads_do_not_take_the_place_of_actionable_recommendations() -> None:
    """The per-table cap counts executable recommendations; leads ride along."""

    health = summarize_columnstore_indexes([_row_groups("COMPRESSED", 48, 48_000_000, deleted=14_400_000)])
    plan = _scan(residual={"SaleId": "eq"}, output=("Amount",), estimated_rows=3)
    settings = AdvisorSettings(lookback_days=7, max_recommendations_per_table=1)
    report = build_index_advice(
        _fact_inputs(
            [_workload_query(1, plan, cpu_us=9_000_000)],
            [_columnstore("CCI_FactSales", clustered=True, reads=9)],
            health=health,
            settings=settings,
        )
    )
    assert len(_by_action(report, "create_index")) == 1
    assert len(_maintenance(report)) == 1


# --- the live review reads row-group health only when a columnstore index exists ---


class _HealthExecutor(RoutingExecutor):
    def __init__(self, health: Any, **overrides: Any) -> None:
        super().__init__(**overrides)
        self.health = health
        self.health_reads = 0

    async def fetch_all(self, database_name: str, query: str, params=None, **kwargs: Any):
        if "dm_db_column_store_row_group_physical_stats" in query:
            self.health_reads += 1
            if isinstance(self.health, Exception):
                raise self.health
            return self.health
        return await super().fetch_all(database_name, query, params, **kwargs)


def _patch_existing(monkeypatch: pytest.MonkeyPatch, indexes: list[ExistingIndex]) -> None:
    async def fake_collect(executor, database_name, **kwargs):
        return indexes

    monkeypatch.setattr(workload_module, "collect_existing_indexes", fake_collect)


_FACT_TABLE_ROW = {
    "object_id": 9, "schema_name": "dbo", "table_name": "FactSales", "row_count": FACT_ROWS,
    "base_used_pages": 300_000, "all_used_pages": 400_000, "is_heap": 0, "is_memory_optimized": 0,
}


@pytest.mark.asyncio
async def test_review_reads_row_group_health_and_reports_the_maintenance_lead(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_existing(monkeypatch, [_columnstore("CCI_FactSales", clustered=True, reads=9)])
    executor = _HealthExecutor(
        [_row_groups("COMPRESSED", 48, 48_000_000, deleted=14_400_000)], workload=[], tables=[_FACT_TABLE_ROW]
    )

    report = await WorkloadIndexAdvisor(executor).review("appdb", now=WORKLOAD_NOW)

    assert executor.health_reads == 1
    lead = _maintenance(report)[0]
    assert lead["ddl"] == "ALTER INDEX [CCI_FactSales] ON [dbo].[FactSales] REORGANIZE;"
    assert not any("row-group health" in gap for gap in report["gaps"])


@pytest.mark.asyncio
async def test_review_reports_a_gap_when_row_group_health_cannot_be_read(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_existing(monkeypatch, [_columnstore("CCI_FactSales", clustered=True, reads=9)])
    executor = _HealthExecutor(RuntimeError("VIEW DATABASE PERFORMANCE STATE permission was denied"), workload=[], tables=[_FACT_TABLE_ROW])

    report = await WorkloadIndexAdvisor(executor).review("appdb", now=WORKLOAD_NOW)

    assert _maintenance(report) == []
    assert any("row-group health" in gap for gap in report["gaps"])


@pytest.mark.asyncio
async def test_review_does_not_read_row_group_health_without_a_columnstore_index(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_existing(monkeypatch, [_rowstore_pk()])
    executor = _HealthExecutor([], workload=[], tables=[_FACT_TABLE_ROW])

    await WorkloadIndexAdvisor(executor).review("appdb", now=WORKLOAD_NOW)

    assert executor.health_reads == 0
