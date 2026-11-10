"""Index findings of the 2026-11-03 review that were still open in the code.

31 filter literals, 32 offline rollback, 33 schema compare fidelity,
55 hints that name no index, 84 leftover test indexes, and the
impact_score that counted executions twice.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp import workload_index_advisor as workload_module
from azure_sql_mcp.ddl_generator import generate_migration_script
from azure_sql_mcp.index_advisor import build_index_advice
from azure_sql_mcp.index_ddl import render_candidate_rollback
from azure_sql_mcp.index_ddl import render_reverse_index_ddl
from azure_sql_mcp.index_metadata import existing_index_covers_candidate
from azure_sql_mcp.index_metadata import is_test_index_name
from azure_sql_mcp.index_review import IndexReviewService
from azure_sql_mcp.query_index_analysis import QueryIndexAnalysisService
from azure_sql_mcp.safe_sql import SafeSqlValidator
from azure_sql_mcp.schema_diff import DiffCategory
from azure_sql_mcp.schema_diff import compare_snapshots
from azure_sql_mcp.schema_snapshot import IndexDef
from azure_sql_mcp.schema_snapshot import SchemaSnapshot
from azure_sql_mcp.schema_snapshot import TableSnapshot
from azure_sql_mcp.schema_snapshot import _build_tables
from azure_sql_mcp.workload_index_advisor import WorkloadIndexAdvisor
from tests.unit.test_index_advisor import _index
from tests.unit.test_index_advisor import _inputs
from tests.unit.test_index_advisor import _orders_indexes
from tests.unit.test_index_advisor import _query
from tests.unit.test_index_review import _index as _review_subject
from tests.unit.test_workload_index_advisor import LONG
from tests.unit.test_workload_index_advisor import NO_REFERENCES
from tests.unit.test_workload_index_advisor import RoutingExecutor
from tests.unit.test_workload_index_advisor import _unused_drop


def _recs(report: dict[str, Any], action: str) -> list[dict[str, Any]]:
    return [rec for rec in report["recommendations"] if rec["action"] == action]


# --- 31: filter literals -----------------------------------------------------


def test_filtered_indexes_that_differ_only_in_literal_case_are_not_duplicates() -> None:
    """Under a case-sensitive collation Code = 'a' and Code = 'A' select different rows."""

    lower = _index("Sales", "Orders", "IX_Orders_a", ("OrderDate",), index_id=5, filter_definition="([Comments]='a')")
    upper = _index("Sales", "Orders", "IX_Orders_A", ("OrderDate",), index_id=6, filter_definition="([Comments]='A')")
    report = build_index_advice(_inputs([], [*_orders_indexes(), lower, upper]))
    assert _recs(report, "consolidate_index") == []

    same = _index("Sales", "Orders", "IX_Orders_a2", ("OrderDate",), index_id=7, filter_definition="([Comments]='a')")
    report = build_index_advice(_inputs([], [*_orders_indexes(), lower, same]))
    assert len(_recs(report, "consolidate_index")) == 1


# --- 32: rollback builds online ---------------------------------------------


def test_rollback_of_a_drop_or_consolidation_recreates_the_index_online() -> None:
    """A rollback run in business hours must not block every writer of the table."""

    unused = _index("Sales", "Orders", "IX_Orders_Status", ("Status",), index_id=5, reads=0)
    drop = _recs(
        build_index_advice(
            _inputs([], [*_orders_indexes(), unused], references={("sales", "orders", "ix_orders_status"): 0}, window_days=40)
        ),
        "drop_index",
    )[0]
    assert drop["rollback_ddl"].count("ONLINE = ON") == 1
    assert "ONLINE" not in drop["ddl"]

    twins = [
        _index("Sales", "Orders", "IX_A", ("OrderDate",), index_id=5, reads=5),
        _index("Sales", "Orders", "IX_B", ("OrderDate",), index_id=6, reads=50),
    ]
    consolidate = _recs(build_index_advice(_inputs([], [*_orders_indexes(), *twins])), "consolidate_index")[0]
    assert consolidate["rollback_ddl"].count("ONLINE = ON") == 1


def test_rebuild_statements_still_list_online_exactly_once() -> None:
    report = build_index_advice(
        _inputs([_query(1, "seek_residual_lookup_sort.xml", cpu_us=9_000_000)], _orders_indexes())
    )
    widen = _recs(report, "widen_index")[0]
    assert widen["ddl"].count("ONLINE = ON") == 1
    assert widen["rollback_ddl"].count("ONLINE = ON") == 1


def test_reverse_ddl_is_offline_unless_asked_so_sandbox_ddl_keeps_its_form() -> None:
    index = _index("Sales", "Orders", "IX_A", ("OrderDate",), index_id=5)
    assert "ONLINE" not in render_reverse_index_ddl(index)["ddl"]
    assert "WITH (ONLINE = ON, PAD_INDEX = OFF" in render_reverse_index_ddl(index, online=True)["ddl"]


def test_portfolio_rollback_and_create_candidate_build_online() -> None:
    subject = {**_review_subject(), "state": "drop_candidate"}
    rollback = render_candidate_rollback(subject)
    assert rollback is not None and rollback.count("ONLINE = ON") == 1

    from azure_sql_mcp.index_review import _create_candidate_ddl

    create = _create_candidate_ddl(
        {"schema_name": "dbo", "table_name": "Orders", "key_columns": ["CustomerId"], "include_columns": ["Status"], "subject_id": "missing:1"}
    )
    assert create is not None and create.endswith("INCLUDE ([Status]) WITH (ONLINE = ON);")


# --- 33: schema compare keeps direction and renders valid columnstore DDL ----


def _index_row(index: str, index_type: str, column: str, *, ordinal: int, descending: bool = False, included: bool = False) -> dict[str, Any]:
    return {
        "schema_name": "dbo",
        "table_name": "T",
        "index_name": index,
        "index_type": index_type,
        "is_unique": 0,
        "is_primary_key": 0,
        "key_ordinal": 0 if included else ordinal,
        "is_included_column": 1 if included else 0,
        "is_descending_key": 1 if descending else 0,
        "index_column_ordinal": ordinal,
        "column_name": column,
        "filter_definition": None,
        "data_compression": "NONE",
    }


def test_snapshot_keeps_key_sort_direction() -> None:
    rows = [
        _index_row("IX_T_Date_Desc", "NONCLUSTERED", "TenantId", ordinal=1),
        _index_row("IX_T_Date_Desc", "NONCLUSTERED", "CreatedAt", ordinal=2, descending=True),
    ]
    (index,) = _build_tables([], rows, [])[("dbo", "T")].indexes
    assert index.key_columns == ("TenantId", "CreatedAt")
    assert index.key_directions == ("ASC", "DESC")


def _snapshot(*indexes: IndexDef) -> SchemaSnapshot:
    return SchemaSnapshot(
        database_name="testdb",
        captured_at="2026-11-01T00:00:00Z",
        tables={("dbo", "T"): TableSnapshot("dbo", "T", (), tuple(indexes), ())},
        views={},
        procedures={},
        functions={},
        sequences=None,
    )


def test_ascending_versus_descending_key_is_reported_and_scripted() -> None:
    """ORDER BY CreatedAt DESC with TOP regains a sort when the migrated index is ascending."""

    ascending = IndexDef("IX_T_Date", "NONCLUSTERED", False, False, ("CreatedAt",), ())
    descending = IndexDef("IX_T_Date", "NONCLUSTERED", False, False, ("CreatedAt",), (), key_directions=("DESC",))

    diffs = compare_snapshots(_snapshot(ascending), _snapshot(descending))
    assert len(diffs) == 1 and diffs[0].category is DiffCategory.INDEX
    assert "key_directions" in diffs[0].detail

    script = generate_migration_script(diffs, _snapshot(ascending), _snapshot(descending))
    assert "CREATE NONCLUSTERED INDEX [IX_T_Date] ON [dbo].[T] ([CreatedAt] DESC);" in script

    # A snapshot without directions (older capture) equals an all-ascending one.
    explicit = IndexDef("IX_T_Date", "NONCLUSTERED", False, False, ("CreatedAt",), (), key_directions=("ASC",))
    assert compare_snapshots(_snapshot(ascending), _snapshot(explicit)) == []


def test_columnstore_indexes_are_scripted_with_valid_syntax() -> None:
    clustered = IndexDef("CCI_T", "CLUSTERED COLUMNSTORE", False, False, (), ("a", "b"))
    nonclustered = IndexDef("NCCI_T", "NONCLUSTERED COLUMNSTORE", False, False, (), ("a", "b"))
    target = _snapshot(clustered, nonclustered)
    script = generate_migration_script(compare_snapshots(_snapshot(), target), _snapshot(), target)

    assert "CREATE CLUSTERED COLUMNSTORE INDEX [CCI_T] ON [dbo].[T];" in script
    assert "CREATE NONCLUSTERED COLUMNSTORE INDEX [NCCI_T] ON [dbo].[T] ([a], [b]);" in script
    assert "INCLUDE" not in script


def test_index_types_the_generator_cannot_script_become_a_comment_with_the_reason() -> None:
    target = _snapshot(IndexDef("XI_T_Doc", "XML", False, False, ("Doc",), ()))
    script = generate_migration_script(compare_snapshots(_snapshot(), target), _snapshot(), target)

    assert "CREATE XML INDEX" not in script.replace("-- ", "")
    assert "-- Not scripted: [XI_T_Doc] on [dbo].[T] is a XML index" in script


# --- 55: a hint that names no index here does not block every removal --------


class _HintSession:
    def __init__(self, texts: dict[str, list[str]]) -> None:
        self.texts = texts

    def fetch_all(self, query: str, params: Any = None) -> list[dict[str, Any]]:
        if "retained_query_text" in query:
            return [{"query_id": n, "retained_query_text": text} for n, text in enumerate(self.texts.get("query", []), 1)]
        if "module_definition" in query:
            return [{"object_id": n, "module_definition": text} for n, text in enumerate(self.texts.get("module", []), 1)]
        return []


def _hint_coverage(texts: dict[str, list[str]]) -> dict[str, Any]:
    index = _index("Sales", "Orders", "IX_Orders_Status", ("Status",), index_id=5)
    coverage, _evidence = IndexReviewService._collect_hints(
        _HintSession(texts), [index], observation_window_minutes=60  # type: ignore[arg-type]
    )
    return coverage


def test_temp_table_index_hint_leaves_hint_coverage_complete() -> None:
    """An ETL procedure that hints #work must not stop every index decision in the database."""

    coverage = _hint_coverage({"module": ["SELECT a FROM #work WITH (INDEX(ix_work_a)) WHERE a = 1"]})

    assert coverage["status"] == "complete"
    assert coverage["blockers"] == []
    assert coverage["sources"]["module_definitions"]["reason"] == "index_hint_names_no_index"


def test_hint_that_cannot_be_parsed_still_makes_hint_coverage_incomplete() -> None:
    coverage = _hint_coverage({"module": ["SELECT a FROM Sales.Orders WITH (FORCESEEK) WHERE a = 1"]})
    assert coverage["status"] == "incomplete"
    assert "unresolved_forceseek_index_hint" in coverage["blockers"]


# --- 84: leftover test indexes are not ordinary indexes ----------------------


def test_leftover_test_index_never_covers_a_candidate() -> None:
    """An orphan from an earlier experiment made the next candidate 'already covered'."""

    orphan = _index("Sales", "Orders", "IX_Testing_0123456789abcdef", ("OrderDate",), index_id=9, includes=("Status",))
    real = _index("Sales", "Orders", "IX_Orders_OrderDate", ("OrderDate",), index_id=10, includes=("Status",))
    candidate = {"schema": "Sales", "table": "Orders", "key_columns": ["OrderDate"], "include_columns": ["Status"]}

    assert is_test_index_name("ix_testing_abc") and is_test_index_name("IX_Tuning_w1_abc")
    assert not is_test_index_name("IX_Orders_OrderDate")
    assert existing_index_covers_candidate(real, **candidate) is True
    assert existing_index_covers_candidate(orphan, **candidate) is False


def test_advisor_reports_a_leftover_test_index_and_keeps_it_out_of_the_design() -> None:
    """The orphan must not absorb a real index, and a real index must not be advised away into it."""

    orphan = _index("Sales", "Orders", "IX_Testing_0123456789abcdef", ("CustomerID", "OrderDate"), index_id=9, reads=0)
    report = build_index_advice(_inputs([], [*_orders_indexes(), orphan]))

    assert _recs(report, "consolidate_index") == [] and _recs(report, "drop_index") == []
    leftover = [rec for rec in report["recommendations"] if "leftover_test_index" in rec["reason_codes"]]
    assert [rec["index_name"] for rec in leftover] == ["IX_Testing_0123456789abcdef"]
    assert "drop_test_index" in leftover[0]["rationale"]


# --- impact_score counted executions twice -----------------------------------


def test_impact_score_ranks_by_cpu_saved_not_by_cpu_times_executions() -> None:
    """Total CPU already includes every execution; multiplying again buries the costly rare query."""

    service = QueryIndexAnalysisService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]

    def rec(table: str, cpu_us: float, executions: float) -> dict[str, Any]:
        return {
            "schema": "dbo",
            "table": table,
            "equality_columns": ["a"],
            "inequality_columns": [],
            "include_columns": [],
            "impact_pct": 50.0,
            "query_total_cpu_us": cpu_us,
            "query_executions": executions,
            "source_query": table,
        }

    ranked = service._consolidate_recommendations(
        [rec("Cheap", 10_000_000, 1_000_000), rec("Costly", 100_000_000, 10)]
    )

    assert [item["table"] for item in ranked] == ["Costly", "Cheap"]
    assert ranked[0]["impact_score"] == 50.0  # seconds of CPU in the window
    assert ranked[1]["impact_score"] == 5.0


# --- 55 in the workload review: the same hint does not cap every removal ------


@pytest.fixture
def unused_index(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sales.Orders with one index that has had no reads for 61 days."""

    indexes = _orders_indexes() + [
        _index("Sales", "OrderLines", "PK_Sales_OrderLines", ("OrderLineID",), index_id=1, type_code=1, primary_key=True),
        _index("Sales", "Orders", "IX_Odd]Name", ("OrderDate",), index_id=7, reads=0, updates=900),
    ]
    for index in indexes:
        index.usage_context["engine_start_time_utc"] = "2026-08-01T00:00:00Z"

    async def fake_collect(executor, database_name, **kwargs):
        return indexes

    monkeypatch.setattr(workload_module, "collect_existing_indexes", fake_collect)


@pytest.mark.asyncio
async def test_temp_table_hint_in_a_module_does_not_cap_unrelated_index_removals(unused_index: None) -> None:
    """One #temp index hint in an ETL procedure used to cap every removal in the database at medium."""

    executor = RoutingExecutor(
        references=NO_REFERENCES,
        hint_modules=[
            {
                "object_id": 77,
                "module_definition": "CREATE PROCEDURE dbo.etl AS SELECT a FROM #work WITH (INDEX(ix_work_a));",
            }
        ],
    )

    report = await WorkloadIndexAdvisor(executor).review("appdb", **LONG)

    rec = _unused_drop(report)
    assert "hint_reference_check_incomplete" not in rec["blockers"]
    assert rec["confidence"] == "high" and rec["ddl"] is not None
    assert any("name no index in this database" in gap for gap in report["gaps"])


    # A plain FORCESEEK names no index and its table is not resolved, so it still limits removals.
    forceseek = RoutingExecutor(
        references=NO_REFERENCES,
        hint_modules=[
            {"object_id": 78, "module_definition": "CREATE PROCEDURE dbo.p AS SELECT 1 FROM Sales.Orders WITH (FORCESEEK);"}
        ],
    )
    capped = _unused_drop(await WorkloadIndexAdvisor(forceseek).review("appdb", **LONG))
    assert "hint_reference_check_incomplete" in capped["blockers"]
    assert capped["confidence"] == "medium"
