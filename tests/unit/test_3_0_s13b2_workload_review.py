"""Workload index review (friction F23, B05; proposal P9).

O22: the recommendation ignored ORDER BY Reputation DESC, Id and was merged
with other queries, in a 34 KB report. D04: the review proposed a non-aligned
ON [PRIMARY] index on a clustered columnstore table, which the engine refuses.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from datetime import timezone
from typing import Any

import pytest

from azure_sql_mcp.index_advisor import AdvisorSettings
from azure_sql_mcp.index_advisor import _Access
from azure_sql_mcp.index_advisor import _access_candidate
from azure_sql_mcp.index_advisor import build_index_advice
from azure_sql_mcp.server import _lead_with_requested_queries
from azure_sql_mcp.showplan_access import TableAccess
from tests.unit.test_3_0_s8_partitioned import _clustered
from tests.unit.test_index_advisor import _inputs
from tests.unit.test_index_advisor import _query


def _access(**fields: Any) -> TableAccess:
    values: dict[str, Any] = {
        "node_id": 1, "physical_op": "Clustered Index Scan", "logical_op": "Clustered Index Scan",
        "schema": "dbo", "table": "Users", "alias": "u", "index_name": "PK_Users",
        "index_kind": "Clustered", "storage": "RowStore", "operation": "scan",
        "output_columns": ("Id", "DisplayName", "Reputation", "CreationDate"),
        "estimated_rows": 20.0,
    }
    values.update(fields)
    return TableAccess(**values)


def _keys(access: TableAccess) -> list[tuple[str, str]]:
    query = _query(7, "hash_join_stream_aggregate.xml", cpu_us=1_000_000)
    candidate = _access_candidate(
        _Access(query=query, access=access, attributed=1.0, statement_type="SELECT"),
        {}, ("Id",), {}, _inputs([query], []), 2_300_000.0, AdvisorSettings(),
    )
    assert candidate is not None
    return candidate.keys


def test_order_by_columns_follow_the_equality_column_in_the_query_direction() -> None:
    """O22: WHERE DisplayName = @n ORDER BY Reputation DESC, Id."""

    keys = _keys(_access(
        residual={"DisplayName": "eq"},
        order_columns=(("Reputation", "DESC"), ("Id", "ASC")),
    ))

    assert keys == [("DisplayName", "ASC"), ("Reputation", "DESC"), ("Id", "ASC")]


def test_the_range_column_comes_after_the_order_by_columns() -> None:
    """Equality, then ORDER BY, then one range column: the index returns the rows in order."""

    keys = _keys(_access(
        residual={"DisplayName": "eq", "CreationDate": "range"},
        order_columns=(("Reputation", "DESC"),),
    ))

    assert keys == [("DisplayName", "ASC"), ("Reputation", "DESC"), ("CreationDate", "ASC")]


def test_a_range_column_that_is_also_the_order_column_appears_once_in_its_direction() -> None:
    keys = _keys(_access(
        residual={"DisplayName": "eq", "CreationDate": "range"},
        order_columns=(("CreationDate", "DESC"),),
    ))

    assert keys == [("DisplayName", "ASC"), ("CreationDate", "DESC")]


def test_with_no_order_by_the_rule_is_equality_then_one_range_column() -> None:
    keys = _keys(_access(residual={"DisplayName": "eq", "CreationDate": "range", "Reputation": "range"}))

    assert keys[0] == ("DisplayName", "ASC")
    assert len(keys) == 2 and keys[1][0] in {"CreationDate", "Reputation"}


# --- the design for the query the caller named ---------------------------------------


def test_the_design_for_the_named_query_is_returned_alone_and_unmerged() -> None:
    named = _query(1, "hash_join_stream_aggregate.xml", cpu_us=9_000_000)
    other = _query(2, "clustered_scan_residuals.xml", cpu_us=2_000_000)
    settings = AdvisorSettings(requested_query_ids=frozenset({1}))

    report = build_index_advice(_inputs([named, other], [], settings=settings))

    designs = report["requested_query_design"]
    assert designs and {design["query_id"] for design in designs} == {1}
    design = designs[0]
    assert design["basis"] == "this_query_alone"
    assert design["key_columns"] and design["ddl"].startswith("CREATE NONCLUSTERED INDEX")
    assert design["rule"] == "equality columns, then ORDER BY columns, then one range column"
    assert "requested_query_design" not in build_index_advice(_inputs([named, other], []))


def _report(count_for: int, count_other: int) -> dict[str, Any]:
    recommendations = [
        {"id": f"R{n}", "action": "create_index", "supporting_queries": [{"query_id": 9}]}
        for n in range(count_other)
    ] + [
        {"id": f"N{n}", "action": "create_index", "supporting_queries": [{"query_id": 42}]}
        for n in range(count_for)
    ]
    return {
        "result_status": "ok",
        "tables": [{"table": "Posts", "recommendation_ids": [item["id"] for item in recommendations]}],
        "recommendations": recommendations,
        "requested_query_design": [{"query_id": 42, "key_columns": ["OwnerUserId"]}],
    }


def test_the_named_query_leads_the_report_and_the_other_recommendations_are_counted() -> None:
    shaped = _lead_with_requested_queries(_report(1, 6), [42])

    assert list(shaped)[:2] == ["requested_queries", "requested_query_design"]
    assert [item["id"] for item in shaped["recommendations"]] == ["N0"]
    assert shaped["requested_queries"]["recommendations_for_them"] == 1
    assert shaped["requested_queries"]["other_recommendations_omitted"] == 6
    assert "include_other_recommendations" in shaped["requested_queries"]["message"]


def test_the_other_recommendations_come_back_on_request_after_the_named_ones() -> None:
    shaped = _lead_with_requested_queries(_report(1, 6), [42], include_others=True)

    assert [item["id"] for item in shaped["recommendations"]][0] == "N0"
    assert len(shaped["recommendations"]) == 7
    assert "other_recommendations_omitted" not in shaped["requested_queries"]


def test_no_recommendation_for_the_named_query_is_said_plainly_without_the_others() -> None:
    shaped = _lead_with_requested_queries(_report(0, 6), [42])

    assert shaped["recommendations"] == []
    assert shaped["requested_queries"]["recommendations_for_them"] == 0
    assert "No recommendation serves query id 42" in shaped["requested_queries"]["message"]
    assert shaped["requested_queries"]["other_recommendations_omitted"] == 6


# --- partitioned and columnstore tables -------------------------------------------------


def _create(report: dict[str, Any]) -> dict[str, Any]:
    [rec] = [rec for rec in report["recommendations"] if rec["action"] == "create_index"]
    return rec


def test_a_partitioned_clustered_columnstore_table_gets_no_non_aligned_proposal() -> None:
    """D04: the engine refuses a nonclustered index that is not partition-aligned there."""

    query = _query(1, "hash_join_stream_aggregate.xml", cpu_us=9_000_000)
    columnstore = replace(
        _clustered(column="ExpectedDeliveryDate"),
        index_type="CLUSTERED COLUMNSTORE", index_type_code=5, key_columns=(), is_primary_key=False,
    )

    rec = _create(build_index_advice(_inputs([query], [columnstore])))

    part = rec["partitioning"]
    assert part["alignment"] == "aligned"
    assert part["non_aligned_alternative"] is None
    assert "must be partition-aligned" in part["note"]
    assert "ON [PRIMARY]" not in str(rec)


def test_a_partitioned_rowstore_table_keeps_the_non_aligned_trade() -> None:
    query = _query(1, "hash_join_stream_aggregate.xml", cpu_us=9_000_000)

    rec = _create(build_index_advice(_inputs([query], [_clustered(column="ExpectedDeliveryDate")])))

    assert rec["partitioning"]["non_aligned_alternative"]["ddl"].endswith("ON [PRIMARY];")


# --- the tool's own benchmark statements are not workload -----------------------------


@pytest.mark.asyncio
async def test_benchmark_marked_statements_are_left_out_of_the_workload_unless_named() -> None:
    """O22: the review merged the query with one-execution probes of earlier test runs."""

    from azure_sql_mcp.performance_workflows import BENCHMARK_MARKER_PREFIX
    from azure_sql_mcp.workload_index_advisor import WorkloadIndexAdvisor
    from tests.unit.test_workload_index_advisor import _workload_row

    application = _workload_row(1, "hash_join_stream_aggregate.xml", 9_000_000)
    probe = {
        **_workload_row(2, "clustered_scan_residuals.xml", 2_000_000),
        "query_text_preview": f"{BENCHMARK_MARKER_PREFIX}0a1b2c3d4e5f6071 */ SELECT ...",
    }

    class Executor:
        async def fetch_all(self, _database: str, _sql: str, **_kwargs: Any) -> list[dict[str, Any]]:
            return [application, probe]

    advisor = WorkloadIndexAdvisor(Executor())  # type: ignore[arg-type]
    start, end = datetime(2026, 11, 1, tzinfo=timezone.utc), datetime(2026, 11, 8, tzinfo=timezone.utc)

    queries, _totals = await advisor._workload("appdb", "cpu", start, end, 100, 1)
    named, _totals = await advisor._workload("appdb", "cpu", start, end, 100, 1, query_ids=[2])

    assert [query.query_id for query in queries] == [1]
    assert [query.query_id for query in named] == [1, 2]
