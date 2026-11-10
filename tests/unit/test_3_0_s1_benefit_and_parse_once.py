"""Benefit per finding, ranking across statements, and one parse per tool call
(review findings 91 and 95)."""

from __future__ import annotations

import asyncio
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from azure_sql_mcp.plan_digest import build_plan_digest
from azure_sql_mcp.plan_digest import describe_plan_node
from azure_sql_mcp.plan_rules import analyze_plan
from azure_sql_mcp.plan_rules import read_plan
from azure_sql_mcp.plan_sources import _statement_estimated_rows
from azure_sql_mcp.plan_tree import parse_plan
from azure_sql_mcp.showplan_access import parse_plan_access

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "showplans"


def _rules(plan_xml: str, **options) -> dict[str, dict]:
    return {finding["rule"]: finding for finding in analyze_plan(plan_xml, **options)["findings"]}


def _lookup_plan(showplan, *, timed: bool = True) -> str:
    """A seek that feeds 200,000 key lookups: lookup self 6,000 ms, its loop 2,500 ms, of 10,000 ms."""

    def rt(rows: int, executions: int, elapsed: int) -> str:
        return showplan.runtime((0, rows, executions, elapsed if timed else None, elapsed if timed else None))

    seek = showplan.relop(
        2, "Index Seek", wrapper="IndexScan", est_rows=200_000, cost=0.1,
        body='<Object Database="[db]" Schema="[dbo]" Table="[Orders]" Index="[IX_Orders_CustomerID]" IndexKind="NonClustered" />'
        + "<SeekPredicates>" + showplan.seek_keys("Orders", "CustomerID", "@cust") + "</SeekPredicates>",
        outputs=showplan.column("Orders", "OrderID"), runtime=rt(200_000, 1, 500),
    )
    lookup = showplan.relop(
        3, "Key Lookup", wrapper="IndexScan", est_rows=1, cost=0.6, attrs='EstimateRebinds="199999" EstimateRewinds="0"',
        body='<Object Database="[db]" Schema="[dbo]" Table="[Orders]" Index="[PK_Orders]" IndexKind="Clustered" />',
        outputs=showplan.column("Orders", "Comments"), runtime=rt(200_000, 200_000, 6000),
    )
    loop = showplan.relop(1, "Nested Loops", "Inner Join", seek, lookup, wrapper="NestedLoops", cost=0.8, est_rows=200_000, runtime=rt(200_000, 1, 9000))
    spool = showplan.relop(0, "Index Spool", "Eager Spool", loop, wrapper="Spool", cost=1.0, est_rows=200_000, runtime=rt(200_000, 1, 9200))
    children = '<QueryTimeStats ElapsedTime="10000" CpuTime="9900" />' if timed else ""
    return showplan.plan(spool, plan_children=children)


# --- benefit ------------------------------------------------------------------------------


def test_lookup_benefit_is_the_lookup_plus_its_driving_loop(showplan) -> None:
    # Covering the lookup removes its 6,000 ms and the loop's 2,500 ms: 85% of the statement.
    finding = _rules(_lookup_plan(showplan))["key_lookup"]

    assert finding["self_elapsed_ms"] == 6000
    assert finding["elapsed_share"] == 0.6
    assert finding["benefit_ms"] == 8500
    assert finding["benefit_share"] == 0.85
    assert finding["benefit_basis"] == "self time of the lookup (node 3) and its driving Nested Loops (node 1)"
    assert finding["plan_share"] == 0.85


def test_operator_finding_benefit_defaults_to_its_self_time(showplan) -> None:
    finding = _rules(_lookup_plan(showplan))["eager_index_spool"]

    assert finding["benefit_ms"] == 200
    assert finding["benefit_share"] == 0.02
    assert finding["benefit_basis"] == "self time of node 0"


def test_untimed_plan_states_no_benefit(showplan) -> None:
    finding = _rules(_lookup_plan(showplan, timed=False))["key_lookup"]

    assert finding["benefit_ms"] is None
    assert finding["benefit_share"] is None
    assert finding["benefit_basis"] is None
    assert finding["plan_share"] == finding["estimated_cost_share"]


def test_statement_level_benefit_states_its_own_basis(showplan) -> None:
    plan = showplan.plan(
        showplan.scan(0, runtime=showplan.runtime((0, 10, 1, 1000, 990))),
        plan_children=(
            '<MemoryGrantInfo GrantedMemory="4096" MaxUsedMemory="4000" GrantWaitTime="4" />'
            '<QueryTimeStats ElapsedTime="10000" CpuTime="990" UdfElapsedTime="3000" UdfCpuTime="2900" />'
        ),
    )

    rules = _rules(plan)

    assert rules["scalar_udf_time"]["benefit_share"] == 0.3
    assert rules["scalar_udf_time"]["benefit_basis"] == "scalar UDF elapsed time (QueryTimeStats)"
    assert rules["memory_grant_wait"]["benefit_share"] == 0.4
    assert rules["memory_grant_wait"]["benefit_basis"] == "time spent waiting for the memory grant"


def test_filter_benefit_is_the_subtree_it_discards(showplan) -> None:
    rt = showplan.runtime
    join = showplan.relop(
        2, "Hash Match", "Inner Join",
        showplan.scan(3, "Orders", runtime=rt((0, 2_000_000, 1, 3000, 3000))),
        showplan.scan(4, "Customers", runtime=rt((0, 50_000, 1, 500, 500))),
        wrapper="Hash", runtime=rt((0, 2_000_000, 1, 8000, 8000)),
    )
    predicate = (
        '<Predicate><ScalarOperator ScalarString="o.Total&gt;c.Limit"><Compare CompareOp="GT"><ScalarOperator><Identifier>'
        f'{showplan.column("Orders", "Total")}</Identifier></ScalarOperator><ScalarOperator><Identifier>'
        f'{showplan.column("Customers", "Limit")}</Identifier></ScalarOperator></Compare></ScalarOperator></Predicate>'
    )
    plan = showplan.plan(
        showplan.relop(1, "Filter", None, join, body=predicate, runtime=rt((0, 900, 1, 8400, 8400))),
        plan_children='<QueryTimeStats ElapsedTime="10000" CpuTime="9900" />',
    )

    finding = _rules(plan)["late_filter"]

    assert finding["benefit_ms"] == 8000  # the join and both scans below the Filter
    assert finding["benefit_share"] == 0.8
    assert "subtree under the Filter" in finding["benefit_basis"]


# --- ranking -----------------------------------------------------------------------------------


def test_measured_hotspot_ranks_above_a_static_high_finding(showplan) -> None:
    # The eager spool is a fixed "high" at 2% of the time. The nested loop is a fixed "medium",
    # but half of the elapsed time is its own: it is where the answer starts.
    rt = showplan.runtime
    inner = showplan.scan(3, "Posts", physical="Index Seek", est_rows=1, cost=0.3, runtime=rt((0, 20_000, 20_000, 4000, 4000)))
    outer = showplan.scan(2, "Users", est_rows=20_000, cost=0.2, runtime=rt((0, 20_000, 1, 800, 800)))
    loop = showplan.relop(1, "Nested Loops", "Inner Join", outer, inner, wrapper="NestedLoops", cost=0.9, est_rows=20_000, runtime=rt((0, 20_000, 1, 9800, 9800)))
    spool = showplan.relop(0, "Index Spool", "Eager Spool", loop, wrapper="Spool", cost=1.0, est_rows=20_000, runtime=rt((0, 20_000, 1, 10_000, 10_000)))
    plan = showplan.plan(spool, plan_children='<QueryTimeStats ElapsedTime="10000" CpuTime="9900" />')

    findings = analyze_plan(plan)["findings"]
    by_rule = {finding["rule"]: finding for finding in findings}

    assert by_rule["nested_loops_high_inner_executions"]["severity"] == "medium"
    assert by_rule["nested_loops_high_inner_executions"]["benefit_share"] == 0.5
    assert by_rule["eager_index_spool"]["severity"] == "high"
    assert by_rule["eager_index_spool"]["benefit_share"] == 0.02
    assert [finding["rule"] for finding in findings][:2] == ["nested_loops_high_inner_executions", "eager_index_spool"]


def test_information_about_a_hot_operator_does_not_lead_the_list(showplan) -> None:
    # A benign NoJoinPredicate note on the slowest join is still only information.
    rt = showplan.runtime
    plan = showplan.plan(
        showplan.relop(
            0, "Nested Loops", "Inner Join",
            showplan.scan(1, "A", runtime=rt((0, 1000, 1, 5, 5))),
            showplan.scan(2, "B", est_rows=1, runtime=rt((0, 500_000, 1000, 50, 50))),
            body=f"<OuterReferences>{showplan.column('A', 'Id')}</OuterReferences>",
            warnings='<Warnings NoJoinPredicate="true" />', runtime=rt((0, 500_000, 1, 1000, 1000)),
        )
    )

    findings = analyze_plan(plan)["findings"]

    assert findings[0]["severity"] != "info"
    assert findings[-1]["rule"] == "no_join_predicate" and findings[-1]["severity"] == "info"


def _seekable_scan(showplan, table: str, *, cost: float, cardinality: int) -> str:
    predicate = (
        '<Predicate><ScalarOperator><Compare CompareOp="EQ"><ScalarOperator><Identifier>'
        f'{showplan.column(table, "Code")}</Identifier></ScalarOperator><ScalarOperator><Identifier>'
        '<ColumnReference Column="@code" /></Identifier></ScalarOperator></Compare></ScalarOperator></Predicate>'
    )
    scan = showplan.scan(0, table, cost=cost, attrs=f'TableCardinality="{cardinality}"', body=predicate)
    return f'<StmtSimple StatementText="SELECT 1" StatementType="SELECT" StatementSubTreeCost="{cost}"><QueryPlan>{scan}</QueryPlan></StmtSimple>'


def test_trivial_statement_does_not_outrank_the_expensive_one(showplan) -> None:
    # A 20-row settings table and a 50M-row table, both scanned with a seekable filter.
    plan = showplan.batch(
        _seekable_scan(showplan, "Config", cost=0.0033, cardinality=20),
        _seekable_scan(showplan, "Orders", cost=2500.0, cardinality=50_000_000),
    )

    result = analyze_plan(plan)
    scans = [finding for finding in result["findings"] if finding["rule"] == "scan_with_seekable_predicate"]

    assert [finding["evidence"]["table"] for finding in scans] == ["dbo.Orders"]
    assert scans[0]["statement_index"] == 1 and scans[0]["severity"] == "high"
    assert [statement["weight"] for statement in result["statements"]] == [0.0, 1.0]
    assert scans[0]["statement_weight"] == 1.0


def test_small_share_of_a_batch_is_graded_by_its_share_of_the_whole_plan(showplan) -> None:
    # Two large tables; the second statement is 1% of the batch cost.
    plan = showplan.batch(
        _seekable_scan(showplan, "Orders", cost=990.0, cardinality=50_000_000),
        _seekable_scan(showplan, "Invoices", cost=10.0, cardinality=2_000_000),
    )

    by_table = {
        finding["evidence"]["table"]: finding
        for finding in analyze_plan(plan)["findings"]
        if finding["rule"] == "scan_with_seekable_predicate"
    }

    assert by_table["dbo.Orders"]["severity"] == "high"
    assert "dbo.Invoices" not in by_table  # 1% of the plan: under the rule's floor


def test_statement_indexes_limit_the_analysis_and_keep_the_indexes(showplan) -> None:
    # The requested statement of a cached batch is judged by itself, not against its siblings.
    plan = showplan.batch(
        _seekable_scan(showplan, "Orders", cost=990.0, cardinality=50_000_000),
        _seekable_scan(showplan, "Invoices", cost=10.0, cardinality=2_000_000),
    )

    result = analyze_plan(plan, statement_indexes=[1])

    assert [statement["statement_index"] for statement in result["statements"]] == [1]
    assert {finding["statement_index"] for finding in result["findings"]} == {1}
    assert {finding["rule"]: finding for finding in result["findings"]}["scan_with_seekable_predicate"]["severity"] == "high"


# --- parse once ---------------------------------------------------------------------------------


@pytest.fixture
def parse_count(monkeypatch) -> list[int]:
    calls: list[int] = []
    original = ET.fromstring

    def counting(text, *args, **kwargs):
        calls.append(len(text))
        return original(text, *args, **kwargs)

    monkeypatch.setattr(ET, "fromstring", counting)
    return calls


def test_read_plan_parses_the_xml_once_for_rules_digest_and_drill_down(parse_count: list[int]) -> None:
    plan = (FIXTURES / "seek_residual_lookup_sort.xml").read_text(encoding="utf-8")

    reading = read_plan(plan)
    node = describe_plan_node(reading["parsed"], 0)
    estimated_rows = _statement_estimated_rows(reading["parsed"])
    accesses = parse_plan_access(reading["parsed"])

    assert len(parse_count) == 1
    assert node is not None and node["node_id"] == 0
    assert estimated_rows is not None
    assert [access.table for access in accesses.accesses]


def test_read_plan_gives_the_same_results_as_the_separate_calls() -> None:
    plan = (FIXTURES / "columnstore_filter_not_pushed.xml").read_text(encoding="utf-8")

    reading = read_plan(plan, top_n=5, tree_budget=40)

    assert reading["analysis"] == analyze_plan(plan)
    assert reading["digest"] == build_plan_digest(plan, top_n=5, tree_budget=40)


def test_analyze_plan_alone_parses_once(parse_count: list[int], showplan) -> None:
    analyze_plan(showplan.plan(showplan.scan(0)))

    assert len(parse_count) == 1


def test_read_plan_reports_an_unusable_plan_without_raising() -> None:
    reading = read_plan("<broken")

    assert reading["parsed"] is None
    assert reading["analysis"]["parse_error"]
    assert reading["digest"]["parse_error"]


def test_read_plan_can_skip_the_digest(showplan) -> None:
    assert read_plan(showplan.plan(showplan.scan(0)), include_digest=False)["digest"] is None


def test_read_plan_runs_in_a_worker_thread(showplan) -> None:
    plan = showplan.plan(showplan.scan(0))

    reading = asyncio.run(asyncio.to_thread(read_plan, plan, in_flight=True))

    assert reading["analysis"]["in_flight"] is True
    assert reading["digest"]["in_flight"] is True
    assert parse_plan(reading["parsed"]) is reading["parsed"]
