"""Noise fixes from the 2026-11-03 review (findings 27, 28, 29, 30, 76), IF and WHILE
condition plans, misestimate origins and exclusive operator times."""

from __future__ import annotations

import pytest

from azure_sql_mcp.plan_digest import build_plan_digest
from azure_sql_mcp.plan_rules import analyze_plan
from azure_sql_mcp.plan_tree import build_tree
from azure_sql_mcp.plan_tree import parse_plan
from azure_sql_mcp.plan_tree import parse_showplan
from azure_sql_mcp.plan_tree import statements
from azure_sql_mcp.plans import PlansService
from azure_sql_mcp.safe_sql import SafeSqlValidator
from azure_sql_mcp.showplan_access import parse_plan_access

NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"


def _rules(plan_xml: str, **options) -> dict[str, dict]:
    return {finding["rule"]: finding for finding in analyze_plan(plan_xml, **options)["findings"]}


def _run(rows: int, *, executions: int = 1, elapsed: int | None = None, end_of_scans: int | None = None) -> str:
    extra = f' ActualElapsedms="{elapsed}" ActualCPUms="{elapsed}"' if elapsed is not None else ""
    extra += f' ActualEndOfScans="{end_of_scans}"' if end_of_scans is not None else ""
    return (
        f'<RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="{rows}" '
        f'ActualExecutions="{executions}" ActualExecutionMode="Row"{extra} /></RunTimeInformation>'
    )


def _converted_order_id(showplan) -> str:
    return (
        '<ScalarOperator ScalarString="CONVERT(varchar(20),[db].[dbo].[Orders].[OrderId],0)">'
        '<Convert DataType="varchar" Length="20" Style="0" Implicit="0"><ScalarOperator><Identifier>'
        f'{showplan.column("Orders", "OrderId")}</Identifier></ScalarOperator></Convert></ScalarOperator>'
    )


_CONVERT_WARNING = (
    '<Warnings><PlanAffectingConvert ConvertIssue="{issue}" '
    'Expression="CONVERT(varchar(20),[db].[dbo].[Orders].[OrderId],0)" /></Warnings>'
)


# --- plan_affecting_convert (findings 29 and 30) -------------------------------------------


def test_convert_in_the_select_list_only_is_information(showplan) -> None:
    # SELECT CONVERT(varchar(20), o.OrderId) FROM dbo.Orders o WHERE o.OrderId = @id: a clean seek.
    seek = showplan.scan(
        1, "Orders", physical="Clustered Index Seek",
        body="<SeekPredicates>" + showplan.seek_keys("Orders", "OrderId", "@id") + "</SeekPredicates>",
    )
    output = f'<DefinedValues><DefinedValue><ColumnReference Column="Expr1002" />{_converted_order_id(showplan)}</DefinedValue></DefinedValues>'
    plan = showplan.plan(
        showplan.relop(0, "Compute Scalar", None, seek, body=output),
        plan_children=_CONVERT_WARNING.format(issue="Cardinality Estimate"),
    )

    finding = _rules(plan)["plan_affecting_convert"]

    assert finding["severity"] == "info"
    assert finding["evidence"]["position"] == "output_only"
    assert "only in an output expression" in finding["message"]
    assert "types match" not in finding["fix"]


def test_convert_in_a_predicate_stays_a_finding_and_names_the_explicit_convert(showplan) -> None:
    predicate = (
        '<Predicate><ScalarOperator><Compare CompareOp="EQ">'
        + _converted_order_id(showplan)
        + '<ScalarOperator><Identifier><ColumnReference Column="@id" /></Identifier></ScalarOperator>'
        "</Compare></ScalarOperator></Predicate>"
    )
    plan = showplan.plan(
        showplan.scan(0, "Orders", body=predicate), plan_children=_CONVERT_WARNING.format(issue="Cardinality Estimate")
    )

    finding = _rules(plan)["plan_affecting_convert"]

    assert finding["severity"] == "medium"
    assert finding["evidence"]["position"] == "plan_shaping"
    assert "explicit CONVERT" in finding["fix"]


def test_converted_value_that_a_sort_uses_shapes_the_plan(showplan) -> None:
    # ORDER BY CONVERT(varchar(20), OrderId): the Compute Scalar defines it, the Sort orders by it.
    output = f'<DefinedValues><DefinedValue><ColumnReference Column="Expr1002" />{_converted_order_id(showplan)}</DefinedValue></DefinedValues>'
    compute = showplan.relop(1, "Compute Scalar", None, showplan.scan(2, "Orders"), body=output)
    order_by = '<OrderBy><OrderByColumn Ascending="1"><ColumnReference Column="Expr1002" /></OrderByColumn></OrderBy>'
    plan = showplan.plan(
        showplan.relop(0, "Sort", None, compute, body=order_by),
        plan_children=_CONVERT_WARNING.format(issue="Cardinality Estimate"),
    )

    finding = _rules(plan)["plan_affecting_convert"]

    assert finding["severity"] == "medium"
    assert finding["evidence"]["position"] == "plan_shaping"


def test_convert_that_the_plan_does_not_show_keeps_its_severity(showplan) -> None:
    plan = showplan.plan(showplan.scan(0, "Orders"), plan_children=_CONVERT_WARNING.format(issue="Cardinality Estimate"))

    finding = _rules(plan)["plan_affecting_convert"]

    assert finding["severity"] == "medium"
    assert finding["evidence"]["position"] is None


def test_seek_plan_convert_is_always_high(showplan) -> None:
    plan = showplan.plan(showplan.scan(0, "Orders"), plan_children=_CONVERT_WARNING.format(issue="Seek Plan"))

    assert _rules(plan)["plan_affecting_convert"]["severity"] == "high"


# --- table variables (finding 76) -------------------------------------------------------------


def test_table_variable_with_a_real_row_count_is_low_and_does_not_lead_with_a_temp_table(showplan) -> None:
    # Deferred compilation (compatibility level 150+): 48,000 of 48,000 rows estimated.
    plan = showplan.plan(showplan.scan(0, "@staging", est_rows=48_000, attrs='TableCardinality="48000"'))

    finding = _rules(plan)["table_variable"]

    assert finding["severity"] == "low"
    assert finding["evidence"]["fixed_guess"] is False
    assert "48,000 rows" in finding["message"]
    assert finding["fix"].startswith("No change is needed")


def test_table_variable_with_the_one_row_guess_stays_medium(showplan) -> None:
    finding = _rules(showplan.plan(showplan.scan(0, "@staging", est_rows=1)))["table_variable"]

    assert finding["severity"] == "medium"
    assert finding["evidence"]["fixed_guess"] is True
    assert "compatibility level 150" in finding["fix"]


# --- in-flight plans (finding 28) ---------------------------------------------------------------


def _running_scan(showplan, *, estimated: int, produced: int, end_of_scans: int) -> str:
    return showplan.plan(
        showplan.scan(0, "Orders", est_rows=estimated, runtime=_run(produced, elapsed=4000, end_of_scans=end_of_scans)),
        plan_children='<MemoryGrantInfo GrantedMemory="2097152" MaxUsedMemory="1024" />',
    )


def test_running_scan_is_not_called_overestimated(showplan) -> None:
    # 40,000 of an estimated 1,000,000 rows so far: the query is 4% through, not 25x overestimated.
    plan = _running_scan(showplan, estimated=1_000_000, produced=40_000, end_of_scans=0)

    in_flight = analyze_plan(plan, in_flight=True)
    finished = analyze_plan(plan)
    rules = {finding["rule"] for finding in in_flight["findings"]}

    assert "row_estimate_gap" not in rules and "scan_cardinality_misestimate" not in rules
    assert "excessive_memory_grant" not in rules  # the memory is not used yet
    assert in_flight["in_flight"] is True
    assert {"row_estimate_gap", "excessive_memory_grant"} <= {finding["rule"] for finding in finished["findings"]}
    assert "in_flight" not in finished


def test_operator_that_finished_in_a_running_plan_keeps_its_overestimate(showplan) -> None:
    plan = _running_scan(showplan, estimated=1_000_000, produced=40_000, end_of_scans=1)

    finding = _rules(plan, in_flight=True)["row_estimate_gap"]

    assert finding["evidence"]["direction"] == "over"
    assert finding["evidence"]["in_flight"] is True


def test_underestimate_on_a_running_operator_is_already_real(showplan) -> None:
    plan = _running_scan(showplan, estimated=10, produced=40_000, end_of_scans=0)

    assert _rules(plan, in_flight=True)["row_estimate_gap"]["evidence"]["direction"] == "under"


def test_digest_of_a_running_plan_hides_unfinished_overestimates(showplan) -> None:
    plan = _running_scan(showplan, estimated=1_000_000, produced=40_000, end_of_scans=0)

    in_flight = build_plan_digest(plan, in_flight=True)
    finished = build_plan_digest(plan)

    assert in_flight["in_flight"] is True
    assert "still growing" in in_flight["what_this_plan_can_show"]
    assert "cardinality_skew" not in in_flight["statements"][0]
    assert finished["statements"][0]["cardinality_skew"][0]["direction"] == "over"
    assert "in_flight" not in finished


# --- summary ratios (finding 27) -----------------------------------------------------------------


def test_summary_ratio_of_an_inner_seek_is_per_execution(showplan) -> None:
    # Outer 4,000 rows; the inner seek returns 1 row on each of its 4,000 executions, as estimated.
    plan = showplan.plan(
        showplan.relop(
            0, "Nested Loops", "Inner Join",
            showplan.scan(1, "Users", est_rows=4000, runtime=_run(4000)),
            showplan.scan(2, "Posts", physical="Index Seek", est_rows=1, runtime=_run(4000, executions=4000)),
            wrapper="NestedLoops", est_rows=4000, runtime=_run(4000),
        )
    )
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]

    seek = next(item for item in service.summarize_showplan_xml(plan)["operators"] if item["node_id"] == 2)

    assert seek["actual_rows"] == 4000
    assert seek["actual_rows_per_execution"] == 1.0
    assert seek["estimate_error_ratio"] == 1.0
    assert seek["actual_rows_ratio"] == 1.0


# --- IF and WHILE condition plans -----------------------------------------------------------------


def _condition_plan(showplan, *, with_query: bool = True) -> str:
    predicate = (
        '<Predicate><ScalarOperator><Compare CompareOp="EQ"><ScalarOperator><Convert DataType="date" Style="0" Implicit="0">'
        f'<ScalarOperator><Identifier>{showplan.column("Orders", "CreatedAt")}</Identifier></ScalarOperator></Convert>'
        '</ScalarOperator><ScalarOperator><Identifier><ColumnReference Column="@d" /></Identifier></ScalarOperator>'
        "</Compare></ScalarOperator></Predicate>"
    )
    condition = (
        "<Condition><QueryPlan>" + showplan.scan(0, "Orders", est_rows=90_000_000, cost=900.0, body=predicate) + "</QueryPlan></Condition>"
        if with_query
        else "<Condition />"
    )
    return (
        f'<ShowPlanXML xmlns="{NS}" Version="1.564"><BatchSequence><Batch><Statements>'
        '<StmtCond StatementText="IF EXISTS (SELECT 1 FROM dbo.Orders WHERE CONVERT(date, CreatedAt) = @d)" '
        f'StatementType="COND" StatementSubTreeCost="900">{condition}<Then><Statements>'
        '<StmtSimple StatementText="RETURN" StatementType="RETURN" /></Statements></Then></StmtCond>'
        "</Statements></Batch></BatchSequence></ShowPlanXML>"
    )


def test_if_exists_condition_plan_is_analysed(showplan) -> None:
    # The whole cost of this batch is the IF EXISTS scan of 90M rows.
    plan = _condition_plan(showplan)

    result = analyze_plan(plan)
    finding = {item["rule"]: item for item in result["findings"]}["non_sargable_predicate"]

    assert [(item["statement_type"], item["statement_kind"]) for item in result["statements"]] == [
        ("COND", "condition"),
        ("RETURN", "statement"),
    ]
    assert finding["statement_index"] == 0
    assert finding["evidence"]["column"] == "CreatedAt"
    assert parse_plan_access(plan).statements[0].accesses[0].table == "Orders"


def test_condition_plan_is_digested_with_its_kind(showplan) -> None:
    statement = build_plan_digest(_condition_plan(showplan))["statements"][0]

    assert statement["statement_kind"] == "condition"
    assert statement["operator_count"] == 1
    assert statement["top_operators"][0]["node_id"] == 0


def test_condition_without_a_query_is_not_listed(showplan) -> None:
    # IF @x = 1 has no plan of its own: listing it would add an empty statement to every digest.
    found = statements(parse_showplan(_condition_plan(showplan, with_query=False)))

    assert [statement.get("StatementType") for statement, _ in found] == ["RETURN"]


# --- one misestimate, one finding --------------------------------------------------------------------


def _chain(showplan, *, join_rows: int | None = None) -> str:
    seek = showplan.scan(4, "Orders", physical="Index Seek", est_rows=10, runtime=_run(20_000, elapsed=10))
    filter_ = showplan.relop(3, "Filter", None, seek, est_rows=10, runtime=_run(20_000, elapsed=12))
    compute = showplan.relop(2, "Compute Scalar", None, filter_, est_rows=10)
    if join_rows is not None:
        other = showplan.scan(5, "Customers", est_rows=100, runtime=_run(100, elapsed=1))
        compute = showplan.relop(
            2, "Hash Match", "Inner Join", filter_, other, wrapper="Hash", est_rows=10, runtime=_run(join_rows, elapsed=30)
        )
    sort = showplan.relop(
        1, "Sort", None, compute, est_rows=10, runtime=_run(join_rows or 20_000, elapsed=40),
        warnings='<Warnings><SpillToTempDb SpillLevel="1" SpilledThreadCount="1" /></Warnings>',
    )
    top = showplan.relop(0, "Top", None, sort, est_rows=10, runtime=_run(join_rows or 20_000, elapsed=41))
    statistics = (
        '<OptimizerStatsUsage><StatisticsInfo Database="[db]" Schema="[dbo]" Table="[Orders]" Statistics="[IX_Orders_Date]" '
        'ModificationCount="380000" SamplingPercent="0.4" LastUpdate="2026-06-01T02:00:00.00" /></OptimizerStatsUsage>'
    )
    return showplan.plan(top, plan_children=statistics)


def test_one_root_misestimate_is_one_finding(showplan) -> None:
    # The seek is 2,000x underestimated; the Filter, Sort and Top above it only inherit the error.
    result = analyze_plan(_chain(showplan))
    gaps = [finding for finding in result["findings"] if finding["rule"] == "row_estimate_gap"]

    assert [finding["node_id"] for finding in gaps] == [4]
    assert gaps[0]["evidence"]["propagated_node_ids"] == [0, 1, 3]
    assert "node(s) 0, 1, 3" in gaps[0]["message"]
    assert "tempdb_spill" in {finding["rule"] for finding in result["findings"]}


def test_misestimate_carries_the_statistics_the_optimizer_used(showplan) -> None:
    gap = _rules(_chain(showplan))["row_estimate_gap"]

    assert gap["evidence"]["statistics"] == [
        {
            "statistics": "IX_Orders_Date",
            "modification_count": 380000.0,
            "sampling_percent": 0.4,
            "last_update": "2026-06-01T02:00:00.00",
        }
    ]


def test_join_that_makes_the_error_far_worse_is_its_own_origin(showplan) -> None:
    # The join turns a 2,000x error into 5,000,000x: a second cause, above the seek's.
    result = analyze_plan(_chain(showplan, join_rows=50_000_000))
    gaps = {finding["node_id"]: finding for finding in result["findings"] if finding["rule"] == "row_estimate_gap"}

    assert sorted(gaps) == [2, 4]
    assert gaps[2]["evidence"]["amplifies_node_id"] == 3
    assert gaps[2]["evidence"]["propagated_node_ids"] == [0, 1]
    assert gaps[4]["evidence"]["propagated_node_ids"] == [3]


def test_digest_points_inherited_skew_at_its_origin(showplan) -> None:
    skew = {item["node_id"]: item for item in build_plan_digest(_chain(showplan))["statements"][0]["cardinality_skew"]}

    assert "inherited_from_node_id" not in skew[4]
    assert skew[1]["inherited_from_node_id"] == 4


# --- exclusive operator times --------------------------------------------------------------------------


@pytest.mark.parametrize(("exclusive", "parent_self"), [("true", 100.0), ("false", 0.0)])
def test_exclusive_operator_times_are_not_subtracted(showplan, exclusive: str, parent_self: float) -> None:
    # ExclusiveProfileTimeActive: each operator reports its own time, so the parent's 100 ms
    # is its own and must not have the child's 900 ms taken from it.
    plan = showplan.plan(
        showplan.relop(0, "Sort", None, showplan.scan(1, runtime=_run(10, elapsed=900)), runtime=_run(10, elapsed=100)),
        plan_attrs=f'ExclusiveProfileTimeActive="{exclusive}"',
    )
    query_plan = statements(parse_showplan(plan))[0][1]

    nodes = build_tree(query_plan)

    assert nodes[0].self_elapsed_ms == parent_self
    assert nodes[1].self_elapsed_ms == 900.0
    assert build_plan_digest(plan)["runtime"].get("exclusive_operator_times", False) is (exclusive == "true")


def test_parse_plan_returns_a_parsed_plan_unchanged(showplan) -> None:
    parsed = parse_plan(showplan.plan(showplan.scan(0)))

    assert parse_plan(parsed) is parsed
    assert [len(item.nodes) for item in parsed.statements] == [1]
