from __future__ import annotations

from pathlib import Path

import pytest

from azure_sql_mcp.plan_rules import analyze_plan

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "showplans"
NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _plan(relops: str, *, stmt_attrs: str = "", plan_attrs: str = "", plan_children: str = "") -> str:
    return (
        f'<ShowPlanXML xmlns="{NS}" Version="1.564"><BatchSequence><Batch><Statements>'
        f'<StmtSimple StatementType="SELECT" StatementSubTreeCost="1.0" {stmt_attrs}>'
        f"<QueryPlan {plan_attrs}>{plan_children}{relops}</QueryPlan>"
        "</StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"
    )


def _relop(node_id: int, physical: str, logical: str, inner: str = "", *, cost: float = 1.0, rows: float = 1.0, attrs: str = "") -> str:
    return (
        f'<RelOp NodeId="{node_id}" PhysicalOp="{physical}" LogicalOp="{logical}" '
        f'EstimateRows="{rows}" EstimatedTotalSubtreeCost="{cost}" {attrs}>'
        f"<OutputList />{inner}</RelOp>"
    )


def _scan(node_id: int, table: str = "[T]", *, cost: float = 0.5, rows: float = 1.0, runtime: str = "") -> str:
    return _relop(
        node_id,
        "Clustered Index Scan",
        "Clustered Index Scan",
        f"{runtime}<IndexScan Ordered=\"0\"><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"{table}\" Index=\"[PK]\" IndexKind=\"Clustered\" /></IndexScan>",
        cost=cost,
        rows=rows,
    )


def _rules(result: dict) -> dict[str, dict]:
    return {finding["rule"]: finding for finding in result["findings"]}


def test_non_sargable_and_implicit_conversions_are_high_when_they_dominate() -> None:
    result = analyze_plan(_fixture("heap_scan_nonsargable.xml"))
    rules = _rules(result)

    assert result["plan_kind"] == "estimated"
    assert rules["non_sargable_predicate"]["severity"] == "high"
    assert rules["non_sargable_predicate"]["evidence"]["column"] == "CreatedAt"
    assert rules["implicit_conversion_on_column"]["evidence"]["column"] == "AccountNumber"
    assert rules["plan_affecting_convert"]["severity"] == "high"
    assert result["families"]["predicates"] >= 3


def test_key_lookup_names_the_columns_and_the_feeding_index() -> None:
    rules = _rules(analyze_plan(_fixture("seek_residual_lookup_sort.xml")))

    lookup = rules["key_lookup"]
    assert lookup["severity"] == "high"
    assert lookup["evidence"]["fetched_columns"] == ["Comments"]
    assert lookup["evidence"]["feeding_index"] == "FK_Sales_Orders_CustomerID"
    assert "FK_Sales_Orders_CustomerID" in lookup["fix"]
    assert lookup["estimated_cost_share"] == pytest.approx(0.8324 / 0.85, rel=1e-3)
    assert rules["missing_index_hint"]["severity"] == "medium"
    assert rules["missing_index_hint"]["evidence"]["equality"] == ["CustomerID", "Status"]


def test_rid_lookup_and_scan_with_seekable_filters_are_index_leads() -> None:
    rid = _rules(analyze_plan(_fixture("heap_seek_rid_lookup.xml")))
    scan = _rules(analyze_plan(_fixture("clustered_scan_residuals.xml")))

    assert rid["rid_lookup"]["family"] == "indexes"
    assert scan["scan_with_seekable_predicate"]["severity"] == "high"
    assert set(scan["scan_with_seekable_predicate"]["evidence"]["residual_predicates"]) >= {"StockItemID"}


@pytest.mark.parametrize(
    ("relop", "rule", "severity"),
    [
        (_relop(0, "Index Spool", "Eager Spool"), "eager_index_spool", "high"),
        (_relop(0, "Row Count Spool", "Lazy Spool"), "row_count_spool", "medium"),
        (_relop(0, "Nested Loops", "Inner Join", '<Warnings NoJoinPredicate="true" /><NestedLoops />'), "no_join_predicate", "high"),
        (_relop(0, "Merge Join", "Inner Join", '<Merge ManyToMany="1" />'), "many_to_many_merge_join", "medium"),
        (
            _relop(0, "Compute Scalar", "Compute Scalar", '<ComputeScalar><DefinedValues><DefinedValue><ScalarOperator><UserDefinedFunction FunctionName="[db].[dbo].[fn_tax]" /></ScalarOperator></DefinedValue></DefinedValues></ComputeScalar>'),
            "scalar_udf",
            "medium",
        ),
        (_relop(0, "Table-valued function", "Table-valued function", "<TableValuedFunction />", rows=100), "multi_statement_tvf", "medium"),
        (_scan(0, "[@staging]", rows=1, cost=1.0), "table_variable", "medium"),
    ],
)
def test_operator_rules(relop: str, rule: str, severity: str) -> None:
    rules = _rules(analyze_plan(_plan(relop)))

    assert rules[rule]["severity"] == severity
    assert rules[rule]["node_id"] == 0


def test_nested_loops_with_a_very_busy_inner_side_is_flagged() -> None:
    inner = _relop(2, "Index Seek", "Index Seek", attrs='EstimateRebinds="25000" EstimateRewinds="0"', cost=0.6)
    outer = _scan(1, cost=0.3)
    relop = _relop(0, "Nested Loops", "Inner Join", f"<NestedLoops>{outer}{inner}</NestedLoops>", cost=1.0)

    finding = _rules(analyze_plan(_plan(relop)))["nested_loops_high_inner_executions"]

    assert finding["evidence"]["inner_executions"] == 25001
    assert finding["evidence"]["actual"] is False


def test_statement_level_warnings() -> None:
    plan = _plan(
        _scan(0),
        stmt_attrs='StatementOptmEarlyAbortReason="TimeOut"',
        plan_attrs='NonParallelPlanReason="TSQLUserDefinedFunctionsNotParallelizable" CompileCPU="1500" CompileTime="1600"',
        plan_children=(
            "<Warnings><ColumnsWithNoStatistics>"
            '<ColumnReference Database="[db]" Schema="[dbo]" Table="[T]" Column="Region" />'
            "</ColumnsWithNoStatistics></Warnings>"
            '<UnmatchedIndexes><Parameterization><Object Database="[db]" Schema="[dbo]" Table="[T]" Index="[IX_F]" /></Parameterization></UnmatchedIndexes>'
        ),
    )

    rules = _rules(analyze_plan(plan))

    assert rules["optimizer_early_abort"]["evidence"]["reason"] == "TimeOut"
    assert rules["non_parallel_plan_reason"]["severity"] == "medium"
    assert rules["high_compile_cpu"]["evidence"]["compile_cpu_ms"] == 1500
    assert rules["column_without_statistics"]["evidence"]["column"] == "Region"
    assert "unmatched_filtered_index" in rules


def test_actual_plan_runtime_rules() -> None:
    runtime = (
        "<RunTimeInformation>"
        '<RunTimeCountersPerThread Thread="0" ActualRows="0" ActualExecutions="1" />'
        '<RunTimeCountersPerThread Thread="1" ActualRows="190000" ActualExecutions="1" />'
        '<RunTimeCountersPerThread Thread="2" ActualRows="5000" ActualExecutions="1" />'
        '<RunTimeCountersPerThread Thread="3" ActualRows="5000" ActualExecutions="1" />'
        "</RunTimeInformation>"
    )
    sort = _relop(
        1,
        "Sort",
        "Sort",
        '<Warnings><SpillToTempDb SpillLevel="2" SpilledThreadCount="1" /><SortSpillDetails WritesToTempDb="4096" /></Warnings>'
        '<RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="200000" ActualExecutions="1" /></RunTimeInformation>',
        rows=2000,
        cost=0.5,
    )
    plan = _plan(
        sort.replace("</RelOp>", _scan(2, rows=1000, runtime=runtime) + "</RelOp>"),
        plan_children=(
            '<MemoryGrantInfo GrantedMemory="2097152" MaxUsedMemory="10240" GrantWaitTime="1200" />'
            '<ParameterList><ColumnReference Column="@region" ParameterCompiledValue="(1)" ParameterRuntimeValue="(6)" /></ParameterList>'
        ),
    )

    result = analyze_plan(plan)
    rules = _rules(result)

    assert result["plan_kind"] == "actual"
    assert rules["tempdb_spill"]["severity"] == "high"
    assert rules["row_estimate_gap"]["severity"] == "high"
    assert rules["parallel_thread_skew"]["evidence"]["max_thread_rows"] == 190000
    assert rules["parallel_thread_skew"]["evidence"]["threads"] == 3
    assert rules["memory_grant_wait"]["evidence"]["grant_wait_ms"] == 1200
    assert rules["excessive_memory_grant"]["severity"] == "medium"
    assert rules["sniffed_parameter_differs"]["evidence"]["runtime"] == "(6)"


def test_an_even_parallel_split_is_not_skew() -> None:
    runtime = "<RunTimeInformation>" + "".join(
        f'<RunTimeCountersPerThread Thread="{thread}" ActualRows="25000" ActualExecutions="1" />'
        for thread in range(1, 5)
    ) + "</RunTimeInformation>"

    rules = _rules(analyze_plan(_plan(_scan(0, rows=100000, runtime=runtime))))

    assert "parallel_thread_skew" not in rules


def test_estimated_plans_never_report_runtime_findings() -> None:
    plan = _plan(
        _scan(0),
        plan_children='<MemoryGrantInfo GrantedMemory="2097152" MaxUsedMemory="10" GrantWaitTime="99" />',
    )

    rules = _rules(analyze_plan(plan))

    assert not {"memory_grant_wait", "excessive_memory_grant", "row_estimate_gap", "tempdb_spill"} & set(rules)


def test_findings_are_sorted_by_severity_then_cost_share() -> None:
    result = analyze_plan(_fixture("seek_residual_lookup_sort.xml"))
    order = [finding["severity"] for finding in result["findings"]]
    rank = {"high": 3, "medium": 2, "low": 1, "info": 0}

    assert [rank[item] for item in order] == sorted((rank[item] for item in order), reverse=True)


@pytest.mark.parametrize("bad", ["", "<broken", "   "])
def test_unusable_input_reports_a_parse_error(bad: str) -> None:
    result = analyze_plan(bad)

    assert result["findings"] == []
    assert result["parse_error"]
