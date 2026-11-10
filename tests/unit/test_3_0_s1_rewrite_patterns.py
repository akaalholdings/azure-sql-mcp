"""Rewrite patterns named from the plan, so the optimizer gets a pattern and not only a symptom.

Each finding carries ``evidence.rewrite_pattern``, a stable code. Every pattern
has a plan that shows it and a near miss that must not.
"""

from __future__ import annotations

import pytest

from azure_sql_mcp.plan_rules import analyze_plan


def _rules(plan_xml: str) -> dict[str, dict]:
    return {finding["rule"]: finding for finding in analyze_plan(plan_xml)["findings"]}


def _run(rows: int, *, executions: int = 1, elapsed: int | None = None) -> str:
    timing = f' ActualElapsedms="{elapsed}" ActualCPUms="{elapsed}"' if elapsed is not None else ""
    return (
        f'<RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="{rows}" '
        f'ActualExecutions="{executions}" ActualExecutionMode="Row"{timing} /></RunTimeInformation>'
    )


def _operand(showplan, value: str) -> str:
    """A constant, an ``@variable`` or ``Table.Column`` as one ScalarOperator."""

    if value.startswith("@"):
        return f'<ScalarOperator><Identifier><ColumnReference Column="{value}" /></Identifier></ScalarOperator>'
    if value == "NULL" or value.startswith("("):
        return f'<ScalarOperator><Const ConstValue="{value}" /></ScalarOperator>'
    table, column = value.split(".")
    return f"<ScalarOperator><Identifier>{showplan.column(table, column)}</Identifier></ScalarOperator>"


def _compare(showplan, left: str, op: str, right: str) -> str:
    return (
        f'<ScalarOperator><Compare CompareOp="{op}">{_operand(showplan, left)}{_operand(showplan, right)}'
        "</Compare></ScalarOperator>"
    )


def _logical(operation: str, *operands: str) -> str:
    return f'<ScalarOperator><Logical Operation="{operation}">{"".join(operands)}</Logical></ScalarOperator>'


def _predicate(scalar: str) -> str:
    return f"<Predicate>{scalar}</Predicate>"


def _scan(showplan, predicate: str, *, physical: str = "Clustered Index Scan", attrs: str = "") -> str:
    return showplan.plan(showplan.scan(0, "Orders", physical=physical, body=predicate, attrs=attrs))


# --- catch-all optional parameters ----------------------------------------------------


def test_catch_all_optional_parameters_are_named(showplan) -> None:
    # WHERE (@status IS NULL OR Status = @status) AND (@cust IS NULL OR CustomerID = @cust)
    predicate = _predicate(
        _logical(
            "AND",
            _logical("OR", _compare(showplan, "@status", "IS", "NULL"), _compare(showplan, "Orders.Status", "EQ", "@status")),
            _logical("OR", _compare(showplan, "@cust", "IS", "NULL"), _compare(showplan, "Orders.CustomerID", "EQ", "@cust")),
        )
    )

    finding = _rules(_scan(showplan, predicate))["catch_all_predicate"]

    assert finding["node_id"] == 0
    assert finding["severity"] == "high"  # the scan is the whole plan
    assert finding["evidence"]["rewrite_pattern"] == "catch_all_optional_parameter"
    assert finding["evidence"]["parameters"] == ["@status", "@cust"]
    assert finding["evidence"]["columns"] == ["Status", "CustomerID"]
    assert "node 0" in finding["message"] and "@status" in finding["message"]
    assert "OPTION (RECOMPILE)" in finding["fix"] and "sp_executesql" in finding["fix"]


def test_isnull_form_of_the_catch_all_is_named(showplan) -> None:
    # WHERE Status = ISNULL(@status, Status)
    predicate = _predicate(
        '<ScalarOperator><Compare CompareOp="EQ">'
        + _operand(showplan, "Orders.Status")
        + '<ScalarOperator><Intrinsic FunctionName="isnull">'
        + _operand(showplan, "@status")
        + _operand(showplan, "Orders.Status")
        + "</Intrinsic></ScalarOperator></Compare></ScalarOperator>"
    )

    finding = _rules(_scan(showplan, predicate))["catch_all_predicate"]

    assert finding["evidence"]["parameters"] == ["@status"]
    assert finding["evidence"]["columns"] == ["Status"]


def test_column_is_null_or_equals_is_not_a_catch_all(showplan) -> None:
    # WHERE Status IS NULL OR Status = @status tests the column, not the parameter.
    predicate = _predicate(
        _logical("OR", _compare(showplan, "Orders.Status", "IS", "NULL"), _compare(showplan, "Orders.Status", "EQ", "@status"))
    )

    assert "catch_all_predicate" not in _rules(_scan(showplan, predicate))


def test_plain_parameter_predicate_is_not_a_catch_all(showplan) -> None:
    # With OPTION (RECOMPILE) the NULL branch is folded away: only the comparison is left.
    assert "catch_all_predicate" not in _rules(_scan(showplan, _predicate(_compare(showplan, "Orders.Status", "EQ", "@status"))))


# --- OR across columns ---------------------------------------------------------------------


def test_or_across_different_columns_is_named(showplan) -> None:
    predicate = _predicate(
        _logical("OR", _compare(showplan, "Orders.CustomerID", "EQ", "@c"), _compare(showplan, "Orders.SalesRepID", "EQ", "@r"))
    )

    finding = _rules(_scan(showplan, predicate))["or_across_columns"]

    assert finding["node_id"] == 0
    assert finding["evidence"]["columns"] == ["CustomerID", "SalesRepID"]
    assert finding["evidence"]["rewrite_pattern"] == "or_across_columns"
    assert "CustomerID, SalesRepID" in finding["message"]
    assert "UNION ALL" in finding["fix"]


def test_in_list_on_one_column_is_not_an_or_across_columns(showplan) -> None:
    predicate = _predicate(
        _logical("OR", _compare(showplan, "Orders.Status", "EQ", "(1)"), _compare(showplan, "Orders.Status", "EQ", "(2)"))
    )

    assert "or_across_columns" not in _rules(_scan(showplan, predicate))


def test_or_on_a_small_table_is_not_reported(showplan) -> None:
    predicate = _predicate(
        _logical("OR", _compare(showplan, "Orders.CustomerID", "EQ", "@c"), _compare(showplan, "Orders.SalesRepID", "EQ", "@r"))
    )

    assert "or_across_columns" not in _rules(_scan(showplan, predicate, attrs='TableCardinality="400"'))


# --- deep OFFSET paging ---------------------------------------------------------------------


def _paged(showplan, offset: str, *, parameters: str = "", runtime: tuple[int, int] | None = None) -> str:
    operand = (
        f'<Identifier><ColumnReference Column="{offset}" /></Identifier>'
        if offset.startswith("@")
        else f'<Const ConstValue="({offset})" />'
    )
    body = f'<OffsetExpression><ScalarOperator ScalarString="{offset}">{operand}</ScalarOperator></OffsetExpression>'
    child_rows, top_rows = runtime or (0, 0)
    top = showplan.relop(
        0, "Top", None,
        showplan.scan(1, "Orders", runtime=_run(child_rows, elapsed=900) if runtime else ""),
        body=body, runtime=_run(top_rows, elapsed=950) if runtime else "",
    )
    return showplan.plan(top, plan_children=parameters)


def test_deep_offset_is_named(showplan) -> None:
    finding = _rules(_paged(showplan, "200000"))["deep_offset_paging"]

    assert finding["node_id"] == 0
    assert finding["evidence"]["rows_skipped"] == 200_000
    assert finding["evidence"]["measured"] is False
    assert finding["evidence"]["rewrite_pattern"] == "deep_offset_paging"
    assert "node 0" in finding["message"] and "200,000 rows" in finding["message"]
    assert "keyset" in finding["fix"]


def test_offset_from_a_parameter_uses_its_value(showplan) -> None:
    parameters = '<ParameterList><ColumnReference Column="@skip" ParameterCompiledValue="(50000)" /></ParameterList>'

    assert _rules(_paged(showplan, "@skip", parameters=parameters))["deep_offset_paging"]["evidence"]["rows_skipped"] == 50_000


def test_measured_skip_is_used_on_an_actual_plan(showplan) -> None:
    finding = _rules(_paged(showplan, "@skip", runtime=(150_020, 20)))["deep_offset_paging"]

    assert finding["evidence"]["rows_skipped"] == 150_000
    assert finding["evidence"]["measured"] is True
    assert finding["benefit_share"] is not None


@pytest.mark.parametrize("offset", ["20", "@skip"])
def test_shallow_or_unknown_offset_is_not_reported(showplan, offset: str) -> None:
    # An unknown @skip on an estimated plan is not evidence of a deep page.
    assert "deep_offset_paging" not in _rules(_paged(showplan, offset))


# --- DISTINCT over a join fan-out -------------------------------------------------------------


def _distinct_join(showplan, *, joined: int = 480_000, outputs: str | None = None, aggregate: bool = False) -> str:
    customers = showplan.scan(2, "Customers", est_rows=1000, cost=0.1, runtime=_run(1000, elapsed=10))
    orders = showplan.scan(3, "Orders", est_rows=500_000, cost=0.4, runtime=_run(500_000, elapsed=600))
    join = showplan.relop(
        1, "Hash Match", "Inner Join", customers, orders, wrapper="Hash", cost=0.7, est_rows=joined,
        runtime=_run(joined, elapsed=1900),
    )
    columns = outputs or (showplan.column("Customers", "CustomerID") + showplan.column("Customers", "Name"))
    if aggregate:
        body = '<DefinedValues><DefinedValue><ColumnReference Column="Expr1004" /><ScalarOperator><Aggregate AggType="SUM" Distinct="0" /></ScalarOperator></DefinedValue></DefinedValues>'
        top = showplan.relop(0, "Hash Match", "Aggregate", join, wrapper="Hash", cost=1.0, body=body, outputs=columns, runtime=_run(950, elapsed=2400))
    else:
        top = showplan.relop(0, "Sort", "Distinct Sort", join, cost=1.0, outputs=columns, runtime=_run(950, elapsed=2400))
    return showplan.plan(top, plan_children='<QueryTimeStats ElapsedTime="2400" CpuTime="2390" />')


def test_distinct_that_collapses_a_join_fan_out_is_named(showplan) -> None:
    # 1,000 customers joined to 480,000 order rows, then DISTINCT back to 950 customers.
    finding = _rules(_distinct_join(showplan))["distinct_over_join_fanout"]

    assert finding["node_id"] == 0
    assert finding["evidence"]["rewrite_pattern"] == "distinct_over_join_fanout"
    assert finding["evidence"]["table"] == "dbo.Customers"
    assert finding["evidence"]["join_node_id"] == 1
    assert finding["evidence"]["rows_joined"] == 480_000
    assert finding["evidence"]["rows_kept"] == 950
    assert "node 0" in finding["message"] and "node 1" in finding["message"]
    assert "EXISTS" in finding["fix"]


def test_distinct_over_columns_of_both_tables_is_not_reported(showplan) -> None:
    both = showplan.column("Customers", "CustomerID") + showplan.column("Orders", "Status")

    assert "distinct_over_join_fanout" not in _rules(_distinct_join(showplan, outputs=both))


def test_aggregate_over_a_join_is_not_a_distinct(showplan) -> None:
    # SUM over the order rows of each customer needs the joined rows.
    assert "distinct_over_join_fanout" not in _rules(_distinct_join(showplan, aggregate=True))


def test_group_by_with_count_in_a_stored_plan_is_not_a_distinct() -> None:
    # SELECT c.CustomerName, COUNT(*) ... GROUP BY c.CustomerName: 40,000 joined rows become 660
    # groups, and the count needs every one of them.
    from pathlib import Path

    plan = (Path(__file__).resolve().parents[1] / "fixtures" / "showplans" / "hash_join_stream_aggregate.xml").read_text(
        encoding="utf-8"
    )

    assert "distinct_over_join_fanout" not in _rules(plan)


def test_distinct_over_a_join_that_does_not_multiply_is_not_reported(showplan) -> None:
    assert "distinct_over_join_fanout" not in _rules(_distinct_join(showplan, joined=1200))


# --- patterns on existing rules ------------------------------------------------------------------


def _wrapped(showplan, element: str) -> str:
    column = f"<ScalarOperator><Identifier>{showplan.column('Orders', 'CreatedAt')}</Identifier></ScalarOperator>"
    return _predicate(
        f'<ScalarOperator><Compare CompareOp="EQ"><ScalarOperator>{element.format(column=column)}</ScalarOperator>'
        '<ScalarOperator><Identifier><ColumnReference Column="@d" /></Identifier></ScalarOperator></Compare></ScalarOperator>'
    )


@pytest.mark.parametrize(
    ("element", "rule", "pattern"),
    [
        ('<Intrinsic FunctionName="year">{column}</Intrinsic>', "non_sargable_predicate", "function_on_column"),
        ('<Convert DataType="date" Style="0" Implicit="0">{column}</Convert>', "non_sargable_predicate", "function_on_column"),
        (
            '<Convert DataType="nvarchar" Style="0" Implicit="1">{column}</Convert>',
            "implicit_conversion_on_column",
            "implicit_conversion_on_column",
        ),
        ('<UserDefinedFunction FunctionName="[db].[dbo].[fn_Day]">{column}</UserDefinedFunction>', "scalar_udf", "scalar_udf_in_predicate"),
    ],
)
def test_wrapped_column_findings_name_their_rewrite_pattern(showplan, element: str, rule: str, pattern: str) -> None:
    finding = _rules(_scan(showplan, _wrapped(showplan, element)))[rule]

    assert finding["evidence"]["rewrite_pattern"] == pattern


def test_scalar_udf_in_the_select_list_names_its_pattern(showplan) -> None:
    udf = (
        '<DefinedValues><DefinedValue><ColumnReference Column="Expr1002" /><ScalarOperator>'
        '<UserDefinedFunction FunctionName="[db].[dbo].[fn_Tax]" /></ScalarOperator></DefinedValue></DefinedValues>'
    )
    plan = showplan.plan(showplan.relop(0, "Compute Scalar", None, showplan.scan(1), body=udf))

    assert _rules(plan)["scalar_udf"]["evidence"]["rewrite_pattern"] == "scalar_udf_in_select_list"


def test_bare_column_predicate_names_no_rewrite_pattern(showplan) -> None:
    result = analyze_plan(_scan(showplan, _predicate(_compare(showplan, "Orders.Status", "EQ", "@status"))))

    assert not [finding for finding in result["findings"] if finding["evidence"].get("rewrite_pattern")]
