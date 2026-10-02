from __future__ import annotations

from pathlib import Path

import pytest

from azure_sql_mcp.showplan_access import parse_plan_access
from azure_sql_mcp.showplan_access import unquote

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "showplans"


def _plan(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _access(summary, *, table: str, operation: str):
    matches = [a for a in summary.accesses if a.table == table and a.operation == operation]
    assert len(matches) == 1, [(a.table, a.operation) for a in summary.accesses]
    return matches[0]


def test_seek_with_residual_range_and_sort_is_extracted() -> None:
    summary = parse_plan_access(_plan("seek_residual_lookup_sort.xml"))
    seek = _access(summary, table="Orders", operation="seek")

    assert seek.schema == "Sales"
    assert seek.index_name == "FK_Sales_Orders_CustomerID"
    assert seek.index_kind == "NonClustered"
    assert seek.seek_eq_columns == ("CustomerID",)
    assert seek.seek_range_columns == ()
    assert seek.residual == {"OrderDate": "range"}
    assert seek.output_columns == ("OrderID", "OrderDate")
    # The sort above the join needs OrderDate order from this table.
    assert seek.order_columns == (("OrderDate", "ASC"),)


def test_key_lookup_is_paired_with_the_index_that_fed_it() -> None:
    summary = parse_plan_access(_plan("seek_residual_lookup_sort.xml"))
    lookup = _access(summary, table="Orders", operation="lookup")

    assert lookup.index_name == "PK_Sales_Orders"
    assert lookup.paired_index == "FK_Sales_Orders_CustomerID"
    assert lookup.output_columns == ("Comments",)
    assert lookup.residual == {"Status": "eq"}
    assert lookup.estimated_executions == pytest.approx(400.0)
    # The lookup dominates the plan: its own cost is most of the statement.
    assert lookup.cost_share == pytest.approx(0.8324 / 0.85, rel=1e-3)


def test_statement_cost_shares_add_up_to_the_statement() -> None:
    summary = parse_plan_access(_plan("seek_residual_lookup_sort.xml"))
    shares = sum(access.cost_share for access in summary.accesses)

    assert 0.97 < shares <= 1.0


def test_missing_index_hint_is_parsed_without_brackets() -> None:
    summary = parse_plan_access(_plan("seek_residual_lookup_sort.xml"))
    hint = summary.statements[0].missing_indexes[0]

    assert (hint.schema, hint.table) == ("Sales", "Orders")
    assert hint.equality == ("CustomerID", "Status")
    assert hint.inequality == ("OrderDate",)
    assert hint.include == ("Comments",)
    assert hint.impact == pytest.approx(72.5)


def test_scan_residuals_are_classified_by_how_they_can_seek() -> None:
    summary = parse_plan_access(_plan("clustered_scan_residuals.xml"))
    scan = _access(summary, table="OrderLines", operation="scan")

    assert scan.index_kind == "Clustered"
    assert scan.residual["StockItemID"] == "eq"
    assert scan.residual["PickingCompletedWhen"] == "eq"  # IS NULL can seek
    assert scan.residual["PackageTypeID"] == "eq"  # an IN list is a multi-seek
    # Columns that only appear across an OR of different columns cannot seek.
    assert scan.residual["Description"] == "other"
    assert scan.residual["UnitPrice"] == "other"
    assert scan.output_columns == ("OrderLineID", "Quantity")
    assert scan.cost_share == pytest.approx(1.0)


def test_functions_and_implicit_conversions_on_columns_are_non_sargable() -> None:
    summary = parse_plan_access(_plan("heap_scan_nonsargable.xml"))
    scan = _access(summary, table="EventLog", operation="heap_scan")

    assert scan.index_kind == "Heap"
    assert scan.residual == {}
    assert scan.nonsargable == {"CreatedAt": "convert", "AccountNumber": "convert_implicit"}
    assert scan.implicit_conversion_columns == ("AccountNumber",)


BOOLEAN_FORMS = [("1", "0"), ("true", "false"), ("True", "False")]


@pytest.mark.parametrize(("true", "false"), BOOLEAN_FORMS)
def test_boolean_attributes_accept_true_false_and_one_zero(true: str, false: str) -> None:
    # xsd:boolean allows 1/0 and true/false; SSMS re-saves plans with true/false.
    plan = (
        _plan("heap_scan_nonsargable.xml")
        .replace('Implicit="1"', f'Implicit="{true}"')
        .replace('Implicit="0"', f'Implicit="{false}"')
        .replace('Ordered="0" ForcedIndex="0"', f'Ordered="{true}" ForcedIndex="{true}"')
    )
    scan = _access(parse_plan_access(plan), table="EventLog", operation="heap_scan")

    # A false Implicit is an explicit CONVERT the query wrote; a true one is the type mismatch.
    assert scan.nonsargable == {"CreatedAt": "convert", "AccountNumber": "convert_implicit"}
    assert scan.implicit_conversion_columns == ("AccountNumber",)
    assert scan.ordered is True
    assert scan.forced_index is True


@pytest.mark.parametrize(("true", "false"), BOOLEAN_FORMS)
def test_lookup_flag_and_sort_direction_accept_both_boolean_forms(true: str, false: str) -> None:
    plan = (
        _plan("seek_residual_lookup_sort.xml")
        .replace(
            'PhysicalOp="Key Lookup" LogicalOp="Key Lookup"',
            'PhysicalOp="Clustered Index Seek" LogicalOp="Clustered Index Seek"',
        )
        .replace('Lookup="1"', f'Lookup="{true}"')
        .replace('Ascending="1"', f'Ascending="{false}"')
    )
    summary = parse_plan_access(plan)

    lookup = _access(summary, table="Orders", operation="lookup")
    seek = _access(summary, table="Orders", operation="seek")
    assert lookup.paired_index == "FK_Sales_Orders_CustomerID"
    assert seek.order_columns == (("OrderDate", "DESC"),)


def test_scalar_udf_on_a_column_is_non_sargable(showplan) -> None:
    # The showplan XSD names a scalar UDF call UserDefinedFunction inside a ScalarOperator.
    predicate = (
        '<Predicate><ScalarOperator><Compare CompareOp="EQ">'
        '<ScalarOperator><UserDefinedFunction FunctionName="[db].[dbo].[fn_Active]"><ScalarOperator><Identifier>'
        f'{showplan.column("Customers", "CustomerID")}'
        "</Identifier></ScalarOperator></UserDefinedFunction></ScalarOperator>"
        '<ScalarOperator><Const ConstValue="(1)" /></ScalarOperator>'
        "</Compare></ScalarOperator></Predicate>"
    )
    summary = parse_plan_access(showplan.plan(showplan.scan(0, "Customers", body=predicate)))
    scan = _access(summary, table="Customers", operation="scan")

    assert scan.nonsargable == {"CustomerID": "udf"}
    assert scan.residual == {}


@pytest.mark.parametrize("udf_first", [True, False])
def test_a_udf_wrap_never_hides_another_wrapper_on_the_column(showplan, udf_first: bool) -> None:
    # dbo.fn_Norm(AccountNumber) = 1 AND CONVERT_IMPLICIT(nvarchar, AccountNumber) = @acct.
    column = f"<ScalarOperator><Identifier>{showplan.column('Accounts', 'AccountNumber')}</Identifier></ScalarOperator>"
    udf = (
        '<ScalarOperator><Compare CompareOp="EQ"><ScalarOperator><UserDefinedFunction FunctionName="[db].[dbo].[fn_Norm]">'
        f'{column}</UserDefinedFunction></ScalarOperator><ScalarOperator><Const ConstValue="(1)" /></ScalarOperator>'
        "</Compare></ScalarOperator>"
    )
    implicit = (
        '<ScalarOperator><Compare CompareOp="EQ"><ScalarOperator><Convert DataType="nvarchar" Style="0" Implicit="1">'
        f'{column}</Convert></ScalarOperator><ScalarOperator><Identifier><ColumnReference Column="@acct" /></Identifier>'
        "</ScalarOperator></Compare></ScalarOperator>"
    )
    conjuncts = udf + implicit if udf_first else implicit + udf
    predicate = f'<Predicate><ScalarOperator><Logical Operation="AND">{conjuncts}</Logical></ScalarOperator></Predicate>'

    scan = _access(parse_plan_access(showplan.plan(showplan.scan(0, "Accounts", body=predicate))), table="Accounts", operation="scan")

    assert scan.nonsargable == {"AccountNumber": "convert_implicit"}
    assert scan.implicit_conversion_columns == ("AccountNumber",)


def test_rid_lookup_on_a_heap_is_paired_and_internal_columns_are_dropped() -> None:
    summary = parse_plan_access(_plan("heap_seek_rid_lookup.xml"))
    seek = _access(summary, table="EventLog", operation="seek")
    lookup = _access(summary, table="EventLog", operation="rid_lookup")

    assert seek.seek_eq_columns == ("UserID",)
    assert seek.output_columns == ("EventID",)  # Bmk1000 is internal
    assert lookup.paired_index == "IX_EventLog_UserID"
    assert lookup.output_columns == ("Payload", "Severity")
    assert lookup.seek_eq_columns == ()


def test_dml_targets_list_every_index_the_statement_maintains() -> None:
    summary = parse_plan_access(_plan("update_scan_dml.xml"))
    statement = summary.statements[0]

    assert statement.statement_type == "UPDATE"
    target = statement.dml_targets[0]
    assert (target.schema, target.table, target.operation) == ("Sales", "Orders", "update")
    assert target.indexes_maintained == ("PK_Sales_Orders", "IX_Orders_Status")
    scan = _access(summary, table="Orders", operation="scan")
    assert scan.residual == {"CustomerID": "eq", "Status": "eq"}


def test_join_keys_and_group_order_attach_to_the_right_tables() -> None:
    summary = parse_plan_access(_plan("hash_join_stream_aggregate.xml"))
    customers = _access(summary, table="Customers", operation="scan")
    orders = _access(summary, table="Orders", operation="scan")

    assert customers.join_columns == ("CustomerID",)
    assert customers.order_columns == (("CustomerName", "ASC"),)
    assert orders.join_columns == ("CustomerID",)
    assert orders.residual == {"OrderDate": "range"}
    assert orders.order_columns == ()


def test_legacy_seek_predicate_format_yields_equality_then_range() -> None:
    summary = parse_plan_access(_plan("legacy_seek_predicate_range.xml"))
    seek = _access(summary, table="Products", operation="seek")

    assert seek.seek_eq_columns == ("CategoryID",)
    assert seek.seek_range_columns == ("Price",)


@pytest.mark.parametrize("bad", ["", "   ", "<not-xml", "<ShowPlanXML/>"])
def test_unusable_plans_never_raise(bad: str) -> None:
    summary = parse_plan_access(bad)

    assert list(summary.accesses) == []


def test_xml_declaration_is_tolerated() -> None:
    summary = parse_plan_access('<?xml version="1.0" encoding="utf-16"?>\n' + _plan("legacy_seek_predicate_range.xml"))

    assert summary.parse_error is None
    assert len(list(summary.accesses)) == 1


def test_unquote_handles_escaped_brackets() -> None:
    assert unquote("[dbo]") == "dbo"
    assert unquote("[a]]b]") == "a]b"
    assert unquote("plain") == "plain"
    assert unquote(None) is None


def test_eager_index_spool_gives_its_keys_and_cost_to_the_feeding_scan() -> None:
    summary = parse_plan_access(_plan("eager_index_spool.xml"))
    feeder = _access(summary, table="OrderLines", operation="scan")

    assert feeder.spool_node_id == 2
    assert feeder.spool_eq_columns == ("OrderID",)
    assert set(feeder.output_columns) >= {"Quantity", "UnitPrice"}
    # The spool's own cost (10.5 - 4) joins the scan's: one index removes both.
    assert feeder.cost_share == pytest.approx((4 + 6.5) / 12)
    assert feeder.as_dict()["eager_spool"] == {"node_id": 2, "eq_columns": ["OrderID"], "range_columns": []}
    assert feeder.residual == {}
