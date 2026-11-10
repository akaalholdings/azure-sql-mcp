"""Columnstore, batch mode and partition elimination: rules and digest evidence.

Each rule has a fixture plan that triggers it and a near miss that must not.
The near misses are the same fixtures with one fact changed, so the test shows
which fact the rule depends on.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from azure_sql_mcp.plan_digest import build_plan_digest
from azure_sql_mcp.plan_digest import describe_plan_node
from azure_sql_mcp.plan_rules import analyze_plan

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "showplans"


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _rules(plan_xml: str, **options) -> dict[str, dict]:
    return {finding["rule"]: finding for finding in analyze_plan(plan_xml, **options)["findings"]}


def _swap(plan_xml: str, old: str, new: str, *, count: int = 1) -> str:
    assert plan_xml.count(old) >= count, f"fixture no longer holds {old!r}"
    return plan_xml.replace(old, new)


def _without_runtime(plan_xml: str) -> str:
    """The estimated form of an actual plan fixture: what Query Store keeps."""

    plan_xml = re.sub(r"<RunTimeInformation>.*?</RunTimeInformation>", "", plan_xml, flags=re.DOTALL)
    plan_xml = re.sub(r"<RunTimePartitionSummary>.*?</RunTimePartitionSummary>", "", plan_xml, flags=re.DOTALL)
    return re.sub(r"<QueryTimeStats [^>]*/>", "", plan_xml)


# --- segment elimination -----------------------------------------------------


def test_columnstore_scan_that_skips_no_segment_is_named() -> None:
    # 48 rowgroups read, none skipped, to return 1.2M of 50M rows: the filter column
    # is not aligned with the rowgroups, and the scan is 93% of the elapsed time.
    rules = _rules(_fixture("columnstore_scan_no_segment_elimination.xml"))

    finding = rules["columnstore_no_segment_elimination"]
    assert finding["node_id"] == 2
    assert finding["family"] == "batch_mode"
    assert finding["severity"] == "high"
    assert finding["evidence"]["segments_read"] == 48
    assert finding["evidence"]["segments_skipped"] == 0
    assert finding["evidence"]["predicate_columns"] == ["SaleDate"]
    assert "node 2" in finding["message"] and "SaleDate" in finding["message"]
    assert "ORDER" in finding["fix"] and "partition" in finding["fix"]
    # A rowstore seek index is the wrong advice for a columnstore scan.
    assert "scan_with_seekable_predicate" not in rules


def test_columnstore_scan_that_skips_most_segments_is_healthy() -> None:
    plan = _swap(
        _fixture("columnstore_scan_no_segment_elimination.xml"),
        'ActualRowsRead="50331648" Batches="1338"',
        'ActualRowsRead="4194304" Batches="1338"',
    )
    plan = _swap(plan, 'SegmentReads="48" SegmentSkips="0"', 'SegmentReads="4" SegmentSkips="44"')

    rules = _rules(plan)

    assert "columnstore_no_segment_elimination" not in rules
    assert "columnstore_low_selectivity_scan" not in rules


def test_columnstore_scan_without_a_predicate_has_nothing_to_eliminate() -> None:
    plan = re.sub(
        r"<Predicate>.*?</Predicate>", "", _fixture("columnstore_scan_no_segment_elimination.xml"), flags=re.DOTALL
    )

    assert "columnstore_no_segment_elimination" not in _rules(plan)


def test_segment_rules_need_runtime_counters() -> None:
    rules = _rules(_without_runtime(_fixture("columnstore_scan_no_segment_elimination.xml")))

    assert "columnstore_no_segment_elimination" not in rules
    assert "columnstore_low_selectivity_scan" not in rules


def test_columnstore_scan_that_reads_far_more_rows_than_it_returns_is_named() -> None:
    # Some rowgroups are skipped, yet 41.9M rows are read to return 12,000.
    plan = _swap(
        _fixture("columnstore_scan_no_segment_elimination.xml"),
        'ActualRows="1204000" ActualRowsRead="50331648"',
        'ActualRows="12000" ActualRowsRead="41943040"',
    )
    plan = _swap(plan, 'SegmentReads="48" SegmentSkips="0"', 'SegmentReads="40" SegmentSkips="8"')

    rules = _rules(plan)

    finding = rules["columnstore_low_selectivity_scan"]
    assert finding["node_id"] == 2
    assert finding["evidence"]["rows_read"] == 41943040
    assert finding["evidence"]["rows"] == 12000
    assert finding["evidence"]["segments_skipped"] == 8
    assert "columnstore_no_segment_elimination" not in rules


def test_a_cheap_columnstore_scan_is_not_reported() -> None:
    # The same scan at 2% of the statement's time is not worth a finding.
    plan = _swap(
        _fixture("columnstore_scan_no_segment_elimination.xml"),
        'ActualExecutionMode="Batch" ActualElapsedms="3900" ActualCPUms="3880"',
        'ActualExecutionMode="Batch" ActualElapsedms="80" ActualCPUms="80"',
    )

    assert "columnstore_no_segment_elimination" not in _rules(plan)


# --- batch mode ----------------------------------------------------------------


def test_columnstore_scan_in_row_mode_is_named() -> None:
    rules = _rules(_fixture("columnstore_row_mode_scan.xml"))

    finding = rules["columnstore_row_mode"]
    assert finding["node_id"] == 2
    assert finding["family"] == "batch_mode"
    assert finding["evidence"]["actual_mode"] == "Row"
    assert finding["evidence"]["rows"] == 50331648
    assert "node 2" in finding["message"] and "row mode" in finding["message"]
    assert finding["severity"] in {"medium", "high"}


def test_small_columnstore_scan_in_row_mode_is_not_reported() -> None:
    plan = _fixture("columnstore_row_mode_scan.xml")
    for old, new in (
        ('EstimateRows="50331648" EstimatedRowsRead="50331648"', 'EstimateRows="5000" EstimatedRowsRead="5000"'),
        ('TableCardinality="50331648"', 'TableCardinality="5000"'),
        ('ActualRows="50331648" ActualRowsRead="50331648"', 'ActualRows="5000" ActualRowsRead="5000"'),
    ):
        plan = _swap(plan, old, new)

    assert "columnstore_row_mode" not in _rules(plan)


def test_batch_mode_columnstore_scan_is_not_called_row_mode() -> None:
    rules = _rules(_fixture("columnstore_scan_no_segment_elimination.xml"))

    assert not {"columnstore_row_mode", "columnstore_feeds_row_mode", "batch_mode_fallback"} & set(rules)


def test_estimated_plan_reports_a_row_mode_columnstore_scan() -> None:
    # Query Store keeps the estimated plan: EstimatedExecutionMode is all it has.
    finding = _rules(_without_runtime(_fixture("columnstore_row_mode_scan.xml")))["columnstore_row_mode"]

    assert finding["node_id"] == 2
    assert finding["evidence"]["estimated_mode"] == "Row"
    assert finding["evidence"]["actual_mode"] is None


def _row_mode_hash(plan: str, *, estimated: str) -> str:
    """Fixture 1 with its Hash Match (node 1) run in row mode."""

    plan = _swap(
        plan,
        'ActualExecutionMode="Batch" ActualElapsedms="150" ActualCPUms="150"',
        'ActualExecutionMode="Row" ActualElapsedms="4190" ActualCPUms="4040"',
    )
    return _swap(
        plan,
        'EstimateRewinds="0" EstimatedExecutionMode="Batch">\n                  <OutputList>\n                    <ColumnReference Column="Expr1006" />',
        f'EstimateRewinds="0" EstimatedExecutionMode="{estimated}">\n                  <OutputList>\n                    <ColumnReference Column="Expr1006" />',
    )


def test_batch_scan_feeding_a_row_mode_aggregate_is_named() -> None:
    rules = _rules(_row_mode_hash(_fixture("columnstore_scan_no_segment_elimination.xml"), estimated="Row"))

    finding = rules["columnstore_feeds_row_mode"]
    assert finding["node_id"] == 1
    assert finding["evidence"]["scan_node_id"] == 2
    assert finding["evidence"]["rows_from_scan"] == 1204000
    assert "node 1" in finding["message"] and "node 2" in finding["message"]
    assert "batch_mode_fallback" not in rules


def test_few_rows_into_a_row_mode_operator_is_not_reported() -> None:
    plan = _row_mode_hash(_fixture("columnstore_scan_no_segment_elimination.xml"), estimated="Row")
    plan = _swap(plan, 'ActualRows="1204000"', 'ActualRows="900"')
    plan = _swap(plan, 'EstimateRows="1500000"', 'EstimateRows="900"')

    assert "columnstore_feeds_row_mode" not in _rules(plan)


def test_operator_planned_for_batch_mode_that_ran_in_row_mode_is_named() -> None:
    rules = _rules(_row_mode_hash(_fixture("columnstore_scan_no_segment_elimination.xml"), estimated="Batch"))

    finding = rules["batch_mode_fallback"]
    assert finding["node_id"] == 1
    assert finding["evidence"]["estimated_mode"] == "Batch"
    assert finding["evidence"]["actual_mode"] == "Row"
    assert "node 1" in finding["message"]
    # One finding for the operator: the fallback already says it ran in row mode.
    assert "columnstore_feeds_row_mode" not in rules


def test_predicate_not_pushed_into_a_columnstore_scan_is_named() -> None:
    # UPPER(Region) = N'NORTH' runs in a Filter on all 50M rows the scan returns.
    rules = _rules(_fixture("columnstore_filter_not_pushed.xml"))

    finding = rules["columnstore_predicate_not_pushed"]
    assert finding["node_id"] == 1
    assert finding["family"] == "batch_mode"
    assert finding["severity"] == "high"
    assert finding["evidence"]["scan_node_id"] == 2
    assert finding["evidence"]["columns"] == ["Region"]
    assert finding["evidence"]["wrappers"] == ["function:upper"]
    assert finding["evidence"]["rows_in"] == 50331648
    assert finding["evidence"]["rows_out"] == 6291456
    assert "node 1" in finding["message"] and "node 2" in finding["message"] and "Region" in finding["message"]
    assert "computed column" in finding["fix"]


def test_filter_above_a_rowstore_scan_is_not_a_columnstore_finding() -> None:
    plan = _fixture("columnstore_filter_not_pushed.xml").replace('Storage="ColumnStore"', 'Storage="RowStore"')

    assert "columnstore_predicate_not_pushed" not in _rules(plan)


def test_filter_that_keeps_most_rows_is_not_reported() -> None:
    plan = _swap(_fixture("columnstore_filter_not_pushed.xml"), 'ActualRows="6291456"', 'ActualRows="49000000"')

    assert "columnstore_predicate_not_pushed" not in _rules(plan)


# --- partition elimination -----------------------------------------------------


def test_partitioned_scan_that_reads_every_partition_is_a_lead_when_the_column_is_unknown() -> None:
    # CONVERT(date, OrderDate) = @Day: no partition seek key, 48 partitions read.
    finding = _rules(_fixture("partitioned_scan_no_elimination.xml"))["no_partition_elimination"]

    assert finding["node_id"] == 0
    assert finding["family"] == "partitioning"
    assert finding["severity"] == "low"
    assert finding["evidence"]["partitions_accessed"] == 48
    assert finding["evidence"]["candidate_columns"] == ["OrderDate"]
    assert finding["evidence"]["partitioning_column_known"] is False
    assert "node 0" in finding["message"] and "48 partitions" in finding["message"]
    assert "If OrderDate is the partitioning column" in finding["message"]


def test_partitioned_scan_is_graded_by_time_when_the_partitioning_column_is_given() -> None:
    finding = _rules(
        _fixture("partitioned_scan_no_elimination.xml"), partition_columns={"DBO.factorders": "orderdate"}
    )["no_partition_elimination"]

    assert finding["severity"] == "high"
    assert finding["evidence"]["partitioning_column_known"] is True
    assert finding["evidence"]["partitioning_column"] == "OrderDate"
    assert "partitioning column OrderDate" in finding["message"]
    assert "half-open range" in finding["fix"]


def test_wrapped_column_that_is_not_the_partitioning_column_is_not_reported() -> None:
    rules = _rules(_fixture("partitioned_scan_no_elimination.xml"), partition_columns={"dbo.FactOrders": "RegionId"})

    assert "no_partition_elimination" not in rules
    assert "non_sargable_predicate" in rules  # the wrapped column keeps its own finding


def test_partition_seek_key_means_elimination_ran() -> None:
    # The seek starts at RangePartitionNew(@Day, ...): one partition read. The wrapped
    # Amount column is a residual, not a lost elimination.
    rules = _rules(_fixture("partitioned_scan_eliminated.xml"), partition_columns={"dbo.FactOrders": "OrderDate"})

    assert "no_partition_elimination" not in rules


def test_unpartitioned_scan_is_not_reported() -> None:
    plan = _swap(_fixture("partitioned_scan_no_elimination.xml"), 'Partitioned="1"', 'Partitioned="0"')
    plan = re.sub(r"<RunTimePartitionSummary>.*?</RunTimePartitionSummary>", "", plan, flags=re.DOTALL)

    assert "no_partition_elimination" not in _rules(plan)


def test_estimated_plan_reports_a_partitioned_scan_without_a_partition_seek_key() -> None:
    finding = _rules(_without_runtime(_fixture("partitioned_scan_no_elimination.xml")))["no_partition_elimination"]

    assert finding["evidence"]["partitions_accessed"] is None
    assert "every partition" in finding["message"]


# --- digest ----------------------------------------------------------------------


def test_digest_lists_columnstore_scans_with_their_segment_counts() -> None:
    statement = build_plan_digest(_fixture("columnstore_scan_no_segment_elimination.xml"))["statements"][0]

    assert statement["columnstore_scans"] == [
        {
            "node_id": 2,
            "object": "dbo.FactSales.CCI_FactSales AS fs",
            "execution_mode": "Batch",
            "segments_read": 48,
            "segments_skipped": 0,
            "rows": 1204000,
            "rows_read": 50331648,
        }
    ]


def test_digest_has_no_columnstore_section_for_a_rowstore_plan() -> None:
    statement = build_plan_digest(_fixture("partitioned_scan_no_elimination.xml"))["statements"][0]

    assert "columnstore_scans" not in statement


def test_node_detail_shows_segments_and_partitions() -> None:
    scan = describe_plan_node(_fixture("columnstore_scan_no_segment_elimination.xml"), 2)
    all_partitions = describe_plan_node(_fixture("partitioned_scan_no_elimination.xml"), 0)
    one_partition = describe_plan_node(_fixture("partitioned_scan_eliminated.xml"), 0)

    assert scan is not None and all_partitions is not None and one_partition is not None
    assert scan["storage"] == "ColumnStore"
    assert scan["actuals"]["segments_read"] == 48
    assert scan["actuals"]["segments_skipped"] == 0
    assert all_partitions["partitions"] == {"partitioned": True, "accessed": 48, "ranges": ["1-48"], "seek_key": False}
    # Three boundary values in RangePartitionNew: four partitions.
    assert one_partition["partitions"] == {
        "partitioned": True,
        "accessed": 1,
        "ranges": ["39"],
        "seek_key": True,
        "partition_count": 4,
    }


@pytest.mark.parametrize("value", ["true", "1"])
def test_statement_engine_section_reports_batch_mode_on_rowstore(showplan, value: str) -> None:
    plan = showplan.plan(showplan.scan(0), statement_attrs=f'BatchModeOnRowStoreUsed="{value}"')

    statement = build_plan_digest(plan)["statements"][0]

    assert statement["engine"]["batch_mode_on_rowstore"] is True


def test_statement_engine_section_is_absent_when_nothing_applies(showplan) -> None:
    statement = build_plan_digest(showplan.plan(showplan.scan(0)))["statements"][0]

    assert "engine" not in statement


def test_statement_engine_section_reads_plan_variant_hint_and_dispatcher(showplan) -> None:
    dispatcher = (
        '<Dispatcher><ParameterSensitivePredicate LowBoundary="100" HighBoundary="1000000">'
        '<Predicate><ScalarOperator ScalarString="[o].[CustomerID]=[@cust]" /></Predicate>'
        "</ParameterSensitivePredicate></Dispatcher>"
    )
    hinted = showplan.plan(
        showplan.scan(0),
        statement_attrs='QueryStoreStatementHintId="7" QueryStoreStatementHintText="OPTION(MAXDOP 2)" QueryStoreStatementHintSource="User"',
        plan_attrs='QueryVariantID="2" ContainsInlineScalarTsqlUdfs="true"',
    )
    dispatched = showplan.plan(showplan.scan(0)).replace("<QueryPlan >", dispatcher + "<QueryPlan >")

    engine = build_plan_digest(hinted)["statements"][0]["engine"]
    psp = build_plan_digest(dispatched)["statements"][0]["engine"]["parameter_sensitive_plan"]

    assert engine["query_store_hint"] == {"id": 7, "text": "OPTION(MAXDOP 2)", "source": "User"}
    assert engine["contains_inlined_scalar_udfs"] is True
    assert engine["parameter_sensitive_plan"]["role"] == "variant"
    assert engine["parameter_sensitive_plan"]["query_variant_id"] == 2
    assert psp == {
        "role": "dispatcher",
        "predicates": [{"low_boundary": 100.0, "high_boundary": 1000000.0, "predicate": "[o].[CustomerID]=[@cust]"}],
    }


def test_digest_lists_adaptive_joins_with_the_branch_that_ran(showplan) -> None:
    runtime = (
        '<RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="40" ActualExecutions="1" '
        'ActualExecutionMode="Batch" ActualJoinType="NestedLoops" /></RunTimeInformation>'
    )
    join = showplan.relop(
        0, "Adaptive Join", "Inner Join",
        showplan.scan(1, "Orders", runtime=showplan.runtime((0, 40, 1, None, None))),
        showplan.scan(2, "Customers", runtime=showplan.runtime((0, 40, 1, None, None))),
        wrapper="AdaptiveJoin", attrs='IsAdaptive="1" AdaptiveThresholdRows="1500" EstimatedJoinType="Hash Match"',
        runtime=runtime,
    )

    statement = build_plan_digest(showplan.plan(join))["statements"][0]

    assert statement["adaptive_joins"] == [
        {
            "node_id": 0,
            "threshold_rows": 1500.0,
            "estimated_join_type": "Hash Match",
            "actual_join_type": "NestedLoops",
            "build_rows": 40,
        }
    ]
