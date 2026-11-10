"""Plan rules added from the PerformanceMonitor comparison.

Each rule has a plan that triggers it and a near miss that must not: a rule
that fires on a healthy plan sends the DBA after nothing.
"""

from __future__ import annotations

import pytest

from azure_sql_mcp.plan_rules import analyze_plan


def _rules(plan_xml: str, **options) -> dict[str, dict]:
    return {finding["rule"]: finding for finding in analyze_plan(plan_xml, **options)["findings"]}


def _run(rows: int, *, executions: int = 1, elapsed: int | None = None, extra: str = "", mode: str = "Row") -> str:
    """Runtime counters of one serial operator; ``extra`` holds attributes the builder does not know."""

    timing = f' ActualElapsedms="{elapsed}" ActualCPUms="{elapsed}"' if elapsed is not None else ""
    return (
        f'<RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="{rows}" '
        f'ActualExecutions="{executions}" ActualExecutionMode="{mode}"{timing} {extra} /></RunTimeInformation>'
    )


def _compare(showplan, table: str, column: str, value: str, *, op: str = "EQ") -> str:
    """``table.column op value`` where value is a constant, an @variable or ``Other.Column``."""

    if value.startswith("@"):
        operand = f'<Identifier><ColumnReference Column="{value}" /></Identifier>'
    elif "." in value and not value[0].isdigit():
        other_table, other_column = value.split(".")
        operand = f"<Identifier>{showplan.column(other_table, other_column)}</Identifier>"
    else:
        operand = f'<Const ConstValue="{value}" />'
    return (
        f'<ScalarOperator><Compare CompareOp="{op}"><ScalarOperator><Identifier>{showplan.column(table, column)}'
        f"</Identifier></ScalarOperator><ScalarOperator>{operand}</ScalarOperator></Compare></ScalarOperator>"
    )


def _predicate(inner: str, text: str = "predicate") -> str:
    return f'<Predicate><ScalarOperator ScalarString="{text}">{inner.removeprefix("<ScalarOperator>").removesuffix("</ScalarOperator>")}</ScalarOperator></Predicate>'


# --- Filter late in the plan ---------------------------------------------------


def _filtered_join(showplan, *, predicate: str, kept: int = 24_000, startup: bool = False) -> str:
    join = showplan.relop(
        2, "Hash Match", "Inner Join",
        showplan.scan(3, "Orders", est_rows=2_400_000, cost=4.0, runtime=_run(2_400_000, elapsed=3000)),
        showplan.scan(4, "Customers", est_rows=50_000, cost=1.0, runtime=_run(50_000, elapsed=200)),
        wrapper="Hash", est_rows=2_400_000, cost=9.0, runtime=_run(2_400_000, elapsed=9000),
    )
    filter_ = showplan.relop(
        1, "Filter", None, join, est_rows=kept, cost=9.4, body=predicate, runtime=_run(kept, elapsed=9400)
    )
    if startup:
        filter_ = filter_.replace("<Filter>", '<Filter StartupExpression="1">')
    return showplan.plan(filter_, cost=9.4, plan_children='<QueryTimeStats ElapsedTime="9500" CpuTime="9400" />')


def _date_gap_predicate(showplan) -> str:
    return (
        '<Predicate><ScalarOperator ScalarString="datediff(day,[o].[OrderDate],[c].[SignupDate])&gt;(30)">'
        '<Compare CompareOp="GT"><ScalarOperator><Intrinsic FunctionName="datediff">'
        f'<ScalarOperator><Identifier>{showplan.column("Orders", "OrderDate")}</Identifier></ScalarOperator>'
        f'<ScalarOperator><Identifier>{showplan.column("Customers", "SignupDate")}</Identifier></ScalarOperator>'
        '</Intrinsic></ScalarOperator><ScalarOperator><Const ConstValue="(30)" /></ScalarOperator></Compare>'
        "</ScalarOperator></Predicate>"
    )


def test_filter_that_discards_most_of_a_join_is_named(showplan) -> None:
    # 2.4M joined rows, 24,000 kept: 95% of the elapsed time is spent on rows that are thrown away.
    finding = _rules(_filtered_join(showplan, predicate=_date_gap_predicate(showplan)))["late_filter"]

    assert finding["node_id"] == 1
    assert finding["severity"] == "high"
    assert finding["evidence"]["rows_in"] == 2_400_000
    assert finding["evidence"]["rows_out"] == 24_000
    assert finding["evidence"]["subtree_root_node_id"] == 2
    assert "node 1" in finding["message"] and "node 2" in finding["message"]
    assert "datediff" in finding["message"]
    assert "bare columns of one table" in finding["fix"]


def test_having_filter_on_an_aggregate_is_not_late(showplan) -> None:
    # HAVING COUNT(*) > 5 compares a computed column: it cannot move below the aggregate.
    having = (
        '<Predicate><ScalarOperator ScalarString="[Expr1004]&gt;(5)"><Compare CompareOp="GT"><ScalarOperator>'
        '<Identifier><ColumnReference Column="Expr1004" /></Identifier></ScalarOperator>'
        '<ScalarOperator><Const ConstValue="(5)" /></ScalarOperator></Compare></ScalarOperator></Predicate>'
    )

    assert "late_filter" not in _rules(_filtered_join(showplan, predicate=having))


def test_filter_that_keeps_most_rows_is_not_late(showplan) -> None:
    plan = _filtered_join(showplan, predicate=_date_gap_predicate(showplan), kept=1_500_000)

    assert "late_filter" not in _rules(plan)


def test_startup_filter_is_not_late(showplan) -> None:
    plan = _filtered_join(showplan, predicate=_date_gap_predicate(showplan), startup=True)

    assert "late_filter" not in _rules(plan)


# --- lazy spool ------------------------------------------------------------------


def _spooled_loop(showplan, *, rebinds: int, rewinds: int, measured: bool = True) -> str:
    counters = f'ActualRebinds="{rebinds}" ActualRewinds="{rewinds}"'
    spool = showplan.relop(
        2, "Table Spool", "Lazy Spool", showplan.scan(3, "OrderLines", cost=0.4, runtime=_run(90_000, executions=rebinds, elapsed=1800) if measured else ""),
        wrapper="Spool", cost=0.7,
        runtime=_run(95_000, executions=rebinds + rewinds, elapsed=4200, extra=counters) if measured else "",
    )
    loop = showplan.relop(
        0, "Nested Loops", "Inner Join",
        showplan.scan(1, "Orders", cost=0.2, runtime=_run(rebinds + rewinds, elapsed=100) if measured else ""),
        spool, wrapper="NestedLoops", cost=1.0, runtime=_run(95_000, elapsed=4500) if measured else "",
    )
    return showplan.plan(loop, plan_children='<QueryTimeStats ElapsedTime="4500" CpuTime="4400" />' if measured else "")


def test_lazy_spool_that_is_reloaded_on_most_executions_is_named(showplan) -> None:
    finding = _rules(_spooled_loop(showplan, rebinds=5000, rewinds=120))["lazy_spool_ineffective"]

    assert finding["node_id"] == 2
    assert finding["family"] == "joins"
    assert finding["evidence"]["rebinds"] == 5000
    assert finding["evidence"]["rewinds"] == 120
    assert finding["severity"] == "high"  # 2,400 of 4,500 ms are the spool's own
    assert "node 2" in finding["message"] and "5,000" in finding["message"]
    assert "node 1" in finding["fix"]


@pytest.mark.parametrize(("rebinds", "rewinds"), [(200, 4900), (60, 10)])
def test_lazy_spool_that_is_reused_or_rarely_run_is_healthy(showplan, rebinds: int, rewinds: int) -> None:
    assert "lazy_spool_ineffective" not in _rules(_spooled_loop(showplan, rebinds=rebinds, rewinds=rewinds))


def test_lazy_spool_needs_measured_rebinds(showplan) -> None:
    assert "lazy_spool_ineffective" not in _rules(_spooled_loop(showplan, rebinds=5000, rewinds=0, measured=False))


# --- OR in a join ----------------------------------------------------------------


def _or_join(showplan, *, executions: int) -> str:
    intervals = showplan.relop(
        3, "Merge Interval", None,
        showplan.relop(
            4, "Concatenation", None,
            showplan.relop(5, "Constant Scan", None, cost=0.0001, runtime=_run(executions, executions=executions, elapsed=10)),
            showplan.relop(6, "Constant Scan", None, cost=0.0001, runtime=_run(executions, executions=executions, elapsed=10)),
            cost=0.001, runtime=_run(2 * executions, executions=executions, elapsed=40),
        ),
        cost=0.01, runtime=_run(2 * executions, executions=executions, elapsed=900),
    )
    seek = showplan.scan(
        7, "Orders", physical="Index Seek", index="IX_Orders_Customer", cost=0.5,
        runtime=_run(60_000, executions=2 * executions, elapsed=2500),
    )
    inner = showplan.relop(
        2, "Nested Loops", "Inner Join", intervals, seek, wrapper="NestedLoops", cost=0.6,
        runtime=_run(60_000, executions=executions, elapsed=3600),
    )
    outer = showplan.scan(1, "Customers", cost=0.3, runtime=_run(executions, elapsed=100))
    loop = showplan.relop(0, "Nested Loops", "Inner Join", outer, inner, wrapper="NestedLoops", cost=1.0, runtime=_run(60_000, elapsed=4000))
    return showplan.plan(loop, plan_children='<QueryTimeStats ElapsedTime="4000" CpuTime="3900" />')


def test_or_in_a_join_expanded_per_outer_row_is_named(showplan) -> None:
    finding = _rules(_or_join(showplan, executions=20_000))["join_or_predicate"]

    assert finding["node_id"] == 3
    assert finding["evidence"]["branches"] == 2
    assert finding["evidence"]["executions"] == 20_000
    assert finding["evidence"]["table"] == "dbo.Orders"
    assert finding["evidence"]["rewrite_pattern"] == "or_in_join_predicate"
    assert finding["severity"] == "high"
    assert "node 3" in finding["message"] and "dbo.Orders" in finding["message"]
    assert "UNION ALL" in finding["fix"]


def test_in_list_expanded_once_is_healthy(showplan) -> None:
    # WHERE CustomerID IN (@a, @b): the same operators, run once.
    assert "join_or_predicate" not in _rules(_or_join(showplan, executions=1))


# --- Top above a scan ----------------------------------------------------------------


def _top_over_scan(showplan, *, executions: int, predicate: bool = True, inner: bool = True) -> str:
    body = _predicate(_compare(showplan, "Orders", "CustomerID", "Customers.CustomerID")) if predicate else ""
    scan = showplan.scan(
        3, "Orders", est_rows=1, cost=0.6, body=body,
        runtime=_run(executions, executions=executions, elapsed=7000, extra='ActualRowsRead="40000000"'),
    )
    top = showplan.relop(2, "Top", None, scan, cost=0.61, runtime=_run(executions, executions=executions, elapsed=7100))
    if not inner:
        return showplan.plan(top, plan_children='<QueryTimeStats ElapsedTime="7200" CpuTime="7100" />')
    outer = showplan.scan(1, "Customers", cost=0.3, runtime=_run(executions, elapsed=50))
    loop = showplan.relop(0, "Nested Loops", "Left Outer Join", outer, top, wrapper="NestedLoops", cost=1.0, runtime=_run(executions, elapsed=7200))
    return showplan.plan(loop, plan_children='<QueryTimeStats ElapsedTime="7200" CpuTime="7100" />')


def test_top_above_a_scan_on_the_inner_side_of_a_loop_is_named(showplan) -> None:
    # OUTER APPLY (SELECT TOP (1) ... ORDER BY ...) with no index: one scan per customer.
    finding = _rules(_top_over_scan(showplan, executions=5000))["top_above_scan"]

    assert finding["node_id"] == 2
    assert finding["severity"] == "high"
    assert finding["evidence"]["scan_node_id"] == 3
    assert finding["evidence"]["loop_node_id"] == 0
    assert finding["evidence"]["executions"] == 5000
    assert finding["evidence"]["filter_columns"] == ["CustomerID"]
    assert "node 2" in finding["message"] and "node 3" in finding["message"] and "5,000 times" in finding["message"]
    assert "dbo.Orders" in finding["fix"] and "CustomerID" in finding["fix"]


def test_top_above_a_scan_run_once_is_not_reported(showplan) -> None:
    assert "top_above_scan" not in _rules(_top_over_scan(showplan, executions=1, inner=False))


def test_top_above_an_unfiltered_scan_is_not_reported(showplan) -> None:
    assert "top_above_scan" not in _rules(_top_over_scan(showplan, executions=5000, predicate=False))


# --- statement text ------------------------------------------------------------------


@pytest.mark.parametrize("hint", ["OPTION (OPTIMIZE FOR UNKNOWN)", "OPTION (OPTIMIZE FOR (@id UNKNOWN))"])
def test_optimize_for_unknown_in_the_text_is_reported(showplan, hint: str) -> None:
    plan = showplan.plan(showplan.scan(0), text=f"SELECT 1 FROM dbo.T WHERE Id = @id {hint}")

    finding = _rules(plan)["optimize_for_unknown"]

    assert finding["severity"] == "info"
    assert "average density" in finding["message"]


def test_optimize_for_a_value_is_not_optimize_for_unknown(showplan) -> None:
    plan = showplan.plan(showplan.scan(0), text="SELECT 1 FROM dbo.T WHERE Id = @id OPTION (OPTIMIZE FOR (@id = 5))")

    assert "optimize_for_unknown" not in _rules(plan)


def test_statement_text_cut_by_the_showplan_is_reported(showplan) -> None:
    long_text = "SELECT 1 FROM dbo.T WHERE Id IN (" + ", ".join(str(number) for number in range(1000)) + ")"
    cut = _rules(showplan.plan(showplan.scan(0), text=long_text[:3995]))
    whole = _rules(showplan.plan(showplan.scan(0), text=long_text[:500]))

    assert cut["truncated_statement_text"]["evidence"]["statement_text_chars"] == 3995
    assert "sys.query_store_query_text" in cut["truncated_statement_text"]["fix"]
    assert "truncated_statement_text" not in whole


# --- cursors -------------------------------------------------------------------------


def test_dynamic_cursor_without_local_is_reported_once(showplan) -> None:
    cursor = showplan.cursor(("PopulateQuery", showplan.scan(0, "Orders")), ("FetchQuery", showplan.scan(0, "Orders")))

    result = analyze_plan(showplan.batch(cursor))
    found = [finding["rule"] for finding in result["findings"] if finding["family"] == "cursor"]

    assert sorted(found) == ["cursor_without_local", "dynamic_cursor"]  # once, not once per operation
    by_rule = {finding["rule"]: finding for finding in result["findings"]}
    assert by_rule["dynamic_cursor"]["severity"] == "low"
    assert "FAST_FORWARD" in by_rule["dynamic_cursor"]["fix"]
    assert by_rule["cursor_without_local"]["severity"] == "info"
    assert "CURSOR_DEFAULT" in by_rule["cursor_without_local"]["message"]


def test_local_fast_forward_cursor_is_not_reported(showplan) -> None:
    cursor = showplan.cursor(("FetchQuery", showplan.scan(0, "Orders")), cursor_type="FastForward")
    cursor = cursor.replace("DECLARE c CURSOR FOR", "DECLARE c CURSOR LOCAL FAST_FORWARD FOR")

    families = {finding["family"] for finding in analyze_plan(showplan.batch(cursor))["findings"]}

    assert "cursor" not in families


# --- bare scan -------------------------------------------------------------------------


def _whole_table_scan(showplan, *, columns: int = 2, rows: int = 5_000_000, attrs: str = "", body: str = "", runtime: str = "") -> str:
    outputs = "".join(showplan.column("Orders", f"C{number}") for number in range(columns))
    return showplan.plan(
        showplan.scan(
            0, "Orders", est_rows=rows, cost=40.0, attrs=f'TableCardinality="{rows}" {attrs}', outputs=outputs,
            body=body, runtime=runtime,
        ),
        cost=40.0,
    )


def test_whole_table_scan_for_two_columns_suggests_a_narrow_index(showplan) -> None:
    finding = _rules(_whole_table_scan(showplan))["bare_scan"]

    assert finding["node_id"] == 0
    assert finding["evidence"]["suggestion"] == "narrow_nonclustered_index"
    assert finding["evidence"]["output_columns"] == ["C0", "C1"]
    assert "node 0" in finding["message"] and "5,000,000 rows" in finding["message"]
    assert "nonclustered index" in finding["message"]


def test_whole_table_scan_for_many_columns_suggests_columnstore(showplan) -> None:
    finding = _rules(_whole_table_scan(showplan, columns=6))["bare_scan"]

    assert finding["evidence"]["suggestion"] == "nonclustered_columnstore_index"
    assert "columnstore" in finding["message"]


@pytest.mark.parametrize(
    "variant",
    ["small_table", "has_predicate", "row_goal", "stopped_early"],
)
def test_scans_that_do_not_read_the_table_for_its_own_sake_are_not_bare(showplan, variant: str) -> None:
    plan = {
        "small_table": lambda: _whole_table_scan(showplan, rows=20_000),
        "has_predicate": lambda: _whole_table_scan(showplan, body=_predicate(_compare(showplan, "Orders", "Status", "(1)"))),
        "row_goal": lambda: _whole_table_scan(showplan, attrs='EstimateRowsWithoutRowGoal="5000000"'),
        "stopped_early": lambda: _whole_table_scan(showplan, runtime=_run(10)),
    }[variant]()

    assert "bare_scan" not in _rules(plan)


# --- memory grants -------------------------------------------------------------------


def _granted(showplan, granted_kb: int, used_kb: int, *, attrs: str = "", warnings: str = "") -> str:
    sort = showplan.relop(
        0, "Sort", None,
        showplan.scan(1, "Orders", est_rows=8_000_000, cost=5.0, attrs='AvgRowSize="400"', runtime=_run(8_000_000, elapsed=2000)),
        cost=20.0, est_rows=8_000_000, runtime=_run(8_000_000, elapsed=9000),
    )
    return showplan.plan(
        sort, cost=20.0,
        plan_children=f'{warnings}<MemoryGrantInfo GrantedMemory="{granted_kb}" MaxUsedMemory="{used_kb}" {attrs} />',
    )


def test_large_memory_grant_names_its_consumers(showplan) -> None:
    finding = _rules(_granted(showplan, 3 * 1024 * 1024, 2_600_000))["large_memory_grant"]

    assert finding["severity"] == "medium"
    assert finding["node_id"] == 0
    assert finding["evidence"]["consumers"][0]["node_id"] == 0
    assert "3,072 MB" in finding["message"] and "Sort (node 0)" in finding["message"]
    assert "RESOURCE_SEMAPHORE" in finding["fix"]
    assert finding["benefit_share"] is None  # memory is the cost, not this operator's time


def test_four_gigabyte_grant_is_high(showplan) -> None:
    assert _rules(_granted(showplan, 5 * 1024 * 1024, 4_800_000))["large_memory_grant"]["severity"] == "high"


def test_grant_that_is_most_of_what_a_query_may_get_is_large_on_a_small_database(showplan) -> None:
    rules = _rules(_granted(showplan, 300_000, 250_000, attrs='MaxQueryMemory="500000"'))

    assert rules["large_memory_grant"]["evidence"]["share_of_max_query_memory"] == 0.6


def test_moderate_used_grant_is_healthy(showplan) -> None:
    rules = _rules(_granted(showplan, 300_000, 250_000, attrs='MaxQueryMemory="12000000"'))

    assert not {"large_memory_grant", "excessive_memory_grant"} & set(rules)


def test_unused_grant_is_graded_against_the_query_memory_limit(showplan) -> None:
    # 200 MB granted, 5 MB used. A third of what one query may get on a small database.
    small_database = _rules(_granted(showplan, 204_800, 5_120, attrs='MaxQueryMemory="614400" IsMemoryGrantFeedbackAdjusted="Yes: Adjusting"'))
    large_database = _rules(_granted(showplan, 204_800, 5_120, attrs='MaxQueryMemory="12000000"'))

    finding = small_database["excessive_memory_grant"]
    assert finding["severity"] == "medium"
    assert finding["evidence"]["grant_feedback"] == "Yes: Adjusting"
    assert "already adjusting" in finding["message"]
    assert "large_memory_grant" not in small_database
    assert "excessive_memory_grant" not in large_database


def test_excessive_grant_warning_needs_a_grant_worth_fixing(showplan) -> None:
    def warning(granted_kb: int) -> str:
        return (
            f'<Warnings><MemoryGrantWarning GrantWarningKind="Excessive Grant" RequestedMemory="{granted_kb}" '
            f'GrantedMemory="{granted_kb}" MaxUsedMemory="64" /></Warnings>'
        )

    small = _rules(_granted(showplan, 20_480, 64, warnings=warning(20_480)))
    large = _rules(_granted(showplan, 2_097_152, 64, warnings=warning(2_097_152)))

    assert "memory_grant_warning" not in small
    assert large["memory_grant_warning"]["severity"] == "medium"


# --- spills ---------------------------------------------------------------------------


def _sorted_scan(showplan, *, plan_warnings: str = "", sort_warnings: str = "") -> str:
    sort = showplan.relop(
        0, "Sort", None, showplan.scan(1, "Orders", runtime=_run(1000)), warnings=sort_warnings, runtime=_run(1000)
    )
    return showplan.plan(sort, plan_children=plan_warnings)


def test_spill_recorded_without_its_operator_is_a_finding(showplan) -> None:
    # A last actual plan (lightweight profiling) says only that a spill happened.
    plan = _sorted_scan(showplan, plan_warnings='<Warnings><SpillOccurred Detail="0" /></Warnings>')

    finding = _rules(plan)["tempdb_spill"]

    assert finding["node_id"] is None
    assert finding["severity"] == "medium"
    assert finding["evidence"]["operator_known"] is False
    assert finding["evidence"]["candidate_node_ids"] == [0]
    assert "Sort (node 0)" in finding["message"]
    assert "analyze=true" in finding["fix"]


def test_no_spill_warning_no_spill_finding(showplan) -> None:
    assert "tempdb_spill" not in _rules(_sorted_scan(showplan))


def test_spill_with_its_operator_is_reported_once(showplan) -> None:
    plan = _sorted_scan(
        showplan,
        plan_warnings='<Warnings><SpillOccurred Detail="1" /></Warnings>',
        sort_warnings='<Warnings><SpillToTempDb SpillLevel="1" SpilledThreadCount="1" /></Warnings>',
    )

    found = [finding for finding in analyze_plan(plan)["findings"] if finding["rule"] == "tempdb_spill"]

    assert [finding["node_id"] for finding in found] == [0]


# --- estimates ---------------------------------------------------------------------------


def test_failed_row_goal_is_named(showplan) -> None:
    # TOP (10) with a filter: the optimizer expected to stop after a few rows and read 900,000.
    scan = showplan.scan(
        1, "Orders", est_rows=10, attrs='EstimateRowsWithoutRowGoal="1000000"', runtime=_run(900_000, elapsed=4000)
    )
    plan = showplan.plan(showplan.relop(0, "Top", None, scan, est_rows=10, runtime=_run(10, elapsed=4010)))

    rules = _rules(plan)
    finding = rules["row_goal_misestimate"]

    assert finding["node_id"] == 1
    assert finding["family"] == "row_goal"
    assert finding["evidence"]["estimated_rows_without_row_goal"] == 1_000_000
    assert "DISABLE_OPTIMIZER_ROWGOAL" in finding["fix"]
    assert "row_estimate_gap" not in rules


def test_row_goal_that_held_is_not_reported(showplan) -> None:
    scan = showplan.scan(1, "Orders", est_rows=10, attrs='EstimateRowsWithoutRowGoal="1000000"', runtime=_run(10, elapsed=1))
    plan = showplan.plan(showplan.relop(0, "Top", None, scan, est_rows=10, runtime=_run(10, elapsed=1)))

    assert "row_goal_misestimate" not in _rules(plan)


def _dated_seek(showplan, *, parameters: str) -> str:
    seek = showplan.scan(
        0, "Orders", physical="Index Seek", index="IX_Orders_OrderDate", est_rows=3000,
        body="<SeekPredicates>" + showplan.seek_keys("Orders", "OrderDate", "@since", scan_type="GE") + "</SeekPredicates>",
        runtime=_run(900_000, elapsed=800),
    )
    return showplan.plan(seek, plan_children=parameters)


def test_misestimate_on_a_local_variable_names_the_variable(showplan) -> None:
    # DECLARE @since date = ...: not in the parameter list, so the optimizer never saw its value.
    rules = _rules(_dated_seek(showplan, parameters=""))
    finding = rules["local_variable_estimate"]

    assert finding["node_id"] == 0
    assert finding["evidence"]["local_variables"] == ["@since"]
    assert finding["evidence"]["rewrite_pattern"] == "local_variable_predicate"
    assert "@since" in finding["message"] and "node 0" in finding["message"]
    assert "sp_executesql" in finding["fix"]
    assert "row_estimate_gap" not in rules


def test_misestimate_on_a_sniffed_parameter_is_a_plain_gap(showplan) -> None:
    sniffed = (
        '<ParameterList><ColumnReference Column="@since" ParameterCompiledValue="\'2026-11-01\'" '
        'ParameterRuntimeValue="\'2020-01-01\'" /></ParameterList>'
    )

    rules = _rules(_dated_seek(showplan, parameters=sniffed))

    assert "local_variable_estimate" not in rules
    assert rules["row_estimate_gap"]["node_id"] == 0


def _costly_scan(showplan, *, returned: int, scan_elapsed: int) -> str:
    scan = showplan.scan(
        1, "Orders", est_rows=600_000, cost=0.9, attrs='TableCardinality="2000000"',
        body=_predicate(_compare(showplan, "Orders", "Status", "(3)")), runtime=_run(returned, elapsed=scan_elapsed),
    )
    plan = showplan.relop(0, "Sort", None, scan, est_rows=600_000, cost=1.0, runtime=_run(returned, elapsed=1000))
    return showplan.plan(plan, plan_children='<QueryTimeStats ElapsedTime="1000" CpuTime="990" />')


def test_scan_chosen_for_an_overestimate_is_named(showplan) -> None:
    # Costed for 600,000 rows, returned 900 (0.05% of the table) and took 90% of the time.
    rules = _rules(_costly_scan(showplan, returned=900, scan_elapsed=900))
    finding = rules["scan_cardinality_misestimate"]

    assert finding["node_id"] == 1
    assert finding["severity"] == "high"
    assert finding["evidence"]["selectivity"] == 0.00045
    assert "check_statistics_health" in finding["fix"]
    assert not [f for f in analyze_plan(_costly_scan(showplan, returned=900, scan_elapsed=900))["findings"] if f["rule"] == "row_estimate_gap" and f["node_id"] == 1]


def test_overestimated_scan_that_is_cheap_stays_a_plain_gap(showplan) -> None:
    rules = _rules(_costly_scan(showplan, returned=900, scan_elapsed=100))

    assert "scan_cardinality_misestimate" not in rules
    assert rules["row_estimate_gap"]["node_id"] == 1


# --- missing index requests ---------------------------------------------------------------


def _missing_index(impact: float, *, equality: int = 1, include: int = 0, table: str = "Orders") -> str:
    def group(usage: str, count: int) -> str:
        columns = "".join(f'<Column Name="[{usage[:2]}{number}]" ColumnId="{number + 1}" />' for number in range(count))
        return f'<ColumnGroup Usage="{usage}">{columns}</ColumnGroup>' if count else ""

    return (
        f'<MissingIndexGroup Impact="{impact}"><MissingIndex Database="[db]" Schema="[dbo]" Table="[{table}]">'
        f'{group("EQUALITY", equality)}{group("INCLUDE", include)}</MissingIndex></MissingIndexGroup>'
    )


def _hints(showplan, *groups: str) -> list[dict]:
    plan = showplan.plan(showplan.scan(0), plan_children=f"<MissingIndexes>{''.join(groups)}</MissingIndexes>")
    return [finding for finding in analyze_plan(plan)["findings"] if finding["rule"] == "missing_index_hint"]


def test_missing_index_request_with_little_impact_is_info(showplan) -> None:
    low, solid = _hints(showplan, _missing_index(18.0))[0], _hints(showplan, _missing_index(72.0))[0]

    assert low["severity"] == "info"
    assert solid["severity"] == "medium"
    assert solid["evidence"]["quality_flags"] == []


def test_missing_index_request_quality_is_checked(showplan) -> None:
    wide = _hints(showplan, _missing_index(80.0, equality=5, include=6))[0]
    repeated = _hints(showplan, _missing_index(80.0), _missing_index(60.0, equality=2))

    assert wide["evidence"]["wide_include_list"] is True
    assert len(wide["evidence"]["quality_flags"]) == 2
    assert "5 key columns" in wide["message"] and "6 include columns" in wide["message"]
    assert all("2 requests for this table" in finding["message"] for finding in repeated)


def test_requests_for_different_tables_are_not_called_repeated(showplan) -> None:
    found = _hints(showplan, _missing_index(80.0), _missing_index(60.0, table="Customers"))

    assert all(finding["evidence"]["quality_flags"] == [] for finding in found)


# --- table-valued functions -------------------------------------------------------------------


def _tvf(showplan, obj: str, rows: float = 100) -> str:
    return showplan.plan(
        showplan.relop(0, "Table-valued function", None, wrapper="TableValuedFunction", est_rows=rows, body=obj)
    )


@pytest.mark.parametrize("name", ["STRING_SPLIT", "OPENJSON_DEFAULT", "GENERATE_SERIES"])
def test_engine_function_is_not_a_multi_statement_tvf(showplan, name: str) -> None:
    # The plan names an engine function without a schema; nothing about it can be inlined.
    assert "multi_statement_tvf" not in _rules(_tvf(showplan, f'<Object Table="[{name}]" />', rows=50))


def test_user_tvf_with_the_fixed_guess_is_medium_and_with_a_real_count_low(showplan) -> None:
    obj = '<Object Database="[db]" Schema="[dbo]" Table="[fn_OpenOrders]" />'
    guessed = _rules(_tvf(showplan, obj, rows=100))["multi_statement_tvf"]
    counted = _rules(_tvf(showplan, obj, rows=3120))["multi_statement_tvf"]

    assert guessed["severity"] == "medium"
    assert guessed["evidence"]["function"] == "dbo.fn_OpenOrders"
    assert "dbo.fn_OpenOrders" in guessed["message"] and "node 0" in guessed["message"]
    assert counted["severity"] == "low"
    assert "interleaved execution" in counted["message"]


# --- adaptive join ----------------------------------------------------------------------------


def _adaptive(showplan, *, ran: str) -> str:
    join = showplan.relop(
        0, "Adaptive Join", "Inner Join",
        showplan.scan(1, "Orders", runtime=_run(40)), showplan.scan(2, "Customers", runtime=_run(40)),
        wrapper="AdaptiveJoin",
        attrs='IsAdaptive="1" AdaptiveThresholdRows="1500" EstimatedJoinType="Hash Match"',
        runtime=_run(40, extra=f'ActualJoinType="{ran}"'),
    )
    return showplan.plan(join)


def test_adaptive_join_that_switched_strategy_is_information(showplan) -> None:
    finding = _rules(_adaptive(showplan, ran="NestedLoops"))["adaptive_join_switched"]

    assert finding["severity"] == "info"
    assert finding["evidence"]["threshold_rows"] == 1500
    assert finding["evidence"]["build_rows"] == 40


def test_adaptive_join_that_ran_as_planned_is_not_reported(showplan) -> None:
    assert "adaptive_join_switched" not in _rules(_adaptive(showplan, ran="HashMatch"))
