"""False plan findings and missing reads from the first live comparison.

Each fixture is the plan shape an agent saw on the live database. A finding
that is wrong sends the DBA after nothing; a missing read count sends the agent
back to raw DMVs.
"""

from __future__ import annotations

from typing import Any

from azure_sql_mcp.plan_digest import build_plan_digest
from azure_sql_mcp.plan_rules import analyze_plan
from azure_sql_mcp.plans import PlansService
from azure_sql_mcp.safe_sql import SafeSqlValidator

NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"


def _column(table: str, column: str, alias: str) -> str:
    return (
        f'<ColumnReference Database="[appdb]" Schema="[dbo]" Table="[{table}]" '
        f'Alias="[{alias}]" Column="{column}" />'
    )


def _counters(rows: int, *, threads: int = 0, reads: int | None = None, lob: int = 0) -> str:
    """Runtime counters: serial (thread 0 only) or a coordinator plus ``threads`` workers."""

    def one(thread: int, thread_rows: int, thread_reads: int | None) -> str:
        io = (
            f' ActualLogicalReads="{thread_reads}" ActualPhysicalReads="0" '
            f'ActualLobLogicalReads="{lob if thread <= 1 else 0}"'
            if thread_reads is not None
            else ""
        )
        return (
            f'<RunTimeCountersPerThread Thread="{thread}" ActualRows="{thread_rows}" '
            f'ActualExecutions="{1 if thread or not threads else 0}" ActualElapsedms="40" '
            f'ActualCPUms="30" ActualExecutionMode="Row"{io} />'
        )

    if not threads:
        return f"<RunTimeInformation>{one(0, rows, reads)}</RunTimeInformation>"
    workers = "".join(
        one(thread, rows // threads, None if reads is None else reads // threads)
        for thread in range(1, threads + 1)
    )
    return f"<RunTimeInformation>{one(0, 0, None if reads is None else 0)}{workers}</RunTimeInformation>"


def _access(
    node_id: int,
    table: str,
    alias: str,
    index: str,
    *,
    seek: str = "",
    predicate: str = "",
    runtime: str = "",
    rows: float = 100,
    cost: float = 1.0,
    kind: str = "NonClustered",
) -> str:
    op = "Index Seek" if seek else "Index Scan"
    if kind == "Clustered":
        op = f"Clustered {op}"
    seek_xml = (
        f'<SeekPredicates><SeekPredicateNew><SeekKeys><Prefix ScanType="EQ">{seek}'
        "</Prefix></SeekKeys></SeekPredicateNew></SeekPredicates>"
        if seek
        else ""
    )
    return (
        f'<RelOp NodeId="{node_id}" PhysicalOp="{op}" LogicalOp="{op}" EstimateRows="{rows}" '
        f'EstimatedTotalSubtreeCost="{cost}" TableCardinality="7600000" AvgRowSize="11" '
        f'EstimateRebinds="0" EstimateRewinds="0">{runtime}<IndexScan Ordered="1">'
        f'<Object Database="[appdb]" Schema="[dbo]" Table="[{table}]" Index="[{index}]" '
        f'Alias="[{alias}]" IndexKind="{kind}" />{seek_xml}{predicate}</IndexScan></RelOp>'
    )


def _eq_seek(table: str, column: str, alias: str, parameter: str) -> str:
    return (
        f"<RangeColumns>{_column(table, column, alias)}</RangeColumns><RangeExpressions>"
        f'<ScalarOperator ScalarString="[{parameter}]"><Identifier>'
        f'<ColumnReference Column="{parameter}" /></Identifier></ScalarOperator></RangeExpressions>'
    )


def _plan(
    relop: str,
    *,
    cost: float = 1.0,
    level: str = "FULL",
    parameters: str = "",
    dop: int = 1,
    text: str = "SELECT 1",
) -> str:
    parameter_list = f"<ParameterList>{parameters}</ParameterList>" if parameters else ""
    return (
        f'<ShowPlanXML xmlns="{NS}" Version="1.564"><BatchSequence><Batch><Statements>'
        f'<StmtSimple StatementText="{text}" StatementType="SELECT" StatementId="1" '
        f'StatementSubTreeCost="{cost}" StatementOptmLevel="{level}" QueryHash="0xA1" '
        f'QueryPlanHash="0xB2"><QueryPlan DegreeOfParallelism="{dop}">'
        f'<QueryTimeStats ElapsedTime="120" CpuTime="300" />{relop}{parameter_list}'
        "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"
    )


def _findings(plan_xml: str, **options: Any) -> list[dict[str, Any]]:
    return analyze_plan(plan_xml, **options)["findings"]


# --- PROBE is a bitmap filter, not a function wrapped around the column (task T3) ---


def _bitmap_join(threads: int = 4) -> str:
    probe = (
        '<Predicate><ScalarOperator ScalarString="PROBE([Opt_Bitmap1005],[appdb].[dbo].[Badges].'
        '[UserId] as [b].[UserId])"><Intrinsic FunctionName="PROBE"><ScalarOperator><Identifier>'
        '<ColumnReference Column="Opt_Bitmap1005" /></Identifier></ScalarOperator><ScalarOperator>'
        f"<Identifier>{_column('Badges', 'UserId', 'b')}</Identifier></ScalarOperator></Intrinsic>"
        "</ScalarOperator></Predicate>"
    )
    badges = _access(
        3, "Badges", "b", "IX_Badges_UserId", predicate=probe, rows=7_600_000, cost=18.0,
        runtime=_counters(184_540, threads=threads, reads=21_480),
    )
    users = _access(
        2, "Users", "u", "PK_Users", rows=2_300_000, cost=6.0, kind="Clustered",
        runtime=_counters(2_300_000, threads=threads, reads=44_000),
    )
    join = (
        '<RelOp NodeId="1" PhysicalOp="Hash Match" LogicalOp="Inner Join" EstimateRows="7600000" '
        f'EstimatedTotalSubtreeCost="40" Parallel="1">{_counters(184_540, threads=threads)}'
        f"<Hash>{users}{badges}</Hash></RelOp>"
    )
    return _plan(join, cost=40.0, dop=4)


def test_bitmap_probe_in_a_parallel_hash_join_is_not_a_non_sargable_predicate() -> None:
    findings = _findings(_bitmap_join())

    assert [item for item in findings if item["rule"] == "non_sargable_predicate"] == []
    assert not any("PROBE" in str(item.get("message")) for item in findings)


# --- A deliberate "col + 0" steers the plan and is graded low (task T2) ---


def _identity_compare(constant: str = "(0)", operation: str = "ADD") -> str:
    return (
        '<Predicate><ScalarOperator ScalarString="[c].[UserId]+(0)=[@UserId]"><Compare CompareOp="EQ">'
        f'<ScalarOperator><Arithmetic Operation="{operation}"><ScalarOperator><Identifier>'
        f"{_column('Comments', 'UserId', 'c')}</Identifier></ScalarOperator><ScalarOperator>"
        f'<Const ConstValue="{constant}" /></ScalarOperator></Arithmetic></ScalarOperator>'
        '<ScalarOperator><Identifier><ColumnReference Column="@UserId" /></Identifier>'
        "</ScalarOperator></Compare></ScalarOperator></Predicate>"
    )


def test_identity_expression_beside_another_seek_is_low_and_says_what_it_disables() -> None:
    seek = _access(
        0, "Comments", "c", "IX_Comments_PostId",
        seek=_eq_seek("Comments", "PostId", "c", "@PostId"), predicate=_identity_compare(),
        runtime=_counters(12), rows=12,
    )

    finding = next(
        item for item in _findings(_plan(seek)) if item["rule"] == "non_sargable_predicate"
    )

    assert finding["severity"] == "low"
    assert finding["evidence"]["wrapper"] == "arithmetic_identity"
    assert finding["evidence"]["deliberate_steering"] is True
    assert "an identity expression" in finding["message"]
    assert "seeks on PostId" in finding["message"]
    assert "disables every index seek and the histogram estimate on UserId" in finding["fix"]


def test_identity_expression_with_no_other_seek_keeps_its_cost_based_grade() -> None:
    # Nothing else limits the rows: the expression makes the whole table a scan.
    scan = _access(
        0, "Comments", "c", "IX_Comments_UserId", predicate=_identity_compare(),
        runtime=_counters(12), rows=22_000_000, cost=60.0,
    )

    finding = next(
        item for item in _findings(_plan(scan, cost=60.0)) if item["rule"] == "non_sargable_predicate"
    )

    assert finding["severity"] == "high"
    assert finding["evidence"]["deliberate_steering"] is False


def test_real_arithmetic_on_a_column_is_not_taken_for_an_identity() -> None:
    seek = _access(
        0, "Comments", "c", "IX_Comments_PostId",
        seek=_eq_seek("Comments", "PostId", "c", "@PostId"), predicate=_identity_compare("(1)"),
        runtime=_counters(12), rows=12,
    )

    finding = next(
        item for item in _findings(_plan(seek)) if item["rule"] == "non_sargable_predicate"
    )

    assert finding["evidence"]["wrapper"] == "arithmetic"


# --- A sniffed value cannot change a single-row seek (task T4) ---


def _key_lookup_plan(*, level: str, index: str = "PK_Users", kind: str = "Clustered") -> str:
    seek = _access(
        0, "Users", "u", index, seek=_eq_seek("Users", "Id", "u", "@Id"),
        runtime=_counters(1, reads=3), rows=1, cost=0.0033, kind=kind,
    )
    parameters = (
        '<ColumnReference Column="@Id" ParameterDataType="int" '
        'ParameterCompiledValue="(22656)" ParameterRuntimeValue="(1144035)" />'
    )
    return _plan(seek, cost=0.0033, level=level, parameters=parameters)


def test_sniffed_value_on_a_trivial_plan_is_not_reported() -> None:
    findings = _findings(_key_lookup_plan(level="TRIVIAL"))

    assert [item["rule"] for item in findings if item["rule"] == "sniffed_parameter_differs"] == []


def test_sniffed_value_on_an_equality_seek_of_a_unique_key_is_not_reported() -> None:
    plan = _key_lookup_plan(level="FULL")

    without_catalog = _findings(plan)
    with_catalog = _findings(plan, unique_keys={"dbo.Users": [["Id"]]})

    # The plan does not say an index is unique: only the catalog can clear it.
    assert any(item["rule"] == "sniffed_parameter_differs" for item in without_catalog)
    assert not any(item["rule"] == "sniffed_parameter_differs" for item in with_catalog)


def test_sniffed_value_on_a_non_unique_seek_is_still_reported() -> None:
    plan = _key_lookup_plan(level="FULL", index="IX_Users_Reputation", kind="NonClustered")

    findings = _findings(plan, unique_keys={"dbo.Users": [["AccountId"]]})

    assert any(item["rule"] == "sniffed_parameter_differs" for item in findings)


# --- Threads are labelled as threads in the digest tree (task T2) ---


def test_digest_tree_labels_worker_threads_and_not_as_executions() -> None:
    digest = build_plan_digest(_bitmap_join(), top_n=3, tree_budget=10)

    line = next(item for item in digest["statements"][0]["tree"] if "IX_Badges_UserId" in item)
    assert "actual 184,540/exec" in line
    assert "4 threads" in line
    assert "x4" not in line


def test_digest_tree_still_counts_real_executions() -> None:
    inner = _access(
        1, "Users", "u", "PK_Users", seek=_eq_seek("Users", "Id", "u", "@Id"), rows=1, kind="Clustered",
        runtime=(
            '<RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="46135" '
            'ActualExecutions="46135" ActualExecutionMode="Row" /></RunTimeInformation>'
        ),
    )

    digest = build_plan_digest(_plan(inner), top_n=3, tree_budget=10)

    assert "x46,135 executions" in digest["statements"][0]["tree"][0]


# --- Logical reads come from the actual plan (tasks T2, T3, T5) ---


def test_actual_plan_summary_sums_logical_reads_per_statement_and_object() -> None:
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]

    metrics = service.summarize_showplan_xml(_bitmap_join())["actual_metrics"]

    assert metrics["actual_logical_reads"] == 65_480
    assert metrics["read_metric_source"] == "showplan_operator_runtime_counters"
    assert metrics["statement_reads"] == [
        {
            "statement_ordinal": 1,
            "logical_reads": 65_480,
            "lob_logical_reads": 0,
            "physical_reads": 0,
            "objects": [
                {"object": "dbo.Users.PK_Users", "logical_reads": 44_000, "lob_logical_reads": 0},
                {"object": "dbo.Badges.IX_Badges_UserId", "logical_reads": 21_480, "lob_logical_reads": 0},
            ],
        }
    ]


def test_actual_plan_without_io_counters_says_so() -> None:
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]
    seek = _access(0, "Users", "u", "PK_Users", runtime=_counters(1), rows=1, kind="Clustered")

    metrics = service.summarize_showplan_xml(_plan(seek))["actual_metrics"]

    assert metrics["actual_logical_reads"] is None
    assert metrics["read_metric_source"] == "not_available_as_reliable_query_total"
    assert metrics["read_metric_note"] == (
        "This actual plan has no I/O counters (ActualLogicalReads) on its operators. "
        "Read logical reads from the benchmark tools or Query Store."
    )


def test_digest_reports_logical_reads_by_object() -> None:
    statement = build_plan_digest(_bitmap_join(), top_n=3, tree_budget=10)["statements"][0]

    assert statement["io"]["logical_reads"] == 65_480
    assert statement["io"]["by_object"][0] == {"object": "dbo.Users.PK_Users", "logical_reads": 44_000}

    seek = _access(0, "Users", "u", "PK_Users", runtime=_counters(1), rows=1, kind="Clustered")
    without = build_plan_digest(_plan(seek), top_n=3, tree_budget=10)["statements"][0]
    assert without["io"] == {"note": "The plan has no I/O counters on its operators."}
