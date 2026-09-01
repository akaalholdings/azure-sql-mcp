from __future__ import annotations

from types import SimpleNamespace

import pytest

from azure_sql_mcp.artifacts import ExplainPlanArtifact
from azure_sql_mcp.connection import ProfiledExecution
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.param_binding import ParameterExecutionContract
from azure_sql_mcp.param_binding import SqlParameterType
from azure_sql_mcp.param_binding import TypedParameter
from azure_sql_mcp.plans import PlansService
from azure_sql_mcp.plans import parse_showplan_index_evidence
from azure_sql_mcp.safe_sql import SafeSqlValidator

SAMPLE_SHOWPLAN = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence>
    <Batch>
      <Statements>
        <StmtSimple StatementText="SELECT name FROM sys.objects" StatementType="SELECT" StatementSubTreeCost="0.013" StatementEstRows="10">
          <QueryPlan>
            <RelOp PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" EstimateRows="10" EstimateIO="0.01" EstimateCPU="0.003" EstimatedTotalSubtreeCost="0.013">
              <Warnings NoJoinPredicate="false" />
            </RelOp>
          </QueryPlan>
        </StmtSimple>
      </Statements>
    </Batch>
  </BatchSequence>
</ShowPlanXML>
"""

ACTUAL_SHOWPLAN = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence>
    <Batch>
      <Statements>
        <StmtSimple StatementText="SELECT * FROM dbo.Orders WHERE CustomerId = @CustomerId"
                    StatementType="SELECT"
                    StatementSubTreeCost="0.42"
                    StatementEstRows="12"
                    StatementOptmLevel="FULL"
                    CardinalityEstimationModelVersion="170"
                    QueryHash="0x90FC7E5399EA52A5"
                    QueryPlanHash="0x36B85B3A4F4A25A6">
          <QueryPlan>
            <MemoryGrantInfo SerialRequiredMemory="1024" SerialDesiredMemory="2048"
                             RequestedMemory="2048" GrantedMemory="2048"
                             MaxUsedMemory="512" />
            <ParameterList>
              <ColumnReference Column="@CustomerId"
                               ParameterCompiledValue="(1)"
                               ParameterRuntimeValue="(42)" />
            </ParameterList>
            <MissingIndexes>
              <MissingIndexGroup Impact="87.5">
                <MissingIndex Database="[appdb]" Schema="[dbo]" Table="[Orders]">
                  <ColumnGroup Usage="EQUALITY">
                    <Column Name="[CustomerId]" />
                  </ColumnGroup>
                  <ColumnGroup Usage="INCLUDE">
                    <Column Name="[Status]" />
                  </ColumnGroup>
                </MissingIndex>
              </MissingIndexGroup>
            </MissingIndexes>
            <RelOp PhysicalOp="Hash Match" LogicalOp="Inner Join"
                   EstimateRows="12" EstimateIO="0.01" EstimateCPU="0.03"
                   EstimatedTotalSubtreeCost="0.42">
              <RunTimeInformation>
                <RunTimeCountersPerThread Thread="0"
                                          ActualRows="34"
                                          ActualExecutions="1"
                                          ActualLogicalReads="120"
                                          ActualPhysicalReads="3"
                                          ActualCPUms="8"
                                          ActualElapsedms="11" />
              </RunTimeInformation>
              <Warnings>
                <SpillToTempDb SpillLevel="1" SpilledThreadCount="1" />
              </Warnings>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
      </Statements>
    </Batch>
  </BatchSequence>
</ShowPlanXML>
"""

DETAILED_SHOWPLAN = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence><Batch><Statements>
    <StmtSimple StatementType="SELECT" StatementEstRows="1" QueryHash="0x1111111111111111">
      <QueryPlan>
        <MemoryGrantInfo RequestedMemory="4096" GrantedMemory="4096"
                          MaxUsedMemory="1024" IsMemoryGrantFeedbackAdjusted="Yes: 1" />
        <ParameterList>
          <ColumnReference Column="@CustomerId" ParameterDataType="int"
                           ParameterCompiledValue="(1)" ParameterRuntimeValue="(42)" />
        </ParameterList>
        <RelOp NodeId="1" PhysicalOp="Index Seek" LogicalOp="Index Seek"
               EstimateRows="10" EstimateRowsWithoutRowGoal="100"
               EstimatedExecutionMode="Batch" Parallel="true">
          <IndexScan Lookup="true">
            <Object Database="[appdb]" Schema="[dbo]" Table="[Orders]" Index="[IX_Orders_CustomerId]" />
            <SeekPredicates><SeekPredicateNew>
              <SeekKeys><ScalarOperator ScalarString="[o].[CustomerId]=[@CustomerId]" /></SeekKeys>
            </SeekPredicateNew></SeekPredicates>
            <Predicate><ScalarOperator ScalarString="[o].[Status]=N'Active'" /></Predicate>
          </IndexScan>
          <Top RowCount="1" />
          <RunTimeInformation><RunTimeCountersPerThread Thread="1" ActualRows="25"
              ActualRowsRead="100" ActualExecutions="1" ActualExecutionMode="Batch" /></RunTimeInformation>
          <Warnings>
            <SpillToTempDb SpillLevel="2" SpilledThreadCount="2" />
            <PlanAffectingConvert ConvertIssue="Seek Plan" Expression="CONVERT_IMPLICIT(int,[x],0)" />
          </Warnings>
        </RelOp>
        <RelOp NodeId="2" PhysicalOp="Parallelism" LogicalOp="Gather Streams">
          <RunTimeInformation><RunTimeCountersPerThread Thread="1" ActualRows="100" />
            <RunTimeCountersPerThread Thread="2" ActualRows="10" /></RunTimeInformation>
        </RelOp>
        <ParameterSensitivePredicate LowBoundary="1" HighBoundary="100" />
      </QueryPlan>
    </StmtSimple>
  </Statements></Batch></BatchSequence>
</ShowPlanXML>
"""

STATEMENT_METRICS_SHOWPLAN = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence><Batch><Statements>
    <StmtSimple StatementText="DECLARE @CustomerId int" StatementType="DECLARE">
      <QueryTimeStats CpuTime="1" ElapsedTime="2" />
    </StmtSimple>
    <StmtSimple StatementText="SET @CustomerId = 42" StatementType="SET">
      <QueryTimeStats CpuTime="2" ElapsedTime="3" />
    </StmtSimple>
    <StmtSimple StatementText="SELECT * FROM dbo.Orders" StatementType="SELECT">
      <QueryPlan CompileTime="9" CompileCPU="7" CompileMemory="1024">
        <QueryTimeStats CpuTime="8" ElapsedTime="11" />
        <RelOp PhysicalOp="Index Scan" LogicalOp="Index Scan">
          <RunTimeInformation>
            <RunTimeCountersPerThread ActualRows="34" ActualExecutions="1" />
          </RunTimeInformation>
        </RelOp>
      </QueryPlan>
    </StmtSimple>
  </Statements></Batch></BatchSequence>
</ShowPlanXML>
"""

MULTI_SELECT_SHOWPLAN = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence><Batch><Statements>
    <StmtSimple StatementText="SELECT 1" StatementType="SELECT">
      <QueryPlan><QueryTimeStats CpuTime="8" ElapsedTime="11" />
        <RelOp PhysicalOp="Constant Scan" LogicalOp="Constant Scan">
          <RunTimeInformation><RunTimeCountersPerThread ActualRows="1" /></RunTimeInformation>
        </RelOp>
      </QueryPlan>
    </StmtSimple>
    <StmtSimple StatementText="SELECT 2" StatementType="SELECT">
      <QueryPlan><QueryTimeStats CpuTime="3" ElapsedTime="4" />
        <RelOp PhysicalOp="Constant Scan" LogicalOp="Constant Scan">
          <RunTimeInformation><RunTimeCountersPerThread ActualRows="1" /></RunTimeInformation>
        </RelOp>
      </QueryPlan>
    </StmtSimple>
  </Statements></Batch></BatchSequence>
</ShowPlanXML>
"""

MALFORMED_COMPILE_SHOWPLAN = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence><Batch><Statements>
    <StmtSimple StatementType="SELECT">
      <QueryPlan CompileTime="9" CompileCPU="not-a-number" CompileMemory="1024" />
    </StmtSimple>
  </Statements></Batch></BatchSequence>
</ShowPlanXML>
"""


class FakeExecutor:
    def __init__(self, *, can_create_index=True, fail_create=False, row_limit=200):
        self.can_create_index = can_create_index
        self.fail_create = fail_create
        self.config = SimpleNamespace(row_limit=row_limit)
        self.fetch_history = []
        self.batch_history = []
        self.session_history = []
        self.session_params = []
        self.session_input_sizes = []
        self.profile_history = []
        self.profile_input_sizes = []
        self.non_query_history = []

    async def fetch_all(self, database_name, query, params=None):
        self.fetch_history.append((database_name, query, params))
        return [{"can_create_index": 1 if self.can_create_index else 0}]

    async def execute_batches(self, database_name, query, params=None):
        self.batch_history.append((database_name, query, params))

        class Result:
            rows = [{"plan_xml": SAMPLE_SHOWPLAN}]

        return [Result()]

    async def execute_session(
        self,
        database_name,
        statements,
        *,
        max_rows=None,
        statement_params=None,
        statement_input_sizes=None,
    ):
        self.session_history.append((database_name, list(statements), max_rows))
        self.session_params.append(statement_params)
        self.session_input_sizes.append(statement_input_sizes)

        class Result:
            rows = [{"plan_xml": SAMPLE_SHOWPLAN}]

        # Return one results-list per statement; the user query is at index 1.
        return [[], [Result()], []]

    async def execute_profiled_read_only(
        self,
        database_name,
        query,
        params=None,
        *,
        max_rows=None,
        input_sizes=None,
    ):
        self.profile_history.append((database_name, query, params, max_rows))
        self.profile_input_sizes.append(input_sizes)
        return ProfiledExecution(
            result_sets=[
                QueryResult(
                    columns=("plan_xml",),
                    rows=[{"plan_xml": SAMPLE_SHOWPLAN}],
                )
            ],
            elapsed_wall_ms=3.0,
        )

    async def execute_non_query(self, database_name, query, params=None):
        self.non_query_history.append((database_name, query, params))
        if self.fail_create and query.startswith("CREATE NONCLUSTERED INDEX"):
            raise RuntimeError("create index failed")
        return 0


def test_summarize_showplan_xml():
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]
    summary = service.summarize_showplan_xml(SAMPLE_SHOWPLAN)

    assert summary["statement_count"] == 1
    assert summary["operator_count"] == 1
    assert summary["statements"][0]["statement_type"] == "SELECT"
    assert summary["top_operators"][0]["physical_op"] == "Clustered Index Scan"
    assert summary["warnings"] == [{"NoJoinPredicate": "false"}]


def test_summarize_showplan_xml_extracts_actual_plan_evidence():
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]
    summary = service.summarize_showplan_xml(ACTUAL_SHOWPLAN)

    assert summary["statements"][0]["cardinality_estimation_model_version"] == "170"
    # Stable Query Store correlation ids: tune_query matches history by hash.
    assert summary["statements"][0]["query_hash"] == "0x90FC7E5399EA52A5"
    assert summary["statements"][0]["query_plan_hash"] == "0x36B85B3A4F4A25A6"
    assert summary["actual_metrics"]["actual_rows"] == 34
    # Operator/thread counters are not valid query totals. This fixture has no
    # statement-level QueryTimeStats, so totals stay explicitly unavailable.
    assert summary["actual_metrics"]["actual_cpu_ms"] is None
    assert summary["actual_metrics"]["actual_elapsed_ms"] is None
    assert summary["actual_metrics"]["actual_logical_reads"] is None
    assert summary["actual_metrics"]["read_metric_source"] == (
        "not_available_as_reliable_query_total"
    )
    assert summary["memory_grants"][0]["granted_memory_kb"] == 2048
    assert summary["missing_indexes"][0]["impact_pct"] == 87.5
    assert summary["missing_indexes"][0]["equality_columns"] == ["CustomerId"]
    assert summary["parameters"][0]["compiled_value"] == "(1)"
    assert summary["parameters"][0]["runtime_value"] == "(42)"
    assert summary["warnings"][0]["spills_to_tempdb"][0]["SpillLevel"] == "1"


def test_actual_metrics_select_single_user_select_around_declare_and_set() -> None:
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]
    summary = service.summarize_showplan_xml(STATEMENT_METRICS_SHOWPLAN)

    assert summary["actual_metrics"]["user_select_count"] == 1
    assert summary["actual_metrics"]["actual_cpu_ms"] == 8
    assert summary["actual_metrics"]["actual_elapsed_ms"] == 11
    assert summary["actual_metrics"]["actual_rows"] == 34
    assert summary["actual_metrics"]["query_metric_source"] == (
        "showplan_query_time_stats"
    )
    assert summary["compile_metrics"] == {
        "query_plan_count": 1,
        "compile_ms": 9,
        "compile_cpu_ms": 7,
        "compile_memory_kb": 1024,
        "metric_provenance": "showplan_query_plan_compile_attributes",
    }


def test_actual_metrics_keep_multiple_selects_ambiguous() -> None:
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]
    summary = service.summarize_showplan_xml(MULTI_SELECT_SHOWPLAN)

    assert summary["actual_metrics"]["user_select_count"] == 2
    assert summary["actual_metrics"]["actual_cpu_ms"] is None
    assert summary["actual_metrics"]["actual_elapsed_ms"] is None
    assert summary["actual_metrics"]["actual_rows"] is None
    assert summary["actual_metrics"]["query_metric_source"] == (
        "unavailable_for_multi_select_plan"
    )


def test_explain_plan_artifact_exposes_separate_compile_and_execution_metrics() -> None:
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]
    summary = service.summarize_showplan_xml(STATEMENT_METRICS_SHOWPLAN)
    artifact = ExplainPlanArtifact(
        database_name="appdb",
        analyze=True,
        summary=summary,
        raw_xml=STATEMENT_METRICS_SHOWPLAN,
    )

    payload = artifact.as_dict(include_raw_xml=False)

    assert payload["plan_kind"] == "actual"
    assert payload["query_executed"] is True
    assert payload["compile_ms"] == 9
    assert payload["execution_ms"] == 11
    assert payload["metric_provenance"] == {
        "compile_ms": "showplan_query_plan_compile_attributes",
        "execution_ms": "showplan_query_time_stats",
    }

    estimated = ExplainPlanArtifact(
        database_name="appdb",
        analyze=False,
        summary=summary,
        raw_xml=STATEMENT_METRICS_SHOWPLAN,
    )
    assert estimated.plan_kind == "estimated"
    assert estimated.query_executed is False
    assert estimated.compile_ms == 9
    assert estimated.execution_ms is None
    assert estimated.metric_provenance["execution_ms"] == (
        "not_applicable_estimated_plan"
    )


def test_plan_truth_fields_cannot_contradict_analyze() -> None:
    with pytest.raises(ValueError, match="plan_kind must agree"):
        ExplainPlanArtifact(
            database_name="appdb",
            analyze=False,
            summary={},
            raw_xml="<ShowPlanXML />",
            plan_kind="actual",
        )

    with pytest.raises(ValueError, match="query_executed must agree"):
        ExplainPlanArtifact(
            database_name="appdb",
            analyze=True,
            summary={},
            raw_xml="<ShowPlanXML />",
            query_executed=False,
        )


def test_compile_metrics_ignore_malformed_attributes_without_raising() -> None:
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]
    summary = service.summarize_showplan_xml(MALFORMED_COMPILE_SHOWPLAN)

    assert summary["compile_metrics"]["compile_ms"] == 9
    assert summary["compile_metrics"]["compile_cpu_ms"] is None
    assert summary["compile_metrics"]["compile_memory_kb"] == 1024


def test_summarize_showplan_xml_extracts_actionable_operator_diagnostics() -> None:
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]
    summary = service.summarize_showplan_xml(DETAILED_SHOWPLAN)

    seek = summary["operators"][0]
    assert seek["node_id"] == 1
    assert seek["physical_op"] == "Index Seek"
    assert seek["logical_op"] == "Index Seek"
    assert seek["object"]["qualified_name"] == "appdb.dbo.Orders"
    assert seek["index_name"] == "IX_Orders_CustomerId"
    assert seek["seek_predicates"] == ["[o].[CustomerId]=[@CustomerId]"]
    assert seek["residual_predicates"] == ["[o].[Status]=N'Active'"]
    assert seek["actual_rows"] == 25
    assert seek["actual_rows_ratio"] == 2.5
    assert seek["execution_mode"] == "Batch"
    assert seek["row_goal"] is True
    assert seek["row_goal_details"]["top_row_count"] == 1
    assert seek["lookup"] is True
    assert seek["spills"][0]["SpillLevel"] == "2"
    assert seek["implicit_conversions"][0]["ConvertIssue"] == "Seek Plan"
    assert summary["memory_grants"][0]["granted_memory_kb"] == 4096
    assert summary["feedback"][0]["IsMemoryGrantFeedbackAdjusted"] == "Yes: 1"
    assert summary["feedback"][1]["element"] == "ParameterSensitivePredicate"
    assert summary["parameters"][0]["data_type"] == "int"

    parallel = summary["operators"][1]["parallel_exchange"]
    assert parallel["thread_count"] == 2
    assert parallel["row_skew_ratio"] == pytest.approx(1.8181818)
    assert summary["actual_metrics"]["actual_logical_reads"] is None


def test_redacted_showplan_parser_deduplicates_multi_object_references() -> None:
    multi_object_plan = DETAILED_SHOWPLAN.replace(
        "</QueryPlan>",
        "<RelOp PhysicalOp=\"Index Scan\" LogicalOp=\"Index Scan\"><IndexScan>"
        "<Object Database=\"[appdb]\" Schema=\"[dbo]\" Table=\"[Orders]\" "
        "Index=\"[IX_Orders_CustomerId]\" /></IndexScan></RelOp></QueryPlan>",
    )
    result = parse_showplan_index_evidence(
        multi_object_plan,
        query_id=42,
        plan_id=7,
        execution_count=12,
        runtime_interval_ids=[9, 9],
        last_seen="2026-07-15T10:00:00Z",
        is_forced_plan=True,
    )

    assert len(result["index_references"]) == 1
    reference = result["index_references"][0]
    assert reference["database_name"] == "appdb"
    assert reference["schema_name"] == "dbo"
    assert reference["object_name"] == "Orders"
    assert reference["index_name"] == "IX_Orders_CustomerId"
    assert reference["operator_kind"] == "Multiple"
    assert reference["operator_kinds"] == ["Index Scan", "Index Seek"]
    assert reference["is_forced_plan"] is True
    assert reference["execution_count"] == 12
    assert reference["runtime_interval_ids"] == [9]
    assert reference["plan_fingerprint"] == result["plan_fingerprint"]
    assert result["coverage"]["status"] == "complete"


def test_redacted_showplan_parser_returns_canonical_candidate_signatures() -> None:
    result = parse_showplan_index_evidence(
        ACTUAL_SHOWPLAN,
        query_id=42,
        plan_id=8,
        execution_count=4,
        runtime_interval_ids=[1, 2],
    )

    [candidate] = result["missing_index_candidates"]
    assert candidate["database_name"] == "appdb"
    assert candidate["schema_name"] == "dbo"
    assert candidate["object_name"] == "Orders"
    assert candidate["key_signature"] == "=CustomerId"
    assert candidate["include_signature"] == "Status"
    assert candidate["filter_signature"] is None
    assert candidate["impact_pct"] == 87.5
    assert candidate["candidate_signature"] == (
        "appdb.dbo.Orders|key:=CustomerId|include:Status|filter:"
    )
    assert candidate["runtime_interval_ids"] == [1, 2]


def test_redacted_showplan_parser_never_returns_text_parameters_predicates_or_xml() -> None:
    result = parse_showplan_index_evidence(
        DETAILED_SHOWPLAN,
        query_id=42,
        plan_id=7,
    )
    serialized = repr(result)

    for secret in (
        "SELECT * FROM dbo.Orders",
        "ScalarString",
        "@CustomerId",
        "ParameterCompiledValue",
        "ShowPlanXML",
    ):
        assert secret not in serialized


@pytest.mark.parametrize(
    "raw_xml",
    ("<ShowPlanXML>", "<NotShowPlan />", "<ShowPlanXML />"),
)
def test_redacted_showplan_parser_fails_closed_for_malformed_or_wrong_namespace(
    raw_xml: str,
) -> None:
    result = parse_showplan_index_evidence(raw_xml, query_id=42, plan_id=7)

    assert result["index_references"] == []
    assert result["missing_index_candidates"] == []
    assert result["coverage"]["status"] == "incomplete"
    assert result["coverage"]["malformed"] == 1


def test_redacted_showplan_parser_reports_input_caps_and_bad_numeric_facts() -> None:
    result = parse_showplan_index_evidence(
        DETAILED_SHOWPLAN,
        query_id=42,
        plan_id=7,
        execution_count=-1,
        runtime_interval_ids=["not-an-id"],  # type: ignore[list-item]
        max_xml_chars=10,
    )

    assert result["coverage"]["status"] == "incomplete"
    assert result["coverage"]["capped"] is True
    assert "execution_count_malformed" in result["coverage"]["blockers"]
    assert "runtime_interval_id_malformed" in result["coverage"]["blockers"]
    assert "showplan_xml_capped" in result["coverage"]["blockers"]


def test_statistics_io_is_per_sample_and_sourced_from_messages() -> None:
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]
    sample = service.summarize_statistics_io(
        [
            "Table 'Orders'. Scan count 1, logical reads 12, physical reads 0, "
            "read-ahead reads 4, lob logical reads 0, lob physical reads 0.",
            "Table 'Worktable'. Scan count 0, logical reads 3, physical reads 0, "
            "read-ahead reads 0, lob logical reads 0, lob physical reads 0.",
        ],
        sample_id="candidate-rare-1",
    )

    assert sample["sample_id"] == "candidate-rare-1"
    assert sample["query_totals"] == {
        "scan_count": 1,
        "logical_reads": 15,
        "physical_reads": 0,
        "read_ahead_reads": 4,
        "lob_logical_reads": 0,
        "lob_physical_reads": 0,
    }
    assert sample["query_totals_source"] == "statistics_io_table_messages"
    assert sample["operator_thread_counters_not_used"] is True
    assert all(row["provenance"] for row in sample["tables"])


@pytest.mark.parametrize(
    "driver_message",
    (
        (0, "Table 'Orders'. Scan count 1, logical reads 12, physical reads 0."),
        [0, "Table 'Orders'. Scan count 1, logical reads 12, physical reads 0."],
    ),
)
def test_statistics_io_extracts_text_from_pyodbc_tuple_or_list_message(
    driver_message,
) -> None:
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]

    sample = service.summarize_statistics_io(
        [driver_message],
        sample_id="driver-message",
    )

    assert sample["tables"][0]["table"] == "Orders"
    assert sample["query_totals"]["logical_reads"] == 12


@pytest.mark.asyncio
async def test_explain_query_rejects_hypothetical_indexes_without_writes():
    executor = FakeExecutor()
    service = PlansService(executor=executor, validator=SafeSqlValidator())

    with pytest.raises(ValueError, match="Hypothetical index analysis is disabled"):
        await service.explain_query(
            database_name="appdb",
            sql="SELECT name FROM sys.objects",
            analyze=False,
            hypothetical_indexes=[
                {"schema": "dbo", "table": "Orders", "columns": ["CustomerId"]}
            ],
        )

    assert executor.non_query_history == []
    assert executor.session_history == []


@pytest.mark.asyncio
async def test_explain_query_bounds_result_set_fetches():
    """With STATISTICS XML the user query actually executes; the session fetch
    must be bounded so a huge SELECT cannot pull an entire table into memory."""
    executor = FakeExecutor(row_limit=50)
    service = PlansService(executor=executor, validator=SafeSqlValidator())

    artifact = await service.explain_query(
        database_name="appdb",
        sql="SELECT name FROM sys.objects",
        analyze=True,
    )

    assert artifact.raw_xml == SAMPLE_SHOWPLAN
    database_name, statements, max_rows = executor.session_history[0]
    assert statements[0] == "SET STATISTICS XML ON"
    assert max_rows == 51


def _typed_contract() -> ParameterExecutionContract:
    return ParameterExecutionContract(
        sql_text="SELECT name FROM sys.objects WHERE object_id = @ObjectId",
        bucket_id="common",
        parameters=(
            TypedParameter(
                name="@ObjectId",
                sql_type=SqlParameterType.from_sql("int"),
                value=42,
                provenance="synthetic",
            ),
        ),
        provenance="synthetic",
    )


@pytest.mark.asyncio
async def test_estimated_parameterized_plan_uses_typed_sp_executesql_arguments():
    executor = FakeExecutor(row_limit=50)
    service = PlansService(executor=executor, validator=SafeSqlValidator())
    contract = _typed_contract()

    artifact = await service.explain_parameterized_query(
        "appdb",
        contract,
        analyze=False,
    )

    assert artifact.raw_xml == SAMPLE_SHOWPLAN
    assert executor.session_history[0][1][1] == contract.sp_executesql_sql
    assert executor.session_params[0] == [
        None,
        contract.sp_executesql_values,
        None,
    ]
    assert executor.session_input_sizes[0] == [
        None,
        contract.sp_executesql_input_sizes,
        None,
    ]


@pytest.mark.asyncio
async def test_actual_parameterized_plan_executes_one_typed_profile_sample():
    executor = FakeExecutor(row_limit=50)
    service = PlansService(executor=executor, validator=SafeSqlValidator())
    contract = _typed_contract()

    artifact = await service.explain_parameterized_query(
        "appdb",
        contract,
        analyze=True,
    )

    assert artifact.analyze is True
    assert executor.profile_history == [
        (
            "appdb",
            contract.sp_executesql_sql,
            contract.sp_executesql_values,
            51,
        )
    ]
    assert executor.profile_input_sizes == [contract.sp_executesql_input_sizes]
