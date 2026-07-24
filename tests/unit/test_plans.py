from __future__ import annotations

from types import SimpleNamespace

import pytest

from azure_sql_mcp.connection import ProfiledExecution
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.param_binding import ParameterExecutionContract
from azure_sql_mcp.param_binding import SqlParameterType
from azure_sql_mcp.param_binding import TypedParameter
from azure_sql_mcp.plans import PlansService
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


class FakeExecutor:
    def __init__(self, *, can_create_index=True, fail_create=False, row_limit=200):
        self.can_create_index = can_create_index
        self.fail_create = fail_create
        self.config = SimpleNamespace(row_limit=row_limit)
        self.fetch_history = []
        self.batch_history = []
        self.session_history = []
        self.session_params = []
        self.profile_history = []
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
    ):
        self.session_history.append((database_name, list(statements), max_rows))
        self.session_params.append(statement_params)

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
    ):
        self.profile_history.append((database_name, query, params, max_rows))
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
