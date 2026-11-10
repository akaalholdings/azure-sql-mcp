from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import IndexKeyColumn
from azure_sql_mcp.param_binding import ParameterExecutionContract
from azure_sql_mcp.param_binding import SqlParameterType
from azure_sql_mcp.param_binding import TypedParameter
from azure_sql_mcp.query_index_analysis import QueryIndexAnalysisService
from azure_sql_mcp.safe_sql import SafeSqlValidator

# Smallest plan Azure SQL Database returns for a compiled SELECT: one statement
# with a QueryPlan and a RelOp.
MINIMAL_PLAN = (
    '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">'
    "<BatchSequence><Batch><Statements>"
    '<StmtSimple StatementType="SELECT"><QueryPlan>'
    '<RelOp NodeId="0" PhysicalOp="Constant Scan" LogicalOp="Constant Scan" />'
    "</QueryPlan></StmtSimple>"
    "</Statements></Batch></BatchSequence></ShowPlanXML>"
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("query", "contract_sql"),
    [
        ("SELECT 1", "select 1"),
        ("SELECT  1", "SELECT 1"),
    ],
)
async def test_analyze_queries_rejects_nonidentical_contract_sql(
    query: str,
    contract_sql: str,
) -> None:
    executor = MagicMock()
    executor.config.row_limit = 200
    executor.fetch_all = AsyncMock(return_value=[])
    service = QueryIndexAnalysisService(executor, SafeSqlValidator())
    contract = ParameterExecutionContract(
        sql_text=contract_sql,
        bucket_id="common",
        parameters=(),
        provenance="synthetic",
    )

    with pytest.raises(ValueError, match="exactly match"):
        await service.analyze_queries(
            "appdb",
            [query],
            execution_contracts=[contract],
        )

    executor.fetch_all.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_estimated_plan_uses_execute_session() -> None:
    executor = MagicMock()
    executor.config.row_limit = 200
    executor.execute_session = AsyncMock(
        return_value=[
            [],
            [QueryResult(columns=("plan_xml",), rows=[{"plan_xml": MINIMAL_PLAN}])],
            [],
        ]
    )
    service = QueryIndexAnalysisService(executor, SafeSqlValidator())

    result = await service._get_estimated_plan("appdb", "SELECT 1")

    assert result == MINIMAL_PLAN
    executor.execute_session.assert_awaited_once_with(
        "appdb",
        ["SET SHOWPLAN_XML ON", "SELECT 1", "SET SHOWPLAN_XML OFF"],
        max_rows=201,
    )


@pytest.mark.asyncio
async def test_analyze_queries_uses_typed_contract_for_estimated_plan() -> None:
    executor = MagicMock()
    executor.config.row_limit = 200
    executor.fetch_all = AsyncMock(return_value=[])
    executor.execute_session = AsyncMock(
        return_value=[
            [],
            [QueryResult(columns=("plan_xml",), rows=[{"plan_xml": MINIMAL_PLAN}])],
            [],
        ]
    )
    service = QueryIndexAnalysisService(executor, SafeSqlValidator())
    service._extract_missing_indexes = MagicMock(return_value=[])
    sql = "SELECT name FROM dbo.Widgets WHERE WidgetId = @WidgetId"
    contract = ParameterExecutionContract(
        sql_text=sql,
        bucket_id="common",
        parameters=(
            TypedParameter(
                name="@WidgetId",
                sql_type=SqlParameterType.from_sql("bigint"),
                value=42,
                provenance="synthetic",
            ),
        ),
        provenance="synthetic",
    )

    result = await service.analyze_queries(
        "appdb",
        [sql],
        execution_contracts=[contract],
    )

    assert result["queries_analyzed"] == 1
    plan_call = executor.execute_session.await_args
    # SHOWPLAN_XML returns nothing for parameter markers through mssql-python
    # (live, 2026-11-04): the estimated plan sends constants only.
    assert plan_call.args[1][1] == contract.estimated_showplan_sql
    assert "statement_params" not in plan_call.kwargs
    assert result["missing_index_provenance"]["source"] == "SHOWPLAN_XML"
    assert result["missing_index_provenance"][
        "zero_hint_is_not_proof_no_index_can_help"
    ] is True
    assert result["recommendation_basis"] == "showplan_missing_index_hints"
    assert result["zero_recommendations_mean"] == (
        "no_uncovered_optimizer_hint_after_existing_index_filtering"
    )
    assert result["raw_missing_index_hint_count"] == 0
    assert result["recommendation_count_after_filtering"] == 0


@pytest.mark.asyncio
async def test_typed_query_detail_states_its_estimated_plan_basis() -> None:
    # A typed plan compiles local variables, not parameters: its missing-index
    # hints carry the same "PSP/optional-parameter variants not modeled" caveat
    # explain_query reports. An untyped query compiles itself and has no basis.
    executor = MagicMock()
    executor.config.row_limit = 200
    executor.fetch_all = AsyncMock(return_value=[])
    executor.execute_session = AsyncMock(
        return_value=[
            [],
            [QueryResult(columns=("plan_xml",), rows=[{"plan_xml": MINIMAL_PLAN}])],
            [],
        ]
    )
    service = QueryIndexAnalysisService(executor, SafeSqlValidator())
    typed_sql = "SELECT name FROM dbo.Widgets WHERE WidgetId = @WidgetId"
    typed = ParameterExecutionContract(
        sql_text=typed_sql,
        bucket_id="common",
        parameters=(
            TypedParameter(
                name="@WidgetId",
                sql_type=SqlParameterType.from_sql("bigint"),
                value=42,
                provenance="synthetic",
            ),
        ),
        provenance="synthetic",
    )
    untyped = ParameterExecutionContract(
        sql_text="SELECT 1",
        bucket_id="common",
        parameters=(),
        provenance="synthetic",
    )

    result = await service.analyze_queries(
        "appdb",
        [typed_sql, "SELECT 1"],
        execution_contracts=[typed, untyped],
    )

    typed_detail, untyped_detail = result["query_details"]
    assert typed_detail["estimated_plan_basis"] == typed.estimated_showplan_basis
    assert "parameter_sensitive_plan" in (
        typed_detail["estimated_plan_basis"]["not_modeled"]
    )
    assert "estimated_plan_basis" not in untyped_detail


@pytest.mark.asyncio
async def test_analyze_workload_binds_query_store_parameterized_sql() -> None:
    """Query Store text like '(@P1 int)SELECT ... WHERE x = @P1' cannot be
    SHOWPLAN-compiled as-is ('must declare the scalar variable'); the workload
    analyzer must strip the declaration prefix AND bind the parameters."""
    executor = MagicMock()
    executor.config.row_limit = 200

    async def fetch_router(database_name, query, params=None):
        if "query_store_runtime_stats" in query:
            return [
                {
                    "query_id": 1,
                    "plan_id": 10,
                    "query_sql_text": "(@P1 int)SELECT name FROM dbo.Widgets WHERE WidgetId = @P1",
                    "executions": 3,
                    "total_cpu_us": 1000,
                    "total_duration_us": 2000,
                    "total_logical_io_reads": 50,
                }
            ]
        if "sys.stats_columns" in query:
            return [
                {
                    "table_name": "Widgets",
                    "schema_name": "dbo",
                    "data_type": "int",
                    "max_length": 4,
                    "precision": 10,
                    "scale": 0,
                    "stats_id": 1,
                }
            ]
        return []  # histogram, existing indexes, DMV recommendations

    executor.fetch_all = AsyncMock(side_effect=fetch_router)
    executor.execute_session = AsyncMock(
        return_value=[
            [],
            [QueryResult(columns=("plan_xml",), rows=[{"plan_xml": MINIMAL_PLAN}])],
            [],
        ]
    )

    service = QueryIndexAnalysisService(executor, SafeSqlValidator())
    # Parsing a minimal plan is enough here; no missing index nodes required.
    service._extract_missing_indexes = MagicMock(return_value=[])

    result = await service.analyze_workload("appdb", window_minutes=60, top_n=5)

    assert result["queries_analyzed"] == 1
    assert result["analysis_status"] == "available"
    assert result["analyzed_queries"][0]["missing_index_count"] == 0
    assert result["analyzed_queries"][0]["analysis_status"] == "available"
    assert result["analyzed_queries"][0]["missing_index_provenance"][
        "zero_hint_is_not_proof_no_index_can_help"
    ] is True
    assert "error" not in result["analyzed_queries"][0]

    # The plan session must have received an executable DECLARE-bound batch
    # (sqlglot normalizes the type spelling, e.g. int -> INTEGER).
    session_batch = executor.execute_session.await_args.args[1]
    assert session_batch[1].upper().startswith("DECLARE @P1 INT")
    # No invented value: a typed local variable compiles the same estimated plan.
    assert "SET @P1" not in session_batch[1]
    assert "(@P1 int)SELECT" not in session_batch[1]


@pytest.mark.asyncio
async def test_analyze_queries_distinguishes_raw_hints_from_filtered_output() -> None:
    executor = MagicMock()
    executor.config.row_limit = 200
    service = QueryIndexAnalysisService(executor, SafeSqlValidator())
    service._get_existing_indexes = AsyncMock(
        return_value=[
            ExistingIndex(
                schema="dbo",
                table="Widgets",
                index_id=2,
                name="IX_Widgets_ExternalCode",
                index_type="NONCLUSTERED",
                key_columns=(IndexKeyColumn("ExternalCode"),),
            )
        ]
    )
    service._get_estimated_plan = AsyncMock(return_value="<ShowPlanXML/>")
    service._extract_missing_indexes = MagicMock(
        return_value=[
            {
                "schema": "dbo",
                "table": "Widgets",
                "equality_columns": ["ExternalCode"],
                "inequality_columns": [],
                "include_columns": [],
                "impact_pct": 90.0,
                "create_index_sql": (
                    "CREATE INDEX IX_Widgets_ExternalCode "
                    "ON dbo.Widgets (ExternalCode)"
                ),
            }
        ]
    )

    result = await service.analyze_queries(
        "appdb",
        ["SELECT WidgetId FROM dbo.Widgets WHERE ExternalCode = 'A100'"],
    )

    assert result["query_details"][0]["missing_index_count"] == 1
    assert result["raw_missing_index_hint_count"] == 1
    assert result["recommendations"] == []
    assert result["recommendation_count_after_filtering"] == 0
    assert result["hints_removed_by_filtering_or_consolidation"] == 1
    assert result["zero_recommendations_mean"] == (
        "no_uncovered_optimizer_hint_after_existing_index_filtering"
    )


@pytest.mark.asyncio
async def test_failed_workload_plan_is_unavailable_not_zero_hints() -> None:
    executor = MagicMock()
    executor.config.row_limit = 200
    service = QueryIndexAnalysisService(executor, SafeSqlValidator())
    service._get_top_workload_queries = AsyncMock(
        return_value=[
            {
                "query_id": 42,
                "plan_id": 7,
                "query_sql_text": "SELECT WidgetId FROM dbo.Widgets",
            }
        ]
    )
    service._get_existing_indexes = AsyncMock(return_value=[])
    service._get_dmv_missing_indexes = AsyncMock(return_value=[])
    service._get_estimated_plan = AsyncMock(
        side_effect=RuntimeError("No SHOWPLAN XML returned")
    )

    result = await service.analyze_workload("appdb")

    detail = result["analyzed_queries"][0]
    assert detail["analysis_status"] == "unavailable"
    assert detail["missing_index_count"] is None
    assert detail["missing_index_provenance"]["available"] is False
    assert "zero_missing_index_count_mean" not in detail["missing_index_provenance"]
    assert result["analysis_status"] == "unavailable"
    assert result["queries_with_plan_evidence"] == 0
    assert result["queries_with_unavailable_plan_evidence"] == 1
    assert result["zero_recommendations_mean"] == (
        "inconclusive_when_plan_analysis_is_unavailable"
    )


@pytest.mark.asyncio
async def test_empty_workload_is_no_evidence_not_zero_hints() -> None:
    executor = MagicMock()
    executor.config.row_limit = 200
    service = QueryIndexAnalysisService(executor, SafeSqlValidator())
    service._get_top_workload_queries = AsyncMock(return_value=[])
    service._get_existing_indexes = AsyncMock(return_value=[])
    service._get_dmv_missing_indexes = AsyncMock(return_value=[])

    result = await service.analyze_workload("appdb")

    assert result["queries_analyzed"] == 0
    assert result["queries_with_plan_evidence"] == 0
    assert result["analysis_status"] == "no_evidence"
    assert result["zero_recommendations_mean"] == (
        "inconclusive_without_plan_evidence"
    )


@pytest.mark.asyncio
async def test_analyze_workload_skips_non_select_statements() -> None:
    """Query Store also captures DDL/DML (index maintenance, stats updates);
    those must be skipped, not surfaced as validator errors."""
    executor = MagicMock()
    executor.config.row_limit = 200

    async def fetch_router(database_name, query, params=None):
        if "query_store_runtime_stats" in query:
            return [
                {"query_id": 1, "plan_id": 1, "query_sql_text": "CREATE NONCLUSTERED INDEX [IX_Testing_X] ON [dbo].[T] ([A])",
                 "executions": 1, "total_cpu_us": 10, "total_duration_us": 10, "total_logical_io_reads": 1},
                {"query_id": 2, "plan_id": 2, "query_sql_text": "(@samplePercent float)UPDATE STATISTICS [dbo].[T] WITH SAMPLE 50 PERCENT",
                 "executions": 1, "total_cpu_us": 10, "total_duration_us": 10, "total_logical_io_reads": 1},
                {"query_id": 3, "plan_id": 3, "query_sql_text": "SELECT name FROM dbo.T",
                 "executions": 1, "total_cpu_us": 10, "total_duration_us": 10, "total_logical_io_reads": 1},
            ]
        return []

    executor.fetch_all = AsyncMock(side_effect=fetch_router)
    executor.execute_session = AsyncMock(
        return_value=[
            [],
            [QueryResult(columns=("plan_xml",), rows=[{"plan_xml": MINIMAL_PLAN}])],
            [],
        ]
    )

    service = QueryIndexAnalysisService(executor, SafeSqlValidator())
    service._extract_missing_indexes = MagicMock(return_value=[])

    result = await service.analyze_workload("appdb", window_minutes=60, top_n=5)

    assert result["skipped_non_select"] == 2
    assert result["queries_analyzed"] == 1
    assert all("error" not in q for q in result["analyzed_queries"])


# Azure SQL Database, live 2026-11-04: SHOWPLAN_XML does not compile the
# statement inside EXEC sys.sp_executesql; it returns this stub with no plan.
EXEC_STUB_PLAN = (
    '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">'
    "<BatchSequence><Batch><Statements>"
    '<StmtSimple StatementText="EXEC sys.sp_executesql N&apos;SELECT ...&apos;" '
    'StatementType="EXECUTE PROC" RetrievedFromCache="false"></StmtSimple>'
    "</Statements></Batch></BatchSequence></ShowPlanXML>"
)
STATUS_AMOUNT_PLAN = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence><Batch><Statements>
    <StmtSimple StatementType="SELECT"><QueryPlan>
      <MissingIndexes><MissingIndexGroup Impact="97.1">
        <MissingIndex Database="[appdb]" Schema="[dbo]" Table="[orders]">
          <ColumnGroup Usage="EQUALITY"><Column Name="[status]" /><Column Name="[amount]" /></ColumnGroup>
        </MissingIndex>
      </MissingIndexGroup></MissingIndexes>
      <RelOp NodeId="0" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" />
    </QueryPlan></StmtSimple>
  </Statements></Batch></BatchSequence>
</ShowPlanXML>
"""
TYPED_STATUS_AMOUNT_SQL = "SELECT id FROM dbo.orders WHERE status = @st AND amount = @amt"


def _azure_shaped_executor(*, compiled_plan: str = STATUS_AMOUNT_PLAN) -> MagicMock:
    executor = MagicMock()
    executor.config.row_limit = 200
    executor.fetch_all = AsyncMock(return_value=[])

    async def execute_session(database_name, statements, **kwargs):
        sql = statements[1]
        plan = EXEC_STUB_PLAN if sql.upper().startswith("EXEC SYS.SP_EXECUTESQL") else compiled_plan
        column = "Microsoft SQL Server 2005 XML Showplan"
        return [[], [QueryResult(columns=(column,), rows=[{column: plan}])], []]

    executor.execute_session = AsyncMock(side_effect=execute_session)
    return executor


def _status_amount_contract() -> ParameterExecutionContract:
    return ParameterExecutionContract(
        sql_text=TYPED_STATUS_AMOUNT_SQL,
        bucket_id="common",
        parameters=(
            TypedParameter(name="@st", sql_type=SqlParameterType.from_sql("tinyint"), value=3, provenance="synthetic"),
            TypedParameter(name="@amt", sql_type=SqlParameterType.from_sql("decimal(12,2)"), value=150, provenance="synthetic"),
        ),
        provenance="synthetic",
    )


@pytest.mark.asyncio
async def test_typed_parameters_find_the_same_hint_as_the_literal_query() -> None:
    # Live: the literal query had a (status, amount) hint, the typed one
    # reported "available" with 0 hints: a false "no missing index".
    service = QueryIndexAnalysisService(_azure_shaped_executor(), SafeSqlValidator())

    result = await service.analyze_queries(
        "appdb",
        [TYPED_STATUS_AMOUNT_SQL],
        execution_contracts=[_status_amount_contract()],
    )

    assert result["query_details"][0]["analysis_status"] == "available"
    assert result["query_details"][0]["missing_index_count"] == 1
    assert result["recommendations"][0]["equality_columns"] == ["status", "amount"]


@pytest.mark.asyncio
async def test_plan_with_no_compiled_query_plan_is_never_zero_hints() -> None:
    service = QueryIndexAnalysisService(
        _azure_shaped_executor(compiled_plan=EXEC_STUB_PLAN),
        SafeSqlValidator(),
    )

    with pytest.raises(RuntimeError, match="no compiled query plan"):
        await service.analyze_queries(
            "appdb",
            [TYPED_STATUS_AMOUNT_SQL],
            execution_contracts=[_status_amount_contract()],
        )


@pytest.mark.asyncio
async def test_typed_query_store_text_compiles_without_its_declaration_prefix() -> None:
    # Query Store keeps "(@p int)SELECT ..." for parameterized text. The
    # estimated plan DECLAREs the parameters itself, so the prefix left in
    # place would be a syntax error that fails the whole tool call.
    executor = MagicMock()
    executor.config.row_limit = 200
    executor.fetch_all = AsyncMock(return_value=[])
    executor.execute_session = AsyncMock(
        return_value=[
            [],
            [QueryResult(columns=("plan_xml",), rows=[{"plan_xml": MINIMAL_PLAN}])],
            [],
        ]
    )
    service = QueryIndexAnalysisService(executor, SafeSqlValidator())
    sql = "(@p int)SELECT name FROM sys.objects WHERE object_id = @p"
    contract = ParameterExecutionContract(
        sql_text=sql,
        bucket_id="common",
        parameters=(
            TypedParameter(
                name="@p",
                sql_type=SqlParameterType.from_sql("int"),
                value=5,
                provenance="synthetic",
            ),
        ),
        provenance="synthetic",
    )

    result = await service.analyze_queries(
        "appdb", [sql], execution_contracts=[contract]
    )

    compiled = executor.execute_session.await_args.args[1][1]
    assert compiled == (
        "DECLARE @p int;\n"
        "SELECT name FROM sys.objects WHERE object_id = @p\n"
        "OPTION (OPTIMIZE FOR (@p = 5))"
    )
    assert result["query_details"][0]["analysis_status"] == "available"
