from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from types import SimpleNamespace

import pytest

from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import IndexKeyColumn
from azure_sql_mcp.param_binding import ParameterExecutionContract
from azure_sql_mcp.param_binding import SqlParameterType
from azure_sql_mcp.param_binding import TypedParameter
from azure_sql_mcp.query_index_analysis import QueryIndexAnalysisService
from azure_sql_mcp.safe_sql import SafeSqlValidator


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
            [QueryResult(columns=("plan_xml",), rows=[{"plan_xml": "<ShowPlanXML/>"}])],
            [],
        ]
    )
    service = QueryIndexAnalysisService(executor, SafeSqlValidator())

    result = await service._get_estimated_plan("appdb", "SELECT 1")

    assert result == "<ShowPlanXML/>"
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
            [QueryResult(columns=("plan_xml",), rows=[{"plan_xml": "<ShowPlanXML/>"}])],
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
    assert plan_call.args[1][1] == contract.sp_executesql_sql
    assert plan_call.kwargs["statement_params"] == [
        None,
        contract.sp_executesql_values,
        None,
    ]
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
async def test_analyze_queries_propagates_contract_input_sizes_to_plan_statement() -> None:
    executor = MagicMock()
    executor.config.row_limit = 200
    executor.fetch_all = AsyncMock(return_value=[])
    executor.execute_session = AsyncMock(
        return_value=[
            [],
            [QueryResult(columns=("plan_xml",), rows=[{"plan_xml": "<ShowPlanXML/>"}])],
            [],
        ]
    )
    service = QueryIndexAnalysisService(executor, SafeSqlValidator())
    service._extract_missing_indexes = MagicMock(return_value=[])
    sql = "SELECT name FROM dbo.Widgets WHERE WidgetId = @WidgetId"
    base_contract = ParameterExecutionContract(
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
    contract = SimpleNamespace(
        sql_text=base_contract.sql_text,
        parameters=base_contract.parameters,
        sp_executesql_sql=base_contract.sp_executesql_sql,
        sp_executesql_values=base_contract.sp_executesql_values,
        sp_executesql_input_sizes=("stmt-size", "params-size", "value-size"),
    )

    await service.analyze_queries("appdb", [sql], execution_contracts=[contract])

    plan_call = executor.execute_session.await_args
    assert plan_call.kwargs["statement_input_sizes"] == [
        None,
        contract.sp_executesql_input_sizes,
        None,
    ]


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
            [QueryResult(columns=("plan_xml",), rows=[{"plan_xml": "<ShowPlanXML/>"}])],
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
    assert "SET @P1 = 1" in session_batch[1]
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
            [QueryResult(columns=("plan_xml",), rows=[{"plan_xml": "<ShowPlanXML/>"}])],
            [],
        ]
    )

    service = QueryIndexAnalysisService(executor, SafeSqlValidator())
    service._extract_missing_indexes = MagicMock(return_value=[])

    result = await service.analyze_workload("appdb", window_minutes=60, top_n=5)

    assert result["skipped_non_select"] == 2
    assert result["queries_analyzed"] == 1
    assert all("error" not in q for q in result["analyzed_queries"])
