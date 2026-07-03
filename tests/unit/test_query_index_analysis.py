from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.query_index_analysis import QueryIndexAnalysisService
from azure_sql_mcp.safe_sql import SafeSqlValidator


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
    assert result["analyzed_queries"][0]["missing_index_count"] == 0
    assert "error" not in result["analyzed_queries"][0]

    # The plan session must have received an executable DECLARE-bound batch
    # (sqlglot normalizes the type spelling, e.g. int -> INTEGER).
    session_batch = executor.execute_session.await_args.args[1]
    assert session_batch[1].upper().startswith("DECLARE @P1 INT")
    assert "SET @P1 = 1" in session_batch[1]
    assert "(@P1 int)SELECT" not in session_batch[1]


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
