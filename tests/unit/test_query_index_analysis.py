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
    )


@pytest.mark.asyncio
async def test_analyze_workload_accepts_query_store_parameterized_sql_prefix() -> None:
    executor = MagicMock()
    executor.fetch_all = AsyncMock()
    # Top workload query.
    executor.fetch_all.side_effect = [
        [
            {
                "query_id": 1,
                "plan_id": 10,
                "query_sql_text": "(@P1 nvarchar(30))SELECT name FROM sys.objects WHERE type = @P1",
                "executions": 3,
                "total_cpu_us": 1000,
                "total_duration_us": 2000,
                "total_logical_io_reads": 50,
            }
        ],
        # Existing indexes query
        [],
        # DMV recommendations query
        [],
    ]
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
