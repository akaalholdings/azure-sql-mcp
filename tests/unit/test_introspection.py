from __future__ import annotations

import pytest

from azure_sql_mcp.introspection import IntrospectionService


class StubExecutor:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def fetch_all(self, database_name: str, query: str, params=None):
        self.calls.append((database_name, query, params))
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_get_dependencies_uses_supported_dependency_query() -> None:
    executor = StubExecutor(
        [
            [
                {
                    "referenced_schema": "mcp_integration",
                    "referenced_object": "Orders",
                    "referenced_column": "CustomerId",
                    "referencing_class_desc": "OBJECT_OR_COLUMN",
                }
            ],
            [
                {
                    "referencing_schema": "mcp_integration",
                    "referencing_object": "usp_GetOrders",
                    "referencing_type": "SQL_STORED_PROCEDURE",
                }
            ],
        ]
    )
    service = IntrospectionService(executor)

    payload = await service.get_dependencies(
        "appdb",
        "mcp_integration",
        "vw_OrderSummary",
    )

    depends_on_query = executor.calls[0][1]
    assert "referenced_minor_name" not in depends_on_query
    assert "LEFT JOIN sys.columns AS c" in depends_on_query
    assert payload["depends_on"][0]["column"] == "CustomerId"
    assert payload["depended_on_by"][0]["object"] == "usp_GetOrders"


@pytest.mark.asyncio
async def test_get_table_stats_aggregates_partitions_and_counts_indexes_independently() -> None:
    executor = StubExecutor([[]])
    service = IntrospectionService(executor)

    await service.get_table_stats("appdb", schema_name="dbo")

    query = executor.calls[0][1]
    assert "WITH partition_storage AS" in query
    assert "au.type IN (1, 3)" in query
    assert "au.container_id = p.hobt_id" in query
    assert "au.type = 2" in query
    assert "au.container_id = p.partition_id" in query
    assert "WHEN index_id IN (0, 1) THEN partition_rows" in query
    assert "WHEN index_id IN (0, 1) THEN used_pages" in query
    assert "WHEN index_id > 1 THEN used_pages" in query
    assert "i.index_id > 1" in query
    assert "i.is_hypothetical = 0" in query
    assert "COUNT(DISTINCT i.index_id)" not in query
    assert "SUM(p.rows)" not in query
    assert executor.calls[0][2] == ["dbo", "dbo"]
