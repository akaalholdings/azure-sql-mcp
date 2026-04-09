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
