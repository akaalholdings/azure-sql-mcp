from __future__ import annotations

import os

import pytest

from azure_sql_mcp.server import AzureSqlMcpApplication

pytestmark = pytest.mark.skipif(
    not os.getenv("AZURE_SQL_SERVER"),
    reason="AZURE_SQL_SERVER not set",
)


@pytest.mark.asyncio
async def test_list_schemas_and_object_details(
    integration_app: AzureSqlMcpApplication,
    integration_database_name: str,
    prepared_test_schema: str,
) -> None:
    schemas = await integration_app.introspection.list_schemas(integration_database_name)
    assert any(row["schema_name"] == prepared_test_schema for row in schemas)

    tables = await integration_app.introspection.list_objects(
        integration_database_name,
        prepared_test_schema,
        "table",
    )
    assert {row["object_name"] for row in tables} >= {"Customers", "Orders"}

    views = await integration_app.introspection.list_objects(
        integration_database_name,
        prepared_test_schema,
        "view",
    )
    assert {row["object_name"] for row in views} >= {"vw_OrderSummary"}

    procedures = await integration_app.introspection.list_objects(
        integration_database_name,
        prepared_test_schema,
        "procedure",
    )
    assert {row["object_name"] for row in procedures} >= {"usp_GetOrders"}

    details = await integration_app.introspection.get_object_details(
        integration_database_name,
        prepared_test_schema,
        "Orders",
        "table",
    )
    assert {column["column_name"] for column in details["columns"]} >= {
        "OrderId",
        "CustomerId",
        "TotalAmount",
    }
    assert any(
        index["index_name"] == f"IX_{prepared_test_schema}_Orders_CustomerId"
        for index in details["indexes"]
    )
