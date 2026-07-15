from __future__ import annotations

import os

import pytest

from azure_sql_mcp.server import AzureSqlMcpApplication

pytestmark = pytest.mark.skipif(
    not os.getenv("AZURE_SQL_SERVER"),
    reason="AZURE_SQL_SERVER not set",
)


@pytest.mark.asyncio
async def test_search_objects_dependencies_and_table_stats(
    integration_app: AzureSqlMcpApplication,
    integration_database_name: str,
    prepared_test_schema: str,
) -> None:
    matches = await integration_app.introspection.search_objects(
        integration_database_name,
        "%Order%",
    )
    matched_names = {row["object_name"] for row in matches}
    assert matched_names >= {"Orders", "vw_OrderSummary", "usp_GetOrders"}

    dependencies = await integration_app.introspection.get_dependencies(
        integration_database_name,
        prepared_test_schema,
        "vw_OrderSummary",
    )
    depends_on = {
        (row["schema"], row["object"])
        for row in dependencies["depends_on"]
    }
    assert (prepared_test_schema, "Orders") in depends_on
    assert (prepared_test_schema, "Customers") in depends_on

    stats = await integration_app.introspection.get_table_stats(
        integration_database_name,
        prepared_test_schema,
    )
    stats_by_table = {row["table_name"]: row for row in stats}
    assert stats_by_table["Orders"]["approximate_row_count"] >= 2
    assert stats_by_table["Customers"]["approximate_row_count"] >= 2


@pytest.mark.asyncio
async def test_execute_safe_sql_against_prepared_objects(
    integration_app: AzureSqlMcpApplication,
    integration_database_name: str,
    prepared_test_schema: str,
) -> None:
    result = await integration_app._execute_safe_sql(
        integration_database_name,
        (
            f"SELECT COUNT(*) AS order_count "
            f"FROM [{prepared_test_schema}].[Orders]"
        ),
    )

    assert result["database_name"] == integration_database_name
    assert result["rows"][0]["order_count"] >= 2
