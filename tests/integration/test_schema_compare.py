from __future__ import annotations

import os

import pytest

from azure_sql_mcp.schema_compare import SchemaCompareService
from azure_sql_mcp.server import AzureSqlMcpApplication

pytestmark = pytest.mark.skipif(
    not os.getenv("AZURE_SQL_SERVER"),
    reason="AZURE_SQL_SERVER not set",
)


@pytest.mark.asyncio
async def test_capture_compare_and_generate_script_for_prepared_schema(
    integration_app: AzureSqlMcpApplication,
    integration_database_name: str,
    prepared_test_schema: str,
) -> None:
    service = SchemaCompareService(integration_app.executor)

    snapshot = await service.capture_schema_snapshot(
        integration_database_name,
        prepared_test_schema,
    )
    assert f"{prepared_test_schema}.Orders" in snapshot["tables"]
    assert f"{prepared_test_schema}.Customers" in snapshot["tables"]
    assert f"{prepared_test_schema}.vw_OrderSummary" in snapshot["views"]
    assert snapshot["tables"][f"{prepared_test_schema}.Orders"]["schema_name"] == prepared_test_schema

    comparison = await service.compare_schemas(
        integration_database_name,
        integration_database_name,
        prepared_test_schema,
    )
    assert comparison["difference_count"] == 0
    assert comparison["differences"] == {}
    assert comparison["summary"]["by_type"] == {}
    assert comparison["summary"]["by_category"] == {}

    script = await service.generate_migration_script(
        integration_database_name,
        integration_database_name,
        prepared_test_schema,
    )
    assert "SET XACT_ABORT ON;" in script
    assert "BEGIN TRANSACTION;" in script
    assert "No schema changes detected." in script
    assert script.rstrip().endswith("COMMIT TRANSACTION;")

