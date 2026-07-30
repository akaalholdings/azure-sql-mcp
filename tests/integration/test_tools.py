from __future__ import annotations

import os

import pytest

from azure_sql_mcp.param_binding import ParameterExecutionContract
from azure_sql_mcp.param_binding import SqlParameterType
from azure_sql_mcp.param_binding import TypedParameter
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
    orders = stats_by_table["Orders"]
    assert orders["approximate_row_count"] == 2
    assert orders["index_count"] == 2
    assert float(orders["index_size_mb"]) > 0
    assert abs(
        float(orders["used_size_mb"])
        - float(orders["data_size_mb"])
        - float(orders["index_size_mb"])
    ) <= 0.02
    assert stats_by_table["Customers"]["approximate_row_count"] == 2
    assert stats_by_table["Customers"]["index_count"] == 0


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


@pytest.mark.asyncio
async def test_parameterized_execution_pins_only_sp_executesql_controls(
    integration_app: AzureSqlMcpApplication,
    integration_database_name: str,
    prepared_test_schema: str,
) -> None:
    probe = ParameterExecutionContract(
        sql_text="SELECT @P1 AS probe_value",
        bucket_id="driver-probe",
        parameters=(
            TypedParameter(
                name="@P1",
                sql_type=SqlParameterType.from_sql("int"),
                value=13,
                provenance="integration_test",
            ),
        ),
        provenance="integration_test",
    )

    estimated_probe = await integration_app.plans.explain_parameterized_query(
        integration_database_name,
        probe,
        analyze=False,
    )
    actual_probe = await integration_app.plans.explain_parameterized_query(
        integration_database_name,
        probe,
        analyze=True,
    )

    assert estimated_probe.plan_kind == "estimated"
    assert actual_probe.plan_kind == "actual"
    assert actual_probe.query_executed is True

    index_name = f"IX_{prepared_test_schema}_Orders_ExternalCode"
    varchar_sql = (
        f"SELECT [OrderId] FROM [{prepared_test_schema}].[Orders] "
        f"WITH (FORCESEEK, INDEX([{index_name}])) "
        "WHERE [ExternalCode] = @P1"
    )
    varchar_probe = ParameterExecutionContract(
        sql_text=varchar_sql,
        bucket_id="varchar-seek-probe",
        parameters=(
            TypedParameter(
                name="@P1",
                sql_type=SqlParameterType.from_sql("varchar(20)"),
                value="A100",
                provenance="integration_test",
            ),
        ),
        provenance="integration_test",
    )

    varchar_plan = await integration_app.plans.explain_parameterized_query(
        integration_database_name,
        varchar_probe,
        analyze=True,
    )

    assert index_name in varchar_plan.raw_xml
    assert 'PhysicalOp="Index Seek"' in varchar_plan.raw_xml
    assert "CONVERT_IMPLICIT" not in varchar_plan.raw_xml.upper()
