from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters
from mcp.client.stdio import stdio_client

from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.server import AzureSqlMcpApplication

pytestmark = pytest.mark.skipif(
    not os.getenv("AZURE_SQL_SERVER"),
    reason="AZURE_SQL_SERVER not set",
)


def _server_env(app: AzureSqlMcpApplication, database_name: str) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "AZURE_SQL_SERVER": app.config.server,
            "AZURE_SQL_DEFAULT_DATABASE": database_name,
            "AZURE_SQL_ALLOWED_DATABASES": ",".join(app.config.allowed_databases),
            "AZURE_SQL_AUTH_MODE": app.config.auth_mode.value,
            "AZURE_SQL_ACCESS_MODE": app.config.access_mode.value,
            "AZURE_SQL_QUERY_TIMEOUT_SECONDS": str(app.config.query_timeout_seconds),
            "AZURE_SQL_ROW_LIMIT": str(app.config.row_limit),
            "AZURE_SQL_POOL_SIZE": str(app.config.pool_size),
            "AZURE_SQL_MAX_RETRIES": str(app.config.max_retries),
            "AZURE_SQL_TOOL_TIMEOUT_SECONDS": str(app.config.tool_timeout_seconds),
            "AZURE_SQL_TRANSPORT": "stdio",
            "AZURE_SQL_LOG_LEVEL": "WARNING",
            "AZURE_SQL_LOG_FORMAT": app.config.log_format,
        }
    )

    optional_values = {
        "AZURE_SQL_USERNAME": app.config.username,
        "AZURE_SQL_PASSWORD": app.config.password,
        "AZURE_TENANT_ID": app.config.tenant_id,
        "AZURE_CLIENT_ID": app.config.client_id,
        "AZURE_CLIENT_SECRET": app.config.client_secret,
        "AZURE_SQL_TRUST_SERVER_CERTIFICATE": (
            "true" if app.config.trust_server_certificate else None
        ),
    }
    for key, value in optional_values.items():
        if value:
            env[key] = value

    return env


def _tool_payload(result: Any) -> Any:
    assert result.isError is False
    assert len(result.content) == 1
    return json.loads(result.content[0].text)


def _rows(payload: Any) -> Any:
    """List-returning tools are wrapped by _format_response as {'result': [...]}."""
    if isinstance(payload, dict) and set(payload) == {"result"}:
        return payload["result"]
    return payload


def _resource_payload(result: Any) -> Any:
    assert len(result.contents) == 1
    return json.loads(result.contents[0].text)


@pytest.mark.asyncio
async def test_mcp_stdio_end_to_end(
    integration_app: AzureSqlMcpApplication,
    integration_database_name: str,
    prepared_test_schema: str,
) -> None:
    if (
        integration_app.config.auth_mode == AuthMode.SQL_PASSWORD
        and integration_app.config.server.lower() in {"localhost", "127.0.0.1", "sqlserver"}
        and not integration_app.config.trust_server_certificate
    ):
        pytest.skip(
            "stdio server integration test against local SQL Server requires "
            "AZURE_SQL_TRUST_SERVER_CERTIFICATE=true (self-signed certificate)."
        )

    project_root = Path(__file__).resolve().parents[2]
    server = StdioServerParameters(
        command=str(project_root / ".venv/bin/python"),
        args=["-m", "azure_sql_mcp.server"],
        env=_server_env(integration_app, integration_database_name),
        cwd=project_root,
    )

    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            init = await session.initialize()
            assert init.serverInfo.name == "azure-sql-mcp"

            tools = await session.list_tools()
            assert {
                "list_databases",
                "check_capabilities",
                "list_schemas",
                "list_objects",
                "search_objects",
                "get_object_details",
                "get_dependencies",
                "get_table_stats",
                "capture_schema_snapshot",
                "compare_schemas",
                "generate_migration_script",
                "get_active_sessions",
                "execute_sql",
                "explain_query",
                "get_top_queries",
                "analyze_query_indexes",
                "analyze_workload_indexes",
                "analyze_index_recommendations",
                "analyze_db_health",
            }.issubset({tool.name for tool in tools.tools})

            prompts = await session.list_prompts()
            assert {
                "analyze-slow-queries",
                "review-index-health",
                "explore-schema",
                "compare-schemas",
                "troubleshoot-performance",
            }.issubset({prompt.name for prompt in prompts.prompts})

            resources = await session.list_resource_templates()
            assert {
                "azuresql://{database}/schemas",
                "azuresql://{database}/{schema}/tables",
                "azuresql://{database}/{schema}/{table}",
                "azuresql://{database}/{schema}/views",
                "azuresql://{database}/{schema}/procedures",
            }.issubset({resource.uriTemplate for resource in resources.resourceTemplates})

            databases = _tool_payload(await session.call_tool("list_databases", {}))
            assert integration_database_name in databases["allowed_databases"]

            capabilities = _tool_payload(
                await session.call_tool(
                    "check_capabilities",
                    {"database_name": integration_database_name},
                )
            )
            capability_checks = capabilities["checks"]

            schemas = _tool_payload(
                await session.call_tool(
                    "list_schemas",
                    {"database_name": integration_database_name},
                )
            )
            assert any(row["schema_name"] == prepared_test_schema for row in _rows(schemas))

            tables = _tool_payload(
                await session.call_tool(
                    "list_objects",
                    {
                        "database_name": integration_database_name,
                        "schema_name": prepared_test_schema,
                        "object_type": "table",
                    },
                )
            )
            assert {row["object_name"] for row in _rows(tables)} >= {"Customers", "Orders"}

            views = _tool_payload(
                await session.call_tool(
                    "list_objects",
                    {
                        "database_name": integration_database_name,
                        "schema_name": prepared_test_schema,
                        "object_type": "view",
                    },
                )
            )
            assert {row["object_name"] for row in _rows(views)} >= {"vw_OrderSummary"}

            procedures = _tool_payload(
                await session.call_tool(
                    "list_objects",
                    {
                        "database_name": integration_database_name,
                        "schema_name": prepared_test_schema,
                        "object_type": "procedure",
                    },
                )
            )
            assert {row["object_name"] for row in _rows(procedures)} >= {"usp_GetOrders"}

            matches = _tool_payload(
                await session.call_tool(
                    "search_objects",
                    {
                        "database_name": integration_database_name,
                        "pattern": "%Order%",
                    },
                )
            )
            assert {row["object_name"] for row in _rows(matches)} >= {
                "Orders",
                "vw_OrderSummary",
                "usp_GetOrders",
            }

            details = _tool_payload(
                await session.call_tool(
                    "get_object_details",
                    {
                        "database_name": integration_database_name,
                        "schema_name": prepared_test_schema,
                        "object_name": "Orders",
                        "object_type": "table",
                    },
                )
            )
            assert {column["column_name"] for column in details["columns"]} >= {
                "OrderId",
                "CustomerId",
                "TotalAmount",
            }

            dependencies = _tool_payload(
                await session.call_tool(
                    "get_dependencies",
                    {
                        "database_name": integration_database_name,
                        "schema_name": prepared_test_schema,
                        "object_name": "vw_OrderSummary",
                    },
                )
            )
            depends_on = {
                (row["schema"], row["object"])
                for row in dependencies["depends_on"]
            }
            assert (prepared_test_schema, "Orders") in depends_on
            assert (prepared_test_schema, "Customers") in depends_on

            stats = _tool_payload(
                await session.call_tool(
                    "get_table_stats",
                    {
                        "database_name": integration_database_name,
                        "schema_name": prepared_test_schema,
                    },
                )
            )
            stats_by_table = {row["table_name"]: row for row in _rows(stats)}
            assert stats_by_table["Orders"]["approximate_row_count"] >= 2

            active_sessions = _tool_payload(
                await session.call_tool(
                    "get_active_sessions",
                    {"database_name": integration_database_name},
                )
            )
            assert "sessions" in active_sessions

            sql_result = _tool_payload(
                await session.call_tool(
                    "execute_sql",
                    {
                        "database_name": integration_database_name,
                        "sql": f"SELECT COUNT(*) AS order_count FROM [{prepared_test_schema}].[Orders]",
                    },
                )
            )
            assert sql_result["rows"][0]["order_count"] >= 2

            health = _tool_payload(
                await session.call_tool(
                    "analyze_db_health",
                    {
                        "database_name": integration_database_name,
                        "health_type": "all",
                    },
                )
            )
            assert "checks" in health
            assert "index" in health["checks"]

            recommendations = _tool_payload(
                await session.call_tool(
                    "analyze_index_recommendations",
                    {"database_name": integration_database_name},
                )
            )
            assert "missing_indexes" in recommendations
            assert "automatic_tuning_recommendations" in recommendations

            snapshot = _tool_payload(
                await session.call_tool(
                    "capture_schema_snapshot",
                    {
                        "database_name": integration_database_name,
                        "schema_filter": prepared_test_schema,
                    },
                )
            )
            assert f"{prepared_test_schema}.vw_OrderSummary" in snapshot["views"]

            comparison = _tool_payload(
                await session.call_tool(
                    "compare_schemas",
                    {
                        "source_database": integration_database_name,
                        "target_database": integration_database_name,
                        "schema_filter": prepared_test_schema,
                    },
                )
            )
            assert comparison["difference_count"] == 0

            migration = _tool_payload(
                await session.call_tool(
                    "generate_migration_script",
                    {
                        "source_database": integration_database_name,
                        "target_database": integration_database_name,
                        "schema_filter": prepared_test_schema,
                    },
                )
            )
            assert "BEGIN TRANSACTION;" in migration["migration_script"]

            schema_resource = _resource_payload(
                await session.read_resource(
                    f"azuresql://{integration_database_name}/schemas"
                )
            )
            assert any(row["schema_name"] == prepared_test_schema for row in schema_resource)

            table_resource = _resource_payload(
                await session.read_resource(
                    f"azuresql://{integration_database_name}/{prepared_test_schema}/tables"
                )
            )
            assert {row["object_name"] for row in table_resource} >= {"Customers", "Orders"}

            detail_resource = _resource_payload(
                await session.read_resource(
                    f"azuresql://{integration_database_name}/{prepared_test_schema}/Orders"
                )
            )
            assert detail_resource["object_name"] == "Orders"

            view_resource = _resource_payload(
                await session.read_resource(
                    f"azuresql://{integration_database_name}/{prepared_test_schema}/views"
                )
            )
            assert {row["object_name"] for row in view_resource} >= {"vw_OrderSummary"}

            procedure_resource = _resource_payload(
                await session.read_resource(
                    f"azuresql://{integration_database_name}/{prepared_test_schema}/procedures"
                )
            )
            assert {row["object_name"] for row in procedure_resource} >= {"usp_GetOrders"}

            analyze_prompt = await session.get_prompt(
                "analyze-slow-queries",
                {
                    "database_name": integration_database_name,
                    "window_minutes": "30",
                },
            )
            assert "get_top_queries" in analyze_prompt.messages[0].content.text

            explore_prompt = await session.get_prompt(
                "explore-schema",
                {
                    "database_name": integration_database_name,
                    "schema_name": prepared_test_schema,
                },
            )
            assert prepared_test_schema in explore_prompt.messages[0].content.text

            compare_prompt = await session.get_prompt(
                "compare-schemas",
                {
                    "source_database": integration_database_name,
                    "target_database": integration_database_name,
                },
            )
            assert "generate_migration_script" in compare_prompt.messages[0].content.text

            troubleshoot_prompt = await session.get_prompt(
                "troubleshoot-performance",
                {"database_name": integration_database_name},
            )
            assert "get_active_sessions" in troubleshoot_prompt.messages[0].content.text

            if capability_checks["estimated_plan"]["available"]:
                estimated_plan = _tool_payload(
                    await session.call_tool(
                        "explain_query",
                        {
                            "database_name": integration_database_name,
                            "sql": (
                                f"SELECT TOP 1 * FROM [{prepared_test_schema}].[Orders] "
                                "ORDER BY [OrderId]"
                            ),
                            "analyze": False,
                        },
                    )
                )
                assert estimated_plan["summary"]["statement_count"] >= 1

                query_index_analysis = _tool_payload(
                    await session.call_tool(
                        "analyze_query_indexes",
                        {
                            "database_name": integration_database_name,
                            "queries": [
                                f"SELECT * FROM [{prepared_test_schema}].[Orders] WHERE [CustomerId] = 1"
                            ],
                        },
                    )
                )
                assert "recommendations" in query_index_analysis

                hypothetical = await session.call_tool(
                    "explain_query",
                    {
                        "database_name": integration_database_name,
                        "sql": (
                            f"SELECT * FROM [{prepared_test_schema}].[Orders] "
                            "WHERE [CustomerId] = 1"
                        ),
                        "analyze": False,
                        "hypothetical_indexes": [
                            {
                                "schema": prepared_test_schema,
                                "table": "Orders",
                                "columns": ["CustomerId"],
                            }
                        ],
                    },
                )
                if hypothetical.isError:
                    pytest.fail(
                        "explain_query returned an MCP error for hypothetical indexes."
                    )
                hypothetical_payload = json.loads(hypothetical.content[0].text)
                assert hypothetical_payload["summary"]["statement_count"] >= 1

            if capability_checks["actual_plan"]["available"]:
                actual_plan = _tool_payload(
                    await session.call_tool(
                        "explain_query",
                        {
                            "database_name": integration_database_name,
                            "sql": (
                                f"SELECT TOP 1 * FROM [{prepared_test_schema}].[Orders] "
                                "ORDER BY [OrderId]"
                            ),
                            "analyze": True,
                        },
                    )
                )
                assert actual_plan["summary"]["statement_count"] >= 1

            query_store_enabled = (
                capability_checks["query_store"]["available"]
                and capability_checks["query_store"]["detail"].get("enabled")
            )
            if query_store_enabled:
                top_queries = _tool_payload(
                    await session.call_tool(
                        "get_top_queries",
                        {
                            "database_name": integration_database_name,
                            "sort_by": "resource_blend",
                            "window_minutes": 60,
                            "limit": 5,
                        },
                    )
                )
                assert "rows" in top_queries

                workload = _tool_payload(
                    await session.call_tool(
                        "analyze_workload_indexes",
                        {
                            "database_name": integration_database_name,
                            "window_minutes": 60,
                            "top_n": 5,
                        },
                    )
                )
                assert "recommendations" in workload
