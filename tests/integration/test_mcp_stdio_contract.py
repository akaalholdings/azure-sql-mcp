from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters
from mcp.client.stdio import stdio_client


def _isolated_server_env(tmp_path: Path) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("AZURE_SQL_", "AZURE_CLIENT_", "AZURE_TENANT_"))
    }
    env.update(
        {
            "AZURE_SQL_SERVER": "server.database.windows.net",
            "AZURE_SQL_DEFAULT_DATABASE": "appdb",
            "AZURE_SQL_ALLOWED_DATABASES": "appdb",
            "AZURE_SQL_AUTH_MODE": "entra-default",
            "AZURE_SQL_ACCESS_MODE": "restricted",
            "AZURE_SQL_WRITE_POLICY": "disabled",
            "AZURE_SQL_PROFILE": "optimizer",
            "AZURE_SQL_TOOL_GROUPS": "core,performance",
            "AZURE_SQL_TRANSPORT": "stdio",
            "AZURE_SQL_LOG_LEVEL": "WARNING",
            "AZURE_SQL_AUDIT_DIR": str(tmp_path / "audit"),
            "AZURE_SQL_PERFORMANCE_STATE_DIR": str(tmp_path / "state"),
        }
    )
    return env


def _payload(result):
    assert result.isError is False
    assert len(result.content) == 1
    return json.loads(result.content[0].text)


@pytest.mark.asyncio
async def test_stdio_publishes_and_executes_optimizer_contracts(
    tmp_path: Path,
) -> None:
    project_root = Path(__file__).resolve().parents[2]
    server = StdioServerParameters(
        command=sys.executable,
        args=["-m", "azure_sql_mcp.server"],
        env=_isolated_server_env(tmp_path),
        cwd=project_root,
    )

    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            initialized = await session.initialize()
            assert initialized.serverInfo.name == "azure-sql-mcp"

            listed = await session.list_tools()
            tools = {tool.name: tool for tool in listed.tools}
            assert "check_equivalence_preflight" in tools
            assert tools["start_performance_case"].inputSchema["properties"][
                "objective"
            ]["enum"] == [
                "elapsed_time",
                "cpu",
                "logical_reads",
                "physical_reads",
            ]
            assert "headline" in tools["finalize_tuning_session"].outputSchema[
                "properties"
            ]

            runtime = _payload(
                await session.call_tool("check_runtime_status", {})
            )
            assert runtime["package_version"] == "2.2.1"
            assert runtime["tool_groups"] == ["core", "performance"]
            assert "check_equivalence_preflight" in runtime["tool_names"]

            preflight = _payload(
                await session.call_tool(
                    "check_equivalence_preflight",
                    {
                        "sql": "SELECT GETDATE() AS captured_at",
                        "database_name": "appdb",
                    },
                )
            )
            assert preflight["classification"] == "proof_contract_required"
            assert preflight["functions"][0]["function"] == "GETDATE"
            assert preflight["headline"]["risk_count"] == 1

            invalid = await session.call_tool(
                "check_runtime_status",
                {"unexpected": "caller-secret-value"},
            )
            assert invalid.isError is True
            invalid_text = invalid.content[0].text
            assert json.loads(invalid_text)["code"] == "invalid_arguments"
            assert "caller-secret-value" not in invalid_text
            assert "pydantic" not in invalid_text.casefold()
