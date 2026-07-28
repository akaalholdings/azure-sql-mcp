from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters
from mcp.client.stdio import stdio_client


LEARNING_TOOLS = {
    "record_decision",
    "review_decision",
    "propose_lesson",
    "recall_lessons",
    "list_learning_candidates",
    "create_handoff",
    "get_handoff",
    "resolve_handoff",
}


def _env(tmp_path: Path) -> dict[str, str]:
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
            "AZURE_SQL_TOOL_GROUPS": "all",
            "AZURE_SQL_TRANSPORT": "stdio",
            "AZURE_SQL_LOG_LEVEL": "WARNING",
            "AZURE_SQL_AUDIT_DIR": str(tmp_path / "audit"),
            "AZURE_SQL_PERFORMANCE_STATE_DIR": str(tmp_path / "state"),
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
        }
    )
    return env


def _payload(result):
    assert result.isError is False
    assert len(result.content) == 1
    return json.loads(result.content[0].text)


async def _lifecycle(server: StdioServerParameters):
    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            initialized = await session.initialize()
            assert initialized.serverInfo.name == "azure-sql-mcp"
            listed = await session.list_tools()
            tool_names = {tool.name for tool in listed.tools}
            assert LEARNING_TOOLS <= tool_names

            resources = await session.list_resource_templates()
            assert not any(
                resource.uriTemplate.startswith("azuresql-learning://")
                for resource in resources.resourceTemplates
            )

            runtime = _payload(await session.call_tool("check_runtime_status", {}))
            recalled = _payload(
                await session.call_tool(
                    "recall_lessons",
                    {
                        "skill": "sql-optimizer",
                        "skill_version": "2.3.0",
                        "runtime_compatibility_fingerprint": runtime[
                            "runtime_compatibility_fingerprint"
                        ],
                        "tool_schema_fingerprint": runtime["tool_schema_fingerprint"],
                        "sanitized_config_fingerprint": runtime[
                            "sanitized_config_fingerprint"
                        ],
                        "database_name": "appdb",
                    },
                )
            )
            assert recalled == {"lessons": [], "count": 0, "max_results": 3}
            return runtime


@pytest.mark.asyncio
async def test_learning_stdio_lifecycle_and_restart_stability(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    server = StdioServerParameters(
        command=sys.executable,
        args=["-m", "azure_sql_mcp.server"],
        env=_env(tmp_path),
        cwd=project_root,
    )

    first = await _lifecycle(server)
    second = await _lifecycle(server)

    assert first["runtime_compatibility_fingerprint"] == second[
        "runtime_compatibility_fingerprint"
    ]
    assert first["runtime_fingerprint"] != second["runtime_fingerprint"]
