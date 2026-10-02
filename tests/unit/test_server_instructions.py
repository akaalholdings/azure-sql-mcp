from __future__ import annotations

import re
from pathlib import Path

import pytest

from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.server_instructions import SERVER_INSTRUCTIONS
from tests.unit.test_server import make_config


def test_initialize_carries_the_azure_instructions(tmp_path: Path) -> None:
    app = AzureSqlMcpApplication(make_config(tmp_path))

    assert app.mcp.instructions == SERVER_INSTRUCTIONS
    for status in ("ok", "empty", "unavailable", "not_supported", "precondition"):
        assert f"- {status}:" in SERVER_INSTRUCTIONS


def test_blocked_agents_report_once_and_stop_retrying_that_call() -> None:
    blocked = SERVER_INSTRUCTIONS.split("## When you are blocked", 1)[1].split("##", 1)[0]

    assert "report_stuck" in blocked
    assert "stop retrying that exact call" in blocked
    # A delayed retry of a failure the server marks transient is correct.
    assert "failure_diagnostic.transient" in blocked
    # Without these the report cannot be matched to a skill release or a tool.
    for argument in ("skill", "skill_version", "last_tool"):
        assert argument in " ".join(blocked.split())


@pytest.mark.asyncio
async def test_every_tool_named_in_the_instructions_exists(tmp_path: Path) -> None:
    app = AzureSqlMcpApplication(make_config(tmp_path, tool_groups=frozenset({ToolGroup.ALL})))
    registered = {tool.name for tool in await app.mcp.list_tools()}
    named = set(re.findall(r"\b([a-z]+(?:_[a-z]+)+)\b", SERVER_INSTRUCTIONS))
    tool_prefixes = (
        "get_", "check_", "list_", "start_", "add_", "benchmark_", "finalize_",
        "review_", "detect_", "explain_", "collect_", "plan_", "prepare_", "analyze_",
    )
    parameter_suffixes = ("_id", "_utc", "_minutes", "_seconds", "_pct", "_percent")
    candidates = {
        name
        for name in named
        if name.startswith(tool_prefixes) and not name.endswith(parameter_suffixes)
    }

    missing = sorted(candidates - registered)
    assert missing == []
