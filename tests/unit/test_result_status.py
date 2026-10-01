from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.result_status import PRIMARY_ROW_KEYS
from azure_sql_mcp.result_status import RESULT_STATUS_VALUES
from azure_sql_mcp.result_status import ResultStatus
from azure_sql_mcp.result_status import apply_result_status
from azure_sql_mcp.result_status import status_payload
from azure_sql_mcp.server import AzureSqlMcpApplication

from tests.unit.test_server import make_config


def test_declared_primary_rows_that_are_empty_mean_a_true_negative() -> None:
    result = apply_result_status("get_deadlock_history", {"deadlocks": [], "deadlock_count": 0})

    assert result["result_status"] == "empty"
    assert "true negative" in result["result_status_reason"]


def test_incidental_empty_lists_never_turn_a_result_empty() -> None:
    # `warnings` is metadata; an empty warnings list beside real data is still data.
    result = apply_result_status(
        "get_resource_limits",
        {"governance_limits": {"max_cpu_percent": 100}, "warnings": []},
    )

    assert result["result_status"] == "ok"


def test_explicit_service_status_is_never_overwritten() -> None:
    payload = {
        "deadlocks": [],
        **status_payload(
            ResultStatus.PRECONDITION,
            "No deadlock capture session exists.",
            remediation="CREATE EVENT SESSION ...",
        ),
    }

    result = apply_result_status("get_deadlock_history", payload)

    assert result["result_status"] == "precondition"
    assert result["remediation"] == "CREATE EVENT SESSION ..."


class _EmptyExecutor:
    """Every read succeeds and returns nothing; no network is touched."""

    async def fetch_all(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return []

    async def _empty(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []


def _install_empty_executor(app: AzureSqlMcpApplication) -> None:
    fake = _EmptyExecutor()
    for name in (
        "fetch_all",
        "execute_batches",
        "execute_session",
        "execute_session_exactly_once",
        "execute_transaction_exactly_once",
        "execute_non_query",
        "execute_profiled_read_only",
    ):
        replacement = fake.fetch_all if name == "fetch_all" else fake._empty
        setattr(app.executor, name, replacement)


@pytest.fixture
def all_tools_app(tmp_path: Path) -> AzureSqlMcpApplication:
    app = AzureSqlMcpApplication(make_config(tmp_path, tool_groups=frozenset({ToolGroup.ALL})))
    _install_empty_executor(app)
    return app


@pytest.mark.asyncio
async def test_every_primary_row_key_names_a_registered_tool(
    all_tools_app: AzureSqlMcpApplication,
) -> None:
    registered = {tool.name for tool in await all_tools_app.mcp.list_tools()}

    assert set(PRIMARY_ROW_KEYS) <= registered


@pytest.mark.asyncio
async def test_every_argument_free_tool_reports_a_valid_status(
    all_tools_app: AzureSqlMcpApplication,
) -> None:
    tools = await all_tools_app.mcp.list_tools()
    checked: dict[str, str] = {}
    for tool in tools:
        if tool.inputSchema.get("required"):
            continue
        try:
            result = await all_tools_app.mcp._tool_manager.call_tool(tool.name, {})
        except Exception:
            # Tools that need catalog rows to proceed may raise a structured tool
            # error; errors are reported as errors, never as a silent empty result.
            continue
        assert isinstance(result, dict), tool.name
        assert result["result_status"] in RESULT_STATUS_VALUES, tool.name
        checked[tool.name] = result["result_status"]

    empty_reads = {name for name in checked if name in PRIMARY_ROW_KEYS}
    assert empty_reads, "expected at least one row tool to be exercised"
    for name in empty_reads:
        # No rows is never "ok": it is a true negative, or an explicit
        # precondition/unavailable/not_supported status from the service.
        assert checked[name] != "ok", name
    assert checked["get_wait_stats"] == "empty"
    assert checked["get_deadlock_history"] == "precondition"
    assert checked["check_runtime_status"] == "ok"


@pytest.mark.asyncio
async def test_wire_result_carries_the_status(all_tools_app: AzureSqlMcpApplication) -> None:
    content = await all_tools_app.mcp.call_tool("get_wait_stats", {})

    blocks = content[0] if isinstance(content, tuple) else content
    payload = json.loads(blocks[0].text)
    assert payload["result_status"] == "empty"
