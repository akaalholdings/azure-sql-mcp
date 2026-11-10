"""Profile-aware next_tools (design W7 step 2, review item agent-ergonomics-12).

diagnose_database tells the agent which tool to call next. In the optimizer
profile six of those tools are not registered, so the agent's next call fails
in the middle of an incident. The hint must name only tools the profile has,
and say where the others live.
"""

from __future__ import annotations

from typing import Any

from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import PROFILE_TOOL_ALLOWLISTS
from azure_sql_mcp.tool_contracts import annotate_next_tools
from azure_sql_mcp.tool_contracts import profiles_exposing


def _payload() -> dict[str, Any]:
    return {
        "findings": [
            {
                "code": "log_rate_governor_waits",
                "next_tools": [
                    {"tool": "get_resource_limits", "arguments": {}},
                    {"tool": "get_top_queries", "arguments": {"sort_by": "cpu"}},
                ],
            },
            {
                "code": "persistent_version_store_large",
                "next_tools": ["get_open_transactions", "get_active_sessions"],
            },
        ],
        "gaps": [],
    }


def _optimizer_has(tool_name: str) -> bool:
    return tool_name in PROFILE_TOOL_ALLOWLISTS[McpProfile.OPTIMIZER]


def test_next_tools_name_only_tools_the_profile_has() -> None:
    result = annotate_next_tools(_payload(), _optimizer_has)

    first, second = result["findings"]
    assert first["next_tools"] == [{"tool": "get_top_queries", "arguments": {"sort_by": "cpu"}}]
    assert first["unavailable_next_tools"] == [
        {"tool": "get_resource_limits", "available_in_profiles": ["triage"]}
    ]
    # Plain tool names keep their shape.
    assert second["next_tools"] == ["get_active_sessions"]
    assert second["unavailable_next_tools"] == [
        {"tool": "get_open_transactions", "available_in_profiles": ["triage"]}
    ]


def test_nothing_changes_when_every_tool_is_available() -> None:
    payload = _payload()

    result = annotate_next_tools(payload, lambda _tool: True)

    assert result == payload
    assert "unavailable_next_tools" not in result["findings"][0]


def test_the_input_is_not_modified() -> None:
    payload = _payload()

    annotate_next_tools(payload, _optimizer_has)

    assert payload == _payload()


def test_profiles_exposing_lists_profile_names_in_order() -> None:
    assert profiles_exposing("benchmark_index_candidate") == ["sandbox"]
    assert profiles_exposing("review_workload_indexes") == [
        "index-review",
        "optimizer",
        "sandbox",
        "triage",
    ]
    assert profiles_exposing("no_such_tool") == []
