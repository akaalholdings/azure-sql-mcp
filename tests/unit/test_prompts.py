from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest

from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import PROFILE_TOOL_ALLOWLISTS
from azure_sql_mcp.config import TOOL_GROUPS
from azure_sql_mcp.prompts import PROMPT_TOOLS
from azure_sql_mcp.prompts import register_prompts


@dataclass
class RegisteredPrompt:
    name: str
    func: Callable[..., list[Any]]
    description: str | None


class FakeMCP:
    def __init__(self) -> None:
        self.prompts: dict[str, RegisteredPrompt] = {}

    def prompt(
        self,
        name: str | None = None,
        *,
        title: str | None = None,
        description: str | None = None,
        icons: list[Any] | None = None,
    ) -> Callable[[Callable[..., list[Any]]], Callable[..., list[Any]]]:
        def decorator(func: Callable[..., list[Any]]) -> Callable[..., list[Any]]:
            key = name or func.__name__
            self.prompts[key] = RegisteredPrompt(
                name=key,
                func=func,
                description=description,
            )
            return func

        return decorator


@pytest.fixture
def registered_prompts(sample_server_config: Any) -> FakeMCP:
    mcp = FakeMCP()
    register_prompts(mcp, sample_server_config)  # type: ignore[arg-type]
    return mcp


def _prompt_text(prompt_messages: list[Any]) -> str:
    assert len(prompt_messages) == 1
    message = prompt_messages[0]
    assert message.role == "user"
    return message.content.text


def test_register_prompts_exposes_expected_templates(registered_prompts: FakeMCP) -> None:
    assert set(registered_prompts.prompts) == {
        "analyze-slow-queries",
        "review-index-health",
        "explore-schema",
        "compare-schemas",
        "troubleshoot-performance",
    }
    assert registered_prompts.prompts["compare-schemas"].description == (
        "Compare schemas between two databases to find differences."
    )


def test_analyze_slow_queries_prompt_targets_current_tool_surface(
    registered_prompts: FakeMCP,
) -> None:
    text = _prompt_text(
        registered_prompts.prompts["analyze-slow-queries"].func("appdb", window_minutes=45)
    )

    assert "get_top_queries" in text
    # get_top_queries returns query ids. The stored plan is read by id: the
    # Query Store text of a parameterized query, "(@p int)SELECT ...", does not
    # compile when it is sent back to explain_query.
    assert "analyze_query_plan" in text and "query_id" in text
    assert "explain_query" not in text
    assert "get_query_store_trend" in text
    # Index design comes from the workload tool every read profile exposes,
    # scoped to the slow queries; the missing-index DMV tool is not in triage.
    assert "review_workload_indexes" in text and "query_ids" in text
    assert "analyze_index_recommendations" not in text
    assert "window_minutes=45" in text
    assert "generate_migration_script" not in text


def test_review_index_health_prompt_targets_current_tool_surface(
    registered_prompts: FakeMCP,
) -> None:
    text = _prompt_text(registered_prompts.prompts["review-index-health"].func("appdb"))

    # analyze_db_health retired health_type='index': the old first step failed.
    assert "analyze_db_health" not in text and "health_type" not in text
    assert "review_workload_indexes" in text
    assert "lookback_days=7" in text and "lookback_days=40" in text
    assert "rollback" in text
    # A fragmentation-driven rebuild plan is not index health on Azure SQL.
    assert "rebuild" not in text.lower() and "reorganize" not in text.lower()


def test_explore_schema_prompt_includes_dependency_and_object_browsing(
    registered_prompts: FakeMCP,
) -> None:
    text = _prompt_text(registered_prompts.prompts["explore-schema"].func("appdb", "dbo"))

    assert "list_schemas" in text
    # list_objects is in no profile; these two are in optimizer and sandbox.
    assert "list_objects" not in text
    assert "get_table_stats" in text and "search_objects" in text
    assert "get_object_details" in text
    assert "get_dependencies" in text


def test_compare_schemas_prompt_uses_schema_compare_tools(
    registered_prompts: FakeMCP,
) -> None:
    text = _prompt_text(
        registered_prompts.prompts["compare-schemas"].func("appdb", "reportingdb")
    )

    assert "compare_schemas" in text
    assert "generate_migration_script" in text
    assert "get_object_details" not in text


def test_troubleshoot_performance_prompt_uses_performance_workflow_tools(
    registered_prompts: FakeMCP,
) -> None:
    text = _prompt_text(registered_prompts.prompts["troubleshoot-performance"].func("appdb"))

    # One call that ranks findings and names the next tools comes first.
    assert "diagnose_database" in text
    assert "analyze_db_health" not in text
    assert "get_top_queries" in text
    assert "analyze_query_plan" in text and "explain_query" not in text
    # The legacy tool recompiles each top query through SHOWPLAN and is in no
    # profile; the prompt must name the tool the profiles and skills use, with
    # arguments that tool accepts.
    assert "review_workload_indexes" in text
    assert "analyze_workload_indexes" not in text
    assert "top_n" not in text and "top_queries=20" in text
    assert "get_active_sessions" in text


_TOOL_PREFIXES = (
    "get_", "check_", "list_", "start_", "add_", "benchmark_", "finalize_", "review_",
    "detect_", "explain_", "collect_", "plan_", "prepare_", "analyze_", "compare_",
    "diagnose_", "search_", "generate_",
)
_PROMPT_ARGUMENTS = {
    "analyze-slow-queries": ("appdb",),
    "review-index-health": ("appdb",),
    "explore-schema": ("appdb", "dbo"),
    "compare-schemas": ("appdb", "reportingdb"),
    "troubleshoot-performance": ("appdb",),
}


@pytest.mark.parametrize("name", sorted(_PROMPT_ARGUMENTS))
def test_a_prompt_names_only_the_tools_it_declares(
    registered_prompts: FakeMCP,
    name: str,
) -> None:
    # A prompt step that names a tool the profile does not expose fails at
    # that step, after the agent has already spent calls on the earlier ones.
    text = _prompt_text(registered_prompts.prompts[name].func(*_PROMPT_ARGUMENTS[name]))
    named = {
        token
        for token in re.findall(r"\b([a-z]+(?:_[a-z]+)+)\b", text)
        if token.startswith(_TOOL_PREFIXES) and token in TOOL_GROUPS
    }

    assert named == set(PROMPT_TOOLS[name])
    assert set(PROMPT_TOOLS[name]) <= set(TOOL_GROUPS)


@pytest.mark.parametrize("profile", list(McpProfile))
def test_a_profile_registers_only_the_prompts_it_can_run(
    server_config_factory: Callable[..., Any],
    profile: McpProfile,
) -> None:
    mcp = FakeMCP()
    register_prompts(mcp, server_config_factory(profile=profile))  # type: ignore[arg-type]

    runnable = {
        name
        for name, tools in PROMPT_TOOLS.items()
        if set(tools) <= PROFILE_TOOL_ALLOWLISTS[profile]
    }
    assert set(mcp.prompts) == runnable


def test_the_read_profiles_keep_their_guided_workflows(
    server_config_factory: Callable[..., Any],
) -> None:
    def prompts_for(profile: McpProfile) -> set[str]:
        mcp = FakeMCP()
        register_prompts(mcp, server_config_factory(profile=profile))  # type: ignore[arg-type]
        return set(mcp.prompts)

    assert prompts_for(McpProfile.TRIAGE) == {
        "analyze-slow-queries",
        "review-index-health",
        "troubleshoot-performance",
    }
    assert prompts_for(McpProfile.OPTIMIZER) == {
        "analyze-slow-queries",
        "review-index-health",
        "explore-schema",
        "troubleshoot-performance",
    }
    assert prompts_for(McpProfile.INDEX_REVIEW) == {"review-index-health"}
