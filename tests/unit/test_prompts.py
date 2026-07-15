from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest

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
    assert "explain_query" in text
    assert "analyze_index_recommendations" in text
    assert "generate_migration_script" not in text


def test_review_index_health_prompt_targets_current_tool_surface(
    registered_prompts: FakeMCP,
) -> None:
    text = _prompt_text(registered_prompts.prompts["review-index-health"].func("appdb"))

    assert "analyze_db_health" in text
    assert "analyze_index_recommendations" in text
    assert "get_table_stats" in text


def test_explore_schema_prompt_includes_dependency_and_object_browsing(
    registered_prompts: FakeMCP,
) -> None:
    text = _prompt_text(registered_prompts.prompts["explore-schema"].func("appdb", "dbo"))

    assert "list_schemas" in text
    assert "list_objects" in text
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

    assert "analyze_db_health" in text
    assert "get_top_queries" in text
    assert "explain_query" in text
    assert "analyze_workload_indexes" in text
    assert "get_active_sessions" in text
