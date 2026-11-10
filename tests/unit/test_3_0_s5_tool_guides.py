"""On-demand tool guides.

The skills carried this reading material on every load. A guide is fetched only
when needed, so it must be short, must name only tools that exist, and must
answer the four questions an agent has at that moment.
"""

from __future__ import annotations

import re

import pytest

from azure_sql_mcp.config import TOOL_GROUPS
from azure_sql_mcp.tool_guides import MAX_GUIDE_CHARS
from azure_sql_mcp.tool_guides import get_tool_guide
from azure_sql_mcp.tool_guides import list_tool_guides

# Tools this release adds. The orchestrator registers them in server.py and
# config.py; until then they are not in TOOL_GROUPS.
NOT_YET_REGISTERED = {"get_column_statistics"}
KNOWN_TOOLS = set(TOOL_GROUPS) | NOT_YET_REGISTERED

GUIDE_NAMES = [entry["name"] for entry in list_tool_guides()]

TOOL_PREFIXES = (
    "get_", "check_", "list_", "start_", "add_", "benchmark_", "finalize_", "review_",
    "detect_", "explain_", "collect_", "plan_", "prepare_", "analyze_", "compare_",
    "diagnose_", "capture_", "apply_", "verify_", "rollback_", "recall_", "report_",
    "tune_", "optimize_", "execute_", "force_", "kill_", "rebuild_", "update_",
)
# Response fields and arguments that share a tool prefix.
FIELD_NAMES = {"plan_kind", "plan_digest", "plan_findings", "plan_count", "plan_id",
               "plan_handle", "plan_changes_in_window", "plan_changed", "rollback_ddl"}
MARKETING_WORDS = (
    "powerful", "seamless", "robust", "cutting-edge", "world-class", "best-in-class",
    "blazing", "effortless", "leverage", "comprehensive", "revolutionary", "unlock",
    "supercharge", "state-of-the-art", "game-changing",
)


def test_the_registry_covers_the_major_tools_and_the_cross_tool_topics() -> None:
    entries = list_tool_guides()
    tools = {entry["name"] for entry in entries if entry["kind"] == "tool"}
    topics = {entry["name"] for entry in entries if entry["kind"] == "topic"}

    assert topics == {
        "reading_a_plan",
        "proving_a_rewrite",
        "parameter_sensitivity",
        "index_design",
        "blocking_triage",
        "columnstore",
        "client_report",
    }
    # A tool guide for a tool that does not exist would never be fetched.
    assert tools <= KNOWN_TOOLS
    assert {
        "diagnose_database",
        "analyze_query_plan",
        "explain_query",
        "review_workload_indexes",
        "benchmark_tuning_candidate",
        "get_column_statistics",
    } <= tools
    assert all(set(entry) == {"name", "kind", "title"} for entry in entries)


@pytest.mark.parametrize("name", GUIDE_NAMES)
def test_each_guide_is_short_and_answers_the_four_questions(name: str) -> None:
    guide = get_tool_guide(name)

    assert guide["found"] is True
    text = guide["guide"]
    assert len(text) <= MAX_GUIDE_CHARS == 2500
    # When to use it, how to read it, what to avoid, what to call next.
    positions = [text.find(label) for label in ("Use when:", "Read:", "Avoid:", "Next:")]
    assert all(position >= 0 for position in positions)
    assert positions == sorted(positions)
    avoid = text.split("Avoid:", 1)[1].split("Next:", 1)[0]
    assert 2 <= len(re.findall(r"^- ", avoid, flags=re.MULTILINE)) <= 4


@pytest.mark.parametrize("name", GUIDE_NAMES)
def test_each_guide_names_only_tools_that_exist(name: str) -> None:
    # A guide that sends the agent to a tool that is not there costs a failed
    # call and a retry in the middle of an incident.
    guide = get_tool_guide(name)
    named = set(re.findall(r"\b([a-z]+(?:_[a-z0-9]+)+)\b", guide["guide"]))
    cited = {
        token for token in named if token.startswith(TOOL_PREFIXES) and token not in FIELD_NAMES
    }

    assert sorted(cited - KNOWN_TOOLS) == []
    assert sorted(set(guide["next_tools"]) - KNOWN_TOOLS) == []


@pytest.mark.parametrize("name", GUIDE_NAMES)
def test_guides_use_plain_words(name: str) -> None:
    text = get_tool_guide(name)["guide"].casefold()

    assert [word for word in MARKETING_WORDS if word in text] == []
    assert not re.search(r"[^\x00-\x7f]", text)


def test_names_are_forgiving_and_an_unknown_name_lists_what_exists() -> None:
    assert get_tool_guide(" Reading-A-Plan ")["name"] == "reading_a_plan"
    assert get_tool_guide("Explain_Query")["kind"] == "tool"

    missing = get_tool_guide("no_such_guide")
    assert missing["found"] is False
    assert missing["available"] == GUIDE_NAMES
    assert "guide" not in missing
