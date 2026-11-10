"""The guides say what changed in stream S13d: the agent reads them, not the code."""

from __future__ import annotations

from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.tool_guides import MAX_GUIDE_CHARS
from azure_sql_mcp.tool_guides import get_tool_guide
from azure_sql_mcp.tool_guides import list_tool_guides


def _text(name: str) -> str:
    guide = get_tool_guide(name)
    assert guide["found"] is True
    assert len(guide["guide"]) <= MAX_GUIDE_CHARS
    return " ".join(guide["guide"].split())


def test_a_workflow_guide_says_how_to_keep_and_reuse_test_objects() -> None:
    text = _text("session_held_objects")

    for phrase in (
        "keep_for_session=true",
        "reuse them without a build",
        "test_objects",
        "session_before_hold",
        "finalize_tuning_session before you deliver the answer",
        "lease_minutes",
        "compare_query_results and explain_query",
    ):
        assert phrase in text, phrase
    listed = {entry["name"]: entry["kind"] for entry in list_tool_guides()}
    assert listed["session_held_objects"] == "workflow"


def test_the_time_guide_states_the_large_gain_rule_the_cap_the_retry_and_time_spent() -> None:
    text = _text("measurement_time_rules")

    for phrase in (
        "one tenth",
        "one_run_each_large_gain",
        "time_spent",
        "max_call_seconds",
        "partial",
        "retried once",
        "build_retries",
    ):
        assert phrase in text, phrase


def test_the_proof_guide_says_what_a_tested_match_means() -> None:
    text = _text("proving_a_rewrite")

    assert "tested_match" in text and "snapshot_test" in text
    assert "ordered_rows_match and multiset_match" in text
    assert "today's data" in text
    assert "handled_by_statement" in text and "conditional_preconditions" in text


def test_the_batch_guide_lists_what_a_guard_may_hold() -> None:
    text = _text("branching_batch")

    assert "COLLATE" in text and "TRY_CONVERT" in text
    assert "no subquery" in text


def test_the_view_guide_names_the_shared_options_and_the_schema() -> None:
    text = _text("indexed_view_experiment")

    assert "plan_reuse" in text and "keep_for_session" in text
    assert "<schema>.<view>" in text


def test_every_guide_names_only_tools_that_exist(server_config_factory) -> None:
    app = AzureSqlMcpApplication(server_config_factory())
    names = {tool.name for tool in app.mcp._tool_manager.list_tools()}
    guide = get_tool_guide("session_held_objects")

    assert set(guide["next_tools"]) - {"benchmark_index_candidate", "benchmark_indexed_view_candidate"} <= names
