"""S13a F6: the guides tell an agent the rules it met the hard way.

Without them the agent retried a refused online build three times and could
not read why a test index build failed.
"""

from __future__ import annotations

from azure_sql_mcp.tool_guides import MAX_GUIDE_CHARS
from azure_sql_mcp.tool_guides import get_tool_guide


def _text(name: str) -> str:
    text = get_tool_guide(name)["guide"]
    assert len(text) <= MAX_GUIDE_CHARS
    return " ".join(text.split())


def test_benchmark_guide_states_the_columnstore_rule_and_the_failure_fields() -> None:
    text = _text("benchmark_index_candidate")

    assert "clustered columnstore index is built offline" in text
    assert "allow_offline_test_index" in text and "offline_build" in text
    for field in ("failed_step", "cause_class", "engine_message", "timeout_seconds"):
        assert field in text, field
    assert "create_failed" in text


def test_index_set_guide_states_the_name_rule() -> None:
    text = _text("index_set_experiment")

    assert "extends it" in text
    assert "<prefix><Table>_<KeyColumn1>_<KeyColumn2>_<8 hex>" in text
    assert "index_name_note" in text


def test_review_guide_says_ddl_for_a_columnstore_table_builds_offline() -> None:
    text = _text("review_workload_indexes")

    assert "columnstore" in text and "offline_build" in text
    assert "no ONLINE option" in text
