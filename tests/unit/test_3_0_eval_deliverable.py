"""The client-deliverable checker.

A client pays for a decision, the evidence behind it, the risk both ways and a
way back. Tool names and field names in that text show the client the
machinery instead of the answer. Each rule here has a text that passes and a
text that fails for that rule alone.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from azure_sql_mcp.config import TOOL_GROUPS

SCRIPT = Path(__file__).resolve().parents[2] / "eval" / "scoring.py"


def _load():
    spec = importlib.util.spec_from_file_location("eval_scoring", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


scoring = _load()

GOOD = """# Order search is slow

We recommend one new index on the orders table; the query text stays as it is.

The search reads 1.1M pages per call and returns at most 2 rows. The index turns
that into about 4 reads.

Risk of acting: the build adds write load for about 20 minutes. Risk of not
acting: CPU stays pinned during settlement bursts and payments queue.

Rollback: drop the new index. Nothing else changes.
"""


def check(text: str, *, is_change: bool = True, **lists: list[str]) -> dict:
    return scoring.check_client_deliverable(text, is_change=is_change, **lists)


def test_good_deliverable_passes_every_rule() -> None:
    assert check(GOOD) == {
        "recommendation_first": True,
        "evidence_cited": True,
        "risk_of_acting": True,
        "risk_of_not_acting": True,
        "rollback": True,
        "internal_names": [],
        "passed": True,
    }


def test_recommendation_must_be_in_the_first_paragraph() -> None:
    # The reader may stop after one paragraph; background first buries the decision.
    background_first = GOOD.replace(
        "We recommend one new index on the orders table; the query text stays as it is.",
        "The orders table has 60 million rows and takes 180 inserts per second.",
    ) + "\nOur advice: you should add one index.\n"

    result = check(background_first)

    assert result["recommendation_first"] is False
    assert result["passed"] is False


def test_a_heading_is_not_the_first_paragraph() -> None:
    assert check("# We recommend\n\n## Summary\n\nThe table has 60 million rows.")["recommendation_first"] is False
    assert check("# Findings\n\nDo not scale up this week.")["recommendation_first"] is True


def test_evidence_needs_a_measured_figure() -> None:
    vague = GOOD.replace("reads 1.1M pages per call and returns at most 2 rows", "reads far too much").replace(
        "about 4 reads", "almost nothing"
    ).replace("about 20 minutes", "a short time")

    result = check(vague)

    assert result["evidence_cited"] is False
    assert result["passed"] is False


@pytest.mark.parametrize("figure", ["31% of waits", "2.8 s per call", "140 ms", "96 MiB/s", "12,000 rows", "16 vCores", "40 times"])
def test_evidence_accepts_a_figure_with_a_unit(figure: str) -> None:
    assert check(f"Keep the index.\n\nMeasured: {figure}.")["evidence_cited"] is True


@pytest.mark.parametrize("text", ["Keep the index.\n\nReviewed on 2026-09-24 in step 2.", "Keep index IX_Orders_1."])
def test_evidence_does_not_accept_a_date_or_a_name_with_digits(text: str) -> None:
    assert check(text)["evidence_cited"] is False


def test_both_risks_must_be_present() -> None:
    no_acting = GOOD.replace("Risk of acting: the build adds write load for about 20 minutes. ", "")
    no_waiting = GOOD.replace("Risk of not\nacting: CPU stays pinned during settlement bursts and payments queue.", "")

    acting_missing, waiting_missing = check(no_acting), check(no_waiting)

    assert (acting_missing["risk_of_acting"], acting_missing["risk_of_not_acting"]) == (False, True)
    assert (waiting_missing["risk_of_acting"], waiting_missing["risk_of_not_acting"]) == (True, False)
    assert acting_missing["passed"] is False and waiting_missing["passed"] is False


def test_risk_of_not_acting_is_not_read_as_risk_of_acting() -> None:
    only_waiting = "Keep it.\n\nRisk of not acting: 40 ms more per call."

    assert check(only_waiting, is_change=False)["risk_of_acting"] is False


def test_rollback_is_required_for_a_change() -> None:
    no_way_back = GOOD.replace("Rollback: drop the new index. Nothing else changes.", "Nothing else changes.")

    result = check(no_way_back)

    assert result["rollback"] is False
    assert result["passed"] is False


def test_rollback_is_not_required_when_nothing_changes() -> None:
    no_change = (
        "We recommend no change to this query.\n\nIt runs in 6 ms with 85 reads.\n\n"
        "Risk of acting: a rewrite can lose rows at the day boundary. Risk of not acting: none measured."
    )

    result = check(no_change, is_change=False)

    assert result["rollback"] is None
    assert result["passed"] is True
    assert check(no_change, is_change=True)["passed"] is False


def test_every_registered_tool_name_is_flagged() -> None:
    # The list comes from the registry: a tool added later is covered without an edit here.
    for name in TOOL_GROUPS:
        result = check(GOOD + f"\nSource: {name}.\n")
        assert result["internal_names"] == [name], name
        assert result["passed"] is False


def test_tool_names_are_matched_as_whole_names_in_any_case() -> None:
    assert check(GOOD + "\nWe ran Review_Workload_Indexes twice.")["internal_names"] == ["review_workload_indexes"]
    assert check(GOOD + "\nSee my_get_wait_stats_copy.")["internal_names"] == []


def test_plain_english_about_the_same_work_is_not_flagged() -> None:
    plain = GOOD + "\nWe explained the query plan, reviewed the workload indexes and killed no session.\n"

    assert check(plain)["internal_names"] == []


def test_default_field_names_are_flagged() -> None:
    leaked = GOOD + "\nThe result_status was ok and proof_scope is snapshot_matched.\n"

    result = check(leaked)

    assert result["internal_names"] == ["proof_scope", "result_status", "snapshot_matched"]
    assert result["passed"] is False


def test_field_name_list_is_configurable() -> None:
    text = GOOD + "\nThe wait_category was Lock and result_status was ok.\n"

    assert check(text, field_names=["wait_category"])["internal_names"] == ["wait_category"]
    assert check(text, tool_names=[], field_names=[])["internal_names"] == []


def test_shipped_field_name_list_holds_only_identifiers_the_server_emits() -> None:
    # An ordinary word in this list would fail honest client text.
    names = scoring.load_field_names()
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (SCRIPT.parents[1] / "src" / "azure_sql_mcp").glob("*.py")
    ) + SCRIPT.read_text(encoding="utf-8")

    assert names == sorted(set(names))
    for name in names:
        assert "_" in name, name
        assert f'"{name}"' in source, name


def test_scored_answer_carries_the_deliverable_check() -> None:
    task = {
        "format_version": 1,
        "id": "t",
        "kind": "diagnosis",
        "key": {
            "root_cause": "log_rate_cap",
            "required_facts": [],
            "harmful_conclusions": [],
        },
    }
    answer = {"task_id": "t", "root_cause": "log_rate_cap", "recommendations": [], "client_text": GOOD}

    result = scoring.score_answer(task, answer)

    # No recommendation means no change, so a rollback is not demanded.
    assert result["deliverable"]["rollback"] is None
    assert result["deliverable"]["passed"] is True
