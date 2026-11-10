"""The shipped task files: valid keys, full exam coverage, usable by the live tier.

A task whose key cannot score its own reference, or whose trap goes
undetected, measures nothing. These tests keep the corpus honest as it grows.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp.tool_contracts import ParameterCaseInput

EVAL = Path(__file__).resolve().parents[2] / "eval"
SCRIPT = EVAL / "scoring.py"


def _load():
    spec = importlib.util.spec_from_file_location("eval_scoring", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


scoring = _load()
TASKS: list[dict[str, Any]] = scoring.load_tasks()
BY_ID = {task["id"]: task for task in TASKS}
BY_SOURCE = {task["source"]: task for task in TASKS}
TUNING = [task for task in TASKS if task["kind"] == "tuning"]
MANIFEST = json.loads((EVAL / "tasks" / "manifest.json").read_text(encoding="utf-8"))

# The problem classes the 3.0 brief asks the new tuning tasks to cover.
NEW_TUNING_TAGS = {
    "catch-all-parameters",
    "or-across-columns",
    "deep-offset-paging",
    "distinct-fan-out",
    "not-in-null",
    "implicit-conversion",
    "scalar-udf",
    "key-lookup",
    "parameter-sensitivity",
    "no-change",
    "columnstore",
    "partitioned-table",
}


def _answer(task: dict[str, Any], **changes: Any) -> dict[str, Any]:
    answer = {"task_id": task["id"], "presented_as_safe": True, "proof_scope": "none", "indexes": []}
    answer.update(changes)
    return answer


def _trap_sql(task: dict[str, Any], trap_id: str) -> str:
    return next(trap["example_sql"] for trap in task["key"]["traps"] if trap["id"] == trap_id)


@pytest.mark.parametrize("task", TASKS, ids=lambda task: task["id"])
def test_shipped_task_is_valid(task: dict[str, Any]) -> None:
    # Includes the self-check: the reference earns full points, each trap example is caught.
    assert scoring.validate_task(task) == []


def test_task_ids_are_unique_and_sources_are_not_reused() -> None:
    assert len(BY_ID) == len(TASKS)
    assert len(BY_SOURCE) == len(TASKS)
    assert {task["split"] for task in TASKS} == {"public"}


def test_all_16_rewrite_exam_items_are_tuning_tasks() -> None:
    for number in range(1, 17):
        task = BY_SOURCE.get(f"rewrite-exam-v1:Q{number:02d}")
        assert task is not None and task["kind"] == "tuning", number


@pytest.mark.parametrize(("exam", "items", "kind"), [("index-exam-v1", 14, "index_review"), ("case-exam-v1", 12, "diagnosis")])
def test_every_exam_item_is_converted_or_declared_not_converted(exam: str, items: int, kind: str) -> None:
    # An item dropped without a word would make the old and new scores look comparable when they are not.
    declared = {entry["source"]: entry["reason"] for entry in MANIFEST["not_converted"]}
    for number in range(1, items + 1):
        source = f"{exam}:{number}"
        converted = [task for task in TASKS if task["source"].split("/")[0] == source]
        assert converted or declared.get(source), source
        assert all(task["kind"] == kind for task in converted), source
        assert not (converted and source in declared), source


def test_new_tuning_tasks_cover_every_named_problem_class() -> None:
    new = [task for task in TUNING if task["source"].startswith("s6-new")]
    covered = {tag for task in new for tag in task["tags"]}

    assert len(new) >= 12
    assert NEW_TUNING_TAGS <= covered, NEW_TUNING_TAGS - covered


@pytest.mark.parametrize("task", TUNING, ids=lambda task: task["id"])
def test_tuning_task_runs_only_in_the_eval_schema(task: dict[str, Any]) -> None:
    # The live tier executes this SQL on a shared bench database.
    ddl = task["schema"]["ddl"]
    for table in task["schema"]["tables"]:
        assert table.startswith("qe."), table
        assert f"CREATE TABLE {table} " in ddl, table
    sql = [task["baseline_sql"]]
    sql += [family["reference_sql"] for family in task["key"]["correct_families"]]
    sql += [trap["example_sql"] for trap in task["key"]["traps"] if "example_sql" in trap]
    for statement in sql:
        assert "dbo." not in statement, statement


@pytest.mark.parametrize("task", TUNING, ids=lambda task: task["id"])
def test_tuning_parameter_cases_match_the_benchmark_tool_contract(task: dict[str, Any]) -> None:
    cases = task["parameter_cases"]

    assert cases, "the live tier needs at least one case to run"
    assert sum(case["weight"] for case in cases) == pytest.approx(1.0)
    for case in cases:
        ParameterCaseInput(**case)
        for name in case["values"]:
            assert f"@{name}" in task["baseline_sql"], name


@pytest.mark.parametrize("task", TUNING, ids=lambda task: task["id"])
def test_tuning_key_states_the_trap_that_changes_results_and_how_to_tell(task: dict[str, Any]) -> None:
    for trap in task["key"]["traps"]:
        if trap["class"] == "not_equivalent":
            assert trap.get("told_apart_by"), f"{trap['id']}: name the data or case that tells it apart"


def test_plain_not_exists_for_not_in_is_harm_when_presented_as_safe() -> None:
    # It adds about 160k customers without an email address to a marketing list.
    task = BY_SOURCE["rewrite-exam-v1:Q12"]

    result = scoring.score_answer(task, _answer(task, decision="rewrite", sql=_trap_sql(task, "plain_not_exists")))

    assert result["correct"] is False
    assert result["harm"] == 1
    assert "trap:plain_not_exists" in result["harm_reasons"]


def test_row_number_for_latest_order_with_ties_is_harm_when_presented_as_safe() -> None:
    task = BY_SOURCE["rewrite-exam-v1:Q05"]

    result = scoring.score_answer(task, _answer(task, decision="rewrite", sql=_trap_sql(task, "row_number")))

    assert (result["correct"], result["harm"]) == (False, 1)


def test_no_change_task_rewards_leaving_a_healthy_query_alone() -> None:
    task = BY_SOURCE["rewrite-exam-v1:Q16"]
    left_alone = scoring.score_answer(task, _answer(task, decision="no_change"))
    rewritten = scoring.score_answer(task, _answer(task, decision="rewrite", sql=_trap_sql(task, "end_of_day_literal")))

    assert (left_alone["points"], left_alone["max_points"], left_alone["harm"]) == (4, 4, 0)
    assert (rewritten["correct"], rewritten["harm"]) == (False, 1)


def test_scale_up_answer_to_the_log_rate_case_is_harm() -> None:
    # The client's own plan: 50% more compute spend that leaves the batch as slow as before.
    task = BY_SOURCE["case-exam-v1:1"]
    answer = {
        "task_id": task["id"],
        "root_cause": "cpu_capacity",
        "cited_facts": ["CPU shows 100% in the portal."],
        "recommendations": [{"action": "Scale up to 24 vCores on Friday.", "presented_as_safe": True}],
    }

    result = scoring.score_answer(task, answer)

    assert result["points"] == 0
    assert result["harm"] >= 1


def test_dropping_the_foreign_key_index_is_harm() -> None:
    task = BY_SOURCE["index-exam-v1:2"]
    answer = {
        "task_id": task["id"],
        "decisions": [{"index": "IX_Shipment_OrderId", "action": "drop", "presented_as_safe": True}],
        "create": [],
    }

    result = scoring.score_answer(task, answer)

    assert (result["points"], result["correct"], result["harm"]) == (0, False, 1)


def test_root_cause_vocabulary_is_larger_than_the_answers() -> None:
    # With one code per task the list would give the answers away.
    codes = scoring.load_root_causes()
    used = {task["key"]["root_cause"] for task in TASKS if task["kind"] == "diagnosis"}

    assert used <= set(codes)
    assert len(codes) >= 2 * len(used)
    assert all(description.strip() for description in codes.values())


def test_cli_validates_the_shipped_tasks(capsys: pytest.CaptureFixture[str]) -> None:
    assert scoring.main(["validate"]) == 0
    assert f"{len(TASKS)} tasks are valid" in capsys.readouterr().out
