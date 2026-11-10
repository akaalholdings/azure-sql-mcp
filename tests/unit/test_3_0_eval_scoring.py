"""The static scorer: each rule earns and loses its points, and harm is exact.

The scorer decides whether the product beats a bare model. A rule that gives
points to a wrong answer, or a harm counter that misses a dangerous claim or
punishes an honest one, makes that verdict worthless.
"""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "eval" / "scoring.py"


def _load():
    spec = importlib.util.spec_from_file_location("eval_scoring", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


scoring = _load()

BASELINE_SQL = (
    "SELECT o.OrderID, o.OrderDate FROM qe.Orders o "
    "WHERE o.OrderDate = (SELECT MAX(o2.OrderDate) FROM qe.Orders o2 WHERE o2.CustomerID = o.CustomerID)"
)
TIES_SQL = (
    "SELECT lo.OrderID, lo.OrderDate FROM qe.Customers c CROSS APPLY ("
    "SELECT TOP (1) WITH TIES o.OrderID, o.OrderDate FROM qe.Orders o "
    "WHERE o.CustomerID = c.CustomerID ORDER BY o.OrderDate DESC) lo"
)
ROW_NUMBER_SQL = (
    "SELECT t.OrderID, t.OrderDate FROM (SELECT o.OrderID, o.OrderDate, "
    "ROW_NUMBER() OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate DESC) AS rn FROM qe.Orders o) t WHERE t.rn = 1"
)
RECOMPILE_SQL = BASELINE_SQL + " OPTION (RECOMPILE)"
ACCEPTED_INDEX = {"table": "qe.Orders", "key_columns": ["CustomerID", "OrderDate"], "include_columns": ["TotalAmount"]}

TUNING: dict[str, Any] = {
    "format_version": 1,
    "id": "sample-latest-order",
    "kind": "tuning",
    "title": "Latest order per customer, ties are part of the result",
    "source": "test",
    "prompt": "Nightly job. Customers with two orders on the latest date must keep both rows.",
    "schema": {"tables": ["qe.Orders", "qe.Customers"]},
    "baseline_sql": BASELINE_SQL,
    "parameter_cases": [],
    "key": {
        "decision": "rewrite",
        "correct_families": [
            {
                "id": "top_with_ties",
                "description": "One seek per customer that returns every tied row.",
                "signals": {"all": [r"top\s*\(1\)\s*with ties"]},
                "reference_sql": TIES_SQL,
            }
        ],
        "traps": [
            {
                "id": "row_number",
                "class": "not_equivalent",
                "why": "ROW_NUMBER keeps one row per customer and drops the tied orders.",
                "signals": {"all": [r"row_number\s*\("]},
                "example_sql": ROW_NUMBER_SQL,
            },
            {
                "id": "recompile",
                "class": "ineffective",
                "why": "A fresh compile picks the same plan.",
                "signals": {"all": [r"option\s*\(\s*recompile"]},
                "example_sql": RECOMPILE_SQL,
            },
            {
                "id": "unique_index",
                "class": "unsafe",
                "on": "indexes",
                "why": "Two orders can share a date, so a unique build fails or blocks later inserts.",
                "signals": {"all": [r"^create unique|; create unique"]},
                "example_index": {**ACCEPTED_INDEX, "unique": True},
            },
        ],
        "index": {"role": "required", "accepted": [ACCEPTED_INDEX]},
        "min_improvement": {"metric": "logical_reads", "ratio": 5.0},
    },
}

DIAGNOSIS: dict[str, Any] = {
    "format_version": 1,
    "id": "sample-log-cap",
    "kind": "diagnosis",
    "title": "Night batch overruns; the client wants more vCores",
    "source": "test",
    "prompt": "The batch misses its 06:00 deadline. The client plans to scale from 16 to 24 vCores.",
    "evidence": [
        {
            "id": "waits",
            "tool": "get_wait_stats",
            "arguments": {},
            "result": {"result_status": "ok", "top_waits": [{"wait_type": "LOG_RATE_GOVERNOR", "pct_of_total": 31.0}]},
        },
        {
            "id": "limits",
            "tool": "get_resource_limits",
            "arguments": {},
            "result": {"result_status": "ok", "max_log_rate_mb_per_sec": 96},
        },
    ],
    "key": {
        "root_cause": "log_rate_cap",
        "required_facts": [
            {
                "id": "top_wait",
                "evidence": "waits",
                "description": "The log rate governor is the top wait.",
                "signals": {"all": ["LOG_RATE_GOVERNOR", r"\b31(\.0)?\s?%"]},
                "example": "LOG_RATE_GOVERNOR is the top wait at 31% of wait time.",
            },
            {
                "id": "same_cap",
                "evidence": "limits",
                "description": "The log cap is the same at 24 vCores.",
                "signals": {"all": [r"\b96\b", r"\b(both|same)\b"]},
                "example": "The log cap is 96 MiB/s on both sizes.",
            },
        ],
        "harmful_conclusions": [
            {
                "id": "scale_up",
                "description": "More vCores do not raise the log cap; the spend buys nothing for the batch.",
                "root_causes": ["cpu_capacity"],
                "signals": {"any": [r"scale (up|to)", r"\b24 vcores\b"]},
                "example": "Scale up to 24 vCores on Friday.",
            }
        ],
    },
}
ROOT_CAUSES = {"log_rate_cap": "Log generation is at the cap.", "cpu_capacity": "The compute size is too small."}
FACTS = [fact["example"] for fact in DIAGNOSIS["key"]["required_facts"]]


def _usage(seeks: int, updates: int) -> dict[str, int]:
    return {"user_seeks": seeks, "user_scans": 0, "user_lookups": 0, "user_updates": updates}


INDEX_REVIEW: dict[str, Any] = {
    "format_version": 1,
    "id": "sample-portfolio",
    "kind": "index_review",
    "title": "Small portfolio with one of each decision",
    "source": "test",
    "prompt": "Review the indexes. Usage counters cover nine days since a failover.",
    "usage_window": {"days": 9.3, "reset_by": "failover"},
    "inventory": [
        {"table": "qe.Orders", "name": "IX_A", "key_columns": ["CustomerId", "OrderDate"], "include_columns": ["Status"], "usage": _usage(3_982_000, 22_700_000)},
        {"table": "qe.Orders", "name": "IX_B", "key_columns": ["CustomerId", "OrderDate"], "include_columns": ["ChannelId"], "usage": _usage(1_157_000, 3_240_000)},
        {"table": "qe.Shipment", "name": "IX_FK", "key_columns": ["OrderId"], "usage": _usage(372, 3_420_000)},
        {"table": "qe.Orders", "name": "IX_Dead", "key_columns": ["Notes"], "usage": _usage(0, 26_300_000)},
    ],
    "key": {
        "decisions": [
            {"index": "IX_FK", "action": "keep", "why": "It serves the foreign key check of the nightly purge."},
            {"index": "IX_Dead", "action": "drop", "why": "No reader in any window and no constraint."},
        ],
        "merges": [{"indexes": ["IX_A", "IX_B"], "include_columns": ["Status", "ChannelId"], "why": "Same key."}],
        "create": [
            {
                "id": "velocity",
                "accepted": [{"table": "qe.OrderLine", "key_columns": ["ProductId", "CreatedAt"], "include_columns": ["Qty"]}],
                "why": "Equality column first, then the range column.",
            }
        ],
        "must_not_create": [
            {"id": "advisor_script", "table": "qe.OrderLine", "key_columns": ["CreatedAt"], "why": "Range column first."}
        ],
        "harmful_removals": [{"index": "IX_FK", "why": "Each purge batch would scan the table twice."}],
    },
}


def tuning_answer(**changes: Any) -> dict[str, Any]:
    answer = {
        "task_id": TUNING["id"],
        "decision": "rewrite",
        "sql": TIES_SQL,
        "indexes": [ACCEPTED_INDEX],
        "presented_as_safe": False,
        "proof_scope": "none",
    }
    answer.update(changes)
    return answer


def diagnosis_answer(**changes: Any) -> dict[str, Any]:
    answer = {"task_id": DIAGNOSIS["id"], "root_cause": "log_rate_cap", "cited_facts": FACTS, "recommendations": []}
    answer.update(changes)
    return answer


def decision(index: str, action: str, safe: bool = False, **extra: Any) -> dict[str, Any]:
    return {"index": index, "action": action, "presented_as_safe": safe, **extra}


GOOD_CREATE = {"table": "qe.OrderLine", "key_columns": ["ProductId", "CreatedAt"], "include_columns": ["Qty"], "presented_as_safe": False}
MERGE_INTO_A = [
    decision("IX_A", "alter", include_columns=["Status", "ChannelId"]),
    decision("IX_B", "merge", into="IX_A"),
]


def index_answer(decisions: list[dict[str, Any]] | None = None, create: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    if decisions is None:
        decisions = [decision("IX_FK", "keep"), decision("IX_Dead", "drop"), *MERGE_INTO_A]
    return {"task_id": INDEX_REVIEW["id"], "decisions": decisions, "create": [GOOD_CREATE] if create is None else create}


def score(task: dict[str, Any], answer: Any) -> dict[str, Any]:
    return scoring.score_answer(task, answer)


def with_key(task: dict[str, Any], **key_changes: Any) -> dict[str, Any]:
    changed = copy.deepcopy(task)
    changed["key"].update(key_changes)
    return changed


# ---------------------------------------------------------------- the samples


@pytest.mark.parametrize("task", [TUNING, DIAGNOSIS, INDEX_REVIEW], ids=lambda task: task["kind"])
def test_sample_tasks_are_valid(task: dict[str, Any]) -> None:
    assert scoring.validate_task(task, ROOT_CAUSES) == []


# --------------------------------------------------------------------- tuning


def test_tuning_reference_answer_earns_every_static_point() -> None:
    result = score(TUNING, tuning_answer())

    assert (result["points"], result["max_points"]) == (4, 4)
    assert result["correct"] is True
    assert result["harm"] == 0
    assert result["matched_families"] == ["top_with_ties"]


def test_tuning_decision_point_is_lost_on_the_wrong_decision() -> None:
    # "Only add the index" is a different recommendation to the client.
    result = score(TUNING, tuning_answer(decision="index_only", sql=None))

    assert result["components"]["decision"] == 0
    assert result["correct"] is False


def test_tuning_accepted_alternative_decision_earns_decision_and_shape() -> None:
    task = with_key(TUNING, accepted_decisions=["rewrite", "index_only"])

    result = score(task, tuning_answer(decision="index_only", sql=None))

    assert result["components"]["decision"] == 1
    assert result["components"]["shape"] == 1


def test_tuning_shape_point_needs_a_known_correct_family() -> None:
    unknown_shape = "SELECT o.OrderID, o.OrderDate FROM qe.Orders o JOIN qe.LatestOrder l ON l.OrderID = o.OrderID"

    result = score(TUNING, tuning_answer(sql=unknown_shape))

    assert result["components"]["shape"] == 0
    assert result["components"]["no_trap"] == 1
    assert result["correct"] is False


def test_tuning_trap_loses_the_no_trap_point_and_correctness() -> None:
    result = score(TUNING, tuning_answer(sql=ROW_NUMBER_SQL))

    assert result["matched_traps"] == ["row_number"]
    assert result["components"]["no_trap"] == 0
    assert result["correct"] is False


@pytest.mark.parametrize(
    "index",
    [
        ACCEPTED_INDEX,
        {"table": "[qe].[Orders]", "key_columns": ["[CustomerID]", "OrderDate DESC"], "include_columns": ["TotalAmount", "Status"]},
        {"table": "qe.Orders", "key_columns": ["CustomerID", "OrderDate", "TotalAmount"]},
    ],
    ids=["exact", "brackets-direction-wider-include", "include-moved-into-key"],
)
def test_tuning_index_point_is_earned_by_an_index_that_gives_the_same_access(index: dict[str, Any]) -> None:
    assert score(TUNING, tuning_answer(indexes=[index]))["components"]["index"] == 1


@pytest.mark.parametrize(
    "indexes",
    [
        [],
        [{"table": "qe.Orders", "key_columns": ["OrderDate", "CustomerID"], "include_columns": ["TotalAmount"]}],
        [{"table": "qe.Orders", "key_columns": ["CustomerID", "OrderDate"]}],
        [{"table": "qe.Payments", "key_columns": ["CustomerID", "OrderDate"], "include_columns": ["TotalAmount"]}],
        [{**ACCEPTED_INDEX, "filter": "Status = 'Paid'"}],
    ],
    ids=["none", "wrong-key-order", "missing-include", "wrong-table", "filtered"],
)
def test_tuning_index_point_is_lost_when_the_index_does_not_serve_the_query(indexes: list[dict[str, Any]]) -> None:
    result = score(TUNING, tuning_answer(indexes=indexes))

    assert result["components"]["index"] == 0
    # A missing index costs points; the rewrite itself is still correct.
    assert result["correct"] is True


def test_tuning_index_point_is_lost_by_an_index_nobody_needs() -> None:
    task = with_key(TUNING, index={"role": "none"})

    assert score(task, tuning_answer(indexes=[]))["components"]["index"] == 1
    assert score(task, tuning_answer())["components"]["index"] == 0


def test_tuning_optional_index_is_not_scored() -> None:
    task = with_key(TUNING, index={"role": "optional"})

    result = score(task, tuning_answer(indexes=[]))

    assert result["components"]["index"] is None
    assert (result["points"], result["max_points"]) == (3, 3)


def test_tuning_improvement_is_scored_only_when_the_live_tier_measured_it() -> None:
    unmeasured = score(TUNING, tuning_answer())
    enough = score(TUNING, tuning_answer(measured={"equivalent": True, "improvement_ratio": 5.0}))
    too_small = score(TUNING, tuning_answer(measured={"equivalent": True, "improvement_ratio": 4.9}))

    assert unmeasured["components"]["improvement"] is None and unmeasured["max_points"] == 4
    assert (enough["components"]["improvement"], enough["points"], enough["max_points"]) == (1, 5, 5)
    assert (too_small["components"]["improvement"], too_small["points"], too_small["max_points"]) == (0, 4, 5)


def test_tuning_improvement_is_lost_when_one_parameter_case_gets_worse() -> None:
    # A 20x average gain that makes one customer group 2x slower is a regression for that group.
    measured = {"equivalent": True, "improvement_ratio": 20.0, "worst_case_ratio": 0.5}

    assert score(TUNING, tuning_answer(measured=measured))["components"]["improvement"] == 0


def test_tuning_measured_difference_outranks_a_matching_family() -> None:
    # The text looks like the reference, but the live comparison found different rows.
    result = score(TUNING, tuning_answer(measured={"equivalent": False, "improvement_ratio": 9.0}))

    assert result["matched_families"] == ["top_with_ties"]
    assert result["components"]["no_trap"] == 0
    assert result["correct"] is False


def test_tuning_unlisted_shape_earns_the_shape_point_only_on_a_measured_win() -> None:
    # The family list cannot name every correct rewrite. A measured equal result
    # with the required gain is the stronger evidence; a claim without it is not.
    unlisted = "SELECT o.OrderID, o.OrderDate FROM qe.Orders o JOIN qe.LatestOrder l ON l.OrderID = o.OrderID"
    won = {"equivalent": True, "improvement_ratio": 8.0}
    equal_but_slow = {"equivalent": True, "improvement_ratio": 1.1}
    fast_but_unchecked = {"improvement_ratio": 8.0}

    assert score(TUNING, tuning_answer(sql=unlisted, measured=won))["components"]["shape"] == 1
    assert score(TUNING, tuning_answer(sql=unlisted, measured=won))["correct"] is True
    assert score(TUNING, tuning_answer(sql=unlisted, measured=equal_but_slow))["components"]["shape"] == 0
    assert score(TUNING, tuning_answer(sql=unlisted, measured=fast_but_unchecked))["components"]["shape"] == 0


NO_CHANGE = with_key(
    TUNING,
    decision="no_change",
    correct_families=[],
    index={"role": "none"},
    min_improvement={"metric": "logical_reads", "ratio": 0.9},
)


def test_no_change_task_is_won_by_leaving_the_query_alone() -> None:
    assert scoring.validate_task(NO_CHANGE) == []
    untouched = {"task_id": TUNING["id"], "decision": "no_change", "presented_as_safe": True}
    reformatted = {**untouched, "sql": "-- unchanged\n" + BASELINE_SQL.replace(" WHERE", "\n  WHERE"), "proof_scope": "unchanged_sql"}

    for answer in (untouched, reformatted):
        result = score(NO_CHANGE, answer)
        assert (result["points"], result["max_points"], result["correct"], result["harm"]) == (4, 4, True, 0)


def test_no_change_task_is_lost_by_a_needless_rewrite() -> None:
    result = score(NO_CHANGE, tuning_answer(indexes=[]))

    assert result["components"] == {"decision": 0, "shape": 0, "no_trap": 1, "index": 1, "improvement": None}
    assert result["correct"] is False


# ---------------------------------------------------------------- tuning harm


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"sql": ROW_NUMBER_SQL, "proof_scope": "constraint_backed"}, "trap:row_number"),
        ({"proof_scope": "none"}, "unproven:none"),
        ({"proof_scope": "snapshot_matched"}, "unproven:snapshot_matched"),
        ({"proof_scope": "constraint_backed", "measured": {"equivalent": False}}, "measured_not_equivalent"),
        ({"proof_scope": "constraint_backed", "measured": {"proof_scope": "snapshot_matched"}}, "unproven:snapshot_matched"),
        ({"proof_scope": "constraint_backed", "indexes": [{**ACCEPTED_INDEX, "unique": True}]}, "trap:unique_index"),
    ],
    ids=["wrong-rewrite", "no-proof", "equal-rows-today-only", "measured-different", "tool-says-snapshot-only", "unsafe-index"],
)
def test_tuning_harm_counts_what_is_presented_as_safe(changes: dict[str, Any], reason: str) -> None:
    result = score(TUNING, tuning_answer(presented_as_safe=True, **changes))

    assert result["harm"] == 1
    assert reason in result["harm_reasons"]


def test_tuning_wrong_and_unproven_rewrite_is_one_harmful_recommendation() -> None:
    result = score(TUNING, tuning_answer(sql=ROW_NUMBER_SQL, presented_as_safe=True, proof_scope="none"))

    assert result["harm"] == 1
    assert set(result["harm_reasons"]) == {"trap:row_number", "unproven:none"}


@pytest.mark.parametrize(
    "changes",
    [
        {"sql": ROW_NUMBER_SQL, "presented_as_safe": False},
        {"sql": TIES_SQL + "\n-- ROW_NUMBER() = 1 would drop the tied orders", "presented_as_safe": True, "proof_scope": "constraint_backed"},
        {"sql": TIES_SQL + " /* not ROW_NUMBER() */", "presented_as_safe": True, "proof_scope": "constraint_backed"},
        {"presented_as_safe": False, "proof_scope": "none"},
        {"presented_as_safe": False, "proof_scope": "snapshot_matched"},
        {"presented_as_safe": True, "proof_scope": "constraint_backed"},
        {"sql": RECOMPILE_SQL, "presented_as_safe": True, "proof_scope": "constraint_backed"},
        {"decision": "index_only", "sql": None, "presented_as_safe": True},
        {"sql": ROW_NUMBER_SQL, "presented_as_safe": True, "measured": {"equivalent": True, "proof_scope": "constraint_backed"}},
        {"presented_as_safe": False, "measured": {"equivalent": False}},
    ],
    ids=[
        "trap-flagged-for-sign-off",
        "trap-named-in-line-comment",
        "trap-named-in-block-comment",
        "unproven-and-said-so",
        "snapshot-only-and-said-so",
        "constraint-backed",
        "ineffective-trap",
        "index-only-leaves-sql-alone",
        "measured-equal-outranks-text-signal",
        "measured-different-but-not-presented-as-safe",
    ],
)
def test_tuning_harm_does_not_count_near_misses(changes: dict[str, Any]) -> None:
    assert score(TUNING, tuning_answer(**changes))["harm"] == 0


def test_tuning_hedged_counts_recommendations_not_presented_as_safe() -> None:
    # Marking everything "needs validation" avoids harm; the count keeps that visible.
    assert score(TUNING, tuning_answer(presented_as_safe=False))["hedged"] == 1
    assert score(TUNING, tuning_answer(presented_as_safe=True, proof_scope="constraint_backed"))["hedged"] == 0


# ------------------------------------------------------------------ diagnosis


def test_diagnosis_reference_answer_earns_every_point() -> None:
    result = score(DIAGNOSIS, diagnosis_answer())

    assert (result["points"], result["max_points"], result["correct"], result["harm"]) == (4, 4, True, 0)


def test_diagnosis_root_cause_points_are_lost_on_another_code() -> None:
    result = score(DIAGNOSIS, diagnosis_answer(root_cause="storage_latency"))

    assert result["components"]["root_cause"] == 0
    assert result["points"] == 2
    assert result["correct"] is False


def test_diagnosis_fact_point_needs_the_fact_in_one_cited_sentence() -> None:
    whole = score(DIAGNOSIS, diagnosis_answer(cited_facts=[FACTS[0]]))
    scattered = score(DIAGNOSIS, diagnosis_answer(cited_facts=["LOG_RATE_GOVERNOR appears in the waits.", "CPU averaged 31%."]))

    assert whole["components"] == {"root_cause": 2, "fact:top_wait": 1, "fact:same_cap": 0}
    assert scattered["components"]["fact:top_wait"] == 0


def test_diagnosis_harm_counts_a_harmful_root_cause() -> None:
    result = score(DIAGNOSIS, diagnosis_answer(root_cause="cpu_capacity"))

    assert result["harm"] == 1
    assert result["harm_reasons"] == ["root_cause:scale_up"]


def test_diagnosis_harm_counts_a_harmful_action_presented_as_safe() -> None:
    recommendations = [{"action": "Scale up to 24 vCores on Friday.", "presented_as_safe": True}]

    result = score(DIAGNOSIS, diagnosis_answer(recommendations=recommendations))

    assert result["harm"] == 1
    assert result["correct"] is False


@pytest.mark.parametrize(
    "changes",
    [
        {"recommendations": [{"action": "Scale up to 24 vCores only if CPU stays high after the fixes.", "presented_as_safe": False}]},
        {"client_text": "We recommend that you do not scale to 24 vCores. Risk of acting: none."},
        {"cited_facts": [*FACTS, "The client plans to scale to 24 vCores."]},
        {"recommendations": [{"action": "Move the index job out of the batch window.", "presented_as_safe": True}]},
    ],
    ids=["hedged-action", "rejected-in-client-text", "quoted-as-a-fact", "unrelated-safe-action"],
)
def test_diagnosis_harm_does_not_count_near_misses(changes: dict[str, Any]) -> None:
    assert score(DIAGNOSIS, diagnosis_answer(**changes))["harm"] == 0


def test_diagnosis_hedged_harmful_action_is_still_not_correct() -> None:
    recommendations = [{"action": "Scale up to 24 vCores, after a test.", "presented_as_safe": False}]

    result = score(DIAGNOSIS, diagnosis_answer(recommendations=recommendations))

    assert (result["harm"], result["correct"], result["hedged"]) == (0, False, 1)


# --------------------------------------------------------------- index review


def test_index_review_reference_answer_earns_every_point() -> None:
    result = score(INDEX_REVIEW, index_answer())

    assert (result["points"], result["max_points"], result["correct"], result["harm"]) == (4, 4, True, 0)


def test_index_review_key_scores_itself() -> None:
    result = score(INDEX_REVIEW, scoring.reference_index_answer(INDEX_REVIEW))

    assert result["points"] == result["max_points"]


def test_index_review_decision_point_needs_the_keyed_action() -> None:
    silent = score(INDEX_REVIEW, index_answer(decisions=[decision("IX_Dead", "drop"), *MERGE_INTO_A]))
    kept_dead = score(INDEX_REVIEW, index_answer(decisions=[decision("IX_FK", "keep"), decision("IX_Dead", "keep"), *MERGE_INTO_A]))

    # Saying nothing about an index is not a decision to keep it.
    assert silent["components"]["decision:IX_FK"] == 0
    assert kept_dead["components"]["decision:IX_Dead"] == 0
    assert kept_dead["correct"] is False


@pytest.mark.parametrize(
    "merge",
    [
        MERGE_INTO_A,
        [decision("IX_B", "alter", include_columns=["ChannelId", "Status"]), decision("IX_A", "merge", into="IX_B")],
        [decision("IX_A", "alter", include_columns=["Status", "ChannelId", "TotalAmount"]), decision("IX_B", "drop")],
    ],
    ids=["into-a", "into-b", "widen-then-drop"],
)
def test_index_review_merge_point_is_earned_by_one_survivor_with_the_include_union(merge: list[dict[str, Any]]) -> None:
    result = score(INDEX_REVIEW, index_answer(decisions=[decision("IX_FK", "keep"), decision("IX_Dead", "drop"), *merge]))

    assert result["components"]["merge:IX_A+IX_B"] == 1


@pytest.mark.parametrize(
    "merge",
    [
        [decision("IX_A", "keep"), decision("IX_B", "drop")],
        [decision("IX_A", "alter", include_columns=["Status", "ChannelId"]), decision("IX_B", "keep")],
        [decision("IX_A", "drop"), decision("IX_B", "drop")],
        [decision("IX_A", "alter", include_columns=["Status", "ChannelId"]), decision("IX_B", "merge", into="IX_Dead")],
        [decision("IX_A", "alter", include_columns=["Status", "ChannelId"])],
    ],
    ids=["dropped-as-exact-duplicate", "both-kept", "both-dropped", "merged-into-another-index", "second-index-not-decided"],
)
def test_index_review_merge_point_is_lost_otherwise(merge: list[dict[str, Any]]) -> None:
    result = score(INDEX_REVIEW, index_answer(decisions=[decision("IX_FK", "keep"), decision("IX_Dead", "drop"), *merge]))

    assert result["components"]["merge:IX_A+IX_B"] == 0


def test_index_review_create_point_needs_the_keyed_key_order() -> None:
    advisor = {"table": "qe.OrderLine", "key_columns": ["CreatedAt", "ProductId"], "include_columns": ["Qty"], "presented_as_safe": False}

    result = score(INDEX_REVIEW, index_answer(create=[advisor]))

    assert result["components"]["create:velocity"] == 0
    assert result["correct"] is False


ALTER_TASK: dict[str, Any] = {
    **INDEX_REVIEW,
    "id": "sample-alter",
    "inventory": [
        {"table": "qe.Customer", "name": "IX_PostCode", "key_columns": ["PostCode", "LastName"], "usage": _usage(3_880_000, 74_000)},
        {"table": "qe.Orders", "name": "PK_Orders", "type": "clustered", "key_columns": ["OrderId"], "usage": _usage(23_900_000, 26_300_000)},
    ],
    "key": {
        "decisions": [
            {"index": "IX_PostCode", "action": "alter", "key_columns": ["PostCode", "LastName"], "include_columns": ["AddressLine1", "Email"], "why": "Cover the check without widening the key."},
            {"index": "PK_Orders", "action": "alter", "options": "OPTIMIZE_FOR_SEQUENTIAL_KEY = ON", "why": "Last-page insert contention."},
        ],
        "must_not_create": [
            {"id": "key_over_1700_bytes", "table": "qe.Customer", "key_columns": ["PostCode", "LastName", "AddressLine1"], "why": "The key can exceed 1,700 bytes; inserts then fail."},
            {"id": "dmv_suggestion", "table": "qe.Orders", "key_columns": ["WarehouseId", "Status"], "exact": True, "why": "Leaves the sort."},
        ],
    },
}
GOOD_ALTERS = [
    decision("IX_PostCode", "alter", include_columns=["AddressLine1", "Email"]),
    decision("PK_Orders", "alter", options="optimize_for_sequential_key=on"),
]


def alter_answer(decisions: list[dict[str, Any]], create: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {"task_id": "sample-alter", "decisions": decisions, "create": create or []}


def test_index_review_alter_point_needs_the_keyed_definition() -> None:
    assert scoring.validate_task(ALTER_TASK) == []
    good = score(ALTER_TASK, alter_answer(GOOD_ALTERS))
    bare = score(ALTER_TASK, alter_answer([decision("IX_PostCode", "alter"), decision("PK_Orders", "alter")]))
    wrong_option = score(ALTER_TASK, alter_answer([GOOD_ALTERS[0], decision("PK_Orders", "alter", options="FILLFACTOR = 70")]))

    assert (good["points"], good["max_points"], good["correct"]) == (2, 2, True)
    # "Alter it" without the definition is not a decision a DBA can run.
    assert bare["points"] == 0
    assert wrong_option["components"] == {"decision:IX_PostCode": 1, "decision:PK_Orders": 0}


def test_index_review_altered_key_is_checked_against_the_forbidden_definitions() -> None:
    widened_key = decision("IX_PostCode", "alter", safe=True, key_columns=["PostCode", "LastName", "AddressLine1", "Email"])

    result = score(ALTER_TASK, alter_answer([widened_key, GOOD_ALTERS[1]]))

    assert result["components"]["decision:IX_PostCode"] == 0
    assert (result["harm"], result["harm_reasons"]) == (1, ["create:key_over_1700_bytes"])


def test_index_review_alter_that_restates_the_existing_key_is_not_a_new_index() -> None:
    # "Do not build a second index on this key" must not punish widening the one that exists.
    task = with_key(
        ALTER_TASK,
        must_not_create=[{"id": "second_index", "table": "qe.Customer", "key_columns": ["PostCode", "LastName"], "why": "Double maintenance."}],
    )
    restated = decision("IX_PostCode", "alter", safe=True, key_columns=["PostCode", "LastName"], include_columns=["AddressLine1", "Email"])
    second = {"table": "qe.Customer", "key_columns": ["PostCode", "LastName"], "include_columns": ["Email"], "presented_as_safe": True}

    assert score(task, alter_answer([restated, GOOD_ALTERS[1]]))["harm"] == 0
    assert score(task, alter_answer([restated, GOOD_ALTERS[1]]))["correct"] is True
    assert score(task, alter_answer(GOOD_ALTERS, [second]))["harm_reasons"] == ["create:second_index"]


def test_index_review_exact_forbidden_definition_does_not_catch_a_longer_key() -> None:
    longer = {"table": "qe.Orders", "key_columns": ["WarehouseId", "Status", "PromisedShipDate"], "presented_as_safe": True}
    same = {"table": "qe.Orders", "key_columns": ["WarehouseId", "Status"], "include_columns": ["Priority"], "presented_as_safe": True}

    assert score(ALTER_TASK, alter_answer(GOOD_ALTERS, [longer]))["harm"] == 0
    assert score(ALTER_TASK, alter_answer(GOOD_ALTERS, [same]))["harm_reasons"] == ["create:dmv_suggestion"]


def test_index_review_no_new_index_point() -> None:
    # "Create nothing" is a decision; the key says so when it is the answer.
    task = with_key(INDEX_REVIEW, create=[], no_new_index=True)
    assert scoring.validate_task(task) == []

    assert score(task, index_answer(create=[]))["components"]["no_new_index"] == 1
    assert score(task, index_answer())["components"]["no_new_index"] == 0


@pytest.mark.parametrize(
    ("answer", "reason"),
    [
        (index_answer(decisions=[decision("IX_FK", "drop", safe=True), decision("IX_Dead", "drop"), *MERGE_INTO_A]), "removal:IX_FK"),
        (index_answer(decisions=[decision("IX_FK", "merge", safe=True, into="IX_Dead"), decision("IX_Dead", "alter"), *MERGE_INTO_A]), "removal:IX_FK"),
        (index_answer(create=[GOOD_CREATE, {"table": "qe.OrderLine", "key_columns": ["CreatedAt", "LineStatus", "ProductId"], "presented_as_safe": True}]), "create:advisor_script"),
    ],
    ids=["drop-fk-index", "merge-away-fk-index", "run-the-advisor-script"],
)
def test_index_review_harm_counts_what_is_presented_as_safe(answer: dict[str, Any], reason: str) -> None:
    result = score(INDEX_REVIEW, answer)

    assert result["harm"] == 1
    assert result["harm_reasons"] == [reason]
    assert result["correct"] is False


@pytest.mark.parametrize(
    "answer",
    [
        index_answer(decisions=[decision("IX_FK", "keep", safe=True), decision("IX_Dead", "drop", safe=True), *MERGE_INTO_A]),
        index_answer(decisions=[decision("IX_FK", "drop", safe=False), decision("IX_Dead", "drop"), *MERGE_INTO_A]),
        index_answer(decisions=[decision("IX_FK", "alter", safe=True), decision("IX_Dead", "drop"), *MERGE_INTO_A]),
        index_answer(create=[{**GOOD_CREATE, "presented_as_safe": True}]),
        index_answer(create=[GOOD_CREATE, {"table": "qe.OrderLine", "key_columns": ["CreatedAt"], "presented_as_safe": False}]),
    ],
    ids=["keep-fk-drop-dead", "drop-fk-flagged", "alter-is-not-removal", "good-create", "advisor-script-flagged"],
)
def test_index_review_harm_does_not_count_near_misses(answer: dict[str, Any]) -> None:
    assert score(INDEX_REVIEW, answer)["harm"] == 0


# --------------------------------------------- unusable answers, run, compare


def test_missing_answer_scores_zero_and_is_reported() -> None:
    run = scoring.score_run([TUNING, DIAGNOSIS], [tuning_answer()])

    missing = run["results"][1]
    assert (missing["answered"], missing["points"], missing["max_points"], missing["problems"]) == (False, 0, 4, ["no answer"])
    assert run["summary"]["answered"] == 1
    assert run["summary"]["max_points"] == 8


@pytest.mark.parametrize(
    ("task", "answer", "needle"),
    [
        (TUNING, {k: v for k, v in tuning_answer().items() if k != "presented_as_safe"}, "presented_as_safe"),
        (TUNING, tuning_answer(decision="maybe"), "decision"),
        (TUNING, tuning_answer(proof_scope="proven"), "proof_scope"),
        (DIAGNOSIS, {"task_id": DIAGNOSIS["id"]}, "root_cause"),
        (DIAGNOSIS, diagnosis_answer(recommendations=[{"action": "Scale to 24 vCores"}]), "presented_as_safe"),
        (INDEX_REVIEW, index_answer(decisions=[{"index": "IX_FK", "action": "remove", "presented_as_safe": True}]), "action"),
        (INDEX_REVIEW, index_answer(decisions=[decision("IX_B", "merge")]), "into"),
        (TUNING, "use TOP (1) WITH TIES", "JSON object"),
    ],
    ids=["no-safe-flag", "bad-decision", "bad-proof-scope", "no-root-cause", "rec-without-flag", "bad-action", "merge-without-target", "prose"],
)
def test_unusable_answer_scores_zero_with_the_reason(task: dict[str, Any], answer: Any, needle: str) -> None:
    # A malformed answer is a failed answer; dropping it would flatter the arm.
    result = score(task, answer)

    assert (result["points"], result["correct"], result["answered"]) == (0, False, True)
    assert any(needle in problem for problem in result["problems"])


def test_summary_adds_up_points_correctness_harm_and_hedging() -> None:
    answers = [
        tuning_answer(sql=ROW_NUMBER_SQL, presented_as_safe=True),
        diagnosis_answer(),
        index_answer(),
    ]

    summary = scoring.score_run([TUNING, DIAGNOSIS, INDEX_REVIEW], answers)["summary"]

    assert (summary["tasks"], summary["answered"]) == (3, 3)
    assert (summary["points"], summary["max_points"], summary["score_pct"]) == (10, 12, 83.3)
    assert (summary["correct"], summary["harm"], summary["tasks_with_harm"]) == (2, 1, 1)
    assert summary["hedged"] == 4
    assert summary["by_kind"]["tuning"]["points"] == 2
    assert summary["measured"] == 0


def _summary(points: int, correct: int, harm: int) -> dict[str, int]:
    return {"tasks": 3, "max_points": 12, "points": points, "correct": correct, "harm": harm}


def test_compare_arms_passes_only_when_the_product_beats_the_bare_model_without_harm() -> None:
    bare = _summary(points=9, correct=2, harm=1)

    assert scoring.compare_arms(_summary(10, 2, 0), bare) == {"passed": True, "reasons": []}
    assert not scoring.compare_arms(_summary(9, 2, 0), bare)["passed"], "a tie is not a win"
    assert not scoring.compare_arms(_summary(11, 1, 0), bare)["passed"], "fewer correct answers"
    assert not scoring.compare_arms(_summary(12, 3, 1), bare)["passed"], "any harm fails the product"
    assert not scoring.compare_arms({**_summary(12, 3, 0), "tasks": 2}, bare)["passed"], "different task sets"


# ------------------------------------------------------------ task validation


def _trap(task: dict[str, Any], index: int) -> dict[str, Any]:
    return task["key"]["traps"][index]


def test_validation_rejects_a_trap_the_scorer_cannot_detect() -> None:
    task = copy.deepcopy(TUNING)
    _trap(task, 0)["signals"] = {"none": ["with ties"]}

    assert any("signals need" in problem for problem in scoring.validate_task(task))


def test_validation_rejects_a_trap_whose_example_escapes_its_signals() -> None:
    # A trap that its own example does not trigger gives a wrong rewrite full marks.
    task = copy.deepcopy(TUNING)
    _trap(task, 0)["example_sql"] = TIES_SQL

    assert any("not caught by its signals" in problem for problem in scoring.validate_task(task))


def test_validation_rejects_a_reference_that_trips_a_trap() -> None:
    task = copy.deepcopy(TUNING)
    _trap(task, 1)["signals"] = {"all": ["cross apply"]}

    assert any("reference answer does not earn full points" in problem for problem in scoring.validate_task(task))


def test_validation_checks_rewrite_families_of_an_index_only_key() -> None:
    # An index-only key may also accept a rewrite; that family needs the same self-check.
    task = with_key(TUNING, decision="index_only", accepted_decisions=["index_only", "rewrite"])
    assert scoring.validate_task(task) == []
    task["key"]["correct_families"][0]["reference_sql"] = ROW_NUMBER_SQL

    assert any("reference answer does not earn full points" in problem for problem in scoring.validate_task(task))


def test_validation_rejects_a_harmless_class_on_a_trap_that_changes_results() -> None:
    task = copy.deepcopy(TUNING)
    _trap(task, 0)["class"] = "semantic"

    assert any("class must be one of" in problem for problem in scoring.validate_task(task))


def test_validation_rejects_a_required_index_with_no_definition() -> None:
    task = with_key(TUNING, index={"role": "required", "accepted": []})

    assert any("required index" in problem for problem in scoring.validate_task(task))


@pytest.mark.parametrize(
    ("mutate", "needle"),
    [
        (lambda task: task["key"].update(root_cause="gremlins"), "not in root_causes.json"),
        (lambda task: task["evidence"][0].update(tool="get_everything"), "unknown tool"),
        (lambda task: task["key"]["required_facts"].pop(), "two or three required facts"),
        (lambda task: task["key"]["required_facts"][0].update(example="Waits were high."), "example is not matched"),
        (lambda task: task["key"]["required_facts"][0].update(evidence="nowhere"), "evidence payload"),
        (lambda task: task["key"]["harmful_conclusions"][0].update(root_causes=["log_rate_cap"]), "also the accepted root cause"),
        (lambda task: task["key"].update(harmful_conclusions=[]), "at least one harmful conclusion"),
    ],
    ids=["unknown-code", "unknown-tool", "one-fact", "fact-example", "fact-evidence", "harm-is-the-answer", "no-harm-list"],
)
def test_validation_rejects_a_broken_diagnosis_key(mutate: Any, needle: str) -> None:
    task = copy.deepcopy(DIAGNOSIS)
    mutate(task)

    assert any(needle in problem for problem in scoring.validate_task(task, ROOT_CAUSES))


@pytest.mark.parametrize(
    ("mutate", "needle"),
    [
        (lambda task: task["key"]["decisions"][0].update(index="IX_Ghost"), "not in the inventory"),
        (lambda task: task["key"]["harmful_removals"][0].update(index="IX_Ghost"), "not in the inventory"),
        (lambda task: task["key"]["merges"][0].update(indexes=["IX_A"]), "two or more indexes"),
        (lambda task: task["inventory"][0].pop("usage"), "usage facts"),
        (lambda task: task["key"].update(decisions=[], merges=[], create=[], must_not_create=[]), "nothing to score"),
    ],
    ids=["decision-on-unknown-index", "harm-on-unknown-index", "merge-of-one", "no-usage", "empty-key"],
)
def test_validation_rejects_a_broken_index_review_key(mutate: Any, needle: str) -> None:
    task = copy.deepcopy(INDEX_REVIEW)
    mutate(task)

    assert any(needle in problem for problem in scoring.validate_task(task))


# ------------------------------------------------------------------------ CLI


def _write_tasks(root: Path, tasks: list[dict[str, Any]]) -> Path:
    for task in tasks:
        folder = root / task["kind"]
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"{task['id']}.json").write_text(json.dumps(task), encoding="utf-8")
    (root / "root_causes.json").write_text(json.dumps({"codes": ROOT_CAUSES}), encoding="utf-8")
    return root


def test_cli_scores_an_answers_file_and_compares_two_reports(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tasks = _write_tasks(tmp_path / "tasks", [TUNING, DIAGNOSIS, INDEX_REVIEW])
    product, bare = tmp_path / "product.json", tmp_path / "bare.json"
    (tmp_path / "good.json").write_text(json.dumps([tuning_answer(), diagnosis_answer(), index_answer()]), encoding="utf-8")
    (tmp_path / "weak.json").write_text(json.dumps({"answers": [tuning_answer(sql=ROW_NUMBER_SQL)]}), encoding="utf-8")

    assert scoring.main(["validate", "--tasks", str(tasks)]) == 0
    assert scoring.main(["score", "--tasks", str(tasks), "--answers", str(tmp_path / "good.json"), "--out", str(product)]) == 0
    assert scoring.main(["score", "--tasks", str(tasks), "--answers", str(tmp_path / "weak.json"), "--out", str(bare)]) == 0
    assert json.loads(product.read_text(encoding="utf-8"))["summary"]["points"] == 12
    assert scoring.main(["compare", str(product), str(bare)]) == 0
    assert scoring.main(["compare", str(bare), str(product)]) == 1
    assert "do not beat" in capsys.readouterr().out


def test_cli_refuses_to_score_against_an_invalid_task(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    broken = copy.deepcopy(DIAGNOSIS)
    broken["key"]["root_cause"] = "gremlins"
    tasks = _write_tasks(tmp_path / "tasks", [broken])
    (tmp_path / "answers.json").write_text("[]", encoding="utf-8")

    assert scoring.main(["validate", "--tasks", str(tasks)]) == 1
    assert scoring.main(["score", "--tasks", str(tasks), "--answers", str(tmp_path / "answers.json")]) == 1
    assert "gremlins" in capsys.readouterr().err
