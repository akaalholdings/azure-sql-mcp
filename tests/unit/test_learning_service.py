from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from azure_sql_mcp.learning_contracts import DecisionRecordV1, HandoffV1, OutcomeReviewV1
from azure_sql_mcp.learning_service import LearningService, LearningServiceError
from azure_sql_mcp.learning_store import LearningStore


BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def ts(minutes: int) -> str:
    return (BASE + timedelta(minutes=minutes)).isoformat()


def make_decision(index: int, *, session: str, query: str, **overrides) -> DecisionRecordV1:
    values = {
        "skill": "sql_optimizer",
        "skill_version": "1.0",
        "case_id": "case-1",
        "session_id": session,
        "candidate_id": f"candidate-{index}",
        "learning_key": "tactic-1",
        "consumed_evidence_refs": (f"evidence-{index}",),
        "subject_kind": "query",
        "subject_fingerprint": f"subject-{index}",
        "query_fingerprint": query,
        "based_on_review_ids": (),
        "tactic": "bounded-rewrite",
        "expected_result": {"class": "improved"},
        "confidence": 0.8,
        "uncertainty": {"kind": "bounded"},
        "evaluator_fingerprint": "evaluator-1",
        "runtime_fingerprint": "runtime-1",
        "runtime_compatibility_fingerprint": "runtime-compat-1",
        "tool_schema_fingerprint": "tools-1",
        "sanitized_config_fingerprint": "config-1",
        "scope": {"database_fingerprint": "db-1", "server_fingerprint": "server-1"},
        "tags": ("join",),
        "created_at_utc": ts(index * 3),
        "updated_at_utc": ts(index * 3),
    }
    values.update(overrides)
    return DecisionRecordV1(**values)


def make_review(decision: DecisionRecordV1, index: int, **overrides) -> OutcomeReviewV1:
    values = {
        "decision_id": decision.decision_id,
        "terminal_evidence_refs": (f"terminal-link-{index}",),
        "observed_result": {"class": "improved"},
        "prediction_error": {"class": "none"},
        "counterexamples": ({"kind": "bounded-risk"},),
        "next_observation": {"kind": "observe-next-run"},
        "causal_strength": "strong",
        "created_at_utc": ts(index * 3 + 1),
        "completed_at_utc": ts(index * 3 + 2),
        "complete": True,
        "alignment": "aligned",
        "safety_signal": "passed",
        "equivalence_signal": "passed",
        "cleanup_signal": "passed",
        "material_regression_signal": "passed",
        "unknown_outcome": False,
    }
    values.update(overrides)
    return OutcomeReviewV1(**values)


def service() -> tuple[LearningStore, LearningService]:
    store = LearningStore(db_path=":memory:")
    return store, LearningService(store, clock=lambda: ts(20))


def add_review(
    svc: LearningService,
    index: int,
    session: str,
    query: str,
    *,
    decision_overrides: dict | None = None,
    **review_overrides,
) -> OutcomeReviewV1:
    overrides = dict(decision_overrides or {})
    if "based_on_review_ids" not in overrides:
        existing_reviews = svc.store.list_reviews()
        if existing_reviews:
            latest_review = max(
                existing_reviews,
                key=lambda item: item.completed_at_utc or item.created_at_utc,
            )
            overrides["based_on_review_ids"] = (latest_review.review_id,)
    decision = make_decision(index, session=session, query=query, **overrides)
    svc.record_decision(decision)
    terminal = svc.record_terminal_link(
        decision_id=decision.decision_id,
        source_tool="benchmark",
        database_fingerprint="db-1",
        scope=decision.scope,
        outcome_summary={"class": "improved"},
        evidence_refs=(f"evidence-{index}",),
        response_fingerprint=f"response-{index}",
        created_at_utc=ts(index * 3 + 1),
    )
    return svc.record_outcome_review(
        make_review(
            decision,
            index,
            terminal_evidence_refs=(terminal["link_id"],),
            **review_overrides,
        )
    )


def propose(svc: LearningService, reviews: list[OutcomeReviewV1]):
    return svc.propose_lesson(
        learning_key="tactic-1",
        review_ids=[review.review_id for review in reviews],
        trigger={"kind": "candidate"},
        action={"kind": "bounded-review"},
        preconditions={"kind": "scope-bound"},
        counterexamples=({"kind": "bounded-risk"},),
        required_evidence=("evidence-1",),
        applicable_skills=("sql_optimizer",),
    )


def initial_reviews(svc: LearningService) -> list[OutcomeReviewV1]:
    return [
        add_review(svc, 1, "session-1", "query-1"),
        add_review(svc, 2, "session-1", "query-2"),
        add_review(svc, 3, "session-2", "query-1"),
    ]


def test_normal_eligibility_requires_three_aligned_reviews_two_sessions_and_two_subjects() -> None:
    store, svc = service()
    lesson = propose(svc, initial_reviews(svc))
    assert lesson.status == "eligible"
    assert lesson.proposal_kind == "normal"
    assert lesson.based_on_review_ids == lesson.support_refs
    store.close()


def test_urgent_failure_proposal_is_inactive_until_maintainer_approval() -> None:
    store, svc = service()
    review = add_review(
        svc,
        1,
        "session-1",
        "query-1",
        alignment="contradiction",
        safety_signal="proven_failure",
    )
    lesson = propose(svc, [review])
    assert lesson.proposal_kind == "urgent"
    assert lesson.status == "proposed"
    store.close()


def test_explicit_human_correction_creates_an_urgent_inactive_proposal() -> None:
    store, svc = service()
    review = add_review(
        svc,
        1,
        "session-1",
        "query-1",
        alignment="contradiction",
        explicit_correction=True,
        correction={"kind": "maintainer-correction"},
    )
    lesson = propose(svc, [review])
    assert lesson.proposal_kind == "urgent"
    assert lesson.status == "proposed"
    store.close()


def test_new_contradiction_revokes_unreviewed_eligibility() -> None:
    store, svc = service()
    lesson = propose(svc, initial_reviews(svc))
    assert lesson.status == "eligible"
    contradiction = add_review(
        svc,
        4,
        "session-3",
        "query-3",
        alignment="contradiction",
        safety_signal="proven_failure",
    )
    refreshed = store.get_lesson(lesson.lesson_id)
    assert refreshed.status == "proposed"
    assert contradiction.review_id in refreshed.contradiction_refs
    store.close()


def test_one_safety_failure_quarantines_active_lesson() -> None:
    store, svc = service()
    propose(svc, initial_reviews(svc))
    lesson = store.list_lessons()[0]
    store.transition_lesson(lesson.lesson_id, "active", actor="cli", reviewer="maintainer", at_utc=ts(20))
    add_review(
        svc,
        4,
        "session-3",
        "query-3",
        decision_overrides={"applied_lesson_ids": (lesson.lesson_id,)},
        alignment="contradiction",
        equivalence_signal="proven_failure",
    )
    assert store.get_lesson(lesson.lesson_id).status == "quarantined"
    store.close()


def test_cleanup_contradictions_do_not_trigger_the_material_regression_rule() -> None:
    store, svc = service()
    propose(svc, initial_reviews(svc))
    lesson = store.list_lessons()[0]
    store.transition_lesson(
        lesson.lesson_id,
        "active",
        actor="cli",
        reviewer="maintainer",
        at_utc=ts(21),
    )
    for index, session in ((4, "session-3"), (5, "session-4")):
        add_review(
            svc,
            index,
            session,
            f"query-{index}",
            decision_overrides={"applied_lesson_ids": (lesson.lesson_id,)},
            alignment="contradiction",
            cleanup_signal="proven_failure",
        )
    assert store.get_lesson(lesson.lesson_id).status == "active"
    store.close()


def test_two_independent_material_contradictions_quarantine_active_lesson() -> None:
    store, svc = service()
    propose(svc, initial_reviews(svc))
    lesson = store.list_lessons()[0]
    store.transition_lesson(lesson.lesson_id, "active", actor="cli", reviewer="maintainer", at_utc=ts(21))
    for index, session in ((4, "session-3"), (5, "session-4")):
        add_review(
            svc,
            index,
            session,
            f"query-{index}",
            decision_overrides={"applied_lesson_ids": (lesson.lesson_id,)},
            alignment="contradiction",
            material_regression_signal="proven_failure",
        )
        if index == 4:
            assert store.get_lesson(lesson.lesson_id).status == "active"
    assert store.get_lesson(lesson.lesson_id).status == "quarantined"
    store.close()


def test_recall_is_scoped_versioned_fresh_and_capped_at_three() -> None:
    store, svc = service()
    propose(svc, initial_reviews(svc))
    lesson = store.list_lessons()[0]
    store.transition_lesson(lesson.lesson_id, "active", actor="cli", reviewer="maintainer", at_utc=ts(20))
    recall_args = {
        "skill_version": "1.0",
        "tool_schema_fingerprint": "tools-1",
        "sanitized_config_fingerprint": "config-1",
    }
    scope = {"database_fingerprint": "db-1", "server_fingerprint": "server-1"}
    assert len(svc.recall(skill="sql_optimizer", query_fingerprint="query-1", scope=scope, runtime_compatibility_fingerprint="runtime-compat-1", tags=("join",), now_utc=ts(20), **recall_args)) == 1
    assert svc.recall(skill="sql_optimizer", query_fingerprint="query-1", scope={**scope, "database_fingerprint": "other-db"}, runtime_compatibility_fingerprint="runtime-compat-1", now_utc=ts(20), **recall_args) == []
    assert svc.recall(skill="sql_optimizer", query_fingerprint="query-1", scope=scope, runtime_compatibility_fingerprint="runtime-compat-other", now_utc=ts(20), **recall_args) == []
    assert svc.recall(skill="sql_optimizer", query_fingerprint="query-1", scope=scope, runtime_compatibility_fingerprint="runtime-compat-1", now_utc=ts(60 * 24 * 181), **recall_args) == []
    assert svc.recall(skill="sql_optimizer", query_fingerprint="query-1", scope=scope, runtime_compatibility_fingerprint="runtime-compat-1", now_utc=ts(20), skill_version="2.0", tool_schema_fingerprint="tools-1", sanitized_config_fingerprint="config-1") == []
    store.close()


def test_process_runtime_fingerprint_does_not_partition_compatibility_scope() -> None:
    store, svc = service()
    first = make_decision(
        1,
        session="session-1",
        query="query-1",
        scope={
            "database_fingerprint": "db-1",
            "server_fingerprint": "server-1",
            "runtime_fingerprint": "process-a",
        },
    )
    second = make_decision(
        2,
        session="session-2",
        query="query-2",
        scope={
            "database_fingerprint": "db-1",
            "server_fingerprint": "server-1",
            "runtime_fingerprint": "process-b",
        },
    )
    assert svc._scope_key(first) == svc._scope_key(second)
    store.close()


def test_eligibility_requires_moderate_or_strong_causal_reviews() -> None:
    store, svc = service()
    reviews = [
        add_review(svc, 1, "session-1", "query-1", causal_strength="weak"),
        add_review(svc, 2, "session-1", "query-2"),
        add_review(svc, 3, "session-2", "query-3"),
    ]
    assert propose(svc, reviews).status == "proposed"
    store.close()


def test_unknown_review_does_not_count_as_a_contradiction() -> None:
    store, svc = service()
    unknown = add_review(
        svc,
        1,
        "session-0",
        "query-0",
        causal_strength="unknown",
        alignment="unknown",
        safety_signal="unknown",
        equivalence_signal="unknown",
        cleanup_signal="unknown",
        material_regression_signal="unknown",
        unknown_outcome=True,
    )
    aligned = [
        add_review(svc, 2, "session-1", "query-1"),
        add_review(svc, 3, "session-1", "query-2"),
        add_review(svc, 4, "session-2", "query-3"),
    ]

    lesson = propose(svc, [unknown, *aligned])

    assert lesson.status == "eligible"
    assert lesson.contradiction_refs == ()
    store.close()


def test_decision_lineage_requires_existing_reviewed_prior_compatible_decision() -> None:
    store, svc = service()
    prior_review = add_review(svc, 1, "session-1", "query-1")
    with pytest.raises(
        LearningServiceError,
        match="must reference the prior outcome review",
    ):
        svc.record_decision(
            make_decision(
                2,
                session="session-2",
                query="query-2",
                created_at_utc=ts(10),
                updated_at_utc=ts(10),
            )
        )
    linked = make_decision(
        2,
        session="session-2",
        query="query-2",
        based_on_review_ids=(prior_review.review_id,),
        created_at_utc=ts(10),
        updated_at_utc=ts(10),
    )
    svc.record_decision(linked)
    with pytest.raises(
        LearningServiceError,
        match="must be reviewed before the next decision",
    ):
        svc.record_decision(
            make_decision(
                3,
                session="session-3",
                query="query-3",
                based_on_review_ids=(prior_review.review_id,),
                created_at_utc=ts(11),
                updated_at_utc=ts(11),
            )
        )
    with pytest.raises(LearningServiceError):
        svc.record_decision(
            make_decision(
                3,
                session="session-3",
                query="query-3",
                based_on_review_ids=("review-missing",),
                created_at_utc=ts(11),
                updated_at_utc=ts(11),
            )
        )
    with pytest.raises(LearningServiceError):
        svc.record_decision(
            make_decision(
                4,
                session="session-4",
                query="query-4",
                based_on_review_ids=(prior_review.review_id,),
                scope={"database_fingerprint": "other-db", "server_fingerprint": "server-1"},
                created_at_utc=ts(11),
                updated_at_utc=ts(11),
            )
        )
    with pytest.raises(LearningServiceError):
        svc.record_decision(
            make_decision(
                5,
                session="session-5",
                query="query-5",
                based_on_review_ids=(prior_review.review_id,),
                created_at_utc=ts(2),
                updated_at_utc=ts(2),
            )
        )
    store.close()


def test_handoff_supports_claim_resolve_reopen_and_cancel() -> None:
    store, svc = service()
    handoff = svc.create_handoff(HandoffV1(source_skill="sql_optimizer", target_skill="sql_plan_enforcer", handoff_type="plan-verification", objective={"kind": "verify"}, evidence_refs=("evidence-1",), constraints={}, acceptance_criteria=({"kind": "equivalence"},), created_at_utc=ts(0), updated_at_utc=ts(0)))
    claimed = svc.claim_handoff(handoff.handoff_id, "maintainer")
    resolved = svc.resolve_handoff(claimed.handoff_id, {"class": "verified", "human_decision": True})
    reopened = svc.reopen_handoff(resolved.handoff_id)
    cancelled = svc.cancel_handoff(reopened.handoff_id)
    assert claimed.status == "claimed"
    assert resolved.status == "resolved"
    assert reopened.status == "open" and reopened.reopen_count == 1
    assert cancelled.status == "cancelled"
    store.close()
