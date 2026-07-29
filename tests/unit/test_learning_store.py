from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from azure_sql_mcp.learning_contracts import DecisionRecordV1, LessonV1, OutcomeReviewV1
from azure_sql_mcp.learning_store import (
    ConcurrencyError,
    IdempotencyConflictError,
    LearningStore,
    LearningStoreError,
    LifecycleError,
)


def make_decision(**overrides) -> DecisionRecordV1:
    values = {
        "skill": "sql_optimizer",
        "skill_version": "1.0",
        "case_id": "case-1",
        "session_id": "session-1",
        "candidate_id": "candidate-1",
        "learning_key": "tactic-1",
        "consumed_evidence_refs": ("evidence-1",),
        "subject_kind": "query",
        "subject_fingerprint": "subject-1",
        "query_fingerprint": "query-1",
        "based_on_review_ids": (),
        "tactic": "bounded-rewrite",
        "expected_result": {"class": "improved"},
        "confidence": 0.7,
        "uncertainty": {"kind": "bounded"},
        "evaluator_fingerprint": "evaluator-1",
        "runtime_fingerprint": "runtime-1",
        "runtime_compatibility_fingerprint": "runtime-compat-1",
        "scope": {"database_fingerprint": "db-1", "server_fingerprint": "server-1"},
    }
    values.update(overrides)
    return DecisionRecordV1(**values)


def make_lesson(**overrides) -> LessonV1:
    values = {
        "learning_key": "tactic-1",
        "trigger": {"kind": "candidate"},
        "action": {"kind": "review"},
        "preconditions": {"kind": "bounded"},
        "required_evidence": ("evidence-1",),
        "applicable_skills": ("sql_optimizer",),
        "applicable_scopes": ({"database_fingerprint": "db-1", "runtime_compatibility_fingerprint": "runtime-compat-1"},),
        "query_fingerprints": ("query-1", "query-2"),
        "counterexamples": ({"kind": "bounded-risk"},),
        "support_refs": ("review-1",),
        "reviewer": "maintainer",
        "support_session_ids": ("session-1", "session-2"),
        "support_query_fingerprints": ("query-1", "query-2"),
    }
    values.update(overrides)
    return LessonV1(**values)


def test_store_is_private_additive_and_restarts(tmp_path) -> None:
    with LearningStore(state_dir=tmp_path) as store:
        decision = store.create_decision(make_decision(), idempotency_key="decision-create")
        database_path = tmp_path / "performance.sqlite3"
        assert database_path.exists()
        if os.name != "nt":
            assert database_path.stat().st_mode & 0o777 == 0o600
            assert tmp_path.stat().st_mode & 0o777 == 0o700
        assert store.list_events(aggregate_id=decision.decision_id)[0]["event_type"] == "decision.created"
    with LearningStore(state_dir=tmp_path) as reopened:
        assert reopened.get_decision(decision.decision_id) == decision
        assert len(reopened.list_events()) == 1


def test_store_rejects_unknown_decision_lineage(tmp_path) -> None:
    with LearningStore(state_dir=tmp_path) as store:
        with pytest.raises(LifecycleError):
            store.create_decision(
                make_decision(based_on_review_ids=("review-missing",))
            )


def test_idempotency_replays_and_conflicts_without_persisting_raw_key(tmp_path) -> None:
    with LearningStore(state_dir=tmp_path) as store:
        original = make_decision()
        replay = store.create_decision(original, idempotency_key="same-request")
        assert store.create_decision(original, idempotency_key="same-request") == replay
        with pytest.raises(IdempotencyConflictError):
            store.create_decision(make_decision(session_id="session-2"), idempotency_key="same-request")
        raw = (tmp_path / "performance.sqlite3").read_bytes()
        assert b"same-request" not in raw


def test_optimistic_versions_and_immutable_evidence_refs(tmp_path) -> None:
    with LearningStore(state_dir=tmp_path) as store:
        original = store.create_decision(make_decision())
        reviewed = replace(original, lifecycle="reviewed", version=1, updated_at_utc=original.updated_at_utc)
        store.save_decision(reviewed, expected_version=0)
        with pytest.raises(ConcurrencyError):
            store.save_decision(replace(reviewed, version=2), expected_version=0)
        with pytest.raises(LifecycleError):
            store.save_decision(
                replace(reviewed, consumed_evidence_refs=("evidence-2",), version=2),
                expected_version=1,
            )
        with pytest.raises(LifecycleError):
            store.save_decision(
                replace(reviewed, tactic="different-tactic", version=2),
                expected_version=1,
            )


def test_only_cli_can_activate_reject_retire_or_supersede_lessons(tmp_path) -> None:
    with LearningStore(state_dir=tmp_path) as store:
        developing = store.create_lesson(
            make_lesson(lesson_id="lesson-developing", status="proposed")
        )
        lesson = store.create_lesson(make_lesson(status="eligible"))
        replacement = store.create_lesson(
            make_lesson(
                lesson_id="lesson-replacement",
                status="eligible",
                supersedes_lesson_id=lesson.lesson_id,
            )
        )
        with pytest.raises(
            LifecycleError,
            match="must reach eligible status",
        ):
            store.transition_lesson(
                developing.lesson_id,
                "active",
                actor="cli",
                reviewer="maintainer",
            )
        with pytest.raises(LifecycleError):
            store.transition_lesson(lesson.lesson_id, "active", actor="service")
        active = store.transition_lesson(lesson.lesson_id, "active", actor="cli", reviewer="maintainer")
        assert active.status == "active"
        with pytest.raises(LifecycleError):
            store.save_lesson(
                replace(active, reviewer="other-reviewer", version=2),
                expected_version=1,
                actor="cli",
            )
        active_replacement = store.transition_lesson(
            replacement.lesson_id,
            "active",
            actor="cli",
            reviewer="maintainer",
        )
        with pytest.raises(LifecycleError):
            store.transition_lesson(
                active.lesson_id,
                "superseded",
                actor="service",
                superseded_by_lesson_id=active_replacement.lesson_id,
            )
        superseded = store.transition_lesson(
            active.lesson_id,
            "superseded",
            actor="cli",
            reviewer="maintainer",
            superseded_by_lesson_id=active_replacement.lesson_id,
        )
        assert superseded.status == "superseded"
        retired = store.transition_lesson(
            active_replacement.lesson_id,
            "retired",
            actor="cli",
            reviewer="maintainer",
        )
        assert retired.status == "retired"
        with pytest.raises(LifecycleError):
            store.import_active_lesson(active)


def test_reviews_are_append_only(tmp_path) -> None:
    with LearningStore(state_dir=tmp_path) as store:
        decision = store.create_decision(make_decision())
        terminal = store.record_terminal_link(
            decision_id=decision.decision_id,
            source_tool="benchmark",
            database_fingerprint="db-1",
            scope=decision.scope,
            outcome_summary={"class": "improved"},
            evidence_refs=(),
            response_fingerprint="response-1",
            created_at_utc=decision.created_at_utc,
        )
        review = OutcomeReviewV1(
            decision_id=decision.decision_id,
            terminal_evidence_refs=(terminal["link_id"],),
            observed_result={"class": "improved"},
            prediction_error={"class": "none"},
            causal_strength="strong",
            created_at_utc=decision.created_at_utc,
            completed_at_utc=decision.created_at_utc,
            complete=True,
            alignment="aligned",
            safety_signal="passed",
            equivalence_signal="passed",
            cleanup_signal="passed",
            material_regression_signal="passed",
            unknown_outcome=False,
        )
        stored = store.create_review(review)
        with pytest.raises(LifecycleError):
            store.create_review(replace(stored, version=1))
        reviewed_decision = replace(
            decision,
            lifecycle="reviewed",
            version=1,
        )
        store.save_decision(reviewed_decision, expected_version=0)
        later = (
            datetime.fromisoformat(decision.created_at_utc) + timedelta(seconds=1)
        ).isoformat()
        other_decision = store.create_decision(
            replace(
                reviewed_decision,
                decision_id="decision-2",
                session_id="session-2",
                based_on_review_ids=(stored.review_id,),
                created_at_utc=later,
                updated_at_utc=later,
                lifecycle="recorded",
                version=0,
            )
        )
        with pytest.raises(LifecycleError):
            store.create_review(
                replace(
                    stored,
                    review_id="review-cross",
                    decision_id=other_decision.decision_id,
                )
            )
        assert len(store.list_events(aggregate_type="review")) == 1


def test_terminal_link_validates_supplied_underlying_refs_but_allows_zero(tmp_path) -> None:
    with LearningStore(state_dir=tmp_path) as store:
        decision = store.create_decision(make_decision())
        empty = store.record_terminal_link(
            decision_id=decision.decision_id,
            source_tool="benchmark",
            database_fingerprint="db-1",
            scope=decision.scope,
            outcome_summary={"class": "observed"},
            evidence_refs=(),
            response_fingerprint="response-empty",
        )
        assert empty["evidence_refs"] == []
        with pytest.raises(LifecycleError):
            store.record_terminal_link(
                decision_id=decision.decision_id,
                source_tool="benchmark",
                database_fingerprint="db-1",
                scope={},
                outcome_summary={"class": "observed"},
                evidence_refs=(),
                response_fingerprint="response-missing-scope",
            )
        with pytest.raises(LearningStoreError):
            store.record_terminal_link(
                decision_id=decision.decision_id,
                source_tool="benchmark",
                database_fingerprint="db-1",
                scope=decision.scope,
                outcome_summary={"class": "observed"},
                evidence_refs=("terminal-link-not-underlying",),
                response_fingerprint="response-invalid",
            )
