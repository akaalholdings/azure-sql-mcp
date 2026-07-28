from __future__ import annotations

from dataclasses import replace

import pytest

from azure_sql_mcp.learning_contracts import (
    ContractValidationError,
    DecisionRecordV1,
    HandoffV1,
    LessonV1,
    OutcomeReviewV1,
)


def decision(**overrides) -> DecisionRecordV1:
    values = {
        "skill": "sql_optimizer",
        "skill_version": "1.0",
        "case_id": "case-1",
        "session_id": "session-1",
        "candidate_id": "candidate-1",
        "learning_key": "seek-tactic",
        "consumed_evidence_refs": ("evidence-1",),
        "subject_kind": "query",
        "subject_fingerprint": "subject-fp-1",
        "query_fingerprint": "query-fp-1",
        "based_on_review_ids": (),
        "tactic": "bounded-rewrite",
        "expected_result": {"latency_class": "improved"},
        "confidence": 0.8,
        "uncertainty": {"kind": "bounded"},
        "evaluator_fingerprint": "evaluator-fp",
        "runtime_fingerprint": "runtime-fp",
        "runtime_compatibility_fingerprint": "runtime-compat-fp",
        "scope": {"database_fingerprint": "db-fp", "server_fingerprint": "server-fp"},
        "tags": ("join", "seek"),
    }
    values.update(overrides)
    return DecisionRecordV1(**values)


def review(**overrides) -> OutcomeReviewV1:
    values = {
        "decision_id": "decision-1",
        "terminal_evidence_refs": ("terminal-link-1",),
        "observed_result": {"latency_class": "improved"},
        "prediction_error": {"class": "none"},
        "counterexamples": ({"kind": "bounded-risk"},),
        "next_observation": {"kind": "observe-next-run"},
        "causal_strength": "strong",
        "created_at_utc": "2026-01-01T00:01:00+00:00",
        "completed_at_utc": "2026-01-01T00:02:00+00:00",
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


def lesson(**overrides) -> LessonV1:
    values = {
        "learning_key": "seek-tactic",
        "trigger": {"tactic": "bounded-rewrite"},
        "action": {"operation": "prefer-seek"},
        "preconditions": {"scope": "known"},
        "required_evidence": ("evidence-1",),
        "applicable_skills": ("sql_optimizer",),
        "applicable_scopes": ({"database_fingerprint": "db-fp", "runtime_compatibility_fingerprint": "runtime-compat-fp"},),
        "query_fingerprints": ("query-fp-1", "query-fp-2"),
        "tags": ("join",),
        "support_refs": ("review-1",),
        "reviewer": "maintainer",
        "support_session_ids": ("session-1", "session-2"),
        "support_query_fingerprints": ("query-fp-1", "query-fp-2"),
    }
    values.update(overrides)
    return LessonV1(**values)


def test_decision_round_trip_preserves_correct_domain_fields() -> None:
    original = decision()
    restored = DecisionRecordV1.from_json(original.to_json())

    assert restored == original
    assert restored.consumed_evidence_refs == ("evidence-1",)
    assert restored.applied_lesson_ids == ()
    assert restored.scope["database_fingerprint"] == "db-fp"


def test_contract_boundary_rejects_raw_sql_credentials_parameters_results_and_reasoning() -> None:
    with pytest.raises(ContractValidationError):
        decision(expected_result={"statement": "SELECT * FROM dbo.Users"})
    with pytest.raises(ContractValidationError):
        decision(uncertainty={"connection_string": "Server=secret"})
    with pytest.raises(ContractValidationError):
        review(observed_result={"parameter_values": [1, 2]})
    with pytest.raises(ContractValidationError):
        review(prediction_error={"chain_of_thought": "private reasoning"})
    with pytest.raises(ContractValidationError):
        decision(expected_result={"environment": "production"})


def test_subject_fields_and_evidence_prefixes_are_strict_and_summaries_are_immutable() -> None:
    record = decision(subject_kind="plan", subject_fingerprint="plan-fp")
    assert record.subject_kind == "plan"
    assert record.subject_fingerprint == "plan-fp"
    with pytest.raises(ContractValidationError):
        decision(subject_kind="candidate")
    with pytest.raises(ContractValidationError):
        lesson(subject_kind="candidate")
    with pytest.raises(ContractValidationError):
        decision(consumed_evidence_refs=("unknown-1",))
    with pytest.raises(ContractValidationError):
        review(terminal_evidence_refs=("evidence-1",))
    with pytest.raises(ContractValidationError):
        lesson(next_observation={"sql": "SELECT 1"})
    with pytest.raises(TypeError):
        record.expected_result["new"] = "value"


def test_decision_subject_and_runtime_compatibility_fields_are_required_without_workflow_ids() -> None:
    record = decision(
        case_id=None,
        session_id=None,
        candidate_id=None,
        query_fingerprint=None,
    )
    assert record.subject_fingerprint == "subject-fp-1"
    assert record.runtime_compatibility_fingerprint == "runtime-compat-fp"
    with pytest.raises(ContractValidationError):
        decision(subject_kind="")
    with pytest.raises(ContractValidationError):
        decision(subject_fingerprint=None)
    with pytest.raises(ContractValidationError):
        decision(runtime_compatibility_fingerprint="")


def test_review_correction_fields_round_trip() -> None:
    original = review()
    restored = OutcomeReviewV1.from_json(original.to_json())
    assert restored.counterexamples == original.counterexamples
    assert restored.next_observation == original.next_observation


def test_review_signals_are_strict_and_unknown_outcomes_do_not_claim_completion() -> None:
    unknown = review(
        completed_at_utc=None,
        complete=False,
        alignment="unknown",
        safety_signal="unknown",
        equivalence_signal="unknown",
        cleanup_signal="unknown",
        material_regression_signal="unknown",
        unknown_outcome=True,
    )
    assert not unknown.is_aligned_complete

    failure = review(
        alignment="contradiction",
        safety_signal="proven_failure",
        equivalence_signal="passed",
    )
    assert failure.has_safety_or_equivalence_failure
    with pytest.raises(ContractValidationError):
        review(alignment="aligned", safety_signal="proven_failure")


def test_lesson_lifecycle_requires_rejection_and_supersession_audit_fields() -> None:
    with pytest.raises(ContractValidationError):
        lesson(status="rejected")
    rejected = lesson(
        status="rejected",
        rejection_code="not-supported",
        rejected_by="maintainer",
        created_at_utc="2026-01-01T00:00:00+00:00",
        updated_at_utc="2026-01-01T00:01:00+00:00",
        rejected_at_utc="2026-01-01T00:01:00+00:00",
    )
    assert rejected.status == "rejected"
    with pytest.raises(ContractValidationError):
        lesson(status="superseded")
    superseded = lesson(
        status="superseded",
        superseded_by_lesson_id="lesson-next",
        created_at_utc="2026-01-01T00:00:00+00:00",
        updated_at_utc="2026-01-01T00:01:00+00:00",
        reviewed_at_utc="2026-01-01T00:00:00+00:00",
    )
    assert superseded.superseded_by_lesson_id == "lesson-next"


def test_handoff_is_a_workflow_object_with_reopen_and_cancel_states() -> None:
    handoff = HandoffV1(
        source_skill="sql_optimizer",
        target_skill="sql_plan_enforcer",
        objective={"kind": "review-plan"},
        evidence_refs=("evidence-1",),
        constraints={"dry_run": True},
        gaps=({"kind": "cleanup-proof"},),
        acceptance_criteria=({"kind": "equivalence"},),
    )
    assert handoff.status == "open"
    assert "lesson_ids" not in handoff.to_dict()
    with pytest.raises(ContractValidationError):
        replace(handoff, status="resolved")
