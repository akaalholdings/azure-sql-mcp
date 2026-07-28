from __future__ import annotations

import pytest

from azure_sql_mcp.candidate_lineage import combined_parent_id
from azure_sql_mcp.candidate_lineage import validate_combined_parent_request
from azure_sql_mcp.candidate_lineage import validate_combined_parent
from azure_sql_mcp.performance_contracts import EvidenceEnvelopeV1
from azure_sql_mcp.performance_contracts import TuningCandidateV1


def _parent(**overrides) -> TuningCandidateV1:
    values = {
        "candidate_id": "candidate-parent",
        "session_id": "session-one",
        "strategy": "predicate",
        "rewrite_fingerprint": "sha256:" + "a" * 64,
        "state": "improved",
        "finalist_runs": 5,
        "evidence_ids": ("evidence-parent",),
    }
    values.update(overrides)
    return TuningCandidateV1(**values)


def _child(**overrides) -> TuningCandidateV1:
    values = {
        "candidate_id": "candidate-child",
        "session_id": "session-one",
        "strategy": "combined",
        "rewrite_fingerprint": "sha256:" + "a" * 64,
        "rewrite_artifact_ref": "candidate:candidate-parent",
    }
    values.update(overrides)
    return TuningCandidateV1(**values)


def _proof(**overrides) -> EvidenceEnvelopeV1:
    values = {
        "evidence_id": "evidence-parent",
        "kind": "tuning_finalist",
        "observed_execution_count": 12,
        "metrics": {"classification": "improved"},
        "metadata": {
            "session_id": "session-one",
            "candidate_id": "candidate-parent",
            "phase": "finalist",
            "proof_scope": "direct_snapshot",
            "equivalence": [
                {
                    "status": "match",
                    "proven_for_parameter_case": True,
                    "same_snapshot": True,
                    "snapshot_isolation_verified": True,
                }
            ],
        },
    }
    values.update(overrides)
    return EvidenceEnvelopeV1(**values)


def test_combined_parent_reference_is_explicit() -> None:
    assert combined_parent_id(_child()) == "candidate-parent"

    with pytest.raises(ValueError, match="artifact_ref"):
        combined_parent_id(_child(rewrite_artifact_ref=None))


def test_proven_parent_produces_a_redacted_lineage_contract() -> None:
    lineage = validate_combined_parent(_child(), _parent(), [_proof()])

    assert lineage == {
        "lineage_contract_version": 1,
        "parent_candidate_id": "candidate-parent",
        "parent_evidence_id": "evidence-parent",
        "parent_rewrite_fingerprint": "sha256:" + "a" * 64,
        "parent_equivalence": "proven",
        "marginal_experiment": "rewrite_without_index_with_index_after_cleanup",
    }


def test_request_can_be_validated_before_child_is_persisted() -> None:
    lineage = validate_combined_parent_request(
        session_id="session-one",
        rewrite_fingerprint="sha256:" + "a" * 64,
        parent_reference="candidate:candidate-parent",
        parent=_parent(),
        evidence=[_proof()],
    )

    assert lineage["parent_candidate_id"] == "candidate-parent"
    assert lineage["parent_equivalence"] == "proven"


def test_persisted_child_keeps_its_original_parent_proof() -> None:
    original = _proof(evidence_id="evidence-original")
    newer = _proof(evidence_id="evidence-newer")
    child = _child(
        metadata={
            "lineage": {
                "parent_evidence_id": original.evidence_id,
            }
        }
    )

    lineage = validate_combined_parent(child, _parent(), [original, newer])

    assert lineage["parent_evidence_id"] == original.evidence_id


def test_invalid_pinned_parent_proof_is_not_replaced_by_a_newer_proof() -> None:
    invalid = _proof(
        evidence_id="evidence-invalid",
        metrics={"classification": "neutral"},
    )
    newer = _proof(evidence_id="evidence-newer")
    child = _child(
        metadata={
            "lineage": {
                "parent_evidence_id": invalid.evidence_id,
            }
        }
    )

    with pytest.raises(ValueError, match="no complete improved finalist"):
        validate_combined_parent(child, _parent(), [invalid, newer])


def test_parent_proof_requires_runtime_snapshot_attestation() -> None:
    proof = _proof(
        metadata={
            "session_id": "session-one",
            "candidate_id": "candidate-parent",
            "phase": "finalist",
            "proof_scope": "direct_snapshot",
            "equivalence": [
                {
                    "status": "match",
                    "proven_for_parameter_case": True,
                    "same_snapshot": True,
                }
            ],
        }
    )

    with pytest.raises(ValueError, match="no complete improved finalist"):
        validate_combined_parent(_child(), _parent(), [proof])


@pytest.mark.parametrize(
    ("parent", "proof", "message"),
    (
        (_parent(state="screening"), _proof(), "improved finalist"),
        (
            _parent(),
            _proof(metrics={"classification": "neutral"}),
            "no complete improved finalist",
        ),
        (
            _parent(),
            _proof(
                metadata={
                    "session_id": "session-one",
                    "candidate_id": "candidate-parent",
                    "phase": "finalist",
                    "proof_scope": "performance_only",
                    "equivalence": [
                        {
                            "status": "match",
                            "proven_for_parameter_case": True,
                        }
                    ],
                }
            ),
            "no complete improved finalist",
        ),
        (
            _parent(),
            _proof(
                metadata={
                    "session_id": "session-one",
                    "candidate_id": "candidate-parent",
                    "phase": "finalist",
                    "equivalence": [
                        {
                            "status": "mismatch",
                            "proven_for_parameter_case": False,
                        }
                    ],
                }
            ),
            "no complete improved finalist",
        ),
    ),
)
def test_unproven_parent_is_rejected_before_an_index_experiment(
    parent: TuningCandidateV1,
    proof: EvidenceEnvelopeV1,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        validate_combined_parent(_child(), parent, [proof])


def test_cross_session_parent_and_changed_sql_are_rejected() -> None:
    with pytest.raises(ValueError, match="another tuning session"):
        validate_combined_parent(
            _child(session_id="session-two"),
            _parent(),
            [_proof()],
        )

    with pytest.raises(ValueError, match="exactly match"):
        validate_combined_parent(
            _child(rewrite_fingerprint="sha256:" + "b" * 64),
            _parent(),
            [_proof()],
        )
