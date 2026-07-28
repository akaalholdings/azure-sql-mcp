from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from .performance_contracts import EvidenceEnvelopeV1
from .performance_contracts import TuningCandidateV1


COMBINED_PARENT_REFERENCE_PREFIX = "candidate:"


def parse_combined_parent_reference(reference: str | None) -> str:
    """Parse the only supported combined-parent artifact reference."""

    reference = (reference or "").strip()
    if not reference.startswith(COMBINED_PARENT_REFERENCE_PREFIX):
        raise ValueError(
            "A combined candidate requires artifact_ref='candidate:<proven-parent-id>'."
        )
    parent_id = reference.removeprefix(COMBINED_PARENT_REFERENCE_PREFIX).strip()
    if not parent_id or ":" in parent_id:
        raise ValueError(
            "A combined candidate parent reference must contain exactly one candidate id."
        )
    return parent_id


def combined_parent_id(candidate: TuningCandidateV1) -> str:
    """Return the durable parent id from a combined candidate artifact reference."""

    if candidate.strategy != "combined":
        raise ValueError("Only a combined candidate may reference a rewrite parent.")
    return parse_combined_parent_reference(candidate.rewrite_artifact_ref)


def validate_combined_parent_request(
    *,
    session_id: str,
    rewrite_fingerprint: str,
    parent_reference: str | None,
    parent: TuningCandidateV1,
    evidence: Sequence[EvidenceEnvelopeV1],
    pinned_evidence_id: str | None = None,
) -> dict[str, Any]:
    """Validate lineage before a combined candidate is persisted."""

    expected_parent_id = parse_combined_parent_reference(parent_reference)
    if parent.candidate_id != expected_parent_id:
        raise ValueError("Combined candidate parent reference does not match the parent.")
    if session_id != parent.session_id:
        raise ValueError("Combined candidate parent belongs to another tuning session.")
    if not rewrite_fingerprint or rewrite_fingerprint != parent.rewrite_fingerprint:
        raise ValueError(
            "Combined candidate SQL must exactly match its proven rewrite parent."
        )
    if parent.state != "improved" or parent.finalist_runs <= 0:
        raise ValueError(
            "Combined candidate parent must be an improved finalist with measured runs."
        )

    proof = next(
        (
            item
            for item in reversed(tuple(evidence))
            if (
                pinned_evidence_id is None
                or item.evidence_id == pinned_evidence_id
            )
            and _is_proven_finalist_evidence(item, parent)
        ),
        None,
    )
    if proof is None:
        raise ValueError(
            "Combined candidate parent has no complete improved finalist "
            "equivalence evidence."
        )

    return {
        "lineage_contract_version": 1,
        "parent_candidate_id": parent.candidate_id,
        "parent_evidence_id": proof.evidence_id,
        "parent_rewrite_fingerprint": parent.rewrite_fingerprint,
        "parent_equivalence": "proven",
        "marginal_experiment": "rewrite_without_index_with_index_after_cleanup",
    }


def validate_combined_parent(
    child: TuningCandidateV1,
    parent: TuningCandidateV1,
    evidence: Sequence[EvidenceEnvelopeV1],
) -> dict[str, Any]:
    """Prove that an index experiment extends one equivalent rewrite.

    The combined experiment measures only the index's marginal A-B-A effect.
    Original-versus-rewrite equivalence remains anchored to the parent's finalist
    evidence and is never inferred from timings.
    """

    expected_parent_id = combined_parent_id(child)
    if parent.candidate_id != expected_parent_id:
        raise ValueError("Combined candidate parent reference does not match the parent.")
    if child.candidate_id == parent.candidate_id:
        raise ValueError("A combined candidate cannot be its own parent.")
    lineage = child.metadata.get("lineage")
    pinned_evidence_id = (
        str(lineage.get("parent_evidence_id") or "")
        if isinstance(lineage, dict)
        else ""
    )
    return validate_combined_parent_request(
        session_id=child.session_id,
        rewrite_fingerprint=child.rewrite_fingerprint or "",
        parent_reference=child.rewrite_artifact_ref,
        parent=parent,
        evidence=evidence,
        pinned_evidence_id=pinned_evidence_id or None,
    )


def _is_proven_finalist_evidence(
    evidence: EvidenceEnvelopeV1,
    parent: TuningCandidateV1,
) -> bool:
    metadata = evidence.metadata
    metrics = evidence.metrics
    comparisons = metadata.get("equivalence")
    if not isinstance(comparisons, (list, tuple)) or not comparisons:
        return False
    return (
        evidence.kind == "tuning_finalist"
        and evidence.observed_execution_count > 0
        and metadata.get("session_id") == parent.session_id
        and metadata.get("candidate_id") == parent.candidate_id
        and metadata.get("phase") == "finalist"
        and metadata.get("proof_scope") == "direct_snapshot"
        and metrics.get("classification") == "improved"
        and all(
            isinstance(comparison, dict)
            and comparison.get("status") == "match"
            and comparison.get("proven_for_parameter_case") is True
            and comparison.get("same_snapshot") is True
            and comparison.get("snapshot_isolation_verified") is True
            for comparison in comparisons
        )
    )
