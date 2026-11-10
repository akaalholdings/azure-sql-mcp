from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from .performance_contracts import EvidenceEnvelopeV1
from .performance_contracts import TuningCandidateV1


COMBINED_PARENT_REFERENCE_PREFIX = "candidate:"
LINEAGE_BACKED_STRATEGIES = frozenset({"combined", "rewrite_plus_index"})
# A rewrite that needs its index is often neutral or slower alone. It is a
# valid parent of an index experiment when its results matched the original.
NOT_FASTER_ALONE_PARENT_STATES = frozenset({"neutral", "regressed"})
ONE_CALL_ROUTE = (
    "Route: register the rewrite with add_tuning_candidate(strategy="
    "'rewrite_plus_index') and no artifact_ref, then call benchmark_index_candidate "
    "with sql = the statement of the performance case and candidate_sql = the "
    "rewrite. That call measures the original against the rewrite with its "
    "indexes and compares the results itself."
)


def parse_combined_parent_reference(reference: str | None) -> str:
    """Parse the only supported combined-parent artifact reference."""

    reference = (reference or "").strip()
    if not reference.startswith(COMBINED_PARENT_REFERENCE_PREFIX):
        raise ValueError("artifact_ref must start with candidate:")
    parent_id = reference.removeprefix(COMBINED_PARENT_REFERENCE_PREFIX).strip()
    if not parent_id or ":" in parent_id:
        raise ValueError(
            "A combined candidate parent reference must contain exactly one candidate id."
        )
    return parent_id


def combined_parent_id(candidate: TuningCandidateV1) -> str:
    """Return a lineage parent's id for a legacy or current child candidate."""

    if candidate.strategy not in LINEAGE_BACKED_STRATEGIES:
        raise ValueError(
            "Only a lineage-backed combined or rewrite_plus_index candidate may "
            "reference a rewrite parent."
        )
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
    """Validate legacy combined lineage before a child is persisted.

    ``combined`` remains the compatibility name for old index children, but a
    new multi-family rewrite may also use it without an artifact reference.
    New index children should call :func:`validate_rewrite_plus_index_parent`.
    """

    return _validate_parent_request(
        session_id=session_id,
        rewrite_fingerprint=rewrite_fingerprint,
        parent_reference=parent_reference,
        parent=parent,
        evidence=evidence,
        pinned_evidence_id=pinned_evidence_id,
        allow_performance_only=False,
    )


def validate_rewrite_plus_index_parent_request(
    *,
    session_id: str,
    rewrite_fingerprint: str,
    parent_reference: str | None,
    parent: TuningCandidateV1,
    evidence: Sequence[EvidenceEnvelopeV1],
    pinned_evidence_id: str | None = None,
) -> dict[str, Any]:
    """Validate a new index child of a measured rewrite.

    A performance-only parent is a valid starting point for an index
    experiment, but the child inherits that uncertainty and can never turn it
    into proven equivalence through index timings.

    A parent whose finalist was neutral or regressed is valid too, when its
    results matched the original for every parameter case: a rewrite that
    needs its index is not faster alone. A refusal names the parent state,
    the reason and the route that needs no parent.
    """

    return _validate_parent_request(
        session_id=session_id,
        rewrite_fingerprint=rewrite_fingerprint,
        parent_reference=parent_reference,
        parent=parent,
        evidence=evidence,
        pinned_evidence_id=pinned_evidence_id,
        allow_performance_only=True,
    )


def _validate_parent_request(
    *,
    session_id: str,
    rewrite_fingerprint: str,
    parent_reference: str | None,
    parent: TuningCandidateV1,
    evidence: Sequence[EvidenceEnvelopeV1],
    pinned_evidence_id: str | None,
    allow_performance_only: bool,
) -> dict[str, Any]:
    """Validate shared parent identity and select one pinned finalist proof."""

    expected_parent_id = parse_combined_parent_reference(parent_reference)
    if parent.candidate_id != expected_parent_id:
        raise ValueError("Lineage child parent reference does not match the parent.")
    if session_id != parent.session_id:
        raise ValueError("Lineage child parent belongs to another tuning session.")
    if not rewrite_fingerprint or rewrite_fingerprint != parent.rewrite_fingerprint:
        raise ValueError(
            "Lineage child SQL must exactly match its rewrite parent."
        )
    # ``allow_performance_only`` marks the rewrite_plus_index lineage; the
    # legacy combined lineage keeps its improved-only rule.
    route = f" {ONE_CALL_ROUTE}" if allow_performance_only else ""
    not_faster_alone = allow_performance_only and parent.state in NOT_FASTER_ALONE_PARENT_STATES
    allowed_parent_states = (
        {"improved", "performance_only"} | NOT_FASTER_ALONE_PARENT_STATES
        if allow_performance_only
        else {"improved"}
    )
    if parent.state not in allowed_parent_states or parent.finalist_runs <= 0:
        raise ValueError(
            "Lineage child parent must be a completed finalist with measured runs; "
            f"the parent is in state '{parent.state}' with {parent.finalist_runs} "
            f"finalist run(s).{route}"
        )

    candidates = [
        item
        for item in reversed(tuple(evidence))
        if pinned_evidence_id is None or item.evidence_id == pinned_evidence_id
    ]
    proof = next(
        (
            item
            for item in candidates
            if (
                _is_matched_finalist_evidence(item, parent)
                and (not_faster_alone or item.metrics.get("classification") == "improved")
            )
            or (
                allow_performance_only
                and not not_faster_alone
                and _is_performance_only_finalist_evidence(item, parent)
            )
        ),
        None,
    )
    if proof is None:
        if not not_faster_alone:
            raise ValueError(
                "Lineage child parent has no complete improved finalist evidence; "
                f"the parent is in state '{parent.state}'.{route}"
            )
        compared = any(
            isinstance(item.metadata.get("equivalence"), (list, tuple))
            and item.metadata.get("equivalence")
            for item in candidates
        )
        raise ValueError(
            f"The parent finalist is '{parent.state}' and its results "
            + (
                "did not match the original for every parameter case"
                if compared
                else "were not compared with the original"
            )
            + f", so an index measured on it would prove nothing about the rewrite.{route}"
        )

    parent_equivalence = (
        "proven"
        if _is_matched_finalist_evidence(proof, parent)
        else "unproven"
    )

    lineage = {
        "lineage_contract_version": 1,
        "parent_candidate_id": parent.candidate_id,
        "parent_evidence_id": proof.evidence_id,
        "parent_rewrite_fingerprint": parent.rewrite_fingerprint,
        "parent_equivalence": parent_equivalence,
        "marginal_experiment": "rewrite_without_index_with_index_after_cleanup",
    }
    if not_faster_alone:
        # Added only for these parents, so stored lineage of an improved or
        # performance-only parent stays equal to what is recomputed here.
        lineage["parent_state"] = parent.state
    return lineage


def validate_combined_parent(
    child: TuningCandidateV1,
    parent: TuningCandidateV1,
    evidence: Sequence[EvidenceEnvelopeV1],
) -> dict[str, Any]:
    """Read legacy combined or current rewrite-plus-index lineage.

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
    validator = (
        validate_rewrite_plus_index_parent_request
        if child.strategy == "rewrite_plus_index"
        else validate_combined_parent_request
    )
    return validator(
        session_id=child.session_id,
        rewrite_fingerprint=child.rewrite_fingerprint or "",
        parent_reference=child.rewrite_artifact_ref,
        parent=parent,
        evidence=evidence,
        pinned_evidence_id=pinned_evidence_id or None,
    )


def validate_rewrite_plus_index_parent(
    child: TuningCandidateV1,
    parent: TuningCandidateV1,
    evidence: Sequence[EvidenceEnvelopeV1],
) -> dict[str, Any]:
    """Validate the explicit rewrite-plus-index lineage type."""

    if child.strategy != "rewrite_plus_index":
        raise ValueError("Only a rewrite_plus_index candidate may use this validator.")
    return validate_combined_parent(child, parent, evidence)


def _is_matched_finalist_evidence(
    evidence: EvidenceEnvelopeV1,
    parent: TuningCandidateV1,
) -> bool:
    """Finalist evidence whose results matched the original for every case.

    Equivalence only: how the timings were classified is a separate question.
    """

    metadata = evidence.metadata
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
        and all(
            isinstance(comparison, dict)
            and comparison.get("status") == "match"
            and comparison.get("proven_for_parameter_case") is True
            and comparison.get("same_snapshot") is True
            and comparison.get("snapshot_isolation_verified") is True
            for comparison in comparisons
        )
    )


def _is_performance_only_finalist_evidence(
    evidence: EvidenceEnvelopeV1,
    parent: TuningCandidateV1,
) -> bool:
    metadata = evidence.metadata
    metrics = evidence.metrics
    return (
        evidence.kind == "tuning_finalist"
        and evidence.observed_execution_count > 0
        and metadata.get("session_id") == parent.session_id
        and metadata.get("candidate_id") == parent.candidate_id
        and metadata.get("phase") == "finalist"
        and metadata.get("proof_scope") == "performance_only"
        and metrics.get("classification") == "performance_only"
        and metrics.get("performance_classification") == "improved"
        and metadata.get("equivalence_deferred") is True
    )
