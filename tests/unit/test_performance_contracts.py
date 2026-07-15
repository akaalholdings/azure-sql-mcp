from __future__ import annotations

import json

import pytest

from azure_sql_mcp.performance_contracts import (
    ContractValidationError,
    EvidenceEnvelopeV1,
    PlanActionIntentV1,
    PerformanceCaseV1,
    TuningCandidateV1,
    TuningSessionV1,
    deserialize_contract,
)


def test_contracts_round_trip_as_explicit_versioned_json() -> None:
    contracts = [
        EvidenceEnvelopeV1(
            evidence_id="evidence-1",
            query_fingerprint="query-hash",
            metrics={"avg_duration_ms": 12.5},
        ),
        PerformanceCaseV1(case_id="case-1", query_fingerprint="query-hash"),
        TuningSessionV1(session_id="session-1", performance_case_id="case-1"),
        TuningCandidateV1(candidate_id="candidate-1", session_id="session-1"),
        PlanActionIntentV1(
            intent_id="intent-1",
            session_id="session-1",
            query_fingerprint="query-hash",
        ),
    ]

    for contract in contracts:
        encoded = contract.to_json()
        decoded = deserialize_contract(encoded)
        assert decoded == contract
        assert json.loads(encoded)["contract_version"] == 1
        assert json.loads(encoded)["contract_type"] == contract.contract_type


def test_contract_metadata_drops_raw_sql_and_secret_like_fields() -> None:
    evidence = EvidenceEnvelopeV1(
        evidence_id="evidence-2",
        query_fingerprint="query-hash",
        metadata={
            "raw_sql": "SELECT private_value FROM dbo.PrivateTable",
            "password": "do-not-store",
            "review_note": "safe aggregate observation",
        },
    )

    payload = evidence.to_json()

    assert "SELECT private_value" not in payload
    assert "do-not-store" not in payload
    assert "safe aggregate observation" in payload


def test_invalid_contract_version_and_terminal_state_are_rejected() -> None:
    with pytest.raises(ContractValidationError):
        EvidenceEnvelopeV1(evidence_id="evidence-3", contract_version=2)

    with pytest.raises(ContractValidationError):
        TuningCandidateV1(
            candidate_id="candidate-3",
            session_id="session-3",
            state="failed",
        )


def test_candidate_artifact_reference_cannot_be_raw_sql() -> None:
    with pytest.raises(ContractValidationError, match="artifact, not contain raw SQL"):
        TuningCandidateV1(
            candidate_id="candidate-4",
            session_id="session-4",
            rewrite_artifact_ref="SELECT synthetic_value FROM dbo.SyntheticTable",
        )
