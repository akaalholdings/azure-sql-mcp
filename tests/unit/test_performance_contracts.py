from __future__ import annotations

import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from uuid import UUID

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


def test_legacy_performance_case_defaults_to_zero_state_version() -> None:
    legacy_payload = json.dumps(
        {
            "contract_type": "PerformanceCaseV1",
            "contract_version": 1,
            "case_id": "case-legacy",
            "query_fingerprint": "query-hash",
        }
    )

    case = PerformanceCaseV1.from_json(legacy_payload)

    assert case.version == 0


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


def test_evidence_normalizes_supported_database_scalars_recursively() -> None:
    evidence = EvidenceEnvelopeV1(
        evidence_id="evidence-scalars",
        query_fingerprint="query-hash",
        metrics={
            "request_id": UUID("12345678-1234-5678-1234-567812345678"),
            "captured_at": datetime(2026, 7, 28, 10, 11, 12, 345678, tzinfo=timezone.utc),
            "observed_on": date(2026, 7, 28),
            "cutoff": time(10, 11, 12, 345678),
            "amount": Decimal("123.4500"),
            "nested": {
                "values": (
                    UUID("87654321-4321-8765-4321-876543218765"),
                    Decimal("0.0001000"),
                )
            },
        },
    )

    assert evidence.metrics == {
        "request_id": "12345678-1234-5678-1234-567812345678",
        "captured_at": "2026-07-28T10:11:12.345678+00:00",
        "observed_on": "2026-07-28",
        "cutoff": "10:11:12.345678",
        "amount": "123.4500",
        "nested": {
            "values": [
                "87654321-4321-8765-4321-876543218765",
                "0.0001000",
            ]
        },
    }


def test_evidence_rejects_unknown_metadata_objects() -> None:
    class UnknownScalar:
        pass

    with pytest.raises(ContractValidationError, match="Unsupported value type UnknownScalar"):
        EvidenceEnvelopeV1(
            evidence_id="evidence-unknown-scalar",
            query_fingerprint="query-hash",
            metadata={"unknown": UnknownScalar()},
        )


def test_invalid_contract_version_and_terminal_state_are_rejected() -> None:
    with pytest.raises(ContractValidationError):
        EvidenceEnvelopeV1(evidence_id="evidence-3", contract_version=2)

    with pytest.raises(ContractValidationError):
        TuningCandidateV1(
            candidate_id="candidate-3",
            session_id="session-3",
            state="failed",
        )


def test_performance_only_candidate_round_trips_as_terminal() -> None:
    candidate = TuningCandidateV1(
        candidate_id="candidate-performance-only",
        session_id="session-performance-only",
        state="performance_only",
        finalist_runs=5,
        executions=10,
    )

    decoded = deserialize_contract(candidate.to_json())

    assert decoded == candidate
    assert candidate.is_terminal is True


def test_candidate_artifact_reference_cannot_be_raw_sql() -> None:
    with pytest.raises(ContractValidationError, match="artifact, not contain raw SQL"):
        TuningCandidateV1(
            candidate_id="candidate-4",
            session_id="session-4",
            rewrite_artifact_ref="SELECT synthetic_value FROM dbo.SyntheticTable",
        )
