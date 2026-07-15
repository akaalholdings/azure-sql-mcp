from __future__ import annotations

import os
import sqlite3
from dataclasses import replace

import pytest

from azure_sql_mcp.performance_contracts import EvidenceEnvelopeV1, PerformanceCaseV1
from azure_sql_mcp.performance_store import (
    ConcurrencyError,
    PerformanceStore,
)


def test_store_persists_redacted_contracts_and_secure_file_modes(tmp_path) -> None:
    state_dir = tmp_path / "state"
    evidence = EvidenceEnvelopeV1(
        evidence_id="evidence-1",
        query_fingerprint="query-hash",
        metadata={"raw_sql": "SELECT secret_value", "source": "mcp"},
    )
    with PerformanceStore(state_dir) as store:
        store.create_evidence(evidence)

    assert os.stat(state_dir).st_mode & 0o777 == 0o700
    database_path = state_dir / "performance.sqlite3"
    assert os.stat(database_path).st_mode & 0o777 == 0o600

    with sqlite3.connect(database_path) as connection:
        payload = connection.execute(
            "SELECT payload FROM evidence_envelopes WHERE evidence_id = ?",
            (evidence.evidence_id,),
        ).fetchone()[0]
    assert "SELECT secret_value" not in payload

    with PerformanceStore(state_dir) as reopened:
        assert reopened.get_evidence(evidence.evidence_id) == evidence


def test_create_and_event_operations_are_idempotent(tmp_path) -> None:
    case = PerformanceCaseV1(case_id="case-1", query_fingerprint="query-hash")
    with PerformanceStore(tmp_path / "state") as store:
        first = store.create_performance_case(case, idempotency_key="case-create")
        replay = store.create_performance_case(
            PerformanceCaseV1(case_id="case-retry", query_fingerprint="different"),
            idempotency_key="case-create",
        )
        first_event = store.append_event(
            event_type="test.observed",
            aggregate_type="performance_case",
            aggregate_id=case.case_id,
            payload={"raw_sql": "SELECT private_value", "count": 1},
            idempotency_key="event-1",
        )
        replay_event = store.append_event(
            event_type="test.observed",
            aggregate_type="performance_case",
            aggregate_id=case.case_id,
            payload={"raw_sql": "SELECT another_private_value", "count": 99},
            idempotency_key="event-1",
        )
        events = store.list_events(
            aggregate_type="performance_case", aggregate_id=case.case_id
        )

    assert first == case
    assert replay == case
    assert replay_event == first_event
    assert len(events) == 2
    assert events[-1]["payload"] == {"count": 1}


def test_optimistic_update_rejects_stale_session_version(tmp_path) -> None:
    from azure_sql_mcp.performance_contracts import TuningSessionV1

    session = TuningSessionV1(session_id="session-1", performance_case_id="case-1")
    with PerformanceStore(tmp_path / "state") as store:
        store.create_session(session)
        updated = replace(session, version=1)
        store.save_session(updated, expected_version=0)
        with pytest.raises(ConcurrencyError):
            store.save_session(updated, expected_version=0)


def test_index_leases_are_durable_and_cleanup_state_is_queryable() -> None:
    store = PerformanceStore(db_path=":memory:")
    lease = store.create_index_lease(
        lease_id="lease-1",
        database_fingerprint="database-fingerprint",
        session_id="session-1",
        candidate_id="candidate-1",
        index_name="IX_Testing_synthetic",
        object_fingerprint="object-fingerprint",
        expires_at_utc="2026-07-15T12:00:00+00:00",
        metadata={"sql": "SELECT private_value FROM dbo.Items"},
    )

    assert lease["status"] == "pending_create"
    assert "sql" not in lease["metadata"]
    assert store.list_open_index_leases() == [lease]

    cleaned = store.update_index_lease("lease-1", status="cleaned")
    assert cleaned["status"] == "cleaned"
    assert store.list_open_index_leases() == []
    store.close()
