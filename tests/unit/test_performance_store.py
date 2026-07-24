from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from azure_sql_mcp.performance_contracts import EvidenceEnvelopeV1, PerformanceCaseV1
from azure_sql_mcp.performance_contracts import TuningCandidateV1, TuningSessionV1
from azure_sql_mcp.performance_store import (
    ConcurrencyError,
    IdempotencyConflictError,
    LeaseConflictError,
    LeaseFencingError,
    PerformanceStore,
    PerformanceStoreError,
    ReservationError,
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


def test_idempotent_create_binds_key_to_request_fingerprint() -> None:
    store = PerformanceStore(db_path=":memory:")
    case = PerformanceCaseV1(case_id="case-1", query_fingerprint="query-hash")
    first = store.create_performance_case(
        case,
        idempotency_key="case-create",
        request_fingerprint="request-a",
    )
    replay = store.create_performance_case(
        PerformanceCaseV1(case_id="case-retry", query_fingerprint="query-hash"),
        idempotency_key="case-create",
        request_fingerprint="request-a",
    )

    assert replay == first
    with pytest.raises(IdempotencyConflictError, match="different request"):
        store.create_performance_case(
            PerformanceCaseV1(case_id="case-other", query_fingerprint="other"),
            idempotency_key="case-create",
            request_fingerprint="request-b",
        )
    store.close()


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


def test_reservation_and_lease_payloads_redact_authority_but_updates_are_fenced() -> None:
    store = PerformanceStore(db_path=":memory:")
    case = store.create_performance_case(
        PerformanceCaseV1(case_id="case-redaction", query_fingerprint="query-hash")
    )
    session = store.create_session(
        TuningSessionV1(
            session_id="session-redaction",
            performance_case_id=case.case_id,
            max_candidates=1,
            execution_limit=2,
        )
    )
    slot = store.reserve_candidate_slot(
        session.session_id,
        "candidate-redaction",
        owner_reference="candidate-owner",
    )
    assert "owner_token" not in slot
    candidate = store.create_candidate_and_attach(
        session,
        TuningCandidateV1(
            candidate_id="candidate-redaction",
            session_id=session.session_id,
        ),
        reservation_id=slot["reservation_id"],
        owner_reference="candidate-owner",
    )
    reservation = store.reserve_execution_attempts(
        session.session_id,
        candidate.candidate_id,
        2,
        "execution-redaction",
        owner_reference="execution-owner",
    )
    assert "owner_token" not in reservation
    with pytest.raises(LeaseFencingError):
        store.complete_execution_attempts(
            reservation["reservation_id"],
            owner_reference="wrong-owner",
            expected_version=reservation["version"],
        )
    completed = store.complete_execution_attempts(
        reservation["reservation_id"],
        owner_reference="execution-owner",
        expected_version=reservation["version"],
    )
    assert completed["status"] == "completed"

    lease = store.create_index_lease(
        lease_id="lease-redaction",
        database_fingerprint="database-fingerprint",
        session_id=session.session_id,
        candidate_id=candidate.candidate_id,
        index_name="IX_Redaction",
        object_fingerprint="object-fingerprint",
        expires_at_utc="2026-07-15T12:00:00+00:00",
        owner_reference="lease-owner",
    )
    assert "owner_token" not in lease
    assert "fencing_token" not in lease
    updated = store.update_index_lease(
        lease["lease_id"],
        status="active",
        owner_reference="lease-owner",
        expected_version=lease["version"],
    )
    assert "owner_token" not in updated
    assert "fencing_token" not in updated
    assert "owner_token" not in store.get_index_lease(lease["lease_id"])
    assert "fencing_token" not in store.get_index_lease(lease["lease_id"])
    recovered = store.recover_index_lease(
        lease["lease_id"],
        status="cleanup_pending",
        expected_version=updated["version"],
    )
    assert recovered["status"] == "cleanup_pending"
    assert "owner_token" not in recovered
    assert "fencing_token" not in recovered
    store.close()


def test_concurrent_candidate_and_execution_reservations_are_bounded(tmp_path) -> None:
    database_path = tmp_path / "shared" / "performance.sqlite3"
    first = PerformanceStore(db_path=database_path)
    case = first.create_performance_case(
        PerformanceCaseV1(case_id="case-1", query_fingerprint="query-hash")
    )
    session = first.create_session(
        TuningSessionV1(
            session_id="session-1",
            performance_case_id=case.case_id,
            max_candidates=1,
            execution_limit=3,
        )
    )
    second = PerformanceStore(db_path=database_path)

    def reserve_candidate(store, request):
        try:
            owner = f"candidate-owner-{request}"
            return store.reserve_candidate_slot(
                session.session_id,
                request,
                owner_reference=owner,
            ), owner
        except ReservationError:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        candidate_results = list(
            executor.map(
                lambda args: reserve_candidate(*args),
                ((first, "candidate-r1"), (second, "candidate-r2")),
            )
        )
    assert sum(result is not None for result in candidate_results) == 1
    winning_slot, winning_owner = next(
        result for result in candidate_results if result is not None
    )
    candidate = first.create_candidate_and_attach(
        session,
        TuningCandidateV1(candidate_id="candidate-1", session_id=session.session_id),
        reservation_id=winning_slot["reservation_id"],
        owner_reference=winning_owner,
    )

    def reserve_execution(store, request):
        try:
            return store.reserve_execution_attempts(
                session.session_id,
                candidate.candidate_id,
                2,
                request,
                owner_reference="execution-owner",
            )
        except ReservationError:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        execution_results = list(
            executor.map(
                lambda args: reserve_execution(*args),
                ((first, "execution-r1"), (second, "execution-r2")),
            )
        )
    assert sum(result is not None for result in execution_results) == 1

    winning = next(result for result in execution_results if result is not None)
    replay_store = first if execution_results[0] is not None else second
    replay = replay_store.reserve_execution_attempts(
        session.session_id,
        candidate.candidate_id,
        2,
        winning["request_fingerprint"],
        owner_reference="execution-owner",
    )
    assert replay["reservation_id"] == next(
        result["reservation_id"] for result in execution_results if result is not None
    )
    first.close()
    second.close()


def test_execution_idempotency_key_is_hashed_and_binds_full_request() -> None:
    store = PerformanceStore(db_path=":memory:")
    case = store.create_performance_case(
        PerformanceCaseV1(case_id="case-idempotency", query_fingerprint="query-hash")
    )
    session = store.create_session(
        TuningSessionV1(
            session_id="session-idempotency",
            performance_case_id=case.case_id,
            execution_limit=5,
        )
    )
    candidate = store.create_candidate_and_attach(
        session,
        TuningCandidateV1(
            candidate_id="candidate-idempotency",
            session_id=session.session_id,
        ),
    )
    reservation = store.reserve_execution_attempts(
        session.session_id,
        candidate.candidate_id,
        2,
        "request-full-v1",
        owner_reference="execution-owner",
        idempotency_key="caller-secret-key",
    )
    replay = store.reserve_execution_attempts(
        session.session_id,
        candidate.candidate_id,
        2,
        "request-full-v1",
        owner_reference="execution-owner",
        idempotency_key="caller-secret-key",
    )

    assert replay["reservation_id"] == reservation["reservation_id"]
    binding = store._connection.execute(
        """
        SELECT aggregate_type, aggregate_id, idempotency_key, request_fingerprint
        FROM operation_idempotency
        WHERE scope = 'execution.reserve'
        """
    ).fetchone()
    assert binding is not None
    assert binding["aggregate_type"] == "candidate"
    assert binding["aggregate_id"] == candidate.candidate_id
    assert binding["request_fingerprint"] == "request-full-v1"
    assert "caller-secret-key" not in binding["idempotency_key"]

    with pytest.raises(IdempotencyConflictError, match="different.*idempotency key"):
        store.reserve_execution_attempts(
            session.session_id,
            candidate.candidate_id,
            2,
            "request-full-v1",
            owner_reference="execution-owner",
            idempotency_key="another-caller-key",
        )

    with pytest.raises(IdempotencyConflictError, match="different request"):
        store.reserve_execution_attempts(
            session.session_id,
            candidate.candidate_id,
            3,
            "request-changed-runs",
            owner_reference="execution-owner",
            idempotency_key="caller-secret-key",
        )
    store.close()


def test_index_benchmark_request_binding_requires_exact_key_and_shape() -> None:
    store = PerformanceStore(db_path=":memory:")
    case = store.create_performance_case(
        PerformanceCaseV1(case_id="case-index-binding", query_fingerprint="query-hash")
    )
    session = store.create_session(
        TuningSessionV1(
            session_id="session-index-binding",
            performance_case_id=case.case_id,
        )
    )
    candidate = store.create_candidate_and_attach(
        session,
        TuningCandidateV1(
            candidate_id="candidate-index-binding",
            session_id=session.session_id,
        ),
    )

    first = store.bind_index_benchmark_request(
        session.session_id,
        candidate.candidate_id,
        "screening",
        "request-shape-v1",
        idempotency_key="caller-key",
    )
    replay = store.bind_index_benchmark_request(
        session.session_id,
        candidate.candidate_id,
        "screening",
        "request-shape-v1",
        idempotency_key="caller-key",
    )

    assert first == {"replayed": False}
    assert replay == {"replayed": True}
    binding = store._connection.execute(
        """
        SELECT idempotency_key, request_fingerprint
        FROM operation_idempotency
        WHERE scope = 'index-benchmark.request.screening'
        """
    ).fetchone()
    assert binding is not None
    assert binding["request_fingerprint"] == "request-shape-v1"
    assert "caller-key" not in binding["idempotency_key"]

    with pytest.raises(IdempotencyConflictError, match="different request"):
        store.bind_index_benchmark_request(
            session.session_id,
            candidate.candidate_id,
            "screening",
            "request-shape-v2",
            idempotency_key="caller-key",
        )
    with pytest.raises(IdempotencyConflictError, match="different idempotency key"):
        store.bind_index_benchmark_request(
            session.session_id,
            candidate.candidate_id,
            "screening",
            "request-shape-v1",
            idempotency_key="another-key",
        )

    finalist = store.bind_index_benchmark_request(
        session.session_id,
        candidate.candidate_id,
        "finalist",
        "request-finalist-v1",
        idempotency_key="finalist-key",
    )
    assert finalist == {"replayed": False}
    store.close()


def test_candidate_ordinals_skip_all_historical_slots() -> None:
    store = PerformanceStore(db_path=":memory:")
    case = store.create_performance_case(
        PerformanceCaseV1(case_id="case-ordinal", query_fingerprint="query-hash")
    )
    session = store.create_session(
        TuningSessionV1(
            session_id="session-ordinal",
            performance_case_id=case.case_id,
            max_candidates=3,
        )
    )
    first_slot = store.reserve_candidate_slot(
        session.session_id,
        "candidate-slot-one",
        owner_reference="candidate-owner-one",
    )
    store.release_candidate_slot(
        first_slot["reservation_id"],
        owner_reference="candidate-owner-one",
        expected_version=first_slot["version"],
    )
    second_slot = store.reserve_candidate_slot(
        session.session_id,
        "candidate-slot-two",
        owner_reference="candidate-owner-two",
    )
    assert first_slot["ordinal"] == 0
    assert second_slot["ordinal"] == 1

    first_candidate = store.create_candidate_and_attach(
        session,
        TuningCandidateV1(
            candidate_id="candidate-ordinal-one",
            session_id=session.session_id,
        ),
        reservation_id=second_slot["reservation_id"],
        owner_reference="candidate-owner-two",
    )
    second_candidate = store.create_candidate_and_attach(
        session,
        TuningCandidateV1(
            candidate_id="candidate-ordinal-two",
            session_id=session.session_id,
        ),
    )
    assert first_candidate.ordinal == 1
    assert second_candidate.ordinal == 2
    store.close()


def test_concurrent_case_evidence_appends_are_not_lost(tmp_path) -> None:
    database_path = tmp_path / "shared" / "performance.sqlite3"
    first = PerformanceStore(db_path=database_path)
    case = first.create_performance_case(
        PerformanceCaseV1(case_id="case-evidence-append", query_fingerprint="query-hash")
    )
    second = PerformanceStore(db_path=database_path)

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(
            executor.map(
                lambda args: args[0].append_performance_case_evidence(
                    case.case_id,
                    args[1],
                ),
                ((first, "evidence-a"), (second, "evidence-b")),
            )
        )

    evidence_ids = set(first.get_performance_case(case.case_id).baseline_evidence_ids)
    assert evidence_ids == {"evidence-a", "evidence-b"}
    assert first.get_performance_case(case.case_id).version == 2
    first.close()
    second.close()


def test_case_evidence_attachment_fences_stale_case_writer() -> None:
    store = PerformanceStore(db_path=":memory:")
    case = store.create_performance_case(
        PerformanceCaseV1(case_id="case-evidence-version", query_fingerprint="query-hash")
    )

    attached = store.append_performance_case_evidence(case.case_id, "evidence-a")

    assert attached.version == 1
    with pytest.raises(ConcurrencyError):
        store.save_performance_case(
            replace(
                case,
                status="ready",
                version=1,
            ),
            expected_version=case.version,
        )
    assert store.get_performance_case(case.case_id).baseline_evidence_ids == (
        "evidence-a",
    )
    store.close()


def test_execution_reservation_horizon_uses_maximum_request_runtime() -> None:
    store = PerformanceStore(db_path=":memory:")
    case = store.create_performance_case(
        PerformanceCaseV1(case_id="case-runtime", query_fingerprint="query-hash")
    )
    session = store.create_session(
        TuningSessionV1(
            session_id="session-runtime",
            performance_case_id=case.case_id,
            execution_limit=2,
        )
    )
    candidate = store.create_candidate_and_attach(
        session,
        TuningCandidateV1(
            candidate_id="candidate-runtime",
            session_id=session.session_id,
        ),
    )

    reservation = store.reserve_execution_attempts(
        session.session_id,
        candidate.candidate_id,
        2,
        "execution-runtime",
        owner_reference="runtime-owner",
        max_runtime_seconds=2 * 60 * 60,
    )
    created = datetime.fromisoformat(reservation["created_at_utc"])
    expires = datetime.fromisoformat(reservation["expires_at_utc"])

    assert expires - created >= timedelta(hours=2, seconds=60)
    assert created.tzinfo == timezone.utc
    store.close()


def test_partial_execution_completion_charges_dispatched_count_and_fences_replay() -> None:
    store = PerformanceStore(db_path=":memory:")
    case = store.create_performance_case(
        PerformanceCaseV1(case_id="case-partial", query_fingerprint="query-hash")
    )
    session = store.create_session(
        TuningSessionV1(
            session_id="session-partial",
            performance_case_id=case.case_id,
            execution_limit=5,
        )
    )
    candidate = store.create_candidate_and_attach(
        session,
        TuningCandidateV1(
            candidate_id="candidate-partial",
            session_id=session.session_id,
        ),
    )
    reservation = store.reserve_execution_attempts(
        session.session_id,
        candidate.candidate_id,
        4,
        "execution-partial",
        owner_reference="owner-partial",
    )
    persisted_candidate = replace(candidate, executions=2, version=1)
    store.save_candidate(persisted_candidate, expected_version=candidate.version)

    completed = store.complete_execution_attempts(
        reservation["reservation_id"],
        owner_reference="owner-partial",
        expected_version=reservation["version"],
    )

    assert completed["status"] == "completed"
    assert completed["attempt_count"] == 4
    assert completed["dispatched_attempt_count"] == 2
    assert store.execution_budget_usage(session.session_id) == {
        "execution_limit": 5,
        "consumed": 2,
        "reserved": 0,
        "committed": 2,
        "remaining": 3,
    }
    replay = store.reserve_execution_attempts(
        session.session_id,
        candidate.candidate_id,
        4,
        "execution-partial",
        owner_reference="owner-partial",
    )
    assert replay["replayed"] is True
    assert replay["status"] == "completed"
    assert replay["dispatched_attempt_count"] == 2

    second_candidate = store.create_candidate_and_attach(
        session,
        TuningCandidateV1(
            candidate_id="candidate-released-partial",
            session_id=session.session_id,
        ),
    )
    second_reservation = store.reserve_execution_attempts(
        session.session_id,
        second_candidate.candidate_id,
        2,
        "execution-released-partial",
        owner_reference="owner-released-partial",
    )
    store.save_candidate(
        replace(second_candidate, executions=1, version=1),
        expected_version=second_candidate.version,
    )
    released = store.release_execution_attempts(
        second_reservation["reservation_id"],
        dispatched_attempt_count=1,
        owner_reference="owner-released-partial",
        expected_version=second_reservation["version"],
    )
    assert released["status"] == "released"
    assert released["dispatched_attempt_count"] == 1
    assert store.execution_budget_usage(session.session_id)["consumed"] == 3
    store.close()


def test_execution_reservation_migration_does_not_charge_released_work(tmp_path) -> None:
    database_path = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE execution_reservations (
                reservation_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                request_fingerprint TEXT NOT NULL,
                attempt_count INTEGER NOT NULL,
                status TEXT NOT NULL,
                owner_token TEXT NOT NULL,
                reservation_version INTEGER NOT NULL DEFAULT 0,
                expires_at_utc TEXT,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                UNIQUE (session_id, request_fingerprint)
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO execution_reservations
                (reservation_id, session_id, candidate_id, request_fingerprint,
                 attempt_count, status, owner_token, created_at_utc, updated_at_utc)
            VALUES (?, 'session', 'candidate', ?, 4, ?, 'owner', ?, ?)
            """,
            (
                ("completed", "completed-request", "completed", "2026-07-24", "2026-07-24"),
                ("released", "released-request", "released", "2026-07-24", "2026-07-24"),
            ),
        )

    store = PerformanceStore(db_path=database_path)
    rows = store._connection.execute(
        """
        SELECT status, dispatched_attempt_count
        FROM execution_reservations
        ORDER BY status
        """
    ).fetchall()

    assert [(row["status"], row["dispatched_attempt_count"]) for row in rows] == [
        ("completed", 4),
        ("released", 0),
    ]
    store.close()


def test_legacy_null_idempotency_fingerprint_allows_matching_replay() -> None:
    store = PerformanceStore(db_path=":memory:")
    case = store.create_performance_case(
        PerformanceCaseV1(
            case_id="legacy-case",
            query_fingerprint="legacy-query",
        )
    )
    store._connection.execute(
        """
        INSERT INTO operation_idempotency
            (scope, idempotency_key, aggregate_type, aggregate_id,
             request_fingerprint, created_at_utc)
        VALUES ('performance_case.create', 'legacy-key', 'performance_case',
                ?, NULL, '2026-07-24T00:00:00+00:00')
        """,
        (case.case_id,),
    )

    replay = store.create_performance_case(
        PerformanceCaseV1(query_fingerprint="legacy-query"),
        idempotency_key="legacy-key",
        request_fingerprint="request-v1:matching",
    )

    assert replay.case_id == case.case_id
    binding = store._connection.execute(
        """
        SELECT request_fingerprint
        FROM operation_idempotency
        WHERE scope = 'performance_case.create'
          AND idempotency_key = 'legacy-key'
        """
    ).fetchone()
    assert binding is not None
    assert binding["request_fingerprint"] == "request-v1:matching"
    store.close()


def test_legacy_null_idempotency_fingerprint_fails_closed_on_mismatch() -> None:
    store = PerformanceStore(db_path=":memory:")
    case = store.create_performance_case(
        PerformanceCaseV1(
            case_id="legacy-case-mismatch",
            query_fingerprint="legacy-query",
        )
    )
    store._connection.execute(
        """
        INSERT INTO operation_idempotency
            (scope, idempotency_key, aggregate_type, aggregate_id,
             request_fingerprint, created_at_utc)
        VALUES ('performance_case.create', 'legacy-key-mismatch',
                'performance_case', ?, NULL,
                '2026-07-24T00:00:00+00:00')
        """,
        (case.case_id,),
    )

    with pytest.raises(IdempotencyConflictError, match="different request"):
        store.create_performance_case(
            PerformanceCaseV1(query_fingerprint="new-query"),
            idempotency_key="legacy-key-mismatch",
            request_fingerprint="request-v1:new",
        )

    store.close()


def test_view_change_intent_requires_opt_in_and_survives_restart(tmp_path) -> None:
    state_dir = tmp_path / "state"
    payload = {
        "target_definition": "SELECT [Id] FROM [dbo].[SyntheticSource]",
        "prior_definition": (
            "CREATE VIEW [dbo].[SyntheticView] AS "
            "SELECT [Id] FROM [dbo].[SyntheticArchive];"
        ),
    }
    with PerformanceStore(state_dir) as store:
        with pytest.raises(PermissionError, match="raw SQL"):
            store.create_view_change_intent(
                change_id="view-change",
                database_fingerprint="database-fingerprint",
                request_fingerprint="request-fingerprint",
                payload=payload,
                raw_sql_persistence_authorized=False,
            )
        prepared = store.create_view_change_intent(
            change_id="view-change",
            database_fingerprint="database-fingerprint",
            request_fingerprint="request-fingerprint",
            payload=payload,
            raw_sql_persistence_authorized=True,
        )
        applying = store.update_view_change_intent(
            "view-change",
            status="applying",
            expected_version=prepared["version"],
            raw_sql_persistence_authorized=True,
        )
        store.update_view_change_intent(
            "view-change",
            status="applied",
            expected_version=applying["version"],
            raw_sql_persistence_authorized=True,
            receipt={"object_id": 17, "target_fingerprint": "target-fingerprint"},
        )

    with PerformanceStore(state_dir) as reopened:
        restored = reopened.get_view_change_intent("view-change")

    assert restored["status"] == "applied"
    assert restored["payload"] == payload
    assert restored["receipt"]["object_id"] == 17
    assert os.stat(state_dir / "performance.sqlite3").st_mode & 0o777 == 0o600


def test_view_change_intent_rejects_identifier_reuse(tmp_path) -> None:
    store = PerformanceStore(tmp_path / "state")
    store.create_view_change_intent(
        change_id="view-change",
        database_fingerprint="database-fingerprint",
        request_fingerprint="request-a",
        payload={"definition": "SELECT 1 AS [Value]"},
        raw_sql_persistence_authorized=True,
    )

    with pytest.raises(IdempotencyConflictError, match="different request"):
        store.create_view_change_intent(
            change_id="view-change",
            database_fingerprint="database-fingerprint",
            request_fingerprint="request-b",
            payload={"definition": "SELECT 2 AS [Value]"},
            raw_sql_persistence_authorized=True,
        )
    store.close()


def test_idempotent_view_change_intent_lookup_survives_replay(tmp_path) -> None:
    store = PerformanceStore(tmp_path / "state")
    payload = {
        "request": {
            "database_name": "appdb",
            "schema_name": "dbo",
            "view_name": "ReplayView",
            "definition": "SELECT 1",
            "idempotency_key": "replay-key",
        }
    }
    prepared = store.create_view_change_intent(
        change_id="view-replay",
        database_fingerprint="database-fingerprint",
        request_fingerprint="request-fingerprint",
        payload=payload,
        raw_sql_persistence_authorized=True,
    )

    replay = store.get_idempotent_view_change_intent(
        database_fingerprint="database-fingerprint",
        idempotency_key="replay-key",
    )

    assert replay is not None
    assert replay["change_id"] == "view-replay"
    stored_key = replay["payload"]["request"]["idempotency_key"]
    assert stored_key.startswith("idempotency-v1:")
    assert stored_key != "replay-key"
    assert prepared["payload"] == replay["payload"]
    row = store._connection.execute(
        """
        SELECT idempotency_key_digest, payload
        FROM view_change_intents
        WHERE change_id = 'view-replay'
        """
    ).fetchone()
    assert row is not None
    assert row["idempotency_key_digest"] == stored_key
    assert "replay-key" not in row["payload"]
    assert "replay-key" not in json.dumps(replay)
    store.close()


def test_view_idempotency_prefix_is_not_trusted_without_a_full_digest(tmp_path) -> None:
    store = PerformanceStore(tmp_path / "state")
    caller_key = "idempotency-v1:not-a-valid-digest-token"
    prepared = store.create_view_change_intent(
        change_id="view-prefixed-key",
        database_fingerprint="database-fingerprint",
        request_fingerprint="request-fingerprint",
        payload={
            "request": {
                "database_name": "appdb",
                "schema_name": "dbo",
                "view_name": "PrefixedKeyView",
                "definition": "SELECT 1",
                "idempotency_key": caller_key,
            }
        },
        raw_sql_persistence_authorized=True,
    )

    replay = store.get_idempotent_view_change_intent(
        database_fingerprint="database-fingerprint",
        idempotency_key=caller_key,
    )
    stored_key = prepared["payload"]["request"]["idempotency_key"]
    row = store._connection.execute(
        "SELECT idempotency_key_digest, payload FROM view_change_intents"
    ).fetchone()

    assert replay is not None
    assert replay["change_id"] == "view-prefixed-key"
    assert stored_key.startswith("idempotency-v1:")
    assert len(stored_key) == len("idempotency-v1:") + 64
    assert stored_key != caller_key
    assert row is not None
    assert row["idempotency_key_digest"] == stored_key
    assert caller_key not in row["payload"]
    store.close()


def test_legacy_view_idempotency_key_is_migrated_to_digest(tmp_path) -> None:
    database_path = tmp_path / "legacy-view.sqlite3"
    payload = {
        "state_version": 1,
        "request": {
            "database_name": "appdb",
            "schema_name": "dbo",
            "view_name": "LegacyView",
            "definition": "SELECT 1",
            "idempotency_key": "legacy-plaintext-key",
        },
    }
    serialized_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    original_fingerprint = hashlib.sha256(
        serialized_payload.encode("utf-8")
    ).hexdigest()
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE view_change_intents (
                change_id TEXT PRIMARY KEY,
                database_fingerprint TEXT NOT NULL,
                request_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                receipt TEXT,
                intent_version INTEGER NOT NULL DEFAULT 0,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO view_change_intents
                (change_id, database_fingerprint, request_fingerprint,
                 status, payload, receipt, intent_version,
                 created_at_utc, updated_at_utc)
            VALUES ('legacy-view', 'database-fingerprint',
                    ?, 'prepared', ?, NULL, 0,
                    '2026-07-24T00:00:00+00:00',
                    '2026-07-24T00:00:00+00:00')
            """,
            (original_fingerprint, serialized_payload),
        )

    with PerformanceStore(db_path=database_path) as store:
        replay = store.get_idempotent_view_change_intent(
            database_fingerprint="database-fingerprint",
            idempotency_key="legacy-plaintext-key",
        )
        row = store._connection.execute(
            """
            SELECT idempotency_key_digest, payload, request_fingerprint
            FROM view_change_intents
            WHERE change_id = 'legacy-view'
            """
        ).fetchone()

    assert replay is not None
    assert row is not None
    assert row["idempotency_key_digest"].startswith("idempotency-v1:")
    assert "legacy-plaintext-key" not in row["payload"]
    assert "legacy-plaintext-key" not in json.dumps(replay)
    assert row["request_fingerprint"] != original_fingerprint


def test_legacy_view_migration_rejects_tampered_payload(tmp_path) -> None:
    database_path = tmp_path / "tampered-legacy-view.sqlite3"
    payload = json.dumps(
        {
            "state_version": 1,
            "request": {
                "database_name": "appdb",
                "schema_name": "dbo",
                "view_name": "TamperedView",
                "definition": "SELECT 2",
                "idempotency_key": "legacy-key",
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE view_change_intents (
                change_id TEXT PRIMARY KEY,
                database_fingerprint TEXT NOT NULL,
                request_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                receipt TEXT,
                intent_version INTEGER NOT NULL DEFAULT 0,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO view_change_intents
                (change_id, database_fingerprint, request_fingerprint,
                 status, payload, receipt, intent_version,
                 created_at_utc, updated_at_utc)
            VALUES ('tampered-view', 'database-fingerprint',
                    'mismatched-fingerprint', 'prepared', ?, NULL, 0,
                    '2026-07-24T00:00:00+00:00',
                    '2026-07-24T00:00:00+00:00')
            """,
            (payload,),
        )

    with pytest.raises(PerformanceStoreError, match="integrity check"):
        PerformanceStore(db_path=database_path)


def test_expired_execution_reservations_are_conservatively_charged() -> None:
    store = PerformanceStore(db_path=":memory:")
    case = store.create_performance_case(
        PerformanceCaseV1(case_id="case-expiry", query_fingerprint="query-hash")
    )
    session = store.create_session(
        TuningSessionV1(
            session_id="session-expiry",
            performance_case_id=case.case_id,
            max_candidates=1,
            execution_limit=3,
        )
    )
    candidate_slot = store.reserve_candidate_slot(
        session.session_id,
        "candidate-request-a",
    )
    store._connection.execute(
        """
        UPDATE candidate_slot_reservations
        SET expires_at_utc = '2000-01-01T00:00:00+00:00'
        WHERE reservation_id = ?
        """,
        (candidate_slot["reservation_id"],),
    )
    replacement_slot = store.reserve_candidate_slot(
        session.session_id,
        "candidate-request-b",
        owner_reference="replacement-owner",
    )
    expired_slot = store.reserve_candidate_slot(
        session.session_id,
        "candidate-request-a",
    )

    assert replacement_slot["status"] == "reserved"
    assert expired_slot["status"] == "expired"
    assert expired_slot["replayed"] is True

    candidate = store.create_candidate_and_attach(
        session,
        TuningCandidateV1(
            candidate_id="candidate-expiry",
            session_id=session.session_id,
        ),
        reservation_id=replacement_slot["reservation_id"],
        owner_reference="replacement-owner",
    )
    execution = store.reserve_execution_attempts(
        session.session_id,
        candidate.candidate_id,
        3,
        "execution-request-a",
        owner_reference="execution-owner-a",
    )
    store._connection.execute(
        """
        UPDATE execution_reservations
        SET expires_at_utc = '2000-01-01T00:00:00+00:00'
        WHERE reservation_id = ?
        """,
        (execution["reservation_id"],),
    )

    usage = store.execution_budget_usage(session.session_id)
    with pytest.raises(ReservationError, match="reserved"):
        store.reserve_execution_attempts(
            session.session_id,
            candidate.candidate_id,
            1,
            "execution-request-b",
            owner_reference="execution-owner-b",
        )
    expired_execution = store.reserve_execution_attempts(
        session.session_id,
        candidate.candidate_id,
        3,
        "execution-request-a",
        owner_reference="execution-owner-a",
    )

    assert usage["reserved"] == 0
    assert usage["consumed"] == 3
    assert usage["remaining"] == 0
    assert expired_execution["status"] == "expired"
    assert expired_execution["replayed"] is True
    store.close()


@pytest.mark.parametrize(
    ("finalizer", "expected_status"),
    (("release", "expired"), ("complete", "completed")),
)
def test_expired_execution_finalization_preserves_full_charge(
    finalizer: str,
    expected_status: str,
) -> None:
    store = PerformanceStore(db_path=":memory:")
    case = store.create_performance_case(
        PerformanceCaseV1(
            case_id=f"case-expired-{finalizer}",
            query_fingerprint="query-hash",
        )
    )
    session = store.create_session(
        TuningSessionV1(
            session_id=f"session-expired-{finalizer}",
            performance_case_id=case.case_id,
            execution_limit=3,
        )
    )
    candidate = store.create_candidate_and_attach(
        session,
        TuningCandidateV1(
            candidate_id=f"candidate-expired-{finalizer}",
            session_id=session.session_id,
        ),
    )
    reservation = store.reserve_execution_attempts(
        session.session_id,
        candidate.candidate_id,
        3,
        f"request-expired-{finalizer}",
        owner_reference=f"owner-expired-{finalizer}",
    )
    store._connection.execute(
        """
        UPDATE execution_reservations
        SET expires_at_utc = '2000-01-01T00:00:00+00:00'
        WHERE reservation_id = ?
        """,
        (reservation["reservation_id"],),
    )
    store.execution_budget_usage(session.session_id)
    expired = store.get_execution_reservation(reservation["reservation_id"])

    if finalizer == "release":
        finalized = store.release_execution_attempts(
            reservation["reservation_id"],
            dispatched_attempt_count=0,
            owner_reference=f"owner-expired-{finalizer}",
            expected_version=expired["version"],
        )
    else:
        finalized = store.complete_execution_attempts(
            reservation["reservation_id"],
            dispatched_attempt_count=1,
            owner_reference=f"owner-expired-{finalizer}",
            expected_version=expired["version"],
        )

    assert finalized["status"] == expected_status
    assert store.execution_budget_usage(session.session_id)["consumed"] == 3
    if finalizer == "complete":
        assert finalized["dispatched_attempt_count"] == 3
    store.close()


def test_index_lease_fencing_and_active_target_reservation(tmp_path) -> None:
    store = PerformanceStore(tmp_path / "state")
    lease = store.create_index_lease(
        lease_id="lease-owner",
        database_fingerprint="database-fingerprint",
        session_id="session-1",
        candidate_id="candidate-1",
        index_name="IX_Testing_synthetic",
        object_fingerprint="object-fingerprint",
        expires_at_utc="2026-07-15T12:00:00+00:00",
        owner_reference="owner-a",
        request_fingerprint="lease-request-1",
    )
    with pytest.raises(LeaseFencingError):
        store.update_index_lease(
            "lease-owner",
            status="active",
            owner_reference="owner-b",
        )
    updated = store.update_index_lease(
        "lease-owner",
        status="active",
        owner_reference="owner-a",
        expected_version=lease["version"],
    )
    with pytest.raises(ConcurrencyError):
        store.update_index_lease(
            "lease-owner",
            status="cleanup_pending",
            owner_reference="owner-a",
            expected_version=lease["version"],
        )
    with pytest.raises(LeaseConflictError):
        store.create_index_lease(
            lease_id="lease-other",
            database_fingerprint="database-fingerprint",
            session_id="session-2",
            candidate_id="candidate-2",
            index_name="IX_Other",
            object_fingerprint="object-other",
            expires_at_utc="2026-07-15T12:00:00+00:00",
            owner_reference="owner-b",
        )
    assert updated["version"] == lease["version"] + 1
    store.close()
