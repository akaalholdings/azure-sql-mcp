from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from azure_sql_mcp.observability import sanitize_error_message
from azure_sql_mcp.performance_contracts import (
    STOPPING_REASON_MAX_LENGTH,
    ContractValidationError,
    EvidenceEnvelopeV1,
    PerformanceCaseV1,
)
from azure_sql_mcp.performance_store import IdempotencyConflictError, PerformanceStore
from azure_sql_mcp.tuning_sessions import (
    InvalidTransitionError,
    TuningBudgetExceeded,
    TuningSessionStateMachine,
)


class MutableClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.value


def _new_machine(tmp_path, *, clock=None):
    store = PerformanceStore(tmp_path / "state")
    case = store.create_performance_case(
        PerformanceCaseV1(case_id="case-1", query_fingerprint="query-hash")
    )
    return store, TuningSessionStateMachine(store, clock=clock), case


def test_defaults_and_candidate_failure_keep_session_and_rewrite_state(tmp_path) -> None:
    store, machine, case = _new_machine(tmp_path)
    try:
        session = machine.create_session(case, idempotency_key="session-create")
        assert session.max_candidates == 10
        assert session.screen_runs_per_candidate == 3
        assert session.finalist_runs_per_candidate == 5
        assert session.parameter_case_limit == 4
        assert session.execution_limit == 80
        assert session.time_limit_seconds == 20 * 60

        candidate = machine.add_candidate(
            session.session_id,
            strategy="join-order",
            rewrite_fingerprint="rewrite-hash",
            rewrite_artifact_ref="artifact-ref-1",
        )
        machine.start_screening(session.session_id)
        machine.record_candidate_result(
            session.session_id,
            candidate.candidate_id,
            screen_runs=1,
            parameter_cases=2,
            executions=2,
            evidence_ids=("evidence-1",),
        )
        final_session, failed_candidate = machine.mark_candidate_terminal(
            session.session_id,
            candidate.candidate_id,
            "regressed",
            failure_code="slower",
            idempotency_key="candidate-regression",
        )

        assert final_session.status == "screening"
        assert failed_candidate.state == "regressed"
        assert failed_candidate.rewrite_fingerprint == "rewrite-hash"
        assert failed_candidate.rewrite_artifact_ref == "artifact-ref-1"
        assert failed_candidate.screen_runs == 1
        assert failed_candidate.evidence_ids == ("evidence-1",)
        assert machine.get_session(session.session_id).status == "screening"
        assert any(
            event["event_type"] == "session.candidate.result"
            for event in store.list_events(aggregate_type="session")
        )
    finally:
        store.close()


def test_candidate_and_result_operations_are_idempotent(tmp_path) -> None:
    store, machine, case = _new_machine(tmp_path)
    try:
        session = machine.create_session(case)
        first = machine.add_candidate(
            session.session_id,
            strategy="predicate",
            rewrite_fingerprint="rewrite-hash",
            idempotency_key="candidate-create",
        )
        replay = machine.add_candidate(
            session.session_id,
            strategy="predicate",
            rewrite_fingerprint="rewrite-hash",
            idempotency_key="candidate-create",
        )
        assert replay == first
        with pytest.raises(IdempotencyConflictError):
            machine.add_candidate(
                session.session_id,
                strategy="different-strategy",
                rewrite_fingerprint="different-rewrite",
                idempotency_key="candidate-create",
            )

        machine.start_screening(session.session_id)
        first_result = machine.record_candidate_result(
            session.session_id,
            first.candidate_id,
            screen_runs=1,
            executions=1,
            idempotency_key="screen-1",
        )
        replay_result = machine.record_candidate_result(
            session.session_id,
            first.candidate_id,
            screen_runs=1,
            executions=1,
            idempotency_key="screen-1",
        )
        assert replay_result == first_result
        assert machine.get_candidate(first.candidate_id).screen_runs == 1
    finally:
        store.close()


def test_only_explicit_lineage_work_can_add_a_candidate_during_finalist_validation(
    tmp_path,
) -> None:
    store, machine, case = _new_machine(tmp_path)
    try:
        session = machine.create_session(case)
        parent = machine.add_candidate(
            session.session_id,
            strategy="predicate",
            rewrite_fingerprint="rewrite-hash",
        )
        machine.start_screening(session.session_id)
        machine.mark_candidate_finalist(session.session_id, parent.candidate_id)
        evidence = store.create_evidence(
            EvidenceEnvelopeV1(
                evidence_id="evidence-parent",
                kind="tuning_finalist",
                observed_execution_count=2,
                metrics={"classification": "improved"},
                metadata={
                    "session_id": session.session_id,
                    "candidate_id": parent.candidate_id,
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
            )
        )
        _session, parent = machine.record_candidate_result(
            session.session_id,
            parent.candidate_id,
            state="improved",
            finalist_runs=1,
            executions=1,
            evidence_ids=(evidence.evidence_id,),
        )

        with pytest.raises(InvalidTransitionError, match="lineage-backed combined"):
            machine.add_candidate(
                session.session_id,
                strategy="combined",
                rewrite_fingerprint="rewrite-hash",
            )

        with pytest.raises(InvalidTransitionError, match="lineage-backed combined"):
            machine.add_candidate(
                session.session_id,
                strategy="predicate",
                rewrite_fingerprint="rewrite-hash",
            )

        with pytest.raises(InvalidTransitionError, match="lineage-backed combined"):
            machine.add_candidate(
                session.session_id,
                strategy="combined",
                rewrite_fingerprint="rewrite-hash",
                rewrite_artifact_ref=f"candidate:{parent.candidate_id}",
            )

        child = machine.add_candidate(
            session.session_id,
            strategy="combined",
            rewrite_fingerprint="rewrite-hash",
            rewrite_artifact_ref=f"candidate:{parent.candidate_id}",
            metadata={
                "lineage": {
                    "lineage_contract_version": 1,
                    "parent_candidate_id": parent.candidate_id,
                    "parent_evidence_id": evidence.evidence_id,
                    "parent_rewrite_fingerprint": "rewrite-hash",
                    "parent_equivalence": "proven",
                    "marginal_experiment": (
                        "rewrite_without_index_with_index_after_cleanup"
                    ),
                }
            },
        )

        assert child.strategy == "combined"
        assert child.session_id == session.session_id
    finally:
        store.close()


def test_rewrite_plus_index_accepts_performance_only_parent_without_proving_child(
    tmp_path,
) -> None:
    store, machine, case = _new_machine(tmp_path)
    try:
        session = machine.create_session(case)
        parent = machine.add_candidate(
            session.session_id,
            strategy="predicate",
            rewrite_fingerprint="rewrite-hash",
        )
        machine.start_screening(session.session_id)
        machine.mark_candidate_finalist(session.session_id, parent.candidate_id)
        evidence = store.create_evidence(
            EvidenceEnvelopeV1(
                evidence_id="evidence-performance-only-parent",
                kind="tuning_finalist",
                observed_execution_count=10,
                metrics={
                    "classification": "performance_only",
                    "performance_classification": "improved",
                },
                metadata={
                    "session_id": session.session_id,
                    "candidate_id": parent.candidate_id,
                    "phase": "finalist",
                    "proof_scope": "performance_only",
                    "equivalence_deferred": True,
                    "equivalence": [],
                },
            )
        )
        _session, parent = machine.record_candidate_result(
            session.session_id,
            parent.candidate_id,
            state="performance_only",
            finalist_runs=1,
            executions=10,
            evidence_ids=(evidence.evidence_id,),
        )

        child = machine.add_candidate(
            session.session_id,
            strategy="rewrite_plus_index",
            rewrite_fingerprint="rewrite-hash",
            rewrite_artifact_ref=f"candidate:{parent.candidate_id}",
            metadata={
                "lineage": {
                    "lineage_contract_version": 1,
                    "parent_candidate_id": parent.candidate_id,
                    "parent_evidence_id": evidence.evidence_id,
                    "parent_rewrite_fingerprint": "rewrite-hash",
                    "parent_equivalence": "unproven",
                    "marginal_experiment": (
                        "rewrite_without_index_with_index_after_cleanup"
                    ),
                }
            },
        )

        assert child.strategy == "rewrite_plus_index"
        assert child.metadata["lineage"]["parent_equivalence"] == "unproven"
    finally:
        store.close()


def test_transition_replay_is_bound_to_the_exact_candidate(tmp_path) -> None:
    store, machine, case = _new_machine(tmp_path)
    try:
        session = machine.create_session(case)
        candidate_a = machine.add_candidate(session.session_id, strategy="predicate")
        candidate_b = machine.add_candidate(session.session_id, strategy="join-shape")
        machine.start_screening(session.session_id)
        machine.record_candidate_result(
            session.session_id,
            candidate_a.candidate_id,
            screen_runs=1,
            executions=1,
            idempotency_key="candidate-a-result",
        )
        binding = store._connection.execute(
            """
            SELECT request_fingerprint
            FROM operation_idempotency
            WHERE scope = 'tuning.transition'
              AND idempotency_key = 'candidate-a-result'
            """
        ).fetchone()
        assert binding is not None

        with pytest.raises(IdempotencyConflictError, match="candidate event"):
            store.replay_session_and_candidate_transition(
                session.session_id,
                candidate_b.candidate_id,
                idempotency_key="candidate-a-result",
                request_fingerprint=binding["request_fingerprint"],
            )
    finally:
        store.close()


def test_legacy_transition_replay_uses_candidate_event_to_upgrade_binding(
    tmp_path,
) -> None:
    store, machine, case = _new_machine(tmp_path)
    try:
        session = machine.create_session(case)
        candidate = machine.add_candidate(session.session_id, strategy="predicate")
        machine.start_screening(session.session_id)
        expected = machine.record_candidate_result(
            session.session_id,
            candidate.candidate_id,
            screen_runs=1,
            executions=1,
            idempotency_key="legacy-candidate-result",
        )
        binding = store._connection.execute(
            """
            SELECT request_fingerprint
            FROM operation_idempotency
            WHERE scope = 'tuning.transition'
              AND idempotency_key = 'legacy-candidate-result'
            """
        ).fetchone()
        assert binding is not None
        request_fingerprint = binding["request_fingerprint"]
        store._connection.execute(
            """
            UPDATE operation_idempotency
            SET request_fingerprint = NULL
            WHERE scope = 'tuning.transition'
              AND idempotency_key = 'legacy-candidate-result'
            """
        )

        replay = store.replay_session_and_candidate_transition(
            session.session_id,
            candidate.candidate_id,
            idempotency_key="legacy-candidate-result",
            request_fingerprint=request_fingerprint,
        )
        upgraded = store._connection.execute(
            """
            SELECT request_fingerprint
            FROM operation_idempotency
            WHERE scope = 'tuning.transition'
              AND idempotency_key = 'legacy-candidate-result'
            """
        ).fetchone()

        assert replay == expected
        assert upgraded is not None
        assert upgraded["request_fingerprint"] == request_fingerprint
    finally:
        store.close()


def test_hard_candidate_and_execution_budgets_are_enforced(tmp_path) -> None:
    store, machine, case = _new_machine(tmp_path)
    try:
        session = machine.create_session(
            case,
            max_candidates=1,
            screen_runs_per_candidate=1,
            execution_limit=2,
        )
        candidate = machine.add_candidate(session.session_id, strategy="one")
        with pytest.raises(TuningBudgetExceeded):
            machine.add_candidate(session.session_id, strategy="two")
        machine.start_screening(session.session_id)
        machine.record_candidate_result(
            session.session_id,
            candidate.candidate_id,
            screen_runs=1,
            executions=2,
        )
        with pytest.raises(TuningBudgetExceeded):
            machine.record_candidate_result(
                session.session_id,
                candidate.candidate_id,
                executions=1,
            )
    finally:
        store.close()


def test_time_budget_blocks_new_work(tmp_path) -> None:
    clock = MutableClock()
    store, machine, case = _new_machine(tmp_path, clock=clock)
    try:
        session = machine.create_session(case, time_limit_seconds=60)
        candidate = machine.add_candidate(session.session_id, strategy="late-result")
        machine.start_screening(session.session_id)
        clock.value += timedelta(seconds=61)
        with pytest.raises(TuningBudgetExceeded):
            machine.record_candidate_result(
                session.session_id,
                candidate.candidate_id,
                executions=1,
            )
        _, failed = machine.mark_candidate_terminal(
            session.session_id,
            candidate.candidate_id,
            "inconclusive",
            failure_code="timeout",
        )
        assert failed.state == "inconclusive"
        assert machine.get_session(session.session_id).status == "screening"
    finally:
        store.close()


def test_expiry_is_derived_but_late_terminal_result_and_finalization_are_allowed(
    tmp_path,
) -> None:
    clock = MutableClock()
    store, machine, case = _new_machine(tmp_path, clock=clock)
    try:
        session = machine.create_session(case, time_limit_seconds=60)
        candidate = machine.add_candidate(session.session_id, strategy="late-result")
        machine.start_screening(session.session_id)
        clock.value += timedelta(seconds=61)

        availability = machine.session_availability(machine.get_session(session.session_id))
        assert availability == {
            "lifecycle_status": "screening",
            "effective_status": "expired",
            "availability": "expired",
            "deadline_exceeded": True,
            "accepts_new_work": False,
            "accepts_finalization": True,
            "available": False,
            "actionable": False,
            "reason": "deadline_expired",
        }
        assert machine.get_session(session.session_id).status == "screening"

        _session, terminal = machine.mark_candidate_terminal(
            session.session_id,
            candidate.candidate_id,
            "inconclusive",
            failure_code="timeout",
        )
        completed = machine.complete_session(
            session.session_id,
            stopping_reason="late result recorded",
        )

        assert terminal.state == "inconclusive"
        assert completed.status == "completed"
        assert machine.get_session(session.session_id).status == "completed"
    finally:
        store.close()


def test_expired_created_session_can_record_unresolved_candidate_before_finalize(
    tmp_path,
) -> None:
    clock = MutableClock()
    store, machine, case = _new_machine(tmp_path, clock=clock)
    try:
        session = machine.create_session(case, time_limit_seconds=60)
        candidate = machine.add_candidate(session.session_id, strategy="not-started")
        clock.value += timedelta(seconds=61)

        with pytest.raises(TuningBudgetExceeded):
            machine.record_candidate_result(
                session.session_id,
                candidate.candidate_id,
                state="screening",
            )
        _session, unresolved = machine.record_candidate_result(
            session.session_id,
            candidate.candidate_id,
            state="inconclusive",
            failure_code="session_expired",
        )

        assert unresolved.state == "inconclusive"
        assert machine.get_session(session.session_id).status == "created"
    finally:
        store.close()


def test_finalist_reuses_screened_parameter_cases_without_double_counting(
    tmp_path,
) -> None:
    store, machine, case = _new_machine(tmp_path)
    try:
        session = machine.create_session(case, execution_limit=80)
        candidate = machine.add_candidate(session.session_id, strategy="predicate")
        machine.start_screening(session.session_id)
        machine.record_candidate_result(
            session.session_id,
            candidate.candidate_id,
            state="screening",
            screen_runs=3,
            parameter_cases=4,
            executions=32,
        )
        machine.mark_candidate_finalist(session.session_id, candidate.candidate_id)

        _session, finalist = machine.record_candidate_result(
            session.session_id,
            candidate.candidate_id,
            state="improved",
            finalist_runs=5,
            parameter_cases=4,
            executions=48,
        )

        assert finalist.parameter_cases == 4
        assert finalist.executions == 80
        assert finalist.state == "improved"
    finally:
        store.close()


def test_cancellation_persists_stopping_and_replay_metadata(tmp_path) -> None:
    store, machine, case = _new_machine(tmp_path)
    try:
        session = machine.create_session(
            case,
            replay_metadata={"resume_marker": "resume-1"},
            idempotency_key="session-replay",
        )
        cancelled = machine.cancel_session(
            session.session_id,
            stopping_reason="operator_requested",
            replay_metadata={"last_attempt": 2},
            idempotency_key="cancel-1",
        )

        assert cancelled.status == "cancelled"
        assert cancelled.stopping_reason == "operator_requested"
        assert cancelled.replay_metadata["resume_marker"] == "resume-1"
        assert cancelled.replay_metadata["last_attempt"] == 2
        assert machine.get_session(session.session_id) == cancelled
    finally:
        store.close()


def test_stopping_reason_limit_is_shared_and_persists_at_the_boundary(tmp_path) -> None:
    state_path = tmp_path / "state"
    store, machine, case = _new_machine(tmp_path)
    reason = "x" * STOPPING_REASON_MAX_LENGTH
    try:
        session = machine.create_session(case)
        cancelled = machine.cancel_session(
            session.session_id,
            stopping_reason=reason,
        )
        assert cancelled.stopping_reason == reason
    finally:
        store.close()

    reopened = PerformanceStore(state_path)
    try:
        assert reopened.get_session(session.session_id).stopping_reason == reason
    finally:
        reopened.close()

    store, machine, case = _new_machine(tmp_path / "too-long")
    try:
        session = machine.create_session(case)
        with pytest.raises(
            ContractValidationError,
            match=f"at most {STOPPING_REASON_MAX_LENGTH} characters",
        ):
            machine.cancel_session(
                session.session_id,
                stopping_reason="x" * (STOPPING_REASON_MAX_LENGTH + 1),
            )
    finally:
        store.close()


def test_public_session_status_guard_preserves_validated_states_after_sanitizing(
    tmp_path,
) -> None:
    store, machine, case = _new_machine(tmp_path)
    try:
        session = machine.create_session(case)
        machine.cancel_session(session.session_id)

        with pytest.raises(InvalidTransitionError) as raised:
            machine.require_session_status(
                session.session_id,
                allowed={"screening", "finalist_validation"},
            )

        sanitized = sanitize_error_message(str(raised.value))
        assert "is cancelled" in sanitized
        assert "screening" in sanitized
        assert "finalist_validation" in sanitized
        assert "[REDACTED]" not in sanitized

        with pytest.raises(ValueError, match="recognized session statuses"):
            machine.require_session_status(
                session.session_id,
                allowed={"screening", "untrusted state"},
            )
    finally:
        store.close()
