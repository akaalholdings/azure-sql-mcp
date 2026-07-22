from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from azure_sql_mcp.performance_contracts import PerformanceCaseV1
from azure_sql_mcp.performance_store import PerformanceStore
from azure_sql_mcp.tuning_sessions import (
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
            strategy="different-strategy",
            rewrite_fingerprint="different-rewrite",
            idempotency_key="candidate-create",
        )
        assert replay == first

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
