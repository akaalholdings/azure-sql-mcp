"""Database-independent tuning-session state machine.

The MCP server remains the only component allowed to execute SQL.  This
module coordinates redacted contracts, budgets, and durable state transitions
without accepting an executor or connection object.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping

from .candidate_lineage import validate_combined_parent_request
from .candidate_lineage import validate_rewrite_plus_index_parent_request
from .performance_contracts import (
    ALL_CANDIDATE_STATES,
    TERMINAL_CANDIDATE_STATES,
    PerformanceCaseV1,
    TuningCandidateV1,
    TuningSessionV1,
    new_id,
)
from .performance_store import PerformanceStore
from .performance_store import ReservationError
from .query_identity import request_fingerprint


DEFAULT_MAX_CANDIDATES = 10
DEFAULT_SCREEN_RUNS = 3
DEFAULT_FINALIST_RUNS = 5
DEFAULT_PARAMETER_CASES = 4
DEFAULT_EXECUTIONS = 80
DEFAULT_TIME_LIMIT_SECONDS = 20 * 60


class TuningSessionError(RuntimeError):
    """Base class for state-machine errors."""


class InvalidTransitionError(TuningSessionError):
    """Raised when a requested session or candidate transition is invalid."""


class TuningBudgetExceeded(TuningSessionError):
    """Raised when a hard tuning budget would be exceeded."""


Clock = Callable[[], datetime]


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _as_utc(value).isoformat()


def _parse_iso(value: str) -> datetime:
    return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _unique_strings(values: Iterable[str]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            result.append(value)
            seen.add(value)
    return tuple(result)


def _safe_failure_code(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if not normalized or len(normalized) > 100:
        raise ValueError("failure_code must be a non-empty code of at most 100 characters.")
    if any(character not in "abcdefghijklmnopqrstuvwxyz0123456789_.:-" for character in normalized):
        raise ValueError("failure_code must contain only lowercase code characters.")
    return normalized


class TuningSessionStateMachine:
    """Coordinate durable session and candidate transitions."""

    def __init__(self, store: PerformanceStore, *, clock: Clock | None = None) -> None:
        self.store = store
        self._clock = clock or _default_clock

    def create_session(
        self,
        performance_case: PerformanceCaseV1 | str,
        *,
        session_id: str | None = None,
        max_candidates: int = DEFAULT_MAX_CANDIDATES,
        screen_runs_per_candidate: int = DEFAULT_SCREEN_RUNS,
        finalist_runs_per_candidate: int = DEFAULT_FINALIST_RUNS,
        parameter_case_limit: int = DEFAULT_PARAMETER_CASES,
        execution_limit: int = DEFAULT_EXECUTIONS,
        time_limit_seconds: int = DEFAULT_TIME_LIMIT_SECONDS,
        metadata: Mapping[str, Any] | None = None,
        replay_metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> TuningSessionV1:
        case_id = performance_case.case_id if isinstance(performance_case, PerformanceCaseV1) else performance_case
        now = self._clock()
        operation_fingerprint = request_fingerprint(
            "tuning-session.create",
            {
                "performance_case_id": case_id,
                "max_candidates": max_candidates,
                "screen_runs_per_candidate": screen_runs_per_candidate,
                "finalist_runs_per_candidate": finalist_runs_per_candidate,
                "parameter_case_limit": parameter_case_limit,
                "execution_limit": execution_limit,
                "time_limit_seconds": time_limit_seconds,
                "metadata": metadata or {},
            },
        )
        durable_replay_metadata = {
            "request_fingerprint": operation_fingerprint,
            **dict(replay_metadata or {}),
        }
        session = TuningSessionV1(
            session_id=session_id or new_id("session"),
            performance_case_id=case_id,
            created_at_utc=_iso(now),
            updated_at_utc=_iso(now),
            started_at_utc=_iso(now),
            deadline_at_utc=_iso(now + timedelta(seconds=time_limit_seconds)),
            max_candidates=max_candidates,
            screen_runs_per_candidate=screen_runs_per_candidate,
            finalist_runs_per_candidate=finalist_runs_per_candidate,
            parameter_case_limit=parameter_case_limit,
            execution_limit=execution_limit,
            time_limit_seconds=time_limit_seconds,
            replay_metadata=durable_replay_metadata,
            metadata=metadata or {},
        )
        return self.store.create_session(
            session,
            idempotency_key=idempotency_key,
            request_fingerprint=operation_fingerprint,
        )

    def get_session(self, session_id: str) -> TuningSessionV1:
        return self.store.get_session(session_id)

    def get_candidate(self, candidate_id: str) -> TuningCandidateV1:
        return self.store.get_candidate(candidate_id)

    def list_candidates(self, session_id: str) -> list[TuningCandidateV1]:
        return self.store.list_candidates(session_id)

    def add_candidate(
        self,
        session_id: str,
        *,
        strategy: str,
        rewrite_fingerprint: str | None = None,
        rewrite_artifact_ref: str | None = None,
        candidate_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        request_fingerprint_metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> TuningCandidateV1:
        fingerprint_metadata = (
            metadata
            if request_fingerprint_metadata is None
            else request_fingerprint_metadata
        )
        replay = self.replay_candidate_creation(
            session_id,
            strategy=strategy,
            rewrite_fingerprint=rewrite_fingerprint,
            rewrite_artifact_ref=rewrite_artifact_ref,
            metadata=fingerprint_metadata,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            return replay
        session = self._active_session(
            session_id,
            allowed={"created", "screening", "finalist_validation"},
        )
        if session.status == "finalist_validation":
            try:
                lineage_validator = {
                    "combined": validate_combined_parent_request,
                    "rewrite_plus_index": validate_rewrite_plus_index_parent_request,
                }.get(strategy)
                if lineage_validator is None:
                    raise ValueError("Candidate strategy is not lineage-backed.")
                parent_reference = str(rewrite_artifact_ref or "")
                parent_id = parent_reference.removeprefix("candidate:")
                parent = self.get_candidate(parent_id)
                expected_lineage = lineage_validator(
                    session_id=session_id,
                    rewrite_fingerprint=rewrite_fingerprint or "",
                    parent_reference=parent_reference,
                    parent=parent,
                    evidence=[
                        self.store.get_evidence(evidence_id)
                        for evidence_id in parent.evidence_ids
                    ],
                )
            except (KeyError, TypeError, ValueError) as exc:
                lineage_name = (
                    "lineage-backed rewrite_plus_index"
                    if strategy == "rewrite_plus_index"
                    else "lineage-backed combined"
                )
                raise InvalidTransitionError(
                    "Finalist-validation candidate creation requires a valid "
                    f"{lineage_name} parent."
                ) from exc
            if dict((metadata or {}).get("lineage") or {}) != expected_lineage:
                lineage_name = (
                    "lineage-backed rewrite_plus_index"
                    if strategy == "rewrite_plus_index"
                    else "lineage-backed combined"
                )
                raise InvalidTransitionError(
                    "Only a valid "
                    f"{lineage_name} child candidate may be created during "
                    "finalist validation."
                )
        self._check_deadline(session)
        candidate = TuningCandidateV1(
            candidate_id=candidate_id or new_id("candidate"),
            session_id=session_id,
            ordinal=0,
            strategy=strategy,
            rewrite_fingerprint=rewrite_fingerprint,
            rewrite_artifact_ref=rewrite_artifact_ref,
            metadata=metadata or {},
        )
        operation_fingerprint = self.candidate_creation_request_fingerprint(
            session_id,
            strategy=strategy,
            rewrite_fingerprint=rewrite_fingerprint,
            rewrite_artifact_ref=rewrite_artifact_ref,
            metadata=fingerprint_metadata,
            idempotency_key=idempotency_key,
            candidate_id=candidate.candidate_id,
        )
        try:
            return self.store.create_candidate_and_attach(
                session,
                candidate,
                expected_session_version=session.version,
                idempotency_key=idempotency_key,
                request_fingerprint=operation_fingerprint,
            )
        except ReservationError as exc:
            raise TuningBudgetExceeded(str(exc)) from exc

    def replay_candidate_creation(
        self,
        session_id: str,
        *,
        strategy: str,
        rewrite_fingerprint: str | None,
        rewrite_artifact_ref: str | None,
        metadata: Mapping[str, Any] | None,
        idempotency_key: str | None,
    ) -> TuningCandidateV1 | None:
        """Replay a committed candidate request before caller-side duplicate checks."""

        if not idempotency_key:
            return None
        operation_fingerprint = self.candidate_creation_request_fingerprint(
            session_id,
            strategy=strategy,
            rewrite_fingerprint=rewrite_fingerprint,
            rewrite_artifact_ref=rewrite_artifact_ref,
            metadata=metadata,
            idempotency_key=idempotency_key,
            candidate_id=None,
        )
        return self.store.get_idempotent_candidate_creation(
            session_id,
            idempotency_key,
            request_fingerprint=operation_fingerprint,
        )

    @staticmethod
    def candidate_creation_request_fingerprint(
        session_id: str,
        *,
        strategy: str,
        rewrite_fingerprint: str | None,
        rewrite_artifact_ref: str | None,
        metadata: Mapping[str, Any] | None,
        idempotency_key: str | None,
        candidate_id: str | None,
    ) -> str:
        return request_fingerprint(
            "tuning-session.add-candidate",
            {
                "session_id": session_id,
                "strategy": strategy,
                "rewrite_fingerprint": rewrite_fingerprint,
                "rewrite_artifact_ref": rewrite_artifact_ref,
                "metadata": metadata or {},
                "candidate_request_key": idempotency_key or candidate_id,
            },
        )

    def ensure_dispatch_allowed(self, session_id: str) -> TuningSessionV1:
        """Read durable state and reject a new SQL dispatch after the deadline."""

        session = self._active_session(
            session_id,
            allowed={"screening", "finalist_validation"},
        )
        self._check_deadline(session)
        return session

    def start_screening(
        self,
        session_id: str,
        *,
        idempotency_key: str | None = None,
    ) -> TuningSessionV1:
        session = self._active_session(session_id, allowed={"created", "screening"})
        self._check_deadline(session)
        if session.status == "screening":
            return session
        if not session.candidate_ids:
            raise InvalidTransitionError("At least one candidate is required before screening.")
        updated = self._next_session(session, status="screening")
        return self.store.save_session(
            updated,
            expected_version=session.version,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint(
                "tuning-session.start-screening", {"session_id": session_id}
            ),
            event_type="screening.started",
            event_payload={"candidate_count": len(session.candidate_ids)},
        )

    def mark_candidate_finalist(
        self,
        session_id: str,
        candidate_id: str,
        *,
        idempotency_key: str | None = None,
    ) -> TuningCandidateV1:
        session = self._active_session(session_id, allowed={"screening", "finalist_validation"})
        candidate = self._candidate_in_session(session, candidate_id)
        self._check_deadline(session)
        if candidate.is_terminal:
            raise InvalidTransitionError("A terminal candidate cannot become a finalist.")
        if (
            candidate.state == "finalist"
            and candidate_id in session.finalist_candidate_ids
            and session.status == "finalist_validation"
        ):
            return candidate
        updated_candidate = self._next_candidate(candidate, state="finalist")
        updated_session = session
        if candidate_id not in session.finalist_candidate_ids:
            updated_session = self._next_session(
                session,
                finalist_candidate_ids=_unique_strings(
                    (*session.finalist_candidate_ids, candidate_id)
                ),
            )
        if updated_session.status == "screening":
            updated_session = replace(updated_session, status="finalist_validation")
        _session, updated_candidate = self.store.save_session_and_candidate(
            updated_session,
            updated_candidate,
            expected_session_version=session.version,
            expected_candidate_version=candidate.version,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint(
                "tuning-session.mark-finalist",
                {"session_id": session_id, "candidate_id": candidate_id},
            ),
            event_type="candidate.finalist",
            event_payload={"candidate_id": candidate_id},
        )
        return updated_candidate

    def record_candidate_result(
        self,
        session_id: str,
        candidate_id: str,
        *,
        state: str | None = None,
        screen_runs: int = 0,
        finalist_runs: int = 0,
        parameter_cases: int = 0,
        executions: int = 0,
        evidence_ids: Iterable[str] = (),
        failure_code: str | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[TuningSessionV1, TuningCandidateV1]:
        """Record a candidate result without allowing it to fail the session."""

        if any(value < 0 for value in (screen_runs, finalist_runs, parameter_cases, executions)):
            raise ValueError("Result counters must not be negative.")
        evidence_values = tuple(evidence_ids)
        if state is not None and state not in ALL_CANDIDATE_STATES:
            raise InvalidTransitionError(f"Unsupported candidate state: {state!r}.")
        safe_failure_code = _safe_failure_code(failure_code)
        operation_fingerprint = request_fingerprint(
            "tuning-session.record-candidate-result",
            {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "state": state,
                "screen_runs": screen_runs,
                "finalist_runs": finalist_runs,
                "parameter_cases": parameter_cases,
                "executions": executions,
                "evidence_ids": evidence_values,
                "failure_code": safe_failure_code,
            },
        )
        replay = self.store.replay_session_and_candidate_transition(
            session_id,
            candidate_id,
            idempotency_key=idempotency_key,
            request_fingerprint=operation_fingerprint,
        )
        if replay is not None:
            return replay

        session = self._active_session(
            session_id,
            allowed={"screening", "finalist_validation"},
        )
        candidate = self._candidate_in_session(session, candidate_id)
        if candidate.is_terminal:
            raise InvalidTransitionError("A terminal candidate cannot receive another result.")
        # A late failure/equivalence result must still be durable so it cannot
        # turn into a session failure merely because the work budget expired.
        self._check_deadline(
            session,
            allow_expired=state in TERMINAL_CANDIDATE_STATES,
        )
        if state in {"proposed", "screening"} and session.status != "screening":
            raise InvalidTransitionError("Screening results require screening status.")
        if state in {"finalist", "validating"} and session.status != "finalist_validation":
            raise InvalidTransitionError("Finalist results require finalist validation status.")
        if screen_runs and candidate.screen_runs + screen_runs > session.screen_runs_per_candidate:
            raise TuningBudgetExceeded("Screen-run budget has been reached for this candidate.")
        if finalist_runs and candidate.finalist_runs + finalist_runs > session.finalist_runs_per_candidate:
            raise TuningBudgetExceeded("Finalist-run budget has been reached for this candidate.")
        next_parameter_case_count = max(candidate.parameter_cases, parameter_cases)
        if next_parameter_case_count > session.parameter_case_limit:
            raise TuningBudgetExceeded("Parameter-case budget has been reached for this candidate.")
        total_executions = sum(item.executions for item in self.store.list_candidates(session_id))
        if total_executions + executions > session.execution_limit:
            raise TuningBudgetExceeded("Execution budget has been reached for this session.")
        next_state = state
        if next_state is None:
            next_state = "validating" if session.status == "finalist_validation" else "screening"
        updated_candidate = self._next_candidate(
            candidate,
            state=next_state,
            screen_runs=candidate.screen_runs + screen_runs,
            finalist_runs=candidate.finalist_runs + finalist_runs,
            parameter_cases=next_parameter_case_count,
            executions=candidate.executions + executions,
            evidence_ids=_unique_strings((*candidate.evidence_ids, *evidence_values)),
            failure_code=safe_failure_code or candidate.failure_code,
        )
        updated_session = self._next_session(session)
        return self.store.save_session_and_candidate(
            updated_session,
            updated_candidate,
            expected_session_version=session.version,
            expected_candidate_version=candidate.version,
            idempotency_key=idempotency_key,
            request_fingerprint=operation_fingerprint,
            event_type="candidate.result",
            event_payload={
                "candidate_id": candidate_id,
                "state": next_state,
                "screen_runs": screen_runs,
                "finalist_runs": finalist_runs,
                "parameter_cases": parameter_cases,
                "executions": executions,
                "failure_code": safe_failure_code,
            },
        )

    def mark_candidate_terminal(
        self,
        session_id: str,
        candidate_id: str,
        state: str,
        *,
        failure_code: str | None = None,
        evidence_ids: Iterable[str] = (),
        idempotency_key: str | None = None,
    ) -> tuple[TuningSessionV1, TuningCandidateV1]:
        if state not in TERMINAL_CANDIDATE_STATES:
            raise InvalidTransitionError(f"{state!r} is not a terminal candidate state.")
        return self.record_candidate_result(
            session_id,
            candidate_id,
            state=state,
            failure_code=failure_code,
            evidence_ids=evidence_ids,
            idempotency_key=idempotency_key,
        )

    def recover_candidate_for_finalist(
        self,
        session_id: str,
        candidate_id: str,
        *,
        idempotency_key: str | None = None,
    ) -> TuningCandidateV1:
        """Reopen only an old proof-contract screening result for validation."""

        session = self._active_session(
            session_id,
            allowed={"screening", "finalist_validation"},
        )
        candidate = self._candidate_in_session(session, candidate_id)
        if (
            candidate.state != "inconclusive"
            or candidate.failure_code != "proof_contract_required"
            or candidate.screen_runs <= 0
            or candidate.finalist_runs != 0
            or candidate.executions <= 0
        ):
            raise InvalidTransitionError(
                "Only the exact legacy proof_contract_required screening result "
                "may be recovered for finalist validation."
            )
        self._check_deadline(session)
        updated_candidate = self._next_candidate(
            candidate,
            state="finalist",
            failure_code=None,
        )
        updated_session = session
        if candidate_id not in session.finalist_candidate_ids:
            updated_session = self._next_session(
                session,
                finalist_candidate_ids=_unique_strings(
                    (*session.finalist_candidate_ids, candidate_id)
                ),
            )
        if updated_session.status == "screening":
            updated_session = replace(updated_session, status="finalist_validation")
        _session, updated_candidate = self.store.save_session_and_candidate(
            updated_session,
            updated_candidate,
            expected_session_version=session.version,
            expected_candidate_version=candidate.version,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint(
                "tuning-session.recover-proof-contract-finalist",
                {"session_id": session_id, "candidate_id": candidate_id},
            ),
            event_type="candidate.finalist.recovered",
            event_payload={"candidate_id": candidate_id},
        )
        return updated_candidate

    def complete_session(
        self,
        session_id: str,
        *,
        selected_candidate_id: str | None = None,
        stopping_reason: str = "completed",
        replay_metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> TuningSessionV1:
        session = self._active_session(
            session_id,
            allowed={"created", "screening", "finalist_validation"},
        )
        self._check_deadline(session, allow_expired=True)
        if selected_candidate_id is not None:
            candidate = self._candidate_in_session(session, selected_candidate_id)
            selection_scope = str(
                (replay_metadata or {}).get("selection_scope") or "proven"
            )
            expected_state = (
                "performance_only"
                if selection_scope == "performance_only"
                else "improved"
            )
            if candidate.state != expected_state:
                raise InvalidTransitionError(
                    f"Selection scope {selection_scope} requires a candidate in "
                    f"state {expected_state}."
                )
        updated = self._next_session(
            session,
            status="completed",
            selected_candidate_id=selected_candidate_id,
            stopping_reason=stopping_reason,
            replay_metadata={**session.replay_metadata, **dict(replay_metadata or {})},
        )
        return self.store.save_session(
            updated,
            expected_version=session.version,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint(
                "tuning-session.complete",
                {
                    "session_id": session_id,
                    "selected_candidate_id": selected_candidate_id,
                    "stopping_reason": stopping_reason,
                    "replay_metadata": replay_metadata or {},
                },
            ),
            event_type="session.completed",
            event_payload={"selected_candidate_id": selected_candidate_id},
        )

    def cancel_session(
        self,
        session_id: str,
        *,
        stopping_reason: str = "cancelled",
        replay_metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> TuningSessionV1:
        session = self._active_session(
            session_id,
            allowed={"created", "screening", "finalist_validation"},
        )
        updated = self._next_session(
            session,
            status="cancelled",
            stopping_reason=stopping_reason,
            replay_metadata={**session.replay_metadata, **dict(replay_metadata or {})},
        )
        return self.store.save_session(
            updated,
            expected_version=session.version,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint(
                "tuning-session.cancel",
                {
                    "session_id": session_id,
                    "stopping_reason": stopping_reason,
                    "replay_metadata": replay_metadata or {},
                },
            ),
            event_type="session.cancelled",
        )

    def _candidate_for_operation(
        self,
        session_id: str,
        idempotency_key: str | None,
    ) -> TuningCandidateV1 | None:
        if not idempotency_key:
            return None
        # Candidate creation uses a durable idempotency key.  A missing key
        # is deliberately indistinguishable from a first operation here.
        try:
            candidates = self.store.list_candidates(session_id)
        except KeyError:
            return None
        # The session update is the authoritative second half of add_candidate;
        # a retry is recognized by the candidate already being listed.
        marker = f"{idempotency_key}:candidate"
        for candidate in candidates:
            events = self.store.list_events(
                aggregate_type="candidate",
                aggregate_id=candidate.candidate_id,
            )
            if any(event["idempotency_key"] == f"candidate.create:{marker}" for event in events):
                return candidate
        return None

    def _active_session(
        self,
        session_id: str,
        *,
        allowed: set[str] | None = None,
    ) -> TuningSessionV1:
        session = self.store.get_session(session_id)
        if allowed is not None and session.status not in allowed:
            raise InvalidTransitionError(
                f"Session {session_id} is {session.status!r}; expected one of {sorted(allowed)}."
            )
        return session

    def _candidate_in_session(
        self,
        session: TuningSessionV1,
        candidate_id: str,
    ) -> TuningCandidateV1:
        if candidate_id not in session.candidate_ids:
            raise InvalidTransitionError("Candidate does not belong to this tuning session.")
        candidate = self.store.get_candidate(candidate_id)
        if candidate.session_id != session.session_id:
            raise InvalidTransitionError("Candidate session ownership does not match.")
        return candidate

    def _check_deadline(self, session: TuningSessionV1, *, allow_expired: bool = False) -> None:
        if allow_expired or not session.deadline_at_utc:
            return
        if self._clock() >= _parse_iso(session.deadline_at_utc):
            raise TuningBudgetExceeded("The tuning session time budget has expired.")

    def _next_session(self, session: TuningSessionV1, **changes: Any) -> TuningSessionV1:
        changes.setdefault("updated_at_utc", _iso(self._clock()))
        changes["version"] = session.version + 1
        return replace(session, **changes)

    def _next_candidate(self, candidate: TuningCandidateV1, **changes: Any) -> TuningCandidateV1:
        changes.setdefault("updated_at_utc", _iso(self._clock()))
        changes["version"] = candidate.version + 1
        return replace(candidate, **changes)


# Short aliases keep integration code readable while preserving the explicit
# state-machine name for callers that want to make the boundary obvious.
TuningSessionService = TuningSessionStateMachine
