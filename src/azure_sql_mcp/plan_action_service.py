"""Durable prepared Query Store actions with exact-state restoration."""

from __future__ import annotations

import asyncio
import hashlib
import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping, Sequence

from .admin_policy import AdminAction, AdminPolicy
from .config import AccessMode, McpProfile, ServerConfig, WritePolicy
from .connection import AzureSqlExecutor, QueryResult
from .database_policy import DatabasePolicySet
from .incident_log import note_exception
from .observability import sanitize_error_message
from .performance_contracts import PlanActionIntentV1, redact_metadata, utc_now
from .performance_store import ConcurrencyError, PerformanceStore
from .plan_verification import decide_verification, hash_evidence
from .query_hints import validate_query_hints
from .query_identity import request_fingerprint
from .query_identity import server_database_identity
from .query_identity import server_database_identity_matches


_OPERATIONS = {"force_plan", "unforce_plan", "set_hints", "clear_hints"}
DEFAULT_INTENT_TTL = timedelta(hours=24)
_BASELINE_WINDOW = timedelta(hours=24)
_MAX_EVIDENCE_ROWS = 200
_METRIC_TOTALS = (
    ("avg_duration", "total_duration"),
    ("avg_cpu_time", "total_cpu_time"),
    ("avg_logical_io_reads", "total_logical_io_reads"),
)
# Only whole Query Store intervals strictly before or after apply are read, so
# the interval that contains the apply time never mixes pre and post runs.
_RUNTIME_EVIDENCE_SQL = """
DECLARE @applied_at datetimeoffset(7) =
    TODATETIMEOFFSET(CONVERT(datetime2(7), ?, 126), 0);
SELECT
    CASE WHEN rsi.start_time >= @applied_at THEN 'post' ELSE 'baseline' END
        AS window_name,
    rs.plan_id,
    SUM(rs.count_executions) AS count_executions,
    SUM(rs.avg_duration * rs.count_executions) AS total_duration,
    SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu_time,
    SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
    SUM(rs.count_executions * (SQUARE(rs.stdev_duration) + SQUARE(rs.avg_duration)))
        AS total_duration_sq,
    MAX(rs.max_duration) AS max_duration
FROM sys.query_store_runtime_stats AS rs
INNER JOIN sys.query_store_runtime_stats_interval AS rsi
    ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
INNER JOIN sys.query_store_plan AS p
    ON p.plan_id = rs.plan_id
WHERE p.query_id = ?
  AND (
        rsi.start_time >= @applied_at
        OR (
            rsi.end_time <= @applied_at
            AND rsi.start_time >= DATEADD(SECOND, ?, @applied_at)
        )
  )
GROUP BY
    CASE WHEN rsi.start_time >= @applied_at THEN 'post' ELSE 'baseline' END,
    rs.plan_id
ORDER BY window_name, rs.plan_id
"""


# Force precheck: a forced plan must have run within MAX_FORCED_PLAN_AGE, and
# a query whose plans return row counts PARAMETER_SENSITIVE_ROW_SPREAD apart
# needs a plan per value range, so no single plan is forced for it.
FORCE_PRECHECK_WINDOW = timedelta(days=7)
MAX_FORCED_PLAN_AGE = timedelta(days=4)
PARAMETER_SENSITIVE_ROW_SPREAD = 10.0
_PRECHECK_PLAN_SQL = """
SELECT
    p.plan_id,
    p.query_id,
    p.plan_type_desc,
    p.force_failure_count,
    p.last_force_failure_reason_desc,
    DATEDIFF(MINUTE, p.last_execution_time, SYSUTCDATETIME()) AS minutes_since_last_execution,
    CASE
        WHEN qt.query_sql_text COLLATE Latin1_General_CI_AS LIKE N'%OPTION%(%RECOMPILE%'
        THEN 1 ELSE 0
    END AS has_option_recompile,
    CASE
        WHEN CAST(p.query_plan AS nvarchar(max)) LIKE N'%Table="[[]#%' THEN 1 ELSE 0
    END AS plan_uses_temp_table,
    (SELECT TOP (1) o.actual_state_desc FROM sys.database_query_store_options AS o)
        AS query_store_state
FROM sys.query_store_plan AS p
INNER JOIN sys.query_store_query AS q
    ON q.query_id = p.query_id
INNER JOIN sys.query_store_query_text AS qt
    ON qt.query_text_id = q.query_text_id
WHERE p.plan_id = ?
"""
_PRECHECK_VARIANT_SQL = """
SELECT qv.parent_query_id, qv.dispatcher_plan_id
FROM sys.query_store_query_variant AS qv
WHERE qv.query_variant_query_id = ?
"""
# One row per plan of the query: Regular runs and rows per execution.
_PRECHECK_PLAN_BUCKETS_SQL = """
SELECT
    rs.plan_id,
    SUM(rs.count_executions) AS executions,
    SUM(rs.avg_rowcount * rs.count_executions)
        / NULLIF(SUM(rs.count_executions), 0) AS avg_rowcount
FROM sys.query_store_runtime_stats AS rs
INNER JOIN sys.query_store_runtime_stats_interval AS rsi
    ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
INNER JOIN sys.query_store_plan AS p
    ON p.plan_id = rs.plan_id
WHERE p.query_id = ?
  AND rs.execution_type = 0
  AND rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
GROUP BY rs.plan_id
"""

# Enforcement guardrails, kept as events in the performance store.
ACTION_COOLDOWN = timedelta(hours=24)
DAILY_ACTION_CAP = 3
FAILURE_COOLDOWN = timedelta(hours=168)
FAILURES_FOR_LONG_COOLDOWN = 2
_GUARDRAIL_AGGREGATE = "plan_action_guardrail"
# The guardrails limit new interventions. Removing one (unforce, clear hints,
# rollback) is never delayed.
_GUARDED_OPERATIONS = frozenset({"force_plan", "set_hints"})
_TIMEOUT_MARKERS = ("HYT00", "HYT01", "QUERY TIMEOUT", "TIMED OUT")


class PlanActionOutcomeUnknownError(RuntimeError):
    """A plan action was dispatched, then its outcome could not be recorded."""


_OUTCOME_UNKNOWN = (
    "plan action was dispatched, then its outcome could not be recorded; "
    "read the intent and the Query Store force state before any retry"
)


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _database_fingerprint(server_name: str, database_name: str) -> str:
    return server_database_identity(
        server_name.strip().casefold(),
        database_name.strip().casefold(),
    )


def _parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else None


def _as_number(value: Any) -> float:
    if isinstance(value, bool) or value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _pooled_stdev(count: float, total: float, total_sq: float | None) -> float | None:
    # Pools per-interval stdevs: variance = E[x^2] - mean^2 over all executions.
    if count <= 0 or total_sq is None:
        return None
    mean = total / count
    return math.sqrt(max(total_sq / count - mean * mean, 0.0))


def _is_timeout(exc: BaseException) -> bool:
    """True when the statement may still be running on the server."""

    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, TimeoutError):
            return True
        text = " ".join(str(argument) for argument in current.args).upper()
        if any(marker in text for marker in _TIMEOUT_MARKERS):
            return True
        current = current.__cause__ or current.__context__
    return False


def evaluate_force_precheck(
    plan_row: Mapping[str, Any] | None,
    variant_rows: Sequence[Mapping[str, Any]] | None,
    bucket_rows: Sequence[Mapping[str, Any]],
    *,
    query_id: int,
    plan_id: int,
) -> dict[str, Any]:
    """Grade a force_plan target. A blocker rejects the intent; a warning does not.

    ``variant_rows`` is None when the variant catalog could not be read.
    """

    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    facts: dict[str, Any] = {"query_id": query_id, "plan_id": plan_id}
    if plan_row is None:
        blockers.append(
            {
                "code": "target_plan_not_found",
                "message": f"Plan {plan_id} is not in Query Store, so it cannot be forced.",
            }
        )
    elif int(_as_number(plan_row.get("query_id"))) != query_id:
        owner = int(_as_number(plan_row.get("query_id")))
        blockers.append(
            {
                "code": "plan_belongs_to_another_query",
                "message": (
                    f"Plan {plan_id} belongs to query {owner}, not query {query_id}; the "
                    "engine refuses this pair."
                ),
                "owning_query_id": owner,
            }
        )
    else:
        minutes = plan_row.get("minutes_since_last_execution")
        facts["minutes_since_last_execution"] = minutes
        facts["plan_type_desc"] = plan_row.get("plan_type_desc")
        max_minutes = MAX_FORCED_PLAN_AGE.total_seconds() / 60
        if minutes is None or _as_number(minutes) > max_minutes:
            age = "has no recorded execution" if minutes is None else (
                f"last ran {_as_number(minutes) / 1440:.1f} days ago"
            )
            blockers.append(
                {
                    "code": "plan_not_run_recently",
                    "message": (
                        f"Plan {plan_id} {age}. A forced plan must have run in the last "
                        f"{MAX_FORCED_PLAN_AGE.days} days: an older plan is not proven "
                        "against the current data and schema."
                    ),
                }
            )
        if plan_row.get("has_option_recompile"):
            warnings.append(
                {
                    "code": "option_recompile",
                    "message": (
                        "The statement has OPTION (RECOMPILE): it compiles on every run, "
                        "so a plan problem here is a per-value problem. Check that one "
                        "forced plan fits every value."
                    ),
                }
            )
        if plan_row.get("plan_uses_temp_table"):
            warnings.append(
                {
                    "code": "temp_table_in_plan",
                    "message": (
                        "The plan reads a #temp table. Its schema and statistics change "
                        "per session, so forcing often fails; watch force_failure_count."
                    ),
                }
            )
        if str(plan_row.get("query_store_state") or "").upper() == "READ_ONLY":
            warnings.append(
                {
                    "code": "query_store_read_only",
                    "message": (
                        "Query Store is READ_ONLY: it records no new runtime statistics, "
                        "so the action cannot be verified until it is READ_WRITE again."
                    ),
                }
            )
        failures = int(_as_number(plan_row.get("force_failure_count")))
        if failures > 0:
            warnings.append(
                {
                    "code": "prior_force_failures",
                    "message": (
                        f"Forcing this plan failed {failures} time(s) before (last reason: "
                        f"{plan_row.get('last_force_failure_reason_desc') or 'unknown'})."
                    ),
                    "force_failure_count": failures,
                }
            )
    if variant_rows is None:
        warnings.append(
            {
                "code": "variant_lookup_unavailable",
                "message": (
                    "sys.query_store_query_variant could not be read, so it is not known "
                    "whether this query is a parameter-sensitive plan variant."
                ),
            }
        )
    elif variant_rows:
        parent = variant_rows[0].get("parent_query_id")
        warnings.append(
            {
                "code": "psp_variant",
                "message": (
                    f"Query {query_id} is a parameter-sensitive plan variant of query "
                    f"{parent}. A forced variant gets no executions once the parent "
                    "statement dispatches to another variant; review the parent first."
                ),
                "parent_query_id": parent,
                "dispatcher_plan_id": variant_rows[0].get("dispatcher_plan_id"),
            }
        )
    rows_per_plan = [
        _as_number(row.get("avg_rowcount"))
        for row in bucket_rows
        if _as_number(row.get("executions")) > 0
    ]
    facts["plans_in_window"] = len(rows_per_plan)
    if len(rows_per_plan) > 1:
        # Under one row per execution the spread is noise.
        spread = max(max(rows_per_plan), 1.0) / max(min(rows_per_plan), 1.0)
        facts["rows_per_execution_spread"] = round(spread, 1)
        if spread >= PARAMETER_SENSITIVE_ROW_SPREAD:
            blockers.append(
                {
                    "code": "parameter_sensitive_query",
                    "message": (
                        f"The query ran {len(rows_per_plan)} plans whose rows per execution "
                        f"differ {spread:.0f}x: it is parameter-sensitive, and one forced "
                        "plan is wrong for part of its values. Use a Query Store hint or "
                        "fix the statement instead of forcing a plan."
                    ),
                }
            )
    return {
        "status": "block" if blockers else "warn" if warnings else "pass",
        "blockers": blockers,
        "warnings": warnings,
        "facts": facts,
    }


def evaluate_guardrails(
    events: Sequence[Mapping[str, Any]],
    *,
    query_id: int,
    now: datetime,
) -> dict[str, Any]:
    """Decide whether one more plan action may be applied on this database.

    An action is a dispatched apply attempt, unless that attempt failed with
    the state unchanged; such an attempt is not a failed action either.
    Limits: one action per query per ACTION_COOLDOWN, DAILY_ACTION_CAP actions
    per database per 24 hours, and FAILURE_COOLDOWN for a query after
    FAILURES_FOR_LONG_COOLDOWN failed actions (rolled back after a regression,
    or forcing failed after apply).
    """

    unchanged: set[tuple[Any, Any]] = set()
    failures: list[datetime] = []
    dispatched: list[tuple[datetime, Any, tuple[Any, Any]]] = []
    for event in events:
        payload = event.get("payload")
        if not isinstance(payload, Mapping):
            continue
        at = _parse_utc(payload.get("at_utc"))
        if at is None:
            continue
        attempt = (payload.get("intent_id"), payload.get("attempt"))
        if event.get("event_type") == "plan_action.failed":
            if payload.get("state_changed") is False:
                unchanged.add(attempt)
            elif payload.get("query_id") == query_id:
                failures.append(at)
        elif event.get("event_type") == "plan_action.dispatched":
            dispatched.append((at, payload.get("query_id"), attempt))
    actions = [(at, query) for at, query, attempt in dispatched if attempt not in unchanged]
    day_ago = now - timedelta(hours=24)
    recent = sorted(at for at, _query in actions if at > day_ago)
    recent_failures = sorted(at for at in failures if at > now - FAILURE_COOLDOWN)
    query_actions = [at for at, query in actions if query == query_id]
    result: dict[str, Any] = {
        "allowed": True,
        "actions_last_24h": len(recent),
        "daily_action_cap": DAILY_ACTION_CAP,
        "failed_actions": len(recent_failures),
    }
    blocked: tuple[str, datetime, str] | None = None
    if len(recent_failures) >= FAILURES_FOR_LONG_COOLDOWN:
        blocked = (
            "failure_cooldown",
            recent_failures[-1] + FAILURE_COOLDOWN,
            f"{len(recent_failures)} plan actions failed for query {query_id} in the last "
            f"{int(FAILURE_COOLDOWN.total_seconds() // 3600)} hours.",
        )
    elif query_actions and max(query_actions) + ACTION_COOLDOWN > now:
        blocked = (
            "query_cooldown",
            max(query_actions) + ACTION_COOLDOWN,
            f"A plan action was applied to query {query_id} in the last "
            f"{int(ACTION_COOLDOWN.total_seconds() // 3600)} hours.",
        )
    elif len(recent) >= DAILY_ACTION_CAP:
        blocked = (
            "daily_action_cap",
            recent[-DAILY_ACTION_CAP] + timedelta(hours=24),
            f"{len(recent)} plan actions were applied to this database in the last 24 hours.",
        )
    if blocked is not None:
        code, retry_after, reason = blocked
        result.update(
            allowed=False,
            blocked_by=code,
            reason=reason,
            retry_after_utc=retry_after.isoformat(),
        )
    return result


def _first_rows(results: Sequence[QueryResult]) -> list[dict[str, Any]]:
    for result in results:
        if result.columns:
            return result.rows
    return []


class PlanActionService:
    """Prepare, apply, verify, and exactly restore one reviewed plan action."""

    def __init__(
        self,
        *,
        config: ServerConfig,
        executor: AzureSqlExecutor,
        admin_policy: AdminPolicy,
        database_policy: DatabasePolicySet,
        store: PerformanceStore,
        intent_ttl: timedelta = DEFAULT_INTENT_TTL,
    ) -> None:
        if intent_ttl <= timedelta(0):
            raise ValueError("intent_ttl must be positive.")
        self.config = config
        self.executor = executor
        self.admin_policy = admin_policy
        self.database_policy = database_policy
        self.store = store
        self.intent_ttl = intent_ttl

    async def prepare(
        self,
        database_name: str,
        *,
        session_id: str,
        candidate_id: str | None,
        operation: str,
        query_id: int,
        plan_id: int | None,
        query_hints: str | None,
        evidence: Mapping[str, Any],
        reviewed_by: str,
        reason: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self._require_review_profile()
        self.store.get_session(session_id)
        if candidate_id is not None:
            candidate = self.store.get_candidate(candidate_id)
            if candidate.session_id != session_id:
                raise ValueError("Candidate does not belong to the tuning session.")
        operation = self._validate_operation(operation, plan_id, query_hints)
        if query_id <= 0:
            raise ValueError("query_id must be greater than 0.")
        if not evidence:
            raise ValueError("Reviewed baseline evidence is required.")
        if not reviewed_by.strip() or not reason.strip() or not idempotency_key.strip():
            raise ValueError("reviewed_by, reason, and idempotency_key are required.")

        normalized_hints = (
            validate_query_hints(query_hints or "")
            if operation == "set_hints"
            else None
        )
        prior_state = await self._capture_control_state(database_name, query_id)
        ownership = str(prior_state["ownership"])
        precheck = (
            await self._force_precheck(database_name, query_id, int(plan_id or 0))
            if operation == "force_plan"
            else None
        )
        baseline_evidence = redact_metadata(evidence)
        evidence_hash = hash_evidence(baseline_evidence)
        database_fingerprint = _database_fingerprint(self.config.server, database_name)
        intent_id = "intent-" + _hash_text(
            f"{database_fingerprint}:{idempotency_key}"
        )[:32]
        rejection_reason = None
        if ownership != "manual":
            rejection_reason = "automatic or unknown ownership is review-only"
        elif precheck is not None and precheck["blockers"]:
            rejection_reason = "force precheck: " + precheck["blockers"][0]["message"]
        status = "prepared" if rejection_reason is None else "rejected"
        created_at = utc_now()
        expires_at = (datetime.fromisoformat(created_at) + self.intent_ttl).isoformat()
        intent = PlanActionIntentV1(
            intent_id=intent_id,
            session_id=session_id,
            candidate_id=candidate_id,
            query_fingerprint=_hash_text(f"query-store:{query_id}"),
            action=operation,
            status=status,
            created_at_utc=created_at,
            updated_at_utc=created_at,
            metadata={
                "expires_at_utc": expires_at,
                "database_fingerprint": database_fingerprint,
                "query_id": query_id,
                "plan_id": plan_id,
                "query_hints": normalized_hints,
                "prior_state": prior_state,
                "ownership": ownership,
                "evidence_hash": evidence_hash,
                "baseline_evidence": baseline_evidence,
                "reviewed_by": reviewed_by,
                "reason": reason,
                "idempotency_fingerprint": _hash_text(idempotency_key),
                "rejection_reason": rejection_reason,
                **({"force_precheck": precheck} if precheck is not None else {}),
            },
        )
        intent = self.store.create_plan_action_intent(
            intent,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint(
                "plan-action.prepare",
                {
                    "database_fingerprint": database_fingerprint,
                    "session_id": session_id,
                    "candidate_id": candidate_id,
                    "operation": operation,
                    "query_id": query_id,
                    "plan_id": plan_id,
                    "query_hints": normalized_hints,
                    "evidence_hash": evidence_hash,
                    "reviewed_by": reviewed_by,
                    "reason": reason,
                },
            ),
        )
        result = {
            "intent": intent.to_dict(),
            "prepared": intent.status == "prepared",
            "expires_at_utc": intent.metadata.get("expires_at_utc"),
            "apply_gates": self._gate_status(database_name),
            # What apply would decide now for force_plan and set_hints; apply
            # checks again. unforce_plan and clear_hints are never delayed.
            "guardrails": self._guardrails(database_fingerprint, query_id),
            "raw_sql_persisted": False,
        }
        stored_precheck = intent.metadata.get("force_precheck")
        if stored_precheck is not None:
            result["force_precheck"] = stored_precheck
        return result

    async def apply(
        self,
        database_name: str,
        intent_id: str,
        *,
        authorization_reference: str,
    ) -> dict[str, Any]:
        self._require_apply_gates(database_name, authorization_reference)
        intent = self._validated_intent(database_name, intent_id)
        if intent.status in {"applied", "observing", "kept", "rolled_back", "hold"}:
            return {"intent": intent.to_dict(), "idempotent": True}
        if intent.status in {"applying", "rolling_back", "unknown"}:
            return {
                "intent": intent.to_dict(),
                "confirmed": False,
                "idempotent": True,
                "reconciliation_required": True,
            }
        if intent.status != "prepared":
            raise PermissionError(f"Only a prepared intent may be applied; status={intent.status}.")
        if self._intent_expired(intent):
            updated = self._save_status(
                intent,
                "hold",
                {
                    "apply_confirmation": "intent_expired",
                    "apply_refused_at_utc": utc_now(),
                    "hold_reason": "Prepared intent expired; prepare it again from fresh evidence.",
                },
            )
            return {"intent": updated.to_dict(), "confirmed": False, "expired": True}
        database_fingerprint = str(intent.metadata.get("database_fingerprint") or "")
        guarded = intent.action in _GUARDED_OPERATIONS
        guardrail = self._guardrails(database_fingerprint, int(intent.metadata["query_id"]))
        if guarded and not guardrail["allowed"]:
            # Nothing is read or changed, and the intent stays prepared.
            return {"intent": intent.to_dict(), "confirmed": False, "guardrail": guardrail}
        try:
            intent = self._save_status(
                intent,
                "applying",
                {
                    "apply_started_at_utc": utc_now(),
                    "authorization_fingerprint": _hash_text(authorization_reference),
                },
            )
        except ConcurrencyError:
            latest = self.store.get_plan_action_intent(intent_id)
            return {
                "intent": latest.to_dict(),
                "confirmed": False,
                "idempotent": True,
                "reconciliation_required": True,
            }
        metadata = dict(intent.metadata)
        query_id = int(metadata["query_id"])
        try:
            current_state = await self._capture_control_state(database_name, query_id)
        except asyncio.CancelledError:
            self._save_status(
                intent,
                "unknown",
                {"apply_confirmation": "cancelled_during_precondition_check"},
            )
            raise
        except Exception as exc:
            updated = self._save_status(
                intent,
                "unknown",
                {
                    "apply_confirmation": "precondition_check_failed",
                    "apply_error_type": type(exc).__name__,
                },
            )
            return {"intent": updated.to_dict(), "confirmed": False}
        prior_state = metadata.get("prior_state")
        if not isinstance(prior_state, Mapping) or not self._control_state_matches(
            current_state,
            prior_state,
        ):
            updated = self._save_status(
                intent,
                "hold",
                {
                    "apply_confirmation": "precondition_changed",
                    "observed_control_state": current_state,
                },
            )
            return {"intent": updated.to_dict(), "confirmed": False}
        if guarded:
            # Recorded before dispatch, so an interrupted apply still counts.
            self._record_guardrail_event("dispatched", intent, per_attempt=True)
        try:
            await self._execute_action(
                database_name,
                intent,
                current_state=current_state,
            )
        except asyncio.CancelledError:
            self._save_status(
                intent,
                "unknown",
                {
                    "apply_confirmation": "cancelled_during_apply",
                    "authorization_fingerprint": _hash_text(authorization_reference),
                },
            )
            raise
        except Exception as exc:
            failed = await self._apply_failed_without_change(
                database_name, intent, prior_state, exc
            )
            if failed is not None:
                return failed
            return self._unknown_after_dispatch(
                intent,
                {
                    "apply_confirmation": "uncertain_after_error",
                    "apply_error_type": type(exc).__name__,
                    "authorization_fingerprint": _hash_text(authorization_reference),
                },
            )
        try:
            expected_state = self._expected_applied_state(intent, current_state)
            confirmed_state = await self._capture_control_state(database_name, query_id)
        except Exception as exc:
            return self._unknown_after_dispatch(
                intent,
                {
                    "apply_confirmation": "confirm_read_failed",
                    "apply_error_type": type(exc).__name__,
                    "authorization_fingerprint": _hash_text(authorization_reference),
                },
            )
        try:
            if not self._control_state_matches(confirmed_state, expected_state):
                return {
                    "intent": self._save_status(
                        intent,
                        "unknown",
                        {
                            "apply_confirmation": "state_mismatch",
                            "authorization_fingerprint": _hash_text(authorization_reference),
                        },
                    ).to_dict(),
                    "confirmed": False,
                }
            updated = self._save_status(
                intent,
                "applied",
                {
                    "applied_at_utc": utc_now(),
                    "authorization_fingerprint": _hash_text(authorization_reference),
                    "applied_state": confirmed_state,
                },
            )
        except Exception as exc:
            # The action was sent: a retry could force twice or undo someone else.
            raise PlanActionOutcomeUnknownError(_OUTCOME_UNKNOWN) from exc
        return {"intent": updated.to_dict(), "confirmed": True, "idempotent": False}

    async def verify(
        self,
        database_name: str,
        intent_id: str,
        *,
        candidate_evidence: Mapping[str, Any] | None = None,
        authorization_reference: str,
    ) -> dict[str, Any]:
        """Decide keep/rollback/hold from Query Store evidence the MCP reads.

        ``candidate_evidence`` from the caller is context only: it can turn a
        keep into a hold, but it can never produce a keep on its own.
        """
        intent = self._validated_intent(database_name, intent_id)
        if intent.status not in {"applied", "observing", "hold"}:
            raise ValueError(f"Intent status={intent.status} cannot be verified.")
        metadata = dict(intent.metadata)
        baseline = metadata.get("baseline_evidence")
        if not isinstance(baseline, Mapping):
            raise ValueError("Prepared intent has no baseline evidence.")
        prior_state = metadata.get("prior_state")
        if not isinstance(prior_state, Mapping):
            raise ValueError("Prepared intent has no exact prior state.")
        current_state = await self._capture_control_state(
            database_name,
            int(metadata["query_id"]),
        )
        expected_state = self._expected_applied_state(intent, prior_state)
        if not self._control_state_matches(current_state, expected_state):
            updated = self._save_status(
                intent,
                "hold",
                {
                    "verification": {
                        "action": "hold",
                        "reason": (
                            "Query Store control state or ownership changed after apply; "
                            "manual review is required."
                        ),
                    },
                    "observed_control_state": current_state,
                },
            )
            return {"intent": updated.to_dict(), "decision": "hold"}
        query_id = int(metadata["query_id"])
        forced_plan_id = expected_state.get("force_plan_id")
        applied_state = metadata.get("applied_state")
        failures_at_apply = int(
            _as_number(
                applied_state.get("force_failure_count")
                if isinstance(applied_state, Mapping)
                else None
            )
        )
        failures_now = int(_as_number(current_state.get("force_failure_count")))
        applied_at = _parse_utc(metadata.get("applied_at_utc"))
        policy = self.database_policy.require(database_name)
        provenance = {
            "environment": policy.environment,
            "database_name": database_name,
            "query_id": query_id,
        }
        evidence: dict[str, Any] = {
            "source": "mcp_query_store",
            "caller_evidence_used_for_keep": False,
            "forced_plan_id": forced_plan_id,
            "force_failure_count": failures_now,
            "force_failure_count_at_apply": failures_at_apply,
            "last_force_failure_reason_desc": current_state.get(
                "last_force_failure_reason_desc"
            ),
        }
        hold_reason: str | None = None
        if forced_plan_id is not None and failures_now > failures_at_apply:
            hold_reason = (
                f"Plan forcing failed {failures_now - failures_at_apply} time(s) after "
                "apply (last reason: "
                f"{current_state.get('last_force_failure_reason_desc') or 'unknown'})."
            )
        elif applied_at is None:
            hold_reason = "Intent has no valid applied_at_utc; post-apply evidence cannot be read."
        if hold_reason is not None:
            if forced_plan_id is not None and failures_now > failures_at_apply:
                self._record_guardrail_event(
                    "failed", intent, per_attempt=False, failure="force_failed"
                )
            return self._verification_hold(intent, hold_reason, evidence)
        assert applied_at is not None
        mcp_baseline, mcp_candidate, collected = await self._collect_runtime_evidence(
            database_name,
            query_id,
            applied_at,
            forced_plan_id,
            provenance,
        )
        evidence.update(collected)
        share = evidence.get("forced_plan_execution_share")
        if isinstance(share, float) and share <= 0.5:
            return self._verification_hold(
                intent,
                (
                    f"The forced plan ran {share:.0%} of executions since apply; "
                    "most executions must use the forced plan_id."
                ),
                evidence,
            )
        decision = decide_verification(
            mcp_baseline,
            mcp_candidate,
            expected_provenance=provenance,
            require_parameter_buckets=False,
        )
        if decision.action == "keep" and candidate_evidence:
            context = decide_verification(
                baseline,
                candidate_evidence,
                expected_provenance=provenance,
            )
            evidence["caller_context_decision"] = context.action
            if context.action == "rollback":
                decision = replace(
                    decision,
                    action="hold",
                    reason=(
                        "Query Store evidence improved, but caller evidence shows a "
                        f"regression ({context.reason}); manual review is required."
                    ),
                    regressed_metrics=context.regressed_metrics,
                )
        plan_count = evidence.get("baseline_plan_count")
        if decision.action == "keep" and isinstance(plan_count, int) and plan_count > 1:
            decision = replace(
                decision,
                action="hold",
                reason=(
                    f"The baseline window ran {plan_count} plans, so this query is "
                    "likely parameter-sensitive, and Query Store has no per-parameter "
                    "runtime stats: the blended improvement can hide a regression for "
                    "a minority parameter range. Verify each parameter bucket "
                    "(get_query_parameter_buckets, or a benchmark with representative "
                    "parameters) before deciding to keep."
                ),
            )
        new_plans = evidence.get("post_plans_without_baseline")
        if decision.action == "keep" and new_plans:
            # The tail baseline then comes from other plans, whose outliers
            # can hide a slower minority of executions on the new plan.
            decision = replace(
                decision,
                action="hold",
                reason=(
                    f"Plan(s) {', '.join(str(p) for p in new_plans)} did not run in the "
                    "baseline window, so their max and spread cannot be compared with "
                    "their own baseline, and Query Store has no per-parameter runtime "
                    "stats. Verify each parameter bucket (get_query_parameter_buckets, "
                    "or a benchmark with representative parameters) before deciding "
                    "to keep."
                ),
            )
        if decision.action == "hold":
            return self._verification_hold(
                intent, decision.reason, evidence, decision.regressed_metrics
            )
        if decision.action == "keep":
            updated = self._save_status(
                intent,
                "kept",
                {
                    "verification": {
                        "action": decision.action,
                        "reason": decision.reason,
                        "improvement_pct": decision.improvement_pct,
                        "evidence": evidence,
                    }
                },
            )
            return {"intent": updated.to_dict(), "decision": "keep", "evidence": evidence}
        self._record_guardrail_event(
            "failed", intent, per_attempt=False, failure="rolled_back_after_regression"
        )
        rollback = await self.rollback(
            database_name,
            intent_id,
            authorization_reference=authorization_reference,
            reason=decision.reason,
            verification={
                "action": "rollback",
                "reason": decision.reason,
                "improvement_pct": decision.improvement_pct,
                "regressed_metrics": list(decision.regressed_metrics),
                "evidence": evidence,
            },
        )
        rollback["decision"] = "rollback"
        rollback["evidence"] = evidence
        rollback["regressed_metrics"] = list(decision.regressed_metrics)
        return rollback

    def _verification_hold(
        self,
        intent: PlanActionIntentV1,
        reason: str,
        evidence: Mapping[str, Any],
        regressed_metrics: Sequence[str] = (),
    ) -> dict[str, Any]:
        updated = self._save_status(
            intent,
            "hold",
            {
                "verification": {
                    "action": "hold",
                    "reason": reason,
                    "regressed_metrics": list(regressed_metrics),
                    "evidence": dict(evidence),
                }
            },
        )
        return {"intent": updated.to_dict(), "decision": "hold", "evidence": dict(evidence)}

    async def _collect_runtime_evidence(
        self,
        database_name: str,
        query_id: int,
        applied_at: datetime,
        forced_plan_id: Any,
        provenance: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        """Read pre/post apply runtime stats for the query from Query Store."""

        now = datetime.now(UTC)
        rows = await self.executor.fetch_all(
            database_name,
            _RUNTIME_EVIDENCE_SQL,
            params=[
                applied_at.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                int(query_id),
                -int(_BASELINE_WINDOW.total_seconds()),
            ],
            max_rows=_MAX_EVIDENCE_ROWS + 1,
        )
        truncated = len(rows) > _MAX_EVIDENCE_ROWS
        rows = rows[:_MAX_EVIDENCE_ROWS]
        per_plan: list[dict[str, Any]] = []
        windows: dict[str, dict[str, Any]] = {
            "baseline": {"count": 0.0, "max_duration": 0.0, "total_duration_sq": 0.0},
            "post": {"count": 0.0, "max_duration": 0.0, "total_duration_sq": 0.0},
        }
        forced_executions = 0.0
        forced_baseline: dict[str, Any] | None = None
        baseline_plans: set[int] = set()
        post_plans: set[int] = set()
        for row in rows:
            window = str(row.get("window_name") or "")
            if window not in windows:
                continue
            count = _as_number(row.get("count_executions"))
            totals = windows[window]
            totals["count"] += count
            totals["max_duration"] = max(
                totals["max_duration"], _as_number(row.get("max_duration"))
            )
            total_sq = row.get("total_duration_sq")
            plan_sq = None if total_sq is None else _as_number(total_sq)
            if plan_sq is None or totals["total_duration_sq"] is None:
                totals["total_duration_sq"] = None
            else:
                totals["total_duration_sq"] += plan_sq
            plan_id = (
                int(_as_number(row.get("plan_id"))) if row.get("plan_id") is not None else None
            )
            is_forced = forced_plan_id is not None and plan_id == int(forced_plan_id)
            plan_summary: dict[str, Any] = {
                "window": window,
                "plan_id": row.get("plan_id"),
                "count_executions": int(count),
                "max_duration": _as_number(row.get("max_duration")),
                "stdev_duration": _pooled_stdev(
                    count, _as_number(row.get("total_duration")), plan_sq
                ),
            }
            for metric, total_name in _METRIC_TOTALS:
                total = _as_number(row.get(total_name))
                totals[total_name] = totals.get(total_name, 0.0) + total
                plan_summary[metric] = total / count if count else None
            per_plan.append(plan_summary)
            if window == "baseline" and count > 0 and plan_id is not None:
                baseline_plans.add(plan_id)
                if is_forced:
                    forced_baseline = plan_summary
            if window == "post" and count > 0 and plan_id is not None:
                post_plans.add(plan_id)
            if window == "post" and is_forced:
                forced_executions += count

        def payload(name: str, start: datetime, end: datetime) -> dict[str, Any]:
            totals = windows[name]
            count = totals["count"]
            result: dict[str, Any] = {
                "count_executions": int(count),
                "max_duration": totals["max_duration"] if count else None,
                "stdev_duration": _pooled_stdev(
                    count, totals.get("total_duration", 0.0), totals["total_duration_sq"]
                ),
                "units": {
                    "avg_duration": "microseconds",
                    "avg_cpu_time": "microseconds",
                    "avg_logical_io_reads": "8kb_pages",
                    "max_duration": "microseconds",
                    "stdev_duration": "microseconds",
                },
                "evidence": {
                    **provenance,
                    "source": "query-store",
                    "provenance": "mcp-runtime-stats-v1",
                    "window_start": start.isoformat(),
                    "window_end": end.isoformat(),
                    "post_change": name == "post",
                    "truncated": truncated,
                },
            }
            for metric, total_name in _METRIC_TOTALS:
                result[metric] = totals.get(total_name, 0.0) / count if count else None
            return result

        baseline = payload("baseline", applied_at - _BASELINE_WINDOW, applied_at)
        candidate = payload("post", applied_at, now)
        if forced_baseline is not None:
            # The slow sniffed plan inflates the all-plan baseline tail, so the
            # forced plan's tail is compared with its own pre-apply tail.
            baseline["max_duration"] = forced_baseline["max_duration"]
            baseline["stdev_duration"] = forced_baseline["stdev_duration"]
        post_count = windows["post"]["count"]
        summary_keys = (
            "count_executions",
            *(m for m, _ in _METRIC_TOTALS),
            "max_duration",
            "stdev_duration",
        )
        collected = {
            "baseline_window": {
                "start": baseline["evidence"]["window_start"],
                "end": baseline["evidence"]["window_end"],
            },
            "post_window": {
                "start": candidate["evidence"]["window_start"],
                "end": candidate["evidence"]["window_end"],
            },
            "baseline": {key: baseline[key] for key in summary_keys},
            "post": {key: candidate[key] for key in summary_keys},
            "per_plan": per_plan,
            "forced_plan_execution_share": (
                forced_executions / post_count
                if forced_plan_id is not None and post_count
                else None
            ),
            "truncated": truncated,
            "baseline_plan_count": len(baseline_plans),
            "post_plans_without_baseline": sorted(post_plans - baseline_plans),
            "tail_baseline_basis": "forced_plan" if forced_baseline is not None else "all_plans",
            "parameter_buckets": "unavailable",
            "parameter_buckets_note": (
                "Query Store does not record runtime parameter values per execution, "
                "so no parameter buckets were compared. max_duration and "
                "stdev_duration are checked against the forced plan's own baseline "
                "where it ran, to catch a slower minority of executions."
            ),
        }
        return baseline, candidate, collected

    async def rollback(
        self,
        database_name: str,
        intent_id: str,
        *,
        authorization_reference: str,
        reason: str = "explicit rollback requested",
        verification: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require_apply_gates(database_name, authorization_reference)
        intent = self._validated_intent(database_name, intent_id)
        if intent.status == "rolled_back":
            return {"intent": intent.to_dict(), "confirmed": True, "idempotent": True}
        if intent.status in {"rolling_back", "applying", "unknown"}:
            return {
                "intent": intent.to_dict(),
                "confirmed": False,
                "idempotent": True,
                "reconciliation_required": True,
            }
        if intent.status not in {"applied", "observing", "hold", "kept"}:
            raise ValueError(f"Intent status={intent.status} cannot be rolled back.")
        if not intent.metadata.get("applied_at_utc"):
            # Held before apply (expired or precondition changed): this intent
            # changed nothing, so restoring its prior state would undo someone
            # else's change.
            return {"intent": intent.to_dict(), "confirmed": False, "not_applied": True}
        try:
            intent = self._save_status(
                intent,
                "rolling_back",
                {
                    "rollback_started_at_utc": utc_now(),
                    "rollback_authorization_fingerprint": _hash_text(
                        authorization_reference
                    ),
                    # Kept on every later outcome, so a held or unknown
                    # rollback still says which metrics regressed.
                    **(
                        {"verification": dict(verification)}
                        if verification is not None
                        else {}
                    ),
                },
            )
        except ConcurrencyError:
            latest = self.store.get_plan_action_intent(intent_id)
            return {
                "intent": latest.to_dict(),
                "confirmed": False,
                "idempotent": True,
                "reconciliation_required": True,
            }
        metadata = dict(intent.metadata)
        prior_state = metadata.get("prior_state")
        if not isinstance(prior_state, Mapping):
            raise ValueError("Prepared intent has no exact prior state.")
        query_id = int(metadata["query_id"])
        try:
            current_state = await self._capture_control_state(database_name, query_id)
        except asyncio.CancelledError:
            self._save_status(
                intent,
                "unknown",
                {"rollback_confirmation": "cancelled_during_precondition_check"},
            )
            raise
        except Exception as exc:
            updated = self._save_status(
                intent,
                "unknown",
                {
                    "rollback_confirmation": "precondition_check_failed",
                    "rollback_error_type": type(exc).__name__,
                },
            )
            return {"intent": updated.to_dict(), "confirmed": False}
        if current_state.get("ownership") != "manual":
            updated = self._save_status(
                intent,
                "hold",
                {
                    "rollback_confirmation": "ownership_changed",
                    "rollback_reason": reason,
                    "observed_control_state": current_state,
                },
            )
            return {"intent": updated.to_dict(), "confirmed": False, "decision": "hold"}
        if not self._control_state_matches(
            current_state, self._expected_applied_state(intent, prior_state)
        ):
            updated = self._save_status(
                intent,
                "hold",
                {
                    "rollback_confirmation": "state_changed",
                    "rollback_reason": reason,
                    "observed_control_state": current_state,
                },
            )
            return {"intent": updated.to_dict(), "confirmed": False, "decision": "hold"}
        try:
            await self._restore_prior_state(
                database_name,
                intent,
                query_id,
                current_state,
                prior_state,
            )
        except asyncio.CancelledError:
            self._save_status(
                intent,
                "unknown",
                {
                    "rollback_confirmation": "cancelled_during_rollback",
                    "rollback_authorization_fingerprint": _hash_text(
                        authorization_reference
                    ),
                },
            )
            raise
        except Exception as exc:
            return self._unknown_after_dispatch(
                intent,
                {
                    "rollback_confirmation": "uncertain_after_error",
                    "rollback_error_type": type(exc).__name__,
                    "rollback_authorization_fingerprint": _hash_text(
                        authorization_reference
                    ),
                },
            )
        try:
            confirmed = await self._capture_control_state(database_name, query_id)
        except Exception as exc:
            return self._unknown_after_dispatch(
                intent,
                {
                    "rollback_confirmation": "confirm_read_failed",
                    "rollback_error_type": type(exc).__name__,
                    "rollback_authorization_fingerprint": _hash_text(
                        authorization_reference
                    ),
                },
            )
        if not self._control_state_matches(confirmed, prior_state):
            return self._unknown_after_dispatch(
                intent,
                {"rollback_confirmation": "state_mismatch"},
            )
        try:
            updated = self._save_status(
                intent,
                "rolled_back",
                {
                    "rolled_back_at_utc": utc_now(),
                    "rollback_reason": reason,
                    "rollback_authorization_fingerprint": _hash_text(
                        authorization_reference
                    ),
                    "restored_state": confirmed,
                },
            )
        except Exception as exc:
            # The restore was sent: a retry could undo someone else's change.
            raise PlanActionOutcomeUnknownError(_OUTCOME_UNKNOWN) from exc
        return {"intent": updated.to_dict(), "confirmed": True, "idempotent": False}

    async def _force_precheck(
        self,
        database_name: str,
        query_id: int,
        plan_id: int,
    ) -> dict[str, Any]:
        """Read the force target and grade it; a failed read blocks (fail closed)."""

        try:
            plan_rows = await self.executor.fetch_all(
                database_name, _PRECHECK_PLAN_SQL, params=[plan_id], max_rows=2
            )
            bucket_rows = await self.executor.fetch_all(
                database_name,
                _PRECHECK_PLAN_BUCKETS_SQL,
                params=[query_id, int(FORCE_PRECHECK_WINDOW.total_seconds() // 60)],
                max_rows=_MAX_EVIDENCE_ROWS,
            )
        except Exception as exc:
            note_exception(exc, "plan_action_service.force_precheck")
            return {
                "status": "block",
                "blockers": [
                    {
                        "code": "precheck_unavailable",
                        "message": (
                            "The force precheck could not read Query Store "
                            f"({sanitize_error_message(str(exc))}); prepare again."
                        ),
                    }
                ],
                "warnings": [],
                "facts": {"query_id": query_id, "plan_id": plan_id},
            }
        variant_rows: list[dict[str, Any]] | None
        try:
            variant_rows = await self.executor.fetch_all(
                database_name, _PRECHECK_VARIANT_SQL, params=[query_id], max_rows=2
            )
        except Exception as exc:
            note_exception(exc, "plan_action_service.force_precheck_variant")
            variant_rows = None
        return evaluate_force_precheck(
            plan_rows[0] if plan_rows else None,
            variant_rows,
            bucket_rows,
            query_id=query_id,
            plan_id=plan_id,
        )

    def _guardrails(self, database_fingerprint: str, query_id: int) -> dict[str, Any]:
        events = self.store.list_events(
            aggregate_type=_GUARDRAIL_AGGREGATE,
            aggregate_id=database_fingerprint,
        )
        return evaluate_guardrails(events, query_id=query_id, now=datetime.now(UTC))

    def _record_guardrail_event(
        self,
        kind: str,
        intent: PlanActionIntentV1,
        *,
        per_attempt: bool,
        **payload: Any,
    ) -> None:
        """Append one guardrail event.

        An apply attempt is named by the intent version at dispatch, so a
        refused attempt and a later retry of the same intent stay apart. A
        verification failure is recorded once per intent, however often verify
        is repeated.
        """

        label = str(payload.get("failure") or kind)
        key = f"{intent.intent_id}:guardrail:{label}"
        if per_attempt:
            payload["attempt"] = intent.version
            key += f":{intent.version}"
        self.store.append_event(
            event_type=f"plan_action.{kind}",
            aggregate_type=_GUARDRAIL_AGGREGATE,
            aggregate_id=str(intent.metadata.get("database_fingerprint") or ""),
            payload={
                "intent_id": intent.intent_id,
                "query_id": int(intent.metadata["query_id"]),
                "at_utc": utc_now(),
                **payload,
            },
            idempotency_key=key,
        )

    async def _apply_failed_without_change(
        self,
        database_name: str,
        intent: PlanActionIntentV1,
        prior_state: Mapping[str, Any],
        exc: Exception,
    ) -> dict[str, Any] | None:
        """Return the intent to prepared when a failed apply changed nothing.

        A timeout may still be running on the server, so it stays unknown. For
        any other error the control state is read again: equal to the prior
        state means the engine did not apply the action.
        """

        if _is_timeout(exc):
            return None
        try:
            observed = await self._capture_control_state(
                database_name, int(intent.metadata["query_id"])
            )
        except Exception as read_exc:
            note_exception(read_exc, "plan_action_service.apply_failure_read")
            return None
        if not self._control_state_matches(observed, prior_state):
            return None
        try:
            self._record_guardrail_event(
                "failed", intent, per_attempt=True, failure="apply_failed", state_changed=False
            )
            updated = self._save_status(
                intent,
                "prepared",
                {
                    "apply_confirmation": "failed_no_change",
                    "apply_error_type": type(exc).__name__,
                    "apply_error": sanitize_error_message(str(exc)),
                    "apply_failed_at_utc": utc_now(),
                },
            )
        except Exception as save_exc:
            note_exception(save_exc, "plan_action_service.apply_failure_record")
            return None
        return {
            "intent": updated.to_dict(),
            "confirmed": False,
            "apply_failed": True,
            "reason": (
                "The engine refused the action and the Query Store control state is "
                "unchanged, so nothing was applied. Fix the cause and apply again."
            ),
        }

    def _unknown_after_dispatch(
        self,
        intent: Any,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """The action was sent: record "unknown", or raise if even that fails."""

        try:
            updated = self._save_status(intent, "unknown", metadata)
        except Exception as save_exc:
            raise PlanActionOutcomeUnknownError(_OUTCOME_UNKNOWN) from save_exc
        return {"intent": updated.to_dict(), "confirmed": False}

    async def _capture_control_state(
        self,
        database_name: str,
        query_id: int,
    ) -> dict[str, Any]:
        statements = [
            "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
            "BEGIN TRANSACTION",
            (
                "SELECT plan_id, plan_forcing_type_desc, force_failure_count, "
                "last_force_failure_reason_desc "
                "FROM sys.query_store_plan "
                f"WHERE query_id = {int(query_id)} AND is_forced_plan = 1"
            ),
            (
                "SELECT query_hint_text FROM sys.query_store_query_hints "
                f"WHERE query_id = {int(query_id)}"
            ),
            (
                "SELECT TOP (1) 1 AS automatic_owner "
                "FROM sys.dm_db_tuning_recommendations "
                "WHERE TRY_CONVERT(bigint, "
                f"JSON_VALUE(details, '$.planForceDetails.queryId')) = {int(query_id)} "
                "AND JSON_VALUE(state, '$.currentValue') IN "
                "('Active', 'Verifying', 'Success') "
                "AND (execute_action_initiated_by = 'System' "
                "OR (type = 'FORCE_LAST_GOOD_PLAN' "
                "AND JSON_VALUE(state, '$.currentValue') = 'Active' "
                # Microsoft: automatic tuning is off; apply manually. ISNULL
                # keeps a NULL reason engine-owned.
                "AND ISNULL(JSON_VALUE(state, '$.reason'), '') "
                "<> 'AutomaticTuningOptionNotEnabled'))"
            ),
            "ROLLBACK TRANSACTION",
        ]
        results = await self.executor.execute_session(
            database_name,
            statements,
            max_rows=10,
        )
        forced_rows = _first_rows(results[2]) if len(results) > 2 else []
        hint_rows = _first_rows(results[3]) if len(results) > 3 else []
        automatic_rows = _first_rows(results[4]) if len(results) > 4 else []
        if len(forced_rows) > 1 or len(hint_rows) > 1:
            raise RuntimeError("Query Store returned an ambiguous control state.")
        forcing_type = (
            str(forced_rows[0].get("plan_forcing_type_desc") or "")
            if forced_rows
            else ""
        )
        if automatic_rows or forcing_type.casefold() == "auto":
            ownership = "automatic"
        elif forcing_type and forcing_type.casefold() != "manual":
            ownership = "unknown"
        else:
            ownership = "manual"
        return {
            "force_plan_id": int(forced_rows[0]["plan_id"]) if forced_rows else None,
            "query_store_hints": (
                str(hint_rows[0].get("query_hint_text")) if hint_rows else None
            ),
            "ownership": ownership,
            "captured": True,
            "force_failure_count": (
                int(_as_number(forced_rows[0].get("force_failure_count")))
                if forced_rows
                else None
            ),
            "last_force_failure_reason_desc": (
                forced_rows[0].get("last_force_failure_reason_desc") if forced_rows else None
            ),
        }

    async def _execute_action(
        self,
        database_name: str,
        intent: PlanActionIntentV1,
        *,
        current_state: Mapping[str, Any],
    ) -> None:
        metadata = intent.metadata
        query_id = int(metadata["query_id"])
        plan_id = metadata.get("plan_id")
        hints = metadata.get("query_hints")
        if intent.action == "force_plan":
            if not isinstance(plan_id, int):
                raise ValueError("Prepared force_plan intent has no plan_id.")
            sql = "EXEC sys.sp_query_store_force_plan @query_id = ?, @plan_id = ?"
            params: tuple[Any, ...] = (query_id, plan_id)
        elif intent.action == "unforce_plan":
            if not isinstance(plan_id, int):
                raise ValueError("Prepared unforce_plan intent has no plan_id.")
            sql = "EXEC sys.sp_query_store_unforce_plan @query_id = ?, @plan_id = ?"
            params = (query_id, plan_id)
        elif intent.action == "set_hints":
            sql = (
                "DECLARE @hints nvarchar(max) = ?; "
                "EXEC sys.sp_query_store_set_hints @query_id = ?, @query_hints = @hints"
            )
            params = (str(hints), query_id)
        else:
            sql = "EXEC sys.sp_query_store_clear_hints @query_id = ?"
            params = (query_id,)
        expected_state = self._expected_applied_state(intent, current_state)
        rollback_sql, rollback_params = self._state_restore_batch(
            query_id,
            expected_state,
            current_state,
        )
        policy = self.database_policy.require(database_name)
        await self.admin_policy.execute(
            AdminAction(
                tool_name="apply_prepared_plan_action",
                database_name=database_name,
                action_type="query_store",
                sql=sql,
                params=params,
                rollback_sql=rollback_sql,
                rollback_params=rollback_params,
                trusted_generated=True,
                reviewed_intent=True,
                idempotency_key=f"{intent.intent_id}:apply",
                exactly_once=True,
                policy_verified=True,
                non_production=policy.is_non_production,
                verification_required=True,
            ),
            self.executor,
            dry_run=False,
        )

    async def _restore_prior_state(
        self,
        database_name: str,
        intent: PlanActionIntentV1,
        query_id: int,
        current_state: Mapping[str, Any],
        prior_state: Mapping[str, Any],
    ) -> None:
        sql, params = self._state_restore_batch(
            query_id,
            current_state,
            prior_state,
        )
        rollback_sql, rollback_params = self._state_restore_batch(
            query_id,
            prior_state,
            current_state,
        )
        policy = self.database_policy.require(database_name)
        await self.admin_policy.execute(
            AdminAction(
                tool_name="rollback_plan_action",
                database_name=database_name,
                action_type="query_store",
                sql=sql,
                params=params,
                rollback_sql=rollback_sql,
                rollback_params=rollback_params,
                trusted_generated=True,
                reviewed_intent=True,
                idempotency_key=f"{intent.intent_id}:rollback:{intent.version}",
                exactly_once=True,
                policy_verified=True,
                non_production=policy.is_non_production,
                verification_required=True,
            ),
            self.executor,
            dry_run=False,
        )

    @staticmethod
    def _state_restore_batch(
        query_id: int,
        current_state: Mapping[str, Any],
        target_state: Mapping[str, Any],
    ) -> tuple[str, tuple[Any, ...]]:
        current_plan_id = current_state.get("force_plan_id")
        target_plan_id = target_state.get("force_plan_id")
        target_hints = target_state.get("query_store_hints")
        statements: list[str] = []
        params: list[Any] = []
        if current_plan_id is not None:
            statements.append(
                "EXEC sys.sp_query_store_unforce_plan @query_id = ?, @plan_id = ?"
            )
            params.extend((query_id, int(current_plan_id)))
        statements.append("EXEC sys.sp_query_store_clear_hints @query_id = ?")
        params.append(query_id)
        if target_hints is not None:
            statements.append(
                "DECLARE @hints nvarchar(max) = ?; "
                "EXEC sys.sp_query_store_set_hints @query_id = ?, @query_hints = @hints"
            )
            params.extend((str(target_hints), query_id))
        if target_plan_id is not None:
            statements.append(
                "EXEC sys.sp_query_store_force_plan @query_id = ?, @plan_id = ?"
            )
            params.extend((query_id, int(target_plan_id)))
        return "; ".join(statements), tuple(params)

    def _expected_applied_state(
        self,
        intent: PlanActionIntentV1,
        prior_state: Mapping[str, Any],
    ) -> dict[str, Any]:
        expected = dict(prior_state)
        if intent.action == "force_plan":
            expected["force_plan_id"] = int(intent.metadata["plan_id"])
        elif intent.action == "unforce_plan":
            expected["force_plan_id"] = None
        elif intent.action == "set_hints":
            expected["query_store_hints"] = str(intent.metadata["query_hints"])
        elif intent.action == "clear_hints":
            expected["query_store_hints"] = None
        expected["ownership"] = "manual"
        return expected

    @staticmethod
    def _control_state_matches(
        actual: Mapping[str, Any],
        expected: Mapping[str, Any],
    ) -> bool:
        return all(
            actual.get(name) == expected.get(name)
            for name in ("force_plan_id", "query_store_hints", "ownership", "captured")
        )

    def _validated_intent(
        self,
        database_name: str,
        intent_id: str,
    ) -> PlanActionIntentV1:
        intent = self.store.get_plan_action_intent(intent_id)
        stored_database_fingerprint = str(
            intent.metadata.get("database_fingerprint") or ""
        )
        current_database_fingerprint = _database_fingerprint(
            self.config.server,
            database_name,
        )
        allow_legacy_migration = bool(self.config.legacy_state_server_binding)
        if not server_database_identity_matches(
            stored_database_fingerprint,
            self.config.server.strip().casefold(),
            database_name.strip().casefold(),
            allow_legacy=allow_legacy_migration,
        ):
            raise PermissionError("Intent database fingerprint does not match.")
        if (
            allow_legacy_migration
            and stored_database_fingerprint != current_database_fingerprint
        ):
            migrated = replace(
                intent,
                metadata={
                    **intent.metadata,
                    "database_fingerprint": current_database_fingerprint,
                    "identity_migration": "legacy-database-to-server-bound-v1",
                },
                updated_at_utc=utc_now(),
                version=intent.version + 1,
            )
            try:
                intent = self.store.save_plan_action_intent(
                    migrated,
                    expected_version=intent.version,
                )
            except ConcurrencyError:
                intent = self.store.get_plan_action_intent(intent_id)
                if not server_database_identity_matches(
                    str(intent.metadata.get("database_fingerprint") or ""),
                    self.config.server.strip().casefold(),
                    database_name.strip().casefold(),
                ):
                    raise
        baseline_evidence = intent.metadata.get("baseline_evidence")
        evidence_hash = intent.metadata.get("evidence_hash")
        if (
            not isinstance(baseline_evidence, Mapping)
            or not isinstance(evidence_hash, str)
            or hash_evidence(baseline_evidence) != evidence_hash
        ):
            raise RuntimeError("Prepared intent evidence hash does not match durable evidence.")
        return intent

    def _intent_expired(self, intent: PlanActionIntentV1) -> bool:
        # Intents prepared before expiry existed fall back to created + TTL.
        expires_at = _parse_utc(intent.metadata.get("expires_at_utc"))
        if expires_at is None:
            created_at = _parse_utc(intent.created_at_utc)
            if created_at is None:
                return True
            expires_at = created_at + self.intent_ttl
        return datetime.now(UTC) >= expires_at

    def _save_status(
        self,
        intent: PlanActionIntentV1,
        status: str,
        metadata: Mapping[str, Any],
    ) -> PlanActionIntentV1:
        merged = dict(intent.metadata)
        merged.update(metadata)
        updated = replace(
            intent,
            status=status,
            metadata=merged,
            updated_at_utc=utc_now(),
            version=intent.version + 1,
        )
        return self.store.save_plan_action_intent(
            updated,
            expected_version=intent.version,
        )

    def _require_review_profile(self) -> None:
        if self.config.profile not in {
            McpProfile.ENFORCER_REVIEW,
            McpProfile.ENFORCER_APPLY,
        }:
            raise PermissionError(
                "Preparing plan actions requires the enforcer-review or enforcer-apply profile."
            )

    def _require_apply_gates(
        self,
        database_name: str,
        authorization_reference: str,
    ) -> None:
        if self.config.profile != McpProfile.ENFORCER_APPLY:
            raise PermissionError("Applying plan actions requires the enforcer-apply profile.")
        if self.config.access_mode != AccessMode.UNRESTRICTED:
            raise PermissionError("Plan apply requires unrestricted access mode.")
        if self.config.write_policy != WritePolicy.APPLY:
            raise PermissionError("Server write policy does not permit plan apply.")
        if self.config.plan_apply_kill_switch:
            raise PermissionError("Plan apply kill switch is engaged.")
        if not self.database_policy.allows_plan_apply(database_name):
            raise PermissionError("Database policy does not permit plan apply.")
        if not authorization_reference.strip():
            raise PermissionError("Explicit authorization_reference is required.")

    def _gate_status(self, database_name: str) -> dict[str, bool]:
        return {
            "server_apply_policy": self.config.write_policy == WritePolicy.APPLY,
            "database_apply_policy": self.database_policy.allows_plan_apply(database_name),
            "kill_switch_approved": not self.config.plan_apply_kill_switch,
            "apply_profile": self.config.profile == McpProfile.ENFORCER_APPLY,
        }

    @staticmethod
    def _validate_operation(
        operation: str,
        plan_id: int | None,
        query_hints: str | None,
    ) -> str:
        normalized = operation.strip().casefold()
        if normalized not in _OPERATIONS:
            raise ValueError(f"operation must be one of {sorted(_OPERATIONS)}.")
        if normalized in {"force_plan", "unforce_plan"}:
            if plan_id is None or plan_id <= 0:
                raise ValueError(f"{normalized} requires a positive plan_id.")
            if query_hints is not None:
                raise ValueError(f"{normalized} cannot include query_hints.")
        elif normalized == "set_hints":
            if plan_id is not None:
                raise ValueError("set_hints cannot include plan_id.")
            validate_query_hints(query_hints or "")
        elif plan_id is not None or query_hints is not None:
            raise ValueError("clear_hints cannot include plan_id or query_hints.")
        return normalized


__all__ = [
    "ACTION_COOLDOWN",
    "DAILY_ACTION_CAP",
    "DEFAULT_INTENT_TTL",
    "FAILURE_COOLDOWN",
    "PlanActionService",
    "evaluate_force_precheck",
    "evaluate_guardrails",
]
