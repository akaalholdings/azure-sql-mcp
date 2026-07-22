"""Durable prepared Query Store actions with exact-state restoration."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from typing import Any, Mapping, Sequence

from .admin_policy import AdminAction, AdminPolicy
from .config import AccessMode, McpProfile, ServerConfig, WritePolicy
from .connection import AzureSqlExecutor, QueryResult
from .database_policy import DatabasePolicySet
from .performance_contracts import PlanActionIntentV1, redact_metadata, utc_now
from .performance_store import ConcurrencyError, PerformanceStore
from .plan_verification import decide_verification, hash_evidence
from .query_hints import validate_query_hints


_OPERATIONS = {"force_plan", "unforce_plan", "set_hints", "clear_hints"}


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _database_fingerprint(database_name: str) -> str:
    return _hash_text(f"database:{database_name.casefold()}")


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
    ) -> None:
        self.config = config
        self.executor = executor
        self.admin_policy = admin_policy
        self.database_policy = database_policy
        self.store = store

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
        baseline_evidence = redact_metadata(evidence)
        evidence_hash = hash_evidence(baseline_evidence)
        intent_id = "intent-" + _hash_text(
            f"{_database_fingerprint(database_name)}:{idempotency_key}"
        )[:32]
        status = "prepared" if ownership == "manual" else "rejected"
        intent = PlanActionIntentV1(
            intent_id=intent_id,
            session_id=session_id,
            candidate_id=candidate_id,
            query_fingerprint=_hash_text(f"query-store:{query_id}"),
            action=operation,
            status=status,
            created_at_utc=utc_now(),
            updated_at_utc=utc_now(),
            metadata={
                "database_fingerprint": _database_fingerprint(database_name),
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
                "rejection_reason": (
                    None
                    if status == "prepared"
                    else "automatic or unknown ownership is review-only"
                ),
            },
        )
        intent = self.store.create_plan_action_intent(
            intent,
            idempotency_key=idempotency_key,
        )
        return {
            "intent": intent.to_dict(),
            "prepared": intent.status == "prepared",
            "apply_gates": self._gate_status(database_name),
            "raw_sql_persisted": False,
        }

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
        try:
            await self._execute_action(database_name, intent)
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
            updated = self._save_status(
                intent,
                "unknown",
                {
                    "apply_confirmation": "uncertain_after_error",
                    "apply_error_type": type(exc).__name__,
                    "authorization_fingerprint": _hash_text(authorization_reference),
                },
            )
            return {"intent": updated.to_dict(), "confirmed": False}
        expected_state = self._expected_applied_state(intent, current_state)
        confirmed_state = await self._capture_control_state(database_name, query_id)
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
        return {"intent": updated.to_dict(), "confirmed": True, "idempotent": False}

    async def verify(
        self,
        database_name: str,
        intent_id: str,
        *,
        candidate_evidence: Mapping[str, Any],
        authorization_reference: str,
    ) -> dict[str, Any]:
        intent = self._validated_intent(database_name, intent_id)
        if intent.status not in {"applied", "observing", "hold"}:
            raise ValueError(f"Intent status {intent.status!r} cannot be verified.")
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
        policy = self.database_policy.require(database_name)
        decision = decide_verification(
            baseline,
            candidate_evidence,
            expected_provenance={
                "environment": policy.environment,
                "database_name": database_name,
                "query_id": int(metadata["query_id"]),
            },
        )
        if decision.action == "hold":
            updated = self._save_status(
                intent,
                "hold",
                {
                    "verification": {
                        "action": decision.action,
                        "reason": decision.reason,
                    }
                },
            )
            return {"intent": updated.to_dict(), "decision": "hold"}
        if decision.action == "keep":
            updated = self._save_status(
                intent,
                "kept",
                {
                    "verification": {
                        "action": decision.action,
                        "reason": decision.reason,
                        "improvement_pct": decision.improvement_pct,
                    }
                },
            )
            return {"intent": updated.to_dict(), "decision": "keep"}
        rollback = await self.rollback(
            database_name,
            intent_id,
            authorization_reference=authorization_reference,
            reason=decision.reason,
        )
        rollback["decision"] = "rollback"
        return rollback

    async def rollback(
        self,
        database_name: str,
        intent_id: str,
        *,
        authorization_reference: str,
        reason: str = "explicit rollback requested",
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
            raise ValueError(f"Intent status {intent.status!r} cannot be rolled back.")
        try:
            intent = self._save_status(
                intent,
                "rolling_back",
                {
                    "rollback_started_at_utc": utc_now(),
                    "rollback_authorization_fingerprint": _hash_text(
                        authorization_reference
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
        try:
            await self._restore_prior_state(
                database_name,
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
            updated = self._save_status(
                intent,
                "unknown",
                {
                    "rollback_confirmation": "uncertain_after_error",
                    "rollback_error_type": type(exc).__name__,
                    "rollback_authorization_fingerprint": _hash_text(
                        authorization_reference
                    ),
                },
            )
            return {"intent": updated.to_dict(), "confirmed": False}
        confirmed = await self._capture_control_state(database_name, query_id)
        if not self._control_state_matches(confirmed, prior_state):
            updated = self._save_status(
                intent,
                "unknown",
                {"rollback_confirmation": "state_mismatch"},
            )
            return {"intent": updated.to_dict(), "confirmed": False}
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
        return {"intent": updated.to_dict(), "confirmed": True, "idempotent": False}

    async def _capture_control_state(
        self,
        database_name: str,
        query_id: int,
    ) -> dict[str, Any]:
        statements = [
            "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
            "BEGIN TRANSACTION",
            (
                "SELECT plan_id, plan_forcing_type_desc "
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
                "AND JSON_VALUE(state, '$.currentValue') = 'Active'))"
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
        }

    async def _execute_action(
        self,
        database_name: str,
        intent: PlanActionIntentV1,
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
        await self.admin_policy.execute(
            AdminAction(
                tool_name="apply_prepared_plan_action",
                database_name=database_name,
                action_type="query_store",
                sql=sql,
                params=params,
                trusted_generated=True,
            ),
            self.executor,
            dry_run=False,
        )

    async def _restore_prior_state(
        self,
        database_name: str,
        query_id: int,
        current_state: Mapping[str, Any],
        prior_state: Mapping[str, Any],
    ) -> None:
        current_plan_id = current_state.get("force_plan_id")
        prior_plan_id = prior_state.get("force_plan_id")
        prior_hints = prior_state.get("query_store_hints")
        statements: list[str] = []
        params: list[Any] = []
        if current_plan_id is not None:
            statements.append(
                "EXEC sys.sp_query_store_unforce_plan @query_id = ?, @plan_id = ?"
            )
            params.extend((query_id, int(current_plan_id)))
        statements.append("EXEC sys.sp_query_store_clear_hints @query_id = ?")
        params.append(query_id)
        if prior_hints is not None:
            statements.append(
                "DECLARE @hints nvarchar(max) = ?; "
                "EXEC sys.sp_query_store_set_hints @query_id = ?, @query_hints = @hints"
            )
            params.extend((str(prior_hints), query_id))
        if prior_plan_id is not None:
            statements.append(
                "EXEC sys.sp_query_store_force_plan @query_id = ?, @plan_id = ?"
            )
            params.extend((query_id, int(prior_plan_id)))
        await self.admin_policy.execute(
            AdminAction(
                tool_name="rollback_plan_action",
                database_name=database_name,
                action_type="query_store",
                sql="; ".join(statements),
                params=tuple(params),
                trusted_generated=True,
            ),
            self.executor,
            dry_run=False,
        )

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
        if intent.metadata.get("database_fingerprint") != _database_fingerprint(database_name):
            raise PermissionError("Intent database fingerprint does not match.")
        baseline_evidence = intent.metadata.get("baseline_evidence")
        evidence_hash = intent.metadata.get("evidence_hash")
        if (
            not isinstance(baseline_evidence, Mapping)
            or not isinstance(evidence_hash, str)
            or hash_evidence(baseline_evidence) != evidence_hash
        ):
            raise RuntimeError("Prepared intent evidence hash does not match durable evidence.")
        return intent

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


__all__ = ["PlanActionService"]
