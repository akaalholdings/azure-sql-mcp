from __future__ import annotations

from collections import Counter
from typing import Any

from .admin_policy import AdminAction
from .admin_policy import AdminPolicy
from .connection import AzureSqlExecutor
from .query_regression import QueryRegressionService
from .result_status import ResultStatus
from .result_status import status_payload


class PlanEnforcementService:
    """Read-first Query Store plan enforcement workflow."""

    def __init__(
        self,
        executor: AzureSqlExecutor,
        query_regression: QueryRegressionService,
        admin_policy: AdminPolicy,
    ) -> None:
        self.executor = executor
        self.query_regression = query_regression
        self.admin_policy = admin_policy

    async def review(
        self,
        database_name: str,
        *,
        window_minutes: int = 1440,
        top_n: int = 20,
    ) -> dict[str, Any]:
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")
        if top_n <= 0:
            raise ValueError("top_n must be greater than 0.")

        regressions = await self.query_regression.detect_regressed_queries(
            database_name,
            window_minutes=window_minutes,
        )
        forced_plans = await self.query_regression.get_forced_plans(
            database_name,
            window_minutes=window_minutes,
        )

        actions: list[dict[str, Any]] = []
        excluded: list[dict[str, Any]] = []
        for row in regressions.get("recommendations", []):
            action, exclusion = self._recommend_force_action(row)
            if action:
                actions.append(action)
            if exclusion:
                excluded.append(exclusion)

        for row in forced_plans.get("forced_plans", []):
            unforce, exclusion = self._recommend_unforce_action(row)
            if unforce:
                actions.append(unforce)
            if exclusion:
                excluded.append(exclusion)

        ranked = sorted(
            actions,
            key=lambda item: (item.get("priority", 0), item.get("score", 0)),
            reverse=True,
        )[:top_n]
        for rank, action in enumerate(ranked, start=1):
            action["rank"] = rank

        status: dict[str, Any] = {}
        if not ranked and excluded:
            # Not a true negative: the excluded candidates still need reading.
            counts = Counter(str(item["exclusion_reason"]) for item in excluded)
            summary = ", ".join(f"{code}={count}" for code, count in sorted(counts.items()))
            reason = (
                f"No action is ranked; excluded_recommendations holds {len(excluded)} "
                f"candidate(s) ({summary})."
            )
            if "ownership_review" in counts:
                reason += " ownership_review items need a human to choose one owner."
            status = status_payload(ResultStatus.OK, reason)

        return {
            **status,
            "database_name": database_name,
            "mode": "review",
            "window_minutes": window_minutes,
            "top_n": top_n,
            "regression_recommendation_count": regressions.get("recommendation_count", 0),
            "forced_plan_count": forced_plans.get("forced_plan_count", 0),
            "recommended_action_count": len(ranked),
            "recommended_actions": ranked,
            "excluded_recommendations": excluded,
            "forced_plan_warnings": forced_plans.get("warnings", []),
        }

    async def dry_run_action(
        self,
        database_name: str,
        *,
        action: str,
        query_id: int,
        plan_id: int,
    ) -> dict[str, Any]:
        # async so the tool wrapper can await it like every other service call
        # (as a sync method the wrapper awaited its dict and the tool always
        # failed with "'dict' object can't be awaited").
        return self.admin_policy.preview(
            self._build_action(
                database_name,
                action,
                query_id,
                plan_id,
                tool_name="dry_run_plan_action",
            )
        )

    async def apply_action(
        self,
        database_name: str,
        *,
        action: str,
        query_id: int,
        plan_id: int,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if not dry_run:
            raise PermissionError(
                "Direct plan actions are preview-only; use prepare_plan_action and "
                "apply_prepared_plan_action."
            )
        admin_action = self._build_action(
            database_name,
            action,
            query_id,
            plan_id,
            tool_name="apply_plan_action",
        )
        payload = await self.admin_policy.execute(
            admin_action,
            self.executor,
            dry_run=dry_run,
        )
        payload["query_id"] = query_id
        payload["plan_id"] = plan_id
        payload["plan_action"] = self._normalize_action(action)
        return payload

    async def tick(
        self,
        database_name: str,
        *,
        window_minutes: int = 1440,
        max_actions: int = 1,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if max_actions <= 0:
            raise ValueError("max_actions must be greater than 0.")
        if not dry_run:
            raise PermissionError(
                "plan_enforcer_tick is permanently preview-only; use the prepared workflow."
            )

        review = await self.review(
            database_name,
            window_minutes=window_minutes,
            top_n=max_actions,
        )
        candidates = review.get("recommended_actions", [])[:max_actions]
        action_results: list[dict[str, Any]] = []
        for candidate in candidates:
            action = str(candidate["action"])
            query_id = int(candidate["query_id"])
            plan_id = int(candidate["plan_id"])
            admin_action = self._build_action(
                database_name,
                action,
                query_id,
                plan_id,
                tool_name="plan_enforcer_tick",
            )
            result = self.admin_policy.preview(admin_action)
            result["query_id"] = query_id
            result["plan_id"] = plan_id
            result["plan_action"] = self._normalize_action(action)
            result["candidate"] = candidate
            action_results.append(result)

        return {
            "database_name": database_name,
            "mode": "preview",
            "dry_run": True,
            "window_minutes": window_minutes,
            "max_actions": max_actions,
            "candidate_count": len(candidates),
            "action_count": len(action_results),
            "actions": action_results,
            "review": review,
        }

    def _build_action(
        self,
        database_name: str,
        action: str,
        query_id: int,
        plan_id: int,
        tool_name: str,
    ) -> AdminAction:
        normalized = self._normalize_action(action)
        if query_id <= 0:
            raise ValueError("query_id must be greater than 0.")
        if plan_id <= 0:
            raise ValueError("plan_id must be greater than 0.")

        if normalized == "force":
            sql = "EXEC sp_query_store_force_plan @query_id = ?, @plan_id = ?"
            rollback_sql = (
                "EXEC sp_query_store_unforce_plan "
                f"@query_id = {int(query_id)}, @plan_id = {int(plan_id)}"
            )
        else:
            sql = "EXEC sp_query_store_unforce_plan @query_id = ?, @plan_id = ?"
            rollback_sql = (
                "EXEC sp_query_store_force_plan "
                f"@query_id = {int(query_id)}, @plan_id = {int(plan_id)}"
            )

        return AdminAction(
            tool_name=tool_name,
            database_name=database_name,
            action_type="query_store",
            sql=sql,
            params=(int(query_id), int(plan_id)),
            rollback_sql=rollback_sql,
            trusted_generated=True,
        )

    @staticmethod
    def _normalize_action(action: str) -> str:
        normalized = action.strip().lower().replace("_plan", "")
        if normalized in {"force", "forced"}:
            return "force"
        if normalized in {"unforce", "unforced"}:
            return "unforce"
        raise ValueError("action must be 'force' or 'unforce'.")

    @staticmethod
    def _recommend_force_action(
        row: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """Return (force action, None) or (None, exclusion with its reason).

        Force is ranked only for an Active, executable recommendation that
        automatic tuning leaves to a human (AutomaticTuningOptionNotEnabled),
        whose regressed plan still runs and whose last-good plan is in Query
        Store and not already forced.
        """
        query_id = _coerce_int(row.get("query_id"))
        plan_id = _coerce_int(row.get("recommended_plan_id"))
        score = _coerce_float(row.get("score")) or 0.0
        state = row.get("current_state")
        if query_id is None or plan_id is None:
            exclusion = (
                "plan_ids_missing",
                "The recommendation details name no query_id and plan_id to force.",
            )
        elif state is None:
            exclusion = ("state_unknown", "The recommendation state could not be read.")
        elif str(state).casefold() != "active":
            exclusion = _FORCE_STATE_EXCLUSIONS.get(
                str(state).casefold(),
                ("state_not_active", f"The recommendation state is {state}, not Active."),
            )
        elif str(row.get("execute_action_initiated_by") or "").casefold() == "system":
            exclusion = (
                "ownership_review",
                "Automatic tuning initiated this recommendation; the engine owns it.",
            )
        elif str(row.get("state_reason") or "").casefold() != "automatictuningoptionnotenabled":
            # Only this reason is Microsoft's "apply manually"; otherwise
            # FORCE_LAST_GOOD_PLAN is on and the engine applies it itself.
            exclusion = (
                "ownership_review",
                f"The state reason is {row.get('state_reason')}, not "
                "AutomaticTuningOptionNotEnabled: automatic tuning owns this "
                "recommendation and will apply it.",
            )
        elif not row.get("is_executable_action"):
            exclusion = (
                "not_executable",
                "The engine marks this recommendation as not executable.",
            )
        elif row.get("recommended_plan_is_forced") is None:
            exclusion = (
                "recommended_plan_not_in_query_store",
                "The recommended plan is no longer in Query Store, so it cannot be forced.",
            )
        elif row.get("recommended_plan_is_forced"):
            exclusion = (
                "recommended_plan_already_forced",
                "The recommended plan is already forced.",
            )
        elif not row.get("live"):
            exclusion = (
                "regressed_plan_not_live",
                "The regressed plan did not run in the window; the regression is not current.",
            )
        else:
            return (
                {
                    "action": "force",
                    "query_id": query_id,
                    "plan_id": plan_id,
                    "regressed_plan_id": row.get("regressed_plan_id"),
                    "reason": row.get("reason") or "query_store_regression",
                    "score": score,
                    "priority": 100,
                    "estimated_cpu_gain": row.get("estimated_cpu_gain"),
                    "estimated_cpu_gain_unit": row.get("estimated_cpu_gain_unit"),
                    "estimated_duration_gain": row.get("estimated_duration_gain"),
                    "state": state,
                },
                None,
            )
        code, explanation = exclusion
        return (
            None,
            {
                "candidate_action": "force",
                "query_id": query_id,
                "plan_id": plan_id,
                "regressed_plan_id": row.get("regressed_plan_id"),
                "state": state,
                "state_reason": row.get("state_reason"),
                "score": score,
                "exclusion_reason": code,
                "explanation": explanation,
            },
        )

    @staticmethod
    def _recommend_unforce_action(
        row: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """Return (unforce action, None), (None, ownership exclusion) or (None, None).

        force_failure_count is cumulative, so a failure ranks unforce only when
        forcing fails now: the query ran in the window but the forced plan did
        not. Older failures stay in get_forced_plans warnings. Only MANUAL
        forcing is unforced; an engine-owned plan goes to ownership review.
        """
        query_id = _coerce_int(row.get("query_id"))
        plan_id = _coerce_int(row.get("plan_id"))
        if query_id is None or plan_id is None:
            return None, None
        force_failure_count = _coerce_int(row.get("force_failure_count")) or 0
        days_since_last_exec = _coerce_int(row.get("days_since_last_exec")) or 0
        last_failure = str(row.get("last_force_failure_reason_desc") or "")
        failing_now = (
            force_failure_count > 0
            and last_failure.casefold() not in {"", "none"}
            and (_coerce_int(row.get("query_recent_execution_count")) or 0) > 0
            and (_coerce_int(row.get("recent_execution_count")) or 0) == 0
        )
        if failing_now:
            action = {
                "action": "unforce",
                "query_id": query_id,
                "plan_id": plan_id,
                "reason": "forced_plan_failure",
                "score": force_failure_count,
                "priority": 90,
                "last_force_failure_reason_desc": row.get(
                    "last_force_failure_reason_desc"
                ),
            }
        elif days_since_last_exec > 30:
            action = {
                "action": "unforce",
                "query_id": query_id,
                "plan_id": plan_id,
                "reason": "stale_forced_plan",
                "score": days_since_last_exec,
                "priority": 50,
                "days_since_last_exec": days_since_last_exec,
            }
        else:
            return None, None

        forcing_type = row.get("plan_forcing_type_desc")
        if str(forcing_type or "").casefold() != "manual":
            return (
                None,
                {
                    "candidate_action": "unforce",
                    "query_id": query_id,
                    "plan_id": plan_id,
                    "plan_forcing_type_desc": forcing_type,
                    "unforce_reason": action["reason"],
                    "exclusion_reason": "ownership_review",
                    "explanation": (
                        f"plan_forcing_type_desc is {forcing_type}, not MANUAL: the "
                        "engine owns this forced plan. Review ownership; do not unforce."
                    ),
                },
            )
        return action, None


_FORCE_STATE_EXCLUSIONS: dict[str, tuple[str, str]] = {
    "success": (
        "already_applied",
        "Automatic tuning or a user already applied this recommendation.",
    ),
    "reverted": (
        "reverted_by_automatic_tuning",
        "Automatic tuning reverted this plan because it gave no significant gain.",
    ),
    "expired": (
        "expired",
        "The recommendation expired (for example schema or statistics changed).",
    ),
    "verifying": (
        "engine_verifying",
        "The engine applied this recommendation and is verifying it; the engine owns it.",
    ),
}


def _coerce_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
