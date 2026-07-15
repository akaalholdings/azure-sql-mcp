from __future__ import annotations

from typing import Any

from .admin_policy import AdminAction
from .admin_policy import AdminPolicy
from .connection import AzureSqlExecutor
from .query_regression import QueryRegressionService


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
        for row in regressions.get("recommendations", [])[:top_n]:
            action = self._recommend_force_action(row)
            if action:
                actions.append(action)

        for row in forced_plans.get("forced_plans", []):
            unforce = self._recommend_unforce_action(row)
            if unforce:
                actions.append(unforce)

        ranked = sorted(
            actions,
            key=lambda item: (item.get("priority", 0), item.get("score", 0)),
            reverse=True,
        )[:top_n]
        for rank, action in enumerate(ranked, start=1):
            action["rank"] = rank

        return {
            "database_name": database_name,
            "mode": "review",
            "window_minutes": window_minutes,
            "top_n": top_n,
            "regression_recommendation_count": regressions.get("recommendation_count", 0),
            "forced_plan_count": forced_plans.get("forced_plan_count", 0),
            "recommended_action_count": len(ranked),
            "recommended_actions": ranked,
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
            if dry_run:
                result = self.admin_policy.preview(admin_action)
            else:
                result = await self.admin_policy.execute(
                    admin_action,
                    self.executor,
                    dry_run=False,
                )
            result["query_id"] = query_id
            result["plan_id"] = plan_id
            result["plan_action"] = self._normalize_action(action)
            result["candidate"] = candidate
            action_results.append(result)

        return {
            "database_name": database_name,
            "mode": "dry_run" if dry_run else "apply",
            "dry_run": dry_run,
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
    def _recommend_force_action(row: dict[str, Any]) -> dict[str, Any] | None:
        query_id = _coerce_int(row.get("query_id"))
        plan_id = _coerce_int(row.get("recommended_plan_id"))
        if query_id is None or plan_id is None:
            return None
        score = _coerce_float(row.get("score")) or 0.0
        return {
            "action": "force",
            "query_id": query_id,
            "plan_id": plan_id,
            "reason": row.get("reason") or "query_store_regression",
            "score": score,
            "priority": 100,
            "estimated_cpu_gain": row.get("estimated_cpu_gain"),
            "estimated_duration_gain": row.get("estimated_duration_gain"),
            "state": row.get("current_state"),
        }

    @staticmethod
    def _recommend_unforce_action(row: dict[str, Any]) -> dict[str, Any] | None:
        query_id = _coerce_int(row.get("query_id"))
        plan_id = _coerce_int(row.get("plan_id"))
        if query_id is None or plan_id is None:
            return None
        force_failure_count = _coerce_int(row.get("force_failure_count")) or 0
        days_since_last_exec = _coerce_int(row.get("days_since_last_exec")) or 0
        if force_failure_count > 0:
            return {
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
        if days_since_last_exec > 30:
            return {
                "action": "unforce",
                "query_id": query_id,
                "plan_id": plan_id,
                "reason": "stale_forced_plan",
                "score": days_since_last_exec,
                "priority": 50,
                "days_since_last_exec": days_since_last_exec,
            }
        return None


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
