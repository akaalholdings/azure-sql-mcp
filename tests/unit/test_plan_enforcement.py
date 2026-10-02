from __future__ import annotations

import json

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from azure_sql_mcp.plan_enforcement import PlanEnforcementService


def _recommendation(**overrides) -> dict:
    """A parsed detect_regressed_queries row that is safe to force."""
    row = {
        "type": "FORCE_LAST_GOOD_PLAN",
        "query_id": 42,
        "regressed_plan_id": 9,
        "recommended_plan_id": 7,
        "reason": "regressed_plan",
        "score": 93,
        "current_state": "Active",
        "state_reason": "AutomaticTuningOptionNotEnabled",
        "is_executable_action": True,
        "execute_action_initiated_by": None,
        "live": True,
        "recommended_plan_is_forced": False,
        "estimated_cpu_gain": 48.1,
        "estimated_cpu_gain_unit": "cpu_seconds",
        "estimated_duration_gain": None,
    }
    row.update(overrides)
    return row


def _forced_plan(**overrides) -> dict:
    row = {
        "plan_id": 201,
        "query_id": 99,
        "is_forced_plan": True,
        "plan_forcing_type_desc": "MANUAL",
        "force_failure_count": 0,
        "last_force_failure_reason_desc": None,
        "recent_execution_count": 50,
        "query_recent_execution_count": 50,
        "days_since_last_exec": 0,
    }
    row.update(overrides)
    return row


class FakeRegressionService:
    def __init__(
        self,
        recommendations: list[dict] | None = None,
        forced_plans: list[dict] | None = None,
    ) -> None:
        self.detect_windows: list[int] = []
        self.forced_windows: list[int] = []
        self.recommendations = (
            [_recommendation()] if recommendations is None else recommendations
        )
        self.forced_plans = forced_plans or []

    async def detect_regressed_queries(
        self,
        database_name: str,
        window_minutes: int = 1440,
    ) -> dict:
        self.detect_windows.append(window_minutes)
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "recommendation_count": len(self.recommendations),
            "recommendations": self.recommendations,
        }

    async def get_forced_plans(
        self,
        database_name: str,
        window_minutes: int = 1440,
    ) -> dict:
        self.forced_windows.append(window_minutes)
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "forced_plan_count": len(self.forced_plans),
            "forced_plans": self.forced_plans,
            "warnings": [],
        }


def _service(regression) -> PlanEnforcementService:
    return PlanEnforcementService(
        executor=object(),  # type: ignore[arg-type]
        query_regression=regression,  # type: ignore[arg-type]
        admin_policy=FakeAdminPolicy(),  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_review_produces_force_action_from_raw_dmv_row():
    """End to end through the real parser: a documented DMV row whose
    regressed plan is running must rank one force of the last-good plan."""
    from tests.unit.test_query_regression import _FORCE_DETAILS_42
    from tests.unit.test_query_regression import FakeExecutor
    from tests.unit.test_query_regression import _dmv_row

    from azure_sql_mcp.query_regression import QueryRegressionService

    activity = [
        {"plan_id": 103, "query_id": 42, "is_forced_plan": False,
         "last_seen_utc": "2026-10-01T11:00:00", "recent_execution_count": 250},
        {"plan_id": 101, "query_id": 42, "is_forced_plan": False,
         "last_seen_utc": None, "recent_execution_count": None},
    ]
    executor = FakeExecutor([[_dmv_row(_FORCE_DETAILS_42)], activity, []])
    service = _service(QueryRegressionService(executor))  # type: ignore[arg-type]

    result = await service.review("appdb", window_minutes=60)

    assert result["recommended_action_count"] == 1
    action = result["recommended_actions"][0]
    assert (action["action"], action["query_id"], action["plan_id"]) == ("force", 42, 101)
    assert action["regressed_plan_id"] == 103
    assert action["estimated_cpu_gain_unit"] == "cpu_seconds"
    assert result["excluded_recommendations"] == []


@pytest.mark.asyncio
async def test_review_does_not_rank_force_for_recommendations_it_does_not_own():
    """Forcing again what automatic tuning applied, reverted or let expire is
    wrong advice; each is reported as excluded with the reason."""
    regression = FakeRegressionService(
        recommendations=[
            _recommendation(query_id=1, current_state="Success",
                            execute_action_initiated_by="System"),
            _recommendation(query_id=2, current_state="Reverted"),
            _recommendation(query_id=3, current_state="Expired"),
            _recommendation(query_id=4, current_state="Verifying"),
            _recommendation(query_id=5, current_state=None),
            _recommendation(query_id=6, execute_action_initiated_by="System"),
            _recommendation(query_id=7, is_executable_action=False),
            _recommendation(query_id=8, live=False),
            _recommendation(query_id=9, recommended_plan_is_forced=True),
            _recommendation(query_id=10, recommended_plan_is_forced=None),
            _recommendation(query_id=None, recommended_plan_id=None),
        ]
    )

    result = await _service(regression).review("appdb")

    assert result["recommended_actions"] == []
    reasons = {
        entry["query_id"]: entry["exclusion_reason"]
        for entry in result["excluded_recommendations"]
    }
    assert reasons == {
        1: "already_applied",
        2: "reverted_by_automatic_tuning",
        3: "expired",
        4: "engine_verifying",
        5: "state_unknown",
        6: "ownership_review",
        7: "not_executable",
        8: "regressed_plan_not_live",
        9: "recommended_plan_already_forced",
        10: "recommended_plan_not_in_query_store",
        None: "plan_ids_missing",
    }
    assert all(
        entry["candidate_action"] == "force" and entry["explanation"]
        for entry in result["excluded_recommendations"]
    )


@pytest.mark.asyncio
async def test_review_routes_auto_forced_plan_to_ownership_review_not_unforce():
    regression = FakeRegressionService(
        recommendations=[],
        forced_plans=[
            _forced_plan(
                plan_forcing_type_desc="AUTO",
                force_failure_count=2,
                last_force_failure_reason_desc="NO_PLAN",
                recent_execution_count=0,
                query_recent_execution_count=40,
            )
        ],
    )

    result = await _service(regression).review("appdb")

    assert result["recommended_actions"] == []
    [entry] = result["excluded_recommendations"]
    assert entry["candidate_action"] == "unforce"
    assert (entry["query_id"], entry["plan_id"]) == (99, 201)
    assert entry["exclusion_reason"] == "ownership_review"
    assert entry["plan_forcing_type_desc"] == "AUTO"


@pytest.mark.asyncio
async def test_review_does_not_unforce_manual_plan_whose_failures_are_old():
    # force_failure_count is cumulative; the forced plan still runs now.
    regression = FakeRegressionService(
        recommendations=[],
        forced_plans=[
            _forced_plan(
                force_failure_count=3,
                last_force_failure_reason_desc="SCHEMA_CHANGE",
                recent_execution_count=50,
                query_recent_execution_count=50,
            )
        ],
    )

    result = await _service(regression).review("appdb")

    assert result["recommended_actions"] == []
    assert result["excluded_recommendations"] == []


@pytest.mark.asyncio
async def test_review_unforces_manual_plan_whose_forcing_fails_now():
    regression = FakeRegressionService(
        recommendations=[],
        forced_plans=[
            _forced_plan(
                force_failure_count=3,
                last_force_failure_reason_desc="NO_INDEX",
                recent_execution_count=0,
                query_recent_execution_count=40,
            ),
            _forced_plan(
                plan_id=301,
                query_id=77,
                force_failure_count=1,
                last_force_failure_reason_desc="NONE",
                recent_execution_count=0,
                query_recent_execution_count=40,
            ),
            _forced_plan(plan_id=401, query_id=88, days_since_last_exec=45),
        ],
    )

    result = await _service(regression).review("appdb")

    actions = [
        (a["action"], a["plan_id"], a["reason"]) for a in result["recommended_actions"]
    ]
    assert actions == [
        ("unforce", 201, "forced_plan_failure"),
        ("unforce", 401, "stale_forced_plan"),
    ]


@pytest.mark.asyncio
async def test_review_leaves_active_recommendations_to_automatic_tuning_when_it_is_on():
    """Microsoft documents state reason AutomaticTuningOptionNotEnabled as 'apply
    manually'. Any other Active row means FORCE_LAST_GOOD_PLAN (on by default in
    Azure SQL Database) will apply it; a custom force would race the engine."""
    regression = FakeRegressionService(
        recommendations=[
            _recommendation(query_id=1, state_reason=None),
            _recommendation(query_id=2, state_reason="SomethingElse"),
            _recommendation(query_id=3, state_reason="AutomaticTuningOptionNotEnabled"),
        ]
    )

    result = await _service(regression).review("appdb")

    assert [(a["action"], a["query_id"]) for a in result["recommended_actions"]] == [
        ("force", 3)
    ]
    excluded = {e["query_id"]: e for e in result["excluded_recommendations"]}
    assert set(excluded) == {1, 2}
    assert {e["exclusion_reason"] for e in excluded.values()} == {"ownership_review"}
    assert excluded[2]["state_reason"] == "SomethingElse"
    assert "automatic tuning" in excluded[1]["explanation"].casefold()


@pytest.mark.asyncio
async def test_review_ranks_ready_actions_above_force_that_needs_an_owner_decision():
    """prepare_plan_action treats every Active FORCE_LAST_GOOD_PLAN recommendation
    as automatic ownership and rejects it, so even a manual-apply force cannot be
    prepared until a human settles ownership. It is flagged and must not crowd
    out an unforce that can proceed: plan_enforcer_tick previews only the top."""
    failing_now = _forced_plan(
        force_failure_count=3,
        last_force_failure_reason_desc="NO_INDEX",
        recent_execution_count=0,
        query_recent_execution_count=40,
    )
    regression = FakeRegressionService(
        recommendations=[_recommendation()], forced_plans=[failing_now]
    )

    result = await _service(regression).review("appdb")

    ranked = [(a["rank"], a["action"]) for a in result["recommended_actions"]]
    assert ranked == [(1, "unforce"), (2, "force")]
    force = result["recommended_actions"][1]
    assert force["owner_decision_required"] is True
    assert "prepare_plan_action" in force["owner_decision"]

    policy = FakeAdminPolicy()
    tick = await PlanEnforcementService(
        executor=object(),  # type: ignore[arg-type]
        query_regression=regression,  # type: ignore[arg-type]
        admin_policy=policy,  # type: ignore[arg-type]
    ).tick("appdb")
    assert [a["plan_action"] for a in tick["actions"]] == ["unforce"]


@pytest.mark.asyncio
async def test_review_does_not_unforce_failing_manual_plan_when_the_query_did_not_run():
    # No executions of the query in the window: no evidence that forcing fails
    # now; the query may simply be idle.
    regression = FakeRegressionService(
        recommendations=[],
        forced_plans=[
            _forced_plan(
                force_failure_count=3,
                last_force_failure_reason_desc="NO_INDEX",
                recent_execution_count=0,
                query_recent_execution_count=0,
            )
        ],
    )

    result = await _service(regression).review("appdb")

    assert result["recommended_actions"] == []
    assert result["excluded_recommendations"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("forcing_type", [None, "NONE", "SOMETHING_NEW"])
async def test_review_routes_unknown_forcing_type_to_ownership_review(forcing_type):
    # Only MANUAL forcing is ours to unforce; unknown ownership is review-only.
    regression = FakeRegressionService(
        recommendations=[],
        forced_plans=[
            _forced_plan(
                plan_forcing_type_desc=forcing_type,
                force_failure_count=2,
                last_force_failure_reason_desc="NO_PLAN",
                recent_execution_count=0,
                query_recent_execution_count=40,
            )
        ],
    )

    result = await _service(regression).review("appdb")

    assert result["recommended_actions"] == []
    [entry] = result["excluded_recommendations"]
    assert entry["exclusion_reason"] == "ownership_review"


@pytest.mark.asyncio
async def test_review_status_is_not_a_true_negative_while_items_await_review():
    """Agents read result_status before the data. An engine-owned forced plan
    that fails now is only in excluded_recommendations; 'empty' would tell the
    agent there is nothing to review."""
    from azure_sql_mcp.result_status import apply_result_status

    regression = FakeRegressionService(
        recommendations=[],
        forced_plans=[
            _forced_plan(
                plan_forcing_type_desc="AUTO",
                force_failure_count=4,
                last_force_failure_reason_desc="NO_INDEX",
                recent_execution_count=0,
                query_recent_execution_count=500,
            )
        ],
    )

    stamped = apply_result_status(
        "review_plan_enforcement", await _service(regression).review("appdb")
    )

    assert stamped["recommended_actions"] == []
    assert stamped["result_status"] == "ok"
    assert "ownership_review" in stamped["result_status_reason"]

    nothing = apply_result_status(
        "review_plan_enforcement",
        await _service(FakeRegressionService(recommendations=[])).review("appdb"),
    )
    assert nothing["result_status"] == "empty"


@pytest.mark.asyncio
async def test_plan_enforcer_tick_with_only_excluded_candidates_previews_nothing():
    policy = FakeAdminPolicy()
    regression = FakeRegressionService(
        recommendations=[_recommendation(current_state="Success")],
        forced_plans=[_forced_plan(plan_forcing_type_desc="AUTO", days_since_last_exec=60)],
    )
    service = PlanEnforcementService(
        executor=object(),  # type: ignore[arg-type]
        query_regression=regression,  # type: ignore[arg-type]
        admin_policy=policy,  # type: ignore[arg-type]
    )

    result = await service.tick("appdb")

    assert result["action_count"] == 0
    assert policy.previewed == []
    assert len(result["review"]["excluded_recommendations"]) == 2


class FakeAdminPolicy:
    def __init__(self) -> None:
        self.previewed = []
        self.executed = []

    def preview(self, action):
        self.previewed.append(action)
        return {
            "status": "dry_run",
            "tool_name": action.tool_name,
            "sql": action.sql,
            "rollback_sql": action.rollback_sql,
        }

    async def execute(self, action, executor, *, dry_run: bool, max_rows=None):
        self.executed.append((action, dry_run, max_rows))
        return {
            "status": "completed",
            "tool_name": action.tool_name,
            "sql": action.sql,
        }


@pytest.mark.asyncio
async def test_plan_enforcer_tick_defaults_to_dry_run_preview():
    policy = FakeAdminPolicy()
    regression = FakeRegressionService()
    service = PlanEnforcementService(
        executor=object(),  # type: ignore[arg-type]
        query_regression=regression,  # type: ignore[arg-type]
        admin_policy=policy,  # type: ignore[arg-type]
    )

    result = await service.tick("appdb", window_minutes=60)

    assert result["dry_run"] is True
    assert result["action_count"] == 1
    assert result["actions"][0]["status"] == "dry_run"
    assert result["actions"][0]["plan_action"] == "force"
    assert "sp_query_store_force_plan" in result["actions"][0]["sql"]
    assert regression.detect_windows == [60]
    assert regression.forced_windows == [60]
    assert len(policy.previewed) == 1
    assert policy.executed == []


@pytest.mark.asyncio
async def test_plan_enforcer_tick_rejects_apply_requests():
    policy = FakeAdminPolicy()
    service = PlanEnforcementService(
        executor=object(),  # type: ignore[arg-type]
        query_regression=FakeRegressionService(),  # type: ignore[arg-type]
        admin_policy=policy,  # type: ignore[arg-type]
    )

    with pytest.raises(PermissionError, match="permanently preview-only"):
        await service.tick("appdb", dry_run=False)
    assert policy.previewed == []
    assert policy.executed == []


@pytest.mark.asyncio
async def test_dry_run_plan_action_works_through_the_tool_wrapper(tmp_path) -> None:
    """Regression: dry_run_action was sync, so the tool wrapper awaited its
    dict and the registered tool ALWAYS failed with
    "'dict' object can't be awaited" (found live)."""
    from unittest.mock import MagicMock

    from tests.unit.test_server import make_config
    from azure_sql_mcp.config import AccessMode
    from azure_sql_mcp.server import AzureSqlMcpApplication

    app = AzureSqlMcpApplication(
        make_config(tmp_path, AccessMode.UNRESTRICTED)
    )
    app.admin_policy.preview = MagicMock(  # type: ignore[method-assign]
        return_value={"status": "dry_run", "dry_run": True, "audit_id": "a1"}
    )

    payload = await app.mcp._tool_manager.call_tool(
        "dry_run_plan_action",
        {"action": "force", "query_id": 42, "plan_id": 7, "database_name": "appdb"},
    )

    assert payload.get("code") is None, payload.get("message")
    assert payload["status"] == "dry_run"


@pytest.mark.asyncio
async def test_direct_apply_plan_action_cannot_mutate(tmp_path) -> None:
    """Only a prepared intent may cross the mutation boundary."""
    from unittest.mock import AsyncMock

    from tests.unit.test_server import make_config
    from azure_sql_mcp.config import AccessMode
    from azure_sql_mcp.server import AzureSqlMcpApplication

    app = AzureSqlMcpApplication(
        make_config(tmp_path, AccessMode.UNRESTRICTED)
    )
    app.admin_policy.execute = AsyncMock(  # type: ignore[method-assign]
        return_value={"status": "dry_run", "dry_run": True, "audit_id": "a2"}
    )

    # no dry_run argument -> must preview, not execute
    payload = await app.mcp._tool_manager.call_tool(
        "apply_plan_action",
        {"action": "force", "query_id": 42, "plan_id": 7, "database_name": "appdb"},
    )
    assert payload.get("code") is None, payload.get("message")
    assert app.admin_policy.execute.await_args.kwargs["dry_run"] is True

    # Explicit direct apply is rejected and never reaches a live policy call.
    with pytest.raises(ToolError, match="preview-only"):
        await app.mcp._tool_manager.call_tool(
            "apply_plan_action",
            {
                "action": "force",
                "query_id": 42,
                "plan_id": 7,
                "dry_run": False,
                "database_name": "appdb",
            },
        )
    assert app.admin_policy.execute.await_count == 1


@pytest.mark.asyncio
async def test_plan_enforcer_tick_apply_request_is_an_mcp_error(tmp_path) -> None:
    from tests.unit.test_server import make_config
    from azure_sql_mcp.config import AccessMode
    from azure_sql_mcp.server import AzureSqlMcpApplication

    app = AzureSqlMcpApplication(
        make_config(tmp_path, AccessMode.UNRESTRICTED)
    )

    with pytest.raises(ToolError) as error:
        await app.mcp._tool_manager.call_tool(
            "plan_enforcer_tick",
            {"dry_run": False, "database_name": "appdb"},
        )

    payload = json.loads(str(error.value).split(": ", 1)[1])
    assert payload["code"] == "preview_only"
