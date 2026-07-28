from __future__ import annotations

import json

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from azure_sql_mcp.plan_enforcement import PlanEnforcementService


class FakeRegressionService:
    def __init__(self) -> None:
        self.detect_windows: list[int] = []
        self.forced_windows: list[int] = []

    async def detect_regressed_queries(
        self,
        database_name: str,
        window_minutes: int = 1440,
    ) -> dict:
        self.detect_windows.append(window_minutes)
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "recommendation_count": 1,
            "recommendations": [
                {
                    "query_id": "42",
                    "recommended_plan_id": "7",
                    "reason": "regressed_plan",
                    "score": 93,
                }
            ],
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
            "forced_plan_count": 0,
            "forced_plans": [],
            "warnings": [],
        }


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
async def test_dry_run_plan_action_works_through_the_tool_wrapper() -> None:
    """Regression: dry_run_action was sync, so the tool wrapper awaited its
    dict and the registered tool ALWAYS failed with
    "'dict' object can't be awaited" (found live)."""
    from unittest.mock import MagicMock

    from tests.unit.test_server import make_config
    from azure_sql_mcp.config import AccessMode
    from azure_sql_mcp.server import AzureSqlMcpApplication

    app = AzureSqlMcpApplication(make_config(AccessMode.UNRESTRICTED))
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
async def test_direct_apply_plan_action_cannot_mutate() -> None:
    """Only a prepared intent may cross the mutation boundary."""
    from unittest.mock import AsyncMock

    from tests.unit.test_server import make_config
    from azure_sql_mcp.config import AccessMode
    from azure_sql_mcp.server import AzureSqlMcpApplication

    app = AzureSqlMcpApplication(make_config(AccessMode.UNRESTRICTED))
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
async def test_plan_enforcer_tick_apply_request_is_an_mcp_error() -> None:
    from tests.unit.test_server import make_config
    from azure_sql_mcp.config import AccessMode
    from azure_sql_mcp.server import AzureSqlMcpApplication

    app = AzureSqlMcpApplication(make_config(AccessMode.UNRESTRICTED))

    with pytest.raises(ToolError) as error:
        await app.mcp._tool_manager.call_tool(
            "plan_enforcer_tick",
            {"dry_run": False, "database_name": "appdb"},
        )

    payload = json.loads(str(error.value).split(": ", 1)[1])
    assert payload["code"] == "preview_only"
