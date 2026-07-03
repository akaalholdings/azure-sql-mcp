from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.admin_policy import AdminAction
from azure_sql_mcp.admin_policy import AdminPolicy
from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.connection import QueryResult


def _policy(server_config_factory, tmp_path: Path, write_policy: WritePolicy) -> AdminPolicy:
    config = server_config_factory(
        access_mode=AccessMode.UNRESTRICTED,
        write_policy=write_policy,
        audit_dir=str(tmp_path),
        audit_full_sql=False,
    )
    return AdminPolicy(config)


def _audit_events(tmp_path: Path) -> list[dict[str, Any]]:
    [path] = tmp_path.glob("*.jsonl")
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_admin_policy_blocks_hard_denied_arbitrary_sql(server_config_factory, tmp_path: Path) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    action = AdminAction(
        tool_name="execute_tsql_unrestricted",
        database_name="appdb",
        action_type="query",
        sql="EXEC sp_executesql N'DROP TABLE dbo.Users'",
    )

    with pytest.raises(PermissionError, match="hard denylist"):
        policy.preview(action)
    assert _audit_events(tmp_path)[0]["outcome"] == "blocked"


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM dbo.Users WHERE UserId = 1",
        "ALTER TABLE dbo.Users ADD IsDeleted bit NULL",
        "GRANT SELECT ON dbo.Users TO app_user",
        "EXEC('DR' + 'OP TABLE dbo.Users')",
        "SELECT * INTO dbo.UsersCopy FROM dbo.Users",
    ],
)
def test_admin_policy_blocks_untrusted_raw_write_sql(
    server_config_factory,
    tmp_path: Path,
    sql: str,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    action = AdminAction(
        tool_name="execute_tsql_unrestricted",
        database_name="appdb",
        action_type="query",
        sql=sql,
    )

    with pytest.raises(PermissionError, match="hard denylist"):
        policy.preview(action)

    assert _audit_events(tmp_path)[0]["outcome"] == "blocked"


def test_admin_policy_allows_untrusted_read_only_select_with_dangerous_string_literal(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.REVIEW)
    action = AdminAction(
        tool_name="execute_tsql_unrestricted",
        database_name="appdb",
        action_type="query",
        sql="SELECT 'DROP TABLE dbo.Users' AS harmless_text",
    )

    payload = policy.preview(action)

    assert payload["status"] == "dry_run"
    assert _audit_events(tmp_path)[0]["outcome"] == "preview"


def test_admin_policy_preview_audits_without_full_sql(server_config_factory, tmp_path: Path) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.REVIEW)
    action = AdminAction(
        tool_name="update_statistics",
        database_name="appdb",
        action_type="maintenance",
        sql="UPDATE STATISTICS [dbo].[Orders]",
        trusted_generated=True,
    )

    payload = policy.preview(action)

    assert payload["status"] == "dry_run"
    assert payload["dry_run"] is True
    events = _audit_events(tmp_path)
    assert events[0]["outcome"] == "preview"
    assert events[0]["sql_hash"]
    assert "sql" not in events[0]


@pytest.mark.asyncio
async def test_admin_policy_blocks_apply_when_write_policy_is_review(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.REVIEW)
    action = AdminAction(
        tool_name="rebuild_index",
        database_name="appdb",
        action_type="maintenance",
        sql="ALTER INDEX [IX] ON [dbo].[Orders] REBUILD",
        trusted_generated=True,
    )
    executor = AsyncMock()

    with pytest.raises(PermissionError, match="AZURE_SQL_WRITE_POLICY=apply"):
        await policy.execute(action, executor, dry_run=False)

    assert executor.execute_non_query.await_count == 0
    assert _audit_events(tmp_path)[0]["outcome"] == "blocked"


@pytest.mark.asyncio
async def test_admin_policy_executes_generated_query_store_action_when_apply_enabled(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    action = AdminAction(
        tool_name="apply_plan_action",
        database_name="appdb",
        action_type="query_store",
        sql="EXEC sp_query_store_force_plan @query_id = ?, @plan_id = ?",
        params=(42, 7),
        trusted_generated=True,
    )
    executor = AsyncMock()
    executor.execute_non_query = AsyncMock(return_value=0)

    payload = await policy.execute(action, executor, dry_run=False)

    executor.execute_non_query.assert_awaited_once_with(
        "appdb",
        "EXEC sp_query_store_force_plan @query_id = ?, @plan_id = ?",
        params=(42, 7),
    )
    assert payload["status"] == "completed"
    assert [event["outcome"] for event in _audit_events(tmp_path)] == [
        "apply_started",
        "apply_completed",
    ]


@pytest.mark.asyncio
async def test_admin_policy_query_results_are_json_safe(server_config_factory, tmp_path: Path) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    action = AdminAction(
        tool_name="execute_tsql_unrestricted",
        database_name="appdb",
        action_type="query",
        sql="SELECT 1 AS value",
    )
    executor = AsyncMock()
    executor.execute_batches = AsyncMock(
        return_value=[QueryResult(columns=("value",), rows=[{"value": 1}])]
    )

    payload = await policy.execute(action, executor, dry_run=False, max_rows=10)

    assert payload["result_sets"] == [
        {
            "columns": ["value"],
            "rows": [{"value": 1}],
            "row_count": 1,
        }
    ]
