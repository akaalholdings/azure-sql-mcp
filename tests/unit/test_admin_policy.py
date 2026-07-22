from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.admin_policy import AdminAction
from azure_sql_mcp.admin_policy import AdminPolicy
from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.connection import AdminBatchOutcomeUnknownError
from azure_sql_mcp.connection import BatchExecutionMode
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


def _raw_action(sql: str) -> AdminAction:
    return AdminAction(
        tool_name="execute_tsql_unrestricted",
        database_name="appdb",
        action_type="query",
        sql=sql,
    )


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE DATABASE reporting",
        "ALTER DATABASE appdb SET READ_COMMITTED_SNAPSHOT ON",
        "CREATE TABLE dbo.NewTable (Id int NOT NULL)",
        "ALTER TABLE dbo.NewTable ADD Name nvarchar(100)",
        "DROP TABLE dbo.NewTable",
        "CREATE LOGIN app_login WITH PASSWORD = 'local-fixture'",
        "ALTER LOGIN app_login DISABLE",
        "DROP LOGIN app_login",
        "CREATE USER app_user FOR LOGIN app_login",
        "ALTER ROLE db_datareader ADD MEMBER app_user",
        "DROP USER app_user",
        "BACKUP DATABASE appdb TO DISK = N'/backup/appdb.bak'",
        "RESTORE VERIFYONLY FROM DISK = N'/backup/appdb.bak'",
        "DBCC CHECKDB (appdb)",
        "KILL 71",
        "TRUNCATE TABLE dbo.NewTable",
        "GRANT SELECT ON dbo.NewTable TO app_user",
        "DENY DELETE ON dbo.NewTable TO app_user",
        "REVOKE UPDATE ON dbo.NewTable FROM app_user",
        "BULK INSERT dbo.Stage FROM '/imports/stage.csv'",
        "EXEC dbo.usp_refresh_reporting",
        "EXEC sys.sp_executesql N'ALTER INDEX ALL ON dbo.Orders REBUILD'",
        "SELECT sp_executesql + N'DROP DATABASE appdb' FROM dbo.CommandNames",
        "USE reporting; UPDATE dbo.Settings SET Value = N'enabled'",
        "SET IDENTITY_INSERT dbo.NewTable ON",
        "SELECT * INTO dbo.UsersCopy FROM dbo.Users",
        "DELETE FROM dbo.Users WHERE UserId = 1",
        "UPDATE dbo.Users SET IsDeleted = 1 WHERE UserId = 1",
        "INSERT dbo.Users (UserId) VALUES (1)",
        "MERGE dbo.Target AS target USING dbo.Source AS source "
        "ON target.Id = source.Id WHEN MATCHED THEN DELETE;",
        "DECLARE @sql nvarchar(max); "
        "SELECT @sql = CommandText FROM dbo.AdminQueue WHERE CommandId = 1; "
        "EXEC(@sql)",
        "DECLARE @message nvarchar(max) = N'DROP DATABASE appdb'; PRINT @message",
        "DECLARE @sql nvarchar(max) = N'DROP DATABASE appdb'; "
        "SET @sql = N'SELECT 1'; EXEC(@sql)",
    ],
)
def test_raw_admin_sql_allows_dba_operations(
    server_config_factory,
    tmp_path: Path,
    sql: str,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.REVIEW)

    payload = policy.preview(_raw_action(sql))

    assert payload["status"] == "dry_run"
    assert _audit_events(tmp_path)[0]["outcome"] == "preview"


@pytest.mark.parametrize(
    "sql",
    [
        "DROP DATABASE appdb",
        "dRoP /* comment between tokens */ DATABASE appdb",
        "SELECT 1; DROP\n-- comment between tokens\nDATABASE appdb",
        "CREATE PROCEDURE dbo.remove_database AS DROP DATABASE appdb",
        "EXEC(N'DROP DATABASE appdb')",
        "EXECUTE(N'DROP' + N' DATABASE appdb')",
        "EXEC sys.sp_executesql N'DROP DATABASE appdb'",
        "EXEC(N'EXEC(N''DROP DATABASE appdb'')')",
        "DECLARE @sql nvarchar(max) = N'DROP' + N' DATABASE appdb' EXEC(@sql)",
        "DECLARE @sql nvarchar(max); SET @sql = N'DROP DATABASE appdb' "
        "EXEC sys.sp_executesql @stmt = @sql",
        "EXEC(N'DROP DATABASE ' + QUOTENAME(@database_name))",
    ],
)
def test_admin_policy_blocks_drop_database(
    server_config_factory,
    tmp_path: Path,
    sql: str,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)

    with pytest.raises(PermissionError, match="DROP DATABASE"):
        policy.preview(_raw_action(sql))

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


def test_admin_policy_allows_drop_database_text_passed_to_normal_procedure(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.REVIEW)

    payload = policy.preview(
        _raw_action("EXEC dbo.usp_log_message @message = N'DROP DATABASE appdb'")
    )

    assert payload["status"] == "dry_run"
    assert _audit_events(tmp_path)[0]["outcome"] == "preview"


@pytest.mark.asyncio
async def test_trusted_generated_actions_cannot_bypass_drop_database_guard(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    action = AdminAction(
        tool_name="generated_fixture",
        database_name="appdb",
        action_type="maintenance",
        sql="DROP DATABASE appdb",
        trusted_generated=True,
    )
    executor = AsyncMock()

    with pytest.raises(PermissionError, match="DROP DATABASE"):
        await policy.execute(action, executor, dry_run=False)

    executor.execute_non_query.assert_not_awaited()


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

    executor.execute_batches.assert_awaited_once_with(
        "appdb",
        "SELECT 1 AS value",
        params=(),
        max_rows=10,
        execution_mode=BatchExecutionMode.ADMIN,
    )
    assert payload["result_sets"] == [
        {
            "columns": ["value"],
            "rows": [{"value": 1}],
            "row_count": 1,
        }
    ]


@pytest.mark.asyncio
async def test_admin_policy_audits_unknown_admin_outcome_and_redacts_error(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    executor = AsyncMock()
    executor.execute_batches = AsyncMock(
        side_effect=AdminBatchOutcomeUnknownError(
            "Connection lost after executing N'SuperSecret-123!'"
        )
    )

    with pytest.raises(AdminBatchOutcomeUnknownError):
        await policy.execute(
            _raw_action("EXEC dbo.RotateCredential N'SuperSecret-123!'"),
            executor,
            dry_run=False,
        )

    events = _audit_events(tmp_path)
    assert [event["outcome"] for event in events] == [
        "apply_started",
        "apply_outcome_unknown",
    ]
    assert all("SuperSecret-123!" not in json.dumps(event) for event in events)


@pytest.mark.asyncio
async def test_admin_policy_audits_timeout_as_unknown_outcome(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    executor = AsyncMock()
    executor.execute_batches = AsyncMock(side_effect=TimeoutError("query timed out"))

    with pytest.raises(TimeoutError):
        await policy.execute(
            _raw_action("ALTER INDEX ALL ON dbo.T REBUILD"),
            executor,
            dry_run=False,
        )

    assert [event["outcome"] for event in _audit_events(tmp_path)] == [
        "apply_started",
        "apply_outcome_unknown",
    ]


@pytest.mark.asyncio
async def test_admin_policy_audits_cancellation_as_unknown_outcome(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    executor = AsyncMock()
    executor.execute_batches = AsyncMock(side_effect=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await policy.execute(
            _raw_action("ALTER INDEX ALL ON dbo.T REBUILD"),
            executor,
            dry_run=False,
        )

    assert [event["outcome"] for event in _audit_events(tmp_path)] == [
        "apply_started",
        "apply_outcome_unknown",
    ]


def test_admin_policy_redacts_preview_and_rollback_literals(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.REVIEW)
    action = AdminAction(
        tool_name="rotate_fixture",
        database_name="appdb",
        action_type="maintenance",
        sql="EXEC dbo.RotateCredential N'SuperSecret-123!' -- private ticket",
        rollback_sql="EXEC dbo.RotateCredential N'OldSecret-456!'",
        trusted_generated=True,
    )

    payload = policy.preview(action)

    assert "SuperSecret-123!" not in payload["sql_preview"]
    assert "private ticket" not in payload["sql_preview"]
    assert "OldSecret-456!" not in payload["rollback_sql"]
    assert all("Secret" not in json.dumps(event) for event in _audit_events(tmp_path))
