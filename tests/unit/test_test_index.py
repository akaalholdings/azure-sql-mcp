"""create_test_index / drop_test_index manage ONLY disposable, prefix-namespaced test
indexes: the prefix rule and strict identifier validation are what make them safe to
expose as admin tools, so both are pinned test by test."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import ServerConfig
from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.config import TransportConfig
from azure_sql_mcp.config import TransportMode
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.server import TEST_INDEX_DEFINITION_PROPERTY
from azure_sql_mcp.server import TEST_INDEX_OWNER_PROPERTY
from azure_sql_mcp.server import TEST_INDEX_PREFIX


def make_config(
    access_mode: AccessMode = AccessMode.RESTRICTED,
    *,
    write_policy: WritePolicy = WritePolicy.DISABLED,
    profile: McpProfile | None = None,
) -> ServerConfig:
    return ServerConfig(
        server="server.database.windows.net",
        default_database="appdb",
        allowed_databases=("appdb",),
        auth_mode=AuthMode.ENTRA_DEFAULT,
        access_mode=access_mode,
        query_timeout_seconds=30,
        row_limit=200,
        pool_size=5,
        max_retries=3,
        tool_timeout_seconds=45,
        log_format="text",
        username=None,
        password=None,
        trust_server_certificate=False,
        tenant_id=None,
        client_id=None,
        client_secret=None,
        transport=TransportConfig(mode=TransportMode.STDIO, host="127.0.0.1", port=8000),
        tool_groups=frozenset({ToolGroup.ALL}),
        log_level="INFO",
        mcp_bearer_token=None,
        write_policy=write_policy,
        audit_dir="/tmp/azure-sql-mcp-test-audit",
        audit_full_sql=False,
        remote_admin_enabled=False,
        profile=profile,
    )


def unrestricted_app() -> AzureSqlMcpApplication:
    return AzureSqlMcpApplication(make_config(access_mode=AccessMode.UNRESTRICTED))


def test_tools_register_only_in_unrestricted_mode() -> None:
    restricted = AzureSqlMcpApplication(make_config())
    assert "create_test_index" not in restricted.mcp._tool_manager._tools
    assert "drop_test_index" not in restricted.mcp._tool_manager._tools

    tools = unrestricted_app().mcp._tool_manager._tools
    assert "create_test_index" in tools
    assert "drop_test_index" in tools
    assert tools["drop_test_index"].annotations.destructiveHint is True
    assert tools["create_test_index"].annotations.readOnlyHint is False


@pytest.mark.asyncio
async def test_create_dry_run_builds_ddl_with_rollback() -> None:
    payload = await unrestricted_app()._create_test_index(
        "appdb", "dbo", "Shipments",
        f"{TEST_INDEX_PREFIX}Shipments_ShipDate_a1b2",
        key_columns=["ShipDate", "StatusCode DESC"],
        include_columns=["CustomerID", "TotalDue"],
        online=True,
        dry_run=True,
    )
    assert payload["status"] == "dry_run"
    sql = payload["sql_preview"]
    assert "CREATE NONCLUSTERED INDEX" in sql
    assert "[ShipDate] ASC, [StatusCode] DESC" in sql
    assert "INCLUDE ([CustomerID], [TotalDue])" in sql
    assert "WITH (ONLINE = ON)" in sql
    assert payload["rollback_sql"] == (
        f"DROP INDEX [{TEST_INDEX_PREFIX}Shipments_ShipDate_a1b2] "
        "ON [dbo].[Shipments];"
    )


@pytest.mark.asyncio
async def test_create_without_online_and_includes() -> None:
    payload = await unrestricted_app()._create_test_index(
        "appdb", "dbo", "Orders",
        f"{TEST_INDEX_PREFIX}Orders_Status",
        key_columns=["Status"],
        include_columns=None,
        online=False,
        dry_run=True,
    )
    assert "ONLINE" not in payload["sql_preview"]
    assert "INCLUDE" not in payload["sql_preview"]


@pytest.mark.asyncio
async def test_create_refuses_non_test_prefix() -> None:
    with pytest.raises(ValueError, match="test prefix"):
        await unrestricted_app()._create_test_index(
            "appdb", "dbo", "Orders", "IX_Orders_Status",
            key_columns=["Status"], include_columns=None, online=True, dry_run=True,
        )


@pytest.mark.asyncio
async def test_drop_refuses_non_test_prefix() -> None:
    # The safety property: this tool can NEVER touch a real index.
    with pytest.raises(ValueError, match="test prefix"):
        await unrestricted_app()._drop_test_index(
            "appdb", "dbo", "Orders", "PK_Orders", dry_run=True,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("schema", "table", "columns"),
    [
        ("dbo; DROP TABLE x", "Orders", ["Status"]),
        ("dbo", "Orders]", ["Status"]),
        ("dbo", "Orders", ["Status; --"]),
        ("dbo", "Orders", ["Status DELETE"]),   # bad direction keyword
        ("dbo", "Orders", []),                   # no key columns
    ],
)
async def test_create_rejects_malformed_identifiers(schema, table, columns) -> None:
    with pytest.raises(ValueError):
        await unrestricted_app()._create_test_index(
            "appdb", schema, table, f"{TEST_INDEX_PREFIX}X",
            key_columns=columns, include_columns=None, online=True, dry_run=True,
        )


@pytest.mark.asyncio
async def test_execution_blocked_without_apply_policy() -> None:
    app = unrestricted_app()
    assert app.config.write_policy is not WritePolicy.APPLY
    with pytest.raises(PermissionError, match="preview-only"):
        await app._create_test_index(
            "appdb", "dbo", "Orders", f"{TEST_INDEX_PREFIX}Orders_Status",
            key_columns=["Status"], include_columns=None, online=True, dry_run=False,
        )


def test_live_test_index_database_requires_sandbox_profile_and_policy() -> None:
    app = AzureSqlMcpApplication(
        make_config(
            access_mode=AccessMode.UNRESTRICTED,
            write_policy=WritePolicy.APPLY,
            profile=McpProfile.SANDBOX,
        )
    )
    app.database_policy = DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "allow_test_indexes": True,
                    "allow_plan_apply": False,
                    "max_benchmark_executions": 80,
                }
            },
        }
    )
    app._require_test_index_database("appdb")


@pytest.mark.asyncio
async def test_live_create_and_drop_require_managed_lease_workflow() -> None:
    app = AzureSqlMcpApplication(
        make_config(
            access_mode=AccessMode.UNRESTRICTED,
            write_policy=WritePolicy.APPLY,
            profile=McpProfile.SANDBOX,
        )
    )

    with pytest.raises(PermissionError, match="preview-only"):
        await app._create_test_index(
            "appdb",
            "dbo",
            "Orders",
            f"{TEST_INDEX_PREFIX}Orders_Status",
            key_columns=["Status"],
            include_columns=None,
            online=True,
            dry_run=False,
        )
    with pytest.raises(PermissionError, match="preview-only"):
        await app._drop_test_index(
            "appdb",
            "dbo",
            "Orders",
            f"{TEST_INDEX_PREFIX}Orders_Status",
            dry_run=False,
        )


@pytest.mark.asyncio
async def test_managed_index_ddl_uses_private_atomic_ownership_marker() -> None:
    app = AzureSqlMcpApplication(
        make_config(
            access_mode=AccessMode.UNRESTRICTED,
            write_policy=WritePolicy.APPLY,
            profile=McpProfile.SANDBOX,
        )
    )
    app.database_policy = DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "allow_test_indexes": True,
                    "max_benchmark_executions": 80,
                }
            },
        }
    )
    app.admin_policy.execute = AsyncMock(  # type: ignore[method-assign]
        return_value={"status": "completed"}
    )
    owner = "index-lease-private-owner-1234"

    await app._create_test_index(
        "appdb",
        "dbo",
        "Orders",
        f"{TEST_INDEX_PREFIX}Orders_Status",
        key_columns=["Status"],
        include_columns=None,
        online=True,
        dry_run=False,
        workflow_managed=True,
        idempotency_key="create-owner-marker",
        ownership_proof=owner,
    )
    create_action = app.admin_policy.execute.await_args.args[0]
    assert "BEGIN TRANSACTION" in create_action.sql
    assert "sp_addextendedproperty" in create_action.sql
    assert TEST_INDEX_OWNER_PROPERTY in create_action.sql
    assert TEST_INDEX_DEFINITION_PROPERTY in create_action.sql
    assert owner not in create_action.sql
    assert create_action.params[0] == owner
    assert len(create_action.params) == 2
    definition_fingerprint = create_action.params[1]
    assert isinstance(definition_fingerprint, str)
    assert create_action.rollback_params == (owner, definition_fingerprint, None)

    await app._drop_test_index(
        "appdb",
        "dbo",
        "Orders",
        f"{TEST_INDEX_PREFIX}Orders_Status",
        dry_run=False,
        workflow_managed=True,
        idempotency_key="drop-owner-marker",
        ownership_proof=owner,
        expected_definition_fingerprint=definition_fingerprint,
        expected_index_id=2,
    )
    drop_action = app.admin_policy.execute.await_args.args[0]
    assert "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE" in drop_action.sql
    assert "WITH (TABLOCK, HOLDLOCK)" in drop_action.sql
    assert "WITH (UPDLOCK, HOLDLOCK)" in drop_action.sql
    assert "BEGIN TRY" in drop_action.sql
    assert "XACT_STATE()" in drop_action.sql
    assert "IF NOT EXISTS" in drop_action.sql
    assert TEST_INDEX_OWNER_PROPERTY in drop_action.sql
    assert TEST_INDEX_DEFINITION_PROPERTY in drop_action.sql
    assert "ep.major_id = @object_id" in drop_action.sql
    assert "ep.minor_id = @index_id" in drop_action.sql
    assert "@expected_index_id" in drop_action.sql
    assert "DROP INDEX" in drop_action.sql
    assert owner not in drop_action.sql
    assert drop_action.params == (owner, definition_fingerprint, 2)
