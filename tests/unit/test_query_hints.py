"""Query Store hint validation is the one free-text surface in plan enforcement, so the
grammar is pinned test by test: every allowed shape passes, everything else — injection
attempts, unknown hints, malformed clauses — is rejected with a clear reason."""

from __future__ import annotations

from pathlib import Path

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.config import ServerConfig
from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.config import TransportConfig
from azure_sql_mcp.config import TransportMode
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.query_hints import validate_query_hints
from azure_sql_mcp.server import AzureSqlMcpApplication


def make_config(
    tmp_path: Path,
    access_mode: AccessMode = AccessMode.RESTRICTED,
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
        write_policy=WritePolicy.DISABLED,
        audit_dir=str(tmp_path / "audit"),
        audit_full_sql=False,
        remote_admin_enabled=False,
    )


# --- allowed shapes ---

@pytest.mark.parametrize(
    "hints",
    [
        "OPTION(RECOMPILE)",
        "OPTION ( RECOMPILE )",
        "option(recompile)",
        "OPTION(OPTIMIZE FOR UNKNOWN)",
        "OPTION(OPTIMIZE FOR (@region = 'EU'))",
        "OPTION(OPTIMIZE FOR (@customer_id = 42))",
        "OPTION(OPTIMIZE FOR (@start = N'2026-01-01', @rows UNKNOWN))",
        "OPTION(MAXDOP 1)",
        "OPTION(MAX_GRANT_PERCENT = 10)",
        "OPTION(MIN_GRANT_PERCENT = 2.5)",
        "OPTION(FAST 100)",
        "OPTION(MAXRECURSION 500)",
        "OPTION(KEEP PLAN)",
        "OPTION(KEEPFIXED PLAN)",
        "OPTION(FORCE ORDER)",
        "OPTION(EXPAND VIEWS)",
        "OPTION(IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX)",
        "OPTION(NO_PERFORMANCE_SPOOL)",
        "OPTION(HASH GROUP)",
        "OPTION(ORDER GROUP)",
        "OPTION(MERGE UNION)",
        "OPTION(LOOP JOIN)",
        "OPTION(HASH JOIN)",
        "OPTION(USE HINT('DISABLE_PARAMETER_SNIFFING'))",
        "OPTION(USE HINT('FORCE_LEGACY_CARDINALITY_ESTIMATION', 'DISABLE_OPTIMIZER_ROWGOAL'))",
        "OPTION(USE HINT('QUERY_OPTIMIZER_COMPATIBILITY_LEVEL_160'))",
        "OPTION(RECOMPILE, MAXDOP 4)",
        "OPTION(FORCE ORDER, USE HINT('ENABLE_QUERY_OPTIMIZER_HOTFIXES'))",
    ],
)
def test_allowed_hint_shapes_pass(hints: str) -> None:
    assert validate_query_hints(hints) == hints.strip()


# --- rejected shapes ---

@pytest.mark.parametrize(
    ("hints", "reason_fragment"),
    [
        ("", "non-empty"),
        ("   ", "non-empty"),
        ("RECOMPILE", "OPTION"),                                   # missing wrapper
        ("OPTION()", "at least one hint"),
        ("OPTION(RECOMPILE); DROP TABLE dbo.orders", "terminators"),
        ("OPTION(RECOMPILE) -- comment", "terminators"),
        ("OPTION(RECOMPILE /* x */)", "terminators"),
        ("OPTION(TABLE HINT(t, FORCESEEK))", "unsupported"),       # not QS-settable
        ("OPTION(MAXDOP 'high')", "unsupported"),
        ("OPTION(OPTIMIZE FOR (@p = 'it''s'))", "unsupported"),    # embedded quote
        ("OPTION(OPTIMIZE FOR (@p = SELECT 1))", "unsupported"),
        ("OPTION(USE HINT('XP_CMDSHELL'))", "USE HINT"),
        ("OPTION(USE HINT('QUERY_OPTIMIZER_COMPATIBILITY_LEVEL_ABC'))", "USE HINT"),
        ("OPTION(RECOMPILE, DROP INDEX)", "unsupported"),
        ("OPTION([RECOMPILE])", "brackets"),
    ],
)
def test_rejected_hint_shapes_raise(hints: str, reason_fragment: str) -> None:
    with pytest.raises(ValueError, match=reason_fragment):
        validate_query_hints(hints)


# --- tool registration and policy behavior ---

def test_hint_tools_register_only_in_unrestricted_mode(tmp_path: Path) -> None:
    restricted = AzureSqlMcpApplication(make_config(tmp_path))
    assert "set_query_store_hints" not in restricted.mcp._tool_manager._tools
    assert "clear_query_store_hints" not in restricted.mcp._tool_manager._tools

    unrestricted = AzureSqlMcpApplication(
        make_config(tmp_path, access_mode=AccessMode.UNRESTRICTED)
    )
    tools = unrestricted.mcp._tool_manager._tools
    assert "set_query_store_hints" in tools
    assert "clear_query_store_hints" in tools
    assert tools["set_query_store_hints"].annotations.readOnlyHint is False
    assert tools["set_query_store_hints"].annotations.idempotentHint is True


@pytest.mark.asyncio
async def test_set_hints_dry_run_previews_with_rollback(tmp_path: Path) -> None:
    app = AzureSqlMcpApplication(
        make_config(tmp_path, access_mode=AccessMode.UNRESTRICTED)
    )
    payload = await app._set_query_store_hints(
        "appdb", 42, "OPTION(RECOMPILE)", dry_run=True,
    )
    assert payload["status"] == "dry_run"
    assert payload["query_id"] == 42
    assert payload["query_hints"] == "OPTION(RECOMPILE)"
    assert "sp_query_store_clear_hints" in payload["rollback_sql"]
    assert "sp_query_store_set_hints" in payload["sql_preview"]


@pytest.mark.asyncio
async def test_set_hints_rejects_bad_hints_before_any_policy_work(
    tmp_path: Path,
) -> None:
    app = AzureSqlMcpApplication(
        make_config(tmp_path, access_mode=AccessMode.UNRESTRICTED)
    )
    with pytest.raises(ValueError, match="unsupported"):
        await app._set_query_store_hints(
            "appdb", 42, "OPTION(EVIL_HINT)", dry_run=True,
        )
    with pytest.raises(ValueError, match="query_id"):
        await app._set_query_store_hints(
            "appdb", 0, "OPTION(RECOMPILE)", dry_run=True,
        )


@pytest.mark.asyncio
async def test_set_hints_execution_blocked_without_apply_policy(
    tmp_path: Path,
) -> None:
    # Direct mutation is blocked before any write policy can be consulted.
    app = AzureSqlMcpApplication(
        make_config(tmp_path, access_mode=AccessMode.UNRESTRICTED)
    )
    assert app.config.write_policy is not WritePolicy.APPLY
    with pytest.raises(PermissionError, match="prepared workflow"):
        await app._set_query_store_hints(
            "appdb", 42, "OPTION(RECOMPILE)", dry_run=False,
        )


@pytest.mark.asyncio
async def test_clear_hints_dry_run_previews(tmp_path: Path) -> None:
    app = AzureSqlMcpApplication(
        make_config(tmp_path, access_mode=AccessMode.UNRESTRICTED)
    )
    payload = await app._clear_query_store_hints("appdb", 42, dry_run=True)
    assert payload["status"] == "dry_run"
    assert payload["action"] == "hints_cleared"
    assert "sp_query_store_clear_hints" in payload["sql_preview"]


@pytest.mark.asyncio
async def test_direct_set_hints_cannot_bypass_prepared_workflow(
    tmp_path: Path,
) -> None:
    app = AzureSqlMcpApplication(
        make_config(tmp_path, access_mode=AccessMode.UNRESTRICTED)
    )
    executed: dict = {}

    async def fake_execute(action, executor, *, dry_run, max_rows=None):
        executed["sql"] = action.sql
        executed["params"] = action.params
        return {"status": "completed", "dry_run": False, "audit_id": "x",
                "database_name": action.database_name, "tool_name": action.tool_name,
                "action_type": action.action_type, "sql_preview": action.sql,
                "sql_hash": "h"}

    app.admin_policy.execute = fake_execute  # type: ignore[method-assign]
    with pytest.raises(PermissionError, match="prepared workflow"):
        await app._set_query_store_hints(
            "appdb", 42, "OPTION(RECOMPILE)", dry_run=False
        )
    assert executed == {}
