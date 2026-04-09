from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.config import ServerConfig
from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.config import TransportConfig
from azure_sql_mcp.config import TransportMode
from azure_sql_mcp.server import async_main
from azure_sql_mcp.server import AzureSqlMcpApplication


def make_config(
    access_mode: AccessMode = AccessMode.RESTRICTED,
    *,
    tool_timeout_seconds: float = 45,
) -> ServerConfig:
    return ServerConfig(
        server="server.database.windows.net",
        default_database="appdb",
        allowed_databases=("appdb",),
        auth_mode=AuthMode.ENTRA_DEFAULT,
        access_mode=access_mode,
        query_timeout_seconds=30,
        row_limit=2,
        pool_size=5,
        max_retries=3,
        tool_timeout_seconds=tool_timeout_seconds,
        log_format="text",
        username=None,
        password=None,
        tenant_id=None,
        client_id=None,
        client_secret=None,
        transport=TransportConfig(
            mode=TransportMode.STDIO,
            host="127.0.0.1",
            port=8000,
        ),
        tool_groups=frozenset({ToolGroup.ALL}),
        log_level="INFO",
    )


@pytest.fixture
def app() -> AzureSqlMcpApplication:
    return AzureSqlMcpApplication(make_config())


def test_registers_expected_tools(app: AzureSqlMcpApplication) -> None:
    tools = app.mcp._tool_manager._tools

    assert set(tools) == {
        "list_databases",
        "check_capabilities",
        "list_schemas",
        "list_objects",
        "search_objects",
        "get_object_details",
        "get_dependencies",
        "get_table_stats",
        "capture_schema_snapshot",
        "compare_schemas",
        "generate_migration_script",
        "get_active_sessions",
        "execute_sql",
        "explain_query",
        "get_top_queries",
        "analyze_query_indexes",
        "analyze_workload_indexes",
        "analyze_index_recommendations",
        "optimize_indexes",
        # Phase 9: Wait Statistics
        "get_wait_stats",
        "get_query_wait_stats",
        "get_currently_waiting_tasks",
        # Phase 10: Lock & Transaction
        "get_lock_details",
        "get_open_transactions",
        "get_deadlock_history",
        # Phase 11: Tempdb & Memory
        "get_tempdb_usage",
        "get_tempdb_space_breakdown",
        "get_memory_grants",
        # Phase 12: I/O & Resource Governance
        "get_io_stats",
        "get_resource_limits",
        "get_resource_stats_history",
        # Phase 13: Statistics & Plan Cache
        "check_statistics_health",
        "get_plan_cache_analysis",
        "get_query_compilation_stats",
        # Phase 14: Parameter Sniffing & Regression
        "detect_parameter_sniffing",
        "detect_regressed_queries",
        "compare_query_plans",
        "get_forced_plans",
        "analyze_db_health",
    }

    assert tools["list_databases"].annotations.readOnlyHint is True
    assert tools["list_databases"].annotations.openWorldHint is False
    assert tools["search_objects"].annotations.idempotentHint is True
    assert tools["get_dependencies"].annotations.openWorldHint is True
    assert tools["execute_sql"].annotations.idempotentHint is False
    assert tools["analyze_db_health"].annotations.idempotentHint is True


def test_registers_resources_and_prompts(app: AzureSqlMcpApplication) -> None:
    assert set(app.mcp._resource_manager._templates) == {
        "azuresql://{database}/schemas",
        "azuresql://{database}/{schema}/tables",
        "azuresql://{database}/{schema}/{table}",
        "azuresql://{database}/{schema}/views",
        "azuresql://{database}/{schema}/procedures",
    }
    assert set(app.mcp._prompt_manager._prompts) == {
        "analyze-slow-queries",
        "review-index-health",
        "explore-schema",
        "compare-schemas",
        "troubleshoot-performance",
    }


@pytest.mark.asyncio
async def test_run_tool_formats_errors_from_callback(app: AzureSqlMcpApplication) -> None:
    async def boom(_: str) -> dict[str, str]:
        raise RuntimeError("boom")

    response = await app._run_tool("sample_tool", None, boom)

    assert len(response) == 1
    payload = response[0].text
    assert '"code": "tool_error"' in payload
    assert '"message": "boom"' in payload


@pytest.mark.asyncio
async def test_run_tool_logs_sanitized_errors(monkeypatch: pytest.MonkeyPatch, app: AzureSqlMcpApplication) -> None:
    logged: dict[str, object] = {}

    def capture(message: str, *args, **kwargs) -> None:
        logged["message"] = message
        logged["extra"] = kwargs.get("extra")

    monkeypatch.setattr("azure_sql_mcp.server.logger.error", capture)

    async def boom(_: str) -> dict[str, str]:
        raise RuntimeError(
            "SERVER=tcp:prod.database.windows.net;DATABASE=appdb;UID=sa;PWD=secret!;"
        )

    response = await app._run_tool("sample_tool", None, boom)

    assert '"code": "tool_error"' in response[0].text
    assert "secret!" not in response[0].text

    extra = logged["extra"]
    assert isinstance(extra, dict)
    assert extra["error_type"] == "RuntimeError"
    assert "secret!" not in str(extra["error"])
    assert "UID=sa" not in str(extra["error"])
    assert "prod.database.windows.net" not in str(extra["error"])


@pytest.mark.asyncio
async def test_run_database_pair_tool_logs_sanitized_errors(
    monkeypatch: pytest.MonkeyPatch,
    app: AzureSqlMcpApplication,
) -> None:
    logged: dict[str, object] = {}

    def capture(message: str, *args, **kwargs) -> None:
        logged["message"] = message
        logged["extra"] = kwargs.get("extra")

    monkeypatch.setattr("azure_sql_mcp.server.logger.error", capture)

    async def boom(_: str, __: str) -> dict[str, str]:
        raise RuntimeError(
            "SERVER=tcp:prod.database.windows.net;DATABASE=appdb;UID=sa;PWD=secret!;"
        )

    response = await app._run_database_pair_tool("sample_pair_tool", "appdb", "appdb", boom)

    assert '"code": "tool_error"' in response[0].text
    assert "secret!" not in response[0].text

    extra = logged["extra"]
    assert isinstance(extra, dict)
    assert extra["error_type"] == "RuntimeError"
    assert "secret!" not in str(extra["error"])
    assert "UID=sa" not in str(extra["error"])
    assert "prod.database.windows.net" not in str(extra["error"])


def test_truncate_rows_enforces_row_limit(app: AzureSqlMcpApplication) -> None:
    payload = app._truncate_rows(
        {
            "rows": [
                {"id": 1},
                {"id": 2},
                {"id": 3},
            ]
        }
    )

    assert payload["row_count"] == 3
    assert payload["truncated"] is True
    assert payload["rows"] == [{"id": 1}, {"id": 2}]


def test_format_error_returns_serialized_error_payload(app: AzureSqlMcpApplication) -> None:
    response = app._format_error("bad_request", "invalid input")

    assert len(response) == 1
    assert '"code": "bad_request"' in response[0].text
    assert '"message": "invalid input"' in response[0].text


@pytest.mark.asyncio
async def test_run_tool_uses_configured_database_when_missing(app: AzureSqlMcpApplication) -> None:
    observed: list[str] = []

    async def capture(database_name: str) -> dict[str, str]:
        observed.append(database_name)
        return {"database_name": database_name}

    response = await app._run_tool("sample_tool", None, capture)

    assert observed == ["appdb"]
    assert '"database_name": "appdb"' in response[0].text


@pytest.mark.asyncio
async def test_run_tool_returns_timeout_error() -> None:
    app = AzureSqlMcpApplication(make_config(tool_timeout_seconds=0.01))

    async def slow(_: str) -> dict[str, str]:
        await asyncio.sleep(0.05)
        return {"status": "ok"}

    response = await app._run_tool("slow_tool", None, slow)

    assert '"code": "timeout"' in response[0].text
    assert "timed out after 0.01s" in response[0].text


@pytest.mark.asyncio
async def test_explain_query_rejects_hypothetical_indexes_in_read_only_tool(app: AzureSqlMcpApplication) -> None:
    with pytest.raises(ValueError, match="disabled on explain_query"):
        await app._explain_query(
            "appdb",
            "SELECT 1",
            analyze=False,
            hypothetical_indexes=[
                {"schema": "dbo", "table": "Orders", "columns": ["CustomerId"]},
            ],
        )


def test_quote_identifier_escapes_closing_brackets(app: AzureSqlMcpApplication) -> None:
    assert app._quote_identifier("na]me") == "[na]]me]"


@pytest.mark.asyncio
async def test_run_closes_pool_on_shutdown(app: AzureSqlMcpApplication, monkeypatch: pytest.MonkeyPatch) -> None:
    run_stdio = AsyncMock()
    close_all = AsyncMock()

    monkeypatch.setattr(app.mcp, "run_stdio_async", run_stdio)
    monkeypatch.setattr(app.pool, "close_all", close_all)

    await app.run()

    run_stdio.assert_awaited_once()
    close_all.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_main_configures_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    config = make_config()
    configure = Mock()
    run = AsyncMock()

    monkeypatch.setattr("azure_sql_mcp.server.load_server_config", lambda argv=None: config)
    monkeypatch.setattr("azure_sql_mcp.server.configure_logging", configure)
    monkeypatch.setattr("azure_sql_mcp.server.AzureSqlMcpApplication.run", run)

    await async_main([])

    configure.assert_called_once_with(config.log_level, config.log_format)
    run.assert_awaited_once()
