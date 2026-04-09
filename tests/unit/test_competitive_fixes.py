"""Tests for competitive analysis fixes (R1-R3, R7).

R1: Execution tools (rebuild_index, update_statistics, force_query_plan, kill_session)
R2: Memory-safe row fetching (max_rows parameter)
R3: Tool grouping / selective exposure
R7: Logging on swallowed exceptions
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.connection import AzureSqlExecutor
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.server import AzureSqlMcpApplication


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_app(
    access_mode: AccessMode = AccessMode.UNRESTRICTED,
    tool_groups: frozenset[ToolGroup] = frozenset({ToolGroup.ALL}),
    server_config_factory=None,
) -> AzureSqlMcpApplication:
    """Build an AzureSqlMcpApplication with the given config overrides."""
    if server_config_factory is not None:
        config = server_config_factory(
            access_mode=access_mode,
            tool_groups=tool_groups,
        )
    else:
        # Fallback: import and build directly
        from azure_sql_mcp.config import (
            AuthMode,
            ServerConfig,
            TransportConfig,
            TransportMode,
        )
        config = ServerConfig(
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
            tenant_id=None,
            client_id=None,
            client_secret=None,
            transport=TransportConfig(
                mode=TransportMode.STDIO,
                host="127.0.0.1",
                port=8000,
            ),
            tool_groups=tool_groups,
            log_level="INFO",
        )
    return AzureSqlMcpApplication(config)


# ===========================================================================
# R1: Execution tools are registered in UNRESTRICTED mode
# ===========================================================================

class TestExecutionToolsRegistration:
    def test_admin_tools_registered_in_unrestricted_mode(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        tools = {t.name for t in app.mcp._tool_manager.list_tools()}
        assert "rebuild_index" in tools
        assert "update_statistics" in tools
        assert "force_query_plan" in tools
        assert "kill_session" in tools

    def test_admin_tools_not_registered_in_restricted_mode(self) -> None:
        app = _make_app(access_mode=AccessMode.RESTRICTED)
        tools = {t.name for t in app.mcp._tool_manager.list_tools()}
        assert "rebuild_index" not in tools
        assert "update_statistics" not in tools
        assert "force_query_plan" not in tools
        assert "kill_session" not in tools

    def test_kill_session_is_destructive(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        tools = {t.name: t for t in app.mcp._tool_manager.list_tools()}
        assert tools["kill_session"].annotations.destructiveHint is True

    def test_rebuild_index_is_idempotent(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        tools = {t.name: t for t in app.mcp._tool_manager.list_tools()}
        assert tools["rebuild_index"].annotations.idempotentHint is True


class TestExecutionToolImplementation:
    @pytest.mark.asyncio
    async def test_rebuild_index_generates_correct_sql(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        app.executor.execute_non_query = AsyncMock(return_value=0)

        result = await app._rebuild_index(
            "appdb", "dbo", "Orders", "IX_Orders_Date", "REBUILD", True,
        )

        app.executor.execute_non_query.assert_awaited_once()
        sql = app.executor.execute_non_query.call_args[0][1]
        assert "ALTER INDEX [IX_Orders_Date]" in sql
        assert "REBUILD" in sql
        assert "ONLINE = ON" in sql
        assert result["status"] == "completed"

    @pytest.mark.asyncio
    async def test_rebuild_index_reorganize_no_online(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        app.executor.execute_non_query = AsyncMock(return_value=0)

        result = await app._rebuild_index(
            "appdb", "dbo", "Orders", "IX_Orders_Date", "REORGANIZE", True,
        )

        sql = app.executor.execute_non_query.call_args[0][1]
        assert "REORGANIZE" in sql
        assert "ONLINE" not in sql

    @pytest.mark.asyncio
    async def test_rebuild_index_rejects_invalid_operation(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        with pytest.raises(ValueError, match="REBUILD or REORGANIZE"):
            await app._rebuild_index(
                "appdb", "dbo", "Orders", "IX_1", "DROP", False,
            )

    @pytest.mark.asyncio
    async def test_update_statistics_all(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        app.executor.execute_non_query = AsyncMock(return_value=0)

        result = await app._update_statistics("appdb", "dbo", "Orders", None, None)

        sql = app.executor.execute_non_query.call_args[0][1]
        assert sql == "UPDATE STATISTICS [dbo].[Orders]"
        assert result["statistic"] == "(all)"

    @pytest.mark.asyncio
    async def test_update_statistics_with_sample(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        app.executor.execute_non_query = AsyncMock(return_value=0)

        await app._update_statistics("appdb", "dbo", "Orders", "IX_Stat", 50)

        sql = app.executor.execute_non_query.call_args[0][1]
        assert "[IX_Stat]" in sql
        assert "SAMPLE 50 PERCENT" in sql

    @pytest.mark.asyncio
    async def test_update_statistics_rejects_bad_sample(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        with pytest.raises(ValueError, match="sample_percent"):
            await app._update_statistics("appdb", "dbo", "T", None, 0)

    @pytest.mark.asyncio
    async def test_force_query_plan_force(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        app.executor.execute_non_query = AsyncMock(return_value=0)

        result = await app._force_query_plan("appdb", 42, 7, False)

        sql = app.executor.execute_non_query.call_args[0][1]
        assert "sp_query_store_force_plan" in sql
        assert result["action"] == "forced"

    @pytest.mark.asyncio
    async def test_force_query_plan_unforce(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        app.executor.execute_non_query = AsyncMock(return_value=0)

        result = await app._force_query_plan("appdb", 42, 7, True)

        sql = app.executor.execute_non_query.call_args[0][1]
        assert "sp_query_store_unforce_plan" in sql
        assert result["action"] == "unforced"

    @pytest.mark.asyncio
    async def test_kill_session_issues_kill(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        app.executor.execute_non_query = AsyncMock(return_value=0)

        result = await app._kill_session("appdb", 123)

        sql = app.executor.execute_non_query.call_args[0][1]
        assert sql == "KILL 123"
        assert result["status"] == "kill_issued"

    @pytest.mark.asyncio
    async def test_kill_session_rejects_system_spid(self) -> None:
        app = _make_app(access_mode=AccessMode.UNRESTRICTED)
        with pytest.raises(ValueError, match="system sessions"):
            await app._kill_session("appdb", 10)


# ===========================================================================
# R2: Memory-safe row fetching
# ===========================================================================

class TestMaxRowsFetching:
    def test_consume_batches_uses_fetchmany_when_max_rows_set(
        self, sample_server_config,
    ) -> None:
        executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

        cursor = MagicMock()
        cursor.description = [("id",), ("name",)]
        cursor.fetchmany.return_value = [
            (1, "alpha"),
            (2, "beta"),
        ]
        cursor.nextset.return_value = False

        results = executor._consume_batches(cursor, max_rows=5)

        cursor.fetchmany.assert_called_once_with(5)
        cursor.fetchall.assert_not_called()
        assert len(results) == 1
        assert len(results[0].rows) == 2

    def test_consume_batches_uses_fetchall_when_no_max_rows(
        self, sample_server_config,
    ) -> None:
        executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

        cursor = MagicMock()
        cursor.description = [("id",)]
        cursor.fetchall.return_value = [(1,), (2,), (3,)]
        cursor.nextset.return_value = False

        results = executor._consume_batches(cursor)

        cursor.fetchall.assert_called_once()
        cursor.fetchmany.assert_not_called()
        assert len(results[0].rows) == 3

    @pytest.mark.asyncio
    async def test_fetch_all_passes_max_rows_through(
        self, sample_server_config,
    ) -> None:
        pool = SimpleNamespace(
            acquire=AsyncMock(),
            release=AsyncMock(),
            discard=AsyncMock(),
        )
        executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)
        executor.execute_batches = AsyncMock(
            return_value=[QueryResult(columns=("v",), rows=[{"v": 1}])],
        )

        await executor.fetch_all("appdb", "SELECT 1", max_rows=10)

        executor.execute_batches.assert_awaited_once()
        _, kwargs = executor.execute_batches.call_args
        assert kwargs["max_rows"] == 10


# ===========================================================================
# execute_session: session-scoped statement execution
# ===========================================================================
#
# Required for SET options like SHOWPLAN_XML that:
#   1. must be alone in their batch (so they cannot be concatenated with the
#      query under inspection), AND
#   2. only persist for the lifetime of the session that ran them, so the
#      SET and the query MUST share a single pooled connection.

class TestExecuteSession:
    def test_execute_session_runs_each_statement_on_one_connection(
        self, sample_server_config,
    ) -> None:
        executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

        cursors: list[MagicMock] = []

        def make_cursor() -> MagicMock:
            cursor = MagicMock()
            cursor.description = None
            cursor.nextset.return_value = False
            cursors.append(cursor)
            return cursor

        connection = MagicMock()
        connection.cursor.side_effect = make_cursor

        results = executor._execute_session_with_connection(
            connection,
            "appdb",
            ("SET SHOWPLAN_XML ON", "SELECT 1", "SET SHOWPLAN_XML OFF"),
            None,
        )

        # All three statements ran against the SAME connection object — this
        # is what makes session-scoped SET options work.
        assert connection.cursor.call_count == 3
        assert len(results) == 3
        # Each cursor should receive exactly one statement (no batching).
        executed = [c.execute.call_args[0][0] for c in cursors]
        assert executed == [
            "SET SHOWPLAN_XML ON",
            "SELECT 1",
            "SET SHOWPLAN_XML OFF",
        ]

    def test_execute_session_returns_per_statement_results(
        self, sample_server_config,
    ) -> None:
        executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

        # First statement returns no rows (SET); second returns plan XML; third
        # returns no rows (SET OFF).
        plan_cursor = MagicMock()
        plan_cursor.description = [("plan_xml",)]
        plan_cursor.fetchall.return_value = [("<ShowPlanXML/>",)]
        plan_cursor.nextset.return_value = False

        empty_cursor_a = MagicMock()
        empty_cursor_a.description = None
        empty_cursor_a.nextset.return_value = False

        empty_cursor_b = MagicMock()
        empty_cursor_b.description = None
        empty_cursor_b.nextset.return_value = False

        connection = MagicMock()
        connection.cursor.side_effect = [empty_cursor_a, plan_cursor, empty_cursor_b]

        results = executor._execute_session_with_connection(
            connection,
            "appdb",
            ("SET SHOWPLAN_XML ON", "SELECT 1", "SET SHOWPLAN_XML OFF"),
            None,
        )

        # Index 1 corresponds to the user query — that's where the plan lives.
        assert results[0] == []
        assert len(results[1]) == 1
        assert results[1][0].rows == [{"plan_xml": "<ShowPlanXML/>"}]
        assert results[2] == []

    @pytest.mark.asyncio
    async def test_execute_session_acquires_a_single_connection(
        self, sample_server_config,
    ) -> None:
        connection = MagicMock()
        cursor = MagicMock()
        cursor.description = None
        cursor.nextset.return_value = False
        connection.cursor.return_value = cursor

        pool = SimpleNamespace(
            acquire=AsyncMock(return_value=connection),
            release=AsyncMock(),
            discard=AsyncMock(),
        )
        executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

        await executor.execute_session("appdb", ["SET A ON", "SELECT 1", "SET A OFF"])

        # Critical invariant: the pool was hit ONCE — all three statements
        # share the same physical connection so SET options persist.
        pool.acquire.assert_awaited_once_with("appdb")
        pool.release.assert_awaited_once()
        pool.discard.assert_not_awaited()
        assert connection.cursor.call_count == 3


# ===========================================================================
# R3: Tool grouping
# ===========================================================================

class TestToolGrouping:
    def test_core_only_registers_core_tools(self) -> None:
        app = _make_app(
            access_mode=AccessMode.RESTRICTED,
            tool_groups=frozenset({ToolGroup.CORE}),
        )
        tools = {t.name for t in app.mcp._tool_manager.list_tools()}
        assert "execute_sql" in tools
        assert "list_schemas" in tools
        assert "analyze_db_health" in tools
        # Performance tools should be pruned
        assert "get_wait_stats" not in tools
        assert "detect_parameter_sniffing" not in tools
        # Schema tools should be pruned
        assert "compare_schemas" not in tools

    def test_performance_group_includes_performance_tools(self) -> None:
        app = _make_app(
            access_mode=AccessMode.RESTRICTED,
            tool_groups=frozenset({ToolGroup.PERFORMANCE}),
        )
        tools = {t.name for t in app.mcp._tool_manager.list_tools()}
        assert "get_wait_stats" in tools
        assert "detect_parameter_sniffing" in tools
        # Core tools should be pruned
        assert "execute_sql" not in tools

    def test_multiple_groups_union(self) -> None:
        app = _make_app(
            access_mode=AccessMode.RESTRICTED,
            tool_groups=frozenset({ToolGroup.CORE, ToolGroup.SCHEMA}),
        )
        tools = {t.name for t in app.mcp._tool_manager.list_tools()}
        assert "execute_sql" in tools
        assert "compare_schemas" in tools
        assert "get_wait_stats" not in tools

    def test_all_group_registers_everything(self) -> None:
        app_all = _make_app(
            access_mode=AccessMode.RESTRICTED,
            tool_groups=frozenset({ToolGroup.ALL}),
        )
        app_core = _make_app(
            access_mode=AccessMode.RESTRICTED,
            tool_groups=frozenset({ToolGroup.CORE}),
        )
        all_tools = {t.name for t in app_all.mcp._tool_manager.list_tools()}
        core_tools = {t.name for t in app_core.mcp._tool_manager.list_tools()}
        assert len(all_tools) > len(core_tools)

    def test_is_tool_enabled_returns_false_for_disabled(self) -> None:
        app = _make_app(tool_groups=frozenset({ToolGroup.CORE}))
        assert app.config.is_tool_enabled("execute_sql") is True
        assert app.config.is_tool_enabled("get_wait_stats") is False
        assert app.config.is_tool_enabled("compare_schemas") is False

    def test_is_tool_enabled_unknown_tool_always_enabled(self) -> None:
        app = _make_app(tool_groups=frozenset({ToolGroup.CORE}))
        assert app.config.is_tool_enabled("some_unknown_tool") is True

    def test_admin_tools_require_both_unrestricted_and_admin_group(self) -> None:
        # UNRESTRICTED + CORE only — admin tools not in group
        app = _make_app(
            access_mode=AccessMode.UNRESTRICTED,
            tool_groups=frozenset({ToolGroup.CORE}),
        )
        tools = {t.name for t in app.mcp._tool_manager.list_tools()}
        assert "rebuild_index" not in tools
        assert "execute_sql" in tools


# ===========================================================================
# R7: Logging on swallowed exceptions
# ===========================================================================

class TestExceptionLogging:
    @pytest.mark.asyncio
    async def test_param_binding_logs_type_resolution_failure(
        self, sample_server_config,
    ) -> None:
        from azure_sql_mcp.param_binding import ParameterBindingService

        executor = MagicMock()
        executor.fetch_all = AsyncMock(side_effect=Exception("permission denied"))
        service = ParameterBindingService(executor)

        with patch("azure_sql_mcp.param_binding.logger") as mock_logger:
            result = await service._resolve_from_stats(
                "appdb", "Customers", "customer_name",
            )

        assert result is None
        mock_logger.warning.assert_called_once()
        assert "customer_name" in mock_logger.warning.call_args[0][1]

    @pytest.mark.asyncio
    async def test_param_binding_logs_histogram_failure(
        self, sample_server_config,
    ) -> None:
        from azure_sql_mcp.param_binding import ParameterBindingService

        executor = MagicMock()
        # First call (type resolution) succeeds; second call (histogram) fails
        executor.fetch_all = AsyncMock(
            side_effect=[
                [{"data_type": "nvarchar", "max_length": 100, "stats_id": 1,
                  "table_name": "Customers", "schema_name": "dbo"}],
                Exception("throttled"),
            ],
        )
        service = ParameterBindingService(executor)

        with patch("azure_sql_mcp.param_binding.logger") as mock_logger:
            result = await service._resolve_from_stats(
                "appdb", "Customers", "customer_name",
            )

        assert result["source"] == "type_fallback"
        mock_logger.warning.assert_called_once()
        assert "histogram" in mock_logger.warning.call_args[0][0]
