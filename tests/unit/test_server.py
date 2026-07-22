from __future__ import annotations

import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from azure_sql_mcp.artifacts import ExplainPlanArtifact
from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.config import ServerConfig
from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.config import TransportConfig
from azure_sql_mcp.config import TransportMode
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.server import async_main
from azure_sql_mcp.server import AzureSqlMcpApplication


def make_config(
    access_mode: AccessMode = AccessMode.RESTRICTED,
    *,
    tool_timeout_seconds: float = 45,
    tool_groups: frozenset[ToolGroup] | None = None,
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
        trust_server_certificate=False,
        tenant_id=None,
        client_id=None,
        client_secret=None,
        transport=TransportConfig(
            mode=TransportMode.STDIO,
            host="127.0.0.1",
            port=8000,
        ),
        tool_groups=tool_groups or frozenset({ToolGroup.ALL}),
        log_level="INFO",
        mcp_bearer_token=None,
        write_policy=WritePolicy.DISABLED,
        audit_dir="/tmp/azure-sql-mcp-test-audit",
        audit_full_sql=False,
        remote_admin_enabled=False,
        performance_state_dir=":memory:",
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
        "tune_query",
        "benchmark_query_rewrite",
        "start_performance_case",
        "collect_performance_evidence",
        "get_performance_case",
        "start_tuning_session",
        "add_tuning_candidate",
        "benchmark_tuning_candidate",
        "benchmark_index_candidate",
        "finalize_tuning_session",
        "compare_query_results",
        "compare_plan_summaries",
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
        "get_connection_pool_stats",
        # Phase 22: Diagnostic Query Parity
        "get_database_configuration",
        "get_storage_diagnostics",
        "get_connection_diagnostics",
        "get_top_cached_queries",
        "get_cached_routine_stats",
        "get_object_index_diagnostics",
        # Phase 13: Statistics & Plan Cache
        "check_statistics_health",
        "get_plan_cache_analysis",
        "get_query_compilation_stats",
        # Phase 14: Parameter Sniffing & Regression
        "detect_parameter_sniffing",
        "detect_regressed_queries",
        "get_query_parameter_buckets",
        "compare_query_plans",
        "get_forced_plans",
        "plan_health_review",
        "plan_enforcer_tick",
        "review_plan_enforcement",
        "dry_run_plan_action",
        "prepare_plan_action",
        "analyze_db_health",
    }

    assert tools["list_databases"].annotations.readOnlyHint is True
    assert tools["list_databases"].annotations.openWorldHint is False
    assert tools["search_objects"].annotations.idempotentHint is True
    assert tools["get_dependencies"].annotations.openWorldHint is True
    assert tools["execute_sql"].annotations.idempotentHint is False
    assert tools["analyze_db_health"].annotations.idempotentHint is True
    assert tools["get_database_configuration"].annotations.readOnlyHint is True
    assert tools["get_storage_diagnostics"].annotations.readOnlyHint is True
    assert tools["get_connection_diagnostics"].annotations.readOnlyHint is True
    assert tools["get_top_cached_queries"].annotations.readOnlyHint is True
    assert tools["get_cached_routine_stats"].annotations.readOnlyHint is True
    assert tools["get_object_index_diagnostics"].annotations.readOnlyHint is True


def test_registers_resources_and_prompts(app: AzureSqlMcpApplication) -> None:
    assert set(app.mcp._resource_manager._templates) == {
        "azuresql://{database}/schemas",
        "azuresql://{database}/{schema}/tables",
        "azuresql://{database}/{schema}/{table}",
        "azuresql://{database}/{schema}/views",
        "azuresql://{database}/{schema}/procedures",
        "azuresql-artifact://{artifact_id}",
    }
    assert set(app.mcp._prompt_manager._prompts) == {
        "analyze-slow-queries",
        "review-index-health",
        "explore-schema",
        "compare-schemas",
        "troubleshoot-performance",
    }


def test_tools_advertise_structured_output_schemas(app: AzureSqlMcpApplication) -> None:
    for name in (
        "list_databases",
        "execute_sql",
        "explain_query",
        "get_database_configuration",
        "get_storage_diagnostics",
        "get_connection_diagnostics",
        "get_top_cached_queries",
        "get_cached_routine_stats",
        "get_object_index_diagnostics",
    ):
        schema = app.mcp._tool_manager._tools[name].fn_metadata.output_schema
        assert schema is not None
        assert schema["type"] == "object"


def test_registered_query_regression_input_schemas(app: AzureSqlMcpApplication) -> None:
    tools = app.mcp._tool_manager._tools

    for name in ("detect_regressed_queries", "get_forced_plans"):
        schema = tools[name].fn_metadata.arg_model.model_json_schema()
        window_schema = schema["properties"]["window_minutes"]
        assert window_schema["type"] == "integer"
        assert window_schema["default"] == 1440
        assert window_schema["minimum"] == 1

    index_schema = tools["analyze_query_indexes"].fn_metadata.arg_model.model_json_schema()
    queries_schema = index_schema["properties"]["queries"]
    assert queries_schema["type"] == "array"
    assert queries_schema["items"] == {"type": "string"}
    assert queries_schema["minItems"] == 1
    assert queries_schema["maxItems"] == 10
    assert "queries" in index_schema["required"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "service_method"),
    (
        ("detect_regressed_queries", "detect_regressed_queries"),
        ("get_forced_plans", "get_forced_plans"),
    ),
)
async def test_registered_query_regression_tools_forward_window_minutes(
    app: AzureSqlMcpApplication,
    tool_name: str,
    service_method: str,
) -> None:
    service_call = AsyncMock(return_value={"tool": tool_name})
    setattr(app.query_regression, service_method, service_call)

    payload = await app.mcp._tool_manager.call_tool(
        tool_name,
        {"database_name": "appdb", "window_minutes": 90},
    )

    assert payload == {"tool": tool_name}
    service_call.assert_awaited_once_with("appdb", 90)


@pytest.mark.asyncio
async def test_registered_analyze_query_indexes_forwards_queries_array(
    app: AzureSqlMcpApplication,
) -> None:
    app._analyze_query_indexes = AsyncMock(  # type: ignore[method-assign]
        return_value={"queries_analyzed": 1}
    )

    payload = await app.mcp._tool_manager.call_tool(
        "analyze_query_indexes",
        {"database_name": "appdb", "queries": ["SELECT 1"]},
    )

    assert payload == {"queries_analyzed": 1}
    app._analyze_query_indexes.assert_awaited_once_with(
        "appdb", ["SELECT 1"], False, None
    )


def test_diagnostic_tools_are_performance_group_and_available_restricted() -> None:
    app = AzureSqlMcpApplication(
        make_config(tool_groups=frozenset({ToolGroup.PERFORMANCE}))
    )
    tools = app.mcp._tool_manager._tools

    for name in (
        "get_database_configuration",
        "get_storage_diagnostics",
        "get_connection_diagnostics",
        "get_top_cached_queries",
        "get_cached_routine_stats",
        "get_object_index_diagnostics",
    ):
        assert name in tools
        assert tools[name].annotations.readOnlyHint is True
        assert tools[name].annotations.destructiveHint is False

    assert "execute_tsql_unrestricted" not in tools


def test_remote_transport_hides_admin_tools_without_remote_admin_opt_in() -> None:
    config = replace(
        make_config(access_mode=AccessMode.UNRESTRICTED),
        transport=TransportConfig(
            mode=TransportMode.STREAMABLE_HTTP,
            host="127.0.0.1",
            port=8000,
        ),
        mcp_bearer_token="token",
        remote_admin_enabled=False,
    )
    app = AzureSqlMcpApplication(config)

    tools = app.mcp._tool_manager._tools

    assert "execute_tsql_unrestricted" not in tools
    assert "apply_plan_action" not in tools
    assert "rebuild_index" not in tools


def test_unrestricted_dba_tool_advertises_execution_contract() -> None:
    config = replace(
        make_config(access_mode=AccessMode.UNRESTRICTED),
        write_policy=WritePolicy.APPLY,
    )
    app = AzureSqlMcpApplication(config)

    tool = app.mcp._tool_manager._tools["execute_tsql_unrestricted"]
    description = tool.description
    input_schema = tool.fn_metadata.arg_model.model_json_schema()

    assert "statically recoverable DROP DATABASE" in description
    assert "assembled only at runtime cannot be proven or blocked" in description
    assert "one submission with no retry" in description
    assert "isolated connection that is discarded" in description
    assert "drains every result set" in description
    assert "GO is a client batch separator" in description
    assert "Do not include the client-side GO separator" in (
        input_schema["properties"]["sql"]["description"]
    )
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is True
    assert tool.annotations.idempotentHint is False
    assert tool.annotations.openWorldHint is True


@pytest.mark.asyncio
async def test_run_tool_formats_errors_from_callback(app: AzureSqlMcpApplication) -> None:
    async def boom(_: str) -> dict[str, str]:
        raise RuntimeError("boom")

    response = await app._run_tool("sample_tool", None, boom)

    assert response["code"] == "tool_error"
    assert response["message"] == "boom"


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

    assert response["code"] == "tool_error"
    assert "secret!" not in response["message"]

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

    assert response["code"] == "tool_error"
    assert "secret!" not in response["message"]

    extra = logged["extra"]
    assert isinstance(extra, dict)
    assert extra["error_type"] == "RuntimeError"
    assert "secret!" not in str(extra["error"])
    assert "UID=sa" not in str(extra["error"])
    assert "prod.database.windows.net" not in str(extra["error"])


def test_truncate_rows_enforces_row_limit(app: AzureSqlMcpApplication) -> None:
    """Fetches use row_limit + 1 to detect truncation; row_count must describe
    the rows actually returned, not include the sentinel row (row_limit=2)."""
    payload = app._truncate_rows(
        {
            "rows": [
                {"id": 1},
                {"id": 2},
                {"id": 3},
            ]
        }
    )

    assert payload["row_count"] == 2
    assert payload["truncated"] is True
    assert len(payload["rows"]) == 2

    exact = app._truncate_rows({"rows": [{"id": 1}, {"id": 2}]})
    assert exact["truncated"] is False
    assert exact["row_count"] == 2
    assert payload["rows"] == [{"id": 1}, {"id": 2}]


def test_format_error_returns_serialized_error_payload(app: AzureSqlMcpApplication) -> None:
    response = app._format_error("bad_request", "invalid input")

    assert response["code"] == "bad_request"
    assert response["message"] == "invalid input"


@pytest.mark.asyncio
async def test_run_tool_uses_configured_database_when_missing(app: AzureSqlMcpApplication) -> None:
    observed: list[str] = []

    async def capture(database_name: str) -> dict[str, str]:
        observed.append(database_name)
        return {"database_name": database_name}

    response = await app._run_tool("sample_tool", None, capture)

    assert observed == ["appdb"]
    assert response["database_name"] == "appdb"


@pytest.mark.asyncio
async def test_run_tool_returns_timeout_error() -> None:
    app = AzureSqlMcpApplication(make_config(tool_timeout_seconds=0.01))

    async def slow(_: str) -> dict[str, str]:
        await asyncio.sleep(0.05)
        return {"status": "ok"}

    response = await app._run_tool("slow_tool", None, slow)

    assert response["code"] == "timeout"
    assert "timed out after 0.01s" in response["message"]


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


@pytest.mark.asyncio
async def test_explain_query_omits_raw_xml_by_default(app: AzureSqlMcpApplication) -> None:
    app.plans.explain_query = AsyncMock(
        return_value=ExplainPlanArtifact(
            database_name="appdb",
            analyze=False,
            summary={"statement_count": 1},
            raw_xml="<ShowPlanXML />",
        )
    )

    payload = await app._explain_query("appdb", "SELECT 1", analyze=False)

    assert "raw_xml" not in payload
    assert payload["raw_xml_length"] == len("<ShowPlanXML />")
    assert payload["raw_xml_resource_uri"].startswith("azuresql-artifact://showplan-xml-")
    artifact_id = payload["raw_xml_resource"]["artifact_id"]
    assert app.artifacts.get(artifact_id).text == "<ShowPlanXML />"


@pytest.mark.asyncio
async def test_explain_query_can_include_raw_xml_when_requested(app: AzureSqlMcpApplication) -> None:
    app.plans.explain_query = AsyncMock(
        return_value=ExplainPlanArtifact(
            database_name="appdb",
            analyze=False,
            summary={"statement_count": 1},
            raw_xml="<ShowPlanXML />",
        )
    )

    payload = await app._explain_query(
        "appdb",
        "SELECT 1",
        analyze=False,
        include_raw_xml=True,
    )

    assert payload["raw_xml"] == "<ShowPlanXML />"


@pytest.mark.asyncio
async def test_tune_query_returns_structured_evidence_pack(app: AzureSqlMcpApplication) -> None:
    app.performance_workflows.start_case = Mock(
        return_value=Mock(case_id="case-1")
    )
    app._collect_performance_evidence = AsyncMock(  # type: ignore[method-assign]
        return_value={"outcome": "healthy", "evidence": {"evidence_id": "ev-1"}}
    )
    app.performance_workflows.start_session = Mock(
        return_value={"session_id": "session-1"}
    )

    payload = await app._tune_query(
        "appdb",
        "SELECT id FROM dbo.Orders",
        analyze=True,
        auto_bind_params=False,
        include_raw_xml=False,
        window_minutes=60,
    )

    assert payload["database_name"] == "appdb"
    assert payload["query_hash"]
    assert payload["performance_case_id"] == "case-1"
    assert payload["tuning_session_id"] == "session-1"
    assert payload["evidence"]["outcome"] == "healthy"
    assert "concrete rewrites" in payload["next_step"]
    assert "No database changes" in payload["scripts"]["rollback"]


@pytest.mark.asyncio
async def test_tune_query_binds_explicit_parameters_once(app: AzureSqlMcpApplication) -> None:
    app.param_binding.bind_parameters = AsyncMock(
        return_value={
            "original_sql": "SELECT @OrderId",
            "bound_sql": "DECLARE @OrderId int = 42;\nSELECT @OrderId",
            "parameters": [
                {
                    "name": "@OrderId",
                    "value": "42",
                    "source": "explicit",
                    "data_type": "int",
                }
            ],
        }
    )
    app.performance_workflows.start_case = Mock(return_value=Mock(case_id="case-2"))
    app._collect_performance_evidence = AsyncMock(return_value={"outcome": "partial"})  # type: ignore[method-assign]
    app.performance_workflows.start_session = Mock(
        return_value={"session_id": "session-2"}
    )

    payload = await app._tune_query(
        "appdb",
        "SELECT @OrderId",
        analyze=False,
        auto_bind_params=False,
        include_raw_xml=False,
        window_minutes=60,
        parameter_values={"OrderId": 42},
    )

    assert payload["parameter_binding"]["parameters"][0]["source"] == "explicit"
    app.param_binding.bind_parameters.assert_awaited_once_with(
        "appdb",
        "SELECT @OrderId",
        parameter_values={"OrderId": 42},
    )


@pytest.mark.asyncio
async def test_tune_query_auto_bind_params_survives_validation(app: AzureSqlMcpApplication) -> None:
    """Regression: auto-bound DECLARE/SET + SELECT batches must pass the read-only
    validator end-to-end. Only the executor is mocked; param binding, the
    validator, and the plans service run for real."""
    app.executor.fetch_all = AsyncMock(return_value=[])  # type: ignore[method-assign]
    effective_sql, binding = await app._prepare_query(
        "appdb",
        "SELECT * FROM dbo.Users WHERE UserId = @UserId",
        True,
    )

    assert binding is not None
    assert binding["parameters"][0]["name"] == "@UserId"
    assert "DECLARE" in effective_sql.upper()
    assert app.validator.validate_read_only(effective_sql).normalized_sql


@pytest.mark.asyncio
async def test_benchmark_query_rewrite_reports_sample_equivalence(app: AzureSqlMcpApplication) -> None:
    app.performance_workflows.start_case = Mock(return_value=Mock(case_id="case-b1"))
    app.performance_workflows.start_session = Mock(
        return_value={"session_id": "session-b1"}
    )
    app.performance_workflows.add_candidate = Mock(
        return_value={"candidate_id": "candidate-b1"}
    )
    app.performance_workflows.benchmark_candidate = AsyncMock(
        return_value={
            "classification": "improved",
            "equivalence": [{"status": "match", "proven_for_parameter_case": True}],
            "executions": 4,
        }
    )

    payload = await app._benchmark_query_rewrite(
        "appdb",
        "SELECT id FROM dbo.Orders",
        "SELECT id FROM dbo.Orders",
        analyze=True,
        auto_bind_params=False,
        include_raw_xml=False,
    )

    assert payload["classification"] == "improved"
    assert payload["equivalence"][0]["status"] == "match"
    assert payload["winning_sql"] == "SELECT id FROM dbo.Orders"
    assert payload["performance_case_id"] == "case-b1"


@pytest.mark.asyncio
async def test_benchmark_query_rewrite_runs_k_reports_median_and_spread(
    app: AzureSqlMcpApplication,
) -> None:
    app.performance_workflows.start_case = Mock(return_value=Mock(case_id="case-b2"))
    app.performance_workflows.start_session = Mock(
        return_value={"session_id": "session-b2"}
    )
    app.performance_workflows.add_candidate = Mock(
        return_value={"candidate_id": "candidate-b2"}
    )
    app.performance_workflows.benchmark_candidate = AsyncMock(
        return_value={"classification": "neutral", "executions": 8, "equivalence": []}
    )

    payload = await app._benchmark_query_rewrite(
        "appdb",
        "SELECT id FROM dbo.Orders",
        "SELECT id FROM dbo.Orders",
        analyze=True,
        auto_bind_params=False,
        include_raw_xml=False,
        runs=3,
    )

    assert payload["classification"] == "neutral"
    assert (
        app.performance_workflows.benchmark_candidate.await_args.kwargs[
            "runs_override"
        ]
        == 3
    )


@pytest.mark.asyncio
async def test_benchmark_can_compare_unordered_result_samples(
    app: AzureSqlMcpApplication,
) -> None:
    app.performance_workflows.start_case = Mock(return_value=Mock(case_id="case-b3"))
    app.performance_workflows.start_session = Mock(
        return_value={"session_id": "session-b3"}
    )
    app.performance_workflows.add_candidate = Mock(
        return_value={"candidate_id": "candidate-b3"}
    )
    app.performance_workflows.benchmark_candidate = AsyncMock(
        return_value={
            "classification": "neutral",
            "equivalence": [{"status": "match", "order_compared": False}],
        }
    )

    payload = await app._benchmark_query_rewrite(
        "appdb",
        "SELECT id FROM dbo.Orders",
        "SELECT id FROM dbo.Orders",
        analyze=True,
        auto_bind_params=False,
        include_raw_xml=False,
        compare_order=False,
    )

    assert payload["equivalence"][0]["status"] == "match"
    assert (
        app.performance_workflows.benchmark_candidate.await_args.kwargs[
            "compare_order"
        ]
        is False
    )


@pytest.mark.asyncio
async def test_analyze_query_indexes_scopes_explicit_values_per_query(
    app: AzureSqlMcpApplication,
) -> None:
    app.param_binding.bind_parameters = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            {"parameters": [], "bound_sql": "SELECT 1"},
            {"parameters": [], "bound_sql": "SELECT 2"},
        ]
    )
    app.query_index_analysis.analyze_queries = AsyncMock(  # type: ignore[method-assign]
        return_value={"queries_analyzed": 2}
    )
    queries = ["SELECT @First", "SELECT @Second"]

    result = await app._analyze_query_indexes(
        "appdb",
        queries,
        parameter_values={"First": 1, "Second": 2},
    )

    assert result == {"queries_analyzed": 2}
    assert app.param_binding.bind_parameters.await_args_list[0].kwargs["parameter_values"] == {
        "first": 1
    }
    assert app.param_binding.bind_parameters.await_args_list[1].kwargs["parameter_values"] == {
        "second": 2
    }


@pytest.mark.asyncio
async def test_benchmark_query_rewrite_rejects_out_of_range_runs(
    app: AzureSqlMcpApplication,
) -> None:
    for bad in (0, 4, -1):
        with pytest.raises(ValueError, match="runs"):
            await app._benchmark_query_rewrite(
                "appdb", "SELECT 1", "SELECT 1",
                analyze=True, auto_bind_params=False, include_raw_xml=False, runs=bad,
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


@pytest.mark.asyncio
async def test_tune_history_prefers_query_hash(app: AzureSqlMcpApplication) -> None:
    """History must match by plan query_hash when available — text matching
    fails for parameterized queries because stored text uses @P1 naming."""
    app.query_store.get_query_history_by_hash = AsyncMock(
        return_value={"matches": [{"query_id": 7}]}
    )
    app.query_store.get_query_history_by_text = AsyncMock()

    plan = {"summary": {"statements": [{"query_hash": "0x90FC7E5399EA52A5"}]}}
    history = await app._query_store_history_for_plan("appdb", "SELECT 1", plan, 60)

    assert history["matched_by"] == "query_hash"
    assert history["matches"] == [{"query_id": 7}]
    app.query_store.get_query_history_by_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_tune_history_falls_back_to_original_sql_text(app: AzureSqlMcpApplication) -> None:
    """Without a usable hash (or with no hash matches), fall back to text
    matching using the ORIGINAL sql — the bound DECLARE batch never matches."""
    app.query_store.get_query_history_by_hash = AsyncMock(return_value={"matches": []})
    app.query_store.get_query_history_by_text = AsyncMock(return_value={"matches": []})

    plan = {"summary": {"statements": [{"query_hash": "0x90FC7E5399EA52A5"}]}}
    original_sql = "SELECT * FROM dbo.Users WHERE UserId = @UserId"
    history = await app._query_store_history_for_plan("appdb", original_sql, plan, 60)

    assert history["matched_by"] == "text"
    text_call = app.query_store.get_query_history_by_text.await_args
    assert text_call.args[1] == original_sql
