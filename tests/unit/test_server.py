from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import CallToolRequest
from mcp.types import CallToolRequestParams

from azure_sql_mcp.artifacts import ExplainPlanArtifact
from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import ServerConfig
from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.config import TransportConfig
from azure_sql_mcp.config import TransportMode
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.learning_contracts import DecisionRecordV1
from azure_sql_mcp.learning_store import LearningStoreError
from azure_sql_mcp.performance_contracts import EvidenceEnvelopeV1
from azure_sql_mcp.query_identity import legacy_database_fingerprint
from azure_sql_mcp.performance_workflows import database_fingerprint
from azure_sql_mcp.performance_workflows import fingerprint_json
from azure_sql_mcp.server import async_main
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.tuning_sessions import InvalidTransitionError
from azure_sql_mcp.view_workflows import ViewChangeRequest
from azure_sql_mcp.view_workflows import ViewSnapshot
from azure_sql_mcp.view_workflows import prepared_view_change_state


def make_config(
    tmp_path: Path,
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
        audit_dir=str(tmp_path / "audit"),
        audit_full_sql=False,
        remote_admin_enabled=False,
        performance_state_dir=":memory:",
    )


def make_read_policy(*database_names: str) -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                database_name: {
                    "environment": "test",
                    "allow_read": True,
                }
                for database_name in database_names
            },
        }
    )


@pytest.fixture
def app(tmp_path: Path) -> AzureSqlMcpApplication:
    return AzureSqlMcpApplication(make_config(tmp_path))


def stub_case_preflight(
    app: AzureSqlMcpApplication,
) -> AsyncMock:
    preflight = AsyncMock(
        return_value={
            "contract_version": 2,
            "classification": "direct_snapshot",
            "direct_snapshot_supported": True,
            "coverage_complete": True,
        }
    )
    app._check_equivalence_preflight = preflight  # type: ignore[method-assign]
    return preflight


def test_registers_expected_tools(app: AzureSqlMcpApplication) -> None:
    tools = app.mcp._tool_manager._tools

    assert set(tools) == {
        "check_runtime_status",
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
        "check_equivalence_preflight",
        "start_performance_case",
        "collect_performance_evidence",
        "get_performance_case",
        "start_tuning_session",
        "get_tuning_session",
        "add_tuning_candidate",
        "benchmark_tuning_candidate",
        "benchmark_index_candidate",
        "finalize_tuning_session",
        "compare_query_results",
        "compare_plan_summaries",
        "prepare_view_change",
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
        "record_decision",
        "review_decision",
        "propose_lesson",
        "recall_lessons",
        "list_learning_candidates",
        "create_handoff",
        "get_handoff",
        "resolve_handoff",
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
    tuning_parameters = tools["start_tuning_session"].parameters["properties"]
    assert tuning_parameters["max_candidates"]["default"] == 10
    assert tuning_parameters["execution_limit"]["default"] == 80
    assert tuning_parameters["time_limit_minutes"]["default"] == 20
    decision_properties = tools["record_decision"].parameters["properties"]
    assert {
        "subject_kind",
        "subject_fingerprint",
        "based_on_review_ids",
        "runtime_fingerprint",
        "runtime_compatibility_fingerprint",
    } <= set(decision_properties)
    maintained_skills = [
        "sql-health-triage",
        "sql-optimizer",
        "sql-plan-enforcer",
    ]
    assert decision_properties["skill"]["enum"] == maintained_skills
    assert decision_properties["subject_kind"]["enum"] == [
        "query",
        "plan",
        "incident",
        "database",
    ]
    review_properties = tools["review_decision"].parameters["properties"]
    assert {"counterexamples", "next_observation", "terminal_evidence_refs"} <= set(
        review_properties
    )
    recall_properties = tools["recall_lessons"].parameters["properties"]
    assert {"skill", "skill_version", "runtime_compatibility_fingerprint"} <= set(
        recall_properties
    )
    assert "runtime_fingerprint" not in recall_properties
    assert recall_properties["skill"]["enum"] == maintained_skills
    proposal_properties = tools["propose_lesson"].parameters["properties"]
    assert "next_observation" in proposal_properties
    assert proposal_properties["applicable_skills"]["items"]["enum"] == maintained_skills
    for linked_tool in (
        "analyze_db_health",
        "collect_performance_evidence",
        "benchmark_tuning_candidate",
        "benchmark_index_candidate",
        "finalize_tuning_session",
        "resolve_handoff",
    ):
        assert "decision_id" in tools[linked_tool].parameters["properties"]
    assert "decision_id" not in tools["prepare_plan_action"].parameters["properties"]


def test_unrestricted_plan_action_tools_publish_decision_linkage(
    tmp_path: Path,
) -> None:
    app = AzureSqlMcpApplication(
        make_config(tmp_path, access_mode=AccessMode.UNRESTRICTED)
    )
    tools = app.mcp._tool_manager._tools

    for linked_tool in ("verify_plan_action", "rollback_plan_action"):
        assert "decision_id" in tools[linked_tool].parameters["properties"]


def test_learning_store_failure_preserves_static_tool_surface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "azure_sql_mcp.server.LearningStore",
        Mock(side_effect=LearningStoreError("malformed learning state")),
    )

    app = AzureSqlMcpApplication(make_config(tmp_path))
    tools = app.mcp._tool_manager._tools

    assert app.learning_store is None
    assert app.learning_service is None
    assert "analyze_db_health" in tools
    assert "record_decision" not in tools
    assert "recall_lessons" not in tools


@pytest.mark.asyncio
async def test_registered_tool_arguments_are_strict_and_reject_unknown_fields(
    app: AzureSqlMcpApplication,
) -> None:
    for tool in app.mcp._tool_manager.list_tools():
        argument_model = tool.fn_metadata.arg_model
        assert argument_model.model_config["extra"] == "forbid"
        assert tool.parameters["additionalProperties"] is False
        assert argument_model.model_json_schema()["additionalProperties"] is False

    with pytest.raises(ToolError):
        await app.mcp._tool_manager.call_tool(
            "check_runtime_status",
            {"unexpected": "value"},
        )


@pytest.mark.asyncio
async def test_validation_errors_are_sanitized_invalid_argument_envelopes(
    app: AzureSqlMcpApplication,
) -> None:
    handler = app.mcp._mcp_server.request_handlers[CallToolRequest]
    response = await handler(
        CallToolRequest(
            params=CallToolRequestParams(
                name="check_runtime_status",
                arguments={"unexpected": "caller-secret-value"},
            )
        )
    )

    assert response.root.isError is True
    text = response.root.content[0].text
    payload = json.loads(text)
    assert payload == {
        "ok": False,
        "code": "invalid_arguments",
        "message": "Tool arguments failed validation.",
        "details": {
            "issues": [
                {
                    "path": "unexpected",
                    "code": "extra_forbidden",
                    "message": "Extra arguments are not permitted.",
                }
            ]
        },
    }
    assert "caller-secret-value" not in text
    assert "input_value" not in text
    assert "pydantic" not in text.casefold()


@pytest.mark.asyncio
async def test_stopping_reason_length_error_is_sanitized(
    app: AzureSqlMcpApplication,
) -> None:
    caller_value = "s" * 2001
    handler = app.mcp._mcp_server.request_handlers[CallToolRequest]
    response = await handler(
        CallToolRequest(
            params=CallToolRequestParams(
                name="finalize_tuning_session",
                arguments={
                    "session_id": "session-1",
                    "stopping_reason": caller_value,
                },
            )
        )
    )

    assert response.root.isError is True
    text = response.root.content[0].text
    payload = json.loads(text)
    assert payload == {
        "ok": False,
        "code": "invalid_arguments",
        "message": "Tool arguments failed validation.",
        "details": {
            "issues": [
                {
                    "path": "stopping_reason",
                    "code": "string_too_long",
                    "message": "Value is above the permitted maximum length.",
                }
            ]
        },
    }
    assert caller_value not in text
    assert "input_value" not in text
    assert "pydantic" not in text.casefold()


@pytest.mark.asyncio
async def test_sanitized_tool_error_preserves_candidate_reference_format(
    app: AzureSqlMcpApplication,
) -> None:
    async def reject(_: str) -> dict[str, object]:
        raise ValueError("artifact_ref must start with candidate:")

    with pytest.raises(ToolError) as exc_info:
        await app._run_tool("add_tuning_candidate", "appdb", reject)

    payload = json.loads(str(exc_info.value))
    assert payload["code"] == "tool_error"
    assert payload["message"] == "artifact_ref must start with candidate:"


@pytest.mark.asyncio
async def test_mcp_handler_marks_tool_errors_as_is_error(
    app: AzureSqlMcpApplication,
) -> None:
    handler = app.mcp._mcp_server.request_handlers[CallToolRequest]
    response = await handler(
        CallToolRequest(
            params=CallToolRequestParams(
                name="explain_query",
                arguments={
                    "sql": "SELECT 1",
                    "analyze": False,
                    "hypothetical_indexes": [
                        {
                            "schema": "dbo",
                            "table": "Orders",
                            "columns": ["CustomerId"],
                        }
                    ],
                },
            )
        )
    )

    assert response.root.isError is True
    assert "tool_error" in response.root.content[0].text


@pytest.mark.asyncio
async def test_runtime_status_is_db_free_stable_and_sanitized(
    tmp_path: Path,
) -> None:
    test_auth_values = {
        "user" + "name": "sa",
        "pass" + "word": "test-password",
        "client" + "_secret": "test-client-secret",
        "mcp_bearer" + "_token": "test-token",
    }
    app = AzureSqlMcpApplication(
        replace(
            make_config(tmp_path),
            **test_auth_values,
        )
    )
    first = await app.mcp._tool_manager.call_tool("check_runtime_status", {})
    second = await app.mcp._tool_manager.call_tool("check_runtime_status", {})

    assert first == second
    assert first["startup_timestamp"] == app._startup_timestamp
    assert first["package_version"] == "2.2.1"
    assert first["profile"] is None
    assert first["transport"] == "stdio"
    assert first["tool_groups"] == ["all"]
    assert first["tool_count"] == len(first["tool_names"])
    assert first["tool_names"] == sorted(first["tool_names"])
    assert first["contracts"] == {
        "strict_arguments": True,
        "mcp_errors": True,
        "sanitized_validation_errors": True,
        "performance_only_selection": True,
    }
    assert first["strict_argument_models"] is True
    assert first["mcp_tool_errors"] is True
    assert len(first["runtime_fingerprint"]) == 64
    assert len(first["runtime_compatibility_fingerprint"]) == 64
    assert len(first["tool_schema_fingerprint"]) == 64
    assert len(first["sanitized_config_fingerprint"]) == 64
    assert "server.database.windows.net" not in json.dumps(first)
    assert "test-" not in json.dumps(first)


@pytest.mark.asyncio
async def test_runtime_compatibility_fingerprint_is_restart_stable(
    tmp_path: Path,
) -> None:
    config = replace(make_config(tmp_path), performance_state_dir=str(tmp_path / "state"))
    first_app = AzureSqlMcpApplication(config)
    first = await first_app.mcp._tool_manager.call_tool("check_runtime_status", {})
    first_app.performance_store.close()
    assert first_app.learning_store is not None
    first_app.learning_store.close()

    second_app = AzureSqlMcpApplication(config)
    second = await second_app.mcp._tool_manager.call_tool("check_runtime_status", {})

    assert first["runtime_compatibility_fingerprint"] == second[
        "runtime_compatibility_fingerprint"
    ]
    assert first["runtime_fingerprint"] != second["runtime_fingerprint"]


@pytest.mark.asyncio
async def test_successful_operation_links_terminal_outcome_and_learning_failure_is_nonfatal(
    app: AzureSqlMcpApplication,
) -> None:
    assert app.learning_service is not None
    assert app.learning_store is not None
    scope = app._current_learning_scope("appdb")
    runtime = await app.mcp._tool_manager.call_tool("check_runtime_status", {})
    evidence = app.performance_store.create_evidence(
        EvidenceEnvelopeV1(
            database_fingerprint=scope["database_fingerprint"],
            query_fingerprint="query-fingerprint",
            metrics={"classification": "baseline"},
        )
    )
    decision = app.learning_service.record_decision(
        DecisionRecordV1(
            skill="sql-optimizer",
            skill_version="2.3.0",
            case_id="case-1",
            learning_key="health-check",
            consumed_evidence_refs=(evidence.evidence_id,),
            subject_kind="database",
            subject_fingerprint="subject-fingerprint",
            based_on_review_ids=(),
            tactic="inspect-health",
            expected_result={"class": "available"},
            confidence=0.8,
            uncertainty={"kind": "bounded"},
            evaluator_fingerprint="evaluator-fingerprint",
            runtime_fingerprint=runtime["runtime_fingerprint"],
            runtime_compatibility_fingerprint=runtime[
                "runtime_compatibility_fingerprint"
            ],
            tool_schema_fingerprint=scope["tool_schema_fingerprint"],
            sanitized_config_fingerprint=scope["sanitized_config_fingerprint"],
            scope=scope,
        )
    )

    callback = AsyncMock(
        return_value={
            "status": "ok",
            "evidence_id": evidence.evidence_id,
            "rows": [{"secret": "not persisted"}],
        }
    )
    linked = await app._run_tool(
        "analyze_db_health",
        "appdb",
        callback,
        decision_id=decision.decision_id,
    )

    assert linked["learning_link_status"] == "linked"
    assert str(linked["terminal_link_id"]).startswith("terminal-link-")
    terminal = app.learning_store.get_terminal_link(linked["terminal_link_id"])
    assert terminal["source_tool"] == "analyze_db_health"
    assert terminal["evidence_refs"] == [evidence.evidence_id]
    assert "rows" not in terminal["outcome_summary"]

    app.learning_service.record_terminal_link = Mock(  # type: ignore[method-assign]
        side_effect=RuntimeError("learning store unavailable")
    )
    nonfatal_callback = AsyncMock(return_value={"status": "ok"})
    nonfatal = await app._run_tool(
        "analyze_db_health",
        "appdb",
        nonfatal_callback,
        decision_id=decision.decision_id,
    )
    assert nonfatal["status"] == "ok"
    assert nonfatal["learning_link_status"] == "failed"
    assert nonfatal["terminal_link_id"] is None


@pytest.mark.asyncio
async def test_unknown_decision_does_not_fail_successful_operation(
    app: AzureSqlMcpApplication,
) -> None:
    callback = AsyncMock(return_value={"status": "ok"})

    result = await app._run_tool(
        "analyze_db_health",
        "appdb",
        callback,
        decision_id="decision-does-not-exist",
    )

    callback.assert_awaited_once_with("appdb")
    assert result["status"] == "ok"
    assert result["learning_link_status"] == "failed"
    assert result["terminal_link_id"] is None


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


def test_legacy_state_binding_is_opt_in_for_performance_workflows(
    tmp_path: Path,
) -> None:
    strict_app = AzureSqlMcpApplication(make_config(tmp_path))
    assert strict_app.performance_workflows.allow_legacy_state is False

    legacy_config = replace(
        make_config(tmp_path),
        legacy_state_server_binding="server.database.windows.net",
    )
    legacy_app = AzureSqlMcpApplication(legacy_config)
    assert legacy_app.performance_workflows.allow_legacy_state is True


@pytest.mark.asyncio
async def test_direct_server_fingerprint_check_honours_legacy_binding(
    tmp_path: Path,
) -> None:
    legacy_case = Mock(
        database_fingerprint=legacy_database_fingerprint("appdb"),
    )
    strict_app = AzureSqlMcpApplication(make_config(tmp_path))
    strict_app.performance_store.get_performance_case = Mock(  # type: ignore[method-assign]
        return_value=legacy_case,
    )
    with pytest.raises(PermissionError, match="another database"):
        await strict_app._start_tuning_session(
            "appdb",
            "case-legacy",
            10,
            80,
            20,
            "strict-state-test",
        )

    legacy_config = replace(
        make_config(tmp_path),
        legacy_state_server_binding="server.database.windows.net",
    )
    app = AzureSqlMcpApplication(legacy_config)
    app.performance_store.get_performance_case = Mock(  # type: ignore[method-assign]
        return_value=legacy_case,
    )
    app.performance_workflows.start_session = Mock(
        return_value={"session_id": "session-legacy"}
    )

    payload = await app._start_tuning_session(
        "appdb",
        "case-legacy",
        10,
        80,
        20,
        "legacy-state-test",
    )

    assert payload == {"session_id": "session-legacy"}
    app.performance_workflows.start_session.assert_called_once()


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

    for name in (
        "explain_query",
        "benchmark_query_rewrite",
        "start_performance_case",
        "get_performance_case",
        "start_tuning_session",
        "get_tuning_session",
        "benchmark_tuning_candidate",
        "benchmark_index_candidate",
        "finalize_tuning_session",
        "check_equivalence_preflight",
    ):
        schema = app.mcp._tool_manager._tools[name].fn_metadata.output_schema
        assert schema is not None
        assert "headline" in schema["properties"]


def test_optimizer_inputs_publish_closed_enums(
    app: AzureSqlMcpApplication,
) -> None:
    tools = app.mcp._tool_manager._tools
    assert tools["start_performance_case"].parameters["properties"]["objective"][
        "enum"
    ] == ["elapsed_time", "cpu", "logical_reads", "physical_reads"]
    assert tools["add_tuning_candidate"].parameters["properties"]["strategy"][
        "enum"
    ] == [
        "predicate",
        "join",
        "aggregation",
        "cardinality",
        "index",
        "combined",
        "rewrite_plus_index",
    ]
    for name in ("benchmark_tuning_candidate", "benchmark_index_candidate"):
        assert tools[name].parameters["properties"]["phase"]["enum"] == [
            "screening",
            "finalist",
        ]
    selection = tools["finalize_tuning_session"].parameters["properties"][
        "selection_scope"
    ]
    assert selection["enum"] == ["proven", "performance_only"]
    assert selection["default"] == "proven"
    stopping_reason = tools["finalize_tuning_session"].parameters["properties"][
        "stopping_reason"
    ]
    assert stopping_reason["minLength"] == 1
    assert stopping_reason["maxLength"] == 2000


@pytest.mark.asyncio
async def test_registered_preflight_returns_typed_summary_and_headline(
    app: AzureSqlMcpApplication,
) -> None:
    result = await app.mcp._tool_manager.call_tool(
        "check_equivalence_preflight",
        {
            "sql": "SELECT GETDATE() AS captured_at",
            "database_name": "appdb",
        },
    )

    assert result["classification"] == "proof_contract_required"
    assert result["coverage_complete"] is True
    assert result["functions"][0]["function"] == "GETDATE"
    assert result["headline"] == {
        "classification": "proof_contract_required",
        "coverage_complete": True,
        "direct_snapshot_supported": False,
        "risk_count": 1,
        "unresolved_dependency_count": 0,
    }


@pytest.mark.asyncio
async def test_optimizer_responses_add_stable_headlines(
    app: AzureSqlMcpApplication,
) -> None:
    async def case_callback(_: str) -> dict[str, object]:
        return {
            "case_id": "case-1",
            "metadata": {
                "equivalence_preflight": {
                    "classification": "proof_contract_required",
                    "direct_snapshot_supported": False,
                }
            },
        }

    async def session_callback(_: str) -> dict[str, object]:
        return {
            "session": {
                "session_id": "session-1",
                "status": "screening",
                "time_limit_seconds": 1200,
            },
            "budget": {
                "executions_remaining": 72,
                "candidate_slots_remaining": 8,
            },
        }

    async def benchmark_callback(_: str) -> dict[str, object]:
        return {
            "session_id": "session-1",
            "candidate_id": "candidate-1",
            "classification": "performance_only",
            "objective": "elapsed_time",
            "executions": 8,
            "proof_scope": "performance_only",
            "parameter_results": [
                {
                    "weight": 1,
                    "baseline": {"elapsed_ms": 100.0},
                    "candidate": {"elapsed_ms": 40.0},
                }
            ],
        }

    async def plan_callback(_: str) -> dict[str, object]:
        return {
            "plan_kind": "actual",
            "summary": {
                "statement_count": 1,
                "operator_count": 4,
                "warnings": [{"kind": "spill"}],
                "missing_indexes": [],
            },
        }

    case = await app._run_tool("start_performance_case", "appdb", case_callback)
    session = await app._run_tool("get_tuning_session", "appdb", session_callback)
    benchmark = await app._run_tool(
        "benchmark_tuning_candidate",
        "appdb",
        benchmark_callback,
    )
    plan = await app._run_tool("explain_query", "appdb", plan_callback)

    assert case["headline"] == {
        "case_id": "case-1",
        "classification": "proof_contract_required",
        "proof_scope": "performance_only",
    }
    assert session["headline"] == {
        "session_id": "session-1",
        "status": "screening",
        "time_limit_minutes": 20,
        "executions_remaining": 72,
        "candidate_slots_remaining": 8,
    }
    assert benchmark["headline"] == {
        "session_id": "session-1",
        "candidate_id": "candidate-1",
        "classification": "performance_only",
        "objective": "elapsed_time",
        "metric": "elapsed_ms",
        "relative_improvement_pct": 60.0,
        "parameter_case_count": 1,
        "executions": 8,
        "proof_scope": "performance_only",
    }
    assert plan["headline"] == {
        "plan_kind": "actual",
        "statement_count": 1,
        "operator_count": 4,
        "warning_count": 1,
        "missing_index_count": 0,
    }


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
        "appdb", ["SELECT 1"], False, None, None
    )


def test_diagnostic_tools_are_performance_group_and_available_restricted(
    tmp_path: Path,
) -> None:
    app = AzureSqlMcpApplication(
        make_config(
            tmp_path,
            tool_groups=frozenset({ToolGroup.PERFORMANCE}),
        )
    )
    tools = app.mcp._tool_manager._tools

    assert "check_runtime_status" in tools
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


def test_remote_transport_hides_admin_tools_without_remote_admin_opt_in(
    tmp_path: Path,
) -> None:
    config = replace(
        make_config(tmp_path, access_mode=AccessMode.UNRESTRICTED),
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
    assert not {
        "record_decision",
        "review_decision",
        "propose_lesson",
        "recall_lessons",
        "list_learning_candidates",
        "create_handoff",
        "get_handoff",
        "resolve_handoff",
    } & set(tools)


def test_unrestricted_dba_tool_advertises_execution_contract(
    tmp_path: Path,
) -> None:
    config = replace(
        make_config(tmp_path, access_mode=AccessMode.UNRESTRICTED),
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

    with pytest.raises(ToolError) as error:
        await app._run_tool("sample_tool", None, boom)

    assert json.loads(str(error.value)) == {
        "code": "tool_error",
        "message": "boom",
        "ok": False,
    }


@pytest.mark.asyncio
async def test_run_tool_preserves_intentional_tool_error(
    app: AzureSqlMcpApplication,
) -> None:
    async def reject(_: str) -> dict[str, str]:
        app._raise_tool_error("preview_only", "Apply is disabled.")

    with pytest.raises(ToolError) as error:
        await app._run_tool("sample_tool", None, reject)

    assert json.loads(str(error.value)) == {
        "code": "preview_only",
        "message": "Apply is disabled.",
        "ok": False,
    }


@pytest.mark.parametrize(
    "tool_name",
    (
        "list_schemas",
        "list_objects",
        "search_objects",
        "get_object_details",
        "get_dependencies",
        "get_table_stats",
        "capture_schema_snapshot",
    ),
)
@pytest.mark.asyncio
async def test_catalog_tools_require_allow_read_before_callback(
    app: AzureSqlMcpApplication,
    tool_name: str,
) -> None:
    callback = AsyncMock(return_value={"ok": True})

    with pytest.raises(ToolError) as error:
        await app._run_tool(tool_name, "appdb", callback)

    payload = json.loads(str(error.value))
    assert payload["code"] == "tool_error"
    assert payload["message"] == "Database policy does not permit read access."
    callback.assert_not_awaited()


@pytest.mark.asyncio
async def test_catalog_tool_runs_when_allow_read_is_granted(
    app: AzureSqlMcpApplication,
) -> None:
    app.database_policy = make_read_policy("appdb")
    callback = AsyncMock(return_value={"objects": []})

    result = await app._run_tool("list_objects", "appdb", callback)

    assert result == {"objects": []}
    callback.assert_awaited_once_with("appdb")


@pytest.mark.asyncio
async def test_schema_pair_tools_require_allow_read_for_both_databases(
    tmp_path: Path,
) -> None:
    config = replace(
        make_config(tmp_path),
        allowed_databases=("appdb", "reportingdb"),
    )
    app = AzureSqlMcpApplication(config)
    app.database_policy = make_read_policy("appdb")
    callback = AsyncMock(return_value={"differences": []})

    with pytest.raises(ToolError) as error:
        await app._run_database_pair_tool(
            "compare_schemas",
            "appdb",
            "reportingdb",
            callback,
        )

    payload = json.loads(str(error.value))
    assert payload["message"] == "Database policy does not permit read access."
    callback.assert_not_awaited()


@pytest.mark.parametrize("tool_name", ("compare_schemas", "generate_migration_script"))
@pytest.mark.asyncio
async def test_schema_pair_tools_run_when_both_databases_allow_read(
    tmp_path: Path,
    tool_name: str,
) -> None:
    config = replace(
        make_config(tmp_path),
        allowed_databases=("appdb", "reportingdb"),
    )
    app = AzureSqlMcpApplication(config)
    app.database_policy = make_read_policy("appdb", "reportingdb")
    callback = AsyncMock(return_value={"ok": True})

    result = await app._run_database_pair_tool(
        tool_name,
        "appdb",
        "reportingdb",
        callback,
    )

    assert result == {"ok": True}
    callback.assert_awaited_once_with("appdb", "reportingdb")


@pytest.mark.asyncio
async def test_state_machine_error_keeps_safe_enum_codes_visible(
    app: AzureSqlMcpApplication,
) -> None:
    async def reject(_: str) -> dict[str, str]:
        raise InvalidTransitionError(
            "Session status completed is not in allowed statuses "
            "[finalist_validation, screening]."
        )

    with pytest.raises(ToolError) as error:
        await app._run_tool("benchmark_tuning_candidate", "appdb", reject)

    payload = json.loads(str(error.value))
    assert payload["message"] == (
        "Session status completed is not in allowed statuses "
        "[finalist_validation, screening]."
    )


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

    with pytest.raises(ToolError) as error:
        await app._run_tool("sample_tool", None, boom)

    error_payload = json.loads(str(error.value))
    assert error_payload["code"] == "tool_error"
    assert "secret!" not in error_payload["message"]

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

    with pytest.raises(ToolError) as error:
        await app._run_database_pair_tool("sample_pair_tool", "appdb", "appdb", boom)

    error_payload = json.loads(str(error.value))
    assert error_payload["code"] == "tool_error"
    assert "secret!" not in error_payload["message"]

    extra = logged["extra"]
    assert isinstance(extra, dict)
    assert extra["error_type"] == "RuntimeError"
    assert "secret!" not in str(extra["error"])
    assert "UID=sa" not in str(extra["error"])
    assert "prod.database.windows.net" not in str(extra["error"])


@pytest.mark.asyncio
async def test_run_database_pair_tool_preserves_intentional_tool_error(
    app: AzureSqlMcpApplication,
) -> None:
    async def reject(_: str, __: str) -> dict[str, str]:
        app._raise_tool_error("preview_only", "Apply is disabled.")

    with pytest.raises(ToolError) as error:
        await app._run_database_pair_tool(
            "sample_pair_tool",
            "appdb",
            "appdb",
            reject,
        )

    assert json.loads(str(error.value)) == {
        "code": "preview_only",
        "message": "Apply is disabled.",
        "ok": False,
    }


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


def test_raise_tool_error_serializes_error_payload(app: AzureSqlMcpApplication) -> None:
    with pytest.raises(ToolError) as raised:
        app._raise_tool_error("bad_request", "invalid input")

    response = json.loads(str(raised.value))
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


def test_benchmark_tool_timeout_scales_to_policy_execution_budget(
    app: AzureSqlMcpApplication,
) -> None:
    app.database_policy = DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "max_benchmark_executions": 80,
                }
            },
        }
    )

    assert app._timeout_for_tool("benchmark_tuning_candidate", "appdb") == (
        80 * app.config.query_timeout_seconds + 5 * 60
    )


@pytest.mark.asyncio
async def test_run_tool_returns_timeout_error(tmp_path: Path) -> None:
    app = AzureSqlMcpApplication(
        make_config(tmp_path, tool_timeout_seconds=0.01)
    )

    async def slow(_: str) -> dict[str, str]:
        await asyncio.sleep(0.05)
        return {"status": "ok"}

    with pytest.raises(ToolError) as error:
        await app._run_tool("slow_tool", None, slow)

    error_payload = json.loads(str(error.value))
    assert error_payload["code"] == "timeout"
    assert "timed out after 0.01s" in error_payload["message"]


@pytest.mark.asyncio
async def test_run_tool_bounds_workflow_timeout_by_persisted_deadline(
    app: AzureSqlMcpApplication,
) -> None:
    async def slow(_: str) -> dict[str, str]:
        await asyncio.sleep(0.05)
        return {"status": "ok"}

    deadline = (
        datetime.now(timezone.utc) + timedelta(seconds=0.01)
    ).isoformat()
    with pytest.raises(ToolError) as error:
        await app._run_tool(
            "benchmark_index_candidate",
            "appdb",
            slow,
            deadline_provider=lambda: deadline,
        )

    assert json.loads(str(error.value))["code"] == "timeout"


@pytest.mark.asyncio
async def test_run_database_pair_tool_returns_mcp_timeout_error(
    tmp_path: Path,
) -> None:
    app = AzureSqlMcpApplication(
        make_config(tmp_path, tool_timeout_seconds=0.01)
    )

    async def slow(_: str, __: str) -> dict[str, str]:
        await asyncio.sleep(0.05)
        return {"status": "ok"}

    with pytest.raises(ToolError) as error:
        await app._run_database_pair_tool("slow_pair_tool", "appdb", "appdb", slow)

    error_payload = json.loads(str(error.value))
    assert error_payload["code"] == "timeout"
    assert "timed out after 0.01s" in error_payload["message"]


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
async def test_explain_query_uses_typed_parameter_execution_and_redacts_values(
    app: AzureSqlMcpApplication,
) -> None:
    app.plans.explain_parameterized_query = AsyncMock(
        return_value=ExplainPlanArtifact(
            database_name="appdb",
            analyze=False,
            summary={"statement_count": 1},
            raw_xml="<ShowPlanXML />",
        )
    )

    payload = await app._explain_query(
        "appdb",
        "SELECT object_id FROM sys.objects WHERE object_id = @ObjectId",
        analyze=False,
        parameter_values={"ObjectId": 42},
        parameter_types={"ObjectId": "int"},
    )

    contract = app.plans.explain_parameterized_query.await_args.args[1]
    assert contract.sp_executesql_sql == "EXEC sys.sp_executesql ?, ?, ?"
    assert payload["parameter_binding"]["values_redacted"] is True
    assert payload["parameter_binding"]["parameters"] == [
        {
            "name": "@ObjectId",
            "data_type": "int",
            "provenance": "explicit_value_and_type",
            "provenance_detail": {},
        }
    ]


@pytest.mark.asyncio
async def test_tune_query_returns_structured_evidence_pack(app: AzureSqlMcpApplication) -> None:
    preflight = stub_case_preflight(app)
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
    preflight.assert_awaited_once_with(
        "appdb",
        "SELECT id FROM dbo.Orders",
    )
    assert (
        app.performance_workflows.start_case.call_args.kwargs["metadata"][
            "equivalence_preflight"
        ]["contract_version"]
        == 2
    )


@pytest.mark.asyncio
async def test_start_tuning_session_passes_multi_hour_budget_to_durable_state(
    app: AzureSqlMcpApplication,
) -> None:
    policy = DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "max_benchmark_executions": 80,
                    "max_tuning_candidates": 60,
                    "max_tuning_session_executions": 2000,
                    "max_tuning_session_minutes": 360,
                }
            },
        }
    )
    app.database_policy = policy
    app.performance_workflows.database_policy = policy
    case = app.performance_workflows.start_case(
        "appdb",
        "SELECT object_id FROM sys.objects",
    )

    session = await app._start_tuning_session(
        "appdb",
        case.case_id,
        60,
        2000,
        360,
        "six-hour-session",
    )

    assert session["max_candidates"] == 60
    assert session["execution_limit"] == 2000
    assert session["time_limit_seconds"] == 360 * 60


@pytest.mark.asyncio
async def test_start_tuning_session_rejects_policy_overrun_without_shortening(
    app: AzureSqlMcpApplication,
) -> None:
    policy = DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "max_benchmark_executions": 80,
                    "max_tuning_candidates": 60,
                    "max_tuning_session_executions": 2000,
                    "max_tuning_session_minutes": 360,
                }
            },
        }
    )
    app.database_policy = policy
    app.performance_workflows.database_policy = policy
    case = app.performance_workflows.start_case(
        "appdb",
        "SELECT object_id FROM sys.objects",
    )

    with pytest.raises(PermissionError, match=r"361 minutes.*360 minutes"):
        await app._start_tuning_session(
            "appdb",
            case.case_id,
            60,
            2000,
            361,
            "over-budget-session",
        )


@pytest.mark.asyncio
async def test_real_session_responses_report_actual_remaining_budgets(
    app: AzureSqlMcpApplication,
) -> None:
    policy = DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "max_benchmark_executions": 80,
                    "max_tuning_candidates": 10,
                    "max_tuning_session_executions": 80,
                    "max_tuning_session_minutes": 20,
                }
            },
        }
    )
    app.database_policy = policy
    app.performance_workflows.database_policy = policy
    case = app.performance_workflows.start_case(
        "appdb",
        "SELECT object_id FROM sys.objects",
    )

    async def start(database_name: str) -> dict[str, object]:
        return await app._start_tuning_session(
            database_name,
            case.case_id,
            10,
            80,
            20,
            "headline-start",
        )

    started = await app._run_tool("start_tuning_session", "appdb", start)
    assert started["headline"] == {
        "session_id": started["session_id"],
        "status": "created",
        "time_limit_minutes": 20,
        "executions_remaining": 80,
        "candidate_slots_remaining": 10,
    }

    async def finalize(database_name: str) -> dict[str, object]:
        return await app._finalize_tuning_session(
            database_name,
            str(started["session_id"]),
            None,
            "no viable candidate",
            "headline-finalize",
        )

    finalized = await app._run_tool(
        "finalize_tuning_session",
        "appdb",
        finalize,
    )
    assert finalized["headline"] == {
        "session_id": started["session_id"],
        "status": "completed",
        "time_limit_minutes": 20,
        "executions_remaining": 80,
        "candidate_slots_remaining": 10,
    }


@pytest.mark.asyncio
async def test_tune_query_binds_explicit_parameters_once(app: AzureSqlMcpApplication) -> None:
    stub_case_preflight(app)
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
        parameter_types={"OrderId": "int"},
    )

    assert payload["parameter_binding"]["values_redacted"] is True
    assert (
        payload["parameter_binding"]["parameters"][0]["provenance"]
        == "explicit_value_and_type"
    )
    assert "value" not in payload["parameter_binding"]["parameters"][0]
    start_call = app.performance_workflows.start_case.call_args
    assert start_call.args == ("appdb", "SELECT @OrderId")
    assert start_call.kwargs["parameter_cases"][0]["types"] == {"OrderId": "int"}


@pytest.mark.asyncio
async def test_benchmark_query_rewrite_reports_sample_equivalence(app: AzureSqlMcpApplication) -> None:
    stub_case_preflight(app)
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
    assert (
        app.performance_workflows.add_candidate.call_args.kwargs["strategy"]
        == "predicate"
    )


@pytest.mark.asyncio
async def test_benchmark_query_rewrite_runs_k_reports_median_and_spread(
    app: AzureSqlMcpApplication,
) -> None:
    stub_case_preflight(app)
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
    stub_case_preflight(app)
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
    app.query_index_analysis.analyze_queries = AsyncMock(  # type: ignore[method-assign]
        return_value={"queries_analyzed": 2}
    )
    queries = ["SELECT @First", "SELECT @Second"]

    result = await app._analyze_query_indexes(
        "appdb",
        queries,
        parameter_values={"First": 1, "Second": 2},
        parameter_types={"First": "int", "Second": "bigint"},
    )

    assert result["queries_analyzed"] == 2
    assert all(item["values_redacted"] is True for item in result["parameter_binding"])
    contracts = app.query_index_analysis.analyze_queries.await_args.kwargs[
        "execution_contracts"
    ]
    assert contracts[0].sp_executesql_values[-1] == 1
    assert contracts[0].parameter_definition == "@First int"
    assert contracts[1].sp_executesql_values[-1] == 2
    assert contracts[1].parameter_definition == "@Second bigint"


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
async def test_async_main_configures_logging(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    configure = Mock()
    run = AsyncMock()

    monkeypatch.setattr("azure_sql_mcp.server.load_server_config", lambda argv=None: config)
    monkeypatch.setattr("azure_sql_mcp.server.configure_logging", configure)
    monkeypatch.setattr("azure_sql_mcp.server.AzureSqlMcpApplication.run", run)

    await async_main([])

    configure.assert_called_once_with(config.log_level, config.log_format)
    run.assert_awaited_once()


@pytest.mark.asyncio
async def test_tune_history_requires_exact_identity_even_for_one_hash_page_match(
    app: AzureSqlMcpApplication,
) -> None:
    app.query_store.get_query_history_by_hash = AsyncMock(
        return_value={"matches": [{"query_id": 7}]}
    )
    app.query_store.resolve_query_identity = AsyncMock(
        return_value={
            "status": "resolved",
            "query_id": 7,
            "query_hash": "0x90FC7E5399EA52A5",
        }
    )
    app.query_store.get_query_history_by_id = AsyncMock(
        return_value={"matches": [{"query_id": 7}]}
    )
    app.query_store.get_query_history_by_text = AsyncMock()

    plan = {"summary": {"statements": [{"query_hash": "0x90FC7E5399EA52A5"}]}}
    history = await app._query_store_history_for_plan("appdb", "SELECT 1", plan, 60)

    assert history["matched_by"] == "query_id"
    assert history["query_hash_corroborated"] is True
    assert history["fuzzy_match_used"] is False
    assert history["matches"] == [{"query_id": 7}]
    app.query_store.get_query_history_by_hash.assert_not_awaited()
    app.query_store.get_query_history_by_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_tune_history_requires_exact_query_store_identity(app: AzureSqlMcpApplication) -> None:
    app.query_store.resolve_query_identity = AsyncMock(
        return_value={"status": "resolved", "query_id": 19}
    )
    app.query_store.get_query_history_by_id = AsyncMock(
        return_value={"matches": [{"query_id": 19}]}
    )
    app.query_store.get_query_history_by_text = AsyncMock()

    plan = {"summary": {"statements": [{"query_hash": "0x90FC7E5399EA52A5"}]}}
    original_sql = "SELECT * FROM dbo.Users WHERE UserId = @UserId"
    history = await app._query_store_history_for_plan("appdb", original_sql, plan, 60)

    assert history["matched_by"] == "query_id"
    assert history["fuzzy_match_used"] is False
    assert history["matches"] == [{"query_id": 19}]
    app.query_store.resolve_query_identity.assert_awaited_once_with(
        "appdb", original_sql
    )
    app.query_store.get_query_history_by_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_tune_history_is_inconclusive_when_plan_hash_disagrees(
    app: AzureSqlMcpApplication,
) -> None:
    app.query_store.resolve_query_identity = AsyncMock(
        return_value={
            "status": "resolved",
            "query_id": 19,
            "query_hash": "0x1111111111111111",
        }
    )
    app.query_store.get_query_history_by_id = AsyncMock(
        return_value={"matches": [{"query_id": 19}]}
    )

    history = await app._query_store_history_for_plan(
        "appdb",
        "SELECT object_id FROM sys.objects",
        {"summary": {"statements": [{"query_hash": "0x2222222222222222"}]}},
        60,
    )

    assert history["status"] == "inconclusive"
    assert history["matches"] == []
    assert history["query_hash_corroborated"] is False
    assert "different query hash" in history["reason"]


@pytest.mark.asyncio
async def test_view_recovery_does_not_adopt_target_without_durable_receipt(
    app: AzureSqlMcpApplication,
) -> None:
    verification = Mock()
    verification.verified = True
    verification.workflow_commit_proven = False
    verification.as_dict.return_value = {"verified": True}
    app.view_workflows.verify_view_change = AsyncMock(return_value=verification)
    app.view_workflows.register_apply_receipt = Mock()
    app.performance_store.update_view_change_intent = Mock(
        return_value={"status": "hold"}
    )

    result = await app._reconcile_durable_view_change(
        "view-change-1",
        Mock(),
        {"version": 3, "receipt": None},
    )

    assert result["status"] == "hold"
    assert result["workflow_applied"] is False
    assert "no durable dispatch/commit receipt" in result["reason"]
    app.view_workflows.register_apply_receipt.assert_not_called()
    app.performance_store.update_view_change_intent.assert_called_once_with(
        "view-change-1",
        status="hold",
        expected_version=3,
        raw_sql_persistence_authorized=True,
    )


@pytest.mark.asyncio
async def test_view_recovery_preserves_the_original_durable_receipt(
    app: AzureSqlMcpApplication,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = Mock()
    original_receipt = {
        "marker_name": "AzureSqlMcp_View_v1_" + ("a" * 48),
        "dispatch_proof": {"dispatch_id": "original-dispatch-proof"},
    }
    receipt_snapshot = Mock()
    verification = Mock()
    verification.verified = True
    verification.as_dict.return_value = {"verified": True}
    app.view_workflows.verify_view_change = AsyncMock(return_value=verification)
    app.view_workflows.register_apply_receipt = Mock()
    app.performance_store.update_view_change_intent = Mock(
        return_value={"status": "applied"}
    )
    parse_receipt = Mock(return_value=receipt_snapshot)
    monkeypatch.setattr(
        "azure_sql_mcp.server.view_snapshot_from_receipt",
        parse_receipt,
    )
    intent = {"version": 7, "receipt": original_receipt}

    result = await app._reconcile_durable_view_change(
        "view-change-with-receipt",
        prepared,
        intent,
    )

    assert result["status"] == "reconciled_applied"
    assert intent["receipt"] is original_receipt
    parse_receipt.assert_called_once_with(original_receipt)
    app.view_workflows.register_apply_receipt.assert_called_once_with(
        prepared,
        receipt_snapshot,
    )
    app.performance_store.update_view_change_intent.assert_called_once_with(
        "view-change-with-receipt",
        status="applied",
        expected_version=7,
        raw_sql_persistence_authorized=True,
    )


@pytest.mark.asyncio
async def test_interrupted_noop_view_recovery_uses_unchanged_prior_snapshot(
    app: AzureSqlMcpApplication,
) -> None:
    prepared = Mock(operation="noop")
    verification = Mock()
    verification.verified = False
    verification.workflow_commit_proven = False
    verification.as_dict.return_value = {"verified": False}
    current = Mock()
    current.as_dict.return_value = {"exists": True}
    app.view_workflows.verify_view_change = AsyncMock(return_value=verification)
    app.view_workflows.prior_state_restored = AsyncMock(
        return_value=(True, current)
    )
    app.performance_store.update_view_change_intent = Mock(
        return_value={"status": "already_applied"}
    )

    result = await app._reconcile_durable_view_change(
        "view-noop-interrupted",
        prepared,
        {"version": 4, "receipt": None},
    )

    assert result["status"] == "already_applied"
    assert result["intent_status"] == "already_applied"
    assert result["workflow_applied"] is False
    assert result["prior_state_restored"] is True
    assert "performed no mutation" in result["reason"]
    app.performance_store.update_view_change_intent.assert_called_once_with(
        "view-noop-interrupted",
        status="already_applied",
        expected_version=4,
        raw_sql_persistence_authorized=True,
    )


@pytest.mark.asyncio
async def test_durable_view_prepare_replays_intent_before_generating_marker(
    server_config_factory,
    tmp_path,
) -> None:
    app = AzureSqlMcpApplication(
        server_config_factory(
            access_mode=AccessMode.UNRESTRICTED,
            profile=McpProfile.SANDBOX,
            performance_state_dir=str(tmp_path / "state"),
            persist_view_sql_state=True,
        )
    )

    async def capture_view(
        database_name: str,
        schema_name: str,
        view_name: str,
        *,
        marker_name: str,
    ) -> ViewSnapshot:
        return ViewSnapshot(
            database_name=database_name,
            schema_name=schema_name,
            view_name=view_name,
            exists=False,
            marker_name=marker_name,
        )

    app.view_workflows.capture_view = AsyncMock(side_effect=capture_view)  # type: ignore[method-assign]
    request = ViewChangeRequest(
        database_name="appdb",
        schema_name="dbo",
        view_name="ReplayView",
        definition="SELECT [Id] FROM [dbo].[Source]",
        operation="create",
        idempotency_key="replay-view",
    )
    prepared = await app.view_workflows.prepare_view_change(request)
    payload = prepared_view_change_state(prepared)
    app.performance_store.create_view_change_intent(
        change_id="view-existing-replay",
        database_fingerprint=database_fingerprint("appdb", app.config.server),
        request_fingerprint=fingerprint_json(payload),
        payload=payload,
        raw_sql_persistence_authorized=True,
    )
    app.view_workflows.prepare_view_change = AsyncMock(  # type: ignore[method-assign]
        side_effect=AssertionError("replay must not prepare a new marker")
    )

    replay = await app._prepare_view_change(
        "appdb",
        "dbo",
        "ReplayView",
        "SELECT [Id] FROM [dbo].[Source]",
        "create",
        False,
        False,
        "replay-view",
    )

    assert replay["change_id"] == "view-existing-replay"
    assert replay["intent_status"] == "prepared"
    app.view_workflows.prepare_view_change.assert_not_awaited()
    app.performance_store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("history", "parameter_buckets", "available", "complete", "status"),
    (
        (
            {"matches": []},
            {"buckets": []},
            False,
            False,
            "inconclusive",
        ),
        (
            {"matches": [{"query_id": 19}]},
            {"buckets": []},
            True,
            False,
            "partial",
        ),
        (
            {"matches": [{"query_id": 19}], "truncated": True},
            {"buckets": [{"plan_id": 7}]},
            True,
            False,
            "partial",
        ),
        (
            {"matches": [{"query_id": 19}] * 20},
            {"buckets": [{"plan_id": 7}]},
            True,
            False,
            "partial",
        ),
        (
            {"matches": [{"query_id": 19}]},
            {"buckets": [{"plan_id": 7}]},
            True,
            True,
            "resolved",
        ),
    ),
)
async def test_query_store_evidence_does_not_overstate_coverage(
    app: AzureSqlMcpApplication,
    history: dict[str, object],
    parameter_buckets: dict[str, object],
    available: bool,
    complete: bool,
    status: str,
) -> None:
    app.query_store.resolve_query_identity = AsyncMock(
        return_value={
            "status": "resolved",
            "query_id": 19,
            "query_hash": "0x90FC7E5399EA52A5",
        }
    )
    app.query_store.get_query_history_by_id = AsyncMock(return_value=history)
    app.query_store.get_parameter_runtime_buckets = AsyncMock(
        return_value=parameter_buckets
    )

    evidence = await app._collect_query_store_evidence(
        "appdb",
        "SELECT * FROM dbo.Users WHERE UserId = @UserId",
        60,
    )

    assert evidence["available"] is available
    assert evidence["complete"] is complete
    assert evidence["status"] == status


@pytest.mark.asyncio
async def test_unparameterized_query_store_history_does_not_require_buckets(
    app: AzureSqlMcpApplication,
) -> None:
    app.query_store.resolve_query_identity = AsyncMock(
        return_value={
            "status": "resolved",
            "query_id": 19,
            "query_hash": "0x90FC7E5399EA52A5",
        }
    )
    app.query_store.get_query_history_by_id = AsyncMock(
        return_value={"matches": [{"query_id": 19}]}
    )
    app.query_store.get_parameter_runtime_buckets = AsyncMock(
        return_value={"buckets": []}
    )

    evidence = await app._collect_query_store_evidence(
        "appdb",
        "SELECT UserId FROM dbo.Users",
        60,
    )

    assert evidence["available"] is True
    assert evidence["complete"] is True
    assert evidence["status"] == "resolved"
    assert evidence["coverage"]["parameter_buckets"]["required"] is False


@pytest.mark.asyncio
async def test_capability_check_publishes_tuning_contract(
    app: AzureSqlMcpApplication,
) -> None:
    app.capabilities.check = AsyncMock(return_value={"query_store": True})
    app.platform_capabilities.get_summary = AsyncMock(
        return_value={"platform": "azure_sql_database_paas"}
    )

    result = await app._check_database_capabilities("appdb")

    assert result["mcp_contract"] == {
        "contract_version": 1,
        "performance_tuning": 1,
        "durable_view_change": 1,
        "prepared_plan_action": 1,
    }
    assert result["local_tuning_policy"] == {
        "configured": False,
        "environment": "unknown",
        "allow_read": False,
        "allow_benchmark": False,
        "allow_test_indexes": False,
        "allow_view_apply": False,
        "allow_plan_apply": False,
        "max_benchmark_executions": 0,
        "max_tuning_candidates": 0,
        "max_tuning_session_executions": 0,
        "max_tuning_session_minutes": 0,
    }


@pytest.mark.asyncio
async def test_tune_history_rejects_ambiguous_exact_identity_despite_plan_hash(
    app: AzureSqlMcpApplication,
) -> None:
    app.query_store.get_query_history_by_hash = AsyncMock(
        return_value={"matches": [{"query_id": 41}, {"query_id": 42}]}
    )
    app.query_store.resolve_query_identity = AsyncMock(
        return_value={
            "status": "ambiguous",
            "matches": [{"query_id": 41}, {"query_id": 42}],
        }
    )
    app.query_store.get_query_history_by_id = AsyncMock()

    plan = {"summary": {"statements": [{"query_hash": "0x90FC7E5399EA52A5"}]}}
    history = await app._query_store_history_for_plan(
        "appdb",
        "SELECT object_id FROM sys.objects",
        plan,
        60,
    )

    assert history["status"] == "inconclusive"
    assert history["reason"] == "exact Query Store identity was not uniquely resolved"
    assert history["plan_query_hash"] == "0x90FC7E5399EA52A5"
    assert history["matched_by"] == "none"
    app.query_store.get_query_history_by_hash.assert_not_awaited()
    app.query_store.get_query_history_by_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_tune_history_is_inconclusive_when_exact_identity_is_ambiguous(
    app: AzureSqlMcpApplication,
) -> None:
    app.query_store.resolve_query_identity = AsyncMock(
        return_value={
            "status": "ambiguous",
            "matches": [{"query_id": 1}, {"query_id": 2}],
        }
    )
    app.query_store.get_query_history_by_id = AsyncMock()
    app.query_store.get_query_history_by_text = AsyncMock()

    history = await app._query_store_history_for_plan(
        "appdb",
        "SELECT object_id FROM sys.objects",
        {"summary": {"statements": []}},
        60,
    )

    assert history["status"] == "inconclusive"
    assert history["matched_by"] == "none"
    assert history["fuzzy_match_used"] is False
    app.query_store.get_query_history_by_id.assert_not_awaited()
    app.query_store.get_query_history_by_text.assert_not_awaited()
