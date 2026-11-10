"""B5 wiring: Query Store grouping, failed runs, plan-action confirmation,
diagnosis sources, the new diagnostics tools and the history adapters."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import TOOL_GROUPS
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.database_policy import DatabasePolicyValidationError
from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_server import make_config


def _policy(environment: str = "test", **extra: Any) -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {"environment": environment, "allow_read": True, **extra},
            },
        }
    )


async def _call(app: AzureSqlMcpApplication, name: str, arguments: dict[str, Any]) -> Any:
    return await app.mcp._tool_manager.call_tool(name, arguments)


async def _error(app: AzureSqlMcpApplication, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    with pytest.raises(ToolError) as error:
        await _call(app, name, arguments)
    text = str(error.value)
    return json.loads(text[text.index("{") :])


@pytest.fixture
def app(tmp_path: Path) -> AzureSqlMcpApplication:
    return AzureSqlMcpApplication(make_config(tmp_path, AccessMode.UNRESTRICTED))


def test_every_registered_tool_name_has_a_tool(app: AzureSqlMcpApplication) -> None:
    # A name in a profile allowlist with no tool behind it is advertised by
    # the guides and next_tools and then fails as "unknown tool".
    registered = {tool.name for tool in app.mcp._tool_manager.list_tools()}

    assert set(TOOL_GROUPS) - registered == set()


@pytest.mark.asyncio
async def test_top_queries_can_fold_literal_variants_and_pick_a_replica(
    app: AzureSqlMcpApplication,
) -> None:
    # Per query_id an ad hoc workload shows a thousand small rows; the family
    # that burns the CPU is only visible per query_hash.
    app.query_store.get_status = AsyncMock(return_value={"enabled": True})
    app.query_store.get_top_queries = AsyncMock(
        return_value=[{"query_hash": "0x01", "query_text_preview": "SELECT 1", "query_count": 900}]
    )

    result = await _call(
        app, "get_top_queries", {"group_by": "query_hash", "replica_role": "primary"}
    )

    kwargs = app.query_store.get_top_queries.await_args.kwargs
    assert (kwargs["group_by"], kwargs["replica_role"]) == ("query_hash", "primary")
    assert result["group_by"] == "query_hash"
    assert result["rows"][0]["query_count"] == 900


@pytest.mark.asyncio
async def test_top_queries_rejects_an_unknown_grouping_before_any_read(
    app: AzureSqlMcpApplication,
) -> None:
    app.executor.fetch_all = AsyncMock(return_value=[])

    payload = await _error(app, "get_top_queries", {"group_by": "table"})

    assert payload["code"] == "invalid_arguments"
    app.executor.fetch_all.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_executions_tool_reads_the_trend_service(
    app: AzureSqlMcpApplication,
) -> None:
    # Timeouts and aborted runs never show in the top-queries averages.
    app.query_store_trends.failed_executions = AsyncMock(
        return_value={"rising": [], "top_failing_queries": [], "summary": {"failed_runs": 0}}
    )

    result = await _call(
        app, "get_failed_executions", {"recent_minutes": 120, "replica_role": "primary", "top": 5}
    )

    app.query_store_trends.failed_executions.assert_awaited_once_with(
        "appdb",
        recent_minutes=120,
        baseline_minutes=20160,
        as_of_utc=None,
        top=5,
        replica_role="primary",
    )
    assert result["result_status"] == "empty"


@pytest.mark.asyncio
async def test_identity_resolution_gets_the_plan_query_hash(app: AzureSqlMcpApplication) -> None:
    # Text match alone misses a parameterized statement; the hash finds it.
    app.query_store.resolve_query_identity = AsyncMock(return_value={"status": "not_found"})

    await app._query_store_history_for_plan(
        "appdb",
        "SELECT 1",
        {"summary": {"statements": [{"query_hash": "0x4D5E6F708192A3B4"}]}},
        60,
    )

    assert app.query_store.resolve_query_identity.await_args.kwargs == {
        "query_hash": "0x4D5E6F708192A3B4"
    }


def _prepared(intent_id: str = "intent-1") -> dict[str, Any]:
    return {
        "intent": {"intent_id": intent_id, "metadata": {"evidence_hash": "a" * 64}},
        "prepared": True,
        "guardrails": {"allowed": True},
        "force_precheck": {"blocked": False},
    }


class _Intent:
    intent_id = "intent-1"
    metadata = {"evidence_hash": "a" * 64}


def _plan_app(tmp_path: Path, environment: str) -> AzureSqlMcpApplication:
    app = AzureSqlMcpApplication(make_config(tmp_path, AccessMode.UNRESTRICTED))
    app.database_policy = _policy(environment, allow_plan_apply=True)
    app.plan_actions.prepare = AsyncMock(return_value=_prepared())
    app.plan_actions.apply = AsyncMock(return_value={"confirmed": True})
    app.plan_actions.rollback = AsyncMock(return_value={"confirmed": True})
    app.performance_store.get_plan_action_intent = lambda _id: _Intent()  # type: ignore[method-assign]
    return app


_PREPARE = {
    "session_id": "s",
    "operation": "force_plan",
    "query_id": 7,
    "plan_id": 9,
    "evidence": {"reason": "regression"},
    "reviewed_by": "dba",
    "reason": "regressed plan",
    "idempotency_key": "k",
}


@pytest.mark.asyncio
async def test_production_plan_apply_needs_the_token_prepare_returned(tmp_path: Path) -> None:
    # Finding 99: a free-text "authorization_reference" let an agent approve
    # its own production change.
    app = _plan_app(tmp_path, "production")
    prepared = await _call(app, "prepare_plan_action", _PREPARE)
    token = prepared["confirmation"]["confirmation_token"]
    assert prepared["force_precheck"] == {"blocked": False}
    assert prepared["guardrails"] == {"allowed": True}

    refused = await _error(
        app,
        "apply_prepared_plan_action",
        {"intent_id": "intent-1", "authorization_reference": "CHG-1"},
    )
    assert "confirmation token" in refused["message"].lower()
    app.plan_actions.apply.assert_not_awaited()

    wrong = await _error(
        app,
        "apply_prepared_plan_action",
        {"intent_id": "intent-1", "authorization_reference": "CHG-1", "confirmation_token": "AAAA-AAAA-AAAA"},
    )
    assert "does not match" in wrong["message"]
    app.plan_actions.apply.assert_not_awaited()

    await _call(
        app,
        "apply_prepared_plan_action",
        {"intent_id": "intent-1", "authorization_reference": "CHG-1", "confirmation_token": token},
    )
    app.plan_actions.apply.assert_awaited_once()

    replay = await _error(
        app,
        "apply_prepared_plan_action",
        {"intent_id": "intent-1", "authorization_reference": "CHG-1", "confirmation_token": token},
    )
    assert "confirmation token" in replay["message"].lower()


@pytest.mark.asyncio
async def test_production_rollback_issues_its_own_token_then_runs(tmp_path: Path) -> None:
    app = _plan_app(tmp_path, "production")

    first = await _call(
        app, "rollback_plan_action", {"intent_id": "intent-1", "authorization_reference": "CHG-1"}
    )
    assert first["confirmed"] is False
    assert first["confirmation"]["action"] == "rollback"
    app.plan_actions.rollback.assert_not_awaited()

    await _call(
        app,
        "rollback_plan_action",
        {
            "intent_id": "intent-1",
            "authorization_reference": "CHG-1",
            "confirmation_token": first["confirmation"]["confirmation_token"],
        },
    )
    app.plan_actions.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_non_production_plan_apply_needs_no_token(tmp_path: Path) -> None:
    app = _plan_app(tmp_path, "test")

    prepared = await _call(app, "prepare_plan_action", _PREPARE)
    await _call(
        app,
        "apply_prepared_plan_action",
        {"intent_id": "intent-1", "authorization_reference": "CHG-1"},
    )

    assert "confirmation" not in prepared
    app.plan_actions.apply.assert_awaited_once()


def test_diagnosis_reads_every_optional_source(app: AzureSqlMcpApplication) -> None:
    # Unwired, the stories for memory pressure, pool limits and plan cache
    # churn can never fire: diagnose reports "not observed" for all of them.
    diagnosis = app.database_diagnosis

    assert diagnosis.memory_pressure is app.memory_pressure
    assert diagnosis.tempdb_memory is app.tempdb_memory
    assert diagnosis.elastic_pool is app.elastic_pool
    assert diagnosis.columnstore_health is app.columnstore_health
    assert diagnosis.plan_cache is app.plan_cache
    assert diagnosis.baseline is app.database_baseline
    assert diagnosis.executor is app.executor
    assert diagnosis.source_timeout_seconds == app.config.query_timeout_seconds


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "service", "method", "arguments", "expected_args", "expected_kwargs"),
    [
        ("get_memory_pressure", "memory_pressure", "get_memory_pressure", {"top_clerks": 5}, ("appdb",), {"top_clerks": 5}),
        ("get_elastic_pool_pressure", "elastic_pool", "get_pool_pressure", {"window_minutes": 15}, ("appdb",), {"window_minutes": 15}),
        (
            "get_columnstore_health",
            "columnstore_health",
            "get_columnstore_health",
            {"schema_name": "dbo", "table_name": "FactOrders"},
            ("appdb",),
            {"worst_n": 10, "schema_name": "dbo", "table_name": "FactOrders"},
        ),
        ("assess_capacity", "capacity_assessment", "assess", {"window_days": 7}, ("appdb",), {"window_days": 7, "master_available": False}),
        (
            "analyze_application_patterns",
            "application_patterns",
            "analyze",
            {"window_minutes": 30},
            ("appdb",),
            {"window_minutes": 30, "top": 20},
        ),
    ],
)
async def test_new_diagnostics_tools_reach_their_services(
    app: AzureSqlMcpApplication,
    tool: str,
    service: str,
    method: str,
    arguments: dict[str, Any],
    expected_args: tuple[Any, ...],
    expected_kwargs: dict[str, Any],
) -> None:
    stub = AsyncMock(return_value={"result_status": "ok", "marker": tool})
    setattr(getattr(app, service), method, stub)

    result = await _call(app, tool, arguments)

    stub.assert_awaited_once_with(*expected_args, **expected_kwargs)
    assert result["marker"] == tool


@pytest.mark.asyncio
async def test_database_baseline_tool_serves_resource_and_query_bands(
    app: AzureSqlMcpApplication,
) -> None:
    # "Is this normal for a Monday 09:00?" needs the band, not the average.
    app.database_baseline.resource_bands = AsyncMock(return_value={"result_status": "ok", "kind": "resource"})
    app.database_baseline.query_bands = AsyncMock(return_value={"result_status": "ok", "kind": "query"})

    resource = await _call(app, "get_database_baseline", {"window_minutes": 30})
    query = await _call(app, "get_database_baseline", {"scope": "queries", "query_ids": [7, 9]})

    assert resource["kind"] == "resource"
    app.database_baseline.resource_bands.assert_awaited_once_with(
        "appdb", window_minutes=30, as_of_utc=None, master_available=False
    )
    assert query["kind"] == "query"
    assert app.database_baseline.query_bands.await_args.kwargs["query_ids"] == [7, 9]


def test_history_sources_policy_is_validated_strictly() -> None:
    # A typo in a workspace binding must stop startup, not silently turn the
    # recorder off and let "no events" read as a clean history.
    with pytest.raises(DatabasePolicyValidationError, match="history_sources"):
        _policy(history_sources={"log_analitics": {}})
    with pytest.raises(DatabasePolicyValidationError, match="history_sources"):
        _policy(history_sources={"log_analytics": {"workspace_id": "not-a-guid"}})

    policy = _policy(
        history_sources={
            "azure_monitor_metrics": {
                "resource_id": (
                    "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-bench"
                    "/providers/Microsoft.Sql/servers/sql-bench/databases/appdb"
                )
            }
        }
    )
    assert set(policy.policy_for("appdb").history_sources) == {"azure_monitor_metrics"}


@pytest.mark.asyncio
async def test_diagnostic_history_without_any_recorder_is_a_clean_precondition(
    app: AzureSqlMcpApplication,
) -> None:
    # The bench has no recorder: the answer is "set one up, here is how",
    # never an error and never "no timeouts happened".
    app.executor.fetch_all = AsyncMock(return_value=[])

    result = await _call(app, "get_diagnostic_history", {"category": "timeout", "window_minutes": 60})

    # The mocked catalog lists no Extended Events fields, so that source is
    # "unavailable"; on a real database with no capture session it is "precondition".
    assert result["result_status"] in {"precondition", "unavailable"}
    assert result["events"] == []
    by_source = {item["source"]: item for item in result["coverage"]}
    assert by_source["log_analytics"]["status"] == "precondition"
    assert "history_sources" in by_source["log_analytics"]["remediation"]
    assert by_source["loki"]["status"] == "precondition"


@pytest.mark.asyncio
async def test_diagnostic_history_coverage_lists_the_recorders(app: AzureSqlMcpApplication) -> None:
    app.executor.fetch_all = AsyncMock(return_value=[])

    result = await _call(app, "get_diagnostic_history", {"category": "coverage"})

    names = {source["source"] for source in result["sources"]}
    assert "extended_events" in names
    assert any(source["configured"] is False for source in result["sources"])


@pytest.mark.asyncio
async def test_deadlock_history_reads_the_recorders_only_when_asked(
    app: AzureSqlMcpApplication,
) -> None:
    # The ring buffer holds minutes; last week's deadlock is in a recorder.
    app.executor.fetch_all = AsyncMock(return_value=[])
    app.lock_diagnostics.get_deadlock_history = AsyncMock(return_value={"deadlocks": [{"id": 1}]})

    live = await _call(app, "get_deadlock_history", {})
    recorded = await _call(app, "get_deadlock_history", {"source": "auto", "window_minutes": 10080})

    assert live["deadlocks"] == [{"id": 1}]
    app.lock_diagnostics.get_deadlock_history.assert_awaited_once()
    assert recorded["category"] == "deadlock"
    assert recorded["result_status"] in {"precondition", "unavailable", "empty"}
