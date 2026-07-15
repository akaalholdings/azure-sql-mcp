from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.artifacts import ExplainPlanArtifact
from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.plans import ProfiledPlanResult
from azure_sql_mcp.performance_workflows import database_fingerprint
from azure_sql_mcp.server import AzureSqlMcpApplication


def _policy() -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "sandbox",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "allow_test_indexes": True,
                    "allow_plan_apply": False,
                    "max_benchmark_executions": 80,
                }
            },
        }
    )


def _profile(elapsed_ms: float) -> ProfiledPlanResult:
    summary = {
        "statement_count": 1,
        "operator_count": 1,
        "actual_metrics": {
            "actual_cpu_ms": elapsed_ms / 2,
            "actual_elapsed_ms": elapsed_ms,
            "actual_rows": 1,
            "actual_logical_reads": None,
            "actual_physical_reads": None,
            "query_metric_source": "showplan_query_time_stats",
            "read_metric_source": "not_available_as_reliable_query_total",
        },
        "warnings": [],
        "missing_indexes": [],
        "top_operators": [],
    }
    return ProfiledPlanResult(
        plan=ExplainPlanArtifact(
            database_name="appdb",
            analyze=True,
            summary=summary,
            raw_xml="<ShowPlanXML />",
        ),
        result_sets=[QueryResult(columns=("id",), rows=[{"id": 1}])],
        elapsed_wall_ms=elapsed_ms,
        user_query_executions=1,
        truncated=False,
        metric_provenance="test",
    )


def _app(server_config_factory) -> AzureSqlMcpApplication:
    app = AzureSqlMcpApplication(
        server_config_factory(
            access_mode=AccessMode.UNRESTRICTED,
            write_policy=WritePolicy.APPLY,
            profile=McpProfile.SANDBOX,
            performance_state_dir=":memory:",
        )
    )
    policy = _policy()
    app.database_policy = policy
    app.performance_workflows.database_policy = policy
    return app


def _candidate(
    app: AzureSqlMcpApplication,
    sql: str,
    *,
    parameter_cases: list[dict[str, object]] | None = None,
) -> tuple[str, str]:
    case = app.performance_workflows.start_case(
        "appdb",
        sql,
        parameter_cases=parameter_cases,
    )
    session = app.performance_workflows.start_session(case.case_id)
    candidate = app.performance_workflows.add_candidate(
        session["session_id"],
        sql,
        strategy="index",
    )
    return session["session_id"], candidate["candidate_id"]


@pytest.mark.asyncio
async def test_first_index_slower_second_index_faster_and_session_continues(
    server_config_factory,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, first_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.performance_workflows.compare_query_results = AsyncMock(  # type: ignore[method-assign]
        return_value={
            "status": "match",
            "proven_for_parameter_case": True,
            "executions": 2,
        }
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[*[_profile(100) for _ in range(3)], *[_profile(140) for _ in range(3)]]
    )

    first = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        first_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        ["id"],
        "screening",
        True,
        30,
        "first-index",
    )
    assert first["classification"] == "regressed"
    assert first["session_continues"] is True
    assert first["lease"]["status"] == "cleaned"

    second = app.performance_workflows.add_candidate(
        session_id,
        sql,
        strategy="index",
        idempotency_key="second-index-candidate",
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[*[_profile(100) for _ in range(3)], *[_profile(45) for _ in range(3)]]
    )
    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        second["candidate_id"],
        sql,
        "dbo",
        "Items",
        ["status", "id"],
        None,
        "screening",
        True,
        30,
        "second-index",
    )

    assert result["classification"] == "improved"
    assert result["durable_state"] == "screening"
    assert result["session_continues"] is True
    assert app.tuning_sessions.get_candidate(first_id).state == "regressed"
    assert app.tuning_sessions.get_candidate(second["candidate_id"]).state == "screening"
    assert app._drop_test_index.await_count == 2


@pytest.mark.asyncio
async def test_index_cleanup_failure_is_durable_and_blocks_silent_success(
    server_config_factory,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(side_effect=RuntimeError("synthetic cleanup"))  # type: ignore[method-assign]
    app.performance_workflows.compare_query_results = AsyncMock(  # type: ignore[method-assign]
        return_value={"status": "match", "proven_for_parameter_case": True, "executions": 2}
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[*[_profile(100) for _ in range(3)], *[_profile(50) for _ in range(3)]]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        "screening",
        True,
        30,
        "cleanup-failure",
    )

    assert result["classification"] == "cleanup_required"
    assert result["lease"]["status"] == "cleanup_required"
    assert app.tuning_sessions.get_candidate(candidate_id).state == "cleanup_required"

    with pytest.raises(ValueError, match="terminal benchmark result"):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            None,
            "screening",
            True,
            30,
            "cleanup-failure",
        )


@pytest.mark.asyncio
async def test_index_candidate_timeout_is_inconclusive_without_create(
    server_config_factory,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(side_effect=TimeoutError("synthetic timeout"))  # type: ignore[method-assign]

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        "screening",
        True,
        30,
        "timeout-index",
    )

    assert result["classification"] == "inconclusive"
    assert result["lease"]["status"] == "create_failed"
    app._create_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_uncertain_index_create_is_reconciled_and_cleaned(
    server_config_factory,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(100) for _ in range(3)]
    )
    app._create_test_index = AsyncMock(  # type: ignore[method-assign]
        side_effect=TimeoutError("synthetic uncertain create")
    )
    app.executor.fetch_all = AsyncMock(return_value=[{"found": 1}])  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        "screening",
        True,
        30,
        "uncertain-index-create",
    )

    assert result["classification"] == "inconclusive"
    assert result["lease"]["status"] == "cleaned"
    app._drop_test_index.assert_awaited_once()


@pytest.mark.asyncio
async def test_index_candidate_measures_all_recorded_parameter_buckets(
    server_config_factory,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = @p"
    parameter_cases: list[dict[str, object]] = [
        {"name": "common", "values": {"p": 1}},
        {"name": "rare", "values": {"p": 999999}},
        {"name": "NULL", "values": {"p": None}},
        {"name": "boundary", "values": {"p": 0}},
    ]
    session_id, candidate_id = _candidate(
        app,
        sql,
        parameter_cases=parameter_cases,
    )

    async def bind(_database: str, query: str, values) -> str:
        value = values["p"]
        literal = "NULL" if value is None else str(value)
        return f"DECLARE @p int = {literal}; {query}"

    app.performance_workflows.parameter_binder = bind
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.performance_workflows.compare_query_results = AsyncMock(  # type: ignore[method-assign]
        return_value={
            "status": "match",
            "proven_for_parameter_case": True,
            "executions": 2,
        }
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(12)],
            *[_profile(50) for _ in range(12)],
        ]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        ["id"],
        "screening",
        True,
        30,
        "parameterized-index",
        parameter_cases,
    )

    assert result["classification"] == "improved"
    assert result["executions"] == 32
    assert [
        item["parameter_case"] for item in result["parameter_results"]
    ] == ["common", "rare", "NULL", "boundary"]
    assert app.tuning_sessions.get_candidate(candidate_id).parameter_cases == 4


@pytest.mark.asyncio
async def test_expired_index_lease_is_reconciled_on_sandbox_startup(
    server_config_factory,
) -> None:
    app = _app(server_config_factory)
    app.performance_store.create_index_lease(
        lease_id="lease-expired",
        database_fingerprint=database_fingerprint("appdb"),
        session_id="session-recovery",
        candidate_id="candidate-recovery",
        index_name="IX_Testing_recovery",
        object_fingerprint="object-recovery",
        expires_at_utc=(datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        metadata={"target_schema": "dbo", "target_table": "Items"},
    )
    app.executor.fetch_all = AsyncMock(return_value=[{"found": 1}])  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]

    summary = await app._cleanup_expired_index_leases()

    assert summary == {"examined": 1, "cleaned": 1, "cleanup_required": 0}
    assert app.performance_store.get_index_lease("lease-expired")["status"] == "cleaned"
    app._drop_test_index.assert_awaited_once()
