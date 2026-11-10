"""B3 wiring: the index benchmark uses the safer DDL, leases and measurement helpers."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import parse_candidate_key
from azure_sql_mcp.performance_workflows import BENCHMARK_MARKER_PREFIX
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.view_workflows import ViewWorkflowService
from tests.unit.test_index_benchmark_workflow import _app
from tests.unit.test_index_benchmark_workflow import _candidate
from tests.unit.test_index_benchmark_workflow import _install_catalog
from tests.unit.test_index_benchmark_workflow import _profile

SQL = "SELECT id FROM dbo.Items"


def _policy(environment: str) -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": environment,
                    "allow_read": True,
                    "allow_benchmark": True,
                    "allow_test_indexes": True,
                    "allow_view_apply": True,
                    "max_benchmark_executions": 80,
                }
            },
        }
    )


async def _benchmark(
    app: AzureSqlMcpApplication,
    session_id: str,
    candidate_id: str,
    *,
    key: str = "b3-key",
    online: bool = True,
    parameter_cases: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        SQL,
        "dbo",
        "Items",
        ["id"],
        None,
        None,
        False,
        "screening",
        online,
        True,
        30,
        key,
        parameter_cases,
    )


def _profiles(index_name: str) -> list[Any]:
    return [
        *[_profile(100) for _ in range(3)],
        *[_profile(50, index_name=index_name) for _ in range(3)],
        *[_profile(100) for _ in range(3)],
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("environment", ["prd", "prod-eu", "customer-a"])
async def test_test_indexes_are_refused_unless_the_label_is_known_non_production(
    server_config_factory, environment: str
) -> None:
    # The old denylist let "prd" and any unknown label build indexes on what may be production.
    app = _app(server_config_factory)
    session_id, candidate_id = _candidate(app, SQL)
    policy = _policy(environment)
    app.database_policy = policy
    app.performance_workflows.database_policy = policy

    with pytest.raises(PermissionError):
        await _benchmark(app, session_id, candidate_id)
    with pytest.raises(PermissionError):
        app._require_test_index_database("appdb")


@pytest.mark.parametrize("environment", ["prd", "production-uk", "customer-a"])
def test_view_apply_is_refused_unless_the_label_is_known_non_production(
    environment: str,
) -> None:
    service = ViewWorkflowService(AsyncMock(), _policy(environment), AsyncMock())

    assert service._apply_policy_reason("appdb") == (
        "view DDL is restricted to non-production environments"
    )
    assert ViewWorkflowService(AsyncMock(), _policy("test"), AsyncMock())._apply_policy_reason(
        "appdb"
    ) is None


@pytest.mark.asyncio
async def test_workflow_create_waits_at_low_priority_and_refuses_an_offline_build(
    server_config_factory,
) -> None:
    # An index build that waits at normal priority heads a blocking chain on
    # the sandbox; an offline build blocks every writer for the whole build.
    app = _app(server_config_factory)

    preview = await app._create_test_index(
        "appdb", "dbo", "Items", "IX_Testing_b3", ["id"], None, True, dry_run=True
    )
    assert "WAIT_AT_LOW_PRIORITY" in preview["sql_preview"]
    assert "ABORT_AFTER_WAIT = SELF" in preview["sql_preview"]
    with pytest.raises(PermissionError, match="offline"):
        await app._create_test_index(
            "appdb", "dbo", "Items", "IX_Testing_b3", ["id"], None, False, dry_run=True
        )


def test_guarded_drop_bounds_its_table_lock_wait(server_config_factory) -> None:
    # The drop used to wait the whole statement timeout behind one long reader.
    app = _app(server_config_factory)

    batch = app._build_guarded_index_drop_sql("dbo", "Items", "IX_Testing_b3")

    probe = batch.index("SET LOCK_TIMEOUT 5000;")
    assert probe < batch.index("WITH (TABLOCK, HOLDLOCK)") < batch.index("DROP INDEX")


@pytest.mark.asyncio
async def test_benchmark_runs_marked_sql_and_reports_a_compact_plan_delta(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Unmarked, the benchmark reuses and replaces the application's cached
    # plan and merges its runs into the application's Query Store history.
    app = _app(server_config_factory)
    session_id, candidate_id = _candidate(app, SQL)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(monkeypatch, key_columns=["id"])
    app.plans.profile_query = AsyncMock(side_effect=_profiles(index_name))  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    executed = {call.args[1] for call in app.plans.profile_query.await_args_list}
    assert len(executed) == 1
    [text] = executed
    assert text.startswith(BENCHMARK_MARKER_PREFIX) and text.endswith(SQL)
    delta = result["parameter_results"][0]["plan_delta"]
    assert set(delta) == {"plan_changed", "baseline", "candidate"}
    assert "top_operators" in delta["candidate"]
    assert result["plan_controls_not_applied"] is True


@pytest.mark.asyncio
async def test_a_stuck_lease_in_another_database_does_not_block_this_one(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Finding 44: one unresolved cleanup anywhere stopped index work everywhere.
    app = _app(server_config_factory)
    session_id, candidate_id = _candidate(app, SQL)
    seen: list[str | None] = []

    async def cleanup(*, database_fingerprint: str | None = None, **_options: object) -> dict[str, object]:
        seen.append(database_fingerprint)
        return {"examined": 0, "cleaned": 0, "cleanup_required": 0, "dropped": [], "remaining": []}

    # The benchmark reconciles the leases of its own database only (S13e seam).
    app._reconcile_leases = cleanup  # type: ignore[method-assign]
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(monkeypatch, key_columns=["id"])
    app.plans.profile_query = AsyncMock(side_effect=_profiles(index_name))  # type: ignore[method-assign]

    await _benchmark(app, session_id, candidate_id)

    case = app.performance_store.get_performance_case(
        app.tuning_sessions.get_session(session_id).performance_case_id
    )
    assert seen == [case.database_fingerprint]


@pytest.mark.asyncio
async def test_leftover_test_index_on_the_table_stops_the_benchmark_before_any_work(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Finding 84: a test index left by a lost run changes the baseline plan,
    # so the new index is measured against the wrong "before".
    app = _app(server_config_factory)
    session_id, candidate_id = _candidate(app, SQL)
    leftover = ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=7,
        name="IX_Testing_0123456789abcdef",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("status"),),
        include_columns=(),
        filter_definition=None,
        is_unique=False,
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes", AsyncMock(return_value=[leftover])
    )
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    assert result["classification"] == "cleanup_required"
    assert result["failure_code"] == "leftover_test_index"
    assert result["leftover_test_indexes"] == ["IX_Testing_0123456789abcdef"]
    assert result["executions"] == 0
    assert "DROP INDEX [IX_Testing_0123456789abcdef] ON [dbo].[Items]" in result["remediation"]
    app._create_test_index.assert_not_awaited()
    app.plans.profile_query.assert_not_awaited()
    assert app.tuning_sessions.get_candidate(candidate_id).is_terminal is False


@pytest.mark.asyncio
async def test_create_that_the_engine_rolled_back_leaves_no_cleanup_debt(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A lock-timeout abort (1222) rolled the CREATE back. Recording it as
    # cleanup_required blocked every later index benchmark on the database.
    app = _app(server_config_factory)
    session_id, candidate_id = _candidate(app, SQL)
    app._create_test_index = AsyncMock(  # type: ignore[method-assign]
        side_effect=RuntimeError(
            "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Lock request time out "
            "period exceeded. (1222)"
        )
    )
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]
    _install_catalog(monkeypatch, key_columns=["id"], sequence=("empty", "empty", "empty"))
    app.plans.profile_query = AsyncMock(side_effect=[_profile(100) for _ in range(3)])  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    assert result["classification"] == "inconclusive"
    assert result["lease"]["status"] == "create_failed"
    assert result["create_failure"]["outcome"] == "definite_rollback"
    assert result["create_failure"]["error_number"] == 1222
    assert app.performance_store.list_open_index_leases() == []
    app._drop_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_with_an_unknown_outcome_still_requires_cleanup(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The gate must not loosen: a lost connection may have committed the index.
    app = _app(server_config_factory)
    session_id, candidate_id = _candidate(app, SQL)
    app._create_test_index = AsyncMock(  # type: ignore[method-assign]
        side_effect=ConnectionError("Communication link failure")
    )
    _install_catalog(monkeypatch, key_columns=["id"], sequence=("empty", "empty", "empty"))
    app.plans.profile_query = AsyncMock(side_effect=[_profile(100) for _ in range(3)])  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    assert result["lease"]["status"] == "cleanup_required"


@pytest.mark.asyncio
async def test_parameter_cases_can_be_named_or_left_out(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Retyping every value of every case on each call is where agents made
    # the fingerprint mismatches the log shows.
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE id = @id"
    cases = [{"name": "common", "values": {"id": 5}, "types": {"id": "int"}, "weight": 1}]
    session_id, candidate_id = _candidate(app, sql, parameter_cases=cases)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(monkeypatch, key_columns=["id"])
    app.plans.profile_parameterized_query = AsyncMock(side_effect=_profiles(index_name))  # type: ignore[method-assign]
    app.param_binding.build_parameter_bucket = AsyncMock(  # type: ignore[method-assign]
        side_effect=lambda *_a, **kw: {"values": kw["parameter_values"]}
    )
    contract = AsyncMock()
    contract.parameters = (("id", 5),)
    app.param_binding.build_execution_contract = lambda sql_text, _bucket, **_kw: contract  # type: ignore[method-assign]

    result = await app._benchmark_index_candidate(
        "appdb", session_id, candidate_id, sql, "dbo", "Items", ["id"], None, None, False,
        "screening", True, True, 30, "named-cases", [{"name": "common"}],
    )

    assert result["parameter_results"][0]["parameter_case"] == "common"
    assert app.plans.profile_parameterized_query.await_count == 9
