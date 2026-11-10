"""B2 wiring: proof scopes, session budgets and hint profiles reach the tools."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.equivalence_contract import statement_hint_profile
from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_server import make_config
from tests.unit.test_server import stub_case_preflight


def _policy(**overrides: Any) -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "max_benchmark_executions": 80,
                    "max_tuning_candidates": 20,
                    "max_tuning_session_executions": 400,
                    "max_tuning_session_minutes": 120,
                    **overrides,
                }
            },
        }
    )


def _use_policy(app: AzureSqlMcpApplication, policy: DatabasePolicySet) -> None:
    app.database_policy = policy
    app.performance_workflows.database_policy = policy


async def _call(app: AzureSqlMcpApplication, name: str, arguments: dict[str, Any]) -> Any:
    return await app.mcp._tool_manager.call_tool(name, arguments)


@pytest.fixture
def app(tmp_path: Path) -> AzureSqlMcpApplication:
    application = AzureSqlMcpApplication(make_config(tmp_path))
    _use_policy(application, _policy())
    return application


def _session(app: AzureSqlMcpApplication) -> tuple[str, str]:
    case = app.performance_workflows.start_case("appdb", "SELECT object_id FROM sys.objects")
    session = app.performance_workflows.start_session(case.case_id, "appdb")
    return case.case_id, session["session_id"]


@pytest.mark.asyncio
async def test_start_tuning_session_without_limits_takes_the_protocol_default(
    app: AzureSqlMcpApplication,
) -> None:
    # The tool's literal 80 could not pay for the documented screening and
    # finalist runs; the module default (120) only applies when the tool passes none.
    case = app.performance_workflows.start_case("appdb", "SELECT object_id FROM sys.objects")

    session = await _call(app, "start_tuning_session", {"case_id": case.case_id})

    assert session["execution_limit"] == 120
    assert session["max_candidates"] == 10


@pytest.mark.asyncio
async def test_start_tuning_session_still_refuses_a_stated_limit_over_policy(
    app: AzureSqlMcpApplication,
) -> None:
    case = app.performance_workflows.start_case("appdb", "SELECT object_id FROM sys.objects")

    with pytest.raises(Exception, match="401"):
        await _call(
            app, "start_tuning_session", {"case_id": case.case_id, "execution_limit": 401}
        )


@pytest.mark.asyncio
async def test_get_tuning_session_is_a_summary_unless_full_is_asked(
    app: AzureSqlMcpApplication,
) -> None:
    # A campaign that reads the session after each benchmark paid for every
    # earlier evidence record again on each call.
    _, session_id = _session(app)

    summary = await _call(app, "get_tuning_session", {"session_id": session_id})
    full = await _call(app, "get_tuning_session", {"session_id": session_id, "view": "full"})

    assert "evidence" not in summary
    assert full["evidence"] == []


@pytest.mark.asyncio
async def test_finalize_verifies_declared_preconditions_in_the_catalog(
    app: AzureSqlMcpApplication,
) -> None:
    # Without this call every rewrite stays deployment_ready=false, and with
    # an unverified list a NOT IN rewrite could be called constraint-backed.
    _, session_id = _session(app)
    verification = {"declared": [{"kind": "not_null", "verified": True}], "requirements": []}
    app.performance_workflows.verify_logical_preconditions = AsyncMock(return_value=verification)
    app.performance_workflows.finalize_session = Mock(return_value={"session": {}})
    preconditions = [
        {"kind": "not_null", "schema_name": "dbo", "table_name": "Posts", "column_name": "OwnerUserId"}
    ]

    await _call(
        app,
        "finalize_tuning_session",
        {
            "session_id": session_id,
            "selected_candidate_id": "cand-1",
            "stopping_reason": "winner found",
            "logical_preconditions": preconditions,
        },
    )

    app.performance_workflows.verify_logical_preconditions.assert_awaited_once_with(
        "appdb", session_id, "cand-1", preconditions
    )
    assert (
        app.performance_workflows.finalize_session.call_args.kwargs["precondition_verification"]
        is verification
    )


@pytest.mark.asyncio
async def test_finalize_without_a_winner_reads_no_catalog(app: AzureSqlMcpApplication) -> None:
    _, session_id = _session(app)
    app.performance_workflows.verify_logical_preconditions = AsyncMock()

    result = await _call(
        app,
        "finalize_tuning_session",
        {"session_id": session_id, "stopping_reason": "nothing better"},
    )

    app.performance_workflows.verify_logical_preconditions.assert_not_awaited()
    assert result["session"]["status"] == "completed"


@pytest.mark.asyncio
async def test_benchmark_tuning_candidate_forwards_the_cross_case(
    app: AzureSqlMcpApplication,
) -> None:
    # The parameter-sensitivity test only exists if the tool can ask for it.
    _, session_id = _session(app)
    app.performance_workflows.benchmark_candidate = AsyncMock(
        return_value={"classification": "regressed"}
    )

    await _call(
        app,
        "benchmark_tuning_candidate",
        {
            "session_id": session_id,
            "candidate_id": "cand-1",
            "baseline_sql": "SELECT 1",
            "candidate_sql": "SELECT 1",
            "cross_case": {"compile_case": "rare", "run_case": "common"},
        },
    )

    assert app.performance_workflows.benchmark_candidate.await_args.kwargs["cross_case"] == {
        "compile_case": "rare",
        "run_case": "common",
    }


@pytest.mark.asyncio
async def test_tune_query_on_a_read_only_policy_returns_evidence_not_an_error(
    tmp_path: Path,
) -> None:
    # The evidence was already collected; refusing the whole call threw it away.
    app = AzureSqlMcpApplication(make_config(tmp_path))
    _use_policy(app, _policy(allow_benchmark=False))
    stub_case_preflight(app)
    app._collect_performance_evidence = AsyncMock(  # type: ignore[method-assign]
        return_value={"outcome": "healthy"}
    )

    result = await _call(
        app, "tune_query", {"sql": "SELECT object_id FROM sys.objects", "analyze": False}
    )

    assert result["evidence"] == {"outcome": "healthy"}
    assert result["tuning_session_id"] is None
    assert result["session"]["session_available"] is False
    assert "policy" in result["next_step"]


@pytest.mark.asyncio
async def test_benchmark_query_rewrite_states_the_policy_reason_and_measures_nothing(
    tmp_path: Path,
) -> None:
    app = AzureSqlMcpApplication(make_config(tmp_path))
    _use_policy(app, _policy(allow_benchmark=False))
    stub_case_preflight(app)
    app.performance_workflows.benchmark_candidate = AsyncMock()

    with pytest.raises(Exception, match="does not permit a tuning session"):
        await _call(
            app,
            "benchmark_query_rewrite",
            {
                "baseline_sql": "SELECT object_id FROM sys.objects",
                "rewrite_sql": "SELECT object_id FROM sys.objects AS o",
            },
        )

    app.performance_workflows.benchmark_candidate.assert_not_awaited()


def test_hint_profile_is_the_validator_inventory() -> None:
    # Two scanners disagreed: the proof gate did not see a locking table hint
    # (HOLDLOCK changes what a statement reads and blocks) that the validator did.
    locking = statement_hint_profile("SELECT t.a FROM dbo.T AS t WITH (HOLDLOCK)")
    real = statement_hint_profile("SELECT p.Id FROM dbo.Posts AS p OPTION (FORCE ORDER, RECOMPILE)")

    assert locking["isolation_table_hints"] == ["HOLDLOCK"]
    assert real["hinted"] is True
    assert real["plan_forcing_hints"] == ["FORCE ORDER"]
    assert real["compile_options"] == ["RECOMPILE"]


@pytest.mark.asyncio
async def test_added_candidate_records_its_query_hints_without_values(
    app: AzureSqlMcpApplication,
) -> None:
    # The session must show which candidate is a hint, after the SQL text is gone.
    _, session_id = _session(app)

    candidate = await _call(
        app,
        "add_tuning_candidate",
        {
            "session_id": session_id,
            "candidate_sql": (
                "SELECT object_id FROM sys.objects WHERE object_id = 4 OPTION (MAXDOP 1, RECOMPILE)"
            ),
            "strategy": "cardinality",
        },
    )

    assert candidate["metadata"]["statement_hints"] == ["MAXDOP", "RECOMPILE"]
    assert "sys.objects" not in str(candidate)
