"""benchmark_indexed_view_candidate takes the options of benchmark_index_candidate.

O06: the view tool had no ``plan_reuse``, so the shared-plan run across the
parameter cases was not measured for the view. A replayed idempotency key
returned a notice instead of the result, and a baseline too slow to repeat
was run again for the view after the index tool had already measured it.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_3_0_s13b2_indexed_view_tool import BODY
from tests.unit.test_3_0_s13b2_indexed_view_tool import VIEW
from tests.unit.test_3_0_s13b2_indexed_view_tool import _ViewSandbox
from tests.unit.test_3_0_s13b2_indexed_view_tool import _app
from tests.unit.test_3_0_s13b2_indexed_view_tool import _run
from tests.unit.test_3_0_s13b2_indexed_view_tool import _session
from tests.unit.test_3_0_s13b2_indexed_view_tool import _candidate_sql
from tests.unit.test_3_0_s13b2_indexed_view_tool import _profiles
from tests.unit.test_index_benchmark_workflow import _profile

ORIGINAL = (
    "SELECT p.OwnerUserId, COUNT_BIG(*) AS PostCount FROM dbo.Posts AS p "
    "WHERE p.PostTypeId = 1 AND p.OwnerUserId = @u GROUP BY p.OwnerUserId"
)
CANDIDATE = (
    f"SELECT v.OwnerUserId, SUM(v.PostCount) AS PostCount FROM dbo.{VIEW} AS v "
    "WITH (NOEXPAND) WHERE v.OwnerUserId = @u GROUP BY v.OwnerUserId"
)
CASES = [
    {"name": "rare", "values": {"u": 22656}, "types": {"u": "int"}, "weight": 1.0},
    {"name": "common", "values": {"u": 5}, "types": {"u": "int"}, "weight": 1.0},
]


def _parameterized(
    app: AzureSqlMcpApplication, sandbox: _ViewSandbox, *, baseline_ms: float = 900
) -> tuple[str, str, list[Any]]:
    case = app.performance_workflows.start_case("appdb", ORIGINAL, parameter_cases=CASES)
    session_id = app.performance_workflows.start_session(case.case_id, "appdb")["session_id"]
    candidate_id = app.performance_workflows.add_candidate(
        session_id, CANDIDATE, strategy="rewrite_plus_index"
    )["candidate_id"]
    clustered = str(sandbox.plan.clustered_index.index_name)
    contracts: list[Any] = []

    async def profile(_database: str, contract: Any, **_kwargs: Any) -> Any:
        contracts.append(contract)
        reads_view = VIEW in contract.sql_text
        return _profile(4 if reads_view else baseline_ms, index_name=clustered if reads_view else None)

    app.plans.profile_parameterized_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]
    return session_id, candidate_id, contracts


async def _run_cases(app: AzureSqlMcpApplication, session_id: str, candidate_id: str, **options: Any) -> dict[str, Any]:
    return await app._benchmark_indexed_view_candidate(
        "appdb", session_id, candidate_id, BODY, sql=ORIGINAL, candidate_sql=CANDIDATE,
        view_name=VIEW, parameter_cases=CASES, idempotency_key=options.pop("key", "view-key"),
        **options,
    )


@pytest.mark.asyncio
async def test_first_case_plan_reuse_runs_every_case_of_the_view_statement_on_one_text(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _ViewSandbox(app, monkeypatch)
    session_id, candidate_id, contracts = _parameterized(app, sandbox)

    result = await _run_cases(app, session_id, candidate_id, plan_reuse="first_case")

    view_texts = {contract.sql_text for contract in contracts if VIEW in contract.sql_text}
    original_texts = {contract.sql_text for contract in contracts if VIEW not in contract.sql_text}
    # One text per statement: the plan the first case compiled serves the others.
    assert len(view_texts) == 1 and len(original_texts) == 1
    assert result["plan_reuse"] == "first_case"
    assert [item["plan_compiled_for"] for item in result["parameter_results"]] == ["rare", "rare"]
    assert result["classification"] in {"promising", "improved"}, result["reason"]


@pytest.mark.asyncio
async def test_per_case_plan_reuse_stays_the_default_with_one_text_per_case(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _ViewSandbox(app, monkeypatch)
    session_id, candidate_id, contracts = _parameterized(app, sandbox)

    result = await _run_cases(app, session_id, candidate_id)

    assert len({contract.sql_text for contract in contracts if VIEW in contract.sql_text}) == 2
    assert result["plan_reuse"] == "per_case"
    assert [item["plan_compiled_for"] for item in result["parameter_results"]] == ["rare", "common"]
    with pytest.raises(ValueError, match="plan_reuse must be per_case or first_case"):
        await _run_cases(app, session_id, candidate_id, plan_reuse="shared", key="other")


@pytest.mark.asyncio
async def test_a_declared_slow_baseline_runs_once_and_the_next_tool_does_not_run_it_again(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _ViewSandbox(app, monkeypatch)
    session_id, candidate_id, contracts = _parameterized(app, sandbox, baseline_ms=150_000)

    result = await _run_cases(
        app, session_id, candidate_id, slow_baseline_cases={"rare": 150, "common": 150}
    )

    originals = [contract.bucket_id for contract in contracts if VIEW not in contract.sql_text]
    assert originals == ["rare", "common"]  # once each, and not again after the drop
    assert [item["baseline_source"] for item in result["parameter_results"]] == [
        "single_run", "single_run",
    ]
    # The session keeps them: a second view candidate does not run the original at all.
    second = app.performance_workflows.add_candidate(
        session_id, CANDIDATE + " OPTION (MAXDOP 1)", strategy="rewrite_plus_index"
    )["candidate_id"]
    contracts.clear()
    again = await app._benchmark_indexed_view_candidate(
        "appdb", session_id, second, BODY, sql=ORIGINAL,
        candidate_sql=CANDIDATE + " OPTION (MAXDOP 1)", view_name=VIEW,
        parameter_cases=CASES, idempotency_key="view-key-2",
    )

    assert all(VIEW in contract.sql_text for contract in contracts)
    assert [item["baseline_source"] for item in again["parameter_results"]] == [
        "session_single_run", "session_single_run",
    ]
    with pytest.raises(ValueError, match="slow_baseline_cases"):
        await _run_cases(app, session_id, candidate_id, slow_baseline_cases={"nope": 5}, key="k3")


@pytest.mark.asyncio
async def test_a_replayed_idempotency_key_returns_the_stored_result_and_runs_nothing(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _ViewSandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, _candidate_sql())
    seen = _profiles(app, sandbox)
    first = await _run(app, session_id, candidate_id)
    events, runs = list(sandbox.events), len(seen)

    replayed = await _run(app, session_id, candidate_id)

    assert replayed["replayed"] is True
    assert sandbox.events == events and len(seen) == runs
    for key in (
        "classification", "performance_classification", "phase", "executions", "proof_scope",
        "evidence_id", "time_spent", "reason",
    ):
        assert replayed[key] == first[key], key
    for key in ("built", "reused", "still_held"):
        assert replayed["test_objects"][key] == first["test_objects"][key]
    stored, measured = replayed["parameter_results"][0], first["parameter_results"][0]
    for side in ("baseline", "candidate"):
        assert stored[side]["elapsed_ms"] == measured[side]["elapsed_ms"]
    assert stored["runs"] == measured["runs"]
    assert replayed["equivalence"][0]["status"] == first["equivalence"][0]["status"] == "match"
    assert replayed["view"]["view"] == first["view"]["view"]
    assert replayed["lease"]["status"] == "cleaned"


@pytest.mark.asyncio
async def test_the_tool_passes_the_new_options(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    _ViewSandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, _candidate_sql())
    app._benchmark_indexed_view_candidate = AsyncMock(return_value={"classification": "neutral"})  # type: ignore[method-assign]

    await app.mcp._tool_manager.call_tool(
        "benchmark_indexed_view_candidate",
        {
            "view_definition": BODY,
            "session_id": session_id,
            "candidate_id": candidate_id,
            "sql": ORIGINAL,
            "candidate_sql": CANDIDATE,
            "plan_reuse": "first_case",
            "slow_baseline_cases": {"rare": 150},
            "keep_for_session": True,
            "max_call_seconds": 600,
            "idempotency_key": "tool-key",
        },
    )

    kwargs = app._benchmark_indexed_view_candidate.await_args.kwargs
    assert kwargs["plan_reuse"] == "first_case"
    assert kwargs["slow_baseline_cases"] == {"rare": 150}
    assert kwargs["keep_for_session"] is True
    assert kwargs["max_call_seconds"] == 600
