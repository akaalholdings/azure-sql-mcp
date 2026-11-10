"""Parameter cases in the index benchmark (friction F04, F09, F10; proposal P4).

One build must answer the question for every parameter case: a per-case
table, the case that gains least, a run count the caller can lower, and the
plan-reuse mode a plan cache gives a parameterized statement.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.performance_workflows import aggregate_samples
from azure_sql_mcp.performance_workflows import classify_benchmark
from azure_sql_mcp.performance_workflows import compact_benchmark_result
from azure_sql_mcp.server import TEST_INDEX_PREFIX
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.server import _compact_benchmark_payload
from tests.unit.test_3_0_wiring2_index_set import _Sandbox
from tests.unit.test_3_0_wiring2_index_set import _app
from tests.unit.test_3_0_wiring2_index_set import _benchmark
from tests.unit.test_3_0_wiring2_index_set import _names
from tests.unit.test_3_0_wiring2_index_set import _plan
from tests.unit.test_3_0_wiring2_index_set import _session
from tests.unit.test_index_benchmark_workflow import _profile

SQL = "SELECT p.Id FROM dbo.Posts AS p WHERE p.OwnerUserId = @u"
CASES = [
    {"name": "selective", "values": {"u": 22656}, "types": {"u": "int"}, "weight": 1.0},
    {"name": "common", "values": {"u": 5}, "types": {"u": "int"}, "weight": 1.0},
    {"name": "all", "values": {"u": 0}, "types": {"u": "int"}, "weight": 1.0},
]
OWNER = {"table_name": "Posts", "key_columns": ["OwnerUserId"]}


def _scripted(
    app: AzureSqlMcpApplication,
    sandbox: _Sandbox,
    with_index_ms: dict[str, float],
    *,
    baseline_ms: float = 100,
) -> list[Any]:
    """Each case runs at ``baseline_ms`` without the index and at its own time with it."""

    [index_name] = _names(TEST_INDEX_PREFIX, OWNER)
    contracts: list[Any] = []

    async def profile(_database: str, contract: Any, **_kwargs: Any) -> Any:
        contracts.append(contract)
        if index_name in sandbox.indexes:
            return _plan(index_name, elapsed_ms=with_index_ms[contract.bucket_id])
        return _profile(baseline_ms)

    app.plans.profile_parameterized_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]
    return contracts


@pytest.mark.asyncio
async def test_three_cases_are_measured_under_one_build_with_a_table_and_a_worst_case(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL, parameter_cases=CASES)
    contracts = _scripted(app, sandbox, {"selective": 10, "common": 50, "all": 98})

    result = await _benchmark(
        app,
        session_id,
        candidate_id,
        sql=SQL,
        phase="finalist",
        parameter_cases=CASES,
        indexes=[OWNER],
        runs=2,
    )

    assert len(sandbox.created) == 1 and sandbox.dropped == sandbox.created
    # Each case: 2 runs before, 2 with the index, 2 after it was dropped.
    assert result["executions"] == 3 * 2 * 3
    assert [contract.bucket_id for contract in contracts[:6]] == [
        "selective", "selective", "common", "common", "all", "all",
    ]
    decision = _compact_benchmark_payload(result)["decision"]
    assert [(row["parameter_case"], row["verdict"]) for row in decision["cases"]] == [
        ("selective", "improved"),
        ("common", "improved"),
        ("all", "no_material_difference"),
    ]
    assert decision["classification"] == "improved"
    assert decision["worst_case"] == {
        "parameter_case": "all",
        "verdict": "no_material_difference",
        "speedup": 1.02,
    }


@pytest.mark.asyncio
async def test_the_worst_case_is_the_regressed_one_even_when_others_gain_more(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL, parameter_cases=CASES)
    _scripted(app, sandbox, {"selective": 1, "common": 300, "all": 50})

    result = await _benchmark(
        app, session_id, candidate_id, sql=SQL, parameter_cases=CASES, indexes=[OWNER]
    )

    decision = _compact_benchmark_payload(result)["decision"]
    assert decision["classification"] == "regressed"
    assert decision["worst_case"]["parameter_case"] == "common"
    assert decision["worst_case"]["verdict"] == "regressed"


def test_one_case_needs_no_worst_case_row() -> None:
    samples = [
        {
            "elapsed_ms": value,
            "elapsed_source": "showplan_query_time_stats",
        }
        for value in (100.0, 100.0, 100.0)
    ]
    case = {
        "parameter_case": "default",
        "baseline": aggregate_samples(samples),
        "candidate": aggregate_samples([{**item, "elapsed_ms": 10.0} for item in samples]),
    }
    classify_benchmark([case], [], require_equivalence=False)

    decision = compact_benchmark_result({"parameter_results": [case]})["decision"]

    assert "worst_case" not in decision


@pytest.mark.asyncio
@pytest.mark.parametrize("runs", [0, 4])
async def test_runs_outside_the_session_limit_are_refused_before_any_work(
    server_config_factory, monkeypatch: pytest.MonkeyPatch, runs: int
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL, parameter_cases=CASES)

    with pytest.raises(ValueError, match="runs must be between 1 and the screening limit of 3"):
        await _benchmark(
            app, session_id, candidate_id, sql=SQL, parameter_cases=CASES,
            indexes=[OWNER], runs=runs,
        )

    assert sandbox.create_calls == []


@pytest.mark.asyncio
async def test_a_finalist_with_a_subset_says_how_many_cases_and_what_to_use_instead(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F10: 'must cover every registered parameter case' cost O03 one call."""

    app = _app(server_config_factory)
    _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL, parameter_cases=CASES)

    with pytest.raises(ValueError) as refusal:
        await _benchmark(
            app, session_id, candidate_id, sql=SQL, phase="finalist",
            parameter_cases=CASES[:2], indexes=[OWNER],
        )

    message = str(refusal.value)
    assert "all 3 registered parameter cases" in message
    assert "2" in message
    assert 'phase="screening"' in message


@pytest.mark.asyncio
async def test_each_case_compiles_its_own_plan_by_default(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL, parameter_cases=CASES)
    contracts = _scripted(app, sandbox, {"selective": 10, "common": 50, "all": 98})

    result = await _benchmark(
        app, session_id, candidate_id, sql=SQL, parameter_cases=CASES, indexes=[OWNER]
    )

    texts = {contract.bucket_id: contract.sql_text for contract in contracts}
    assert len(set(texts.values())) == 3
    assert result["plan_reuse"] == "per_case"
    assert [item["plan_compiled_for"] for item in result["parameter_results"]] == [
        "selective", "common", "all",
    ]


@pytest.mark.asyncio
async def test_first_case_plan_reuse_runs_every_case_on_one_statement_text(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A plan cache compiles once: the later cases run on the plan of the first case."""

    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL, parameter_cases=CASES)
    contracts = _scripted(app, sandbox, {"selective": 10, "common": 50, "all": 98})

    result = await _benchmark(
        app, session_id, candidate_id, sql=SQL, parameter_cases=CASES,
        indexes=[OWNER], plan_reuse="first_case",
    )

    # One text for all cases: the engine reuses the plan the first case compiled.
    assert len({contract.sql_text for contract in contracts}) == 1
    assert result["plan_reuse"] == "first_case"
    assert [item["plan_compiled_for"] for item in result["parameter_results"]] == [
        "selective", "selective", "selective",
    ]


@pytest.mark.asyncio
async def test_an_unknown_plan_reuse_mode_is_refused_with_the_accepted_values(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL, parameter_cases=CASES)

    with pytest.raises(ValueError, match="plan_reuse must be per_case or first_case"):
        await _benchmark(
            app, session_id, candidate_id, sql=SQL, parameter_cases=CASES,
            indexes=[OWNER], plan_reuse="shared",
        )


@pytest.mark.asyncio
async def test_the_tool_passes_runs_plan_reuse_and_candidate_sql(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL, parameter_cases=CASES)
    app._benchmark_index_candidate = AsyncMock(return_value={"classification": "neutral"})  # type: ignore[method-assign]

    await app.mcp._tool_manager.call_tool(
        "benchmark_index_candidate",
        {
            "session_id": session_id,
            "candidate_id": candidate_id,
            "sql": SQL,
            "candidate_sql": SQL + " OPTION (RECOMPILE)",
            "indexes": [OWNER],
            "runs": 2,
            "plan_reuse": "first_case",
            "idempotency_key": "tool-key",
        },
    )

    kwargs = app._benchmark_index_candidate.await_args.kwargs
    assert kwargs["runs"] == 2
    assert kwargs["plan_reuse"] == "first_case"
    assert kwargs["candidate_sql"] == SQL + " OPTION (RECOMPILE)"
