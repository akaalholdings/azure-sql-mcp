"""A branching candidate is judged per executed branch (S14 item 1).

A rewrite into IF / ELSE often keeps the original statement for the parameter
values the new index does not serve. The case that runs that branch cannot use
the test index, by design. It must not make the whole candidate inconclusive;
it must still show a regression when the original branch got slower.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.server import TEST_INDEX_PREFIX
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.server import _compact_benchmark_payload
from tests.unit import test_3_0_s13b2_indexed_view_tool as view_tool
from tests.unit.test_3_0_wiring2_index_set import _Sandbox
from tests.unit.test_3_0_wiring2_index_set import _app
from tests.unit.test_3_0_wiring2_index_set import _benchmark
from tests.unit.test_3_0_wiring2_index_set import _names
from tests.unit.test_index_benchmark_workflow import _profile

ORIGINAL = "SELECT p.Id FROM dbo.Posts AS p WHERE p.OwnerUserId = @u OR @u = 0"
SERVED = "SELECT p.Id FROM dbo.Posts AS p WHERE p.OwnerUserId = @u"
BATCH = f"IF @u = 0\n    {ORIGINAL}\nELSE\n    {SERVED}"
CASES = [
    {"name": "one_owner", "values": {"u": 22656}, "types": {"u": "int"}, "weight": 1.0},
    {"name": "all_owners", "values": {"u": 0}, "types": {"u": "int"}, "weight": 1.0},
]
OWNER = {"table_name": "Posts", "key_columns": ["OwnerUserId"]}
_NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"


def _branch_plan(statement: str, *, elapsed_ms: float, index_name: str | None) -> Any:
    """An actual plan of the batch: only the branch that ran has a query plan."""

    profile = _profile(elapsed_ms)
    scan = f'<IndexScan Index="[dbo].[{index_name}]" />' if index_name else ""
    object.__setattr__(
        profile.plan,
        "raw_xml",
        f'<ShowPlanXML xmlns="{_NS}"><StmtCond StatementText="IF @u = 0" />'
        f'<StmtSimple StatementText="{statement}"><QueryPlan>{scan}</QueryPlan></StmtSimple>'
        "</ShowPlanXML>",
    )
    return profile


def _session(app: AzureSqlMcpApplication) -> tuple[str, str]:
    case = app.performance_workflows.start_case("appdb", ORIGINAL, parameter_cases=CASES)
    session = app.performance_workflows.start_session(case.case_id, "appdb")
    candidate = app.performance_workflows.add_candidate(
        session["session_id"], BATCH, strategy="rewrite_plus_index"
    )
    return session["session_id"], candidate["candidate_id"]


def _scripted(
    app: AzureSqlMcpApplication, sandbox: _Sandbox, *, original_branch_ms: float
) -> None:
    """The ELSE branch seeks the test index; the IF branch is the original statement."""

    [index_name] = _names(TEST_INDEX_PREFIX, OWNER)

    async def profile(_database: str, contract: Any, **_kwargs: Any) -> Any:
        if index_name not in sandbox.indexes:
            return _profile(100)
        if contract.bucket_id == "all_owners":
            return _branch_plan(ORIGINAL, elapsed_ms=original_branch_ms, index_name=None)
        return _branch_plan(SERVED, elapsed_ms=10, index_name=index_name)

    app.plans.profile_parameterized_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]


async def _run(app: AzureSqlMcpApplication, session_id: str, candidate_id: str) -> dict[str, Any]:
    return await _benchmark(
        app,
        session_id,
        candidate_id,
        sql=ORIGINAL,
        candidate_sql=BATCH,
        parameter_cases=CASES,
        indexes=[OWNER],
        runs=2,
    )


@pytest.mark.asyncio
async def test_the_case_on_the_original_branch_is_unchanged_by_design_not_inconclusive(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app)
    _scripted(app, sandbox, original_branch_ms=100)

    result = await _run(app, session_id, candidate_id)

    assert result["classification"] == "promising", result["reason"]
    served, original = result["parameter_results"]
    assert "case_verdict" not in served
    assert original["case_verdict"] == "unchanged_by_design"
    assert original["executed_branch"]["ordinal"] == 1
    assert original["executed_branch"]["conditions"] == [
        {"condition": "@u = 0", "is_true": True}
    ]
    assert "does not use the test index" in original["case_verdict_note"]
    assert served["executed_branch"]["ordinal"] == 2
    decision = _compact_benchmark_payload(result)["decision"]
    assert [(row["parameter_case"], row["verdict"]) for row in decision["cases"]] == [
        ("one_owner", "improved"),
        ("all_owners", "unchanged_by_design"),
    ]


@pytest.mark.asyncio
async def test_a_slower_original_branch_is_still_a_regression(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app)
    _scripted(app, sandbox, original_branch_ms=400)

    result = await _run(app, session_id, candidate_id)

    assert result["classification"] == "regressed"
    assert "all_owners" in result["reason"]
    assert "case_verdict" not in result["parameter_results"][1]


@pytest.mark.asyncio
async def test_no_case_on_a_branch_that_uses_the_index_stays_inconclusive(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nothing measured the test index: there is no verdict to take from other cases."""

    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app)
    [index_name] = _names(TEST_INDEX_PREFIX, OWNER)

    async def profile(_database: str, _contract: Any, **_kwargs: Any) -> Any:
        if index_name not in sandbox.indexes:
            return _profile(100)
        return _branch_plan(ORIGINAL, elapsed_ms=100, index_name=None)

    app.plans.profile_parameterized_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]

    result = await _run(app, session_id, candidate_id)

    assert result["classification"] == "inconclusive"
    assert "not used" in result["reason"]


@pytest.mark.asyncio
async def test_a_statement_without_branches_that_skips_the_index_stays_inconclusive(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    case = app.performance_workflows.start_case("appdb", SERVED, parameter_cases=CASES)
    session = app.performance_workflows.start_session(case.case_id, "appdb")
    candidate = app.performance_workflows.add_candidate(
        session["session_id"], SERVED, strategy="index"
    )
    [index_name] = _names(TEST_INDEX_PREFIX, OWNER)

    async def profile(_database: str, contract: Any, **_kwargs: Any) -> Any:
        if index_name not in sandbox.indexes or contract.bucket_id == "all_owners":
            return _profile(100)
        return _profile(10, index_name=index_name)

    app.plans.profile_parameterized_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]

    result = await _benchmark(
        app,
        session["session_id"],
        candidate["candidate_id"],
        sql=SERVED,
        parameter_cases=CASES,
        indexes=[OWNER],
        runs=2,
    )

    assert result["classification"] == "inconclusive"


# --- the indexed-view tool -------------------------------------------------------

VIEW_ORIGINAL = (
    "SELECT p.OwnerUserId, COUNT_BIG(*) AS PostCount FROM dbo.Posts AS p "
    "WHERE p.PostTypeId = @t GROUP BY p.OwnerUserId"
)
VIEW_CASES = [
    {"name": "questions", "values": {"t": 1}, "types": {"t": "int"}, "weight": 1.0},
    {"name": "answers", "values": {"t": 2}, "types": {"t": "int"}, "weight": 1.0},
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("original_branch_ms", "classification", "case_verdict"),
    [(900, "improved", "unchanged_by_design"), (4000, "regressed", None)],
)
async def test_the_view_tool_judges_a_branching_candidate_per_executed_branch(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
    original_branch_ms: float,
    classification: str,
    case_verdict: str | None,
) -> None:
    """The view holds PostTypeId = 1 only; every other value keeps the original."""

    reads_view = view_tool._candidate_sql()
    batch = f"IF @t = 1\n    {reads_view}\nELSE\n    {VIEW_ORIGINAL}"
    app = view_tool._app(server_config_factory)
    sandbox = view_tool._ViewSandbox(app, monkeypatch)
    case = app.performance_workflows.start_case("appdb", VIEW_ORIGINAL, parameter_cases=VIEW_CASES)
    session = app.performance_workflows.start_session(case.case_id, "appdb")
    candidate = app.performance_workflows.add_candidate(
        session["session_id"], batch, strategy="rewrite_plus_index"
    )
    clustered = str(sandbox.plan.clustered_index.index_name)

    async def profile(_database: str, contract: Any, **_kwargs: Any) -> Any:
        if not sandbox.state.exists:
            return _profile(900)
        if contract.bucket_id == "answers":
            return _branch_plan(VIEW_ORIGINAL, elapsed_ms=original_branch_ms, index_name=None)
        return _branch_plan(reads_view, elapsed_ms=4, index_name=clustered)

    app.plans.profile_parameterized_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]

    result = await app._benchmark_indexed_view_candidate(
        "appdb",
        session["session_id"],
        candidate["candidate_id"],
        view_tool.BODY,
        sql=VIEW_ORIGINAL,
        candidate_sql=batch,
        view_name=view_tool.VIEW,
        phase="finalist",
        idempotency_key="view-branch",
        parameter_cases=VIEW_CASES,
        runs=2,
    )

    assert result["classification"] == classification, result["reason"]
    served, original = result["parameter_results"]
    assert served["executed_branch"]["ordinal"] == 1
    assert original["executed_branch"]["ordinal"] == 2
    assert original.get("case_verdict") == case_verdict
    assert "case_verdict" not in served
