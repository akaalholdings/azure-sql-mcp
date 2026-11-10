"""Time: one run of each side for a large gain, a cap per call, one retry, and a breakdown.

O01: two calls of 11 minutes on a 7.6M-row table, because baselines of 6 to 7
seconds ran six times per case. D08: one call of 22 minutes with no way to
bound it. O06: a build died as a deadlock victim after 8.5 minutes and was
not retried. No result said where the time went.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.index_optimizer import classify_create_index_failure
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.tuning_sessions import decisive_first_runs
from tests.unit.test_3_0_s13b2_indexed_view_tool import BODY
from tests.unit.test_3_0_s13b2_indexed_view_tool import ORIGINAL as VIEW_ORIGINAL
from tests.unit.test_3_0_s13b2_indexed_view_tool import VIEW
from tests.unit.test_3_0_s13b2_indexed_view_tool import _ViewSandbox
from tests.unit.test_3_0_s13b2_indexed_view_tool import _app as _view_app
from tests.unit.test_3_0_s13b2_indexed_view_tool import _candidate_sql as _view_sql
from tests.unit.test_3_0_s13d_session_held import ORIGINAL
from tests.unit.test_3_0_s13d_session_held import REWRITE
from tests.unit.test_3_0_s13d_session_held import IndexSandbox
from tests.unit.test_3_0_s13d_session_held import _bench
from tests.unit.test_3_0_s13d_session_held import _candidate
from tests.unit.test_3_0_s13d_session_held import _session
from tests.unit.test_index_benchmark_workflow import _app
from tests.unit.test_index_benchmark_workflow import _profile

DEADLOCK = RuntimeError(
    "Transaction (Process ID 71) was deadlocked on lock resources with another process "
    "and has been chosen as the deadlock victim. Rerun the transaction. (1205)"
)


def _timed(app: AzureSqlMcpApplication, sandbox: IndexSandbox, baseline_ms: float, with_index_ms: float) -> list[bool]:
    """Profile runs that take ``baseline_ms`` without an index and ``with_index_ms`` with one."""

    runs: list[bool] = []

    async def profile(_database: str, _sql: str, **_kwargs: Any) -> Any:
        used = next(iter(sandbox.indexes), None)
        runs.append(used is not None)
        return _profile(with_index_ms if used else baseline_ms, index_name=used)

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]
    return runs


@pytest.mark.parametrize(
    ("baseline_seconds", "candidate_seconds", "decisive"),
    [
        (6.5, 0.07, True),
        (6.5, 0.65, True),
        (6.5, 0.7, False),
        # Under one second the statement is cheap to repeat and the timer is coarse.
        (0.5, 0.001, False),
        (150.0, 2.0, True),
    ],
)
def test_one_run_of_each_side_decides_only_for_a_tenfold_gain_on_a_statement_of_a_second_or_more(
    baseline_seconds: float, candidate_seconds: float, decisive: bool
) -> None:
    assert decisive_first_runs(baseline_seconds, candidate_seconds) is decisive


@pytest.mark.asyncio
async def test_a_large_gain_on_a_statement_of_seconds_is_decided_by_one_run_of_each_side(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O01: a 6.5 s baseline against a 70 ms candidate needs 2 runs, not 9."""

    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    candidate_id = _candidate(app, session_id, ORIGINAL, "index")
    runs = _timed(app, sandbox, 6_500, 70)

    result = await _bench(app, session_id, candidate_id, "k1", phase="finalist")

    assert (runs.count(False), runs.count(True)) == (1, 1)
    case = result["parameter_results"][0]
    assert case["runs"] == {"baseline_before": 1, "candidate": 1, "baseline_after": 0}
    assert case["run_rule"] == "one_run_each_large_gain"
    assert "one tenth" in case["run_rule_note"]
    assert case["baseline_single_sample"] is True and case["candidate_single_sample"] is True
    assert result["classification"] == "improved", result["reason"]
    assert result["equivalence"][0]["status"] == "match"
    assert sandbox.indexes == {}


@pytest.mark.asyncio
async def test_a_small_gain_on_a_statement_of_seconds_keeps_the_repeated_runs(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    candidate_id = _candidate(app, session_id, ORIGINAL, "index")
    runs = _timed(app, sandbox, 4_000, 3_000)

    result = await _bench(app, session_id, candidate_id, "k1", phase="finalist")

    # One baseline run before the build; the rest follow the drop once the
    # first candidate run has shown that the gain is not large.
    case = result["parameter_results"][0]
    assert case["runs"] == {"baseline_before": 1, "candidate": 3, "baseline_after": 3}
    assert "run_rule" not in case
    assert (runs.count(False), runs.count(True)) == (4, 3)
    assert result["equivalence"][0]["status"] == "match"


@pytest.mark.asyncio
async def test_every_index_benchmark_result_says_where_the_time_went(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    candidate_id = _candidate(app, session_id, REWRITE, "rewrite_plus_index")
    _timed(app, sandbox, 100, 20)

    result = await _bench(app, session_id, candidate_id, "k1", candidate_sql=REWRITE)

    spent = result["time_spent"]
    assert set(spent) == {
        "baseline_runs", "build", "candidate_runs", "comparison", "cleanup", "other", "total",
    }
    assert all(isinstance(value, float) and value >= 0 for value in spent.values())
    assert spent["total"] >= spent["build"] + spent["cleanup"]


@pytest.mark.asyncio
async def test_a_call_at_its_cap_returns_a_labelled_partial_result_and_keeps_the_held_index(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D08: one call ran 22 minutes; the next call paid for the build again."""

    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    first = _candidate(app, session_id, REWRITE, "rewrite_plus_index")
    _timed(app, sandbox, 100, 20)
    clock = {"now": 1000.0}
    monkeypatch.setattr("azure_sql_mcp.server.time.monotonic", lambda: clock["now"])
    create = sandbox._create

    async def slow_create(*args: Any, **kwargs: Any) -> dict[str, Any]:
        clock["now"] += 500  # the build uses up the call
        return await create(*args, **kwargs)

    app._create_test_index = AsyncMock(side_effect=slow_create)  # type: ignore[method-assign]

    result = await _bench(
        app, session_id, first, "k1", candidate_sql=REWRITE,
        keep_for_session=True, max_call_seconds=300,
    )

    assert result["classification"] == "inconclusive"
    assert result["partial"] is True
    assert result["failure_diagnostic"]["diagnostic_code"] == "call_time_cap"
    assert result["failure_diagnostic"]["failed_step"] == "candidate_measure"
    assert "max_call_seconds" in result["reason"]
    assert result["time_spent"]["build"] == 500.0
    # The index that took the time is still there for the next call.
    held = result["test_objects"]["still_held"]
    assert [item["index_name"] for item in held] == list(sandbox.indexes)

    # The same rewrite can be measured again: its first candidate has no verdict.
    again = _candidate(app, session_id, REWRITE, "rewrite_plus_index")
    app._create_test_index = AsyncMock(side_effect=create)  # type: ignore[method-assign]
    finished = await _bench(app, session_id, again, "k2", candidate_sql=REWRITE)

    assert finished["classification"] in {"promising", "improved"}, finished["reason"]
    assert [item["index_name"] for item in finished["test_objects"]["reused"]] == [
        held[0]["index_name"]
    ]
    assert finished["time_spent"]["build"] == 0.0


@pytest.mark.asyncio
async def test_a_build_the_cap_cannot_afford_is_not_started(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    first = _candidate(app, session_id, ORIGINAL, "index")
    second = _candidate(app, session_id, REWRITE, "rewrite_plus_index")
    _timed(app, sandbox, 100, 20)
    clock = {"now": 1000.0}
    monkeypatch.setattr("azure_sql_mcp.server.time.monotonic", lambda: clock["now"])
    create = sandbox._create

    async def slow_create(*args: Any, **kwargs: Any) -> dict[str, Any]:
        clock["now"] += 400
        return await create(*args, **kwargs)

    app._create_test_index = AsyncMock(side_effect=slow_create)  # type: ignore[method-assign]
    app._table_row_count = AsyncMock(return_value=8_000_000)  # type: ignore[method-assign]
    app._built_index_facts = AsyncMock(  # type: ignore[method-assign]
        return_value={"size_mb": 100.0, "index_depth": 3, "row_count": 8_000_000}
    )
    measured = await _bench(app, session_id, first, "k1")
    assert measured["build_rate_rows_per_second"] == 20_000.0
    events = len(sandbox.events)

    # The same table at the measured rate needs 400 s: more than the cap.
    result = await _bench(
        app, session_id, second, "k2", key_columns=["status"], candidate_sql=REWRITE,
        max_call_seconds=120,
    )

    assert result["partial"] is True
    assert result["failure_diagnostic"]["failed_step"] == "create_index"
    estimate = result["build_estimate"]
    assert estimate["estimated_seconds"] == 400.0
    assert estimate["table_rows"] == 8_000_000
    assert estimate["rate_rows_per_second"] == 20_000.0
    assert len(sandbox.events) == events  # no build was started
    assert sandbox.indexes == {}


def test_a_deadlock_victim_is_a_build_that_rolled_back_for_certain() -> None:
    failure = classify_create_index_failure(DEADLOCK)

    assert failure["outcome"] == "definite_rollback"
    assert failure["error_number"] == 1205
    assert failure["reason"] == "deadlock_victim"


@pytest.mark.asyncio
async def test_a_build_that_was_the_deadlock_victim_is_retried_once_and_the_result_says_so(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O06: 8.5 minutes of build lost to a deadlock with another build; no retry."""

    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    sandbox.create_errors.append(DEADLOCK)
    session_id, _case = _session(app)
    candidate_id = _candidate(app, session_id, ORIGINAL, "index")

    result = await _bench(app, session_id, candidate_id, "k1")

    index_name = result["indexes"][0]["index_name"]
    assert sandbox.events[:2] == [f"create:{index_name}", f"create:{index_name}"]
    assert result["classification"] in {"promising", "improved"}, result["reason"]
    assert result["build_retries"] == [
        {
            "index_name": index_name,
            "error_number": 1205,
            "reason": "deadlock_victim",
            "retried": True,
            "succeeded": True,
        }
    ]
    assert sandbox.indexes == {}


@pytest.mark.asyncio
async def test_a_second_deadlock_is_not_retried_again_and_is_marked_transient(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    sandbox.create_errors.extend([DEADLOCK, DEADLOCK])
    session_id, _case = _session(app)
    candidate_id = _candidate(app, session_id, ORIGINAL, "index")

    result = await _bench(app, session_id, candidate_id, "k1")

    assert len([event for event in sandbox.events if event.startswith("create:")]) == 2
    assert result["classification"] == "inconclusive"
    assert result["build_retries"][0]["succeeded"] is False
    assert result["create_failure"]["transient"] is True
    assert result["lease"]["status"] == "create_failed"
    assert app.performance_store.list_open_index_leases() == []


@pytest.mark.asyncio
async def test_the_view_benchmark_reports_time_and_decides_a_large_gain_with_one_run_each(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D01: a 6 to 11 s baseline ran three times before and three times after per case."""

    app = _view_app(server_config_factory)
    views = _ViewSandbox(app, monkeypatch)
    case = app.performance_workflows.start_case("appdb", VIEW_ORIGINAL)
    session_id = app.performance_workflows.start_session(case.case_id, "appdb")["session_id"]
    candidate_id = app.performance_workflows.add_candidate(
        session_id, _view_sql(), strategy="rewrite_plus_index"
    )["candidate_id"]
    clustered = str(views.plan.clustered_index.index_name)
    seen: list[str] = []

    async def profile(_database: str, sql: str, **_kwargs: Any) -> Any:
        reads_view = VIEW in sql
        seen.append("view" if reads_view else "original")
        return _profile(70 if reads_view else 8_000, index_name=clustered if reads_view else None)

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]

    result = await app._benchmark_indexed_view_candidate(
        "appdb", session_id, candidate_id, BODY, sql=VIEW_ORIGINAL, candidate_sql=_view_sql(),
        view_name=VIEW, phase="finalist", idempotency_key="view-1",
    )

    assert seen == ["original", "view"]
    case_result = result["parameter_results"][0]
    assert case_result["runs"] == {"baseline_before": 1, "candidate": 1, "baseline_after": 0}
    assert case_result["run_rule"] == "one_run_each_large_gain"
    assert result["classification"] == "improved", result["reason"]
    assert set(result["time_spent"]) == {
        "baseline_runs", "build", "candidate_runs", "comparison", "cleanup", "other", "total",
    }
    assert not views.state.exists


async def _view_session(app: AzureSqlMcpApplication, views: _ViewSandbox) -> tuple[str, str]:
    case = app.performance_workflows.start_case("appdb", VIEW_ORIGINAL)
    session_id = app.performance_workflows.start_session(case.case_id, "appdb")["session_id"]
    candidate_id = app.performance_workflows.add_candidate(
        session_id, _view_sql(), strategy="rewrite_plus_index"
    )["candidate_id"]
    clustered = str(views.plan.clustered_index.index_name)

    async def profile(_database: str, sql: str, **_kwargs: Any) -> Any:
        reads_view = VIEW in sql
        return _profile(4 if reads_view else 900, index_name=clustered if reads_view else None)

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]
    return session_id, candidate_id


@pytest.mark.asyncio
async def test_a_view_build_that_was_the_deadlock_victim_is_retried_once(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _view_app(server_config_factory)
    views = _ViewSandbox(app, monkeypatch)
    session_id, candidate_id = await _view_session(app, views)
    admin = views._admin
    failures = [
        RuntimeError("The test view build was rolled back. Engine error 1205: deadlock victim.")
    ]

    async def flaky(action: Any, executor: Any, **kwargs: Any) -> dict[str, Any]:
        if "CREATE VIEW" in action.sql and failures:
            views.events.append("build_view")
            raise failures.pop()
        return await admin(action, executor, **kwargs)

    app.admin_policy.execute = AsyncMock(side_effect=flaky)  # type: ignore[method-assign]

    result = await app._benchmark_indexed_view_candidate(
        "appdb", session_id, candidate_id, BODY, sql=VIEW_ORIGINAL, candidate_sql=_view_sql(),
        view_name=VIEW, phase="finalist", idempotency_key="view-1",
    )

    assert views.events[:2] == ["build_view", "build_view"]
    assert result["classification"] == "improved", result["reason"]
    assert result["build_retries"] == [
        {"view": f"dbo.{VIEW}", "error_number": 1205, "retried": True, "succeeded": True}
    ]
    assert result["lease"]["status"] == "cleaned"


@pytest.mark.asyncio
async def test_a_view_call_at_its_cap_is_partial_and_keeps_the_held_view(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _view_app(server_config_factory)
    views = _ViewSandbox(app, monkeypatch)
    session_id, candidate_id = await _view_session(app, views)
    clock = {"now": 1000.0}
    monkeypatch.setattr("azure_sql_mcp.server.time.monotonic", lambda: clock["now"])
    admin = views._admin

    async def slow_build(action: Any, executor: Any, **kwargs: Any) -> dict[str, Any]:
        clock["now"] += 700
        return await admin(action, executor, **kwargs)

    app.admin_policy.execute = AsyncMock(side_effect=slow_build)  # type: ignore[method-assign]

    result = await app._benchmark_indexed_view_candidate(
        "appdb", session_id, candidate_id, BODY, sql=VIEW_ORIGINAL, candidate_sql=_view_sql(),
        view_name=VIEW, phase="finalist", idempotency_key="view-1",
        keep_for_session=True, max_call_seconds=600,
    )

    assert result["partial"] is True
    assert result["classification"] == "inconclusive"
    assert result["failure_diagnostic"]["failed_step"] == "candidate_measure"
    assert result["time_spent"]["build"] == 700.0
    assert views.state.exists
    assert result["test_objects"]["still_held"][0]["name"] == f"dbo.{VIEW}"
    assert "still held" in result["reason"]
