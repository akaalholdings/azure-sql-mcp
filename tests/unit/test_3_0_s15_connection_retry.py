"""A lost connection inside a benchmark, and the engine error of a failed statement (S15 items 2, 4).

On a shared bench the result comparison of one case failed on a dropped
connection: the whole candidate became inconclusive and the result named no
cause. Elsewhere a measured statement failed and the result gave no error
number and no message, so an arithmetic overflow could not be told from a
deadlock. A lost connection now gets one more try on a new connection, the
result says so, and a failure carries the engine's number and redacted text.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.index_optimizer import is_connection_failure
from azure_sql_mcp.server import TEST_INDEX_PREFIX
from tests.unit import test_3_0_wiring2_index_set as index_set
from tests.unit.test_3_0_s4_measurement import BASELINE
from tests.unit.test_3_0_s4_measurement import CANDIDATE
from tests.unit.test_3_0_s4_measurement import CASES
from tests.unit.test_3_0_s4_measurement import ScriptedPlans
from tests.unit.test_3_0_s4_measurement import _session
from tests.unit.test_index_benchmark_workflow import _profile
from tests.unit.test_performance_workflows import _service
from tests.unit.test_performance_workflows import _typed_binder


def _link_failure() -> Exception:
    return RuntimeError(
        "[08S01] [Microsoft][ODBC Driver 18 for SQL Server]TCP Provider: Error code 0x274C "
        "(10060) (SQLExecDirectW); [08S01] [Microsoft][ODBC Driver 18 for SQL Server]"
        "Communication link failure (10060)"
    )


def _conversion_failure() -> Exception:
    return RuntimeError(
        "[22018] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Conversion failed "
        "when converting the varchar value 'ACME-7731-secret' to data type int. (245) "
        "(SQLExecDirectW)"
    )


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (_link_failure(), True),
        (RuntimeError("[08001] [Microsoft][ODBC Driver 18 for SQL Server]Login timeout expired"), True),
        (ConnectionResetError("connection reset by peer"), True),
        # The statement ran for the whole limit: a retry runs it as long again.
        (RuntimeError("[HYT00] [Microsoft][ODBC Driver 18 for SQL Server]Query timeout expired"), False),
        (TimeoutError("cancelled at the cap"), False),
        (_conversion_failure(), False),
        (
            RuntimeError(
                "[40001] [Microsoft][SQL Server]Transaction (Process ID 71) was deadlocked on "
                "lock resources with another process and has been chosen as the deadlock "
                "victim. (1205)"
            ),
            False,
        ),
    ],
)
def test_only_a_failure_of_the_connection_is_a_connection_failure(
    error: Exception, expected: bool
) -> None:
    assert is_connection_failure(error) is expected


def _rewrite_service(elapsed: Any):
    plans = ScriptedPlans(elapsed)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (candidate_id,) = _session(service, CANDIDATE)

    async def run(key: str = "first") -> dict[str, Any]:
        return await service.benchmark_candidate(
            session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
            phase="screening", parameter_cases=CASES, idempotency_key=key,
        )

    return service, plans, run


@pytest.mark.asyncio
async def test_a_run_that_lost_its_connection_once_is_retried_and_the_result_says_so() -> None:
    failed: list[str] = []

    def elapsed(side: str, case: str, call: int) -> float:
        if side == "candidate" and case == "common" and call == 1 and not failed:
            failed.append(case)
            raise _link_failure()
        return 400 if side == "baseline" else 100

    _service_, plans, run = _rewrite_service(elapsed)

    result = await run()

    assert result["classification"] == "promising", result["reason"]
    assert result["connection_retries"] == [
        {
            "step": "candidate_measure",
            "parameter_case": "common",
            "cause_class": "connection_lost",
            "retried": True,
            "succeeded": True,
        }
    ]
    # The failed attempt reached the server or not: it is charged either way.
    assert result["executions"] == len(plans.calls)


@pytest.mark.asyncio
async def test_a_connection_that_stays_lost_fails_the_run_with_its_cause() -> None:
    def elapsed(side: str, _case: str, _call: int) -> float:
        if side == "candidate":
            raise _link_failure()
        return 400

    _service_, plans, run = _rewrite_service(elapsed)

    result = await run()

    assert result["classification"] == "inconclusive"
    assert result["rerun_allowed"] is True
    diagnostic = result["failure_diagnostic"]
    assert diagnostic["failed_step"] == "candidate_measure"
    assert diagnostic["cause_class"] == "connection_lost"
    assert "Communication link failure" in diagnostic["engine_message"]
    assert result["connection_retries"][0]["succeeded"] is False
    # One try and one retry of the candidate statement, no more.
    assert plans.count("candidate") == 2


@pytest.mark.asyncio
async def test_a_statement_error_is_not_retried_and_returns_the_engine_number_and_text() -> None:
    def elapsed(side: str, _case: str, _call: int) -> float:
        if side == "candidate":
            raise _conversion_failure()
        return 400

    _service_, plans, run = _rewrite_service(elapsed)

    result = await run()

    diagnostic = result["failure_diagnostic"]
    assert diagnostic["failed_step"] == "candidate_measure"
    assert diagnostic["cause_class"] == "engine_error"
    assert diagnostic["native_error_code"] == 245
    assert "Conversion failed when converting the varchar value" in diagnostic["engine_message"]
    # The value the engine quoted is a row value: it never leaves the server.
    assert "ACME-7731-secret" not in str(result)
    assert "connection_retries" not in result
    assert plans.count("candidate") == 1


# --- the index tool -----------------------------------------------------------------


def _index_run(server_config_factory, monkeypatch: pytest.MonkeyPatch, fail: Any):
    app = index_set._app(server_config_factory)
    sandbox = index_set._Sandbox(app, monkeypatch)
    session_id, candidate_id = index_set._session(app)
    [badges] = index_set._names(TEST_INDEX_PREFIX, index_set.BADGES)
    calls: list[str] = []

    async def profile(_database: str, _sql: str, **_kwargs: Any) -> Any:
        arm = "candidate" if badges in sandbox.indexes else "baseline"
        calls.append(arm)
        error = fail(arm, calls.count(arm))
        if error is not None:
            raise error
        return index_set._plan(badges, elapsed_ms=20) if arm == "candidate" else _profile(400)

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]

    async def run(key: str = "first") -> dict[str, Any]:
        return await index_set._benchmark(
            app, session_id, candidate_id, key=key, indexes=[index_set.BADGES]
        )

    return app, sandbox, calls, run


@pytest.mark.asyncio
async def test_an_index_baseline_that_lost_its_connection_once_is_retried(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    _app, sandbox, calls, run = _index_run(
        server_config_factory,
        monkeypatch,
        lambda arm, number: _link_failure() if arm == "baseline" and number == 2 else None,
    )

    result = await run()

    assert result["classification"] == "improved", result["reason"]
    assert result["connection_retries"] == [
        {
            "step": "baseline_before",
            "parameter_case": "default",
            "cause_class": "connection_lost",
            "retried": True,
            "succeeded": True,
        }
    ]
    assert result["executions"] == len(calls)
    assert sandbox.indexes == {}


@pytest.mark.asyncio
async def test_a_failed_index_candidate_statement_returns_the_engine_number_and_text(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    _app, sandbox, calls, run = _index_run(
        server_config_factory,
        monkeypatch,
        lambda arm, _number: _conversion_failure() if arm == "candidate" else None,
    )

    result = await run()

    assert result["classification"] == "inconclusive"
    diagnostic = result["failure_diagnostic"]
    assert diagnostic["failed_step"] == "candidate_measure"
    assert diagnostic["cause_class"] == "engine_error"
    assert diagnostic["native_error_code"] == 245
    assert "Conversion failed" in diagnostic["engine_message"]
    assert "ACME-7731-secret" not in str(result)
    assert calls.count("candidate") == 1
    assert sandbox.indexes == {}


@pytest.mark.asyncio
async def test_an_index_run_whose_connection_stays_lost_names_connection_lost(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    _app, _sandbox, calls, run = _index_run(
        server_config_factory,
        monkeypatch,
        lambda arm, _number: _link_failure() if arm == "baseline" else None,
    )

    result = await run()

    assert result["classification"] == "inconclusive"
    assert result["rerun_allowed"] is True
    assert result["failure_diagnostic"]["failed_step"] == "baseline_before"
    assert result["failure_diagnostic"]["cause_class"] == "connection_lost"
    assert "Communication link failure" in result["failure_diagnostic"]["engine_message"]
    assert calls == ["baseline", "baseline"]


# --- the comparison -----------------------------------------------------------------


def _comparison_service(outcomes: list[Any]):
    from azure_sql_mcp.connection import QueryResult

    service, _store, _plans, executor = _service(
        plans=ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 100),
        binder=_typed_binder(),
    )
    attempts: list[int] = []

    async def snapshot(_database: str, _statements: list[str], **kwargs: Any) -> Any:
        attempts.append(len(attempts))
        hook = kwargs.get("before_statement_dispatch")
        outcome = outcomes[min(len(attempts), len(outcomes)) - 1]
        for index in range(4):
            if hook is not None:
                hook(index)
            if index == 2 and isinstance(outcome, Exception):
                raise outcome
        rows = [QueryResult(columns=("id",), rows=[{"id": 1}], column_type_signatures=("int",))]
        return [[], [], rows, rows, [], []]

    executor.execute_session_exactly_once = AsyncMock(side_effect=snapshot)
    return service, attempts


class _Budget:
    """Grants every retry and counts what it charged."""

    def __init__(self) -> None:
        self.executions = 0
        self.retries: list[dict[str, Any]] = []

    def reserve(self, attempts: int) -> bool:
        self.executions += attempts
        return True

    def take(self) -> int:
        return 0


async def _compare(service: Any, budget: Any) -> dict[str, Any]:
    bucket = await service._bind_bucket("appdb", BASELINE, CASES[0])
    baseline, candidate = service._contracts_from_bucket(BASELINE, CANDIDATE, bucket)
    return await service._compare_execution_contracts(
        "appdb", baseline, candidate, compare_order=False, retry_budget=budget
    )


@pytest.mark.asyncio
async def test_a_comparison_that_lost_its_connection_once_is_run_again() -> None:
    service, attempts = _comparison_service([_link_failure(), None])
    budget = _Budget()

    comparison = await _compare(service, budget)

    assert comparison["status"] == "match"
    assert comparison["connection_retry"] == {
        "step": "compare_results",
        "cause_class": "connection_lost",
        "retried": True,
        "succeeded": True,
    }
    assert len(attempts) == 2
    # The lost attempt dispatched its baseline statement: that is the count of the
    # call. The two statements of the second attempt are charged to the retry budget.
    assert comparison["executions"] == 1
    assert budget.executions == 2


@pytest.mark.asyncio
async def test_a_comparison_whose_connection_stays_lost_says_so() -> None:
    service, attempts = _comparison_service([_link_failure()])

    comparison = await _compare(service, _Budget())

    assert comparison["status"] == "inconclusive"
    assert comparison["cause_class"] == "connection_lost"
    assert "Communication link failure" in comparison["engine_message"]
    assert comparison["connection_retry"]["succeeded"] is False
    assert len(attempts) == 2


@pytest.mark.asyncio
async def test_a_comparison_that_failed_in_the_engine_is_not_run_again() -> None:
    service, attempts = _comparison_service([_conversion_failure()])
    budget = _Budget()

    comparison = await _compare(service, budget)

    assert comparison["status"] == "inconclusive"
    assert comparison["cause_class"] == "engine_error"
    assert comparison["native_error_code"] == 245
    assert "ACME-7731-secret" not in str(comparison)
    assert "connection_retry" not in comparison
    assert len(attempts) == 1
    assert budget.executions == 0


@pytest.mark.asyncio
async def test_a_retry_is_refused_when_the_session_budget_has_no_execution_left() -> None:
    from azure_sql_mcp.performance_workflows import ConnectionRetryBudget

    service, store, _plans, _executor = _service(
        plans=ScriptedPlans(lambda _side, _case, _call: 400), binder=_typed_binder()
    )
    session_id, (candidate_id,) = _session(service, CANDIDATE)
    limit = service.sessions.get_session(session_id).execution_limit
    reservation = store.reserve_execution_attempts(
        session_id, candidate_id, limit - 1, "request-one", owner_reference="owner-one"
    )
    budget = ConnectionRetryBudget(store, reservation["reservation_id"], "owner-one")

    # One execution is left: the first retry takes it, the second is refused.
    assert budget.reserve(1) is True
    assert budget.reserve(1) is False
    assert budget.take() == 1
    assert store.execution_budget_usage(session_id)["remaining"] == 0
    # The call finalizes with the version it holds, and may report the retried run.
    store.complete_execution_attempts(
        reservation["reservation_id"],
        dispatched_attempt_count=limit,
        owner_reference="owner-one",
        expected_version=reservation["version"],
    )
