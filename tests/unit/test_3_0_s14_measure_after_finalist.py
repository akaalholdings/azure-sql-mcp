"""A further measurement on a candidate that already has its finalist verdict (S14 item 3).

In five benchmark tasks a cross-case or shared-plan run after the finalist was
refused, and the route (register the statement again) cost the agent calls.
The same call is now accepted: it adds a measurement to the candidate, it can
lower the verdict (a regression on a shared plan must show) and it can never
raise it.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.performance_store import IdempotencyConflictError
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

CROSS = {"compile_case": "rare", "run_case": "common"}


async def _after_finalist(elapsed: Any) -> tuple[Any, ScriptedPlans, str, str, dict[str, Any]]:
    plans = ScriptedPlans(elapsed)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (candidate_id,) = _session(service, CANDIDATE)
    first = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
        phase="finalist", parameter_cases=CASES,
    )
    return service, plans, session_id, candidate_id, first


def _candidate(service: Any, session_id: str, candidate_id: str) -> Any:
    return next(
        item for item in service.sessions.list_candidates(session_id)
        if item.candidate_id == candidate_id
    )


@pytest.mark.asyncio
async def test_a_cross_case_run_after_the_finalist_is_measured_and_keeps_the_verdict() -> None:
    service, _plans, session_id, candidate_id, first = await _after_finalist(
        lambda side, _case, _call: 400 if side == "baseline" else 100
    )
    assert first["classification"] == "improved", first["reason"]

    crossed = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
        phase="finalist", parameter_cases=CASES, cross_case=CROSS,
    )

    assert crossed["measurement_only"] is True
    assert crossed["candidate_id"] == candidate_id
    assert crossed["measurement_candidate_id"] != candidate_id
    assert crossed["cross_case_result"]["compile_case"] == "rare"
    assert crossed["candidate_classification_before"] == "improved"
    assert crossed["candidate_classification"] == "improved"
    assert crossed["verdict_changed"] is False
    assert crossed["executions"] > 0
    candidate = _candidate(service, session_id, candidate_id)
    assert candidate.state == "improved"
    # The measurement is kept with the candidate; the finalist proof still selects it.
    assert crossed["evidence_id"] in candidate.evidence_ids
    final = service.finalize_session(
        session_id, selected_candidate_id=candidate_id, stopping_reason="measured"
    )
    assert final["selected_candidate_id"] == candidate_id


@pytest.mark.asyncio
async def test_a_regression_on_a_shared_plan_lowers_the_verdict_of_the_finalist() -> None:
    slow_from = {"call": 10**6}

    def elapsed(side: str, case: str, call: int) -> float:
        if side == "baseline":
            return 400
        # Fast on its own plan, slow for "common" on the plan compiled for "rare".
        return 100 if case == "rare" or call < slow_from["call"] else 4000

    service, plans, session_id, candidate_id, first = await _after_finalist(elapsed)
    assert first["classification"] == "improved", first["reason"]
    # The second call measures "common" on its own plan first, as the finalist did.
    slow_from["call"] = 2 * plans.count("candidate", "common")

    crossed = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
        phase="finalist", parameter_cases=CASES, cross_case=CROSS,
    )

    assert crossed["classification"] == "regressed"
    assert "plan compiled for parameter case rare" in crossed["reason"]
    assert crossed["candidate_classification_before"] == "improved"
    assert crossed["candidate_classification"] == "regressed"
    assert crossed["verdict_changed"] is True
    assert _candidate(service, session_id, candidate_id).state == "regressed"
    with pytest.raises(ValueError, match="improved"):
        service.finalize_session(
            session_id, selected_candidate_id=candidate_id, stopping_reason="measured"
        )


@pytest.mark.asyncio
async def test_a_better_measurement_never_raises_a_neutral_verdict() -> None:
    def elapsed(side: str, _case: str, call: int) -> float:
        # The candidate was no faster in the finalist and is 4x faster afterwards.
        return 400 if side == "baseline" or call < 8 else 100

    service, _plans, session_id, candidate_id, first = await _after_finalist(elapsed)
    assert first["classification"] == "neutral", first["reason"]

    again = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
        phase="finalist", parameter_cases=CASES, measurement_only=True,
    )

    assert again["measurement_only"] is True
    assert again["classification"] == "improved"
    assert again["candidate_classification"] == "neutral"
    assert again["verdict_changed"] is False
    assert "never raises" in again["measurement_note"]
    assert _candidate(service, session_id, candidate_id).state == "neutral"


@pytest.mark.asyncio
async def test_the_same_measurement_call_is_replayed_and_not_run_twice() -> None:
    service, plans, session_id, candidate_id, _first = await _after_finalist(
        lambda side, _case, _call: 400 if side == "baseline" else 100
    )
    arguments: dict[str, Any] = {
        "phase": "finalist", "parameter_cases": CASES, "cross_case": CROSS,
        "idempotency_key": "cross-once",
    }

    crossed = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE, **arguments
    )
    executions = len(plans.calls)
    replayed = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE, **arguments
    )

    assert len(plans.calls) == executions
    assert replayed["measurement_candidate_id"] == crossed["measurement_candidate_id"]
    assert replayed["evidence_id"] == crossed["evidence_id"]
    assert len(service.sessions.list_candidates(session_id)) == 2


@pytest.mark.asyncio
async def test_a_second_finalist_without_the_argument_is_still_refused_and_names_it() -> None:
    service, _plans, session_id, candidate_id, _first = await _after_finalist(
        lambda side, _case, _call: 400 if side == "baseline" else 100
    )

    with pytest.raises(ValueError) as refusal:
        await service.benchmark_candidate(
            session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
            phase="finalist", parameter_cases=CASES,
        )

    message = str(refusal.value)
    assert "terminal benchmark result (improved)" in message
    assert "measurement_only" in message and "cross_case" in message


@pytest.mark.asyncio
async def test_measurement_only_needs_a_candidate_that_has_its_verdict() -> None:
    plans = ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 100)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (candidate_id,) = _session(service, CANDIDATE)

    with pytest.raises(ValueError, match="measurement_only"):
        await service.benchmark_candidate(
            session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
            phase="finalist", parameter_cases=CASES, measurement_only=True,
        )
    assert plans.calls == []


# --- the index tool -----------------------------------------------------------------

INDEX_SQL = "SELECT p.Id FROM dbo.Posts AS p WHERE p.OwnerUserId = @u"
INDEX_CASES = [
    {"name": "selective", "values": {"u": 22656}, "types": {"u": "int"}, "weight": 1.0},
    {"name": "common", "values": {"u": 5}, "types": {"u": "int"}, "weight": 1.0},
]
OWNER = {"table_name": "Posts", "key_columns": ["OwnerUserId"]}


def _index_app(server_config_factory, monkeypatch: pytest.MonkeyPatch, with_index_ms: Any):
    app = index_set._app(server_config_factory)
    sandbox = index_set._Sandbox(app, monkeypatch)
    session_id, candidate_id = index_set._session(app, INDEX_SQL, parameter_cases=INDEX_CASES)
    [index_name] = index_set._names(TEST_INDEX_PREFIX, OWNER)
    shared_plan_calls: list[str] = []

    async def profile(_database: str, contract: Any, **_kwargs: Any) -> Any:
        if index_name not in sandbox.indexes:
            return _profile(100)
        shared = len(sandbox.created) > 1
        if shared:
            shared_plan_calls.append(contract.bucket_id)
        return index_set._plan(index_name, elapsed_ms=with_index_ms(contract.bucket_id, shared))

    app.plans.profile_parameterized_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]

    async def run(key: str, **options: Any) -> dict[str, Any]:
        return await index_set._benchmark(
            app, session_id, candidate_id, sql=INDEX_SQL, key=key, phase="finalist",
            parameter_cases=INDEX_CASES, indexes=[OWNER], runs=2, **options,
        )

    return app, sandbox, session_id, candidate_id, run


@pytest.mark.asyncio
async def test_a_shared_plan_run_after_an_index_finalist_is_measured_on_the_same_candidate(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, sandbox, session_id, candidate_id, run = _index_app(
        server_config_factory, monkeypatch, lambda _case, _shared: 10
    )
    first = await run("finalist")
    assert first["classification"] == "improved", first["reason"]

    shared = await run("shared-plan", plan_reuse="first_case")

    assert shared["measurement_only"] is True
    assert shared["candidate_id"] == candidate_id
    assert shared["plan_reuse"] == "first_case"
    assert shared["candidate_classification"] == "improved"
    assert shared["verdict_changed"] is False
    # The index was built again for the run and dropped again.
    assert len(sandbox.created) == 2 and sandbox.dropped == sandbox.created
    candidate = app.tuning_sessions.get_candidate(candidate_id)
    assert candidate.state == "improved"
    assert shared["evidence_id"] in candidate.evidence_ids

    replayed = await run("shared-plan", plan_reuse="first_case")
    assert len(sandbox.created) == 2
    assert replayed["evidence_id"] == shared["evidence_id"]


@pytest.mark.asyncio
async def test_a_case_that_regresses_on_the_shared_plan_lowers_the_index_verdict(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, _sandbox, _session_id, candidate_id, run = _index_app(
        server_config_factory,
        monkeypatch,
        lambda case, shared: 900 if shared and case == "common" else 10,
    )
    first = await run("finalist")
    assert first["classification"] == "improved", first["reason"]

    shared = await run("shared-plan", plan_reuse="first_case")

    assert shared["classification"] == "regressed"
    assert shared["candidate_classification_before"] == "improved"
    assert shared["candidate_classification"] == "regressed"
    assert shared["verdict_changed"] is True
    assert app.tuning_sessions.get_candidate(candidate_id).state == "regressed"


@pytest.mark.asyncio
async def test_a_second_index_finalist_without_the_argument_is_refused_and_names_it(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    _app, sandbox, _session_id, _candidate_id, run = _index_app(
        server_config_factory, monkeypatch, lambda _case, _shared: 10
    )
    await run("finalist")

    with pytest.raises(IdempotencyConflictError, match="measurement_only"):
        await run("again")

    assert len(sandbox.created) == 1
    again = await run("measure", measurement_only=True)
    assert again["measurement_only"] is True
