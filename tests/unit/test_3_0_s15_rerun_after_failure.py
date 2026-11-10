"""A candidate survives a refusal or a failure that is not its fault (S15 item 1).

On a shared bench an index benchmark was refused by the engine because another
session held the table's one columnstore slot. After that the candidate could
not be measured: a new idempotency key was refused, and so was registering the
same statement again. The agent wrote a textually different rewrite to go on.
A run that gave no verdict must leave its candidate open for a new run; a
measured verdict stays final.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.index_optimizer import ExistingIndex
from azure_sql_mcp.index_optimizer import parse_candidate_key
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

ENGINE_REFUSAL = RuntimeError(
    "[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Multiple columnstore "
    "indexes are not supported. (35339) (SQLExecDirectW)"
)
CONNECTION_LOST = RuntimeError(
    "[08S01] [Microsoft][ODBC Driver 18 for SQL Server]TCP Provider: Error code 0x274C "
    "(10060) (SQLExecDirectW); [08S01] [Microsoft][ODBC Driver 18 for SQL Server]"
    "Communication link failure (10060)"
)


def _index_run(server_config_factory, monkeypatch: pytest.MonkeyPatch):
    app = index_set._app(server_config_factory)
    sandbox = index_set._Sandbox(app, monkeypatch)
    session_id, candidate_id = index_set._session(app)
    [badges] = index_set._names(TEST_INDEX_PREFIX, index_set.BADGES)

    async def profile(_database: str, _sql: str, **_kwargs: Any) -> Any:
        if badges in sandbox.indexes:
            return index_set._plan(badges, elapsed_ms=20)
        return _profile(400)

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]

    async def run(key: str, **options: Any) -> dict[str, Any]:
        return await index_set._benchmark(
            app, session_id, candidate_id, key=key, indexes=[index_set.BADGES], **options
        )

    return app, sandbox, session_id, candidate_id, run


@pytest.mark.asyncio
async def test_a_candidate_whose_build_the_engine_refused_is_measured_under_a_new_key(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, sandbox, _session_id, candidate_id, run = _index_run(server_config_factory, monkeypatch)
    sandbox.fail[0] = ENGINE_REFUSAL
    sandbox.commit_before_failure = False

    refused = await run("first")

    assert refused["classification"] == "inconclusive"
    assert refused["rerun_allowed"] is True
    assert "new idempotency_key" in refused["reason"]
    assert "reject this candidate" not in refused["reason"]
    baseline_runs = refused["executions"]
    assert baseline_runs > 0

    # The other session's index is gone: the same candidate is measured now.
    sandbox.fail.clear()
    measured = await run("second")

    assert measured["candidate_id"] == candidate_id
    assert measured["classification"] == "improved", measured["reason"]
    assert "rerun_allowed" not in measured
    candidate = app.tuning_sessions.get_candidate(candidate_id)
    assert candidate.state == "screening"
    # The budget counts what the refused call ran as well.
    assert candidate.executions == baseline_runs + measured["executions"]
    assert refused["evidence_id"] in candidate.evidence_ids
    assert measured["evidence_id"] in candidate.evidence_ids
    # Each key replays its own result; nothing is built again.
    builds = len(sandbox.create_calls)
    assert (await run("second"))["evidence_id"] == measured["evidence_id"]
    assert (await run("first"))["evidence_id"] == refused["evidence_id"]
    assert len(sandbox.create_calls) == builds


@pytest.mark.asyncio
async def test_the_failed_call_itself_is_still_replayed_and_builds_nothing(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    _app, sandbox, _session_id, _candidate_id, run = _index_run(server_config_factory, monkeypatch)
    sandbox.fail[0] = ENGINE_REFUSAL
    sandbox.commit_before_failure = False
    refused = await run("first")
    sandbox.fail.clear()

    replayed = await run("first")

    # The same key is the same request: its durable result, no second build.
    assert replayed["evidence_id"] == refused["evidence_id"]
    assert replayed["classification"] == "inconclusive"
    assert len(sandbox.create_calls) == 1


@pytest.mark.asyncio
async def test_a_refusal_for_another_sessions_leftover_does_not_bind_the_candidate(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    _app, sandbox, _session_id, _candidate_id, run = _index_run(server_config_factory, monkeypatch)
    sandbox.indexes["IX_Testing_0123456789abcdef"] = ExistingIndex(
        schema="dbo",
        table="Badges",
        index_id=9,
        name="IX_Testing_0123456789abcdef",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("Date"),),
    )

    refused = await run("first")
    assert refused["failure_code"] == "leftover_test_index"

    # The other index is gone. A new key and a changed request (runs) are accepted.
    del sandbox.indexes["IX_Testing_0123456789abcdef"]
    measured = await run("second", runs=2)

    assert measured["classification"] == "improved", measured["reason"]


@pytest.mark.asyncio
async def test_a_measured_verdict_is_final_and_a_new_key_is_refused(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, sandbox, _session_id, candidate_id, run = _index_run(server_config_factory, monkeypatch)
    first = await run("first", phase="finalist")
    assert first["classification"] == "improved", first["reason"]
    assert "rerun_allowed" not in first

    with pytest.raises(IdempotencyConflictError):
        await run("second", phase="finalist")

    assert len(sandbox.create_calls) == 1
    assert app.tuning_sessions.get_candidate(candidate_id).state == "improved"


@pytest.mark.asyncio
async def test_an_index_the_plans_did_not_use_is_a_verdict_and_takes_no_rerun(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, sandbox, _session_id, candidate_id, run = _index_run(server_config_factory, monkeypatch)
    # Built, measured, and no plan read it: that is a result of the design.
    app.plans.profile_query = AsyncMock(return_value=_profile(400))  # type: ignore[method-assign]

    unused = await run("first")

    assert unused["classification"] == "inconclusive"
    assert "rerun_allowed" not in unused
    with pytest.raises(IdempotencyConflictError):
        await run("second")
    assert len(sandbox.create_calls) == 1
    assert app.tuning_sessions.get_candidate(candidate_id).state == "inconclusive"


@pytest.mark.asyncio
async def test_a_rewrite_whose_run_lost_its_connection_is_measured_under_a_new_key() -> None:
    failing = {"on": True}

    def elapsed(side: str, _case: str, _call: int) -> float:
        if failing["on"] and side == "candidate":
            raise CONNECTION_LOST
        return 400 if side == "baseline" else 100

    plans = ScriptedPlans(elapsed)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (candidate_id,) = _session(service, CANDIDATE)

    async def run(key: str) -> dict[str, Any]:
        return await service.benchmark_candidate(
            session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
            phase="finalist", parameter_cases=CASES, idempotency_key=key,
        )

    failed = await run("first")
    assert failed["classification"] == "inconclusive"
    assert failed["rerun_allowed"] is True
    assert "new idempotency_key" in failed["reason"]
    ran = failed["executions"]
    assert ran > 0

    failing["on"] = False
    measured = await run("second")

    assert measured["classification"] == "improved", measured["reason"]
    candidate = service.sessions.get_candidate(candidate_id)
    assert candidate.state == "improved"
    assert candidate.executions == ran + measured["executions"]
    # The verdict is final now.
    with pytest.raises(ValueError, match="terminal benchmark result"):
        await run("third")


@pytest.mark.asyncio
async def test_a_rewrite_with_an_inconclusive_measurement_takes_no_rerun() -> None:
    # Measured in full, and the samples do not agree on a verdict: a result, not a failure.
    plans = ScriptedPlans(
        lambda side, _case, call: (400 if side == "baseline" else 395) + (call % 2) * 5
    )
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (candidate_id,) = _session(service, CANDIDATE)
    first = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
        phase="finalist", parameter_cases=CASES, idempotency_key="first",
    )
    assert "rerun_allowed" not in first

    with pytest.raises(ValueError, match="terminal benchmark result"):
        await service.benchmark_candidate(
            session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
            phase="finalist", parameter_cases=CASES, idempotency_key="second",
        )


@pytest.mark.asyncio
async def test_a_view_candidate_whose_build_failed_is_measured_under_a_new_key(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.unit import test_3_0_s13b2_indexed_view_tool as view_tool

    app = view_tool._app(server_config_factory)
    sandbox = view_tool._ViewSandbox(app, monkeypatch)
    sandbox.build_error = RuntimeError(
        "[40001] [Microsoft][SQL Server]Transaction (Process ID 71) was deadlocked on lock "
        "resources with another process and has been chosen as the deadlock victim. (1205)"
    )
    session_id, candidate_id = view_tool._session(app, view_tool._candidate_sql())
    view_tool._profiles(app, sandbox)

    failed = await view_tool._run(app, session_id, candidate_id, idempotency_key="first")
    assert failed["classification"] == "inconclusive"
    assert failed["rerun_allowed"] is True
    builds = sandbox.events.count("build_view")

    sandbox.build_error = None
    measured = await view_tool._run(app, session_id, candidate_id, idempotency_key="second")

    assert measured["candidate_id"] == candidate_id
    assert measured["classification"] == "improved", measured["reason"]
    assert sandbox.events.count("build_view") == builds + 1
    # Each key replays its own result and builds nothing.
    again = await view_tool._run(app, session_id, candidate_id, idempotency_key="second")
    assert again["evidence_id"] == measured["evidence_id"]
    first_again = await view_tool._run(app, session_id, candidate_id, idempotency_key="first")
    assert first_again["evidence_id"] == failed["evidence_id"]
    assert sandbox.events.count("build_view") == builds + 1
    # The verdict is final: a third key is refused.
    with pytest.raises(IdempotencyConflictError):
        await view_tool._run(app, session_id, candidate_id, idempotency_key="third")
