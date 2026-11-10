"""Time guards (friction F08, F09; proposal P7).

A statement that takes minutes was run 15 times per candidate, and the same
slow original was measured again by each benchmark tool. Run counts now
follow the speed of the statement, and a slow baseline runs once per session.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.performance_workflows import aggregate_samples
from azure_sql_mcp.performance_workflows import classify_benchmark
from azure_sql_mcp.server import TEST_INDEX_PREFIX
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.tool_guides import MAX_GUIDE_CHARS
from azure_sql_mcp.tool_guides import get_tool_guide
from azure_sql_mcp.tuning_sessions import FAST_STATEMENT_SECONDS
from azure_sql_mcp.tuning_sessions import SLOW_STATEMENT_SECONDS
from azure_sql_mcp.tuning_sessions import adaptive_run_count
from tests.unit.test_3_0_wiring2_index_set import _Sandbox
from tests.unit.test_3_0_wiring2_index_set import _app
from tests.unit.test_3_0_wiring2_index_set import _benchmark
from tests.unit.test_3_0_wiring2_index_set import _names
from tests.unit.test_3_0_wiring2_index_set import _plan
from tests.unit.test_3_0_wiring2_index_set import _session
from tests.unit.test_index_benchmark_workflow import _profile
from tests.unit.test_performance_workflows import RoutedPlans
from tests.unit.test_performance_workflows import _service

SQL = "SELECT p.Id FROM dbo.Posts AS p WHERE p.OwnerUserId = @u"
CASES = [{"name": "scan", "values": {"u": 5}, "types": {"u": "int"}, "weight": 1.0}]
OWNER = {"table_name": "Posts", "key_columns": ["OwnerUserId"]}
OWNER_WITH_ID = {"table_name": "Posts", "key_columns": ["OwnerUserId", "Id"]}


# --- the rule ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("first_run_seconds", "requested", "expected"),
    [
        (0.05, 5, 5),  # fast: every requested run
        (0.99, 5, 5),
        (1.0, 5, 3),  # seconds: at most three
        (4.0, 5, 3),
        (4.0, 2, 2),
        (10.0, 5, 1),  # slow: once
        (197.0, 5, 1),
    ],
)
def test_run_count_follows_the_speed_of_the_statement(
    first_run_seconds: float, requested: int, expected: int
) -> None:
    assert (FAST_STATEMENT_SECONDS, SLOW_STATEMENT_SECONDS) == (1.0, 10.0)
    assert adaptive_run_count(first_run_seconds, requested) == expected


def test_run_count_never_exceeds_the_request_and_never_drops_below_one() -> None:
    for requested in range(1, 8):
        for tenths in range(0, 400, 3):
            for available in (None, 0.0, 2.0, 30.0, 600.0):
                runs = adaptive_run_count(
                    tenths / 10, requested, seconds_available=available
                )
                assert 1 <= runs <= requested


def test_run_count_fits_the_time_the_session_has_left() -> None:
    # 4 s per run and 9 s left for this arm: two runs fit, not three.
    assert adaptive_run_count(4.0, 5, seconds_available=9.0) == 2
    assert adaptive_run_count(0.2, 5, seconds_available=0.5) == 2
    assert adaptive_run_count(4.0, 5, seconds_available=None) == 3


# --- single samples are judged by the 2x rule -------------------------------------


def _single(baseline_ms: float, candidate_ms: list[float], **flags: Any) -> dict[str, Any]:
    def side(values: list[float]) -> dict[str, Any]:
        return aggregate_samples(
            [
                {"elapsed_ms": value, "elapsed_source": "showplan_query_time_stats"}
                for value in values
            ]
        )

    return {
        "parameter_case": "scan",
        "baseline": side([baseline_ms]),
        "candidate": side(candidate_ms),
        "baseline_single_sample": True,
        **flags,
    }


def test_one_slow_candidate_run_counts_only_when_it_halves_the_baseline() -> None:
    """Both sides ran once: no noise was measured, so only a 2x gain is a gain."""

    half = _single(150_000, [20_000], candidate_single_sample=True)
    close = _single(150_000, [100_000], candidate_single_sample=True)

    assert classify_benchmark([half], [], require_equivalence=False)[0] == "promising"
    assert half["comparison_margin"]["comparison"] == "single_samples"
    assert classify_benchmark([close], [], require_equivalence=False)[0] == "neutral"


def test_one_candidate_run_without_the_flag_stays_inconclusive() -> None:
    result = _single(150_000, [20_000])

    state, reason = classify_benchmark([result], [], require_equivalence=False)

    assert state == "inconclusive"
    assert "two measured samples" in reason


# --- index benchmark ---------------------------------------------------------------


def _scripted(
    app: AzureSqlMcpApplication,
    sandbox: _Sandbox,
    index_spec: dict[str, Any],
    *,
    baseline_ms: float,
    with_index_ms: float,
) -> list[bool]:
    [index_name] = _names(TEST_INDEX_PREFIX, index_spec)
    runs: list[bool] = []

    async def profile(_database: str, _contract: Any, **_kwargs: Any) -> Any:
        indexed = index_name in sandbox.indexes
        runs.append(indexed)
        return _plan(index_name, elapsed_ms=with_index_ms) if indexed else _profile(baseline_ms)

    app.plans.profile_parameterized_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]
    return runs


@pytest.mark.asyncio
async def test_a_baseline_that_takes_minutes_runs_once_without_being_told(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D08: 197 s per run, 3 runs before and 3 after. The agent had to know to declare it."""

    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL, parameter_cases=CASES)
    runs = _scripted(app, sandbox, OWNER, baseline_ms=150_000, with_index_ms=2)

    result = await _benchmark(
        app, session_id, candidate_id, sql=SQL, parameter_cases=CASES, indexes=[OWNER]
    )

    assert runs.count(False) == 1  # the baseline: once, and not again after the drop
    assert runs.count(True) == 3
    assert result["executions"] == 4
    case = result["parameter_results"][0]
    assert case["baseline_single_sample"] is True
    assert case["baseline_source"] == "single_run"
    assert case["runs"] == {"baseline_before": 1, "candidate": 3, "baseline_after": 0}
    assert "150 seconds" in case["baseline_note"]
    assert result["classification"] == "improved", result["reason"]
    assert result["equivalence"][0]["status"] == "match"


@pytest.mark.asyncio
async def test_the_next_index_candidate_reuses_the_slow_baseline_it_found(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL, parameter_cases=CASES)
    _scripted(app, sandbox, OWNER, baseline_ms=150_000, with_index_ms=2)
    first = await _benchmark(
        app, session_id, candidate_id, sql=SQL, parameter_cases=CASES, indexes=[OWNER]
    )
    second = app.performance_workflows.add_candidate(
        session_id, SQL, strategy="index", idempotency_key="second"
    )
    runs = _scripted(app, sandbox, OWNER_WITH_ID, baseline_ms=150_000, with_index_ms=2)

    result = await _benchmark(
        app, session_id, second["candidate_id"], sql=SQL, key="second-key",
        parameter_cases=CASES, indexes=[OWNER_WITH_ID],
    )

    assert False not in runs  # the original statement did not run again
    case = result["parameter_results"][0]
    assert case["baseline_source"] == "session_single_run"
    assert case["baseline_evidence_id"] == first["evidence_id"]
    assert result["classification"] == "improved", result["reason"]


@pytest.mark.asyncio
async def test_a_statement_that_takes_seconds_gets_three_runs_per_arm_not_five(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D05: 27 executions of a statement that takes seconds: 8.5 minutes."""

    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL, parameter_cases=CASES)
    runs = _scripted(app, sandbox, OWNER, baseline_ms=4_000, with_index_ms=20)

    result = await _benchmark(
        app, session_id, candidate_id, sql=SQL, phase="finalist",
        parameter_cases=CASES, indexes=[OWNER],
    )

    # Finalist default is 5 runs: the 4 s baseline gets 3, the 20 ms candidate all 5.
    assert result["parameter_results"][0]["runs"] == {
        "baseline_before": 3, "candidate": 5, "baseline_after": 3,
    }
    assert (runs.count(False), runs.count(True)) == (6, 5)
    assert result["executions"] == 11
    assert result["classification"] == "improved", result["reason"]


@pytest.mark.asyncio
async def test_a_candidate_that_is_still_slow_is_measured_once_and_flagged(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL, parameter_cases=CASES)
    runs = _scripted(app, sandbox, OWNER, baseline_ms=150_000, with_index_ms=20_000)

    result = await _benchmark(
        app, session_id, candidate_id, sql=SQL, parameter_cases=CASES, indexes=[OWNER]
    )

    assert (runs.count(False), runs.count(True)) == (1, 1)
    case = result["parameter_results"][0]
    assert case["candidate_single_sample"] is True
    assert case["comparison_margin"]["comparison"] == "single_samples"
    # 150 s to 20 s is more than a 2x gain.
    assert result["classification"] == "improved", result["reason"]


# --- one pool for both benchmark tools ---------------------------------------------


REWRITE = "SELECT candidate.Id FROM dbo.Posts AS candidate WHERE candidate.OwnerUserId = @u"


class _SlowOriginal(RoutedPlans):
    """The original takes 150 s; the rewrite is fast."""

    def __init__(self) -> None:
        super().__init__(baseline_ms=150_000, candidate_ms=40)


@pytest.mark.asyncio
async def test_the_rewrite_benchmark_finds_a_slow_baseline_and_runs_it_once() -> None:
    from tests.unit.test_performance_workflows import _typed_binder

    plans = _SlowOriginal()
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    case = service.start_case("appdb", SQL.replace("@u", "@p"), parameter_cases=[
        {"name": "scan", "values": {"p": 5}, "types": {"p": "int"}}
    ])
    session_id = service.start_session(case.case_id, "appdb")["session_id"]
    candidate = service.add_candidate(
        session_id, REWRITE.replace("@u", "@p"), strategy="predicate"
    )

    result = await service.benchmark_candidate(
        session_id,
        candidate["candidate_id"],
        "appdb",
        SQL.replace("@u", "@p"),
        REWRITE.replace("@u", "@p"),
        phase="screening",
    )

    baseline_runs = [sql for sql in plans.calls if "candidate" not in sql.casefold()]
    assert len(baseline_runs) == 1
    case_result = result["parameter_results"][0]
    assert case_result["baseline_single_sample"] is True
    assert case_result["baseline_source"] == "single_run"
    assert "150 seconds" in case_result["baseline_note"]
    assert result["classification"] == "promising", result["reason"]


@pytest.mark.asyncio
async def test_the_index_benchmark_reuses_the_slow_baseline_of_the_rewrite_benchmark(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D08: the same slow original was measured by each tool (197 s, then 117 s)."""

    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, index_candidate_id = _session(app, SQL, parameter_cases=CASES)
    rewrite = app.performance_workflows.add_candidate(session_id, REWRITE, strategy="predicate")

    async def original_or_rewrite(_database: str, contract: Any, **_kwargs: Any) -> Any:
        return _profile(40 if "candidate" in contract.sql_text else 150_000)

    app.plans.profile_parameterized_query = AsyncMock(side_effect=original_or_rewrite)  # type: ignore[method-assign]
    first = await app.performance_workflows.benchmark_candidate(
        session_id, rewrite["candidate_id"], "appdb", SQL, REWRITE,
        phase="screening", parameter_cases=CASES,
    )
    assert first["parameter_results"][0]["baseline_source"] == "single_run"
    runs = _scripted(app, sandbox, OWNER, baseline_ms=150_000, with_index_ms=2)

    result = await _benchmark(
        app, session_id, index_candidate_id, sql=SQL, parameter_cases=CASES, indexes=[OWNER]
    )

    assert False not in runs
    case = result["parameter_results"][0]
    assert case["baseline_source"] == "session_single_run"
    assert case["baseline_evidence_id"] == first["evidence_id"]
    assert case["baseline"]["elapsed_ms"] == 150_000
    # The kept result digest of that one run still proves the index changed no row.
    assert result["equivalence"][0]["status"] == "match"
    assert result["classification"] == "improved", result["reason"]


@pytest.mark.asyncio
async def test_the_rewrite_benchmark_reuses_the_slow_baseline_of_the_index_benchmark(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, index_candidate_id = _session(app, SQL, parameter_cases=CASES)
    _scripted(app, sandbox, OWNER, baseline_ms=150_000, with_index_ms=2)
    first = await _benchmark(
        app, session_id, index_candidate_id, sql=SQL, parameter_cases=CASES, indexes=[OWNER]
    )
    rewrite = app.performance_workflows.add_candidate(session_id, REWRITE, strategy="predicate")
    seen: list[str] = []

    async def original_or_rewrite(_database: str, contract: Any, **_kwargs: Any) -> Any:
        seen.append("rewrite" if "candidate" in contract.sql_text else "original")
        return _profile(40 if "candidate" in contract.sql_text else 150_000)

    app.plans.profile_parameterized_query = AsyncMock(side_effect=original_or_rewrite)  # type: ignore[method-assign]

    result = await app.performance_workflows.benchmark_candidate(
        session_id, rewrite["candidate_id"], "appdb", SQL, REWRITE,
        phase="screening", parameter_cases=CASES,
    )

    assert "original" not in seen
    case = result["parameter_results"][0]
    assert case["baseline_source"] == "session_single_run"
    assert case["baseline_evidence_id"] == first["evidence_id"]
    assert result["classification"] == "promising", result["reason"]


# --- the guide -----------------------------------------------------------------------


def test_the_guide_states_the_time_rules() -> None:
    guide = get_tool_guide("measurement_time_rules")

    assert guide["found"] is True
    text = " ".join(guide["guide"].split())
    assert len(guide["guide"]) <= MAX_GUIDE_CHARS
    for phrase in (
        "under 1 second",
        "1 to 10 seconds",
        "10 seconds or more",
        "at most 3",
        "half",
        "once in the session",
        "both benchmark tools",
        "max_seconds",
    ):
        assert phrase in text, phrase
