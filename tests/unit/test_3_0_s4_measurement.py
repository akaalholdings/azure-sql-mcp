"""Benchmark measurement: server-side metrics, warm-up, paired decision, one baseline per session."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from azure_sql_mcp.artifacts import ExplainPlanArtifact
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.param_binding import ParameterExecutionContract
from azure_sql_mcp.performance_workflows import BENCHMARK_MARKER_PREFIX
from azure_sql_mcp.performance_workflows import aggregate_samples
from azure_sql_mcp.performance_workflows import classify_benchmark
from azure_sql_mcp.performance_workflows import extract_profile_metrics
from azure_sql_mcp.plans import ProfiledPlanResult
from tests.unit.test_performance_workflows import _service
from tests.unit.test_performance_workflows import _typed_binder

NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"


def _plan_xml(*, fetch_wait_ms: float | None = None) -> str:
    waits = (
        f'<WaitStats><Wait WaitType="ASYNC_NETWORK_IO" WaitTimeMs="{fetch_wait_ms}" '
        'WaitCount="40" /><Wait WaitType="PAGEIOLATCH_SH" WaitTimeMs="15" WaitCount="3" />'
        "</WaitStats>"
        if fetch_wait_ms is not None
        else ""
    )
    return (
        f'<ShowPlanXML xmlns="{NS}"><BatchSequence><Batch><Statements><StmtSimple>'
        f"<QueryPlan>{waits}</QueryPlan></StmtSimple></Statements></Batch></BatchSequence>"
        "</ShowPlanXML>"
    )


def _profile(
    server_elapsed_ms: float | None,
    *,
    wall_ms: float | None = None,
    cpu_ms: float | None = None,
    fetch_wait_ms: float | None = None,
    logical_reads: int | None = None,
    lob_logical_reads: int = 0,
    plan_hash: str = "0xPLAN",
    parameters: list[dict[str, Any]] | None = None,
) -> ProfiledPlanResult:
    summary: dict[str, Any] = {
        "statement_count": 1,
        "operator_count": 1,
        "statements": [{"query_hash": "0xQ", "query_plan_hash": plan_hash}],
        "actual_metrics": {
            "actual_cpu_ms": cpu_ms if cpu_ms is not None else server_elapsed_ms,
            "actual_elapsed_ms": server_elapsed_ms,
            "actual_rows": 1,
            "query_metric_source": (
                "showplan_query_time_stats"
                if server_elapsed_ms is not None
                else "unavailable_for_multi_select_plan"
            ),
        },
        "warnings": [],
        "missing_indexes": [],
        "parameters": parameters or [],
    }
    if logical_reads is not None:
        summary["statistics_io"] = {
            "query_totals": {
                "logical_reads": logical_reads,
                "physical_reads": 0,
                "lob_logical_reads": lob_logical_reads,
            },
            "query_totals_source": "statistics_io_table_messages",
        }
    return ProfiledPlanResult(
        plan=ExplainPlanArtifact(
            database_name="appdb",
            analyze=True,
            summary=summary,
            raw_xml=_plan_xml(fetch_wait_ms=fetch_wait_ms),
        ),
        result_sets=[
            QueryResult(columns=("id",), rows=[{"id": 1}], column_type_signatures=("int",))
        ],
        elapsed_wall_ms=wall_ms if wall_ms is not None else float(server_elapsed_ms or 0),
        user_query_executions=1,
        truncated=False,
        metric_provenance="test",
    )


def test_elapsed_is_server_time_without_the_wait_for_the_client_to_fetch_rows() -> None:
    """A 200k-row result over a WAN: 5 s on the client, 0.2 s of work on the server."""

    metrics = extract_profile_metrics(_profile(800, wall_ms=5000, fetch_wait_ms=600))

    assert metrics["elapsed_ms"] == 200
    assert metrics["elapsed_source"] == "showplan_query_time_stats"
    assert metrics["server_elapsed_ms"] == 800
    assert metrics["client_fetch_wait_ms"] == 600
    assert metrics["client_elapsed_ms"] == 5000


def test_elapsed_falls_back_to_the_client_clock_when_the_plan_has_no_statement_time() -> None:
    metrics = extract_profile_metrics(_profile(None, wall_ms=310))

    assert metrics["elapsed_ms"] == 310
    assert metrics["elapsed_source"] == "client_wall_clock"
    assert metrics["server_elapsed_ms"] is None


def test_logical_reads_count_lob_pages() -> None:
    """A rewrite that stops reading an nvarchar(max) column saves LOB reads, not in-row reads."""

    metrics = extract_profile_metrics(_profile(10, logical_reads=120, lob_logical_reads=4000))

    assert metrics["logical_reads"] == 4120
    assert metrics["lob_logical_reads"] == 4000
    assert metrics["logical_reads_definition"] == "in_row_plus_lob"


def _result(
    baseline: list[float],
    candidate: list[float],
    *,
    metric: str = "elapsed_ms",
    paired: bool = True,
    weight: float = 1.0,
    extra: dict[str, list[float]] | None = None,
) -> dict[str, Any]:
    def samples(values: list[float], side: str) -> list[dict[str, Any]]:
        rows = []
        for index, value in enumerate(values):
            row: dict[str, Any] = {
                "elapsed_ms": 50.0,
                "cpu_ms": 50.0,
                "logical_reads": 1000,
                "elapsed_source": "showplan_query_time_stats",
                "cpu_source": "showplan_query_time_stats",
                "read_source": "statistics_io_table_messages",
            }
            row[metric] = value
            for name, series in (extra or {}).items():
                if name.startswith(side):
                    row[name.split(":", 1)[1]] = series[index]
            rows.append(row)
        return rows

    return {
        "parameter_case": "default",
        "weight": weight,
        "paired": paired,
        "baseline": aggregate_samples(samples(baseline, "baseline")),
        "candidate": aggregate_samples(samples(candidate, "candidate")),
    }


def test_one_slow_sample_does_not_turn_a_four_times_win_into_neutral() -> None:
    """Review finding 39: one 820 ms spike in five runs must not discard a 4x faster rewrite."""

    result = _result([400, 410, 395, 405, 400], [100, 102, 820, 99, 101])

    state, _reason = classify_benchmark([result], [], require_equivalence=False)

    assert state == "promising"
    assert result["comparison_margin"]["bucket_verdict"] == "improved"
    assert result["decision_basis"] == "per_bucket_median_shift_v2"


def test_drift_that_moves_both_sides_cancels_in_the_paired_ratio() -> None:
    """Another tenant slows every later run; each pair still shows the same 2x gain."""

    paired = _result([100, 200, 300], [50, 100, 150], paired=True)
    pooled = _result([100, 200, 300], [50, 100, 150], paired=False)

    assert classify_benchmark([paired], [], require_equivalence=False)[0] == "promising"
    assert paired["comparison_margin"]["paired_ratio_median"] == pytest.approx(0.5)
    # Without pairing the same numbers are inside the baseline's own spread.
    assert classify_benchmark([pooled], [], require_equivalence=False)[0] == "neutral"


def test_a_slower_candidate_is_regressed_even_with_a_cold_baseline_sample() -> None:
    result = _result([900, 400, 400], [600, 600, 600])

    state, reason = classify_benchmark([result], [], require_equivalence=False)

    assert state == "regressed"
    assert "default" in reason


def test_a_gain_inside_the_noise_stays_neutral() -> None:
    result = _result([100, 140, 95, 120, 105], [98, 100, 135, 104, 96])

    assert classify_benchmark([result], [], require_equivalence=False)[0] == "neutral"


def test_a_regression_in_one_bucket_rejects_a_candidate_that_wins_on_average() -> None:
    common = _result([400, 400, 400], [100, 100, 100], weight=9.0)
    rare = _result([100, 100, 100], [300, 300, 300], weight=1.0)
    rare["parameter_case"] = "rare"

    state, reason = classify_benchmark([common, rare], [], require_equivalence=False)

    assert state == "regressed"
    assert "rare" in reason


def test_fewer_reads_bought_with_more_cpu_is_a_regression() -> None:
    """Design step 5: a rewrite that halves reads but triples CPU must not win on reads."""

    result = _result(
        [10_000, 10_000, 10_000],
        [5_000, 5_000, 5_000],
        metric="logical_reads",
        extra={"baseline:cpu_ms": [100, 100, 100], "candidate:cpu_ms": [300, 300, 300]},
    )

    state, reason = classify_benchmark(
        [result], [], objective="logical_reads", require_equivalence=False
    )

    assert state == "regressed"
    assert "CPU" in reason


def test_physical_reads_cannot_be_an_objective_on_azure_sql_database() -> None:
    result = _result([50, 50, 50], [5, 5, 5], metric="physical_reads")

    state, reason = classify_benchmark(
        [result], [], objective="physical_reads", require_equivalence=False
    )

    assert state == "inconclusive"
    assert "cold-cache" in reason


def test_both_sides_use_the_client_clock_when_one_side_has_no_server_time() -> None:
    """A multi-statement candidate has no single statement time; do not mix two clocks."""

    def side(server: float | None, wall: float) -> dict[str, Any]:
        return aggregate_samples(
            [extract_profile_metrics(_profile(server, wall_ms=wall)) for _ in range(3)]
        )

    result = {
        "parameter_case": "default",
        "weight": 1.0,
        "paired": True,
        "baseline": side(400, 450),
        "candidate": side(None, 120),
    }

    state, _reason = classify_benchmark([result], [], require_equivalence=False)

    assert state == "promising"
    assert result["comparison_margin"]["metric"] == "client_elapsed_ms"
    assert result["comparison_margin"]["metric_source"] == "client_wall_clock"


class ScriptedPlans:
    """Server-side elapsed time per execution, chosen by side, case and call number."""

    def __init__(self, elapsed: Callable[[str, str, int], float]) -> None:
        self.elapsed = elapsed
        self.calls: list[tuple[str, str, str]] = []
        self.counts: dict[tuple[str, str], int] = {}

    async def profile_query(self, _database_name: str, sql: str) -> ProfiledPlanResult:
        return self._run(sql, "default")

    async def profile_parameterized_query(
        self,
        _database_name: str,
        contract: ParameterExecutionContract,
    ) -> ProfiledPlanResult:
        return self._run(contract.sql_text, contract.bucket_id)

    def _run(self, sql: str, case: str) -> ProfiledPlanResult:
        side = "candidate" if "candidate" in sql.casefold().split("*/", 1)[-1] else "baseline"
        call = self.counts.get((side, case), 0)
        self.counts[(side, case)] = call + 1
        self.calls.append((side, case, sql))
        return _profile(self.elapsed(side, case, call), plan_hash=f"0x{side}")

    def count(self, side: str, case: str | None = None) -> int:
        return sum(
            1 for item in self.calls if item[0] == side and (case is None or item[1] == case)
        )


BASELINE = "SELECT id FROM dbo.Items WHERE owner = @p"
CANDIDATE = "SELECT id FROM dbo.Items AS candidate WHERE owner = @p"
CASES = [
    {"name": "common", "values": {"p": 1}, "types": {"p": "int"}, "weight": 9.0},
    {"name": "rare", "values": {"p": 2}, "types": {"p": "int"}, "weight": 1.0},
]


def _session(service: Any, *candidates: str, cases: list[dict[str, Any]] | None = None):
    case = service.start_case("appdb", BASELINE, parameter_cases=cases or CASES)
    session = service.start_session(case.case_id, "appdb")
    ids = [
        service.add_candidate(session["session_id"], sql, strategy="predicate")["candidate_id"]
        for sql in candidates
    ]
    return session["session_id"], ids


@pytest.mark.asyncio
async def test_the_cold_first_execution_is_run_charged_and_left_out_of_the_samples() -> None:
    """Review finding 39: the candidate's first run compiles and reads cold pages."""

    plans = ScriptedPlans(
        lambda side, _case, call: 450 if call == 0 else (400 if side == "baseline" else 200)
    )
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (candidate_id,) = _session(service, CANDIDATE, cases=CASES[:1])

    result = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
        phase="screening", parameter_cases=CASES[:1],
    )

    assert result["classification"] == "promising"
    case_result = result["parameter_results"][0]
    assert case_result["baseline"]["sample_count"] == 3
    assert case_result["candidate"]["elapsed_ms"] == 200
    assert case_result["candidate"]["spread"]["elapsed_ms"]["max"] == 200
    # One discarded run and three measured runs per side: the budget counts all eight.
    assert result["executions"] == 8
    assert result["execution_protocol"] == {
        "version": 2,
        "warmup_runs_per_side": 1,
        "measured_runs": 3,
        "comparison_executions": 0,
        "expected_executions": 8,
    }


@pytest.mark.asyncio
async def test_baseline_is_measured_once_per_parameter_case_in_a_session() -> None:
    """Review finding 96: ten candidates must not pay for ten identical baselines."""

    second = CANDIDATE.replace("candidate", "candidate2")
    plans = ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 200)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (first_id, second_id) = _session(service, CANDIDATE, second)

    first = await service.benchmark_candidate(
        session_id, first_id, "appdb", BASELINE, CANDIDATE,
        phase="screening", parameter_cases=CASES,
    )
    baseline_runs_after_first = plans.count("baseline")
    later = await service.benchmark_candidate(
        session_id, second_id, "appdb", BASELINE, second,
        phase="screening", parameter_cases=CASES,
    )

    assert baseline_runs_after_first == 8  # (1 warm-up + 3 measured) x 2 cases
    # One drift check per case, not a full baseline.
    assert plans.count("baseline") - baseline_runs_after_first == 2
    assert later["classification"] == "promising"
    assert [item["baseline_source"] for item in first["parameter_results"]] == [
        "measured",
        "measured",
    ]
    for item in later["parameter_results"]:
        assert item["baseline_source"] == "session_pool"
        assert item["baseline_evidence_id"] == first["evidence_id"]
        assert item["drift_check"]["drifted"] is False
        assert item["baseline"]["sample_count"] == 3
    assert later["executions"] == 2 * (1 + 3 + 1)


@pytest.mark.asyncio
async def test_a_baseline_that_drifted_is_measured_again_not_reused() -> None:
    second = CANDIDATE.replace("candidate", "candidate2")
    slow = {"on": False}

    def elapsed(side: str, _case: str, _call: int) -> float:
        if side == "baseline":
            return 1200 if slow["on"] else 400
        return 600 if slow["on"] else 200

    plans = ScriptedPlans(elapsed)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (first_id, second_id) = _session(service, CANDIDATE, second, cases=CASES[:1])
    await service.benchmark_candidate(
        session_id, first_id, "appdb", BASELINE, CANDIDATE,
        phase="screening", parameter_cases=CASES[:1],
    )

    slow["on"] = True  # the database got three times slower for everyone
    later = await service.benchmark_candidate(
        session_id, second_id, "appdb", BASELINE, second,
        phase="screening", parameter_cases=CASES[:1],
    )

    item = later["parameter_results"][0]
    assert item["baseline_source"] == "remeasured_after_drift"
    assert item["drift_check"]["drifted"] is True
    assert item["baseline"]["elapsed_ms"] == 1200
    # Against the stale 400 ms baseline this candidate would be a false regression.
    assert later["classification"] == "promising"


@pytest.mark.asyncio
async def test_a_finalist_always_measures_its_own_interleaved_baseline() -> None:
    plans = ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 200)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (candidate_id,) = _session(service, CANDIDATE, cases=CASES[:1])
    await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
        phase="screening", parameter_cases=CASES[:1],
    )
    before = plans.count("baseline")

    finalist = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
        phase="finalist", parameter_cases=CASES[:1],
    )

    assert plans.count("baseline") - before == 6  # 1 warm-up + 5 measured
    assert finalist["parameter_results"][0]["baseline_source"] == "measured"
    assert finalist["parameter_results"][0]["paired"] is True
    assert finalist["executions"] == 14


@pytest.mark.asyncio
async def test_each_parameter_case_compiles_its_own_plan() -> None:
    """Without this, case 2 silently runs the plan sniffed for case 1."""

    plans = ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 200)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (candidate_id,) = _session(service, CANDIDATE)

    result = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
        phase="screening", parameter_cases=CASES,
    )

    texts = {(side, case): sql for side, case, sql in plans.calls}
    assert all(sql.startswith(BENCHMARK_MARKER_PREFIX) for sql in texts.values())
    # Four distinct statement texts: one plan cache entry per side and per case.
    assert len(set(texts.values())) == 4
    assert texts[("baseline", "common")].endswith(BASELINE)
    for item in result["parameter_results"]:
        assert item["compile_state"]["requested"] == "fresh_plan_per_parameter_case"
    assert result["plan_controls_not_applied"] is True


def test_compile_state_is_read_from_the_plan_not_assumed() -> None:
    own = extract_profile_metrics(
        _profile(10, parameters=[{"name": "@p", "compiled_value": "(1)", "runtime_value": "(1)"}])
    )
    other = extract_profile_metrics(
        _profile(10, parameters=[{"name": "@p", "compiled_value": "(1)", "runtime_value": "(2)"}])
    )

    assert own["compile_state"] == "compiled_for_these_values"
    assert other["compile_state"] == "compiled_for_other_values"
    assert extract_profile_metrics(_profile(10))["compile_state"] == "no_parameters"


@pytest.mark.asyncio
async def test_the_cross_case_runs_one_case_on_the_plan_compiled_for_another() -> None:
    """The parameter-sensitivity test: the rare value's plan, executed for the common value."""

    def elapsed(side: str, case: str, call: int) -> float:
        if side == "baseline":
            return 400
        # The candidate is fast on its own plan and slow on the plan compiled for "rare".
        return 200 if call < 4 or case == "rare" else 2000

    plans = ScriptedPlans(elapsed)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (candidate_id,) = _session(service, CANDIDATE)

    result = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
        phase="screening", parameter_cases=CASES,
        cross_case={"compile_case": "rare", "run_case": "common"},
    )

    cross = result["cross_case_result"]
    assert cross["compile_case"] == "rare"
    assert cross["run_case"] == "common"
    assert cross["candidate"]["elapsed_ms"] == 2000
    assert cross["candidate_cross_to_own_ratio"] == pytest.approx(10.0)
    assert result["classification"] == "regressed"
    assert "plan compiled for parameter case rare" in result["reason"]
    # 2 cases x 8, plus one compile run and three measured runs per side.
    assert result["executions"] == 16 + 8
    cross_sql = [sql for side, case, sql in plans.calls if case == "common"][-1]
    own_sql = [sql for side, case, sql in plans.calls if case == "common"][0]
    assert cross_sql != own_sql


@pytest.mark.asyncio
async def test_cross_case_names_must_be_two_measured_cases() -> None:
    plans = ScriptedPlans(lambda _side, _case, _call: 100)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (candidate_id,) = _session(service, CANDIDATE)

    with pytest.raises(ValueError, match="cross_case"):
        await service.benchmark_candidate(
            session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
            phase="screening", parameter_cases=CASES,
            cross_case={"compile_case": "rare", "run_case": "rare"},
        )
