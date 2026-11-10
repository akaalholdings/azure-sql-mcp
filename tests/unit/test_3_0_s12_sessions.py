"""A session stays usable after a finalist, and a slow baseline is measured once.

From the first live comparison: one candidate sent straight to finalist locked
the session (task T3), and a 150-second baseline could not be rerun for every
measurement (task T5). Both times the delivered statement was measured outside
the session, so the product's measurement was not used.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.performance_workflows import classify_benchmark
from azure_sql_mcp.performance_workflows import compact_benchmark_result
from azure_sql_mcp.performance_workflows import compact_evidence_result
from tests.unit.test_3_0_s4_measurement import BASELINE
from tests.unit.test_3_0_s4_measurement import CANDIDATE
from tests.unit.test_3_0_s4_measurement import CASES
from tests.unit.test_3_0_s4_measurement import ScriptedPlans
from tests.unit.test_3_0_s4_measurement import _result
from tests.unit.test_3_0_s4_measurement import _session
from tests.unit.test_performance_workflows import _service
from tests.unit.test_performance_workflows import _typed_binder

SECOND = "SELECT id FROM dbo.Items AS candidate WHERE owner = @p AND 1 = 1"
THIRD = "SELECT id FROM dbo.Items AS candidate WHERE owner = @p AND 2 = 2"


@pytest.mark.asyncio
async def test_a_finalist_run_does_not_stop_the_next_screening_candidate() -> None:
    plans = ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 200)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (first,) = _session(service, CANDIDATE)

    # The skill's fast path: the first candidate goes straight to finalist.
    finalist = await service.benchmark_candidate(
        session_id, first, "appdb", BASELINE, CANDIDATE, phase="finalist", parameter_cases=CASES
    )
    assert finalist["classification"] == "improved"
    assert service.get_session(session_id)["session"]["status"] == "finalist_validation"

    second = service.add_candidate(session_id, SECOND, strategy="join")["candidate_id"]
    screened = await service.benchmark_candidate(
        session_id, second, "appdb", BASELINE, SECOND, phase="screening", parameter_cases=CASES
    )

    assert screened["classification"] == "promising"
    assert screened["durable_state"] == "screening"
    # The second candidate can then be validated and selected in the same session.
    validated = await service.benchmark_candidate(
        session_id, second, "appdb", BASELINE, SECOND, phase="finalist", parameter_cases=CASES
    )
    assert validated["classification"] == "improved"
    budget = service.get_session(session_id)["budget"]
    assert budget["accepts_candidate_creation"] is True


@pytest.mark.asyncio
async def test_a_finalized_or_spent_session_still_refuses_new_candidates() -> None:
    plans = ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 200)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (first,) = _session(service, CANDIDATE)
    await service.benchmark_candidate(
        session_id, first, "appdb", BASELINE, CANDIDATE, phase="finalist", parameter_cases=CASES
    )
    service.finalize_session(session_id, selected_candidate_id=first, stopping_reason="selected")

    with pytest.raises(Exception, match="completed"):
        service.add_candidate(session_id, SECOND, strategy="join")


def _slow_case_plans() -> ScriptedPlans:
    # "rare" is the 150-second full scan of task T5; the rewrite seeks in 40 ms.
    def elapsed(side: str, case: str, _call: int) -> float:
        if side == "baseline":
            return 150_000 if case == "rare" else 400
        return 40 if case == "rare" else 200

    return ScriptedPlans(elapsed)


@pytest.mark.asyncio
async def test_a_baseline_too_slow_to_repeat_runs_once_and_is_labelled_one_sample() -> None:
    plans = _slow_case_plans()
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (first, second) = _session(service, CANDIDATE, SECOND)

    result = await service.benchmark_candidate(
        session_id,
        first,
        "appdb",
        BASELINE,
        CANDIDATE,
        phase="screening",
        parameter_cases=CASES,
        slow_baseline_cases={"rare": 150},
    )

    assert plans.count("baseline", "rare") == 1  # not 1 warm-up + 3 runs: 10 minutes saved
    assert plans.count("candidate", "rare") == 4  # one warm-up and three measured runs
    assert plans.count("baseline", "common") == 4  # the fast case keeps the paired protocol
    rare = next(item for item in result["parameter_results"] if item["parameter_case"] == "rare")
    assert rare["baseline_source"] == "single_run"
    assert rare["baseline_single_sample"] is True
    assert rare["paired"] is False
    assert rare["baseline_note"] == (
        "baseline too slow to repeat: 150 seconds. It ran once; the candidate was measured "
        "alone and compared with that one sample."
    )
    assert rare["comparison_margin"]["comparison"] == "single_baseline_sample"
    assert rare["comparison_margin"]["bucket_verdict"] == "improved"
    assert rare["plan_delta"]["baseline"] is not None
    assert result["classification"] == "promising"

    # A later candidate reuses the one baseline run with its plan: no execution.
    later = await service.benchmark_candidate(
        session_id,
        second,
        "appdb",
        BASELINE,
        SECOND,
        phase="screening",
        parameter_cases=CASES,
        slow_baseline_cases={"rare": 150},
    )

    assert plans.count("baseline", "rare") == 1
    later_rare = next(
        item for item in later["parameter_results"] if item["parameter_case"] == "rare"
    )
    assert later_rare["baseline_source"] == "session_single_run"
    assert later_rare["baseline_evidence_id"] == result["evidence_id"]
    assert later_rare["plan_delta"]["baseline"] == rare["plan_delta"]["baseline"]
    assert later_rare["baseline"]["elapsed_ms"] == 150_000


@pytest.mark.asyncio
async def test_a_finalist_with_a_slow_baseline_runs_it_only_for_the_snapshot_proof() -> None:
    plans = _slow_case_plans()
    service, _store, _plans, executor = _service(plans=plans, binder=_typed_binder())
    session_id, (first,) = _session(service, CANDIDATE)

    result = await service.benchmark_candidate(
        session_id,
        first,
        "appdb",
        BASELINE,
        CANDIDATE,
        phase="finalist",
        parameter_cases=CASES,
        slow_baseline_cases={"rare": 150},
    )

    assert plans.count("baseline", "rare") == 1
    assert len(executor.sessions) == 2  # one snapshot comparison per parameter case
    assert result["classification"] == "improved"
    assert result["execution_protocol"]["slow_baseline_cases"] == {"rare": 150.0}


@pytest.mark.asyncio
async def test_slow_baseline_cases_must_name_measured_cases() -> None:
    service, _store, _plans, _executor = _service(
        plans=_slow_case_plans(), binder=_typed_binder()
    )
    session_id, (first,) = _session(service, CANDIDATE)

    with pytest.raises(ValueError, match="slow_baseline_cases"):
        await service.benchmark_candidate(
            session_id,
            first,
            "appdb",
            BASELINE,
            CANDIDATE,
            parameter_cases=CASES,
            slow_baseline_cases={"unknown": 150},
        )


def test_one_baseline_sample_needs_a_large_gain_and_never_hides_a_regression() -> None:
    def verdict(baseline: float, candidate: list[float]) -> str:
        result = _result([baseline], candidate, paired=False)
        result["baseline_single_sample"] = True
        classify_benchmark([result], [], require_equivalence=False)
        return result["comparison_margin"]["bucket_verdict"]

    # One sample has no measured noise: 20% faster is not a finding, 4x faster is.
    assert verdict(1000, [800, 810, 790]) == "no_material_difference"
    assert verdict(1000, [250, 240, 260]) == "improved"
    assert verdict(1000, [1500, 1490, 1510]) == "regressed"

    # Without the single-sample mark, one baseline sample stays inconclusive.
    unmarked = _result([1000], [250, 240, 260], paired=False)
    assert classify_benchmark([unmarked], [], require_equivalence=False)[0] == "inconclusive"


@pytest.mark.asyncio
async def test_benchmark_result_leads_with_a_small_decision_block() -> None:
    plans = ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 100)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (first,) = _session(service, CANDIDATE)
    result = await service.benchmark_candidate(
        session_id, first, "appdb", BASELINE, CANDIDATE, phase="finalist", parameter_cases=CASES
    )

    compact = compact_benchmark_result(result)

    assert list(compact)[:1] == ["decision"]
    decision = compact["decision"]
    assert decision["classification"] == "improved"
    assert decision["equivalence"] == {
        "status": "match",
        "proof_scope": "direct_snapshot",
        "deferred": False,
        "risk_codes": [],
        "rewrite_requirements": [],
    }
    common = decision["cases"][0]
    assert common == {
        "parameter_case": "common",
        "verdict": "improved",
        "speedup": 4.0,
        "baseline": {"elapsed_ms": 400.0, "cpu_ms": 400.0, "logical_reads": None},
        "candidate": {"elapsed_ms": 100.0, "cpu_ms": 100.0, "logical_reads": None},
    }
    # Spread, coverage and operator detail sit under one key the server can drop.
    assert "spread" not in str({key: value for key, value in compact.items() if key != "detail"})
    assert compact["detail"]["parameter_results"] == result["parameter_results"]
    assert compact["evidence_id"] == result["evidence_id"]
    import json

    lead = {key: value for key, value in compact.items() if key != "detail"}
    assert len(json.dumps(lead, default=str)) < 2000
    assert len(json.dumps(result, default=str)) > 4 * len(json.dumps(lead, default=str))


def test_evidence_result_summary_has_counts_and_ids() -> None:
    result: dict[str, Any] = {
        "performance_case_id": "case-1",
        "outcome": "actionable",
        "evidence": {"evidence_id": "evidence-9", "metadata": {"sections": {"big": "x" * 5000}}},
        "sections": {
            "query_store": {"available": True, "complete": True, "data": {"rows": [1, 2, 3]}},
            "wait_stats": {"available": False, "complete": False, "error": "timeout"},
        },
        "profile": {
            "plan_findings": {"findings": [{"rule": "a"}, {"rule": "b"}]},
            "metrics": {"elapsed_ms": 12.0, "cpu_ms": 9.0, "logical_reads": 813, "warning_count": 0},
        },
        "outcome_basis": {"profile_findings": 2},
        "incomplete_evidence_can_be_healthy": False,
    }

    compact = compact_evidence_result(result)

    assert compact["summary"] == {
        "performance_case_id": "case-1",
        "evidence_id": "evidence-9",
        "outcome": "actionable",
        "sections_total": 2,
        "sections_available": 1,
        "sections_complete": 1,
        "sections_unavailable": ["wait_stats"],
        "profile_findings": 2,
        "profile_metrics": {"elapsed_ms": 12.0, "cpu_ms": 9.0, "logical_reads": 813},
    }
    assert compact["detail"]["sections"] == result["sections"]
    assert list(compact) == ["summary", "detail"]
