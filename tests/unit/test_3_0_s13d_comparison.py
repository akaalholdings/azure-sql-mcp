"""Two statements that return rows are compared, and the result says how far it goes.

O01: a view statement measured at 4 reads came back ``not_compared``, and no
manual comparison was possible afterwards. The bare model simply compares
the two result sets. A comparison now runs under every gap that leaves the
rows stable inside one snapshot, reports the ordered and the multiset
outcome, and labels a match under a gap as a test on today's data.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.performance_workflows import label_comparison
from azure_sql_mcp.performance_workflows import order_contract_removed
from azure_sql_mcp.performance_workflows import preflight_gap_codes
from tests.unit.test_performance_workflows import RoutedPlans
from tests.unit.test_performance_workflows import SnapshotExecutor
from tests.unit.test_performance_workflows import _service

ORDERED = "SELECT i.id FROM dbo.Items AS i WHERE i.kind = 5 ORDER BY i.id"
NO_ORDER = "SELECT candidate.id FROM dbo.Items AS candidate WHERE candidate.kind = 5"
TIED = "SELECT TOP (10) i.id FROM dbo.Items AS i ORDER BY i.kind"
TIED_REWRITE = "SELECT TOP (10) candidate.id FROM dbo.Items AS candidate ORDER BY candidate.kind"


def _rows(*values: int) -> list[dict[str, Any]]:
    return [{"id": value} for value in values]


def _raw(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "rows_match": status == "match",
        "proven_for_parameter_case": status == "match",
        "baseline_duplicate_rows": 0,
        "candidate_duplicate_rows": 0,
    }


def test_an_ordered_match_without_a_proven_total_order_is_a_tested_match_with_the_tie_caveat() -> None:
    result = _raw("match")

    label_comparison(result, ["result_order_ties"], True)

    assert result["status"] == "match"
    assert result["ordered_rows_match"] is True
    assert result["multiset_match"] is True
    assert result["tested_match"] is True
    assert result["proof_scope"] == "snapshot_test"
    assert result["proven_for_parameter_case"] is False
    assert any("tied on the ORDER BY" in caveat for caveat in result["caveats"])


def test_a_clean_ordered_comparison_reports_both_outcomes_and_no_caveat() -> None:
    matched, reordered = _raw("match"), _raw("order_only_mismatch")

    label_comparison(matched, [], True)
    label_comparison(reordered, [], True)

    assert (matched["ordered_rows_match"], matched["multiset_match"]) == (True, True)
    assert "tested_match" not in matched and "caveats" not in matched
    # Same rows in another order: with a proven total order that is a real difference.
    assert (reordered["ordered_rows_match"], reordered["multiset_match"]) == (False, True)
    assert reordered["status"] == "order_only_mismatch"


def test_a_mismatch_reports_that_neither_comparison_matched() -> None:
    result = _raw("mismatch")

    label_comparison(result, ["result_order_ties"], True)

    assert (result["ordered_rows_match"], result["multiset_match"]) == (False, False)
    assert "tested_match" not in result


def _removed_order_preflight() -> dict[str, dict[str, Any]]:
    risk = {"risk": "order_contract_changed", "change": "removed", "reason": "removed"}
    return {
        "baseline": {"risk_codes": []},
        "candidate": {"risk_codes": ["order_contract_changed"], "details": [risk]},
    }


def test_a_removed_order_by_no_longer_blocks_the_comparison_and_is_named() -> None:
    preflight = _removed_order_preflight()

    gaps, blocking = preflight_gap_codes(preflight)

    assert gaps == ["order_contract_changed"]
    assert blocking == []
    assert order_contract_removed(preflight) is True


@pytest.mark.parametrize(
    "code",
    # S14: unordered_row_limit left this list; TOP without ORDER BY is now compared.
    ["row_volatile_function", "statement_stable_clock"],
)
def test_a_result_that_is_unstable_by_construction_still_blocks(code: str) -> None:
    _gaps, blocking = preflight_gap_codes(
        {"baseline": {"risk_codes": [code]}, "candidate": {"risk_codes": []}}
    )

    assert blocking == [code]


@pytest.mark.asyncio
async def test_a_candidate_that_drops_the_order_by_is_compared_as_a_multiset_with_the_caveat() -> None:
    executor = SnapshotExecutor(baseline_rows=_rows(1, 2), candidate_rows=_rows(2, 1))
    service, _store, _plans, _executor = _service(executor=executor)

    result = await service.compare_query_results("appdb", ORDERED, NO_ORDER)

    assert len(executor.sessions) == 1
    assert result["status"] == "match"
    assert result["multiset_match"] is True
    assert result["ordered_rows_match"] is False
    assert result["proof_scope"] == "snapshot_test"
    assert result["proven_for_parameter_case"] is False
    assert any("no outer ORDER BY" in caveat for caveat in result["caveats"])


@pytest.mark.asyncio
async def test_a_finalist_rewrite_under_a_tie_gap_is_compared_and_is_a_tested_match() -> None:
    """D08: TOP ... ORDER BY with possible ties was measured and never compared.

    S14: the same rows in the same order count as improved with a tested
    match (selection_scope tested); before, the candidate was performance-only.
    """

    executor = SnapshotExecutor(baseline_rows=_rows(1, 2), candidate_rows=_rows(1, 2))
    service, _store, _plans, _executor = _service(
        plans=RoutedPlans(baseline_ms=100, candidate_ms=20), executor=executor
    )
    case = service.start_case("appdb", TIED)
    session_id = service.start_session(case.case_id, "appdb")["session_id"]
    candidate = service.add_candidate(session_id, TIED_REWRITE, strategy="predicate")

    result = await service.benchmark_candidate(
        session_id, candidate["candidate_id"], "appdb", TIED, TIED_REWRITE, phase="finalist"
    )

    assert len(executor.sessions) == 1
    assert result["classification"] == "improved"
    assert result["proof_scope"] == "snapshot_test"
    comparison = result["equivalence"][0]
    assert comparison["status"] == "match"
    assert comparison["tested_match"] is True
    assert comparison["proof_scope"] == "snapshot_test"
    assert comparison["proven_for_parameter_case"] is False
    assert "tested" in result["reason"]
    # A rewrite benchmark builds nothing, and says where its time went too.
    assert set(result["time_spent"]) == {
        "baseline_runs", "build", "candidate_runs", "comparison", "cleanup", "other", "total",
    }
    assert result["time_spent"]["build"] == 0.0
    # The session can select it as a tested match, never as proven.
    final = service.finalize_session(
        session_id,
        selected_candidate_id=candidate["candidate_id"],
        selection_scope="tested",
        stopping_reason="measured",
    )
    assert final["selected_candidate_id"] == candidate["candidate_id"]
    assert final["deployment_ready"] is False


@pytest.mark.asyncio
async def test_a_finalist_rewrite_under_a_gap_that_returns_other_rows_fails_equivalence() -> None:
    executor = SnapshotExecutor(baseline_rows=_rows(1, 2), candidate_rows=_rows(1, 3))
    service, _store, _plans, _executor = _service(
        plans=RoutedPlans(baseline_ms=100, candidate_ms=20), executor=executor
    )
    case = service.start_case("appdb", ORDERED)
    session_id = service.start_session(case.case_id, "appdb")["session_id"]
    candidate = service.add_candidate(session_id, NO_ORDER, strategy="predicate")

    result = await service.benchmark_candidate(
        session_id, candidate["candidate_id"], "appdb", ORDERED, NO_ORDER, phase="finalist"
    )

    assert result["classification"] == "equivalence_failed"
    assert result["equivalence"][0]["status"] == "mismatch"
