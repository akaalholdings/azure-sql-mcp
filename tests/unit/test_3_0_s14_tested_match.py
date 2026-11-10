"""A tested ordered match counts (S14 item 2).

A candidate that returned the same rows in the same order as the original on
every measured case, in one snapshot per case, was classed performance-only
with no selectable winner because a static order check still had a gap. A DBA
compares the two results and accepts the match as a test. The tool now does
the same: the candidate is improved, its match is labelled tested, the gap is
a caveat, and it can be selected as tested. It is never called proven.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.server import _compact_benchmark_payload
from tests.unit.test_3_0_s13b2_view_equivalence import CANDIDATE
from tests.unit.test_3_0_s13b2_view_equivalence import _app
from tests.unit.test_3_0_s13b2_view_equivalence import _FactPostSandbox
from tests.unit.test_3_0_s13b2_view_equivalence import _run_view
from tests.unit.test_performance_workflows import RoutedPlans
from tests.unit.test_performance_workflows import SnapshotExecutor
from tests.unit.test_performance_workflows import _service

TIED = "SELECT TOP (10) i.id FROM dbo.Items AS i ORDER BY i.kind"
TIED_REWRITE = "SELECT TOP (10) candidate.id FROM dbo.Items AS candidate ORDER BY candidate.kind"
ORDERED = "SELECT i.id FROM dbo.Items AS i WHERE i.kind = 5 ORDER BY i.id"
NO_ORDER = "SELECT candidate.id FROM dbo.Items AS candidate WHERE candidate.kind = 5"


def _rows(*values: int) -> list[dict[str, Any]]:
    return [{"id": value} for value in values]


async def _finalist(
    baseline: str, rewrite: str, candidate_rows: list[dict[str, Any]]
) -> tuple[Any, str, str, dict[str, Any]]:
    executor = SnapshotExecutor(baseline_rows=_rows(1, 2), candidate_rows=candidate_rows)
    service, _store, _plans, _executor = _service(
        plans=RoutedPlans(baseline_ms=100, candidate_ms=20), executor=executor
    )
    case = service.start_case("appdb", baseline)
    session_id = service.start_session(case.case_id, "appdb")["session_id"]
    candidate = service.add_candidate(session_id, rewrite, strategy="predicate")
    result = await service.benchmark_candidate(
        session_id, candidate["candidate_id"], "appdb", baseline, rewrite, phase="finalist"
    )
    return service, session_id, candidate["candidate_id"], result


@pytest.mark.asyncio
async def test_the_same_rows_in_the_same_order_on_every_case_is_improved_and_tested() -> None:
    service, session_id, candidate_id, result = await _finalist(TIED, TIED_REWRITE, _rows(1, 2))

    assert result["classification"] == "improved", result["reason"]
    assert result["performance_classification"] == "improved"
    assert result["proof_scope"] == "snapshot_test"
    comparison = result["equivalence"][0]
    assert comparison["status"] == "match"
    assert comparison["ordered_rows_match"] is True
    assert comparison["tested_match"] is True
    # Tested on today's data: the static gap is named, never dropped.
    assert comparison["proven_for_parameter_case"] is False
    assert any("top_ties_possible" in caveat or "ORDER BY" in caveat for caveat in result["caveats"])
    assert "tested" in result["reason"]

    final = service.finalize_session(
        session_id,
        selected_candidate_id=candidate_id,
        selection_scope="tested",
        stopping_reason="measured",
    )

    assert final["selected_candidate_id"] == candidate_id
    assert final["selection_scope"] == "tested"
    assert final["selected_candidate_proof_scope"] == "snapshot_test"
    assert final["deployment_ready"] is False
    blockers = {item["code"]: item["reason"] for item in final["deployment_blockers"]}
    assert "tested on today's data" in blockers["tested_match_only"]


@pytest.mark.asyncio
async def test_a_tested_match_is_never_selected_as_proven_and_the_refusal_names_the_scope() -> None:
    service, session_id, candidate_id, _result = await _finalist(TIED, TIED_REWRITE, _rows(1, 2))

    with pytest.raises(ValueError, match="selection_scope 'tested'"):
        service.finalize_session(
            session_id,
            selected_candidate_id=candidate_id,
            selection_scope="proven",
            stopping_reason="measured",
        )


@pytest.mark.asyncio
async def test_the_same_rows_in_another_order_stay_performance_only_and_are_not_tested() -> None:
    """The order was not shown: a multiset match is not an ordered match."""

    service, session_id, candidate_id, result = await _finalist(ORDERED, NO_ORDER, _rows(2, 1))

    assert result["equivalence"][0]["ordered_rows_match"] is False
    assert result["classification"] == "performance_only"
    assert result["proof_scope"] == "performance_only"
    with pytest.raises(ValueError, match="tested"):
        service.finalize_session(
            session_id,
            selected_candidate_id=candidate_id,
            selection_scope="tested",
            stopping_reason="measured",
        )


@pytest.mark.asyncio
async def test_a_proven_candidate_is_not_selected_as_tested() -> None:
    plain = "SELECT i.id FROM dbo.Items AS i WHERE i.kind = 5 ORDER BY i.id"
    rewrite = "SELECT candidate.id FROM dbo.Items AS candidate WHERE candidate.kind = 5 ORDER BY candidate.id"
    service, session_id, candidate_id, result = await _finalist(plain, rewrite, _rows(1, 2))
    assert result["proof_scope"] == "direct_snapshot"

    with pytest.raises(ValueError, match="selection_scope 'proven'"):
        service.finalize_session(
            session_id,
            selected_candidate_id=candidate_id,
            selection_scope="tested",
            stopping_reason="measured",
        )


@pytest.mark.asyncio
async def test_a_view_statement_that_matched_in_order_under_an_order_gap_is_improved_and_tested(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _FactPostSandbox(app, monkeypatch)
    by_count = CANDIDATE.replace("ORDER BY v.PostTypeKey", "ORDER BY v.PostCount")

    result = await _run_view(app, sandbox, candidate_sql=by_count)

    assert result["classification"] == "improved", result["reason"]
    assert result["proof_scope"] == "snapshot_test"
    assert result["equivalence_deferred"] is False
    assert result["equivalence"][0]["ordered_rows_match"] is True
    assert any("ORDER BY" in caveat for caveat in result["caveats"])
    decision = _compact_benchmark_payload(result)["decision"]["equivalence"]
    assert decision["status"] == "match"
    assert decision["proof_scope"] == "snapshot_test"
    assert decision["risk_codes"] == ["order_contract_changed"]

    final = app.performance_workflows.finalize_session(
        result["session_id"],
        selected_candidate_id=result["candidate_id"],
        selection_scope="tested",
        stopping_reason="measured",
    )
    assert final["selected_candidate_proof_scope"] == "snapshot_test"
    assert final["deployment_ready"] is False


@pytest.mark.asyncio
async def test_a_candidate_without_the_order_by_is_not_a_tested_ordered_match_by_chance() -> None:
    """The rows came back in the same order, but nothing in the candidate promises it."""

    _service_, _session_id, _candidate_id, result = await _finalist(ORDERED, NO_ORDER, _rows(1, 2))

    assert result["equivalence"][0]["ordered_rows_match"] is True
    assert result["classification"] == "performance_only"
    assert "caveats" not in result
