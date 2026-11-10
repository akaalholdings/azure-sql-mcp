"""Rewrite plus index in one session (friction F06, F07, F11; proposal P3).

A rewrite that is faster only with its index is neutral or slower alone. The
lineage rule refused such a parent, so the agent opened a second case and
session and reported two measurements for one answer.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.candidate_lineage import validate_combined_parent_request
from azure_sql_mcp.candidate_lineage import validate_rewrite_plus_index_parent_request
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.performance_contracts import EvidenceEnvelopeV1
from azure_sql_mcp.performance_contracts import TuningCandidateV1
from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_index_benchmark_workflow import _app
from tests.unit.test_index_benchmark_workflow import _install_catalog
from tests.unit.test_index_benchmark_workflow import _profile
from tests.unit.test_performance_workflows import RoutedPlans
from tests.unit.test_performance_workflows import _service

BASELINE = "SELECT id FROM dbo.Items"
REWRITE = "SELECT id FROM dbo.Items AS candidate"
FINGERPRINT = "sha256:" + "a" * 64
MATCHED = {
    "status": "match",
    "proven_for_parameter_case": True,
    "same_snapshot": True,
    "snapshot_isolation_verified": True,
}


def _parent(state: str) -> TuningCandidateV1:
    return TuningCandidateV1(
        candidate_id="candidate-parent",
        session_id="session-one",
        strategy="predicate",
        rewrite_fingerprint=FINGERPRINT,
        state=state,
        finalist_runs=5,
        evidence_ids=("evidence-parent",),
    )


def _finalist(classification: str, equivalence: list[dict[str, Any]], **metadata: Any) -> EvidenceEnvelopeV1:
    return EvidenceEnvelopeV1(
        evidence_id="evidence-parent",
        kind="tuning_finalist",
        observed_execution_count=12,
        metrics={"classification": classification},
        metadata={
            "session_id": "session-one",
            "candidate_id": "candidate-parent",
            "phase": "finalist",
            "proof_scope": "direct_snapshot",
            "equivalence": equivalence,
            **metadata,
        },
    )


def _validate(parent: TuningCandidateV1, proof: EvidenceEnvelopeV1) -> dict[str, Any]:
    return validate_rewrite_plus_index_parent_request(
        session_id="session-one",
        rewrite_fingerprint=FINGERPRINT,
        parent_reference="candidate:candidate-parent",
        parent=parent,
        evidence=[proof],
    )


@pytest.mark.parametrize("state", ["neutral", "regressed"])
def test_a_rewrite_that_is_not_faster_alone_is_a_valid_parent_when_its_results_matched(
    state: str,
) -> None:
    """O01, O04, D08: the rewrite needs its index; alone it is neutral or slower."""

    lineage = _validate(_parent(state), _finalist(state, [MATCHED]))

    assert lineage["parent_candidate_id"] == "candidate-parent"
    assert lineage["parent_equivalence"] == "proven"
    assert lineage["parent_state"] == state


def test_an_improved_parent_keeps_the_lineage_it_always_had() -> None:
    # Stored lineage of earlier sessions is compared with this dict for equality.
    lineage = _validate(_parent("improved"), _finalist("improved", [MATCHED]))

    assert "parent_state" not in lineage


def test_a_neutral_parent_whose_results_did_not_match_is_refused_with_state_and_route() -> None:
    mismatch = {**MATCHED, "status": "mismatch", "proven_for_parameter_case": False}

    with pytest.raises(ValueError) as refusal:
        _validate(_parent("neutral"), _finalist("neutral", [mismatch]))

    message = str(refusal.value)
    assert "neutral" in message
    assert "did not match" in message
    assert "candidate_sql" in message and "benchmark_index_candidate" in message


def test_a_neutral_parent_with_no_result_comparison_is_refused_with_state_and_route() -> None:
    with pytest.raises(ValueError) as refusal:
        _validate(
            _parent("neutral"),
            _finalist("neutral", [], proof_scope="performance_only", equivalence_deferred=True),
        )

    message = str(refusal.value)
    assert "neutral" in message
    assert "not compared" in message
    assert "candidate_sql" in message


def test_a_parent_that_never_ran_as_finalist_is_refused_with_its_state() -> None:
    with pytest.raises(ValueError) as refusal:
        _validate(_parent("screening"), _finalist("improved", [MATCHED]))

    message = str(refusal.value)
    assert "completed finalist" in message
    assert "'screening'" in message
    assert "candidate_sql" in message


def test_the_legacy_combined_lineage_still_needs_an_improved_parent() -> None:
    with pytest.raises(ValueError, match="completed finalist"):
        validate_combined_parent_request(
            session_id="session-one",
            rewrite_fingerprint=FINGERPRINT,
            parent_reference="candidate:candidate-parent",
            parent=_parent("neutral"),
            evidence=[_finalist("neutral", [MATCHED])],
        )


@pytest.mark.asyncio
async def test_a_neutral_finalist_takes_a_rewrite_plus_index_child_in_the_same_session() -> None:
    service, _store, _plans, _executor = _service(
        plans=RoutedPlans(baseline_ms=100, candidate_ms=100)
    )
    case = service.start_case("appdb", BASELINE)
    session_id = service.start_session(case.case_id, "appdb")["session_id"]
    parent = service.add_candidate(session_id, REWRITE, strategy="predicate")
    result = await service.benchmark_candidate(
        session_id, parent["candidate_id"], "appdb", BASELINE, REWRITE, phase="finalist"
    )
    assert result["classification"] == "neutral"

    child = service.add_candidate(
        session_id,
        REWRITE,
        strategy="rewrite_plus_index",
        artifact_ref=f"candidate:{parent['candidate_id']}",
    )

    assert child["metadata"]["lineage"]["parent_equivalence"] == "proven"
    assert child["metadata"]["lineage"]["parent_state"] == "neutral"


@pytest.mark.asyncio
async def test_a_rewrite_plus_index_candidate_needs_no_parent_even_after_a_finalist() -> None:
    """F11: after one finalist the session refused every rewrite_plus_index candidate."""

    service, _store, _plans, _executor = _service()
    case = service.start_case("appdb", BASELINE)
    session_id = service.start_session(case.case_id, "appdb")["session_id"]
    first = service.add_candidate(session_id, REWRITE, strategy="predicate")
    await service.benchmark_candidate(
        session_id, first["candidate_id"], "appdb", BASELINE, REWRITE, phase="finalist"
    )

    alone = service.add_candidate(
        session_id,
        "SELECT id FROM dbo.Items AS candidate WHERE id > 0",
        strategy="rewrite_plus_index",
    )

    assert alone["strategy"] == "rewrite_plus_index"
    assert alone["rewrite_artifact_ref"] is None
    assert "lineage" not in alone["metadata"]


# --- the one-call path: original against rewrite with its indexes ------------------


def _one_call_app(
    server_config_factory,
    *,
    candidate_rows: list[dict[str, Any]] | None = None,
) -> tuple[AzureSqlMcpApplication, str, str, list[str]]:
    app = _app(server_config_factory)
    case = app.performance_workflows.start_case("appdb", BASELINE)
    session_id = app.performance_workflows.start_session(case.case_id, "appdb")["session_id"]
    candidate = app.performance_workflows.add_candidate(
        session_id, REWRITE, strategy="rewrite_plus_index"
    )
    events: list[str] = []

    async def create(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        events.append("create_index")
        return {"status": "completed"}

    async def drop(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        events.append("drop_index")
        return {"status": "completed"}

    async def snapshot(_database: str, statements: list[str], **kwargs: Any) -> list[list[QueryResult]]:
        events.append("compare")
        hook = kwargs.get("before_statement_dispatch")
        for index in range(4):
            if hook is not None:
                hook(index)
        rows = [{"id": 1}]
        return [
            [],
            [],
            [QueryResult(columns=("id",), rows=rows, column_type_signatures=("synthetic-int",))],
            [
                QueryResult(
                    columns=("id",),
                    rows=rows if candidate_rows is None else candidate_rows,
                    column_type_signatures=("synthetic-int",),
                )
            ],
            [],
            [],
        ]

    app._create_test_index = AsyncMock(side_effect=create)  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(side_effect=drop)  # type: ignore[method-assign]
    app.executor.execute_session_exactly_once = AsyncMock(side_effect=snapshot)  # type: ignore[method-assign]
    return app, session_id, candidate["candidate_id"], events


async def _run_one_call(
    app: AzureSqlMcpApplication,
    session_id: str,
    candidate_id: str,
    *,
    sql: str = BASELINE,
    candidate_sql: str | None = REWRITE,
    phase: str = "finalist",
) -> dict[str, Any]:
    return await app._benchmark_index_candidate(
        "appdb", session_id, candidate_id, sql, "dbo", "Items", ["id"], None, None, False,
        phase, True, True, 30, "one-call", None, candidate_sql=candidate_sql,
    )


@pytest.mark.asyncio
async def test_one_call_measures_the_original_against_the_rewrite_with_its_index(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, session_id, candidate_id, events = _one_call_app(server_config_factory)
    index_name, _ = _install_catalog(monkeypatch, key_columns=["id"])
    profiled_sql: list[str] = []

    async def profile(_database: str, sql: str, **_kwargs: Any) -> Any:
        profiled_sql.append(sql)
        is_rewrite = "AS candidate" in sql
        return _profile(20 if is_rewrite else 100, index_name=index_name if is_rewrite else None)

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]

    result = await _run_one_call(app, session_id, candidate_id)

    assert result["classification"] == "improved", result["reason"]
    assert result["experiment"] == "original_against_rewrite_with_indexes"
    assert result["proof_scope"] == "direct_snapshot"
    assert result["lease"]["status"] == "cleaned"
    # Baseline arms ran the original, the candidate arm ran the rewrite.
    arms = ["rewrite" if "AS candidate" in sql else "original" for sql in profiled_sql]
    assert arms == ["original"] * 5 + ["rewrite"] * 5 + ["original"] * 5
    # The results were compared in the same call, while the index existed.
    assert events == ["create_index", "compare", "drop_index"]
    comparison = result["equivalence"][0]
    assert comparison["status"] == "match"
    assert comparison["same_snapshot"] is True
    assert comparison["snapshot_isolation_verified"] is True
    assert comparison["plan_used_expected_index"] is True
    assert result["executions"] == 15 + 2
    case = result["parameter_results"][0]
    assert case["baseline"]["elapsed_ms"] == 100
    assert case["candidate"]["elapsed_ms"] == 20

    final = app.performance_workflows.finalize_session(
        session_id, selected_candidate_id=candidate_id, stopping_reason="measured"
    )
    # A match on today's data, with no constraint behind it: not more than that.
    assert final["selected_candidate_proof_scope"] == "snapshot_matched"
    assert final["deployment_ready"] is False


@pytest.mark.asyncio
async def test_one_call_fails_the_candidate_when_the_rewrite_returns_other_rows(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, session_id, candidate_id, _events = _one_call_app(
        server_config_factory, candidate_rows=[{"id": 2}]
    )
    index_name, _ = _install_catalog(monkeypatch, key_columns=["id"])

    async def profile(_database: str, sql: str, **_kwargs: Any) -> Any:
        is_rewrite = "AS candidate" in sql
        return _profile(20 if is_rewrite else 100, index_name=index_name if is_rewrite else None)

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]

    result = await _run_one_call(app, session_id, candidate_id)

    assert result["classification"] == "equivalence_failed"
    assert result["lease"]["status"] == "cleaned"
    # The timings that were paid for are still judged per case.
    assert result["parameter_results"][0]["comparison_margin"]["bucket_verdict"] == "improved"


@pytest.mark.asyncio
async def test_an_index_candidate_with_rewritten_sql_is_told_the_one_call_route(
    server_config_factory,
) -> None:
    """F07: 'does not match the performance case' did not say what to do instead."""

    app = _app(server_config_factory)
    case = app.performance_workflows.start_case("appdb", BASELINE)
    session_id = app.performance_workflows.start_session(case.case_id, "appdb")["session_id"]
    candidate = app.performance_workflows.add_candidate(session_id, REWRITE, strategy="index")

    with pytest.raises(ValueError) as refusal:
        await _run_one_call(
            app, session_id, candidate["candidate_id"], sql=REWRITE, candidate_sql=None
        )

    message = str(refusal.value)
    assert "does not match the performance case" in message
    assert "rewrite_plus_index" in message and "candidate_sql" in message


@pytest.mark.asyncio
async def test_a_parentless_rewrite_plus_index_candidate_must_pass_candidate_sql(
    server_config_factory,
) -> None:
    app, session_id, candidate_id, events = _one_call_app(server_config_factory)

    with pytest.raises(ValueError) as refusal:
        await _run_one_call(app, session_id, candidate_id, sql=REWRITE, candidate_sql=None)

    assert "candidate_sql" in str(refusal.value)
    assert "sql" in str(refusal.value)
    assert events == []


@pytest.mark.asyncio
async def test_candidate_sql_must_be_the_registered_rewrite(server_config_factory) -> None:
    app, session_id, candidate_id, events = _one_call_app(server_config_factory)

    with pytest.raises(ValueError, match="candidate_sql does not match"):
        await _run_one_call(
            app,
            session_id,
            candidate_id,
            candidate_sql="SELECT id FROM dbo.Items AS candidate WHERE id > 5",
        )

    assert events == []


UNION_ORIGINAL = (
    "SELECT pl.PostId, pl.RelatedPostId FROM dbo.Items AS pl "
    "WHERE pl.PostId = 5 OR pl.RelatedPostId = 5 ORDER BY pl.PostId, pl.RelatedPostId"
)
UNION_REWRITE = (
    "SELECT candidate.PostId, candidate.RelatedPostId FROM ("
    "SELECT PostId, RelatedPostId FROM dbo.Items WHERE PostId = 5 "
    "UNION ALL "
    "SELECT PostId, RelatedPostId FROM dbo.Items WHERE RelatedPostId = 5 AND PostId <> 5"
    ") AS candidate ORDER BY candidate.PostId, candidate.RelatedPostId"
)


async def _union_all_one_call(
    server_config_factory, monkeypatch: pytest.MonkeyPatch, candidate_rows: list[dict[str, Any]] | None
) -> dict[str, Any]:
    app = _app(server_config_factory)
    case = app.performance_workflows.start_case("appdb", UNION_ORIGINAL)
    session_id = app.performance_workflows.start_session(case.case_id, "appdb")["session_id"]
    candidate = app.performance_workflows.add_candidate(
        session_id, UNION_REWRITE, strategy="rewrite_plus_index"
    )
    index_name, _ = _install_catalog(monkeypatch, key_columns=["id"])

    async def profile(_database: str, sql: str, **_kwargs: Any) -> Any:
        is_rewrite = "AS candidate" in sql
        return _profile(20 if is_rewrite else 100, index_name=index_name if is_rewrite else None)

    async def snapshot(_database: str, _statements: list[str], **kwargs: Any) -> list[list[QueryResult]]:
        hook = kwargs.get("before_statement_dispatch")
        for index in range(4):
            if hook is not None:
                hook(index)
        rows = [{"id": 1}]
        return [
            [], [],
            [QueryResult(columns=("id",), rows=rows, column_type_signatures=("synthetic-int",))],
            [
                QueryResult(
                    columns=("id",),
                    rows=rows if candidate_rows is None else candidate_rows,
                    column_type_signatures=("synthetic-int",),
                )
            ],
            [], [],
        ]

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.executor.execute_session_exactly_once = AsyncMock(side_effect=snapshot)  # type: ignore[method-assign]
    return await app._benchmark_index_candidate(
        "appdb", session_id, candidate["candidate_id"], UNION_ORIGINAL, "dbo", "Items", ["id"],
        None, None, False, "finalist", True, True, 30, "union-all", None,
        candidate_sql=UNION_REWRITE,
    )


@pytest.mark.asyncio
async def test_a_union_all_rewrite_is_tested_in_the_same_call_and_stays_performance_only(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O04: the static proof has a gap, so the results are tested, not proven."""

    result = await _union_all_one_call(server_config_factory, monkeypatch, None)

    assert result["classification"] == "performance_only", result["reason"]
    comparison = result["equivalence"][0]
    assert comparison["status"] == "match"
    assert comparison["proof_scope"] == "snapshot_test"
    assert comparison["proven_for_parameter_case"] is False
    assert comparison["comparison_basis"] == "multiset_with_duplicate_counts"
    assert result["proof_scope"] == "performance_only"
    assert "tested" in result["reason"] and "order_contract_changed" in result["reason"]


@pytest.mark.asyncio
async def test_a_union_all_rewrite_that_returns_other_rows_fails_in_the_same_call(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = await _union_all_one_call(
        server_config_factory, monkeypatch, [{"id": 1}, {"id": 1}]
    )

    assert result["classification"] == "equivalence_failed"
    assert result["equivalence"][0]["status"] == "mismatch"
