"""compare_query_results runs when a comparison can settle the gap (friction F16, F20; P5).

The tool returned ``proof_contract_required`` with zero executions for every
preflight gap, so the agent compared the two statements by hand through
execute_sql: seven refused calls in one task, and one hand comparison that
ran for over ten minutes with no cap.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.performance_workflows import MAX_COMPARISON_CASES
from azure_sql_mcp.performance_workflows import compare_result_sets
from tests.unit.test_index_benchmark_workflow import _app
from tests.unit.test_performance_workflows import SnapshotExecutor
from tests.unit.test_performance_workflows import _service
from tests.unit.test_performance_workflows import _typed_binder

ORIGINAL = (
    "SELECT pl.PostId, pl.RelatedPostId FROM dbo.PostLinks AS pl "
    "WHERE pl.PostId = 5 OR pl.RelatedPostId = 5 ORDER BY pl.PostId, pl.RelatedPostId"
)
UNION_ALL = (
    "SELECT u.PostId, u.RelatedPostId FROM ("
    "SELECT PostId, RelatedPostId FROM dbo.PostLinks WHERE PostId = 5 "
    "UNION ALL "
    "SELECT PostId, RelatedPostId FROM dbo.PostLinks WHERE RelatedPostId = 5 AND PostId <> 5"
    ") AS u ORDER BY u.PostId, u.RelatedPostId"
)
TOP_ONE = "SELECT TOP (1) d.Band FROM dbo.DimUser AS d WHERE d.UserId = 5 ORDER BY d.ValidFrom DESC"
TOP_ONE_REWRITE = (
    "SELECT TOP (1) candidate.Band FROM dbo.DimUser AS candidate "
    "WHERE candidate.UserId = 5 ORDER BY candidate.ValidFrom DESC"
)


def _rows(*values: int) -> list[dict[str, Any]]:
    return [{"id": value} for value in values]


def _gap(code: str):
    async def analyze(_database: str, _sql: str) -> dict[str, Any]:
        return {
            "direct_snapshot_supported": False,
            "classification": "proof_contract_required",
            "risk_codes": [code],
            "risks": [{"risk": code, "reason": f"reason for {code}"}],
        }

    return analyze


@pytest.mark.asyncio
async def test_a_union_all_rewrite_is_compared_and_the_scope_says_test_not_proof() -> None:
    """O04: the delivered UNION ALL form was refused seven times and compared by hand."""

    executor = SnapshotExecutor(baseline_rows=_rows(1, 2, 2), candidate_rows=_rows(2, 1, 2))
    service, _store, _plans, _executor = _service(executor=executor)

    result = await service.compare_query_results("appdb", ORIGINAL, UNION_ALL)

    assert len(executor.sessions) == 1  # it ran
    assert result["executions"] == 2
    assert result["status"] == "match"
    # The static proof has a gap, so a match is a test on today's data.
    assert result["preflight_gaps"] == ["order_contract_changed"]
    assert result["proof_scope"] == "snapshot_test"
    assert result["equivalence_scope"] == "snapshot_test_for_parameter_case"
    assert result["proven_for_parameter_case"] is False
    assert result["comparison_basis"] == "multiset_with_duplicate_counts"
    assert result["duplicate_rows"] == {"baseline": 1, "candidate": 1}
    assert result["ordered_rows_match"] is False
    assert result["not_settled"][0]["code"] == "order_contract_changed"
    assert "ORDER BY" in result["not_settled"][0]["reason"]


@pytest.mark.asyncio
async def test_a_union_all_rewrite_that_doubles_a_row_is_a_mismatch() -> None:
    """A branch pair that is not disjoint returns the shared row twice."""

    executor = SnapshotExecutor(baseline_rows=_rows(1, 2), candidate_rows=_rows(1, 2, 2))
    service, _store, _plans, _executor = _service(executor=executor)

    result = await service.compare_query_results("appdb", ORIGINAL, UNION_ALL)

    assert result["status"] == "mismatch"
    assert result["duplicate_rows"] == {"baseline": 0, "candidate": 1}
    assert result["proven_for_parameter_case"] is False


@pytest.mark.asyncio
async def test_a_top_with_possible_ties_is_compared_and_a_mismatch_is_labelled() -> None:
    """D08: TOP (1) ORDER BY ValidFrom with no unique key was refused outright."""

    matching = SnapshotExecutor(baseline_rows=_rows(7), candidate_rows=_rows(7))
    service, _store, _plans, _executor = _service(executor=matching)
    match = await service.compare_query_results("appdb", TOP_ONE, TOP_ONE_REWRITE)

    differing = SnapshotExecutor(baseline_rows=_rows(7), candidate_rows=_rows(8))
    service, _store, _plans, _executor = _service(executor=differing)
    mismatch = await service.compare_query_results("appdb", TOP_ONE, TOP_ONE_REWRITE)

    assert match["status"] == "match"
    assert match["preflight_gaps"] == ["row_limit_total_order_unproven"]
    assert match["proof_scope"] == "snapshot_test"
    assert mismatch["status"] == "mismatch"
    # Rows tied on the ORDER BY key may be returned in either order.
    assert mismatch["mismatch_may_be_a_tie"] is True


@pytest.mark.asyncio
async def test_statements_with_a_total_order_are_compared_as_ordered_rows() -> None:
    executor = SnapshotExecutor(baseline_rows=_rows(1, 2), candidate_rows=_rows(1, 2))
    service, _store, _plans, _executor = _service(executor=executor)

    result = await service.compare_query_results(
        "appdb",
        "SELECT id FROM dbo.Items ORDER BY id",
        "SELECT candidate.id FROM dbo.Items AS candidate ORDER BY candidate.id",
    )

    assert result["status"] == "match"
    assert result["comparison_basis"] == "ordered_rows"
    assert result["proof_scope"] == "direct_snapshot"
    assert result["equivalence_scope"] == "snapshot_matched_for_parameter_case"
    assert result["proven_for_parameter_case"] is True
    assert "preflight_gaps" not in result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    # S14: unordered_row_limit left this list; TOP without ORDER BY is now compared.
    ["row_volatile_function", "statement_stable_clock", "nonrepeatable_table_sample",
     "isolation_table_hint", "clock_function_in_view"],
)
async def test_a_gap_no_comparison_can_settle_is_still_refused_and_named(code: str) -> None:
    service, _store, _plans, executor = _service(equivalence_analyzer=_gap(code))

    result = await service.compare_query_results(
        "appdb", "SELECT id FROM dbo.Items", "SELECT id FROM dbo.Items AS candidate"
    )

    assert result["status"] == "proof_contract_required"
    assert result["executions"] == 0
    assert executor.sessions == []
    assert code in result["reason"]
    assert result["blocking_gaps"] == [code]


def test_duplicate_rows_are_counted_per_side() -> None:
    def result(*values: int) -> QueryResult:
        return QueryResult(
            columns=("id",), rows=_rows(*values), column_type_signatures=("synthetic-int",)
        )

    compared = compare_result_sets(
        result(1, 1, 1, 2), result(1, 1, 2, 2), row_limit=10, compare_order=False,
        same_snapshot=True,
    )

    assert compared["status"] == "mismatch"
    assert compared["baseline_duplicate_rows"] == 2
    assert compared["candidate_duplicate_rows"] == 2


# --- the time cap -------------------------------------------------------------------


class _SlowExecutor(SnapshotExecutor):
    """Each statement takes the given seconds; a cancel stops it, as the driver does."""

    def __init__(self, baseline_seconds: float, candidate_seconds: float) -> None:
        super().__init__()
        self.seconds = (baseline_seconds, candidate_seconds)
        self.cancelled = False

    async def execute_session_exactly_once(self, _database: str, statements: list[str], **kwargs: Any):
        hook = kwargs["before_statement_dispatch"]
        self.sessions.append(statements)
        try:
            hook(0)
            hook(1)
            hook(2)
            await asyncio.sleep(self.seconds[0])
            hook(3)
            await asyncio.sleep(self.seconds[1])
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        rows = [QueryResult(columns=("id",), rows=_rows(1), column_type_signatures=("synthetic-int",))]
        return [[], [], rows, rows, [], []]


@pytest.mark.asyncio
async def test_a_comparison_over_the_cap_stops_and_says_which_statement_was_running() -> None:
    """D08: one comparison ran for over ten minutes with no warning and no cap."""

    executor = _SlowExecutor(baseline_seconds=5, candidate_seconds=0)
    service, _store, _plans, _executor = _service(executor=executor)

    result = await service.compare_query_results(
        "appdb", "SELECT id FROM dbo.Items", "SELECT id FROM dbo.Items AS candidate",
        max_seconds=0.05,
    )

    assert executor.cancelled is True
    assert result["status"] == "timed_out"
    assert result["timed_out_arm"] == "baseline"
    assert result["completed_arms"] == []
    assert result["max_seconds"] == 0.05
    assert result["proven_for_parameter_case"] is False
    assert result["executions"] == 1
    assert "max_seconds" in result["reason"]


@pytest.mark.asyncio
async def test_the_partial_result_keeps_the_time_of_the_statement_that_finished() -> None:
    executor = _SlowExecutor(baseline_seconds=0.02, candidate_seconds=5)
    service, _store, _plans, _executor = _service(executor=executor)

    result = await service.compare_query_results(
        "appdb", "SELECT id FROM dbo.Items", "SELECT id FROM dbo.Items AS candidate",
        max_seconds=0.2,
    )

    assert result["status"] == "timed_out"
    assert result["timed_out_arm"] == "candidate"
    assert result["completed_arms"] == ["baseline"]
    assert 0.01 <= result["baseline_seconds"] < 0.2
    assert result["executions"] == 2


@pytest.mark.asyncio
async def test_a_comparison_the_cap_cannot_afford_is_not_started_and_says_so() -> None:
    service, _store, _plans, executor = _service()

    result = await service.compare_query_results(
        "appdb", "SELECT id FROM dbo.Items", "SELECT id FROM dbo.Items AS candidate",
        max_seconds=120, estimated_seconds=197,
    )

    assert executor.sessions == []
    assert result["status"] == "not_run"
    assert result["executions"] == 0
    assert result["needed_seconds"] == 394
    assert "max_seconds" in result["reason"] and "394" in result["reason"]


# --- several parameter cases in one call --------------------------------------------


PARAMETERIZED = "SELECT id FROM dbo.Items WHERE status = @p"
PARAMETERIZED_REWRITE = "SELECT candidate.id FROM dbo.Items AS candidate WHERE candidate.status = @p"


def _case(name: str, value: int) -> dict[str, Any]:
    return {"name": name, "values": {"p": value}, "types": {"p": "int"}, "weight": 1.0}


@pytest.mark.asyncio
async def test_several_parameter_cases_are_compared_in_one_call() -> None:
    """O04 made seven comparison calls for three cases."""

    executor = SnapshotExecutor()
    service, _store, _plans, _executor = _service(executor=executor, binder=_typed_binder())

    result = await service.compare_query_results(
        "appdb", PARAMETERIZED, PARAMETERIZED_REWRITE,
        parameter_cases=[_case("common", 1), _case("rare", 9), _case("none", 0)],
    )

    assert len(executor.sessions) == 3
    assert result["status"] == "match"
    assert result["executions"] == 6
    assert [(item["parameter_case"], item["status"]) for item in result["cases"]] == [
        ("common", "match"), ("rare", "match"), ("none", "match"),
    ]


@pytest.mark.asyncio
async def test_more_cases_than_the_limit_are_refused_with_the_limit() -> None:
    service, _store, _plans, executor = _service(binder=_typed_binder())

    with pytest.raises(ValueError, match=f"at most {MAX_COMPARISON_CASES} parameter cases"):
        await service.compare_query_results(
            "appdb", PARAMETERIZED, PARAMETERIZED_REWRITE,
            parameter_cases=[_case(f"case{index}", index) for index in range(5)],
        )

    assert executor.sessions == []


@pytest.mark.asyncio
async def test_the_tool_applies_a_default_cap_and_passes_the_new_arguments(
    server_config_factory,
) -> None:
    app = _app(server_config_factory)
    app.performance_workflows.compare_query_results = AsyncMock(return_value={"status": "match"})  # type: ignore[method-assign]

    await app.mcp._tool_manager.call_tool(
        "compare_query_results",
        {"baseline_sql": "SELECT 1 AS a", "candidate_sql": "SELECT 1 AS a"},
    )
    default = app.performance_workflows.compare_query_results.await_args.kwargs
    await app.mcp._tool_manager.call_tool(
        "compare_query_results",
        {
            "baseline_sql": PARAMETERIZED,
            "candidate_sql": PARAMETERIZED_REWRITE,
            "max_seconds": 30,
            "estimated_seconds": 4,
            "parameter_cases": [_case("common", 1), _case("rare", 9)],
        },
    )
    passed = app.performance_workflows.compare_query_results.await_args.kwargs

    # 120 seconds, or less when the server tool timeout would end the call first.
    assert default["max_seconds"] == min(120, app.config.tool_timeout_seconds - 5)
    assert passed["max_seconds"] == 30
    assert passed["estimated_seconds"] == 4
    assert [case["name"] for case in passed["parameter_cases"]] == ["common", "rare"]


@pytest.mark.asyncio
async def test_a_candidate_that_drops_the_order_by_is_compared_but_its_order_is_never_proven() -> None:
    """A run can return the baseline order by accident: the rows are compared as a
    multiset, and the result says that the order is not promised."""

    service, _store, _plans, executor = _service()

    result = await service.compare_query_results(
        "appdb", ORIGINAL, UNION_ALL.rsplit(" ORDER BY", 1)[0]
    )

    assert len(executor.sessions) == 1
    assert result["status"] == "match"
    assert result["comparison_basis"] == "multiset_with_duplicate_counts"
    assert result["proven_for_parameter_case"] is False
    assert any("no outer ORDER BY" in caveat for caveat in result["caveats"])
