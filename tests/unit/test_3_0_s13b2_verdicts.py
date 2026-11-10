"""Verdicts that survive a zero-CPU case and a metadata-only difference (friction F12, F14, F15)."""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.performance_workflows import aggregate_samples
from azure_sql_mcp.performance_workflows import classify_benchmark
from azure_sql_mcp.performance_workflows import compact_benchmark_result
from azure_sql_mcp.performance_workflows import compare_result_collections
from azure_sql_mcp.performance_workflows import compare_result_sets
from tests.unit.test_performance_workflows import SnapshotExecutor
from tests.unit.test_performance_workflows import _service
from tests.unit.test_performance_workflows import _start


def _case(
    name: str,
    baseline: dict[str, list[float]],
    candidate: dict[str, list[float]],
) -> dict[str, Any]:
    def samples(series: dict[str, list[float]]) -> list[dict[str, Any]]:
        count = len(next(iter(series.values())))
        return [
            {
                "elapsed_ms": 50.0,
                "cpu_ms": 50.0,
                "logical_reads": 1000,
                "elapsed_source": "showplan_query_time_stats",
                "cpu_source": "showplan_query_time_stats",
                "read_source": "statistics_io_table_messages",
                **{metric: values[index] for metric, values in series.items()},
            }
            for index in range(count)
        ]

    return {
        "parameter_case": name,
        "weight": 1.0,
        "paired": True,
        "baseline": aggregate_samples(samples(baseline)),
        "candidate": aggregate_samples(samples(candidate)),
    }


def _match(name: str) -> dict[str, Any]:
    return {"parameter_case": name, "status": "match", "vacuous": False}


def test_a_zero_cpu_case_does_not_hide_a_58x_win_on_the_other_cases() -> None:
    """D08: two cases improved 6x and 58x; the empty-day case used 0 ms CPU on both sides."""

    month = _case("october", {"cpu_ms": [2900, 2950, 2900]}, {"cpu_ms": [50, 51, 50]})
    day = _case("one_day", {"cpu_ms": [600, 610, 600]}, {"cpu_ms": [100, 101, 100]})
    empty = _case("empty_day", {"cpu_ms": [0, 0, 0]}, {"cpu_ms": [0, 0, 0]})

    state, reason = classify_benchmark(
        [month, day, empty],
        [_match("october"), _match("one_day"), _match("empty_day")],
        objective="cpu",
    )

    assert state == "improved", reason
    assert empty["comparison_margin"]["bucket_verdict"] == "unchanged_at_zero"
    assert month["comparison_margin"]["bucket_verdict"] == "improved"


def test_a_zero_cpu_case_that_now_costs_cpu_is_a_regression() -> None:
    """The rewrite must not buy its win on the big case with new work on the free case."""

    month = _case("october", {"cpu_ms": [2900, 2950, 2900]}, {"cpu_ms": [50, 51, 50]})
    empty = _case("empty_day", {"cpu_ms": [0, 0, 0]}, {"cpu_ms": [40, 41, 40]})

    state, reason = classify_benchmark(
        [month, empty], [], objective="cpu", require_equivalence=False
    )

    assert state == "regressed"
    assert "empty_day" in reason
    assert empty["comparison_margin"]["bucket_verdict"] == "regressed"


def test_a_candidate_within_the_noise_floor_of_a_zero_baseline_is_not_a_regression() -> None:
    """1 ms CPU against 0 ms is timer resolution, not a regression (floor: 5 ms)."""

    month = _case("october", {"cpu_ms": [2900, 2950, 2900]}, {"cpu_ms": [50, 51, 50]})
    empty = _case("empty_day", {"cpu_ms": [0, 0, 0]}, {"cpu_ms": [1, 0, 2]})

    state, _reason = classify_benchmark(
        [month, empty], [], objective="cpu", require_equivalence=False
    )

    assert state == "promising"
    assert empty["comparison_margin"]["bucket_verdict"] == "unchanged_at_zero"


def test_every_case_at_zero_cpu_is_neutral_and_points_to_the_other_metrics() -> None:
    only = _case("point", {"cpu_ms": [0, 0, 0]}, {"cpu_ms": [0, 0, 0]})

    state, reason = classify_benchmark([only], [], objective="cpu", require_equivalence=False)

    assert state == "neutral"
    assert "by_metric" in reason


def test_decision_reports_every_metric_so_no_new_case_is_needed_to_change_objective() -> None:
    """O03: CPU was 0 ms in one case, so the agent opened a third case to judge reads."""

    point = _case(
        "point",
        {"cpu_ms": [0, 0, 0], "logical_reads": [90_000, 90_000, 90_000], "elapsed_ms": [3, 3, 3]},
        {"cpu_ms": [0, 0, 0], "logical_reads": [4, 4, 4], "elapsed_ms": [1, 1, 1]},
    )
    classification, reason = classify_benchmark(
        [point], [], objective="cpu", require_equivalence=False
    )

    decision = compact_benchmark_result(
        {
            "classification": classification,
            "reason": reason,
            "objective": "cpu",
            "parameter_results": [point],
            "equivalence": [],
        }
    )["decision"]

    assert decision["by_metric"]["logical_reads"]["classification"] == "improved"
    assert decision["by_metric"]["logical_reads"]["cases"] == {"point": "improved"}
    assert decision["by_metric"]["cpu"]["classification"] == "neutral"
    assert decision["by_metric"]["cpu"]["cases"] == {"point": "unchanged_at_zero"}
    assert set(decision["by_metric"]) == {"cpu", "elapsed_time", "logical_reads"}


def test_speedup_has_a_lower_bound_when_the_candidate_is_below_resolution() -> None:
    """O22: 31 ms CPU became 0 ms; speedup was null and the agent read medians by hand."""

    case = _case("default", {"cpu_ms": [31, 31, 32]}, {"cpu_ms": [0, 0, 0]})
    classification, reason = classify_benchmark(
        [case], [], objective="cpu", require_equivalence=False
    )

    decided = compact_benchmark_result(
        {
            "classification": classification,
            "reason": reason,
            "objective": "cpu",
            "parameter_results": [case],
        }
    )["decision"]["cases"][0]

    assert decided["verdict"] == "improved"
    assert decided["speedup"] is None
    assert decided["candidate_below_resolution"] is True
    assert decided["speedup_at_least"] == 31.0


def test_a_failed_equivalence_still_returns_the_per_case_verdict_and_speedup() -> None:
    """D05: 8.5 minutes of timings came back with null verdicts because one type differed."""

    case = _case("default", {"cpu_ms": [600, 610, 600]}, {"cpu_ms": [100, 101, 100]})

    state, _reason = classify_benchmark(
        [case],
        [{"parameter_case": "default", "status": "mismatch"}],
        objective="cpu",
    )

    assert state == "equivalence_failed"
    assert case["comparison_margin"]["bucket_verdict"] == "improved"


def _result(null_ok: bool, *, precision: int = 19, type_name: str = "builtins.int") -> QueryResult:
    return QueryResult(
        columns=("PostTypeKey", "TotalScore"),
        rows=[{"PostTypeKey": 1, "TotalScore": 10}],
        column_type_signatures=(
            "builtins.int|None|10|10|0|False",
            f"{type_name}|None|{precision}|{precision}|0|{null_ok!r}",
        ),
    )


def test_a_nullability_only_difference_is_named_and_is_not_a_result_mismatch() -> None:
    """D05 and D08: ISNULL(SUM(x), 0) is NOT NULL, SUM(x) is nullable; the rows are equal."""

    result = compare_result_sets(
        _result(True), _result(False), row_limit=10, compare_order=False, same_snapshot=True
    )

    assert result["status"] == "match"
    assert result["types_match"] is True
    assert result["metadata_difference"] == "nullability_only"
    assert result["type_differences"] == [
        {
            "ordinal": 2,
            "column": "TotalScore",
            "attribute": "nullability",
            "baseline": "nullable",
            "candidate": "not_null",
        }
    ]


def test_a_real_type_difference_names_the_column_and_the_attribute() -> None:
    result = compare_result_sets(
        _result(True),
        _result(True, precision=10),
        row_limit=10,
        compare_order=False,
        same_snapshot=True,
    )

    assert result["status"] == "mismatch"
    assert result["types_match"] is False
    assert "metadata_difference" not in result
    assert {(item["column"], item["attribute"]) for item in result["type_differences"]} == {
        ("TotalScore", "length"),
        ("TotalScore", "precision"),
    }
    assert "TotalScore" in result["reason"]


def test_matching_metadata_reports_no_difference() -> None:
    result = compare_result_sets(
        _result(True), _result(True), row_limit=10, compare_order=False, same_snapshot=True
    )

    assert result["status"] == "match"
    assert result["type_differences"] == []
    assert "metadata_difference" not in result


def test_the_nullability_difference_reaches_the_single_result_summary() -> None:
    result = compare_result_collections(
        [_result(True)], [_result(False)], row_limit=10, compare_order=False, same_snapshot=True
    )

    assert result["status"] == "match"
    assert result["metadata_difference"] == "nullability_only"
    assert result["type_differences"][0]["column"] == "TotalScore"


def test_the_decision_block_lists_metadata_differences_per_case() -> None:
    comparison = compare_result_collections(
        [_result(True)], [_result(False)], row_limit=10, compare_order=False, same_snapshot=True
    )

    decision = compact_benchmark_result(
        {
            "classification": "improved",
            "parameter_results": [],
            "equivalence": [{**comparison, "parameter_case": "default"}],
        }
    )["decision"]

    assert decision["equivalence"]["status"] == "match"
    assert decision["equivalence"]["metadata_differences"] == [
        {
            "parameter_case": "default",
            "ordinal": 2,
            "column": "TotalScore",
            "attribute": "nullability",
            "baseline": "nullable",
            "candidate": "not_null",
        }
    ]


@pytest.mark.asyncio
async def test_a_nullability_difference_keeps_the_verdict_and_blocks_deployment_by_name() -> None:
    """The rewrite is measured and wins; the changed column contract is a named blocker."""

    executor = SnapshotExecutor(
        baseline_type_signatures=("builtins.int|None|10|10|0|True",),
        candidate_type_signatures=("builtins.int|None|10|10|0|False",),
    )
    service, _store, _plans, _executor = _service(executor=executor)
    session_id, candidate_id = _start(service)

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        phase="finalist",
    )
    final = service.finalize_session(
        session_id, selected_candidate_id=candidate_id, stopping_reason="measured"
    )

    assert result["classification"] == "improved", result["reason"]
    assert result["equivalence"][0]["metadata_difference"] == "nullability_only"
    blocker = next(
        item
        for item in final["deployment_blockers"]
        if item["code"] == "result_nullability_changed"
    )
    assert "[id]" in blocker["reason"]
    assert "nullable -> not_null" in blocker["reason"]
    assert final["deployment_ready"] is False
