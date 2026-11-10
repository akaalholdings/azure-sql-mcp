"""One absolute floor per metric below which a difference is noise (S15 item 3).

A rewrite took the main case from 15,549 ms to 2,538 ms CPU and was classed
regressed as a whole, because an empty parameter case went from 0 ms to 5, 8
or 20 ms CPU. An index-only candidate at 4 ms on the same case was
unchanged_at_zero, and the result named no threshold. A case whose candidate
median stays under the floor is never a regression, and the result names the
floor.
"""

from __future__ import annotations

import pytest

from azure_sql_mcp.performance_workflows import REGRESSION_NOISE_FLOOR
from azure_sql_mcp.performance_workflows import classify_benchmark
from azure_sql_mcp.performance_workflows import verdicts_by_metric
from tests.unit.test_3_0_s13b2_verdicts import _case


def test_the_floors_are_stated_per_metric() -> None:
    assert REGRESSION_NOISE_FLOOR["cpu"] == 50.0
    assert REGRESSION_NOISE_FLOOR["elapsed_time"] == 50.0
    assert REGRESSION_NOISE_FLOOR["logical_reads"] == 200.0


@pytest.mark.parametrize("empty_case_cpu", [5, 8, 20])
def test_an_empty_case_at_a_few_milliseconds_does_not_fail_a_6x_win(empty_case_cpu: int) -> None:
    main = _case("october", {"cpu_ms": [15549, 15600, 15500]}, {"cpu_ms": [2538, 2540, 2530]})
    empty = _case("january", {"cpu_ms": [0, 0, 0]}, {"cpu_ms": [empty_case_cpu] * 3})

    state, reason = classify_benchmark(
        [main, empty], [], objective="cpu", require_equivalence=False
    )

    assert state == "promising", reason
    margin = empty["comparison_margin"]
    assert margin["bucket_verdict"] == "unchanged_at_zero"
    assert margin["noise_floor"] == 50.0
    assert margin["noise_floor_applied"] is True
    assert main["comparison_margin"]["noise_floor"] == 50.0
    assert main["comparison_margin"]["noise_floor_applied"] is False
    assert verdicts_by_metric([main, empty])["cpu"]["cases"]["january"] == "unchanged_at_zero"


def test_an_empty_case_that_reaches_the_floor_is_a_regression() -> None:
    main = _case("october", {"cpu_ms": [15549, 15600, 15500]}, {"cpu_ms": [2538, 2540, 2530]})
    empty = _case("january", {"cpu_ms": [0, 0, 0]}, {"cpu_ms": [60, 61, 60]})

    state, reason = classify_benchmark(
        [main, empty], [], objective="cpu", require_equivalence=False
    )

    assert state == "regressed"
    assert "january" in reason
    assert empty["comparison_margin"]["noise_floor_applied"] is False


def test_a_small_case_that_is_worse_but_stays_under_the_floor_is_not_a_regression() -> None:
    small = _case("small", {"cpu_ms": [10, 11, 10]}, {"cpu_ms": [30, 31, 30]})

    state, reason = classify_benchmark([small], [], objective="cpu", require_equivalence=False)

    assert state == "neutral", reason
    assert small["comparison_margin"]["bucket_verdict"] == "no_material_difference"
    assert small["comparison_margin"]["noise_floor_applied"] is True


def test_a_case_above_the_floor_that_is_worse_beyond_the_threshold_is_still_a_regression() -> None:
    slower = _case("slower", {"cpu_ms": [100, 101, 100]}, {"cpu_ms": [140, 141, 140]})

    state, _reason = classify_benchmark([slower], [], objective="cpu", require_equivalence=False)

    assert state == "regressed"
    assert slower["comparison_margin"]["noise_floor_applied"] is False


@pytest.mark.parametrize(("reads", "verdict"), [(150, "unchanged_at_zero"), (250, "regressed")])
def test_the_read_floor_is_200_pages(reads: int, verdict: str) -> None:
    empty = _case("empty", {"logical_reads": [0, 0, 0]}, {"logical_reads": [reads] * 3})

    classify_benchmark([empty], [], objective="logical_reads", require_equivalence=False)

    assert empty["comparison_margin"]["bucket_verdict"] == verdict
    assert empty["comparison_margin"]["noise_floor"] == 200.0


def test_the_compact_result_names_the_floor_on_the_case_it_decided() -> None:
    from azure_sql_mcp.performance_workflows import compact_benchmark_result

    main = _case("october", {"cpu_ms": [15549, 15600, 15500]}, {"cpu_ms": [2538, 2540, 2530]})
    empty = _case("january", {"cpu_ms": [0, 0, 0]}, {"cpu_ms": [8, 8, 8]})
    state, reason = classify_benchmark(
        [main, empty], [], objective="cpu", require_equivalence=False
    )

    compact = compact_benchmark_result(
        {
            "classification": state,
            "reason": reason,
            "objective": "cpu",
            "parameter_results": [main, empty],
            "equivalence": [],
        }
    )

    cases = {case["parameter_case"]: case for case in compact["decision"]["cases"]}
    assert cases["january"]["noise_floor"] == 50.0
    assert cases["january"]["noise_floor_metric"] == "cpu_ms"
    assert "noise_floor" not in cases["october"]
