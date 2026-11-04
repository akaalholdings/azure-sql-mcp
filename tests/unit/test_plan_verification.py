from __future__ import annotations

from copy import deepcopy

from azure_sql_mcp.plan_verification import decide_verification, hash_evidence


def _sample(*, duration: float, start: str, end: str, post_change: bool) -> dict:
    return {
        "count_executions": 40,
        "avg_duration": duration,
        "avg_cpu_time": 40.0 if not post_change else 30.0,
        "avg_logical_io_reads": 1000.0 if not post_change else 800.0,
        "units": {
            "avg_duration": "milliseconds",
            "avg_cpu_time": "milliseconds",
            "avg_logical_io_reads": "reads",
        },
        "evidence": {
            "source": "query-store",
            "provenance": "capture-v1",
            "environment": "test",
            "database_name": "appdb",
            "query_id": 42,
            "window_start": start,
            "window_end": end,
            "post_change": post_change,
            "parameter_buckets": ["common", "rare"],
            "truncated": False,
        },
        "per_bucket": {
            "common": {"avg_duration": duration},
            "rare": {"avg_duration": duration},
        },
    }


def _pair() -> tuple[dict, dict]:
    return (
        _sample(
            duration=100.0,
            start="2026-07-15T08:00:00Z",
            end="2026-07-15T09:00:00Z",
            post_change=False,
        ),
        _sample(
            duration=70.0,
            start="2026-07-15T09:00:00Z",
            end="2026-07-15T10:00:00Z",
            post_change=True,
        ),
    )


def _expected() -> dict:
    return {"environment": "test", "database_name": "appdb", "query_id": 42}


def test_keeps_material_improvement_with_matching_windows_and_buckets() -> None:
    baseline, candidate = _pair()

    decision = decide_verification(
        baseline,
        candidate,
        expected_provenance=_expected(),
    )

    assert decision.action == "keep"
    assert decision.improvement_pct == 0.3


def test_rolls_back_material_supporting_metric_regression() -> None:
    baseline, candidate = _pair()
    candidate["avg_cpu_time"] = 60.0

    decision = decide_verification(
        baseline,
        candidate,
        expected_provenance=_expected(),
    )

    assert decision.action == "rollback"
    assert decision.regressed_metrics == ("avg_cpu_time",)


def test_holds_for_overlapping_windows() -> None:
    baseline, candidate = _pair()
    candidate["evidence"]["window_start"] = "2026-07-15T08:30:00Z"

    decision = decide_verification(
        baseline,
        candidate,
        expected_provenance=_expected(),
    )

    assert decision.action == "hold"
    assert "overlap" in decision.reason.lower()


def test_holds_for_different_parameter_buckets() -> None:
    baseline, candidate = _pair()
    candidate["evidence"]["parameter_buckets"] = ["common", "boundary"]

    decision = decide_verification(
        baseline,
        candidate,
        expected_provenance=_expected(),
    )

    assert decision.action == "hold"
    assert "buckets" in decision.reason.lower()


def test_holds_for_target_or_provenance_drift() -> None:
    baseline, candidate = _pair()
    candidate["evidence"]["query_id"] = 99

    decision = decide_verification(
        baseline,
        candidate,
        expected_provenance=_expected(),
    )

    assert decision.action == "hold"
    assert "provenance" in decision.reason.lower()


def test_rolls_back_when_one_bucket_regresses_inside_a_better_blended_average() -> None:
    # Forced plans classically help the common parameter range and hurt a rare
    # one. 990 small runs 100 -> 50 ms and 10 large runs 100 -> 2000 ms blend
    # to a 30.5% "improvement"; the large-bucket 20x regression must win.
    baseline, candidate = _pair()
    baseline["per_bucket"] = {
        "common": {"avg_duration": 100.0},
        "rare": {"avg_duration": 100.0},
    }
    candidate["avg_duration"] = (990 * 50 + 10 * 2000) / 1000
    candidate["per_bucket"] = {
        "common": {"avg_duration": 50.0},
        "rare": {"avg_duration": 2000.0},
    }

    decision = decide_verification(
        baseline,
        candidate,
        expected_provenance=_expected(),
    )

    assert decision.action == "rollback"
    assert decision.regressed_metrics == ("rare.avg_duration",)


def test_holds_when_named_buckets_have_no_per_bucket_metrics() -> None:
    # Bucket names alone prove nothing: without per-bucket metrics the check
    # falls back to the blended average that hides a minority regression.
    baseline, candidate = _pair()
    del baseline["per_bucket"]
    del candidate["per_bucket"]

    decision = decide_verification(
        baseline,
        candidate,
        expected_provenance=_expected(),
    )

    assert decision.action == "hold"
    assert "per-bucket" in decision.reason.lower()


def _unbucketed_pair() -> tuple[dict, dict]:
    baseline, candidate = _pair()
    for sample in (baseline, candidate):
        del sample["evidence"]["parameter_buckets"]
        del sample["per_bucket"]
    return baseline, candidate


def test_unbucketed_evidence_can_keep_only_when_buckets_are_not_required() -> None:
    baseline, candidate = _unbucketed_pair()

    required = decide_verification(
        baseline,
        candidate,
        expected_provenance=_expected(),
    )
    optional = decide_verification(
        baseline,
        candidate,
        expected_provenance=_expected(),
        require_parameter_buckets=False,
    )

    assert required.action == "hold"
    assert optional.action == "keep"


def test_holds_when_max_duration_regresses_without_bucket_evidence() -> None:
    # Without parameter buckets the tail is the only signal that a minority
    # range got slower while the average improved.
    baseline, candidate = _unbucketed_pair()
    baseline["max_duration"] = 150.0
    candidate["max_duration"] = 2000.0

    decision = decide_verification(
        baseline,
        candidate,
        expected_provenance=_expected(),
        require_parameter_buckets=False,
    )

    assert decision.action == "hold"
    assert "max_duration" in decision.reason


def test_holds_when_duration_spread_regresses_under_an_unchanged_max() -> None:
    # A minority range that gets slower widens the spread before it moves the
    # max, so stdev is a second tail signal when there are no buckets.
    baseline, candidate = _unbucketed_pair()
    baseline["max_duration"] = candidate["max_duration"] = 500.0
    baseline["stdev_duration"] = 20.0
    candidate["stdev_duration"] = 90.0

    decision = decide_verification(
        baseline,
        candidate,
        expected_provenance=_expected(),
        require_parameter_buckets=False,
    )

    assert decision.action == "hold"
    assert decision.regressed_metrics == ("stdev_duration",)


def test_evidence_hash_is_stable_and_detects_changes() -> None:
    baseline, _candidate = _pair()
    same = deepcopy(baseline)
    changed = deepcopy(baseline)
    changed["avg_duration"] = 101.0

    assert hash_evidence(baseline) == hash_evidence(same)
    assert hash_evidence(baseline) != hash_evidence(changed)
