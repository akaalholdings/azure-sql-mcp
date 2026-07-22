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


def test_evidence_hash_is_stable_and_detects_changes() -> None:
    baseline, _candidate = _pair()
    same = deepcopy(baseline)
    changed = deepcopy(baseline)
    changed["avg_duration"] = 101.0

    assert hash_evidence(baseline) == hash_evidence(same)
    assert hash_evidence(baseline) != hash_evidence(changed)
