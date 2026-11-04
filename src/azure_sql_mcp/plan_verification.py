"""Fail-closed comparison of Query Store pre/post enforcement evidence."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


_METRICS = ("avg_duration", "avg_cpu_time", "avg_logical_io_reads")
# Tail signals: optional, compared only when both sides report them.
_TAIL_METRICS = ("max_duration", "stdev_duration")
_PROVENANCE_FIELDS = ("source", "provenance", "environment", "database_name", "query_id")


@dataclass(frozen=True, slots=True)
class VerificationDecision:
    action: str
    reason: str
    improvement_pct: float | None = None
    regressed_metrics: tuple[str, ...] = ()


def hash_evidence(evidence: Mapping[str, Any]) -> str:
    """Hash one JSON-safe evidence payload using a stable representation."""

    payload = json.dumps(
        evidence,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def decide_verification(
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    expected_provenance: Mapping[str, Any],
    min_executions: int = 30,
    min_improvement_pct: float = 0.20,
    regression_tolerance_pct: float = 0.10,
    require_parameter_buckets: bool = True,
) -> VerificationDecision:
    """Return keep, rollback, or hold from comparable non-overlapping windows.

    Named parameter buckets must carry per-bucket metrics on both sides. With
    ``require_parameter_buckets=False`` evidence without buckets may still be
    compared, and a ``max_duration`` or ``stdev_duration`` regression (when
    both sides report it) blocks a keep.
    """

    baseline_meta = _evidence_metadata(baseline)
    candidate_meta = _evidence_metadata(candidate)
    if baseline_meta is None or candidate_meta is None:
        return _hold("Both baseline and candidate require evidence metadata.")

    for field in _PROVENANCE_FIELDS:
        baseline_value = baseline_meta.get(field)
        candidate_value = candidate_meta.get(field)
        if baseline_value is None or candidate_value is None:
            return _hold(f"Evidence provenance field {field!r} is missing.")
        if baseline_value != candidate_value:
            return _hold(f"Evidence provenance field {field!r} does not match.")
        expected = expected_provenance.get(field)
        if expected is not None and baseline_value != expected:
            return _hold(f"Evidence provenance field {field!r} is not the reviewed target.")

    if baseline_meta.get("post_change") is not False:
        return _hold("Baseline evidence must be explicitly pre-change.")
    if candidate_meta.get("post_change") is not True:
        return _hold("Candidate evidence must be explicitly post-change.")
    if bool(baseline_meta.get("truncated")) or bool(candidate_meta.get("truncated")):
        return _hold("Truncated evidence cannot verify a plan action.")
    if (
        baseline_meta.get("available", True) is not True
        or candidate_meta.get("available", True) is not True
    ):
        return _hold("Unavailable evidence cannot verify a plan action.")

    baseline_buckets = _parameter_buckets(baseline_meta.get("parameter_buckets"))
    candidate_buckets = _parameter_buckets(candidate_meta.get("parameter_buckets"))
    buckets_given = (
        baseline_meta.get("parameter_buckets") is not None
        or candidate_meta.get("parameter_buckets") is not None
    )
    if require_parameter_buckets or buckets_given:
        if baseline_buckets is None or candidate_buckets is None:
            return _hold("Named parameter buckets are required for verification.")
        if baseline_buckets != candidate_buckets:
            return _hold("Pre/post parameter buckets do not match.")

    baseline_start = _timestamp(baseline_meta.get("window_start"))
    baseline_end = _timestamp(baseline_meta.get("window_end"))
    candidate_start = _timestamp(candidate_meta.get("window_start"))
    candidate_end = _timestamp(candidate_meta.get("window_end"))
    if None in (baseline_start, baseline_end, candidate_start, candidate_end):
        return _hold("Valid collection windows are required for verification.")
    assert baseline_start is not None
    assert baseline_end is not None
    assert candidate_start is not None
    assert candidate_end is not None
    if baseline_start >= baseline_end or candidate_start >= candidate_end:
        return _hold("Evidence collection windows must have positive duration.")
    if candidate_start < baseline_end:
        return _hold("Pre/post evidence windows overlap.")

    baseline_count = _execution_count(baseline.get("count_executions"))
    candidate_count = _execution_count(candidate.get("count_executions"))
    if baseline_count is None or candidate_count is None:
        return _hold("Execution counts are missing or invalid.")
    if baseline_count < min_executions or candidate_count < min_executions:
        return _hold(
            f"At least {min_executions} executions are required in each evidence window."
        )

    if baseline.get("units") != candidate.get("units") and (
        baseline.get("units") is not None or candidate.get("units") is not None
    ):
        return _hold("Pre/post metric units do not match.")

    deltas: dict[str, float] = {}
    for metric in _METRICS:
        before = _finite_nonnegative(baseline.get(metric))
        after = _finite_nonnegative(candidate.get(metric))
        if before is None or after is None or before == 0:
            return _hold(f"Comparable non-zero {metric} evidence is required.")
        deltas[metric] = (before - after) / before

    regressed = tuple(
        metric
        for metric, improvement in deltas.items()
        if improvement < -regression_tolerance_pct
    )
    if baseline_buckets is not None and candidate_buckets is not None:
        # A blended average hides a regression in a minority bucket, so the
        # tolerance applies to each named bucket on its own.
        bucket_deltas = _bucket_deltas(baseline, candidate, baseline_buckets)
        if bucket_deltas is None:
            return _hold("Per-bucket metrics are required for every named parameter bucket.")
        regressed += tuple(
            name
            for name, improvement in bucket_deltas.items()
            if improvement < -regression_tolerance_pct
        )
    duration_improvement = deltas["avg_duration"]
    if regressed:
        return VerificationDecision(
            action="rollback",
            reason="One or more verified metrics regressed beyond tolerance.",
            improvement_pct=duration_improvement,
            regressed_metrics=regressed,
        )
    tail_regressed = tuple(
        metric
        for metric in _TAIL_METRICS
        if _regressed(baseline.get(metric), candidate.get(metric), regression_tolerance_pct)
    )
    if tail_regressed:
        return VerificationDecision(
            action="hold",
            reason=(
                f"{', '.join(tail_regressed)} regressed beyond tolerance; a slower "
                "minority of executions cannot be ruled out."
            ),
            improvement_pct=duration_improvement,
            regressed_metrics=tail_regressed,
        )
    if duration_improvement >= min_improvement_pct:
        return VerificationDecision(
            action="keep",
            reason="Duration improved without a material supporting-metric regression.",
            improvement_pct=duration_improvement,
        )
    return VerificationDecision(
        action="hold",
        reason="Evidence is comparable but does not meet the improvement threshold.",
        improvement_pct=duration_improvement,
    )


def _evidence_metadata(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    value = payload.get("evidence")
    return value if isinstance(value, Mapping) else None


def _parameter_buckets(value: Any) -> tuple[str, ...] | None:
    if not isinstance(value, (list, tuple)) or not value:
        return None
    buckets = [str(item).strip() for item in value]
    if any(not bucket for bucket in buckets) or len(set(buckets)) != len(buckets):
        return None
    return tuple(sorted(buckets))


def _bucket_deltas(
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
    buckets: tuple[str, ...],
) -> dict[str, float] | None:
    baseline_metrics = _per_bucket(baseline)
    candidate_metrics = _per_bucket(candidate)
    if baseline_metrics is None or candidate_metrics is None:
        return None
    deltas: dict[str, float] = {}
    for bucket in buckets:
        before_bucket = baseline_metrics.get(bucket)
        after_bucket = candidate_metrics.get(bucket)
        if not isinstance(before_bucket, Mapping) or not isinstance(after_bucket, Mapping):
            return None
        for metric in _METRICS:
            if metric not in before_bucket and metric not in after_bucket:
                if metric == "avg_duration":
                    return None
                continue
            before = _finite_nonnegative(before_bucket.get(metric))
            after = _finite_nonnegative(after_bucket.get(metric))
            if before is None or after is None or before == 0:
                return None
            deltas[f"{bucket}.{metric}"] = (before - after) / before
    return deltas


def _per_bucket(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    value = payload.get("per_bucket")
    if value is None:
        metadata = _evidence_metadata(payload)
        value = metadata.get("per_bucket") if metadata is not None else None
    if not isinstance(value, Mapping):
        return None
    return {str(name).strip(): metrics for name, metrics in value.items()}


def _regressed(before_value: Any, after_value: Any, tolerance: float) -> bool:
    before = _finite_nonnegative(before_value)
    after = _finite_nonnegative(after_value)
    if before is None or after is None or before == 0:
        return False
    return (before - after) / before < -tolerance


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _execution_count(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _finite_nonnegative(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    if not math.isfinite(number) or number < 0:
        return None
    return number


def _hold(reason: str) -> VerificationDecision:
    return VerificationDecision(action="hold", reason=reason)


__all__ = ["VerificationDecision", "decide_verification", "hash_evidence"]
