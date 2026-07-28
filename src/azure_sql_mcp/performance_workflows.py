"""Durable Azure SQL performance cases and iterative benchmark execution.

The workflow persists fingerprints and aggregate evidence only. SQL text is
validated and used for the current call, but is never written to the state
database.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import statistics
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Mapping, Sequence

from .candidate_lineage import parse_combined_parent_reference
from .candidate_lineage import validate_combined_parent_request
from .connection import AzureSqlExecutor
from .connection import QueryResult
from .connection import StatementDispatchPrevented
from .database_policy import DatabasePolicySet
from .equivalence_contract import analyze_equivalence_preflight
from .performance_contracts import (
    EvidenceEnvelopeV1,
    PerformanceCaseV1,
    new_id,
    utc_now,
)
from .performance_store import PerformanceStore
from .param_binding import ParameterExecutionContract, TypedParameterBucket
from .param_binding import detect_parameters
from .plans import PlansService, ProfiledPlanResult
from .query_identity import query_identity
from .query_identity import query_identity_matches
from .query_identity import request_fingerprint
from .query_identity import server_database_identity
from .query_identity import server_database_identity_matches
from .safe_sql import SafeSqlValidator
from .tuning_sessions import DEFAULT_EXECUTIONS
from .tuning_sessions import DEFAULT_MAX_CANDIDATES
from .tuning_sessions import DEFAULT_TIME_LIMIT_SECONDS
from .tuning_sessions import TuningSessionStateMachine


ParameterCase = dict[str, Any]
EvidenceCollector = Callable[[], Awaitable[Any]]
ParameterBinder = Callable[
    [str, str, Mapping[str, Any]],
    Awaitable[ParameterExecutionContract],
]


OBJECTIVE_METRICS = {
    "elapsed_time": "elapsed_ms",
    "cpu": "cpu_ms",
    "logical_reads": "logical_reads",
    "physical_reads": "physical_reads",
}
OBJECTIVE_SOURCE_REQUIREMENTS = {
    "elapsed_time": ("elapsed_ms", "client_wall_clock"),
    "cpu": ("cpu_ms", "showplan_query_time_stats"),
    "logical_reads": ("reads", "statistics_io_table_messages"),
    "physical_reads": ("reads", "statistics_io_table_messages"),
}
MIN_RELATIVE_IMPROVEMENT = 0.10
MAX_ACCEPTABLE_NOISE_RATIO = 0.25
MIN_ABSOLUTE_IMPROVEMENT = {
    "elapsed_time": 5.0,
    "cpu": 5.0,
    "logical_reads": 100.0,
    "physical_reads": 10.0,
}


def fingerprint_text(value: str) -> str:
    return query_identity(value)


def fingerprint_text_matches(
    stored: str | None,
    value: str,
    *,
    allow_legacy: bool = False,
) -> bool:
    return query_identity_matches(stored, value, allow_legacy=allow_legacy)


def fingerprint_json(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def parameter_case_fingerprint(parameter_case: Mapping[str, Any]) -> str:
    return fingerprint_json(
        {
            "name": parameter_case.get("name"),
            "values": parameter_case.get("values", {}),
            "types": parameter_case.get("types", {}),
            "weight": parameter_case.get("weight", 1.0),
        }
    )


def database_fingerprint(
    database_name: str,
    server_name: str = "unspecified",
) -> str:
    return server_database_identity(
        server_name.strip().casefold(),
        database_name.strip().casefold(),
    )


def database_fingerprint_matches(
    stored: str,
    database_name: str,
    server_name: str = "unspecified",
    *,
    allow_legacy: bool = False,
) -> bool:
    return server_database_identity_matches(
        stored,
        server_name.strip().casefold(),
        database_name.strip().casefold(),
        allow_legacy=allow_legacy,
    )


def normalize_tuning_objective(value: Any) -> str:
    objective = str(value or "elapsed_time").strip().casefold()
    if objective not in OBJECTIVE_METRICS:
        raise ValueError(
            "objective must be elapsed_time, cpu, logical_reads, or physical_reads."
        )
    return objective


def _evidence_gaps(value: Any, path: str = "data") -> list[dict[str, str]]:
    """Find explicit availability/completeness gaps without copying source values."""

    gaps: list[dict[str, str]] = []
    if isinstance(value, Mapping):
        if value.get("available") is False:
            gaps.append({"path": path, "reason": "unavailable"})
        if value.get("ok") is False:
            gaps.append({"path": path, "reason": "collector_reported_failure"})
        if value.get("truncated") is True:
            gaps.append({"path": path, "reason": "truncated"})
        if value.get("complete") is False:
            gaps.append({"path": path, "reason": "incomplete"})
        status = value.get("status")
        if isinstance(status, str) and status.casefold() in {
            "error",
            "inconclusive",
            "partial",
            "unavailable",
        }:
            gaps.append({"path": path, "reason": f"status_{status.casefold()}"})
        for key, nested in value.items():
            if key in {"available", "ok", "truncated", "complete", "status"}:
                continue
            gaps.extend(_evidence_gaps(nested, f"{path}.{key}"))
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            gaps.extend(_evidence_gaps(nested, f"{path}[{index}]"))
    return gaps


def _canonical_row(row: Sequence[Any]) -> str:
    typed = [
        {
            "ordinal": ordinal,
            "python_type": f"{type(value).__module__}.{type(value).__qualname__}",
            "value": value,
        }
        for ordinal, value in enumerate(row)
    ]
    return json.dumps(typed, separators=(",", ":"), default=str)


def _first_tabular_result(results: Sequence[QueryResult]) -> QueryResult | None:
    return next((result for result in results if result.columns), None)


def _tabular_results(results: Sequence[QueryResult]) -> list[QueryResult]:
    return [result for result in results if result.columns]


def _positional_rows(result: QueryResult) -> list[tuple[Any, ...]]:
    return [tuple(row) for row in result.comparison_rows()]


def _bounded_result(
    result: QueryResult | None,
    row_limit: int,
) -> tuple[list[tuple[Any, ...]], bool]:
    if result is None:
        return [], False
    rows = _positional_rows(result)
    return rows[:row_limit], len(rows) > row_limit


def compare_result_sets(
    baseline: QueryResult | None,
    candidate: QueryResult | None,
    *,
    row_limit: int,
    compare_order: bool,
    same_snapshot: bool,
) -> dict[str, Any]:
    """Duplicate-aware complete comparison for bounded snapshot results."""

    if baseline is None or candidate is None:
        return {
            "status": "inconclusive",
            "reason": "a query did not return a tabular result set",
            "proven_for_parameter_case": False,
            "same_snapshot": same_snapshot,
        }
    positional_metadata_available = bool(
        getattr(baseline, "positional_rows_exact", False)
        and getattr(candidate, "positional_rows_exact", False)
    )
    duplicate_names = len(set(baseline.columns)) != len(baseline.columns) or len(
        set(candidate.columns)
    ) != len(candidate.columns)
    if duplicate_names and not positional_metadata_available:
        return {
            "status": "inconclusive",
            "reason": "duplicate output names require positional driver rows",
            "proven_for_parameter_case": False,
            "same_snapshot": same_snapshot,
            "columns_match": baseline.columns == candidate.columns,
            "type_metadata_available": (
                len(baseline.column_type_signatures) == len(baseline.columns)
                and len(candidate.column_type_signatures) == len(candidate.columns)
            ),
            "positional_metadata_available": False,
            "types_match": (
                baseline.column_type_signatures == candidate.column_type_signatures
                if len(baseline.column_type_signatures) == len(baseline.columns)
                and len(candidate.column_type_signatures) == len(candidate.columns)
                else None
            ),
            "rows_match": None,
            "baseline_row_count": min(len(baseline.rows), row_limit),
            "candidate_row_count": min(len(candidate.rows), row_limit),
            "truncated": len(baseline.rows) > row_limit or len(candidate.rows) > row_limit,
        }
    baseline_rows, baseline_truncated = _bounded_result(baseline, row_limit)
    candidate_rows, candidate_truncated = _bounded_result(candidate, row_limit)
    columns_match = baseline.columns == candidate.columns
    row_widths_match = all(
        len(row) == len(baseline.columns) for row in baseline_rows
    ) and all(len(row) == len(candidate.columns) for row in candidate_rows)
    type_metadata_available = (
        len(baseline.column_type_signatures) == len(baseline.columns)
        and len(candidate.column_type_signatures) == len(candidate.columns)
    )
    types_match = (
        baseline.column_type_signatures == candidate.column_type_signatures
        if type_metadata_available
        else None
    )
    baseline_canonical = [_canonical_row(row) for row in baseline_rows]
    candidate_canonical = [_canonical_row(row) for row in candidate_rows]
    if compare_order:
        rows_match = baseline_canonical == candidate_canonical
    else:
        rows_match = Counter(baseline_canonical) == Counter(candidate_canonical)
    truncated = baseline_truncated or candidate_truncated
    if not columns_match or not row_widths_match or not rows_match or types_match is False:
        status = "mismatch"
        reason = "result shape, types, duplicates, values, or required order differ"
        proven = False
    elif (
        not same_snapshot
        or truncated
        or not type_metadata_available
        or (duplicate_names and not positional_metadata_available)
    ):
        status = "inconclusive"
        if truncated:
            reason = "comparison exceeded the bounded full-result limit"
        elif not same_snapshot:
            reason = "queries were not compared in one snapshot"
        elif duplicate_names and not positional_metadata_available:
            reason = "duplicate output names require positional driver rows"
        else:
            reason = "driver result-type metadata was unavailable"
        proven = False
    else:
        status = "match"
        reason = (
            "complete duplicate-aware results and type metadata match for this parameter case"
        )
        proven = True
    return {
        "status": status,
        "reason": reason,
        "proven_for_parameter_case": proven,
        "same_snapshot": same_snapshot,
        "order_compared": compare_order,
        "columns_match": columns_match,
        "type_metadata_available": type_metadata_available,
        "positional_metadata_available": positional_metadata_available,
        "types_match": types_match,
        "rows_match": rows_match,
        "baseline_row_count": len(baseline_rows),
        "candidate_row_count": len(candidate_rows),
        "truncated": truncated,
    }


def compare_result_collections(
    baseline: Sequence[QueryResult],
    candidate: Sequence[QueryResult],
    *,
    row_limit: int,
    compare_order: bool,
    same_snapshot: bool,
) -> dict[str, Any]:
    """Compare every tabular result set in statement order."""

    baseline_sets = _tabular_results(baseline)
    candidate_sets = _tabular_results(candidate)
    if len(baseline_sets) != len(candidate_sets):
        return {
            "status": "mismatch",
            "reason": "the number of tabular result sets differs",
            "proven_for_parameter_case": False,
            "same_snapshot": same_snapshot,
            "baseline_result_set_count": len(baseline_sets),
            "candidate_result_set_count": len(candidate_sets),
            "result_sets": [],
        }
    if not baseline_sets:
        return {
            "status": "inconclusive",
            "reason": "neither query returned a tabular result set",
            "proven_for_parameter_case": False,
            "same_snapshot": same_snapshot,
            "baseline_result_set_count": 0,
            "candidate_result_set_count": 0,
            "result_sets": [],
        }

    comparisons = [
        {
            "ordinal": ordinal,
            **compare_result_sets(
                baseline_result,
                candidate_result,
                row_limit=row_limit,
                compare_order=compare_order,
                same_snapshot=same_snapshot,
            ),
        }
        for ordinal, (baseline_result, candidate_result) in enumerate(
            zip(baseline_sets, candidate_sets, strict=True),
            start=1,
        )
    ]
    statuses = {str(result["status"]) for result in comparisons}
    if "mismatch" in statuses:
        status = "mismatch"
        reason = "at least one tabular result set differs"
    elif statuses == {"match"}:
        status = "match"
        reason = "every tabular result set matches completely"
    else:
        status = "inconclusive"
        reason = "at least one tabular result set could not be proven equivalent"
    payload: dict[str, Any] = {
        "status": status,
        "reason": reason,
        "proven_for_parameter_case": status == "match",
        "same_snapshot": same_snapshot,
        "baseline_result_set_count": len(baseline_sets),
        "candidate_result_set_count": len(candidate_sets),
        "result_sets": comparisons,
    }
    if len(comparisons) == 1:
        payload.update(
            {
                key: value
                for key, value in comparisons[0].items()
                if key not in {"ordinal", "status", "reason", "proven_for_parameter_case"}
            }
        )
    return payload


def profile_result_fingerprint(
    profile: ProfiledPlanResult,
    *,
    compare_order: bool,
) -> dict[str, Any]:
    """Fingerprint every complete bounded result set without exposing values."""

    results = _tabular_results(profile.result_sets)
    if not results:
        return {
            "status": "inconclusive",
            "reason": "query did not return a tabular result",
            "complete": False,
        }
    result_payloads: list[dict[str, Any]] = []
    for result in results:
        rows = _positional_rows(result)
        type_metadata_available = (
            len(result.column_type_signatures) == len(result.columns)
        )
        positional_rows_exact = bool(result.positional_rows_exact)
        if not type_metadata_available or not positional_rows_exact:
            return {
                "status": "inconclusive",
                "reason": "exact positional/type metadata was unavailable",
                "complete": False,
                "truncated": profile.truncated,
                "result_set_count": len(results),
            }
        canonical_rows = [_canonical_row(row) for row in rows]
        if not compare_order:
            canonical_rows.sort()
        result_payloads.append(
            {
                "columns": result.columns,
                "types": result.column_type_signatures,
                "rows": canonical_rows,
                "order_compared": compare_order,
            }
        )
    if profile.truncated:
        return {
            "status": "inconclusive",
            "reason": "at least one result set exceeded the comparison limit",
            "complete": False,
            "truncated": True,
            "result_set_count": len(results),
        }
    return {
        "status": "complete",
        "complete": True,
        "fingerprint": fingerprint_json(result_payloads),
        "result_set_count": len(results),
        "row_counts": [len(payload["rows"]) for payload in result_payloads],
        "row_count": sum(len(payload["rows"]) for payload in result_payloads),
        "order_compared": compare_order,
        "truncated": False,
    }


def profile_payload(
    profile: ProfiledPlanResult,
    *,
    include_plan_xml: bool = False,
    include_result_sample: bool = False,
) -> dict[str, Any]:
    plan = profile.plan.as_dict(include_raw_xml=include_plan_xml)
    result = _first_tabular_result(profile.result_sets)
    rows = result.rows if result is not None else []
    payload = {
        "plan": plan,
        "result_shape": list(result.columns) if result is not None else [],
        "result_type_signatures": (
            list(result.column_type_signatures) if result is not None else []
        ),
        "row_count": len(rows),
        "truncated": profile.truncated,
        "user_query_executions": profile.user_query_executions,
        "metric_provenance": profile.metric_provenance,
        "metrics": extract_profile_metrics(profile),
    }
    if include_result_sample:
        payload["result_sample"] = rows
    return payload


def extract_profile_metrics(profile: ProfiledPlanResult) -> dict[str, Any]:
    metrics = profile.plan.summary.get("actual_metrics", {})
    statistics_io = profile.plan.summary.get("statistics_io", {})
    read_totals = (
        statistics_io.get("query_totals", {})
        if isinstance(statistics_io, Mapping)
        else {}
    )
    read_source = (
        statistics_io.get("query_totals_source")
        if isinstance(statistics_io, Mapping) and read_totals
        else metrics.get("read_metric_source")
    )
    return {
        "elapsed_ms": float(profile.elapsed_wall_ms),
        "cpu_ms": metrics.get("actual_cpu_ms"),
        "actual_rows": metrics.get("actual_rows"),
        "logical_reads": read_totals.get("logical_reads"),
        "physical_reads": read_totals.get("physical_reads"),
        "warning_count": len(profile.plan.summary.get("warnings", [])),
        "plan_fingerprint": structural_plan_fingerprint(profile.plan.summary),
        "elapsed_source": "client_wall_clock",
        "cpu_source": metrics.get("query_metric_source"),
        "read_source": read_source,
    }


def structural_plan_fingerprint(summary: Mapping[str, Any]) -> str:
    """Fingerprint compiled plan shape without per-execution counters."""

    statements = summary.get("statements")
    if isinstance(statements, list) and statements:
        plan_hashes = [
            {
                "query_hash": statement.get("query_hash"),
                "query_plan_hash": statement.get("query_plan_hash"),
            }
            for statement in statements
            if isinstance(statement, Mapping)
            and statement.get("query_plan_hash")
        ]
        if len(plan_hashes) == len(statements):
            return fingerprint_json({"query_plan_hashes": plan_hashes})

    operator_rows = summary.get("operators")
    if not isinstance(operator_rows, list) or not operator_rows:
        operator_rows = summary.get("top_operators")
    operators = []
    for operator in operator_rows if isinstance(operator_rows, list) else ():
        if not isinstance(operator, Mapping):
            continue
        operators.append(
            {
                key: operator.get(key)
                for key in (
                    "node_id",
                    "physical_op",
                    "logical_op",
                    "object",
                    "object_name",
                    "index_name",
                    "seek_predicates",
                    "residual_predicates",
                    "estimated_rows",
                    "estimated_rows_without_row_goal",
                    "estimated_io",
                    "estimated_cpu",
                    "estimated_subtree_cost",
                    "estimated_execution_mode",
                    "row_goal",
                    "row_goal_details",
                    "lookup",
                )
                if key in operator
            }
        )
    statement_shape = []
    for statement in statements if isinstance(statements, list) else ():
        if not isinstance(statement, Mapping):
            continue
        statement_shape.append(
            {
                key: statement.get(key)
                for key in (
                    "statement_text",
                    "statement_type",
                    "statement_subtree_cost",
                    "statement_est_rows",
                    "statement_optm_level",
                    "statement_optm_early_abort_reason",
                    "cardinality_estimation_model_version",
                    "query_hash",
                )
                if key in statement
            }
        )
    return fingerprint_json(
        {
            "statements": statement_shape,
            "operators": operators,
            "missing_indexes": summary.get("missing_indexes", []),
        }
    )


def _numeric(values: Sequence[Any]) -> list[float]:
    return [
        float(value)
        for value in values
        if isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    ]


def aggregate_samples(samples: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    aggregate: dict[str, Any] = {"sample_count": len(samples)}
    spread: dict[str, dict[str, float]] = {}
    coverage: dict[str, dict[str, Any]] = {}
    for name in ("elapsed_ms", "cpu_ms", "actual_rows", "logical_reads", "physical_reads"):
        values = _numeric([sample.get(name) for sample in samples])
        coverage[name] = {
            "available_samples": len(values),
            "required_samples": len(samples),
            "complete": bool(samples) and len(values) == len(samples),
        }
        if not values:
            aggregate[name] = None
            continue
        median = float(statistics.median(values))
        absolute_deviations = [abs(value - median) for value in values]
        mad = float(statistics.median(absolute_deviations))
        aggregate[name] = median
        spread[name] = {
            "min": min(values),
            "max": max(values),
            "range": max(values) - min(values),
            "median_absolute_deviation": mad,
            "relative_range": (
                (max(values) - min(values)) / median if median > 0 else 0.0
            ),
        }
    elapsed = aggregate.get("elapsed_ms")
    elapsed_range = spread.get("elapsed_ms", {}).get("range")
    aggregate["noise_ratio"] = (
        float(elapsed_range) / float(elapsed)
        if isinstance(elapsed, (int, float)) and elapsed > 0 and elapsed_range is not None
        else None
    )
    aggregate["spread"] = spread
    aggregate["metric_coverage"] = coverage
    aggregate["plan_fingerprints"] = sorted(
        {
            str(sample["plan_fingerprint"])
            for sample in samples
            if sample.get("plan_fingerprint")
        }
    )
    aggregate["plan_stable"] = len(aggregate["plan_fingerprints"]) <= 1
    aggregate["metric_sources"] = {
        "elapsed_ms": sorted(
            {
                str(sample["elapsed_source"])
                for sample in samples
                if sample.get("elapsed_source")
            }
        ),
        "cpu_ms": sorted(
            {
                str(sample["cpu_source"])
                for sample in samples
                if sample.get("cpu_source")
            }
        ),
        "reads": sorted(
            {
                str(sample["read_source"])
                for sample in samples
                if sample.get("read_source")
            }
        ),
    }
    return aggregate


def classify_benchmark(
    parameter_results: Sequence[Mapping[str, Any]],
    equivalence: Sequence[Mapping[str, Any]],
    *,
    objective: str = "elapsed_time",
    require_equivalence: bool = True,
    require_snapshot_attestation: bool = False,
) -> tuple[str, str]:
    if require_equivalence:
        if require_snapshot_attestation and (
            not equivalence
            or any(
                result.get("same_snapshot") is not True
                or result.get("snapshot_isolation_verified") is not True
                for result in equivalence
            )
        ):
            return (
                "inconclusive",
                "snapshot isolation was not verified for every parameter case",
            )
        if any(result.get("status") == "mismatch" for result in equivalence):
            return "equivalence_failed", "at least one parameter case returned different results"
        if not equivalence or any(
            result.get("status") != "match" for result in equivalence
        ):
            return (
                "inconclusive",
                "full snapshot equivalence was not proven for every parameter case",
            )

    objective = normalize_tuning_objective(objective)
    metric_name = OBJECTIVE_METRICS[objective]
    source_key, trusted_source = OBJECTIVE_SOURCE_REQUIREMENTS[objective]
    improvements: list[float] = []
    absolute_improvements: list[float] = []
    weights: list[float] = []
    noise_floor = MIN_RELATIVE_IMPROVEMENT

    def metric_noise_ratio(sample: Mapping[str, Any]) -> float:
        median = sample.get(metric_name)
        spread = sample.get("spread")
        metric_spread = spread.get(metric_name) if isinstance(spread, Mapping) else None
        observed_range = (
            metric_spread.get("range") if isinstance(metric_spread, Mapping) else None
        )
        if (
            isinstance(median, (int, float))
            and median > 0
            and isinstance(observed_range, (int, float))
        ):
            return float(observed_range) / float(median)
        if metric_name == "elapsed_ms":
            return float(sample.get("noise_ratio") or 0)
        return 0.0

    for result in parameter_results:
        baseline = result.get("baseline", {})
        candidate = result.get("candidate", {})
        if not isinstance(baseline, Mapping) or not isinstance(candidate, Mapping):
            return "inconclusive", "a parameter case did not return metric aggregates"
        before = baseline.get(metric_name)
        after = candidate.get(metric_name)
        if not isinstance(before, (int, float)) or before <= 0 or not isinstance(after, (int, float)):
            return "inconclusive", f"{objective} was unavailable for at least one parameter case"
        for aggregate in (baseline, candidate):
            sources = aggregate.get("metric_sources")
            objective_sources = (
                sources.get(source_key) if isinstance(sources, Mapping) else None
            )
            if objective_sources != [trusted_source]:
                return (
                    "inconclusive",
                    f"{objective} metric provenance was missing, mixed, or untrusted",
                )
        baseline_coverage = baseline.get("metric_coverage", {})
        candidate_coverage = candidate.get("metric_coverage", {})
        for coverage in (baseline_coverage, candidate_coverage):
            if isinstance(coverage, Mapping):
                metric_coverage = coverage.get(metric_name)
                if (
                    isinstance(metric_coverage, Mapping)
                    and metric_coverage.get("complete") is False
                ):
                    return (
                        "inconclusive",
                        f"{objective} did not have complete per-sample coverage",
                    )
        baseline_samples = int(baseline.get("sample_count", 0) or 0)
        candidate_samples = int(candidate.get("sample_count", 0) or 0)
        if min(baseline_samples, candidate_samples) < 2:
            return "inconclusive", "at least two paired samples are required"
        relative_improvement = (float(before) - float(after)) / float(before)
        improvements.append(relative_improvement)
        absolute_improvements.append(float(before) - float(after))
        raw_weight = result.get("weight", 1.0)
        weight = (
            float(raw_weight)
            if isinstance(raw_weight, (int, float))
            and not isinstance(raw_weight, bool)
            and float(raw_weight) > 0
            else 1.0
        )
        weights.append(weight)
        observed_noise = max(
            metric_noise_ratio(baseline),
            metric_noise_ratio(candidate),
        )
        if observed_noise > MAX_ACCEPTABLE_NOISE_RATIO:
            return (
                "inconclusive",
                f"{objective} variance exceeded the maximum trustworthy noise ratio",
            )
        noise_floor = max(
            noise_floor,
            observed_noise,
        )
    if not improvements:
        return "inconclusive", "no parameter cases were measured"
    regression_tolerance = max(0.10, noise_floor)
    if any(improvement < -regression_tolerance for improvement in improvements):
        return "regressed", "candidate materially regressed at least one tested parameter bucket"
    total_weight = sum(weights)
    weighted_improvement = sum(
        improvement * weight for improvement, weight in zip(improvements, weights)
    ) / total_weight
    weighted_absolute_improvement = sum(
        improvement * weight
        for improvement, weight in zip(absolute_improvements, weights)
    ) / total_weight
    if (
        weighted_improvement > noise_floor
        and weighted_absolute_improvement >= MIN_ABSOLUTE_IMPROVEMENT[objective]
    ):
        if require_equivalence:
            return (
                "improved",
                "candidate improved the weighted objective beyond noise and absolute thresholds",
            )
        return (
            "promising",
            "screening signal improved beyond noise; finalist equivalence is still required",
        )
    if weighted_improvement < -noise_floor:
        return "regressed", "candidate regressed beyond observed timing noise"
    return (
        "neutral",
        "candidate did not beat both observed noise and the absolute improvement threshold",
    )


def compare_plan_summaries_payload(
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    def summary(value: Mapping[str, Any]) -> Mapping[str, Any]:
        nested = value.get("summary")
        return nested if isinstance(nested, Mapping) else value

    before = summary(baseline)
    after = summary(candidate)
    before_metrics = before.get("actual_metrics", {})
    after_metrics = after.get("actual_metrics", {})
    deltas: dict[str, Any] = {}
    for name in ("actual_cpu_ms", "actual_elapsed_ms", "actual_rows"):
        left = before_metrics.get(name) if isinstance(before_metrics, Mapping) else None
        right = after_metrics.get(name) if isinstance(after_metrics, Mapping) else None
        deltas[name] = (
            float(right) - float(left)
            if isinstance(left, (int, float)) and isinstance(right, (int, float))
            else None
        )
    return {
        "baseline_fingerprint": fingerprint_json(before),
        "candidate_fingerprint": fingerprint_json(after),
        "metric_deltas": deltas,
        "operator_count_delta": int(after.get("operator_count", 0))
        - int(before.get("operator_count", 0)),
        "warning_count_delta": len(after.get("warnings", []))
        - len(before.get("warnings", [])),
        "missing_index_count_delta": len(after.get("missing_indexes", []))
        - len(before.get("missing_indexes", [])),
        "baseline_top_operators": before.get("top_operators", []),
        "candidate_top_operators": after.get("top_operators", []),
    }


class PerformanceWorkflowService:
    """MCP-owned cases, evidence, benchmarks, and session persistence."""

    def __init__(
        self,
        *,
        executor: AzureSqlExecutor,
        plans: PlansService,
        validator: SafeSqlValidator,
        store: PerformanceStore,
        sessions: TuningSessionStateMachine,
        database_policy: DatabasePolicySet,
        row_limit: int,
        parameter_binder: ParameterBinder | None = None,
        collector_timeout_seconds: float = 35.0,
        comparison_row_limit: int | None = None,
        server_name: str = "unspecified",
        allow_legacy_state: bool = False,
    ) -> None:
        self.executor = executor
        self.plans = plans
        self.validator = validator
        self.store = store
        self.sessions = sessions
        self.database_policy = database_policy
        self.row_limit = row_limit
        self.comparison_row_limit = max(
            row_limit,
            comparison_row_limit or row_limit,
        )
        self.parameter_binder = parameter_binder
        self.collector_timeout_seconds = collector_timeout_seconds
        self.server_name = server_name
        self.allow_legacy_state = allow_legacy_state

    def start_case(
        self,
        database_name: str,
        sql: str,
        *,
        parameter_cases: Sequence[ParameterCase] | None = None,
        metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> PerformanceCaseV1:
        normalized = self.validator.validate_read_only(sql).execution_sql
        cases = self._normalize_parameter_cases(parameter_cases)
        expected_parameters = {
            name.casefold() for name in detect_parameters(normalized)
        }
        for parameter_case in cases:
            value_parameters = {
                str(name).lstrip("@").casefold()
                for name in parameter_case["values"]
            }
            typed_parameters = {
                str(name).lstrip("@").casefold()
                for name in parameter_case["types"]
            }
            if (
                value_parameters != expected_parameters
                or typed_parameters != expected_parameters
            ):
                raise ValueError(
                    "Every performance-case parameter requires one explicit value "
                    "and SQL type in every parameter case."
                )
        normalized_metadata = dict(metadata or {})
        normalized_metadata["objective"] = normalize_tuning_objective(
            normalized_metadata.get("objective")
        )
        case = PerformanceCaseV1(
            query_fingerprint=fingerprint_text(normalized),
            database_fingerprint=database_fingerprint(
                database_name,
                self.server_name,
            ),
            parameter_case_fingerprints=tuple(
                parameter_case_fingerprint(case) for case in cases
            ),
            metadata={
                "parameter_case_names": [case["name"] for case in cases],
                "parameter_case_weights": [case["weight"] for case in cases],
                **normalized_metadata,
            },
        )
        return self.store.create_performance_case(
            case,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint(
                "performance-case.start",
                {
                    "query_fingerprint": case.query_fingerprint,
                    "database_fingerprint": case.database_fingerprint,
                    "parameter_case_fingerprints": case.parameter_case_fingerprints,
                    "metadata": case.metadata,
                },
            ),
        )

    def get_case(self, case_id: str) -> dict[str, Any]:
        case = self.store.get_performance_case(case_id)
        return {
            "case": case.to_dict(),
            "evidence": [
                self.store.get_evidence(evidence_id).to_dict()
                for evidence_id in case.baseline_evidence_ids
            ],
            "events": self.store.list_events(
                aggregate_type="performance_case",
                aggregate_id=case_id,
            ),
        }

    async def collect_case_evidence(
        self,
        case_id: str,
        database_name: str,
        sql: str,
        collectors: Mapping[str, EvidenceCollector],
        *,
        window_minutes: int,
        execute_query: bool = False,
        execution_contract: ParameterExecutionContract | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        case = self.store.get_performance_case(case_id)
        normalized = self.validator.validate_read_only(sql).execution_sql
        if not fingerprint_text_matches(
            case.query_fingerprint,
            normalized,
            allow_legacy=self.allow_legacy_state,
        ):
            raise ValueError("SQL fingerprint does not match the performance case.")
        if not database_fingerprint_matches(
            case.database_fingerprint or "",
            database_name,
            self.server_name,
            allow_legacy=self.allow_legacy_state,
        ):
            raise ValueError("Database fingerprint does not match the performance case.")
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")

        evidence_request_fingerprint = request_fingerprint(
            "performance-case.evidence",
            {
                "case_id": case_id,
                "database_name": database_name,
                "query_fingerprint": fingerprint_text(normalized),
                "database_fingerprint": case.database_fingerprint,
                "collector_names": tuple(sorted(collectors)),
                "window_minutes": window_minutes,
                "execute_query": execute_query,
                "execution_contract": (
                    {
                        "sql_fingerprint": fingerprint_text(execution_contract.sql_text),
                        "bucket_id": execution_contract.bucket_id,
                        "parameters": [
                            parameter.to_dict()
                            for parameter in execution_contract.parameters
                        ],
                    }
                    if execution_contract is not None
                    else None
                ),
            },
        )
        evidence_idempotency_key = (
            request_fingerprint(
                "performance-case.evidence.idempotency-key",
                {"key": idempotency_key},
            )
            if idempotency_key
            else None
        )

        committed_evidence = self.store.get_idempotent_evidence(
            evidence_idempotency_key,
            request_fingerprint=evidence_request_fingerprint,
        )
        if committed_evidence is not None:
            committed_metadata = dict(committed_evidence.metadata)
            committed_sections = committed_metadata.get("sections", {})
            committed_outcome = committed_metadata.get("outcome", "inconclusive")
            self.store.append_performance_case_evidence(
                case_id,
                committed_evidence.evidence_id,
                status=(
                    "ready"
                    if committed_outcome in {"healthy", "actionable"}
                    else "open"
                ),
                idempotency_key=evidence_idempotency_key,
                request_fingerprint=evidence_request_fingerprint,
            )
            return {
                "performance_case_id": case_id,
                "outcome": committed_outcome,
                "evidence": committed_evidence.to_dict(),
                "sections": (
                    committed_sections
                    if isinstance(committed_sections, Mapping)
                    else {}
                ),
                "profile": committed_metadata.get("profile_summary"),
                "incomplete_evidence_can_be_healthy": False,
                "recovered_from_durable_evidence": True,
            }

        window_end = datetime.now(timezone.utc)
        window_start = window_end - timedelta(minutes=window_minutes)
        started = utc_now()
        async def collect_one(
            name: str,
            collector: EvidenceCollector,
        ) -> tuple[str, dict[str, Any]]:
            captured_at = utc_now()
            try:
                data = await asyncio.wait_for(
                    collector(),
                    timeout=self.collector_timeout_seconds,
                )
                gaps = _evidence_gaps(data)
                top_level_available = not (
                    isinstance(data, Mapping) and data.get("available") is False
                )
                section = {
                    "available": top_level_available,
                    "complete": top_level_available and not gaps,
                    "collection_window_minutes": window_minutes,
                    "window_start_utc": window_start.isoformat(),
                    "window_end_utc": window_end.isoformat(),
                    "captured_at_utc": captured_at,
                    "truncated": any(gap["reason"] == "truncated" for gap in gaps),
                    "units": "source-defined",
                    "provenance": f"azure_sql_mcp:{name}",
                    "query_fingerprint": case.query_fingerprint,
                    "database_fingerprint": case.database_fingerprint,
                    "evidence_gaps": gaps,
                    "data": data,
                }
            except Exception as exc:
                section = {
                    "available": False,
                    "complete": False,
                    "collection_window_minutes": window_minutes,
                    "window_start_utc": window_start.isoformat(),
                    "window_end_utc": window_end.isoformat(),
                    "captured_at_utc": captured_at,
                    "truncated": False,
                    "units": "source-defined",
                    "provenance": f"azure_sql_mcp:{name}",
                    "query_fingerprint": case.query_fingerprint,
                    "database_fingerprint": case.database_fingerprint,
                    "evidence_gaps": [
                        {"path": "collector", "reason": "collection_error"}
                    ],
                    "error_type": type(exc).__name__,
                }
            return name, section

        collected = await asyncio.gather(
            *(
                collect_one(name, collector)
                for name, collector in collectors.items()
            )
        )
        sections = dict(collected)

        profile: dict[str, Any] | None = None
        observed_executions = 0
        if execute_query:
            self._require_benchmark_policy(database_name, 1)
            if execution_contract is not None:
                if not fingerprint_text_matches(
                    case.query_fingerprint,
                    execution_contract.sql_text,
                    allow_legacy=self.allow_legacy_state,
                ):
                    raise ValueError(
                        "Execution contract SQL does not match the performance case."
                    )
                profiled = await self._profile_execution_contract(
                    database_name,
                    execution_contract,
                )
            else:
                if detect_parameters(normalized):
                    raise ValueError(
                        "Active evidence for parameterized SQL requires one explicit "
                        "typed parameter case."
                    )
                profiled = await self.plans.profile_query(database_name, normalized)
            profile = profile_payload(profiled)
            observed_executions = profiled.user_query_executions

        available_count = sum(
            1 for section in sections.values() if section.get("available") is True
        )
        complete_count = sum(
            1 for section in sections.values() if section.get("complete") is True
        )
        actionable = self._has_actionable_evidence(sections)
        if available_count == 0 and profile is None:
            outcome = "inconclusive"
        elif complete_count < len(sections):
            outcome = "partial"
        elif actionable:
            outcome = "actionable"
        else:
            outcome = "healthy"

        evidence = EvidenceEnvelopeV1(
            source="azure-sql-mcp",
            kind="performance_triage",
            query_fingerprint=case.query_fingerprint,
            database_fingerprint=case.database_fingerprint,
            observed_execution_count=observed_executions,
            metrics={
                "available_sections": available_count,
                "complete_sections": complete_count,
                "total_sections": len(sections),
            },
            metadata={
                "outcome": outcome,
                "collection_started_at_utc": started,
                "collection_completed_at_utc": utc_now(),
                "collection_window_minutes": window_minutes,
                "window_start_utc": window_start.isoformat(),
                "window_end_utc": window_end.isoformat(),
                "sections": sections,
                "profile_summary": (
                    {key: value for key, value in profile.items() if key != "result_sample"}
                    if profile is not None
                    else None
                ),
            },
        )
        evidence = self.store.create_evidence(
            evidence,
            idempotency_key=evidence_idempotency_key,
            request_fingerprint=evidence_request_fingerprint,
        )
        self.store.append_performance_case_evidence(
            case_id,
            evidence.evidence_id,
            status="ready" if outcome in {"healthy", "actionable"} else "open",
            idempotency_key=evidence_idempotency_key,
            request_fingerprint=evidence_request_fingerprint,
        )
        return {
            "performance_case_id": case_id,
            "outcome": outcome,
            "evidence": evidence.to_dict(),
            "sections": sections,
            "profile": profile,
            "incomplete_evidence_can_be_healthy": False,
        }

    def start_session(
        self,
        case_id: str,
        database_name: str,
        *,
        max_candidates: int = DEFAULT_MAX_CANDIDATES,
        execution_limit: int = DEFAULT_EXECUTIONS,
        time_limit_minutes: int = DEFAULT_TIME_LIMIT_SECONDS // 60,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        case = self.store.get_performance_case(case_id)
        if not database_fingerprint_matches(
            case.database_fingerprint or "",
            database_name,
            self.server_name,
            allow_legacy=self.allow_legacy_state,
        ):
            raise PermissionError("Performance case belongs to another database.")
        policy = self.database_policy.require(database_name)
        if not policy.can_start_tuning_session(
            candidates=max_candidates,
            executions=execution_limit,
            minutes=time_limit_minutes,
        ):
            raise PermissionError(
                "Requested tuning session exceeds the local database policy: "
                f"requested {max_candidates} candidates, {execution_limit} executions, "
                f"and {time_limit_minutes} minutes; permitted up to "
                f"{policy.max_tuning_candidates} candidates, "
                f"{policy.max_tuning_session_executions} executions, and "
                f"{policy.max_tuning_session_minutes} minutes."
            )
        session = self.sessions.create_session(
            case,
            max_candidates=max_candidates,
            execution_limit=execution_limit,
            time_limit_seconds=time_limit_minutes * 60,
            metadata={"database_fingerprint": case.database_fingerprint},
            idempotency_key=idempotency_key,
        )
        return session.to_dict()

    def get_session(self, session_id: str) -> dict[str, Any]:
        session = self.sessions.get_session(session_id)
        candidates = self.sessions.list_candidates(session_id)
        evidence_ids = tuple(
            dict.fromkeys(
                evidence_id
                for candidate in candidates
                for evidence_id in candidate.evidence_ids
            )
        )
        execution_budget = self.store.execution_budget_usage(session_id)
        return {
            "session": session.to_dict(),
            "leaderboard": [candidate.to_dict() for candidate in candidates],
            "evidence": [
                self.store.get_evidence(evidence_id).to_dict()
                for evidence_id in evidence_ids
            ],
            "events": self.store.list_events(
                aggregate_type="session",
                aggregate_id=session_id,
            ),
            "budget": {
                "candidate_limit": session.max_candidates,
                "candidates_used": len(candidates),
                "candidate_slots_remaining": max(
                    0,
                    session.max_candidates - len(candidates),
                ),
                "execution_limit": session.execution_limit,
                "executions_used": execution_budget["consumed"],
                "executions_reserved": execution_budget["reserved"],
                "executions_committed": execution_budget["committed"],
                "executions_remaining": execution_budget["remaining"],
                "deadline_at_utc": session.deadline_at_utc,
            },
            "raw_sql_persisted": False,
        }

    def add_candidate(
        self,
        session_id: str,
        candidate_sql: str,
        *,
        strategy: str,
        artifact_ref: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        normalized = self.validator.validate_read_only(candidate_sql).execution_sql
        normalized_strategy = strategy.strip().casefold()
        allowed_strategies = {
            "predicate",
            "join",
            "aggregation",
            "cardinality",
            "index",
            "combined",
        }
        if normalized_strategy not in allowed_strategies:
            raise ValueError(
                "strategy must be predicate, join, aggregation, cardinality, "
                "index, or combined."
            )
        rewrite_fingerprint = fingerprint_text(normalized)
        request_metadata: dict[str, Any] = {"sql_persisted": False}
        candidate_metadata = dict(request_metadata)
        replay = self.sessions.replay_candidate_creation(
            session_id,
            strategy=normalized_strategy,
            rewrite_fingerprint=rewrite_fingerprint,
            rewrite_artifact_ref=artifact_ref,
            metadata=request_metadata,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            return replay.to_dict()
        if normalized_strategy == "combined":
            parent_id = parse_combined_parent_reference(artifact_ref)
            parent = self.sessions.get_candidate(parent_id)
            parent_evidence = [
                self.store.get_evidence(evidence_id)
                for evidence_id in parent.evidence_ids
            ]
            candidate_metadata["lineage"] = validate_combined_parent_request(
                session_id=session_id,
                rewrite_fingerprint=rewrite_fingerprint,
                parent_reference=artifact_ref,
                parent=parent,
                evidence=parent_evidence,
            )
        duplicate = next(
            (
                item
                for item in self.sessions.list_candidates(session_id)
                if fingerprint_text_matches(
                    item.rewrite_fingerprint,
                    normalized,
                    allow_legacy=self.allow_legacy_state,
                )
                and item.strategy == normalized_strategy
                and normalized_strategy != "index"
            ),
            None,
        )
        if duplicate is not None:
            raise ValueError(
                f"An equivalent {normalized_strategy} candidate is already registered."
            )
        candidate = self.sessions.add_candidate(
            session_id,
            strategy=normalized_strategy,
            rewrite_fingerprint=rewrite_fingerprint,
            rewrite_artifact_ref=artifact_ref,
            metadata=candidate_metadata,
            request_fingerprint_metadata=request_metadata,
            idempotency_key=idempotency_key,
        )
        return candidate.to_dict()

    async def compare_query_results(
        self,
        database_name: str,
        baseline_sql: str,
        candidate_sql: str,
        *,
        compare_order: bool = True,
        parameter_case: ParameterCase | None = None,
    ) -> dict[str, Any]:
        baseline = self.validator.validate_read_only(baseline_sql).execution_sql
        candidate = self.validator.validate_read_only(candidate_sql).execution_sql
        preflight = {
            "baseline": analyze_equivalence_preflight(baseline).as_dict(),
            "candidate": analyze_equivalence_preflight(candidate).as_dict(),
        }
        if any(
            not item["direct_snapshot_supported"] for item in preflight.values()
        ):
            return {
                "status": "proof_contract_required",
                "reason": (
                    "This MCP contract has no deterministic proof input for this "
                    "SQL shape; equivalence remains unproven."
                ),
                "proven_for_parameter_case": False,
                "executions": 0,
                "equivalence_preflight": preflight,
            }
        if parameter_case is not None:
            cases = self._normalize_parameter_cases([parameter_case])
            baseline_contract, candidate_contract = await self._bind_comparison_case(
                database_name,
                baseline,
                candidate,
                cases[0],
            )
            case_name = cases[0]["name"]
        else:
            if detect_parameters(baseline) or detect_parameters(candidate):
                raise ValueError(
                    "Parameterized result comparison requires one exact typed "
                    "parameter_case."
                )
            baseline_contract, candidate_contract = self._contracts_from_bucket(
                baseline,
                candidate,
                TypedParameterBucket(
                    bucket_id="unparameterized",
                    parameters=(),
                    provenance="direct_comparison",
                ),
            )
            case_name = "unparameterized"
        result = await self._compare_execution_contracts(
            database_name,
            baseline_contract,
            candidate_contract,
            compare_order=compare_order,
        )
        result["parameter_case"] = case_name
        return result

    async def _compare_execution_contracts(
        self,
        database_name: str,
        baseline: ParameterExecutionContract,
        candidate: ParameterExecutionContract,
        *,
        compare_order: bool,
        before_dispatch: Callable[[], Any] | None = None,
    ) -> dict[str, Any]:
        self._require_benchmark_policy(database_name, 2)
        baseline_sql = (
            baseline.sp_executesql_sql
            if baseline.parameters
            else baseline.sql_text
        )
        candidate_sql = (
            candidate.sp_executesql_sql
            if candidate.parameters
            else candidate.sql_text
        )
        baseline_params = (
            baseline.sp_executesql_values if baseline.parameters else ()
        )
        candidate_params = (
            candidate.sp_executesql_values if candidate.parameters else ()
        )
        dispatched_query_count = 0
        last_statement_index: int | None = None

        def before_statement_dispatch(statement_index: int) -> None:
            nonlocal dispatched_query_count, last_statement_index
            last_statement_index = statement_index
            if statement_index not in {2, 3}:
                return
            if before_dispatch is not None:
                before_dispatch()
            dispatched_query_count = statement_index - 1

        try:
            per_statement = await self.executor.execute_session_exactly_once(
                database_name,
                [
                    "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
                    (
                        "BEGIN TRANSACTION;\n"
                        "IF (@@TRANCOUNT <> 1)\n"
                        "   OR NOT EXISTS (\n"
                        "       SELECT 1\n"
                        "       FROM sys.dm_exec_sessions\n"
                        "       WHERE session_id = @@SPID\n"
                        "         AND transaction_isolation_level = 5\n"
                        "   )\n"
                        "BEGIN\n"
                        "    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;\n"
                        "    THROW 51000, "
                        "'Snapshot transaction could not be verified.', 1;\n"
                        "END"
                    ),
                    baseline_sql,
                    candidate_sql,
                    "ROLLBACK TRANSACTION",
                    "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
                ],
                max_rows=self.comparison_row_limit + 1,
                statement_params=[
                    None,
                    None,
                    baseline_params,
                    candidate_params,
                    None,
                    None,
                ],
                before_statement_dispatch=before_statement_dispatch,
            )
        except StatementDispatchPrevented as exc:
            if exc.statement_index not in {2, 3}:
                raise
            return {
                "status": "inconclusive",
                "reason": "snapshot comparison stopped before query dispatch",
                "error_type": type(exc.cause).__name__,
                "same_snapshot": False,
                "snapshot_isolation_verified": False,
                "proven_for_parameter_case": False,
                "executions": dispatched_query_count,
                "execution_count_is_conservative": False,
            }
        except Exception as exc:
            execution_count_is_conservative = last_statement_index is None
            return {
                "status": "inconclusive",
                "reason": "snapshot comparison could not complete",
                "error_type": type(exc).__name__,
                "same_snapshot": False,
                "snapshot_isolation_verified": False,
                "proven_for_parameter_case": False,
                "executions": (
                    2
                    if execution_count_is_conservative
                    else dispatched_query_count
                ),
                "execution_count_is_conservative": (
                    execution_count_is_conservative
                ),
            }
        baseline_results = per_statement[2] if len(per_statement) > 2 else []
        candidate_results = per_statement[3] if len(per_statement) > 3 else []
        comparison = compare_result_collections(
            baseline_results,
            candidate_results,
            row_limit=self.comparison_row_limit,
            compare_order=compare_order,
            same_snapshot=True,
        )
        comparison["snapshot_isolation_verified"] = True
        comparison["executions"] = 2
        comparison["execution_count_is_conservative"] = False
        return comparison

    def _recover_benchmark_result(
        self,
        *,
        session_id: str,
        candidate_id: str,
        phase: str,
        runs: int,
        parameter_case_count: int,
        reservation: Mapping[str, Any],
        reservation_owner: str,
        benchmark_request_fingerprint: str,
        evidence_operation_key: str | None,
        benchmark_operation_key: str | None,
        envelope: EvidenceEnvelopeV1 | None = None,
    ) -> dict[str, Any]:
        """Finish a committed benchmark result without dispatching SQL again."""

        if envelope is None:
            envelope = self.store.get_idempotent_evidence(
                evidence_operation_key,
                request_fingerprint=benchmark_request_fingerprint,
            )
        if envelope is None:
            reservation_status = str(reservation["status"])
            if reservation_status == "reserved":
                return {
                    "session_id": session_id,
                    "candidate_id": candidate_id,
                    "state": "inconclusive",
                    "reason": (
                        "This idempotent benchmark request has an active durable "
                        "reservation but no committed result. The query was not rerun."
                    ),
                    "failure_code": "benchmark_request_reconciliation_required",
                    "executions": 0,
                    "session_continues": True,
                    "execution_reservation_id": reservation["reservation_id"],
                    "reservation_status": reservation_status,
                }

            session = self.sessions.get_session(session_id)
            case = self.store.get_performance_case(
                session.performance_case_id
            )
            measured_executions = (
                int(reservation.get("attempt_count", 0))
                if reservation_status == "expired"
                else int(reservation.get("dispatched_attempt_count", 0))
            )
            failure_code = (
                "benchmark_request_expired"
                if reservation_status == "expired"
                else "benchmark_request_already_finalized"
            )
            reason = (
                "Recovered an expired benchmark reservation conservatively "
                "without rerunning SQL."
                if reservation_status == "expired"
                else "Recovered a finalized benchmark request without rerunning SQL."
            )
            envelope = EvidenceEnvelopeV1(
                source="azure-sql-mcp",
                kind=f"tuning_{phase}_failure",
                query_fingerprint=case.query_fingerprint,
                database_fingerprint=case.database_fingerprint,
                observed_execution_count=measured_executions,
                metrics={
                    "classification": "inconclusive",
                    "objective": normalize_tuning_objective(
                        case.metadata.get("objective")
                    ),
                    "parameter_results": [],
                },
                metadata={
                    "session_id": session_id,
                    "candidate_id": candidate_id,
                    "equivalence": [],
                    "equivalence_deferred": phase != "finalist",
                    "phase": phase,
                    "reason": reason,
                    "failure_code": failure_code,
                    "benchmark_failed": True,
                    "completed_parameter_cases": 0,
                    "execution_count_is_conservative": (
                        reservation_status == "expired"
                    ),
                    "execution_reservation_id": reservation["reservation_id"],
                    "sql_persisted": False,
                },
            )
            envelope = self.store.create_evidence(
                envelope,
                idempotency_key=evidence_operation_key,
                request_fingerprint=benchmark_request_fingerprint,
            )

        metadata = dict(envelope.metadata)
        if (
            metadata.get("session_id") != session_id
            or metadata.get("candidate_id") != candidate_id
            or metadata.get("phase") != phase
            or metadata.get("execution_reservation_id")
            != reservation["reservation_id"]
        ):
            raise RuntimeError(
                "Committed benchmark evidence does not match its execution reservation."
            )
        metrics = dict(envelope.metrics)
        state = str(metrics.get("classification") or "")
        if not state:
            raise RuntimeError("Committed benchmark evidence has no classification.")
        performance_classification = str(
            metrics.get("performance_classification") or state
        )
        candidate_state = (
            "inconclusive" if state == "proof_contract_required" else state
        )
        measured_executions = int(envelope.observed_execution_count)
        benchmark_failed = metadata.get("benchmark_failed") is True
        failure_code = str(
            metadata.get("failure_code") or state
        )
        completed_parameter_cases = int(
            metadata.get("completed_parameter_cases", parameter_case_count)
        )
        if benchmark_failed:
            _session, updated_candidate = self.sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="inconclusive",
                screen_runs=0,
                finalist_runs=0,
                parameter_cases=completed_parameter_cases,
                executions=measured_executions,
                evidence_ids=(envelope.evidence_id,),
                failure_code=failure_code[:100].replace(" ", "_"),
                idempotency_key=benchmark_operation_key,
            )
            durable_state = updated_candidate.state
        elif phase == "screening" and candidate_state in {"promising", "improved"}:
            _session, updated_candidate = self.sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="screening",
                screen_runs=runs,
                parameter_cases=parameter_case_count,
                executions=measured_executions,
                evidence_ids=(envelope.evidence_id,),
                idempotency_key=benchmark_operation_key,
            )
            durable_state = "screening"
        else:
            _session, updated_candidate = self.sessions.record_candidate_result(
                session_id,
                candidate_id,
                state=candidate_state,
                screen_runs=runs if phase == "screening" else 0,
                finalist_runs=runs if phase == "finalist" else 0,
                parameter_cases=parameter_case_count,
                executions=measured_executions,
                evidence_ids=(envelope.evidence_id,),
                failure_code=(
                    None
                    if candidate_state in {"improved", "neutral", "regressed"}
                    else state
                ),
                idempotency_key=benchmark_operation_key,
            )
            durable_state = updated_candidate.state

        reservation_status = str(reservation["status"])
        if reservation_status in {"reserved", "expired"}:
            reservation_update = (
                self.store.complete_execution_attempts
                if measured_executions
                else self.store.release_execution_attempts
            )
            finalized_reservation = reservation_update(
                str(reservation["reservation_id"]),
                dispatched_attempt_count=measured_executions,
                owner_reference=reservation_owner,
                expected_version=int(reservation["version"]),
            )
            reservation_status = str(finalized_reservation["status"])

        parameter_results = metrics.get("parameter_results")
        equivalence = metadata.get("equivalence")
        result = {
            "session_id": session_id,
            "candidate_id": candidate_id,
            "classification": state,
            "performance_classification": performance_classification,
            "objective": metrics.get("objective"),
            "durable_state": durable_state,
            "reason": metadata.get("reason")
            or "Recovered a committed benchmark result without rerunning SQL.",
            "phase": phase,
            "runs_per_parameter_case": runs,
            "executions": measured_executions,
            "parameter_results": (
                parameter_results if isinstance(parameter_results, list) else []
            ),
            "equivalence": equivalence if isinstance(equivalence, list) else [],
            "equivalence_deferred": bool(metadata.get("equivalence_deferred")),
            "equivalence_preflight": metadata.get("equivalence_preflight"),
            "proof_scope": metadata.get("proof_scope"),
            "evidence_id": envelope.evidence_id,
            "execution_reservation_id": reservation["reservation_id"],
            "reservation_status": reservation_status,
            "recovered_from_durable_evidence": True,
            "session_continues": True,
            "candidate": updated_candidate.to_dict(),
        }
        if benchmark_failed:
            result["state"] = "inconclusive"
            result["failure_code"] = failure_code
        return result

    def _persist_benchmark_failure_receipt(
        self,
        *,
        session_id: str,
        candidate_id: str,
        phase: str,
        runs: int,
        cases: Sequence[ParameterCase],
        case: PerformanceCaseV1,
        parameter_results: Sequence[Mapping[str, Any]],
        equivalence: Sequence[Mapping[str, Any]],
        completed_parameter_cases: int,
        measured_executions: int,
        failure_code: str,
        reservation: Mapping[str, Any],
        reservation_owner: str,
        benchmark_request_fingerprint: str,
        evidence_operation_key: str | None,
        benchmark_operation_key: str | None,
        objective: str,
        should_prove_equivalence: bool,
    ) -> dict[str, Any]:
        reason = "candidate benchmark failed; continue with the next candidate"
        envelope = EvidenceEnvelopeV1(
            source="azure-sql-mcp",
            kind=f"tuning_{phase}_failure",
            query_fingerprint=case.query_fingerprint,
            database_fingerprint=case.database_fingerprint,
            parameters_fingerprint=fingerprint_json(
                [
                    {
                        "name": parameter_case["name"],
                        "values_fingerprint": fingerprint_json(
                            parameter_case["values"]
                        ),
                        "types_fingerprint": fingerprint_json(
                            parameter_case["types"]
                        ),
                        "weight": parameter_case["weight"],
                    }
                    for parameter_case in cases
                ]
            ),
            observed_execution_count=measured_executions,
            metrics={
                "classification": "inconclusive",
                "objective": objective,
                "parameter_results": list(parameter_results),
            },
            metadata={
                "session_id": session_id,
                "candidate_id": candidate_id,
                "equivalence": list(equivalence),
                "equivalence_deferred": not should_prove_equivalence,
                "phase": phase,
                "reason": reason,
                "failure_code": failure_code,
                "benchmark_failed": True,
                "completed_parameter_cases": completed_parameter_cases,
                "execution_count_is_conservative": False,
                "execution_reservation_id": reservation["reservation_id"],
                "sql_persisted": False,
            },
        )
        envelope = self.store.create_evidence(
            envelope,
            idempotency_key=evidence_operation_key,
            request_fingerprint=benchmark_request_fingerprint,
        )
        result = self._recover_benchmark_result(
            session_id=session_id,
            candidate_id=candidate_id,
            phase=phase,
            runs=runs,
            parameter_case_count=len(cases),
            reservation=reservation,
            reservation_owner=reservation_owner,
            benchmark_request_fingerprint=benchmark_request_fingerprint,
            evidence_operation_key=evidence_operation_key,
            benchmark_operation_key=benchmark_operation_key,
            envelope=envelope,
        )
        result["recovered_from_durable_evidence"] = False
        return result

    async def benchmark_candidate(
        self,
        session_id: str,
        candidate_id: str,
        database_name: str,
        baseline_sql: str,
        candidate_sql: str,
        *,
        phase: str = "screening",
        parameter_cases: Sequence[ParameterCase] | None = None,
        compare_order: bool = True,
        runs_override: int | None = None,
        prove_equivalence: bool | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        session = self.sessions.get_session(session_id)
        candidate = self.sessions.get_candidate(candidate_id)
        if candidate.session_id != session_id:
            raise ValueError("Candidate does not belong to the tuning session.")
        if candidate.strategy in {"index", "combined"}:
            raise ValueError(
                "Index and combined candidates must use benchmark_index_candidate."
            )
        baseline = self.validator.validate_read_only(baseline_sql).execution_sql
        rewrite = self.validator.validate_read_only(candidate_sql).execution_sql
        case = self.store.get_performance_case(session.performance_case_id)
        if not fingerprint_text_matches(
            case.query_fingerprint,
            baseline,
            allow_legacy=self.allow_legacy_state,
        ):
            raise ValueError("Baseline SQL fingerprint does not match the performance case.")
        if not fingerprint_text_matches(
            candidate.rewrite_fingerprint,
            rewrite,
            allow_legacy=self.allow_legacy_state,
        ):
            raise ValueError("Candidate SQL fingerprint does not match the stored candidate.")
        if not database_fingerprint_matches(
            case.database_fingerprint or "",
            database_name,
            self.server_name,
            allow_legacy=self.allow_legacy_state,
        ):
            raise ValueError("Database fingerprint does not match the tuning session.")
        cases = self._normalize_parameter_cases(parameter_cases)
        if len(cases) > session.parameter_case_limit:
            raise ValueError(
                f"parameter_cases exceeds the session limit of {session.parameter_case_limit}."
            )
        supplied_case_fingerprints = tuple(
            parameter_case_fingerprint(parameter_case)
            for parameter_case in cases
        )
        registered_case_fingerprints = set(case.parameter_case_fingerprints)
        if (
            len(set(supplied_case_fingerprints)) != len(supplied_case_fingerprints)
            or any(
                fingerprint not in registered_case_fingerprints
                for fingerprint in supplied_case_fingerprints
            )
        ):
            raise ValueError(
                "Benchmark parameter cases must be an unchanged subset of the performance case."
            )
        if phase not in {"screening", "finalist"}:
            raise ValueError("phase must be screening or finalist.")
        should_prove_equivalence = (
            phase == "finalist" if prove_equivalence is None else prove_equivalence
        )
        equivalence_preflight = {
            "baseline": analyze_equivalence_preflight(baseline).as_dict(),
            "candidate": analyze_equivalence_preflight(rewrite).as_dict(),
        }
        direct_snapshot_supported = all(
            item["direct_snapshot_supported"]
            for item in equivalence_preflight.values()
        )
        if phase == "finalist" and not should_prove_equivalence:
            raise ValueError("Finalist validation requires full result equivalence.")
        if should_prove_equivalence and not direct_snapshot_supported:
            return {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "classification": "proof_contract_required",
                "durable_state": candidate.state,
                "reason": (
                    "This MCP contract has no deterministic proof input for this "
                    "SQL shape; finalist validation cannot proceed."
                ),
                "phase": phase,
                "executions": 0,
                "equivalence": [],
                "equivalence_preflight": equivalence_preflight,
                "proof_scope": "performance_only",
                "session_continues": True,
                "candidate": candidate.to_dict(),
            }
        if phase == "finalist" and set(supplied_case_fingerprints) != (
            registered_case_fingerprints
        ):
            raise ValueError(
                "Finalist validation must cover every registered parameter case."
            )
        default_runs = (
            session.screen_runs_per_candidate
            if phase == "screening"
            else session.finalist_runs_per_candidate
        )
        runs = runs_override if runs_override is not None else default_runs
        if not 1 <= runs <= default_runs:
            raise ValueError(
                f"runs must be between 1 and the {phase} limit of {default_runs}."
            )
        if phase == "screening" and runs < 2:
            raise ValueError("Screening requires at least two paired runs.")
        requested_executions = len(cases) * (
            runs * 2 + (2 if should_prove_equivalence else 0)
        )
        self._require_benchmark_policy(database_name, requested_executions)
        benchmark_request_fingerprint = request_fingerprint(
            "tuning-candidate.benchmark",
            {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "phase": phase,
                "baseline_fingerprint": fingerprint_text(baseline),
                "rewrite_fingerprint": fingerprint_text(rewrite),
                "parameter_case_fingerprints": supplied_case_fingerprints,
                "compare_order": compare_order,
                "runs": runs,
                "prove_equivalence": should_prove_equivalence,
                "equivalence_preflight": equivalence_preflight,
                "idempotency_key": idempotency_key or new_id("benchmark-call"),
            },
        )
        benchmark_operation_key = (
            request_fingerprint(
                "tuning-candidate.benchmark.idempotency-key",
                {"key": idempotency_key},
            )
            if idempotency_key
            else None
        )
        evidence_operation_key = (
            request_fingerprint(
                "tuning-candidate.evidence.idempotency-key",
                {"key": idempotency_key},
            )
            if idempotency_key
            else None
        )
        reservation_owner = f"benchmark-owner-{benchmark_request_fingerprint}"
        reservation = self.store.get_idempotent_execution_reservation(
            session_id,
            candidate_id,
            benchmark_request_fingerprint,
            owner_reference=reservation_owner,
            idempotency_key=idempotency_key,
        )
        if reservation is not None:
            return self._recover_benchmark_result(
                session_id=session_id,
                candidate_id=candidate_id,
                phase=phase,
                runs=runs,
                parameter_case_count=len(cases),
                reservation=reservation,
                reservation_owner=reservation_owner,
                benchmark_request_fingerprint=benchmark_request_fingerprint,
                evidence_operation_key=evidence_operation_key,
                benchmark_operation_key=benchmark_operation_key,
            )
        if candidate.is_terminal:
            raise ValueError("Candidate already has a terminal benchmark result.")
        if phase == "screening" and candidate.screen_runs:
            raise ValueError("Candidate screening has already been measured.")
        if phase == "finalist" and candidate.finalist_runs:
            raise ValueError("Candidate finalist validation has already been measured.")
        reservation = self.store.reserve_execution_attempts(
            session_id,
            candidate_id,
            requested_executions,
            benchmark_request_fingerprint,
            owner_reference=reservation_owner,
            idempotency_key=idempotency_key,
            max_runtime_seconds=self._maximum_benchmark_runtime_seconds(
                requested_executions
            ),
        )
        if reservation["status"] != "reserved" or reservation.get("replayed") is True:
            return self._recover_benchmark_result(
                session_id=session_id,
                candidate_id=candidate_id,
                phase=phase,
                runs=runs,
                parameter_case_count=len(cases),
                reservation=reservation,
                reservation_owner=reservation_owner,
                benchmark_request_fingerprint=benchmark_request_fingerprint,
                evidence_operation_key=evidence_operation_key,
                benchmark_operation_key=benchmark_operation_key,
            )

        try:
            if phase == "screening":
                self.sessions.start_screening(session_id)
            else:
                self.sessions.mark_candidate_finalist(session_id, candidate_id)
        except Exception:
            self.store.release_execution_attempts(
                reservation["reservation_id"],
                owner_reference=reservation_owner,
                expected_version=reservation["version"],
            )
            raise

        measured_executions = 0
        completed_parameter_cases = 0
        parameter_results: list[dict[str, Any]] = []
        equivalence: list[dict[str, Any]] = []
        objective = normalize_tuning_objective(case.metadata.get("objective"))
        try:
            for parameter_case in cases:
                case_baseline, case_rewrite = await self._bind_comparison_case(
                    database_name,
                    baseline,
                    rewrite,
                    parameter_case,
                )
                baseline_samples: list[dict[str, Any]] = []
                rewrite_samples: list[dict[str, Any]] = []
                baseline_plan: Mapping[str, Any] = {}
                rewrite_plan: Mapping[str, Any] = {}
                for run_number in range(runs):
                    side_order = (
                        (("baseline", case_baseline), ("candidate", case_rewrite))
                        if run_number % 2 == 0
                        else (("candidate", case_rewrite), ("baseline", case_baseline))
                    )
                    for side, execution_contract in side_order:
                        self.sessions.ensure_dispatch_allowed(session_id)
                        # Charge the attempt before awaiting it. A timeout may
                        # occur after SQL Server executed the query, and the
                        # hard budget must never under-count uncertain work.
                        measured_executions += 1
                        profiled = await self._profile_execution_contract(
                            database_name,
                            execution_contract,
                        )
                        if profiled.user_query_executions != 1:
                            raise RuntimeError(
                                "Profiled samples must execute the user query exactly once."
                            )
                        sample = extract_profile_metrics(profiled)
                        if side == "baseline":
                            baseline_samples.append(sample)
                            baseline_plan = profiled.plan.summary
                        else:
                            rewrite_samples.append(sample)
                            rewrite_plan = profiled.plan.summary
                parameter_results.append(
                    {
                        "parameter_case": parameter_case["name"],
                        "weight": parameter_case["weight"],
                        "baseline": aggregate_samples(baseline_samples),
                        "candidate": aggregate_samples(rewrite_samples),
                        "plan_delta": compare_plan_summaries_payload(
                            {"summary": baseline_plan},
                            {"summary": rewrite_plan},
                        ),
                    }
                )
                if should_prove_equivalence:
                    self.sessions.ensure_dispatch_allowed(session_id)
                    measured_executions += 2
                    comparison = await self._compare_execution_contracts(
                        database_name,
                        case_baseline,
                        case_rewrite,
                        compare_order=compare_order,
                        before_dispatch=lambda: self.sessions.ensure_dispatch_allowed(
                            session_id
                        ),
                    )
                    comparison_executions = int(comparison.get("executions", 0))
                    if comparison_executions not in {0, 1, 2}:
                        raise RuntimeError(
                            "Snapshot comparisons must report zero, one, or two "
                            "query executions."
                        )
                    # Two attempts are charged before awaiting the session, then
                    # reconciled to the synchronous worker-thread dispatch hook.
                    measured_executions -= 2 - comparison_executions
                    comparison["parameter_case"] = parameter_case["name"]
                    equivalence.append(comparison)
                completed_parameter_cases += 1
            state, reason = classify_benchmark(
                parameter_results,
                equivalence,
                objective=objective,
                require_equivalence=should_prove_equivalence,
                require_snapshot_attestation=should_prove_equivalence,
            )
        except asyncio.CancelledError:
            self._persist_benchmark_failure_receipt(
                session_id=session_id,
                candidate_id=candidate_id,
                phase=phase,
                runs=runs,
                cases=cases,
                case=case,
                parameter_results=parameter_results,
                equivalence=equivalence,
                completed_parameter_cases=completed_parameter_cases,
                measured_executions=measured_executions,
                failure_code="timeout",
                reservation=reservation,
                reservation_owner=reservation_owner,
                benchmark_request_fingerprint=benchmark_request_fingerprint,
                evidence_operation_key=evidence_operation_key,
                benchmark_operation_key=benchmark_operation_key,
                objective=objective,
                should_prove_equivalence=should_prove_equivalence,
            )
            raise
        except Exception as exc:
            return self._persist_benchmark_failure_receipt(
                session_id=session_id,
                candidate_id=candidate_id,
                phase=phase,
                runs=runs,
                cases=cases,
                case=case,
                parameter_results=parameter_results,
                equivalence=equivalence,
                completed_parameter_cases=completed_parameter_cases,
                measured_executions=measured_executions,
                failure_code=type(exc).__name__,
                reservation=reservation,
                reservation_owner=reservation_owner,
                benchmark_request_fingerprint=benchmark_request_fingerprint,
                evidence_operation_key=evidence_operation_key,
                benchmark_operation_key=benchmark_operation_key,
                objective=objective,
                should_prove_equivalence=should_prove_equivalence,
            )

        performance_classification = state
        performance_reason = reason
        candidate_state = state
        if (
            phase == "screening"
            and not direct_snapshot_supported
            and state in {"promising", "improved"}
        ):
            state = "proof_contract_required"
            candidate_state = "inconclusive"
            reason = (
                "Performance screening improved, but this MCP contract has no "
                "deterministic proof input for this SQL shape; the candidate was "
                "not promoted."
            )

        envelope = EvidenceEnvelopeV1(
            source="azure-sql-mcp",
            kind=f"tuning_{phase}",
            query_fingerprint=case.query_fingerprint,
            database_fingerprint=case.database_fingerprint,
            parameters_fingerprint=fingerprint_json(
                [
                    {
                        "name": parameter_case["name"],
                        "values_fingerprint": fingerprint_json(parameter_case["values"]),
                        "types_fingerprint": fingerprint_json(parameter_case["types"]),
                        "weight": parameter_case["weight"],
                    }
                    for parameter_case in cases
                ]
            ),
            observed_execution_count=measured_executions,
            metrics={
                "classification": state,
                "performance_classification": performance_classification,
                "objective": objective,
                "parameter_results": parameter_results,
            },
            metadata={
                "session_id": session_id,
                "candidate_id": candidate_id,
                "equivalence": equivalence,
                "equivalence_deferred": not should_prove_equivalence,
                "equivalence_preflight": equivalence_preflight,
                "proof_scope": (
                    "performance_only"
                    if not direct_snapshot_supported
                    else "direct_snapshot"
                ),
                "phase": phase,
                "reason": reason,
                "performance_reason": performance_reason,
                "execution_reservation_id": reservation["reservation_id"],
                "sql_persisted": False,
            },
        )
        envelope = self.store.create_evidence(
            envelope,
            idempotency_key=evidence_operation_key,
            request_fingerprint=benchmark_request_fingerprint,
        )

        if phase == "screening" and candidate_state in {"promising", "improved"}:
            _session, updated_candidate = self.sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="screening",
                screen_runs=runs,
                parameter_cases=len(cases),
                executions=measured_executions,
                evidence_ids=(envelope.evidence_id,),
                idempotency_key=benchmark_operation_key,
            )
            durable_state = "screening"
        else:
            _session, updated_candidate = self.sessions.record_candidate_result(
                session_id,
                candidate_id,
                state=candidate_state,
                screen_runs=runs if phase == "screening" else 0,
                finalist_runs=runs if phase == "finalist" else 0,
                parameter_cases=len(cases),
                executions=measured_executions,
                evidence_ids=(envelope.evidence_id,),
                failure_code=(
                    None
                    if candidate_state in {"improved", "neutral", "regressed"}
                    else state
                ),
                idempotency_key=benchmark_operation_key,
            )
            durable_state = updated_candidate.state

        self.store.complete_execution_attempts(
            reservation["reservation_id"],
            dispatched_attempt_count=measured_executions,
            owner_reference=reservation_owner,
            expected_version=reservation["version"],
        )

        return {
            "session_id": session_id,
            "candidate_id": candidate_id,
            "classification": state,
            "performance_classification": performance_classification,
            "objective": objective,
            "durable_state": durable_state,
            "reason": reason,
            "phase": phase,
            "runs_per_parameter_case": runs,
            "executions": measured_executions,
            "parameter_results": parameter_results,
            "equivalence": equivalence,
            "equivalence_deferred": not should_prove_equivalence,
            "equivalence_preflight": equivalence_preflight,
            "proof_scope": (
                "performance_only"
                if not direct_snapshot_supported
                else "direct_snapshot"
            ),
            "evidence_id": envelope.evidence_id,
            "execution_reservation_id": reservation["reservation_id"],
            "session_continues": True,
            "candidate": updated_candidate.to_dict(),
        }

    def finalize_session(
        self,
        session_id: str,
        *,
        selected_candidate_id: str | None,
        stopping_reason: str,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        if not stopping_reason.strip():
            raise ValueError("stopping_reason is required.")
        candidates = self.sessions.list_candidates(session_id)
        if selected_candidate_id is not None:
            selected = next(
                (
                    candidate
                    for candidate in candidates
                    if candidate.candidate_id == selected_candidate_id
                ),
                None,
            )
            if selected is None:
                raise ValueError("Selected candidate does not belong to the tuning session.")
            if selected.state != "improved":
                raise ValueError("Only a finalist classified as improved may be selected.")
        current_session = self.sessions.get_session(session_id)
        if candidates and current_session.status == "created":
            self.sessions.start_screening(session_id)
        for candidate in candidates:
            if not candidate.is_terminal:
                self.sessions.mark_candidate_terminal(
                    session_id,
                    candidate.candidate_id,
                    "inconclusive",
                    failure_code="session_finalized_unresolved",
                    idempotency_key=(
                        f"{idempotency_key}:{candidate.candidate_id}:finalize"
                        if idempotency_key
                        else None
                    ),
                )
        session = self.sessions.complete_session(
            session_id,
            selected_candidate_id=selected_candidate_id,
            stopping_reason=stopping_reason.strip(),
            idempotency_key=idempotency_key,
        )
        candidates = self.sessions.list_candidates(session_id)
        return {
            "session": session.to_dict(),
            "leaderboard": [candidate.to_dict() for candidate in candidates],
            "selected_candidate_id": selected_candidate_id,
            "stopping_reason": stopping_reason,
            "rejected_experiments": [
                candidate.to_dict()
                for candidate in candidates
                if candidate.candidate_id != selected_candidate_id
            ],
        }

    def _require_benchmark_policy(self, database_name: str, executions: int) -> None:
        policy = self.database_policy.policy_for(database_name)
        if not policy.can_benchmark(executions):
            raise PermissionError(
                "Database policy does not permit this benchmark execution count."
            )

    def _maximum_benchmark_runtime_seconds(self, requested_executions: int) -> float:
        """Return the reservation horizon for this request's worst-case runtime.

        The server wires ``collector_timeout_seconds`` to the configured query
        timeout plus a small driver margin. Using it per possible execution
        keeps durable reservations live for authorized multi-hour campaigns.
        """

        per_execution = max(float(self.collector_timeout_seconds), 1.0)
        return requested_executions * per_execution + per_execution + 60.0

    @staticmethod
    def _normalize_parameter_cases(
        parameter_cases: Sequence[ParameterCase] | None,
    ) -> list[ParameterCase]:
        if not parameter_cases:
            return [{"name": "default", "values": {}, "types": {}, "weight": 1.0}]
        normalized: list[ParameterCase] = []
        seen: set[str] = set()
        for ordinal, raw_case in enumerate(parameter_cases, start=1):
            if not isinstance(raw_case, Mapping):
                raise ValueError("Each parameter case must be an object.")
            name = str(raw_case.get("name") or f"case-{ordinal}").strip()
            values = raw_case.get("values", {})
            types = raw_case.get("types", {})
            raw_weight = raw_case.get("weight", 1.0)
            if not name or name.casefold() in seen:
                raise ValueError("Parameter case names must be unique and non-empty.")
            if not isinstance(values, Mapping):
                raise ValueError("Parameter case values must be an object.")
            if not isinstance(types, Mapping):
                raise ValueError("Parameter case types must be an object.")
            if (
                not isinstance(raw_weight, (int, float))
                or isinstance(raw_weight, bool)
                or float(raw_weight) <= 0
            ):
                raise ValueError("Parameter case weight must be a positive number.")
            seen.add(name.casefold())
            normalized.append(
                {
                    "name": name,
                    "values": dict(values),
                    "types": {str(key): str(value) for key, value in types.items()},
                    "weight": float(raw_weight),
                }
            )
        if len(normalized) > 4:
            raise ValueError("At most four parameter cases are supported.")
        return normalized

    async def _bind_case(
        self,
        database_name: str,
        sql: str,
        parameter_case: ParameterCase,
    ) -> ParameterExecutionContract:
        values = parameter_case.get("values", {})
        types = parameter_case.get("types", {})
        if self.parameter_binder is not None:
            return await self.parameter_binder(database_name, sql, parameter_case)
        if not values and not types:
            return ParameterExecutionContract(
                sql_text=sql,
                bucket_id=str(parameter_case["name"]),
                parameters=(),
                provenance="unparameterized",
            )
        raise ValueError("Parameterized benchmark cases require a parameter binder.")

    async def _bind_comparison_case(
        self,
        database_name: str,
        baseline_sql: str,
        candidate_sql: str,
        parameter_case: ParameterCase,
    ) -> tuple[ParameterExecutionContract, ParameterExecutionContract]:
        """Bind both sides from one canonical typed parameter bucket."""

        seed_contract = await self._bind_case(
            database_name,
            baseline_sql,
            parameter_case,
        )
        bucket = TypedParameterBucket(
            bucket_id=seed_contract.bucket_id,
            parameters=seed_contract.parameters,
            provenance=seed_contract.provenance,
        )
        return self._contracts_from_bucket(baseline_sql, candidate_sql, bucket)

    @staticmethod
    def _contracts_from_bucket(
        baseline_sql: str,
        candidate_sql: str,
        bucket: TypedParameterBucket,
    ) -> tuple[ParameterExecutionContract, ParameterExecutionContract]:
        return (
            bucket.for_sql(baseline_sql, provenance="baseline"),
            bucket.for_sql(candidate_sql, provenance="candidate"),
        )

    async def _profile_execution_contract(
        self,
        database_name: str,
        contract: ParameterExecutionContract,
        *,
        max_result_rows: int | None = None,
    ) -> ProfiledPlanResult:
        if contract.parameters:
            if max_result_rows is None:
                return await self.plans.profile_parameterized_query(
                    database_name,
                    contract,
                )
            return await self.plans.profile_parameterized_query(
                database_name,
                contract,
                max_result_rows=max_result_rows,
            )
        if max_result_rows is None:
            return await self.plans.profile_query(
                database_name,
                contract.sql_text,
            )
        return await self.plans.profile_query(
            database_name,
            contract.sql_text,
            max_result_rows=max_result_rows,
        )

    @staticmethod
    def _has_actionable_evidence(sections: Mapping[str, Any]) -> bool:
        def actionable(value: Any) -> bool:
            if isinstance(value, (list, tuple)):
                return any(actionable(item) for item in value)
            if not isinstance(value, Mapping):
                return False
            for key in (
                "recommendation_count",
                "affected_query_count",
                "blocking_session_count",
                "stale_statistics_count",
                "stale_count",
                "high_modification_count",
                "regressed_query_count",
                "waiting_locks",
            ):
                count = value.get(key)
                if isinstance(count, (int, float)) and count > 0:
                    return True
            status = value.get("status")
            if isinstance(status, str) and status.casefold() in {
                "critical",
                "warning",
                "actionable",
            }:
                return True
            warnings = value.get("warnings")
            if isinstance(warnings, list) and warnings:
                return True
            recommendations = value.get("recommendations")
            if isinstance(recommendations, list) and recommendations:
                return True
            findings = value.get("findings")
            if isinstance(findings, list) and findings:
                return True
            if value.get("enabled") is False:
                return True
            return any(actionable(nested) for nested in value.values())

        return any(
            actionable(section.get("data"))
            for section in sections.values()
            if isinstance(section, Mapping)
        )

    def _record_interrupted_candidate(
        self,
        session_id: str,
        candidate_id: str,
        executions: int,
        parameter_cases: int,
        phase: str,
        failure_code: str,
    ) -> bool:
        try:
            self.sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="inconclusive",
                screen_runs=0,
                finalist_runs=0,
                parameter_cases=parameter_cases,
                executions=executions,
                failure_code=failure_code[:100].replace(" ", "_"),
            )
        except Exception:
            # The original failure remains primary; an already-terminal or
            # exhausted candidate must not mask it.
            return False
        return True


__all__ = [
    "PerformanceWorkflowService",
    "aggregate_samples",
    "classify_benchmark",
    "compare_plan_summaries_payload",
    "compare_result_collections",
    "compare_result_sets",
    "database_fingerprint",
    "database_fingerprint_matches",
    "extract_profile_metrics",
    "fingerprint_json",
    "fingerprint_text",
    "fingerprint_text_matches",
    "normalize_tuning_objective",
    "profile_payload",
]
