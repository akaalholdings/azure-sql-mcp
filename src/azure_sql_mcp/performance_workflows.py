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
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Mapping, Sequence

from .connection import AzureSqlExecutor, QueryResult
from .database_policy import DatabasePolicySet
from .performance_contracts import (
    EvidenceEnvelopeV1,
    PerformanceCaseV1,
    utc_now,
)
from .performance_store import PerformanceStore
from .plans import PlansService, ProfiledPlanResult
from .safe_sql import SafeSqlValidator
from .tuning_sessions import TuningSessionStateMachine


ParameterCase = dict[str, Any]
EvidenceCollector = Callable[[], Awaitable[Any]]
ParameterBinder = Callable[[str, str, Mapping[str, Any]], Awaitable[str]]
OBJECTIVE_METRICS = {
    "elapsed_time": "elapsed_ms",
    "cpu": "cpu_ms",
    "logical_reads": "logical_reads",
    "physical_reads": "physical_reads",
}


def fingerprint_text(value: str) -> str:
    normalized = " ".join(value.split()).casefold()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def fingerprint_json(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def database_fingerprint(database_name: str) -> str:
    return fingerprint_text(f"database:{database_name}")


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


def _canonical_row(row: Mapping[str, Any]) -> str:
    typed = {
        key: {
            "python_type": f"{type(value).__module__}.{type(value).__qualname__}",
            "value": value,
        }
        for key, value in row.items()
    }
    return json.dumps(typed, sort_keys=True, separators=(",", ":"), default=str)


def _first_tabular_result(results: Sequence[QueryResult]) -> QueryResult | None:
    return next((result for result in results if result.columns), None)


def _bounded_result(result: QueryResult | None, row_limit: int) -> tuple[list[dict[str, Any]], bool]:
    if result is None:
        return [], False
    return result.rows[:row_limit], len(result.rows) > row_limit


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
    baseline_rows, baseline_truncated = _bounded_result(baseline, row_limit)
    candidate_rows, candidate_truncated = _bounded_result(candidate, row_limit)
    columns_match = baseline.columns == candidate.columns
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
    if not columns_match or not rows_match or types_match is False:
        status = "mismatch"
        reason = "result shape, types, duplicates, values, or required order differ"
        proven = False
    elif not same_snapshot or truncated or not type_metadata_available:
        status = "inconclusive"
        if truncated:
            reason = "comparison exceeded the bounded full-result limit"
        elif not same_snapshot:
            reason = "queries were not compared in one snapshot"
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
        "types_match": types_match,
        "rows_match": rows_match,
        "baseline_row_count": len(baseline_rows),
        "candidate_row_count": len(candidate_rows),
        "truncated": truncated,
    }


def profile_payload(profile: ProfiledPlanResult, *, include_plan_xml: bool = False) -> dict[str, Any]:
    plan = profile.plan.as_dict(include_raw_xml=include_plan_xml)
    result = _first_tabular_result(profile.result_sets)
    rows = result.rows if result is not None else []
    return {
        "plan": plan,
        "result_shape": list(result.columns) if result is not None else [],
        "result_type_signatures": (
            list(result.column_type_signatures) if result is not None else []
        ),
        "result_sample": rows,
        "row_count": len(rows),
        "truncated": profile.truncated,
        "user_query_executions": profile.user_query_executions,
        "metric_provenance": profile.metric_provenance,
        "metrics": extract_profile_metrics(profile),
    }


def extract_profile_metrics(profile: ProfiledPlanResult) -> dict[str, Any]:
    metrics = profile.plan.summary.get("actual_metrics", {})
    return {
        "elapsed_ms": float(profile.elapsed_wall_ms),
        "cpu_ms": metrics.get("actual_cpu_ms"),
        "actual_rows": metrics.get("actual_rows"),
        "logical_reads": metrics.get("actual_logical_reads"),
        "physical_reads": metrics.get("actual_physical_reads"),
        "warning_count": len(profile.plan.summary.get("warnings", [])),
        "plan_fingerprint": fingerprint_json(profile.plan.summary),
        "elapsed_source": "client_wall_clock",
        "cpu_source": metrics.get("query_metric_source"),
        "read_source": metrics.get("read_metric_source"),
    }


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
    for name in ("elapsed_ms", "cpu_ms", "actual_rows", "logical_reads", "physical_reads"):
        values = _numeric([sample.get(name) for sample in samples])
        if not values:
            aggregate[name] = None
            continue
        median = float(statistics.median(values))
        aggregate[name] = median
        spread[name] = {
            "min": min(values),
            "max": max(values),
            "range": max(values) - min(values),
        }
    elapsed = aggregate.get("elapsed_ms")
    elapsed_range = spread.get("elapsed_ms", {}).get("range")
    aggregate["noise_ratio"] = (
        float(elapsed_range) / float(elapsed)
        if isinstance(elapsed, (int, float)) and elapsed > 0 and elapsed_range is not None
        else None
    )
    aggregate["spread"] = spread
    aggregate["plan_fingerprints"] = sorted(
        {
            str(sample["plan_fingerprint"])
            for sample in samples
            if sample.get("plan_fingerprint")
        }
    )
    return aggregate


def classify_benchmark(
    parameter_results: Sequence[Mapping[str, Any]],
    equivalence: Sequence[Mapping[str, Any]],
    *,
    objective: str = "elapsed_time",
) -> tuple[str, str]:
    if any(result.get("status") == "mismatch" for result in equivalence):
        return "equivalence_failed", "at least one parameter case returned different results"
    if any(result.get("status") != "match" for result in equivalence):
        return "inconclusive", "full snapshot equivalence was not proven for every parameter case"

    objective = normalize_tuning_objective(objective)
    metric_name = OBJECTIVE_METRICS[objective]
    improvements: list[float] = []
    noise_floor = 0.05

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
        before = baseline.get(metric_name)
        after = candidate.get(metric_name)
        if not isinstance(before, (int, float)) or before <= 0 or not isinstance(after, (int, float)):
            return "inconclusive", f"{objective} was unavailable for at least one parameter case"
        improvements.append((float(before) - float(after)) / float(before))
        noise_floor = max(
            noise_floor,
            metric_noise_ratio(baseline),
            metric_noise_ratio(candidate),
        )
    if not improvements:
        return "inconclusive", "no parameter cases were measured"
    regression_tolerance = max(0.10, noise_floor)
    if any(improvement < -regression_tolerance for improvement in improvements):
        return "regressed", "candidate materially regressed at least one tested parameter bucket"
    median_improvement = statistics.median(improvements)
    if median_improvement > noise_floor:
        return "improved", "candidate improved the median objective without bucket regression"
    if median_improvement < -noise_floor:
        return "regressed", "candidate regressed beyond observed timing noise"
    return "neutral", "candidate did not beat observed timing noise"


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
    ) -> None:
        self.executor = executor
        self.plans = plans
        self.validator = validator
        self.store = store
        self.sessions = sessions
        self.database_policy = database_policy
        self.row_limit = row_limit
        self.parameter_binder = parameter_binder

    def start_case(
        self,
        database_name: str,
        sql: str,
        *,
        parameter_cases: Sequence[ParameterCase] | None = None,
        metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> PerformanceCaseV1:
        normalized = self.validator.validate_read_only(sql).normalized_sql
        cases = self._normalize_parameter_cases(parameter_cases)
        normalized_metadata = dict(metadata or {})
        normalized_metadata["objective"] = normalize_tuning_objective(
            normalized_metadata.get("objective")
        )
        case = PerformanceCaseV1(
            query_fingerprint=fingerprint_text(normalized),
            database_fingerprint=database_fingerprint(database_name),
            parameter_case_fingerprints=tuple(
                fingerprint_json(case["values"]) for case in cases
            ),
            metadata={
                "parameter_case_names": [case["name"] for case in cases],
                **normalized_metadata,
            },
        )
        return self.store.create_performance_case(case, idempotency_key=idempotency_key)

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
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        case = self.store.get_performance_case(case_id)
        normalized = self.validator.validate_read_only(sql).normalized_sql
        if fingerprint_text(normalized) != case.query_fingerprint:
            raise ValueError("SQL fingerprint does not match the performance case.")
        if database_fingerprint(database_name) != case.database_fingerprint:
            raise ValueError("Database fingerprint does not match the performance case.")
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")

        window_end = datetime.now(timezone.utc)
        window_start = window_end - timedelta(minutes=window_minutes)
        started = utc_now()
        sections: dict[str, Any] = {}
        for name, collector in collectors.items():
            captured_at = utc_now()
            try:
                data = await collector()
                gaps = _evidence_gaps(data)
                top_level_available = not (
                    isinstance(data, Mapping) and data.get("available") is False
                )
                sections[name] = {
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
                sections[name] = {
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

        profile: dict[str, Any] | None = None
        observed_executions = 0
        if execute_query:
            self._require_benchmark_policy(database_name, 1)
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
        evidence = self.store.create_evidence(evidence, idempotency_key=idempotency_key)
        updated = replace(
            case,
            updated_at_utc=utc_now(),
            baseline_evidence_ids=tuple(
                dict.fromkeys((*case.baseline_evidence_ids, evidence.evidence_id))
            ),
            status="ready" if outcome in {"healthy", "actionable"} else "open",
        )
        self.store.save_performance_case(updated)
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
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        case = self.store.get_performance_case(case_id)
        session = self.sessions.create_session(
            case,
            metadata={"database_fingerprint": case.database_fingerprint},
            idempotency_key=idempotency_key,
        )
        return session.to_dict()

    def add_candidate(
        self,
        session_id: str,
        candidate_sql: str,
        *,
        strategy: str,
        artifact_ref: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        normalized = self.validator.validate_read_only(candidate_sql).normalized_sql
        candidate = self.sessions.add_candidate(
            session_id,
            strategy=strategy,
            rewrite_fingerprint=fingerprint_text(normalized),
            rewrite_artifact_ref=artifact_ref,
            metadata={"sql_persisted": False},
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
    ) -> dict[str, Any]:
        baseline = self.validator.validate_read_only(baseline_sql).normalized_sql
        candidate = self.validator.validate_read_only(candidate_sql).normalized_sql
        self._require_benchmark_policy(database_name, 2)
        try:
            per_statement = await self.executor.execute_session_exactly_once(
                database_name,
                [
                    "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
                    "BEGIN TRANSACTION",
                    baseline,
                    candidate,
                    "ROLLBACK TRANSACTION",
                    "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
                ],
                max_rows=self.row_limit + 1,
            )
        except Exception as exc:
            return {
                "status": "inconclusive",
                "reason": "snapshot comparison could not complete",
                "error_type": type(exc).__name__,
                "same_snapshot": False,
                "proven_for_parameter_case": False,
                # The exact statement boundary is unknowable after a driver
                # error, so charge the full reserved pair to the hard budget.
                "executions": 2,
                "execution_count_is_conservative": True,
            }
        baseline_results = per_statement[2] if len(per_statement) > 2 else []
        candidate_results = per_statement[3] if len(per_statement) > 3 else []
        comparison = compare_result_sets(
            _first_tabular_result(baseline_results),
            _first_tabular_result(candidate_results),
            row_limit=self.row_limit,
            compare_order=compare_order,
            same_snapshot=True,
        )
        comparison["executions"] = 2
        return comparison

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
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        session = self.sessions.get_session(session_id)
        candidate = self.sessions.get_candidate(candidate_id)
        if candidate.session_id != session_id:
            raise ValueError("Candidate does not belong to the tuning session.")
        if candidate.is_terminal:
            raise ValueError("Candidate already has a terminal benchmark result.")
        baseline = self.validator.validate_read_only(baseline_sql).normalized_sql
        rewrite = self.validator.validate_read_only(candidate_sql).normalized_sql
        case = self.store.get_performance_case(session.performance_case_id)
        if fingerprint_text(baseline) != case.query_fingerprint:
            raise ValueError("Baseline SQL fingerprint does not match the performance case.")
        if fingerprint_text(rewrite) != candidate.rewrite_fingerprint:
            raise ValueError("Candidate SQL fingerprint does not match the stored candidate.")
        if database_fingerprint(database_name) != case.database_fingerprint:
            raise ValueError("Database fingerprint does not match the tuning session.")
        cases = self._normalize_parameter_cases(parameter_cases)
        if len(cases) > session.parameter_case_limit:
            raise ValueError(
                f"parameter_cases exceeds the session limit of {session.parameter_case_limit}."
            )
        supplied_case_fingerprints = tuple(
            fingerprint_json(parameter_case["values"])
            for parameter_case in cases
        )
        if supplied_case_fingerprints != case.parameter_case_fingerprints:
            raise ValueError(
                "Benchmark parameter cases do not match the performance case."
            )
        if phase not in {"screening", "finalist"}:
            raise ValueError("phase must be screening or finalist.")
        if phase == "screening" and candidate.screen_runs:
            raise ValueError("Candidate screening has already been measured.")
        if phase == "finalist" and candidate.finalist_runs:
            raise ValueError("Candidate finalist validation has already been measured.")
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
        requested_executions = len(cases) * (runs * 2 + 2)
        self._require_benchmark_policy(database_name, requested_executions)
        already_executed = sum(
            item.executions for item in self.sessions.list_candidates(session_id)
        )
        if already_executed + requested_executions > session.execution_limit:
            raise ValueError("The tuning session execution budget would be exceeded.")

        if phase == "screening":
            self.sessions.start_screening(session_id)
        else:
            self.sessions.mark_candidate_finalist(session_id, candidate_id)

        measured_executions = 0
        try:
            parameter_results: list[dict[str, Any]] = []
            equivalence: list[dict[str, Any]] = []
            for parameter_case in cases:
                case_baseline = await self._bind_case(
                    database_name,
                    baseline,
                    parameter_case,
                )
                case_rewrite = await self._bind_case(
                    database_name,
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
                    for side, sql in side_order:
                        # Charge the attempt before awaiting it. A timeout may
                        # occur after SQL Server executed the query, and the
                        # hard budget must never under-count uncertain work.
                        measured_executions += 1
                        profiled = await self.plans.profile_query(database_name, sql)
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
                        "baseline": aggregate_samples(baseline_samples),
                        "candidate": aggregate_samples(rewrite_samples),
                        "plan_delta": compare_plan_summaries_payload(
                            {"summary": baseline_plan},
                            {"summary": rewrite_plan},
                        ),
                    }
                )
                measured_executions += 2
                comparison = await self.compare_query_results(
                    database_name,
                    case_baseline,
                    case_rewrite,
                    compare_order=compare_order,
                )
                if int(comparison.get("executions", 0)) != 2:
                    raise RuntimeError(
                        "Snapshot comparisons must reserve exactly two query executions."
                    )
                comparison["parameter_case"] = parameter_case["name"]
                equivalence.append(comparison)
            objective = normalize_tuning_objective(case.metadata.get("objective"))
            state, reason = classify_benchmark(
                parameter_results,
                equivalence,
                objective=objective,
            )
        except asyncio.CancelledError:
            self._record_interrupted_candidate(
                session_id,
                candidate_id,
                measured_executions,
                len(cases),
                phase,
                "timeout",
            )
            raise
        except Exception as exc:
            self._record_interrupted_candidate(
                session_id,
                candidate_id,
                measured_executions,
                len(cases),
                phase,
                type(exc).__name__.casefold(),
            )
            return {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "state": "inconclusive",
                "reason": "candidate benchmark failed; continue with the next candidate",
                "failure_code": type(exc).__name__,
                "executions": measured_executions,
                "session_continues": True,
            }

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
                    }
                    for parameter_case in cases
                ]
            ),
            observed_execution_count=measured_executions,
            metrics={
                "classification": state,
                "objective": objective,
                "parameter_results": parameter_results,
            },
            metadata={
                "session_id": session_id,
                "candidate_id": candidate_id,
                "equivalence": equivalence,
                "phase": phase,
                "sql_persisted": False,
            },
        )
        envelope = self.store.create_evidence(
            envelope,
            idempotency_key=(f"{idempotency_key}:evidence" if idempotency_key else None),
        )

        if phase == "screening" and state == "improved":
            _session, updated_candidate = self.sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="screening",
                screen_runs=runs,
                parameter_cases=len(cases),
                executions=measured_executions,
                evidence_ids=(envelope.evidence_id,),
                idempotency_key=idempotency_key,
            )
            durable_state = "screening"
        else:
            _session, updated_candidate = self.sessions.record_candidate_result(
                session_id,
                candidate_id,
                state=state,
                screen_runs=runs if phase == "screening" else 0,
                finalist_runs=runs if phase == "finalist" else 0,
                parameter_cases=len(cases),
                executions=measured_executions,
                evidence_ids=(envelope.evidence_id,),
                failure_code=None if state in {"improved", "neutral", "regressed"} else state,
                idempotency_key=idempotency_key,
            )
            durable_state = updated_candidate.state

        return {
            "session_id": session_id,
            "candidate_id": candidate_id,
            "classification": state,
            "objective": objective,
            "durable_state": durable_state,
            "reason": reason,
            "phase": phase,
            "runs_per_parameter_case": runs,
            "executions": measured_executions,
            "parameter_results": parameter_results,
            "equivalence": equivalence,
            "evidence_id": envelope.evidence_id,
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

    @staticmethod
    def _normalize_parameter_cases(
        parameter_cases: Sequence[ParameterCase] | None,
    ) -> list[ParameterCase]:
        if not parameter_cases:
            return [{"name": "default", "values": {}}]
        normalized: list[ParameterCase] = []
        seen: set[str] = set()
        for ordinal, raw_case in enumerate(parameter_cases, start=1):
            if not isinstance(raw_case, Mapping):
                raise ValueError("Each parameter case must be an object.")
            name = str(raw_case.get("name") or f"case-{ordinal}").strip()
            values = raw_case.get("values", {})
            if not name or name.casefold() in seen:
                raise ValueError("Parameter case names must be unique and non-empty.")
            if not isinstance(values, Mapping):
                raise ValueError("Parameter case values must be an object.")
            seen.add(name.casefold())
            normalized.append({"name": name, "values": dict(values)})
        return normalized

    async def _bind_case(
        self,
        database_name: str,
        sql: str,
        parameter_case: ParameterCase,
    ) -> str:
        values = parameter_case.get("values", {})
        if not values:
            return sql
        if self.parameter_binder is None:
            raise ValueError("Parameterized benchmark cases require a parameter binder.")
        return await self.parameter_binder(database_name, sql, values)

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
            if value.get("status") in {"critical", "warning", "actionable"}:
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
    ) -> None:
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
            return


__all__ = [
    "PerformanceWorkflowService",
    "aggregate_samples",
    "classify_benchmark",
    "compare_plan_summaries_payload",
    "compare_result_sets",
    "database_fingerprint",
    "extract_profile_metrics",
    "fingerprint_json",
    "fingerprint_text",
    "normalize_tuning_objective",
    "profile_payload",
]
