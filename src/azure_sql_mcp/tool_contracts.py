from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator


TuningObjective = Literal[
    "elapsed_time",
    "cpu",
    "logical_reads",
    "physical_reads",
]
TuningStrategy = Literal[
    "predicate",
    "join",
    "aggregation",
    "cardinality",
    "index",
    "combined",
    "rewrite_plus_index",
]
BenchmarkPhase = Literal["screening", "finalist"]
SelectionScope = Literal["proven", "performance_only"]


class ParameterCaseInput(BaseModel):
    """Typed public shape for one exact benchmark parameter case."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    values: dict[str, Any]
    types: dict[str, str]
    weight: float = Field(gt=0)

    @field_validator("name")
    @classmethod
    def reject_blank_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("name must not be blank")
        return value

    @field_validator("weight", mode="before")
    @classmethod
    def reject_boolean_weight(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("weight must be a positive number")
        return value


class _ExtensibleOutput(BaseModel):
    """Typed stable fields plus backward-compatible tool-specific payloads."""

    model_config = ConfigDict(extra="allow")


class CaseHeadline(BaseModel):
    case_id: str | None
    classification: str
    proof_scope: str


class SessionHeadline(BaseModel):
    session_id: str | None
    status: str
    time_limit_minutes: int | None
    executions_remaining: int | None
    candidate_slots_remaining: int | None
    deadline_exceeded: bool | None = None
    accepts_new_work: bool | None = None
    accepts_finalization: bool | None = None


class BenchmarkHeadline(BaseModel):
    session_id: str | None
    candidate_id: str | None
    classification: str
    objective: str | None
    metric: str | None
    relative_improvement_pct: float | None
    parameter_case_count: int
    executions: int
    proof_scope: str | None


class PlanHeadline(BaseModel):
    plan_kind: str | None
    statement_count: int
    operator_count: int
    warning_count: int
    missing_index_count: int


class PreflightHeadline(BaseModel):
    classification: str
    coverage_complete: bool
    direct_snapshot_supported: bool
    risk_count: int
    unresolved_dependency_count: int


class CaseToolOutput(_ExtensibleOutput):
    headline: CaseHeadline


class SessionToolOutput(_ExtensibleOutput):
    headline: SessionHeadline


class BenchmarkToolOutput(_ExtensibleOutput):
    headline: BenchmarkHeadline


class PlanToolOutput(_ExtensibleOutput):
    headline: PlanHeadline


class PreflightToolOutput(_ExtensibleOutput):
    headline: PreflightHeadline
    contract_version: int
    classification: str
    direct_snapshot_supported: bool
    coverage_complete: bool
    analysis_scope: str
    risk_codes: list[str]
    functions: list[dict[str, Any]]
    resolved_dependencies: list[dict[str, Any]]
    unresolved_dependencies: list[dict[str, Any]]


_OBJECTIVE_METRICS = {
    "elapsed_time": "elapsed_ms",
    "cpu": "cpu_ms",
    "logical_reads": "logical_reads",
    "physical_reads": "physical_reads",
}

_CASE_TOOLS = {
    "start_performance_case",
    "get_performance_case",
}
_SESSION_TOOLS = {
    "start_tuning_session",
    "get_tuning_session",
    "finalize_tuning_session",
}
_BENCHMARK_TOOLS = {
    "benchmark_query_rewrite",
    "benchmark_tuning_candidate",
    "benchmark_index_candidate",
}


def add_tool_headline(tool_name: str, payload: Any) -> Any:
    """Add a stable, shallow summary while retaining every existing response key."""

    if not isinstance(payload, dict):
        return payload
    result = dict(payload)
    if tool_name in _CASE_TOOLS:
        result["headline"] = _case_headline(result)
    elif tool_name in _SESSION_TOOLS:
        result["headline"] = _session_headline(result)
    elif tool_name in _BENCHMARK_TOOLS:
        result["headline"] = _benchmark_headline(result)
    elif tool_name == "explain_query":
        result["headline"] = _plan_headline(result)
    elif tool_name == "check_equivalence_preflight":
        result["headline"] = _preflight_headline(result)
    return result


def _case_headline(payload: Mapping[str, Any]) -> dict[str, Any]:
    case = _mapping(payload.get("case")) or payload
    metadata = _mapping(case.get("metadata"))
    preflight = _mapping(metadata.get("equivalence_preflight"))
    classification = str(
        preflight.get("classification")
        or payload.get("classification")
        or "unknown"
    )
    proof_scope = str(
        payload.get("proof_scope")
        or (
            "direct_snapshot"
            if preflight.get("direct_snapshot_supported") is True
            else "performance_only"
            if preflight
            else "unknown"
        )
    )
    return {
        "case_id": _optional_string(case.get("case_id") or payload.get("case_id")),
        "classification": classification,
        "proof_scope": proof_scope,
    }


def _session_headline(payload: Mapping[str, Any]) -> dict[str, Any]:
    session = _mapping(payload.get("session")) or payload
    budget = _mapping(payload.get("budget"))
    time_limit = session.get("time_limit_seconds")
    time_limit_minutes = (
        max(1, round(float(time_limit) / 60))
        if isinstance(time_limit, (int, float)) and not isinstance(time_limit, bool)
        else None
    )
    executions_remaining = _optional_int(budget.get("executions_remaining"))
    candidate_slots_remaining = _optional_int(
        budget.get("candidate_slots_remaining")
    )
    if candidate_slots_remaining is None:
        limit = _optional_int(session.get("max_candidates"))
        candidate_ids = session.get("candidate_ids")
        if limit is not None and isinstance(candidate_ids, (list, tuple)):
            candidate_slots_remaining = max(0, limit - len(candidate_ids))
    result = {
        "session_id": _optional_string(
            session.get("session_id") or payload.get("session_id")
        ),
        "status": str(
            session.get("effective_status")
            or session.get("status")
            or payload.get("effective_status")
            or payload.get("status")
            or "unknown"
        ),
        "time_limit_minutes": time_limit_minutes,
        "executions_remaining": executions_remaining,
        "candidate_slots_remaining": candidate_slots_remaining,
    }
    optional_availability = {
        "deadline_exceeded": _optional_bool(
            session.get("deadline_exceeded")
            if "deadline_exceeded" in session
            else budget.get("deadline_exceeded")
        ),
        "accepts_new_work": _optional_bool(
            session.get("accepts_new_work")
            if "accepts_new_work" in session
            else budget.get("accepts_new_work")
        ),
        "accepts_finalization": _optional_bool(
            session.get("accepts_finalization")
            if "accepts_finalization" in session
            else budget.get("accepts_finalization")
        ),
    }
    result.update(
        {
            key: value
            for key, value in optional_availability.items()
            if value is not None
        }
    )
    return result


def _benchmark_headline(payload: Mapping[str, Any]) -> dict[str, Any]:
    objective = _optional_string(payload.get("objective"))
    metric = _OBJECTIVE_METRICS.get(objective or "")
    parameter_results = payload.get("parameter_results")
    results = parameter_results if isinstance(parameter_results, list) else []
    return {
        "session_id": _optional_string(payload.get("session_id")),
        "candidate_id": _optional_string(payload.get("candidate_id")),
        "classification": str(payload.get("classification") or "unknown"),
        "objective": objective,
        "metric": metric,
        "relative_improvement_pct": _weighted_relative_improvement(
            results,
            metric,
        ),
        "parameter_case_count": len(results),
        "executions": _optional_int(payload.get("executions")) or 0,
        "proof_scope": _optional_string(payload.get("proof_scope")),
    }


def _plan_headline(payload: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(payload.get("summary"))
    warnings = summary.get("warnings")
    missing_indexes = summary.get("missing_indexes")
    return {
        "plan_kind": _optional_string(payload.get("plan_kind")),
        "statement_count": _optional_int(summary.get("statement_count")) or 0,
        "operator_count": _optional_int(summary.get("operator_count")) or 0,
        "warning_count": len(warnings) if isinstance(warnings, list) else 0,
        "missing_index_count": (
            len(missing_indexes) if isinstance(missing_indexes, list) else 0
        ),
    }


def _preflight_headline(payload: Mapping[str, Any]) -> dict[str, Any]:
    risk_codes = payload.get("risk_codes")
    unresolved = payload.get("unresolved_dependencies")
    return {
        "classification": str(payload.get("classification") or "unknown"),
        "coverage_complete": payload.get("coverage_complete") is True,
        "direct_snapshot_supported": (
            payload.get("direct_snapshot_supported") is True
        ),
        "risk_count": len(risk_codes) if isinstance(risk_codes, list) else 0,
        "unresolved_dependency_count": (
            len(unresolved) if isinstance(unresolved, list) else 0
        ),
    }


def _weighted_relative_improvement(
    results: list[Any],
    metric: str | None,
) -> float | None:
    if metric is None:
        return None
    improvements: list[tuple[float, float]] = []
    for item in results:
        row = _mapping(item)
        baseline = _mapping(row.get("baseline"))
        candidate = _mapping(row.get("candidate"))
        before = baseline.get(metric)
        after = candidate.get(metric)
        if (
            not isinstance(before, (int, float))
            or isinstance(before, bool)
            or before <= 0
            or not isinstance(after, (int, float))
            or isinstance(after, bool)
        ):
            continue
        raw_weight = row.get("weight", 1.0)
        weight = (
            float(raw_weight)
            if isinstance(raw_weight, (int, float))
            and not isinstance(raw_weight, bool)
            and raw_weight > 0
            else 1.0
        )
        improvements.append(((float(before) - float(after)) / float(before), weight))
    if not improvements:
        return None
    total_weight = sum(weight for _, weight in improvements)
    value = sum(change * weight for change, weight in improvements) / total_weight
    return round(value * 100, 4)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_int(value: Any) -> int | None:
    return (
        int(value)
        if isinstance(value, (int, float)) and not isinstance(value, bool)
        else None
    )


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None
