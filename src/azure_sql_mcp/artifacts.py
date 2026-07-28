from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Literal


def json_text(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, indent=2, default=str)


@dataclass(frozen=True)
class ErrorPayload:
    code: str
    message: str
    details: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = {"ok": False, "code": self.code, "message": self.message}
        if self.details:
            payload["details"] = self.details
        return payload


@dataclass(frozen=True)
class ExplainPlanArtifact:
    database_name: str
    analyze: bool
    summary: dict[str, Any]
    raw_xml: str
    plan_kind: Literal["actual", "estimated"] | None = None
    query_executed: bool | None = None
    compile_ms: int | None = None
    execution_ms: float | None = None
    metric_provenance: dict[str, str] | None = None

    def __post_init__(self) -> None:
        expected_plan_kind: Literal["actual", "estimated"] = (
            "actual" if self.analyze else "estimated"
        )
        plan_kind = self.plan_kind or expected_plan_kind
        if plan_kind != expected_plan_kind:
            raise ValueError("plan_kind must agree with analyze")
        query_executed = self.analyze if self.query_executed is None else self.query_executed
        if query_executed is not self.analyze:
            raise ValueError("query_executed must agree with analyze")
        compile_metrics = self.summary.get("compile_metrics", {})
        if not isinstance(compile_metrics, dict):
            compile_metrics = {}
        actual_metrics = self.summary.get("actual_metrics", {})
        if not isinstance(actual_metrics, dict):
            actual_metrics = {}
        compile_ms = (
            compile_metrics.get("compile_ms")
            if self.compile_ms is None
            else self.compile_ms
        )
        measured_wall_ms = actual_metrics.get("measured_wall_elapsed_ms")
        showplan_elapsed_ms = actual_metrics.get("actual_elapsed_ms")
        execution_ms = self.execution_ms
        execution_source = "unavailable"
        if execution_ms is None and query_executed:
            if measured_wall_ms is not None:
                execution_ms = measured_wall_ms
                execution_source = str(
                    actual_metrics.get(
                        "measured_wall_elapsed_source",
                        "client_wall_clock",
                    )
                )
            elif showplan_elapsed_ms is not None:
                execution_ms = showplan_elapsed_ms
                execution_source = str(
                    actual_metrics.get(
                        "query_metric_source",
                        "showplan_query_time_stats",
                    )
                )
        elif execution_ms is not None:
            execution_source = str(
                actual_metrics.get("query_metric_source", "explicit")
            )
        provenance = self.metric_provenance or {
            "compile_ms": (
                compile_metrics.get("metric_provenance", "unavailable")
                if compile_ms is not None
                else "unavailable"
            ),
            "execution_ms": (
                execution_source
                if execution_ms is not None
                else (
                    "not_applicable_estimated_plan"
                    if not query_executed
                    else "unavailable"
                )
            ),
        }
        object.__setattr__(self, "plan_kind", plan_kind)
        object.__setattr__(self, "query_executed", query_executed)
        object.__setattr__(self, "compile_ms", compile_ms)
        object.__setattr__(self, "execution_ms", execution_ms)
        object.__setattr__(self, "metric_provenance", provenance)

    def as_dict(
        self,
        *,
        include_raw_xml: bool = True,
        raw_xml_resource: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "database_name": self.database_name,
            "analyze": self.analyze,
            "plan_kind": self.plan_kind,
            "query_executed": self.query_executed,
            "compile_ms": self.compile_ms,
            "execution_ms": self.execution_ms,
            "metric_provenance": self.metric_provenance,
            "summary": self.summary,
            "raw_xml_length": len(self.raw_xml),
            "raw_xml_hash": hashlib.sha256(self.raw_xml.encode("utf-8")).hexdigest(),
        }
        if raw_xml_resource:
            payload["raw_xml_resource"] = raw_xml_resource
            payload["raw_xml_resource_uri"] = raw_xml_resource["resource_uri"]
        if include_raw_xml:
            payload["raw_xml"] = self.raw_xml
        return payload
