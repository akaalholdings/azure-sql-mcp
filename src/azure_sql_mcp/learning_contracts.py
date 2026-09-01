"""Versioned, redacted contracts for evidence-governed learning.

The learning boundary accepts identifiers, fingerprints, bounded labels, and
structured summaries only.  It has no representation for SQL text, credentials,
parameter values, result data, or chain-of-thought.
"""

from __future__ import annotations

import json
import math
import re
import uuid
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, ClassVar, Mapping, TypeVar, cast


CONTRACT_VERSION = 1
DEFAULT_FRESHNESS_DAYS = 180
SUBJECT_KINDS = frozenset({"query", "plan", "incident", "database", "index"})
UTC = timezone.utc
ContractT = TypeVar("ContractT", bound="VersionedLearningContract")

_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_SQL_PATTERN = re.compile(
    r"\b(?:SELECT|WITH|INSERT|UPDATE|DELETE|MERGE|EXEC(?:UTE)?|DECLARE|CREATE|ALTER|DROP)\b",
    re.IGNORECASE,
)
_CONTROL_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_CREDENTIAL_VALUE_PATTERN = re.compile(
    r"\b(?:password|pwd|secret|token|client_secret|connection_string)\s*=",
    re.IGNORECASE,
)
_SENSITIVE_PARTS = (
    "credential",
    "password",
    "secret",
    "token",
    "connection",
    "raw_sql",
    "sql_text",
    "statement",
    "parameter",
    "param_value",
    "param",
    "credential",
    "result",
    "row",
    "result_data",
    "result_rows",
    "chain_of_thought",
    "chainofthought",
    "cot",
    "reasoning",
)
_SENSITIVE_KEYS = frozenset(
    {
        "sql",
        "query",
        "query_text",
        "rows",
        "row_data",
        "result",
        "results",
        "values",
        "secrets",
        "environment",
        "environment_value",
        "server",
        "server_name",
        "database",
        "database_name",
        "endpoint",
        "tenant_id",
        "client_id",
        "username",
        "user_name",
    }
)
_EVIDENCE_PREFIXES = ("evidence-", "terminal-link-")


class ContractValidationError(ValueError):
    """Raised when a learning contract is malformed or unsafe."""


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def parse_timestamp(value: str, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ContractValidationError(f"{field_name} must be a non-empty timestamp.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractValidationError(f"{field_name} must be an ISO timestamp.") from exc
    if parsed.tzinfo is None:
        raise ContractValidationError(f"{field_name} must include a timezone.")
    return parsed


def timestamp_is_before_or_equal(left: str, right: str) -> bool:
    return parse_timestamp(left, "left") <= parse_timestamp(right, "right")


def _validate_version(value: int) -> None:
    if value != CONTRACT_VERSION:
        raise ContractValidationError(
            f"Unsupported contract_version={value!r}; expected {CONTRACT_VERSION}."
        )


def _validate_id(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not _ID_PATTERN.fullmatch(value):
        raise ContractValidationError(
            f"{field_name} must be a non-empty identifier of at most 200 characters."
        )


def _validate_optional_id(value: str | None, field_name: str) -> None:
    if value is not None:
        _validate_id(value, field_name)


def validate_evidence_ref(value: str, field_name: str = "evidence_ref") -> str:
    """Accept only identifiers owned by the evidence or terminal-link stores."""

    _validate_id(value, field_name)
    if not value.startswith(_EVIDENCE_PREFIXES):
        raise ContractValidationError(
            f"{field_name} has an unsupported evidence prefix; expected evidence- or terminal-link-."
        )
    return value


def validate_terminal_link_ref(
    value: str, field_name: str = "terminal_evidence_ref"
) -> str:
    """Reviews may cite terminal links, never arbitrary evidence identifiers."""

    _validate_id(value, field_name)
    if not value.startswith("terminal-link-"):
        raise ContractValidationError(
            f"{field_name} must reference a terminal-link- identifier."
        )
    return value


def _validate_fingerprint(value: str | None, field_name: str, *, required: bool = False) -> None:
    if value is None:
        if required:
            raise ContractValidationError(f"{field_name} must not be empty.")
        return
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise ContractValidationError(
            f"{field_name} must be a non-empty fingerprint of at most 512 characters."
        )
    if _SQL_PATTERN.search(value) or _CONTROL_PATTERN.search(value) or _CREDENTIAL_VALUE_PATTERN.search(value):
        raise ContractValidationError(f"{field_name} must be a redacted fingerprint.")


def validate_fingerprint(
    value: str | None, field_name: str, *, required: bool = False
) -> str | None:
    """Public fingerprint validator for persistence boundaries."""

    _validate_fingerprint(value, field_name, required=required)
    return value


def _safe_text(value: str, field_name: str, *, required: bool = False, limit: int = 1000) -> str:
    if not isinstance(value, str):
        raise ContractValidationError(f"{field_name} must be a string.")
    value = value.strip()
    if required and not value:
        raise ContractValidationError(f"{field_name} must not be empty.")
    if len(value) > limit:
        raise ContractValidationError(f"{field_name} must be at most {limit} characters.")
    if _CONTROL_PATTERN.search(value) or _SQL_PATTERN.search(value) or _CREDENTIAL_VALUE_PATTERN.search(value):
        raise ContractValidationError(
            f"{field_name} must not contain SQL-shaped or control-character content."
        )
    return value


def _refs(value: Any, field_name: str, *, required: bool = True) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise ContractValidationError(f"{field_name} must be a sequence of immutable references.")
    try:
        result = tuple(value)
    except TypeError as exc:
        raise ContractValidationError(f"{field_name} must be a sequence of immutable references.") from exc
    if required and not result:
        raise ContractValidationError(f"{field_name} must contain at least one reference.")
    if len(result) > 1000:
        raise ContractValidationError(f"{field_name} must contain at most 1000 references.")
    for index, ref in enumerate(result):
        _validate_id(ref, f"{field_name}[{index}]")
    if len(set(result)) != len(result):
        raise ContractValidationError(f"{field_name} must not contain duplicates.")
    return result


def _labels(value: Any, field_name: str, *, required: bool = False, limit: int = 300) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise ContractValidationError(f"{field_name} must be a sequence of labels.")
    try:
        result = tuple(value)
    except TypeError as exc:
        raise ContractValidationError(f"{field_name} must be a sequence of labels.") from exc
    if required and not result:
        raise ContractValidationError(f"{field_name} must contain at least one label.")
    if len(result) > 1000:
        raise ContractValidationError(f"{field_name} must contain at most 1000 labels.")
    for index, label in enumerate(result):
        _safe_text(label, f"{field_name}[{index}]", required=True, limit=limit)
    if len(set(result)) != len(result):
        raise ContractValidationError(f"{field_name} must not contain duplicates.")
    return result


def _sensitive_key(key: str) -> bool:
    normalized = key.casefold().replace("-", "_").replace(" ", "_")
    return normalized in _SENSITIVE_KEYS or any(part in normalized for part in _SENSITIVE_PARTS)


def _json_safe(value: Any, *, key: str | None = None, redact: bool) -> Any:
    if key is not None and _sensitive_key(key):
        if redact:
            return None
        raise ContractValidationError(f"Sensitive field {key!r} is not allowed.")
    if value is None or isinstance(value, (bool, int, str)):
        if isinstance(value, str) and (
            _SQL_PATTERN.search(value)
            or _CONTROL_PATTERN.search(value)
            or _CREDENTIAL_VALUE_PATTERN.search(value)
        ):
            if redact:
                return None
            raise ContractValidationError("SQL-shaped or control-character text is not allowed.")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ContractValidationError("Summary numbers must be finite.")
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            if not isinstance(raw_key, str):
                raise ContractValidationError("Structured summary keys must be strings.")
            field_key = raw_key
            sanitized = _json_safe(raw_value, key=field_key, redact=redact)
            if sanitized is not None:
                result[field_key] = sanitized
        return result
    if isinstance(value, (list, tuple)):
        result_list: list[Any] = []
        for item in value:
            sanitized = _json_safe(item, redact=redact)
            if sanitized is not None:
                result_list.append(sanitized)
        return result_list
    raise ContractValidationError(f"Unsupported summary type: {type(value).__name__}.")


def structured_summary(value: Mapping[str, Any], field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ContractValidationError(f"{field_name} must be a structured mapping.")
    summary = _json_safe(value, redact=False)
    if not isinstance(summary, dict):  # pragma: no cover
        raise ContractValidationError(f"{field_name} must be a structured mapping.")
    return summary


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _public_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _public_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_public_value(item) for item in value]
    if isinstance(value, list):
        return [_public_value(item) for item in value]
    return value


def redact_metadata(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Redact untrusted auxiliary metadata before persistence or export."""

    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ContractValidationError("metadata must be a mapping.")
    redacted = _json_safe(value, redact=True)
    if not isinstance(redacted, dict):  # pragma: no cover
        raise ContractValidationError("metadata must be a mapping.")
    return redacted


def _scope(value: Mapping[str, Any] | None, field_name: str = "scope") -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ContractValidationError(f"{field_name} must be a mapping.")
    return structured_summary(value, field_name)


def _tags(value: Any) -> tuple[str, ...]:
    return _labels(value, "tags", required=False, limit=80)


class VersionedLearningContract:
    contract_type: ClassVar[str]

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"contract_type": self.contract_type}
        for item in fields(cast(Any, self)):
            result[item.name] = _public_value(getattr(self, item.name))
        return result

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @classmethod
    def from_dict(cls: type[ContractT], data: Mapping[str, Any]) -> ContractT:
        raise NotImplementedError

    @classmethod
    def from_json(cls: type[ContractT], payload: str) -> ContractT:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ContractValidationError("Contract JSON is invalid.") from exc
        if not isinstance(data, dict):
            raise ContractValidationError("Contract JSON must contain an object.")
        return cls.from_dict(data)


def _contract_data(cls: type[ContractT], data: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(data, Mapping):
        raise ContractValidationError("Contract data must be a mapping.")
    if data.get("contract_type") not in (None, cls.contract_type):
        raise ContractValidationError(f"Expected contract_type={cls.contract_type!r}.")
    allowed = {item.name for item in fields(cast(Any, cls))}
    unknown = set(data) - allowed - {"contract_type"}
    if unknown:
        raise ContractValidationError(f"Unknown contract field(s): {', '.join(sorted(map(str, unknown)))}.")
    return {key: value for key, value in data.items() if key in allowed}


def _choice(value: str, allowed: frozenset[str], field_name: str) -> None:
    if value not in allowed:
        raise ContractValidationError(f"Unsupported {field_name}: {value!r}.")


def _validate_scope_fingerprints(scope: Mapping[str, Any]) -> None:
    for key in (
        "server_fingerprint",
        "database_fingerprint",
        "runtime_fingerprint",
        "runtime_compatibility_fingerprint",
        "tool_schema_fingerprint",
        "sanitized_config_fingerprint",
    ):
        value = scope.get(key)
        if value is not None:
            _validate_fingerprint(value, f"scope.{key}")


@dataclass(frozen=True, slots=True)
class DecisionRecordV1(VersionedLearningContract):
    """A versioned skill decision backed by consumed, immutable evidence."""

    contract_type: ClassVar[str] = "DecisionRecordV1"
    contract_version: int = CONTRACT_VERSION
    decision_id: str = field(default_factory=lambda: new_id("decision"))
    skill: str = ""
    skill_version: str = ""
    case_id: str | None = None
    session_id: str | None = None
    candidate_id: str | None = None
    learning_key: str = ""
    consumed_evidence_refs: tuple[str, ...] = ()
    subject_kind: str = ""
    subject_fingerprint: str = ""
    query_fingerprint: str | None = None
    based_on_review_ids: tuple[str, ...] = ()
    tactic: str = ""
    expected_result: Mapping[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    uncertainty: Mapping[str, Any] = field(default_factory=dict)
    applied_lesson_ids: tuple[str, ...] = ()
    evaluator_fingerprint: str = ""
    runtime_fingerprint: str = ""
    runtime_compatibility_fingerprint: str = ""
    tool_schema_fingerprint: str | None = None
    sanitized_config_fingerprint: str | None = None
    scope: Mapping[str, Any] = field(default_factory=dict)
    tags: tuple[str, ...] = ()
    created_at_utc: str = field(default_factory=utc_now)
    updated_at_utc: str = field(default_factory=utc_now)
    lifecycle: str = "recorded"
    version: int = 0

    def __post_init__(self) -> None:
        _validate_version(self.contract_version)
        _validate_id(self.decision_id, "decision_id")
        _safe_text(self.skill, "skill", required=True)
        _safe_text(self.skill_version, "skill_version", required=True, limit=100)
        _validate_optional_id(self.case_id, "case_id")
        _validate_optional_id(self.session_id, "session_id")
        _validate_optional_id(self.candidate_id, "candidate_id")
        _safe_text(self.learning_key, "learning_key", required=True, limit=200)
        _refs(self.consumed_evidence_refs, "consumed_evidence_refs")
        for index, ref in enumerate(self.consumed_evidence_refs):
            validate_evidence_ref(ref, f"consumed_evidence_refs[{index}]")
        _safe_text(self.subject_kind, "subject_kind", required=True, limit=100)
        _choice(self.subject_kind, SUBJECT_KINDS, "subject_kind")
        _validate_fingerprint(self.subject_fingerprint, "subject_fingerprint", required=True)
        _validate_fingerprint(self.query_fingerprint, "query_fingerprint")
        based_on_review_ids = _refs(
            self.based_on_review_ids,
            "based_on_review_ids",
            required=False,
        )
        for index, review_id in enumerate(based_on_review_ids):
            if not review_id.startswith("review-"):
                raise ContractValidationError(
                    f"based_on_review_ids[{index}] must reference a review- identifier."
                )
        _safe_text(self.tactic, "tactic", required=True, limit=500)
        object.__setattr__(self, "expected_result", structured_summary(self.expected_result, "expected_result"))
        object.__setattr__(self, "uncertainty", structured_summary(self.uncertainty, "uncertainty"))
        if isinstance(self.confidence, bool) or not isinstance(self.confidence, (int, float)) or not 0.0 <= float(self.confidence) <= 1.0:
            raise ContractValidationError("confidence must be between 0 and 1.")
        _refs(self.applied_lesson_ids, "applied_lesson_ids", required=False)
        _validate_fingerprint(self.evaluator_fingerprint, "evaluator_fingerprint", required=True)
        _validate_fingerprint(self.runtime_fingerprint, "runtime_fingerprint", required=True)
        _validate_fingerprint(
            self.runtime_compatibility_fingerprint,
            "runtime_compatibility_fingerprint",
            required=True,
        )
        _validate_fingerprint(
            self.tool_schema_fingerprint,
            "tool_schema_fingerprint",
        )
        _validate_fingerprint(
            self.sanitized_config_fingerprint,
            "sanitized_config_fingerprint",
        )
        scope = _scope(self.scope)
        _validate_scope_fingerprints(scope)
        _tags(self.tags)
        parse_timestamp(self.created_at_utc, "created_at_utc")
        parse_timestamp(self.updated_at_utc, "updated_at_utc")
        if not timestamp_is_before_or_equal(self.created_at_utc, self.updated_at_utc):
            raise ContractValidationError("created_at_utc must not be after updated_at_utc.")
        _choice(self.lifecycle, frozenset({"recorded", "reviewed", "superseded", "closed"}), "decision lifecycle")
        if self.version < 0:
            raise ContractValidationError("version must not be negative.")
        object.__setattr__(self, "consumed_evidence_refs", tuple(self.consumed_evidence_refs))
        object.__setattr__(self, "applied_lesson_ids", tuple(self.applied_lesson_ids))
        object.__setattr__(self, "based_on_review_ids", based_on_review_ids)
        object.__setattr__(self, "scope", scope)
        object.__setattr__(self, "tags", _tags(self.tags))
        object.__setattr__(self, "expected_result", _freeze(self.expected_result))
        object.__setattr__(self, "uncertainty", _freeze(self.uncertainty))
        object.__setattr__(self, "scope", _freeze(self.scope))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DecisionRecordV1:
        values = _contract_data(cls, data)
        for key in ("consumed_evidence_refs", "applied_lesson_ids", "based_on_review_ids", "tags"):
            if key in values:
                values[key] = tuple(values[key])
        return cls(**values)


_SIGNALS = frozenset({"passed", "proven_failure", "not_applicable", "unknown"})


@dataclass(frozen=True, slots=True)
class OutcomeReviewV1(VersionedLearningContract):
    """A terminal, complete, aligned, or explicitly unresolved review."""

    contract_type: ClassVar[str] = "OutcomeReviewV1"
    contract_version: int = CONTRACT_VERSION
    review_id: str = field(default_factory=lambda: new_id("review"))
    decision_id: str = ""
    terminal_evidence_refs: tuple[str, ...] = ()
    observed_result: Mapping[str, Any] = field(default_factory=dict)
    prediction_error: Mapping[str, Any] = field(default_factory=dict)
    counterexamples: tuple[Mapping[str, Any], ...] = ()
    next_observation: Mapping[str, Any] | None = None
    causal_strength: str = "unknown"
    correction: Mapping[str, Any] | None = None
    unresolved_gaps: tuple[str, ...] = ()
    created_at_utc: str = field(default_factory=utc_now)
    completed_at_utc: str | None = None
    complete: bool = False
    alignment: str = "unknown"
    safety_signal: str = "unknown"
    equivalence_signal: str = "unknown"
    cleanup_signal: str = "unknown"
    material_regression_signal: str = "unknown"
    unknown_outcome: bool = True
    explicit_correction: bool = False
    version: int = 0

    def __post_init__(self) -> None:
        _validate_version(self.contract_version)
        _validate_id(self.review_id, "review_id")
        _validate_id(self.decision_id, "decision_id")
        if not self.terminal_evidence_refs:
            raise ContractValidationError("terminal_evidence_refs must not be empty.")
        for index, ref in enumerate(self.terminal_evidence_refs):
            validate_terminal_link_ref(ref, f"terminal_evidence_refs[{index}]")
        object.__setattr__(self, "observed_result", structured_summary(self.observed_result, "observed_result"))
        object.__setattr__(self, "prediction_error", structured_summary(self.prediction_error, "prediction_error"))
        counterexamples: list[Mapping[str, Any]] = []
        for counterexample in self.counterexamples:
            counterexamples.append(structured_summary(counterexample, "counterexample"))
        object.__setattr__(self, "counterexamples", tuple(counterexamples))
        if self.next_observation is not None:
            object.__setattr__(
                self,
                "next_observation",
                structured_summary(self.next_observation, "next_observation"),
            )
        _choice(self.causal_strength, frozenset({"strong", "moderate", "weak", "none", "unknown"}), "causal_strength")
        if self.correction is not None:
            object.__setattr__(self, "correction", structured_summary(self.correction, "correction"))
        _labels(self.unresolved_gaps, "unresolved_gaps", required=False)
        parse_timestamp(self.created_at_utc, "created_at_utc")
        if self.completed_at_utc is not None:
            parse_timestamp(self.completed_at_utc, "completed_at_utc")
            if not timestamp_is_before_or_equal(self.created_at_utc, self.completed_at_utc):
                raise ContractValidationError("completed_at_utc cannot precede created_at_utc.")
        _choice(self.alignment, frozenset({"aligned", "contradiction", "unknown"}), "alignment")
        for field_name in ("safety_signal", "equivalence_signal", "cleanup_signal", "material_regression_signal"):
            _choice(getattr(self, field_name), _SIGNALS, field_name)
        if not isinstance(self.complete, bool) or not isinstance(self.unknown_outcome, bool):
            raise ContractValidationError("complete and unknown_outcome must be booleans.")
        if not isinstance(self.explicit_correction, bool):
            raise ContractValidationError("explicit_correction must be a boolean.")
        if not self.complete:
            if self.completed_at_utc is not None or self.alignment != "unknown":
                raise ContractValidationError("An incomplete review must remain unresolved.")
            if any(getattr(self, name) not in {"unknown", "not_applicable"} for name in ("safety_signal", "equivalence_signal", "cleanup_signal", "material_regression_signal")):
                raise ContractValidationError("An incomplete review cannot assert an outcome signal.")
            if self.explicit_correction or self.correction is not None:
                raise ContractValidationError("An incomplete review cannot assert a correction.")
            if not self.unknown_outcome:
                raise ContractValidationError("An incomplete review must set unknown_outcome.")
        else:
            if self.completed_at_utc is None:
                raise ContractValidationError("A complete review must have completed_at_utc.")
            if self.unknown_outcome:
                if self.alignment != "unknown":
                    raise ContractValidationError(
                        "An unknown terminal outcome cannot claim alignment."
                    )
                if any(
                    getattr(self, name) not in {"unknown", "not_applicable"}
                    for name in (
                        "safety_signal",
                        "equivalence_signal",
                        "cleanup_signal",
                        "material_regression_signal",
                    )
                ):
                    raise ContractValidationError(
                        "An unknown terminal outcome cannot assert an outcome signal."
                    )
            elif self.alignment == "unknown":
                raise ContractValidationError(
                    "A known complete review must declare alignment."
                )
            proven_failure = any(
                getattr(self, name) == "proven_failure"
                for name in ("safety_signal", "equivalence_signal", "cleanup_signal", "material_regression_signal")
            )
            if not self.unknown_outcome and self.alignment == "aligned" and proven_failure:
                raise ContractValidationError("An aligned review cannot contain a proven contradiction.")
            if (
                not self.unknown_outcome
                and self.alignment == "contradiction"
                and not (proven_failure or self.explicit_correction)
            ):
                raise ContractValidationError("A contradiction needs a proven failure or explicit correction.")
            if self.explicit_correction and self.correction is None:
                raise ContractValidationError("explicit_correction requires a structured correction.")
        if self.version < 0:
            raise ContractValidationError("version must not be negative.")
        object.__setattr__(self, "terminal_evidence_refs", tuple(self.terminal_evidence_refs))
        object.__setattr__(self, "unresolved_gaps", tuple(self.unresolved_gaps))
        object.__setattr__(self, "observed_result", _freeze(self.observed_result))
        object.__setattr__(self, "prediction_error", _freeze(self.prediction_error))
        object.__setattr__(self, "counterexamples", _freeze(self.counterexamples))
        if self.next_observation is not None:
            object.__setattr__(self, "next_observation", _freeze(self.next_observation))
        if self.correction is not None:
            object.__setattr__(self, "correction", _freeze(self.correction))

    @property
    def is_aligned_complete(self) -> bool:
        return self.complete and self.alignment == "aligned" and not self.unknown_outcome

    @property
    def has_safety_or_equivalence_failure(self) -> bool:
        return self.safety_signal == "proven_failure" or self.equivalence_signal == "proven_failure"

    @property
    def has_cleanup_failure(self) -> bool:
        return self.cleanup_signal == "proven_failure"

    @property
    def has_material_contradiction(self) -> bool:
        return self.material_regression_signal == "proven_failure" or self.alignment == "contradiction"

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> OutcomeReviewV1:
        values = _contract_data(cls, data)
        for key in ("terminal_evidence_refs", "counterexamples", "unresolved_gaps"):
            if key in values:
                values[key] = tuple(values[key])
        return cls(**values)


@dataclass(frozen=True, slots=True)
class LessonV1(VersionedLearningContract):
    """A review-governed lesson with auditable support and contradiction history."""

    contract_type: ClassVar[str] = "LessonV1"
    contract_version: int = CONTRACT_VERSION
    lesson_id: str = field(default_factory=lambda: new_id("lesson"))
    learning_key: str = ""
    subject_kind: str = "query"
    subject_fingerprint: str | None = None
    trigger: Mapping[str, Any] = field(default_factory=dict)
    action: Mapping[str, Any] = field(default_factory=dict)
    preconditions: Mapping[str, Any] = field(default_factory=dict)
    counterexamples: tuple[Mapping[str, Any], ...] = ()
    next_observation: Mapping[str, Any] | None = None
    required_evidence: tuple[str, ...] = ()
    applicable_skills: tuple[str, ...] = ()
    applicable_scopes: tuple[Mapping[str, Any], ...] = ()
    query_fingerprints: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    support_refs: tuple[str, ...] = ()
    based_on_review_ids: tuple[str, ...] = ()
    contradiction_refs: tuple[str, ...] = ()
    reviewer: str | None = None
    reviewed_at_utc: str | None = None
    freshness_days: int = DEFAULT_FRESHNESS_DAYS
    created_at_utc: str = field(default_factory=utc_now)
    updated_at_utc: str = field(default_factory=utc_now)
    status_changed_at_utc: str | None = None
    last_supported_at_utc: str | None = None
    status: str = "proposed"
    proposal_kind: str = "normal"
    rejection_code: str | None = None
    rejected_by: str | None = None
    rejected_at_utc: str | None = None
    supersedes_lesson_id: str | None = None
    superseded_by_lesson_id: str | None = None
    support_session_ids: tuple[str, ...] = ()
    support_query_fingerprints: tuple[str, ...] = ()
    source_provenance: Mapping[str, Any] | None = None
    version: int = 0

    def __post_init__(self) -> None:
        _validate_version(self.contract_version)
        _validate_id(self.lesson_id, "lesson_id")
        _safe_text(self.learning_key, "learning_key", required=True, limit=200)
        _safe_text(self.subject_kind, "subject_kind", required=True, limit=100)
        _choice(self.subject_kind, SUBJECT_KINDS, "subject_kind")
        if self.subject_fingerprint is not None:
            _validate_fingerprint(self.subject_fingerprint, "subject_fingerprint")
        for field_name, value in (("trigger", self.trigger), ("action", self.action), ("preconditions", self.preconditions)):
            summary = structured_summary(value, field_name)
            if field_name in {"trigger", "action"} and not summary:
                raise ContractValidationError(f"{field_name} must not be empty.")
            object.__setattr__(self, field_name, summary)
        counterexamples: list[Mapping[str, Any]] = []
        for counterexample in self.counterexamples:
            counterexamples.append(structured_summary(counterexample, "counterexample"))
        object.__setattr__(self, "counterexamples", tuple(counterexamples))
        if self.next_observation is not None:
            object.__setattr__(self, "next_observation", structured_summary(self.next_observation, "next_observation"))
        _labels(self.required_evidence, "required_evidence", required=False)
        for index, ref in enumerate(self.required_evidence):
            validate_evidence_ref(ref, f"required_evidence[{index}]")
        _labels(self.applicable_skills, "applicable_skills", required=True)
        scopes: list[Mapping[str, Any]] = []
        for scope in self.applicable_scopes:
            scopes.append(_scope(scope, "applicable_scope"))
        if not scopes:
            raise ContractValidationError(
                "applicable_scopes must contain at least one scope."
            )
        object.__setattr__(self, "applicable_scopes", tuple(scopes))
        query_fingerprints = _refs(
            self.query_fingerprints,
            "query_fingerprints",
            required=False,
        )
        for fingerprint in query_fingerprints:
            _validate_fingerprint(fingerprint, "query_fingerprint", required=True)
        _tags(self.tags)
        support_refs = _refs(self.support_refs, "support_refs", required=False)
        based_on_review_ids = _refs(
            self.based_on_review_ids or support_refs,
            "based_on_review_ids",
        )
        if self.based_on_review_ids and support_refs and set(self.based_on_review_ids) != set(support_refs):
            raise ContractValidationError("based_on_review_ids and support_refs must identify the same reviews.")
        if not support_refs:
            support_refs = based_on_review_ids
        _refs(self.contradiction_refs, "contradiction_refs", required=False)
        if self.reviewer is not None:
            _safe_text(self.reviewer, "reviewer", required=True, limit=200)
        if self.reviewed_at_utc is not None:
            parse_timestamp(self.reviewed_at_utc, "reviewed_at_utc")
        if self.freshness_days <= 0:
            raise ContractValidationError("freshness_days must be greater than zero.")
        parse_timestamp(self.created_at_utc, "created_at_utc")
        parse_timestamp(self.updated_at_utc, "updated_at_utc")
        if not timestamp_is_before_or_equal(self.created_at_utc, self.updated_at_utc):
            raise ContractValidationError("created_at_utc must not be after updated_at_utc.")
        if self.status_changed_at_utc is not None:
            parse_timestamp(self.status_changed_at_utc, "status_changed_at_utc")
            if not timestamp_is_before_or_equal(self.created_at_utc, self.status_changed_at_utc):
                raise ContractValidationError("status_changed_at_utc cannot precede created_at_utc.")
            if not timestamp_is_before_or_equal(self.status_changed_at_utc, self.updated_at_utc):
                raise ContractValidationError("status_changed_at_utc cannot follow updated_at_utc.")
        if self.last_supported_at_utc is not None:
            parse_timestamp(self.last_supported_at_utc, "last_supported_at_utc")
            if not timestamp_is_before_or_equal(self.last_supported_at_utc, self.updated_at_utc):
                raise ContractValidationError("last_supported_at_utc cannot follow updated_at_utc.")
        if self.reviewed_at_utc is not None:
            if not timestamp_is_before_or_equal(
                self.created_at_utc,
                self.reviewed_at_utc,
            ):
                raise ContractValidationError(
                    "reviewed_at_utc cannot precede created_at_utc."
                )
            if not timestamp_is_before_or_equal(
                self.reviewed_at_utc,
                self.updated_at_utc,
            ):
                raise ContractValidationError(
                    "reviewed_at_utc cannot follow updated_at_utc."
                )
        _choice(self.status, frozenset({"proposed", "eligible", "active", "quarantined", "superseded", "retired", "rejected"}), "lesson status")
        _choice(self.proposal_kind, frozenset({"normal", "urgent", "imported"}), "proposal kind")
        _validate_optional_id(self.rejection_code, "rejection_code") if self.rejection_code else None
        _validate_optional_id(self.rejected_by, "rejected_by")
        if self.rejected_at_utc is not None:
            parse_timestamp(self.rejected_at_utc, "rejected_at_utc")
            if not timestamp_is_before_or_equal(self.created_at_utc, self.rejected_at_utc):
                raise ContractValidationError("rejected_at_utc cannot precede created_at_utc.")
            if not timestamp_is_before_or_equal(self.rejected_at_utc, self.updated_at_utc):
                raise ContractValidationError("rejected_at_utc cannot follow updated_at_utc.")
        _validate_optional_id(self.supersedes_lesson_id, "supersedes_lesson_id")
        _validate_optional_id(self.superseded_by_lesson_id, "superseded_by_lesson_id")
        _refs(
            self.support_session_ids,
            "support_session_ids",
            required=False,
        )
        support_query_fingerprints = _refs(
            self.support_query_fingerprints,
            "support_query_fingerprints",
            required=False,
        )
        for fingerprint in support_query_fingerprints:
            _validate_fingerprint(fingerprint, "support_query_fingerprint", required=True)
        if self.status == "eligible" and self.proposal_kind == "urgent":
            raise ContractValidationError("Urgent proposals remain proposed until maintainer approval.")
        reviewed_statuses = {"active", "quarantined", "superseded", "retired"}
        if self.status in reviewed_statuses and not (
            self.reviewer and self.reviewed_at_utc
        ):
            raise ContractValidationError(
                f"{self.status} lessons require reviewer and reviewed_at_utc."
            )
        if self.status not in reviewed_statuses and self.reviewed_at_utc is not None:
            raise ContractValidationError(
                "Only reviewed lesson states may carry reviewer fields."
            )
        if self.status == "active" and not self.counterexamples:
            raise ContractValidationError(
                "Active lessons require at least one counterexample or bounded risk."
            )
        if self.status == "rejected" and not (self.rejection_code and self.rejected_by and self.rejected_at_utc):
            raise ContractValidationError("Rejected lessons require rejection code, reviewer, and timestamp.")
        if self.status != "rejected" and (self.rejection_code is not None or self.rejected_by is not None or self.rejected_at_utc is not None):
            raise ContractValidationError("Only rejected lessons may carry rejection audit fields.")
        if self.status == "superseded" and not self.superseded_by_lesson_id:
            raise ContractValidationError("Superseded lessons require superseded_by_lesson_id.")
        if self.version < 0:
            raise ContractValidationError("version must not be negative.")
        if self.source_provenance is not None:
            object.__setattr__(self, "source_provenance", structured_summary(self.source_provenance, "source_provenance"))
        object.__setattr__(self, "required_evidence", tuple(self.required_evidence))
        object.__setattr__(self, "applicable_skills", tuple(self.applicable_skills))
        object.__setattr__(self, "query_fingerprints", query_fingerprints)
        object.__setattr__(self, "tags", _tags(self.tags))
        object.__setattr__(self, "support_refs", support_refs)
        object.__setattr__(self, "based_on_review_ids", based_on_review_ids)
        object.__setattr__(self, "contradiction_refs", tuple(self.contradiction_refs))
        object.__setattr__(self, "support_session_ids", tuple(self.support_session_ids))
        object.__setattr__(self, "support_query_fingerprints", support_query_fingerprints)
        object.__setattr__(self, "trigger", _freeze(self.trigger))
        object.__setattr__(self, "action", _freeze(self.action))
        object.__setattr__(self, "preconditions", _freeze(self.preconditions))
        object.__setattr__(self, "counterexamples", tuple(_freeze(item) for item in self.counterexamples))
        if self.next_observation is not None:
            object.__setattr__(self, "next_observation", _freeze(self.next_observation))
        object.__setattr__(self, "applicable_scopes", tuple(_freeze(item) for item in self.applicable_scopes))
        if self.source_provenance is not None:
            object.__setattr__(self, "source_provenance", _freeze(self.source_provenance))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LessonV1:
        values = _contract_data(cls, data)
        for key in ("required_evidence", "applicable_skills", "query_fingerprints", "tags", "support_refs", "based_on_review_ids", "contradiction_refs", "support_session_ids", "support_query_fingerprints"):
            if key in values:
                values[key] = tuple(values[key])
        if "counterexamples" in values:
            values["counterexamples"] = tuple(values["counterexamples"])
        if "applicable_scopes" in values:
            values["applicable_scopes"] = tuple(values["applicable_scopes"])
        return cls(**values)


@dataclass(frozen=True, slots=True)
class HandoffV1(VersionedLearningContract):
    """A cross-skill workflow handoff, not a recall response."""

    contract_type: ClassVar[str] = "HandoffV1"
    contract_version: int = CONTRACT_VERSION
    handoff_id: str = field(default_factory=lambda: new_id("handoff"))
    handoff_type: str = "workflow"
    source_skill: str = ""
    target_skill: str = ""
    case_id: str | None = None
    session_id: str | None = None
    scope: Mapping[str, Any] = field(default_factory=dict)
    objective: Mapping[str, Any] = field(default_factory=dict)
    evidence_refs: tuple[str, ...] = ()
    constraints: Mapping[str, Any] = field(default_factory=dict)
    gaps: tuple[Mapping[str, Any], ...] = ()
    acceptance_criteria: tuple[Mapping[str, Any], ...] = ()
    status: str = "open"
    resolution: Mapping[str, Any] | None = None
    resolution_evidence_refs: tuple[str, ...] = ()
    owner: str | None = None
    created_at_utc: str = field(default_factory=utc_now)
    updated_at_utc: str = field(default_factory=utc_now)
    claimed_at_utc: str | None = None
    resolved_at_utc: str | None = None
    cancelled_at_utc: str | None = None
    reopen_count: int = 0
    version: int = 0

    def __post_init__(self) -> None:
        _validate_version(self.contract_version)
        _validate_id(self.handoff_id, "handoff_id")
        _safe_text(self.handoff_type, "handoff_type", required=True, limit=100)
        _safe_text(self.source_skill, "source_skill", required=True, limit=200)
        _safe_text(self.target_skill, "target_skill", required=True, limit=200)
        if self.source_skill == self.target_skill:
            raise ContractValidationError("source_skill and target_skill must differ.")
        _validate_optional_id(self.case_id, "case_id")
        _validate_optional_id(self.session_id, "session_id")
        scope = _scope(self.scope)
        _validate_scope_fingerprints(scope)
        object.__setattr__(self, "scope", scope)
        for field_name in ("objective", "constraints"):
            object.__setattr__(self, field_name, structured_summary(getattr(self, field_name), field_name))
        _refs(self.evidence_refs, "evidence_refs")
        for index, ref in enumerate(self.evidence_refs):
            validate_evidence_ref(ref, f"evidence_refs[{index}]")
        for field_name in ("gaps", "acceptance_criteria"):
            entries: list[Mapping[str, Any]] = []
            for entry in getattr(self, field_name):
                entries.append(structured_summary(entry, field_name[:-1]))
            object.__setattr__(self, field_name, tuple(entries))
        _choice(self.status, frozenset({"open", "claimed", "resolved", "cancelled"}), "handoff status")
        if self.resolution is not None:
            object.__setattr__(self, "resolution", structured_summary(self.resolution, "resolution"))
        _refs(
            self.resolution_evidence_refs,
            "resolution_evidence_refs",
            required=False,
        )
        for index, ref in enumerate(self.resolution_evidence_refs):
            validate_evidence_ref(ref, f"resolution_evidence_refs[{index}]")
        _validate_optional_id(self.owner, "owner")
        parse_timestamp(self.created_at_utc, "created_at_utc")
        parse_timestamp(self.updated_at_utc, "updated_at_utc")
        if not timestamp_is_before_or_equal(self.created_at_utc, self.updated_at_utc):
            raise ContractValidationError("created_at_utc must not be after updated_at_utc.")
        for field_name in ("claimed_at_utc", "resolved_at_utc", "cancelled_at_utc"):
            value = getattr(self, field_name)
            if value is not None:
                parse_timestamp(value, field_name)
                if not timestamp_is_before_or_equal(self.created_at_utc, value):
                    raise ContractValidationError(f"{field_name} cannot precede created_at_utc.")
                if not timestamp_is_before_or_equal(value, self.updated_at_utc):
                    raise ContractValidationError(
                        f"{field_name} cannot follow updated_at_utc."
                    )
        if self.status == "claimed" and (self.owner is None or self.claimed_at_utc is None):
            raise ContractValidationError("Claimed handoffs require owner and claimed_at_utc.")
        if self.status == "resolved" and (self.resolution is None or self.resolved_at_utc is None):
            raise ContractValidationError("Resolved handoffs require resolution and resolved_at_utc.")
        if (
            self.status == "resolved"
            and not self.resolution_evidence_refs
            and not bool((self.resolution or {}).get("human_decision"))
        ):
            raise ContractValidationError(
                "Resolved handoffs require terminal evidence or an explicit human decision."
            )
        if self.status == "cancelled" and self.cancelled_at_utc is None:
            raise ContractValidationError("Cancelled handoffs require cancelled_at_utc.")
        if self.reopen_count < 0 or self.version < 0:
            raise ContractValidationError("reopen_count and version must not be negative.")
        object.__setattr__(self, "evidence_refs", tuple(self.evidence_refs))
        object.__setattr__(
            self,
            "resolution_evidence_refs",
            tuple(self.resolution_evidence_refs),
        )
        object.__setattr__(self, "scope", _freeze(self.scope))
        object.__setattr__(self, "objective", _freeze(self.objective))
        object.__setattr__(self, "constraints", _freeze(self.constraints))
        object.__setattr__(self, "gaps", tuple(_freeze(item) for item in self.gaps))
        object.__setattr__(self, "acceptance_criteria", tuple(_freeze(item) for item in self.acceptance_criteria))
        if self.resolution is not None:
            object.__setattr__(self, "resolution", _freeze(self.resolution))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HandoffV1:
        values = _contract_data(cls, data)
        for key in (
            "evidence_refs",
            "resolution_evidence_refs",
            "gaps",
            "acceptance_criteria",
        ):
            if key in values:
                values[key] = tuple(values[key])
        return cls(**values)


CONTRACT_TYPES: dict[str, type[VersionedLearningContract]] = {
    DecisionRecordV1.contract_type: DecisionRecordV1,
    OutcomeReviewV1.contract_type: OutcomeReviewV1,
    LessonV1.contract_type: LessonV1,
    HandoffV1.contract_type: HandoffV1,
}


def deserialize_learning_contract(payload: str) -> VersionedLearningContract:
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ContractValidationError("Stored learning contract JSON is invalid.") from exc
    if not isinstance(data, dict):
        raise ContractValidationError("Stored learning contract must be an object.")
    contract_type = data.get("contract_type")
    contract_cls = CONTRACT_TYPES.get(contract_type) if isinstance(contract_type, str) else None
    if contract_cls is None:
        raise ContractValidationError(f"Unsupported learning contract type: {data.get('contract_type')!r}.")
    return contract_cls.from_dict(data)
