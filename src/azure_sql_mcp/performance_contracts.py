"""Versioned, database-independent contracts for performance tuning state.

The contracts deliberately contain fingerprints, references, and aggregate
metrics rather than SQL text.  They are the boundary between MCP execution
and the durable tuning workflow.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field, fields
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, ClassVar, Mapping, TypeVar, cast


CONTRACT_VERSION = 1
UTC = timezone.utc
_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_SQL_PREFIX_PATTERN = re.compile(
    r"^\s*(?:SELECT|WITH|INSERT|UPDATE|DELETE|MERGE|EXEC(?:UTE)?|DECLARE|CREATE|ALTER|DROP)\b",
    re.IGNORECASE,
)

_RAW_SQL_KEYS = frozenset(
    {
        "command",
        "command_text",
        "connection_string",
        "original_sql",
        "query",
        "query_sql_text",
        "query_text",
        "raw_sql",
        "rewritten_sql",
        "secret",
        "sql",
        "sql_text",
        "statement",
        "statement_text",
        "token",
        "password",
    }
)
_SENSITIVE_KEY_PARTS = ("connection_string", "password", "secret", "token")

ContractT = TypeVar("ContractT", bound="VersionedContract")


class ContractValidationError(ValueError):
    """Raised when a versioned contract is malformed."""


def utc_now() -> str:
    """Return an RFC 3339 UTC timestamp suitable for persisted state."""

    return datetime.now(UTC).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def _validate_id(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value or not _ID_PATTERN.fullmatch(value):
        raise ContractValidationError(
            f"{field_name} must be a non-empty identifier of at most 200 characters."
        )
    return value


def _validate_timestamp(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ContractValidationError(f"{field_name} must be a non-empty timestamp.")
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractValidationError(f"{field_name} must be an ISO timestamp.") from exc
    return value


def _validate_fingerprint(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ContractValidationError(
            f"{field_name} must be a non-empty fingerprint of at most 512 characters."
        )
    if _SQL_PREFIX_PATTERN.match(value):
        raise ContractValidationError(f"{field_name} must not contain raw SQL text.")
    return value


def _validate_artifact_ref(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip() or len(value) > 2048:
        raise ContractValidationError(
            "rewrite_artifact_ref must be a non-empty reference of at most 2048 characters."
        )
    if _SQL_PREFIX_PATTERN.match(value):
        raise ContractValidationError(
            "rewrite_artifact_ref must reference an artifact, not contain raw SQL text."
        )
    if any(character in value for character in ("\r", "\n", "\x00")):
        raise ContractValidationError(
            "rewrite_artifact_ref must not contain control characters."
        )
    return value


def _validate_version(value: int) -> int:
    if value != CONTRACT_VERSION:
        raise ContractValidationError(
            f"Unsupported contract_version={value!r}; expected {CONTRACT_VERSION}."
        )
    return value


def _redact_value(value: Any, *, key: str | None = None) -> Any:
    """Return JSON-safe metadata while dropping raw SQL and secret-like fields."""

    normalized_key = key.casefold().strip() if key else None
    if normalized_key in _RAW_SQL_KEYS or (
        normalized_key
        and any(part in normalized_key for part in _SENSITIVE_KEY_PARTS)
    ):
        return None
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            field_key = str(raw_key)
            redacted = _redact_value(raw_value, key=field_key)
            if redacted is not None:
                result[field_key] = redacted
        return result
    if isinstance(value, (list, tuple)):
        return [_redact_value(item) for item in value]
    if isinstance(value, str) and _SQL_PREFIX_PATTERN.match(value):
        return None
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ContractValidationError(
        f"Unsupported value type {type(value).__name__} in serializable contract."
    )


def redact_metadata(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Redact untrusted metadata before it crosses a persistence boundary."""

    if value is None:
        return {}
    redacted = _redact_value(value)
    if not isinstance(redacted, dict):  # pragma: no cover - guarded by _redact_value
        raise ContractValidationError("Metadata must be a mapping.")
    return redacted


class VersionedContract:
    """Mixin implementing stable dictionary and JSON serialization."""

    contract_type: ClassVar[str]

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "contract_type": self.contract_type,
        }
        for contract_field in fields(cast(Any, self)):
            value = getattr(self, contract_field.name)
            if isinstance(value, tuple):
                value = list(value)
            if contract_field.name in {"metadata", "metrics", "limits"}:
                value = _redact_value(value)
            result[contract_field.name] = value
        return result

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls: type[ContractT], data: Mapping[str, Any]) -> ContractT:
        raise NotImplementedError(f"{cls.__name__} must implement from_dict().")

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
    if data.get("contract_type") not in (None, cls.contract_type):
        raise ContractValidationError(
            f"Expected contract_type={cls.contract_type!r}; got {data.get('contract_type')!r}."
        )
    allowed = {contract_field.name for contract_field in fields(cast(Any, cls))}
    return {key: value for key, value in data.items() if key in allowed}


@dataclass(frozen=True, slots=True)
class EvidenceEnvelopeV1(VersionedContract):
    """Redacted evidence captured by the MCP execution layer."""

    contract_type: ClassVar[str] = "EvidenceEnvelopeV1"
    contract_version: int = CONTRACT_VERSION
    evidence_id: str = field(default_factory=lambda: new_id("evidence"))
    captured_at_utc: str = field(default_factory=utc_now)
    source: str = "mcp"
    kind: str = "performance"
    query_fingerprint: str | None = None
    database_fingerprint: str | None = None
    parameters_fingerprint: str | None = None
    plan_fingerprint: str | None = None
    observed_execution_count: int = 0
    metrics: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_version(self.contract_version)
        _validate_id(self.evidence_id, "evidence_id")
        _validate_timestamp(self.captured_at_utc, "captured_at_utc")
        if not self.source or not self.kind:
            raise ContractValidationError("source and kind must not be empty.")
        _validate_fingerprint(self.query_fingerprint, "query_fingerprint")
        _validate_fingerprint(self.database_fingerprint, "database_fingerprint")
        _validate_fingerprint(self.parameters_fingerprint, "parameters_fingerprint")
        _validate_fingerprint(self.plan_fingerprint, "plan_fingerprint")
        if self.observed_execution_count < 0:
            raise ContractValidationError("observed_execution_count must not be negative.")
        object.__setattr__(self, "metrics", redact_metadata(self.metrics))
        object.__setattr__(self, "metadata", redact_metadata(self.metadata))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EvidenceEnvelopeV1:
        return cls(**_contract_data(cls, data))


@dataclass(frozen=True, slots=True)
class PerformanceCaseV1(VersionedContract):
    """A repeatable performance comparison case identified without SQL text."""

    contract_type: ClassVar[str] = "PerformanceCaseV1"
    contract_version: int = CONTRACT_VERSION
    case_id: str = field(default_factory=lambda: new_id("case"))
    created_at_utc: str = field(default_factory=utc_now)
    updated_at_utc: str = field(default_factory=utc_now)
    query_fingerprint: str = ""
    database_fingerprint: str | None = None
    baseline_evidence_ids: tuple[str, ...] = ()
    parameter_case_fingerprints: tuple[str, ...] = ()
    status: str = "open"
    version: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_version(self.contract_version)
        _validate_id(self.case_id, "case_id")
        _validate_timestamp(self.created_at_utc, "created_at_utc")
        _validate_timestamp(self.updated_at_utc, "updated_at_utc")
        if not self.query_fingerprint:
            raise ContractValidationError("query_fingerprint must not be empty.")
        _validate_fingerprint(self.query_fingerprint, "query_fingerprint")
        _validate_fingerprint(self.database_fingerprint, "database_fingerprint")
        if self.status not in {"open", "ready", "closed"}:
            raise ContractValidationError(f"Unsupported performance case status: {self.status!r}.")
        if self.version < 0:
            raise ContractValidationError("version must not be negative.")
        object.__setattr__(self, "baseline_evidence_ids", tuple(self.baseline_evidence_ids))
        object.__setattr__(
            self,
            "parameter_case_fingerprints",
            tuple(self.parameter_case_fingerprints),
        )
        object.__setattr__(self, "metadata", redact_metadata(self.metadata))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PerformanceCaseV1:
        values = _contract_data(cls, data)
        for key in ("baseline_evidence_ids", "parameter_case_fingerprints"):
            if key in values:
                values[key] = tuple(values[key])
        return cls(**values)


TERMINAL_CANDIDATE_STATES = frozenset(
    {
        "improved",
        "performance_only",
        "neutral",
        "regressed",
        "equivalence_failed",
        "inconclusive",
        "cleanup_required",
    }
)
NON_TERMINAL_CANDIDATE_STATES = frozenset(
    {"proposed", "screening", "finalist", "validating"}
)
ALL_CANDIDATE_STATES = TERMINAL_CANDIDATE_STATES | NON_TERMINAL_CANDIDATE_STATES


@dataclass(frozen=True, slots=True)
class TuningCandidateV1(VersionedContract):
    """Candidate state whose rewrite is represented by a fingerprint/reference."""

    contract_type: ClassVar[str] = "TuningCandidateV1"
    contract_version: int = CONTRACT_VERSION
    candidate_id: str = field(default_factory=lambda: new_id("candidate"))
    session_id: str = ""
    ordinal: int = 0
    strategy: str = ""
    rewrite_fingerprint: str | None = None
    rewrite_artifact_ref: str | None = None
    state: str = "proposed"
    screen_runs: int = 0
    finalist_runs: int = 0
    parameter_cases: int = 0
    executions: int = 0
    evidence_ids: tuple[str, ...] = ()
    failure_code: str | None = None
    created_at_utc: str = field(default_factory=utc_now)
    updated_at_utc: str = field(default_factory=utc_now)
    version: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_version(self.contract_version)
        _validate_id(self.candidate_id, "candidate_id")
        _validate_id(self.session_id, "session_id")
        _validate_timestamp(self.created_at_utc, "created_at_utc")
        _validate_timestamp(self.updated_at_utc, "updated_at_utc")
        if self.ordinal < 0 or self.version < 0:
            raise ContractValidationError("ordinal and version must not be negative.")
        if self.state not in ALL_CANDIDATE_STATES:
            raise ContractValidationError(f"Unsupported candidate state: {self.state!r}.")
        _validate_fingerprint(self.rewrite_fingerprint, "rewrite_fingerprint")
        _validate_artifact_ref(self.rewrite_artifact_ref)
        for name in ("screen_runs", "finalist_runs", "parameter_cases", "executions"):
            if getattr(self, name) < 0:
                raise ContractValidationError(f"{name} must not be negative.")
        object.__setattr__(self, "evidence_ids", tuple(self.evidence_ids))
        object.__setattr__(self, "metadata", redact_metadata(self.metadata))

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_CANDIDATE_STATES

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TuningCandidateV1:
        values = _contract_data(cls, data)
        if "evidence_ids" in values:
            values["evidence_ids"] = tuple(values["evidence_ids"])
        return cls(**values)


SESSION_STATUSES = frozenset(
    {"created", "screening", "finalist_validation", "completed", "cancelled"}
)


@dataclass(frozen=True, slots=True)
class TuningSessionV1(VersionedContract):
    """Durable state and hard limits for one tuning attempt."""

    contract_type: ClassVar[str] = "TuningSessionV1"
    contract_version: int = CONTRACT_VERSION
    session_id: str = field(default_factory=lambda: new_id("session"))
    performance_case_id: str = ""
    status: str = "created"
    created_at_utc: str = field(default_factory=utc_now)
    updated_at_utc: str = field(default_factory=utc_now)
    started_at_utc: str | None = None
    deadline_at_utc: str | None = None
    max_candidates: int = 10
    screen_runs_per_candidate: int = 3
    finalist_runs_per_candidate: int = 5
    parameter_case_limit: int = 4
    execution_limit: int = 80
    time_limit_seconds: int = 20 * 60
    candidate_ids: tuple[str, ...] = ()
    finalist_candidate_ids: tuple[str, ...] = ()
    selected_candidate_id: str | None = None
    stopping_reason: str | None = None
    replay_metadata: Mapping[str, Any] = field(default_factory=dict)
    version: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_version(self.contract_version)
        _validate_id(self.session_id, "session_id")
        _validate_id(self.performance_case_id, "performance_case_id")
        _validate_timestamp(self.created_at_utc, "created_at_utc")
        _validate_timestamp(self.updated_at_utc, "updated_at_utc")
        for name in (
            "max_candidates",
            "screen_runs_per_candidate",
            "finalist_runs_per_candidate",
            "parameter_case_limit",
            "execution_limit",
            "time_limit_seconds",
        ):
            if getattr(self, name) <= 0:
                raise ContractValidationError(f"{name} must be greater than 0.")
        if self.status not in SESSION_STATUSES:
            raise ContractValidationError(f"Unsupported tuning session status: {self.status!r}.")
        if self.stopping_reason is not None:
            if not isinstance(self.stopping_reason, str) or not self.stopping_reason.strip():
                raise ContractValidationError("stopping_reason must be a non-empty string when set.")
            if len(self.stopping_reason) > 200:
                raise ContractValidationError("stopping_reason must be at most 200 characters.")
        if self.version < 0:
            raise ContractValidationError("version must not be negative.")
        for name in ("started_at_utc", "deadline_at_utc"):
            value = getattr(self, name)
            if value is not None:
                _validate_timestamp(value, name)
        object.__setattr__(self, "candidate_ids", tuple(self.candidate_ids))
        object.__setattr__(self, "finalist_candidate_ids", tuple(self.finalist_candidate_ids))
        object.__setattr__(self, "replay_metadata", redact_metadata(self.replay_metadata))
        object.__setattr__(self, "metadata", redact_metadata(self.metadata))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TuningSessionV1:
        values = _contract_data(cls, data)
        for key in ("candidate_ids", "finalist_candidate_ids"):
            if key in values:
                values[key] = tuple(values[key])
        return cls(**values)


@dataclass(frozen=True, slots=True)
class PlanActionIntentV1(VersionedContract):
    """A database-independent intent for a later MCP-owned plan action."""

    contract_type: ClassVar[str] = "PlanActionIntentV1"
    contract_version: int = CONTRACT_VERSION
    intent_id: str = field(default_factory=lambda: new_id("intent"))
    session_id: str = ""
    candidate_id: str | None = None
    query_fingerprint: str = ""
    rewrite_fingerprint: str | None = None
    action: str = "review"
    status: str = "reviewed"
    expected_candidate_state: str | None = None
    created_at_utc: str = field(default_factory=utc_now)
    updated_at_utc: str = field(default_factory=utc_now)
    version: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_version(self.contract_version)
        _validate_id(self.intent_id, "intent_id")
        _validate_id(self.session_id, "session_id")
        if self.candidate_id is not None:
            _validate_id(self.candidate_id, "candidate_id")
        _validate_timestamp(self.created_at_utc, "created_at_utc")
        _validate_timestamp(self.updated_at_utc, "updated_at_utc")
        if not self.query_fingerprint:
            raise ContractValidationError("query_fingerprint must not be empty.")
        _validate_fingerprint(self.query_fingerprint, "query_fingerprint")
        _validate_fingerprint(self.rewrite_fingerprint, "rewrite_fingerprint")
        if not self.action:
            raise ContractValidationError("action must not be empty.")
        if self.status not in {
            "reviewed",
            "prepared",
            "applying",
            "applied",
            "observing",
            "kept",
            "rolling_back",
            "rolled_back",
            "hold",
            "unknown",
            "rejected",
        }:
            raise ContractValidationError(f"Unsupported plan action status: {self.status!r}.")
        if self.version < 0:
            raise ContractValidationError("version must not be negative.")
        if self.expected_candidate_state is not None and self.expected_candidate_state not in ALL_CANDIDATE_STATES:
            raise ContractValidationError(
                f"Unsupported expected_candidate_state: {self.expected_candidate_state!r}."
            )
        object.__setattr__(self, "metadata", redact_metadata(self.metadata))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PlanActionIntentV1:
        return cls(**_contract_data(cls, data))


CONTRACT_TYPES: dict[str, type[VersionedContract]] = {
    EvidenceEnvelopeV1.contract_type: EvidenceEnvelopeV1,
    PerformanceCaseV1.contract_type: PerformanceCaseV1,
    TuningSessionV1.contract_type: TuningSessionV1,
    TuningCandidateV1.contract_type: TuningCandidateV1,
    PlanActionIntentV1.contract_type: PlanActionIntentV1,
}


def deserialize_contract(data: Mapping[str, Any] | str) -> VersionedContract:
    """Deserialize a supported V1 contract using its explicit contract type."""

    if isinstance(data, str):
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError as exc:
            raise ContractValidationError("Contract JSON is invalid.") from exc
    else:
        parsed = dict(data)
    if not isinstance(parsed, dict):
        raise ContractValidationError("Contract payload must contain an object.")
    contract_type = parsed.get("contract_type")
    if not isinstance(contract_type, str):
        raise ContractValidationError("Contract payload must include contract_type.")
    try:
        contract_cls = CONTRACT_TYPES[contract_type]
    except KeyError as exc:
        raise ContractValidationError(f"Unknown contract_type: {contract_type!r}.") from exc
    return contract_cls.from_dict(parsed)
