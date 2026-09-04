"""Deterministic, redacted Azure SQL index review over the installed SQL contract.

The two history tables are an external prerequisite.  This module never
creates, migrates, updates, or deletes database objects.  Reviews are pure
projections of validated history and are intentionally not persisted.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from .connection import SqlTransactionSession
from .connection import TransactionCommitOutcomeUnknownError
from .database_policy import DatabasePolicySet
from .index_ddl import render_reverse_index_definition
from .index_ddl import render_inert_candidate_rollback
from .index_ddl import render_inert_proposed_drop
from .index_ddl import render_validation_selects
from .index_metadata import ENGINE_START_TIME_SQL
from .index_metadata import DATABASE_INCARNATION_SQL
from .index_metadata import EXISTING_INDEX_METADATA_SQL
from .index_metadata import INDEX_PROTECTION_METADATA_SQL
from .index_metadata import ExistingIndex
from .index_metadata import existing_index_covers_candidate
from .index_metadata import reversible_index_definition_fingerprint
from .index_metadata import parse_existing_index_rows
from .index_metadata import parse_protection_evidence
from .index_recommendations import IndexRecommendationService
from .plans import parse_showplan_index_evidence
from .index_optimizer import score_index_candidate
from .query_store import INDEX_EVIDENCE_QUERY
from .query_store import MODULE_HINTS_SQL
from .query_store import PLAN_GUIDE_HINTS_SQL
from .query_store import QUERY_STORE_QUERY_HINTS_SQL
from .query_store import QUERY_STORE_TEXT_HINTS_SQL
from .query_store import _resolve_index_hints
from .query_store import _hint_source_id
from .query_store import _sha256_text
from .query_store import _extract_statement_subtree_cost
from .query_store import INDEX_CANDIDATE_IMPACT_FLOOR_PCT
from .query_store import INDEX_CANDIDATE_ROW_COUNT_SQL
from .query_store import INDEX_CANDIDATE_WRITE_RATIO_SQL
from .query_store import _INDEX_CANDIDATE_NON_LEAF_MULTIPLIER
from .query_store import _INDEX_CANDIDATE_PAGE_SIZE_BYTES
from .query_store import _INDEX_CANDIDATE_ROW_OVERHEAD
from .query_store import _INDEX_CANDIDATE_SLOT_ARRAY_ENTRY
from .query_store import _INDEX_CANDIDATE_USABLE_PAGE_BYTES
from .query_store import _MAX_INDEX_CANDIDATE_COLUMNS
from .query_store import _build_index_candidate_column_widths_sql


PUBLIC_CONTRACT_VERSION = "2.3.0"
INDEX_HISTORY_SCHEMA_VERSION = "index-history-v1"
INDEX_HISTORY_CONTRACT_VERSION = PUBLIC_CONTRACT_VERSION
INDEX_REVIEW_CONTRACT_VERSION = PUBLIC_CONTRACT_VERSION
INDEX_REVIEW_ALGORITHM_VERSION = "index-review-algorithm-v1"
INDEX_REVIEW_CLASSIFIER_POLICY_VERSION = "index-review-classifier-v1"
INDEX_REVIEW_COLLECTOR_VERSION = "index-review-collector-v1"
INDEX_REVIEW_SKILL = "sql-index-manager"
INDEX_REVIEW_SKILL_VERSION = "1.0.1"
MIN_OBSERVATION_DAYS = 90
MAX_CAPTURE_ROWS = 10_000
MAX_PLAN_XML_CHARS = 4_000_000
SNAPSHOT_REUSE_HOURS = 48
MAX_SUBJECTS = 10_000
MIN_QUERY_STORE_IMPACT_PCT = INDEX_CANDIDATE_IMPACT_FLOOR_PCT

INDEX_REVIEW_STATES = frozenset(
    {"keep", "create_candidate", "consolidate_candidate", "drop_candidate", "observe"}
)
INDEX_REVIEW_OVERALL_STATES = frozenset(
    {"actionable", "no_change", "partial", "inconclusive"}
)

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_CODE_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_REVIEW_ID = re.compile(r"^ir1(?:\.[a-z]+=[A-Za-z0-9_-]+){8}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_RAW_TEXT_KEYS = frozenset(
    {
        "query",
        "query_text",
        "query_sql_text",
        "sql",
        "sql_text",
        "statement",
        "statement_text",
        "module_text",
        "hint_text",
        "plan_xml",
        "query_plan_xml",
        "raw_plan_xml",
        "parameters",
        "parameter_values",
        "values",
        "module_definition",
        "plan_guide_hints",
        "query_hint_text",
        "retained_query_text",
        "source_text",
        "query_plan",
    }
)

_SUBJECT_KEYS = frozenset(
    {
        "subject_kind", "subject_id", "subject_fingerprint", "candidate_fingerprint",
        "object_id", "index_id", "schema_name", "object_name", "table_name", "index_name",
        "definition", "definition_fingerprint", "counter_epoch_fingerprint", "counters",
        "observed_at_utc", "first_observed_at_utc", "last_observed_at_utc", "size_pages",
        "size_bytes", "write_burden", "query_store_references", "protections",
        "missing_signature", "aggregates", "coverage", "key_columns", "include_columns",
        "current_score", "query_store_complete", "recurring_executed", "runtime_interval_ids",
        "positive_runtime_interval_ids",
        "query_ids", "plan_ids", "covered_by", "projected_database_storage_percent", "dmv_only",
        "state", "reason_codes", "removal_gate", "create_gate", "recheck", "candidate_signature",
        "statement_subtree_cost", "execution_count", "impact_pct", "estimated_size_bytes",
        "estimated_size_mb", "table_write_ratio", "write_ratio", "optimizer_score",
        "candidate_fingerprint",
        "survivor_reference", "overlap_relation", "read_count", "request_count", "query_store_executions",
        "runtime_interval_count", "covered_by", "projected_database_storage_percent",
        "statement_subtree_cost", "estimated_index_size_bytes", "material_score",
        "dmv_user_seeks", "dmv_user_scans", "dmv_avg_user_impact", "dmv_avg_total_user_cost",
    }
)
_DEFINITION_KEYS = frozenset(
    {"reversible_definition", "reversible_definition_fingerprint_v1", "definition_fingerprint",
     "reversibility_blockers", "index_type", "key_columns", "include_columns",
     "filter_signature", "is_unique"}
)
_CREATE_DEFINITION_KEYS = frozenset(
    {"index_type", "key_columns", "include_columns", "filter_signature", "is_unique"}
)
_REVERSIBLE_KEYS = frozenset(
    {"version", "object_id", "parent_object_type", "parent_object_type_code", "schema", "table",
     "index_id", "index_name", "index_type", "index_type_code", "is_primary_key",
     "is_unique_constraint", "constraint_name", "constraint_type", "is_disabled", "is_hypothetical",
     "is_auto_created", "key_columns", "include_columns", "nonkey_columns", "filter", "is_unique",
     "is_padded", "fill_factor", "ignore_dup_key", "statistics_no_recompute", "statistics_incremental",
     "allow_row_locks", "allow_page_locks", "optimize_for_sequential_key", "suppress_dup_key_messages",
     "data_space", "partition_compression", "xml_compression"}
)
_FILTER_KEYS = frozenset({"has_filter", "definition"})
_DATA_SPACE_KEYS = frozenset({"name", "type", "partition_scheme", "partition_function", "partition_columns"})
_PAIR_KEYS = frozenset({"partition_number", "compression"})
_KEY_COLUMN_KEYS = frozenset({"name", "direction"})
_COUNTER_KEYS = frozenset({"user_seeks", "user_scans", "user_lookups", "user_updates"})
_REFERENCE_KEYS = frozenset(
    {"database_name", "database", "schema_name", "schema", "object_name", "object", "index_name", "index",
     "operator_kind", "operator_kinds", "query_id", "query_ids", "plan_id", "plan_ids", "is_forced_plan", "plan_hash",
     "plan_fingerprint", "execution_count", "runtime_interval_ids", "last_seen", "executed"}
)
_MISSING_SIGNATURE_KEYS = frozenset(
    {"schema_name", "table_name", "equality_columns", "inequality_columns", "include_columns",
     "filter_signature", "index_type"}
)
_AGGREGATE_KEYS = frozenset(
    {"read_count", "write_burden", "current_score", "request_count", "query_store_executions",
     "runtime_interval_count", "covered_by", "projected_database_storage_percent", "statement_subtree_cost",
     "execution_count", "impact_pct", "estimated_index_size_bytes", "table_write_ratio", "material_score",
     "dmv_user_seeks", "dmv_user_scans", "dmv_avg_user_impact", "dmv_avg_total_user_cost",
     "candidate_signature", "candidate_fingerprint", "estimated_size_bytes", "estimated_size_mb",
     "estimated_index_size_mb", "write_ratio", "optimizer_score", "positive_runtime_interval_ids",
     "dmv_only"}
)
_COVERAGE_KEYS = frozenset(
    {"status", "blockers", "malformed", "eligible", "scanned", "capped", "truncated", "source_count",
     "sources", "query_store", "hints", "hint", "dependency", "protection", "usage", "storage", "gaps",
     "window_start_utc", "window_end_utc", "retention_days", "stale_query_threshold_days",
     "capture_mode", "state", "runtime_intervals", "runtime_interval_count", "required",
     "covered", "reason", "error_type", "requested_window_minutes", "required_window_minutes",
     "plan_evidence_empty_allowed", "interval_count", "plan_evidence", "runtime_window",
     "engine", "database_incarnation"}
)
_PROTECTION_KEYS = frozenset(
    {"object_id", "index_id", "coverage", "blockers", "primary_key", "unique_constraint", "standalone_unique",
     "clustered", "indexed_view", "disabled", "hypothetical", "auto_created", "has_index_extended_properties",
     "extended_properties", "referenced_foreign_key_key_index_ids", "referenced_foreign_keys",
     "child_foreign_key_support", "automatic_tuning", "hinted_or_forced_plan", "partition_switch_dependency",
     "specialist_type", "safe_to_remove"}
)
_QUERY_STORE_KEYS = frozenset(
    {"state", "capture_mode", "enabled", "complete", "database_name", "coverage", "query_count",
     "executed_count", "window_minutes", "runtime_window", "candidate_count", "index_references",
     "executed_plan_references", "missing_index_candidates", "candidates", "retention_days",
     "stale_query_threshold_days", "runtime_interval_count", "window_start_utc", "window_end_utc"}
)
_OBSERVATION_KEYS = frozenset(
    {"snapshot_count", "as_of_observed_at_utc", "minimum_observation_days", "business_cycle_extension_days",
     "history_fingerprint", "state_counts", "recommend_only", "history_status", "next_action", "stale_hours",
     "gaps", "database_incarnation_fingerprint", "engine_identity", "engine_start_time_utc",
     "observed_snapshot_count", "expected_snapshot_count", "observation_days", "elapsed_hours",
     "required_elapsed_hours", "first_run", "no_gap_over_48_hours", "daily_continuity",
     "max_gap_hours", "stable_engine", "stable_database_incarnation", "stable_counter_epoch",
     "stable_definition", "enough_observation_days", "complete_subject_history",
     "complete_usable_counter_coverage", "reset_detected", "physical_database_change",
     "engine_epoch_change", "usage_window_covers_history"}
)

_GATE_KEYS = frozenset(
    {
        "enabled_user_created", "nonunique", "standalone_type_2_rowstore", "fully_reversible",
        "stable_engine_and_database", "stable_counter_epoch", "stable_definition",
        "continuous_usable_days", "complete_usable_counter_coverage", "counters_never_decrease",
        "zero_seek_scan_lookup_deltas", "measurable_write_or_storage_burden",
        "query_store_coverage_complete", "hint_coverage_complete", "dependency_coverage_complete",
        "protection_coverage_complete", "not_protected", "no_foreign_key_dependency",
        "no_historical_query_store_execution_reference", "no_stored_plan_without_execution",
        "exact_request_recurs_in_two_runtime_intervals", "executed_plan_source",
        "exact_filter_reconstruction_available", "impact_pct_at_or_above_existing_floor",
        "material_positive_existing_mcp_score", "scoring_inputs_complete", "no_exact_or_covering_index",
        "projected_storage_strictly_below_90_percent", "complete_evidence", "positive_execution_evidence",
    }
)
_COVERAGE_SOURCE_KEYS = frozenset(
    {"query_store_text", "query_store_query_hints", "plan_guides", "module_definitions", "plan_evidence",
     "runtime_window", "missing_index_dmv", "storage", "protection", "usage", "hints"}
)
_GAP_KEYS = frozenset({"from_utc", "to_utc", "hours", "left_run_id", "right_run_id", "reason"})
_RUNTIME_INTERVAL_KEYS = frozenset({"runtime_interval_id", "start_time", "end_time", "execution_count"})
_FOREIGN_KEY_REFERENCE_KEYS = frozenset({"foreign_key_id", "key_index_id"})
_QUERY_STORE_CANDIDATE_KEYS = frozenset(
    {
        "candidate_signature", "candidate_fingerprint", "database_name", "schema_name", "object_name", "table_name", "database", "schema",
        "object", "key_signature", "include_signature", "filter_signature", "equality_columns",
        "inequality_columns", "include_columns", "included_columns", "query_id", "query_ids",
        "plan_id", "plan_ids", "is_forced_plan", "plan_hash", "plan_fingerprint", "execution_count",
        "runtime_interval_ids", "last_seen", "impact_pct", "statement_subtree_cost", "estimated_size_mb",
        "estimated_size_bytes", "estimated_index_size_mb", "write_ratio", "current_score", "optimizer_score", "request_count",
        "recurring", "scoring_blockers", "dmv_only", "table_write_ratio", "query_store_executions",
        "positive_runtime_interval_ids",
    }
)


class IndexReviewError(RuntimeError):
    """Base class for index review failures."""


class IndexReviewPolicyError(IndexReviewError):
    """The local database policy denied the requested operation."""


class IndexReviewSchemaError(IndexReviewError):
    """The manually installed history contract is absent or invalid."""


class IndexReviewIntegrityError(IndexReviewError):
    """Persisted history failed an integrity or redaction check."""


class IndexReviewCollectionError(IndexReviewError):
    """Read-only telemetry collection failed before a commit."""


class IndexReviewWriteError(IndexReviewError):
    """A pre-commit history write failed and was rolled back."""


class IndexReviewOutcomeUnknownError(IndexReviewError):
    """A committed capture could not be reconciled by one fresh read."""


class IndexReviewNotFoundError(IndexReviewError, KeyError):
    """A requested capture or review selector is not present."""


class IndexReviewIdempotencyConflictError(IndexReviewError):
    """An idempotency hash was already bound to different request material."""


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(prefix: str, value: Any) -> str:
    return hashlib.sha256(f"{prefix}:{_canonical(value)}".encode("utf-8")).hexdigest()


def _identifier(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text or not _IDENTIFIER.fullmatch(text):
        raise ValueError(f"{field_name} must be a bounded identifier.")
    return text


def _timestamp(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an ISO timestamp with timezone.") from exc
    if parsed.tzinfo is None:
        # Azure SQL datetime2 values are returned by drivers without an offset;
        # this contract stores them as UTC by definition.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any) -> datetime:
    return datetime.fromisoformat(_timestamp(value, "timestamp").replace("Z", "+00:00"))


def _parse_utc_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        return _parse_time(value)
    except (TypeError, ValueError):
        return None


_SCHEMA_ALLOWED_KEYS: dict[str, frozenset[str]] = {
    "subject": _SUBJECT_KEYS,
    "definition": _DEFINITION_KEYS,
    "create_definition": _CREATE_DEFINITION_KEYS,
    "reversible": _REVERSIBLE_KEYS,
    "filter": _FILTER_KEYS,
    "data_space": _DATA_SPACE_KEYS,
    "pair": _PAIR_KEYS,
    "key_column": _KEY_COLUMN_KEYS,
    "counters": _COUNTER_KEYS,
    "reference": _REFERENCE_KEYS,
    "missing_signature": _MISSING_SIGNATURE_KEYS,
    "aggregates": _AGGREGATE_KEYS,
    "coverage": _COVERAGE_KEYS,
    "protections": _PROTECTION_KEYS,
    "query_store": _QUERY_STORE_KEYS,
    "observation": _OBSERVATION_KEYS,
    "state_counts": INDEX_REVIEW_STATES,
    "recheck": frozenset({"is_recheck", "prior_state", "transition"}),
    "gate": frozenset(
        {
            "passed", "gates", "blockers", "observation", "read_count", "write_burden",
            "historical_reference",
            "exact_request_recurs_in_two_runtime_intervals", "executed_plan_source",
            "exact_filter_reconstruction_available", "material_positive_existing_mcp_score",
            "no_exact_or_covering_index", "projected_storage_strictly_below_90_percent",
            "complete_evidence",
        }
    ) | _GATE_KEYS,
    "gate_map": _GATE_KEYS,
    "observation_gate": _OBSERVATION_KEYS,
    "coverage_sources": _COVERAGE_SOURCE_KEYS,
    "gap": _GAP_KEYS,
    "runtime_interval": _RUNTIME_INTERVAL_KEYS,
    "foreign_key_reference": _FOREIGN_KEY_REFERENCE_KEYS,
    "query_store_candidate": _QUERY_STORE_CANDIDATE_KEYS,
    "child_fk": frozenset({"foreign_key_id", "child_object_id", "leading_key_supported", "constraint_ordinals", "index_key_ordinals", "metadata_incomplete", "blocker"}),
    "scalar": frozenset(),
}

_SCHEMA_CONTAINER_TYPES: dict[tuple[str, str], str] = {
    ("subject", "definition"): "mapping",
    ("subject", "counters"): "mapping",
    ("subject", "protections"): "mapping",
    ("subject", "query_store_references"): "list",
    ("subject", "missing_signature"): "mapping",
    ("subject", "aggregates"): "mapping",
    ("subject", "coverage"): "mapping",
    ("subject", "removal_gate"): "mapping",
    ("subject", "create_gate"): "mapping",
    ("subject", "recheck"): "mapping",
    ("subject", "query_ids"): "list",
    ("subject", "plan_ids"): "list",
    ("subject", "runtime_interval_ids"): "list",
    ("subject", "key_columns"): "list",
    ("subject", "include_columns"): "list",
    ("subject", "positive_runtime_interval_ids"): "list",
    ("subject", "covered_by"): "list",
    ("definition", "reversible_definition"): "mapping",
    ("create_definition", "key_columns"): "list",
    ("create_definition", "include_columns"): "list",
    ("reversible", "filter"): "mapping",
    ("reversible", "data_space"): "mapping",
    ("reversible", "key_columns"): "list",
    ("reversible", "include_columns"): "list",
    ("reversible", "nonkey_columns"): "list",
    ("reversible", "partition_compression"): "list",
    ("reversible", "xml_compression"): "list",
    ("data_space", "partition_columns"): "list",
    ("reference", "operator_kinds"): "list",
    ("reference", "query_ids"): "list",
    ("reference", "plan_ids"): "list",
    ("reference", "runtime_interval_ids"): "list",
    ("missing_signature", "equality_columns"): "list",
    ("missing_signature", "inequality_columns"): "list",
    ("missing_signature", "include_columns"): "list",
    ("aggregates", "covered_by"): "list",
    ("aggregates", "positive_runtime_interval_ids"): "list",
    ("coverage", "sources"): "mapping",
    ("coverage", "gaps"): "list",
    ("coverage", "runtime_intervals"): "list",
    ("coverage", "runtime_window"): "mapping",
    ("coverage_sources", "runtime_window"): "mapping",
    ("query_store", "coverage"): "mapping",
    ("query_store", "runtime_window"): "mapping",
    ("query_store", "index_references"): "list",
    ("query_store", "executed_plan_references"): "list",
    ("query_store", "missing_index_candidates"): "list",
    ("query_store", "candidates"): "list",
    ("query_store_candidate", "query_ids"): "list",
    ("query_store_candidate", "plan_ids"): "list",
    ("query_store_candidate", "runtime_interval_ids"): "list",
    ("query_store_candidate", "equality_columns"): "list",
    ("query_store_candidate", "inequality_columns"): "list",
    ("query_store_candidate", "include_columns"): "list",
    ("query_store_candidate", "included_columns"): "list",
    ("query_store_candidate", "scoring_blockers"): "list",
    ("protections", "child_foreign_key_support"): "list",
    ("protections", "referenced_foreign_keys"): "list",
    ("protections", "referenced_foreign_key_key_index_ids"): "list",
    ("child_fk", "constraint_ordinals"): "list",
    ("child_fk", "index_key_ordinals"): "list",
    ("observation", "state_counts"): "mapping",
    ("gate", "gates"): "mapping",
    ("gate", "observation"): "mapping",
}


def _child_payload_schema(schema: str | None, key: str) -> str | None:
    if schema == "subject":
        return {
            "definition": "definition", "counters": "counters", "protections": "protections",
            "query_store_references": "reference", "missing_signature": "missing_signature",
            "aggregates": "aggregates", "coverage": "coverage", "removal_gate": "gate",
            "create_gate": "gate", "recheck": "recheck", "query_ids": "scalar", "plan_ids": "scalar",
            "runtime_interval_ids": "scalar", "key_columns": "scalar", "include_columns": "scalar",
            "positive_runtime_interval_ids": "scalar",
        }.get(key)
    if schema == "create_definition" and key in {"key_columns", "include_columns"}:
        return "scalar"
    if schema == "definition" and key == "reversible_definition":
        return "reversible"
    if schema == "reversible":
        return {
            "filter": "filter", "data_space": "data_space",
            "key_columns": "key_column", "include_columns": "scalar", "nonkey_columns": "scalar",
            "partition_compression": "pair",
            "xml_compression": "pair",
        }.get(key)
    if schema == "data_space" and key == "partition_columns":
        return "scalar"
    if schema == "reference" and key in {
        "operator_kinds", "query_ids", "plan_ids", "runtime_interval_ids"
    }:
        return "scalar"
    if schema == "missing_signature" and key in {
        "equality_columns", "inequality_columns", "include_columns"
    }:
        return "scalar"
    if schema == "aggregates" and key in {
        "covered_by", "positive_runtime_interval_ids"
    }:
        return "scalar"
    if schema == "coverage":
        if key == "sources":
            return "coverage_sources"
        if key == "gaps":
            return "gap"
        if key == "runtime_intervals":
            return "runtime_interval"
        if key == "runtime_window":
            return "coverage"
        if key in {"query_store", "hints", "hint", "dependency", "protection", "usage", "storage", "missing_index_dmv"}:
            return "coverage"
    if schema == "coverage_sources":
        return "coverage"
    if schema == "query_store" and key in {"coverage", "runtime_window"}:
        return "coverage"
    if schema == "query_store" and key in {"index_references", "executed_plan_references"}:
        return "reference"
    if schema == "query_store" and key in {"missing_index_candidates", "candidates"}:
        return "query_store_candidate"
    if schema == "query_store_candidate" and key in {
        "query_ids", "plan_ids", "runtime_interval_ids", "equality_columns",
        "inequality_columns", "include_columns", "included_columns", "scoring_blockers",
    }:
        return "scalar"
    if schema == "protections" and key == "child_foreign_key_support":
        return "child_fk"
    if schema == "protections" and key == "referenced_foreign_keys":
        return "foreign_key_reference"
    if schema == "protections" and key == "referenced_foreign_key_key_index_ids":
        return "scalar"
    if schema == "child_fk" and key in {"constraint_ordinals", "index_key_ordinals"}:
        return "scalar"
    if schema == "observation" and key == "state_counts":
        return "state_counts"
    if schema == "gate" and key == "gates":
        return "gate_map"
    if schema == "gate" and key == "observation":
        return "observation_gate"
    return None


def _safe_payload(
    value: Any,
    *,
    path: str = "payload",
    reversible: bool = False,
    schema: str | None = None,
) -> Any:
    """Copy one exact persisted JSON schema and reject all unknown material."""

    if schema is None and reversible:
        schema = "reversible"
    if isinstance(value, Mapping):
        if schema is None:
            raise IndexReviewIntegrityError(
                f"{path} is an untyped nested map and is not allowed."
            )
        result: dict[str, Any] = {}
        allowed = _SCHEMA_ALLOWED_KEYS.get(schema)
        if allowed is None:
            raise IndexReviewIntegrityError(f"{path} has no assigned persisted schema.")
        for raw_key, raw_value in value.items():
            key = str(raw_key)
            normalized = key.casefold()
            field_path = f"{path}.{key}"
            if allowed is not None and key not in allowed:
                raise IndexReviewIntegrityError(f"{field_path} is not an allowed persisted field.")
            if key in {"blockers", "reason_codes", "reversibility_blockers", "scoring_blockers"}:
                if not isinstance(raw_value, (list, tuple)):
                    raise IndexReviewIntegrityError(f"{field_path} must be a list of code identifiers.")
                if any(
                    not isinstance(code, str) or _CODE_IDENTIFIER.fullmatch(code) is None
                    for code in raw_value
                ):
                    raise IndexReviewIntegrityError(
                        f"{field_path} contains a non-code value."
                    )
            if normalized in _RAW_TEXT_KEYS:
                raise IndexReviewIntegrityError(f"{field_path} contains prohibited raw material.")
            if normalized == "filter_definition":
                raise IndexReviewIntegrityError(
                    f"{field_path} is not the structurally scoped filter.definition field."
                )
            if normalized == "definition" and schema != "filter":
                if schema != "subject" or not isinstance(raw_value, Mapping):
                    raise IndexReviewIntegrityError(f"{field_path} contains prohibited raw text material.")
            expected_container = _SCHEMA_CONTAINER_TYPES.get((schema, key))
            if expected_container == "mapping" and raw_value is None and not (
                schema == "subject" and key == "missing_signature"
            ):
                raise IndexReviewIntegrityError(f"{field_path} must be an object.")
            if expected_container == "mapping" and raw_value is not None and not isinstance(raw_value, Mapping):
                raise IndexReviewIntegrityError(f"{field_path} must be an object.")
            if expected_container == "list" and not isinstance(raw_value, (list, tuple)):
                raise IndexReviewIntegrityError(f"{field_path} must be an array.")
            if schema == "coverage" and key in {
                "query_store", "hints", "hint", "dependency", "protection",
                "usage", "storage", "missing_index_dmv",
            } and not isinstance(raw_value, (str, Mapping)):
                raise IndexReviewIntegrityError(
                    f"{field_path} must be a coverage status or object."
                )
            if schema == "filter" and key == "definition" and raw_value is not None and not isinstance(raw_value, str):
                raise IndexReviewIntegrityError(f"{field_path} must be predicate text or null.")
            child_schema = _child_payload_schema(schema, key)
            if schema == "subject" and key == "definition":
                child_schema = (
                    "definition"
                    if isinstance(raw_value, Mapping)
                    and "reversible_definition" in raw_value
                    else "create_definition"
                )
            result[key] = _safe_payload(
                raw_value,
                path=field_path,
                reversible=reversible or child_schema == "reversible",
                schema=child_schema,
            )
            if schema == "missing_signature" and key == "filter_signature" and raw_value not in (None, ""):
                raise IndexReviewIntegrityError(
                    f"{field_path} must be a null structural signature, not predicate text."
                )
        return result
    if isinstance(value, (list, tuple)):
        if schema not in {None, "scalar"} and any(
            not isinstance(item, Mapping) for item in value
        ):
            raise IndexReviewIntegrityError(f"{path} must contain objects.")
        return [
            _safe_payload(item, path=f"{path}[]", reversible=reversible, schema=schema)
            for item in value
        ]
    if isinstance(value, datetime):
        return _timestamp(value.isoformat(), path)
    if isinstance(value, str) and re.search(r"<\s*showplanxml\b", value, re.IGNORECASE):
        raise IndexReviewIntegrityError(f"{path} contains raw plan XML.")
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise IndexReviewIntegrityError(f"{path} contains an unsupported value.")


def _as_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "on"}
    return bool(value)


def _mapping_value(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _is_nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _reversible_definition(subject: Mapping[str, Any]) -> Mapping[str, Any]:
    definition = _mapping_value(subject.get("definition"))
    return _mapping_value(definition.get("reversible_definition"))


def _subject_definition_identity(subject: Mapping[str, Any]) -> str | None:
    reversible = _reversible_definition(subject)
    if reversible:
        try:
            return reversible_index_definition_fingerprint(reversible)
        except (TypeError, ValueError, KeyError):
            return None
    value = subject.get("definition_fingerprint")
    return value if isinstance(value, str) else None


def _reversible_definition_fingerprint(definition: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        _canonical(
            {"version": "reversible_definition_fingerprint_v1", "definition": definition}
        ).encode("utf-8")
    ).hexdigest()


def _definition_fingerprint_from_reversible(definition: Mapping[str, Any]) -> str:
    try:
        return reversible_index_definition_fingerprint(definition)
    except (TypeError, ValueError, KeyError) as exc:
        raise IndexReviewIntegrityError("Reversible index definition is malformed.") from exc


def daily_idempotency_key(database_fingerprint_value: str, now_utc: datetime | None = None) -> str:
    moment = now_utc or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    normalized_fingerprint = str(database_fingerprint_value).strip().casefold()
    if not _HEX64.fullmatch(normalized_fingerprint):
        raise ValueError("database_fingerprint must be a SHA-256 hexadecimal fingerprint.")
    return f"index-review:{normalized_fingerprint}:{moment.astimezone(timezone.utc).date().isoformat()}"


def idempotency_key_hash(database_fingerprint_value: str, key: str) -> str:
    if not isinstance(key, str) or not key.strip() or len(key) > 512:
        raise ValueError("idempotency_key must be non-empty and at most 512 characters.")
    return _digest(
        "index-history-idempotency-v1",
        {"database_fingerprint": str(database_fingerprint_value).strip().casefold(), "key": key},
    )


def database_fingerprint(server: str, database_name: str) -> str:
    return _digest(
        "azure-sql-database-fingerprint-v1",
        {"server": str(server).strip().casefold(), "database": database_name.strip().casefold()},
    )


def database_incarnation_fingerprint(identity: str) -> str:
    normalized = str(identity or "").strip().casefold()
    if not normalized or len(normalized) > 128:
        raise ValueError("database incarnation identity must be bounded and non-empty.")
    return _digest("azure-sql-database-incarnation-v1", normalized)


def engine_fingerprint(identity: str, start_time_utc: Any) -> str:
    normalized_identity = str(identity or "").strip().casefold()
    normalized_start = _timestamp(start_time_utc, "engine_start_time_utc")
    if not normalized_identity or len(normalized_identity) > 128:
        raise ValueError("engine identity must be bounded and non-empty.")
    return _digest(
        "azure-sql-engine-epoch-v1",
        {"identity": normalized_identity, "start_time_utc": normalized_start},
    )


def _compact(value: str) -> str:
    raw = (
        bytes.fromhex(value)
        if _HEX64.fullmatch(value)
        else hashlib.sha256(value.encode("utf-8")).digest()
    )
    return base64.b32encode(raw).decode("ascii").rstrip("=").lower()[:26]


def _subject_fingerprint(subject_kind: str, subject: Mapping[str, Any]) -> str:
    if subject_kind == "existing_index":
        material = {
            "subject_id": subject.get("subject_id"),
            "definition": subject.get("definition"),
        }
    else:
        material = {
            "subject_id": subject.get("subject_id"),
            "missing_signature": subject.get("missing_signature"),
        }
    return _digest("index-review-subject-v2", material)


def _snapshot_set_fingerprint(subjects: Sequence[Mapping[str, Any]]) -> str:
    manifest = [
        {
            "subject_id": subject.get("subject_id"),
            "subject_kind": subject.get("subject_kind"),
            "subject_fingerprint": subject.get("subject_fingerprint"),
        }
        for subject in sorted(subjects, key=lambda item: str(item.get("subject_id", "")))
    ]
    return _digest("index-review-snapshot-set-v1", manifest)


def _snapshot_row_material(subject: Mapping[str, Any]) -> dict[str, Any]:
    """Return only the fields represented by one persisted snapshot row."""

    return {
        name: subject.get(name)
        for name in (
            "subject_id",
            "subject_kind",
            "subject_fingerprint",
            "object_id",
            "index_id",
            "schema_name",
            "object_name",
            "index_name",
            "definition",
            "definition_fingerprint",
            "counter_epoch_fingerprint",
            "counters",
            "observed_at_utc",
            "first_observed_at_utc",
            "last_observed_at_utc",
            "size_pages",
            "size_bytes",
            "write_burden",
            "query_store_references",
            "protections",
            "missing_signature",
            "aggregates",
            "coverage",
        )
    }


def _snapshot_row_id(run_id: str, subject: Mapping[str, Any]) -> str:
    return f"{run_id}:subject:{_digest('index-review-snapshot-row-v1', _snapshot_row_material(subject))[:32]}"


def _inventory_fingerprint(subjects: Sequence[Mapping[str, Any]]) -> str:
    return _digest(
        "index-review-inventory-v1",
        [
            {
                "subject_id": item.get("subject_id"),
                "subject_fingerprint": item.get("subject_fingerprint"),
            }
            for item in sorted(
                (subject for subject in subjects if subject.get("subject_kind") == "existing_index"),
                key=lambda item: str(item.get("subject_id", "")),
            )
        ],
    )


def _redacted_index(
    index: ExistingIndex,
    *,
    query_store_references: Sequence[Mapping[str, Any]] = (),
    hint_coverage: Mapping[str, Any] | None = None,
    query_store_coverage: Mapping[str, Any] | None = None,
    hint_evidence: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    protection = _safe_payload(index.protection_evidence, schema="protections")
    hinted = any(
        isinstance(resolved, Mapping)
        and _as_optional_int(resolved.get("object_id")) == index.object_id
        and _as_optional_int(resolved.get("index_id")) == index.index_id
        for evidence in hint_evidence
        for resolved in evidence.get("resolved_indexes", ())
    )
    hint_status = str((hint_coverage or {}).get("status", "incomplete"))
    protection["hinted_or_forced_plan"] = (
        True if hinted else False if hint_status == "complete" else None
    )
    counters = {
        key: index.usage.get(key)
        for key in ("user_seeks", "user_scans", "user_lookups", "user_updates")
    }
    size_pages = sum(
        value for _partition, value in index.partition_page_counts if value is not None and value >= 0
    )
    reversible = _safe_payload(index.reversible_definition, schema="reversible")
    definition = {
        "reversible_definition": reversible,
        "reversible_definition_fingerprint_v1": index.reversible_definition_fingerprint_v1,
        "definition_fingerprint": index.definition_fingerprint,
        "reversibility_blockers": list(index.reversibility_blockers),
    }
    references = [_safe_payload(dict(item), schema="reference") for item in query_store_references]
    coverage = {
        "protection": protection.get("coverage", "incomplete"),
        "query_store": (query_store_coverage or {}).get("status", "incomplete"),
        "hint": (hint_coverage or {}).get("status", "incomplete"),
        "usage": index.usage_context.get("coverage", "incomplete"),
        "dependency": (
            "incomplete"
            if protection.get("partition_switch_dependency") is None
            else "complete"
        ),
        "malformed": [],
    }
    subject: dict[str, Any] = {
        "subject_kind": "existing_index",
        "subject_id": f"index:{index.object_id}:{index.index_id}",
        "schema_name": index.schema,
        "object_name": index.table,
        "table_name": index.table,
        "index_id": index.index_id,
        "object_id": index.object_id,
        "index_name": index.name,
        "definition": definition,
        "definition_fingerprint": index.definition_fingerprint,
        "counters": counters,
        "counter_epoch_fingerprint": index.usage_context.get("counter_epoch_fingerprint"),
        "observed_at_utc": index.provenance.get("collected_at_utc"),
        "size_pages": size_pages,
        "size_bytes": size_pages * 8192,
        "write_burden": counters["user_updates"],
        "query_store_references": references,
        "protections": protection,
        "missing_signature": None,
        "aggregates": {
            "read_count": (
                sum(counters[key] or 0 for key in ("user_seeks", "user_scans", "user_lookups"))
                if all(counters[key] is not None for key in ("user_seeks", "user_scans", "user_lookups"))
                else None
            ),
            "write_burden": counters["user_updates"],
        },
        "coverage": coverage,
    }
    subject["subject_fingerprint"] = _subject_fingerprint("existing_index", subject)
    return subject


def _unquote_column_name(value: Any) -> str:
    text = str(value).strip()
    if len(text) < 2 or not text.startswith("[") or not text.endswith("]"):
        return text
    result: list[str] = []
    position = 1
    while position < len(text) - 1:
        character = text[position]
        if character == "]":
            if position + 1 < len(text) - 1 and text[position + 1] == "]":
                result.append("]")
                position += 2
                continue
            return text
        result.append(character)
        position += 1
    return "".join(result)


def _split_column_names(value: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    in_brackets = False
    position = 0
    while position < len(value):
        character = value[position]
        if character == "[" and not in_brackets:
            in_brackets = True
        elif character == "]" and in_brackets:
            if position + 1 < len(value) and value[position + 1] == "]":
                current.extend(("]", "]"))
                position += 2
                continue
            in_brackets = False
        elif character == "," and not in_brackets:
            item = "".join(current).strip()
            if item:
                items.append(item)
            current = []
            position += 1
            continue
        current.append(character)
        position += 1
    item = "".join(current).strip()
    if item:
        items.append(item)
    return items


def _column_names(value: Any) -> list[str]:
    raw_items = (
        [str(item).strip() for item in value]
        if isinstance(value, (list, tuple))
        else _split_column_names(str(value))
        if value
        else []
    )
    return [name for item in raw_items if (name := _unquote_column_name(item))]


def _candidate_signature(row: Mapping[str, Any]) -> str:
    return _digest(
        "index-review-create-definition-v1",
        {
            "schema_name": str(row.get("schema_name") or "dbo"),
            "table_name": str(row.get("table_name") or row.get("object_name") or ""),
            "equality_columns": _column_names(row.get("equality_columns")),
            "inequality_columns": _column_names(row.get("inequality_columns")),
            "include_columns": _column_names(row.get("include_columns") or row.get("included_columns")),
            "filter_signature": row.get("filter_signature"),
            "index_type": "NONCLUSTERED",
        },
    )


def _redacted_candidate(
    row: Mapping[str, Any],
    *,
    query_store_coverage: Mapping[str, Any],
    existing_indexes: Sequence[ExistingIndex],
    storage: Mapping[str, Any],
) -> dict[str, Any]:
    equality = _column_names(row.get("equality_columns"))
    inequality = _column_names(row.get("inequality_columns"))
    includes = _column_names(row.get("include_columns") or row.get("included_columns"))
    keys = equality + inequality
    schema = str(row.get("schema_name") or "dbo")
    table = str(row.get("table_name") or row.get("object_name") or "")
    exact_signature = str(row.get("candidate_signature") or _candidate_signature(row))
    signature = {
        "schema_name": schema,
        "table_name": table,
        "equality_columns": equality,
        "inequality_columns": inequality,
        "include_columns": includes,
        "filter_signature": row.get("filter_signature"),
        "index_type": "NONCLUSTERED",
    }
    candidate_id = _digest("index-review-missing-request-v1", signature)
    score = next(
        (
            _as_float(row.get(key))
            for key in ("optimizer_score", "current_score", "estimated_improvement", "impact_score", "avg_user_impact")
            if _as_float(row.get(key)) is not None
        ),
        None,
    )
    intervals = sorted(
        {
            _as_int(value)
            for value in row.get("runtime_interval_ids", ())
            if _as_int(value) > 0
        }
    )
    positive_intervals = sorted(
        {
            _as_int(value)
            for value in row.get("positive_runtime_interval_ids", intervals)
            if _as_int(value) > 0
        }
    )
    estimated_size_bytes = _as_optional_int(row.get("estimated_size_bytes"))
    current_percent = _as_float(storage.get("used_percent"))
    max_size_bytes = _as_optional_int(storage.get("max_size_bytes"))
    projected_percent = _as_float(row.get("projected_database_storage_percent"))
    if (
        current_percent is not None
        and max_size_bytes is not None
        and max_size_bytes > 0
        and estimated_size_bytes is not None
    ):
        projected_percent = projected_percent if projected_percent is not None else current_percent + estimated_size_bytes / max_size_bytes * 100.0
    covered_by = [
        index.name
        for index in existing_indexes
        if existing_index_covers_candidate(
            index,
            schema=schema,
            table=table,
            key_columns=keys,
            include_columns=includes,
            filter_definition=None,
            exact_catalog_names=True,
        )
    ]
    dmv_only = _as_bool(row.get("dmv_only"))
    query_store_references = [] if dmv_only else [
        {
            "query_ids": sorted({_as_int(value) for value in row.get("query_ids", ()) if _as_int(value) > 0}),
            "plan_ids": sorted({_as_int(value) for value in row.get("plan_ids", ()) if _as_int(value) > 0}),
            "runtime_interval_ids": intervals,
            "is_forced_plan": _as_bool(row.get("is_forced_plan")),
            "execution_count": _as_int(row.get("execution_count")),
        }
    ]
    subject: dict[str, Any] = {
        "subject_kind": "missing_index",
        "subject_id": f"missing:{candidate_id}",
        "candidate_fingerprint": candidate_id,
        "candidate_signature": exact_signature,
        "schema_name": schema,
        "object_name": table,
        "table_name": table,
        "index_name": None,
        "object_id": _as_optional_int(row.get("object_id")),
        "index_id": None,
        "definition": {
            "index_type": "NONCLUSTERED",
            "key_columns": keys,
            "include_columns": includes,
            "filter_signature": row.get("filter_signature"),
            "is_unique": False,
        },
        "definition_fingerprint": _digest("index-review-create-definition-v1", signature),
        "counters": {},
        "counter_epoch_fingerprint": None,
        "observed_at_utc": row.get("last_seen"),
        "size_pages": None,
        "size_bytes": estimated_size_bytes,
        "write_burden": None,
        "query_store_references": query_store_references,
        "protections": {},
        "missing_signature": signature,
        "aggregates": {
            "current_score": score,
            "request_count": _as_int(row.get("request_count")),
            "query_store_executions": _as_int(row.get("execution_count", row.get("query_store_executions"))),
            "runtime_interval_count": len(intervals),
            "covered_by": covered_by,
            "projected_database_storage_percent": projected_percent,
            "statement_subtree_cost": _as_float(row.get("statement_subtree_cost")),
            "execution_count": _as_float(row.get("execution_count")),
            "impact_pct": _as_float(row.get("impact_pct")),
            "estimated_index_size_bytes": estimated_size_bytes,
            "estimated_index_size_mb": (
                _as_float(row.get("estimated_size_mb"))
                if _as_float(row.get("estimated_size_mb")) is not None
                else estimated_size_bytes / (1024.0 * 1024.0)
                if estimated_size_bytes is not None
                else None
            ),
            "table_write_ratio": _as_float(row.get("write_ratio")),
            "candidate_signature": exact_signature,
            "candidate_fingerprint": candidate_id,
            "positive_runtime_interval_ids": positive_intervals,
            "estimated_size_bytes": estimated_size_bytes,
            "estimated_size_mb": (
                _as_float(row.get("estimated_size_mb"))
                if _as_float(row.get("estimated_size_mb")) is not None
                else estimated_size_bytes / (1024.0 * 1024.0)
                if estimated_size_bytes is not None
                else None
            ),
            "write_ratio": _as_float(row.get("write_ratio")),
            "optimizer_score": score,
            "dmv_only": dmv_only,
        },
        "coverage": {
            "query_store": query_store_coverage.get("status", "incomplete"),
            "malformed": list(query_store_coverage.get("blockers", [])),
        },
        "key_columns": keys,
        "include_columns": includes,
        "current_score": score,
        "optimizer_score": score,
        "query_store_complete": query_store_coverage.get("status") == "complete",
        "recurring_executed": len(positive_intervals) >= 2,
        "runtime_interval_ids": intervals,
        "positive_runtime_interval_ids": positive_intervals,
        "query_ids": sorted({_as_int(value) for value in row.get("query_ids", ()) if _as_int(value) > 0}),
        "plan_ids": sorted({_as_int(value) for value in row.get("plan_ids", ()) if _as_int(value) > 0}),
        "covered_by": covered_by,
        "projected_database_storage_percent": projected_percent,
        "statement_subtree_cost": _as_float(row.get("statement_subtree_cost")),
        "execution_count": _as_float(row.get("execution_count")),
        "impact_pct": _as_float(row.get("impact_pct")),
        "estimated_index_size_mb": (
            _as_float(row.get("estimated_size_mb"))
            if _as_float(row.get("estimated_size_mb")) is not None
            else estimated_size_bytes / (1024.0 * 1024.0)
            if estimated_size_bytes is not None
            else None
        ),
        "table_write_ratio": _as_float(
            row.get("write_ratio", row.get("table_write_ratio"))
        ),
        "write_ratio": _as_float(
            row.get("write_ratio", row.get("table_write_ratio"))
        ),
        "estimated_size_bytes": estimated_size_bytes,
        "estimated_size_mb": (
            _as_float(row.get("estimated_size_mb"))
            if _as_float(row.get("estimated_size_mb")) is not None
            else estimated_size_bytes / (1024.0 * 1024.0)
            if estimated_size_bytes is not None
            else None
        ),
        "dmv_only": dmv_only,
    }
    subject["subject_fingerprint"] = _subject_fingerprint("missing_index", subject)
    return subject


@dataclass(frozen=True, slots=True)
class IndexReviewSnapshotV1:
    """One validated point-in-time projection of subject rows."""

    run_id: str
    snapshot_id: str
    database_name: str
    database_fingerprint: str
    observed_at_utc: str
    counter_epoch_fingerprint: str | None
    indexes: tuple[Mapping[str, Any], ...] = ()
    query_store: Mapping[str, Any] = field(default_factory=dict)
    create_candidates: tuple[Mapping[str, Any], ...] = ()
    inventory_fingerprint: str = ""
    snapshot_fingerprint: str = ""
    contract_version: str = INDEX_HISTORY_CONTRACT_VERSION
    schema_version: str = INDEX_HISTORY_SCHEMA_VERSION
    engine_fingerprint: str | None = None
    engine_identity: str | None = None
    engine_start_time_utc: str | None = None
    database_incarnation_fingerprint: str | None = None
    database_incarnation_identity: str | None = None
    coverage: Mapping[str, Any] = field(default_factory=dict)
    subjects: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if self.contract_version != INDEX_HISTORY_CONTRACT_VERSION:
            raise ValueError("Unsupported index history contract version.")
        if self.schema_version != INDEX_HISTORY_SCHEMA_VERSION:
            raise ValueError("Unsupported index history schema version.")
        for name in ("run_id", "snapshot_id", "database_name", "database_fingerprint"):
            _identifier(getattr(self, name), name)
        observed = _timestamp(self.observed_at_utc, "observed_at_utc")
        source_subjects = list(self.subjects)
        if not source_subjects:
            source_subjects = [
                {"subject_kind": "existing_index", **dict(item)} for item in self.indexes
            ] + [
                {"subject_kind": "missing_index", **dict(item)}
                for item in self.create_candidates
            ]
        normalized = tuple(_safe_payload(dict(item), schema="subject") for item in source_subjects)
        object.__setattr__(self, "observed_at_utc", observed)
        if self.engine_start_time_utc is not None:
            object.__setattr__(
                self,
                "engine_start_time_utc",
                _timestamp(self.engine_start_time_utc, "engine_start_time_utc"),
            )
        object.__setattr__(self, "subjects", normalized)
        object.__setattr__(self, "indexes", tuple(item for item in normalized if item.get("subject_kind") == "existing_index"))
        object.__setattr__(self, "create_candidates", tuple(item for item in normalized if item.get("subject_kind") == "missing_index"))
        object.__setattr__(self, "query_store", _safe_payload(dict(self.query_store), schema="query_store"))
        object.__setattr__(self, "coverage", _safe_payload(dict(self.coverage), schema="coverage"))
        if not self.inventory_fingerprint:
            object.__setattr__(self, "inventory_fingerprint", _inventory_fingerprint(normalized))
        if not self.snapshot_fingerprint:
            object.__setattr__(self, "snapshot_fingerprint", _snapshot_set_fingerprint(normalized))
        if self.counter_epoch_fingerprint is not None:
            _identifier(self.counter_epoch_fingerprint, "counter_epoch_fingerprint")
        for name, value in (
            ("engine_fingerprint", self.engine_fingerprint),
            ("database_incarnation_fingerprint", self.database_incarnation_fingerprint),
        ):
            if value is not None and not _HEX64.fullmatch(str(value)):
                raise ValueError(f"{name} must be a SHA-256 fingerprint when present.")

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "snapshot_id": self.snapshot_id,
            "database_name": self.database_name,
            "database_fingerprint": self.database_fingerprint,
            "engine_fingerprint": self.engine_fingerprint,
            "engine_identity": self.engine_identity,
            "engine_start_time_utc": self.engine_start_time_utc,
            "database_incarnation_fingerprint": self.database_incarnation_fingerprint,
            "database_incarnation_identity": self.database_incarnation_identity,
            "observed_at_utc": self.observed_at_utc,
            "counter_epoch_fingerprint": self.counter_epoch_fingerprint,
            "inventory_fingerprint": self.inventory_fingerprint,
            "snapshot_fingerprint": self.snapshot_fingerprint,
            "subjects": [dict(item) for item in self.subjects],
            "indexes": [dict(item) for item in self.indexes],
            "create_candidates": [dict(item) for item in self.create_candidates],
            "query_store": dict(self.query_store),
            "coverage": dict(self.coverage),
        }


@dataclass(frozen=True, slots=True)
class IndexReviewRunV1:
    run_id: str
    database_name: str
    database_fingerprint: str
    idempotency_key_hash: str
    request_fingerprint: str
    observed_at_utc: str
    counter_epoch_fingerprint: str | None
    inventory_fingerprint: str
    query_store_fingerprint: str
    contract_version: str = INDEX_HISTORY_CONTRACT_VERSION
    schema_version: str = INDEX_HISTORY_SCHEMA_VERSION
    collector_version: str = INDEX_REVIEW_COLLECTOR_VERSION
    engine_fingerprint: str | None = None
    engine_identity: str | None = None
    engine_start_time_utc: str | None = None
    database_incarnation_fingerprint: str | None = None
    database_incarnation_identity: str | None = None
    query_store_state: str = "unknown"
    query_capture_mode: str | None = None
    observation_start_utc: str | None = None
    observation_end_utc: str | None = None
    coverage: Mapping[str, Any] = field(default_factory=dict)
    subject_count: int = 0
    snapshot_set_fingerprint: str = ""
    query_store: Mapping[str, Any] = field(default_factory=dict)
    created_at_utc: str | None = None

    def __post_init__(self) -> None:
        if self.contract_version != INDEX_HISTORY_CONTRACT_VERSION:
            raise ValueError("Unsupported index history contract version.")
        if self.schema_version != INDEX_HISTORY_SCHEMA_VERSION:
            raise ValueError("Unsupported index history schema version.")
        if self.collector_version != INDEX_REVIEW_COLLECTOR_VERSION:
            raise ValueError("Unsupported index collector version.")
        for name in (
            "run_id",
            "database_name",
            "database_fingerprint",
            "idempotency_key_hash",
            "request_fingerprint",
            "inventory_fingerprint",
            "query_store_fingerprint",
        ):
            _identifier(getattr(self, name), name)
        object.__setattr__(self, "observed_at_utc", _timestamp(self.observed_at_utc, "observed_at_utc"))
        for name in (
            "engine_start_time_utc",
            "observation_start_utc",
            "observation_end_utc",
            "created_at_utc",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _timestamp(value, name))
        object.__setattr__(self, "coverage", _safe_payload(dict(self.coverage), schema="coverage"))
        object.__setattr__(self, "query_store", _safe_payload(dict(self.query_store), schema="query_store"))
        if self.subject_count < 0:
            raise ValueError("subject_count must not be negative.")
        for name, value in (
            ("engine_fingerprint", self.engine_fingerprint),
            ("database_incarnation_fingerprint", self.database_incarnation_fingerprint),
        ):
            if value is not None and not _HEX64.fullmatch(str(value)):
                raise ValueError(f"{name} must be a SHA-256 fingerprint when present.")

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "schema_version": self.schema_version,
            "collector_version": self.collector_version,
            "run_id": self.run_id,
            "database_name": self.database_name,
            "database_fingerprint": self.database_fingerprint,
            "engine_fingerprint": self.engine_fingerprint,
            "engine_identity": self.engine_identity,
            "engine_start_time_utc": self.engine_start_time_utc,
            "database_incarnation_fingerprint": self.database_incarnation_fingerprint,
            "database_incarnation_identity": self.database_incarnation_identity,
            "idempotency_key_hash": self.idempotency_key_hash,
            "request_fingerprint": self.request_fingerprint,
            "observed_at_utc": self.observed_at_utc,
            "created_at_utc": self.created_at_utc,
            "counter_epoch_fingerprint": self.counter_epoch_fingerprint,
            "inventory_fingerprint": self.inventory_fingerprint,
            "query_store_fingerprint": self.query_store_fingerprint,
            "query_store_state": self.query_store_state,
            "query_capture_mode": self.query_capture_mode,
            "observation_start_utc": self.observation_start_utc,
            "observation_end_utc": self.observation_end_utc,
            "coverage": dict(self.coverage),
            "subject_count": self.subject_count,
            "snapshot_set_fingerprint": self.snapshot_set_fingerprint,
            "query_store": dict(self.query_store),
        }


@dataclass(frozen=True, slots=True)
class IndexReviewV1:
    review_id: str
    database_name: str
    as_of_run_id: str
    prior_review_id: str | None
    overall_state: str
    subjects: tuple[Mapping[str, Any], ...]
    observation: Mapping[str, Any]
    contract_version: str = INDEX_REVIEW_CONTRACT_VERSION
    algorithm_version: str = INDEX_REVIEW_ALGORITHM_VERSION
    classifier_policy_version: str = INDEX_REVIEW_CLASSIFIER_POLICY_VERSION
    minimum_observation_days: int = MIN_OBSERVATION_DAYS
    history_fingerprint: str = ""
    prior_base_run_id: str | None = None
    database_fingerprint: str | None = None
    database_incarnation_fingerprint: str | None = None
    evidence_id: str | None = None

    def __post_init__(self) -> None:
        if len(self.review_id) >= 200 or not _REVIEW_ID.fullmatch(self.review_id):
            raise ValueError("review_id must be a bounded deterministic selector.")
        _identifier(self.database_name, "database_name")
        _identifier(self.as_of_run_id, "as_of_run_id")
        if self.prior_review_id is not None:
            _identifier(self.prior_review_id, "prior_review_id")
        if self.overall_state not in INDEX_REVIEW_OVERALL_STATES:
            raise ValueError("Unsupported index review overall state.")
        if self.contract_version != INDEX_REVIEW_CONTRACT_VERSION:
            raise ValueError("Unsupported index review contract version.")
        if self.algorithm_version != INDEX_REVIEW_ALGORITHM_VERSION:
            raise ValueError("Unsupported index review algorithm version.")
        if self.classifier_policy_version != INDEX_REVIEW_CLASSIFIER_POLICY_VERSION:
            raise ValueError("Unsupported index review classifier policy version.")
        if self.minimum_observation_days < MIN_OBSERVATION_DAYS:
            raise ValueError("minimum_observation_days is below the fixed floor.")
        object.__setattr__(self, "subjects", tuple(_safe_payload(dict(item), schema="subject") for item in self.subjects))
        object.__setattr__(self, "observation", _safe_payload(dict(self.observation), schema="observation"))

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "algorithm_version": self.algorithm_version,
            "classifier_policy_version": self.classifier_policy_version,
            "review_id": self.review_id,
            "database_name": self.database_name,
            "database_fingerprint": self.database_fingerprint,
            "database_incarnation_fingerprint": self.database_incarnation_fingerprint,
            "as_of_run_id": self.as_of_run_id,
            "prior_review_id": self.prior_review_id,
            "prior_base_run_id": self.prior_base_run_id,
            "minimum_observation_days": self.minimum_observation_days,
            "history_fingerprint": self.history_fingerprint,
            "overall_state": self.overall_state,
            "subjects": [dict(item) for item in self.subjects],
            "observation": dict(self.observation),
            "recommend_only": True,
            "evidence_id": self.evidence_id,
        }


@dataclass(frozen=True, slots=True)
class CaptureResult:
    run: IndexReviewRunV1
    snapshot: IndexReviewSnapshotV1
    already_captured: bool = False
    reconciled: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": "already_captured" if self.already_captured else "captured",
            "already_captured": self.already_captured,
            "reconciled": self.reconciled,
            "run": self.run.as_dict(),
            "snapshot": self.snapshot.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class CaptureContext:
    database_name: str
    database_fingerprint: str
    run_id: str
    idempotency_key_hash: str
    request_fingerprint: str
    observed_at_utc: str
    minimum_observation_days: int


@dataclass(frozen=True, slots=True)
class ContractProbeResult:
    schema_fingerprint: str
    allow_read: bool
    allow_write: bool
    dangerous_permissions_absent: bool


RUN_CONTRACT_COLUMNS: tuple[tuple[str, str, int, bool], ...] = (
    # MaxLength is sys.columns.max_length, which is bytes for nvarchar and
    # the fixed storage width for datetime2/int/bigint.
    ("RunId", "nvarchar", 400, False),
    ("ContractVersion", "varchar", 16, False),
    ("SchemaVersion", "varchar", 32, False),
    ("CollectorVersion", "varchar", 64, False),
    ("DatabaseName", "nvarchar", 256, False),
    ("DatabaseFingerprint", "char", 64, False),
    ("DatabaseIncarnationFingerprint", "char", 64, True),
    ("DatabaseIncarnationIdentity", "varchar", 128, True),
    ("EngineFingerprint", "char", 64, True),
    ("EngineIdentity", "varchar", 128, True),
    ("EngineStartTimeUtc", "datetime2", 8, True),
    ("IdempotencyKeyHash", "char", 64, False),
    ("RequestFingerprint", "char", 64, False),
    ("ObservedAtUtc", "datetime2", 8, False),
    ("CreatedAtUtc", "datetime2", 8, False),
    ("CounterEpochFingerprint", "char", 64, True),
    ("InventoryFingerprint", "char", 64, False),
    ("QueryStoreFingerprint", "char", 64, False),
    ("QueryStoreState", "varchar", 32, False),
    ("QueryCaptureMode", "varchar", 64, True),
    ("ObservationStartUtc", "datetime2", 8, True),
    ("ObservationEndUtc", "datetime2", 8, True),
    ("CoverageJson", "nvarchar", -1, False),
    ("SubjectCount", "int", 4, False),
    ("SnapshotSetFingerprint", "char", 64, False),
    ("QueryStoreJson", "nvarchar", -1, False),
)
SNAPSHOT_CONTRACT_COLUMNS: tuple[tuple[str, str, int, bool], ...] = (
    ("SnapshotId", "nvarchar", 400, False),
    ("RunId", "nvarchar", 400, False),
    ("SubjectId", "nvarchar", 400, False),
    ("SubjectKind", "varchar", 32, False),
    ("SubjectFingerprint", "char", 64, False),
    ("ObjectId", "bigint", 8, True),
    ("IndexId", "int", 4, True),
    ("SchemaName", "nvarchar", 256, True),
    ("ObjectName", "nvarchar", 256, True),
    ("IndexName", "nvarchar", 256, True),
    ("DefinitionJson", "nvarchar", -1, False),
    ("DefinitionFingerprint", "char", 64, False),
    ("CounterEpochFingerprint", "char", 64, True),
    ("CountersJson", "nvarchar", -1, False),
    ("ObservedAtUtc", "datetime2", 8, False),
    ("FirstObservedAtUtc", "datetime2", 8, True),
    ("LastObservedAtUtc", "datetime2", 8, True),
    ("SizePages", "bigint", 8, True),
    ("SizeBytes", "bigint", 8, True),
    ("WriteBurden", "bigint", 8, True),
    ("QueryStoreReferencesJson", "nvarchar", -1, False),
    ("ProtectionsJson", "nvarchar", -1, False),
    ("MissingSignatureJson", "nvarchar", -1, True),
    ("AggregatesJson", "nvarchar", -1, False),
    ("CoverageJson", "nvarchar", -1, False),
)

_CONTRACT_TYPE_PRECISION_SCALE: dict[str, tuple[int, int]] = {
    "nvarchar": (0, 0),
    "varchar": (0, 0),
    "char": (0, 0),
    "datetime2": (27, 7),
    "int": (10, 0),
    "bigint": (19, 0),
}

# Constraint definitions are returned by SQL Server with implementation-added
# parentheses and, depending on the catalog path, identifier brackets.  Keep
# every meaningful token while making only those presentation differences
# irrelevant to the contract comparison.
_SQL_FUNCTION_NAMES = frozenset(
    {"sysutcdatetime", "len", "isjson", "json_query", "left", "ltrim"}
)


def _matching_sql_close(
    tokens: Sequence[tuple[str, str]],
    start: int,
) -> int | None:
    close_for = {
        "group_open": "group_close",
        "call_open": "call_close",
    }
    expected: list[str] = []
    for position in range(start, len(tokens)):
        kind = tokens[position][0]
        if kind in close_for:
            expected.append(close_for[kind])
        elif expected and kind == expected[-1]:
            expected.pop()
            if not expected:
                return position
    return None


def _is_single_sql_atom(tokens: Sequence[tuple[str, str]]) -> bool:
    if len(tokens) == 1:
        return True
    return (
        len(tokens) >= 3
        and tokens[0][0] == "identifier"
        and tokens[1][0] == "call_open"
        and _matching_sql_close(tokens, 1) == len(tokens) - 1
    )


def _strip_redundant_sql_groups(
    tokens: Sequence[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    normalised: list[tuple[str, str]] = []
    position = 0
    while position < len(tokens):
        token = tokens[position]
        if token[0] != "group_open":
            normalised.append(token)
            position += 1
            continue
        close = _matching_sql_close(tokens, position)
        if close is None:
            normalised.append(token)
            position += 1
            continue
        inner = _strip_redundant_sql_groups(tokens[position + 1 : close])
        if _is_single_sql_atom(inner):
            normalised.extend(inner)
        else:
            normalised.append(("group_open", "("))
            normalised.extend(inner)
            normalised.append(("group_close", ")"))
        position = close + 1
    while (
        normalised
        and normalised[0][0] == "group_open"
        and _matching_sql_close(normalised, 0) == len(normalised) - 1
    ):
        normalised = normalised[1:-1]
    return tuple(normalised)


def _canonical_sql_tokens(value: Any) -> tuple[str, ...]:
    text = str(value or "")
    tokens: list[tuple[str, str]] = []
    open_stack: list[str] = []
    position = 0
    while position < len(text):
        char = text[position]
        if char.isspace():
            position += 1
            continue
        if text.startswith("--", position):
            newline = text.find("\n", position + 2)
            position = len(text) if newline < 0 else newline + 1
            continue
        if text.startswith("/*", position):
            end = text.find("*/", position + 2)
            if end < 0:
                position = len(text)
            else:
                position = end + 2
            continue
        if char == "[":
            end = position + 1
            value_parts: list[str] = []
            while end < len(text):
                if text[end] == "]":
                    if end + 1 < len(text) and text[end + 1] == "]":
                        value_parts.append("]")
                        end += 2
                        continue
                    break
                value_parts.append(text[end])
                end += 1
            if end >= len(text):
                tokens.append(("identifier", "[" + "".join(value_parts).casefold()))
                position = end
            else:
                tokens.append(("identifier", "".join(value_parts).casefold()))
                position = end + 1
            continue
        if char in {"'", '"'}:
            quote = char
            end = position + 1
            while end < len(text):
                if text[end] == quote:
                    if end + 1 < len(text) and text[end + 1] == quote:
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
            tokens.append(("literal", text[position:end]))
            position = end
            continue
        match = re.match(r"[A-Za-z_][A-Za-z0-9_$#@]*", text[position:])
        if match:
            tokens.append(("identifier", match.group(0).casefold()))
            position += len(match.group(0))
            continue
        match = re.match(r"[0-9]+(?:\.[0-9]+)?", text[position:])
        if match:
            tokens.append(("literal", match.group(0)))
            position += len(match.group(0))
            continue
        operator = next(
            (
                candidate
                for candidate in ("<>", "!=", "<=", ">=")
                if text.startswith(candidate, position)
            ),
            None,
        )
        if operator is not None:
            tokens.append(("operator", operator))
            position += len(operator)
            continue
        if char == "(":
            previous = tokens[-1][1] if tokens else ""
            is_call = previous in _SQL_FUNCTION_NAMES
            tokens.append(("call_open" if is_call else "group_open", char))
            open_stack.append("call" if is_call else "group")
            position += 1
            continue
        if char == ")":
            parenthesis_kind = open_stack.pop() if open_stack else "group"
            tokens.append(
                (
                    "call_close"
                    if parenthesis_kind == "call"
                    else "group_close",
                    char,
                )
            )
            position += 1
            continue
        tokens.append(("operator" if char in "<>=+-*/" else "punctuation", char.casefold()))
        position += 1
    return tuple(value for _kind, value in _strip_redundant_sql_groups(tokens))


def _canonical_constraint_definitions(
    definitions: Sequence[tuple[str, str, str]],
) -> tuple[tuple[str, str, tuple[str, ...]], ...]:
    return tuple(
        (table, name, _canonical_sql_tokens(definition))
        for table, name, definition in definitions
    )


CONTRACT_DEFAULTS: tuple[tuple[str, str, str], ...] = (
    ("IndexReviewRun", "DF_IndexReviewRun_CreatedAtUtc", "SYSUTCDATETIME()"),
)
CONTRACT_CHECKS: tuple[tuple[str, str, str], ...] = (
    ("IndexReviewRun", "CK_IndexReviewRun_ContractVersion", "ContractVersion = '2.3.0'"),
    ("IndexReviewRun", "CK_IndexReviewRun_SchemaVersion", "SchemaVersion = 'index-history-v1'"),
    ("IndexReviewRun", "CK_IndexReviewRun_DatabaseFingerprint", "DatabaseFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(DatabaseFingerprint) = 64"),
    ("IndexReviewRun", "CK_IndexReviewRun_DatabaseIncarnationFingerprint", "DatabaseIncarnationFingerprint IS NULL OR (DatabaseIncarnationFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(DatabaseIncarnationFingerprint) = 64)"),
    ("IndexReviewRun", "CK_IndexReviewRun_EngineFingerprint", "EngineFingerprint IS NULL OR (EngineFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(EngineFingerprint) = 64)"),
    ("IndexReviewRun", "CK_IndexReviewRun_IdempotencyKeyHash", "IdempotencyKeyHash NOT LIKE '%[^0-9a-fA-F]%' AND LEN(IdempotencyKeyHash) = 64"),
    ("IndexReviewRun", "CK_IndexReviewRun_RequestFingerprint", "RequestFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(RequestFingerprint) = 64"),
    ("IndexReviewRun", "CK_IndexReviewRun_CounterEpochFingerprint", "CounterEpochFingerprint IS NULL OR (CounterEpochFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(CounterEpochFingerprint) = 64)"),
    ("IndexReviewRun", "CK_IndexReviewRun_InventoryFingerprint", "InventoryFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(InventoryFingerprint) = 64"),
    ("IndexReviewRun", "CK_IndexReviewRun_QueryStoreFingerprint", "QueryStoreFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(QueryStoreFingerprint) = 64"),
    ("IndexReviewRun", "CK_IndexReviewRun_CoverageJson", "ISJSON(CoverageJson) = 1 AND JSON_QUERY(CoverageJson) IS NOT NULL"),
    ("IndexReviewRun", "CK_IndexReviewRun_SubjectCount", "SubjectCount >= 0"),
    ("IndexReviewRun", "CK_IndexReviewRun_SnapshotSetFingerprint", "SnapshotSetFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(SnapshotSetFingerprint) = 64"),
    ("IndexReviewRun", "CK_IndexReviewRun_QueryStoreJson", "ISJSON(QueryStoreJson) = 1 AND JSON_QUERY(QueryStoreJson) IS NOT NULL"),
    ("IndexReviewRun", "CK_IndexReviewRun_DatabaseIncarnationIdentity", "(DatabaseIncarnationFingerprint IS NULL AND DatabaseIncarnationIdentity IS NULL) OR (DatabaseIncarnationFingerprint IS NOT NULL AND DatabaseIncarnationIdentity IS NOT NULL)"),
    ("IndexReviewRun", "CK_IndexReviewRun_EngineEpochIdentity", "(EngineFingerprint IS NULL AND EngineIdentity IS NULL AND EngineStartTimeUtc IS NULL) OR (EngineFingerprint IS NOT NULL AND EngineIdentity IS NOT NULL AND EngineStartTimeUtc IS NOT NULL)"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_SubjectKind", "SubjectKind IN ('existing_index', 'missing_index')"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_SubjectFingerprint", "SubjectFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(SubjectFingerprint) = 64"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_DefinitionJson", "ISJSON(DefinitionJson) = 1 AND JSON_QUERY(DefinitionJson) IS NOT NULL"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_DefinitionFingerprint", "DefinitionFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(DefinitionFingerprint) = 64"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_CounterEpochFingerprint", "CounterEpochFingerprint IS NULL OR (CounterEpochFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(CounterEpochFingerprint) = 64)"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_CountersJson", "ISJSON(CountersJson) = 1 AND JSON_QUERY(CountersJson) IS NOT NULL"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_SizePages", "SizePages IS NULL OR SizePages >= 0"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_SizeBytes", "SizeBytes IS NULL OR SizeBytes >= 0"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_WriteBurden", "WriteBurden IS NULL OR WriteBurden >= 0"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_QueryStoreReferencesJson", "ISJSON(QueryStoreReferencesJson) = 1 AND LEFT(LTRIM(QueryStoreReferencesJson), 1) = '['"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_ProtectionsJson", "ISJSON(ProtectionsJson) = 1 AND JSON_QUERY(ProtectionsJson) IS NOT NULL"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_MissingSignatureJson", "MissingSignatureJson IS NULL OR (ISJSON(MissingSignatureJson) = 1 AND JSON_QUERY(MissingSignatureJson) IS NOT NULL)"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_AggregatesJson", "ISJSON(AggregatesJson) = 1 AND JSON_QUERY(AggregatesJson) IS NOT NULL"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_CoverageJson", "ISJSON(CoverageJson) = 1 AND JSON_QUERY(CoverageJson) IS NOT NULL"),
    ("IndexReviewSnapshot", "CK_IndexReviewSnapshot_SubjectActionInvariants", "SubjectKind IN ('existing_index', 'missing_index') AND (SubjectKind = 'existing_index' OR MissingSignatureJson IS NOT NULL)"),
)


def _schema_material() -> dict[str, Any]:
    return {
        "schema_version": INDEX_HISTORY_SCHEMA_VERSION,
        "run_columns": RUN_CONTRACT_COLUMNS,
        "snapshot_columns": SNAPSHOT_CONTRACT_COLUMNS,
        "type_precision_scale": _CONTRACT_TYPE_PRECISION_SCALE,
        "primary_keys": [("IndexReviewRun", ("RunId",)), ("IndexReviewSnapshot", ("SnapshotId",))],
        "unique_indexes": [
            ("IndexReviewRun", ("DatabaseFingerprint", "IdempotencyKeyHash")),
            ("IndexReviewSnapshot", ("RunId", "SubjectId")),
        ],
        "foreign_keys": [("IndexReviewSnapshot", ("RunId",), "IndexReviewRun", ("RunId",))],
        "defaults": _canonical_constraint_definitions(CONTRACT_DEFAULTS),
        "checks": _canonical_constraint_definitions(CONTRACT_CHECKS),
    }


CONTRACT_SCHEMA_FINGERPRINT = _digest("index-history-schema-v1", _schema_material())

CONTRACT_PROBE_SQL = """
SELECT
    t.name AS TableName, c.name AS ColumnName, ty.name AS DataType,
    c.max_length AS MaxLength, c.is_nullable AS IsNullable,
    c.precision AS PrecisionValue, c.scale AS ScaleValue
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
INNER JOIN sys.columns AS c ON c.object_id = t.object_id
INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
WHERE s.name = N'dbatools' AND t.name IN (N'IndexReviewRun', N'IndexReviewSnapshot')
ORDER BY t.name, c.column_id;
SELECT
    t.name AS TableName, i.name AS IndexName, i.is_primary_key AS IsPrimaryKey,
    i.is_unique AS IsUnique, ic.key_ordinal AS KeyOrdinal, c.name AS ColumnName
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
INNER JOIN sys.indexes AS i ON i.object_id = t.object_id
INNER JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
INNER JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE s.name = N'dbatools' AND t.name IN (N'IndexReviewRun', N'IndexReviewSnapshot')
  AND (i.is_primary_key = 1 OR i.is_unique = 1)
ORDER BY t.name, i.name, ic.key_ordinal;
SELECT
    child.name AS ChildTableName, parent.name AS ParentTableName,
    fkc.constraint_column_id AS ColumnOrdinal,
    child_column.name AS ChildColumnName, parent_column.name AS ParentColumnName
FROM sys.foreign_keys AS fk
INNER JOIN sys.tables AS child ON child.object_id = fk.parent_object_id
INNER JOIN sys.tables AS parent ON parent.object_id = fk.referenced_object_id
INNER JOIN sys.schemas AS child_schema ON child_schema.schema_id = child.schema_id
INNER JOIN sys.schemas AS parent_schema ON parent_schema.schema_id = parent.schema_id
INNER JOIN sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id
INNER JOIN sys.columns AS child_column ON child_column.object_id = fkc.parent_object_id AND child_column.column_id = fkc.parent_column_id
INNER JOIN sys.columns AS parent_column ON parent_column.object_id = fkc.referenced_object_id AND parent_column.column_id = fkc.referenced_column_id
WHERE child_schema.name = N'dbatools' AND parent_schema.name = N'dbatools'
  AND child.name = N'IndexReviewSnapshot' AND parent.name = N'IndexReviewRun'
ORDER BY fkc.constraint_column_id;
SELECT
    t.name AS TableName,
    dc.name AS ConstraintName,
    N'DEFAULT' AS ConstraintType,
    dc.definition AS Definition
FROM sys.default_constraints AS dc
INNER JOIN sys.tables AS t ON t.object_id = dc.parent_object_id
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name = N'dbatools' AND t.name IN (N'IndexReviewRun', N'IndexReviewSnapshot')
UNION ALL
SELECT
    t.name AS TableName,
    cc.name AS ConstraintName,
    N'CHECK' AS ConstraintType,
    cc.definition AS Definition
FROM sys.check_constraints AS cc
INNER JOIN sys.tables AS t ON t.object_id = cc.parent_object_id
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name = N'dbatools' AND t.name IN (N'IndexReviewRun', N'IndexReviewSnapshot')
ORDER BY TableName, ConstraintType, ConstraintName;
SELECT
    TableName,
    HAS_PERMS_BY_NAME(N'dbatools.' + TableName, N'OBJECT', N'SELECT') AS SelectState,
    HAS_PERMS_BY_NAME(N'dbatools.' + TableName, N'OBJECT', N'INSERT') AS InsertState,
    HAS_PERMS_BY_NAME(N'dbatools.' + TableName, N'OBJECT', N'UPDATE') AS UpdateState,
    HAS_PERMS_BY_NAME(N'dbatools.' + TableName, N'OBJECT', N'DELETE') AS DeleteState,
    HAS_PERMS_BY_NAME(N'dbatools.' + TableName, N'OBJECT', N'ALTER') AS AlterState,
    HAS_PERMS_BY_NAME(N'dbatools.' + TableName, N'OBJECT', N'CONTROL') AS ControlState
    ,HAS_PERMS_BY_NAME(N'dbatools.' + TableName, N'OBJECT', N'EXECUTE') AS ExecuteState
    ,HAS_PERMS_BY_NAME(N'dbatools.' + TableName, N'OBJECT', N'REFERENCES') AS ReferencesState
    ,HAS_PERMS_BY_NAME(N'dbatools.' + TableName, N'OBJECT', N'VIEW DEFINITION') AS ViewDefinitionState
    ,HAS_PERMS_BY_NAME(N'dbatools.' + TableName, N'OBJECT', N'TAKE OWNERSHIP') AS TakeOwnershipState
FROM (VALUES (N'IndexReviewRun'), (N'IndexReviewSnapshot')) AS names(TableName);
"""

HISTORY_READ_SQL = """
SELECT RunId, ContractVersion, SchemaVersion, CollectorVersion, DatabaseName,
       DatabaseFingerprint, DatabaseIncarnationFingerprint, DatabaseIncarnationIdentity,
       EngineFingerprint, EngineIdentity, EngineStartTimeUtc,
       IdempotencyKeyHash, RequestFingerprint, ObservedAtUtc, CreatedAtUtc,
       CounterEpochFingerprint, InventoryFingerprint, QueryStoreFingerprint,
       QueryStoreState, QueryCaptureMode, ObservationStartUtc, ObservationEndUtc,
       CoverageJson, SubjectCount, SnapshotSetFingerprint, QueryStoreJson
FROM dbatools.IndexReviewRun
WHERE DatabaseName = ? AND (? IS NULL OR RunId = ?)
ORDER BY ObservedAtUtc, RunId;
SELECT SnapshotId, RunId, SubjectId, SubjectKind, SubjectFingerprint,
       ObjectId, IndexId, SchemaName, ObjectName, IndexName, DefinitionJson,
       DefinitionFingerprint, CounterEpochFingerprint, CountersJson,
       ObservedAtUtc, FirstObservedAtUtc, LastObservedAtUtc, SizePages, SizeBytes,
       WriteBurden, QueryStoreReferencesJson, ProtectionsJson, MissingSignatureJson,
       AggregatesJson, CoverageJson
FROM dbatools.IndexReviewSnapshot
WHERE RunId IN (SELECT RunId FROM dbatools.IndexReviewRun WHERE DatabaseName = ? AND (? IS NULL OR RunId = ?))
ORDER BY RunId, SubjectId;
"""
CAPTURE_READ_SQL = """
SELECT RunId, ContractVersion, SchemaVersion, CollectorVersion, DatabaseName,
       DatabaseFingerprint, DatabaseIncarnationFingerprint, DatabaseIncarnationIdentity,
       EngineFingerprint, EngineIdentity, EngineStartTimeUtc,
       IdempotencyKeyHash, RequestFingerprint, ObservedAtUtc, CreatedAtUtc,
       CounterEpochFingerprint, InventoryFingerprint, QueryStoreFingerprint,
       QueryStoreState, QueryCaptureMode, ObservationStartUtc, ObservationEndUtc,
       CoverageJson, SubjectCount, SnapshotSetFingerprint, QueryStoreJson
FROM dbatools.IndexReviewRun
WHERE DatabaseFingerprint = ? AND IdempotencyKeyHash = ?;
SELECT SnapshotId, RunId, SubjectId, SubjectKind, SubjectFingerprint,
       ObjectId, IndexId, SchemaName, ObjectName, IndexName, DefinitionJson,
       DefinitionFingerprint, CounterEpochFingerprint, CountersJson,
       ObservedAtUtc, FirstObservedAtUtc, LastObservedAtUtc, SizePages, SizeBytes,
       WriteBurden, QueryStoreReferencesJson, ProtectionsJson, MissingSignatureJson,
       AggregatesJson, CoverageJson
FROM dbatools.IndexReviewSnapshot
WHERE RunId IN (
    SELECT RunId FROM dbatools.IndexReviewRun
    WHERE DatabaseFingerprint = ? AND IdempotencyKeyHash = ?
)
ORDER BY RunId, SubjectId;
"""
MISSING_INDEX_DMV_SQL = """
SELECT TOP (?)
    OBJECT_SCHEMA_NAME(mid.object_id) AS schema_name,
    OBJECT_NAME(mid.object_id) AS table_name,
    mid.equality_columns,
    mid.inequality_columns,
    mid.included_columns,
    migs.user_seeks,
    migs.user_scans,
    migs.avg_total_user_cost,
    migs.avg_user_impact,
    CONVERT(decimal(18, 2),
        migs.avg_total_user_cost * migs.avg_user_impact
        * (migs.user_seeks + migs.user_scans)) AS estimated_improvement
FROM sys.dm_db_missing_index_details AS mid
INNER JOIN sys.dm_db_missing_index_groups AS mig
    ON mig.index_handle = mid.index_handle
INNER JOIN sys.dm_db_missing_index_group_stats AS migs
    ON migs.group_handle = mig.index_group_handle
WHERE mid.database_id = DB_ID()
ORDER BY estimated_improvement DESC, mid.index_handle;
"""
QUERY_STORE_WINDOW_SQL = """
SELECT TOP (1)
    CONVERT(datetime2(7), MIN(rsi.start_time)) AS window_start_utc,
    CONVERT(datetime2(7), MAX(rsi.end_time)) AS window_end_utc,
    COUNT(DISTINCT rsi.runtime_stats_interval_id) AS runtime_interval_count
FROM sys.query_store_runtime_stats_interval AS rsi
WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME());
"""
IDEMPOTENCY_LOCK_SQL = """
SELECT RunId, RequestFingerprint
FROM dbatools.IndexReviewRun WITH (UPDLOCK, HOLDLOCK)
WHERE DatabaseFingerprint = ? AND IdempotencyKeyHash = ?;
"""
INSERT_RUN_SQL = """
INSERT INTO dbatools.IndexReviewRun
    (RunId, ContractVersion, SchemaVersion, CollectorVersion, DatabaseName,
     DatabaseFingerprint, DatabaseIncarnationFingerprint, DatabaseIncarnationIdentity,
     EngineFingerprint, EngineIdentity, EngineStartTimeUtc,
     IdempotencyKeyHash, RequestFingerprint, ObservedAtUtc, CounterEpochFingerprint,
     InventoryFingerprint, QueryStoreFingerprint, QueryStoreState, QueryCaptureMode,
     ObservationStartUtc, ObservationEndUtc, CoverageJson, SubjectCount,
     SnapshotSetFingerprint, QueryStoreJson)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
INSERT_SNAPSHOT_SQL = """
INSERT INTO dbatools.IndexReviewSnapshot
    (SnapshotId, RunId, SubjectId, SubjectKind, SubjectFingerprint, ObjectId, IndexId,
     SchemaName, ObjectName, IndexName, DefinitionJson, DefinitionFingerprint,
     CounterEpochFingerprint, CountersJson, ObservedAtUtc, FirstObservedAtUtc,
     LastObservedAtUtc, SizePages, SizeBytes, WriteBurden, QueryStoreReferencesJson,
     ProtectionsJson, MissingSignatureJson, AggregatesJson, CoverageJson)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def _rows_from_sets(value: Any) -> list[list[dict[str, Any]]]:
    if isinstance(value, list) and value and all(isinstance(item, list) for item in value):
        return [[dict(row) for row in item] for item in value]
    if isinstance(value, list):
        return [[dict(row) for row in value]]
    return []


def validate_contract_probe(result_sets: Sequence[Any]) -> ContractProbeResult:
    sets = _rows_from_sets(list(result_sets))
    if len(sets) != 5:
        raise IndexReviewSchemaError(
            "History contract probe must return exactly five result sets."
        )
    columns = sets[0]
    expected_columns = {
        "IndexReviewRun": {name for name, *_ in RUN_CONTRACT_COLUMNS},
        "IndexReviewSnapshot": {name for name, *_ in SNAPSHOT_CONTRACT_COLUMNS},
    }
    actual: dict[str, set[str]] = {key: set() for key in expected_columns}
    for row in columns:
        table = str(row.get("TableName", ""))
        if table in actual:
            actual[table].add(str(row.get("ColumnName", "")))
    missing_tables = [
        f"dbatools.{table_name}"
        for table_name, column_names in actual.items()
        if not column_names
    ]
    if missing_tables:
        noun = "table is" if len(missing_tables) == 1 else "tables are"
        raise IndexReviewSchemaError(
            f"Index history {noun} missing: {', '.join(missing_tables)}."
        )
    if actual != expected_columns:
        raise IndexReviewSchemaError("History contract columns do not match the versioned schema.")
    for table_name, expected in expected_columns.items():
        rows = [row for row in columns if row.get("TableName") == table_name]
        if len(rows) != len(expected):
            raise IndexReviewSchemaError(f"History contract {table_name} has duplicate or missing columns.")
        by_name = {str(row.get("ColumnName")): row for row in rows}
        expected_specs = {
            name: (data_type, max_length, nullable)
            for name, data_type, max_length, nullable in (
                RUN_CONTRACT_COLUMNS if table_name == "IndexReviewRun" else SNAPSHOT_CONTRACT_COLUMNS
            )
        }
        for name, (data_type, max_length, nullable) in expected_specs.items():
            row = by_name[name]
            expected_precision, expected_scale = _CONTRACT_TYPE_PRECISION_SCALE[data_type]
            if (
                str(row.get("DataType", "")).casefold() != data_type.casefold()
                or _as_int(row.get("MaxLength"), -1_000_000) != max_length
                or _as_bool(row.get("IsNullable")) != nullable
                or _as_int(row.get("PrecisionValue"), -1_000_000) != expected_precision
                or _as_int(row.get("ScaleValue"), -1_000_000) != expected_scale
            ):
                raise IndexReviewSchemaError(f"History contract column {table_name}.{name} has the wrong type or nullability.")
    unique_rows = sets[1]
    index_keys: dict[tuple[str, str], list[tuple[int, str]]] = {}
    index_flags: dict[tuple[str, str], tuple[bool, bool]] = {}
    for row in unique_rows:
        key = (str(row.get("TableName")), str(row.get("IndexName")))
        index_keys.setdefault(key, []).append(
            (_as_int(row.get("KeyOrdinal"), -1), str(row.get("ColumnName")))
        )
        flags = (_as_bool(row.get("IsPrimaryKey")), _as_bool(row.get("IsUnique")))
        previous_flags = index_flags.setdefault(key, flags)
        if previous_flags != flags:
            raise IndexReviewSchemaError(f"History contract index {key[0]}.{key[1]} has inconsistent flags.")
    normalized_index_keys = {
        key: tuple(column for _ordinal, column in sorted(columns))
        for key, columns in index_keys.items()
    }
    expected_index_keys = {
        ("IndexReviewRun", "PK_IndexReviewRun"): ("RunId",),
        ("IndexReviewRun", "UQ_IndexReviewRun_Database_Idempotency"): (
            "DatabaseFingerprint",
            "IdempotencyKeyHash",
        ),
        ("IndexReviewSnapshot", "PK_IndexReviewSnapshot"): ("SnapshotId",),
        ("IndexReviewSnapshot", "UQ_IndexReviewSnapshot_Run_Subject"): (
            "RunId",
            "SubjectId",
        ),
    }
    if normalized_index_keys != expected_index_keys:
        raise IndexReviewSchemaError("History contract primary or unique keys do not match.")
    expected_index_flags = {
        ("IndexReviewRun", "PK_IndexReviewRun"): (True, True),
        ("IndexReviewRun", "UQ_IndexReviewRun_Database_Idempotency"): (False, True),
        ("IndexReviewSnapshot", "PK_IndexReviewSnapshot"): (True, True),
        ("IndexReviewSnapshot", "UQ_IndexReviewSnapshot_Run_Subject"): (False, True),
    }
    if index_flags != expected_index_flags:
        raise IndexReviewSchemaError("History contract primary or unique index flags do not match.")
    foreign_keys = sets[2]
    actual_foreign_keys = {
        (
            row.get("ChildTableName"),
            row.get("ParentTableName"),
            _as_int(row.get("ColumnOrdinal"), -1),
            row.get("ChildColumnName"),
            row.get("ParentColumnName"),
        )
        for row in foreign_keys
    }
    if actual_foreign_keys != {
        ("IndexReviewSnapshot", "IndexReviewRun", 1, "RunId", "RunId")
    }:
        raise IndexReviewSchemaError("History contract foreign key is missing.")
    constraint_rows = sets[3]
    actual_constraints: dict[tuple[str, str], tuple[str, tuple[str, ...]]] = {}
    for row in constraint_rows:
        table = str(row.get("TableName"))
        name = str(row.get("ConstraintName"))
        kind = str(row.get("ConstraintType", "")).upper()
        definition = _canonical_sql_tokens(row.get("Definition"))
        if table in {"IndexReviewRun", "IndexReviewSnapshot"} and name:
            actual_constraints[(table, name)] = (kind, definition)
    expected_constraints = {
        (table, name): ("DEFAULT", _canonical_sql_tokens(definition))
        for table, name, definition in CONTRACT_DEFAULTS
    }
    expected_constraints.update(
        {
            (table, name): ("CHECK", _canonical_sql_tokens(definition))
            for table, name, definition in CONTRACT_CHECKS
        }
    )
    relevant_constraint_rows = [
        row
        for row in constraint_rows
        if str(row.get("TableName")) in {"IndexReviewRun", "IndexReviewSnapshot"}
    ]
    if (
        len(relevant_constraint_rows) != len(expected_constraints)
        or set(actual_constraints) != set(expected_constraints)
    ):
        raise IndexReviewSchemaError(
            "History contract defaults or checks do not match the installer contract."
        )
    for table, name, definition in CONTRACT_DEFAULTS:
        actual_constraint = actual_constraints.get((table, name))
        if actual_constraint != ("DEFAULT", _canonical_sql_tokens(definition)):
            raise IndexReviewSchemaError(f"History contract default {table}.{name} is missing or drifted.")
    for table, name, definition in CONTRACT_CHECKS:
        actual_constraint = actual_constraints.get((table, name))
        if actual_constraint != ("CHECK", _canonical_sql_tokens(definition)):
            raise IndexReviewSchemaError(f"History contract check {table}.{name} is missing or drifted.")

    permissions = sets[4]
    if len(permissions) != 2 or {str(row.get("TableName")) for row in permissions} != {"IndexReviewRun", "IndexReviewSnapshot"}:
        raise IndexReviewSchemaError("History contract permissions probe was incomplete.")
    def granted(value: Any) -> bool:
        return value in (1, True, "1", "GRANT", "GRANT_WITH_GRANT") or str(value).upper() in {
            "GRANT",
            "GRANT_WITH_GRANT",
        }

    dangerous_absent = all(
        not granted(row.get(permission))
        for row in permissions
        for permission in (
            "UpdateState", "DeleteState", "AlterState", "ControlState", "ExecuteState",
            "ReferencesState", "ViewDefinitionState", "TakeOwnershipState",
        )
    )
    allow_read = all(granted(row.get("SelectState")) for row in permissions)
    allow_write = all(
        granted(row.get("SelectState"))
        and granted(row.get("InsertState"))
        for row in permissions
    )
    observed_material = {
        "schema_version": INDEX_HISTORY_SCHEMA_VERSION,
        "run_columns": tuple(
            (
                str(row.get("ColumnName")),
                str(row.get("DataType")).casefold(),
                _as_int(row.get("MaxLength")),
                _as_bool(row.get("IsNullable")),
            )
            for row in columns
            if row.get("TableName") == "IndexReviewRun"
        ),
        "snapshot_columns": tuple(
            (
                str(row.get("ColumnName")),
                str(row.get("DataType")).casefold(),
                _as_int(row.get("MaxLength")),
                _as_bool(row.get("IsNullable")),
            )
            for row in columns
            if row.get("TableName") == "IndexReviewSnapshot"
        ),
        "type_precision_scale": _CONTRACT_TYPE_PRECISION_SCALE,
        "primary_keys": tuple(
            (table, tuple(column for _ordinal, column in sorted(values)))
            for (table, _name), values in index_keys.items()
            if index_flags.get((table, _name), (False, False))[0]
        ),
        "unique_indexes": tuple(
            (table, tuple(column for _ordinal, column in sorted(values)))
            for (table, _name), values in index_keys.items()
            if not index_flags.get((table, _name), (False, False))[0]
        ),
        "foreign_keys": (
            (
                "IndexReviewSnapshot",
                ("RunId",),
                "IndexReviewRun",
                ("RunId",),
            ),
        ),
        "defaults": tuple(
            (table, name, actual_constraints[(table, name)][1])
            for table, name, _definition in CONTRACT_DEFAULTS
        ),
        "checks": tuple(
            (table, name, actual_constraints[(table, name)][1])
            for table, name, _definition in CONTRACT_CHECKS
        ),
    }
    if _digest("index-history-schema-v1", observed_material) != CONTRACT_SCHEMA_FINGERPRINT:
        raise IndexReviewSchemaError("History contract live metadata fingerprint does not match the installer contract.")
    return ContractProbeResult(CONTRACT_SCHEMA_FINGERPRINT, allow_read, allow_write, dangerous_absent)


def _run_from_row(row: Mapping[str, Any]) -> IndexReviewRunV1:
    def json_object(key: str) -> dict[str, Any]:
        raw = row.get(key)
        try:
            value = json.loads(raw) if isinstance(raw, str) else raw or {}
        except json.JSONDecodeError as exc:
            raise IndexReviewIntegrityError(f"Run {row.get('RunId')} contains invalid {key}.") from exc
        if not isinstance(value, dict):
            raise IndexReviewIntegrityError(f"Run {row.get('RunId')} contains non-object {key}.")
        return value

    subject_count = row.get("SubjectCount")
    if isinstance(subject_count, bool) or not isinstance(subject_count, int):
        raise IndexReviewIntegrityError(f"Run {row.get('RunId')} contains an invalid subject count.")
    try:
        return IndexReviewRunV1(
            run_id=str(row["RunId"]),
            database_name=str(row["DatabaseName"]),
            database_fingerprint=str(row["DatabaseFingerprint"]),
            idempotency_key_hash=str(row["IdempotencyKeyHash"]),
            request_fingerprint=str(row["RequestFingerprint"]),
            observed_at_utc=str(row["ObservedAtUtc"]),
            counter_epoch_fingerprint=row.get("CounterEpochFingerprint"),
            inventory_fingerprint=str(row["InventoryFingerprint"]),
            query_store_fingerprint=str(row["QueryStoreFingerprint"]),
            contract_version=str(row.get("ContractVersion", PUBLIC_CONTRACT_VERSION)),
            schema_version=str(row.get("SchemaVersion", INDEX_HISTORY_SCHEMA_VERSION)),
            collector_version=str(row.get("CollectorVersion", INDEX_REVIEW_COLLECTOR_VERSION)),
            engine_fingerprint=row.get("EngineFingerprint"),
            engine_identity=row.get("EngineIdentity"),
            engine_start_time_utc=row.get("EngineStartTimeUtc"),
            database_incarnation_fingerprint=row.get("DatabaseIncarnationFingerprint"),
            database_incarnation_identity=row.get("DatabaseIncarnationIdentity"),
            query_store_state=str(row.get("QueryStoreState") or "unknown"),
            query_capture_mode=row.get("QueryCaptureMode"),
            observation_start_utc=row.get("ObservationStartUtc"),
            observation_end_utc=row.get("ObservationEndUtc"),
            coverage=json_object("CoverageJson"),
            subject_count=subject_count,
            snapshot_set_fingerprint=str(row["SnapshotSetFingerprint"]),
            query_store=json_object("QueryStoreJson"),
            created_at_utc=row.get("CreatedAtUtc"),
        )
    except IndexReviewIntegrityError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise IndexReviewIntegrityError(f"Run {row.get('RunId')} contains invalid history data.") from exc


def _json_value(row: Mapping[str, Any], key: str, *, nullable: bool = False) -> Any:
    raw = row.get(key)
    if raw is None and nullable:
        return None
    try:
        value = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError as exc:
        raise IndexReviewIntegrityError(f"Snapshot {row.get('SnapshotId')} contains invalid {key}.") from exc
    schema = {
        "DefinitionJson": "definition",
        "CountersJson": "counters",
        "ProtectionsJson": "protections",
        "MissingSignatureJson": "missing_signature",
        "AggregatesJson": "aggregates",
        "CoverageJson": "coverage",
    }.get(key)
    if key == "QueryStoreReferencesJson":
        if not isinstance(value, list):
            raise IndexReviewIntegrityError(
                f"Snapshot {row.get('SnapshotId')} contains non-list {key}."
            )
        return [_safe_payload(item, schema="reference") for item in value]
    return _safe_payload(value, schema=schema)


def _snapshot_from_rows(run: IndexReviewRunV1, rows: Sequence[Mapping[str, Any]]) -> IndexReviewSnapshotV1:
    subjects: list[dict[str, Any]] = []
    for row in rows:
        aggregate_payload = _json_value(row, "AggregatesJson")
        if not isinstance(aggregate_payload, dict):
            raise IndexReviewIntegrityError("Snapshot aggregate payload must be an object.")
        subject = dict(aggregate_payload)
        subject["aggregates"] = aggregate_payload
        subject.update(
            {
                "subject_id": row.get("SubjectId"),
                "subject_kind": row.get("SubjectKind"),
                "subject_fingerprint": row.get("SubjectFingerprint"),
                "object_id": row.get("ObjectId"),
                "index_id": row.get("IndexId"),
                "schema_name": row.get("SchemaName"),
                "object_name": row.get("ObjectName"),
                "table_name": row.get("ObjectName"),
                "index_name": row.get("IndexName"),
                "definition": _json_value(row, "DefinitionJson"),
                "definition_fingerprint": row.get("DefinitionFingerprint"),
                "counter_epoch_fingerprint": row.get("CounterEpochFingerprint"),
                "counters": _json_value(row, "CountersJson"),
                "observed_at_utc": row.get("ObservedAtUtc"),
                "first_observed_at_utc": row.get("FirstObservedAtUtc"),
                "last_observed_at_utc": row.get("LastObservedAtUtc"),
                "size_pages": row.get("SizePages"),
                "size_bytes": row.get("SizeBytes"),
                "write_burden": row.get("WriteBurden"),
                "query_store_references": _json_value(row, "QueryStoreReferencesJson"),
                "protections": _json_value(row, "ProtectionsJson"),
                "missing_signature": _json_value(row, "MissingSignatureJson", nullable=True),
                "coverage": _json_value(row, "CoverageJson"),
            }
        )
        if subject.get("subject_kind") == "missing_index":
            missing = _mapping_value(subject.get("missing_signature"))
            references = subject.get("query_store_references")
            reference = references[0] if isinstance(references, list) and references else {}
            definition = _mapping_value(subject.get("definition"))
            subject.update(
                {
                    "key_columns": list(
                        definition.get(
                            "key_columns",
                            list(missing.get("equality_columns", []))
                            + list(missing.get("inequality_columns", [])),
                        )
                    ),
                    "include_columns": list(
                        definition.get(
                            "include_columns", missing.get("include_columns", [])
                        )
                    ),
                    "current_score": subject["aggregates"].get("current_score"),
                    "query_store_complete": subject["coverage"].get("query_store") == "complete",
                    "recurring_executed": len(
                        subject["aggregates"].get(
                            "positive_runtime_interval_ids",
                            reference.get("runtime_interval_ids", []),
                        )
                    )
                    >= 2,
                    "runtime_interval_ids": list(reference.get("runtime_interval_ids", [])),
                    "positive_runtime_interval_ids": list(
                        subject["aggregates"].get(
                            "positive_runtime_interval_ids",
                            reference.get("runtime_interval_ids", []),
                        )
                    ),
                    "query_ids": list(reference.get("query_ids", [])),
                    "plan_ids": list(reference.get("plan_ids", [])),
                    "covered_by": list(subject["aggregates"].get("covered_by", [])),
                    "projected_database_storage_percent": subject["aggregates"].get(
                        "projected_database_storage_percent"
                    ),
                }
            )
        subjects.append(subject)
    snapshot_id = _digest("index-review-snapshot-id-v1", {"run_id": run.run_id, "manifest": _snapshot_set_fingerprint(subjects)})
    return IndexReviewSnapshotV1(
        run_id=run.run_id,
        snapshot_id=f"snapshot-{snapshot_id}",
        database_name=run.database_name,
        database_fingerprint=run.database_fingerprint,
        observed_at_utc=run.observed_at_utc,
        counter_epoch_fingerprint=run.counter_epoch_fingerprint,
        subjects=tuple(subjects),
        query_store=run.query_store,
        inventory_fingerprint=run.inventory_fingerprint,
        snapshot_fingerprint=run.snapshot_set_fingerprint,
        contract_version=run.contract_version,
        schema_version=run.schema_version,
        engine_fingerprint=run.engine_fingerprint,
        engine_identity=run.engine_identity,
        engine_start_time_utc=run.engine_start_time_utc,
        database_incarnation_fingerprint=run.database_incarnation_fingerprint,
        database_incarnation_identity=run.database_incarnation_identity,
        coverage=run.coverage,
    )


class SqlIndexHistoryRepository:
    """Async append-only repository over the two manually installed tables."""

    def __init__(self, executor: Any, database_policy: DatabasePolicySet | None = None):
        self.executor = executor
        self.database_policy = database_policy

    def _require_read(self, database_name: str) -> None:
        if self.database_policy is None:
            raise IndexReviewPolicyError("Index history requires an explicit database policy.")
        try:
            self.database_policy.require_read(database_name)
        except PermissionError as exc:
            raise IndexReviewPolicyError(str(exc)) from exc

    def _require_write(self, database_name: str) -> None:
        self._require_read(database_name)
        policy = self.database_policy.policy_for(database_name) if self.database_policy else None
        if policy is None or not policy.allow_index_history_write:
            raise IndexReviewPolicyError("Database policy does not permit index history writes.")

    async def _execute_sets(
        self,
        database_name: str,
        statement: str,
        params: Sequence[Any] = (),
    ) -> list[list[dict[str, Any]]]:
        execute_batches = getattr(self.executor, "execute_batches", None)
        if not callable(execute_batches):
            fetch_all = getattr(self.executor, "fetch_all", None)
            if not callable(fetch_all):
                raise IndexReviewSchemaError("History executor has no supported read capability.")
            fetch = cast(Callable[..., Awaitable[list[dict[str, Any]]]], fetch_all)
            rows = await fetch(database_name, statement, params=params)
            return [rows]
        # Capability is selected before dispatch. An AttributeError raised by
        # the chosen implementation is a post-dispatch failure and must not
        # replay the statement through another API.
        execute = cast(Callable[..., Awaitable[Sequence[Any]]], execute_batches)
        results = await execute(database_name, statement, params=params)
        return [
            list(result.rows) if hasattr(result, "rows") else list(result)
            for result in results
        ]

    async def probe_contract(self, database_name: str, *, for_write: bool = False) -> ContractProbeResult:
        self._require_write(database_name) if for_write else self._require_read(database_name)
        probe = validate_contract_probe(
            [rows for rows in await self._execute_sets(database_name, CONTRACT_PROBE_SQL)]
        )
        permission_allowed = probe.allow_write if for_write else probe.allow_read
        if not permission_allowed:
            required = "SELECT and INSERT" if for_write else "SELECT"
            raise IndexReviewSchemaError(
                "Current database identity lacks the required "
                f"{required} permissions on both index history tables."
            )
        return probe

    async def _read_history(
        self,
        database_name: str,
        *,
        run_id: str | None = None,
    ) -> tuple[list[IndexReviewRunV1], list[IndexReviewSnapshotV1]]:
        self._require_read(database_name)
        await self.probe_contract(database_name)
        params = [database_name, run_id, run_id, database_name, run_id, run_id]
        result_sets = await self._execute_sets(database_name, HISTORY_READ_SQL, params)
        if len(result_sets) < 2:
            raise IndexReviewSchemaError("History read did not return run and snapshot sets.")
        runs = [_run_from_row(row) for row in result_sets[0]]
        if run_id is not None and not runs:
            raise IndexReviewNotFoundError(f"Unknown index review run: {run_id}.")
        snapshots: list[IndexReviewSnapshotV1] = []
        for run in runs:
            rows = [row for row in result_sets[1] if row.get("RunId") == run.run_id]
            snapshot = _snapshot_from_rows(run, rows)
            self._validate_history(run, snapshot, rows)
            snapshots.append(snapshot)
        return runs, snapshots

    @staticmethod
    def _validate_history(
        run: IndexReviewRunV1,
        snapshot: IndexReviewSnapshotV1,
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        if run.subject_count != len(rows):
            raise IndexReviewIntegrityError(f"Run {run.run_id} subject count does not match its snapshot rows.")
        if len({row.get("SnapshotId") for row in rows}) != len(rows):
            raise IndexReviewIntegrityError(f"Run {run.run_id} contains duplicate snapshot ids.")
        if len({row.get("SubjectId") for row in rows}) != len(rows):
            raise IndexReviewIntegrityError(f"Run {run.run_id} contains duplicate subject ids.")
        if run.database_fingerprint != snapshot.database_fingerprint:
            raise IndexReviewIntegrityError(f"Run {run.run_id} database fingerprint mismatch.")
        for name, value in (
            ("database_fingerprint", run.database_fingerprint),
            ("idempotency_key_hash", run.idempotency_key_hash),
            ("request_fingerprint", run.request_fingerprint),
            ("inventory_fingerprint", run.inventory_fingerprint),
            ("query_store_fingerprint", run.query_store_fingerprint),
            ("snapshot_set_fingerprint", run.snapshot_set_fingerprint),
        ):
            if not _HEX64.fullmatch(value):
                raise IndexReviewIntegrityError(f"Run {run.run_id} contains an invalid {name}.")
        for name, value in (
            ("counter_epoch_fingerprint", run.counter_epoch_fingerprint),
            ("engine_fingerprint", run.engine_fingerprint),
            ("database_incarnation_fingerprint", run.database_incarnation_fingerprint),
        ):
            if value is not None and not _HEX64.fullmatch(str(value)):
                raise IndexReviewIntegrityError(f"Run {run.run_id} contains an invalid {name}.")
        if (run.database_incarnation_identity is None) != (
            run.database_incarnation_fingerprint is None
        ):
            raise IndexReviewIntegrityError(
                f"Run {run.run_id} contains incomplete database incarnation identity."
            )
        if run.database_incarnation_identity is not None and run.database_incarnation_fingerprint != database_incarnation_fingerprint(run.database_incarnation_identity):
            raise IndexReviewIntegrityError(
                f"Run {run.run_id} contains an invalid database incarnation fingerprint."
            )
        if (run.engine_identity is None) != (run.engine_start_time_utc is None):
            raise IndexReviewIntegrityError(
                f"Run {run.run_id} contains incomplete engine identity/start metadata."
            )
        if run.engine_identity is not None and run.engine_start_time_utc is not None:
            if run.engine_fingerprint != engine_fingerprint(
                run.engine_identity, run.engine_start_time_utc
            ):
                raise IndexReviewIntegrityError(
                    f"Run {run.run_id} contains inconsistent engine identity/start fingerprint."
                )
        subjects = list(snapshot.subjects)
        if run.snapshot_set_fingerprint != _snapshot_set_fingerprint(subjects):
            raise IndexReviewIntegrityError(f"Run {run.run_id} snapshot-set manifest is invalid.")
        if run.inventory_fingerprint != _inventory_fingerprint(subjects):
            raise IndexReviewIntegrityError(f"Run {run.run_id} inventory fingerprint is invalid.")
        if run.query_store_fingerprint != _digest("index-review-query-store-v1", run.query_store):
            raise IndexReviewIntegrityError(f"Run {run.run_id} Query Store fingerprint is invalid.")
        if len(subjects) != len(rows):
            raise IndexReviewIntegrityError(f"Run {run.run_id} snapshot row materialisation is incomplete.")
        for row, subject in zip(rows, subjects, strict=True):
            kind = str(subject.get("subject_kind"))
            if kind not in {"existing_index", "missing_index"}:
                raise IndexReviewIntegrityError(f"Run {run.run_id} contains an invalid subject kind.")
            if row.get("RunId") != run.run_id or row.get("SubjectId") != subject.get("subject_id"):
                raise IndexReviewIntegrityError(f"Run {run.run_id} contains a foreign or mismatched snapshot row.")
            _identifier(subject.get("subject_id"), "subject_id")
            if row.get("SnapshotId") in (None, ""):
                raise IndexReviewIntegrityError(f"Run {run.run_id} contains a missing snapshot id.")
            _identifier(row.get("SnapshotId"), "snapshot_id")
            if row.get("SnapshotId") != _snapshot_row_id(run.run_id, subject):
                raise IndexReviewIntegrityError(f"Run {run.run_id} contains a non-canonical snapshot id.")
            for name, value in (
                ("subject_fingerprint", subject.get("subject_fingerprint")),
                ("definition_fingerprint", subject.get("definition_fingerprint")),
            ):
                if not isinstance(value, str) or not _HEX64.fullmatch(value):
                    raise IndexReviewIntegrityError(f"Run {run.run_id} contains an invalid {name}.")
            if subject.get("subject_fingerprint") != _subject_fingerprint(kind, subject):
                raise IndexReviewIntegrityError(f"Run {run.run_id} contains a subject fingerprint mismatch.")
            if not isinstance(subject.get("definition"), Mapping):
                raise IndexReviewIntegrityError(f"Run {run.run_id} contains invalid definition JSON.")
            definition = _mapping_value(subject.get("definition"))
            embedded_definition_fingerprint = definition.get("definition_fingerprint")
            if embedded_definition_fingerprint is not None and embedded_definition_fingerprint != subject.get("definition_fingerprint"):
                raise IndexReviewIntegrityError(f"Run {run.run_id} contains a definition fingerprint mismatch.")
            reversible = definition.get("reversible_definition")
            reversible_fingerprint = definition.get("reversible_definition_fingerprint_v1")
            if reversible is not None:
                if not isinstance(reversible, Mapping) or not isinstance(reversible_fingerprint, str):
                    raise IndexReviewIntegrityError(f"Run {run.run_id} contains invalid reversible definition JSON.")
                expected_reversible = _reversible_definition_fingerprint(reversible)
                if reversible_fingerprint != expected_reversible:
                    raise IndexReviewIntegrityError(f"Run {run.run_id} contains a reversible definition fingerprint mismatch.")
                if kind == "existing_index":
                    if _definition_fingerprint_from_reversible(reversible) != subject.get(
                        "definition_fingerprint"
                    ):
                        raise IndexReviewIntegrityError(
                            f"Run {run.run_id} contains a definition fingerprint mismatch."
                        )
            elif kind == "missing_index":
                missing = subject.get("missing_signature")
                if isinstance(missing, Mapping):
                    expected_missing = _digest(
                        "index-review-create-definition-v1",
                        {
                            "schema_name": subject.get("schema_name"),
                            "table_name": subject.get("table_name"),
                            "equality_columns": missing.get("equality_columns", []),
                            "inequality_columns": missing.get("inequality_columns", []),
                            "include_columns": missing.get("include_columns", []),
                            "filter_signature": missing.get("filter_signature"),
                            "index_type": "NONCLUSTERED",
                        },
                    )
                    if expected_missing != subject.get("definition_fingerprint"):
                        raise IndexReviewIntegrityError(
                            f"Run {run.run_id} contains a missing-definition fingerprint mismatch."
                        )
            for name in ("counters", "protections", "aggregates", "coverage"):
                if not isinstance(subject.get(name), Mapping):
                    raise IndexReviewIntegrityError(f"Run {run.run_id} contains invalid {name} JSON.")
            if not isinstance(subject.get("query_store_references"), list):
                raise IndexReviewIntegrityError(f"Run {run.run_id} contains invalid Query Store reference JSON.")
            if subject.get("missing_signature") is not None and not isinstance(subject.get("missing_signature"), Mapping):
                raise IndexReviewIntegrityError(f"Run {run.run_id} contains invalid missing signature JSON.")
            for name in ("size_pages", "size_bytes", "write_burden"):
                value = subject.get(name)
                if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                    raise IndexReviewIntegrityError(f"Run {run.run_id} contains invalid non-negative {name}.")
            for name in ("observed_at_utc", "first_observed_at_utc", "last_observed_at_utc"):
                value = subject.get(name)
                if value is not None:
                    _timestamp(value, name)

    async def list_history(self, database_name: str) -> list[tuple[IndexReviewRunV1, IndexReviewSnapshotV1]]:
        runs, snapshots = await self._read_history(database_name)
        by_run = {snapshot.run_id: snapshot for snapshot in snapshots}
        result = [(run, by_run[run.run_id]) for run in runs if run.run_id in by_run or run.subject_count == 0]
        return sorted(result, key=lambda pair: (pair[0].observed_at_utc, pair[0].run_id))

    async def get_capture_by_idempotency(
        self,
        database_name: str,
        key_hash: str,
        *,
        request_fingerprint: str | None = None,
        database_fingerprint_value: str | None = None,
    ) -> CaptureResult | None:
        self._require_read(database_name)
        await self.probe_contract(database_name)
        if database_fingerprint_value is None:
            server = getattr(getattr(self.executor, "config", None), "server", "")
            database_fingerprint_value = database_fingerprint(str(server), database_name)
        result_sets = await self._execute_sets(
            database_name,
            CAPTURE_READ_SQL,
            [database_fingerprint_value, key_hash, database_fingerprint_value, key_hash],
        )
        for row in result_sets[0] if result_sets else []:
            if row.get("IdempotencyKeyHash") != key_hash:
                continue
            run = _run_from_row(row)
            if request_fingerprint is not None and run.request_fingerprint != request_fingerprint:
                raise IndexReviewIdempotencyConflictError("Index history idempotency key was replayed with different request material.")
            snapshot_rows = [item for item in result_sets[1] if item.get("RunId") == run.run_id]
            snapshot = _snapshot_from_rows(run, snapshot_rows)
            self._validate_history(run, snapshot, snapshot_rows)
            return CaptureResult(run, snapshot, already_captured=True)
        return None

    async def append_capture(
        self,
        context: CaptureContext,
        collector: Callable[[SqlTransactionSession, CaptureContext], CaptureResult],
    ) -> CaptureResult:
        self._require_write(context.database_name)
        await self.probe_contract(context.database_name, for_write=True)

        def transaction(session: SqlTransactionSession) -> bool:
            locked = session.fetch_all(
                IDEMPOTENCY_LOCK_SQL,
                [context.database_fingerprint, context.idempotency_key_hash],
            )
            if locked:
                stored_request = locked[0].get("RequestFingerprint")
                if stored_request != context.request_fingerprint:
                    raise IndexReviewIdempotencyConflictError("Index history idempotency key was replayed with different request material.")
                return False
            captured = collector(session, context)
            if captured.run.run_id != context.run_id or captured.run.request_fingerprint != context.request_fingerprint:
                raise IndexReviewIntegrityError("Capture collector returned an unexpected run identity.")
            run = captured.run
            snapshot = captured.snapshot
            if (
                run.database_name != context.database_name
                or run.database_fingerprint != context.database_fingerprint
                or run.idempotency_key_hash != context.idempotency_key_hash
                or snapshot.run_id != run.run_id
                or snapshot.database_name != run.database_name
                or snapshot.database_fingerprint != run.database_fingerprint
                or run.subject_count != len(snapshot.subjects)
                or run.snapshot_set_fingerprint != _snapshot_set_fingerprint(snapshot.subjects)
                or run.inventory_fingerprint != _inventory_fingerprint(snapshot.subjects)
                or run.query_store_fingerprint != _digest("index-review-query-store-v1", run.query_store)
            ):
                raise IndexReviewIntegrityError("Capture collector returned inconsistent history material.")
            for subject in snapshot.subjects:
                kind = str(subject.get("subject_kind"))
                if kind not in {"existing_index", "missing_index"}:
                    raise IndexReviewIntegrityError("Capture collector returned an invalid subject kind.")
                if subject.get("subject_fingerprint") != _subject_fingerprint(kind, subject):
                    raise IndexReviewIntegrityError("Capture collector returned an invalid subject fingerprint.")
                if not isinstance(subject.get("definition"), Mapping):
                    raise IndexReviewIntegrityError("Capture collector returned invalid definition JSON.")
            session.execute(INSERT_RUN_SQL, _run_params(run))
            for subject in snapshot.subjects:
                session.execute(INSERT_SNAPSHOT_SQL, _snapshot_params(run, subject))
            return True

        try:
            inserted = await self.executor.execute_transaction_exactly_once(
                context.database_name,
                transaction,
            )
        except TransactionCommitOutcomeUnknownError:
            try:
                reconciled = await self.get_capture_by_idempotency(
                    context.database_name,
                    context.idempotency_key_hash,
                    request_fingerprint=context.request_fingerprint,
                    database_fingerprint_value=context.database_fingerprint,
                )
            except IndexReviewIdempotencyConflictError:
                raise
            except Exception as exc:
                raise IndexReviewOutcomeUnknownError("Capture commit outcome could not be reconciled.") from exc
            if reconciled is None:
                raise IndexReviewOutcomeUnknownError("Capture commit outcome could not be reconciled.")
            return replace(reconciled, reconciled=True, already_captured=True)
        except IndexReviewIdempotencyConflictError:
            raise
        except IndexReviewIntegrityError:
            raise
        except IndexReviewCollectionError:
            raise
        except Exception as exc:
            raise IndexReviewWriteError("Index history transaction failed before commit.") from exc

        result = await self.get_capture_by_idempotency(
            context.database_name,
            context.idempotency_key_hash,
            request_fingerprint=context.request_fingerprint,
            database_fingerprint_value=context.database_fingerprint,
        )
        if result is None:
            raise IndexReviewIntegrityError("Committed capture was not readable after commit.")
        return replace(result, already_captured=not bool(inserted))

    async def get_snapshot_history(self, database_name: str) -> list[IndexReviewSnapshotV1]:
        return [snapshot for _run, snapshot in await self.list_history(database_name)]


def _run_params(run: IndexReviewRunV1) -> list[Any]:
    return [
        run.run_id,
        run.contract_version,
        run.schema_version,
        run.collector_version,
        run.database_name,
        run.database_fingerprint,
        run.database_incarnation_fingerprint,
        run.database_incarnation_identity,
        run.engine_fingerprint,
        run.engine_identity,
        run.engine_start_time_utc,
        run.idempotency_key_hash,
        run.request_fingerprint,
        run.observed_at_utc,
        run.counter_epoch_fingerprint,
        run.inventory_fingerprint,
        run.query_store_fingerprint,
        run.query_store_state,
        run.query_capture_mode,
        run.observation_start_utc,
        run.observation_end_utc,
        _canonical(run.coverage),
        run.subject_count,
        run.snapshot_set_fingerprint,
        _canonical(run.query_store),
    ]


def _snapshot_params(run: IndexReviewRunV1, subject: Mapping[str, Any]) -> list[Any]:
    missing = subject.get("missing_signature")

    def nonnegative(name: str) -> int | None:
        value = subject.get(name)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise IndexReviewIntegrityError(f"Capture contains invalid non-negative {name}.")
        return value

    return [
        _snapshot_row_id(run.run_id, subject),
        run.run_id,
        subject.get("subject_id"),
        subject.get("subject_kind"),
        subject.get("subject_fingerprint"),
        subject.get("object_id"),
        subject.get("index_id"),
        subject.get("schema_name"),
        subject.get("object_name"),
        subject.get("index_name"),
        _canonical(subject.get("definition", {})),
        subject.get("definition_fingerprint"),
        subject.get("counter_epoch_fingerprint"),
        _canonical(subject.get("counters", {})),
        subject.get("observed_at_utc") or run.observed_at_utc,
        subject.get("first_observed_at_utc"),
        subject.get("last_observed_at_utc"),
        nonnegative("size_pages"),
        nonnegative("size_bytes"),
        nonnegative("write_burden"),
        _canonical(subject.get("query_store_references", [])),
        _canonical(subject.get("protections", {})),
        _canonical(missing) if missing is not None else None,
        _canonical(subject.get("aggregates", {})),
        _canonical(subject.get("coverage", {})),
    ]


class IndexReviewService:
    """Capture through the SQL repository and classify through pure functions."""

    def __init__(
        self,
        executor: Any,
        repository: SqlIndexHistoryRepository,
        *,
        database_policy: DatabasePolicySet | None = None,
        recommendations: IndexRecommendationService | None = None,
        query_store: Any | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.executor = executor
        self.repository = repository
        self.database_policy = database_policy
        self.recommendations = recommendations
        self.query_store = query_store
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def _require_read(self, database_name: str) -> None:
        if self.database_policy is None:
            raise IndexReviewPolicyError("Index review requires an explicit database policy.")
        try:
            self.database_policy.require_read(database_name)
        except PermissionError as exc:
            raise IndexReviewPolicyError(str(exc)) from exc

    def _require_capture(self, database_name: str) -> int:
        self._require_read(database_name)
        policy = self.database_policy.policy_for(database_name) if self.database_policy else None
        if policy is None or not policy.allow_index_history_write:
            raise IndexReviewPolicyError("Database policy does not permit index history writes.")
        return MIN_OBSERVATION_DAYS + policy.business_cycle_extension_days

    async def capture_snapshot(self, database_name: str, idempotency_key: str | None = None) -> CaptureResult:
        minimum_days = self._require_capture(database_name)
        moment = self.clock()
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        moment = moment.astimezone(timezone.utc)
        server = getattr(getattr(self.executor, "config", None), "server", "")
        dbfp = database_fingerprint(str(server), database_name)
        key = idempotency_key or daily_idempotency_key(dbfp, moment)
        key_hash = idempotency_key_hash(dbfp, key)
        request_fp = _digest(
            "index-review-capture-request-v2",
            {
                "contract_version": PUBLIC_CONTRACT_VERSION,
                "schema_version": INDEX_HISTORY_SCHEMA_VERSION,
                "collector_version": INDEX_REVIEW_COLLECTOR_VERSION,
                "database_fingerprint": dbfp,
                "idempotency_key_hash": key_hash,
                "minimum_observation_days": minimum_days,
            },
        )
        context = CaptureContext(
            database_name=database_name,
            database_fingerprint=dbfp,
            run_id=f"run-{_digest('index-review-run-v2', {'database_fingerprint': dbfp, 'idempotency_key_hash': key_hash})}",
            idempotency_key_hash=key_hash,
            request_fingerprint=request_fp,
            observed_at_utc=moment.isoformat().replace("+00:00", "Z"),
            minimum_observation_days=minimum_days,
        )
        try:
            return await self.repository.append_capture(context, self._collect_capture)
        except (IndexReviewError, TransactionCommitOutcomeUnknownError):
            raise
        except Exception as exc:
            raise IndexReviewCollectionError("Index review telemetry collection failed.") from exc

    def _collect_capture(self, session: SqlTransactionSession, context: CaptureContext) -> CaptureResult:
        try:
            index_rows = session.fetch_all(EXISTING_INDEX_METADATA_SQL)
            engine_rows = session.fetch_all(ENGINE_START_TIME_SQL)
            protection_rows = session.fetch_all(INDEX_PROTECTION_METADATA_SQL)
            try:
                incarnation_rows = session.fetch_all(DATABASE_INCARNATION_SQL)
            except Exception:
                incarnation_rows = []
            engine_row = engine_rows[0] if engine_rows else {}
            engine_start = engine_row.get("engine_start_time_utc")
            engine_identity = engine_row.get("engine_identity")
            incarnation_identity = (
                incarnation_rows[0].get("database_incarnation_identity")
                if incarnation_rows
                else None
            )
            epoch_fingerprint = (
                _digest("index-review-epoch-v1", engine_start)
                if engine_start
                else None
            )
            usage_context = {
                "source": "sys.dm_db_index_usage_stats",
                "counter_epoch_source": "sys.dm_os_sys_info.sqlserver_start_time",
                "availability": "available" if engine_start else "unavailable",
                "coverage": "unknown" if engine_start else "unavailable",
                "counter_epoch_utc": engine_start,
                "counter_epoch_fingerprint": epoch_fingerprint,
                "engine_identity": engine_identity,
                "engine_start_time_utc": engine_start,
            }
            indexes = parse_existing_index_rows(
                index_rows,
                observation_window_minutes=context.minimum_observation_days * 1440,
                usage_context=usage_context,
                protection_evidence=parse_protection_evidence(protection_rows),
            )
        except Exception as exc:
            raise IndexReviewCollectionError("Index metadata collection failed.") from exc

        query_store, references, missing_rows = self._collect_query_store(session, context)
        dmv_rows, dmv_coverage = self._collect_missing_index_dmv(session)
        candidate_rows = self._merge_missing_index_rows(missing_rows, dmv_rows)
        for row in candidate_rows:
            scoring_blockers = list(row.get("scoring_blockers", []))
            if _as_optional_int(row.get("estimated_size_bytes")) is None:
                estimated_size = self._estimate_candidate_size(session, row)
                if estimated_size is not None:
                    row["estimated_size_bytes"] = estimated_size
                    row["estimated_size_mb"] = estimated_size / (1024.0 * 1024.0)
                elif not _as_bool(row.get("dmv_only")):
                    scoring_blockers.append("index_candidate_size_unavailable")
            if (
                not _as_bool(row.get("dmv_only"))
                and _as_float(row.get("write_ratio")) is None
            ):
                row["write_ratio"] = self._collect_candidate_write_ratio(session, row)
                if row["write_ratio"] is None:
                    scoring_blockers.append(
                        "index_candidate_write_ratio_unavailable"
                    )
            if _as_bool(row.get("dmv_only")):
                row["current_score"] = None
            else:
                row["current_score"] = score_index_candidate(
                    _as_float(row.get("statement_subtree_cost")),
                    _as_float(row.get("execution_count")),
                    _as_float(row.get("impact_pct")),
                    _as_float(row.get("estimated_size_mb")),
                    _as_float(row.get("write_ratio")),
                )
                if row["current_score"] is None:
                    scoring_blockers.append(
                        "index_candidate_scoring_inputs_incomplete"
                    )
            row["scoring_blockers"] = list(dict.fromkeys(scoring_blockers))
            query_store_coverage = query_store.get("coverage", {})
            if isinstance(query_store_coverage, dict):
                for blocker in row["scoring_blockers"]:
                    if blocker == "impact_pct_below_existing_floor":
                        continue
                    query_store_coverage["status"] = "incomplete"
                    query_store["complete"] = False
                    blockers = query_store_coverage.setdefault("blockers", [])
                    if blocker not in blockers:
                        blockers.append(blocker)
        storage = self._collect_storage(session)
        hint_coverage, hint_evidence = self._collect_hints(
            session,
            indexes,
            observation_window_minutes=context.minimum_observation_days * 1440,
        )
        redacted_indexes = []
        for index in indexes:
            matching_refs = [
                reference
                for reference in references
                if reference.get("schema_name") == index.schema
                and reference.get("object_name") == index.table
                and reference.get("index_name") == index.name
            ]
            redacted_indexes.append(
                _redacted_index(
                    index,
                    query_store_references=matching_refs,
                    hint_coverage=hint_coverage,
                    query_store_coverage=query_store.get("coverage", {}),
                    hint_evidence=hint_evidence,
                )
            )
        missing = [
            _redacted_candidate(
                row,
                query_store_coverage=query_store.get("coverage", {}),
                existing_indexes=indexes,
                storage=storage,
            )
            for row in candidate_rows
        ]
        subjects = tuple(redacted_indexes + missing)
        if len(subjects) > MAX_SUBJECTS:
            raise IndexReviewCollectionError(
                "Index review subject cap was exceeded; no partial run was stored."
            )
        snapshot_set = _snapshot_set_fingerprint(subjects)
        inventory = _inventory_fingerprint(subjects)
        epoch = _digest("index-review-epoch-v1", engine_start) if engine_start else None
        query_store_payload = dict(query_store)
        query_store_payload.pop("raw", None)
        query_store_fp = _digest("index-review-query-store-v1", query_store_payload)
        usage_states = [
            str(index.usage_context.get("coverage", "incomplete"))
            for index in indexes
        ]
        usage_coverage = (
            "complete"
            if not usage_states or all(state == "covered" for state in usage_states)
            else "partial"
            if any(state == "partial" for state in usage_states)
            else "incomplete"
        )
        protection_coverage = (
            "complete"
            if protection_rows
            and all(
                item.get("coverage") == "complete"
                for item in parse_protection_evidence(protection_rows).values()
            )
            else "incomplete"
        )
        coverage = {
            "query_store": query_store.get("coverage", {}),
            "hints": hint_coverage,
            "storage": storage.get("coverage", "incomplete"),
            "protection": protection_coverage,
            "usage": usage_coverage,
            "missing_index_dmv": dmv_coverage,
            "database_incarnation": "complete" if incarnation_identity else "incomplete",
            "engine": "complete" if engine_identity and engine_start else "incomplete",
            # A single capture has no inter-capture gap; later projections
            # compute and persist the observed gap facts in their gates.
            "gaps": [],
            "malformed": list(query_store.get("coverage", {}).get("blockers", [])),
        }
        run = IndexReviewRunV1(
            run_id=context.run_id,
            database_name=context.database_name,
            database_fingerprint=context.database_fingerprint,
            idempotency_key_hash=context.idempotency_key_hash,
            request_fingerprint=context.request_fingerprint,
            observed_at_utc=context.observed_at_utc,
            counter_epoch_fingerprint=epoch,
            inventory_fingerprint=inventory,
            query_store_fingerprint=query_store_fp,
            engine_fingerprint=(
                engine_fingerprint(str(engine_identity), engine_start)
                if engine_identity and engine_start
                else None
            ),
            engine_identity=str(engine_identity) if engine_identity else None,
            engine_start_time_utc=engine_start,
            database_incarnation_fingerprint=(
                database_incarnation_fingerprint(str(incarnation_identity))
                if incarnation_identity
                else None
            ),
            database_incarnation_identity=(
                str(incarnation_identity) if incarnation_identity else None
            ),
            query_store_state=str(query_store.get("state", "unknown")),
            query_capture_mode=query_store.get("capture_mode"),
            observation_start_utc=(moment_minus_days(context.observed_at_utc, context.minimum_observation_days)),
            observation_end_utc=context.observed_at_utc,
            coverage=coverage,
            subject_count=len(subjects),
            snapshot_set_fingerprint=snapshot_set,
            query_store=query_store_payload,
            created_at_utc=context.observed_at_utc,
        )
        snapshot = IndexReviewSnapshotV1(
            run_id=run.run_id,
            snapshot_id=f"snapshot-{_digest('index-review-snapshot-v1', {'run_id': run.run_id, 'manifest': snapshot_set})}",
            database_name=run.database_name,
            database_fingerprint=run.database_fingerprint,
            observed_at_utc=run.observed_at_utc,
            counter_epoch_fingerprint=run.counter_epoch_fingerprint,
            subjects=subjects,
            query_store=query_store_payload,
            inventory_fingerprint=inventory,
            snapshot_fingerprint=snapshot_set,
            engine_fingerprint=run.engine_fingerprint,
            engine_identity=run.engine_identity,
            engine_start_time_utc=run.engine_start_time_utc,
            database_incarnation_fingerprint=run.database_incarnation_fingerprint,
            database_incarnation_identity=run.database_incarnation_identity,
            coverage=coverage,
        )
        return CaptureResult(run, snapshot)

    @staticmethod
    def _collect_query_store(
        session: SqlTransactionSession,
        context: CaptureContext,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        try:
            status_rows = session.fetch_all(
                """
                SELECT actual_state_desc, desired_state_desc, query_capture_mode_desc,
                       current_storage_size_mb, max_storage_size_mb,
                       stale_query_threshold_days
                FROM sys.database_query_store_options;
                """
            )
            status = status_rows[0] if status_rows else {}
        except Exception as exc:
            return (
                {
                    "state": "unavailable",
                    "coverage": {"status": "incomplete", "blockers": ["query_store_status_unavailable"], "error_type": type(exc).__name__},
                    "enabled": False,
                },
                [],
                [],
            )
        actual_state = str(status.get("actual_state_desc", "OFF")).upper()
        capture_mode = str(status.get("query_capture_mode_desc") or "").upper()
        enabled = actual_state not in {"OFF", "ERROR", ""}
        window_minutes = context.minimum_observation_days * 1440
        retention_days = _as_optional_int(status.get("stale_query_threshold_days"))
        try:
            runtime_rows = session.fetch_all(QUERY_STORE_WINDOW_SQL, [window_minutes])
        except Exception as exc:
            runtime_rows = []
            runtime_error = type(exc).__name__.lower()
        else:
            runtime_error = None
        runtime_row = runtime_rows[0] if runtime_rows else {}
        runtime_start = _parse_utc_timestamp(runtime_row.get("window_start_utc"))
        runtime_end = _parse_utc_timestamp(runtime_row.get("window_end_utc"))
        runtime_intervals = _as_optional_int(runtime_row.get("runtime_interval_count"))
        required_runtime_minutes = SNAPSHOT_REUSE_HOURS * 60
        runtime_complete = (
            runtime_start is not None
            and runtime_end is not None
            and runtime_end >= runtime_start
            and (runtime_end - runtime_start).total_seconds() >= required_runtime_minutes * 60
            and (runtime_intervals or 0) > 0
        )
        runtime_window: dict[str, Any] = {
            "status": "complete" if runtime_complete else "incomplete",
            "window_start_utc": (
                _timestamp(runtime_start, "window_start_utc")
                if runtime_start is not None
                else None
            ),
            "window_end_utc": (
                _timestamp(runtime_end, "window_end_utc")
                if runtime_end is not None
                else None
            ),
            "runtime_interval_count": runtime_intervals or 0,
            "requested_window_minutes": window_minutes,
            "required_window_minutes": required_runtime_minutes,
        }
        if runtime_error:
            runtime_window["error_type"] = runtime_error
        try:
            rows = session.fetch_all(INDEX_EVIDENCE_QUERY, [MAX_CAPTURE_ROWS + 1, window_minutes])
        except Exception as exc:
            return (
                {
                    "state": status.get("actual_state_desc", "unknown"),
                    "capture_mode": status.get("query_capture_mode_desc"),
                    "enabled": enabled,
                    "retention_days": retention_days,
                    "stale_query_threshold_days": retention_days,
                    "window_minutes": window_minutes,
                    "runtime_window": runtime_window,
                    "coverage": {"status": "incomplete", "blockers": ["query_store_plan_unavailable"], "error_type": type(exc).__name__.lower(), "runtime_window": runtime_window},
                },
                [],
                [],
            )
        references: dict[tuple[Any, ...], dict[str, Any]] = {}
        candidates: dict[str, dict[str, Any]] = {}
        coverage: dict[str, Any] = {
            "status": "complete",
            "eligible": len(rows),
            "scanned": 0,
            "malformed": 0,
            "capped": len(rows) > MAX_CAPTURE_ROWS,
            "truncated": False,
            "blockers": [],
            "runtime_window": runtime_window,
            "retention_days": retention_days,
            "stale_query_threshold_days": retention_days,
            "requested_window_minutes": window_minutes,
            "required_window_minutes": required_runtime_minutes,
            "plan_evidence_empty_allowed": False,
        }
        if coverage["capped"]:
            coverage["status"] = "incomplete"
            coverage["blockers"].append("query_store_plan_cap_reached")
        if actual_state != "READ_WRITE":
            coverage["status"] = "incomplete"
            coverage["blockers"].append("query_store_state_not_read_write")
        if capture_mode != "ALL":
            coverage["status"] = "incomplete"
            coverage["blockers"].append("query_store_capture_mode_not_all")
        if retention_days is None:
            coverage["status"] = "incomplete"
            coverage["blockers"].append("query_store_stale_threshold_unavailable")
        elif retention_days < context.minimum_observation_days:
            coverage["status"] = "incomplete"
            coverage["blockers"].append("query_store_stale_threshold_insufficient")
        if not runtime_complete:
            coverage["status"] = "incomplete"
            coverage["blockers"].append("query_store_runtime_window_gap")
        if not rows and runtime_complete:
            coverage["plan_evidence_empty_allowed"] = True
        elif not rows:
            coverage["status"] = "incomplete"
            coverage["blockers"].append("query_store_plan_evidence_empty")
        for row in rows[:MAX_CAPTURE_ROWS]:
            raw_execution_count = row.get("execution_count")
            execution_count = _as_optional_int(raw_execution_count)
            if raw_execution_count is not None and execution_count is None:
                execution_count = -1
            parsed = parse_showplan_index_evidence(
                str(row.get("query_plan_xml") or ""),
                query_id=_as_int(row.get("query_id")),
                plan_id=_as_int(row.get("plan_id")),
                plan_hash=str(row.get("query_plan_hash")) if row.get("query_plan_hash") is not None else None,
                execution_count=execution_count,
                runtime_interval_ids=[_as_int(row.get("runtime_stats_interval_id"))] if row.get("runtime_stats_interval_id") is not None else [],
                last_seen=row.get("last_seen_utc"),
                is_forced_plan=_as_bool(row.get("is_forced_plan")),
                input_truncated=len(str(row.get("query_plan_xml") or "")) >= MAX_PLAN_XML_CHARS,
                max_xml_chars=MAX_PLAN_XML_CHARS,
            )
            parsed_coverage = parsed.get("coverage", {})
            coverage["scanned"] += 1
            coverage["malformed"] += _as_int(parsed_coverage.get("malformed"))
            for blocker in parsed_coverage.get("blockers", []):
                if blocker not in coverage["blockers"]:
                    coverage["blockers"].append(blocker)
            for reference in parsed.get("index_references", []):
                key = tuple(
                    reference.get(name)
                    for name in (
                        "query_id",
                        "plan_id",
                        "database_name",
                        "schema_name",
                        "object_name",
                        "index_name",
                    )
                )
                item = references.get(key)
                if item is None:
                    item = dict(reference)
                    item["runtime_interval_ids"] = set(
                        reference.get("runtime_interval_ids", [])
                    )
                    item["operator_kinds"] = set(
                        reference.get("operator_kinds", [])
                    )
                    operator_kind = reference.get("operator_kind")
                    if operator_kind and operator_kind != "Multiple":
                        item["operator_kinds"].add(operator_kind)
                    references[key] = item
                else:
                    item["execution_count"] = _as_int(
                        item.get("execution_count")
                    ) + _as_int(reference.get("execution_count"))
                    item.setdefault("runtime_interval_ids", set()).update(
                        reference.get("runtime_interval_ids", [])
                    )
                    operator_kinds = item.setdefault("operator_kinds", set())
                    operator_kinds.update(reference.get("operator_kinds", []))
                    operator_kind = reference.get("operator_kind")
                    if operator_kind and operator_kind != "Multiple":
                        operator_kinds.add(operator_kind)
                    last_seen = reference.get("last_seen")
                    if last_seen is not None and (
                        item.get("last_seen") is None
                        or last_seen > item["last_seen"]
                    ):
                        item["last_seen"] = last_seen
                    item["is_forced_plan"] = _as_bool(
                        item.get("is_forced_plan")
                    ) or _as_bool(reference.get("is_forced_plan"))
            for candidate in parsed.get("missing_index_candidates", []):
                signature_value = candidate.get("candidate_signature")
                if not signature_value:
                    coverage["malformed"] += 1
                    blocker = "missing_index_candidate_signature_unavailable"
                    if blocker not in coverage["blockers"]:
                        coverage["blockers"].append(blocker)
                    continue
                signature = str(signature_value)
                impact_pct = _as_float(candidate.get("impact_pct"))
                statement_cost = _as_float(row.get("statement_subtree_cost"))
                if statement_cost is None:
                    statement_cost = _extract_statement_subtree_cost(
                        str(row.get("query_plan_xml") or "")
                    )
                candidate["statement_subtree_cost"] = statement_cost
                candidate["execution_count"] = _as_float(row.get("execution_count"))
                candidate["impact_pct"] = impact_pct
                candidate["estimated_size_mb"] = _as_float(
                    row.get("estimated_index_size_mb", row.get("estimated_size_mb"))
                )
                candidate["write_ratio"] = _as_float(
                    row.get("table_write_ratio", row.get("write_ratio"))
                )
                item = candidates.get(signature)
                if item is None:
                    item = dict(candidate)
                    item["query_ids"] = set()
                    item["plan_ids"] = set()
                    item["runtime_interval_ids"] = set()
                    item["positive_runtime_interval_ids"] = set()
                    item["execution_count"] = 0.0
                    item["request_count"] = 0
                    item["current_score"] = None
                    item["_statement_costs_by_plan"] = {}
                    item["_scoring_blockers"] = list(
                        item.pop("scoring_blockers", [])
                    )
                    candidates[signature] = item
                query_id = candidate.get("query_id")
                plan_id = candidate.get("plan_id")
                if query_id is not None:
                    item["query_ids"].add(query_id)
                if plan_id is not None:
                    item["plan_ids"].add(plan_id)
                interval_ids = candidate.get("runtime_interval_ids", [])
                item["runtime_interval_ids"].update(interval_ids)
                candidate_execution_count = _as_float(
                    candidate.get("execution_count")
                )
                if candidate_execution_count is None:
                    item["_scoring_blockers"].append(
                        "execution_count_unavailable"
                    )
                else:
                    item["execution_count"] += candidate_execution_count
                    if candidate_execution_count > 0:
                        item["positive_runtime_interval_ids"].update(interval_ids)
                item["request_count"] += 1
                item["is_forced_plan"] = _as_bool(
                    item.get("is_forced_plan")
                ) or _as_bool(candidate.get("is_forced_plan"))
                current_impact = _as_float(item.get("impact_pct"))
                if impact_pct is not None and (
                    current_impact is None or impact_pct > current_impact
                ):
                    item["impact_pct"] = impact_pct
                if statement_cost is None:
                    item["_scoring_blockers"].append(
                        "statement_subtree_cost_unavailable"
                    )
                elif plan_id not in item["_statement_costs_by_plan"]:
                    item["_statement_costs_by_plan"][plan_id] = statement_cost
                for metric_name in ("estimated_size_mb", "write_ratio"):
                    metric = candidate.get(metric_name)
                    if metric is None:
                        continue
                    current_metric = item.get(metric_name)
                    if current_metric is None:
                        item[metric_name] = metric
                    elif current_metric != metric:
                        item["_scoring_blockers"].append(
                            f"{metric_name}_conflicting"
                        )
                last_seen = candidate.get("last_seen")
                if last_seen is not None and (
                    item.get("last_seen") is None
                    or last_seen > item["last_seen"]
                ):
                    item["last_seen"] = last_seen
        if coverage["malformed"] or coverage["blockers"]:
            coverage["status"] = "incomplete"
        normalized_references = []
        for reference in references.values():
            interval_ids = sorted(reference.get("runtime_interval_ids", set()))
            operator_kinds = sorted(reference.get("operator_kinds", set()))
            reference["runtime_interval_ids"] = interval_ids
            reference["operator_kinds"] = operator_kinds
            reference["operator_kind"] = (
                operator_kinds[0]
                if len(operator_kinds) == 1
                else "Multiple"
                if operator_kinds
                else None
            )
            normalized_references.append(reference)
        normalized_references.sort(
            key=lambda item: (
                _as_int(item.get("query_id")),
                _as_int(item.get("plan_id")),
                str(item.get("database_name") or ""),
                str(item.get("schema_name") or ""),
                str(item.get("object_name") or ""),
                str(item.get("index_name") or ""),
            )
        )
        result = {
            "state": status.get("actual_state_desc", "unknown"),
            "capture_mode": status.get("query_capture_mode_desc"),
            "enabled": enabled,
            "retention_days": retention_days,
            "stale_query_threshold_days": retention_days,
            "window_minutes": window_minutes,
            "runtime_window": runtime_window,
            "complete": coverage["status"] == "complete",
            "coverage": coverage,
            "query_count": len({item.get("query_id") for item in references.values()}),
            "executed_count": sum(_as_int(item.get("execution_count")) for item in candidates.values()),
        }
        normalized_candidates = []
        for candidate in candidates.values():
            statement_costs = candidate.pop("_statement_costs_by_plan", {})
            candidate["statement_subtree_cost"] = (
                sum(statement_costs.values()) if statement_costs else None
            )
            scoring_blockers = list(
                dict.fromkeys(candidate.pop("_scoring_blockers", []))
            )
            impact_pct = _as_float(candidate.get("impact_pct"))
            if impact_pct is None:
                scoring_blockers.append("impact_pct_unavailable")
            elif impact_pct < INDEX_CANDIDATE_IMPACT_FLOOR_PCT:
                scoring_blockers.append("impact_pct_below_existing_floor")
            candidate["query_ids"] = sorted(candidate.get("query_ids", set()))
            candidate["plan_ids"] = sorted(candidate.get("plan_ids", set()))
            candidate["runtime_interval_ids"] = sorted(
                candidate.get("runtime_interval_ids", set())
            )
            candidate["positive_runtime_interval_ids"] = sorted(
                candidate.get("positive_runtime_interval_ids", set())
            )
            candidate["recurring"] = len(candidate["positive_runtime_interval_ids"]) >= 2
            candidate["current_score"] = None
            if scoring_blockers:
                candidate["scoring_blockers"] = list(
                    dict.fromkeys(scoring_blockers)
                )
                for blocker in candidate["scoring_blockers"]:
                    if blocker == "impact_pct_below_existing_floor":
                        continue
                    coverage["status"] = "incomplete"
                    if blocker not in coverage["blockers"]:
                        coverage["blockers"].append(blocker)
            normalized_candidates.append(candidate)
        normalized_candidates.sort(
            key=lambda item: (
                not bool(item["recurring"]),
                -(_as_float(item.get("execution_count")) or 0.0),
                str(item["candidate_signature"]),
            )
        )
        result["complete"] = coverage["status"] == "complete"
        return result, normalized_references, normalized_candidates

    @staticmethod
    def _collect_missing_index_dmv(
        session: SqlTransactionSession,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        coverage: dict[str, Any] = {
            "status": "complete",
            "eligible": 0,
            "scanned": 0,
            "capped": False,
            "truncated": False,
            "malformed": 0,
            "blockers": [],
        }
        try:
            rows = session.fetch_all(MISSING_INDEX_DMV_SQL, [MAX_CAPTURE_ROWS + 1])
        except Exception as exc:
            return [], {
                **coverage,
                "status": "incomplete",
                "blockers": ["missing_index_dmv_unavailable", type(exc).__name__],
            }
        coverage["eligible"] = len(rows)
        if len(rows) > MAX_CAPTURE_ROWS:
            coverage["status"] = "incomplete"
            coverage["capped"] = True
            coverage["blockers"].append("missing_index_dmv_cap_reached")
            rows = rows[:MAX_CAPTURE_ROWS]
        coverage["scanned"] = len(rows)
        clean: list[dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            if not row.get("schema_name") or not row.get("table_name"):
                coverage["malformed"] += 1
                coverage["blockers"].append("missing_index_dmv_identity_malformed")
                continue
            row["dmv_only"] = True
            row["dmv_user_seeks"] = _as_int(row.get("user_seeks"))
            row["dmv_user_scans"] = _as_int(row.get("user_scans"))
            row["dmv_avg_user_impact"] = _as_float(row.get("avg_user_impact"))
            row["dmv_avg_total_user_cost"] = _as_float(row.get("avg_total_user_cost"))
            clean.append(row)
        if coverage["malformed"]:
            coverage["status"] = "incomplete"
        return clean, coverage

    @staticmethod
    def _estimate_candidate_size(
        session: SqlTransactionSession,
        row: Mapping[str, Any],
    ) -> int | None:
        schema = str(row.get("schema_name") or "")
        table = str(row.get("table_name") or row.get("object_name") or "")
        if not schema or not table:
            return None
        columns = list(
            dict.fromkeys(
                [
                    *_column_names(row.get("equality_columns")),
                    *_column_names(row.get("inequality_columns")),
                    *_column_names(
                        row.get("include_columns") or row.get("included_columns")
                    ),
                ]
            )
        )
        if not columns or len(columns) > _MAX_INDEX_CANDIDATE_COLUMNS:
            return None
        try:
            rows = session.fetch_all(
                INDEX_CANDIDATE_ROW_COUNT_SQL,
                [1, schema, table],
            )
        except Exception:
            return None
        if not rows:
            return None
        row_count = _as_optional_int(rows[0].get("row_count"))
        if row_count is None or row_count < 0:
            return None
        if row_count == 0:
            return 0
        try:
            width_rows = session.fetch_all(
                _build_index_candidate_column_widths_sql(len(columns)),
                [
                    _MAX_INDEX_CANDIDATE_COLUMNS + 1,
                    schema,
                    table,
                    *columns,
                ],
            )
        except Exception:
            return None
        widths: dict[str, int] = {}
        for width_row in width_rows:
            column_name = width_row.get("column_name")
            width = _as_optional_int(width_row.get("max_length"))
            if isinstance(column_name, str) and width is not None and width >= 0:
                widths[column_name] = width
        if any(column not in widths for column in columns):
            return None
        total_width = (
            sum(widths[column] for column in columns)
            + _INDEX_CANDIDATE_ROW_OVERHEAD
        )
        rows_per_page = max(
            1,
            _INDEX_CANDIDATE_USABLE_PAGE_BYTES
            // (total_width + _INDEX_CANDIDATE_SLOT_ARRAY_ENTRY),
        )
        leaf_pages = math.ceil(row_count / rows_per_page)
        estimate = math.ceil(
            leaf_pages
            * _INDEX_CANDIDATE_PAGE_SIZE_BYTES
            * _INDEX_CANDIDATE_NON_LEAF_MULTIPLIER
        )
        return estimate if estimate <= 9_000_000_000_000_000_000 else None

    @staticmethod
    def _collect_candidate_write_ratio(
        session: SqlTransactionSession,
        row: Mapping[str, Any],
    ) -> float | None:
        schema = str(row.get("schema_name") or "")
        table = str(row.get("table_name") or row.get("object_name") or "")
        if not schema or not table:
            return None
        try:
            rows = session.fetch_all(
                INDEX_CANDIDATE_WRITE_RATIO_SQL,
                [1, schema, table],
            )
        except Exception:
            return None
        value = _as_float(rows[0].get("write_ratio")) if rows else None
        return value if value is not None and 0 <= value <= 1 else None

    @staticmethod
    def _merge_missing_index_rows(
        query_store_rows: Sequence[Mapping[str, Any]],
        dmv_rows: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for source_rows in (query_store_rows, dmv_rows):
            for source in source_rows:
                signature = _candidate_signature(source)
                current = merged.setdefault(signature, dict(source))
                if source.get("dmv_only") is True:
                    current["dmv_user_seeks"] = source.get("dmv_user_seeks")
                    current["dmv_user_scans"] = source.get("dmv_user_scans")
                    current["dmv_avg_user_impact"] = source.get("dmv_avg_user_impact")
                    current["dmv_avg_total_user_cost"] = source.get("dmv_avg_total_user_cost")
                else:
                    current["dmv_only"] = False
        return list(merged.values())

    @staticmethod
    def _collect_storage(session: SqlTransactionSession) -> dict[str, Any]:
        try:
            rows = session.fetch_all(
                """
                SELECT
                    CONVERT(bigint, DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes')) AS max_size_bytes,
                    SUM(CONVERT(bigint, size)) * CONVERT(bigint, 8192) AS used_size_bytes
                FROM sys.database_files
                WHERE type = 0;
                """
            )
        except Exception as exc:
            return {"coverage": "incomplete", "blocker": "storage_unavailable", "error_type": type(exc).__name__}
        row = rows[0] if rows else {}
        max_size = _as_optional_int(row.get("max_size_bytes"))
        used_size = _as_optional_int(row.get("used_size_bytes"))
        if max_size is None or max_size <= 0:
            max_size = None
        if used_size is None or used_size < 0:
            used_size = None
        used_percent = used_size / max_size * 100.0 if max_size and used_size is not None else None
        return {
            "coverage": "complete" if max_size and used_size is not None else "incomplete",
            "max_size_bytes": max_size,
            "used_size_bytes": used_size,
            "used_percent": used_percent,
        }

    @staticmethod
    def _collect_hints(
        session: SqlTransactionSession,
        indexes: Sequence[ExistingIndex],
        *,
        observation_window_minutes: int,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        identities = [
            {
                "object_id": index.object_id,
                "index_id": index.index_id,
                "schema": index.schema,
                "table": index.table,
                "index_name": index.name,
            }
            for index in indexes
        ]
        sources = (
            ("query_store_text", QUERY_STORE_TEXT_HINTS_SQL, [MAX_CAPTURE_ROWS + 1], "retained_query_text"),
            ("query_store_query_hints", QUERY_STORE_QUERY_HINTS_SQL, [MAX_CAPTURE_ROWS + 1], "query_hint_text"),
            ("plan_guides", PLAN_GUIDE_HINTS_SQL, [MAX_CAPTURE_ROWS + 1], "plan_guide_hints"),
            ("module_definitions", MODULE_HINTS_SQL, [MAX_CAPTURE_ROWS + 1], "module_definition"),
        )
        evidence: list[dict[str, Any]] = []
        blockers: list[str] = []
        source_coverage: dict[str, dict[str, Any]] = {}
        for source, query, params, text_key in sources:
            try:
                rows = session.fetch_all(query, params)
            except Exception as exc:
                blockers.append(f"{source}_unavailable")
                source_coverage[source] = {
                    "status": "incomplete",
                    "blockers": [f"{source}_unavailable"],
                    "error_type": type(exc).__name__,
                    "scanned": 0,
                    "capped": False,
                }
                continue
            source_blockers: list[str] = []
            capped = len(rows) > MAX_CAPTURE_ROWS
            if capped:
                source_blockers.append(f"{source}_cap_reached")
                blockers.extend(source_blockers)
            for row in rows[:MAX_CAPTURE_ROWS]:
                text_value = row.get(text_key)
                if not isinstance(text_value, str) or not text_value.strip():
                    source_blockers.append(f"{source}_text_unavailable")
                    continue
                matches, parse_blockers = _resolve_index_hints(text_value, identities)
                source_blockers.extend(parse_blockers)
                if matches:
                    evidence.append(
                        {
                            "source": source,
                            "source_id": _hint_source_id(row),
                            "text_hash": _sha256_text(text_value),
                            "resolved_indexes": matches,
                        }
                    )
            source_blockers = list(dict.fromkeys(source_blockers))
            blockers.extend(source_blockers)
            source_coverage[source] = {
                "status": "complete" if not source_blockers else "incomplete",
                "blockers": source_blockers,
                "scanned": min(len(rows), MAX_CAPTURE_ROWS),
                "capped": capped,
            }
        unique_blockers = list(dict.fromkeys(blockers))
        return (
            {
                "status": "complete" if not unique_blockers else "incomplete",
                "blockers": unique_blockers,
                "source_count": len(sources),
                "sources": source_coverage,
            },
            evidence,
        )

    async def review_portfolio(
        self,
        database_name: str,
        as_of_run_id: str | None = None,
        prior_review_id: str | None = None,
    ) -> IndexReviewV1:
        minimum_days = self._minimum_days(database_name)
        self._require_read(database_name)
        history = await self.repository.list_history(database_name)
        if not history:
            review = _missing_history_review(self, database_name, minimum_days, status="missing")
            return review
        selected = self._select_history(history, as_of_run_id)
        age = self.clock().astimezone(timezone.utc) - _parse_time(selected[0].observed_at_utc)
        if as_of_run_id is None and age > timedelta(hours=SNAPSHOT_REUSE_HOURS):
            return _missing_history_review(
                self,
                database_name,
                minimum_days,
                status="stale",
                selected_run_id=selected[0].run_id,
                stale_hours=max(0.0, age.total_seconds() / 3600.0),
                history=history,
            )
        prior = None
        if prior_review_id is not None:
            prior = await self.get_review(database_name, prior_review_id)
            if prior.prior_review_id is not None:
                raise IndexReviewPolicyError("prior_review_id must reference a base review.")
            if prior.database_fingerprint != selected[0].database_fingerprint:
                raise IndexReviewPolicyError("Prior review belongs to another database.")
            if prior.minimum_observation_days != minimum_days:
                raise IndexReviewPolicyError("Prior review uses another observation policy.")
            prior_run = next((run for run, _snapshot in history if run.run_id == prior.as_of_run_id), None)
            if prior_run is None or _parse_time(prior_run.observed_at_utc) >= _parse_time(selected[0].observed_at_utc):
                raise IndexReviewPolicyError("prior_review_id must be strictly earlier than as_of_run_id.")
        review = review_index_portfolio(
            database_name,
            [snapshot for run, snapshot in history],
            as_of_run_id=selected[0].run_id,
            prior_review=prior,
            business_cycle_extension_days=minimum_days - MIN_OBSERVATION_DAYS,
        )
        return review

    async def get_review(self, database_name: str, review_id: str) -> IndexReviewV1:
        self._require_read(database_name)
        selector = parse_review_id(review_id)
        history = await self.repository.list_history(database_name)
        selected = self._find_compact_run(history, selector["as_of"])
        if selected is None:
            raise IndexReviewNotFoundError(f"Unknown index review: {review_id}.")
        prior = None
        if selector["prior"] is not None:
            prior_pair = self._find_compact_run(history, selector["prior"])
            if prior_pair is None:
                raise IndexReviewNotFoundError(f"Unknown prior review run for: {review_id}.")
            base = review_index_portfolio(
                database_name,
                [snapshot for run, snapshot in history],
                as_of_run_id=prior_pair[0].run_id,
                business_cycle_extension_days=selector["minimum_days"] - MIN_OBSERVATION_DAYS,
            )
            prior = base
        review = review_index_portfolio(
            database_name,
            [snapshot for run, snapshot in history],
            as_of_run_id=selected[0].run_id,
            prior_review=prior,
            business_cycle_extension_days=selector["minimum_days"] - MIN_OBSERVATION_DAYS,
        )
        if review.review_id != review_id:
            raise IndexReviewIntegrityError("Review selector does not match its deterministic projection.")
        return review

    def _minimum_days(self, database_name: str) -> int:
        extension = self.database_policy.policy_for(database_name).business_cycle_extension_days if self.database_policy else 0
        return MIN_OBSERVATION_DAYS + extension

    @staticmethod
    def _select_history(
        history: Sequence[tuple[IndexReviewRunV1, IndexReviewSnapshotV1]],
        run_id: str | None,
    ) -> tuple[IndexReviewRunV1, IndexReviewSnapshotV1]:
        if run_id is None:
            return history[-1]
        for pair in history:
            if pair[0].run_id == run_id:
                return pair
        raise IndexReviewNotFoundError(f"Unknown index review run: {run_id}.")

    @staticmethod
    def _find_compact_run(
        history: Sequence[tuple[IndexReviewRunV1, IndexReviewSnapshotV1]],
        compact: str,
    ) -> tuple[IndexReviewRunV1, IndexReviewSnapshotV1] | None:
        return next((pair for pair in history if _compact(pair[0].run_id) == compact), None)


def moment_minus_days(value: str, days: int) -> str:
    return (_parse_time(value) - timedelta(days=days)).isoformat().replace("+00:00", "Z")


def _missing_history_review(
    service: IndexReviewService,
    database_name: str,
    minimum_days: int,
    *,
    status: str,
    selected_run_id: str = "missing",
    stale_hours: float | None = None,
    history: Sequence[tuple[IndexReviewRunV1, IndexReviewSnapshotV1]] = (),
) -> IndexReviewV1:
    server = getattr(getattr(service.executor, "config", None), "server", "")
    database_fp = database_fingerprint(str(server), database_name)
    history_fingerprint = _digest(
        "index-review-history-v1",
        [
            {"run_id": run.run_id, "snapshot_fingerprint": snapshot.snapshot_fingerprint}
            for run, snapshot in history
        ],
    )
    observed_at = service.clock()
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    observed_at_text = observed_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    review_id = _review_id(
        database_fp,
        selected_run_id,
        None,
        minimum_days,
        history_fingerprint,
    )
    observation: dict[str, Any] = {
        "snapshot_count": len(history),
        "as_of_observed_at_utc": (
            history[-1][0].observed_at_utc if history else observed_at_text
        ),
        "minimum_observation_days": minimum_days,
        "business_cycle_extension_days": minimum_days - MIN_OBSERVATION_DAYS,
        "history_fingerprint": history_fingerprint,
        "history_status": status,
        "next_action": (
            "Invoke capture_index_review_snapshot after policy checks."
        ),
        "state_counts": {state: 0 for state in sorted(INDEX_REVIEW_STATES)},
        "recommend_only": True,
    }
    if stale_hours is not None:
        observation["stale_hours"] = round(stale_hours, 3)
    return IndexReviewV1(
        review_id=review_id,
        database_name=database_name,
        database_fingerprint=database_fp,
        as_of_run_id=selected_run_id,
        prior_review_id=None,
        prior_base_run_id=None,
        overall_state="inconclusive",
        subjects=(),
        observation=observation,
        minimum_observation_days=minimum_days,
        history_fingerprint=history_fingerprint,
    )


def _reference_has_execution(reference: Mapping[str, Any]) -> bool:
    if reference.get("executed") is True:
        return True
    execution_count = _as_float(reference.get("execution_count"))
    return execution_count is not None and execution_count > 0


def _coverage_incomplete(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return True
    if value.get("complete") is False:
        return True
    status = str(value.get("status", "")).casefold()
    if status in {"incomplete", "unknown", "unavailable", "partial"}:
        return True
    if _as_int(value.get("malformed")) > 0 or _as_bool(value.get("capped")) or _as_bool(value.get("truncated")):
        return True
    gaps = value.get("gaps")
    if isinstance(gaps, (list, tuple, set)) and gaps:
        return True
    blockers = value.get("blockers")
    if isinstance(blockers, (list, tuple, set)) and bool(blockers):
        return True
    for key, nested in value.items():
        if key in {"sources", "query_store", "hints", "runtime_window", "storage", "missing_index_dmv"} and isinstance(nested, Mapping):
            if _coverage_incomplete(nested):
                return True
    return False


def _observation_gate(
    subject: Mapping[str, Any],
    snapshots: Sequence[IndexReviewSnapshotV1],
    *,
    minimum_days: int,
) -> dict[str, Any]:
    subject_id = subject.get("subject_id")
    ordered = sorted(snapshots, key=lambda item: (item.observed_at_utc, item.run_id))
    history_by_snapshot = [
        next((item for item in snapshot.subjects if item.get("subject_id") == subject_id), None)
        for snapshot in ordered
    ]
    observations = [item for item in history_by_snapshot if item is not None]
    times = [_parse_time(snapshot.observed_at_utc) for snapshot in ordered]
    gaps = [
        (right - left).total_seconds() / 3600.0
        for left, right in zip(times, times[1:])
    ]
    engine_epochs = {
        (snapshot.engine_identity, snapshot.engine_start_time_utc, snapshot.engine_fingerprint)
        for snapshot in ordered
    }
    database_epochs = {
        (snapshot.database_incarnation_identity, snapshot.database_incarnation_fingerprint)
        for snapshot in ordered
    }
    counter_epochs = {
        item.get("counter_epoch_fingerprint") or snapshot.counter_epoch_fingerprint
        for snapshot, item in zip(ordered, history_by_snapshot)
        if item is not None
    }
    definitions = {_subject_definition_identity(item) for item in observations}
    stable_engine = (
        len(engine_epochs) == 1
        and all(value is not None for value in next(iter(engine_epochs), (None, None, None)))
    )
    stable_database_incarnation = (
        len(database_epochs) == 1
        and all(
            value is not None
            for value in next(iter(database_epochs), (None, None))
        )
    )
    engine_start = (
        _parse_utc_timestamp(next(iter(engine_epochs))[1])
        if stable_engine
        else None
    )
    engine_covers_history = bool(times) and engine_start is not None and engine_start <= times[0]
    required_counter_keys = ("user_seeks", "user_scans", "user_lookups", "user_updates")
    usable_flags = []
    for item in history_by_snapshot:
        counters = _mapping_value(item.get("counters")) if item is not None else {}
        item_coverage = _mapping_value(item.get("coverage")) if item is not None else {}
        usable_flags.append(
            item is not None
            and all(
                _is_nonnegative_int(counters.get(key))
                for key in required_counter_keys
            )
            and (
                item_coverage.get("usage") == "covered"
                or item_coverage.get("usage") == "partial" and engine_covers_history
            )
        )
    dates = {value.date() for value in times}
    expected_dates = (
        {
            times[0].date() + timedelta(days=offset)
            for offset in range((times[-1].date() - times[0].date()).days + 1)
        }
        if times
        else set()
    )
    max_gap = max(gaps, default=None)
    elapsed_hours = (
        (times[-1] - times[0]).total_seconds() / 3600.0 if len(times) >= 2 else 0.0
    )
    enough_days = (
        len(times) >= minimum_days + 1
        and elapsed_hours >= minimum_days * 24
        and len(dates) >= minimum_days + 1
    )
    return {
        "observed_snapshot_count": len(observations),
        "expected_snapshot_count": len(ordered),
        "observation_days": len(dates),
        "minimum_observation_days": minimum_days,
        "elapsed_hours": elapsed_hours,
        "required_elapsed_hours": minimum_days * 24,
        "first_run": len(observations) <= 1,
        "no_gap_over_48_hours": bool(times) and (max_gap is None or max_gap <= 48.0),
        "daily_continuity": bool(times) and expected_dates.issubset(dates),
        "max_gap_hours": max_gap,
        "stable_engine": stable_engine and engine_covers_history,
        "stable_database_incarnation": stable_database_incarnation,
        "stable_counter_epoch": len(counter_epochs) == 1 and None not in counter_epochs,
        "stable_definition": len(definitions) == 1 and None not in definitions,
        "enough_observation_days": enough_days,
        "complete_subject_history": len(observations) == len(ordered),
        "complete_usable_counter_coverage": bool(usable_flags) and all(usable_flags),
        "reset_detected": len(counter_epochs - {None}) > 1,
        "physical_database_change": len({value for value in database_epochs if value[0] is not None}) > 1,
        "engine_epoch_change": len({value for value in engine_epochs if value[0] is not None}) > 1,
        "usage_window_covers_history": engine_covers_history,
    }


def _subject_history(subject: Mapping[str, Any], snapshots: Sequence[IndexReviewSnapshotV1]) -> list[Mapping[str, Any]]:
    subject_id = subject.get("subject_id")
    items = [
        item
        for snapshot in snapshots
        for item in snapshot.subjects
        if item.get("subject_id") == subject_id
    ]
    return sorted(items, key=lambda item: str(item.get("observed_at_utc", "")))


def _specialist_or_uncertain_protection(
    protection: Mapping[str, Any],
) -> bool:
    return (
        bool(str(protection.get("specialist_type") or "").strip())
        or _as_bool(protection.get("has_index_extended_properties"))
        or _as_bool(protection.get("extended_properties"))
        or _as_bool(protection.get("automatic_tuning"))
        or protection.get("safe_to_remove") is not True
    )


def evaluate_removal_gate(
    subject: Mapping[str, Any],
    snapshots: Sequence[IndexReviewSnapshotV1],
    *,
    minimum_days: int = MIN_OBSERVATION_DAYS,
) -> dict[str, Any]:
    definition = _mapping_value(subject.get("definition"))
    reversible = _reversible_definition(subject)
    try:
        reverse_ddl = render_reverse_index_definition(reversible)
    except (TypeError, ValueError):
        reverse_ddl = {"executable": False}
    history = _subject_history(subject, snapshots)
    observation = _observation_gate(subject, snapshots, minimum_days=minimum_days)
    filter_payload = _mapping_value(reversible.get("filter"))
    data_space = _mapping_value(reversible.get("data_space"))
    counters = [_mapping_value(item.get("counters")) for item in history]
    read_keys = ("user_seeks", "user_scans", "user_lookups")
    valid_counters = bool(counters) and all(
        all(
            _is_nonnegative_int(counter.get(key))
            for key in read_keys + ("user_updates",)
        )
        for counter in counters
    )
    nondecreasing = valid_counters and all(
        all(_as_int(right.get(key)) >= _as_int(left.get(key)) for key in read_keys + ("user_updates",))
        for left, right in zip(counters, counters[1:])
    )
    read_deltas_zero = nondecreasing and all(
        all(_as_int(right.get(key)) - _as_int(left.get(key)) == 0 for key in read_keys)
        for left, right in zip(counters, counters[1:])
    )
    historical_protections = [_mapping_value(item.get("protections")) for item in history]
    historical_coverages = [_mapping_value(item.get("coverage")) for item in history]
    references = [
        reference
        for item in history
        for reference in item.get("query_store_references", [])
        if isinstance(reference, Mapping)
    ]
    executed_reference = any(_reference_has_execution(reference) for reference in references)
    stored_plan_without_execution = bool(references) and not executed_reference
    child_fk_protected = any(
        bool(protection.get("child_foreign_key_support"))
        for protection in historical_protections
    )
    referenced_fk = any(
        bool(protection.get("referenced_foreign_key_key_index_ids"))
        for protection in historical_protections
    )
    historical_hint = any(_as_bool(protection.get("hinted_or_forced_plan")) for protection in historical_protections)
    historical_dependency = any(_as_bool(protection.get("partition_switch_dependency")) for protection in historical_protections)
    historical_protected = any(
        _specialist_or_uncertain_protection(protection)
        or any(
            _as_bool(protection.get(key))
            for key in (
                "primary_key", "unique_constraint", "indexed_view", "clustered",
                "disabled", "hypothetical", "auto_created",
                "partition_switch_dependency",
            )
        )
        for protection in historical_protections
    )
    complete_coverage = bool(historical_coverages) and all(
        not _coverage_incomplete(coverage)
        and coverage.get("query_store") == "complete"
        and coverage.get("hint") == "complete"
        and coverage.get("dependency") == "complete"
        and coverage.get("protection") == "complete"
        for coverage in historical_coverages
    )
    dangerous_type = str(reversible.get("index_type") or "").upper() != "NONCLUSTERED" or _as_int(reversible.get("index_type_code"), 2) != 2
    gate = {
        "enabled_user_created": reversible.get("is_disabled") is False and reversible.get("is_auto_created") is False and reversible.get("is_hypothetical") is False,
        "nonunique": reversible.get("is_unique") is False,
        "standalone_type_2_rowstore": (
            not dangerous_type
            and reversible.get("is_primary_key") is False
            and reversible.get("is_unique_constraint") is False
            and reversible.get("constraint_name") is None
            and reversible.get("constraint_type") is None
            and filter_payload.get("has_filter") is False
            and str(data_space.get("type") or "").upper()
            not in {"PARTITION_SCHEME", "PARTITION SCHEME"}
            and not data_space.get("partition_columns")
        ),
        "fully_reversible": (
            reverse_ddl.get("executable") is True
            and not definition.get("reversibility_blockers")
        ),
        "stable_engine_and_database": (
            observation["stable_engine"]
            and observation["stable_database_incarnation"]
            and not observation["physical_database_change"]
            and not observation["engine_epoch_change"]
        ),
        "stable_counter_epoch": observation["stable_counter_epoch"],
        "stable_definition": observation["stable_definition"],
        "continuous_usable_days": (
            observation["enough_observation_days"]
            and observation["no_gap_over_48_hours"]
            and observation["daily_continuity"]
            and observation["complete_subject_history"]
        ),
        "complete_usable_counter_coverage": (
            observation["complete_usable_counter_coverage"] and valid_counters
        ),
        "counters_never_decrease": nondecreasing,
        "zero_seek_scan_lookup_deltas": bool(counters) and read_deltas_zero,
        "measurable_write_or_storage_burden": any(_as_int(item.get("write_burden")) > 0 or _as_int(item.get("size_pages")) > 0 for item in history),
        "query_store_coverage_complete": complete_coverage and not executed_reference,
        "hint_coverage_complete": (
            complete_coverage and not historical_hint
        ),
        "dependency_coverage_complete": (
            complete_coverage and not historical_dependency
        ),
        "protection_coverage_complete": complete_coverage,
        "not_protected": not historical_protected,
        "no_foreign_key_dependency": not referenced_fk and not child_fk_protected,
        "no_historical_query_store_execution_reference": not executed_reference,
        "no_stored_plan_without_execution": not stored_plan_without_execution,
    }
    blockers = [name for name, passed in gate.items() if not passed]
    return {
        "passed": not blockers,
        "gates": gate,
        "blockers": blockers,
        "observation": observation,
        "historical_reference": executed_reference,
        "read_count": (
            sum(_as_int(counters[-1].get(key)) for key in read_keys)
            if counters and valid_counters
            else None
        ),
        "write_burden": max((_as_int(item.get("write_burden")) for item in history), default=None),
    }


def evaluate_drop_gate(subject: Mapping[str, Any], snapshots: Sequence[IndexReviewSnapshotV1], *, minimum_days: int = MIN_OBSERVATION_DAYS) -> dict[str, Any]:
    return evaluate_removal_gate(subject, snapshots, minimum_days=minimum_days)


def _query_store_complete(snapshot: IndexReviewSnapshotV1) -> bool:
    coverage = snapshot.coverage.get("query_store") if isinstance(snapshot.coverage, Mapping) else None
    return isinstance(coverage, Mapping) and coverage.get("status") == "complete"


def _definition_semantics(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return exact index semantics while excluding only physical identity."""

    return {
        key: item
        for key, item in value.items()
        if key not in {"object_id", "index_id", "index_name"}
    }


def _survivor_reference_for_subject(subject: Mapping[str, Any]) -> str | None:
    schema = subject.get("schema_name") or subject.get("schema")
    table = subject.get("table_name") or subject.get("object_name") or subject.get("table")
    name = subject.get("index_name") or subject.get("name")
    if not all(isinstance(value, str) and 0 < len(value) <= 128 for value in (schema, table, name)):
        return None
    return ".".join(_quote_identifier(value) for value in (schema, table, name))


def _definition_covers(wider: Mapping[str, Any], narrower: Mapping[str, Any]) -> bool:
    if not wider.get("key_columns") or not narrower.get("key_columns"):
        return False
    wider_semantics = _definition_semantics(wider)
    narrower_semantics = _definition_semantics(narrower)
    for name, value in narrower_semantics.items():
        if name in {"key_columns", "include_columns"}:
            continue
        if wider_semantics.get(name) != value:
            return False
    wider_keys = wider.get("key_columns", [])
    narrower_keys = narrower.get("key_columns", [])
    if len(wider_keys) < len(narrower_keys) or wider_keys[: len(narrower_keys)] != narrower_keys:
        return False
    includes = set(wider.get("include_columns", [])) | {str(item.get("name")) for item in wider_keys}
    return set(narrower.get("include_columns", [])).issubset(includes)


def _classify_create(
    candidate: Mapping[str, Any],
    current_subjects: Sequence[Mapping[str, Any]],
    snapshot: IndexReviewSnapshotV1,
) -> tuple[str, list[str], dict[str, Any]]:
    coverage = _mapping_value(candidate.get("coverage"))
    existing = [
        item
        for item in current_subjects
        if item.get("subject_kind") == "existing_index"
    ]
    covered_by = [
        item.get("index_name")
        for item in existing
        if item.get("index_name") in candidate.get("covered_by", [])
    ]
    projected = _as_float(candidate.get("projected_database_storage_percent"))
    impact_pct = _as_float(candidate.get("impact_pct"))
    model_score = score_index_candidate(
        _as_float(candidate.get("statement_subtree_cost")),
        _as_float(candidate.get("execution_count")),
        impact_pct,
        _as_float(candidate.get("estimated_index_size_mb", candidate.get("estimated_size_mb"))),
        _as_float(candidate.get("table_write_ratio")),
    )
    positive_intervals = candidate.get(
        "positive_runtime_interval_ids", candidate.get("runtime_interval_ids", [])
    )
    positive_execution_count = _as_float(candidate.get("execution_count"))
    gates = {
        "exact_request_recurs_in_two_runtime_intervals": len(set(positive_intervals)) >= 2,
        "positive_execution_evidence": positive_execution_count is not None and positive_execution_count > 0,
        "executed_plan_source": candidate.get("query_store_complete") is True and not _as_bool(candidate.get("dmv_only")),
        "exact_filter_reconstruction_available": not bool(
            _mapping_value(candidate.get("missing_signature")).get("filter_signature")
        ),
        "impact_pct_at_or_above_existing_floor": impact_pct is not None and impact_pct >= MIN_QUERY_STORE_IMPACT_PCT,
        "material_positive_existing_mcp_score": model_score is not None and model_score > 0,
        "scoring_inputs_complete": model_score is not None,
        "no_exact_or_covering_index": not covered_by,
        "projected_storage_strictly_below_90_percent": projected is not None and projected < 90.0,
        "complete_evidence": coverage.get("query_store") == "complete" and not coverage.get("malformed"),
    }
    blockers = [name for name, passed in gates.items() if not passed]
    return ("create_candidate" if not blockers else "observe", blockers, gates)


def _review_id(
    database_fingerprint_value: str,
    selected_run_id: str,
    prior_base_run_id: str | None,
    minimum_days: int,
    history_fingerprint: str,
) -> str:
    policy = _digest(
        "index-review-policy-v1",
        {
            "algorithm": INDEX_REVIEW_ALGORITHM_VERSION,
            "classifier": INDEX_REVIEW_CLASSIFIER_POLICY_VERSION,
            "minimum_days": minimum_days,
        },
    )
    material = {
        "algorithm": INDEX_REVIEW_ALGORITHM_VERSION,
        "classifier": INDEX_REVIEW_CLASSIFIER_POLICY_VERSION,
        "minimum_days": minimum_days,
        "database_fingerprint": database_fingerprint_value,
        "as_of_run_id": selected_run_id,
        "prior_base_run_id": prior_base_run_id,
        "history_fingerprint": history_fingerprint,
    }
    signature = _compact(_digest("index-review-selector-signature-v1", material))
    return ".".join(
        (
            "ir1",
            "a=" + _compact(_digest("algorithm", INDEX_REVIEW_ALGORITHM_VERSION)),
            "d=" + str(minimum_days),
            "p=" + _compact(policy),
            "db=" + _compact(database_fingerprint_value),
            "as=" + _compact(selected_run_id),
            "pr=" + (_compact(prior_base_run_id) if prior_base_run_id else "-"),
            "h=" + _compact(history_fingerprint),
            "s=" + signature,
        )
    )


def parse_review_id(review_id: str) -> dict[str, Any]:
    if not isinstance(review_id, str) or not _REVIEW_ID.fullmatch(review_id):
        raise IndexReviewNotFoundError("Review id is not a valid deterministic selector.")
    values: dict[str, str] = {}
    for part in review_id.split(".")[1:]:
        if "=" not in part:
            raise IndexReviewNotFoundError("Review id is not parseable.")
        key, value = part.split("=", 1)
        if key in values:
            raise IndexReviewNotFoundError("Review id contains a duplicate selector.")
        values[key] = value
    try:
        minimum_days = int(values["d"])
    except (KeyError, ValueError) as exc:
        raise IndexReviewNotFoundError("Review id has an invalid minimum-day selector.") from exc
    if minimum_days < MIN_OBSERVATION_DAYS:
        raise IndexReviewNotFoundError("Review id is below the fixed observation floor.")
    required = {"a", "p", "db", "as", "pr", "h", "s"}
    if set(values) != required | {"d"}:
        raise IndexReviewNotFoundError("Review id has an invalid selector shape.")
    return {"minimum_days": minimum_days, "database": values["db"], "as_of": values["as"], "prior": None if values["pr"] == "-" else values["pr"], "history": values["h"], "signature": values["s"]}


def review_index_portfolio(
    database_name: str,
    snapshots: Sequence[IndexReviewSnapshotV1],
    *,
    as_of_run_id: str | None = None,
    prior_review: IndexReviewV1 | None = None,
    prior_review_id: str | None = None,
    business_cycle_extension_days: int = 0,
) -> IndexReviewV1:
    if business_cycle_extension_days < 0:
        raise ValueError("business_cycle_extension_days must be non-negative.")
    ordered = sorted(snapshots, key=lambda item: (item.observed_at_utc, item.run_id))
    if not ordered:
        raise IndexReviewNotFoundError("At least one index review snapshot is required.")
    selected = next((item for item in ordered if item.run_id == as_of_run_id), None) if as_of_run_id else ordered[-1]
    if selected is None:
        raise IndexReviewNotFoundError(f"Unknown index review run: {as_of_run_id}.")
    minimum_days = MIN_OBSERVATION_DAYS + business_cycle_extension_days
    prior_id = prior_review_id or (prior_review.review_id if prior_review else None)
    prior_run_id = prior_review.as_of_run_id if prior_review else None
    if prior_review is not None:
        if prior_review.prior_review_id is not None:
            raise IndexReviewPolicyError("prior_review_id must reference a base review.")
        if prior_review.database_name != database_name or prior_review.minimum_observation_days != minimum_days:
            raise IndexReviewPolicyError("Prior review is outside the current policy scope.")
        if _parse_time(prior_review.observation["as_of_observed_at_utc"]) >= _parse_time(selected.observed_at_utc):
            raise IndexReviewPolicyError("Prior review must be strictly earlier than the selected run.")
    history = [item for item in ordered if item.observed_at_utc <= selected.observed_at_utc]
    if prior_review is not None:
        history = [item for item in history if item.observed_at_utc > prior_review.observation["as_of_observed_at_utc"]]
    history_fingerprint = _digest(
        "index-review-history-v1",
        [{"run_id": item.run_id, "snapshot_fingerprint": item.snapshot_fingerprint} for item in history],
    )
    prior_subjects = {item.get("subject_id"): item for item in (prior_review.subjects if prior_review else ())}
    current_subjects = list(selected.subjects)
    duplicate_groups: dict[str, list[Mapping[str, Any]]] = {}
    for current in current_subjects:
        if current.get("subject_kind") != "existing_index":
            continue
        reversible = _reversible_definition(current)
        if not reversible:
            continue
        group_key = _canonical(
            {
                "schema_name": current.get("schema_name"),
                "table_name": current.get("table_name") or current.get("object_name"),
                "definition": _definition_semantics(reversible),
            }
        )
        duplicate_groups.setdefault(group_key, []).append(current)
    result_subjects: list[dict[str, Any]] = []
    for subject in sorted(current_subjects, key=lambda item: str(item.get("subject_id", ""))):
        if subject.get("subject_kind") == "missing_index":
            state, reasons, gates = _classify_create(subject, current_subjects, selected)
            entry = {**dict(subject), "state": state, "reason_codes": reasons, "create_gate": gates}
        else:
            removal = evaluate_removal_gate(subject, history, minimum_days=minimum_days)
            protections = _mapping_value(subject.get("protections"))
            has_valid_read_delta = removal["gates"].get("zero_seek_scan_lookup_deltas") is False and removal["gates"].get("counters_never_decrease") is True
            has_reference = removal["historical_reference"] is True
            protected = any(
                _as_bool(protections.get(key))
                for key in (
                    "primary_key",
                    "unique_constraint",
                    "indexed_view",
                    "clustered",
                    "auto_created",
                    "hinted_or_forced_plan",
                    "partition_switch_dependency",
                    "referenced_foreign_key_key_index_ids",
                    "child_foreign_key_support",
                )
            )
            if protected or has_valid_read_delta or has_reference:
                state, reasons = "keep", ["protected_or_used"]
            elif removal["passed"]:
                state, reasons = "drop_candidate", []
            else:
                state, reasons = "observe", removal["blockers"]
            entry = {**dict(subject), "state": state, "reason_codes": reasons, "removal_gate": removal}
            reversible = _reversible_definition(subject)
            group_key = _canonical(
                {
                    "schema_name": subject.get("schema_name"),
                    "table_name": subject.get("table_name") or subject.get("object_name"),
                    "definition": _definition_semantics(reversible),
                }
            ) if reversible else None
            duplicate_group = duplicate_groups.get(group_key, []) if group_key else []
            if len(duplicate_group) > 1:
                canonical_subject = min(
                    duplicate_group,
                    key=lambda item: (
                        str(item.get("index_name", "")),
                        str(item.get("subject_id", "")),
                    ),
                )
                if subject.get("subject_id") == canonical_subject.get("subject_id"):
                    entry["state"], entry["reason_codes"] = "keep", [
                        "canonical_duplicate_definition"
                    ]
                elif removal["passed"]:
                    entry["state"], entry["reason_codes"] = (
                        "consolidate_candidate",
                        ["exact_duplicate_definition"],
                    )
                    entry["overlap_relation"] = "exact_duplicate"
                    entry["survivor_reference"] = _survivor_reference_for_subject(
                        canonical_subject
                    )
                else:
                    entry["state"], entry["reason_codes"] = "observe", [
                        "independent_removal_gate_not_passed"
                    ]
            else:
                covering_index = next(
                    (
                        other
                        for other in current_subjects
                        if other.get("subject_id") != subject.get("subject_id")
                        and other.get("subject_kind") == "existing_index"
                        and other.get("schema_name") == subject.get("schema_name")
                        and other.get("table_name") == subject.get("table_name")
                        and _definition_covers(
                            _reversible_definition(other),
                            _reversible_definition(subject),
                        )
                    ),
                    None,
                )
                if covering_index is not None and removal["passed"]:
                    entry["state"], entry["reason_codes"] = (
                        "consolidate_candidate",
                        ["strict_coverage_overlap"],
                    )
                    entry["overlap_relation"] = "strict_coverage"
                    entry["survivor_reference"] = _survivor_reference_for_subject(
                        covering_index
                    )
                elif covering_index is not None:
                    entry["state"], entry["reason_codes"] = "observe", [
                        "independent_removal_gate_not_passed"
                    ]
                else:
                    covers_other = any(
                        other.get("subject_id") != subject.get("subject_id")
                        and other.get("subject_kind") == "existing_index"
                        and other.get("schema_name") == subject.get("schema_name")
                        and other.get("table_name") == subject.get("table_name")
                        and _definition_covers(
                            _reversible_definition(subject),
                            _reversible_definition(other),
                        )
                        for other in current_subjects
                    )
                    if covers_other:
                        entry["state"], entry["reason_codes"] = "keep", [
                            "covers_other_index"
                        ]
        previous = prior_subjects.get(subject.get("subject_id"))
        entry["recheck"] = {
            "is_recheck": previous is not None,
            "prior_state": previous.get("state") if previous else None,
            "transition": (
                "first_observation"
                if previous is None
                else "unchanged" if previous.get("state") == entry["state"] else f"{previous.get('state')}_to_{entry['state']}"
            ),
        }
        result_subjects.append(entry)
    states = [str(item.get("state")) for item in result_subjects]
    actionable = sum(state in {"create_candidate", "consolidate_candidate", "drop_candidate"} for state in states)
    observed = states.count("observe")
    current_incomplete = (
        _coverage_incomplete(selected.coverage)
        or _coverage_incomplete(selected.query_store)
        or any(_coverage_incomplete(item.get("coverage")) for item in result_subjects)
    )
    if not result_subjects:
        overall = "no_change" if not current_incomplete else "inconclusive"
    elif actionable and (observed or current_incomplete):
        overall = "partial"
    elif all(state == "keep" for state in states):
        overall = "no_change"
    elif actionable:
        overall = "actionable"
    else:
        overall = "inconclusive"
    database_fp = selected.database_fingerprint
    review_id = _review_id(database_fp, selected.run_id, prior_run_id, minimum_days, history_fingerprint)
    observation = {
        "snapshot_count": len(history),
        "as_of_observed_at_utc": selected.observed_at_utc,
        "minimum_observation_days": minimum_days,
        "business_cycle_extension_days": business_cycle_extension_days,
        "history_fingerprint": history_fingerprint,
        "state_counts": {state: states.count(state) for state in sorted(INDEX_REVIEW_STATES)},
        "recommend_only": True,
    }
    return IndexReviewV1(
        review_id=review_id,
        database_name=database_name,
        database_fingerprint=database_fp,
        as_of_run_id=selected.run_id,
        prior_review_id=prior_id,
        prior_base_run_id=prior_run_id,
        overall_state=overall,
        subjects=tuple(result_subjects),
        observation=observation,
        minimum_observation_days=minimum_days,
        history_fingerprint=history_fingerprint,
    )


def classify_index_subject(
    subject: Mapping[str, Any],
    snapshots: Sequence[IndexReviewSnapshotV1],
    *,
    minimum_days: int = MIN_OBSERVATION_DAYS,
) -> dict[str, Any]:
    gate = evaluate_removal_gate(subject, snapshots, minimum_days=minimum_days)
    protections = _mapping_value(subject.get("protections"))
    references = subject.get("query_store_references")
    executed_reference = (
        any(
            _reference_has_execution(reference)
            for reference in references
            if isinstance(reference, Mapping)
        )
        if isinstance(references, (list, tuple))
        else False
    )
    if any(
        _as_bool(protections.get(key))
        for key in (
            "primary_key", "unique_constraint", "indexed_view", "clustered",
            "auto_created", "hinted_or_forced_plan",
            "partition_switch_dependency", "referenced_foreign_key_key_index_ids",
            "child_foreign_key_support",
        )
    ) or executed_reference:
        state = "keep"
    elif gate["passed"]:
        state = "drop_candidate"
    else:
        state = "observe"
    return {"state": state, "removal_gate": gate}


def _quote_identifier(value: Any) -> str:
    text = str(value or "")
    if not text:
        return "[invalid_identifier]"
    return "[" + text.replace("]", "]]") + "]"


def _comment_sql(value: str) -> str:
    return "\n".join("-- " + line if line else "--" for line in value.splitlines())


def _create_candidate_ddl(subject: Mapping[str, Any]) -> str | None:
    keys = subject.get("key_columns")
    if not isinstance(keys, list) or not keys:
        return None
    key_sql = [f"{_quote_identifier(raw)} ASC" for raw in keys]
    includes = subject.get("include_columns") or []
    include_sql = " INCLUDE (" + ", ".join(_quote_identifier(column) for column in includes) + ")" if includes else ""
    name = _quote_identifier(_candidate_index_name(subject))
    return f"CREATE NONCLUSTERED INDEX {name} ON {_quote_identifier(subject.get('schema_name'))}.{_quote_identifier(subject.get('table_name'))} ({', '.join(key_sql)}){include_sql};"


def _candidate_index_name(subject: Mapping[str, Any]) -> str:
    fingerprint = str(
        subject.get("candidate_fingerprint")
        or subject.get("candidate_signature")
        or subject.get("subject_id")
        or "unknown"
    )
    return "IX_Review_" + fingerprint[:16]


def _candidate_drop_ddl(subject: Mapping[str, Any]) -> str | None:
    schema = subject.get("schema_name") or subject.get("schema")
    table = subject.get("table_name") or subject.get("object_name") or subject.get("table")
    if not all(isinstance(value, str) and value for value in (schema, table)):
        return None
    return (
        "DROP INDEX "
        + _quote_identifier(_candidate_index_name(subject))
        + " ON "
        + _quote_identifier(schema)
        + "."
        + _quote_identifier(table)
        + ";"
    )


def render_index_review_artifacts(review: IndexReviewV1 | Mapping[str, Any]) -> dict[str, str]:
    payload = review.as_dict() if isinstance(review, IndexReviewV1) else dict(review)
    subjects = list(payload.get("subjects", []))
    json_text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    md_lines = [
        "# Index portfolio review",
        "",
        f"Database: `{payload.get('database_name', '')}`",
        f"Review: `{payload.get('review_id', '')}`",
        f"Overall: `{payload.get('overall_state', '')}`",
        "recommend-only: `true`",
        "",
        "| Subject | Type | State | Reasons |",
        "| --- | --- | --- | --- |",
    ]
    create_lines = ["-- Recommend-only artifact. Every statement is commented and inert."]
    consolidate_lines = ["-- Recommend-only artifact. Every statement is commented and inert."]
    drop_lines = ["-- Recommend-only artifact. Every statement is commented and inert."]
    rollback_lines = ["-- Recommend-only artifact. Every statement is commented and inert."]
    validation_lines = ["-- Recommend-only artifact. Verify current metadata before any separately authorised change."]
    for subject in sorted(subjects, key=lambda item: str(item.get("subject_id", ""))):
        state = str(subject.get("state", "observe"))
        reasons = ", ".join(str(item) for item in subject.get("reason_codes", [])) or "-"
        md_lines.append(f"| `{subject.get('subject_id', '')}` | `{subject.get('subject_kind', '')}` | `{state}` | {reasons} |")
        if state == "create_candidate":
            ddl = _create_candidate_ddl(subject)
            if ddl:
                create_lines.append(f"-- create_candidate {subject.get('subject_fingerprint')}")
                create_lines.append(_comment_sql(ddl))
            inverse = _candidate_drop_ddl(subject)
            if inverse:
                rollback_lines.append(
                    f"-- exact inverse for create candidate {subject.get('subject_id')}"
                )
                rollback_lines.append(_comment_sql(inverse))
        elif state == "consolidate_candidate":
            consolidate_lines.append(
                f"-- consolidate_candidate {subject.get('subject_id')}; proposed removal requires independent review."
            )
            survivor = subject.get("survivor_reference")
            if isinstance(survivor, str) and survivor:
                consolidate_lines.append(f"-- Surviving index: {survivor}")
            proposed = render_inert_proposed_drop(subject)
            if proposed:
                consolidate_lines.append(proposed)
        elif state == "drop_candidate":
            drop_lines.append(
                f"-- drop_candidate {subject.get('subject_id')}; proposed removal requires independent review."
            )
            proposed = render_inert_proposed_drop(subject)
            if proposed:
                drop_lines.append(proposed)
        if state in {"drop_candidate", "consolidate_candidate"}:
            rollback = render_inert_candidate_rollback(subject)
            if rollback:
                rollback_lines.append(f"-- exact recreation for {subject.get('subject_id')}")
                rollback_lines.append(rollback)
        validation = render_validation_selects(subject)
        if validation:
            validation_lines.append(validation)
        else:
            validation_lines.append(
                f"-- {subject.get('subject_id')}: validate the recorded definition and coverage."
            )
    return {
        "index-review.json": json_text,
        "index-review.md": "\n".join(md_lines) + "\n",
        "create-candidates.sql": "\n".join(create_lines) + "\n",
        "consolidation-candidates.sql": "\n".join(consolidate_lines) + "\n",
        "drop-candidates.sql": "\n".join(drop_lines) + "\n",
        "rollback.sql": "\n".join(rollback_lines) + "\n",
        "validation.sql": "\n".join(validation_lines) + "\n",
    }


__all__ = [
    "CaptureContext",
    "CaptureResult",
    "CONTRACT_PROBE_SQL",
    "CONTRACT_SCHEMA_FINGERPRINT",
    "CAPTURE_READ_SQL",
    "ContractProbeResult",
    "HISTORY_READ_SQL",
    "IDEMPOTENCY_LOCK_SQL",
    "INDEX_HISTORY_CONTRACT_VERSION",
    "INDEX_HISTORY_SCHEMA_VERSION",
    "INDEX_REVIEW_ALGORITHM_VERSION",
    "INDEX_REVIEW_CLASSIFIER_POLICY_VERSION",
    "INDEX_REVIEW_COLLECTOR_VERSION",
    "INDEX_REVIEW_CONTRACT_VERSION",
    "INDEX_REVIEW_OVERALL_STATES",
    "INDEX_REVIEW_SKILL",
    "INDEX_REVIEW_SKILL_VERSION",
    "INDEX_REVIEW_STATES",
    "IndexReviewCollectionError",
    "IndexReviewError",
    "IndexReviewIdempotencyConflictError",
    "IndexReviewIntegrityError",
    "IndexReviewNotFoundError",
    "IndexReviewOutcomeUnknownError",
    "IndexReviewPolicyError",
    "IndexReviewRunV1",
    "IndexReviewSchemaError",
    "IndexReviewService",
    "IndexReviewSnapshotV1",
    "IndexReviewV1",
    "IndexReviewWriteError",
    "MIN_OBSERVATION_DAYS",
    "PUBLIC_CONTRACT_VERSION",
    "SNAPSHOT_REUSE_HOURS",
    "SqlIndexHistoryRepository",
    "classify_index_subject",
    "daily_idempotency_key",
    "database_fingerprint",
    "evaluate_drop_gate",
    "evaluate_removal_gate",
    "idempotency_key_hash",
    "parse_review_id",
    "render_index_review_artifacts",
    "review_index_portfolio",
    "validate_contract_probe",
]
