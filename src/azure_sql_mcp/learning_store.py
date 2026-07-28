"""Additive owner-only SQLite persistence for evidence-governed learning."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import threading
from contextlib import contextmanager
from dataclasses import fields
from pathlib import Path
from typing import Any, Iterator, Mapping, cast

from .learning_contracts import (
    CONTRACT_VERSION,
    DecisionRecordV1,
    HandoffV1,
    LessonV1,
    OutcomeReviewV1,
    VersionedLearningContract,
    deserialize_learning_contract,
    redact_metadata,
    structured_summary,
    validate_fingerprint,
    validate_evidence_ref,
    validate_terminal_link_ref,
    parse_timestamp,
    timestamp_is_before_or_equal,
    utc_now,
)


class LearningStoreError(RuntimeError):
    """Base class for durable learning errors."""


class ContractNotFoundError(LearningStoreError, KeyError):
    """A requested contract is not present."""


class IdempotencyConflictError(LearningStoreError):
    """An idempotency key was replayed with different request material."""


class ConcurrencyError(LearningStoreError):
    """An optimistic version precondition failed."""


class LifecycleError(LearningStoreError):
    """A lifecycle transition or mutation boundary was invalid."""


_TABLES: dict[str, tuple[str, type[VersionedLearningContract], str]] = {
    "decision": ("learning_decisions", DecisionRecordV1, "decision_id"),
    "review": ("learning_reviews", OutcomeReviewV1, "review_id"),
    "lesson": ("learning_lessons", LessonV1, "lesson_id"),
    "handoff": ("learning_handoffs", HandoffV1, "handoff_id"),
}
_IMMUTABLE_FIELDS: dict[str, tuple[str, ...]] = {
    "decision": tuple(
        item.name
        for item in fields(DecisionRecordV1)
        if item.name not in {"version", "lifecycle", "updated_at_utc"}
    ),
    "review": tuple(item.name for item in fields(OutcomeReviewV1)),
    "lesson": (
        "learning_key",
        "subject_kind",
        "subject_fingerprint",
        "trigger",
        "action",
        "preconditions",
        "counterexamples",
        "next_observation",
        "required_evidence",
        "applicable_skills",
        "applicable_scopes",
        "tags",
        "freshness_days",
        "source_provenance",
        "supersedes_lesson_id",
        "created_at_utc",
        "proposal_kind",
    ),
    "handoff": (
        "handoff_type",
        "source_skill",
        "target_skill",
        "case_id",
        "session_id",
        "scope",
        "objective",
        "evidence_refs",
        "constraints",
        "gaps",
        "acceptance_criteria",
    ),
}
_APPEND_ONLY_FIELDS: dict[str, tuple[str, ...]] = {
    "lesson": (
        "query_fingerprints",
        "support_refs",
        "based_on_review_ids",
        "contradiction_refs",
        "support_session_ids",
        "support_query_fingerprints",
    ),
    "handoff": ("resolution_evidence_refs",),
}
_LIFECYCLE_FIELDS = {"decision": "lifecycle", "lesson": "status", "handoff": "status"}
_TRANSITIONS: dict[str, dict[str, frozenset[str]]] = {
    "decision": {
        "recorded": frozenset({"reviewed", "superseded", "closed"}),
        "reviewed": frozenset({"superseded", "closed"}),
        "superseded": frozenset({"closed"}),
        "closed": frozenset(),
    },
    "lesson": {
        "proposed": frozenset({"eligible", "active", "rejected", "quarantined"}),
        "eligible": frozenset({"proposed", "active", "rejected", "quarantined"}),
        "active": frozenset({"quarantined", "superseded", "retired"}),
        "quarantined": frozenset({"superseded", "retired"}),
        "superseded": frozenset({"retired"}),
        "retired": frozenset(),
        "rejected": frozenset(),
    },
    "handoff": {
        "open": frozenset({"claimed", "cancelled"}),
        "claimed": frozenset({"resolved", "open", "cancelled"}),
        "resolved": frozenset({"open", "cancelled"}),
        "cancelled": frozenset({"open"}),
    },
}
_CLI_ONLY_TARGETS = frozenset({"active", "rejected", "retired", "superseded"})
_LESSON_LIFECYCLE_AUDIT_FIELDS = (
    "reviewer",
    "reviewed_at_utc",
    "rejection_code",
    "rejected_by",
    "rejected_at_utc",
    "superseded_by_lesson_id",
)


def _private_mode(path: Path, mode: int) -> None:
    try:
        os.chmod(path, mode)
        actual = stat.S_IMODE(path.stat().st_mode)
    except OSError as exc:
        raise LearningStoreError(f"Could not secure learning state path {path}.") from exc
    if os.name != "nt" and actual != mode:
        raise LearningStoreError(f"Learning state path {path} must have mode {oct(mode)}.")


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _decision_scope_key(decision: DecisionRecordV1) -> str:
    scope = dict(decision.scope)
    scope.pop("runtime_fingerprint", None)
    scope.pop("compatibility_fingerprint", None)
    scope.setdefault("skill_version", decision.skill_version)
    scope.setdefault(
        "runtime_compatibility_fingerprint",
        decision.runtime_compatibility_fingerprint,
    )
    if decision.tool_schema_fingerprint is not None:
        scope.setdefault("tool_schema_fingerprint", decision.tool_schema_fingerprint)
    if decision.sanitized_config_fingerprint is not None:
        scope.setdefault(
            "sanitized_config_fingerprint",
            decision.sanitized_config_fingerprint,
        )
    return _canonical(
        {
            key: value
            for key, value in scope.items()
            if key
            in {
                "skill_version",
                "server_fingerprint",
                "database_fingerprint",
                "runtime_compatibility_fingerprint",
                "tool_schema_fingerprint",
                "sanitized_config_fingerprint",
            }
        }
    )


def _request_fingerprint(contract: VersionedLearningContract) -> str:
    payload = contract.to_dict()
    for key in (
        "decision_id",
        "review_id",
        "lesson_id",
        "handoff_id",
        "created_at_utc",
        "updated_at_utc",
        "completed_at_utc",
        "claimed_at_utc",
        "resolved_at_utc",
        "cancelled_at_utc",
        "status_changed_at_utc",
        "last_supported_at_utc",
        "version",
    ):
        payload.pop(key, None)
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def _fingerprint(contract: VersionedLearningContract, supplied: str | None) -> str:
    if supplied is None:
        return _request_fingerprint(contract)
    return hashlib.sha256(supplied.encode("utf-8")).hexdigest()


def _key_digest(scope: str, key: str) -> str:
    if not isinstance(key, str) or not key.strip() or len(key) > 512:
        raise IdempotencyConflictError("idempotency_key must be non-empty and at most 512 characters.")
    return hashlib.sha256(f"learning-idempotency-v1:{scope}:{key}".encode()).hexdigest()


class LearningStore:
    """Additive learning tables in the protected performance SQLite store."""

    schema_version = CONTRACT_VERSION

    def __init__(self, state_dir: str | Path | None = None, *, db_path: str | Path | None = None) -> None:
        if str(db_path) == ":memory:":
            self.state_dir = None
            self.db_path = None
            self._connection = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
        else:
            if db_path is not None:
                database_path = Path(db_path).expanduser()
                directory = Path(state_dir).expanduser() if state_dir else database_path.parent
            else:
                directory = Path(state_dir).expanduser() if state_dir else Path.home() / ".azure-sql-mcp" / "state"
                database_path = directory / "performance.sqlite3"
            directory.mkdir(parents=True, exist_ok=True)
            if directory != Path("."):
                _private_mode(directory, 0o700)
            database_path.touch(mode=0o600, exist_ok=True)
            _private_mode(database_path, 0o600)
            self.state_dir = directory
            self.db_path = database_path
            self._connection = sqlite3.connect(str(database_path), isolation_level=None, check_same_thread=False)

        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA journal_mode = DELETE")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._migrate_additive()
        if self.db_path is not None:
            _private_mode(self.db_path, 0o600)

    def __enter__(self) -> LearningStore:
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()

    def _migrate_additive(self) -> None:
        with self._transaction():
            self._connection.execute(
                "CREATE TABLE IF NOT EXISTS learning_schema (schema_name TEXT PRIMARY KEY, schema_version INTEGER NOT NULL)"
            )
            row = self._connection.execute(
                "SELECT schema_version FROM learning_schema WHERE schema_name = 'learning'"
            ).fetchone()
            if row is not None and int(row["schema_version"]) > self.schema_version:
                raise LearningStoreError("Learning database schema is newer than this application.")
            for table_name, _contract_cls, id_field in _TABLES.values():
                self._connection.execute(
                    f"""CREATE TABLE IF NOT EXISTS {table_name} (
                        {id_field} TEXT PRIMARY KEY,
                        contract_version INTEGER NOT NULL,
                        payload TEXT NOT NULL,
                        created_at_utc TEXT NOT NULL,
                        updated_at_utc TEXT NOT NULL
                    )"""
                )
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS learning_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    aggregate_type TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    request_fingerprint TEXT,
                    created_at_utc TEXT NOT NULL
                )"""
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_learning_events_aggregate ON learning_events (aggregate_type, aggregate_id, sequence)"
            )
            self._connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_learning_reviews_decision ON learning_reviews (json_extract(payload, '$.decision_id'))"
            )
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS learning_idempotency (
                    scope TEXT NOT NULL,
                    key_digest TEXT NOT NULL,
                    aggregate_type TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    request_fingerprint TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    PRIMARY KEY (scope, key_digest)
                )"""
            )
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS learning_support_links (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    link_id TEXT NOT NULL UNIQUE,
                    lesson_id TEXT NOT NULL,
                    relationship TEXT NOT NULL
                        CHECK (relationship IN ('support', 'contradiction')),
                    reference_type TEXT NOT NULL,
                    reference_id TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    UNIQUE (lesson_id, relationship, reference_type, reference_id)
                )"""
            )
            self._connection.execute(
                """CREATE INDEX IF NOT EXISTS idx_learning_support_links_lesson
                    ON learning_support_links (lesson_id, sequence)"""
            )
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS learning_terminal_links (
                    link_id TEXT PRIMARY KEY,
                    decision_id TEXT NOT NULL,
                    source_tool TEXT NOT NULL,
                    database_fingerprint TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    outcome_summary TEXT NOT NULL,
                    evidence_refs TEXT NOT NULL,
                    response_fingerprint TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    UNIQUE (decision_id, source_tool, response_fingerprint)
                )"""
            )
            self._connection.execute(
                """CREATE INDEX IF NOT EXISTS idx_learning_terminal_links_decision
                    ON learning_terminal_links (decision_id, created_at_utc, link_id)"""
            )
            if row is None:
                self._connection.execute(
                    "INSERT INTO learning_schema (schema_name, schema_version) VALUES ('learning', ?)",
                    (self.schema_version,),
                )
            elif int(row["schema_version"]) < self.schema_version:
                self._connection.execute(
                    "UPDATE learning_schema SET schema_version = ? WHERE schema_name = 'learning'",
                    (self.schema_version,),
                )

    @staticmethod
    def _event_payload(payload: Mapping[str, Any] | None) -> str:
        return _canonical(redact_metadata(payload))

    def _get_locked(self, aggregate_type: str, identifier: str) -> VersionedLearningContract:
        table_name, contract_cls, id_field = _TABLES[aggregate_type]
        row = self._connection.execute(
            f"SELECT payload FROM {table_name} WHERE {id_field} = ?", (identifier,)
        ).fetchone()
        if row is None:
            raise ContractNotFoundError(f"Unknown {contract_cls.contract_type}: {identifier}.")
        try:
            contract = deserialize_learning_contract(row["payload"])
        except (TypeError, ValueError) as exc:
            raise LearningStoreError(f"Stored {contract_cls.contract_type} is invalid.") from exc
        if not isinstance(contract, contract_cls):
            raise LearningStoreError(f"Stored contract has wrong type for {aggregate_type}.")
        return contract

    def _get(self, aggregate_type: str, identifier: str) -> VersionedLearningContract:
        with self._lock:
            return self._get_locked(aggregate_type, identifier)

    def _idempotent_replay_locked(
        self,
        scope: str,
        key: str | None,
        aggregate_type: str,
        fingerprint: str,
        *,
        requested_id: str | None = None,
    ) -> str | None:
        if key is None:
            return None
        digest = _key_digest(scope, key)
        row = self._connection.execute(
            "SELECT * FROM learning_idempotency WHERE scope = ? AND key_digest = ?",
            (scope, digest),
        ).fetchone()
        if row is None:
            return None
        if row["aggregate_type"] != aggregate_type or (requested_id is not None and row["aggregate_id"] != requested_id):
            raise IdempotencyConflictError("Idempotency key is bound to another learning aggregate.")
        if row["request_fingerprint"] != fingerprint:
            raise IdempotencyConflictError("Idempotency key was replayed with different request material.")
        return str(row["aggregate_id"])

    def _record_idempotency_locked(self, scope: str, key: str | None, aggregate_type: str, aggregate_id: str, fingerprint: str) -> None:
        if key is None:
            return
        try:
            self._connection.execute(
                """INSERT INTO learning_idempotency
                    (scope, key_digest, aggregate_type, aggregate_id, request_fingerprint, created_at_utc)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                (scope, _key_digest(scope, key), aggregate_type, aggregate_id, fingerprint, utc_now()),
            )
        except sqlite3.IntegrityError as exc:
            raise IdempotencyConflictError("Idempotency key was concurrently reused.") from exc

    def _append_event_locked(self, aggregate_type: str, aggregate_id: str, event_type: str, payload: Mapping[str, Any] | None, request_fingerprint: str | None) -> dict[str, Any]:
        event_id = f"learning-event-{os.urandom(12).hex()}"
        self._connection.execute(
            """INSERT INTO learning_events
                (event_id, aggregate_type, aggregate_id, event_type, payload, request_fingerprint, created_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (event_id, aggregate_type, aggregate_id, event_type, self._event_payload(payload), request_fingerprint, utc_now()),
        )
        row = self._connection.execute("SELECT * FROM learning_events WHERE event_id = ?", (event_id,)).fetchone()
        return self._event_dict(row)

    def _append_lesson_links_locked(self, lesson: LessonV1) -> None:
        for relationship, reference_ids in (
            ("support", lesson.support_refs),
            ("contradiction", lesson.contradiction_refs),
        ):
            for reference_id in reference_ids:
                link_id = "lesson-link-" + hashlib.sha256(
                    f"{lesson.lesson_id}:{relationship}:review:{reference_id}".encode()
                ).hexdigest()[:32]
                self._connection.execute(
                    """INSERT OR IGNORE INTO learning_support_links
                        (link_id, lesson_id, relationship, reference_type, reference_id, created_at_utc)
                        VALUES (?, ?, ?, 'review', ?, ?)""",
                    (
                        link_id,
                        lesson.lesson_id,
                        relationship,
                        reference_id,
                        utc_now(),
                    ),
                )

    def _validate_decision_lineage_locked(self, decision: DecisionRecordV1) -> None:
        current_created = parse_timestamp(
            decision.created_at_utc,
            "decision.created_at_utc",
        )
        for review_id in decision.based_on_review_ids:
            try:
                review = cast(OutcomeReviewV1, self._get_locked("review", review_id))
                prior = cast(
                    DecisionRecordV1,
                    self._get_locked("decision", review.decision_id),
                )
            except ContractNotFoundError as exc:
                raise LifecycleError(
                    f"Decision lineage references unknown review {review_id}."
                ) from exc
            if prior.decision_id == decision.decision_id:
                raise LifecycleError("A decision cannot be based on a review of itself.")
            if (
                prior.skill != decision.skill
                or prior.skill_version != decision.skill_version
                or prior.learning_key != decision.learning_key
                or _decision_scope_key(prior) != _decision_scope_key(decision)
            ):
                raise LifecycleError(
                    "Decision lineage review is outside the compatible scope."
                )
            if prior.lifecycle != "reviewed" or not review.complete:
                raise LifecycleError(
                    "Decision lineage must reference a reviewed prior decision."
                )
            if parse_timestamp(prior.created_at_utc, "prior decision.created_at_utc") >= current_created:
                raise LifecycleError("Decision lineage must precede the new decision.")
            if parse_timestamp(
                review.completed_at_utc or review.created_at_utc,
                "review timestamp",
            ) >= current_created:
                raise LifecycleError("Decision lineage must precede the new decision.")
        decision_rows = self._connection.execute(
            "SELECT payload FROM learning_decisions"
        ).fetchall()
        prior_decisions = [
            cast(
                DecisionRecordV1,
                deserialize_learning_contract(row["payload"]),
            )
            for row in decision_rows
        ]
        related = [
            candidate
            for candidate in prior_decisions
            if candidate.decision_id != decision.decision_id
            and candidate.skill == decision.skill
            and candidate.skill_version == decision.skill_version
            and candidate.learning_key == decision.learning_key
            and _decision_scope_key(candidate) == _decision_scope_key(decision)
            and parse_timestamp(
                candidate.created_at_utc,
                "decision.created_at_utc",
            )
            <= current_created
        ]
        if related:
            latest_decision = max(
                related,
                key=lambda item: parse_timestamp(
                    item.created_at_utc,
                    "decision.created_at_utc",
                ),
            )
            if latest_decision.lifecycle != "reviewed":
                raise LifecycleError(
                    "The prior decision must be reviewed before the next decision."
                )
            review_row = self._connection.execute(
                """SELECT payload FROM learning_reviews
                    WHERE json_extract(payload, '$.decision_id') = ?""",
                (latest_decision.decision_id,),
            ).fetchone()
            if review_row is None:
                raise LifecycleError(
                    "The prior decision is missing its outcome review."
                )
            latest_review = cast(
                OutcomeReviewV1,
                deserialize_learning_contract(review_row["payload"]),
            )
            if latest_review.review_id not in decision.based_on_review_ids:
                raise LifecycleError(
                    "The next decision must reference the prior outcome review."
                )

    @staticmethod
    def _event_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "sequence": int(row["sequence"]),
            "event_id": row["event_id"],
            "aggregate_type": row["aggregate_type"],
            "aggregate_id": row["aggregate_id"],
            "event_type": row["event_type"],
            "payload": json.loads(row["payload"]),
            "request_fingerprint": row["request_fingerprint"],
            "created_at_utc": row["created_at_utc"],
        }

    @staticmethod
    def _lifecycle_field(aggregate_type: str) -> str | None:
        return _LIFECYCLE_FIELDS.get(aggregate_type)

    def _check_update(self, aggregate_type: str, current: VersionedLearningContract, incoming: VersionedLearningContract, actor: str) -> None:
        for field_name in _IMMUTABLE_FIELDS.get(aggregate_type, ()):
            if getattr(current, field_name) != getattr(incoming, field_name):
                raise LifecycleError(f"{field_name} is immutable for {aggregate_type}.")
        for field_name in _APPEND_ONLY_FIELDS.get(aggregate_type, ()):
            current_values = tuple(getattr(current, field_name))
            incoming_values = tuple(getattr(incoming, field_name))
            if incoming_values[: len(current_values)] != current_values:
                raise LifecycleError(f"{field_name} is append-only for {aggregate_type}.")
        if aggregate_type == "review":
            raise LifecycleError("Outcome reviews are append-only.")
        field_name = self._lifecycle_field(aggregate_type)
        if field_name is None:
            return
        current_state = getattr(current, field_name)
        incoming_state = getattr(incoming, field_name)
        if current_state == incoming_state:
            if aggregate_type == "lesson" and any(
                getattr(current, audit_field) != getattr(incoming, audit_field)
                for audit_field in _LESSON_LIFECYCLE_AUDIT_FIELDS
            ):
                raise LifecycleError(
                    "Lesson lifecycle audit fields may change only during a lifecycle transition."
                )
            return
        if incoming_state not in _TRANSITIONS[aggregate_type].get(current_state, frozenset()):
            raise LifecycleError(f"Invalid {aggregate_type} transition {current_state!r} -> {incoming_state!r}.")
        if aggregate_type == "lesson" and incoming_state in _CLI_ONLY_TARGETS and actor != "cli":
            raise LifecycleError(
                "Only the local maintainer CLI may activate, reject, retire, or supersede lessons."
            )

    def _save_locked(self, aggregate_type: str, contract: VersionedLearningContract, *, expected_version: int | None, actor: str) -> VersionedLearningContract:
        table_name, contract_cls, id_field = _TABLES[aggregate_type]
        if not isinstance(contract, contract_cls):
            raise TypeError(f"Expected {contract_cls.__name__}.")
        identifier = str(getattr(contract, id_field))
        row = self._connection.execute(f"SELECT payload FROM {table_name} WHERE {id_field} = ?", (identifier,)).fetchone()
        if row is None:
            if expected_version not in (None, -1):
                raise ConcurrencyError(f"Cannot update missing {aggregate_type} {identifier}.")
            lifecycle_field = self._lifecycle_field(aggregate_type)
            if (
                aggregate_type == "lesson"
                and getattr(contract, lifecycle_field or "status") not in {"proposed", "eligible"}
            ):
                raise LifecycleError(
                    "Lessons must enter as proposed or eligible; activation requires a local transition."
                )
            if aggregate_type == "decision" and getattr(contract, "lifecycle") != "recorded":
                raise LifecycleError("Decisions must enter the store as recorded.")
            if aggregate_type == "handoff" and getattr(contract, "status") != "open":
                raise LifecycleError("Handoffs must enter the store as open.")
            created_at = getattr(contract, "created_at_utc", utc_now())
            updated_at = getattr(contract, "updated_at_utc", created_at)
            self._connection.execute(
                f"INSERT INTO {table_name} ({id_field}, contract_version, payload, created_at_utc, updated_at_utc) VALUES (?, ?, ?, ?, ?)",
                (identifier, getattr(contract, "contract_version"), contract.to_json(), created_at, updated_at),
            )
            return contract
        current = self._get_locked(aggregate_type, identifier)
        current_version = int(getattr(current, "version", 0))
        if expected_version is None or current_version != expected_version:
            raise ConcurrencyError(f"{aggregate_type} {identifier} is version {current_version}; expected {expected_version}.")
        if int(getattr(contract, "version")) != current_version + 1:
            raise ConcurrencyError(f"{aggregate_type} {identifier} must advance to version {current_version + 1}.")
        current_updated = getattr(current, "updated_at_utc", None)
        incoming_updated = getattr(contract, "updated_at_utc", None)
        if current_updated is not None and incoming_updated is not None and not timestamp_is_before_or_equal(current_updated, incoming_updated):
            raise LifecycleError("Updated chronology cannot move backwards.")
        self._check_update(aggregate_type, current, contract, actor)
        updated_at = getattr(contract, "updated_at_utc", utc_now())
        self._connection.execute(
            f"UPDATE {table_name} SET contract_version = ?, payload = ?, updated_at_utc = ? WHERE {id_field} = ?",
            (getattr(contract, "contract_version"), contract.to_json(), updated_at, identifier),
        )
        return contract

    def _create(self, aggregate_type: str, contract: VersionedLearningContract, *, idempotency_key: str | None, request_fingerprint: str | None, event_type: str) -> VersionedLearningContract:
        table_name, contract_cls, id_field = _TABLES[aggregate_type]
        if not isinstance(contract, contract_cls):
            raise TypeError(f"Expected {contract_cls.__name__}.")
        fingerprint = _fingerprint(contract, request_fingerprint)
        scope = f"{aggregate_type}.create"
        identifier = str(getattr(contract, id_field))
        with self._transaction():
            replay = self._idempotent_replay_locked(scope, idempotency_key, aggregate_type, fingerprint)
            if replay is not None:
                return self._get_locked(aggregate_type, replay)
            existing = self._connection.execute(
                f"SELECT 1 FROM {table_name} WHERE {id_field} = ?", (identifier,)
            ).fetchone()
            if existing is not None:
                if aggregate_type == "review":
                    raise LifecycleError("Outcome reviews are append-only and cannot be duplicated.")
                raise LearningStoreError(f"{aggregate_type} {identifier} already exists.")
            if aggregate_type == "decision":
                self._validate_decision_lineage_locked(cast(DecisionRecordV1, contract))
            if aggregate_type == "review":
                decision_id = str(getattr(contract, "decision_id"))
                if self._connection.execute(
                    "SELECT 1 FROM learning_reviews WHERE json_extract(payload, '$.decision_id') = ?",
                    (decision_id,),
                ).fetchone() is not None:
                    raise LifecycleError("Each decision may have only one outcome review.")
            try:
                self._save_locked(aggregate_type, contract, expected_version=None, actor="service")
            except sqlite3.IntegrityError as exc:
                if aggregate_type == "review":
                    raise LifecycleError("Each decision may have only one outcome review.") from exc
                raise
            if aggregate_type == "lesson":
                self._append_lesson_links_locked(cast(LessonV1, contract))
            self._append_event_locked(aggregate_type, identifier, event_type, {"contract_type": contract.contract_type, "version": getattr(contract, "version")}, fingerprint)
            self._record_idempotency_locked(scope, idempotency_key, aggregate_type, identifier, fingerprint)
        return contract

    def _save(self, aggregate_type: str, contract: VersionedLearningContract, *, expected_version: int | None, idempotency_key: str | None, request_fingerprint: str | None, event_type: str, actor: str) -> VersionedLearningContract:
        _table, contract_cls, id_field = _TABLES[aggregate_type]
        if not isinstance(contract, contract_cls):
            raise TypeError(f"Expected {contract_cls.__name__}.")
        fingerprint = _fingerprint(contract, request_fingerprint)
        scope = f"{aggregate_type}.update"
        identifier = str(getattr(contract, id_field))
        with self._transaction():
            replay = self._idempotent_replay_locked(scope, idempotency_key, aggregate_type, fingerprint, requested_id=identifier)
            if replay is not None:
                return self._get_locked(aggregate_type, replay)
            saved = self._save_locked(aggregate_type, contract, expected_version=expected_version, actor=actor)
            if aggregate_type == "lesson":
                self._append_lesson_links_locked(cast(LessonV1, contract))
            self._append_event_locked(aggregate_type, identifier, event_type, {"contract_type": contract.contract_type, "version": getattr(contract, "version"), "lifecycle": getattr(contract, "lifecycle", None), "status": getattr(contract, "status", None), "rejection_code": getattr(contract, "rejection_code", None), "rejected_by": getattr(contract, "rejected_by", None)}, fingerprint)
            self._record_idempotency_locked(scope, idempotency_key, aggregate_type, identifier, fingerprint)
        return saved

    def create_decision(self, decision: DecisionRecordV1, *, idempotency_key: str | None = None, request_fingerprint: str | None = None) -> DecisionRecordV1:
        for index, evidence_ref in enumerate(decision.consumed_evidence_refs):
            validate_evidence_ref(evidence_ref, f"consumed_evidence_refs[{index}]")
        return cast(DecisionRecordV1, self._create("decision", decision, idempotency_key=idempotency_key, request_fingerprint=request_fingerprint, event_type="decision.created"))

    def get_decision(self, decision_id: str) -> DecisionRecordV1:
        return cast(DecisionRecordV1, self._get("decision", decision_id))

    def save_decision(self, decision: DecisionRecordV1, *, expected_version: int, idempotency_key: str | None = None, request_fingerprint: str | None = None) -> DecisionRecordV1:
        return cast(DecisionRecordV1, self._save("decision", decision, expected_version=expected_version, idempotency_key=idempotency_key, request_fingerprint=request_fingerprint, event_type="decision.updated", actor="service"))

    def create_review(self, review: OutcomeReviewV1, *, idempotency_key: str | None = None, request_fingerprint: str | None = None) -> OutcomeReviewV1:
        decision = self.get_decision(review.decision_id)
        if not timestamp_is_before_or_equal(decision.created_at_utc, review.created_at_utc):
            raise LifecycleError("Review chronology precedes its decision.")
        if review.completed_at_utc is not None and not timestamp_is_before_or_equal(review.created_at_utc, review.completed_at_utc):
            raise LifecycleError("Review completion precedes review creation.")
        for evidence_ref in review.terminal_evidence_refs:
            validate_terminal_link_ref(evidence_ref, "terminal_evidence_ref")
            row = self._connection.execute(
                "SELECT decision_id FROM learning_terminal_links WHERE link_id = ?",
                (evidence_ref,),
            ).fetchone()
            if row is None:
                raise LifecycleError("Outcome reviews may cite terminal links only, and every link must exist.")
            if str(row["decision_id"]) != review.decision_id:
                raise LifecycleError("Terminal evidence link belongs to a different decision scope.")
        return cast(OutcomeReviewV1, self._create("review", review, idempotency_key=idempotency_key, request_fingerprint=request_fingerprint, event_type="review.created"))

    def get_review(self, review_id: str) -> OutcomeReviewV1:
        return cast(OutcomeReviewV1, self._get("review", review_id))

    def create_lesson(self, lesson: LessonV1, *, idempotency_key: str | None = None, request_fingerprint: str | None = None) -> LessonV1:
        return cast(LessonV1, self._create("lesson", lesson, idempotency_key=idempotency_key, request_fingerprint=request_fingerprint, event_type="lesson.proposed"))

    def import_active_lesson(
        self,
        lesson: LessonV1,
        *,
        idempotency_key: str | None = None,
    ) -> LessonV1:
        del lesson, idempotency_key
        raise LifecycleError(
            "Direct active lesson imports are disabled; import as proposed and approve locally."
        )

    def get_lesson(self, lesson_id: str) -> LessonV1:
        return cast(LessonV1, self._get("lesson", lesson_id))

    def save_lesson(self, lesson: LessonV1, *, expected_version: int, actor: str = "service", idempotency_key: str | None = None, request_fingerprint: str | None = None) -> LessonV1:
        return cast(LessonV1, self._save("lesson", lesson, expected_version=expected_version, idempotency_key=idempotency_key, request_fingerprint=request_fingerprint, event_type="lesson.updated", actor=actor))

    def transition_lesson(
        self,
        lesson_id: str,
        target_status: str,
        *,
        expected_version: int | None = None,
        actor: str = "service",
        idempotency_key: str | None = None,
        at_utc: str | None = None,
        audit_ref: str | None = None,
        rejection_code: str | None = None,
        reviewer: str | None = None,
        superseded_by_lesson_id: str | None = None,
    ) -> LessonV1:
        with self._transaction():
            current = cast(LessonV1, self._get_locked("lesson", lesson_id))
            fingerprint = hashlib.sha256(
                _canonical(
                    {
                        "lesson_id": lesson_id,
                        "target_status": target_status,
                        "audit_ref": audit_ref,
                        "rejection_code": rejection_code,
                        "reviewer": reviewer,
                        "superseded_by_lesson_id": superseded_by_lesson_id,
                    }
                ).encode()
            ).hexdigest()
            scope = "lesson.transition"
            replay = self._idempotent_replay_locked(
                scope,
                idempotency_key,
                "lesson",
                fingerprint,
                requested_id=lesson_id,
            )
            if replay is not None:
                return cast(LessonV1, self._get_locked("lesson", replay))
            if getattr(current, "status") == target_status:
                raise LifecycleError(
                    f"Lesson is already in target status {target_status!r}."
                )
            if expected_version is None:
                expected_version = int(getattr(current, "version"))
            if (
                target_status == "active"
                and current.status == "proposed"
                and current.proposal_kind == "normal"
            ):
                raise LifecycleError(
                    "Normal lessons must reach eligible status before activation."
                )
            payload = current.to_dict()
            payload["status"] = target_status
            payload["version"] = int(getattr(current, "version")) + 1
            payload["updated_at_utc"] = at_utc or utc_now()
            payload["status_changed_at_utc"] = payload["updated_at_utc"]
            if target_status == "active":
                if actor != "cli" or not reviewer:
                    raise LifecycleError(
                        "Lesson activation requires a named local maintainer reviewer."
                    )
                payload["reviewer"] = reviewer
                payload["reviewed_at_utc"] = payload["updated_at_utc"]
            if target_status == "rejected":
                if actor != "cli" or not reviewer:
                    raise LifecycleError(
                        "Lesson rejection requires a named local maintainer reviewer."
                    )
                payload["rejection_code"] = rejection_code or "maintainer-rejected"
                payload["rejected_by"] = reviewer
                payload["rejected_at_utc"] = payload["updated_at_utc"]
            if target_status in {"retired", "superseded"} and (
                actor != "cli" or not reviewer
            ):
                raise LifecycleError(
                    f"Lesson {target_status} requires a named local maintainer reviewer."
                )
            if target_status == "superseded":
                if superseded_by_lesson_id is None:
                    raise LifecycleError("Superseded lessons require superseded_by_lesson_id.")
                replacement = cast(
                    LessonV1,
                    self._get_locked("lesson", superseded_by_lesson_id),
                )
                if replacement.status != "active":
                    raise LifecycleError("A superseding lesson must already be active.")
                if replacement.supersedes_lesson_id != lesson_id:
                    raise LifecycleError(
                        "The replacement lesson does not declare this supersession."
                    )
                payload["superseded_by_lesson_id"] = superseded_by_lesson_id
            updated = LessonV1.from_dict(payload)
            saved = self._save_locked("lesson", updated, expected_version=expected_version, actor=actor)
            self._append_event_locked(
                "lesson",
                lesson_id,
                f"lesson.{target_status}",
                {
                    "status": target_status,
                    "audit_ref": audit_ref,
                    "reviewer": reviewer,
                    "superseded_by_lesson_id": superseded_by_lesson_id,
                },
                fingerprint,
            )
            self._record_idempotency_locked(scope, idempotency_key, "lesson", lesson_id, fingerprint)
            return cast(LessonV1, saved)

    def create_handoff(self, handoff: HandoffV1, *, idempotency_key: str | None = None, request_fingerprint: str | None = None) -> HandoffV1:
        for index, evidence_ref in enumerate(handoff.evidence_refs):
            validate_evidence_ref(evidence_ref, f"evidence_refs[{index}]")
        return cast(HandoffV1, self._create("handoff", handoff, idempotency_key=idempotency_key, request_fingerprint=request_fingerprint, event_type="handoff.created"))

    def get_handoff(self, handoff_id: str) -> HandoffV1:
        return cast(HandoffV1, self._get("handoff", handoff_id))

    def save_handoff(self, handoff: HandoffV1, *, expected_version: int, idempotency_key: str | None = None, request_fingerprint: str | None = None) -> HandoffV1:
        return cast(HandoffV1, self._save("handoff", handoff, expected_version=expected_version, idempotency_key=idempotency_key, request_fingerprint=request_fingerprint, event_type="handoff.updated", actor="service"))

    def record_terminal_link(
        self,
        *,
        decision_id: str,
        source_tool: str,
        database_fingerprint: str,
        scope: Mapping[str, Any],
        outcome_summary: Mapping[str, Any],
        evidence_refs: tuple[str, ...],
        response_fingerprint: str,
        idempotency_key: str | None = None,
        created_at_utc: str | None = None,
    ) -> dict[str, Any]:
        decision = self.get_decision(decision_id)
        if not isinstance(source_tool, str) or not source_tool.strip():
            raise LearningStoreError("source_tool must be a non-empty tool identifier.")
        validate_fingerprint(database_fingerprint, "database_fingerprint", required=True)
        validate_fingerprint(response_fingerprint, "response_fingerprint", required=True)
        created_at = created_at_utc or utc_now()
        if not timestamp_is_before_or_equal(decision.created_at_utc, created_at):
            raise LifecycleError("Terminal evidence cannot precede its decision.")
        safe_scope = structured_summary(scope, "scope")
        decision_scope = dict(decision.scope)
        if any(
            safe_scope.get(key) != expected
            for key, expected in decision_scope.items()
        ):
            raise LifecycleError("Terminal evidence link is outside the decision scope.")
        if decision_scope.get("database_fingerprint") not in (None, database_fingerprint):
            raise LifecycleError("Terminal evidence database scope does not match its decision.")
        safe_scope.setdefault("database_fingerprint", database_fingerprint)
        safe_outcome = structured_summary(outcome_summary, "outcome_summary")
        safe_evidence = tuple(dict.fromkeys(evidence_refs))
        for index, evidence_ref in enumerate(safe_evidence):
            if not isinstance(evidence_ref, str) or not evidence_ref.startswith("evidence-"):
                raise LearningStoreError(
                    f"evidence_refs[{index}] must reference an evidence- identifier."
                )
            try:
                validate_evidence_ref(evidence_ref, f"evidence_refs[{index}]")
            except ValueError as exc:
                raise LearningStoreError(str(exc)) from exc
        request = {
            "decision_id": decision_id,
            "source_tool": source_tool,
            "database_fingerprint": database_fingerprint,
            "scope": safe_scope,
            "outcome_summary": safe_outcome,
            "evidence_refs": list(safe_evidence),
            "response_fingerprint": response_fingerprint,
        }
        fingerprint = hashlib.sha256(_canonical(request).encode()).hexdigest()
        link_id = "terminal-link-" + fingerprint[:32]
        scope_name = "terminal-link.create"
        with self._transaction():
            replay = self._idempotent_replay_locked(
                scope_name,
                idempotency_key,
                "terminal_link",
                fingerprint,
                requested_id=link_id,
            )
            if replay is not None:
                return self.get_terminal_link(replay)
            row = self._connection.execute(
                """SELECT link_id FROM learning_terminal_links
                    WHERE decision_id = ? AND source_tool = ? AND response_fingerprint = ?""",
                (decision_id, source_tool, response_fingerprint),
            ).fetchone()
            if row is not None:
                existing = self.get_terminal_link(str(row["link_id"]))
                if existing["request_fingerprint"] != fingerprint:
                    raise IdempotencyConflictError(
                        "Terminal evidence was replayed with different redacted material."
                    )
                return existing
            self._connection.execute(
                """INSERT INTO learning_terminal_links
                    (link_id, decision_id, source_tool, database_fingerprint, scope,
                     outcome_summary, evidence_refs, response_fingerprint, created_at_utc)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    link_id,
                    decision_id,
                    source_tool,
                    database_fingerprint,
                    _canonical(safe_scope),
                    _canonical(safe_outcome),
                    _canonical(list(safe_evidence)),
                    response_fingerprint,
                    created_at,
                ),
            )
            self._append_event_locked(
                "decision",
                decision_id,
                "decision.terminal_linked",
                {
                    "link_id": link_id,
                    "source_tool": source_tool,
                    "database_fingerprint": database_fingerprint,
                },
                fingerprint,
            )
            self._record_idempotency_locked(
                scope_name,
                idempotency_key,
                "terminal_link",
                link_id,
                fingerprint,
            )
        return self.get_terminal_link(link_id)

    def get_terminal_link(self, link_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM learning_terminal_links WHERE link_id = ?",
                (link_id,),
            ).fetchone()
        if row is None:
            raise ContractNotFoundError(f"Unknown terminal evidence link: {link_id}.")
        payload = {
            "link_id": row["link_id"],
            "decision_id": row["decision_id"],
            "source_tool": row["source_tool"],
            "database_fingerprint": row["database_fingerprint"],
            "scope": json.loads(row["scope"]),
            "outcome_summary": json.loads(row["outcome_summary"]),
            "evidence_refs": json.loads(row["evidence_refs"]),
            "response_fingerprint": row["response_fingerprint"],
            "created_at_utc": row["created_at_utc"],
        }
        request = {key: value for key, value in payload.items() if key not in {"link_id", "created_at_utc"}}
        payload["request_fingerprint"] = hashlib.sha256(_canonical(request).encode()).hexdigest()
        return payload

    def terminal_link_exists(
        self,
        link_id: str,
        *,
        decision_id: str | None = None,
    ) -> bool:
        with self._lock:
            if decision_id is None:
                row = self._connection.execute(
                    "SELECT 1 FROM learning_terminal_links WHERE link_id = ?",
                    (link_id,),
                ).fetchone()
            else:
                row = self._connection.execute(
                    """SELECT 1 FROM learning_terminal_links
                        WHERE link_id = ? AND decision_id = ?""",
                    (link_id, decision_id),
                ).fetchone()
        return row is not None

    def list_support_links(self, lesson_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """SELECT link_id, lesson_id, relationship, reference_type,
                          reference_id, created_at_utc
                    FROM learning_support_links
                    WHERE lesson_id = ? ORDER BY sequence""",
                (lesson_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_lessons(self, *, status: str | None = None) -> list[LessonV1]:
        lessons = cast(list[LessonV1], self._list("lesson"))
        if status is not None:
            lessons = [lesson for lesson in lessons if lesson.status == status]
        return sorted(lessons, key=lambda item: item.lesson_id)

    def list_decisions(self) -> list[DecisionRecordV1]:
        return sorted(cast(list[DecisionRecordV1], self._list("decision")), key=lambda item: item.decision_id)

    def list_reviews(self) -> list[OutcomeReviewV1]:
        return sorted(cast(list[OutcomeReviewV1], self._list("review")), key=lambda item: item.review_id)

    def list_handoffs(self) -> list[HandoffV1]:
        return sorted(cast(list[HandoffV1], self._list("handoff")), key=lambda item: item.handoff_id)

    def _list(self, aggregate_type: str) -> list[VersionedLearningContract]:
        table_name, contract_cls, _id_field = _TABLES[aggregate_type]
        with self._lock:
            rows = self._connection.execute(f"SELECT payload FROM {table_name} ORDER BY rowid").fetchall()
        result: list[VersionedLearningContract] = []
        for row in rows:
            contract = deserialize_learning_contract(row["payload"])
            if not isinstance(contract, contract_cls):
                raise LearningStoreError(f"Stored contract has wrong type for {aggregate_type}.")
            result.append(contract)
        return result

    def list_events(self, *, aggregate_type: str | None = None, aggregate_id: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[str] = []
        if aggregate_type is not None:
            clauses.append("aggregate_type = ?")
            parameters.append(aggregate_type)
        if aggregate_id is not None:
            clauses.append("aggregate_id = ?")
            parameters.append(aggregate_id)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._connection.execute(f"SELECT * FROM learning_events{where} ORDER BY sequence", parameters).fetchall()
        return [self._event_dict(row) for row in rows]
