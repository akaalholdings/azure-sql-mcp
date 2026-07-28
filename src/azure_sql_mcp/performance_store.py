"""Durable SQLite persistence for performance and reviewed action state.

This module has no database execution capability.  It stores only the
versioned contracts and event metadata supplied by the MCP layer. Normal
contracts are redacted; exact view rollback state is stored only through the
separate methods that require an explicit raw-SQL authorization flag.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import threading
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, cast

from .performance_contracts import (
    EvidenceEnvelopeV1,
    PlanActionIntentV1,
    PerformanceCaseV1,
    TuningCandidateV1,
    TuningSessionV1,
    VersionedContract,
    deserialize_contract,
    redact_metadata,
    utc_now,
)


class PerformanceStoreError(RuntimeError):
    """Base class for durable performance state errors."""


class ContractNotFoundError(PerformanceStoreError, KeyError):
    """Raised when a requested contract does not exist."""


class IdempotencyConflictError(PerformanceStoreError):
    """Raised when one idempotency key is reused for another aggregate."""


class ConcurrencyError(PerformanceStoreError):
    """Raised when an optimistic version check fails."""


class ReservationError(PerformanceStoreError):
    """Raised when an atomic budget reservation cannot be made."""


class LeaseConflictError(ReservationError):
    """Raised when another active lease owns the target resource."""


class LeaseFencingError(ConcurrencyError):
    """Raised when a lease update is attempted by a different owner."""


_CONTRACT_TABLES: dict[str, tuple[str, type[VersionedContract], str]] = {
    "evidence": ("evidence_envelopes", EvidenceEnvelopeV1, "evidence_id"),
    "performance_case": ("performance_cases", PerformanceCaseV1, "case_id"),
    "session": ("tuning_sessions", TuningSessionV1, "session_id"),
    "candidate": ("tuning_candidates", TuningCandidateV1, "candidate_id"),
    "plan_action_intent": (
        "plan_action_intents",
        PlanActionIntentV1,
        "intent_id",
    ),
}
_RESERVATION_TTL = timedelta(minutes=30)
_RESERVATION_CLEANUP_GRACE_SECONDS = 60
_IDEMPOTENCY_DIGEST_PATTERN = re.compile(r"^idempotency-v1:[0-9a-f]{64}$")
_INDEX_BENCHMARK_REPLAY_GUIDANCE = (
    "Index benchmark replay requires the exact original request and exact "
    "original idempotency key. Retrieve committed results with get_tuning_session."
)
_VIEW_CHANGE_STATUSES = frozenset(
    {
        "prepared",
        "applying",
        "applied",
        "already_applied",
        "hold",
        "rolled_back",
    }
)


def _require_private_mode(path: Path, mode: int) -> None:
    try:
        os.chmod(path, mode)
        actual = stat.S_IMODE(path.stat().st_mode)
    except OSError as exc:
        raise PerformanceStoreError(
            f"Could not secure performance state path {path}."
        ) from exc
    if os.name != "nt" and actual != mode:
        raise PerformanceStoreError(
            f"Performance state path {path} has mode {oct(actual)}; expected {oct(mode)}."
        )


class PerformanceStore:
    """SQLite-backed store with atomic writes and idempotent operations."""

    def __init__(
        self,
        state_dir: str | Path | None = None,
        *,
        db_path: str | Path | None = None,
    ) -> None:
        if str(db_path) == ":memory:":
            self.state_dir = None
            self.db_path = None
            self._connection = sqlite3.connect(
                ":memory:",
                isolation_level=None,
                check_same_thread=False,
            )
        else:
            if db_path is not None:
                database_path = Path(db_path).expanduser()
                directory = Path(state_dir).expanduser() if state_dir else database_path.parent
            else:
                directory = Path(state_dir).expanduser() if state_dir else (
                    Path.home() / ".azure-sql-mcp" / "state"
                )
                database_path = directory / "performance.sqlite3"
            directory.mkdir(parents=True, exist_ok=True)
            # A relative database path such as ``state.sqlite3`` has ``.``
            # as its parent; never change the permissions of the process cwd.
            if directory != Path("."):
                _require_private_mode(directory, 0o700)
            self.state_dir = directory
            self.db_path = database_path
            database_path.touch(mode=0o600, exist_ok=True)
            _require_private_mode(database_path, 0o600)
            self._connection = sqlite3.connect(
                str(database_path),
                isolation_level=None,
                check_same_thread=False,
            )
            _require_private_mode(database_path, 0o600)

        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA journal_mode = DELETE")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._create_schema()
        if self.db_path is not None:
            _require_private_mode(self.db_path, 0o600)

    def __enter__(self) -> PerformanceStore:
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def _create_schema(self) -> None:
        with self._transaction():
            for table_name, _contract_cls, id_field in _CONTRACT_TABLES.values():
                self._connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {id_field} TEXT PRIMARY KEY,
                        contract_version INTEGER NOT NULL,
                        payload TEXT NOT NULL,
                        created_at_utc TEXT NOT NULL,
                        updated_at_utc TEXT NOT NULL
                    )
                    """
                )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS tuning_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    idempotency_key TEXT UNIQUE,
                    aggregate_type TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    request_fingerprint TEXT,
                    created_at_utc TEXT NOT NULL
                )
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tuning_events_aggregate
                ON tuning_events (aggregate_type, aggregate_id, sequence)
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS operation_idempotency (
                    scope TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    aggregate_type TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    PRIMARY KEY (scope, idempotency_key)
                )
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS index_leases (
                    lease_id TEXT PRIMARY KEY,
                    database_fingerprint TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    candidate_id TEXT NOT NULL,
                    index_name TEXT NOT NULL,
                    object_fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL,
                    expires_at_utc TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    owner_token TEXT NOT NULL DEFAULT '',
                    lease_version INTEGER NOT NULL DEFAULT 0,
                    request_fingerprint TEXT,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_index_leases_active
                ON index_leases (database_fingerprint, status, expires_at_utc)
                """
            )
            self._ensure_column("operation_idempotency", "request_fingerprint", "TEXT")
            self._ensure_column("tuning_events", "request_fingerprint", "TEXT")
            self._ensure_column("index_leases", "owner_token", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(
                "index_leases",
                "lease_version",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column("index_leases", "request_fingerprint", "TEXT")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS candidate_slot_reservations (
                    reservation_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    request_fingerprint TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    owner_token TEXT NOT NULL,
                    reservation_version INTEGER NOT NULL DEFAULT 0,
                    expires_at_utc TEXT,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    UNIQUE (session_id, request_fingerprint)
                )
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_candidate_slot_reservations_session
                ON candidate_slot_reservations (session_id, status)
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_reservations (
                    reservation_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    candidate_id TEXT NOT NULL,
                    request_fingerprint TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL,
                    dispatched_attempt_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    owner_token TEXT NOT NULL,
                    reservation_version INTEGER NOT NULL DEFAULT 0,
                    expires_at_utc TEXT,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    UNIQUE (session_id, request_fingerprint)
                )
                """
            )
            self._ensure_column(
                "candidate_slot_reservations",
                "expires_at_utc",
                "TEXT",
            )
            dispatched_count_added = self._ensure_column(
                "execution_reservations",
                "dispatched_attempt_count",
                "INTEGER NOT NULL DEFAULT 0",
            )
            if dispatched_count_added:
                # Completed rows were charged their full reservation before
                # partial accounting existed. Released rows represented work
                # that never started and must remain uncharged.
                self._connection.execute(
                    """
                    UPDATE execution_reservations
                    SET dispatched_attempt_count = attempt_count
                    WHERE status = 'completed'
                    """
                )
            self._ensure_column(
                "execution_reservations",
                "expires_at_utc",
                "TEXT",
            )
            for table_name in (
                "candidate_slot_reservations",
                "execution_reservations",
            ):
                self._connection.execute(
                    f"""
                    UPDATE {table_name}
                    SET expires_at_utc = updated_at_utc
                    WHERE expires_at_utc IS NULL
                    """
                )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_execution_reservations_session
                ON execution_reservations (session_id, status)
                """
            )
            self._connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_reservations_active_candidate
                ON execution_reservations (candidate_id)
                WHERE status = 'reserved'
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS view_change_intents (
                    change_id TEXT PRIMARY KEY,
                    database_fingerprint TEXT NOT NULL,
                    idempotency_key_digest TEXT,
                    request_fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    receipt TEXT,
                    intent_version INTEGER NOT NULL DEFAULT 0,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            self._ensure_column(
                "view_change_intents",
                "idempotency_key_digest",
                "TEXT",
            )
            self._migrate_view_idempotency_keys_locked()
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_view_change_intents_idempotency
                ON view_change_intents (
                    database_fingerprint,
                    idempotency_key_digest
                )
                """
            )

    def _ensure_column(self, table: str, column: str, definition: str) -> bool:
        columns = {
            str(row[1])
            for row in self._connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in columns:
            self._connection.execute(
                f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
            )
            return True
        return False

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

    def _migrate_view_idempotency_keys_locked(self) -> None:
        rows = self._connection.execute(
            """
            SELECT change_id, idempotency_key_digest, request_fingerprint, payload
            FROM view_change_intents
            """
        ).fetchall()
        for row in rows:
            try:
                payload = json.loads(row["payload"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise PerformanceStoreError(
                    f"Stored view change intent {row['change_id']} is invalid."
                ) from exc
            if not isinstance(payload, dict):
                raise PerformanceStoreError(
                    f"Stored view change intent {row['change_id']} is invalid."
                )
            request = payload.get("request")
            stored_digest = row["idempotency_key_digest"]
            if not isinstance(request, dict):
                continue
            original_serialized = json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            )
            original_fingerprint = hashlib.sha256(
                original_serialized.encode("utf-8")
            ).hexdigest()
            if row["request_fingerprint"] != original_fingerprint:
                raise PerformanceStoreError(
                    f"Stored view change intent {row['change_id']} failed its "
                    "payload integrity check."
                )
            stored_key = request.get("idempotency_key")
            if stored_digest is not None:
                if (
                    not isinstance(stored_digest, str)
                    or not _IDEMPOTENCY_DIGEST_PATTERN.fullmatch(stored_digest)
                    or stored_key != stored_digest
                ):
                    raise PerformanceStoreError(
                        f"Stored view change intent {row['change_id']} has an "
                        "invalid idempotency digest."
                    )
            elif stored_key is not None:
                if not isinstance(stored_key, str) or not stored_key.strip():
                    raise PerformanceStoreError(
                        f"Stored view change intent {row['change_id']} has an "
                        "invalid idempotency key."
                    )
                stored_digest = self._view_idempotency_key_digest(stored_key)
                request["idempotency_key"] = stored_digest
            else:
                # There is no idempotency material to migrate. Keep the exact
                # payload untouched so normal read-time integrity checks apply.
                continue
            serialized = json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            )
            payload_fingerprint = hashlib.sha256(
                serialized.encode("utf-8")
            ).hexdigest()
            if (
                serialized != row["payload"]
                or stored_digest != row["idempotency_key_digest"]
                or payload_fingerprint != row["request_fingerprint"]
            ):
                self._connection.execute(
                    """
                    UPDATE view_change_intents
                    SET idempotency_key_digest = ?, request_fingerprint = ?,
                        payload = ?
                    WHERE change_id = ?
                    """,
                    (
                        stored_digest,
                        payload_fingerprint,
                        serialized,
                        row["change_id"],
                    ),
                )

    @staticmethod
    def _payload(contract: VersionedContract) -> str:
        # Contract serialization performs the final redaction as well as the
        # explicit contract_type/version encoding.
        return contract.to_json()

    @staticmethod
    def _event_payload(payload: Mapping[str, Any] | None) -> str:
        return json.dumps(
            redact_metadata(payload),
            sort_keys=True,
            separators=(",", ":"),
        )

    def _idempotent_aggregate_locked(
        self,
        scope: str,
        idempotency_key: str | None,
        aggregate_type: str,
        requested_id: str | None = None,
        request_fingerprint: str | None = None,
        legacy_request_contract: VersionedContract | None = None,
    ) -> str | None:
        if not idempotency_key:
            return None
        row = self._connection.execute(
            """
            SELECT aggregate_type, aggregate_id
                   , request_fingerprint
            FROM operation_idempotency
            WHERE scope = ? AND idempotency_key = ?
            """,
            (scope, idempotency_key),
        ).fetchone()
        if row is None:
            return None
        if row["aggregate_type"] != aggregate_type or (
            requested_id is not None and row["aggregate_id"] != requested_id
        ):
            raise IdempotencyConflictError(
                f"Idempotency key {idempotency_key!r} is already bound to "
                f"{row['aggregate_type']}:{row['aggregate_id']}."
            )
        stored_fingerprint = row["request_fingerprint"]
        if (
            request_fingerprint is not None
            and stored_fingerprint is not None
            and stored_fingerprint != request_fingerprint
        ):
            raise IdempotencyConflictError(
                f"Idempotency key {idempotency_key!r} was replayed with a different request."
            )
        if request_fingerprint is not None and stored_fingerprint is None:
            if legacy_request_contract is None:
                raise IdempotencyConflictError(
                    "Legacy idempotency binding has no request fingerprint and "
                    "cannot be safely verified."
                )
            try:
                stored_contract = self._get_locked(
                    aggregate_type,
                    str(row["aggregate_id"]),
                )
            except PerformanceStoreError as exc:
                raise IdempotencyConflictError(
                    "Legacy idempotency binding references an aggregate that "
                    "cannot be safely verified."
                ) from exc
            if self._legacy_request_signature(
                stored_contract
            ) != self._legacy_request_signature(legacy_request_contract):
                raise IdempotencyConflictError(
                    "Legacy idempotency key was replayed with a different request."
                )
            self._connection.execute(
                """
                UPDATE operation_idempotency
                SET request_fingerprint = ?
                WHERE scope = ? AND idempotency_key = ?
                  AND request_fingerprint IS NULL
                """,
                (request_fingerprint, scope, idempotency_key),
            )
        return str(row["aggregate_id"])

    @staticmethod
    def _legacy_request_signature(
        contract: VersionedContract,
    ) -> str:
        payload = contract.to_dict()
        ignored_fields: dict[type[VersionedContract], frozenset[str]] = {
            EvidenceEnvelopeV1: frozenset(
                {"evidence_id", "captured_at_utc"}
            ),
            PerformanceCaseV1: frozenset(
                {
                    "case_id",
                    "created_at_utc",
                    "updated_at_utc",
                    "baseline_evidence_ids",
                    "status",
                    "version",
                }
            ),
            TuningCandidateV1: frozenset(
                {
                    "candidate_id",
                    "ordinal",
                    "state",
                    "screen_runs",
                    "finalist_runs",
                    "parameter_cases",
                    "executions",
                    "evidence_ids",
                    "failure_code",
                    "created_at_utc",
                    "updated_at_utc",
                    "version",
                }
            ),
            TuningSessionV1: frozenset(
                {
                    "session_id",
                    "status",
                    "created_at_utc",
                    "updated_at_utc",
                    "started_at_utc",
                    "deadline_at_utc",
                    "candidate_ids",
                    "finalist_candidate_ids",
                    "selected_candidate_id",
                    "stopping_reason",
                    "version",
                }
            ),
            PlanActionIntentV1: frozenset(
                {
                    "intent_id",
                    "status",
                    "created_at_utc",
                    "updated_at_utc",
                    "version",
                }
            ),
        }
        ignored = ignored_fields.get(type(contract))
        if ignored is None:
            raise IdempotencyConflictError(
                "Legacy idempotency binding references an unsupported aggregate."
            )
        comparable = {
            key: value
            for key, value in payload.items()
            if key not in ignored
        }
        return json.dumps(
            comparable,
            sort_keys=True,
            separators=(",", ":"),
        )

    def _record_idempotency_locked(
        self,
        scope: str,
        idempotency_key: str | None,
        aggregate_type: str,
        aggregate_id: str,
        request_fingerprint: str | None = None,
    ) -> None:
        if not idempotency_key:
            return
        try:
            self._connection.execute(
                """
                INSERT INTO operation_idempotency
                    (scope, idempotency_key, aggregate_type, aggregate_id,
                     request_fingerprint, created_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    scope,
                    idempotency_key,
                    aggregate_type,
                    aggregate_id,
                    request_fingerprint,
                    utc_now(),
                ),
            )
        except sqlite3.IntegrityError:
            self._idempotent_aggregate_locked(
                scope,
                idempotency_key,
                aggregate_type,
                requested_id=aggregate_id,
                request_fingerprint=request_fingerprint,
            )

    def _row_to_contract(
        self,
        row: sqlite3.Row | None,
        contract_cls: type[VersionedContract],
        identifier: str,
    ) -> VersionedContract:
        if row is None:
            raise ContractNotFoundError(f"Unknown {contract_cls.contract_type}: {identifier}.")
        try:
            contract = deserialize_contract(row["payload"])
        except (TypeError, ValueError) as exc:
            raise PerformanceStoreError(
                f"Stored {contract_cls.contract_type} {identifier} is invalid."
            ) from exc
        if not isinstance(contract, contract_cls):
            raise PerformanceStoreError(
                f"Stored contract for {identifier} is not {contract_cls.contract_type}."
            )
        return contract

    def _get_locked(self, aggregate_type: str, identifier: str) -> VersionedContract:
        table_name, contract_cls, id_field = _CONTRACT_TABLES[aggregate_type]
        row = self._connection.execute(
            f"SELECT payload FROM {table_name} WHERE {id_field} = ?",
            (identifier,),
        ).fetchone()
        return self._row_to_contract(row, contract_cls, identifier)

    def _save_locked(
        self,
        aggregate_type: str,
        contract: VersionedContract,
        *,
        insert_only: bool,
        expected_version: int | None,
    ) -> None:
        table_name, _contract_cls, id_field = _CONTRACT_TABLES[aggregate_type]
        identifier = str(getattr(contract, id_field))
        payload = self._payload(contract)
        existing = self._connection.execute(
            f"SELECT payload FROM {table_name} WHERE {id_field} = ?",
            (identifier,),
        ).fetchone()
        if insert_only and existing is not None:
            raise PerformanceStoreError(f"{aggregate_type} {identifier} already exists.")
        if existing is None:
            if expected_version not in (None, -1):
                raise ConcurrencyError(f"Cannot update missing {aggregate_type} {identifier}.")
            now = utc_now()
            self._connection.execute(
                f"""
                INSERT INTO {table_name}
                    ({id_field}, contract_version, payload, created_at_utc, updated_at_utc)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    identifier,
                    int(getattr(contract, "contract_version")),
                    payload,
                    now,
                    now,
                ),
            )
            return

        current = deserialize_contract(existing["payload"])
        current_version = int(getattr(current, "version", 0))
        if expected_version is not None and current_version != expected_version:
            raise ConcurrencyError(
                f"{aggregate_type} {identifier} is version {current_version}; "
                f"expected {expected_version}."
            )
        incoming_version = int(getattr(contract, "version", 0))
        if incoming_version < current_version:
            raise ConcurrencyError(
                f"{aggregate_type} {identifier} cannot move from version "
                f"{current_version} back to {incoming_version}."
            )
        if expected_version is not None and incoming_version != current_version + 1:
            raise ConcurrencyError(
                f"{aggregate_type} {identifier} must advance to version "
                f"{current_version + 1}; got {incoming_version}."
            )
        self._connection.execute(
            f"""
            UPDATE {table_name}
            SET contract_version = ?, payload = ?, updated_at_utc = ?
            WHERE {id_field} = ?
            """,
            (
                int(getattr(contract, "contract_version")),
                payload,
                utc_now(),
                identifier,
            ),
        )

    def _append_event_locked(
        self,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: Mapping[str, Any] | None,
        event_id: str | None = None,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        if idempotency_key:
            existing = self._connection.execute(
                "SELECT * FROM tuning_events WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["aggregate_type"] != aggregate_type
                    or existing["aggregate_id"] != aggregate_id
                ):
                    raise IdempotencyConflictError(
                        f"Event idempotency key {idempotency_key!r} is already bound elsewhere."
                    )
                if (
                    request_fingerprint is not None
                    and existing["request_fingerprint"] is not None
                    and existing["request_fingerprint"] != request_fingerprint
                ):
                    raise IdempotencyConflictError(
                        f"Event idempotency key {idempotency_key!r} was replayed with a different request."
                    )
                return self._event_dict(existing)
        if event_id:
            existing = self._connection.execute(
                "SELECT * FROM tuning_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if existing is not None:
                return self._event_dict(existing)
        actual_event_id = event_id or f"event-{os.urandom(12).hex()}"
        self._connection.execute(
            """
            INSERT INTO tuning_events
                (event_id, idempotency_key, aggregate_type, aggregate_id,
                 event_type, payload, request_fingerprint, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                actual_event_id,
                idempotency_key,
                aggregate_type,
                aggregate_id,
                event_type,
                self._event_payload(payload),
                request_fingerprint,
                utc_now(),
            ),
        )
        row = self._connection.execute(
            "SELECT * FROM tuning_events WHERE event_id = ?",
            (actual_event_id,),
        ).fetchone()
        return self._event_dict(row)

    @staticmethod
    def _event_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "sequence": int(row["sequence"]),
            "event_id": row["event_id"],
            "idempotency_key": row["idempotency_key"],
            "aggregate_type": row["aggregate_type"],
            "aggregate_id": row["aggregate_id"],
            "event_type": row["event_type"],
            "payload": json.loads(row["payload"]),
            "request_fingerprint": row["request_fingerprint"],
            "created_at_utc": row["created_at_utc"],
        }

    def _create(
        self,
        aggregate_type: str,
        contract: VersionedContract,
        *,
        idempotency_key: str | None,
        event_type: str,
        request_fingerprint: str | None = None,
    ) -> VersionedContract:
        _table, contract_cls, id_field = _CONTRACT_TABLES[aggregate_type]
        identifier = str(getattr(contract, id_field))
        scope = f"{aggregate_type}.create"
        with self._transaction():
            existing_id = self._idempotent_aggregate_locked(
                scope,
                idempotency_key,
                aggregate_type,
                request_fingerprint=request_fingerprint,
                legacy_request_contract=contract,
            )
            if existing_id is not None:
                return self._get_locked(aggregate_type, existing_id)
            self._save_locked(
                aggregate_type,
                contract,
                insert_only=True,
                expected_version=None,
            )
            self._append_event_locked(
                event_type=event_type,
                aggregate_type=aggregate_type,
                aggregate_id=identifier,
                payload={"contract_type": contract_cls.contract_type},
                idempotency_key=(f"{scope}:{idempotency_key}" if idempotency_key else None),
                request_fingerprint=request_fingerprint,
            )
            self._record_idempotency_locked(
                scope,
                idempotency_key,
                aggregate_type,
                identifier,
                request_fingerprint=request_fingerprint,
            )
        return contract

    def _save(
        self,
        aggregate_type: str,
        contract: VersionedContract,
        *,
        expected_version: int | None,
        idempotency_key: str | None,
        event_type: str,
        event_payload: Mapping[str, Any] | None,
        request_fingerprint: str | None = None,
    ) -> VersionedContract:
        _table, _contract_cls, id_field = _CONTRACT_TABLES[aggregate_type]
        identifier = str(getattr(contract, id_field))
        scope = f"{aggregate_type}.save"
        with self._transaction():
            existing_id = self._idempotent_aggregate_locked(
                scope,
                idempotency_key,
                aggregate_type,
                requested_id=identifier,
                request_fingerprint=request_fingerprint,
            )
            if existing_id is not None:
                return self._get_locked(aggregate_type, existing_id)
            self._save_locked(
                aggregate_type,
                contract,
                insert_only=False,
                expected_version=expected_version,
            )
            self._append_event_locked(
                event_type=event_type,
                aggregate_type=aggregate_type,
                aggregate_id=identifier,
                payload=event_payload,
                idempotency_key=(f"{scope}:{idempotency_key}" if idempotency_key else None),
                request_fingerprint=request_fingerprint,
            )
            self._record_idempotency_locked(
                scope,
                idempotency_key,
                aggregate_type,
                identifier,
                request_fingerprint=request_fingerprint,
            )
        return contract

    def create_evidence(
        self,
        evidence: EvidenceEnvelopeV1,
        *,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> EvidenceEnvelopeV1:
        return self._create(
            "evidence",
            evidence,
            idempotency_key=idempotency_key,
            event_type="evidence.created",
            request_fingerprint=request_fingerprint,
        )  # type: ignore[return-value]

    def get_evidence(self, evidence_id: str) -> EvidenceEnvelopeV1:
        return self._get("evidence", evidence_id)  # type: ignore[return-value]

    def list_evidence_for_session(
        self,
        session_id: str,
    ) -> list[EvidenceEnvelopeV1]:
        """Return durable evidence whose redacted metadata names one session."""

        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id is required.")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT evidence_id, payload
                FROM evidence_envelopes
                ORDER BY evidence_id
                """
            ).fetchall()
        evidence = [
            cast(
                EvidenceEnvelopeV1,
                self._row_to_contract(
                    row,
                    EvidenceEnvelopeV1,
                    str(row["evidence_id"]),
                ),
            )
            for row in rows
        ]
        return sorted(
            (
                envelope
                for envelope in evidence
                if envelope.metadata.get("session_id") == session_id
            ),
            key=lambda envelope: (
                envelope.captured_at_utc,
                envelope.evidence_id,
            ),
        )

    def get_idempotent_evidence(
        self,
        idempotency_key: str | None,
        *,
        request_fingerprint: str | None = None,
    ) -> EvidenceEnvelopeV1 | None:
        """Return evidence already committed for one idempotent request."""

        if not idempotency_key:
            return None
        with self._transaction():
            evidence_id = self._idempotent_aggregate_locked(
                "evidence.create",
                idempotency_key,
                "evidence",
                request_fingerprint=request_fingerprint,
            )
            if evidence_id is None:
                return None
            return cast(EvidenceEnvelopeV1, self._get_locked("evidence", evidence_id))

    def create_performance_case(
        self,
        performance_case: PerformanceCaseV1,
        *,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> PerformanceCaseV1:
        return self._create(
            "performance_case",
            performance_case,
            idempotency_key=idempotency_key,
            event_type="performance_case.created",
            request_fingerprint=request_fingerprint,
        )  # type: ignore[return-value]

    def get_performance_case(self, case_id: str) -> PerformanceCaseV1:
        return self._get("performance_case", case_id)  # type: ignore[return-value]

    def save_performance_case(
        self,
        performance_case: PerformanceCaseV1,
        *,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> PerformanceCaseV1:
        return self._save(
            "performance_case",
            performance_case,
            expected_version=expected_version,
            idempotency_key=idempotency_key,
            event_type="performance_case.updated",
            event_payload={"contract_type": PerformanceCaseV1.contract_type},
            request_fingerprint=request_fingerprint,
        )  # type: ignore[return-value]

    def create_session(
        self,
        session: TuningSessionV1,
        *,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> TuningSessionV1:
        return self._create(
            "session",
            session,
            idempotency_key=idempotency_key,
            event_type="session.created",
            request_fingerprint=request_fingerprint,
        )  # type: ignore[return-value]

    def get_session(self, session_id: str) -> TuningSessionV1:
        return self._get("session", session_id)  # type: ignore[return-value]

    def save_session(
        self,
        session: TuningSessionV1,
        *,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
        event_type: str = "session.updated",
        event_payload: Mapping[str, Any] | None = None,
    ) -> TuningSessionV1:
        return self._save(
            "session",
            session,
            expected_version=expected_version,
            idempotency_key=idempotency_key,
            event_type=event_type,
            event_payload=event_payload,
            request_fingerprint=request_fingerprint,
        )  # type: ignore[return-value]

    def create_candidate(
        self,
        candidate: TuningCandidateV1,
        *,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> TuningCandidateV1:
        return self._create(
            "candidate",
            candidate,
            idempotency_key=idempotency_key,
            event_type="candidate.created",
            request_fingerprint=request_fingerprint,
        )  # type: ignore[return-value]

    def get_candidate(self, candidate_id: str) -> TuningCandidateV1:
        return self._get("candidate", candidate_id)  # type: ignore[return-value]

    def save_candidate(
        self,
        candidate: TuningCandidateV1,
        *,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
        event_type: str = "candidate.updated",
        event_payload: Mapping[str, Any] | None = None,
    ) -> TuningCandidateV1:
        return self._save(
            "candidate",
            candidate,
            expected_version=expected_version,
            idempotency_key=idempotency_key,
            event_type=event_type,
            event_payload=event_payload,
            request_fingerprint=request_fingerprint,
        )  # type: ignore[return-value]

    def list_candidates(self, session_id: str) -> list[TuningCandidateV1]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT payload FROM tuning_candidates ORDER BY candidate_id"
            ).fetchall()
        candidates = [TuningCandidateV1.from_json(row["payload"]) for row in rows]
        return sorted(
            (candidate for candidate in candidates if candidate.session_id == session_id),
            key=lambda candidate: (candidate.ordinal, candidate.candidate_id),
        )

    @staticmethod
    def _new_token(prefix: str) -> str:
        return f"{prefix}-{os.urandom(16).hex()}"

    @staticmethod
    def _hashed_idempotency_key(scope: str, value: str) -> str:
        digest = hashlib.sha256(
            f"idempotency-v1:{scope}:{value}".encode("utf-8")
        ).hexdigest()
        return f"idempotency-v1:{digest}"

    @staticmethod
    def _view_idempotency_key_digest(value: str) -> str:
        if _IDEMPOTENCY_DIGEST_PATTERN.fullmatch(value):
            return value
        return PerformanceStore._hashed_idempotency_key(
            "view-change.intent",
            value,
        )

    @staticmethod
    def _require_fingerprint(value: str, field_name: str = "request_fingerprint") -> str:
        if not isinstance(value, str) or not value.strip() or len(value) > 512:
            raise ValueError(f"{field_name} must be a non-empty string of at most 512 characters.")
        return value

    @staticmethod
    def _check_owner(stored: str, supplied: str | None, resource: str) -> None:
        if stored and supplied != stored:
            raise LeaseFencingError(f"{resource} is owned by another caller.")

    def reserve_candidate_slot(
        self,
        session_id: str,
        request_fingerprint: str,
        *,
        owner_reference: str | None = None,
    ) -> dict[str, Any]:
        """Atomically reserve one candidate ordinal against the session budget."""

        fingerprint = self._require_fingerprint(request_fingerprint)
        reservation_owner = owner_reference or self._new_token("candidate-owner")
        now, expires_at = self._reservation_times()
        with self._transaction():
            self._expire_reservations_locked(now)
            existing = self._connection.execute(
                """
                SELECT * FROM candidate_slot_reservations
                WHERE session_id = ? AND request_fingerprint = ?
                """,
                (session_id, fingerprint),
            ).fetchone()
            if existing is not None:
                if owner_reference is not None:
                    self._check_owner(
                        existing["owner_token"],
                        owner_reference,
                        f"candidate slot {existing['reservation_id']}",
                    )
                result = self._candidate_slot_dict(existing)
                result["replayed"] = True
                return result
            session = cast(TuningSessionV1, self._get_locked("session", session_id))
            candidate_count = len(self._list_candidates_locked(session_id))
            held_count = int(
                self._connection.execute(
                    """
                    SELECT COUNT(*) FROM candidate_slot_reservations
                    WHERE session_id = ? AND status = 'reserved'
                    """,
                    (session_id,),
                ).fetchone()[0]
            )
            if candidate_count + held_count >= session.max_candidates:
                raise ReservationError("Maximum candidate budget has been reserved.")
            ordinal = self._next_candidate_ordinal_locked(session_id)
            reservation_id = self._new_token("candidate-slot")
            self._connection.execute(
                """
                INSERT INTO candidate_slot_reservations
                    (reservation_id, session_id, request_fingerprint, ordinal, status,
                     owner_token, reservation_version, expires_at_utc,
                     created_at_utc, updated_at_utc)
                VALUES (?, ?, ?, ?, 'reserved', ?, 0, ?, ?, ?)
                """,
                (
                    reservation_id,
                    session_id,
                    fingerprint,
                    ordinal,
                    reservation_owner,
                    expires_at,
                    now,
                    now,
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM candidate_slot_reservations WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()
            assert row is not None
            result = self._candidate_slot_dict(row)
            result["replayed"] = False
            return result

    def release_candidate_slot(
        self,
        reservation_id: str,
        *,
        owner_reference: str | None = None,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        return self._update_candidate_slot(
            reservation_id,
            status="released",
            owner_reference=owner_reference,
            expected_version=expected_version,
        )

    def _update_candidate_slot(
        self,
        reservation_id: str,
        *,
        status: str,
        owner_reference: str | None,
        expected_version: int | None,
    ) -> dict[str, Any]:
        if status not in {"consumed", "released"}:
            raise ValueError(f"Unsupported candidate slot status: {status!r}.")
        with self._transaction():
            self._expire_reservations_locked(utc_now())
            row = self._connection.execute(
                "SELECT * FROM candidate_slot_reservations WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()
            if row is None:
                raise ContractNotFoundError(f"Unknown candidate slot reservation: {reservation_id}.")
            self._check_owner(
                row["owner_token"],
                owner_reference,
                f"candidate slot {reservation_id}",
            )
            current_version = int(row["reservation_version"])
            if expected_version is not None and current_version != expected_version:
                raise ConcurrencyError(
                    f"Candidate slot {reservation_id} is version {current_version}; "
                    f"expected {expected_version}."
                )
            if row["status"] != "reserved":
                return self._candidate_slot_dict(row)
            self._connection.execute(
                """
                UPDATE candidate_slot_reservations
                SET status = ?, reservation_version = reservation_version + 1,
                    updated_at_utc = ?
                WHERE reservation_id = ? AND reservation_version = ?
                """,
                (status, utc_now(), reservation_id, current_version),
            )
            updated = self._connection.execute(
                "SELECT * FROM candidate_slot_reservations WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()
            assert updated is not None
            return self._candidate_slot_dict(updated)

    def create_candidate_and_attach(
        self,
        session: TuningSessionV1,
        candidate: TuningCandidateV1,
        *,
        expected_session_version: int | None = None,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
        reservation_id: str | None = None,
        owner_reference: str | None = None,
    ) -> TuningCandidateV1:
        """Create a candidate and attach it to its session in one transaction."""

        if candidate.session_id != session.session_id:
            raise ValueError("Candidate and session identifiers do not match.")
        operation_fingerprint = (
            self._require_fingerprint(request_fingerprint)
            if request_fingerprint is not None
            else None
        )
        fingerprint = operation_fingerprint or f"candidate:{candidate.candidate_id}"
        scope = "candidate.attach"
        with self._transaction():
            self._expire_reservations_locked(utc_now())
            existing_id = self._idempotent_aggregate_locked(
                scope,
                idempotency_key,
                "candidate",
                request_fingerprint=operation_fingerprint,
                legacy_request_contract=candidate,
            )
            if existing_id is not None:
                return cast(TuningCandidateV1, self._get_locked("candidate", existing_id))
            current_session = cast(TuningSessionV1, self._get_locked("session", session.session_id))
            if (
                expected_session_version is not None
                and current_session.version != expected_session_version
            ):
                raise ConcurrencyError(
                    f"session {session.session_id} is version {current_session.version}; "
                    f"expected {expected_session_version}."
                )
            candidate_count = len(self._list_candidates_locked(session.session_id))
            if candidate_count >= current_session.max_candidates:
                raise ReservationError("Maximum candidate budget has been reached.")

            if reservation_id is not None:
                reservation = self._connection.execute(
                    "SELECT * FROM candidate_slot_reservations WHERE reservation_id = ?",
                    (reservation_id,),
                ).fetchone()
                if reservation is None or reservation["session_id"] != session.session_id:
                    raise ContractNotFoundError(
                        f"Unknown candidate slot reservation: {reservation_id}."
                    )
                self._check_owner(
                    reservation["owner_token"],
                    owner_reference,
                    f"candidate slot {reservation_id}",
                )
                if reservation["status"] != "reserved":
                    raise ConcurrencyError(f"Candidate slot {reservation_id} is not available.")
                ordinal = int(reservation["ordinal"])
                self._connection.execute(
                    """
                    UPDATE candidate_slot_reservations
                    SET status = 'consumed', reservation_version = reservation_version + 1,
                        updated_at_utc = ?
                    WHERE reservation_id = ?
                    """,
                    (utc_now(), reservation_id),
                )
            else:
                ordinal = self._next_candidate_ordinal_locked(session.session_id)
                self._connection.execute(
                    """
                    INSERT INTO candidate_slot_reservations
                        (reservation_id, session_id, request_fingerprint, ordinal, status,
                         owner_token, reservation_version, expires_at_utc,
                         created_at_utc, updated_at_utc)
                    VALUES (?, ?, ?, ?, 'consumed', ?, 0, ?, ?, ?)
                    """,
                    (
                        self._new_token("candidate-slot"),
                        session.session_id,
                        fingerprint,
                        ordinal,
                        owner_reference or self._new_token("candidate-owner"),
                        utc_now(),
                        utc_now(),
                        utc_now(),
                    ),
                )

            created_candidate = replace(candidate, ordinal=ordinal)
            updated_session = replace(
                current_session,
                candidate_ids=tuple((*current_session.candidate_ids, created_candidate.candidate_id)),
                updated_at_utc=utc_now(),
                version=current_session.version + 1,
            )
            self._save_locked(
                "candidate",
                created_candidate,
                insert_only=True,
                expected_version=None,
            )
            self._save_locked(
                "session",
                updated_session,
                insert_only=False,
                expected_version=current_session.version,
            )
            payload = {"candidate_id": created_candidate.candidate_id}
            self._append_event_locked(
                event_type="candidate.created",
                aggregate_type="candidate",
                aggregate_id=created_candidate.candidate_id,
                payload=payload,
                idempotency_key=(f"{scope}:{idempotency_key}:candidate" if idempotency_key else None),
                request_fingerprint=operation_fingerprint,
            )
            self._append_event_locked(
                event_type="candidate.added",
                aggregate_type="session",
                aggregate_id=session.session_id,
                payload=payload,
                idempotency_key=(f"{scope}:{idempotency_key}:session" if idempotency_key else None),
                request_fingerprint=operation_fingerprint,
            )
            self._record_idempotency_locked(
                scope,
                idempotency_key,
                "candidate",
                created_candidate.candidate_id,
                request_fingerprint=fingerprint,
            )
            return created_candidate

    def get_idempotent_candidate_creation(
        self,
        session_id: str,
        idempotency_key: str | None,
        *,
        request_fingerprint: str,
    ) -> TuningCandidateV1 | None:
        """Return a committed candidate creation without changing state."""

        if not idempotency_key:
            return None
        fingerprint = self._require_fingerprint(request_fingerprint)
        with self._transaction():
            candidate_id = self._idempotent_aggregate_locked(
                "candidate.attach",
                idempotency_key,
                "candidate",
                request_fingerprint=fingerprint,
            )
            if candidate_id is None:
                return None
            candidate = cast(
                TuningCandidateV1,
                self._get_locked("candidate", candidate_id),
            )
            if candidate.session_id != session_id:
                raise IdempotencyConflictError(
                    f"Idempotency key {idempotency_key!r} belongs to another session."
                )
            return candidate

    @staticmethod
    def _candidate_slot_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "reservation_id": row["reservation_id"],
            "session_id": row["session_id"],
            "request_fingerprint": row["request_fingerprint"],
            "ordinal": int(row["ordinal"]),
            "status": row["status"],
            "version": int(row["reservation_version"]),
            "expires_at_utc": row["expires_at_utc"],
            "created_at_utc": row["created_at_utc"],
            "updated_at_utc": row["updated_at_utc"],
        }

    def reserve_execution_attempts(
        self,
        session_id: str,
        candidate_id: str,
        attempt_count: int,
        request_fingerprint: str,
        *,
        owner_reference: str,
        idempotency_key: str | None = None,
        max_runtime_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Atomically reserve execution budget for one idempotent request."""

        if not isinstance(attempt_count, int) or attempt_count <= 0:
            raise ValueError("attempt_count must be greater than 0.")
        fingerprint = self._require_fingerprint(request_fingerprint)
        if not isinstance(owner_reference, str) or not owner_reference.strip():
            raise ValueError("owner_reference is required for execution reservations.")
        if max_runtime_seconds is not None and (
            isinstance(max_runtime_seconds, bool)
            or not isinstance(max_runtime_seconds, (int, float))
            or not math.isfinite(float(max_runtime_seconds))
            or max_runtime_seconds <= 0
        ):
            raise ValueError("max_runtime_seconds must be a positive finite number.")
        reservation_owner = owner_reference
        hashed_idempotency_key = (
            self._hashed_idempotency_key("execution.reserve", idempotency_key)
            if idempotency_key
            else None
        )
        with self._transaction():
            reservation_runtime_seconds = max_runtime_seconds
            if reservation_runtime_seconds is None:
                session_row = self._connection.execute(
                    "SELECT payload FROM tuning_sessions WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
                if session_row is not None:
                    session = cast(
                        TuningSessionV1,
                        self._row_to_contract(
                            session_row,
                            TuningSessionV1,
                            session_id,
                        ),
                    )
                    reservation_runtime_seconds = session.time_limit_seconds
            now, expires_at = self._reservation_times(
                max_runtime_seconds=reservation_runtime_seconds,
            )
            self._expire_reservations_locked(now)
            if hashed_idempotency_key is not None:
                existing_id = self._idempotent_aggregate_locked(
                    "execution.reserve",
                    hashed_idempotency_key,
                    "candidate",
                    requested_id=candidate_id,
                    request_fingerprint=fingerprint,
                )
                if existing_id is not None:
                    existing = self._connection.execute(
                        """
                        SELECT * FROM execution_reservations
                        WHERE session_id = ? AND candidate_id = ?
                          AND request_fingerprint = ?
                        """,
                        (session_id, candidate_id, fingerprint),
                    ).fetchone()
                    if existing is None:
                        raise PerformanceStoreError(
                            "Execution idempotency binding has no reservation."
                        )
                    if int(existing["attempt_count"]) != attempt_count:
                        raise IdempotencyConflictError(
                            "Execution idempotency key was reused for a different attempt."
                        )
                    self._check_owner(
                        existing["owner_token"],
                        owner_reference,
                        f"execution reservation {existing['reservation_id']}",
                    )
                    result = self._execution_reservation_dict(existing)
                    result["replayed"] = True
                    return result
                conflicting_binding = self._connection.execute(
                    """
                    SELECT 1 FROM operation_idempotency
                    WHERE scope = 'execution.reserve'
                      AND aggregate_type = 'candidate'
                      AND aggregate_id = ?
                      AND request_fingerprint = ?
                    LIMIT 1
                    """,
                    (candidate_id, fingerprint),
                ).fetchone()
                if conflicting_binding is not None:
                    raise IdempotencyConflictError(
                        "Execution request was replayed with a different "
                        "idempotency key."
                    )
            existing = self._connection.execute(
                """
                SELECT * FROM execution_reservations
                WHERE session_id = ? AND request_fingerprint = ?
                """,
                (session_id, fingerprint),
            ).fetchone()
            if existing is not None:
                if (
                    existing["candidate_id"] != candidate_id
                    or int(existing["attempt_count"]) != attempt_count
                ):
                    raise IdempotencyConflictError(
                        "Execution request fingerprint was reused for a different attempt."
                    )
                if owner_reference is not None:
                    self._check_owner(
                        existing["owner_token"],
                        owner_reference,
                        f"execution reservation {existing['reservation_id']}",
                    )
                result = self._execution_reservation_dict(existing)
                result["replayed"] = True
                if hashed_idempotency_key is not None:
                    self._record_idempotency_locked(
                        "execution.reserve",
                        hashed_idempotency_key,
                        "candidate",
                        candidate_id,
                        request_fingerprint=fingerprint,
                    )
                return result
            session = cast(TuningSessionV1, self._get_locked("session", session_id))
            candidate = cast(TuningCandidateV1, self._get_locked("candidate", candidate_id))
            if candidate.session_id != session_id:
                raise ValueError("Candidate does not belong to this tuning session.")
            candidates = self._list_candidates_locked(session_id)
            actual_by_candidate = {
                item.candidate_id: int(item.executions) for item in candidates
            }
            reservation_rows = self._connection.execute(
                """
                SELECT candidate_id, status,
                       COALESCE(
                           SUM(
                               CASE
                                   WHEN status IN ('reserved', 'expired')
                                       THEN attempt_count
                                   ELSE dispatched_attempt_count
                               END
                           ),
                           0
                       ) AS attempts
                FROM execution_reservations
                WHERE session_id = ?
                  AND status IN ('reserved', 'completed', 'released', 'expired')
                GROUP BY candidate_id, status
                """,
                (session_id,),
            ).fetchall()
            reserved = sum(
                int(row["attempts"])
                for row in reservation_rows
                if row["status"] == "reserved"
            )
            finalized_by_candidate: dict[str, int] = {}
            for row in reservation_rows:
                if row["status"] in {"completed", "released", "expired"}:
                    candidate_key = str(row["candidate_id"])
                    finalized_by_candidate[candidate_key] = (
                        finalized_by_candidate.get(candidate_key, 0)
                        + int(row["attempts"])
                    )
            consumed = sum(
                max(
                    actual_by_candidate.get(candidate_key, 0),
                    finalized_by_candidate.get(candidate_key, 0),
                )
                for candidate_key in (
                    set(actual_by_candidate) | set(finalized_by_candidate)
                )
            )
            if consumed + reserved + attempt_count > session.execution_limit:
                raise ReservationError("Execution budget has been reserved.")
            if hashed_idempotency_key is not None:
                self._record_idempotency_locked(
                    "execution.reserve",
                    hashed_idempotency_key,
                    "candidate",
                    candidate_id,
                    request_fingerprint=fingerprint,
                )
            reservation_id = self._new_token("execution")
            self._connection.execute(
                """
                INSERT INTO execution_reservations
                    (reservation_id, session_id, candidate_id, request_fingerprint,
                     attempt_count, dispatched_attempt_count, status,
                     owner_token, reservation_version,
                     expires_at_utc, created_at_utc, updated_at_utc)
                VALUES (?, ?, ?, ?, ?, 0, 'reserved', ?, 0, ?, ?, ?)
                """,
                (
                    reservation_id,
                    session_id,
                    candidate_id,
                    fingerprint,
                    attempt_count,
                    reservation_owner,
                    expires_at,
                    now,
                    now,
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM execution_reservations WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()
            assert row is not None
            result = self._execution_reservation_dict(row)
            result["replayed"] = False
            return result

    def append_performance_case_evidence(
        self,
        case_id: str,
        evidence_id: str,
        *,
        status: str | None = None,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> PerformanceCaseV1:
        """Atomically append one evidence ID to a performance case."""

        if not isinstance(case_id, str) or not case_id.strip():
            raise ValueError("case_id is required.")
        if not isinstance(evidence_id, str) or not evidence_id.strip():
            raise ValueError("evidence_id is required.")
        fingerprint = (
            self._require_fingerprint(request_fingerprint)
            if request_fingerprint is not None
            else None
        )
        scope = "performance_case.evidence.append"
        with self._transaction():
            existing_id = self._idempotent_aggregate_locked(
                scope,
                idempotency_key,
                "performance_case",
                requested_id=case_id,
                request_fingerprint=fingerprint,
            )
            if existing_id is not None:
                return cast(PerformanceCaseV1, self._get_locked("performance_case", existing_id))
            case = cast(PerformanceCaseV1, self._get_locked("performance_case", case_id))
            next_status = status or case.status
            if (
                evidence_id in case.baseline_evidence_ids
                and next_status == case.status
            ):
                updated = case
            else:
                updated = replace(
                    case,
                    baseline_evidence_ids=tuple(
                        dict.fromkeys((*case.baseline_evidence_ids, evidence_id))
                    ),
                    status=next_status,
                    updated_at_utc=utc_now(),
                    version=case.version + 1,
                )
                self._save_locked(
                    "performance_case",
                    updated,
                    insert_only=False,
                    expected_version=case.version,
                )
                self._append_event_locked(
                    event_type="performance_case.evidence_attached",
                    aggregate_type="performance_case",
                    aggregate_id=case_id,
                    payload={"evidence_id": evidence_id},
                    idempotency_key=(
                        f"{scope}:{idempotency_key}" if idempotency_key else None
                    ),
                    request_fingerprint=fingerprint,
                )
            self._record_idempotency_locked(
                scope,
                idempotency_key,
                "performance_case",
                case_id,
                request_fingerprint=fingerprint,
            )
            return updated

    def _next_candidate_ordinal_locked(self, session_id: str) -> int:
        candidate_ordinals = [
            candidate.ordinal for candidate in self._list_candidates_locked(session_id)
        ]
        reservation_row = self._connection.execute(
            """
            SELECT MAX(ordinal) AS max_ordinal
            FROM candidate_slot_reservations
            WHERE session_id = ?
            """,
            (session_id,),
        ).fetchone()
        historical_max = max(candidate_ordinals, default=-1)
        if reservation_row is not None and reservation_row["max_ordinal"] is not None:
            historical_max = max(historical_max, int(reservation_row["max_ordinal"]))
        return historical_max + 1

    def get_execution_reservation(self, reservation_id: str) -> dict[str, Any]:
        with self._transaction():
            self._expire_reservations_locked(utc_now())
            row = self._connection.execute(
                "SELECT * FROM execution_reservations WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()
        if row is None:
            raise ContractNotFoundError(f"Unknown execution reservation: {reservation_id}.")
        return self._execution_reservation_dict(row)

    def get_idempotent_execution_reservation(
        self,
        session_id: str,
        candidate_id: str,
        request_fingerprint: str,
        *,
        owner_reference: str,
        idempotency_key: str | None,
    ) -> dict[str, Any] | None:
        """Read an existing fenced execution request without reserving new work."""

        if not idempotency_key:
            return None
        fingerprint = self._require_fingerprint(request_fingerprint)
        hashed_idempotency_key = self._hashed_idempotency_key(
            "execution.reserve",
            idempotency_key,
        )
        with self._transaction():
            self._expire_reservations_locked(utc_now())
            existing_id = self._idempotent_aggregate_locked(
                "execution.reserve",
                hashed_idempotency_key,
                "candidate",
                requested_id=candidate_id,
                request_fingerprint=fingerprint,
            )
            if existing_id is None:
                conflicting_binding = self._connection.execute(
                    """
                    SELECT 1 FROM operation_idempotency
                    WHERE scope = 'execution.reserve'
                      AND aggregate_type = 'candidate'
                      AND aggregate_id = ?
                      AND request_fingerprint = ?
                    LIMIT 1
                    """,
                    (candidate_id, fingerprint),
                ).fetchone()
                if conflicting_binding is not None:
                    raise IdempotencyConflictError(
                        "Execution request was replayed with a different "
                        "idempotency key."
                    )
                return None
            row = self._connection.execute(
                """
                SELECT * FROM execution_reservations
                WHERE session_id = ? AND candidate_id = ?
                  AND request_fingerprint = ?
                """,
                (session_id, candidate_id, fingerprint),
            ).fetchone()
            if row is None:
                raise PerformanceStoreError(
                    "Execution idempotency binding has no reservation."
                )
            self._check_owner(
                row["owner_token"],
                owner_reference,
                f"execution reservation {row['reservation_id']}",
            )
            result = self._execution_reservation_dict(row)
            result["replayed"] = True
            return result

    def bind_index_benchmark_request(
        self,
        session_id: str,
        candidate_id: str,
        phase: str,
        request_fingerprint: str,
        *,
        idempotency_key: str,
    ) -> dict[str, bool]:
        """Bind one candidate phase to one exact index benchmark request."""

        if phase not in {"screening", "finalist"}:
            raise ValueError("phase must be screening or finalist.")
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise ValueError("idempotency_key is required.")
        fingerprint = self._require_fingerprint(request_fingerprint)
        scope = f"index-benchmark.request.{phase}"
        hashed_idempotency_key = self._hashed_idempotency_key(
            scope,
            idempotency_key,
        )
        with self._transaction():
            candidate = cast(
                TuningCandidateV1,
                self._get_locked("candidate", candidate_id),
            )
            if candidate.session_id != session_id:
                raise ValueError("Candidate does not belong to this tuning session.")

            try:
                existing_id = self._idempotent_aggregate_locked(
                    scope,
                    hashed_idempotency_key,
                    "candidate",
                    requested_id=candidate_id,
                    request_fingerprint=fingerprint,
                )
            except IdempotencyConflictError as exc:
                raise IdempotencyConflictError(
                    f"{_INDEX_BENCHMARK_REPLAY_GUIDANCE} "
                    "The supplied request is a different request."
                ) from exc
            if existing_id is not None:
                return {"replayed": True}

            conflicting_binding = self._connection.execute(
                """
                SELECT request_fingerprint
                FROM operation_idempotency
                WHERE scope = ?
                  AND aggregate_type = 'candidate'
                  AND aggregate_id = ?
                LIMIT 1
                """,
                (scope, candidate_id),
            ).fetchone()
            if conflicting_binding is not None:
                if conflicting_binding["request_fingerprint"] != fingerprint:
                    raise IdempotencyConflictError(
                        f"{_INDEX_BENCHMARK_REPLAY_GUIDANCE} "
                        "The supplied request is a different request."
                    )
                raise IdempotencyConflictError(
                    f"{_INDEX_BENCHMARK_REPLAY_GUIDANCE} "
                    "The supplied key is a different idempotency key."
                )

            self._record_idempotency_locked(
                scope,
                hashed_idempotency_key,
                "candidate",
                candidate_id,
                request_fingerprint=fingerprint,
            )
            return {"replayed": False}

    def execution_budget_usage(self, session_id: str) -> dict[str, int]:
        """Return conservative consumed/reserved execution counts."""

        with self._transaction():
            self._expire_reservations_locked(utc_now())
            session = cast(TuningSessionV1, self._get_locked("session", session_id))
            candidates = self._list_candidates_locked(session_id)
            actual_by_candidate = {
                item.candidate_id: int(item.executions) for item in candidates
            }
            rows = self._connection.execute(
                """
                SELECT candidate_id, status,
                       COALESCE(
                           SUM(
                               CASE
                                   WHEN status IN ('reserved', 'expired')
                                       THEN attempt_count
                                   ELSE dispatched_attempt_count
                               END
                           ),
                           0
                       ) AS attempts
                FROM execution_reservations
                WHERE session_id = ?
                  AND status IN ('reserved', 'completed', 'released', 'expired')
                GROUP BY candidate_id, status
                """,
                (session_id,),
            ).fetchall()
            reserved = sum(
                int(row["attempts"]) for row in rows if row["status"] == "reserved"
            )
            finalized: dict[str, int] = {}
            for row in rows:
                if row["status"] in {"completed", "released", "expired"}:
                    candidate_key = str(row["candidate_id"])
                    finalized[candidate_key] = (
                        finalized.get(candidate_key, 0) + int(row["attempts"])
                    )
            consumed = sum(
                max(
                    actual_by_candidate.get(candidate_id, 0),
                    finalized.get(candidate_id, 0),
                )
                for candidate_id in set(actual_by_candidate) | set(finalized)
            )
        committed = consumed + reserved
        return {
            "execution_limit": session.execution_limit,
            "consumed": consumed,
            "reserved": reserved,
            "committed": committed,
            "remaining": max(0, session.execution_limit - committed),
        }

    def complete_execution_attempts(
        self,
        reservation_id: str,
        *,
        dispatched_attempt_count: int | None = None,
        owner_reference: str | None = None,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        return self._update_execution_reservation(
            reservation_id,
            status="completed",
            dispatched_attempt_count=dispatched_attempt_count,
            owner_reference=owner_reference,
            expected_version=expected_version,
        )

    def release_execution_attempts(
        self,
        reservation_id: str,
        *,
        dispatched_attempt_count: int = 0,
        owner_reference: str | None = None,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        return self._update_execution_reservation(
            reservation_id,
            status="released",
            dispatched_attempt_count=dispatched_attempt_count,
            owner_reference=owner_reference,
            expected_version=expected_version,
        )

    def _update_execution_reservation(
        self,
        reservation_id: str,
        *,
        status: str,
        dispatched_attempt_count: int | None,
        owner_reference: str | None,
        expected_version: int | None,
    ) -> dict[str, Any]:
        if status not in {"completed", "released"}:
            raise ValueError(f"Unsupported execution reservation status: {status!r}.")
        with self._transaction():
            self._expire_reservations_locked(utc_now())
            row = self._connection.execute(
                "SELECT * FROM execution_reservations WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()
            if row is None:
                raise ContractNotFoundError(f"Unknown execution reservation: {reservation_id}.")
            self._check_owner(
                row["owner_token"],
                owner_reference,
                f"execution reservation {reservation_id}",
            )
            current_version = int(row["reservation_version"])
            if expected_version is not None and current_version != expected_version:
                raise ConcurrencyError(
                    f"Execution reservation {reservation_id} is version {current_version}; "
                    f"expected {expected_version}."
                )
            if row["status"] not in {"reserved", "expired"}:
                return self._execution_reservation_dict(row)
            resolved_attempt_count = self._resolve_dispatched_attempt_count_locked(
                row,
                dispatched_attempt_count,
            )
            if row["status"] == "expired" and status == "released":
                # Expiry means dispatch outcome is unknown. A later release
                # cannot erase the conservative full-budget charge.
                return self._execution_reservation_dict(row)
            charged_attempt_count = (
                int(row["attempt_count"])
                if row["status"] == "expired"
                else resolved_attempt_count
            )
            self._connection.execute(
                """
                UPDATE execution_reservations
                SET status = ?, dispatched_attempt_count = ?,
                    reservation_version = reservation_version + 1,
                    updated_at_utc = ?
                WHERE reservation_id = ? AND reservation_version = ?
                """,
                (
                    status,
                    charged_attempt_count,
                    utc_now(),
                    reservation_id,
                    current_version,
                ),
            )
            updated = self._connection.execute(
                "SELECT * FROM execution_reservations WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()
            assert updated is not None
            return self._execution_reservation_dict(updated)

    def _resolve_dispatched_attempt_count_locked(
        self,
        row: sqlite3.Row,
        dispatched_attempt_count: int | None,
    ) -> int:
        requested = int(row["attempt_count"])
        if dispatched_attempt_count is not None:
            if (
                not isinstance(dispatched_attempt_count, int)
                or isinstance(dispatched_attempt_count, bool)
                or not 0 <= dispatched_attempt_count <= requested
            ):
                raise ValueError(
                    "dispatched_attempt_count must be between 0 and the reserved attempt count."
                )
            return dispatched_attempt_count

        candidate = cast(
            TuningCandidateV1,
            self._get_locked("candidate", str(row["candidate_id"])),
        )
        prior_charged = int(
            self._connection.execute(
                """
                SELECT COALESCE(
                    SUM(
                        CASE
                            WHEN status = 'expired' THEN attempt_count
                            ELSE dispatched_attempt_count
                        END
                    ),
                    0
                )
                FROM execution_reservations
                WHERE session_id = ?
                  AND candidate_id = ?
                  AND reservation_id <> ?
                  AND status IN ('completed', 'released', 'expired')
                """,
                (row["session_id"], row["candidate_id"], row["reservation_id"]),
            ).fetchone()[0]
        )
        # The candidate is persisted before its reservation is finalized. The
        # delta therefore covers only this request, while the clamp protects
        # the hard budget from malformed or stale candidate state.
        return min(max(int(candidate.executions) - prior_charged, 0), requested)

    def _list_candidates_locked(self, session_id: str) -> list[TuningCandidateV1]:
        rows = self._connection.execute(
            "SELECT payload FROM tuning_candidates",
        ).fetchall()
        return [
            candidate
            for row in rows
            if (candidate := TuningCandidateV1.from_json(row["payload"])).session_id == session_id
        ]

    @staticmethod
    def _execution_reservation_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "reservation_id": row["reservation_id"],
            "session_id": row["session_id"],
            "candidate_id": row["candidate_id"],
            "request_fingerprint": row["request_fingerprint"],
            "attempt_count": int(row["attempt_count"]),
            "dispatched_attempt_count": int(row["dispatched_attempt_count"]),
            "status": row["status"],
            "version": int(row["reservation_version"]),
            "expires_at_utc": row["expires_at_utc"],
            "created_at_utc": row["created_at_utc"],
            "updated_at_utc": row["updated_at_utc"],
        }

    @staticmethod
    def _reservation_times(
        *,
        max_runtime_seconds: float | None = None,
    ) -> tuple[str, str]:
        now = datetime.now(timezone.utc)
        ttl_seconds = _RESERVATION_TTL.total_seconds()
        if max_runtime_seconds is not None:
            ttl_seconds = max(
                ttl_seconds,
                math.ceil(float(max_runtime_seconds))
                + _RESERVATION_CLEANUP_GRACE_SECONDS,
            )
        return now.isoformat(), (now + timedelta(seconds=ttl_seconds)).isoformat()

    def _expire_reservations_locked(self, now_utc: str) -> None:
        for table_name in (
            "candidate_slot_reservations",
            "execution_reservations",
        ):
            self._connection.execute(
                f"""
                UPDATE {table_name}
                SET status = 'expired',
                    reservation_version = reservation_version + 1,
                    updated_at_utc = ?
                WHERE status = 'reserved'
                  AND (expires_at_utc IS NULL OR expires_at_utc <= ?)
                """,
                (now_utc, now_utc),
            )

    def create_view_change_intent(
        self,
        *,
        change_id: str,
        database_fingerprint: str,
        request_fingerprint: str,
        payload: Mapping[str, Any],
        raw_sql_persistence_authorized: bool,
    ) -> dict[str, Any]:
        """Persist an exact view rollback contract after explicit raw-SQL opt-in."""

        if not raw_sql_persistence_authorized:
            raise PermissionError(
                "Durable view state contains raw SQL and requires explicit authorization."
            )
        if not all(
            isinstance(value, str) and value.strip()
            for value in (change_id, database_fingerprint, request_fingerprint)
        ):
            raise ValueError(
                "change_id, database_fingerprint, and request_fingerprint are required."
            )
        stored_payload = json.loads(
            json.dumps(
                dict(payload),
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        request = stored_payload.get("request")
        idempotency_key_digest: str | None = None
        if isinstance(request, dict):
            idempotency_key = request.get("idempotency_key")
            if idempotency_key is not None:
                if (
                    not isinstance(idempotency_key, str)
                    or not idempotency_key.strip()
                ):
                    raise ValueError(
                        "View change idempotency_key must be a non-empty string."
                    )
                idempotency_key_digest = self._view_idempotency_key_digest(
                    idempotency_key
                )
                request["idempotency_key"] = idempotency_key_digest
        serialized = json.dumps(
            stored_payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        stored_request_fingerprint = hashlib.sha256(
            serialized.encode("utf-8")
        ).hexdigest()
        with self._transaction():
            existing = self._connection.execute(
                "SELECT * FROM view_change_intents WHERE change_id = ?",
                (change_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["database_fingerprint"] != database_fingerprint
                    or existing["request_fingerprint"]
                    != stored_request_fingerprint
                    or existing["idempotency_key_digest"]
                    != idempotency_key_digest
                ):
                    raise IdempotencyConflictError(
                        "Prepared view change identifier was reused for a different request."
                    )
                return self._view_change_intent_dict(existing)
            if idempotency_key_digest is not None:
                duplicate = self._connection.execute(
                    """
                    SELECT change_id
                    FROM view_change_intents
                    WHERE database_fingerprint = ?
                      AND idempotency_key_digest = ?
                    LIMIT 1
                    """,
                    (database_fingerprint, idempotency_key_digest),
                ).fetchone()
                if duplicate is not None:
                    raise IdempotencyConflictError(
                        "View idempotency key is already bound to another "
                        "durable request."
                    )
            now = utc_now()
            self._connection.execute(
                """
                INSERT INTO view_change_intents
                    (change_id, database_fingerprint, idempotency_key_digest,
                     request_fingerprint, status, payload, receipt, intent_version,
                     created_at_utc, updated_at_utc)
                VALUES (?, ?, ?, ?, 'prepared', ?, NULL, 0, ?, ?)
                """,
                (
                    change_id,
                    database_fingerprint,
                    idempotency_key_digest,
                    stored_request_fingerprint,
                    serialized,
                    now,
                    now,
                ),
            )
            created = self._connection.execute(
                "SELECT * FROM view_change_intents WHERE change_id = ?",
                (change_id,),
            ).fetchone()
            assert created is not None
            return self._view_change_intent_dict(created)

    def get_view_change_intent(self, change_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM view_change_intents WHERE change_id = ?",
                (change_id,),
            ).fetchone()
        if row is None:
            raise ContractNotFoundError(f"Unknown view change intent: {change_id}.")
        return self._view_change_intent_dict(row)

    def get_idempotent_view_change_intent(
        self,
        *,
        database_fingerprint: str,
        idempotency_key: str,
    ) -> dict[str, Any] | None:
        """Find the durable view intent bound to one database/idempotency key."""

        if not isinstance(database_fingerprint, str) or not database_fingerprint.strip():
            raise ValueError("database_fingerprint is required.")
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise ValueError("idempotency_key is required.")
        idempotency_key_digest = self._view_idempotency_key_digest(idempotency_key)
        with self._lock:
            matches = self._connection.execute(
                """
                SELECT * FROM view_change_intents
                WHERE database_fingerprint = ?
                  AND idempotency_key_digest = ?
                ORDER BY created_at_utc
                """,
                (database_fingerprint, idempotency_key_digest),
            ).fetchall()
            if len(matches) > 1:
                raise IdempotencyConflictError(
                    "View idempotency key is bound to multiple durable requests."
                )
            return (
                self._view_change_intent_dict(matches[0])
                if matches
                else None
            )

    def update_view_change_intent(
        self,
        change_id: str,
        *,
        status: str,
        expected_version: int,
        raw_sql_persistence_authorized: bool,
        receipt: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not raw_sql_persistence_authorized:
            raise PermissionError(
                "Durable view state contains raw SQL and requires explicit authorization."
            )
        normalized_status = str(status).strip().casefold()
        if normalized_status not in _VIEW_CHANGE_STATUSES:
            raise ValueError(f"Unsupported view change status: {status!r}.")
        with self._transaction():
            current = self._connection.execute(
                "SELECT * FROM view_change_intents WHERE change_id = ?",
                (change_id,),
            ).fetchone()
            if current is None:
                raise ContractNotFoundError(f"Unknown view change intent: {change_id}.")
            current_version = int(current["intent_version"])
            if current_version != expected_version:
                raise ConcurrencyError(
                    f"View change intent {change_id} is version {current_version}; "
                    f"expected {expected_version}."
                )
            serialized_receipt = (
                json.dumps(dict(receipt), sort_keys=True, separators=(",", ":"))
                if receipt is not None
                else current["receipt"]
            )
            self._connection.execute(
                """
                UPDATE view_change_intents
                SET status = ?, receipt = ?,
                    intent_version = intent_version + 1, updated_at_utc = ?
                WHERE change_id = ? AND intent_version = ?
                """,
                (
                    normalized_status,
                    serialized_receipt,
                    utc_now(),
                    change_id,
                    current_version,
                ),
            )
            updated = self._connection.execute(
                "SELECT * FROM view_change_intents WHERE change_id = ?",
                (change_id,),
            ).fetchone()
            assert updated is not None
            return self._view_change_intent_dict(updated)

    @staticmethod
    def _view_change_intent_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "change_id": row["change_id"],
            "database_fingerprint": row["database_fingerprint"],
            "request_fingerprint": row["request_fingerprint"],
            "status": row["status"],
            "payload": json.loads(row["payload"]),
            "receipt": json.loads(row["receipt"]) if row["receipt"] else None,
            "version": int(row["intent_version"]),
            "created_at_utc": row["created_at_utc"],
            "updated_at_utc": row["updated_at_utc"],
        }

    def create_plan_action_intent(
        self,
        intent: PlanActionIntentV1,
        *,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> PlanActionIntentV1:
        return self._create(
            "plan_action_intent",
            intent,
            idempotency_key=idempotency_key,
            event_type="plan_action_intent.created",
            request_fingerprint=request_fingerprint,
        )  # type: ignore[return-value]

    def get_plan_action_intent(self, intent_id: str) -> PlanActionIntentV1:
        return self._get("plan_action_intent", intent_id)  # type: ignore[return-value]

    def save_plan_action_intent(
        self,
        intent: PlanActionIntentV1,
        *,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> PlanActionIntentV1:
        return self._save(
            "plan_action_intent",
            intent,
            expected_version=expected_version,
            idempotency_key=idempotency_key,
            event_type="plan_action_intent.updated",
            event_payload={"contract_type": PlanActionIntentV1.contract_type},
            request_fingerprint=request_fingerprint,
        )  # type: ignore[return-value]

    def _get(self, aggregate_type: str, identifier: str) -> VersionedContract:
        with self._lock:
            return self._get_locked(aggregate_type, identifier)

    def append_event(
        self,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: Mapping[str, Any] | None = None,
        event_id: str | None = None,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        if not event_type or not aggregate_type or not aggregate_id:
            raise ValueError("event_type, aggregate_type, and aggregate_id are required.")
        with self._transaction():
            return self._append_event_locked(
                event_type=event_type,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                payload=payload,
                event_id=event_id,
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
            )

    def list_events(
        self,
        *,
        aggregate_type: str | None = None,
        aggregate_id: str | None = None,
        after_sequence: int = 0,
    ) -> list[dict[str, Any]]:
        clauses = ["sequence > ?"]
        params: list[Any] = [after_sequence]
        if aggregate_type is not None:
            clauses.append("aggregate_type = ?")
            params.append(aggregate_type)
        if aggregate_id is not None:
            clauses.append("aggregate_id = ?")
            params.append(aggregate_id)
        query = (
            "SELECT * FROM tuning_events WHERE "
            + " AND ".join(clauses)
            + " ORDER BY sequence"
        )
        with self._lock:
            rows = self._connection.execute(query, params).fetchall()
        return [self._event_dict(row) for row in rows]

    def create_index_lease(
        self,
        *,
        lease_id: str,
        database_fingerprint: str,
        session_id: str,
        candidate_id: str,
        index_name: str,
        object_fingerprint: str,
        expires_at_utc: str,
        metadata: Mapping[str, Any] | None = None,
        owner_reference: str | None = None,
        fencing_token: str | None = None,
        request_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        """Persist a cleanup lease before a temporary index is created."""

        required = (
            lease_id,
            database_fingerprint,
            session_id,
            candidate_id,
            index_name,
            object_fingerprint,
            expires_at_utc,
        )
        if any(not isinstance(value, str) or not value.strip() for value in required):
            raise ValueError("Index lease identifiers and expiry must be non-empty strings.")
        if (
            owner_reference is not None
            and fencing_token is not None
            and owner_reference != fencing_token
        ):
            raise ValueError(
                "owner_reference and fencing_token must match when both are supplied."
            )
        owner = owner_reference or fencing_token or ""
        fingerprint = (
            self._require_fingerprint(request_fingerprint)
            if request_fingerprint is not None
            else None
        )
        now = utc_now()
        with self._transaction():
            existing = self._connection.execute(
                "SELECT * FROM index_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
            if existing is not None:
                if (
                    fingerprint is not None
                    and existing["request_fingerprint"] is not None
                    and existing["request_fingerprint"] != fingerprint
                ):
                    raise IdempotencyConflictError(
                        f"Index lease {lease_id!r} was replayed with a different request."
                    )
                if owner:
                    self._check_owner(existing["owner_token"], owner, f"index lease {lease_id}")
                return self._index_lease_dict(existing)
            conflict = self._connection.execute(
                """
                SELECT lease_id FROM index_leases
                WHERE database_fingerprint = ?
                  AND status IN ('pending_create', 'active', 'cleanup_pending', 'cleanup_required')
                LIMIT 1
                """,
                (database_fingerprint,),
            ).fetchone()
            if conflict is not None:
                raise LeaseConflictError(
                    f"Database {database_fingerprint!r} already has active lease {conflict['lease_id']}."
                )
            self._connection.execute(
                """
                INSERT INTO index_leases
                    (lease_id, database_fingerprint, session_id, candidate_id,
                     index_name, object_fingerprint, status, expires_at_utc,
                     metadata, owner_token, lease_version, request_fingerprint,
                     created_at_utc, updated_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, 'pending_create', ?, ?, ?, 0, ?, ?, ?)
                """,
                (
                    lease_id,
                    database_fingerprint,
                    session_id,
                    candidate_id,
                    index_name,
                    object_fingerprint,
                    expires_at_utc,
                    self._event_payload(metadata),
                    owner,
                    fingerprint,
                    now,
                    now,
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM index_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
            assert row is not None
            return self._index_lease_dict(row)

    def update_index_lease(
        self,
        lease_id: str,
        *,
        status: str,
        metadata: Mapping[str, Any] | None = None,
        object_fingerprint: str | None = None,
        owner_reference: str | None = None,
        fencing_token: str | None = None,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        allowed = {
            "pending_create",
            "active",
            "cleanup_pending",
            "cleaned",
            "cleanup_required",
            "create_failed",
        }
        if status not in allowed:
            raise ValueError(f"Unsupported index lease status: {status!r}.")
        if (
            owner_reference is not None
            and fencing_token is not None
            and owner_reference != fencing_token
        ):
            raise ValueError(
                "owner_reference and fencing_token must match when both are supplied."
            )
        owner = owner_reference or fencing_token
        with self._transaction():
            existing = self._connection.execute(
                "SELECT * FROM index_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
            if existing is None:
                raise ContractNotFoundError(f"Unknown index lease: {lease_id}.")
            self._check_owner(existing["owner_token"], owner, f"index lease {lease_id}")
            return self._update_index_lease_locked(
                existing,
                status=status,
                metadata=metadata,
                object_fingerprint=object_fingerprint,
                expected_version=expected_version,
            )

    def recover_index_lease(
        self,
        lease_id: str,
        *,
        status: str,
        metadata: Mapping[str, Any] | None = None,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        """Advance expired-lease cleanup without disclosing its fencing secret."""

        if status not in {"cleanup_pending", "cleaned", "cleanup_required"}:
            raise ValueError(f"Unsupported recovery lease status: {status!r}.")
        with self._transaction():
            existing = self._connection.execute(
                "SELECT * FROM index_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
            if existing is None:
                raise ContractNotFoundError(f"Unknown index lease: {lease_id}.")
            return self._update_index_lease_locked(
                existing,
                status=status,
                metadata=metadata,
                object_fingerprint=None,
                expected_version=expected_version,
            )

    def _update_index_lease_locked(
        self,
        existing: sqlite3.Row,
        *,
        status: str,
        metadata: Mapping[str, Any] | None,
        object_fingerprint: str | None,
        expected_version: int | None,
    ) -> dict[str, Any]:
        lease_id = str(existing["lease_id"])
        current_version = int(existing["lease_version"])
        if expected_version is not None and current_version != expected_version:
            raise ConcurrencyError(
                f"Index lease {lease_id} is version {current_version}; "
                f"expected {expected_version}."
            )
        if object_fingerprint is not None and (
            not isinstance(object_fingerprint, str)
            or not object_fingerprint.strip()
        ):
            raise ValueError("object_fingerprint must be a non-empty string.")
        if (
            existing["status"] == status
            and metadata is None
            and object_fingerprint is None
        ):
            return self._index_lease_dict(existing)
        merged = json.loads(existing["metadata"])
        merged.update(redact_metadata(metadata))
        self._connection.execute(
            """
            UPDATE index_leases
            SET status = ?, metadata = ?, object_fingerprint = ?,
                lease_version = lease_version + 1,
                updated_at_utc = ?
            WHERE lease_id = ? AND lease_version = ?
            """,
            (
                status,
                json.dumps(merged, sort_keys=True, separators=(",", ":")),
                object_fingerprint or str(existing["object_fingerprint"]),
                utc_now(),
                lease_id,
                current_version,
            ),
        )
        row = self._connection.execute(
            "SELECT * FROM index_leases WHERE lease_id = ?",
            (lease_id,),
        ).fetchone()
        assert row is not None
        return self._index_lease_dict(row)

    def get_index_lease(self, lease_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM index_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
        if row is None:
            raise ContractNotFoundError(f"Unknown index lease: {lease_id}.")
        return self._index_lease_dict(row)

    def list_open_index_leases(
        self,
        *,
        database_fingerprint: str | None = None,
    ) -> list[dict[str, Any]]:
        open_states = (
            "pending_create",
            "active",
            "cleanup_pending",
            "cleanup_required",
        )
        placeholders = ",".join("?" for _ in open_states)
        query = f"SELECT * FROM index_leases WHERE status IN ({placeholders})"
        params: list[Any] = list(open_states)
        if database_fingerprint is not None:
            query += " AND database_fingerprint = ?"
            params.append(database_fingerprint)
        query += " ORDER BY created_at_utc"
        with self._lock:
            rows = self._connection.execute(query, params).fetchall()
        return [self._index_lease_dict(row) for row in rows]

    @staticmethod
    def _index_lease_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "lease_id": row["lease_id"],
            "database_fingerprint": row["database_fingerprint"],
            "session_id": row["session_id"],
            "candidate_id": row["candidate_id"],
            "index_name": row["index_name"],
            "object_fingerprint": row["object_fingerprint"],
            "status": row["status"],
            "expires_at_utc": row["expires_at_utc"],
            "metadata": json.loads(row["metadata"]),
            "version": int(row["lease_version"]),
            "request_fingerprint": row["request_fingerprint"],
            "created_at_utc": row["created_at_utc"],
            "updated_at_utc": row["updated_at_utc"],
        }

    def save_session_and_candidate(
        self,
        session: TuningSessionV1,
        candidate: TuningCandidateV1,
        *,
        expected_session_version: int | None = None,
        expected_candidate_version: int | None = None,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
        event_type: str = "tuning.transition",
        event_payload: Mapping[str, Any] | None = None,
    ) -> tuple[TuningSessionV1, TuningCandidateV1]:
        """Atomically persist a state transition and its two durable events."""

        if candidate.session_id != session.session_id:
            raise ValueError("Candidate and session identifiers do not match.")
        if idempotency_key and request_fingerprint is None:
            raise ValueError(
                "Idempotent tuning transitions require a request fingerprint."
            )
        scope = "tuning.transition"
        with self._transaction():
            replay = self._replay_session_and_candidate_transition_locked(
                session.session_id,
                candidate.candidate_id,
                idempotency_key,
                request_fingerprint,
            )
            if replay is not None:
                return replay
            self._save_locked(
                "candidate",
                candidate,
                insert_only=False,
                expected_version=expected_candidate_version,
            )
            self._save_locked(
                "session",
                session,
                insert_only=False,
                expected_version=expected_session_version,
            )
            transition_payload = dict(event_payload or {})
            transition_payload.setdefault("candidate_id", candidate.candidate_id)
            self._append_event_locked(
                event_type="candidate." + event_type,
                aggregate_type="candidate",
                aggregate_id=candidate.candidate_id,
                payload=transition_payload,
                idempotency_key=(f"{scope}:{idempotency_key}:candidate" if idempotency_key else None),
                request_fingerprint=request_fingerprint,
            )
            self._append_event_locked(
                event_type="session." + event_type,
                aggregate_type="session",
                aggregate_id=session.session_id,
                payload=transition_payload,
                idempotency_key=(f"{scope}:{idempotency_key}:session" if idempotency_key else None),
                request_fingerprint=request_fingerprint,
            )
            self._record_idempotency_locked(
                scope,
                idempotency_key,
                "session",
                session.session_id,
                request_fingerprint=request_fingerprint,
            )
        return session, candidate

    def replay_session_and_candidate_transition(
        self,
        session_id: str,
        candidate_id: str,
        *,
        idempotency_key: str | None,
        request_fingerprint: str,
    ) -> tuple[TuningSessionV1, TuningCandidateV1] | None:
        """Return an exact durable transition replay without mutating state."""

        if not idempotency_key:
            return None
        with self._transaction():
            return self._replay_session_and_candidate_transition_locked(
                session_id,
                candidate_id,
                idempotency_key,
                request_fingerprint,
            )

    def _replay_session_and_candidate_transition_locked(
        self,
        session_id: str,
        candidate_id: str,
        idempotency_key: str | None,
        request_fingerprint: str | None,
    ) -> tuple[TuningSessionV1, TuningCandidateV1] | None:
        if not idempotency_key:
            return None
        if request_fingerprint is None:
            raise IdempotencyConflictError(
                "Tuning transition replay has no request fingerprint."
            )
        binding = self._connection.execute(
            """
            SELECT aggregate_type, aggregate_id, request_fingerprint
            FROM operation_idempotency
            WHERE scope = 'tuning.transition' AND idempotency_key = ?
            """,
            (idempotency_key,),
        ).fetchone()
        if binding is None:
            return None
        if (
            binding["aggregate_type"] != "session"
            or binding["aggregate_id"] != session_id
        ):
            raise IdempotencyConflictError(
                "Tuning transition idempotency key is bound to another session."
            )
        candidate_event = self._connection.execute(
            """
            SELECT aggregate_type, aggregate_id, request_fingerprint
            FROM tuning_events
            WHERE idempotency_key = ?
            """,
            (f"tuning.transition:{idempotency_key}:candidate",),
        ).fetchone()
        if (
            candidate_event is None
            or candidate_event["aggregate_type"] != "candidate"
            or candidate_event["aggregate_id"] != candidate_id
            or candidate_event["request_fingerprint"] != request_fingerprint
        ):
            raise IdempotencyConflictError(
                "Tuning transition replay does not match the durable candidate event."
            )
        stored_fingerprint = binding["request_fingerprint"]
        if (
            stored_fingerprint is not None
            and stored_fingerprint != request_fingerprint
        ):
            raise IdempotencyConflictError(
                "Tuning transition idempotency key was replayed with a "
                "different request."
            )
        if stored_fingerprint is None:
            self._connection.execute(
                """
                UPDATE operation_idempotency
                SET request_fingerprint = ?
                WHERE scope = 'tuning.transition' AND idempotency_key = ?
                  AND request_fingerprint IS NULL
                """,
                (request_fingerprint, idempotency_key),
            )
        candidate = cast(
            TuningCandidateV1,
            self._get_locked("candidate", candidate_id),
        )
        if candidate.session_id != session_id:
            raise IdempotencyConflictError(
                "Candidate transition replay belongs to another session."
            )
        return (
            cast(TuningSessionV1, self._get_locked("session", session_id)),
            candidate,
        )
