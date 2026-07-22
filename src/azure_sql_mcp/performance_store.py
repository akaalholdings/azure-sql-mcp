"""Durable, redacted SQLite persistence for performance contracts.

This module has no database execution capability.  It stores only the
versioned contracts and event metadata supplied by the MCP layer.
"""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import threading
from contextlib import contextmanager
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
    ) -> str | None:
        if not idempotency_key:
            return None
        row = self._connection.execute(
            """
            SELECT aggregate_type, aggregate_id
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
        return str(row["aggregate_id"])

    def _record_idempotency_locked(
        self,
        scope: str,
        idempotency_key: str | None,
        aggregate_type: str,
        aggregate_id: str,
    ) -> None:
        if not idempotency_key:
            return
        try:
            self._connection.execute(
                """
                INSERT INTO operation_idempotency
                    (scope, idempotency_key, aggregate_type, aggregate_id, created_at_utc)
                VALUES (?, ?, ?, ?, ?)
                """,
                (scope, idempotency_key, aggregate_type, aggregate_id, utc_now()),
            )
        except sqlite3.IntegrityError:
            self._idempotent_aggregate_locked(
                scope,
                idempotency_key,
                aggregate_type,
                requested_id=aggregate_id,
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
                 event_type, payload, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                actual_event_id,
                idempotency_key,
                aggregate_type,
                aggregate_id,
                event_type,
                self._event_payload(payload),
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
            "created_at_utc": row["created_at_utc"],
        }

    def _create(
        self,
        aggregate_type: str,
        contract: VersionedContract,
        *,
        idempotency_key: str | None,
        event_type: str,
    ) -> VersionedContract:
        _table, contract_cls, id_field = _CONTRACT_TABLES[aggregate_type]
        identifier = str(getattr(contract, id_field))
        scope = f"{aggregate_type}.create"
        with self._transaction():
            existing_id = self._idempotent_aggregate_locked(
                scope,
                idempotency_key,
                aggregate_type,
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
            )
            self._record_idempotency_locked(
                scope,
                idempotency_key,
                aggregate_type,
                identifier,
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
            )
            self._record_idempotency_locked(
                scope,
                idempotency_key,
                aggregate_type,
                identifier,
            )
        return contract

    def create_evidence(
        self,
        evidence: EvidenceEnvelopeV1,
        *,
        idempotency_key: str | None = None,
    ) -> EvidenceEnvelopeV1:
        return self._create(
            "evidence",
            evidence,
            idempotency_key=idempotency_key,
            event_type="evidence.created",
        )  # type: ignore[return-value]

    def get_evidence(self, evidence_id: str) -> EvidenceEnvelopeV1:
        return self._get("evidence", evidence_id)  # type: ignore[return-value]

    def create_performance_case(
        self,
        performance_case: PerformanceCaseV1,
        *,
        idempotency_key: str | None = None,
    ) -> PerformanceCaseV1:
        return self._create(
            "performance_case",
            performance_case,
            idempotency_key=idempotency_key,
            event_type="performance_case.created",
        )  # type: ignore[return-value]

    def get_performance_case(self, case_id: str) -> PerformanceCaseV1:
        return self._get("performance_case", case_id)  # type: ignore[return-value]

    def save_performance_case(
        self,
        performance_case: PerformanceCaseV1,
        *,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
    ) -> PerformanceCaseV1:
        return self._save(
            "performance_case",
            performance_case,
            expected_version=expected_version,
            idempotency_key=idempotency_key,
            event_type="performance_case.updated",
            event_payload={"contract_type": PerformanceCaseV1.contract_type},
        )  # type: ignore[return-value]

    def create_session(
        self,
        session: TuningSessionV1,
        *,
        idempotency_key: str | None = None,
    ) -> TuningSessionV1:
        return self._create(
            "session",
            session,
            idempotency_key=idempotency_key,
            event_type="session.created",
        )  # type: ignore[return-value]

    def get_session(self, session_id: str) -> TuningSessionV1:
        return self._get("session", session_id)  # type: ignore[return-value]

    def save_session(
        self,
        session: TuningSessionV1,
        *,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
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
        )  # type: ignore[return-value]

    def create_candidate(
        self,
        candidate: TuningCandidateV1,
        *,
        idempotency_key: str | None = None,
    ) -> TuningCandidateV1:
        return self._create(
            "candidate",
            candidate,
            idempotency_key=idempotency_key,
            event_type="candidate.created",
        )  # type: ignore[return-value]

    def get_candidate(self, candidate_id: str) -> TuningCandidateV1:
        return self._get("candidate", candidate_id)  # type: ignore[return-value]

    def save_candidate(
        self,
        candidate: TuningCandidateV1,
        *,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
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

    def create_plan_action_intent(
        self,
        intent: PlanActionIntentV1,
        *,
        idempotency_key: str | None = None,
    ) -> PlanActionIntentV1:
        return self._create(
            "plan_action_intent",
            intent,
            idempotency_key=idempotency_key,
            event_type="plan_action_intent.created",
        )  # type: ignore[return-value]

    def get_plan_action_intent(self, intent_id: str) -> PlanActionIntentV1:
        return self._get("plan_action_intent", intent_id)  # type: ignore[return-value]

    def save_plan_action_intent(
        self,
        intent: PlanActionIntentV1,
        *,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
    ) -> PlanActionIntentV1:
        return self._save(
            "plan_action_intent",
            intent,
            expected_version=expected_version,
            idempotency_key=idempotency_key,
            event_type="plan_action_intent.updated",
            event_payload={"contract_type": PlanActionIntentV1.contract_type},
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
        now = utc_now()
        with self._transaction():
            existing = self._connection.execute(
                "SELECT * FROM index_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
            if existing is not None:
                return self._index_lease_dict(existing)
            self._connection.execute(
                """
                INSERT INTO index_leases
                    (lease_id, database_fingerprint, session_id, candidate_id,
                     index_name, object_fingerprint, status, expires_at_utc,
                     metadata, created_at_utc, updated_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, 'pending_create', ?, ?, ?, ?)
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
        with self._transaction():
            existing = self._connection.execute(
                "SELECT * FROM index_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
            if existing is None:
                raise ContractNotFoundError(f"Unknown index lease: {lease_id}.")
            merged = json.loads(existing["metadata"])
            merged.update(redact_metadata(metadata))
            self._connection.execute(
                """
                UPDATE index_leases
                SET status = ?, metadata = ?, updated_at_utc = ?
                WHERE lease_id = ?
                """,
                (
                    status,
                    json.dumps(merged, sort_keys=True, separators=(",", ":")),
                    utc_now(),
                    lease_id,
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
        event_type: str = "tuning.transition",
        event_payload: Mapping[str, Any] | None = None,
    ) -> tuple[TuningSessionV1, TuningCandidateV1]:
        """Atomically persist a state transition and its two durable events."""

        if candidate.session_id != session.session_id:
            raise ValueError("Candidate and session identifiers do not match.")
        scope = "tuning.transition"
        with self._transaction():
            existing_id = self._idempotent_aggregate_locked(
                scope,
                idempotency_key,
                "session",
                requested_id=session.session_id,
            )
            if existing_id is not None:
                return (
                    cast(TuningSessionV1, self._get_locked("session", existing_id)),
                    cast(
                        TuningCandidateV1,
                        self._get_locked("candidate", candidate.candidate_id),
                    ),
                )
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
            )
            self._append_event_locked(
                event_type="session." + event_type,
                aggregate_type="session",
                aggregate_id=session.session_id,
                payload=transition_payload,
                idempotency_key=(f"{scope}:{idempotency_key}:session" if idempotency_key else None),
            )
            self._record_idempotency_locked(
                scope,
                idempotency_key,
                "session",
                session.session_id,
            )
        return session, candidate
