from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime

import pytest

from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.index_review import CAPTURE_READ_SQL
from azure_sql_mcp.index_review import CONTRACT_CHECKS
from azure_sql_mcp.index_review import CONTRACT_DEFAULTS
from azure_sql_mcp.index_review import CONTRACT_PROBE_SQL
from azure_sql_mcp.index_review import CaptureContext
from azure_sql_mcp.index_review import CaptureResult
from azure_sql_mcp.index_review import IDEMPOTENCY_LOCK_SQL
from azure_sql_mcp.index_review import INSERT_RUN_SQL
from azure_sql_mcp.index_review import INSERT_SNAPSHOT_SQL
from azure_sql_mcp.index_review import IndexReviewIdempotencyConflictError
from azure_sql_mcp.index_review import IndexReviewRunV1
from azure_sql_mcp.index_review import IndexReviewSnapshotV1
from azure_sql_mcp.index_review import SqlIndexHistoryRepository
from azure_sql_mcp.index_review import _digest
from azure_sql_mcp.index_review import _subject_fingerprint
from azure_sql_mcp.index_review import engine_fingerprint


def _probe_rows() -> list[list[dict[str, object]]]:
    from azure_sql_mcp.index_review import RUN_CONTRACT_COLUMNS
    from azure_sql_mcp.index_review import SNAPSHOT_CONTRACT_COLUMNS

    columns = []
    for table, specs in (
        ("IndexReviewRun", RUN_CONTRACT_COLUMNS),
        ("IndexReviewSnapshot", SNAPSHOT_CONTRACT_COLUMNS),
    ):
        columns.extend(
            {
                "TableName": table,
                "ColumnName": name,
                "DataType": data_type,
                "MaxLength": max_length,
                "IsNullable": nullable,
                "PrecisionValue": {"datetime2": 27, "int": 10, "bigint": 19}.get(data_type, 0),
                "ScaleValue": 7 if data_type == "datetime2" else 0,
            }
            for name, data_type, max_length, nullable in specs
        )
    indexes = []
    for table, name, key_columns, primary in (
        ("IndexReviewRun", "PK_IndexReviewRun", ("RunId",), True),
        (
            "IndexReviewRun",
            "UQ_IndexReviewRun_Database_Idempotency",
            ("DatabaseFingerprint", "IdempotencyKeyHash"),
            False,
        ),
        ("IndexReviewSnapshot", "PK_IndexReviewSnapshot", ("SnapshotId",), True),
        (
            "IndexReviewSnapshot",
            "UQ_IndexReviewSnapshot_Run_Subject",
            ("RunId", "SubjectId"),
            False,
        ),
    ):
        indexes.extend(
            {
                "TableName": table,
                "IndexName": name,
                "IsPrimaryKey": primary,
                "IsUnique": True,
                "KeyOrdinal": ordinal,
                "ColumnName": column,
            }
            for ordinal, column in enumerate(key_columns, start=1)
        )
    foreign_keys = [
        {
            "ChildTableName": "IndexReviewSnapshot",
            "ParentTableName": "IndexReviewRun",
            "ColumnOrdinal": 1,
            "ChildColumnName": "RunId",
            "ParentColumnName": "RunId",
        }
    ]
    permissions = [
        {
            "TableName": table,
            "SelectState": 1,
            "InsertState": 1,
            "UpdateState": 0,
            "DeleteState": 0,
            "AlterState": 0,
            "ControlState": 0,
        }
        for table in ("IndexReviewRun", "IndexReviewSnapshot")
    ]
    constraints = [
        {"TableName": table, "ConstraintName": name, "ConstraintType": "DEFAULT", "Definition": definition}
        for table, name, definition in CONTRACT_DEFAULTS
    ] + [
        {"TableName": table, "ConstraintName": name, "ConstraintType": "CHECK", "Definition": definition}
        for table, name, definition in CONTRACT_CHECKS
    ]
    return [columns, indexes, foreign_keys, constraints, permissions]


def _capture() -> CaptureResult:
    subject = {
        "subject_kind": "existing_index",
        "subject_id": "index:100:2",
        "definition": {},
        "definition_fingerprint": _digest("definition", {}),
        "counters": {"user_seeks": 0, "user_scans": 0, "user_lookups": 0, "user_updates": 1},
        "counter_epoch_fingerprint": _digest("epoch", "1"),
        "observed_at_utc": "2026-08-28T10:00:00Z",
        "coverage": {},
        "protections": {},
        "query_store_references": [],
        "missing_signature": None,
        "aggregates": {},
    }
    subject["subject_fingerprint"] = _subject_fingerprint("existing_index", subject)
    snapshot = IndexReviewSnapshotV1(
        run_id="run-1",
        snapshot_id="snapshot-1",
        database_name="appdb",
        database_fingerprint=_digest("database", "1"),
        observed_at_utc="2026-08-28T10:00:00Z",
        counter_epoch_fingerprint=_digest("epoch", "1"),
        engine_fingerprint=_digest("engine", "1"),
        subjects=(subject,),
        query_store={},
        coverage={},
    )
    run = IndexReviewRunV1(
        run_id="run-1",
        database_name="appdb",
        database_fingerprint=_digest("database", "1"),
        idempotency_key_hash=_digest("key-hash", "1"),
        request_fingerprint=_digest("request", "1"),
        observed_at_utc=snapshot.observed_at_utc,
        counter_epoch_fingerprint=_digest("epoch", "1"),
        inventory_fingerprint=snapshot.inventory_fingerprint,
        query_store_fingerprint=_digest("index-review-query-store-v1", {}),
        engine_fingerprint=_digest("engine", "1"),
        subject_count=1,
        snapshot_set_fingerprint=snapshot.snapshot_fingerprint,
        query_store={},
        created_at_utc=snapshot.observed_at_utc,
    )
    return CaptureResult(run, snapshot)


class _CursorResult:
    def __init__(self, rows):
        self.rows = rows


class _Session:
    def __init__(self, owner):
        self.owner = owner
        self.capture = None
        self.run_params = None
        self.snapshot_params = []

    def fetch_all(self, statement, params=None):
        if statement != IDEMPOTENCY_LOCK_SQL:
            raise AssertionError("unexpected transaction statement")
        return self.owner.lock_rows

    def execute(self, statement, params=None):
        if statement == INSERT_RUN_SQL:
            self.run_params = list(params or ())
        elif statement == INSERT_SNAPSHOT_SQL:
            self.snapshot_params.append(list(params or ()))
        else:
            raise AssertionError("unexpected transaction statement")


class _Executor:
    def __init__(self):
        self.run_rows = []
        self.snapshot_rows = []
        self.lock_rows = []
        self.transaction_calls = 0

    async def execute_batches(self, database_name, statement, params=None):
        assert database_name == "appdb"
        if statement == CONTRACT_PROBE_SQL:
            return [_CursorResult(rows) for rows in _probe_rows()]
        if statement == CAPTURE_READ_SQL:
            return [_CursorResult(self.run_rows), _CursorResult(self.snapshot_rows)]
        raise AssertionError("unexpected read statement")

    async def execute_transaction_exactly_once(self, database_name, callback):
        assert database_name == "appdb"
        self.transaction_calls += 1
        session = _Session(self)
        result = callback(session)
        if session.capture is not None:
            capture = session.capture
            run = capture.run
            run_values = session.run_params
            run_names = (
                "RunId", "ContractVersion", "SchemaVersion", "CollectorVersion",
                "DatabaseName", "DatabaseFingerprint", "DatabaseIncarnationFingerprint",
                "DatabaseIncarnationIdentity", "EngineFingerprint", "EngineIdentity",
                "EngineStartTimeUtc", "IdempotencyKeyHash",
                "RequestFingerprint", "ObservedAtUtc", "CounterEpochFingerprint",
                "InventoryFingerprint", "QueryStoreFingerprint", "QueryStoreState",
                "QueryCaptureMode", "ObservationStartUtc", "ObservationEndUtc",
                "CoverageJson", "SubjectCount", "SnapshotSetFingerprint", "QueryStoreJson",
            )
            row = dict(zip(run_names, run_values, strict=True))
            row["CreatedAtUtc"] = run.created_at_utc
            self.run_rows = [row]
            self.lock_rows = [{"RunId": run.run_id, "RequestFingerprint": run.request_fingerprint}]
            snapshot_names = (
                "SnapshotId", "RunId", "SubjectId", "SubjectKind", "SubjectFingerprint",
                "ObjectId", "IndexId", "SchemaName", "ObjectName", "IndexName",
                "DefinitionJson", "DefinitionFingerprint", "CounterEpochFingerprint",
                "CountersJson", "ObservedAtUtc", "FirstObservedAtUtc", "LastObservedAtUtc",
                "SizePages", "SizeBytes", "WriteBurden", "QueryStoreReferencesJson",
                "ProtectionsJson", "MissingSignatureJson", "AggregatesJson", "CoverageJson",
            )
            self.snapshot_rows = [
                dict(zip(snapshot_names, values, strict=True))
                for values in session.snapshot_params
            ]
        return result


@pytest.mark.asyncio
async def test_repository_is_idempotent_and_rejects_conflicting_request() -> None:
    executor = _Executor()
    repository = SqlIndexHistoryRepository(executor, _policy(allow_write=True))
    capture = _capture()
    context = CaptureContext(
        "appdb",
        capture.run.database_fingerprint,
        "run-1",
        capture.run.idempotency_key_hash,
        capture.run.request_fingerprint,
        capture.run.observed_at_utc,
        90,
    )
    collector_calls = 0

    def collector(session, _context):
        nonlocal collector_calls
        collector_calls += 1
        session.capture = capture
        return capture

    first = await repository.append_capture(context, collector)
    second = await repository.append_capture(context, collector)
    assert first.already_captured is False
    assert second.already_captured is True
    assert collector_calls == 1
    assert executor.transaction_calls == 2

    conflict = CaptureContext(
        "appdb",
        capture.run.database_fingerprint,
        "run-1",
        capture.run.idempotency_key_hash,
        "different-request",
        capture.run.observed_at_utc,
        90,
    )
    with pytest.raises(IndexReviewIdempotencyConflictError):
        await repository.append_capture(conflict, collector)
    assert collector_calls == 1


@pytest.mark.asyncio
async def test_repository_round_trip_normalizes_driver_datetimes_for_json() -> None:
    executor = _Executor()
    repository = SqlIndexHistoryRepository(executor, _policy(allow_write=True))
    capture = _capture()
    context = CaptureContext(
        "appdb",
        capture.run.database_fingerprint,
        "run-1",
        capture.run.idempotency_key_hash,
        capture.run.request_fingerprint,
        capture.run.observed_at_utc,
        90,
    )

    def collector(session, _context):
        session.capture = capture
        return capture

    await repository.append_capture(context, collector)
    observed = datetime(2026, 8, 28, 10, 0, 0)
    engine_started = datetime(2026, 8, 1, 8, 30, 0)
    executor.run_rows[0].update(
        {
            "ObservedAtUtc": observed,
            "CreatedAtUtc": observed,
            "ObservationStartUtc": datetime(2026, 5, 30, 10, 0, 0),
            "ObservationEndUtc": observed,
            "EngineIdentity": "azure-sql-database",
            "EngineStartTimeUtc": engine_started,
            "EngineFingerprint": engine_fingerprint(
                "azure-sql-database", engine_started
            ),
        }
    )
    executor.snapshot_rows[0]["ObservedAtUtc"] = observed

    restored = await repository.get_capture_by_idempotency(
        "appdb",
        capture.run.idempotency_key_hash,
        request_fingerprint=capture.run.request_fingerprint,
        database_fingerprint_value=capture.run.database_fingerprint,
    )

    assert restored is not None
    assert restored.run.engine_start_time_utc == "2026-08-01T08:30:00Z"
    assert restored.run.observation_start_utc == "2026-05-30T10:00:00Z"
    assert restored.run.observation_end_utc == "2026-08-28T10:00:00Z"
    assert restored.run.created_at_utc == "2026-08-28T10:00:00Z"
    assert restored.snapshot.engine_start_time_utc == "2026-08-01T08:30:00Z"
    assert restored.snapshot.subjects[0]["observed_at_utc"] == "2026-08-28T10:00:00Z"
    json.dumps(restored.as_dict())


@pytest.mark.asyncio
async def test_repository_round_trip_preserves_dmv_only_source_marker() -> None:
    executor = _Executor()
    repository = SqlIndexHistoryRepository(executor, _policy(allow_write=True))
    original = _capture()
    subject = dict(original.snapshot.subjects[0])
    subject["dmv_only"] = True
    subject["aggregates"] = {"dmv_only": True}
    subject["subject_fingerprint"] = _subject_fingerprint(
        str(subject["subject_kind"]), subject
    )
    snapshot = replace(
        original.snapshot,
        subjects=(subject,),
        inventory_fingerprint="",
        snapshot_fingerprint="",
    )
    run = replace(
        original.run,
        inventory_fingerprint=snapshot.inventory_fingerprint,
        snapshot_set_fingerprint=snapshot.snapshot_fingerprint,
    )
    capture = CaptureResult(run, snapshot)
    context = CaptureContext(
        "appdb",
        run.database_fingerprint,
        run.run_id,
        run.idempotency_key_hash,
        run.request_fingerprint,
        run.observed_at_utc,
        90,
    )

    def collector(session, _context):
        session.capture = capture
        return capture

    await repository.append_capture(context, collector)
    restored = await repository.get_capture_by_idempotency(
        "appdb",
        run.idempotency_key_hash,
        request_fingerprint=run.request_fingerprint,
        database_fingerprint_value=run.database_fingerprint,
    )

    assert restored is not None
    restored_subject = restored.snapshot.subjects[0]
    assert restored_subject["dmv_only"] is True
    assert restored_subject["aggregates"]["dmv_only"] is True


def _policy(*, allow_write: bool) -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_index_history_write": allow_write,
                }
            },
        }
    )
