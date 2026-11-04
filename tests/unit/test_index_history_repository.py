from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from azure_sql_mcp.connection import AzureSqlExecutor
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.index_review import CAPTURE_READ_SQL
from azure_sql_mcp.index_review import CONTRACT_CHECKS
from azure_sql_mcp.index_review import CONTRACT_DEFAULTS
from azure_sql_mcp.index_review import CONTRACT_PROBE_SQL
from azure_sql_mcp.index_review import CaptureContext
from azure_sql_mcp.index_review import CaptureResult
from azure_sql_mcp.index_review import EXISTING_INDEX_METADATA_SQL
from azure_sql_mcp.index_review import IDEMPOTENCY_LOCK_SQL
from azure_sql_mcp.index_review import INSERT_RUN_SQL
from azure_sql_mcp.index_review import INSERT_SNAPSHOT_SQL
from azure_sql_mcp.index_review import IndexReviewCollectionError
from azure_sql_mcp.index_review import IndexReviewIdempotencyConflictError
from azure_sql_mcp.index_review import IndexReviewRunV1
from azure_sql_mcp.index_review import IndexReviewSchemaError
from azure_sql_mcp.index_review import IndexReviewSnapshotV1
from azure_sql_mcp.index_review import SqlIndexHistoryRepository
from azure_sql_mcp.index_review import _digest
from azure_sql_mcp.index_review import _run_params
from azure_sql_mcp.index_review import _snapshot_params
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


READ_COMMITTED_SQL = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"


class _Session:
    def __init__(self, owner):
        self.owner = owner
        self.capture = None
        self.run_params = None
        self.snapshot_params = []
        self.statements = []

    def fetch_all(self, statement, params=None):
        self.statements.append(statement)
        if statement == IDEMPOTENCY_LOCK_SQL:
            return self.owner.lock_rows
        if statement == EXISTING_INDEX_METADATA_SQL:
            return []
        raise AssertionError("unexpected transaction statement")

    def execute(self, statement, params=None):
        self.statements.append(statement)
        if statement == INSERT_RUN_SQL:
            self.run_params = list(params or ())
        elif statement == INSERT_SNAPSHOT_SQL:
            self.snapshot_params.append(list(params or ()))
        elif statement != READ_COMMITTED_SQL:
            raise AssertionError("unexpected transaction statement")


class _Executor:
    def __init__(self, probe_rows=None):
        self.run_rows = []
        self.snapshot_rows = []
        self.lock_rows = []
        self.transaction_calls = 0
        self.sessions = []
        self.probe_rows = probe_rows or _probe_rows()

    async def execute_batches(self, database_name, statement, params=None):
        assert database_name == "appdb"
        if statement == CONTRACT_PROBE_SQL:
            return [_CursorResult(rows) for rows in self.probe_rows]
        if statement == CAPTURE_READ_SQL:
            return [_CursorResult(self.run_rows), _CursorResult(self.snapshot_rows)]
        raise AssertionError("unexpected read statement")

    async def execute_transaction_exactly_once(self, database_name, callback):
        assert database_name == "appdb"
        self.transaction_calls += 1
        session = _Session(self)
        self.sessions.append(session)
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


@pytest.mark.asyncio
async def test_repository_requires_select_for_review_and_insert_for_capture() -> None:
    review_only = _probe_rows()
    for row in review_only[4]:
        row["InsertState"] = 0
    repository = SqlIndexHistoryRepository(
        _Executor(review_only), _policy(allow_write=True)
    )

    probe = await repository.probe_contract("appdb")
    assert probe.allow_read is True
    assert probe.allow_write is False
    with pytest.raises(IndexReviewSchemaError, match="required SELECT and INSERT"):
        await repository.probe_contract("appdb", for_write=True)

    no_read = _probe_rows()
    no_read[4][0]["SelectState"] = 0
    repository = SqlIndexHistoryRepository(
        _Executor(no_read), _policy(allow_write=True)
    )
    with pytest.raises(IndexReviewSchemaError, match="required SELECT permissions"):
        await repository.probe_contract("appdb")


@pytest.mark.asyncio
async def test_repository_accepts_existing_broader_effective_permissions() -> None:
    broad = _probe_rows()
    broad[4][0]["ControlState"] = 1
    repository = SqlIndexHistoryRepository(
        _Executor(broad), _policy(allow_write=True)
    )

    probe = await repository.probe_contract("appdb", for_write=True)

    assert probe.allow_read is True
    assert probe.allow_write is True
    assert probe.dangerous_permissions_absent is False


def _context(capture: CaptureResult) -> CaptureContext:
    return CaptureContext(
        "appdb",
        capture.run.database_fingerprint,
        capture.run.run_id,
        capture.run.idempotency_key_hash,
        capture.run.request_fingerprint,
        capture.run.observed_at_utc,
        35,
    )


@pytest.mark.asyncio
async def test_capture_collects_under_read_committed_after_the_idempotency_lock() -> None:
    # SERIALIZABLE (set by the exactly-once transaction) would hold range
    # locks on catalog, Query Store and module rows for the whole collection.
    # Only the idempotency lock needs it, and HOLDLOCK keeps that lock to
    # commit after the session level drops to READ COMMITTED.
    executor = _Executor()
    repository = SqlIndexHistoryRepository(executor, _policy(allow_write=True))
    capture = _capture()

    def collector(session, _context):
        session.fetch_all(EXISTING_INDEX_METADATA_SQL)
        session.capture = capture
        return capture

    await repository.append_capture(_context(capture), collector)

    statements = executor.sessions[0].statements
    assert statements[:3] == [
        IDEMPOTENCY_LOCK_SQL,
        READ_COMMITTED_SQL,
        EXISTING_INDEX_METADATA_SQL,
    ]
    assert statements[3:] == [INSERT_RUN_SQL, INSERT_SNAPSHOT_SQL]


@pytest.mark.asyncio
async def test_replayed_capture_changes_no_isolation_and_collects_nothing() -> None:
    executor = _Executor()
    repository = SqlIndexHistoryRepository(executor, _policy(allow_write=True))
    capture = _capture()

    def collector(session, _context):
        session.capture = capture
        return capture

    await repository.append_capture(_context(capture), collector)
    await repository.append_capture(_context(capture), collector)

    assert executor.sessions[1].statements == [IDEMPOTENCY_LOCK_SQL]


@pytest.mark.asyncio
async def test_capture_isolation_change_never_reaches_a_pooled_connection(
    sample_server_config,
) -> None:
    # The real exactly-once transaction runs on a fake driver connection. The
    # connection that carried SERIALIZABLE and READ COMMITTED is discarded,
    # never released back to the pool other tools read through.
    connection = MagicMock()
    cursor = MagicMock()
    cursor.description = None
    connection.cursor.return_value = cursor
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = _Executor()
    executor.execute_transaction_exactly_once = AzureSqlExecutor(
        sample_server_config, MagicMock(), pool
    ).execute_transaction_exactly_once
    repository = SqlIndexHistoryRepository(executor, _policy(allow_write=True))
    capture = _capture()

    def collector(session, _context):
        session.fetch_all(EXISTING_INDEX_METADATA_SQL)
        stored = _Session(executor)
        stored.execute(INSERT_RUN_SQL, _run_params(capture.run))
        for subject in capture.snapshot.subjects:
            stored.execute(INSERT_SNAPSHOT_SQL, _snapshot_params(capture.run, subject))
        executor.run_rows, executor.snapshot_rows = _stored_rows(capture, stored)
        return capture

    result = await repository.append_capture(_context(capture), collector)

    assert result.already_captured is False
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[:6] == [
        "SET XACT_ABORT ON",
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "BEGIN TRANSACTION",
        IDEMPOTENCY_LOCK_SQL,
        READ_COMMITTED_SQL,
        EXISTING_INDEX_METADATA_SQL,
    ]
    connection.commit.assert_called_once_with()
    pool.release.assert_not_awaited()
    pool.discard.assert_awaited_once_with("appdb", connection)


@pytest.mark.asyncio
async def test_failed_capture_after_isolation_change_never_reaches_a_pooled_connection(
    sample_server_config,
) -> None:
    # A collector failure after the READ COMMITTED switch rolls back and
    # discards the connection too; no session state goes back to the pool.
    connection = MagicMock()
    cursor = MagicMock()
    cursor.description = None
    connection.cursor.return_value = cursor
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = _Executor()
    executor.execute_transaction_exactly_once = AzureSqlExecutor(
        sample_server_config, MagicMock(), pool
    ).execute_transaction_exactly_once
    repository = SqlIndexHistoryRepository(executor, _policy(allow_write=True))
    capture = _capture()

    def collector(session, _context):
        raise IndexReviewCollectionError("collection failed")

    with pytest.raises(IndexReviewCollectionError):
        await repository.append_capture(_context(capture), collector)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[-2:] == [IDEMPOTENCY_LOCK_SQL, READ_COMMITTED_SQL]
    connection.rollback.assert_called_once_with()
    connection.commit.assert_not_called()
    pool.release.assert_not_awaited()
    pool.discard.assert_awaited_once_with("appdb", connection)


def _stored_rows(capture: CaptureResult, session: _Session):
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
    run_row = dict(zip(run_names, session.run_params, strict=True))
    run_row["CreatedAtUtc"] = capture.run.created_at_utc
    snapshot_names = (
        "SnapshotId", "RunId", "SubjectId", "SubjectKind", "SubjectFingerprint",
        "ObjectId", "IndexId", "SchemaName", "ObjectName", "IndexName",
        "DefinitionJson", "DefinitionFingerprint", "CounterEpochFingerprint",
        "CountersJson", "ObservedAtUtc", "FirstObservedAtUtc", "LastObservedAtUtc",
        "SizePages", "SizeBytes", "WriteBurden", "QueryStoreReferencesJson",
        "ProtectionsJson", "MissingSignatureJson", "AggregatesJson", "CoverageJson",
    )
    snapshot_rows = [
        dict(zip(snapshot_names, values, strict=True))
        for values in session.snapshot_params
    ]
    return [run_row], snapshot_rows


class _RealDatabaseSession(_Session):
    """Realistic catalog, Query Store and DMV rows for the real collector."""

    def fetch_all(self, statement, params=None):
        from azure_sql_mcp.index_review import DATABASE_INCARNATION_SQL
        from azure_sql_mcp.index_review import ENGINE_START_TIME_SQL
        from azure_sql_mcp.index_review import INDEX_PROTECTION_METADATA_SQL
        from azure_sql_mcp.index_review import MISSING_INDEX_DMV_SQL
        from azure_sql_mcp.index_review import QUERY_STORE_WINDOW_SQL
        from azure_sql_mcp.query_store import INDEX_CANDIDATE_ROW_COUNT_SQL
        from azure_sql_mcp.query_store import INDEX_CANDIDATE_WRITE_RATIO_SQL

        self.statements.append(statement)
        if "DATABASEPROPERTYEX(DB_NAME(), 'Edition')" in statement or "geo_replication" in statement:
            # The capture transaction runs with SET XACT_ABORT ON: a failed
            # read here rolls back the idempotency lock and the snapshot.
            self.owner.transaction_doomed = True
            raise RuntimeError("The SELECT permission was denied. (229)")
        if statement == IDEMPOTENCY_LOCK_SQL:
            return self.owner.lock_rows
        if statement == EXISTING_INDEX_METADATA_SQL:
            base = {
                "object_id": 101, "parent_object_type_code": "U",
                "parent_object_type": "USER_TABLE", "schema_name": "Sales",
                "table_name": "Orders", "index_id": 2,
                "index_name": "IX_Orders_CustomerID", "index_type_code": 2,
                "index_type": "NONCLUSTERED", "is_unique": False,
                "is_primary_key": False, "is_unique_constraint": False,
                "is_disabled": False, "is_hypothetical": False,
                "auto_created": False, "has_filter": False,
                "filter_definition": None, "fill_factor": 0,
                "data_space_name": "PRIMARY", "data_space_type": "ROWS_FILEGROUP",
                "partition_number": 1, "partition_row_count": 1000,
                "partition_page_count": 8, "data_compression_desc": "NONE",
                "xml_compression_desc": "OFF", "user_seeks": 0, "user_scans": 0,
                "user_lookups": 0, "user_updates": 25,
                "collected_at_utc": "2026-11-03T10:00:00Z",
            }
            return [{
                **base, "index_column_id": 1, "key_ordinal": 1,
                "is_included_column": False, "is_descending_key": False,
                "partition_ordinal": 0, "column_name": "CustomerID",
            }]
        if statement == ENGINE_START_TIME_SQL:
            return [{
                "engine_start_time_utc": "2026-09-01T00:00:00Z",
                "engine_identity": "srv.database.windows.net",
            }]
        if statement == INDEX_PROTECTION_METADATA_SQL:
            return [{
                "object_id": 101, "index_id": 2, "index_type_code": 2,
                "index_type": "NONCLUSTERED", "is_unique": 0, "is_primary_key": 0,
                "is_unique_constraint": 0, "is_disabled": 0, "is_hypothetical": 0,
                "auto_created": 0, "partition_switch_dependency": 0,
                "is_indexed_view": 0, "has_index_extended_properties": 0,
                "child_foreign_key_ids": None, "referenced_foreign_key_ids": None,
            }]
        if statement == DATABASE_INCARNATION_SQL:
            return [{"database_incarnation_identity": "phys-appdb-1"}]
        if "sys.database_query_store_options" in statement:
            return [{
                "actual_state_desc": "READ_WRITE", "desired_state_desc": "READ_WRITE",
                "query_capture_mode_desc": "ALL", "current_storage_size_mb": 100,
                "max_storage_size_mb": 1000, "stale_query_threshold_days": 60,
            }]
        if statement == QUERY_STORE_WINDOW_SQL:
            return [{
                "window_start_utc": "2026-09-29T10:00:00Z",
                "window_end_utc": "2026-11-03T09:00:00Z",
                "runtime_interval_count": 840,
            }]
        if statement == MISSING_INDEX_DMV_SQL:
            return [{
                "schema_name": "Sales", "table_name": "Orders",
                "equality_columns": "[SalespersonPersonID]",
                "inequality_columns": None, "included_columns": "[OrderDate]",
                "user_seeks": 40, "user_scans": 0, "avg_total_user_cost": 2.5,
                "avg_user_impact": 80.0, "estimated_improvement": 8000.0,
            }]
        if statement == INDEX_CANDIDATE_ROW_COUNT_SQL:
            return [{"row_count": 1000}]
        if "max_length" in statement and "sys.columns" in statement:
            return [{"column_name": name, "max_length": 4} for name in params[3:]]
        if statement == INDEX_CANDIDATE_WRITE_RATIO_SQL:
            return [{"write_ratio": 0.1}]
        if "sys.database_files" in statement:
            return [{"max_size_bytes": 32 * 1024**3, "used_size_bytes": 1024**3}]
        return []


class _RealCaptureExecutor(_Executor):
    config = SimpleNamespace(server="srv.database.windows.net")
    transaction_doomed = False
    edition = "GeneralPurpose"
    geo_links: object = 0

    async def fetch_all(self, database_name, statement, params=None, **_):
        # Pooled reads outside the capture transaction.
        if "DATABASEPROPERTYEX(DB_NAME(), 'Edition')" in statement:
            return [{"edition": self.edition}]
        if "FROM sys.dm_geo_replication_link_status" in statement:
            if isinstance(self.geo_links, Exception):
                raise self.geo_links
            return [{"geo_replication_link_count": self.geo_links}]
        if "sys.geo_replication_links" in statement:
            raise RuntimeError("Invalid object name 'sys.geo_replication_links'. (208)")
        raise AssertionError(f"unexpected read: {statement[:80]}")

    async def execute_batches(self, database_name, statement, params=None):
        from azure_sql_mcp.index_review import HISTORY_READ_SQL

        if statement == HISTORY_READ_SQL:
            return [_CursorResult(self.run_rows), _CursorResult(self.snapshot_rows)]
        return await super().execute_batches(database_name, statement, params)

    async def execute_transaction_exactly_once(self, database_name, callback):
        self.transaction_calls += 1
        session = _RealDatabaseSession(self)
        self.sessions.append(session)
        result = callback(session)
        if session.run_params is not None:
            run_rows, snapshot_rows = _stored_rows(
                SimpleNamespace(run=SimpleNamespace(created_at_utc=session.run_params[13])),
                session,
            )
            self.run_rows, self.snapshot_rows = run_rows, snapshot_rows
            self.lock_rows = [{"RunId": run_rows[0]["RunId"], "RequestFingerprint": run_rows[0]["RequestFingerprint"]}]
        return result


@pytest.mark.asyncio
async def test_real_collector_capture_is_stored_and_read_back_as_history() -> None:
    # The 35-day removal evidence only exists if the real collector output
    # passes the persisted-field allowlists. Every run carries DMV coverage
    # and most real databases have at least one missing-index request, so a
    # rejected key here means no portfolio history is ever stored.
    from datetime import timezone

    from azure_sql_mcp.index_review import IndexReviewService

    executor = _RealCaptureExecutor()
    policy = _policy(allow_write=True)
    repository = SqlIndexHistoryRepository(executor, policy)
    service = IndexReviewService(
        executor,
        repository,
        database_policy=policy,
        clock=lambda: datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc),
    )

    result = await service.capture_snapshot("appdb")

    assert result.already_captured is False
    assert executor.sessions[0].statements.count(INSERT_RUN_SQL) == 1
    kinds = sorted(subject["subject_kind"] for subject in result.snapshot.subjects)
    assert kinds == ["existing_index", "missing_index"]
    assert result.run.coverage["missing_index_dmv"]["status"] == "complete"
    history = await repository.list_history("appdb")
    assert [run.run_id for run, _snapshot in history] == [result.run.run_id]
    missing = next(
        subject
        for subject in history[0][1].subjects
        if subject["subject_kind"] == "missing_index"
    )
    assert missing["aggregates"]["estimated_index_size_mb"] is not None
    review = await service.review_portfolio("appdb")
    assert review.observation.get("history_status") != "missing"
    assert review.as_of_run_id == result.run.run_id
    assert len(review.subjects) == 2


@pytest.mark.asyncio
async def test_missing_history_tables_map_to_precondition_with_installer_step() -> None:
    # The history tables are an optional DBA install. An agent must get
    # result_status=precondition with the installer step to show a DBA, not a
    # raw tool_error it may retry or report as an MCP failure.
    from azure_sql_mcp.index_review import IndexReviewSetupError
    from azure_sql_mcp.index_review import index_history_setup_precondition

    repository = SqlIndexHistoryRepository(
        _Executor([[], [], [], [], []]), _policy(allow_write=True)
    )
    with pytest.raises(IndexReviewSetupError) as raised:
        await repository.list_history("appdb")

    payload = index_history_setup_precondition(raised.value)
    assert payload["result_status"] == "precondition"
    assert "dbatools.IndexReviewRun" in payload["result_status_reason"]
    assert "sql/Install-IndexReviewHistory-v1.sql" in payload["remediation"]
    assert "GRANT SELECT ON dbatools.IndexReviewSnapshot" in payload["remediation"]
    assert "INSERT" not in payload["remediation"]

    capture = index_history_setup_precondition(raised.value, for_write=True)
    assert "GRANT SELECT, INSERT ON dbatools.IndexReviewRun" in capture["remediation"]


def test_history_contract_mismatch_is_not_a_setup_precondition() -> None:
    # A wrong column set means a broken or foreign install; that must stay an
    # error, never be reported as a missing optional setup step.
    from azure_sql_mcp.index_review import IndexReviewSetupError
    from azure_sql_mcp.index_review import validate_contract_probe

    probe = _probe_rows()
    probe[0] = [row for row in probe[0] if row["ColumnName"] != "CoverageJson"]
    with pytest.raises(IndexReviewSchemaError) as raised:
        validate_contract_probe(probe)
    assert not isinstance(raised.value, IndexReviewSetupError)


def _real_capture_service(executor):
    from datetime import timezone

    from azure_sql_mcp.index_review import IndexReviewService

    policy = _policy(allow_write=True)
    return IndexReviewService(
        executor,
        SqlIndexHistoryRepository(executor, policy),
        database_policy=policy,
        clock=lambda: datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc),
    )


def _existing_subject_coverage(result: CaptureResult) -> dict:
    subject = next(
        subject for subject in result.snapshot.subjects if subject["subject_kind"] == "existing_index"
    )
    return subject["coverage"]


@pytest.mark.asyncio
async def test_capture_reads_replica_signals_outside_its_transaction() -> None:
    # The capture transaction holds the idempotency lock under XACT_ABORT ON.
    # An optional replica read that fails inside it would roll that back and
    # let the snapshot inserts run in a new implicit transaction. A General
    # Purpose database with no geo link must also record "none", or the
    # removal gate can never reach drop_candidate on that tier.
    executor = _RealCaptureExecutor()

    result = await _real_capture_service(executor).capture_snapshot("appdb")

    assert executor.transaction_doomed is False
    assert _existing_subject_coverage(result)["readable_replicas"] == "none"
    assert executor.sessions[0].statements.count(INSERT_RUN_SQL) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("edition", "geo_links", "expected"),
    [
        ("BusinessCritical", 0, "possible"),
        ("GeneralPurpose", 1, "possible"),
        ("GeneralPurpose", PermissionError("VIEW DATABASE STATE denied"), "unknown"),
    ],
)
async def test_capture_records_replica_status_without_failing_the_capture(
    edition: str, geo_links: object, expected: str
) -> None:
    executor = _RealCaptureExecutor()
    executor.edition = edition
    executor.geo_links = geo_links

    result = await _real_capture_service(executor).capture_snapshot("appdb")

    assert executor.transaction_doomed is False
    assert _existing_subject_coverage(result)["readable_replicas"] == expected
