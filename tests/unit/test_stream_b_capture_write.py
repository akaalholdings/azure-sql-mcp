"""Capture write path: chunked snapshot inserts and the summary result.

Review findings 67 and 69. One INSERT round trip per subject kept the capture
transaction open for N round trips, and the tool returned every stored subject
after re-reading all of them.
"""

from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import datetime
from datetime import timezone

import pytest

from azure_sql_mcp.connection import TransactionCommitOutcomeUnknownError
from azure_sql_mcp.index_review import CAPTURE_READ_SQL
from azure_sql_mcp.index_review import CONTRACT_PROBE_SQL
from azure_sql_mcp.index_review import INSERT_SNAPSHOT_SQL
from azure_sql_mcp.index_review import CaptureResult
from azure_sql_mcp.index_review import IndexReviewIdempotencyConflictError
from azure_sql_mcp.index_review import IndexReviewIntegrityError
from azure_sql_mcp.index_review import IndexReviewService
from azure_sql_mcp.index_review import SqlIndexHistoryRepository
from azure_sql_mcp.index_review import _snapshot_params
from azure_sql_mcp.index_review import _subject_fingerprint
from tests.unit.test_index_history_repository import _capture
from tests.unit.test_index_history_repository import _context
from tests.unit.test_index_history_repository import _Executor
from tests.unit.test_index_history_repository import _policy
from tests.unit.test_index_history_repository import _RealCaptureExecutor

SNAPSHOT_INSERT = "INSERT INTO dbatools.IndexReviewSnapshot"
SQL_SERVER_PARAMETER_LIMIT = 2_100


def _capture_with_subjects(count: int) -> CaptureResult:
    original = _capture()
    subjects = []
    # Capture order is not subject-id order: the stored order must follow it.
    for number in reversed(range(count)):
        subject = dict(original.snapshot.subjects[0])
        subject["subject_id"] = f"index:100:{number + 2}"
        subject["index_id"] = number + 2
        subject["index_name"] = f"IX_{number}"
        subject["size_pages"] = number
        subject["subject_fingerprint"] = _subject_fingerprint("existing_index", subject)
        subjects.append(subject)
    snapshot = replace(
        original.snapshot,
        subjects=tuple(subjects),
        inventory_fingerprint="",
        snapshot_fingerprint="",
    )
    run = replace(
        original.run,
        subject_count=count,
        inventory_fingerprint=snapshot.inventory_fingerprint,
        snapshot_set_fingerprint=snapshot.snapshot_fingerprint,
        coverage={
            "query_store": {
                "status": "incomplete",
                "eligible": 3,
                "scanned": 3,
                "blockers": ["query_store_stale_threshold_insufficient"],
            },
            "hints": {"status": "complete", "blockers": []},
            "storage": "complete",
            "usage": "partial",
            "gaps": [],
            "malformed": ["query_store_stale_threshold_insufficient"],
        },
    )
    return CaptureResult(run, snapshot)


class _RecordingExecutor(_Executor):
    def __init__(self) -> None:
        super().__init__()
        self.reads: list[str] = []

    async def execute_batches(self, database_name, statement, params=None):
        self.reads.append(statement)
        return await super().execute_batches(database_name, statement, params)


def _repository(executor: _Executor) -> SqlIndexHistoryRepository:
    return SqlIndexHistoryRepository(executor, _policy(allow_write=True))


def _collector(capture: CaptureResult):
    def collector(session, _context):
        session.capture = capture
        return capture

    return collector


# --- 67: chunked inserts ------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("subject_count", [1, 83, 84, 200])
async def test_snapshot_rows_are_inserted_in_chunks_under_the_parameter_limit(subject_count: int) -> None:
    # A 3,000-index database was 3,000 round trips inside the open transaction:
    # about 90 s from a workstation, past the tool timeout, so the capture
    # rolled back and nothing was ever stored.
    parameters_per_row = INSERT_SNAPSHOT_SQL.count("?")
    chunk_rows = (SQL_SERVER_PARAMETER_LIMIT - 1) // parameters_per_row
    assert (parameters_per_row, chunk_rows) == (25, 83)
    executor = _Executor()
    capture = _capture_with_subjects(subject_count)

    await _repository(executor).append_capture(_context(capture), _collector(capture))

    session = executor.sessions[0]
    inserts = [
        (statement, params)
        for statement, params in session.executed
        if statement.lstrip().startswith(SNAPSHOT_INSERT)
    ]
    assert len(inserts) == math.ceil(subject_count / chunk_rows)
    stored_rows: list[list] = []
    for statement, params in inserts:
        assert statement.count("?") == len(params) < SQL_SERVER_PARAMETER_LIMIT
        assert statement.count("OPENJSON") == 0
        assert len(params) % parameters_per_row == 0
        stored_rows.extend(
            params[start : start + parameters_per_row]
            for start in range(0, len(params), parameters_per_row)
        )
    # Same values, same order as one INSERT per subject.
    assert stored_rows == [
        _snapshot_params(capture.run, subject) for subject in capture.snapshot.subjects
    ]
    assert [len(params) // parameters_per_row for _statement, params in inserts][:-1] == [
        chunk_rows
    ] * (len(inserts) - 1)


@pytest.mark.asyncio
async def test_single_subject_capture_sends_the_single_row_statement() -> None:
    executor = _Executor()
    capture = _capture_with_subjects(1)

    await _repository(executor).append_capture(_context(capture), _collector(capture))

    assert executor.sessions[0].statements[-1] == INSERT_SNAPSHOT_SQL


@pytest.mark.asyncio
async def test_chunked_capture_reads_back_as_the_same_history() -> None:
    executor = _Executor()
    capture = _capture_with_subjects(200)

    result = await _repository(executor).append_capture(_context(capture), _collector(capture))

    assert result.snapshot is not None
    assert len(result.snapshot.subjects) == 200
    assert result.run.snapshot_set_fingerprint == capture.run.snapshot_set_fingerprint


# --- 69: summary result and cheap read-back -----------------------------------


@pytest.mark.asyncio
async def test_summary_capture_reads_one_run_row_and_no_snapshot_rows() -> None:
    # The useful result of a daily capture is the run id, the counts and the
    # coverage blockers. Re-reading 2,000 stored subjects to return about 5 MB
    # cost two extra round trips and the agent's context.
    executor = _RecordingExecutor()
    capture = _capture_with_subjects(200)

    result = await _repository(executor).append_capture(
        _context(capture), _collector(capture), read_back_snapshot=False
    )

    assert executor.reads.count(CONTRACT_PROBE_SQL) == 1
    assert CAPTURE_READ_SQL not in executor.reads
    assert len(executor.reads) == 2
    assert "COUNT" in executor.reads[1] and "DefinitionJson" not in executor.reads[1]
    assert result.snapshot is None
    summary = result.as_summary()
    assert summary == {
        "status": "captured",
        "already_captured": False,
        "reconciled": False,
        "run_id": capture.run.run_id,
        "observed_at_utc": capture.run.observed_at_utc,
        "subject_count": 200,
        "snapshot_set_fingerprint": capture.run.snapshot_set_fingerprint,
        "coverage": {
            "complete": False,
            "sources": {
                "hints": "complete",
                "query_store": "incomplete",
                "storage": "complete",
                "usage": "partial",
            },
            "blockers": {"query_store": ["query_store_stale_threshold_insufficient"]},
        },
    }
    assert len(json.dumps(summary)) < 1_000
    with pytest.raises(ValueError, match="as_summary"):
        result.as_dict()


@pytest.mark.asyncio
async def test_full_capture_result_has_the_same_summary_and_keeps_the_snapshot() -> None:
    executor = _Executor()
    capture = _capture_with_subjects(200)
    repository = _repository(executor)

    full = await repository.append_capture(_context(capture), _collector(capture))
    replay = await repository.append_capture(
        _context(capture), _collector(capture), read_back_snapshot=False
    )

    assert len(full.as_dict()["snapshot"]["subjects"]) == 200
    assert len(json.dumps(full.as_dict())) > 100 * len(json.dumps(full.as_summary()))
    assert replay.as_summary() == {
        **full.as_summary(),
        "status": "already_captured",
        "already_captured": True,
    }


@pytest.mark.asyncio
async def test_summary_read_back_fails_when_snapshot_rows_are_missing() -> None:
    # The read-back is the only check that the committed rows are all there.
    executor = _Executor()
    capture = _capture_with_subjects(5)
    committed = executor.execute_transaction_exactly_once

    async def lose_one_row(database_name, callback):
        result = await committed(database_name, callback)
        executor.snapshot_rows.pop()
        return result

    executor.execute_transaction_exactly_once = lose_one_row

    with pytest.raises(IndexReviewIntegrityError, match="subject count does not match"):
        await _repository(executor).append_capture(
            _context(capture), _collector(capture), read_back_snapshot=False
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "column", ["SnapshotSetFingerprint", "InventoryFingerprint", "QueryStoreFingerprint"]
)
async def test_summary_read_back_fails_when_the_stored_run_is_not_what_was_written(column: str) -> None:
    executor = _Executor()
    capture = _capture_with_subjects(5)
    committed = executor.execute_transaction_exactly_once

    async def corrupt_run(database_name, callback):
        result = await committed(database_name, callback)
        executor.run_rows[0][column] = "0" * 64
        return result

    executor.execute_transaction_exactly_once = corrupt_run

    with pytest.raises(IndexReviewIntegrityError):
        await _repository(executor).append_capture(
            _context(capture), _collector(capture), read_back_snapshot=False
        )


@pytest.mark.asyncio
async def test_summary_read_back_fails_when_the_run_row_is_not_readable() -> None:
    executor = _Executor()
    capture = _capture_with_subjects(2)
    committed = executor.execute_transaction_exactly_once

    async def lose_run(database_name, callback):
        result = await committed(database_name, callback)
        executor.run_rows = []
        return result

    executor.execute_transaction_exactly_once = lose_run

    with pytest.raises(IndexReviewIntegrityError, match="not readable after commit"):
        await _repository(executor).append_capture(
            _context(capture), _collector(capture), read_back_snapshot=False
        )


@pytest.mark.asyncio
async def test_summary_replay_with_other_request_material_is_a_conflict() -> None:
    executor = _Executor()
    capture = _capture_with_subjects(2)
    repository = _repository(executor)
    await repository.append_capture(_context(capture), _collector(capture), read_back_snapshot=False)
    # The lock row is what a replay normally conflicts on; without it the
    # read-back is the last place the stored request can be compared.
    executor.lock_rows = []
    executor.run_rows[0]["RequestFingerprint"] = "another-request"

    with pytest.raises(IndexReviewIdempotencyConflictError):
        await repository._read_capture_summary(_context(capture), None)


@pytest.mark.asyncio
async def test_summary_capture_reconciles_an_unknown_commit_outcome() -> None:
    executor = _Executor()
    capture = _capture_with_subjects(3)
    committed = executor.execute_transaction_exactly_once

    async def commit_then_lose_the_reply(database_name, callback):
        await committed(database_name, callback)
        raise TransactionCommitOutcomeUnknownError("connection dropped after COMMIT was sent")

    executor.execute_transaction_exactly_once = commit_then_lose_the_reply

    result = await _repository(executor).append_capture(
        _context(capture), _collector(capture), read_back_snapshot=False
    )

    assert result.as_summary()["status"] == "already_captured"
    assert result.reconciled is True
    assert result.as_summary()["subject_count"] == 3


@pytest.mark.asyncio
async def test_service_capture_can_return_the_summary_from_the_real_collector() -> None:
    executor = _RealCaptureExecutor()
    policy = _policy(allow_write=True)
    service = IndexReviewService(
        executor,
        SqlIndexHistoryRepository(executor, policy),
        database_policy=policy,
        clock=lambda: datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc),
    )

    result = await service.capture_snapshot("appdb", include_snapshot=False)

    summary = result.as_summary()
    assert summary["status"] == "captured"
    assert summary["subject_count"] == 2
    assert summary["coverage"]["sources"]["missing_index_dmv"] == "complete"
    assert "subjects" not in json.dumps(summary)
    history = await service.repository.list_history("appdb")
    assert [run.run_id for run, _snapshot in history] == [summary["run_id"]]
