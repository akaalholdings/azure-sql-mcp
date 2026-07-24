from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import pytest

from azure_sql_mcp.connection import AdminBatchOutcomeUnknownError
from azure_sql_mcp.connection import AzureSqlExecutor
from azure_sql_mcp.connection import BatchExecutionMode
from azure_sql_mcp.connection import ProfiledExecution
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.connection import StatementDispatchPrevented


class RowCountCursor:
    def __init__(self, rowcounts: list[int]):
        self._rowcounts = rowcounts
        self._index = 0
        self.rowcount = rowcounts[0]

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        self.query = query
        self.params = params

    def nextset(self) -> bool:
        self._index += 1
        if self._index >= len(self._rowcounts):
            return False
        self.rowcount = self._rowcounts[self._index]
        return True


@pytest.mark.asyncio
async def test_fetch_all_returns_first_tabular_result(sample_server_config) -> None:
    pool = SimpleNamespace(
        acquire=AsyncMock(),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)
    executor.execute_batches = AsyncMock(
        return_value=[
            QueryResult(columns=(), rows=[]),
            QueryResult(columns=("name",), rows=[{"name": "alpha"}]),
            QueryResult(columns=("name",), rows=[{"name": "beta"}]),
        ]
    )

    rows = await executor.fetch_all("appdb", "SELECT name FROM sys.tables")

    assert rows == [{"name": "alpha"}]


@pytest.mark.asyncio
async def test_execute_batches_discards_failed_connections_across_retries(
    sample_server_config,
) -> None:
    connection_one = MagicMock()
    connection_two = MagicMock()
    connection_three = MagicMock()
    pool = SimpleNamespace(
        acquire=AsyncMock(side_effect=[connection_one, connection_two, connection_three]),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    with patch.object(
        executor,
        "_execute_with_connection",
        side_effect=[
            Exception("40501 transient one"),
            Exception("40501 transient two"),
            [QueryResult(columns=("value",), rows=[{"value": 1}])],
        ],
    ) as execute_mock:
        results = await executor.execute_batches("appdb", "SELECT 1")

    assert results == [QueryResult(columns=("value",), rows=[{"value": 1}])]
    assert execute_mock.call_count == 3
    assert pool.discard.await_args_list == [
        call("appdb", connection_one),
        call("appdb", connection_two),
    ]
    pool.release.assert_awaited_once_with("appdb", connection_three)


@pytest.mark.asyncio
async def test_execute_batches_admin_mode_does_not_replay_transient_failure(
    sample_server_config,
) -> None:
    connection = MagicMock()
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    with patch.object(
        executor,
        "_execute_with_connection",
        side_effect=Exception("40501 transient failure"),
    ) as execute_mock:
        with pytest.raises(Exception, match="40501 transient failure"):
            await executor.execute_batches(
                "appdb",
                "CREATE TABLE dbo.canary (id int)",
                execution_mode=BatchExecutionMode.ADMIN,
            )

    execute_mock.assert_called_once()
    assert execute_mock.call_args.args[4] is None
    assert execute_mock.call_args.kwargs == {
        "drain_all_result_sets": True,
        "outcome_unknown_after_dispatch": True,
    }
    pool.discard.assert_awaited_once_with("appdb", connection)
    pool.release.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_batches_admin_mode_discards_successful_connection(
    sample_server_config,
) -> None:
    connection = MagicMock()
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)
    expected = [QueryResult(columns=(), rows=[])]

    with patch.object(
        executor,
        "_execute_with_connection",
        return_value=expected,
    ) as execute_mock:
        result = await executor.execute_batches(
            "appdb",
            "USE tempdb; SET NOCOUNT ON",
            execution_mode=BatchExecutionMode.ADMIN,
        )

    assert result == expected
    execute_mock.assert_called_once()
    pool.discard.assert_awaited_once_with("appdb", connection)
    pool.release.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_batches_admin_cancellation_defers_discard_without_replay(
    sample_server_config,
) -> None:
    connection = MagicMock()
    worker_started = threading.Event()
    worker_release = threading.Event()
    discard_finished = asyncio.Event()

    async def discard(database_name: str, discarded_connection) -> None:
        assert database_name == "appdb"
        assert discarded_connection is connection
        discard_finished.set()

    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(side_effect=discard),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    def blocked_execution(*args, **kwargs) -> list[QueryResult]:
        worker_started.set()
        worker_release.wait(timeout=2)
        return []

    with patch.object(
        executor,
        "_execute_with_connection",
        side_effect=blocked_execution,
    ) as execute_mock:
        task = asyncio.create_task(
            executor.execute_batches(
                "appdb",
                "WAITFOR DELAY '00:00:01'",
                execution_mode=BatchExecutionMode.ADMIN,
            )
        )
        assert await asyncio.to_thread(worker_started.wait, 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        pool.discard.assert_not_awaited()
        worker_release.set()
        await asyncio.wait_for(discard_finished.wait(), timeout=1)

    execute_mock.assert_called_once()
    connection.cancel.assert_called_once()
    pool.discard.assert_awaited_once_with("appdb", connection)
    pool.release.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_non_query_returns_cursor_rowcount(sample_server_config) -> None:
    cursor = RowCountCursor([2, -1, 3, 0])
    connection = MagicMock()
    connection.cursor.return_value = cursor
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    rowcount = await executor.execute_non_query(
        "appdb",
        "UPDATE dbo.items SET enabled = 1",
    )

    assert rowcount == 5
    pool.release.assert_awaited_once_with("appdb", connection)
    pool.discard.assert_not_called()


@pytest.mark.asyncio
async def test_execute_non_query_does_not_retry_after_transient_failure(
    sample_server_config,
) -> None:
    connection = MagicMock()
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    with patch.object(
        executor,
        "_execute_non_query_with_connection",
        side_effect=AdminBatchOutcomeUnknownError("40501 after dispatch"),
    ) as execute_mock:
        with pytest.raises(AdminBatchOutcomeUnknownError, match="40501"):
            await executor.execute_non_query(
                "appdb",
                "ALTER INDEX ALL ON dbo.items REBUILD",
            )

    execute_mock.assert_called_once()
    pool.acquire.assert_awaited_once_with("appdb")
    pool.discard.assert_awaited_once_with("appdb", connection)
    pool.release.assert_not_awaited()


def test_execute_non_query_with_connection_counts_only_positive_rowcounts(
    sample_server_config,
) -> None:
    cursor = RowCountCursor([4, -1, 0, 6])
    connection = MagicMock()
    connection.cursor.return_value = cursor
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    rowcount = executor._execute_non_query_with_connection(
        connection,
        "appdb",
        "DELETE FROM dbo.logs",
        (),
    )

    assert rowcount == 10


def test_execute_non_query_marks_post_dispatch_failure_as_unknown(
    sample_server_config,
) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    cursor = MagicMock()
    cursor.rowcount = 1
    cursor.nextset.side_effect = RuntimeError("40501 after dispatch")
    connection = MagicMock()
    connection.cursor.return_value = cursor

    with pytest.raises(AdminBatchOutcomeUnknownError, match="40501"):
        executor._execute_non_query_with_connection(
            connection,
            "appdb",
            "UPDATE STATISTICS dbo.items",
            (),
        )

    assert cursor.execute.call_args_list == [
        call("SET LOCK_TIMEOUT 30000", ()),
        call("UPDATE STATISTICS dbo.items", ()),
    ]
    cursor.close.assert_called_once()


@pytest.mark.asyncio
async def test_profiled_execution_runs_one_user_query_without_retry(
    sample_server_config,
) -> None:
    connection = MagicMock()
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)
    expected = ProfiledExecution(result_sets=[], elapsed_wall_ms=12.0)

    with patch.object(
        executor,
        "_execute_profiled_with_connection",
        return_value=expected,
    ) as execute_mock:
        result = await executor.execute_profiled_read_only("appdb", "SELECT 1")

    assert result.user_query_executions == 1
    execute_mock.assert_called_once()
    pool.release.assert_awaited_once_with("appdb", connection)
    pool.discard.assert_not_awaited()


@pytest.mark.asyncio
async def test_profiled_execution_failure_is_not_automatically_retried(
    sample_server_config,
) -> None:
    connection = MagicMock()
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    with patch.object(
        executor,
        "_execute_profiled_with_connection",
        side_effect=RuntimeError("synthetic failure"),
    ) as execute_mock:
        with pytest.raises(RuntimeError, match="synthetic failure"):
            await executor.execute_profiled_read_only("appdb", "SELECT 1")

    execute_mock.assert_called_once()
    pool.release.assert_not_awaited()
    pool.discard.assert_awaited_once_with("appdb", connection)


@pytest.mark.asyncio
async def test_exact_session_failure_is_not_automatically_retried(
    sample_server_config,
) -> None:
    connection = MagicMock()
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    with patch.object(
        executor,
        "_execute_session_with_connection",
        side_effect=RuntimeError("synthetic comparison failure"),
    ) as execute_mock:
        with pytest.raises(RuntimeError, match="synthetic comparison failure"):
            await executor.execute_session_exactly_once(
                "appdb",
                ["BEGIN TRANSACTION", "SELECT 1", "ROLLBACK TRANSACTION"],
            )

    execute_mock.assert_called_once()
    pool.release.assert_not_awaited()
    pool.discard.assert_awaited_once_with("appdb", connection)


@pytest.mark.asyncio
async def test_exact_session_statement_hook_runs_in_worker(
    sample_server_config,
) -> None:
    cursors = [MagicMock() for _ in range(3)]
    for cursor in cursors:
        cursor.description = None
        cursor.nextset.return_value = False
    connection = MagicMock()
    connection.cursor.side_effect = cursors
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)
    callback_indices: list[int] = []
    callback_threads: list[int] = []
    main_thread = threading.get_ident()

    def before_dispatch(statement_index: int) -> None:
        callback_indices.append(statement_index)
        callback_threads.append(threading.get_ident())

    await executor.execute_session_exactly_once(
        "appdb",
        ["BEGIN TRANSACTION", "SELECT 1", "ROLLBACK TRANSACTION"],
        before_statement_dispatch=before_dispatch,
    )

    assert callback_indices == [0, 1, 2]
    assert callback_threads
    assert all(thread_id != main_thread for thread_id in callback_threads)
    pool.release.assert_awaited_once_with("appdb", connection)


@pytest.mark.asyncio
async def test_exact_session_hook_prevents_selected_statement_dispatch(
    sample_server_config,
) -> None:
    cursors = [MagicMock() for _ in range(3)]
    for cursor in cursors:
        cursor.description = None
        cursor.nextset.return_value = False
    connection = MagicMock()
    connection.cursor.side_effect = cursors
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    def reject_candidate(statement_index: int) -> None:
        if statement_index == 1:
            raise RuntimeError("deadline expired")

    with pytest.raises(StatementDispatchPrevented) as exc_info:
        await executor.execute_session_exactly_once(
            "appdb",
            ["BEGIN TRANSACTION", "SELECT 1", "ROLLBACK TRANSACTION"],
            before_statement_dispatch=reject_candidate,
        )

    assert exc_info.value.statement_index == 1
    assert isinstance(exc_info.value.cause, RuntimeError)
    cursors[1].execute.assert_not_called()
    pool.release.assert_not_awaited()
    pool.discard.assert_awaited_once_with("appdb", connection)


def test_session_execution_binds_parameters_to_the_matching_statement(
    sample_server_config,
) -> None:
    cursors = [MagicMock() for _ in range(3)]
    for cursor in cursors:
        cursor.description = None
        cursor.nextset.return_value = False
    connection = MagicMock()
    connection.cursor.side_effect = cursors
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    executor._execute_session_with_connection(
        connection,
        "appdb",
        ("SET SHOWPLAN_XML ON", "SELECT ?", "SET SHOWPLAN_XML OFF"),
        max_rows=10,
        statement_params=((), (42,), ()),
    )

    cursors[1].execute.assert_called_once_with("SELECT ?", (42,))
    cursors[2].execute.assert_called_once_with("SET SHOWPLAN_XML OFF")


def test_coerce_value_handles_memoryview_and_bytes(sample_server_config) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    assert executor._coerce_value(memoryview(b"hello")) == "hello"
    assert executor._coerce_value(b"world") == "world"
    assert executor._coerce_value(42) == 42


def test_coerce_value_hex_encodes_non_utf8_binary(sample_server_config) -> None:
    """Genuine binary data (hashes, rowversion) must round-trip losslessly as
    hex instead of being mangled by a lossy utf-8 decode."""
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    assert executor._coerce_value(b"\x00\x01\xff") == "0x0001FF"
    assert executor._coerce_value(memoryview(b"\x8b\xad")) == "0x8BAD"


def test_execute_with_connection_closes_cursor(sample_server_config) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    cursor = MagicMock()
    cursor.description = [("id",)]
    cursor.fetchall.return_value = [(1,)]
    cursor.nextset.return_value = False
    connection = MagicMock()
    connection.cursor.return_value = cursor

    rows = executor._execute_with_connection(connection, "appdb", "SELECT 1", (), max_rows=None)

    assert rows == [QueryResult(columns=("id",), rows=[{"id": 1}])]
    assert cursor.timeout == sample_server_config.query_timeout_seconds
    assert cursor.execute.call_args_list[0] == call("SET LOCK_TIMEOUT 30000", ())
    assert cursor.execute.call_args_list[1] == call("SELECT 1", ())
    cursor.close.assert_called_once()


def test_execute_with_connection_admin_mode_drains_capped_result_sets(
    sample_server_config,
) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    cursor = MagicMock()
    cursor.description = [("id",)]
    cursor.fetchmany.side_effect = [[(1,), (2,)], [(3,), (4,)]]
    cursor.nextset.side_effect = [True, False]
    connection = MagicMock()
    connection.cursor.return_value = cursor

    results = executor._execute_with_connection(
        connection,
        "appdb",
        "SELECT 1; SELECT 2",
        (),
        max_rows=2,
        drain_all_result_sets=True,
    )

    assert results == [
        QueryResult(columns=("id",), rows=[{"id": 1}, {"id": 2}]),
        QueryResult(columns=("id",), rows=[{"id": 3}, {"id": 4}]),
    ]
    assert cursor.fetchmany.call_args_list == [call(2), call(2)]
    assert cursor.nextset.call_count == 2


def test_execute_with_connection_marks_admin_error_after_dispatch_as_unknown(
    sample_server_config,
) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor

    with patch.object(
        executor,
        "_consume_batches",
        side_effect=RuntimeError("result drain failed after commit"),
    ):
        with pytest.raises(
            AdminBatchOutcomeUnknownError, match="result drain failed after commit"
        ):
            executor._execute_with_connection(
                connection,
                "appdb",
                "COMMIT; SELECT 1",
                (),
                outcome_unknown_after_dispatch=True,
            )

    assert cursor.execute.call_count == 2
    cursor.close.assert_called_once()


def test_execute_with_connection_keeps_admin_setup_error_ordinary(
    sample_server_config,
) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("setup failed before dispatch")
    connection = MagicMock()
    connection.cursor.return_value = cursor

    with pytest.raises(RuntimeError, match="setup failed before dispatch") as exc_info:
        executor._execute_with_connection(
            connection,
            "appdb",
            "SELECT 1",
            (),
            outcome_unknown_after_dispatch=True,
        )

    assert not isinstance(exc_info.value, AdminBatchOutcomeUnknownError)
    cursor.execute.assert_called_once()
    cursor.close.assert_called_once()


def test_execute_non_query_with_connection_closes_cursor(sample_server_config) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    cursor = RowCountCursor([1])
    connection = MagicMock()
    connection.cursor.return_value = cursor
    cursor.close = MagicMock()

    rowcount = executor._execute_non_query_with_connection(
        connection,
        "appdb",
        "UPDATE dbo.items SET enabled = 1",
        (),
    )

    assert rowcount == 1
    assert cursor.timeout == sample_server_config.query_timeout_seconds
    cursor.close.assert_called_once()


def test_request_cancel_uses_driver_cancel_when_available(sample_server_config) -> None:
    connection = MagicMock()
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    executor._request_cancel(connection)

    connection.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_discard_connection_when_task_finishes_waits_then_discards(
    sample_server_config,
) -> None:
    connection = MagicMock()
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)
    worker_gate = asyncio.Event()

    async def fake_worker():
        await worker_gate.wait()
        return [QueryResult(columns=("value",), rows=[{"value": 1}])]

    execution_task = asyncio.create_task(fake_worker())
    cleanup_task = asyncio.create_task(
        executor._discard_connection_when_task_finishes(
            "appdb",
            connection,
            execution_task,
        )
    )

    await asyncio.sleep(0)
    pool.discard.assert_not_awaited()
    worker_gate.set()
    await cleanup_task

    pool.discard.assert_awaited_once_with("appdb", connection)


def test_consume_batches_stops_at_cap_without_draining(sample_server_config) -> None:
    """Hitting the row cap must not call nextset(): advancing would drain every
    remaining row over the wire (minutes on a 17.9M-row table); closing the
    cursor discards them protocol-side instead."""
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    cursor = MagicMock()
    cursor.description = [("id",)]
    cursor.fetchmany.return_value = [(i,) for i in range(3)]

    results = executor._consume_batches(cursor, max_rows=3)

    assert len(results) == 1
    assert len(results[0].rows) == 3
    cursor.nextset.assert_not_called()


def test_consume_batches_session_path_keeps_draining_for_plan_xml(sample_server_config) -> None:
    """SHOWPLAN sessions need the result set AFTER the capped data rows —
    stop_on_cap=False keeps advancing."""
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    cursor = MagicMock()
    cursor.description = [("col",)]
    cursor.fetchmany.side_effect = [[("row",)] * 3, [("<ShowPlanXML/>",)]]
    cursor.nextset.side_effect = [True, False]

    results = executor._consume_batches(cursor, max_rows=3, stop_on_cap=False)

    assert len(results) == 2
    assert results[1].rows == [{"col": "<ShowPlanXML/>"}]


def test_consume_batches_captures_driver_type_and_nullability_metadata(
    sample_server_config,
) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    cursor = MagicMock()
    cursor.description = [("id", int, None, 8, 10, 0, False)]
    cursor.fetchall.return_value = [(1,)]
    cursor.nextset.return_value = False

    results = executor._consume_batches(cursor)

    assert results[0].column_type_signatures == (
        "builtins.int|None|8|10|0|False",
    )


def test_consume_batches_retains_duplicate_columns_positionally(sample_server_config) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    cursor = MagicMock()
    cursor.description = [
        ("value", str, None, 20, 0, 0, True),
        ("value", int, None, 8, 10, 0, True),
    ]
    cursor.fetchall.return_value = [("left", 7)]
    cursor.nextset.return_value = False

    result = executor._consume_batches(cursor)[0]

    assert result.rows == [{"value": 7}]
    assert result.positional_rows == (("left", 7),)
    assert result.typed_rows == (("left", 7),)
