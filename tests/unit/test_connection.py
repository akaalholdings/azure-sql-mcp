from __future__ import annotations

import asyncio
import logging
import threading
import time
import warnings
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import pytest
from mssql_python import SQL_WVARCHAR

from azure_sql_mcp.connection import AdminBatchOutcomeUnknownError
from azure_sql_mcp.connection import AzureSqlExecutor
from azure_sql_mcp.connection import BatchExecutionMode
from azure_sql_mcp.connection import ProfiledExecution
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.connection import StatementDispatchPrevented
from azure_sql_mcp.connection import StatementDispatchStopped
from azure_sql_mcp.connection import TransactionCommitOutcomeUnknownError
from azure_sql_mcp.connection import TransactionRollbackError
from azure_sql_mcp.connection import statement_deadline


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
    kwargs = dict(execute_mock.call_args.kwargs)
    assert isinstance(kwargs.pop("cancel_event"), threading.Event)
    assert kwargs == {
        "drain_all_result_sets": True,
        "outcome_unknown_after_dispatch": True,
        "lock_timeout_in_batch": False,
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


@pytest.mark.asyncio
async def test_transaction_exactly_once_uses_one_connection_and_discards_it(
    sample_server_config,
) -> None:
    connection = MagicMock()
    cursor = MagicMock()
    cursor.description = [("value",)]
    cursor.fetchall.return_value = [(1,)]
    connection.cursor.return_value = cursor
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    def callback(session):
        assert session.fetch_all("SELECT 1") == [{"value": 1}]
        session.execute("INSERT INTO dbatools.IndexReviewRun VALUES (?)", ("run",))
        return "ok"

    with patch("azure_sql_mcp.connection.with_retry", side_effect=AssertionError("retry used")):
        result = await executor.execute_transaction_exactly_once("appdb", callback)

    assert result == "ok"
    pool.acquire.assert_awaited_once_with("appdb")
    pool.release.assert_not_awaited()
    pool.discard.assert_awaited_once_with("appdb", connection)
    assert connection.cursor.call_count == 3
    assert [call.args[0] for call in cursor.execute.call_args_list[:2]] == [
        "SET XACT_ABORT ON",
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    ]
    assert connection.autocommit is False
    connection.commit.assert_called_once_with()
    connection.rollback.assert_not_called()


class _SqlServerTransactionConnection:
    """ODBC transaction semantics of SQL Server, as seen live on Azure SQL Database.

    With autocommit off the driver opens an implicit transaction (@@TRANCOUNT 1).
    A further BEGIN TRANSACTION nests it (@@TRANCOUNT 2). commit() ends one
    level only; rows are durable when @@TRANCOUNT reaches 0. A connection that
    is discarded with an open transaction rolls back.
    """

    def __init__(self) -> None:
        self.autocommit = True
        self.trancount = 0
        self.pending: list[Any] = []
        self.durable: list[Any] = []

    def cursor(self) -> Any:
        connection = self

        class _Cursor:
            description = None
            rowcount = 1

            def execute(self, sql: str, params: Any = None) -> None:
                if not connection.autocommit and connection.trancount == 0:
                    connection.trancount = 1
                if sql.strip().upper().startswith("BEGIN TRAN"):
                    connection.trancount += 1
                elif sql.strip().upper().startswith("INSERT"):
                    connection.pending.append(params)

            def fetchall(self) -> list[Any]:
                return []

            def nextset(self) -> bool:
                return False

            def close(self) -> None:
                pass

        return _Cursor()

    def commit(self) -> None:
        if self.trancount > 0:
            self.trancount -= 1
        if self.trancount == 0:
            self.durable.extend(self.pending)
            self.pending = []

    def rollback(self) -> None:
        self.trancount = 0
        self.pending = []

    def close(self) -> None:
        self.rollback()


@pytest.mark.asyncio
async def test_transaction_exactly_once_commit_is_durable_with_sql_server_semantics(
    sample_server_config,
) -> None:
    # Live repro (2026-11-04, Azure SQL Database): @@TRANCOUNT was 2 inside the
    # callback and the inserted row was gone after the call, so index history
    # capture could never store a run.
    connection = _SqlServerTransactionConnection()

    async def discard(_database: str, conn: Any) -> None:
        conn.close()

    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(side_effect=discard),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    def callback(session):
        session.execute("INSERT INTO dbatools.IndexReviewRun VALUES (?)", ("run",))
        return connection.trancount

    trancount_inside = await executor.execute_transaction_exactly_once("appdb", callback)

    assert trancount_inside == 1
    assert connection.durable == [("run",)]


def test_transaction_precommit_failure_rolls_back_and_does_not_commit(
    sample_server_config,
) -> None:
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value = cursor
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    def callback(session):
        session.execute("INSERT INTO dbatools.IndexReviewRun VALUES (?)", ("run",))
        raise RuntimeError("second subject failed")

    with pytest.raises(RuntimeError, match="second subject failed"):
        executor._execute_transaction_with_connection(connection, "appdb", callback)

    connection.rollback.assert_called_once_with()
    connection.commit.assert_not_called()


def test_transaction_rollback_failure_is_explicit(sample_server_config) -> None:
    connection = MagicMock()
    connection.cursor.return_value = MagicMock()
    connection.rollback.side_effect = OSError("rollback unavailable")
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    with pytest.raises(TransactionRollbackError):
        executor._execute_transaction_with_connection(
            connection,
            "appdb",
            lambda _session: (_ for _ in ()).throw(RuntimeError("callback failed")),
        )


def test_transaction_commit_dispatch_uncertainty_is_not_retried(sample_server_config) -> None:
    connection = MagicMock()
    connection.cursor.return_value = MagicMock()
    connection.commit.side_effect = OSError("connection dropped after commit dispatch")
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    with pytest.raises(TransactionCommitOutcomeUnknownError):
        executor._execute_transaction_with_connection(
            connection,
            "appdb",
            lambda _session: "committed-or-unknown",
        )

    connection.commit.assert_called_once_with()
    connection.rollback.assert_not_called()


@pytest.mark.asyncio
async def test_transaction_cancellation_defers_discard_until_worker_finishes(
    sample_server_config,
) -> None:
    connection = MagicMock()
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)
    worker_started = threading.Event()
    worker_release = threading.Event()

    def blocked_callback(_session):
        worker_started.set()
        worker_release.wait(timeout=2)
        return "done"

    task = asyncio.create_task(
        executor.execute_transaction_exactly_once("appdb", blocked_callback)
    )
    assert await asyncio.to_thread(worker_started.wait, 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    pool.discard.assert_not_awaited()
    connection.cancel.assert_called_once_with()
    worker_release.set()
    for _ in range(20):
        if pool.discard.await_count:
            break
        await asyncio.sleep(0.01)
    pool.discard.assert_awaited_once_with("appdb", connection)


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


def test_profiled_execution_captures_messages_before_each_nextset(
    sample_server_config,
) -> None:
    setup_cursor = MagicMock()
    query_cursor = MagicMock()
    query_cursor.description = [("value",)]
    query_cursor.fetchall.side_effect = [[(1,)], [(2,)]]
    first_message = (0, "Table 'Orders'. logical reads 12.")
    second_message = (0, "Table 'Worktable'. logical reads 3.")
    query_cursor.messages = [first_message]
    nextset_index = 0

    def advance_result_set() -> bool:
        nonlocal nextset_index
        nextset_index += 1
        if nextset_index == 1:
            query_cursor.messages.append(second_message)
            return True
        query_cursor.messages = []
        return False

    query_cursor.nextset.side_effect = advance_result_set
    teardown_cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.side_effect = [setup_cursor, query_cursor, teardown_cursor]
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    result = executor._execute_profiled_with_connection(
        connection,
        "appdb",
        "SELECT 1",
        (),
        max_rows=None,
    )

    assert result.statistics_io_messages == (first_message, second_message)
    assert result.metric_provenance == (
        "client_wall_clock_statistics_xml_and_statistics_io"
    )


def test_profiled_execution_applies_control_sizes_to_query_cursor_only(
    sample_server_config,
) -> None:
    setup_cursor = MagicMock()
    query_cursor = MagicMock()
    teardown_cursor = MagicMock()
    for cursor in (setup_cursor, query_cursor, teardown_cursor):
        cursor.description = None
        cursor.nextset.return_value = False
    connection = MagicMock()
    connection.cursor.side_effect = [setup_cursor, query_cursor, teardown_cursor]
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    input_sizes = (SQL_WVARCHAR, SQL_WVARCHAR)
    params = ("SELECT 1", "", 42)

    executor._execute_profiled_with_connection(
        connection,
        "appdb",
        "EXEC sys.sp_executesql ?, ?, ?",
        params,
        max_rows=None,
        input_sizes=input_sizes,
    )

    setup_cursor.setinputsizes.assert_not_called()
    query_cursor.setinputsizes.assert_called_once_with(input_sizes)
    query_cursor.execute.assert_called_once_with(
        "EXEC sys.sp_executesql ?, ?, ?",
        params,
    )
    teardown_cursor.setinputsizes.assert_not_called()


def test_short_input_size_warning_is_filtered_without_hiding_other_warnings(
    sample_server_config,
) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    cursor = MagicMock()
    expected_warning = (
        "Number of input sizes (2) does not match number of parameters (3). "
        "This may lead to unexpected behavior."
    )

    def emit_expected_warning(statement, params):
        warnings.warn(expected_warning, Warning)
        return cursor

    cursor.execute.side_effect = emit_expected_warning
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        executor._execute_statement(
            cursor,
            "EXEC sys.sp_executesql ?, ?, ?",
            ("SELECT 1", "", 42),
            input_sizes=(SQL_WVARCHAR, SQL_WVARCHAR),
        )
    assert captured == []

    cursor.execute.side_effect = lambda statement, params: warnings.warn(
        "unrelated warning", UserWarning
    )
    with pytest.warns(UserWarning, match="unrelated warning"):
        executor._execute_statement(
            cursor,
            "EXEC sys.sp_executesql ?, ?, ?",
            ("SELECT 1", "", 42),
            input_sizes=(SQL_WVARCHAR, SQL_WVARCHAR),
        )


def test_profiled_execution_preserves_repeated_messages_after_buffer_reset(
    sample_server_config,
) -> None:
    setup_cursor = MagicMock()
    query_cursor = MagicMock()
    query_cursor.description = [("value",)]
    query_cursor.fetchall.side_effect = [[(1,)], [(2,)]]
    repeated_message = (0, "Table 'Orders'. logical reads 12.")
    additional_message = (0, "Table 'Worktable'. logical reads 3.")
    query_cursor.messages = [repeated_message]
    nextset_index = 0

    def advance_result_set() -> bool:
        nonlocal nextset_index
        nextset_index += 1
        if nextset_index == 1:
            query_cursor.messages.extend([repeated_message, additional_message])
            return True
        query_cursor.messages.clear()
        return False

    query_cursor.nextset.side_effect = advance_result_set
    teardown_cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.side_effect = [setup_cursor, query_cursor, teardown_cursor]
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    result = executor._execute_profiled_with_connection(
        connection,
        "appdb",
        "SELECT 1",
        (),
        max_rows=None,
    )

    assert result.statistics_io_messages == (
        repeated_message,
        repeated_message,
        additional_message,
    )


def test_profiled_execution_preserves_identical_immutable_messages_across_result_sets(
    sample_server_config,
) -> None:
    setup_cursor = MagicMock()
    query_cursor = MagicMock()
    query_cursor.description = [("value",)]
    query_cursor.fetchall.side_effect = [[(1,)], [(2,)]]
    repeated_message = (0, "Table 'Orders'. logical reads 12.")
    query_cursor.messages = (repeated_message,)
    nextset_index = 0

    def advance_result_set() -> bool:
        nonlocal nextset_index
        nextset_index += 1
        if nextset_index == 1:
            query_cursor.messages = tuple([repeated_message])
            return True
        query_cursor.messages = ()
        return False

    query_cursor.nextset.side_effect = advance_result_set
    teardown_cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.side_effect = [setup_cursor, query_cursor, teardown_cursor]
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    result = executor._execute_profiled_with_connection(
        connection,
        "appdb",
        "SELECT 1",
        (),
        max_rows=None,
    )

    assert result.statistics_io_messages == (
        repeated_message,
        repeated_message,
    )


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
@pytest.mark.parametrize(
    "method_name",
    ("execute_session", "execute_session_exactly_once"),
)
async def test_session_entrypoints_propagate_statement_input_sizes(
    sample_server_config,
    method_name,
) -> None:
    connection = MagicMock()
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(),
    )
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)
    input_sizes = (SQL_WVARCHAR, SQL_WVARCHAR)

    with patch.object(
        executor,
        "_execute_session_with_connection",
        return_value=[],
    ) as execute_mock:
        result = await getattr(executor, method_name)(
            "appdb",
            ["SET SHOWPLAN_XML ON", "EXEC sys.sp_executesql ?, ?, ?", "SET SHOWPLAN_XML OFF"],
            statement_params=[None, ("SELECT 1", "", 42), None],
            statement_input_sizes=[None, input_sizes, None],
        )

    assert result == []
    assert execute_mock.call_args.kwargs["statement_input_sizes"] == (
        None,
        input_sizes,
        None,
    )


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


def test_exact_session_stops_before_queries_when_snapshot_guard_fails(
    sample_server_config,
) -> None:
    setup_cursor = MagicMock()
    guard_cursor = MagicMock()
    baseline_cursor = MagicMock()
    candidate_cursor = MagicMock()
    for cursor in (setup_cursor, guard_cursor, baseline_cursor, candidate_cursor):
        cursor.description = None
        cursor.nextset.return_value = False
    guard_cursor.execute.side_effect = RuntimeError("snapshot guard failed")
    connection = MagicMock()
    connection.cursor.side_effect = [
        setup_cursor,
        guard_cursor,
        baseline_cursor,
        candidate_cursor,
    ]
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    with pytest.raises(RuntimeError, match="snapshot guard failed"):
        executor._execute_session_with_connection(
            connection,
            "appdb",
            [
                "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
                "BEGIN TRANSACTION; THROW 51000, 'guard', 1;",
                "SELECT 1",
                "SELECT 1",
            ],
            max_rows=2,
        )

    assert connection.cursor.call_count == 2
    baseline_cursor.execute.assert_not_called()
    candidate_cursor.execute.assert_not_called()


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


def test_session_execution_applies_control_sizes_to_matching_statement_cursor(
    sample_server_config,
) -> None:
    cursors = [MagicMock() for _ in range(3)]
    for cursor in cursors:
        cursor.description = None
        cursor.nextset.return_value = False
    connection = MagicMock()
    connection.cursor.side_effect = cursors
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    input_sizes = (SQL_WVARCHAR, SQL_WVARCHAR)

    executor._execute_session_with_connection(
        connection,
        "appdb",
        ("SET SHOWPLAN_XML ON", "EXEC sys.sp_executesql ?, ?, ?", "SET SHOWPLAN_XML OFF"),
        max_rows=10,
        statement_params=((), ("SELECT 1", "", 42), ()),
        statement_input_sizes=(None, input_sizes, None),
    )

    cursors[0].setinputsizes.assert_not_called()
    cursors[1].setinputsizes.assert_called_once_with(input_sizes)
    cursors[1].execute.assert_called_once_with(
        "EXEC sys.sp_executesql ?, ?, ?",
        ("SELECT 1", "", 42),
    )
    cursors[2].setinputsizes.assert_not_called()


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


class DriverLikeConnection:
    """Behaves like mssql-python 1.10: no cancel call, and each new cursor
    copies ``connection.timeout`` as its statement timeout."""

    def __init__(self, *, block_on: str | None = None, on_execute=None) -> None:
        self.timeout = 30
        self.dispatched: list[str] = []
        self.cursor_timeouts: list[int] = []
        self.block_on = block_on
        self.on_execute = on_execute
        self.blocked = threading.Event()
        self.unblock = threading.Event()

    def cursor(self):
        self.cursor_timeouts.append(self.timeout)
        connection = self

        class Cursor:
            description = None
            rowcount = -1
            messages: list[object] = []

            def execute(self, statement, params=None):
                connection.dispatched.append(statement)
                if connection.on_execute is not None:
                    connection.on_execute(statement)
                if statement == connection.block_on:
                    connection.blocked.set()
                    connection.unblock.wait(timeout=2)

            def nextset(self):
                return False

            def close(self):
                pass

        return Cursor()


def _recording_pool(connection) -> tuple[SimpleNamespace, asyncio.Event]:
    discarded = asyncio.Event()
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(side_effect=lambda *_args: discarded.set()),
    )
    return pool, discarded


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["execute_session", "execute_session_exactly_once"])
async def test_cancelled_session_dispatches_no_later_statements(
    sample_server_config,
    entrypoint: str,
) -> None:
    # mssql-python cannot cancel the statement in flight. After a tool timeout
    # the executor must at least stop the rest of the session; otherwise every
    # remaining benchmark run still loads the server and holds the pool slot.
    connection = DriverLikeConnection(block_on="SELECT 2")
    pool, discarded = _recording_pool(connection)
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)
    statements = [f"SELECT {index}" for index in range(1, 6)]

    task = asyncio.create_task(getattr(executor, entrypoint)("appdb", statements))
    assert await asyncio.to_thread(connection.blocked.wait, 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    connection.unblock.set()
    await asyncio.wait_for(discarded.wait(), timeout=2)

    user_statements = [s for s in connection.dispatched if s.startswith("SELECT")]
    assert user_statements == ["SELECT 1", "SELECT 2"]
    pool.release.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelled_profiled_sample_does_not_dispatch_user_query(
    sample_server_config,
) -> None:
    # A measured sample cancelled during its SET setup must not start the
    # user query afterwards: nobody will read the measurement.
    connection = DriverLikeConnection(
        block_on="SET LOCK_TIMEOUT 30000; SET STATISTICS IO ON; SET STATISTICS XML ON;"
    )
    pool, discarded = _recording_pool(connection)
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    task = asyncio.create_task(
        executor.execute_profiled_read_only("appdb", "SELECT heavy FROM dbo.t")
    )
    assert await asyncio.to_thread(connection.blocked.wait, 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    connection.unblock.set()
    await asyncio.wait_for(discarded.wait(), timeout=2)

    assert "SELECT heavy FROM dbo.t" not in connection.dispatched
    pool.release.assert_not_awaited()


def test_session_statement_timeouts_shrink_to_the_remaining_deadline(
    sample_server_config,
) -> None:
    # Each later statement gets min(query_timeout, time left). The driver then
    # sends the attention itself, so no statement outlives the tool budget by
    # a full query timeout. A statement past the deadline is never sent.
    clock = [1000.0]
    advance_after = {"SELECT 1": 12.0, "SELECT 2": 9.0}

    def advance(statement: str) -> None:
        clock[0] += advance_after.get(statement, 0.0)

    connection = DriverLikeConnection(on_execute=advance)
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    fake_time = SimpleNamespace(
        monotonic=lambda: clock[0],
        perf_counter=time.perf_counter,
    )

    with patch("azure_sql_mcp.connection.time", fake_time):
        with statement_deadline(20):
            with pytest.raises(StatementDispatchStopped):
                executor._execute_session_with_connection(
                    connection,
                    "appdb",
                    ("SELECT 1", "SELECT 2", "SELECT 3"),
                    max_rows=10,
                )

    # 20 s left -> 21, then 8 s left -> 9, then -1 s left -> not dispatched.
    # The driver timeout lands just after the deadline, never before it, so
    # the caller's wait_for still owns the 'timeout' outcome.
    assert connection.cursor_timeouts == [21, 9]
    assert "SELECT 3" not in connection.dispatched
    # The pooled connection keeps its configured timeout for the next lease.
    assert connection.timeout == 30


@pytest.mark.asyncio
async def test_statement_deadline_reaches_the_worker_thread(sample_server_config) -> None:
    connection = DriverLikeConnection()
    pool, _discarded = _recording_pool(connection)
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    with statement_deadline(5.5):
        await executor.execute_session("appdb", ["SET SHOWPLAN_XML ON", "SELECT 1"])
        await executor.fetch_all("appdb", "SELECT 2")

    assert connection.cursor_timeouts == [7, 7, 7]
    assert connection.timeout == 30

    await executor.fetch_all("appdb", "SELECT 3")
    assert connection.cursor_timeouts[-1] == sample_server_config.query_timeout_seconds


def test_request_cancel_reports_once_that_the_driver_cannot_cancel(
    sample_server_config,
    caplog,
) -> None:
    # mssql-python has no cancel call. Say so once instead of pretending.
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    connection = SimpleNamespace()

    with caplog.at_level(logging.WARNING, logger="azure_sql_mcp.connection"):
        executor._request_cancel(connection)
        executor._request_cancel(connection)

    reports = [r for r in caplog.records if "cancel" in r.getMessage().lower()]
    assert len(reports) == 1


class _DriverQueryTimeout(Exception):
    """Stands in for the HYT00 error the driver raises at its own timeout."""


@pytest.mark.asyncio
async def test_statement_in_flight_at_the_tool_deadline_surfaces_as_timeout(
    sample_server_config,
) -> None:
    # _run_tool maps asyncio.TimeoutError to 'timeout' and anything else to
    # 'tool_error'; benchmark receipts record failure_code='timeout' only on
    # the timeout path. The shrunk driver timeout must therefore fire just
    # after the tool deadline, so a statement still running at the deadline
    # ends in asyncio.TimeoutError and not in a driver error.
    release = threading.Event()
    cursor_timeouts: list[int] = []

    class Connection:
        timeout = 30

        def cursor(self):
            limit = self.timeout
            cursor_timeouts.append(limit)

            class Cursor:
                description = None
                rowcount = -1
                messages: list[object] = []

                def execute(self, statement, params=None):
                    if statement == "SELECT 1":
                        time.sleep(0.1)
                    if statement != "SELECT heavy":
                        return
                    if not release.wait(timeout=limit):
                        raise _DriverQueryTimeout(f"HYT00 timeout after {limit}s")

                def nextset(self):
                    return False

                def close(self):
                    pass

            return Cursor()

    pool, discarded = _recording_pool(Connection())
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    try:
        with statement_deadline(2.0):
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    executor.execute_session("appdb", ["SELECT 1", "SELECT heavy"]),
                    timeout=2.0,
                )
    finally:
        release.set()
    await asyncio.wait_for(discarded.wait(), timeout=2)

    # About 1.9 s were left for the heavy statement: its driver timeout must
    # pass the deadline but stay close to it, so orphaned work stays bounded.
    assert cursor_timeouts[-1] == 3
