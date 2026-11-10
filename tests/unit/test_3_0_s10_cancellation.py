"""Stopping work on the server after a tool timeout or a client cancel.

mssql-python 1.10 has no cancel call. What the executor can do is: send no
further statement of a cancelled call, never commit a cancelled transaction,
and give each statement a driver timeout that fits the time left, so the driver
itself sends the attention to Azure SQL.
"""

from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from azure_sql_mcp.connection import AzureSqlExecutor
from azure_sql_mcp.connection import StatementDispatchStopped
from azure_sql_mcp.connection import driver_cancel_supported
from azure_sql_mcp.connection import statement_deadline


class DriverLikeConnection:
    """Behaves like mssql-python 1.10: no cancel call, and each new cursor
    copies ``connection.timeout`` as its statement timeout."""

    def __init__(self, *, block_on: str | None = None) -> None:
        self.timeout = 30
        self.autocommit = True
        self.dispatched: list[str] = []
        self.cursor_timeouts: list[int] = []
        self.dead_timeout_writes: list[int] = []
        self.commits = 0
        self.rollbacks = 0
        self.block_on = block_on
        self.blocked = threading.Event()
        self.unblock = threading.Event()

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def cursor(self):
        self.cursor_timeouts.append(self.timeout)
        connection = self

        class Cursor:
            description = None
            rowcount = 1
            messages: list[object] = []

            @property
            def timeout(self) -> int:
                raise AttributeError("mssql-python cursors have no timeout")

            @timeout.setter
            def timeout(self, value: int) -> None:
                # The real driver stores this on the instance and never reads it.
                connection.dead_timeout_writes.append(value)

            def execute(self, statement, params=None):
                connection.dispatched.append(statement)
                if statement == connection.block_on:
                    connection.blocked.set()
                    connection.unblock.wait(timeout=2)

            def nextset(self):
                return False

            def close(self):
                pass

        return Cursor()


def _pool(connection) -> tuple[SimpleNamespace, asyncio.Event]:
    discarded = asyncio.Event()
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection),
        release=AsyncMock(),
        discard=AsyncMock(side_effect=lambda *_args: discarded.set()),
    )
    return pool, discarded


@pytest.mark.asyncio
async def test_cancelled_transaction_sends_no_later_statement_and_never_commits(
    sample_server_config,
) -> None:
    # A tool timeout during a multi-statement write must not leave the worker
    # thread sending the rest of the transaction and committing it after the
    # caller was told the call failed.
    connection = DriverLikeConnection(block_on="INSERT INTO dbo.audit VALUES (1)")
    pool, discarded = _pool(connection)
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    def callback(session):
        session.execute("INSERT INTO dbo.audit VALUES (1)")
        session.execute("INSERT INTO dbo.audit VALUES (2)")
        return "done"

    task = asyncio.create_task(executor.execute_transaction_exactly_once("appdb", callback))
    assert await asyncio.to_thread(connection.blocked.wait, 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    connection.unblock.set()
    await asyncio.wait_for(discarded.wait(), timeout=2)

    assert "INSERT INTO dbo.audit VALUES (2)" not in connection.dispatched
    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.asyncio
async def test_transaction_cancelled_after_its_last_statement_is_not_committed(
    sample_server_config,
) -> None:
    # COMMIT is a statement too: a call cancelled while the callback finishes
    # must roll back, because nobody reads the result of the commit.
    connection = DriverLikeConnection(block_on="INSERT INTO dbo.audit VALUES (1)")
    pool, discarded = _pool(connection)
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), pool)

    task = asyncio.create_task(
        executor.execute_transaction_exactly_once(
            "appdb", lambda session: session.execute("INSERT INTO dbo.audit VALUES (1)")
        )
    )
    assert await asyncio.to_thread(connection.blocked.wait, 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    connection.unblock.set()
    await asyncio.wait_for(discarded.wait(), timeout=2)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_transaction_past_the_tool_deadline_rolls_back(sample_server_config) -> None:
    # The tool budget is spent: the caller has timed out, so the transaction
    # must not commit seconds later behind its back.
    connection = DriverLikeConnection()
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    def slow_callback(session):
        session.execute("INSERT INTO dbo.audit VALUES (1)")
        time.sleep(0.15)
        return "done"

    with statement_deadline(0.05):
        with pytest.raises(StatementDispatchStopped):
            executor._execute_transaction_with_connection(connection, "appdb", slow_callback)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_write_paths_get_a_driver_timeout_that_fits_the_time_left(
    sample_server_config,
) -> None:
    # The driver reads the statement timeout from connection.timeout when the
    # cursor is created. A write that starts 5.5 s before the tool deadline
    # must be stopped by the driver about then, not after the full 30 s.
    non_query = DriverLikeConnection()
    transaction = DriverLikeConnection()
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    with statement_deadline(5.5):
        executor._execute_non_query_with_connection(
            non_query, "appdb", "UPDATE dbo.t SET a = 1", ()
        )
        executor._execute_transaction_with_connection(
            transaction, "appdb", lambda session: session.execute("UPDATE dbo.t SET a = 1")
        )

    assert set(non_query.cursor_timeouts) == {7}
    assert set(transaction.cursor_timeouts) == {7}
    assert non_query.timeout == 30
    assert transaction.timeout == 30


def test_no_path_relies_on_the_cursor_timeout_attribute_the_driver_ignores(
    sample_server_config,
) -> None:
    # Review finding 80: mssql-python cursors have no timeout attribute, so
    # writing one changes nothing on the server and hides that fact.
    connection = DriverLikeConnection()
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    executor._execute_with_connection(connection, "appdb", "SELECT 1", ())
    executor._execute_session_with_connection(connection, "appdb", ("SELECT 1",), None)
    executor._execute_profiled_with_connection(connection, "appdb", "SELECT 1", (), None)
    executor._execute_non_query_with_connection(connection, "appdb", "UPDATE dbo.t SET a = 1", ())
    executor._execute_transaction_with_connection(
        connection, "appdb", lambda session: session.fetch_all("SELECT 1")
    )

    assert connection.dead_timeout_writes == []
    assert set(connection.cursor_timeouts) == {sample_server_config.query_timeout_seconds}


def test_statement_timeout_seconds_sets_the_cap_for_a_measured_statement(
    sample_server_config,
) -> None:
    # A slow baseline (45-60 s) cannot be measured under the 30 s default.
    # The caller may raise the cap for the measured statement; the time left
    # in the tool budget still bounds it.
    connection = DriverLikeConnection()
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    executor._execute_profiled_with_connection(
        connection, "appdb", "SELECT heavy FROM dbo.t", (), None, statement_timeout_seconds=120
    )
    measured = connection.dispatched.index("SELECT heavy FROM dbo.t")
    assert connection.cursor_timeouts[measured] == 120
    assert connection.timeout == 30

    bounded = DriverLikeConnection()
    with statement_deadline(50):
        executor._execute_session_with_connection(
            bounded, "appdb", ("SELECT heavy FROM dbo.t",), None, statement_timeout_seconds=120
        )
    assert bounded.cursor_timeouts == [51]


def test_request_cancel_says_whether_the_driver_cancelled(sample_server_config) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    with_cancel: Any = MagicMock()

    assert executor._request_cancel(DriverLikeConnection()) is False
    assert executor._request_cancel(with_cancel) is True
    with_cancel.cancel.assert_called_once_with()


def test_cancellation_report_states_the_real_bound(sample_server_config) -> None:
    # check_runtime_status must tell the agent what a timeout leaves running.
    import mssql_python.connection

    has_cancel = callable(getattr(mssql_python.connection.Connection, "cancel", None))
    assert driver_cancel_supported() is has_cancel
    # The pinned driver has none; when an upgrade adds one this fails and the
    # report below must be reviewed.
    assert has_cancel is False

    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    report = executor.cancellation_report()

    assert report["driver_cancel_supported"] is False
    assert report["later_statements_sent_after_cancel"] is False
    assert report["in_flight_statement_stopped_by"] == "statement_timeout"
    assert (
        report["max_in_flight_seconds_after_client_cancel"]
        == sample_server_config.query_timeout_seconds
    )
    assert report["max_in_flight_seconds_after_tool_timeout"] == 2
