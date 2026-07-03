from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import pytest

from azure_sql_mcp.connection import AzureSqlExecutor
from azure_sql_mcp.connection import QueryResult


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
