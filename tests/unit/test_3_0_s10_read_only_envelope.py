"""Defense in depth behind the read-only validator (design W1 step 1).

A caller's SQL can run inside a transaction the executor always rolls back.
A write that got past the validator is then undone, and a batch that ends or
commits the transaction is detected and its connection thrown away.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from mssql_python.exceptions import ProgrammingError

from azure_sql_mcp.connection import AzureSqlExecutor
from azure_sql_mcp.connection import BatchExecutionMode
from azure_sql_mcp.connection import ReadOnlyEnvelopeViolation

BEGIN = "SET LOCK_TIMEOUT 30000; BEGIN TRANSACTION"


class TransactionalConnection:
    """Tracks @@TRANCOUNT and uncommitted writes the way SQL Server does."""

    def __init__(self, *, fail_on: str | None = None) -> None:
        self.timeout = 30
        self.autocommit = True
        self.closed = False
        self.trancount = 0
        self.uncommitted: list[str] = []
        self.durable: list[str] = []
        self.dispatched: list[str] = []
        self.fail_on = fail_on

    def cursor(self):
        connection = self

        class Cursor:
            description = None
            messages: list[object] = []
            _row = None

            def execute(self, statement, params=None):
                connection.dispatched.append(statement)
                if connection.fail_on and connection.fail_on in statement:
                    raise ProgrammingError(
                        "Base table or view not found",
                        "[Microsoft][SQL Server]Invalid object name 'dbo.missing'. (208)",
                    )
                self.description = None
                if "@mcp_trancount" in statement:
                    self._row = (connection.trancount,)
                    self.description = [("trancount", int, None, None, None, None, None)]
                    if connection.trancount > 0:
                        connection.uncommitted.clear()
                        connection.trancount = 0
                    return
                for part in (item.strip() for item in statement.split(";")):
                    if part.endswith("BEGIN TRANSACTION"):
                        connection.trancount += 1
                    elif part.startswith("INSERT"):
                        target = connection.uncommitted if connection.trancount else connection.durable
                        target.append(part)
                    elif part == "COMMIT":
                        connection.durable.extend(connection.uncommitted)
                        connection.uncommitted.clear()
                        connection.trancount = 0
                    elif part.startswith("SELECT"):
                        self._row = (1,)
                        self.description = [("value", int, None, None, None, None, None)]

            def fetchone(self):
                return self._row

            def fetchall(self):
                return [self._row]

            def fetchmany(self, size):
                return [self._row][:size]

            def nextset(self):
                return False

            def close(self):
                pass

        return Cursor()


def _executor(sample_server_config, connection):
    pool = SimpleNamespace(
        acquire=AsyncMock(return_value=connection), release=AsyncMock(), discard=AsyncMock()
    )
    return AzureSqlExecutor(sample_server_config, MagicMock(), pool), pool


@pytest.mark.asyncio
async def test_envelope_wraps_the_exact_batch_and_rolls_back(sample_server_config) -> None:
    connection = TransactionalConnection()
    executor, pool = _executor(sample_server_config, connection)

    rows = await executor.fetch_all("appdb", "SELECT 1 AS value", read_only_envelope=True)

    assert rows == [{"value": 1}]
    assert connection.dispatched[0] == BEGIN
    # The caller's batch text is sent unchanged, as its own batch.
    assert connection.dispatched[1] == "SELECT 1 AS value"
    assert "ROLLBACK TRANSACTION" in connection.dispatched[2] and len(connection.dispatched) == 3
    assert connection.trancount == 0
    pool.release.assert_awaited_once_with("appdb", connection)


@pytest.mark.asyncio
async def test_a_write_that_slipped_past_the_validator_is_rolled_back(
    sample_server_config,
) -> None:
    connection = TransactionalConnection()
    executor, _pool = _executor(sample_server_config, connection)

    await executor.fetch_all(
        "appdb", "SELECT 1; INSERT INTO dbo.audit VALUES (1)", read_only_envelope=True
    )

    assert connection.durable == []
    assert connection.uncommitted == []


@pytest.mark.asyncio
async def test_a_batch_that_commits_is_reported_and_its_connection_discarded(
    sample_server_config,
) -> None:
    # The envelope cannot undo a COMMIT. It must say so, once, and never
    # put that connection back in the pool.
    connection = TransactionalConnection()
    executor, pool = _executor(sample_server_config, connection)

    with pytest.raises(ReadOnlyEnvelopeViolation):
        await executor.fetch_all(
            "appdb", "SELECT 1; INSERT INTO dbo.audit VALUES (1); COMMIT", read_only_envelope=True
        )

    assert pool.acquire.await_count == 1
    pool.discard.assert_awaited_once_with("appdb", connection)
    pool.release.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_statement_in_the_envelope_never_returns_its_connection(
    sample_server_config,
) -> None:
    # Outside the envelope an "invalid object" error keeps the connection.
    # Inside it a transaction may still be open, so the connection goes.
    connection = TransactionalConnection(fail_on="dbo.missing")
    executor, pool = _executor(sample_server_config, connection)

    with pytest.raises(ProgrammingError):
        await executor.fetch_all("appdb", "SELECT 1 FROM dbo.missing", read_only_envelope=True)

    pool.discard.assert_awaited_once_with("appdb", connection)
    pool.release.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_envelope_begins_before_the_first_and_ends_after_the_last_statement(
    sample_server_config,
) -> None:
    connection = TransactionalConnection()
    executor, pool = _executor(sample_server_config, connection)

    results = await executor.execute_session(
        "appdb",
        ["SET STATISTICS XML ON", "SELECT 1 AS value", "SET STATISTICS XML OFF"],
        read_only_envelope=True,
    )

    assert len(results) == 3
    assert connection.dispatched[0] == BEGIN
    assert connection.dispatched[1:4] == [
        "SET STATISTICS XML ON",
        "SELECT 1 AS value",
        "SET STATISTICS XML OFF",
    ]
    assert "ROLLBACK TRANSACTION" in connection.dispatched[4]
    pool.release.assert_awaited_once_with("appdb", connection)


@pytest.mark.asyncio
async def test_envelope_is_refused_for_admin_batches(sample_server_config) -> None:
    executor, pool = _executor(sample_server_config, TransactionalConnection())

    with pytest.raises(ValueError):
        await executor.execute_batches(
            "appdb",
            "CREATE INDEX ix ON dbo.t (a)",
            execution_mode=BatchExecutionMode.ADMIN,
            read_only_envelope=True,
        )
    pool.acquire.assert_not_awaited()
