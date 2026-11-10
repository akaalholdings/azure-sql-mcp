"""Round trips and connection reuse on the read path (review findings 59, 60).

Each cursor.execute is one network round trip to Azure SQL. A new connection is
a token fetch, a TLS handshake and a gateway login. The tests count both.
"""

from __future__ import annotations

from typing import Any
from typing import Callable
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from mssql_python.exceptions import sqlstate_to_exception

from azure_sql_mcp.connection import AdminBatchOutcomeUnknownError
from azure_sql_mcp.connection import AzureSqlExecutor
from azure_sql_mcp.connection import BatchExecutionMode
from azure_sql_mcp.connection_pool import ConnectionPool

LOCK_TIMEOUT = "SET LOCK_TIMEOUT 30000"
PROFILED_SETUP = f"{LOCK_TIMEOUT}; SET STATISTICS IO ON; SET STATISTICS XML ON;"
PROFILED_TEARDOWN = "SET STATISTICS XML OFF; SET STATISTICS IO OFF;"


class CountingConnection:
    """A driver-like connection that records each cursor.execute call."""

    def __init__(self, on_execute: Callable[[str], None] | None = None) -> None:
        self.timeout = 30
        self.autocommit = True
        self.closed = False
        self.executes: list[tuple[str, tuple[Any, ...] | None]] = []
        self.on_execute = on_execute

    @property
    def statements(self) -> list[str]:
        return [statement for statement, _params in self.executes]

    def cursor(self):
        connection = self

        class Cursor:
            description = None
            rowcount = -1
            messages: list[object] = []

            def execute(self, statement, params=None):
                connection.executes.append((statement, params))
                if connection.on_execute is not None:
                    connection.on_execute(statement)
                # Like the driver: a SET statement returns no result set, so
                # the first result set of a batch is the query's.
                if "SELECT" in statement.upper():
                    self.description = [("value", int, None, None, None, None, None)]

            def fetchone(self):
                return (1,)

            def fetchall(self):
                return [(1,)]

            def fetchmany(self, size):
                return [(1,)][:size]

            def nextset(self):
                return False

            def close(self):
                pass

        return Cursor()

    def close(self) -> None:
        self.closed = True


def _driver_error(sqlstate: str, text: str) -> Exception:
    """The exception mssql-python raises for this SQLSTATE and server text."""

    error = sqlstate_to_exception(sqlstate, text)
    assert error is not None
    return error


def _executor_with_pool(
    config,
    connections: list[CountingConnection],
) -> tuple[AzureSqlExecutor, ConnectionPool, MagicMock]:
    pool = ConnectionPool(config, MagicMock())
    create = MagicMock(side_effect=connections)
    pool._create_connection = create  # type: ignore[method-assign]
    return AzureSqlExecutor(config, MagicMock(), pool), pool, create


# --- finding 59: SET LOCK_TIMEOUT in the same batch as the read ---------------


@pytest.mark.asyncio
async def test_read_sends_lock_timeout_and_query_in_one_round_trip(
    sample_server_config,
) -> None:
    connection = CountingConnection()
    executor, _pool, _create = _executor_with_pool(sample_server_config, [connection])

    for _ in range(10):
        rows = await executor.fetch_all("appdb", "SELECT value FROM sys.objects")
        assert rows == [{"value": 1}]

    # 10 reads were 20 round trips. The lock timeout still goes with each read.
    assert len(connection.executes) == 10
    assert set(connection.statements) == {f"{LOCK_TIMEOUT}; SELECT value FROM sys.objects"}


@pytest.mark.asyncio
async def test_lock_timeout_prefix_keeps_parameters_and_ends_before_a_cte(
    sample_server_config,
) -> None:
    connection = CountingConnection()
    executor, _pool, _create = _executor_with_pool(sample_server_config, [connection])
    query = "WITH q AS (SELECT ? AS a, ? AS b) SELECT a, b FROM q"

    await executor.fetch_all("appdb", query, params=(1, "x"))

    # A CTE needs the statement before it closed with a semicolon. The prefix
    # holds no parameter marker, so the markers still bind in order.
    statement, params = connection.executes[0]
    assert len(connection.executes) == 1
    assert statement == f"{LOCK_TIMEOUT}; {query}"
    assert statement.count("?") == len(params or ()) == 2
    assert params == (1, "x")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query",
    [
        "CREATE VIEW dbo.v AS SELECT 1 AS value",
        "SET SHOWPLAN_XML ON",
        "EXEC sys.sp_query_store_flush_db",
        "/* note */ SELECT 1 AS value",
    ],
)
async def test_statement_that_is_not_a_plain_select_keeps_its_own_batch(
    sample_server_config,
    query: str,
) -> None:
    # CREATE VIEW must be first in its batch and SET SHOWPLAN_XML must be alone
    # in it. Only a batch that starts with SELECT or WITH takes the prefix.
    connection = CountingConnection()
    executor, _pool, _create = _executor_with_pool(sample_server_config, [connection])

    await executor.fetch_all("appdb", query)

    assert connection.statements == [LOCK_TIMEOUT, query]


@pytest.mark.asyncio
async def test_null_parameter_read_keeps_the_separate_lock_timeout(
    sample_server_config,
) -> None:
    # The driver asks the server to describe a NULL parameter from the batch
    # text. That path is not proven with a prefixed batch, so it stays as it was.
    connection = CountingConnection()
    executor, _pool, _create = _executor_with_pool(sample_server_config, [connection])

    await executor.fetch_all("appdb", "SELECT ? AS value", params=(None,))

    assert connection.executes == [(LOCK_TIMEOUT, ()), ("SELECT ? AS value", (None,))]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution_mode",
    [BatchExecutionMode.DEFAULT, BatchExecutionMode.ADMIN],
)
async def test_gated_write_batches_keep_the_separate_lock_timeout(
    sample_server_config,
    execution_mode: BatchExecutionMode,
) -> None:
    # Gated writes call execute_batches directly (admin_policy). Their batch
    # text must reach the server unchanged, even when it starts with SELECT.
    connection = CountingConnection()
    executor, _pool, _create = _executor_with_pool(sample_server_config, [connection])

    await executor.execute_batches(
        "appdb", "SELECT 1 AS value", execution_mode=execution_mode
    )

    assert connection.statements == [LOCK_TIMEOUT, "SELECT 1 AS value"]


@pytest.mark.asyncio
async def test_showplan_session_statements_stay_alone_in_their_batches(
    sample_server_config,
) -> None:
    connection = CountingConnection()
    executor, _pool, _create = _executor_with_pool(sample_server_config, [connection])
    statements = ["SET SHOWPLAN_XML ON", "SELECT 1 AS value", "SET SHOWPLAN_XML OFF"]

    await executor.execute_session("appdb", statements)

    assert connection.statements == [LOCK_TIMEOUT, *statements]


@pytest.mark.asyncio
async def test_profiled_sample_uses_three_round_trips(sample_server_config) -> None:
    connection = CountingConnection()
    executor, _pool, _create = _executor_with_pool(sample_server_config, [connection])

    result = await executor.execute_profiled_read_only("appdb", "SELECT 1 AS value")

    # Setup, the measured query, teardown: was 6 round trips. The query text is
    # unchanged, and STATISTICS XML is last on and first off so that no setup
    # or teardown statement runs with it on.
    assert connection.statements == [PROFILED_SETUP, "SELECT 1 AS value", PROFILED_TEARDOWN]
    assert result.user_query_executions == 1
    assert [result_set.rows for result_set in result.result_sets] == [[{"value": 1}]]


@pytest.mark.asyncio
async def test_failed_profiled_sample_still_runs_teardown_and_discards(
    sample_server_config,
) -> None:
    def fail_query(statement: str) -> None:
        if statement == "SELECT 1 FROM dbo.missing":
            raise _driver_error("42S02", "[Microsoft][SQL Server]Invalid object name 'dbo.missing'.")

    connection = CountingConnection(fail_query)
    executor, pool, _create = _executor_with_pool(sample_server_config, [connection])

    with pytest.raises(Exception, match="Invalid object name"):
        await executor.execute_profiled_read_only("appdb", "SELECT 1 FROM dbo.missing")

    # A failed sample can leave STATISTICS state behind: never pool it.
    assert connection.statements[-1] == PROFILED_TEARDOWN
    assert connection.closed is True
    assert pool.get_metrics("appdb")["appdb"]["release_count"] == 0


# --- finding 60: keep the connection after a deterministic statement error ----

INVALID_OBJECT = ("42S02", "[Microsoft][SQL Server]Invalid object name 'sys.dm_missing'.")
PERMISSION_DENIED_WITH_NUMBER = (
    "42000",
    "[Microsoft][SQL Server]The SELECT permission was denied on the object "
    "'dm_db_stats_histogram', database 'mssqlsystemresource', schema 'sys'. (229)",
)


def _fail_reads_of(marker: str, error: tuple[str, str]) -> Callable[[str], None]:
    def on_execute(statement: str) -> None:
        if marker in statement:
            raise _driver_error(*error)

    return on_execute


# The shapes mssql-python 1.10 really returns (seen on Azure SQL Database):
# SQLSTATE class 42000 and no error number.
PERMISSION_DENIED = (
    "42000",
    "[Microsoft][SQL Server]The SELECT permission was denied on the object 'Users', "
    "database 'appdb', schema 'dbo'.",
)
NO_PERMISSION_FOR_ACTION = (
    "42000",
    "[Microsoft][SQL Server]The user does not have permission to perform this action.",
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [PERMISSION_DENIED_WITH_NUMBER, PERMISSION_DENIED, NO_PERMISSION_FOR_ACTION, INVALID_OBJECT],
)
async def test_read_after_a_statement_error_reuses_the_connection(
    sample_server_config,
    error: tuple[str, str],
) -> None:
    # An optional DMV read that the login may not run fails on purpose and the
    # caller swallows it. The session is still healthy: the next read must not
    # pay a new login.
    connections = [CountingConnection(_fail_reads_of("optional_dmv", error)) for _ in range(3)]
    executor, pool, create = _executor_with_pool(sample_server_config, connections)

    assert await executor.fetch_all("appdb", "SELECT 1 AS value") == [{"value": 1}]
    for _ in range(2):
        with pytest.raises(
            Exception, match="permission was denied|does not have permission|Invalid object name"
        ):
            await executor.fetch_all("appdb", "SELECT * FROM sys.optional_dmv")
        assert await executor.fetch_all("appdb", "SELECT 2 AS value") == [{"value": 1}]

    metrics = pool.get_metrics("appdb")["appdb"]
    assert create.call_count == 1
    assert metrics["discard_count"] == 0
    assert connections[0].closed is False
    # Each kept connection is proved with one SELECT 1 before its next use.
    assert connections[0].statements.count("SELECT 1") == 2


@pytest.mark.asyncio
async def test_connection_kept_after_an_error_is_checked_before_reuse(
    sample_server_config,
) -> None:
    # The driver cannot tell that the server ended a session. So the next
    # acquire proves the kept connection with SELECT 1 and replaces a dead one.
    first = CountingConnection(_fail_reads_of("optional_dmv", INVALID_OBJECT))
    second = CountingConnection()
    executor, pool, create = _executor_with_pool(sample_server_config, [first, second])

    with pytest.raises(Exception, match="Invalid object name"):
        await executor.fetch_all("appdb", "SELECT * FROM sys.optional_dmv")
    with patch.object(pool, "_validate_connection", return_value=False) as validate:
        assert await executor.fetch_all("appdb", "SELECT 2 AS value") == [{"value": 1}]

    validate.assert_called_once_with(first)
    assert create.call_count == 2
    assert first.closed is True
    assert second.statements == [f"{LOCK_TIMEOUT}; SELECT 2 AS value"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        ("HYT00", "[Microsoft]Query timeout expired"),
        ("HYT01", "[Microsoft]Connection timeout expired"),
        ("08S01", "[Microsoft]Communication link failure"),
        ("HY008", "[Microsoft]Operation canceled"),
        ("HY000", "[Microsoft][SQL Server]A severe error occurred on the current command."),
        # Severity 20 and transient service errors carry a number off the list.
        ("42000", "[Microsoft][SQL Server]Service is busy. (40501)"),
        ("42000", "[Microsoft][SQL Server]Lock request time out period exceeded. (1222)"),
        # mssql-python 1.10 gives this class no error number and no severity,
        # so a 42000 error that is not a permission error stays unknown.
        ("42000", "[Microsoft][SQL Server]Incorrect syntax near 'FORM'."),
        ("42000", "[Microsoft][SQL Server]Resource ID : 1. The request limit is 300."),
    ],
)
async def test_read_error_that_is_not_a_known_statement_error_discards(
    sample_server_config,
    server_config_factory,
    error: tuple[str, str],
) -> None:
    config = server_config_factory(max_retries=0)
    connections = [CountingConnection(_fail_reads_of("heavy", error)), CountingConnection()]
    executor, pool, create = _executor_with_pool(config, connections)

    with pytest.raises(Exception):
        await executor.fetch_all("appdb", "SELECT * FROM dbo.heavy")
    await executor.fetch_all("appdb", "SELECT 2 AS value")

    assert connections[0].closed is True
    assert create.call_count == 2
    assert pool.get_metrics("appdb")["appdb"]["discard_count"] == 1


@pytest.mark.asyncio
async def test_error_of_unknown_shape_discards(sample_server_config) -> None:
    # Text that only looks like a statement error is not proof of one.
    def on_execute(statement: str) -> None:
        if "heavy" in statement:
            raise RuntimeError("Invalid object name 'dbo.heavy'. (208)")

    connections = [CountingConnection(on_execute), CountingConnection()]
    executor, _pool, create = _executor_with_pool(sample_server_config, connections)

    with pytest.raises(RuntimeError):
        await executor.fetch_all("appdb", "SELECT * FROM dbo.heavy")
    await executor.fetch_all("appdb", "SELECT 2 AS value")

    assert connections[0].closed is True
    assert create.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["closed", "manual_commit"])
async def test_statement_error_on_an_unsafe_connection_discards(
    sample_server_config,
    state: str,
) -> None:
    first = CountingConnection()

    def fail(statement: str) -> None:
        if "optional_dmv" in statement:
            if state == "closed":
                first.closed = True
            raise _driver_error(*INVALID_OBJECT)

    first.on_execute = fail
    if state == "manual_commit":
        # Outside autocommit a failed statement can leave a transaction open.
        first.autocommit = False
    executor, pool, create = _executor_with_pool(
        sample_server_config, [first, CountingConnection()]
    )

    with pytest.raises(Exception, match="Invalid object name"):
        await executor.fetch_all("appdb", "SELECT * FROM sys.optional_dmv")
    await executor.fetch_all("appdb", "SELECT 2 AS value")

    assert create.call_count == 2
    assert pool.get_metrics("appdb")["appdb"]["discard_count"] == 1


@pytest.mark.asyncio
async def test_statement_error_in_a_read_that_is_not_a_plain_select_discards(
    sample_server_config,
) -> None:
    # A batch that does not start with SELECT or WITH may have changed session
    # or transaction state before it failed.
    first = CountingConnection(_fail_reads_of("optional_proc", INVALID_OBJECT))
    executor, _pool, create = _executor_with_pool(
        sample_server_config, [first, CountingConnection()]
    )

    with pytest.raises(Exception, match="Invalid object name"):
        await executor.fetch_all("appdb", "EXEC dbo.optional_proc")
    await executor.fetch_all("appdb", "SELECT 2 AS value")

    assert first.closed is True
    assert create.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution_mode",
    [BatchExecutionMode.DEFAULT, BatchExecutionMode.ADMIN],
)
async def test_statement_error_in_a_gated_write_batch_discards(
    sample_server_config,
    execution_mode: BatchExecutionMode,
) -> None:
    # A failed write batch can leave an open transaction or SET state behind.
    first = CountingConnection(_fail_reads_of("dbo.missing", INVALID_OBJECT))
    executor, pool, _create = _executor_with_pool(sample_server_config, [first])
    expected = (
        AdminBatchOutcomeUnknownError
        if execution_mode is BatchExecutionMode.ADMIN
        else Exception
    )

    with pytest.raises(expected, match="Invalid object name"):
        await executor.execute_batches(
            "appdb",
            "SELECT * FROM dbo.missing",
            execution_mode=execution_mode,
        )

    assert first.closed is True
    assert pool.get_metrics("appdb")["appdb"]["release_count"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["execute_session", "execute_session_exactly_once"])
async def test_statement_error_in_a_session_discards(
    sample_server_config,
    entrypoint: str,
) -> None:
    # The failed statement skips SET SHOWPLAN_XML OFF: the session is not clean.
    first = CountingConnection(_fail_reads_of("dbo.missing", INVALID_OBJECT))
    executor, pool, _create = _executor_with_pool(sample_server_config, [first])

    with pytest.raises(Exception, match="Invalid object name"):
        await getattr(executor, entrypoint)(
            "appdb",
            ["SET SHOWPLAN_XML ON", "SELECT * FROM dbo.missing", "SET SHOWPLAN_XML OFF"],
        )

    assert first.closed is True
    assert "SET SHOWPLAN_XML OFF" not in first.statements
    assert pool.get_metrics("appdb")["appdb"]["release_count"] == 0
