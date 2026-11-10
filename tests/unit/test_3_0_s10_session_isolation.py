"""Review finding 38: a session that changes the isolation level must not
return its connection to the pool at that level.

The pool does not reset a session. The next tool on that connection would
read at SNAPSHOT: benchmark samples then differ by which pooled connection
they got, and In-Memory OLTP tables fail with error 41332.
"""

from __future__ import annotations

import re
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from azure_sql_mcp.connection import AzureSqlExecutor

RESET = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
_LEVEL = re.compile(r"SET TRANSACTION ISOLATION LEVEL (.+)", re.IGNORECASE)


class IsolationTrackingConnection:
    """Keeps the session isolation level the way SQL Server does."""

    def __init__(self) -> None:
        self.timeout = 30
        self.isolation_level = "READ COMMITTED"
        self.dispatched: list[str] = []

    def cursor(self):
        connection = self

        class Cursor:
            description = None
            messages: list[object] = []

            def execute(self, statement, params=None):
                connection.dispatched.append(statement)
                match = _LEVEL.search(statement)
                if match:
                    connection.isolation_level = match.group(1).strip().upper()

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


CONTROL_STATE_CAPTURE = [
    "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
    "BEGIN TRANSACTION",
    "SELECT actual_state_desc FROM sys.database_query_store_options",
    "ROLLBACK TRANSACTION",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["execute_session", "execute_session_exactly_once"])
async def test_connection_goes_back_to_the_pool_at_read_committed(
    sample_server_config, entrypoint: str
) -> None:
    connection = IsolationTrackingConnection()
    executor, pool = _executor(sample_server_config, connection)

    results = await getattr(executor, entrypoint)("appdb", CONTROL_STATE_CAPTURE)

    pool.release.assert_awaited_once_with("appdb", connection)
    assert connection.isolation_level == "READ COMMITTED"
    assert connection.dispatched[-1] == RESET
    # The reset is the executor's own statement, not one of the caller's results.
    assert len(results) == len(CONTROL_STATE_CAPTURE)


@pytest.mark.asyncio
async def test_no_extra_round_trip_when_the_level_is_unchanged_or_already_reset(
    sample_server_config,
) -> None:
    plain = IsolationTrackingConnection()
    executor, _pool = _executor(sample_server_config, plain)
    await executor.execute_session("appdb", ["SET SHOWPLAN_XML ON", "SELECT 1", "SET SHOWPLAN_XML OFF"])
    assert RESET not in plain.dispatched

    resets_itself = IsolationTrackingConnection()
    executor, _pool = _executor(sample_server_config, resets_itself)
    await executor.execute_session("appdb", [*CONTROL_STATE_CAPTURE, RESET])
    assert resets_itself.dispatched.count(RESET) == 1
