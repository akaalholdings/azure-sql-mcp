"""Retry only what the engine reported as transient, and file a failing
server-built statement as a product bug.

A retry of a deterministic error runs the same heavy query again, up to four
times, on a database that is already slow.
"""

from __future__ import annotations

import json
import logging
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from mssql_python.exceptions import OperationalError
from mssql_python.exceptions import ProgrammingError

from azure_sql_mcp.connection import AzureSqlExecutor
from azure_sql_mcp.incident_log import describe_exception
from azure_sql_mcp.observability import sql_origin
from azure_sql_mcp.retry import with_retry


def _tool_error(root: BaseException) -> ToolError:
    payload = json.dumps({"code": "tool_error", "message": "sanitized", "ok": False})
    try:
        try:
            raise root
        except BaseException as inner:
            raise ToolError(payload) from inner
    except ToolError as outer:
        return outer


async def _attempts(error: BaseException) -> int:
    func = AsyncMock(side_effect=error)
    with patch("azure_sql_mcp.retry.asyncio.sleep", new=AsyncMock()):
        with pytest.raises(type(error)):
            await with_retry(func, max_retries=3)
    return func.await_count


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        # Error 245: the row value 1205 is data, not the deadlock error number.
        ProgrammingError(
            "Syntax error or access violation",
            "[Microsoft][SQL Server]Conversion failed when converting the varchar "
            "value '1205' to data type int. (245)",
        ),
        # Error 229: the object name ends with a transient code.
        ProgrammingError(
            "Syntax error or access violation",
            "[Microsoft][SQL Server]The SELECT permission was denied on the object "
            "'Orders_40613', database 'appdb', schema 'dbo'. (229)",
        ),
        # Error 102: a syntax error near a literal.
        ProgrammingError(
            "Syntax error or access violation",
            "[Microsoft][SQL Server]Incorrect syntax near '40501'. (102)",
        ),
        # Error 208: an object name with a transport error number in it.
        ProgrammingError(
            "Base table or view not found",
            "[Microsoft][SQL Server]Invalid object name 'dbo.Orders_10054'. (208)",
        ),
    ],
)
async def test_deterministic_errors_are_sent_once(error: BaseException) -> None:
    assert await _attempts(error) == 1


@pytest.mark.asyncio
async def test_reported_transient_error_is_still_retried() -> None:
    failover = OperationalError(
        "Communication link failure",
        "[Microsoft][SQL Server]Database 'appdb' on server 'srv' is not currently "
        "available. (40613)",
    )
    func = AsyncMock(side_effect=[failover, "ok"])
    with patch("azure_sql_mcp.retry.asyncio.sleep", new=AsyncMock()):
        assert await with_retry(func, max_retries=3) == "ok"
    assert func.await_count == 2


@pytest.mark.asyncio
async def test_retry_warning_does_not_log_server_names_or_values(caplog) -> None:
    # Review finding 101: JSON logs go to the MCP client's log file.
    failover = OperationalError(
        "Communication link failure",
        "[Microsoft][SQL Server]Database 'appdb' on server "
        "'contoso-sql.database.windows.net' is not currently available. (40613)",
    )
    func = AsyncMock(side_effect=[failover, "ok"])
    with (
        patch("azure_sql_mcp.retry.asyncio.sleep", new=AsyncMock()),
        caplog.at_level(logging.WARNING, logger="azure_sql_mcp.retry"),
    ):
        await with_retry(func, max_retries=1)

    logged = [getattr(record, "error", "") for record in caplog.records]
    assert logged and all(isinstance(text, str) for text in logged)
    assert "contoso-sql" not in "".join(logged)
    assert "appdb" not in "".join(logged)


class _FailingConnection:
    """Fails the first statement whose text contains ``fail_on``."""

    timeout = 30

    def __init__(self, fail_on: str) -> None:
        self.fail_on = fail_on

    def cursor(self):
        connection = self

        class Cursor:
            description = None
            messages: list[object] = []

            def execute(self, statement, params=None):
                if connection.fail_on in statement:
                    raise ProgrammingError(
                        "Syntax error or access violation",
                        "[Microsoft][SQL Server]Incorrect syntax near 'x'. (102)",
                    )

            def nextset(self):
                return False

            def close(self):
                pass

        return Cursor()


def test_executor_tags_the_statement_origin_on_a_driver_error(sample_server_config) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    with pytest.raises(ProgrammingError) as server_built:
        executor._execute_with_connection(
            _FailingConnection("sys.dm_exec_requests"),
            "appdb",
            "SELECT 1 FROM sys.dm_exec_requests",
            (),
            sql_origin="server_built",
        )
    assert sql_origin(server_built.value) == "server_built"

    with pytest.raises(ProgrammingError) as caller:
        executor._execute_session_with_connection(
            _FailingConnection("dbo.Orders"),
            "appdb",
            ("SET SHOWPLAN_XML ON", "SELECT 1 FROM dbo.Orders"),
            None,
            sql_origin="caller_supplied",
        )
    assert sql_origin(caller.value) == "caller_supplied"

    # The measured statement of a profiled run is always the caller's query.
    with pytest.raises(ProgrammingError) as profiled:
        executor._execute_profiled_with_connection(
            _FailingConnection("dbo.Orders"), "appdb", "SELECT 1 FROM dbo.Orders", (), None
        )
    assert sql_origin(profiled.value) == "caller_supplied"


def test_executor_setup_statement_failure_is_server_built(sample_server_config) -> None:
    # SET LOCK_TIMEOUT and the STATISTICS options are the server's own text,
    # whatever the origin of the query that follows them.
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())

    with pytest.raises(ProgrammingError) as lock_timeout:
        executor._execute_session_with_connection(
            _FailingConnection("SET LOCK_TIMEOUT"),
            "appdb",
            ("SELECT 1 FROM dbo.Orders",),
            None,
            sql_origin="caller_supplied",
        )
    assert sql_origin(lock_timeout.value) == "server_built"

    with pytest.raises(ProgrammingError) as setup:
        executor._execute_profiled_with_connection(
            _FailingConnection("SET STATISTICS"), "appdb", "SELECT 1 FROM dbo.Orders", (), None
        )
    assert sql_origin(setup.value) == "server_built"


def test_untagged_statement_keeps_no_origin(sample_server_config) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    with pytest.raises(ProgrammingError) as untagged:
        executor._execute_with_connection(
            _FailingConnection("dbo.Orders"), "appdb", "SELECT 1 FROM dbo.Orders", ()
        )
    assert sql_origin(untagged.value) is None


def _syntax_error(origin: str | None) -> ProgrammingError:
    from azure_sql_mcp.observability import tag_sql_origin

    error = ProgrammingError(
        "Syntax error or access violation",
        "[Microsoft][SQL Server]Incorrect syntax near 'x'. (102)",
    )
    if origin is not None:
        tag_sql_origin(error, origin)
    return error


def test_incident_log_files_a_failing_server_built_statement_as_a_product_bug() -> None:
    # tune_query takes SQL, but the statement that failed was a DMV read the
    # server built. Filing it as a caller error hides a product defect.
    described = describe_exception(
        _tool_error(_syntax_error("server_built")), sql_arguments=True
    )
    assert (described["category"], described["priority"]) == ("product_bug", "P2")


def test_incident_log_files_failing_caller_sql_as_a_caller_error() -> None:
    # benchmark_tuning_candidate takes a candidate id, not SQL; the candidate
    # text that failed to compile is still the caller's.
    described = describe_exception(
        _tool_error(_syntax_error("caller_supplied")), sql_arguments=False
    )
    assert (described["category"], described["priority"]) == ("caller_error", "P4")


def test_incident_log_keeps_the_argument_rule_for_untagged_errors() -> None:
    with_sql = describe_exception(_tool_error(_syntax_error(None)), sql_arguments=True)
    without_sql = describe_exception(_tool_error(_syntax_error(None)), sql_arguments=False)
    assert with_sql["category"] == "caller_error"
    assert without_sql["category"] == "product_bug"
