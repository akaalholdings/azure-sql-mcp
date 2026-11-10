"""S13a F2: a failed experiment DDL statement reports the engine's own words.

An agent that only sees ``sql_execution_failed`` cannot tell a refused ONLINE
option from a lock timeout, so it retries the same build.
"""

from __future__ import annotations

from mssql_python.exceptions import ProgrammingError

from azure_sql_mcp.connection import AdminBatchOutcomeUnknownError
from azure_sql_mcp.observability import extract_failure_diagnostic

ONLINE_REFUSED = (
    "create statement failed because the operation cannot be performed online on a "
    "table with a columnstore index. Perform the operation without specifying the "
    "ONLINE option or drop (or disable) the columnstore index before performing the "
    "operation using the ONLINE option."
)


def _driver_error(text: str) -> ProgrammingError:
    return ProgrammingError(
        driver_error="Syntax error or access violation",
        ddbc_error=f"[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]{text}",
    )


def _admin_failure(text: str) -> Exception:
    # The admin lane wraps every driver error of a dispatched batch.
    cause = _driver_error(text)
    try:
        raise AdminBatchOutcomeUnknownError(str(cause)) from cause
    except AdminBatchOutcomeUnknownError as wrapped:
        return wrapped


def test_ddl_failure_returns_the_engine_message_without_driver_tags() -> None:
    diagnostic = extract_failure_diagnostic(_admin_failure(ONLINE_REFUSED), engine_text=True)

    assert diagnostic["diagnostic_code"] == "sql_execution_failed"
    assert diagnostic["engine_message"] == ONLINE_REFUSED
    assert "[Microsoft]" not in diagnostic["engine_message"]


def test_engine_error_number_is_returned_when_the_driver_reports_it() -> None:
    error = RuntimeError(
        "[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Lock request "
        "time out period exceeded. (1222) (SQLExecDirectW)"
    )

    diagnostic = extract_failure_diagnostic(error, engine_text=True)

    assert diagnostic["native_error_code"] == 1222
    assert diagnostic["engine_message"].startswith("Lock request time out period exceeded.")


def test_engine_message_never_carries_a_row_value_or_a_server_name() -> None:
    # A unique test index that meets duplicates names the duplicate key: row data.
    duplicate = _admin_failure(
        "The CREATE UNIQUE INDEX statement terminated because a duplicate key was found "
        "for the object name 'dbo.Customers' and the index name 'IX_Testing_ab'. The "
        "duplicate key value is (SENTINEL_SURNAME, 48211)."
    )
    conversion = _admin_failure(
        "Conversion failed when converting the nvarchar value 'SENTINEL_VALUE' to data "
        "type int. Server sentinel-prod.database.windows.net at 10.20.30.40."
    )

    first = extract_failure_diagnostic(duplicate, engine_text=True)["engine_message"]
    second = extract_failure_diagnostic(conversion, engine_text=True)["engine_message"]

    assert "SENTINEL" not in first and "48211" not in first
    assert "duplicate key value is [redacted]" in first
    assert "SENTINEL" not in second
    assert "sentinel-prod" not in second and "10.20.30.40" not in second


def test_engine_message_keeps_the_identifier_a_compile_error_names() -> None:
    diagnostic = extract_failure_diagnostic(
        _admin_failure("Invalid column name 'OwnerUserID'."), engine_text=True
    )

    assert diagnostic["engine_message"] == "Invalid column name 'OwnerUserID'."


def test_engine_message_is_capped() -> None:
    diagnostic = extract_failure_diagnostic(_admin_failure("x" * 5000), engine_text=True)

    assert len(diagnostic["engine_message"]) <= 500


def test_engine_text_stays_off_unless_the_caller_asks() -> None:
    # Caller SQL can echo its own literals in an engine message; the default
    # diagnostic is persisted with evidence and stays value-free.
    assert "engine_message" not in extract_failure_diagnostic(_admin_failure(ONLINE_REFUSED))


def test_an_error_that_is_not_engine_text_adds_no_message() -> None:
    diagnostic = extract_failure_diagnostic(
        RuntimeError("Created index definition could not be verified exactly."),
        engine_text=True,
    )

    assert diagnostic == {"diagnostic_code": "sql_execution_failed"}
