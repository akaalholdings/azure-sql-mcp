"""Error contract, module parts (finding 41, 2.6.2 incident log, retry backlog).

An agent recovers from a failure only when the error says what kind it is. A
lost session id that reads as "SQL execution failed" sends the agent to retry
and report a database fault; a syntax error that reads as transient runs a
heavy query four times.
"""

from __future__ import annotations

import pytest

from azure_sql_mcp.index_review import IndexReviewNotFoundError
from azure_sql_mcp.observability import extract_failure_diagnostic
from azure_sql_mcp.observability import is_transient_error
from azure_sql_mcp.observability import native_error_codes
from azure_sql_mcp.observability import not_found_details
from azure_sql_mcp.observability import sanitize_error_message
from azure_sql_mcp.observability import sql_origin
from azure_sql_mcp.observability import tag_sql_origin
from azure_sql_mcp.performance_store import ContractNotFoundError
from azure_sql_mcp.retry import TRANSIENT_ERROR_CODES

SESSION_ID = "tsn_7f3a9c40613b"


# --- finding 41: unknown, expired or other-process ids ------------------------


@pytest.mark.parametrize(
    ("exc", "id_kind"),
    [
        (ContractNotFoundError(f"Unknown TuningSessionV1: {SESSION_ID}."), "tuning_session"),
        (ContractNotFoundError("Unknown PerformanceCaseV1: case_1."), "performance_case"),
        (ContractNotFoundError("Unknown index lease: lease_9."), "index_lease"),
        (IndexReviewNotFoundError("Unknown index review run: run_3."), "index_review_run"),
        (IndexReviewNotFoundError("Review id is not parseable."), "index_review"),
    ],
)
def test_an_unknown_id_is_not_found_with_its_kind_not_a_sql_failure(
    exc: Exception,
    id_kind: str,
) -> None:
    assert extract_failure_diagnostic(exc) == {
        "diagnostic_code": "not_found",
        "id_kind": id_kind,
    }


def test_the_not_found_message_names_the_kind_and_holds_no_id() -> None:
    exc = ContractNotFoundError(f"Unknown TuningSessionV1: {SESSION_ID}.")

    details = not_found_details(exc)

    assert details is not None
    assert details["id_kind"] == "tuning_session"
    assert "tuning session" in details["message"]
    assert SESSION_ID not in details["message"]
    # The agent must stop retrying and must not report a database fault.
    assert "Do not retry" in details["message"]
    # The server sanitizes str(exc). str() of a KeyError is the quoted repr,
    # which the quote redactor used to turn into "'[REDACTED]'".
    sanitized = sanitize_error_message(str(exc))
    assert sanitized == details["message"]


def test_a_wrapped_not_found_error_keeps_its_meaning() -> None:
    try:
        try:
            raise ContractNotFoundError("Unknown TuningCandidateV1: cand_1.")
        except KeyError as inner:
            raise RuntimeError("benchmark step failed") from inner
    except RuntimeError as outer:
        assert extract_failure_diagnostic(outer)["id_kind"] == "tuning_candidate"


def test_an_exception_can_name_its_own_id_kind() -> None:
    class LeaseNotFoundError(KeyError):
        id_kind = "index_lease"

    assert not_found_details(LeaseNotFoundError("abc"))["id_kind"] == "index_lease"  # type: ignore[index]


def test_a_plain_key_error_is_a_defect_not_a_missing_id() -> None:
    # A bare KeyError is a bug in the server; calling it not_found would tell
    # the agent to fix an id that is not wrong.
    assert not_found_details(KeyError("session_id")) is None
    assert extract_failure_diagnostic(KeyError("session_id")) == {
        "diagnostic_code": "sql_execution_failed"
    }


# --- 2.6.2 incident log: identifiers in 207/208 messages ----------------------

ODBC_207 = (
    "('42S22', \"[42S22] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
    "Invalid column name 'CustomerIdd'. (207) (SQLExecDirectW)\")"
)
DDBC_208 = (
    "Driver Error: Base table or view not found; DDBC Error: [Microsoft]"
    "[ODBC Driver 18 for SQL Server][SQL Server]Invalid object name 'dbo.Ordres'."
)


def test_compile_errors_keep_the_identifier_that_is_wrong() -> None:
    # "Invalid column name '[REDACTED]'" cannot be fixed without a second call.
    assert "Invalid column name 'CustomerIdd'" in sanitize_error_message(
        ODBC_207, keep_identifiers=True
    )
    assert "Invalid object name 'dbo.Ordres'" in sanitize_error_message(
        DDBC_208, keep_identifiers=True
    )
    assert 'The multi-part identifier "o.CustomerIdd" could not be bound' in (
        sanitize_error_message(
            'The multi-part identifier "o.CustomerIdd" could not be bound. (4104)',
            keep_identifiers=True,
        )
    )
    assert "Could not find stored procedure 'dbo.usp_Missing'" in sanitize_error_message(
        "Could not find stored procedure 'dbo.usp_Missing'. (2812)", keep_identifiers=True
    )
    assert "near the keyword 'FROM'" in sanitize_error_message(
        "Incorrect syntax near the keyword 'FROM'. (156)", keep_identifiers=True
    )


def test_identifiers_stay_redacted_unless_the_caller_asks() -> None:
    assert "CustomerIdd" not in sanitize_error_message(ODBC_207)


def test_row_values_stay_redacted_when_identifiers_are_kept() -> None:
    message = (
        "Invalid column name 'Total'. Conversion failed when converting the varchar "
        "value 'a@b.example' to data type int. (245)"
    )

    sanitized = sanitize_error_message(message, keep_identifiers=True)

    assert "Invalid column name 'Total'" in sanitized
    assert "a@b.example" not in sanitized
    # A syntax error can quote a piece of a literal, so it is never kept.
    assert "Brien" not in sanitize_error_message(
        "Incorrect syntax near 'Brien'. (102)", keep_identifiers=True
    )
    assert "sqlprod01" not in sanitize_error_message(
        "Invalid object name 'x'. Server=sqlprod01.database.windows.net;", keep_identifiers=True
    )


# --- 2.6.2 incident log: server-built SQL is not the caller's error -----------


def test_the_executor_can_tag_where_failing_sql_came_from() -> None:
    inner = RuntimeError("Invalid column name 'referenced_schema_id'. (207)")
    tag_sql_origin(inner, "server_built")
    try:
        raise ValueError("tool step failed") from inner
    except ValueError as outer:
        assert sql_origin(outer) == "server_built"

    assert sql_origin(RuntimeError("untagged")) is None
    with pytest.raises(ValueError):
        tag_sql_origin(RuntimeError("x"), "somewhere")


# --- backlog: a number in a message is not an error code ----------------------


@pytest.mark.parametrize(
    "message",
    [
        # Deterministic errors whose text happens to hold a transient code.
        "Conversion failed when converting the varchar value '40613' to data type int. (245)",
        "Invalid object name 'dbo.Orders_10054'. (208) (SQLExecDirectW)",
        "Arithmetic overflow error converting numeric to data type numeric: 40501 rows. (8115)",
        "Incorrect syntax near '1205'. (102)",
        "Statement took 10928 ms and returned 233 rows",
    ],
)
def test_a_number_inside_a_message_does_not_make_an_error_transient(message: str) -> None:
    # Retrying a deterministic failure runs a heavy query up to four times.
    assert is_transient_error(Exception(message), TRANSIENT_ERROR_CODES) is False


@pytest.mark.parametrize(
    "exc",
    [
        Exception(
            "[40001] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Transaction "
            "(Process ID 73) was deadlocked on lock resources. (1205) (SQLExecDirectW)"
        ),
        Exception("Database is not currently available. (Error 40613)"),
        Exception("Msg 40501, Level 20, State 1: The service is currently busy."),
        Exception("08S01", 10054),
        Exception(("08S01", "Communication link failure (10054) (SQLExecDirectW)")),
    ],
)
def test_a_reported_transient_code_is_still_transient(exc: Exception) -> None:
    assert is_transient_error(exc, TRANSIENT_ERROR_CODES) is True


def test_native_codes_come_from_reported_positions_only() -> None:
    exc = Exception(
        "[42S22] Invalid column name 'c40613'. (207) (SQLExecDirectW); "
        "[42000] Statement(s) could not be prepared. (8180)"
    )
    try:
        raise RuntimeError("wrapped 1205") from exc
    except RuntimeError as outer:
        assert native_error_codes(outer) == (207, 8180)
