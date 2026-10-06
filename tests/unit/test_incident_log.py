from __future__ import annotations

import asyncio
import copy
import itertools
import json
import linecache
import logging
import os
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import textwrap
import threading
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import anyio
import pytest
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import BaseModel, ConfigDict, ValidationError
from mssql_python.exceptions import DataError, IntegrityError, OperationalError, ProgrammingError

from azure_sql_mcp.connection import AdminBatchOutcomeUnknownError
from azure_sql_mcp.connection import TransactionCommitOutcomeUnknownError
from azure_sql_mcp.database_policy import DatabasePolicyError
from azure_sql_mcp import incident_log
from azure_sql_mcp.equivalence_contract import analyze_equivalence_preflight
from azure_sql_mcp.incident_log import (
    IncidentLog,
    IncidentSettings,
    build_backlog,
    build_scrub_terms,
    describe_exception,
    error_fingerprint,
    note_condition,
    note_exception,
    parse_incident_settings,
    redact_agent_text,
    record_startup_failure,
    redact_text,
    render_markdown,
    resolve_incident_dir,
    summarize_backlog,
)
from azure_sql_mcp.index_review import IndexReviewNotFoundError
from azure_sql_mcp.index_review import IndexReviewSchemaError
from azure_sql_mcp.index_review import SqlIndexHistoryRepository
from azure_sql_mcp.index_review import validate_contract_probe
from azure_sql_mcp.learning_store import LearningStoreError
from azure_sql_mcp.performance_contracts import (
    PerformanceCaseV1,
    PlanActionIntentV1,
    TuningCandidateV1,
    TuningSessionV1,
)
from azure_sql_mcp.performance_store import (
    ContractNotFoundError,
    IdempotencyConflictError,
    PerformanceStore,
    PerformanceStoreError,
)
from azure_sql_mcp.plan_tree import PlanParseError
from azure_sql_mcp.safe_sql import SafeSqlValidator
from azure_sql_mcp.tuning_sessions import InvalidTransitionError
from tests.unit.test_index_history_repository import _Executor as HistoryExecutor
from tests.unit.test_index_history_repository import _policy as history_policy
from tests.unit.test_index_history_repository import _probe_rows as history_probe_rows

# Functions compiled under this module name have azure_sql_mcp frames, exactly
# like package code, so origin rules see a real traceback.
PACKAGE_SITE = "azure_sql_mcp._incident_test_site"


_SOURCES = itertools.count()


def package_function(source: str, name: str, *, padding: int = 0, module: str = PACKAGE_SITE):
    namespace: dict = {"__name__": module, "asyncio": asyncio}
    code = "\n" * padding + textwrap.dedent(source)
    # Like a module on disk, the source stays readable for traceback lines.
    filename = f"<{module}-{next(_SOURCES)}>"
    linecache.cache[filename] = (len(code), None, code.splitlines(True), filename)
    exec(compile(code, filename, "exec"), namespace)
    return namespace[name]


def raised(func, *args) -> BaseException:
    try:
        func(*args)
    except BaseException as exc:  # noqa: BLE001 - the test needs the instance
        return exc
    raise AssertionError("expected an exception")


def tool_error_chain(
    root: BaseException,
    *,
    code: str = "tool_error",
    message: str = "sanitized",
    tool: str = "execute_sql",
) -> ToolError:
    """Rebuild the chain _run_tool and FastMCP Tool.run produce."""

    payload = json.dumps({"code": code, "message": message, "ok": False}, sort_keys=True)
    try:
        try:
            raise root
        except BaseException:
            raise ToolError(payload)
    except ToolError as json_error:
        try:
            raise ToolError(f"Error executing tool {tool}: {json_error}") from json_error
        except ToolError as outer:
            return outer


def test_settings_parse_with_defaults_bounds_and_named_errors() -> None:
    assert parse_incident_settings({}) == IncidentSettings(
        enabled=True, directory=None, retention_days=30, slow_seconds=60
    )
    assert parse_incident_settings({"AZURE_SQL_INCIDENT_LOG": "off"}).enabled is False
    assert parse_incident_settings(
        {"AZURE_SQL_INCIDENT_RETENTION_DAYS": "7", "AZURE_SQL_INCIDENT_SLOW_SECONDS": "20"}
    ) == IncidentSettings(retention_days=7, slow_seconds=20)
    # An optional knob never stops the server: a bad value turns the log off
    # and names the variable.
    for name, value in (
        ("AZURE_SQL_INCIDENT_RETENTION_DAYS", "0"),
        ("AZURE_SQL_INCIDENT_RETENTION_DAYS", "366"),
        ("AZURE_SQL_INCIDENT_SLOW_SECONDS", "4"),
        ("AZURE_SQL_INCIDENT_SLOW_SECONDS", "abc"),
        ("AZURE_SQL_INCIDENT_LOG", "disabled"),
    ):
        settings = parse_incident_settings({name: value, "AZURE_SQL_INCIDENT_DIR": "/x"})
        assert (settings.enabled, settings.invalid, settings.directory) == (False, name, "/x")


def test_incident_dir_follows_state_dir_and_is_off_for_memory_state(tmp_path) -> None:
    assert resolve_incident_dir(IncidentSettings(), ":memory:") is None
    assert resolve_incident_dir(IncidentSettings(), str(tmp_path)) == tmp_path / "incidents"
    explicit = IncidentSettings(directory=str(tmp_path / "custom"))
    assert resolve_incident_dir(explicit, ":memory:") == tmp_path / "custom"


def test_duplicate_key_engine_messages_keep_code_and_skeleton_only() -> None:
    # Real 2601, 2627 and 1505 templates; the row values are not quoted.
    templates = {
        2601: (
            "Cannot insert duplicate key row in object 'dbo.SENTINEL_TABLE' with "
            "unique index 'SENTINEL_INDEX'. The duplicate key value is "
            "(SENTINEL_EMAIL, SENTINEL_NAME)."
        ),
        2627: (
            "Violation of PRIMARY KEY constraint 'SENTINEL_PK'. Cannot insert "
            "duplicate key in object 'dbo.SENTINEL_TABLE'. The duplicate key value "
            "is (SENTINEL_ORDER)."
        ),
        1505: (
            "The CREATE UNIQUE INDEX statement terminated because a duplicate key "
            "was found for the object name 'dbo.SENTINEL_TABLE' and the index name "
            "'IX_Testing_SENTINEL'. The duplicate key value is (SENTINEL_SURNAME, "
            "SENTINEL_GIVEN)."
        ),
    }
    for code, text in templates.items():
        root = IntegrityError(
            "Integrity constraint violation",
            f"[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]{text} ({code})",
        )
        described = describe_exception(tool_error_chain(root))
        blob = json.dumps(described)
        assert "SENTINEL" not in blob
        assert "dbo" not in blob
        assert described["error"]["native_error_code"] == code
        assert described["error"]["class"] == "IntegrityError"
        assert described["error"]["module_of_class"] == "mssql_python.exceptions"
        assert "duplicate key value is" in described["error"]["message"]


def test_redact_text_removes_names_principals_secrets_paths_and_tokens() -> None:
    terms = build_scrub_terms(
        server="sentinel-srv.database.windows.net",
        databases=("SentinelDb",),
        principals=("svc_sentinel", "11111111-2222-3333-4444-555555555555"),
        secrets=("hunter2-sentinel",),
    )
    text = (
        "Login to sentinel-srv.database.windows.net failed for alice@contoso.com "
        "from 10.1.2.3 and fe80::1:2:3:4; Server=tcp:sentinel-srv;Database=SentinelDb;"
        "Pwd=hunter2-sentinel; tenant 72f988bf-86f1-41af-91ab-2d7cd011db47 token "
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl path /srv/sentinel/x.db "
        "and C:\\Sentinel\\x.txt id 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822c "
        "at 0x7f3a2b1c db SentinelDb user svc_sentinel https://sentinel.example/x "
        "[SentinelSchema].[SentinelTable] count 12345\nsecond line SENTINEL_LINE"
    )
    out = redact_text(text, scrub_terms=terms)
    for leaked in (
        "sentinel",
        "Sentinel",
        "alice",
        "contoso",
        "10.1.2.3",
        "fe80",
        "hunter2",
        "72f988bf",
        "eyJ",
        "9f86d081",
        "7f3a2b1c",
        "12345",
        "SENTINEL_LINE",
    ):
        assert leaked not in out, leaked
    assert "[server]" in out
    assert "[email]" in out
    assert "0xADDR" in out
    assert len(redact_text("x " * 500)) <= 300


def test_engine_and_driver_messages_keep_no_names_or_values() -> None:
    # Real engine and driver templates with sentinel names, without scrub terms.
    messages = (
        "Named Pipes Provider: cannot reach sentinel-host.privatelink.database.windows.net",
        "Login failed for user 'sentinel_user@sentinel.example'. (18456)",
        "Cannot open server 'sentinelsrv' requested by the login. Client with IP "
        "address '203.0.113.5' is not allowed to access the server. (40615)",
        "Conversion failed when converting the varchar value 'SentinelValue' to data type int.",
        "String or binary data would be truncated in table 'SentinelDb.dbo.SentinelT', "
        "column 'SentinelCol'. Truncated value: 'SentinelVal'.",
        'The INSERT statement conflicted with the FOREIGN KEY constraint "FK_Sentinel". '
        'The conflict occurred in database "SentinelDb", table "dbo.SentinelT".',
        "Transaction (Process ID 57) was deadlocked on lock resources with another process.",
        "AADSTS700016: Application with identifier 'sentinel-app' was not found in the "
        "directory sentinel.onmicrosoft.com.",
        "SSL Provider: [error:0A000086:SSL routines::sentinel verify failed]",
        "mail sentinel.person@sentinel.example about it",
    )
    for message in messages:
        out = redact_text(message)
        assert "entinel" not in out, out
    assert "AADSTS700016" in redact_text(messages[7])
    key_error = describe_exception(tool_error_chain(KeyError("sentinel_column")))
    assert "sentinel" not in key_error["error"]["message"]


def test_row_values_with_apostrophes_never_survive_redaction(tmp_path) -> None:
    # The engine does not escape quotes inside message parameters, so a value
    # like O'Connor closes a naive quote scan early (real 245, 2628, 8152).
    templates = {
        245: "Conversion failed when converting the nvarchar value "
        "'O'Connor-Whitfield HIV positive' to data type int.",
        2628: "String or binary data would be truncated in table 'SentinelDb.dbo.Patients', "
        "column 'Surname'. Truncated value: 'D'Angelo Margaret'.",
        8152: "String or binary data would be truncated.",
    }
    log = make_log(tmp_path)
    messages = {}
    for code, text in templates.items():
        root = DataError("String data, right truncation", f"[Microsoft][SQL Server]{text} ({code})")
        for sql_arguments in (True, False):
            described = describe_exception(tool_error_chain(root), sql_arguments=sql_arguments)
            assert described["error"]["native_error_code"] == code
            messages[code] = described["error"]["message"]
        log.end(log.begin("execute_sql", {"sql": "x"}), exc=tool_error_chain(root))
    reason = "Read failed: value 'O'Connor-Whitfield' was rejected."
    log.end(
        log.begin("get_wait_stats", {}),
        result={"result_status": "unavailable", "result_status_reason": reason},
    )

    assert messages[245].endswith(
        "Conversion failed when converting the nvarchar value '[REDACTED]' to data type int. (...)"
    )
    assert "would be truncated" in messages[8152]
    exported = build_backlog(
        tmp_path / "incidents", now=FIXED_NOW, min_priority="P4", include_summaries=True
    )
    blob = json.dumps([messages, read_records(tmp_path / "incidents"), exported])
    for private in ("Connor", "Whitfield", "HIV", "Angelo", "Margaret", "Patients", "Surname"):
        assert private not in blob, private


def test_paths_with_spaces_are_removed_whole() -> None:
    # Default Windows work layouts put the user's full name and employer in the path.
    paths = (
        "C:\\Users\\Jane Doe\\OneDrive - Contoso Pharma Ltd\\azure-sql-mcp\\state",
        "C:/Users/Jane Doe/OneDrive - Contoso Pharma Ltd/state",
        "\\\\fileserver01\\Finance Team\\mcp-state",
        "/Users/Jane Doe/OneDrive - Contoso Pharma Ltd/policy.json",
    )
    for path in paths:
        for message in (
            f"Could not secure performance state path {path}.",
            f"could not read database policy file {path}: [Errno 2] No such file: '{path}'",
            f"database policy file {path} is not valid JSON: Expecting value",
        ):
            out = redact_text(message)
            for private in ("Jane", "Doe", "Contoso", "Pharma", "OneDrive", "fileserver", "Finance"):
                assert private not in out, (message, out)
            assert "[path]" in out
    assert redact_text(f"database policy file {paths[0]} is not valid JSON") == (
        "database policy file [path] is not valid JSON"
    )
    described = describe_exception(
        tool_error_chain(PerformanceStoreError(f"Could not secure performance state path {paths[0]}."))
    )
    assert described["error"]["message"] == "Could not secure performance state path [path]"


def test_builtin_programming_error_from_package_keeps_identifier_tokens() -> None:
    explode = package_function(
        """
        def explode(value):
            return value.get("k")
        """,
        "explode",
    )
    described = describe_exception(tool_error_chain(raised(explode, None)))
    assert described["category"] == "product_bug"
    assert described["priority"] == "P1"
    assert described["error"]["message"] == "'NoneType' object has no attribute 'get'"
    assert described["error"]["frame"] == f"{PACKAGE_SITE}:explode"


def test_value_errors_the_interpreter_raises_in_package_code_are_product_bugs() -> None:
    to_int = package_function(
        """
        def to_int(row):
            return int(row["value"])
        """,
        "to_int",
    )
    to_date = package_function(
        """
        def to_date(text):
            from datetime import datetime
            return datetime.fromisoformat(text)
        """,
        "to_date",
    )
    unpack = package_function(
        """
        def unpack(rows):
            first, second = rows
            return first
        """,
        "unpack",
    )
    for exc in (
        raised(to_int, {"value": "1,234"}),
        raised(to_date, "Oct  2 2026 12:00PM"),
        raised(unpack, [1, 2, 3]),
    ):
        described = describe_exception(tool_error_chain(exc))
        assert (described["category"], described["priority"]) == ("product_bug", "P1")
        assert "1,234" not in described["error"]["message"]
        assert "Oct" not in described["error"]["message"]


def test_a_missing_row_key_in_package_code_keeps_a_code_style_key_name(tmp_path) -> None:
    lookup = package_function(
        """
        def lookup(row, key):
            return row[key]
        """,
        "lookup",
    )
    log = make_log(tmp_path)
    for key in ("query_plan_hash", "SentinelPatients"):  # a code alias; a data-shaped name
        log.end(log.begin("get_top_queries", {}), exc=tool_error_chain(raised(lookup, {}, key)))

    messages = [record["error"]["message"] for record in read_records(tmp_path / "incidents")]
    assert messages == ["'query_plan_hash'", "'[REDACTED]'"]
    items = build_backlog(tmp_path / "incidents", now=FIXED_NOW)["items"]
    assert {item["example"]["error"]["message"] for item in items} == set(messages)
    assert any(item["title"].endswith("- 'query_plan_hash'") for item in items)


def test_caller_value_error_redacts_tokens_and_keeps_tsql_function_names() -> None:
    reject = package_function(
        """
        def reject(name):
            raise ValueError(f"Database '{name}' is not in AZURE_SQL_ALLOWED_DATABASES.")
        """,
        "reject",
    )
    described = describe_exception(tool_error_chain(raised(reject, "SentinelDb")))
    assert (described["category"], described["priority"]) == ("caller_error", "P4")
    assert "SentinelDb" not in described["error"]["message"]

    restricted = raised(SafeSqlValidator().validate_read_only, "SELECT xp_cmdshell(1)")
    message = describe_exception(tool_error_chain(restricted))["error"]["message"]
    assert message == "Function 'xp_cmdshell' is not allowed in restricted mode."


def test_agent_text_scrubs_code_spans_dotted_names_urls_and_sql_like_text() -> None:
    out = redact_agent_text(
        "Index on `SentinelSchema.SentinelPII(Email)` failed; see "
        "https://sentinel.example/x and SentinelSchema.SentinelPII again"
    )
    assert "Sentinel" not in out
    assert "[code]" in out and "[ident]" in out and "[url]" in out
    assert redact_agent_text("I ran SELECT Email FROM t WHERE Id = 4") == (
        "[sql-like text withheld]"
    )
    assert redact_agent_text("then select email from customers") == (
        "[sql-like text withheld]"
    )
    plain = "The tool timed out twice while waiting for the pool."
    assert redact_agent_text(plain) == plain
    assert len(redact_agent_text("word " * 300)) <= 500


def test_agent_text_drops_identifier_shaped_names_but_keeps_tool_names() -> None:
    summary = (
        "User asked why oncology visits load slowly; tune_query fails on "
        "PatientDiagnoses filtered by MRN in WhitfieldClinicalDb with IX_Patients_SSN"
    )
    out = redact_agent_text(summary, keep_words={"tune_query"})
    for private in ("PatientDiagnoses", "WhitfieldClinicalDb", "IX_Patients_SSN"):
        assert private not in out
    assert "tune_query fails on [ident]" in out
    assert "ContosoHR" not in redact_agent_text("database ContosoHR not in allowlist")
    # One lower-case clause with a comparison is still SQL.
    assert redact_agent_text(
        "tune_query on query against Customers where LastName = Smith keeps timing out"
    ) == "[sql-like text withheld]"
    plain = "The ROW_NUMBER window in the plan is slow after 3 tries."
    assert redact_agent_text(plain) == "The ROW_NUMBER window in the plan is slow after N tries."


def test_parse_failure_on_construct_keyword_is_product_gap_without_sql() -> None:
    sql = "SELECT SENTINEL_COL FROM SENTINEL_TABLE OPTION (USE HINT ('SENTINEL_HINT'))"
    for root in (
        raised(SafeSqlValidator().validate_read_only, sql),
        raised(analyze_equivalence_preflight, sql),
    ):
        described = describe_exception(tool_error_chain(root), sql_arguments=True)
        assert (described["category"], described["priority"]) == ("product_gap", "P2")
        assert described["error"]["parse"]["keyword"] == "HINT"
        assert "SENTINEL" not in json.dumps(described)


def test_sql_graph_match_is_a_product_gap_without_sql() -> None:
    graph = "SELECT p1.SentinelName FROM SentinelPerson p1, SentinelFriend f, SentinelPerson p2 WHERE MATCH(p1-(f)->p2)"
    root = raised(SafeSqlValidator().validate_read_only, graph)
    described = describe_exception(tool_error_chain(root), sql_arguments=True)
    assert (described["category"], described["priority"]) == ("product_gap", "P2")
    assert described["error"]["parse"]["keyword"] == "MATCH"
    assert "sentinel" not in json.dumps(described).lower()


def test_parse_descriptions_keep_only_tsql_and_parser_words() -> None:
    # The parser upper-cases words it copies from the SQL, and a token's repr
    # carries its comments: any word that is neither goes.
    for sql in (
        "SELECT name FROM dbo.SentinelT OPTION ('Sentinel Mixedcase 078-05-1120' RECOMPILE)",
        "SELECT a FROM CROSS /* step -> <" + "~" * 110 + "> Sentinel Mixedcase salary 250000 */ x",
        "SELECT a FROM t FOR /* Sentinel > Mixedcase */ x",
    ):
        root = raised(SafeSqlValidator().validate_read_only, sql)
        described = describe_exception(tool_error_chain(root), sql_arguments=True)
        blob = json.dumps(described).lower()
        for private in ("sentinel", "mixedcase", "salary", "250000", "1120", "~~"):
            assert private not in blob
    # An option the parser does not know is the caller's unless it is T-SQL.
    root = raised(SafeSqlValidator().validate_read_only, "SELECT a FROM t OPTION (Sentinel RECOMPILE)")
    described = describe_exception(tool_error_chain(root), sql_arguments=True)
    assert (described["category"], described["priority"]) == ("caller_error", "P4")
    assert described["error"]["message"] == "Unknown option [sql]"


def test_parse_failure_on_plain_token_or_incomplete_sql_is_caller_error_without_sql() -> None:
    # SQL Server rejects these too: a missing parenthesis, a clause cut short,
    # or a construct keyword as the very last token is the agent's typo.
    for sql in (
        "SELECT SENTINEL_A SENTINEL_B SENTINEL_C FROM t",
        "SELEC SENTINEL_A FROM t",
        "SELECT * FROM SENTINEL_T WHERE a = 1 OPTION RECOMPILE",
        "SELECT * FROM SENTINEL_T WHERE a = 1 OPTION (RECOMPILE",
        "SELECT * FROM SENTINEL_T OUTER APPLY",
        "SELECT * FROM SENTINEL_T WHERE a LIKE 'x' COLLATE",
        "SELECT SENTINEL_COL FROM SENTINEL_TABLE CROSS APPLY OPENJSON(SENTINEL_COL) AS j PIVOT",
    ):
        root = raised(SafeSqlValidator().validate_read_only, sql)
        described = describe_exception(tool_error_chain(root), sql_arguments=True)
        assert (described["category"], described["priority"]) == ("caller_error", "P4")
        assert described["error"]["parse"].get("keyword") is None
        assert "SENTINEL" not in json.dumps(described)


def test_tokenizer_failure_is_a_caller_error_without_sql() -> None:
    root = raised(analyze_equivalence_preflight, 'SELECT "SENTINEL_A\'SENTINEL_B FROM t')
    described = describe_exception(tool_error_chain(root), sql_arguments=True)
    assert (described["category"], described["priority"]) == ("caller_error", "P4")
    assert described["error"]["message"] == "tokenizer error"
    assert "SENTINEL" not in json.dumps(described)


def test_missing_showplan_shape_survives_redaction() -> None:
    # The shape says why no plan came back; redaction must not blank it.
    error = RuntimeError(
        "No SHOWPLAN XML was returned; result sets: two; values seen: text, null. "
        "Confirm SHOWPLAN access and that the statement is supported."
    )
    message = describe_exception(tool_error_chain(error))["error"]["message"]
    assert "result sets: two; values seen: text, null" in message


def test_query_timeout_is_a_timeout_not_a_transient_error() -> None:
    # Live incident 2026-10-05: a statement that ran for the whole 30-minute
    # query timeout was filed as transient, the label that means "retry".
    ran_out = OperationalError("Timeout expired", "Query timeout expired")
    described = describe_exception(tool_error_chain(ran_out))
    assert (described["category"], described["priority"]) == ("timeout", "P3")
    assert described["error"]["sqlstate"] == "HYT00"
    assert described["error"]["transient"] is False

    login = describe_exception(
        tool_error_chain(OperationalError("Timeout expired", "Login timeout expired"))
    )
    assert login["category"] == "transient"
    assert login["error"]["transient"] is True


def test_driver_errors_classify_by_native_code_sqlstate_and_origin() -> None:
    transient = OperationalError(
        "Communication link failure",
        "[Microsoft][SQL Server]Database 'SENTINEL_DB' on server 'SENTINEL_SRV' "
        "is not currently available. (40613)",
    )
    described = describe_exception(tool_error_chain(transient))
    assert (described["category"], described["priority"]) == ("transient", "P3")
    assert described["error"]["native_error_code"] == 40613
    assert described["error"]["transient"] is True
    assert "SENTINEL" not in json.dumps(described)

    bare = describe_exception(tool_error_chain(OperationalError("Communication link failure", "")))
    assert bare["category"] == "transient"
    assert bare["error"]["sqlstate"] == "08S01"

    login = OperationalError(
        "Invalid authorization specification",
        "[Microsoft][SQL Server]Login failed for user 'SENTINEL_USER'. (18456)",
    )
    assert describe_exception(tool_error_chain(login))["category"] == "environment"

    missing = ProgrammingError(
        "Base table or view not found",
        "[Microsoft][SQL Server]Invalid object name 'SENTINEL_T'. (208)",
    )
    caller = describe_exception(tool_error_chain(missing), sql_arguments=True)
    assert (caller["category"], caller["priority"]) == ("caller_error", "P4")
    server_sql = describe_exception(tool_error_chain(missing), sql_arguments=False)
    assert (server_sql["category"], server_sql["priority"]) == ("product_bug", "P2")

    dmv = ProgrammingError(
        "Base table or view not found",
        "[Microsoft][SQL Server]Invalid object name 'sys.dm_db_resource_stats'. (208)",
    )
    message = describe_exception(tool_error_chain(dmv))["error"]["message"]
    assert "'sys.dm_db_resource_stats'" in message


def test_admin_batch_engine_errors_classify_by_the_driver_cause() -> None:
    def outcome_unknown(cause: Exception) -> AdminBatchOutcomeUnknownError:
        try:
            try:
                raise cause
            except Exception as exc:
                raise AdminBatchOutcomeUnknownError(str(exc)) from exc
        except AdminBatchOutcomeUnknownError as wrapped:
            return wrapped

    typo = ProgrammingError(
        "Syntax error", "[Microsoft][SQL Server]Incorrect syntax near 'SENTINEL_FORM'. (102)"
    )
    described = describe_exception(tool_error_chain(outcome_unknown(typo)), sql_arguments=True)
    assert (described["category"], described["priority"]) == ("caller_error", "P4")
    assert described["error"]["native_error_code"] == 102
    assert "SENTINEL" not in json.dumps(described)

    lost = OperationalError("Communication link failure", "")
    described = describe_exception(tool_error_chain(outcome_unknown(lost)), sql_arguments=True)
    assert (described["category"], described["priority"]) == ("outcome_unknown", "P1")
    assert described["error"]["sqlstate"] == "08S01"


def test_server_built_query_failures_group_by_the_service_that_built_them() -> None:
    execute = package_function(
        """
        def execute(error):
            raise error
        """,
        "execute",
        module="azure_sql_mcp.connection",
    )
    services = [
        package_function(
            f"""
            def {name}(execute, error):
                return execute(error)
            """,
            name,
            module=f"azure_sql_mcp.{module}",
        )
        for name, module in (("get_tempdb_usage", "tempdb_x"), ("get_io_stats", "io_x"))
    ]

    def described(service, error: Exception) -> dict:
        return describe_exception(tool_error_chain(raised(service, execute, error)))

    def column() -> ProgrammingError:
        return ProgrammingError(
            "Column not found", "[Microsoft][SQL Server]Invalid column name 'x'. (207)"
        )

    tempdb, io = (described(service, column()) for service in services)
    assert (tempdb["category"], tempdb["priority"]) == ("product_bug", "P2")
    assert tempdb["error"]["frame"] == "azure_sql_mcp.tempdb_x:get_tempdb_usage"
    assert error_fingerprint(tempdb) != error_fingerprint(io)

    def busy() -> OperationalError:
        return OperationalError(
            "Communication link failure", "[Microsoft][SQL Server]Service busy. (40501)"
        )

    # Transient failures stay one item at the shared executor frame.
    first, second = (described(service, busy()) for service in services)
    assert first["error"]["frame"] == "azure_sql_mcp.connection:execute"
    assert error_fingerprint(first) == error_fingerprint(second)


def test_exception_groups_are_described_by_their_first_real_leaf(tmp_path) -> None:
    explode = package_function(
        """
        def explode():
            raise TypeError("boom")
        """,
        "explode",
    )
    disconnect = BaseExceptionGroup(
        "unhandled errors in a TaskGroup",
        [asyncio.CancelledError(), ExceptionGroup("inner", [BrokenPipeError("stdout closed")])],
    )
    crash = ExceptionGroup("unhandled errors in a TaskGroup", [ExceptionGroup("inner", [raised(explode)])])
    assert describe_exception(disconnect)["error"]["class"] == "BrokenPipeError"
    assert describe_exception(crash)["error"]["class"] == "TypeError"
    assert error_fingerprint(describe_exception(disconnect)) != error_fingerprint(
        describe_exception(crash)
    )

    # mcp's stdio transport answers on a stream anyio closed when stdout broke.
    closed_stream = ExceptionGroup("tg", [ExceptionGroup("tg", [anyio.ClosedResourceError()])])
    log = make_log(tmp_path)
    log.record_process_failure(disconnect, phase="run")
    log.record_process_failure(closed_stream, phase="run")
    log.record_process_failure(crash, phase="run")
    quit_record, closed_record, crash_record = read_records(tmp_path / "incidents")
    assert (quit_record["priority"], quit_record["cause"]) == ("P4", "host_disconnect")
    assert (closed_record["priority"], closed_record["cause"]) == ("P4", "host_disconnect")
    assert (crash_record["priority"], crash_record["error"]["frame"]) == (
        "P1",
        f"{PACKAGE_SITE}:explode",
    )
    assert "cause" not in crash_record


@pytest.mark.asyncio
async def test_missing_index_history_setup_is_environment_but_drift_is_a_bug() -> None:
    no_read = history_probe_rows()
    no_read[4][0]["SelectState"] = 0
    repository = SqlIndexHistoryRepository(HistoryExecutor(no_read), history_policy(allow_write=True))
    with pytest.raises(IndexReviewSchemaError) as denied:
        await repository.probe_contract("appdb")
    missing = raised(validate_contract_probe, [[], [], [], [], []])
    for exc in (missing, denied.value):  # optional setup a DBA has not done
        described = describe_exception(tool_error_chain(exc))
        assert (described["category"], described["priority"]) == ("environment", "P3")
    drifted = history_probe_rows()
    drifted[0][0]["DataType"] = "int"
    described = describe_exception(tool_error_chain(raised(validate_contract_probe, drifted)))
    assert (described["category"], described["priority"]) == ("product_bug", "P2")


def test_classification_table_by_class_and_origin() -> None:
    from_package = package_function(
        """
        def fail(error):
            raise error
        """,
        "fail",
    )

    def classify(error: BaseException) -> tuple[str, str]:
        described = describe_exception(tool_error_chain(raised(from_package, error)))
        return described["category"], described["priority"]

    assert classify(ValueError("bad input")) == ("caller_error", "P4")
    assert classify(TypeError("x")) == ("product_bug", "P1")
    assert classify(KeyError("x")) == ("product_bug", "P1")
    assert classify(IndexReviewNotFoundError("x")) == ("caller_error", "P4")
    assert classify(ContractNotFoundError("x")) == ("caller_error", "P4")
    assert classify(InvalidTransitionError("x")) == ("caller_error", "P4")
    assert classify(IdempotencyConflictError("x")) == ("caller_error", "P4")
    assert classify(TransactionCommitOutcomeUnknownError("x")) == ("outcome_unknown", "P1")
    assert classify(PerformanceStoreError("x")) == ("state_store", "P1")
    assert classify(LearningStoreError("x")) == ("state_store", "P1")
    assert classify(sqlite3.OperationalError("database is locked")) == ("state_store", "P1")
    assert classify(DatabasePolicyError("x")) == ("policy", "P4")
    assert classify(PermissionError("x")) == ("policy", "P4")
    assert classify(NotImplementedError("x")) == ("product_gap", "P2")
    assert classify(PlanParseError("x")) == ("product_gap", "P2")
    assert classify(RuntimeError("x")) == ("product_bug", "P2")

    # A ValueError raised by a dependency, not by package code, is not a caller error.
    third_party = describe_exception(tool_error_chain(raised(int, "not-a-number")))
    assert third_party["category"] != "caller_error"


def test_tool_error_payload_codes_classify_without_root() -> None:
    issues = {
        "code": "invalid_arguments",
        "details": {
            "issues": [
                {"code": "missing", "message": "Field required.", "path": "sql"},
                {"code": "extra_forbidden", "message": "x", "path": "SentinelKey"},
            ]
        },
        "message": "Tool arguments failed validation.",
        "ok": False,
    }
    invalid = ToolError(json.dumps(issues, sort_keys=True))
    described = describe_exception(invalid, declared_keys=("sql", "database_name"))
    assert (described["category"], described["priority"]) == ("caller_error", "P4")
    assert described["error"]["code"] == "invalid_arguments"
    assert described["error"]["validation_issues"] == [
        {"code": "missing", "path": "sql"},
        {"code": "extra_forbidden", "path": "[unknown]"},
    ]
    unknown = describe_exception(ToolError("Unknown tool: get_sentinel_data"))
    assert (unknown["category"], unknown["error"]["code"]) == ("contract_drift", "unknown_tool")
    # The requested name is agent text: it may hold a database or table name.
    assert "sentinel" not in json.dumps(unknown)
    expired = tool_error_chain(TimeoutError("budget"), code="session_expired")
    assert describe_exception(expired)["category"] == "caller_error"


def test_validation_failures_keep_the_payload_message_and_no_input_values() -> None:
    class Arguments(BaseModel):
        model_config = ConfigDict(extra="forbid")
        row_limit: int

    payload = {
        "code": "invalid_arguments",
        "details": {"issues": [{"code": "int_parsing", "path": "row_limit"}]},
        "message": "Tool arguments failed validation.",
        "ok": False,
    }
    try:
        Arguments(row_limit="SENTINEL_VALUE")  # type: ignore[arg-type]
    except ValidationError as validation_error:
        try:
            try:
                raise ToolError(f"Error executing tool x: {validation_error}") from validation_error
            except ToolError:
                raise ToolError(json.dumps(payload)) from None
        except ToolError as final:
            described = describe_exception(final, declared_keys=("row_limit",))
    assert described["error"]["message"] == "Tool arguments failed validation."
    assert described["error"]["validation_issues"] == [{"code": "int_parsing", "path": "row_limit"}]
    assert "SENTINEL" not in json.dumps(described)


def test_bracketed_identifiers_are_never_taken_for_sqlstates() -> None:
    error = ProgrammingError(
        "Base table or view not found",
        "[Microsoft][SQL Server]Invalid object name [SENTL].[Items]. (208)",
    )
    described = describe_exception(tool_error_chain(error))
    assert "SENTL" not in json.dumps(described)
    assert described["error"]["sqlstate"] == "42S02"


def test_timeout_records_suspension_frame_and_stage() -> None:
    waiter = package_function(
        """
        async def wait_forever():
            await asyncio.sleep(30)
        """,
        "wait_forever",
    )

    async def run() -> ToolError:
        try:
            try:
                await asyncio.wait_for(waiter(), timeout=0.01)
            except asyncio.TimeoutError:
                raise ToolError(json.dumps({"code": "timeout", "message": "t", "ok": False}))
        except ToolError as json_error:
            outer = ToolError("Error executing tool get_top_queries")
            outer.__cause__ = json_error
            return outer
        raise AssertionError

    described = describe_exception(asyncio.run(run()))
    assert described["kind"] == "tool_timeout"
    assert (described["category"], described["priority"]) == ("timeout", "P3")
    assert described["where"][-1] == f"{PACKAGE_SITE}:wait_forever"
    assert described["stage"] == "_incident_test_site:wait_forever"


def test_error_fingerprint_ignores_tool_literals_and_lines_but_not_frame() -> None:
    first = package_function(
        """
        def explode(value):
            raise TypeError(f"bad '{value}' at 0x7f00aa {value}")
        """,
        "explode",
    )
    moved = package_function(
        """
        def explode(value):
            raise TypeError(f"bad '{value}' at 0x7f00aa {value}")
        """,
        "explode",
        padding=7,
    )
    other = package_function(
        """
        def other(value):
            raise TypeError(f"bad '{value}' at 0x7f00aa {value}")
        """,
        "other",
    )
    base = describe_exception(tool_error_chain(raised(first, 1), tool="a"))
    same = describe_exception(tool_error_chain(raised(moved, 2), tool="b"))
    different = describe_exception(tool_error_chain(raised(other, 1)))
    assert base["error"]["frame_line"] != same["error"]["frame_line"]
    assert error_fingerprint(base) == error_fingerprint(same)
    assert error_fingerprint(base) != error_fingerprint(different)


# --- private JSONL writer ------------------------------------------------------

FIXED_NOW = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)


def make_log(tmp_path, **kwargs) -> IncidentLog:
    kwargs.setdefault("server_version", "2.6.0")
    kwargs.setdefault("utc_now", lambda: FIXED_NOW)
    return IncidentLog(tmp_path / "incidents", **kwargs)


def read_records(directory) -> list[dict]:
    return [
        json.loads(line)
        for path in sorted(directory.glob("incidents-*.jsonl"))
        for line in path.read_text(encoding="utf-8").splitlines()
    ]


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits")
def test_directory_and_daily_file_are_owner_only(tmp_path) -> None:
    directory = tmp_path / "incidents"
    directory.mkdir()
    os.chmod(directory, 0o755)
    log = make_log(tmp_path)
    log.note(TypeError("x"), site="server.optional_payload")

    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    [path] = directory.glob("incidents-*.jsonl")
    assert path.name == "incidents-2026-10-02.jsonl"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    [record] = read_records(directory)
    assert record["schema"] == "azure-sql-mcp-incident/1"
    assert record["kind"] == "swallowed_exception"
    assert record["site"] == "server.optional_payload"
    assert (record["category"], record["priority"]) == ("product_bug", "P1")
    assert record["runtime"]["server_version"] == "2.6.0"
    assert record["ts_utc"] == "2026-10-02T12:00:00.000Z"


def test_disabled_logs_never_touch_disk(tmp_path) -> None:
    off = IncidentLog.from_settings(
        IncidentSettings(enabled=False), performance_state_dir=str(tmp_path)
    )
    memory = IncidentLog.from_settings(IncidentSettings(), performance_state_dir=":memory:")
    assert off.status()["reason"] == "disabled_by_config"
    assert memory.status()["reason"] == "no_durable_state_dir"
    for log in (off, memory):
        assert log.status()["enabled"] is False
        log.note(TypeError("x"), site="s")
        log.end(log.begin("execute_sql", {}), exc=TypeError("x"))
    assert not (tmp_path / "incidents").exists()


def test_unusable_directory_disables_log_without_raising(tmp_path) -> None:
    (tmp_path / "incidents").write_text("not a directory", encoding="utf-8")
    log = make_log(tmp_path)
    assert log.status() == {
        "enabled": False,
        "reason": "insecure_dir",
        "orphan_detection": "journal",
        "retention_days": 30,
        "slow_seconds": 60,
        "records_written": 0,
        "capped_day": None,
    }
    log.note(TypeError("x"), site="s")


def test_three_write_failures_disable_log_and_never_raise(tmp_path) -> None:
    log = make_log(tmp_path)
    # A directory where today's file belongs makes every open fail for real.
    (tmp_path / "incidents" / "incidents-2026-10-02.jsonl").mkdir()
    for _ in range(2):
        log.note(TypeError("x"), site="s")
    assert log.status()["enabled"] is True
    log.note(TypeError("x"), site="s")
    assert (log.status()["enabled"], log.status()["reason"]) == (False, "write_failed")
    log.note(TypeError("x"), site="s")


def test_daily_byte_cap_is_shared_across_processes_and_marked_once(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(incident_log, "DAY_BYTE_CAP", 4000)
    first, second = make_log(tmp_path), make_log(tmp_path)
    for index in range(40):
        (first if index % 2 else second).note(TypeError(f"x{index}"), site=f"s{index}")
    path = tmp_path / "incidents" / "incidents-2026-10-02.jsonl"
    records = read_records(tmp_path / "incidents")
    markers = [record for record in records if record["kind"] == "log_cap_reached"]
    assert len(markers) == 2  # at most one per process per day
    assert path.stat().st_size <= 4000 + 2 * 1024


def test_routine_p4_noise_leaves_room_for_a_later_p1_and_status_shows_the_cap(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(incident_log, "DAY_BYTE_CAP", 20_000)
    explode = package_function(
        """
        def explode():
            return None.items()
        """,
        "explode",
    )
    log = make_log(tmp_path)
    assert log.status()["capped_day"] is None
    for _ in range(100):  # least-privilege sources, swallowed on every call
        log.note(PermissionError("VIEW DATABASE STATE permission denied"), "database_diagnosis.waits")
    log.end(log.begin("diagnose_database", {}), exc=tool_error_chain(raised(explode)))

    records = read_records(tmp_path / "incidents")
    assert ("tool_error", "P1") in {(record["kind"], record["priority"]) for record in records}
    assert [record["kind"] for record in records].count("log_cap_reached") == 1
    assert (tmp_path / "incidents" / "incidents-2026-10-02.jsonl").stat().st_size <= 20_000
    assert log.status()["capped_day"] == "2026-10-02"


def test_a_report_that_was_not_written_says_why_and_never_coalesces(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(incident_log, "DAY_BYTE_CAP", 1000)
    capped, failing = make_log(tmp_path / "capped"), make_log(tmp_path / "failing")
    (tmp_path / "capped" / "incidents" / "incidents-2026-10-02.jsonl").write_text("x" * 1000)
    (tmp_path / "failing" / "incidents" / "incidents-2026-10-02.jsonl").mkdir()
    for log, reason in ((capped, "day_cap_reached"), (failing, "write_failed")):
        context = fake_context()
        outcomes = [
            log.report_blocker(
                skill="sql-optimizer",
                blocker_kind="tool_hang_or_timeout",
                summary="tune_query hangs twice in a row",
                context=context,
            )
            for _ in range(2)
        ]
        assert [(o["recorded"], o["coalesced"], o["reason"]) for o in outcomes] == [
            (False, False, reason),
            (False, False, reason),
        ]
        assert all(o.get("incident_id") is None for o in outcomes)


def test_prune_removes_old_then_oldest_own_files_but_never_today(tmp_path, monkeypatch) -> None:
    directory = tmp_path / "incidents"
    directory.mkdir()
    expired = directory / "incidents-2026-08-01.jsonl"
    older = directory / "incidents-2026-09-20.jsonl"
    recent = directory / "incidents-2026-09-30.jsonl"
    today = directory / "incidents-2026-10-02.jsonl"
    foreign = directory / "notes.txt"
    for path, size in ((expired, 10), (older, 80), (recent, 80), (today, 150), (foreign, 500)):
        path.write_text("x" * size, encoding="utf-8")

    make_log(tmp_path, retention_days=30).prune()
    assert not expired.exists()
    assert older.exists() and recent.exists() and today.exists() and foreign.exists()

    monkeypatch.setattr(incident_log, "MAX_TOTAL_BYTES", 100)
    make_log(tmp_path).prune()
    assert not older.exists() and not recent.exists()
    assert today.exists() and foreign.exists()


def test_prune_skips_a_file_with_an_invalid_date_and_keeps_going(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(incident_log, "MAX_TOTAL_BYTES", 100)
    directory = tmp_path / "incidents"
    directory.mkdir()
    invalid = directory / "incidents-2026-02-30.jsonl"  # the name matches, the date does not
    expired = directory / "incidents-2026-06-01.jsonl"
    older = directory / "incidents-2026-09-20.jsonl"
    recent = directory / "incidents-2026-09-30.jsonl"
    for path, size in ((invalid, 1), (expired, 1), (older, 80), (recent, 80)):
        path.write_text("x" * size, encoding="utf-8")
    make_log(tmp_path).prune()
    assert [path.exists() for path in (invalid, expired, older, recent)] == [True, False, False, True]


@pytest.mark.skipif(not hasattr(os, "chflags"), reason="needs BSD file flags")
def test_prune_keeps_enforcing_the_size_cap_past_an_undeletable_file(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(incident_log, "MAX_TOTAL_BYTES", 100)
    directory = tmp_path / "incidents"
    directory.mkdir()
    days = [directory / f"incidents-2026-09-{day}.jsonl" for day in range(25, 30)]
    for path in days:
        path.write_text("x" * 80, encoding="utf-8")
    os.chflags(days[0], stat.UF_IMMUTABLE)  # like a file another process holds on Windows
    try:
        make_log(tmp_path).prune()
    finally:
        os.chflags(days[0], 0)
    assert [path.exists() for path in days] == [True, False, False, False, False]


@pytest.mark.skipif(os.name == "nt", reason="an open file cannot be deleted on Windows")
def test_deleting_the_log_while_the_server_runs_recreates_it(tmp_path) -> None:
    directory = tmp_path / "incidents"
    log = make_log(tmp_path)
    log.note(TypeError("x"), site="before")
    (directory / "incidents-2026-10-02.jsonl").unlink()  # the owner clears it after an export
    log.note(TypeError("x"), site="after_file")
    assert [record["site"] for record in read_records(directory)] == ["after_file"]
    shutil.rmtree(directory)
    log.note(TypeError("x"), site="after_dir")
    assert [record["site"] for record in read_records(directory)] == ["after_dir"]
    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert log.status()["records_written"] == 3


# --- tool calls ---------------------------------------------------------------


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class FakeSession:
    """Stands in for an MCP ServerSession: weak-referenceable, has clientInfo."""

    def __init__(self, name: str, version: str) -> None:
        self.client_params = SimpleNamespace(
            clientInfo=SimpleNamespace(name=name, version=version)
        )


def fake_context(name: str = "copilot-cli", version: str = "1.2.3") -> SimpleNamespace:
    return SimpleNamespace(request_context=SimpleNamespace(session=FakeSession(name, version)))


def test_tool_error_record_has_call_and_session_but_no_argument_values(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(
        tmp_path,
        clock=clock,
        declared_for=lambda tool: ("sql", "database_name"),
        budget_for=lambda tool, arguments: 45,
    )
    arguments = {
        "sql": "SELECT SENTINEL_COLUMN FROM t",
        "database_name": "SentinelDb",
        "SentinelKey": "SENTINEL_VALUE",
    }
    call = log.begin("execute_sql", arguments, fake_context(name="copilot<cli>"))
    clock.now += 2.5
    missing = ProgrammingError(
        "Base table or view not found",
        "[Microsoft][SQL Server]Invalid object name 'SENTINEL_T'. (208)",
    )
    log.end(call, exc=tool_error_chain(missing))

    [record] = read_records(tmp_path / "incidents")
    blob = json.dumps(record)
    assert "SENTINEL" not in blob and "Sentinel" not in blob
    assert (record["kind"], record["tool"]) == ("tool_error", "execute_sql")
    assert (record["category"], record["priority"]) == ("caller_error", "P4")
    assert record["error"]["native_error_code"] == 208
    assert record["call"]["argument_keys"] == ["database_name", "sql"]
    assert record["call"]["unknown_argument_count"] == 1
    assert record["call"]["elapsed_ms"] == 2500
    assert record["call"]["budget_s"] == 45
    assert record["session"]["client"] == {"name": "copilotcli", "version": "1.2.3"}
    assert len(record["session"]["session_ref"]) == 12


def test_ok_results_write_nothing_and_are_never_modified(tmp_path) -> None:
    log = make_log(tmp_path)
    result = {"result_status": "ok", "rows": [{"a": 1}]}
    snapshot = copy.deepcopy(result)
    log.end(log.begin("get_top_queries", {}), result=result)
    for status in ("empty", "precondition", "not_supported"):
        log.end(log.begin("get_top_queries", {}), result={"result_status": status})
    assert result == snapshot
    assert read_records(tmp_path / "incidents") == []


def test_unavailable_result_records_degraded_with_redacted_reason(tmp_path) -> None:
    log = make_log(tmp_path)
    result = {
        "result_status": "unavailable",
        "result_status_reason": "VIEW DATABASE STATE permission denied in 'SentinelDb'.",
    }
    log.end(log.begin("get_wait_stats", {}), result=result)
    [record] = read_records(tmp_path / "incidents")
    assert (record["kind"], record["category"], record["priority"]) == (
        "degraded_result",
        "degraded",
        "P4",
    )
    assert record["call"]["result_status"] == "unavailable"
    assert record["reason"] == "VIEW DATABASE STATE permission denied in '[REDACTED]'."


def client_cancelled_context() -> SimpleNamespace:
    """A request the client cancelled with notifications/cancelled."""

    context = fake_context()
    context.request_context.request_id = 7
    context.request_context.session._in_flight = {7: SimpleNamespace(cancelled=True)}
    return context


def test_cancel_priority_follows_elapsed_cause_and_shutdown(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(tmp_path, clock=clock)
    log.end(log.begin("diagnose_database", {}, client_cancelled_context()), exc=asyncio.CancelledError())
    for context in (client_cancelled_context(), fake_context()):
        call = log.begin("diagnose_database", {}, context)
        clock.now += 61
        log.end(call, exc=asyncio.CancelledError())
    log.mark_shutting_down()
    log.end(log.begin("diagnose_database", {}, client_cancelled_context()), exc=asyncio.CancelledError())

    records = read_records(tmp_path / "incidents")
    cancels = [record for record in records if record["kind"] == "tool_cancelled"]
    assert [(r["priority"], r["cause"]) for r in cancels] == [
        ("P4", "client"),
        ("P2", "client"),
        ("P4", "unknown"),
        ("P4", "shutdown"),
    ]
    assert len({record["fingerprint"] for record in cancels}) == 1
    assert [record["kind"] for record in records].count("slow_call") == 2


def test_slow_call_is_recorded_at_end_once_whatever_the_outcome(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(tmp_path, clock=clock)
    call = log.begin("review_workload_indexes", {})
    clock.now += 75
    log.end(call, result={"result_status": "ok"})
    [record] = read_records(tmp_path / "incidents")
    assert (record["kind"], record["category"], record["priority"]) == ("slow_call", "slow", "P3")
    assert record["call"]["elapsed_ms"] == 75000
    assert record["call"]["outcome"] == "ok"
    assert record["call"]["stage"] == "unobserved"


def test_a_slow_call_seen_running_keeps_its_final_duration_and_outcome(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(tmp_path, clock=clock)
    for total in (280, 300, 290):  # the host gave up at 60 s; the server finished
        call = log.begin("review_workload_indexes", {})
        clock.now += 61
        log.tick()
        clock.now += total - 61
        log.end(call, result={"result_status": "ok"})

    [item] = build_backlog(tmp_path / "incidents", now=FIXED_NOW)["items"]
    assert item["count"] == 3
    assert item["elapsed_ms"] == {"p50": 290000, "max": 300000}
    assert "(3x, max 300s)" in item["title"]
    assert item["example"]["outcome"] == "ok"


def test_engine_errors_on_agent_sql_lists_are_caller_errors(tmp_path) -> None:
    log = make_log(tmp_path)
    missing = ProgrammingError(
        "Base table or view not found",
        "[Microsoft][SQL Server]Invalid object name 'SENTINEL_T'. (208)",
    )
    for arguments in ({"queries": ["SELECT n FROM SENTINEL_T"]}, {"query_hints": "OPTION (x)"}):
        log.end(log.begin("analyze_query_indexes", arguments), exc=tool_error_chain(missing))
    assert {(r["category"], r["priority"]) for r in read_records(tmp_path / "incidents")} == {
        ("caller_error", "P4")
    }


def test_one_slow_tool_is_one_item_whether_or_not_a_tick_saw_it(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(tmp_path, clock=clock)

    async def during() -> None:
        clock.now += 61
        await asyncio.to_thread(log.tick)

    run_tool_until_released(log, "execute_sql", during)
    call = log.begin("execute_sql", {})
    clock.now += 62
    log.end(call, result={"result_status": "ok"})

    records = read_records(tmp_path / "incidents")
    assert [(r["call"]["running"], r["call"]["stage"]) for r in records] == [
        (True, "_incident_test_site:wait_here"),
        (False, "_incident_test_site:wait_here"),  # the same call, ended
        (False, "unobserved"),
    ]
    assert len({record["fingerprint"] for record in records}) == 1
    [item] = build_backlog(tmp_path / "incidents", now=FIXED_NOW)["items"]
    assert item["title"].startswith(
        "execute_sql runs past the slow threshold in _incident_test_site:wait_here (2x"
    )


def test_third_identical_failure_in_one_session_records_one_agent_loop(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(tmp_path, clock=clock)
    explode = package_function(
        """
        def explode():
            raise TypeError("boom")
        """,
        "explode",
    )
    looping, other = fake_context(), fake_context()
    for _ in range(4):
        log.end(log.begin("tune_query", {}, looping), exc=tool_error_chain(raised(explode)))
    for _ in range(2):
        log.end(log.begin("tune_query", {}, other), exc=tool_error_chain(raised(explode)))
    records = read_records(tmp_path / "incidents")
    loops = [record for record in records if record["kind"] == "agent_loop"]
    assert len(loops) == 1
    errors = [record for record in records if record["kind"] == "tool_error"]
    assert loops[0]["loop"]["repeat_count"] == 3
    assert loops[0]["loop"]["base_fingerprint"] == errors[0]["fingerprint"]
    assert (loops[0]["category"], loops[0]["priority"]) == ("agent_loop", "P2")

    clock.now += 901
    for _ in range(3):
        log.end(log.begin("tune_query", {}, looping), exc=tool_error_chain(raised(explode)))
    assert [r["kind"] for r in read_records(tmp_path / "incidents")].count("agent_loop") == 2


def test_agent_loops_need_the_same_call_and_skip_retries_and_true_negatives(tmp_path) -> None:
    log = make_log(tmp_path)
    context = fake_context()
    for index in range(3):  # three different queries that fail the same way
        missing = ProgrammingError(
            "Column not found", "[Microsoft][SQL Server]Invalid column name 'c'. (207)"
        )
        call = log.begin("execute_sql", {"sql": f"SELECT c{index} FROM t"}, context)
        log.end(call, exc=tool_error_chain(missing))
    for _ in range(3):  # transient errors are retried as the guidance says
        busy = OperationalError(
            "Communication link failure",
            "[Microsoft][SQL Server]Database 'd' is not currently available. (40613)",
        )
        log.end(log.begin("get_top_queries", {}, context), exc=tool_error_chain(busy))
    for _ in range(3):  # live triage re-samples; empty is a true negative
        call = log.begin("get_currently_waiting_tasks", {}, context)
        log.end(call, result={"result_status": "empty"})
    kinds = [record["kind"] for record in read_records(tmp_path / "incidents")]
    assert "agent_loop" not in kinds


def test_identical_non_ok_calls_record_agent_loop_without_persisting_a_digest(tmp_path) -> None:
    log = make_log(tmp_path)
    context = fake_context()
    for _ in range(3):
        call = log.begin("get_query_store_trend", {"query_id": 4242, "database_name": "SentinelDb"}, context)
        log.end(call, result={"result_status": "precondition"})
    for query_id in (1, 2, 3):
        call = log.begin("get_query_store_trend", {"query_id": query_id}, context)
        log.end(call, result={"result_status": "empty"})
    [record] = read_records(tmp_path / "incidents")
    assert record["kind"] == "agent_loop"
    assert record["loop"]["result_status"] == "precondition"
    blob = json.dumps(record)
    assert "SentinelDb" not in blob and "4242" not in blob and "digest" not in blob


def test_ten_identical_ok_calls_in_a_session_record_a_p3_polling_loop(tmp_path) -> None:
    log = make_log(tmp_path)
    context = fake_context()
    for _ in range(12):
        call = log.begin("get_tuning_session", {"session_id": "session-sentinel"}, context)
        log.end(call, result={"result_status": "ok"})
    for _ in range(12):  # live state is re-sampled on purpose
        log.end(log.begin("get_currently_waiting_tasks", {}, context), result={"result_status": "empty"})
    for index in range(12):  # new arguments each time are progress
        call = log.begin("get_tuning_session", {"session_id": f"session-{index}"}, context)
        log.end(call, result={"result_status": "ok"})

    [record] = read_records(tmp_path / "incidents")
    assert (record["kind"], record["tool"], record["priority"]) == (
        "agent_loop",
        "get_tuning_session",
        "P3",
    )
    assert (record["loop"]["repeat_count"], record["loop"]["result_status"]) == (10, "ok")
    assert "sentinel" not in json.dumps(record)


def test_hot_path_never_fsyncs_or_opens_sqlite(tmp_path, monkeypatch) -> None:
    calls: list[str] = []

    def forbidden(name):
        def fail(*args, **kwargs):
            calls.append(name)
            raise AssertionError(f"{name} on the hot path")

        return fail

    monkeypatch.setattr(os, "fsync", forbidden("fsync"))
    monkeypatch.setattr(sqlite3, "connect", forbidden("sqlite3.connect"))
    clock = FakeClock()
    log = make_log(tmp_path, clock=clock)
    log.end(log.begin("execute_sql", {"sql": "SELECT 1"}), exc=tool_error_chain(TypeError("x")))
    log.end(log.begin("get_wait_stats", {}), result={"result_status": "unavailable"})
    slow = log.begin("diagnose_database", {})
    clock.now += 90
    log.end(slow, result={"result_status": "ok"})
    log.note(TypeError("y"), site="server.optional_payload")
    assert calls == []
    assert len(read_records(tmp_path / "incidents")) == 4


def test_note_exception_uses_the_current_call_and_is_quiet_for_driver_errors(tmp_path) -> None:
    note_exception(TypeError("x"), "server.optional_payload")  # no log active: no-op
    log = make_log(tmp_path)
    call = log.begin("diagnose_database", {})
    note_exception(OperationalError("Communication link failure", ""), "database_diagnosis.gather")
    note_condition("connection_pool.circuit_breaker_open")
    log.end(call, result={"result_status": "ok"})
    note_exception(TypeError("x"), "server.optional_payload")  # call ended: no-op
    swallowed, condition = read_records(tmp_path / "incidents")
    assert (swallowed["kind"], swallowed["tool"]) == ("swallowed_exception", "diagnose_database")
    assert (swallowed["category"], swallowed["priority"]) == ("transient", "P4")
    assert swallowed["site"] == "database_diagnosis.gather"
    assert (condition["kind"], condition["site"]) == (
        "condition",
        "connection_pool.circuit_breaker_open",
    )
    assert (condition["category"], condition["priority"]) == ("environment", "P3")


def test_logger_exception_in_package_becomes_swallowed_exception(tmp_path) -> None:
    log = make_log(tmp_path, tick_seconds=60)
    log.start()
    package_logger = logging.getLogger("azure_sql_mcp.connection")
    try:
        try:
            raise RuntimeError("discard failed")
        except RuntimeError:
            package_logger.exception("Failed to discard timed-out connection %s", "SENTINEL_ARG")
            package_logger.error("No exception attached")
            logging.getLogger("azure_sql_mcp.incident_log").exception("own logger")
    finally:
        log.close()
    try:
        raise RuntimeError("after close")
    except RuntimeError:
        package_logger.exception("not captured")
    [record] = read_records(tmp_path / "incidents")
    assert record["kind"] == "swallowed_exception"
    assert record["site"] == "log:azure_sql_mcp.connection"
    assert record["log_template"] == "Failed to discard timed-out connection %s"
    assert "SENTINEL_ARG" not in json.dumps(record)


def test_startup_failure_resolves_dir_from_env_and_never_raises(tmp_path) -> None:
    fail = package_function(
        """
        def load(server):
            raise ValueError(f"AZURE_SQL_SERVER {server} is not reachable.")
        """,
        "load",
    )
    exc = raised(fail, "sentinel-srv.database.windows.net")
    env = {
        "AZURE_SQL_PERFORMANCE_STATE_DIR": str(tmp_path),
        "AZURE_SQL_SERVER": "sentinel-srv.database.windows.net",
    }
    record_startup_failure(exc, environ=env)
    record_startup_failure(exc, environ={**env, "AZURE_SQL_INCIDENT_LOG": "off"})
    record_startup_failure(exc, environ={"AZURE_SQL_PERFORMANCE_STATE_DIR": ":memory:"})
    record_startup_failure(KeyboardInterrupt(), environ=env)
    # A bad incident setting turns the log off here too, as in the server.
    record_startup_failure(exc, environ={**env, "AZURE_SQL_INCIDENT_RETENTION_DAYS": "bogus"})
    records = read_records(tmp_path / "incidents")
    assert len(records) == 1
    assert {record["kind"] for record in records} == {"startup_failure"}
    assert records[0]["priority"] == "P2"
    assert records[0]["phase"] == "startup"
    assert "sentinel" not in json.dumps(records)


# --- watchdog, liveness journal, orphans --------------------------------------


def run_tool_until_released(log: IncidentLog, tool: str, during) -> None:
    """Run one call parked in a package coroutine while `during` runs."""

    wait_here = package_function(
        """
        async def wait_here(event):
            await event.wait()
        """,
        "wait_here",
    )

    async def scenario() -> None:
        event = asyncio.Event()

        async def tool_call() -> None:
            call = log.begin(tool, {})
            await wait_here(event)
            log.end(call, result={"result_status": "ok"})

        task = asyncio.create_task(tool_call())
        await asyncio.sleep(0)
        await during()
        event.set()
        await task

    asyncio.run(scenario())


def test_watchdog_records_slow_once_while_running_with_await_frames(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(tmp_path, clock=clock)

    async def during() -> None:
        clock.now += 61
        await asyncio.to_thread(log.tick)
        await asyncio.to_thread(log.tick)

    run_tool_until_released(log, "diagnose_database", during)
    record, final = read_records(tmp_path / "incidents")
    assert (record["kind"], record["priority"]) == ("slow_call", "P3")
    assert record["call"]["running"] is True
    assert record["call"]["where"][-1] == f"{PACKAGE_SITE}:wait_here"
    assert record["call"]["stage"] == "_incident_test_site:wait_here"
    assert (final["call"]["running"], final["call"]["outcome"], final["call"]["stage"]) == (
        False,
        "ok",
        "_incident_test_site:wait_here",
    )


def test_a_call_ending_during_a_tick_capture_gets_one_slow_record(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(tmp_path, clock=clock)
    call = log.begin("execute_sql", {})
    clock.now += 61
    real_capture = log._capture_where

    def capture_while_the_call_ends(target):
        where = real_capture(target)
        log.end(target, result={"result_status": "ok"})  # the loop finishes it meanwhile
        return where

    log._capture_where = capture_while_the_call_ends  # type: ignore[method-assign]
    log.tick()
    assert call.slow_flagged is True
    assert [record["kind"] for record in read_records(tmp_path / "incidents")] == ["slow_call"]


def test_call_past_budget_and_grace_records_one_stuck_call(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(tmp_path, clock=clock, budget_for=lambda tool, arguments: 10)

    async def during() -> None:
        clock.now += 39
        await asyncio.to_thread(log.tick)
        clock.now += 1
        await asyncio.to_thread(log.tick)
        await asyncio.to_thread(log.tick)

    run_tool_until_released(log, "get_top_queries", during)
    [record] = read_records(tmp_path / "incidents")
    assert (record["kind"], record["category"], record["priority"]) == ("stuck_call", "stuck", "P1")
    assert record["call"]["budget_s"] == 10
    assert record["call"]["past_budget"] is True
    assert record["call"]["where"][-1] == f"{PACKAGE_SITE}:wait_here"


def test_blocked_event_loop_is_recorded_with_loop_thread_frames(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(tmp_path, clock=clock, capture_timeout=0.2)
    block = package_function(
        """
        def block(seconds):
            import time
            time.sleep(seconds)
        """,
        "block",
    )

    async def scenario() -> None:
        call = log.begin("get_wait_stats", {})
        clock.now += 61
        watchdog = threading.Thread(target=log.tick)
        watchdog.start()
        block(1.0)  # the loop thread is busy, so the await-chain capture cannot run
        watchdog.join()
        log.end(call, result={"result_status": "ok"})

    asyncio.run(scenario())
    [record] = [r for r in read_records(tmp_path / "incidents") if r["kind"] != "slow_call"]
    assert (record["kind"], record["category"], record["priority"]) == (
        "event_loop_blocked",
        "stuck",
        "P1",
    )
    assert record["call"]["where"][-1] == f"{PACKAGE_SITE}:block"


def test_close_holding_the_loop_is_not_recorded_as_a_blocked_loop(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(
        tmp_path,
        clock=clock,
        budget_for=lambda tool, arguments: 0,
        tick_seconds=0.01,
        capture_timeout=1.0,
    )

    async def scenario() -> None:
        log.begin("get_wait_stats", {})
        clock.now += 61
        log.start()
        time.sleep(0.2)  # the first tick is now waiting on this loop
        log.close()  # joins the watchdog on the loop thread

    asyncio.run(scenario())
    kinds = {record["kind"] for record in read_records(tmp_path / "incidents")}
    assert "stuck_call" in kinds  # the tick ran while close() held the loop
    assert "event_loop_blocked" not in kinds


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits")
def test_journal_holds_live_calls_only_and_is_removed_when_idle(tmp_path) -> None:
    log = make_log(tmp_path)
    call = log.begin("tune_query", {"sql": "SELECT SENTINEL_COLUMN"})
    log.tick()
    [journal] = (tmp_path / "incidents").glob("inflight-*.json")
    assert stat.S_IMODE(journal.stat().st_mode) == 0o600
    text = journal.read_text(encoding="utf-8")
    assert "SENTINEL" not in text
    assert [item["tool"] for item in json.loads(text)["calls"]] == ["tune_query"]
    log.end(call, result={"result_status": "ok"})
    log.tick()
    assert list((tmp_path / "incidents").glob("inflight-*")) == []


def write_journal(directory, *, pid: int, run_id: str, calls, shutting_down=False, age_s=600):
    path = directory / f"inflight-{pid}-{run_id}.json"
    payload = {
        "schema": "azure-sql-mcp-inflight/1",
        "pid": pid,
        "run_id": run_id,
        "server_version": "2.5.9",
        "written_utc": "2026-10-02T11:50:00.000Z",
        "shutting_down": shutting_down,
        "calls": calls,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    moment = time.time() - age_s
    os.utime(path, (moment, moment))
    return path


def dead_pid() -> int:
    process = subprocess.Popen([sys.executable, "-c", "pass"])
    process.wait()
    return process.pid


def journal_call(tool: str, elapsed_s: float) -> dict:
    return {
        "call_id": "deadbeef-1",
        "tool": tool,
        "started_utc": "2026-10-02T11:40:00.000Z",
        "elapsed_s": elapsed_s,
    }


@pytest.mark.skipif(os.name == "nt", reason="process liveness check is POSIX only")
def test_dead_process_journals_become_orphans_by_age_and_are_removed(tmp_path) -> None:
    directory = tmp_path / "incidents"
    directory.mkdir()
    pid = dead_pid()
    crashed = write_journal(
        directory,
        pid=pid,
        run_id="deadbeef",
        calls=[journal_call("benchmark_tuning_candidate", 300), journal_call("get_top_queries", 2)],
    )
    stopping = write_journal(
        directory,
        pid=pid,
        run_id="cafebabe",
        calls=[journal_call("tune_query", 400)],
        shutting_down=True,
    )
    alive = write_journal(directory, pid=os.getpid(), run_id="0badf00d", calls=[journal_call("x", 1)])
    fresh = write_journal(directory, pid=pid, run_id="feedface", calls=[journal_call("x", 1)], age_s=5)

    log = make_log(tmp_path, tick_seconds=60)
    log.start()
    log.close()
    orphans = {
        record["tool"]: record
        for record in read_records(directory)
        if record["kind"] == "orphaned_call"
    }
    assert {tool: (r["priority"], r["reason"]) for tool, r in orphans.items()} == {
        "benchmark_tuning_candidate": ("P2", "process_exit"),
        "get_top_queries": ("P4", "process_exit"),
        "tune_query": ("P4", "shutdown"),
    }
    assert orphans["get_top_queries"]["orphan"]["server_version"] == "2.5.9"
    assert not crashed.exists() and not stopping.exists()
    assert alive.exists() and fresh.exists()


@pytest.mark.skipif(os.name == "nt", reason="process liveness check is POSIX only")
def test_a_malformed_dead_journal_never_stops_the_watchdog(tmp_path) -> None:
    directory = tmp_path / "incidents"
    directory.mkdir()
    pid = dead_pid()
    wrong_shape = write_journal(directory, pid=pid, run_id="deadbeef", calls=5)
    deep = write_journal(directory, pid=pid, run_id="cafebabe", calls=[])
    deep.write_text("[" * 100_000 + "]" * 100_000, encoding="utf-8")
    huge_pid = write_journal(directory, pid=10**30, run_id="0badf00d", calls=[])
    for path in (deep, huge_pid):
        os.utime(path, (time.time() - 600, time.time() - 600))
    write_journal(directory, pid=pid, run_id="feedface", calls=[journal_call("tune_query", 300)])

    log = make_log(tmp_path, tick_seconds=60)
    log.start()
    try:
        assert log._thread is not None and log._thread.is_alive()
        assert log._handler is not None
    finally:
        log.close()
    assert not wrong_shape.exists()
    orphans = [r for r in read_records(directory) if r["kind"] == "orphaned_call"]
    assert [record["tool"] for record in orphans] == ["tune_query"]


@pytest.mark.skipif(os.name == "nt", reason="process liveness check is POSIX only")
def test_two_processes_starting_together_record_each_orphan_once(tmp_path, monkeypatch) -> None:
    directory = tmp_path / "incidents"
    directory.mkdir()
    calls = [journal_call(tool, 90) for tool in ("tune_query", "get_top_queries", "diagnose_database")]
    write_journal(directory, pid=dead_pid(), run_id="deadbeef", calls=calls)
    both_listed = threading.Barrier(2)
    real_dead_journals = incident_log.dead_journals

    def listed_by_both(*args, **kwargs):
        found = real_dead_journals(*args, **kwargs)
        both_listed.wait(timeout=5)  # neither removes the journal before both read it
        return found

    monkeypatch.setattr(incident_log, "dead_journals", listed_by_both)
    starters = [threading.Thread(target=make_log(tmp_path)._reconcile_orphans) for _ in range(2)]
    for thread in starters:
        thread.start()
    for thread in starters:
        thread.join(timeout=10)

    orphans = [r for r in read_records(directory) if r["kind"] == "orphaned_call"]
    assert sorted(record["tool"] for record in orphans) == sorted(c["tool"] for c in calls)
    assert list(directory.glob("inflight-*")) == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX signals")
def test_sigterm_journals_open_calls_as_shutdown_then_kills_as_before(tmp_path) -> None:
    directory = tmp_path / "incidents"
    script = textwrap.dedent(
        """
        import asyncio, os, signal, sys
        from pathlib import Path
        from azure_sql_mcp.incident_log import IncidentLog

        async def main():
            log = IncidentLog(Path(sys.argv[1]), tick_seconds=60)
            log.start()
            log.begin("tune_query", {})
            os.kill(os.getpid(), signal.SIGTERM)
            await asyncio.sleep(10)

        asyncio.run(main())
        """
    )
    completed = subprocess.run([sys.executable, "-c", script, str(directory)], timeout=60)
    assert completed.returncode == -signal.SIGTERM
    [journal] = directory.glob("inflight-*.json")
    payload = json.loads(journal.read_text(encoding="utf-8"))
    assert payload["shutting_down"] is True
    assert [item["tool"] for item in payload["calls"]] == ["tune_query"]

    os.utime(journal, (time.time() - 600, time.time() - 600))
    log = make_log(tmp_path, tick_seconds=60)
    log.start()
    log.close()
    [orphan] = [r for r in read_records(directory) if r["kind"] == "orphaned_call"]
    assert (orphan["reason"], orphan["priority"]) == ("shutdown", "P4")
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_DFL


@pytest.mark.skipif(os.name == "nt", reason="POSIX signals")
@pytest.mark.parametrize("signal_name", ["SIGTERM", "SIGKILL"])
def test_a_journaled_call_that_finished_is_never_an_orphan(tmp_path, signal_name) -> None:
    directory = tmp_path / "incidents"
    script = textwrap.dedent(
        """
        import asyncio, os, signal, sys
        from pathlib import Path
        from azure_sql_mcp.incident_log import IncidentLog

        async def main():
            log = IncidentLog(Path(sys.argv[1]), tick_seconds=60)
            log.start()
            call = log.begin("diagnose_database", {})
            log.tick()  # the call lives through a tick, so it is journaled
            log.end(call, result={"result_status": "ok"})
            os.kill(os.getpid(), getattr(signal, sys.argv[2]))
            await asyncio.sleep(10)

        asyncio.run(main())
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script, str(directory), signal_name], timeout=60
    )
    assert completed.returncode == -getattr(signal, signal_name)
    assert list(directory.glob("inflight-*")) == []
    log = make_log(tmp_path, tick_seconds=60)
    log.start()
    log.close()
    assert [r for r in read_records(directory) if r["kind"] == "orphaned_call"] == []


def test_sigterm_refreshes_the_journal_even_with_no_call_open(tmp_path) -> None:
    log = make_log(tmp_path)
    log.begin("tune_query", {})
    log.tick()
    with log._state_lock:  # the call left without end(), as a cancelled task can
        log._inflight.clear()
    log._previous_sigterm = signal.SIG_IGN
    previous = signal.signal(signal.SIGTERM, signal.SIG_IGN)
    try:
        log._on_sigterm(signal.SIGTERM, None)
    finally:
        signal.signal(signal.SIGTERM, previous)
    assert list((tmp_path / "incidents").glob("inflight-*")) == []


def test_close_records_open_calls_as_shutdown_orphans_and_removes_journal(tmp_path) -> None:
    log = make_log(tmp_path, tick_seconds=60)
    log.start()
    log.begin("tune_query", {})
    log.tick()
    assert list((tmp_path / "incidents").glob("inflight-*.json"))
    log.close()
    [record] = read_records(tmp_path / "incidents")
    assert (record["kind"], record["reason"], record["priority"]) == (
        "orphaned_call",
        "shutdown",
        "P4",
    )
    assert list((tmp_path / "incidents").glob("inflight-*")) == []


def test_tick_prunes_after_day_rollover(tmp_path) -> None:
    now = {"value": FIXED_NOW}
    log = make_log(tmp_path, utc_now=lambda: now["value"], retention_days=2)
    log.tick()
    stale = tmp_path / "incidents" / "incidents-2026-09-30.jsonl"
    stale.write_text("{}\n", encoding="utf-8")
    now["value"] = FIXED_NOW + timedelta(hours=11)
    log.tick()
    assert stale.exists()  # same day: no second prune
    now["value"] = FIXED_NOW + timedelta(days=1)
    log.tick()
    assert not stale.exists()  # 2026-10-03 keeps two days back to 2026-10-01


def test_watchdog_thread_ticks_until_close(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(tmp_path, clock=clock, tick_seconds=0.05)
    log.begin("diagnose_database", {})
    clock.now += 61
    log.start()
    path = tmp_path / "incidents" / "incidents-2026-10-02.jsonl"
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not (path.exists() and path.stat().st_size):
        time.sleep(0.05)
    log.close()
    assert [record["kind"] for record in read_records(tmp_path / "incidents")] == [
        "slow_call",
        "orphaned_call",
    ]
    assert not any(
        thread.name == "azure-sql-mcp-incident-watchdog" for thread in threading.enumerate()
    )


# --- agent reports --------------------------------------------------------------


def test_report_blocker_records_redacted_report_and_flags_version_mismatch(tmp_path) -> None:
    log = make_log(tmp_path)
    context = fake_context()
    log.end(log.begin("tune_query", {}, context), exc=tool_error_chain(TypeError("x")))
    session_id = "session-" + "a" * 32
    result = log.report_blocker(
        skill="sql-optimizer",
        skill_version="2.4.0",
        skill_version_expected="2.5.0",
        blocker_kind="skill_tool_contradiction",
        last_tool="tune_query",
        summary="Skill says pass `SentinelSchema.SentinelPII` but tune_query rejects it",
        attempts=2,
        related_ids=[session_id, "SentinelSchema.SentinelPII", "b" * 32],
        context=context,
    )
    assert result["recorded"] is True
    assert (result["coalesced"], result["skill_version_mismatch"]) == (False, True)
    assert "Sentinel" not in result["summary"]
    error_record, report_record = read_records(tmp_path / "incidents")
    assert report_record["incident_id"] == result["incident_id"]
    assert (report_record["kind"], report_record["priority"]) == ("agent_report", "P2")
    report = report_record["report"]
    assert report["related_ids"] == [session_id, "b" * 32]
    assert report["recent_fingerprints"] == [error_record["fingerprint"]]
    assert "Sentinel" not in json.dumps(report_record)


def test_report_blocker_coalesces_repeats_and_rate_limits_a_session(tmp_path) -> None:
    clock = FakeClock()
    log = make_log(tmp_path, clock=clock)
    context = fake_context()

    def report(last_tool: str = "tune_query") -> dict:
        return log.report_blocker(
            skill="sql-optimizer",
            blocker_kind="tool_hang_or_timeout",
            last_tool=last_tool,
            summary="tune_query hangs twice in a row",
            context=context,
        )

    first = report()
    assert first["recorded"] is True
    repeat = report()
    assert (repeat["recorded"], repeat["coalesced"]) == (False, True)
    assert repeat["incident_id"] == first["incident_id"]
    clock.now += 601
    assert report()["recorded"] is True
    outcomes = [report(f"tool_{index}")["recorded"] for index in range(30)]
    assert outcomes.count(True) == 18  # 20 reports per session per hour
    assert report("tool_new")["reason"] == "rate_limited"


def test_report_without_a_skill_version_is_unknown_not_a_mismatch(tmp_path) -> None:
    log = make_log(tmp_path)
    result = log.report_blocker(
        skill="sql-optimizer",
        skill_version_expected="2.5.0",
        blocker_kind="repeated_tool_failure",
        summary="benchmark_tuning_candidate keeps failing the same way.",
    )
    assert result["skill_version_mismatch"] is False
    [record] = read_records(tmp_path / "incidents")
    assert record["report"]["skill_version_mismatch"] is False


def test_from_config_scrubs_configured_names_counts_writes_and_follows_the_switch(
    tmp_path, server_config_factory
) -> None:
    config = server_config_factory(
        server="sentinelsrv.database.windows.net",
        default_database="SentinelDb",
        allowed_databases=("SentinelDb",),
        username="sentinel_user",
        performance_state_dir=str(tmp_path / "state"),
    )
    log = IncidentLog.from_config(config, server_version="2.6.0")
    assert log.directory == tmp_path / "state" / "incidents"
    assert log.status()["records_written"] == 0

    call = log.begin("list_databases", {})
    log.end(call, exc=OSError("cannot reach sentinelsrv for SentinelDb as sentinel_user"))

    [record] = read_records(log.directory)
    assert "sentinel" not in json.dumps(record).lower()
    assert (record["runtime"]["transport"], record["runtime"]["profile"]) == ("stdio", None)
    assert log.status()["records_written"] == 1
    assert IncidentLog.from_config(
        replace(config, incident=IncidentSettings(enabled=False))
    ).status()["reason"] == "disabled_by_config"
    assert IncidentLog.from_config(
        replace(config, performance_state_dir=":memory:")
    ).status()["reason"] == "no_durable_state_dir"


def test_backlog_from_the_log_scrubs_configured_names_from_old_records(
    tmp_path, server_config_factory
) -> None:
    config = server_config_factory(
        server="sentinelsrv.database.windows.net",
        performance_state_dir=str(tmp_path / "state"),
    )
    log = IncidentLog.from_config(config, server_version="2.6.0")
    now = datetime.now(timezone.utc)
    legacy = {
        "schema": "azure-sql-mcp-incident/1",
        "ts_utc": now.isoformat().replace("+00:00", "Z"),
        "kind": "tool_error",
        "category": "product_bug",
        "priority": "P1",
        "fingerprint": "0" * 16,
        "tool": "execute_sql",
        "error": {"class": "TypeError", "message": "sentinelsrv refused"},
    }
    day_file = log.directory / f"incidents-{now.date().isoformat()}.jsonl"
    day_file.write_text(json.dumps(legacy) + "\n", encoding="utf-8")

    backlog = log.backlog(performance_db=None, min_priority="P4")

    [item] = backlog["items"]
    assert item["example"]["error"]["message"] == "[server] refused"
    assert "sentinelsrv" not in json.dumps(backlog)


def test_report_blocker_is_unrecorded_not_an_error_when_log_is_off() -> None:
    off = IncidentLog(None, disabled_reason="disabled_by_config")
    assert off.report_blocker(skill="sql-optimizer", blocker_kind="other", summary="x" * 20) == {
        "recorded": False,
        "reason": "disabled_by_config",
    }


# --- backlog -----------------------------------------------------------------------


def test_backlog_groups_one_root_cause_across_tools_versions_and_sessions(tmp_path) -> None:
    explode = package_function(
        """
        def explode():
            raise TypeError("boom")
        """,
        "explode",
    )
    old = make_log(tmp_path, server_version="2.5.0", utc_now=lambda: FIXED_NOW - timedelta(days=3))
    new = make_log(tmp_path, server_version="2.6.0", utc_now=lambda: FIXED_NOW - timedelta(days=1))
    expired = make_log(tmp_path, utc_now=lambda: FIXED_NOW - timedelta(days=20))
    old.end(old.begin("tune_query", {}, fake_context()), exc=tool_error_chain(raised(explode)))
    new.end(
        new.begin("collect_performance_evidence", {}, fake_context(name="codex")),
        exc=tool_error_chain(raised(explode)),
    )
    new.end(new.begin("tune_query", {}, fake_context()), exc=tool_error_chain(raised(explode)))
    expired.end(expired.begin("tune_query", {}), exc=tool_error_chain(raised(explode)))

    backlog = build_backlog(tmp_path / "incidents", now=FIXED_NOW)
    assert backlog["schema"] == "azure-sql-mcp-incident-backlog/1"
    assert backlog["window"]["days"] == 14
    assert backlog["totals"]["incidents"] == 3
    [item] = backlog["items"]
    assert item["count"] == 3
    assert item["sessions"] == 3
    assert item["tools"] == ["collect_performance_evidence", "tune_query"]
    assert item["server_versions"] == ["2.5.0", "2.6.0"]
    assert item["clients"] == ["codex 1.2.3", "copilot-cli 1.2.3"]
    assert item["first_seen_utc"] == "2026-09-29T12:00:00.000Z"
    assert item["last_seen_utc"] == "2026-10-01T12:00:00.000Z"
    assert item["title"] == f"2 tools: TypeError in {PACKAGE_SITE}:explode - boom"
    assert item["labels"] == ["incident", "category:product_bug", "priority:P1"]
    assert item["example"]["error"]["frame"] == f"{PACKAGE_SITE}:explode"
    blob = json.dumps(backlog)
    for private in ("session_ref", "call_id", "run_id", "incident_id", '"pid"'):
        assert private not in blob


def test_default_export_shows_one_type_error_not_fifty_caller_errors(tmp_path) -> None:
    reject = package_function(
        """
        def reject(name):
            raise ValueError(f"Database '{name}' is not in AZURE_SQL_ALLOWED_DATABASES.")
        """,
        "reject",
    )
    log = make_log(tmp_path)
    context = fake_context()  # one agent session trying fifty databases
    for index in range(50):
        call = log.begin("get_top_queries", {"database_name": f"db{index}"}, context)
        log.end(call, exc=tool_error_chain(raised(reject, f"db{index}")))
    log.end(log.begin("get_top_queries", {}), exc=tool_error_chain(TypeError("x")))

    backlog = build_backlog(tmp_path / "incidents", now=FIXED_NOW)
    assert [item["example"]["error"]["class"] for item in backlog["items"]] == ["TypeError"]
    assert backlog["totals"]["below_min_priority"] == 1
    everything = build_backlog(tmp_path / "incidents", now=FIXED_NOW, min_priority="P4")
    caller = [item for item in everything["items"] if item["category"] == "caller_error"]
    assert [(item["count"], item["priority"]) for item in caller] == [(50, "P4")]


def test_volume_across_sessions_escalates_one_level(tmp_path) -> None:
    log = make_log(tmp_path)
    for _ in range(3):
        call = log.begin("get_top_queries", {}, fake_context())
        log.end(call, exc=tool_error_chain(TimeoutError(), code="timeout"))
    [item] = build_backlog(tmp_path / "incidents", now=FIXED_NOW)["items"]
    assert (item["category"], item["priority"]) == ("timeout", "P2")
    assert item["title"] == "get_top_queries times out in an unobserved stage (3x, max 0s)"


def test_p4_items_never_escalate_into_the_default_export(tmp_path) -> None:
    log = make_log(tmp_path)
    for _ in range(3):
        context = fake_context()
        result = {
            "result_status": "unavailable",
            "result_status_reason": "VIEW SERVER STATE permission was denied.",
        }
        log.end(log.begin("get_version_store_stats", {}, context), result=result)
        log.end(log.begin("execute_sql", {}, context), exc=asyncio.CancelledError())
    backlog = build_backlog(tmp_path / "incidents", now=FIXED_NOW)
    assert backlog["items"] == []
    assert backlog["totals"]["below_min_priority"] == 2


def test_caller_errors_group_per_tool_while_bugs_group_by_root(tmp_path) -> None:
    payload = {"code": "invalid_arguments", "message": "Tool arguments failed validation.", "ok": False}
    log = make_log(tmp_path)
    for tool in ("execute_sql", "explain_query", "execute_sql"):
        log.end(log.begin(tool, {}), exc=ToolError(json.dumps(payload)))
    log.end(log.begin("get_sentinel_rows", {}), exc=ToolError("Unknown tool: get_sentinel_rows"))
    items = build_backlog(tmp_path / "incidents", now=FIXED_NOW, min_priority="P4")["items"]
    assert sorted((item["title"], item["count"]) for item in items) == [
        ("Agents call an unknown tool: get_sentinel_rows", 1),
        ("execute_sql: invalid arguments", 2),
        ("explain_query: invalid arguments", 1),
    ]


def test_export_withholds_leaking_items_and_counts_unparsed_lines(tmp_path) -> None:
    directory = tmp_path / "incidents"
    directory.mkdir()
    legacy = {
        "schema": "azure-sql-mcp-incident/1",
        "ts_utc": "2026-10-01T10:00:00.000Z",
        "kind": "tool_error",
        "category": "product_bug",
        "priority": "P1",
        "fingerprint": "0123456789abcdef",
        "tool": "sentineldb",
        "runtime": {"server_version": "2.5.0"},
    }
    lines = [json.dumps(legacy), "{not json", json.dumps({**legacy, "schema": "other/9"})]
    (directory / "incidents-2026-10-01.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    backlog = build_backlog(
        directory,
        now=FIXED_NOW,
        scrub_terms=build_scrub_terms(databases=("SentinelDb",)),
    )
    assert backlog["items"] == []
    assert (backlog["totals"]["withheld"], backlog["totals"]["unparsed"]) == (1, 2)
    assert "sentinel" not in json.dumps(backlog).lower()


def test_a_database_named_like_an_export_word_withholds_nothing(tmp_path) -> None:
    log = make_log(tmp_path)
    log.end(log.begin("list_schemas", {}), exc=tool_error_chain(TypeError("boom")))
    for _ in range(3):
        call = log.begin("get_top_queries", {}, fake_context())
        log.end(call, exc=tool_error_chain(TimeoutError(), code="timeout"))
    # Next steps say "unit test", "stage" and "logs"; keys say "sessions", "example".
    for name in ("Test", "Sessions", "Example", "Stage", "Logs"):
        backlog = build_backlog(
            tmp_path / "incidents",
            now=FIXED_NOW,
            scrub_terms=build_scrub_terms(databases=(name,)),
        )
        assert backlog["totals"]["withheld"] == 0
        assert [item["category"] for item in backlog["items"]] == ["product_bug", "timeout"]


def test_process_failure_titles_keep_the_redacted_message(tmp_path) -> None:
    fail = package_function(
        """
        def load(message):
            raise ValueError(message)
        """,
        "load",
    )
    log = make_log(tmp_path)
    for message in (
        "AZURE_SQL_SERVER is required.",
        "AZURE_SQL_DEFAULT_DATABASE must be included in AZURE_SQL_ALLOWED_DATABASES.",
    ):
        log.record_process_failure(raised(fail, message), phase="startup")
    titles = sorted(item["title"] for item in build_backlog(tmp_path / "incidents", now=FIXED_NOW)["items"])
    assert titles == [
        f"Server startup failed: ValueError in {PACKAGE_SITE}:load - "
        "AZURE_SQL_DEFAULT_DATABASE must be included in AZURE_SQL_ALLOWED_DATABASES.",
        f"Server startup failed: ValueError in {PACKAGE_SITE}:load - AZURE_SQL_SERVER is required.",
    ]


def test_markdown_has_banner_titles_fingerprints_and_checkboxes(tmp_path) -> None:
    log = make_log(tmp_path)
    log.end(log.begin("tune_query", {}), exc=tool_error_chain(TypeError("x")))
    backlog = build_backlog(tmp_path / "incidents", now=FIXED_NOW)
    [item] = backlog["items"]
    markdown = render_markdown(backlog)
    assert markdown.startswith("# azure-sql-mcp incident backlog\n")
    assert "Redacted local export. Review before filing in a public repo." in markdown
    assert f"### [P1] {item['title']}" in markdown
    assert f"Fingerprint: `{item['fingerprint']}`" in markdown
    assert "- [ ] filed" in markdown
    assert markdown.isascii()


def test_agent_summaries_need_the_explicit_flag_and_never_reach_the_summary(tmp_path) -> None:
    log = make_log(tmp_path)
    for index in range(4):
        log.report_blocker(
            skill="sql-index-manager",
            blocker_kind="unclear_next_step",
            last_tool="review_workload_indexes",
            summary=f"No next step after the review, attempt {'abcd'[index]}",
            context=fake_context(),
        )
    hidden = build_backlog(tmp_path / "incidents", now=FIXED_NOW)
    [item] = hidden["items"]
    assert "summaries" not in item and "summary" not in item["example"]["report"]
    assert item["title"] == "sql-index-manager blocked: unclear_next_step at review_workload_indexes"
    shown = build_backlog(tmp_path / "incidents", now=FIXED_NOW, include_summaries=True)
    assert len(shown["items"][0]["summaries"]) == 3
    compact = summarize_backlog(shown)
    assert set(compact["items"][0]) == {
        "fingerprint",
        "title",
        "priority",
        "category",
        "count",
        "last_seen_utc",
    }
    assert "attempt" not in json.dumps(compact)


def test_exported_summaries_keep_tool_names_and_withheld_markers(tmp_path) -> None:
    log = make_log(tmp_path)
    log.report_blocker(
        skill="sql-optimizer",
        blocker_kind="other",
        summary="tune_query fails on PatientDiagnoses every time",
        tool_names={"tune_query"},
        context=fake_context(),
    )
    log.report_blocker(
        skill="sql-optimizer",
        blocker_kind="unclear_next_step",
        summary="I ran SELECT Email FROM t WHERE Id = 4",
        context=fake_context(),
    )
    backlog = build_backlog(tmp_path / "incidents", now=FIXED_NOW, include_summaries=True)
    summaries = sorted(text for item in backlog["items"] for text in item["summaries"])
    assert summaries == ["[sql-like text withheld]", "tune_query fails on [ident] every time"]
    examples = sorted(item["example"]["report"]["summary"] for item in backlog["items"])
    assert examples == summaries


@pytest.mark.skipif(os.name == "nt", reason="process liveness check is POSIX only")
def test_export_includes_dead_journal_orphans_without_deleting_them(tmp_path) -> None:
    directory = tmp_path / "incidents"
    directory.mkdir()
    journal = write_journal(
        directory, pid=dead_pid(), run_id="deadbeef", calls=[journal_call("tune_query", 300)]
    )
    [item] = build_backlog(directory, now=FIXED_NOW)["items"]
    assert (item["kind"], item["priority"]) == ("orphaned_call", "P2")
    assert item["title"] == "tune_query never returned (process_exit)"
    assert journal.exists()


def test_stalled_workflows_flag_real_stalls_and_skip_designed_states(tmp_path) -> None:
    store = PerformanceStore(tmp_path)
    case = store.create_performance_case(PerformanceCaseV1(query_fingerprint="query-fp"))
    abandoned = store.create_session(
        TuningSessionV1(
            performance_case_id=case.case_id,
            status="screening",
            deadline_at_utc="2026-10-02T10:00:00+00:00",
        )
    )
    finished = store.create_session(
        TuningSessionV1(
            performance_case_id=case.case_id,
            status="completed",
            deadline_at_utc="2026-10-01T10:00:00+00:00",
        )
    )
    # Finalize leaves unmeasured candidates non-terminal by design.
    store.create_candidate(TuningCandidateV1(session_id=finished.session_id, strategy="x"))
    store.create_plan_action_intent(
        PlanActionIntentV1(intent_id="intent-stale", session_id=finished.session_id, query_fingerprint="q", status="applying")
    )
    store.create_plan_action_intent(
        PlanActionIntentV1(intent_id="intent-fresh", session_id=finished.session_id, query_fingerprint="q", status="applying")
    )
    for lease_id in ("lease-failed", "lease-expired-active"):
        store.create_index_lease(
            lease_id=lease_id,
            database_fingerprint=f"database-{lease_id}",
            session_id=abandoned.session_id,
            candidate_id="candidate-1",
            index_name="IX_Testing_synthetic",
            object_fingerprint="object-fingerprint",
            expires_at_utc="2026-10-01T12:00:00+00:00",
        )
    lease = store.get_index_lease("lease-failed")
    store.recover_index_lease(
        "lease-failed",
        status="cleanup_required",
        metadata={"recovery_error_type": "OperationalError"},
        expected_version=lease["version"],
    )
    store.close()
    database = tmp_path / "performance.sqlite3"
    with sqlite3.connect(database) as connection:
        stamp = "UPDATE {table} SET updated_at_utc = ? WHERE {key} = ?"
        connection.execute(stamp.format(table="plan_action_intents", key="intent_id"), ("2026-10-02T11:40:00+00:00", "intent-stale"))
        connection.execute(stamp.format(table="plan_action_intents", key="intent_id"), ("2026-10-02T11:55:00+00:00", "intent-fresh"))
        connection.execute(stamp.format(table="performance_cases", key="case_id"), ("2026-09-20T12:00:00+00:00", case.case_id))
        connection.execute(stamp.format(table="index_leases", key="lease_id"), ("2026-10-01T12:00:00+00:00", "lease-expired-active"))
    before = database.read_bytes()

    backlog = build_backlog(
        tmp_path / "incidents", performance_db=database, now=FIXED_NOW, min_priority="P4"
    )
    found = {
        (item["example"]["subject"]["type"], item["example"]["subject"]["reason"]): item
        for item in backlog["items"]
    }
    assert {key: item["priority"] for key, item in found.items()} == {
        ("index_lease", "cleanup_required"): "P1",
        ("plan_action_intent", "apply_unconfirmed"): "P1",
        ("tuning_session", "deadline_passed_unfinalized"): "P4",
        ("performance_case", "abandoned_case"): "P4",
    }
    lease_item = found[("index_lease", "cleanup_required")]
    assert lease_item["example"]["subject"]["recovery_error_type"] == "OperationalError"
    assert lease_item["subjects"] == 1
    assert "lease-failed" not in json.dumps(backlog)
    assert database.read_bytes() == before
