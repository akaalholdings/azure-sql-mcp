"""Local, redacted incident log and fix backlog for the MCP server.

Records tool failures, slow and stuck calls, crash orphans, agent loops and
swallowed errors as owner-only daily JSON Lines files. Every entry point
swallows its own errors: the log never changes or breaks a tool call. Records
never hold SQL text, literals, argument values, result rows, server, database
or host names, file paths, tokens or connection strings.
"""

from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
import linecache
import logging
import math
import os
import platform
import re
import signal
import sqlite3
import stat
import sys
import threading
import time
import uuid
import weakref
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from importlib.metadata import version as package_version
from pathlib import Path
from types import FrameType, TracebackType
from typing import Any, Callable, Collection, Mapping

from mcp.server.fastmcp.exceptions import ToolError
from sqlglot.errors import ParseError, TokenError

from .config import IncidentSettings
from .config import ServerConfig
from .config import build_arg_parser
from .config import parse_incident_settings
from .observability import extract_sql_error_info, sanitize_error_message
from .retry import TRANSIENT_ERROR_CODES
from .safe_sql import BLOCKED_FUNCTIONS, LOCKING_HINTS, STATEMENT_KEYWORDS


logger = logging.getLogger(__name__)

MESSAGE_LIMIT = 300
AGENT_TEXT_LIMIT = 500
WITHHELD = "[sql-like text withheld]"

_PACKAGE_PREFIX = "azure_sql_mcp."


def resolve_incident_dir(settings: IncidentSettings, performance_state_dir: str) -> Path | None:
    """Return the incident directory, or None when only memory state exists."""

    if settings.directory:
        return Path(settings.directory).expanduser()
    if performance_state_dir == ":memory:":
        return None
    return Path(performance_state_dir).expanduser() / "incidents"


def build_scrub_terms(
    *,
    server: str | None = None,
    databases: Collection[str] = (),
    principals: Collection[str | None] = (),
    secrets: Collection[str | None] = (),
) -> dict[str, str]:
    """Map exact configured names and secrets to their placeholders."""

    terms: dict[str, str] = {}
    if server:
        host = server.strip()
        host = host.removeprefix("tcp:").split(",", 1)[0]
        terms[host] = "[server]"
        terms[host.split(".", 1)[0]] = "[server]"
    for database in databases:
        terms[database.strip()] = "[db]"
    for principal in principals:
        if principal:
            terms[principal.strip()] = "[principal]"
    for secret in secrets:
        if secret:
            terms[secret.strip()] = "[secret]"
    # Very short terms would scrub ordinary words and carry no identity.
    return {term: label for term, label in terms.items() if len(term) >= 3}


def scrub_terms_from_env(environ: Mapping[str, str]) -> dict[str, str]:
    return build_scrub_terms(
        server=environ.get("AZURE_SQL_SERVER"),
        databases=[
            item
            for item in (environ.get("AZURE_SQL_ALLOWED_DATABASES") or "").split(",")
            if item.strip()
        ]
        + [environ.get("AZURE_SQL_DEFAULT_DATABASE") or ""],
        principals=(
            environ.get("AZURE_SQL_USERNAME"),
            environ.get("AZURE_CLIENT_ID"),
            environ.get("AZURE_TENANT_ID"),
        ),
        secrets=(
            environ.get("AZURE_SQL_PASSWORD"),
            environ.get("AZURE_CLIENT_SECRET"),
            environ.get("AZURE_SQL_MCP_BEARER_TOKEN"),
        ),
    )


# --- redaction ---------------------------------------------------------------

# Constructs a parser may not support. A parse failure that stops on one of
# these is a product gap; one that stops on anything else is a caller typo.
_GAP_KEYWORDS = frozenset(
    """
    APPLY PIVOT UNPIVOT OPTION WITHIN OVER PARTITION MERGE OUTPUT OPENJSON
    OPENXML OPENROWSET OPENQUERY OPENDATASOURCE TABLESAMPLE FOR XML JSON PATH
    MATCH GRAPH SYSTEM_TIME CONTAINED COLLATE FETCH OFFSET ROWS RANGE PRECEDING
    FOLLOWING UNBOUNDED GROUPING SETS CUBE ROLLUP STRING_AGG PERCENTILE_CONT
    PERCENTILE_DISC APPROX_PERCENTILE_CONT APPROX_COUNT_DISTINCT TRY_CONVERT
    TRY_CAST TRY_PARSE PARSE IIF CHOOSE LAG LEAD FIRST_VALUE LAST_VALUE NTILE
    CUME_DIST PERCENT_RANK HASHBYTES CHECKSUM FORMAT DATE_BUCKET DATETRUNC
    GENERATE_SERIES GREATEST LEAST TRANSLATE CONCAT_WS JSON_VALUE JSON_QUERY
    JSON_MODIFY JSON_OBJECT JSON_ARRAY ISJSON WINDOW HINT RECOMPILE MAXDOP
    FORCESEEK FORCESCAN NOEXPAND READPAST NOLOCK SNAPSHOT KEEPFIXED FAST
    OPTIMIZE PARAMETERIZATION EXPAND IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX
    USE PLAN AT ZONE IDENTITY NEXT VALUE CONTAINS FREETEXT CONTAINSTABLE
    FREETEXTTABLE SEMANTICKEYPHRASETABLE TOP PERCENT TIES CROSS OUTER
    """.split()
)
_TSQL_WORDS = (
    _GAP_KEYWORDS
    | STATEMENT_KEYWORDS
    | LOCKING_HINTS
    | frozenset(name.upper() for name in BLOCKED_FUNCTIONS)
    | frozenset(
        """
        SELECT FROM WHERE JOIN INNER LEFT RIGHT FULL ON AS AND OR NOT NULL IS IN
        EXISTS LIKE BETWEEN GROUP BY ORDER HAVING DISTINCT UNION ALL EXCEPT
        INTERSECT CASE WHEN THEN ELSE END CAST CONVERT WITH INTO VALUES SET
        DECLARE ASC DESC COUNT SUM MIN MAX AVG ROW_NUMBER RANK DENSE_RANK
        COALESCE ISNULL NULLIF DATEADD DATEDIFF DATEPART GETDATE GETUTCDATE
        SYSDATETIME SYSUTCDATETIME NEWID RAND ABS ROUND FLOOR CEILING LEN
        SUBSTRING CHARINDEX REPLACE UPPER LOWER LTRIM RTRIM TRIM CONCAT
        OBJECT_ID OBJECT_NAME SCHEMA_NAME DB_NAME DB_ID INDEX INDEXES TABLE VIEW
        PROCEDURE FUNCTION TRIGGER SCHEMA DATABASE STATISTICS
        """.split()
    )
)
_IDENTIFIER_TOKEN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,40}$")
# A bare KeyError of a lower-case snake_case key is a column alias the
# package's own SQL names; a key shaped like a table or person name is data.
_CODE_KEY = re.compile(r"'[a-z_][a-z0-9_]{0,40}'")
_SYSTEM_OBJECT = re.compile(r"sys\.[a-z_]{1,64}", re.IGNORECASE)
_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_CONTROL = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]")
_DUPLICATE_KEY = re.compile(r"(duplicate key value is)\b.*", re.IGNORECASE | re.DOTALL)
_PAREN_GROUP = re.compile(r"\([^()]*\)")
_DRIVER_TAG = re.compile(
    r"\[(?:Microsoft|SQL Server|ODBC Driver \d+ for SQL Server|ODBC SQL Server Driver"
    r"|[0-9A-Z]{5})\]"
)
_URL = re.compile(r"\b[a-z][a-z0-9+.-]*://\S+", re.IGNORECASE)
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_GUID = re.compile(r"\b[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\b")
_JWT = re.compile(r"\beyJ[\w-]*\.[\w-]+\.[\w-]*")
_HEX_ADDRESS = re.compile(r"\b0x[0-9a-fA-F]+\b")
_LONG_TOKEN = re.compile(r"[A-Za-z0-9+/_=-]{32,}")
# Folders may hold spaces (C:\Users\First Last\OneDrive - Employer\...): every
# segment up to the last separator goes; the final segment stops at a space.
_PATH = re.compile(
    r"(?:\b[A-Za-z]:[\\/]|\\\\|(?<![\w.:/\\\]])(?:~|\.{1,2})?/)(?=[^\s'\"])"
    r"(?:[^\\/'\"\n:]*[\\/])*[^\s'\"(),;:\\/]*"
)
# Engine messages do not escape quotes inside values (O'Brien): everything from
# the first quote to the last one on the line is one span.
_QUOTED_SPAN = re.compile(r"[\"'].*[\"']")
_IPV6 = re.compile(r"(?<![\w:])(?:[0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{1,4}(?![\w:])")
_BRACKETED = re.compile(r"\[([^\[\]]{1,128})\]")
_DOTTED_NAME = re.compile(r"(?<![\w-])[A-Za-z_#@][\w$#@-]*(?:\.[A-Za-z_#@][\w$#@-]*)+")
_DIGITS = re.compile(r"(AADSTS\d+|0xADDR)|\d+")
_BACKTICK_SPAN = re.compile(r"`[^`]*`?")
_SQL_CLAUSES = re.compile(
    r"\b(SELECT|FROM|WHERE|JOIN|GROUP BY|ORDER BY|HAVING|UNION|INSERT|UPDATE|DELETE)\b",
    re.IGNORECASE,
)
_SQL_SHAPES = re.compile(
    r"\bselect\b[^.;]*\bfrom\b|\binsert\s+into\b|\bdelete\s+from\b|\bupdate\s+\S+\s+set\b"
    r"|\bwhere\s+[\w.\[\]]+\s*(?:=|<>|!=|<|>|\blike\b|\bin\s*\()"
    r"|\bexec(?:ute)?\s+[\w.\[\]]+|@\w+\s*=",
    re.IGNORECASE,
)
_WORD = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
_IDENTIFIER_SHAPE = re.compile(r"_|\d|[a-z][A-Z]")
# Mid-sentence capitalized words are names (people, tables) unless one of these.
_PROPER_WORDS = frozenset(
    "Azure Microsoft Entra Query Store Server Copilot Claude Codex Windows Linux Python".split()
)
_SENTENCE_START = re.compile(r"(?:^|[.!?]\s+)$")
_TOOL_WORD = re.compile(r"\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+\b")
_PLACEHOLDERS = frozenset(
    "REDACTED server ip email guid token path url code ident db principal secret sql".split()
)


def redact_text(
    text: object,
    *,
    scrub_terms: Mapping[str, str] | None = None,
    keep_identifiers: bool = False,
    scan_quotes: bool = False,
    limit: int = MESSAGE_LIMIT,
) -> str:
    """Reduce free text to a value-free skeleton that is safe to persist.

    Native codes must be read before this runs: the duplicate-key tail and any
    parenthesized group are dropped first, because engine messages put row
    values there without quotes. Quoted text goes as one greedy span unless
    the text comes from package code (keep_identifiers) or an agent
    (scan_quotes), where apostrophes are words, not value delimiters.
    """

    value = _first_line(str(text))
    value = _DUPLICATE_KEY.sub(r"\1 (...)", value)
    for _ in range(3):
        value = _PAREN_GROUP.sub("(...)", value).replace("((...))", "(...)")
    value = _DRIVER_TAG.sub("", value)
    value = _scrub_terms(value, scrub_terms)
    if not (keep_identifiers or scan_quotes):
        value = _QUOTED_SPAN.sub(_redact_quoted_span, value)
    value = _URL.sub("[url]", value)
    value = _PATH.sub("[path]", value)
    value = _EMAIL.sub("[email]", value)
    value = _JWT.sub("[token]", value)
    # Whole dotted names first, so no host label survives a partial match.
    value = _DOTTED_NAME.sub(
        lambda match: match.group(0) if _SYSTEM_OBJECT.fullmatch(match.group(0)) else "[ident]",
        value,
    )
    value = sanitize_error_message(
        value,
        keep_quoted=lambda token: _keep_quoted(token, keep_identifiers),
    )
    value = _GUID.sub("[guid]", value)
    value = _HEX_ADDRESS.sub("0xADDR", value)
    value = _LONG_TOKEN.sub(
        lambda match: "[token]" if any(c.isdigit() for c in match.group(0)) else match.group(0),
        value,
    )
    value = _IPV6.sub("[ip]", value)
    value = _BRACKETED.sub(
        lambda match: match.group(0) if match.group(1) in _PLACEHOLDERS else "[ident]",
        value,
    )
    value = _DIGITS.sub(lambda match: match.group(1) or "N", value)
    value = " ".join(value.split())
    return value[:limit]


def redact_agent_text(
    text: object,
    *,
    scrub_terms: Mapping[str, str] | None = None,
    keep_words: Collection[str] = (),
    limit: int = AGENT_TEXT_LIMIT,
) -> str:
    """Redact text an agent wrote: code spans, URLs, SQL-like text,
    identifier-shaped names (PatientDiagnoses, IX_A_B, Db2) and capitalized
    words mid-sentence (people, tables) go too."""

    value = _first_line(str(text))
    # Again with dotted names gone, as the export sees the stored text.
    for probe in (value, _DOTTED_NAME.sub("[ident]", value)):
        clauses = {clause.upper() for clause in _SQL_CLAUSES.findall(probe)}
        if probe == WITHHELD or len(clauses) >= 2 or _SQL_SHAPES.search(probe):
            return WITHHELD
    value = _BACKTICK_SPAN.sub("[code]", value)
    value = _WORD.sub(lambda match: _agent_word(match, keep_words), value)
    # After a placeholder, a possessive's apostrophe would open a quoted span.
    value = value.replace("[ident]'s", "[ident]")
    return redact_text(value, scrub_terms=scrub_terms, scan_quotes=True, limit=limit)


def _agent_word(match: re.Match[str], keep_words: Collection[str]) -> str:
    word = match.group(0)
    if word in keep_words or word.upper() in _TSQL_WORDS:
        return word
    if _IDENTIFIER_SHAPE.search(word):
        return "[ident]"
    capitalized = word[0].isupper() and word[1:].islower()
    if capitalized and word not in _PROPER_WORDS and not _SENTENCE_START.search(match.string[: match.start()]):
        return "[ident]"
    return word


def _first_line(text: str) -> str:
    text = _ANSI.sub("", text[:2000])
    text = text.replace("\r", "\n").split("\n", 1)[0]
    return _CONTROL.sub(" ", text)


def _scrub_terms(text: str, scrub_terms: Mapping[str, str] | None) -> str:
    for term in sorted(scrub_terms or {}, key=len, reverse=True):
        pattern = re.compile(rf"(?<![\w-]){re.escape(term)}(?![\w-])", re.IGNORECASE)
        text = pattern.sub(scrub_terms[term], text)  # type: ignore[index]
    return text


def _redact_quoted_span(match: re.Match[str]) -> str:
    span = match.group(0)
    inner = span[1:-1]
    # One quoted T-SQL word or system object name comes from code, not data.
    if span[0] == span[-1] and not re.search(r"[\"']", inner) and _keep_quoted(inner, False):
        return span
    return f"{span[0]}[REDACTED]{span[0]}"


def _keep_quoted(token: str, keep_identifiers: bool) -> bool:
    # T-SQL words and system catalog names come from code, never from data.
    if token.upper() in _TSQL_WORDS or _SYSTEM_OBJECT.fullmatch(token):
        return True
    return keep_identifiers and bool(_IDENTIFIER_TOKEN.fullmatch(token))


# --- classification ----------------------------------------------------------

_ENVIRONMENT_CODES = frozenset({18456, 40615, 40532, 229, 230, 262, 297, 300, 916, 4060})
_TRANSIENT_SQLSTATES = frozenset({"HYT00", "HYT01", "40001"})
_ENVIRONMENT_SQLSTATES = frozenset({"28000"})
_SQL_ARGUMENT_KEYS = frozenset(
    """
    sql baseline_sql candidate_sql original_sql rewrite_sql definition filter_definition
    queries query_hints
    """.split()
)
_BUILTIN_BUGS = (
    TypeError,
    AttributeError,
    IndexError,
    NameError,
    AssertionError,
    ZeroDivisionError,
    RecursionError,
)
# Expected state-machine rejections of a caller's request, not defects.
_CALLER_STATE_ERRORS = frozenset(
    """
    InvalidTransitionError IdempotencyConflictError IndexReviewIdempotencyConflictError
    ConcurrencyError ReservationError TuningBudgetExceeded LifecycleError
    """.split()
)
_POLICY_ERRORS = frozenset({"DatabasePolicyError", "IndexReviewPolicyError"})
# An optional setup step a DBA has not done: not a code defect.
_SETUP_ERRORS = frozenset({"IndexReviewSetupError"})
_STATE_ERRORS = frozenset({"PerformanceStoreError", "LearningStoreError"})
_CALLER_PAYLOAD_CODES = frozenset({"invalid_arguments", "session_expired", "preview_only"})
# The shared executor runs every query; a failing server-built query is filed
# at the service frame that built it.
_EXECUTOR_MODULES = frozenset(
    {"azure_sql_mcp.connection", "azure_sql_mcp.retry", "azure_sql_mcp.connection_pool"}
)
_ROOT_GROUPED = frozenset(
    {"product_bug", "product_gap", "outcome_unknown", "state_store", "environment", "transient"}
)
_STAGE_BY_MODULE = {
    "azure_sql_mcp.connection_pool": "pool_wait",
    "azure_sql_mcp.connection": "sql_execute",
    "azure_sql_mcp.retry": "retry_backoff",
    "azure_sql_mcp.auth": "token_acquire",
    "azure_sql_mcp.performance_store": "state_store",
    "azure_sql_mcp.learning_store": "state_store",
    "azure_sql_mcp.plan_tree": "plan_analysis",
    "azure_sql_mcp.plan_rules": "plan_analysis",
    "azure_sql_mcp.plan_digest": "plan_analysis",
    "azure_sql_mcp.plan_diagnostics": "plan_analysis",
}
_DRIVER_SQLSTATES = "08001 08002 08003 08004 08007 08S01 HYT00 HYT01 40001 28000 23000 42000 42S02 42S22".split()
_SQLSTATE = re.compile(r"(?:0[178]|2[1-58]|3[4CDF]|4[024]|HY|IM)[0-9A-Z]{3}")
_driver_sqlstate_map: dict[str, str] | None = None


def describe_exception(
    exc: BaseException,
    *,
    sql_arguments: bool = False,
    declared_keys: Collection[str] = (),
    scrub_terms: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Classify a tool exception chain by class and origin, value-free."""

    exc = _leaf_exception(exc)
    chain = _exception_chain(exc)
    payload = _error_payload(chain)
    code = payload.get("code") if payload else None
    root = next((item for item in chain if not isinstance(item, ToolError)), None)
    if root is None and str(exc).startswith("Unknown tool:"):
        code = "unknown_tool"
    subject = root or exc
    frames = package_frames(subject.__traceback__)
    error: dict[str, Any] = {
        "class": type(subject).__name__,
        "module_of_class": type(subject).__module__,
        "code": code if isinstance(code, str) else None,
    }
    if frames:
        error["frame"], error["frame_line"] = frames[-1]
    where = [label for label, _line in frames[-3:]]
    kind, stage = "tool_error", None

    if isinstance(exc, asyncio.CancelledError):
        kind, category, priority = "tool_cancelled", "cancelled", "P4"
        stage = _stage(where)
    elif code == "invalid_arguments":
        category, priority = "caller_error", "P4"
        error["validation_issues"] = _validation_issues(payload, declared_keys)
    elif code == "unknown_tool":
        category, priority = "contract_drift", "P3"
        error["message"] = "Unknown tool name."  # the name is agent text
    elif code in _CALLER_PAYLOAD_CODES:
        category, priority = "caller_error", "P4"
    elif code == "timeout" or (code is None and isinstance(root, TimeoutError)):
        kind, category, priority = "tool_timeout", "timeout", "P3"
        suspended = root.__cause__ if root is not None and root.__cause__ else subject
        where = [label for label, _line in package_frames(suspended.__traceback__)[-3:]]
        stage = _stage(where)
    else:
        category, priority = _classify_root(subject, chain, sql_arguments, error)
        # "transient" is set only by driver classification.
        if category == "product_bug" and error.get("transient") is False:
            service = [frame for frame in frames if frame[0].split(":", 1)[0] not in _EXECUTOR_MODULES]
            if service:
                error["frame"], error["frame_line"] = service[-1]

    if "message" not in error:
        # Server payload messages are fixed text; only tool_error carries the
        # root's own wording, which redaction can keep more of.
        if payload and isinstance(payload.get("message"), str) and (root is None or code != "tool_error"):
            text = payload["message"]
        else:
            text = str(subject) if not isinstance(subject, asyncio.CancelledError) else ""
        keep = _raised_in_package(subject) and (
            isinstance(subject, _BUILTIN_BUGS)
            or (type(subject) is KeyError and bool(_CODE_KEY.fullmatch(text)))
        )
        error["message"] = redact_text(text, scrub_terms=scrub_terms, keep_identifiers=keep) or None
    return {
        "kind": kind,
        "category": category,
        "priority": priority,
        "error": {key: value for key, value in error.items() if value is not None},
        "where": where,
        "stage": stage,
    }


def _classify_root(
    root: BaseException,
    chain: list[BaseException],
    sql_arguments: bool,
    error: dict[str, Any],
) -> tuple[str, str]:
    names = {cls.__name__ for cls in type(root).__mro__}
    # Parser and tokenizer messages quote the SQL itself: never keep their text.
    parse_error = next((item for item in chain if isinstance(item, (ParseError, TokenError))), None)
    if parse_error is not None:
        error["parse"] = _parse_details(parse_error)
        error["message"] = error["parse"]["description"]
        if error["parse"].get("keyword"):
            return "product_gap", "P2"
        return "caller_error", "P4"
    if any(name.endswith("OutcomeUnknownError") for name in names):
        cause = root.__cause__
        if cause is not None and type(cause).__module__.startswith("mssql_python"):
            category, priority = _classify_driver(cause, sql_arguments, error)
            # The engine answered: only a lost connection, timeout or cancel
            # after dispatch leaves the outcome unknown.
            if category != "transient" and error.get("sqlstate") != "HY008":
                return category, priority
        return "outcome_unknown", "P1"
    if "TransactionRollbackError" in names:
        return "outcome_unknown", "P1"
    if isinstance(root, KeyError) and type(root).__name__.endswith("NotFoundError"):
        return "caller_error", "P4"
    if names & _CALLER_STATE_ERRORS:
        return "caller_error", "P4"
    if names & _STATE_ERRORS or isinstance(root, sqlite3.Error):
        return "state_store", "P1"
    if isinstance(root, PermissionError) or names & _POLICY_ERRORS:
        return "policy", "P4"
    if names & _SETUP_ERRORS:
        return "environment", "P3"
    if type(root).__module__.startswith("mssql_python"):
        return _classify_driver(root, sql_arguments, error)
    if isinstance(root, _BUILTIN_BUGS) or type(root) is KeyError:
        return "product_bug", "P1"
    if isinstance(root, NotImplementedError) or "PlanParseError" in names:
        return "product_gap", "P2"
    if isinstance(root, ValueError) and _raised_in_package(root):
        return ("caller_error", "P4") if _deliberate(root) else ("product_bug", "P1")
    if type(root).__name__ == "ValidationError" and type(root).__module__.startswith("pydantic"):
        return "product_bug", "P1"  # result conversion failed after the tool ran
    if isinstance(root, OSError) or type(root).__module__.startswith("azure."):
        return "environment", "P3"
    return "product_bug", "P2"


def _classify_driver(
    root: BaseException,
    sql_arguments: bool,
    error: dict[str, Any],
) -> tuple[str, str]:
    native, sqlstate = native_codes(root)
    if native is not None:
        error["native_error_code"] = native
    if sqlstate is not None:
        error["sqlstate"] = sqlstate
    driver_error = getattr(root, "driver_error", None)
    if isinstance(driver_error, str):
        error["driver_category"] = redact_text(driver_error, limit=80)
    transient = native in TRANSIENT_ERROR_CODES or (
        sqlstate is not None
        and (sqlstate.startswith("08") or sqlstate in _TRANSIENT_SQLSTATES)
    )
    error["transient"] = transient
    if transient:
        return "transient", "P3"
    if native in _ENVIRONMENT_CODES or sqlstate in _ENVIRONMENT_SQLSTATES:
        return "environment", "P3"
    # Caller-supplied SQL that the engine rejects is the caller's error; a
    # server-built query that fails is ours.
    if sql_arguments:
        return "caller_error", "P4"
    return "product_bug", "P2"


def native_codes(exc: BaseException) -> tuple[int | None, str | None]:
    """Return (native error code, SQLSTATE) without reading row values."""

    info = extract_sql_error_info(exc) if isinstance(exc, Exception) else {}
    native = info.get("native_error_code")
    sqlstate = info.get("sqlstate")
    # "[\w{5}]" also matches a bracketed five-letter name; keep real classes only.
    if not (isinstance(sqlstate, str) and _SQLSTATE.fullmatch(sqlstate)):
        sqlstate = None
    if native is None:
        # Only a code at the very end: engine text ends a row value with ".".
        text = str(getattr(exc, "ddbc_error", "") or exc).strip()
        match = re.search(r"\((\d{3,6})\)\s*(?:\(SQL\w+\))?$", text)
        if match:
            native = int(match.group(1))
    if sqlstate is None:
        driver_error = getattr(exc, "driver_error", None)
        if isinstance(driver_error, str):
            sqlstate = _driver_sqlstates().get(driver_error)
    return native, sqlstate


def _driver_sqlstates() -> dict[str, str]:
    """Reverse-map mssql-python's fixed per-SQLSTATE driver text."""

    global _driver_sqlstate_map
    if _driver_sqlstate_map is None:
        mapping: dict[str, str] = {}
        try:
            from mssql_python.exceptions import sqlstate_to_exception

            for state in _DRIVER_SQLSTATES:
                mapped = sqlstate_to_exception(state, "")
                text = getattr(mapped, "driver_error", None)
                if isinstance(text, str):
                    mapping.setdefault(text, state)
        except Exception:
            pass
        _driver_sqlstate_map = mapping
    return _driver_sqlstate_map


def _parse_details(error: ParseError | TokenError) -> dict[str, Any]:
    if isinstance(error, TokenError):
        return {"description": "tokenizer error", "keyword": None}
    detail = error.errors[0] if error.errors else {}
    # A token's repr carries its comments: nothing after it is kept.
    description = re.sub(r"<Token.*", "<token>", str(detail.get("description") or ""), flags=re.S)
    # Any word that is neither T-SQL nor the parser's own was copied from the SQL.
    description = _PARSE_WORD.sub(_parse_word, description)
    highlight = str(detail.get("highlight") or "").strip().upper()
    construct = re.search(r"missing for <([A-Z][a-z]+)", description)
    named = construct.group(1).upper() if construct else None
    # A stop at the very last token is SQL cut short; a missing token or an
    # option that is no T-SQL word is the caller's too, unless the parser
    # names a construct it lacks.
    at_end = not str(detail.get("end_context") or "").strip()
    incomplete = (
        at_end
        or description.startswith(("Expecting", "Unknown option [sql]"))
        or "Required keyword" in description
    )
    if named in _GAP_KEYWORDS and not at_end:
        keyword: str | None = named
    else:
        keyword = highlight if highlight in _GAP_KEYWORDS and not incomplete else None
    return {"description": redact_text(description) or "unparsed", "keyword": keyword}


# sqlglot's own message words; every other non-T-SQL word came from the SQL.
_PARSE_TEMPLATE_WORDS = frozenset(
    """
    expected expecting required keyword missing for table name but got unknown option
    invalid expression unexpected token after to have alias either or an any the of
    cannot parse failed statement following closing found clause type database
    unable support does not
    """.split()
)
_PARSE_WORD = re.compile(r"<class '[^']*\.(\w+)'>|[\w@#$]+")


def _parse_word(match: re.Match[str]) -> str:
    if match.group(1):
        return f"<{match.group(1)}>"  # an expression class the parser names
    word = match.group(0)
    if word.upper() in _TSQL_WORDS or word.lower() in _PARSE_TEMPLATE_WORDS:
        return word
    return "[sql]"


def _validation_issues(
    payload: Mapping[str, Any] | None,
    declared_keys: Collection[str],
) -> list[dict[str, str]]:
    details = (payload or {}).get("details")
    issues = details.get("issues") if isinstance(details, dict) else None
    result: list[dict[str, str]] = []
    for issue in issues if isinstance(issues, list) else []:
        if not isinstance(issue, dict):
            continue
        code = str(issue.get("code") or "invalid")
        top = str(issue.get("path") or "").split(".", 1)[0]
        result.append(
            {
                "code": code if re.fullmatch(r"[a-z_]{1,40}", code) else "invalid",
                "path": top if top in declared_keys and code != "extra_forbidden" else "[unknown]",
            }
        )
    return result[:20]


def _leaf_exception(exc: BaseException) -> BaseException:
    """The first leaf of an exception group that is not a cancel."""

    pending: list[BaseException] = [exc]
    cancel: BaseException | None = None
    for _ in range(100):
        if not pending:
            break
        current = pending.pop(0)
        if isinstance(current, BaseExceptionGroup):
            pending[:0] = list(current.exceptions)
        elif not isinstance(current, asyncio.CancelledError):
            return current
        elif cancel is None:
            cancel = current
    return cancel or exc


def _exception_chain(exc: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and len(chain) < 20 and not any(current is item for item in chain):
        chain.append(current)
        current = current.__cause__ or current.__context__
    return chain


def _error_payload(chain: list[BaseException]) -> dict[str, Any] | None:
    for item in chain:
        text = str(item)
        if isinstance(item, ToolError) and text.startswith("{"):
            try:
                payload = json.loads(text)
            except ValueError:
                continue
            if isinstance(payload, dict) and "code" in payload:
                return payload
    return None


def package_frames(tb: TracebackType | None) -> list[tuple[str, int]]:
    """Return (module:qualname, line) for package frames, outermost first."""

    frames: list[tuple[str, int]] = []
    while tb is not None:
        frame = tb.tb_frame
        module = str(frame.f_globals.get("__name__", ""))
        qualname = frame.f_code.co_qualname
        if (
            module.startswith(_PACKAGE_PREFIX)
            and module != __name__
            and not qualname.startswith("_SanitizingToolManager.")
            and not qualname.endswith("._raise_tool_error")
        ):
            frames.append((f"{module}:{qualname}", tb.tb_lineno))
        tb = tb.tb_next
    return frames


def _raised_in_package(exc: BaseException) -> bool:
    tb = exc.__traceback__
    if tb is None:
        return False
    while tb.tb_next is not None:
        tb = tb.tb_next
    return str(tb.tb_frame.f_globals.get("__name__", "")).startswith(_PACKAGE_PREFIX)


def _deliberate(exc: BaseException) -> bool:
    """A package class or a raise statement rejects input; int(), unpacking
    or fromisoformat failing inside package code is a bug."""

    if type(exc).__module__.startswith(_PACKAGE_PREFIX):
        return True
    tb = exc.__traceback__
    while tb is not None and tb.tb_next is not None:
        tb = tb.tb_next
    if tb is None:
        return True
    line = linecache.getline(tb.tb_frame.f_code.co_filename, tb.tb_lineno).strip()
    return not line or line.startswith("raise")  # no source: keep the old reading


def _stage(where: list[str]) -> str | None:
    for label in reversed(where):
        stage = _STAGE_BY_MODULE.get(label.split(":", 1)[0])
        if stage:
            return stage
    return where[-1].removeprefix(_PACKAGE_PREFIX) if where else None


def fingerprint(*parts: object) -> str:
    text = "v1|" + "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def error_fingerprint(described: Mapping[str, Any], tool: str | None = None) -> str:
    """Defects group by root cause across tools; caller friction per tool."""

    error = described.get("error") or {}
    parse = error.get("parse") or {}
    return fingerprint(
        "error",
        described.get("category"),
        None if described.get("category") in _ROOT_GROUPED else tool,
        error.get("class"),
        error.get("code"),
        error.get("native_error_code"),
        error.get("message"),
        error.get("frame"),
        parse.get("keyword"),
    )


# --- the log -----------------------------------------------------------------

# One JSON object per line. Always present: schema, incident_id, ts_utc, kind,
# category, priority (P1-P4), fingerprint, tool and runtime. Kind-specific
# blocks: call, error, session, site, log_template, reason, cause, loop,
# report, orphan, phase. Kinds: tool_error, tool_timeout, tool_cancelled,
# degraded_result, slow_call, stuck_call, event_loop_blocked, orphaned_call,
# agent_loop, agent_report, swallowed_exception, condition, startup_failure,
# server_exit, log_cap_reached. stalled_workflow items exist only in exports.
SCHEMA = "azure-sql-mcp-incident/1"
DAY_BYTE_CAP = 5 * 1024 * 1024
MAX_TOTAL_BYTES = 20 * 1024 * 1024
# anyio errors on the transport's streams once the host closed stdio.
_CLOSED_STREAM_ERRORS = frozenset({"ClosedResourceError", "BrokenResourceError", "EndOfStream"})
_DAY_FILE = re.compile(r"^incidents-(\d{4}-\d{2}-\d{2})\.jsonl$")
_SITE_LABEL = re.compile(r"^[A-Za-z0-9_.:-]{1,80}$")
_QUIET_CATEGORIES = frozenset({"transient", "environment", "policy", "caller_error"})
# Three repeats of these is a loop; "ok" and "empty" need POLL_THRESHOLD.
_LOOP_STATUSES = frozenset({"unavailable", "precondition", "not_supported"})
# Live state that triage re-samples on purpose: never a polling loop.
_SAMPLED_TOOLS = frozenset(
    """
    get_currently_waiting_tasks get_active_sessions get_lock_details get_open_transactions
    get_wait_stats plan_enforcer_tick
    """.split()
)
_PRIORITIES = ("P1", "P2", "P3", "P4")
_CATEGORIES = frozenset(
    """
    product_bug product_gap outcome_unknown state_store contract_drift caller_error
    policy transient environment timeout cancelled slow stuck degraded agent_loop
    agent_report orphaned agent_abandoned stalled_workflow startup server_exit incident_log
    """.split()
)
LOOP_THRESHOLD = 3
POLL_THRESHOLD = 10
LOOP_WINDOW_SECONDS = 15 * 60
LOOP_KEY_LIMIT = 1000
DEFAULT_STATE_DIR = "~/.azure-sql-mcp/state"
STUCK_GRACE_SECONDS = 30
JOURNAL_SCHEMA = "azure-sql-mcp-inflight/1"
JOURNAL_STALE_SECONDS = 120
_JOURNAL_FILE = re.compile(r"^inflight-(\d+)-([0-9a-f]{8})\.json$")
BLOCKER_KINDS = frozenset(
    """
    skill_tool_contradiction repeated_tool_failure tool_hang_or_timeout missing_capability
    unclear_next_step policy_blocks_required_step other
    """.split()
)
REPORT_COALESCE_SECONDS = 600
REPORT_RATE_LIMIT = 20
# Ids the package mints (new_id), or a bare incident id: never free text.
_RELATED_ID = re.compile(
    r"(?:(?:case|session|candidate|decision|evidence|handoff|intent|lesson|review)-)?[0-9a-f]{32}"
)

_CURRENT_CALL: ContextVar[_Call | None] = ContextVar("azure_sql_mcp_incident_call", default=None)
_ACTIVE_LOG: IncidentLog | None = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(eq=False)
class _Call:
    log: IncidentLog
    call_id: str
    tool: str
    arguments: Mapping[str, Any]
    started: float
    started_utc: str
    budget_s: float | None
    session_ref: str | None
    client: dict[str, str] | None
    argument_keys: list[str]
    unknown_argument_count: int
    sql_arguments: bool
    task: asyncio.Task[Any] | None
    loop: asyncio.AbstractEventLoop | None
    loop_thread: int | None
    token: Token[_Call | None] | None = None
    slow_flagged: bool = False
    stuck_flagged: bool = False
    journaled: bool = False
    responder: Any = None
    seen_where: list[str] | None = None  # where the watchdog saw it run slow


class IncidentLog:
    """Append-only, owner-only incident capture. No method ever raises."""

    def __init__(
        self,
        directory: Path | None,
        *,
        disabled_reason: str | None = None,
        retention_days: int = 30,
        slow_seconds: float = 60,
        server_version: str = "unknown",
        profile: str | None = None,
        transport: str | None = None,
        scrub_terms: Mapping[str, str] | None = None,
        budget_for: Callable[[str, Mapping[str, Any]], float | None] | None = None,
        declared_for: Callable[[str], Collection[str]] | None = None,
        on_tick: Callable[[], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
        utc_now: Callable[[], datetime] = _utc_now,
        tick_seconds: float = 5.0,
        capture_timeout: float = 2.0,
    ) -> None:
        self.directory = directory
        self.retention_days = retention_days
        self.slow_seconds = slow_seconds
        self._reason = disabled_reason or (None if directory is not None else "no_durable_state_dir")
        self._scrub_terms = dict(scrub_terms or {})
        self._budget_for = budget_for
        self._declared_for = declared_for
        self._on_tick = on_tick
        self._clock = clock
        self._utc_now = utc_now
        self._tick_seconds = tick_seconds
        self._capture_timeout = capture_timeout
        self.run_id = uuid.uuid4().hex[:8]
        self._runtime = {
            "server_version": server_version,
            "mcp_version": _package_version("mcp"),
            "python": platform.python_version(),
            "os": platform.system(),
            "profile": profile,
            "transport": transport,
            "run_id": self.run_id,
            "pid": os.getpid(),
        }
        self._write_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._fd: int | None = None
        self._fd_day: str | None = None
        self._cap_marked_day: str | None = None
        self._failures = 0
        self._written = 0
        self._counter = itertools.count(1)
        self._inflight: dict[str, _Call] = {}
        self._loops: dict[tuple[Any, ...], list[Any]] = {}
        self._recent: dict[str, list[str]] = {}
        self._reports: dict[str, list[tuple[float, str, str]]] = {}
        self._session_refs: weakref.WeakKeyDictionary[Any, str] = weakref.WeakKeyDictionary()
        self._salt = os.urandom(16)
        self._shutting_down = False
        self._handler: logging.Handler | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._journal_written = False
        self._journal_lock = threading.Lock()
        self._pruned_day: date | None = None
        self._main_task: asyncio.Task[Any] | None = None
        self._previous_sigterm: Any = None
        if self._reason is None:
            self._prepare_directory()

    @classmethod
    def from_settings(
        cls,
        settings: IncidentSettings,
        *,
        performance_state_dir: str,
        **kwargs: Any,
    ) -> IncidentLog:
        if settings.invalid:
            logger.warning(
                "Incident log disabled: invalid setting.", extra={"variable": settings.invalid}
            )
            return cls(None, disabled_reason="invalid_config", **kwargs)
        if not settings.enabled:
            return cls(None, disabled_reason="disabled_by_config", **kwargs)
        return cls(
            resolve_incident_dir(settings, performance_state_dir),
            retention_days=settings.retention_days,
            slow_seconds=settings.slow_seconds,
            **kwargs,
        )

    @classmethod
    def from_config(cls, config: ServerConfig, **kwargs: Any) -> IncidentLog:
        """Build from the server config; its names become scrub terms."""

        return cls.from_settings(
            config.incident,
            performance_state_dir=config.performance_state_dir,
            profile=config.profile.value if config.profile is not None else None,
            transport=config.transport.mode.value,
            scrub_terms=build_scrub_terms(
                server=config.server,
                databases=(*config.allowed_databases, config.default_database),
                principals=(config.username, config.client_id, config.tenant_id),
                secrets=(config.password, config.client_secret, config.mcp_bearer_token),
            ),
            **kwargs,
        )

    @property
    def enabled(self) -> bool:
        return self._reason is None

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "reason": self._reason,
            "orphan_detection": "journal",
            "retention_days": self.retention_days,
            "slow_seconds": self.slow_seconds,
            "records_written": self._written,
            "capped_day": self._cap_marked_day,
        }

    def backlog(self, *, performance_db: Path | None, **options: Any) -> dict[str, Any]:
        """Read-only export of this log's directory with its scrub terms."""

        return build_backlog(
            self.directory,
            performance_db=performance_db,
            scrub_terms=self._scrub_terms,
            **options,
        )

    # -- tool calls -----------------------------------------------------------

    def begin(self, tool: str, arguments: Mapping[str, Any] | None, context: Any = None) -> _Call | None:
        """Register a call in flight; returns None when the log is off."""

        if not self.enabled:
            return None
        try:
            arguments = arguments if isinstance(arguments, Mapping) else {}
            declared = self._declared(tool)
            try:
                task: asyncio.Task[Any] | None = asyncio.current_task()
                loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
            except RuntimeError:
                task, loop = None, None
            session_ref, client = self._session_info(context)
            call = _Call(
                log=self,
                call_id=f"{self.run_id}-{next(self._counter)}",
                tool=_tool_label(tool),
                arguments=arguments,
                started=self._clock(),
                started_utc=_iso(self._utc_now()),
                budget_s=self._budget(tool, arguments),
                session_ref=session_ref,
                client=client,
                argument_keys=sorted(key for key in arguments if key in declared),
                unknown_argument_count=sum(1 for key in arguments if key not in declared),
                sql_arguments=any(key in _SQL_ARGUMENT_KEYS for key in arguments),
                task=task,
                loop=loop,
                loop_thread=threading.get_ident() if loop is not None else None,
                responder=_responder(context),
            )
            with self._state_lock:
                self._inflight[call.call_id] = call
            call.token = _CURRENT_CALL.set(call)
            return call
        except Exception:
            return None

    def end(
        self,
        call: _Call | None,
        *,
        exc: BaseException | None = None,
        result: Any = None,
    ) -> None:
        """Record the outcome. Reads the result's status only; never alters it."""

        if call is None:
            return
        try:
            with self._state_lock:
                self._inflight.pop(call.call_id, None)
            if call.token is not None:
                try:
                    _CURRENT_CALL.reset(call.token)
                except ValueError:
                    pass
            if call.journaled:  # only calls that lived through a tick cost I/O here
                try:
                    self._sync_journal()
                except Exception:
                    pass  # the outcome is still recorded
            elapsed = self._clock() - call.started
            stage = None
            if exc is not None:
                stage, outcome = self._record_failure(call, exc, elapsed)
            else:
                status = result.get("result_status") if isinstance(result, Mapping) else None
                outcome = status if isinstance(status, str) else "ok"
                if outcome == "unavailable":
                    self._record_degraded(call, result, elapsed)
                if outcome in _LOOP_STATUSES:
                    self._check_loop(call, ("status", outcome, self._digest(call)), status=outcome)
                elif call.session_ref is not None and call.tool not in _SAMPLED_TOOLS:
                    self._check_loop(
                        call,
                        ("status", outcome, self._digest(call)),
                        status=outcome,
                        threshold=POLL_THRESHOLD,
                        priority="P3",
                    )
            if elapsed >= self.slow_seconds:
                # Also after the watchdog saw it running: the export keeps
                # this final duration and outcome for the call.
                self._claim(call, "slow_flagged")
                where = call.seen_where or []
                self._record_slow(call, elapsed, stage or _stage(where) or "unobserved", where, outcome=outcome)
        except Exception:
            pass

    def mark_shutting_down(self) -> None:
        """Cancels after this point are tagged cause=shutdown, not client."""

        self._shutting_down = True

    def _main_cancelling(self) -> bool:
        # asyncio.run cancels the main task on SIGINT before in-flight calls end.
        task = self._main_task
        return task is not None and not task.done() and task.cancelling() > 0

    def _record_failure(self, call: _Call, exc: BaseException, elapsed: float) -> tuple[str | None, str]:
        described = describe_exception(
            exc,
            sql_arguments=call.sql_arguments,
            declared_keys=self._declared(call.tool),
            scrub_terms=self._scrub_terms,
        )
        kind, stage = described["kind"], described["stage"]
        priority = described["priority"]
        if kind in ("tool_timeout", "tool_cancelled"):
            fingerprint_value = fingerprint(kind, call.tool, stage)
        else:
            fingerprint_value = error_fingerprint(described, call.tool)
        cause = None
        if kind == "tool_cancelled":
            # A transport stopping (uvicorn on SIGTERM) cancels calls before
            # the server knows it is shutting down: only the client's own
            # notifications/cancelled makes a long cancel a P2.
            if self._shutting_down or self._main_cancelling():
                cause = "shutdown"
            elif getattr(call.responder, "cancelled", False) is True:
                cause = "client"
            else:
                cause = "unknown"
            long_running = elapsed >= self.slow_seconds
            priority = "P2" if cause == "client" and long_running else "P4"
        record = self._record(kind, described["category"], priority, fingerprint_value, call.tool)
        record["call"] = self._call_block(call, elapsed, where=described["where"], stage=stage)
        record["error"] = described["error"]
        record["session"] = self._session_block(call)
        if cause:
            record["cause"] = cause
        self._write(record)
        self._remember(call.session_ref, fingerprint_value)
        # Only the same call failing the same way is a loop; transient errors
        # are retried as the guidance says.
        if kind != "tool_cancelled" and described["category"] != "transient":
            self._check_loop(call, ("error", self._digest(call), fingerprint_value), base=fingerprint_value)
        return stage, kind

    def _record_degraded(self, call: _Call, result: Mapping[str, Any], elapsed: float) -> None:
        reason = redact_text(result.get("result_status_reason") or "", scrub_terms=self._scrub_terms)
        record = self._record(
            "degraded_result",
            "degraded",
            "P4",
            fingerprint("degraded_result", call.tool, reason),
            call.tool,
        )
        record["call"] = self._call_block(call, elapsed, result_status="unavailable")
        record["reason"] = reason
        record["session"] = self._session_block(call)
        self._write(record)

    def _record_slow(
        self,
        call: _Call,
        elapsed: float,
        stage: str,
        where: list[str],
        *,
        outcome: str | None = None,
    ) -> None:
        # By tool only: the stages count in the item shows where it waits.
        record = self._record("slow_call", "slow", "P3", fingerprint("slow_call", call.tool), call.tool)
        record["call"] = self._call_block(call, elapsed, where=where, stage=stage)
        record["call"]["running"] = outcome is None
        if outcome is not None:
            record["call"]["outcome"] = outcome
        record["session"] = self._session_block(call)
        self._write(record)

    def _claim(self, call: _Call, flag: str) -> bool:
        """Set a once-per-call flag; False if the watchdog or end() has it."""

        with self._state_lock:
            if getattr(call, flag):
                return False
            setattr(call, flag, True)
            return True

    # -- agent loops ----------------------------------------------------------

    def _check_loop(
        self,
        call: _Call,
        key_tail: tuple[Any, ...],
        *,
        base: str | None = None,
        status: str | None = None,
        threshold: int = LOOP_THRESHOLD,
        priority: str = "P2",
    ) -> None:
        """Observe-only: N identical outcomes in one session within the window."""

        if call.session_ref is None:
            return
        key = (call.session_ref, call.tool, *key_tail)
        now = self._clock()
        with self._state_lock:
            state = self._loops.pop(key, None) or [[], None]
            hits = [moment for moment in state[0] if now - moment < LOOP_WINDOW_SECONDS]
            hits.append(now)
            state[0] = hits
            self._loops[key] = state
            while len(self._loops) > LOOP_KEY_LIMIT:
                self._loops.pop(next(iter(self._loops)))
            flagged_at = state[1]
            if len(hits) < threshold or (
                flagged_at is not None and now - flagged_at < LOOP_WINDOW_SECONDS
            ):
                return
            state[1] = now
            repeat_count = len(hits)
        record = self._record(
            "agent_loop",
            "agent_loop",
            priority,
            fingerprint("agent_loop", call.tool, base or status),
            call.tool,
        )
        record["loop"] = {
            "repeat_count": repeat_count,
            "window_s": LOOP_WINDOW_SECONDS,
            "base_fingerprint": base,
            "result_status": status,
        }
        record["session"] = self._session_block(call)
        self._write(record)

    def _digest(self, call: _Call) -> str:
        # Salted per process and kept in memory only: equal arguments match,
        # but the digest cannot be reversed or compared across runs.
        text = json.dumps(call.arguments, sort_keys=True, default=str)
        return hashlib.blake2b(text.encode("utf-8"), key=self._salt, digest_size=8).hexdigest()

    def _remember(self, session_ref: str | None, fingerprint_value: str) -> None:
        if session_ref is None:
            return
        with self._state_lock:
            recent = self._recent.pop(session_ref, [])
            self._recent[session_ref] = (recent + [fingerprint_value])[-5:]
            while len(self._recent) > LOOP_KEY_LIMIT:
                self._recent.pop(next(iter(self._recent)))

    # -- swallowed errors -----------------------------------------------------

    def note(self, exc: BaseException, site: str, *, log_template: str | None = None) -> None:
        """Record an error the package caught and did not re-raise."""

        if not self.enabled:
            return
        try:
            call = _CURRENT_CALL.get()
            if call is not None and call.log is not self:
                call = None
            described = describe_exception(
                exc,
                sql_arguments=bool(call and call.sql_arguments),
                scrub_terms=self._scrub_terms,
            )
            priority = "P4" if described["category"] in _QUIET_CATEGORIES else described["priority"]
            label = site if _SITE_LABEL.fullmatch(site) else "[site]"
            record = self._record(
                "swallowed_exception",
                described["category"],
                priority,
                fingerprint("swallowed", label, error_fingerprint(described, call.tool if call else None)),
                call.tool if call else None,
            )
            record.update(site=label, error=described["error"], where=described["where"])
            if log_template:
                record["log_template"] = redact_text(log_template, scrub_terms=self._scrub_terms)
            if call is not None:
                record["session"] = self._session_block(call)
            self._write(record)
        except Exception:
            pass

    def note_condition(self, site: str, *, category: str = "environment", priority: str = "P3") -> None:
        """Record a degraded condition the package logs but does not raise."""

        if not self.enabled:
            return
        try:
            call = _CURRENT_CALL.get()
            if call is not None and call.log is not self:
                call = None
            label = site if _SITE_LABEL.fullmatch(site) else "[site]"
            record = self._record(
                "condition",
                category if category in _CATEGORIES else "environment",
                priority if priority in _PRIORITIES else "P3",
                fingerprint("condition", label),
                call.tool if call else None,
            )
            record["site"] = label
            if call is not None:
                record["session"] = self._session_block(call)
            self._write(record)
        except Exception:
            pass

    def record_process_failure(self, exc: BaseException, *, phase: str) -> None:
        """Record a startup failure or a server exit by exception."""

        leaf = _leaf_exception(exc)
        if not self.enabled or isinstance(
            leaf, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)
        ):
            return
        try:
            described = describe_exception(leaf, scrub_terms=self._scrub_terms)
            kind = "startup_failure" if phase == "startup" else "server_exit"
            # The host closed stdio mid-call: routine, not a crash.
            host_quit = phase == "run" and (
                isinstance(leaf, (BrokenPipeError, ConnectionResetError, EOFError))
                or (
                    type(leaf).__module__.startswith("anyio")
                    and type(leaf).__name__ in _CLOSED_STREAM_ERRORS
                )
            )
            priority = "P1" if described["priority"] == "P1" else "P2"
            record = self._record(
                kind,
                "startup" if phase == "startup" else "server_exit",
                "P4" if host_quit else priority,
                fingerprint(kind, error_fingerprint(described)),
                None,
            )
            record.update(phase=_label(phase), error=described["error"], where=described["where"])
            if host_quit:
                record["cause"] = "host_disconnect"
            self._write(record)
        except Exception:
            pass

    # -- agent self-reports ---------------------------------------------------

    def report_blocker(
        self,
        *,
        skill: str,
        blocker_kind: str,
        summary: str,
        skill_version: str | None = None,
        skill_version_expected: str | None = None,
        last_tool: str | None = None,
        attempts: int = 1,
        related_ids: Collection[str] = (),
        context: Any = None,
        tool_names: Collection[str] = (),
    ) -> dict[str, Any]:
        """Record an agent's own blocker report; repeats coalesce, never error.

        tool_names are kept in the agent's text; other identifier-shaped words go.
        """

        if not self.enabled:
            return {"recorded": False, "reason": self._reason}
        try:
            session_ref, client = self._session_info(context)
            skill_label = skill if re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", skill) else "[invalid]"
            kind_label = blocker_kind if blocker_kind in BLOCKER_KINDS else "other"
            tool_label = _tool_label(last_tool) if last_tool else None
            fingerprint_value = fingerprint("agent_report", skill_label, kind_label, tool_label)
            stored_summary = redact_agent_text(
                summary, scrub_terms=self._scrub_terms, keep_words=tool_names
            )
            # No version given is unknown, not a mismatch.
            mismatch = (
                skill_version_expected is not None
                and skill_version is not None
                and skill_version != skill_version_expected
            )
            outcome = {
                "fingerprint": fingerprint_value,
                "summary": stored_summary,
                "skill_version_mismatch": mismatch,
            }
            record = self._record("agent_report", "agent_report", "P2", fingerprint_value, tool_label)
            now = self._clock()
            with self._state_lock:
                key = session_ref or "no-session"
                history = [entry for entry in self._reports.pop(key, []) if now - entry[0] < 3600]
                self._reports[key] = history
                while len(self._reports) > LOOP_KEY_LIMIT:
                    self._reports.pop(next(iter(self._reports)))
                earlier = next(
                    (
                        incident_id
                        for moment, value, incident_id in history
                        if value == fingerprint_value and now - moment < REPORT_COALESCE_SECONDS
                    ),
                    None,
                )
                if earlier is not None:
                    return {**outcome, "recorded": False, "coalesced": True, "incident_id": earlier}
                if len(history) >= REPORT_RATE_LIMIT:
                    return {**outcome, "recorded": False, "coalesced": False, "reason": "rate_limited"}
                recent = list(self._recent.get(session_ref or "", []))
            record["report"] = {
                "skill": skill_label,
                "skill_version": _version_label(skill_version),
                "skill_version_expected": _version_label(skill_version_expected),
                "skill_version_mismatch": mismatch,
                "blocker_kind": kind_label,
                "last_tool": tool_label,
                "summary": stored_summary,
                "attempts": min(max(int(attempts), 1), 100),
                "related_ids": [item for item in related_ids if _RELATED_ID.fullmatch(str(item))][:5],
                "recent_fingerprints": recent,
            }
            record["session"] = {"session_ref": session_ref, "client": client}
            dropped = self._write(record)
            if dropped is not None:  # a later repeat must not get an id that was never written
                return {**outcome, "recorded": False, "coalesced": False, "reason": dropped}
            with self._state_lock:
                history.append((now, fingerprint_value, record["incident_id"]))
            return {
                **outcome,
                "recorded": True,
                "coalesced": False,
                "incident_id": record["incident_id"],
            }
        except Exception:
            return {"recorded": False, "reason": "internal_error"}

    # -- lifecycle ------------------------------------------------------------

    def start(self) -> None:
        """Reconcile crash orphans, prune, then start the watchdog."""

        global _ACTIVE_LOG
        if not self.enabled or self._thread is not None:
            return
        try:
            self._reconcile_orphans()
        except Exception:
            pass  # a bad journal never stops the watchdog
        self.prune()
        try:
            self._main_task = asyncio.current_task()
        except RuntimeError:
            self._main_task = None
        self._install_sigterm()
        try:
            self._handler = _SwallowedErrorHandler(self)
            logging.getLogger("azure_sql_mcp").addHandler(self._handler)
            _ACTIVE_LOG = self
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run_watchdog,
                name="azure-sql-mcp-incident-watchdog",
                daemon=True,
            )
            self._thread.start()
        except Exception:
            pass

    def close(self) -> None:
        """Stop the watchdog; calls still open become shutdown orphans."""

        global _ACTIVE_LOG
        try:
            self._stop.set()
            if self._thread is not None:
                self._thread.join(timeout=self._tick_seconds + self._capture_timeout + 1)
                self._thread = None
            if self._handler is not None:
                logging.getLogger("azure_sql_mcp").removeHandler(self._handler)
                self._handler = None
            if _ACTIVE_LOG is self:
                _ACTIVE_LOG = None
            try:
                if signal.getsignal(signal.SIGTERM) == self._on_sigterm:
                    self._restore_sigterm()
            except (TypeError, ValueError, OSError):
                pass
            if self.enabled:
                now = self._clock()
                with self._state_lock:
                    calls = list(self._inflight.values())
                    self._inflight.clear()
                for call in calls:
                    self._record_orphan(
                        call.tool,
                        elapsed=now - call.started,
                        reason="shutdown",
                        started_utc=call.started_utc,
                        server_version=self._runtime["server_version"],
                    )
            if self.directory is not None:
                self._journal_path().unlink(missing_ok=True)
            with self._write_lock:
                if self._fd is not None:
                    os.close(self._fd)
                    self._fd = None
        except Exception:
            pass

    def _install_sigterm(self) -> None:
        """On SIGTERM, journal open calls as a shutdown, then die as before."""

        try:
            if threading.current_thread() is threading.main_thread():
                self._previous_sigterm = signal.signal(signal.SIGTERM, self._on_sigterm)
        except (AttributeError, OSError, ValueError):
            pass

    def _on_sigterm(self, signum: int, _frame: FrameType | None) -> None:
        # No locks here: the interrupted code may hold them.
        try:
            self._shutting_down = True
            self._write_journal(list(self._inflight.values()), self._clock())
        except Exception:
            pass
        try:
            self._restore_sigterm()
        except (TypeError, ValueError, OSError):
            signal.signal(signum, signal.SIG_DFL)
        signal.raise_signal(signum)

    def _restore_sigterm(self) -> None:
        previous = self._previous_sigterm
        signal.signal(signal.SIGTERM, signal.SIG_DFL if previous is None else previous)

    # -- watchdog -------------------------------------------------------------

    def _run_watchdog(self) -> None:
        while not self._stop.wait(self._tick_seconds):
            self.tick()

    def tick(self) -> None:
        """One watchdog pass over a snapshot of the calls in flight."""

        if not self.enabled:
            return
        try:
            now = self._clock()
            with self._state_lock:
                calls = list(self._inflight.values())
            blocked: dict[int, list[str]] = {}
            for call in calls:
                elapsed = now - call.started
                slow = not call.slow_flagged and elapsed >= self.slow_seconds
                stuck = (
                    not call.stuck_flagged
                    and call.budget_s is not None
                    and elapsed >= call.budget_s + STUCK_GRACE_SECONDS
                )
                if not (slow or stuck):
                    continue
                loop_key = id(call.loop)
                if loop_key in blocked:
                    where, loop_blocked = blocked[loop_key], True
                else:
                    where, loop_blocked = self._capture_where(call)
                    if loop_blocked:
                        blocked[loop_key] = where
                # The call may end during the capture: end() and this tick
                # each claim a flag before they write.
                slow = slow and self._claim(call, "slow_flagged")
                stuck = stuck and self._claim(call, "stuck_flagged")
                if loop_blocked and (slow or stuck):
                    self._record_running(call, "event_loop_blocked", elapsed, where)
                elif stuck:
                    self._record_running(call, "stuck_call", elapsed, where)
                if slow and not loop_blocked:
                    call.seen_where = where
                    self._record_slow(call, elapsed, _stage(where) or "unobserved", where)
            self._sync_journal()
            if self._pruned_day != self._utc_now().date():
                self.prune()
        except Exception:
            pass
        try:
            if self._on_tick is not None:
                self._on_tick()  # e.g. connections held past their lease
        except Exception:
            pass

    def _record_running(self, call: _Call, kind: str, elapsed: float, where: list[str]) -> None:
        stage = _stage(where) or "unobserved"
        # A blocked loop blocks every call, so it groups by where it blocks.
        parts = (kind, stage) if kind == "event_loop_blocked" else (kind, call.tool, stage)
        record = self._record(kind, "stuck", "P1", fingerprint(*parts), call.tool)
        record["call"] = self._call_block(call, elapsed, where=where, stage=stage)
        record["call"]["past_budget"] = (
            call.budget_s is not None and elapsed >= call.budget_s + STUCK_GRACE_SECONDS
        )
        record["session"] = self._session_block(call)
        self._write(record)

    def _capture_where(self, call: _Call) -> tuple[list[str], bool]:
        """Await-chain frames of the call's task; loop frames if it is blocked."""

        loop, task = call.loop, call.task
        if loop is None or task is None or loop.is_closed():
            return [], False
        if threading.get_ident() == call.loop_thread:
            return _await_chain_where(task), False
        done = threading.Event()
        found: list[list[str]] = []

        def capture() -> None:
            try:
                found.append(_await_chain_where(task))
            finally:
                done.set()

        try:
            loop.call_soon_threadsafe(capture)
        except RuntimeError:
            return [], False
        if done.wait(self._capture_timeout):
            return (found[0] if found else []), False
        if self._stop.is_set():  # close() holds the loop while it joins this thread
            return [], False
        frame = sys._current_frames().get(call.loop_thread or 0)
        return _stack_where(frame), True

    # -- liveness journal and orphans -----------------------------------------

    def _journal_path(self) -> Path:
        assert self.directory is not None
        return self.directory / f"inflight-{os.getpid()}-{self.run_id}.json"

    def _sync_journal(self) -> None:
        # One writer at a time, each with a fresh snapshot: the last write
        # always matches the calls still open.
        with self._journal_lock:
            with self._state_lock:
                calls = list(self._inflight.values())
            self._write_journal(calls, self._clock())

    def _write_journal(self, calls: list[_Call], now: float) -> None:
        """Calls alive at a tick are journaled: no I/O on the hot path until
        a journaled call ends, which rewrites the journal at once."""

        path = self._journal_path()
        if not calls:
            if self._journal_written:
                path.unlink(missing_ok=True)
                self._journal_written = False
            return
        payload = {
            "schema": JOURNAL_SCHEMA,
            "pid": os.getpid(),
            "run_id": self.run_id,
            "server_version": self._runtime["server_version"],
            "written_utc": _iso(self._utc_now()),
            "shutting_down": self._shutting_down,
            "calls": [
                {
                    "call_id": call.call_id,
                    "tool": call.tool,
                    "started_utc": call.started_utc,
                    "elapsed_s": round(now - call.started, 1),
                }
                for call in calls
            ],
        }
        # Per thread: the SIGTERM handler may write while the watchdog does.
        temporary = path.with_name(f".{path.name}.{threading.get_ident()}.tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            _fchmod_private(fd)
            os.write(fd, _encode(payload))
        finally:
            os.close(fd)
        os.replace(temporary, path)
        self._journal_written = True
        for call in calls:
            call.journaled = True

    def _reconcile_orphans(self) -> None:
        if self.directory is None:
            return
        for path, payload in dead_journals(self.directory, exclude_run=self.run_id):
            # Servers often start together: the one that renames a journal
            # records it; the others skip it.
            claimed = path.with_name(f"{path.name}.claimed-{self.run_id}")
            try:
                os.rename(path, claimed)
            except OSError:
                continue
            try:
                shutting_down = payload.get("shutting_down") is True
                for item in payload.get("calls") or []:
                    if isinstance(item, dict):
                        self._record_orphan(
                            _tool_label(item.get("tool")),
                            elapsed=_number(item.get("elapsed_s")),
                            reason="shutdown" if shutting_down else "process_exit",
                            started_utc=_timestamp_label(item.get("started_utc")),
                            server_version=_label(payload.get("server_version")),
                            last_journal_write_utc=_timestamp_label(payload.get("written_utc")),
                        )
            except Exception:
                pass  # a malformed journal is dropped
            finally:
                claimed.unlink(missing_ok=True)

    def _record_orphan(
        self,
        tool: str,
        *,
        elapsed: float,
        reason: str,
        started_utc: str | None,
        server_version: str,
        last_journal_write_utc: str | None = None,
    ) -> None:
        record = self._record(
            "orphaned_call",
            "orphaned",
            _orphan_priority(reason, elapsed, self.slow_seconds),
            fingerprint("orphaned_call", tool, reason),
            tool,
        )
        record["reason"] = reason
        record["orphan"] = {
            "elapsed_s": round(elapsed, 1),
            "started_utc": started_utc,
            "last_journal_write_utc": last_journal_write_utc,
            "server_version": server_version,
        }
        self._write(record)

    # -- storage --------------------------------------------------------------

    def prune(self) -> None:
        """Delete own daily files past retention, then oldest past the size cap."""

        if self.directory is None:
            return
        try:
            today = self._utc_now().date()
            self._pruned_day = today
            cutoff = today - timedelta(days=self.retention_days)
            kept: list[tuple[date, str, int]] = []
            for entry in os.scandir(self.directory):
                match = _DAY_FILE.match(entry.name)
                if match is None:
                    continue
                # One bad name or locked file must not stop retention or the cap.
                try:
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    day = date.fromisoformat(match.group(1))
                    if day < cutoff:
                        os.unlink(entry.path)
                    else:
                        kept.append((day, entry.path, entry.stat(follow_symlinks=False).st_size))
                except (OSError, ValueError):
                    continue
            total = sum(size for _day, _path, size in kept)
            for day, path, size in sorted(kept):
                if total <= MAX_TOTAL_BYTES:
                    break
                if day < today:  # today's file may be open in other processes
                    try:
                        os.unlink(path)
                    except OSError:
                        continue
                    total -= size
        except Exception:
            pass

    def _prepare_directory(self) -> None:
        assert self.directory is not None
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            if os.name != "nt":
                os.chmod(self.directory, 0o700)
                if stat.S_IMODE(self.directory.stat().st_mode) != 0o700:
                    raise OSError("incident directory is not private")
        except OSError:
            self._disable("insecure_dir")

    def _disable(self, reason: str) -> None:
        if self._reason is None:
            self._reason = reason
            logger.warning("Incident log disabled for this process.", extra={"reason": reason})

    def _record(
        self,
        kind: str,
        category: str,
        priority: str,
        fingerprint_value: str,
        tool: str | None,
    ) -> dict[str, Any]:
        return {
            "schema": SCHEMA,
            "incident_id": uuid.uuid4().hex,
            "ts_utc": _iso(self._utc_now()),
            "kind": kind,
            "category": category,
            "priority": priority,
            "fingerprint": fingerprint_value,
            "tool": tool,
            "runtime": self._runtime,
        }

    def _write(self, record: dict[str, Any]) -> str | None:
        """Append one line; returns None, or why the record was dropped."""

        if not self.enabled or self.directory is None:
            return self._reason or "disabled"
        line = _encode(record)
        day = str(record["ts_utc"])[:10]
        # P3 and P4 stop at 80% of the day, so routine noise never crowds
        # out a later crash or stuck call.
        limit = DAY_BYTE_CAP if record["priority"] in ("P1", "P2") else DAY_BYTE_CAP * 4 // 5
        dropped = None
        try:
            with self._write_lock:
                fd = self._day_fd(day)
                info = os.fstat(fd)
                if info.st_nlink == 0:  # deleted while open: recreate dir and file
                    os.close(fd)
                    self._fd = None
                    self._prepare_directory()
                    if not self.enabled:
                        return self._reason
                    fd = self._day_fd(day)
                    info = os.fstat(fd)
                # The size of the shared file bounds every process together.
                if info.st_size + len(line) > limit:
                    dropped = "day_cap_reached"
                    if self._cap_marked_day == day:
                        return dropped
                    self._cap_marked_day = day
                    line = _encode(
                        self._record(
                            "log_cap_reached",
                            "incident_log",
                            "P3",
                            fingerprint("log_cap_reached"),
                            None,
                        )
                    )
                os.write(fd, line)
        except OSError:
            self._failures += 1
            if self._failures >= 3:
                self._disable("write_failed")
            return "write_failed"
        self._failures = 0
        self._written += dropped is None
        return dropped

    def _day_fd(self, day: str) -> int:
        if self._fd is not None and self._fd_day == day:
            return self._fd
        assert self.directory is not None
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        fd = os.open(
            self.directory / f"incidents-{day}.jsonl",
            os.O_APPEND | os.O_CREAT | os.O_WRONLY,
            0o600,
        )
        _fchmod_private(fd)
        self._fd, self._fd_day = fd, day
        return fd

    # -- helpers --------------------------------------------------------------

    def _declared(self, tool: str) -> Collection[str]:
        if self._declared_for is None:
            return ()
        try:
            return self._declared_for(tool) or ()
        except Exception:
            return ()

    def _budget(self, tool: str, arguments: Mapping[str, Any]) -> float | None:
        if self._budget_for is None:
            return None
        try:
            budget = self._budget_for(tool, arguments)
        except Exception:
            return None
        return float(budget) if budget is not None and math.isfinite(budget) else None

    def _session_info(self, context: Any) -> tuple[str | None, dict[str, str] | None]:
        try:
            session = context.request_context.session
        except Exception:
            return None, None
        if session is None:
            return None, None
        try:
            session_ref = self._session_refs.get(session)
            if session_ref is None:
                session_ref = self._session_refs[session] = uuid.uuid4().hex[:12]
        except TypeError:  # not weak-referenceable: an id can be reused later
            session_ref = hashlib.sha256(f"{self.run_id}:{id(session)}".encode()).hexdigest()[:12]
        client = None
        info = getattr(getattr(session, "client_params", None), "clientInfo", None)
        if info is not None:
            client = {
                "name": _label(getattr(info, "name", "")),
                "version": _label(getattr(info, "version", "")),
            }
        return session_ref, client

    @staticmethod
    def _session_block(call: _Call) -> dict[str, Any]:
        return {"session_ref": call.session_ref, "client": call.client}

    @staticmethod
    def _call_block(
        call: _Call,
        elapsed: float,
        *,
        where: list[str] | None = None,
        stage: str | None = None,
        result_status: str | None = None,
    ) -> dict[str, Any]:
        block: dict[str, Any] = {
            "call_id": call.call_id,
            "elapsed_ms": round(elapsed * 1000),
            "budget_s": call.budget_s,
            "argument_keys": call.argument_keys,
            "unknown_argument_count": call.unknown_argument_count,
            "where": where or [],
            "stage": stage,
            "result_status": result_status,
        }
        return {key: value for key, value in block.items() if value is not None}


class _SwallowedErrorHandler(logging.Handler):
    """Secondary net: logger.exception records from package code."""

    def __init__(self, log: IncidentLog) -> None:
        super().__init__(logging.ERROR)
        self._log = log

    def emit(self, record: logging.LogRecord) -> None:
        try:
            exc = record.exc_info[1] if record.exc_info else None
            if exc is None or record.name == __name__:
                return
            # The template, never getMessage(): arguments may carry values.
            self._log.note(exc, f"log:{record.name}", log_template=str(record.msg))
        except Exception:
            pass


def note_exception(exc: BaseException, site: str) -> None:
    """Record an error a package aggregator swallowed; no-op without a log."""

    try:
        call = _CURRENT_CALL.get()
        log = call.log if call is not None else _ACTIVE_LOG
        if log is not None:
            log.note(exc, site)
    except Exception:
        pass


def note_condition(site: str, *, category: str = "environment", priority: str = "P3") -> None:
    try:
        call = _CURRENT_CALL.get()
        log = call.log if call is not None else _ACTIVE_LOG
        if log is not None:
            log.note_condition(site, category=category, priority=priority)
    except Exception:
        pass


def record_startup_failure(
    exc: BaseException,
    *,
    argv: list[str] | None = None,
    phase: str = "startup",
    environ: Mapping[str, str] | None = None,
) -> None:
    """Best effort before config exists: flags over env, as config reads them."""

    # --help, bad flags and Ctrl+C are not failures: record and create nothing.
    if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
        return
    try:
        env = dict(os.environ if environ is None else environ)
        try:
            args, _unknown = build_arg_parser().parse_known_args(argv)
            env.update({name.upper(): value for name, value in vars(args).items() if value is not None})
        except (Exception, SystemExit):
            pass
        settings = parse_incident_settings(env)
        if not settings.enabled:
            return
        log = IncidentLog.from_settings(
            settings,
            performance_state_dir=env.get("AZURE_SQL_PERFORMANCE_STATE_DIR") or DEFAULT_STATE_DIR,
            server_version=_package_version("azure-sql-mcp"),
            scrub_terms=scrub_terms_from_env(env),
        )
        log.record_process_failure(exc, phase=phase)
        log.close()
    except Exception:
        pass


def dead_journals(directory: Path, *, exclude_run: str | None = None) -> list[tuple[Path, dict[str, Any]]]:
    """Journals whose owner exited: stale, and (on POSIX) the pid is gone."""

    found: list[tuple[Path, dict[str, Any]]] = []
    try:
        entries = list(os.scandir(directory))
    except OSError:
        return found
    for entry in entries:
        match = _JOURNAL_FILE.match(entry.name)
        if match is None or match.group(2) == exclude_run:
            continue
        try:
            age = time.time() - entry.stat(follow_symlinks=False).st_mtime
            if age < JOURNAL_STALE_SECONDS:
                continue
            # Past a day the pid may have been reused; treat the owner as gone.
            if age < 86_400 and _pid_alive(int(match.group(1))):
                continue
            payload = json.loads(Path(entry.path).read_text(encoding="utf-8"))
        except Exception:  # unreadable, too deep, or a pid out of range
            continue
        if isinstance(payload, dict) and payload.get("schema") == JOURNAL_SCHEMA:
            found.append((Path(entry.path), payload))
    return found


def _pid_alive(pid: int) -> bool:
    if os.name == "nt":
        return False  # signal 0 would terminate the process on Windows
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _responder(context: Any) -> Any:
    """The MCP request's responder; its `cancelled` is set only by the client."""

    try:
        request_context = context.request_context
        return request_context.session._in_flight.get(request_context.request_id)
    except Exception:
        return None


def _await_chain_where(task: asyncio.Task[Any]) -> list[str]:
    labels: list[str] = []
    awaitable: Any = task.get_coro()
    for _ in range(200):
        if awaitable is None:
            break
        frame = getattr(awaitable, "cr_frame", None) or getattr(awaitable, "gi_frame", None)
        if frame is not None:
            label = _frame_label(frame)
            if label:
                labels.append(label)
        awaitable = getattr(awaitable, "cr_await", None) or getattr(awaitable, "gi_yieldfrom", None)
    return labels[-3:]


def _stack_where(frame: FrameType | None) -> list[str]:
    labels: list[str] = []
    while frame is not None and len(labels) < 3:
        label = _frame_label(frame)
        if label:
            labels.append(label)
        frame = frame.f_back
    return list(reversed(labels))


def _frame_label(frame: FrameType) -> str | None:
    module = str(frame.f_globals.get("__name__", ""))
    if not module.startswith(_PACKAGE_PREFIX) or module == __name__:
        return None
    return f"{module}:{frame.f_code.co_qualname}"


def _orphan_priority(reason: str, elapsed: float, slow_seconds: float) -> str:
    # A user abort looks like a crash; only a long-running call is a P2.
    return "P2" if reason == "process_exit" and elapsed >= slow_seconds else "P4"


def _version_label(value: object) -> str | None:
    text = str(value or "")
    return text if re.fullmatch(r"[0-9][0-9A-Za-z.+-]{0,31}", text) else None


def _number(value: object) -> float:
    return float(value) if isinstance(value, (int, float)) and math.isfinite(value) else 0.0


def _timestamp_label(value: object) -> str | None:
    text = str(value or "")
    return text if re.fullmatch(r"[0-9T:.\-]{10,30}Z?", text) else None


def _fchmod_private(fd: int) -> None:
    # os.fchmod is missing on Windows before Python 3.13; there the mode is advisory.
    fchmod = getattr(os, "fchmod", None)
    if fchmod is not None:
        fchmod(fd, 0o600)


def _encode(record: Mapping[str, Any]) -> bytes:
    text = json.dumps(record, sort_keys=True, ensure_ascii=True, separators=(",", ":"), default=str)
    return (text + "\n").encode("ascii")


def _label(value: object) -> str:
    return re.sub(r"[^A-Za-z0-9._ -]", "", str(value))[:64]


def _tool_label(tool: object) -> str:
    text = str(tool)
    return text if re.fullmatch(r"[a-z][a-z0-9_]{0,63}", text) else "[invalid]"


def _package_version(name: str) -> str:
    try:
        return package_version(name)
    except Exception:
        return "unknown"


# --- backlog -------------------------------------------------------------------

BACKLOG_SCHEMA = "azure-sql-mcp-incident-backlog/1"
_TIMED_KINDS = frozenset({"tool_timeout", "tool_cancelled", "slow_call", "stuck_call", "orphaned_call"})
_BUILTIN_BUG_NAMES = frozenset(cls.__name__ for cls in _BUILTIN_BUGS)
_NEXT_STEPS = {
    "product_bug": "Reproduce with a unit test at the frame shown, then fix it.",
    "product_gap": "Add a synthetic SQL case with this construct to tests/unit/test_safe_sql.py, then support it.",
    "outcome_unknown": "Check the target state by hand, then make this write path report a definite outcome.",
    "state_store": "Inspect the local state store and add a test for the failing store path.",
    "contract_drift": "Align the skill or server instructions with the tools this profile exposes.",
    "caller_error": "Improve the tool description or skill guidance so agents send valid input.",
    "policy": "Confirm the policy is intended; adjust skill guidance if agents keep hitting it.",
    "transient": "Confirm retry coverage for this path; no fix is needed if it stays rare.",
    "environment": "Check permissions, firewall and login configuration for this deployment.",
    "timeout": "Find what waits in this stage; tune the work, the budget or partial results.",
    "cancelled": "Check whether the client gives up before the server budget; return partial results sooner.",
    "slow": "Find what waits in this stage; hosts often give up near 60 seconds.",
    "stuck": "A timeout did not fire: find the wait that ignores cancellation in this stage.",
    "orphaned": "Check host logs at this time for a crash or kill during the call.",
    "degraded": "Check the permission or feature the reason names.",
    "agent_loop": "Make the result say what to do next so agents stop repeating the call.",
    "agent_report": "Fix the skill text or tool behavior the agent reports.",
    "agent_abandoned": "Consider expiring or auto-finalizing abandoned work.",
    "stalled_workflow": "Recover the item by hand, then add a test for the interrupted transition.",
    "startup": "Fix the configuration or crash shown; the server was unavailable.",
    "server_exit": "Fix the crash shown; the server stopped serving.",
    "incident_log": "Fix the repeating incident that filled the daily cap.",
}


def build_backlog(
    incident_dir: Path | None,
    *,
    performance_db: Path | None = None,
    since_days: int = 14,
    min_priority: str = "P3",
    max_items: int = 20,
    scrub_terms: Mapping[str, str] | None = None,
    include_summaries: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Group incidents into a redacted, prioritized fix backlog. Read-only."""

    until = now or _utc_now()
    since = until - timedelta(days=since_days)
    terms = dict(scrub_terms or {})
    records, unparsed = _read_records(incident_dir, since, until)
    records = _merge_slow_calls(records)
    if incident_dir is not None:
        records.extend(_journal_orphans(incident_dir))
    stalled, stall_scan = _stalled_workflows(performance_db, until)
    records.extend(stalled)

    groups: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        groups.setdefault(str(record.get("fingerprint")), []).append(record)
    kept: list[dict[str, Any]] = []
    withheld = 0
    for fingerprint_value, group in groups.items():
        item = _backlog_item(fingerprint_value, group, terms, include_summaries)
        if _leaks(item, terms):
            withheld += 1
        else:
            kept.append(item)
    visible = [item for item in kept if item["priority"] <= min_priority]
    visible.sort(key=lambda item: item["last_seen_utc"], reverse=True)
    visible.sort(key=lambda item: (item["priority"], -item["count"]))
    shown = visible[:max_items]
    return {
        "schema": BACKLOG_SCHEMA,
        "generated_at_utc": _iso(until),
        "window": {"since_utc": _iso(since), "until_utc": _iso(until), "days": since_days},
        "server_versions": sorted(
            {str(r.get("runtime", {}).get("server_version")) for r in records if r.get("runtime")}
        ),
        "totals": {
            "incidents": len(records),
            "items": len(shown),
            "by_category": _counts(item["category"] for item in kept),
            "by_priority": _counts(item["priority"] for item in kept),
            "below_min_priority": len(kept) - len(visible),
            "withheld": withheld,
            "unparsed": unparsed,
            "stalled_workflow_scan": stall_scan,
        },
        "items": shown,
    }


def summarize_backlog(backlog: Mapping[str, Any]) -> dict[str, Any]:
    """Titles, counts, priorities and fingerprints only: safe to show an agent."""

    return {
        "schema": backlog.get("schema"),
        "window": backlog.get("window"),
        "totals": backlog.get("totals"),
        "items": [
            {
                key: item.get(key)
                for key in ("fingerprint", "title", "priority", "category", "count", "last_seen_utc")
            }
            for item in backlog.get("items", [])
        ],
    }


def render_markdown(backlog: Mapping[str, Any]) -> str:
    window = backlog.get("window", {})
    totals = backlog.get("totals", {})
    lines = [
        "# azure-sql-mcp incident backlog",
        "",
        "Redacted local export. Review before filing in a public repo.",
        "",
        f"Window: {window.get('since_utc')} to {window.get('until_utc')} ({window.get('days')} days).",
        f"Server versions: {', '.join(backlog.get('server_versions') or []) or 'none'}.",
        f"Totals: {totals.get('incidents', 0)} incidents, {totals.get('items', 0)} items shown, "
        f"{totals.get('below_min_priority', 0)} below the priority filter, "
        f"{totals.get('withheld', 0)} withheld, {totals.get('unparsed', 0)} unparsed lines.",
        "",
        "| Priority | Items |",
        "|---|---|",
    ]
    for priority in _PRIORITIES:
        lines.append(f"| {priority} | {totals.get('by_priority', {}).get(priority, 0)} |")
    for item in backlog.get("items", []):
        lines += [
            "",
            f"### [{item['priority']}] {item['title']}",
            "",
            f"- Category: {item['category']} ({item['kind']})",
            f"- Count: {item['count']} in {item['sessions']} sessions; "
            f"first {item['first_seen_utc']}, last {item['last_seen_utc']}",
            f"- Tools: {', '.join(item['tools']) or 'none'}",
            f"- Versions: {', '.join(item['server_versions']) or 'unknown'}; "
            f"clients: {', '.join(item['clients']) or 'unknown'}",
        ]
        if item["native_error_codes"] or item["sqlstates"]:
            codes = ", ".join(str(code) for code in item["native_error_codes"]) or "none"
            lines.append(f"- Native codes: {codes}; SQLSTATE: {', '.join(item['sqlstates']) or 'none'}")
        if item["stages"]:
            lines.append(
                "- Stages: " + ", ".join(f"{stage} {count}" for stage, count in item["stages"].items())
            )
        for summary in item.get("summaries", []):
            lines.append(f"- Agent summary: {summary}")
        lines += [
            f"- Next step: {item['suggested_next_step']}",
            f"- Labels: {', '.join(item['labels'])}",
            "",
            "<details><summary>Example (redacted)</summary>",
            "",
            "```json",
            json.dumps(item["example"], indent=2, sort_keys=True, ensure_ascii=True),
            "```",
            "",
            "</details>",
            "",
            f"Fingerprint: `{item['fingerprint']}`",
            "",
            "- [ ] filed",
        ]
    return "\n".join(lines) + "\n"


def _read_records(
    incident_dir: Path | None,
    since: datetime,
    until: datetime,
) -> tuple[list[dict[str, Any]], int]:
    records: list[dict[str, Any]] = []
    unparsed = 0
    if incident_dir is None or not incident_dir.is_dir():
        return records, unparsed
    since_text, until_text = _iso(since), _iso(until)
    for path in sorted(incident_dir.glob("incidents-*.jsonl")):
        match = _DAY_FILE.match(path.name)
        if match is None or match.group(1) < since_text[:10]:
            continue
        try:
            handle = path.open(encoding="utf-8", errors="replace")
        except OSError:
            continue
        with handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except ValueError:
                    unparsed += 1
                    continue
                if not isinstance(record, dict) or record.get("schema") != SCHEMA:
                    unparsed += 1
                    continue
                if since_text <= str(record.get("ts_utc")) <= until_text:
                    records.append(record)
    return records, unparsed


def _merge_slow_calls(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One slow call is one record: the longest, which is its final one."""

    longest: dict[str, dict[str, Any]] = {}
    merged: list[dict[str, Any]] = []
    for record in records:
        call_id = (record.get("call") or {}).get("call_id")
        if record.get("kind") != "slow_call" or not isinstance(call_id, str):
            merged.append(record)
        elif _number(record["call"].get("elapsed_ms")) >= _number(
            (longest.get(call_id) or {}).get("call", {}).get("elapsed_ms")
        ):
            longest[call_id] = record
    return merged + list(longest.values())


def _journal_orphans(incident_dir: Path) -> list[dict[str, Any]]:
    """Calls a dead process never finished, before the next start reconciles."""

    records: list[dict[str, Any]] = []
    for _path, payload in dead_journals(incident_dir):
        reason = "shutdown" if payload.get("shutting_down") is True else "process_exit"
        version = _label(payload.get("server_version"))
        for item in payload.get("calls") or []:
            if not isinstance(item, dict):
                continue
            tool = _tool_label(item.get("tool"))
            elapsed = _number(item.get("elapsed_s"))
            records.append(
                {
                    "schema": SCHEMA,
                    "ts_utc": _timestamp_label(payload.get("written_utc")) or "",
                    "kind": "orphaned_call",
                    "category": "orphaned",
                    "priority": _orphan_priority(reason, elapsed, 60),
                    "fingerprint": fingerprint("orphaned_call", tool, reason),
                    "tool": tool,
                    "reason": reason,
                    "orphan": {"elapsed_s": round(elapsed, 1), "server_version": version},
                    "runtime": {"server_version": version},
                }
            )
    return records


def _stalled_workflows(
    database: Path | None,
    now: datetime,
) -> tuple[list[dict[str, Any]], str]:
    """Durable work left mid-transition, read through a read-only connection.

    Designed states are not flagged: finalize leaves candidates non-terminal,
    reservations expire lazily and expired leases are cleaned in sandbox only.
    """

    if database is None or not database.is_file():
        return [], "skipped"
    try:
        connection = sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True, timeout=1.0)
    except sqlite3.Error:
        return [], "unavailable"
    found: list[dict[str, Any]] = []
    status = "ok"
    checks = (
        (
            "tuning_session",
            "SELECT session_id, json_extract(payload, '$.status'), updated_at_utc, "
            "json_extract(payload, '$.deadline_at_utc') FROM tuning_sessions "
            "WHERE json_extract(payload, '$.status') IN ('created', 'screening', 'finalist_validation')",
        ),
        (
            "index_lease",
            "SELECT lease_id, status, updated_at_utc, metadata FROM index_leases "
            "WHERE status = 'cleanup_required'",
        ),
        (
            "view_change_intent",
            "SELECT change_id, status, updated_at_utc, NULL FROM view_change_intents "
            "WHERE status = 'applying'",
        ),
        (
            "plan_action_intent",
            "SELECT intent_id, json_extract(payload, '$.status'), updated_at_utc, NULL "
            "FROM plan_action_intents WHERE json_extract(payload, '$.status') "
            "IN ('applying', 'rolling_back', 'unknown')",
        ),
        (
            "performance_case",
            "SELECT case_id, json_extract(payload, '$.status'), updated_at_utc, NULL "
            "FROM performance_cases WHERE json_extract(payload, '$.status') = 'open'",
        ),
    )
    try:
        connection.execute("PRAGMA busy_timeout = 1000")
        for subject_type, query in checks:
            try:
                rows = connection.execute(query).fetchall()
            except sqlite3.Error:
                status = "partial"
                continue
            for subject_id, subject_status, updated, extra in rows:
                stalled = _stall_rule(subject_type, now, _parse_utc(updated), extra)
                if stalled is not None:
                    found.append(
                        _stalled_record(subject_type, str(subject_id), str(subject_status), updated, stalled, now)
                    )
    finally:
        connection.close()
    return found, status


def _stall_rule(
    subject_type: str,
    now: datetime,
    updated: datetime | None,
    extra: Any,
) -> tuple[str, str, str, str | None] | None:
    """Return (reason, category, priority, recovery error type) or None."""

    age = (now - updated) if updated is not None else timedelta(0)
    if subject_type == "tuning_session":
        deadline = _parse_utc(extra)
        if deadline is not None and now - deadline > timedelta(minutes=5):
            return "deadline_passed_unfinalized", "agent_abandoned", "P4", None
        if deadline is None and age > timedelta(minutes=60):
            return "no_progress", "agent_abandoned", "P4", None
        return None
    if subject_type == "index_lease":
        # A temporary index may still exist on the database.
        try:
            error_type = json.loads(extra or "{}").get("recovery_error_type")
        except (ValueError, AttributeError):
            error_type = None
        label = error_type if isinstance(error_type, str) and _IDENTIFIER_TOKEN.fullmatch(error_type) else None
        return "cleanup_required", "stalled_workflow", "P1", label
    if subject_type in ("view_change_intent", "plan_action_intent"):
        if age > timedelta(minutes=15):
            return "apply_unconfirmed", "stalled_workflow", "P1", None
        return None
    if subject_type == "performance_case" and age > timedelta(days=7):
        return "abandoned_case", "agent_abandoned", "P4", None
    return None


def _stalled_record(
    subject_type: str,
    subject_id: str,
    subject_status: str,
    updated: Any,
    rule: tuple[str, str, str, str | None],
    now: datetime,
) -> dict[str, Any]:
    reason, category, priority, error_type = rule
    updated_at = _parse_utc(updated)
    subject = {
        "type": subject_type,
        "id": subject_id,
        "status": _label(subject_status),
        "reason": reason,
        "age_s": round((now - updated_at).total_seconds()) if updated_at else None,
        "recovery_error_type": error_type,
    }
    return {
        "schema": SCHEMA,
        "ts_utc": _iso(updated_at or now),
        "kind": "stalled_workflow",
        "category": category,
        "priority": priority,
        "fingerprint": fingerprint("stalled_workflow", subject_type, subject["status"], reason),
        "tool": None,
        "subject": {key: value for key, value in subject.items() if value is not None},
    }


def _backlog_item(
    fingerprint_value: str,
    group: list[dict[str, Any]],
    terms: Mapping[str, str],
    include_summaries: bool,
) -> dict[str, Any]:
    group.sort(key=lambda record: str(record.get("ts_utc")))
    newest = group[-1]
    category = str(newest.get("category"))
    priority = min(
        (str(r.get("priority")) for r in group if r.get("priority") in _PRIORITIES),
        default="P4",
    )
    sessions = {
        (r.get("session") or {}).get("session_ref")
        for r in group
        if (r.get("session") or {}).get("session_ref")
    }
    # Volume lifts P3 to P2; P4 (caller friction, degraded, short cancels,
    # abandoned work) stays out of the default export however often it occurs.
    if priority == "P3" and (len(group) >= 20 or len(sessions) >= 3):
        priority = "P2"
    calls = [r.get("call") or {} for r in group]
    errors = [r.get("error") or {} for r in group]
    elapsed = sorted(
        float(call.get("elapsed_ms") or (r.get("orphan") or {}).get("elapsed_s", 0) * 1000)
        for r, call in zip(group, calls)
        if r.get("kind") in _TIMED_KINDS
    )
    example = _example(newest, terms, include_summaries)
    item: dict[str, Any] = {
        "fingerprint": fingerprint_value,
        "priority": priority,
        "category": category,
        "kind": newest.get("kind"),
        "tools": sorted({str(r["tool"]) for r in group if r.get("tool")}),
        "count": len(group),
        "sessions": len(sessions),
        "subjects": len({(r.get("subject") or {}).get("id") for r in group if r.get("subject")}),
        "first_seen_utc": group[0].get("ts_utc"),
        "last_seen_utc": newest.get("ts_utc"),
        "server_versions": _distinct((r.get("runtime") or {}).get("server_version") for r in group),
        "mcp_versions": _distinct((r.get("runtime") or {}).get("mcp_version") for r in group),
        "clients": _distinct(
            f"{client.get('name')} {client.get('version')}"
            for client in ((r.get("session") or {}).get("client") for r in group)
            if isinstance(client, dict)
        ),
        "skills": _distinct(
            f"{report.get('skill')}@{report.get('skill_version')}"
            for report in (r.get("report") for r in group)
            if isinstance(report, dict)
        ),
        "native_error_codes": sorted(
            {e["native_error_code"] for e in errors if isinstance(e.get("native_error_code"), int)}
        ),
        "sqlstates": _distinct(e.get("sqlstate") for e in errors),
        "stages": _counts(call.get("stage") for call in calls if call.get("stage")),
        "elapsed_ms": (
            {"p50": round(elapsed[len(elapsed) // 2]), "max": round(elapsed[-1])} if elapsed else None
        ),
        "example": example,
        "labels": ["incident", f"category:{category}", f"priority:{priority}"],
        "suggested_next_step": _NEXT_STEPS.get(category, "Investigate the example below."),
    }
    if include_summaries:
        summaries = _distinct(
            _export_summary(report.get("summary"), terms)
            for report in (r.get("report") for r in group)
            if isinstance(report, dict) and report.get("summary")
        )
        if summaries:
            item["summaries"] = summaries[:3]
    item["title"] = _title(item, example)
    return item


def _example(
    record: Mapping[str, Any],
    terms: Mapping[str, str],
    include_summaries: bool,
) -> dict[str, Any]:
    """The newest record, reduced: no ids, sessions, run ids or pids."""

    error = dict(record.get("error") or {})
    keep = error.get("class") in _BUILTIN_BUG_NAMES or (
        error.get("class") == "KeyError" and bool(_CODE_KEY.fullmatch(str(error.get("message"))))
    )
    for key in ("message", "driver_category"):
        if isinstance(error.get(key), str):
            error[key] = redact_text(error[key], scrub_terms=terms, keep_identifiers=keep)
    if isinstance(error.get("parse"), dict):
        parse = dict(error["parse"])
        parse["description"] = redact_text(parse.get("description") or "", scrub_terms=terms)
        error["parse"] = parse
    call = record.get("call") or {}
    report = record.get("report")
    if isinstance(report, dict):
        report = {
            key: report.get(key)
            for key in (
                "skill",
                "skill_version",
                "skill_version_expected",
                "skill_version_mismatch",
                "blocker_kind",
                "last_tool",
                "attempts",
                "recent_fingerprints",
            )
        }
        if include_summaries:
            report["summary"] = _export_summary((record.get("report") or {}).get("summary"), terms)
    subject = record.get("subject")
    if isinstance(subject, dict):
        subject = {key: value for key, value in subject.items() if key != "id"}
    loop = record.get("loop")
    example = {
        "tool": record.get("tool"),
        "kind": record.get("kind"),
        "error": {key: value for key, value in error.items() if key not in ("frame_line",)} or None,
        "where": call.get("where") or record.get("where") or None,
        "stage": call.get("stage"),
        "elapsed_ms": call.get("elapsed_ms"),
        "budget_s": call.get("budget_s"),
        "argument_keys": call.get("argument_keys") or None,
        "result_status": call.get("result_status"),
        "outcome": call.get("outcome"),
        "reason": redact_text(record["reason"], scrub_terms=terms) if record.get("reason") else None,
        "cause": record.get("cause"),
        "site": record.get("site"),
        "phase": record.get("phase"),
        "log_template": (
            redact_text(record["log_template"], scrub_terms=terms) if record.get("log_template") else None
        ),
        "client": (record.get("session") or {}).get("client"),
        "server_version": (record.get("runtime") or {}).get("server_version"),
        "subject": subject,
        "report": report,
        "loop": loop if isinstance(loop, dict) else None,
        "orphan": record.get("orphan"),
    }
    return {key: value for key, value in example.items() if value is not None}


def _export_summary(summary: object, terms: Mapping[str, str]) -> str:
    # Records keep only tool names in snake_case; the export pass keeps them too.
    text = str(summary or "")
    return redact_agent_text(text, scrub_terms=terms, keep_words=set(_TOOL_WORD.findall(text)))


def _title(item: Mapping[str, Any], example: Mapping[str, Any]) -> str:
    kind = example.get("kind")
    tools = item["tools"]
    tool = tools[0] if len(tools) == 1 else (f"{len(tools)} tools" if tools else "server")
    error = example.get("error") or {}
    error_class = error.get("class") or "error"
    frame = error.get("frame") or example.get("site") or "an unknown frame"
    message = str(error.get("message") or "")[:80]
    stage = example.get("stage")
    stage = stage if stage and stage != "unobserved" else "an unobserved stage"
    longest = round((item.get("elapsed_ms") or {}).get("max", 0) / 1000)
    subject = example.get("subject") or {}
    report = example.get("report") or {}
    loop = example.get("loop") or {}
    if kind == "tool_timeout":
        title = f"{tool} times out in {stage} ({item['count']}x, max {longest}s)"
    elif kind == "tool_cancelled":
        cause = "by the client" if example.get("cause") == "client" else f"({example.get('cause')})"
        title = f"{tool} cancelled {cause} after up to {longest}s in {stage}"
    elif kind == "slow_call":
        # Name the stage the watchdog saw most, not the newest record's.
        seen = {name: count for name, count in item["stages"].items() if name != "unobserved"}
        if seen:
            stage = max(seen, key=lambda name: seen[name])
        title = f"{tool} runs past the slow threshold in {stage} ({item['count']}x, max {longest}s)"
    elif kind == "stuck_call":
        title = f"{tool} stuck in {stage} past its {example.get('budget_s')}s budget"
    elif kind == "event_loop_blocked":
        title = f"Server event loop blocked in {stage}"
    elif kind == "orphaned_call":
        title = f"{tool} never returned ({(example.get('reason') or 'process_exit')})"
    elif kind == "stalled_workflow":
        title = (
            f"{subject.get('type')} left {subject.get('status')}: "
            f"{subject.get('reason')} ({item['subjects']} items)"
        )
    elif kind == "agent_loop":
        repeated = "error" if loop.get("base_fingerprint") else f"status {loop.get('result_status')}"
        title = f"Agents repeat {tool} {loop.get('repeat_count')}x with the same {repeated}"
    elif kind == "agent_report":
        title = (
            f"{report.get('skill')} blocked: {report.get('blocker_kind')} "
            f"at {report.get('last_tool') or 'no tool'}"
        )
    elif kind == "swallowed_exception":
        title = f"Swallowed {error_class} in {frame}: {example.get('log_template') or message}"
    elif kind == "condition":
        title = f"{example.get('site')} reported ({item['count']}x)"
    elif kind == "degraded_result":
        title = f"{tool} returns unavailable: {str(example.get('reason') or '')[:80]}"
    elif kind in ("startup_failure", "server_exit"):
        title = f"Server {example.get('phase')} failed: {error_class} in {frame}"
        if message:
            title += f" - {message}"
    elif kind == "log_cap_reached":
        title = "Incident log reached its daily size cap"
    elif error.get("code") == "unknown_tool":
        title = f"Agents call an unknown tool: {tool}"
    elif error.get("code") == "invalid_arguments":
        title = f"{tool}: invalid arguments"
    elif item["category"] == "product_gap" and (error.get("parse") or {}).get("keyword"):
        title = f"{tool}: valid-looking T-SQL rejected near {error['parse']['keyword']}"
    else:
        native = f" ({error['native_error_code']})" if error.get("native_error_code") else ""
        title = f"{tool}: {error_class}{native} in {frame}"
        if message:
            title += f" - {message}"
    return title[:200]


# Fixed export wording, never record data: a database named "Test" must not
# match "unit test" in a next step and hide the item.
_TEMPLATE_KEYS = frozenset({"title", "suggested_next_step", "labels", "kind", "category", "priority", "fingerprint"})


def _leaks(item: Mapping[str, Any], terms: Mapping[str, str]) -> bool:
    if not terms:
        return False
    texts: list[str] = list(item.get("stages") or {})
    pending: list[Any] = [item]
    while pending:
        value = pending.pop()
        if isinstance(value, Mapping):
            pending.extend(v for k, v in value.items() if k not in _TEMPLATE_KEYS)
        elif isinstance(value, list):
            pending.extend(value)
        elif isinstance(value, str):
            texts.append(value)
    blob = "\n".join(texts)
    return any(
        re.search(rf"(?<![\w-]){re.escape(term)}(?![\w-])", blob, re.IGNORECASE) for term in terms
    )


def _counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


def _distinct(values: Any) -> list[str]:
    return sorted({str(value) for value in values if value})


def _parse_utc(value: Any) -> datetime | None:
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)
