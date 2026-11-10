from __future__ import annotations

import hashlib
import re
from collections.abc import Collection
from typing import Any, Callable

from .query_text import split_parameter_declarations


# Patterns to strip from user-facing error messages
_CONN_STRING_PATTERN = re.compile(
    r"(Server|Data Source|Initial Catalog|Database|User ID|UID|Password|PWD|Authentication)"
    r"\s*=\s*[^;]+;?",
    re.IGNORECASE,
)
_SERVER_NAME_PATTERN = re.compile(
    r"\b[a-zA-Z0-9_-]+\.database\.windows\.net\b",
)
_IP_PATTERN = re.compile(
    r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
)
_SP_EXECUTESQL_UNICODE_CONTROL_PATTERN = re.compile(
    r"(?:\[42000\]\s+)?"
    r"(?:\[Microsoft\])?"
    r"(?:\[ODBC Driver [0-9]+(?:\.[0-9]+)? for SQL Server\])?"
    r"(?:\[SQL Server\])?"
    r"Procedure expects parameter '@(?:statement|params)' of type "
    r"'ntext/nchar/nvarchar'\."
    r"(?: \(214\))?"
    r"(?: \([A-Za-z0-9_]+\))?"
    r"$",
    re.IGNORECASE,
)
_MAX_FAILURE_DIAGNOSTIC_TEXT = 240
_MAX_ENGINE_MESSAGE_TEXT = 500
# What marks a text as a driver diagnostic record, and the tags to strip from it.
_DRIVER_TAG_PATTERN = re.compile(
    r"\[(?:Microsoft|SQL Server|ODBC Driver \d+(?:\.\d+)? for SQL Server"
    r"|ODBC SQL Server Driver|[0-9A-Z]{5})\]\s*"
)
_ENGINE_RECORD_MARKER = re.compile(r"\[(?:Microsoft|SQL Server)\]")
# The code that ends a record: "(1222)" or "(1222) (SQLExecDirectW)".
_TRAILING_NATIVE_CODE_PATTERN = re.compile(r"\s*\(\d{1,6}\)(?:\s*\(SQL\w+\))?\s*$")
# A duplicate-key error names the key of the row that collided.
_DUPLICATE_KEY_VALUE_PATTERN = re.compile(
    r"(duplicate key value is)\b.*", re.IGNORECASE | re.DOTALL
)
# 3621 "The statement has been terminated" only accompanies another error.
_COMPANION_NATIVE_CODES = frozenset({3621})

# The state stores raise "Unknown <kind>: <id>." from a KeyError subclass named
# ...NotFoundError. str() of a KeyError is the quoted repr of that text.
_NOT_FOUND_KIND_PATTERN = re.compile(r"Unknown ([A-Za-z][A-Za-z0-9 ]{0,60}?): ")
_NOT_FOUND_TEXT_PATTERN = re.compile(r"'?Unknown ([A-Za-z][A-Za-z0-9 ]{0,60}?): [^']*\.'?")
_ID_KIND_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,39}")

# Engine compile errors whose quoted token is an identifier or a T-SQL keyword
# from the statement text, never a row value. "Incorrect syntax near '<token>'"
# is left out: the token can be a piece of a literal.
_IDENTIFIER_ERROR_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"Invalid column name (?P<q>'[^'\r\n]{1,260}')",
        r"Invalid object name (?P<q>'[^'\r\n]{1,260}')",
        r"Ambiguous column name (?P<q>'[^'\r\n]{1,260}')",
        r"The multi-part identifier (?P<q>\"[^\"\r\n]{1,260}\") could not be bound",
        r"Could not find stored procedure (?P<q>'[^'\r\n]{1,260}')",
        r"(?P<q>'[^'\r\n]{1,260}') is not a recognized built-in function name",
        r"Column (?P<q>'[^'\r\n]{1,260}') is invalid in the select list",
        r"The column (?P<q>'[^'\r\n]{1,260}') was specified multiple times",
        r"Incorrect syntax near the keyword (?P<q>'[A-Za-z_ ]{1,40}')",
    )
)

SQL_ORIGINS = ("server_built", "caller_supplied")
_SQL_ORIGIN_ATTRIBUTE = "azure_sql_mcp_sql_origin"

# Where a driver reports the engine error number: "(Error 40613)", the "(1205)"
# or "(1205) (SQLExecDirectW)" that ends one diagnostic record, and
# "Msg 40501, Level 20". A number anywhere else is message text; engine text
# ends a row value with ".", so "(40613)." is a value, not a code.
_NATIVE_CODE_PATTERN = re.compile(
    r"\(Error\s+(\d{1,6})\)"
    r"|\((\d{1,6})\)(?:\s*\(SQL\w+\))?['\"\s)]*$"
    r"|\bMsg\s+(\d{1,6}),\s*Level\b",
    re.IGNORECASE,
)


def extract_sql_error_info(exc: Exception) -> dict[str, Any]:
    """Extract SQLSTATE and native error code from pyodbc/mssql exceptions."""
    info: dict[str, Any] = {}
    current: BaseException | None = exc
    while current is not None:
        args = getattr(current, "args", ())
        for arg in args:
            if isinstance(arg, str):
                # Look for [SQLSTATE] pattern
                sqlstate_match = re.search(r"\[(\w{5})\]", arg)
                if sqlstate_match and "sqlstate" not in info:
                    info["sqlstate"] = sqlstate_match.group(1)
                # Look for native error code like (Error 40613)
                native_match = re.search(r"\(Error\s+(\d+)\)", arg, re.IGNORECASE)
                if native_match and "native_error_code" not in info:
                    info["native_error_code"] = int(native_match.group(1))
            elif isinstance(arg, tuple) and len(arg) >= 2:
                # pyodbc often passes (sqlstate, message) tuples
                if isinstance(arg[0], str) and len(arg[0]) == 5 and "sqlstate" not in info:
                    info["sqlstate"] = arg[0]
        current = current.__cause__ or current.__context__
    return info


def _exception_chain(exc: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        chain.append(current)
        current = current.__cause__ or current.__context__
    return chain


def _id_kind(text: str) -> str | None:
    """Turn 'TuningSessionV1' or 'index review run' into a snake_case kind."""

    words = re.sub(r"V\d+$", "", text.strip())
    words = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", words)
    kind = "_".join(words.lower().split())
    return kind if _ID_KIND_PATTERN.fullmatch(kind) else None


def _not_found_message(id_kind: str) -> str:
    return (
        f"No {id_kind.replace('_', ' ')} with this id exists in this MCP state store. "
        "Check the id. Each profile and state directory has its own store, and an "
        "in-memory store is empty after a restart. Do not retry the call with the "
        "same id; this is not a database failure."
    )


def not_found_details(exc: BaseException) -> dict[str, str] | None:
    """Describe an unknown, expired or other-process id without its value.

    Recognises a ``KeyError`` subclass whose name ends in ``NotFoundError``
    anywhere in the exception chain. The id kind comes from the exception's
    ``id_kind`` attribute when it has one, otherwise from its
    ``Unknown <kind>: <id>.`` text or its class name. Returns
    ``diagnostic_code``, ``id_kind`` and a value-free ``message``.
    """

    for current in _exception_chain(exc):
        name = type(current).__name__
        if not (isinstance(current, KeyError) and name.endswith("NotFoundError")):
            continue
        declared = getattr(current, "id_kind", None)
        first = current.args[0] if current.args else ""
        match = _NOT_FOUND_KIND_PATTERN.match(first) if isinstance(first, str) else None
        kind = (
            (_id_kind(declared) if isinstance(declared, str) else None)
            or (_id_kind(match.group(1)) if match else None)
            or _id_kind(name.removesuffix("NotFoundError"))
        )
        if kind in (None, "contract"):
            kind = "record"
        return {
            "diagnostic_code": "not_found",
            "id_kind": kind,
            "message": _not_found_message(kind),
        }
    return None


def tag_sql_origin(exc: BaseException, origin: str) -> None:
    """Record on an exception whether the failing SQL was built by the server.

    The executor tags the driver error of a statement it built itself as
    ``server_built`` and the error of SQL a caller supplied as
    ``caller_supplied``. A server-built statement that fails is a server
    defect, never the caller's error.
    """

    if origin not in SQL_ORIGINS:
        raise ValueError(f"origin must be one of: {', '.join(SQL_ORIGINS)}.")
    setattr(exc, _SQL_ORIGIN_ATTRIBUTE, origin)


def sql_origin(exc: BaseException) -> str | None:
    """Return the origin tag nearest the root cause, or None when untagged."""

    for current in reversed(_exception_chain(exc)):
        origin = getattr(current, _SQL_ORIGIN_ATTRIBUTE, None)
        if origin in SQL_ORIGINS:
            return origin
    return None


def native_error_codes(exc: BaseException) -> tuple[int, ...]:
    """Engine error numbers the driver reported, from reported positions only.

    Reads integer arguments, ``(Error N)``, the ``(N)`` that ends an ODBC
    diagnostic record, and ``Msg N, Level``. A number inside the message text
    (a row count, a value, a name) is never an error number.
    """

    codes: list[int] = []
    for current in _exception_chain(exc):
        ddbc_error = getattr(current, "ddbc_error", None)
        arguments = (ddbc_error,) if isinstance(ddbc_error, str) else getattr(current, "args", ())
        for argument in arguments:
            for item in argument if isinstance(argument, tuple) else (argument,):
                if isinstance(item, bool):
                    continue
                if isinstance(item, int):
                    codes.append(item)
                elif isinstance(item, str):
                    # One ODBC message can hold several records, joined by "; ".
                    for record in item.split("; "):
                        for match in _NATIVE_CODE_PATTERN.finditer(record):
                            codes.append(int(next(group for group in match.groups() if group)))
    return tuple(dict.fromkeys(codes))


def is_transient_error(exc: BaseException, transient_codes: Collection[int]) -> bool:
    """True when the driver reported one of ``transient_codes`` for ``exc``."""

    return any(code in transient_codes for code in native_error_codes(exc))


def engine_error_message(exc: BaseException) -> str | None:
    """The engine's message for a failed statement, redacted and capped.

    Reads the driver record nearest the root cause (``ddbc_error``, a text
    with a driver tag, or a ``(sqlstate, message)`` pair). Driver tags and the
    trailing error number are removed. ``sanitize_error_message`` redacts
    quoted values, server names and addresses and keeps the identifier a
    compile error names; a duplicate key value is removed as well. Returns
    None when the chain holds no engine text.
    """

    for current in reversed(_exception_chain(exc)):
        texts: list[str] = []
        ddbc_error = getattr(current, "ddbc_error", None)
        if isinstance(ddbc_error, str):
            texts.append(ddbc_error)
        for arg in getattr(current, "args", ()):
            marker = _ENGINE_RECORD_MARKER.search(arg) if isinstance(arg, str) else None
            if marker is not None:
                # Drops a wrapper prefix such as "Driver Error: ...; DDBC Error: ".
                texts.append(arg[marker.start() :])
            elif (
                isinstance(arg, tuple)
                and len(arg) >= 2
                and isinstance(arg[0], str)
                and len(arg[0]) == 5
                and isinstance(arg[1], str)
            ):
                texts.append(arg[1])
        for text in texts:
            message = _TRAILING_NATIVE_CODE_PATTERN.sub(
                "", _DRIVER_TAG_PATTERN.sub("", " ".join(text.split()))
            )
            message = _DUPLICATE_KEY_VALUE_PATTERN.sub(
                r"\1 [redacted]", sanitize_error_message(message, keep_identifiers=True)
            ).strip()
            if message:
                return message[:_MAX_ENGINE_MESSAGE_TEXT]
    return None


def extract_failure_diagnostic(exc: Exception, *, engine_text: bool = False) -> dict[str, Any]:
    """Return only an allowlisted, value-free failure diagnostic.

    Generic exception text is intentionally not persisted.  An unknown,
    expired or other-process id is ``not_found`` with its ``id_kind``: it is a
    caller error, not a SQL failure.  The one useful execution defect
    currently known to the workflow is the SQL Server 214 diagnostic emitted
    when an ``sp_executesql`` control argument is bound as a non-Unicode
    value.  Its engine text contains no query or parameter values, so it can
    be returned as a stable, capped receipt.

    ``engine_text=True`` is for a statement the server built itself, such as
    test index DDL. The diagnostic then also carries ``native_error_code``
    when the driver reported one and ``engine_message``
    (``engine_error_message``), so the caller can tell why the engine refused
    the statement. Leave it off for caller SQL.
    """

    not_found = not_found_details(exc)
    if not_found is not None:
        return {"diagnostic_code": "not_found", "id_kind": not_found["id_kind"]}
    info = extract_sql_error_info(exc)
    current: BaseException | None = exc
    while current is not None:
        candidates: list[tuple[str, bool]] = []
        ddbc_error = getattr(current, "ddbc_error", None)
        if isinstance(ddbc_error, str):
            candidates.append((ddbc_error, True))
        for arg in getattr(current, "args", ()):
            if isinstance(arg, str):
                candidates.append((arg, False))
            elif (
                isinstance(arg, tuple)
                and len(arg) >= 2
                and isinstance(arg[1], str)
            ):
                candidates.append((arg[1], False))
        for diagnostic_text, is_driver_ddbc_error in candidates:
            normalized_arg = " ".join(diagnostic_text.split())
            if (
                is_driver_ddbc_error or info.get("sqlstate") == "42000"
            ) and _SP_EXECUTESQL_UNICODE_CONTROL_PATTERN.fullmatch(normalized_arg):
                text = (
                    "Procedure expects an sp_executesql control parameter "
                    "(@statement or @params) of type ntext/nchar/nvarchar."
                )[:_MAX_FAILURE_DIAGNOSTIC_TEXT]
                return {
                    "diagnostic_code": "sp_executesql_unicode_control_argument",
                    "sqlstate": "42000",
                    "native_error_code": 214,
                    "text": text,
                }
        current = current.__cause__ or current.__context__

    diagnostic: dict[str, Any] = {"diagnostic_code": "sql_execution_failed"}
    if isinstance(info.get("sqlstate"), str):
        diagnostic["sqlstate"] = info["sqlstate"]
    if isinstance(info.get("native_error_code"), int):
        diagnostic["native_error_code"] = info["native_error_code"]
    if engine_text:
        message = engine_error_message(exc)
        if message is not None:
            diagnostic["engine_message"] = message
            code = next(
                (
                    code
                    for code in native_error_codes(exc)
                    if code not in _COMPANION_NATIVE_CODES
                ),
                None,
            )
            if code is not None:
                diagnostic.setdefault("native_error_code", code)
    return diagnostic


def compute_query_hash(sql: str) -> str:
    """Compute a stable hash of normalized SQL for deduplication/logging."""
    normalized = re.sub(r"\s+", " ", sql.strip().lower())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def redact_sql_literals(sql: str) -> str:
    """Redact quoted values and comment bodies from T-SQL previews."""
    redacted: list[str] = []
    index = 0
    while index < len(sql):
        if sql.startswith("--", index):
            redacted.append("--[REDACTED]")
            index += 2
            while index < len(sql) and sql[index] not in "\r\n":
                index += 1
            continue
        if sql.startswith("/*", index):
            redacted.append("/*[REDACTED]*/")
            index += 2
            depth = 1
            while index < len(sql) and depth:
                if sql.startswith("/*", index):
                    depth += 1
                    index += 2
                elif sql.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            continue
        quote = sql[index]
        if quote != "'":
            redacted.append(quote)
            index += 1
            continue
        redacted.append(f"{quote}[REDACTED]{quote}")
        index += 1
        while index < len(sql):
            if sql[index] != quote:
                index += 1
            elif index + 1 < len(sql) and sql[index + 1] == quote:
                index += 2
            else:
                index += 1
                break
    return "".join(redacted)


def _redact_quoted_content(
    text: str,
    keep_quoted: Callable[[str], bool] | None = None,
    keep_spans: Collection[tuple[int, int]] = (),
) -> str:
    redacted: list[str] = []
    kept_ends = dict(keep_spans)
    # Quotes that wrap a whole driver message (the repr of an ODBC error
    # tuple); they are punctuation, not a value.
    wrapper_quotes: set[int] = set()
    index = 0
    while index < len(text):
        if index in kept_ends:
            redacted.append(text[index : kept_ends[index]])
            index = kept_ends[index]
            continue
        quote = text[index]
        if (
            quote not in {"'", '"'}
            or index in wrapper_quotes
            or (quote == "'" and not _starts_sql_literal(text, index))
        ):
            redacted.append(text[index])
            index += 1
            continue
        start = index
        closed = False
        index += 1
        while index < len(text):
            if text[index] != quote:
                index += 1
            elif index + 1 < len(text) and text[index + 1] == quote:
                index += 2
            else:
                index += 1
                closed = True
                break
        if closed and any(start < kept < index for kept in kept_ends):
            # The span holds an identifier to keep: redact inside it, token by token.
            wrapper_quotes.add(index - 1)
            redacted.append(quote)
            index = start + 1
            continue
        if (
            closed
            and keep_quoted is not None
            and keep_quoted(text[start + 1 : index - 1])
        ):
            redacted.append(text[start:index])
        else:
            redacted.append(f"{quote}[REDACTED]{quote}")
    return "".join(redacted)


def _starts_sql_literal(sql: str, quote_index: int) -> bool:
    if quote_index == 0:
        return True
    previous = sql[quote_index - 1]
    if previous in {"N", "n"}:
        return quote_index == 1 or not _is_sql_identifier_character(
            sql[quote_index - 2]
        )
    return not _is_sql_identifier_character(previous)


def _is_sql_identifier_character(character: str) -> bool:
    return character.isalnum() or character in "_@$#"


def sanitize_error_message(
    message: str,
    *,
    keep_quoted: Callable[[str], bool] | None = None,
    keep_identifiers: bool = False,
) -> str:
    """Strip SQL literals, connection strings, server names, and IPs.

    ``keep_quoted`` may keep a quoted token the caller knows comes from code,
    such as a T-SQL function name; every other quoted token is redacted.
    ``keep_identifiers`` keeps the identifier an engine compile error names
    (``Invalid column name 'X'``, ``Invalid object name 'X'`` and the like),
    so the caller can fix it; row values stay redacted. A state-store
    "Unknown <kind>: <id>." text becomes a value-free not-found message.
    """
    not_found = _NOT_FOUND_TEXT_PATTERN.fullmatch(message.strip())
    id_kind = _id_kind(not_found.group(1)) if not_found else None
    if id_kind is not None:
        return _not_found_message(id_kind)
    keep_spans = (
        [
            match.span("q")
            for pattern in _IDENTIFIER_ERROR_PATTERNS
            for match in pattern.finditer(message)
        ]
        if keep_identifiers
        else []
    )
    sanitized = _redact_quoted_content(message, keep_quoted, keep_spans)
    sanitized = _CONN_STRING_PATTERN.sub("[REDACTED];", sanitized)
    sanitized = _SERVER_NAME_PATTERN.sub("[server]", sanitized)
    sanitized = _IP_PATTERN.sub("[ip]", sanitized)
    return sanitized


# --- Data-detail switch -------------------------------------------------------
#
# One server setting decides how much customer data a tool response may carry.
# It is applied in one place, to every response, by key name:
#   full           statement text, predicates and parameter values as read
#   mask_literals  literals in statement text and predicates masked; parameter
#                  and histogram values masked; raw plan XML withheld
#   metadata_only  statement text and predicates withheld as well
# Statement text that matches a credential pattern is withheld at every level.

DATA_DETAIL_LEVELS = ("full", "mask_literals", "metadata_only")
CREDENTIAL_WITHHELD = "[withheld: credential pattern]"
MASKED_VALUE = "[masked]"

_STATEMENT_TEXT_KEYS = frozenset(
    {
        "analysis_sql", "baseline_sql", "bound_sql", "candidate_sql", "command_text",
        "create_definition", "definition", "driver_sql", "exact_sql_text", "execution_sql",
        "module_definition", "module_text", "normalized_sql", "normalized_text",
        "original_sql", "query_sql_text", "query_store_text", "query_text",
        "query_text_preview", "raw_sql", "retained_query_text", "rewritten_sql",
        "source_definition", "source_query", "sql", "sql_preview", "sql_text", "statement",
        "statement_text", "submitted_sql", "target_definition", "winning_sql",
    }
)
_PLAN_XML_KEYS = frozenset({"plan_xml", "query_plan", "query_plan_xml", "raw_plan_xml", "showplan_xml"})
_PREDICATE_KEYS = frozenset(
    {
        "computed_definition", "default_definition", "expected_predicate",
        "filter_definition", "predicate", "predicates", "seek", "seek_predicates",
    }
)
# Keys that always hold a data value.
_VALUE_KEYS = frozenset(
    {
        "compiled_value", "compiled_values", "driver_values", "parameter_values",
        "range_high_key", "raw_value", "runtime_value", "sp_executesql_values",
    }
)
# Keys that hold a data value only below one of the containers that follow:
# "value" is also a configuration value, and "runtime" a plan section.
_VALUE_KEYS_IN_PARAMETERS = frozenset({"compiled", "runtime", "value", "values"})
_PARAMETER_CONTAINER_KEYS = frozenset(
    {
        "compiled_parameters", "distinct_parameter_sets", "heavy_hitters", "histogram",
        "parameter_case", "parameter_cases", "parameters", "runtime_parameters",
        "typed_bucket", "value_picks",
    }
)
_CREDENTIAL_PATTERN = re.compile(
    r"\b(?:CREATE|ALTER)\s+(?:LOGIN|USER|CREDENTIAL|DATABASE\s+SCOPED\s+CREDENTIAL|"
    r"MASTER\s+KEY|APPLICATION\s+ROLE)\b"
    r"|\b(?:PASSWORD|PWD|SECRET)\s*="
    r"|\bPASSWORD\s+N?'"
    r"|\b(?:OPENROWSET|OPENDATASOURCE|OPENQUERY)\b"
    r"|\bsp_addlinkedsrvlogin\b",
    re.IGNORECASE,
)
_NUMBER_PATTERN = re.compile(r"0[xX][0-9A-Fa-f]*|\d+(?:\.\d+)?(?:[eE][+-]?\d+)?")


def normalize_data_detail(value: str | None) -> str:
    """Validate a data-detail level; None or blank means ``full``."""

    level = (value or "full").strip().lower()
    if level not in DATA_DETAIL_LEVELS:
        raise ValueError(f"data detail must be one of: {', '.join(DATA_DETAIL_LEVELS)}.")
    return level


def contains_credential_pattern(text: str) -> bool:
    """True for statement text that can carry a secret.

    Matches statements that create or alter a login, user or credential, a
    ``PASSWORD``, ``PWD`` or ``SECRET`` assignment (also inside a connection
    string literal), and ``OPENROWSET``-style remote access.
    """

    return _CREDENTIAL_PATTERN.search(text) is not None


def mask_sql_literals(sql: str) -> str:
    """Replace every literal and comment body in T-SQL text with ``?``.

    String, numeric and binary literals and comments are masked. Identifiers,
    also delimited ones with digits or spaces, are kept, and so is a leading
    Query Store ``(@p type, ...)`` list: declared types hold no data.
    """

    declared, statement = split_parameter_declarations(sql)
    prefix = sql[: len(sql) - len(statement)] if declared else ""
    body = statement if declared else sql
    output: list[str] = [prefix]
    index = 0
    length = len(body)
    while index < length:
        char = body[index]
        previous = body[index - 1] if index else ""
        if char in "Nn" and body.startswith("'", index + 1) and not _is_sql_identifier_character(previous):
            index += 1
            continue
        if char == "'":
            index = _quoted_end(body, index, "'")
            output.append("'?'")
        elif body.startswith("--", index):
            newline = body.find("\n", index)
            index = length if newline < 0 else newline
            output.append("--?")
        elif body.startswith("/*", index):
            depth = 1
            index += 2
            while index < length and depth:
                if body.startswith("/*", index):
                    depth += 1
                    index += 2
                elif body.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            output.append("/*?*/")
        elif char in '["':
            end = _quoted_end(body, index, "]" if char == "[" else '"')
            output.append(body[index:end])
            index = end
        elif char.isdigit() and not _is_sql_identifier_character(previous):
            match = _NUMBER_PATTERN.match(body, index)
            index = match.end() if match else index + 1
            output.append("?")
        else:
            output.append(char)
            index += 1
    return "".join(output)


def _quoted_end(text: str, start: int, closing: str) -> int:
    """Index just past a literal or delimited identifier; a doubled closer is an escape."""

    index = start + 1
    while index < len(text):
        if text[index] == closing:
            if text.startswith(closing, index + 1):
                index += 2
                continue
            return index + 1
        index += 1
    return len(text)


def apply_data_detail(payload: Any, level: str = "full") -> Any:
    """Apply the server's data-detail level to one tool response.

    Returns a new structure; the input is not changed. A dict response at a
    level other than ``full`` gains ``data_detail`` so the reader knows why
    text or values are masked. Credential text is withheld at every level.
    """

    level = normalize_data_detail(level)
    result = _apply_detail(payload, level, None, False)
    if level != "full" and isinstance(result, dict):
        result["data_detail"] = level
    return result


def _apply_detail(value: Any, level: str, key: str | None, in_parameters: bool) -> Any:
    holds_values = key in _VALUE_KEYS or (in_parameters and key in _VALUE_KEYS_IN_PARAMETERS)
    if holds_values and level != "full":
        return _mask_values(value)
    if isinstance(value, dict):
        return {
            child_key: _apply_detail(
                child,
                level,
                child_key if isinstance(child_key, str) else None,
                in_parameters or child_key in _PARAMETER_CONTAINER_KEYS,
            )
            for child_key, child in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_apply_detail(item, level, key, in_parameters) for item in value]
    if not isinstance(value, str):
        return value
    withheld = f"[withheld: data detail is {level}]"
    if key in _PLAN_XML_KEYS:
        if contains_credential_pattern(value):
            return CREDENTIAL_WITHHELD
        return value if level == "full" else withheld
    if key in _STATEMENT_TEXT_KEYS:
        if contains_credential_pattern(value):
            return CREDENTIAL_WITHHELD
        if level == "mask_literals":
            return mask_sql_literals(value)
        return value if level == "full" else withheld
    if key in _PREDICATE_KEYS and level != "full":
        return mask_sql_literals(value) if level == "mask_literals" else withheld
    return value


def _mask_values(value: Any) -> Any:
    """Mask every leaf; keep names, shape and NULL."""

    if isinstance(value, dict):
        return {key: _mask_values(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_mask_values(item) for item in value]
    return None if value is None else MASKED_VALUE
