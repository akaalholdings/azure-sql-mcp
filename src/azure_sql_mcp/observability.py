from __future__ import annotations

import hashlib
import re
from typing import Any


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


def _redact_quoted_content(text: str) -> str:
    redacted: list[str] = []
    index = 0
    while index < len(text):
        quote = text[index]
        if quote not in {"'", '"'} or (
            quote == "'" and not _starts_sql_literal(text, index)
        ):
            redacted.append(text[index])
            index += 1
            continue
        redacted.append(f"{quote}[REDACTED]{quote}")
        index += 1
        while index < len(text):
            if text[index] != quote:
                index += 1
            elif index + 1 < len(text) and text[index + 1] == quote:
                index += 2
            else:
                index += 1
                break
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


def sanitize_error_message(message: str) -> str:
    """Strip SQL literals, connection strings, server names, and IPs."""
    sanitized = _redact_quoted_content(message)
    sanitized = _CONN_STRING_PATTERN.sub("[REDACTED];", sanitized)
    sanitized = _SERVER_NAME_PATTERN.sub("[server]", sanitized)
    sanitized = _IP_PATTERN.sub("[ip]", sanitized)
    return sanitized
