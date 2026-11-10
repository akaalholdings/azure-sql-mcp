"""Stable identities for SQL text, Azure SQL targets, and requests.

Identity is deliberately based on the bytes supplied by the caller.  SQL
text is not case-folded, whitespace-collapsed, parsed, or re-emitted here:
literal values and the exact executable text therefore remain part of the
identity contract.

Matching a statement against Query Store is a separate question, answered by
``normalize_statement_text``, ``statement_match_kind`` and ``plan_query_hashes``
at the end of this module. They compare; they never change the text that runs.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import xml.etree.ElementTree as ET
from typing import Any

from .query_text import split_parameter_declarations


QUERY_IDENTITY_VERSION = "query-v1"
DATABASE_IDENTITY_VERSION = "database-v1"
REQUEST_FINGERPRINT_VERSION = "request-v1"


def _digest(parts: tuple[str, ...]) -> str:
    hasher = hashlib.sha256()
    for part in parts:
        encoded = part.encode("utf-8")
        hasher.update(len(encoded).to_bytes(8, "big"))
        hasher.update(encoded)
    return hasher.hexdigest()


def query_identity(sql: str) -> str:
    """Return a versioned identity for the exact submitted SQL text."""

    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("sql must be a non-empty string.")
    return f"{QUERY_IDENTITY_VERSION}:{_digest((QUERY_IDENTITY_VERSION, sql))}"


def legacy_query_fingerprint(sql: str) -> str:
    """Return the pre-v1 normalized query fingerprint for upgrade reads."""

    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("sql must be a non-empty string.")
    normalized = " ".join(sql.split()).casefold()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def query_identity_matches(
    stored: str | None,
    sql: str,
    *,
    allow_legacy: bool = False,
) -> bool:
    """Match an exact v1 identity, with legacy reads explicitly opt-in."""

    if not isinstance(stored, str) or not stored:
        return False
    candidates = (query_identity(sql),)
    if allow_legacy:
        candidates += (legacy_query_fingerprint(sql),)
    return any(hmac.compare_digest(stored, candidate) for candidate in candidates)


def server_database_identity(server: str, database: str) -> str:
    """Return a target identity that cannot collide across SQL servers."""

    if not isinstance(server, str) or not server:
        raise ValueError("server must be a non-empty string.")
    if not isinstance(database, str) or not database:
        raise ValueError("database must be a non-empty string.")
    return f"{DATABASE_IDENTITY_VERSION}:{_digest((DATABASE_IDENTITY_VERSION, server, database))}"


def legacy_database_fingerprint(database: str) -> str:
    """Return the server-agnostic database fingerprint used before v1."""

    if not isinstance(database, str) or not database.strip():
        raise ValueError("database must be a non-empty string.")
    normalized = " ".join(f"database:{database}".split()).casefold()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def server_database_identity_matches(
    stored: str,
    server: str,
    database: str,
    *,
    allow_legacy: bool = False,
) -> bool:
    """Match an exact server-bound v1 target, with legacy reads opt-in."""

    if not isinstance(stored, str) or not stored:
        return False
    candidates = (
        server_database_identity(server, database),
    )
    if allow_legacy:
        candidates += (legacy_database_fingerprint(database),)
    return any(hmac.compare_digest(stored, candidate) for candidate in candidates)


def database_identity(server: str, database: str) -> str:
    """Compatibility spelling for :func:`server_database_identity`."""

    return server_database_identity(server, database)


def request_fingerprint(operation: str, request: Any) -> str:
    """Return a versioned fingerprint for an idempotent operation request.

    JSON mappings are sorted for stable request replay, but string values are
    preserved exactly.  This helper is for request identity, not SQL identity.
    """

    if not isinstance(operation, str) or not operation:
        raise ValueError("operation must be a non-empty string.")
    encoded = json.dumps(
        request,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return f"{REQUEST_FINGERPRINT_VERSION}:{_digest((REQUEST_FINGERPRINT_VERSION, operation, encoded))}"


# --- Matching a statement against Query Store --------------------------------

_SHOWPLAN_NAMESPACE = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"
_QUERY_HASH_PATTERN = re.compile(r"0[xX][0-9a-fA-F]{16}")
_ZERO_QUERY_HASH = "0x" + "0" * 16


def normalize_statement_text(sql: str, *, fold_case: bool = False) -> str:
    """Return statement text in the form used to compare with Query Store text.

    The leading ``(@p type, ...)`` list of a parameterized statement is removed,
    comments are dropped, whitespace runs outside literals and delimited
    identifiers become one space, and a trailing semicolon is removed. String
    literals and delimited identifiers are kept byte for byte. ``fold_case``
    also case-folds the code outside them; leave it off for a database with a
    case-sensitive collation, where two names can differ only in case.
    """

    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("sql must be a non-empty string.")
    _, statement = split_parameter_declarations(sql)
    output: list[str] = []
    pending_space = False
    index = 0
    length = len(statement)
    while index < length:
        char = statement[index]
        if char.isspace():
            pending_space = True
            index += 1
            continue
        if statement.startswith("--", index):
            newline = statement.find("\n", index)
            index = length if newline < 0 else newline
            pending_space = True
            continue
        if statement.startswith("/*", index):
            index = _block_comment_end(statement, index)
            pending_space = True
            continue
        if pending_space and output:
            output.append(" ")
        pending_space = False
        closing = {"'": "'", "[": "]", '"': '"'}.get(char)
        if closing is None:
            output.append(char.casefold() if fold_case else char)
            index += 1
            continue
        end = _delimited_end(statement, index, closing)
        output.append(statement[index:end])
        index = end
    normalized = "".join(output).rstrip("; ")
    if not normalized:
        raise ValueError("sql holds no statement text.")
    return normalized


def statement_match_kind(submitted: str, stored: str) -> str | None:
    """Say how a submitted statement matches Query Store text, or None.

    The result names the weakest step that was needed, strongest first:
    ``exact_text``, ``parameter_prefix`` (equal once the ``(@p type, ...)`` list
    is removed), ``normalized_text`` (layout and comments differ) and
    ``case_insensitive_text``. When both texts declare parameters and the
    declarations differ, the result is None: the same statement with other
    parameter types is a different compiled query.
    """

    if submitted == stored:
        return "exact_text"
    submitted_parameters, submitted_statement = split_parameter_declarations(submitted)
    stored_parameters, stored_statement = split_parameter_declarations(stored)
    if submitted_parameters and stored_parameters:
        if _declaration_key(submitted_parameters) != _declaration_key(stored_parameters):
            return None
    if submitted_statement.strip() == stored_statement.strip():
        return "parameter_prefix"
    if normalize_statement_text(submitted) == normalize_statement_text(stored):
        return "normalized_text"
    if normalize_statement_text(submitted, fold_case=True) == normalize_statement_text(
        stored, fold_case=True
    ):
        return "case_insensitive_text"
    return None


def normalize_query_hash(value: object) -> str:
    """Return a query_hash or query_plan_hash as ``0x`` plus 16 upper-case hex digits.

    Accepts the text form (plan XML, ``CONVERT(varchar(18), hash, 1)``) or the
    8 raw bytes. Raises ValueError for anything else, and for the all-zero
    hash the engine reports when a statement has none.
    """

    if isinstance(value, (bytes, bytearray)) and len(value) == 8:
        text = "0x" + bytes(value).hex()
    elif isinstance(value, str):
        text = value.strip()
    else:
        text = ""
    if not _QUERY_HASH_PATTERN.fullmatch(text):
        raise ValueError("A query hash must be 0x followed by 16 hex digits (binary(8)).")
    normalized = "0x" + text[2:].upper()
    if normalized == _ZERO_QUERY_HASH:
        raise ValueError("The all-zero query hash means the statement has no hash.")
    return normalized


def plan_query_hashes(plan_xml: str) -> list[dict[str, Any]]:
    """Return the validated hashes of each statement in a showplan, in order.

    One entry per statement that carries a usable ``QueryHash`` or
    ``QueryPlanHash``: ``statement_id``, ``statement_type``, ``query_hash`` and
    ``query_plan_hash``. A value that is not a binary(8) hash is returned as
    None, so plan text can never reach a hash lookup unchecked. Raises
    ValueError when the text is not showplan XML.
    """

    try:
        root = ET.fromstring(plan_xml)
    except (ET.ParseError, TypeError) as exc:
        raise ValueError("The plan is not well-formed showplan XML.") from exc
    prefix = f"{{{_SHOWPLAN_NAMESPACE}}}"
    if root.tag != f"{prefix}ShowPlanXML":
        raise ValueError("The plan is not showplan XML.")
    statements: list[dict[str, Any]] = []
    for element in root.iter():
        if not element.tag.startswith(f"{prefix}Stmt"):
            continue
        query_hash = _optional_query_hash(element.get("QueryHash"))
        query_plan_hash = _optional_query_hash(element.get("QueryPlanHash"))
        if query_hash is None and query_plan_hash is None:
            continue
        statement_id = element.get("StatementId") or ""
        statements.append(
            {
                "statement_id": int(statement_id) if statement_id.isdigit() else None,
                "statement_type": element.get("StatementType"),
                "query_hash": query_hash,
                "query_plan_hash": query_plan_hash,
            }
        )
    return statements


def _optional_query_hash(value: str | None) -> str | None:
    try:
        return normalize_query_hash(value)
    except ValueError:
        return None


def _declaration_key(parameters: tuple[tuple[str, str], ...]) -> tuple[tuple[str, str], ...]:
    return tuple((name.casefold(), data_type.casefold()) for name, data_type in parameters)


def _delimited_end(text: str, start: int, closing: str) -> int:
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


def _block_comment_end(text: str, start: int) -> int:
    index = start + 2
    depth = 1
    while index < len(text) and depth:
        if text.startswith("/*", index):
            depth += 1
            index += 2
        elif text.startswith("*/", index):
            depth -= 1
            index += 2
        else:
            index += 1
    return index


# Names used by integration code that historically called these values
# fingerprints rather than identities.
query_fingerprint = query_identity
server_database_fingerprint = server_database_identity
database_fingerprint = server_database_identity
fingerprint_text = query_identity


__all__ = [
    "DATABASE_IDENTITY_VERSION",
    "QUERY_IDENTITY_VERSION",
    "REQUEST_FINGERPRINT_VERSION",
    "database_identity",
    "database_fingerprint",
    "fingerprint_text",
    "legacy_database_fingerprint",
    "legacy_query_fingerprint",
    "normalize_query_hash",
    "normalize_statement_text",
    "plan_query_hashes",
    "query_fingerprint",
    "query_identity",
    "query_identity_matches",
    "request_fingerprint",
    "server_database_fingerprint",
    "server_database_identity",
    "server_database_identity_matches",
    "statement_match_kind",
]
