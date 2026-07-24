"""Stable identities for SQL text, Azure SQL targets, and requests.

Identity is deliberately based on the bytes supplied by the caller.  SQL
text is not case-folded, whitespace-collapsed, parsed, or re-emitted here:
literal values and the exact executable text therefore remain part of the
identity contract.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any


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
    "query_fingerprint",
    "query_identity",
    "query_identity_matches",
    "request_fingerprint",
    "server_database_fingerprint",
    "server_database_identity",
    "server_database_identity_matches",
]
