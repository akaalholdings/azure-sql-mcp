from __future__ import annotations

from azure_sql_mcp.query_identity import database_identity
from azure_sql_mcp.query_identity import legacy_database_fingerprint
from azure_sql_mcp.query_identity import legacy_query_fingerprint
from azure_sql_mcp.query_identity import query_identity
from azure_sql_mcp.query_identity import query_identity_matches
from azure_sql_mcp.query_identity import request_fingerprint
from azure_sql_mcp.query_identity import server_database_identity
from azure_sql_mcp.query_identity import server_database_identity_matches


def test_query_identity_is_versioned_and_literal_sensitive() -> None:
    first = query_identity("SELECT 'MiXeD  Value'")

    assert first.startswith("query-v1:")
    assert query_identity("SELECT 'mixed  Value'") != first
    assert query_identity("SELECT 'MiXeD Value'") != first
    assert query_identity("SELECT 'MiXeD  Value'") == first


def test_server_database_identity_separates_targets_without_casefolding() -> None:
    first = server_database_identity("server-a", "AppDb")

    assert first.startswith("database-v1:")
    assert database_identity("server-a", "AppDb") == first
    assert server_database_identity("server-b", "AppDb") != first
    assert server_database_identity("server-a", "appdb") != first


def test_request_fingerprint_is_stable_for_replay() -> None:
    payload = {"sql": "SELECT 'MiXeD  Value'", "attempts": 2}

    assert request_fingerprint("benchmark", payload) == request_fingerprint(
        "benchmark", {"attempts": 2, "sql": "SELECT 'MiXeD  Value'"}
    )
    assert request_fingerprint("benchmark", payload) != request_fingerprint(
        "benchmark", {"attempts": 3, "sql": "SELECT 'MiXeD  Value'"}
    )


def test_identity_matchers_are_strict_unless_legacy_migration_is_explicit() -> None:
    sql = "SELECT  1"
    assert not query_identity_matches(legacy_query_fingerprint(sql), sql)
    assert query_identity_matches(legacy_query_fingerprint(sql), sql, allow_legacy=True)
    assert not query_identity_matches(
        legacy_query_fingerprint(sql),
        "SELECT 2",
    )
    assert query_identity_matches(query_identity(sql), sql)
    assert not query_identity_matches(query_identity(sql), "SELECT  '1'")

    legacy_database = legacy_database_fingerprint("appdb")
    assert not server_database_identity_matches(
        legacy_database,
        "server.database.windows.net",
        "appdb",
    )
    assert server_database_identity_matches(
        legacy_database,
        "server.database.windows.net",
        "appdb",
        allow_legacy=True,
    )
    assert not server_database_identity_matches(
        legacy_database,
        "other-server.database.windows.net",
        "appdb",
    )
    exact_database = server_database_identity("server-a", "AppDb")
    assert server_database_identity_matches(exact_database, "server-a", "AppDb")
    assert not server_database_identity_matches(
        legacy_database,
        "server.database.windows.net",
        "otherdb",
    )
