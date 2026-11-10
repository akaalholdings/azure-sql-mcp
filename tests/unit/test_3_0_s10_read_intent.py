"""A readable replica as the diagnosis target.

Diagnosis reads (DMVs, Query Store, measured SELECTs) can run on a read
scale-out replica so they add no load to the primary. The policy entry of a
database asks for it; the connection is opened with ApplicationIntent=ReadOnly;
and a write is refused before it is sent, because a replica cannot take it.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from mssql_python.connection_string_parser import _ConnectionStringParser

from azure_sql_mcp.auth import AzureSqlAuthenticator
from azure_sql_mcp.auth import ConnectionArguments
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.connection import AzureSqlExecutor
from azure_sql_mcp.connection import BatchExecutionMode
from azure_sql_mcp.connection import ReadOnlyIntentError
from azure_sql_mcp.connection_pool import ConnectionPool
from azure_sql_mcp.database_policy import DatabasePolicy
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.database_policy import DatabasePolicyValidationError


def _policy(**fields) -> DatabasePolicy:
    return DatabasePolicy.from_mapping(
        "appdb", {"environment": "production", "allow_read": True, **fields}
    )


def test_policy_application_intent_is_optional_and_read_write_by_default() -> None:
    assert _policy().application_intent == "read_write"
    assert _policy(application_intent="read_only").application_intent == "read_only"
    assert DatabasePolicy.denied("appdb").application_intent == "read_write"


@pytest.mark.parametrize("value", ["ReadOnly", "readonly", "", None, True, "primary"])
def test_policy_rejects_an_unknown_application_intent(value) -> None:
    with pytest.raises(DatabasePolicyValidationError):
        _policy(application_intent=value)


def test_policy_set_lists_the_intent_of_each_database() -> None:
    policies = DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {"environment": "production", "allow_read": True},
                "AppDb_Replica": {
                    "environment": "production",
                    "allow_read": True,
                    "application_intent": "read_only",
                },
            },
        }
    )

    assert policies.application_intents() == {"AppDb_Replica": "read_only"}


def test_connection_string_carries_read_intent_only_when_asked(server_config_factory) -> None:
    authenticator = AzureSqlAuthenticator(
        server_config_factory(auth_mode=AuthMode.SQL_PASSWORD, username="u", password="p")
    )
    parser = _ConnectionStringParser(validate_keywords=False)

    default = parser._parse(authenticator.build_connection_arguments("appdb").connection_string)
    replica = parser._parse(
        authenticator.build_connection_arguments(
            "appdb", application_intent="read_only"
        ).connection_string
    )

    assert "applicationintent" not in default
    assert replica["applicationintent"] == "ReadOnly"
    with pytest.raises(ValueError):
        authenticator.build_connection_arguments("appdb", application_intent="ReadWrite;x=y")


def _pool(sample_server_config, **kwargs) -> tuple[ConnectionPool, MagicMock]:
    authenticator = MagicMock()
    authenticator.build_connection_arguments.return_value = ConnectionArguments("Driver=test;")
    pool = ConnectionPool(sample_server_config, authenticator, **kwargs)
    return pool, authenticator


@pytest.mark.asyncio
async def test_pool_opens_a_read_intent_connection_for_a_read_only_policy(
    sample_server_config, monkeypatch
) -> None:
    driver = MagicMock()
    monkeypatch.setattr("azure_sql_mcp.connection_pool._import_mssql_python", lambda: driver)
    pool, authenticator = _pool(sample_server_config, application_intents={"APPDB": "read_only"})

    await pool.acquire("appdb")
    await pool.acquire("reportingdb")

    assert pool.application_intent("appdb") == "read_only"
    assert pool.application_intent("reportingdb") == "read_write"
    calls = authenticator.build_connection_arguments.call_args_list
    assert calls[0].args == ("appdb",) and calls[0].kwargs == {"application_intent": "read_only"}
    # A primary connection is requested exactly as before.
    assert calls[1].args == ("reportingdb",) and calls[1].kwargs == {}


@pytest.mark.asyncio
async def test_pool_never_hands_a_replica_connection_to_a_primary_caller(
    sample_server_config,
) -> None:
    # The two kinds of connection reach different replicas. Reusing one for
    # the other would send a write to a replica or a heavy read to the primary.
    pool, _authenticator = _pool(sample_server_config)
    created: list[tuple[str, str]] = []

    def create(database_name, application_intent="read_write"):
        created.append((database_name, application_intent))
        return MagicMock(name=f"{database_name}-{application_intent}")

    pool._create_connection = create  # type: ignore[method-assign]

    replica = await pool.acquire("appdb", application_intent="read_only")
    await pool.release("appdb", replica)
    primary = await pool.acquire("appdb")
    await pool.release("appdb", primary)
    replica_again = await pool.acquire("appdb", application_intent="read_only")

    assert created == [("appdb", "read_only"), ("appdb", "read_write")]
    assert primary is not replica
    assert replica_again is replica


def _executor_on_replica(sample_server_config) -> tuple[AzureSqlExecutor, SimpleNamespace]:
    pool = SimpleNamespace(
        acquire=AsyncMock(),
        release=AsyncMock(),
        discard=AsyncMock(),
        application_intent=lambda database_name: "read_only",
    )
    return AzureSqlExecutor(sample_server_config, MagicMock(), pool), pool


@pytest.mark.asyncio
async def test_writes_are_refused_on_a_read_intent_database_before_they_are_sent(
    sample_server_config,
) -> None:
    executor, pool = _executor_on_replica(sample_server_config)

    with pytest.raises(ReadOnlyIntentError):
        await executor.execute_non_query("appdb", "UPDATE dbo.t SET a = 1")
    with pytest.raises(ReadOnlyIntentError):
        await executor.execute_transaction_exactly_once("appdb", lambda session: None)
    with pytest.raises(ReadOnlyIntentError):
        await executor.execute_batches(
            "appdb", "CREATE INDEX ix ON dbo.t (a)", execution_mode=BatchExecutionMode.ADMIN
        )

    pool.acquire.assert_not_awaited()


@pytest.mark.asyncio
async def test_reads_run_on_a_read_intent_database(sample_server_config) -> None:
    executor, pool = _executor_on_replica(sample_server_config)
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.description = [("value",)]
    cursor.fetchall.return_value = [(1,)]
    cursor.nextset.return_value = False
    pool.acquire.return_value = connection

    assert await executor.fetch_all("appdb", "SELECT 1 AS value") == [{"value": 1}]
