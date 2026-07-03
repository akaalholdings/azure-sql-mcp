from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from azure_sql_mcp.auth import ConnectionArguments
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.connection_pool import CIRCUIT_BREAKER_COOLDOWN
from azure_sql_mcp.connection_pool import CIRCUIT_BREAKER_THRESHOLD
from azure_sql_mcp.connection_pool import ConnectionPool
from azure_sql_mcp.connection_pool import TOKEN_REFRESH_SECONDS
from azure_sql_mcp.connection_pool import VALIDATION_TIMEOUT_SECONDS


def _make_connection(name: str) -> MagicMock:
    connection = MagicMock(name=name)
    cursor = MagicMock(name=f"{name}_cursor")
    cursor.fetchone.return_value = (1,)
    connection.cursor.return_value = cursor
    return connection


@pytest.mark.asyncio
async def test_acquire_creates_new_connection(sample_server_config) -> None:
    authenticator = MagicMock()
    authenticator.build_connection_arguments.return_value = ConnectionArguments("Driver=test;")
    connection = _make_connection("connection_one")

    with patch("azure_sql_mcp.connection_pool._import_mssql_python") as import_mssql_python:
        import_mssql_python.return_value.connect.return_value = connection
        pool = ConnectionPool(sample_server_config, authenticator)

        acquired = await pool.acquire("appdb")

    assert acquired is connection
    assert import_mssql_python.return_value.connect.call_count == 1
    assert import_mssql_python.return_value.connect.call_args.kwargs["timeout"] == (
        sample_server_config.query_timeout_seconds
    )
    assert pool._pool_sizes["appdb"] == 1


@pytest.mark.asyncio
async def test_release_returns_connection_for_reuse(sample_server_config) -> None:
    authenticator = MagicMock()
    authenticator.build_connection_arguments.return_value = ConnectionArguments("Driver=test;")
    connection = _make_connection("reusable_connection")

    with patch("azure_sql_mcp.connection_pool._import_mssql_python") as import_mssql_python:
        import_mssql_python.return_value.connect.return_value = connection
        pool = ConnectionPool(sample_server_config, authenticator)

        first = await pool.acquire("appdb")
        await pool.release("appdb", first)
        second = await pool.acquire("appdb")

    assert second is first
    assert import_mssql_python.return_value.connect.call_count == 1


@pytest.mark.asyncio
async def test_stale_connection_is_evicted_and_replaced(sample_server_config) -> None:
    authenticator = MagicMock()
    authenticator.build_connection_arguments.return_value = ConnectionArguments("Driver=test;")
    stale_connection = _make_connection("stale_connection")
    fresh_connection = _make_connection("fresh_connection")

    with patch("azure_sql_mcp.connection_pool._import_mssql_python") as import_mssql_python:
        import_mssql_python.return_value.connect.side_effect = [stale_connection, fresh_connection]
        pool = ConnectionPool(sample_server_config, authenticator)

        first = await pool.acquire("appdb")
        await pool.release("appdb", first)
        with patch.object(pool, "_validate_connection", side_effect=[False]):
            second = await pool.acquire("appdb")

    assert second is fresh_connection
    stale_connection.close.assert_called_once()
    assert pool._pool_sizes["appdb"] == 1


def test_validate_connection_uses_short_validation_timeout(sample_server_config) -> None:
    authenticator = MagicMock()
    pool = ConnectionPool(sample_server_config, authenticator)
    connection = _make_connection("validated_connection")
    connection.timeout = 30

    def execute_side_effect(_: str) -> None:
        assert connection.timeout == VALIDATION_TIMEOUT_SECONDS

    connection.cursor.return_value.execute.side_effect = execute_side_effect

    assert pool._validate_connection(connection) is True
    assert connection.timeout == 30


@pytest.mark.asyncio
async def test_token_refresh_discards_idle_entra_connection(
    server_config_factory,
) -> None:
    config = server_config_factory(auth_mode=AuthMode.ENTRA_DEFAULT)
    authenticator = MagicMock()
    authenticator.build_connection_arguments.return_value = ConnectionArguments("Driver=test;")
    old_connection = _make_connection("old_connection")
    refreshed_connection = _make_connection("refreshed_connection")

    with patch("azure_sql_mcp.connection_pool._import_mssql_python") as import_mssql_python:
        import_mssql_python.return_value.connect.side_effect = [old_connection, refreshed_connection]
        pool = ConnectionPool(config, authenticator)

        first = await pool.acquire("appdb")
        await pool.release("appdb", first)
        pool._token_acquired_at = time.monotonic() - TOKEN_REFRESH_SECONDS - 1

        second = await pool.acquire("appdb")

    assert second is refreshed_connection
    old_connection.close.assert_called_once()
    assert authenticator.build_connection_arguments.call_count == 2


@pytest.mark.asyncio
async def test_pool_at_capacity_waits_for_released_connection(
    server_config_factory,
) -> None:
    config = server_config_factory(pool_size=1, auth_mode=AuthMode.SQL_PASSWORD)
    authenticator = MagicMock()
    authenticator.build_connection_arguments.return_value = ConnectionArguments("Driver=test;")
    connection = _make_connection("single_connection")

    with patch("azure_sql_mcp.connection_pool._import_mssql_python") as import_mssql_python:
        import_mssql_python.return_value.connect.return_value = connection
        pool = ConnectionPool(config, authenticator)

        first = await pool.acquire("appdb")
        blocked_acquire = asyncio.create_task(pool.acquire("appdb"))
        await asyncio.sleep(0)
        assert not blocked_acquire.done()

        await pool.release("appdb", first)
        second = await asyncio.wait_for(blocked_acquire, timeout=0.5)

    assert second is connection
    assert import_mssql_python.return_value.connect.call_count == 1


@pytest.mark.asyncio
async def test_close_all_drains_idle_connections(sample_server_config) -> None:
    authenticator = MagicMock()
    authenticator.build_connection_arguments.return_value = ConnectionArguments("Driver=test;")
    connection_one = _make_connection("connection_one")
    connection_two = _make_connection("connection_two")

    with patch("azure_sql_mcp.connection_pool._import_mssql_python") as import_mssql_python:
        import_mssql_python.return_value.connect.side_effect = [connection_one, connection_two]
        pool = ConnectionPool(sample_server_config, authenticator)

        first = await pool.acquire("appdb")
        second = await pool.acquire("reportingdb")
        await pool.release("appdb", first)
        await pool.release("reportingdb", second)
        await pool.close_all()

    connection_one.close.assert_called_once()
    connection_two.close.assert_called_once()
    assert pool._pool_sizes["appdb"] == 0
    assert pool._pool_sizes["reportingdb"] == 0


@pytest.mark.asyncio
async def test_discard_releases_pool_slot(sample_server_config) -> None:
    authenticator = MagicMock()
    authenticator.build_connection_arguments.return_value = ConnectionArguments("Driver=test;")
    connection = _make_connection("broken_connection")
    pool = ConnectionPool(sample_server_config, authenticator)
    pool._pool_sizes["appdb"] = 1

    await pool.discard("appdb", connection)

    connection.close.assert_called_once()
    assert pool._pool_sizes["appdb"] == 0


# --- Phase 19: Circuit Breaker Tests ---


def test_circuit_breaker_opens_after_threshold(sample_server_config) -> None:
    authenticator = MagicMock()
    pool = ConnectionPool(sample_server_config, authenticator)

    for _ in range(CIRCUIT_BREAKER_THRESHOLD):
        pool._record_failure("appdb")

    assert pool._circuit_breakers["appdb"].is_open is True
    with pytest.raises(ConnectionError, match="Circuit breaker open"):
        pool._check_circuit_breaker("appdb")


def test_circuit_breaker_resets_on_success(sample_server_config) -> None:
    authenticator = MagicMock()
    pool = ConnectionPool(sample_server_config, authenticator)

    for _ in range(CIRCUIT_BREAKER_THRESHOLD):
        pool._record_failure("appdb")

    assert pool._circuit_breakers["appdb"].is_open is True

    pool._record_success("appdb")
    assert pool._circuit_breakers["appdb"].is_open is False
    assert pool._circuit_breakers["appdb"].consecutive_failures == 0
    pool._check_circuit_breaker("appdb")  # Should not raise


def test_circuit_breaker_allows_probe_after_cooldown(sample_server_config) -> None:
    authenticator = MagicMock()
    pool = ConnectionPool(sample_server_config, authenticator)

    for _ in range(CIRCUIT_BREAKER_THRESHOLD):
        pool._record_failure("appdb")

    # Simulate cooldown elapsed
    pool._circuit_breakers["appdb"].last_failure_time = (
        time.monotonic() - CIRCUIT_BREAKER_COOLDOWN - 1
    )

    pool._check_circuit_breaker("appdb")  # Should not raise — half-open
    assert pool._circuit_breakers["appdb"].is_open is False


# --- Phase 19: Pool Metrics Tests ---


@pytest.mark.asyncio
async def test_pool_metrics_tracked_on_acquire_release(sample_server_config) -> None:
    authenticator = MagicMock()
    authenticator.build_connection_arguments.return_value = ConnectionArguments("Driver=test;")
    connection = _make_connection("metrics_connection")

    with patch("azure_sql_mcp.connection_pool._import_mssql_python") as import_mssql_python:
        import_mssql_python.return_value.connect.return_value = connection
        pool = ConnectionPool(sample_server_config, authenticator)

        conn = await pool.acquire("appdb")
        metrics = pool.get_metrics("appdb")

        assert metrics["appdb"]["acquire_count"] == 1
        assert metrics["appdb"]["create_count"] == 1
        assert metrics["appdb"]["active_connections"] == 1

        await pool.release("appdb", conn)
        metrics = pool.get_metrics("appdb")

        assert metrics["appdb"]["release_count"] == 1
        assert metrics["appdb"]["active_connections"] == 0


# --- Phase 19: Leak Detection Tests ---


def test_leak_detection_reports_long_held_connections(sample_server_config) -> None:
    authenticator = MagicMock()
    pool = ConnectionPool(sample_server_config, authenticator)

    fake_conn = MagicMock()
    # Simulate a lease that's been held too long
    pool._leases[id(fake_conn)] = ("appdb", time.monotonic() - 600, "stack trace here")

    leaked = pool.check_leaked_connections()
    assert len(leaked) == 1
    assert leaked[0]["database_name"] == "appdb"
    assert leaked[0]["held_seconds"] > 300


def test_leak_detection_ignores_fresh_connections(sample_server_config) -> None:
    authenticator = MagicMock()
    pool = ConnectionPool(sample_server_config, authenticator)

    fake_conn = MagicMock()
    pool._leases[id(fake_conn)] = ("appdb", time.monotonic(), "stack trace here")

    leaked = pool.check_leaked_connections()
    assert len(leaked) == 0
