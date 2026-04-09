from __future__ import annotations

import asyncio
import logging
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from .auth import AzureSqlAuthenticator
from .config import AuthMode
from .config import ServerConfig

logger = logging.getLogger(__name__)

# Azure AD tokens are valid for ~60-75 minutes; refresh proactively at 45 min
TOKEN_REFRESH_SECONDS = 45 * 60
VALIDATION_TIMEOUT_SECONDS = 2

# Circuit breaker defaults
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_COOLDOWN = 30.0

# Connection leak detection
LEASE_TIMEOUT_SECONDS = 300.0  # 5 minutes


@dataclass
class PoolMetrics:
    """Tracks pool utilization metrics for a single database."""

    acquire_count: int = 0
    release_count: int = 0
    discard_count: int = 0
    create_count: int = 0
    timeout_count: int = 0
    active_connections: int = 0
    peak_active: int = 0
    total_wait_time_ms: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "acquire_count": self.acquire_count,
            "release_count": self.release_count,
            "discard_count": self.discard_count,
            "create_count": self.create_count,
            "timeout_count": self.timeout_count,
            "active_connections": self.active_connections,
            "peak_active": self.peak_active,
            "total_wait_time_ms": round(self.total_wait_time_ms, 2),
        }


@dataclass
class CircuitBreakerState:
    """Per-database circuit breaker state."""

    consecutive_failures: int = 0
    last_failure_time: float = 0.0
    is_open: bool = False


def _import_mssql_python():
    try:
        import mssql_python  # type: ignore
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "mssql-python is required. Install project dependencies before running the MCP server."
        ) from exc

    # The application already owns pooling and token-refresh behavior.
    mssql_python.pooling(enabled=False)
    return mssql_python


class ConnectionPool:
    """Per-database connection pool using asyncio.Queue for async acquire/release."""

    def __init__(
        self,
        config: ServerConfig,
        authenticator: AzureSqlAuthenticator,
    ):
        self.config = config
        self.authenticator = authenticator
        self._pools: dict[str, asyncio.Queue] = defaultdict(
            lambda: asyncio.Queue(maxsize=config.pool_size)
        )
        self._pool_sizes: dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()
        self._token_lock = asyncio.Lock()
        self._token_acquired_at: float | None = None
        self._closed = False
        self._metrics: dict[str, PoolMetrics] = defaultdict(PoolMetrics)
        self._circuit_breakers: dict[str, CircuitBreakerState] = defaultdict(
            CircuitBreakerState
        )
        self._leases: dict[int, tuple[str, float, str]] = {}  # conn_id -> (db, time, stack)

    def _needs_token_refresh(self) -> bool:
        if self.config.auth_mode == AuthMode.SQL_PASSWORD:
            return False
        if self._token_acquired_at is None:
            return True
        return (time.monotonic() - self._token_acquired_at) >= TOKEN_REFRESH_SECONDS

    def _create_connection(self, database_name: str):
        driver = _import_mssql_python()
        if self._needs_token_refresh():
            self._token_acquired_at = time.monotonic()
        connect_args = self.authenticator.build_connection_arguments(database_name)
        connection = driver.connect(
            connect_args.connection_string,
            attrs_before=connect_args.attrs_before or {},
            autocommit=True,
            timeout=self.config.query_timeout_seconds,
        )
        connection.timeout = self.config.query_timeout_seconds
        return connection

    def _check_circuit_breaker(self, database_name: str) -> None:
        """Raise if circuit breaker is open and cooldown hasn't elapsed."""
        cb = self._circuit_breakers[database_name]
        if not cb.is_open:
            return
        elapsed = time.monotonic() - cb.last_failure_time
        if elapsed < CIRCUIT_BREAKER_COOLDOWN:
            raise ConnectionError(
                f"Circuit breaker open for '{database_name}': "
                f"{cb.consecutive_failures} consecutive failures. "
                f"Retry after {CIRCUIT_BREAKER_COOLDOWN - elapsed:.0f}s."
            )
        # Cooldown elapsed — allow a probe attempt (half-open)
        cb.is_open = False

    def _record_success(self, database_name: str) -> None:
        cb = self._circuit_breakers[database_name]
        cb.consecutive_failures = 0
        cb.is_open = False

    def _record_failure(self, database_name: str) -> None:
        cb = self._circuit_breakers[database_name]
        cb.consecutive_failures += 1
        cb.last_failure_time = time.monotonic()
        if cb.consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD:
            cb.is_open = True
            logger.warning(
                "Circuit breaker opened",
                extra={
                    "database_name": database_name,
                    "consecutive_failures": cb.consecutive_failures,
                },
            )

    def _track_lease(self, database_name: str, connection) -> None:
        conn_id = id(connection)
        stack = "".join(traceback.format_stack()[-4:-1])
        self._leases[conn_id] = (database_name, time.monotonic(), stack)

    def _release_lease(self, connection) -> None:
        self._leases.pop(id(connection), None)

    def check_leaked_connections(self) -> list[dict[str, Any]]:
        """Return info about connections held longer than LEASE_TIMEOUT_SECONDS."""
        now = time.monotonic()
        leaked: list[dict[str, Any]] = []
        for conn_id, (db, acquired_at, stack) in self._leases.items():
            held_seconds = now - acquired_at
            if held_seconds > LEASE_TIMEOUT_SECONDS:
                leaked.append(
                    {
                        "connection_id": conn_id,
                        "database_name": db,
                        "held_seconds": round(held_seconds, 1),
                        "acquire_stack": stack,
                    }
                )
                logger.warning(
                    "Possible connection leak detected",
                    extra={
                        "connection_id": conn_id,
                        "database_name": db,
                        "held_seconds": round(held_seconds, 1),
                    },
                )
        return leaked

    def get_metrics(self, database_name: str | None = None) -> dict[str, Any]:
        """Return pool utilization metrics."""
        if database_name:
            return {database_name: self._metrics[database_name].as_dict()}
        return {db: m.as_dict() for db, m in self._metrics.items()}

    def _validate_connection(self, connection) -> bool:
        original_timeout = getattr(connection, "timeout", None)
        try:
            if original_timeout is not None:
                if original_timeout <= 0:
                    connection.timeout = VALIDATION_TIMEOUT_SECONDS
                else:
                    connection.timeout = min(original_timeout, VALIDATION_TIMEOUT_SECONDS)
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            return False
        finally:
            if original_timeout is not None:
                connection.timeout = original_timeout

    async def acquire(self, database_name: str):
        """Acquire a connection from the pool, creating one if needed."""
        if self._closed:
            raise RuntimeError("Connection pool is closed.")

        self._check_circuit_breaker(database_name)

        metrics = self._metrics[database_name]
        metrics.acquire_count += 1
        wait_start = time.monotonic()
        pool = self._pools[database_name]

        while True:
            # First, try to reuse an idle connection immediately.
            try:
                connection = pool.get_nowait()
            except asyncio.QueueEmpty:
                connection = None

            if connection is not None:
                is_valid = await asyncio.to_thread(self._validate_connection, connection)
                if is_valid and not self._needs_token_refresh():
                    logger.debug(
                        "Reusing pooled connection",
                        extra={"database_name": database_name},
                    )
                    self._record_success(database_name)
                    metrics.active_connections += 1
                    metrics.peak_active = max(metrics.peak_active, metrics.active_connections)
                    metrics.total_wait_time_ms += (time.monotonic() - wait_start) * 1000
                    self._track_lease(database_name, connection)
                    return connection

                await self.discard(database_name, connection)
                continue

            create_new = False
            async with self._lock:
                if self._pool_sizes[database_name] < self.config.pool_size:
                    self._pool_sizes[database_name] += 1
                    create_new = True

            if create_new:
                logger.debug(
                    "Creating new pooled connection",
                    extra={"database_name": database_name},
                )
                try:
                    conn = await asyncio.to_thread(
                        self._create_connection, database_name
                    )
                    self._record_success(database_name)
                    metrics.create_count += 1
                    metrics.active_connections += 1
                    metrics.peak_active = max(metrics.peak_active, metrics.active_connections)
                    metrics.total_wait_time_ms += (time.monotonic() - wait_start) * 1000
                    self._track_lease(database_name, conn)
                    return conn
                except Exception:
                    async with self._lock:
                        self._pool_sizes[database_name] -= 1
                    self._record_failure(database_name)
                    raise

            logger.debug(
                "Pool at capacity, waiting for release",
                extra={"database_name": database_name},
            )
            connection = await pool.get()
            is_valid = await asyncio.to_thread(self._validate_connection, connection)
            if is_valid and not self._needs_token_refresh():
                self._record_success(database_name)
                metrics.active_connections += 1
                metrics.peak_active = max(metrics.peak_active, metrics.active_connections)
                metrics.total_wait_time_ms += (time.monotonic() - wait_start) * 1000
                self._track_lease(database_name, connection)
                return connection
            await self.discard(database_name, connection)

    async def release(self, database_name: str, connection) -> None:
        """Return a connection to the pool."""
        self._release_lease(connection)
        metrics = self._metrics[database_name]
        metrics.release_count += 1
        metrics.active_connections = max(0, metrics.active_connections - 1)

        if self._closed:
            await self.discard(database_name, connection)
            return

        pool = self._pools[database_name]
        try:
            pool.put_nowait(connection)
        except asyncio.QueueFull:
            # Pool is full; discard the connection
            await self.discard(database_name, connection)

    async def discard(self, database_name: str, connection) -> None:
        """Close a broken connection and release its pool slot."""
        self._release_lease(connection)
        self._metrics[database_name].discard_count += 1
        await asyncio.to_thread(self._close_connection, connection)
        async with self._lock:
            if self._pool_sizes[database_name] > 0:
                self._pool_sizes[database_name] -= 1

    async def close_all(self) -> None:
        """Drain all pools and close all connections."""
        self._closed = True
        for database_name, pool in self._pools.items():
            while not pool.empty():
                try:
                    connection = pool.get_nowait()
                    await self.discard(database_name, connection)
                except asyncio.QueueEmpty:
                    break
        logger.info("All connection pools drained.")

    def _close_connection(self, connection) -> None:
        try:
            connection.close()
        except Exception:
            pass
