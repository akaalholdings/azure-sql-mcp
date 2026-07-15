from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import time
from dataclasses import dataclass
from typing import Any
from typing import Sequence

from .auth import AzureSqlAuthenticator
from .config import ServerConfig
from .connection_pool import ConnectionPool
from .retry import with_retry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: list[dict[str, Any]]
    column_type_signatures: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProfiledExecution:
    """One user-query execution plus its STATISTICS XML result sets."""

    result_sets: list[QueryResult]
    elapsed_wall_ms: float
    user_query_executions: int = 1
    metric_provenance: str = "client_wall_clock_and_statistics_xml"


class AzureSqlExecutor:
    def __init__(
        self,
        config: ServerConfig,
        authenticator: AzureSqlAuthenticator,
        pool: ConnectionPool,
    ):
        self.config = config
        self.authenticator = authenticator
        self.pool = pool

    async def fetch_all(
        self,
        database_name: str,
        query: str,
        params: Sequence[Any] | None = None,
        *,
        max_rows: int | None = None,
    ) -> list[dict[str, Any]]:
        results = await self.execute_batches(
            database_name, query, params=params, max_rows=max_rows,
        )
        for result in results:
            if result.columns:
                return result.rows
        return []

    async def execute_batches(
        self,
        database_name: str,
        query: str,
        params: Sequence[Any] | None = None,
        *,
        max_rows: int | None = None,
    ) -> list[QueryResult]:
        validated_database = self.config.validate_database_name(database_name)

        async def _attempt() -> list[QueryResult]:
            connection = await self.pool.acquire(validated_database)
            succeeded = False
            deferred_cleanup = False
            execution_task: asyncio.Task[list[QueryResult]] | None = None
            try:
                execution_task = asyncio.create_task(
                    asyncio.to_thread(
                        self._execute_with_connection,
                        connection,
                        validated_database,
                        query,
                        tuple(params or ()),
                        max_rows,
                    )
                )
                result = await asyncio.shield(execution_task)
                succeeded = True
                return result
            except asyncio.CancelledError:
                if execution_task is not None:
                    self._request_cancel(connection)
                    deferred_cleanup = True
                    asyncio.create_task(
                        self._discard_connection_when_task_finishes(
                            validated_database,
                            connection,
                            execution_task,
                        )
                    )
                raise
            finally:
                if succeeded:
                    await self.pool.release(validated_database, connection)
                elif not deferred_cleanup:
                    # On error, don't return connection to pool — it may be in a bad state.
                    await self.pool.discard(validated_database, connection)

        return await with_retry(_attempt, max_retries=self.config.max_retries)

    async def execute_session(
        self,
        database_name: str,
        statements: Sequence[str],
        *,
        max_rows: int | None = None,
    ) -> list[list[QueryResult]]:
        """Execute several statements on the SAME pooled connection.

        Required for session-scoped SET options (e.g. SET SHOWPLAN_XML ON)
        which must be in their own batch but must persist across the
        subsequent statements run on the same session.
        """
        validated_database = self.config.validate_database_name(database_name)

        async def _attempt() -> list[list[QueryResult]]:
            connection = await self.pool.acquire(validated_database)
            succeeded = False
            deferred_cleanup = False
            execution_task: asyncio.Task[list[list[QueryResult]]] | None = None
            try:
                execution_task = asyncio.create_task(
                    asyncio.to_thread(
                        self._execute_session_with_connection,
                        connection,
                        validated_database,
                        tuple(statements),
                        max_rows,
                    )
                )
                result = await asyncio.shield(execution_task)
                succeeded = True
                return result
            except asyncio.CancelledError:
                if execution_task is not None:
                    self._request_cancel(connection)
                    deferred_cleanup = True
                    asyncio.create_task(
                        self._discard_connection_when_task_finishes(
                            validated_database,
                            connection,
                            execution_task,
                        )
                    )
                raise
            finally:
                if succeeded:
                    await self.pool.release(validated_database, connection)
                elif not deferred_cleanup:
                    await self.pool.discard(validated_database, connection)

        return await with_retry(_attempt, max_retries=self.config.max_retries)

    async def execute_session_exactly_once(
        self,
        database_name: str,
        statements: Sequence[str],
        *,
        max_rows: int | None = None,
    ) -> list[list[QueryResult]]:
        """Execute one same-connection sequence without automatic retry.

        This is the bounded comparison path: retrying the sequence after one
        statement ran would under-report executions and invalidate the budget.
        """

        validated_database = self.config.validate_database_name(database_name)
        connection = await self.pool.acquire(validated_database)
        succeeded = False
        deferred_cleanup = False
        execution_task: asyncio.Task[list[list[QueryResult]]] | None = None
        try:
            execution_task = asyncio.create_task(
                asyncio.to_thread(
                    self._execute_session_with_connection,
                    connection,
                    validated_database,
                    tuple(statements),
                    max_rows,
                )
            )
            result = await asyncio.shield(execution_task)
            succeeded = True
            return result
        except asyncio.CancelledError:
            if execution_task is not None:
                self._request_cancel(connection)
                deferred_cleanup = True
                asyncio.create_task(
                    self._discard_connection_when_task_finishes(
                        validated_database,
                        connection,
                        execution_task,
                    )
                )
            raise
        finally:
            if succeeded:
                await self.pool.release(validated_database, connection)
            elif not deferred_cleanup:
                await self.pool.discard(validated_database, connection)

    async def execute_non_query(
        self,
        database_name: str,
        query: str,
        params: Sequence[Any] | None = None,
    ) -> int:
        validated_database = self.config.validate_database_name(database_name)

        async def _attempt() -> int:
            connection = await self.pool.acquire(validated_database)
            succeeded = False
            deferred_cleanup = False
            execution_task: asyncio.Task[int] | None = None
            try:
                execution_task = asyncio.create_task(
                    asyncio.to_thread(
                        self._execute_non_query_with_connection,
                        connection,
                        validated_database,
                        query,
                        tuple(params or ()),
                    )
                )
                rowcount = await asyncio.shield(execution_task)
                succeeded = True
                return rowcount
            except asyncio.CancelledError:
                if execution_task is not None:
                    self._request_cancel(connection)
                    deferred_cleanup = True
                    asyncio.create_task(
                        self._discard_connection_when_task_finishes(
                            validated_database,
                            connection,
                            execution_task,
                        )
                    )
                raise
            finally:
                if succeeded:
                    await self.pool.release(validated_database, connection)
                elif not deferred_cleanup:
                    await self.pool.discard(validated_database, connection)

        return await with_retry(_attempt, max_retries=self.config.max_retries)

    async def execute_profiled_read_only(
        self,
        database_name: str,
        query: str,
        params: Sequence[Any] | None = None,
        *,
        max_rows: int | None = None,
    ) -> ProfiledExecution:
        """Execute a measured user query exactly once on one connection.

        Setup/teardown SET statements are not user-query executions. Measured
        calls deliberately do not use automatic retry because a retry would make
        the requested sample execute more than once.
        """

        validated_database = self.config.validate_database_name(database_name)
        connection = await self.pool.acquire(validated_database)
        succeeded = False
        deferred_cleanup = False
        execution_task: asyncio.Task[ProfiledExecution] | None = None
        try:
            execution_task = asyncio.create_task(
                asyncio.to_thread(
                    self._execute_profiled_with_connection,
                    connection,
                    validated_database,
                    query,
                    tuple(params or ()),
                    max_rows,
                )
            )
            result = await asyncio.shield(execution_task)
            succeeded = True
            return result
        except asyncio.CancelledError:
            if execution_task is not None:
                self._request_cancel(connection)
                deferred_cleanup = True
                asyncio.create_task(
                    self._discard_connection_when_task_finishes(
                        validated_database,
                        connection,
                        execution_task,
                    )
                )
            raise
        finally:
            if succeeded:
                await self.pool.release(validated_database, connection)
            elif not deferred_cleanup:
                await self.pool.discard(validated_database, connection)

    async def _discard_connection_when_task_finishes(
        self,
        database_name: str,
        connection,
        execution_task: asyncio.Task[Any],
    ) -> None:
        # If a tool call is cancelled (for example by an outer timeout), keep the
        # in-flight DB operation attached to this connection until the worker
        # thread finishes, then discard the connection so it is never reused.
        with contextlib.suppress(Exception):
            await execution_task
        try:
            await self.pool.discard(database_name, connection)
        except Exception:
            logger.exception(
                "Failed to discard timed-out connection",
                extra={"database_name": database_name},
            )

    def _execute_with_connection(
        self,
        connection,
        database_name: str,
        query: str,
        params: Sequence[Any],
        max_rows: int | None = None,
    ) -> list[QueryResult]:
        logger.debug(
            "Executing query",
            extra={
                "database_name": database_name,
                "server": self.config.server,
            },
        )
        cursor = connection.cursor()
        try:
            self._configure_cursor(cursor)
            self._set_lock_timeout(cursor)
            cursor.execute(query, params)
            return self._consume_batches(cursor, max_rows=max_rows)
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    def _execute_session_with_connection(
        self,
        connection,
        database_name: str,
        statements: Sequence[str],
        max_rows: int | None,
    ) -> list[list[QueryResult]]:
        logger.debug(
            "Executing session",
            extra={
                "database_name": database_name,
                "server": self.config.server,
                "statement_count": len(statements),
            },
        )
        per_statement_results: list[list[QueryResult]] = []
        for index, statement in enumerate(statements):
            cursor = connection.cursor()
            try:
                self._configure_cursor(cursor)
                if index == 0:
                    self._set_lock_timeout(cursor)
                cursor.execute(statement)
                per_statement_results.append(
                    self._consume_batches(cursor, max_rows=max_rows, stop_on_cap=False)
                )
            finally:
                cursor.close()
        return per_statement_results

    def _execute_non_query_with_connection(
        self,
        connection,
        database_name: str,
        query: str,
        params: Sequence[Any],
    ) -> int:
        logger.debug(
            "Executing non-query",
            extra={
                "database_name": database_name,
                "server": self.config.server,
            },
        )
        cursor = connection.cursor()
        try:
            self._configure_cursor(cursor)
            self._set_lock_timeout(cursor)
            cursor.execute(query, params)
            rowcount = 0
            while True:
                if cursor.rowcount and cursor.rowcount > 0:
                    rowcount += cursor.rowcount
                if not cursor.nextset():
                    break
            return rowcount
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    def _execute_profiled_with_connection(
        self,
        connection,
        database_name: str,
        query: str,
        params: Sequence[Any],
        max_rows: int | None,
    ) -> ProfiledExecution:
        logger.debug(
            "Executing one profiled query sample",
            extra={"database_name": database_name, "server": self.config.server},
        )

        setup_cursor = connection.cursor()
        try:
            self._configure_cursor(setup_cursor)
            self._set_lock_timeout(setup_cursor)
            setup_cursor.execute("SET STATISTICS XML ON", ())
        finally:
            with contextlib.suppress(Exception):
                setup_cursor.close()

        query_cursor = connection.cursor()
        try:
            self._configure_cursor(query_cursor)
            started = time.perf_counter()
            query_cursor.execute(query, params)
            result_sets = self._consume_batches(
                query_cursor,
                max_rows=max_rows,
                stop_on_cap=False,
            )
            elapsed_wall_ms = (time.perf_counter() - started) * 1000.0
            return ProfiledExecution(
                result_sets=result_sets,
                elapsed_wall_ms=elapsed_wall_ms,
            )
        finally:
            active_error = sys.exc_info()[0] is not None
            with contextlib.suppress(Exception):
                query_cursor.close()
            teardown_cursor = connection.cursor()
            try:
                self._configure_cursor(teardown_cursor)
                teardown_cursor.execute("SET STATISTICS XML OFF", ())
            except Exception:
                if not active_error:
                    raise
            finally:
                with contextlib.suppress(Exception):
                    teardown_cursor.close()

    def _configure_cursor(self, cursor) -> None:
        with contextlib.suppress(Exception):
            cursor.timeout = self.config.query_timeout_seconds

    def _set_lock_timeout(self, cursor) -> None:
        lock_timeout_ms = max(1, int(self.config.query_timeout_seconds)) * 1000
        cursor.execute(f"SET LOCK_TIMEOUT {lock_timeout_ms}", ())

    def _request_cancel(self, connection) -> None:
        for method_name in ("cancel", "interrupt"):
            method = getattr(connection, method_name, None)
            if callable(method):
                with contextlib.suppress(Exception):
                    method()
                return

    def _consume_batches(
        self, cursor, *, max_rows: int | None = None, stop_on_cap: bool = True,
    ) -> list[QueryResult]:
        results: list[QueryResult] = []
        while True:
            if cursor.description:
                description = tuple(cursor.description)
                columns = tuple(column[0] for column in description)
                type_signatures = tuple(
                    signature
                    for column in description
                    if (signature := self._column_type_signature(column)) is not None
                )
                if len(type_signatures) != len(columns):
                    type_signatures = ()
                raw_rows = (
                    cursor.fetchmany(max_rows)
                    if max_rows is not None
                    else cursor.fetchall()
                )
                rows = [
                    {
                        column: self._coerce_value(value)
                        for column, value in zip(columns, raw_row)
                    }
                    for raw_row in raw_rows
                ]
                results.append(
                    QueryResult(
                        columns=columns,
                        rows=rows,
                        column_type_signatures=type_signatures,
                    )
                )

                if stop_on_cap and max_rows is not None and len(raw_rows) >= max_rows:
                    # Row cap hit: this result set is being truncated anyway.
                    # nextset() would DRAIN every remaining row over the wire
                    # to advance (minutes for a 17.9M-row SELECT); closing the
                    # cursor discards pending rows protocol-side instead, so
                    # stop here and skip any later result sets in the batch.
                    # SHOWPLAN sessions pass stop_on_cap=False: their plan XML
                    # arrives as a later result set and needs the drain.
                    break

            has_next = cursor.nextset()
            if not has_next:
                break
        return results

    @staticmethod
    def _column_type_signature(description: Sequence[Any]) -> str | None:
        """Return stable driver shape metadata without retaining result values."""

        if len(description) < 2 or description[1] is None:
            return None
        type_code = description[1]
        if isinstance(type_code, type):
            type_name = f"{type_code.__module__}.{type_code.__qualname__}"
        else:
            type_name = type(type_code).__name__ + ":" + str(type_code)
        attributes = tuple(
            description[index] if len(description) > index else None
            for index in range(2, 7)
        )
        return "|".join((type_name, *(repr(value) for value in attributes)))

    def _coerce_value(self, value: Any) -> Any:
        if isinstance(value, memoryview):
            value = value.tobytes()
        if isinstance(value, bytes):
            # Text stored in binary columns decodes cleanly; genuine binary
            # data (hashes, rowversion, images) is hex-encoded rather than
            # silently mangled by a lossy decode.
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                return "0x" + value.hex().upper()
        return value
