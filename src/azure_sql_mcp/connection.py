from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import sys
import time
import warnings
from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Callable
from typing import Mapping
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
    positional_rows: tuple[tuple[Any, ...], ...] | None = None
    positional_rows_exact: bool | None = None

    def __post_init__(self) -> None:
        columns = tuple(self.columns)
        object.__setattr__(self, "columns", columns)
        supplied_rows = self.positional_rows
        supplied_positionally = supplied_rows is not None
        if not supplied_positionally:
            positional_rows: list[tuple[Any, ...]] = []
            for row in self.rows:
                if isinstance(row, Mapping):
                    values = tuple(row.get(column) for column in columns)
                else:  # Compatibility for callers that supplied positional rows.
                    values = tuple(row)
                positional_rows.append(values)
            exact_rows = tuple(positional_rows)
        else:
            exact_rows = tuple(tuple(row) for row in supplied_rows)
        if any(len(row) != len(columns) for row in exact_rows):
            raise ValueError("Every positional result row must match the column count.")
        object.__setattr__(self, "positional_rows", exact_rows)
        inferred_exact = supplied_positionally or len(set(columns)) == len(columns)
        if self.positional_rows_exact is True and not inferred_exact:
            raise ValueError(
                "Duplicate output names require driver-supplied positional rows."
            )
        object.__setattr__(
            self,
            "positional_rows_exact",
            inferred_exact
            if self.positional_rows_exact is None
            else self.positional_rows_exact,
        )

    @property
    def rows_positional(self) -> tuple[tuple[Any, ...], ...]:
        """Alias for integration code migrating from the dict-row view."""

        return self.positional_rows or ()

    @property
    def typed_rows(self) -> tuple[tuple[Any, ...], ...]:
        """Exact typed cells in driver column order."""

        return self.positional_rows or ()

    def comparison_rows(self) -> tuple[tuple[Any, ...], ...]:
        """Return positional rows; inspect ``positional_rows_exact`` before proof."""

        return self.positional_rows or ()


@dataclass(frozen=True)
class ProfiledExecution:
    """One user-query execution plus its STATISTICS XML result sets."""

    result_sets: list[QueryResult]
    elapsed_wall_ms: float
    user_query_executions: int = 1
    metric_provenance: str = "client_wall_clock_and_statistics_xml"
    statistics_io_messages: tuple[Any, ...] = ()


class BatchExecutionMode(str, Enum):
    DEFAULT = "default"
    ADMIN = "admin"


class AdminBatchOutcomeUnknownError(RuntimeError):
    """The admin batch was dispatched, so its final database state is unknown."""


class StatementDispatchPrevented(RuntimeError):
    """A statement hook rejected dispatch before the driver was called."""

    def __init__(self, statement_index: int, cause: Exception) -> None:
        super().__init__(str(cause))
        self.statement_index = statement_index
        self.cause = cause


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
        execution_mode: BatchExecutionMode = BatchExecutionMode.DEFAULT,
    ) -> list[QueryResult]:
        validated_database = self.config.validate_database_name(database_name)
        execution_mode = BatchExecutionMode(execution_mode)
        is_admin_batch = execution_mode is BatchExecutionMode.ADMIN

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
                        drain_all_result_sets=is_admin_batch,
                        outcome_unknown_after_dispatch=is_admin_batch,
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
                if succeeded and not is_admin_batch:
                    await self.pool.release(validated_database, connection)
                elif not deferred_cleanup:
                    # Failed and arbitrary admin batches may leave transaction,
                    # database, or SET state behind. Never return them to a pool.
                    await self.pool.discard(validated_database, connection)

        max_retries = 0 if is_admin_batch else self.config.max_retries
        return await with_retry(_attempt, max_retries=max_retries)

    async def execute_session(
        self,
        database_name: str,
        statements: Sequence[str],
        *,
        max_rows: int | None = None,
        statement_params: Sequence[Sequence[Any] | None] | None = None,
        statement_input_sizes: Sequence[Sequence[Any] | None] | None = None,
    ) -> list[list[QueryResult]]:
        """Execute several statements on the SAME pooled connection.

        Required for session-scoped SET options (e.g. SET SHOWPLAN_XML ON)
        which must be in their own batch but must persist across the
        subsequent statements run on the same session.
        """
        validated_database = self.config.validate_database_name(database_name)
        if statement_params is not None and len(statement_params) != len(statements):
            raise ValueError("statement_params must match the statement count.")
        if statement_input_sizes is not None and len(statement_input_sizes) != len(statements):
            raise ValueError("statement_input_sizes must match the statement count.")
        normalized_params = (
            tuple(tuple(params or ()) for params in statement_params)
            if statement_params is not None
            else None
        )
        normalized_input_sizes = (
            tuple(
                tuple(input_sizes) if input_sizes is not None else None
                for input_sizes in statement_input_sizes
            )
            if statement_input_sizes is not None
            else None
        )
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
                        statement_params=normalized_params,
                        statement_input_sizes=normalized_input_sizes,
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
        statement_params: Sequence[Sequence[Any] | None] | None = None,
        statement_input_sizes: Sequence[Sequence[Any] | None] | None = None,
        before_statement_dispatch: Callable[[int], Any] | None = None,
    ) -> list[list[QueryResult]]:
        """Execute one same-connection sequence without automatic retry.

        This is the bounded comparison path: retrying the sequence after one
        statement ran would under-report executions and invalidate the budget.
        ``before_statement_dispatch`` runs synchronously in the worker thread
        immediately before the selected statement's driver dispatch. If it
        raises, that statement is not dispatched.
        """

        validated_database = self.config.validate_database_name(database_name)
        if statement_params is not None and len(statement_params) != len(statements):
            raise ValueError("statement_params must match the statement count.")
        if statement_input_sizes is not None and len(statement_input_sizes) != len(statements):
            raise ValueError("statement_input_sizes must match the statement count.")
        normalized_params = (
            tuple(tuple(params or ()) for params in statement_params)
            if statement_params is not None
            else None
        )
        normalized_input_sizes = (
            tuple(
                tuple(input_sizes) if input_sizes is not None else None
                for input_sizes in statement_input_sizes
            )
            if statement_input_sizes is not None
            else None
        )
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
                    statement_params=normalized_params,
                    before_statement_dispatch=before_statement_dispatch,
                    statement_input_sizes=normalized_input_sizes,
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

        # Mutations must never be replayed after the server may have accepted
        # the request.  A transient failure therefore has to be surfaced to
        # the caller as an unknown outcome rather than retried transparently.
        return await _attempt()

    async def execute_profiled_read_only(
        self,
        database_name: str,
        query: str,
        params: Sequence[Any] | None = None,
        *,
        max_rows: int | None = None,
        input_sizes: Sequence[Any] | None = None,
    ) -> ProfiledExecution:
        """Execute a measured user query exactly once on one connection.

        Setup/teardown SET statements are not user-query executions. Measured
        calls deliberately do not use automatic retry because a retry would make
        the requested sample execute more than once.
        """

        validated_database = self.config.validate_database_name(database_name)
        connection = await self.pool.acquire(validated_database)
        execution_kwargs = (
            {"input_sizes": tuple(input_sizes)}
            if input_sizes is not None
            else {}
        )
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
                    **execution_kwargs,
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
        *,
        drain_all_result_sets: bool = False,
        outcome_unknown_after_dispatch: bool = False,
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
            try:
                cursor.execute(query, params)
                return self._consume_batches(
                    cursor,
                    max_rows=max_rows,
                    stop_on_cap=not drain_all_result_sets,
                )
            except Exception as exc:
                if outcome_unknown_after_dispatch:
                    raise AdminBatchOutcomeUnknownError(str(exc)) from exc
                raise
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    def _execute_session_with_connection(
        self,
        connection,
        database_name: str,
        statements: Sequence[str],
        max_rows: int | None,
        statement_params: Sequence[Sequence[Any]] | None = None,
        before_statement_dispatch: Callable[[int], Any] | None = None,
        statement_input_sizes: Sequence[Sequence[Any] | None] | None = None,
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
                params = statement_params[index] if statement_params is not None else ()
                input_sizes = (
                    statement_input_sizes[index]
                    if statement_input_sizes is not None
                    else None
                )
                if before_statement_dispatch is not None:
                    try:
                        before_statement_dispatch(index)
                    except Exception as exc:
                        raise StatementDispatchPrevented(index, exc) from exc
                self._execute_statement(
                    cursor,
                    statement,
                    params if params else None,
                    input_sizes=input_sizes,
                )
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
            try:
                cursor.execute(query, params)
                rowcount = 0
                while True:
                    if cursor.rowcount and cursor.rowcount > 0:
                        rowcount += cursor.rowcount
                    if not cursor.nextset():
                        break
                return rowcount
            except Exception as exc:
                raise AdminBatchOutcomeUnknownError(str(exc)) from exc
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
        input_sizes: Sequence[Any] | None = None,
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
            setup_cursor.execute("SET STATISTICS IO ON", ())
        finally:
            with contextlib.suppress(Exception):
                setup_cursor.close()

        query_cursor = connection.cursor()
        try:
            self._configure_cursor(query_cursor)
            started = time.perf_counter()
            self._execute_statement(
                query_cursor,
                query,
                params,
                input_sizes=input_sizes,
            )
            statistics_io_messages: list[Any] = []
            result_sets = self._consume_batches(
                query_cursor,
                max_rows=max_rows,
                stop_on_cap=False,
                message_sink=statistics_io_messages,
            )
            elapsed_wall_ms = (time.perf_counter() - started) * 1000.0
            return ProfiledExecution(
                result_sets=result_sets,
                elapsed_wall_ms=elapsed_wall_ms,
                metric_provenance=(
                    "client_wall_clock_statistics_xml_and_statistics_io"
                    if statistics_io_messages
                    else "client_wall_clock_and_statistics_xml"
                ),
                statistics_io_messages=tuple(statistics_io_messages),
            )
        finally:
            active_error = sys.exc_info()[0] is not None
            with contextlib.suppress(Exception):
                query_cursor.close()
            teardown_cursor = connection.cursor()
            try:
                self._configure_cursor(teardown_cursor)
                teardown_cursor.execute("SET STATISTICS IO OFF", ())
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

    @staticmethod
    def _execute_statement(
        cursor,
        statement: str,
        params: Sequence[Any] | None = None,
        *,
        input_sizes: Sequence[Any] | None = None,
    ) -> Any:
        """Execute one statement, optionally applying driver input sizes.

        mssql-python permits a short ``setinputsizes`` list, but emits one
        predictable warning because the remaining parameters are inferred.
        Suppress only that warning and only around this matching execute.
        """

        if input_sizes is None:
            if params is None:
                return cursor.execute(statement)
            return cursor.execute(statement, params)

        bound_params = () if params is None else params
        cursor.setinputsizes(input_sizes)
        if len(input_sizes) < len(bound_params):
            warning_message = (
                f"Number of input sizes ({len(input_sizes)}) does not match "
                f"number of parameters ({len(bound_params)}). "
                "This may lead to unexpected behavior."
            )
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=f"^{re.escape(warning_message)}$",
                    category=Warning,
                )
                return cursor.execute(statement, bound_params)
        return cursor.execute(statement, bound_params)

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
        self,
        cursor,
        *,
        max_rows: int | None = None,
        stop_on_cap: bool = True,
        message_sink: list[Any] | None = None,
    ) -> list[QueryResult]:
        results: list[QueryResult] = []
        captured_messages: tuple[Any, ...] = ()
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
                positional_rows = tuple(
                    tuple(self._coerce_value(value) for value in raw_row)
                    for raw_row in raw_rows
                )
                results.append(
                    QueryResult(
                        columns=columns,
                        rows=rows,
                        column_type_signatures=type_signatures,
                        positional_rows=positional_rows,
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

            if message_sink is not None:
                captured_messages = self._capture_cursor_messages(
                    cursor,
                    message_sink,
                    captured_messages,
                )
            has_next = cursor.nextset()
            if message_sink is not None:
                captured_messages = self._capture_cursor_messages(
                    cursor,
                    message_sink,
                    () if has_next else captured_messages,
                )
            if not has_next:
                break
        return results

    @staticmethod
    def _capture_cursor_messages(
        cursor,
        message_sink: list[Any],
        previous_snapshot: tuple[Any, ...],
    ) -> tuple[Any, ...]:
        """Capture messages before ``nextset`` can clear the driver buffer.

        Mutable DB-API message buffers are drained after each capture. Immutable
        snapshots are prefix-deduplicated only within one result-set epoch;
        ``nextset`` starts a new message-buffer epoch.
        """

        raw_messages = getattr(cursor, "messages", ()) or ()
        try:
            current_snapshot = tuple(raw_messages)
        except TypeError:
            return previous_snapshot
        if not current_snapshot:
            return current_snapshot

        if (
            previous_snapshot
            and len(current_snapshot) >= len(previous_snapshot)
            and current_snapshot[: len(previous_snapshot)] == previous_snapshot
        ):
            message_sink.extend(current_snapshot[len(previous_snapshot) :])
        else:
            message_sink.extend(current_snapshot)

        clear_messages = getattr(raw_messages, "clear", None)
        if callable(clear_messages):
            try:
                clear_messages()
            except (AttributeError, RuntimeError, TypeError):
                pass
            else:
                return ()
        return current_snapshot

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
