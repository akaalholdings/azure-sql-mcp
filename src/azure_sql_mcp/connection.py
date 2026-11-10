from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import math
import re
import sys
import threading
import time
import warnings
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Callable
from typing import Iterator
from typing import Mapping
from typing import Sequence
from typing import TypeVar

from .auth import AzureSqlAuthenticator
from .config import ServerConfig
from .connection_pool import ConnectionPool
from .observability import SQL_ORIGINS
from .observability import tag_sql_origin
from .retry import with_retry

logger = logging.getLogger(__name__)

# Monotonic time after which no statement may be dispatched. It flows into
# asyncio.to_thread workers because to_thread copies the caller's context.
_STATEMENT_DEADLINE: ContextVar[float | None] = ContextVar(
    "azure_sql_mcp_statement_deadline", default=None
)


_DRIVER_CANCEL_METHODS = ("cancel", "interrupt")
# A statement's driver timeout is set this long after the deadline, so the
# caller's own timeout is reported first and not a driver error.
_DEADLINE_GRACE_SECONDS = 1


@contextlib.contextmanager
def statement_deadline(seconds: float | None) -> Iterator[None]:
    """Cap every statement started in this context to ``seconds`` from now.

    A nested deadline never extends an outer one. ``None`` adds no limit.
    """

    if seconds is None:
        yield
        return
    deadline = time.monotonic() + max(0.0, seconds)
    outer = _STATEMENT_DEADLINE.get()
    if outer is not None:
        deadline = min(deadline, outer)
    token = _STATEMENT_DEADLINE.set(deadline)
    try:
        yield
    finally:
        _STATEMENT_DEADLINE.reset(token)


def _validated_origin(origin: str | None) -> str | None:
    if origin is not None and origin not in SQL_ORIGINS:
        raise ValueError(f"sql_origin must be one of: {', '.join(SQL_ORIGINS)}.")
    return origin


@contextlib.contextmanager
def _statement_origin(origin: str | None) -> Iterator[None]:
    """Tag an error raised inside with who built the failing statement."""

    try:
        yield
    except Exception as exc:
        if origin is not None:
            tag_sql_origin(exc, origin)
        raise


def driver_cancel_supported() -> bool:
    """True when the installed driver can cancel a statement from another thread.

    Reads the driver class only; no connection is opened. mssql-python 1.10
    has no such call.
    """

    try:
        from mssql_python.connection import Connection
    except Exception:
        return False
    return any(
        callable(getattr(Connection, name, None)) for name in _DRIVER_CANCEL_METHODS
    )


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
    # fetch_all only. Gated writes use DEFAULT or ADMIN and keep their batch
    # text and their discard-on-error behaviour.
    READ = "read"


# A batch that starts with SELECT or WITH holds no statement that must be
# first in its batch, so SET LOCK_TIMEOUT can go in front of it.
_PLAIN_READ_START = re.compile(r"\s*(?:SELECT|WITH)\b", re.IGNORECASE)

# Statement errors with a fixed severity below 20: invalid column or object,
# permission denied, unbound identifier, divide by zero.
_REUSABLE_ERROR_NUMBERS = frozenset({207, 208, 229, 297, 300, 4104, 8134})
# The ODBC driver gives each of these SQLSTATE classes to one kind of statement
# error (208, 207, 8134). 42000 is its catch-all for server errors.
_SPECIFIC_STATEMENT_SQLSTATES = ("42S02", "42S22", "22012")
_GENERIC_STATEMENT_SQLSTATE = "42000"
_ERROR_NUMBER_SUFFIX = re.compile(r"\((\d{1,6})\)\s*$")
# A session-level SET that outlives the statements of one call. The pool does
# not reset a session, so the executor puts the default back before release.
_ISOLATION_LEVEL_SET = re.compile(
    r"\bSET\s+TRANSACTION\s+ISOLATION\s+LEVEL\s+"
    r"(READ\s+UNCOMMITTED|READ\s+COMMITTED|REPEATABLE\s+READ|SNAPSHOT|SERIALIZABLE)\b",
    re.IGNORECASE,
)
_DEFAULT_ISOLATION_LEVEL = "READ COMMITTED"
# mssql-python 1.10 returns permission errors (229, 297, 300) in the catch-all
# class with no number, so the server text is the only thing that names them.
_PERMISSION_DENIED_TEXT = re.compile(
    r"permission was denied|does not have permission to perform this action", re.IGNORECASE
)
_statement_error_texts: dict[str, str] | None = None


def _statement_error_sqlstate(exc: BaseException) -> str | None:
    """Return the statement-error SQLSTATE of a mssql-python exception.

    The driver puts one fixed text per SQLSTATE in ``driver_error``.
    """

    global _statement_error_texts
    if _statement_error_texts is None:
        texts: dict[str, str] = {}
        try:
            from mssql_python.exceptions import sqlstate_to_exception

            for state in (*_SPECIFIC_STATEMENT_SQLSTATES, _GENERIC_STATEMENT_SQLSTATE):
                text = getattr(sqlstate_to_exception(state, ""), "driver_error", None)
                if isinstance(text, str):
                    texts[text] = state
        except Exception:
            texts = {}
        _statement_error_texts = texts
    driver_error = getattr(exc, "driver_error", None)
    if not isinstance(driver_error, str):
        return None
    return _statement_error_texts.get(driver_error)


def _is_reusable_statement_error(exc: BaseException) -> bool:
    """True when the error proves the session is still usable.

    mssql-python 1.10 exposes no error number and no severity. So the SQLSTATE
    class must be one that only a harmless statement error gets, the server
    text must end with a listed error number, or it must be a permission
    error. All else is unknown.
    """

    sqlstate = _statement_error_sqlstate(exc)
    if sqlstate is None:
        return False
    ddbc_error = getattr(exc, "ddbc_error", None)
    number = _ERROR_NUMBER_SUFFIX.search(ddbc_error) if isinstance(ddbc_error, str) else None
    if number is not None:
        return int(number.group(1)) in _REUSABLE_ERROR_NUMBERS
    if sqlstate in _SPECIFIC_STATEMENT_SQLSTATES:
        return True
    return isinstance(ddbc_error, str) and _PERMISSION_DENIED_TEXT.search(ddbc_error) is not None


class AdminBatchOutcomeUnknownError(RuntimeError):
    """The admin batch was dispatched, so its final database state is unknown."""


class StatementDispatchPrevented(RuntimeError):
    """A statement hook rejected dispatch before the driver was called."""

    def __init__(self, statement_index: int, cause: Exception) -> None:
        super().__init__(str(cause))
        self.statement_index = statement_index
        self.cause = cause


class ReadOnlyEnvelopeViolation(RuntimeError):
    """A batch run in the read-only envelope ended or committed its transaction."""


_ENVELOPE_END = (
    "DECLARE @mcp_trancount int = @@TRANCOUNT; "
    "IF @mcp_trancount > 0 ROLLBACK TRANSACTION; "
    "SELECT @mcp_trancount AS trancount;"
)


class ReadOnlyIntentError(PermissionError):
    """A write was not sent: this database is connected with read intent."""


class StatementDispatchStopped(TimeoutError):
    """A statement was not sent: the caller was cancelled or the deadline passed."""


TransactionResultT = TypeVar("TransactionResultT")


class TransactionCommitOutcomeUnknownError(RuntimeError):
    """The commit was dispatched but its final database outcome is unknown."""


class TransactionRollbackError(RuntimeError):
    """A pre-commit failure could not be rolled back safely."""


class SqlTransactionSession:
    """Synchronous operations over the one connection owned by a transaction."""

    def __init__(
        self,
        connection: Any,
        executor: "AzureSqlExecutor",
        cancel_event: threading.Event | None = None,
    ) -> None:
        self._connection = connection
        self._executor = executor
        self._cancel_event = cancel_event

    def execute(
        self,
        statement: str,
        params: Sequence[Any] | None = None,
    ) -> int | None:
        cursor = self._executor._open_cursor(self._connection, self._cancel_event)
        try:
            self._executor._execute_statement(cursor, statement, params)
            return cursor.rowcount
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    def fetch_all(
        self,
        statement: str,
        params: Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        cursor = self._executor._open_cursor(self._connection, self._cancel_event)
        try:
            self._executor._execute_statement(cursor, statement, params)
            if not cursor.description:
                return []
            columns = tuple(column[0] for column in cursor.description)
            return [
                {
                    column: self._executor._coerce_value(value)
                    for column, value in zip(columns, row)
                }
                for row in cursor.fetchall()
            ]
        finally:
            with contextlib.suppress(Exception):
                cursor.close()


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
        self._cancel_unsupported_logged = False

    async def fetch_all(
        self,
        database_name: str,
        query: str,
        params: Sequence[Any] | None = None,
        *,
        max_rows: int | None = None,
        sql_origin: str | None = None,
        read_only_envelope: bool = False,
    ) -> list[dict[str, Any]]:
        if sql_origin is None and not read_only_envelope:
            # Unchanged call for a plain read.
            results = await self.execute_batches(
                database_name,
                query,
                params=params,
                max_rows=max_rows,
                execution_mode=BatchExecutionMode.READ,
            )
        else:
            results = await self.execute_batches(
                database_name,
                query,
                params=params,
                max_rows=max_rows,
                execution_mode=BatchExecutionMode.READ,
                sql_origin=sql_origin,
                read_only_envelope=read_only_envelope,
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
        sql_origin: str | None = None,
        read_only_envelope: bool = False,
    ) -> list[QueryResult]:
        """Run one batch. ``sql_origin`` ("server_built" or "caller_supplied")
        is recorded on a driver error so the incident log can tell a server
        defect from a caller error.

        ``read_only_envelope`` runs the batch, text unchanged, inside a
        transaction that is always rolled back, and raises
        ``ReadOnlyEnvelopeViolation`` when the batch ended or committed it.
        It undoes an uncommitted write; it cannot undo a COMMIT in the batch.
        """

        validated_database = self.config.validate_database_name(database_name)
        sql_origin = _validated_origin(sql_origin)
        execution_mode = BatchExecutionMode(execution_mode)
        is_admin_batch = execution_mode is BatchExecutionMode.ADMIN
        is_read = execution_mode is BatchExecutionMode.READ
        if is_admin_batch:
            if read_only_envelope:
                raise ValueError("An admin batch cannot run in the read-only envelope.")
            self._refuse_write_on_read_intent(validated_database)

        async def _attempt() -> list[QueryResult]:
            connection = await self.pool.acquire(validated_database)
            succeeded = False
            keep_after_error = False
            deferred_cleanup = False
            execution_task: asyncio.Task[list[QueryResult]] | None = None
            cancel_event = threading.Event()
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
                        cancel_event=cancel_event,
                        lock_timeout_in_batch=is_read and not read_only_envelope,
                        sql_origin=sql_origin,
                        read_only_envelope=read_only_envelope,
                    )
                )
                result = await asyncio.shield(execution_task)
                succeeded = True
                return result
            except asyncio.CancelledError:
                if execution_task is not None:
                    cancel_event.set()
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
            except Exception as exc:
                # A failed batch in the envelope may leave its transaction open.
                keep_after_error = (
                    is_read
                    and not read_only_envelope
                    and self._connection_survives_error(connection, query, exc)
                )
                raise
            finally:
                if succeeded and not is_admin_batch:
                    await self.pool.release(validated_database, connection)
                elif keep_after_error:
                    # A new login costs far more than the SELECT 1 that proves
                    # this connection on its next acquire.
                    await self.pool.release(
                        validated_database,
                        connection,
                        validate_before_reuse=True,
                    )
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
        sql_origin: str | None = None,
        read_only_envelope: bool = False,
    ) -> list[list[QueryResult]]:
        """Execute several statements on the SAME pooled connection.

        Required for session-scoped SET options (e.g. SET SHOWPLAN_XML ON)
        which must be in their own batch but must persist across the
        subsequent statements run on the same session.
        ``read_only_envelope`` wraps the whole sequence as in execute_batches.
        """
        validated_database = self.config.validate_database_name(database_name)
        sql_origin = _validated_origin(sql_origin)
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
            cancel_event = threading.Event()
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
                        cancel_event=cancel_event,
                        sql_origin=sql_origin,
                        read_only_envelope=read_only_envelope,
                    )
                )
                result = await asyncio.shield(execution_task)
                succeeded = True
                return result
            except asyncio.CancelledError:
                if execution_task is not None:
                    cancel_event.set()
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
        statement_timeout_seconds: int | None = None,
        sql_origin: str | None = None,
    ) -> list[list[QueryResult]]:
        """Execute one same-connection sequence without automatic retry.

        This is the bounded comparison path: retrying the sequence after one
        statement ran would under-report executions and invalidate the budget.
        ``before_statement_dispatch`` runs synchronously in the worker thread
        immediately before the selected statement's driver dispatch. If it
        raises, that statement is not dispatched.
        ``statement_timeout_seconds`` replaces the configured query timeout
        for each statement; the tool's statement deadline still bounds it.
        """

        validated_database = self.config.validate_database_name(database_name)
        sql_origin = _validated_origin(sql_origin)
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
        cancel_event = threading.Event()
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
                    cancel_event=cancel_event,
                    statement_timeout_seconds=statement_timeout_seconds,
                    sql_origin=sql_origin,
                )
            )
            result = await asyncio.shield(execution_task)
            succeeded = True
            return result
        except asyncio.CancelledError:
            if execution_task is not None:
                cancel_event.set()
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

    async def execute_transaction_exactly_once(
        self,
        database_name: str,
        callback: Callable[[SqlTransactionSession], TransactionResultT],
    ) -> TransactionResultT:
        """Run one synchronous callback in one isolated, never-retried transaction.

        The connection is always discarded because transaction/session state is
        deliberately not returned to the general pool.  Commit exceptions are
        treated as unknown after dispatch; callers must reconcile by reading,
        never by replaying the transaction.
        """

        validated_database = self.config.validate_database_name(database_name)
        self._refuse_write_on_read_intent(validated_database)
        connection = await self.pool.acquire(validated_database)
        deferred_cleanup = False
        execution_task: asyncio.Task[TransactionResultT] | None = None
        cancel_event = threading.Event()
        try:
            execution_task = asyncio.create_task(
                asyncio.to_thread(
                    self._execute_transaction_with_connection,
                    connection,
                    validated_database,
                    callback,
                    cancel_event,
                )
            )
            result = await asyncio.shield(execution_task)
            return result
        except asyncio.CancelledError:
            if execution_task is not None:
                cancel_event.set()
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
            if not deferred_cleanup:
                # This is intentionally discard-on-success as well as on
                # failure: a caller callback may have changed session state.
                await self.pool.discard(validated_database, connection)

    def _execute_transaction_with_connection(
        self,
        connection: Any,
        database_name: str,
        callback: Callable[[SqlTransactionSession], TransactionResultT],
        cancel_event: threading.Event | None = None,
    ) -> TransactionResultT:
        del database_name
        original_autocommit = getattr(connection, "autocommit", None)
        try:
            if original_autocommit is not None:
                connection.autocommit = False
            setup_cursor = self._open_cursor(connection, cancel_event)
            try:
                setup_cursor.execute("SET XACT_ABORT ON")
                setup_cursor.execute(
                    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
                )
                # No BEGIN TRANSACTION: with autocommit off the driver already
                # opens one. A second level makes commit() end only the inner
                # one, and the discarded connection then rolls everything back.
            finally:
                with contextlib.suppress(Exception):
                    setup_cursor.close()

            session = SqlTransactionSession(connection, self, cancel_event)
            result = callback(session)
            if inspect.isawaitable(result):
                raise TypeError(
                    "execute_transaction_exactly_once requires a synchronous callback."
                )
            # COMMIT is a statement too. The caller of a cancelled or timed-out
            # call has stopped waiting, so the work rolls back.
            self._raise_if_cancelled(cancel_event)
            self._raise_if_deadline_passed()
        except BaseException:
            try:
                connection.rollback()
            except BaseException as rollback_error:
                raise TransactionRollbackError(
                    "The transaction failed and rollback was not confirmed."
                ) from rollback_error
            raise

        try:
            # Keep commit outside the pre-commit handler.  Once dispatched, a
            # driver exception cannot prove whether SQL Server committed.
            connection.commit()
        except BaseException as commit_error:
            raise TransactionCommitOutcomeUnknownError(
                "Commit outcome is unknown; perform one fresh read reconciliation."
            ) from commit_error
        return result

    async def execute_non_query(
        self,
        database_name: str,
        query: str,
        params: Sequence[Any] | None = None,
    ) -> int:
        validated_database = self.config.validate_database_name(database_name)
        self._refuse_write_on_read_intent(validated_database)

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
        statement_timeout_seconds: int | None = None,
    ) -> ProfiledExecution:
        """Execute a measured user query exactly once on one connection.

        Setup/teardown SET statements are not user-query executions. Measured
        calls deliberately do not use automatic retry because a retry would make
        the requested sample execute more than once.
        ``statement_timeout_seconds`` replaces the configured query timeout
        for the measured statement; the tool's statement deadline still bounds it.
        """

        validated_database = self.config.validate_database_name(database_name)
        connection = await self.pool.acquire(validated_database)
        execution_kwargs: dict[str, Any] = (
            {"input_sizes": tuple(input_sizes)}
            if input_sizes is not None
            else {}
        )
        if statement_timeout_seconds is not None:
            execution_kwargs["statement_timeout_seconds"] = statement_timeout_seconds
        succeeded = False
        deferred_cleanup = False
        execution_task: asyncio.Task[ProfiledExecution] | None = None
        cancel_event = threading.Event()
        try:
            execution_task = asyncio.create_task(
                asyncio.to_thread(
                    self._execute_profiled_with_connection,
                    connection,
                    validated_database,
                    query,
                    tuple(params or ()),
                    max_rows,
                    cancel_event=cancel_event,
                    **execution_kwargs,
                )
            )
            result = await asyncio.shield(execution_task)
            succeeded = True
            return result
        except asyncio.CancelledError:
            if execution_task is not None:
                cancel_event.set()
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

    def _refuse_write_on_read_intent(self, database_name: str) -> None:
        """Raise before a write is sent to a database connected with read intent.

        Covers the write lanes: non-query, transaction and admin batch. Any
        other statement that writes is refused by the replica itself.
        """

        intent_of = getattr(self.pool, "application_intent", None)
        if callable(intent_of) and intent_of(database_name) == "read_only":
            raise ReadOnlyIntentError(
                f"Database '{database_name}' is connected with read intent "
                "(application_intent=read_only in the database policy). "
                "The write was not sent."
            )

    async def _discard_connection_when_task_finishes(
        self,
        database_name: str,
        connection,
        execution_task: asyncio.Task[Any],
    ) -> None:
        # If a tool call is cancelled (for example by an outer timeout), keep the
        # in-flight DB operation attached to this connection until the worker
        # thread finishes, then discard the connection so it is never reused.
        with contextlib.suppress(BaseException):
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
        cancel_event: threading.Event | None = None,
        lock_timeout_in_batch: bool = False,
        sql_origin: str | None = None,
        read_only_envelope: bool = False,
    ) -> list[QueryResult]:
        logger.debug(
            "Executing query",
            extra={
                "database_name": database_name,
                "server": self.config.server,
            },
        )
        if read_only_envelope:
            self._begin_read_only_envelope(connection, cancel_event)
            cursor = self._open_cursor(connection, cancel_event)
            try:
                with _statement_origin(sql_origin):
                    cursor.execute(query, params)
                    # Rows past the cap are dropped when the cursor closes.
                    results = self._consume_batches(cursor, max_rows=max_rows)
            finally:
                with contextlib.suppress(Exception):
                    cursor.close()
            self._end_read_only_envelope(connection, database_name)
            return results
        cursor = self._open_cursor(connection, cancel_event)
        try:
            if (
                lock_timeout_in_batch
                and _PLAIN_READ_START.match(query)
                # The driver describes a NULL parameter from the batch text;
                # that path is not proven with a prefixed batch.
                and not any(value is None for value in params)
            ):
                # One round trip, not two. SET returns no result set, so the
                # first result set is still the query's.
                query = f"{self._lock_timeout_statement()}; {query}"
            else:
                self._set_lock_timeout(cursor)
            try:
                with _statement_origin(sql_origin):
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
        cancel_event: threading.Event | None = None,
        statement_timeout_seconds: int | None = None,
        sql_origin: str | None = None,
        read_only_envelope: bool = False,
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
            cursor = self._open_cursor(
                connection,
                cancel_event,
                statement_timeout_seconds=statement_timeout_seconds,
            )
            try:
                if index == 0:
                    if read_only_envelope:
                        self._begin_read_only_envelope(connection, cancel_event)
                    else:
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
                self._raise_if_cancelled(cancel_event)
                with _statement_origin(sql_origin):
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
        if read_only_envelope and statements:
            self._end_read_only_envelope(connection, database_name)
        self._restore_default_isolation_level(connection, statements)
        return per_statement_results

    def _begin_read_only_envelope(
        self, connection, cancel_event: threading.Event | None
    ) -> None:
        cursor = self._open_cursor(connection, cancel_event)
        try:
            with _statement_origin("server_built"):
                cursor.execute(f"{self._lock_timeout_statement()}; BEGIN TRANSACTION", ())
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    @staticmethod
    def _end_read_only_envelope(connection, database_name: str) -> None:
        """Roll the envelope back and check that the batch left it open.

        Any other transaction count means the batch committed, rolled back or
        nested the transaction: what it wrote may be durable.
        """

        # Not _open_cursor: the rollback must run after a cancel or a passed deadline.
        cursor = connection.cursor()
        try:
            with _statement_origin("server_built"):
                cursor.execute(_ENVELOPE_END, ())
                row = cursor.fetchone()
        finally:
            with contextlib.suppress(Exception):
                cursor.close()
        trancount = row[0] if row else None
        if trancount != 1:
            logger.error(
                "Read-only envelope violated",
                extra={"database_name": database_name},
            )
            raise ReadOnlyEnvelopeViolation(
                "The batch ended or committed the read-only transaction; its "
                "changes may be durable. The connection was discarded."
            )

    @staticmethod
    def _restore_default_isolation_level(connection, statements: Sequence[str]) -> None:
        """Put READ COMMITTED back when the session left another level set.

        Runs only after every statement succeeded; a failed session's
        connection is discarded. A failure here fails the call, so the
        connection is discarded and never reused at the wrong level.
        """

        levels = [
            " ".join(match.group(1).upper().split())
            for statement in statements
            for match in _ISOLATION_LEVEL_SET.finditer(statement)
        ]
        if not levels or levels[-1] == _DEFAULT_ISOLATION_LEVEL:
            return
        # Not _open_cursor: the reset must run even after the deadline passed.
        cursor = connection.cursor()
        try:
            with _statement_origin("server_built"):
                cursor.execute(
                    f"SET TRANSACTION ISOLATION LEVEL {_DEFAULT_ISOLATION_LEVEL}", ()
                )
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

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
        cursor = self._open_cursor(connection)
        try:
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
        cancel_event: threading.Event | None = None,
        statement_timeout_seconds: int | None = None,
    ) -> ProfiledExecution:
        logger.debug(
            "Executing one profiled query sample",
            extra={"database_name": database_name, "server": self.config.server},
        )

        setup_cursor = self._open_cursor(connection, cancel_event)
        try:
            # One batch. STATISTICS XML goes on last (and off first below) so
            # that no setup or teardown statement runs with it on.
            with _statement_origin("server_built"):
                setup_cursor.execute(
                    f"{self._lock_timeout_statement()}; "
                    "SET STATISTICS IO ON; SET STATISTICS XML ON;",
                    (),
                )
        finally:
            with contextlib.suppress(Exception):
                setup_cursor.close()

        query_cursor = self._open_cursor(
            connection,
            cancel_event,
            statement_timeout_seconds=statement_timeout_seconds,
        )
        try:
            statistics_io_messages: list[Any] = []
            started = time.perf_counter()
            # The measured statement is the caller's query, never server text.
            with _statement_origin("caller_supplied"):
                self._execute_statement(
                    query_cursor,
                    query,
                    params,
                    input_sizes=input_sizes,
                )
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
            # Not _open_cursor: the session options must go off even after a
            # cancel or a passed deadline.
            teardown_cursor = connection.cursor()
            try:
                teardown_cursor.execute(
                    "SET STATISTICS XML OFF; SET STATISTICS IO OFF;", ()
                )
            except Exception:
                if not active_error:
                    raise
            finally:
                with contextlib.suppress(Exception):
                    teardown_cursor.close()

    def _open_cursor(
        self,
        connection,
        cancel_event: threading.Event | None = None,
        *,
        statement_timeout_seconds: int | None = None,
    ):
        """Open a cursor whose timeout fits the time left for this call.

        mssql-python has no SQLCancel, and closing a cursor that runs on
        another thread is not safe. So a cancelled call cannot stop the
        statement in flight. Instead, no later statement is dispatched, and
        each statement gets min(statement timeout, time left + about 1 s) so
        the driver itself sends the attention soon after the budget runs out.
        ``statement_timeout_seconds`` replaces the configured query timeout
        for this cursor; the time left still bounds it.
        """

        self._raise_if_cancelled(cancel_event)
        timeout = statement_timeout_seconds or self.config.query_timeout_seconds
        deadline = _STATEMENT_DEADLINE.get()
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise StatementDispatchStopped(
                    "The statement deadline has passed; the statement was not sent."
                )
            # Fire just after the deadline, not before it, so the caller's
            # wait_for still reports a timeout and not a driver error.
            timeout = max(1, min(timeout, math.ceil(remaining) + _DEADLINE_GRACE_SECONDS))
        # mssql-python copies connection.timeout into each new cursor; a
        # cursor has no timeout of its own to set.
        original_timeout = getattr(connection, "timeout", None)
        if original_timeout is None or original_timeout == timeout:
            return connection.cursor()
        connection.timeout = timeout
        try:
            return connection.cursor()
        finally:
            connection.timeout = original_timeout

    @staticmethod
    def _raise_if_cancelled(cancel_event: threading.Event | None) -> None:
        if cancel_event is not None and cancel_event.is_set():
            raise StatementDispatchStopped(
                "The call was cancelled; no further statements were sent."
            )

    @staticmethod
    def _raise_if_deadline_passed() -> None:
        deadline = _STATEMENT_DEADLINE.get()
        if deadline is not None and deadline <= time.monotonic():
            raise StatementDispatchStopped(
                "The statement deadline has passed; the statement was not sent."
            )

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

    def _lock_timeout_statement(self) -> str:
        lock_timeout_ms = max(1, int(self.config.query_timeout_seconds)) * 1000
        return f"SET LOCK_TIMEOUT {lock_timeout_ms}"

    def _set_lock_timeout(self, cursor) -> None:
        with _statement_origin("server_built"):
            cursor.execute(self._lock_timeout_statement(), ())

    @staticmethod
    def _connection_survives_error(connection, query: str, exc: Exception) -> bool:
        """True when a failed plain read left an open autocommit connection.

        In autocommit mode a failed SELECT leaves no transaction open. Any
        doubt (another batch shape, a closed or manual-commit connection, an
        error that is not a known statement error) means discard.
        """

        if _PLAIN_READ_START.match(query) is None:
            return False
        try:
            usable = connection.closed is False and bool(connection.autocommit)
        except Exception:
            return False
        return usable and _is_reusable_statement_error(exc)

    def _request_cancel(self, connection) -> bool:
        """Ask the driver to cancel the statement in flight; False when it cannot."""

        for method_name in _DRIVER_CANCEL_METHODS:
            method = getattr(connection, method_name, None)
            if callable(method):
                with contextlib.suppress(Exception):
                    method()
                return True
        if not self._cancel_unsupported_logged:
            self._cancel_unsupported_logged = True
            logger.warning(
                "The database driver has no cancel call. A cancelled statement "
                "runs until its own timeout; later statements are not sent."
            )
        return False

    def cancellation_report(self) -> dict[str, Any]:
        """What a tool timeout or a client cancel leaves running on the server."""

        supported = driver_cancel_supported()
        return {
            "driver_cancel_supported": supported,
            "later_statements_sent_after_cancel": False,
            "cancelled_transaction_commits": False,
            "in_flight_statement_stopped_by": (
                "driver_cancel" if supported else "statement_timeout"
            ),
            # ceil(time left) + grace: under two seconds past the tool deadline.
            "max_in_flight_seconds_after_tool_timeout": (
                0 if supported else 1 + _DEADLINE_GRACE_SECONDS
            ),
            "max_in_flight_seconds_after_client_cancel": (
                0 if supported else self.config.query_timeout_seconds
            ),
            "not_stopped": [
                "one multi-statement batch sent as a single text runs to its end "
                "or to its statement timeout",
            ],
        }

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
