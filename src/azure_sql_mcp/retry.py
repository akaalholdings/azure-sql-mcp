from __future__ import annotations

import asyncio
import logging
import random
from typing import cast
from typing import Any, Awaitable, Callable, TypeVar

logger = logging.getLogger(__name__)

# Azure SQL transient error codes that are safe to retry.
# References:
#   https://learn.microsoft.com/en-us/azure/azure-sql/database/troubleshoot-common-errors-issues
#   https://learn.microsoft.com/en-us/azure/azure-sql/database/troubleshoot-common-connectivity-issues
TRANSIENT_ERROR_CODES = frozenset(
    {
        233,     # Connection closed during login
        1205,    # Deadlock victim — safe to retry the transaction
        4221,    # Login to read-secondary failed
        10053,   # Transport-level error (connection reset)
        10054,   # Transport-level error (connection forcibly closed)
        10928,   # Resource ID limit reached
        10929,   # Resource ID limit reached (minimum guarantee)
        40197,   # Service error processing request
        40501,   # Service is busy
        40549,   # Session terminated because of long-running transaction
        40554,   # Session terminated due to excessive log usage
        40613,   # Database not currently available
        49918,   # Cannot process request - not enough resources
        49919,   # Cannot process create/update request
        49920,   # Cannot process request due to too many operations
    }
)

T = TypeVar("T")


def _is_transient(exc: Exception) -> bool:
    """Check whether a database-driver exception carries a transient error code."""
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        args = getattr(current, "args", ())
        for arg in args:
            if isinstance(arg, str):
                for code in TRANSIENT_ERROR_CODES:
                    if str(code) in arg:
                        return True
            elif isinstance(arg, int) and arg in TRANSIENT_ERROR_CODES:
                return True
            elif isinstance(arg, tuple):
                for nested in arg:
                    if isinstance(nested, int) and nested in TRANSIENT_ERROR_CODES:
                        return True
                    if isinstance(nested, str):
                        for code in TRANSIENT_ERROR_CODES:
                            if str(code) in nested:
                                return True
        current = current.__cause__ or current.__context__
    return False


async def with_retry(
    func: Callable[[], Awaitable[T]],
    max_retries: int,
    base_delay: float = 1.0,
    max_delay: float = 8.0,
    jitter_factor: float = 0.5,
) -> T:
    """Execute *func* with exponential-backoff retry on transient Azure SQL errors.

    Parameters
    ----------
    func:
        An async callable (zero-arg) to invoke.
    max_retries:
        Maximum number of retry attempts.  ``0`` means no retries.
    base_delay:
        Initial backoff in seconds before the first retry.
    max_delay:
        Upper cap for the computed backoff delay.
    jitter_factor:
        Random jitter range as a fraction of the computed delay.
        A value of 0.5 means the actual sleep will be
        ``delay +/- delay * 0.5 * random()``.
    """
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0")

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return await func()
        except asyncio.CancelledError:
            # Never retry cancelled operations; propagate cancellation immediately.
            raise
        except Exception as exc:
            last_exc = exc
            if attempt >= max_retries or not _is_transient(exc):
                raise
            delay = min(base_delay * (2 ** attempt), max_delay)
            jitter_ceiling = max(0.0, delay * jitter_factor)
            sleep_time = delay + random.uniform(0.0, jitter_ceiling)
            logger.warning(
                "Transient error on attempt %d/%d, retrying in %.2fs",
                attempt + 1,
                1 + max_retries,
                sleep_time,
                extra={"error": str(exc), "attempt": attempt + 1},
            )
            await asyncio.sleep(sleep_time)

    # Should not be reachable, but satisfy type-checkers.
    assert last_exc is not None  # noqa: S101
    raise cast(Exception, last_exc)
