from __future__ import annotations

import asyncio
import logging
import random
from typing import cast
from typing import Awaitable, Callable, TypeVar

from .observability import is_transient_error
from .observability import sanitize_error_message

logger = logging.getLogger(__name__)

# Azure SQL transient error codes that are safe to retry.
# Azure SQL Database can return transient throttling, failover, connection,
# transport, deadlock, and service-busy errors.  Only the explicitly classified
# retry-safe operations below may be retried, using bounded exponential backoff
# with jitter.  Profiled samples and mutations are deliberately excluded because
# replay would corrupt measurements or repeat a side effect.
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


def _is_transient(exc: BaseException) -> bool:
    """True when the driver reported a transient Azure SQL error number.

    Only a number in a position where the driver reports the engine error
    counts. A number in the message text (a row value, an object name, a row
    count) does not: that error is deterministic and a retry runs the same
    statement again for the same result. A query timeout reports no transient
    number, so it is never retried here.
    """

    return is_transient_error(exc, TRANSIENT_ERROR_CODES)


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
                extra={"error": sanitize_error_message(str(exc)), "attempt": attempt + 1},
            )
            await asyncio.sleep(sleep_time)

    # Should not be reachable, but satisfy type-checkers.
    assert last_exc is not None  # noqa: S101
    raise cast(Exception, last_exc)
