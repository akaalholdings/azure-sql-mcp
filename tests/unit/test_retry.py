from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from azure_sql_mcp.retry import _is_transient
from azure_sql_mcp.retry import with_retry


@pytest.mark.asyncio
async def test_with_retry_retries_transient_errors_until_success() -> None:
    transient_error = Exception("Azure SQL transient error 40501")
    func = AsyncMock(side_effect=[transient_error, "ok"])

    with (
        patch("azure_sql_mcp.retry.asyncio.sleep", new=AsyncMock()) as sleep_mock,
        patch("azure_sql_mcp.retry.random.uniform", return_value=0.0),
    ):
        result = await with_retry(func, max_retries=2)

    assert result == "ok"
    assert func.await_count == 2
    sleep_mock.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_with_retry_does_not_retry_non_transient_errors() -> None:
    func = AsyncMock(side_effect=ValueError("boom"))

    with patch("azure_sql_mcp.retry.asyncio.sleep", new=AsyncMock()) as sleep_mock:
        with pytest.raises(ValueError, match="boom"):
            await with_retry(func, max_retries=3)

    assert func.await_count == 1
    sleep_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_with_retry_respects_zero_retries() -> None:
    transient_error = Exception("40501 busy")
    func = AsyncMock(side_effect=transient_error)

    with patch("azure_sql_mcp.retry.asyncio.sleep", new=AsyncMock()) as sleep_mock:
        with pytest.raises(Exception, match="40501"):
            await with_retry(func, max_retries=0)

    assert func.await_count == 1
    sleep_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_with_retry_applies_exponential_backoff_and_jitter() -> None:
    func = AsyncMock(
        side_effect=[
            Exception("40501 first"),
            Exception("40613 second"),
            "ok",
        ]
    )

    with (
        patch("azure_sql_mcp.retry.asyncio.sleep", new=AsyncMock()) as sleep_mock,
        patch("azure_sql_mcp.retry.random.uniform", side_effect=[0.25, 0.5]),
    ):
        result = await with_retry(
            func,
            max_retries=3,
            base_delay=1.0,
            max_delay=2.0,
            jitter_factor=0.5,
        )

    assert result == "ok"
    assert [call.args[0] for call in sleep_mock.await_args_list] == [1.25, 2.5]


@pytest.mark.asyncio
async def test_with_retry_detects_transient_codes_in_exception_chain() -> None:
    root_cause = RuntimeError("40197 service error")
    error = RuntimeError("wrapper")
    error.__cause__ = root_cause
    func = AsyncMock(side_effect=[error, "ok"])

    with (
        patch("azure_sql_mcp.retry.asyncio.sleep", new=AsyncMock()) as sleep_mock,
        patch("azure_sql_mcp.retry.random.uniform", return_value=0.0),
    ):
        result = await with_retry(func, max_retries=1)

    assert result == "ok"
    sleep_mock.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_with_retry_rejects_negative_retry_counts() -> None:
    func = AsyncMock(return_value="ok")

    with pytest.raises(ValueError, match="max_retries must be >= 0"):
        await with_retry(func, max_retries=-1)


@pytest.mark.asyncio
async def test_with_retry_propagates_cancellation_without_retrying() -> None:
    func = AsyncMock(side_effect=asyncio.CancelledError())

    with patch("azure_sql_mcp.retry.asyncio.sleep", new=AsyncMock()) as sleep_mock:
        with pytest.raises(asyncio.CancelledError):
            await with_retry(func, max_retries=3)

    assert func.await_count == 1
    sleep_mock.assert_not_awaited()


def test_is_transient_handles_cycle_in_exception_chain() -> None:
    exc = RuntimeError("boom")
    exc.__context__ = exc

    assert _is_transient(exc) is False
