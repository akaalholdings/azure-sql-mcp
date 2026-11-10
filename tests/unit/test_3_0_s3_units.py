"""Plan cache figures carry their unit in the field name (review finding 79).

``avg_elapsed_time`` 250000 from sys.dm_exec_query_stats is 250 ms. Beside
``avg_duration_us`` or a ``*_ms`` field from another tool it reads as 250 seconds.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.diagnostics import DEPRECATED_TIME_FIELDS
from azure_sql_mcp.diagnostics import DiagnosticQueryService


class RowsExecutor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    async def fetch_all(self, database_name: str, query: str, params: Any = None, **_: Any) -> list[dict[str, Any]]:
        self.queries.append(query)
        return [dict(row) for row in self.rows]


STAT_ROW = {
    "execution_count": 4,
    "total_worker_time": 800_000,
    "avg_worker_time": 200_000,
    "total_elapsed_time": 1_000_000,
    "avg_elapsed_time": 250_000,
}


@pytest.mark.asyncio
async def test_cached_query_times_are_named_in_microseconds() -> None:
    result = await DiagnosticQueryService(RowsExecutor([STAT_ROW])).get_top_cached_queries("appdb")  # type: ignore[arg-type]

    (row,) = result["cached_queries"]["rows"]
    assert row["avg_elapsed_time_us"] == 250_000
    assert row["avg_worker_time_us"] == 200_000
    assert row["total_elapsed_time_us"] == 1_000_000
    assert row["total_worker_time_us"] == 800_000
    # The old names stay for one release so callers do not break.
    assert row["avg_elapsed_time"] == 250_000
    assert result["deprecated_fields"] == DEPRECATED_TIME_FIELDS
    assert result["deprecated_fields"]["avg_elapsed_time"] == "avg_elapsed_time_us"


@pytest.mark.asyncio
async def test_cached_routine_times_are_named_in_microseconds() -> None:
    result = await DiagnosticQueryService(RowsExecutor([STAT_ROW])).get_cached_routine_stats("appdb")  # type: ignore[arg-type]

    for section in ("procedures", "functions"):
        (row,) = result[section]["rows"]
        assert row["avg_worker_time_us"] == 200_000 and row["avg_worker_time"] == 200_000
    assert result["deprecated_fields"] == DEPRECATED_TIME_FIELDS


@pytest.mark.asyncio
async def test_calls_per_minute_is_not_integer_division() -> None:
    # 59 calls in 60 minutes is 0.98 per minute. bigint / int gave 0.
    executor = RowsExecutor([])

    await DiagnosticQueryService(executor).get_cached_routine_stats("appdb", routine_type="procedure")  # type: ignore[arg-type]

    query = " ".join(executor.queries[0].split())
    assert "qs.execution_count * 1.0 / NULLIF(DATEDIFF(MINUTE, qs.cached_time, GETDATE()), 0)" in query
    assert "ISNULL(qs.execution_count / NULLIF(" not in query


@pytest.mark.parametrize(
    ("sort_by", "sort_sql"),
    [
        ("avg_worker_time_us", "qs.total_worker_time / NULLIF(qs.execution_count, 0)"),
        ("total_elapsed_time_us", "qs.total_elapsed_time"),
        ("avg_elapsed_time", "qs.total_elapsed_time / NULLIF(qs.execution_count, 0)"),
    ],
)
@pytest.mark.asyncio
async def test_sort_keys_accept_the_old_and_the_unit_bearing_name(sort_by: str, sort_sql: str) -> None:
    executor = RowsExecutor([])

    await DiagnosticQueryService(executor).get_top_cached_queries("appdb", sort_by=sort_by)  # type: ignore[arg-type]

    assert f"ORDER BY {sort_sql} DESC" in " ".join(executor.queries[0].split())


@pytest.mark.asyncio
async def test_input_buffer_cpu_is_named_in_milliseconds_and_low_session_ids_are_kept() -> None:
    executor = RowsExecutor([{"session_id": 12, "cpu_time": 1_500}])

    result = await DiagnosticQueryService(executor).get_connection_diagnostics(  # type: ignore[arg-type]
        "appdb", include_input_buffer=True
    )

    (row,) = result["input_buffers"]["rows"]
    assert row["cpu_time_ms"] == 1_500 and row["cpu_time"] == 1_500
    # On Azure SQL Database a user session can have an id of 50 or less.
    assert "session_id > 50" not in executor.queries[-1]
    assert "es.is_user_process = 1" in executor.queries[-1]
