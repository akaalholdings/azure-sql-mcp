"""Absolute wait bars: wait time divided by observed time, next to the relative share.

A share of 70% of almost nothing is not a problem, and 8% of a lot can be. The
bar is wait seconds per observed second, from the calibrated list in the gap
review (2C item 10).
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.wait_stats import WaitStatsService
from azure_sql_mcp.wait_stats import absolute_wait_bar
from azure_sql_mcp.wait_stats import absolute_wait_level
from tests.azure_dmv_contract import StrictDmvExecutor


class SnapshotExecutor:
    """Returns the queued wait snapshots in order, and the counter epoch."""

    def __init__(self, snapshots: list[list[dict[str, Any]]], seconds_since_start: int | None = 86_400) -> None:
        self.snapshots = list(snapshots)
        self.seconds_since_start = seconds_since_start
        self.queries: list[str] = []

    async def fetch_all(self, database_name: str, query: str, params: Any = None, **_: Any) -> list[dict[str, Any]]:
        self.queries.append(query)
        if "sqlserver_start_time" in query:
            if self.seconds_since_start is None:
                raise RuntimeError("VIEW DATABASE STATE permission was denied")
            return [
                {
                    "engine_start_time_utc": "2026-11-09T12:00:00",
                    "seconds_since_engine_start": self.seconds_since_start,
                }
            ]
        return self.snapshots.pop(0)


def wait(wait_type: str, tasks: int, wait_ms: int, signal_ms: int = 0) -> dict[str, Any]:
    return {
        "wait_type": wait_type,
        "waiting_tasks_count": tasks,
        "wait_time_ms": wait_ms,
        "max_wait_time_ms": wait_ms,
        "signal_wait_time_ms": signal_ms,
        "resource_wait_time_ms": wait_ms - signal_ms,
    }


async def no_sleep(seconds: float) -> None:
    return None


@pytest.mark.parametrize(
    ("wait_type", "above_bar", "high"),
    [
        ("SOS_SCHEDULER_YIELD", 0.75, None),
        ("CXPACKET", 0.25, None),
        ("CXCONSUMER", 0.25, None),
        ("PAGEIOLATCH_SH", 0.25, None),
        ("RESOURCE_SEMAPHORE", 0.01, 0.10),
        ("RESOURCE_SEMAPHORE_QUERY_COMPILE", 0.01, 0.10),
        ("WRITELOG", 0.25, 0.50),
        ("LCK_M_X", 0.10, None),
        ("LCK_M_S", 0.05, None),
        ("LCK_M_IS", 0.05, None),
        ("LCK_M_RS_S", 0.01, 0.10),
        ("LCK_M_SCH_M", 0.01, 0.10),
        ("LATCH_EX", 0.25, None),
        ("PAGELATCH_UP", round(10 / 3600, 5), None),
        ("THREADPOOL", 0.25, None),
    ],
)
def test_calibrated_bars(wait_type: str, above_bar: float, high: float | None) -> None:
    bar = absolute_wait_bar(wait_type)

    assert bar is not None
    assert (bar.above_bar, bar.high) == (above_bar, high)


def test_waits_with_no_calibrated_bar_say_so() -> None:
    assert absolute_wait_bar("ASYNC_NETWORK_IO") is None
    assert absolute_wait_level("ASYNC_NETWORK_IO", 5.0, 100.0) == "no_bar"


def test_threadpool_needs_both_parts_of_its_bar() -> None:
    # 15 minutes of THREADPOOL per hour made of 50 ms waits is scheduling noise.
    # Only long waits that add up are worker exhaustion.
    assert absolute_wait_bar("THREADPOOL").min_avg_wait_ms == 1000  # type: ignore[union-attr]
    assert absolute_wait_level("THREADPOOL", 0.30, 50.0) == "below_bar"
    assert absolute_wait_level("THREADPOOL", 0.10, 4_000.0) == "below_bar"
    assert absolute_wait_level("THREADPOOL", 0.30, 1_500.0) == "above_bar"


def test_two_step_bars_have_a_high_level_and_unknown_time_is_not_a_pass() -> None:
    assert absolute_wait_level("WRITELOG", 0.20, 5.0) == "below_bar"
    assert absolute_wait_level("WRITELOG", 0.30, 5.0) == "above_bar"
    assert absolute_wait_level("WRITELOG", 0.60, 5.0) == "high"
    assert absolute_wait_level("WRITELOG", None, 5.0) == "unknown_observed_time"


@pytest.mark.asyncio
async def test_dominant_share_of_a_quiet_sample_is_below_its_bar() -> None:
    # PAGEIOLATCH is 100% of the waits, yet 200 ms of waiting in 15 s is an idle database.
    executor = SnapshotExecutor([[], [wait("PAGEIOLATCH_SH", 4, 200)]])

    result = await WaitStatsService(executor).get_wait_stats("appdb", sample_seconds=15, sleep=no_sleep)  # type: ignore[arg-type]

    (row,) = result["top_waits"]
    assert row["pct_of_total"] == 100.0
    assert row["wait_seconds_per_observed_second"] == round(0.2 / 15, 4)
    assert row["absolute_bar"] == 0.25
    assert row["absolute_level"] == "below_bar"
    assert result["observed_seconds"] == 15
    assert result["absolute_bars"]["unit"] == "wait_seconds_per_observed_second"


@pytest.mark.asyncio
async def test_small_share_of_a_busy_sample_is_above_its_bar() -> None:
    executor = SnapshotExecutor(
        [[], [wait("CXPACKET", 5_000, 400_000), wait("RESOURCE_SEMAPHORE", 10, 3_000)]]
    )

    result = await WaitStatsService(executor).get_wait_stats("appdb", sample_seconds=15, sleep=no_sleep)  # type: ignore[arg-type]

    by_type = {row["wait_type"]: row for row in result["top_waits"]}
    # Under 1% of the wait time, but 0.2 s of grant waiting per second is far over 0.10.
    assert by_type["RESOURCE_SEMAPHORE"]["pct_of_total"] < 1.0
    assert by_type["RESOURCE_SEMAPHORE"]["absolute_level"] == "high"
    assert by_type["RESOURCE_SEMAPHORE"]["avg_wait_ms"] == 300.0
    assert by_type["CXPACKET"]["absolute_level"] == "above_bar"
    memory = next(item for item in result["categories"] if item["category"] == "Memory")
    assert memory["wait_seconds_per_observed_second"] == 0.2


@pytest.mark.asyncio
async def test_cumulative_mode_divides_by_the_time_since_the_counters_started() -> None:
    executor = SnapshotExecutor([[wait("WRITELOG", 1_000_000, 30_240_000)]], seconds_since_start=86_400)

    result = await WaitStatsService(executor).get_wait_stats("appdb")  # type: ignore[arg-type]

    (row,) = result["top_waits"]
    assert result["observed_seconds"] == 86_400
    assert row["wait_seconds_per_observed_second"] == 0.35
    assert row["absolute_level"] == "above_bar"


@pytest.mark.asyncio
async def test_unknown_counter_epoch_leaves_the_absolute_level_unknown() -> None:
    executor = SnapshotExecutor([[wait("WRITELOG", 10, 9_000_000)]], seconds_since_start=None)

    result = await WaitStatsService(executor).get_wait_stats("appdb")  # type: ignore[arg-type]

    (row,) = result["top_waits"]
    assert result["observed_seconds"] is None
    assert row["wait_seconds_per_observed_second"] is None
    assert row["absolute_level"] == "unknown_observed_time"


@pytest.mark.asyncio
async def test_threadpool_advice_is_about_workers_not_a_scale_up() -> None:
    executor = SnapshotExecutor([[], [wait("THREADPOOL", 3, 6_000)]])

    result = await WaitStatsService(executor).get_wait_stats("appdb", sample_seconds=15, sleep=no_sleep)  # type: ignore[arg-type]

    (row,) = result["top_waits"]
    assert row["absolute_level"] == "above_bar"
    assert "worker" in row["recommendation"].lower()
    assert "MAXDOP" in row["recommendation"] and "blocking" in row["recommendation"]
    assert "scaling up" not in row["recommendation"]


@pytest.mark.asyncio
async def test_results_name_their_source() -> None:
    executor = SnapshotExecutor([[wait("WRITELOG", 1, 10)], []])
    service = WaitStatsService(executor)  # type: ignore[arg-type]

    assert (await service.get_wait_stats("appdb"))["source_dmv"] == "sys.dm_db_wait_stats"
    waiting = await service.get_currently_waiting_tasks("appdb")
    assert waiting["source_dmv"] == "sys.dm_os_waiting_tasks"
    assert waiting["collection_window"] == {"mode": "point_in_time"}


@pytest.mark.asyncio
async def test_waiting_tasks_filter_user_sessions_by_flag_not_by_session_id() -> None:
    # On Azure SQL Database user sessions can have ids of 50 or less, and system
    # sessions ids above 50. session_id > 50 drops real waiters.
    executor = StrictDmvExecutor()

    await WaitStatsService(executor).get_currently_waiting_tasks("appdb")  # type: ignore[arg-type]

    assert executor.violations == []
    query = executor.calls[0][1]
    assert "> 50" not in query
    assert "is_user_process = 1" in query


class QueryWaitExecutor:
    def __init__(self, options: Any) -> None:
        self.options = options
        self.queries: list[str] = []

    async def fetch_all(self, database_name: str, query: str, params: Any = None, **_: Any) -> list[dict[str, Any]]:
        self.queries.append(query)
        if "sys.database_query_store_options" in query:
            if isinstance(self.options, BaseException):
                raise self.options
            return self.options
        return []


@pytest.mark.parametrize(
    ("options", "fragment"),
    [
        ([{"actual_state_desc": "READ_WRITE", "wait_stats_capture_mode_desc": "OFF"}], "WAIT_STATS_CAPTURE_MODE = ON"),
        ([{"actual_state_desc": "OFF", "wait_stats_capture_mode_desc": "ON"}], "QUERY_STORE = ON"),
    ],
)
@pytest.mark.asyncio
async def test_no_query_waits_because_capture_is_off_is_a_precondition(options: Any, fragment: str) -> None:
    # With wait capture off Query Store records no waits at all. "No waits in
    # the window" would be read as "nothing waited", the opposite of unknown.
    result = await WaitStatsService(QueryWaitExecutor(options)).get_query_wait_stats("appdb")  # type: ignore[arg-type]

    assert result["query_wait_stats"] == []
    assert result["result_status"] == "precondition"
    assert fragment in result["remediation"]


@pytest.mark.asyncio
async def test_no_query_waits_with_capture_on_stays_a_true_negative() -> None:
    options = [{"actual_state_desc": "READ_WRITE", "wait_stats_capture_mode_desc": "ON"}]

    result = await WaitStatsService(QueryWaitExecutor(options)).get_query_wait_stats("appdb")  # type: ignore[arg-type]

    assert "result_status" not in result  # the server stamps "empty" from the empty rows
    assert result["capture"] == {"query_store_state": "READ_WRITE", "wait_stats_capture_mode": "ON"}


@pytest.mark.asyncio
async def test_unreadable_capture_state_is_reported_not_assumed() -> None:
    result = await WaitStatsService(QueryWaitExecutor(RuntimeError("timeout"))).get_query_wait_stats("appdb")  # type: ignore[arg-type]

    assert "result_status" not in result
    assert result["capture"] == {"query_store_state": "unknown", "wait_stats_capture_mode": "unknown"}
