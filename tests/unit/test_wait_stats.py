from __future__ import annotations

import pytest

from azure_sql_mcp.wait_stats import (
    BENIGN_WAITS,
    WaitStatsService,
    classify_wait,
)


class FakeExecutor:
    def __init__(self, results: list[dict] | None = None):
        self.results = results or []

    async def fetch_all(self, database_name: str, query: str) -> list[dict]:
        return self.results


class TestClassifyWait:
    def test_known_wait(self):
        assert classify_wait("LCK_M_X") == "Lock"

    def test_prefix_lock(self):
        assert classify_wait("LCK_M_SOMETHING_NEW") == "Lock"

    def test_prefix_pageiolatch(self):
        assert classify_wait("PAGEIOLATCH_UP") == "I/O"

    def test_prefix_pagelatch(self):
        assert classify_wait("PAGELATCH_EX") == "Latch"

    def test_prefix_latch(self):
        assert classify_wait("LATCH_EX") == "Latch"

    def test_prefix_preemptive(self):
        assert classify_wait("PREEMPTIVE_OS_SOMETHING") == "Preemptive"

    def test_unknown(self):
        assert classify_wait("TOTALLY_UNKNOWN_WAIT") == "Other"

    def test_cpu_wait(self):
        assert classify_wait("SOS_SCHEDULER_YIELD") == "CPU"

    def test_memory_wait(self):
        assert classify_wait("RESOURCE_SEMAPHORE") == "Memory"

    def test_network_wait(self):
        assert classify_wait("ASYNC_NETWORK_IO") == "Network"

    def test_log_io(self):
        assert classify_wait("WRITELOG") == "Log I/O"

    def test_parallelism(self):
        assert classify_wait("CXPACKET") == "Parallelism"


class TestBenignWaits:
    def test_sleep_task_is_benign(self):
        assert "SLEEP_TASK" in BENIGN_WAITS

    def test_lck_m_x_is_not_benign(self):
        assert "LCK_M_X" not in BENIGN_WAITS


@pytest.mark.asyncio
async def test_get_wait_stats_filters_benign_and_categorizes():
    rows = [
        {"wait_type": "LCK_M_X", "waiting_tasks_count": 100, "wait_time_ms": 5000, "max_wait_time_ms": 200, "signal_wait_time_ms": 100},
        {"wait_type": "SLEEP_TASK", "waiting_tasks_count": 9999, "wait_time_ms": 999999, "max_wait_time_ms": 1, "signal_wait_time_ms": 0},
        {"wait_type": "SOS_SCHEDULER_YIELD", "waiting_tasks_count": 50, "wait_time_ms": 3000, "max_wait_time_ms": 10, "signal_wait_time_ms": 2500},
    ]
    service = WaitStatsService(FakeExecutor(rows))
    result = await service.get_wait_stats("testdb", top_n=10)

    assert result["database_name"] == "testdb"
    # SLEEP_TASK should be filtered out
    wait_types = [w["wait_type"] for w in result["top_waits"]]
    assert "SLEEP_TASK" not in wait_types
    assert "LCK_M_X" in wait_types
    assert "SOS_SCHEDULER_YIELD" in wait_types

    # Check categories assigned
    for w in result["top_waits"]:
        if w["wait_type"] == "LCK_M_X":
            assert w["category"] == "Lock"
        if w["wait_type"] == "SOS_SCHEDULER_YIELD":
            assert w["category"] == "CPU"

    # Check category aggregation
    cats = {c["category"] for c in result["categories"]}
    assert "Lock" in cats
    assert "CPU" in cats

    # Percentages should add to 100
    total_pct = sum(w["pct_of_total"] for w in result["top_waits"])
    assert abs(total_pct - 100.0) < 0.1


@pytest.mark.asyncio
async def test_get_wait_stats_empty():
    service = WaitStatsService(FakeExecutor([]))
    result = await service.get_wait_stats("testdb")
    assert result["top_waits"] == []
    assert result["categories"] == []


@pytest.mark.asyncio
async def test_get_query_wait_stats():
    rows = [
        {"query_id": 1, "query_sql_text": "SELECT 1", "wait_category_desc": "CPU", "total_wait_time_ms": 100, "weighted_avg_wait_ms": 10, "max_wait_time_ms": 50},
    ]
    service = WaitStatsService(FakeExecutor(rows))
    result = await service.get_query_wait_stats("testdb", 60, 10)
    assert result["window_minutes"] == 60
    assert len(result["query_wait_stats"]) == 1


@pytest.mark.asyncio
async def test_get_currently_waiting_tasks():
    rows = [
        {
            "session_id": 55,
            "wait_type": "LCK_M_X",
            "wait_duration_ms": 3000,
            "blocking_session_id": 54,
            "resource_description": "KEY: 5:72057594044153856 (hash)",
            "command": "SELECT",
            "request_status": "suspended",
            "cpu_time_ms": 100,
            "elapsed_time_ms": 5000,
            "current_statement": "SELECT * FROM Orders",
        },
    ]
    service = WaitStatsService(FakeExecutor(rows))
    result = await service.get_currently_waiting_tasks("testdb")
    assert result["currently_waiting_count"] == 1
    assert result["waiting_tasks"][0]["category"] == "Lock"
    assert result["waiting_tasks"][0]["recommendation"] != ""


class SequencedExecutor:
    """Return one prepared result per wait snapshot, plus the counter epoch."""

    def __init__(self, snapshots: list[list[dict]], epoch: str | None = "2026-09-01T00:00:00") -> None:
        self.snapshots = list(snapshots)
        self.epoch = epoch
        self.queries: list[str] = []

    async def fetch_all(self, database_name: str, query: str, *args, **kwargs) -> list[dict]:
        self.queries.append(query)
        if "dm_os_sys_info" in query:
            return [{"engine_start_time_utc": self.epoch}] if self.epoch else []
        return self.snapshots.pop(0)


def _wait(wait_type: str, tasks: int, wait_ms: int, signal_ms: int = 0) -> dict:
    return {
        "wait_type": wait_type,
        "waiting_tasks_count": tasks,
        "wait_time_ms": wait_ms,
        "max_wait_time_ms": wait_ms,
        "signal_wait_time_ms": signal_ms,
        "resource_wait_time_ms": wait_ms - signal_ms,
    }


@pytest.mark.asyncio
async def test_interval_mode_reports_only_waits_that_accrued_during_the_sample():
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    executor = SequencedExecutor(
        [
            [_wait("PAGEIOLATCH_SH", 1000, 900_000, 1_000), _wait("WRITELOG", 50, 2_000, 100)],
            [_wait("PAGEIOLATCH_SH", 1000, 900_000, 1_000), _wait("WRITELOG", 80, 9_000, 400), _wait("LCK_M_X", 3, 6_000, 0)],
        ]
    )
    service = WaitStatsService(executor)

    result = await service.get_wait_stats("testdb", sample_seconds=10, sleep=fake_sleep)

    assert sleeps == [10]
    # The huge lifetime PAGEIOLATCH total did not move, so it is absent.
    assert [w["wait_type"] for w in result["top_waits"]] == ["WRITELOG", "LCK_M_X"]
    assert result["top_waits"][0]["wait_time_ms"] == 7_000
    assert result["top_waits"][0]["signal_wait_time_ms"] == 300
    assert result["window"] == {"mode": "interval", "sample_seconds": 10, "counter_reset_detected": False}


@pytest.mark.asyncio
async def test_interval_mode_detects_a_counter_reset_between_snapshots():
    async def fake_sleep(seconds: float) -> None:
        return None

    executor = SequencedExecutor(
        [[_wait("WRITELOG", 500, 90_000)], [_wait("WRITELOG", 20, 1_000)]]
    )

    result = await WaitStatsService(executor).get_wait_stats("testdb", sample_seconds=5, sleep=fake_sleep)

    assert result["window"]["counter_reset_detected"] is True
    assert result["top_waits"][0]["wait_time_ms"] == 1_000


@pytest.mark.asyncio
async def test_cumulative_mode_discloses_the_counter_epoch():
    executor = SequencedExecutor([[_wait("WRITELOG", 50, 2_000)]])

    result = await WaitStatsService(executor).get_wait_stats("testdb")

    assert result["window"]["mode"] == "cumulative"
    assert result["window"]["since_utc"] == "2026-09-01T00:00:00"
    assert "sample_seconds" in result["window"]["note"]


@pytest.mark.asyncio
async def test_sample_length_is_capped():
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    executor = SequencedExecutor([[], []])

    await WaitStatsService(executor).get_wait_stats("testdb", sample_seconds=999, sleep=fake_sleep)

    assert slept == [30]
