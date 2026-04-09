from __future__ import annotations

import pytest

from azure_sql_mcp.wait_stats import (
    BENIGN_WAITS,
    CATEGORY_RECOMMENDATIONS,
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
