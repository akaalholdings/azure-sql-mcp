from __future__ import annotations

import pytest

from azure_sql_mcp.tempdb_memory import TempdbMemoryService


class FakeExecutor:
    def __init__(self, results_by_call: list[list[dict]] | None = None):
        self._calls: list[list[dict]] = results_by_call or [[]]
        self._call_idx = 0

    async def fetch_all(self, database_name: str, query: str, params=None) -> list[dict]:
        if self._call_idx < len(self._calls):
            result = self._calls[self._call_idx]
            self._call_idx += 1
            return result
        return []


@pytest.mark.asyncio
async def test_get_tempdb_usage():
    rows = [
        {"session_id": 55, "login_name": "sa", "session_status": "running", "host_name": "h", "program_name": "p", "user_objects_alloc_mb": 10.0, "user_objects_dealloc_mb": 2.0, "user_objects_net_mb": 8.0, "internal_objects_alloc_mb": 5.0, "internal_objects_dealloc_mb": 1.0, "internal_objects_net_mb": 4.0, "total_net_mb": 12.0},
    ]
    service = TempdbMemoryService(FakeExecutor([rows]))
    result = await service.get_tempdb_usage("testdb")
    assert result["session_count"] == 1
    assert result["sessions"][0]["total_net_mb"] == 12.0


@pytest.mark.asyncio
async def test_get_tempdb_space_breakdown():
    rows = [
        {"version_store_mb": 50.0, "user_objects_mb": 100.0, "internal_objects_mb": 25.0, "free_space_mb": 825.0, "total_mb": 1000.0},
    ]
    service = TempdbMemoryService(FakeExecutor([rows]))
    result = await service.get_tempdb_space_breakdown("testdb")
    assert result["breakdown"]["version_store_mb"] == 50.0
    assert result["breakdown"]["total_mb"] == 1000.0


@pytest.mark.asyncio
async def test_get_tempdb_space_breakdown_empty():
    service = TempdbMemoryService(FakeExecutor([[]]))
    result = await service.get_tempdb_space_breakdown("testdb")
    assert result["breakdown"] == {}


@pytest.mark.asyncio
async def test_get_memory_grants_with_pending():
    rows = [
        {"session_id": 55, "request_time": "2026-04-01", "grant_time": None, "requested_memory_kb": 50000, "granted_memory_kb": None, "required_memory_kb": 10000, "used_memory_kb": 0, "max_used_memory_kb": 0, "ideal_memory_kb": 50000, "is_small": False, "timeout_sec": 25, "wait_time_ms": 5000, "dop": 4, "query_cost": 100.0, "grant_status": "Pending", "current_statement": "SELECT ..."},
        {"session_id": 56, "request_time": "2026-04-01", "grant_time": "2026-04-01", "requested_memory_kb": 20000, "granted_memory_kb": 20000, "required_memory_kb": 5000, "used_memory_kb": 19500, "max_used_memory_kb": 19500, "ideal_memory_kb": 20000, "is_small": False, "timeout_sec": 25, "wait_time_ms": 0, "dop": 2, "query_cost": 50.0, "grant_status": "Granted", "current_statement": "INSERT ..."},
    ]
    service = TempdbMemoryService(FakeExecutor([rows]))
    result = await service.get_memory_grants("testdb")

    assert result["total_grants"] == 2
    assert result["pending_count"] == 1
    assert result["granted_count"] == 1
    assert result["likely_spilling_count"] == 1  # 19500/20000 = 97.5% >= 95%

    warning_types = [w["type"] for w in result["warnings"]]
    assert "pending_grants" in warning_types
    assert "likely_spilling" in warning_types


@pytest.mark.asyncio
async def test_get_memory_grants_no_warnings():
    rows = [
        {"session_id": 55, "request_time": "2026-04-01", "grant_time": "2026-04-01", "requested_memory_kb": 20000, "granted_memory_kb": 20000, "required_memory_kb": 5000, "used_memory_kb": 5000, "max_used_memory_kb": 5000, "ideal_memory_kb": 20000, "is_small": False, "timeout_sec": 25, "wait_time_ms": 0, "dop": 2, "query_cost": 10.0, "grant_status": "Granted", "current_statement": "SELECT 1"},
    ]
    service = TempdbMemoryService(FakeExecutor([rows]))
    result = await service.get_memory_grants("testdb")
    assert result["warnings"] == []
    assert result["likely_spilling_count"] == 0
