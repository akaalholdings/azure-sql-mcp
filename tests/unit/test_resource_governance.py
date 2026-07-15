from __future__ import annotations

import pytest

from azure_sql_mcp.resource_governance import ResourceGovernanceService


class FakeExecutor:
    def __init__(self, results_by_call: list[list[dict]] | None = None):
        self._calls: list[list[dict]] = results_by_call or [[]]
        self._call_idx = 0

    async def fetch_all(self, database_name: str, query: str) -> list[dict]:
        if self._call_idx < len(self._calls):
            result = self._calls[self._call_idx]
            self._call_idx += 1
            return result
        return []


@pytest.mark.asyncio
async def test_get_io_stats_warns_on_high_latency():
    rows = [
        {"file_name": "data", "file_type": "ROWS", "physical_name": "/data/file.mdf", "file_size_mb": 1024, "num_of_reads": 10000, "num_of_writes": 5000, "read_mb": 500, "write_mb": 200, "avg_read_latency_ms": 25.0, "avg_write_latency_ms": 5.0, "io_stall_read_ms": 250000, "io_stall_write_ms": 25000, "total_io_stall_ms": 275000},
        {"file_name": "log", "file_type": "LOG", "physical_name": "/data/file.ldf", "file_size_mb": 256, "num_of_reads": 100, "num_of_writes": 20000, "read_mb": 5, "write_mb": 1000, "avg_read_latency_ms": 2.0, "avg_write_latency_ms": 3.0, "io_stall_read_ms": 200, "io_stall_write_ms": 60000, "total_io_stall_ms": 60200},
    ]
    service = ResourceGovernanceService(FakeExecutor([rows]))
    result = await service.get_io_stats("testdb")

    assert len(result["files"]) == 2
    assert len(result["warnings"]) == 1  # data file read latency > 20ms
    assert result["warnings"][0]["type"] == "high_read_latency"
    assert result["warnings"][0]["file"] == "data"


@pytest.mark.asyncio
async def test_get_io_stats_no_warnings():
    rows = [
        {"file_name": "data", "file_type": "ROWS", "physical_name": "f", "file_size_mb": 100, "num_of_reads": 100, "num_of_writes": 50, "read_mb": 10, "write_mb": 5, "avg_read_latency_ms": 5.0, "avg_write_latency_ms": 3.0, "io_stall_read_ms": 500, "io_stall_write_ms": 150, "total_io_stall_ms": 650},
    ]
    service = ResourceGovernanceService(FakeExecutor([rows]))
    result = await service.get_io_stats("testdb")
    assert result["warnings"] == []


@pytest.mark.asyncio
async def test_get_resource_limits():
    gov_rows = [
        {"primary_group_id": 1, "primary_max_cpu_percent": 100, "primary_max_log_rate_per_db_in_bytes_per_second": 50331648, "primary_group_max_io": 5000, "pool_max_io": 12800, "max_db_memory": 1048576, "max_workers_per_query": 8, "checkpoint_rate_mbps": 100, "volume_local_iops": 4800, "volume_pfs_iops": 4800},
    ]
    slo_rows = [
        {"edition": "GeneralPurpose", "service_objective": "GP_Gen5_2", "elastic_pool_name": None},
    ]
    service = ResourceGovernanceService(FakeExecutor([gov_rows, slo_rows]))
    result = await service.get_resource_limits("testdb")

    assert result["service_objective"]["service_objective"] == "GP_Gen5_2"
    assert result["governance_limits"]["primary_max_cpu_percent"] == 100
    assert result["governance_limits"]["max_log_rate_mb_per_sec"] == 48.0  # 50331648 / 1048576


@pytest.mark.asyncio
async def test_get_resource_stats_history_warns_sustained_pressure():
    # Create 10 samples, 7 with CPU > 80% → should warn (70% > 30%)
    rows = []
    for i in range(10):
        cpu = 85.0 if i < 7 else 30.0
        rows.append({
            "end_time": f"2026-04-01T10:{i:02d}:00",
            "avg_cpu_percent": cpu,
            "avg_data_io_percent": 10.0,
            "avg_log_write_percent": 5.0,
            "avg_memory_usage_percent": 40.0,
            "max_worker_percent": 20.0,
            "max_session_percent": 15.0,
            "avg_instance_cpu_percent": cpu,
        })
    service = ResourceGovernanceService(FakeExecutor([rows]))
    result = await service.get_resource_stats_history("testdb", window_minutes=60)

    assert result["sample_count"] == 10
    assert "avg_cpu_percent" in result["summary"]
    assert result["summary"]["avg_cpu_percent"]["samples_above_80_pct"] == 7

    warning_types = [w["type"] for w in result["warnings"]]
    assert "sustained_avg_cpu_percent" in warning_types
    # Other metrics should not warn
    assert "sustained_avg_data_io_percent" not in warning_types


@pytest.mark.asyncio
async def test_get_resource_stats_history_no_warnings():
    rows = [
        {"end_time": "2026-04-01T10:00:00", "avg_cpu_percent": 20.0, "avg_data_io_percent": 5.0, "avg_log_write_percent": 2.0, "avg_memory_usage_percent": 30.0, "max_worker_percent": 10.0, "max_session_percent": 5.0, "avg_instance_cpu_percent": 20.0},
    ]
    service = ResourceGovernanceService(FakeExecutor([rows]))
    result = await service.get_resource_stats_history("testdb")
    assert result["warnings"] == []
