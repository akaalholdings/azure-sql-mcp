from __future__ import annotations

import pytest

from azure_sql_mcp.plan_cache import PlanCacheService


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
async def test_check_statistics_health_flags_stale_and_high_mod():
    rows = [
        {"schema_name": "dbo", "table_name": "Orders", "stats_name": "IX_Date", "auto_created": True, "last_updated": "2026-03-01", "total_rows": 100000, "rows_sampled": 10000, "modification_counter": 30000, "modification_pct": 30.0, "sample_pct": 10.0, "days_since_update": 31},
        {"schema_name": "dbo", "table_name": "Items", "stats_name": "PK_Items", "auto_created": False, "last_updated": "2026-03-30", "total_rows": 50000, "rows_sampled": 50000, "modification_counter": 100, "modification_pct": 0.2, "sample_pct": 100.0, "days_since_update": 2},
        {"schema_name": "dbo", "table_name": "Logs", "stats_name": "IX_Time", "auto_created": True, "last_updated": "2026-03-29", "total_rows": 500000, "rows_sampled": 10000, "modification_counter": 150000, "modification_pct": 30.0, "sample_pct": 2.0, "days_since_update": 3},
    ]
    service = PlanCacheService(FakeExecutor([rows]))
    result = await service.check_statistics_health("testdb", stale_days=7, mod_pct_threshold=20.0)

    assert result["total_stats"] == 3
    assert result["stale_count"] == 1  # Only Orders (31 days)
    assert result["high_modification_count"] == 2  # Orders + Logs (both 30%)
    assert result["low_sample_count"] == 1  # Logs (2% sample, 500k rows)


@pytest.mark.asyncio
async def test_check_statistics_health_all_healthy():
    rows = [
        {"schema_name": "dbo", "table_name": "t", "stats_name": "s", "auto_created": False, "last_updated": "2026-04-01", "total_rows": 1000, "rows_sampled": 1000, "modification_counter": 5, "modification_pct": 0.5, "sample_pct": 100.0, "days_since_update": 0},
    ]
    service = PlanCacheService(FakeExecutor([rows]))
    result = await service.check_statistics_health("testdb")
    assert result["stale_count"] == 0
    assert result["high_modification_count"] == 0
    assert result["low_sample_count"] == 0


@pytest.mark.asyncio
async def test_get_plan_cache_analysis_warns_single_use_bloat():
    distribution = [
        {"plan_type": "Adhoc", "plan_count": 8000, "total_size_mb": 400.0, "total_use_counts": 10000, "single_use_count": 6000, "single_use_size_mb": 300.0},
        {"plan_type": "Prepared", "plan_count": 2000, "total_size_mb": 100.0, "total_use_counts": 50000, "single_use_count": 500, "single_use_size_mb": 20.0},
    ]
    top_plans = [
        {"plan_type": "Adhoc", "usecounts": 1, "size_kb": 500.0, "cacheobjtype": "Compiled Plan", "sql_text_preview": "SELECT * FROM Orders WHERE id = 1"},
    ]
    service = PlanCacheService(FakeExecutor([distribution, top_plans]))
    result = await service.get_plan_cache_analysis("testdb")

    assert result["total_plans"] == 10000
    assert result["total_single_use_plans"] == 6500
    assert len(result["warnings"]) == 1
    assert result["warnings"][0]["type"] == "single_use_plan_bloat"


@pytest.mark.asyncio
async def test_get_plan_cache_analysis_no_warning_when_reuse_good():
    distribution = [
        {"plan_type": "Prepared", "plan_count": 100, "total_size_mb": 50.0, "total_use_counts": 100000, "single_use_count": 10, "single_use_size_mb": 1.0},
    ]
    service = PlanCacheService(FakeExecutor([distribution, []]))
    result = await service.get_plan_cache_analysis("testdb")
    assert result["warnings"] == []


@pytest.mark.asyncio
async def test_get_query_compilation_stats_flags_excessive():
    rows = [
        {"query_hash": b"\x01\x02", "recompile_count": 100, "execution_count": 150, "recompile_ratio": 0.67, "creation_time": "2026-04-01", "last_execution_time": "2026-04-01", "total_elapsed_ms": 5000, "total_cpu_ms": 3000, "total_logical_reads": 100000, "sql_text_preview": "SELECT ..."},
        {"query_hash": b"\x03\x04", "recompile_count": 5, "execution_count": 1000, "recompile_ratio": 0.005, "creation_time": "2026-04-01", "last_execution_time": "2026-04-01", "total_elapsed_ms": 100, "total_cpu_ms": 50, "total_logical_reads": 500, "sql_text_preview": "INSERT ..."},
    ]
    service = PlanCacheService(FakeExecutor([rows]))
    result = await service.get_query_compilation_stats("testdb", top_n=20)

    assert result["recompiled_query_count"] == 2
    assert result["excessive_recompile_count"] == 1  # Only first (0.67 > 0.5 and 150 > 10)
    assert len(result["warnings"]) == 1
