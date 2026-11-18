from __future__ import annotations

import pytest

from azure_sql_mcp.plan_cache import PlanCacheService


class FakeExecutor:
    def __init__(self, results_by_call: list[list[dict]] | None = None):
        self._calls: list[list[dict]] = results_by_call or [[]]
        self._call_idx = 0
        self.queries: list[str] = []

    async def fetch_all(self, database_name: str, query: str) -> list[dict]:
        self.queries.append(query)
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
        {"plan_type": "Adhoc", "plan_count": 8000, "compiled_plan_count": 8000, "total_size_mb": 400.0, "total_use_counts": 10000, "single_use_count": 6000, "single_use_size_mb": 300.0},
        {"plan_type": "Prepared", "plan_count": 2000, "compiled_plan_count": 2000, "total_size_mb": 100.0, "total_use_counts": 50000, "single_use_count": 500, "single_use_size_mb": 20.0},
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
        {"plan_type": "Prepared", "plan_count": 100, "compiled_plan_count": 100, "total_size_mb": 50.0, "total_use_counts": 100000, "single_use_count": 10, "single_use_size_mb": 1.0},
    ]
    service = PlanCacheService(FakeExecutor([distribution, []]))
    result = await service.get_plan_cache_analysis("testdb")
    assert result["warnings"] == []


@pytest.mark.asyncio
async def test_single_use_plans_under_the_size_floor_are_not_bloat():
    # 65% of the compiled plans were used once, but they hold 40 MB: there is
    # nothing worth reclaiming, and a small idle cache always looks like this.
    distribution = [
        {"plan_type": "Adhoc", "plan_count": 1000, "compiled_plan_count": 1000, "total_size_mb": 60.0, "total_use_counts": 1500, "single_use_count": 650, "single_use_size_mb": 40.0},
    ]
    result = await PlanCacheService(FakeExecutor([distribution, []])).get_plan_cache_analysis("testdb")

    assert result["single_use_plan_pct"] == 65.0
    assert result["warnings"] == []
    assert result["single_use_bloat_rule"] == {"min_share_of_compiled_plans": 0.5, "min_single_use_size_mb": 100.0}


@pytest.mark.asyncio
async def test_parse_trees_are_cache_entries_not_plans():
    # 9,000 view parse trees, each used once, are not plans that an ad hoc
    # setting can shrink. Only compiled plans count.
    distribution = [
        {"plan_type": "View", "plan_count": 9000, "compiled_plan_count": 0, "total_size_mb": 300.0, "total_use_counts": 9000, "single_use_count": 0, "single_use_size_mb": 0.0},
        {"plan_type": "Prepared", "plan_count": 1000, "compiled_plan_count": 1000, "total_size_mb": 200.0, "total_use_counts": 90000, "single_use_count": 50, "single_use_size_mb": 5.0},
    ]
    result = await PlanCacheService(FakeExecutor([distribution, []])).get_plan_cache_analysis("testdb")

    assert result["total_cache_entries"] == 10000
    assert result["total_plans"] == 1000
    assert result["total_single_use_plans"] == 50
    assert result["single_use_plan_pct"] == 5.0
    assert result["warnings"] == []


@pytest.mark.asyncio
async def test_single_use_is_counted_on_compiled_plans_in_the_statement():
    # A plan stub (the ad hoc setting already on) and a parse tree have usecounts 1 too.
    executor = FakeExecutor([[], []])
    await PlanCacheService(executor).get_plan_cache_analysis("testdb")

    distribution_sql = " ".join(executor.queries[0].split())
    assert distribution_sql.count("usecounts = 1 AND cacheobjtype = 'Compiled Plan'") == 2
    assert "SUM(CASE WHEN cacheobjtype = 'Compiled Plan' THEN 1 ELSE 0 END) AS compiled_plan_count" in distribution_sql
    # usecounts is an int: summed over a busy cache it overflows without the cast.
    assert "SUM(CAST(usecounts AS BIGINT))" in distribution_sql


BLOATED = [
    {"plan_type": "Adhoc", "plan_count": 8000, "compiled_plan_count": 8000, "total_size_mb": 400.0, "total_use_counts": 10000, "single_use_count": 6000, "single_use_size_mb": 300.0},
]


@pytest.mark.asyncio
async def test_bloat_advice_leaves_out_the_setting_when_it_is_already_on():
    # With plan stubs already on, telling the DBA to turn them on is wrong advice.
    result = await PlanCacheService(FakeExecutor([BLOATED, [], [{"value": "1"}]])).get_plan_cache_analysis("testdb")

    (warning,) = result["warnings"]
    assert result["optimize_for_ad_hoc_workloads"] is True
    assert "remediation" not in warning
    assert "already on" in warning["message"]
    assert "parameterize" in warning["message"].lower()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("single_use", "size_mb", "warned"),
    [
        (4000, 300.0, False),  # exactly 50% is not "most of the plans"
        (4001, 100.0, True),  # just over 50%, exactly at the size floor
        (4001, 99.99, False),  # just over 50%, just under the floor
    ],
)
async def test_bloat_rule_at_its_edges(single_use: int, size_mb: float, warned: bool):
    distribution = [{**BLOATED[0], "single_use_count": single_use, "single_use_size_mb": size_mb}]
    result = await PlanCacheService(FakeExecutor([distribution, [], [{"value": "0"}]])).get_plan_cache_analysis("testdb")

    assert bool(result["warnings"]) is warned


@pytest.mark.asyncio
async def test_cache_with_no_compiled_plan_has_no_share_and_no_warning():
    distribution = [
        {"plan_type": "Adhoc", "plan_count": 500, "compiled_plan_count": 0, "total_size_mb": 2.0, "total_use_counts": 500, "single_use_count": 0, "single_use_size_mb": 0.0},
    ]
    result = await PlanCacheService(FakeExecutor([distribution, [], [{"value": "1"}]])).get_plan_cache_analysis("testdb")

    assert (result["total_plans"], result["single_use_plan_pct"], result["warnings"]) == (0, 0.0, [])


@pytest.mark.asyncio
async def test_bloat_advice_names_the_database_scoped_setting():
    result = await PlanCacheService(FakeExecutor([BLOATED, [], [{"value": "0"}]])).get_plan_cache_analysis("testdb")

    assert result["optimize_for_ad_hoc_workloads"] is False
    (warning,) = result["warnings"]
    assert warning["remediation"] == "ALTER DATABASE SCOPED CONFIGURATION SET OPTIMIZE_FOR_AD_HOC_WORKLOADS = ON;"
    assert "6000 of 8000 compiled plans (75%)" in warning["message"]
    # Azure SQL Database has no server option to set.
    assert "sp_configure" not in warning["message"]
    assert "'optimize for ad hoc workloads'" not in warning["message"]


@pytest.mark.asyncio
async def test_get_query_compilation_stats_flags_excessive():
    rows = [
        {"query_hash": b"\x01\x02", "recompile_count": 100, "execution_count": 150, "minutes_since_last_compile": 3, "creation_time": "2026-04-01", "last_execution_time": "2026-04-01", "total_elapsed_ms": 5000, "total_cpu_ms": 3000, "total_logical_reads": 100000, "sql_text_preview": "SELECT ..."},
        {"query_hash": b"\x03\x04", "recompile_count": 5, "execution_count": 1000, "minutes_since_last_compile": 3, "creation_time": "2026-04-01", "last_execution_time": "2026-04-01", "total_elapsed_ms": 100, "total_cpu_ms": 50, "total_logical_reads": 500, "sql_text_preview": "INSERT ..."},
    ]
    service = PlanCacheService(FakeExecutor([rows]))
    result = await service.get_query_compilation_stats("testdb", top_n=20)

    assert result["recompiled_query_count"] == 2
    assert result["excessive_recompile_count"] == 1  # Only the first: 100 generations, compiled 3 minutes ago
    assert len(result["warnings"]) == 1
