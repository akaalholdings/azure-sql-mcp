from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any

import pytest

from azure_sql_mcp.query_store import QueryStoreService
from azure_sql_mcp.query_store_trends import QueryStoreTrendService

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


class RoutingExecutor:
    def __init__(self, **responses: Any) -> None:
        self.responses = {
            "state": [{"actual_state_desc": "READ_WRITE", "query_capture_mode_desc": "ALL"}],
            "trend": [],
            "plans": [],
            "coverage": [{"first_interval_utc": "2026-09-01T00:00:00", "intervals_in_baseline": 168}],
            "regressions": [],
        }
        self.responses.update(responses)
        self.calls: list[tuple[str, str, list[Any] | None]] = []

    async def fetch_all(self, database_name: str, query: str, params=None, **kwargs: Any):
        if "database_query_store_options" in query:
            key = "state"
        elif "plan_period" in query:
            key = "regressions"
        elif "intervals_in_baseline" in query:
            key = "coverage"
        elif "GROUP BY rs.plan_id" in query:
            key = "plans"
        else:
            key = "trend"
        self.calls.append((key, query, list(params) if params is not None else None))
        result = self.responses[key]
        if isinstance(result, Exception):
            raise result
        return result


def _bucket(hour: int, executions: float, duration_ms: float, plans: int = 1) -> dict[str, Any]:
    return {
        "bucket_start_utc": datetime(2026, 10, 1, hour, 0),
        "plan_count": plans,
        "executions": executions,
        "total_cpu_ms": duration_ms / 2,
        "total_duration_ms": duration_ms,
        "total_logical_reads": executions * 100,
        "total_physical_reads": 0,
    }


@pytest.mark.asyncio
async def test_workload_trend_buckets_carry_averages_and_a_peak() -> None:
    executor = RoutingExecutor(trend=[_bucket(9, 100, 1_000), _bucket(10, 50, 4_000, plans=2)])

    result = await QueryStoreTrendService(executor).trend(
        "appdb", window_minutes=240, bucket_minutes=60, now=NOW
    )

    trend_call = next(call for call in executor.calls if call[0] == "trend")
    assert trend_call[2] == ["2026-10-01T12:00:00", "2026-10-01T08:00:00"]
    assert "p.query_id = ?" not in trend_call[1]
    assert result["buckets"][1] == {
        "bucket_start_utc": "2026-10-01T10:00:00Z",
        "executions": 50,
        "plan_count": 2,
        "total_cpu_ms": 2000.0,
        "total_duration_ms": 4000.0,
        "total_logical_reads": 5000.0,
        "total_physical_reads": 0.0,
        "avg_cpu_ms": 40.0,
        "avg_duration_ms": 80.0,
        "avg_logical_reads": 100.0,
    }
    assert result["totals"]["peak_bucket_utc"] == "2026-10-01T10:00:00Z"
    assert "plans" not in result


@pytest.mark.asyncio
async def test_query_trend_includes_the_plan_breakdown() -> None:
    executor = RoutingExecutor(
        trend=[_bucket(9, 10, 100)],
        plans=[
            {"plan_id": 7, "is_forced_plan": 0, "executions": 8, "total_cpu_ms": 40, "total_duration_ms": 80, "total_logical_reads": 800, "first_seen_utc": "2026-09-30T00:00:00", "last_seen_utc": "2026-10-01T09:00:00"},
            {"plan_id": 9, "is_forced_plan": 0, "executions": 2, "total_cpu_ms": 100, "total_duration_ms": 400, "total_logical_reads": 9000, "first_seen_utc": "2026-10-01T08:00:00", "last_seen_utc": "2026-10-01T09:00:00"},
        ],
    )

    result = await QueryStoreTrendService(executor).trend("appdb", query_id=42, now=NOW)

    trend_call = next(call for call in executor.calls if call[0] == "trend")
    assert "p.query_id = ?" in trend_call[1]
    assert trend_call[2][-1] == 42
    plan_call = next(call for call in executor.calls if call[0] == "plans")
    assert plan_call[2] == ["2026-10-01T12:00:00", "2026-09-30T12:00:00", 42]
    assert [plan["plan_id"] for plan in result["plans"]] == [7, 9]
    assert result["plans"][1]["avg_duration_ms"] == 200.0
    assert result["plan_changes_in_window"] == 1


@pytest.mark.asyncio
async def test_long_windows_widen_buckets_to_stay_compact() -> None:
    executor = RoutingExecutor()

    result = await QueryStoreTrendService(executor).trend(
        "appdb", window_minutes=43200, bucket_minutes=5, now=NOW
    )

    assert result["bucket_minutes"] == 90
    assert result["bucket_size_adjusted"] is True
    assert "/ 90) * 90" in next(call for call in executor.calls if call[0] == "trend")[1]


@pytest.mark.asyncio
async def test_query_store_off_is_a_precondition() -> None:
    executor = RoutingExecutor(state=[{"actual_state_desc": "OFF"}])

    result = await QueryStoreTrendService(executor).trend("appdb", now=NOW)

    assert result["result_status"] == "precondition"
    assert "QUERY_STORE = ON" in result["remediation"]
    assert result["buckets"] == []


@pytest.mark.asyncio
async def test_regressions_without_baseline_history_are_unavailable_not_clean() -> None:
    executor = RoutingExecutor(coverage=[{"first_interval_utc": None, "intervals_in_baseline": 0}])

    result = await QueryStoreTrendService(executor).regressions("appdb", now=NOW)

    assert result["result_status"] == "unavailable"
    assert "not a clean bill of health" in result["result_status_reason"]
    assert all(call[0] != "regressions" for call in executor.calls)


@pytest.mark.asyncio
async def test_regressions_rank_by_weighted_extra_cost_and_flag_new_plans() -> None:
    executor = RoutingExecutor(
        regressions=[
            {
                # 10 ms -> 40 ms per execution over 100 recent runs: +3,000 ms.
                "query_id": 1, "recent_executions": 100, "recent_total_duration_us": 4_000_000,
                "recent_total_cpu_us": 0, "recent_total_logical_reads": 0, "recent_plan_ids": "5,8",
                "baseline_executions": 1000, "baseline_total_duration_us": 10_000_000,
                "baseline_total_cpu_us": 0, "baseline_total_logical_reads": 0, "baseline_plan_ids": "5",
                "object_schema": "dbo", "object_name": "usp_orders", "query_text_preview": "SELECT 1",
            },
            {
                # 1 ms -> 3 ms per execution over 5,000 recent runs: +10,000 ms.
                "query_id": 2, "recent_executions": 5000, "recent_total_duration_us": 15_000_000,
                "recent_total_cpu_us": 0, "recent_total_logical_reads": 0, "recent_plan_ids": "3",
                "baseline_executions": 50000, "baseline_total_duration_us": 50_000_000,
                "baseline_total_cpu_us": 0, "baseline_total_logical_reads": 0, "baseline_plan_ids": "3",
                "object_schema": None, "object_name": None, "query_text_preview": "SELECT 2",
            },
            {
                # 10% worse: below the 25% default threshold.
                "query_id": 3, "recent_executions": 100, "recent_total_duration_us": 1_100_000,
                "recent_total_cpu_us": 0, "recent_total_logical_reads": 0, "recent_plan_ids": "4",
                "baseline_executions": 100, "baseline_total_duration_us": 1_000_000,
                "baseline_total_cpu_us": 0, "baseline_total_logical_reads": 0, "baseline_plan_ids": "4",
                "object_schema": None, "object_name": None, "query_text_preview": "SELECT 3",
            },
        ]
    )

    result = await QueryStoreTrendService(executor).regressions(
        "appdb", recent_minutes=60, baseline_minutes=1440, now=NOW
    )

    regression_call = next(call for call in executor.calls if call[0] == "regressions")
    assert regression_call[2] == [
        "2026-10-01T11:00:00",
        "2026-10-01T12:00:00",
        "2026-09-30T11:00:00",
        10,
        10,
    ]
    assert [item["query_id"] for item in result["regressions"]] == [2, 1]
    first, second = result["regressions"]
    assert first["regression_pct"] == 200.0
    assert first["extra_cost"] == 10_000.0
    assert second["new_plan_ids"] == [8]
    assert second["plan_changed"] is True
    assert second["object_name"] == "dbo.usp_orders"
    assert result["window"]["recent_start_utc"] == "2026-10-01T11:00:00Z"


@pytest.mark.asyncio
async def test_future_as_of_is_refused() -> None:
    with pytest.raises(ValueError, match="future"):
        await QueryStoreTrendService(RoutingExecutor()).trend(
            "appdb", as_of_utc="2027-01-01T00:00:00Z", now=NOW
        )


class CapturingExecutor:
    def __init__(self) -> None:
        self.query = ""
        self.params: list[Any] = []

    async def fetch_all(self, database_name: str, query: str, params=None, **kwargs: Any):
        self.query = query
        self.params = list(params or [])
        return []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sort_by", "expected"),
    [
        ("cpu", [10, "2026-09-30T04:00:00", "2026-09-30T05:00:00"]),
        ("resource_blend", ["2026-09-30T04:00:00", "2026-09-30T05:00:00", 10]),
    ],
)
async def test_top_queries_read_an_explicit_past_window(sort_by: str, expected: list[Any]) -> None:
    executor = CapturingExecutor()

    await QueryStoreService(executor).get_top_queries(
        "appdb", sort_by, 60, 10, window_end=datetime(2026, 9, 30, 5, 0, tzinfo=timezone.utc)
    )

    assert "DATEADD(MINUTE, -?, SYSUTCDATETIME())" not in executor.query
    assert "rsi.start_time < TODATETIMEOFFSET(CAST(? AS datetime2), 0)" in executor.query
    assert executor.params == expected


@pytest.mark.asyncio
async def test_top_queries_default_window_is_unchanged() -> None:
    executor = CapturingExecutor()

    await QueryStoreService(executor).get_top_queries("appdb", "cpu", 60, 10)

    assert "DATEADD(MINUTE, -?, SYSUTCDATETIME())" in executor.query
    assert executor.params == [10, 60]
