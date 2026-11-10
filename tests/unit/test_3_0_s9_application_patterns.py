"""Bad actor by shape, pile-ups, chatty callers and row-by-row calls.

The fix for a query that runs a million times at 1 ms is in the application
(call it less). The fix for a query that runs ten times at 30 s is in the plan.
Sending a DBA to tune the plan of the first one wastes the incident.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.application_patterns import ApplicationPatternService
from azure_sql_mcp.application_patterns import chatty_caller
from azure_sql_mcp.application_patterns import query_shape
from azure_sql_mcp.application_patterns import row_by_row


def _row(**values: Any) -> dict[str, Any]:
    row = {
        "query_hash": "0x1A2B",
        "sample_query_id": 7,
        "executions": 500,
        "total_cpu_us": 500 * 20_000,
        "avg_logical_io_reads": 100,
        "avg_rowcount": 10,
    }
    row.update(values)
    return row


def test_frequent_light_and_rare_heavy_get_different_advice() -> None:
    light = query_shape(_row(executions=600_000, total_cpu_us=600_000 * 2_000, avg_logical_io_reads=12), 60)
    heavy = query_shape(_row(executions=40, total_cpu_us=40 * 9_000_000, avg_logical_io_reads=900_000), 60)

    assert light["shape"] == "frequent_light"
    assert heavy["shape"] == "rare_heavy"
    assert "fewer calls" in light["advice"]
    assert "plan" in heavy["advice"]
    assert light["advice"] != heavy["advice"]


def test_severity_is_execution_tier_times_cost_per_execution() -> None:
    # 2.5 s of CPU per execution: heavy. The more often it runs, the worse.
    def severity(executions: int) -> Any:
        return query_shape(_row(executions=executions, total_cpu_us=executions * 2_500_000), 60)["severity"]

    assert severity(50) is None  # under 100 executions the shape is not scored
    assert severity(150) == "medium"
    assert severity(5_000) == "high"


def test_a_light_query_that_runs_seldom_is_not_a_bad_actor() -> None:
    shape = query_shape(_row(executions=300, total_cpu_us=300 * 4_000, avg_logical_io_reads=50), 60)

    assert shape["shape"] == "moderate"
    assert shape["severity"] is None


def test_reads_alone_make_a_query_heavy() -> None:
    shape = query_shape(_row(executions=200, total_cpu_us=200 * 10_000, avg_logical_io_reads=400_000), 60)

    assert shape["shape"] == "frequent_heavy"
    assert shape["severity"] == "medium"


def test_row_by_row_needs_a_high_rate_and_tiny_row_counts() -> None:
    per_second = 900
    chatty = _row(executions=per_second * 3600, avg_rowcount=1.0)

    assert row_by_row(chatty, 60)["executions_per_second"] == 900.0
    # The same rate returning 500 rows a call is a real workload, not a loop.
    assert row_by_row(_row(executions=per_second * 3600, avg_rowcount=500.0), 60) is None
    assert row_by_row(_row(executions=3600, avg_rowcount=1.0), 60) is None


def test_chatty_caller_is_one_program_with_most_of_the_requests() -> None:
    callers = [
        {"program_name": "report-runner", "host_name": "vm-17", "login_name": "svc", "request_count": 18},
        {"program_name": "web", "host_name": "vm-02", "login_name": "svc", "request_count": 2},
    ]

    caller = chatty_caller(callers)

    assert caller == {"program_name": "report-runner", "request_count": 18, "share_pct": 90.0}
    assert chatty_caller([{"program_name": "a", "request_count": 3}, {"program_name": "b", "request_count": 2}]) is None
    # Two requests from one program is not a pattern.
    assert chatty_caller([{"program_name": "a", "request_count": 2}]) is None


def _service(**overrides: Any) -> ApplicationPatternService:
    query_store = AsyncMock()
    query_store.get_top_queries = AsyncMock(return_value=[])
    sessions = AsyncMock()
    sessions.get_active_sessions = AsyncMock(return_value={"pileups": [], "load_by_caller": []})
    services = {"query_store": query_store, "sessions": sessions}
    services.update(overrides)
    return ApplicationPatternService(**services)


@pytest.mark.asyncio
async def test_analyze_reports_each_pattern_with_its_own_advice() -> None:
    query_store = AsyncMock()
    query_store.get_top_queries = AsyncMock(
        return_value=[
            _row(query_hash="0xAA", executions=900 * 3600, total_cpu_us=900 * 3600 * 800, avg_rowcount=1.0),
            _row(query_hash="0xBB", executions=120, total_cpu_us=120 * 6_000_000, avg_logical_io_reads=700_000),
        ]
    )
    sessions = AsyncMock()
    sessions.get_active_sessions = AsyncMock(
        return_value={
            "pileups": [{"query_hash": "0xAA", "request_count": 14, "blocked_request_count": 0}],
            "load_by_caller": [
                {"program_name": "order-sync", "host_name": "vm-secret-9", "request_count": 15},
                {"program_name": "web", "host_name": "vm-secret-2", "request_count": 1},
            ],
        }
    )

    result = await _service(query_store=query_store, sessions=sessions).analyze("appdb", window_minutes=60)

    patterns = {(item["code"], item["evidence"].get("query_hash")): item for item in result["patterns"]}
    assert ("row_by_row_calls", "0xAA") in patterns
    assert ("rare_heavy_query", "0xBB") in patterns or ("frequent_heavy_query", "0xBB") in patterns
    assert ("query_pileup", "0xAA") in patterns
    assert any(item["code"] == "chatty_caller" for item in result["patterns"])
    assert result["result_status"] == "ok"
    # Host names identify machines and people; the program name is enough for the operator.
    assert "vm-secret" not in json.dumps(result)
    query_store.get_top_queries.assert_any_await("appdb", "executions", 60, 20, group_by="query_hash")
    query_store.get_top_queries.assert_any_await("appdb", "cpu", 60, 20, group_by="query_hash")


@pytest.mark.asyncio
async def test_quiet_database_is_an_explicit_empty_result() -> None:
    result = await _service().analyze("appdb")

    assert result["patterns"] == []
    assert result["result_status"] == "empty"


@pytest.mark.asyncio
async def test_unreadable_query_store_is_a_gap_not_a_clean_result() -> None:
    query_store = AsyncMock()
    query_store.get_top_queries = AsyncMock(side_effect=PermissionError("VIEW DATABASE STATE denied"))

    result = await _service(query_store=query_store).analyze("appdb")

    assert result["sources"]["top_by_executions"] == "unavailable"
    assert result["result_status"] == "ok"
    assert "not an all-clear" in result["result_status_reason"]
    assert any(gap.startswith("top_by_executions:") for gap in result["gaps"])


@pytest.mark.asyncio
async def test_no_readable_source_is_unavailable() -> None:
    query_store = AsyncMock()
    query_store.get_top_queries = AsyncMock(side_effect=TimeoutError("timed out"))
    sessions = AsyncMock()
    sessions.get_active_sessions = AsyncMock(return_value={"result_status": "unavailable", "result_status_reason": "denied"})

    result = await _service(query_store=query_store, sessions=sessions).analyze("appdb")

    assert result["result_status"] == "unavailable"
    assert len(result["gaps"]) == 3
