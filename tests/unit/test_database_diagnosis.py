from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.database_diagnosis import DatabaseDiagnosisService
from azure_sql_mcp.database_diagnosis import diagnose_findings


def _codes(findings: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {finding["code"]: finding for finding in findings}


def test_resource_pressure_maps_to_severity_and_next_tools() -> None:
    findings = _codes(
        diagnose_findings(
            {
                "resource_history": {
                    "source": "recent",
                    "summary": {
                        "avg_cpu_percent": {"avg": 85.0, "max": 100.0},
                        "avg_data_io_percent": {"avg": 40.0, "max": 88.0},
                        "avg_log_write_percent": {"avg": 10.0, "max": 20.0},
                    },
                }
            }
        )
    )

    assert findings["cpu_pressure"]["severity"] == "high"
    assert findings["cpu_pressure"]["next_tools"][0] == {"tool": "get_top_queries", "arguments": {"sort_by": "cpu"}}
    assert findings["data_io_pressure"]["severity"] == "medium"
    assert "log_write_pressure" not in findings


def test_log_governor_waits_are_called_out_explicitly() -> None:
    findings = _codes(
        diagnose_findings(
            {
                "waits": {
                    "top_waits": [
                        {"wait_type": "LOG_RATE_GOVERNOR", "pct_of_total": 70.0},
                        {"wait_type": "WRITELOG", "pct_of_total": 20.0},
                    ],
                    "window": {"mode": "interval"},
                }
            }
        )
    )

    assert findings["log_rate_governor_waits"]["severity"] == "high"
    assert "dominant_wait" not in findings


def test_dominant_lock_wait_points_to_blocking_tools() -> None:
    findings = _codes(
        diagnose_findings(
            {"waits": {"top_waits": [{"wait_type": "LCK_M_X", "pct_of_total": 55.0}], "window": {"mode": "cumulative"}}}
        )
    )

    dominant = findings["dominant_wait"]
    assert "since the counters reset" in dominant["message"]
    assert {tool["tool"] for tool in dominant["next_tools"]} == {"get_active_sessions", "get_lock_details"}


def test_blocking_regressions_and_dominant_queries_become_findings() -> None:
    findings = _codes(
        diagnose_findings(
            {
                "sessions": {
                    "blocking_chains": [
                        {"head_blocker_session_id": 61, "blocked_sessions": [{}, {}, {}, {}, {}, {}]}
                    ]
                },
                "top_queries": [
                    {"query_id": 9, "plan_id": 90, "total_cpu_us": 8_000_000},
                    {"query_id": 10, "plan_id": 100, "total_cpu_us": 1_000_000},
                ],
                "regressions": {
                    "regressions": [
                        {"query_id": 9, "regression_pct": 340.0, "plan_changed": True, "new_plan_ids": [91], "extra_cost": 5000.0},
                        {"query_id": 11, "regression_pct": 30.0, "plan_changed": False},
                    ]
                },
            }
        )
    )

    assert findings["active_blocking"]["severity"] == "high"
    assert "head blocker session 61" in findings["active_blocking"]["message"]
    assert findings["dominant_query"]["next_tools"][0] == {"tool": "analyze_query_plan", "arguments": {"query_id": 9}}
    assert findings["query_regression"]["severity"] == "high"
    assert findings["query_regression"]["evidence"]["new_plan_ids"] == [91]


def test_quiet_evidence_yields_no_findings() -> None:
    assert diagnose_findings(
        {
            "resource_history": {"summary": {"avg_cpu_percent": {"avg": 10.0, "max": 30.0}}},
            "waits": {"top_waits": [{"wait_type": "WRITELOG", "pct_of_total": 20.0}]},
            "sessions": {"blocking_chains": []},
            "top_queries": [],
            "regressions": {"regressions": []},
            "version_store": {"findings": []},
        }
    ) == []


def _service(**overrides: Any) -> DatabaseDiagnosisService:
    defaults = {
        "resource_governance": AsyncMock(),
        "wait_stats": AsyncMock(),
        "sessions": AsyncMock(),
        "query_store": AsyncMock(),
        "query_store_trends": AsyncMock(),
        "version_store": AsyncMock(),
    }
    defaults["resource_governance"].get_resource_stats_history = AsyncMock(
        return_value={"summary": {"avg_cpu_percent": {"avg": 90.0, "max": 99.0}}, "source": "recent"}
    )
    defaults["resource_governance"].get_resource_limits = AsyncMock(return_value={"service_objective": {"service_objective": "GP_S_Gen5_2"}})
    defaults["wait_stats"].get_wait_stats = AsyncMock(return_value={"top_waits": [], "window": {"mode": "cumulative"}})
    defaults["sessions"].get_active_sessions = AsyncMock(return_value={"blocking_chains": [], "active_session_count": 3})
    defaults["query_store"].get_top_queries = AsyncMock(return_value=[])
    defaults["query_store_trends"].regressions = AsyncMock(return_value={"regressions": []})
    defaults["version_store"].get_version_store_stats = AsyncMock(return_value={"findings": [], "persistent_version_store": {"size_mb": 12.0}})
    defaults.update(overrides)
    return DatabaseDiagnosisService(**defaults)


@pytest.mark.asyncio
async def test_diagnose_reports_findings_facts_and_passes_the_window() -> None:
    service = _service()

    result = await service.diagnose("appdb", window_minutes=120, sample_seconds=10, master_available=True)

    assert result["result_status"] == "ok"
    assert result["verdict"] == "findings"
    assert result["findings"][0]["code"] == "cpu_pressure"
    assert result["facts"]["service_objective"] == {"service_objective": "GP_S_Gen5_2"}
    service.resource_governance.get_resource_stats_history.assert_awaited_once_with(
        "appdb", 120, master_available=True
    )
    service.wait_stats.get_wait_stats.assert_awaited_once_with("appdb", 10, sample_seconds=10)
    service.query_store.get_top_queries.assert_awaited_once_with("appdb", "cpu", 120, 5)


@pytest.mark.asyncio
async def test_failed_sources_are_gaps_and_never_a_clean_verdict() -> None:
    sessions = AsyncMock()
    sessions.get_active_sessions = AsyncMock(side_effect=PermissionError("VIEW DATABASE STATE denied"))
    resource = AsyncMock()
    resource.get_resource_stats_history = AsyncMock(return_value={"summary": {}})
    resource.get_resource_limits = AsyncMock(return_value={})
    service = _service(sessions=sessions, resource_governance=resource)

    result = await service.diagnose("appdb")

    assert result["sources"]["sessions"] == "unavailable"
    assert result["verdict"] == "partial"
    assert any(gap.startswith("sessions:") for gap in result["gaps"])


@pytest.mark.asyncio
async def test_every_source_failing_is_unavailable() -> None:
    def failing() -> AsyncMock:
        return AsyncMock(side_effect=TimeoutError("timed out"))

    resource = AsyncMock()
    resource.get_resource_stats_history = failing()
    resource.get_resource_limits = failing()
    waits = AsyncMock()
    waits.get_wait_stats = failing()
    sessions = AsyncMock()
    sessions.get_active_sessions = failing()
    store = AsyncMock()
    store.get_top_queries = failing()
    trends = AsyncMock()
    trends.regressions = failing()
    pvs = AsyncMock()
    pvs.get_version_store_stats = failing()
    service = DatabaseDiagnosisService(
        resource_governance=resource,
        wait_stats=waits,
        sessions=sessions,
        query_store=store,
        query_store_trends=trends,
        version_store=pvs,
    )

    result = await service.diagnose("appdb")

    assert result["result_status"] == "unavailable"
    assert len(result["gaps"]) == 7
