"""analyze_db_health: only Active tuning recommendations are pending, and the sweep leads with a summary.

sys.dm_db_tuning_recommendations keeps Success, Reverted and Expired rows. Counting
them as "pending review" warned on nearly every Azure database, and a warning
that is always on teaches a DBA to ignore the check.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.health import HealthService
from tests.azure_dmv_contract import StrictDmvExecutor
from tests.unit.test_health import FakeQueryStoreService
from tests.unit.test_health import build_service

_OPTIONS = [
    {"name": "FORCE_LAST_GOOD_PLAN", "desired_state_desc": "ON", "actual_state_desc": "ON", "reason_desc": None}
]


def _states(**counts: int) -> list[dict[str, Any]]:
    return [{"current_state": state, "recommendation_count": count} for state, count in counts.items()]


async def _tuning(rows: list[dict[str, Any]]) -> tuple[dict[str, Any], StrictDmvExecutor]:
    executor = StrictDmvExecutor(
        rows={"sys.database_automatic_tuning_options": _OPTIONS},
        responses=[("sys.dm_db_tuning_recommendations", rows)],
    )
    service = HealthService(executor, FakeQueryStoreService())  # type: ignore[arg-type]
    return await service._tuning_health("appdb"), executor


@pytest.mark.asyncio
async def test_finished_recommendations_are_not_pending() -> None:
    check, executor = await _tuning(_states(Success=5, Reverted=2, Expired=3))

    assert executor.violations == []
    assert check["status"] == "pass"
    assert check["findings"] == []
    assert check["details"]["pending_recommendation_count"] == 0
    assert check["details"]["recommendations_by_state"] == {"Success": 5, "Reverted": 2, "Expired": 3}


@pytest.mark.asyncio
async def test_only_active_recommendations_warn() -> None:
    check, _ = await _tuning(_states(Active=2, Success=5, Verifying=1))

    assert check["status"] == "warning"
    assert check["findings"] == ["2 automatic tuning recommendations are pending review (state Active)."]
    assert check["details"]["pending_recommendation_count"] == 2
    assert check["details"]["recommendation_count"] == 8


@pytest.mark.asyncio
async def test_a_state_that_cannot_be_read_is_counted_and_not_called_pending() -> None:
    check, _ = await _tuning([{"current_state": None, "recommendation_count": 4}])

    assert check["status"] == "pass"
    assert check["details"]["recommendations_by_state"] == {"unknown": 4}


@pytest.mark.asyncio
async def test_sweep_leads_with_a_summary_and_an_unread_check_is_not_a_pass() -> None:
    service, _, _ = build_service(
        responses=[
            ("sys.database_automatic_tuning_options", _OPTIONS),
            ("FROM sys.dm_db_tuning_recommendations", _states(Active=1)),
        ],
        errors=[("sys.database_files", PermissionError("VIEW DATABASE STATE permission denied"))],
    )

    payload = await service.analyze("appdb", "tuning,storage,constraint")

    assert list(payload)[:3] == ["database_name", "summary", "checks"]
    summary = payload["summary"]
    assert summary["status"] == "warning"
    assert summary["warning"] == ["tuning", "storage"]
    assert summary["not_assessed"] == ["storage"]
    assert summary["passed"] == ["constraint"]
    assert "not a pass" in summary["headline"]


@pytest.mark.asyncio
async def test_summary_of_a_clean_sweep_says_so() -> None:
    service, _, _ = build_service()

    payload = await service.analyze("appdb", "constraint,identity")

    assert payload["summary"] == {
        "status": "pass",
        "critical": [],
        "warning": [],
        "not_assessed": [],
        "passed": ["constraint", "identity"],
        "headline": "All 2 checks passed.",
    }
