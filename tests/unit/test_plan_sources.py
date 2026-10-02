from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.plan_sources import PlanSourceService


class ScriptedExecutor:
    """Returns or raises per statement, matched by a needle in the SQL."""

    def __init__(self, script: list[tuple[str, Any]]) -> None:
        self.script = script

    async def fetch_all(self, database_name: str, query: str, *_: Any, **__: Any) -> list[dict[str, Any]]:
        for needle, result in self.script:
            if needle in query:
                if isinstance(result, Exception):
                    raise result
                return result
        return []


@pytest.mark.asyncio
async def test_last_actual_plan_permission_failure_names_the_pool_and_basic_tier_grant() -> None:
    # The last actual plan joins sys.dm_exec_query_stats, which on Basic/S0/S1 and pooled
    # databases VIEW DATABASE STATE cannot read.
    executor = ScriptedExecutor(
        [
            ("LAST_QUERY_PLAN_STATS", [{"value": "ON"}]),
            ("sys.dm_exec_query_stats", PermissionError("VIEW SERVER STATE permission was denied")),
        ]
    )

    source = await PlanSourceService(executor).last_actual("appdb", plan_id=7, query_id=None)  # type: ignore[arg-type]

    assert source.xml is None
    assert source.status["result_status"] == "unavailable"
    reason = source.status["result_status_reason"]
    assert "##MS_ServerStateReader##" in reason
    assert "elastic-pool" in reason
    assert "sys.dm_exec_query_stats" in reason


@pytest.mark.asyncio
async def test_live_plan_permission_failure_names_the_admin_requirement_on_basic_and_standard() -> None:
    executor = ScriptedExecutor([("sys.dm_exec_query_statistics_xml", PermissionError("denied"))])

    source = await PlanSourceService(executor).live("appdb", 61)  # type: ignore[arg-type]

    reason = source.status["result_status_reason"]
    assert "server admin" in reason and "Basic and Standard" in reason


@pytest.mark.asyncio
async def test_query_store_runtime_failure_keeps_the_database_grant() -> None:
    executor = ScriptedExecutor([("query_store_runtime_stats", PermissionError("denied"))])

    status = await PlanSourceService(executor).query_store_runtime("appdb", 7, "<x/>")  # type: ignore[arg-type]

    assert status["result_status"] == "unavailable"
    assert status["result_status_reason"].endswith("VIEW DATABASE STATE is required.")
