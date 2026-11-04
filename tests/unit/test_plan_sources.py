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


_NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"


def _batch_plan(*hashes: str) -> str:
    body = "".join(
        f'<StmtSimple StatementText="s{index}" StatementType="SELECT" QueryPlanHash="{plan_hash}"/>'
        for index, plan_hash in enumerate(hashes)
    )
    return (
        f'<ShowPlanXML xmlns="{_NS}" Version="1.564"><BatchSequence><Batch><Statements>{body}'
        "</Statements></Batch></BatchSequence></ShowPlanXML>"
    )


@pytest.mark.asyncio
async def test_last_actual_plan_names_the_requested_statements_in_the_cached_batch() -> None:
    # The DMV returns the whole cached batch. Without the matching statement indexes the caller
    # cannot tell which statement it asked about; the hash text case can differ between
    # CONVERT(varchar(18), ..., 1) and the showplan attribute.
    plan = _batch_plan("0x0000000000000001", "0xaaaaaaaaaaaaaaaa", "0x0000000000000002", "0xAAAAAAAAAAAAAAAA")
    executor = ScriptedExecutor(
        [
            ("LAST_QUERY_PLAN_STATS", [{"value": "ON"}]),
            (
                "sys.dm_exec_query_plan_stats",
                [{"plan_id": 7, "query_id": 3, "query_plan_hash": "0xAAAAAAAAAAAAAAAA", "query_plan": plan}],
            ),
        ]
    )

    source = await PlanSourceService(executor).last_actual("appdb", plan_id=None, query_id=3)  # type: ignore[arg-type]

    assert source.xml == plan
    assert source.source["requested_statement_indexes"] == [1, 3]


@pytest.mark.asyncio
async def test_last_actual_plan_says_when_the_requested_statement_is_not_in_the_batch() -> None:
    plan = _batch_plan("0x0000000000000001", "0x0000000000000002")
    executor = ScriptedExecutor(
        [
            ("LAST_QUERY_PLAN_STATS", [{"value": "ON"}]),
            (
                "sys.dm_exec_query_plan_stats",
                [{"plan_id": 7, "query_id": 3, "query_plan_hash": "0xAAAAAAAAAAAAAAAA", "query_plan": plan}],
            ),
        ]
    )

    source = await PlanSourceService(executor).last_actual("appdb", plan_id=7, query_id=None)  # type: ignore[arg-type]

    assert source.source["requested_statement_indexes"] == []
    assert "not found" in source.source["source_note"]
