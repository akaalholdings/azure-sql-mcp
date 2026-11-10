"""Memory grant pressure comes from the resource semaphores, not from memory percent.

``avg_memory_usage_percent`` near 100 is normal on Azure SQL Database. Queries
that wait for a grant, time out waiting, or run on a forced minimum grant are the
real signal, and the per-query grant list cannot show the last two.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.memory_pressure import MemoryPressureService
from tests.azure_dmv_contract import StrictDmvExecutor
from tests.azure_dmv_contract import documented_row

SEMAPHORES = "sys.dm_exec_query_resource_semaphores"
DENIED = "VIEW DATABASE STATE permission was denied in database 'appdb'. (262)"
POOLED_TIER = [{"edition": "GeneralPurpose", "service_objective": "ElasticPool", "elastic_pool_name": "pool-a"}]


def semaphore(**values: Any) -> dict[str, Any]:
    base = {
        "resource_semaphore_id": 0,
        "pool_id": 2,
        "target_memory_kb": 4_194_304,
        "max_target_memory_kb": 4_194_304,
        "total_memory_kb": 4_194_304,
        "available_memory_kb": 1_048_576,
        "granted_memory_kb": 3_145_728,
        "used_memory_kb": 2_097_152,
        "grantee_count": 4,
        "waiter_count": 0,
        "timeout_error_count": 0,
        "forced_grant_count": 0,
    }
    base.update(values)
    return documented_row(SEMAPHORES, **base)


def small_query_semaphore() -> dict[str, Any]:
    # The small-query semaphore documents NULL for the two cumulative counters.
    return semaphore(
        resource_semaphore_id=1,
        max_target_memory_kb=None,
        granted_memory_kb=0,
        used_memory_kb=0,
        grantee_count=0,
        timeout_error_count=None,
        forced_grant_count=None,
    )


def executor_for(
    semaphores: list[dict[str, Any]],
    clerks: list[dict[str, Any]] | None = None,
    token_kb: int | None = 0,
    extra: list[tuple[str, Any]] | None = None,
) -> StrictDmvExecutor:
    responses: list[tuple[str, Any]] = list(extra or [])
    responses += [
        ("USERSTORE_TOKENPERM", [{"pages_kb": token_kb}]),
        ("GROUP BY type", clerks if clerks is not None else [{"clerk_type": "MEMORYCLERK_SQLBUFFERPOOL", "pages_kb": 8_388_608}]),
        ("sqlserver_start_time", [{"engine_start_time_utc": "2026-11-01T00:00:00"}]),
    ]
    return StrictDmvExecutor({SEMAPHORES: semaphores}, responses=responses)


@pytest.mark.asyncio
async def test_waiters_timeouts_and_forced_grants_are_reported_with_units() -> None:
    executor = executor_for(
        [semaphore(waiter_count=6, timeout_error_count=3, forced_grant_count=2), small_query_semaphore()]
    )

    result = await MemoryPressureService(executor).get_memory_pressure("appdb")  # type: ignore[arg-type]

    assert executor.violations == []
    assert result["result_status"] == "ok"
    section = result["resource_semaphores"]
    assert section["source_dmv"] == SEMAPHORES
    assert section["totals"] == {
        "waiter_count": 6,
        "grantee_count": 4,
        "timeout_error_count_since_engine_start": 3,
        "forced_grant_count_since_engine_start": 2,
        "granted_memory_mb": 3072.0,
        "available_memory_mb": 2048.0,
    }
    regular, small = section["rows"]
    assert regular["semaphore_kind"] == "regular" and small["semaphore_kind"] == "small_query"
    assert regular["target_memory_mb"] == 4096.0
    assert small["timeout_error_count_since_engine_start"] is None
    assert not any(key.endswith("_kb") for key in regular)
    codes = {finding["code"]: finding for finding in result["findings"]}
    assert codes["memory_grant_waiters"]["severity"] == "high"
    assert codes["memory_grant_timeouts"]["evidence"]["window"] == "cumulative_since_engine_start"
    assert "forced_memory_grants" in codes


@pytest.mark.parametrize(("waiters", "severity"), [(1, "medium"), (4, "medium"), (5, "high")])
@pytest.mark.asyncio
async def test_waiter_severity_follows_the_one_to_five_bar(waiters: int, severity: str) -> None:
    result = await MemoryPressureService(executor_for([semaphore(waiter_count=waiters)])).get_memory_pressure("appdb")  # type: ignore[arg-type]

    (finding,) = result["findings"]
    assert (finding["code"], finding["severity"]) == ("memory_grant_waiters", severity)


@pytest.mark.asyncio
async def test_a_warm_database_with_no_waiters_has_no_finding() -> None:
    # Granted memory at 75% of target with nobody waiting is a busy database, not
    # memory pressure. A finding here would repeat the memory-percent false alarm.
    result = await MemoryPressureService(executor_for([semaphore(), small_query_semaphore()])).get_memory_pressure("appdb")  # type: ignore[arg-type]

    assert result["result_status"] == "ok"
    assert result["findings"] == []
    assert result["collection_window"]["cumulative_counters_since_utc"] == "2026-11-01T00:00:00"


@pytest.mark.parametrize(("token_kb", "flagged"), [(1_572_864, True), (1_048_576, True), (204_800, False)])
@pytest.mark.asyncio
async def test_token_and_perm_user_store_is_flagged_from_one_gigabyte(token_kb: int, flagged: bool) -> None:
    clerks = [
        {"clerk_type": "MEMORYCLERK_SQLBUFFERPOOL", "pages_kb": 8_388_608},
        {"clerk_type": "USERSTORE_TOKENPERM", "pages_kb": token_kb},
    ]
    result = await MemoryPressureService(executor_for([semaphore()], clerks, token_kb)).get_memory_pressure("appdb")  # type: ignore[arg-type]

    clerk_section = result["memory_clerks"]
    assert clerk_section["token_and_perm_user_store_mb"] == round(token_kb / 1024, 1)
    assert clerk_section["top_clerks"][0] == {"clerk_type": "MEMORYCLERK_SQLBUFFERPOOL", "pages_mb": 8192.0}
    assert ("token_and_perm_user_store_large" in {f["code"] for f in result["findings"]}) is flagged


@pytest.mark.asyncio
async def test_clerk_list_is_bounded_by_top_clerks() -> None:
    executor = executor_for([semaphore()])

    await MemoryPressureService(executor).get_memory_pressure("appdb", top_clerks=500)  # type: ignore[arg-type]

    clerk_call = next(call for call in executor.calls if "GROUP BY type" in call[1])
    assert clerk_call[2] == (25,)


@pytest.mark.asyncio
async def test_one_refused_section_does_not_hide_the_other() -> None:
    executor = executor_for(
        [semaphore(waiter_count=2)],
        extra=[("sys.dm_os_memory_clerks", RuntimeError(DENIED)), ("'Edition'", POOLED_TIER)],
    )

    result = await MemoryPressureService(executor).get_memory_pressure("appdb")  # type: ignore[arg-type]

    assert result["result_status"] == "ok"
    assert result["resource_semaphores"]["result_status"] == "ok"
    clerks = result["memory_clerks"]
    assert clerks["result_status"] == "unavailable"
    assert "in an elastic pool" in clerks["result_status_reason"]
    assert "ALTER SERVER ROLE ##MS_ServerStateReader##" in clerks["remediation"]
    assert [finding["code"] for finding in result["findings"]] == ["memory_grant_waiters"]


@pytest.mark.asyncio
async def test_both_sections_refused_is_unavailable_never_a_clean_result() -> None:
    executor = StrictDmvExecutor(
        responses=[
            ("'Edition'", POOLED_TIER),
            ("sys.dm_exec_query_resource_semaphores", RuntimeError(DENIED)),
            ("sys.dm_os_memory_clerks", RuntimeError(DENIED)),
        ]
    )

    result = await MemoryPressureService(executor).get_memory_pressure("appdb")  # type: ignore[arg-type]

    assert result["result_status"] == "unavailable"
    assert result["findings"] == []
    assert "##MS_ServerStateReader##" in result["remediation"]
