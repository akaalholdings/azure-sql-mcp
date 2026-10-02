"""Every DMV column the diagnostic code names exists in Azure SQL Database.

A column that Microsoft Learn does not document fails on a real database with
error 207. Callers often swallow that error and report a clean result, so these
tests assert on the executor's recorded violations, not on the tool output.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp.diagnostics import DiagnosticQueryService
from azure_sql_mcp.health import HealthService
from azure_sql_mcp.index_metadata import collect_existing_indexes
from azure_sql_mcp.plan_cache import PlanCacheService
from azure_sql_mcp.resource_governance import ResourceGovernanceService
from azure_sql_mcp.tempdb_memory import TempdbMemoryService
from azure_sql_mcp.version_store import VersionStoreService
from azure_sql_mcp.wait_stats import WaitStatsService
from azure_sql_mcp.workload_index_advisor import TABLES_SQL
from tests.azure_dmv_contract import LIVE_DIR
from tests.azure_dmv_contract import DmvColumnError
from tests.azure_dmv_contract import StrictDmvExecutor
from tests.azure_dmv_contract import UnparsedStatementError
from tests.azure_dmv_contract import documented_entries
from tests.azure_dmv_contract import documented_row
from tests.azure_dmv_contract import live_contracts

GOVERNANCE = "sys.dm_user_db_resource_governance"
GEO_LINK = "sys.dm_geo_replication_link_status"
RESOURCE_STATS = "sys.dm_db_resource_stats"


class FakeQueryStore:
    async def get_status(self, database_name: str) -> dict[str, Any]:
        return {"enabled": True, "status": {"actual_state_desc": "READ_WRITE"}}


def governance_row(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "database_id": 5,
        "slo_name": "SQLDB_OP_GP_GEN5_2",
        "cpu_limit": 2,
        "primary_group_max_cpu": 100.0,
        "primary_max_log_rate": 50331648,
        "pool_max_log_rate": 50331648,
        "instance_max_log_rate": 104857600,
        "primary_group_max_io": 640,
        "pool_max_io": 640,
        "primary_group_max_workers": 200,
        "primary_pool_max_workers": 200,
        "max_sessions": 30000,
        "max_dop": 0,
        "replica_role": 0,
    }
    values.update(overrides)
    return documented_row(GOVERNANCE, **values)


def strict_executor(**extra_rows: list[dict[str, Any]]) -> StrictDmvExecutor:
    rows = {
        GOVERNANCE: [governance_row()],
        RESOURCE_STATS: [
            documented_row(RESOURCE_STATS, end_time="2026-10-02T10:00:00", avg_cpu_percent=12.0, max_worker_percent=3.0)
        ],
        **{name.replace("__", "."): value for name, value in extra_rows.items()},
    }
    return StrictDmvExecutor(rows, responses=[("AS idle_with_open_transaction", [{"total_sessions": 300}])])


# --- the harness itself -------------------------------------------------------


@pytest.mark.asyncio
async def test_strict_executor_rejects_undocumented_column() -> None:
    executor = StrictDmvExecutor()

    with pytest.raises(DmvColumnError, match=r"Invalid column name 'user_sessions_limit'\. \(207\)"):
        await executor.fetch_all("appdb", "SELECT drs.user_sessions_limit FROM sys.dm_user_db_resource_governance AS drs")
    assert executor.violations == ["user_sessions_limit"]


@pytest.mark.asyncio
async def test_strict_executor_rejects_internal_columns_named_explicitly() -> None:
    # max_db_memory is documented as "Internal use only"; SELECT * may still return it.
    executor = StrictDmvExecutor({GOVERNANCE: [governance_row(max_db_memory=10)]})

    with pytest.raises(DmvColumnError):
        await executor.fetch_all("appdb", "SELECT max_db_memory FROM sys.dm_user_db_resource_governance")
    star = await executor.fetch_all("appdb", "SELECT TOP (1) * FROM sys.dm_user_db_resource_governance")
    assert star[0]["max_db_memory"] == 10
    assert set(star[0]) == set(documented_entries()[GOVERNANCE]["columns"])


@pytest.mark.asyncio
async def test_strict_executor_checks_table_valued_functions_and_apply() -> None:
    executor = StrictDmvExecutor()

    with pytest.raises(DmvColumnError):
        await executor.fetch_all(
            "appdb",
            "SELECT ps.plan_xml FROM sys.dm_exec_query_stats AS qs "
            "CROSS APPLY sys.dm_exec_query_plan_stats(qs.plan_handle) AS ps",
        )
    with pytest.raises(DmvColumnError):
        await executor.fetch_all("appdb", "SELECT fs.read_latency FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) AS fs")
    await executor.fetch_all(
        "appdb",
        "SELECT fs.num_of_reads, df.name FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) AS fs "
        "INNER JOIN sys.database_files AS df ON fs.file_id = df.file_id",
    )


@pytest.mark.asyncio
async def test_strict_executor_projects_plain_and_cast_columns() -> None:
    executor = StrictDmvExecutor({RESOURCE_STATS: [documented_row(RESOURCE_STATS, avg_cpu_percent=41.5)]})

    rows = await executor.fetch_all(
        "appdb", "SELECT CAST(avg_cpu_percent AS DECIMAL(5, 2)) AS cpu FROM sys.dm_db_resource_stats ORDER BY cpu"
    )

    assert rows == [{"cpu": 41.5}]


@pytest.mark.asyncio
async def test_unparsed_statements_fail_unless_allowlisted_with_a_reason() -> None:
    broken = "SELECT FROM WHERE ((("

    with pytest.raises(UnparsedStatementError):
        await StrictDmvExecutor().fetch_all("appdb", broken)
    with pytest.raises(UnparsedStatementError):
        await StrictDmvExecutor(allowlist={"FROM WHERE": ""}).fetch_all("appdb", broken)
    assert await StrictDmvExecutor(allowlist={"FROM WHERE": "parser gap, checked live"}).fetch_all("appdb", broken) == []


def test_fixture_rows_must_use_documented_columns() -> None:
    with pytest.raises(KeyError):
        documented_row(GOVERNANCE, primary_max_log_rate_per_db_in_bytes_per_second=1)
    with pytest.raises(KeyError):
        StrictDmvExecutor({GEO_LINK: [{"synchronization_health_desc": "NOT_HEALTHY"}]})


# --- the code under contract --------------------------------------------------


@pytest.mark.asyncio
async def test_health_connection_check_uses_documented_columns() -> None:
    executor = strict_executor()

    payload = await HealthService(executor, FakeQueryStore()).analyze("appdb", "connection")  # type: ignore[arg-type]

    assert executor.violations == []
    check = payload["checks"]["connection"]
    assert check["details"]["session_limit"] == 30000
    assert check["details"]["session_limit_utilization_percent"] == 1.0


@pytest.mark.asyncio
async def test_health_replication_check_uses_documented_columns() -> None:
    link = documented_row(
        GEO_LINK,
        partner_server="secondary",
        partner_database="appdb",
        role_desc="PRIMARY",
        replication_state_desc="SUSPENDED",
        replication_lag_sec=0,
    )
    executor = strict_executor(sys__dm_geo_replication_link_status=[link])

    payload = await HealthService(executor, FakeQueryStore()).analyze("appdb", "replication")  # type: ignore[arg-type]

    assert executor.violations == []
    assert payload["checks"]["replication"]["status"] == "critical"


@pytest.mark.asyncio
async def test_every_health_check_uses_documented_columns() -> None:
    executor = strict_executor()

    payload = await HealthService(executor, FakeQueryStore()).analyze("appdb", "all")  # type: ignore[arg-type]

    assert executor.violations == []
    collected = [name for name, check in payload["checks"].items() if check["details"].get("error")]
    assert collected == []


@pytest.mark.asyncio
async def test_resource_limits_reports_log_cap_from_documented_row() -> None:
    executor = strict_executor()

    result = await ResourceGovernanceService(executor).get_resource_limits("appdb")  # type: ignore[arg-type]

    assert executor.violations == []
    assert result["governance_limits"]["max_log_rate_mb_per_sec"] == 48.0


@pytest.mark.asyncio
async def test_resource_governance_reads_use_documented_columns() -> None:
    executor = strict_executor()
    service = ResourceGovernanceService(executor)  # type: ignore[arg-type]

    await service.get_resource_limits("appdb")
    await service.get_io_stats("appdb")
    await service.get_resource_stats_history("appdb", 60)
    await service.get_resource_stats_history("appdb", 7 * 24 * 60, master_available=True)

    assert executor.violations == []


@pytest.mark.asyncio
async def test_version_store_reads_use_documented_columns() -> None:
    pvs = documented_row("sys.dm_tran_persistent_version_store_stats", database_id=5, persistent_version_store_size_kb=1024)
    executor = strict_executor(sys__dm_tran_persistent_version_store_stats=[pvs])

    result = await VersionStoreService(executor).get_version_store_stats("appdb")  # type: ignore[arg-type]

    assert executor.violations == []
    assert result["persistent_version_store"]["size_mb"] == 1.0


@pytest.mark.parametrize(
    "call",
    [
        lambda executor: TempdbMemoryService(executor).get_tempdb_usage("appdb"),
        lambda executor: TempdbMemoryService(executor).get_memory_grants("appdb"),
        lambda executor: DiagnosticQueryService(executor).get_database_configuration("appdb"),
        lambda executor: DiagnosticQueryService(executor).get_storage_diagnostics("appdb"),
        lambda executor: WaitStatsService(executor).get_wait_stats("appdb"),
        lambda executor: WaitStatsService(executor).get_currently_waiting_tasks("appdb"),
        lambda executor: PlanCacheService(executor).get_plan_cache_analysis("appdb"),
    ],
    ids=["tempdb", "memory_grants", "configuration", "storage", "waits", "waiting_tasks", "plan_cache"],
)
@pytest.mark.asyncio
async def test_other_diagnostic_reads_use_documented_columns(call) -> None:
    executor = strict_executor()

    await call(executor)

    assert executor.violations == []


@pytest.mark.asyncio
async def test_index_metadata_reads_use_documented_columns() -> None:
    # Usage counters, engine start time and partition sizes feed every index review.
    executor = StrictDmvExecutor()

    await collect_existing_indexes(executor, "appdb")  # type: ignore[arg-type]
    await executor.fetch_all("appdb", TABLES_SQL)

    assert executor.violations == []
    read = " ".join(query for _, query, _ in executor.calls)
    contract = documented_entries()
    for dmv in ("sys.dm_db_index_usage_stats", "sys.dm_os_sys_info", "sys.dm_db_partition_stats"):
        assert dmv in read and dmv in contract


@pytest.mark.parametrize("capture", sorted(LIVE_DIR.glob("*.json")), ids=lambda path: path.stem)
@pytest.mark.asyncio
async def test_owner_live_captures_cover_every_projected_column(capture: Path) -> None:
    contracts = live_contracts(capture)
    executor = StrictDmvExecutor(contracts=contracts, responses=[("sys.dm_exec_sessions", [])])

    await HealthService(executor, FakeQueryStore()).analyze("appdb", "all")  # type: ignore[arg-type]
    service = ResourceGovernanceService(executor)  # type: ignore[arg-type]
    await service.get_resource_limits("appdb")
    await service.get_io_stats("appdb")
    await service.get_resource_stats_history("appdb", 60)

    assert executor.violations == []
