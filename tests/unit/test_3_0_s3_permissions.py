"""A refused DMV read is ``unavailable`` with the tier-correct fix, never a raw tool error.

On Basic, S0, S1 and elastic-pool databases most diagnostic DMVs need a login in
``##MS_ServerStateReader##``; ``GRANT VIEW DATABASE STATE`` does nothing there. A
DBA who gets "permission was denied" with no path loses the incident window.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp import azure_tier
from azure_sql_mcp.azure_tier import classify_service_tier
from azure_sql_mcp.azure_tier import dmv_permission_hint
from azure_sql_mcp.azure_tier import dmv_permission_remediation
from azure_sql_mcp.azure_tier import gated_read_unavailable
from azure_sql_mcp.azure_tier import is_permission_error
from azure_sql_mcp.diagnostics import DiagnosticQueryService
from azure_sql_mcp.lock_diagnostics import LockDiagnosticsService
from azure_sql_mcp.resource_governance import ResourceGovernanceService
from azure_sql_mcp.sessions import SessionsService
from azure_sql_mcp.tempdb_memory import TempdbMemoryService
from azure_sql_mcp.version_store import VersionStoreService
from azure_sql_mcp.wait_stats import WaitStatsService

DENIED = "VIEW DATABASE STATE permission was denied in database 'appdb'. (262)"
POOLED = {"edition": "GeneralPurpose", "service_objective": "ElasticPool", "elastic_pool_name": "pool-a"}
SINGLE_GP = {"edition": "GeneralPurpose", "service_objective": "GP_Gen5_4", "elastic_pool_name": None}


class DeniedExecutor:
    """Refuses every read that names ``denied``; answers the tier read."""

    def __init__(self, denied: str, tier: dict[str, Any] | None = None, error: Exception | None = None) -> None:
        self.denied = denied
        self.tier = tier
        self.error = error or RuntimeError(DENIED)
        self.calls: list[str] = []

    async def fetch_all(self, database_name: str, query: str, params: Any = None, **_: Any) -> list[dict[str, Any]]:
        self.calls.append(query)
        if "DATABASEPROPERTYEX(DB_NAME(), 'Edition')" in query:
            if self.tier is None:
                raise RuntimeError("tier read failed")
            return [self.tier]
        if self.denied in query:
            raise self.error
        return []


def test_permission_errors_are_told_apart_from_other_failures() -> None:
    assert is_permission_error(RuntimeError(DENIED))
    assert is_permission_error(RuntimeError("The user does not have permission to perform this action. (297)"))
    assert is_permission_error(PermissionError("refused"))
    # A timeout or a bad column must stay a tool error: calling it a grant problem
    # would send the DBA to fix permissions that are fine.
    assert not is_permission_error(RuntimeError("Query timeout expired"))
    assert not is_permission_error(RuntimeError("Invalid column name 'x'. (207)"))


def test_remediation_follows_the_tier() -> None:
    pooled = classify_service_tier("GeneralPurpose", "ElasticPool", "pool-a")
    single = classify_service_tier("GeneralPurpose", "GP_Gen5_4", None)

    on_pool = dmv_permission_remediation("sys.dm_tran_locks", pooled)
    assert "ALTER SERVER ROLE ##MS_ServerStateReader## ADD MEMBER [<mcp_principal>]" in on_pool
    assert "CREATE USER [<mcp_principal>] FROM LOGIN [<mcp_principal>]" in on_pool
    assert "GRANT VIEW DATABASE STATE" not in on_pool

    on_single = dmv_permission_remediation("sys.dm_tran_locks", single)
    assert on_single == "GRANT VIEW DATABASE STATE TO [<mcp_principal>];"

    # Tier unknown: both paths, each labelled, so neither is run blind.
    unknown = dmv_permission_remediation("sys.dm_tran_locks", None)
    assert "GRANT VIEW DATABASE STATE" in unknown and "##MS_ServerStateReader##" in unknown


def test_tempdb_space_views_need_the_server_role_on_every_tier() -> None:
    # A contained database user has no user in tempdb, so no grant in the user
    # database opens tempdb space views, even on a tier where VIEW DATABASE STATE
    # is enough for everything else.
    single = classify_service_tier("GeneralPurpose", "GP_Gen5_4", None)

    hint = dmv_permission_hint("tempdb.sys.dm_db_file_space_usage", single)
    assert "contained database user" in hint
    assert "##MS_ServerStateReader##" in hint
    assert hint != "VIEW DATABASE STATE is required."
    remediation = dmv_permission_remediation("tempdb.sys.dm_db_file_space_usage", single)
    assert "FROM LOGIN" in remediation and "GRANT VIEW DATABASE STATE" not in remediation


def test_newly_read_gated_dmvs_are_in_the_gated_set() -> None:
    for dmv in (
        "sys.dm_tran_locks",
        "sys.dm_exec_query_resource_semaphores",
        "sys.dm_os_memory_clerks",
        "sys.dm_db_file_space_usage",
    ):
        assert dmv in azure_tier.SERVER_STATE_GATED_DMVS


@pytest.mark.asyncio
async def test_gated_read_unavailable_reads_the_tier_and_ignores_other_errors() -> None:
    executor = DeniedExecutor("sys.dm_tran_locks", POOLED)

    blocked = await gated_read_unavailable(executor, "appdb", "sys.dm_tran_locks", RuntimeError(DENIED), site="t")  # type: ignore[arg-type]
    assert blocked is not None
    assert blocked["result_status"] == "unavailable"
    assert blocked["available"] is False
    assert blocked["source_dmv"] == "sys.dm_tran_locks"
    assert "in an elastic pool" in blocked["result_status_reason"]
    assert "##MS_ServerStateReader##" in blocked["remediation"]

    other = await gated_read_unavailable(executor, "appdb", "sys.dm_tran_locks", RuntimeError("timeout"), site="t")  # type: ignore[arg-type]
    assert other is None


ROLE_PATH = "CREATE USER [<mcp_principal>] FROM LOGIN [<mcp_principal>]"
GRANT_PATH = "GRANT VIEW DATABASE STATE TO [<mcp_principal>];"

# (id, text that the refused statement holds, DMV named in the result, call, rows key, fix).
# The fix is the server role only for DMVs whose Learn page gates them on pools.
CASES = [
    (
        "lock_details",
        "sys.dm_tran_locks",
        "sys.dm_tran_locks",
        lambda e: LockDiagnosticsService(e).get_lock_details("appdb"),
        "locks",
        ROLE_PATH,
    ),
    (
        "open_transactions",
        "sys.dm_tran_active_transactions",
        "sys.dm_tran_database_transactions",
        lambda e: LockDiagnosticsService(e).get_open_transactions("appdb"),
        "transactions",
        ROLE_PATH,
    ),
    (
        "active_sessions",
        "sys.dm_exec_requests",
        "sys.dm_exec_requests",
        lambda e: SessionsService(e).get_active_sessions("appdb"),
        "sessions",
        GRANT_PATH,
    ),
    (
        "wait_stats",
        "sys.dm_db_wait_stats",
        "sys.dm_db_wait_stats",
        lambda e: WaitStatsService(e).get_wait_stats("appdb"),
        "top_waits",
        GRANT_PATH,
    ),
    (
        "waiting_tasks",
        "sys.dm_os_waiting_tasks",
        "sys.dm_os_waiting_tasks",
        lambda e: WaitStatsService(e).get_currently_waiting_tasks("appdb"),
        "waiting_tasks",
        ROLE_PATH,
    ),
    (
        "query_wait_stats",
        "sys.query_store_wait_stats",
        "sys.query_store_wait_stats",
        lambda e: WaitStatsService(e).get_query_wait_stats("appdb"),
        "query_wait_stats",
        GRANT_PATH,
    ),
    (
        "tempdb_usage",
        "sys.dm_db_session_space_usage",
        "sys.dm_db_session_space_usage",
        lambda e: TempdbMemoryService(e).get_tempdb_usage("appdb"),
        "sessions",
        ROLE_PATH,
    ),
    (
        "memory_grants",
        "sys.dm_exec_query_memory_grants",
        "sys.dm_exec_query_memory_grants",
        lambda e: TempdbMemoryService(e).get_memory_grants("appdb"),
        "grants",
        GRANT_PATH,
    ),
    (
        "io_stats",
        "sys.dm_io_virtual_file_stats",
        "sys.dm_io_virtual_file_stats",
        lambda e: ResourceGovernanceService(e).get_io_stats("appdb"),
        "files",
        GRANT_PATH,
    ),
    (
        "resource_history",
        "sys.dm_db_resource_stats",
        "sys.dm_db_resource_stats",
        lambda e: ResourceGovernanceService(e).get_resource_stats_history("appdb", 30),
        "history",
        GRANT_PATH,
    ),
]


@pytest.mark.parametrize(
    ("needle", "dmv", "call", "rows_key", "fix"), [case[1:] for case in CASES], ids=[case[0] for case in CASES]
)
@pytest.mark.asyncio
async def test_refused_read_on_a_pooled_database_is_unavailable_with_the_fix(needle, dmv, call, rows_key, fix) -> None:
    executor = DeniedExecutor(needle, POOLED)

    result = await call(executor)

    assert result["result_status"] == "unavailable"
    assert result["available"] is False
    assert result[rows_key] == []
    assert dmv in result["result_status_reason"]
    assert fix in result["remediation"]
    # The statement is for a DBA; the server never ran it.
    assert not any("ALTER SERVER ROLE" in query or "GRANT " in query for query in executor.calls)


@pytest.mark.parametrize(
    ("needle", "call"), [(case[1], case[3]) for case in CASES], ids=[case[0] for case in CASES]
)
@pytest.mark.asyncio
async def test_a_failure_that_is_not_a_permission_error_still_raises(needle, call) -> None:
    # Only a refusal has a grant fix. Anything else keeps its real error.
    executor = DeniedExecutor(needle, POOLED, error=RuntimeError("Query timeout expired"))

    with pytest.raises(RuntimeError, match="timeout"):
        await call(executor)


@pytest.mark.asyncio
async def test_tempdb_breakdown_refused_for_a_contained_user_names_the_login_path() -> None:
    # The single GP database is not gated, yet tempdb space is still unreadable
    # for a contained user: the remediation must not be GRANT VIEW DATABASE STATE.
    executor = DeniedExecutor("tempdb.sys.dm_db_file_space_usage", SINGLE_GP)

    result = await TempdbMemoryService(executor).get_tempdb_space_breakdown("appdb")  # type: ignore[arg-type]

    assert result["result_status"] == "unavailable"
    assert result["breakdown"] == {}
    assert "contained database user" in result["result_status_reason"]
    assert "GRANT VIEW DATABASE STATE" not in result["remediation"]
    assert "FROM LOGIN" in result["remediation"]


@pytest.mark.asyncio
async def test_version_store_refusal_uses_the_tier_and_carries_remediation() -> None:
    executor = DeniedExecutor("sys.dm_tran_persistent_version_store_stats", POOLED)

    result = await VersionStoreService(executor).get_version_store_stats("appdb")  # type: ignore[arg-type]

    assert result["result_status"] == "unavailable"
    assert "in an elastic pool" in result["result_status_reason"]
    assert "##MS_ServerStateReader##" in result["remediation"]


@pytest.mark.asyncio
async def test_a_refused_diagnostic_section_carries_the_permission_path() -> None:
    executor = DeniedExecutor("sys.dm_exec_query_stats", POOLED)

    result = await DiagnosticQueryService(executor).get_top_cached_queries("appdb")  # type: ignore[arg-type]

    section = result["cached_queries"]
    assert section["available"] is False
    assert section["result_status"] == "unavailable"
    assert "##MS_ServerStateReader##" in section["remediation"]
