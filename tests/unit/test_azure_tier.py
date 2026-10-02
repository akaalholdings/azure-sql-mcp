from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp import azure_tier
from azure_sql_mcp.azure_tier import classify_service_tier
from azure_sql_mcp.azure_tier import dmv_permission_hint
from azure_sql_mcp.azure_tier import read_tier_context
from tests.azure_dmv_contract import documented_entries


@pytest.mark.parametrize(
    ("edition", "objective", "pool", "required"),
    [
        ("Basic", "Basic", None, True),
        ("Standard", "S0", None, True),
        ("Standard", "S1", None, True),
        ("Standard", "S2", None, False),
        ("Premium", "P1", None, False),
        ("GeneralPurpose", "GP_Gen5_2", None, False),
        ("GeneralPurpose", "ElasticPool", "pool-a", True),
        ("Standard", "ElasticPool", None, True),
    ],
)
def test_server_state_reader_is_required_on_basic_s0_s1_and_pools(edition, objective, pool, required) -> None:
    # Microsoft Learn: on Basic, S0, S1 and elastic-pool databases the tier-gated DMVs
    # need the server admin, the Entra admin or ##MS_ServerStateReader##.
    assert classify_service_tier(edition, objective, pool).server_state_reader_required is required


def test_pooled_is_read_from_the_objective_when_the_pool_name_is_hidden() -> None:
    # sys.database_service_objectives shows no row to a non-owner; the objective still says ElasticPool.
    tier = classify_service_tier("GeneralPurpose", "ElasticPool", None)

    assert tier.pooled is True
    assert tier.edition_family == "general_purpose"


@pytest.mark.parametrize(
    ("edition", "objective", "model"),
    [
        ("GeneralPurpose", "GP_S_Gen5_2", "serverless"),
        ("Hyperscale", "HS_S_Gen5_4", "serverless"),
        ("GeneralPurpose", "GP_Gen5_2", "provisioned"),
        ("Hyperscale", "HS_Gen5_4", "provisioned"),
        ("GeneralPurpose", "ElasticPool", "provisioned"),
    ],
)
def test_compute_model(edition, objective, model) -> None:
    assert classify_service_tier(edition, objective, None).compute_model == model


def test_storage_and_latency_expectations_follow_the_resource_limit_tables() -> None:
    hyperscale = classify_service_tier("Hyperscale", "HS_Gen5_4", None)
    business_critical = classify_service_tier("BusinessCritical", "BC_Gen5_8", None)
    general_purpose = classify_service_tier("GeneralPurpose", "GP_Gen5_2", None)
    standard = classify_service_tier("Standard", "S3", None)

    assert hyperscale.hyperscale is True and hyperscale.local_storage_governed is False
    assert business_critical.local_storage_governed is True
    assert classify_service_tier("Premium", "P2", None).local_storage_governed is True
    assert general_purpose.local_storage_governed is False
    assert general_purpose.io_latency_expectation_ms == {"read": (5, 10), "write": (5, 7)}
    assert business_critical.io_latency_expectation_ms == {"read": (1, 2), "write": (1, 2)}
    assert hyperscale.io_latency_expectation_ms == {"local_read": (1, 2), "remote_read": (1, 4), "write": (1, 4)}
    assert standard.io_latency_expectation_ms == {"read": (5, 5), "write": (10, 10)}


def test_unknown_tier_is_not_guessed() -> None:
    tier = classify_service_tier(None, None, None)

    assert tier.edition_family == "unknown"
    assert tier.server_state_reader_required is False
    assert tier.io_latency_expectation_ms == {}


class TierExecutor:
    def __init__(self, tier_row: dict[str, Any], pool: list[dict[str, Any]] | Exception | None = None) -> None:
        self.tier_row = tier_row
        self.pool = pool
        self.queries: list[str] = []

    async def fetch_all(self, database_name: str, query: str, *_: Any, **__: Any) -> list[dict[str, Any]]:
        self.queries.append(query)
        if "dm_elastic_pool_resource_stats" in query:
            if isinstance(self.pool, Exception):
                raise self.pool
            return self.pool or []
        return [self.tier_row]


@pytest.mark.asyncio
async def test_read_tier_context_reads_the_latest_pool_row() -> None:
    executor = TierExecutor(
        {"edition": "GeneralPurpose", "service_objective": "ElasticPool", "elastic_pool_name": "pool-a"},
        [{"end_time": "2026-10-02T10:00:00", "used_storage_mb": 900, "storage_limit_mb": 1000, "used_storage_percent": 90.0}],
    )

    reading = await read_tier_context(executor, "appdb", include_pool_usage=True)  # type: ignore[arg-type]

    assert reading.tier.pooled is True
    assert reading.pool_usage_status == "ok"
    assert reading.pool_usage == {"end_time": "2026-10-02T10:00:00", "used_storage_mb": 900, "storage_limit_mb": 1000, "used_storage_percent": 90.0}
    assert "DATABASEPROPERTYEX" in executor.queries[0]


@pytest.mark.asyncio
async def test_read_tier_context_pool_usage_permission_denied_is_unavailable_with_hint() -> None:
    executor = TierExecutor(
        {"edition": "Standard", "service_objective": "ElasticPool", "elastic_pool_name": None},
        PermissionError("VIEW SERVER STATE permission was denied"),
    )

    reading = await read_tier_context(executor, "appdb", include_pool_usage=True)  # type: ignore[arg-type]

    assert reading.pool_usage is None
    assert reading.pool_usage_status == "unavailable"
    assert reading.pool_usage_reason is not None
    assert "VIEW SERVER STATE" in reading.pool_usage_reason
    assert "##MS_ServerStateReader##" in reading.pool_usage_reason


@pytest.mark.asyncio
async def test_pool_usage_is_not_read_for_single_databases_or_by_default() -> None:
    single = TierExecutor({"edition": "GeneralPurpose", "service_objective": "GP_Gen5_2", "elastic_pool_name": None})
    pooled = TierExecutor({"edition": "GeneralPurpose", "service_objective": "ElasticPool", "elastic_pool_name": "p"})

    assert (await read_tier_context(single, "appdb", include_pool_usage=True)).pool_usage_status == "not_pooled"  # type: ignore[arg-type]
    assert (await read_tier_context(pooled, "appdb")).pool_usage_status == "not_requested"  # type: ignore[arg-type]
    assert not any("dm_elastic_pool_resource_stats" in query for query in single.queries + pooled.queries)


def test_gated_hint_names_the_role_its_scope_and_the_alternative() -> None:
    hint = dmv_permission_hint("sys.dm_exec_query_stats")

    assert "##MS_ServerStateReader##" in hint
    assert "elastic-pool" in hint
    assert "VIEW DATABASE STATE" in hint
    # The role reaches every database where the login has a user: an owner decision, not a default.
    assert "every database" in hint
    assert "unavailable" in hint


def test_gated_hint_follows_the_known_tier() -> None:
    pooled = classify_service_tier("GeneralPurpose", "ElasticPool", "pool-a")
    provisioned = classify_service_tier("GeneralPurpose", "GP_Gen5_2", None)

    assert dmv_permission_hint("sys.dm_exec_query_stats", provisioned) == "VIEW DATABASE STATE is required."
    pooled_hint = dmv_permission_hint("sys.dm_exec_query_stats", pooled)
    assert "elastic pool" in pooled_hint and "not enough" in pooled_hint
    assert "##MS_ServerStateReader##" in pooled_hint


def test_an_unknown_tier_gets_the_full_gated_hint() -> None:
    # Without Edition and ServiceObjective the tier cannot rule out Basic/S0/S1 or a pool.
    unknown = classify_service_tier(None, None, None)

    assert dmv_permission_hint("sys.dm_exec_query_stats", unknown) == dmv_permission_hint("sys.dm_exec_query_stats")


def test_ungated_and_special_hints() -> None:
    assert dmv_permission_hint("sys.dm_db_resource_stats") == "VIEW DATABASE STATE is required."
    assert dmv_permission_hint("Query Store runtime statistics") == "VIEW DATABASE STATE is required."
    standard = classify_service_tier("Standard", "S3", None)
    live = dmv_permission_hint("sys.dm_exec_query_statistics_xml", standard)
    assert "server admin" in live and "Microsoft Entra admin" in live
    assert dmv_permission_hint("sys.dm_exec_query_statistics_xml", classify_service_tier("Premium", "P1", None)) == (
        "VIEW DATABASE STATE is required."
    )


@pytest.mark.parametrize(
    ("dmv", "gated"),
    [
        ("sys.dm_db_index_usage_stats", True),
        ("sys.dm_os_sys_info", True),
        ("sys.dm_db_partition_stats", False),
    ],
)
def test_existing_index_metadata_dmvs_follow_their_documented_permissions(dmv, gated) -> None:
    # collect_existing_indexes reads all three. Microsoft Learn: usage stats and
    # sys_info need the server role on Basic, S0, S1 and pools; partition stats
    # needs VIEW DATABASE STATE (and VIEW DEFINITION) on every tier.
    s0 = classify_service_tier("Standard", "S0", None)
    provisioned = classify_service_tier("GeneralPurpose", "GP_Gen5_2", None)

    hint = dmv_permission_hint(dmv, s0)

    assert ("##MS_ServerStateReader##" in hint and "not enough" in hint) is gated
    assert dmv_permission_hint(dmv, provisioned) == "VIEW DATABASE STATE is required."


def test_permission_sets_match_the_documented_permissions() -> None:
    entries = documented_entries()
    gated = {name for name, entry in entries.items() if entry["permission"].get("basic_s0_s1_pool")}
    server_scoped = {
        name: entry["permission"]["default"]
        for name, entry in entries.items()
        if entry["permission"]["default"].startswith("VIEW SERVER")
    }
    admin_on_basic_standard = {name for name, entry in entries.items() if entry["permission"].get("basic_standard")}

    assert azure_tier.SERVER_STATE_GATED_DMVS == gated
    assert azure_tier.SERVER_SCOPED_DMVS == server_scoped
    assert azure_tier.ADMIN_ONLY_ON_BASIC_STANDARD_DMVS == admin_on_basic_standard


def test_no_other_module_classifies_serverless_objectives() -> None:
    # One classifier: W1 S7, W5 S13 and the index advisor import azure_tier instead.
    source_dir = Path(azure_tier.__file__).parent
    offenders = [
        path.name
        for path in source_dir.glob("*.py")
        if path.name != "azure_tier.py" and ("(GP|HS)_S_" in path.read_text() or "GP_S_" in path.read_text())
    ]
    assert offenders == []
