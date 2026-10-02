"""Azure SQL Database service tier context and tier-aware DMV permission hints.

The only service-tier classifier in the package: other modules import
``classify_service_tier`` and ``read_tier_context`` instead of parsing service
objectives themselves.

Permission facts come from the Microsoft Learn Permissions section of each DMV
(``tests/fixtures/azure_dmv/columns.json``; a unit test keeps the sets below equal
to it). On Basic, S0, S1 and elastic-pool databases many DMVs need the server
admin, the Microsoft Entra admin or ``##MS_ServerStateReader##``; elsewhere
``VIEW DATABASE STATE`` is enough. That server role gives VIEW DATABASE STATE in
every database where its login has a user, so a hint names it with that scope and
with the alternative of accepting ``unavailable``; it is never a default.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from .connection import AzureSqlExecutor
from .incident_log import note_exception
from .observability import sanitize_error_message

TIER_SQL = """
SELECT
    CAST(DATABASEPROPERTYEX(DB_NAME(), 'Edition') AS nvarchar(128)) AS edition,
    CAST(DATABASEPROPERTYEX(DB_NAME(), 'ServiceObjective') AS nvarchar(128)) AS service_objective,
    (
        SELECT TOP (1) dso.elastic_pool_name
        FROM sys.database_service_objectives AS dso
        WHERE dso.database_id = DB_ID()
    ) AS elastic_pool_name
"""

POOL_USAGE_SQL = """
SELECT TOP (1)
    end_time,
    used_storage_mb,
    storage_limit_mb,
    used_storage_percent,
    avg_cpu_percent,
    avg_log_write_percent,
    max_worker_percent,
    avg_instance_cpu_percent
FROM sys.dm_elastic_pool_resource_stats
ORDER BY end_time DESC
"""

# Basic/S0/S1/elastic pool: server admin, Entra admin or ##MS_ServerStateReader##
# (##MS_ServerPerformanceStateReader## on two pages; ##MS_ServerStateReader## covers it).
SERVER_STATE_GATED_DMVS = frozenset(
    {
        "sys.dm_user_db_resource_governance",
        "sys.dm_tran_persistent_version_store_stats",
        "sys.dm_tran_active_snapshot_database_transactions",
        "sys.dm_tran_database_transactions",
        "sys.dm_db_session_space_usage",
        "sys.dm_db_task_space_usage",
        "sys.dm_os_out_of_memory_events",
        "sys.dm_exec_cached_plans",
        "sys.dm_exec_query_stats",
        "sys.dm_os_waiting_tasks",
    }
)
# A server-level permission on every service objective.
SERVER_SCOPED_DMVS = {
    "sys.dm_elastic_pool_resource_stats": "VIEW SERVER STATE",
    "sys.dm_db_tuning_recommendations": "VIEW SERVER PERFORMANCE STATE",
}
# Server admin or Entra admin on Basic and Standard; VIEW DATABASE STATE on Premium.
ADMIN_ONLY_ON_BASIC_STANDARD_DMVS = frozenset({"sys.dm_exec_query_statistics_xml"})

DATABASE_STATE_HINT = "VIEW DATABASE STATE is required."
_SERVER_ROLE = (
    "the server admin, the Microsoft Entra admin, or membership in the ##MS_ServerStateReader## "
    "server role (a login in master plus a user created FROM LOGIN in this database)"
)
_ROLE_SCOPE = (
    "##MS_ServerStateReader## gives VIEW DATABASE STATE in every database where that login has a "
    "user, so adding it is the owner's decision; the alternative is to accept that this source "
    "stays unavailable."
)

_EDITION_FAMILIES = {
    "basic": "basic",
    "standard": "standard",
    "premium": "premium",
    "generalpurpose": "general_purpose",
    "businesscritical": "business_critical",
    "hyperscale": "hyperscale",
}
_SERVERLESS_OBJECTIVE = re.compile(r"^(GP|HS)_S_", re.IGNORECASE)
_DTU_LOW_OBJECTIVES = frozenset({"S0", "S1"})
# Approximate steady-state IO latency (ms) from the vCore and DTU resource-limit tables.
_IO_LATENCY_MS: dict[str, dict[str, tuple[int, int]]] = {
    "general_purpose": {"read": (5, 10), "write": (5, 7)},
    "business_critical": {"read": (1, 2), "write": (1, 2)},
    "hyperscale": {"local_read": (1, 2), "remote_read": (1, 4), "write": (1, 4)},
    "basic": {"read": (5, 5), "write": (10, 10)},
    "standard": {"read": (5, 5), "write": (10, 10)},
    "premium": {"read": (2, 2), "write": (2, 2)},
}


@dataclass(frozen=True)
class TierContext:
    edition: str | None
    service_objective: str | None
    elastic_pool_name: str | None
    edition_family: str
    compute_model: str
    pooled: bool
    dtu_low_tier: bool
    server_state_reader_required: bool
    local_storage_governed: bool
    hyperscale: bool
    io_latency_expectation_ms: dict[str, tuple[int, int]] = field(default_factory=dict)


@dataclass(frozen=True)
class TierReading:
    tier: TierContext
    pool_usage: dict[str, Any] | None
    pool_usage_status: str
    pool_usage_reason: str | None = None


def classify_service_tier(
    edition: str | None,
    service_objective: str | None,
    elastic_pool_name: str | None,
) -> TierContext:
    """Classify an Azure SQL Database from Edition, ServiceObjective and pool name."""

    family = _EDITION_FAMILIES.get((edition or "").replace(" ", "").lower(), "unknown")
    objective = (service_objective or "").strip()
    pooled = bool(elastic_pool_name) or objective.lower() == "elasticpool"
    dtu_low_tier = family == "basic" or (family == "standard" and objective.upper() in _DTU_LOW_OBJECTIVES)
    if not objective:
        compute_model = "unknown"
    elif _SERVERLESS_OBJECTIVE.match(objective):
        compute_model = "serverless"
    else:
        compute_model = "provisioned"
    return TierContext(
        edition=edition,
        service_objective=service_objective,
        elastic_pool_name=elastic_pool_name,
        edition_family=family,
        compute_model=compute_model,
        pooled=pooled,
        dtu_low_tier=dtu_low_tier,
        server_state_reader_required=pooled or dtu_low_tier,
        local_storage_governed=family in {"premium", "business_critical"},
        hyperscale=family == "hyperscale",
        io_latency_expectation_ms=dict(_IO_LATENCY_MS.get(family, {})),
    )


async def read_tier_context(
    executor: AzureSqlExecutor,
    database_name: str,
    *,
    include_pool_usage: bool = False,
) -> TierReading:
    """Read the tier; with ``include_pool_usage``, the latest pool row for a pooled database."""

    rows = await executor.fetch_all(database_name, TIER_SQL)
    row = rows[0] if rows else {}
    tier = classify_service_tier(
        _text(row.get("edition")), _text(row.get("service_objective")), _text(row.get("elastic_pool_name"))
    )
    if not include_pool_usage:
        return TierReading(tier, None, "not_requested")
    if not tier.pooled:
        return TierReading(tier, None, "not_pooled")
    try:
        pool_rows = await executor.fetch_all(database_name, POOL_USAGE_SQL)
    except Exception as exc:
        note_exception(exc, "azure_tier.pool_usage")
        reason = (
            "sys.dm_elastic_pool_resource_stats could not be read: "
            f"{sanitize_error_message(str(exc))}. "
            + dmv_permission_hint("sys.dm_elastic_pool_resource_stats", tier)
        )
        return TierReading(tier, None, "unavailable", reason)
    if not pool_rows:
        return TierReading(tier, None, "empty")
    return TierReading(tier, dict(pool_rows[0]), "ok")


def dmv_permission_hint(dmv: str, tier: TierContext | None = None) -> str:
    """The grant that reads ``dmv`` on Azure SQL Database, for an ``unavailable`` reason."""

    if tier is not None and tier.edition_family == "unknown" and not tier.pooled:
        tier = None  # cannot rule out Basic, S0, S1 or a pool
    if dmv in SERVER_SCOPED_DMVS:
        return (
            f"{dmv} needs {SERVER_SCOPED_DMVS[dmv]}, which no database-level grant gives: it needs "
            f"{_SERVER_ROLE}. {_ROLE_SCOPE}"
        )
    if dmv in ADMIN_ONLY_ON_BASIC_STANDARD_DMVS:
        if tier is None:
            return (
                f"On Basic and Standard databases {dmv} needs the server admin or the Microsoft Entra "
                "admin account; elsewhere VIEW DATABASE STATE is enough."
            )
        if tier.edition_family in {"basic", "standard"}:
            return (
                f"This database is on {tier.edition}, so {dmv} needs the server admin or the "
                "Microsoft Entra admin account; VIEW DATABASE STATE is not enough here."
            )
        return DATABASE_STATE_HINT
    if dmv not in SERVER_STATE_GATED_DMVS:
        return DATABASE_STATE_HINT
    if tier is None:
        return (
            f"On Basic, S0, S1 and elastic-pool databases {dmv} needs {_SERVER_ROLE}; "
            f"elsewhere VIEW DATABASE STATE is enough. {_ROLE_SCOPE}"
        )
    if not tier.server_state_reader_required:
        return DATABASE_STATE_HINT
    where = "in an elastic pool" if tier.pooled else f"on the {tier.service_objective} service objective"
    return (
        f"This database is {where}, so {dmv} needs {_SERVER_ROLE}; VIEW DATABASE STATE is not "
        f"enough here. {_ROLE_SCOPE}"
    )


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
