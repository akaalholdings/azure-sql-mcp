"""Azure SQL Database compatibility and intelligent-query-processing evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .connection import AzureSqlExecutor


CAPABILITY_METADATA_SQL = """
SELECT
    d.name AS database_name,
    d.compatibility_level,
    d.is_auto_create_stats_on,
    d.is_auto_update_stats_on,
    d.is_auto_update_stats_async_on,
    d.is_parameterization_forced,
    CAST(SERVERPROPERTY('EngineEdition') AS int) AS engine_edition,
    CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)) AS product_version,
    CAST(DATABASEPROPERTYEX(DB_NAME(), 'Edition') AS nvarchar(128)) AS service_edition,
    CAST(DATABASEPROPERTYEX(DB_NAME(), 'ServiceObjective') AS nvarchar(128))
        AS service_objective,
    qso.actual_state_desc AS query_store_actual_state,
    qso.desired_state_desc AS query_store_desired_state,
    qso.readonly_reason AS query_store_readonly_reason,
    qso.current_storage_size_mb AS query_store_current_storage_size_mb,
    qso.max_storage_size_mb AS query_store_max_storage_size_mb,
    qso.interval_length_minutes AS query_store_interval_length_minutes,
    qso.stale_query_threshold_days AS query_store_stale_query_threshold_days,
    qso.query_capture_mode_desc AS query_store_capture_mode,
    qso.wait_stats_capture_mode_desc AS query_store_wait_stats_capture_mode,
    c.name AS configuration_name,
    c.value AS configuration_value,
    c.value_for_secondary AS configuration_value_for_secondary
FROM sys.databases AS d
LEFT JOIN sys.database_scoped_configurations AS c
    ON c.name IN (
        'PARAMETER_SENSITIVE_PLAN_OPTIMIZATION',
        'OPTIONAL_PARAMETER_OPTIMIZATION',
        'MEMORY_GRANT_FEEDBACK_PERSISTENCE',
        'MEMORY_GRANT_FEEDBACK_PERCENTILE_GRANT',
        'BATCH_MODE_MEMORY_GRANT_FEEDBACK',
        'ROW_MODE_MEMORY_GRANT_FEEDBACK',
        'BATCH_MODE_ADAPTIVE_JOINS',
        'BATCH_MODE_ON_ROWSTORE',
        'DEFERRED_COMPILATION_TV',
        'INTERLEAVED_EXECUTION_TVF',
        'TSQL_SCALAR_UDF_INLINING',
        'CE_FEEDBACK',
        'DOP_FEEDBACK'
    )
LEFT JOIN sys.database_query_store_options AS qso
    ON 1 = 1
WHERE d.name = DB_NAME()
"""

_CONFIG_NAMES = {
    "parameter_sensitive_plan_optimization": {
        "PARAMETER_SENSITIVE_PLAN_OPTIMIZATION"
    },
    "optional_parameter_optimization": {"OPTIONAL_PARAMETER_OPTIMIZATION"},
    "memory_grant_feedback": {
        "MEMORY_GRANT_FEEDBACK_PERSISTENCE",
        "MEMORY_GRANT_FEEDBACK_PERCENTILE_GRANT",
        "BATCH_MODE_MEMORY_GRANT_FEEDBACK",
        "ROW_MODE_MEMORY_GRANT_FEEDBACK",
    },
    "batch_mode_memory_grant_feedback": {"BATCH_MODE_MEMORY_GRANT_FEEDBACK"},
    "row_mode_memory_grant_feedback": {"ROW_MODE_MEMORY_GRANT_FEEDBACK"},
    "memory_grant_feedback_persistence": {"MEMORY_GRANT_FEEDBACK_PERSISTENCE"},
    "memory_grant_feedback_percentile": {
        "MEMORY_GRANT_FEEDBACK_PERCENTILE_GRANT"
    },
    "batch_mode_adaptive_joins": {"BATCH_MODE_ADAPTIVE_JOINS"},
    "batch_mode_on_rowstore": {"BATCH_MODE_ON_ROWSTORE"},
    "table_variable_deferred_compilation": {"DEFERRED_COMPILATION_TV"},
    "interleaved_execution_mstvf": {"INTERLEAVED_EXECUTION_TVF"},
    "scalar_udf_inlining": {"TSQL_SCALAR_UDF_INLINING"},
    "cardinality_estimation_feedback": {"CE_FEEDBACK"},
    "degree_of_parallelism_feedback": {"DOP_FEEDBACK"},
}

_MINIMUM_COMPATIBILITY_LEVEL = {
    "parameter_sensitive_plan_optimization": 160,
    "optional_parameter_optimization": 170,
    "memory_grant_feedback": 140,
    "batch_mode_memory_grant_feedback": 140,
    "row_mode_memory_grant_feedback": 150,
    "memory_grant_feedback_persistence": 150,
    "memory_grant_feedback_percentile": 150,
    "batch_mode_adaptive_joins": 140,
    "batch_mode_on_rowstore": 150,
    "table_variable_deferred_compilation": 150,
    "interleaved_execution_mstvf": 140,
    "scalar_udf_inlining": 150,
    "cardinality_estimation_feedback": 160,
    "degree_of_parallelism_feedback": 160,
}

_QUERY_STORE_READ_WRITE_REQUIRED = frozenset(
    {
        "memory_grant_feedback_persistence",
        "memory_grant_feedback_percentile",
        "cardinality_estimation_feedback",
        "degree_of_parallelism_feedback",
    }
)


def _as_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().casefold()
    if normalized in {"on", "enabled", "true", "1", "yes"}:
        return True
    if normalized in {"off", "disabled", "false", "0", "no"}:
        return False
    return None


def _observed_value(
    observed: Mapping[str, Any] | None,
    feature: str,
    rows: list[Mapping[str, Any]],
) -> Any:
    if observed and feature in observed:
        return observed[feature]
    candidates = {
        feature,
        f"{feature}_observed",
        f"observed_{feature}",
    }
    abbreviations = {
        "parameter_sensitive_plan_optimization": "psp_observed",
        "optional_parameter_optimization": "oppo_observed",
        "memory_grant_feedback": "memory_grant_feedback_observed",
        "cardinality_estimation_feedback": "ce_feedback_observed",
        "degree_of_parallelism_feedback": "dop_feedback_observed",
    }
    abbreviation = abbreviations.get(feature)
    if abbreviation:
        candidates.add(abbreviation)
    for row in rows:
        for key, value in row.items():
            if str(key).casefold() in {candidate.casefold() for candidate in candidates}:
                return value
    return None


def summarize_azure_sql_capabilities(
    metadata: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    observed: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Summarize capability evidence without prescribing a query hint."""
    rows = [metadata] if isinstance(metadata, Mapping) else list(metadata)
    first = rows[0] if rows else {}
    raw_compatibility_level = first.get("compatibility_level")
    try:
        compatibility_level = (
            int(str(raw_compatibility_level))
            if raw_compatibility_level is not None
            else None
        )
    except (TypeError, ValueError):
        compatibility_level = None

    configurations: dict[str, dict[str, Any]] = {}
    for row in rows:
        raw_name = row.get("configuration_name") or row.get("name")
        if not raw_name:
            continue
        name = str(raw_name).upper()
        configurations[name] = {
            "value": row.get("configuration_value", row.get("value")),
            "value_for_secondary": row.get(
                "configuration_value_for_secondary",
                row.get("value_for_secondary"),
            ),
        }

    query_store_actual_state = first.get("query_store_actual_state")
    query_store_read_write = (
        None
        if query_store_actual_state is None
        else str(query_store_actual_state).strip().casefold() == "read_write"
    )
    features: dict[str, dict[str, Any]] = {}
    for feature, config_names in _CONFIG_NAMES.items():
        minimum_level = _MINIMUM_COMPATIBILITY_LEVEL[feature]
        applicable = (
            None
            if compatibility_level is None
            else compatibility_level >= minimum_level
        )
        matching_configs = {
            name: configurations[name]
            for name in config_names
            if name in configurations
        }
        enabled_values = [
            _as_bool(config.get("value"))
            for config in matching_configs.values()
        ]
        enabled_values = [value for value in enabled_values if value is not None]
        enabled = (
            enabled_values[0]
            if enabled_values and len(set(enabled_values)) == 1
            else None
        )
        observed_raw = _observed_value(observed, feature, rows)
        requires_query_store_read_write = (
            feature in _QUERY_STORE_READ_WRITE_REQUIRED
        )
        features[feature] = {
            "applicable": applicable,
            "enabled": enabled,
            "observed": _as_bool(observed_raw),
            "observed_state": observed_raw,
            "configuration": matching_configs,
            "minimum_compatibility_level": minimum_level,
            "requires_query_store_read_write": requires_query_store_read_write,
            "prerequisites_met": (
                False
                if applicable is False
                else (
                    None
                    if applicable is None
                    else (
                        query_store_read_write
                        if requires_query_store_read_write
                        else True
                    )
                )
            ),
            "evidence": "compatibility_level_and_database_scoped_configuration",
        }

    engine_edition = first.get("engine_edition")
    try:
        normalized_engine_edition = (
            int(str(engine_edition)) if engine_edition is not None else None
        )
    except (TypeError, ValueError):
        normalized_engine_edition = None

    return {
        "platform": "azure_sql_database_paas",
        "platform_verified": (
            normalized_engine_edition == 5
            if normalized_engine_edition is not None
            else None
        ),
        "database_name": first.get("database_name"),
        "compatibility_level": compatibility_level,
        "engine_edition": normalized_engine_edition,
        "product_version": first.get("product_version"),
        "service_edition": first.get("service_edition"),
        "service_objective": first.get("service_objective"),
        "statistics_options": {
            "auto_create_statistics": _as_bool(first.get("is_auto_create_stats_on")),
            "auto_update_statistics": _as_bool(first.get("is_auto_update_stats_on")),
            "auto_update_statistics_async": _as_bool(
                first.get("is_auto_update_stats_async_on")
            ),
            "forced_parameterization": _as_bool(
                first.get("is_parameterization_forced")
            ),
        },
        "query_store": {
            "actual_state": query_store_actual_state,
            "desired_state": first.get("query_store_desired_state"),
            "read_write": query_store_read_write,
            "readonly_reason": first.get("query_store_readonly_reason"),
            "current_storage_size_mb": first.get(
                "query_store_current_storage_size_mb"
            ),
            "max_storage_size_mb": first.get("query_store_max_storage_size_mb"),
            "interval_length_minutes": first.get(
                "query_store_interval_length_minutes"
            ),
            "stale_query_threshold_days": first.get(
                "query_store_stale_query_threshold_days"
            ),
            "capture_mode": first.get("query_store_capture_mode"),
            "wait_stats_capture_mode": first.get(
                "query_store_wait_stats_capture_mode"
            ),
        },
        "database_scoped_configurations": configurations,
        "features": features,
        "hint_policy": {
            "compatibility_level_is_not_a_hint_recommendation": True,
            "message": "Use plan, runtime, and Query Store evidence before selecting a hint.",
        },
    }


class PlatformCapabilitiesService:
    """Fetch Azure SQL metadata and turn it into an evidence-only summary."""

    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_summary(self, database_name: str) -> dict[str, Any]:
        rows = await self.executor.fetch_all(database_name, CAPABILITY_METADATA_SQL)
        return summarize_azure_sql_capabilities(rows)

    async def get_database_capabilities(self, database_name: str) -> dict[str, Any]:
        """Descriptive alias for integrations that expose database capabilities."""
        return await self.get_summary(database_name)
