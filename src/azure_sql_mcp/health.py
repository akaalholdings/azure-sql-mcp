from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from typing import Awaitable
from typing import Callable

from .azure_tier import dmv_permission_hint
from .connection import AzureSqlExecutor
from .incident_log import note_exception
from .observability import sanitize_error_message
from .query_store import QueryStoreService
from .resource_governance import GOVERNANCE_DMV
from .resource_governance import GOVERNANCE_SQL
from .resource_governance import MEMORY_METRICS
from .resource_governance import MEMORY_NOTE
from .resource_governance import RECENT_SAMPLE_SECONDS
from .resource_governance import ResourceGovernanceService
from .resource_governance import log_rate_caps

HealthCheck = Callable[[str], Awaitable[dict[str, Any]]]

logger = logging.getLogger(__name__)

CHECK_NAMES = (
    "connection",
    "constraint",
    "replication",
    "identity",
    "query_store",
    "tuning",
    "resource",
    "storage",
    "statistics",
)

RETIRED_QUERY_HEALTH_CHECKS = frozenset({"index", "buffer"})

STATUS_SEVERITY = {
    "pass": 0,
    "warning": 1,
    "critical": 2,
}

# Resource pressure is judged on the full hour of 15-second samples: a single
# sample never escalates on its own.
RESOURCE_WINDOW_MINUTES = 60
PRESSURE_WARNING_P95 = 80.0
PRESSURE_CRITICAL_P95 = 95.0
PRESSURE_CRITICAL_MINUTES_ABOVE_95 = 5.0
# Below 20 samples (5 minutes) the nearest-rank p95 is the single maximum, so p95
# is not judged: the view holds only minutes after a failover, scale operation or
# serverless resume.
PRESSURE_MIN_P95_SAMPLES = 20
PRESSURE_METRICS = (
    ("avg_cpu_percent", "CPU"),
    ("avg_data_io_percent", "Data IO"),
    ("avg_log_write_percent", "Log write"),
    ("max_worker_percent", "Workers"),
    ("max_session_percent", "Sessions"),
    ("xtp_storage_percent", "In-Memory OLTP storage"),
)
PEAK_METRICS = (
    "avg_cpu_percent",
    "avg_data_io_percent",
    "avg_log_write_percent",
    "avg_memory_usage_percent",
    "xtp_storage_percent",
    "max_worker_percent",
    "max_session_percent",
)

SESSION_LIMITS_SQL = """
SELECT
    max_sessions,
    primary_group_max_workers
FROM sys.dm_user_db_resource_governance
WHERE database_id = DB_ID()
"""

GEO_LINK_SQL = """
SELECT
    link_guid,
    partner_server,
    partner_database,
    role_desc,
    replication_state_desc,
    replication_lag_sec,
    last_replication,
    last_commit,
    secondary_allow_connections_desc
FROM sys.dm_geo_replication_link_status
"""

REPLICA_STATES_SQL = """
SELECT
    synchronization_state_desc,
    synchronization_health_desc,
    is_suspended,
    secondary_lag_seconds,
    redo_queue_size
FROM sys.dm_database_replica_states
WHERE database_id = DB_ID()
  AND is_local = 0
"""


class HealthService:
    def __init__(
        self,
        executor: AzureSqlExecutor,
        query_store_service: QueryStoreService,
        resource_governance: ResourceGovernanceService | None = None,
    ):
        self.executor = executor
        self.query_store_service = query_store_service
        self.resource_governance = resource_governance or ResourceGovernanceService(executor)

    async def analyze(self, database_name: str, health_type: str) -> dict[str, Any]:
        requested = self._parse_requested_checks(health_type)
        handlers: dict[str, HealthCheck] = {
            "connection": self._connection_health,
            "constraint": self._constraint_health,
            "replication": self._replication_health,
            "identity": self._identity_health,
            "query_store": self._query_store_health,
            "tuning": self._tuning_health,
            "resource": self._resource_health,
            "storage": self._storage_health,
            "statistics": self._statistics_health,
        }

        payload: dict[str, Any] = {"database_name": database_name, "checks": {}}
        for check_name in CHECK_NAMES:
            if check_name not in requested:
                continue
            payload["checks"][check_name] = await self._run_check(
                check_name,
                database_name,
                handlers[check_name],
            )
        return payload

    def _parse_requested_checks(self, health_type: str) -> set[str]:
        normalized = health_type.lower()
        requested: set[str] = (
            set(CHECK_NAMES)
            if normalized == "all"
            else {part.strip() for part in normalized.split(",") if part.strip()}
        )
        invalid = requested - set(CHECK_NAMES)
        if invalid:
            retired = invalid & RETIRED_QUERY_HEALTH_CHECKS
            if retired:
                raise ValueError(
                    "PLE, buffer-cache ratio, and fragmentation are not query-health "
                    "classifiers. Use collect_performance_evidence for Azure SQL "
                    "resource, Query Store, wait, blocking, statistics, parameter "
                    "sensitivity, and regression evidence."
                )
            raise ValueError(
                f"Unsupported health_type values: {', '.join(sorted(invalid))}."
            )
        return requested

    async def _run_check(
        self,
        check_name: str,
        database_name: str,
        callback: HealthCheck,
    ) -> dict[str, Any]:
        try:
            return await callback(database_name)
        except Exception as exc:
            note_exception(exc, "health.check")
            return self._build_check(
                status="warning",
                details={"available": False, "error": sanitize_error_message(str(exc))},
                thresholds={},
                findings=[f"{check_name} health data could not be collected: {sanitize_error_message(str(exc))}"],
            )

    async def _index_health(self, database_name: str) -> dict[str, Any]:
        fragmentation_query = """
        SELECT TOP (10)
            OBJECT_SCHEMA_NAME(ips.object_id) AS schema_name,
            OBJECT_NAME(ips.object_id) AS table_name,
            i.name AS index_name,
            ips.avg_fragmentation_in_percent,
            ips.page_count
        FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS ips
        INNER JOIN sys.indexes AS i
            ON ips.object_id = i.object_id
           AND ips.index_id = i.index_id
        WHERE ips.page_count >= 1000
          AND i.name IS NOT NULL
        ORDER BY ips.avg_fragmentation_in_percent DESC
        """
        usage_query = """
        SELECT TOP (10)
            OBJECT_SCHEMA_NAME(i.object_id) AS schema_name,
            OBJECT_NAME(i.object_id) AS table_name,
            i.name AS index_name,
            COALESCE(ius.user_seeks, 0) AS user_seeks,
            COALESCE(ius.user_scans, 0) AS user_scans,
            COALESCE(ius.user_lookups, 0) AS user_lookups,
            COALESCE(ius.user_updates, 0) AS user_updates
        FROM sys.indexes AS i
        LEFT JOIN sys.dm_db_index_usage_stats AS ius
            ON i.object_id = ius.object_id
           AND i.index_id = ius.index_id
           AND ius.database_id = DB_ID()
        WHERE i.object_id > 100
          AND i.name IS NOT NULL
          AND i.is_primary_key = 0
          AND i.is_unique_constraint = 0
          AND COALESCE(ius.user_seeks, 0)
              + COALESCE(ius.user_scans, 0)
              + COALESCE(ius.user_lookups, 0) = 0
        ORDER BY COALESCE(ius.user_updates, 0) DESC, i.name
        """
        duplicate_query = """
        WITH IndexKeyCols AS (
            SELECT
                i.object_id,
                i.index_id,
                i.name AS index_name,
                i.type_desc AS index_type,
                STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns
            FROM sys.indexes AS i
            INNER JOIN sys.index_columns AS ic
                ON i.object_id = ic.object_id
               AND i.index_id = ic.index_id
            INNER JOIN sys.columns AS c
                ON ic.object_id = c.object_id
               AND ic.column_id = c.column_id
            WHERE ic.is_included_column = 0
              AND i.name IS NOT NULL
              AND i.type IN (1, 2)
            GROUP BY i.object_id, i.index_id, i.name, i.type_desc
        )
        SELECT
            OBJECT_SCHEMA_NAME(a.object_id) AS schema_name,
            OBJECT_NAME(a.object_id) AS table_name,
            a.index_name AS index_a,
            a.index_type AS type_a,
            b.index_name AS index_b,
            b.index_type AS type_b,
            a.key_columns
        FROM IndexKeyCols AS a
        INNER JOIN IndexKeyCols AS b
            ON a.object_id = b.object_id
           AND a.key_columns = b.key_columns
           AND a.index_id < b.index_id
        ORDER BY schema_name, table_name, a.index_name
        """

        fragmented_indexes = await self.executor.fetch_all(database_name, fragmentation_query)
        unused_indexes = await self.executor.fetch_all(database_name, usage_query)
        duplicate_indexes = await self.executor.fetch_all(database_name, duplicate_query)

        warning_fragmented = [
            row
            for row in fragmented_indexes
            if (self._to_float(row.get("avg_fragmentation_in_percent")) or 0.0) >= 30.0
        ]
        critical_fragmented = [
            row
            for row in fragmented_indexes
            if (self._to_float(row.get("avg_fragmentation_in_percent")) or 0.0) >= 50.0
        ]

        status = "pass"
        findings: list[str] = []
        if critical_fragmented:
            status = self._escalate(
                status,
                "critical",
                findings,
                (
                    f"{len(critical_fragmented)} indexes are at or above 50% fragmentation "
                    f"({self._describe_items(critical_fragmented, 'index_name')})."
                ),
            )
        elif warning_fragmented:
            status = self._escalate(
                status,
                "warning",
                findings,
                (
                    f"{len(warning_fragmented)} indexes are at or above 30% fragmentation "
                    f"({self._describe_items(warning_fragmented, 'index_name')})."
                ),
            )

        if unused_indexes:
            status = self._escalate(
                status,
                "warning",
                findings,
                (
                    f"{len(unused_indexes)} indexes show zero reads and still incur writes "
                    f"({self._describe_items(unused_indexes, 'index_name')})."
                ),
            )
        if duplicate_indexes:
            status = self._escalate(
                status,
                "warning",
                findings,
                (
                    f"{len(duplicate_indexes)} duplicate index pairs were detected "
                    f"({self._describe_items(duplicate_indexes, 'index_a')})."
                ),
            )

        return self._build_check(
            status=status,
            details={
                "fragmented_index_count": len(warning_fragmented),
                "unused_index_count": len(unused_indexes),
                "duplicate_index_count": len(duplicate_indexes),
                "fragmented_indexes": fragmented_indexes,
                "unused_indexes": unused_indexes,
                "duplicate_indexes": duplicate_indexes,
            },
            thresholds={
                "fragmentation_warning_percent": 30.0,
                "fragmentation_critical_percent": 50.0,
                "minimum_page_count": 1000,
                "unused_indexes_warning_count": 1,
                "duplicate_indexes_warning_count": 1,
            },
            findings=findings,
        )

    async def _buffer_health(self, database_name: str) -> dict[str, Any]:
        ratio_query = """
        SELECT
            CAST(a.cntr_value AS FLOAT) * 100.0
                / NULLIF(CAST(b.cntr_value AS FLOAT), 0) AS buffer_cache_hit_ratio
        FROM sys.dm_os_performance_counters AS a
        CROSS JOIN sys.dm_os_performance_counters AS b
        WHERE a.counter_name = 'Buffer cache hit ratio'
          AND a.object_name LIKE '%Buffer Manager%'
          AND b.counter_name = 'Buffer cache hit ratio base'
          AND b.object_name LIKE '%Buffer Manager%'
        """
        ple_query = """
        SELECT cntr_value AS page_life_expectancy_seconds
        FROM sys.dm_os_performance_counters
        WHERE counter_name = 'Page life expectancy'
          AND object_name LIKE '%Buffer Manager%'
        """

        ratio_rows, ratio_error = await self._fetch_optional_rows(database_name, ratio_query)
        ple_rows, ple_error = await self._fetch_optional_rows(database_name, ple_query)

        buffer_cache_hit_ratio = None
        if ratio_rows:
            buffer_cache_hit_ratio = self._round(self._to_float(ratio_rows[0].get("buffer_cache_hit_ratio")))

        page_life_expectancy_seconds = None
        if ple_rows:
            page_life_expectancy_seconds = self._to_int(
                ple_rows[0].get("page_life_expectancy_seconds")
            )

        status = "pass"
        findings: list[str] = []
        if buffer_cache_hit_ratio is not None:
            if buffer_cache_hit_ratio < 90.0:
                status = self._escalate(
                    status,
                    "critical",
                    findings,
                    (
                        "Buffer cache hit ratio "
                        f"({buffer_cache_hit_ratio}%) is below the critical threshold (90%)."
                    ),
                )
            elif buffer_cache_hit_ratio < 95.0:
                status = self._escalate(
                    status,
                    "warning",
                    findings,
                    (
                        "Buffer cache hit ratio "
                        f"({buffer_cache_hit_ratio}%) is below the warning threshold (95%)."
                    ),
                )

        if page_life_expectancy_seconds is not None and page_life_expectancy_seconds < 300:
            status = self._escalate(
                status,
                "warning",
                findings,
                (
                    "Page life expectancy "
                    f"({page_life_expectancy_seconds}s) is below the warning threshold (300s)."
                ),
            )

        if ratio_error:
            status = self._escalate(
                status,
                "warning",
                findings,
                f"Buffer cache hit ratio metric is unavailable: {ratio_error}",
            )
        if ple_error:
            status = self._escalate(
                status,
                "warning",
                findings,
                f"Page life expectancy metric is unavailable: {ple_error}",
            )

        return self._build_check(
            status=status,
            details={
                "available": buffer_cache_hit_ratio is not None
                or page_life_expectancy_seconds is not None,
                "buffer_cache_hit_ratio": buffer_cache_hit_ratio,
                "page_life_expectancy_seconds": page_life_expectancy_seconds,
            },
            thresholds={
                "buffer_cache_hit_ratio_warning": 95.0,
                "buffer_cache_hit_ratio_critical": 90.0,
                "page_life_expectancy_warning_seconds": 300,
            },
            findings=findings,
        )

    async def _connection_health(self, database_name: str) -> dict[str, Any]:
        query = """
        SELECT
            COUNT(*) AS total_sessions,
            SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS active_requests,
            SUM(CASE WHEN status = 'sleeping' AND open_transaction_count > 0 THEN 1 ELSE 0 END)
                AS idle_with_open_transaction,
            SUM(CASE WHEN status = 'sleeping' THEN 1 ELSE 0 END) AS idle_sessions
        FROM sys.dm_exec_sessions
        WHERE is_user_process = 1
        """
        rows = await self.executor.fetch_all(database_name, query)
        row = rows[0] if rows else {}

        total_sessions = self._to_int(row.get("total_sessions")) or 0
        active_requests = self._to_int(row.get("active_requests")) or 0
        idle_with_open_transaction = self._to_int(
            row.get("idle_with_open_transaction")
        ) or 0
        idle_sessions = self._to_int(row.get("idle_sessions")) or 0

        # The documented limits, read on their own so a tier-gated permission
        # failure loses only the limit, not the check.
        limit_rows, limit_error = await self._fetch_optional_rows(database_name, SESSION_LIMITS_SQL)
        limits = limit_rows[0] if limit_rows else {}
        session_limit = self._positive_int(limits.get("max_sessions"))
        worker_limit = self._positive_int(limits.get("primary_group_max_workers"))
        limit_reason = None
        if limit_error:
            limit_reason = (
                f"{GOVERNANCE_DMV} could not be read: {limit_error}. {dmv_permission_hint(GOVERNANCE_DMV)}"
            )

        # max_session_percent and max_worker_percent are the documented pressure
        # signal, as a percentage of this database's limits.
        pressure: dict[str, Any] = {}
        pressure_error = None
        try:
            history = await self.resource_governance.get_resource_stats_history(
                database_name, RESOURCE_WINDOW_MINUTES
            )
            summary = history.get("summary") or {}
            pressure = {
                metric: summary[metric]
                for metric in ("max_session_percent", "max_worker_percent")
                if metric in summary
            }
        except Exception as exc:
            note_exception(exc, "health.optional_rows")
            pressure_error = (
                "sys.dm_db_resource_stats could not be read: "
                f"{sanitize_error_message(str(exc))}. {dmv_permission_hint('sys.dm_db_resource_stats')}"
            )

        session_limit_utilization_percent = None
        if session_limit:
            session_limit_utilization_percent = self._round(
                total_sessions / session_limit * 100.0
            )

        status = "pass"
        findings: list[str] = []
        if session_limit_utilization_percent is not None:
            if session_limit_utilization_percent >= 95.0:
                status = self._escalate(
                    status,
                    "critical",
                    findings,
                    (
                        "Session usage is "
                        f"{session_limit_utilization_percent}% of the limit ({total_sessions}/{session_limit})."
                    ),
                )
            elif session_limit_utilization_percent >= 80.0:
                status = self._escalate(
                    status,
                    "warning",
                    findings,
                    (
                        "Session usage is "
                        f"{session_limit_utilization_percent}% of the limit ({total_sessions}/{session_limit})."
                    ),
                )

        units = {
            "max_session_percent": ("Sessions", f" of {session_limit} sessions" if session_limit else ""),
            "max_worker_percent": ("Workers", f" of {worker_limit} workers" if worker_limit else ""),
        }
        for metric, stats in pressure.items():
            label, of_limit = units[metric]
            assessed = self._pressure(label, stats, of_limit)
            if assessed:
                status = self._escalate(status, assessed[0], findings, assessed[1])

        if idle_with_open_transaction > 20:
            status = self._escalate(
                status,
                "warning",
                findings,
                (
                    f"{idle_with_open_transaction} idle sessions have open transactions; "
                    "these can block cleanup and prolong locks."
                ),
            )

        if session_limit is None and not pressure:
            reasons = "; ".join(reason for reason in (limit_reason, pressure_error) if reason)
            status = self._escalate(
                status,
                "warning",
                findings,
                "Session and worker limits are not available; this is not a pass."
                + (f" {reasons}" if reasons else ""),
            )
        elif pressure_error:
            status = self._escalate(
                status,
                "warning",
                findings,
                f"Session and worker pressure are not available; this is not a pass. {pressure_error}",
            )

        return self._build_check(
            status=status,
            details={
                "total_sessions": total_sessions,
                "active_requests": active_requests,
                "idle_with_open_transaction": idle_with_open_transaction,
                "idle_sessions": idle_sessions,
                "session_limit": session_limit,
                "session_limit_utilization_percent": session_limit_utilization_percent,
                "worker_limit": worker_limit,
                "session_limit_unavailable_reason": limit_reason,
                "pressure_unavailable_reason": pressure_error,
                "max_session_percent_peak": (pressure.get("max_session_percent") or {}).get("max"),
                "max_worker_percent_peak": (pressure.get("max_worker_percent") or {}).get("max"),
                "pressure_summary": pressure,
            },
            thresholds={
                "session_limit_warning_percent": 80.0,
                "session_limit_critical_percent": 95.0,
                "idle_with_open_transaction_warning": 20,
                "pressure_warning_p95": PRESSURE_WARNING_P95,
                "pressure_critical_p95": PRESSURE_CRITICAL_P95,
                "pressure_critical_minutes_above_95": PRESSURE_CRITICAL_MINUTES_ABOVE_95,
            },
            findings=findings,
        )

    async def _constraint_health(self, database_name: str) -> dict[str, Any]:
        query = """
        WITH TableRowCounts AS (
            SELECT
                object_id,
                SUM(row_count) AS row_count
            FROM sys.dm_db_partition_stats
            WHERE index_id IN (0, 1)
            GROUP BY object_id
        )
        SELECT
            OBJECT_SCHEMA_NAME(fk.parent_object_id) AS schema_name,
            OBJECT_NAME(fk.parent_object_id) AS table_name,
            fk.name AS constraint_name,
            'FOREIGN_KEY' AS constraint_type,
            fk.is_disabled,
            COALESCE(rc.row_count, 0) AS row_count
        FROM sys.foreign_keys AS fk
        LEFT JOIN TableRowCounts AS rc
            ON fk.parent_object_id = rc.object_id
        WHERE fk.is_not_trusted = 1

        UNION ALL

        SELECT
            OBJECT_SCHEMA_NAME(cc.parent_object_id) AS schema_name,
            OBJECT_NAME(cc.parent_object_id) AS table_name,
            cc.name AS constraint_name,
            'CHECK' AS constraint_type,
            cc.is_disabled,
            COALESCE(rc.row_count, 0) AS row_count
        FROM sys.check_constraints AS cc
        LEFT JOIN TableRowCounts AS rc
            ON cc.parent_object_id = rc.object_id
        WHERE cc.is_not_trusted = 1

        ORDER BY schema_name, table_name, constraint_name
        """
        rows = await self.executor.fetch_all(database_name, query)

        enriched_rows: list[dict[str, Any]] = []
        large_table_count = 0
        for row in rows:
            enriched = dict(row)
            row_count = self._to_int(row.get("row_count")) or 0
            if row_count > 100000:
                large_table_count += 1
            enriched["row_count"] = row_count
            enriched["remediation_sql"] = (
                "ALTER TABLE "
                f"[{row.get('schema_name')}].[{row.get('table_name')}] "
                f"WITH CHECK CHECK CONSTRAINT [{row.get('constraint_name')}]"
            )
            enriched_rows.append(enriched)

        status = "pass"
        findings: list[str] = []
        if enriched_rows:
            severity = "critical" if large_table_count else "warning"
            status = self._escalate(
                status,
                severity,
                findings,
                (
                    f"{len(enriched_rows)} untrusted constraints were found "
                    f"({self._describe_items(enriched_rows, 'constraint_name')})."
                ),
            )
            if large_table_count:
                findings.append(
                    f"{large_table_count} untrusted constraints are on tables with more than 100,000 rows."
                )

        return self._build_check(
            status=status,
            details={
                "untrusted_constraint_count": len(enriched_rows),
                "large_table_constraint_count": large_table_count,
                "untrusted_constraints": enriched_rows,
            },
            thresholds={
                "untrusted_constraint_warning_count": 1,
                "large_table_row_count_critical": 100000,
            },
            findings=findings,
        )

    async def _replication_health(self, database_name: str) -> dict[str, Any]:
        rows, error = await self._fetch_optional_rows(database_name, GEO_LINK_SQL)
        replicas, replica_error = await self._fetch_optional_rows(database_name, REPLICA_STATES_SQL)
        configured = bool(rows)

        status = "pass"
        findings: list[str] = []
        informational: list[str] = []
        seeding = False
        for row in rows:
            partner = row.get("partner_database") or "the secondary"
            state = str(row.get("replication_state_desc") or "").upper()
            lag_seconds = self._to_int(row.get("replication_lag_sec"))
            if state in {"SEEDING", "PENDING"}:
                seeding = True
                informational.append(
                    f"Geo-replication link to {partner} is {state}: the secondary is not synchronized yet."
                )
                continue
            if state == "SUSPENDED":
                status = self._escalate(
                    status,
                    "critical",
                    findings,
                    f"Geo-replication link to {partner} is SUSPENDED: data movement has stopped, "
                    "so the secondary falls further behind.",
                )
            elif lag_seconds is not None and lag_seconds > 120:
                status = self._escalate(
                    status,
                    "critical",
                    findings,
                    f"Geo-replication link to {partner} is {lag_seconds}s behind (critical above 120s).",
                )
            elif lag_seconds is not None and lag_seconds > 30:
                status = self._escalate(
                    status,
                    "warning",
                    findings,
                    f"Geo-replication link to {partner} is {lag_seconds}s behind (warning above 30s).",
                )
            unacknowledged = self._seconds_between(row.get("last_replication"), row.get("last_commit"))
            if (
                str(row.get("role_desc") or "").upper() == "PRIMARY"
                and unacknowledged is not None
                and unacknowledged > 300
            ):
                status = self._escalate(
                    status,
                    "critical",
                    findings,
                    f"Geo-replication link to {partner}: the last {unacknowledged:.0f}s of commits on the "
                    "primary are not acknowledged by the secondary (critical above 300s).",
                )

        if not seeding:
            for replica in replicas:
                if str(replica.get("synchronization_health_desc") or "").upper() == "NOT_HEALTHY":
                    status = self._escalate(
                        status,
                        "critical",
                        findings,
                        "A secondary replica of this database is NOT_HEALTHY "
                        f"(synchronization {replica.get('synchronization_state_desc') or 'unknown'}, "
                        f"{replica.get('secondary_lag_seconds')}s behind).",
                    )

        if error:
            status = self._escalate(
                status,
                "warning",
                findings,
                f"Geo-replication state could not be read; this is not a pass: {error}. "
                + dmv_permission_hint("sys.dm_geo_replication_link_status"),
            )

        details: dict[str, Any] = {
            "available": error is None,
            "configured": configured,
            "replication_link_count": len(rows),
            "links": rows,
            "replicas": replicas,
            "replicas_available": replica_error is None,
            "informational": informational,
        }
        if replica_error:
            details["replicas_unavailable_reason"] = (
                f"sys.dm_database_replica_states could not be read: {replica_error}. "
                + dmv_permission_hint("sys.dm_database_replica_states")
            )
        return self._build_check(
            status=status,
            details=details,
            thresholds={
                "replication_lag_warning_seconds": 30,
                "replication_lag_critical_seconds": 120,
                "synchronization_health_critical": "NOT_HEALTHY",
                "replication_state_critical": "SUSPENDED",
                "unacknowledged_commit_critical_seconds": 300,
            },
            findings=findings,
        )

    async def _identity_health(self, database_name: str) -> dict[str, Any]:
        query = """
        SELECT
            OBJECT_SCHEMA_NAME(ic.object_id) AS schema_name,
            OBJECT_NAME(ic.object_id) AS table_name,
            c.name AS column_name,
            t.name AS data_type,
            ic.seed_value,
            ic.increment_value,
            ic.last_value,
            CASE t.name
                WHEN 'tinyint' THEN 255
                WHEN 'smallint' THEN 32767
                WHEN 'int' THEN 2147483647
                WHEN 'bigint' THEN 9223372036854775807
            END AS max_value,
            CASE
                WHEN ic.last_value IS NULL THEN 0.0
                ELSE CAST(ic.last_value AS FLOAT)
                    / CAST(CASE t.name
                        WHEN 'tinyint' THEN 255
                        WHEN 'smallint' THEN 32767
                        WHEN 'int' THEN 2147483647
                        WHEN 'bigint' THEN 9223372036854775807
                    END AS FLOAT) * 100.0
            END AS pct_used
        FROM sys.identity_columns AS ic
        INNER JOIN sys.columns AS c
            ON ic.object_id = c.object_id
           AND ic.column_id = c.column_id
        INNER JOIN sys.types AS t
            ON c.user_type_id = t.user_type_id
        WHERE t.name IN ('tinyint', 'smallint', 'int', 'bigint')
        ORDER BY pct_used DESC
        """
        rows = await self.executor.fetch_all(database_name, query)

        reported_rows = []
        warning_rows = []
        critical_rows = []
        for row in rows:
            pct_used = self._round(self._to_float(row.get("pct_used")))
            if pct_used is None:
                continue

            data_type = str(row.get("data_type") or "").lower()
            if data_type == "int":
                if pct_used <= 60.0:
                    continue
            elif pct_used <= 80.0:
                continue

            enriched = dict(row)
            enriched["pct_used"] = pct_used
            reported_rows.append(enriched)
            if pct_used > 95.0:
                critical_rows.append(enriched)
            elif pct_used > 80.0:
                warning_rows.append(enriched)

        status = "pass"
        findings: list[str] = []
        if critical_rows:
            status = self._escalate(
                status,
                "critical",
                findings,
                (
                    f"{len(critical_rows)} identity columns are above 95% utilization "
                    f"({self._describe_items(critical_rows, 'column_name')})."
                ),
            )
        elif warning_rows:
            status = self._escalate(
                status,
                "warning",
                findings,
                (
                    f"{len(warning_rows)} identity columns are above 80% utilization "
                    f"({self._describe_items(warning_rows, 'column_name')})."
                ),
            )

        return self._build_check(
            status=status,
            details={
                "reported_column_count": len(reported_rows),
                "identity_columns": reported_rows,
            },
            thresholds={
                "int_monitoring_percent": 60.0,
                "identity_warning_percent": 80.0,
                "identity_critical_percent": 95.0,
            },
            findings=findings,
        )

    async def _query_store_health(self, database_name: str) -> dict[str, Any]:
        result = await self.query_store_service.get_status(database_name)
        details = dict(result)
        raw_status = result.get("status")
        status_row: dict[str, Any] = raw_status if isinstance(raw_status, dict) else {}

        storage_used_percent = None
        current_storage_size_mb = self._to_float(
            status_row.get("current_storage_size_mb")
        )
        max_storage_size_mb = self._to_float(status_row.get("max_storage_size_mb"))
        if max_storage_size_mb and current_storage_size_mb is not None:
            storage_used_percent = self._round(
                current_storage_size_mb / max_storage_size_mb * 100.0
            )

        details["storage_used_percent"] = storage_used_percent

        status = "pass"
        findings: list[str] = []
        if not result.get("enabled", False):
            status = self._escalate(
                status,
                "critical",
                findings,
                str(result.get("message") or "Query Store is not enabled."),
            )

        actual_state = str(status_row.get("actual_state_desc") or "")
        if actual_state == "ERROR":
            status = self._escalate(
                status,
                "critical",
                findings,
                "Query Store is in ERROR state.",
            )

        readonly_reason = status_row.get("readonly_reason")
        if readonly_reason not in (None, 0, "0"):
            status = self._escalate(
                status,
                "warning",
                findings,
                f"Query Store is read-only for reason code {readonly_reason}.",
            )

        if storage_used_percent is not None:
            if storage_used_percent >= 95.0:
                status = self._escalate(
                    status,
                    "critical",
                    findings,
                    f"Query Store storage is {storage_used_percent}% full.",
                )
            elif storage_used_percent >= 80.0:
                status = self._escalate(
                    status,
                    "warning",
                    findings,
                    f"Query Store storage is {storage_used_percent}% full.",
                )

        return self._build_check(
            status=status,
            details=details,
            thresholds={
                "storage_warning_percent": 80.0,
                "storage_critical_percent": 95.0,
            },
            findings=findings,
        )

    async def _tuning_health(self, database_name: str) -> dict[str, Any]:
        options_query = """
        SELECT
            name,
            desired_state_desc,
            actual_state_desc,
            reason_desc
        FROM sys.database_automatic_tuning_options
        ORDER BY name
        """
        recommendations_query = """
        SELECT COUNT(*) AS recommendation_count
        FROM sys.dm_db_tuning_recommendations
        """
        options = await self.executor.fetch_all(database_name, options_query)
        recommendation_rows = await self.executor.fetch_all(
            database_name,
            recommendations_query,
        )
        recommendation_count = (
            self._to_int(recommendation_rows[0].get("recommendation_count"))
            if recommendation_rows
            else 0
        ) or 0

        mismatched_options = [
            row
            for row in options
            if row.get("desired_state_desc") != row.get("actual_state_desc")
        ]
        errored_options = [
            row for row in options if str(row.get("actual_state_desc") or "") == "ERROR"
        ]

        status = "pass"
        findings: list[str] = []
        if errored_options:
            status = self._escalate(
                status,
                "critical",
                findings,
                (
                    f"{len(errored_options)} automatic tuning options are in ERROR "
                    f"({self._describe_items(errored_options, 'name')})."
                ),
            )
        elif mismatched_options:
            status = self._escalate(
                status,
                "warning",
                findings,
                (
                    f"{len(mismatched_options)} automatic tuning options differ from their desired state "
                    f"({self._describe_items(mismatched_options, 'name')})."
                ),
            )

        if recommendation_count > 0:
            status = self._escalate(
                status,
                "warning",
                findings,
                f"{recommendation_count} automatic tuning recommendations are pending review.",
            )

        return self._build_check(
            status=status,
            details={
                "options": options,
                "recommendation_count": recommendation_count,
            },
            thresholds={
                "recommendation_warning_count": 1,
            },
            findings=findings,
        )

    async def _resource_health(self, database_name: str) -> dict[str, Any]:
        history = await self.resource_governance.get_resource_stats_history(
            database_name, RESOURCE_WINDOW_MINUTES
        )
        rows: list[dict[str, Any]] = history.get("history") or []
        summary: dict[str, Any] = history.get("summary") or {}
        governance = await self._fetch_governance_limits(database_name)

        status = "pass"
        findings: list[str] = []
        if not rows:
            status = self._escalate(
                status,
                "warning",
                findings,
                "No rows were returned from sys.dm_db_resource_stats.",
            )

        for metric_name, label in PRESSURE_METRICS:
            stats = summary.get(metric_name)
            if not stats:
                continue
            assessed = self._pressure(label, stats, self._limit_units(metric_name, governance))
            if assessed:
                status = self._escalate(status, assessed[0], findings, assessed[1])

        # Memory near 100% is expected after ramp-up; it never sets the status.
        informational = [
            {
                "metric": metric_name,
                "avg": summary[metric_name]["avg"],
                "p95": summary[metric_name]["p95"],
                "max": summary[metric_name]["max"],
                "note": MEMORY_NOTE,
            }
            for metric_name in MEMORY_METRICS
            if metric_name in summary
        ]
        if 0 < len(rows) < PRESSURE_MIN_P95_SAMPLES:
            informational.append(
                {
                    "metric": "sample_window",
                    "sample_count": len(rows),
                    "note": (
                        f"Only {len(rows)} samples ({self._window_minutes(len(rows))} min) are retained, for "
                        "example after a failover, scale operation or serverless resume. p95 is judged from "
                        f"{PRESSURE_MIN_P95_SAMPLES} samples; peaks are in peak_usage."
                    ),
                }
            )

        return self._build_check(
            status=status,
            details={
                "recent_intervals": rows[:12],
                "peak_usage": {
                    metric_name: (summary.get(metric_name) or {}).get("max") for metric_name in PEAK_METRICS
                },
                "summary": {
                    metric_name: summary[metric_name]
                    for metric_name, _ in PRESSURE_METRICS
                    if metric_name in summary
                },
                "informational": informational,
                "governance_limits": governance,
                "window_minutes": RESOURCE_WINDOW_MINUTES,
                "sample_count": len(rows),
            },
            thresholds={
                "usage_warning_percent": PRESSURE_WARNING_P95,
                "usage_critical_percent": PRESSURE_CRITICAL_P95,
                "critical_minutes_above_95": PRESSURE_CRITICAL_MINUTES_ABOVE_95,
                "p95_min_samples": PRESSURE_MIN_P95_SAMPLES,
                "statistic": "p95 of 15-second samples over the last hour",
            },
            findings=findings,
        )

    # Documented columns of sys.dm_user_db_resource_governance (Microsoft Learn);
    # max_db_memory and checkpoint_rate_* are internal use only and never read.
    _GOVERNANCE_FIELDS = (
        "slo_name",
        "cpu_limit",
        "dtu_limit",
        "primary_group_max_cpu",
        "primary_max_log_rate",
        "pool_max_log_rate",
        "instance_max_log_rate",
        "primary_group_max_io",
        "pool_max_io",
        "primary_group_max_workers",
        "primary_pool_max_workers",
        "max_sessions",
        "max_dop",
        "max_transaction_size",
        "user_data_directory_space_quota_mb",
        "user_data_directory_space_usage_mb",
        "replica_role",
    )

    async def _fetch_governance_limits(
        self, database_name: str,
    ) -> dict[str, Any]:
        # SELECT * and project: column availability varies by tier, so a
        # missing column degrades to an absent field instead of failing the
        # probe. In an elastic pool the view has a row per pool database.
        try:
            rows = await self.executor.fetch_all(database_name, GOVERNANCE_SQL)
        except Exception as exc:
            # WARNING is below the ERROR-level swallow handler: record it here.
            note_exception(exc, "health.governance_limits")
            logger.warning(
                "Failed to fetch resource governance limits for '%s'",
                database_name,
                exc_info=True,
            )
            return {}
        if not rows:
            return {}
        row = rows[0]
        limits = {
            field: row.get(field)
            for field in self._GOVERNANCE_FIELDS
            if row.get(field) is not None
        }
        limits.update({key: value for key, value in log_rate_caps(row).items() if value is not None})
        return limits

    def _limit_units(self, metric_name: str, governance: dict[str, Any]) -> str:
        """The absolute limit a percentage refers to, as ' of <n> <unit>' (no new arithmetic)."""

        if metric_name == "avg_cpu_percent":
            vcores = self._to_float(governance.get("cpu_limit"))
            if vcores:
                return f" of {vcores:g} vCores"
            if governance.get("dtu_limit"):
                return f" of {governance['dtu_limit']} DTUs"
            return ""
        source = {
            "avg_log_write_percent": ("max_log_rate_mb_per_sec", "MB/s"),
            "avg_data_io_percent": ("primary_group_max_io", "IOPS"),
            "max_worker_percent": ("primary_group_max_workers", "workers"),
            "max_session_percent": ("max_sessions", "sessions"),
        }.get(metric_name)
        if source and governance.get(source[0]):
            return f" of {governance[source[0]]} {source[1]}"
        return ""

    def _pressure(self, label: str, stats: dict[str, Any], of_limit: str) -> tuple[str, str] | None:
        """Warning at p95 >= 80%; critical at p95 >= 95% or 5 minutes above 95%.

        p95 counts only from PRESSURE_MIN_P95_SAMPLES samples on.
        """

        p95 = self._to_float(stats.get("p95")) or 0.0
        samples = self._to_int(stats.get("total_samples")) or 0
        judged_p95 = p95 if samples >= PRESSURE_MIN_P95_SAMPLES else 0.0
        minutes_above_95 = self._to_float(stats.get("minutes_above_95")) or 0.0
        if judged_p95 >= PRESSURE_CRITICAL_P95 or minutes_above_95 >= PRESSURE_CRITICAL_MINUTES_ABOVE_95:
            severity = "critical"
        elif judged_p95 >= PRESSURE_WARNING_P95:
            severity = "warning"
        else:
            return None
        return severity, (
            f"{label} p95 was {p95}% of the limit over the last {self._window_minutes(samples)} min; peak "
            f"{self._to_float(stats.get('max'))}%{of_limit}, {minutes_above_95} min above 95%."
        )

    def _window_minutes(self, samples: int) -> float:
        return round(samples * RECENT_SAMPLE_SECONDS / 60.0, 1)

    async def _statistics_health(self, database_name: str) -> dict[str, Any]:
        query = """
        SELECT
            OBJECT_SCHEMA_NAME(sp.object_id) AS schema_name,
            OBJECT_NAME(sp.object_id) AS table_name,
            sp.stats_id,
            s.name AS statistics_name,
            sp.last_updated,
            sp.rows AS total_rows,
            sp.modification_counter,
            CASE
                WHEN sp.rows = 0 THEN 0.0
                ELSE CAST(sp.modification_counter * 100.0 / sp.rows AS DECIMAL(10, 2))
            END AS modification_pct
        FROM sys.stats AS s
        CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
        WHERE OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
          AND sp.rows > 0
          AND (
              sp.last_updated < DATEADD(DAY, -7, GETDATE())
              OR (sp.modification_counter * 100.0 / sp.rows) > 20.0
          )
        ORDER BY modification_pct DESC
        """
        rows, error = await self._fetch_optional_rows(database_name, query)

        status = "pass"
        findings: list[str] = []
        stale_count = 0
        high_mod_count = 0

        if error:
            status = self._escalate(
                status,
                "warning",
                findings,
                f"Could not query statistics health: {error}",
            )
        else:
            for row in rows:
                mod_pct = self._to_float(row.get("modification_pct")) or 0.0
                if mod_pct > 20.0:
                    high_mod_count += 1
            stale_count = len(rows)

            if high_mod_count >= 10:
                status = self._escalate(
                    status,
                    "critical",
                    findings,
                    f"{high_mod_count} statistics have >20% row modifications — query plans may be suboptimal.",
                )
            elif high_mod_count > 0:
                status = self._escalate(
                    status,
                    "warning",
                    findings,
                    f"{high_mod_count} statistics have >20% row modifications.",
                )

            if stale_count > high_mod_count:
                outdated = stale_count - high_mod_count
                status = self._escalate(
                    status,
                    "warning",
                    findings,
                    f"{outdated} statistics are older than 7 days.",
                )

        return self._build_check(
            status=status,
            details={
                "stale_or_modified_statistics": rows[:20],
                "stale_count": stale_count,
                "high_modification_count": high_mod_count,
            },
            thresholds={
                "stale_days": 7,
                "modification_warning_pct": 20.0,
                "high_mod_critical_count": 10,
            },
            findings=findings,
        )

    async def _storage_health(self, database_name: str) -> dict[str, Any]:
        query = """
        SELECT
            name,
            type_desc,
            CAST(size * 8.0 / 1024 AS DECIMAL(18, 2)) AS size_mb,
            CASE
                WHEN max_size = -1 THEN NULL
                ELSE CAST(max_size * 8.0 / 1024 AS DECIMAL(18, 2))
            END AS max_size_mb
        FROM sys.database_files
        ORDER BY file_id
        """
        rows = await self.executor.fetch_all(database_name, query)

        files = []
        status = "pass"
        findings: list[str] = []
        for row in rows:
            file_info = dict(row)
            size_mb = self._to_float(row.get("size_mb"))
            max_size_mb = self._to_float(row.get("max_size_mb"))
            used_percent = None
            if max_size_mb and size_mb is not None:
                used_percent = self._round(size_mb / max_size_mb * 100.0)
            if used_percent is not None:
                if used_percent >= 95.0:
                    status = self._escalate(
                        status,
                        "critical",
                        findings,
                        f"Database file {row.get('name')} is {used_percent}% full.",
                    )
                elif used_percent >= 80.0:
                    status = self._escalate(
                        status,
                        "warning",
                        findings,
                        f"Database file {row.get('name')} is {used_percent}% full.",
                    )
            file_info["used_percent"] = used_percent
            files.append(file_info)

        if not files:
            status = self._escalate(
                status,
                "warning",
                findings,
                "No database files were returned from sys.database_files.",
            )

        return self._build_check(
            status=status,
            details={"files": files},
            thresholds={
                "file_usage_warning_percent": 80.0,
                "file_usage_critical_percent": 95.0,
            },
            findings=findings,
        )

    async def _fetch_optional_rows(
        self,
        database_name: str,
        query: str,
    ) -> tuple[list[dict[str, Any]], str | None]:
        try:
            return await self.executor.fetch_all(database_name, query), None
        except Exception as exc:
            note_exception(exc, "health.optional_rows")
            return [], sanitize_error_message(str(exc))

    def _build_check(
        self,
        status: str,
        details: dict[str, Any],
        thresholds: dict[str, Any],
        findings: list[str],
    ) -> dict[str, Any]:
        return {
            "status": status,
            "details": details,
            "thresholds": thresholds,
            "findings": findings,
        }

    def _escalate(
        self,
        current_status: str,
        next_status: str,
        findings: list[str],
        message: str,
    ) -> str:
        findings.append(message)
        if STATUS_SEVERITY[next_status] > STATUS_SEVERITY[current_status]:
            return next_status
        return current_status

    def _describe_items(
        self,
        rows: list[dict[str, Any]],
        field_name: str,
        limit: int = 3,
    ) -> str:
        names = [
            str(row.get(field_name))
            for row in rows
            if row.get(field_name) is not None
        ]
        if not names:
            return "no named objects"
        preview = ", ".join(names[:limit])
        if len(names) > limit:
            preview += ", ..."
        return preview

    def _round(self, value: float | None, digits: int = 2) -> float | None:
        if value is None:
            return None
        return round(value, digits)

    def _to_float(self, value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _to_int(self, value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _positive_int(self, value: Any) -> int | None:
        number = self._to_int(value)
        return number if number and number > 0 else None

    def _seconds_between(self, start: Any, end: Any) -> float | None:
        try:
            first = start if isinstance(start, datetime) else datetime.fromisoformat(str(start))
            last = end if isinstance(end, datetime) else datetime.fromisoformat(str(end))
            return (last - first).total_seconds()
        except (TypeError, ValueError):
            return None
