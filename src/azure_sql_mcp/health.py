from __future__ import annotations

import logging
from typing import Any
from typing import Awaitable
from typing import Callable

from .connection import AzureSqlExecutor
from .observability import sanitize_error_message
from .query_store import QueryStoreService

HealthCheck = Callable[[str], Awaitable[dict[str, Any]]]

logger = logging.getLogger(__name__)

CHECK_NAMES = (
    "index",
    "buffer",
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

STATUS_SEVERITY = {
    "pass": 0,
    "warning": 1,
    "critical": 2,
}


class HealthService:
    def __init__(
        self,
        executor: AzureSqlExecutor,
        query_store_service: QueryStoreService,
    ):
        self.executor = executor
        self.query_store_service = query_store_service

    async def analyze(self, database_name: str, health_type: str) -> dict[str, Any]:
        requested = self._parse_requested_checks(health_type)
        handlers: dict[str, HealthCheck] = {
            "index": self._index_health,
            "buffer": self._buffer_health,
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
            SUM(CASE WHEN status = 'sleeping' THEN 1 ELSE 0 END) AS idle_sessions,
            COALESCE(
                TRY_CAST(
                    (SELECT TOP (1) drs.user_sessions_limit
                     FROM sys.dm_user_db_resource_governance AS drs) AS INT
                ),
                TRY_CAST(
                    (SELECT TOP (1) c.value_in_use
                     FROM sys.configurations AS c
                     WHERE c.name = 'user connections') AS INT
                )
            ) AS session_limit
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
        session_limit = self._to_int(row.get("session_limit"))
        if session_limit is not None and session_limit <= 0:
            session_limit = None

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

        return self._build_check(
            status=status,
            details={
                "total_sessions": total_sessions,
                "active_requests": active_requests,
                "idle_with_open_transaction": idle_with_open_transaction,
                "idle_sessions": idle_sessions,
                "session_limit": session_limit,
                "session_limit_utilization_percent": session_limit_utilization_percent,
            },
            thresholds={
                "session_limit_warning_percent": 80.0,
                "session_limit_critical_percent": 95.0,
                "idle_with_open_transaction_warning": 20,
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
        query = """
        SELECT
            ag.name AS replication_group,
            drs.partner_server,
            drs.partner_database,
            drs.role_desc,
            drs.replication_state_desc,
            drs.synchronization_health_desc,
            drs.replication_lag_sec,
            drs.last_replication
        FROM sys.dm_geo_replication_link_status AS drs
        LEFT JOIN sys.availability_groups AS ag
            ON drs.group_id = ag.group_id
        """
        rows, error = await self._fetch_optional_rows(database_name, query)
        configured = bool(rows)

        status = "pass"
        findings: list[str] = []
        critical_links = []
        warning_links = []
        for row in rows:
            lag_seconds = self._to_int(row.get("replication_lag_sec")) or 0
            replication_state = str(row.get("replication_state_desc") or "")
            sync_health = str(row.get("synchronization_health_desc") or "")
            if (
                replication_state != "SEEDING"
                and sync_health == "NOT_HEALTHY"
            ) or lag_seconds > 120:
                critical_links.append(row)
            elif lag_seconds > 30:
                warning_links.append(row)

        if critical_links:
            status = self._escalate(
                status,
                "critical",
                findings,
                (
                    f"{len(critical_links)} geo-replication links are unhealthy or over 120s behind "
                    f"({self._describe_items(critical_links, 'partner_database')})."
                ),
            )
        elif warning_links:
            status = self._escalate(
                status,
                "warning",
                findings,
                (
                    f"{len(warning_links)} geo-replication links are over 30s behind "
                    f"({self._describe_items(warning_links, 'partner_database')})."
                ),
            )

        if error:
            findings.append(f"Geo-replication DMV is unavailable: {error}")

        return self._build_check(
            status=status,
            details={
                "available": error is None,
                "configured": configured,
                "replication_link_count": len(rows),
                "links": rows,
            },
            thresholds={
                "replication_lag_warning_seconds": 30,
                "replication_lag_critical_seconds": 120,
                "synchronization_health_critical": "NOT_HEALTHY",
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
        query = """
        SELECT TOP (12)
            end_time,
            avg_cpu_percent,
            avg_data_io_percent,
            avg_log_write_percent,
            avg_memory_usage_percent,
            xtp_storage_percent,
            max_worker_percent,
            max_session_percent,
            dtu_limit
        FROM sys.dm_db_resource_stats
        ORDER BY end_time DESC
        """
        rows = await self.executor.fetch_all(database_name, query)

        governance = await self._fetch_governance_limits(database_name)

        metric_names = (
            "avg_cpu_percent",
            "avg_data_io_percent",
            "avg_log_write_percent",
            "avg_memory_usage_percent",
            "xtp_storage_percent",
            "max_worker_percent",
            "max_session_percent",
        )
        peak_usage: dict[str, float | None] = {}
        for metric_name in metric_names:
            peak_usage[metric_name] = self._peak(rows, metric_name)

        status = "pass"
        findings: list[str] = []
        if not rows:
            status = self._escalate(
                status,
                "warning",
                findings,
                "No rows were returned from sys.dm_db_resource_stats.",
            )

        for metric_name, peak_value in peak_usage.items():
            if peak_value is None:
                continue
            if peak_value >= 95.0:
                status = self._escalate(
                    status,
                    "critical",
                    findings,
                    f"{self._label(metric_name)} peaked at {peak_value}%.",
                )
            elif peak_value >= 80.0:
                status = self._escalate(
                    status,
                    "warning",
                    findings,
                    f"{self._label(metric_name)} peaked at {peak_value}%.",
                )

        governance_findings = self._compare_against_governance(peak_usage, governance)
        for severity, message in governance_findings:
            status = self._escalate(status, severity, findings, message)

        return self._build_check(
            status=status,
            details={
                "recent_intervals": rows,
                "peak_usage": peak_usage,
                "governance_limits": governance,
            },
            thresholds={
                "usage_warning_percent": 80.0,
                "usage_critical_percent": 95.0,
            },
            findings=findings,
        )

    async def _fetch_governance_limits(
        self, database_name: str,
    ) -> dict[str, Any]:
        query = """
        SELECT TOP 1
            primary_max_cpu_percent,
            primary_max_log_rate_per_db_in_bytes_per_second,
            primary_group_max_io,
            pool_max_io,
            max_db_memory,
            primary_max_worker_count_for_single_query AS max_workers_per_query,
            checkpoint_rate_mbps
        FROM sys.dm_user_db_resource_governance
        """
        try:
            rows = await self.executor.fetch_all(database_name, query)
            return rows[0] if rows else {}
        except Exception:
            logger.warning(
                "Failed to fetch resource governance limits for '%s'",
                database_name,
                exc_info=True,
            )
            return {}

    def _compare_against_governance(
        self,
        peak_usage: dict[str, float | None],
        governance: dict[str, Any],
    ) -> list[tuple[str, str]]:
        results: list[tuple[str, str]] = []
        if not governance:
            return results

        max_cpu = self._to_float(governance.get("primary_max_cpu_percent"))
        peak_cpu = peak_usage.get("avg_cpu_percent")
        if max_cpu and peak_cpu:
            pct_of_limit = round(peak_cpu / max_cpu * 100, 1)
            if pct_of_limit >= 95:
                results.append((
                    "critical",
                    f"CPU at {pct_of_limit}% of governance limit ({peak_cpu}% / {max_cpu}%).",
                ))
            elif pct_of_limit >= 80:
                results.append((
                    "warning",
                    f"CPU at {pct_of_limit}% of governance limit ({peak_cpu}% / {max_cpu}%).",
                ))

        max_io = self._to_int(governance.get("primary_group_max_io"))
        peak_io = peak_usage.get("avg_data_io_percent")
        if max_io and peak_io and peak_io >= 80:
            results.append((
                "warning",
                f"Data I/O peaked at {peak_io}% with governance IOPS limit of {max_io}.",
            ))

        max_memory_kb = self._to_int(governance.get("max_db_memory"))
        peak_memory = peak_usage.get("avg_memory_usage_percent")
        if max_memory_kb and peak_memory and peak_memory >= 80:
            max_memory_mb = round(max_memory_kb / 1024, 0)
            results.append((
                "warning",
                f"Memory peaked at {peak_memory}% with governance limit of {max_memory_mb} MB.",
            ))

        return results

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

    def _peak(self, rows: list[dict[str, Any]], field_name: str) -> float | None:
        peak_value = None
        for row in rows:
            current_value = self._to_float(row.get(field_name))
            if current_value is None:
                continue
            peak_value = current_value if peak_value is None else max(peak_value, current_value)
        return self._round(peak_value)

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

    def _label(self, field_name: str) -> str:
        return field_name.replace("_", " ")

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
