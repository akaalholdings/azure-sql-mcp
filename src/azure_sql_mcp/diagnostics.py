from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from .connection import AzureSqlExecutor
from .observability import sanitize_error_message


class DiagnosticQueryService:
    """Azure SQL DB-safe diagnostics adapted from Glenn Berry-style checks."""

    MAX_LIMIT = 100

    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_database_configuration(self, database_name: str) -> dict[str, Any]:
        version = await self._section(
            database_name,
            """
            SELECT
                @@SERVERNAME AS server_name,
                @@VERSION AS sql_version,
                SERVERPROPERTY('EngineEdition') AS engine_edition,
                SERVERPROPERTY('ProductVersion') AS product_version,
                SERVERPROPERTY('ProductLevel') AS product_level,
                SERVERPROPERTY('Edition') AS edition
            """,
        )
        instance_configurations = await self._section(
            database_name,
            """
            SELECT
                name,
                value,
                value_in_use,
                minimum,
                maximum,
                description,
                is_dynamic,
                is_advanced
            FROM sys.configurations
            ORDER BY name
            """,
        )
        database_properties = await self._section(
            database_name,
            """
            SELECT
                db.name AS database_name,
                db.recovery_model_desc,
                db.state_desc,
                db.containment_desc,
                db.log_reuse_wait_desc,
                db.compatibility_level,
                db.page_verify_option_desc,
                db.is_auto_create_stats_on,
                db.is_auto_update_stats_on,
                db.is_auto_update_stats_async_on,
                db.is_parameterization_forced,
                db.snapshot_isolation_state_desc,
                db.is_read_committed_snapshot_on,
                db.is_auto_close_on,
                db.is_auto_shrink_on,
                db.target_recovery_time_in_seconds,
                db.is_cdc_enabled,
                db.is_memory_optimized_elevate_to_snapshot_on,
                db.delayed_durability_desc,
                db.is_query_store_on,
                db.is_temporal_history_retention_enabled,
                db.is_accelerated_database_recovery_on,
                db.is_memory_optimized_enabled
            FROM sys.databases AS db
            WHERE db.database_id = DB_ID()
            """,
        )
        scoped_configurations = await self._section(
            database_name,
            """
            SELECT
                configuration_id,
                name,
                value,
                value_for_secondary
            FROM sys.database_scoped_configurations
            ORDER BY configuration_id
            """,
        )
        query_store_options = await self._section(
            database_name,
            """
            SELECT *
            FROM sys.database_query_store_options
            """,
        )
        automatic_tuning_options = await self._section(
            database_name,
            """
            SELECT
                name,
                desired_state_desc,
                actual_state_desc,
                reason_desc
            FROM sys.database_automatic_tuning_options
            ORDER BY name
            """,
        )
        geo_replication_links = await self._section(
            database_name,
            """
            SELECT
                link_guid,
                partner_server,
                partner_database,
                last_replication,
                replication_lag_sec,
                replication_state_desc,
                role_desc,
                secondary_allow_connections_desc
            FROM sys.dm_geo_replication_link_status
            ORDER BY partner_server, partner_database
            """,
        )
        azure_properties = await self._section(
            database_name,
            """
            SELECT
                DATABASEPROPERTYEX(DB_NAME(DB_ID()), 'Edition') AS database_edition,
                DATABASEPROPERTYEX(DB_NAME(DB_ID()), 'ServiceObjective') AS service_objective,
                DATABASEPROPERTYEX(DB_NAME(DB_ID()), 'MaxSizeInBytes') AS max_size_in_bytes,
                DATABASEPROPERTYEX(DB_NAME(DB_ID()), 'IsXTPSupported') AS is_xtp_supported
            """,
        )

        return self._with_coverage({
            "database_name": database_name,
            "version": version,
            "instance_configurations": instance_configurations,
            "database_properties": database_properties,
            "database_scoped_configurations": scoped_configurations,
            "query_store_options": query_store_options,
            "automatic_tuning_options": automatic_tuning_options,
            "geo_replication_links": geo_replication_links,
            "azure_properties": azure_properties,
        }, (
            "version",
            "instance_configurations",
            "database_properties",
            "database_scoped_configurations",
            "query_store_options",
            "automatic_tuning_options",
            "geo_replication_links",
            "azure_properties",
        ))

    async def get_storage_diagnostics(self, database_name: str) -> dict[str, Any]:
        database_size = await self._section(
            database_name,
            """
            SELECT
                CAST(
                    SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint) * 8192.0)
                    / 1024 / 1024 AS DECIMAL(18, 2)
                ) AS database_size_mb,
                CAST(
                    SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint) * 8192.0)
                    / 1024 / 1024 / 1024 AS DECIMAL(18, 2)
                ) AS database_size_gb
            FROM sys.database_files
            WHERE type_desc = N'ROWS'
            """,
        )
        file_space = await self._section(
            database_name,
            """
            SELECT
                f.name AS file_name,
                f.physical_name,
                f.type_desc,
                CAST(f.size / 128.0 AS DECIMAL(18, 2)) AS total_size_mb,
                CAST(
                    f.size / 128.0
                    - COALESCE(CAST(FILEPROPERTY(f.name, 'SpaceUsed') AS bigint), 0)
                        / 128.0
                    AS DECIMAL(18, 2)
                ) AS available_space_mb,
                f.file_id,
                fg.name AS filegroup_name,
                f.is_percent_growth,
                f.growth,
                fg.is_default,
                fg.is_read_only,
                fg.is_autogrow_all_files
            FROM sys.database_files AS f
            LEFT JOIN sys.filegroups AS fg
                ON f.data_space_id = fg.data_space_id
            ORDER BY f.file_id
            """,
        )
        log_space = await self._section(
            database_name,
            """
            SELECT
                DB_NAME(lsu.database_id) AS database_name,
                db.recovery_model_desc,
                CAST(lsu.total_log_size_in_bytes / 1048576.0 AS DECIMAL(18, 2))
                    AS total_log_space_mb,
                CAST(lsu.used_log_space_in_bytes / 1048576.0 AS DECIMAL(18, 2))
                    AS used_log_space_mb,
                CAST(lsu.used_log_space_in_percent AS DECIMAL(10, 2))
                    AS used_log_space_percent,
                CAST(lsu.log_space_in_bytes_since_last_backup / 1048576.0 AS DECIMAL(18, 2))
                    AS used_log_space_since_last_backup_mb,
                db.log_reuse_wait_desc
            FROM sys.dm_db_log_space_usage AS lsu
            INNER JOIN sys.databases AS db
                ON lsu.database_id = db.database_id
            WHERE lsu.database_id = DB_ID()
            """,
        )
        vlf_counts = await self._section(
            database_name,
            """
            SELECT
                DB_NAME(DB_ID()) AS database_name,
                file_id,
                COUNT(*) AS vlf_count
            FROM sys.dm_db_log_info(DB_ID())
            GROUP BY file_id
            ORDER BY vlf_count DESC
            """,
        )
        last_vlf_status = await self._section(
            database_name,
            """
            SELECT TOP (1)
                DB_NAME(li.database_id) AS database_name,
                li.file_id,
                li.vlf_size_mb,
                li.vlf_sequence_number,
                li.vlf_active,
                li.vlf_status
            FROM sys.dm_db_log_info(DB_ID()) AS li
            ORDER BY li.vlf_sequence_number DESC
            """,
        )

        return self._with_coverage({
            "database_name": database_name,
            "database_size": database_size,
            "file_space": file_space,
            "log_space": log_space,
            "vlf_counts": vlf_counts,
            "last_vlf_status": last_vlf_status,
            "warnings": self._storage_warnings(log_space, vlf_counts, file_space),
        }, (
            "database_size",
            "file_space",
            "log_space",
            "vlf_counts",
            "last_vlf_status",
        ))

    async def get_connection_diagnostics(
        self,
        database_name: str,
        limit: int = 50,
        include_input_buffer: bool = False,
    ) -> dict[str, Any]:
        bounded_limit = self._clamp_limit(limit)
        connection_counts = await self._section(
            database_name,
            f"""
            SELECT TOP ({bounded_limit})
                COALESCE(c.client_net_address, '<unknown>') AS client_net_address,
                COUNT(*) AS connection_count
            FROM sys.dm_exec_connections AS c
            INNER JOIN sys.dm_exec_sessions AS s
                ON c.session_id = s.session_id
            WHERE s.is_user_process = 1
            GROUP BY COALESCE(c.client_net_address, '<unknown>')
            ORDER BY connection_count DESC
            """,
        )
        session_summary = await self._section(
            database_name,
            """
            SELECT
                COUNT(*) AS total_sessions,
                SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_sessions,
                SUM(CASE WHEN status = 'sleeping' THEN 1 ELSE 0 END) AS sleeping_sessions,
                SUM(
                    CASE WHEN status = 'sleeping' AND open_transaction_count > 0
                         THEN 1 ELSE 0 END
                ) AS idle_with_open_transaction,
                SUM(open_transaction_count) AS open_transaction_count
            FROM sys.dm_exec_sessions
            WHERE is_user_process = 1
            """,
        )
        if include_input_buffer:
            input_buffers = await self._section(
                database_name,
                f"""
                SELECT TOP ({bounded_limit})
                    es.session_id,
                    DB_NAME(es.database_id) AS database_name,
                    es.login_time,
                    es.cpu_time,
                    es.logical_reads,
                    es.status,
                    ib.event_info AS input_buffer
                FROM sys.dm_exec_sessions AS es
                CROSS APPLY sys.dm_exec_input_buffer(es.session_id, NULL) AS ib
                WHERE es.database_id = DB_ID()
                  AND es.session_id > 50
                  AND es.session_id <> @@SPID
                  AND es.is_user_process = 1
                ORDER BY es.cpu_time DESC, es.logical_reads DESC
                """,
            )
        else:
            input_buffers = {
                "available": False,
                "row_count": 0,
                "rows": [],
                "skipped": True,
                "reason": "include_input_buffer is false.",
            }

        return self._with_coverage({
            "database_name": database_name,
            "limit": bounded_limit,
            "connection_counts_by_ip": connection_counts,
            "session_summary": session_summary,
            "input_buffers": input_buffers,
        }, (
            "connection_counts_by_ip",
            "session_summary",
            "input_buffers",
        ))

    async def get_top_cached_queries(
        self,
        database_name: str,
        sort_by: str = "total_worker_time",
        limit: int = 25,
    ) -> dict[str, Any]:
        bounded_limit = self._clamp_limit(limit)
        order_by = self._cached_query_sort(sort_by)
        rows = await self._section(
            database_name,
            f"""
            SELECT TOP ({bounded_limit})
                CONVERT(varchar(130), qs.query_hash, 1) AS query_hash,
                CONVERT(varchar(130), qs.query_plan_hash, 1) AS query_plan_hash,
                qs.execution_count,
                qs.total_worker_time,
                qs.total_worker_time / NULLIF(qs.execution_count, 0) AS avg_worker_time,
                qs.total_elapsed_time,
                qs.total_elapsed_time / NULLIF(qs.execution_count, 0) AS avg_elapsed_time,
                qs.total_logical_reads,
                qs.total_logical_reads / NULLIF(qs.execution_count, 0)
                    AS avg_logical_reads,
                qs.total_physical_reads,
                qs.total_logical_writes,
                qs.creation_time,
                qs.last_execution_time,
                CASE
                    WHEN CONVERT(nvarchar(max), qp.query_plan) LIKE N'%<MissingIndexes>%'
                    THEN CAST(1 AS bit)
                    ELSE CAST(0 AS bit)
                END AS has_missing_index,
                REPLACE(
                    REPLACE(
                        LEFT(
                            SUBSTRING(
                                st.text,
                                (qs.statement_start_offset / 2) + 1,
                                (
                                    (
                                        CASE qs.statement_end_offset
                                            WHEN -1 THEN DATALENGTH(st.text)
                                            ELSE qs.statement_end_offset
                                        END
                                    ) - qs.statement_start_offset
                                ) / 2 + 1
                            ),
                            500
                        ),
                        CHAR(13),
                        ' '
                    ),
                    CHAR(10),
                    ' '
                ) AS query_text_preview
            FROM sys.dm_exec_query_stats AS qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
            OUTER APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
            WHERE st.dbid IS NULL OR st.dbid = DB_ID()
            ORDER BY {order_by} DESC
            """,
        )
        return self._with_coverage({
            "database_name": database_name,
            "sort_by": sort_by,
            "limit": bounded_limit,
            "cached_queries": rows,
        }, ("cached_queries",))

    async def get_cached_routine_stats(
        self,
        database_name: str,
        routine_type: str = "all",
        sort_by: str = "total_worker_time",
        limit: int = 25,
    ) -> dict[str, Any]:
        normalized_type = routine_type.strip().lower()
        if normalized_type not in {"all", "procedure", "function"}:
            raise ValueError("routine_type must be all, procedure, or function.")

        bounded_limit = self._clamp_limit(limit)
        order_by = self._routine_sort(sort_by)
        procedures: dict[str, Any]
        functions: dict[str, Any]
        if normalized_type in {"all", "procedure"}:
            procedures = await self._section(
                database_name,
                f"""
                SELECT TOP ({bounded_limit})
                    SCHEMA_NAME(p.schema_id) AS schema_name,
                    p.name AS routine_name,
                    'PROCEDURE' AS routine_type,
                    qs.execution_count,
                    qs.total_worker_time,
                    qs.total_worker_time / NULLIF(qs.execution_count, 0)
                        AS avg_worker_time,
                    qs.total_elapsed_time,
                    qs.total_elapsed_time / NULLIF(qs.execution_count, 0)
                        AS avg_elapsed_time,
                    qs.total_logical_reads,
                    qs.total_logical_reads / NULLIF(qs.execution_count, 0)
                        AS avg_logical_reads,
                    qs.total_physical_reads,
                    qs.total_logical_writes,
                    ISNULL(qs.execution_count / NULLIF(DATEDIFF(MINUTE, qs.cached_time, GETDATE()), 0), 0)
                        AS calls_per_minute,
                    qs.cached_time,
                    qs.last_execution_time,
                    CASE
                        WHEN CONVERT(nvarchar(max), qp.query_plan) LIKE N'%<MissingIndexes>%'
                        THEN CAST(1 AS bit)
                        ELSE CAST(0 AS bit)
                    END AS has_missing_index
                FROM sys.procedures AS p
                INNER JOIN sys.dm_exec_procedure_stats AS qs
                    ON p.object_id = qs.object_id
                OUTER APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
                WHERE qs.database_id = DB_ID()
                ORDER BY {order_by} DESC
                """,
            )
        else:
            procedures = self._skipped("routine_type excludes procedures.")

        if normalized_type in {"all", "function"}:
            functions = await self._section(
                database_name,
                f"""
                SELECT TOP ({bounded_limit})
                    SCHEMA_NAME(o.schema_id) AS schema_name,
                    OBJECT_NAME(fs.object_id) AS routine_name,
                    fs.type_desc AS routine_type,
                    fs.execution_count,
                    fs.total_worker_time,
                    fs.total_worker_time / NULLIF(fs.execution_count, 0)
                        AS avg_worker_time,
                    fs.total_elapsed_time,
                    fs.total_elapsed_time / NULLIF(fs.execution_count, 0)
                        AS avg_elapsed_time,
                    fs.total_logical_reads,
                    fs.total_logical_reads / NULLIF(fs.execution_count, 0)
                        AS avg_logical_reads,
                    fs.total_physical_reads,
                    fs.total_logical_writes,
                    fs.cached_time,
                    fs.last_execution_time,
                    CASE
                        WHEN CONVERT(nvarchar(max), qp.query_plan) LIKE N'%<MissingIndexes>%'
                        THEN CAST(1 AS bit)
                        ELSE CAST(0 AS bit)
                    END AS has_missing_index
                FROM sys.dm_exec_function_stats AS fs
                INNER JOIN sys.objects AS o
                    ON fs.object_id = o.object_id
                OUTER APPLY sys.dm_exec_query_plan(fs.plan_handle) AS qp
                WHERE fs.database_id = DB_ID()
                ORDER BY {order_by} DESC
                """,
            )
        else:
            functions = self._skipped("routine_type excludes functions.")

        return self._with_coverage({
            "database_name": database_name,
            "routine_type": normalized_type,
            "sort_by": sort_by,
            "limit": bounded_limit,
            "procedures": procedures,
            "functions": functions,
        }, ("procedures", "functions"))

    async def get_object_index_diagnostics(
        self,
        database_name: str,
        schema_name: str | None = None,
        table_name: str | None = None,
        limit: int = 25,
    ) -> dict[str, Any]:
        bounded_limit = self._clamp_limit(limit)
        params = [schema_name, schema_name, table_name, table_name]
        object_filter = """
          AND (? IS NULL OR SCHEMA_NAME(o.schema_id) = ?)
          AND (? IS NULL OR o.name = ?)
        """
        table_filter = """
          AND (? IS NULL OR SCHEMA_NAME(t.schema_id) = ?)
          AND (? IS NULL OR t.name = ?)
        """
        table_properties = await self._section(
            database_name,
            f"""
            SELECT TOP ({bounded_limit})
                SCHEMA_NAME(t.schema_id) AS schema_name,
                t.name AS table_name,
                t.temporal_type_desc,
                OBJECT_SCHEMA_NAME(t.history_table_id) AS history_schema_name,
                OBJECT_NAME(t.history_table_id) AS history_table_name,
                t.is_memory_optimized,
                t.durability_desc,
                t.lock_escalation_desc,
                t.is_tracked_by_cdc,
                t.create_date,
                t.modify_date
            FROM sys.tables AS t
            WHERE t.is_ms_shipped = 0
              {table_filter}
            ORDER BY SCHEMA_NAME(t.schema_id), t.name
            """,
            params=params,
        )
        bad_nonclustered_indexes = await self._section(
            database_name,
            f"""
            SELECT TOP ({bounded_limit})
                SCHEMA_NAME(o.schema_id) AS schema_name,
                OBJECT_NAME(i.object_id) AS table_name,
                i.name AS index_name,
                ius.user_updates,
                ius.user_seeks,
                ius.user_scans,
                ius.user_lookups,
                COALESCE(ius.user_seeks, 0)
                    + COALESCE(ius.user_scans, 0)
                    + COALESCE(ius.user_lookups, 0) AS user_reads,
                CASE
                    WHEN COALESCE(ius.user_seeks, 0)
                       + COALESCE(ius.user_scans, 0)
                       + COALESCE(ius.user_lookups, 0) = 0
                    THEN NULL
                    ELSE CAST(
                        ius.user_updates * 1.0
                        / NULLIF(
                            COALESCE(ius.user_seeks, 0)
                            + COALESCE(ius.user_scans, 0)
                            + COALESCE(ius.user_lookups, 0),
                            0
                        ) AS DECIMAL(18, 2)
                    )
                END AS writes_per_read
            FROM sys.indexes AS i
            INNER JOIN sys.objects AS o
                ON i.object_id = o.object_id
            LEFT JOIN sys.dm_db_index_usage_stats AS ius
                ON i.object_id = ius.object_id
               AND i.index_id = ius.index_id
               AND ius.database_id = DB_ID()
            WHERE o.is_ms_shipped = 0
              AND i.type_desc = N'NONCLUSTERED'
              AND i.is_primary_key = 0
              AND i.is_unique_constraint = 0
              AND COALESCE(ius.user_updates, 0)
                  > COALESCE(ius.user_seeks, 0)
                    + COALESCE(ius.user_scans, 0)
                    + COALESCE(ius.user_lookups, 0)
              {object_filter}
            ORDER BY ius.user_updates DESC
            """,
            params=params,
        )
        index_usage_reads = await self._section(
            database_name,
            f"""
            SELECT TOP ({bounded_limit})
                SCHEMA_NAME(t.schema_id) AS schema_name,
                t.name AS table_name,
                i.name AS index_name,
                i.type_desc,
                COALESCE(ius.user_seeks, 0) AS user_seeks,
                COALESCE(ius.user_scans, 0) AS user_scans,
                COALESCE(ius.user_lookups, 0) AS user_lookups,
                COALESCE(ius.user_updates, 0) AS user_updates,
                COALESCE(ius.user_seeks, 0)
                    + COALESCE(ius.user_scans, 0)
                    + COALESCE(ius.user_lookups, 0) AS user_reads
            FROM sys.tables AS t
            INNER JOIN sys.indexes AS i
                ON t.object_id = i.object_id
            LEFT JOIN sys.dm_db_index_usage_stats AS ius
                ON i.object_id = ius.object_id
               AND i.index_id = ius.index_id
               AND ius.database_id = DB_ID()
            WHERE i.name IS NOT NULL
              {table_filter}
            ORDER BY user_reads DESC
            """,
            params=params,
        )
        index_usage_writes = await self._section(
            database_name,
            f"""
            SELECT TOP ({bounded_limit})
                SCHEMA_NAME(t.schema_id) AS schema_name,
                t.name AS table_name,
                i.name AS index_name,
                i.type_desc,
                COALESCE(ius.user_updates, 0) AS user_updates,
                COALESCE(ius.user_seeks, 0) AS user_seeks,
                COALESCE(ius.user_scans, 0) AS user_scans,
                COALESCE(ius.user_lookups, 0) AS user_lookups
            FROM sys.tables AS t
            INNER JOIN sys.indexes AS i
                ON t.object_id = i.object_id
            LEFT JOIN sys.dm_db_index_usage_stats AS ius
                ON i.object_id = ius.object_id
               AND i.index_id = ius.index_id
               AND ius.database_id = DB_ID()
            WHERE i.name IS NOT NULL
              {table_filter}
            ORDER BY user_updates DESC
            """,
            params=params,
        )
        buffer_usage = await self._section(
            database_name,
            f"""
            SELECT TOP ({bounded_limit})
                SCHEMA_NAME(o.schema_id) AS schema_name,
                OBJECT_NAME(p.object_id) AS object_name,
                i.name AS index_name,
                i.type_desc,
                COUNT(*) * 8.0 / 1024 AS buffer_mb
            FROM sys.dm_os_buffer_descriptors AS bd
            INNER JOIN sys.allocation_units AS au
                ON bd.allocation_unit_id = au.allocation_unit_id
            INNER JOIN sys.partitions AS p
                ON au.container_id = p.hobt_id
            INNER JOIN sys.objects AS o
                ON p.object_id = o.object_id
            INNER JOIN sys.indexes AS i
                ON p.object_id = i.object_id
               AND p.index_id = i.index_id
            WHERE bd.database_id = DB_ID()
              AND o.is_ms_shipped = 0
              {object_filter}
            GROUP BY o.schema_id, p.object_id, i.name, i.type_desc
            ORDER BY buffer_mb DESC
            """,
            params=params,
        )
        volatile_statistics = await self._section(
            database_name,
            f"""
            SELECT TOP ({bounded_limit})
                SCHEMA_NAME(o.schema_id) AS schema_name,
                OBJECT_NAME(s.object_id) AS table_name,
                s.name AS stats_name,
                sp.rows AS total_rows,
                sp.modification_counter,
                CASE
                    WHEN sp.rows > 0
                    THEN CAST(sp.modification_counter * 100.0 / sp.rows AS DECIMAL(10, 2))
                    ELSE 0
                END AS modification_pct,
                sp.last_updated
            FROM sys.stats AS s
            INNER JOIN sys.objects AS o
                ON s.object_id = o.object_id
            CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
            WHERE o.is_ms_shipped = 0
              AND sp.rows > 0
              {object_filter}
            ORDER BY sp.modification_counter DESC
            """,
            params=params,
        )
        columnstore_physical_stats = await self._section(
            database_name,
            f"""
            SELECT TOP ({bounded_limit})
                SCHEMA_NAME(o.schema_id) AS schema_name,
                OBJECT_NAME(cs.object_id) AS table_name,
                i.name AS index_name,
                cs.partition_number,
                cs.row_group_id,
                cs.state_desc,
                cs.total_rows,
                cs.deleted_rows,
                CASE
                    WHEN cs.total_rows > 0
                    THEN CAST(cs.deleted_rows * 100.0 / cs.total_rows AS DECIMAL(10, 2))
                    ELSE 0
                END AS deleted_rows_pct,
                cs.size_in_bytes / 1048576.0 AS size_mb
            FROM sys.dm_db_column_store_row_group_physical_stats AS cs
            INNER JOIN sys.objects AS o
                ON cs.object_id = o.object_id
            INNER JOIN sys.indexes AS i
                ON cs.object_id = i.object_id
               AND cs.index_id = i.index_id
            WHERE o.is_ms_shipped = 0
              {object_filter}
            ORDER BY deleted_rows_pct DESC, total_rows DESC
            """,
            params=params,
        )
        lock_waits = await self._section(
            database_name,
            f"""
            SELECT TOP ({bounded_limit})
                SCHEMA_NAME(o.schema_id) AS schema_name,
                OBJECT_NAME(ios.object_id) AS table_name,
                i.name AS index_name,
                ios.row_lock_wait_count,
                ios.row_lock_wait_in_ms,
                ios.page_lock_wait_count,
                ios.page_lock_wait_in_ms,
                ios.index_lock_promotion_attempt_count,
                ios.index_lock_promotion_count
            FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) AS ios
            INNER JOIN sys.objects AS o
                ON ios.object_id = o.object_id
            INNER JOIN sys.indexes AS i
                ON ios.object_id = i.object_id
               AND ios.index_id = i.index_id
            WHERE o.is_ms_shipped = 0
              AND (
                    ios.row_lock_wait_count > 0
                 OR ios.page_lock_wait_count > 0
                 OR ios.index_lock_promotion_attempt_count > 0
              )
              {object_filter}
            ORDER BY
                ios.row_lock_wait_in_ms + ios.page_lock_wait_in_ms DESC
            """,
            params=params,
        )
        resumable_index_operations = await self._section(
            database_name,
            f"""
            SELECT TOP ({bounded_limit})
                OBJECT_SCHEMA_NAME(iro.object_id) AS schema_name,
                OBJECT_NAME(iro.object_id) AS object_name,
                iro.index_id,
                iro.name AS index_name,
                LEFT(iro.sql_text, 500) AS sql_text_preview,
                iro.last_max_dop_used,
                iro.partition_number,
                iro.state_desc,
                iro.start_time,
                iro.percent_complete
            FROM sys.index_resumable_operations AS iro
            INNER JOIN sys.objects AS o
                ON iro.object_id = o.object_id
            WHERE 1 = 1
              {object_filter}
            ORDER BY iro.start_time DESC
            """,
            params=params,
        )

        return self._with_coverage({
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "limit": bounded_limit,
            "table_properties": table_properties,
            "bad_nonclustered_indexes": bad_nonclustered_indexes,
            "index_usage_reads": index_usage_reads,
            "index_usage_writes": index_usage_writes,
            "buffer_usage": buffer_usage,
            "volatile_statistics": volatile_statistics,
            "columnstore_physical_stats": columnstore_physical_stats,
            "lock_waits": lock_waits,
            "resumable_index_operations": resumable_index_operations,
        }, (
            "table_properties",
            "bad_nonclustered_indexes",
            "index_usage_reads",
            "index_usage_writes",
            "buffer_usage",
            "volatile_statistics",
            "columnstore_physical_stats",
            "lock_waits",
            "resumable_index_operations",
        ))

    async def _section(
        self,
        database_name: str,
        query: str,
        *,
        params: Sequence[Any] | None = None,
    ) -> dict[str, Any]:
        try:
            rows = await self.executor.fetch_all(database_name, query, params=params)
        except Exception as exc:
            return {
                "available": False,
                "row_count": 0,
                "rows": [],
                "error": sanitize_error_message(str(exc)),
            }
        return {
            "available": True,
            "row_count": len(rows),
            "rows": rows,
        }

    @staticmethod
    def _with_coverage(
        payload: dict[str, Any],
        section_names: Sequence[str],
    ) -> dict[str, Any]:
        available: list[str] = []
        unavailable: list[str] = []
        skipped: list[str] = []
        for name in section_names:
            section = payload.get(name)
            if not isinstance(section, dict):
                continue
            if section.get("available") is True:
                available.append(name)
            elif section.get("skipped") is True:
                skipped.append(name)
            else:
                unavailable.append(name)

        available_count = len(available)
        if unavailable:
            status = "partial" if available_count else "unavailable"
        elif skipped:
            status = "partial"
        else:
            status = "complete"

        payload["coverage"] = {
            "status": status,
            "section_count": len(section_names),
            "available_count": available_count,
            "skipped_sections": skipped,
            "unavailable_sections": unavailable,
            "is_complete": status == "complete",
        }
        return payload

    def _clamp_limit(self, limit: int) -> int:
        return min(max(int(limit), 1), self.MAX_LIMIT)

    def _cached_query_sort(self, sort_by: str) -> str:
        sort_map = {
            "execution_count": "qs.execution_count",
            "total_worker_time": "qs.total_worker_time",
            "avg_worker_time": "qs.total_worker_time / NULLIF(qs.execution_count, 0)",
            "total_elapsed_time": "qs.total_elapsed_time",
            "avg_elapsed_time": "qs.total_elapsed_time / NULLIF(qs.execution_count, 0)",
            "total_logical_reads": "qs.total_logical_reads",
            "total_physical_reads": "qs.total_physical_reads",
            "total_logical_writes": "qs.total_logical_writes",
        }
        try:
            return sort_map[sort_by]
        except KeyError as exc:
            supported = ", ".join(sorted(sort_map))
            raise ValueError(f"Unsupported sort_by. Use {supported}.") from exc

    def _routine_sort(self, sort_by: str) -> str:
        sort_map = {
            "execution_count": "execution_count",
            "total_worker_time": "total_worker_time",
            "avg_worker_time": "total_worker_time / NULLIF(execution_count, 0)",
            "total_elapsed_time": "total_elapsed_time",
            "avg_elapsed_time": "total_elapsed_time / NULLIF(execution_count, 0)",
            "total_logical_reads": "total_logical_reads",
            "total_physical_reads": "total_physical_reads",
            "total_logical_writes": "total_logical_writes",
        }
        try:
            return sort_map[sort_by]
        except KeyError as exc:
            supported = ", ".join(sorted(sort_map))
            raise ValueError(f"Unsupported sort_by. Use {supported}.") from exc

    def _storage_warnings(
        self,
        log_space: dict[str, Any],
        vlf_counts: dict[str, Any],
        file_space: dict[str, Any],
    ) -> list[dict[str, Any]]:
        warnings: list[dict[str, Any]] = []
        for row in log_space.get("rows", []):
            used_pct = self._to_float(row.get("used_log_space_percent"))
            if used_pct is not None and used_pct >= 95.0:
                warnings.append(
                    {
                        "type": "critical_log_space_usage",
                        "message": f"Transaction log is {used_pct}% used.",
                    }
                )
            elif used_pct is not None and used_pct >= 80.0:
                warnings.append(
                    {
                        "type": "high_log_space_usage",
                        "message": f"Transaction log is {used_pct}% used.",
                    }
                )
        for row in vlf_counts.get("rows", []):
            vlf_count = self._to_int(row.get("vlf_count"))
            if vlf_count is not None and vlf_count > 1000:
                warnings.append(
                    {
                        "type": "critical_vlf_count",
                        "message": f"VLF count is {vlf_count}; review log growth patterns.",
                    }
                )
            elif vlf_count is not None and vlf_count > 200:
                warnings.append(
                    {
                        "type": "high_vlf_count",
                        "message": f"VLF count is {vlf_count}; keep it below 200 when possible.",
                    }
                )
        for row in file_space.get("rows", []):
            total_mb = self._to_float(row.get("total_size_mb"))
            available_mb = self._to_float(row.get("available_space_mb"))
            if total_mb and available_mb is not None:
                used_pct = round((total_mb - available_mb) / total_mb * 100, 2)
                if used_pct >= 95.0:
                    warnings.append(
                        {
                            "type": "critical_file_usage",
                            "file_name": row.get("file_name"),
                            "used_percent": used_pct,
                        }
                    )
                elif used_pct >= 80.0:
                    warnings.append(
                        {
                            "type": "high_file_usage",
                            "file_name": row.get("file_name"),
                            "used_percent": used_pct,
                        }
                    )
        return warnings

    def _skipped(self, reason: str) -> dict[str, Any]:
        return {
            "available": False,
            "row_count": 0,
            "rows": [],
            "skipped": True,
            "reason": reason,
        }

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
