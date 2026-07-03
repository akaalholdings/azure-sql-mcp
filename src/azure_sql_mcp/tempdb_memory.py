from __future__ import annotations

from typing import Any

from .connection import AzureSqlExecutor


MAX_SESSION_LIMIT = 1000


class TempdbMemoryService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_tempdb_usage(
        self,
        database_name: str,
        limit: int = 200,
    ) -> dict[str, Any]:
        """Per-session tempdb consumption from sys.dm_db_session_space_usage."""
        bounded_limit = max(1, min(int(limit), MAX_SESSION_LIMIT))
        query = """
        SELECT TOP (?)
            su.session_id,
            s.login_name,
            s.status AS session_status,
            s.host_name,
            s.program_name,
            su.user_objects_alloc_page_count * 8 / 1024.0 AS user_objects_alloc_mb,
            su.user_objects_dealloc_page_count * 8 / 1024.0 AS user_objects_dealloc_mb,
            (su.user_objects_alloc_page_count - su.user_objects_dealloc_page_count) * 8 / 1024.0 AS user_objects_net_mb,
            su.internal_objects_alloc_page_count * 8 / 1024.0 AS internal_objects_alloc_mb,
            su.internal_objects_dealloc_page_count * 8 / 1024.0 AS internal_objects_dealloc_mb,
            (su.internal_objects_alloc_page_count - su.internal_objects_dealloc_page_count) * 8 / 1024.0 AS internal_objects_net_mb,
            (
                (su.user_objects_alloc_page_count - su.user_objects_dealloc_page_count)
                + (su.internal_objects_alloc_page_count - su.internal_objects_dealloc_page_count)
            ) * 8 / 1024.0 AS total_net_mb
        FROM sys.dm_db_session_space_usage AS su
        INNER JOIN sys.dm_exec_sessions AS s
            ON su.session_id = s.session_id
        WHERE s.is_user_process = 1
          AND (
            su.user_objects_alloc_page_count > 0
            OR su.internal_objects_alloc_page_count > 0
          )
        ORDER BY total_net_mb DESC
        """
        rows = await self.executor.fetch_all(
            database_name, query, params=[bounded_limit + 1],
        )
        truncated = len(rows) > bounded_limit
        rows = rows[:bounded_limit]
        return {
            "database_name": database_name,
            "session_count": len(rows),
            "limit": bounded_limit,
            "truncated": truncated,
            "sessions": rows,
        }

    async def get_tempdb_space_breakdown(
        self,
        database_name: str,
    ) -> dict[str, Any]:
        """Tempdb space breakdown: version store, user objects, internal objects, free."""
        query = """
        SELECT
            SUM(version_store_reserved_page_count) * 8 / 1024.0 AS version_store_mb,
            SUM(user_object_reserved_page_count) * 8 / 1024.0 AS user_objects_mb,
            SUM(internal_object_reserved_page_count) * 8 / 1024.0 AS internal_objects_mb,
            SUM(unallocated_extent_page_count) * 8 / 1024.0 AS free_space_mb,
            SUM(
                version_store_reserved_page_count
                + user_object_reserved_page_count
                + internal_object_reserved_page_count
                + unallocated_extent_page_count
            ) * 8 / 1024.0 AS total_mb
        FROM tempdb.sys.dm_db_file_space_usage
        """
        rows = await self.executor.fetch_all(database_name, query)
        breakdown = rows[0] if rows else {}
        return {
            "database_name": database_name,
            "breakdown": breakdown,
        }

    async def get_memory_grants(
        self,
        database_name: str,
    ) -> dict[str, Any]:
        """Active and pending memory grants from sys.dm_exec_query_memory_grants."""
        query = """
        SELECT
            mg.session_id,
            mg.request_time,
            mg.grant_time,
            mg.requested_memory_kb,
            mg.granted_memory_kb,
            mg.required_memory_kb,
            mg.used_memory_kb,
            mg.max_used_memory_kb,
            mg.ideal_memory_kb,
            mg.is_small,
            mg.timeout_sec,
            mg.wait_time_ms,
            mg.dop,
            mg.query_cost,
            CASE
                WHEN mg.grant_time IS NULL THEN 'Pending'
                ELSE 'Granted'
            END AS grant_status,
            SUBSTRING(
                st.text,
                (r.statement_start_offset / 2) + 1,
                (
                    CASE r.statement_end_offset
                        WHEN -1 THEN DATALENGTH(st.text)
                        ELSE r.statement_end_offset
                    END - r.statement_start_offset
                ) / 2 + 1
            ) AS current_statement
        FROM sys.dm_exec_query_memory_grants AS mg
        LEFT JOIN sys.dm_exec_requests AS r
            ON mg.session_id = r.session_id
        OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
        ORDER BY mg.requested_memory_kb DESC
        """
        rows = await self.executor.fetch_all(database_name, query)

        pending = [r for r in rows if r.get("grant_status") == "Pending"]
        granted = [r for r in rows if r.get("grant_status") == "Granted"]

        # Detect queries that might be spilling
        spilling = [
            r
            for r in granted
            if (r.get("max_used_memory_kb") or 0) >= (r.get("granted_memory_kb") or 1) * 0.95
        ]

        return {
            "database_name": database_name,
            "total_grants": len(rows),
            "pending_count": len(pending),
            "granted_count": len(granted),
            "likely_spilling_count": len(spilling),
            "grants": rows,
            "warnings": (
                [
                    {
                        "type": "pending_grants",
                        "message": f"{len(pending)} query/queries waiting for memory grant (RESOURCE_SEMAPHORE)",
                        "sessions": [r.get("session_id") for r in pending],
                    }
                ]
                if pending
                else []
            )
            + (
                [
                    {
                        "type": "likely_spilling",
                        "message": f"{len(spilling)} query/queries using >=95% of granted memory (likely spilling to tempdb)",
                        "sessions": [r.get("session_id") for r in spilling],
                    }
                ]
                if spilling
                else []
            ),
        }
