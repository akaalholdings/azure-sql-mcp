from __future__ import annotations

from typing import Any

from .connection import AzureSqlExecutor


MAX_SESSION_LIMIT = 1000


class SessionsService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_active_sessions(
        self,
        database_name: str,
        limit: int = 200,
    ) -> dict[str, Any]:
        bounded_limit = max(1, min(int(limit), MAX_SESSION_LIMIT))
        query = """
        SELECT TOP (?)
            r.session_id,
            s.login_name,
            s.status AS session_status,
            r.status AS request_status,
            r.command,
            r.wait_type,
            r.wait_time AS wait_time_ms,
            r.wait_resource,
            r.blocking_session_id,
            r.cpu_time AS cpu_time_ms,
            r.total_elapsed_time AS elapsed_time_ms,
            r.reads AS logical_reads,
            r.writes,
            r.row_count,
            CAST(r.granted_query_memory * 8.0 / 1024 AS DECIMAL(18, 2)) AS granted_memory_mb,
            CASE s.transaction_isolation_level
                WHEN 0 THEN 'Unspecified'
                WHEN 1 THEN 'ReadUncommitted'
                WHEN 2 THEN 'ReadCommitted'
                WHEN 3 THEN 'RepeatableRead'
                WHEN 4 THEN 'Serializable'
                WHEN 5 THEN 'Snapshot'
                ELSE CAST(s.transaction_isolation_level AS VARCHAR(10))
            END AS isolation_level,
            s.open_transaction_count,
            r.transaction_id,
            at.transaction_begin_time,
            DATEDIFF(SECOND, at.transaction_begin_time, GETDATE()) AS transaction_duration_seconds,
            SUBSTRING(
                st.text,
                (r.statement_start_offset / 2) + 1,
                (
                    CASE r.statement_end_offset
                        WHEN -1 THEN DATALENGTH(st.text)
                        ELSE r.statement_end_offset
                    END - r.statement_start_offset
                ) / 2 + 1
            ) AS current_statement,
            qp.query_plan AS execution_plan_xml
        FROM sys.dm_exec_requests AS r
        INNER JOIN sys.dm_exec_sessions AS s
            ON r.session_id = s.session_id
        LEFT JOIN sys.dm_tran_active_transactions AS at
            ON r.transaction_id = at.transaction_id
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
        OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) AS qp
        WHERE s.is_user_process = 1
          AND r.session_id != @@SPID
        ORDER BY r.total_elapsed_time DESC
        """
        sessions = await self.executor.fetch_all(
            database_name, query, params=[bounded_limit + 1],
        )
        truncated = len(sessions) > bounded_limit
        sessions = sessions[:bounded_limit]
        return {
            "database_name": database_name,
            "active_session_count": len(sessions),
            "limit": bounded_limit,
            "truncated": truncated,
            "sessions": sessions,
            "blocking_chains": self._detect_blocking_chains(sessions),
        }

    def _detect_blocking_chains(
        self,
        sessions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        by_session_id = {
            session["session_id"]: session
            for session in sessions
            if session.get("session_id") is not None
        }
        blocked_sessions = {
            session["session_id"]: session
            for session in sessions
            if session.get("blocking_session_id")
        }
        if not blocked_sessions:
            return []

        head_blockers = {
            session["blocking_session_id"]
            for session in blocked_sessions.values()
            if session.get("blocking_session_id") not in blocked_sessions
        }

        chains: list[dict[str, Any]] = []
        for head_blocker in sorted(head_blockers):
            chains.append(
                {
                    "head_blocker_session_id": head_blocker,
                    "head_blocker": by_session_id.get(head_blocker),
                    "blocked_sessions": [
                        session
                        for session in sessions
                        if session.get("blocking_session_id") == head_blocker
                    ],
                }
            )
        return chains
