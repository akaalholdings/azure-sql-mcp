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
        include_plans: bool = False,
    ) -> dict[str, Any]:
        bounded_limit = max(1, min(int(limit), MAX_SESSION_LIMIT))
        # Each dm_exec_query_plan call builds showplan XML on the database, so
        # the plan is opt-in. plan_handle and query_hash let a caller ask for
        # one plan.
        plan_column = ""
        plan_apply = ""
        if include_plans:
            plan_column = ",\n            qp.query_plan AS execution_plan_xml"
            plan_apply = "\n        OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) AS qp"
        query = f"""
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
            CONVERT(varchar(130), r.plan_handle, 1) AS plan_handle,
            CONVERT(varchar(130), r.query_hash, 1) AS query_hash,
            SUBSTRING(
                st.text,
                (r.statement_start_offset / 2) + 1,
                (
                    CASE r.statement_end_offset
                        WHEN -1 THEN DATALENGTH(st.text)
                        ELSE r.statement_end_offset
                    END - r.statement_start_offset
                ) / 2 + 1
            ) AS current_statement{plan_column}
        FROM sys.dm_exec_requests AS r
        INNER JOIN sys.dm_exec_sessions AS s
            ON r.session_id = s.session_id
        LEFT JOIN sys.dm_tran_active_transactions AS at
            ON r.transaction_id = at.transaction_id
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS st{plan_apply}
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

        # Ids and a short wait summary only: the full rows are in "sessions".
        # A head blocker with no request is idle in an open transaction.
        active_session_ids = {session.get("session_id") for session in sessions}
        chains: list[dict[str, Any]] = []
        for head_blocker in sorted(head_blockers):
            blocked = [
                session
                for session in sessions
                if session.get("blocking_session_id") == head_blocker
            ]
            chains.append(
                {
                    "head_blocker_session_id": head_blocker,
                    "head_blocker_has_active_request": head_blocker in active_session_ids,
                    "blocked_session_ids": [session["session_id"] for session in blocked],
                    "blocked_session_waits": [
                        {
                            "session_id": session["session_id"],
                            "wait_type": session.get("wait_type"),
                            "wait_time_ms": session.get("wait_time_ms"),
                            "wait_resource": session.get("wait_resource"),
                        }
                        for session in blocked
                    ],
                }
            )
        return chains
