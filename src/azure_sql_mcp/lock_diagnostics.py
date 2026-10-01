from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

from .connection import AzureSqlExecutor
from .deadlocks import DeadlockHistoryReader
from .deadlocks import parse_deadlock_graph


MAX_ROW_LIMIT = 1000


def _clamp_limit(limit: int, default: int) -> int:
    try:
        value = int(limit)
    except (TypeError, ValueError):
        return default
    return max(1, min(value, MAX_ROW_LIMIT))


class LockDiagnosticsService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_lock_details(
        self,
        database_name: str,
        limit: int = 200,
    ) -> dict[str, Any]:
        """Current locks from sys.dm_tran_locks with owning session and SQL text.

        sys.dm_tran_locks can hold hundreds of thousands of rows on a busy
        system; results are bounded with waiting locks sorted first.
        """
        bounded_limit = _clamp_limit(limit, default=200)
        query = """
        SELECT TOP (?)
            tl.resource_type,
            tl.resource_subtype,
            tl.request_mode,
            tl.request_status,
            tl.request_session_id AS session_id,
            tl.resource_description,
            CASE tl.resource_type
                WHEN 'OBJECT' THEN OBJECT_NAME(tl.resource_associated_entity_id)
                ELSE NULL
            END AS object_name,
            s.login_name,
            s.status AS session_status,
            r.command,
            r.wait_type,
            r.wait_time AS wait_time_ms,
            r.blocking_session_id,
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
        FROM sys.dm_tran_locks AS tl
        INNER JOIN sys.dm_exec_sessions AS s
            ON tl.request_session_id = s.session_id
        LEFT JOIN sys.dm_exec_requests AS r
            ON tl.request_session_id = r.session_id
        OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
        WHERE tl.resource_database_id = DB_ID()
          AND s.is_user_process = 1
        ORDER BY
            CASE WHEN tl.request_status = 'WAIT' THEN 0 ELSE 1 END,
            tl.resource_type,
            tl.request_mode
        """
        rows = await self.executor.fetch_all(
            database_name, query, params=[bounded_limit + 1],
        )
        truncated = len(rows) > bounded_limit
        rows = rows[:bounded_limit]

        # Summarize by resource type
        by_type: dict[str, int] = {}
        for row in rows:
            rt = row.get("resource_type", "UNKNOWN")
            by_type[rt] = by_type.get(rt, 0) + 1

        waiting = [r for r in rows if r.get("request_status") == "WAIT"]

        return {
            "database_name": database_name,
            "total_locks": len(rows),
            "waiting_locks": len(waiting),
            "locks_by_resource_type": by_type,
            "limit": bounded_limit,
            "truncated": truncated,
            "locks": rows,
        }

    async def get_open_transactions(
        self,
        database_name: str,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Active transactions with duration, type, and log bytes used.

        Bounded, oldest transactions first — those matter most when truncated.
        """
        bounded_limit = _clamp_limit(limit, default=100)
        query = """
        SELECT TOP (?)
            at.transaction_id,
            at.name AS transaction_name,
            CASE at.transaction_type
                WHEN 1 THEN 'Read/Write'
                WHEN 2 THEN 'Read-Only'
                WHEN 3 THEN 'System'
                WHEN 4 THEN 'Distributed'
                ELSE CAST(at.transaction_type AS VARCHAR(10))
            END AS transaction_type,
            CASE at.transaction_state
                WHEN 0 THEN 'Not fully initialized'
                WHEN 1 THEN 'Initialized, not started'
                WHEN 2 THEN 'Active'
                WHEN 3 THEN 'Ended (read-only)'
                WHEN 4 THEN 'Commit initiated'
                WHEN 5 THEN 'Prepared, awaiting resolution'
                WHEN 6 THEN 'Committed'
                WHEN 7 THEN 'Rolling back'
                WHEN 8 THEN 'Rolled back'
                ELSE CAST(at.transaction_state AS VARCHAR(10))
            END AS transaction_state,
            at.transaction_begin_time,
            DATEDIFF(SECOND, at.transaction_begin_time, GETDATE()) AS duration_seconds,
            st.session_id,
            s.login_name,
            s.status AS session_status,
            s.host_name,
            s.program_name,
            dt.database_transaction_log_bytes_used AS log_bytes_used,
            dt.database_transaction_log_bytes_reserved AS log_bytes_reserved
        FROM sys.dm_tran_active_transactions AS at
        INNER JOIN sys.dm_tran_session_transactions AS st
            ON at.transaction_id = st.transaction_id
        INNER JOIN sys.dm_exec_sessions AS s
            ON st.session_id = s.session_id
        LEFT JOIN sys.dm_tran_database_transactions AS dt
            ON at.transaction_id = dt.transaction_id
            AND dt.database_id = DB_ID()
        WHERE s.is_user_process = 1
        ORDER BY at.transaction_begin_time ASC
        """
        rows = await self.executor.fetch_all(
            database_name, query, params=[bounded_limit + 1],
        )
        truncated = len(rows) > bounded_limit
        rows = rows[:bounded_limit]

        long_running = [
            r for r in rows if (r.get("duration_seconds") or 0) > 300
        ]
        idle_with_txn = [
            r
            for r in rows
            if r.get("session_status") == "sleeping"
            and (r.get("duration_seconds") or 0) > 60
        ]

        return {
            "database_name": database_name,
            "open_transaction_count": len(rows),
            "long_running_count": len(long_running),
            "idle_with_open_txn_count": len(idle_with_txn),
            "limit": bounded_limit,
            "truncated": truncated,
            "transactions": rows,
            "warnings": (
                [
                    {
                        "type": "long_running",
                        "message": f"{len(long_running)} transaction(s) open for more than 5 minutes",
                        "sessions": [r.get("session_id") for r in long_running],
                    }
                ]
                if long_running
                else []
            )
            + (
                [
                    {
                        "type": "idle_with_open_txn",
                        "message": f"{len(idle_with_txn)} idle session(s) with open transactions",
                        "sessions": [r.get("session_id") for r in idle_with_txn],
                    }
                ]
                if idle_with_txn
                else []
            ),
        }

    async def get_deadlock_history(
        self,
        database_name: str,
        max_events: int = 10,
        *,
        master_available: bool = False,
        include_graph_xml: bool = False,
    ) -> dict[str, Any]:
        """Deadlocks from database-scoped XE ring buffers and master telemetry."""
        return await DeadlockHistoryReader(self.executor).read(
            database_name,
            max_events=max_events,
            master_available=master_available,
            include_graph_xml=include_graph_xml,
        )

    @staticmethod
    def _parse_deadlock_xml(xml_str: str) -> dict[str, Any]:
        """Parse one deadlock graph document; malformed input yields an empty graph."""
        empty: dict[str, Any] = {"participants": [], "victim_session_id": None, "resources": []}
        if not xml_str or not xml_str.strip():
            return empty
        try:
            root = ET.fromstring(xml_str)
        except ET.ParseError:
            return empty
        graph = root if root.tag == "deadlock" else root.find(".//deadlock")
        if graph is None:
            return empty
        return parse_deadlock_graph(graph)
