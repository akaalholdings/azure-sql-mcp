from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

from .azure_tier import gated_read_unavailable
from .connection import AzureSqlExecutor
from .deadlocks import DeadlockHistoryReader
from .deadlocks import parse_deadlock_graph


MAX_ROW_LIMIT = 1000
LOCKS_DMV = "sys.dm_tran_locks"
DATABASE_TRANSACTIONS_DMV = "sys.dm_tran_database_transactions"

# Totals over user sessions, apart from the bounded row read, so truncation
# cannot hide a lock storm.
LOCK_TOTALS_SQL = """
SELECT
    tl.resource_type,
    COUNT_BIG(*) AS lock_count,
    SUM(CASE WHEN tl.request_status = 'WAIT' THEN 1 ELSE 0 END) AS waiting_count
FROM sys.dm_tran_locks AS tl
WHERE tl.resource_database_id = DB_ID()
  AND EXISTS (
        SELECT 1
        FROM sys.dm_exec_sessions AS us
        WHERE us.session_id = tl.request_session_id
          AND us.is_user_process = 1
  )
GROUP BY tl.resource_type
"""


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
        system; the rows are bounded with waiting locks sorted first, and the
        totals come from a separate aggregate so truncation cannot hide a storm.
        The two reads are not one snapshot, so the totals can differ slightly
        from the rows.
        """
        bounded_limit = _clamp_limit(limit, default=200)
        try:
            totals = await self.executor.fetch_all(database_name, LOCK_TOTALS_SQL)
        except Exception as exc:
            blocked = await gated_read_unavailable(
                self.executor, database_name, LOCKS_DMV, exc, site="lock_diagnostics.locks"
            )
            if blocked is None:
                raise
            return {
                "database_name": database_name,
                "total_locks": None,
                "waiting_locks": None,
                "locks_by_resource_type": {},
                "returned_locks": 0,
                "limit": bounded_limit,
                "truncated": False,
                "locks": [],
                **blocked,
            }
        # TOP is taken in the CTE so the joins and dm_exec_sql_text run for the
        # returned rows only, not once per lock. KEY, PAGE, RID and HOBT locks
        # carry a hobt_id, which sys.partitions maps to the object.
        query = """
        WITH l AS (
            SELECT TOP (?)
                tl.resource_type,
                tl.resource_subtype,
                tl.request_mode,
                tl.request_status,
                tl.request_session_id,
                tl.resource_description,
                tl.resource_associated_entity_id
            FROM sys.dm_tran_locks AS tl
            WHERE tl.resource_database_id = DB_ID()
              AND EXISTS (
                    SELECT 1
                    FROM sys.dm_exec_sessions AS us
                    WHERE us.session_id = tl.request_session_id
                      AND us.is_user_process = 1
              )
            ORDER BY
                CASE WHEN tl.request_status = 'WAIT' THEN 0 ELSE 1 END,
                tl.resource_type,
                tl.request_mode
        )
        SELECT
            l.resource_type,
            l.resource_subtype,
            l.request_mode,
            l.request_status,
            l.request_session_id AS session_id,
            l.resource_description,
            CASE
                WHEN l.resource_type = 'OBJECT'
                    THEN OBJECT_NAME(l.resource_associated_entity_id)
                WHEN l.resource_type IN ('KEY', 'PAGE', 'RID', 'HOBT')
                    THEN OBJECT_NAME(p.object_id)
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
        FROM l
        INNER JOIN sys.dm_exec_sessions AS s
            ON l.request_session_id = s.session_id
        LEFT JOIN sys.dm_exec_requests AS r
            ON l.request_session_id = r.session_id
        LEFT JOIN sys.partitions AS p
            ON p.hobt_id = l.resource_associated_entity_id
           AND l.resource_type IN ('KEY', 'PAGE', 'RID', 'HOBT')
        OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
        ORDER BY
            CASE WHEN l.request_status = 'WAIT' THEN 0 ELSE 1 END,
            l.resource_type,
            l.request_mode
        """
        rows = await self.executor.fetch_all(
            database_name, query, params=[bounded_limit + 1],
        )
        truncated = len(rows) > bounded_limit
        rows = rows[:bounded_limit]

        by_type = {
            row["resource_type"]: int(row.get("lock_count") or 0) for row in totals
        }

        return {
            "database_name": database_name,
            "source_dmv": LOCKS_DMV,
            "collection_window": {"mode": "point_in_time"},
            "total_locks": sum(by_type.values()),
            "waiting_locks": sum(int(row.get("waiting_count") or 0) for row in totals),
            "locks_by_resource_type": by_type,
            "returned_locks": len(rows),
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
        try:
            rows = await self.executor.fetch_all(
                database_name, query, params=[bounded_limit + 1],
            )
        except Exception as exc:
            blocked = await gated_read_unavailable(
                self.executor,
                database_name,
                DATABASE_TRANSACTIONS_DMV,
                exc,
                site="lock_diagnostics.open_transactions",
            )
            if blocked is None:
                raise
            return {
                "database_name": database_name,
                "open_transaction_count": 0,
                "limit": bounded_limit,
                "truncated": False,
                "transactions": [],
                "warnings": [],
                **blocked,
            }
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
            "source_dmv": DATABASE_TRANSACTIONS_DMV,
            "collection_window": {"mode": "point_in_time"},
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
