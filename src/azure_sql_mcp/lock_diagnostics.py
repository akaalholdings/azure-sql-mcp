from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

from .connection import AzureSqlExecutor


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
    ) -> dict[str, Any]:
        """Extract recent deadlock graphs from system_health XE session."""
        query = """
        SELECT TOP ({max_events})
            CAST(xet.target_data AS XML).value(
                '(event/@timestamp)[1]', 'DATETIME2'
            ) AS deadlock_time,
            CAST(
                CAST(xet.target_data AS XML).query(
                    'event/data[@name="xml_report"]/value/deadlock'
                ) AS NVARCHAR(MAX)
            ) AS deadlock_xml
        FROM sys.dm_xe_sessions AS xes
        INNER JOIN sys.dm_xe_session_targets AS xet
            ON xes.address = xet.event_session_address
        WHERE xes.name = 'system_health'
          AND xet.target_name = 'ring_buffer'
          AND CAST(xet.target_data AS XML).value(
                '(event/@name)[1]', 'VARCHAR(100)'
              ) = 'xml_deadlock_report'
        ORDER BY deadlock_time DESC
        """.format(max_events=int(max_events))

        try:
            rows = await self.executor.fetch_all(database_name, query)
        except Exception:
            # Fallback: the ring_buffer query shape varies across Azure SQL tiers
            rows = []

        deadlocks: list[dict[str, Any]] = []
        for row in rows:
            dl_xml = row.get("deadlock_xml") or ""
            parsed = self._parse_deadlock_xml(dl_xml)
            deadlocks.append(
                {
                    "deadlock_time": str(row.get("deadlock_time", "")),
                    "participants": parsed.get("participants", []),
                    "victim_session_id": parsed.get("victim_session_id"),
                    "resources": parsed.get("resources", []),
                }
            )

        return {
            "database_name": database_name,
            "deadlock_count": len(deadlocks),
            "deadlocks": deadlocks,
        }

    @staticmethod
    def _parse_deadlock_xml(xml_str: str) -> dict[str, Any]:
        """Best-effort parse of deadlock XML."""
        if not xml_str or not xml_str.strip():
            return {"participants": [], "victim_session_id": None, "resources": []}
        try:
            root = ET.fromstring(xml_str)
        except ET.ParseError:
            return {"participants": [], "victim_session_id": None, "resources": []}

        victim_id = None
        participants: list[dict[str, Any]] = []
        resources: list[dict[str, Any]] = []

        # Find victim
        victim_list = root.find(".//victim-list")
        if victim_list is not None:
            for v in victim_list:
                victim_id = v.get("id")

        # Find process-list
        process_list = root.find(".//process-list")
        if process_list is not None:
            for proc in process_list:
                participants.append(
                    {
                        "process_id": proc.get("id"),
                        "session_id": proc.get("spid"),
                        "is_victim": proc.get("id") == victim_id,
                        "wait_resource": proc.get("waitresource", ""),
                        "lock_mode": proc.get("lockMode", ""),
                        "sql_text": (
                            proc.findtext(".//inputbuf", default="").strip()
                        ),
                    }
                )

        # Find resource-list
        resource_list = root.find(".//resource-list")
        if resource_list is not None:
            for res in resource_list:
                resources.append(
                    {
                        "type": res.tag,
                        "attributes": dict(res.attrib),
                    }
                )

        return {
            "participants": participants,
            "victim_session_id": victim_id,
            "resources": resources,
        }
