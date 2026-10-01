"""Persistent version store (ADR) health for Azure SQL Database.

Accelerated database recovery is always on in Azure SQL Database, so row
versions live in the user database's persistent version store (PVS). A long
open transaction or snapshot reader stops cleanup and the PVS grows, consuming
storage. This reads the PVS state, the transactions that can hold cleanup back,
and turns them into findings.
"""

from __future__ import annotations

from typing import Any

from .connection import AzureSqlExecutor
from .observability import sanitize_error_message
from .result_status import ResultStatus
from .result_status import status_payload

PVS_STATS_SQL = """
SELECT *
FROM sys.dm_tran_persistent_version_store_stats
WHERE database_id = DB_ID()
"""

DATA_SPACE_SQL = """
SELECT
    SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint)) * 8 / 1024.0 AS data_used_mb,
    SUM(CAST(size AS bigint)) * 8 / 1024.0 AS data_allocated_mb
FROM sys.database_files
WHERE type_desc = N'ROWS'
"""

OLDEST_TRANSACTIONS_SQL = """
SELECT TOP (5)
    at.transaction_id,
    at.name AS transaction_name,
    CONVERT(varchar(33), at.transaction_begin_time, 126) AS begin_time,
    DATEDIFF(SECOND, at.transaction_begin_time, SYSDATETIME()) AS age_seconds,
    st.session_id,
    s.login_name,
    s.host_name,
    s.program_name,
    s.status AS session_status
FROM sys.dm_tran_active_transactions AS at
LEFT JOIN sys.dm_tran_session_transactions AS st
    ON st.transaction_id = at.transaction_id
LEFT JOIN sys.dm_exec_sessions AS s
    ON s.session_id = st.session_id
WHERE st.session_id IS NOT NULL
ORDER BY at.transaction_begin_time
"""

SNAPSHOT_TRANSACTIONS_SQL = """
SELECT TOP (5)
    session_id,
    transaction_id,
    elapsed_time_seconds,
    is_snapshot
FROM sys.dm_tran_active_snapshot_database_transactions
ORDER BY elapsed_time_seconds DESC
"""

LARGE_PVS_PCT = 10.0
LARGE_PVS_MB = 10 * 1024
LONG_TRANSACTION_SECONDS = 15 * 60


class VersionStoreService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_version_store_stats(self, database_name: str) -> dict[str, Any]:
        try:
            rows = await self.executor.fetch_all(database_name, PVS_STATS_SQL)
        except Exception as exc:
            return {
                "database_name": database_name,
                **status_payload(
                    ResultStatus.UNAVAILABLE,
                    "sys.dm_tran_persistent_version_store_stats could not be read: "
                    + sanitize_error_message(str(exc))
                    + ". VIEW DATABASE STATE is required.",
                ),
            }
        if not rows:
            return {
                "database_name": database_name,
                **status_payload(
                    ResultStatus.UNAVAILABLE,
                    "The persistent version store view returned no row for this database.",
                ),
            }
        row = rows[0]
        gaps: list[str] = []
        space = await self._optional(database_name, DATA_SPACE_SQL, gaps, "data file space")
        oldest = await self._optional(database_name, OLDEST_TRANSACTIONS_SQL, gaps, "active transactions")
        snapshots = await self._optional(
            database_name, SNAPSHOT_TRANSACTIONS_SQL, gaps, "snapshot transactions"
        )
        pvs_mb = _float(row.get("persistent_version_store_size_kb")) / 1024.0
        used_mb = _float(space[0].get("data_used_mb")) if space else 0.0
        pvs = {
            "size_mb": round(pvs_mb, 1),
            "online_index_version_store_mb": round(
                _float(row.get("online_index_version_store_size_kb")) / 1024.0, 1
            ),
            "pct_of_used_data": round(100.0 * pvs_mb / used_mb, 2) if used_mb > 0 else None,
            "current_aborted_transaction_count": _int(row.get("current_aborted_transaction_count")),
            "oldest_active_transaction_id": row.get("oldest_active_transaction_id"),
            "offrow_version_cleaner_start_time": _text(row.get("offrow_version_cleaner_start_time")),
            "offrow_version_cleaner_end_time": _text(row.get("offrow_version_cleaner_end_time")),
            "aborted_version_cleaner_end_time": _text(row.get("aborted_version_cleaner_end_time")),
        }
        transactions = [
            {
                "transaction_id": item.get("transaction_id"),
                "transaction_name": item.get("transaction_name"),
                "age_seconds": _int(item.get("age_seconds")),
                "session_id": item.get("session_id"),
                "login_name": item.get("login_name"),
                "host_name": item.get("host_name"),
                "program_name": item.get("program_name"),
                "session_status": item.get("session_status"),
            }
            for item in oldest
        ]
        snapshot_rows = [
            {
                "session_id": item.get("session_id"),
                "transaction_id": item.get("transaction_id"),
                "elapsed_seconds": _int(item.get("elapsed_time_seconds")),
                "is_snapshot": bool(item.get("is_snapshot")),
            }
            for item in snapshots
        ]
        return {
            "database_name": database_name,
            "persistent_version_store": pvs,
            "data_used_mb": round(used_mb, 1) if space else None,
            "oldest_transactions": transactions,
            "snapshot_transactions": snapshot_rows,
            "findings": _findings(pvs, transactions, snapshot_rows),
            "gaps": gaps,
        }

    async def _optional(
        self, database_name: str, query: str, gaps: list[str], label: str
    ) -> list[dict[str, Any]]:
        try:
            return await self.executor.fetch_all(database_name, query)
        except Exception as exc:
            gaps.append(f"{label} could not be read: {sanitize_error_message(str(exc))}")
            return []


def _findings(
    pvs: dict[str, Any],
    transactions: list[dict[str, Any]],
    snapshots: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    size_mb = float(pvs["size_mb"] or 0.0)
    pct = pvs.get("pct_of_used_data")
    large = size_mb >= LARGE_PVS_MB or (pct is not None and pct >= LARGE_PVS_PCT)
    if large:
        findings.append(
            {
                "code": "persistent_version_store_large",
                "severity": "high" if (pct or 0) >= 25 or size_mb >= 5 * LARGE_PVS_MB else "medium",
                "message": (
                    f"The persistent version store holds {size_mb:,.0f} MB"
                    + (f" ({pct:.1f}% of used data space)." if pct is not None else ".")
                ),
                "next_tools": ["get_open_transactions", "get_active_sessions"],
            }
        )
    holders = [item for item in transactions if (item["age_seconds"] or 0) >= LONG_TRANSACTION_SECONDS]
    holders += [
        {"session_id": item["session_id"], "age_seconds": item["elapsed_seconds"], "snapshot": True}
        for item in snapshots
        if (item["elapsed_seconds"] or 0) >= LONG_TRANSACTION_SECONDS
    ]
    if holders:
        oldest = max(holders, key=lambda item: item.get("age_seconds") or 0)
        findings.append(
            {
                "code": "version_cleanup_held_by_long_transaction",
                "severity": "high" if large else "medium",
                "message": (
                    f"Session {oldest.get('session_id')} has held a "
                    f"{'snapshot ' if oldest.get('snapshot') else ''}transaction open for "
                    f"{(oldest.get('age_seconds') or 0) / 60:,.0f} minutes; version cleanup "
                    "cannot pass it."
                ),
                "evidence": {"holders": holders[:5]},
                "next_tools": ["get_open_transactions", "get_active_sessions"],
                "fix": (
                    "Have the owning application commit or roll back. Ending the session is a "
                    "DBA decision: it rolls back the open work."
                ),
            }
        )
    aborted = pvs.get("current_aborted_transaction_count") or 0
    if aborted:
        findings.append(
            {
                "code": "aborted_transactions_awaiting_cleanup",
                "severity": "info",
                "message": f"{aborted} aborted transaction(s) are waiting for version cleanup.",
                "next_tools": [],
            }
        )
    return findings


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)
