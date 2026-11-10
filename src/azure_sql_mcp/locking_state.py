"""Optimized locking, accelerated database recovery and row-versioning state.

The one reader of these flags. ``DATABASEPROPERTYEX(..., 'IsOptimizedLockingOn')``
returns NULL when the property is not available, and a flag that could not be
read is reported as ``unknown``: reporting it as ``off`` would send lock tuning
after row and page locks that the database no longer holds.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .azure_tier import read_dmv
from .connection import AzureSqlExecutor
from .result_status import ResultStatus

LOCKING_STATE_COLUMNS = """
    CAST(DATABASEPROPERTYEX(DB_NAME(), 'IsOptimizedLockingOn') AS int) AS optimized_locking_property,
    d.is_accelerated_database_recovery_on,
    d.is_read_committed_snapshot_on,
    d.snapshot_isolation_state_desc"""

LOCKING_STATE_SQL = f"""
SELECT{LOCKING_STATE_COLUMNS}
FROM sys.databases AS d
WHERE d.database_id = DB_ID()
"""

LOCKING_STATE_SOURCE = "sys.databases; DATABASEPROPERTYEX(DB_NAME(), 'IsOptimizedLockingOn')"

_TID_NOTE = (
    "Optimized locking is on: row and page locks are released as each row is changed and one "
    "transaction ID (TID, resource type XACT) lock is held to the end of the transaction. "
    "Per-index lock counters and KEY/PAGE lock rows understate lock activity."
)
_LAQ_NOTE = (
    "Lock after qualification is not in effect because READ_COMMITTED_SNAPSHOT is off; only TID "
    "locking applies."
)
_UNKNOWN_NOTE = "A flag reported as unknown could not be read. Do not treat it as off."


def _flag(value: Any) -> str:
    if value is None or value == "":
        return "unknown"
    if isinstance(value, str):
        text = value.strip().casefold()
        if text in {"1", "on", "true"}:
            return "on"
        if text in {"0", "off", "false"}:
            return "off"
        return "unknown"
    return "on" if bool(value) else "off"


def summarize_locking_state(row: Mapping[str, Any] | None) -> dict[str, Any]:
    """Turn the locking columns of one ``sys.databases`` row into on / off / unknown."""

    row = row or {}
    optimized = _flag(row.get("optimized_locking_property"))
    rcsi = _flag(row.get("is_read_committed_snapshot_on"))
    snapshot = str(row.get("snapshot_isolation_state_desc") or "").strip().lower() or "unknown"
    if optimized == "unknown" or rcsi == "unknown":
        laq = "unknown" if optimized != "off" else "inactive"
    else:
        laq = "active" if optimized == "on" and rcsi == "on" else "inactive"
    state: dict[str, Any] = {
        "optimized_locking": optimized,
        "accelerated_database_recovery": _flag(row.get("is_accelerated_database_recovery_on")),
        "read_committed_snapshot": rcsi,
        "snapshot_isolation": snapshot,
        "lock_after_qualification": laq,
    }
    notes: list[str] = []
    if optimized == "on":
        notes.append(_TID_NOTE)
        if rcsi == "off":
            notes.append(_LAQ_NOTE)
    if "unknown" in state.values():
        notes.append(_UNKNOWN_NOTE)
    state["notes"] = notes
    return state


async def read_locking_state(executor: AzureSqlExecutor, database_name: str) -> dict[str, Any]:
    """Read the locking state of the current database; every flag is unknown when the read fails."""

    rows, gap = await read_dmv(
        executor, database_name, "sys.databases", LOCKING_STATE_SQL, site="locking_state.read"
    )
    payload: dict[str, Any] = {
        "database_name": database_name,
        "source": LOCKING_STATE_SOURCE,
        "collection_window": {"mode": "point_in_time"},
        **summarize_locking_state(rows[0] if rows else None),
    }
    if gap is not None:
        payload.update(gap)
    else:
        payload["result_status"] = (ResultStatus.OK if rows else ResultStatus.EMPTY).value
    return payload
