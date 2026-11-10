"""Memory grant pressure and memory clerk evidence for Azure SQL Database.

``avg_memory_usage_percent`` near 100 is the buffer pool doing its job. Pressure
shows in the resource semaphores: queries that wait for a grant, time out waiting
(error 8645) or run on a forced minimum grant. ``waiter_count`` is the state now;
the timeout and forced-grant counters accumulate since the engine started.
"""

from __future__ import annotations

from typing import Any

from .azure_tier import read_dmv
from .connection import AzureSqlExecutor
from .result_status import ResultStatus
from .result_status import status_payload
from .wait_stats import COUNTER_EPOCH_SQL

SEMAPHORE_DMV = "sys.dm_exec_query_resource_semaphores"
CLERK_DMV = "sys.dm_os_memory_clerks"

SEMAPHORE_SQL = """
SELECT
    resource_semaphore_id,
    pool_id,
    target_memory_kb,
    max_target_memory_kb,
    total_memory_kb,
    available_memory_kb,
    granted_memory_kb,
    used_memory_kb,
    grantee_count,
    waiter_count,
    timeout_error_count,
    forced_grant_count
FROM sys.dm_exec_query_resource_semaphores
"""

TOP_CLERKS_SQL = """
SELECT TOP (?)
    type AS clerk_type,
    SUM(pages_kb) AS pages_kb
FROM sys.dm_os_memory_clerks
GROUP BY type
ORDER BY SUM(pages_kb) DESC
"""

TOKEN_PERM_SQL = """
SELECT SUM(pages_kb) AS pages_kb
FROM sys.dm_os_memory_clerks
WHERE type = N'USERSTORE_TOKENPERM'
  AND name = N'TokenAndPermUserStore'
"""

MAX_TOP_CLERKS = 25
# Bars from the gap review (2B item 6): one waiter is pressure, five is severe;
# a security token cache of 1 GB or more is worth a look.
WAITERS_HIGH = 5
TOKEN_PERM_LARGE_MB = 1024.0


def _mb(value: Any) -> float | None:
    return None if value is None else round(float(value) / 1024.0, 1)


def _count(value: Any) -> int | None:
    return None if value is None else int(value)


class MemoryPressureService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_memory_pressure(self, database_name: str, top_clerks: int = 10) -> dict[str, Any]:
        """Resource semaphore state and the largest memory clerks, each with its own status."""

        clerk_limit = max(1, min(int(top_clerks), MAX_TOP_CLERKS))
        semaphore_rows, semaphore_gap = await read_dmv(
            self.executor, database_name, SEMAPHORE_DMV, SEMAPHORE_SQL, site="memory_pressure.semaphores"
        )
        clerk_rows, clerk_gap = await read_dmv(
            self.executor, database_name, CLERK_DMV, TOP_CLERKS_SQL, [clerk_limit], site="memory_pressure.clerks"
        )
        token_rows: list[dict[str, Any]] = []
        if clerk_gap is None:
            token_rows, clerk_gap = await read_dmv(
                self.executor, database_name, CLERK_DMV, TOKEN_PERM_SQL, site="memory_pressure.token_perm"
            )
        epoch_rows, _ = await read_dmv(
            self.executor, database_name, "sys.dm_os_sys_info", COUNTER_EPOCH_SQL, site="memory_pressure.epoch"
        )

        semaphores = semaphore_gap or _semaphore_section(semaphore_rows)
        clerks = clerk_gap or _clerk_section(clerk_rows, token_rows)
        findings = _findings(semaphores, clerks)
        payload: dict[str, Any] = {
            "database_name": database_name,
            "source_dmvs": [SEMAPHORE_DMV, CLERK_DMV],
            "collection_window": {
                "mode": "point_in_time",
                "cumulative_counters": ["timeout_error_count_since_engine_start", "forced_grant_count_since_engine_start"],
                "cumulative_counters_since_utc": (
                    epoch_rows[0].get("engine_start_time_utc") if epoch_rows else None
                ),
            },
            "resource_semaphores": semaphores,
            "memory_clerks": clerks,
            "findings": findings,
        }
        if semaphore_gap is not None and clerk_gap is not None:
            payload.update(
                status_payload(
                    ResultStatus.UNAVAILABLE,
                    "No memory source could be read. "
                    f"{semaphore_gap['result_status_reason']} {clerk_gap['result_status_reason']}",
                    remediation=semaphore_gap.get("remediation") or clerk_gap.get("remediation"),
                )
            )
        else:
            payload.update(
                status_payload(ResultStatus.OK, f"{len(findings)} memory finding(s) from the readable sources.")
            )
        return payload


def _semaphore_section(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "source_dmv": SEMAPHORE_DMV,
            "rows": [],
            **status_payload(ResultStatus.EMPTY, f"{SEMAPHORE_DMV} returned no rows."),
        }
    shaped = [
        {
            "resource_semaphore_id": row.get("resource_semaphore_id"),
            "semaphore_kind": "small_query" if row.get("resource_semaphore_id") == 1 else "regular",
            "pool_id": row.get("pool_id"),
            "target_memory_mb": _mb(row.get("target_memory_kb")),
            "max_target_memory_mb": _mb(row.get("max_target_memory_kb")),
            "total_memory_mb": _mb(row.get("total_memory_kb")),
            "available_memory_mb": _mb(row.get("available_memory_kb")),
            "granted_memory_mb": _mb(row.get("granted_memory_kb")),
            "used_memory_mb": _mb(row.get("used_memory_kb")),
            "grantee_count": _count(row.get("grantee_count")),
            "waiter_count": _count(row.get("waiter_count")),
            "timeout_error_count_since_engine_start": _count(row.get("timeout_error_count")),
            "forced_grant_count_since_engine_start": _count(row.get("forced_grant_count")),
        }
        for row in rows
    ]

    def total(key: str) -> Any:
        return sum(item[key] or 0 for item in shaped)

    return {
        "source_dmv": SEMAPHORE_DMV,
        "result_status": ResultStatus.OK.value,
        "totals": {
            "waiter_count": total("waiter_count"),
            "grantee_count": total("grantee_count"),
            "timeout_error_count_since_engine_start": total("timeout_error_count_since_engine_start"),
            "forced_grant_count_since_engine_start": total("forced_grant_count_since_engine_start"),
            "granted_memory_mb": round(total("granted_memory_mb"), 1),
            "available_memory_mb": round(total("available_memory_mb"), 1),
        },
        "rows": shaped,
    }


def _clerk_section(rows: list[dict[str, Any]], token_rows: list[dict[str, Any]]) -> dict[str, Any]:
    token_kb = token_rows[0].get("pages_kb") if token_rows else None
    return {
        "source_dmv": CLERK_DMV,
        "result_status": (ResultStatus.OK if rows else ResultStatus.EMPTY).value,
        "top_clerks": [
            {"clerk_type": row.get("clerk_type"), "pages_mb": _mb(row.get("pages_kb"))} for row in rows
        ],
        "token_and_perm_user_store_mb": _mb(token_kb or 0),
    }


def _findings(semaphores: dict[str, Any], clerks: dict[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    totals = semaphores.get("totals") or {}
    waiters = totals.get("waiter_count") or 0
    if waiters:
        findings.append(
            {
                "code": "memory_grant_waiters",
                "severity": "high" if waiters >= WAITERS_HIGH else "medium",
                "message": f"{waiters} query/queries are waiting for a memory grant right now.",
                "evidence": {"waiter_count": waiters, "window": "point_in_time", "source_dmv": SEMAPHORE_DMV},
                "next_tools": ["get_memory_grants"],
            }
        )
    for code, key, severity, text in (
        (
            "memory_grant_timeouts",
            "timeout_error_count_since_engine_start",
            "medium",
            "query/queries timed out waiting for a memory grant (error 8645)",
        ),
        (
            "forced_memory_grants",
            "forced_grant_count_since_engine_start",
            "low",
            "query/queries ran on a forced minimum memory grant and are likely to have spilled",
        ),
    ):
        count = totals.get(key) or 0
        if count:
            findings.append(
                {
                    "code": code,
                    "severity": severity,
                    "message": f"{count} {text} since the engine started; the count does not say when.",
                    "evidence": {key: count, "window": "cumulative_since_engine_start", "source_dmv": SEMAPHORE_DMV},
                    "next_tools": ["get_memory_grants", "get_wait_stats"],
                }
            )
    token_mb = clerks.get("token_and_perm_user_store_mb") or 0.0
    if token_mb >= TOKEN_PERM_LARGE_MB:
        findings.append(
            {
                "code": "token_and_perm_user_store_large",
                "severity": "medium",
                "message": (
                    f"The security token cache (TokenAndPermUserStore) holds {token_mb:,.0f} MB. A cache "
                    "this large takes memory from data and plans and can slow permission checks."
                ),
                "evidence": {"token_and_perm_user_store_mb": token_mb, "window": "point_in_time", "source_dmv": CLERK_DMV},
                "next_tools": ["get_plan_cache_analysis"],
            }
        )
    return findings
