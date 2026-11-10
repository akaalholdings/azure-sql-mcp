from __future__ import annotations

import asyncio
import re
from collections import Counter
from typing import Any

from .azure_tier import gated_read_unavailable
from .azure_tier import read_dmv
from .connection import AzureSqlExecutor
from .result_status import ResultStatus
from .result_status import status_payload


MAX_SESSION_LIMIT = 1000
PILEUP_MIN_REQUESTS = 3
MAX_PILEUPS = 10
MAX_CALLERS = 10
MAX_IDS_PER_GROUP = 20
MAX_RESOLVED_RESOURCES = 25
MAX_CHAIN_RESOURCES = 5

REQUESTS_DMV = "sys.dm_exec_requests"
TASK_SPACE_DMV = "sys.dm_db_task_space_usage"

# Net tempdb pages each running request holds now. The session-level view counts
# a task only after it ends, so a running sort or spill shows here alone.
TASK_SPACE_SQL = """
SELECT
    tsu.session_id,
    tsu.request_id,
    CAST(
        SUM(tsu.user_objects_alloc_page_count - tsu.user_objects_dealloc_page_count) * 8 / 1024.0
        AS DECIMAL(18, 2)
    ) AS tempdb_user_objects_mb,
    CAST(
        SUM(tsu.internal_objects_alloc_page_count - tsu.internal_objects_dealloc_page_count) * 8 / 1024.0
        AS DECIMAL(18, 2)
    ) AS tempdb_internal_objects_mb
FROM sys.dm_db_task_space_usage AS tsu
WHERE tsu.session_id <> @@SPID
GROUP BY tsu.session_id, tsu.request_id
HAVING SUM(tsu.user_objects_alloc_page_count + tsu.internal_objects_alloc_page_count) > 0
"""

HEAD_BLOCKERS_SQL = """
SELECT
    s.session_id,
    s.status AS session_status,
    s.open_transaction_count,
    s.login_name,
    s.host_name,
    s.program_name,
    DATEDIFF_BIG(SECOND, s.last_request_end_time, GETDATE()) AS idle_seconds
FROM sys.dm_exec_sessions AS s
WHERE s.session_id IN ({placeholders})
"""

HOBT_NAMES_SQL = """
SELECT
    DB_ID() AS database_id,
    p.hobt_id,
    OBJECT_SCHEMA_NAME(p.object_id) AS schema_name,
    OBJECT_NAME(p.object_id) AS object_name,
    i.name AS index_name
FROM sys.partitions AS p
LEFT JOIN sys.indexes AS i
    ON i.object_id = p.object_id
   AND i.index_id = p.index_id
WHERE p.hobt_id IN ({placeholders})
"""

OBJECT_NAMES_SQL = """
SELECT
    DB_ID() AS database_id,
    o.object_id,
    SCHEMA_NAME(o.schema_id) AS schema_name,
    o.name AS object_name
FROM sys.objects AS o
WHERE o.object_id IN ({placeholders})
"""

_LOCK_WAIT = re.compile(r"^LCK_M_(.+?)(?:_LOW_PRIORITY|_ABORT_BLOCKERS)?$")
# Shared and intent-shared waits are the readers that row versioning (RCSI)
# removes. Serializable range waits and schema waits stay under RCSI.
_READER_MODES = frozenset({"S", "IS"})
_SCHEMA_MODES = frozenset({"SCH_S", "SCH_M"})
_RANGE_PREFIXES = ("RS_", "RIN_", "RX_")
_HOBT_RESOURCE = re.compile(r"^(KEY|HOBT):\s*(\d+):(\d+)")
_OBJECT_RESOURCE = re.compile(r"^(OBJECT|TAB):\s*(\d+):(\d+)")
_PAGE_RESOURCE = re.compile(r"^(PAGE|RID):\s*(\d+):(\d+):(\d+)")
_RESOURCE_TYPE = re.compile(r"^([A-Z_]+)\s*:")
_NO_STATEMENT_HASH = re.compile(r"^0x0*$")


def lock_mode(wait_type: Any) -> str | None:
    """The requested lock mode of a lock wait (``LCK_M_RS_S`` -> ``RS_S``), else None."""

    match = _LOCK_WAIT.match(str(wait_type or ""))
    return match.group(1) if match else None


def _victim_kind(mode: str | None) -> str:
    if mode is None:
        return "non_lock"
    if mode in _READER_MODES:
        return "reader"
    if mode in _SCHEMA_MODES:
        return "schema"
    if mode.upper().startswith(_RANGE_PREFIXES):
        return "range"
    return "writer"


def parse_wait_resource(text: Any) -> dict[str, Any]:
    """Split ``wait_resource`` into its type and the ids the catalog can name."""

    value = str(text or "").strip()
    if not value:
        return {}
    match = _HOBT_RESOURCE.match(value)
    if match:
        return {"resource_type": match.group(1), "database_id": int(match.group(2)), "hobt_id": int(match.group(3))}
    match = _OBJECT_RESOURCE.match(value)
    if match:
        return {"resource_type": match.group(1), "database_id": int(match.group(2)), "object_id": int(match.group(3))}
    match = _PAGE_RESOURCE.match(value)
    if match:
        return {
            "resource_type": match.group(1),
            "database_id": int(match.group(2)),
            "file_id": int(match.group(3)),
            "page_id": int(match.group(4)),
        }
    match = _RESOURCE_TYPE.match(value)
    return {"resource_type": match.group(1)} if match else {}


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def query_pileups(sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Statements that three or more requests run at once, largest group first."""

    groups: dict[str, list[dict[str, Any]]] = {}
    for session in sessions:
        query_hash = session.get("query_hash")
        if not query_hash or _NO_STATEMENT_HASH.match(str(query_hash)):
            continue
        groups.setdefault(str(query_hash), []).append(session)
    pileups = [
        {
            "query_hash": query_hash,
            "request_count": len(members),
            "blocked_request_count": sum(1 for member in members if member.get("blocking_session_id")),
            "max_elapsed_time_ms": int(max(_number(member.get("elapsed_time_ms")) for member in members)),
            "total_cpu_time_ms": int(sum(_number(member.get("cpu_time_ms")) for member in members)),
            "session_ids": [member.get("session_id") for member in members[:MAX_IDS_PER_GROUP]],
        }
        for query_hash, members in groups.items()
        if len(members) >= PILEUP_MIN_REQUESTS
    ]
    pileups.sort(key=lambda item: (-item["request_count"], -item["max_elapsed_time_ms"]))
    return pileups[:MAX_PILEUPS]


def load_by_caller(sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Active requests grouped by program, host and login, busiest caller first."""

    groups: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = {}
    for session in sessions:
        key = (session.get("program_name"), session.get("host_name"), session.get("login_name"))
        groups.setdefault(key, []).append(session)
    callers = [
        {
            "program_name": program,
            "host_name": host,
            "login_name": login,
            "request_count": len(members),
            "blocked_request_count": sum(1 for member in members if member.get("blocking_session_id")),
            "total_cpu_time_ms": int(sum(_number(member.get("cpu_time_ms")) for member in members)),
            "total_elapsed_time_ms": int(sum(_number(member.get("elapsed_time_ms")) for member in members)),
            "total_granted_memory_mb": round(sum(_number(member.get("granted_memory_mb")) for member in members), 2),
        }
        for (program, host, login), members in groups.items()
    ]
    callers.sort(key=lambda item: (-item["request_count"], -item["total_cpu_time_ms"]))
    return callers[:MAX_CALLERS]


def _resource_key(parsed: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(parsed.get(name) for name in ("resource_type", "database_id", "hobt_id", "object_id", "file_id", "page_id"))


def blocking_chains(
    sessions: list[dict[str, Any]],
    *,
    head_sessions: dict[Any, dict[str, Any]] | None = None,
    resource_names: dict[tuple[Any, ...], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """One entry per head blocker: ids, depth, victim lock modes and blocked resources.

    Ids and short wait summaries only; the full rows stay in ``sessions``.
    ``head_sessions`` holds the session rows of head blockers with no request,
    and ``resource_names`` the catalog names of blocked resources, when read.
    """

    blocked = {session["session_id"]: session for session in sessions if session.get("blocking_session_id")}
    if not blocked:
        return []
    by_id = {session.get("session_id"): session for session in sessions}
    children: dict[Any, list[dict[str, Any]]] = {}
    for session in blocked.values():
        children.setdefault(session["blocking_session_id"], []).append(session)

    heads: list[tuple[Any, bool]] = [(head, False) for head in sorted(children) if head not in blocked]
    reached: set[Any] = set()
    chains: list[dict[str, Any]] = []
    while True:
        if not heads:
            # Sessions that only block each other (a deadlock in progress) have no head.
            leftover = sorted(set(blocked) - reached)
            if not leftover:
                break
            heads = [(leftover[0], True)]
        head, cycle = heads.pop(0)
        direct = children.get(head, [])
        victims: list[dict[str, Any]] = []
        depth = 0
        level = direct
        seen = {head}
        while level:
            level = [victim for victim in level if victim["session_id"] not in seen]
            if not level:
                break
            depth += 1
            victims.extend(level)
            seen.update(victim["session_id"] for victim in level)
            level = [child for victim in level for child in children.get(victim["session_id"], [])]
        if cycle:
            # The chosen head waits too, on a session inside its own chain.
            victims.append(blocked[head])
        reached.update(victim["session_id"] for victim in victims)

        modes = [lock_mode(victim.get("wait_type")) for victim in victims]
        split = Counter(_victim_kind(mode) for mode in modes)
        direct_ids = [victim["session_id"] for victim in direct]
        direct_set = set(direct_ids)
        chain: dict[str, Any] = {
            "head_blocker_session_id": head,
            "head_blocker_has_active_request": head in by_id,
            **_head_blocker(head, by_id, head_sessions),
            "blocked_session_ids": direct_ids,
            "indirect_blocked_session_ids": [
                victim["session_id"] for victim in victims if victim["session_id"] not in direct_set
            ],
            "total_blocked_count": len(victims),
            "chain_depth": depth,
            "max_wait_time_ms": int(max((_number(victim.get("wait_time_ms")) for victim in victims), default=0)),
            "victims_by_lock_mode": dict(Counter(mode for mode in modes if mode)),
            "victim_split": {kind: split.get(kind, 0) for kind in ("reader", "writer", "range", "schema", "non_lock")},
            "reader_victim_share": round(split.get("reader", 0) / len(victims), 2) if victims else None,
            "blocked_resources": _blocked_resources(victims, resource_names or {}),
            "blocked_session_waits": [
                {
                    "session_id": victim["session_id"],
                    "wait_type": victim.get("wait_type"),
                    "lock_mode": lock_mode(victim.get("wait_type")),
                    "wait_time_ms": victim.get("wait_time_ms"),
                    "wait_resource": victim.get("wait_resource"),
                }
                for victim in direct
            ],
        }
        if cycle:
            chain["cycle"] = True
        chains.append(chain)
    return chains


def _head_blocker(
    head: Any,
    by_id: dict[Any, dict[str, Any]],
    head_sessions: dict[Any, dict[str, Any]] | None,
) -> dict[str, Any]:
    request = by_id.get(head)
    if request is not None:
        return {
            "head_blocker_state": "running",
            "head_blocker": {
                "login_name": request.get("login_name"),
                "host_name": request.get("host_name"),
                "program_name": request.get("program_name"),
                "open_transaction_count": request.get("open_transaction_count"),
            },
        }
    row = (head_sessions or {}).get(head)
    if row is None:
        # Not read, or the read failed: an idle transaction must not be assumed.
        return {"head_blocker_state": "unknown", "head_blocker": None}
    open_transactions = int(_number(row.get("open_transaction_count")))
    return {
        "head_blocker_state": "sleeping_open_transaction" if open_transactions > 0 else "sleeping",
        "head_blocker": {
            "login_name": row.get("login_name"),
            "host_name": row.get("host_name"),
            "program_name": row.get("program_name"),
            "open_transaction_count": open_transactions,
            "idle_seconds": row.get("idle_seconds"),
        },
    }


def _blocked_resources(
    victims: list[dict[str, Any]],
    resource_names: dict[tuple[Any, ...], dict[str, Any]],
) -> list[dict[str, Any]]:
    counts: Counter[tuple[Any, ...]] = Counter()
    parsed_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for victim in victims:
        parsed = parse_wait_resource(victim.get("wait_resource"))
        if not parsed:
            continue
        key = _resource_key(parsed)
        counts[key] += 1
        parsed_by_key[key] = parsed
    return [
        {**parsed_by_key[key], **resource_names.get(key, {}), "waiter_count": count}
        for key, count in counts.most_common(MAX_CHAIN_RESOURCES)
    ]


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
            r.request_id,
            s.login_name,
            s.host_name,
            s.program_name,
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
            r.dop,
            r.parallel_worker_count,
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
            CONVERT(varchar(130), r.query_plan_hash, 1) AS query_plan_hash,
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
        try:
            sessions = await self.executor.fetch_all(
                database_name, query, params=[bounded_limit + 1],
            )
        except Exception as exc:
            blocked = await gated_read_unavailable(
                self.executor, database_name, REQUESTS_DMV, exc, site="sessions.active"
            )
            if blocked is None:
                raise
            return {
                "database_name": database_name,
                "active_session_count": 0,
                "limit": bounded_limit,
                "truncated": False,
                "sessions": [],
                "blocking_chains": [],
                **blocked,
            }
        truncated = len(sessions) > bounded_limit
        sessions = sessions[:bounded_limit]
        tempdb_usage, head_sessions, resource_names = await self._side_reads(database_name, sessions)
        return {
            "database_name": database_name,
            "source_dmvs": [REQUESTS_DMV, "sys.dm_exec_sessions", TASK_SPACE_DMV],
            "collection_window": {"mode": "point_in_time"},
            "active_session_count": len(sessions),
            "limit": bounded_limit,
            "truncated": truncated,
            "sessions": sessions,
            "tempdb_usage": tempdb_usage,
            "pileups": query_pileups(sessions),
            "load_by_caller": load_by_caller(sessions),
            "blocking_chains": blocking_chains(
                sessions, head_sessions=head_sessions, resource_names=resource_names
            ),
        }

    async def _side_reads(
        self,
        database_name: str,
        sessions: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[Any, dict[str, Any]], dict[tuple[Any, ...], dict[str, Any]]]:
        """Tempdb per request, idle head blockers and blocked-resource names.

        Each is optional: a failure leaves that detail unknown and keeps the rest.
        The catalog reads run only while something is blocked.
        """

        if not sessions:
            return (
                {"source_dmv": TASK_SPACE_DMV, **status_payload(ResultStatus.EMPTY, "No active request to measure.")},
                {},
                {},
            )
        active_ids = {session.get("session_id") for session in sessions}
        victims = [session for session in sessions if session.get("blocking_session_id")]
        idle_heads = sorted(
            {
                victim["blocking_session_id"]
                for victim in victims
                if isinstance(victim["blocking_session_id"], int)
                and victim["blocking_session_id"] > 0
                and victim["blocking_session_id"] not in active_ids
            }
        )[:MAX_IDS_PER_GROUP]
        parsed = [parse_wait_resource(victim.get("wait_resource")) for victim in victims]
        hobt_ids = sorted({item["hobt_id"] for item in parsed if "hobt_id" in item})[:MAX_RESOLVED_RESOURCES]
        object_ids = sorted({item["object_id"] for item in parsed if "object_id" in item})[:MAX_RESOLVED_RESOURCES]

        async def none() -> tuple[list[dict[str, Any]], None]:
            return [], None

        def by_ids(dmv: str, template: str, ids: list[int], site: str) -> Any:
            if not ids:
                return none()
            sql = template.format(placeholders=", ".join("?" for _ in ids))
            return read_dmv(self.executor, database_name, dmv, sql, ids, site=site)

        (tempdb_rows, tempdb_gap), (head_rows, _), (hobt_rows, _), (object_rows, _) = await asyncio.gather(
            read_dmv(self.executor, database_name, TASK_SPACE_DMV, TASK_SPACE_SQL, site="sessions.tempdb"),
            by_ids("sys.dm_exec_sessions", HEAD_BLOCKERS_SQL, idle_heads, "sessions.head_blockers"),
            by_ids("sys.partitions", HOBT_NAMES_SQL, hobt_ids, "sessions.hobt_names"),
            by_ids("sys.objects", OBJECT_NAMES_SQL, object_ids, "sessions.object_names"),
        )

        usage = {(row.get("session_id"), row.get("request_id")): row for row in tempdb_rows}
        for session in sessions:
            row = usage.get((session.get("session_id"), session.get("request_id")), {})
            user_mb = None if tempdb_gap else _number(row.get("tempdb_user_objects_mb"))
            internal_mb = None if tempdb_gap else _number(row.get("tempdb_internal_objects_mb"))
            session["tempdb_user_objects_mb"] = user_mb
            session["tempdb_internal_objects_mb"] = internal_mb
            session["tempdb_allocated_mb"] = (
                None if user_mb is None or internal_mb is None else round(user_mb + internal_mb, 2)
            )
        tempdb_usage = tempdb_gap or {"source_dmv": TASK_SPACE_DMV, "result_status": ResultStatus.OK.value}

        names: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in hobt_rows:
            for resource_type in ("KEY", "HOBT"):
                names[(resource_type, row.get("database_id"), row.get("hobt_id"), None, None, None)] = {
                    "schema_name": row.get("schema_name"),
                    "object_name": row.get("object_name"),
                    "index_name": row.get("index_name"),
                }
        for row in object_rows:
            for resource_type in ("OBJECT", "TAB"):
                names[(resource_type, row.get("database_id"), None, row.get("object_id"), None, None)] = {
                    "schema_name": row.get("schema_name"),
                    "object_name": row.get("object_name"),
                }
        return tempdb_usage, {row.get("session_id"): row for row in head_rows}, names

    def _detect_blocking_chains(
        self,
        sessions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        return blocking_chains(sessions)
