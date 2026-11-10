"""Deadlock history for Azure SQL Database.

Azure SQL Database has no server-scoped ``system_health`` session. Deadlocks are
captured by two sources, and this module reads both:

1. Database-scoped Extended Events sessions that include the
   ``database_xml_deadlock_report`` event with a ``ring_buffer`` target. The ring
   buffer is memory-resident: a failover or session restart clears it.
2. Azure's file-backed deadlock telemetry,
   ``sys.fn_xe_telemetry_blob_target_read_file('dl', ...)``. It is master-scoped:
   from a user database the same call returns zero rows without an error, so it is
   read only through a master connection and never treated as a negative from a
   user database.

A read that cannot reach any source never reports "0 deadlocks"; it reports
``precondition`` with the exact capture DDL, or ``unavailable`` with the reason.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from .azure_tier import dmv_permission_hint
from .connection import AzureSqlExecutor
from .incident_log import note_exception
from .observability import sanitize_error_message
from .result_status import ResultStatus
from .result_status import status_payload

DEADLOCK_EVENT = "database_xml_deadlock_report"
SERVER_DEADLOCK_EVENT = "xml_deadlock_report"
SUGGESTED_SESSION_NAME = "azure_sql_mcp_deadlocks"
MAX_SQL_TEXT_CHARS = 2000
MAX_GRAPH_XML_CHARS = 200_000
MAX_EVENTS = 200

CAPTURE_SESSION_DDL = f"""CREATE EVENT SESSION [{SUGGESTED_SESSION_NAME}] ON DATABASE
ADD EVENT sqlserver.{DEADLOCK_EVENT}
ADD TARGET package0.ring_buffer (SET max_events_limit = 200, max_memory = 4096)
WITH (STARTUP_STATE = ON);
ALTER EVENT SESSION [{SUGGESTED_SESSION_NAME}] ON DATABASE STATE = START;"""

RUNNING_SESSIONS_SQL = f"""
SELECT
    s.name AS session_name,
    t.target_name,
    CAST(t.target_data AS nvarchar(max)) AS target_data
FROM sys.dm_xe_database_sessions AS s
INNER JOIN sys.dm_xe_database_session_targets AS t
    ON t.event_session_address = s.address
WHERE EXISTS (
    SELECT 1
    FROM sys.dm_xe_database_session_events AS e
    WHERE e.event_session_address = s.address
      AND e.event_name = N'{DEADLOCK_EVENT}'
)
"""

DEFINED_SESSIONS_SQL = f"""
SELECT s.name AS session_name
FROM sys.database_event_sessions AS s
WHERE EXISTS (
    SELECT 1
    FROM sys.database_event_session_events AS e
    WHERE e.event_session_id = s.event_session_id
      AND e.name = N'{DEADLOCK_EVENT}'
)
"""

# The 'dl' telemetry holds every database of the logical server, so the filter
# and the bound run in master. Parameters: row limit, then database name.
MASTER_TELEMETRY_SQL = """
SELECT TOP (?) x.event_data
FROM (
    SELECT
        CAST(t.event_data AS xml) AS event_xml,
        CAST(t.event_data AS nvarchar(max)) AS event_data
    FROM sys.fn_xe_telemetry_blob_target_read_file(N'dl', NULL, NULL, NULL) AS t
) AS x
WHERE x.event_xml.value('(/event/data[@name="database_name"]/value)[1]', 'sysname') = ?
ORDER BY x.event_xml.value('(/event/@timestamp)[1]', 'datetime2') DESC
"""

LOST_EVENTS_REMEDIATION = (
    "The ring buffer lost or cut deadlock events. Recreate the capture session with a smaller "
    "max_events_limit (a large ring buffer is cut when it is rendered as XML), or add an "
    "event_file target for durable capture."
)


@dataclass(frozen=True)
class RingBufferRead:
    """One ring_buffer target document: its deadlock events and what it lost."""

    events: list[dict[str, Any]]
    parse_failed: bool = False
    truncated: bool = False
    event_count: int | None = None
    dropped_count: int = 0
    # Deadlock events whose graph could not be read.
    unreadable_events: int = 0

    @property
    def lost_events(self) -> bool:
        return self.parse_failed or self.truncated or self.dropped_count > 0 or self.unreadable_events > 0


@dataclass
class _SourceResult:
    source: str
    status: ResultStatus
    detail: str
    sessions: list[str] = field(default_factory=list)
    remediation: str | None = None
    # Ring buffer loss counters; ``incomplete`` when events were cut, dropped or unreadable.
    extra: dict[str, Any] = field(default_factory=dict)
    incomplete: bool = False

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "source": self.source,
            "status": self.status.value,
            "detail": self.detail,
            **self.extra,
        }
        if self.incomplete:
            payload["incomplete"] = True
        if self.sessions:
            payload["sessions"] = self.sessions
        if self.remediation:
            payload["remediation"] = self.remediation
        return payload


class DeadlockHistoryReader:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def read(
        self,
        database_name: str,
        *,
        max_events: int = 10,
        master_available: bool = False,
        include_graph_xml: bool = False,
    ) -> dict[str, Any]:
        limit = max(1, min(int(max_events), MAX_EVENTS))
        events: list[dict[str, Any]] = []
        sources: list[_SourceResult] = []

        database_source, database_events = await self._read_database_sessions(database_name)
        sources.append(database_source)
        events.extend(database_events)

        telemetry_source, telemetry_events = await self._read_master_telemetry(
            database_name, master_available=master_available, limit=limit
        )
        sources.append(telemetry_source)
        events.extend(telemetry_events)

        deadlocks = _dedupe_newest_first(events)[:limit]
        rendered = [
            _render_deadlock(event, include_graph_xml=include_graph_xml) for event in deadlocks
        ]
        payload: dict[str, Any] = {
            "database_name": database_name,
            "deadlock_count": len(rendered),
            "deadlocks": rendered,
            "sources": [source.as_dict() for source in sources],
            "max_events": limit,
        }
        payload.update(_overall_status(rendered, sources))
        return payload

    async def _read_database_sessions(
        self, database_name: str
    ) -> tuple[_SourceResult, list[dict[str, Any]]]:
        source = "database_xe_ring_buffer"
        try:
            rows = await self.executor.fetch_all(database_name, RUNNING_SESSIONS_SQL)
        except Exception as exc:  # permission or transient failure
            note_exception(exc, "deadlocks.database_sessions")
            return (
                _SourceResult(
                    source,
                    ResultStatus.UNAVAILABLE,
                    "Database-scoped XE sessions could not be read: "
                    f"{sanitize_error_message(str(exc))}. "
                    + dmv_permission_hint("sys.dm_xe_database_sessions"),
                ),
                [],
            )

        sessions = sorted({str(row.get("session_name") or "") for row in rows} - {""})
        ring_rows = [row for row in rows if str(row.get("target_name") or "") == "ring_buffer"]
        if not sessions:
            return await self._missing_session_result(database_name, source), []
        if not ring_rows:
            return (
                _SourceResult(
                    source,
                    ResultStatus.PRECONDITION,
                    "Running deadlock sessions write only to targets this tool does not read "
                    "(for example event_file). Add a ring_buffer target to read them here.",
                    sessions=sessions,
                    remediation=CAPTURE_SESSION_DDL,
                ),
                [],
            )

        reads = [
            read_ring_buffer(
                str(row.get("target_data") or ""),
                source=source,
                session_name=str(row.get("session_name") or ""),
            )
            for row in ring_rows
        ]
        events = [event for read in reads for event in read.events]
        failed = sum(1 for read in reads if read.parse_failed)
        dropped = sum(read.dropped_count for read in reads)
        unreadable = sum(read.unreadable_events for read in reads)
        truncated = any(read.truncated for read in reads)
        incomplete = any(read.lost_events for read in reads)
        detail = (
            f"Read {len(ring_rows)} running ring_buffer target(s). Ring buffers hold recent "
            "events only and are cleared by failover or session restart."
        )
        if incomplete:
            detail += (
                f" Events were lost: {failed} target document(s) could not be parsed, truncated="
                f"{'yes' if truncated else 'no'}, {dropped} dropped, {unreadable} with an "
                "unreadable graph."
            )
        if events:
            status = ResultStatus.OK
        elif incomplete:
            # Lost events with nothing left is not "no deadlocks".
            status = ResultStatus.UNAVAILABLE
        else:
            status = ResultStatus.EMPTY
        return (
            _SourceResult(
                source,
                status,
                detail,
                sessions=sessions,
                remediation=LOST_EVENTS_REMEDIATION if incomplete else None,
                extra={
                    "truncated": truncated,
                    "event_count": sum(read.event_count or 0 for read in reads),
                    "dropped_count": dropped,
                    "unparsed_targets": failed,
                    "unreadable_events": unreadable,
                },
                incomplete=incomplete,
            ),
            events,
        )

    async def _missing_session_result(self, database_name: str, source: str) -> _SourceResult:
        try:
            defined = await self.executor.fetch_all(database_name, DEFINED_SESSIONS_SQL)
        except Exception:
            defined = []
        stopped = sorted({str(row.get("session_name") or "") for row in defined} - {""})
        if stopped:
            start_ddl = "\n".join(
                f"ALTER EVENT SESSION [{name}] ON DATABASE STATE = START;" for name in stopped
            )
            return _SourceResult(
                source,
                ResultStatus.PRECONDITION,
                "A deadlock capture session is defined but not running.",
                sessions=stopped,
                remediation=start_ddl,
            )
        return _SourceResult(
            source,
            ResultStatus.PRECONDITION,
            "No database-scoped Extended Events session captures "
            f"{DEADLOCK_EVENT} in this database.",
            remediation=CAPTURE_SESSION_DDL,
        )

    async def _read_master_telemetry(
        self, database_name: str, *, master_available: bool, limit: int
    ) -> tuple[_SourceResult, list[dict[str, Any]]]:
        source = "master_deadlock_telemetry"
        if not master_available:
            return (
                _SourceResult(
                    source,
                    ResultStatus.PRECONDITION,
                    "Azure's file-backed deadlock telemetry is read from master only. Add "
                    "master to AZURE_SQL_ALLOWED_DATABASES (the login needs master access) "
                    "to include it.",
                ),
                [],
            )
        try:
            rows = await self.executor.fetch_all(
                "master", MASTER_TELEMETRY_SQL, params=[limit, database_name]
            )
        except Exception as exc:
            note_exception(exc, "deadlocks.master_telemetry")
            return (
                _SourceResult(
                    source,
                    ResultStatus.UNAVAILABLE,
                    "Deadlock telemetry could not be read from master: "
                    f"{sanitize_error_message(str(exc))}",
                ),
                [],
            )
        events: list[dict[str, Any]] = []
        for row in rows:
            event = parse_telemetry_event(str(row.get("event_data") or ""), source=source)
            if event is None:
                continue
            event_database = event.get("database_name")
            if event_database and str(event_database).casefold() != database_name.casefold():
                continue
            events.append(event)
        status = ResultStatus.OK if events else ResultStatus.EMPTY
        return (
            _SourceResult(
                source,
                status,
                f"Read the newest {limit} telemetry event(s) at most for {database_name} from "
                f"master; {len(events)} returned. Events with no database name are not included.",
            ),
            events,
        )


def read_ring_buffer(target_data: str, *, source: str, session_name: str) -> RingBufferRead:
    """Parse a ring_buffer target document and report what it lost.

    ``truncated`` and ``droppedCount`` are the target's own loss counters. Roll-off
    under ``max_events_limit`` is retention, not loss, and is not counted.
    """

    try:
        root = ET.fromstring(target_data)
    except ET.ParseError:
        # An empty or cut document: the target could not be read at all.
        return RingBufferRead([], parse_failed=True)
    events: list[dict[str, Any]] = []
    unreadable = 0
    for event in root.iter("event"):
        if event.get("name") not in {DEADLOCK_EVENT, SERVER_DEADLOCK_EVENT}:
            continue
        parsed = _event_payload(event, source=source)
        if parsed is None:
            unreadable += 1
            continue
        parsed["session_name"] = session_name
        events.append(parsed)
    return RingBufferRead(
        events,
        truncated=str(root.get("truncated") or "0") not in {"0", ""},
        event_count=_int_or_none(root.get("eventCount")),
        dropped_count=_int_or_none(root.get("droppedCount")) or 0,
        unreadable_events=unreadable,
    )


def parse_ring_buffer(target_data: str, *, source: str, session_name: str) -> list[dict[str, Any]]:
    """The deadlock events of a ring_buffer target document."""

    return read_ring_buffer(target_data, source=source, session_name=session_name).events


def parse_telemetry_event(event_data: str, *, source: str) -> dict[str, Any] | None:
    if not event_data.strip():
        return None
    try:
        event = ET.fromstring(event_data)
    except ET.ParseError:
        return None
    if event.tag != "event":
        nested = event.find(".//event")
        if nested is None:
            return None
        event = nested
    if event.get("name") not in {DEADLOCK_EVENT, SERVER_DEADLOCK_EVENT}:
        return None
    return _event_payload(event, source=source)


def _event_payload(event: ET.Element, *, source: str) -> dict[str, Any] | None:
    graph = event.find("./data[@name='xml_report']/value/deadlock")
    if graph is None:
        return None
    database_name = event.findtext("./data[@name='database_name']/value")
    return {
        "timestamp_utc": event.get("timestamp") or "",
        "source": source,
        "database_name": database_name.strip() if database_name else None,
        "graph": graph,
    }


def _dedupe_newest_first(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[tuple[str, tuple[str, ...]], dict[str, Any]] = {}
    for event in events:
        graph: ET.Element = event["graph"]
        process_ids = tuple(
            sorted(str(process.get("id") or "") for process in graph.iter("process"))
        )
        key = (str(event.get("timestamp_utc") or "")[:19], process_ids)
        existing = seen.get(key)
        if existing is None:
            seen[key] = dict(event, also_seen_in=[])
        elif event["source"] != existing["source"]:
            existing["also_seen_in"].append(event["source"])
    return sorted(seen.values(), key=lambda item: str(item.get("timestamp_utc") or ""), reverse=True)


def _render_deadlock(event: dict[str, Any], *, include_graph_xml: bool) -> dict[str, Any]:
    graph: ET.Element = event["graph"]
    parsed = parse_deadlock_graph(graph)
    rendered: dict[str, Any] = {
        "deadlock_time": event.get("timestamp_utc") or "",
        "source": event["source"],
        **parsed,
    }
    if event.get("session_name"):
        rendered["session_name"] = event["session_name"]
    if event.get("also_seen_in"):
        rendered["also_seen_in"] = event["also_seen_in"]
    if include_graph_xml:
        text = ET.tostring(graph, encoding="unicode")
        rendered["graph_xml"] = text[:MAX_GRAPH_XML_CHARS]
        rendered["graph_xml_truncated"] = len(text) > MAX_GRAPH_XML_CHARS
    return rendered


def parse_deadlock_graph(root: ET.Element) -> dict[str, Any]:
    victim_ids = [
        str(victim.get("id"))
        for victim in root.findall("./victim-list/victimProcess")
        if victim.get("id")
    ]
    participants: list[dict[str, Any]] = []
    for process in root.findall("./process-list/process"):
        procedures = sorted(
            {
                str(frame.get("procname"))
                for frame in process.findall("./executionStack/frame")
                if frame.get("procname") and frame.get("procname") not in {"adhoc", "unknown"}
            }
        )
        participants.append(
            {
                "process_id": process.get("id"),
                "session_id": process.get("spid"),
                "is_victim": process.get("id") in victim_ids,
                "wait_resource": process.get("waitresource", ""),
                "lock_mode": process.get("lockMode", ""),
                "wait_time_ms": _int_or_none(process.get("waittime")),
                "isolation_level": process.get("isolationlevel"),
                "transaction_name": process.get("transactionname"),
                "transaction_count": _int_or_none(process.get("trancount")),
                "log_used_bytes": _int_or_none(process.get("logused")),
                "status": process.get("status"),
                "login_name": process.get("loginname"),
                "host_name": process.get("hostname"),
                "client_app": process.get("clientapp"),
                "procedures": procedures,
                "sql_text": (process.findtext("./inputbuf") or "").strip()[:MAX_SQL_TEXT_CHARS],
            }
        )
    resources: list[dict[str, Any]] = []
    for resource in root.findall("./resource-list/*"):
        resources.append(
            {
                "type": resource.tag,
                "object_name": resource.get("objectname"),
                "index_name": resource.get("indexname"),
                "mode": resource.get("mode"),
                "owners": [
                    {"process_id": owner.get("id"), "mode": owner.get("mode")}
                    for owner in resource.findall("./owner-list/owner")
                ],
                "waiters": [
                    {
                        "process_id": waiter.get("id"),
                        "mode": waiter.get("mode"),
                        "request_type": waiter.get("requestType"),
                    }
                    for waiter in resource.findall("./waiter-list/waiter")
                ],
                "attributes": dict(resource.attrib),
            }
        )
    objects = sorted({str(item["object_name"]) for item in resources if item.get("object_name")})
    victim_sessions = [
        str(item["session_id"]) for item in participants if item["is_victim"] and item["session_id"]
    ]
    return {
        # The spid, which get_active_sessions and application logs know; the
        # process ids stay in victim_process_ids.
        "victim_session_id": victim_sessions[0] if victim_sessions else None,
        "victim_session_ids": victim_sessions,
        "victim_process_ids": victim_ids,
        "participants": participants,
        "resources": resources,
        "objects": objects,
    }


def _overall_status(
    deadlocks: list[dict[str, Any]], sources: list[_SourceResult]
) -> dict[str, Any]:
    lossy = [source for source in sources if source.incomplete]
    loss = " ".join(source.detail for source in lossy)
    if deadlocks:
        if lossy:
            return {
                "incomplete": True,
                **status_payload(
                    ResultStatus.OK,
                    f"Found {len(deadlocks)} deadlock(s). This is an undercount: {loss}",
                    remediation=LOST_EVENTS_REMEDIATION,
                ),
            }
        return status_payload(ResultStatus.OK, f"Found {len(deadlocks)} deadlock(s).")
    read_ok = [source for source in sources if source.status in {ResultStatus.OK, ResultStatus.EMPTY}]
    if lossy and not read_ok:
        return {
            "incomplete": True,
            **status_payload(
                ResultStatus.UNAVAILABLE,
                f"No deadlock could be read, and that is not a negative: {loss}",
                remediation=LOST_EVENTS_REMEDIATION,
            ),
        }
    if read_ok:
        names = ", ".join(source.source for source in read_ok)
        return status_payload(
            ResultStatus.EMPTY,
            f"No deadlocks found in the readable sources ({names}). Ring buffers are "
            "memory-resident and telemetry retention is limited, so this covers only the "
            "retained window.",
        )
    details = " ".join(f"{source.source}: {source.detail}" for source in sources)
    preconditions = [source for source in sources if source.status is ResultStatus.PRECONDITION]
    if preconditions:
        remediation = next(
            (source.remediation for source in preconditions if source.remediation), None
        )
        return status_payload(
            ResultStatus.PRECONDITION,
            "No deadlock source is readable yet. " + details,
            remediation=remediation,
        )
    return status_payload(
        ResultStatus.UNAVAILABLE,
        "Deadlock sources could not be read. " + details,
    )


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
