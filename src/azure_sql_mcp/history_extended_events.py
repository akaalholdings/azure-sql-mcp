"""History from database-scoped Extended Events sessions that already exist.

This source reads what a client's own sessions recorded: blocked process
reports, attention (a client timeout or cancel), completed batches, RPCs and
statements, and deadlock reports. It reads ``ring_buffer`` and ``event_file``
targets. It never creates, starts or changes a session: when no session
records a category, the result is ``precondition`` and carries the capture DDL
as text for a DBA.

Three things keep a quiet result honest:

- A session vouches only for the time it ran. A ring buffer is emptied by a
  failover or a restart, and a full one rolls its oldest events off. The start
  of what a target still holds is the retention floor; a window that begins
  before it is never ``empty``.
- A target that dropped or cut events, or holds a report it cannot parse, is
  never ``empty``.
- Event field names are not in Microsoft's reference. Before parsing, the source
  reads ``sys.dm_xe_object_columns`` and confirms that the engine lists each
  field the parser needs. An event that fails the check is not parsed
  (``unverified_schema``). Time units come from the same catalogue.

The SQL here has not been run against Azure SQL Database.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

from .azure_tier import dmv_permission_hint
from .connection import AzureSqlExecutor
from .history_sources import DEFAULT_LIMIT
from .history_sources import MAX_LIMIT
from .history_sources import CompletedStatement
from .history_sources import Coverage
from .history_sources import HistoryCategory
from .history_sources import HistoryEvent
from .history_sources import HistoryReadError
from .history_sources import SchemaBasis
from .history_sources import SourceDescription
from .history_sources import SourceRead
from .history_sources import TimeoutEvent
from .history_sources import TimeWindow
from .history_sources import clean_text
from .history_sources import deadlock_event
from .history_sources import parse_blocked_process_report
from .history_sources import parse_utc
from .history_sources import parse_xml
from .history_sources import settle_read
from .history_sources import to_float
from .history_sources import to_int
from .history_sources import unverified_schema_gate
from .incident_log import note_exception
from .observability import sanitize_error_message
from .result_status import ResultStatus
from .time_windows import sql_utc

SOURCE = "extended_events"
FAMILY = "extended_events"
# MAX_DISPATCH_LATENCY defaults to 30 seconds: an event can be that late in a target.
DEFAULT_DISPATCH_LATENCY_SECONDS = 30

EVENTS_BY_CATEGORY: Mapping[HistoryCategory, tuple[str, ...]] = {
    HistoryCategory.BLOCKING: ("blocked_process_report",),
    HistoryCategory.TIMEOUT: ("attention",),
    HistoryCategory.LONG_STATEMENT: (
        "sql_batch_completed",
        "rpc_completed",
        "sql_statement_completed",
        "sp_statement_completed",
    ),
    HistoryCategory.DEADLOCK: ("database_xml_deadlock_report",),
}
# The fields the parser cannot work without, per event.
REQUIRED_FIELDS: Mapping[str, frozenset[str]] = {
    "blocked_process_report": frozenset({"blocked_process"}),
    "attention": frozenset(),
    "sql_batch_completed": frozenset({"duration"}),
    "rpc_completed": frozenset({"duration"}),
    "sql_statement_completed": frozenset({"duration"}),
    "sp_statement_completed": frozenset({"duration"}),
    "database_xml_deadlock_report": frozenset({"xml_report"}),
}
_ALL_EVENTS = tuple(name for names in EVENTS_BY_CATEGORY.values() for name in names)
_ACTIONS = "ACTION (sqlserver.session_id, sqlserver.client_app_name, sqlserver.sql_text)"
# Inert text for a DBA. Nothing in this module runs it.
CAPTURE_DDL: Mapping[HistoryCategory, str] = {
    HistoryCategory.BLOCKING: (
        "CREATE EVENT SESSION [azure_sql_mcp_blocking] ON DATABASE\n"
        "ADD EVENT sqlserver.blocked_process_report\n"
        "ADD TARGET package0.ring_buffer (SET max_events_limit = 200, max_memory = 4096)\n"
        "WITH (STARTUP_STATE = ON);\n"
        "ALTER EVENT SESSION [azure_sql_mcp_blocking] ON DATABASE STATE = START;"
    ),
    HistoryCategory.TIMEOUT: (
        "CREATE EVENT SESSION [azure_sql_mcp_timeouts] ON DATABASE\n"
        f"ADD EVENT sqlserver.attention ({_ACTIONS})\n"
        "ADD TARGET package0.ring_buffer (SET max_events_limit = 200, max_memory = 4096)\n"
        "WITH (STARTUP_STATE = ON);\n"
        "ALTER EVENT SESSION [azure_sql_mcp_timeouts] ON DATABASE STATE = START;"
    ),
    HistoryCategory.LONG_STATEMENT: (
        "CREATE EVENT SESSION [azure_sql_mcp_long_statements] ON DATABASE\n"
        f"ADD EVENT sqlserver.sql_batch_completed ({_ACTIONS} WHERE duration >= 5000000),\n"
        f"ADD EVENT sqlserver.rpc_completed ({_ACTIONS} WHERE duration >= 5000000)\n"
        "ADD TARGET package0.ring_buffer (SET max_events_limit = 200, max_memory = 4096)\n"
        "WITH (STARTUP_STATE = ON);\n"
        "ALTER EVENT SESSION [azure_sql_mcp_long_statements] ON DATABASE STATE = START;"
    ),
    HistoryCategory.DEADLOCK: (
        "CREATE EVENT SESSION [azure_sql_mcp_deadlocks] ON DATABASE\n"
        "ADD EVENT sqlserver.database_xml_deadlock_report\n"
        "ADD TARGET package0.ring_buffer (SET max_events_limit = 200, max_memory = 4096)\n"
        "WITH (STARTUP_STATE = ON);\n"
        "ALTER EVENT SESSION [azure_sql_mcp_deadlocks] ON DATABASE STATE = START;"
    ),
}


def _name_list(names: Sequence[str]) -> str:
    # Event names are constants of this module, never caller text.
    if not names or not all(name in REQUIRED_FIELDS for name in names):
        raise ValueError("Unknown Extended Events event name.")
    return ", ".join(f"N'{name}'" for name in names)


CATALOG_SQL = f"""
SELECT c.object_name AS event_name, c.name AS field_name, c.description
FROM sys.dm_xe_object_columns AS c
WHERE c.column_type = N'data'
  AND c.object_name IN ({_name_list(_ALL_EVENTS)})
"""


def running_targets_sql(names: Sequence[str]) -> str:
    """Running sessions that record one of the events, with their targets."""

    return f"""
SELECT
    s.name AS session_name,
    s.create_time AS session_start_utc,
    t.target_name,
    CAST(t.target_data AS nvarchar(max)) AS target_data
FROM sys.dm_xe_database_sessions AS s
INNER JOIN sys.dm_xe_database_session_targets AS t
    ON t.event_session_address = s.address
WHERE EXISTS (
    SELECT 1
    FROM sys.dm_xe_database_session_events AS e
    WHERE e.event_session_address = s.address
      AND e.event_name IN ({_name_list(names)})
)
"""


def defined_sessions_sql(names: Sequence[str]) -> str:
    return f"""
SELECT s.name AS session_name
FROM sys.database_event_sessions AS s
WHERE EXISTS (
    SELECT 1
    FROM sys.database_event_session_events AS e
    WHERE e.event_session_id = s.event_session_id
      AND e.name IN ({_name_list(names)})
)
"""


def file_targets_sql(names: Sequence[str]) -> str:
    """The configured blob name of each event_file target that records the events."""

    return f"""
SELECT s.name AS session_name, CAST(f.value AS nvarchar(4000)) AS file_name
FROM sys.database_event_sessions AS s
INNER JOIN sys.database_event_session_targets AS t
    ON t.event_session_id = s.event_session_id
INNER JOIN sys.database_event_session_fields AS f
    ON f.event_session_id = t.event_session_id
   AND f.object_id = t.target_id
WHERE t.name = N'event_file'
  AND f.name = N'filename'
  AND EXISTS (
    SELECT 1
    FROM sys.database_event_session_events AS e
    WHERE e.event_session_id = s.event_session_id
      AND e.name IN ({_name_list(names)})
)
"""


def file_events_sql(names: Sequence[str]) -> str:
    """Events of the window from an event_file target, oldest first.

    Parameters: row limit, blob path prefix, window start, window end.
    """

    return f"""
SELECT TOP (?) f.object_name AS event_name, f.event_data, f.timestamp_utc
FROM sys.fn_xe_file_target_read_file(?, NULL, NULL, NULL) AS f
WHERE f.object_name IN ({_name_list(names)})
  AND f.timestamp_utc >= CAST(? AS datetime2)
  AND f.timestamp_utc < CAST(? AS datetime2)
ORDER BY f.timestamp_utc
"""


# The oldest event the files still hold. Parameter: blob path prefix.
FILE_BOUNDS_SQL = """
SELECT MIN(f.timestamp_utc) AS oldest_utc
FROM sys.fn_xe_file_target_read_file(?, NULL, NULL, NULL) AS f
"""


@dataclass(frozen=True)
class EventCatalog:
    """What ``sys.dm_xe_object_columns`` lists for the events this module parses."""

    fields: Mapping[str, frozenset[str]]
    divisors: Mapping[tuple[str, str], float]

    def confirmed(self, event: str) -> bool:
        listed = self.fields.get(event)
        return listed is not None and REQUIRED_FIELDS[event] <= listed

    def to_ms(self, event: str, name: str) -> float | None:
        """The number to divide a time field by to get milliseconds; None when unknown."""

        return self.divisors.get((event, name))


def catalog_from_rows(rows: Sequence[Mapping[str, Any]]) -> EventCatalog:
    listed: dict[str, set[str]] = {}
    divisors: dict[tuple[str, str], float] = {}
    for row in rows:
        event = str(row.get("event_name") or "")
        name = str(row.get("field_name") or "")
        if not event or not name:
            continue
        listed.setdefault(event, set()).add(name)
        description = str(row.get("description") or "").casefold()
        if "microsecond" in description:
            divisors[(event, name)] = 1000.0
        elif "millisecond" in description:
            divisors[(event, name)] = 1.0
    return EventCatalog(
        {event: frozenset(names) for event, names in listed.items()}, divisors
    )


@dataclass
class TargetRead:
    """One ring_buffer document: its events of interest and what it lost."""

    events: list[ET.Element] = field(default_factory=list)
    # The oldest event of any kind that the buffer still holds.
    oldest: datetime | None = None
    # More events passed through the buffer than it holds now.
    rolled: bool = False
    loss: list[str] = field(default_factory=list)


def read_ring_buffer_target(target_data: object, names: set[str] | frozenset[str]) -> TargetRead:
    root = parse_xml(target_data)
    if root is None:
        return TargetRead(loss=["a ring_buffer document could not be parsed"])
    read = TargetRead()
    for event in root.iter("event"):
        time = parse_utc(event.get("timestamp"))
        if time is not None and (read.oldest is None or time < read.oldest):
            read.oldest = time
        if event.get("name") in names:
            read.events.append(event)
    if str(root.get("truncated") or "0") not in {"0", ""}:
        read.loss.append("a ring_buffer document was cut when it was rendered")
    dropped = to_int(root.get("droppedCount")) or 0
    if dropped:
        read.loss.append(f"a ring_buffer target dropped {dropped} event(s)")
    processed = to_int(root.get("totalEventsProcessed"))
    held = to_int(root.get("eventCount"))
    read.rolled = processed is not None and held is not None and processed > held
    return read


def _data(event: ET.Element, name: str) -> str | None:
    node = event.find(f"./data[@name='{name}']")
    if node is None:
        return None
    # A mapped value (lock_mode) has its name in <text>.
    return node.findtext("text") or node.findtext("value")


def _action(event: ET.Element, name: str) -> str | None:
    return event.findtext(f"./action[@name='{name}']/value")


def _milliseconds(
    event: ET.Element, name: str, catalog: EventCatalog, unknown_units: set[str]
) -> float | None:
    raw = to_float(_data(event, name))
    if raw is None:
        return None
    divisor = catalog.to_ms(str(event.get("name")), name)
    if divisor is None:
        unknown_units.add(f"{event.get('name')}.{name}")
        return None
    return raw / divisor


def parse_xe_event(
    event: ET.Element,
    catalog: EventCatalog,
    *,
    database: str,
    source: str,
    fallback_time: datetime | None = None,
    unknown_units: set[str] | None = None,
) -> HistoryEvent | None:
    """One target event as a normalized event, or None when it cannot be read."""

    units = unknown_units if unknown_units is not None else set()
    name = str(event.get("name") or "")
    time = parse_utc(event.get("timestamp")) or fallback_time
    if time is None:
        return None
    if name == "blocked_process_report":
        report = event.find("./data[@name='blocked_process']/value/blocked-process-report")
        if report is None:
            return None
        return parse_blocked_process_report(
            report,
            time=time,
            database=database,
            source=source,
            duration_ms=_milliseconds(event, "duration", catalog, units),
            lock_mode=clean_text(_data(event, "lock_mode")),
            resource_owner_type=clean_text(_data(event, "resource_owner_type")),
        )
    if name == "database_xml_deadlock_report":
        graph = event.find("./data[@name='xml_report']/value/deadlock")
        if graph is None:
            return None
        return deadlock_event(graph, time=time, database=database, source=source)
    session_id = to_int(_action(event, "session_id"))
    client_app = clean_text(_action(event, "client_app_name"))
    host_name = clean_text(_action(event, "client_hostname"))
    login_name = clean_text(_action(event, "username")) or clean_text(
        _action(event, "server_principal_name")
    )
    if name == "attention":
        return TimeoutEvent(
            time=time,
            database=database,
            source=source,
            kind="attention",
            session_id=session_id,
            duration_ms=_milliseconds(event, "duration", catalog, units),
            client_app=client_app,
            host_name=host_name,
            login_name=login_name,
            statement_text=clean_text(_action(event, "sql_text")),
        )
    if name not in REQUIRED_FIELDS or _data(event, "duration") is None:
        return None
    return CompletedStatement(
        time=time,
        database=database,
        source=source,
        kind=name,
        duration_ms=_milliseconds(event, "duration", catalog, units),
        cpu_ms=_milliseconds(event, "cpu_time", catalog, units),
        logical_reads=to_int(_data(event, "logical_reads")),
        physical_reads=to_int(_data(event, "physical_reads")),
        writes=to_int(_data(event, "writes")),
        row_count=to_int(_data(event, "row_count")),
        session_id=session_id,
        client_app=client_app,
        host_name=host_name,
        login_name=login_name,
        object_name=clean_text(_data(event, "object_name")),
        statement_text=(
            clean_text(_data(event, "batch_text"))
            or clean_text(_data(event, "statement"))
            or clean_text(_action(event, "sql_text"))
        ),
    )


class ExtendedEventsHistorySource:
    """Events recorded by the database's own Extended Events sessions."""

    name = SOURCE
    family = FAMILY

    def __init__(
        self,
        database_name: str,
        executor: AzureSqlExecutor,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        dispatch_latency_seconds: int = DEFAULT_DISPATCH_LATENCY_SECONDS,
    ) -> None:
        self.database_name = database_name
        self._executor = executor
        self._now = now
        self._latency = timedelta(seconds=dispatch_latency_seconds)
        self._catalog: EventCatalog | None = None

    def gate(self, category: HistoryCategory, window: TimeWindow) -> Coverage | None:
        if category not in EVENTS_BY_CATEGORY:
            return Coverage.gated(
                self.name, self.family, category, window, ResultStatus.NOT_SUPPORTED,
                f"No Extended Events event read here records {category.value}.",
            )
        return None

    async def describe(self) -> SourceDescription:
        recorded: list[str] = []
        reason: str | None = None
        try:
            for category, names in EVENTS_BY_CATEGORY.items():
                if await self._executor.fetch_all(self.database_name, running_targets_sql(names)):
                    recorded.append(category.value)
        except Exception as exc:  # permission or transient failure
            note_exception(exc, "history_extended_events.describe")
            reason = self._sql_error(exc, "sys.dm_xe_database_sessions").args[0]
        return SourceDescription(
            self.name,
            self.family,
            self.database_name,
            True,
            tuple(recorded),
            expected_ingest_lag_seconds=self._latency.total_seconds(),
            # A target is read live: it holds everything up to the dispatch latency.
            watermark=self._now() if reason is None else None,
            watermark_reason=reason,
            notes=(
                "Retention is what each target still holds: a ring_buffer is emptied by a "
                "failover or a restart and rolls off its oldest events.",
            ),
        )

    async def read(
        self,
        category: HistoryCategory,
        window: TimeWindow,
        filters: Mapping[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> SourceRead:
        gate = self.gate(category, window)
        if gate is not None:
            return SourceRead([], gate)
        limit = max(1, min(int(limit), MAX_LIMIT))
        minimum = _min_duration(filters)
        now = self._now()
        try:
            return await self._read(category, window, limit, minimum, now)
        except HistoryReadError as exc:
            return settle_read(
                source=self.name,
                family=self.family,
                category=category,
                window=window,
                now=now,
                read_error=exc,
                schema_basis=SchemaBasis.ENGINE_CATALOG,
            )

    async def _read(
        self,
        category: HistoryCategory,
        window: TimeWindow,
        limit: int,
        minimum: float | None,
        now: datetime,
    ) -> SourceRead:
        catalog = await self._event_catalog()
        wanted = EVENTS_BY_CATEGORY[category]
        names = tuple(name for name in wanted if catalog.confirmed(name))
        unconfirmed = sorted(set(wanted) - set(names))
        if not names:
            return SourceRead(
                [],
                unverified_schema_gate(
                    self.name, self.family, category, window,
                    "sys.dm_xe_object_columns does not list the fields this source reads "
                    f"for {', '.join(unconfirmed)}.",
                ),
            )
        targets = await self._fetch(running_targets_sql(names), "sys.dm_xe_database_sessions")
        if not targets:
            return SourceRead([], await self._missing_session(category, window, names))
        readable = [
            row for row in targets if row.get("target_name") in {"ring_buffer", "event_file"}
        ]
        if not readable:
            return SourceRead(
                [],
                Coverage.gated(
                    self.name, self.family, category, window, ResultStatus.PRECONDITION,
                    "The running sessions that record this write only to targets this "
                    "source does not read. It reads ring_buffer or event_file targets.",
                    remediation=CAPTURE_DDL[category],
                ),
            )

        elements: list[tuple[ET.Element, datetime | None]] = []
        floors: list[datetime] = []
        never_empty: list[str] = []
        truncated: str | None = None
        for row in readable:
            if row.get("target_name") != "ring_buffer":
                continue
            session = str(row.get("session_name") or "")
            started = parse_utc(row.get("session_start_utc"))
            ring = read_ring_buffer_target(row.get("target_data"), set(names))
            elements.extend((element, None) for element in ring.events)
            never_empty.extend(f"Session {session}: {loss}." for loss in ring.loss)
            floor = ring.oldest if ring.rolled and ring.oldest is not None else started
            if floor is None:
                never_empty.append(f"Session {session}: its start time is not known.")
            else:
                floors.append(floor)
        file_sessions = {
            str(row.get("session_name") or ""): parse_utc(row.get("session_start_utc"))
            for row in readable
            if row.get("target_name") == "event_file"
        }
        if file_sessions:
            file_rows, file_floors, file_notes, cut = await self._read_files(
                names, file_sessions, window, limit
            )
            elements.extend(file_rows)
            floors.extend(file_floors)
            never_empty.extend(file_notes)
            truncated = "max_rows" if cut else None

        events: list[HistoryEvent] = []
        unparsed = 0
        unknown_units: set[str] = set()
        for element, fallback_time in elements:
            event = parse_xe_event(
                element,
                catalog,
                database=self.database_name,
                source=self.name,
                fallback_time=fallback_time,
                unknown_units=unknown_units,
            )
            if event is None:
                unparsed += 1
            elif minimum is None or (getattr(event, "duration_ms", None) or 0) >= minimum:
                events.append(event)
        warnings: list[str] = []
        if unconfirmed:
            warnings.append(
                "Not read (unverified_schema): " + ", ".join(unconfirmed) + "."
            )
        if unknown_units:
            warnings.append(
                "The engine's catalogue does not state the unit of "
                + ", ".join(sorted(unknown_units))
                + ", so those values are left out."
            )
        if category is HistoryCategory.LONG_STATEMENT:
            warnings.append(
                "A session records only what its predicate lets through: no record means "
                "nothing the session was set to keep."
            )
        if category is HistoryCategory.BLOCKING:
            warnings.append(
                "A blocked process report exists only for a wait longer than the blocked "
                "process threshold."
            )
        return settle_read(
            source=self.name,
            family=self.family,
            category=category,
            window=window,
            now=now,
            events=events,
            rows_read=len(elements),
            rows_unparsed=unparsed,
            watermark=now,
            ingest_lag=self._latency,
            retention_floor=min(floors) if floors else None,
            truncated_reason=truncated,
            read_through=max((event.time for event in events), default=None)
            if truncated else None,
            never_empty_reasons=never_empty,
            schema_basis=SchemaBasis.ENGINE_CATALOG,
            warnings=warnings,
        )

    async def _read_files(
        self,
        names: Sequence[str],
        sessions: Mapping[str, datetime | None],
        window: TimeWindow,
        limit: int,
    ) -> tuple[list[tuple[ET.Element, datetime | None]], list[datetime], list[str], bool]:
        files = await self._fetch(file_targets_sql(names), "sys.database_event_session_fields")
        elements: list[tuple[ET.Element, datetime | None]] = []
        floors: list[datetime] = []
        notes: list[str] = []
        cut = False
        paths = {
            str(row.get("session_name") or ""): _blob_prefix(row.get("file_name"))
            for row in files
            if str(row.get("session_name") or "") in sessions
        }
        for session, started in sessions.items():
            path = paths.get(session)
            if path is None:
                notes.append(f"Session {session}: the event_file name could not be read.")
                continue
            rows = await self._fetch(
                file_events_sql(names),
                "sys.fn_xe_file_target_read_file",
                [limit + 1, path, sql_utc(window.start), sql_utc(window.end)],
            )
            if len(rows) > limit:
                rows, cut = rows[:limit], True
            for row in rows:
                element = parse_xml(row.get("event_data"))
                if element is None:
                    notes.append(f"Session {session}: an event in the file could not be parsed.")
                    continue
                elements.append((element, parse_utc(row.get("timestamp_utc"))))
            if rows:
                continue
            # Nothing in the window: how far back do the files reach?
            bounds = await self._fetch(
                FILE_BOUNDS_SQL, "sys.fn_xe_file_target_read_file", [path]
            )
            oldest = parse_utc(bounds[0].get("oldest_utc")) if bounds else None
            floor = oldest or started
            if floor is None:
                notes.append(f"Session {session}: how far back its files reach is not known.")
            else:
                floors.append(floor)
        return elements, floors, notes, cut

    async def _missing_session(
        self, category: HistoryCategory, window: TimeWindow, names: Sequence[str]
    ) -> Coverage:
        try:
            defined = await self._executor.fetch_all(
                self.database_name, defined_sessions_sql(names)
            )
        except Exception:
            defined = []
        stopped = sorted({str(row.get("session_name") or "") for row in defined} - {""})
        if stopped:
            return Coverage.gated(
                self.name, self.family, category, window, ResultStatus.PRECONDITION,
                f"A session that records {category.value} is defined but not running.",
                remediation="\n".join(
                    f"ALTER EVENT SESSION [{name.replace(']', ']]')}] ON DATABASE STATE = START;"
                    for name in stopped
                ),
            )
        return Coverage.gated(
            self.name, self.family, category, window, ResultStatus.PRECONDITION,
            f"No database-scoped Extended Events session records {', '.join(names)} in "
            "this database.",
            remediation=CAPTURE_DDL[category],
        )

    async def _event_catalog(self) -> EventCatalog:
        if self._catalog is None:
            rows = await self._fetch(CATALOG_SQL, "sys.dm_xe_object_columns")
            self._catalog = catalog_from_rows(rows)
        return self._catalog

    async def _fetch(
        self, query: str, dmv: str, params: Sequence[Any] | None = None
    ) -> list[dict[str, Any]]:
        try:
            return await self._executor.fetch_all(self.database_name, query, params=params)
        except Exception as exc:  # permission or transient failure
            note_exception(exc, "history_extended_events.read")
            raise self._sql_error(exc, dmv) from None

    def _sql_error(self, exc: Exception, dmv: str) -> HistoryReadError:
        return HistoryReadError(
            "sql",
            f"{dmv} could not be read: {sanitize_error_message(str(exc))}. "
            + dmv_permission_hint(dmv),
        )


def _blob_prefix(file_name: object) -> str | None:
    """The path prefix that matches every rollover blob of an event_file target.

    The engine adds a numeric suffix before ``.xel`` to each blob name.
    """

    text = clean_text(file_name, limit=260)
    if text is None:
        return None
    return text[:-4] if text.casefold().endswith(".xel") else text


def _min_duration(filters: Mapping[str, Any] | None) -> float | None:
    value = (filters or {}).get("min_duration_ms")
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError("min_duration_ms must be a number that is not negative.")
    return float(value)
