"""One interface over the recorders a client already has.

The live DMV tools cannot see the past: a blocking episode, a timeout or "what
ran at 02:10" is gone before a tool runs. This module does not record anything.
It reads the recorders that exist (Azure Monitor, database watcher, Extended
Events sessions, Loki) through one ``HistorySource`` interface, turns their
rows into one set of event shapes, and says plainly when no recorder covers the
question.

The rule that matters most: ``empty`` is a true negative. It is returned only
when a source is declared to record the category for the database and the whole
window is ingested. A window that ends after the newest record seen, minus the
source's ingest lag, is ``unavailable`` with the reason "not ingested yet". A
false ``empty`` during an incident is the worst result this module can give.

Statement text and error text pass through the server's data-detail level
before they leave: ``HistoryResult.to_payload``. Nothing here writes a log line,
and read errors are value-free: a kind and a status code, never a token, a URL,
a query or a response body.
"""

from __future__ import annotations

import asyncio
import json
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from enum import Enum
from typing import TYPE_CHECKING
from typing import Any
from typing import Protocol

from azure.identity import ClientSecretCredential
from azure.identity import DefaultAzureCredential
from azure.identity import InteractiveBrowserCredential

from .config import AuthMode
from .config import ServerConfig
from .deadlocks import parse_deadlock_graph
from .observability import apply_data_detail
from .observability import normalize_data_detail
from .observability import sanitize_error_message
from .result_status import REMEDIATION_KEY
from .result_status import ResultStatus
from .result_status import status_payload
from .time_windows import iso_z

if TYPE_CHECKING:
    from azure.core.credentials import AccessToken
    from azure.core.credentials import TokenCredential

DEFAULT_LIMIT = 200
MAX_LIMIT = 1000
MAX_RESPONSE_BYTES = 32 * 1024 * 1024
MAX_XML_CHARS = 2 * 1024 * 1024
MAX_STATEMENT_CHARS = 4000
# Reports of one chain arrive once per blocked process threshold. Two reports
# further apart than this are two episodes.
EPISODE_GAP_SECONDS = 90
_SNAPSHOT_SECONDS = 5
_TOKEN_REFRESH_MARGIN_SECONDS = 300
_ERROR_BODY_BYTES = 4096


class HistoryCategory(str, Enum):
    BLOCKING = "blocking"
    TIMEOUT = "timeout"
    DEADLOCK = "deadlock"
    ERROR = "error"
    WAIT_STATS = "wait_stats"
    SESSION_SNAPSHOT = "session_snapshot"
    RESOURCE_METRIC = "resource_metric"
    LONG_STATEMENT = "long_statement"
    AUDIT_STATEMENT = "audit_statement"


class SchemaBasis(str, Enum):
    """Where a parser's table and field names come from."""

    DOCUMENTED = "documented"  # published by Microsoft or the service's API reference
    DERIVED = "derived"  # a documented name changed by a documented rule
    ENGINE_CATALOG = "engine_catalog"  # confirmed against the engine at read time
    UNVERIFIED = "unverified_schema"  # not confirmed: the adapter does not parse it


# Events of these categories carry an identity that is the same in every
# recorder, so reads from different recorders merge. For the other categories a
# record in one recorder cannot be matched to a record in another: the resolver
# returns the events of one source and does not add the others to it.
IDENTITY_MERGE_CATEGORIES = frozenset({HistoryCategory.BLOCKING, HistoryCategory.DEADLOCK})

# Live tools for the minutes a recorder has not ingested yet.
LIVE_TOOLS: Mapping[HistoryCategory, str] = {
    HistoryCategory.BLOCKING: "get_active_sessions, get_lock_details, get_open_transactions",
    HistoryCategory.TIMEOUT: "get_active_sessions",
    HistoryCategory.DEADLOCK: "get_deadlock_history",
    HistoryCategory.WAIT_STATS: "get_wait_stats with sample_seconds, get_currently_waiting_tasks",
    HistoryCategory.SESSION_SNAPSHOT: "get_active_sessions",
    HistoryCategory.RESOURCE_METRIC: "get_resource_stats_history",
    HistoryCategory.LONG_STATEMENT: "get_top_queries, get_active_sessions",
}


@dataclass(frozen=True)
class TimeWindow:
    """A half-open UTC window: ``start`` is inside, ``end`` is not."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("A history window needs timezone-aware instants.")
        if self.start >= self.end:
            raise ValueError("A history window must start before it ends.")

    def contains(self, instant: datetime) -> bool:
        return self.start <= instant < self.end


@dataclass(frozen=True)
class DatabaseIdentity:
    """How one database is named inside a recorder: short server name and database."""

    logical_server: str
    database: str

    def __post_init__(self) -> None:
        if not isinstance(self.logical_server, str) or not _LOGICAL_SERVER.fullmatch(
            self.logical_server
        ):
            raise ValueError(
                "logical_server must be the short server name (1 to 63 letters, digits or "
                "hyphens, no hyphen at either end), not the host name."
            )
        if not _is_database_name(self.database):
            raise ValueError(
                "database must be 1 to 128 characters without control characters, quotes "
                "or <>*%&:\\/?, and must not start with a space or end with a space or period."
            )


_LOGICAL_SERVER = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")
_DATABASE_NAME_RESERVED = frozenset("<>*%&:\\/?\"'")


def _is_database_name(value: object) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= 128
        and value[0] != " "
        and value[-1] not in {" ", "."}
        and not any(
            character in _DATABASE_NAME_RESERVED or not character.isprintable()
            for character in value
        )
    )


class HistoryReadError(Exception):
    """A value-free read failure: a kind and a status code, never a URL, body or query."""

    def __init__(
        self,
        kind: str,
        message: str,
        *,
        status_code: int | None = None,
        transient: bool = False,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.status_code = status_code
        self.transient = transient


# --- Normalized events ----------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class HistoryEvent:
    """One recorded fact. ``statement_text`` fields are masked by ``to_payload``."""

    time: datetime
    database: str
    source: str
    # Other recorders that hold the same event.
    seen_in: tuple[str, ...] = ()

    def dedupe_key(self) -> tuple[Any, ...]:
        raise NotImplementedError

    def to_dict(self) -> dict[str, Any]:
        payload = {item.name: _jsonable(getattr(self, item.name)) for item in fields(self)}
        if not payload["seen_in"]:
            del payload["seen_in"]
        return payload


@dataclass(frozen=True, kw_only=True)
class SessionRef:
    """A session as a blocked process report or a snapshot describes it."""

    session_id: int | None = None
    status: str | None = None
    transaction_count: int | None = None
    transaction_id: str | None = None
    last_batch_started: str | None = None
    last_batch_completed: str | None = None
    client_app: str | None = None
    host_name: str | None = None
    login_name: str | None = None
    isolation_level: str | None = None
    statement_text: str | None = None
    # Statement references: sql_handle with the statement offsets inside it.
    frames: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True, kw_only=True)
class BlockedProcessReport(HistoryEvent):
    """One blocked process report: one waiter and the session directly ahead of it."""

    blocked: SessionRef
    blocking: SessionRef
    monitor_loop: int | None = None
    wait_resource: str | None = None
    wait_time_ms: int | None = None
    lock_mode: str | None = None
    resource_owner_type: str | None = None
    duration_ms: float | None = None

    def dedupe_key(self) -> tuple[Any, ...]:
        # No time: recorders stamp one report a few seconds apart. The wait time
        # inside the report grows with every report, so it tells reports apart.
        return (
            "blocked_process_report",
            self.monitor_loop,
            self.blocked.session_id,
            self.blocking.session_id,
            self.wait_time_ms,
            self.wait_resource,
        )


@dataclass(frozen=True, kw_only=True)
class BlockingVictim:
    session_id: int | None
    blocked_by_session_id: int | None
    max_wait_ms: int | None = None
    lock_mode: str | None = None
    wait_resource: str | None = None
    client_app: str | None = None
    login_name: str | None = None
    statement_text: str | None = None
    frames: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True, kw_only=True)
class BlockingEpisode(HistoryEvent):
    """One blocking chain over consecutive reports, named by its head blocker."""

    first_seen: datetime
    last_seen: datetime
    head_blocker: SessionRef
    victims: tuple[BlockingVictim, ...]
    # The longest any victim waited: how long the chain held work up.
    max_wait_ms: int | None
    max_chain_depth: int
    report_count: int
    wait_resources: tuple[str, ...]
    lock_modes: tuple[str, ...]
    sources: tuple[str, ...]
    # Sleeping with an open transaction: the client left a transaction open.
    # The fix is in that code path, not in the engine.
    head_blocker_idle_in_transaction: bool = False

    def dedupe_key(self) -> tuple[Any, ...]:
        return ("blocking_episode", self.first_seen, self.head_blocker.session_id)


@dataclass(frozen=True, kw_only=True)
class TimeoutEvent(HistoryEvent):
    """A client timeout or cancel. ``kind`` is ``timeout`` (diagnostic log) or ``attention``."""

    kind: str = "timeout"
    query_hash: str | None = None
    query_plan_hash: str | None = None
    error_state: int | None = None
    session_id: int | None = None
    duration_ms: float | None = None
    client_app: str | None = None
    host_name: str | None = None
    login_name: str | None = None
    statement_text: str | None = None

    def dedupe_key(self) -> tuple[Any, ...]:
        return (
            self.kind,
            self.time,
            self.query_hash,
            self.query_plan_hash,
            self.error_state,
            self.session_id,
        )


@dataclass(frozen=True, kw_only=True)
class DeadlockEvent(HistoryEvent):
    """One deadlock graph, parsed by ``deadlocks.parse_deadlock_graph``."""

    detail: Mapping[str, Any]
    # Process ids with each process's last batch start: the same in every recorder.
    fingerprint: tuple[str, ...] = ()

    def dedupe_key(self) -> tuple[Any, ...]:
        if self.fingerprint:
            return ("deadlock", self.fingerprint)
        return ("deadlock", self.time.replace(microsecond=0))

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        del payload["fingerprint"]
        payload.update(payload.pop("detail"))
        return payload


@dataclass(frozen=True, kw_only=True)
class ErrorEvent(HistoryEvent):
    error_number: int | None = None
    severity: int | None = None
    state: int | None = None
    user_defined: bool | None = None
    message: str | None = None
    query_hash: str | None = None
    query_plan_hash: str | None = None

    def dedupe_key(self) -> tuple[Any, ...]:
        return (
            "error",
            self.time,
            self.error_number,
            self.state,
            self.query_hash,
            self.message,
        )


@dataclass(frozen=True, kw_only=True)
class WaitStatsDelta(HistoryEvent):
    """Wait time added in one interval for one wait type. ``time`` is the interval end."""

    wait_type: str
    interval_start: datetime | None = None
    interval_end: datetime | None = None
    wait_time_ms: float | None = None
    signal_wait_time_ms: float | None = None
    waiting_tasks_count: float | None = None
    max_wait_time_ms: float | None = None

    def dedupe_key(self) -> tuple[Any, ...]:
        return ("wait_stats", self.wait_type, self.interval_start, self.interval_end, self.time)


@dataclass(frozen=True, kw_only=True)
class SessionSnapshot(HistoryEvent):
    """One session as a recorder saw it at ``time``."""

    session: SessionRef
    blocking_session_id: int | None = None
    wait_type: str | None = None
    wait_time_ms: float | None = None

    def dedupe_key(self) -> tuple[Any, ...]:
        return ("session_snapshot", self.time, self.session.session_id)


@dataclass(frozen=True, kw_only=True)
class ResourceMetricSample(HistoryEvent):
    metric: str
    grain: str | None = None
    unit: str | None = None
    average: float | None = None
    minimum: float | None = None
    maximum: float | None = None
    total: float | None = None
    count: float | None = None

    def dedupe_key(self) -> tuple[Any, ...]:
        return ("resource_metric", self.metric.casefold(), self.time, self.grain)


@dataclass(frozen=True, kw_only=True)
class CompletedStatement(HistoryEvent):
    """A statement, batch or RPC that an event session recorded as completed."""

    kind: str
    duration_ms: float | None = None
    cpu_ms: float | None = None
    logical_reads: int | None = None
    physical_reads: int | None = None
    writes: int | None = None
    row_count: int | None = None
    session_id: int | None = None
    client_app: str | None = None
    host_name: str | None = None
    login_name: str | None = None
    object_name: str | None = None
    statement_text: str | None = None

    def dedupe_key(self) -> tuple[Any, ...]:
        return (
            "completed",
            self.kind,
            self.time,
            self.session_id,
            self.duration_ms,
            self.statement_text,
        )


@dataclass(frozen=True, kw_only=True)
class AuditRecord(HistoryEvent):
    """One audited statement: who ran what, and whether it succeeded."""

    action_name: str | None = None
    succeeded: bool | None = None
    server_principal_name: str | None = None
    database_principal_name: str | None = None
    application_name: str | None = None
    session_id: int | None = None
    duration_ms: float | None = None
    affected_rows: int | None = None
    response_rows: int | None = None
    schema_name: str | None = None
    object_name: str | None = None
    statement_text: str | None = None

    def dedupe_key(self) -> tuple[Any, ...]:
        return (
            "audit",
            self.time,
            self.session_id,
            self.action_name,
            self.server_principal_name,
            self.statement_text,
        )


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return iso_utc(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (SessionRef, BlockingVictim)):
        return {item.name: _jsonable(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, Mapping):
        return {key: _jsonable(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def iso_utc(value: datetime) -> str:
    """ISO text in UTC; fractions are kept only when the instant has them."""

    utc = value.astimezone(timezone.utc)
    if utc.microsecond:
        return utc.replace(tzinfo=None).isoformat(timespec="milliseconds") + "Z"
    return iso_z(utc)


# --- Coverage and status rules --------------------------------------------------


@dataclass
class Coverage:
    """What one source could and could not see for one question."""

    source: str
    family: str
    category: HistoryCategory
    window: TimeWindow
    status: ResultStatus
    reason: str
    remediation: str | None = None
    watermark: datetime | None = None
    ingest_lag_seconds: float = 0.0
    retention_floor: datetime | None = None
    rows_read: int = 0
    rows_unparsed: int = 0
    rows_rejected_identity: int = 0
    duplicates_dropped: int = 0
    rows_outside_window: int = 0
    truncated_reason: str | None = None
    read_through: datetime | None = None
    schema_basis: str | None = None
    warnings: list[str] = field(default_factory=list)
    error: HistoryReadError | None = None
    # Set by the resolver.
    merged: bool | None = None
    not_merged_reason: str | None = None

    @classmethod
    def gated(
        cls,
        source: str,
        family: str,
        category: HistoryCategory,
        window: TimeWindow,
        status: ResultStatus,
        reason: str,
        *,
        remediation: str | None = None,
        schema_basis: str | None = None,
    ) -> "Coverage":
        """Coverage for a source that is not read: not configured, not supported, unverified."""

        return cls(
            source, family, category, window, status, reason,
            remediation=remediation, schema_basis=schema_basis,
        )

    @property
    def settled_through(self) -> datetime | None:
        """Records up to here are in the store: the watermark minus the ingest lag."""

        if self.watermark is None:
            return None
        return self.watermark - timedelta(seconds=self.ingest_lag_seconds)

    @property
    def not_ingested_from(self) -> datetime | None:
        settled = self.settled_through
        if self.status in {ResultStatus.PRECONDITION, ResultStatus.NOT_SUPPORTED}:
            return None
        if settled is None:
            return self.window.start
        return max(settled, self.window.start) if self.window.end > settled else None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "source": self.source,
            "family": self.family,
            "status": self.status.value,
            "reason": self.reason,
            "watermark_utc": _iso_or_none(self.watermark),
            "ingest_lag_seconds": self.ingest_lag_seconds,
            "settled_through_utc": _iso_or_none(self.settled_through),
            "not_ingested_from_utc": _iso_or_none(self.not_ingested_from),
            "rows_read": self.rows_read,
            "rows_unparsed": self.rows_unparsed,
            "rows_rejected_identity": self.rows_rejected_identity,
            "duplicates_dropped": self.duplicates_dropped,
            "rows_outside_window": self.rows_outside_window,
            "truncated": self.truncated_reason is not None,
        }
        optional = {
            REMEDIATION_KEY: self.remediation,
            "retention_floor_utc": _iso_or_none(self.retention_floor),
            "truncated_reason": self.truncated_reason,
            "read_through_utc": _iso_or_none(self.read_through),
            "schema_basis": self.schema_basis,
            "merged": self.merged,
            "not_merged_reason": self.not_merged_reason,
        }
        payload.update({key: value for key, value in optional.items() if value is not None})
        if self.warnings:
            payload["warnings"] = list(self.warnings)
        if self.error is not None:
            payload["read_error"] = {
                "kind": self.error.kind,
                "status_code": self.error.status_code,
                "transient": self.error.transient,
            }
        return payload


@dataclass(frozen=True)
class ReadFacts:
    """What a read found; ``decide_status`` turns it into a status."""

    category: HistoryCategory
    window: TimeWindow
    now: datetime
    event_count: int
    rows_read: int = 0
    # Rows that parsed and belong to the database. None means all of them.
    rows_accepted: int | None = None
    watermark: datetime | None = None
    ingest_lag: timedelta = timedelta(0)
    retention_floor: datetime | None = None
    truncated_reason: str | None = None
    never_empty_reasons: tuple[str, ...] = ()
    read_error: HistoryReadError | None = None
    watermark_error: HistoryReadError | None = None


def decide_status(facts: ReadFacts) -> tuple[ResultStatus, str]:
    """The status of one source's read. The first matching rule wins."""

    name = facts.category.value
    if facts.read_error is not None:
        return ResultStatus.UNAVAILABLE, f"The source could not be read. {facts.read_error}"
    accepted = facts.rows_read if facts.rows_accepted is None else facts.rows_accepted
    if facts.rows_read and not accepted:
        return (
            ResultStatus.UNAVAILABLE,
            f"The source returned {facts.rows_read} row(s), but none parsed as a {name} "
            "record for this database. The format or the identity fields may have "
            "changed. This is not 'no events'.",
        )
    settled = None if facts.watermark is None else facts.watermark - facts.ingest_lag
    if facts.event_count:
        reason = f"Read {facts.event_count} {name} record(s)."
        if settled is None or facts.window.end > settled:
            start = facts.window.start if settled is None else max(settled, facts.window.start)
            reason += f" Time from {iso_z(start)} on is not ingested yet."
        if facts.retention_floor is not None and facts.window.start < facts.retention_floor:
            reason += f" Time before {iso_z(facts.retention_floor)} is past the source's retention."
        if facts.truncated_reason is not None:
            reason += f" The read stopped early ({facts.truncated_reason}); more records exist."
        return ResultStatus.OK, reason
    if facts.watermark is None or settled is None:
        if facts.watermark_error is not None:
            return (
                ResultStatus.UNAVAILABLE,
                f"The newest record could not be read ({facts.watermark_error}), so the "
                "window cannot be confirmed as ingested. This is not 'no events'.",
            )
        return (
            ResultStatus.UNAVAILABLE,
            f"The source holds no record for this database since {iso_z(facts.window.start)}, "
            "so ingest progress is unknown: the pipeline may be stopped, the export off, "
            "the database mapping wrong, or the database paused. This is not 'no events'.",
        )
    if facts.window.end > settled:
        reason = (
            f"The window ends at {iso_z(facts.window.end)}, but the source is ingested only "
            f"through {iso_z(settled)} (newest record {iso_z(facts.watermark)}, less an "
            f"ingest lag of {_minutes(facts.ingest_lag)} min). The time after that is not "
            "ingested yet. That is not 'no events'."
        )
        live = LIVE_TOOLS.get(facts.category)
        if live:
            reason += f" For the most recent minutes use the live tools: {live}."
        return ResultStatus.UNAVAILABLE, reason
    if facts.retention_floor is not None and facts.window.start < facts.retention_floor:
        return (
            ResultStatus.UNAVAILABLE,
            f"The window starts at {iso_z(facts.window.start)}, before the source's "
            f"retention floor ({iso_z(facts.retention_floor)}). Older records are gone, so "
            "the window cannot be confirmed as free of events.",
        )
    if facts.truncated_reason is not None:
        return (
            ResultStatus.UNAVAILABLE,
            f"The read stopped early ({facts.truncated_reason}) and found no {name} records "
            "in what it read. Narrow the window.",
        )
    if facts.never_empty_reasons:
        return (
            ResultStatus.UNAVAILABLE,
            "No records were found, but coverage of the window could not be confirmed: "
            + " ".join(facts.never_empty_reasons),
        )
    return (
        ResultStatus.EMPTY,
        f"No {name} records for this database from {iso_z(facts.window.start)} to "
        f"{iso_z(facts.window.end)}; the source is ingested through {iso_z(settled)}. "
        "This is a true negative for the window.",
    )


@dataclass
class SourceRead:
    events: list[HistoryEvent]
    coverage: Coverage


def settle_read(
    *,
    source: str,
    family: str,
    category: HistoryCategory,
    window: TimeWindow,
    now: datetime,
    events: Sequence[HistoryEvent] = (),
    rows_read: int = 0,
    rows_unparsed: int = 0,
    rows_rejected_identity: int = 0,
    watermark: datetime | None = None,
    ingest_lag: timedelta = timedelta(0),
    retention_floor: datetime | None = None,
    truncated_reason: str | None = None,
    read_through: datetime | None = None,
    never_empty_reasons: Sequence[str] = (),
    read_error: HistoryReadError | None = None,
    watermark_error: HistoryReadError | None = None,
    schema_basis: SchemaBasis | None = None,
    warnings: Sequence[str] = (),
) -> SourceRead:
    """Finish one source's read: window filter, dedupe, coverage and status.

    Every adapter ends here, so the status rules exist once.
    """

    kept: list[HistoryEvent] = []
    seen: set[tuple[Any, ...]] = set()
    duplicates = 0
    outside = 0
    newest: datetime | None = None
    for event in events:
        key = event.dedupe_key()
        if key in seen:
            # Delivery is at least once: a recorder can hold one record twice.
            duplicates += 1
            continue
        seen.add(key)
        if newest is None or event.time > newest:
            newest = event.time
        if window.contains(event.time):
            kept.append(event)
        else:
            outside += 1
    kept.sort(key=lambda event: event.time)
    # A record newer than the watermark read proves ingest reached that far.
    if newest is not None and (watermark is None or newest > watermark):
        watermark = newest
    status, reason = decide_status(
        ReadFacts(
            category=category,
            window=window,
            now=now,
            event_count=len(kept),
            rows_read=rows_read,
            rows_accepted=rows_read - rows_unparsed - rows_rejected_identity,
            watermark=watermark,
            ingest_lag=ingest_lag,
            retention_floor=retention_floor,
            truncated_reason=truncated_reason,
            never_empty_reasons=tuple(never_empty_reasons),
            read_error=read_error,
            watermark_error=watermark_error,
        )
    )
    notes = list(warnings)
    if watermark_error is not None:
        notes.append(f"The newest record could not be read ({watermark_error}).")
    coverage = Coverage(
        source=source,
        family=family,
        category=category,
        window=window,
        status=status,
        reason=reason,
        watermark=watermark,
        ingest_lag_seconds=ingest_lag.total_seconds(),
        retention_floor=retention_floor,
        rows_read=rows_read,
        rows_unparsed=rows_unparsed,
        rows_rejected_identity=rows_rejected_identity,
        duplicates_dropped=duplicates,
        rows_outside_window=outside,
        truncated_reason=truncated_reason,
        read_through=read_through,
        schema_basis=schema_basis.value if schema_basis is not None else None,
        warnings=notes,
        error=read_error,
    )
    return SourceRead(kept if read_error is None else [], coverage)


def unverified_schema_gate(
    source: str,
    family: str,
    category: HistoryCategory,
    window: TimeWindow,
    what: str,
) -> Coverage:
    """``unavailable`` for a schema that is not confirmed: never parse on a guess."""

    return Coverage.gated(
        source,
        family,
        category,
        window,
        ResultStatus.UNAVAILABLE,
        f"{SchemaBasis.UNVERIFIED.value}: {what} This source is not read for {category.value} "
        "until the names are confirmed. This is not 'no events'.",
        schema_basis=SchemaBasis.UNVERIFIED.value,
    )


# --- The source interface and the resolver -------------------------------------


@dataclass(frozen=True)
class SourceDescription:
    source: str
    family: str
    database_name: str
    configured: bool
    # Categories this source records for the database.
    categories: tuple[str, ...]
    retention_days: float | None = None
    expected_ingest_lag_seconds: float | None = None
    # The newest record seen for the database.
    watermark: datetime | None = None
    watermark_reason: str | None = None
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "family": self.family,
            "database_name": self.database_name,
            "configured": self.configured,
            "categories": list(self.categories),
            "retention_days": self.retention_days,
            "expected_ingest_lag_seconds": self.expected_ingest_lag_seconds,
            "watermark_utc": _iso_or_none(self.watermark),
            "watermark_reason": self.watermark_reason,
            "notes": list(self.notes),
        }


class HistorySource(Protocol):
    """One recorder, bound to one database."""

    name: str
    # Sources of one family read the same feed (one export, two stores).
    family: str
    database_name: str

    def gate(self, category: HistoryCategory, window: TimeWindow) -> Coverage | None:
        """Why this source is not read for the category, or None when it can be read.

        No network call. ``precondition`` carries the exact setting to enable, as
        inert text for a human.
        """
        ...

    async def describe(self) -> SourceDescription: ...

    async def read(
        self,
        category: HistoryCategory,
        window: TimeWindow,
        filters: Mapping[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> SourceRead: ...


@dataclass
class HistoryResult:
    database_name: str
    category: HistoryCategory
    window: TimeWindow
    events: list[HistoryEvent]
    coverage: list[Coverage]
    status: ResultStatus
    reason: str
    remediation: str | None = None
    truncated: bool = False
    next_window_start: datetime | None = None

    def to_payload(self, data_detail: str = "mask_literals") -> dict[str, Any]:
        """The tool payload. Statement and error text leave only through here.

        ``data_detail`` is the server's data-detail level. The default masks
        literals: history holds statements of other sessions and users.
        """

        level = normalize_data_detail(data_detail)
        events = [event.to_dict() for event in self.events]
        if level != "full":
            for event in events:
                _redact_messages(event, level)
        payload: dict[str, Any] = {
            "database_name": self.database_name,
            "category": self.category.value,
            "window_start_utc": iso_z(self.window.start),
            "window_end_utc": iso_z(self.window.end),
            "event_count": len(events),
            "events": events,
            "truncated": self.truncated,
            "coverage": [item.to_dict() for item in self.coverage],
        }
        if self.next_window_start is not None:
            payload["next_window_start_utc"] = iso_z(self.next_window_start)
        payload.update(status_payload(self.status, self.reason, remediation=self.remediation))
        return apply_data_detail(payload, level)


# The same rule as the incident log: a duplicate key message ends with row values.
_DUPLICATE_KEY = re.compile(r"(duplicate key value is)\b.*", re.IGNORECASE | re.DOTALL)


def _redact_messages(event: dict[str, Any], level: str) -> None:
    message = event.get("message")
    if not isinstance(message, str):
        return
    if level == "metadata_only":
        event["message"] = f"[withheld: data detail is {level}]"
        return
    event["message"] = _DUPLICATE_KEY.sub(r"\1 [redacted]", sanitize_error_message(message))


class HistoryResolver:
    """Answers one question (category and window) from the sources of one database."""

    def __init__(
        self,
        sources: Sequence[HistorySource],
        *,
        database_name: str,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        # Order is priority: the first source's copy of an event is the one kept.
        self._sources = list(sources)
        self._database_name = database_name
        self._now = now

    async def describe(self) -> list[SourceDescription]:
        return list(await asyncio.gather(*(source.describe() for source in self._sources)))

    async def read(
        self,
        category: HistoryCategory,
        window: TimeWindow,
        *,
        filters: Mapping[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> HistoryResult:
        limit = max(1, min(int(limit), MAX_LIMIT))
        gates: list[Coverage] = []
        readable: list[HistorySource] = []
        for source in self._sources:
            gate = source.gate(category, window)
            if gate is None:
                readable.append(source)
            else:
                gates.append(gate)
        reads = list(
            await asyncio.gather(
                *(source.read(category, window, filters, limit) for source in readable)
            )
        )
        events: list[HistoryEvent]
        if category in IDENTITY_MERGE_CATEGORIES:
            events = _merge_by_identity(reads)
        else:
            events = _merge_one_source(reads)
        if category is HistoryCategory.BLOCKING:
            reports = [event for event in events if isinstance(event, BlockedProcessReport)]
            events = [*build_blocking_episodes(reports)]
        events.sort(key=lambda event: event.time)
        next_start = events[limit].time if len(events) > limit else None
        coverage = [read.coverage for read in reads] + gates
        status, reason, remediation = _overall_status(category, events, coverage)
        return HistoryResult(
            database_name=self._database_name,
            category=category,
            window=window,
            events=events[:limit],
            coverage=coverage,
            status=status,
            reason=reason,
            remediation=remediation,
            truncated=next_start is not None
            or any(read.coverage.truncated_reason is not None for read in reads),
            next_window_start=next_start,
        )


def _merge_by_identity(reads: Sequence[SourceRead]) -> list[HistoryEvent]:
    kept: dict[tuple[Any, ...], HistoryEvent] = {}
    for read in reads:
        read.coverage.merged = True
        for event in read.events:
            key = event.dedupe_key()
            first = kept.get(key)
            if first is None:
                kept[key] = event
            elif event.source != first.source and event.source not in first.seen_in:
                kept[key] = replace(first, seen_in=(*first.seen_in, event.source))
    return list(kept.values())


def _merge_one_source(reads: Sequence[SourceRead]) -> list[HistoryEvent]:
    """Events of the one source that covers the window best.

    A record in one recorder cannot be matched to a record in another for these
    categories, so adding two recorders together would count an event twice.
    The source ingested furthest is used; on a tie, the first in priority order.
    """

    chosen: SourceRead | None = None
    for read in reads:
        if not read.events:
            continue
        if chosen is None or _settled(read) > _settled(chosen):
            chosen = read
    for read in reads:
        read.coverage.merged = chosen is None or read is chosen
        if not read.coverage.merged and read.events and chosen is not None:
            read.coverage.not_merged_reason = (
                f"{len(read.events)} record(s) were read but not added: a record here "
                f"cannot be matched to a record from {chosen.coverage.source}, so adding "
                "them would double count."
            )
    return list(chosen.events) if chosen is not None else []


def _settled(read: SourceRead) -> datetime:
    return read.coverage.settled_through or datetime.min.replace(tzinfo=timezone.utc)


def _overall_status(
    category: HistoryCategory,
    events: Sequence[HistoryEvent],
    coverage: Sequence[Coverage],
) -> tuple[ResultStatus, str, str | None]:
    def having(status: ResultStatus) -> list[Coverage]:
        return [item for item in coverage if item.status is status]

    name = category.value
    unavailable = having(ResultStatus.UNAVAILABLE)
    if events:
        reason = f"Found {len(events)} {name} event(s)."
        partial = [item for item in having(ResultStatus.OK) if item.not_ingested_from is not None]
        if partial:
            reason += " " + " ".join(f"{item.source}: {item.reason}" for item in partial)
        if unavailable:
            reason += " Not read: " + ", ".join(item.source for item in unavailable) + "."
        return ResultStatus.OK, reason, None
    empty = having(ResultStatus.EMPTY)
    if empty:
        return (
            ResultStatus.EMPTY,
            " ".join(f"{item.source}: {item.reason}" for item in empty),
            None,
        )
    if unavailable:
        return (
            ResultStatus.UNAVAILABLE,
            " ".join(f"{item.source}: {item.reason}" for item in unavailable),
            None,
        )
    preconditions = having(ResultStatus.PRECONDITION)
    if preconditions:
        steps = [
            f"{item.source}: {item.remediation}" for item in preconditions if item.remediation
        ]
        return (
            ResultStatus.PRECONDITION,
            f"No configured source records {name} for this database. "
            + " ".join(f"{item.source}: {item.reason}" for item in preconditions),
            "\n".join(steps) or None,
        )
    if coverage:
        return (
            ResultStatus.NOT_SUPPORTED,
            " ".join(f"{item.source}: {item.reason}" for item in coverage),
            None,
        )
    return (
        ResultStatus.PRECONDITION,
        f"No history source is configured for this database, so {name} history cannot be "
        "read. This is not 'no events'.",
        "Map at least one history source for this database in the local database policy "
        "(AZURE_SQL_DATABASE_POLICY_FILE): a Log Analytics workspace, a database watcher "
        "data store, an Extended Events session, or Loki.",
    )


# --- Parsers shared by the adapters --------------------------------------------

_UTC_TEXT = re.compile(
    r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?\s*(Z|[+-]\d{2}:?\d{2})?"
)
_HEX_HASH = re.compile(r"0[xX]([0-9A-Fa-f]{1,16})")
_DECIMAL = re.compile(r"-?\d+")


def parse_utc(value: object) -> datetime | None:
    """A recorder timestamp as UTC. Fractions past microseconds are cut; no zone means UTC."""

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    match = _UTC_TEXT.fullmatch(value.strip())
    if match is None:
        return None
    day, clock, fraction, zone = match.groups()
    offset = "+00:00" if zone in {None, "Z"} else zone[:3] + ":" + zone[-2:]
    microseconds = (fraction or "0")[:6].ljust(6, "0")
    try:
        parsed = datetime.fromisoformat(f"{day}T{clock}.{microseconds}{offset}")
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc)


def normalize_query_hash(value: object) -> str | None:
    """A query hash as Query Store shows it: ``0x`` and 16 hex digits.

    The diagnostic logs carry the hash as a signed decimal number. Microsoft's
    rule to match it is ``CAST(CAST(<hash_value> AS bigint) AS binary(8))``.
    """

    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, float):
        if not value.is_integer():
            return None
        value = int(value)
    text = str(value).strip()
    match = _HEX_HASH.fullmatch(text)
    if match is not None:
        return "0x" + match.group(1).upper().rjust(16, "0")
    if not _DECIMAL.fullmatch(text):
        return None
    number = int(text)
    if not -(2**63) <= number < 2**64:
        return None
    return f"0x{number & 0xFFFFFFFFFFFFFFFF:016X}"


def to_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(float(value)) if not isinstance(value, int) else value  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def to_float(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def to_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().casefold()
    if text in {"1", "true"}:
        return True
    if text in {"0", "false"}:
        return False
    return None


def clean_text(value: object, limit: int = MAX_STATEMENT_CHARS) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text[:limit] if text else None


def parse_xml(text: object) -> ET.Element | None:
    """Parse recorder XML. Oversized or malformed text is None, never an exception."""

    if not isinstance(text, str) or not text.strip() or len(text) > MAX_XML_CHARS:
        return None
    try:
        return ET.fromstring(text)
    except ET.ParseError:
        return None


def parse_blocked_process_report(
    report: str | ET.Element,
    *,
    time: datetime,
    database: str,
    source: str,
    duration_ms: float | None = None,
    lock_mode: str | None = None,
    resource_owner_type: str | None = None,
) -> BlockedProcessReport | None:
    """One blocked process report. None when either process node is missing."""

    root = report if isinstance(report, ET.Element) else parse_xml(report)
    if root is None:
        return None
    if root.tag != "blocked-process-report":
        nested = root.find(".//blocked-process-report")
        if nested is None:
            return None
        root = nested
    blocked = root.find("./blocked-process/process")
    blocking = root.find("./blocking-process/process")
    if blocked is None or blocking is None:
        return None
    return BlockedProcessReport(
        time=time,
        database=database,
        source=source,
        blocked=_session_ref(blocked),
        blocking=_session_ref(blocking),
        monitor_loop=to_int(root.get("monitorLoop")),
        wait_resource=clean_text(blocked.get("waitresource")),
        wait_time_ms=to_int(blocked.get("waittime")),
        lock_mode=clean_text(blocked.get("lockMode")) or lock_mode,
        resource_owner_type=resource_owner_type,
        duration_ms=duration_ms,
    )


def _session_ref(process: ET.Element) -> SessionRef:
    frames = tuple(
        {
            "sql_handle": frame.get("sqlhandle"),
            "statement_start": to_int(frame.get("stmtstart")),
            "statement_end": to_int(frame.get("stmtend")),
            "line": to_int(frame.get("line")),
        }
        for frame in process.findall("./executionStack/frame")
        if frame.get("sqlhandle")
    )
    return SessionRef(
        session_id=to_int(process.get("spid")),
        status=clean_text(process.get("status")),
        transaction_count=to_int(process.get("trancount")),
        transaction_id=clean_text(process.get("xactid")),
        last_batch_started=clean_text(process.get("lastbatchstarted")),
        last_batch_completed=clean_text(process.get("lastbatchcompleted")),
        client_app=clean_text(process.get("clientapp")),
        host_name=clean_text(process.get("hostname")),
        login_name=clean_text(process.get("loginname")),
        isolation_level=clean_text(process.get("isolationlevel")),
        statement_text=clean_text(process.findtext("./inputbuf")),
        frames=frames,
    )


def deadlock_event(
    graph: str | ET.Element,
    *,
    time: datetime,
    database: str,
    source: str,
) -> DeadlockEvent | None:
    """One deadlock graph as an event. None when the graph names no process."""

    root = graph if isinstance(graph, ET.Element) else parse_xml(graph)
    if root is None:
        return None
    if root.tag != "deadlock":
        nested = root.find(".//deadlock")
        if nested is None:
            return None
        root = nested
    processes = root.findall("./process-list/process")
    if not processes:
        return None
    fingerprint = tuple(
        sorted(f"{process.get('id')}|{process.get('lastbatchstarted')}" for process in processes)
    )
    return DeadlockEvent(
        time=time,
        database=database,
        source=source,
        detail=parse_deadlock_graph(root),
        fingerprint=fingerprint,
    )


def build_blocking_episodes(
    reports: Sequence[BlockedProcessReport],
    *,
    gap_seconds: int = EPISODE_GAP_SECONDS,
) -> list[BlockingEpisode]:
    """Group reports into episodes, one per head blocker.

    Reports of one deadlock monitor pass form a snapshot of the chains. The head
    blocker is the session that blocks others and waits on nobody in that
    snapshot. Snapshots of the same head blocker closer than ``gap_seconds`` are
    one episode.
    """

    open_episodes: dict[tuple[Any, ...], _EpisodeBuilder] = {}
    finished: list[_EpisodeBuilder] = []
    for snapshot in _snapshots(sorted(reports, key=lambda report: report.time)):
        blocked_by = {
            report.blocked.session_id: report.blocking.session_id for report in snapshot
        }
        for report in snapshot:
            head, depth = _head_of(report, blocked_by)
            head_ref = next(
                (item.blocking for item in snapshot if item.blocking.session_id == head),
                report.blocking,
            )
            key = (head, head_ref.transaction_id)
            builder = open_episodes.get(key)
            if builder is not None and (
                report.time - builder.last_seen
            ).total_seconds() > gap_seconds:
                finished.append(builder)
                builder = None
            if builder is None:
                builder = _EpisodeBuilder(head_ref, report.time)
                open_episodes[key] = builder
            builder.add(report, depth)
    finished.extend(open_episodes.values())
    return sorted((builder.build() for builder in finished), key=lambda episode: episode.time)


def _snapshots(reports: Sequence[BlockedProcessReport]) -> list[list[BlockedProcessReport]]:
    snapshots: list[list[BlockedProcessReport]] = []
    for report in reports:
        current = snapshots[-1] if snapshots else None
        if (
            current is not None
            and current[0].monitor_loop == report.monitor_loop
            and (report.time - current[0].time).total_seconds() <= _SNAPSHOT_SECONDS
        ):
            current.append(report)
        else:
            snapshots.append([report])
    return snapshots


def _head_of(
    report: BlockedProcessReport, blocked_by: Mapping[int | None, int | None]
) -> tuple[int | None, int]:
    head = report.blocking.session_id
    depth = 1
    visited = {report.blocked.session_id}
    while head in blocked_by and head not in visited:
        visited.add(head)
        head = blocked_by[head]
        depth += 1
    return head, depth


class _EpisodeBuilder:
    def __init__(self, head: SessionRef, first_seen: datetime) -> None:
        self.head = head
        self.first_seen = first_seen
        self.last_seen = first_seen
        self.reports = 0
        self.depth = 0
        self.victims: dict[int | None, BlockingVictim] = {}
        self.wait_resources: set[str] = set()
        self.lock_modes: set[str] = set()
        self.sources: set[str] = set()
        self.database = ""

    def add(self, report: BlockedProcessReport, depth: int) -> None:
        self.reports += 1
        self.depth = max(self.depth, depth)
        self.last_seen = max(self.last_seen, report.time)
        self.database = report.database
        self.sources.update({report.source, *report.seen_in})
        if report.wait_resource:
            self.wait_resources.add(report.wait_resource)
        if report.lock_mode:
            self.lock_modes.add(report.lock_mode)
        # A blocking-process node is the fullest view of the head blocker's state.
        if report.blocking.session_id == self.head.session_id:
            self.head = report.blocking
        session = report.blocked.session_id
        earlier = self.victims.get(session)
        waits = [
            wait
            for wait in (report.wait_time_ms, earlier.max_wait_ms if earlier else None)
            if wait is not None
        ]
        self.victims[session] = BlockingVictim(
            session_id=session,
            blocked_by_session_id=report.blocking.session_id,
            max_wait_ms=max(waits) if waits else None,
            lock_mode=report.lock_mode,
            wait_resource=report.wait_resource,
            client_app=report.blocked.client_app,
            login_name=report.blocked.login_name,
            statement_text=report.blocked.statement_text,
            frames=report.blocked.frames,
        )

    def build(self) -> BlockingEpisode:
        waits = [victim.max_wait_ms for victim in self.victims.values() if victim.max_wait_ms]
        sources = tuple(sorted(self.sources))
        return BlockingEpisode(
            time=self.first_seen,
            database=self.database,
            source=sources[0] if sources else "",
            first_seen=self.first_seen,
            last_seen=self.last_seen,
            head_blocker=self.head,
            victims=tuple(self.victims.values()),
            max_wait_ms=max(waits) if waits else None,
            max_chain_depth=self.depth,
            report_count=self.reports,
            wait_resources=tuple(sorted(self.wait_resources)),
            lock_modes=tuple(sorted(self.lock_modes)),
            sources=sources,
            head_blocker_idle_in_transaction=(
                (self.head.status or "").casefold() == "sleeping"
                and (self.head.transaction_count or 0) > 0
            ),
        )


# --- Transport shared by the HTTP adapters -------------------------------------


@dataclass(frozen=True)
class HttpRequest:
    method: str
    url: str
    params: tuple[tuple[str, str], ...] = ()
    headers: Mapping[str, str] = field(default_factory=dict)
    json_body: Any = None
    timeout: float = 30.0
    max_bytes: int = MAX_RESPONSE_BYTES


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes
    # The body was cut at the request's ``max_bytes``.
    truncated: bool = False


class HttpTransport(Protocol):
    async def send(self, request: HttpRequest) -> HttpResponse: ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        # A redirect is a sign-in page; the token must never follow it elsewhere.
        return None


class UrllibTransport:
    """HTTPS with the standard library: no redirects, a size cap, value-free errors."""

    def __init__(self, *, ca_bundle: str | None = None) -> None:
        self._ca_bundle = ca_bundle

    async def send(self, request: HttpRequest) -> HttpResponse:
        if urllib.parse.urlsplit(request.url).scheme != "https":
            raise HistoryReadError("config", "A history source address must use https.")
        return await asyncio.to_thread(self._send, request)

    def _send(self, request: HttpRequest) -> HttpResponse:
        url = request.url
        if request.params:
            url += "?" + urllib.parse.urlencode(request.params)
        data = None
        headers = {"Accept": "application/json", "User-Agent": "azure-sql-mcp", **request.headers}
        if request.json_body is not None:
            data = json.dumps(request.json_body).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        context = ssl.create_default_context(cafile=self._ca_bundle)
        opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=context), _NoRedirect()
        )
        outgoing = urllib.request.Request(url, data=data, headers=headers, method=request.method)
        try:
            with opener.open(outgoing, timeout=request.timeout) as response:
                body = response.read(request.max_bytes + 1)
                return HttpResponse(
                    response.status, body[: request.max_bytes], len(body) > request.max_bytes
                )
        except urllib.error.HTTPError as exc:
            return HttpResponse(exc.code, exc.read(_ERROR_BODY_BYTES))
        except TimeoutError:
            raise HistoryReadError("timeout", "The request timed out.", transient=True) from None
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, ssl.SSLError):
                raise HistoryReadError(
                    "tls",
                    "TLS verification failed. Configure the CA bundle for the service's "
                    "certificate chain.",
                ) from None
            if isinstance(exc.reason, TimeoutError):
                raise HistoryReadError(
                    "timeout", "The request timed out.", transient=True
                ) from None
            raise HistoryReadError(
                "network", "The service could not be reached.", transient=True
            ) from None
        except OSError:
            raise HistoryReadError(
                "network", "The service could not be reached.", transient=True
            ) from None


def raise_for_status(
    response: HttpResponse,
    *,
    service: str,
    hints: Mapping[int, str] | None = None,
) -> None:
    """Raise a value-free error for a response that is not 200.

    The body can quote the query and real names, so it is never repeated.
    ``hints`` adds a fixed sentence for a status code, for example the role to grant.
    """

    status = response.status
    if status == 200:
        return
    hint = (hints or {}).get(status, "")
    tail = f" {hint}" if hint else ""
    if 300 <= status < 400:
        raise HistoryReadError(
            "auth",
            f"{service} redirected the request, usually to a sign-in page: the token was "
            f"not accepted.{tail}",
            status_code=status,
        )
    if status == 401:
        raise HistoryReadError(
            "auth", f"{service} rejected the token (401).{tail}", status_code=status
        )
    if status == 403:
        raise HistoryReadError(
            "auth", f"{service} refused access (403).{tail}", status_code=status
        )
    if status == 404:
        raise HistoryReadError(
            "not_found",
            f"{service} returned 404: the configured resource was not found.{tail}",
            status_code=status,
        )
    if status == 429:
        raise HistoryReadError(
            "rate_limited",
            f"{service} is limiting the request rate (429).",
            status_code=status,
            transient=True,
        )
    if status >= 500:
        raise HistoryReadError(
            "server", f"{service} failed ({status}).", status_code=status, transient=True
        )
    if status in {400, 422}:
        raise HistoryReadError(
            "rejected", f"{service} rejected the query ({status}).{tail}", status_code=status
        )
    raise HistoryReadError("http", f"{service} returned HTTP {status}.", status_code=status)


def decode_json(response: HttpResponse, *, service: str) -> Any:
    if response.truncated:
        raise HistoryReadError(
            "too_large", f"The {service} response was larger than the size cap."
        )
    try:
        return json.loads(response.body)
    except ValueError:
        raise HistoryReadError(
            "protocol", f"{service} returned a response that is not JSON."
        ) from None


def credential_for(config: ServerConfig) -> "TokenCredential":
    """The Entra credential for the history sources, by the server's auth mode.

    Follows ``auth.AzureSqlAuthenticator``. SQL password mode has no Entra
    identity, so the default credential chain is used.
    """

    if config.auth_mode == AuthMode.INTERACTIVE:
        return InteractiveBrowserCredential(
            tenant_id=config.tenant_id, client_id=config.client_id
        )
    if config.auth_mode == AuthMode.SERVICE_PRINCIPAL:
        return ClientSecretCredential(
            tenant_id=config.tenant_id or "",
            client_id=config.client_id or "",
            client_secret=config.client_secret or "",
        )
    return DefaultAzureCredential()


class EntraTokenProvider:
    """Keeps one Entra access token for one scope until shortly before it expires."""

    def __init__(
        self,
        credential: "TokenCredential",
        scope: str,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._credential = credential
        self._scope = scope
        self._clock = clock
        self._token: AccessToken | None = None
        self._lock = asyncio.Lock()

    async def bearer_token(self) -> str:
        async with self._lock:
            token = self._token
            if token is None or token.expires_on - _TOKEN_REFRESH_MARGIN_SECONDS <= self._clock():
                try:
                    # The CLI credential starts a process: keep it off the event loop.
                    token = await asyncio.to_thread(self._credential.get_token, self._scope)
                except Exception:
                    raise HistoryReadError(
                        "auth", "Could not get an Entra token for the history source."
                    ) from None
                self._token = token
            return token.token


async def send_authorized(
    transport: HttpTransport, tokens: EntraTokenProvider, request: HttpRequest
) -> HttpResponse:
    token = await tokens.bearer_token()
    return await transport.send(
        replace(request, headers={**request.headers, "Authorization": f"Bearer {token}"})
    )


def _iso_or_none(value: datetime | None) -> str | None:
    return iso_z(value) if value is not None else None


def _minutes(value: timedelta) -> float:
    return round(value.total_seconds() / 60, 1)
