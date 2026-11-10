"""History from Azure SQL diagnostic logs kept in Loki, read through Grafana.

Only Grafana is called, never Loki directly: direct Loki selects a tenant by the
``X-Scope-OrgID`` header alone, which proves no identity. Grafana checks an Entra
token, and its datasource sets the tenant.

The request address carries LogQL with real server and database names. Nothing
here logs it, and errors are value-free.

LogQL is built here and only here. Its parts are constants, the validated
identity from the local database policy, and validated labels from the settings.
No caller text reaches it.

Loki stamps a line when it ingests it, which can be long after the record's own
``time``. A read therefore covers the window plus the configured stamp delay in
Loki time, and records are kept by their own ``time``.

Schema: the Loki ``query_range`` response and the Azure resource log envelope
are documented. The names inside ``properties`` are the documented
``AzureDiagnostics`` column names without the type suffix. That is a derivation,
not a recorded sample: a line whose properties do not hold the expected field
counts as unparsed, and a read where no line parses is ``unavailable``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import ssl
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from .history_azure_monitor import CATEGORY_KEY_BY_HISTORY
from .history_azure_monitor import DIAGNOSTICS_FAMILY
from .history_azure_monitor import POLICY_FILE_HINT
from .history_azure_monitor import diagnostic_event
from .history_azure_monitor import export_property_name
from .history_sources import DEFAULT_LIMIT
from .history_sources import MAX_RESPONSE_BYTES
from .history_sources import Coverage
from .history_sources import DatabaseIdentity
from .history_sources import EntraTokenProvider
from .history_sources import HistoryCategory
from .history_sources import HistoryEvent
from .history_sources import HistoryReadError
from .history_sources import HttpRequest
from .history_sources import HttpTransport
from .history_sources import SchemaBasis
from .history_sources import SourceDescription
from .history_sources import SourceRead
from .history_sources import TimeWindow
from .history_sources import decode_json
from .history_sources import parse_utc
from .history_sources import raise_for_status
from .history_sources import send_authorized
from .history_sources import settle_read
from .incident_log import note_exception
from .result_status import ResultStatus

SOURCE = "loki"
ENV_GRAFANA_URL = "AZURE_SQL_LOKI_GRAFANA_URL"
ENV_DATASOURCE_UID = "AZURE_SQL_LOKI_DATASOURCE_UID"
ENV_CA_BUNDLE = "AZURE_SQL_LOKI_CA_BUNDLE"
ENV_TOKEN_SCOPE = "AZURE_SQL_LOKI_TOKEN_SCOPE"
ENV_SELECTOR = "AZURE_SQL_LOKI_SELECTOR"
ENV_IDENTITY_LABELS = "AZURE_SQL_LOKI_IDENTITY_LABELS"
ENV_STAMP_DELAY_MINUTES = "AZURE_SQL_LOKI_STAMP_DELAY_MINUTES"
ENV_TIMEOUT_SECONDS = "AZURE_SQL_LOKI_TIMEOUT_SECONDS"
ENV_MAX_LINES = "AZURE_SQL_LOKI_MAX_LINES"
ENV_NAMES = (
    ENV_GRAFANA_URL, ENV_DATASOURCE_UID, ENV_CA_BUNDLE, ENV_TOKEN_SCOPE, ENV_SELECTOR,
    ENV_IDENTITY_LABELS, ENV_STAMP_DELAY_MINUTES, ENV_TIMEOUT_SECONDS, ENV_MAX_LINES,
)
# Azure Managed Grafana's Entra application. A self-hosted Grafana has its own.
AZURE_MANAGED_GRAFANA_SCOPE = "ce34e7e5-485f-4d76-964f-b3d2b16d1e4f/.default"
DEFAULT_SELECTOR: tuple[tuple[str, str], ...] = (("service_name", "mssql"),)
CATEGORY_LABEL = "category"
# Identity fields inside each line; no label names the database today.
SERVER_FIELD = "LogicalServerName"
DATABASE_FIELD = "properties.DatabaseName"
# The categories read from Loki, spelled as the lowercase label values. Audit
# events are security data and are not read from here.
LOKI_CATEGORIES = frozenset(
    {"blocks", "deadlocks", "timeouts", "errors", "databasewaitstatistics"}
)
MAX_PAGES = 200
_QUERY_RANGE_PATH = "/api/datasources/proxy/uid/{uid}/loki/api/v1/query_range"
# Clock skew allowed between a record's time and its Loki timestamp.
_STAMP_TOLERANCE = timedelta(minutes=2)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_RE2_META = frozenset("\\.+*?()|[]{}^$")
_LABEL_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_DATASOURCE_UID = re.compile(r"[A-Za-z0-9_-]{1,64}")
_ENTRA_SCOPE = re.compile(r"\S+/\.default")
# Large XML documents: fewer lines per page.
_SMALL_PAGE_CATEGORIES = frozenset({"blocks", "deadlocks"})


# --- Settings -------------------------------------------------------------------------


@dataclass(frozen=True)
class LokiSettings:
    """The Grafana datasource proxy to read, shared by every database."""

    grafana_url: str | None = None
    datasource_uid: str | None = None
    ca_bundle: str | None = None
    token_scope: str = AZURE_MANAGED_GRAFANA_SCOPE
    selector: tuple[tuple[str, str], ...] = DEFAULT_SELECTOR
    identity_labels: tuple[str, str] | None = None  # (server label, database label)
    stamp_delay_minutes: int = 180
    timeout_seconds: int = 30
    max_lines: int = 10_000
    invalid: str | None = None  # the variable whose bad value turned the source off
    invalid_reason: str | None = None  # never repeats the configured value

    @property
    def enabled(self) -> bool:
        return self.invalid is None and bool(self.grafana_url and self.datasource_uid)


class _InvalidSetting(ValueError):
    def __init__(self, variable: str, reason: str) -> None:
        super().__init__(reason)
        self.variable = variable
        self.reason = reason


def parse_loki_settings(values: Mapping[str, str | None]) -> LokiSettings:
    """Parse raw environment strings.

    A bad or partial value turns the source off and is named in ``invalid``, with
    a reason that never repeats the value: an optional source must not stop the
    server, and Grafana addresses stay private.
    """

    try:
        return _parse_settings(values)
    except _InvalidSetting as exc:
        return LokiSettings(invalid=exc.variable, invalid_reason=exc.reason)


def _parse_settings(values: Mapping[str, str | None]) -> LokiSettings:
    def raw(name: str) -> str | None:
        return (values.get(name) or "").strip() or None

    url = raw(ENV_GRAFANA_URL)
    uid = raw(ENV_DATASOURCE_UID)
    if url is None and uid is None:
        return LokiSettings()
    if url is None:
        raise _InvalidSetting(ENV_GRAFANA_URL, f"is required when {ENV_DATASOURCE_UID} is set.")
    if uid is None:
        raise _InvalidSetting(ENV_DATASOURCE_UID, f"is required when {ENV_GRAFANA_URL} is set.")
    if not _DATASOURCE_UID.fullmatch(uid):
        raise _InvalidSetting(
            ENV_DATASOURCE_UID, "must be 1 to 64 letters, digits, hyphens, or underscores."
        )
    token_scope = raw(ENV_TOKEN_SCOPE) or AZURE_MANAGED_GRAFANA_SCOPE
    if not _ENTRA_SCOPE.fullmatch(token_scope):
        raise _InvalidSetting(ENV_TOKEN_SCOPE, "must be an Entra scope that ends with /.default.")
    selector = _selector(raw(ENV_SELECTOR))
    numbers: dict[str, int] = {}
    for name, default, low, high in (
        (ENV_STAMP_DELAY_MINUTES, 180, 0, 1440),
        (ENV_TIMEOUT_SECONDS, 30, 5, 120),
        (ENV_MAX_LINES, 10_000, 100, 50_000),
    ):
        text = raw(name)
        if text is None:
            numbers[name] = default
        elif text.isdigit() and low <= int(text) <= high:
            numbers[name] = int(text)
        else:
            raise _InvalidSetting(name, f"must be a whole number from {low} to {high}.")
    return LokiSettings(
        grafana_url=_grafana_url(url),
        datasource_uid=uid,
        ca_bundle=_ca_bundle(raw(ENV_CA_BUNDLE)),
        token_scope=token_scope,
        selector=selector,
        identity_labels=_identity_labels(raw(ENV_IDENTITY_LABELS), selector),
        stamp_delay_minutes=numbers[ENV_STAMP_DELAY_MINUTES],
        timeout_seconds=numbers[ENV_TIMEOUT_SECONDS],
        max_lines=numbers[ENV_MAX_LINES],
    )


def _grafana_url(value: str) -> str:
    parts = urlsplit(value)
    if (
        parts.scheme != "https"
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
        or any(character.isspace() or not character.isprintable() for character in value)
    ):
        raise _InvalidSetting(
            ENV_GRAFANA_URL,
            "must be an https URL with a host and no user info, query, or fragment.",
        )
    return urlunsplit(("https", parts.netloc, parts.path.rstrip("/"), "", ""))


def _ca_bundle(value: str | None) -> str | None:
    if value is None:
        return None
    path = str(Path(value).expanduser())
    try:
        ssl.create_default_context(cafile=path)
    except (OSError, ValueError):
        # ssl.SSLError is an OSError: an unreadable file or one with no certificate.
        raise _InvalidSetting(ENV_CA_BUNDLE, "must be a readable PEM certificate bundle.") from None
    return path


def _selector(value: str | None) -> tuple[tuple[str, str], ...]:
    if value is None:
        return DEFAULT_SELECTOR
    pairs: list[tuple[str, str]] = []
    for item in value.split(","):
        name, separator, label_value = (part.strip() for part in item.partition("="))
        if (
            not separator
            or not _LABEL_NAME.fullmatch(name)
            or name == CATEGORY_LABEL
            or name in {existing for existing, _ in pairs}
            or not label_value
            or any(not c.isprintable() or c in {'"', "\\"} for c in label_value)
        ):
            raise _InvalidSetting(
                ENV_SELECTOR,
                "must be comma-separated label=value pairs with distinct label names, "
                "without the category label, quotes, or backslashes.",
            )
        pairs.append((name, label_value))
    return tuple(pairs)


def _identity_labels(
    value: str | None, selector: tuple[tuple[str, str], ...]
) -> tuple[str, str] | None:
    if value is None:
        return None
    names = [part.strip() for part in value.split(",")]
    reserved = {CATEGORY_LABEL} | {name for name, _ in selector}
    if (
        len(names) != 2
        or names[0] == names[1]
        or any(not _LABEL_NAME.fullmatch(name) or name in reserved for name in names)
    ):
        raise _InvalidSetting(
            ENV_IDENTITY_LABELS,
            "must be two distinct label names, the server label first, that are not "
            "category or a selector label.",
        )
    return names[0], names[1]


@dataclass(frozen=True)
class LokiBinding:
    """The ``loki`` policy entry of one database."""

    identity: DatabaseIdentity
    # What the database's diagnostic setting exports to Loki. ``empty`` is
    # possible only for a category listed here.
    categories: tuple[str, ...]
    ingest_lag_minutes: int = 35
    retention_days: int | None = None

    @classmethod
    def from_mapping(cls, database_name: str, value: Any) -> "LokiBinding":
        where = f"loki for database {database_name!r}"
        if not isinstance(value, Mapping):
            raise ValueError(f"{where} must be an object.")
        required = {"logical_server", "database", "categories"}
        optional = {"ingest_lag_minutes", "retention_days"}
        unknown = sorted(set(value) - required - optional)
        if unknown:
            raise ValueError(f"{where}: unknown field(s) {', '.join(map(repr, unknown))}.")
        missing = sorted(required - set(value))
        if missing:
            raise ValueError(f"{where}: missing field(s) {', '.join(missing)}.")
        categories = value["categories"]
        if (
            not isinstance(categories, list)
            or not categories
            or not all(isinstance(item, str) for item in categories)
        ):
            raise ValueError(f"{where}: categories must be a non-empty list of category names.")
        names = {item.strip().casefold() for item in categories}
        unknown_categories = sorted(names - LOKI_CATEGORIES)
        if unknown_categories:
            raise ValueError(
                f"{where}: unknown category {', '.join(map(repr, unknown_categories))}; use: "
                f"{', '.join(sorted(LOKI_CATEGORIES))}. Audit and Query Store categories are "
                "not read from Loki."
            )
        try:
            identity = DatabaseIdentity(value["logical_server"], value["database"])
        except ValueError as exc:
            raise ValueError(f"{where}: {exc}") from None
        lag = value.get("ingest_lag_minutes", 35)
        retention = value.get("retention_days")
        for name, number, low, high in (
            ("ingest_lag_minutes", lag, 0, 1440),
            ("retention_days", retention, 1, 36500),
        ):
            if number is not None and (
                isinstance(number, bool) or not isinstance(number, int) or not low <= number <= high
            ):
                raise ValueError(f"{where}: {name} must be a whole number from {low} to {high}.")
        return cls(identity, tuple(sorted(names)), lag, retention)


# --- LogQL ----------------------------------------------------------------------------


def re2_quote_meta(value: str) -> str:
    """Escape RE2 metacharacters, as Go's ``regexp.QuoteMeta`` does."""

    return "".join(
        "\\" + character if character in _RE2_META else character for character in value
    )


def logql_string(value: str) -> str:
    """Return a double-quoted LogQL string literal that decodes to ``value``."""

    escaped: list[str] = []
    for character in value:
        if character == "\\":
            escaped.append("\\\\")
        elif character == '"':
            escaped.append('\\"')
        elif not character.isprintable():
            code = ord(character)
            escaped.append(f"\\u{code:04x}" if code <= 0xFFFF else f"\\U{code:08x}")
        else:
            escaped.append(character)
    return '"' + "".join(escaped) + '"'


def _exact_case_insensitive(value: str) -> str:
    return logql_string("(?i)^" + re2_quote_meta(value) + "$")


def build_query(
    settings: LokiSettings, identity: DatabaseIdentity, categories: Sequence[str]
) -> str:
    """LogQL for one database's lines in the given diagnostic log categories."""

    if not categories or not set(categories) <= LOKI_CATEGORIES:
        raise ValueError("Every category must be a supported diagnostic log category.")
    matchers = [f"{name}={logql_string(value)}" for name, value in settings.selector]
    names = sorted(set(categories))
    if len(names) == 1:
        matchers.append(f"{CATEGORY_LABEL}={logql_string(names[0])}")
    else:
        matchers.append(f"{CATEGORY_LABEL}=~{logql_string('|'.join(names))}")
    stages: list[str] = []
    if settings.identity_labels is None:
        # The closing quote ends the resourceId value, so SalesDb never matches SalesDb_Dev.
        suffix = f'/SERVERS/{identity.logical_server}/DATABASES/{identity.database}"'
        stages.append("|~ " + logql_string("(?i)" + re2_quote_meta(suffix)))
    else:
        server_label, database_label = settings.identity_labels
        matchers.append(f"{server_label}=~{_exact_case_insensitive(identity.logical_server)}")
        matchers.append(f"{database_label}=~{_exact_case_insensitive(identity.database)}")
    stages.extend(
        [
            f"| json mcp_server={logql_string(SERVER_FIELD)}, "
            f"mcp_database={logql_string(DATABASE_FIELD)}",
            '| __error__=""',
            f"| mcp_server=~{_exact_case_insensitive(identity.logical_server)}",
            f"| mcp_database=~{_exact_case_insensitive(identity.database)}",
        ]
    )
    return "{" + ", ".join(matchers) + "} " + " ".join(stages)


# --- Lines and records ----------------------------------------------------------------


@dataclass(frozen=True)
class LokiEntry:
    timestamp_ns: int
    line: str


@dataclass(frozen=True)
class LokiRead:
    entries: tuple[LokiEntry, ...]
    truncated_reason: str | None  # max_lines, max_bytes, max_pages, or equal_timestamps


def parse_streams(document: Any) -> list[LokiEntry]:
    """The lines of a ``query_range`` streams response, oldest first."""

    data = document.get("data") if isinstance(document, dict) else None
    if (
        not isinstance(document, dict)
        or document.get("status") != "success"
        or not isinstance(data, dict)
        or data.get("resultType") != "streams"
        or not isinstance(data.get("result"), list)
    ):
        raise _protocol_error()
    entries: list[LokiEntry] = []
    for stream in data["result"]:
        values = stream.get("values") if isinstance(stream, dict) else None
        if not isinstance(values, list):
            raise _protocol_error()
        for value in values:
            # [timestamp_ns, line] plus, with categorized labels, a metadata object.
            if (
                not isinstance(value, list)
                or len(value) < 2
                or not isinstance(value[0], str)
                or not value[0].isdigit()
                or not isinstance(value[1], str)
            ):
                raise _protocol_error()
            entries.append(LokiEntry(int(value[0]), value[1]))
    entries.sort(key=lambda entry: entry.timestamp_ns)
    return entries


def _protocol_error() -> HistoryReadError:
    return HistoryReadError(
        "protocol", "Grafana returned a response that is not a Loki streams result."
    )


def record_from_line(
    line: str, identity: DatabaseIdentity
) -> tuple[datetime | None, Mapping[str, Any], str | None]:
    """The record time and properties of one line, or why it is not used.

    The third value is ``unparsed`` or ``identity`` for a line that is skipped.
    """

    try:
        document = json.loads(line)
    except ValueError:
        return None, {}, "unparsed"
    if not isinstance(document, dict):
        return None, {}, "unparsed"
    record_time = parse_utc(document.get("time"))
    properties = document.get("properties")
    if record_time is None or not isinstance(properties, dict):
        return None, {}, "unparsed"
    server = document.get(SERVER_FIELD)
    database = properties.get("DatabaseName")
    resource = document.get("resourceId")
    suffix = f"/SERVERS/{identity.logical_server}/DATABASES/{identity.database}".casefold()
    if not (
        isinstance(server, str)
        and isinstance(database, str)
        and isinstance(resource, str)
        and server.casefold() == identity.logical_server.casefold()
        and database.casefold() == identity.database.casefold()
        and resource.casefold().endswith(suffix)
    ):
        return None, {}, "identity"
    return record_time, properties, None


def _to_ns(value: datetime) -> int:
    return (value - _EPOCH) // timedelta(microseconds=1) * 1000


def _from_ns(value: int) -> datetime:
    return _EPOCH + timedelta(microseconds=value // 1000)


def _entry_key(entry: LokiEntry) -> tuple[int, bytes]:
    digest = hashlib.blake2b(entry.line.encode("utf-8", "surrogatepass"), digest_size=16)
    return entry.timestamp_ns, digest.digest()


# --- The source -----------------------------------------------------------------------

_SETTINGS_REMEDIATION = (
    f"Set {ENV_GRAFANA_URL} and {ENV_DATASOURCE_UID} (and {ENV_CA_BUNDLE} when Grafana uses "
    "a custom CA), then restart the MCP server."
)
_POLICY_SNIPPET = (
    f"Add a loki entry to this database's history_sources in {POLICY_FILE_HINT}:\n"
    '"loki": {"logical_server": "<short server name in resourceId>", "database": "<database '
    'name in the logs>", "categories": ["blocks", "deadlocks", "timeouts", "errors", '
    '"databasewaitstatistics"]}\n'
    "List only the categories that the database's diagnostic setting exports to Loki."
)
_REDIRECT_HINT = f"Check {ENV_TOKEN_SCOPE}."
_HINTS = {
    301: _REDIRECT_HINT,
    302: _REDIRECT_HINT,
    303: _REDIRECT_HINT,
    307: _REDIRECT_HINT,
    308: _REDIRECT_HINT,
    401: f"Check {ENV_TOKEN_SCOPE} and the identity the server signs in with.",
    403: "The identity needs the Grafana Viewer role and query access to the Loki datasource.",
    404: f"Check {ENV_DATASOURCE_UID}; the datasource proxy route may also be disabled.",
}


class LokiHistorySource:
    """Azure SQL diagnostic log categories in Loki, through the Grafana datasource proxy."""

    name = SOURCE
    # The same export feed as the Log Analytics source.
    family = DIAGNOSTICS_FAMILY

    def __init__(
        self,
        database_name: str,
        settings: LokiSettings,
        binding: LokiBinding | None,
        *,
        transport: HttpTransport,
        tokens: EntraTokenProvider,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        page_size: int | None = None,
    ) -> None:
        self.database_name = database_name
        self._settings = settings
        self._binding = binding
        self._transport = transport
        self._tokens = tokens
        self._now = now
        self._page_size = page_size

    def gate(self, category: HistoryCategory, window: TimeWindow) -> Coverage | None:
        def gated(status: ResultStatus, reason: str, remediation: str | None = None) -> Coverage:
            return Coverage.gated(
                self.name, self.family, category, window, status, reason, remediation=remediation
            )

        key = CATEGORY_KEY_BY_HISTORY.get(category)
        if key not in LOKI_CATEGORIES:
            return gated(
                ResultStatus.NOT_SUPPORTED,
                f"The diagnostic log categories read from Loki hold no {category.value} records.",
            )
        settings = self._settings
        if settings.invalid is not None:
            return gated(
                ResultStatus.PRECONDITION,
                "The Loki history source is off: a setting is invalid.",
                f"{settings.invalid} {settings.invalid_reason} Fix it, then restart the MCP "
                "server.",
            )
        if not settings.enabled:
            return gated(
                ResultStatus.PRECONDITION,
                "The Loki history source is not configured.",
                _SETTINGS_REMEDIATION,
            )
        if self._binding is None:
            return gated(
                ResultStatus.PRECONDITION,
                "This database has no loki mapping in the local database policy.",
                _POLICY_SNIPPET,
            )
        if key not in self._binding.categories:
            return gated(
                ResultStatus.PRECONDITION,
                f"The {key} category is not declared for this database.",
                f"Confirm that the database's diagnostic setting exports {key} to Loki, then "
                f'add "{key}" to history_sources.loki.categories in {POLICY_FILE_HINT}.',
            )
        return None

    async def describe(self) -> SourceDescription:
        binding = self._binding
        if binding is None or not self._settings.enabled:
            return SourceDescription(self.name, self.family, self.database_name, False, ())
        now = self._now()
        watermark: datetime | None = None
        reason: str | None = None
        try:
            newest = await self._newest(binding, _to_ns(now - timedelta(days=1)), _to_ns(now))
            if newest is not None:
                watermark, _properties, _problem = record_from_line(newest.line, binding.identity)
            if watermark is None:
                reason = "No line for this database in the last 24 hours."
        except HistoryReadError as exc:
            reason = str(exc)
        return SourceDescription(
            self.name,
            self.family,
            self.database_name,
            True,
            tuple(
                history.value
                for history, key in CATEGORY_KEY_BY_HISTORY.items()
                if key in binding.categories
            ),
            retention_days=binding.retention_days,
            expected_ingest_lag_seconds=binding.ingest_lag_minutes * 60.0,
            watermark=watermark,
            watermark_reason=reason,
        )

    async def read(
        self,
        category: HistoryCategory,
        window: TimeWindow,
        filters: Mapping[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> SourceRead:
        gate = self.gate(category, window)
        binding = self._binding
        if gate is not None or binding is None:
            assert gate is not None
            return SourceRead([], gate)
        settings = self._settings
        key = CATEGORY_KEY_BY_HISTORY[category]
        now = self._now()
        stamp_delay = timedelta(minutes=settings.stamp_delay_minutes)
        start_ns = _to_ns(window.start - _STAMP_TOLERANCE)
        end_ns = _to_ns(min(now, window.end + stamp_delay + _STAMP_TOLERANCE))
        page_size = self._page_size or (200 if key in _SMALL_PAGE_CATEGORIES else 1000)
        newest_result, lines_result = await asyncio.gather(
            self._newest(binding, start_ns, _to_ns(now)),
            self._read_forward(
                build_query(settings, binding.identity, (key,)), start_ns, end_ns, page_size
            ),
            return_exceptions=True,
        )
        read_error = _history_error(lines_result, "history_loki.read")
        watermark_error = _history_error(newest_result, "history_loki.watermark")

        events: list[HistoryEvent] = []
        rows_read = unparsed = rejected = 0
        truncated: str | None = None
        if isinstance(lines_result, LokiRead):
            truncated = lines_result.truncated_reason
            rows_read = len(lines_result.entries)
            for entry in lines_result.entries:
                event, problem = self._event(key, entry.line, binding.identity)
                if event is not None:
                    events.append(event)
                elif problem == "identity":
                    rejected += 1
                else:
                    unparsed += 1
        watermark: datetime | None = None
        never_empty: list[str] = []
        if isinstance(newest_result, LokiEntry):
            watermark, _properties, _problem = record_from_line(
                newest_result.line, binding.identity
            )
            if watermark is None:
                never_empty.append(
                    "The newest line did not parse as a diagnostic record for this database."
                )
            else:
                never_empty.extend(
                    _stamp_delay_problem(_from_ns(newest_result.timestamp_ns) - watermark, stamp_delay)
                )
        return settle_read(
            source=self.name,
            family=self.family,
            category=category,
            window=window,
            now=now,
            events=events,
            rows_read=rows_read,
            rows_unparsed=unparsed,
            rows_rejected_identity=rejected,
            watermark=watermark,
            ingest_lag=timedelta(minutes=binding.ingest_lag_minutes),
            retention_floor=(
                None if binding.retention_days is None
                else now - timedelta(days=binding.retention_days)
            ),
            truncated_reason=truncated,
            read_through=max((event.time for event in events), default=None)
            if truncated else None,
            never_empty_reasons=never_empty,
            read_error=read_error,
            watermark_error=watermark_error,
            schema_basis=SchemaBasis.DERIVED,
            warnings=list(never_empty),
        )

    def _event(
        self, key: str, line: str, identity: DatabaseIdentity
    ) -> tuple[HistoryEvent | None, str | None]:
        record_time, properties, problem = record_from_line(line, identity)
        if record_time is None:
            return None, problem
        by_name = {str(name).casefold(): value for name, value in properties.items()}

        def get(column: str) -> Any:
            return by_name.get(export_property_name(column).casefold(), by_name.get(column.casefold()))

        event = diagnostic_event(
            key, get, time=record_time, database=self.database_name, source=self.name
        )
        return event, None if event is not None else "unparsed"

    async def _newest(
        self, binding: LokiBinding, start_ns: int, end_ns: int
    ) -> LokiEntry | None:
        """The newest line of the database in its declared categories."""

        query = build_query(self._settings, binding.identity, binding.categories)
        page = await self._page(query, start_ns, end_ns, 1, "backward", MAX_RESPONSE_BYTES)
        if page is None:
            raise HistoryReadError("too_large", "A Loki response was larger than the size cap.")
        return page[0][-1] if page[0] else None

    async def _read_forward(
        self, query: str, start_ns: int, end_ns: int, page_size: int
    ) -> LokiRead:
        """Read the range oldest first, within the line, byte and page caps.

        Each page starts at the newest timestamp of the previous one, so lines
        that share a timestamp across the boundary are read again and dropped
        as repeats.
        """

        max_lines = self._settings.max_lines
        entries: list[LokiEntry] = []
        seen: set[tuple[int, bytes]] = set()
        cursor = start_ns
        bytes_left = MAX_RESPONSE_BYTES
        for _ in range(MAX_PAGES):
            if len(entries) >= max_lines:
                return LokiRead(tuple(entries), "max_lines")
            page = await self._page(query, cursor, end_ns, page_size, "forward", bytes_left)
            if page is None:
                return LokiRead(tuple(entries), "max_bytes")
            page_entries, size = page
            bytes_left -= size
            fresh = [entry for entry in page_entries if _entry_key(entry) not in seen]
            room = max_lines - len(entries)
            if len(fresh) > room:
                entries.extend(fresh[:room])
                return LokiRead(tuple(entries), "max_lines")
            entries.extend(fresh)
            seen.update(_entry_key(entry) for entry in fresh)
            if len(page_entries) < page_size:
                return LokiRead(tuple(entries), None)
            if not fresh:
                return LokiRead(tuple(entries), "equal_timestamps")
            cursor = page_entries[-1].timestamp_ns
        return LokiRead(tuple(entries), "max_pages")

    async def _page(
        self, query: str, start_ns: int, end_ns: int, limit: int, direction: str, max_bytes: int
    ) -> tuple[list[LokiEntry], int] | None:
        """One query_range page sorted oldest first, or None when it exceeds ``max_bytes``."""

        settings = self._settings
        assert settings.grafana_url is not None and settings.datasource_uid is not None
        response = await send_authorized(
            self._transport,
            self._tokens,
            HttpRequest(
                "GET",
                settings.grafana_url + _QUERY_RANGE_PATH.format(uid=settings.datasource_uid),
                params=(
                    ("query", query),
                    ("start", str(start_ns)),
                    ("end", str(end_ns)),
                    ("limit", str(limit)),
                    ("direction", direction),
                ),
                timeout=float(settings.timeout_seconds),
                max_bytes=max(1, max_bytes),
            ),
        )
        raise_for_status(response, service="Grafana", hints=_HINTS)
        if response.truncated:
            return None
        entries = parse_streams(decode_json(response, service="Grafana"))
        page = entries[:limit] if direction == "forward" else entries[-limit:]
        return page, len(response.body)


def _stamp_delay_problem(measured: timedelta, configured: timedelta) -> list[str]:
    """Lines stamped outside the read allowance can be missed: never report empty then."""

    if -_STAMP_TOLERANCE <= measured <= configured + _STAMP_TOLERANCE:
        return []
    minutes = round(abs(measured).total_seconds() / 60, 1)
    if measured < timedelta(0):
        return [
            f"The newest line's Loki timestamp is {minutes} min before its record time; "
            "lines near the window start can be missed."
        ]
    return [
        f"The newest line reached Loki {minutes} min after its record time, longer than "
        f"{ENV_STAMP_DELAY_MINUTES} ({int(configured.total_seconds() // 60)}); lines near the "
        "window end can be missed. Raise the setting above the measured delay."
    ]


def _history_error(result: Any, site: str) -> HistoryReadError | None:
    if isinstance(result, HistoryReadError):
        note_exception(result, site)
        return result
    if isinstance(result, BaseException):
        raise result
    return None
