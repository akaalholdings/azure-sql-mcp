"""History from Azure Monitor: Log Analytics diagnostics, audit, and platform metrics.

Two sources:

- ``LogAnalyticsHistorySource`` reads a Log Analytics workspace with KQL through
  the Logs query API. Microsoft Sentinel keeps its data in a Log Analytics
  workspace, so the same source reads a Sentinel workspace: map its workspace id.
  It reads the ``AzureDiagnostics`` table (the Azure SQL Database categories
  Blocks, Timeouts, Deadlocks, Errors, DatabaseWaitStatistics, and the audit
  category SQLSecurityAuditEvents) and the ``AzureMetrics`` table.
- ``AzureMonitorMetricsSource`` reads platform metrics from the Azure Monitor
  ``Metrics - List`` REST API. It needs no diagnostic setting.

Every table, column and metric name here is taken from Microsoft Learn. Azure
SQL Database sends all its resource log categories to ``AzureDiagnostics``; it
has no resource-specific tables, and the ``SQLSecurityAuditEvents`` table is
documented for Azure Synapse only. The KQL is built from that documentation and
has not been run against a workspace.

KQL is built here and only here: constants, the validated identity from the
local database policy, and integers. No caller text reaches a query except
metric names, which must be in the documented list.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from urllib.parse import quote

from .history_sources import DEFAULT_LIMIT
from .history_sources import MAX_LIMIT
from .history_sources import AuditRecord
from .history_sources import Coverage
from .history_sources import DatabaseIdentity
from .history_sources import EntraTokenProvider
from .history_sources import ErrorEvent
from .history_sources import HistoryCategory
from .history_sources import HistoryEvent
from .history_sources import HistoryReadError
from .history_sources import HttpRequest
from .history_sources import HttpTransport
from .history_sources import ResourceMetricSample
from .history_sources import SchemaBasis
from .history_sources import SourceDescription
from .history_sources import SourceRead
from .history_sources import TimeoutEvent
from .history_sources import TimeWindow
from .history_sources import WaitStatsDelta
from .history_sources import clean_text
from .history_sources import deadlock_event
from .history_sources import decode_json
from .history_sources import normalize_query_hash
from .history_sources import parse_blocked_process_report
from .history_sources import parse_utc
from .history_sources import raise_for_status
from .history_sources import send_authorized
from .history_sources import settle_read
from .history_sources import to_bool
from .history_sources import to_float
from .history_sources import to_int
from .incident_log import note_exception
from .result_status import ResultStatus
from .time_windows import iso_z

LOG_ANALYTICS_SOURCE = "log_analytics"
DIAGNOSTICS_FAMILY = "azure_diagnostics"
METRICS_SOURCE = "azure_monitor_metrics"
METRICS_FAMILY = "azure_metrics"
LOG_ANALYTICS_ENDPOINT = "https://api.loganalytics.azure.com"
LOG_ANALYTICS_SCOPE = "https://api.loganalytics.io/.default"
ARM_ENDPOINT = "https://management.azure.com"
ARM_SCOPE = "https://management.azure.com/.default"
METRICS_API_VERSION = "2024-02-01"
# The policy name for "the diagnostic setting also sends metrics to the workspace".
METRICS_CATEGORY = "metrics"
POLICY_FILE_HINT = "the local database policy (AZURE_SQL_DATABASE_POLICY_FILE)"

_GUID = re.compile(r"[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}")
_DATABASE_RESOURCE_ID = re.compile(
    r"/subscriptions/[0-9a-fA-F-]{36}/resourceGroups/[^/\s]{1,90}"
    r"/providers/Microsoft\.Sql/servers/[A-Za-z0-9-]{1,63}/databases/[^/]{1,128}",
    re.IGNORECASE,
)
_TYPE_SUFFIX = re.compile(r"_[sdbtg]$")


@dataclass(frozen=True)
class DiagnosticCategory:
    """One Azure SQL Database resource log category in ``AzureDiagnostics``."""

    name: str  # the Category value
    history: HistoryCategory
    columns: tuple[str, ...]
    # A record without this column's value is not a record of this category.
    required: str | None = None
    server_column: str = "LogicalServerName_s"
    database_column: str = "DatabaseName_s"


# Column names: "Configure streaming export of diagnostic telemetry" and "SQL
# Database audit log format" on Microsoft Learn.
DIAGNOSTIC_CATEGORIES: Mapping[str, DiagnosticCategory] = {
    "blocks": DiagnosticCategory(
        "Blocks",
        HistoryCategory.BLOCKING,
        ("lock_mode_s", "resource_owner_type_s", "blocked_process_filtered_s", "duration_d"),
        required="blocked_process_filtered_s",
    ),
    "timeouts": DiagnosticCategory(
        "Timeouts",
        HistoryCategory.TIMEOUT,
        ("error_state_d", "query_hash_s", "query_plan_hash_s"),
    ),
    "deadlocks": DiagnosticCategory(
        "Deadlocks", HistoryCategory.DEADLOCK, ("deadlock_xml_s",), required="deadlock_xml_s"
    ),
    "errors": DiagnosticCategory(
        "Errors",
        HistoryCategory.ERROR,
        (
            "Message", "user_defined_b", "error_number_d", "Severity", "state_d",
            "query_hash_s", "query_plan_hash_s",
        ),
        required="error_number_d",
    ),
    "databasewaitstatistics": DiagnosticCategory(
        "DatabaseWaitStatistics",
        HistoryCategory.WAIT_STATS,
        (
            "wait_type_s", "start_utc_date_t", "end_utc_date_t", "delta_max_wait_time_ms_d",
            "delta_signal_wait_time_ms_d", "delta_wait_time_ms_d", "delta_waiting_tasks_count_d",
        ),
        required="wait_type_s",
    ),
    "sqlsecurityauditevents": DiagnosticCategory(
        "SQLSecurityAuditEvents",
        HistoryCategory.AUDIT_STATEMENT,
        (
            "event_time_t", "statement_s", "action_name_s", "succeeded_s",
            "server_principal_name_s", "database_principal_name_s", "application_name_s",
            "session_id_d", "duration_milliseconds_d", "affected_rows_d", "response_rows_d",
            "object_name_s", "schema_name_s",
        ),
        required="action_name_s",
        server_column="server_instance_name_s",
        database_column="database_name_s",
    ),
}
CATEGORY_KEY_BY_HISTORY: Mapping[HistoryCategory, str] = {
    **{spec.history: key for key, spec in DIAGNOSTIC_CATEGORIES.items()},
    HistoryCategory.RESOURCE_METRIC: METRICS_CATEGORY,
}
DECLARABLE_CATEGORIES = frozenset(DIAGNOSTIC_CATEGORIES) | {METRICS_CATEGORY}

# "Supported metrics for Microsoft.Sql/servers/databases" on Microsoft Learn,
# without the data warehouse metrics.
SQL_DATABASE_METRICS = frozenset(
    {
        "allocated_data_storage", "app_cpu_billed", "app_cpu_billed_ha_replicas",
        "app_cpu_percent", "app_memory_percent", "blocked_by_firewall", "connection_failed",
        "connection_failed_user_error", "connection_successful", "cpu_limit", "cpu_percent",
        "cpu_used", "deadlock", "dtu_consumption_percent", "dtu_limit", "dtu_used",
        "log_write_percent", "physical_data_read_percent", "sessions_count",
        "sessions_percent", "sql_instance_cpu_percent", "sql_instance_memory_percent",
        "storage", "storage_percent", "tempdb_data_size", "tempdb_log_size",
        "tempdb_log_used_percent", "workers_percent", "xtp_storage_percent",
    }
)
# Percent metrics that take the Average, Maximum and Minimum aggregations.
DEFAULT_METRICS = (
    "cpu_percent", "physical_data_read_percent", "log_write_percent", "workers_percent",
    "sessions_percent",
)


def export_property_name(column: str) -> str:
    """The resource log property behind an ``AzureDiagnostics`` column.

    Azure Monitor names a column after the property and adds a type suffix
    (``NewInfo1`` becomes ``NewInfo1_s``). Without the suffix the name is the
    property as it is in ``AdditionalFields`` and in an exported record.
    """

    return _TYPE_SUFFIX.sub("", column)


def diagnostic_event(
    key: str,
    get: Callable[[str], Any],
    *,
    time: datetime,
    database: str,
    source: str,
) -> HistoryEvent | None:
    """One diagnostic record as an event, or None when it is not recognized.

    ``get`` returns the value for an ``AzureDiagnostics`` column name. The Loki
    adapter passes a reader of the exported record's properties.
    """

    spec = DIAGNOSTIC_CATEGORIES[key]
    if spec.required is not None and get(spec.required) in (None, ""):
        return None
    if key == "blocks":
        duration = to_float(get("duration_d"))
        return parse_blocked_process_report(
            str(get("blocked_process_filtered_s")),
            time=time,
            database=database,
            source=source,
            duration_ms=None if duration is None else duration / 1000,  # microseconds
            lock_mode=clean_text(get("lock_mode_s")),
            resource_owner_type=clean_text(get("resource_owner_type_s")),
        )
    if key == "timeouts":
        return TimeoutEvent(
            time=time,
            database=database,
            source=source,
            query_hash=normalize_query_hash(get("query_hash_s")),
            query_plan_hash=normalize_query_hash(get("query_plan_hash_s")),
            error_state=to_int(get("error_state_d")),
        )
    if key == "deadlocks":
        return deadlock_event(
            str(get("deadlock_xml_s")), time=time, database=database, source=source
        )
    if key == "errors":
        number = to_int(get("error_number_d"))
        if number is None:
            return None
        return ErrorEvent(
            time=time,
            database=database,
            source=source,
            error_number=number,
            severity=to_int(get("Severity")),
            state=to_int(get("state_d")),
            user_defined=to_bool(get("user_defined_b")),
            message=clean_text(get("Message")),
            query_hash=normalize_query_hash(get("query_hash_s")),
            query_plan_hash=normalize_query_hash(get("query_plan_hash_s")),
        )
    if key == "databasewaitstatistics":
        return WaitStatsDelta(
            time=time,
            database=database,
            source=source,
            wait_type=str(get("wait_type_s")),
            interval_start=parse_utc(get("start_utc_date_t")),
            interval_end=parse_utc(get("end_utc_date_t")),
            wait_time_ms=to_float(get("delta_wait_time_ms_d")),
            signal_wait_time_ms=to_float(get("delta_signal_wait_time_ms_d")),
            waiting_tasks_count=to_float(get("delta_waiting_tasks_count_d")),
            max_wait_time_ms=to_float(get("delta_max_wait_time_ms_d")),
        )
    return AuditRecord(
        time=time,
        database=database,
        source=source,
        action_name=clean_text(get("action_name_s")),
        succeeded=to_bool(get("succeeded_s")),
        server_principal_name=clean_text(get("server_principal_name_s")),
        database_principal_name=clean_text(get("database_principal_name_s")),
        application_name=clean_text(get("application_name_s")),
        session_id=to_int(get("session_id_d")),
        duration_ms=to_float(get("duration_milliseconds_d")),
        affected_rows=to_int(get("affected_rows_d")),
        response_rows=to_int(get("response_rows_d")),
        schema_name=clean_text(get("schema_name_s")),
        object_name=clean_text(get("object_name_s")),
        statement_text=clean_text(get("statement_s")),
    )


# --- KQL -------------------------------------------------------------------------


def kql_string(value: str) -> str:
    """A double-quoted KQL string literal that decodes to ``value``."""

    if any(not character.isprintable() for character in value):
        raise ValueError("A KQL string literal must not hold control characters.")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def kql_datetime(value: datetime) -> str:
    return f"datetime({iso_z(value)})"


def _identity_lines(spec: DiagnosticCategory, identity: DatabaseIdentity) -> list[str]:
    # column_ifexists: a workspace that never received this category has no such
    # column, and a bare column name would fail the whole query.
    return [
        f"| extend mcp_server = tostring(column_ifexists({kql_string(spec.server_column)}, "
        f'"")), mcp_database = tostring(column_ifexists({kql_string(spec.database_column)}, ""))',
        f"| where mcp_server =~ {kql_string(identity.logical_server)} and mcp_database =~ "
        f"{kql_string(identity.database)}",
    ]


def build_diagnostics_kql(
    key: str, identity: DatabaseIdentity, window: TimeWindow, *, limit: int
) -> str:
    """KQL for one database's records of one category in the window, oldest first."""

    spec = DIAGNOSTIC_CATEGORIES[key]
    # tostring: every field arrives as text whatever type the workspace gave the column.
    projected = ", ".join(
        f'{column} = tostring(column_ifexists({kql_string(column)}, ""))'
        for column in spec.columns
    )
    return "\n".join(
        [
            "AzureDiagnostics",
            f"| where TimeGenerated >= {kql_datetime(window.start)} and TimeGenerated < "
            f"{kql_datetime(window.end)}",
            f"| where Category == {kql_string(spec.name)}",
            *_identity_lines(spec, identity),
            f"| project TimeGenerated, mcp_server, mcp_database, {projected}, "
            'AdditionalFields = column_ifexists("AdditionalFields", dynamic(null))',
            f"| top {int(limit) + 1} by TimeGenerated asc",
        ]
    )


def build_watermark_kql(
    keys: Sequence[str], identity: DatabaseIdentity, *, since: datetime
) -> str:
    """KQL for the newest record of the database in the given categories."""

    specs = [DIAGNOSTIC_CATEGORIES[key] for key in keys]
    names = ", ".join(kql_string(spec.name) for spec in specs)
    return "\n".join(
        [
            "AzureDiagnostics",
            f"| where TimeGenerated >= {kql_datetime(since)}",
            f"| where Category in ({names})",
            *_identity_lines(specs[0], identity),
            "| summarize watermark = max(TimeGenerated)",
        ]
    )


def _metrics_scope_lines(
    identity: DatabaseIdentity, metric_names: Sequence[str]
) -> list[str]:
    suffix = f"/servers/{identity.logical_server}/databases/{identity.database}"
    lines = [f"| where _ResourceId endswith {kql_string(suffix)}"]
    if metric_names:
        names = ", ".join(kql_string(name) for name in metric_names)
        lines.append(f"| where MetricName in~ ({names})")
    return lines


def build_metrics_kql(
    identity: DatabaseIdentity,
    window: TimeWindow,
    *,
    limit: int,
    metric_names: Sequence[str] = (),
) -> str:
    return "\n".join(
        [
            "AzureMetrics",
            f"| where TimeGenerated >= {kql_datetime(window.start)} and TimeGenerated < "
            f"{kql_datetime(window.end)}",
            *_metrics_scope_lines(identity, metric_names),
            "| project TimeGenerated, MetricName, Average, Minimum, Maximum, Total, Count, "
            "TimeGrain, UnitName",
            f"| top {int(limit) + 1} by TimeGenerated asc",
        ]
    )


def build_metrics_watermark_kql(
    identity: DatabaseIdentity, *, since: datetime, metric_names: Sequence[str] = ()
) -> str:
    # The same metric filter as the read: a filter that matches no stored name
    # then gives no watermark, so the read is "unavailable", not "empty".
    return "\n".join(
        [
            "AzureMetrics",
            f"| where TimeGenerated >= {kql_datetime(since)}",
            *_metrics_scope_lines(identity, metric_names),
            "| summarize watermark = max(TimeGenerated)",
        ]
    )


def rows_from_tables(document: Any) -> tuple[list[dict[str, Any]], bool]:
    """Rows of a Logs query API response as dicts, and whether the result is partial."""

    tables = document.get("tables") if isinstance(document, dict) else None
    if not isinstance(tables, list) or not tables or not isinstance(tables[0], dict):
        raise HistoryReadError(
            "protocol", "Log Analytics returned a response that is not a query result."
        )
    columns = tables[0].get("columns")
    rows = tables[0].get("rows")
    if not isinstance(columns, list) or not isinstance(rows, list):
        raise HistoryReadError(
            "protocol", "Log Analytics returned a response that is not a query result."
        )
    names = [
        str(column.get("name")) if isinstance(column, dict) else "" for column in columns
    ]
    parsed: list[dict[str, Any]] = [
        dict(zip(names, row, strict=False)) for row in rows if isinstance(row, list)
    ]
    # A non-fatal error comes back as 200 with an "error" object: some data is missing.
    return parsed, document.get("error") is not None


# --- Log Analytics ---------------------------------------------------------------


@dataclass(frozen=True)
class LogAnalyticsBinding:
    """The ``log_analytics`` policy entry of one database."""

    workspace_id: str
    identity: DatabaseIdentity
    # What the database's diagnostic setting sends to the workspace. ``empty`` is
    # possible only for a category listed here.
    categories: tuple[str, ...]
    ingest_lag_minutes: int = 15
    retention_days: int | None = None

    @classmethod
    def from_mapping(cls, database_name: str, value: Any) -> "LogAnalyticsBinding":
        where = f"log_analytics for database {database_name!r}"
        fields = _exact_fields(
            value,
            where,
            required={"workspace_id", "logical_server", "database", "categories"},
            optional={"ingest_lag_minutes", "retention_days"},
        )
        workspace_id = fields["workspace_id"]
        if not isinstance(workspace_id, str) or not _GUID.fullmatch(workspace_id):
            raise ValueError(f"{where}: workspace_id must be the workspace GUID.")
        return cls(
            workspace_id=workspace_id.lower(),
            identity=_identity(fields, where),
            categories=_categories(fields["categories"], DECLARABLE_CATEGORIES, where),
            ingest_lag_minutes=_bounded(fields.get("ingest_lag_minutes", 15), 0, 1440, where,
                                        "ingest_lag_minutes"),
            retention_days=_optional_bounded(fields.get("retention_days"), 1, 4383, where,
                                             "retention_days"),
        )


_LOG_ANALYTICS_POLICY_SNIPPET = (
    f"Add a log_analytics entry to this database's history_sources in {POLICY_FILE_HINT}:\n"
    '"log_analytics": {"workspace_id": "<workspace GUID>", "logical_server": "<short server '
    'name>", "database": "<database name>", "categories": ["blocks", "timeouts", "deadlocks", '
    '"errors", "databasewaitstatistics"]}\n'
    "List only the categories that the database's diagnostic setting sends to the workspace. "
    'Add "metrics" when the setting sends metrics, and "sqlsecurityauditevents" when auditing '
    "writes to the workspace."
)
_LOG_ANALYTICS_HINTS = {
    401: "Check the identity the server signs in with.",
    403: "The identity needs the Log Analytics Reader role on the workspace.",
    404: "Check log_analytics.workspace_id in the local database policy.",
}


class LogAnalyticsHistorySource:
    """Azure SQL diagnostic logs, audit records and metrics in a Log Analytics workspace."""

    name = LOG_ANALYTICS_SOURCE
    family = DIAGNOSTICS_FAMILY

    def __init__(
        self,
        database_name: str,
        binding: LogAnalyticsBinding | None,
        *,
        transport: HttpTransport,
        tokens: EntraTokenProvider,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        endpoint: str = LOG_ANALYTICS_ENDPOINT,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.database_name = database_name
        self._binding = binding
        self._transport = transport
        self._tokens = tokens
        self._now = now
        self._endpoint = endpoint.rstrip("/")
        self._timeout = timeout_seconds

    def gate(self, category: HistoryCategory, window: TimeWindow) -> Coverage | None:
        key = CATEGORY_KEY_BY_HISTORY.get(category)
        if key is None:
            return Coverage.gated(
                self.name, self.family, category, window, ResultStatus.NOT_SUPPORTED,
                f"Azure SQL Database diagnostic logs hold no {category.value} records.",
            )
        if self._binding is None:
            return Coverage.gated(
                self.name, self.family, category, window, ResultStatus.PRECONDITION,
                "No Log Analytics workspace is mapped for this database.",
                remediation=_LOG_ANALYTICS_POLICY_SNIPPET,
            )
        if key not in self._binding.categories:
            label = "metrics" if key == METRICS_CATEGORY else DIAGNOSTIC_CATEGORIES[key].name
            return Coverage.gated(
                self.name, self.family, category, window, ResultStatus.PRECONDITION,
                f"The {label} category is not declared for this database, so the workspace "
                "is not known to receive it.",
                remediation=(
                    f"Confirm that the database's diagnostic setting sends {label} to the Log "
                    f'Analytics workspace, then add "{key}" to history_sources.log_analytics.'
                    f"categories in {POLICY_FILE_HINT}."
                ),
            )
        return None

    async def describe(self) -> SourceDescription:
        binding = self._binding
        if binding is None:
            return SourceDescription(self.name, self.family, self.database_name, False, ())
        now = self._now()
        keys = _diagnostic_keys(binding.categories)
        watermark: datetime | None = None
        reason: str | None = None
        if keys:
            try:
                watermark = await self._watermark(
                    build_watermark_kql(keys, binding.identity, since=now - timedelta(days=1)),
                    now - timedelta(days=1),
                    now,
                )
                if watermark is None:
                    reason = "No record for this database in the last 24 hours."
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
        limit = max(1, min(int(limit), MAX_LIMIT))
        key = CATEGORY_KEY_BY_HISTORY[category]
        now = self._now()
        if key == METRICS_CATEGORY:
            names = _metric_names(filters, default=())
            query = build_metrics_kql(binding.identity, window, limit=limit, metric_names=names)
            watermark_query = build_metrics_watermark_kql(
                binding.identity, since=window.start, metric_names=names
            )
        else:
            spec = DIAGNOSTIC_CATEGORIES[key]
            query = build_diagnostics_kql(key, binding.identity, window, limit=limit)
            same_identity = [
                other
                for other in _diagnostic_keys(binding.categories)
                if DIAGNOSTIC_CATEGORIES[other].server_column == spec.server_column
            ]
            watermark_query = build_watermark_kql(
                same_identity, binding.identity, since=window.start
            )
        rows_result, watermark_result = await asyncio.gather(
            self._rows(query, window.start, window.end),
            self._watermark(watermark_query, window.start, now),
            return_exceptions=True,
        )

        read_error = _history_error(rows_result, "history_azure_monitor.read")
        watermark_error = _history_error(watermark_result, "history_azure_monitor.watermark")
        events: list[HistoryEvent] = []
        rows_read = unparsed = rejected = 0
        truncated: str | None = None
        if not isinstance(rows_result, BaseException):
            rows, partial = rows_result
            if partial:
                truncated = "partial_result"
            if len(rows) > limit:
                rows, truncated = rows[:limit], truncated or "max_rows"
            rows_read = len(rows)
            for row in rows:
                event, problem = self._event(key, row, binding.identity)
                if event is not None:
                    events.append(event)
                elif problem == "identity":
                    rejected += 1
                else:
                    unparsed += 1
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
            watermark=None if isinstance(watermark_result, BaseException) else watermark_result,
            ingest_lag=timedelta(minutes=binding.ingest_lag_minutes),
            retention_floor=(
                None if binding.retention_days is None
                else now - timedelta(days=binding.retention_days)
            ),
            truncated_reason=truncated,
            read_through=events[-1].time if truncated and events else None,
            read_error=read_error,
            watermark_error=watermark_error,
            schema_basis=SchemaBasis.DOCUMENTED,
        )

    def _event(
        self, key: str, row: Mapping[str, Any], identity: DatabaseIdentity
    ) -> tuple[HistoryEvent | None, str | None]:
        time = parse_utc(row.get("TimeGenerated"))
        if time is None:
            return None, "unparsed"
        if key == METRICS_CATEGORY:
            metric = clean_text(row.get("MetricName"))
            if metric is None:
                return None, "unparsed"
            return (
                ResourceMetricSample(
                    time=time,
                    database=self.database_name,
                    source=self.name,
                    metric=metric,
                    grain=clean_text(row.get("TimeGrain")),
                    unit=clean_text(row.get("UnitName")),
                    average=to_float(row.get("Average")),
                    minimum=to_float(row.get("Minimum")),
                    maximum=to_float(row.get("Maximum")),
                    total=to_float(row.get("Total")),
                    count=to_float(row.get("Count")),
                ),
                None,
            )
        # The query filters by identity; the check is repeated on what came back.
        if (
            str(row.get("mcp_server") or "").casefold() != identity.logical_server.casefold()
            or str(row.get("mcp_database") or "").casefold() != identity.database.casefold()
        ):
            return None, "identity"
        additional = _additional_fields(row.get("AdditionalFields"))

        def get(column: str) -> Any:
            value = row.get(column)
            if value in (None, ""):
                # Past 500 columns Azure Monitor keeps new fields in AdditionalFields.
                return additional.get(export_property_name(column))
            return value

        event = diagnostic_event(
            key, get, time=time, database=self.database_name, source=self.name
        )
        return event, None if event is not None else "unparsed"

    async def _rows(
        self, query: str, start: datetime, end: datetime
    ) -> tuple[list[dict[str, Any]], bool]:
        binding = self._binding
        assert binding is not None
        response = await send_authorized(
            self._transport,
            self._tokens,
            HttpRequest(
                "POST",
                f"{self._endpoint}/v1/workspaces/{binding.workspace_id}/query",
                json_body={"query": query, "timespan": f"{iso_z(start)}/{iso_z(end)}"},
                timeout=self._timeout,
            ),
        )
        raise_for_status(response, service="Log Analytics", hints=_LOG_ANALYTICS_HINTS)
        return rows_from_tables(decode_json(response, service="Log Analytics"))

    async def _watermark(self, query: str, start: datetime, end: datetime) -> datetime | None:
        rows, _partial = await self._rows(query, start, end)
        return parse_utc(rows[0].get("watermark")) if rows else None


def _additional_fields(value: Any) -> Mapping[str, Any]:
    if isinstance(value, str) and value.strip():
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, Mapping) else {}


def _diagnostic_keys(categories: Sequence[str]) -> list[str]:
    return [key for key in categories if key in DIAGNOSTIC_CATEGORIES]


def _history_error(result: Any, site: str) -> HistoryReadError | None:
    if isinstance(result, HistoryReadError):
        note_exception(result, site)
        return result
    if isinstance(result, BaseException):
        raise result
    return None


# --- Azure Monitor metrics REST API ---------------------------------------------


@dataclass(frozen=True)
class AzureMetricsBinding:
    """The ``azure_monitor_metrics`` policy entry of one database."""

    resource_id: str
    ingest_lag_minutes: int = 5
    # Azure Monitor keeps platform metrics for 93 days.
    retention_days: int = 93

    @classmethod
    def from_mapping(cls, database_name: str, value: Any) -> "AzureMetricsBinding":
        where = f"azure_monitor_metrics for database {database_name!r}"
        fields = _exact_fields(
            value, where, required={"resource_id"}, optional={"ingest_lag_minutes"}
        )
        resource_id = fields["resource_id"]
        if not isinstance(resource_id, str) or not _DATABASE_RESOURCE_ID.fullmatch(resource_id):
            raise ValueError(
                f"{where}: resource_id must be the database's Azure resource id: "
                "/subscriptions/<id>/resourceGroups/<group>/providers/Microsoft.Sql/servers/"
                "<server>/databases/<database>."
            )
        return cls(
            resource_id=resource_id,
            ingest_lag_minutes=_bounded(fields.get("ingest_lag_minutes", 5), 0, 1440, where,
                                        "ingest_lag_minutes"),
        )


_METRICS_POLICY_SNIPPET = (
    f"Add an azure_monitor_metrics entry to this database's history_sources in "
    f"{POLICY_FILE_HINT}:\n"
    '"azure_monitor_metrics": {"resource_id": "/subscriptions/<id>/resourceGroups/<group>/'
    'providers/Microsoft.Sql/servers/<server>/databases/<database>"}\n'
    "The identity needs the Monitoring Reader role on the database."
)
_METRICS_HINTS = {
    403: "The identity needs the Monitoring Reader role on the database.",
    404: "Check azure_monitor_metrics.resource_id in the local database policy.",
}


def parse_metrics_response(
    document: Any, *, database: str, source: str
) -> tuple[list[ResourceMetricSample], list[str]]:
    """Samples of a ``Metrics - List`` response, and the metrics the service refused.

    A point without any aggregation value is an empty time slot, not a sample.
    """

    values = document.get("value") if isinstance(document, dict) else None
    if not isinstance(values, list):
        raise HistoryReadError(
            "protocol", "Azure Monitor returned a response that is not a metrics result."
        )
    grain = clean_text(document.get("interval"))
    samples: list[ResourceMetricSample] = []
    errors: list[str] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        name_object = item.get("name")
        name = clean_text(name_object.get("value")) if isinstance(name_object, dict) else None
        if name is None:
            continue
        code = item.get("errorCode")
        if isinstance(code, str) and code != "Success":
            errors.append(f"{name}: {code}")
            continue
        for series in item.get("timeseries") or []:
            for point in (series.get("data") if isinstance(series, dict) else None) or []:
                if not isinstance(point, dict):
                    continue
                time = parse_utc(point.get("timeStamp"))
                sample = ResourceMetricSample(
                    time=time or datetime.min.replace(tzinfo=timezone.utc),
                    database=database,
                    source=source,
                    metric=name,
                    grain=grain,
                    unit=clean_text(item.get("unit")),
                    average=to_float(point.get("average")),
                    minimum=to_float(point.get("minimum")),
                    maximum=to_float(point.get("maximum")),
                    total=to_float(point.get("total")),
                    count=to_float(point.get("count")),
                )
                has_value = any(
                    value is not None
                    for value in (
                        sample.average, sample.minimum, sample.maximum, sample.total, sample.count
                    )
                )
                if time is not None and has_value:
                    samples.append(sample)
    samples.sort(key=lambda sample: (sample.metric, sample.time))
    return samples, errors


class AzureMonitorMetricsSource:
    """Platform metrics of one database from the Azure Monitor metrics REST API."""

    name = METRICS_SOURCE
    family = METRICS_FAMILY

    def __init__(
        self,
        database_name: str,
        binding: AzureMetricsBinding | None,
        *,
        transport: HttpTransport,
        tokens: EntraTokenProvider,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        endpoint: str = ARM_ENDPOINT,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.database_name = database_name
        self._binding = binding
        self._transport = transport
        self._tokens = tokens
        self._now = now
        self._endpoint = endpoint.rstrip("/")
        self._timeout = timeout_seconds

    def gate(self, category: HistoryCategory, window: TimeWindow) -> Coverage | None:
        if category is not HistoryCategory.RESOURCE_METRIC:
            return Coverage.gated(
                self.name, self.family, category, window, ResultStatus.NOT_SUPPORTED,
                f"Azure Monitor platform metrics hold no {category.value} records.",
            )
        if self._binding is None:
            return Coverage.gated(
                self.name, self.family, category, window, ResultStatus.PRECONDITION,
                "The database's Azure resource id is not mapped, so its platform metrics "
                "cannot be read.",
                remediation=_METRICS_POLICY_SNIPPET,
            )
        return None

    async def describe(self) -> SourceDescription:
        binding = self._binding
        if binding is None:
            return SourceDescription(self.name, self.family, self.database_name, False, ())
        watermark: datetime | None = None
        reason: str | None = None
        try:
            watermark = await self._watermark(self._now())
            if watermark is None:
                reason = "No metric value for this database in the last 6 hours."
        except HistoryReadError as exc:
            reason = str(exc)
        return SourceDescription(
            self.name,
            self.family,
            self.database_name,
            True,
            (HistoryCategory.RESOURCE_METRIC.value,),
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
        names = _metric_names(filters, default=DEFAULT_METRICS, documented_only=True)
        now = self._now()
        minutes = (window.end - window.start).total_seconds() / 60
        interval = "PT1M" if minutes <= 180 else "PT5M" if minutes <= 1440 else "PT1H"
        samples_result, watermark_result = await asyncio.gather(
            self._list(names, window.start, window.end, interval),
            self._watermark(now),
            return_exceptions=True,
        )
        read_error = _history_error(samples_result, "history_azure_monitor.metrics")
        watermark_error = _history_error(
            watermark_result, "history_azure_monitor.metrics_watermark"
        )
        samples: list[ResourceMetricSample] = []
        refused: list[str] = []
        if not isinstance(samples_result, BaseException):
            samples, refused = samples_result
        return settle_read(
            source=self.name,
            family=self.family,
            category=category,
            window=window,
            now=now,
            events=samples,
            rows_read=len(samples),
            watermark=None if isinstance(watermark_result, BaseException) else watermark_result,
            ingest_lag=timedelta(minutes=binding.ingest_lag_minutes),
            retention_floor=now - timedelta(days=binding.retention_days),
            never_empty_reasons=[
                "Azure Monitor refused these metrics (" + "; ".join(refused) + ")."
            ] if refused else [],
            read_error=read_error,
            watermark_error=watermark_error,
            schema_basis=SchemaBasis.DOCUMENTED,
            warnings=["Refused metrics: " + "; ".join(refused) + "."] if refused else [],
        )

    async def _list(
        self, names: Sequence[str], start: datetime, end: datetime, interval: str
    ) -> tuple[list[ResourceMetricSample], list[str]]:
        binding = self._binding
        assert binding is not None
        response = await send_authorized(
            self._transport,
            self._tokens,
            HttpRequest(
                "GET",
                self._endpoint
                + quote(binding.resource_id, safe="/")
                + "/providers/Microsoft.Insights/metrics",
                params=(
                    ("api-version", METRICS_API_VERSION),
                    ("metricnames", ",".join(names)),
                    ("timespan", f"{iso_z(start)}/{iso_z(end)}"),
                    ("interval", interval),
                    ("aggregation", "average,maximum,minimum"),
                    # The service widens a grain it does not keep for the metric.
                    ("AutoAdjustTimegrain", "true"),
                ),
                timeout=self._timeout,
            ),
        )
        raise_for_status(response, service="Azure Monitor", hints=_METRICS_HINTS)
        return parse_metrics_response(
            decode_json(response, service="Azure Monitor"),
            database=self.database_name,
            source=self.name,
        )

    async def _watermark(self, now: datetime) -> datetime | None:
        """The newest minute with a CPU value in the last 6 hours."""

        samples, _refused = await self._list(
            ("cpu_percent",), now - timedelta(hours=6), now, "PT1M"
        )
        return max((sample.time for sample in samples), default=None)


def _metric_names(
    filters: Mapping[str, Any] | None,
    *,
    default: Sequence[str],
    documented_only: bool = False,
) -> tuple[str, ...]:
    requested = (filters or {}).get("metric_names")
    if requested is None:
        return tuple(default)
    if (
        not isinstance(requested, (list, tuple))
        or not requested
        or not all(isinstance(name, str) for name in requested)
    ):
        raise ValueError("metric_names must be a list of metric names.")
    names = tuple(dict.fromkeys(name.strip() for name in requested))
    unknown = [
        name
        for name in names
        if name not in SQL_DATABASE_METRICS
        and (documented_only or not re.fullmatch(r"[A-Za-z0-9_ :%-]{1,128}", name))
    ]
    if unknown:
        raise ValueError(
            "metric_names must be Azure SQL Database metric names such as cpu_percent, "
            "log_write_percent or workers_percent."
        )
    return names


# --- Policy field validation ------------------------------------------------------


def _exact_fields(
    value: Any, where: str, *, required: set[str], optional: set[str]
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{where} must be an object.")
    unknown = sorted(set(value) - required - optional)
    if unknown:
        raise ValueError(f"{where}: unknown field(s) {', '.join(map(repr, unknown))}.")
    missing = sorted(required - set(value))
    if missing:
        raise ValueError(f"{where}: missing field(s) {', '.join(missing)}.")
    return value


def _identity(fields: Mapping[str, Any], where: str) -> DatabaseIdentity:
    try:
        return DatabaseIdentity(fields["logical_server"], fields["database"])
    except ValueError as exc:
        raise ValueError(f"{where}: {exc}") from None


def _categories(value: Any, allowed: frozenset[str], where: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or not all(isinstance(i, str) for i in value):
        raise ValueError(f"{where}: categories must be a non-empty list of category names.")
    names = {item.strip().casefold() for item in value}
    unknown = sorted(names - allowed)
    if unknown:
        raise ValueError(
            f"{where}: unknown category {', '.join(map(repr, unknown))}; use: "
            f"{', '.join(sorted(allowed))}."
        )
    return tuple(sorted(names))


def _bounded(value: Any, low: int, high: int, where: str, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ValueError(f"{where}: {name} must be a whole number from {low} to {high}.")
    return value


def _optional_bounded(value: Any, low: int, high: int, where: str, name: str) -> int | None:
    return None if value is None else _bounded(value, low, high, where, name)
