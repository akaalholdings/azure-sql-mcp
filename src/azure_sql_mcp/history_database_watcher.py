"""History from a database watcher data store, read with KQL.

Database watcher collects monitoring data into an Azure Data Explorer cluster
or a Fabric eventhouse. Both answer the Kusto REST query API, so one source
reads either: map the store's query address and database.

Microsoft publishes the dataset table names and the columns common to every
table, and it shows a few more columns in its sample queries. This adapter uses
only those. A dataset whose columns are not published is not parsed on a
guess: the source returns ``unavailable`` with the reason ``unverified_schema``.

- ``sqldb_database_resource_utilization`` (documented columns): resource metrics.
- ``sqldb_database_wait_stats`` (documented columns): wait time deltas.
- ``sqldb_database_active_sessions`` (columns not published): session snapshots,
  not read.

The KQL is built from that documentation and has not been run against a store.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from urllib.parse import urlsplit

from .history_azure_monitor import POLICY_FILE_HINT
from .history_azure_monitor import kql_datetime
from .history_azure_monitor import kql_string
from .history_sources import DEFAULT_LIMIT
from .history_sources import MAX_LIMIT
from .history_sources import Coverage
from .history_sources import DatabaseIdentity
from .history_sources import EntraTokenProvider
from .history_sources import HistoryCategory
from .history_sources import HistoryEvent
from .history_sources import HistoryReadError
from .history_sources import HttpRequest
from .history_sources import HttpTransport
from .history_sources import ResourceMetricSample
from .history_sources import SchemaBasis
from .history_sources import SourceDescription
from .history_sources import SourceRead
from .history_sources import TimeWindow
from .history_sources import WaitStatsDelta
from .history_sources import clean_text
from .history_sources import decode_json
from .history_sources import parse_utc
from .history_sources import raise_for_status
from .history_sources import send_authorized
from .history_sources import settle_read
from .history_sources import to_float
from .history_sources import unverified_schema_gate
from .incident_log import note_exception
from .result_status import ResultStatus

SOURCE = "database_watcher"
FAMILY = "database_watcher"
# The audience in Microsoft's Kusto REST authentication article.
KUSTO_SCOPE = "https://api.kusto.windows.net/.default"
RESOURCE_TABLE = "sqldb_database_resource_utilization"
WAIT_TABLE = "sqldb_database_wait_stats"
SESSIONS_TABLE = "sqldb_database_active_sessions"

_DATA_STORE_DATABASE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_ .-]{0,127}")
_ENTRA_SCOPE = re.compile(r"\S+/\.default")


@dataclass(frozen=True)
class WatcherDataset:
    table: str
    basis: SchemaBasis


DATASETS: Mapping[HistoryCategory, WatcherDataset] = {
    HistoryCategory.RESOURCE_METRIC: WatcherDataset(RESOURCE_TABLE, SchemaBasis.DOCUMENTED),
    HistoryCategory.WAIT_STATS: WatcherDataset(WAIT_TABLE, SchemaBasis.DOCUMENTED),
    HistoryCategory.SESSION_SNAPSHOT: WatcherDataset(SESSIONS_TABLE, SchemaBasis.UNVERIFIED),
}
_DECLARABLE_DATASETS = frozenset(dataset.table for dataset in DATASETS.values())
# metric name -> (average column, maximum column) in the binned result.
_RESOURCE_METRICS: Mapping[str, tuple[str | None, str]] = {
    "cpu_percent": ("avg_cpu_percent", "max_cpu_percent"),
    "instance_cpu_percent": ("avg_instance_cpu_percent", "max_instance_cpu_percent"),
    "data_io_percent": ("avg_data_io_percent", "max_data_io_percent"),
    "log_write_percent": ("avg_log_write_percent", "max_log_write_percent"),
    "worker_percent": (None, "max_worker_percent"),
}


def _scope_lines(identity: DatabaseIdentity) -> list[str]:
    return [
        f"| where logical_server_name =~ {kql_string(identity.logical_server)}",
        f"| where database_name =~ {kql_string(identity.database)}",
        '| where replica_type =~ "Primary"',
    ]


def build_resource_utilization_kql(
    identity: DatabaseIdentity, window: TimeWindow, *, limit: int
) -> str:
    """One row per minute: average and peak of each resource percentage."""

    aggregates = ", ".join(
        f"avg_{name} = avg(avg_{name}), max_{name} = max(avg_{name})"
        for name in ("cpu_percent", "instance_cpu_percent", "data_io_percent", "log_write_percent")
    )
    return "\n".join(
        [
            RESOURCE_TABLE,
            f"| where sample_time_utc >= {kql_datetime(window.start)} and sample_time_utc < "
            f"{kql_datetime(window.end)}",
            *_scope_lines(identity),
            f"| summarize {aggregates}, max_worker_percent = max(max_worker_percent) "
            "by sample_time_utc = bin(sample_time_utc, 1m)",
            f"| top {int(limit) + 1} by sample_time_utc asc",
        ]
    )


def build_wait_stats_kql(identity: DatabaseIdentity, window: TimeWindow, *, limit: int) -> str:
    """Wait time added per wait type in 5 minute buckets.

    ``wait_time_ms`` is cumulative. As in Microsoft's sample query, a delta is
    taken only between two samples of one replica, and only when the counter
    did not go down (a restart resets it).
    """

    lead_in = window.start - timedelta(minutes=1)
    return "\n".join(
        [
            WAIT_TABLE,
            f"| where sample_time_utc >= {kql_datetime(lead_in)} and sample_time_utc < "
            f"{kql_datetime(window.end)}",
            *_scope_lines(identity),
            "| project replica_id, sample_time_utc, wait_type, wait_time_ms",
            "| sort by replica_id asc, wait_type asc, sample_time_utc asc",
            "| extend delta_wait_time_ms = iif(wait_time_ms >= prev(wait_time_ms) and "
            "replica_id == prev(replica_id) and wait_type == prev(wait_type), "
            "wait_time_ms - prev(wait_time_ms), long(null))",
            f"| where isnotnull(delta_wait_time_ms) and sample_time_utc >= "
            f"{kql_datetime(window.start)}",
            "| summarize wait_time_ms = sum(delta_wait_time_ms), "
            "interval_start = min(sample_time_utc), interval_end = max(sample_time_utc) "
            "by wait_type, bucket = bin(sample_time_utc, 5m)",
            "| where wait_time_ms > 0",
            f"| top {int(limit) + 1} by bucket asc",
        ]
    )


def build_watermark_kql(table: str, identity: DatabaseIdentity, *, since: datetime) -> str:
    if table not in _DECLARABLE_DATASETS:
        raise ValueError("Unknown database watcher dataset table.")
    return "\n".join(
        [
            table,
            f"| where sample_time_utc >= {kql_datetime(since)}",
            *_scope_lines(identity),
            "| summarize watermark = max(sample_time_utc)",
        ]
    )


def rows_from_kusto(document: Any) -> list[dict[str, Any]]:
    """Rows of the first table of a Kusto REST v1 response, as dicts."""

    tables = document.get("Tables") if isinstance(document, dict) else None
    if not isinstance(tables, list) or not tables or not isinstance(tables[0], dict):
        raise HistoryReadError(
            "protocol", "The data store returned a response that is not a query result."
        )
    columns = tables[0].get("Columns")
    rows = tables[0].get("Rows")
    if not isinstance(columns, list) or not isinstance(rows, list):
        raise HistoryReadError(
            "protocol", "The data store returned a response that is not a query result."
        )
    names = [
        str(column.get("ColumnName")) if isinstance(column, dict) else "" for column in columns
    ]
    parsed: list[dict[str, Any]] = []
    for row in rows:
        # A row that is not an array carries a query failure.
        if not isinstance(row, list):
            raise HistoryReadError("partial", "The data store reported a query failure.")
        parsed.append(dict(zip(names, row, strict=False)))
    return parsed


@dataclass(frozen=True)
class DatabaseWatcherBinding:
    """The ``database_watcher`` policy entry of one database."""

    cluster_uri: str
    data_store_database: str
    identity: DatabaseIdentity
    # The dataset tables the watcher collects for this database.
    datasets: tuple[str, ...]
    ingest_lag_minutes: int = 2
    retention_days: int | None = None
    token_scope: str = KUSTO_SCOPE

    @classmethod
    def from_mapping(cls, database_name: str, value: Any) -> "DatabaseWatcherBinding":
        where = f"database_watcher for database {database_name!r}"
        if not isinstance(value, Mapping):
            raise ValueError(f"{where} must be an object.")
        required = {"cluster_uri", "data_store_database", "logical_server", "database", "datasets"}
        optional = {"ingest_lag_minutes", "retention_days", "token_scope"}
        unknown = sorted(set(value) - required - optional)
        if unknown:
            raise ValueError(f"{where}: unknown field(s) {', '.join(map(repr, unknown))}.")
        missing = sorted(required - set(value))
        if missing:
            raise ValueError(f"{where}: missing field(s) {', '.join(missing)}.")
        store = value["data_store_database"]
        if not isinstance(store, str) or not _DATA_STORE_DATABASE.fullmatch(store):
            raise ValueError(
                f"{where}: data_store_database must be the data store's database name "
                "(letters, digits, spaces, dots, hyphens or underscores)."
            )
        datasets = value["datasets"]
        if (
            not isinstance(datasets, list)
            or not datasets
            or not all(isinstance(item, str) for item in datasets)
        ):
            raise ValueError(f"{where}: datasets must be a non-empty list of table names.")
        unknown_datasets = sorted(set(datasets) - _DECLARABLE_DATASETS)
        if unknown_datasets:
            raise ValueError(
                f"{where}: unknown dataset {', '.join(map(repr, unknown_datasets))}; use: "
                f"{', '.join(sorted(_DECLARABLE_DATASETS))}."
            )
        scope = value.get("token_scope", KUSTO_SCOPE)
        if not isinstance(scope, str) or not _ENTRA_SCOPE.fullmatch(scope):
            raise ValueError(f"{where}: token_scope must be an Entra scope ending in /.default.")
        try:
            identity = DatabaseIdentity(value["logical_server"], value["database"])
        except ValueError as exc:
            raise ValueError(f"{where}: {exc}") from None
        return cls(
            cluster_uri=_cluster_uri(value["cluster_uri"], where),
            data_store_database=store,
            identity=identity,
            datasets=tuple(sorted(set(datasets))),
            ingest_lag_minutes=_whole(value.get("ingest_lag_minutes", 2), 0, 1440, where,
                                      "ingest_lag_minutes"),
            retention_days=(
                None if value.get("retention_days") is None
                else _whole(value["retention_days"], 1, 36500, where, "retention_days")
            ),
            token_scope=scope,
        )


def _cluster_uri(value: Any, where: str) -> str:
    parts = urlsplit(value) if isinstance(value, str) else None
    if (
        parts is None
        or parts.scheme != "https"
        or not parts.hostname
        or parts.username is not None
        or parts.path not in {"", "/"}
        or parts.query
        or parts.fragment
    ):
        raise ValueError(
            f"{where}: cluster_uri must be the data store's https query address with a "
            "host and no path, query or user info."
        )
    return f"https://{parts.netloc}"


def _whole(value: Any, low: int, high: int, where: str, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ValueError(f"{where}: {name} must be a whole number from {low} to {high}.")
    return value


_POLICY_SNIPPET = (
    f"Add a database_watcher entry to this database's history_sources in {POLICY_FILE_HINT}:\n"
    '"database_watcher": {"cluster_uri": "https://<cluster>.<region>.kusto.windows.net", '
    '"data_store_database": "<data store database>", "logical_server": "<short server name>", '
    '"database": "<database name>", "datasets": ["sqldb_database_resource_utilization", '
    '"sqldb_database_wait_stats"]}\n'
    "For a Fabric eventhouse use its query address and set token_scope. The identity needs "
    "the Viewer role on the data store database."
)
_HINTS = {
    403: "The identity needs the Viewer role on the data store database.",
    404: "Check database_watcher.cluster_uri and data_store_database in the local database "
    "policy.",
}


class DatabaseWatcherHistorySource:
    """Resource use and wait time deltas from a database watcher data store."""

    name = SOURCE
    family = FAMILY

    def __init__(
        self,
        database_name: str,
        binding: DatabaseWatcherBinding | None,
        *,
        transport: HttpTransport,
        tokens: EntraTokenProvider,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        timeout_seconds: float = 30.0,
    ) -> None:
        self.database_name = database_name
        self._binding = binding
        self._transport = transport
        self._tokens = tokens
        self._now = now
        self._timeout = timeout_seconds

    def gate(self, category: HistoryCategory, window: TimeWindow) -> Coverage | None:
        dataset = DATASETS.get(category)
        if dataset is None:
            return Coverage.gated(
                self.name, self.family, category, window, ResultStatus.NOT_SUPPORTED,
                f"Database watcher has no dataset of {category.value} records.",
            )
        if self._binding is None:
            return Coverage.gated(
                self.name, self.family, category, window, ResultStatus.PRECONDITION,
                "No database watcher data store is mapped for this database.",
                remediation=_POLICY_SNIPPET,
            )
        if dataset.table not in self._binding.datasets:
            return Coverage.gated(
                self.name, self.family, category, window, ResultStatus.PRECONDITION,
                f"The {dataset.table} dataset is not declared for this database.",
                remediation=(
                    f"Confirm that the watcher collects {dataset.table} for this database, then "
                    f'add "{dataset.table}" to history_sources.database_watcher.datasets in '
                    f"{POLICY_FILE_HINT}."
                ),
            )
        if dataset.basis is SchemaBasis.UNVERIFIED:
            return unverified_schema_gate(
                self.name, self.family, category, window,
                f"Microsoft publishes the {dataset.table} table name but not its columns.",
            )
        return None

    async def describe(self) -> SourceDescription:
        binding = self._binding
        if binding is None:
            return SourceDescription(self.name, self.family, self.database_name, False, ())
        now = self._now()
        watermark: datetime | None = None
        reason: str | None = None
        tables = [
            dataset.table
            for dataset in DATASETS.values()
            if dataset.table in binding.datasets and dataset.basis is not SchemaBasis.UNVERIFIED
        ]
        if tables:
            try:
                watermark = await self._watermark(tables[0], now - timedelta(days=1))
                if watermark is None:
                    reason = "No sample for this database in the last 24 hours."
            except HistoryReadError as exc:
                reason = str(exc)
        return SourceDescription(
            self.name,
            self.family,
            self.database_name,
            True,
            tuple(
                category.value
                for category, dataset in DATASETS.items()
                if dataset.table in tables
            ),
            retention_days=binding.retention_days,
            expected_ingest_lag_seconds=binding.ingest_lag_minutes * 60.0,
            watermark=watermark,
            watermark_reason=reason,
            notes=(
                f"{SESSIONS_TABLE} is not read: its columns are not published "
                "(unverified_schema).",
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
        binding = self._binding
        if gate is not None or binding is None:
            assert gate is not None
            return SourceRead([], gate)
        limit = max(1, min(int(limit), MAX_LIMIT))
        now = self._now()
        if category is HistoryCategory.RESOURCE_METRIC:
            # One row holds five metrics.
            row_limit = max(1, limit // len(_RESOURCE_METRICS))
            query = build_resource_utilization_kql(binding.identity, window, limit=row_limit)
        else:
            row_limit = limit
            query = build_wait_stats_kql(binding.identity, window, limit=row_limit)
        table = DATASETS[category].table
        rows_result, watermark_result = await asyncio.gather(
            self._rows(query), self._watermark(table, window.start), return_exceptions=True
        )
        read_error = _history_error(rows_result, "history_database_watcher.read")
        watermark_error = _history_error(watermark_result, "history_database_watcher.watermark")
        events: list[HistoryEvent] = []
        rows_read = unparsed = 0
        truncated: str | None = None
        if not isinstance(rows_result, BaseException):
            rows = rows_result
            if len(rows) > row_limit:
                rows, truncated = rows[:row_limit], "max_rows"
            rows_read = len(rows)
            for row in rows:
                parsed = (
                    self._resource_samples(row)
                    if category is HistoryCategory.RESOURCE_METRIC
                    else self._wait_delta(row)
                )
                if parsed:
                    events.extend(parsed)
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
            watermark=None if isinstance(watermark_result, BaseException) else watermark_result,
            ingest_lag=timedelta(minutes=binding.ingest_lag_minutes),
            retention_floor=(
                None if binding.retention_days is None
                else now - timedelta(days=binding.retention_days)
            ),
            truncated_reason=truncated,
            read_through=max((event.time for event in events), default=None)
            if truncated else None,
            read_error=read_error,
            watermark_error=watermark_error,
            schema_basis=SchemaBasis.DOCUMENTED,
        )

    def _resource_samples(self, row: Mapping[str, Any]) -> list[HistoryEvent]:
        time = parse_utc(row.get("sample_time_utc"))
        if time is None:
            return []
        samples: list[HistoryEvent] = []
        for metric, (average_column, maximum_column) in _RESOURCE_METRICS.items():
            average = to_float(row.get(average_column)) if average_column else None
            maximum = to_float(row.get(maximum_column))
            if average is None and maximum is None:
                continue
            samples.append(
                ResourceMetricSample(
                    time=time,
                    database=self.database_name,
                    source=self.name,
                    metric=metric,
                    grain="PT1M",
                    unit="Percent",
                    average=average,
                    maximum=maximum,
                )
            )
        return samples

    def _wait_delta(self, row: Mapping[str, Any]) -> list[HistoryEvent]:
        wait_type = clean_text(row.get("wait_type"))
        end = parse_utc(row.get("interval_end"))
        if wait_type is None or end is None:
            return []
        return [
            WaitStatsDelta(
                time=end,
                database=self.database_name,
                source=self.name,
                wait_type=wait_type,
                interval_start=parse_utc(row.get("interval_start")),
                interval_end=end,
                wait_time_ms=to_float(row.get("wait_time_ms")),
            )
        ]

    async def _rows(self, query: str) -> list[dict[str, Any]]:
        binding = self._binding
        assert binding is not None
        response = await send_authorized(
            self._transport,
            self._tokens,
            HttpRequest(
                "POST",
                binding.cluster_uri + "/v1/rest/query",
                json_body={"db": binding.data_store_database, "csl": query},
                timeout=self._timeout,
            ),
        )
        raise_for_status(response, service="The database watcher data store", hints=_HINTS)
        return rows_from_kusto(decode_json(response, service="data store"))

    async def _watermark(self, table: str, since: datetime) -> datetime | None:
        binding = self._binding
        assert binding is not None
        rows = await self._rows(build_watermark_kql(table, binding.identity, since=since))
        return parse_utc(rows[0].get("watermark")) if rows else None


def _history_error(result: Any, site: str) -> HistoryReadError | None:
    if isinstance(result, HistoryReadError):
        note_exception(result, site)
        return result
    if isinstance(result, BaseException):
        raise result
    return None
