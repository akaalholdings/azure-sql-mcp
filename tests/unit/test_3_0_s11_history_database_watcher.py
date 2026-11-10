"""Database watcher history through KQL.

No payload here is recorded from a live data store. The fixtures use the Kusto
REST v1 response format (Tables, Columns, Rows) and only the table and column
names that Microsoft's database watcher articles publish: the dataset table
names, the common columns, and the columns in the sample queries for
sqldb_database_resource_utilization and sqldb_database_wait_stats.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp.history_database_watcher import DatabaseWatcherBinding
from azure_sql_mcp.history_database_watcher import DatabaseWatcherHistorySource
from azure_sql_mcp.history_database_watcher import build_resource_utilization_kql
from azure_sql_mcp.history_database_watcher import build_wait_stats_kql
from azure_sql_mcp.history_database_watcher import build_watermark_kql
from azure_sql_mcp.history_sources import DatabaseIdentity
from azure_sql_mcp.history_sources import EntraTokenProvider
from azure_sql_mcp.history_sources import HistoryCategory
from azure_sql_mcp.history_sources import HttpRequest
from azure_sql_mcp.history_sources import HttpResponse
from azure_sql_mcp.history_sources import ResourceMetricSample
from azure_sql_mcp.history_sources import TimeWindow
from azure_sql_mcp.history_sources import WaitStatsDelta
from azure_sql_mcp.result_status import ResultStatus

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "history"
NOW = datetime(2026, 11, 9, 3, 0, tzinfo=timezone.utc)
CLUSTER = "https://contoso-watcher.uksouth.kusto.windows.net"
IDENTITY = DatabaseIdentity("contoso-sql01", "SalesDb")


def _at(clock: str) -> datetime:
    hour, minute = (int(part) for part in clock.split(":"))
    return datetime(2026, 11, 9, hour, minute, tzinfo=timezone.utc)


WINDOW = TimeWindow(_at("02:00"), _at("02:30"))


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


class FakeToken:
    token = "secret-bearer-value"
    expires_on = 4_102_444_800


class FakeCredential:
    scope = ""

    def get_token(self, *scopes: str, **kwargs: Any) -> FakeToken:
        self.scope = scopes[0]
        return FakeToken()


class FakeTransport:
    def __init__(self, handler: Callable[[HttpRequest], HttpResponse]) -> None:
        self.handler = handler
        self.requests: list[HttpRequest] = []

    async def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        return self.handler(request)


def _store(data: str, watermark: str = "database_watcher_watermark.json") -> FakeTransport:
    def handler(request: HttpRequest) -> HttpResponse:
        if "summarize watermark" in request.json_body["csl"]:
            return HttpResponse(200, _fixture(watermark))
        return HttpResponse(200, _fixture(data))

    return FakeTransport(handler)


def _binding(**overrides: Any) -> DatabaseWatcherBinding:
    value: dict[str, Any] = {
        "cluster_uri": CLUSTER,
        "data_store_database": "watcher",
        "logical_server": "contoso-sql01",
        "database": "SalesDb",
        "datasets": [
            "sqldb_database_resource_utilization", "sqldb_database_wait_stats",
            "sqldb_database_active_sessions",
        ],
    }
    value.update(overrides)
    return DatabaseWatcherBinding.from_mapping("SalesDb", value)


def _source(
    transport: FakeTransport, binding: DatabaseWatcherBinding | None = None, *, bound: bool = True
) -> tuple[DatabaseWatcherHistorySource, FakeCredential]:
    credential = FakeCredential()
    bound_to = (binding or _binding()) if bound else None
    source = DatabaseWatcherHistorySource(
        "SalesDb",
        bound_to,
        transport=transport,
        tokens=EntraTokenProvider(
            credential, bound_to.token_scope if bound_to else "https://api.kusto.windows.net/.default"
        ),
        now=lambda: NOW,
    )
    return source, credential


def _read(source: Any, category: HistoryCategory, window: TimeWindow = WINDOW, **kwargs: Any) -> Any:
    return asyncio.run(source.read(category, window, **kwargs))


def test_resource_query_uses_only_the_published_table_and_columns() -> None:
    query = build_resource_utilization_kql(IDENTITY, WINDOW, limit=40)

    assert query.startswith("sqldb_database_resource_utilization\n")
    assert "sample_time_utc >= datetime(2026-11-09T02:00:00Z)" in query
    assert 'logical_server_name =~ "contoso-sql01"' in query
    assert 'database_name =~ "SalesDb"' in query
    # A secondary replica's CPU is not the primary's.
    assert 'replica_type =~ "Primary"' in query
    for column in ("avg_cpu_percent", "avg_instance_cpu_percent", "avg_data_io_percent",
                   "avg_log_write_percent", "max_worker_percent"):
        assert column in query
    assert "bin(sample_time_utc, 1m)" in query
    assert query.rstrip().endswith("| top 41 by sample_time_utc asc")


def test_wait_query_takes_deltas_inside_one_replica_and_wait_type() -> None:
    # wait_time_ms is cumulative. A delta across two replicas or two wait types,
    # or across a restart that reset the counter, is not wait time.
    query = build_wait_stats_kql(IDENTITY, WINDOW, limit=50)

    assert query.startswith("sqldb_database_wait_stats\n")
    assert "| sort by replica_id asc, wait_type asc, sample_time_utc asc" in query
    assert (
        "wait_time_ms >= prev(wait_time_ms) and replica_id == prev(replica_id) and "
        "wait_type == prev(wait_type)"
    ) in query
    # The sample before the window is read so the first delta has a start.
    assert "sample_time_utc >= datetime(2026-11-09T01:59:00Z)" in query
    assert "by wait_type, bucket = bin(sample_time_utc, 5m)" in query


def test_watermark_query_is_the_newest_sample_for_the_database() -> None:
    query = build_watermark_kql("sqldb_database_wait_stats", IDENTITY, since=_at("02:00"))

    assert query.rstrip().endswith("| summarize watermark = max(sample_time_utc)")
    assert 'database_name =~ "SalesDb"' in query


def test_request_is_the_kusto_rest_query_call() -> None:
    transport = _store("database_watcher_resource_utilization.json")
    source, credential = _source(transport)

    read = _read(source, HistoryCategory.RESOURCE_METRIC)

    request = transport.requests[0]
    assert request.method == "POST" and request.url == CLUSTER + "/v1/rest/query"
    assert request.json_body["db"] == "watcher"
    assert request.json_body["csl"].startswith("sqldb_database_resource_utilization")
    assert request.headers["Authorization"] == "Bearer secret-bearer-value"
    assert credential.scope == "https://api.kusto.windows.net/.default"
    text = json.dumps(read.coverage.to_dict())
    assert "contoso-watcher" not in text and "secret-bearer-value" not in text


def test_resource_rows_become_one_sample_per_metric_and_minute() -> None:
    source, _ = _source(_store("database_watcher_resource_utilization.json"))

    read = _read(source, HistoryCategory.RESOURCE_METRIC)

    assert read.coverage.status is ResultStatus.OK
    samples = [event for event in read.events if isinstance(event, ResourceMetricSample)]
    assert len(samples) == 10
    cpu = [sample for sample in samples if sample.metric == "cpu_percent"]
    assert [(sample.time, sample.average, sample.maximum) for sample in cpu] == [
        (_at("02:09"), 41.0, 55.0), (_at("02:10"), 97.4, 100.0),
    ]
    workers = [sample for sample in samples if sample.metric == "worker_percent"]
    assert [sample.maximum for sample in workers] == [6.5, 71.0]
    assert all(sample.grain == "PT1M" and sample.unit == "Percent" for sample in samples)


def test_wait_rows_become_deltas_with_their_interval() -> None:
    source, _ = _source(_store("database_watcher_wait_stats.json"))

    read = _read(source, HistoryCategory.WAIT_STATS)

    lock_wait = next(event for event in read.events if isinstance(event, WaitStatsDelta)
                     and event.wait_type == "LCK_M_U")
    assert lock_wait.wait_time_ms == 184320.0
    assert lock_wait.interval_start is not None and lock_wait.interval_start.minute == 10
    assert lock_wait.time == lock_wait.interval_end


def test_session_snapshots_are_not_parsed_on_a_guessed_schema() -> None:
    # Microsoft publishes the active sessions table name but not its columns.
    transport = _store("database_watcher_empty.json")
    source, _ = _source(transport)

    read = _read(source, HistoryCategory.SESSION_SNAPSHOT)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert read.coverage.reason.startswith("unverified_schema")
    assert "sqldb_database_active_sessions" in read.coverage.reason
    assert transport.requests == []


def test_event_categories_the_watcher_does_not_collect_are_not_supported() -> None:
    source, _ = _source(_store("database_watcher_empty.json"))

    assert _read(source, HistoryCategory.DEADLOCK).coverage.status is ResultStatus.NOT_SUPPORTED


def test_unmapped_database_and_undeclared_dataset_are_preconditions() -> None:
    transport = _store("database_watcher_empty.json")
    unmapped, _ = _source(transport, bound=False)
    partial, _ = _source(transport, _binding(datasets=["sqldb_database_wait_stats"]))

    no_store = _read(unmapped, HistoryCategory.WAIT_STATS)
    no_dataset = _read(partial, HistoryCategory.RESOURCE_METRIC)

    assert no_store.coverage.status is ResultStatus.PRECONDITION
    assert '"cluster_uri"' in (no_store.coverage.remediation or "")
    assert no_dataset.coverage.status is ResultStatus.PRECONDITION
    assert "sqldb_database_resource_utilization" in (no_dataset.coverage.remediation or "")
    assert transport.requests == []


def test_quiet_window_is_empty_only_when_it_is_ingested() -> None:
    source, _ = _source(_store("database_watcher_empty.json"))  # newest sample 02:59:45, lag 2 min

    ingested = _read(source, HistoryCategory.WAIT_STATS, TimeWindow(_at("02:00"), _at("02:30")))
    recent = _read(source, HistoryCategory.WAIT_STATS, TimeWindow(_at("02:40"), _at("02:59")))

    assert ingested.coverage.status is ResultStatus.EMPTY
    assert recent.coverage.status is ResultStatus.UNAVAILABLE
    assert "not ingested yet" in recent.coverage.reason


def test_rejected_token_is_unavailable_and_value_free() -> None:
    transport = FakeTransport(lambda request: HttpResponse(401, b"contoso-watcher SalesDb"))
    source, _ = _source(transport)

    read = _read(source, HistoryCategory.WAIT_STATS)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert "contoso" not in read.coverage.reason and "SalesDb" not in read.coverage.reason
    assert read.coverage.to_dict()["read_error"]["status_code"] == 401


def test_binding_validates_the_cluster_address_and_dataset_names() -> None:
    with pytest.raises(ValueError, match="cluster_uri"):
        _binding(cluster_uri="http://contoso-watcher.uksouth.kusto.windows.net")
    with pytest.raises(ValueError, match="cluster_uri"):
        _binding(cluster_uri=CLUSTER + "/v1/rest/query?x=1")
    with pytest.raises(ValueError, match="unknown dataset"):
        _binding(datasets=["sqldb_database_everything"])
    with pytest.raises(ValueError, match="data_store_database"):
        _binding(data_store_database="watcher; .drop table x")
    fabric = _binding(
        cluster_uri="https://abc123.z0.kusto.fabric.microsoft.com",
        token_scope="https://kusto.kusto.windows.net/.default",
    )
    assert fabric.token_scope == "https://kusto.kusto.windows.net/.default"
    assert _binding().ingest_lag_minutes == 2
