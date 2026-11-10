"""Azure Monitor history: Log Analytics diagnostics, audit, and platform metrics.

No payload here is recorded from a live service. The fixtures are built from:
the Logs query API response format (tables, columns, rows); the AzureDiagnostics
column lists for the Azure SQL categories and the SQL Database audit log format
on Microsoft Learn; the AzureMetrics column list; and the Metrics - List example
responses (api-version 2024-02-01) in Azure/azure-rest-api-specs.
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

from azure_sql_mcp.history_azure_monitor import AzureMetricsBinding
from azure_sql_mcp.history_azure_monitor import AzureMonitorMetricsSource
from azure_sql_mcp.history_azure_monitor import LogAnalyticsBinding
from azure_sql_mcp.history_azure_monitor import LogAnalyticsHistorySource
from azure_sql_mcp.history_azure_monitor import build_diagnostics_kql
from azure_sql_mcp.history_azure_monitor import build_metrics_kql
from azure_sql_mcp.history_azure_monitor import build_watermark_kql
from azure_sql_mcp.history_azure_monitor import export_property_name
from azure_sql_mcp.history_azure_monitor import kql_string
from azure_sql_mcp.history_azure_monitor import parse_metrics_response
from azure_sql_mcp.history_sources import AuditRecord
from azure_sql_mcp.history_sources import BlockedProcessReport
from azure_sql_mcp.history_sources import DatabaseIdentity
from azure_sql_mcp.history_sources import DeadlockEvent
from azure_sql_mcp.history_sources import EntraTokenProvider
from azure_sql_mcp.history_sources import ErrorEvent
from azure_sql_mcp.history_sources import HistoryCategory
from azure_sql_mcp.history_sources import HistoryResolver
from azure_sql_mcp.history_sources import HttpRequest
from azure_sql_mcp.history_sources import HttpResponse
from azure_sql_mcp.history_sources import ResourceMetricSample
from azure_sql_mcp.history_sources import TimeoutEvent
from azure_sql_mcp.history_sources import TimeWindow
from azure_sql_mcp.history_sources import WaitStatsDelta
from azure_sql_mcp.result_status import ResultStatus

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "history"
NOW = datetime(2026, 11, 9, 3, 0, tzinfo=timezone.utc)
WORKSPACE = "11111111-2222-3333-4444-555555555555"
RESOURCE_ID = (
    "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-sales"
    "/providers/Microsoft.Sql/servers/contoso-sql01/databases/SalesDb"
)
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


def _logs(data: str, watermark: str = "log_analytics_watermark.json") -> FakeTransport:
    def handler(request: HttpRequest) -> HttpResponse:
        if "summarize watermark" in request.json_body["query"]:
            return HttpResponse(200, _fixture(watermark))
        return HttpResponse(200, _fixture(data))

    return FakeTransport(handler)


def _binding(**overrides: Any) -> LogAnalyticsBinding:
    value: dict[str, Any] = {
        "workspace_id": WORKSPACE,
        "logical_server": "contoso-sql01",
        "database": "SalesDb",
        "categories": [
            "blocks", "timeouts", "deadlocks", "errors", "databasewaitstatistics",
            "sqlsecurityauditevents", "metrics",
        ],
    }
    value.update(overrides)
    return LogAnalyticsBinding.from_mapping("SalesDb", value)


def _source(
    transport: FakeTransport, binding: LogAnalyticsBinding | None = None, *, bound: bool = True
) -> LogAnalyticsHistorySource:
    credential = FakeCredential()
    transport.credential = credential  # type: ignore[attr-defined]
    return LogAnalyticsHistorySource(
        "SalesDb",
        (binding or _binding()) if bound else None,
        transport=transport,
        tokens=EntraTokenProvider(credential, "https://api.loganalytics.io/.default"),
        now=lambda: NOW,
    )


def _read(source: Any, category: HistoryCategory, window: TimeWindow = WINDOW, **kwargs: Any) -> Any:
    return asyncio.run(source.read(category, window, **kwargs))


# --- query builders ------------------------------------------------------------


def test_blocks_query_uses_the_documented_columns_for_one_database_and_window() -> None:
    query = build_diagnostics_kql("blocks", IDENTITY, WINDOW, limit=200)

    assert query.startswith("AzureDiagnostics\n")
    assert 'Category == "Blocks"' in query
    assert "TimeGenerated >= datetime(2026-11-09T02:00:00Z)" in query
    assert "TimeGenerated < datetime(2026-11-09T02:30:00Z)" in query
    assert 'mcp_server =~ "contoso-sql01" and mcp_database =~ "SalesDb"' in query
    for column in ("lock_mode_s", "resource_owner_type_s", "blocked_process_filtered_s", "duration_d"):
        assert f'{column} = tostring(column_ifexists("{column}", ""))' in query
    # One row more than the limit shows that the read was cut.
    assert query.rstrip().endswith("| top 201 by TimeGenerated asc")


def test_a_missing_column_does_not_fail_the_query() -> None:
    # A workspace that never received Blocks has no blocked_process_filtered_s
    # column. A bare column name fails the whole query; column_ifexists does not.
    query = build_diagnostics_kql("blocks", IDENTITY, WINDOW, limit=10)

    assert 'column_ifexists("LogicalServerName_s", "")' in query
    assert 'column_ifexists("AdditionalFields", dynamic(null))' in query
    assert "| where blocked_process_filtered_s" not in query


def test_audit_query_uses_the_audit_identity_columns() -> None:
    query = build_diagnostics_kql("sqlsecurityauditevents", IDENTITY, WINDOW, limit=10)

    assert 'Category == "SQLSecurityAuditEvents"' in query
    assert 'column_ifexists("server_instance_name_s", "")' in query
    assert 'column_ifexists("database_name_s", "")' in query
    assert 'statement_s = tostring(column_ifexists("statement_s", ""))' in query


def test_watermark_query_reads_the_newest_record_of_the_declared_categories() -> None:
    query = build_watermark_kql(("blocks", "errors"), IDENTITY, since=_at("02:00"))

    assert 'Category in ("Blocks", "Errors")' in query
    assert "TimeGenerated >= datetime(2026-11-09T02:00:00Z)" in query
    assert query.rstrip().endswith("| summarize watermark = max(TimeGenerated)")


def test_metrics_query_scopes_by_resource_and_applies_the_metric_filter() -> None:
    query = build_metrics_kql(IDENTITY, WINDOW, limit=50, metric_names=("cpu_percent",))

    assert query.startswith("AzureMetrics\n")
    assert '_ResourceId endswith "/servers/contoso-sql01/databases/SalesDb"' in query
    assert 'MetricName in~ ("cpu_percent")' in query


def test_kql_string_escapes_quotes_and_backslashes() -> None:
    assert kql_string('a"b\\c') == '"a\\"b\\\\c"'
    with pytest.raises(ValueError):
        kql_string("line\nbreak")


def test_export_property_name_drops_only_the_log_analytics_type_suffix() -> None:
    assert export_property_name("blocked_process_filtered_s") == "blocked_process_filtered"
    assert export_property_name("duration_d") == "duration"
    assert export_property_name("start_utc_date_t") == "start_utc_date"
    assert export_property_name("user_defined_b") == "user_defined"
    assert export_property_name("Message") == "Message"


# --- policy binding --------------------------------------------------------------


def test_binding_refuses_what_it_does_not_know() -> None:
    with pytest.raises(ValueError, match="unknown field"):
        _binding(workspace_key="abc")
    with pytest.raises(ValueError, match="workspace_id"):
        _binding(workspace_id="not-a-guid")
    with pytest.raises(ValueError, match="unknown category"):
        _binding(categories=["blocks", "everything"])
    with pytest.raises(ValueError, match="logical_server"):
        _binding(logical_server="contoso-sql01.database.windows.net")


def test_binding_defaults_follow_the_documented_export_delay() -> None:
    binding = _binding()

    # Azure SQL Database sends its logs every five minutes; 3 to 10 minutes end to end.
    assert binding.ingest_lag_minutes == 15
    assert binding.retention_days is None


# --- reads ----------------------------------------------------------------------


def test_blocks_rows_become_reports_and_another_database_is_rejected() -> None:
    transport = _logs("log_analytics_blocks.json")

    read = _read(_source(transport), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.OK
    assert [type(event) for event in read.events] == [BlockedProcessReport] * 2
    assert read.coverage.rows_read == 3
    # SalesDb_Dev is on the same server. Its blocking is not SalesDb's.
    assert read.coverage.rows_rejected_identity == 1
    first = read.events[0]
    assert isinstance(first, BlockedProcessReport)
    assert first.blocked.session_id == 71 and first.blocking.session_id == 64
    assert first.duration_ms == 21044.0  # duration_d is in microseconds


def test_blocks_read_resolves_to_one_episode_with_the_head_blocker() -> None:
    source = _source(_logs("log_analytics_blocks.json"))
    resolver = HistoryResolver([source], database_name="SalesDb", now=lambda: NOW)

    payload = asyncio.run(resolver.read(HistoryCategory.BLOCKING, WINDOW)).to_payload()

    assert payload["result_status"] == "ok" and payload["event_count"] == 1
    episode = payload["events"][0]
    assert episode["head_blocker"]["session_id"] == 64
    assert episode["head_blocker_idle_in_transaction"] is True
    assert "9912" not in json.dumps(payload)  # literals are masked by default


def test_fields_moved_to_additional_fields_are_still_read() -> None:
    # Past 500 columns Azure Monitor puts new fields in AdditionalFields. Reading
    # only the named columns would see blocking rows with no report in them.
    read = _read(_source(_logs("log_analytics_blocks_additional_fields.json")), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.OK
    event = read.events[0]
    assert isinstance(event, BlockedProcessReport)
    assert event.wait_time_ms == 21044 and event.resource_owner_type == "LOCK"


def test_rows_without_a_report_are_unparsed_and_never_empty() -> None:
    def handler(request: HttpRequest) -> HttpResponse:
        if "summarize watermark" in request.json_body["query"]:
            return HttpResponse(200, _fixture("log_analytics_watermark.json"))
        document = json.loads(_fixture("log_analytics_blocks.json"))
        for row in document["tables"][0]["rows"]:
            row[5] = ""
        return HttpResponse(200, json.dumps(document).encode())

    read = _read(_source(FakeTransport(handler)), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert read.coverage.rows_unparsed == 2
    assert "none parsed" in read.coverage.reason


def test_quiet_window_is_empty_only_when_it_is_ingested() -> None:
    source = _source(_logs("log_analytics_empty.json"))  # newest record 02:55, lag 15 min

    ingested = _read(source, HistoryCategory.BLOCKING, TimeWindow(_at("02:00"), _at("02:30")))
    recent = _read(source, HistoryCategory.BLOCKING, TimeWindow(_at("02:20"), _at("02:50")))

    assert ingested.coverage.status is ResultStatus.EMPTY
    assert recent.coverage.status is ResultStatus.UNAVAILABLE
    assert "not ingested yet" in recent.coverage.reason
    assert recent.coverage.to_dict()["not_ingested_from_utc"] == "2026-11-09T02:40:00Z"


def test_database_with_no_record_at_all_is_unavailable() -> None:
    source = _source(_logs("log_analytics_empty.json", "log_analytics_watermark_none.json"))

    read = _read(source, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert "ingest progress is unknown" in read.coverage.reason


def test_partial_result_is_never_empty() -> None:
    read = _read(_source(_logs("log_analytics_partial_error.json")), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert read.coverage.truncated_reason == "partial_result"


def test_undeclared_category_is_a_precondition_and_sends_nothing() -> None:
    transport = _logs("log_analytics_blocks.json")
    source = _source(transport, _binding(categories=["errors"]))

    read = _read(source, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.PRECONDITION
    assert "Blocks" in read.coverage.reason
    assert '"blocks"' in (read.coverage.remediation or "")
    assert transport.requests == []


def test_database_without_a_workspace_is_a_precondition_with_the_policy_text() -> None:
    transport = _logs("log_analytics_blocks.json")

    read = _read(_source(transport, bound=False), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.PRECONDITION
    assert '"workspace_id"' in (read.coverage.remediation or "")
    assert transport.requests == []


def test_categories_azure_diagnostics_cannot_hold_are_not_supported() -> None:
    read = _read(_source(_logs("log_analytics_empty.json")), HistoryCategory.SESSION_SNAPSHOT)

    assert read.coverage.status is ResultStatus.NOT_SUPPORTED


def test_request_is_the_documented_call_and_nothing_private_leaves_in_coverage() -> None:
    transport = _logs("log_analytics_blocks.json")

    read = _read(_source(transport), HistoryCategory.BLOCKING)

    request = transport.requests[0]
    assert request.method == "POST"
    assert request.url == f"https://api.loganalytics.azure.com/v1/workspaces/{WORKSPACE}/query"
    assert request.params == ()  # the query travels in the body, never in the address
    assert request.headers["Authorization"] == "Bearer secret-bearer-value"
    assert request.json_body["timespan"] == "2026-11-09T02:00:00Z/2026-11-09T02:30:00Z"
    assert transport.credential.scope == "https://api.loganalytics.io/.default"  # type: ignore[attr-defined]
    text = json.dumps(read.coverage.to_dict())
    assert WORKSPACE not in text and "secret-bearer-value" not in text


def test_refused_access_is_unavailable_and_value_free() -> None:
    body = json.dumps({"error": {"message": f"workspace {WORKSPACE} SalesDb denied"}}).encode()
    transport = FakeTransport(lambda request: HttpResponse(403, body))

    read = _read(_source(transport), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert read.events == []
    assert WORKSPACE not in read.coverage.reason and "SalesDb" not in read.coverage.reason
    assert "Log Analytics Reader" in read.coverage.reason
    assert read.coverage.to_dict()["read_error"] == {
        "kind": "auth", "status_code": 403, "transient": False,
    }


def test_timeouts_carry_the_hash_in_query_store_form() -> None:
    read = _read(_source(_logs("log_analytics_timeouts.json")), HistoryCategory.TIMEOUT)

    first, second = read.events
    assert isinstance(first, TimeoutEvent) and isinstance(second, TimeoutEvent)
    assert first.query_hash == "0x8F6B444724D679E5" and first.error_state == 2
    assert first.query_plan_hash == "0x0000011F71FB04CB"
    assert second.query_hash is None  # the hash is "if available"; the timeout still counts


def test_errors_keep_number_severity_and_state() -> None:
    read = _read(_source(_logs("log_analytics_errors.json")), HistoryCategory.ERROR)

    event = read.events[0]
    assert isinstance(event, ErrorEvent)
    assert (event.error_number, event.severity, event.state) == (2627, 14, 1)
    assert event.user_defined is False


def test_wait_statistics_keep_the_measured_interval() -> None:
    read = _read(_source(_logs("log_analytics_wait_stats.json")), HistoryCategory.WAIT_STATS)

    lock_wait = next(event for event in read.events if isinstance(event, WaitStatsDelta)
                     and event.wait_type == "LCK_M_U")
    assert lock_wait.wait_time_ms == 184320.0 and lock_wait.signal_wait_time_ms == 12.0
    assert lock_wait.interval_start == _at("02:10") and lock_wait.interval_end == _at("02:15")


def test_deadlock_xml_is_parsed_with_the_existing_graph_parser() -> None:
    read = _read(_source(_logs("log_analytics_deadlocks.json")), HistoryCategory.DEADLOCK)

    event = read.events[0]
    assert isinstance(event, DeadlockEvent)
    assert event.detail["victim_session_id"] == "61"
    assert event.detail["objects"] == ["SalesDb.dbo.Orders"]


def test_audit_record_says_who_ran_what() -> None:
    read = _read(_source(_logs("log_analytics_audit.json")), HistoryCategory.AUDIT_STATEMENT)

    event = read.events[0]
    assert isinstance(event, AuditRecord)
    assert event.server_principal_name == "export_svc" and event.session_id == 64
    assert event.succeeded is True and event.affected_rows == 412
    assert event.statement_text is not None and "ExportedAt" in event.statement_text


def test_metrics_table_rows_become_samples() -> None:
    read = _read(_source(_logs("log_analytics_metrics.json")), HistoryCategory.RESOURCE_METRIC)

    samples = [event for event in read.events if isinstance(event, ResourceMetricSample)]
    assert len(samples) == 3
    peak = max(samples, key=lambda sample: sample.maximum or 0)
    assert (peak.metric, peak.maximum, peak.grain, peak.unit) == ("cpu_percent", 100.0, "PT1M", "Percent")


# --- Azure Monitor metrics REST API ---------------------------------------------


def _metrics_source(handler: Callable[[HttpRequest], HttpResponse]) -> tuple[AzureMonitorMetricsSource, FakeTransport]:
    transport = FakeTransport(handler)
    binding = AzureMetricsBinding.from_mapping("SalesDb", {"resource_id": RESOURCE_ID})
    source = AzureMonitorMetricsSource(
        "SalesDb",
        binding,
        transport=transport,
        tokens=EntraTokenProvider(FakeCredential(), "https://management.azure.com/.default"),
        now=lambda: NOW,
    )
    return source, transport


def _metrics_handler(data: str) -> Callable[[HttpRequest], HttpResponse]:
    def handler(request: HttpRequest) -> HttpResponse:
        timespan = dict(request.params)["timespan"]
        name = data if timespan.endswith("/2026-11-09T02:30:00Z") else "azure_metrics_list_watermark.json"
        return HttpResponse(200, _fixture(name))

    return handler


def test_metrics_response_points_without_a_value_are_not_samples() -> None:
    samples, errors = parse_metrics_response(
        json.loads(_fixture("azure_metrics_list.json")), database="SalesDb", source="metrics"
    )

    assert errors == []
    assert [(sample.metric, sample.time.minute, sample.average) for sample in samples] == [
        ("cpu_percent", 9, 41.0), ("cpu_percent", 10, 97.4), ("log_write_percent", 10, 12.0),
    ]
    assert samples[1].maximum == 100.0 and samples[1].grain == "PT1M"


def test_metrics_request_is_the_documented_list_call() -> None:
    source, transport = _metrics_source(_metrics_handler("azure_metrics_list.json"))

    read = _read(source, HistoryCategory.RESOURCE_METRIC)

    request = transport.requests[0]
    params = dict(request.params)
    assert request.method == "GET"
    assert request.url == (
        "https://management.azure.com" + RESOURCE_ID + "/providers/Microsoft.Insights/metrics"
    )
    assert params["api-version"] == "2024-02-01"
    assert params["timespan"] == "2026-11-09T02:00:00Z/2026-11-09T02:30:00Z"
    assert params["interval"] == "PT1M" and params["aggregation"] == "average,maximum,minimum"
    assert "cpu_percent" in params["metricnames"].split(",")
    assert read.coverage.status is ResultStatus.OK and len(read.events) == 3
    # The newest point with a value, 02:56: later points are empty slots.
    assert read.coverage.watermark == datetime(2026, 11, 9, 2, 56, tzinfo=timezone.utc)
    assert "00000000-0000" not in json.dumps(read.coverage.to_dict())


def test_metric_the_service_refused_is_never_reported_as_no_data() -> None:
    source, _ = _metrics_source(_metrics_handler("azure_metrics_list_metric_error.json"))

    read = _read(source, HistoryCategory.RESOURCE_METRIC, filters={"metric_names": ["workers_percent"]})

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert "workers_percent" in read.coverage.reason and "InvalidSamplingType" in read.coverage.reason


def test_metric_names_outside_the_documented_list_are_refused() -> None:
    source, transport = _metrics_source(_metrics_handler("azure_metrics_list.json"))

    with pytest.raises(ValueError, match="metric_names"):
        _read(source, HistoryCategory.RESOURCE_METRIC, filters={"metric_names": ["cpu_percent&x=1"]})
    assert transport.requests == []


def test_metrics_binding_needs_a_database_resource_id() -> None:
    with pytest.raises(ValueError, match="resource_id"):
        AzureMetricsBinding.from_mapping("SalesDb", {"resource_id": "/subscriptions/x/servers/y"})
    binding = AzureMetricsBinding.from_mapping("SalesDb", {"resource_id": RESOURCE_ID})
    assert binding.ingest_lag_minutes == 5 and binding.retention_days == 93


def test_metrics_source_serves_only_resource_metrics() -> None:
    source, transport = _metrics_source(_metrics_handler("azure_metrics_list.json"))

    read = _read(source, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.NOT_SUPPORTED and transport.requests == []
