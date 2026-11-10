"""One question answered from several recorders at once.

The same two blocked process reports are held by an Extended Events session, by
Log Analytics and by Loki. The fixtures are the ones the adapter tests use:
built from published formats, not recorded.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

from azure_sql_mcp.history_azure_monitor import AzureMonitorMetricsSource
from azure_sql_mcp.history_azure_monitor import LogAnalyticsBinding
from azure_sql_mcp.history_azure_monitor import LogAnalyticsHistorySource
from azure_sql_mcp.history_database_watcher import DatabaseWatcherHistorySource
from azure_sql_mcp.history_extended_events import ExtendedEventsHistorySource
from azure_sql_mcp.history_loki import LokiBinding
from azure_sql_mcp.history_loki import LokiHistorySource
from azure_sql_mcp.history_loki import LokiSettings
from azure_sql_mcp.history_sources import EntraTokenProvider
from azure_sql_mcp.history_sources import HistoryCategory
from azure_sql_mcp.history_sources import HistoryResolver
from azure_sql_mcp.history_sources import HttpRequest
from azure_sql_mcp.history_sources import HttpResponse
from azure_sql_mcp.history_sources import TimeWindow

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "history"
NOW = datetime(2026, 11, 9, 3, 30, tzinfo=timezone.utc)
WINDOW = TimeWindow(
    datetime(2026, 11, 9, 2, 0, tzinfo=timezone.utc),
    datetime(2026, 11, 9, 2, 30, tzinfo=timezone.utc),
)


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


class FakeToken:
    token = "secret-bearer-value"
    expires_on = 4_102_444_800


class FakeCredential:
    def get_token(self, *scopes: str, **kwargs: Any) -> FakeToken:
        return FakeToken()


class Transport:
    def __init__(self) -> None:
        self.requests: list[HttpRequest] = []

    async def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        if "loganalytics" in request.url:
            watermark = "summarize watermark" in request.json_body["query"]
            return HttpResponse(
                200, _fixture("log_analytics_watermark.json" if watermark else "log_analytics_blocks.json")
            )
        backward = dict(request.params)["direction"] == "backward"
        return HttpResponse(200, _fixture("loki_newest_line.json" if backward else "loki_blocks.json"))


class Executor:
    async def fetch_all(self, database_name: str, query: str, params: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        if "sys.dm_xe_object_columns" in query:
            return [{"event_name": "blocked_process_report", "field_name": name,
                     "description": "The time (in microseconds)" if name == "duration" else None}
                    for name in ("blocked_process", "duration", "lock_mode")]
        # The one running session records blocked process reports only.
        if "sys.dm_xe_database_session_targets" in query and "blocked_process_report" in query:
            return [{"session_name": "blocking_capture",
                     "session_start_utc": datetime(2026, 11, 9, 1, 0),
                     "target_name": "ring_buffer",
                     "target_data": _fixture("xe_ring_buffer_blocking.xml").decode()}]
        return []


def _sources(transport: Transport) -> list[Any]:
    tokens = EntraTokenProvider(FakeCredential(), "https://example.invalid/.default")
    clock = lambda: NOW  # noqa: E731
    return [
        ExtendedEventsHistorySource("SalesDb", Executor(), now=clock),  # type: ignore[arg-type]
        LogAnalyticsHistorySource(
            "SalesDb",
            LogAnalyticsBinding.from_mapping("SalesDb", {
                "workspace_id": "11111111-2222-3333-4444-555555555555",
                "logical_server": "contoso-sql01", "database": "SalesDb", "categories": ["blocks"],
            }),
            transport=transport, tokens=tokens, now=clock,
        ),
        LokiHistorySource(
            "SalesDb",
            LokiSettings(grafana_url="https://grafana.contoso.example", datasource_uid="loki-main"),
            LokiBinding.from_mapping("SalesDb", {
                "logical_server": "contoso-sql01", "database": "SalesDb", "categories": ["blocks"],
                "ingest_lag_minutes": 5,
            }),
            transport=transport, tokens=tokens, now=clock,
        ),
        # Not mapped for this database: each says what to set, and is never called.
        DatabaseWatcherHistorySource("SalesDb", None, transport=transport, tokens=tokens, now=clock),
        AzureMonitorMetricsSource("SalesDb", None, transport=transport, tokens=tokens, now=clock),
    ]


def test_three_recorders_of_one_blocking_chain_give_one_episode() -> None:
    transport = Transport()
    resolver = HistoryResolver(_sources(transport), database_name="SalesDb", now=lambda: NOW)

    payload = asyncio.run(resolver.read(HistoryCategory.BLOCKING, WINDOW)).to_payload()

    assert payload["result_status"] == "ok" and payload["event_count"] == 1
    episode = payload["events"][0]
    # Two victims reported once each, whichever recorder is asked.
    assert episode["report_count"] == 2
    assert episode["sources"] == ["extended_events", "log_analytics", "loki"]
    assert episode["head_blocker"]["session_id"] == 64
    assert episode["max_wait_ms"] == 21044
    statuses = {item["source"]: item["status"] for item in payload["coverage"]}
    assert statuses == {
        "extended_events": "ok", "log_analytics": "ok", "loki": "ok",
        "database_watcher": "not_supported", "azure_monitor_metrics": "not_supported",
    }


def test_question_no_mapped_recorder_holds_lists_what_to_enable() -> None:
    transport = Transport()
    resolver = HistoryResolver(_sources(transport), database_name="SalesDb", now=lambda: NOW)

    payload = asyncio.run(resolver.read(HistoryCategory.RESOURCE_METRIC, WINDOW)).to_payload()

    assert payload["result_status"] == "precondition" and payload["events"] == []
    remediation = payload["remediation"]
    assert '"azure_monitor_metrics"' in remediation and '"database_watcher"' in remediation
    assert '"metrics"' in remediation  # the Log Analytics category that is not declared
    assert transport.requests == []


def test_describe_lists_each_recorder_with_its_lag_and_newest_record() -> None:
    resolver = HistoryResolver(_sources(Transport()), database_name="SalesDb", now=lambda: NOW)

    described = {item.source: item.to_dict() for item in asyncio.run(resolver.describe())}

    assert described["log_analytics"]["categories"] == ["blocking"]
    assert described["log_analytics"]["expected_ingest_lag_seconds"] == 900.0
    assert described["log_analytics"]["watermark_utc"] == "2026-11-09T02:55:00Z"
    assert described["loki"]["watermark_utc"] == "2026-11-09T02:55:00Z"
    assert described["extended_events"]["categories"] == ["blocking"]
    assert described["database_watcher"]["configured"] is False
