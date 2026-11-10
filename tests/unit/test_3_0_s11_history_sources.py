"""History interface: status rules, merge without double counting, privacy.

Fixtures under tests/fixtures/history are built from published schemas, not
recorded from a live service. The blocked process report files follow the XML
the engine writes (monitorLoop, blocked-process, blocking-process).
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp.history_sources import BlockedProcessReport
from azure_sql_mcp.history_sources import Coverage
from azure_sql_mcp.history_sources import EntraTokenProvider
from azure_sql_mcp.history_sources import ErrorEvent
from azure_sql_mcp.history_sources import HistoryCategory
from azure_sql_mcp.history_sources import HistoryReadError
from azure_sql_mcp.history_sources import HistoryResolver
from azure_sql_mcp.history_sources import HttpRequest
from azure_sql_mcp.history_sources import HttpResponse
from azure_sql_mcp.history_sources import ReadFacts
from azure_sql_mcp.history_sources import SourceDescription
from azure_sql_mcp.history_sources import SourceRead
from azure_sql_mcp.history_sources import TimeoutEvent
from azure_sql_mcp.history_sources import TimeWindow
from azure_sql_mcp.history_sources import UrllibTransport
from azure_sql_mcp.history_sources import build_blocking_episodes
from azure_sql_mcp.history_sources import decide_status
from azure_sql_mcp.history_sources import normalize_query_hash
from azure_sql_mcp.history_sources import parse_blocked_process_report
from azure_sql_mcp.history_sources import parse_utc
from azure_sql_mcp.history_sources import raise_for_status
from azure_sql_mcp.history_sources import settle_read
from azure_sql_mcp.result_status import ResultStatus

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "history"
NOW = datetime(2026, 11, 9, 3, 0, tzinfo=timezone.utc)


def _at(clock: str) -> datetime:
    hour, minute, *second = (int(part) for part in clock.split(":"))
    return datetime(2026, 11, 9, hour, minute, second[0] if second else 0, tzinfo=timezone.utc)


WINDOW = TimeWindow(_at("02:00"), _at("02:30"))


def _facts(**overrides: Any) -> ReadFacts:
    values: dict[str, Any] = {
        "category": HistoryCategory.BLOCKING,
        "window": WINDOW,
        "now": NOW,
        "event_count": 0,
        "watermark": _at("02:55"),
        "ingest_lag": timedelta(minutes=10),
    }
    values.update(overrides)
    return ReadFacts(**values)


def _report(name: str, clock: str, source: str = "xe") -> BlockedProcessReport:
    report = parse_blocked_process_report(
        (FIXTURES / name).read_text(),
        time=_at(clock),
        database="SalesDb",
        source=source,
    )
    assert report is not None
    return report


# --- status rules -------------------------------------------------------------


def test_window_newer_than_watermark_minus_lag_is_not_ingested_never_empty() -> None:
    # During an incident the newest minutes are not in the store yet. "No
    # blocking" for them would send the DBA away from the real cause.
    status, reason = decide_status(_facts(watermark=_at("02:35")))

    assert status is ResultStatus.UNAVAILABLE
    assert "not ingested yet" in reason
    assert "2026-11-09T02:25:00Z" in reason  # watermark 02:35 minus the 10 min lag


def test_fully_ingested_window_without_events_is_a_true_negative() -> None:
    status, reason = decide_status(_facts(watermark=_at("02:40")))

    assert status is ResultStatus.EMPTY
    assert "true negative" in reason


def test_no_record_seen_for_the_database_is_unavailable() -> None:
    # A stopped pipeline, a wrong mapping and a quiet database look the same.
    status, reason = decide_status(_facts(watermark=None))

    assert status is ResultStatus.UNAVAILABLE
    assert "ingest progress is unknown" in reason


def test_rows_that_do_not_parse_are_unavailable_not_empty() -> None:
    status, reason = decide_status(_facts(rows_read=4, rows_accepted=0))

    assert status is ResultStatus.UNAVAILABLE
    assert "none parsed" in reason


def test_truncated_read_without_events_is_unavailable() -> None:
    status, _ = decide_status(_facts(truncated_reason="max_rows"))

    assert status is ResultStatus.UNAVAILABLE


def test_window_that_starts_before_retention_is_unavailable() -> None:
    status, reason = decide_status(_facts(retention_floor=_at("02:10")))

    assert status is ResultStatus.UNAVAILABLE
    assert "retention" in reason


def test_events_with_an_uningested_tail_are_ok_and_name_the_gap() -> None:
    status, reason = decide_status(_facts(event_count=2, watermark=_at("02:30")))

    assert status is ResultStatus.OK
    assert "2026-11-09T02:20:00Z" in reason and "not ingested yet" in reason


def test_read_failure_wins_over_everything() -> None:
    error = HistoryReadError("auth", "The token was refused (401).", status_code=401)

    status, reason = decide_status(_facts(event_count=3, read_error=error))

    assert status is ResultStatus.UNAVAILABLE
    assert "401" in reason


def test_settle_read_drops_duplicates_and_rows_outside_the_window() -> None:
    inside = TimeoutEvent(time=_at("02:10"), database="SalesDb", source="logs", query_hash="0x01")
    outside = TimeoutEvent(time=_at("02:45"), database="SalesDb", source="logs", query_hash="0x02")

    read = settle_read(
        source="logs",
        family="azure_diagnostics",
        category=HistoryCategory.TIMEOUT,
        window=WINDOW,
        now=NOW,
        events=[inside, inside, outside],
        rows_read=3,
        watermark=_at("02:50"),
        ingest_lag=timedelta(minutes=10),
    )

    assert [event.time for event in read.events] == [_at("02:10")]
    assert read.coverage.duplicates_dropped == 1
    assert read.coverage.rows_outside_window == 1
    assert read.coverage.status is ResultStatus.OK


# --- blocked process reports and episodes -------------------------------------


def test_blocked_process_report_keeps_blocker_victim_and_statement_references() -> None:
    report = _report("blocked_process_report_victim71.xml", "02:10:05")

    assert report.monitor_loop == 48213
    assert report.blocked.session_id == 71 and report.blocking.session_id == 64
    assert report.wait_time_ms == 21044 and report.lock_mode == "U"
    assert report.wait_resource == "KEY: 5:72057594045661184 (8194443284a0)"
    assert report.blocked.statement_text == "UPDATE dbo.Orders SET Status = 3 WHERE OrderId = 48211"
    assert report.blocked.frames[0]["statement_start"] == 118
    assert report.blocked.frames[0]["sql_handle"].startswith("0x0300050011a2b33c")
    assert report.blocking.status == "sleeping" and report.blocking.transaction_count == 1


def test_malformed_report_xml_is_not_an_event() -> None:
    assert parse_blocked_process_report(
        "<blocked-process-report><blocked-process>", time=NOW, database="SalesDb", source="xe"
    ) is None
    assert parse_blocked_process_report(
        "<blocked-process-report/>", time=NOW, database="SalesDb", source="xe"
    ) is None


def test_chain_names_the_head_blocker_not_the_middle_session() -> None:
    # 64 blocks 71, 71 blocks 83. The session to fix is 64; 71 is a victim.
    reports = [
        _report("blocked_process_report_victim71.xml", "02:10:05"),
        _report("blocked_process_report_victim83.xml", "02:10:05"),
    ]

    episodes = build_blocking_episodes(reports)

    assert len(episodes) == 1
    episode = episodes[0]
    assert episode.head_blocker.session_id == 64
    assert sorted(victim.session_id for victim in episode.victims) == [71, 83]
    assert episode.max_chain_depth == 2
    assert episode.max_wait_ms == 21044
    # A sleeping session with an open transaction: the fix is in the code path.
    assert episode.head_blocker_idle_in_transaction is True


def test_consecutive_reports_are_one_episode_and_a_late_one_is_another() -> None:
    first = _report("blocked_process_report_victim71.xml", "02:10:05")
    second = _report("blocked_process_report_victim71.xml", "02:10:25")
    later = _report("blocked_process_report_victim71.xml", "02:20:00")

    episodes = build_blocking_episodes([later, first, second])

    assert [(episode.first_seen, episode.last_seen, episode.report_count) for episode in episodes] == [
        (_at("02:10:05"), _at("02:10:25"), 2),
        (_at("02:20:00"), _at("02:20:00"), 1),
    ]


# --- small parsers -------------------------------------------------------------


def test_query_hash_decimal_becomes_the_query_store_binary_form() -> None:
    # Microsoft's rule: CAST(CAST(<hash_value> AS bigint) AS binary(8)).
    assert normalize_query_hash("-8112315231426414107") == "0x8F6B444724D679E5"
    assert normalize_query_hash(1234567890123) == "0x0000011F71FB04CB"
    assert normalize_query_hash("0x8f6b444724d679e5") == "0x8F6B444724D679E5"
    assert normalize_query_hash("0x1F") == "0x000000000000001F"
    assert normalize_query_hash("") is None and normalize_query_hash("not-a-hash") is None


def test_parse_utc_reads_seven_fraction_digits_and_assumes_utc() -> None:
    assert parse_utc("2026-11-09T02:10:05.1234567Z") == datetime(
        2026, 11, 9, 2, 10, 5, 123456, tzinfo=timezone.utc
    )
    assert parse_utc("2026-11-09 02:10:05") == _at("02:10:05")
    assert parse_utc("yesterday") is None


# --- resolver ------------------------------------------------------------------


class FakeSource:
    def __init__(
        self,
        name: str,
        family: str,
        result: SourceRead | None = None,
        *,
        gate: Coverage | None = None,
    ) -> None:
        self.name = name
        self.family = family
        self.database_name = "SalesDb"
        self._result = result
        self._gate = gate
        self.reads = 0

    def gate(self, category: HistoryCategory, window: TimeWindow) -> Coverage | None:
        return self._gate

    async def describe(self) -> SourceDescription:
        return SourceDescription(self.name, self.family, self.database_name, True, ())

    async def read(
        self,
        category: HistoryCategory,
        window: TimeWindow,
        filters: Mapping[str, Any] | None = None,
        limit: int = 200,
    ) -> SourceRead:
        self.reads += 1
        assert self._result is not None
        return self._result


def _read(
    name: str,
    family: str,
    category: HistoryCategory,
    events: list[Any],
    *,
    watermark: datetime | None = None,
) -> SourceRead:
    return settle_read(
        source=name,
        family=family,
        category=category,
        window=WINDOW,
        now=NOW,
        events=events,
        rows_read=len(events),
        watermark=watermark or _at("02:55"),
        ingest_lag=timedelta(minutes=10),
    )


def _resolve(sources: list[FakeSource], category: HistoryCategory, **kwargs: Any) -> dict[str, Any]:
    resolver = HistoryResolver(sources, database_name="SalesDb", now=lambda: NOW)
    result = asyncio.run(resolver.read(category, WINDOW, **kwargs))
    return result.to_payload("full")


def test_same_blocked_report_from_two_recorders_is_counted_once() -> None:
    # The diagnostic export and an XE session both record the same report. An
    # episode that says "2 reports" for one would double the blocking seen.
    xe = FakeSource(
        "extended_events",
        "extended_events",
        _read("extended_events", "extended_events", HistoryCategory.BLOCKING,
              [_report("blocked_process_report_victim71.xml", "02:10:05", "extended_events")]),
    )
    logs = FakeSource(
        "log_analytics",
        "azure_diagnostics",
        _read("log_analytics", "azure_diagnostics", HistoryCategory.BLOCKING,
              [_report("blocked_process_report_victim71.xml", "02:10:09", "log_analytics")]),
    )

    payload = _resolve([xe, logs], HistoryCategory.BLOCKING)

    assert payload["result_status"] == "ok"
    assert payload["event_count"] == 1
    episode = payload["events"][0]
    assert episode["report_count"] == 1
    assert episode["sources"] == ["extended_events", "log_analytics"]


def test_timeouts_from_recorders_that_cannot_be_matched_are_not_added_together() -> None:
    # A diagnostic Timeouts record has a hash and no session; an XE attention has
    # a session and no hash. Summing them reports each timeout twice.
    xe = FakeSource(
        "extended_events",
        "extended_events",
        _read("extended_events", "extended_events", HistoryCategory.TIMEOUT, [
            TimeoutEvent(time=_at("02:10"), database="SalesDb", source="extended_events",
                         kind="attention", session_id=71),
        ]),
    )
    logs = FakeSource(
        "log_analytics",
        "azure_diagnostics",
        _read("log_analytics", "azure_diagnostics", HistoryCategory.TIMEOUT, [
            TimeoutEvent(time=_at("02:10"), database="SalesDb", source="log_analytics",
                         query_hash="0x8F6B4C1D0A3E25E5"),
        ]),
    )

    payload = _resolve([xe, logs], HistoryCategory.TIMEOUT)

    assert payload["event_count"] == 1
    assert payload["events"][0]["source"] == "extended_events"
    by_source = {item["source"]: item for item in payload["coverage"]}
    assert by_source["extended_events"]["merged"] is True
    assert by_source["log_analytics"]["merged"] is False
    assert "double count" in by_source["log_analytics"]["not_merged_reason"]


def test_no_configured_source_is_a_precondition_with_the_setting_to_enable() -> None:
    gate = Coverage.gated(
        "log_analytics", "azure_diagnostics", HistoryCategory.BLOCKING, WINDOW,
        ResultStatus.PRECONDITION,
        "No Log Analytics workspace is mapped for this database.",
        remediation='Add "log_analytics": {"workspace_id": "<workspace GUID>"} to the policy.',
    )
    source = FakeSource("log_analytics", "azure_diagnostics", gate=gate)

    payload = _resolve([source], HistoryCategory.BLOCKING)

    assert payload["result_status"] == "precondition"
    assert "workspace_id" in payload["remediation"]
    assert source.reads == 0  # a source that is not configured is never called


def test_no_source_at_all_is_a_precondition_not_an_all_clear() -> None:
    payload = _resolve([], HistoryCategory.BLOCKING)

    assert payload["result_status"] == "precondition"
    assert payload["events"] == []


def test_one_true_negative_makes_the_answer_empty_even_when_another_source_failed() -> None:
    covered = FakeSource(
        "log_analytics", "azure_diagnostics",
        _read("log_analytics", "azure_diagnostics", HistoryCategory.DEADLOCK, []),
    )
    lagging = FakeSource(
        "loki", "azure_diagnostics",
        _read("loki", "azure_diagnostics", HistoryCategory.DEADLOCK, [], watermark=_at("02:20")),
    )

    payload = _resolve([lagging, covered], HistoryCategory.DEADLOCK)

    assert payload["result_status"] == "empty"
    assert "log_analytics" in payload["result_status_reason"]


def test_only_source_not_ingested_keeps_the_answer_unavailable() -> None:
    lagging = FakeSource(
        "loki", "azure_diagnostics",
        _read("loki", "azure_diagnostics", HistoryCategory.DEADLOCK, [], watermark=_at("02:20")),
    )

    payload = _resolve([lagging], HistoryCategory.DEADLOCK)

    assert payload["result_status"] == "unavailable"
    assert "not ingested yet" in payload["result_status_reason"]


def test_limit_cuts_oldest_first_and_says_where_to_continue() -> None:
    events = [
        TimeoutEvent(time=_at(f"02:{minute:02d}"), database="SalesDb", source="logs",
                     query_hash=f"0x{minute:016X}")
        for minute in (5, 10, 15)
    ]
    source = FakeSource("logs", "azure_diagnostics",
                        _read("logs", "azure_diagnostics", HistoryCategory.TIMEOUT, events))

    payload = _resolve([source], HistoryCategory.TIMEOUT, limit=2)

    assert [event["time"] for event in payload["events"]] == [
        "2026-11-09T02:05:00Z", "2026-11-09T02:10:00Z",
    ]
    assert payload["truncated"] is True
    assert payload["next_window_start_utc"] == "2026-11-09T02:15:00Z"


# --- privacy --------------------------------------------------------------------


def _blocking_payload(level: str | None) -> dict[str, Any]:
    source = FakeSource(
        "extended_events", "extended_events",
        _read("extended_events", "extended_events", HistoryCategory.BLOCKING,
              [_report("blocked_process_report_victim71.xml", "02:10:05", "extended_events")]),
    )
    resolver = HistoryResolver([source], database_name="SalesDb", now=lambda: NOW)
    result = asyncio.run(resolver.read(HistoryCategory.BLOCKING, WINDOW))
    return result.to_payload() if level is None else result.to_payload(level)


def test_statement_text_is_masked_unless_the_server_asks_for_full_detail() -> None:
    masked = _blocking_payload(None)["events"][0]
    assert masked["head_blocker"]["statement_text"] == (
        "UPDATE dbo.Orders SET ExportedAt = '?' WHERE CustomerId = ?"
    )
    assert "9912" not in str(masked) and "48211" not in str(masked)

    withheld = _blocking_payload("metadata_only")["events"][0]
    assert withheld["head_blocker"]["statement_text"].startswith("[withheld")
    assert withheld["head_blocker"]["session_id"] == 64  # who blocked is still named

    full = _blocking_payload("full")["events"][0]
    assert "9912" in full["head_blocker"]["statement_text"]


def test_error_message_values_are_removed_below_full_detail() -> None:
    event = ErrorEvent(
        time=_at("02:10"), database="SalesDb", source="logs", error_number=2627, severity=14,
        message="Violation of PRIMARY KEY constraint 'PK_Orders'. The duplicate key value is (48211).",
    )
    source = FakeSource("logs", "azure_diagnostics",
                        _read("logs", "azure_diagnostics", HistoryCategory.ERROR, [event]))
    resolver = HistoryResolver([source], database_name="SalesDb", now=lambda: NOW)
    result = asyncio.run(resolver.read(HistoryCategory.ERROR, WINDOW))

    assert "48211" not in result.to_payload()["events"][0]["message"]
    assert result.to_payload()["events"][0]["error_number"] == 2627
    assert "48211" in result.to_payload("full")["events"][0]["message"]
    assert result.to_payload("metadata_only")["events"][0]["message"].startswith("[withheld")


# --- transport -------------------------------------------------------------------


class FakeToken:
    def __init__(self, token: str, expires_on: int) -> None:
        self.token = token
        self.expires_on = expires_on


class FakeCredential:
    def __init__(self, lifetime: int = 3600, fail: bool = False) -> None:
        self.calls: list[str] = []
        self.lifetime = lifetime
        self.fail = fail
        self.clock = 1_000_000.0

    def get_token(self, *scopes: str, **kwargs: Any) -> FakeToken:
        self.calls.append(scopes[0])
        if self.fail:
            raise RuntimeError("AADSTS700016: secret-tenant-detail")
        return FakeToken(f"token-{len(self.calls)}", int(self.clock) + self.lifetime)


def test_token_is_cached_and_renewed_before_it_expires() -> None:
    credential = FakeCredential()
    provider = EntraTokenProvider(credential, "https://example.invalid/.default",
                                  clock=lambda: credential.clock)

    async def run() -> list[str]:
        first = await provider.bearer_token()
        second = await provider.bearer_token()
        credential.clock += 3600 - 200  # inside the 5 minute renewal margin
        third = await provider.bearer_token()
        return [first, second, third]

    assert asyncio.run(run()) == ["token-1", "token-1", "token-2"]
    assert credential.calls == ["https://example.invalid/.default"] * 2


def test_token_failure_is_value_free() -> None:
    provider = EntraTokenProvider(FakeCredential(fail=True), "https://example.invalid/.default")

    with pytest.raises(HistoryReadError) as caught:
        asyncio.run(provider.bearer_token())

    assert caught.value.kind == "auth"
    assert "secret-tenant-detail" not in str(caught.value)
    assert caught.value.__cause__ is None


def test_http_errors_never_repeat_the_response_body_or_address() -> None:
    body = b'{"error": "query {db=\\"SalesDb\\"} at https://grafana.contoso.example/x?token=abc"}'

    with pytest.raises(HistoryReadError) as caught:
        raise_for_status(HttpResponse(403, body), service="Log Analytics")

    text = str(caught.value)
    assert caught.value.status_code == 403 and caught.value.kind == "auth"
    assert "SalesDb" not in text and "contoso" not in text and "token" not in text.lower().replace(
        "the token", ""
    )


def test_redirect_is_a_refused_token_and_throttling_is_transient() -> None:
    with pytest.raises(HistoryReadError) as redirect:
        raise_for_status(HttpResponse(302, b""), service="Grafana")
    with pytest.raises(HistoryReadError) as throttled:
        raise_for_status(HttpResponse(429, b""), service="Grafana")

    assert redirect.value.kind == "auth" and not redirect.value.transient
    assert throttled.value.transient is True
    raise_for_status(HttpResponse(200, b"{}"), service="Grafana")


def test_default_transport_refuses_plain_http() -> None:
    # A bearer token must never travel without TLS.
    request = HttpRequest("GET", "http://grafana.contoso.example/api")

    with pytest.raises(HistoryReadError) as caught:
        asyncio.run(UrllibTransport().send(request))

    assert caught.value.kind == "config"
    assert "contoso" not in str(caught.value)


def test_the_source_ingested_furthest_answers_when_two_cannot_be_merged() -> None:
    # Loki is 35 minutes behind and holds the first timeout only. Returning it
    # because it is listed first would hide the second one.
    behind = FakeSource(
        "loki", "azure_diagnostics",
        _read("loki", "azure_diagnostics", HistoryCategory.TIMEOUT, [
            TimeoutEvent(time=_at("02:05"), database="SalesDb", source="loki", query_hash="0x01"),
        ], watermark=_at("02:20")),
    )
    current = FakeSource(
        "log_analytics", "azure_diagnostics",
        _read("log_analytics", "azure_diagnostics", HistoryCategory.TIMEOUT, [
            TimeoutEvent(time=_at("02:05"), database="SalesDb", source="log_analytics",
                         query_hash="0x01"),
            TimeoutEvent(time=_at("02:25"), database="SalesDb", source="log_analytics",
                         query_hash="0x02"),
        ]),
    )

    payload = _resolve([behind, current], HistoryCategory.TIMEOUT)

    assert payload["event_count"] == 2
    assert {event["source"] for event in payload["events"]} == {"log_analytics"}


def test_history_credential_follows_the_server_auth_mode() -> None:
    from types import SimpleNamespace

    from azure.identity import ClientSecretCredential
    from azure.identity import DefaultAzureCredential

    from azure_sql_mcp.config import AuthMode
    from azure_sql_mcp.history_sources import credential_for

    placeholder = "x" * 12
    principal = SimpleNamespace(
        auth_mode=AuthMode.SERVICE_PRINCIPAL,
        tenant_id="00000000-0000-0000-0000-000000000001",
        client_id="00000000-0000-0000-0000-000000000002",
    )
    principal.client_secret = placeholder
    # A SQL login has no Entra identity; the history stores still need one.
    sql_login = SimpleNamespace(auth_mode=AuthMode.SQL_PASSWORD)

    assert isinstance(credential_for(principal), ClientSecretCredential)  # type: ignore[arg-type]
    assert isinstance(credential_for(sql_login), DefaultAzureCredential)  # type: ignore[arg-type]
