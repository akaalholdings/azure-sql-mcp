"""Azure SQL diagnostic categories in Loki, read through a Grafana datasource proxy.

No payload here is recorded from a live Loki. The response layout is the Loki
query_range streams format. Each line is an Azure resource log record: the
envelope (time, resourceId, category, properties) is the Azure Monitor resource
log schema, and the identity fields are the ones the owner named for the work
Loki. The property names inside ``properties`` are the documented
AzureDiagnostics column names without the type suffix: derived, not recorded.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp.history_loki import LokiBinding
from azure_sql_mcp.history_loki import LokiHistorySource
from azure_sql_mcp.history_loki import LokiSettings
from azure_sql_mcp.history_loki import build_query
from azure_sql_mcp.history_loki import logql_string
from azure_sql_mcp.history_loki import parse_loki_settings
from azure_sql_mcp.history_loki import re2_quote_meta
from azure_sql_mcp.history_sources import BlockedProcessReport
from azure_sql_mcp.history_sources import DatabaseIdentity
from azure_sql_mcp.history_sources import EntraTokenProvider
from azure_sql_mcp.history_sources import ErrorEvent
from azure_sql_mcp.history_sources import HistoryCategory
from azure_sql_mcp.history_sources import HttpRequest
from azure_sql_mcp.history_sources import HttpResponse
from azure_sql_mcp.history_sources import TimeoutEvent
from azure_sql_mcp.history_sources import TimeWindow
from azure_sql_mcp.result_status import ResultStatus

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "history"
NOW = datetime(2026, 11, 9, 3, 30, tzinfo=timezone.utc)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
GRAFANA = "https://grafana.contoso.example"
SETTINGS = LokiSettings(grafana_url=GRAFANA, datasource_uid="loki-main")
IDENTITY = DatabaseIdentity("contoso-sql01", "SalesDb")


def _at(clock: str) -> datetime:
    hour, minute = (int(part) for part in clock.split(":"))
    return datetime(2026, 11, 9, hour, minute, tzinfo=timezone.utc)


def _ns(value: datetime) -> str:
    return str((value - EPOCH) // timedelta(microseconds=1) * 1000)


WINDOW = TimeWindow(_at("02:00"), _at("02:30"))


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


class FakeToken:
    token = "secret-bearer-value"
    expires_on = 4_102_444_800


class FakeCredential:
    def get_token(self, *scopes: str, **kwargs: Any) -> FakeToken:
        return FakeToken()


class FakeTransport:
    def __init__(self, handler: Callable[[HttpRequest], HttpResponse]) -> None:
        self.handler = handler
        self.requests: list[HttpRequest] = []

    async def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        return self.handler(request)


def _loki(data: str, newest: str = "loki_newest_line.json") -> FakeTransport:
    def handler(request: HttpRequest) -> HttpResponse:
        if dict(request.params)["direction"] == "backward":
            return HttpResponse(200, _fixture(newest))
        return HttpResponse(200, _fixture(data))

    return FakeTransport(handler)


def _binding(**overrides: Any) -> LokiBinding:
    value: dict[str, Any] = {
        "logical_server": "contoso-sql01",
        "database": "SalesDb",
        "categories": ["blocks", "deadlocks", "timeouts", "errors", "databasewaitstatistics"],
        "ingest_lag_minutes": 5,
    }
    value.update(overrides)
    return LokiBinding.from_mapping("SalesDb", value)


def _source(
    transport: FakeTransport,
    binding: LokiBinding | None = None,
    *,
    settings: LokiSettings = SETTINGS,
    bound: bool = True,
) -> LokiHistorySource:
    return LokiHistorySource(
        "SalesDb",
        settings,
        (binding or _binding()) if bound else None,
        transport=transport,
        tokens=EntraTokenProvider(FakeCredential(), settings.token_scope),
        now=lambda: NOW,
    )


def _read(source: LokiHistorySource, category: HistoryCategory, window: TimeWindow = WINDOW) -> Any:
    return asyncio.run(source.read(category, window))


# --- LogQL ---------------------------------------------------------------------------


def test_query_scopes_one_database_and_cannot_match_a_longer_name() -> None:
    query = build_query(SETTINGS, IDENTITY, ("blocks",))

    assert query == (
        '{service_name="mssql", category="blocks"} '
        '|~ "(?i)/SERVERS/contoso-sql01/DATABASES/SalesDb\\"" '
        '| json mcp_server="LogicalServerName", mcp_database="properties.DatabaseName" '
        '| __error__="" '
        '| mcp_server=~"(?i)^contoso-sql01$" '
        '| mcp_database=~"(?i)^SalesDb$"'
    )


def test_identity_labels_move_the_scope_into_the_selector() -> None:
    settings = LokiSettings(
        grafana_url=GRAFANA, datasource_uid="loki-main",
        identity_labels=("logical_server", "database"),
    )

    query = build_query(settings, IDENTITY, ("blocks", "errors"))

    assert query.startswith(
        '{service_name="mssql", category=~"blocks|errors", '
        'logical_server=~"(?i)^contoso-sql01$", database=~"(?i)^SalesDb$"} | json'
    )
    assert "|~" not in query


def test_names_are_escaped_for_the_regex_and_for_the_string() -> None:
    assert re2_quote_meta("Sales.Db(1)") == "Sales\\.Db\\(1\\)"
    assert logql_string('a"b\\c\n') == '"a\\"b\\\\c\\u000a"'
    query = build_query(SETTINGS, DatabaseIdentity("contoso-sql01", "Sales.Db+1"), ("blocks",))
    assert "DATABASES/Sales\\\\.Db\\\\+1" in query


def test_only_known_categories_reach_a_query() -> None:
    with pytest.raises(ValueError):
        build_query(SETTINGS, IDENTITY, ('blocks"} or {x="',))


# --- settings and policy --------------------------------------------------------------


def test_source_is_off_without_settings_and_a_partial_setting_is_named() -> None:
    off = parse_loki_settings({})
    partial = parse_loki_settings({"AZURE_SQL_LOKI_GRAFANA_URL": GRAFANA})

    assert not off.enabled and off.invalid is None
    assert not partial.enabled and partial.invalid == "AZURE_SQL_LOKI_DATASOURCE_UID"


def test_bad_setting_turns_the_source_off_without_repeating_the_value() -> None:
    settings = parse_loki_settings({
        "AZURE_SQL_LOKI_GRAFANA_URL": "http://grafana.contoso.example/?orgId=1",
        "AZURE_SQL_LOKI_DATASOURCE_UID": "loki-main",
    })

    assert not settings.enabled and settings.invalid == "AZURE_SQL_LOKI_GRAFANA_URL"
    assert "contoso" not in (settings.invalid_reason or "")


def test_settings_defaults_and_overrides() -> None:
    settings = parse_loki_settings({
        "AZURE_SQL_LOKI_GRAFANA_URL": GRAFANA + "/",
        "AZURE_SQL_LOKI_DATASOURCE_UID": "loki-main",
        "AZURE_SQL_LOKI_SELECTOR": "service_name=mssql,env=prod",
        "AZURE_SQL_LOKI_STAMP_DELAY_MINUTES": "0",
    })

    assert settings.enabled and settings.grafana_url == GRAFANA
    assert settings.selector == (("service_name", "mssql"), ("env", "prod"))
    assert settings.stamp_delay_minutes == 0 and settings.max_lines == 10_000
    assert settings.token_scope == "ce34e7e5-485f-4d76-964f-b3d2b16d1e4f/.default"
    assert parse_loki_settings({
        "AZURE_SQL_LOKI_GRAFANA_URL": GRAFANA,
        "AZURE_SQL_LOKI_DATASOURCE_UID": "loki-main",
        "AZURE_SQL_LOKI_SELECTOR": 'service_name="mssql"',
    }).invalid == "AZURE_SQL_LOKI_SELECTOR"


def test_binding_lists_only_categories_the_logs_can_answer() -> None:
    assert _binding(ingest_lag_minutes=35).ingest_lag_minutes == 35
    assert LokiBinding.from_mapping("SalesDb", {
        "logical_server": "contoso-sql01", "database": "SalesDb", "categories": ["blocks"],
    }).ingest_lag_minutes == 35
    with pytest.raises(ValueError, match="unknown category"):
        _binding(categories=["blocks", "sqlsecurityauditevents"])
    with pytest.raises(ValueError, match="unknown field"):
        _binding(tenant="x")


# --- reads -----------------------------------------------------------------------------


def test_blocks_lines_become_reports_and_another_database_is_rejected() -> None:
    read = _read(_source(_loki("loki_blocks.json")), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.OK
    assert [type(event) for event in read.events] == [BlockedProcessReport] * 2
    # The fake Loki does not run LogQL; the check on what came back does the scoping.
    assert read.coverage.rows_rejected_identity == 1
    first = read.events[0]
    assert isinstance(first, BlockedProcessReport)
    assert first.blocking.session_id == 64 and first.duration_ms == 21044.0
    assert read.coverage.schema_basis == "derived"


def test_request_goes_through_the_grafana_proxy_with_a_bearer_token_only() -> None:
    transport = _loki("loki_blocks.json")

    read = _read(_source(transport), HistoryCategory.BLOCKING)

    request = next(item for item in transport.requests if dict(item.params)["direction"] == "forward")
    assert request.method == "GET"
    assert request.url == GRAFANA + "/api/datasources/proxy/uid/loki-main/loki/api/v1/query_range"
    assert request.headers["Authorization"] == "Bearer secret-bearer-value"
    # Grafana sets the tenant. A client that sends it could read another tenant.
    assert "X-Scope-OrgID" not in request.headers
    text = json.dumps(read.coverage.to_dict())
    assert "grafana.contoso" not in text and "loki-main" not in text and "secret" not in text


def test_read_range_allows_for_lines_stamped_after_their_record_time() -> None:
    # Loki stamps the ingest time. A block at 02:29 can carry a 03:05 stamp, so the
    # read runs past the window end by the stamp delay and filters on the record time.
    transport = _loki("loki_blocks.json")
    settings = LokiSettings(grafana_url=GRAFANA, datasource_uid="loki-main", stamp_delay_minutes=40)

    _read(_source(transport, settings=settings), HistoryCategory.BLOCKING)

    params = dict(
        next(item for item in transport.requests if dict(item.params)["direction"] == "forward").params
    )
    assert params["start"] == _ns(_at("01:58"))
    assert params["end"] == _ns(_at("03:12"))  # 02:30 + 40 min + 2 min, before now


def test_quiet_window_is_empty_only_when_it_is_ingested() -> None:
    source = _source(_loki("loki_no_lines.json"))  # newest record 02:55, lag 5 min

    ingested = _read(source, HistoryCategory.BLOCKING, TimeWindow(_at("02:00"), _at("02:30")))
    recent = _read(source, HistoryCategory.BLOCKING, TimeWindow(_at("02:30"), _at("03:00")))

    assert ingested.coverage.status is ResultStatus.EMPTY
    assert recent.coverage.status is ResultStatus.UNAVAILABLE
    assert "not ingested yet" in recent.coverage.reason


def test_no_line_for_the_database_is_unavailable() -> None:
    read = _read(_source(_loki("loki_no_lines.json", "loki_no_lines.json")), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert "ingest progress is unknown" in read.coverage.reason


def test_lines_in_another_shape_are_unavailable_not_empty() -> None:
    # If the export names the report field differently, every line fails to parse.
    def handler(request: HttpRequest) -> HttpResponse:
        if dict(request.params)["direction"] == "backward":
            return HttpResponse(200, _fixture("loki_newest_line.json"))
        document = json.loads(_fixture("loki_blocks.json"))
        values = document["data"]["result"][0]["values"]
        for value in values:
            record = json.loads(value[1])
            record["properties"]["report"] = record["properties"].pop("blocked_process_filtered")
            value[1] = json.dumps(record)
        return HttpResponse(200, json.dumps(document).encode())

    read = _read(_source(FakeTransport(handler)), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert read.coverage.rows_unparsed == 2 and "none parsed" in read.coverage.reason


def test_stamp_delay_longer_than_configured_is_never_empty() -> None:
    # The newest line reached Loki 30 minutes after its record time; the setting says 10.
    settings = LokiSettings(grafana_url=GRAFANA, datasource_uid="loki-main", stamp_delay_minutes=10)

    read = _read(_source(_loki("loki_no_lines.json"), settings=settings), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert "AZURE_SQL_LOKI_STAMP_DELAY_MINUTES" in read.coverage.reason


def test_timeout_and_error_lines_parse_with_derived_property_names() -> None:
    timeout = _read(_source(_loki("loki_timeouts.json")), HistoryCategory.TIMEOUT).events[0]
    error = _read(_source(_loki("loki_errors.json")), HistoryCategory.ERROR).events[0]

    assert isinstance(timeout, TimeoutEvent) and timeout.query_hash == "0x8F6B444724D679E5"
    assert isinstance(error, ErrorEvent)
    assert (error.error_number, error.severity, error.user_defined) == (2627, 14, False)
    assert error.message is not None and error.message.startswith("Violation of PRIMARY KEY")


def _paged_source(values: list[Any], calls: list[dict[str, str]]) -> LokiHistorySource:
    """A Loki that honours start and limit, read two lines at a time."""

    document = json.loads(_fixture("loki_blocks.json"))

    def handler(request: HttpRequest) -> HttpResponse:
        params = dict(request.params)
        if params["direction"] == "backward":
            return HttpResponse(200, _fixture("loki_newest_line.json"))
        calls.append(params)
        page = [value for value in values if int(value[0]) >= int(params["start"])]
        document["data"]["result"][0]["values"] = page[: int(params["limit"])]
        return HttpResponse(200, json.dumps(document).encode())

    return LokiHistorySource(
        "SalesDb", SETTINGS, _binding(), transport=FakeTransport(handler),
        tokens=EntraTokenProvider(FakeCredential(), SETTINGS.token_scope), now=lambda: NOW,
        page_size=2,
    )


def test_pages_continue_from_the_last_stamp_and_drop_repeats() -> None:
    values = json.loads(_fixture("loki_blocks.json"))["data"]["result"][0]["values"]
    values[1][0] = str(int(values[1][0]) + 1000)  # three lines, three stamps
    calls: list[dict[str, str]] = []

    read = _read(_paged_source(values, calls), HistoryCategory.BLOCKING)

    # Page 2 starts at the last stamp of page 1 and reads that line again.
    assert [call["start"] for call in calls[1:]] == [values[1][0], values[2][0]]
    assert read.coverage.rows_read == 3 and len(read.events) == 2
    assert read.coverage.truncated_reason is None


def test_page_full_of_one_stamp_stops_the_read_and_says_so() -> None:
    # The first two lines share a stamp and fill a page: the next page cannot move on.
    values = json.loads(_fixture("loki_blocks.json"))["data"]["result"][0]["values"]
    calls: list[dict[str, str]] = []

    read = _read(_paged_source(values, calls), HistoryCategory.BLOCKING)

    assert len(calls) == 2
    assert read.coverage.truncated_reason == "equal_timestamps"
    assert read.coverage.status is ResultStatus.OK and "stopped early" in read.coverage.reason


def test_line_cap_marks_the_read_as_cut() -> None:
    settings = LokiSettings(grafana_url=GRAFANA, datasource_uid="loki-main", max_lines=1)

    read = _read(_source(_loki("loki_blocks.json"), settings=settings), HistoryCategory.BLOCKING)

    assert read.coverage.truncated_reason == "max_lines"
    assert read.coverage.rows_read == 1


# --- not configured, refused -------------------------------------------------------------


def test_source_that_is_off_names_the_settings_to_set() -> None:
    transport = _loki("loki_blocks.json")

    read = _read(_source(transport, settings=LokiSettings()), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.PRECONDITION
    assert "AZURE_SQL_LOKI_GRAFANA_URL" in (read.coverage.remediation or "")
    assert transport.requests == []


def test_invalid_setting_is_named_in_the_precondition() -> None:
    settings = parse_loki_settings({"AZURE_SQL_LOKI_GRAFANA_URL": GRAFANA})

    read = _read(_source(_loki("loki_blocks.json"), settings=settings), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.PRECONDITION
    assert "AZURE_SQL_LOKI_DATASOURCE_UID" in (read.coverage.remediation or "")


def test_unmapped_database_and_undeclared_category_are_preconditions() -> None:
    transport = _loki("loki_blocks.json")

    unmapped = _read(_source(transport, bound=False), HistoryCategory.BLOCKING)
    undeclared = _read(_source(transport, _binding(categories=["errors"])), HistoryCategory.BLOCKING)

    assert unmapped.coverage.status is ResultStatus.PRECONDITION
    assert '"loki"' in (unmapped.coverage.remediation or "")
    assert undeclared.coverage.status is ResultStatus.PRECONDITION
    assert '"blocks"' in (undeclared.coverage.remediation or "")
    assert transport.requests == []


def test_categories_the_export_does_not_carry_are_not_supported() -> None:
    read = _read(_source(_loki("loki_no_lines.json")), HistoryCategory.RESOURCE_METRIC)

    assert read.coverage.status is ResultStatus.NOT_SUPPORTED


def test_redirect_to_a_sign_in_page_is_a_refused_token_and_value_free() -> None:
    transport = FakeTransport(
        lambda request: HttpResponse(302, b'<a href="https://grafana.contoso.example/login">')
    )

    read = _read(_source(transport), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert read.coverage.to_dict()["read_error"]["kind"] == "auth"
    assert "contoso" not in read.coverage.reason and "loki-main" not in read.coverage.reason
    assert "AZURE_SQL_LOKI_TOKEN_SCOPE" in read.coverage.reason


def test_response_that_is_not_a_streams_result_is_unavailable() -> None:
    transport = FakeTransport(lambda request: HttpResponse(200, b'{"status": "error"}'))

    read = _read(_source(transport), HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert read.coverage.to_dict()["read_error"]["kind"] == "protocol"
