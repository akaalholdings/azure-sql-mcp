"""History from existing database-scoped Extended Events sessions.

No payload here is recorded from a live database. The ring_buffer fixtures use
the target layout Microsoft documents (RingBufferTarget with truncated,
totalEventsProcessed, eventCount and droppedCount; event with name and
timestamp; data and action children). Event field names (blocked_process,
duration, batch_text and so on) are not in the public reference: the adapter
confirms them against sys.dm_xe_object_columns before it parses, and these
tests supply that catalogue.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp.history_extended_events import ExtendedEventsHistorySource
from azure_sql_mcp.history_extended_events import catalog_from_rows
from azure_sql_mcp.history_extended_events import read_ring_buffer_target
from azure_sql_mcp.history_sources import BlockedProcessReport
from azure_sql_mcp.history_sources import CompletedStatement
from azure_sql_mcp.history_sources import DeadlockEvent
from azure_sql_mcp.history_sources import HistoryCategory
from azure_sql_mcp.history_sources import HistoryResolver
from azure_sql_mcp.history_sources import TimeoutEvent
from azure_sql_mcp.history_sources import TimeWindow
from azure_sql_mcp.result_status import ResultStatus

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "history"
NOW = datetime(2026, 11, 9, 3, 0, tzinfo=timezone.utc)
MICROSECONDS = "The time (in microseconds) that the event took."
BLOB_PREFIX = "https://contosoxe.blob.core.windows.net/xe/blocking"


def _at(clock: str) -> datetime:
    hour, minute = (int(part) for part in clock.split(":"))
    return datetime(2026, 11, 9, hour, minute, tzinfo=timezone.utc)


WINDOW = TimeWindow(_at("02:00"), _at("02:30"))


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def _catalog_rows(**overrides: Any) -> list[dict[str, Any]]:
    fields = {
        "blocked_process_report": ["blocked_process", "duration", "lock_mode", "resource_owner_type"],
        "attention": ["duration", "request_id"],
        "sql_batch_completed": ["duration", "cpu_time", "logical_reads", "physical_reads",
                                "writes", "row_count", "batch_text"],
        "rpc_completed": ["duration", "cpu_time", "logical_reads", "physical_reads", "writes",
                          "row_count", "statement", "object_name"],
        "sql_statement_completed": ["duration", "cpu_time", "statement"],
        "sp_statement_completed": ["duration", "cpu_time", "statement"],
        "database_xml_deadlock_report": ["xml_report"],
    }
    fields.update(overrides)
    return [
        {
            "event_name": event,
            "field_name": name,
            "description": MICROSECONDS if name in {"duration", "cpu_time"} else None,
        }
        for event, names in fields.items()
        for name in names
    ]


class FakeExecutor:
    def __init__(
        self,
        *,
        targets: Sequence[dict[str, Any]] = (),
        catalog: Sequence[dict[str, Any]] | None = None,
        defined: Sequence[dict[str, Any]] = (),
        file_targets: Sequence[dict[str, Any]] = (),
        file_events: Sequence[dict[str, Any]] = (),
        file_bounds: Sequence[dict[str, Any]] = (),
        fail_on: str | None = None,
    ) -> None:
        self.targets = list(targets)
        self.catalog = _catalog_rows() if catalog is None else list(catalog)
        self.defined = list(defined)
        self.file_targets = list(file_targets)
        self.file_events = list(file_events)
        self.file_bounds = list(file_bounds)
        self.fail_on = fail_on
        self.calls: list[tuple[str, str, list[Any]]] = []

    async def fetch_all(
        self, database_name: str, query: str, params: Sequence[Any] | None = None, **kwargs: Any
    ) -> list[dict[str, Any]]:
        self.calls.append((database_name, query, list(params or [])))
        if self.fail_on and self.fail_on in query:
            raise RuntimeError(
                "VIEW DATABASE STATE permission denied on server 'contoso-sql01.database.windows.net'"
            )
        if "sys.dm_xe_object_columns" in query:
            return self.catalog
        if "sys.dm_xe_database_session_targets" in query:
            return self.targets
        if "sys.fn_xe_file_target_read_file" in query:
            return self.file_bounds if "MIN(" in query else self.file_events
        if "sys.database_event_session_fields" in query:
            return self.file_targets
        if "sys.database_event_sessions" in query:
            return self.defined
        raise AssertionError(f"unexpected query: {query}")

    def queries(self, fragment: str) -> list[tuple[str, str, list[Any]]]:
        return [call for call in self.calls if fragment in call[1]]


def _ring(fixture: str, *, started: str = "01:00", session: str = "blocking_capture") -> dict[str, Any]:
    return {
        "session_name": session,
        "session_start_utc": _at(started).replace(tzinfo=None),
        "target_name": "ring_buffer",
        "target_data": _fixture(fixture),
    }


def _source(executor: FakeExecutor) -> ExtendedEventsHistorySource:
    return ExtendedEventsHistorySource("SalesDb", executor, now=lambda: NOW)  # type: ignore[arg-type]


def _read(executor: FakeExecutor, category: HistoryCategory, window: TimeWindow = WINDOW, **kwargs: Any) -> Any:
    return asyncio.run(_source(executor).read(category, window, **kwargs))


# --- ring buffer -------------------------------------------------------------------


def test_blocked_process_reports_are_read_from_a_running_session() -> None:
    executor = FakeExecutor(targets=[_ring("xe_ring_buffer_blocking.xml")])

    read = _read(executor, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.OK
    first, second = read.events
    assert isinstance(first, BlockedProcessReport) and isinstance(second, BlockedProcessReport)
    assert (first.blocked.session_id, first.blocking.session_id) == (71, 64)
    assert first.duration_ms == 21044.0 and first.lock_mode == "U"
    assert first.resource_owner_type == "LOCK"
    assert read.coverage.schema_basis == "engine_catalog"


def test_session_reports_resolve_to_the_head_blocker_episode() -> None:
    executor = FakeExecutor(targets=[_ring("xe_ring_buffer_blocking.xml")])
    resolver = HistoryResolver([_source(executor)], database_name="SalesDb", now=lambda: NOW)

    payload = asyncio.run(resolver.read(HistoryCategory.BLOCKING, WINDOW)).to_payload()

    episode = payload["events"][0]
    assert payload["event_count"] == 1 and episode["head_blocker"]["session_id"] == 64
    assert sorted(victim["session_id"] for victim in episode["victims"]) == [71, 83]


def test_quiet_session_that_ran_for_the_whole_window_is_a_true_negative() -> None:
    executor = FakeExecutor(targets=[_ring("xe_ring_buffer_empty.xml", started="01:00")])

    read = _read(executor, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.EMPTY


def test_session_started_inside_the_window_cannot_vouch_for_the_time_before_it() -> None:
    # A failover or a restart empties the ring buffer and restarts the session.
    executor = FakeExecutor(targets=[_ring("xe_ring_buffer_empty.xml", started="02:20")])

    read = _read(executor, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert "retention" in read.coverage.reason
    assert read.coverage.to_dict()["retention_floor_utc"] == "2026-11-09T02:20:00Z"


def test_rolled_ring_buffer_covers_only_what_it_still_holds() -> None:
    # 60 events passed through; the oldest one kept is from 02:25.
    executor = FakeExecutor(targets=[_ring("xe_ring_buffer_rolled.xml", started="01:00")])

    read = _read(executor, HistoryCategory.BLOCKING, TimeWindow(_at("02:00"), _at("02:20")))

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert read.coverage.to_dict()["retention_floor_utc"] == "2026-11-09T02:25:00Z"


def test_ring_buffer_that_dropped_events_is_never_empty() -> None:
    executor = FakeExecutor(targets=[_ring("xe_ring_buffer_dropped.xml")])

    read = _read(executor, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert "dropped 3" in read.coverage.reason


def test_report_without_its_xml_is_unparsed_not_missing() -> None:
    executor = FakeExecutor(targets=[_ring("xe_ring_buffer_unreadable.xml")])

    read = _read(executor, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert read.coverage.rows_unparsed == 1 and "none parsed" in read.coverage.reason


def test_window_inside_the_dispatch_latency_is_not_ingested_yet() -> None:
    executor = FakeExecutor(targets=[_ring("xe_ring_buffer_empty.xml")])

    read = _read(executor, HistoryCategory.BLOCKING, TimeWindow(_at("02:30"), NOW))

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert "not ingested yet" in read.coverage.reason


def test_ring_buffer_reader_reports_loss_and_roll_over() -> None:
    rolled = read_ring_buffer_target(_fixture("xe_ring_buffer_rolled.xml"), {"blocked_process_report"})
    cut = read_ring_buffer_target("<RingBufferTarget truncated=", {"blocked_process_report"})

    assert rolled.rolled is True and rolled.oldest == _at("02:25") and rolled.loss == []
    assert cut.events == [] and cut.loss == ["a ring_buffer document could not be parsed"]


# --- attention and completed statements ------------------------------------------------


def test_attention_is_a_client_timeout_with_its_session_and_statement() -> None:
    executor = FakeExecutor(targets=[_ring("xe_ring_buffer_statements.xml", session="slow_capture")])

    read = _read(executor, HistoryCategory.TIMEOUT)

    (event,) = read.events
    assert isinstance(event, TimeoutEvent)
    assert event.kind == "attention" and event.session_id == 71
    assert event.duration_ms == pytest.approx(30000.412)
    assert event.client_app == "orders-api" and event.login_name == "orders_app"
    assert event.statement_text == "UPDATE dbo.Orders SET Status = 3 WHERE OrderId = 48211"


def test_completed_statements_carry_duration_cpu_and_reads_in_milliseconds() -> None:
    executor = FakeExecutor(targets=[_ring("xe_ring_buffer_statements.xml", session="slow_capture")])

    read = _read(executor, HistoryCategory.LONG_STATEMENT)

    batch, rpc = read.events
    assert isinstance(batch, CompletedStatement) and isinstance(rpc, CompletedStatement)
    assert (batch.kind, batch.duration_ms, batch.cpu_ms) == ("sql_batch_completed", 12400.0, 8200.0)
    assert (batch.logical_reads, batch.row_count, batch.session_id) == (1843200, 412, 64)
    assert batch.statement_text is not None and "ExportedAt" in batch.statement_text
    assert (rpc.kind, rpc.object_name, rpc.duration_ms) == ("rpc_completed", "usp_ship_order", 5600.0)
    # The session's own predicate decides what "long" is; the result says so.
    assert any("predicate" in warning for warning in read.coverage.warnings)


def test_min_duration_filter_keeps_only_the_slower_statements() -> None:
    executor = FakeExecutor(targets=[_ring("xe_ring_buffer_statements.xml", session="slow_capture")])

    read = _read(executor, HistoryCategory.LONG_STATEMENT, filters={"min_duration_ms": 10000})

    assert [event.kind for event in read.events] == ["sql_batch_completed"]


def test_a_duration_with_an_unknown_unit_is_left_out_not_guessed() -> None:
    rows = [dict(row, description="Duration of the event.") for row in _catalog_rows()]
    executor = FakeExecutor(
        targets=[_ring("xe_ring_buffer_statements.xml", session="slow_capture")], catalog=rows
    )

    read = _read(executor, HistoryCategory.LONG_STATEMENT)

    assert [event.duration_ms for event in read.events] == [None, None]
    assert any("unit" in warning for warning in read.coverage.warnings)


def test_deadlock_report_uses_the_shared_graph_parser() -> None:
    executor = FakeExecutor(targets=[_ring("xe_ring_buffer_deadlock.xml", session="deadlocks")])

    read = _read(executor, HistoryCategory.DEADLOCK)

    (event,) = read.events
    assert isinstance(event, DeadlockEvent) and event.detail["victim_session_id"] == "61"


# --- schema confirmation -----------------------------------------------------------------


def test_event_whose_field_the_engine_does_not_list_is_not_parsed() -> None:
    # The parser reads blocked_process. If this engine names the field
    # differently, parsing would find nothing and report an all-clear.
    executor = FakeExecutor(
        targets=[_ring("xe_ring_buffer_blocking.xml")],
        catalog=_catalog_rows(blocked_process_report=["duration", "lock_mode"]),
    )

    read = _read(executor, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert read.coverage.reason.startswith("unverified_schema")
    assert "blocked_process_report" in read.coverage.reason
    assert executor.queries("sys.dm_xe_database_session_targets") == []


def test_catalog_keeps_field_names_and_time_units() -> None:
    catalog = catalog_from_rows(_catalog_rows())

    assert catalog.confirmed("blocked_process_report") and catalog.confirmed("attention")
    assert catalog.to_ms("sql_batch_completed", "duration") == 1000.0
    assert not catalog_from_rows([]).confirmed("attention")


# --- sessions that are not there ---------------------------------------------------------


def test_no_session_is_a_precondition_with_inert_capture_ddl() -> None:
    executor = FakeExecutor()

    read = _read(executor, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.PRECONDITION
    remediation = read.coverage.remediation or ""
    assert "CREATE EVENT SESSION" in remediation and "sqlserver.blocked_process_report" in remediation
    # The text is for a DBA. This module never creates or starts a session.
    assert not any("EVENT SESSION" in query for _, query, _ in executor.calls)


def test_defined_but_stopped_session_names_the_start_statement() -> None:
    executor = FakeExecutor(defined=[{"session_name": "blocking]capture"}])

    read = _read(executor, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.PRECONDITION
    assert read.coverage.remediation == (
        "ALTER EVENT SESSION [blocking]]capture] ON DATABASE STATE = START;"
    )


def test_session_with_only_targets_this_tool_cannot_read_is_a_precondition() -> None:
    target = dict(_ring("xe_ring_buffer_empty.xml"), target_name="histogram")
    executor = FakeExecutor(targets=[target])

    read = _read(executor, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.PRECONDITION
    assert "ring_buffer or event_file" in read.coverage.reason


def test_refused_dmv_is_unavailable_and_keeps_names_out() -> None:
    executor = FakeExecutor(fail_on="sys.dm_xe_database_session_targets")

    read = _read(executor, HistoryCategory.BLOCKING)

    assert read.coverage.status is ResultStatus.UNAVAILABLE
    assert "contoso-sql01" not in read.coverage.reason
    assert "VIEW DATABASE STATE" in read.coverage.reason


def test_categories_no_event_session_records_are_not_supported() -> None:
    executor = FakeExecutor()

    read = _read(executor, HistoryCategory.RESOURCE_METRIC)

    assert read.coverage.status is ResultStatus.NOT_SUPPORTED and executor.calls == []


# --- event_file --------------------------------------------------------------------------


def _file_session() -> dict[str, Any]:
    return {
        "session_name": "blocking_capture",
        "session_start_utc": _at("01:00").replace(tzinfo=None),
        "target_name": "event_file",
        "target_data": None,
    }


def test_event_file_is_read_for_the_window_through_the_file_function() -> None:
    executor = FakeExecutor(
        targets=[_file_session()],
        file_targets=[{"session_name": "blocking_capture", "file_name": BLOB_PREFIX + ".xel"}],
        file_events=[{
            "event_name": "blocked_process_report",
            "event_data": _fixture("xe_file_event_blocked.xml"),
            "timestamp_utc": datetime(2026, 11, 9, 2, 10, 5, 412000),
        }],
    )

    read = _read(executor, HistoryCategory.BLOCKING, limit=50)

    assert read.coverage.status is ResultStatus.OK
    (event,) = read.events
    assert isinstance(event, BlockedProcessReport) and event.blocking.session_id == 64
    (_, query, params) = executor.queries("sys.fn_xe_file_target_read_file")[0]
    # Row cap, blob prefix without the extension, then the window.
    assert params == [51, BLOB_PREFIX, "2026-11-09T02:00:00", "2026-11-09T02:30:00"]
    assert "N'blocked_process_report'" in query and "ORDER BY f.timestamp_utc" in query


def test_quiet_event_file_is_empty_only_back_to_its_oldest_event() -> None:
    def executor(oldest: datetime | None) -> FakeExecutor:
        return FakeExecutor(
            targets=[_file_session()],
            file_targets=[{"session_name": "blocking_capture", "file_name": BLOB_PREFIX + ".xel"}],
            file_bounds=[{"oldest_utc": oldest}],
        )

    covered = _read(executor(datetime(2026, 11, 1, 9, 0)), HistoryCategory.BLOCKING)
    rolled = _read(executor(datetime(2026, 11, 9, 2, 15)), HistoryCategory.BLOCKING)
    never_fired = _read(executor(None), HistoryCategory.BLOCKING)

    assert covered.coverage.status is ResultStatus.EMPTY
    assert rolled.coverage.status is ResultStatus.UNAVAILABLE
    # No event at all in the files: the running session vouches from its start, 01:00.
    assert never_fired.coverage.status is ResultStatus.EMPTY
