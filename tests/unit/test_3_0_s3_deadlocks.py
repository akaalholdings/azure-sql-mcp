"""Deadlock history: lost events are never a clean empty, and the victim is a session.

Review findings 37 (parse failure or dropped events reported as "no deadlocks"),
78 (victim_session_id held a process id) and 94 (the whole logical server's
telemetry was read for one database).
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

import pytest

from azure_sql_mcp.deadlocks import MASTER_TELEMETRY_SQL
from azure_sql_mcp.deadlocks import DeadlockHistoryReader
from azure_sql_mcp.deadlocks import parse_deadlock_graph
from azure_sql_mcp.deadlocks import read_ring_buffer
from azure_sql_mcp.lock_diagnostics import LockDiagnosticsService
from tests.unit.test_deadlocks import RoutingExecutor
from tests.unit.test_deadlocks import _event


def ring_buffer(*events: str, truncated: int = 0, dropped: int = 0, event_count: int | None = None) -> str:
    count = len(events) if event_count is None else event_count
    return (
        f'<RingBufferTarget truncated="{truncated}" eventCount="{count}" droppedCount="{dropped}">'
        + "".join(events)
        + "</RingBufferTarget>"
    )


def running(target_data: str) -> list[dict[str, Any]]:
    return [{"session_name": "dl", "target_name": "ring_buffer", "target_data": target_data}]


async def read(target_data: str, **kwargs: Any) -> dict[str, Any]:
    return await DeadlockHistoryReader(RoutingExecutor(running=running(target_data))).read("appdb", **kwargs)  # type: ignore[arg-type]


# --- finding 37 -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_unparseable_ring_buffer_is_unavailable_not_empty() -> None:
    # A cut-off XML document is "could not read", the opposite of "no deadlocks".
    result = await read('<RingBufferTarget truncated="0" eventCount="12"><event name="database_xml_dead')

    assert result["result_status"] == "unavailable"
    assert result["deadlock_count"] == 0
    source = result["sources"][0]
    assert source["status"] == "unavailable"
    assert "could not be parsed" in source["detail"]


@pytest.mark.asyncio
async def test_dropped_events_with_nothing_left_are_not_a_true_negative() -> None:
    result = await read(ring_buffer(dropped=57))

    assert result["result_status"] == "unavailable"
    assert result["incomplete"] is True
    assert "57" in result["result_status_reason"]
    assert result["sources"][0]["dropped_count"] == 57
    assert "event_file" in result["remediation"]


@pytest.mark.asyncio
async def test_truncated_buffer_with_events_is_ok_but_marked_incomplete() -> None:
    event = _event("2026-11-01T10:00:00.000Z", "processA", "processB")

    result = await read(ring_buffer(event, truncated=1, event_count=40))

    assert result["result_status"] == "ok"
    assert result["deadlock_count"] == 1
    assert result["incomplete"] is True
    assert "undercount" in result["result_status_reason"]
    source = result["sources"][0]
    assert source["truncated"] is True and source["event_count"] == 40


@pytest.mark.asyncio
async def test_normal_roll_off_is_complete() -> None:
    # A full FIFO buffer that dropped nothing is expected retention, not loss.
    event = _event("2026-11-01T10:00:00.000Z", "processA", "processB")

    result = await read(ring_buffer(event))

    assert result["result_status"] == "ok"
    assert "incomplete" not in result
    assert result["sources"][0]["dropped_count"] == 0


def test_read_ring_buffer_reports_counts_and_unreadable_events() -> None:
    no_graph = '<event name="database_xml_deadlock_report" timestamp="2026-11-01T10:00:00.000Z" />'

    parsed = read_ring_buffer(ring_buffer(no_graph, dropped=3), source="s", session_name="dl")

    assert parsed.events == []
    assert (parsed.parse_failed, parsed.truncated, parsed.dropped_count, parsed.unreadable_events) == (False, False, 3, 1)
    assert read_ring_buffer("<broken", source="s", session_name="dl").parse_failed is True


# --- finding 78 -----------------------------------------------------------------


def test_victim_session_id_is_the_spid_not_the_process_id() -> None:
    graph = ET.fromstring(
        """<deadlock>
        <victim-list><victimProcess id="process1d2c3e"/></victim-list>
        <process-list>
          <process id="process1d2c3e" spid="55" lockMode="X"><inputbuf>UPDATE t1 SET x = 1</inputbuf></process>
          <process id="process9f8e7d" spid="56" lockMode="S"><inputbuf>SELECT x FROM t1</inputbuf></process>
        </process-list>
        <resource-list/>
        </deadlock>"""
    )

    parsed = parse_deadlock_graph(graph)

    # get_active_sessions and application logs know session 55, not "process1d2c3e".
    assert parsed["victim_session_id"] == "55"
    assert parsed["victim_session_ids"] == ["55"]
    assert parsed["victim_process_ids"] == ["process1d2c3e"]


def test_lock_service_parser_returns_the_victim_session() -> None:
    xml = """<deadlock><victim-list><victimProcess id="p1"/></victim-list>
    <process-list><process id="p1" spid="55"/><process id="p2" spid="56"/></process-list>
    <resource-list/></deadlock>"""

    assert LockDiagnosticsService._parse_deadlock_xml(xml)["victim_session_id"] == "55"


# --- finding 94 -----------------------------------------------------------------


def test_master_telemetry_is_filtered_and_bounded_on_the_server() -> None:
    assert "TOP (?)" in MASTER_TELEMETRY_SQL
    assert 'data[@name="database_name"]' in MASTER_TELEMETRY_SQL
    assert "ORDER BY" in MASTER_TELEMETRY_SQL


@pytest.mark.asyncio
async def test_master_telemetry_read_passes_the_limit_and_the_database() -> None:
    class Recording(RoutingExecutor):
        def __init__(self) -> None:
            super().__init__(telemetry=[{"event_data": _event("2026-11-01T09:00:00.000Z", "processC", "processD")}])
            self.params: list[Any] = []

        async def fetch_all(self, database_name: str, query: str, *args: Any, **kwargs: Any):
            if "fn_xe_telemetry_blob_target_read_file" in query:
                self.params.append((database_name, list(kwargs.get("params") or (args[0] if args else []))))
            return await super().fetch_all(database_name, query, *args, **kwargs)

    executor = Recording()

    result = await DeadlockHistoryReader(executor).read("appdb", max_events=7, master_available=True)  # type: ignore[arg-type]

    # TOP first, then the database: one logical server holds every database's deadlocks.
    assert executor.params == [("master", [7, "appdb"])]
    assert result["deadlock_count"] == 1
    telemetry = next(s for s in result["sources"] if s["source"] == "master_deadlock_telemetry")
    assert "newest 7" in telemetry["detail"]
