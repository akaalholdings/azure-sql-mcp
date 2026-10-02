from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.deadlocks import CAPTURE_SESSION_DDL
from azure_sql_mcp.deadlocks import DeadlockHistoryReader
from azure_sql_mcp.deadlocks import parse_ring_buffer
from azure_sql_mcp.lock_diagnostics import LockDiagnosticsService


def _graph(victim: str, other: str, *, table: str = "appdb.dbo.Orders") -> str:
    return f"""<deadlock>
  <victim-list><victimProcess id="{victim}" /></victim-list>
  <process-list>
    <process id="{victim}" spid="61" waitresource="KEY: 5:72057594045661184 (8194443284a0)"
             waittime="2843" lockMode="U" isolationlevel="read committed (2)"
             transactionname="user_transaction" trancount="2" logused="268"
             status="suspended" loginname="app_user" hostname="web01" clientapp="api">
      <executionStack>
        <frame procname="appdb.dbo.usp_ship_order" line="12">UPDATE dbo.Orders SET Status = 2</frame>
      </executionStack>
      <inputbuf>Proc [Database Id = 5 Object Id = 1013578649]</inputbuf>
    </process>
    <process id="{other}" spid="74" waitresource="KEY: 5:72057594045726720 (a0c936a3c965)"
             waittime="2810" lockMode="S" isolationlevel="read committed (2)"
             transactionname="user_transaction" trancount="1" status="suspended">
      <executionStack><frame procname="adhoc" line="1">SELECT * FROM dbo.Orders</frame></executionStack>
      <inputbuf>SELECT OrderID FROM dbo.Orders WHERE CustomerID = @c</inputbuf>
    </process>
  </process-list>
  <resource-list>
    <keylock hobtid="72057594045661184" dbid="5" objectname="{table}" indexname="PK_Orders"
             id="lock1" mode="X" associatedObjectId="72057594045661184">
      <owner-list><owner id="{other}" mode="X" /></owner-list>
      <waiter-list><waiter id="{victim}" mode="U" requestType="wait" /></waiter-list>
    </keylock>
    <keylock hobtid="72057594045726720" dbid="5" objectname="{table}" indexname="IX_Orders_CustomerID"
             id="lock2" mode="X" associatedObjectId="72057594045726720">
      <owner-list><owner id="{victim}" mode="X" /></owner-list>
      <waiter-list><waiter id="{other}" mode="S" requestType="wait" /></waiter-list>
    </keylock>
  </resource-list>
</deadlock>"""


def _event(timestamp: str, victim: str, other: str, *, database: str = "appdb") -> str:
    return (
        f'<event name="database_xml_deadlock_report" package="sqlserver" timestamp="{timestamp}">'
        '<data name="xml_report"><type name="xml" package="package0" />'
        f"<value>{_graph(victim, other)}</value></data>"
        f'<data name="database_name"><value>{database}</value></data>'
        "</event>"
    )


def _ring_buffer(*events: str) -> str:
    return (
        f'<RingBufferTarget truncated="0" eventCount="{len(events)}" droppedCount="0">'
        + "".join(events)
        + "</RingBufferTarget>"
    )


class RoutingExecutor:
    """Answer each deadlock source query separately, per database."""

    def __init__(
        self,
        *,
        running: list[dict[str, Any]] | Exception | None = None,
        defined: list[dict[str, Any]] | None = None,
        telemetry: list[dict[str, Any]] | Exception | None = None,
    ) -> None:
        self.running = running or []
        self.defined = defined or []
        self.telemetry = telemetry or []
        self.calls: list[tuple[str, str]] = []

    async def fetch_all(self, database_name: str, query: str, *args: Any, **kwargs: Any):
        self.calls.append((database_name, query))
        if "fn_xe_telemetry_blob_target_read_file" in query:
            result = self.telemetry
        elif "dm_xe_database_sessions" in query:
            result = self.running
        elif "database_event_sessions" in query:
            result = self.defined
        else:
            result = []
        if isinstance(result, Exception):
            raise result
        return result


def test_ring_buffer_document_yields_every_deadlock_event() -> None:
    events = parse_ring_buffer(
        _ring_buffer(
            _event("2026-09-30T10:00:00.100Z", "process1a", "process2a"),
            _event("2026-09-30T11:00:00.200Z", "process1b", "process2b"),
        ),
        source="database_xe_ring_buffer",
        session_name="dl",
    )

    assert [event["timestamp_utc"] for event in events] == [
        "2026-09-30T10:00:00.100Z",
        "2026-09-30T11:00:00.200Z",
    ]


@pytest.mark.asyncio
async def test_running_session_returns_parsed_deadlocks_newest_first() -> None:
    executor = RoutingExecutor(
        running=[
            {
                "session_name": "azure_sql_mcp_deadlocks",
                "target_name": "ring_buffer",
                "target_data": _ring_buffer(
                    _event("2026-09-30T10:00:00.100Z", "process1a", "process2a"),
                    _event("2026-09-30T11:00:00.200Z", "process1b", "process2b"),
                ),
            }
        ]
    )

    result = await DeadlockHistoryReader(executor).read("appdb", max_events=10)

    assert result["result_status"] == "ok"
    assert result["deadlock_count"] == 2
    newest = result["deadlocks"][0]
    assert newest["deadlock_time"] == "2026-09-30T11:00:00.200Z"
    assert newest["victim_process_ids"] == ["process1b"]
    assert newest["objects"] == ["appdb.dbo.Orders"]
    victim = next(p for p in newest["participants"] if p["is_victim"])
    assert victim["procedures"] == ["appdb.dbo.usp_ship_order"]
    assert victim["lock_mode"] == "U"
    assert {r["index_name"] for r in newest["resources"]} == {"PK_Orders", "IX_Orders_CustomerID"}
    assert "graph_xml" not in newest


@pytest.mark.asyncio
async def test_no_capture_session_is_a_precondition_not_a_false_zero() -> None:
    # The pre-fix tool queried a server-scoped system_health session that Azure SQL
    # Database does not have, swallowed the failure, and reported zero deadlocks.
    result = await DeadlockHistoryReader(RoutingExecutor()).read("appdb")

    assert result["deadlock_count"] == 0
    assert result["result_status"] == "precondition"
    assert result["remediation"] == CAPTURE_SESSION_DDL
    assert "database_xml_deadlock_report" in result["remediation"]


@pytest.mark.asyncio
async def test_defined_but_stopped_session_asks_to_start_it() -> None:
    executor = RoutingExecutor(defined=[{"session_name": "dl_capture"}])

    result = await DeadlockHistoryReader(executor).read("appdb")

    assert result["result_status"] == "precondition"
    assert result["remediation"] == "ALTER EVENT SESSION [dl_capture] ON DATABASE STATE = START;"


@pytest.mark.asyncio
async def test_unreadable_sources_are_never_reported_as_empty() -> None:
    executor = RoutingExecutor(running=PermissionError("VIEW DATABASE STATE permission denied"))

    result = await DeadlockHistoryReader(executor).read("appdb", master_available=False)

    assert result["result_status"] in {"unavailable", "precondition"}
    assert result["result_status"] != "empty"
    database_source = next(s for s in result["sources"] if s["source"] == "database_xe_ring_buffer")
    assert database_source["status"] == "unavailable"
    # The XE session DMVs are not tier-gated: the database grant is the whole fix.
    assert database_source["detail"].endswith("VIEW DATABASE STATE is required.")
    assert "ServerStateReader" not in database_source["detail"]


@pytest.mark.asyncio
async def test_empty_running_ring_buffer_is_a_true_negative() -> None:
    executor = RoutingExecutor(
        running=[
            {"session_name": "dl", "target_name": "ring_buffer", "target_data": _ring_buffer()}
        ]
    )

    result = await DeadlockHistoryReader(executor).read("appdb")

    assert result["result_status"] == "empty"
    assert "retained window" in result["result_status_reason"]


@pytest.mark.asyncio
async def test_master_telemetry_is_filtered_to_the_database_and_deduplicated() -> None:
    shared = _event("2026-09-30T12:00:00.000Z", "processA", "processB")
    executor = RoutingExecutor(
        running=[
            {"session_name": "dl", "target_name": "ring_buffer", "target_data": _ring_buffer(shared)}
        ],
        telemetry=[
            {"event_data": shared},
            {"event_data": _event("2026-09-30T09:00:00.000Z", "processC", "processD")},
            {
                "event_data": _event(
                    "2026-09-30T13:00:00.000Z", "processE", "processF", database="otherdb"
                )
            },
        ],
    )

    result = await DeadlockHistoryReader(executor).read("appdb", master_available=True)

    assert result["deadlock_count"] == 2
    assert result["deadlocks"][0]["also_seen_in"] == ["master_deadlock_telemetry"]
    assert any(database == "master" for database, _ in executor.calls)
    telemetry = next(s for s in result["sources"] if s["source"] == "master_deadlock_telemetry")
    assert telemetry["status"] == "ok"


@pytest.mark.asyncio
async def test_master_telemetry_is_never_read_through_a_user_database() -> None:
    executor = RoutingExecutor()

    await DeadlockHistoryReader(executor).read("appdb", master_available=False)

    assert all("fn_xe_telemetry_blob_target_read_file" not in q for _, q in executor.calls)


@pytest.mark.asyncio
async def test_graph_xml_is_returned_only_when_requested() -> None:
    executor = RoutingExecutor(
        running=[
            {
                "session_name": "dl",
                "target_name": "ring_buffer",
                "target_data": _ring_buffer(_event("2026-09-30T10:00:00Z", "p1", "p2")),
            }
        ]
    )

    result = await DeadlockHistoryReader(executor).read("appdb", include_graph_xml=True)

    assert result["deadlocks"][0]["graph_xml"].startswith("<deadlock>")
    assert result["deadlocks"][0]["graph_xml_truncated"] is False


@pytest.mark.asyncio
async def test_lock_service_delegates_to_the_azure_reader() -> None:
    result = await LockDiagnosticsService(RoutingExecutor()).get_deadlock_history(
        "appdb", max_events=5
    )

    assert result["deadlock_count"] == 0
    assert result["result_status"] == "precondition"
