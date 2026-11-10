"""Live request detail and blocking-chain shape for get_active_sessions.

During an incident the DBA asks: who is calling, is one statement piling up, how
deep is the blocking, and are the victims readers or writers. Row versioning
(RCSI) helps only when readers are most of the victims.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from azure_sql_mcp.sessions import SessionsService
from azure_sql_mcp.sessions import blocking_chains
from azure_sql_mcp.sessions import load_by_caller
from azure_sql_mcp.sessions import parse_wait_resource
from azure_sql_mcp.sessions import query_pileups
from tests.azure_dmv_contract import StrictDmvExecutor

KEY_RESOURCE = "KEY: 5:72057594041925632 (8194443284a0)"
DENIED = "VIEW DATABASE STATE permission was denied in database 'appdb'. (262)"


class RoutedExecutor:
    """Answers each statement by the first needle it holds."""

    def __init__(self, routes: list[tuple[str, Any]]) -> None:
        self.routes = routes
        self.calls: list[tuple[str, tuple[Any, ...] | None]] = []

    async def fetch_all(self, database_name: str, query: str, params: Any = None, **_: Any) -> list[dict[str, Any]]:
        self.calls.append((query, tuple(params) if params is not None else None))
        for needle, result in self.routes:
            if needle in query:
                if isinstance(result, BaseException):
                    raise result
                return result
        return []


def request(session_id: int, blocker: int | None = None, **values: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "session_id": session_id,
        "request_id": 0,
        "login_name": "app_user",
        "host_name": "web-01",
        "program_name": "OrdersApi",
        "command": "SELECT",
        "wait_type": "LCK_M_S" if blocker else None,
        "wait_time_ms": 12_000 if blocker else 0,
        "wait_resource": KEY_RESOURCE if blocker else "",
        "blocking_session_id": blocker,
        "cpu_time_ms": 100,
        "elapsed_time_ms": 12_500,
        "logical_reads": 1_000,
        "granted_memory_mb": 8.0,
        "dop": 1,
        "query_hash": "0x1A2B3C4D5E6F7081",
    }
    row.update(values)
    return row


def routes(rows: list[dict[str, Any]], *extra: tuple[str, Any]) -> list[tuple[str, Any]]:
    return [*extra, ("FROM sys.dm_exec_requests AS r", rows)]


# --- item 2: request detail, pile-ups, load by caller ---------------------------


@pytest.mark.asyncio
async def test_each_request_names_its_caller_plan_shape_and_parallelism() -> None:
    executor = StrictDmvExecutor()

    await SessionsService(executor).get_active_sessions("appdb")  # type: ignore[arg-type]

    assert executor.violations == []
    query = executor.calls[0][1]
    for column in (
        "s.host_name",
        "s.program_name",
        "s.login_name",
        "r.dop",
        "r.parallel_worker_count",
        "r.request_id",
        "AS query_hash",
        "AS query_plan_hash",
        "AS granted_memory_mb",
    ):
        assert column in query
    assert "dm_exec_query_plan" not in query  # plans stay opt-in


@pytest.mark.asyncio
async def test_tempdb_use_is_merged_per_request() -> None:
    rows = [request(61), request(62)]
    tempdb = [{"session_id": 62, "request_id": 0, "tempdb_user_objects_mb": 10.0, "tempdb_internal_objects_mb": 4086.0}]
    executor = RoutedExecutor(routes(rows, ("sys.dm_db_task_space_usage", tempdb)))

    result = await SessionsService(executor).get_active_sessions("appdb")  # type: ignore[arg-type]

    by_id = {row["session_id"]: row for row in result["sessions"]}
    assert by_id[62]["tempdb_internal_objects_mb"] == 4086.0
    assert by_id[62]["tempdb_allocated_mb"] == 4096.0
    assert by_id[61]["tempdb_allocated_mb"] == 0.0
    assert result["tempdb_usage"]["result_status"] == "ok"
    assert result["tempdb_usage"]["source_dmv"] == "sys.dm_db_task_space_usage"


@pytest.mark.asyncio
async def test_refused_tempdb_read_keeps_the_sessions_and_reports_unknown_not_zero() -> None:
    # sys.dm_db_task_space_usage is gated on pools. Losing it must not lose the
    # request list, and tempdb use must read as unknown, not as 0 MB.
    executor = RoutedExecutor(
        routes(
            [request(61)],
            ("sys.dm_db_task_space_usage", RuntimeError(DENIED)),
            ("'Edition'", [{"edition": "Standard", "service_objective": "ElasticPool", "elastic_pool_name": "pool-a"}]),
        )
    )

    result = await SessionsService(executor).get_active_sessions("appdb")  # type: ignore[arg-type]

    assert result["active_session_count"] == 1
    assert result["sessions"][0]["tempdb_allocated_mb"] is None
    assert result["tempdb_usage"]["result_status"] == "unavailable"
    assert "ALTER SERVER ROLE ##MS_ServerStateReader##" in result["tempdb_usage"]["remediation"]


def test_three_requests_on_one_query_hash_are_a_pileup() -> None:
    sessions = [
        request(61, query_hash="0xAAAA", elapsed_time_ms=9_000),
        request(62, query_hash="0xAAAA", elapsed_time_ms=4_000, blocking_session_id=61, wait_type="LCK_M_X"),
        request(63, query_hash="0xAAAA", elapsed_time_ms=1_000),
        request(64, query_hash="0xBBBB"),
        request(65, query_hash="0xBBBB"),
        # A zero hash is "no statement", not one statement run three times.
        request(66, query_hash="0x0000000000000000"),
        request(67, query_hash="0x0000000000000000"),
        request(68, query_hash="0x0000000000000000"),
    ]

    (pileup,) = query_pileups(sessions)

    assert pileup == {
        "query_hash": "0xAAAA",
        "request_count": 3,
        "blocked_request_count": 1,
        "max_elapsed_time_ms": 9_000,
        "total_cpu_time_ms": 300,
        "session_ids": [61, 62, 63],
    }


def test_load_by_caller_groups_requests_by_program_host_and_login() -> None:
    sessions = [
        request(61, cpu_time_ms=5_000),
        request(62, cpu_time_ms=1_000, blocking_session_id=61),
        request(63, program_name="NightlyEtl", host_name="etl-02", login_name="etl_user", cpu_time_ms=90_000),
    ]

    callers = load_by_caller(sessions)

    assert callers[0] == {
        "program_name": "OrdersApi",
        "host_name": "web-01",
        "login_name": "app_user",
        "request_count": 2,
        "blocked_request_count": 1,
        "total_cpu_time_ms": 6_000,
        "total_elapsed_time_ms": 25_000,
        "total_granted_memory_mb": 16.0,
    }
    assert callers[1]["program_name"] == "NightlyEtl" and callers[1]["total_cpu_time_ms"] == 90_000


@pytest.mark.asyncio
async def test_result_describes_its_source_and_window() -> None:
    result = await SessionsService(RoutedExecutor(routes([request(61)]))).get_active_sessions("appdb")  # type: ignore[arg-type]

    assert "sys.dm_exec_requests" in result["source_dmvs"]
    assert result["collection_window"] == {"mode": "point_in_time"}
    assert result["pileups"] == []
    assert result["load_by_caller"][0]["request_count"] == 1


# --- item 3: blocking chains ----------------------------------------------------


def test_chain_counts_transitive_victims_and_depth() -> None:
    # 51 (idle) blocks 60, and 60 blocks 61-66. Counting direct victims only
    # reports one blocked session for the session that stalls seven.
    sessions = [request(60, 51)] + [request(61 + index, 60) for index in range(6)]

    (chain,) = blocking_chains(sessions)

    assert chain["head_blocker_session_id"] == 51
    assert chain["blocked_session_ids"] == [60]
    assert chain["indirect_blocked_session_ids"] == [61, 62, 63, 64, 65, 66]
    assert chain["total_blocked_count"] == 7
    assert chain["chain_depth"] == 2
    assert chain["max_wait_time_ms"] == 12_000


def test_chain_shows_the_reader_writer_split_of_its_victims() -> None:
    sessions = [
        request(60, 51, wait_type="LCK_M_S"),
        request(61, 51, wait_type="LCK_M_IS"),
        request(62, 51, wait_type="LCK_M_S"),
        request(63, 51, wait_type="LCK_M_U"),
        request(64, 51, wait_type="LCK_M_SCH_S"),
        request(65, 51, wait_type="LCK_M_RS_S"),
    ]

    (chain,) = blocking_chains(sessions)

    assert chain["victims_by_lock_mode"] == {"S": 2, "IS": 1, "U": 1, "SCH_S": 1, "RS_S": 1}
    # Serializable range waits and schema-stability waits are not helped by row
    # versioning, so they are not counted as readers.
    assert chain["victim_split"] == {"reader": 3, "writer": 1, "range": 1, "schema": 1, "non_lock": 0}
    assert chain["reader_victim_share"] == 0.5
    assert chain["blocked_session_waits"][0]["lock_mode"] == "S"


def test_writer_only_chain_has_no_reader_share_to_act_on() -> None:
    (chain,) = blocking_chains([request(60, 51, wait_type="LCK_M_X"), request(61, 51, wait_type="LCK_M_U")])

    assert chain["victim_split"]["reader"] == 0
    assert chain["reader_victim_share"] == 0.0


def test_a_blocking_cycle_still_yields_a_chain() -> None:
    # A deadlock in progress has no head; the sessions must not vanish.
    (chain,) = blocking_chains([request(60, 61), request(61, 60)])

    assert chain["head_blocker_session_id"] == 60
    assert chain["cycle"] is True
    assert chain["total_blocked_count"] == 2


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (KEY_RESOURCE, {"resource_type": "KEY", "database_id": 5, "hobt_id": 72057594041925632}),
        ("OBJECT: 5:1977058079:0 ", {"resource_type": "OBJECT", "database_id": 5, "object_id": 1977058079}),
        ("PAGE: 5:1:104", {"resource_type": "PAGE", "database_id": 5, "file_id": 1, "page_id": 104}),
        ("RID: 5:1:104:3", {"resource_type": "RID", "database_id": 5, "file_id": 1, "page_id": 104}),
        ("METADATA: database_id = 5 PERMISSIONS(class = 1)", {"resource_type": "METADATA"}),
        ("", {}),
    ],
)
def test_wait_resource_is_parsed_by_type(text: str, expected: dict[str, Any]) -> None:
    assert parse_wait_resource(text) == expected


@pytest.mark.asyncio
async def test_blocked_key_and_object_resources_are_named() -> None:
    rows = [
        request(60, 51),
        request(61, 51),
        request(62, 51, wait_resource="OBJECT: 5:1977058079:0 ", wait_type="LCK_M_SCH_S"),
        # Another database's hobt id must not be named from this catalog.
        request(63, 51, wait_resource="KEY: 2:72057594041925632 (aa)"),
    ]
    executor = RoutedExecutor(
        routes(
            rows,
            (
                "FROM sys.partitions AS p",
                [
                    {
                        "database_id": 5,
                        "hobt_id": 72057594041925632,
                        "schema_name": "dbo",
                        "object_name": "Orders",
                        "index_name": "PK_Orders",
                    }
                ],
            ),
            ("FROM sys.objects AS o", [{"database_id": 5, "object_id": 1977058079, "schema_name": "dbo", "object_name": "Customers"}]),
        )
    )

    result = await SessionsService(executor).get_active_sessions("appdb")  # type: ignore[arg-type]

    (chain,) = result["blocking_chains"]
    resources = {(item["resource_type"], item.get("object_name")): item for item in chain["blocked_resources"]}
    assert resources[("KEY", "Orders")]["waiter_count"] == 2
    assert resources[("KEY", "Orders")]["index_name"] == "PK_Orders"
    assert resources[("OBJECT", "Customers")]["schema_name"] == "dbo"
    assert resources[("KEY", None)]["database_id"] == 2
    hobt_call = next(call for call in executor.calls if "FROM sys.partitions AS p" in call[0])
    assert hobt_call[1] == (72057594041925632,)


@pytest.mark.asyncio
async def test_idle_head_blocker_is_described_as_a_sleeping_open_transaction() -> None:
    rows = [request(60, 51), request(61, 60)]
    head = [
        {
            "session_id": 51,
            "session_status": "sleeping",
            "open_transaction_count": 1,
            "login_name": "app_user",
            "host_name": "web-07",
            "program_name": "OrdersApi",
            "idle_seconds": 420,
        }
    ]
    executor = RoutedExecutor(routes(rows, ("AS idle_seconds", head)))

    result = await SessionsService(executor).get_active_sessions("appdb")  # type: ignore[arg-type]

    (chain,) = result["blocking_chains"]
    assert chain["head_blocker_has_active_request"] is False
    assert chain["head_blocker_state"] == "sleeping_open_transaction"
    assert chain["head_blocker"] == {
        "login_name": "app_user",
        "host_name": "web-07",
        "program_name": "OrdersApi",
        "open_transaction_count": 1,
        "idle_seconds": 420,
    }
    assert "KILL" not in json.dumps(result).upper().replace("SKILL", "")


@pytest.mark.asyncio
async def test_running_head_blocker_needs_no_second_read() -> None:
    executor = RoutedExecutor(routes([request(51), request(60, 51)]))

    result = await SessionsService(executor).get_active_sessions("appdb")  # type: ignore[arg-type]

    (chain,) = result["blocking_chains"]
    assert chain["head_blocker_state"] == "running"
    assert chain["head_blocker"]["host_name"] == "web-01"
    assert not any("AS idle_seconds" in query for query, _ in executor.calls)


@pytest.mark.asyncio
async def test_unreadable_head_blocker_is_unknown_not_idle() -> None:
    executor = RoutedExecutor(routes([request(60, 51)], ("AS idle_seconds", RuntimeError("timeout"))))

    result = await SessionsService(executor).get_active_sessions("appdb")  # type: ignore[arg-type]

    assert result["blocking_chains"][0]["head_blocker_state"] == "unknown"


@pytest.mark.asyncio
async def test_no_blocking_means_no_extra_catalog_reads() -> None:
    executor = RoutedExecutor(routes([request(61), request(62)]))

    await SessionsService(executor).get_active_sessions("appdb")  # type: ignore[arg-type]

    assert not any("sys.partitions" in query or "sys.objects" in query for query, _ in executor.calls)


@pytest.mark.asyncio
async def test_side_reads_are_valid_tsql_on_documented_columns() -> None:
    executor = StrictDmvExecutor(responses=[("FROM sys.dm_exec_requests AS r", [request(60, 51), request(61, 51, wait_resource="OBJECT: 5:7:0")])])

    await SessionsService(executor).get_active_sessions("appdb")  # type: ignore[arg-type]

    assert executor.violations == []
    issued = " ".join(query for _, query, _ in executor.calls)
    for source in ("sys.dm_db_task_space_usage", "AS idle_seconds", "sys.partitions", "sys.objects"):
        assert source in issued
