"""analyze() runs its checks together and reads shared sources once (finding 58).

A health sweep took the sum of every DMV round trip, and two checks each read
the 60-minute resource history and the governance row.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import patch

import pytest

from azure_sql_mcp.health import CHECK_NAMES
from azure_sql_mcp.health import HealthService
from tests.unit.test_health import FakeQueryStoreService
from tests.unit.test_health import governance_row
from tests.unit.test_health import resource_rows

RESOURCE_STATS = "sys.dm_db_resource_stats"
GOVERNANCE = "sys.dm_user_db_resource_governance"
SESSIONS = "AS idle_with_open_transaction"


class SlowExecutor:
    """Each read takes one event-loop sleep, so overlapping reads are visible."""

    def __init__(
        self,
        responses: list[tuple[str, list[dict[str, Any]]]] | None = None,
        errors: list[tuple[str, Exception]] | None = None,
        delays: dict[str, float] | None = None,
    ) -> None:
        self.responses = responses or []
        self.errors = errors or []
        self.delays = delays or {}
        self.queries: list[str] = []
        self.in_flight = 0
        self.peak_in_flight = 0

    async def fetch_all(
        self,
        database_name: str,
        query: str,
        params: list[Any] | tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        self.queries.append(query)
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            delay = next(
                (seconds for needle, seconds in self.delays.items() if needle in query),
                0.005,
            )
            await asyncio.sleep(delay)
            for needle, error in self.errors:
                if needle in query:
                    raise error
            for needle, rows in self.responses:
                if needle in query:
                    return rows
            return []
        finally:
            self.in_flight -= 1

    def reads_of(self, needle: str) -> int:
        return sum(1 for query in self.queries if needle in query)


def healthy_responses() -> list[tuple[str, list[dict[str, Any]]]]:
    return [
        (SESSIONS, [{"total_sessions": 300, "active_requests": 4, "idle_sessions": 290}]),
        (RESOURCE_STATS, resource_rows(max_worker_percent=97.0)),
        (GOVERNANCE, [governance_row()]),
    ]


def build(**kwargs: Any) -> tuple[HealthService, SlowExecutor]:
    executor = SlowExecutor(**kwargs)
    return HealthService(executor, FakeQueryStoreService()), executor  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_checks_run_together_but_never_more_than_four_at_once() -> None:
    service, executor = build(responses=healthy_responses())

    await service.analyze("appdb", "all")

    # One at a time made the sweep as slow as the sum of its reads. Four stays
    # below the default pool size of five, so another tool call is not starved.
    assert executor.peak_in_flight == 4


@pytest.mark.asyncio
async def test_shared_sources_are_read_once_per_sweep() -> None:
    service, executor = build(responses=healthy_responses())

    payload = await service.analyze("appdb", "all")

    assert executor.reads_of(RESOURCE_STATS) == 1
    assert executor.reads_of(GOVERNANCE) == 1
    # Both checks still got the data.
    connection = payload["checks"]["connection"]["details"]
    resource = payload["checks"]["resource"]["details"]
    assert connection["session_limit"] == 30000
    assert connection["worker_limit"] == 200
    assert connection["max_worker_percent_peak"] == 97.0
    assert resource["governance_limits"]["max_sessions"] == 30000
    assert resource["peak_usage"]["max_worker_percent"] == 97.0
    assert resource["sample_count"] == 240


@pytest.mark.asyncio
async def test_payload_keeps_check_order_when_checks_finish_out_of_order() -> None:
    # The first check finishes last.
    service, _executor = build(responses=healthy_responses(), delays={SESSIONS: 0.05})

    payload = await service.analyze("appdb", "all")
    subset = await service.analyze("appdb", "storage,resource,connection")

    assert list(payload["checks"]) == list(CHECK_NAMES)
    assert list(subset["checks"]) == ["connection", "resource", "storage"]


@pytest.mark.asyncio
async def test_check_results_do_not_depend_on_what_else_runs() -> None:
    service, _executor = build(responses=healthy_responses())

    together = await service.analyze("appdb", "all")

    for name in CHECK_NAMES:
        alone = await service.analyze("appdb", name)
        assert together["checks"][name] == alone["checks"][name], name


@pytest.mark.asyncio
async def test_checks_that_do_not_use_a_shared_source_do_not_read_it() -> None:
    service, executor = build(responses=healthy_responses())

    await service.analyze("appdb", "constraint,identity,storage")

    assert executor.reads_of(RESOURCE_STATS) == 0
    assert executor.reads_of(GOVERNANCE) == 0


@pytest.mark.asyncio
async def test_failed_history_read_degrades_each_check_in_its_own_way() -> None:
    responses = [pair for pair in healthy_responses() if pair[0] != RESOURCE_STATS]
    service, executor = build(
        responses=responses,
        errors=[(RESOURCE_STATS, TimeoutError("Query timeout expired"))],
    )

    with patch("azure_sql_mcp.health.note_exception") as note:
        checks = (await service.analyze("appdb", "all"))["checks"]

    assert executor.reads_of(RESOURCE_STATS) == 1
    # The connection check keeps its session counts and limits, and says that
    # the pressure signal is missing.
    connection = checks["connection"]
    assert connection["status"] == "warning"
    assert connection["details"]["total_sessions"] == 300
    assert connection["details"]["session_limit"] == 30000
    assert "Query timeout expired" in connection["details"]["pressure_unavailable_reason"]
    assert connection["findings"] == [
        "Session and worker pressure are not available; this is not a pass. "
        + connection["details"]["pressure_unavailable_reason"]
    ]
    # The resource check has nothing to judge without the history.
    resource = checks["resource"]
    assert resource["status"] == "warning"
    assert resource["details"] == {"available": False, "error": "Query timeout expired"}
    assert resource["findings"] == [
        "resource health data could not be collected: Query timeout expired"
    ]
    # No other check is affected, and each failure is recorded where it was.
    assert checks["storage"]["details"].get("available") is not False
    assert sorted(call.args[1] for call in note.call_args_list) == [
        "health.check",
        "health.optional_rows",
    ]


@pytest.mark.asyncio
async def test_failed_governance_read_degrades_each_check_in_its_own_way() -> None:
    responses = [pair for pair in healthy_responses() if pair[0] != GOVERNANCE]
    service, executor = build(
        responses=responses,
        errors=[(GOVERNANCE, PermissionError("VIEW SERVER STATE permission denied"))],
    )

    with patch("azure_sql_mcp.health.note_exception") as note:
        checks = (await service.analyze("appdb", "all"))["checks"]

    assert executor.reads_of(GOVERNANCE) == 1
    # The connection check loses the limits only: worker pressure still comes
    # from the resource history.
    connection = checks["connection"]
    assert connection["status"] == "critical"
    assert connection["details"]["session_limit"] is None
    assert connection["details"]["worker_limit"] is None
    assert connection["details"]["max_worker_percent_peak"] == 97.0
    reason = connection["details"]["session_limit_unavailable_reason"]
    assert reason.startswith(
        f"{GOVERNANCE} could not be read: VIEW SERVER STATE permission denied."
    )
    assert "##MS_ServerStateReader##" in reason
    # The resource check still judges pressure, without absolute limits.
    resource = checks["resource"]
    assert resource["status"] == "critical"
    assert resource["details"]["governance_limits"] == {}
    assert resource["details"]["sample_count"] == 240
    assert sorted(call.args[1] for call in note.call_args_list) == [
        "health.governance_limits",
        "health.optional_rows",
    ]


@pytest.mark.asyncio
async def test_cancelled_sweep_stops_every_running_check() -> None:
    service, executor = build(responses=healthy_responses(), delays={"": 5.0})

    sweep = asyncio.create_task(service.analyze("appdb", "all"))
    await asyncio.sleep(0.02)
    sweep.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sweep
    await asyncio.sleep(0)

    # A tool timeout must not leave checks reading in the background.
    assert executor.in_flight == 0
