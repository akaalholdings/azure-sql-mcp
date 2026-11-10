from __future__ import annotations

import json
from pathlib import Path

import pytest

from azure_sql_mcp.database_diagnosis import diagnose_findings
from azure_sql_mcp.sessions import SessionsService
from tests.azure_dmv_contract import StrictDmvExecutor

PLAN_XML = (
    Path(__file__).resolve().parents[1] / "fixtures" / "showplans" / "seek_residual_lookup_sort.xml"
).read_text(encoding="utf-8")


class StubExecutor:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def fetch_all(self, database_name: str, query: str, params=None):
        self.calls.append((database_name, query, params))
        return self.rows


@pytest.mark.asyncio
async def test_get_active_sessions_returns_blocking_chains() -> None:
    rows = [
        {"session_id": 51, "blocking_session_id": None, "command": "SELECT"},
        {"session_id": 52, "blocking_session_id": 51, "command": "UPDATE"},
        {"session_id": 53, "blocking_session_id": 51, "command": "SELECT"},
    ]
    executor = StubExecutor(rows)
    service = SessionsService(executor)

    payload = await service.get_active_sessions("appdb")

    assert payload["database_name"] == "appdb"
    assert payload["active_session_count"] == 3
    assert len(payload["blocking_chains"]) == 1
    assert payload["blocking_chains"][0]["head_blocker_session_id"] == 51
    assert payload["blocking_chains"][0]["blocked_session_ids"] == [52, 53]


@pytest.mark.asyncio
async def test_detect_blocking_chains_returns_empty_for_unblocked_sessions() -> None:
    service = SessionsService(StubExecutor([]))

    assert service._detect_blocking_chains([{"session_id": 7, "blocking_session_id": None}]) == []


@pytest.mark.asyncio
async def test_get_active_sessions_truncates_above_limit() -> None:
    rows = [
        {"session_id": 50 + i, "blocking_session_id": None, "command": "SELECT"}
        for i in range(3)
    ]
    executor = StubExecutor(rows)
    service = SessionsService(executor)

    result = await service.get_active_sessions("appdb", limit=2)

    assert result["truncated"] is True
    assert result["active_session_count"] == 2
    # TOP is parameterized with limit + 1 to detect truncation
    assert executor.calls[0][2] == [3]


def _incident_rows(*, with_plans: bool) -> list[dict]:
    """One head blocker and 80 waiters, shaped like the real 24-column rows."""

    def row(session_id: int, blocker: int | None) -> dict:
        payload = {
            "session_id": session_id,
            "login_name": "app_user",
            "session_status": "running",
            "request_status": "suspended" if blocker else "running",
            "command": "UPDATE",
            "wait_type": "LCK_M_U" if blocker else None,
            "wait_time_ms": 42_000 if blocker else 0,
            "wait_resource": "KEY: 5:72057594041925632 (8194443284a0)" if blocker else "",
            "blocking_session_id": blocker,
            "cpu_time_ms": 12,
            "elapsed_time_ms": 42_010,
            "logical_reads": 40,
            "writes": 0,
            "row_count": 0,
            "granted_memory_mb": 1.0,
            "isolation_level": "ReadCommitted",
            "open_transaction_count": 1,
            "transaction_id": 9_000 + session_id,
            "transaction_begin_time": "2026-11-03T10:00:00",
            "transaction_duration_seconds": 42,
            "plan_handle": "0x06000500" + "ab" * 40,
            "query_hash": "0x1A2B3C4D5E6F7081",
            "current_statement": "UPDATE dbo.Orders SET status = @s WHERE order_id = @id",
        }
        if with_plans:
            payload["execution_plan_xml"] = PLAN_XML
        return payload

    return [row(51, None)] + [row(60 + index, 51) for index in range(80)]


@pytest.mark.asyncio
async def test_default_read_builds_no_plan_xml_and_returns_handles_for_one_plan() -> None:
    # During a blocking incident every dm_exec_query_plan call builds showplan
    # XML on a database that is already stressed, for up to 1000 requests. The
    # agent needs one plan at most, so it gets the handles to ask for it.
    executor = StubExecutor([])
    service = SessionsService(executor)

    await service.get_active_sessions("appdb")

    query = executor.calls[0][1]
    assert "dm_exec_query_plan" not in query
    assert "execution_plan_xml" not in query
    assert "CONVERT(varchar(130), r.plan_handle, 1) AS plan_handle" in query
    assert "CONVERT(varchar(130), r.query_hash, 1) AS query_hash" in query


@pytest.mark.asyncio
async def test_include_plans_reads_the_cached_plan_per_request() -> None:
    executor = StubExecutor([])
    service = SessionsService(executor)

    await service.get_active_sessions("appdb", include_plans=True)

    query = executor.calls[0][1]
    assert "OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) AS qp" in query
    assert "qp.query_plan AS execution_plan_xml" in query
    assert "AS plan_handle" in query


@pytest.mark.parametrize("include_plans", [False, True])
@pytest.mark.asyncio
async def test_active_sessions_sql_is_valid_tsql_on_documented_columns(include_plans: bool) -> None:
    # A column that Azure SQL Database does not have fails the whole tool with
    # error 207, in the middle of an incident.
    executor = StrictDmvExecutor()

    await SessionsService(executor).get_active_sessions("appdb", include_plans=include_plans)  # type: ignore[arg-type]

    assert executor.violations == []


@pytest.mark.asyncio
async def test_blocking_chain_names_sessions_and_waits_without_repeating_rows() -> None:
    # The old chain embedded each blocked session's whole row a second time.
    # With 80 waiters that doubled the payload the agent reads to find one
    # head blocker.
    rows = _incident_rows(with_plans=False)
    service = SessionsService(StubExecutor(rows))

    payload = await service.get_active_sessions("appdb")

    (chain,) = payload["blocking_chains"]
    assert chain["head_blocker_session_id"] == 51
    assert chain["blocked_session_ids"] == [60 + index for index in range(80)]
    assert chain["blocked_session_waits"][0] == {
        "session_id": 60,
        "wait_type": "LCK_M_U",
        "lock_mode": "U",
        "wait_time_ms": 42_000,
        "wait_resource": "KEY: 5:72057594041925632 (8194443284a0)",
    }
    assert len(chain["blocked_session_waits"]) == 80
    # 3.0 adds counts and one grouped resource per chain, never a session row.
    assert chain["total_blocked_count"] == 80
    assert chain["indirect_blocked_session_ids"] == []
    assert len(chain["blocked_resources"]) == 1
    assert chain["head_blocker_has_active_request"] is True
    assert "current_statement" not in json.dumps(chain)
    assert len(json.dumps(chain)) * 4 < len(json.dumps(payload["sessions"]))


@pytest.mark.asyncio
async def test_idle_head_blocker_is_marked_as_having_no_active_request() -> None:
    # A head blocker that is idle in an open transaction has no row in
    # dm_exec_requests. That is an abandoned transaction: the fix is the code
    # path that left it open, so the chain must say the blocker runs nothing.
    rows = [row for row in _incident_rows(with_plans=False) if row["session_id"] != 51]
    payload = await SessionsService(StubExecutor(rows)).get_active_sessions("appdb")

    (chain,) = payload["blocking_chains"]
    assert chain["head_blocker_session_id"] == 51
    assert chain["head_blocker_has_active_request"] is False
    assert len(chain["blocked_session_ids"]) == 80


@pytest.mark.asyncio
async def test_blocking_incident_payload_is_a_fraction_of_the_plan_carrying_one() -> None:
    # Measured on the review's incident shape: 1 head blocker, 80 waiters,
    # one 10 KB plan each. include_plans=True is the opt-in upper bound.
    default_payload = await SessionsService(
        StubExecutor(_incident_rows(with_plans=False))
    ).get_active_sessions("appdb")
    with_plans = await SessionsService(
        StubExecutor(_incident_rows(with_plans=True))
    ).get_active_sessions("appdb", include_plans=True)

    assert "ShowPlanXML" not in json.dumps(default_payload)
    # One copy per session, none repeated inside blocking_chains.
    assert json.dumps(with_plans).count("<ShowPlanXML") == 81
    assert len(json.dumps(default_payload)) * 10 < len(json.dumps(with_plans))


@pytest.mark.asyncio
async def test_diagnose_database_counts_blocked_sessions_from_real_chains() -> None:
    # diagnose_database ranks blocking by how many sessions wait. It reads the
    # chain shape that get_active_sessions emits, so the two must agree.
    payload = await SessionsService(
        StubExecutor(_incident_rows(with_plans=False))
    ).get_active_sessions("appdb")

    (finding,) = [
        finding
        for finding in diagnose_findings({"sessions": payload})
        if finding["code"] == "active_blocking"
    ]

    assert finding["severity"] == "high"
    assert finding["evidence"] == {"chains": 1, "blocked_sessions": 80, "head_blockers": [51]}
    assert "80 session(s) blocked, head blocker session 51" in finding["message"]
