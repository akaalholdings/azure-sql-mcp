from __future__ import annotations

import pytest

from azure_sql_mcp.sessions import SessionsService


class StubExecutor:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def fetch_all(self, database_name: str, query: str):
        self.calls.append((database_name, query))
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
    assert [session["session_id"] for session in payload["blocking_chains"][0]["blocked_sessions"]] == [52, 53]


@pytest.mark.asyncio
async def test_detect_blocking_chains_returns_empty_for_unblocked_sessions() -> None:
    service = SessionsService(StubExecutor([]))

    assert service._detect_blocking_chains([{"session_id": 7, "blocking_session_id": None}]) == []
