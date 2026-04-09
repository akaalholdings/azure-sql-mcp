from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from azure_sql_mcp.capabilities import CapabilityService


@pytest.mark.asyncio
async def test_run_check_sanitizes_error_messages() -> None:
    service = CapabilityService(
        executor=MagicMock(),
        query_store_service=MagicMock(),
        plans_service=MagicMock(),
        recommendation_service=MagicMock(),
    )

    async def boom(_: str):
        raise RuntimeError(
            "Server=tcp:prod.database.windows.net;Database=appdb;UID=sa;PWD=super-secret;"
        )

    result = await service._run_check("appdb", boom)

    assert result["available"] is False
    assert "super-secret" not in result["message"]
    assert "UID=sa" not in result["message"]
    assert "prod.database.windows.net" not in result["message"]


@pytest.mark.asyncio
async def test_run_check_returns_detail_on_success() -> None:
    service = CapabilityService(
        executor=MagicMock(),
        query_store_service=MagicMock(),
        plans_service=MagicMock(),
        recommendation_service=MagicMock(),
    )

    async def ok(_: str):
        return {"status": "ok"}

    result = await service._run_check("appdb", ok)

    assert result == {"available": True, "detail": {"status": "ok"}}
