from __future__ import annotations

from unittest.mock import AsyncMock
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


@pytest.mark.asyncio
async def test_plan_probe_targets_user_table_when_available() -> None:
    """SQL Server exempts catalog-only statements from the SHOWPLAN permission
    check, so probing sys.objects reports plans available even when
    explain_query on real tables is denied (found live with db_datareader).
    The probe must target a user table when one exists."""
    executor = AsyncMock()
    executor.fetch_all = AsyncMock(
        return_value=[{"qualified_name": "[dbo].[Users]"}]
    )
    plans = AsyncMock()
    service = CapabilityService(executor, AsyncMock(), plans, AsyncMock())

    await service._check_estimated_plan("appdb")

    plans.explain_query.assert_awaited_once_with(
        "appdb", "SELECT TOP 1 * FROM [dbo].[Users]", analyze=False,
    )


@pytest.mark.asyncio
async def test_plan_probe_falls_back_to_catalog_when_no_user_tables() -> None:
    executor = AsyncMock()
    executor.fetch_all = AsyncMock(return_value=[])
    plans = AsyncMock()
    service = CapabilityService(executor, AsyncMock(), plans, AsyncMock())

    await service._check_actual_plan("appdb")

    plans.explain_query.assert_awaited_once_with(
        "appdb", "SELECT TOP 1 name FROM sys.objects ORDER BY name", analyze=True,
    )
