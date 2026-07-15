from __future__ import annotations

import pytest

from azure_sql_mcp.schema_snapshot import _build_programmable_objects
from azure_sql_mcp.schema_snapshot import _fetch_snapshot_rows


class StubExecutor:
    def __init__(self):
        self.calls = []

    async def fetch_all(self, database_name: str, query: str, params=None):
        self.calls.append((database_name, query, params))
        return []


@pytest.mark.asyncio
async def test_fetch_snapshot_rows_repeats_constraint_scope_params() -> None:
    executor = StubExecutor()

    await _fetch_snapshot_rows(executor, "appdb", ["mcp_integration"])

    assert len(executor.calls) == 6
    constraint_params = executor.calls[2][2]
    assert constraint_params == ["mcp_integration"] * 4
    # Sequences and triggers queries also use the schema scope param
    assert executor.calls[4][2] == ["mcp_integration"]
    assert executor.calls[5][2] == ["mcp_integration"]


def test_build_programmable_objects_normalizes_padded_type_codes() -> None:
    views, procedures, functions = _build_programmable_objects(
        [
            {
                "schema_name": "mcp_integration",
                "object_name": "vw_OrderSummary",
                "object_type": "V ",
                "definition": "SELECT 1",
            },
            {
                "schema_name": "mcp_integration",
                "object_name": "usp_GetOrders",
                "object_type": "P ",
                "definition": "SELECT 1",
            },
        ]
    )

    assert ("mcp_integration", "vw_OrderSummary") in views
    assert ("mcp_integration", "usp_GetOrders") in procedures
    assert functions == {}
