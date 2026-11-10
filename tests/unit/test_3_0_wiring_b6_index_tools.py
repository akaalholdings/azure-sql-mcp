"""B6 wiring: the index change plan and index detail in get_object_details."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import parse_candidate_key
from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_server import make_config
from tests.unit.test_server import make_read_policy


async def _call(app: AzureSqlMcpApplication, name: str, arguments: dict[str, Any]) -> Any:
    return await app.mcp._tool_manager.call_tool(name, arguments)


@pytest.fixture
def app(tmp_path: Path) -> AzureSqlMcpApplication:
    application = AzureSqlMcpApplication(make_config(tmp_path))
    application.database_policy = make_read_policy("appdb")
    return application


_REPORT = {
    "database_name": "appdb",
    "recommendations": [
        {
            "action": "create_index",
            "schema": "dbo",
            "table": "Posts",
            "index_name": "IX_Posts_OwnerUserId",
            "ddl": "CREATE NONCLUSTERED INDEX [IX_Posts_OwnerUserId] ON [dbo].[Posts] ([OwnerUserId]);",
            "rollback_ddl": "DROP INDEX [IX_Posts_OwnerUserId] ON [dbo].[Posts];",
            "blockers": [],
        }
    ],
}


@pytest.mark.asyncio
async def test_index_review_returns_a_change_plan_only_on_request(
    app: AzureSqlMcpApplication, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A client schedules changes in an order: create, change, observe, remove.
    # The plan is large, so a review that only ranks advice does not carry it.
    app.workload_index_advisor.review = AsyncMock(return_value=dict(_REPORT))
    seen: list[dict[str, Any]] = []

    def plan(report: dict[str, Any], *, soak_days: int) -> dict[str, Any]:
        seen.append({"report": report, "soak_days": soak_days})
        return {"steps": [{"phase": "create"}], "soak_days": soak_days}

    monkeypatch.setattr("azure_sql_mcp.server.build_index_change_plan", plan)

    plain = await _call(app, "review_workload_indexes", {})
    planned = await _call(
        app, "review_workload_indexes", {"include_change_plan": True, "soak_days": 14}
    )

    assert "change_plan" not in plain
    assert planned["change_plan"] == {"steps": [{"phase": "create"}], "soak_days": 14}
    assert seen[0]["report"]["recommendations"] == _REPORT["recommendations"]
    assert seen[0]["soak_days"] == 14


@pytest.mark.asyncio
async def test_index_review_change_plan_is_built_by_the_index_engine(
    app: AzureSqlMcpApplication,
) -> None:
    app.workload_index_advisor.review = AsyncMock(return_value=dict(_REPORT))

    planned = await _call(app, "review_workload_indexes", {"include_change_plan": True})

    assert isinstance(planned["change_plan"], dict)
    assert planned["change_plan"]


def _details() -> dict[str, Any]:
    return {
        "schema_name": "dbo",
        "object_name": "Users",
        "object_type": "table",
        "columns": [
            {"column_name": "Id", "data_type": "int", "max_length": 4, "precision": 10, "scale": 0},
            {"column_name": "DisplayName", "data_type": "nvarchar", "max_length": 80, "precision": 0, "scale": 0},
            {"column_name": "AboutMe", "data_type": "nvarchar", "max_length": -1, "precision": 0, "scale": 0},
            {"column_name": "Code", "data_type": "varchar", "max_length": 12, "precision": 0, "scale": 0},
            {"column_name": "Price", "data_type": "decimal", "max_length": 9, "precision": 18, "scale": 2},
        ],
        "constraints": [],
        "indexes": [
            {"index_name": "IX_Users_Reputation", "index_type": "NONCLUSTERED", "is_unique": False,
             "is_primary_key": False, "key_columns": "Reputation"},
        ],
        "definition": None,
    }


@pytest.mark.asyncio
async def test_object_details_show_what_index_advice_needs(
    app: AzureSqlMcpApplication, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Agents queried sys.indexes by hand to learn an index was filtered and
    # what it included, and read nvarchar(40) as 80 characters.
    app.introspection.get_object_details = AsyncMock(return_value=_details())
    existing = ExistingIndex(
        schema="dbo",
        table="Users",
        index_id=3,
        name="IX_Users_Reputation",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("Reputation DESC"),),
        include_columns=("DisplayName", "Location"),
        filter_definition="([Reputation]>(1000))",
        is_unique=True,
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes", AsyncMock(return_value=[existing])
    )

    result = await _call(
        app, "get_object_details", {"schema_name": "dbo", "object_name": "Users"}
    )

    [index] = result["indexes"]
    assert index["include_columns"] == ["DisplayName", "Location"]
    assert index["filter_definition"] == "([Reputation]>(1000))"
    assert index["is_unique"] is True
    assert index["key_columns_ordered"] == [{"name": "Reputation", "direction": "DESC"}]
    columns = {column["column_name"]: column for column in result["columns"]}
    assert columns["DisplayName"]["length_chars"] == 40
    assert columns["DisplayName"]["declared_type"] == "nvarchar(40)"
    assert columns["AboutMe"]["declared_type"] == "nvarchar(max)"
    assert columns["Code"]["length_chars"] == 12
    assert columns["Price"]["declared_type"] == "decimal(18,2)"
    assert "length_chars" not in columns["Id"]


@pytest.mark.asyncio
async def test_object_details_survive_a_refused_index_read(
    app: AzureSqlMcpApplication, monkeypatch: pytest.MonkeyPatch
) -> None:
    app.introspection.get_object_details = AsyncMock(return_value=_details())
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(side_effect=RuntimeError("permission denied")),
    )

    result = await _call(
        app, "get_object_details", {"schema_name": "dbo", "object_name": "Users"}
    )

    assert result["indexes"][0]["key_columns"] == "Reputation"
    assert "include_columns" not in result["indexes"][0]
    assert "could not be read" in result["index_detail_note"]
