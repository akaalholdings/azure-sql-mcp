from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
from mcp import types

from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.schema_compat import portable_input_schema
from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_server import make_config


def test_nullable_unions_collapse_and_defaults_move_into_the_description() -> None:
    schema = {
        "title": "toolArguments",
        "type": "object",
        "properties": {
            "query_id": {
                "anyOf": [{"type": "integer", "minimum": 1}, {"type": "null"}],
                "default": None,
                "title": "Query Id",
                "description": "Query Store query id.",
            },
            "window_minutes": {"type": "integer", "default": 60, "title": "Window Minutes"},
            "title": {"type": "string", "description": "A parameter literally named title."},
        },
        "required": ["title"],
    }

    portable = portable_input_schema(schema)

    assert portable["properties"]["query_id"] == {
        "type": "integer",
        "minimum": 1,
        "description": "Query Store query id.",
    }
    assert portable["properties"]["window_minutes"] == {"type": "integer", "description": "Default: 60."}
    assert "title" in portable["properties"]
    assert portable["required"] == ["title"]
    assert "title" not in portable


def test_references_are_inlined() -> None:
    schema = {
        "$defs": {
            "Objective": {"enum": ["cpu", "duration"], "type": "string", "title": "Objective"},
            "Case": {
                "type": "object",
                "properties": {"objective": {"$ref": "#/$defs/Objective"}},
                "title": "Case",
            },
        },
        "type": "object",
        "properties": {
            "cases": {"type": "array", "items": {"$ref": "#/$defs/Case"}},
            "objective": {"$ref": "#/$defs/Objective", "default": "cpu"},
        },
    }

    portable = portable_input_schema(schema)

    assert "$defs" not in portable
    assert portable["properties"]["cases"]["items"]["properties"]["objective"]["enum"] == ["cpu", "duration"]
    assert portable["properties"]["objective"] == {
        "enum": ["cpu", "duration"],
        "type": "string",
        "description": "Default: \"cpu\".",
    }
    assert "$ref" not in json.dumps(portable)


async def _wire_tools(app: AzureSqlMcpApplication) -> list[types.Tool]:
    handler = app.mcp._mcp_server.request_handlers[types.ListToolsRequest]
    result = await handler(types.ListToolsRequest(method="tools/list"))
    return result.root.tools


@pytest.mark.asyncio
async def test_wire_tool_list_is_portable_by_default(tmp_path: Path) -> None:
    app = AzureSqlMcpApplication(make_config(tmp_path, tool_groups=frozenset({ToolGroup.ALL})))

    wire = await _wire_tools(app)
    raw = await app.mcp.list_tools()

    assert [tool.name for tool in wire] == [tool.name for tool in raw]
    text = json.dumps([tool.inputSchema for tool in wire])
    assert '"$ref"' not in text
    assert '"default"' not in text
    assert '{"type": "null"}' not in text
    # Server-side models keep the full schema for validation and defaults.
    assert '"default"' in json.dumps([tool.inputSchema for tool in raw])
    assert len(text) < len(json.dumps([tool.inputSchema for tool in raw]))


@pytest.mark.asyncio
async def test_full_profile_keeps_raw_schemas(tmp_path: Path) -> None:
    app = AzureSqlMcpApplication(replace(make_config(tmp_path), schema_profile="full"))

    wire = await _wire_tools(app)

    assert '"default"' in json.dumps([tool.inputSchema for tool in wire])


def test_schema_profile_is_validated(monkeypatch: pytest.MonkeyPatch) -> None:
    from azure_sql_mcp.config import load_server_config

    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    monkeypatch.setenv("AZURE_SQL_SCHEMA_PROFILE", "loose")

    with pytest.raises(ValueError, match="AZURE_SQL_SCHEMA_PROFILE"):
        load_server_config([])
