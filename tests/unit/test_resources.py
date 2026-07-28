from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import pytest

from azure_sql_mcp.artifact_store import ArtifactStore
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.resources import register_resources


@dataclass
class RegisteredResource:
    uri: str
    func: Callable[..., Awaitable[str]]
    description: str | None
    mime_type: str | None


class FakeMCP:
    def __init__(self) -> None:
        self.resources: dict[str, RegisteredResource] = {}

    def resource(
        self,
        uri: str,
        *,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        mime_type: str | None = None,
        icons: list[Any] | None = None,
        annotations: Any | None = None,
        meta: dict[str, Any] | None = None,
    ) -> Callable[[Callable[..., Awaitable[str]]], Callable[..., Awaitable[str]]]:
        def decorator(func: Callable[..., Awaitable[str]]) -> Callable[..., Awaitable[str]]:
            self.resources[uri] = RegisteredResource(
                uri=uri,
                func=func,
                description=description,
                mime_type=mime_type,
            )
            return func

        return decorator


class FakeIntrospection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def list_schemas(self, database_name: str) -> list[dict[str, Any]]:
        self.calls.append(("list_schemas", (database_name,)))
        return [{"schema_name": "dbo", "schema_owner": "dbo_owner", "schema_type": "user"}]

    async def list_objects(
        self,
        database_name: str,
        schema_name: str,
        object_type: str,
    ) -> list[dict[str, Any]]:
        self.calls.append(("list_objects", (database_name, schema_name, object_type)))
        return [
            {
                "schema_name": schema_name,
                "object_name": f"{object_type}_1",
                "object_type": object_type,
            }
        ]

    async def get_object_details(
        self,
        database_name: str,
        schema_name: str,
        object_name: str,
        object_type: str,
    ) -> dict[str, Any]:
        self.calls.append(
            ("get_object_details", (database_name, schema_name, object_name, object_type))
        )
        return {
            "schema_name": schema_name,
            "object_name": object_name,
            "object_type": object_type,
            "columns": [{"column_name": "id"}],
            "constraints": [],
            "indexes": [],
        }


def _database_policy(*, allow_read: bool = True) -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": allow_read,
                }
            },
        }
    )


@pytest.fixture
def registered_resources(sample_server_config: Any) -> tuple[FakeMCP, FakeIntrospection]:
    mcp = FakeMCP()
    introspection = FakeIntrospection()
    register_resources(
        mcp,
        sample_server_config,
        introspection,  # type: ignore[arg-type]
        database_policy=_database_policy(),
    )
    return mcp, introspection


def test_register_resources_exposes_expected_templates(registered_resources: tuple[FakeMCP, FakeIntrospection]) -> None:
    mcp, _ = registered_resources

    assert set(mcp.resources) == {
        "azuresql://{database}/schemas",
        "azuresql://{database}/{schema}/tables",
        "azuresql://{database}/{schema}/{table}",
        "azuresql://{database}/{schema}/views",
        "azuresql://{database}/{schema}/procedures",
    }
    assert list(mcp.resources) == [
        "azuresql://{database}/schemas",
        "azuresql://{database}/{schema}/tables",
        "azuresql://{database}/{schema}/views",
        "azuresql://{database}/{schema}/procedures",
        "azuresql://{database}/{schema}/{table}",
    ]
    assert not any(uri.startswith("azuresql-learning://") for uri in mcp.resources)
    assert mcp.resources["azuresql://{database}/schemas"].mime_type == "application/json"
    assert mcp.resources["azuresql://{database}/{schema}/{table}"].description == (
        "Get columns, constraints, and indexes for a table."
    )


@pytest.mark.asyncio
async def test_artifact_resource_returns_stored_text(sample_server_config: Any) -> None:
    mcp = FakeMCP()
    introspection = FakeIntrospection()
    store = ArtifactStore()
    metadata = store.put_text(
        kind="showplan-xml",
        text="<ShowPlanXML />",
        mime_type="application/xml",
    )

    register_resources(
        mcp,
        sample_server_config,
        introspection,  # type: ignore[arg-type]
        store,
        database_policy=_database_policy(),
    )

    assert "azuresql-artifact://{artifact_id}" in mcp.resources
    text = await mcp.resources["azuresql-artifact://{artifact_id}"].func(
        metadata["artifact_id"]
    )
    assert text == "<ShowPlanXML />"


@pytest.mark.asyncio
async def test_resource_handlers_validate_database_and_return_json_payloads(
    registered_resources: tuple[FakeMCP, FakeIntrospection],
) -> None:
    mcp, introspection = registered_resources

    schemas = json.loads(await mcp.resources["azuresql://{database}/schemas"].func("appdb"))
    tables = json.loads(
        await mcp.resources["azuresql://{database}/{schema}/tables"].func("appdb", "dbo")
    )
    details = json.loads(
        await mcp.resources["azuresql://{database}/{schema}/{table}"].func(
            "appdb",
            "dbo",
            "Orders",
        )
    )
    views = json.loads(
        await mcp.resources["azuresql://{database}/{schema}/views"].func("appdb", "dbo")
    )
    procedures = json.loads(
        await mcp.resources["azuresql://{database}/{schema}/procedures"].func(
            "appdb",
            "dbo",
        )
    )

    assert schemas == [{"schema_name": "dbo", "schema_owner": "dbo_owner", "schema_type": "user"}]
    assert tables == [{"schema_name": "dbo", "object_name": "table_1", "object_type": "table"}]
    assert details["object_name"] == "Orders"
    assert details["columns"] == [{"column_name": "id"}]
    assert views == [{"schema_name": "dbo", "object_name": "view_1", "object_type": "view"}]
    assert procedures == [
        {"schema_name": "dbo", "object_name": "procedure_1", "object_type": "procedure"}
    ]
    assert [
        call[0]
        for call in introspection.calls
    ] == [
        "list_schemas",
        "list_objects",
        "get_object_details",
        "list_objects",
        "list_objects",
    ]


@pytest.mark.asyncio
async def test_resource_handlers_reject_unknown_databases(
    registered_resources: tuple[FakeMCP, FakeIntrospection],
) -> None:
    mcp, _ = registered_resources

    with pytest.raises(ValueError, match="AZURE_SQL_ALLOWED_DATABASES"):
        await mcp.resources["azuresql://{database}/schemas"].func("unknown")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("uri", "arguments"),
    [
        ("azuresql://{database}/schemas", ("appdb",)),
        ("azuresql://{database}/{schema}/tables", ("appdb", "dbo")),
        ("azuresql://{database}/{schema}/views", ("appdb", "dbo")),
        ("azuresql://{database}/{schema}/procedures", ("appdb", "dbo")),
        ("azuresql://{database}/{schema}/{table}", ("appdb", "dbo", "Orders")),
    ],
)
async def test_schema_resources_enforce_read_policy_before_introspection(
    sample_server_config: Any,
    uri: str,
    arguments: tuple[str, ...],
) -> None:
    mcp = FakeMCP()
    introspection = FakeIntrospection()
    register_resources(
        mcp,
        sample_server_config,
        introspection,  # type: ignore[arg-type]
        database_policy=_database_policy(allow_read=False),
    )

    with pytest.raises(PermissionError, match="does not permit read access"):
        await mcp.resources[uri].func(*arguments)

    assert introspection.calls == []
