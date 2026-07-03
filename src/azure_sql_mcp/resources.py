from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from .artifact_store import ArtifactStore
from .artifacts import json_text
from .config import ServerConfig
from .introspection import IntrospectionService


def register_resources(
    mcp: FastMCP,
    config: ServerConfig,
    introspection: IntrospectionService,
    artifact_store: ArtifactStore | None = None,
) -> None:
    """Register MCP resource templates for schema browsing."""

    @mcp.resource(
        "azuresql://{database}/schemas",
        description="List all schemas in a database.",
        mime_type="application/json",
    )
    async def database_schemas(database: str) -> str:
        """List all schemas in the given database."""
        resolved = config.validate_database_name(database)
        result = await introspection.list_schemas(resolved)
        return json_text(result)

    @mcp.resource(
        "azuresql://{database}/{schema}/tables",
        description="List all tables in a schema.",
        mime_type="application/json",
    )
    async def schema_tables(database: str, schema: str) -> str:
        """List all tables in a schema."""
        resolved = config.validate_database_name(database)
        result = await introspection.list_objects(resolved, schema, "table")
        return json_text(result)

    @mcp.resource(
        "azuresql://{database}/{schema}/views",
        description="List all views in a schema.",
        mime_type="application/json",
    )
    async def schema_views(database: str, schema: str) -> str:
        """List all views in a schema."""
        resolved = config.validate_database_name(database)
        result = await introspection.list_objects(resolved, schema, "view")
        return json_text(result)

    @mcp.resource(
        "azuresql://{database}/{schema}/procedures",
        description="List all stored procedures in a schema.",
        mime_type="application/json",
    )
    async def schema_procedures(database: str, schema: str) -> str:
        """List all stored procedures in a schema."""
        resolved = config.validate_database_name(database)
        result = await introspection.list_objects(resolved, schema, "procedure")
        return json_text(result)

    # Register the catch-all table-details resource last so more specific
    # "/views" and "/procedures" templates win during URI template matching.
    @mcp.resource(
        "azuresql://{database}/{schema}/{table}",
        description="Get columns, constraints, and indexes for a table.",
        mime_type="application/json",
    )
    async def table_details(database: str, schema: str, table: str) -> str:
        """Get detailed information about a table."""
        resolved = config.validate_database_name(database)
        result = await introspection.get_object_details(resolved, schema, table, "table")
        return json_text(result)

    if artifact_store is not None:

        @mcp.resource(
            "azuresql-artifact://{artifact_id}",
            description="Read a token-safe Azure SQL MCP artifact by id.",
            mime_type="text/plain",
        )
        async def artifact_text(artifact_id: str) -> str:
            """Read an artifact captured by a tool response."""
            return artifact_store.get(artifact_id).text
