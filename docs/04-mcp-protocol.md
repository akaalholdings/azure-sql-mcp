# Phase 4: MCP Protocol Completeness

## Problem

The MCP protocol defines three pillars: **Tools**, **Resources**, and **Prompts**. Our server only uses Tools. Adding Resources and Prompts makes this the most complete Azure SQL MCP server available.

---

## 4A. MCP Resources

### Purpose

Resources make schema metadata discoverable in the client's resource browser without requiring the LLM to know which tool to call. They provide structured, browsable access to database schema.

### Files
- **New:** `src/azure_sql_mcp/resources.py`
- **Modify:** `src/azure_sql_mcp/server.py`

### Resource Templates

Using FastMCP's `@mcp.resource()` decorator:

#### 1. Database Schemas

```python
@mcp.resource("azuresql://{database}/schemas")
async def get_schemas(database: str) -> list[dict]:
    """List all schemas in the database."""
    config.validate_database_name(database)
    return await introspection.list_schemas(database)
```

**URI:** `azuresql://appdb/schemas`
**Returns:** List of `{schema_name, schema_owner, schema_type}`

#### 2. Tables in Schema

```python
@mcp.resource("azuresql://{database}/{schema}/tables")
async def get_tables(database: str, schema: str) -> list[dict]:
    """List all tables in the schema."""
    config.validate_database_name(database)
    return await introspection.list_objects(database, schema, "table")
```

**URI:** `azuresql://appdb/dbo/tables`
**Returns:** List of `{schema_name, object_name, object_type, create_date, modify_date}`

#### 3. Table Details

```python
@mcp.resource("azuresql://{database}/{schema}/{table}")
async def get_table_details(database: str, schema: str, table: str) -> dict:
    """Get columns, constraints, and indexes for a table."""
    config.validate_database_name(database)
    return await introspection.get_object_details(database, schema, table, "table")
```

**URI:** `azuresql://appdb/dbo/Orders`
**Returns:** `{schema_name, object_name, columns: [...], constraints: [...], indexes: [...]}`

#### 4. Views in Schema

```python
@mcp.resource("azuresql://{database}/{schema}/views")
async def get_views(database: str, schema: str) -> list[dict]:
    config.validate_database_name(database)
    return await introspection.list_objects(database, schema, "view")
```

#### 5. Procedures in Schema

```python
@mcp.resource("azuresql://{database}/{schema}/procedures")
async def get_procedures(database: str, schema: str) -> list[dict]:
    config.validate_database_name(database)
    return await introspection.list_objects(database, schema, "procedure")
```

### Module Structure

```python
# resources.py
from .config import ServerConfig
from .introspection import IntrospectionService

def register_resources(
    mcp: FastMCP,
    config: ServerConfig,
    introspection: IntrospectionService,
) -> None:
    # Register all resource templates here
    # This keeps server.py from growing too large
```

Call from `server.py`:
```python
from .resources import register_resources

class AzureSqlMcpApplication:
    def __init__(self, config):
        ...
        self._register_tools()
        register_resources(self.mcp, self.config, self.introspection)
```

---

## 4B. MCP Prompts

### Purpose

Prompts expose pre-built prompt templates that MCP clients can offer to users. They guide the LLM through multi-step database analysis workflows, dramatically improving the UX.

### Files
- **New:** `src/azure_sql_mcp/prompts.py`
- **Modify:** `src/azure_sql_mcp/server.py`

### Prompt Definitions

#### 1. `analyze-slow-queries`

```python
@mcp.prompt(
    description="Investigate slow-running queries using Query Store and execution plans.",
)
async def analyze_slow_queries(
    database_name: str = "default",
    window_minutes: int = 60,
) -> list[types.PromptMessage]:
    return [
        types.PromptMessage(
            role="user",
            content=types.TextContent(
                type="text",
                text=(
                    f"Analyze slow queries in the '{database_name}' database "
                    f"over the last {window_minutes} minutes.\n\n"
                    "Steps:\n"
                    "1. Use get_top_queries to find the slowest queries\n"
                    "2. For the top 3 slowest, use explain_query to get execution plans\n"
                    "3. Use analyze_index_recommendations to check for missing indexes\n"
                    "4. Summarize findings with specific optimization recommendations"
                ),
            ),
        )
    ]
```

#### 2. `review-index-health`

```python
@mcp.prompt(
    description="Review index health: fragmentation, unused indexes, duplicates, and missing indexes.",
)
async def review_index_health(database_name: str = "default") -> list[types.PromptMessage]:
    return [
        types.PromptMessage(
            role="user",
            content=types.TextContent(
                type="text",
                text=(
                    f"Perform a complete index health review on '{database_name}'.\n\n"
                    "Steps:\n"
                    "1. Use analyze_db_health with health_type='index' to check fragmentation and unused indexes\n"
                    "2. Use analyze_index_recommendations for missing index suggestions\n"
                    "3. Identify duplicate indexes that waste space and slow writes\n"
                    "4. Provide a prioritized action plan: which indexes to create, rebuild, or drop"
                ),
            ),
        )
    ]
```

#### 3. `explore-schema`

```python
@mcp.prompt(
    description="Explore a database schema: tables, relationships, and key objects.",
)
async def explore_schema(
    database_name: str = "default",
    schema_name: str = "dbo",
) -> list[types.PromptMessage]:
    return [
        types.PromptMessage(
            role="user",
            content=types.TextContent(
                type="text",
                text=(
                    f"Explore the '{schema_name}' schema in '{database_name}'.\n\n"
                    "Steps:\n"
                    "1. Use list_schemas to see all available schemas\n"
                    "2. Use list_objects to enumerate tables, views, and procedures\n"
                    "3. For the most important tables (by relationship count), use get_object_details\n"
                    "4. Describe the schema structure, key relationships, and notable patterns"
                ),
            ),
        )
    ]
```

#### 4. `compare-schemas`

```python
@mcp.prompt(
    description="Compare schemas between two databases to find differences.",
)
async def compare_schemas(
    source_database: str,
    target_database: str,
) -> list[types.PromptMessage]:
    return [
        types.PromptMessage(
            role="user",
            content=types.TextContent(
                type="text",
                text=(
                    f"Compare the schemas of '{source_database}' and '{target_database}'.\n\n"
                    "Steps:\n"
                    "1. Use compare_schemas to find all differences\n"
                    "2. Categorize differences: added, removed, and modified objects\n"
                    "3. Use generate_migration_script to create a migration script\n"
                    "4. Review the script for safety and correctness"
                ),
            ),
        )
    ]
```

#### 5. `troubleshoot-performance`

```python
@mcp.prompt(
    description="Comprehensive performance troubleshooting: health, queries, indexes, and resources.",
)
async def troubleshoot_performance(database_name: str = "default") -> list[types.PromptMessage]:
    return [
        types.PromptMessage(
            role="user",
            content=types.TextContent(
                type="text",
                text=(
                    f"Troubleshoot performance issues in '{database_name}'.\n\n"
                    "Steps:\n"
                    "1. Use analyze_db_health with health_type='all' for a full health assessment\n"
                    "2. Use get_top_queries with sort_by='resource_blend' to find resource-heavy queries\n"
                    "3. Use explain_query on the top 3 problem queries\n"
                    "4. Use analyze_workload_indexes for workload-driven index recommendations\n"
                    "5. Check active sessions with get_active_sessions for blocking or long-running queries\n"
                    "6. Provide a prioritized remediation plan"
                ),
            ),
        )
    ]
```

### Module Structure

```python
# prompts.py
def register_prompts(mcp: FastMCP, config: ServerConfig) -> None:
    # Register all prompts here
```

---

## 4C. Tool Annotation Updates

Update every tool in `server.py` to include all relevant annotations:

```python
annotations=ToolAnnotations(
    title="...",
    readOnlyHint=True,
    idempotentHint=True,    # NEW for list_* tools
    openWorldHint=True,      # NEW for all database-accessing tools
)
```

See [02-health-checks.md](02-health-checks.md) Section 2H for the complete annotation table.

---

## Verification

1. **Resources:** Use the MCP Inspector to browse `azuresql://appdb/schemas`, `azuresql://appdb/dbo/tables`, etc.
2. **Prompts:** Use Claude Desktop and verify prompts appear in the prompt picker
3. **Annotations:** Inspect tool metadata via MCP Inspector and verify all hints are correct
4. **Integration:** Verify that using a prompt like `analyze-slow-queries` guides Claude through the correct tool sequence
