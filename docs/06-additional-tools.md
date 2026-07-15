# Phase 6: Additional High-Value Tools

## Problem

The current tool surface is limited to schema introspection and performance analysis. Common DBA tasks like searching for objects, understanding dependencies, checking table sizes, and monitoring active sessions require multiple manual steps.

---

## 6A. `search_objects` Tool

### Purpose

Find database objects across all schemas by name pattern. Eliminates the need to `list_objects` schema-by-schema.

### Files
- **Modify:** `src/azure_sql_mcp/introspection.py`
- **Modify:** `src/azure_sql_mcp/server.py`

### Tool Definition

```python
@self.mcp.tool(
    description="Search for database objects by name pattern across all schemas.",
    annotations=ToolAnnotations(
        title="Search Objects",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
async def search_objects(
    pattern: str = Field(description="SQL LIKE pattern, e.g. '%User%' or 'Order%'."),
    object_type: str | None = Field(
        default=None,
        description="Optional filter: table, view, procedure, function. Omit for all types.",
    ),
    database_name: str | None = Field(default=None),
) -> ResponseType:
```

### SQL Query

```sql
SELECT
    s.name AS schema_name,
    o.name AS object_name,
    o.type_desc AS object_type,
    o.create_date,
    o.modify_date
FROM sys.objects AS o
INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id
WHERE o.name LIKE ?
  AND o.is_ms_shipped = 0
  AND (? IS NULL OR o.type IN (SELECT value FROM STRING_SPLIT(?, ',')))
ORDER BY s.name, o.name
```

Params: `[pattern, type_codes_csv, type_codes_csv]`

---

## 6B. `get_dependencies` Tool

### Purpose

Show what a given object depends on (references) and what depends on it (referenced by). Critical for impact analysis before schema changes.

### Files
- **Modify:** `src/azure_sql_mcp/introspection.py`
- **Modify:** `src/azure_sql_mcp/server.py`

### Tool Definition

```python
@self.mcp.tool(
    description="Get dependency graph for a database object: what it references and what references it.",
    annotations=ToolAnnotations(
        title="Get Dependencies",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
async def get_dependencies(
    schema_name: str = Field(description="Schema name."),
    object_name: str = Field(description="Object name."),
    database_name: str | None = Field(default=None),
) -> ResponseType:
```

### SQL Queries

**Objects this object depends on:**
```sql
SELECT
    COALESCE(referenced_schema_name, 'dbo') AS referenced_schema,
    referenced_entity_name AS referenced_object,
    referenced_minor_name AS referenced_column,
    sed.referencing_class_desc
FROM sys.sql_expression_dependencies AS sed
WHERE sed.referencing_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
ORDER BY referenced_schema, referenced_object
```

**Objects that depend on this object:**
```sql
SELECT
    OBJECT_SCHEMA_NAME(sed.referencing_id) AS referencing_schema,
    OBJECT_NAME(sed.referencing_id) AS referencing_object,
    o.type_desc AS referencing_type
FROM sys.sql_expression_dependencies AS sed
INNER JOIN sys.objects AS o ON sed.referencing_id = o.object_id
WHERE sed.referenced_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
ORDER BY referencing_schema, referencing_object
```

### Response Shape

```json
{
  "schema_name": "dbo",
  "object_name": "Orders",
  "depends_on": [
    {"schema": "dbo", "object": "Customers", "column": "CustomerId", "type": "FOREIGN_KEY"}
  ],
  "depended_on_by": [
    {"schema": "dbo", "object": "vw_OrderSummary", "type": "VIEW"},
    {"schema": "dbo", "object": "sp_ProcessOrders", "type": "SQL_STORED_PROCEDURE"}
  ]
}
```

---

## 6C. `get_table_stats` Tool

### Purpose

Get approximate row counts and sizes for tables without expensive table scans. Uses DMVs for fast results.

### Files
- **Modify:** `src/azure_sql_mcp/introspection.py`
- **Modify:** `src/azure_sql_mcp/server.py`

### Tool Definition

```python
@self.mcp.tool(
    description="Get approximate row counts and storage sizes for tables.",
    annotations=ToolAnnotations(
        title="Get Table Stats",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
async def get_table_stats(
    schema_name: str | None = Field(
        default=None,
        description="Optional schema filter. Omit for all schemas.",
    ),
    database_name: str | None = Field(default=None),
) -> ResponseType:
```

### SQL Query

```sql
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    SUM(p.rows) AS approximate_row_count,
    CAST(SUM(au.total_pages) * 8.0 / 1024 AS DECIMAL(18, 2)) AS total_size_mb,
    CAST(SUM(au.used_pages) * 8.0 / 1024 AS DECIMAL(18, 2)) AS used_size_mb,
    CAST(SUM(CASE WHEN au.type = 1 THEN au.used_pages ELSE 0 END) * 8.0 / 1024
         AS DECIMAL(18, 2)) AS data_size_mb,
    CAST(SUM(CASE WHEN au.type = 2 THEN au.used_pages ELSE 0 END) * 8.0 / 1024
         AS DECIMAL(18, 2)) AS index_size_mb,
    COUNT(DISTINCT i.index_id) - 1 AS index_count  -- Subtract heap/clustered
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
INNER JOIN sys.partitions AS p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
INNER JOIN sys.allocation_units AS au ON p.partition_id = au.container_id
LEFT JOIN sys.indexes AS i ON t.object_id = i.object_id AND i.index_id > 0
WHERE (? IS NULL OR s.name = ?)
GROUP BY s.name, t.name
ORDER BY SUM(p.rows) DESC
```

---

## 6D. `get_active_sessions` Tool

### Purpose

Show currently running queries, blocking chains, and session status. Essential for troubleshooting live performance issues.

### Files
- **New:** `src/azure_sql_mcp/sessions.py`
- **Modify:** `src/azure_sql_mcp/server.py`

### Tool Definition

```python
@self.mcp.tool(
    description="List active sessions and running queries, including blocking information.",
    annotations=ToolAnnotations(
        title="Get Active Sessions",
        readOnlyHint=True,
        openWorldHint=True,
    ),
)
async def get_active_sessions(
    database_name: str | None = Field(default=None),
) -> ResponseType:
```

### SQL Query

```sql
SELECT
    r.session_id,
    s.login_name,
    s.status AS session_status,
    r.status AS request_status,
    r.command,
    r.wait_type,
    r.wait_time AS wait_time_ms,
    r.blocking_session_id,
    r.cpu_time AS cpu_time_ms,
    r.total_elapsed_time AS elapsed_time_ms,
    r.reads AS logical_reads,
    r.writes,
    r.row_count,
    CAST(r.granted_query_memory * 8.0 / 1024 AS DECIMAL(18, 2)) AS granted_memory_mb,
    SUBSTRING(st.text,
        (r.statement_start_offset / 2) + 1,
        (CASE r.statement_end_offset
            WHEN -1 THEN DATALENGTH(st.text)
            ELSE r.statement_end_offset
        END - r.statement_start_offset) / 2 + 1
    ) AS current_statement,
    qp.query_plan AS execution_plan_xml
FROM sys.dm_exec_requests AS r
INNER JOIN sys.dm_exec_sessions AS s ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) AS qp
WHERE s.is_user_process = 1
  AND r.session_id != @@SPID  -- Exclude our own session
ORDER BY r.total_elapsed_time DESC
```

### Response Enhancement

Add blocking chain detection:
```python
def _detect_blocking_chains(self, sessions: list[dict]) -> list[dict]:
    """Identify head blockers and blocked session chains."""
    blocked = {s["session_id"]: s for s in sessions if s.get("blocking_session_id")}
    head_blockers = set()
    for s in blocked.values():
        blocker = s["blocking_session_id"]
        if blocker not in blocked:
            head_blockers.add(blocker)
    # Build chain from each head blocker
    ...
```

---

## 6E. Parallel Introspection Queries

### Purpose

`get_object_details` for tables currently runs 4 sequential queries (columns, constraints, indexes, definition). With connection pooling, these can run concurrently.

### Files
- **Modify:** `src/azure_sql_mcp/introspection.py`

### Implementation

```python
async def _get_table_or_view_details(self, database_name, schema_name, object_name, object_type):
    params = [schema_name, object_name]

    columns, constraints, indexes, definition_rows = await asyncio.gather(
        self.executor.fetch_all(database_name, columns_query, params=params),
        self.executor.fetch_all(
            database_name, constraints_query,
            params=[schema_name, object_name, schema_name, object_name],
        ),
        self.executor.fetch_all(database_name, indexes_query, params=params),
        self.executor.fetch_all(database_name, definition_query, params=params),
    )

    return {
        "schema_name": schema_name,
        "object_name": object_name,
        "object_type": object_type,
        "columns": columns,
        "constraints": constraints,
        "indexes": indexes,
        "definition": definition_rows[0]["definition"] if definition_rows else None,
    }
```

**Impact:** Reduces `get_object_details` latency from ~4x single-query to ~1x (assuming pool has sufficient connections).

---

## Verification

1. **search_objects:** Search for `%Order%` and verify it finds tables, views, and procedures with "Order" in the name
2. **get_dependencies:** Check dependencies for a view that references multiple tables
3. **get_table_stats:** Compare row counts with `SELECT COUNT(*)` for accuracy (should be close but not exact)
4. **get_active_sessions:** Run a long query in one session, verify it appears in the output
5. **Parallel queries:** Time `get_object_details` before and after -- should be noticeably faster
