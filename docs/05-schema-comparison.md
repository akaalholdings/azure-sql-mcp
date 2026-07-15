# Phase 5: Schema Comparison (Our Differentiator)

## Problem

The repository is named "SchemaCompare" but has no schema comparison capability yet. This is our most differentiating capability.

---

## 5A. Schema Snapshot Model

### Files
- **New:** `src/azure_sql_mcp/schema_snapshot.py`

### Design

A `SchemaSnapshot` is a frozen-in-time, normalized representation of a database schema, suitable for comparison.

```python
@dataclass(frozen=True)
class ColumnDef:
    name: str
    data_type: str
    max_length: int
    precision: int
    scale: int
    is_nullable: bool
    default_definition: str | None

@dataclass(frozen=True)
class IndexDef:
    name: str
    index_type: str
    is_unique: bool
    is_primary_key: bool
    key_columns: tuple[str, ...]
    included_columns: tuple[str, ...]

@dataclass(frozen=True)
class ConstraintDef:
    name: str
    constraint_type: str  # PRIMARY_KEY, FOREIGN_KEY, CHECK, UNIQUE, DEFAULT
    columns: tuple[str, ...]
    referenced_schema: str | None  # For FKs
    referenced_table: str | None   # For FKs
    referenced_columns: tuple[str, ...] | None  # For FKs
    definition: str | None  # For CHECK constraints

@dataclass(frozen=True)
class TableSnapshot:
    schema_name: str
    table_name: str
    columns: tuple[ColumnDef, ...]
    indexes: tuple[IndexDef, ...]
    constraints: tuple[ConstraintDef, ...]

@dataclass(frozen=True)
class ProgrammableObjectSnapshot:
    schema_name: str
    object_name: str
    object_type: str  # PROCEDURE, VIEW, FUNCTION
    definition: str | None  # Normalized SQL text

@dataclass(frozen=True)
class SchemaSnapshot:
    database_name: str
    captured_at: str  # ISO timestamp
    tables: dict[tuple[str, str], TableSnapshot]      # (schema, name) -> snapshot
    views: dict[tuple[str, str], ProgrammableObjectSnapshot]
    procedures: dict[tuple[str, str], ProgrammableObjectSnapshot]
    functions: dict[tuple[str, str], ProgrammableObjectSnapshot]
```

### Capture Logic

```python
async def capture_snapshot(
    executor: AzureSqlExecutor,
    database_name: str,
    schema_filter: list[str] | None = None,
) -> SchemaSnapshot:
    # 1. List all schemas (filtered if schema_filter provided)
    # 2. For each schema, fetch all object types in parallel:
    #    - Tables: columns, indexes, constraints
    #    - Views: definition from sys.sql_modules
    #    - Procedures: definition from sys.sql_modules
    #    - Functions: definition from sys.sql_modules
    # 3. Return frozen SchemaSnapshot
```

### Bulk Query Strategy

Instead of N+1 queries per table, use bulk queries:

```sql
-- All columns across all tables in one shot
SELECT
    OBJECT_SCHEMA_NAME(c.object_id) AS schema_name,
    OBJECT_NAME(c.object_id) AS table_name,
    c.name AS column_name,
    t.name AS data_type,
    c.max_length, c.precision, c.scale, c.is_nullable,
    dc.definition AS default_definition
FROM sys.columns AS c
INNER JOIN sys.types AS t ON c.user_type_id = t.user_type_id
LEFT JOIN sys.default_constraints AS dc ON c.default_object_id = dc.object_id
INNER JOIN sys.objects AS o ON c.object_id = o.object_id
WHERE o.type IN ('U')
  AND OBJECT_SCHEMA_NAME(c.object_id) IN (?, ?, ...)
ORDER BY schema_name, table_name, c.column_id
```

Similar bulk queries for indexes, constraints, and definitions. This reduces the snapshot from potentially hundreds of queries to 5-6 bulk queries.

---

## 5B. Schema Diff Engine

### Files
- **New:** `src/azure_sql_mcp/schema_diff.py`

### Design

```python
class DiffType(str, Enum):
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"

class DiffCategory(str, Enum):
    TABLE = "table"
    COLUMN = "column"
    INDEX = "index"
    CONSTRAINT = "constraint"
    VIEW = "view"
    PROCEDURE = "procedure"
    FUNCTION = "function"

@dataclass(frozen=True)
class SchemaDifference:
    diff_type: DiffType
    category: DiffCategory
    schema_name: str
    object_name: str
    detail: str
    source_definition: Any | None = None
    target_definition: Any | None = None

def compare_snapshots(
    source: SchemaSnapshot,
    target: SchemaSnapshot,
) -> list[SchemaDifference]:
```

### Comparison Logic

#### Tables
1. **Set difference on keys:** `(schema, table_name)`
   - In target but not source -> `ADDED TABLE`
   - In source but not target -> `REMOVED TABLE`
   - In both -> compare columns, indexes, constraints

#### Columns (for tables present in both)
1. **Set difference on column names**
   - New column -> `ADDED COLUMN`
   - Missing column -> `REMOVED COLUMN`
   - Same name -> compare: data_type, max_length, precision, scale, is_nullable, default_definition
   - Any mismatch -> `MODIFIED COLUMN` with detail string describing changes

#### Indexes
1. Compare by index name
2. For matching names: compare key_columns, included_columns, is_unique

#### Constraints
1. Compare by constraint name
2. For matching names: compare type, columns, referenced table/columns

#### Views / Procedures / Functions
1. Set difference on keys
2. For matching objects: compare definition text
   - Normalize whitespace before comparing (strip leading/trailing, collapse internal)
   - If definitions differ -> `MODIFIED VIEW/PROCEDURE/FUNCTION`

### Example Output

```json
[
  {
    "diff_type": "added",
    "category": "table",
    "schema_name": "dbo",
    "object_name": "AuditLog",
    "detail": "Table [dbo].[AuditLog] exists in target but not in source"
  },
  {
    "diff_type": "modified",
    "category": "column",
    "schema_name": "dbo",
    "object_name": "Orders.TotalAmount",
    "detail": "Data type changed: decimal(18,2) -> decimal(18,4)"
  },
  {
    "diff_type": "removed",
    "category": "index",
    "schema_name": "dbo",
    "object_name": "IX_Orders_OldIndex",
    "detail": "Index exists in source but not in target"
  }
]
```

---

## 5C. DDL Migration Script Generation

### Files
- **New:** `src/azure_sql_mcp/ddl_generator.py`

### Design

```python
def generate_migration_script(
    differences: list[SchemaDifference],
    source_snapshot: SchemaSnapshot,
    target_snapshot: SchemaSnapshot,
) -> str:
```

### Statement Generation by Category

#### Added Table
```sql
CREATE TABLE [schema].[table] (
    [col1] int NOT NULL,
    [col2] nvarchar(100) NULL DEFAULT (N''),
    CONSTRAINT [PK_table] PRIMARY KEY CLUSTERED ([col1])
);
```

#### Removed Table
```sql
DROP TABLE [schema].[table];
```

#### Added Column
```sql
ALTER TABLE [schema].[table] ADD [column] datatype NULL|NOT NULL [DEFAULT (value)];
```

#### Removed Column
```sql
ALTER TABLE [schema].[table] DROP COLUMN [column];
```

#### Modified Column
```sql
ALTER TABLE [schema].[table] ALTER COLUMN [column] new_datatype NULL|NOT NULL;
```

#### Added/Removed Index
```sql
CREATE INDEX [name] ON [schema].[table] ([cols]) INCLUDE ([cols]);
-- or
DROP INDEX [name] ON [schema].[table];
```

#### Added/Removed Constraint
```sql
ALTER TABLE [schema].[table] ADD CONSTRAINT [name] ...;
-- or
ALTER TABLE [schema].[table] DROP CONSTRAINT [name];
```

#### Modified View/Procedure/Function
```sql
ALTER VIEW [schema].[name] AS ...;
ALTER PROCEDURE [schema].[name] AS ...;
ALTER FUNCTION [schema].[name] ...;
```

### Dependency Ordering

The script must be ordered for dependency correctness:

1. **Drop foreign key constraints** (that reference tables being modified)
2. **Drop indexes** being removed
3. **Drop constraints** being removed
4. **Drop tables** being removed
5. **Create new tables**
6. **Alter existing tables** (add/modify/drop columns)
7. **Create/alter views** (may depend on tables)
8. **Create/alter procedures/functions**
9. **Create new indexes**
10. **Create new constraints** (including FKs)

### Safety Features

- All identifiers wrapped with `QUOTENAME()` equivalent (`[brackets]`)
- Script includes `BEGIN TRANSACTION` / `COMMIT` wrapper
- Includes `PRINT` statements for progress tracking
- Comments describing each change

---

## 5D. MCP Tools

### Files
- **New:** `src/azure_sql_mcp/schema_compare.py`
- **Modify:** `src/azure_sql_mcp/server.py`

### Tool 1: `compare_schemas`

```python
@self.mcp.tool(
    description="Compare schemas between two databases and return all differences.",
    annotations=ToolAnnotations(
        title="Compare Schemas",
        readOnlyHint=True,
        openWorldHint=True,
    ),
)
async def compare_schemas(
    source_database: str = Field(description="Source database name."),
    target_database: str = Field(description="Target database name."),
    schema_filter: str | None = Field(
        default=None,
        description="Comma-separated schema names to compare. Defaults to all user schemas.",
    ),
) -> ResponseType:
```

**Returns:** List of `SchemaDifference` objects as JSON, grouped by category.

### Tool 2: `generate_migration_script`

```python
@self.mcp.tool(
    description="Generate a T-SQL migration script to transform source schema to match target.",
    annotations=ToolAnnotations(
        title="Generate Migration Script",
        readOnlyHint=True,
        openWorldHint=True,
    ),
)
async def generate_migration_script(
    source_database: str = Field(description="Source database name."),
    target_database: str = Field(description="Target database name."),
    schema_filter: str | None = Field(default=None),
) -> ResponseType:
```

**Returns:** T-SQL script as text.

### Tool 3: `capture_schema_snapshot`

```python
@self.mcp.tool(
    description="Capture a point-in-time schema snapshot for a database.",
    annotations=ToolAnnotations(
        title="Capture Schema Snapshot",
        readOnlyHint=True,
        openWorldHint=True,
    ),
)
async def capture_schema_snapshot(
    database_name: str | None = Field(default=None),
    schema_filter: str | None = Field(default=None),
) -> ResponseType:
```

**Returns:** Full schema snapshot as JSON.

### Orchestration Service

```python
class SchemaCompareService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def compare(
        self, source_db: str, target_db: str, schema_filter: list[str] | None
    ) -> dict[str, Any]:
        source_snap, target_snap = await asyncio.gather(
            capture_snapshot(self.executor, source_db, schema_filter),
            capture_snapshot(self.executor, target_db, schema_filter),
        )
        differences = compare_snapshots(source_snap, target_snap)
        return {
            "source_database": source_db,
            "target_database": target_db,
            "difference_count": len(differences),
            "differences": [d.__dict__ for d in differences],
            "summary": self._summarize(differences),
        }

    async def generate_script(
        self, source_db: str, target_db: str, schema_filter: list[str] | None
    ) -> str:
        source_snap, target_snap = await asyncio.gather(
            capture_snapshot(self.executor, source_db, schema_filter),
            capture_snapshot(self.executor, target_db, schema_filter),
        )
        differences = compare_snapshots(source_snap, target_snap)
        return generate_migration_script(differences, source_snap, target_snap)
```

---

## Verification

1. **Setup:** Create two test databases with known differences:
   - DB A: `Orders` table with `int` TotalAmount
   - DB B: `Orders` table with `decimal(18,4)` TotalAmount + `AuditLog` table + extra index
2. **Snapshot:** `capture_schema_snapshot` on both, verify all objects captured
3. **Compare:** `compare_schemas(A, B)` should show: modified column, added table, added index
4. **Script:** `generate_migration_script(A, B)` should produce valid T-SQL that transforms A to match B
5. **Execute:** Run the migration script on a copy of DB A, then re-compare -- should show zero differences
