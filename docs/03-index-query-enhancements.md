# Phase 3: Index Optimization & Query Analysis Enhancements

## Problem

Our current index analysis is passive -- it only reads `sys.dm_db_missing_index_*` DMVs. We need to actively analyze workloads, generate candidate indexes, and estimate impact using SQL Server's built-in capabilities.

---

## 3A. `analyze_query_indexes` Tool (New)

### Purpose

Analyze specific SQL queries to recommend indexes. SQL Server embeds `<MissingIndexes>` hints directly in execution plans -- we can extract these.

### Files
- **New:** `src/azure_sql_mcp/query_index_analysis.py`
- **Modify:** `src/azure_sql_mcp/server.py`

### Tool Definition

```python
@self.mcp.tool(
    description="Analyze up to 10 SQL queries and recommend optimal indexes based on execution plans.",
    annotations=ToolAnnotations(
        title="Analyze Query Indexes",
        readOnlyHint=True,
        openWorldHint=True,
    ),
)
async def analyze_query_indexes(
    queries: list[str] = Field(
        description="List of SQL SELECT queries to analyze (max 10)."
    ),
    database_name: str | None = Field(default=None),
) -> ResponseType:
```

### Implementation

```python
class QueryIndexAnalysisService:
    def __init__(self, executor: AzureSqlExecutor, validator: SafeSqlValidator):
        self.executor = executor
        self.validator = validator

    async def analyze_queries(
        self, database_name: str, queries: list[str]
    ) -> dict[str, Any]:
        if len(queries) > 10:
            raise ValueError("Maximum 10 queries per analysis.")

        recommendations = []
        for sql in queries:
            validated = self.validator.validate_read_only(sql)
            plan_xml = await self._get_estimated_plan(database_name, validated.normalized_sql)
            missing = self._extract_missing_indexes(plan_xml)
            recommendations.extend(missing)

        consolidated = self._consolidate_recommendations(recommendations)
        return {
            "queries_analyzed": len(queries),
            "recommendations": consolidated,
        }
```

### Missing Index Extraction from SHOWPLAN XML

SQL Server execution plans contain `<MissingIndexes>` elements:

```xml
<MissingIndexes>
  <MissingIndexGroup Impact="95.42">
    <MissingIndex Database="[appdb]" Schema="[dbo]" Table="[Orders]">
      <ColumnGroup Usage="EQUALITY">
        <Column Name="[CustomerId]" ColumnId="2" />
      </ColumnGroup>
      <ColumnGroup Usage="INEQUALITY">
        <Column Name="[OrderDate]" ColumnId="3" />
      </ColumnGroup>
      <ColumnGroup Usage="INCLUDE">
        <Column Name="[TotalAmount]" ColumnId="5" />
      </ColumnGroup>
    </MissingIndex>
  </MissingIndexGroup>
</MissingIndexes>
```

Parse with:
```python
SHOWPLAN_NS = {"sp": "http://schemas.microsoft.com/sqlserver/2004/07/showplan"}

def _extract_missing_indexes(self, plan_xml: str) -> list[dict]:
    root = ET.fromstring(plan_xml)
    results = []
    for group in root.findall(".//sp:MissingIndexGroup", SHOWPLAN_NS):
        impact = float(group.attrib.get("Impact", 0))
        for idx in group.findall("sp:MissingIndex", SHOWPLAN_NS):
            schema = idx.attrib.get("Schema", "").strip("[]")
            table = idx.attrib.get("Table", "").strip("[]")
            equality_cols = self._extract_column_group(idx, "EQUALITY")
            inequality_cols = self._extract_column_group(idx, "INEQUALITY")
            include_cols = self._extract_column_group(idx, "INCLUDE")
            results.append({
                "schema": schema,
                "table": table,
                "equality_columns": equality_cols,
                "inequality_columns": inequality_cols,
                "include_columns": include_cols,
                "impact_pct": impact,
                "create_index_sql": self._build_create_index(schema, table, ...),
            })
    return results
```

SHOWPLAN missing-index entries are optimizer hints for the specific plan that
was compiled. A successful per-query result with `missing_index_count: 0`
means that no `MissingIndexGroup` was emitted for that plan; it does not prove that no index
could improve the query. A failed plan analysis reports
`analysis_status: unavailable` and `missing_index_count: null`, so it cannot be
misread as zero hints. Top-level `raw_missing_index_hint_count` is measured
before existing-index filtering and consolidation, while
`recommendation_count_after_filtering` is the actionable output count. An
empty eligible Query Store workload reports `analysis_status: no_evidence`
and remains inconclusive rather than becoming a zero-hint claim. Review
existing indexes, workload evidence, and Query Store history before treating
an empty recommendation set as a decision.

### Consolidation Logic

When multiple queries recommend the same index (same table + key columns), consolidate:
- Merge include columns (union)
- Sum impact across queries
- Deduplicate against existing indexes

---

## 3B. `analyze_workload_indexes` Tool (New)

### Purpose

Automatically identify the most impactful indexes for the current workload by combining Query Store data with execution plan analysis.

### Files
- **Modify:** `src/azure_sql_mcp/index_recommendations.py`
- **Modify:** `src/azure_sql_mcp/server.py`

### Tool Definition

```python
@self.mcp.tool(
    description="Analyze the database workload to identify resource-intensive queries and recommend optimal indexes.",
    annotations=ToolAnnotations(
        title="Analyze Workload Indexes",
        readOnlyHint=True,
        openWorldHint=True,
    ),
)
async def analyze_workload_indexes(
    window_minutes: int = Field(default=60),
    top_n: int = Field(default=20, description="Number of top queries to analyze."),
    database_name: str | None = Field(default=None),
) -> ResponseType:
```

### Implementation Flow

1. **Pull top N resource-heavy queries from Query Store**
   ```sql
   SELECT TOP (?) qt.query_sql_text, ...
   FROM sys.query_store_query_text AS qt
   -- joins ...
   ORDER BY SUM(rs.avg_cpu_time * rs.count_executions) DESC
   ```

2. **Get execution plans for each query** (estimated plans via SHOWPLAN_XML)

3. **Extract `<MissingIndexes>` from each plan** (reuse logic from 3A)

4. **Cross-reference with `sys.dm_db_missing_index_*` DMVs** for additional context (user_seeks, avg_user_impact)

5. **Consolidate overlapping recommendations** -- merge indexes with same key columns

6. **Rank by estimated total workload improvement:**
   ```
   score = impact_pct * query_total_cpu_time * query_executions
   ```

7. **Return ranked recommendations with ready-to-execute CREATE INDEX statements**

### Response Shape

```json
{
  "database_name": "appdb",
  "window_minutes": 60,
  "queries_analyzed": 20,
  "missing_index_provenance": {
    "source": "SHOWPLAN_XML",
    "evidence_kind": "optimizer_missing_index_hint",
    "zero_hint_is_not_proof_no_index_can_help": true
  },
  "recommendations": [
    {
      "rank": 1,
      "schema": "dbo",
      "table": "Orders",
      "key_columns": ["CustomerId", "OrderDate"],
      "include_columns": ["TotalAmount"],
      "impact_score": 1250.5,
      "affected_queries": 3,
      "create_index_sql": "CREATE INDEX [IX_Orders_CustomerId_OrderDate] ON [dbo].[Orders] ([CustomerId], [OrderDate]) INCLUDE ([TotalAmount]);",
      "estimated_size_kb": null
    }
  ],
  "dmv_recommendations": [ ... ]
}
```

---

## 3C. `get_top_queries` Enhancement - Resource Blend Sort

### Purpose

Add a `resource_blend` sort option that considers multiple resource dimensions simultaneously.

### Files
- **Modify:** `src/azure_sql_mcp/query_store.py`
- **Modify:** `src/azure_sql_mcp/server.py`

### Implementation

Add new sort expressions:

```python
SORT_BY_EXPRESSIONS = {
    "total_duration": "SUM(rs.avg_duration * rs.count_executions)",
    "avg_duration": "AVG(rs.avg_duration)",
    "cpu": "SUM(rs.avg_cpu_time * rs.count_executions)",
    "executions": "SUM(rs.count_executions)",
    # NEW
    "logical_io": "SUM(rs.avg_logical_io_reads * rs.count_executions)",
    "physical_io": "SUM(rs.avg_physical_io_reads * rs.count_executions)",
    "memory": "AVG(rs.avg_query_max_used_memory)",
}
```

For `resource_blend`, use a separate query with subquery-based normalization:

```sql
WITH QueryMetrics AS (
    SELECT
        q.query_id, p.plan_id, qt.query_sql_text,
        SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu,
        SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_io,
        SUM(rs.avg_duration * rs.count_executions) AS total_duration,
        SUM(rs.count_executions) AS total_executions
    FROM sys.query_store_query_text AS qt
    -- joins ...
    GROUP BY q.query_id, p.plan_id, qt.query_sql_text
),
MaxMetrics AS (
    SELECT MAX(total_cpu) AS max_cpu, MAX(total_io) AS max_io, MAX(total_duration) AS max_dur
    FROM QueryMetrics
)
SELECT TOP (?) qm.*,
    (CAST(qm.total_cpu AS FLOAT) / NULLIF(mm.max_cpu, 0)
   + CAST(qm.total_io AS FLOAT) / NULLIF(mm.max_io, 0)
   + CAST(qm.total_duration AS FLOAT) / NULLIF(mm.max_dur, 0)) / 3.0 AS resource_blend_score
FROM QueryMetrics AS qm
CROSS JOIN MaxMetrics AS mm
ORDER BY resource_blend_score DESC
```

Update tool description to list all valid sort options.

---

## 3D. Explain Query Safety Decision

### Current v1 behavior

`explain_query` is intentionally enforced as read-only and now rejects hypothetical index payloads.

### Why this changed

- Creating statistics-only indexes requires write-level permissions and can change metadata/state.
- The tool contract for `explain_query` in restricted mode is read-only execution + plan retrieval.
- Mixing read-only plan inspection with write-capable what-if flows weakens operator expectations.

### Supported path instead

- Use `analyze_query_indexes` for per-query index opportunities.
- Use `analyze_workload_indexes` for Query Store-driven workload recommendations.
- Keep `explain_query` focused on estimated/actual plan generation only.

### Future option (explicit)

If what-if index simulation is brought back, expose it as a distinct unrestricted tool with explicit warnings rather than overloading `explain_query`.

---

## Verification

1. **analyze_query_indexes:** Submit 3 known slow queries, verify missing index recommendations match what SSMS shows
2. **analyze_workload_indexes:** Run against a database with active Query Store, verify recommendations are ranked sensibly
3. **resource_blend sort:** Compare output with total_duration sort -- resource_blend should surface IO-heavy queries that duration-only misses
4. **Explain safety guard:** Provide `hypothetical_indexes` to `explain_query` and verify the tool returns a clear "disabled for safety" error.
