# Phase 2: Health Check Parity & Enhancements

## Problem

Our `analyze_db_health` tool has 5 check categories (index, query_store, tuning, resource, storage). We need to expand coverage with additional Azure SQL health checks and threshold-based pass/fail scoring.

---

## Current Health Checks

| Category | What it checks | Status |
|----------|---------------|--------|
| `index` | Fragmented indexes (top 10, >1000 pages), unused indexes | Exists |
| `query_store` | Query Store status (enabled, storage, capture mode) | Exists |
| `tuning` | Automatic tuning options and recommendation count | Exists |
| `resource` | CPU, IO, log write, memory, XTP, workers, sessions, DTU (last 12 intervals) | Exists |
| `storage` | Database files with sizes | Exists |

## New Health Checks to Add

### 2A. `buffer` - Cache Hit Rates

```sql
-- Buffer cache hit ratio
SELECT
    CAST(a.cntr_value AS FLOAT) * 100.0
        / NULLIF(CAST(b.cntr_value AS FLOAT), 0) AS buffer_cache_hit_ratio
FROM sys.dm_os_performance_counters AS a
CROSS JOIN sys.dm_os_performance_counters AS b
WHERE a.counter_name = 'Buffer cache hit ratio'
  AND a.object_name LIKE '%Buffer Manager%'
  AND b.counter_name = 'Buffer cache hit ratio base'
  AND b.object_name LIKE '%Buffer Manager%'
```

```sql
-- Page life expectancy (how long pages stay in buffer)
SELECT cntr_value AS page_life_expectancy_seconds
FROM sys.dm_os_performance_counters
WHERE counter_name = 'Page life expectancy'
  AND object_name LIKE '%Buffer Manager%'
```

**Thresholds:**
- Buffer cache hit ratio < 95% -> `warning`
- Buffer cache hit ratio < 90% -> `critical`
- Page life expectancy < 300 seconds -> `warning`

**Note:** These DMVs may not be available on all Azure SQL tiers. Wrap in try/except for graceful degradation.

### 2B. `connection` - Connection Health

```sql
SELECT
    COUNT(*) AS total_sessions,
    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS active_requests,
    SUM(CASE WHEN status = 'sleeping' AND open_transaction_count > 0 THEN 1 ELSE 0 END)
        AS idle_with_open_transaction,
    SUM(CASE WHEN status = 'sleeping' THEN 1 ELSE 0 END) AS idle_sessions,
    (SELECT CAST(drs.user_sessions_limit AS INT)
     FROM sys.dm_user_db_resource_governance AS drs) AS session_limit
FROM sys.dm_exec_sessions
WHERE is_user_process = 1
```

**Thresholds:**
- total_sessions > 80% of session_limit -> `warning`
- total_sessions > 95% of session_limit -> `critical`
- idle_with_open_transaction > 20 -> `warning`

**Note:** `sys.dm_user_db_resource_governance` is Azure SQL specific. Fall back to `sys.configurations` for on-prem.

### 2C. `constraint` - Constraint Health

```sql
-- Untrusted foreign keys (optimizer can't use these for plan optimization)
SELECT
    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS schema_name,
    OBJECT_NAME(fk.parent_object_id) AS table_name,
    fk.name AS constraint_name,
    'FOREIGN_KEY' AS constraint_type,
    fk.is_disabled
FROM sys.foreign_keys AS fk
WHERE fk.is_not_trusted = 1
ORDER BY schema_name, table_name, fk.name
```

```sql
-- Untrusted check constraints
SELECT
    OBJECT_SCHEMA_NAME(cc.parent_object_id) AS schema_name,
    OBJECT_NAME(cc.parent_object_id) AS table_name,
    cc.name AS constraint_name,
    'CHECK' AS constraint_type,
    cc.is_disabled
FROM sys.check_constraints AS cc
WHERE cc.is_not_trusted = 1
ORDER BY schema_name, table_name, cc.name
```

**Thresholds:**
- Any untrusted constraints found -> `warning`
- Untrusted constraints on tables with >100K rows -> `critical`

**Remediation hint:** Include in response:
```sql
-- Fix with: ALTER TABLE [schema].[table] WITH CHECK CHECK CONSTRAINT [constraint_name]
```

### 2D. `replication` - Geo-Replication Health

```sql
-- Azure SQL geo-replication status
SELECT
    ag.name AS replication_group,
    drs.partner_server,
    drs.partner_database,
    drs.role_desc,
    drs.replication_state_desc,
    drs.synchronization_health_desc,
    drs.replication_lag_sec,
    drs.last_replication
FROM sys.dm_geo_replication_link_status AS drs
LEFT JOIN sys.availability_groups AS ag
    ON drs.group_id = ag.group_id
```

**Thresholds:**
- replication_lag_sec > 30 -> `warning`
- replication_lag_sec > 120 -> `critical`
- replication_state_desc != 'SEEDING' AND synchronization_health_desc = 'NOT_HEALTHY' -> `critical`

**Note:** Only available on Azure SQL Database with geo-replication configured. Returns empty if not set up.

### 2E. `identity` - Identity Column Exhaustion

```sql
SELECT
    OBJECT_SCHEMA_NAME(ic.object_id) AS schema_name,
    OBJECT_NAME(ic.object_id) AS table_name,
    c.name AS column_name,
    t.name AS data_type,
    ic.seed_value,
    ic.increment_value,
    ic.last_value,
    CASE t.name
        WHEN 'tinyint'  THEN 255
        WHEN 'smallint' THEN 32767
        WHEN 'int'      THEN 2147483647
        WHEN 'bigint'   THEN 9223372036854775807
    END AS max_value,
    CASE
        WHEN ic.last_value IS NULL THEN 0.0
        ELSE CAST(ic.last_value AS FLOAT)
            / CAST(CASE t.name
                WHEN 'tinyint'  THEN 255
                WHEN 'smallint' THEN 32767
                WHEN 'int'      THEN 2147483647
                WHEN 'bigint'   THEN 9223372036854775807
            END AS FLOAT) * 100.0
    END AS pct_used
FROM sys.identity_columns AS ic
INNER JOIN sys.columns AS c
    ON ic.object_id = c.object_id AND ic.column_id = c.column_id
INNER JOIN sys.types AS t
    ON c.system_type_id = t.system_type_id
WHERE t.name IN ('tinyint', 'smallint', 'int', 'bigint')
ORDER BY pct_used DESC
```

**Thresholds:**
- pct_used > 80% -> `warning`
- pct_used > 95% -> `critical`
- Only report `int` columns at >60% (bigint overflow is extremely unlikely)

### 2F. Enhance `index` - Duplicate Index Detection

**Add to existing index health check:**

```sql
-- Duplicate indexes (same key columns on same table)
WITH IndexKeyCols AS (
    SELECT
        i.object_id,
        i.index_id,
        i.name AS index_name,
        i.type_desc AS index_type,
        STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns
    FROM sys.indexes AS i
    INNER JOIN sys.index_columns AS ic
        ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN sys.columns AS c
        ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE ic.is_included_column = 0
      AND i.name IS NOT NULL
      AND i.type IN (1, 2)  -- Clustered and nonclustered only
    GROUP BY i.object_id, i.index_id, i.name, i.type_desc
)
SELECT
    OBJECT_SCHEMA_NAME(a.object_id) AS schema_name,
    OBJECT_NAME(a.object_id) AS table_name,
    a.index_name AS index_a,
    a.index_type AS type_a,
    b.index_name AS index_b,
    b.index_type AS type_b,
    a.key_columns
FROM IndexKeyCols AS a
INNER JOIN IndexKeyCols AS b
    ON a.object_id = b.object_id
   AND a.key_columns = b.key_columns
   AND a.index_id < b.index_id
ORDER BY schema_name, table_name, a.index_name
```

**Thresholds:**
- Any duplicate indexes found -> `warning`

---

## 2G. Threshold-Based Response Format

All health checks should return a consistent structure:

```json
{
  "database_name": "appdb",
  "checks": {
    "buffer": {
      "status": "warning",
      "details": {
        "buffer_cache_hit_ratio": 92.3,
        "page_life_expectancy_seconds": 280
      },
      "thresholds": {
        "buffer_cache_hit_ratio_warning": 95,
        "page_life_expectancy_warning": 300
      },
      "findings": [
        "Buffer cache hit ratio (92.3%) is below warning threshold (95%)",
        "Page life expectancy (280s) is below warning threshold (300s)"
      ]
    },
    "connection": {
      "status": "pass",
      "details": { ... },
      "thresholds": { ... },
      "findings": []
    }
  }
}
```

Status values: `pass`, `warning`, `critical`

---

## 2H. Complete Tool Annotations

Update all tool registrations in `server.py`:

| Tool | readOnlyHint | destructiveHint | idempotentHint | openWorldHint |
|------|-------------|----------------|---------------|---------------|
| list_databases | True | False | True | False |
| check_capabilities | True | False | True | True |
| list_schemas | True | False | True | True |
| list_objects | True | False | True | True |
| get_object_details | True | False | True | True |
| execute_sql | True | False | False | True |
| explain_query | True | False | False | True |
| get_top_queries | True | False | False | True |
| analyze_index_recommendations | True | False | True | True |
| analyze_db_health | True | False | True | True |
| execute_tsql_unrestricted | False | True | False | True |

---

## Files to Change

- **Modify:** `src/azure_sql_mcp/health.py` - Add buffer, connection, constraint, replication, identity checks; add threshold-based response format; enhance index check with duplicate detection
- **Modify:** `src/azure_sql_mcp/server.py` - Update tool annotations; update health_type validator to accept new categories

## Verification

1. Run `analyze_db_health` with `health_type="all"` against a real Azure SQL DB
2. Verify each new category returns data (or gracefully degrades if DMV is unavailable)
3. Verify threshold-based status is correct (create a test database with known conditions)
4. Verify duplicate index detection by creating two identical indexes on a test table
5. Verify identity exhaustion by creating a table with a tinyint identity near its max
