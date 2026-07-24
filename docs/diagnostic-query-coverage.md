# Azure SQL Database diagnostic coverage

This matrix defines the platform knowledge built into the MCP server. It is
specific to Azure SQL Database PaaS. Server-level SQL Server checks are excluded
when the managed service does not expose them or when they cannot lead to a
database-scoped action.

Every returned diagnostic should identify its collection window, units,
availability, truncation state, and provenance. Missing or partial evidence must
produce `partial` or `inconclusive`, never `healthy`.

## Query performance

| Diagnostic concern | MCP coverage | Interpretation boundary |
|---|---|---|
| Query Store regressions, runtime history, plans, and parameter-sensitive behavior | `get_top_queries`, `get_query_parameter_buckets`, `explain_query`, `get_performance_case`, and tuning sessions | Match a stable query identity and comparable parameter bucket; do not infer from fuzzy SQL text |
| Current requests, waits, blocking, locks, and memory grants | `get_active_sessions`, `get_currently_waiting_tasks`, `get_lock_details`, and `get_memory_grants` | Current-state evidence is point-in-time and must not be presented as a historical trend |
| Cached high-cost statements and routines | `get_top_cached_queries`, `get_cached_routine_stats`, and `get_plan_cache_analysis` | Cache evidence can reset or omit uncached executions; Query Store is preferred for durable history |
| Estimated and actual plans | `explain_query`, `compare_plan_summaries`, and tuning benchmarks | Actual execution is allowed only for read-only SELECT-shaped work under the selected policy |
| Result equivalence | `compare_query_results` and candidate benchmark workflows | Exact positional columns, types, duplicates, ordering semantics, and typed parameters are required; bounded or unstable evidence is inconclusive |
| Index opportunities and overlap | `analyze_query_indexes`, `analyze_workload_indexes`, `analyze_index_recommendations`, and `optimize_indexes` | Preserve key order and direction, includes, filters, uniqueness, constraints, disabled state, and partition metadata |
| Statistics health | `check_statistics_health` | Statistics age and modification evidence inform an experiment; they do not prove the root cause by themselves |

## Azure SQL resource and storage health

| Diagnostic concern | MCP coverage | Interpretation boundary |
|---|---|---|
| CPU, data IO, log write, worker, session, and storage pressure | `get_resource_stats_history` and `analyze_db_health` | Correlate resource saturation with the same time window as query evidence |
| Per-file IO latency and volume | `get_io_stats` | Report reads, writes, bytes, stall time, and calculated latency with units; avoid server-wide database comparisons |
| Database, file, log, VLF, and storage state | `get_storage_diagnostics` | Storage state is descriptive; changes require a separate reviewed database operation |
| Database properties, compatibility level, scoped configuration, Query Store, Automatic Tuning, and geo-replication | `get_database_configuration` and `check_capabilities` | Capabilities control which experiments are valid; compatibility level alone is not a hint recommendation |
| Table size, index usage, write amplification, columnstore state, and resumable operations | `get_table_stats` and `get_object_index_diagnostics` | Fragmentation and usage counters are supporting evidence, not standalone query-health verdicts |

## Deliberate exclusions

- NUMA, scheduler, server memory clerk, and server-wide buffer-pool analysis:
  these describe managed infrastructure that Azure SQL Database customers
  cannot tune directly.
- Page life expectancy as a primary health signal: use Azure SQL resource
  pressure, memory grants, waits, Query Store, and plan evidence instead.
- Server-wide database rankings: diagnostics stay inside the selected logical
  database and its visible Azure SQL resource scope.
- Automatic remediation from a single DMV counter, missing-index suggestion,
  fragmentation percentage, or cached-plan sample: every change requires a
  measured candidate and the relevant safety policy.
