# Azure SQL Diagnostic Query Coverage

Source reference: `/Users/balwinder/Downloads/Azure SQL Database Diagnostic Information Queries (1).sql`.

The MCP implementation ports Azure SQL Database-safe diagnostics and skips server-level SQL Server checks that are not actionable or consistently visible in Azure SQL DB PaaS.

| Query | Script section | MCP coverage |
|---:|---|---|
| 1 | Version Info | Added in `get_database_configuration` |
| 2 | Configuration Values | Added in `get_database_configuration` |
| 3 | SQL Server NUMA Info | Skipped: server-level managed infrastructure |
| 4 | IO Stalls by File | Covered by `get_io_stats` |
| 5 | IO Usage By Database | Skipped: server-wide view; per-database IO is covered by `get_io_stats` |
| 6 | Total Buffer Usage by Database | Skipped: server-wide buffer view; object buffer footprint added where visible |
| 7 | Connection Counts by IP Address | Added in `get_connection_diagnostics` |
| 8 | Avg Task Counts | Skipped: scheduler/server-level DMV |
| 9 | Detect Blocking | Covered by `get_active_sessions`, `get_currently_waiting_tasks`, and `get_lock_details` |
| 10 | PLE by NUMA Node | Skipped: NUMA-level view; buffer health already uses database-safe counters where visible |
| 11 | Memory Grants Pending | Covered by `get_memory_grants` |
| 12 | Memory Clerk Usage | Skipped: limited PaaS visibility; memory grants provide actionable pressure signal |
| 13 | Ad hoc Queries | Covered by `get_plan_cache_analysis` |
| 14 | Azure SQL DB Size | Added in `get_storage_diagnostics` |
| 15 | File Sizes and Space | Added in `get_storage_diagnostics` |
| 16 | Log Space Usage | Added in `get_storage_diagnostics` |
| 17 | VLF Counts | Added in `get_storage_diagnostics` |
| 18 | Last VLF Status | Added in `get_storage_diagnostics` |
| 19 | Database Properties | Added in `get_database_configuration` |
| 20 | Database-scoped Configurations | Added in `get_database_configuration` |
| 21 | IO Stats By File | Covered by `get_io_stats` |
| 22 | Recent Resource Usage | Covered by `get_resource_stats_history` |
| 23 | Avg-Max Resource Usage | Covered by `get_resource_stats_history` |
| 24 | Top DB Waits | Covered by `get_wait_stats` |
| 25 | Query Execution Counts | Added in `get_top_cached_queries` |
| 26 | Top Worker Time Queries | Added in `get_top_cached_queries` |
| 27 | Top Logical Reads Queries | Added in `get_top_cached_queries` |
| 28 | Top Avg Elapsed Time Queries | Added in `get_top_cached_queries` |
| 29 | SP Execution Counts | Added in `get_cached_routine_stats` |
| 30 | SP Avg Elapsed Time | Added in `get_cached_routine_stats` |
| 31 | SP Worker Time | Added in `get_cached_routine_stats` |
| 32 | SP Logical Reads | Added in `get_cached_routine_stats` |
| 33 | SP Physical Reads | Added in `get_cached_routine_stats` |
| 34 | SP Logical Writes | Added in `get_cached_routine_stats` |
| 35 | Top IO Statements | Added in `get_top_cached_queries` |
| 36 | Bad NC Indexes | Added in `get_object_index_diagnostics` |
| 37 | Missing Indexes | Covered by `analyze_index_recommendations` and `optimize_indexes` |
| 38 | Missing Index Warnings | Covered by `analyze_query_indexes` and `analyze_workload_indexes` |
| 39 | Buffer Usage | Added in `get_object_index_diagnostics` with optional graceful fallback |
| 40 | Table Sizes | Covered by `get_table_stats` |
| 41 | Table Properties | Added in `get_object_index_diagnostics` through object/table filters and usage sections |
| 42 | Statistics Update | Covered by `check_statistics_health` |
| 43 | Volatile Indexes | Added in `get_object_index_diagnostics` |
| 44 | Index Fragmentation | Covered by `analyze_db_health` index check |
| 45 | Overall Index Usage - Reads | Added in `get_object_index_diagnostics` |
| 46 | Overall Index Usage - Writes | Added in `get_object_index_diagnostics` |
| 47 | Columnstore Index Physical Stat | Added in `get_object_index_diagnostics` |
| 48 | Lock Waits | Added in `get_object_index_diagnostics`; session-level locks remain in `get_lock_details` |
| 49 | UDF Statistics | Added in `get_cached_routine_stats` |
| 50 | QueryStore Options | Covered by `check_capabilities`, `analyze_db_health`, and added in `get_database_configuration` |
| 51 | High Aggregate Duration Queries | Covered by `get_top_queries` and plan enforcement review tools |
| 52 | Input Buffer | Added in `get_connection_diagnostics` |
| 53 | Resumable Index Rebuild | Added in `get_object_index_diagnostics` |
| 54 | Automatic Tuning Options | Covered by `analyze_db_health` and added in `get_database_configuration` |
| 55 | Geo-Replication Link Status | Covered by `analyze_db_health` and added in `get_database_configuration` |
| 56 | Azure SQL DB Properties | Added in `get_database_configuration` |

