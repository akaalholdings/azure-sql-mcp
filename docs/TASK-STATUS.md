# Task Status Board

> **IMPORTANT — Read Before Working**
> This file is the single source of truth for task ownership and status.
> Before starting ANY task you MUST:
> 1. Read this file.
> 2. Confirm the task status is `not_started`.
> 3. Change the status to `in_progress` and set your agent ID in the `owner` column.
> 4. Only then begin work.
>
> When you finish, set the status to `done` and fill in the `completed` date.
> If you are blocked, set the status to `blocked` and add a note.
>
> **Never work on a task that is already `in_progress` by another agent.**

---

## Status Legend

| Status | Meaning |
|--------|---------|
| `not_started` | Available to pick up |
| `in_progress` | Currently being worked on — **do not touch** |
| `blocked` | Work started but hit a dependency or issue |
| `done` | Completed and verified |
| `skipped` | Deliberately deferred or removed from scope |

---

## Phase 1 — Production Hardening

| # | Task | Doc | Status | Owner | Started | Completed | Notes |
|---|------|-----|--------|-------|---------|-----------|-------|
| 1.1 | Connection pooling (`connection_pool.py`) | [01](01-production-hardening.md#1a-connection-pooling) | `done` | 019d40bd-a936-7052-af59-6eebde272808 | 2026-03-30 | 2026-03-30 | Pool acquire/release/discard fixed; validation timeout added; covered by new connection/pool tests |
| 1.2 | Retry logic for transient failures (`retry.py`) | [01](01-production-hardening.md#1b-retry-logic-for-transient-failures) | `done` | 019d40bd-a936-7052-af59-6eebde272808 | 2026-03-30 | 2026-03-30 | Retry classification/backoff completed; covered by `test_retry.py` |
| 1.3 | Tool-level timeout in `_run_tool()` | [01](01-production-hardening.md#1c-tool-level-timeout) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Added `asyncio.wait_for(...)` timeout handling and timeout error payloads in `server.py` |
| 1.4 | Structured JSON logging (`logging_config.py`) | [01](01-production-hardening.md#1d-structured-json-logging) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Startup now uses `configure_logging`; tool runs emit correlation IDs and duration metrics |

## Phase 2 — Health Check Parity

| # | Task | Doc | Status | Owner | Started | Completed | Notes |
|---|------|-----|--------|-------|---------|-----------|-------|
| 2.1 | `buffer` health check (cache hit ratios) | [02](02-health-checks.md#2a-buffer---cache-hit-rates) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Implemented in `health.py`; covered by `test_health.py` |
| 2.2 | `connection` health check (session counts) | [02](02-health-checks.md#2b-connection---connection-health) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Implemented in `health.py`; covered by `test_health.py` |
| 2.3 | `constraint` health check (untrusted FKs/CHKs) | [02](02-health-checks.md#2c-constraint---constraint-health) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Implemented in `health.py`; covered by `test_health.py` |
| 2.4 | `replication` health check (geo-rep status) | [02](02-health-checks.md#2d-replication---geo-replication-health) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Implemented in `health.py`; covered by `test_health.py` |
| 2.5 | `identity` health check (column exhaustion) | [02](02-health-checks.md#2e-identity---identity-column-exhaustion) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Implemented in `health.py`; covered by `test_health.py` |
| 2.6 | Duplicate index detection in `index` check | [02](02-health-checks.md#2f-enhance-index---duplicate-index-detection) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Added duplicate-index detection and thresholded index status |
| 2.7 | Threshold-based pass/fail response format | [02](02-health-checks.md#2g-threshold-based-response-format) | `done` | codex-main | 2026-03-30 | 2026-03-30 | All health checks now return status/details/thresholds/findings |
| 2.8 | Complete tool annotations (`openWorldHint` etc.) | [02](02-health-checks.md#2h-complete-tool-annotations) | `done` | 019d409e-39a1-7651-9f00-812f8cc36e9a | 2026-03-30 | 2026-03-30 | `server.py` annotations updated for all registered tools |

## Phase 3 — Index & Query Enhancements

| # | Task | Doc | Status | Owner | Started | Completed | Notes |
|---|------|-----|--------|-------|---------|-----------|-------|
| 3.1 | `analyze_query_indexes` tool | [03](03-index-query-enhancements.md#3a-analyze_query_indexes-tool-new) | `done` | 019d40bd-c8a8-7712-89a9-9b6ea367a40c | 2026-03-30 | 2026-03-30 | Registered in `server.py`; plan-based recommendations consolidated and deduped against existing indexes |
| 3.2 | `analyze_workload_indexes` tool | [03](03-index-query-enhancements.md#3b-analyze_workload_indexes-tool-new) | `done` | 019d40bd-c8a8-7712-89a9-9b6ea367a40c | 2026-03-30 | 2026-03-30 | Workload-driven index analysis added and exposed through MCP |
| 3.3 | `resource_blend` sort for `get_top_queries` | [03](03-index-query-enhancements.md#3c-get_top_queries-enhancement---resource-blend-sort) | `done` | 019d40bd-c8a8-7712-89a9-9b6ea367a40c | 2026-03-30 | 2026-03-30 | Query Store now supports IO/memory sorts and normalized `resource_blend` ranking |
| 3.4 | What-if index support in `explain_query` | [03](03-index-query-enhancements.md#3d-explain-query-safety-decision) | `skipped` | codex-main | 2026-04-07 | 2026-04-07 | Deliberately disabled for v1 safety; `explain_query` remains read-only and rejects hypothetical index payloads |

## Phase 4 — MCP Protocol Completeness

| # | Task | Doc | Status | Owner | Started | Completed | Notes |
|---|------|-----|--------|-------|---------|-----------|-------|
| 4.1 | MCP Resources (`resources.py`) | [04](04-mcp-protocol.md#4a-mcp-resources) | `done` | 019d40bd-e18d-7cf1-a572-b3930fa12de1 | 2026-03-30 | 2026-03-30 | Resource templates registered in `server.py`; resource metadata and tests added |
| 4.2 | MCP Prompts (`prompts.py`) | [04](04-mcp-protocol.md#4b-mcp-prompts) | `done` | 019d40bd-e18d-7cf1-a572-b3930fa12de1 | 2026-03-30 | 2026-03-30 | Prompt templates registered in `server.py`; prompt content aligned with current tool surface and tested |
| 4.3 | Tool annotation updates | [04](04-mcp-protocol.md#4c-tool-annotation-updates) | `done` | 019d409e-39a1-7651-9f00-812f8cc36e9a | 2026-03-30 | 2026-03-30 | Already completed as part of 2.8 |

## Phase 5 — Schema Comparison

| # | Task | Doc | Status | Owner | Started | Completed | Notes |
|---|------|-----|--------|-------|---------|-----------|-------|
| 5.1 | Schema snapshot model (`schema_snapshot.py`) | [05](05-schema-comparison.md#5a-schema-snapshot-model) | `done` | 019d40bf-d693-7393-87c3-0f410ed88417 | 2026-03-30 | 2026-03-30 | Added frozen schema snapshot model with bulk capture queries |
| 5.2 | Schema diff engine (`schema_diff.py`) | [05](05-schema-comparison.md#5b-schema-diff-engine) | `done` | 019d40bf-d693-7393-87c3-0f410ed88417 | 2026-03-30 | 2026-03-30 | Deterministic snapshot comparison added for tables, columns, indexes, constraints, and programmables |
| 5.3 | DDL migration generator (`ddl_generator.py`) | [05](05-schema-comparison.md#5c-ddl-migration-script-generation) | `done` | 019d40bf-d693-7393-87c3-0f410ed88417 | 2026-03-30 | 2026-03-30 | Conservative migration generator added with transaction wrapper and dependency ordering |
| 5.4 | Schema compare tools + service | [05](05-schema-comparison.md#5d-mcp-tools) | `done` | codex-main | 2026-03-30 | 2026-03-30 | `SchemaCompareService` wired into `server.py` with snapshot, compare, and migration-script tools |

## Phase 6 — Additional Tools

| # | Task | Doc | Status | Owner | Started | Completed | Notes |
|---|------|-----|--------|-------|---------|-----------|-------|
| 6.1 | `search_objects` tool | [06](06-additional-tools.md#6a-search_objects-tool) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Added to `introspection.py` and registered in `server.py` |
| 6.2 | `get_dependencies` tool | [06](06-additional-tools.md#6b-get_dependencies-tool) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Added to `introspection.py` and registered in `server.py` |
| 6.3 | `get_table_stats` tool | [06](06-additional-tools.md#6c-get_table_stats-tool) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Added to `introspection.py` and registered in `server.py` |
| 6.4 | `get_active_sessions` tool | [06](06-additional-tools.md#6d-get_active_sessions-tool) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Added `sessions.py`, blocking-chain detection, tool registration, and unit coverage |
| 6.5 | Parallel introspection queries | [06](06-additional-tools.md#6e-parallel-introspection-queries) | `done` | codex-main | 2026-03-30 | 2026-03-30 | `get_object_details` table/view subqueries now run via `asyncio.gather(...)` |

## Phase 7 — SQL Validation Hardening

| # | Task | Doc | Status | Owner | Started | Completed | Notes |
|---|------|-----|--------|-------|---------|-----------|-------|
| 7.1 | Block dangerous functions (xp_cmdshell etc.) | [07](07-sql-validation.md#7a-block-dangerous-system-procedures-and-extended-procedures) | `done` | 019d40c0-1502-74f0-b4b6-8005f42099c1 | 2026-03-30 | 2026-03-30 | Explicit dangerous function/procedure checks added in `safe_sql.py` |
| 7.2 | Block four-part / linked-server names | [07](07-sql-validation.md#7b-block-linked-server--four-part-names) | `done` | 019d40c0-1502-74f0-b4b6-8005f42099c1 | 2026-03-30 | 2026-03-30 | Restricted validator now blocks linked-server / four-part table references |
| 7.3 | Block DBCC commands | [07](07-sql-validation.md#7c-block-dbcc-commands) | `done` | 019d40c0-1502-74f0-b4b6-8005f42099c1 | 2026-03-30 | 2026-03-30 | Added explicit DBCC block in text-rule validation |
| 7.4 | Bypass attempt tests | [07](07-sql-validation.md#7d-add-bypass-attempt-tests) | `done` | 019d40c0-1502-74f0-b4b6-8005f42099c1 | 2026-03-30 | 2026-03-30 | `test_safe_sql.py` expanded for bypass attempts and safe read-only cases |
| 7.5 | Document security model in README | [07](07-sql-validation.md#7e-document-security-model) | `done` | 019d40c0-1502-74f0-b4b6-8005f42099c1 | 2026-03-30 | 2026-03-30 | README now documents restricted-mode security model and allowed/blocked surfaces |

## Phase 8 — Testing & DevOps

| # | Task | Doc | Status | Owner | Started | Completed | Notes |
|---|------|-----|--------|-------|---------|-----------|-------|
| 8.1 | Shared test fixtures (`conftest.py`) | [08](08-testing-devops.md#shared-test-infrastructure) | `done` | 019d40ac-4536-77c2-a0a9-291adec9a79d | 2026-03-30 | 2026-03-30 | Added reusable `ServerConfig` pytest fixtures |
| 8.2 | Unit tests: `connection.py` | [08](08-testing-devops.md#test-files-to-create) | `done` | 019d40bd-a936-7052-af59-6eebde272808 | 2026-03-30 | 2026-03-30 | Added `test_connection.py`; included in full passing unit suite |
| 8.3 | Unit tests: `connection_pool.py` | [08](08-testing-devops.md#test-files-to-create) | `done` | 019d40bd-a936-7052-af59-6eebde272808 | 2026-03-30 | 2026-03-30 | Added `test_connection_pool.py`; includes validation-timeout coverage |
| 8.4 | Unit tests: `auth.py` | [08](08-testing-devops.md#test-files-to-create) | `done` | 019d40ac-4536-77c2-a0a9-291adec9a79d | 2026-03-30 | 2026-03-30 | Added auth coverage; unit suite passes |
| 8.5 | Unit tests: `server.py` | [08](08-testing-devops.md#test-files-to-create) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Added and updated server tests for expanded tool surface |
| 8.6 | Unit tests: `health.py` | [08](08-testing-devops.md#test-files-to-create) | `done` | 019d409e-5121-7331-9142-c917911686a9 | 2026-03-30 | 2026-03-30 | Added `test_health.py`; health-focused pytest suite passes |
| 8.7 | Unit tests: `retry.py` | [08](08-testing-devops.md#test-files-to-create) | `done` | 019d40bd-a936-7052-af59-6eebde272808 | 2026-03-30 | 2026-03-30 | Added `test_retry.py`; included in full passing unit suite |
| 8.8 | Unit tests: `schema_diff.py` + `ddl_generator.py` | [08](08-testing-devops.md#test-files-to-create) | `done` | 019d40bf-d693-7393-87c3-0f410ed88417 | 2026-03-30 | 2026-03-30 | Added schema diff / DDL generator unit coverage and schema-compare integration test |
| 8.9 | Unit tests: `resources.py` + `prompts.py` | [08](08-testing-devops.md#test-files-to-create) | `done` | 019d40bd-e18d-7cf1-a572-b3930fa12de1 | 2026-03-30 | 2026-03-30 | Added `test_resources.py` and `test_prompts.py`; included in full passing unit suite |
| 8.10 | Integration tests | [08](08-testing-devops.md#8b-integration-tests) | `done` | codex-main | 2026-03-30 | 2026-03-30 | Added integration fixtures and live introspection/tool smoke tests; suite skips cleanly without env |
| 8.11 | Dockerfile + docker-compose | [08](08-testing-devops.md#8c-docker-support) | `done` | 019d40ac-74db-7be2-ae80-fb779e680b29 | 2026-03-30 | 2026-03-30 | Implemented Docker support files |
| 8.12 | CI/CD pipeline (GitHub Actions) | [08](08-testing-devops.md#8d-cicd-pipeline) | `done` | 019d40ac-74db-7be2-ae80-fb779e680b29 | 2026-03-30 | 2026-03-30 | Added AzureSqlMcp workflows |
| 8.13 | README improvements | [08](08-testing-devops.md#8e-readme-improvements) | `done` | 019d40ac-74db-7be2-ae80-fb779e680b29 | 2026-03-30 | 2026-03-30 | Updated README for current and newly added tool surface |

---

## Phase 9 — Wait Statistics & Diagnostics

> Azure SQL DB exposes `sys.dm_db_wait_stats` (database-scoped) and `sys.query_store_wait_stats` (per-query).
> Server-level `sys.dm_os_wait_stats` is NOT available in PaaS.

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 9.1 | `get_wait_stats` tool — top waits from `sys.dm_db_wait_stats` with category mapping and root-cause annotations | `done` | — | 2026-04-01 | 2026-04-01 | `wait_stats.py`; benign wait filtering, % of total, category aggregation |
| 9.2 | `get_query_wait_stats` tool — per-query wait breakdown from Query Store wait stats | `done` | — | 2026-04-01 | 2026-04-01 | Ties waits to specific queries via `sys.query_store_wait_stats` |
| 9.3 | `get_currently_waiting_tasks` tool — active waits from `sys.dm_os_waiting_tasks` | `done` | — | 2026-04-01 | 2026-04-01 | Real-time view with SQL text and category annotations |
| 9.4 | Wait category classifier + root cause mapping — benign wait filtering, categorization | `done` | — | 2026-04-01 | 2026-04-01 | `classify_wait()`, `BENIGN_WAITS`, `CATEGORY_RECOMMENDATIONS` in `wait_stats.py` |
| 9.5 | Unit tests for wait stats module | `done` | — | 2026-04-01 | 2026-04-01 | `test_wait_stats.py`: 16 tests |

## Phase 10 — Lock & Transaction Diagnostics

> All `sys.dm_tran_*` DMVs are available in Azure SQL DB.
> Deadlock graphs available via `system_health` extended events session.

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 10.1 | `get_lock_details` tool — `sys.dm_tran_locks` with lock mode, resource type, session SQL text | `done` | — | 2026-04-01 | 2026-04-01 | `lock_diagnostics.py`; groups by resource type, shows waiting locks |
| 10.2 | `get_open_transactions` tool — active transactions with duration, type, log bytes, warnings | `done` | — | 2026-04-01 | 2026-04-01 | Flags long-running (>5min) and idle-in-transaction (sleeping >60s) |
| 10.3 | `get_deadlock_history` tool — deadlock graphs from `system_health` XE session | `done` | — | 2026-04-01 | 2026-04-01 | Parses deadlock XML: victim, participants, resources, SQL text |
| 10.4 | Enhance `get_active_sessions` — add isolation level, transaction begin time, open transaction duration, lock wait resource | `done` | — | 2026-04-01 | 2026-04-01 | Query enhanced with isolation_level, txn info, wait_resource |
| 10.5 | Unit tests for lock & transaction module | `done` | — | 2026-04-01 | 2026-04-01 | `test_lock_diagnostics.py`: 8 tests including deadlock XML parsing |

## Phase 11 — Tempdb & Memory Grant Diagnostics

> `sys.dm_db_session_space_usage` and `sys.dm_db_task_space_usage` available in Azure SQL DB.
> `sys.dm_exec_query_memory_grants` available. `sys.dm_os_memory_clerks` has limited visibility in PaaS.
> ~~NUMA diagnostics~~ STRIPPED: not accessible in Azure SQL DB PaaS.
> ~~Tempdb file management~~ STRIPPED: tempdb files managed by platform.

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 11.1 | `get_tempdb_usage` tool — per-session tempdb consumption in MB | `done` | — | 2026-04-01 | 2026-04-01 | `tempdb_memory.py`; user/internal object alloc/dealloc/net |
| 11.2 | `get_tempdb_space_breakdown` tool — version store, user/internal objects, free space | `done` | — | 2026-04-01 | 2026-04-01 | From `sys.dm_db_file_space_usage` |
| 11.3 | `get_memory_grants` tool — active/pending grants with spill detection | `done` | — | 2026-04-01 | 2026-04-01 | Warns on pending grants (RESOURCE_SEMAPHORE) and likely spilling (>=95% usage) |
| 11.4 | Unit tests for tempdb & memory module | `done` | — | 2026-04-01 | 2026-04-01 | `test_tempdb_memory.py`: 5 tests |

## Phase 12 — I/O & Azure Resource Governance

> `sys.dm_io_virtual_file_stats` available (database-scoped).
> `sys.dm_user_db_resource_governance` and `sys.dm_db_resource_stats` are Azure-specific DMVs.
> ~~sys.dm_os_schedulers / sys.dm_os_workers~~ STRIPPED: server-level, not exposed in PaaS.

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 12.1 | `get_io_stats` tool — per-file I/O latency, throughput, stall times | `done` | — | 2026-04-01 | 2026-04-01 | `resource_governance.py`; warns when avg latency > 20ms |
| 12.2 | `get_resource_limits` tool — governance limits + service tier/objective | `done` | — | 2026-04-01 | 2026-04-01 | `sys.dm_user_db_resource_governance` + `sys.database_service_objectives` |
| 12.3 | Enhance `analyze_db_health` resource check — compare against governance limits | `done` | — | 2026-04-01 | 2026-04-01 | `_fetch_governance_limits` + `_compare_against_governance` in health.py |
| 12.4 | `get_resource_stats_history` tool — resource utilization history with sustained pressure warnings | `done` | — | 2026-04-01 | 2026-04-01 | 15-sec granularity, summary stats, warns when metric >80% for >30% of window |
| 12.5 | Unit tests for I/O & resource governance module | `done` | — | 2026-04-01 | 2026-04-01 | `test_resource_governance.py`: 5 tests |

## Phase 13 — Statistics Currency & Plan Cache Health

> `sys.dm_db_stats_properties` and `sys.dm_exec_cached_plans` available in Azure SQL DB.

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 13.1 | `check_statistics_health` tool — stale stats, high modification, low sample rates | `done` | — | 2026-04-01 | 2026-04-01 | `plan_cache.py`; configurable stale_days and mod_pct_threshold |
| 13.2 | `get_plan_cache_analysis` tool — plan type distribution, single-use bloat detection | `done` | — | 2026-04-01 | 2026-04-01 | Warns when >50% plans are single-use |
| 13.3 | `get_query_compilation_stats` tool — excessive recompilation detection | `done` | — | 2026-04-01 | 2026-04-01 | Flags queries with recompile ratio >50% and >10 executions |
| 13.4 | Add stale statistics check to `analyze_db_health` as a new health category | `done` | — | 2026-04-01 | 2026-04-01 | `_statistics_health()` in health.py; stale (>7 days) and high-modification (>20%) checks |
| 13.5 | Unit tests for statistics & plan cache module | `done` | — | 2026-04-01 | 2026-04-01 | `test_plan_cache.py`: 5 tests |

## Phase 14 — Parameter Sniffing & Query Regression Detection

> Query Store in Azure SQL DB is always ON and has full visibility.

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 14.1 | `detect_parameter_sniffing` tool — queries with multi-plan duration variance > threshold | `done` | — | 2026-04-01 | 2026-04-01 | `query_regression.py`; configurable variance_threshold (default 10x), window |
| 14.2 | `detect_regressed_queries` tool — automatic tuning recommendations with plan forcing scripts | `done` | — | 2026-04-01 | 2026-04-01 | Parses `sys.dm_db_tuning_recommendations` JSON details |
| 14.3 | `compare_query_plans` tool — side-by-side plan comparison with operator extraction | `done` | — | 2026-04-01 | 2026-04-01 | Auto-selects best/worst by duration if plan IDs not specified |
| 14.4 | `get_forced_plans` tool — forced plans with staleness and failure warnings | `done` | — | 2026-04-01 | 2026-04-01 | Warns on stale (>7 days) and failing forced plans |
| 14.5 | Unit tests for parameter sniffing & regression module | `done` | — | 2026-04-01 | 2026-04-01 | `test_query_regression.py`: 10 tests including XML operator extraction |

## Phase 15 — Index Optimization v2

> Optimizer-Signal-Driven Index Tuning engine. Leverages SQL Server optimizer signals (plan XML `<MissingIndexes>` with Impact% + DMV `avg_user_impact`) instead of hypothetical index simulation (DBCC AUTOPILOT unavailable on Azure SQL DB PaaS). Full pipeline: workload collection → candidate generation → enrichment → overlap detection → Pareto scoring → budget-constrained greedy selection.

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 15.1 | Prefix subsumption detection — index (A,B,C) subsumes (A,B); merge into wider index with unioned INCLUDE columns | `done` | — | 2026-03-31 | 2026-03-31 | `_detect_prefix_subsumption()` in `index_optimizer.py`; cross-table not merged |
| 15.2 | Pareto cost-benefit scoring — `log(read_benefit+1) - α·log(size_mb+1) - β·log(write_ratio+0.01)` with configurable α (size penalty) and β (write penalty) | `done` | — | 2026-03-31 | 2026-03-31 | `_score_candidates()` in `index_optimizer.py` |
| 15.3 | Index size estimation — row_count × key_width from `sys.dm_db_partition_stats` + `sys.columns`, with non-leaf 10% overhead | `done` | — | 2026-03-31 | 2026-03-31 | `_estimate_index_size()`, `_get_row_count()`, `_get_column_widths()` |
| 15.4 | Consolidate overlapping recommendations — merge plan XML + DMV candidates by (schema, table, key_cols) signature; confidence=high when both agree | `done` | — | 2026-03-31 | 2026-03-31 | Cross-source merging in `_generate_candidates()` |
| 15.5 | Existing index filtering — remove candidates whose key columns are a prefix of an existing index | `done` | — | 2026-03-31 | 2026-03-31 | `_filter_existing_indexes()` reuses `_get_existing_indexes()` from query_index_analysis |
| 15.6 | Budget-constrained greedy selection — iterate by score descending, deduct estimated_size_mb, merge prefix-overlapping with already-selected | `done` | — | 2026-03-31 | 2026-03-31 | `_greedy_select()` with budget_mb and min_improvement_pct |
| 15.7 | `optimize_indexes` MCP tool + unit tests (14 test cases, 124 total passing) | `done` | — | 2026-03-31 | 2026-03-31 | `server.py` tool registration + `tests/unit/test_index_optimizer.py` |

## Phase 16 — SQL Validation v2

> Flip from blocklist to allowlist approach; add resource exhaustion protection.

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 16.1 | Function allowlist — curate allowlist of safe T-SQL functions | `not_started` | — | — | — | ~200+ functions; deferred — current blocklist approach sufficient for now |
| 16.2 | Block `WAITFOR` — regex check for `WAITFOR DELAY` and `WAITFOR TIME` (DoS vector) | `done` | — | 2026-04-01 | 2026-04-01 | `WAITFOR_PATTERN` in `safe_sql.py`; checked before EXEC pattern |
| 16.3 | Block locking hints — `WITH (UPDLOCK)`, `WITH (XLOCK)`, `WITH (TABLOCKX)` | `done` | — | 2026-04-01 | 2026-04-01 | `DANGEROUS_HINTS_PATTERN`; NOLOCK still allowed |
| 16.4 | Block `EXECUTE AS` — privilege escalation via impersonation | `done` | — | 2026-04-01 | 2026-04-01 | `EXECUTE_AS_PATTERN` checked before generic EXEC pattern |
| 16.5 | Block `MAXRECURSION 0` — unlimited recursive CTE depth bomb | `done` | — | 2026-04-01 | 2026-04-01 | `MAXRECURSION_ZERO_PATTERN`; nonzero values allowed |
| 16.6 | Block `sp_executesql` — dynamic SQL execution | `done` | — | 2026-04-01 | 2026-04-01 | `SP_EXECUTESQL_PATTERN` in text-level checks |
| 16.7 | Unit tests for SQL validation v2 (including bypass attempts) | `done` | — | 2026-04-01 | 2026-04-01 | 10 new test cases in `test_safe_sql.py` (26 total) |

## Phase 17 — Schema Comparison v2

> Fill gaps in schema snapshot fidelity and DDL generation accuracy.

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 17.1 | Capture filtered indexes — `sys.indexes` WHERE `has_filter = 1` + `filter_definition` column | `done` | — | 2026-04-01 | 2026-04-01 | `filter_definition` on IndexDef; DDL includes WHERE clause |
| 17.2 | Capture index/table compression — `sys.partitions.data_compression_desc` (NONE, ROW, PAGE, COLUMNSTORE) | `done` | — | 2026-04-01 | 2026-04-01 | `data_compression` on IndexDef; DDL includes WITH (DATA_COMPRESSION) |
| 17.3 | Capture FK cascade rules — `sys.foreign_keys.delete_referential_action_desc` / `update_referential_action_desc` | `done` | — | 2026-04-01 | 2026-04-01 | `delete_action`/`update_action` on ConstraintDef; DDL includes ON DELETE/UPDATE |
| 17.4 | Capture WITH NOCHECK state — `sys.foreign_keys.is_not_trusted`, `sys.check_constraints.is_not_trusted` | `done` | — | 2026-04-01 | 2026-04-01 | `is_not_trusted` on ConstraintDef; DDL uses WITH NOCHECK |
| 17.5 | Capture sequence objects — `sys.sequences`: data_type, start_value, increment, min/max, is_cycling, current_value | `done` | — | 2026-04-01 | 2026-04-01 | `SequenceDef` dataclass; CREATE/ALTER/DROP SEQUENCE DDL |
| 17.6 | Capture triggers — `sys.triggers` + `sys.sql_modules.definition` for DML triggers on tables | `done` | — | 2026-04-01 | 2026-04-01 | `TriggerDef` dataclass; CREATE/ALTER/DROP TRIGGER DDL |
| 17.7 | Capture computed columns — `sys.computed_columns.definition`, `is_persisted` | `done` | — | 2026-04-01 | 2026-04-01 | `computed_definition`/`is_persisted` on ColumnDef; DDL includes AS expr PERSISTED |
| 17.8 | Capture partition schemes/functions — `sys.partition_schemes`, `sys.partition_functions`, `sys.partition_range_values` | `skipped` | — | — | — | Azure SQL DB PaaS manages partitioning at platform level; deferred |
| 17.9 | Unit tests for schema comparison v2 | `done` | — | 2026-04-01 | 2026-04-01 | 14 new tests covering all v2 features in test_schema_diff.py + test_ddl_generator.py |

## Phase 18 — Stats-Driven Parameter Binding

> Enable realistic EXPLAIN analysis for parameterized Query Store queries.
> Uses `sys.dm_db_stats_histogram` (Azure SQL DB) for distribution data.

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 18.1 | Parameter placeholder detection — identify `@p1, @p2` style parameters in Query Store query text | `done` | — | 2026-04-01 | 2026-04-01 | `detect_parameters()` in `param_binding.py`; regex with @@system var exclusion |
| 18.2 | Stats-driven value selection — `sys.dm_db_stats_histogram` for histogram bounds | `done` | — | 2026-04-01 | 2026-04-01 | `_resolve_from_stats()` finds column→param mappings, queries histogram for most common value |
| 18.3 | Type-based fallback values — when stats unavailable, use sensible defaults per data type | `done` | — | 2026-04-01 | 2026-04-01 | `TYPE_FALLBACKS` dict covering 30+ SQL Server types; `get_type_fallback()` |
| 18.4 | Integration with `explain_query` and `analyze_query_indexes` — auto-bind parameters | `done` | — | 2026-04-01 | 2026-04-01 | `auto_bind_params` flag on both tools; generates DECLARE/SET block prepended to SQL |
| 18.5 | Unit tests for parameter binding module | `done` | — | 2026-04-01 | 2026-04-01 | `test_param_binding.py`: 13 tests covering detection, fallbacks, histogram binding, multi-param |

## Phase 19 — Connection Pool & Retry Hardening

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 19.1 | Circuit breaker — 5 consecutive failures → fast-fail with 30s cooldown, half-open probe | `done` | — | 2026-04-01 | 2026-04-01 | `CircuitBreakerState` + `_check/_record_success/_record_failure` in `connection_pool.py` |
| 19.2 | Connection leak detection — track leases with acquire stack trace, report held >5min | `done` | — | 2026-04-01 | 2026-04-01 | `_track_lease`, `_release_lease`, `check_leaked_connections()` |
| 19.3 | Pool utilization metrics — acquire/release/create/discard counts, active connections, peak, wait time | `done` | — | 2026-04-01 | 2026-04-01 | `PoolMetrics` dataclass + `get_metrics()` per database |
| 19.4 | Coordinated token refresh — `_token_lock` asyncio.Lock on pool | `done` | — | 2026-04-01 | 2026-04-01 | Added `_token_lock` to prevent duplicate Azure AD calls |
| 19.5 | Add missing transient error codes — 1205, 40549, 40554 | `done` | — | 2026-04-01 | 2026-04-01 | Added to `TRANSIENT_ERROR_CODES` in `retry.py` |
| 19.6 | Unit tests for pool & retry hardening | `done` | — | 2026-04-01 | 2026-04-01 | 6 new tests in `test_connection_pool.py` (14 total) |

## Phase 20 — Observability & Telemetry

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 20.1 | Extract SQL Server error code from exceptions — SQLSTATE + native error code | `done` | — | 2026-04-01 | 2026-04-01 | `extract_sql_error_info()` in `observability.py` |
| 20.2 | Row count telemetry — log rows returned per tool call; warn if > 10,000 | `done` | — | 2026-04-01 | 2026-04-01 | `_estimate_row_count` + warning log in `_run_tool` |
| 20.3 | Query hash in logs — compute hash of normalized SQL for deduplication | `done` | — | 2026-04-01 | 2026-04-01 | `compute_query_hash()` in `observability.py` |
| 20.4 | Sanitize error messages — strip connection strings, server names, IPs | `done` | — | 2026-04-01 | 2026-04-01 | `sanitize_error_message()` integrated into both `_run_tool` exception paths |
| 20.5 | Unit tests for observability enhancements | `done` | — | 2026-04-01 | 2026-04-01 | `test_observability.py`: 11 tests |

## Phase 21 — Ultimate MCP Hardening

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 21.1 | HTTP/SSE bearer auth | `done` | codex | 2026-07-01 | 2026-07-01 | `AZURE_SQL_MCP_BEARER_TOKEN` required for `sse`/`streamable-http`; FastMCP auth settings + `token_verifier` use constant-time comparison |
| 21.2 | Admin write policy and audit | `done` | codex | 2026-07-01 | 2026-07-01 | `AZURE_SQL_WRITE_POLICY`, dry-run defaults, raw SQL limited to read-only SELECT batches, remote admin opt-in, JSONL audit with redacted SQL by default |
| 21.3 | Structured output and token-safe artifacts | `done` | codex | 2026-07-01 | 2026-07-01 | Tools return structured dictionaries; raw SHOWPLAN XML defaults to bounded `azuresql-artifact://...` resource metadata |
| 21.4 | Query Store plan enforcement workflow | `done` | codex | 2026-07-01 | 2026-07-01 | `review_plan_enforcement`, `dry_run_plan_action`, `apply_plan_action`, and dry-run `plan_enforcer_tick`; review windows flow into Query Store evidence |
| 21.5 | Quality gates | `done` | codex | 2026-07-01 | 2026-07-01 | Added `py.typed`, ruff/pyright dev deps, CI gates for ruff, pyright, compileall, pytest, and `uv build` |

## Phase 22 — Azure SQL Diagnostic Query Parity

| # | Task | Status | Owner | Started | Completed | Notes |
|---|------|--------|-------|---------|-----------|-------|
| 22.1 | Diagnostic query parity service | `done` | codex | 2026-07-01 | 2026-07-01 | Added `diagnostics.py` with DB-safe configuration, storage/log/VLF, connection, cache, routine, and object/index diagnostics |
| 22.2 | Six read-only performance tools | `done` | codex | 2026-07-01 | 2026-07-01 | `get_database_configuration`, `get_storage_diagnostics`, `get_connection_diagnostics`, `get_top_cached_queries`, `get_cached_routine_stats`, `get_object_index_diagnostics` |
| 22.3 | Diagnostic script coverage matrix | `done` | codex | 2026-07-01 | 2026-07-01 | `docs/diagnostic-query-coverage.md` maps all 56 reference queries to added, covered, or skipped outcomes |
| 22.4 | Unit and registration coverage | `done` | codex | 2026-07-01 | 2026-07-01 | Service tests cover bounds, optional DMV failures, no raw plan XML, and restricted-mode performance tool registration |

---

## Stripped Items (Not Applicable to Azure SQL DB PaaS)

The following items from the original gap analysis were **removed** because the underlying DMVs or features are not accessible in Azure SQL Database (PaaS):

| Item | Reason Stripped |
|---|---|
| NUMA diagnostics (`sys.dm_os_memory_nodes`, cross-node pressure) | Server-level; managed infrastructure |
| CPU scheduler health (`sys.dm_os_schedulers`, `sys.dm_os_workers`, runnable queue) | Server-level; not exposed in PaaS |
| Tempdb file-level contention (PFS/GAM/SGAM latch waits) | Tempdb managed by platform; can't add files or configure |
| AlwaysOn Availability Groups (`sys.availability_replicas`, `sys.dm_hadr_*`) | Azure SQL DB uses geo-replication, not AGs (geo-rep already implemented in Phase 2) |
| `sys.dm_os_wait_stats` (server-level waits) | Use `sys.dm_db_wait_stats` (database-scoped) instead — covered in Phase 9 |
| `sys.dm_os_memory_clerks` detailed breakdown | Limited visibility in PaaS; meaningful memory diagnostics covered via `sys.dm_exec_query_memory_grants` in Phase 11 |

---

## Dependency Graph (Quick Reference)

```
Phase 1-8: DONE

9.4 (wait classifier) ──> 9.1 (wait stats tool)
                       ──> 9.2 (query wait stats)

10.4 (enhance sessions) ── builds on existing sessions.py

12.2 (resource limits) ──> 12.3 (health check enhancement)

13.1 (stats health) ──> 13.4 (health check integration)

14.1 (param sniffing) ── depends on Query Store (already implemented)
14.3 (compare plans) ── builds on existing plans.py

15.1-15.4 (index v2) ── builds on existing index_recommendations.py
15.5 (existing index filtering) ── builds on query_index_analysis existing-index introspection

16.1-16.6 (validation v2) ── replaces/enhances existing safe_sql.py

17.1-17.8 (schema v2) ── enhances existing schema_snapshot.py + schema_diff.py + ddl_generator.py

18.4 (param binding integration) ──> 18.1-18.3 (binding logic)

19.1-19.4 (pool hardening) ── enhances existing connection_pool.py
```

Tasks without listed dependencies can be started independently.

---

## How To Claim a Task

Edit this file to update the row:

```markdown
| 9.1 | get_wait_stats tool | `in_progress` | agent-xyz | 2026-03-31 | — | Working on it |
```

When done:

```markdown
| 9.1 | get_wait_stats tool | `done` | agent-xyz | 2026-03-31 | 2026-03-31 | Implemented + tested |
```
