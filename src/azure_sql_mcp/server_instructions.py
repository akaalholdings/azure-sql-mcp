"""Instructions sent to MCP clients at initialization."""

from __future__ import annotations

SERVER_INSTRUCTIONS = """\
You are connected to azure-sql-mcp, the typed evidence and execution layer for Azure SQL
Database (PaaS). It reads live DMVs and Query Store, and it can run gated, reversible
experiments. It never assumes SQL Server features that Azure SQL Database lacks.

## Start every session
1. check_runtime_status: package version, active profile, tool groups, fingerprints.
2. list_databases: only these allowlisted databases can be used. Let the user choose.
3. check_capabilities(database_name): platform, Query Store state, mcp_contract flags,
   and the local policy (what benchmarks or writes are allowed).

## Read result_status before the data
Every tool result has result_status:
- ok: data returned.
- empty: the source was read and held nothing. A true negative for its window.
- unavailable: the source exists but could not be read (permission, timeout). Not an
  all-clear; report the reason.
- not_supported: the source does not exist on Azure SQL Database. Do not ask anyone to
  enable it.
- precondition: a setup step is missing (Query Store off, no deadlock capture session,
  master not allowlisted). `remediation` holds the exact statement for a DBA. Show it;
  never run it yourself.

## Pick the workflow
- Incident or broad slowness: diagnose_database first (one call, ranked findings with
  next tool calls), then drill in with get_resource_stats_history (window_minutes up to 14 days;
  over 60 minutes needs master allowlisted), get_resource_limits, get_wait_stats
  (sample_seconds=15 during a live incident), get_currently_waiting_tasks,
  get_active_sessions, get_lock_details, get_open_transactions, get_deadlock_history,
  get_top_queries, detect_regressed_queries, then start_performance_case and
  collect_performance_evidence for a durable case.
- Index design and review: review_workload_indexes. It reads Query Store workload and
  stored plans, finds the queries that hit each table, and returns create, extend,
  widen, consolidate, drop, and heap recommendations with inert DDL and rollback.
  Prove a candidate with benchmark_index_candidate (sandbox profile, non-production
  copy) before any production change.
- One slow query: analyze_query_plan (Query Store plan_id or query_id) or explain_query
  (both return rule-based plan findings), get_query_store_trend for its history,
  check_equivalence_preflight, start_performance_case,
  start_tuning_session, add_tuning_candidate, benchmark_tuning_candidate,
  finalize_tuning_session. Measured claims come only from these results.
- Plan stability: plan_health_review, review_plan_enforcement, prepare_plan_action.
  Applying a plan action is a separate, explicitly authorized step.

## Azure wait types and where to look next
- LOG_RATE_GOVERNOR, POOL_LOG_RATE_GOVERNOR, HADR_THROTTLE_LOG_RATE_GOVERNOR: the log
  write rate cap. Check avg_log_write_percent (get_resource_stats_history) and the cap
  (get_resource_limits); look for bulk loads, index rebuilds, and large updates.
- SOS_SCHEDULER_YIELD with avg_cpu_percent near 100: CPU cap. get_top_queries by CPU,
  get_query_wait_stats, then tune the top consumers.
- PAGEIOLATCH_*: data reads, often with avg_data_io_percent high.
  review_workload_indexes(objective=logical_reads) and get_top_queries by reads.
- WRITELOG: log flush latency; compare with avg_log_write_percent.
- LCK_M_*: blocking. get_active_sessions (blocking chains), get_lock_details,
  get_open_transactions; deadlocks via get_deadlock_history.
- RESOURCE_SEMAPHORE: memory grants. get_memory_grants.
- THREADPOOL or max_worker_percent near 100: worker limit, usually blocking chains or
  excessive parallelism.
- CXPACKET, CXCONSUMER: parallelism; check the plans of the top queries.
- PAGELATCH_* on tempdb pages: tempdb contention. get_tempdb_usage,
  get_tempdb_space_breakdown.

## Time windows
Query Store keeps history for weeks. get_query_store_trend shows when a query or the
whole workload changed (with plan changes), get_query_store_regressions compares a recent
window with the baseline before it, and get_top_queries accepts as_of_utc. To study a
past incident, keep the window length and move as_of_utc to the incident's end instead of
widening the window: a wider window changes every top-N and average.

## What resets and what persists
Wait stats, plan-cache stats, and index usage counters reset on failover, scaling, and
restarts; results say when (window.since_utc, usage_counters.days_since_reset). Query
Store persists across those events and is the source for history, regressions, and
index design. Absent Query Store history is unknown, not zero.

## Safety
- Restricted profiles are read-only. Writes happen only through gated tools in the
  sandbox, enforcer-apply, or unprofiled DBA configurations, and each needs the user's
  explicit request.
- DDL inside a recommendation is inert advice for human change control.
- Report numbers only from tool results. Label estimates as estimates; never present an
  optimizer cost share or a sample as a measured gain.
"""
