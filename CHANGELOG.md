# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

## [Unreleased]

## [2.2.1] - 2026-07-28

### Changed

- Benchmark classification now uses conservative observed-range separation
  instead of rejecting fast candidates solely because their independent
  range-to-median ratio is high. Existing spread and noise telemetry remains
  available, with an additive comparison margin explaining the decision.
- Finalization stopping reasons now accept up to 2,000 characters and publish
  the same limit in the MCP input schema.
- Schema and catalog tools and resources now require database-policy
  `allow_read=true`; database-free runtime and policy discovery remain
  available.

### Fixed

- Literal outer `TOP (0)` result comparisons now restore the zero-row
  shape-and-type probe without weakening proof gates for other row limits.
- Tuning-session retrieval now exposes committed index evidence even when a
  crash prevented it from being attached to the candidate, while keeping such
  evidence out of winner selection.
- Index benchmarks now reject terminal sessions before cleanup, request
  binding, catalog access, DDL, or query dispatch.
- State-machine errors keep safe session and candidate enum values visible
  without weakening SQL-literal or secret redaction.

## [2.2.0] - 2026-07-28

### Added

- Added database-aware `check_equivalence_preflight` analysis with recursive view inspection to depth eight, function verdicts, dependency coverage, and fail-closed handling for encrypted, inaccessible, unresolved, cyclic, or depth-exceeded definitions.
- Published input enums for the four tuning objectives, candidate strategies, benchmark phases, and finalist selection scopes.
- Added typed, additive output schemas and stable `headline` summaries for performance-case classification, session budgets, benchmark changes, and plan counts without removing existing response keys.
- Added Windows Python 3.12 to CI alongside Ubuntu Python 3.12 and 3.13.

### Changed

- Added the terminal `performance_only` candidate state. Finalist performance measurements can complete when semantic comparison is impossible, but the state never represents semantic proof or deployment approval.
- `finalize_tuning_session` now accepts `selection_scope`, defaults to `proven`, and requires explicit `performance_only` opt-in before selecting an unproven finalist.
- Restored `combined` for multi-family rewrites and added `rewrite_plus_index` for lineage-backed index children. Existing lineage-backed `combined` records remain readable as a deprecated compatibility form.
- Runtime status now reports the configured `tool_groups`; MCP tool discovery remains in the protocol-standard `tools/list` `result.tools` location.

### Fixed

- Evidence persistence now normalizes UUID values to canonical strings, date and time values to ISO-8601 strings, and `Decimal` values to precision-preserving strings while continuing to reject unknown object types.
- Initial evidence collection and idempotent replay now return the same normalized persisted sections, evidence id, and content.
- Unsupported snapshot proof no longer ends a promising candidate before finalist performance measurement. Complete, nonzero finalist evidence is required before a candidate can become `performance_only`.
- Volatile functions hidden in referenced views now participate in the same preflight used by performance cases, result comparisons, and rewrite and index benchmarks.
- Invalid tool arguments now return a sanitized `invalid_arguments` envelope without echoing caller input or exposing Pydantic internals.
- Test paths now use pytest-managed temporary directories instead of fixed `/tmp` locations.

## [2.1.0] - 2026-07-24

### Added

- Published MCP protocol capabilities for durable performance cases, iterative tuning sessions, arbitrary plan comparison, leased sandbox index experiments, and restart-safe sandbox view changes.
- Added local policy ceilings for multi-hour tuning campaigns and adaptive workflow timeouts sized to the permitted per-request execution count.
- Added exact query identity, Azure SQL capability detection, durable idempotency fencing, and prepared Query Store mutation workflows.

### Fixed

- Candidate benchmarking now executes each measured sample once, uses snapshot-consistent duplicate-aware finalist comparison, and continues after losing experiments.
- Temporary-index and view workflows now preserve exact ownership, cleanup, rollback, and interrupted-operation recovery state.
- Triage and plan metrics no longer infer healthy state or query totals from incomplete or operator-level evidence.

### Changed

- `execute_tsql_unrestricted` now supports general DBA T-SQL instead of limiting raw execution to read-only statements. Direct and statically recoverable `DROP DATABASE` commands remain blocked.
- Applied DBA batches use a single non-retried submission on an isolated connection, drain all result sets, and discard the connection after every outcome.
- Operator guidance now documents the exact unprofiled DBA startup posture and distinguishes Azure control-plane delete protection from T-SQL permissions.

### Security

- The `DROP DATABASE` guard is documented as defense in depth: runtime-assembled SQL and behavior hidden behind existing modules cannot be proven by static inspection. Database permissions, Azure RBAC/resource locks, and alternate client access remain independent controls.
- DBA audit previews, rollback text, and error details redact SQL literals by default; uncertain post-submission failures are recorded as outcome unknown.

## [2.0.0] - 2026-07-15

### Added

- Versioned performance-case, evidence, tuning-session, tuning-candidate, and plan-action contracts with permission-restricted SQLite persistence.
- Named `triage`, `optimizer`, `sandbox`, `enforcer-review`, and `enforcer-apply` profiles plus a fail-closed local database policy.
- Iterative rewrite benchmarking, duplicate- and order-aware snapshot comparison, arbitrary plan-summary comparison, durable index leases, and exact plan-action rollback.
- Fail-closed plan verification for matching parameter buckets and non-overlapping Query Store evidence windows.

### Changed

- Measured samples now execute each user query exactly once while returning its result sample and actual plan.
- Full result comparison now preserves duplicate multiplicity, requested ordering, and column type signatures; unavailable or truncated proof is inconclusive.
- Query tuning continues after neutral, regressed, failed, or cleanup-required candidates and records an explicit terminal outcome for every experiment.
- Sandbox index screening and finalist validation measure every recorded parameter bucket within the same 80-execution session budget.
- Direct Query Store and test-index mutation tools are preview-only; writes use prepared plan actions or the leased sandbox index workflow.
- Query-health triage now uses Azure SQL resource, Query Store, wait, blocking, statistics, parameter-sensitivity, and regression evidence. PLE, buffer-cache ratio, and fragmentation are not health classifiers.

### Security

- Durable state omits raw SQL by default, stores fingerprints and redacted evidence, and uses owner-only directory and file permissions.
- Plan apply requires a reviewed intent, evidence hash, exact prior state, server and database policy, an open kill switch, explicit authorization, and an idempotency key.
- Concurrent or uncertain plan applies and rollbacks enter durable reconciliation states instead of crossing the database boundary twice.

### Fixed

- `apply_plan_action` ignored `dry_run`: the tool exposed no such parameter, so a client passing `dry_run=true` had it silently discarded and the Query Store force/unforce executed for real (only blocked by DB permissions in live testing). The tool now accepts `dry_run` defaulting to true, matching the documented admin-tool contract.
- `dry_run_plan_action` always failed ("'dict' object can't be awaited"): the service method was synchronous while the tool wrapper awaits every callback. Now async; verified live with preview + audit + rollback SQL.

- Row-capped fetches no longer drain the remaining result set: advancing past a truncated result set pulled every leftover row over the wire (a 17.9M-row `SELECT *` hit the tool timeout instead of returning 200 rows; now 0.09s). SHOWPLAN sessions still drain — their plan XML arrives as a later result set.
- `check_capabilities` plan probes now target a user table: SQL Server exempts catalog-only statements from the SHOWPLAN permission check, so the old sys.objects probe reported plans as available under logins where explain_query would be denied.

- `set_query_store_hints` never worked live: the driver binds str parameters as varchar while `sp_query_store_set_hints` requires nvarchar; the hints value is now routed through an `nvarchar(max)` variable.
- Workload index analysis (`analyze_workload_indexes`, `optimize_indexes`) failed on every parameterized Query Store text ('must declare the scalar variable @P1'); stored text is now auto-bound before plan compilation, and captured DDL/DML statements are skipped instead of reported as errors.
- Histogram-based parameter binding never worked live: `range_high_key` is sql_variant, which the driver cannot fetch; it is now CONVERTed server-side (style 121).
- `optimize_indexes` size estimation used `sys.dm_db_partition_stats.rows`; the column is `row_count`.
- `analyze_db_health` resource-governance probe named tier-specific columns (`primary_max_cpu_percent` is absent on serverless GP) and silently returned nothing; it now SELECTs * and projects, so serverless tiers get their real limits.
- `tune_query` Query Store history now matches by query_hash from the captured plan (with original-text fallback); the bound DECLARE batch could never match stored text.
- `tune_query`, `benchmark_query_rewrite`, and `explain_query` with `auto_bind_params` no longer fail on parameterized SQL: the read-only validator accepts a `DECLARE` / `SET @variable` prefix before the single SELECT (T-SQL variables are batch-scoped). Session SET options are still rejected.
- `explain_query` with `analyze=true` bounds its result-set fetches (`row_limit + 1`); previously the executed query's full result set was fetched into memory before the plan XML.
- `get_lock_details`, `get_open_transactions`, `get_active_sessions`, and `get_tempdb_usage` are bounded with clamped `limit` parameters and truncation reporting; waiting locks and oldest transactions sort first.
- Transient error codes match on digit boundaries only (code 233 no longer fires on "12330 rows").
- Non-UTF-8 `varbinary` values are hex-encoded (`0x...`) instead of being mangled by a lossy decode.
- Restricted-mode text rules scan comment/literal-stripped SQL, so string data like `'item#1'` or `'please execute the plan'` no longer rejects legitimate SELECTs.
- `get_query_history_by_text` escapes LIKE wildcards in the query fingerprint.
- Database allowlist matching is case-insensitive, matching Azure SQL semantics.
- Config load rejects `AZURE_SQL_TOOL_TIMEOUT_SECONDS` below the query timeout.
- Hostile user-defined type names from the catalog fall back to `nvarchar(256)` in generated `DECLARE` blocks instead of being interpolated verbatim.

### Added

- `AZURE_SQL_TRUST_SERVER_CERTIFICATE` (default false) for self-hosted SQL Server endpoints with self-signed certificates; encryption stays on. Verified live: the full integration suite (including the stdio end-to-end test, which previously skipped for localhost) passes against a SQL Server 2022 Docker container, and 45 of 48 swept restricted tools work on-prem.

- `get_connection_pool_stats` tool (performance group): per-database pool metrics and connection-leak detection without a database round-trip.

### Changed

- Require `mssql-python>=1.10.0`: versions before 1.10 hold the GIL during `cursor.execute`, serializing the entire server behind any in-flight query and preventing the asyncio tool timeout from firing. Verified live on 1.10.0: GIL released, concurrent tool calls work, all 61 live checks and the integration suite pass. Driver-level query/lock timeouts remain set as defense-in-depth (README gotcha 5).
- Pooled connections are no longer recycled on a 45-minute token clock (tokens only matter at login); `SELECT 1` validation runs only after 60s of idle time instead of on every acquire.
- Named profiles expose intentionally different tool surfaces, so documentation no longer relies on one global tool count.

- Aligned docs with current `explain_query` safety behavior (hypothetical indexes disabled on this tool).
- Updated integration workflow bootstrap scripts to use `mssql_python` instead of undeclared `pyodbc`.
- Added baseline community files: `LICENSE`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, and `SECURITY.md`.

## [1.0.0] - 2026-04-09

### Changed

- Bumped version to 1.0.0 for public release.
- Added PyPI metadata (license, classifiers, project URLs) to `pyproject.toml`.
- Sanitized error messages in internal logging paths (`health.py`, `plans.py`, `query_index_analysis.py`, `index_optimizer.py`) to prevent potential leakage of server names or connection details.

### Added

- GitHub Actions CI workflow for automated testing on Python 3.12 and 3.13.

## [0.1.0] - 2026-04-07

### Added

- Initial standalone Azure SQL MCP server release.
- Restricted/unrestricted execution modes with read-only SQL validator in restricted mode.
- Schema introspection, Query Store analysis, plan inspection, health checks, and admin tools.
- Unit and integration test suites.
