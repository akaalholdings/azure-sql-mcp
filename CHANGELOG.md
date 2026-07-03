# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

## [Unreleased]

### Fixed

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

- `get_connection_pool_stats` tool (performance group): per-database pool metrics and connection-leak detection without a database round-trip.

### Changed

- Pooled connections are no longer recycled on a 45-minute token clock (tokens only matter at login); `SELECT 1` validation runs only after 60s of idle time instead of on every acquire.
- Tool surface is now 63 tools (53 restricted); README counts corrected.

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
