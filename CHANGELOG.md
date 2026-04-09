# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

## [Unreleased]

### Changed

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
