# Contributing

## Development setup

1. Install Python 3.12+ and [`uv`](https://github.com/astral-sh/uv).
2. Install dependencies:

```bash
uv sync --dev
```

## Run checks before opening a PR

```bash
uv run pytest -q
uv run python -m compileall -q src/azure_sql_mcp
uv build
```

## Contribution guidelines

- Keep behavior explicit and production-safe.
- Prefer focused changes over broad refactors.
- For safety-sensitive logic (`safe_sql.py`, admin tools, connection/auth), include tests for both success and failure paths.
- Keep documentation aligned with behavior; update `README.md` and relevant `docs/*.md` for user-visible changes.

## Pull request expectations

- Describe the problem and the exact behavior change.
- Call out backward-incompatible changes.
- Include test evidence (local test command output summary is enough).
- Flag any Azure-SQL-tier or permission assumptions.
