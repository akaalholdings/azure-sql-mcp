# Security Policy

## Supported versions

Only the latest release line is supported for security updates.

## Reporting a vulnerability

Please do not open public issues for security vulnerabilities.

Use one of these private channels:

1. Open a GitHub private security advisory for this repository.
2. Contact project maintainers privately if advisory access is unavailable.

Include:

- A clear description of the issue
- Reproduction steps or proof of concept
- Impact assessment
- Suggested remediation (if available)

We will acknowledge reports as quickly as possible, validate the issue, and provide
remediation guidance and release timelines when confirmed.

## Runtime security model

- Keep `AZURE_SQL_ACCESS_MODE=restricted` unless an operator explicitly needs admin tooling.
- `sse` and `streamable-http` require `AZURE_SQL_MCP_BEARER_TOKEN`; clients must send `Authorization: Bearer <token>`.
- Put HTTP/SSE deployments behind TLS and a private network or gateway. The bearer token is not a replacement for TLS.
- Pass secrets (`AZURE_SQL_PASSWORD`, `AZURE_CLIENT_SECRET`, `AZURE_SQL_MCP_BEARER_TOKEN`) as environment variables, not CLI flags — flags are visible in process listings.
- Remote transports do not expose apply-capable admin behavior unless `AZURE_SQL_ENABLE_REMOTE_ADMIN=1` is set.
- Write-capable generated tools default to dry-run review. Execution requires `dry_run=false` and `AZURE_SQL_WRITE_POLICY=apply`.
- Raw arbitrary SQL execution is limited to read-only SELECT-style batches. Use generated admin tools for writes.
- Audit records are written to `AZURE_SQL_AUDIT_DIR`. By default records include SQL hash + preview, not full raw SQL; set `AZURE_SQL_AUDIT_FULL_SQL=1` only for controlled environments.
- Query Store apply support is limited to reversible `sp_query_store_force_plan` / `sp_query_store_unforce_plan` actions.
