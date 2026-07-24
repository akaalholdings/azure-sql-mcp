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
- Unprofiled `execute_tsql_unrestricted` accepts authorized DBA T-SQL except direct or statically recoverable `DROP DATABASE`. It requires unrestricted access, the `admin` or `all` tool group, write policy `apply`, `dry_run=false`, and an allowlisted initial database.
- Audit records are written to `AZURE_SQL_AUDIT_DIR`. By default records include SQL hash + preview, not full raw SQL; set `AZURE_SQL_AUDIT_FULL_SQL=1` only for controlled environments.
- Exact sandbox view rollback is disabled unless `AZURE_SQL_PERSIST_VIEW_SQL_STATE=true`; that opt-in stores target and prior view definitions in the owner-only performance-state database so recovery survives a process restart.
- Query Store apply support is limited to reversible `sp_query_store_force_plan` / `sp_query_store_unforce_plan` actions.

## General DBA execution boundary

The `DROP DATABASE` scanner is defense in depth. It rejects the command when it is present directly or can be recovered from literal `EXEC` / `sp_executesql` text and simple constant variables, including SQL inside a module definition submitted in the same batch. It cannot prove the behavior of SQL assembled from runtime data, an existing stored procedure, CLR code, or another database client. Do not describe this control as absolute database-drop prevention.

Applied DBA batches have intentionally conservative delivery semantics:

- one submission and no automatic retry;
- one isolated connection, discarded on completion, failure, timeout, or cancellation;
- all result sets drained, with returned rows still bounded by `AZURE_SQL_ROW_LIMIT`;
- an outcome-unknown audit state when execution may have crossed the database boundary;
- no `GO` separators, because `GO` is an SSMS/sqlcmd client command rather than T-SQL.

Keep the general DBA process on local stdio. If remote admin access is explicitly required, use bearer authentication, TLS, a private network boundary, and `AZURE_SQL_ENABLE_REMOTE_ADMIN=1`; this expands the exposure of a destructive, non-idempotent tool.

The database login remains the authoritative data-plane identity. Give it only the SQL permissions the operator requires and restrict alternate clients where possible. No MCP text scanner can constrain a highly privileged principal outside this process.

Azure Resource Manager delete and T-SQL `DROP DATABASE` are separate paths. Azure RBAC and resource locks govern control-plane deletion; SQL permissions govern commands submitted over a database connection. Apply protection to both surfaces and audit overlapping Azure role assignments. Neither an Azure RBAC role that omits resource delete nor this MCP scanner, by itself, proves that T-SQL database deletion is impossible.

Azure SQL Database's subscription-level Block T-SQL CRUD control is a stronger
platform gate, but it also blocks `CREATE DATABASE` and several `ALTER DATABASE`
operations. It therefore does not implement this tool's
broad-DBA-except-drop contract.
