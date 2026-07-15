# Azure SQL MCP

Azure SQL Database server for the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/). It gives an MCP client structured access to Azure SQL Database query execution, schema metadata, performance diagnostics, Query Store evidence, schema comparison, and guarded administration.

The implementation exposes 63 tools when all groups are enabled in unrestricted local mode: 53 non-admin tools plus 10 admin tools. Restricted mode is the default and exposes the 53 non-admin tools. The server also provides five schema resource templates, a token-safe artifact resource, and five guided prompts.

## Contents

- [Support boundary and routing](#support-boundary-and-routing)
- [Prerequisites and installation](#prerequisites-and-installation)
- [Quick start](#quick-start)
- [Run the server](#run-the-server)
- [MCP client configuration](#mcp-client-configuration)
- [Authentication](#authentication)
- [Safety model](#safety-model)
- [Configuration reference](#configuration-reference)
- [Tool groups and recommended paths](#tool-groups-and-recommended-paths)
- [Examples](#examples)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Repository location and history](#repository-location-and-history)

## Support boundary and routing

### Supported surface

- Azure SQL Database data-plane access to databases on one logical server per MCP process.
- Microsoft Entra token authentication through `DefaultAzureCredential`, service principal, or interactive browser credentials.
- Azure SQL Database SQL authentication through `sql-password` mode.
- Local MCP clients over stdio, or HTTP clients over Streamable HTTP or SSE.
- Read-only exploration, query evidence, performance diagnostics, schema comparison, migration-script generation, and explicitly gated maintenance operations.

### Out of scope

- Azure control-plane operations such as ARM, Azure Resource Graph, server provisioning, firewall changes, or service-tier changes.
- Automatic deployment of generated migration scripts, indexes, statistics changes, Query Store changes, or other maintenance.
- A general SQL Server or Azure SQL Managed Instance compatibility layer. `AZURE_SQL_TRUST_SERVER_CERTIFICATE=true` exists for controlled development or test endpoints; it does not expand the supported production target beyond Azure SQL Database.

### Choose the operating path

| Situation | Recommended path |
|---|---|
| Claude Desktop, Cursor, VS Code, or another client on the same machine | Use stdio with `AZURE_SQL_ACCESS_MODE=restricted`. Start with `core`, or use the default `all` group. |
| A private service used by one or more remote clients | Use Streamable HTTP, require `AZURE_SQL_MCP_BEARER_TOKEN`, put it behind TLS and a private network or gateway, and leave remote admin disabled. |
| A client that requires SSE | Use the SSE transport with the same bearer-token and network controls. |
| Schema discovery | Use the `explore-schema` prompt or the `azuresql://...` resources, then `list_objects` and `get_object_details`. |
| Query tuning | Start with `tune_query`; use `benchmark_query_rewrite` for a baseline/rewrite comparison; use `plan_health_review` and `get_top_queries` for workload evidence. |
| Potential writes or plan enforcement | Stay in restricted mode until the read-only evidence is reviewed. If administration is required, use local stdio unrestricted mode, preview first, and apply only through the write gates described below. |

## Prerequisites and installation

You need:

- Python 3.12 or newer.
- [`uv`](https://docs.astral.sh/uv/).
- An Azure SQL Database logical server and at least one database.
- A database principal with the permissions required by the tools you intend to use. DMV, Query Store, SHOWPLAN, and diagnostic visibility varies by principal and database configuration.

From this directory:

```bash
uv sync --dev
uv run azure-sql-mcp --help
```

The project uses `mssql-python`, `azure-identity`, `mcp[cli]`, and `sqlglot`. The Dockerfile installs the Debian runtime libraries needed by `mssql-python`. If a native Linux installation cannot load the driver libraries, use the Docker path below or install the equivalent system libraries for that distribution.

`.env.example` is a configuration reference. The server does not load `.env` files itself; export variables, pass them through the MCP client's `env` configuration, or use a process manager/container to load an environment file. Do not commit credentials.

## Quick start

The following starts a restricted, read-only stdio server. Replace the non-secret placeholders with values for the target Azure SQL Database.

```bash
cd /absolute/path/to/SQL/azure-sql-mcp
uv sync --dev

export AZURE_SQL_SERVER="your-server.database.windows.net"
export AZURE_SQL_DEFAULT_DATABASE="appdb"
export AZURE_SQL_ALLOWED_DATABASES="appdb,reportingdb"
export AZURE_SQL_AUTH_MODE="entra-default"
export AZURE_SQL_ACCESS_MODE="restricted"

# Authenticate through a credential available to DefaultAzureCredential,
# such as an Azure CLI login or managed identity.
uv run azure-sql-mcp
```

`AZURE_SQL_DEFAULT_DATABASE` must appear in `AZURE_SQL_ALLOWED_DATABASES`. A tool call that names another database is rejected before a connection is opened.

## Run the server

### Stdio

Stdio is the default and does not require an MCP bearer token:

```bash
uv run azure-sql-mcp
```

The executable name is `azure-sql-mcp`; the import package is `azure_sql_mcp`. MCP clients should launch the executable through `uv` with the project directory set to this directory.

### Streamable HTTP

Streamable HTTP listens on `127.0.0.1:8000` by default. Non-stdio transports require a bearer token at startup:

```bash
export AZURE_SQL_MCP_BEARER_TOKEN="replace-with-a-long-random-token"
uv run azure-sql-mcp \
  --transport streamable-http \
  --host 127.0.0.1 \
  --port 8000
```

The Streamable HTTP MCP endpoint is `/mcp`:

```bash
curl -i \
  -H "Authorization: Bearer replace-with-a-long-random-token" \
  http://127.0.0.1:8000/mcp
```

Use `--host 0.0.0.0` only when a private network boundary, TLS reverse proxy, or API gateway protects the service. Bearer authentication is not a replacement for TLS.

### SSE

```bash
export AZURE_SQL_MCP_BEARER_TOKEN="replace-with-a-long-random-token"
uv run azure-sql-mcp \
  --transport sse \
  --host 127.0.0.1 \
  --port 8000
```

For FastMCP's SSE transport, clients connect to `http://127.0.0.1:8000/sse` and send the same `Authorization: Bearer ...` header. Check the client transport documentation if it uses a different SSE configuration shape.

### Docker

The Dockerfile starts Streamable HTTP on `0.0.0.0:8000` and uses the same environment variables as the native server. Build it from the monorepo checkout:

```bash
docker build -t azure-sql-mcp ./azure-sql-mcp
docker run --rm \
  --publish 8000:8000 \
  --env-file /absolute/path/to/SQL/azure-sql-mcp/.env \
  azure-sql-mcp
```

The environment file supplied to Docker must contain the required connection variables and `AZURE_SQL_MCP_BEARER_TOKEN`. Docker reads that file; the Python server still does not load dotenv files on its own.

`docker-compose.yml` starts the MCP server together with a local SQL Server Developer container for development and integration testing:

```bash
export MSSQL_SA_PASSWORD="LocalTest-$(openssl rand -hex 16)!Aa1"
export AZURE_SQL_MCP_BEARER_TOKEN="$(openssl rand -hex 32)"
docker compose -f azure-sql-mcp/docker-compose.yml up --build
unset MSSQL_SA_PASSWORD AZURE_SQL_MCP_BEARER_TOKEN
```

That Compose stack is a local test fixture, not an Azure SQL Database deployment. It requires
runtime-generated SQL and bearer credentials and contains no checked-in password. Use it only
as a throwaway development environment.

## MCP client configuration

Keep secrets out of checked-in client configuration. The examples use placeholders only.

### Claude Desktop and Cursor: stdio

Both clients use an `mcpServers` object for local stdio servers. Add the following entry to the relevant client configuration and replace the project path and connection placeholders:

```json
{
  "mcpServers": {
    "azure-sql": {
      "command": "uv",
      "args": [
        "--directory",
        "/absolute/path/to/SQL/azure-sql-mcp",
        "run",
        "azure-sql-mcp"
      ],
      "env": {
        "AZURE_SQL_SERVER": "your-server.database.windows.net",
        "AZURE_SQL_DEFAULT_DATABASE": "appdb",
        "AZURE_SQL_ALLOWED_DATABASES": "appdb",
        "AZURE_SQL_AUTH_MODE": "entra-default",
        "AZURE_SQL_ACCESS_MODE": "restricted",
        "AZURE_SQL_TOOL_GROUPS": "core"
      }
    }
  }
}
```

For SQL password authentication, add `AZURE_SQL_USERNAME` and `AZURE_SQL_PASSWORD` with secret values supplied by the client or process manager. Do not put real passwords in a repository file.

### VS Code: stdio

VS Code uses `.vscode/mcp.json` with a top-level `servers` object:

```json
{
  "servers": {
    "azure-sql": {
      "type": "stdio",
      "command": "uv",
      "args": [
        "--directory",
        "/absolute/path/to/SQL/azure-sql-mcp",
        "run",
        "azure-sql-mcp"
      ],
      "env": {
        "AZURE_SQL_SERVER": "your-server.database.windows.net",
        "AZURE_SQL_DEFAULT_DATABASE": "appdb",
        "AZURE_SQL_ALLOWED_DATABASES": "appdb",
        "AZURE_SQL_AUTH_MODE": "entra-default",
        "AZURE_SQL_ACCESS_MODE": "restricted"
      }
    }
  }
}
```

### HTTP client configuration

For a client that supports remote MCP servers, configure Streamable HTTP with the `/mcp` URL and a bearer header:

```json
{
  "type": "http",
  "url": "http://127.0.0.1:8000/mcp",
  "headers": {
    "Authorization": "Bearer replace-with-a-long-random-token"
  }
}
```

For SSE-capable clients, use `type: "sse"` and `http://127.0.0.1:8000/sse`. Use HTTPS and a secret-management facility for non-local deployments.

## Authentication

There are two separate authentication layers:

1. Database authentication from the MCP process to Azure SQL Database.
2. Static bearer authentication from an HTTP/SSE MCP client to the server.

### Database authentication modes

Set `AZURE_SQL_AUTH_MODE` to one of these values:

| Mode | Required variables | Behavior |
|---|---|---|
| `entra-default` (default) | None enforced by config | Uses `DefaultAzureCredential`, such as Azure CLI credentials, managed identity, or another supported Entra credential source. |
| `service-principal` | `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` | Uses `ClientSecretCredential`. |
| `interactive` | None enforced by config; `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` may be supplied | Uses `InteractiveBrowserCredential`. |
| `sql-password` | `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD` | Uses SQL authentication over an encrypted connection. |

The connection uses `Encrypt=yes` by default. Keep `AZURE_SQL_TRUST_SERVER_CERTIFICATE=false` for Azure SQL Database. Only set it to `true` for a controlled development or test endpoint with a certificate that cannot be validated normally.

### MCP HTTP authentication

`stdio` is local-process transport and does not require `AZURE_SQL_MCP_BEARER_TOKEN`. `sse` and `streamable-http` fail startup without that variable and require clients to send an exact `Authorization: Bearer <token>` value. Pass the token through the environment or a secret manager rather than a shell command that could be exposed in process listings.

## Safety model

### Restricted mode: default

`AZURE_SQL_ACCESS_MODE=restricted` is the safe starting point:

- Admin tools are not registered.
- `execute_sql` accepts a single read-only SELECT-style statement after text checks, T-SQL parsing with `sqlglot`, and AST inspection. The supported parameter-binding form may include `DECLARE` and `SET @variable` statements before the SELECT; session SET options are not accepted.
- DML and DDL, `EXEC`, `GO` batches, `DBCC`, temporary tables, external rowsets, dynamic SQL, cross-database three-part names, linked-server four-part names, and other blocked constructs are rejected by the validator.
- Diagnostic tools remain read-only, although some require database permissions such as Query Store, SHOWPLAN, or DMV visibility.
- Every database argument is resolved against `AZURE_SQL_ALLOWED_DATABASES` before a connection is opened.
- Result fetching is bounded. The executor fetches at most `AZURE_SQL_ROW_LIMIT + 1` rows to detect truncation and returns only the configured limit.

Restricted mode does not mean every diagnostic will be available: capability-sensitive tools can return unavailable or permission-related results for a given principal or Azure SQL Database tier.

### Unrestricted mode: explicit administration surface

`AZURE_SQL_ACCESS_MODE=unrestricted` registers the 10 admin tools in addition to the read-only tools. It does not weaken the validator used by `execute_sql`.

Admin operations use the following gates:

1. Admin tool calls default to `dry_run=true` and return a preview plus an audit ID.
2. `AZURE_SQL_WRITE_POLICY=review` is the unrestricted default. It permits previews but refuses write execution.
3. Execution requires both `dry_run=false` in the tool call and `AZURE_SQL_WRITE_POLICY=apply`.
4. `AZURE_SQL_WRITE_POLICY=disabled` blocks write execution.
5. Remote admin tools are not advertised over SSE or Streamable HTTP unless `AZURE_SQL_ENABLE_REMOTE_ADMIN=1`. Remote `apply` also requires that setting; otherwise configuration fails closed.
6. Live `create_test_index` and `drop_test_index` additionally require the target database to be listed in `AZURE_SQL_TEST_INDEX_DATABASES`. Test indexes use the enforced `IX_Testing_` prefix.

`execute_tsql_unrestricted` is still restricted to read-only SELECT-style batches after a hard denylist and parser checks. Use the generated admin tools for maintenance and Query Store actions. The supported plan-application tools are deliberately narrow and return rollback SQL where applicable.

Audit records are JSONL under `AZURE_SQL_AUDIT_DIR`. By default they include a SQL hash and preview, not full SQL. Set `AZURE_SQL_AUDIT_FULL_SQL=1` only when the environment can safely retain the submitted SQL, because query text can contain sensitive literals.

### Token-safe artifacts

Large plan payloads are stored as bounded, in-memory MCP resources such as `azuresql-artifact://...`. Tool responses include a resource URI, length, hash, and expiry metadata instead of embedding raw SHOWPLAN XML by default. Artifacts are per-process and do not survive a restart.

## Configuration reference

All values can be supplied as environment variables. Supported CLI flags take precedence when explicitly supplied. The server reads no `.env` file automatically.

### Required connection settings

| Variable | Default | Description |
|---|---:|---|
| `AZURE_SQL_SERVER` | — | Required logical server name, normally an Azure SQL Database FQDN. |
| `AZURE_SQL_DEFAULT_DATABASE` | — | Required database used when a tool omits `database_name`; it must be present in the allowlist. |
| `AZURE_SQL_ALLOWED_DATABASES` | — | Required comma-separated database allowlist. |

### Authentication and TLS

| Variable | Default | Description |
|---|---:|---|
| `AZURE_SQL_AUTH_MODE` | `entra-default` | `entra-default`, `service-principal`, `interactive`, or `sql-password`. |
| `AZURE_SQL_USERNAME` | — | Required for `sql-password`. |
| `AZURE_SQL_PASSWORD` | — | Required for `sql-password`; keep it in secret storage. |
| `AZURE_TENANT_ID` | — | Required for `service-principal`; optional input for `interactive`. |
| `AZURE_CLIENT_ID` | — | Required for `service-principal`; optional input for `interactive`. |
| `AZURE_CLIENT_SECRET` | — | Required for `service-principal`; keep it in secret storage. |
| `AZURE_SQL_TRUST_SERVER_CERTIFICATE` | `false` | Whether to trust an unverified server certificate. Keep `false` for Azure SQL Database. |

### Access, tools, and write policy

| Variable | Default | Description |
|---|---:|---|
| `AZURE_SQL_ACCESS_MODE` | `restricted` | `restricted` or `unrestricted`. |
| `AZURE_SQL_TOOL_GROUPS` | `all` | Comma-separated `core`, `performance`, `schema`, `admin`, or `all`. Group pruning controls which tools are advertised. |
| `AZURE_SQL_WRITE_POLICY` | `disabled` in restricted; `review` in unrestricted | `disabled`, `review`, or `apply`. Restricted mode always resolves to `disabled`. |
| `AZURE_SQL_TEST_INDEX_DATABASES` | empty | Comma-separated databases permitted for live test-index create/drop operations. |
| `AZURE_SQL_ENABLE_REMOTE_ADMIN` | `0` | Set `1` to advertise admin tools over SSE or Streamable HTTP and to permit remote apply configuration. |

### Query and process limits

| Variable | Default | Description |
|---|---:|---|
| `AZURE_SQL_ROW_LIMIT` | `200` | Maximum rows returned by bounded query results. |
| `AZURE_SQL_QUERY_TIMEOUT_SECONDS` | `30` | Per-query timeout; must be positive. |
| `AZURE_SQL_TOOL_TIMEOUT_SECONDS` | query timeout + `15` | Outer per-tool timeout; must be at least the query timeout. |
| `AZURE_SQL_POOL_SIZE` | `5` | Maximum pooled connections per database. |
| `AZURE_SQL_MAX_RETRIES` | `3` | Transient connection retry count; may be `0`. |
| `AZURE_SQL_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR`. |
| `AZURE_SQL_LOG_FORMAT` | `text` | `text` or `json`. |

### Transport and audit

| Variable | Default | Description |
|---|---:|---|
| `AZURE_SQL_TRANSPORT` | `stdio` | `stdio`, `sse`, or `streamable-http`. |
| `AZURE_SQL_HOST` | `127.0.0.1` | HTTP/SSE bind host. |
| `AZURE_SQL_PORT` | `8000` | HTTP/SSE bind port. |
| `AZURE_SQL_MCP_BEARER_TOKEN` | — | Required for SSE and Streamable HTTP; not required for stdio. |
| `AZURE_SQL_AUDIT_DIR` | `~/.azure-sql-mcp/audit` | Directory for JSONL previews, applies, blocks, and failures. |
| `AZURE_SQL_AUDIT_FULL_SQL` | `0` | Set `1` to include full SQL in audit records; default is hash plus preview. |

The corresponding frequently used CLI flags are `--transport`, `--host`, `--port`, `--log-level`, `--log-format`, `--azure-sql-server`, `--azure-sql-default-database`, `--azure-sql-allowed-databases`, `--azure-sql-auth-mode`, `--azure-sql-access-mode`, `--azure-sql-tool-groups`, and the `--azure-sql-*` options shown by `uv run azure-sql-mcp --help`.

## Tool groups and recommended paths

The default `all` group advertises every tool allowed by the selected access and transport mode. Set `AZURE_SQL_TOOL_GROUPS=core` for the smallest useful read-only surface. Tools not in the configured group are removed before the MCP client sees them.

### `core` — query, introspection, and health (15 tools)

| Tool | Use |
|---|---|
| `list_databases` | Confirm the server's configured database allowlist. |
| `check_capabilities` | Check Query Store, SHOWPLAN, and DMV-sensitive capabilities. |
| `list_schemas` | Enumerate schemas. |
| `list_objects` | List tables, views, procedures, functions, or indexes in a schema. |
| `search_objects` | Find objects by a SQL `LIKE` pattern across schemas. |
| `get_object_details` | Inspect columns, constraints, indexes, and definitions. |
| `get_dependencies` | Inspect references to and from an object. |
| `get_table_stats` | Get approximate row counts and storage information. |
| `execute_sql` | Execute validated read-only SQL. |
| `explain_query` | Capture an estimated or actual plan for read-only SQL. |
| `tune_query` | Build a single-query evidence pack. |
| `benchmark_query_rewrite` | Compare a baseline and rewrite with bounded samples and plan summaries. |
| `get_top_queries` | Read Query Store top queries by duration, CPU, IO, executions, memory, or resource blend. |
| `analyze_index_recommendations` | Read missing-index DMV and automatic-tuning recommendations. |
| `analyze_db_health` | Run the database health checks supported by the current permissions and tier. |

Recommended exploration path: `list_databases` → `check_capabilities` → `list_schemas` → `list_objects` → `get_object_details` → `get_dependencies` or `get_table_stats`.

Recommended single-query path: `tune_query` → inspect its plan, waits, Query Store, statistics, and index evidence → use `benchmark_query_rewrite` when a candidate rewrite exists. Explicit `parameter_values` are preferred for reproducible query comparisons.

### `performance` — diagnostics and tuning (35 tools)

**Query analysis and plan health**

`analyze_query_indexes`, `analyze_workload_indexes`, `optimize_indexes`, `compare_query_plans`, `detect_regressed_queries`, `detect_parameter_sniffing`, `get_query_parameter_buckets`, `get_forced_plans`, `plan_health_review`, `review_plan_enforcement`, `dry_run_plan_action`, and `plan_enforcer_tick`.

**Waits, blocking, tempdb, and memory**

`get_wait_stats`, `get_query_wait_stats`, `get_currently_waiting_tasks`, `get_active_sessions`, `get_lock_details`, `get_open_transactions`, `get_deadlock_history`, `get_tempdb_usage`, `get_tempdb_space_breakdown`, and `get_memory_grants`.

**Resource, storage, connection, cache, and statistics diagnostics**

`get_io_stats`, `get_resource_limits`, `get_resource_stats_history`, `get_connection_pool_stats`, `get_database_configuration`, `get_storage_diagnostics`, `get_connection_diagnostics`, `get_top_cached_queries`, `get_cached_routine_stats`, `get_object_index_diagnostics`, `get_plan_cache_analysis`, `get_query_compilation_stats`, and `check_statistics_health`.

Recommended performance path:

1. `analyze_db_health` and `get_top_queries` for broad evidence.
2. `explain_query` or `tune_query` for the highest-impact statements.
3. `get_active_sessions`, wait, lock, tempdb, resource, or cache tools for the suspected bottleneck.
4. `analyze_workload_indexes`, `analyze_query_indexes`, or statistics tools for a candidate remediation.
5. `plan_health_review` → `review_plan_enforcement` → `dry_run_plan_action` for Query Store plan decisions.
6. Keep `plan_enforcer_tick` in dry-run mode until the proposed actions have been reviewed.

Some diagnostic queries depend on Azure SQL Database tier or permission visibility. `check_capabilities` is the first check when a DMV-backed tool is unavailable.

### `schema` — comparison and migration-script generation (3 tools)

- `capture_schema_snapshot` — capture a point-in-time snapshot of tables, views, procedures, functions, indexes, and foreign keys.
- `compare_schemas` — compare two allowlisted databases and group the differences.
- `generate_migration_script` — generate T-SQL from a source schema to a target schema; it does not execute the script.

Recommended schema path: confirm both databases with `list_databases` → `capture_schema_snapshot` or `compare_schemas` → review dependencies and destructive changes → `generate_migration_script` → apply through a separately reviewed deployment process.

### `admin` — audited maintenance and destructive operations (10 tools)

These tools are registered only in unrestricted mode and are also hidden from remote transports unless remote admin is explicitly enabled.

- `execute_tsql_unrestricted` — audited raw SQL path that still accepts only read-only SELECT-style batches.
- `rebuild_index` — rebuild or reorganize an index.
- `update_statistics` — update statistics with an optional sample percentage.
- `force_query_plan` — force or unforce a Query Store plan through the supported stored procedures.
- `set_query_store_hints` — set an allowlisted Query Store hint.
- `clear_query_store_hints` — clear Query Store hints.
- `create_test_index` — create only an `IX_Testing_`-prefixed disposable index on an explicitly allowlisted test database.
- `drop_test_index` — drop only an `IX_Testing_`-prefixed test index.
- `apply_plan_action` — apply a reviewed Query Store force/unforce action.
- `kill_session` — kill a non-system session; system SPIDs are refused.

Recommended admin path: inspect read-only evidence → call the relevant admin tool with `dry_run=true` → review the returned SQL, rollback SQL, and audit ID → set `dry_run=false` only with an explicit `AZURE_SQL_WRITE_POLICY=apply` decision.

### Resources and prompts

Resource templates:

- `azuresql://{database}/schemas`
- `azuresql://{database}/{schema}/tables`
- `azuresql://{database}/{schema}/views`
- `azuresql://{database}/{schema}/procedures`
- `azuresql://{database}/{schema}/{table}`
- `azuresql-artifact://{artifact_id}` for token-safe artifacts created during the process

Guided prompts:

- `analyze-slow-queries`
- `review-index-health`
- `explore-schema`
- `compare-schemas`
- `troubleshoot-performance`

## Examples

### Execute a safe catalog query

An MCP client can call `execute_sql` with a database in the allowlist:

```json
{
  "database_name": "appdb",
  "sql": "SELECT TOP (20) name FROM sys.tables ORDER BY name"
}
```

The result is bounded by `AZURE_SQL_ROW_LIMIT`. If more rows are available, the response reports truncation rather than returning an unbounded result.

### Tune a parameterized query

Prefer explicit parameter values when comparing evidence across runs:

```json
{
  "database_name": "appdb",
  "sql": "SELECT OrderId, CustomerId, TotalAmount FROM dbo.Orders WHERE CustomerId = @CustomerId",
  "parameter_values": {
    "CustomerId": 42
  },
  "analyze": true,
  "include_raw_xml": false
}
```

Use these arguments with `tune_query`. The query must still satisfy the read-only validator. `include_raw_xml=false` keeps the plan in the artifact resource path.

### Compare a rewrite

Call `benchmark_query_rewrite` with the original and candidate read-only queries, set `runs` to a value such as `3`, and supply the same `parameter_values` to both sides. A matching bounded sample is evidence for the tested inputs, not proof of full semantic equivalence across all parameters or result-order contracts.

### Preview an administrative action

Use unrestricted mode only in an explicitly controlled environment:

```bash
export AZURE_SQL_ACCESS_MODE="unrestricted"
export AZURE_SQL_WRITE_POLICY="review"
export AZURE_SQL_TEST_INDEX_DATABASES="sandboxdb"
```

Call an admin tool with `dry_run=true`. Review its `sql_preview`, `sql_hash`, `audit_id`, and any `rollback_sql`. A preview does not execute the action. Applying it requires the separate policy and tool-call gates in [Safety model](#safety-model).

## Testing

Run the local checks from this directory:

```bash
uv run ruff check src tests
uv run pyright
uv run python -m compileall -q src tests
uv run pytest -q
uv build
```

The unit suite uses mocks and does not require Azure SQL Database credentials. Integration tests are skipped unless `AZURE_SQL_SERVER` is set. They require a valid target configuration and may create and remove an `mcp_integration` schema in the selected test database; use a disposable or explicitly approved database.

For an Azure SQL Database integration run, export the required connection settings and a valid auth mode before running only the integration tests:

```bash
export AZURE_SQL_SERVER="your-server.database.windows.net"
export AZURE_SQL_DEFAULT_DATABASE="appdb"
export AZURE_SQL_ALLOWED_DATABASES="appdb"
export AZURE_SQL_AUTH_MODE="entra-default"
uv run pytest tests/integration -q
```

The integration suite includes stdio end-to-end coverage, schema/resource/prompt checks, query and diagnostic paths, schema comparison, and guarded admin behavior. Do not point it at a production database without reviewing the fixture setup and cleanup first.

## Troubleshooting

### `AZURE_SQL_SERVER is required` or allowlist validation fails

Export all three required connection variables. `AZURE_SQL_DEFAULT_DATABASE` must be listed in `AZURE_SQL_ALLOWED_DATABASES`; tool calls are also restricted to that allowlist.

### HTTP/SSE startup fails because a bearer token is missing

Set `AZURE_SQL_MCP_BEARER_TOKEN` before using `--transport sse` or `--transport streamable-http`. Clients must send the same token in the `Authorization` header.

### Remote admin tools are missing

Admin tools require `AZURE_SQL_ACCESS_MODE=unrestricted`. For SSE or Streamable HTTP they also require `AZURE_SQL_ENABLE_REMOTE_ADMIN=1`; otherwise the server removes them before advertising its tool list. `AZURE_SQL_TOOL_GROUPS` can remove them as well.

### Remote apply configuration is rejected

For a remote transport, `AZURE_SQL_WRITE_POLICY=apply` requires `AZURE_SQL_ENABLE_REMOTE_ADMIN=1`. Keep remote admin disabled unless the network boundary, authentication, audit retention, and approval process are explicit.

### Entra authentication cannot obtain a token

Check that the selected credential source is available to `DefaultAzureCredential`, or select `service-principal`/`interactive` with the required values. Database permissions are separate from token acquisition; use `check_capabilities` after connecting.

### SQL password authentication fails at startup

Set both `AZURE_SQL_USERNAME` and `AZURE_SQL_PASSWORD` when `AZURE_SQL_AUTH_MODE=sql-password`. Keep `AZURE_SQL_TRUST_SERVER_CERTIFICATE=false` for Azure SQL Database and verify firewall, networking, login, and database access independently.

### A diagnostic tool reports an unavailable DMV or permission error

Run `check_capabilities` for the database. Azure SQL Database DMVs and columns vary by service tier, and visibility varies by principal. A capability failure is not evidence that the MCP transport is unhealthy.

### Results are truncated

This is expected when a result exceeds `AZURE_SQL_ROW_LIMIT` (default `200`). Add a narrower predicate, request an aggregate, or raise the limit deliberately while considering client context and memory.

### A plan response contains a resource URI instead of raw XML

Read the returned `azuresql-artifact://...` resource from the same running MCP process. Artifacts are bounded, in-memory, expire, and disappear when the process exits. Set `include_raw_xml=true` only for a client that needs inline XML.

### The first Azure SQL Database connection times out

A serverless or auto-paused database may need time to wake. Retry after the database is available, keep the query timeout and outer tool timeout consistent, and use a small warm-up query before a test run if the environment requires it.

### Docker starts but the client cannot connect

Confirm that the container is listening on port `8000`, the client uses `/mcp` for Streamable HTTP or `/sse` for SSE, and the bearer header is present. The Compose file's SQL Server service is a local test fixture; it is not the supported Azure SQL Database target.

## Repository location and history

This directory is the monorepo copy at `azure-sql-mcp/` in AkaalHoldings SQL. It is not a standalone checkout. Run package, test, and build commands from this directory, or point the MCP client's `uv --directory` argument at this directory.

The imported source came from `akaalholdings/azure-sql-mcp` at commit `4e590e2`. The monorepo copy is the documentation and integration surface for this checkout; repository-wide navigation and CI are maintained outside this README.

See [SECURITY.md](SECURITY.md), [CONTRIBUTING.md](CONTRIBUTING.md), [CHANGELOG.md](CHANGELOG.md), and [LICENSE](LICENSE) for the package-local policies and history.
