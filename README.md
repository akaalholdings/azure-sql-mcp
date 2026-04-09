# Azure SQL MCP

**Production-grade Model Context Protocol server for Azure SQL Database.**

44 tools across query execution, deep performance diagnostics, schema comparison, and administration — built and live-tested against real Azure SQL DB tiers.

Comprehensive unit and integration coverage is included; run `uv run pytest -q` for the current test counts in your environment.

---

## Why this exists

LLMs are extraordinarily good at writing T-SQL and reading execution plans. They are catastrophically bad at it without structured tools that give them schema, statistics, plans, and DMV state in a token-efficient form.

Most SQL MCP servers are toy wrappers around `cursor.execute()`. Azure SQL MCP is built the way an Azure SQL DBA would build one:

- **Read-only by default.** Restricted mode parses every query through `sqlglot`'s T-SQL dialect and rejects anything that isn't a single safe SELECT — including blocking `EXEC`, `DBCC`, temp tables, external rowsets, dynamic SQL, and cross-database / linked-server names. Defense-in-depth, not a regex.
- **Memory-safe.** Server-side `fetchmany(row_limit + 1)` instead of `fetchall()`. A 10M-row `SELECT *` won't OOM the MCP host or burn the model's context window.
- **Tier-aware.** DMV queries are written to survive across GeneralPurpose / BusinessCritical / Hyperscale, where columns like `primary_max_cpu_percent` and catalogs like `sys.master_files` only exist on some tiers. Found and fixed in live testing — see the [Azure SQL DB gotchas](#azure-sql-db-gotchas) section.
- **Session-scoped SET handling.** Tools like `explain_query` need `SET SHOWPLAN_XML ON` to persist across a pooled connection — Azure SQL MCP has a custom `execute_session` path so the SET and the query share a single physical connection. Most other servers silently get this wrong.
- **Token-efficient by tool grouping.** Run with `--azure-sql-tool-groups core` (13 tools, ~1.1k tokens) instead of all 44 tools (~4.2k tokens) when you want a slim surface. Verified, not estimated.

---

## Quickstart

```bash
git clone https://github.com/AkaalHoldings/azure-sql-mcp.git
cd azure-sql-mcp
uv sync

export AZURE_SQL_SERVER="your-server.database.windows.net"
export AZURE_SQL_DEFAULT_DATABASE="appdb"
export AZURE_SQL_ALLOWED_DATABASES="appdb,reportingdb"
export AZURE_SQL_AUTH_MODE="entra-default"   # uses az login / managed identity
export AZURE_SQL_ACCESS_MODE="restricted"

uv run azure-sql-mcp
```

That's a fully-functional read-only MCP server on stdio, ready to plug into Claude Desktop, Cursor, or any other MCP client.

---

## Install

Requires Python 3.12+ and [`uv`](https://github.com/astral-sh/uv).

```bash
cd azure-sql-mcp
uv sync
```

**On Linux**, `mssql-python` needs the runtime libraries documented by Microsoft. The included `Dockerfile` installs them on Debian:

```bash
docker compose up --build
```

No external ODBC driver manager is required — `mssql-python` ships its own driver.

---

## Configure

All settings are env vars. CLI flags override env vars.

### Required

| Variable | Purpose |
|---|---|
| `AZURE_SQL_SERVER` | Logical server FQDN, e.g. `sqltestit.database.windows.net` |
| `AZURE_SQL_DEFAULT_DATABASE` | Database used when a tool call omits `database_name` |
| `AZURE_SQL_ALLOWED_DATABASES` | Comma-separated allowlist. `default_database` must be in this list. |

### Access mode

| Variable | Default | Values |
|---|---|---|
| `AZURE_SQL_ACCESS_MODE` | `restricted` | `restricted` (read-only validator), `unrestricted` (adds `execute_tsql_unrestricted` + 4 admin tools) |

### Auth (`AZURE_SQL_AUTH_MODE`)

| Mode | Required env vars |
|---|---|
| `entra-default` *(default)* | none — uses `DefaultAzureCredential` (az CLI, managed identity, env, etc.) |
| `service-principal` | `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` |
| `interactive` | none — opens a browser flow |
| `sql-password` | `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD` |

### Tool grouping (token efficiency)

| Variable | Default | Values |
|---|---|---|
| `AZURE_SQL_TOOL_GROUPS` | `all` | Comma-separated list of `core`, `performance`, `schema`, `admin`, `all` |

Example: `AZURE_SQL_TOOL_GROUPS=core,performance` exposes 36 tools instead of all 44.

### Limits & runtime

| Variable | Default | Purpose |
|---|---|---|
| `AZURE_SQL_ROW_LIMIT` | `200` | Max rows returned per query (server-side `fetchmany`) |
| `AZURE_SQL_QUERY_TIMEOUT_SECONDS` | `30` | Per-query timeout |
| `AZURE_SQL_TOOL_TIMEOUT_SECONDS` | `query_timeout + 15` | Outer per-tool-call timeout |
| `AZURE_SQL_POOL_SIZE` | `5` | Per-database connection pool size |
| `AZURE_SQL_MAX_RETRIES` | `3` | Retries for transient connection failures |
| `AZURE_SQL_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `AZURE_SQL_LOG_FORMAT` | `text` | `text` or `json` |

### Transports

```bash
uv run azure-sql-mcp                                                  # stdio (default)
uv run azure-sql-mcp --transport sse --host 0.0.0.0 --port 8000
uv run azure-sql-mcp --transport streamable-http --host 0.0.0.0 --port 8000
```

Remote transports have no built-in authentication or TLS in this server. Do not expose them directly to the public internet. Use a private network boundary and a reverse proxy/API gateway that enforces TLS and authentication.

---

## MCP client setup

### Claude Desktop

`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS):

```json
{
  "mcpServers": {
    "azure-sql": {
      "command": "uv",
      "args": ["--directory", "/path/to/azure-sql-mcp", "run", "azure-sql-mcp"],
      "env": {
        "AZURE_SQL_SERVER": "your-server.database.windows.net",
        "AZURE_SQL_DEFAULT_DATABASE": "appdb",
        "AZURE_SQL_ALLOWED_DATABASES": "appdb",
        "AZURE_SQL_AUTH_MODE": "entra-default"
      }
    }
  }
}
```

### VS Code (MCP extension)

```json
{
  "mcp.servers": {
    "azure-sql": {
      "command": "uv",
      "args": ["--directory", "/path/to/azure-sql-mcp", "run", "azure-sql-mcp"],
      "env": {
        "AZURE_SQL_SERVER": "your-server.database.windows.net",
        "AZURE_SQL_DEFAULT_DATABASE": "appdb",
        "AZURE_SQL_ALLOWED_DATABASES": "appdb"
      }
    }
  }
}
```

### Cursor

```json
{
  "mcpServers": {
    "azure-sql": {
      "command": "uv",
      "args": ["--directory", "/path/to/azure-sql-mcp", "run", "azure-sql-mcp"],
      "env": {
        "AZURE_SQL_SERVER": "your-server.database.windows.net",
        "AZURE_SQL_DEFAULT_DATABASE": "appdb",
        "AZURE_SQL_ALLOWED_DATABASES": "appdb"
      }
    }
  }
}
```

---

## Tool catalogue

44 tools across 5 groups. Tools marked **(unrestricted)** require `AZURE_SQL_ACCESS_MODE=unrestricted`.

### `core` — query, introspection, health (13 tools)

| Tool | Purpose |
|---|---|
| `list_databases` | List databases configured in `AZURE_SQL_ALLOWED_DATABASES` |
| `check_capabilities` | Probe permission-sensitive features (Query Store, SHOWPLAN, DMV access) |
| `list_schemas` | List user schemas |
| `list_objects` | List tables, views, procedures, functions, or indexes in a schema |
| `search_objects` | Cross-schema name-pattern search |
| `get_object_details` | Columns, indexes, constraints, definition |
| `get_dependencies` | What this object references and what references it |
| `get_table_stats` | Approximate row counts and storage breakdown |
| `execute_sql` | Read-only SQL through the safety validator |
| `explain_query` | Estimated or actual execution plan for read-only SQL with optional parameter auto-binding |
| `get_top_queries` | Query Store top-N by duration / CPU / IO / executions / memory / resource blend |
| `analyze_index_recommendations` | Missing-index DMV + automatic-tuning recommendations |
| `analyze_db_health` | 11 health checks: index, buffer, autotune, storage, query-store, log-rate, etc. |

### `performance` — deep diagnostics & tuning (23 tools)

**Query analysis & tuning**
- `analyze_query_indexes` — recommend indexes for up to 10 queries via plan analysis
- `analyze_workload_indexes` — workload-driven index recommendations from Query Store
- `optimize_indexes` — end-to-end index optimization
- `compare_query_plans` — diff two execution plans
- `detect_regressed_queries` — find queries whose plans got worse
- `detect_parameter_sniffing` — identify parameter-sensitive plans
- `get_forced_plans` — list Query Store forced plans

**Wait stats & blocking**
- `get_wait_stats` — `sys.dm_db_wait_stats` with category mapping (CPU/IO/Lock/Memory/Network) and benign filtering
- `get_query_wait_stats` — per-query wait breakdown from Query Store
- `get_currently_waiting_tasks` — real-time blocked tasks from `sys.dm_os_waiting_tasks`
- `get_active_sessions` — running queries with blocking chains
- `get_lock_details` — current locks with mode, resource, and SQL text
- `get_open_transactions` — open transactions with duration, type, log bytes, warnings
- `get_deadlock_history` — parsed deadlock XML from `system_health` xevent session

**Tempdb & memory**
- `get_tempdb_usage` — per-session tempdb consumption
- `get_tempdb_space_breakdown` — version store / user / internal / free
- `get_memory_grants` — granted vs. requested memory with pending grants

**Resource governance & I/O**
- `get_io_stats` — per-file latency, throughput, pending I/O (Azure-SQL-DB-safe)
- `get_resource_limits` — service objective + governance limits (tier-tolerant)
- `get_resource_stats_history` — `sys.dm_db_resource_stats` history with sustained-pressure detection

**Plan cache & statistics**
- `get_plan_cache_analysis` — plan cache pressure, single-use plans, recompiles
- `get_query_compilation_stats` — compilation hot-spots
- `check_statistics_health` — stale / out-of-date statistics

### `schema` — schema comparison & migration (3 tools)

- `capture_schema_snapshot` — point-in-time snapshot of tables, views, procs, functions, indexes, FKs
- `compare_schemas` — diff two databases with grouped, categorized differences
- `generate_migration_script` — emit T-SQL to transform source schema → target schema

### `admin` — write & destructive operations (5 tools, **unrestricted**)

- `execute_tsql_unrestricted` — arbitrary T-SQL bypass of the validator
- `rebuild_index` — `ALTER INDEX REBUILD` / `REORGANIZE` with `ONLINE = ON`
- `update_statistics` — `UPDATE STATISTICS` with optional sample percent
- `force_query_plan` — `sp_query_store_force_plan` / `sp_query_store_unforce_plan`
- `kill_session` — `KILL <spid>` with system-SPID guard (refuses `<= 50`)

---

## Security model

### Restricted mode (default)

The `execute_sql` tool routes every query through `SafeSqlValidator` (`safe_sql.py`), which:

1. **Text pre-checks** reject obvious red flags before parsing.
2. **`sqlglot` AST parse** in T-SQL dialect rejects anything that isn't a single read-only statement.
3. **AST inspection** rejects any node referencing dangerous surface area.

**Allowed:** `SELECT`, CTEs, set operators (`UNION`/`EXCEPT`/`INTERSECT`), `sys.*` catalog reads, `OPENJSON`, parameterized values.

**Blocked:** all DML/DDL, `EXEC`, `GO` batches, `DBCC`, temp tables, external rowsets (`OPENROWSET`, `OPENDATASOURCE`), extended procedures, OLE automation, cross-database three-part names, linked-server four-part names, dynamic SQL.

### Unrestricted mode

Setting `AZURE_SQL_ACCESS_MODE=unrestricted` adds five additional tools that bypass the validator. The restricted-mode `execute_sql` tool **continues to validate** — unrestricted mode adds a separate dangerous tool rather than weakening the safe one.

The 4 admin tools (`rebuild_index`, `update_statistics`, `force_query_plan`, `kill_session`) emit narrow, parameterized SQL — they do not just shell out arbitrary statements.

### Database allowlist

Every tool call resolves `database_name` against `AZURE_SQL_ALLOWED_DATABASES`. Calls referencing a database not in the allowlist fail before any connection is opened. There is no way to escape the allowlist short of changing the env var.

### Memory safety

`AzureSqlExecutor._consume_batches` uses `cursor.fetchmany(row_limit + 1)` instead of `cursor.fetchall()`. The `+ 1` lets the server detect truncation without loading the entire result set. A `SELECT * FROM huge_table` returns 201 rows, sets `truncated: true`, and never blows out memory.

---

## Architecture

```mermaid
graph TD
    Client["MCP Client<br/>(Claude / Cursor / VS Code)"] -->|stdio · SSE · HTTP| FastMCP
    FastMCP --> Tools["44 Tool Handlers<br/>(grouped, prunable)"]
    Tools --> Validator["SafeSqlValidator<br/>(sqlglot T-SQL AST)"]
    Tools --> Services["Service Layer<br/>(introspection · plans · query-store ·<br/>health · waits · locks · tempdb ·<br/>resource-gov · schema-diff · ...)"]
    Services --> Executor["AzureSqlExecutor<br/>fetch_all / execute_batches /<br/>execute_session / execute_non_query"]
    Executor --> Pool["ConnectionPool<br/>(per-database, max_size, retry)"]
    Pool --> Auth["Authenticator<br/>(DefaultAzureCredential / SP / SQL)"]
    Auth --> AzureSQL[("Azure SQL Database")]
```

Key invariants:

- **Single logical server per MCP instance.** Multiple databases on that server are exposed via the allowlist.
- **`execute_session` keeps a single physical connection** across a sequence of statements — required for session-scoped SET options like `SET SHOWPLAN_XML ON`.
- **Connections that error are discarded**, not returned to the pool, so a bad session can't poison the next caller.
- **Tools are pruned post-registration** based on `tool_groups`, so a `core`-only deployment never even advertises `get_wait_stats` to the client.

---

## Azure SQL DB gotchas

These are real bugs found and fixed during live testing against an Azure SQL DB GeneralPurpose Gen5 tier. Worth knowing if you're writing your own DMV-based tools:

1. **`sys.master_files` is server-scoped** and not visible from a user database in Azure SQL DB. Use `sys.database_files` (which lives in every user DB) joined to `sys.dm_io_virtual_file_stats(DB_ID(), NULL)` on `file_id`.

2. **`sys.dm_user_db_resource_governance` columns vary by service tier.** `primary_max_cpu_percent`, `checkpoint_rate_mbps`, `volume_local_iops`, and `volume_pfs_iops` only exist on Premium / BusinessCritical / Hyperscale. Use `SELECT *` and access fields via `.get(...)`.

3. **`SET SHOWPLAN_XML ON` has two conflicting constraints:** it must be alone in its batch (so you can't concatenate it with the user query) *and* it is session-scoped. The SET and the query must run on the same physical connection — pooled batches grab a fresh connection per call. Use `execute_session([SET ON, query, SET OFF])`.

4. **Serverless cold-starts time out.** Auto-paused databases take 30+ seconds to wake up and the first connection usually fails with `TCP Provider: Timeout error [258]`. Prime each database with a `SELECT 1` warm-up before running a test suite.

---

## Development

### Run the test suite

```bash
uv run pytest                                     # all 260 unit tests
uv run pytest tests/unit/test_competitive_fixes.py -v   # R1/R2/R3/R7 + execute_session
```

### Run live integration tests

The repo includes a live integration script that exercises 27 tool paths against a real Azure SQL DB. It verifies:

- All 13 core tools
- Query tools (including R2 fetchmany truncation, SHOWPLAN session fix)
- 10 health & diagnostic tools
- Schema snapshot, compare, migration script generation
- 3 admin tools (with the SPID guard)

You'll need a server in `AZURE_SQL_ALLOWED_DATABASES` and a valid auth chain.

```bash
PATH="$PATH:$(dirname $(which az))" \
  uv run python tests/live/test_live_mcp.py
```

### Project layout

```
src/azure_sql_mcp/
├── server.py                  # FastMCP app, tool registration, pruning
├── config.py                  # ServerConfig, ToolGroup, env/CLI parsing
├── connection.py              # AzureSqlExecutor (fetch_all/execute_batches/
│                              # execute_session/execute_non_query)
├── connection_pool.py         # Per-database pool with retry & discard-on-error
├── auth.py                    # DefaultAzureCredential / SP / SQL auth
├── safe_sql.py                # sqlglot T-SQL AST validator
├── introspection.py           # list_*, get_object_details, dependencies
├── plans.py                   # SHOWPLAN_XML / STATISTICS XML
├── query_store.py             # Query Store top queries
├── query_index_analysis.py    # per-query index recommendations
├── index_recommendations.py   # missing-index DMV + autotune
├── index_optimizer.py         # workload-driven optimizer
├── health.py                  # 11 health checks
├── wait_stats.py              # wait categorization, benign filtering
├── lock_diagnostics.py        # locks, open trans, deadlock XML
├── tempdb_memory.py           # tempdb, memory grants
├── resource_governance.py     # IO stats, governance limits, resource history
├── plan_cache.py              # plan cache, compilation stats
├── query_regression.py        # regressed queries, parameter sniffing
├── schema_compare.py          # snapshot, diff, migration script
├── schema_snapshot.py         # snapshot capture
├── schema_diff.py             # diff engine
├── ddl_generator.py           # T-SQL migration script emitter
├── sessions.py                # active sessions, blocking chains
├── capabilities.py            # capability probe
├── param_binding.py           # @param auto-binding from histograms
├── retry.py                   # transient retry policy
├── observability.py           # structured logging, correlation IDs
└── prompts.py / resources.py  # MCP resources & prompt templates
```

---

## Known limitations

- **Azure SQL Database** is the supported surface for v1. SQL Server on-prem and Azure SQL Managed Instance work for most tools but are not part of the live test matrix.
- **No control-plane integration.** Server-level operations, ARM, and Azure Resource Graph are out of scope.
- **No automatic application of tuning recommendations.** `analyze_index_recommendations` returns the recommendations; applying them is left to the operator.
- **DMV-backed tools require role visibility** that varies by principal. `check_capabilities` reports what the current principal can and can't see.
- **`explain_query` does not support what-if index creation in v1.** Use `analyze_query_indexes` / `analyze_workload_indexes` for read-only index analysis.

---

## Author

**Balwinder Singh** — Senior Data Engineer
Balwinder@khojfrontiers.com

---

## License

MIT. See [LICENSE](LICENSE).

## Contributing

PRs welcome. Please run the unit suite (`uv run pytest`) before submitting. If you touch a DMV query, please verify it against a real Azure SQL DB GeneralPurpose tier — see [Azure SQL DB gotchas](#azure-sql-db-gotchas).
