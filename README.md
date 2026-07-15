# Azure SQL MCP

`azure-sql-mcp` is the typed execution and evidence layer for Azure SQL Database performance work. It gives MCP clients bounded read access, durable performance cases, iterative query benchmarks, leased sandbox index tests, and reviewed Query Store plan actions.

The supported tuning path is evidence-first but rewrite-active: a missing plan lowers confidence; it does not prevent a concrete static rewrite. A failed or slower experiment rejects only that candidate and does not end the session.

## What it owns

- Read-only SQL execution, metadata, plans, Query Store, waits, blocking, resource, statistics, and parameter-sensitivity evidence.
- Versioned `EvidenceEnvelopeV1`, `PerformanceCaseV1`, `TuningSessionV1`, `TuningCandidateV1`, and `PlanActionIntentV1` contracts.
- Redacted SQLite state under `~/.azure-sql-mcp/state` by default.
- Exactly-once measured query samples with the result sample and actual plan from the same execution.
- Interleaved baseline/candidate benchmarking with medians, spread, noise classification, and parameter buckets.
- Snapshot-consistent, shape-, duplicate-, and order-aware result comparison where a complete bounded comparison is possible.
- Durable temporary-index leases, automatic cleanup, and startup recovery of expired leases.
- Prepared Query Store plan actions with prior-state capture, policy checks, verification, and exact rollback.

The Copilot operating instructions live in [`../skills/`](../skills/). The skills decide what to investigate and how to present the result; this package owns database execution, policy, durable state, and deterministic workflow transitions.

## Support boundary

Supported:

- Azure SQL Database PaaS.
- Local MCP clients over stdio.
- Streamable HTTP or SSE for read-only private-service use when bearer authentication and network controls are configured.
- Microsoft Entra authentication through `DefaultAzureCredential`, service principal, or interactive browser credentials.
- SQL password authentication when supplied from protected local secret storage.
- Read-only SELECT-shaped active benchmarks. DML and side-effecting procedures are not executed by the tuning workflow.

Not supported:

- Azure control-plane changes, server provisioning, firewall changes, or service-tier changes.
- Automatic production index deployment.
- Autonomous plan forcing.
- Treating a bounded sample as proof of equivalence.
- Treating PLE, buffer-cache ratio, or fragmentation thresholds as query-health conclusions.

## Install

Requirements: Python 3.12 or newer and [`uv`](https://docs.astral.sh/uv/).

```bash
cd azure-sql-mcp
uv sync --dev --locked
uv run azure-sql-mcp --help
```

The server does not load `.env` files automatically. Supply local settings through the MCP client, a process manager, or protected environment injection. [`.env.example`](.env.example) contains placeholders only.

## Minimal read-only start

```bash
export AZURE_SQL_SERVER="your-server.database.windows.net"
export AZURE_SQL_DEFAULT_DATABASE="your-database"
export AZURE_SQL_ALLOWED_DATABASES="your-database"
export AZURE_SQL_AUTH_MODE="entra-default"
export AZURE_SQL_ACCESS_MODE="restricted"
export AZURE_SQL_WRITE_POLICY="disabled"
export AZURE_SQL_PROFILE="triage"
export AZURE_SQL_TOOL_GROUPS="core,performance"

uv run azure-sql-mcp
```

The default transport is stdio. This configuration can inspect only databases in `AZURE_SQL_ALLOWED_DATABASES`; Azure SQL permissions remain the final authority.

## VS Code Copilot

Create a local `.vscode/mcp.json` in the workspace. Do not commit machine paths or environment-specific values.

```json
{
  "servers": {
    "azure-sql-triage": {
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
        "AZURE_SQL_DEFAULT_DATABASE": "your-database",
        "AZURE_SQL_ALLOWED_DATABASES": "your-database",
        "AZURE_SQL_AUTH_MODE": "entra-default",
        "AZURE_SQL_ACCESS_MODE": "restricted",
        "AZURE_SQL_WRITE_POLICY": "disabled",
        "AZURE_SQL_PROFILE": "triage",
        "AZURE_SQL_TOOL_GROUPS": "core,performance"
      }
    }
  }
}
```

Reload VS Code, enable the server in Copilot Chat, then call `list_databases` and `check_capabilities`. Full client setup and profile runbooks are in [`docs/09-operations.md`](docs/09-operations.md).

## Named profiles

`AZURE_SQL_PROFILE` is enforced by the server. It removes tools that do not belong to the selected workflow; it does not grant database permission or silently enable writes.

| Profile | Purpose | Required posture | Important tools |
| --- | --- | --- | --- |
| `triage` | Incident and broad performance diagnosis | restricted, write disabled | performance cases, evidence collection, waits, blocking, Query Store, resources, statistics |
| `optimizer` | Read-only rewrite benchmarking | restricted, write disabled, benchmark policy | tuning sessions, candidates, rewrite benchmark, result and plan comparison |
| `sandbox` | Disposable non-production index tests | local stdio, unrestricted, write apply, sandbox policy | optimizer tools plus leased `benchmark_index_candidate` |
| `enforcer-review` | Query Store review and intent preparation | restricted, write disabled | plan health, preview-only review, `prepare_plan_action` |
| `enforcer-apply` | One authorized prepared plan action | local stdio, unrestricted, write apply, apply policy, kill switch open | apply, verify, and rollback prepared intents |

Named profiles always hide direct force, hint, raw plan-apply, and direct test-index mutation tools. The compatibility implementations of those tools are preview-only even when a server is started without a profile.

Profiles compose with `AZURE_SQL_TOOL_GROUPS`. A required tool must survive both filters.

## Local database policy

Repeated benchmarks, temporary indexes, and prepared plan actions fail closed unless `AZURE_SQL_DATABASE_POLICY_FILE` points to a valid local JSON document. Keep this file outside Git.

Synthetic policy example:

```json
{
  "version": 1,
  "databases": {
    "your-sandbox-database": {
      "environment": "sandbox",
      "allow_read": true,
      "allow_benchmark": true,
      "allow_test_indexes": true,
      "allow_plan_apply": false,
      "max_benchmark_executions": 80
    },
    "your-production-database": {
      "environment": "production",
      "allow_read": true,
      "allow_benchmark": false,
      "allow_test_indexes": false,
      "allow_plan_apply": false,
      "max_benchmark_executions": 0
    }
  }
}
```

Rules:

- Unknown databases are denied.
- `allow_read` does not imply benchmark or write permission.
- `max_benchmark_executions` is a hard per-request database-policy ceiling; the tuning session also has its own 80-execution default budget.
- Temporary indexes are rejected when the policy environment is `production`, `prod`, or `live`, even if another field is misconfigured.
- Plan apply requires `allow_plan_apply=true` in addition to every server and intent gate.

## Durable state and privacy

`AZURE_SQL_PERFORMANCE_STATE_DIR` defaults to `~/.azure-sql-mcp/state`. The directory is created with owner-only permissions and the SQLite file with owner read/write permissions where the platform supports POSIX modes.

Performance state stores:

- SQL and database fingerprints;
- plan fingerprints and sourced summaries;
- metric aggregates and evidence availability;
- artifact references;
- session/candidate state and budgets;
- plan-action prior state and verification decisions;
- temporary-index lease identifiers and cleanup targets.

Performance state does not persist raw SQL. Secret-like metadata and SQL-shaped metadata fields are dropped at the persistence boundary. The separate admin audit can include full generated SQL only when `AZURE_SQL_AUDIT_FULL_SQL=1`; leave it disabled unless an approved local audit process requires it.

## Read-only triage workflow

1. `start_performance_case` with the affected SELECT and up to four named parameter cases.
2. `collect_performance_evidence` with `execute_query=false` for broad read-only evidence.
3. Inspect the result status: `healthy`, `actionable`, `partial`, or `inconclusive`.
4. Use `get_performance_case` to retrieve redacted evidence and event history.
5. Hand the same case id to the optimizer or the Query Store review process.

Every diagnostic section carries collection window, availability, truncation, units, provenance, and stable query identity. Missing or truncated required evidence cannot produce `healthy`.

`collect_performance_evidence` focuses on Azure SQL resource history, Query Store state/history, waits, blocking/open transactions, statistics, parameter sensitivity, and regressions. `analyze_db_health` remains available for operational checks such as connections, constraints, replication, identity, Query Store configuration, storage, and statistics; it no longer grades PLE, buffer-cache ratio, or fragmentation as query health.

## Iterative optimizer workflow

1. Record result shape, NULL, duplicate, ordering, tie, isolation, and parameter semantics in the client workflow.
2. Produce concrete static rewrites before plan access whenever safe.
3. `start_performance_case` for the baseline and parameter cases.
4. `start_tuning_session`.
5. For each single-change experiment, call `add_tuning_candidate` with one family: predicate, join, aggregation, cardinality, index, or combined.
6. Call `benchmark_tuning_candidate` in `screening` phase.
7. Continue after neutral, regressed, equivalence-failed, timed-out, or otherwise inconclusive candidates.
8. Re-run credible winners in `finalist` phase.
9. Call `finalize_tuning_session` with the winner, if any, and an explicit stopping reason.

Default session limits:

| Limit | Default |
| --- | ---: |
| Candidates | 10 |
| Screening runs per candidate and parameter case | 3 |
| Finalist runs per candidate and parameter case | 5 |
| Parameter cases | 4 |
| Measured query executions | 80 |
| Wall-clock duration | 20 minutes |

Each measured sample runs the user query once. Baseline and rewrite order alternates between runs. The result includes per-side medians, min/max spread, sourced plan deltas, equivalence status, and execution count.

Candidate outcomes are `improved`, `neutral`, `regressed`, `equivalence_failed`, `inconclusive`, or `cleanup_required`. A screening winner remains open for finalist validation; finalization marks every unresolved experiment `inconclusive`, so the leaderboard has no ambiguous unfinished candidate.

### Equivalence

`compare_query_results` executes both SELECT-shaped queries inside one snapshot transaction. A match is proven only for the supplied parameter case when:

- result shape matches;
- the complete result fits inside the configured bound;
- values and duplicate multiplicity match;
- row order matches when `compare_order=true`;
- both statements complete in the same snapshot.

If the result is truncated, snapshot comparison is unavailable, or execution fails, the result is `inconclusive`, never proven. The client remains responsible for testing semantic cases beyond the supplied buckets.

### Compatibility tools

- `tune_query` starts a performance case/session and returns an evidence pack plus the next rewrite step.
- `benchmark_query_rewrite` wraps one screening candidate in the session engine.

They remain available for existing clients, but new integrations should use the explicit case/session tools to preserve a complete leaderboard.

## Sandbox index workflow

Use only `benchmark_index_candidate`; direct create/drop tools cannot perform live DDL.

Required gates:

- `AZURE_SQL_PROFILE=sandbox`;
- local stdio transport;
- `AZURE_SQL_ACCESS_MODE=unrestricted`;
- `AZURE_SQL_WRITE_POLICY=apply`;
- target in the normal database allowlist;
- local policy with non-production environment, benchmark permission, and temporary-index permission;
- active tuning session and matching candidate/query fingerprints.

The workflow writes a durable lease before DDL, measures the baseline and candidate for every parameter case recorded on the performance case, performs a full bounded result comparison for each case, and drops the `IX_Testing_` index in `finally`. The default four-bucket budget is 32 executions for screening and 48 for a finalist, exactly filling the 80-execution session limit. Cleanup failure produces `cleanup_required` and blocks another index experiment for that database. On the next sandbox start, expired leases are checked and cleanup is retried before the server accepts work.

The returned payload contains generated index DDL, rollback DDL, lease state, plan/metric deltas, classification, and the instruction to continue the tuning session.

## Reviewed plan enforcement

The only mutation path is:

1. Use `plan_health_review`, `review_plan_enforcement`, or preview-only `plan_enforcer_tick` under `enforcer-review`.
2. Call `prepare_plan_action` with the shared tuning session id, reviewed evidence, reviewer, reason, operation, and unique idempotency key.
3. Review the intent and exact prior force/hint state.
4. Start a local `enforcer-apply` process that points at the same state directory.
5. Set `AZURE_SQL_PLAN_APPLY_KILL_SWITCH=false` only for the authorized action.
6. Call `apply_prepared_plan_action` with the intent id and an explicit authorization reference.
7. Collect a non-overlapping post-change window for the same parameter buckets.
8. Call `verify_plan_action`.
9. Keep on improvement, return `hold` on insufficient evidence, or restore the exact prior force/hint state on regression.

Apply gates include the named profile, unrestricted local server, write policy, database policy, kill switch, prepared intent, evidence hash, exact current-vs-prior state, manual ownership, authorization reference, and idempotency key. Automatic Tuning ownership is detected and cannot be silently overridden.

`rollback_plan_action` restores the exact force-plan and Query Store hint state captured during preparation, then confirms the resulting state.

## Authentication

| `AZURE_SQL_AUTH_MODE` | Required local values |
| --- | --- |
| `entra-default` | A working `DefaultAzureCredential` source, such as Azure CLI or managed identity |
| `service-principal` | `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` |
| `interactive` | Interactive browser sign-in support |
| `sql-password` | `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD` |

Keep credentials in the operating-system credential store, managed identity, or a protected local environment source. Do not put them in MCP JSON committed to Git.

## Configuration reference

### Connection and workflow

| Variable | Default | Meaning |
| --- | --- | --- |
| `AZURE_SQL_SERVER` | required | Azure SQL logical server host |
| `AZURE_SQL_DEFAULT_DATABASE` | required | Default database for omitted tool arguments |
| `AZURE_SQL_ALLOWED_DATABASES` | required | Comma-separated database allowlist |
| `AZURE_SQL_AUTH_MODE` | `entra-default` | Authentication mode |
| `AZURE_SQL_ACCESS_MODE` | `restricted` | `restricted` or `unrestricted` |
| `AZURE_SQL_PROFILE` | none | Enforced named workflow profile; use one for suite operations |
| `AZURE_SQL_TOOL_GROUPS` | `all` | `core`, `performance`, `schema`, `admin`, or `all` |
| `AZURE_SQL_DATABASE_POLICY_FILE` | none | Local versioned policy; no file means benchmark and write denial |
| `AZURE_SQL_PERFORMANCE_STATE_DIR` | `~/.azure-sql-mcp/state` | Protected durable workflow state |
| `AZURE_SQL_PLAN_APPLY_KILL_SWITCH` | `true` | `true` blocks prepared plan apply; set `false` only during authorization |

### Limits and transport

| Variable | Default | Meaning |
| --- | --- | --- |
| `AZURE_SQL_ROW_LIMIT` | `200` | Maximum returned rows for bounded query paths |
| `AZURE_SQL_QUERY_TIMEOUT_SECONDS` | `30` | Per-query timeout |
| `AZURE_SQL_TOOL_TIMEOUT_SECONDS` | query timeout + 15 | Outer tool timeout; cannot be lower than query timeout |
| `AZURE_SQL_POOL_SIZE` | `5` | Connections per database pool |
| `AZURE_SQL_MAX_RETRIES` | `3` | Retry count for retry-safe connection operations; profiled samples are not retried |
| `AZURE_SQL_TRANSPORT` | `stdio` | `stdio`, `sse`, or `streamable-http` |
| `AZURE_SQL_HOST` | `127.0.0.1` | HTTP/SSE bind host |
| `AZURE_SQL_PORT` | `8000` | HTTP/SSE port |
| `AZURE_SQL_MCP_BEARER_TOKEN` | required remotely | Bearer token for SSE/HTTP |
| `AZURE_SQL_ENABLE_REMOTE_ADMIN` | `0` | Additional remote admin exposure gate; named write profiles should remain local |

### Audit and TLS

| Variable | Default | Meaning |
| --- | --- | --- |
| `AZURE_SQL_WRITE_POLICY` | disabled when restricted, otherwise review | `disabled`, `review`, or `apply` |
| `AZURE_SQL_AUDIT_DIR` | `~/.azure-sql-mcp/audit` | Permission-restricted admin audit directory |
| `AZURE_SQL_AUDIT_FULL_SQL` | `0` | Opt in to full generated SQL in admin audit records |
| `AZURE_SQL_TRUST_SERVER_CERTIFICATE` | `false` | Keep false for Azure SQL Database |
| `AZURE_SQL_LOG_LEVEL` | `INFO` | Logging level |
| `AZURE_SQL_LOG_FORMAT` | `text` | `text` or `json` |

Equivalent `--azure-sql-*` flags are available in `uv run azure-sql-mcp --help`.

## Tool groups

- `core`: bounded query execution, introspection, performance cases, tuning sessions, result/plan comparison, Query Store top queries, and operational health.
- `performance`: waits, blocking, resource history, statistics, query/index analysis, plan regression, and plan review.
- `schema`: schema capture, comparison, and migration-script generation. Generated scripts are not executed.
- `admin`: guarded maintenance and prepared apply tools. Named profiles prune unrelated direct mutation tools.

Resources include schema views and token-safe plan artifacts under `azuresql-artifact://{artifact_id}`. Artifact content is process-local and expires with the server.

## Verification

Normal checks require no database credentials:

```bash
uv sync --dev --locked
uv run ruff check src tests
uv run pyright
uv run python -m compileall -q src tests
uv run pytest -q
uv build
```

Live validation is opt-in. Use only an allowlisted dedicated non-production Azure SQL database. Start with the `optimizer` profile for read-only validation. Use `sandbox` only for leased test indexes and `enforcer-apply` only for one explicitly authorized prepared intent. Production remains read-only.

## Troubleshooting

### A profile tool is missing

Check both `AZURE_SQL_PROFILE` and `AZURE_SQL_TOOL_GROUPS`. Restricted access also removes admin-group tools. Restart the MCP process after changing environment values.

### A benchmark is denied

Confirm the database policy file exists, the database key matches the configured allowlist, `allow_benchmark=true`, and the requested execution count is within both policy and session budgets.

### An equivalence check is inconclusive

Narrow the result safely so the complete set fits inside the row bound, confirm snapshot isolation is available, and retry the same parameter case. Do not relabel a bounded or failed comparison as proven.

### An index lease requires cleanup

Stop further index tests. Restart the approved sandbox profile to retry expired-lease cleanup. If it remains `cleanup_required`, use the returned rollback DDL through the approved database change process and retain the lease as evidence until removal is confirmed.

### Plan apply is blocked

Check the prepared intent, current prior-state match, ownership, `enforcer-apply` profile, local stdio transport, unrestricted access, write policy, database policy, authorization reference, and kill switch. Do not fall back to a direct force or hint tool.

### A diagnostic is partial

Treat unavailable permissions, missing Query Store history, truncation, mismatched windows, or missing parameter buckets as evidence gaps. Recollect only the missing section against the same case instead of starting a new conclusion.

## Repository handoff

This package is part of the SQL monorepo. Repository-wide setup, skills, integrity checks, and CI are documented in [`../README.md`](../README.md). Package operations are in [`docs/09-operations.md`](docs/09-operations.md); release history is in [`CHANGELOG.md`](CHANGELOG.md); security reporting is in [`SECURITY.md`](SECURITY.md).
