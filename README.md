# Azure SQL MCP

`azure-sql-mcp` is the typed execution and evidence layer for Azure SQL Database performance and administration work. It gives MCP clients bounded read access, durable performance cases, iterative query benchmarks, leased sandbox index tests, reversible sandbox view changes, reviewed Query Store plan actions, and an explicitly gated general DBA execution path.

The supported tuning path is evidence-first but rewrite-active: a missing plan lowers confidence; it does not prevent a concrete static rewrite. A failed or slower experiment rejects only that candidate and does not end the session.

## What it owns

- Read-only SQL execution, metadata, plans, Query Store, waits, blocking, resource, statistics, and parameter-sensitivity evidence.
- Versioned `EvidenceEnvelopeV1`, `PerformanceCaseV1`, `TuningSessionV1`, `TuningCandidateV1`, and `PlanActionIntentV1` contracts.
- Redacted SQLite state under `~/.azure-sql-mcp/state` by default.
- Exactly-once measured query samples with the result sample and actual plan from the same execution.
- Interleaved baseline/candidate benchmarking with medians, spread, noise classification, and parameter buckets.
- Snapshot-consistent, shape-, duplicate-, and order-aware result comparison where a complete bounded comparison is possible.
- Database-aware equivalence preflight with recursive view-dependency and volatile-function analysis.
- Durable temporary-index leases, automatic cleanup, and startup recovery of expired leases.
- Reviewed view preparation with sandbox-only apply, durable restart recovery, verification, and exact rollback.
- Prepared Query Store plan actions with prior-state capture, policy checks, verification, and exact rollback.
- Evidence-linked decisions, terminal outcome reviews, reviewed lessons, and typed cross-skill handoffs.
- Audited general DBA T-SQL execution that rejects direct or statically recoverable `DROP DATABASE` statements.

The Copilot operating instructions live in the [`akaalholdings/SQL` skills](https://github.com/akaalholdings/SQL/tree/main/skills). The skills decide what to investigate and how to present the result; this package owns database execution, policy, durable state, and deterministic workflow transitions.

## Support boundary

Supported:

- Azure SQL Database PaaS.
- Local MCP clients over stdio.
- Streamable HTTP or SSE for private-service use when bearer authentication and network controls are configured; admin tools require a separate remote-admin opt-in.
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

## General DBA start

Use a separate unprofiled local process for authorized DBA work. Do not set `AZURE_SQL_PROFILE`: every named profile deliberately hides `execute_tsql_unrestricted`.

```bash
export AZURE_SQL_SERVER="your-server.database.windows.net"
export AZURE_SQL_DEFAULT_DATABASE="master"
export AZURE_SQL_ALLOWED_DATABASES="master,appdb,reportingdb"
export AZURE_SQL_AUTH_MODE="entra-default"
export AZURE_SQL_TRANSPORT="stdio"
export AZURE_SQL_ACCESS_MODE="unrestricted"
export AZURE_SQL_WRITE_POLICY="apply"
export AZURE_SQL_TOOL_GROUPS="all"
export AZURE_SQL_ENABLE_REMOTE_ADMIN="0"
unset AZURE_SQL_PROFILE

uv run azure-sql-mcp
```

Call `execute_tsql_unrestricted` with `dry_run=true` to review the audit preview, then use `dry_run=false` for the authorized execution. The selected `database_name` must be in `AZURE_SQL_ALLOWED_DATABASES`; that allowlist controls the initial connection database, not every object an authorized T-SQL batch might reference.

This path accepts DBA T-SQL, including DDL, DML, maintenance commands, permission changes, module creation, and stored procedure execution. Its one command-level exclusion is `DROP DATABASE` when the statement appears directly or can be recovered statically from literal `EXEC` / `sp_executesql` text and simple constant variables. It does not reject runtime-opaque construction from non-literal data or behavior hidden inside an existing stored procedure. The scanner is defense in depth, not an authoritative database permission boundary.

Each applied DBA batch is submitted once with no retry, runs on an isolated connection that is discarded afterward, and drains every result set while returning only the configured row bound. If the connection fails or the tool is cancelled after submission, treat the outcome as unknown and reconcile database state before another attempt. Submit one T-SQL batch per call; `GO` is a client command used by tools such as SSMS and sqlcmd, not T-SQL understood by the server.

Azure control-plane deletion and T-SQL deletion are separate authorization surfaces. Azure RBAC or a resource lock governs deletion through Azure Resource Manager; SQL permissions govern `DROP DATABASE` submitted over a database connection. Configure both where deletion protection matters. The MCP scanner cannot replace least-privilege SQL credentials, Azure RBAC, resource locks, or restrictions on alternate database clients.

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
        "/absolute/path/to/azure-sql-mcp",
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
| `optimizer` | Read-only rewrite benchmarking and view preparation | restricted, write disabled, benchmark policy | tuning sessions, candidates, rewrite benchmark, result/plan comparison, view preview |
| `sandbox` | Disposable non-production index and view tests | local stdio, unrestricted, write apply, sandbox policy | optimizer tools plus leased index benchmark and prepared view apply/verify/rollback |
| `enforcer-review` | Query Store review and intent preparation | restricted, write disabled | plan health, preview-only review, `prepare_plan_action` |
| `enforcer-apply` | One authorized prepared plan action | local stdio, unrestricted, write apply, apply policy, kill switch open | apply, verify, and rollback prepared intents |

Named profiles always hide direct force, hint, raw plan-apply, and direct test-index mutation tools. The compatibility implementations of those tools are preview-only even when a server is started without a profile.

Profiles compose with `AZURE_SQL_TOOL_GROUPS`. A required tool must survive both filters.

## Local database policy

Repeated benchmarks, temporary indexes, prepared view changes, and prepared plan actions fail closed unless `AZURE_SQL_DATABASE_POLICY_FILE` points to a valid local JSON document. Keep this file outside Git.

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
      "allow_view_apply": true,
      "allow_plan_apply": false,
      "max_benchmark_executions": 80,
      "max_tuning_candidates": 60,
      "max_tuning_session_executions": 2000,
      "max_tuning_session_minutes": 360
    },
    "your-production-database": {
      "environment": "production",
      "allow_read": true,
      "allow_benchmark": false,
      "allow_test_indexes": false,
      "allow_view_apply": false,
      "allow_plan_apply": false,
      "max_benchmark_executions": 0,
      "max_tuning_candidates": 0,
      "max_tuning_session_executions": 0,
      "max_tuning_session_minutes": 0
    }
  }
}
```

Rules:

- Unknown databases are denied.
- `allow_read` is required for schema/catalog tools and schema MCP resources;
  it does not imply benchmark or write permission.
- `max_benchmark_executions` is the hard ceiling for one benchmark request.
- `max_tuning_candidates`, `max_tuning_session_executions`, and `max_tuning_session_minutes` cap the complete campaign. Set them explicitly when a reviewed deep search may run for hours.
- Temporary indexes are rejected when the policy environment is `production`, `prod`, or `live`, even if another field is misconfigured.
- View apply requires `allow_view_apply=true`, a non-production environment, the sandbox profile, and explicit durable view-SQL state.
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

Supported database scalar values are normalized at the persistence boundary:
UUID values become canonical strings, date and time values use ISO-8601, and
`Decimal` values use precision-preserving strings. Unknown object types remain
rejected. Initial collection and idempotent replay return the same normalized
persisted evidence sections.

Performance state does not persist raw query SQL by default. Secret-like metadata and SQL-shaped metadata fields are dropped at the normal persistence boundary. Sandbox view apply is the deliberate exception: exact crash recovery requires the target and prior view definitions, so it is disabled unless `AZURE_SQL_PERSIST_VIEW_SQL_STATE=true`. With that explicit opt-in, only durable view intents store raw view SQL in the same owner-only state directory and mode-0600 SQLite file. The separate admin audit can include full generated SQL only when `AZURE_SQL_AUDIT_FULL_SQL=1`; leave it disabled unless an approved local audit process requires it.

## Read-only triage workflow

1. `check_equivalence_preflight` with the affected SELECT and database.
2. `start_performance_case` with the affected SELECT, up to four typed named
   parameter cases, and an exact `query_store_query_id` when one is known.
3. `collect_performance_evidence` with `execute_query=false` for broad read-only evidence.
4. Inspect the result status: `healthy`, `actionable`, `partial`, or `inconclusive`.
5. Use `get_performance_case` to retrieve redacted evidence and event history.
6. Hand the same case id to the optimizer or the Query Store review process.

Every diagnostic section carries collection window, availability, truncation, units, provenance, and stable query identity. Missing or truncated required evidence cannot produce `healthy`.

Parameter-case values are fingerprinted but not persisted. Case responses
return value-free receipts/templates and exact fingerprint-v1 matching rules.
Session responses derive `deadline_exceeded`, `accepts_new_work`, and
`accepts_finalization` without rewriting the durable lifecycle state.

`collect_performance_evidence` focuses on Azure SQL resource history, Query Store state/history, waits, blocking/open transactions, statistics, parameter sensitivity, and regressions. `analyze_db_health` remains available for operational checks such as connections, constraints, replication, identity, Query Store configuration, storage, and statistics; it no longer grades PLE, buffer-cache ratio, or fragmentation as query health.

## Iterative optimizer workflow

1. Record result shape, NULL, duplicate, ordering, tie, isolation, and parameter semantics in the client workflow.
2. Produce concrete static rewrites before plan access whenever safe.
3. `check_equivalence_preflight` for the baseline and database.
4. `start_performance_case` for the baseline and parameter cases.
5. `start_tuning_session`, passing explicit candidate, execution, and time budgets when the user wants a deep search.
6. For each experiment, call `add_tuning_candidate` with one strategy. Use `combined` for multi-family rewrites and `rewrite_plus_index` only for an index child with a recorded parent.
7. Call `benchmark_tuning_candidate` in `screening` phase.
8. Continue after neutral, regressed, equivalence-failed, timed-out, or otherwise inconclusive candidates.
9. Re-run credible winners in `finalist` phase.
10. Call `finalize_tuning_session` with the winner, if any, an explicit stopping reason, and the default `selection_scope=proven`. Use `selection_scope=performance_only` only for an explicitly accepted unproven finalist.

Compatibility defaults, used only when the caller does not request another
policy-authorized budget:

| Limit | Default |
| --- | ---: |
| Candidates | 10 |
| Screening runs per candidate and parameter case | 3 |
| Finalist runs per candidate and parameter case | 5 |
| Parameter cases | 4 |
| Measured query executions | 80 |
| Wall-clock duration | 20 minutes |

These values are not product ceilings. A local policy can authorize a
multi-hour campaign with a larger candidate and execution budget. The session
remains durable across client restarts and stops at its configured budget,
after all credible candidate families and combinations have terminal evidence,
or at a written evidence-based diminishing-return point.

`check_capabilities` returns the effective `local_tuning_policy` ceilings
without exposing the policy file or connection configuration. Benchmark tool
timeouts scale to the allowed per-request execution count and configured query
timeout, so a policy-authorized long campaign is not cut off by a fixed
20-minute wrapper.

Each measured sample runs the user query once. Parameterized SQL uses typed `sp_executesql`, never a local-variable compatibility batch. Baseline and rewrite order alternates between runs. The result includes per-side medians, min/max spread, sourced plan deltas, equivalence status, and execution count.

Rewrite screening normally defers full equivalence and costs six executions per parameter case: three baseline/candidate pairs. Finalist validation adds one two-query snapshot comparison, so five pairs cost twelve executions per case and 48 for four cases. Screening one case and validating four costs 54; screening all four and validating four costs 72. All work shares the configured session execution limit.

Input contracts publish these enums:

- Objectives: `elapsed_time`, `cpu`, `logical_reads`, and `physical_reads`.
- Strategies: `predicate`, `join`, `aggregation`, `cardinality`, `index`, `combined`, and `rewrite_plus_index`.
- Benchmark phases: `screening` and `finalist`.
- Finalist selection scopes: `proven` and `performance_only`.

Candidate outcomes are `improved`, `performance_only`, `neutral`, `regressed`, `equivalence_failed`, `inconclusive`, or `cleanup_required`. A screening winner remains open for finalist validation; finalization marks every unresolved experiment `inconclusive`, so the leaderboard has no ambiguous unfinished candidate.

`performance_only` requires complete, nonzero finalist measurements that show
improvement when semantic equivalence cannot be proven. It is terminal but
unproven: it never means semantic equivalence, deployment readiness, or
automatic deployment approval. Finalization selects it only through explicit
`selection_scope=performance_only`; the default `proven` scope rejects it.

`combined` is the normal strategy for a rewrite that combines multiple query
families. `rewrite_plus_index` is an index child whose `candidate:` artifact
references a recorded parent candidate. A performance-only parent propagates
`parent_equivalence=unproven`, so its child cannot become a proven winner.
Existing lineage-backed `combined` records remain readable as a deprecated
compatibility form, but new lineage-backed index children use
`rewrite_plus_index`.

### Equivalence

Call `check_equivalence_preflight(sql, database_name)` before opening a case or
comparing or benchmarking candidates. It returns coverage, risks, a verdict for
each detected clock, volatile, or safely seeded function, resolved view
dependencies, and unresolved dependencies. Referenced view definitions are
inspected recursively to a maximum depth of eight. Encrypted, inaccessible,
unresolved, cyclic, or depth-exceeded dependencies fail closed. Only summaries
are persisted; raw definitions are not.

`compare_query_results` executes both SELECT-shaped queries inside one snapshot transaction. A match is proven only for the supplied parameter case when:

- result shape matches;
- the complete result fits inside the configured bound;
- values and duplicate multiplicity match;
- row order matches when `compare_order=true`;
- both statements complete in the same snapshot.

If the result is truncated, snapshot comparison is unavailable, or execution fails, the result is `inconclusive`, never proven. The client remains responsible for testing semantic cases beyond the supplied buckets.

When preflight shows that direct snapshot proof is impossible, finalist
validation still runs its complete performance workload and skips only the
impossible semantic comparison. An improving finalist can then become
`performance_only` under the gates above. Direct-snapshot-safe finalist
behavior is unchanged.

### Compatibility tools

- `tune_query` starts a performance case/session and returns an evidence pack plus the next rewrite step.
- `benchmark_query_rewrite` wraps one screening candidate in the session engine.

They remain available only to unprofiled compatibility clients. Named profiles
intentionally omit them; new integrations should use the explicit case/session
tools to preserve a complete leaderboard.

## Sandbox index workflow

Use only `benchmark_index_candidate`; direct create/drop tools cannot perform live DDL.

Required gates:

- `AZURE_SQL_PROFILE=sandbox`;
- `AZURE_SQL_TOOL_GROUPS=core,performance,admin`;
- local stdio transport;
- `AZURE_SQL_ACCESS_MODE=unrestricted`;
- `AZURE_SQL_WRITE_POLICY=apply`;
- target in the normal database allowlist;
- local policy with non-production environment, benchmark permission, and temporary-index permission;
- active tuning session and matching candidate/query fingerprints.

The workflow writes a durable lease before DDL. `CREATE INDEX` and a private
index-level ownership marker commit in one transaction. Cleanup requires that
marker and the exact observed definition, so a same-name external index is
never adopted or dropped. The workflow then performs
baseline/index/post-cleanup A-B-A measurements, verifies that the expected
index was used, and drops the `IX_Testing_` index before the final baseline
phase. Screening costs nine executions per parameter case; a five-run finalist
costs fifteen per case and 60 for four. Screening may use an unchanged subset
of the recorded cases; a finalist must use all of them.

Because DDL separates the phases, this is not a same-snapshot rewrite-equivalence test. The SQL is unchanged, and MCP requires complete non-truncated result fingerprints to remain stable across A-B-A. Data movement makes the result inconclusive. Cleanup failure produces `cleanup_required` and blocks another index experiment for that database. A completed idempotent reservation is retrieved rather than rerun. On the next sandbox start, expired leases are checked and cleanup is retried before the server accepts work.

The returned payload contains generated index DDL, rollback DDL, lease state, plan/metric deltas, classification, and the instruction to continue the tuning session.

## Sandbox view workflow

`prepare_view_change` is read-only under `optimizer` and `sandbox`. Under `optimizer` it is a process-local preview only. An optimizer preview cannot be applied by another MCP process.

Apply only through a local `sandbox` process:

1. Set a non-production policy entry with `allow_view_apply=true` and set `AZURE_SQL_PERSIST_VIEW_SQL_STATE=true`.
2. Call `prepare_view_change` again in that sandbox process and review the target, dependency, legality, prior-state, apply, rollback, durable change id, and raw-state disclosure.
3. Call `apply_prepared_view_change` with `reviewed_intent=true` and the same caller-stable idempotency key.
4. Call `verify_view_change`.
5. Call `rollback_view_change` when the candidate loses or verification fails. Existing views restore the exact prior definition; a workflow-created view is dropped only when the current definition still matches the prepared target.

The view mutation, its private ownership marker, and its catalog precondition
checks execute in one transaction. A new prepare is rejected while another
suite marker owns the view. The sandbox intent and exact rollback state survive
MCP restarts. If apply is interrupted, call `verify_view_change` with the same
change id. MCP adopts rollback ownership only when the target and durable
database-side marker both match; otherwise it returns `hold` and retains the
original rollback contract. Do not re-prepare against a possibly changed view
until that intent is reconciled.

Production view deployment is outside this workflow and requires its normal owner-approved release path.

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
| `AZURE_SQL_PERSIST_VIEW_SQL_STATE` | `false` | Explicitly permit exact view SQL in the protected durable state store for restart-safe sandbox apply/rollback; requires a filesystem state directory |
| `AZURE_SQL_LEGACY_STATE_SERVER_BINDING` | none | Temporary upgrade attestation for pre-v1 durable state. It must exactly match `AZURE_SQL_SERVER`; without it, server-agnostic legacy identities remain blocked. Remove it after active legacy workflows have completed or been retired. |
| `AZURE_SQL_PLAN_APPLY_KILL_SWITCH` | `true` | `true` blocks prepared plan apply; set `false` only during authorization |

### Limits and transport

| Variable | Default | Meaning |
| --- | --- | --- |
| `AZURE_SQL_ROW_LIMIT` | `200` | Maximum returned rows for bounded query paths |
| `AZURE_SQL_COMPARISON_ROW_LIMIT` | `10000` | Maximum complete rows per result set eligible for equivalence or A-B-A stability proof |
| `AZURE_SQL_QUERY_TIMEOUT_SECONDS` | `30` | Per-query timeout |
| `AZURE_SQL_TOOL_TIMEOUT_SECONDS` | query timeout + 15 | Outer tool timeout; cannot be lower than query timeout |
| `AZURE_SQL_POOL_SIZE` | `5` | Connections per database pool |
| `AZURE_SQL_MAX_RETRIES` | `3` | Retry count for retry-safe connection operations; profiled samples and `execute_tsql_unrestricted` are not retried |
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
- `admin`: guarded maintenance, prepared apply, and unprofiled general DBA execution. Named profiles prune unrelated direct mutation tools.

`check_runtime_status` includes the configured `tool_groups` so a client can
confirm the effective runtime surface after startup.

MCP discovery follows the protocol: the tool array returned by `tools/list` is
under `result.tools`, not at the response root. Input schemas expose the
objective, strategy, phase, and selection-scope enums. Tool responses retain
their existing nested keys while adding typed output schemas and stable
`headline` objects for case classification, session budgets, benchmark
changes, and plan counts. Argument validation failures use a sanitized
`invalid_arguments` envelope; caller input and Pydantic internals are not
returned.

Resources include schema views and token-safe plan artifacts under `azuresql-artifact://{artifact_id}`. Artifact content is process-local and expires with the server.

## Evidence-governed learning

Local stdio servers expose advisory learning tools for `sql-health-triage@1.0.0`,
`sql-optimizer@2.3.0`, and `sql-plan-enforcer@1.0.0`. They persist redacted
`DecisionRecordV1`, `OutcomeReviewV1`, `LessonV1`, and `HandoffV1` contracts in
the existing owner-only `performance.sqlite3`. Remote transports do not expose
these tools, and an unavailable learning store leaves the normal static and
database-operation surfaces unchanged.

Lessons never authorize database changes or modify a skill. Normal lessons need
three aligned terminal reviews across at least two sessions and two subject
fingerprints before becoming eligible. Activation, rejection, retirement, and
supersession require the local maintainer CLI, a named reviewer, and the current
optimistic version:

```bash
uv run azure-sql-mcp-learning list
uv run azure-sql-mcp-learning activate lesson-id --reviewer reviewer-name --expected-version 0
uv run azure-sql-mcp-learning export --output learning-pack.json
uv run azure-sql-mcp-learning import learning-pack.json
```

Exports contain active lessons only. Imports are inactive proposals with
source-pack provenance and require fresh local approval. Learning contracts and
packs reject raw SQL, parameters, result rows, credentials, environment values,
and hidden reasoning.

## Verification

Normal checks require no database credentials:

```bash
uv sync --dev --locked
uv run ruff check src tests scripts
uv run pyright
uv run python -m compileall -q src tests scripts
uv run pytest -q
uv build
uv run python scripts/check_markdown_links.py
uv run python scripts/verify_repository_content.py
```

Live validation is opt-in. Use only an allowlisted dedicated non-production Azure SQL database. Start with the `optimizer` profile for read-only validation. Use `sandbox` only for leased test indexes or reviewed view changes, and `enforcer-apply` only for one explicitly authorized prepared intent. Do not use the general DBA path as a production smoke test.

CI runs the repository checks on Ubuntu with Python 3.12 and 3.13 and on
Windows with Python 3.12. Tests use pytest-managed temporary directories and do
not rely on a fixed `/tmp` path.

## Troubleshooting

### A profile tool is missing

Check both `AZURE_SQL_PROFILE` and `AZURE_SQL_TOOL_GROUPS`. Restricted access also removes admin-group tools. Restart the MCP process after changing environment values.

For general DBA work, leave `AZURE_SQL_PROFILE` unset, use `AZURE_SQL_ACCESS_MODE=unrestricted`, include `admin` or `all` in `AZURE_SQL_TOOL_GROUPS`, and use local stdio. Applied execution additionally requires `AZURE_SQL_WRITE_POLICY=apply` and `dry_run=false`. Remote transports require bearer authentication, private TLS termination, and `AZURE_SQL_ENABLE_REMOTE_ADMIN=1` before admin tools are exposed.

### A DBA batch has an unknown outcome

Do not resubmit automatically. Inspect the audit id and reconcile the intended database state using a separate read-only query. DBA batches are never retried by the server, because a disconnect or cancellation after submission does not prove that SQL Server did not execute the batch.

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

This is the canonical standalone repository for the Azure SQL MCP server. Companion SQL skills and broader assessment tooling remain in [`akaalholdings/SQL`](https://github.com/akaalholdings/SQL). Package operations are in [`docs/09-operations.md`](docs/09-operations.md); release history is in [`CHANGELOG.md`](CHANGELOG.md); security reporting is in [`SECURITY.md`](SECURITY.md).
