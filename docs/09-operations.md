# Operations guide

This guide covers local VS Code Copilot setup, the five enforced MCP profiles, and the separate unprofiled general DBA posture. Keep one MCP process per posture so the advertised tool surface and write authority are obvious.

## Before you start

You need:

- Python 3.12 or newer;
- `uv` on the PATH visible to VS Code;
- a local checkout of this repository;
- Azure SQL authentication supplied outside Git;
- a database principal with only the permissions required for the selected profile;
- a local database-policy file for repeated benchmarks, temporary indexes, view apply, or plan apply.

Install the package:

```bash
cd /absolute/path/to/azure-sql-mcp
uv sync --dev --locked
```

The optional companion skills remain in the
[`akaalholdings/SQL`](https://github.com/akaalholdings/SQL) repository:

```bash
cd /absolute/path/to/SQL
python3 skills/install_all.py --dest "$HOME/.copilot/skills"
python3 skills/check_installed_parity.py --dest "$HOME/.copilot/skills"
```

Reload VS Code after installing or changing a skill.

## VS Code MCP configuration

Create `.vscode/mcp.json` locally. Do not commit it when it contains local paths, database names, tenant details, usernames, tokens, or policy locations.

Read-only triage example:

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

`check_capabilities.local_tuning_policy` shows the effective limits for the
selected database. The server derives each benchmark tool's outer timeout from
`max_benchmark_executions` and the configured query timeout, with cleanup
headroom. Multi-hour session budgets therefore remain usable while every
individual SQL execution still has its own timeout.

The MCP client has an independent tool-call timeout. Increasing
`AZURE_SQL_TOOL_TIMEOUT_SECONDS` changes only the server budget; it cannot make
an earlier client cancellation wait longer. Inspect
`check_runtime_status.timeouts`, then configure the invoking client's
per-server timeout above the required workflow budget with transport headroom.
Use `evidence_workflow_seconds` for evidence collection plus its optional
profile stage. Use `session_workflow_seconds` for the default database's
effective per-request benchmark ceiling; the response also publishes the
policy execution limit and cleanup headroom used to derive it.
Codex uses `tool_timeout_sec`; GitHub Copilot CLI uses `timeout` in
milliseconds. Keep the same idempotency key and retrieve durable case/session
state after an uncertain response. Do not publish a universal
`window_minutes` ceiling: collection cost depends on the database and evidence
window.

After reload:

1. Open Copilot Chat.
2. Enable the configured MCP server in the tools picker.
3. Call `list_databases`.
4. Call `check_capabilities` for the selected database.
5. Run a bounded catalog query before a broader diagnostic.

The server does not load `.env` automatically. Use the MCP `env` block, a process manager, or protected environment injection.

## Local database policy

Create an uncommitted JSON file outside the repository:

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
    }
  }
}
```

Set its absolute path with `AZURE_SQL_DATABASE_POLICY_FILE`. Missing files,
invalid schemas, unknown databases, and omitted permissions fail closed.
Schema/catalog tools and schema MCP resources require `allow_read=true`;
runtime status, database listing, and capability/policy discovery remain
available so a denial can be diagnosed.

### Upgrading durable state from pre-v1 releases

Pre-v1 state identified a database by name only. The current identity also binds
the Azure SQL logical server, so legacy state is rejected by default rather
than being silently adopted on a different server.

To finish an interrupted pre-v1 workflow, first verify that the protected state
directory was created against the currently configured logical server. Then
temporarily set `AZURE_SQL_LEGACY_STATE_SERVER_BINDING` to the exact
`AZURE_SQL_SERVER` value and restart the MCP process. Matching records are
accepted only for that explicitly attested server; records that can be safely
rewritten in place, such as prepared plan intents, are upgraded when resumed.
A different binding fails startup. Remove the temporary variable after all
required legacy workflows have completed or been retired.

## Profile matrix

| Profile | Skill/context | Access | Write policy | Tool groups | Database policy |
| --- | --- | --- | --- | --- | --- |
| `triage` | `sql-health-triage` | restricted | disabled | core,performance | `allow_read` required for schema/catalog evidence |
| `optimizer` | `sql-optimizer`, read-only | restricted | disabled | core,performance | benchmark permission required for measured runs; view preparation remains read-only |
| `sandbox` | `sql-optimizer`, temporary index or view | unrestricted, local stdio | apply | core,performance,admin | benchmark, test-index, or view-apply permission as needed; non-production environment |
| `enforcer-review` | `sql-plan-enforcer`, review | restricted | disabled | core,performance | shared state path required for intent preparation |
| `enforcer-apply` | `sql-plan-enforcer`, one apply | unrestricted, local stdio | apply | core,performance,admin | plan-apply permission and open kill switch |
| unset (general DBA) | explicitly authorized DBA work | unrestricted, local stdio | apply | all | normal database allowlist; SQL permissions remain authoritative |

The profile is a server-side tool filter. Access mode, tool groups, Azure SQL permissions, database policy, and workflow state are independent gates.

## General DBA runbook

Run general DBA work in a separate local stdio process with no named profile. Every named profile hides the direct DBA tool.

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

Operational sequence:

1. Confirm the intended initial database is in `AZURE_SQL_ALLOWED_DATABASES`. Include `master` only when the task genuinely needs a connection to `master`.
2. Call `execute_tsql_unrestricted` with `dry_run=true`; retain the audit id and review the redacted SQL preview.
3. Submit one T-SQL batch without `GO` and call the tool with `dry_run=false`.
4. Review every returned result set and the completion audit record.
5. If the call times out, is cancelled, or loses its connection after submission, treat the result as unknown. Reconcile database state before considering another call.

The tool permits DDL, DML, maintenance, permissions, module creation, and stored procedure execution. It rejects direct `DROP DATABASE` and statically recoverable occurrences inside literal `EXEC` / `sp_executesql` text and simple constant variables. It intentionally does not claim to understand SQL assembled from runtime data or behavior inside an existing module. An authorized batch is submitted once with no retry, runs on an isolated connection that is discarded, and drains all result sets while bounding returned rows.

This guard is an application-layer safety check, not a SQL permission boundary. The database principal still determines what SQL Server accepts, and the principal can act outside MCP if another client is available. Use the least-privileged principal that supports the required task and keep alternate access under the normal DBA controls.

Azure control-plane deletion is separate from T-SQL execution. An Azure RBAC role or resource lock controls deletion through Azure Resource Manager; SQL permissions control `DROP DATABASE` over a database connection. Protect and audit both surfaces. A custom Azure role that omits database resource delete does not, by itself, remove a SQL principal's T-SQL permission, and overlapping role assignments can restore control-plane delete authority.

Azure SQL Database's subscription-level Block T-SQL CRUD control also blocks
`CREATE DATABASE` and several `ALTER DATABASE` operations. It is therefore not
compatible with this exact broad-DBA-except-drop posture. Use a narrower MCP
principal and explicit database allowlist instead of assuming that control can
distinguish the allowed administration statements from the prohibited ones.

For a remote transport, `AZURE_SQL_MCP_BEARER_TOKEN`, private TLS termination, and `AZURE_SQL_ENABLE_REMOTE_ADMIN=1` are additional mandatory server gates. Prefer local stdio because enabling remote admin exposes a destructive, non-idempotent tool across a network boundary.

## Triage runbook

Start:

```bash
export AZURE_SQL_PROFILE="triage"
export AZURE_SQL_ACCESS_MODE="restricted"
export AZURE_SQL_WRITE_POLICY="disabled"
export AZURE_SQL_TOOL_GROUPS="core,performance"
uv run azure-sql-mcp
```

Copilot prompt:

```text
Use sql-health-triage. Diagnose the selected Azure SQL database for the last 30 minutes.
Stay read-only, open one performance case, and report evidence gaps explicitly.
```

Expected sequence:

1. `start_performance_case` for a query-shaped incident, or workload tools for broad discovery.
2. `collect_performance_evidence` with `execute_query=false`.
3. Inspect Azure resource, Query Store, waits, blocking, statistics, parameter sensitivity, and regression sections.
4. Return only `healthy`, `actionable`, `partial`, or `inconclusive`.
5. Pass the case id and redacted artifact references forward.

Do not classify incomplete evidence as healthy. PLE, buffer-cache ratio, and fragmentation do not decide query health.

## Read-only optimizer runbook

Start:

```bash
export AZURE_SQL_PROFILE="optimizer"
export AZURE_SQL_ACCESS_MODE="restricted"
export AZURE_SQL_WRITE_POLICY="disabled"
export AZURE_SQL_TOOL_GROUPS="core,performance"
export AZURE_SQL_DATABASE_POLICY_FILE="/absolute/path/to/local-database-policy.json"
uv run azure-sql-mcp
```

Copilot prompt:

```text
Use sql-optimizer. Record the semantic contract, produce concrete rewrites immediately,
then benchmark one change at a time across common, rare, NULL, and boundary cases.
Continue after losing candidates and return the winning SQL plus the complete leaderboard.
```

Expected sequence:

1. Static semantic review and concrete candidate SQL.
2. `start_performance_case` with up to four parameter cases; each case includes
   `name`, exact `values`, exact SQL `types`, and a positive `weight`. Pass a
   known exact `query_store_query_id` here and again during evidence collection.
3. `start_tuning_session`.
4. `add_tuning_candidate` for one family at a time.
5. `benchmark_tuning_candidate` for three screening runs.
6. Continue after every non-winning result.
7. Re-run credible winners with five finalist runs.
8. `finalize_tuning_session` with a winner or a documented `no_change` stopping reason.

Parameter values are fingerprinted but never persisted. Retain the submitted
case payload in the active client workflow. Case responses return
`parameter_case_receipts`, value-free templates, `fingerprint_v1`, and exact
matching rules so a mismatch identifies its case index and received/expected
fingerprints without exposing values.

Session lifecycle status remains durable, while time availability is derived
at read time. After `deadline_exceeded=true` or `accepts_new_work=false`, do not
dispatch or replay benchmark work. `accepts_finalization=true` means already
measured or late terminal results can still be reconciled and finalized.

Measured candidates use typed `sp_executesql` and exactly-once user-query samples. Screening normally uses three baseline/candidate pairs: six executions per parameter case. Finalists use five pairs plus one two-query snapshot comparison: twelve per case and 48 for four. A full duplicate/order-aware comparison is proven only when both complete results fit `AZURE_SQL_COMPARISON_ROW_LIMIT` in one snapshot; otherwise equivalence is inconclusive.

## Sandbox index runbook

Use a dedicated non-production database and a separate MCP server entry.

```bash
export AZURE_SQL_PROFILE="sandbox"
export AZURE_SQL_TRANSPORT="stdio"
export AZURE_SQL_ACCESS_MODE="unrestricted"
export AZURE_SQL_WRITE_POLICY="apply"
export AZURE_SQL_TOOL_GROUPS="core,performance,admin"
export AZURE_SQL_DATABASE_POLICY_FILE="/absolute/path/to/local-database-policy.json"
uv run azure-sql-mcp
```

Safety gates:

- target database appears in `AZURE_SQL_ALLOWED_DATABASES`;
- local policy environment is not production-like;
- policy allows benchmarks and temporary indexes;
- the tuning case, session, candidate, and SQL fingerprints match;
- index identifiers pass strict validation;
- the lease is written before DDL;
- index creation and its private ownership marker commit atomically;
- no unresolved lease already exists for the database.

Call only `benchmark_index_candidate`, supplying an unchanged subset of
recorded parameter cases for screening and all recorded cases for a finalist.
It creates a namespaced disposable index, writes a private index-level marker
in the same transaction, measures baseline/index/post-cleanup A-B-A, verifies
expected index use, and removes the index only when both the marker and exact
definition match. Screening costs nine executions per case; a five-run
finalist costs fifteen per case and 60 for four.

DDL separates the phases, so the workflow does not call this same-snapshot rewrite equivalence. The SQL is unchanged, and complete non-truncated result fingerprints must remain stable across A-B-A. Data changes make the result inconclusive. A slower index is classified and rejected without ending the session. Cleanup failure returns `cleanup_required` and blocks another test. A finalized idempotent reservation is retrieved rather than rerun. Expired leases are reconciled when the sandbox process starts.

Do not use direct create/drop tools for live DDL; they are preview-only.

The sandbox profile also exposes `execute_sql` for safe, read-only inspection.
It remains statically validated and row-capped by the normal restricted SQL
path. `execute_tsql_unrestricted` is not part of the sandbox profile and must
not be used as a workaround.

Index usage counters are raw DMV values. A missing DMV row remains unavailable,
and `is_unused` is `null` when the best-effort counter epoch does not cover the
requested observation window. A zero counter is usable as unused evidence only
when the response reports covered usage context; it is not a database-wide
proof that an index is unnecessary.

## Sandbox view runbook

Prepare is read-only and is available under both `optimizer` and `sandbox`.
An optimizer preparation is preview-only and cannot be handed to a different
MCP process:

1. Call `prepare_view_change` with the schema, view, complete SELECT-shaped body, and operation.
2. Review legality, dependencies, exact prior definition fingerprint, apply preview, and exact rollback preview.

Apply only in the local `sandbox` process:

1. Confirm the target policy is non-production and sets `allow_view_apply=true`.
2. Set `AZURE_SQL_PERSIST_VIEW_SQL_STATE=true`. This explicitly permits the exact target and prior view definitions to be stored in the owner-only state database for crash recovery.
3. Call `prepare_view_change` again in this sandbox process with a stable idempotency key. Review the durable change id and raw-state disclosure.
4. Call `apply_prepared_view_change` with `reviewed_intent=true` and the same idempotency key.
5. Call `verify_view_change`.
6. Keep the candidate only after its consumer-query/workload validation passes.
7. Otherwise call `rollback_view_change`. The workflow restores the captured definition for an altered view or drops only a workflow-created view whose current definition still matches.

Apply/rollback precondition checks, DDL, and a private view-level ownership
marker run in one transaction. Preparation rejects a view already carrying
another suite marker. If the process stops during apply, restart it with the
same state directory and call `verify_view_change` for the same change id. A
matching target is reconciled without replaying DDL only when its durable
marker also matches. Ambiguous or externally applied state returns `hold` with
the original rollback contract retained. Do not prepare a replacement intent
against the changed view.

This path enables controlled view testing; it does not authorize a production view deployment.

## Plan review runbook

Start a review process that shares `AZURE_SQL_PERFORMANCE_STATE_DIR` with the optimizer or triage handoff:

```bash
export AZURE_SQL_PROFILE="enforcer-review"
export AZURE_SQL_ACCESS_MODE="restricted"
export AZURE_SQL_WRITE_POLICY="disabled"
export AZURE_SQL_TOOL_GROUPS="core,performance"
export AZURE_SQL_PERFORMANCE_STATE_DIR="$HOME/.azure-sql-mcp/state"
uv run azure-sql-mcp
```

Sequence:

1. `plan_health_review` or `review_plan_enforcement`.
2. Compare plans and parameter buckets.
3. Confirm whether Automatic Tuning owns the action.
4. Use preview-only `plan_enforcer_tick` if a bounded review cycle is useful.
5. `prepare_plan_action` with the shared session id, reviewed evidence window, reviewer, reason, exact operation, and unique idempotency key.
6. Stop at the prepared intent. Review does not authorize apply.

## Prepared apply runbook

This is an explicit per-action operation with database blast radius. Use a separate local stdio process only after authorization.

```bash
export AZURE_SQL_PROFILE="enforcer-apply"
export AZURE_SQL_TRANSPORT="stdio"
export AZURE_SQL_ACCESS_MODE="unrestricted"
export AZURE_SQL_WRITE_POLICY="apply"
export AZURE_SQL_TOOL_GROUPS="core,performance,admin"
export AZURE_SQL_DATABASE_POLICY_FILE="/absolute/path/to/local-database-policy.json"
export AZURE_SQL_PERFORMANCE_STATE_DIR="$HOME/.azure-sql-mcp/state"
export AZURE_SQL_PLAN_APPLY_KILL_SWITCH="false"
uv run azure-sql-mcp
```

Apply sequence:

1. Retrieve and review the prepared intent.
2. Call `apply_prepared_plan_action` once with the intent id and authorization reference.
3. Confirm the applied state matches the expected force/hint state.
4. Wait for a non-overlapping post-change Query Store window.
5. Collect the same parameter buckets used in the pre-change window.
6. Call `verify_plan_action`.
7. Accept `keep`, `hold`, or automatic exact rollback on regression.
8. Re-engage the kill switch by setting `AZURE_SQL_PLAN_APPLY_KILL_SWITCH=true` and restart/stop the apply process.

If verification evidence is insufficient, the decision is `hold`; it is not silently treated as success. `rollback_plan_action` is available for an explicitly authorized exact restore.

## Usage contexts

### Inline editor

Use only for static rewriting. No MCP call is needed. The optimizer must mark metrics unmeasured and preserve the recorded semantic contract.

### Copilot Chat

Use for one case with a small number of MCP calls. Keep the case/session ids in the conversation and ask for the complete leaderboard, rejected experiments, and stopping reason.

### Copilot agent/task

Use for triage and iterative tuning across several tool calls. One task does not authorize index DDL or plan apply; the selected server profile and explicit action authorization remain required.

### CI

CI runs unit tests, Ruff, Pyright, build, clean skill install/parity, Markdown-link checks, retired-path checks, and a location-only content secret scan. It does not connect to Azure SQL.

### Live validation

Run only against an allowlisted dedicated non-production Azure SQL database. Validate read-only workflows first with `optimizer`; use `sandbox` only for a leased index or reviewed view test. Do not run plan apply as a smoke test.

## State and audit operations

Default locations:

- performance contracts: `~/.azure-sql-mcp/state/performance.sqlite3`;
- admin audit: `~/.azure-sql-mcp/audit/`;
- prior and retired skill backups: `~/.azure-sql-mcp/backups/retired-skills/`.

These directories may contain private operational metadata. Keep them outside Git, owner-readable only, and under the workstation's normal backup/encryption policy.

Performance state omits raw SQL. Admin audit stores SQL hashes and previews by default; `AZURE_SQL_AUDIT_FULL_SQL=1` is a separate explicit opt-in.

## Troubleshooting

### Server does not appear in VS Code

Validate `.vscode/mcp.json`, confirm VS Code can resolve `uv`, verify the absolute package path, and inspect the MCP output panel. Reload the window after changes.

### Expected tool is absent

Check profile, tool group, access mode, transport, and remote-admin settings. A tool must pass every filter.

For `execute_tsql_unrestricted`, leave `AZURE_SQL_PROFILE` unset, select unrestricted access, include `admin` or `all`, and restart the process. Write policy `apply` and `dry_run=false` are execution gates; they do not make a hidden tool appear.

### General DBA execution outcome is unknown

Do not repeat the batch. Locate the audit id, inspect database state with a separate read-only query, and decide whether the intended change completed, partially completed, or rolled back. The server deliberately does not retry DBA batches after a timeout, cancellation, or post-submission connection failure.

### Authentication succeeds but a DMV fails

Run `check_capabilities`. Azure SQL tier and database permissions can make individual evidence sections unavailable. Preserve that section as an evidence gap.

### Benchmark is policy denied

Check the policy path, JSON version, exact database key, `allow_benchmark`, and `max_benchmark_executions`. Do not widen production policy to make a test pass.

### Candidate timed out

The candidate becomes `inconclusive`; the session continues. Narrow the query or use better parameter cases. Do not invent metrics from a partial run.

### Temporary index cleanup failed

Stop index experiments. Retain the lease id and rollback DDL. Restart the same approved sandbox profile after lease expiry to trigger recovery. If removal is still unconfirmed, reconcile through the normal database change process.

### Plan apply is denied

Check the prepared intent, shared state directory, exact prior state, Automatic Tuning ownership, local stdio transport, access/write policies, database policy, kill switch, and authorization reference. Direct mutation tools are not a fallback.

### Triage returns `partial`

Read the per-section availability and truncation fields. Fix the narrow permission/window/coverage gap and recollect the same case.

## Closeout checklist

- Final tuning session has a stopping reason.
- Every candidate has a terminal classification.
- Winner SQL and semantic assumptions are included in the handoff.
- Parameter buckets and untested gaps are explicit.
- Any temporary-index lease is `cleaned`.
- Plan action is verified, held with owner, or rolled back exactly.
- Kill switch is re-engaged.
- Every general DBA audit with an unknown outcome has been reconciled before another mutation.
- No credential, environment value, raw production SQL, host, database name, or result data was committed.
