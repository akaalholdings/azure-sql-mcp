# Operations guide

This guide covers local VS Code Copilot setup, the five enforced MCP profiles, and the separate unprofiled general DBA posture. Keep one MCP process per posture so the advertised tool surface and write authority are obvious.

## Before you start

You need:

- Python 3.12 or newer;
- `uv` on the PATH visible to VS Code;
- a local checkout of this repository;
- Azure SQL authentication supplied outside Git;
- a database principal with only the permissions required for the selected profile;
- a local database-policy file for repeated benchmarks, temporary indexes, or plan apply.

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
      "allow_plan_apply": false,
      "max_benchmark_executions": 80
    }
  }
}
```

Set its absolute path with `AZURE_SQL_DATABASE_POLICY_FILE`. Missing files, invalid schemas, unknown databases, and omitted dangerous permissions fail closed.

## Profile matrix

| Profile | Skill/context | Access | Write policy | Tool groups | Database policy |
| --- | --- | --- | --- | --- | --- |
| `triage` | `sql-health-triage` | restricted | disabled | core,performance | not required for passive evidence |
| `optimizer` | `sql-optimizer`, read-only | restricted | disabled | core,performance | benchmark permission required for measured runs |
| `sandbox` | `sql-optimizer`, temporary index | unrestricted, local stdio | apply | core,performance,admin | benchmark and test-index permission, non-production environment |
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

Azure's subscription-level [Block T-SQL CRUD](https://learn.microsoft.com/azure/azure-sql/database/block-crud-tsql) feature also blocks `CREATE DATABASE` and several `ALTER DATABASE` operations, so it is not compatible with this exact broad-DBA-except-drop posture.

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
2. `start_performance_case` with up to four parameter cases.
3. `start_tuning_session`.
4. `add_tuning_candidate` for one family at a time.
5. `benchmark_tuning_candidate` for three screening runs.
6. Continue after every non-winning result.
7. Re-run credible winners with five finalist runs.
8. `finalize_tuning_session` with a winner or a documented `no_change` stopping reason.

Measured candidates use exactly-once user-query samples. A full duplicate/order-aware comparison is proven only when both complete results fit the bound in one snapshot; otherwise equivalence is inconclusive.

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
- no unresolved lease already exists for the database.

Call only `benchmark_index_candidate`, supplying the same named parameter cases recorded on the performance case. It creates a namespaced disposable index, measures every bucket, performs one bounded snapshot comparison per bucket, and removes the index automatically. A slower index is classified and rejected without ending the session. Cleanup failure returns `cleanup_required` and blocks another test. Expired leases are reconciled when the sandbox process starts.

Do not use direct create/drop tools for live DDL; they are preview-only.

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

Run only against an allowlisted dedicated non-production Azure SQL database. Validate read-only workflows first with `optimizer`; use `sandbox` only for a leased index test. Do not run plan apply as a smoke test.

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
