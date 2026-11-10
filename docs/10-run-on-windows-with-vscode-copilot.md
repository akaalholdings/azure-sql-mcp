# Run the 3.0 release candidate on Windows with VS Code Copilot

This guide installs `azure-sql-mcp` 3.0.0rc1 and the four skills on a Windows
machine, connects them to a copy of a database, and tells you what to bring
back from a trial.

Status of this guide: the Python steps were run on macOS. The Windows commands
were not run by the author. Run the check in step 3 before you trust the
install.

## Rules for a trial

- Use a copy of the database. Do not point the sandbox profile at production.
  The sandbox profile creates and drops indexes and views, and it runs your
  queries many times.
- The copy holds real data. The model reads query text, plans, object names
  and parameter values. Use only an AI service that your organisation has
  approved for that data.
- Keep server names, database names and the policy file out of Git.
- The release candidate has run on one General Purpose database. It has not
  run on Hyperscale or in an elastic pool. Expect defects and report them.

## 1. What you need

- Windows with Python 3.12 or newer (`py -3.12 --version`).
- VS Code with GitHub Copilot in agent mode.
- Azure CLI, signed in as you (`az login`). The server uses that sign-in
  through `AZURE_SQL_AUTH_MODE=entra-default`.
- On the copy, your identity needs `db_owner`, or `db_ddladmin` with
  `VIEW DATABASE STATE` and read access. Query Store must be on.
- Network access from your machine to the database (firewall or private
  endpoint).

## 2. Install the server

In PowerShell, in a folder of your choice:

You need two files from the release: the wheel and
`requirements-3.0.0rc1.txt`. The requirements file holds the exact dependency
versions the release was tested with. Install those first, then the wheel
without its dependencies. A plain `pip install` of the wheel can pull newer
dependency versions that were not tested.

```powershell
py -3.12 -m venv C:\tools\azure-sql-mcp
C:\tools\azure-sql-mcp\Scripts\python.exe -m pip install --upgrade pip
C:\tools\azure-sql-mcp\Scripts\python.exe -m pip install -r .\requirements-3.0.0rc1.txt
C:\tools\azure-sql-mcp\Scripts\python.exe -m pip install --no-deps .\azure_sql_mcp-3.0.0rc1-py3-none-any.whl
```

Use Python 3.12: the release was tested on 3.12 only.

## 3. Check the install

```powershell
C:\tools\azure-sql-mcp\Scripts\python.exe -c "import azure_sql_mcp.server, importlib.metadata as m; print(m.version('azure-sql-mcp'), m.version('mcp'))"
```

Expected output: `3.0.0rc1 1.26.0`. An import error here means the
dependencies are not the tested ones: repeat step 2 in a new virtual
environment.

## 4. Install the skills

The release has `sql-skills-3.0.0rc1-installed.zip`. It holds the four skill
folders in their installed form. Unpack it into the Copilot skills folder. If
a folder of the same name is already there, rename the old one first.

```powershell
New-Item -ItemType Directory -Force "$env:USERPROFILE\.copilot\skills" | Out-Null
Expand-Archive .\sql-skills-3.0.0rc1-installed.zip -DestinationPath "$env:USERPROFILE\.copilot\skills"
Get-ChildItem "$env:USERPROFILE\.copilot\skills" -Directory | Select-Object Name
```

Expected folders: `sql_health_triage`, `sql_index_manager`, `sql_optimizer`
and `sql_plan_enforcer`.

With a checkout of [`akaalholdings/sql-skills`](https://github.com/akaalholdings/sql-skills) you can use its installer instead:

```powershell
py -3.12 skills\install_all.py --dest "$env:USERPROFILE\.copilot\skills"
py -3.12 skills\check_installed_parity.py --dest "$env:USERPROFILE\.copilot\skills"
```

The skill versions for this server are sql-optimizer 3.0.0, sql-health-triage
1.1.0, sql-plan-enforcer 1.1.0 and sql-index-manager 2.1.0. Reload the VS Code
window after the install.

## 5. Write the database policy file

Save this as `C:\tools\azure-sql-mcp\policy.json`. Replace `your-copy-db` with
the name of the copy. The environment label must name a non-production
environment.

```json
{
  "version": 1,
  "databases": {
    "your-copy-db": {
      "environment": "development",
      "allow_read": true,
      "allow_benchmark": true,
      "allow_test_indexes": true,
      "allow_offline_test_index": true,
      "allow_view_apply": true,
      "allow_plan_apply": false,
      "allow_index_history_write": false,
      "test_index_name_prefixes": ["IX_Testing_"],
      "test_index_maxdop": 4,
      "max_benchmark_executions": 500,
      "max_tuning_candidates": 10,
      "max_tuning_session_executions": 500,
      "max_tuning_session_minutes": 60
    }
  }
}
```

- `allow_offline_test_index` is needed for a test index on a table that has a
  clustered columnstore index. Azure SQL Database refuses an online build
  there. An offline build blocks writes to the table while it runs.
- `allow_view_apply` allows the indexed-view experiment. Set it to `false` if
  indexed views are not a design you can use.
- `test_index_maxdop` caps each test build. Set it for the size of the copy.

## 6. Connect VS Code

Create `.vscode\mcp.json` in the workspace you open in VS Code. Do not commit
it.

```json
{
  "servers": {
    "azure-sql": {
      "type": "stdio",
      "command": "C:\\tools\\azure-sql-mcp\\Scripts\\azure-sql-mcp.exe",
      "env": {
        "AZURE_SQL_SERVER": "your-server.database.windows.net",
        "AZURE_SQL_DEFAULT_DATABASE": "your-copy-db",
        "AZURE_SQL_ALLOWED_DATABASES": "your-copy-db",
        "AZURE_SQL_AUTH_MODE": "entra-default",
        "AZURE_SQL_PROFILE": "sandbox",
        "AZURE_SQL_ACCESS_MODE": "unrestricted",
        "AZURE_SQL_WRITE_POLICY": "apply",
        "AZURE_SQL_TOOL_GROUPS": "all",
        "AZURE_SQL_QUERY_TIMEOUT_SECONDS": "900",
        "AZURE_SQL_PERSIST_VIEW_SQL_STATE": "true",
        "AZURE_SQL_DATABASE_POLICY_FILE": "C:\\tools\\azure-sql-mcp\\policy.json",
        "AZURE_SQL_PERFORMANCE_STATE_DIR": "C:\\tools\\azure-sql-mcp\\state"
      }
    }
  }
}
```

Reload VS Code, start the server in the MCP list, and ask Copilot to run these
three tools in order:

1. `check_runtime_status`: `package_version` is `3.0.0rc1` and `profile` is
   `sandbox`.
2. `list_databases`: only the copy is listed.
3. `check_capabilities` for the copy: Query Store is on, and
   `local_tuning_policy` shows benchmarks and test indexes as allowed.

Stop if any of the three is not as expected.

## 7. Give it a query

Use one chat per query. A prompt that works:

```text
Use the sql-optimizer skill with the azure-sql MCP server, database your-copy-db.
Tune the query below. Measure it. Parameter cases: <name and value for each case>.
Time budget: 30 minutes. Finalize the tuning session before you answer, and
confirm that every test object is dropped.

<the query>
```

Give two or three parameter cases for each query: a common value, a rare value
and a value with no rows. One case hides parameter-sensitive plans.

## 8. After each query: check for leftovers

The answer must say that the session was finalized and that each held object
was dropped. Check it yourself with this read-only query on the copy:

```sql
SELECT OBJECT_SCHEMA_NAME(i.object_id) AS schema_name, OBJECT_NAME(i.object_id) AS object_name, i.name AS index_name
FROM sys.indexes AS i
WHERE i.name LIKE N'IX[_]Testing[_]%'
UNION ALL
SELECT SCHEMA_NAME(o.schema_id), o.name, NULL
FROM sys.objects AS o
WHERE o.name LIKE N'v[_]Testing[_]%';
```

It must return no rows. If a tool result has `cleanup_required: true`, run the
remediation DDL it gives, then start a new session. The next
`start_tuning_session` also drops leftovers that the server owns and reports
them in `cleanup_before_run`.

## 9. What to bring back

Bring back numbers and defects. Do not bring back query text, object names,
server names or data.

For each query, one row:

| Field | Example |
|---|---|
| Query label you invent | Q07 |
| Shape, in general words | star join, 6 tables, type 2 dimension, window function |
| Kind of fix | rewrite / index / columnstore / indexed view / no change / none found |
| CPU ms before and after, per case | 18,400 to 240 |
| Logical reads before and after, per case | 2.1M to 9,800 |
| Results compared | ordered match / multiset match / not compared, and why |
| Minutes used | 22 |
| Would you deploy it | yes / no, and why not |
| What blocked or misled the agent | one line |

The defect log. It holds no SQL text, no data values and no object names:

```powershell
C:\tools\azure-sql-mcp\Scripts\azure-sql-mcp-incidents.exe --state-dir C:\tools\azure-sql-mcp\state summary
C:\tools\azure-sql-mcp\Scripts\azure-sql-mcp-incidents.exe --state-dir C:\tools\azure-sql-mcp\state export --format markdown --output C:\tools\azure-sql-mcp\incidents.md
```

Do not add `--include-summaries`: those lines are free text written by the agent
and can name objects. Read the export before you take it off the machine.

## 10. Known limits in this release candidate

- A final statement with IF / ELSE branches is rated inconclusive when one
  branch does not use the test object. The agent compares that branch
  separately.
- A one-pass result comparison stops at 10,000 rows
  (`AZURE_SQL_COMPARISON_ROW_LIMIT` raises it).
- A test view cannot take a columnstore index or a second nonclustered index
  through the tool.
- `max_call_seconds` does not hold when a connection stalls.
- An indexed view cannot contain an outer join, a subquery, a window function,
  `UNION`, `DISTINCT`, `TOP`, `MIN` or `MAX`. The tool reports which rule a
  definition breaks. While a view exists, `TRUNCATE TABLE`, `ALTER COLUMN` and
  partition `SWITCH` on its base tables fail.

## Rollback

Stop the server in VS Code, delete `.vscode\mcp.json`, and delete
`C:\tools\azure-sql-mcp`. The skills are removed by deleting their four
folders under `%USERPROFILE%\.copilot\skills`. The server creates nothing in
the database except leased test objects, which step 8 lists.
