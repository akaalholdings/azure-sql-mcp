# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

## [Unreleased]

## [3.0.0rc2] - 2026-11-10

Second release candidate. Use it on a non-production copy only.

### Added

- Database policy field `allow_indexed_view_designs` (default true). When it is
  false, no indexed view is tested or proposed, and `check_capabilities`
  reports `indexed_view_experiment.designs_forbidden`.
- A nonclustered columnstore test index takes `order_columns` and
  `data_compression` `COLUMNSTORE_ARCHIVE`.
- `measurement_only` on `benchmark_tuning_candidate` and
  `benchmark_index_candidate`: a further run on a finalist. It can lower the
  verdict and cannot raise it.
- `selection_scope: "tested"` on `finalize_tuning_session`: selects a finalist
  whose results matched on every measured case. It is not a proof, and the
  result is not deployment-ready.

### Changed

- A candidate with IF / ELSE branches is judged for each case on the branch
  that ran. A case on the unchanged original branch is `unchanged_by_design`
  and no longer makes the candidate inconclusive.
- A result above the comparison row limit is compared in full by digest.
- A statement with TOP and no ORDER BY is compared as a multiset, as a tested
  match at most.

### Fixed

- A statement over a view whose definition starts with a comment was refused
  as a cyclic view dependency.

### Known limits

- `measurement_only` with held test objects is refused for a `plan_reuse` mode
  that has no baseline from before the objects were built.
- The view tool does not take `measurement_only`.
- `max_call_seconds` does not hold when a connection stalls.

## [3.0.0rc1] - 2026-11-10

Release candidate. It has run on one Azure SQL Database (General Purpose
serverless) in a benchmark of 38 tuning tasks. It has not run on Hyperscale
or in an elastic pool. Use it on a non-production copy only.

### Added

- `benchmark_indexed_view_candidate`: builds a leased test view with its unique
  clustered index, measures the statement that reads it, compares the results
  and drops the view. The result carries the write warning and the base-table
  restrictions (TRUNCATE TABLE, ALTER COLUMN and partition SWITCH fail while
  the view exists).
- `benchmark_index_candidate`: `candidate_sql` measures the original statement
  against a rewrite with its indexes in one call. It takes several parameter
  cases, `plan_reuse`, `runs`, `max_call_seconds`, `kind`
  (`nonclustered_columnstore`) and `partition_aligned` for each index.
- `keep_for_session` on both benchmark tools: test objects stay in place for
  later calls in the same tuning session. `finalize_tuning_session` drops them.
- Test indexes on a table with a clustered columnstore index: built offline when
  the database policy sets `allow_offline_test_index`, refused before any
  execution when it does not.
- Result fields: `time_spent`, `run_rule`, `build_retries`, `test_objects`,
  `ordered_rows_match`, `multiset_match`, `tested_match`, `caveats`,
  `decision.by_metric` and `decision.worst_case`.
- Tool guides `indexed_view_experiment`, `session_held_objects` and
  `measurement_time_rules`.

### Changed

- A test index call never fails because of its name. A prefix that is not in
  `test_index_name_prefixes` is replaced by the policy prefix, and
  `index_name_note` gives the name used. Generated names are readable:
  `<prefix><Table>_<KeyColumns>_<8 hex>`.
- Every benchmark compares the two result sets, as ordered rows and as
  multisets. A tested match is reported with its scope; it is not a proof.
- A baseline that takes one second or more runs once. A large gain ends the
  measurement early.
- A case at zero CPU no longer makes a candidate inconclusive.
- A failed test index build returns the failed step, the cause class and the
  redacted engine message.
- `finalize_tuning_session` can drop objects, so it is no longer marked
  read-only. A failed drop is retried on a new connection, and a leftover gives
  `cleanup_required` with the remediation DDL.
- Every benchmark call and `start_tuning_session` first drops leftover test
  objects that this server owns in the database, and refuses to run when a drop
  still fails.
- `check_capabilities` returns a summary by default (`detail="full"` for all).
- `review_workload_indexes` orders index keys by equality, ORDER BY, range, and
  leads with the query the caller named.
- The learning loop expects sql-optimizer 3.0.0, sql-health-triage 1.1.0,
  sql-plan-enforcer 1.1.0 and sql-index-manager 2.1.0.
- The `mcp` dependency is capped below version 2. The server does not start
  on `mcp` 2.x, which a plain install would otherwise select.

### Known limits

- A final statement with IF / ELSE branches is rated inconclusive when one
  branch does not use the test object. Compare that branch separately.
- A one-pass result comparison stops at 10,000 rows.
- A test view cannot take a columnstore index or a second nonclustered index
  through the tool.
- `max_call_seconds` does not hold when a connection stalls.

## [2.6.4] - 2026-11-10

Smaller tool responses and cheaper reads on the customer database, from the
2026-11-03 review. No evidence is removed: each trimmed response has an opt-in
that returns the old form.

### Changed

Responses that are smaller by default:

- `explain_query` no longer returns `summary.operators` or the cost-ranked
  `summary.top_operators`. `plan_digest` ranks operators by self time and was
  contradicted by that second list. `include_operator_summary=true` returns the
  operator list, with the cost-ranked list named
  `summary.top_operators_by_estimated_cost`.
- `get_active_sessions` returns the plan XML only with `include_plans=true`.
  Each request now returns `plan_handle` and `query_hash`. A blocking chain
  returns `head_blocker_session_id`, `head_blocker_has_active_request`,
  `blocked_session_ids` and `blocked_session_waits`; it no longer repeats the
  full row of each session (`head_blocker`, `blocked_sessions`). A head blocker
  with no active request is idle in an open transaction.
- `review_index_portfolio` and `get_index_review` return a compact review:
  state counts, the create, consolidate and drop candidates with their inert
  DDL and rollback, blockers by code, and the count of omitted subjects. The
  artifacts leave out `index-review.json`, and `validation.sql` covers the
  candidates only. `get_index_review` returns full subject entries with
  `subject_ids`, or a page with `subject_offset` and `subject_limit`.
  `include_full_review=true` returns the old response.
- `capture_index_review_snapshot` returns the run id, subject count,
  fingerprint and coverage. `include_snapshot=true` returns every stored
  subject.
- `analyze_query_indexes`, `analyze_workload_indexes` and `optimize_indexes`
  list a compact entry for the indexes of the tables they analysed, with
  `existing_indexes_total` and `existing_indexes_omitted`.
  `include_all_existing_indexes=true` returns the full metadata of every index.
- `collect_performance_evidence` and `tune_query` return the sections and the
  profile once. `evidence.metadata.sections` and
  `evidence.metadata.profile_summary` are no longer in the response;
  `get_performance_case` still returns the stored record in full.
- `get_object_index_diagnostics` reads the buffer pool only with
  `include_buffer_usage=true`. That read walks every buffer of the instance,
  also for one table.
- `get_lock_details`: `total_locks`, `waiting_locks` and
  `locks_by_resource_type` are now the true totals for the database. The new
  `returned_locks` is the number of rows in the response.

Tools and profiles:

- `review_workload_indexes` takes `query_ids`: those Query Store queries are
  analysed whatever their rank or table scope. Pass the query that is being
  tuned.
- `analyze_workload_indexes` is no longer in the optimizer and sandbox
  profiles, and the troubleshoot prompt calls `review_workload_indexes`. The
  tool stays available when no profile is set.
- The plan reference check of `review_workload_indexes` compares index names
  with a binary collation. A plan that names an index with different letter
  case no longer counts as a reference.

### Fixed

- `get_lock_details` reported the number of returned rows as the total, so a
  storm of 300,000 locks read as 200. It now names the object of KEY, PAGE, RID
  and HOBT locks; before, only OBJECT locks had a name.
- Index portfolio capture read Query Store evidence as one row per plan and
  runtime interval, each with the plan XML. About 12 hourly plans filled the
  10,000-row cap over 35 days, so coverage was always `capped` once retention
  allowed a full window. The read returns one row per plan, and the cap counts
  plans.
- `capture_index_review_snapshot` had the default tool timeout (query timeout
  plus 15 seconds) for all its reads and one insert per index. On a large
  database every attempt could time out and roll back. Snapshot rows are
  inserted in chunks of up to 83, and the tool has a longer timeout.
- `review_workload_indexes` with `table_names` did not always analyse the query
  that the optimizer was tuning; see `query_ids` above.

### Performance

- Every read sent `SET LOCK_TIMEOUT` as its own round trip. A plain SELECT or
  WITH read now sends it in the same batch, and a profiled run uses three round
  trips, not six. Gated writes and SHOWPLAN sessions are unchanged.
- A failed read closed its pooled connection, so each permission-denied
  optional read cost a new login. A plain read keeps the connection after an
  invalid object or column, a divide by zero or a permission error, and the
  pool checks it with `SELECT 1` before the next use.
- `analyze_db_health` runs its checks four at a time and reads the resource
  history and the governance row once.
- `review_workload_indexes` runs its independent reads together (at most four
  at a time) and reads the Query Store runtime aggregate once; before, the SQL
  expanded it about four times.
- `get_top_cached_queries` and `get_cached_routine_stats` take the top rows
  before they build plan XML.
- Plan summaries walk each operator once. An 801-operator plan took 1.4 s on
  the event loop and now takes 17 ms; the output is the same.
- The index benchmark and lease cleanup read the indexes of one table, not the
  whole catalog, and partitions no longer multiply the column rows of the
  index metadata read.
- `optimize_indexes` reads row count, column widths and write ratio once per
  table.

Measured on a 43 GB Azure SQL Database (General Purpose serverless, 4 vCores,
16 million posts, 112 million votes) against 2.6.3, same script:

- `review_index_portfolio` response: 3.6 MB to 3.3 KB.
  `capture_index_review_snapshot`: 2.7 MB to 755 bytes.
  `get_active_sessions` with 40 blocked sessions: 323 KB to 35 KB.
- `analyze_db_health`: 5.1 s and 28 round trips to 1.4 s and 12.
  `review_workload_indexes`: 11.5 s and 36 round trips to 5.9 s and 18.
  `get_object_index_diagnostics` for one table: 20.6 s to 2.7 s.
- Old and new SQL returned the same rows in all 23 comparisons. The scoped
  workload read used about 10 times less CPU, the plan reference check 3
  times less, the index evidence read about 5 times less.

### Known issues

- `get_top_cached_queries` showed no gain at 1,000 cached statements; the
  top-rows-first change is unproven at that size.
- The whole-database index metadata read costs slightly more on a database
  with no partitioned tables. Its gain is on partitioned tables and on the
  one-table reads.
- The multi-row snapshot insert was run live with 27 indexes only.

## [2.6.3] - 2026-11-06

Correctness fixes from the 2026-11-03 review, found and proven by live testing on
Azure SQL Database; the macOS OpenSSL fix; the plan-reader fixes.

### Added

- `scripts/azure_test_db.sh up|ip|status|down`: a disposable free-offer Azure
  SQL Database for live testing, with the env and policy files the MCP needs.
  It acts only on the resource group it creates (tagged
  `purpose=azure-sql-mcp-testing`).

### Fixed

macOS:

- The MCP could not connect to any database on a Mac where Homebrew's default
  `openssl` is OpenSSL 4: the ODBC driver bundled in mssql-python accepts only
  OpenSSL 1.1 or 3 and failed with "OpenSSL library could not be loaded". The
  server (`azure-sql-mcp`, `python -m azure_sql_mcp.server`) and
  `azure-sql-mcp-index-history` now re-execute once at startup with Homebrew's
  `openssl@3` first on `DYLD_LIBRARY_PATH`, and say `brew install openssl@3`
  when it is missing. The live acceptance script and the integration tests do
  the same.

Index advice and removal:

- `capture_index_review_snapshot` now stores runs. Before, every capture
  failed: the coverage field `missing_index_dmv` and the subject field
  `estimated_index_size_mb` were not allowed, and a DMV-only candidate got a
  snapshot id that failed the read-back check. So `review_index_portfolio`
  always returned `history_status=missing`.
- The index history tools return `result_status=precondition`, with the DBA
  install and GRANT steps, when the `dbatools` history tables or rights are
  missing. A history contract mismatch is still a tool error.
- `review_workload_indexes` no longer gives a high-confidence drop for a new
  index. Index age (from `STATS_DATE`, a lower bound) limits the evidence
  window: younger than 7 days gives `review_index`, younger than 35 days or
  unknown age caps the drop at medium. An index with no usage-stats row is no
  longer counted as zero reads.
- `review_workload_indexes` keeps every range predicate column in the designed
  index. A range column that is not the key goes to INCLUDE.
- Index removal now accounts for readable replicas. On Premium, Business
  Critical, Hyperscale or with a geo link (`sys.dm_geo_replication_link_status`),
  `review_index_portfolio` keeps an unused index at observe and
  `review_workload_indexes` caps the drop at medium, because replica reads are
  not visible on the primary. Capture reads these signals before its
  transaction opens.

Query Store and diagnosis:

- `detect_parameter_sniffing` names the fastest plan as `best_plan_id` and the
  slowest as `worst_plan_id`. Before, it used the lowest and highest plan id.
  It compares execution-weighted averages per plan over Regular runs. A plan
  with 10% or more aborted or exception runs never ranks best and ranks first
  as worst; a plan with only failed runs is reported as the worst plan. New
  fields: `*_aborted_executions`, `*_exception_executions`,
  `*_material_failures`.
- `compare_query_plans` returns one row per plan with execution-weighted
  averages and aborted and exception counts. Before, it returned one row per
  interval, each with its plan XML, and the comparison was empty. With no plan
  ids it compares the best and the worst plan by the same failure rule. A plan
  with no Regular average or with material failures gets no ratio.
- `get_query_wait_stats` includes each interval that ends inside the window.
  Before, it filtered on the interval start, so the default 60-minute window
  could drop the closed interval that held most of the last hour.
- `get_resource_stats_history` keeps the hourly peaks and counts the 5-minute
  samples above 80% and 95% on windows over 60 minutes. `diagnose_database`
  finds CPU, data I/O and log write pressure from these peaks, so a short
  saturation is no longer hidden by the hourly average.

Proof and benchmarks:

- Rewrite and index benchmarks resolve `compare_order` from the SQL when it is
  not set: row order is compared only when the query has an outer ORDER BY.
  Before, the default was true, so most index candidates for unordered queries
  failed with `equivalence_failed`.
- Same rows in a different order now give `order_only_mismatch`
  (inconclusive), not `equivalence_failed`.
- A rewrite that removes or changes the baseline outer ORDER BY gets the risk
  `order_contract_changed` and cannot be proven or marked `deployment_ready`.
- Equivalence preflight no longer reports `STRING_SPLIT`, a user table-valued
  function or XML `nodes()` shredding as a cross-database dependency. It reads
  the body of inline and multi-statement table-valued functions, also under
  CROSS and OUTER APPLY.
- Filtered index benchmarks match the filter text that SQL Server stores, for
  example `([status]=(1))` for `status = 1`. Before, the check failed after a
  real CREATE, the test index stayed, and the lease blocked every later index
  benchmark. Filters can use `N'...'` literals; CAST, CONVERT and
  scientific-notation constants are refused before CREATE.
- The index benchmark drops a temporary index when its ownership marker proves
  that the MCP created it, also after a lost CREATE reply or a failed
  post-create check. Expired-lease cleanup uses the same marker.

Plan actions, views and plans:

- `verify_plan_action` reads the post-apply Query Store evidence itself;
  `candidate_evidence` is optional and can never produce a keep. It holds when
  the forced plan ran half or fewer of the executions, when plan forcing failed
  after apply, or when max duration or spread gets worse against the forced
  plan's own baseline. Without parameter buckets it holds when the baseline ran
  more than one plan or when the forced plan did not run in the baseline.
- Prepared plan-action intents expire after 24 hours. Apply holds an expired
  intent.
- `rollback_plan_action` changes nothing for an intent that was never applied
  (held as expired or after a changed precondition). Before, it restored the
  prior state and so undid a later DBA change. It also holds, with
  `rollback_confirmation=state_changed`, when the current force or hint state is
  no longer the state the intent applied.
- ALTER of a view keeps the existing column list and WITH options
  (VIEW_METADATA, SCHEMABINDING). An encrypted view has no readable
  definition, so prepare refuses it and changes nothing. Before, a body-only change
  removed them and verify still reported success. An unset `schema_bound` keeps
  the existing SCHEMABINDING, and a durable retry with the same key no longer
  fails with an idempotency conflict.
- `analyze_query_plan` with `last_actual=true` keeps the requested statement
  when it cuts a cached batch to five statements, and marks its findings with
  `is_requested_statement`.

Plan reading (ported from sqlserver-query-plans 1.5.5 and 1.5.6, or found by
comparison with PerformanceMonitor; see NOTICE):

- Self CPU of a parallel row-mode operator took the busiest thread, so a scan
  whose four workers burned 400 ms reported 100 ms. CPU now sums across
  threads, and elapsed still takes the slowest thread. The "waited" note
  judges the slowest thread, which sets the operator's time: it waited when
  that thread's own CPU is far below its own elapsed and no thread that worked
  ran nearly as long. A skewed operator whose one busy thread did all the work
  is not called waited.
- A plan with parallel operators that ran serially (runtime
  DegreeOfParallelism 0 or 1) says so, and names the usual causes: a worker
  thread shortage, or a workload group or resource governor MAX_DOP. On the
  Azure SQL free offer the workload group caps MAX_DOP at 1.
- Under a row goal, the "may stop early" note replaced the "reads far more rows
  than it returns" flag. A seek that read 1.37 billion rows to return 5,718 was
  not flagged.
  - The flag now always shows.
  - The note uses `ActualEndOfScans` to say how many executions stopped early,
    or that every execution read to the end.
  - The node drill-down reports `end_of_scans`.
- Cursor statements (`StmtCursor`) got no digest and no findings. Each cursor
  operation's plan is now analyzed. Predicate findings stay on the correct
  statement in a batch that mixes cursors and other statements. Statements
  name their `cursor_operation` (PopulateQuery, FetchQuery) and
  `cursor_actual_type`, and one cursor's operations share its cost once in the
  workload attribution. A fast-forward cursor that blocks parallelism gets
  cursor-specific advice, and plans below the default cost threshold for
  parallelism (5) get no `non_parallel_plan_reason` finding.
- A constant `LIKE` pattern that starts with `%` or `_` was an ordinary
  residual. Plan rules now report it as a non-SARGable predicate
  (`leading_wildcard`) with its own advice: prefix search, an indexed
  `REVERSE()` computed column, or full-text search. The index review routes it
  to `sql-optimizer`.
  - On a scan it is high: every row is read and tested.
  - As a residual on a seek of another column it is low: the seek limits the
    rows. A seek that reads most of the table is graded like a scan, and a
    lookup names the feeding index and suggests adding the column as INCLUDE.
  - A leading wildcard under a function or an implicit conversion is named as
    the blocker. `NOT LIKE` gets no wildcard advice.
- `review_workload_indexes` no longer says "the stored plans did not use" an
  index that the plans seek. A seek counts as passing the index over only when
  it left the next key after its equality prefix as a residual.

Runtime:

- A tool timeout or cancel stops the remaining statements of a multi-statement
  session. Each statement gets the smaller of the query timeout and the time
  left for the tool. The statement that is already running still runs until
  its own timeout, because the mssql-python driver has no cancel call.
  `benchmark_index_candidate` keeps the full query timeout, so it can drop its
  index after a timeout.
- The server instructions name the required `report_stuck` argument
  `blocker_kind`.
- A cancel during the index benchmark's ownership check no longer skips the
  durable settlement: the executions already run are counted and the execution
  reservation is settled before the cancel is raised again.

Found by live acceptance on Azure SQL Database (most were also in 2.6.2):

- Index history capture stored no run at all. The exactly-once transaction
  nested `BEGIN TRANSACTION` inside the driver's own transaction, so commit
  ended only the inner level and the run rolled back. The contract check also
  compared CHECK constraints with the installer text, not the form SQL Server
  stores, and snapshot row ids digested 100 ns timestamp text that reads back
  as microseconds. A fresh install now captures, reads back and replays.
- `sys.objects.type` is `char(2)` (`'U '`): user-table indexes got the blocker
  `parent_is_not_user_table`, so widen, extend and consolidate advice had no
  rollback DDL, and indexed views were missed.
- Estimated plans for typed parameters (`explain_query`, `analyze_query_indexes`)
  never compiled the statement: SHOWPLAN_XML returns nothing for parameter
  markers and only an EXECUTE PROC stub for `sp_executesql`. They now compile
  the statement with typed local variables and `OPTIMIZE FOR` the values, and
  `estimated_plan_basis` says what that does not model (PSP and
  optional-parameter dispatcher plans, parameter embedding). A plan with no
  compiled query plan is refused, never read as zero hints.
- A plan that only maintains an index (INSERT, UPDATE, DELETE, or an internal
  REBUILD or UPDATE STATISTICS) no longer counts as use in
  `review_index_portfolio` and `review_workload_indexes`, so an index on a
  table that takes inserts can become a removal candidate. A forced plan that
  touches the index still protects it.
- A schema- or table-scoped `review_workload_indexes` ranks only the scope's
  queries before the `top_queries` cap and checks only the scope's indexes for
  plan references. Before, a busy database pushed the scope's own queries out
  of the cap and the reference read timed out.
- `get_top_queries`, `get_query_store_trend`, the query history tools and
  `get_query_store_regressions` add `aborted_executions`,
  `exception_executions` and a Regular-only average beside the existing
  averages, which still include failed runs.
- `compare_query_plans` keeps the caller's a/b order, names
  `baseline_plan_id` and `compared_plan_id`, and refuses a single plan id.
- `order_contract_changed` no longer fires when the same ORDER BY key is
  written another way (unqualified, another alias, an ordinal or a select-list
  alias).
- Benchmark `actual_rows` was 0 when a parallel-shaped plan ran serially; an
  actual plan of a constant-only SELECT now gets an accurate error; a digest
  of an actual plan that ran in under 1 ms says why no operator is ranked.
- View rollback restores the original module text, not a rebuilt one, and
  reports `definition_exact` and `text_drift` on every path, including a retry.
- An index benchmark refused before dispatch (filter, table name, parameter
  binding, execution budget) no longer binds the candidate, so the corrected
  request with a new key runs.
- A failure after a plan or view write was sent records `unknown` or raises
  an outcome-unknown error, so a retry cannot force twice or undo another
  change. A verify that decides rollback returns the regressed metrics.
- MCP refusal messages no longer quote examples or statuses, which the error
  sanitizer redacted.

### Changed

- `failure_diagnostic.diagnostic_code` now says what happened when no SQL error
  caused the failure: `request_refused`, `policy_refused`,
  `verification_failed` (a write was sent, then its outcome could not be
  verified or recorded: read state first, do not resubmit), `lease_conflict`
  or `internal_error`. Before, all of these were `sql_execution_failed`, which
  now means a SQL or driver error only. See docs/09-operations.md.

### Known issues

- A durable view change prepared by 2.6.2 (`persist_view_sql_state`) for a view
  with a column list or WITH options does not load in this version: verify and
  rollback of that change id fail with "durable view marker identity is
  invalid". Restore the stored prior definition by hand. This applies only to
  sandbox profiles with persisted view state.
- An unscoped `review_workload_indexes` on a database with many unused indexes
  and large plans can still time out on the plan reference read (the unused
  indexes then stay low confidence). Scoped reviews are fixed; the general fix
  is planned for 2.6.3b.
- Query Store plan references match an index by name. After a table is dropped
  and recreated with the same index names, old plans can still count as
  references until they age out.
- Not verified live: readable-replica holds (the test tier has no replicas)
  and the index-age gate alone (a serverless resume resets usage counters, so
  the counters-recent gate fires first). Both are covered by unit tests.

## [2.6.2] - 2026-10-06

Fixes from the first live incident log (Windows, Python 3.14, Copilot CLI).

### Fixed

- `prepare_view_change` failed on every view with "Invalid column name". Its
  dependency read joined on `referenced_schema_id`, which
  `sys.sql_expression_dependencies` does not have. The schema is now resolved
  through `referenced_id`, and the catalog view is in the DMV column contract
  test.
- `explain_query` could fail with "No SHOWPLAN XML was returned" although the
  driver returned a plan. Plans are now recognized as text, as UTF-8 or
  UTF-16 bytes, with a byte-order mark or an XML prolog, hex-encoded, or
  split across rows. `review_workload_indexes` plan reads and the index
  optimizer use the same recognizer.
- When no plan is found, the error now says what came back by kind only, for
  example `result sets: one; values seen: null`. It never includes a value.
- A connection running a long query was reported as leaked after 5 minutes.
  The leak threshold now allows for the configured query timeout, and the
  incident log does not report a leak while a tool call is running.
- Incident log: a statement that ran for the whole query timeout (SQLSTATE
  HYT00, "Query timeout expired") is filed as `timeout`, not `transient`. A
  login timeout is still `transient`.

## [2.6.1] - 2026-10-02

### Changed

- Index removal (`review_index_portfolio`): the observation window is now 35
  days, was 90. `business_cycle_extension_days` still lengthens it per
  database, for example +60 for quarter-end jobs. The gate reads only the
  trailing window:
  - It sums reads across counter resets, per counter epoch.
  - Captures with usable counters must be at most 48 h apart and cover at
    least 95% of the window. One missed daily capture is tolerated.
  - A counter decrease with no engine-start change is a reset of unknown
    cause, and the index stays in `observe`. A read proven anywhere in the
    window keeps the index.
  - A Query Store retention shorter than the window is reported as a gap.
  - `mcp_contract.index_review_min_observation_days` is now 35.
- `review_workload_indexes`: a high-confidence unused-index drop needs at
  least 35 days of usage counters, was 30, and a Query Store plan-reference
  check over the same span.
- `analyze_db_health` resource checks judge the full hour of 15-second samples
  at p95 (warning at 80%, critical at 95%), not the last 3 minutes. Memory
  near 100% is information, as Microsoft documents for Azure SQL Database:
  - the `sustained_avg_memory_usage_percent` warning is replaced by an
    `informational` entry;
  - `details.governance_limits` drops `max_db_memory`,
    `checkpoint_rate_mbps` and `max_workers_per_query`. Documented limits
    replace them.
- `collect_performance_evidence`: the `regressions` section now holds only
  the automatic-tuning recommendations that bear on the case. These are live
  `Active` rows, scoped to the case's Query Store query when it is known.
  Engine-owned rows (`Verifying`, `Success`, `Reverted`, `Expired`) never
  make a case actionable.
- `review_plan_enforcement` ranks force actions only for recommendations
  marked "apply manually" (`AutomaticTuningOptionNotEnabled`). It no longer
  ranks unforce for plans that automatic tuning forced.
- Index creates, extends and widens carry a `prerequisites` list. An index
  with a computed key or INCLUDE column states the SET options every writer
  needs, or its writes fail with Msg 1934.

### Fixed

- Index advice no longer gives destructive DDL:
  - It never consolidates a covering index into a primary key or unique
    constraint. It never emits two rebuilds of one survivor that undo each
    other.
  - Foreign-key protection is read one row per index and fails closed when
    incomplete. The last unfiltered index supporting a foreign key is never
    a drop candidate.
  - A filtered index is never extended for queries outside its filter.
  - New keys stay within 1,700 bytes.
  - Compressed and partitioned rebuilds are one online statement that keeps
    per-partition compression.
- An index named by an index hint or a forced Query Store plan is never given
  executable DROP DDL. Hints match case-insensitively; a name on several
  tables pins every candidate; INDEX(0) and INDEX(1) are ignored. The
  2,000-row hint scans count only rows that contain hints.
- The Query Store window that actually exists is reported, and writes are
  priced over it. Before, a longer lookback made indexes look cheaper.
- `detect_regressed_queries` always returned empty. It read
  `$.queryId` instead of `$.planForceDetails.queryId`.
- Plan reading:
  - Booleans are read in every form (`1` and `true`).
  - `GrantWaitTime` is converted from seconds.
  - Key lookups are judged on actual executions.
  - Scalar UDFs are found in filters and scans.
  - Dynamic seeks are no longer reported as "cannot seek".
- `analyze_db_health` read DMV columns that do not exist on Azure SQL
  Database. The connection check always failed, and the replication check
  passed when its read failed. An unreadable source is now a warning, never
  a pass.
- `get_resource_limits` returned no log rate cap. It now reads
  `primary_max_log_rate`.
- Permission messages for tier-gated DMVs name the fix for Basic, S0, S1
  and elastic-pool databases.
- Computed columns can again be index keys or includes when they are
  indexable and deterministic.
- Lock hints written as quoted identifiers (`[UPDLOCK]`, `"TABLOCK"`) are
  rejected by the read-only validator.

### Added

- The 2026-10-02 validator attack corpus (1,422 inputs, plus 107 expanded
  forms) is pinned as a regression test.
- A DMV column contract test and `scripts/capture_dmv_columns.py`, which
  the owner runs per Azure tier to check documented columns.
- `get_resource_limits`: pool, instance and effective log-rate caps in MB/s.
  `get_resource_stats_history`: `p95` and minutes above 80% and 95% per
  metric.

### Upgrade notes

- Review ids issued by 2.6.0 `review_index_portfolio` no longer resolve. Run
  the review again.
- A second portfolio capture on the upgrade day is rejected as an idempotency
  conflict, because its windows differ. Capture again the next UTC day.

## [2.6.0] - 2026-10-02

### Added

- Local incident log, on by default. Every tool call goes through one hook that
  records tool errors, timeouts, cancels (`client` only for the client's own
  `notifications/cancelled`; `shutdown` or `unknown` otherwise), `unavailable`
  results, slow calls (`AZURE_SQL_INCIDENT_SLOW_SECONDS`, default 60, recorded
  while still running and again with the final duration and outcome), and
  stuck calls (past the tool's own timeout plus 30 seconds), with the package
  frames the call waits in. It also records calls a crashed or killed process
  never finished, agents that repeat the same call with the same error or
  `unavailable`/`precondition`/`not_supported` status three times (transient
  errors excepted) or with the same `ok`/`empty` result ten times (live-state
  tools excepted), errors the server catches inside an `ok` or `unavailable`
  result (optional sections, source gaps, diagnosis sources, learning links,
  evidence collectors, benchmarks, snapshot comparisons, health checks,
  parameter binding and metadata lookups), circuit-breaker opens, connections
  held over 5 minutes (found by the watchdog), unknown tool names, and startup
  or server-exit failures.
  Records are owner-only daily JSON Lines files under
  `<AZURE_SQL_PERFORMANCE_STATE_DIR>/incidents/`, kept 30 days and capped at
  5 MB a day (P3 and P4 records stop at 80%, so a later P1 or P2 still fits)
  and 20 MB in total. They hold no SQL, literals, argument values, result rows,
  server, database or host names, paths, or tokens. On SIGTERM the server marks
  calls still open as a shutdown, then exits as before.
- `report_stuck`: an agent records that it is blocked, once, with its skill,
  skill version, last tool, related ids the server issued, and a redacted
  one-sentence summary, and gets an `incident_id` to give the user (a repeat
  gets the first report's id). A report the log could not write returns the
  reason and is never coalesced.
- `export_incident_backlog`: titles, counts, priorities, and fingerprints of
  the grouped backlog; `include_details=true` adds redacted examples and agent
  summaries. Both incident tools are local stdio only, in every profile, and
  never touch the database.
- `azure-sql-mcp-incidents export|summary`: the full Markdown or JSON backlog,
  grouped by fingerprint with counts, sessions, versions, priority, a suggested
  title, and a next step. Agent summaries need `export --include-summaries`. It adds stalled durable work (leases in
  `cleanup_required`, intents left mid-apply, abandoned sessions and cases)
  read through a read-only SQLite connection.
- `check_runtime_status.incident_log`: enabled, reason when off, retention,
  slow threshold, records written by this process, and `capped_day` once the
  daily cap was hit.
- Settings `AZURE_SQL_INCIDENT_LOG` (`on`/`off`), `AZURE_SQL_INCIDENT_DIR`,
  `AZURE_SQL_INCIDENT_RETENTION_DAYS`, `AZURE_SQL_INCIDENT_SLOW_SECONDS`, and the
  matching `--azure-sql-incident-*` flags. A `:memory:` state directory turns
  the log off unless `AZURE_SQL_INCIDENT_DIR` is set. An invalid value never
  stops the server: it turns the log off (reason `invalid_config`) and logs a
  warning that names the variable.
- Server instructions: "When you are blocked".
- `tool_error` payloads, including `compare_schemas` and
  `generate_migration_script`, carry `failure_diagnostic.transient: true` when
  the server's retry policy treats the failure as transient (for example 40613).

### Changed

- The two new tools change `tool_schema_fingerprint`, so learning recall scopes
  rotate as in any release that adds tools. `mcp_contract.contract_version` is
  `2.6.0`.
- The incident settings are deliberately left out of
  `sanitized_config_fingerprint` (unlike log level and format): turning the log
  on or off must not rotate learning scopes.

## [2.5.1] - 2026-10-02

### Security

- The read-only validator admitted a write hidden after a SELECT with no
  semicolon, for example `SELECT 1 DELETE FROM dbo.t`: the parser read
  `DELETE` as a column alias, while SQL Server runs it as a second statement
  and commits it on the autocommit connection. This affected `execute_sql`,
  `explain_query(analyze=true)` and the benchmark tools. Any T-SQL reserved
  statement keyword in code is now rejected, whatever the parser makes of it.
- T-SQL is now lexed in one pass, the way SQL Server reads it (nested block
  comments, strings, bracketed and double-quoted identifiers), so `/*` inside a
  string or a line comment can no longer hide code. Unterminated strings,
  identifiers and comments are rejected.

### Changed

- Locking hints are rejected in any position of the hint list and in the
  legacy form without `WITH`. `TABLOCK`, `HOLDLOCK`, `SERIALIZABLE`,
  `REPEATABLEREAD` and `READCOMMITTEDLOCK` are now rejected too: under RCSI they
  make a read block writers. `NOLOCK` is still admitted. Index analysis skips
  workload queries that use these hints, as it already did for `UPDLOCK`.
- `NEXT VALUE FOR` (advances a sequence) and `RAND(seed)` (reseeds the
  connection's generator, which outlives the call on pooled connections) are
  rejected. Unseeded `RAND()` is still admitted.

## [2.5.0] - 2026-10-01

### Added

- Plan digest on `analyze_query_plan`, and a compact `plan_digest` on
  `explain_query`: plan kind and what it can show, statement time and scalar
  UDF share, warnings by node, memory grant use, parameter and local-variable
  tells, top operators by self time (estimated self cost on estimated plans),
  per-execution estimate errors with fixed-guess fingerprints, repeated table
  access, thread skew, plan waits, eager index spools, missing-index hints
  labelled as hints, predicates on cited nodes, and an operator tree capped at
  80 nodes.
- `node_id` (with `statement_index`) on `analyze_query_plan`: one operator in
  full, with estimates, actuals, and per-thread counters.
- Plan sources on `analyze_query_plan`: `last_actual=true` reads the last
  actual plan from the plan cache (`sys.dm_exec_query_plan_stats`; returns
  `precondition` with the exact setting when `LAST_QUERY_PLAN_STATS` is off),
  and `session_id` reads a running request's in-flight plan
  (`sys.dm_exec_query_statistics_xml`). Query Store plans now carry measured
  runtime and wait categories, with the root estimate checked against the rows
  Query Store measured.
- `review_workload_indexes` designs the index an eager index spool was building
  (reason `replace_eager_spool`): the spool suppresses the missing-index request.
- Plan rules `scalar_udf_time`, `stale_statistics`, and `exchange_spill`.
- `mcp_contract.plan_digest: 1`, and plan-reading guidance in the server
  instructions.
- Self-time attribution adapted from Erik Darling's `sqlserver-query-plans`
  plugin (MIT); see `NOTICE`.

### Changed

- On actual plans with operator times, findings carry `self_elapsed_ms` and
  `elapsed_share`, and ranking and access-rule severity use the elapsed share
  instead of estimated cost share. Access findings now cite their `node_id`.
- `row_estimate_gap` compares rows per execution and treats a parallel operator
  that each worker ran once as one execution. Its evidence now has
  `estimated_rows_per_execution`, `actual_rows_per_execution`, `executions`,
  `estimated_executions`, `direction`, and `factor` (`estimated_rows` removed).
- `no_join_predicate` is graded from the join's inputs (`verdict`: correlated,
  implied_predicate, no_multiplication, not_multiplied, multiplied); an
  unverifiable warning on an estimated plan is `medium` (was `high`).
- `eager_index_spool` names the spool's table, key columns, and includes.
- `explain_query` ranks `top_operators` by `estimated_self_cost`.
- Plan parsing refuses input over 64 MB, DTDs and entities, and plans nested
  deeper than 1,000 operators; plan-derived text is flattened to one line.
- `sql-optimizer` learning skill version 2.5.0.

### Fixed

- The inner side of a nested loop was reported as misestimated when its outer
  input was the misestimate.
- `scalar_udf` fired on a Compute Scalar above an operator that held the UDF.
- `explain_query`'s `top_operators` always started with the root operator.

## [2.4.0] - 2026-10-01

### Added

- `review_workload_indexes`: workload-driven index design from Query Store
  runtime history and stored plans. It extracts per-table access patterns,
  designs keys and includes, reconciles candidates with existing indexes
  (covered, extend, widen, create), reviews existing indexes (duplicates,
  left-prefix redundancy, unused with counter-age and Query Store reference
  gates, heaps, disabled, unindexed foreign keys), and returns inert DDL with
  exact rollback, confidence, and a validation path. No install step.
- `result_status` (`ok`, `empty`, `unavailable`, `not_supported`,
  `precondition`) and, where relevant, `remediation` on every tool result.
- `analyze_query_plan` and `plan_findings` on `explain_query`: about twenty
  rule-based anti-pattern checks over estimated and actual plans.
- `get_query_store_trend`, `get_query_store_regressions`, and `as_of_utc` on
  `get_top_queries`.
- `sample_seconds` interval mode for `get_wait_stats`; 14-day hourly resource
  history from `sys.resource_stats` when `master` is allowlisted.
- `diagnose_database`: one-call triage that reads resource use, waits,
  blocking, top Query Store consumers, regressions, and version store health
  concurrently and returns ranked findings with the next tool calls.
- `get_version_store_stats`: ADR persistent version store size, cleanup state,
  and the transactions holding cleanup back.
- Azure SQL Database server instructions in the MCP initialize response.
- Portable tool input schemas for strict function-calling clients
  (`AZURE_SQL_SCHEMA_PROFILE`, default `portable`).
- `scripts/azure_live_acceptance.py` for an end-to-end run on a disposable
  database.

### Fixed

- `get_deadlock_history` queried a server-scoped `system_health` session that
  Azure SQL Database does not have, used an XPath that skipped the ring-buffer
  root, and reported zero deadlocks on any failure. It now reads
  database-scoped XE ring buffers and master deadlock telemetry and returns
  `precondition` with capture DDL when nothing can capture deadlocks.
- Reverse index DDL rejected the real `ROWS_FILEGROUP` data space type, so
  drop and rollback DDL was withheld for every non-partitioned index on a live
  database.
- `get_resource_limits` read resource governance without a database filter and
  could report a sibling database in an elastic pool.

### Changed

- Package and public MCP contract `2.4.0`; the index history-table contract
  stays `2.3.0`.
- Learning recall is pinned to `sql-index-manager@2.0.0` and
  `sql-optimizer@2.4.0`.
- The `index-review` profile adds `review_workload_indexes`,
  `get_query_store_trend`, `get_top_queries`, and `check_statistics_health`.

## [2.3.1]

### Added

- Staged the `2.3.0` recommend-only index portfolio contract with deterministic
  capture/review/retrieval tools, a manual two-table `dbatools` history
  installer, redacted subject snapshots, and the daily capture CLI. It remains
  inactive until the database contract, policy, runtime restart, fingerprints,
  and non-production smoke test are separately approved and verified.
- Added exactly-once transaction capture with idempotency range locking,
  commit-outcome reconciliation, fail-closed contract probing, and seven inert
  review artifacts.
- Registered `sql-index-manager@1.0.1` and learning subject `index` for
  scoped advisory recall only. V1 portfolio selectors are not learning
  evidence references, and no decision or outcome is written until a future
  MCP-owned evidence and terminal-link bridge is explicitly added.
- Performance cases and evidence collection accept an exact positive Query
  Store `query_id`; explicit identities use the by-ID path and never fall back
  to text or fuzzy matching.
- Parameter-case tools publish a typed `name`/`values`/`types`/`weight` input
  schema plus value-free fingerprints, templates, and exact v1 matching rules.
- Runtime and session responses expose effective timeout budgets, derived
  deadline availability, and definitive new-work/finalization flags.
- Workload index metadata includes best-effort DMV counter-epoch coverage, and
  Showplan index analysis states the meaning and provenance of zero hints.

### Changed

- Released package `2.3.1` while retaining public MCP contract `2.3.0` and
  history schema `index-history-v1`. Index history now uses the current runtime
  identity's existing effective permissions: review requires `SELECT`, capture
  requires `SELECT` and `INSERT`, and broader permissions are diagnostic rather
  than a contract rejection. The installer no longer manages a dedicated
  principal, roles, or permissions.
- The sandbox profile exposes only the statically validated, row-capped
  `execute_sql` ad-hoc path, still gated by database-policy `allow_read`.
- Parameter-case inputs require the exact `name`/`values`/`types`/`weight`
  shape and reject unknown keys; Query Store ids reject boolean coercion.

### Fixed

- Parameterized plan, benchmark, index-analysis, and snapshot-comparison paths
  bind only the two `sp_executesql` control arguments as Unicode. User
  parameters retain driver inference and are not widened.
- Known safe SQL Server diagnostics now survive redaction as structured,
  durable failure details without exposing arbitrary SQL, identifiers, values,
  hosts, or secrets.
- Table statistics aggregate allocation units per partition with the documented
  HoBT/partition mapping, count base rows once, and separate base from
  nonclustered-index storage and counts.
- Missing index-usage DMV rows remain unavailable, and zero reads produce
  `is_unused=true` only when the requested observation window is covered.
- Durable Query Store evidence drops observed compiled/runtime parameter
  literals while retaining value-free counts and metadata.
- Session availability accounts for exhausted budgets and executable
  candidates, and missing-index analysis distinguishes filtered hints, failed
  plans, and an empty evidence window.

## [2.2.1] - 2026-07-28

### Changed

- Benchmark classification now uses conservative observed-range separation
  instead of rejecting fast candidates solely because their independent
  range-to-median ratio is high. Existing spread and noise telemetry remains
  available, with an additive comparison margin explaining the decision.
- Finalization stopping reasons now accept up to 2,000 characters and publish
  the same limit in the MCP input schema.
- Schema and catalog tools and resources now require database-policy
  `allow_read=true`; database-free runtime and policy discovery remain
  available.

### Fixed

- Literal outer `TOP (0)` result comparisons now restore the zero-row
  shape-and-type probe without weakening proof gates for other row limits.
- Tuning-session retrieval now exposes committed index evidence even when a
  crash prevented it from being attached to the candidate, while keeping such
  evidence out of winner selection.
- Index benchmarks now reject terminal sessions before cleanup, request
  binding, catalog access, DDL, or query dispatch.
- State-machine errors keep safe session and candidate enum values visible
  without weakening SQL-literal or secret redaction.

## [2.2.0] - 2026-07-28

### Added

- Added database-aware `check_equivalence_preflight` analysis with recursive view inspection to depth eight, function verdicts, dependency coverage, and fail-closed handling for encrypted, inaccessible, unresolved, cyclic, or depth-exceeded definitions.
- Published input enums for the four tuning objectives, candidate strategies, benchmark phases, and finalist selection scopes.
- Added typed, additive output schemas and stable `headline` summaries for performance-case classification, session budgets, benchmark changes, and plan counts without removing existing response keys.
- Added Windows Python 3.12 to CI alongside Ubuntu Python 3.12 and 3.13.

### Changed

- Added the terminal `performance_only` candidate state. Finalist performance measurements can complete when semantic comparison is impossible, but the state never represents semantic proof or deployment approval.
- `finalize_tuning_session` now accepts `selection_scope`, defaults to `proven`, and requires explicit `performance_only` opt-in before selecting an unproven finalist.
- Restored `combined` for multi-family rewrites and added `rewrite_plus_index` for lineage-backed index children. Existing lineage-backed `combined` records remain readable as a deprecated compatibility form.
- Runtime status now reports the configured `tool_groups`; MCP tool discovery remains in the protocol-standard `tools/list` `result.tools` location.

### Fixed

- Evidence persistence now normalizes UUID values to canonical strings, date and time values to ISO-8601 strings, and `Decimal` values to precision-preserving strings while continuing to reject unknown object types.
- Initial evidence collection and idempotent replay now return the same normalized persisted sections, evidence id, and content.
- Unsupported snapshot proof no longer ends a promising candidate before finalist performance measurement. Complete, nonzero finalist evidence is required before a candidate can become `performance_only`.
- Volatile functions hidden in referenced views now participate in the same preflight used by performance cases, result comparisons, and rewrite and index benchmarks.
- Invalid tool arguments now return a sanitized `invalid_arguments` envelope without echoing caller input or exposing Pydantic internals.
- Test paths now use pytest-managed temporary directories instead of fixed `/tmp` locations.

## [2.1.0] - 2026-07-24

### Added

- Published MCP protocol capabilities for durable performance cases, iterative tuning sessions, arbitrary plan comparison, leased sandbox index experiments, and restart-safe sandbox view changes.
- Added local policy ceilings for multi-hour tuning campaigns and adaptive workflow timeouts sized to the permitted per-request execution count.
- Added exact query identity, Azure SQL capability detection, durable idempotency fencing, and prepared Query Store mutation workflows.

### Fixed

- Candidate benchmarking now executes each measured sample once, uses snapshot-consistent duplicate-aware finalist comparison, and continues after losing experiments.
- Temporary-index and view workflows now preserve exact ownership, cleanup, rollback, and interrupted-operation recovery state.
- Triage and plan metrics no longer infer healthy state or query totals from incomplete or operator-level evidence.

### Changed

- `execute_tsql_unrestricted` now supports general DBA T-SQL instead of limiting raw execution to read-only statements. Direct and statically recoverable `DROP DATABASE` commands remain blocked.
- Applied DBA batches use a single non-retried submission on an isolated connection, drain all result sets, and discard the connection after every outcome.
- Operator guidance now documents the exact unprofiled DBA startup posture and distinguishes Azure control-plane delete protection from T-SQL permissions.

### Security

- The `DROP DATABASE` guard is documented as defense in depth: runtime-assembled SQL and behavior hidden behind existing modules cannot be proven by static inspection. Database permissions, Azure RBAC/resource locks, and alternate client access remain independent controls.
- DBA audit previews, rollback text, and error details redact SQL literals by default; uncertain post-submission failures are recorded as outcome unknown.

## [2.0.0] - 2026-07-15

### Added

- Versioned performance-case, evidence, tuning-session, tuning-candidate, and plan-action contracts with permission-restricted SQLite persistence.
- Named `triage`, `optimizer`, `sandbox`, `enforcer-review`, and `enforcer-apply` profiles plus a fail-closed local database policy.
- Iterative rewrite benchmarking, duplicate- and order-aware snapshot comparison, arbitrary plan-summary comparison, durable index leases, and exact plan-action rollback.
- Fail-closed plan verification for matching parameter buckets and non-overlapping Query Store evidence windows.

### Changed

- Measured samples now execute each user query exactly once while returning its result sample and actual plan.
- Full result comparison now preserves duplicate multiplicity, requested ordering, and column type signatures; unavailable or truncated proof is inconclusive.
- Query tuning continues after neutral, regressed, failed, or cleanup-required candidates and records an explicit terminal outcome for every experiment.
- Sandbox index screening and finalist validation measure every recorded parameter bucket within the same 80-execution session budget.
- Direct Query Store and test-index mutation tools are preview-only; writes use prepared plan actions or the leased sandbox index workflow.
- Query-health triage now uses Azure SQL resource, Query Store, wait, blocking, statistics, parameter-sensitivity, and regression evidence. PLE, buffer-cache ratio, and fragmentation are not health classifiers.

### Security

- Durable state omits raw SQL by default, stores fingerprints and redacted evidence, and uses owner-only directory and file permissions.
- Plan apply requires a reviewed intent, evidence hash, exact prior state, server and database policy, an open kill switch, explicit authorization, and an idempotency key.
- Concurrent or uncertain plan applies and rollbacks enter durable reconciliation states instead of crossing the database boundary twice.

### Fixed

- `apply_plan_action` ignored `dry_run`: the tool exposed no such parameter, so a client passing `dry_run=true` had it silently discarded and the Query Store force/unforce executed for real (only blocked by DB permissions in live testing). The tool now accepts `dry_run` defaulting to true, matching the documented admin-tool contract.
- `dry_run_plan_action` always failed ("'dict' object can't be awaited"): the service method was synchronous while the tool wrapper awaits every callback. Now async; verified live with preview + audit + rollback SQL.

- Row-capped fetches no longer drain the remaining result set: advancing past a truncated result set pulled every leftover row over the wire (a 17.9M-row `SELECT *` hit the tool timeout instead of returning 200 rows; now 0.09s). SHOWPLAN sessions still drain — their plan XML arrives as a later result set.
- `check_capabilities` plan probes now target a user table: SQL Server exempts catalog-only statements from the SHOWPLAN permission check, so the old sys.objects probe reported plans as available under logins where explain_query would be denied.

- `set_query_store_hints` never worked live: the driver binds str parameters as varchar while `sp_query_store_set_hints` requires nvarchar; the hints value is now routed through an `nvarchar(max)` variable.
- Workload index analysis (`analyze_workload_indexes`, `optimize_indexes`) failed on every parameterized Query Store text ('must declare the scalar variable @P1'); stored text is now auto-bound before plan compilation, and captured DDL/DML statements are skipped instead of reported as errors.
- Histogram-based parameter binding never worked live: `range_high_key` is sql_variant, which the driver cannot fetch; it is now CONVERTed server-side (style 121).
- `optimize_indexes` size estimation used `sys.dm_db_partition_stats.rows`; the column is `row_count`.
- `analyze_db_health` resource-governance probe named tier-specific columns (`primary_max_cpu_percent` is absent on serverless GP) and silently returned nothing; it now SELECTs * and projects, so serverless tiers get their real limits.
- `tune_query` Query Store history now matches by query_hash from the captured plan (with original-text fallback); the bound DECLARE batch could never match stored text.
- `tune_query`, `benchmark_query_rewrite`, and `explain_query` with `auto_bind_params` no longer fail on parameterized SQL: the read-only validator accepts a `DECLARE` / `SET @variable` prefix before the single SELECT (T-SQL variables are batch-scoped). Session SET options are still rejected.
- `explain_query` with `analyze=true` bounds its result-set fetches (`row_limit + 1`); previously the executed query's full result set was fetched into memory before the plan XML.
- `get_lock_details`, `get_open_transactions`, `get_active_sessions`, and `get_tempdb_usage` are bounded with clamped `limit` parameters and truncation reporting; waiting locks and oldest transactions sort first.
- Transient error codes match on digit boundaries only (code 233 no longer fires on "12330 rows").
- Non-UTF-8 `varbinary` values are hex-encoded (`0x...`) instead of being mangled by a lossy decode.
- Restricted-mode text rules scan comment/literal-stripped SQL, so string data like `'item#1'` or `'please execute the plan'` no longer rejects legitimate SELECTs.
- `get_query_history_by_text` escapes LIKE wildcards in the query fingerprint.
- Database allowlist matching is case-insensitive, matching Azure SQL semantics.
- Config load rejects `AZURE_SQL_TOOL_TIMEOUT_SECONDS` below the query timeout.
- Hostile user-defined type names from the catalog fall back to `nvarchar(256)` in generated `DECLARE` blocks instead of being interpolated verbatim.

### Added

- `AZURE_SQL_TRUST_SERVER_CERTIFICATE` (default false) for self-hosted SQL Server endpoints with self-signed certificates; encryption stays on. Verified live: the full integration suite (including the stdio end-to-end test, which previously skipped for localhost) passes against a SQL Server 2022 Docker container, and 45 of 48 swept restricted tools work on-prem.

- `get_connection_pool_stats` tool (performance group): per-database pool metrics and connection-leak detection without a database round-trip.

### Changed

- Require `mssql-python>=1.10.0`: versions before 1.10 hold the GIL during `cursor.execute`, serializing the entire server behind any in-flight query and preventing the asyncio tool timeout from firing. Verified live on 1.10.0: GIL released, concurrent tool calls work, all 61 live checks and the integration suite pass. Driver-level query/lock timeouts remain set as defense-in-depth (README gotcha 5).
- Pooled connections are no longer recycled on a 45-minute token clock (tokens only matter at login); `SELECT 1` validation runs only after 60s of idle time instead of on every acquire.
- Named profiles expose intentionally different tool surfaces, so documentation no longer relies on one global tool count.

- Aligned docs with current `explain_query` safety behavior (hypothetical indexes disabled on this tool).
- Updated integration workflow bootstrap scripts to use `mssql_python` instead of undeclared `pyodbc`.
- Added baseline community files: `LICENSE`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, and `SECURITY.md`.

## [1.0.0] - 2026-04-09

### Changed

- Bumped version to 1.0.0 for public release.
- Added PyPI metadata (license, classifiers, project URLs) to `pyproject.toml`.
- Sanitized error messages in internal logging paths (`health.py`, `plans.py`, `query_index_analysis.py`, `index_optimizer.py`) to prevent potential leakage of server names or connection details.

### Added

- GitHub Actions CI workflow for automated testing on Python 3.12 and 3.13.

## [0.1.0] - 2026-04-07

### Added

- Initial standalone Azure SQL MCP server release.
- Restricted/unrestricted execution modes with read-only SQL validator in restricted mode.
- Schema introspection, Query Store analysis, plan inspection, health checks, and admin tools.
- Unit and integration test suites.
