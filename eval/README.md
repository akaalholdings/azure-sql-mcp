# Evaluation: static tier

The product must beat a bare model on measured outcomes. This folder holds the part of that
measurement that needs no database: the tasks, their answer keys, a deterministic scorer, a
checker for client text, and the proof adversarial pack.

Nothing here runs SQL on a server or calls a model.

```
eval/
  scoring.py                     task loader, validator, scorer, deliverable checker, CLI
  client_text_field_names.json   field names that must not appear in client text
  tasks/
    manifest.json                sources, and the exam items that were not converted
    root_causes.json             the closed list of root cause codes
    schemas/retail.sql           schema for the tuning tasks (schema qe)
    tuning/*.json                28 tasks
    diagnosis/*.json             11 tasks
    index_review/*.json          13 tasks
  proof_pack/
    pairs.json                   17 baseline and rewrite pairs that are not equivalent
    loader.py                    loader, validator, SQLite runner
```

## Commands

```
uv run python eval/scoring.py validate
uv run python eval/scoring.py score --answers answers.json --out report.json
uv run python eval/scoring.py compare product-report.json bare-report.json
uv run pytest tests/unit -q -k 3_0_eval
```

`validate` exits 1 and lists every problem. `score` refuses to run while a task is invalid.
`compare` exits 1 when the first report does not beat the second.

## What the static tier cannot tell you

- It does not run SQL. It cannot see that a rewrite returns the same rows, and it cannot
  see that a rewrite is faster.
- It recognises a rewrite by text signals (regular expressions) in the answer's SQL. A
  correct rewrite in a shape the key does not list gets no shape point. A wrong rewrite in
  a shape the key does not list is not caught.
- It takes the answer's `proof_scope` as stated. It cannot check a claimed proof.
- The deliverable checker looks for markers. It does not judge whether the advice is good.

The live tier, on the bench database, closes the first three gaps. It runs the baseline and
the answer, compares the rows for each parameter case, measures CPU and reads, and adds a
`measured` block to the answer. The scorer then uses the measured facts in place of the
text signals: see "Tuning" below. Treat a static score as a floor check on keys and
answers, not as proof of value.

## Task files

One JSON file per task, in the folder of its kind. The file name is the task `id`. A text
field can be a string or a list of lines; the loader joins the lines.

Fields of every task:

| Field | Meaning |
|---|---|
| `format_version` | `1` |
| `id` | Unique. Equal to the file name without `.json`. |
| `kind` | `tuning`, `diagnosis` or `index_review` |
| `title`, `tags` | For people. Not scored. |
| `split` | `public`. Only synthetic content with invented names is allowed here. |
| `source` | Where the task comes from, for example `rewrite-exam-v1:Q05` or `s6-new:03`. Unique. |
| `prompt` | What the solver is told: the situation, the measurements, the limits. |
| `key` | The answer key. The solver never sees it. |

### Tuning

| Field | Meaning |
|---|---|
| `schema.file` | DDL file under `tasks/`. The loader puts its text in `schema.ddl`. |
| `schema.tables` | The tables this task needs. All are in schema `qe`. |
| `data_notes` | Row counts and distributions the solver may use. |
| `baseline_kind` | `select`, `procedure` or `batch`: how to run `baseline_sql`. |
| `baseline_sql` | The statement or module to tune. |
| `parameter_cases` | `{name, values, types, weight}` for each case: the same shape the benchmark tools take. Names have no `@`. Weights add up to 1. |

Key:

| Field | Meaning |
|---|---|
| `decision` | `rewrite`, `index_only`, `plan_remedy` or `no_change` |
| `accepted_decisions` | Optional. Other decisions that also earn the decision point. |
| `correct_families` | Correct rewrites or remedies: `{id, description, signals, reference_sql}`. Empty when no accepted decision changes SQL. |
| `traps` | Plausible wrong answers: `{id, class, why, signals, example_sql}`. Add `told_apart_by` when the class is `not_equivalent`. With `"on": "indexes"` the signals read the proposed indexes, and the example is `example_index`. |
| `index.role` | `required` (an index is part of the best answer), `optional` (not scored) or `none` (an index is wrong here). |
| `index.accepted` | Index definitions that earn the index point: `{table, key_columns, include_columns, filter}`. |
| `min_improvement` | `{metric, ratio, basis}`. `ratio` is baseline divided by candidate, weighted over the parameter cases. Optional `worst_case_ratio`, default 0.9. |
| `reference_notes` | For people and for a judge. Not scored. |

Trap classes: `not_equivalent` (the rows can differ), `unsafe` (it can break or block
something), `ineffective` (it does not fix the problem, or fixes it at a cost the prompt
rules out). The first two count as harm when presented as safe. The third only loses
points.

Signals are `{all, any, none}` lists of regular expressions. They are matched without
regard to case against the SQL with comments removed, brackets removed and white space
collapsed. String literals stay, because dynamic SQL is in them. A signal set matches when
every `all` pattern matches, at least one `any` pattern matches when there are any, and no
`none` pattern matches.

An index matches an accepted definition when the table is the same, the accepted key is the
leading key in the same order, every accepted include column is in the key or the includes,
and the filter is the same text. Sort direction is ignored.

### Diagnosis

| Field | Meaning |
|---|---|
| `prompt` | The incident, with what the client said and any client documents. |
| `evidence` | Tool results as fixtures: `{id, tool, arguments, result}`. `tool` is a registered tool name. The payloads hold the facts in the tools' general shape; they are not byte-exact contract fixtures. |

Key:

| Field | Meaning |
|---|---|
| `root_cause` | One code from `tasks/root_causes.json`. |
| `accepted_root_causes` | Optional. Other codes that also earn the points. |
| `required_facts` | Two or three facts: `{id, evidence, description, signals, example}`. `evidence` names the payload that holds the fact. |
| `harmful_conclusions` | `{id, description}` with `root_causes` (codes that are harm when given as the answer), or `signals` and `example` (an action that is harm when recommended), or both. |

### Index review

| Field | Meaning |
|---|---|
| `usage_window` | The period the usage counters cover, and why it starts where it does. |
| `inventory` | Indexes: `{table, name, type, key_columns, include_columns, size_gb, usage{user_seeks, user_scans, user_lookups, user_updates}}`. |
| `workload` | Queries with text, frequency and plan. |
| `facts` | Other facts: constraints, hints, forced plans, jobs outside the window. |

Key:

| Field | Meaning |
|---|---|
| `decisions` | `{index, action, why}` with action `keep`, `drop` or `alter`. An `alter` can state `include_columns`, `key_columns` and `options` that the answer must give. |
| `merges` | `{indexes, include_columns, why}`: the indexes become one index that has those includes. |
| `create` | `{id, accepted, why}`: new indexes, matched like tuning indexes. A `type` of `clustered`, `nonclustered_columnstore` or `clustered_columnstore` must match. |
| `must_not_create` | `{id, table, key_columns, why}`: a definition that must not be built. `key_columns` is a leading key, or the whole key with `"exact": true`. |
| `no_new_index` | `true` when "create nothing" is itself the answer. |
| `harmful_removals` | `{index, why}`: an index whose removal is harm. |

## Answer files

A JSON list of answers, an object with an `answers` list, or a folder with one JSON file per
answer. Each answer has `task_id`. `client_text` is optional on every kind: when present,
the deliverable checker runs on it.

`presented_as_safe` is required wherever shown. `true` means: the answer says this can be
applied as given, with no more testing and no sign-off. `false` means the answer asks for
validation or sign-off first.

Tuning:

```json
{
  "task_id": "exam-q05-latest-order-per-customer-with-ties",
  "decision": "rewrite",
  "sql": "SELECT ...",
  "indexes": [{"table": "qe.Orders", "key_columns": ["CustomerID", "OrderDate"], "include_columns": ["TotalAmount"]}],
  "presented_as_safe": false,
  "proof_scope": "none",
  "measured": {"equivalent": true, "proof_scope": "snapshot_matched", "improvement_ratio": 7.4, "worst_case_ratio": 1.2}
}
```

- `sql` is everything the answer wants run or deployed. Leave it out, or repeat the
  baseline, when the SQL does not change.
- `proof_scope` is required with `sql`: `none`, `snapshot_matched`, `constraint_backed` or
  `unchanged_sql`.
- An index can carry `unique`, `type` and `options`.
- `measured` is written by the live tier only, never by the agent. `equivalent` is the row
  comparison over every parameter case and every adversarial data state. `proof_scope` is
  what the proof tool returned. `improvement_ratio` uses the key's metric.

Diagnosis:

```json
{
  "task_id": "diagnosis-01-upgrade-will-not-fix-the-batch",
  "root_cause": "log_rate_cap",
  "cited_facts": ["LOG_RATE_GOVERNOR is the top wait overnight at 31% of all waits."],
  "recommendations": [{"action": "Move the index job out of the batch window.", "presented_as_safe": false}]
}
```

`recommendations` holds only what the answer says to do. An option the answer rejects
belongs in `client_text`, not here.

Index review:

```json
{
  "task_id": "index-01-near-duplicate-customer-date-indexes",
  "decisions": [
    {"index": "IX_Orders_CustomerId_OrderDate", "action": "alter", "include_columns": ["Status", "TotalAmount", "CurrencyCode", "ChannelId"], "presented_as_safe": false},
    {"index": "IX_Orders_Customer_OrderDate_Hist", "action": "merge", "into": "IX_Orders_CustomerId_OrderDate", "presented_as_safe": false}
  ],
  "create": []
}
```

Actions are `keep`, `drop`, `merge` (with `into`) and `alter`. Each `create` entry is an
index definition with `presented_as_safe`.

## How scoring works

Each answer gets points, a correctness verdict and a harm count. An answer that is missing
or malformed scores zero against the full maximum and is listed with the reason.

Tuning: one point each.

| Component | Earned when |
|---|---|
| `decision` | The decision is the key's, or an accepted one. |
| `shape` | For a rewrite or plan remedy: the SQL matches a correct family, or the live tier measured equal rows and the required gain. For index-only or no-change: the SQL is unchanged. |
| `no_trap` | No trap matches. A `not_equivalent` trap is ignored when the live tier measured equal rows. Measured different rows lose the point. |
| `index` | Role `required`: a proposed index matches an accepted one. Role `none`: no index is proposed. Role `optional`: not scored. |
| `improvement` | Scored only with a `measured` block: the ratio reaches the key's ratio and no parameter case is below the worst-case ratio. |

Diagnosis: 2 points for the root cause, 1 point for each required fact. A fact counts when
one cited sentence matches its signals; words spread over several sentences do not count.

Index review: 1 point for each keyed decision, each merge, each create, and for
`no_new_index`. A merge needs exactly one survivor (`alter` or `keep`) that carries the
keyed includes, with every other index of the group dropped or merged into it. Silence
about an index is not a decision.

## What each number means

| Number | Meaning |
|---|---|
| `points`, `max_points` | Outcome points earned and available. The maximum grows by one for each tuning answer that has a measured improvement, so compare runs that were measured the same way. |
| `score_pct` | 100 x points / max_points. |
| `correct` | Answers that reach the keyed outcome with nothing wrong in them. Tuning: decision, shape and no trap. Diagnosis: the root cause, and no harmful conclusion recommended, safe or hedged. Index review: every keyed item, no harmful removal, no forbidden index. |
| `harm` | Recommendations that are wrong, unsafe or unproven and are presented as safe. See below. |
| `tasks_with_harm` | Answers with at least one. |
| `hedged` | Recommendations not presented as safe. An agent that marks everything "needs validation" has no harm; this count shows it. |
| `measured` | Tuning answers scored with facts from the live tier. |
| `deliverables_checked`, `deliverables_passed` | Client texts checked, and those that passed every rule. |
| `by_kind` | The same totals per task kind. |

Harm counts only what the answer presents as safe:

- Tuning: 1 when the changed SQL matches a `not_equivalent` or `unsafe` trap, or was
  measured as not equivalent, or has a proof scope other than `constraint_backed` or
  `unchanged_sql`. Wrong and unproven together are one harmful recommendation. Plus 1 for
  each harmful index trap. Equal rows on today's data (`snapshot_matched`) are not a proof.
- Diagnosis: 1 when the root cause is a code that a harmful conclusion lists (this needs no
  `presented_as_safe`: a root cause is a claim), and 1 for each harmful conclusion whose
  action is recommended as safe.
- Index review: 1 for each removal (`drop` or `merge`) of an index in `harmful_removals`,
  and 1 for each created or re-keyed index that matches `must_not_create`.

`compare` passes only when the product has more points than the baseline, at least as many
correct answers, and zero harm, on the same tasks.

## Client deliverable checker

`check_client_deliverable(text, is_change=...)` returns one result per rule and `passed`.

| Rule | Passes when |
|---|---|
| `recommendation_first` | The first paragraph that is not a heading holds a recommendation word (recommend, do not, should, keep, drop, no change and others in `RECOMMENDATION_PATTERN`). |
| `evidence_cited` | The text holds a figure with a unit: 31%, 140 ms, 96 MiB/s, 12,000 rows. |
| `risk_of_acting` | A phrase such as "risk of acting" or "risk of the change". |
| `risk_of_not_acting` | A phrase such as "risk of not acting" or "if we do nothing". |
| `rollback` | For a change: rollback, revert, undo or back out. Not applied when nothing changes. |
| `internal_names` | Empty. It lists every registered tool name (from `TOOL_GROUPS` in the server configuration) and every name in `client_text_field_names.json` found in the text. |

Pass `tool_names=` or `field_names=` to use other lists. The patterns are constants at the
top of `scoring.py`.

## Add a task

1. Copy a task of the same kind and change `id`, `source`, `title`, `prompt` and the data.
   Use invented names only. Tuning SQL uses schema `qe` and tables from `schemas/retail.sql`;
   add a table there when the task needs one.
2. Write the key. For each correct family give `reference_sql`. For each trap give an
   example, a class and `why`.
3. Run `uv run python eval/scoring.py validate`. It checks the fields, and it scores the key
   against itself: each reference must earn every point with no harm, each trap example
   must be caught by its own signals, and harm must follow the trap's class. Each
   diagnosis fact and harmful action needs an `example` that its signals match.
4. Run `uv run pytest tests/unit -q -k 3_0_eval`.

Keep signals narrow. A trap signal that matches a correct family's reference fails
validation, because the reference then loses the no-trap point.

## Proof adversarial pack

`proof_pack/pairs.json` holds pairs of baseline and rewrite that return equal rows on one
small data set (`equal_on`) and different rows on another (`differs_on`). Both data sets
obey the declared schema. A proof that accepts a pair is wrong, with any list of declared
preconditions, the empty list included: `invariant` states that no pair may get past
`snapshot_matched` or become deployment ready.

Each pair has `baseline_sql`, `rewrite_sql`, `parameters`, `parameter_types`, `reason`,
`tells_apart`, `expected` (`equal_rows`, `baseline_rows`, `rewrite_rows`) and
`would_be_equivalent_if`: the constraints that would make the rewrite correct and that the
schema does not have. The kinds are `not_null`, `unique`, `trusted_foreign_key` and
`trusted_check`.

`loader.py`:

- `load_proof_pack()` reads and validates the pack.
- `expected_rows(pair, state)` returns the baseline rows and the rewrite rows for
  `equal_on` or `differs_on`, for a fake executor in a unit test.
- `run_pair(pack, pair, state)` runs a portable pair in SQLite.
- `ddl_statements(pack)` and `load_statements(pack, pair, state)` return T-SQL for a live
  run. Run each statement as its own batch.

Fifteen pairs are portable and are executed in the tests. Two use T-SQL that SQLite cannot
run; their expected rows are reasoned and must be confirmed on the bench database.
