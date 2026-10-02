"""Workload-driven index design for Azure SQL Database (pure analysis).

Input: the Query Store workload of a window (runtime totals per query plus the
stored plan of each query's dominant plan), the table and column catalog, the
existing index definitions with usage counters, column selectivity from
statistics histograms, and foreign keys.

Output: per-table recommendations (create, extend, widen, consolidate, drop
candidate, clustered index for a heap) with the queries each one serves, the
evidence behind it, a confidence level, inert DDL with exact rollback, and a
validation path. Everything is recommend-only; nothing here executes SQL.

Attribution: a query's measured Query Store cost (CPU, duration, or reads over
the window) is split across its plan operators in proportion to each
operator's share of the statement's estimated cost. The optimizer's cost model
is a model, not a measurement, so attributed figures are labelled estimates and
are used for ranking, never presented as a promised gain.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Iterable

from .index_ddl import quote_identifier
from .index_ddl import render_reverse_index_ddl
from .index_metadata import ExistingIndex
from .index_metadata import IndexKeyColumn
from .showplan_access import PlanAccessSummary
from .showplan_access import TableAccess

CONTRACT = "workload_index_advice_v1"

OBJECTIVES: dict[str, tuple[str, str]] = {
    "cpu": ("total_cpu_us", "cpu_microseconds"),
    "duration": ("total_duration_us", "duration_microseconds"),
    "logical_reads": ("total_logical_reads", "logical_page_reads"),
    "executions": ("executions", "executions"),
}

# Design cap; the engine allows 32 key columns.
MAX_KEY_COLUMNS = 16
MAX_NONCLUSTERED_KEY_BYTES = 1700
LOW_SELECTIVITY_DISTINCT = 10
LOB_TYPES = {"text", "ntext", "image", "xml", "geography", "geometry"}
KEY_INELIGIBLE_TYPES = LOB_TYPES | {"sql_variant", "hierarchyid_lob"}
ROWSTORE_NONCLUSTERED = 2
UNUSED_MIN_UPTIME_DAYS = 7
# Index removal window (owner decision 2026-10-02): one full month-end plus a
# buffer for close jobs and one missed capture.
UNUSED_HIGH_CONFIDENCE_DAYS = 35
# A plan-reference check shorter than this is no evidence of non-use.
MIN_REFERENCE_WINDOW_DAYS = 7
# Proof handoff: the costliest supporting queries up to 80% of the
# recommendation's attributed cost, at most three.
PROOF_SET_SHARE = 0.8
PROOF_SET_MAX = 3
REGRESSION_SET_MAX = 10
ROLLBACK_CPU_INCREASE_PCT = 20
PIN_BLOCKER = "index_named_by_hint_or_forced_plan"
# An index hint the scan could not tie to exactly one index (case differs from the
# catalog name, or the name is on several tables): it may name any removal.
UNRESOLVED_HINT_BLOCKER = "unresolved_index_hint"
# Removal blockers that cap confidence at medium; any other blocker means low.
_SOFT_REMOVAL_BLOCKERS = frozenset(
    {
        "forced_plan_dependency_check_incomplete",
        "hint_reference_check_incomplete",
        "query_store_reference_window_shorter_than_usage_window",
        "query_store_window_coverage_unknown",
        "statistics_reference_check_unavailable",
    }
)


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


@dataclass
class WorkloadQuery:
    query_id: int
    plan_id: int
    plan: PlanAccessSummary
    executions: float = 0.0
    total_cpu_us: float = 0.0
    total_duration_us: float = 0.0
    total_logical_reads: float = 0.0
    total_logical_writes: float = 0.0
    total_rowcount: float = 0.0
    active_days: int = 0
    object_name: str | None = None
    query_text_preview: str | None = None
    is_forced_plan: bool = False

    def metric(self, objective: str) -> float:
        attribute = OBJECTIVES[objective][0]
        return float(getattr(self, attribute) or 0.0)


@dataclass
class TableInfo:
    schema: str
    table: str
    object_id: int | None = None
    row_count: int | None = None
    base_used_pages: int | None = None
    all_used_pages: int | None = None
    is_heap: bool = False
    is_memory_optimized: bool = False
    forwarded_fetches: int | None = None

    @property
    def key(self) -> tuple[str, str]:
        return (self.schema.casefold(), self.table.casefold())

    @property
    def size_mb(self) -> float | None:
        if self.all_used_pages is None:
            return None
        return round(self.all_used_pages * 8 / 1024, 2)


@dataclass
class ColumnInfo:
    name: str
    type_name: str
    max_length: int
    is_nullable: bool = True
    is_computed: bool = False
    # COLUMNPROPERTY IsIndexable / IsDeterministic; None when not read.
    is_indexable: bool | None = None
    is_deterministic: bool | None = None

    @property
    def is_lob(self) -> bool:
        return self.max_length == -1 or self.type_name.lower() in LOB_TYPES

    @property
    def key_eligible(self) -> bool:
        return (
            not self.is_lob
            and self.type_name.lower() not in KEY_INELIGIBLE_TYPES
            and (not self.is_computed or self.is_indexable is True)
        )

    @property
    def include_eligible(self) -> bool:
        return not self.is_computed or self.is_deterministic is True

    @property
    def width_bytes(self) -> int:
        return 8000 if self.max_length == -1 else max(1, self.max_length)


@dataclass
class ColumnSelectivity:
    distinct_estimate: float | None
    rows: float | None
    modification_counter: int | None = None
    stats_name: str | None = None
    last_updated_utc: str | None = None


@dataclass
class ForeignKeyInfo:
    name: str
    schema: str
    table: str
    columns: tuple[str, ...]
    referenced_schema: str
    referenced_table: str
    delete_action: str | None = None
    is_disabled: bool = False


@dataclass
class AdvisorSettings:
    objective: str = "cpu"
    lookback_days: int = 7
    min_table_rows: int = 10_000
    max_recommendations_per_table: int = 5
    max_include_columns: int = 12
    include_existing_index_review: bool = True
    focus_tables: frozenset[tuple[str, str]] | None = None
    focus_schema: str | None = None


@dataclass
class AdvisorInputs:
    database_name: str
    window_start_utc: str
    window_end_utc: str
    settings: AdvisorSettings
    queries: list[WorkloadQuery]
    workload_totals: dict[str, float]
    tables: dict[tuple[str, str], TableInfo]
    columns: dict[tuple[str, str], dict[str, ColumnInfo]]
    existing_indexes: list[ExistingIndex]
    selectivity: dict[tuple[str, str, str], ColumnSelectivity] = field(default_factory=dict)
    foreign_keys: list[ForeignKeyInfo] = field(default_factory=list)
    index_plan_references: dict[tuple[str, str, str], int | None] = field(default_factory=dict)
    engine_start_time_utc: str | None = None
    query_store: dict[str, Any] = field(default_factory=dict)
    gaps: list[str] = field(default_factory=list)
    observed_at_utc: str | None = None
    # Plans that read each unused index's statistics (OptimizerStatsUsage); None when unchecked.
    index_statistics_references: dict[tuple[str, str, str], int | None] = field(default_factory=dict)
    # Query Store intervals overlapping the window: effective_start_utc,
    # effective_end_utc, interval_count, oldest_interval_start_utc. None when unread.
    query_store_coverage: dict[str, Any] | None = None
    # (query_id, plan_id, schema, table, index_name) for every index a forced
    # Query Store plan reads, from all forced plans, not only the analysed ones.
    forced_plan_accesses: list[tuple[int, int, str, str, str]] = field(default_factory=list)
    # Index hints in query text, Query Store hints, plan guides and modules:
    # (schema, table, index) -> where each hint is.
    index_hint_references: dict[tuple[str, str, str], list[str]] = field(default_factory=dict)
    # "complete" only when every forced plan / every hint source was read.
    # hint_coverage "unresolved": a hint names no single index, so removals fail closed.
    forced_plan_coverage: str = "not_checked"
    hint_coverage: str = "not_checked"


# ---------------------------------------------------------------------------
# Internal working types
# ---------------------------------------------------------------------------


@dataclass
class _Access:
    query: WorkloadQuery
    access: TableAccess
    attributed: float
    statement_type: str


@dataclass
class _Candidate:
    schema: str
    table: str
    keys: list[tuple[str, str]]
    includes: list[str]
    supports: list[_Access] = field(default_factory=list)
    kinds: set[str] = field(default_factory=set)
    savings: float = 0.0
    hint_agreement: bool = False
    partial_covering: bool = False
    low_selectivity: bool = False
    notes: list[str] = field(default_factory=list)

    @property
    def key_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.keys)


@dataclass
class _Pin:
    """Why an index cannot be removed yet, and what must change first."""

    reasons: list[str] = field(default_factory=list)
    prerequisites: list[str] = field(default_factory=list)

    def add(self, reason: str, prerequisite: str) -> None:
        if reason not in self.reasons:
            self.reasons.append(reason)
        if prerequisite not in self.prerequisites:
            self.prerequisites.append(prerequisite)


def _norm(value: str) -> str:
    return value.casefold()


def _table_key(schema: str, table: str) -> tuple[str, str]:
    return (_norm(schema), _norm(table))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def build_index_advice(inputs: AdvisorInputs) -> dict[str, Any]:
    settings = inputs.settings
    objective_attr, objective_unit = OBJECTIVES[settings.objective]
    total = float(inputs.workload_totals.get(objective_attr) or 0.0)
    analyzed_total = sum(query.metric(settings.objective) for query in inputs.queries)

    accesses, writes, rewrites, parse_failures = _attribute_accesses(inputs)
    by_table: dict[tuple[str, str], list[_Access]] = defaultdict(list)
    for item in accesses:
        by_table[_table_key(item.access.schema, item.access.table)].append(item)

    existing_by_table: dict[tuple[str, str], list[ExistingIndex]] = defaultdict(list)
    for index in inputs.existing_indexes:
        existing_by_table[_table_key(index.schema, index.table)].append(index)

    table_keys = set(by_table)
    if settings.focus_tables:
        table_keys |= {key for key in settings.focus_tables if key in inputs.tables}
        table_keys &= set(settings.focus_tables)
    elif settings.focus_schema:
        table_keys = {key for key in table_keys if key[0] == _norm(settings.focus_schema)}
    if settings.include_existing_index_review and not settings.focus_tables:
        table_keys |= {
            key
            for key in existing_by_table
            if key in inputs.tables
            and (settings.focus_schema is None or key[0] == _norm(settings.focus_schema))
        }

    uptime_days = _days_since(inputs.engine_start_time_utc, inputs.observed_at_utc)
    window, _ = _query_store_window(inputs)
    rate_days = _rate_days(inputs)
    recommendations: list[dict[str, Any]] = []
    tables_out: list[dict[str, Any]] = []
    for key in sorted(table_keys, key=lambda k: -sum(a.attributed for a in by_table.get(k, []))):
        info = inputs.tables.get(key)
        table_accesses = by_table.get(key, [])
        schema, table = _display_identity(key, info, table_accesses, existing_by_table.get(key, []))
        columns = inputs.columns.get(key, {})
        existing = [
            index
            for index in existing_by_table.get(key, [])
            if not index.is_hypothetical
        ]
        table_writes = writes.get(key, {"dml_statements": 0, "rows": 0.0, "executions": 0.0})
        table_recs: list[dict[str, Any]] = []
        notes: list[str] = []

        too_small = info is not None and info.row_count is not None and info.row_count < settings.min_table_rows
        if info is not None and info.is_memory_optimized:
            notes.append("memory_optimized_table_not_reviewed")
        elif too_small and table_accesses:
            notes.append("table_below_min_rows")
        elif table_accesses:
            candidates = _candidates_for_table(
                schema, table, table_accesses, columns, inputs, settings
            )
            candidates = _merge_candidates(candidates, columns, settings)
            table_recs.extend(
                _reconcile_with_existing(
                    candidates, existing, columns, info, table_writes, inputs, total,
                    accesses=table_accesses, rate_days=rate_days,
                )
            )
        if settings.include_existing_index_review:
            table_recs.extend(
                _existing_index_findings(
                    schema,
                    table,
                    existing,
                    table_accesses,
                    inputs,
                    uptime_days,
                    total,
                )
            )
            heap = _heap_recommendation(schema, table, info, table_accesses, existing, total)
            if heap is not None:
                table_recs.append(heap)
            table_recs.extend(_foreign_key_findings(schema, table, existing, inputs, writes))

        table_recs.sort(key=lambda rec: (-_ACTION_PRIORITY.get(rec["action"], 0), -rec["score"]))
        actionable = [rec for rec in table_recs if rec["action"] != "review_index"]
        advisory = [rec for rec in table_recs if rec["action"] == "review_index"]
        table_recs = actionable[: settings.max_recommendations_per_table] + advisory
        if not table_accesses and not table_recs:
            continue
        for rec in table_recs:
            rec["id"] = f"R{len(recommendations) + 1}"
            recommendations.append(rec)

        tables_out.append(
            {
                "schema": schema,
                "table": table,
                "row_count": info.row_count if info else None,
                "size_mb": info.size_mb if info else None,
                "is_heap": bool(info.is_heap) if info else None,
                "workload_share_pct": _pct(sum(a.attributed for a in table_accesses), total),
                "access_summary": _access_summary(table_accesses),
                "write_activity": {
                    "dml_statements": int(table_writes["dml_statements"]),
                    "dml_executions_in_window": round(float(table_writes["executions"]), 1),
                    "dml_rows_in_window": round(float(table_writes["rows"]), 1),
                    "dml_rows_per_day": round(float(table_writes["rows"]) / rate_days, 1),
                },
                "existing_indexes": [
                    _existing_index_summary(index, table_accesses, inputs) for index in existing
                ],
                "notes": notes,
                "recommendation_ids": [rec["id"] for rec in table_recs],
            }
        )

    for rec in recommendations:
        rec.pop("_supports", None)
    counts: dict[str, int] = defaultdict(int)
    for rec in recommendations:
        counts[rec["action"]] += 1
    improvements = sorted(
        (rec for rec in recommendations if rec["action"] in _IMPROVEMENT_ACTIONS),
        key=lambda rec: -rec["score"],
    )
    cleanup = [rec for rec in recommendations if rec["action"] in _CLEANUP_ACTIONS]
    gaps = list(
        dict.fromkeys(
            inputs.gaps
            + _coverage_gaps(inputs, analyzed_total, total, parse_failures, uptime_days)
        )
    )
    return {
        "contract": CONTRACT,
        "recommend_only": True,
        "database_name": inputs.database_name,
        "window": window,
        "objective": settings.objective,
        "objective_unit": objective_unit,
        "query_store": dict(inputs.query_store),
        "workload": {
            "queries_in_window": int(inputs.workload_totals.get("workload_query_count") or 0),
            "executions_in_window": float(inputs.workload_totals.get("workload_executions") or 0.0),
            "objective_total": total,
            "queries_analyzed": len(inputs.queries),
            "analyzed_share_pct": _pct(analyzed_total, total),
            "plans_unparseable": parse_failures,
        },
        "usage_counters": {
            "engine_start_time_utc": inputs.engine_start_time_utc,
            "days_since_reset": round(uptime_days, 1) if uptime_days is not None else None,
        },
        "summary": {
            "tables_reviewed": len(tables_out),
            "recommendation_counts": dict(sorted(counts.items())),
            "top_improvement_ids": [rec["id"] for rec in improvements[:10]],
            "cleanup_ids": [rec["id"] for rec in cleanup],
        },
        "tables": tables_out,
        "recommendations": recommendations,
        "rewrite_opportunities": rewrites,
        "gaps": gaps,
        "next_steps": _next_steps(recommendations, rewrites),
    }


_IMPROVEMENT_ACTIONS = frozenset(
    {"create_index", "extend_index", "widen_index", "create_clustered_index"}
)
_CLEANUP_ACTIONS = frozenset({"consolidate_index", "drop_index"})

_ACTION_PRIORITY = {
    "extend_index": 6,
    "widen_index": 5,
    "create_index": 4,
    "consolidate_index": 3,
    "create_clustered_index": 3,
    "drop_index": 2,
    "review_index": 0,
}


# ---------------------------------------------------------------------------
# Attribution
# ---------------------------------------------------------------------------


def _attribute_accesses(
    inputs: AdvisorInputs,
) -> tuple[list[_Access], dict[tuple[str, str], dict[str, float]], list[dict[str, Any]], int]:
    objective = inputs.settings.objective
    total = float(inputs.workload_totals.get(OBJECTIVES[objective][0]) or 0.0)
    accesses: list[_Access] = []
    writes: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: {"dml_statements": 0, "rows": 0.0, "executions": 0.0}
    )
    rewrites: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    parse_failures = 0
    for query in inputs.queries:
        if query.plan.parse_error:
            parse_failures += 1
            continue
        metric = query.metric(objective)
        for statement in query.plan.statements:
            for target in statement.dml_targets:
                if not _reviewable_table(target.schema, target.table):
                    continue
                entry = writes[_table_key(target.schema, target.table)]
                entry["dml_statements"] += 1
                entry["rows"] += query.total_rowcount
                entry["executions"] += query.executions
            for access in statement.accesses:
                if not _reviewable_table(access.schema, access.table):
                    continue
                attributed = metric * access.cost_share
                item = _Access(
                    query=query,
                    access=access,
                    attributed=attributed,
                    statement_type=statement.statement_type,
                )
                accesses.append(item)
                for column, kind in access.nonsargable.items():
                    key = (access.schema, access.table, column, kind)
                    entry = rewrites.setdefault(
                        key,
                        {
                            "schema": access.schema,
                            "table": access.table,
                            "column": column,
                            "pattern": kind,
                            "query_ids": [],
                            "attributed": 0.0,
                        },
                    )
                    if query.query_id not in entry["query_ids"]:
                        entry["query_ids"].append(query.query_id)
                    entry["attributed"] += attributed
    rewrite_list = []
    for entry in sorted(rewrites.values(), key=lambda value: -value["attributed"]):
        pattern = entry["pattern"]
        rewrite_list.append(
            {
                "schema": entry["schema"],
                "table": entry["table"],
                "column": entry["column"],
                "pattern": pattern,
                "explanation": _rewrite_explanation(pattern),
                "query_ids": entry["query_ids"][:20],
                "workload_share_pct": _pct(entry["attributed"], total),
                "owner": "sql-optimizer",
            }
        )
    return accesses, dict(writes), rewrite_list, parse_failures


def _rewrite_explanation(pattern: str) -> str:
    if pattern == "convert_implicit":
        return (
            "An implicit conversion wraps the column, usually a parameter or variable "
            "type that differs from the column type. No index can seek through it; bind "
            "the parameter as the column's exact type."
        )
    if pattern.startswith("function:"):
        return (
            f"The predicate applies {pattern.split(':', 1)[1].upper()} to the column. "
            "Rewrite it so the column is bare (move the work to the parameter side) "
            "before expecting an index to seek."
        )
    return (
        "The predicate wraps the column in an expression, so it cannot seek. Rewrite it "
        "with the bare column before adding an index for it."
    )


def _reviewable_table(schema: str, table: str) -> bool:
    if not schema or not table:
        return False
    if _norm(schema) in {"sys", "information_schema"}:
        return False
    return not table.startswith(("#", "@"))


# ---------------------------------------------------------------------------
# Candidate generation
# ---------------------------------------------------------------------------


def _candidates_for_table(
    schema: str,
    table: str,
    accesses: list[_Access],
    columns: dict[str, ColumnInfo],
    inputs: AdvisorInputs,
    settings: AdvisorSettings,
) -> list[_Candidate]:
    key = _table_key(schema, table)
    info = inputs.tables.get(key)
    table_rows = float(info.row_count) if info and info.row_count else None
    # Equality-column popularity: sum of each distinct query's measured cost.
    # Weighting by query (not operator) keeps an expensive lookup's residual
    # column from outranking the column every query seeks on.
    frequency: dict[str, float] = defaultdict(float)
    seen_columns: set[tuple[int, int, str]] = set()
    for item in accesses:
        columns_used = _equality_columns(item.access)
        if item.access.operation in {"lookup", "rid_lookup"}:
            columns_used = list(item.access.residual_eq_columns)
        for column in columns_used:
            marker = (item.query.query_id, item.query.plan_id, _norm(column))
            if marker in seen_columns:
                continue
            seen_columns.add(marker)
            frequency[_norm(column)] += item.query.metric(settings.objective) + 1e-9
    hints = [
        hint
        for item in accesses
        for statement in item.query.plan.statements
        for hint in statement.missing_indexes
        if _table_key(hint.schema, hint.table) == key
    ]
    existing = [
        index
        for index in inputs.existing_indexes
        if _table_key(index.schema, index.table) == key and not index.is_disabled
    ]
    clustering = _clustering_keys(existing)
    feeders = _lookup_feeders(accesses)
    paired_feeder_ids = {id(feeder) for feeder in feeders.values()}
    candidates: list[_Candidate] = []
    for item in accesses:
        access = item.access
        if item.attributed <= 0 and item.query.executions <= 0:
            continue
        if (access.storage or "").lower() == "columnstore":
            continue
        if id(item) in paired_feeder_ids:
            # Evaluated together with the lookup it feeds.
            continue
        candidate: _Candidate | None
        if access.operation in {"lookup", "rid_lookup"}:
            feeder = feeders.get(id(item))
            if feeder is None:
                continue
            candidate = _lookup_candidate(
                item, feeder, columns, clustering, frequency, inputs, settings
            )
        elif access.operation in {"scan", "heap_scan", "seek"}:
            candidate = _access_candidate(
                item, columns, clustering, frequency, inputs, table_rows, settings
            )
        else:
            candidate = None
        if candidate is None:
            continue
        candidate.hint_agreement = any(
            _hint_agrees(candidate, hint) for hint in hints
        )
        candidates.append(candidate)
    return candidates


def _equality_columns(access: TableAccess) -> list[str]:
    columns = list(access.seek_eq_columns)
    for column in access.residual_eq_columns + access.spool_eq_columns:
        if column not in columns:
            columns.append(column)
    return columns


def _access_candidate(
    item: _Access,
    columns: dict[str, ColumnInfo],
    clustering: tuple[str, ...],
    frequency: dict[str, float],
    inputs: AdvisorInputs,
    table_rows: float | None,
    settings: AdvisorSettings,
) -> _Candidate | None:
    access = item.access
    eq = [column for column in _equality_columns(access) if _key_eligible(columns, column)]
    ranges = [
        column
        for column in (
            list(access.seek_range_columns)
            + list(access.residual_range_columns)
            + list(access.spool_range_columns)
        )
        if column not in eq and _key_eligible(columns, column)
    ]
    if access.operation == "seek":
        residual_keys = [c for c in access.residual_eq_columns + access.residual_range_columns]
        if not residual_keys:
            return None
        kind = "extend_seek"
    else:
        if not eq and not ranges:
            return None
        kind = "replace_eager_spool" if access.spool_node_id is not None else "scan_to_seek"

    order_columns = [
        (column, direction)
        for column, direction in access.order_columns
        if _key_eligible(columns, column)
    ]
    selectivity = inputs.selectivity
    schema, table = access.schema, access.table

    def distinct(column: str) -> float:
        value = selectivity.get((_norm(schema), _norm(table), _norm(column)))
        return float(value.distinct_estimate or 0.0) if value else 0.0

    eq.sort(key=lambda column: (-frequency.get(_norm(column), 0.0), -distinct(column), _norm(column)))
    keys: list[tuple[str, str]] = [(column, "ASC") for column in eq]
    range_column: str | None = None
    if ranges:
        order_names = [_norm(column) for column, _ in order_columns]
        ranges.sort(
            key=lambda column: (
                0 if _norm(column) in order_names else 1,
                -frequency.get(_norm(column), 0.0),
                -distinct(column),
                _norm(column),
            )
        )
        range_column = ranges[0]
        direction = next(
            (d for column, d in order_columns if _norm(column) == _norm(range_column)), "ASC"
        )
        keys.append((range_column, direction))
    elif order_columns:
        for column, direction in order_columns:
            if _norm(column) not in {_norm(name) for name, _ in keys}:
                keys.append((column, direction))
    keys = keys[:MAX_KEY_COLUMNS]

    needed = list(access.output_columns) + list(access.residual) + list(access.nonsargable)
    candidate = _Candidate(
        schema=schema,
        table=table,
        keys=keys,
        includes=[],
        supports=[item],
        kinds={kind},
    )
    _set_includes(candidate, needed, columns, clustering, settings)
    low = [column for column, _ in keys if 0 < distinct(column) < LOW_SELECTIVITY_DISTINCT]
    if keys and len(low) == len(keys):
        candidate.low_selectivity = True
        candidate.notes.append("all_key_columns_low_selectivity")
    selectivity_ratio = 0.5
    if table_rows and table_rows > 0 and access.estimated_rows >= 0:
        selectivity_ratio = min(1.0, access.estimated_rows / table_rows)
    if kind == "scan_to_seek":
        factor = 1.0 - selectivity_ratio
    elif kind == "replace_eager_spool":
        # The scan feeds a spool, so it reads the whole table on every
        # execution; a permanent index removes nearly all of that work.
        factor = 0.9
    else:
        factor = 0.4
    candidate.savings = item.attributed * max(0.0, factor)
    return candidate


def _lookup_feeders(accesses: list[_Access]) -> dict[int, _Access]:
    """Map each lookup to the nonclustered seek or scan that fed it in the same plan."""

    by_node: dict[tuple[int, int, int], _Access] = {
        (item.query.query_id, item.query.plan_id, item.access.node_id): item
        for item in accesses
    }
    feeders: dict[int, _Access] = {}
    for item in accesses:
        access = item.access
        if access.operation not in {"lookup", "rid_lookup"} or access.paired_node_id is None:
            continue
        feeder = by_node.get((item.query.query_id, item.query.plan_id, access.paired_node_id))
        if feeder is not None:
            feeders[id(item)] = feeder
    return feeders


def _lookup_candidate(
    lookup: _Access,
    feeder: _Access,
    columns: dict[str, ColumnInfo],
    clustering: tuple[str, ...],
    frequency: dict[str, float],
    inputs: AdvisorInputs,
    settings: AdvisorSettings,
) -> _Candidate | None:
    """Design one index for a seek or scan plus the lookup it needed.

    The feeder's equality and range columns stay in the key, equality filters that
    were only evaluated at the lookup join them, and every output column from both
    operators becomes an include so the lookup disappears. When covering is not
    possible (LOB columns, or most of the row), the lookup cannot be removed and no
    candidate is produced from it.
    """

    access = lookup.access
    feed = feeder.access
    eq = [
        column
        for column in _equality_columns(feed) + list(access.residual_eq_columns)
        if _key_eligible(columns, column)
    ]
    eq = list(dict.fromkeys(eq))
    ranges = [
        column
        for column in list(feed.seek_range_columns)
        + list(feed.residual_range_columns)
        + list(access.residual_range_columns)
        if column not in eq and _key_eligible(columns, column)
    ]
    selectivity = inputs.selectivity

    def distinct(column: str) -> float:
        value = selectivity.get((_norm(access.schema), _norm(access.table), _norm(column)))
        return float(value.distinct_estimate or 0.0) if value else 0.0

    eq.sort(key=lambda column: (-frequency.get(_norm(column), 0.0), -distinct(column), _norm(column)))
    order_columns = [
        (column, direction)
        for column, direction in (feed.order_columns or access.order_columns)
        if _key_eligible(columns, column)
    ]
    keys: list[tuple[str, str]] = [(column, "ASC") for column in eq]
    if ranges:
        order_names = [_norm(column) for column, _ in order_columns]
        ranges.sort(
            key=lambda column: (
                0 if _norm(column) in order_names else 1,
                -frequency.get(_norm(column), 0.0),
                -distinct(column),
                _norm(column),
            )
        )
        direction = next(
            (d for column, d in order_columns if _norm(column) == _norm(ranges[0])), "ASC"
        )
        keys.append((ranges[0], direction))
    if not keys:
        return None
    needed = (
        list(feed.output_columns)
        + list(feed.residual)
        + list(access.output_columns)
        + list(access.residual)
    )
    candidate = _Candidate(
        schema=access.schema,
        table=access.table,
        keys=keys[:MAX_KEY_COLUMNS],
        includes=[],
        supports=[feeder, lookup],
        kinds={"cover_lookup"} | ({"extend_seek"} if feed.residual else set()),
    )
    _set_includes(candidate, needed, columns, clustering, settings)
    if (
        candidate.partial_covering
        or "lob_columns_not_included" in candidate.notes
        or "computed_columns_not_includable" in candidate.notes
    ):
        return None
    low = [column for column, _ in candidate.keys if 0 < distinct(column) < LOW_SELECTIVITY_DISTINCT]
    if len(low) == len(candidate.keys):
        candidate.low_selectivity = True
        candidate.notes.append("all_key_columns_low_selectivity")
    candidate.savings = lookup.attributed + (feeder.attributed * 0.4 if feed.residual else 0.0)
    return candidate


def _set_includes(
    candidate: _Candidate,
    needed: Iterable[str],
    columns: dict[str, ColumnInfo],
    clustering: tuple[str, ...],
    settings: AdvisorSettings,
) -> None:
    key_names = {_norm(name) for name in candidate.key_names}
    implicit = {_norm(name) for name in clustering}
    includes: list[str] = []
    skipped_lob: list[str] = []
    skipped_computed: list[str] = []
    for column in needed:
        lowered = _norm(column)
        if lowered in key_names or lowered in implicit or lowered in {_norm(c) for c in includes}:
            continue
        info = columns.get(lowered)
        if info is not None and info.is_lob:
            skipped_lob.append(column)
            continue
        if info is not None and not info.include_eligible:
            skipped_computed.append(column)
            continue
        includes.append(column)
    if skipped_lob:
        candidate.notes.append("lob_columns_not_included")
    if skipped_computed:
        candidate.notes.append("computed_columns_not_includable")
    if len(includes) > settings.max_include_columns or _too_wide(includes, columns):
        candidate.partial_covering = True
        candidate.notes.append("covering_would_duplicate_most_of_the_table")
        includes = []
    candidate.includes = includes


def _too_wide(includes: list[str], columns: dict[str, ColumnInfo]) -> bool:
    if not columns or not includes:
        return False
    row_width = sum(column.width_bytes for column in columns.values() if not column.is_lob)
    include_width = sum(
        columns[_norm(name)].width_bytes for name in includes if _norm(name) in columns
    )
    return row_width > 0 and include_width > 0.6 * row_width and len(includes) > 4


def _key_width(keys: list[tuple[str, str]], columns: dict[str, ColumnInfo]) -> int:
    """Declared key width in bytes; columns without catalog metadata count as 0."""

    return sum(columns[_norm(name)].width_bytes for name, _ in keys if _norm(name) in columns)


def _fit_key_width(
    candidate: _Candidate,
    columns: dict[str, ColumnInfo],
    selectivity: dict[tuple[str, str, str], ColumnSelectivity],
) -> None:
    """Move trailing key columns (never the first) to INCLUDE until the key fits.

    A wider key creates with only a warning, then fails INSERT or UPDATE with
    error 1946 once a row's key value exceeds the limit.
    """

    moved: list[str] = []
    while len(candidate.keys) > 1 and _key_width(candidate.keys, columns) > MAX_NONCLUSTERED_KEY_BYTES:
        name, _ = candidate.keys.pop()
        moved.insert(0, name)
    if not moved:
        return
    lowered = {_norm(name) for name in moved}
    candidate.includes = moved + [c for c in candidate.includes if _norm(c) not in lowered]
    candidate.notes.append("key_columns_moved_to_include_for_1700_byte_limit")
    # Selectivity was judged on the designed key; the moved columns may have
    # been the only selective ones.
    stats = [
        selectivity.get((_norm(candidate.schema), _norm(candidate.table), _norm(name)))
        for name in candidate.key_names
    ]
    if not candidate.low_selectivity and all(
        value is not None and 0 < float(value.distinct_estimate or 0.0) < LOW_SELECTIVITY_DISTINCT
        for value in stats
    ):
        candidate.low_selectivity = True
        candidate.notes.append("all_key_columns_low_selectivity")


def _key_eligible(columns: dict[str, ColumnInfo], column: str) -> bool:
    info = columns.get(_norm(column))
    return info is None or info.key_eligible


def _clustering_keys(existing: list[ExistingIndex]) -> tuple[str, ...]:
    for index in existing:
        if index.index_type_code == 1 and not index.is_disabled:
            return tuple(column.name for column in index.key_columns)
    return ()


def _hint_agrees(candidate: _Candidate, hint) -> bool:
    """True when the optimizer's own missing-index hint points at the same keys."""

    if not candidate.key_names:
        return False
    hint_keys = {_norm(column) for column in hint.equality + hint.inequality}
    if not hint_keys:
        return False
    available = {_norm(column) for column in candidate.key_names} | {
        _norm(column) for column in candidate.includes
    }
    return hint_keys <= available and _norm(candidate.key_names[0]) in hint_keys


# ---------------------------------------------------------------------------
# Merging
# ---------------------------------------------------------------------------


def _merge_candidates(
    candidates: list[_Candidate],
    columns: dict[str, ColumnInfo],
    settings: AdvisorSettings,
) -> list[_Candidate]:
    merged: list[_Candidate] = []
    for candidate in sorted(candidates, key=lambda c: -c.savings):
        target = None
        for existing in merged:
            if _prefix_compatible(candidate.keys, existing.keys):
                target = existing
                break
        if target is None:
            merged.append(candidate)
            continue
        longer = candidate.keys if len(candidate.keys) > len(target.keys) else target.keys
        combined_includes = list(target.includes)
        for column in candidate.includes:
            if _norm(column) not in {_norm(c) for c in combined_includes}:
                combined_includes.append(column)
        key_names = {_norm(name) for name, _ in longer}
        combined_includes = [c for c in combined_includes if _norm(c) not in key_names]
        if len(combined_includes) > settings.max_include_columns or _too_wide(
            combined_includes, columns
        ):
            merged.append(candidate)
            continue
        target.keys = list(longer)
        target.includes = combined_includes
        target.supports.extend(candidate.supports)
        target.kinds |= candidate.kinds
        target.savings += candidate.savings
        target.hint_agreement = target.hint_agreement or candidate.hint_agreement
        target.low_selectivity = target.low_selectivity and candidate.low_selectivity
        target.notes = list(dict.fromkeys(target.notes + candidate.notes))
    return merged


def _prefix_compatible(left: list[tuple[str, str]], right: list[tuple[str, str]]) -> bool:
    shorter, longer = (left, right) if len(left) <= len(right) else (right, left)
    if not shorter:
        return False
    for (name_a, dir_a), (name_b, dir_b) in zip(shorter, longer, strict=False):
        if _norm(name_a) != _norm(name_b) or dir_a != dir_b:
            return False
    return True


# ---------------------------------------------------------------------------
# Reconciliation with existing indexes
# ---------------------------------------------------------------------------


def _reconcile_with_existing(
    candidates: list[_Candidate],
    existing: list[ExistingIndex],
    columns: dict[str, ColumnInfo],
    info: TableInfo | None,
    writes: dict[str, float],
    inputs: AdvisorInputs,
    total: float,
    *,
    accesses: list[_Access],
    rate_days: float,
) -> list[dict[str, Any]]:
    """Turn merged candidates into create / extend / widen / already-covered advice.

    Candidates that land on the same existing index are merged first, so each
    existing index gets one recommendation whose DDL carries the final column set.
    """

    settings = inputs.settings
    rowstore = [
        index
        for index in existing
        if not index.is_disabled and index.index_type_code in {1, 2}
    ]
    clustering = _clustering_keys(existing)
    taken_names = {_norm(index.name) for index in existing}
    recommendations: list[dict[str, Any]] = []
    by_target: dict[tuple[str, str], tuple[ExistingIndex, _Candidate]] = {}
    creates: list[_Candidate] = []
    for candidate in candidates:
        _fit_key_width(candidate, columns, inputs.selectivity)
        if candidate.low_selectivity and len(candidate.supports) < 2:
            continue
        covering = next(
            (index for index in rowstore if _covers(index, candidate, clustering)),
            None,
        )
        if covering is not None:
            recommendations.append(
                _review_recommendation(
                    candidate.schema,
                    candidate.table,
                    covering.name,
                    "existing_index_already_covers_access",
                    (
                        f"{covering.name} already provides these keys and columns, yet the "
                        "stored plans did not use it. Check parameter sensitivity, stale "
                        "statistics, or implicit conversions with sql-optimizer before "
                        "adding anything."
                    ),
                    supports=candidate.supports,
                    total=total,
                )
            )
            continue
        extend_target = next(
            (
                index
                for index in rowstore
                if index.index_type_code == ROWSTORE_NONCLUSTERED
                and _same_keys_prefix(candidate.keys, index)
                and not index.is_primary_key
                and not index.is_unique_constraint
                # Plan accesses keep no constants, so the query cannot be
                # proven to satisfy a filter.
                and not index.filter_definition
            ),
            None,
        )
        if extend_target is not None:
            _group_on_target(by_target, ("extend_index", _norm(extend_target.name)), extend_target, candidate)
            continue
        widen_target = next(
            (
                index
                for index in rowstore
                if index.index_type_code == ROWSTORE_NONCLUSTERED
                and _is_proper_prefix_of_candidate(index, candidate.keys)
                and not index.is_unique
                and not index.is_primary_key
                and not index.is_unique_constraint
                and not index.filter_definition
            ),
            None,
        )
        if widen_target is not None:
            group_key = ("widen_index", _norm(widen_target.name))
            grouped = by_target.get(group_key)
            if grouped is None or _prefix_compatible(grouped[1].keys, candidate.keys):
                _group_on_target(by_target, group_key, widen_target, candidate)
                continue
        creates.append(candidate)

    for (action, _), (target, candidate) in by_target.items():
        rec = _change_existing_recommendation(
            action, target, candidate, columns, info, writes, settings, total, clustering,
            accesses=accesses, rate_days=rate_days,
        )
        if rec is not None:
            recommendations.append(rec)
        else:
            creates.append(candidate)
    for candidate in creates:
        name = _generated_name(candidate, taken_names)
        taken_names.add(_norm(name))
        recommendations.append(
            _create_recommendation(
                candidate, name, columns, info, writes, settings, total, rate_days=rate_days
            )
        )
    return recommendations


def _group_on_target(
    groups: dict[tuple[str, str], tuple[ExistingIndex, _Candidate]],
    key: tuple[str, str],
    target: ExistingIndex,
    candidate: _Candidate,
) -> None:
    grouped = groups.get(key)
    if grouped is None:
        groups[key] = (target, candidate)
        return
    merged = grouped[1]
    if len(candidate.keys) > len(merged.keys):
        merged.keys = list(candidate.keys)
    key_names = {_norm(name) for name, _ in merged.keys}
    for column in candidate.includes:
        lowered = _norm(column)
        if lowered not in key_names and lowered not in {_norm(c) for c in merged.includes}:
            merged.includes.append(column)
    merged.includes = [c for c in merged.includes if _norm(c) not in key_names]
    merged.supports.extend(candidate.supports)
    merged.kinds |= candidate.kinds
    merged.savings += candidate.savings
    merged.hint_agreement = merged.hint_agreement or candidate.hint_agreement
    merged.notes = list(dict.fromkeys(merged.notes + candidate.notes))


def _covers(index: ExistingIndex, candidate: _Candidate, clustering: tuple[str, ...]) -> bool:
    if index.filter_definition:
        return False
    index_keys = [(column.name, column.direction) for column in index.key_columns]
    if len(candidate.keys) > len(index_keys):
        return False
    for (name_a, dir_a), (name_b, dir_b) in zip(candidate.keys, index_keys, strict=False):
        if _norm(name_a) != _norm(name_b) or dir_a != dir_b:
            return False
    available = {_norm(name) for name, _ in index_keys} | {
        _norm(column) for column in index.include_columns
    }
    if index.index_type_code == 1:
        return True
    available |= {_norm(column) for column in clustering}
    return {_norm(column) for column in candidate.includes} <= available


def _same_keys_prefix(keys: list[tuple[str, str]], index: ExistingIndex) -> bool:
    index_keys = [(column.name, column.direction) for column in index.key_columns]
    if len(keys) > len(index_keys) or not keys:
        return False
    return all(
        _norm(a) == _norm(b) and da == db
        for (a, da), (b, db) in zip(keys, index_keys, strict=False)
    )


def _is_proper_prefix_of_candidate(index: ExistingIndex, keys: list[tuple[str, str]]) -> bool:
    index_keys = [(column.name, column.direction) for column in index.key_columns]
    if not index_keys or len(index_keys) >= len(keys):
        return False
    return all(
        _norm(a) == _norm(b) and da == db
        for (a, da), (b, db) in zip(index_keys, keys, strict=False)
    )


# ---------------------------------------------------------------------------
# Existing index findings
# ---------------------------------------------------------------------------


def _existing_index_findings(
    schema: str,
    table: str,
    existing: list[ExistingIndex],
    accesses: list[_Access],
    inputs: AdvisorInputs,
    uptime_days: float | None,
    total: float,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    used_in_workload = {
        _norm(item.access.index_name)
        for item in accesses
        if item.access.index_name
    }
    nonclustered = [
        index
        for index in existing
        if index.index_type_code == ROWSTORE_NONCLUSTERED and not index.is_hypothetical
    ]
    clustering = _clustering_keys(existing)
    columns = inputs.columns.get(_table_key(schema, table), {})
    pins = _index_pins(schema, table, accesses, inputs)
    # consumed: indexes that already have a finding. removed: indexes advised
    # for removal. survivors: indexes that absorb another one in this report,
    # so they are never consolidated themselves. rebuilt: survivors that get a
    # rebuild; a second rebuild from the catalogued definition would undo it.
    consumed: set[str] = set()
    removed: set[str] = set()
    survivors: set[str] = set()
    rebuilt: set[str] = set()

    def consolidate(redundant: ExistingIndex, survivor: ExistingIndex, reason: str) -> None:
        consumed.add(_norm(redundant.name))
        needed = _absorbed_columns(redundant, survivor, clustering)
        if needed and _norm(survivor.name) in rebuilt:
            findings.append(
                _consolidation_review(
                    schema,
                    table,
                    redundant,
                    survivor,
                    reason,
                    "survivor_rebuild_already_recommended",
                    f"Another recommendation already rebuilds {survivor.name}. Apply that "
                    f"change first, then re-run this review to consolidate {redundant.name}.",
                    needed,
                    accesses,
                    total,
                )
            )
            return
        rec = _consolidate_recommendation(
            schema, table, redundant, survivor, reason, accesses, total,
            needed=needed, columns=columns, settings=inputs.settings,
        )
        if rec["action"] == "consolidate_index":
            _apply_removal_dependencies(rec, pins.get(_norm(redundant.name)), inputs)
        findings.append(rec)
        if rec["action"] == "consolidate_index":
            removed.add(_norm(redundant.name))
            survivors.add(_norm(survivor.name))
            if needed:
                rebuilt.add(_norm(survivor.name))

    # Exact duplicates first: same keys (order and direction) and same filter.
    groups: dict[tuple[Any, ...], list[ExistingIndex]] = defaultdict(list)
    for index in nonclustered:
        if index.is_disabled:
            continue
        signature = (
            tuple((_norm(c.name), c.direction) for c in index.key_columns),
            (index.filter_definition or "").strip().casefold(),
        )
        groups[signature].append(index)
    for indexes in groups.values():
        if len(indexes) < 2:
            continue
        # A twin that a hint or forced plan names survives, so the cleanup keeps
        # its value without breaking that query.
        keeper = max(indexes, key=lambda index: _keeper_rank(index, pins))
        for duplicate in indexes:
            if duplicate is keeper or not _removable_into(duplicate, keeper):
                continue
            pin = pins.get(_norm(duplicate.name))
            if pin is not None and _norm(keeper.name) in pins:
                consumed.add(_norm(duplicate.name))
                findings.append(
                    _review_recommendation(
                        schema,
                        table,
                        duplicate.name,
                        "duplicate_pinned_by_hint_or_forced_plan",
                        f"{duplicate.name} has exactly the same keys as {keeper.name}, but queries "
                        "name both in hints or forced plans, so removing either one breaks a "
                        "query. First: " + " ".join(pin.prerequisites),
                    )
                )
                continue
            consolidate(duplicate, keeper, "exact_duplicate_keys")

    # Left-prefix redundancy: a narrower nonunique index whose keys lead a wider
    # one. The widest extension survives, so a survivor is never itself a
    # left prefix that gets consolidated later.
    for narrow in nonclustered:
        if (
            _norm(narrow.name) in consumed
            or _norm(narrow.name) in survivors
            or narrow.is_disabled
            or _constraint_protected(narrow)
        ):
            continue
        if narrow.filter_definition:
            continue
        extensions = [
            other
            for other in nonclustered
            if other is not narrow
            and not other.is_disabled
            and _norm(other.name) not in removed
            and not other.filter_definition
            and _removable_into(narrow, other)
            and len(other.key_columns) > len(narrow.key_columns)
            and all(
                _norm(a.name) == _norm(b.name) and a.direction == b.direction
                for a, b in zip(narrow.key_columns, other.key_columns, strict=False)
            )
        ]
        if not extensions:
            continue
        wider = max(extensions, key=lambda other: len(other.key_columns))
        consolidate(narrow, wider, "left_prefix_of_wider_index")

    # Unused nonclustered indexes. A survivor stays: it now serves the seeks of
    # the index consolidated into it.
    for index in nonclustered:
        if _norm(index.name) in consumed or _norm(index.name) in survivors:
            continue
        if index.is_disabled:
            findings.append(
                _review_recommendation(
                    schema,
                    table,
                    index.name,
                    "disabled_index",
                    "The index is disabled: it is not maintained and cannot be used. "
                    "Rebuild it if it is still needed, otherwise remove it through change control.",
                )
            )
            continue
        reads = _reads(index)
        if reads is None or reads > 0 or _norm(index.name) in used_in_workload:
            if reads is not None and reads > 0:
                updates = int(index.usage.get("user_updates") or 0)
                if updates > 100_000 and reads * 100 < updates:
                    findings.append(
                        _review_recommendation(
                            schema,
                            table,
                            index.name,
                            "write_heavy_low_read",
                            f"{updates} writes against {reads} reads since the counters reset. "
                            "Check whether a wider index can absorb its reads.",
                        )
                    )
            continue
        drop = _unused_drop_recommendation(
            schema, table, index, inputs, uptime_days, existing, removed,
            pin=pins.get(_norm(index.name)),
        )
        if drop is not None:
            findings.append(drop)
            if drop["action"] == "drop_index":
                removed.add(_norm(index.name))
    return findings


def _keeper_rank(index: ExistingIndex, pins: dict[str, _Pin]) -> tuple[int, int, int, int, int, int]:
    return (
        1 if _constraint_protected(index) else 0,
        1 if _norm(index.name) in pins else 0,
        len(_fk_support_ids(index)),
        1 if index.is_unique else 0,
        len(index.include_columns),
        _reads(index) or 0,
    )


def _constraint_protected(index: ExistingIndex) -> bool:
    """Protections that no other index can take over."""

    evidence = index.protection_evidence or {}
    return bool(
        index.is_primary_key
        or index.is_unique_constraint
        or index.is_unique
        or evidence.get("referenced_foreign_keys")
        or evidence.get("partition_switch_dependency")
        or evidence.get("indexed_view")
        or evidence.get("auto_created")
        or index.is_auto_created
    )


def _fk_support_ids(index: ExistingIndex) -> frozenset[int]:
    """Child foreign keys whose columns lead this index's keys.

    A filtered index supports none: the RI check seeks every child row of the
    parent key, and a filter cannot be proven to match that.
    """

    if index.filter_definition:
        return frozenset()
    evidence = index.protection_evidence or {}
    ids: set[int] = set()
    for support in evidence.get("child_foreign_key_support") or []:
        if not isinstance(support, dict) or support.get("leading_key_supported") is False:
            continue
        try:
            ids.add(int(support["foreign_key_id"]))
        except (KeyError, TypeError, ValueError):
            continue
    return frozenset(ids)


def _protection_complete(index: ExistingIndex) -> bool:
    return (index.protection_evidence or {}).get("coverage") == "complete"


def _removable_into(redundant: ExistingIndex, survivor: ExistingIndex) -> bool:
    """A redundant index may go only when the survivor keeps its FK support."""

    return not _constraint_protected(redundant) and _fk_support_ids(redundant) <= _fk_support_ids(
        survivor
    )


def _withhold_for_incomplete_protection(rec: dict[str, Any], index: ExistingIndex) -> None:
    """Fail closed: no executable removal DDL without complete protection metadata."""

    if _protection_complete(index):
        return
    rec["blockers"] = list(dict.fromkeys([*rec["blockers"], "protection_metadata_incomplete"]))
    rec["confidence"] = "low"
    rec["ddl"] = None
    rec["rollback_ddl"] = None


def _index_pins(
    schema: str, table: str, accesses: list[_Access], inputs: AdvisorInputs
) -> dict[str, _Pin]:
    """Indexes that a query names in a hint or reads through a forced Query Store plan.

    Dropping one fails the hinted statement with Msg 308; a forced plan that
    reads it fails forcing with NO_INDEX and silently falls back.
    """

    pins: dict[str, _Pin] = defaultdict(_Pin)

    def forced(query_id: int, plan_id: int, name: str) -> None:
        pins[_norm(name)].add(
            "forced_query_store_plan",
            f"Unforce plan {plan_id} for query {query_id} (sys.sp_query_store_unforce_plan), or "
            f"force a plan that does not read {name}, then re-run this review.",
        )

    for item in accesses:
        name = item.access.index_name
        if not name:
            continue
        if item.access.forced_index:
            pins[_norm(name)].add(
                "index_hint_in_plan",
                f"Remove or retarget the table hint that names {name} in query "
                f"{item.query.query_id} (plan {item.query.plan_id}), then re-run this review.",
            )
        if item.query.is_forced_plan:
            forced(item.query.query_id, item.query.plan_id, name)
    key = _table_key(schema, table)
    for query_id, plan_id, access_schema, access_table, name in inputs.forced_plan_accesses:
        if _table_key(access_schema, access_table) == key:
            forced(query_id, plan_id, name)
    for (hint_schema, hint_table, name), places in inputs.index_hint_references.items():
        if _table_key(hint_schema, hint_table) != key:
            continue
        for place in places:
            pins[_norm(name)].add(
                "index_named_in_hint_plan_guide_or_module",
                f"Remove or retarget the index hint in {place}, then re-run this review.",
            )
    return dict(pins)


def _apply_removal_dependencies(
    rec: dict[str, Any], pin: _Pin | None, inputs: AdvisorInputs
) -> None:
    """Hint and forced-plan dependencies of an index advised for removal.

    A pinned index keeps its action so the intended end state stays visible, but
    gets no executable DDL until the prerequisites are done (fail closed).
    """

    rec.setdefault("prerequisites", [])
    if pin is not None:
        rec["reason_codes"] = list(dict.fromkeys([*rec["reason_codes"], *pin.reasons]))
        rec["blockers"] = list(dict.fromkeys([*rec["blockers"], PIN_BLOCKER]))
        rec["prerequisites"] = list(pin.prerequisites)
        rec["confidence"] = "low"
        rec["ddl"] = None
        rec["rollback_ddl"] = None
        return
    if inputs.hint_coverage == "unresolved":
        rec["blockers"] = list(dict.fromkeys([*rec["blockers"], UNRESOLVED_HINT_BLOCKER]))
        rec["prerequisites"] = [
            "Find the index hints listed in gaps that match no single index, confirm that "
            f"none of them names {rec['index_name']} (or retarget them), then re-run this review."
        ]
        rec["confidence"] = "low"
        rec["ddl"] = None
        rec["rollback_ddl"] = None
    incomplete = []
    if inputs.forced_plan_coverage != "complete":
        incomplete.append("forced_plan_dependency_check_incomplete")
    if inputs.hint_coverage not in {"complete", "unresolved"}:
        incomplete.append("hint_reference_check_incomplete")
    if incomplete:
        rec["blockers"] = list(dict.fromkeys([*rec["blockers"], *incomplete]))
        if rec["confidence"] == "high":
            rec["confidence"] = "medium"


def _protection_reasons(index: ExistingIndex) -> list[str]:
    evidence = index.protection_evidence or {}
    reasons = []
    if index.is_primary_key:
        reasons.append("primary_key")
    if index.is_unique_constraint:
        reasons.append("unique_constraint")
    if index.is_unique and not (index.is_primary_key or index.is_unique_constraint):
        reasons.append("enforces_uniqueness")
    if evidence.get("child_foreign_key_support"):
        reasons.append("supports_foreign_key")
    if evidence.get("referenced_foreign_keys"):
        reasons.append("referenced_by_foreign_key")
    if evidence.get("partition_switch_dependency"):
        reasons.append("partition_switch_dependency")
    if evidence.get("auto_created") or index.is_auto_created:
        reasons.append("automatic_tuning_created")
    return reasons


def _reads(index: ExistingIndex) -> int | None:
    values = [index.usage.get(metric) for metric in ("user_seeks", "user_scans", "user_lookups")]
    if all(value is None for value in values):
        return 0 if index.usage else None
    return sum(int(value or 0) for value in values)


def _unused_drop_recommendation(
    schema: str,
    table: str,
    index: ExistingIndex,
    inputs: AdvisorInputs,
    uptime_days: float | None,
    existing: list[ExistingIndex],
    removed: set[str],
    *,
    pin: _Pin | None,
) -> dict[str, Any] | None:
    if index.filter_definition:
        return _review_recommendation(
            schema,
            table,
            index.name,
            "unused_filtered_index",
            "No reads since the counters reset, but filtered indexes often serve rare, "
            "valuable queries. Confirm with the application owner before any change.",
        )
    if _constraint_protected(index):
        return None
    other_support: set[int] = set()
    for other in existing:
        if (
            other is not index
            and not other.is_disabled
            and other.index_type_code in {1, 2}
            and _norm(other.name) not in removed
        ):
            other_support |= _fk_support_ids(other)
    if not _fk_support_ids(index) <= other_support:
        return _review_recommendation(
            schema,
            table,
            index.name,
            "sole_foreign_key_support",
            "No reads since the counters reset, but it is the only index that supports "
            "a foreign key. Without it, deletes and key updates on the parent table scan "
            "this table. Keep it.",
        )
    key = (_norm(schema), _norm(table), _norm(index.name))
    references = inputs.index_plan_references.get(key)
    _, query_store_days = _query_store_window(inputs)
    # The days of Query Store plans the reference check could see.
    reference_days = (
        query_store_days if query_store_days is not None else float(inputs.settings.lookback_days)
    )
    blockers: list[str] = []
    reason_codes = ["no_reads_since_counter_reset"]
    if references is None:
        blockers.append("query_store_reference_check_unavailable")
    elif references > 0:
        return _review_recommendation(
            schema,
            table,
            index.name,
            "unused_by_dmv_but_in_query_store_plans",
            f"Usage counters show no reads, but {references} Query Store plan(s) in the "
            "window reference it. Keep it; the counters were probably reset.",
        )
    elif reference_days >= MIN_REFERENCE_WINDOW_DAYS:
        reason_codes.append("no_query_store_plan_reference_in_window")
    if query_store_days is None:
        blockers.append("query_store_window_coverage_unknown")
    statistics = inputs.index_statistics_references.get(key)
    statistics_used = bool(statistics)
    if statistics is None:
        blockers.append("statistics_reference_check_unavailable")
    elif statistics_used:
        reason_codes.append("index_statistics_used_by_optimizer")
    capture_mode = str(inputs.query_store.get("query_capture_mode") or "").upper()
    if capture_mode and capture_mode != "ALL":
        blockers.append(f"query_store_capture_mode_{capture_mode.lower()}_may_miss_rare_queries")
    if uptime_days is None:
        blockers.append("usage_counter_age_unknown")
    elif uptime_days < UNUSED_MIN_UPTIME_DAYS:
        return _review_recommendation(
            schema,
            table,
            index.name,
            "unused_but_counters_recent",
            f"No reads, but usage counters reset {uptime_days:.1f} day(s) ago. Re-check "
            f"after at least {UNUSED_MIN_UPTIME_DAYS} days, ideally a full business cycle.",
        )
    # Plan references should cover as much of the removal window as the counters do.
    usage_days = float(UNUSED_HIGH_CONFIDENCE_DAYS)
    if uptime_days is not None:
        usage_days = min(uptime_days, usage_days)
    if reference_days < usage_days:
        blockers.append("query_store_reference_window_shorter_than_usage_window")
    hard = [
        blocker
        for blocker in blockers
        if blocker not in _SOFT_REMOVAL_BLOCKERS and not blocker.startswith("query_store_capture_mode")
    ]
    if hard:
        confidence = "low"
    elif (
        uptime_days is not None
        and uptime_days >= UNUSED_HIGH_CONFIDENCE_DAYS
        and not blockers
        and not statistics_used
    ):
        confidence = "high"
    else:
        confidence = "medium"
    updates = int(index.usage.get("user_updates") or 0)
    size_mb = _index_size_mb(index)
    rendered = render_reverse_index_ddl(index)
    executable = rendered.get("executable") is True
    if not executable:
        blockers.extend(rendered.get("blockers") or [])
        confidence = "low"
    ddl = rendered.get("drop_ddl") if executable else None
    rollback_ddl = rendered.get("ddl") if executable else None
    risks = [
        "Rare jobs (month-end, quarter-end) may need it; confirm a full business cycle.",
        "Index hints or plan guides that name it will fail after removal.",
    ]
    if statistics_used:
        # Auto-created statistics would bring back only a sampled single-column
        # histogram, not this index's multi-column density or full-scan histogram.
        statistics_name = quote_identifier(_replacement_statistics_name(index.name))
        table_sql = f"{quote_identifier(index.schema)}.{quote_identifier(index.table)}"
        columns_sql = ", ".join(quote_identifier(column.name) for column in index.key_columns)
        if ddl is not None and rollback_ddl is not None:
            ddl = (
                f"CREATE STATISTICS {statistics_name} ON {table_sql} ({columns_sql}) "
                f"WITH FULLSCAN, PERSIST_SAMPLE_PERCENT = ON;\n{ddl}"
            )
            rollback_ddl = f"{rollback_ddl}\nDROP STATISTICS {table_sql}.{statistics_name};"
        risks.append(
            f"{statistics} Query Store plan(s) read this index's statistics. The replacement "
            "statistics scan the whole table WITH FULLSCAN; on a very large table use SAMPLE n "
            "PERCENT instead and keep PERSIST_SAMPLE_PERCENT = ON."
        )
    rationale = (
        f"No seeks, scans, or lookups in {uptime_days:.0f} day(s) of usage counters"
        if uptime_days is not None
        else "No seeks, scans, or lookups since the usage counters reset"
    )
    rationale += f", {updates} write(s) maintained it"
    if size_mb is not None:
        rationale += f", and it occupies {size_mb} MB"
    rationale += ". Removing it saves write and storage cost; keep the rollback ready."
    rec = {
        "action": "drop_index",
        "schema": schema,
        "table": table,
        "index_name": index.name,
        "target_index": index.name,
        "key_columns": [column.as_dict() for column in index.key_columns],
        "include_columns": list(index.include_columns),
        "supporting_queries": [],
        "workload_share_pct": 0.0,
        "estimated_max_benefit_pct": None,
        "estimated_size_mb": size_mb,
        "write_impact": {"user_updates_since_reset": updates},
        "score": min(50.0, updates / 10_000) + (size_mb or 0) / 1000,
        "confidence": confidence,
        "reason_codes": reason_codes,
        "blockers": blockers,
        "rationale": rationale,
        "risks": risks,
        "ddl": ddl,
        "rollback_ddl": rollback_ddl,
        "reference_window_days": round(reference_days, 2),
        "validation": {
            "before_change": "Confirm no hint, plan guide, or forced plan names the index.",
            "after_change": "Watch Query Store for regressions on queries that touch this table.",
        },
        "evidence_sources": ["index_usage_dmv", "query_store_plan_reference_check"],
    }
    _withhold_for_incomplete_protection(rec, index)
    _apply_removal_dependencies(rec, pin, inputs)
    return rec


def _replacement_statistics_name(index_name: str) -> str:
    name = "st_" + index_name
    if len(name) > 128:
        digest = hashlib.sha256(index_name.encode("utf-8")).hexdigest()[:8]
        name = name[:119] + "_" + digest
    return name


def _absorbed_columns(
    redundant: ExistingIndex, survivor: ExistingIndex, clustering: tuple[str, ...]
) -> list[str]:
    """Columns of the redundant index that the survivor does not already carry."""

    present = (
        {_norm(c.name) for c in survivor.key_columns}
        | {_norm(c) for c in survivor.include_columns}
        | {_norm(c) for c in clustering}
    )
    needed: list[str] = []
    for column in list(redundant.include_columns) + [c.name for c in redundant.key_columns]:
        if _norm(column) not in present:
            present.add(_norm(column))
            needed.append(column)
    return needed


def _consolidation_review(
    schema: str,
    table: str,
    redundant: ExistingIndex,
    survivor: ExistingIndex,
    reason: str,
    review_reason: str,
    detail: str,
    needed: list[str],
    accesses: list[_Access],
    total: float,
) -> dict[str, Any]:
    description = (
        "has exactly the same keys as" if reason == "exact_duplicate_keys" else "is a left prefix of"
    )
    uses = [item for item in accesses if _norm(item.access.index_name or "") == _norm(redundant.name)]
    reads = _reads(redundant)
    return _review_recommendation(
        schema,
        table,
        redundant.name,
        review_reason,
        (
            f"{redundant.name} {description} {survivor.name}, but it also carries "
            f"{', '.join(needed)}, which {survivor.name} does not. {detail} Reads on "
            f"{redundant.name} since the counters reset: {reads if reads is not None else 'unknown'}."
        ),
        supports=uses,
        total=total,
    )


def _consolidate_recommendation(
    schema: str,
    table: str,
    redundant: ExistingIndex,
    survivor: ExistingIndex,
    reason: str,
    accesses: list[_Access],
    total: float,
    *,
    needed: list[str],
    columns: dict[str, ColumnInfo],
    settings: AdvisorSettings,
) -> dict[str, Any]:
    survivor_includes = list(survivor.include_columns) + list(needed)
    survivor_change: dict[str, Any] | None = None
    if needed:
        if survivor.is_primary_key or survivor.is_unique_constraint:
            return _consolidation_review(
                schema, table, redundant, survivor, reason,
                "covering_twin_of_constraint_index",
                f"{survivor.name} enforces a constraint and cannot take INCLUDE columns, so "
                f"{redundant.name} is the covering index for these keys. Keep it.",
                needed, accesses, total,
            )
        if (
            survivor.index_type_code != ROWSTORE_NONCLUSTERED
            or len(survivor_includes) > settings.max_include_columns
            or _too_wide(survivor_includes, columns)
        ):
            return _consolidation_review(
                schema, table, redundant, survivor, reason,
                "survivor_cannot_absorb_includes",
                f"Adding them to {survivor.name} would make it too wide. Keep "
                f"{redundant.name}, or redesign both indexes together.",
                needed, accesses, total,
            )
        changed = replace(survivor, include_columns=tuple(survivor_includes))
        survivor_change = _drop_existing_ddl(changed, survivor)
        if survivor_change.get("blockers"):
            return _consolidation_review(
                schema, table, redundant, survivor, reason,
                "survivor_cannot_absorb_includes",
                f"The rebuild of {survivor.name} cannot be rendered from catalog metadata "
                f"({', '.join(survivor_change['blockers'])}). Keep {redundant.name}.",
                needed, accesses, total,
            )
    rendered = render_reverse_index_ddl(redundant)
    reads = _reads(redundant)
    uses = [item for item in accesses if _norm(item.access.index_name or "") == _norm(redundant.name)]
    confidence = "high" if reason == "exact_duplicate_keys" else "medium"
    blockers = []
    if rendered.get("executable") is not True:
        blockers.extend(rendered.get("blockers") or [])
        confidence = "low"
    if uses and reason == "left_prefix_of_wider_index":
        blockers.append("narrow_index_used_in_workload_scans_may_prefer_it")
        confidence = "low"
    ddl_parts = []
    if survivor_change and survivor_change.get("ddl"):
        ddl_parts.append(survivor_change["ddl"])
    if rendered.get("executable"):
        ddl_parts.append(rendered["drop_ddl"])
    rollback_parts = []
    if rendered.get("executable"):
        rollback_parts.append(rendered["ddl"])
    if survivor_change and survivor_change.get("rollback_ddl"):
        rollback_parts.append(survivor_change["rollback_ddl"])
    description = (
        "has exactly the same keys as" if reason == "exact_duplicate_keys" else "is a left prefix of"
    )
    rec = {
        "action": "consolidate_index",
        "schema": schema,
        "table": table,
        "index_name": redundant.name,
        "target_index": redundant.name,
        "merge_into": survivor.name,
        "key_columns": [c.as_dict() for c in survivor.key_columns],
        "include_columns": survivor_includes,
        "supporting_queries": _supporting_queries(uses, total),
        "workload_share_pct": _pct(sum(item.attributed for item in uses), total),
        "estimated_max_benefit_pct": None,
        "estimated_size_mb": _index_size_mb(redundant),
        "write_impact": {"user_updates_since_reset": int(redundant.usage.get("user_updates") or 0)},
        "score": 20.0 + (_index_size_mb(redundant) or 0) / 1000,
        "confidence": confidence,
        "reason_codes": [reason],
        "blockers": blockers,
        "rationale": (
            f"{redundant.name} {description} {survivor.name}; the wider index serves its seeks"
            + (f" once it also includes {', '.join(needed)}" if needed else "")
            + f". Reads on {redundant.name} since the counters reset: {reads if reads is not None else 'unknown'}."
        ),
        "risks": ["Hints or plan guides that name the redundant index will fail after removal."],
        "ddl": "\n".join(ddl_parts) if ddl_parts else None,
        "rollback_ddl": "\n".join(rollback_parts) if rollback_parts else None,
        "validation": {
            "before_change": "Confirm no hint, plan guide, or forced plan names the redundant index.",
            "after_change": (
                "Confirm queries that used it now seek the surviving index. Roll back if CPU per "
                f"execution of any query in regression_set rises more than {ROLLBACK_CPU_INCREASE_PCT}% "
                "over equal windows."
            ),
            "regression_set": _query_shares(uses, total),
        },
        "evidence_sources": ["index_definitions", "index_usage_dmv"],
    }
    _withhold_for_incomplete_protection(rec, redundant)
    return rec


def _heap_recommendation(
    schema: str,
    table: str,
    info: TableInfo | None,
    accesses: list[_Access],
    existing: list[ExistingIndex],
    total: float,
) -> dict[str, Any] | None:
    if info is None or not info.is_heap:
        return None
    if any(index.index_type_code in {1, 5} for index in existing):
        return None
    rid = [item for item in accesses if item.access.operation == "rid_lookup"]
    heap_scans = [item for item in accesses if item.access.operation == "heap_scan"]
    forwarded = info.forwarded_fetches or 0
    if not rid and not heap_scans and forwarded == 0:
        return None
    primary = next((index for index in existing if index.is_primary_key), None)
    unique = next((index for index in existing if index.is_unique and not index.filter_definition), None)
    basis = primary or unique
    reason_codes = []
    if rid:
        reason_codes.append("rid_lookups_in_workload")
    if heap_scans:
        reason_codes.append("heap_scans_in_workload")
    if forwarded:
        reason_codes.append("forwarded_record_fetches")
    share = _pct(sum(item.attributed for item in rid + heap_scans), total)
    if basis is not None:
        key_text = ", ".join(c.name for c in basis.key_columns)
        rationale = (
            f"{schema}.{table} is a heap. {basis.name} is already unique on ({key_text}); "
            "making that key clustered removes RID lookups and forwarded records."
        )
        keys = [c.as_dict() for c in basis.key_columns]
    else:
        rationale = (
            f"{schema}.{table} is a heap with RID lookups, scans, or forwarded records. Choose a "
            "narrow, unique, ever-increasing key (often an identity column) for a clustered index."
        )
        keys = []
    return {
        "action": "create_clustered_index",
        "schema": schema,
        "table": table,
        "index_name": None,
        "target_index": basis.name if basis else None,
        "key_columns": keys,
        "include_columns": [],
        "supporting_queries": _supporting_queries(rid + heap_scans, total),
        "workload_share_pct": share,
        "estimated_max_benefit_pct": None,
        "estimated_size_mb": info.size_mb,
        "write_impact": {},
        "score": 10.0 + (share or 0.0),
        "confidence": "medium" if (rid or heap_scans) else "low",
        "reason_codes": reason_codes,
        "blockers": ["clustered_key_choice_requires_design_review"] if basis is None else [],
        "rationale": rationale,
        "risks": [
            "Creating a clustered index rebuilds every nonclustered index on the table; "
            "schedule it and size the log.",
        ],
        "ddl": None,
        "rollback_ddl": None,
        "validation": {
            "before_change": "Size the rebuild and the transaction log; prefer ONLINE = ON.",
            "after_change": "Confirm RID lookups and forwarded fetches disappear.",
        },
        "evidence_sources": ["query_store_plan_access", "index_operational_stats"],
    }


def _foreign_key_findings(
    schema: str,
    table: str,
    existing: list[ExistingIndex],
    inputs: AdvisorInputs,
    writes: dict[tuple[str, str], dict[str, float]],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    key = _table_key(schema, table)
    for fk in inputs.foreign_keys:
        if _table_key(fk.schema, fk.table) != key or fk.is_disabled:
            continue
        fk_columns = [_norm(c) for c in fk.columns]
        supported = any(
            sorted(_norm(c.name) for c in index.key_columns[: len(fk_columns)]) == sorted(fk_columns)
            for index in existing
            if not index.is_disabled
            and index.index_type_code in {1, 2}
            and not index.filter_definition
        )
        if supported:
            continue
        parent_writes = writes.get(_table_key(fk.referenced_schema, fk.referenced_table))
        parent_dml = bool(parent_writes and parent_writes.get("dml_statements"))
        findings.append(
            _review_recommendation(
                schema,
                table,
                None,
                "foreign_key_without_supporting_index",
                (
                    f"Foreign key {fk.name} ({', '.join(fk.columns)}) to "
                    f"{fk.referenced_schema}.{fk.referenced_table} has no unfiltered index leading with its "
                    "columns. Deletes or key updates on the parent scan this table"
                    + (" and the workload modifies the parent." if parent_dml else ".")
                ),
            )
        )
    return findings


# ---------------------------------------------------------------------------
# Rendering recommendations
# ---------------------------------------------------------------------------


def _create_recommendation(
    candidate: _Candidate,
    name: str,
    columns: dict[str, ColumnInfo],
    info: TableInfo | None,
    writes: dict[str, float],
    settings: AdvisorSettings,
    total: float,
    *,
    rate_days: float,
) -> dict[str, Any]:
    keys_sql = ", ".join(f"{quote_identifier(n)} {d}" for n, d in candidate.keys)
    include_sql = (
        " INCLUDE (" + ", ".join(quote_identifier(c) for c in candidate.includes) + ")"
        if candidate.includes
        else ""
    )
    table_sql = f"{quote_identifier(candidate.schema)}.{quote_identifier(candidate.table)}"
    ddl = (
        f"CREATE NONCLUSTERED INDEX {quote_identifier(name)} ON {table_sql} "
        f"({keys_sql}){include_sql} WITH (ONLINE = ON);"
    )
    rollback = f"DROP INDEX {quote_identifier(name)} ON {table_sql};"
    rec = _candidate_recommendation(
        "create_index",
        candidate,
        name=name,
        target=None,
        keys=candidate.keys,
        includes=candidate.includes,
        ddl=ddl,
        rollback=rollback,
        columns=columns,
        info=info,
        writes=writes,
        settings=settings,
        total=total,
        write_weight=0.5,
        rate_days=rate_days,
    )
    # _fit_key_width leaves an over-limit key only when the first column alone exceeds it.
    if _key_width(candidate.keys, columns) > MAX_NONCLUSTERED_KEY_BYTES:
        rec["blockers"] = list(rec["blockers"]) + ["key_width_exceeds_1700_bytes"]
        rec["confidence"] = "low"
        rec["ddl"] = None
        rec["rollback_ddl"] = None
        rec["risks"] = list(rec["risks"]) + [
            f"The declared key is wider than {MAX_NONCLUSTERED_KEY_BYTES} bytes: the index would "
            "create with a warning, then INSERT or UPDATE fails with error 1946 for any row whose "
            "key value exceeds the limit. Narrow the column type, or index a persisted computed "
            "hash of the column instead."
        ]
    return rec


def _change_existing_recommendation(
    action: str,
    target: ExistingIndex,
    candidate: _Candidate,
    columns: dict[str, ColumnInfo],
    info: TableInfo | None,
    writes: dict[str, float],
    settings: AdvisorSettings,
    total: float,
    clustering: tuple[str, ...],
    *,
    accesses: list[_Access],
    rate_days: float,
) -> dict[str, Any] | None:
    if action == "extend_index":
        new_keys = [(c.name, c.direction) for c in target.key_columns]
    else:
        new_keys = list(candidate.keys)
    key_names = {_norm(n) for n, _ in new_keys}
    implicit = {_norm(c) for c in clustering}
    includes = [c for c in target.include_columns if _norm(c) not in key_names]
    for column in candidate.includes:
        lowered = _norm(column)
        if lowered in key_names or lowered in implicit or lowered in {_norm(c) for c in includes}:
            continue
        includes.append(column)
    if len(includes) > settings.max_include_columns or _too_wide(includes, columns):
        return None
    changed = replace(
        target,
        key_columns=tuple(IndexKeyColumn(n, d) for n, d in new_keys),
        include_columns=tuple(includes),
    )
    rendered = _drop_existing_ddl(changed, target)
    rec = _candidate_recommendation(
        action,
        candidate,
        name=target.name,
        target=target.name,
        keys=new_keys,
        includes=includes,
        ddl=rendered.get("ddl"),
        rollback=rendered.get("rollback_ddl"),
        columns=columns,
        info=info,
        writes=writes,
        settings=settings,
        total=total,
        write_weight=0.25 if action == "extend_index" else 0.35,
        rate_days=rate_days,
    )
    if rendered.get("blockers"):
        rec["blockers"] = list(rec["blockers"]) + list(rendered["blockers"])
        rec["confidence"] = "low"
    # Other queries that read the target lose or change their index on the rebuild.
    supporting = {item.query.query_id for item in candidate.supports}
    regression = _query_shares(
        [
            item
            for item in accesses
            if _norm(item.access.index_name or "") == _norm(target.name)
            and item.query.query_id not in supporting
        ],
        total,
    )
    validation = rec["validation"]
    validation["regression_set"] = regression
    if regression:
        validation["sandbox"] += (
            " Queries in regression_set read the index this rebuild changes; benchmark them "
            "against the new definition too."
        )
        validation["after_change"] = _after_change_text(with_regression=True)
    return rec


def _drop_existing_ddl(changed: ExistingIndex, original: ExistingIndex) -> dict[str, Any]:
    forward = render_reverse_index_ddl(changed)
    backward = render_reverse_index_ddl(original)
    if forward.get("executable") is not True or backward.get("executable") is not True:
        return {
            "ddl": None,
            "rollback_ddl": None,
            "blockers": list(
                dict.fromkeys((forward.get("blockers") or []) + (backward.get("blockers") or []))
            ),
        }
    return {
        "ddl": _with_drop_existing(forward["ddl"]),
        "rollback_ddl": _with_drop_existing(backward["ddl"]),
        "blockers": [],
    }


def _with_drop_existing(ddl: str) -> str:
    """Rebuild in place, online, in one statement.

    The rendered CREATE already carries the exact per-partition compression.
    """

    return ddl.replace("\nWITH (", "\nWITH (DROP_EXISTING = ON, ONLINE = ON, ", 1)


def _candidate_recommendation(
    action: str,
    candidate: _Candidate,
    *,
    name: str | None,
    target: str | None,
    keys: list[tuple[str, str]],
    includes: list[str],
    ddl: str | None,
    rollback: str | None,
    columns: dict[str, ColumnInfo],
    info: TableInfo | None,
    writes: dict[str, float],
    settings: AdvisorSettings,
    total: float,
    write_weight: float,
    rate_days: float,
) -> dict[str, Any]:
    supports = candidate.supports
    query_ids = {item.query.query_id for item in supports}
    share = _pct(sum(item.attributed for item in supports), total)
    benefit = _pct(candidate.savings, total)
    rows_per_day = float(writes.get("rows", 0.0)) / rate_days
    table_rows = float(info.row_count) if info and info.row_count else None
    churn = min(1.0, rows_per_day / table_rows) if table_rows else 0.0
    penalty = round(min(0.9, write_weight * churn * 4), 3)
    size_mb = _estimate_size_mb(keys, includes, columns, info)
    score = round((benefit or 0.0) * (1 - penalty), 4)
    # Only the requested window exempts a query from a second active day; a thin
    # Query Store history is less evidence, never more.
    recurring = any(item.query.active_days >= 2 for item in supports) or settings.lookback_days < 2
    proof_set = _proof_set(supports)
    if (len(query_ids) >= 2 or (share or 0) >= 5.0) and recurring and not candidate.low_selectivity:
        confidence = "high"
    elif (share or 0) >= 0.5 or len(query_ids) >= 2:
        confidence = "medium"
    else:
        confidence = "low"
    reason_codes = sorted(candidate.kinds)
    if candidate.hint_agreement:
        reason_codes.append("optimizer_missing_index_hint_agrees")
    reason_codes.extend(candidate.notes)
    blockers: list[str] = []
    if ddl is None:
        blockers.append("ddl_not_renderable_from_catalog_metadata")
    rationale = _candidate_rationale(action, candidate, keys, includes, target, share, query_ids)
    return {
        "action": action,
        "schema": candidate.schema,
        "table": candidate.table,
        "index_name": name,
        "target_index": target,
        "key_columns": [{"name": n, "direction": d} for n, d in keys],
        "include_columns": list(includes),
        "supporting_queries": _supporting_queries(supports, total),
        "workload_share_pct": share,
        "estimated_max_benefit_pct": benefit,
        "estimated_max_size_mb": size_mb,
        "write_impact": {
            "dml_rows_per_day": round(rows_per_day, 1),
            "table_churn_per_day": round(churn, 4),
            "penalty": penalty,
        },
        "score": score,
        "confidence": confidence,
        "reason_codes": reason_codes,
        "blockers": blockers,
        "prerequisites": _computed_column_prerequisites(candidate, keys, includes, columns),
        "rationale": rationale,
        "risks": _candidate_risks(action, candidate, penalty),
        "ddl": ddl,
        "rollback_ddl": rollback,
        "validation": {
            "sandbox": (
                "Prove it on a non-production copy with sql-optimizer: start a tuning session "
                f"for query {proof_set[0]['query_id'] if proof_set else 'n/a'} and run "
                "benchmark_index_candidate with these keys and includes. Every query in "
                f"proof_set ({', '.join(str(item['query_id']) for item in proof_set) or 'n/a'}) "
                "must pass."
            ),
            "proof_set": proof_set,
            "after_change": _after_change_text(with_regression=False),
        },
        "evidence_sources": ["query_store_plan_access", "query_store_runtime_stats"]
        + (["missing_index_hint"] if candidate.hint_agreement else []),
        "_supports": list(supports),
    }


def _computed_column_prerequisites(
    candidate: _Candidate,
    keys: list[tuple[str, str]],
    includes: list[str],
    columns: dict[str, ColumnInfo],
) -> list[str]:
    """Writer SET options an index on a computed column (key or INCLUDE) depends on.

    A column without catalog metadata may be computed too, so it gets a check
    step; an empty list means every column was read and none is computed.
    """

    names = list(dict.fromkeys([n for n, _ in keys] + list(includes)))
    computed = [name for name in names if (info := columns.get(_norm(name))) and info.is_computed]
    unread = [name for name in names if _norm(name) not in columns]
    table = f"{candidate.schema}.{candidate.table}"
    options = (
        "ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL and "
        "QUOTED_IDENTIFIER ON and NUMERIC_ROUNDABORT OFF"
    )
    steps: list[str] = []
    if computed:
        shown = ", ".join(f"{table}.{name}" for name in computed)
        steps.append(
            f"This index uses computed column(s) {shown}. Every session and module that runs INSERT, "
            f"UPDATE, DELETE or MERGE against {table} must have {options}, or the statement fails "
            "with Msg 1934 once the index exists; the session that runs this DDL needs them too. "
            "ARITHABORT ON is implied by ANSI_WARNINGS ON at every Azure SQL Database compatibility "
            "level, so connections that leave ARITHABORT OFF are fine. Before you create it, find "
            "the modules that write the table and were created with uses_quoted_identifier = 0 or "
            "uses_ansi_nulls = 0 in sys.sql_modules, and the clients that set QUOTED_IDENTIFIER, "
            "ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS or CONCAT_NULL_YIELDS_NULL OFF or "
            "NUMERIC_ROUNDABORT ON (ODBC sqlcmd without -I runs with QUOTED_IDENTIFIER OFF)."
        )
    if unread:
        shown = ", ".join(f"{table}.{name}" for name in unread)
        steps.append(
            f"Column metadata for {shown} was not read, so the advisor cannot tell whether any of "
            "them is a computed column. Before you create this index, check sys.columns.is_computed "
            f"for them. If one is computed, every session and module that writes {table} must have "
            f"{options}, or its INSERT, UPDATE, DELETE or MERGE fails with Msg 1934."
        )
    return steps


def _candidate_rationale(
    action: str,
    candidate: _Candidate,
    keys: list[tuple[str, str]],
    includes: list[str],
    target: str | None,
    share: float | None,
    query_ids: set[int],
) -> str:
    key_text = ", ".join(f"{n}{' DESC' if d == 'DESC' else ''}" for n, d in keys)
    kinds = candidate.kinds
    reasons = []
    if "replace_eager_spool" in kinds:
        reasons.append("replaces the eager index spool the optimizer builds on every execution")
    if "scan_to_seek" in kinds:
        reasons.append("turns scans with filters into seeks")
    if "extend_seek" in kinds:
        reasons.append("moves residual filters into the seek")
    if "cover_lookup" in kinds:
        reasons.append("removes key or RID lookups")
    if not reasons:
        joined = "serves the workload's access pattern"
    elif len(reasons) == 1:
        joined = reasons[0]
    else:
        joined = ", ".join(reasons[:-1]) + ", and " + reasons[-1]
    lead = {
        "create_index": f"A new index on ({key_text})",
        "extend_index": f"Adding include columns to {target}",
        "widen_index": f"Widening {target} to ({key_text})",
    }.get(action, f"Index ({key_text})")
    text = (
        f"{lead} {joined} for {len(query_ids)} quer{'y' if len(query_ids) == 1 else 'ies'} "
        f"that account for about {share or 0:.2f}% of the analysed workload."
    )
    if includes:
        text += f" Includes: {', '.join(includes)}."
    if candidate.partial_covering:
        text += " Full covering was skipped because the query needs most of the row."
    return text


def _candidate_risks(action: str, candidate: _Candidate, penalty: float) -> list[str]:
    risks = []
    if action == "create_index":
        risks.append("Every insert and delete, and updates to key or included columns, maintain the new index.")
    if action in {"extend_index", "widen_index"}:
        risks.append(
            "Rebuilding the existing index is one online DROP_EXISTING statement that keeps "
            "its current compression; it takes time, log space, and room for a second copy "
            "of the index while it runs."
        )
    if penalty >= 0.3:
        risks.append("The table is write-heavy relative to its size; weigh the write cost.")
    if candidate.low_selectivity:
        risks.append("Key columns have few distinct values; the optimizer may still prefer a scan.")
    risks.append("Validate on a non-production copy before change control; estimates come from the optimizer cost model.")
    return risks


def _review_recommendation(
    schema: str,
    table: str,
    index_name: str | None,
    reason: str,
    rationale: str,
    *,
    supports: list[_Access] | None = None,
    total: float = 0.0,
) -> dict[str, Any]:
    supports = supports or []
    return {
        "action": "review_index",
        "schema": schema,
        "table": table,
        "index_name": index_name,
        "target_index": index_name,
        "key_columns": [],
        "include_columns": [],
        "supporting_queries": _supporting_queries(supports, total),
        "workload_share_pct": _pct(sum(item.attributed for item in supports), total),
        "estimated_max_benefit_pct": None,
        "estimated_size_mb": None,
        "write_impact": {},
        "score": 0.0,
        "confidence": "low",
        "reason_codes": [reason],
        "blockers": [],
        "rationale": rationale,
        "risks": [],
        "ddl": None,
        "rollback_ddl": None,
        "validation": {},
        "evidence_sources": [],
    }


def _supporting_queries(supports: list[_Access], total: float) -> list[dict[str, Any]]:
    by_query: dict[int, dict[str, Any]] = {}
    for item in supports:
        entry = by_query.setdefault(
            item.query.query_id,
            {
                "query_id": item.query.query_id,
                "plan_id": item.query.plan_id,
                "object_name": item.query.object_name,
                "executions": round(item.query.executions, 1),
                "active_days": item.query.active_days,
                "statement_type": item.statement_type,
                "attributed": 0.0,
                "accesses": [],
                "query_text_preview": item.query.query_text_preview,
            },
        )
        entry["attributed"] += item.attributed
        entry["accesses"].append(
            {
                "operation": item.access.operation,
                "index_used": item.access.index_name,
                "seek_columns": list(item.access.seek_eq_columns + item.access.seek_range_columns),
                "residual_columns": dict(item.access.residual),
                "estimated_rows": item.access.estimated_rows,
            }
        )
    rows = sorted(by_query.values(), key=lambda entry: -entry["attributed"])[:10]
    for entry in rows:
        entry["workload_share_pct"] = _pct(entry.pop("attributed"), total)
    return rows


def _ranked_queries(items: list[_Access]) -> list[tuple[int, int, float]]:
    """(query_id, plan_id, attributed cost) per query, costliest first."""

    by_query: dict[int, tuple[int, float]] = {}
    for item in items:
        plan_id, attributed = by_query.get(item.query.query_id, (item.query.plan_id, 0.0))
        by_query[item.query.query_id] = (plan_id, attributed + item.attributed)
    return sorted(
        ((query_id, plan_id, cost) for query_id, (plan_id, cost) in by_query.items()),
        key=lambda entry: (-entry[2], entry[0]),
    )


def _proof_set(supports: list[_Access]) -> list[dict[str, Any]]:
    """The costliest supporting queries until 80% of the recommendation's cost, at most three."""

    ranked = _ranked_queries(supports)
    whole = sum(cost for _, _, cost in ranked)
    chosen: list[dict[str, Any]] = []
    covered = 0.0
    for query_id, plan_id, cost in ranked:
        chosen.append(
            {"query_id": query_id, "plan_id": plan_id, "share_of_recommendation_pct": _pct(cost, whole)}
        )
        covered += cost
        if len(chosen) >= PROOF_SET_MAX or covered >= PROOF_SET_SHARE * whole:
            break
    return chosen


def _query_shares(items: list[_Access], total: float) -> list[dict[str, Any]]:
    return [
        {"query_id": query_id, "plan_id": plan_id, "workload_share_pct": _pct(cost, total)}
        for query_id, plan_id, cost in _ranked_queries(items)[:REGRESSION_SET_MAX]
    ]


def _after_change_text(*, with_regression: bool) -> str:
    sets = "proof_set and regression_set" if with_regression else "proof_set"
    return (
        f"Compare Query Store CPU, duration, and reads for the {sets} queries over equal "
        "windows before and after. Roll back if CPU per execution of any listed query rises "
        f"more than {ROLLBACK_CPU_INCREASE_PCT}%."
    )


def _existing_index_summary(
    index: ExistingIndex, accesses: list[_Access], inputs: AdvisorInputs
) -> dict[str, Any]:
    used = [item for item in accesses if _norm(item.access.index_name or "") == _norm(index.name)]
    return {
        "name": index.name,
        "index_type": index.index_type,
        "key_columns": [c.as_dict() for c in index.key_columns],
        "include_columns": list(index.include_columns),
        "filter_definition": index.filter_definition,
        "is_unique": index.is_unique,
        "is_primary_key": index.is_primary_key,
        "is_disabled": index.is_disabled,
        "size_mb": _index_size_mb(index),
        "usage": {
            "user_seeks": index.usage.get("user_seeks"),
            "user_scans": index.usage.get("user_scans"),
            "user_lookups": index.usage.get("user_lookups"),
            "user_updates": index.usage.get("user_updates"),
        },
        "workload_queries_using": len({item.query.query_id for item in used}),
        "query_store_plan_references": inputs.index_plan_references.get(
            (_norm(index.schema), _norm(index.table), _norm(index.name))
        ),
        "protections": _protection_reasons(index),
    }


def _access_summary(accesses: list[_Access]) -> dict[str, int]:
    summary: dict[str, int] = defaultdict(int)
    for item in accesses:
        summary[item.access.operation] += 1
    return dict(sorted(summary.items()))


def _display_identity(
    key: tuple[str, str],
    info: TableInfo | None,
    accesses: list[_Access],
    existing: list[ExistingIndex],
) -> tuple[str, str]:
    if info is not None:
        return info.schema, info.table
    if accesses:
        return accesses[0].access.schema, accesses[0].access.table
    if existing:
        return existing[0].schema, existing[0].table
    return key


def _generated_name(candidate: _Candidate, taken: set[str]) -> str:
    base = "IX_" + candidate.table + "_" + "_".join(candidate.key_names)
    base = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in base)
    if len(base) > 120:
        digest = hashlib.sha256(base.encode("utf-8")).hexdigest()[:8]
        base = base[:111] + "_" + digest
    name = base
    suffix = 2
    while _norm(name) in taken:
        name = f"{base}_{suffix}"
        suffix += 1
    return name


def _estimate_size_mb(
    keys: list[tuple[str, str]],
    includes: list[str],
    columns: dict[str, ColumnInfo],
    info: TableInfo | None,
) -> float | None:
    if info is None or not info.row_count:
        return None
    width = 0
    for name in [n for n, _ in keys] + list(includes):
        column = columns.get(_norm(name))
        if column is None:
            return None
        width += min(column.width_bytes, 900)
    row_bytes = width + 11  # row header, null bitmap, and locator overhead
    return round(info.row_count * row_bytes / 0.85 / 1024 / 1024, 1)


def _index_size_mb(index: ExistingIndex) -> float | None:
    pages = [count for _, count in index.partition_page_counts if count is not None]
    if not pages:
        return None
    return round(sum(pages) * 8 / 1024, 2)


def _query_store_window(inputs: AdvisorInputs) -> tuple[dict[str, Any], float | None]:
    """The requested window and the part of it that Query Store actually holds.

    Returns the report's ``window`` and the held days (None when coverage was not read).
    """

    lookback = inputs.settings.lookback_days
    window: dict[str, Any] = {
        "start_utc": inputs.window_start_utc,
        "end_utc": inputs.window_end_utc,
        "lookback_days": lookback,
        "requested_days": lookback,
        "query_store_effective_start_utc": None,
        "query_store_effective_end_utc": None,
        "query_store_effective_days": None,
        "query_store_interval_count": None,
        "query_store_oldest_interval_utc": None,
    }
    coverage = inputs.query_store_coverage
    if coverage is None:
        return window, None
    count = int(coverage.get("interval_count") or 0)
    window["query_store_interval_count"] = count
    window["query_store_oldest_interval_utc"] = _utc_text(
        _parse_utc(coverage.get("oldest_interval_start_utc"))
    )
    starts = [
        value
        for value in (_parse_utc(inputs.window_start_utc), _parse_utc(coverage.get("effective_start_utc")))
        if value is not None
    ]
    ends = [
        value
        for value in (_parse_utc(inputs.window_end_utc), _parse_utc(coverage.get("effective_end_utc")))
        if value is not None
    ]
    if count <= 0 or not starts or not ends or min(ends) <= max(starts):
        window["query_store_effective_days"] = 0.0
        return window, 0.0
    start, end = max(starts), min(ends)
    days = max(1 / 24, (end - start).total_seconds() / 86_400)
    window["query_store_effective_start_utc"] = _utc_text(start)
    window["query_store_effective_end_utc"] = _utc_text(end)
    window["query_store_effective_days"] = round(days, 2)
    return window, days


def _rate_days(inputs: AdvisorInputs) -> float:
    """Days that per-day rates divide by: the days Query Store holds, else the requested window."""

    _, days = _query_store_window(inputs)
    return days if days else float(max(1, inputs.settings.lookback_days))


def _utc_text(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if value else None


def _days_since(start_utc: str | None, now_utc: str | None) -> float | None:
    start = _parse_utc(start_utc)
    if start is None:
        return None
    now = _parse_utc(now_utc) or datetime.now(timezone.utc)
    return max(0.0, (now - start).total_seconds() / 86_400)


def _parse_utc(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _pct(part: float, whole: float) -> float | None:
    if not whole:
        return None
    return round(100.0 * part / whole, 3)


def _coverage_gaps(
    inputs: AdvisorInputs,
    analyzed_total: float,
    total: float,
    parse_failures: int,
    uptime_days: float | None,
) -> list[str]:
    gaps = []
    share = _pct(analyzed_total, total)
    if share is not None and share < 80:
        gaps.append(
            f"analysed queries cover {share:.1f}% of the workload objective; raise top_queries for more coverage"
        )
    if parse_failures:
        gaps.append(f"{parse_failures} stored plan(s) could not be parsed")
    capture = str(inputs.query_store.get("query_capture_mode") or "").upper()
    if capture == "AUTO":
        gaps.append("Query Store capture mode AUTO skips infrequent queries; rare access paths may be missing")
    elif capture == "CUSTOM":
        gaps.append(
            "Query Store capture mode CUSTOM keeps only queries over its capture thresholds; "
            "rare access paths may be missing"
        )
    elif capture == "NONE":
        gaps.append("Query Store capture mode NONE captures no new queries; recent workload is missing")
    gaps.extend(_query_store_retention_gaps(inputs))
    if uptime_days is not None and uptime_days < UNUSED_MIN_UPTIME_DAYS:
        gaps.append(f"index usage counters reset {uptime_days:.1f} day(s) ago; unused-index review is limited")
    if not inputs.selectivity:
        gaps.append("column selectivity from statistics was unavailable; key order uses workload frequency only")
    unread: list[str] = []
    for key, table_columns in inputs.columns.items():
        info = inputs.tables.get(key)
        prefix = f"{info.schema}.{info.table}" if info else ".".join(key)
        unread.extend(
            f"{prefix}.{column.name}"
            for column in table_columns.values()
            if column.is_computed and (column.is_indexable is None or column.is_deterministic is None)
        )
    if unread:
        unread.sort()
        shown = ", ".join(unread[:10]) + (f" and {len(unread) - 10} more" if len(unread) > 10 else "")
        gaps.append(
            f"IsIndexable/IsDeterministic were not read for computed column(s) {shown}; they were "
            "not used as index keys or includes, so advice that needs them may be missing"
        )
    return gaps


def _query_store_retention_gaps(inputs: AdvisorInputs) -> list[str]:
    gaps: list[str] = []
    lookback = inputs.settings.lookback_days
    window, days = _query_store_window(inputs)
    if days is not None and days < 0.9 * lookback:
        oldest = window["query_store_oldest_interval_utc"]
        gaps.append(
            f"Query Store holds {days:.1f} of the requested {lookback} day(s)"
            + (f" (oldest retained interval starts {oldest})" if oldest else "")
            + f"; per-day rates and plan-reference checks use the {days:.1f} day(s) it holds"
        )
    options = inputs.query_store
    stale = _float_or_none(options.get("stale_query_threshold_days"))
    if stale is not None:
        shorter_than = []
        if stale < lookback:
            shorter_than.append(f"the {lookback}-day review window")
        if stale < UNUSED_HIGH_CONFIDENCE_DAYS:
            shorter_than.append(f"the {UNUSED_HIGH_CONFIDENCE_DAYS}-day index removal window")
        if shorter_than:
            gaps.append(
                f"Query Store retention (stale_query_threshold_days) is {stale:g} day(s), shorter "
                f"than {' and '.join(shorter_than)}; plans of queries that run only at month-end "
                "may already be purged, so they cannot protect an index from removal"
            )
    current = _float_or_none(options.get("current_storage_size_mb"))
    maximum = _float_or_none(options.get("max_storage_size_mb"))
    if current is not None and maximum and current >= 0.9 * maximum:
        mode = options.get("size_based_cleanup_mode")
        gaps.append(
            f"Query Store storage is at {100 * current / maximum:.0f}% of max_storage_size_mb "
            f"({current:g} of {maximum:g} MB"
            + (f", size_based_cleanup_mode {mode}" if mode else "")
            + "); at the limit it purges the oldest data or turns READ_ONLY, so older or new "
            "workload may be missing"
        )
    return gaps


def _float_or_none(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _next_steps(recommendations: list[dict[str, Any]], rewrites: list[dict[str, Any]]) -> list[str]:
    steps = []
    creates = [r for r in recommendations if r["action"] in {"create_index", "extend_index", "widen_index"}]
    if creates:
        steps.append(
            "Validate the top create/extend/widen recommendations on a non-production copy "
            "with sql-optimizer (start_tuning_session, then benchmark_index_candidate)."
        )
    if any(r["action"] in {"drop_index", "consolidate_index"} for r in recommendations):
        steps.append(
            "Before removing any index, confirm no hint, plan guide, or forced plan names it, "
            "and that the observation window covers a full business cycle."
        )
    if rewrites:
        steps.append(
            "Route non-SARGable predicates and implicit conversions to sql-optimizer; an index cannot fix them."
        )
    steps.append("Every DDL statement is inert advice: a DBA applies changes through change control.")
    return steps


