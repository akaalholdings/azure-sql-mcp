from __future__ import annotations

import logging
import math
import hashlib
import re
import statistics
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from enum import Enum
from typing import Any

from sqlglot import exp
from sqlglot import parse
from sqlglot.errors import ParseError

from .connection import AzureSqlExecutor
from .index_metadata import ExistingIndex
from .index_metadata import coerce_existing_indexes
from .index_metadata import collect_existing_indexes
from .index_metadata import existing_index_covers_candidate
from .index_metadata import index_definition_fingerprint
from .index_metadata import normalize_index_definition
from .index_metadata import parse_candidate_key
from .index_recommendations import split_index_columns
from .observability import sanitize_error_message
from .param_binding import ParameterBindingService
from .safe_sql import SafeSqlValidator

SHOWPLAN_NAMESPACE = {"sp": "http://schemas.microsoft.com/sqlserver/2004/07/showplan"}
logger = logging.getLogger(__name__)

# Minimum row size overhead: null bitmap (2) + row header (1) + child page pointer (6)
_INDEX_ROW_OVERHEAD = 9
# Slot array entry per row
_SLOT_ARRAY_ENTRY = 2
# Usable bytes per 8KB data page
_USABLE_PAGE_BYTES = 8096
# Full page size in bytes
_PAGE_SIZE_BYTES = 8192
# Non-leaf overhead multiplier (typically ~10% for B-tree levels above leaf)
_NON_LEAF_MULTIPLIER = 1.1

# ``build_index_candidate_statement`` emits a rowstore NONCLUSTERED INDEX.
# COLUMNSTORE and COLUMNSTORE_ARCHIVE belong to columnstore index DDL and must
# not be accepted for this statement shape.
_SUPPORTED_ROWSTORE_INDEX_COMPRESSIONS = frozenset({"NONE", "ROW", "PAGE"})


@dataclass(frozen=True)
class IndexCandidate:
    schema: str
    table: str
    key_columns: tuple[str, ...]
    include_columns: tuple[str, ...]
    filter_definition: str | None = None
    is_unique: bool = False
    index_name: str | None = None
    data_space_name: str | None = None
    partition_columns: tuple[str, ...] = ()
    compression: str | None = None
    workflow_id: str | None = None
    partition_scheme_name: str | None = None
    partition_function_name: str | None = None

    @property
    def definition_fingerprint(self) -> str:
        compression = self.compression.upper() if self.compression else None
        partition_compression = ((0, compression),) if compression else ()
        return index_definition_fingerprint(
            schema=self.schema,
            table=self.table,
            index_type="NONCLUSTERED",
            key_columns=self.key_columns,
            include_columns=self.include_columns,
            filter_definition=self.filter_definition,
            is_unique=self.is_unique,
            partition_columns=self.partition_columns,
            data_space_name=self.data_space_name,
            partition_scheme_name=self.partition_scheme_name,
            partition_function_name=self.partition_function_name,
            partition_compression=partition_compression,
        )

    @property
    def fingerprint(self) -> str:
        return self.definition_fingerprint

    @property
    def expected_index_name(self) -> str | None:
        return self.index_name

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "table": self.table,
            "key_columns": list(self.key_columns),
            "include_columns": list(self.include_columns),
            "filter_definition": self.filter_definition,
            "is_unique": self.is_unique,
            "index_name": self.index_name,
            "data_space_name": self.data_space_name,
            "partition_columns": list(self.partition_columns),
            "compression": self.compression,
            "workflow_id": self.workflow_id,
            "partition_scheme_name": self.partition_scheme_name,
            "partition_function_name": self.partition_function_name,
            "definition_fingerprint": self.definition_fingerprint,
        }


@dataclass(frozen=True)
class PlanUseVerification:
    expected_index_name: str
    used: bool
    operator_count: int
    matched_index_names: tuple[str, ...]
    plan_fingerprint: str
    evidence: str = "showplan_xml"

    @property
    def verified(self) -> bool:
        return self.used

    def as_dict(self) -> dict[str, Any]:
        return {
            "expected_index_name": self.expected_index_name,
            "used": self.used,
            "verified": self.verified,
            "operator_count": self.operator_count,
            "matched_index_names": list(self.matched_index_names),
            "plan_fingerprint": self.plan_fingerprint,
            "evidence": self.evidence,
        }


class ExperimentPhase(str, Enum):
    BASELINE_BEFORE = "baseline_before"
    WITH_INDEX = "with_index"
    BASELINE_AFTER = "baseline_after"


@dataclass(frozen=True)
class ExperimentMeasurement:
    phase: ExperimentPhase
    elapsed_ms: float
    result_fingerprint: str
    plan_xml: str | None = None
    plan_use: PlanUseVerification | None = None

    def __post_init__(self) -> None:
        if self.elapsed_ms < 0:
            raise ValueError("experiment elapsed_ms must not be negative")
        if not self.result_fingerprint:
            raise ValueError("experiment result_fingerprint must not be empty")


@dataclass(frozen=True)
class ExperimentAssessment:
    status: str
    baseline_equivalent: bool | None
    candidate_wins: bool | None
    baseline_median_ms: float | None
    candidate_median_ms: float | None
    improvement_pct: float | None
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "baseline_equivalent": self.baseline_equivalent,
            "candidate_wins": self.candidate_wins,
            "baseline_median_ms": self.baseline_median_ms,
            "candidate_median_ms": self.candidate_median_ms,
            "improvement_pct": self.improvement_pct,
            "reason": self.reason,
        }


@dataclass
class BracketedIndexExperiment:
    """A-B-A experiment requiring both no-index brackets before judging B."""

    candidate: IndexCandidate
    measurements: dict[ExperimentPhase, list[ExperimentMeasurement]] = field(
        default_factory=lambda: {phase: [] for phase in ExperimentPhase}
    )

    def add_measurement(
        self,
        phase: ExperimentPhase,
        *,
        elapsed_ms: float,
        result_fingerprint: str,
        plan_xml: str | None = None,
    ) -> ExperimentMeasurement:
        phase = ExperimentPhase(phase)
        plan_use = None
        if phase is ExperimentPhase.WITH_INDEX and plan_xml and self.candidate.index_name:
            try:
                plan_use = verify_plan_uses_index(plan_xml, self.candidate.index_name)
            except ET.ParseError:
                # Preserve the sample as unverified so assessment fails closed.
                plan_use = None
        measurement = ExperimentMeasurement(
            phase=phase,
            elapsed_ms=elapsed_ms,
            result_fingerprint=result_fingerprint,
            plan_xml=plan_xml,
            plan_use=plan_use,
        )
        self.measurements.setdefault(phase, []).append(measurement)
        return measurement

    def assess(self, *, min_improvement_pct: float = 5.0) -> ExperimentAssessment:
        before = self.measurements.get(ExperimentPhase.BASELINE_BEFORE, [])
        candidate = self.measurements.get(ExperimentPhase.WITH_INDEX, [])
        after = self.measurements.get(ExperimentPhase.BASELINE_AFTER, [])
        if not before or not candidate or not after:
            return ExperimentAssessment(
                status="incomplete",
                baseline_equivalent=None,
                candidate_wins=None,
                baseline_median_ms=None,
                candidate_median_ms=None,
                improvement_pct=None,
                reason="A-B-A requires baseline_before, with_index, and baseline_after runs.",
            )

        result_fingerprints = {
            item.result_fingerprint
            for item in (*before, *candidate, *after)
        }
        baseline_equivalent = len(result_fingerprints) == 1
        if not baseline_equivalent:
            return ExperimentAssessment(
                status="inconclusive",
                baseline_equivalent=False,
                candidate_wins=None,
                baseline_median_ms=None,
                candidate_median_ms=None,
                improvement_pct=None,
                reason=(
                    "The before, with-index, and after samples did not share one "
                    "stable result fingerprint."
                ),
            )

        baseline_median = statistics.median(
            [item.elapsed_ms for item in (*before, *after)]
        )
        candidate_median = statistics.median([item.elapsed_ms for item in candidate])
        improvement = ((baseline_median - candidate_median) / baseline_median * 100.0) if baseline_median else 0.0
        plan_use_failures = [
            item
            for item in candidate
            if not _verified_expected_index_use(item, self.candidate.index_name)
        ]
        if plan_use_failures:
            return ExperimentAssessment(
                status="inconclusive",
                baseline_equivalent=True,
                candidate_wins=None,
                baseline_median_ms=baseline_median,
                candidate_median_ms=candidate_median,
                improvement_pct=improvement,
                reason="The candidate plan did not use the expected index.",
            )
        wins = improvement >= min_improvement_pct
        return ExperimentAssessment(
            status="winner" if wins else "loser",
            baseline_equivalent=True,
            candidate_wins=wins,
            baseline_median_ms=baseline_median,
            candidate_median_ms=candidate_median,
            improvement_pct=improvement,
            reason=(
                "Candidate beat the bracketed baseline."
                if wins
                else "Candidate did not beat the bracketed baseline; another candidate may continue."
            ),
        )


def verify_plan_uses_index(plan_xml: str, expected_index_name: str) -> PlanUseVerification:
    """Verify an index seek/scan by name in SHOWPLAN XML, not by timing."""

    root = ET.fromstring(plan_xml)
    expected = _normalize_plan_index_name(expected_index_name)
    matches: list[str] = []
    operators = 0
    for element in root.iter():
        local_name = element.tag.rsplit("}", 1)[-1]
        if local_name not in {"IndexScan", "IndexSeek"}:
            continue
        index_name = element.attrib.get("Index") or element.attrib.get("IndexName")
        if not index_name:
            for descendant in element.iter():
                descendant_name = descendant.tag.rsplit("}", 1)[-1]
                if descendant_name != "Object":
                    continue
                index_name = descendant.attrib.get("Index") or descendant.attrib.get(
                    "IndexName"
                )
                if index_name:
                    break
        if not index_name:
            continue
        operators += 1
        if _normalize_plan_index_name(index_name) == expected:
            matches.append(index_name)
    plan_fingerprint = hashlib.sha256(plan_xml.encode("utf-8")).hexdigest()
    return PlanUseVerification(
        expected_index_name=expected_index_name,
        used=bool(matches),
        operator_count=operators,
        matched_index_names=tuple(matches),
        plan_fingerprint=plan_fingerprint,
    )


@dataclass
class ScoredCandidate:
    candidate: IndexCandidate
    estimated_size_mb: float
    read_benefit: float
    write_ratio: float
    impact_pct: float
    affected_query_count: int
    affected_query_ids: list[int]
    source: str  # "plan_xml" | "dmv" | "merged"
    confidence: str  # "high" | "medium" | "low"
    score: float
    create_index_sql: str


@dataclass(frozen=True)
class OptimizationResult:
    database_name: str
    workload_window_minutes: int
    queries_analyzed: int
    candidates_generated: int
    candidates_after_consolidation: int
    selected_indexes: list[ScoredCandidate]
    total_estimated_size_mb: float
    existing_indexes_count: int
    existing_index_metadata: list[dict[str, Any]]
    budget_mb: float | None
    alpha: float
    beta: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "database_name": self.database_name,
            "workload_window_minutes": self.workload_window_minutes,
            "queries_analyzed": self.queries_analyzed,
            "candidates_generated": self.candidates_generated,
            "candidates_after_consolidation": self.candidates_after_consolidation,
            "selected_indexes": [
                {
                    "rank": rank,
                    "schema": sc.candidate.schema,
                    "table": sc.candidate.table,
                    "key_columns": list(sc.candidate.key_columns),
                    "include_columns": list(sc.candidate.include_columns),
                    "estimated_size_mb": round(sc.estimated_size_mb, 2),
                    "read_benefit": round(sc.read_benefit, 2),
                    "write_ratio": round(sc.write_ratio, 3),
                    "impact_pct": round(sc.impact_pct, 1),
                    "affected_query_count": sc.affected_query_count,
                    "affected_query_ids": sc.affected_query_ids,
                    "source": sc.source,
                    "confidence": sc.confidence,
                    "score": round(sc.score, 4),
                    "create_index_sql": sc.create_index_sql,
                }
                for rank, sc in enumerate(self.selected_indexes, start=1)
            ],
            "total_estimated_size_mb": round(self.total_estimated_size_mb, 2),
            "existing_indexes_count": self.existing_indexes_count,
            "existing_indexes": self.existing_index_metadata,
            "budget_mb": self.budget_mb,
            "scoring": {"alpha": self.alpha, "beta": self.beta},
        }


# ---------------------------------------------------------------------------
# Internal mutable accumulator used during candidate generation / merging
# ---------------------------------------------------------------------------


@dataclass
class _RawCandidate:
    schema: str
    table: str
    key_columns: list[str]
    include_columns: list[str]
    filter_definition: str | None = None
    is_unique: bool = False
    data_space_name: str | None = None
    partition_columns: list[str] = field(default_factory=list)
    compression: str | None = None
    impact_pct: float = 0.0
    statement_subtree_cost: float = 0.0
    total_cpu_us: float = 0.0
    total_executions: float = 0.0
    query_ids: set[int] = field(default_factory=set)
    from_plan_xml: bool = False
    from_dmv: bool = False
    dmv_user_seeks: float = 0.0
    dmv_avg_user_impact: float = 0.0
    dmv_avg_total_user_cost: float = 0.0

    @property
    def signature(self) -> tuple[str, str, tuple[str, ...]]:
        return (self.schema, self.table, tuple(self.key_columns))

    @property
    def definition_signature(self) -> tuple[Any, ...]:
        return (
            self.schema,
            self.table,
            tuple(self.key_columns),
            normalize_index_definition(self.filter_definition),
            self.is_unique,
        )

    @property
    def source(self) -> str:
        if self.from_plan_xml and self.from_dmv:
            return "merged"
        if self.from_dmv:
            return "dmv"
        return "plan_xml"

    @property
    def confidence(self) -> str:
        if self.from_plan_xml and self.from_dmv:
            return "high"
        return "medium"


class IndexOptimizer:
    """Optimizer-signal-driven index tuning engine for Azure SQL Database."""

    def __init__(
        self,
        executor: AzureSqlExecutor,
        validator: SafeSqlValidator,
    ):
        self.executor = executor
        self.validator = validator
        self.param_binding = ParameterBindingService(executor)

    @staticmethod
    def make_temporary_index_candidate(
        candidate: IndexCandidate,
        workflow_id: str,
    ) -> IndexCandidate:
        return make_temporary_index_candidate(candidate, workflow_id)

    @staticmethod
    def verify_expected_index_definition(
        actual: ExistingIndex,
        expected: IndexCandidate,
    ) -> bool:
        return expected_index_definition_matches(actual, expected)

    @staticmethod
    def verify_plan_uses_index(
        plan_xml: str,
        expected_index_name: str,
    ) -> PlanUseVerification:
        return verify_plan_uses_index(plan_xml, expected_index_name)

    async def optimize(
        self,
        database_name: str,
        window_minutes: int = 60,
        top_n: int = 30,
        budget_mb: float | None = None,
        alpha: float = 1.5,
        beta: float = 0.5,
        min_improvement_pct: float = 5.0,
    ) -> OptimizationResult:
        # Step 1: collect workload + existing indexes
        workload = await self._collect_workload(database_name, window_minutes, top_n)
        dmv_recs = await self._get_dmv_missing_indexes(database_name)
        existing = await self._get_existing_indexes(database_name)

        # Step 2: generate candidates
        raw = self._generate_candidates(workload, dmv_recs)
        candidates_generated = len(raw)

        # Step 3: filter against existing indexes (including prefix)
        raw = self._filter_existing_indexes(raw, existing)

        # Step 4: overlap detection & prefix subsumption
        raw = self._detect_prefix_subsumption(raw)
        candidates_after = len(raw)

        # Step 5: enrich with size + write ratio
        for candidate in raw:
            candidate_cols = candidate.key_columns + [
                c for c in candidate.include_columns if c not in candidate.key_columns
            ]
            size = await self._estimate_index_size(
                database_name, candidate.schema, candidate.table, candidate_cols,
            )
            candidate.estimated_size_mb = size  # type: ignore[attr-defined]

            wr = await self._get_table_write_ratio(
                database_name, candidate.schema, candidate.table,
            )
            candidate.write_ratio = wr  # type: ignore[attr-defined]

        # Step 6: score
        scored = self._score_candidates(raw, alpha, beta)

        # Step 7: greedy select
        selected = self._greedy_select(scored, budget_mb, min_improvement_pct)

        total_size = sum(s.estimated_size_mb for s in selected)

        return OptimizationResult(
            database_name=database_name,
            workload_window_minutes=window_minutes,
            queries_analyzed=len(workload),
            candidates_generated=candidates_generated,
            candidates_after_consolidation=candidates_after,
            selected_indexes=selected,
            total_estimated_size_mb=total_size,
            existing_indexes_count=len(existing),
            existing_index_metadata=[index.as_dict() for index in existing],
            budget_mb=budget_mb,
            alpha=alpha,
            beta=beta,
        )

    # ------------------------------------------------------------------
    # Step 1: Workload collection
    # ------------------------------------------------------------------

    async def _collect_workload(
        self, database_name: str, window_minutes: int, top_n: int,
    ) -> list[dict[str, Any]]:
        """Pull top queries from Query Store and attach plan XML + cost."""
        rows = await self._get_top_workload_queries(database_name, window_minutes, top_n)
        results: list[dict[str, Any]] = []
        for row in rows:
            sql_text = row.get("query_sql_text", "")
            if not sql_text or not sql_text.strip():
                continue
            try:
                normalized_sql = await self.param_binding.prepare_query_store_text(
                    database_name, sql_text,
                )
                validated = self.validator.validate_read_only(normalized_sql)
                plan_xml = await self._get_estimated_plan(database_name, validated.normalized_sql)
                subtree_cost = self._extract_statement_cost(plan_xml)
                missing = self._extract_missing_indexes(plan_xml)
                results.append({
                    "query_id": row.get("query_id"),
                    "plan_id": row.get("plan_id"),
                    "sql": validated.normalized_sql,
                    "plan_xml": plan_xml,
                    "statement_subtree_cost": subtree_cost,
                    "missing_indexes": missing,
                    "executions": _to_float(row.get("executions")),
                    "total_cpu_us": _to_float(row.get("total_cpu_us")),
                    "total_duration_us": _to_float(row.get("total_duration_us")),
                    "total_logical_io_reads": _to_float(row.get("total_logical_io_reads")),
                })
            except Exception as exc:
                logger.debug(
                    "Skipping workload query during index optimization",
                    extra={
                        "database_name": database_name,
                        "query_id": row.get("query_id"),
                        "error": sanitize_error_message(str(exc)),
                    },
                )
                continue
        return results

    # ------------------------------------------------------------------
    # Step 2: Candidate generation
    # ------------------------------------------------------------------

    def _generate_candidates(
        self,
        workload: list[dict[str, Any]],
        dmv_recs: list[dict[str, Any]],
    ) -> list[_RawCandidate]:
        seen: dict[tuple[Any, ...], _RawCandidate] = {}

        # From plan XML missing indexes
        for query_row in workload:
            for mi in query_row.get("missing_indexes", []):
                key_cols = mi.get("equality_columns", []) + mi.get("inequality_columns", [])
                if not key_cols:
                    continue
                sig = (
                    mi.get("schema", "dbo"),
                    mi.get("table", ""),
                    tuple(key_cols),
                    normalize_index_definition(mi.get("filter_definition")),
                    bool(mi.get("is_unique", False)),
                )
                if sig in seen:
                    rc = seen[sig]
                    rc.impact_pct = max(rc.impact_pct, mi.get("impact_pct", 0.0))
                    rc.statement_subtree_cost += query_row.get("statement_subtree_cost", 0.0)
                    rc.total_cpu_us += query_row.get("total_cpu_us", 0.0)
                    rc.total_executions += query_row.get("executions", 0.0)
                    rc.include_columns = _merge_unique(
                        rc.include_columns, mi.get("include_columns", []),
                    )
                    if query_row.get("query_id") is not None:
                        rc.query_ids.add(query_row["query_id"])
                else:
                    rc = _RawCandidate(
                        schema=mi.get("schema", "dbo"),
                        table=mi.get("table", ""),
                        key_columns=list(key_cols),
                        include_columns=list(mi.get("include_columns", [])),
                        filter_definition=mi.get("filter_definition"),
                        is_unique=bool(mi.get("is_unique", False)),
                        impact_pct=mi.get("impact_pct", 0.0),
                        statement_subtree_cost=query_row.get("statement_subtree_cost", 0.0),
                        total_cpu_us=query_row.get("total_cpu_us", 0.0),
                        total_executions=query_row.get("executions", 0.0),
                        from_plan_xml=True,
                    )
                    if query_row.get("query_id") is not None:
                        rc.query_ids.add(query_row["query_id"])
                    seen[sig] = rc

        # From DMV missing indexes
        for dmv in dmv_recs:
            key_cols = dmv.get("equality_columns", []) + dmv.get("inequality_columns", [])
            if not key_cols:
                continue
            sig = (
                dmv.get("schema", "dbo"),
                dmv.get("table", ""),
                tuple(key_cols),
                normalize_index_definition(dmv.get("filter_definition")),
                bool(dmv.get("is_unique", False)),
            )
            if sig in seen:
                rc = seen[sig]
                rc.from_dmv = True
                rc.dmv_user_seeks = _to_float(dmv.get("user_seeks"))
                rc.dmv_avg_user_impact = _to_float(dmv.get("avg_user_impact"))
                rc.dmv_avg_total_user_cost = _to_float(dmv.get("avg_total_user_cost"))
                rc.include_columns = _merge_unique(
                    rc.include_columns, dmv.get("include_columns", []),
                )
                # Use DMV impact if higher
                dmv_impact = _to_float(dmv.get("avg_user_impact"))
                if dmv_impact > rc.impact_pct:
                    rc.impact_pct = dmv_impact
            else:
                # DMV-only candidate: estimate benefit from DMV stats
                dmv_improvement = _to_float(dmv.get("estimated_improvement"))
                rc = _RawCandidate(
                    schema=dmv.get("schema", "dbo"),
                    table=dmv.get("table", ""),
                    key_columns=list(key_cols),
                    include_columns=list(dmv.get("include_columns", [])),
                    filter_definition=dmv.get("filter_definition"),
                    is_unique=bool(dmv.get("is_unique", False)),
                    impact_pct=_to_float(dmv.get("avg_user_impact")),
                    statement_subtree_cost=_to_float(dmv.get("avg_total_user_cost")),
                    total_cpu_us=dmv_improvement,
                    total_executions=_to_float(dmv.get("user_seeks")),
                    from_dmv=True,
                    dmv_user_seeks=_to_float(dmv.get("user_seeks")),
                    dmv_avg_user_impact=_to_float(dmv.get("avg_user_impact")),
                    dmv_avg_total_user_cost=_to_float(dmv.get("avg_total_user_cost")),
                )
                seen[sig] = rc

        return list(seen.values())

    # ------------------------------------------------------------------
    # Step 3: Enrichment — size estimation & write ratio
    # ------------------------------------------------------------------

    async def _estimate_index_size(
        self,
        database_name: str,
        schema: str,
        table: str,
        columns: list[str],
    ) -> float:
        """Estimate index size in MB using row count and column widths."""
        row_count = await self._get_row_count(database_name, schema, table)
        if row_count == 0:
            return 0.0

        col_widths = await self._get_column_widths(database_name, schema, table, columns)
        if not col_widths:
            return 0.0

        total_key_width = sum(col_widths.values()) + _INDEX_ROW_OVERHEAD
        rows_per_page = max(1, _USABLE_PAGE_BYTES // (total_key_width + _SLOT_ARRAY_ENTRY))
        leaf_pages = math.ceil(row_count / rows_per_page)
        total_bytes = leaf_pages * _PAGE_SIZE_BYTES * _NON_LEAF_MULTIPLIER
        return total_bytes / (1024.0 * 1024.0)

    async def _get_table_write_ratio(
        self, database_name: str, schema: str, table: str,
    ) -> float:
        """Get write ratio (0.0-1.0) for a table from index usage stats."""
        query = """
        SELECT
            CASE WHEN SUM(s.user_seeks + s.user_scans + s.user_lookups + s.user_updates) = 0
                 THEN 0.5
                 ELSE CAST(SUM(s.user_updates) AS FLOAT)
                      / SUM(s.user_seeks + s.user_scans + s.user_lookups + s.user_updates)
            END AS write_ratio
        FROM sys.indexes i
        JOIN sys.dm_db_index_usage_stats s
            ON i.object_id = s.object_id AND i.index_id = s.index_id
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas sch ON t.schema_id = sch.schema_id
        WHERE sch.name = ? AND t.name = ? AND s.database_id = DB_ID()
        """
        rows = await self.executor.fetch_all(database_name, query, params=[schema, table])
        if rows and rows[0].get("write_ratio") is not None:
            return float(rows[0]["write_ratio"])
        return 0.5  # default: balanced read/write when no stats

    async def _get_row_count(
        self, database_name: str, schema: str, table: str,
    ) -> int:
        query = """
        SELECT SUM(p.row_count) AS row_count
        FROM sys.dm_db_partition_stats p
        JOIN sys.tables t ON p.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ? AND p.index_id IN (0, 1)
        """
        rows = await self.executor.fetch_all(database_name, query, params=[schema, table])
        if rows and rows[0].get("row_count") is not None:
            return int(rows[0]["row_count"])
        return 0

    async def _get_column_widths(
        self,
        database_name: str,
        schema: str,
        table: str,
        columns: list[str],
    ) -> dict[str, int]:
        """Return {column_name: max_length} for requested columns."""
        if not columns:
            return {}
        # Build parameterized IN clause
        placeholders = ", ".join("?" for _ in columns)
        query = f"""
        SELECT c.name AS column_name, c.max_length
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ? AND c.name IN ({placeholders})
        """
        params: list[Any] = [schema, table, *columns]
        rows = await self.executor.fetch_all(database_name, query, params=params)
        return {
            row["column_name"]: int(row.get("max_length") or 8)
            for row in rows
            if row.get("column_name")
        }

    # ------------------------------------------------------------------
    # Step 4: Overlap detection & consolidation
    # ------------------------------------------------------------------

    def _filter_existing_indexes(
        self,
        candidates: list[_RawCandidate],
        existing: (
            list[ExistingIndex]
            | set[tuple[str, str, tuple[str, ...]]]
        ),
    ) -> list[_RawCandidate]:
        """Remove only candidates fully covered by an enabled compatible index."""
        metadata = coerce_existing_indexes(existing)
        result: list[_RawCandidate] = []
        for rc in candidates:
            covered = any(
                existing_index_covers_candidate(
                    index,
                    schema=rc.schema,
                    table=rc.table,
                    key_columns=rc.key_columns,
                    include_columns=rc.include_columns,
                    filter_definition=rc.filter_definition,
                    is_unique=rc.is_unique,
                    data_space_name=rc.data_space_name,
                    partition_columns=rc.partition_columns,
                )
                for index in metadata
            )
            if not covered:
                result.append(rc)
        return result

    def _detect_prefix_subsumption(
        self, candidates: list[_RawCandidate],
    ) -> list[_RawCandidate]:
        """Merge candidates where one's key_columns is a prefix of another's on the same table."""
        if not candidates:
            return []

        # Group by (schema, table)
        groups: dict[tuple[str, str], list[_RawCandidate]] = {}
        for rc in candidates:
            groups.setdefault((rc.schema, rc.table), []).append(rc)

        result: list[_RawCandidate] = []
        for _table_key, group in groups.items():
            # Sort by key_columns length descending so wider indexes come first
            group.sort(key=lambda c: len(c.key_columns), reverse=True)
            merged: list[_RawCandidate] = []
            for rc in group:
                subsumed = False
                for wider in merged:
                    if (
                        _is_prefix(tuple(rc.key_columns), tuple(wider.key_columns))
                        and normalize_index_definition(rc.filter_definition)
                        == normalize_index_definition(wider.filter_definition)
                        and rc.is_unique == wider.is_unique
                    ):
                        # rc is subsumed by wider — merge metrics into wider
                        wider.impact_pct = max(wider.impact_pct, rc.impact_pct)
                        wider.statement_subtree_cost += rc.statement_subtree_cost
                        wider.total_cpu_us += rc.total_cpu_us
                        wider.total_executions += rc.total_executions
                        wider.query_ids |= rc.query_ids
                        wider.include_columns = _merge_unique(
                            wider.include_columns, rc.include_columns,
                        )
                        if rc.data_space_name and not wider.data_space_name:
                            wider.data_space_name = rc.data_space_name
                        wider.partition_columns = list(
                            _merge_unique(wider.partition_columns, rc.partition_columns)
                        )
                        wider.from_plan_xml = wider.from_plan_xml or rc.from_plan_xml
                        wider.from_dmv = wider.from_dmv or rc.from_dmv
                        if rc.dmv_user_seeks > wider.dmv_user_seeks:
                            wider.dmv_user_seeks = rc.dmv_user_seeks
                            wider.dmv_avg_user_impact = rc.dmv_avg_user_impact
                            wider.dmv_avg_total_user_cost = rc.dmv_avg_total_user_cost
                        subsumed = True
                        break
                if not subsumed:
                    merged.append(rc)
            result.extend(merged)
        return result

    # ------------------------------------------------------------------
    # Step 5: Pareto scoring
    # ------------------------------------------------------------------

    def _score_candidates(
        self,
        candidates: list[_RawCandidate],
        alpha: float,
        beta: float,
    ) -> list[ScoredCandidate]:
        scored: list[ScoredCandidate] = []
        for rc in candidates:
            size_mb = getattr(rc, "estimated_size_mb", 0.0)
            write_ratio = getattr(rc, "write_ratio", 0.5)

            read_benefit = (
                (rc.impact_pct / 100.0)
                * max(rc.statement_subtree_cost, 0.001)
                * max(rc.total_executions, 1.0)
            )

            score = (
                math.log(read_benefit + 1.0)
                - alpha * math.log(size_mb + 1.0)
                - beta * math.log(write_ratio + 0.01)
            )

            # Remove key columns from include columns to avoid duplication
            key_set = set(rc.key_columns)
            clean_include = [c for c in rc.include_columns if c not in key_set]

            candidate = IndexCandidate(
                schema=rc.schema,
                table=rc.table,
                key_columns=tuple(rc.key_columns),
                include_columns=tuple(clean_include),
                filter_definition=rc.filter_definition,
                is_unique=rc.is_unique,
                data_space_name=rc.data_space_name,
                partition_columns=tuple(rc.partition_columns),
                compression=rc.compression,
            )

            scored.append(ScoredCandidate(
                candidate=candidate,
                estimated_size_mb=size_mb,
                read_benefit=read_benefit,
                write_ratio=write_ratio,
                impact_pct=rc.impact_pct,
                affected_query_count=max(len(rc.query_ids), 1),
                affected_query_ids=sorted(rc.query_ids),
                source=rc.source,
                confidence=rc.confidence,
                score=score,
                create_index_sql=build_index_candidate_statement(candidate),
            ))

        scored.sort(key=lambda s: s.score, reverse=True)
        return scored

    # ------------------------------------------------------------------
    # Step 6: Greedy selection with budget
    # ------------------------------------------------------------------

    def _greedy_select(
        self,
        scored: list[ScoredCandidate],
        budget_mb: float | None,
        min_improvement_pct: float,
    ) -> list[ScoredCandidate]:
        selected: list[ScoredCandidate] = []
        remaining = budget_mb

        for sc in scored:
            if sc.impact_pct < min_improvement_pct:
                continue

            if remaining is not None and sc.estimated_size_mb > remaining:
                continue

            # Check overlap with already selected — merge if prefix
            merged = False
            for existing in selected:
                if (
                    sc.candidate.schema == existing.candidate.schema
                    and sc.candidate.table == existing.candidate.table
                ):
                    if _is_prefix(sc.candidate.key_columns, existing.candidate.key_columns):
                        # Already covered by wider selected index
                        merged = True
                        break
                    if _is_prefix(existing.candidate.key_columns, sc.candidate.key_columns):
                        # New candidate is wider — replace existing
                        size_diff = sc.estimated_size_mb - existing.estimated_size_mb
                        if remaining is not None and size_diff > remaining:
                            continue
                        selected.remove(existing)
                        selected.append(sc)
                        if remaining is not None:
                            remaining -= size_diff
                        merged = True
                        break

            if not merged:
                selected.append(sc)
                if remaining is not None:
                    remaining -= sc.estimated_size_mb

        # Re-sort by score
        selected.sort(key=lambda s: s.score, reverse=True)
        return selected

    # ------------------------------------------------------------------
    # Reused query helpers (adapted from QueryIndexAnalysisService)
    # ------------------------------------------------------------------

    async def _get_top_workload_queries(
        self, database_name: str, window_minutes: int, top_n: int,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT TOP (?)
            q.query_id,
            p.plan_id,
            qt.query_sql_text,
            SUM(rs.count_executions) AS executions,
            SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu_us,
            SUM(rs.avg_duration * rs.count_executions) AS total_duration_us,
            SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads
        FROM sys.query_store_query_text AS qt
        INNER JOIN sys.query_store_query AS q
            ON qt.query_text_id = q.query_text_id
        INNER JOIN sys.query_store_plan AS p
            ON q.query_id = p.query_id
        INNER JOIN sys.query_store_runtime_stats AS rs
            ON p.plan_id = rs.plan_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
        GROUP BY q.query_id, p.plan_id, qt.query_sql_text
        ORDER BY SUM(rs.avg_cpu_time * rs.count_executions) DESC
        """
        return await self.executor.fetch_all(
            database_name, query, params=[top_n, window_minutes],
        )

    async def _get_estimated_plan(self, database_name: str, sql: str) -> str:
        per_statement_results = await self.executor.execute_session(
            database_name,
            ["SET SHOWPLAN_XML ON", sql, "SET SHOWPLAN_XML OFF"],
            max_rows=self.executor.config.row_limit + 1,
        )
        plan_results = per_statement_results[1] if len(per_statement_results) > 1 else []
        for result in plan_results:
            for row in result.rows:
                for value in row.values():
                    if isinstance(value, str) and value.lstrip().startswith("<ShowPlanXML"):
                        return value
        raise RuntimeError("No SHOWPLAN XML returned.")

    async def _get_dmv_missing_indexes(
        self, database_name: str,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT TOP (50)
            OBJECT_SCHEMA_NAME(mid.object_id) AS schema_name,
            OBJECT_NAME(mid.object_id) AS table_name,
            mid.equality_columns,
            mid.inequality_columns,
            mid.included_columns,
            migs.user_seeks,
            migs.user_scans,
            migs.avg_total_user_cost,
            migs.avg_user_impact,
            CAST(
                (migs.avg_total_user_cost * migs.avg_user_impact
                 * (migs.user_seeks + migs.user_scans))
                AS DECIMAL(18, 2)
            ) AS estimated_improvement
        FROM sys.dm_db_missing_index_details AS mid
        INNER JOIN sys.dm_db_missing_index_groups AS mig
            ON mid.index_handle = mig.index_handle
        INNER JOIN sys.dm_db_missing_index_group_stats AS migs
            ON mig.index_group_handle = migs.group_handle
        WHERE mid.database_id = DB_ID()
        ORDER BY estimated_improvement DESC
        """
        rows = await self.executor.fetch_all(database_name, query)
        for row in rows:
            row["schema"] = row.get("schema_name") or "dbo"
            row["table"] = row.get("table_name") or ""
            row["equality_columns"] = split_index_columns(row.get("equality_columns"))
            row["inequality_columns"] = split_index_columns(row.get("inequality_columns"))
            row["include_columns"] = split_index_columns(row.get("included_columns"))
        return rows

    async def _get_existing_indexes(
        self, database_name: str,
    ) -> list[ExistingIndex]:
        return await collect_existing_indexes(self.executor, database_name)

    # ------------------------------------------------------------------
    # Plan XML helpers
    # ------------------------------------------------------------------

    def _extract_missing_indexes(self, plan_xml: str) -> list[dict[str, Any]]:
        root = ET.fromstring(plan_xml)
        results: list[dict[str, Any]] = []
        for group in root.findall(".//sp:MissingIndexGroup", SHOWPLAN_NAMESPACE):
            impact = float(group.attrib.get("Impact", "0"))
            for idx in group.findall("sp:MissingIndex", SHOWPLAN_NAMESPACE):
                schema = idx.attrib.get("Schema", "[dbo]").strip("[]")
                table = idx.attrib.get("Table", "").strip("[]")
                equality_cols = self._extract_column_group(idx, "EQUALITY")
                inequality_cols = self._extract_column_group(idx, "INEQUALITY")
                include_cols = self._extract_column_group(idx, "INCLUDE")
                key_columns = equality_cols + inequality_cols
                if not key_columns:
                    continue
                results.append({
                    "schema": schema,
                    "table": table,
                    "equality_columns": equality_cols,
                    "inequality_columns": inequality_cols,
                    "include_columns": include_cols,
                    "impact_pct": impact,
                })
        return results

    def _extract_column_group(self, index_node: Any, usage: str) -> list[str]:
        columns: list[str] = []
        for cg in index_node.findall("sp:ColumnGroup", SHOWPLAN_NAMESPACE):
            if cg.attrib.get("Usage") == usage:
                for col in cg.findall("sp:Column", SHOWPLAN_NAMESPACE):
                    name = col.attrib.get("Name", "").strip("[]")
                    if name:
                        columns.append(name)
        return columns

    def _extract_statement_cost(self, plan_xml: str) -> float:
        """Extract the total StatementSubTreeCost from a SHOWPLAN_XML document."""
        root = ET.fromstring(plan_xml)
        for stmt in root.findall(".//sp:StmtSimple", SHOWPLAN_NAMESPACE):
            cost = stmt.attrib.get("StatementSubTreeCost")
            if cost:
                try:
                    return float(cost)
                except (TypeError, ValueError):
                    pass
        return 0.0


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


_FILTERED_PREDICATE_BLOCKED_TOKENS = re.compile(
    r"\b(?:ALTER|BEGIN|COMMIT|CREATE|DBCC|DELETE|DENY|DROP|EXEC(?:UTE)?|"
    r"FOR|GO|GRANT|INSERT|MERGE|ON|OPENQUERY|OPENROWSET|OPTION|PRINT|"
    r"RAISERROR|RECONFIGURE|RETURN|REVOKE|ROLLBACK|SELECT|SET|THROW|"
    r"TRUNCATE|UNION|UPDATE|USE|WAITFOR|WITH)\b",
    re.IGNORECASE,
)
_FILTERED_PREDICATE_UNSAFE_NODES = (
    exp.Alter,
    exp.Command,
    exp.Create,
    exp.Delete,
    exp.Drop,
    exp.Execute,
    exp.Except,
    exp.Insert,
    exp.Intersect,
    exp.Merge,
    exp.Select,
    exp.Subquery,
    exp.Table,
    exp.Transaction,
    exp.Union,
    exp.Update,
)


def _predicate_executable_text(text: str) -> str:
    """Return code tokens while rejecting comments and batch delimiters."""

    executable: list[str] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char in {"'", '"', "["}:
            closing = "]" if char == "[" else char
            executable.append(" ")
            index += 1
            while index < len(text):
                if text[index] == closing:
                    if index + 1 < len(text) and text[index + 1] == closing:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            else:
                raise ValueError("filtered index predicate must be one SQL expression")
            continue
        if char == "-" and index + 1 < len(text) and text[index + 1] == "-":
            raise ValueError("filtered index predicates must not contain comments")
        if char == "/" and index + 1 < len(text) and text[index + 1] == "*":
            raise ValueError("filtered index predicates must not contain comments")
        if char == ";":
            raise ValueError("filtered index predicates must not contain semicolons")
        executable.append(char)
        index += 1
    return "".join(executable)


def _is_filter_column(node: Any) -> bool:
    return (
        isinstance(node, exp.Column)
        and not node.table
        and not node.db
        and not node.catalog
    )


def _is_filter_constant(node: Any) -> bool:
    if isinstance(node, (exp.Literal, exp.Boolean)):
        return True
    if isinstance(node, exp.Neg):
        return isinstance(node.this, exp.Literal) and not node.this.is_string
    if isinstance(node, exp.Cast):
        return _is_filter_constant(node.this)
    if isinstance(node, exp.Convert):
        return _is_filter_constant(node.expression)
    return False


def _is_boolean_predicate(node: Any) -> bool:
    if isinstance(node, exp.Paren):
        return _is_boolean_predicate(node.this)
    if isinstance(node, exp.And):
        return _is_boolean_predicate(node.this) and _is_boolean_predicate(node.expression)
    if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        return (
            _is_filter_column(node.this)
            and _is_filter_constant(node.expression)
        ) or (
            _is_filter_constant(node.this)
            and _is_filter_column(node.expression)
        )
    if isinstance(node, exp.In):
        return (
            _is_filter_column(node.this)
            and node.args.get("query") is None
            and bool(node.expressions)
            and all(_is_filter_constant(value) for value in node.expressions)
        )
    if isinstance(node, exp.Is):
        return _is_filter_column(node.this) and isinstance(
            node.expression,
            exp.Null,
        )
    if isinstance(node, exp.Not) and isinstance(node.this, exp.Is):
        return _is_boolean_predicate(node.this)
    return False


def _validate_filtered_index_predicate(text: str) -> None:
    """Admit exactly one parsed Boolean expression for generated DDL."""

    executable = _predicate_executable_text(text)
    if _FILTERED_PREDICATE_BLOCKED_TOKENS.search(executable):
        raise ValueError("filtered index predicate contains a disallowed SQL token")

    try:
        statements = parse(text, read="tsql")
    except ParseError as exc:
        raise ValueError("filtered index predicate must be one SQL expression") from exc
    if len(statements) != 1 or statements[0] is None:
        raise ValueError("filtered index predicate must be one SQL expression")

    predicate = statements[0]
    if not _is_boolean_predicate(predicate):
        raise ValueError("filtered index predicate must be one Boolean expression")
    if any(isinstance(node, _FILTERED_PREDICATE_UNSAFE_NODES) for node in predicate.walk()):
        raise ValueError("filtered index predicate contains a disallowed SQL construct")


def build_index_candidate_statement(
    candidate: IndexCandidate,
    *,
    online: bool = False,
) -> str:
    """Render a review-only CREATE INDEX statement, including a filter."""

    if not candidate.key_columns:
        raise ValueError("an index candidate requires at least one key column")
    compression = _validate_index_compression(candidate.compression)
    index_name = candidate.index_name or _derived_index_name(candidate)
    parsed_keys = tuple(parse_candidate_key(column) for column in candidate.key_columns)
    keys = ", ".join(
        f"{_quote_validated_identifier(column.name)} {column.direction}"
        for column in parsed_keys
    )
    include = ""
    if candidate.include_columns:
        include = " INCLUDE (" + ", ".join(
            _quote_identifier(column) for column in candidate.include_columns
        ) + ")"
    filter_clause = ""
    if candidate.filter_definition:
        filter_text = candidate.filter_definition.strip()
        _validate_filtered_index_predicate(filter_text)
        filter_clause = f" WHERE {filter_text}"
    options: list[str] = []
    if compression:
        options.append(f"DATA_COMPRESSION = {compression}")
    if online:
        options.append("ONLINE = ON")
    with_clause = f" WITH ({', '.join(options)})" if options else ""
    on_clause = ""
    if candidate.data_space_name:
        partition_clause = ""
        if candidate.partition_columns:
            partition_clause = "(" + ", ".join(
                _quote_identifier(column) for column in candidate.partition_columns
            ) + ")"
        on_clause = f" ON {_quote_identifier(candidate.data_space_name)}{partition_clause}"
    uniqueness = "UNIQUE " if candidate.is_unique else ""
    return (
        f"CREATE {uniqueness}NONCLUSTERED INDEX {_quote_identifier(index_name)} "
        f"ON {_quote_identifier(candidate.schema)}.{_quote_identifier(candidate.table)} "
        f"({keys}){include}{filter_clause}{with_clause}{on_clause};"
    )


def make_temporary_index_candidate(
    candidate: IndexCandidate,
    workflow_id: str,
) -> IndexCandidate:
    """Give a candidate a workflow-owned, deterministic temporary name."""

    workflow = re.sub(r"[^A-Za-z0-9_]", "_", workflow_id.strip())
    if not workflow:
        raise ValueError("workflow_id must not be empty")
    digest = hashlib.sha256(
        f"{candidate.schema}.{candidate.table}.{candidate.definition_fingerprint}".encode(
            "utf-8"
        )
    ).hexdigest()[:12]
    name = f"IX_Tuning_{workflow}_{digest}"[:128]
    return replace(candidate, index_name=name, workflow_id=workflow_id)


def expected_index_definition_matches(
    actual: ExistingIndex,
    expected: IndexCandidate,
) -> bool:
    if _canonical_catalog_identifier(actual.schema) != _canonical_catalog_identifier(
        expected.schema
    ):
        return False
    if _canonical_catalog_identifier(actual.table) != _canonical_catalog_identifier(
        expected.table
    ):
        return False
    if not expected.index_name or _canonical_catalog_identifier(actual.name) != (
        _canonical_catalog_identifier(expected.index_name)
    ):
        return False
    if str(actual.index_type).strip().upper() != "NONCLUSTERED":
        return False
    expected_keys = tuple(parse_candidate_key(column) for column in expected.key_columns)
    if len(actual.key_columns) != len(expected_keys) or any(
        _canonical_catalog_identifier(actual_key.name)
        != _canonical_catalog_identifier(expected_key.name)
        or actual_key.direction != expected_key.direction
        for actual_key, expected_key in zip(actual.key_columns, expected_keys)
    ):
        return False
    if tuple(_canonical_catalog_identifier(column) for column in actual.include_columns) != tuple(
        _canonical_catalog_identifier(column) for column in expected.include_columns
    ):
        return False
    if _canonical_filter_definition(actual.filter_definition) != _canonical_filter_definition(
        expected.filter_definition
    ):
        return False
    if expected.is_unique != actual.is_unique:
        return False
    if actual.is_primary_key or actual.is_unique_constraint:
        return False
    if actual.constraint_name is not None or actual.constraint_type is not None:
        return False
    if actual.is_disabled:
        return False
    if actual.fill_factor != 0:
        return False

    expected_data_space = (
        _canonical_catalog_identifier(expected.data_space_name)
        if expected.data_space_name
        else None
    )
    actual_data_space = (
        _canonical_catalog_identifier(actual.data_space_name)
        if actual.data_space_name
        else None
    )
    if expected_data_space is not None and actual_data_space != expected_data_space:
        return False
    expected_partitions = tuple(
        _canonical_catalog_identifier(column) for column in expected.partition_columns
    )
    actual_partitions = tuple(
        _canonical_catalog_identifier(column) for column in actual.partition_columns
    )
    if actual_partitions != expected_partitions:
        return False
    actual_is_partition_scheme = (
        str(actual.data_space_type or "").strip().upper() == "PARTITION_SCHEME"
    )
    if expected_partitions:
        if expected_data_space is None or not actual_is_partition_scheme:
            return False
        expected_scheme = _canonical_catalog_identifier(
            expected.partition_scheme_name or expected_data_space
        )
        if _canonical_catalog_identifier(actual.partition_scheme_name or "") != expected_scheme:
            return False
        if not actual.partition_function_name:
            return False
        if expected.partition_function_name is not None and (
            _canonical_catalog_identifier(actual.partition_function_name)
            != _canonical_catalog_identifier(expected.partition_function_name)
        ):
            return False
    elif (
        actual_is_partition_scheme
        or actual.partition_scheme_name is not None
        or actual.partition_function_name is not None
        or expected.partition_scheme_name is not None
        or expected.partition_function_name is not None
    ):
        return False
    expected_compression = str(expected.compression or "NONE").strip().upper()
    actual_compression = tuple(
        str(compression).strip().upper()
        for _partition_number, compression in actual.partition_compression
    )
    if expected_compression == "NONE":
        if any(compression != "NONE" for compression in actual_compression):
            return False
    elif not actual_compression or any(
        compression != expected_compression for compression in actual_compression
    ):
        return False
    return True


def _identifier_spelling(value: str) -> str:
    """Remove one SQL identifier delimiter pair without changing its spelling."""

    text = str(value).strip()
    if len(text) >= 2 and text[0] == "[" and text[-1] == "]":
        return text[1:-1]
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        return text[1:-1]
    return text


def _canonical_catalog_identifier(value: str) -> str:
    """Compare names using the catalog's delimiter-independent spelling.

    Azure SQL's normal catalog lookup is case-insensitive for the default
    database collations. The observed name comes from ``sys.*`` while the
    expected name comes from caller SQL, so casefolding and removing one pair
    of identifier delimiters avoids treating the same catalog object as a
    different owner. Constraint, disabled-state, key-order, and full-shape
    checks still fence ownership.
    """

    return _identifier_spelling(value).casefold()


def _canonical_filter_definition(value: str | None) -> str | None:
    """Normalize catalog filter text without changing literal values.

    SQL Server commonly returns generated filter text with redundant
    parentheses. Parse the restricted predicate grammar already admitted by
    this module, remove only ``Paren`` nodes, and casefold identifiers while
    preserving string and numeric literal values.
    """

    normalized = normalize_index_definition(value)
    if normalized is None:
        return None
    try:
        statements = parse(normalized, read="tsql")
        if len(statements) != 1 or statements[0] is None:
            return normalized
        expression = statements[0]
        while any(isinstance(node, exp.Paren) for node in expression.walk()):
            expression = expression.transform(
                lambda node: node.this if isinstance(node, exp.Paren) else node
            )
        for identifier in expression.find_all(exp.Identifier):
            identifier.set("this", str(identifier.this).casefold())
        return expression.sql(dialect="tsql", normalize=True)
    except (ParseError, ValueError):
        # Verification must fail closed for text that cannot be normalized.
        return normalized


def _validate_index_compression(value: str | None) -> str | None:
    """Validate rowstore DATA_COMPRESSION before it reaches generated DDL."""

    if value is None or not str(value).strip():
        return None
    normalized = str(value).strip().upper()
    if normalized not in _SUPPORTED_ROWSTORE_INDEX_COMPRESSIONS:
        supported = ", ".join(sorted(_SUPPORTED_ROWSTORE_INDEX_COMPRESSIONS))
        raise ValueError(
            f"unsupported rowstore index compression {value!r}; use one of {supported}"
        )
    return normalized


def _derived_index_name(candidate: IndexCandidate) -> str:
    digest = candidate.definition_fingerprint[:12]
    table = re.sub(r"[^A-Za-z0-9_]", "_", candidate.table).strip("_") or "Table"
    return f"IX_Tuning_{table}_{digest}"[:128]


def _parse_identifier_input(value: str) -> str:
    raw = str(value).strip()
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", raw):
        return raw
    bracketed = re.fullmatch(r"\[([^\]\r\n]+)\]", raw)
    if bracketed is not None:
        return bracketed.group(1)
    raise ValueError(f"invalid SQL identifier: {value!r}")


def _quote_validated_identifier(value: str) -> str:
    if not value or "]" in value or "\r" in value or "\n" in value:
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return f"[{value}]"


def _quote_identifier(value: str) -> str:
    return _quote_validated_identifier(_parse_identifier_input(value))


def _normalize_plan_index_name(value: str) -> str:
    parts = [part.strip().strip("[]") for part in str(value).split(".")]
    return parts[-1] if parts else ""


def _verified_expected_index_use(
    measurement: ExperimentMeasurement,
    expected_index_name: str | None,
) -> bool:
    plan_use = measurement.plan_use
    if not expected_index_name or plan_use is None or not plan_use.verified:
        return False
    expected = _normalize_plan_index_name(expected_index_name)
    if _normalize_plan_index_name(plan_use.expected_index_name) != expected:
        return False
    return any(
        _normalize_plan_index_name(name) == expected
        for name in plan_use.matched_index_names
    )


def _is_prefix(shorter: tuple[str, ...], longer: tuple[str, ...]) -> bool:
    """Return True if shorter is a prefix of (or equal to) longer."""
    if len(shorter) > len(longer):
        return False
    return longer[: len(shorter)] == shorter


def _merge_unique(first: list[str], second: list[str]) -> list[str]:
    merged: list[str] = list(first)
    for col in second:
        if col and col not in merged:
            merged.append(col)
    return merged


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
