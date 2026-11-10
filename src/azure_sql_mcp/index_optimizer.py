from __future__ import annotations

import asyncio
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
from typing import Mapping
from typing import Sequence

from sqlglot import exp
from sqlglot import parse
from sqlglot.errors import ParseError

from .connection import AzureSqlExecutor
from .index_metadata import ExistingIndex
from .index_metadata import IndexKeyColumn
from .index_metadata import TableColumnFacts
from .index_metadata import coerce_existing_indexes
from .index_metadata import collect_existing_indexes
from .index_metadata import existing_index_covers_candidate
from .index_metadata import existing_index_response
from .index_metadata import index_definition_fingerprint
from .index_metadata import normalize_index_definition
from .index_metadata import parse_candidate_key
from .index_recommendations import split_index_columns
from .observability import sanitize_error_message
from .param_binding import ParameterBindingService
from .plan_tree import describe_result_shape
from .plan_tree import showplan_from_results
from .safe_sql import SafeSqlValidator
from .showplan_access import parse_plan_access

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

# What an IndexCandidate builds. The default is the nonclustered B-tree index
# every candidate was before the kind existed.
INDEX_KIND_ROWSTORE = "rowstore"
# CREATE NONCLUSTERED COLUMNSTORE INDEX: ``key_columns`` holds the columns of the
# index (it has no key, no INCLUDE list and no ASC or DESC).
INDEX_KIND_NONCLUSTERED_COLUMNSTORE = "nonclustered_columnstore"
# The unique clustered index that materialises a schema-bound view. Only the
# view experiment builds it; the test index path refuses it.
INDEX_KIND_VIEW_CLUSTERED = "view_unique_clustered"
# The kinds benchmark_index_candidate may build on a table.
TEST_INDEX_KINDS = (INDEX_KIND_ROWSTORE, INDEX_KIND_NONCLUSTERED_COLUMNSTORE)
_CATALOG_INDEX_TYPE = {
    INDEX_KIND_ROWSTORE: "NONCLUSTERED",
    INDEX_KIND_NONCLUSTERED_COLUMNSTORE: "NONCLUSTERED COLUMNSTORE",
    INDEX_KIND_VIEW_CLUSTERED: "CLUSTERED",
}

# A rowstore index statement takes NONE, ROW or PAGE. COLUMNSTORE and
# COLUMNSTORE_ARCHIVE belong to columnstore index DDL only.
_SUPPORTED_ROWSTORE_INDEX_COMPRESSIONS = frozenset({"NONE", "ROW", "PAGE"})
_COLUMNSTORE_DEFAULT_COMPRESSION = "COLUMNSTORE"
_SUPPORTED_COLUMNSTORE_COMPRESSIONS = frozenset({"COLUMNSTORE", "COLUMNSTORE_ARCHIVE"})
# Engine limit of a nonclustered columnstore index.
MAX_COLUMNSTORE_INDEX_COLUMNS = 1024
# System types a columnstore index can hold (Microsoft Learn, CREATE
# COLUMNSTORE INDEX, limitations; each one and vector observed on Azure SQL
# Database). A (max) length is checked apart from the type: only a clustered
# columnstore index holds it. Every other type is refused there with error
# 35343: text, ntext, image, xml, sql_variant, timestamp, json, CLR types.
_COLUMNSTORE_COLUMN_TYPES = frozenset(
    {
        "bigint", "int", "smallint", "tinyint", "bit",
        "decimal", "numeric", "money", "smallmoney", "float", "real",
        "date", "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time",
        "char", "varchar", "nchar", "nvarchar", "binary", "varbinary",
        "uniqueidentifier", "sysname", "vector",
    }
)  # fmt: skip

# Test-index builds wait for their final schema lock at low priority, so readers
# and writers do not queue behind them, and give up themselves when it runs out.
TEST_INDEX_LOW_PRIORITY_WAIT_MINUTES = 1
TEST_INDEX_DROP_LOCK_TIMEOUT_MS = 5_000
# The engine accepts MAXDOP 0 to 64; 0 means "use the default", which is the absent option here.
_MAX_MAXDOP = 64
REPLACEMENT_HIDE_METHODS = ("table_hint", "disable_in_sandbox")
# A/B/A: baseline before, with the index, baseline after.
PROOF_SET_PHASES = 3
# The advisor's rollback rule: a query that slows more than this fails the proof.
PROOF_SET_MAX_REGRESSION_PCT = 20.0

INDEX_SCORE_ALPHA = 1.5
INDEX_SCORE_BETA = 0.5
INDEX_CANDIDATE_IMPACT_FLOOR_PCT = 5.0


def _index_candidate_score_components(
    statement_subtree_cost: float | None,
    execution_count: float | None,
    impact_pct: float | None,
    estimated_size_mb: float | None,
    write_ratio: float | None,
    alpha: float,
    beta: float,
) -> tuple[float, float] | None:
    """Return the shared read benefit and score, or no score if incomplete."""

    values = (
        statement_subtree_cost,
        execution_count,
        impact_pct,
        estimated_size_mb,
        write_ratio,
        alpha,
        beta,
    )
    numeric_values: list[float] = []
    for value in values:
        if (
            value is None
            or isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
        ):
            return None
        numeric_values.append(float(value))
    (
        statement_subtree_cost_value,
        execution_count_value,
        impact_pct_value,
        estimated_size_mb_value,
        write_ratio_value,
        alpha_value,
        beta_value,
    ) = numeric_values
    if (
        statement_subtree_cost_value < 0
        or execution_count_value < 0
        or not 0 <= impact_pct_value <= 100
        or estimated_size_mb_value < 0
        or not 0 <= write_ratio_value <= 1
        or alpha_value < 0
        or beta_value < 0
    ):
        return None

    read_benefit = (
        (impact_pct_value / 100.0)
        * max(statement_subtree_cost_value, 0.001)
        * max(execution_count_value, 1.0)
    )
    score = (
        math.log(read_benefit + 1.0)
        - alpha_value * math.log(estimated_size_mb_value + 1.0)
        - beta_value * math.log(write_ratio_value + 0.01)
    )
    return read_benefit, score


def score_index_candidate(
    statement_subtree_cost: float | None,
    execution_count: float | None,
    impact_pct: float | None,
    estimated_size_mb: float | None,
    write_ratio: float | None,
    alpha: float = INDEX_SCORE_ALPHA,
    beta: float = INDEX_SCORE_BETA,
) -> float | None:
    """Score one index candidate using the optimizer's established model.

    All five evidence inputs are required.  Returning ``None`` for absent or
    invalid input keeps callers from turning incomplete evidence into a
    recommendation.
    """

    components = _index_candidate_score_components(
        statement_subtree_cost,
        execution_count,
        impact_pct,
        estimated_size_mb,
        write_ratio,
        alpha,
        beta,
    )
    return components[1] if components is not None else None


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
    # One of the INDEX_KIND_ values. For a nonclustered columnstore index,
    # ``key_columns`` holds the columns of the index.
    index_kind: str = INDEX_KIND_ROWSTORE
    # ORDER (...) of a nonclustered columnstore index: columns of the index,
    # in the order the build sorts by. Empty: an index with no order.
    order_columns: tuple[str, ...] = ()

    @property
    def definition_fingerprint(self) -> str:
        compression = self.compression.upper() if self.compression else None
        if (
            self.index_kind == INDEX_KIND_NONCLUSTERED_COLUMNSTORE
            and compression == _COLUMNSTORE_DEFAULT_COMPRESSION
        ):
            compression = None
        partition_compression = ((0, compression),) if compression else ()
        return index_definition_fingerprint(
            schema=self.schema,
            table=self.table,
            # An unknown kind fingerprints as itself; the DDL builder refuses it.
            index_type=_CATALOG_INDEX_TYPE.get(self.index_kind, self.index_kind),
            key_columns=self.key_columns,
            include_columns=self.include_columns,
            filter_definition=self.filter_definition,
            is_unique=self.is_unique,
            partition_columns=self.partition_columns,
            data_space_name=self.data_space_name,
            partition_scheme_name=self.partition_scheme_name,
            partition_function_name=self.partition_function_name,
            partition_compression=partition_compression,
            columnstore_order_columns=self.order_columns,
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
            "index_kind": self.index_kind,
            **({"order_columns": list(self.order_columns)} if self.order_columns else {}),
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
    existing_indexes_omitted: int = 0
    existing_indexes_note: str | None = None

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
            "existing_indexes_total": self.existing_indexes_count,
            "existing_indexes_omitted": self.existing_indexes_omitted,
            **(
                {"existing_indexes_note": self.existing_indexes_note}
                if self.existing_indexes_note
                else {}
            ),
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
        alpha: float = INDEX_SCORE_ALPHA,
        beta: float = INDEX_SCORE_BETA,
        min_improvement_pct: float = INDEX_CANDIDATE_IMPACT_FLOOR_PCT,
        *,
        include_all_existing_indexes: bool = False,
    ) -> OptimizationResult:
        # Step 1: collect workload + existing indexes
        workload = await self._collect_workload(database_name, window_minutes, top_n)
        dmv_recs = await self._get_dmv_missing_indexes(database_name)
        existing = await self._get_existing_indexes(database_name)

        # Step 2: generate candidates
        raw = self._generate_candidates(workload, dmv_recs)
        candidates_generated = len(raw)
        # The response lists the existing indexes of these tables only.
        touched_tables = {(candidate.schema, candidate.table) for candidate in raw}
        for query_row in workload:
            touched_tables.update(
                (access.schema, access.table)
                for access in parse_plan_access(query_row.get("plan_xml") or "").accesses
            )

        # Step 3: filter against existing indexes (including prefix)
        raw = self._filter_existing_indexes(raw, existing)

        # Step 4: overlap detection & prefix subsumption
        raw = self._detect_prefix_subsumption(raw)
        candidates_after = len(raw)

        # Step 5: enrich with size + write ratio. Candidates on one table share
        # its row count, column widths and write ratio for this run.
        lookups: dict[tuple[str, str, str], Any] = {}
        for candidate in raw:
            candidate_cols = candidate.key_columns + [
                c for c in candidate.include_columns if c not in candidate.key_columns
            ]
            size = await self._estimate_index_size(
                database_name, candidate.schema, candidate.table, candidate_cols, lookups,
            )
            candidate.estimated_size_mb = size  # type: ignore[attr-defined]

            ratio_key = ("write_ratio", candidate.schema, candidate.table)
            if ratio_key not in lookups:
                lookups[ratio_key] = await self._get_table_write_ratio(
                    database_name, candidate.schema, candidate.table,
                )
            candidate.write_ratio = lookups[ratio_key]  # type: ignore[attr-defined]

        # Step 6: score
        scored = self._score_candidates(raw, alpha, beta)

        # Step 7: greedy select
        selected = self._greedy_select(scored, budget_mb, min_improvement_pct)

        total_size = sum(s.estimated_size_mb for s in selected)
        existing_response = existing_index_response(
            existing, touched_tables, full=include_all_existing_indexes
        )

        return OptimizationResult(
            database_name=database_name,
            workload_window_minutes=window_minutes,
            queries_analyzed=len(workload),
            candidates_generated=candidates_generated,
            candidates_after_consolidation=candidates_after,
            selected_indexes=selected,
            total_estimated_size_mb=total_size,
            existing_indexes_count=existing_response["existing_indexes_total"],
            existing_index_metadata=existing_response["existing_indexes"],
            budget_mb=budget_mb,
            alpha=alpha,
            beta=beta,
            existing_indexes_omitted=existing_response["existing_indexes_omitted"],
            existing_indexes_note=existing_response.get("existing_indexes_note"),
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
        lookups: dict[tuple[str, str, str], Any] | None = None,
    ) -> float | None:
        """Estimate index size in MB using row count and column widths.

        ``lookups`` keeps a table's row count and column widths for one
        optimize() run, so each is read once per table.
        """
        if lookups is None:
            lookups = {}
        count_key = ("row_count", schema, table)
        if count_key not in lookups:
            lookups[count_key] = await self._get_row_count(database_name, schema, table)
        row_count = lookups[count_key]
        if row_count is None:
            return None
        if row_count == 0:
            return 0.0

        # None marks a column that was asked for and has no width.
        known: dict[str, int | None] = lookups.setdefault(("column_widths", schema, table), {})
        unread = [column for column in columns if column not in known]
        if unread:
            fetched = await self._get_column_widths(database_name, schema, table, unread)
            known.update((column, fetched.get(column)) for column in unread)
        col_widths = [width for column in columns if (width := known[column]) is not None]
        if not col_widths or len(col_widths) != len(columns):
            return None

        total_key_width = sum(col_widths) + _INDEX_ROW_OVERHEAD
        rows_per_page = max(1, _USABLE_PAGE_BYTES // (total_key_width + _SLOT_ARRAY_ENTRY))
        leaf_pages = math.ceil(row_count / rows_per_page)
        total_bytes = leaf_pages * _PAGE_SIZE_BYTES * _NON_LEAF_MULTIPLIER
        return total_bytes / (1024.0 * 1024.0)

    async def _get_table_write_ratio(
        self, database_name: str, schema: str, table: str,
    ) -> float | None:
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
            try:
                write_ratio = float(rows[0]["write_ratio"])
            except (TypeError, ValueError):
                return None
            if math.isfinite(write_ratio) and 0.0 <= write_ratio <= 1.0:
                return write_ratio
        return None

    async def _get_row_count(
        self, database_name: str, schema: str, table: str,
    ) -> int | None:
        query = """
        SELECT SUM(p.row_count) AS row_count
        FROM sys.dm_db_partition_stats p
        JOIN sys.tables t ON p.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ? AND p.index_id IN (0, 1)
        """
        rows = await self.executor.fetch_all(database_name, query, params=[schema, table])
        if rows and rows[0].get("row_count") is not None:
            try:
                row_count = int(rows[0]["row_count"])
            except (TypeError, ValueError):
                return None
            return row_count if row_count >= 0 else None
        return None

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
        widths: dict[str, int] = {}
        for row in rows:
            column_name = row.get("column_name")
            if not column_name or row.get("max_length") is None:
                continue
            try:
                width = int(row["max_length"])
            except (TypeError, ValueError):
                continue
            if width >= 0:
                widths[column_name] = width
        return widths

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
            size_mb = getattr(rc, "estimated_size_mb", None)
            write_ratio = getattr(rc, "write_ratio", None)
            components = _index_candidate_score_components(
                rc.statement_subtree_cost,
                rc.total_executions,
                rc.impact_pct,
                size_mb,
                write_ratio,
                alpha,
                beta,
            )
            if components is None:
                continue
            read_benefit, score = components
            if (
                not isinstance(size_mb, (int, float))
                or isinstance(size_mb, bool)
                or not isinstance(write_ratio, (int, float))
                or isinstance(write_ratio, bool)
            ):
                continue

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
        plan_xml = showplan_from_results(plan_results)
        if plan_xml is None:
            raise RuntimeError(f"No SHOWPLAN XML returned; {describe_result_shape(plan_results)}.")
        return plan_xml

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
    # exp.National is an N'...' Unicode string literal.
    if isinstance(node, (exp.Literal, exp.Boolean, exp.National)):
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
    # SQL Server stores CAST as CONVERT([type],...) and float constants as
    # 1.000000000000000e+003, so a created index with either cannot be
    # verified against sys.indexes.filter_definition.
    for node in predicate.walk():
        if isinstance(node, (exp.Cast, exp.Convert)) or (
            isinstance(node, exp.Literal)
            and not node.is_string
            and "e" in str(node.this).casefold()
        ):
            # No quoted example: the tool error handler redacts quoted text.
            raise ValueError(
                "filtered index predicate must compare columns with a plain literal, "
                "for example [Status] = 1 or [Amount] > 1000.0 (compare a date column "
                "with a plain quoted date string); CAST, CONVERT and "
                "scientific-notation constants cannot be verified after CREATE"
            )


def build_index_candidate_statement(
    candidate: IndexCandidate,
    *,
    online: bool = False,
    low_priority_wait_minutes: int | None = None,
    maxdop: int | None = None,
) -> str:
    """Render the review-only CREATE INDEX statement of a candidate, by its kind.

    ``low_priority_wait_minutes`` (online builds only) adds
    WAIT_AT_LOW_PRIORITY with ABORT_AFTER_WAIT = SELF; ``maxdop`` adds MAXDOP.
    Raises ValueError for a definition the statement of that kind cannot hold.
    """

    if low_priority_wait_minutes is not None and not 1 <= int(low_priority_wait_minutes) <= 1440:
        raise ValueError("low_priority_wait_minutes must be between 1 and 1440")
    if maxdop is not None and not 1 <= int(maxdop) <= _MAX_MAXDOP:
        raise ValueError(f"maxdop must be between 1 and {_MAX_MAXDOP}")
    if candidate.index_kind not in _CATALOG_INDEX_TYPE:
        raise ValueError(
            f"unsupported index_kind {candidate.index_kind!r}; use one of "
            + ", ".join(_CATALOG_INDEX_TYPE)
        )
    if candidate.index_kind == INDEX_KIND_NONCLUSTERED_COLUMNSTORE:
        return _columnstore_index_statement(
            candidate,
            online=online,
            low_priority_wait_minutes=low_priority_wait_minutes,
            maxdop=maxdop,
        )
    if candidate.order_columns:
        raise ValueError(
            f"order_columns belongs to index kind {INDEX_KIND_NONCLUSTERED_COLUMNSTORE}; "
            "a rowstore index is ordered by its key columns"
        )

    if not candidate.key_columns:
        raise ValueError("an index candidate requires at least one key column")
    clustered = candidate.index_kind == INDEX_KIND_VIEW_CLUSTERED
    if clustered:
        if not candidate.is_unique:
            raise ValueError("the clustered index of a view must be unique")
        if candidate.include_columns or candidate.filter_definition:
            raise ValueError("a clustered index has no INCLUDE columns and no filter")
        if online:
            raise ValueError("the first clustered index of a view cannot be built online")
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
    with_clause = _with_clause(compression, online, low_priority_wait_minutes, maxdop)
    uniqueness = "UNIQUE " if candidate.is_unique else ""
    index_type = "CLUSTERED" if clustered else "NONCLUSTERED"
    return (
        f"CREATE {uniqueness}{index_type} INDEX {_quote_identifier(index_name)} "
        f"ON {_quote_identifier(candidate.schema)}.{_quote_identifier(candidate.table)} "
        f"({keys}){include}{_filter_clause(candidate)}{with_clause}{_on_clause(candidate)};"
    )


def _with_clause(
    compression: str | None,
    online: bool,
    low_priority_wait_minutes: int | None,
    maxdop: int | None,
) -> str:
    options: list[str] = []
    if compression:
        options.append(f"DATA_COMPRESSION = {compression}")
    if online and low_priority_wait_minutes is not None:
        options.append(
            "ONLINE = ON (WAIT_AT_LOW_PRIORITY (MAX_DURATION = "
            f"{int(low_priority_wait_minutes)} MINUTES, ABORT_AFTER_WAIT = SELF))"
        )
    elif online:
        options.append("ONLINE = ON")
    if maxdop is not None:
        options.append(f"MAXDOP = {int(maxdop)}")
    return f" WITH ({', '.join(options)})" if options else ""


def _filter_clause(candidate: IndexCandidate) -> str:
    if not candidate.filter_definition:
        return ""
    filter_text = candidate.filter_definition.strip()
    _validate_filtered_index_predicate(filter_text)
    return f" WHERE {filter_text}"


def _on_clause(candidate: IndexCandidate) -> str:
    if not candidate.data_space_name:
        return ""
    partition_clause = ""
    if candidate.partition_columns:
        partition_clause = "(" + ", ".join(
            _quote_identifier(column) for column in candidate.partition_columns
        ) + ")"
    return f" ON {_quote_identifier(candidate.data_space_name)}{partition_clause}"


def _columnstore_column_names(candidate: IndexCandidate) -> tuple[str, ...]:
    """The column names of a columnstore candidate, as written, without delimiters."""

    names: list[str] = []
    for column in candidate.key_columns:
        if re.search(r"\s(?:ASC|DESC)\s*$", str(column), re.IGNORECASE):
            raise ValueError(
                f"a columnstore index column takes no ASC or DESC: {str(column).strip()}"
            )
        names.append(_parse_identifier_input(column))
    return tuple(names)


def columnstore_order_column_names(candidate: IndexCandidate) -> tuple[str, ...]:
    """The validated ORDER (...) column names of a columnstore candidate.

    Each must be a column of the index (the engine refuses any other with
    error 1911), named once, with no ASC or DESC.
    """

    index_columns = {name.casefold() for name in _columnstore_column_names(candidate)}
    names: list[str] = []
    for column in candidate.order_columns:
        if re.search(r"\s(?:ASC|DESC)\s*$", str(column), re.IGNORECASE):
            raise ValueError(
                f"a columnstore ORDER column takes no ASC or DESC: {str(column).strip()}"
            )
        name = _parse_identifier_input(column)
        if name.casefold() not in index_columns:
            raise ValueError(
                f"order column {name} is not in the index column list; ORDER takes "
                "columns of the columnstore index only (engine error 1911)"
            )
        if name.casefold() in {item.casefold() for item in names}:
            raise ValueError(f"column {name} is named twice in order_columns")
        names.append(name)
    return tuple(names)


def _columnstore_index_statement(
    candidate: IndexCandidate,
    *,
    online: bool,
    low_priority_wait_minutes: int | None,
    maxdop: int | None,
) -> str:
    """CREATE NONCLUSTERED COLUMNSTORE INDEX with an optional ORDER (...),
    DATA_COMPRESSION (COLUMNSTORE or COLUMNSTORE_ARCHIVE), ONLINE and MAXDOP.

    Azure SQL Database accepts the WAIT_AT_LOW_PRIORITY form of ONLINE for
    this statement, also with ORDER (observed), though its documented syntax
    does not list it.
    """

    if candidate.include_columns:
        raise ValueError(
            "a columnstore index has no INCLUDE list; name every column in key_columns"
        )
    if candidate.is_unique:
        raise ValueError("a columnstore index cannot be unique")
    names = _columnstore_column_names(candidate)
    if not names:
        raise ValueError("a nonclustered columnstore index requires at least one column")
    if len(names) > MAX_COLUMNSTORE_INDEX_COLUMNS:
        raise ValueError(
            "a nonclustered columnstore index holds at most "
            f"{MAX_COLUMNSTORE_INDEX_COLUMNS} columns"
        )
    folded = [name.casefold() for name in names]
    repeated = next((name for name in names if folded.count(name.casefold()) > 1), None)
    if repeated is not None:
        raise ValueError(f"column {repeated} is named twice in the columnstore index")
    compression = _validate_columnstore_compression(candidate.compression)
    index_name = candidate.index_name or _derived_index_name(candidate)
    columns = ", ".join(_quote_validated_identifier(name) for name in names)
    order_names = columnstore_order_column_names(candidate)
    order_clause = (
        " ORDER (" + ", ".join(_quote_validated_identifier(name) for name in order_names) + ")"
        if order_names
        else ""
    )
    with_clause = _with_clause(compression, online, low_priority_wait_minutes, maxdop)
    return (
        f"CREATE NONCLUSTERED COLUMNSTORE INDEX {_quote_identifier(index_name)} "
        f"ON {_quote_identifier(candidate.schema)}.{_quote_identifier(candidate.table)} "
        f"({columns}){order_clause}{_filter_clause(candidate)}{with_clause}"
        f"{_on_clause(candidate)};"
    )


def columnstore_index_blockers(
    candidate: IndexCandidate,
    *,
    existing_indexes: Sequence[ExistingIndex],
    table_columns: Sequence[TableColumnFacts],
) -> tuple[str, ...]:
    """Why the engine would refuse this nonclustered columnstore index; empty when it would not.

    Checked from the catalog before any query runs: a table holds one
    columnstore index (clustered or nonclustered, enabled or disabled), and a
    nonclustered columnstore index cannot hold a (max) column, a computed or
    sparse column, or a type outside the documented list. ``existing_indexes``
    are the indexes of the database or of the table (collect_existing_indexes);
    ``table_columns`` are the columns of the table (collect_table_column_facts).
    """

    if candidate.index_kind != INDEX_KIND_NONCLUSTERED_COLUMNSTORE:
        raise ValueError(
            f"columnstore_index_blockers checks the {INDEX_KIND_NONCLUSTERED_COLUMNSTORE} "
            f"kind only, not {candidate.index_kind!r}"
        )
    table = f"{candidate.schema}.{candidate.table}"
    blockers: list[str] = []
    for index in existing_indexes:
        if (
            _canonical_catalog_identifier(index.schema)
            == _canonical_catalog_identifier(candidate.schema)
            and _canonical_catalog_identifier(index.table)
            == _canonical_catalog_identifier(candidate.table)
            and (
                index.index_type_code in {5, 6}
                or "COLUMNSTORE" in str(index.index_type).upper()
            )
        ):
            state = ", disabled" if index.is_disabled else ""
            blockers.append(
                f"{table} already has the columnstore index {index.name} "
                f"({str(index.index_type).strip().lower()}{state}). Azure SQL Database allows "
                "one columnstore index on a table (engine error 35339), so a nonclustered "
                "columnstore test index cannot be built on it."
            )
            break
    facts = {column.name.casefold(): column for column in table_columns}
    seen: set[str] = set()
    for name in _columnstore_column_names(candidate):
        folded = name.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        reason = _columnstore_column_blocker(facts.get(folded))
        if reason is not None:
            blockers.append(f"Column {name} of {table} {reason}.")
    return tuple(blockers)


def _columnstore_column_blocker(column: TableColumnFacts | None) -> str | None:
    if column is None:
        return "does not exist"
    type_name = column.type_name.strip().lower()
    if column.is_computed:
        return (
            "is a computed column, which a nonclustered columnstore index cannot hold "
            "(engine error 35307)"
        )
    if column.is_sparse:
        return "is a sparse column, which a columnstore index cannot hold (engine error 35309)"
    if type_name not in _COLUMNSTORE_COLUMN_TYPES:
        return f"has type {type_name}, which a columnstore index cannot hold (engine error 35343)"
    if column.max_length == -1:
        return (
            f"has type {type_name}(max): only a clustered columnstore index holds a (max) "
            "column, a nonclustered one cannot (engine error 35343)"
        )
    return None


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


def build_test_index_statement(
    candidate: IndexCandidate,
    *,
    online: bool = True,
    allow_offline: bool = False,
    maxdop: int | None = None,
    low_priority_wait_minutes: int = TEST_INDEX_LOW_PRIORITY_WAIT_MINUTES,
) -> str:
    """CREATE INDEX for a workflow-managed test index on a sandbox database.

    An online build waits for its last lock at low priority and aborts
    itself, so it never heads a blocking chain. An offline build blocks every
    writer of the table for the whole build: refused unless the database policy
    allows it. ``maxdop`` comes from policy; None leaves the database default.
    The candidate's ``index_kind`` chooses a B-tree or a nonclustered
    columnstore index.
    """

    if candidate.index_kind == INDEX_KIND_VIEW_CLUSTERED:
        raise ValueError(
            "A clustered index is built only on a leased test view, by the view "
            "experiment; a test index on a table is rowstore or nonclustered columnstore."
        )
    if not online and not allow_offline:
        raise PermissionError(
            "An offline test index build blocks all writes to the table while it runs. "
            "The database policy does not allow it; use online=True."
        )
    return build_index_candidate_statement(
        candidate,
        online=online,
        low_priority_wait_minutes=low_priority_wait_minutes if online else None,
        maxdop=maxdop,
    )


def bounded_table_lock_probe_sql(
    schema: str, table: str, *, lock_timeout_ms: int = TEST_INDEX_DROP_LOCK_TIMEOUT_MS
) -> str:
    """The table lock probe of the guarded test-index drop, with a short lock wait.

    The probe and the DROP INDEX after it then fail fast with error 1222
    instead of waiting for the whole statement timeout behind a long reader.
    """

    if not 1 <= int(lock_timeout_ms) <= 60_000:
        raise ValueError("lock_timeout_ms must be between 1 and 60000")
    target = f"{_quote_catalog_identifier(schema)}.{_quote_catalog_identifier(table)}"
    return (
        f"SET LOCK_TIMEOUT {int(lock_timeout_ms)};\n"
        f"SELECT TOP (1) @table_lock_probe = 1 FROM {target} WITH (TABLOCK, HOLDLOCK);"
    )


def _quote_catalog_identifier(value: str) -> str:
    """Quote a name read from the catalog, which may hold any character."""

    if not value or "\r" in value or "\n" in value:
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return "[" + value.replace("]", "]]") + "]"


# Server errors that end a CREATE INDEX batch with nothing committed (the
# workflow runs it under XACT_ABORT ON in one transaction).
_DEFINITE_CREATE_FAILURES: dict[int, str] = {
    # The engine rolls the victim's transaction back before it reports 1205.
    1205: "deadlock_victim",
    1222: "lock_timeout_or_low_priority_abort",
    1505: "duplicate_key",
    102: "compile_error",
    156: "compile_error",
    207: "compile_error",
    208: "compile_error",
    1088: "compile_error",
    1911: "compile_error",
    1913: "compile_error",
    1919: "compile_error",
}
# mssql-python does not always expose the error number; the server text names these.
_DEFINITE_CREATE_FAILURE_TEXTS: tuple[tuple[re.Pattern[str], int], ...] = (
    (re.compile(r"chosen as the deadlock victim", re.IGNORECASE), 1205),
    (re.compile(r"Lock request time out period exceeded", re.IGNORECASE), 1222),
    (re.compile(r"terminated because a duplicate key was found", re.IGNORECASE), 1505),
    (re.compile(r"does not exist in the target table or view", re.IGNORECASE), 1911),
    (re.compile(r"Invalid column name", re.IGNORECASE), 207),
    (re.compile(r"Invalid object name", re.IGNORECASE), 208),
)
# A build that lost to another session, not to its own definition: the same
# statement can succeed on the next try.
TRANSIENT_BUILD_ERRORS = frozenset({1205, 1222})
# 3621 "The statement has been terminated" only accompanies another error.
_COMPANION_ERROR_NUMBERS = frozenset({3621})
# The number that ends one diagnostic record; "(5)." inside the text is a row value.
_RECORD_ERROR_NUMBER = re.compile(r"\((\d{1,6})\)(?=\s*\(SQL\w+\)|\s*;|['\"\s)]*$)")
_SQLSTATE = re.compile(r"\[(\w{5})\]")
_TRANSPORT_TEXT = re.compile(
    r"communication link|tcp provider|timeout expired|connection (?:is |was )?(?:broken|closed|reset)",
    re.IGNORECASE,
)


def _transport_sqlstate(state: str) -> bool:
    upper = state.upper()
    return upper.startswith("08") or upper in {"HYT00", "HYT01", "HY008"}


def classify_create_index_failure(error: BaseException) -> dict[str, Any]:
    """Tell a CREATE INDEX that definitely rolled back from one whose outcome is unknown.

    ``definite_rollback`` needs a listed server error (lock timeout or the
    low-priority abort 1222, a compile error, a duplicate key) and no sign of a
    lost connection, client timeout or cancel. Anything else is ``unknown``:
    the CREATE may still be running or may have committed.
    """

    texts: list[str] = []
    transport = False
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (TimeoutError, ConnectionError, asyncio.CancelledError)):
            transport = True
        for value in (*getattr(current, "args", ()), getattr(current, "ddbc_error", None), getattr(current, "driver_error", None)):
            if not isinstance(value, str):
                continue
            if len(value) == 5 and value.isalnum() and _transport_sqlstate(value):
                transport = True
            texts.append(value)
        current = current.__cause__ or current.__context__
    numbers: list[int] = []
    for text in texts:
        if _TRANSPORT_TEXT.search(text) or any(_transport_sqlstate(state) for state in _SQLSTATE.findall(text)):
            transport = True
        numbers.extend(int(match) for match in _RECORD_ERROR_NUMBER.findall(text))
        numbers.extend(number for pattern, number in _DEFINITE_CREATE_FAILURE_TEXTS if pattern.search(text))
    numbers = [number for number in dict.fromkeys(numbers) if number and number not in _COMPANION_ERROR_NUMBERS]
    definite = next((number for number in numbers if number in _DEFINITE_CREATE_FAILURES), None)
    if transport or definite is None or any(number not in _DEFINITE_CREATE_FAILURES for number in numbers):
        return {
            "outcome": "unknown",
            "error_number": numbers[0] if numbers else None,
            "reason": "connection_or_timeout" if transport else "error_not_proven_to_roll_back",
        }
    return {
        "outcome": "definite_rollback",
        "error_number": definite,
        "reason": _DEFINITE_CREATE_FAILURES[definite],
    }


# ---------------------------------------------------------------------------
# Replacement experiment: prove an extend, widen or consolidate recommendation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReplacementExperiment:
    """How to measure a new index definition against the index or indexes it replaces.

    Sandbox only. The workflow creates ``candidate`` as its test index. Then:
    - ``table_hint``: the candidate arm runs the query with ``candidate_hint`` on
      the table (``hint_candidate_index``). Nothing else changes. The result is
      hinted evidence: the index can serve the query; it does not show that the
      optimizer will choose it.
    - ``disable_in_sandbox``: run ``setup_sql`` before the candidate arm and
      ``teardown_sql`` after it. The old index is disabled, so the optimizer
      chooses freely among what would remain after the change.
    """

    candidate: IndexCandidate
    replaces: tuple[str, ...]
    hide_method: str
    setup_sql: tuple[str, ...]
    teardown_sql: tuple[str, ...]
    candidate_hint: str | None
    proof_scope: str
    notes: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "sandbox_only": True,
            "candidate": self.candidate.as_dict(),
            "replaces": list(self.replaces),
            "hide_method": self.hide_method,
            "setup_sql": list(self.setup_sql),
            "teardown_sql": list(self.teardown_sql),
            "candidate_hint": self.candidate_hint,
            "proof_scope": self.proof_scope,
            "notes": list(self.notes),
        }


def _disable_blocker(index: ExistingIndex) -> str | None:
    """Why an index must not be disabled, even on a sandbox copy."""

    evidence = index.protection_evidence or {}
    if index.index_type_code != 2 or str(index.index_type).strip().upper() != "NONCLUSTERED":
        return "it is a clustered, columnstore or special index, not a nonclustered rowstore index"
    if index.is_primary_key or index.is_unique_constraint or index.is_unique:
        return "it is unique: disabling it stops the constraint and disables the foreign keys that reference it"
    if evidence.get("coverage") != "complete":
        return "its protection metadata is incomplete"
    if evidence.get("child_foreign_key_support") or evidence.get("referenced_foreign_keys"):
        return "a foreign key depends on it"
    if evidence.get("indexed_view") or index.is_disabled:
        return "it is on an indexed view or is already disabled"
    return None


def plan_replacement_experiment(
    candidate: IndexCandidate,
    replaced: Sequence[ExistingIndex],
    *,
    hide_method: str = "table_hint",
) -> ReplacementExperiment:
    """Plan the candidate arm of an experiment in which ``candidate`` replaces ``replaced``.

    ``candidate`` is the new definition under its test-index name (extend and
    widen: the rebuilt index; consolidate: the surviving definition).
    ``replaced`` are the existing indexes the recommendation changes or drops.
    """

    if hide_method not in REPLACEMENT_HIDE_METHODS:
        raise ValueError(f"hide_method must be one of {', '.join(REPLACEMENT_HIDE_METHODS)}")
    if not candidate.index_name:
        raise ValueError("the candidate needs its test index_name")
    if not replaced:
        raise ValueError("a replacement experiment needs at least one replaced index")
    table = (
        _canonical_catalog_identifier(candidate.schema),
        _canonical_catalog_identifier(candidate.table),
    )
    for index in replaced:
        if (
            _canonical_catalog_identifier(index.schema),
            _canonical_catalog_identifier(index.table),
        ) != table:
            raise ValueError(
                f"{index.name} is not an index of {candidate.schema}.{candidate.table}"
            )
    names = tuple(index.name for index in replaced)
    if hide_method == "table_hint":
        return ReplacementExperiment(
            candidate=candidate,
            replaces=names,
            hide_method=hide_method,
            setup_sql=(),
            teardown_sql=(),
            candidate_hint=f"INDEX({_quote_catalog_identifier(candidate.index_name)})",
            proof_scope="hinted_evidence",
            notes=(
                "The candidate arm forces the new index with a table hint, so the old index "
                "cannot be chosen. This shows what the new index costs when it is used; it "
                "does not show that the optimizer will choose it, and the hinted statement "
                "is never a rewrite to deploy.",
            ),
        )
    target = f"{_quote_catalog_identifier(candidate.schema)}.{_quote_catalog_identifier(candidate.table)}"
    for index in replaced:
        blocker = _disable_blocker(index)
        if blocker is not None:
            raise ValueError(
                f"{index.name} cannot be disabled for the experiment: {blocker}. "
                "Use hide_method table_hint."
            )
    return ReplacementExperiment(
        candidate=candidate,
        replaces=names,
        hide_method=hide_method,
        setup_sql=tuple(
            f"ALTER INDEX {_quote_catalog_identifier(name)} ON {target} DISABLE;" for name in names
        ),
        teardown_sql=tuple(
            f"ALTER INDEX {_quote_catalog_identifier(name)} ON {target} REBUILD;" for name in names
        ),
        candidate_hint=None,
        proof_scope="sandbox_replacement",
        notes=(
            "DISABLE keeps the definition and drops the index data; statements that name the "
            "index in a hint fail while it is disabled. REBUILD builds it again and blocks "
            "writes to the table while it runs. Run both only on the sandbox copy, and run "
            "teardown_sql even when the measurement fails.",
        ),
    )


def hint_candidate_index(sql: str, candidate: IndexCandidate) -> str:
    """Add ``WITH (INDEX([candidate]))`` to every reference of the candidate's table.

    Only the hint text is inserted; the rest of the statement keeps its exact
    spelling. Raises ValueError when the table is not referenced, when a
    reference already has a table hint, or when the statement cannot be parsed.
    """

    if not candidate.index_name:
        raise ValueError("the candidate needs its test index_name")
    try:
        statements = parse(sql, read="tsql")
    except ParseError as exc:
        raise ValueError("the statement could not be parsed to place the index hint") from exc
    if len(statements) != 1 or statements[0] is None:
        raise ValueError("the index hint needs exactly one statement")
    tree = statements[0]
    schema = _canonical_catalog_identifier(candidate.schema)
    table = _canonical_catalog_identifier(candidate.table)
    cte_names = {_canonical_catalog_identifier(cte.alias) for cte in tree.find_all(exp.CTE) if cte.alias}
    positions: list[int] = []
    for reference in tree.find_all(exp.Table):
        if _canonical_catalog_identifier(reference.name) != table:
            continue
        if reference.db:
            if _canonical_catalog_identifier(reference.db) != schema:
                continue
        elif table in cte_names:
            continue
        if reference.args.get("hints"):
            raise ValueError(
                f"a reference to {candidate.schema}.{candidate.table} already has a table hint"
            )
        alias = reference.args.get("alias")
        anchor = alias.this if alias is not None and alias.this is not None else reference.this
        end = getattr(anchor, "meta", {}).get("end")
        if not isinstance(end, int):
            raise ValueError("the position of the table reference is not known")
        positions.append(end + 1)
    if not positions:
        raise ValueError(f"the statement does not reference {candidate.schema}.{candidate.table}")
    hint = f" WITH (INDEX({_quote_catalog_identifier(candidate.index_name)}))"
    for position in sorted(set(positions), reverse=True):
        sql = sql[:position] + hint + sql[position:]
    return sql


# ---------------------------------------------------------------------------
# Multi-query proof set
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProofQuery:
    """One Query Store query a recommendation must be measured on.

    role ``proof``: the recommendation claims to serve it, so it must win.
    role ``regression``: it reads an index the change touches, so it must not slow.
    """

    query_id: int
    plan_id: int | None
    role: str
    share_pct: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "plan_id": self.plan_id,
            "role": self.role,
            "share_pct": self.share_pct,
        }


@dataclass(frozen=True)
class ProofQueryResult:
    query: ProofQuery
    assessment: ExperimentAssessment


def proof_queries_from_recommendation(recommendation: Mapping[str, Any]) -> tuple[ProofQuery, ...]:
    """The proof and regression queries of one review_workload_indexes recommendation."""

    validation = recommendation.get("validation")
    validation = validation if isinstance(validation, Mapping) else {}
    queries: list[ProofQuery] = []
    seen: set[int] = set()
    for role, key, share_key in (
        ("proof", "proof_set", "share_of_recommendation_pct"),
        ("regression", "regression_set", "workload_share_pct"),
    ):
        for item in validation.get(key) or []:
            if not isinstance(item, Mapping):
                continue
            try:
                query_id = int(item["query_id"])
            except (KeyError, TypeError, ValueError):
                continue
            if query_id in seen:
                continue
            seen.add(query_id)
            plan_id = item.get("plan_id")
            share = item.get(share_key)
            queries.append(
                ProofQuery(
                    query_id=query_id,
                    plan_id=int(plan_id) if isinstance(plan_id, (int, float)) else None,
                    role=role,
                    share_pct=float(share) if isinstance(share, (int, float)) else None,
                )
            )
    return tuple(queries)


def plan_proof_set(
    queries: Sequence[ProofQuery], *, runs_per_phase: int, max_executions: int
) -> dict[str, Any]:
    """Fit a proof set into the session's execution budget.

    Each query costs three phases of ``runs_per_phase`` executions. Regression
    queries are left out from the end of the list when the budget is short and
    are reported in ``not_measured``. Proof queries are never left out.
    """

    if runs_per_phase < 1:
        raise ValueError("runs_per_phase must be at least 1")
    per_query = PROOF_SET_PHASES * runs_per_phase
    proof = [query for query in queries if query.role == "proof"]
    regression = [query for query in queries if query.role != "proof"]
    if len(proof) * per_query > max_executions:
        raise ValueError(
            f"{len(proof)} proof queries need {len(proof) * per_query} executions and the budget is "
            f"{max_executions}; lower runs_per_phase or raise the policy budget"
        )
    room = (max_executions - len(proof) * per_query) // per_query
    kept = proof + regression[:room]
    return {
        "queries": [query.as_dict() for query in kept],
        "runs_per_phase": runs_per_phase,
        "executions_required": len(kept) * per_query,
        "max_executions": max_executions,
        "not_measured": [
            {"query_id": query.query_id, "role": query.role, "reason": "execution_budget"}
            for query in regression[room:]
        ],
    }


def assess_proof_set(
    results: Sequence[ProofQueryResult],
    *,
    expected: Sequence[ProofQuery] = (),
    max_regression_pct: float = PROOF_SET_MAX_REGRESSION_PCT,
) -> dict[str, Any]:
    """One verdict for a recommendation from the experiments of its queries.

    ``proven``: every proof query is a winner and no regression query slows
    more than ``max_regression_pct``. ``rejected``: a measured query fails that
    rule. ``inconclusive``: a query in ``expected`` has no result, or a result
    is incomplete or inconclusive.
    """

    measured = {result.query.query_id for result in results}
    unmeasured = [query.query_id for query in expected if query.query_id not in measured]
    failed: list[int] = []
    unsettled: list[int] = []
    for result in results:
        assessment = result.assessment
        if assessment.status not in {"winner", "loser"} or assessment.improvement_pct is None:
            unsettled.append(result.query.query_id)
        elif result.query.role == "proof":
            if assessment.status != "winner":
                failed.append(result.query.query_id)
        elif assessment.improvement_pct < -max_regression_pct:
            failed.append(result.query.query_id)
    if failed:
        status = "rejected"
        reason = (
            "A proof query did not beat its baseline, or a regression query slowed more than "
            f"{max_regression_pct:g}%."
        )
    elif unsettled or unmeasured or not results:
        status = "inconclusive"
        reason = "Not every query of the proof set has a settled measurement."
    else:
        status = "proven"
        reason = (
            "Every proof query beat its baseline and no regression query slowed more than "
            f"{max_regression_pct:g}%."
        )
    return {
        "status": status,
        "reason": reason,
        "failed_query_ids": failed,
        "unsettled_query_ids": unsettled,
        "unmeasured_query_ids": unmeasured,
        "queries": [
            {**result.query.as_dict(), **result.assessment.as_dict()} for result in results
        ],
    }


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
    columnstore = expected.index_kind == INDEX_KIND_NONCLUSTERED_COLUMNSTORE
    catalog_type = _CATALOG_INDEX_TYPE.get(expected.index_kind)
    if catalog_type is None or str(actual.index_type).strip().upper() != catalog_type:
        return False
    expected_keys = tuple(parse_candidate_key(column) for column in expected.key_columns)
    if columnstore:
        if not _columnstore_columns_match(actual, expected_keys):
            return False
        # The built order must be the requested one, column for column.
        if tuple(
            _canonical_catalog_identifier(column) for column in actual.columnstore_order_columns
        ) != tuple(
            _canonical_catalog_identifier(parse_candidate_key(column).name)
            for column in expected.order_columns
        ):
            return False
    elif len(actual.key_columns) != len(expected_keys) or any(
        _canonical_catalog_identifier(actual_key.name)
        != _canonical_catalog_identifier(expected_key.name)
        or actual_key.direction != expected_key.direction
        for actual_key, expected_key in zip(actual.key_columns, expected_keys)
    ):
        return False
    elif tuple(
        _canonical_catalog_identifier(column) for column in actual.include_columns
    ) != tuple(_canonical_catalog_identifier(column) for column in expected.include_columns):
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
    default_compression = _COLUMNSTORE_DEFAULT_COMPRESSION if columnstore else "NONE"
    expected_compression = str(expected.compression or default_compression).strip().upper()
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


def _columnstore_columns_match(
    actual: ExistingIndex, expected_columns: Sequence[IndexKeyColumn]
) -> bool:
    """Whether a catalog columnstore index holds the requested columns and no other.

    The catalog lists the columns of a columnstore index as nonkey columns and
    their order has no meaning. The partitioning column of an aligned index
    may be present without being requested.
    """

    expected = {_canonical_catalog_identifier(column.name) for column in expected_columns}
    held = {
        _canonical_catalog_identifier(name)
        for name in (
            *(column.name for column in actual.key_columns),
            *actual.include_columns,
            *actual.nonkey_columns,
        )
    }
    partitioning = {
        _canonical_catalog_identifier(column) for column in actual.partition_columns
    }
    return expected <= held and held - expected <= partitioning


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


def _is_constant_in_list(node: Any) -> bool:
    return (
        isinstance(node, exp.In)
        and node.args.get("query") is None
        and bool(node.expressions)
        and all(_is_filter_constant(value) for value in node.expressions)
    )


def _filter_disjuncts(node: Any) -> list[Any]:
    if isinstance(node, exp.Or):
        return [*_filter_disjuncts(node.this), *_filter_disjuncts(node.expression)]
    if _is_constant_in_list(node):
        return [
            exp.EQ(this=node.this.copy(), expression=value.copy())
            for value in node.expressions
        ]
    return [_canonical_filter_boolean(node)]


def _canonical_filter_boolean(node: Any) -> Any:
    """Give IN lists and OR chains one order-independent spelling.

    SQL Server stores ``x IN (1, 2)`` as ``[x]=(1) OR [x]=(2)``. Each rebuilt
    OR group is parenthesized so that removed parentheses cannot make two
    different AND/OR groupings render the same text.
    """

    if isinstance(node, exp.And):
        return exp.And(
            this=_canonical_filter_boolean(node.this),
            expression=_canonical_filter_boolean(node.expression),
        )
    if isinstance(node, exp.Not):
        return exp.Not(this=_canonical_filter_boolean(node.this))
    if isinstance(node, exp.Or) or _is_constant_in_list(node):
        disjuncts = sorted(
            _filter_disjuncts(node),
            key=lambda disjunct: disjunct.sql(dialect="tsql"),
        )
        if len(disjuncts) == 1:
            return disjuncts[0]
        combined = disjuncts[0]
        for disjunct in disjuncts[1:]:
            combined = exp.Or(this=combined, expression=disjunct)
        return exp.Paren(this=combined)
    return node


def _canonical_filter_definition(value: str | None) -> str | None:
    """Normalize catalog filter text without changing literal values.

    SQL Server stores sys.indexes.filter_definition as regenerated text, for
    example ``([status]=(1))`` for ``status = 1``. Parse the restricted
    predicate grammar already admitted by this module, remove ``Paren``
    nodes, quote and casefold every identifier, and give IN lists
    and OR chains one spelling, while preserving string and numeric literal
    values.
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
            # Quote every identifier: SQL Server brackets all columns, and
            # uniform delimiters keep names with spaces unambiguous.
            identifier.set("quoted", True)
        expression = _canonical_filter_boolean(expression)
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


def _validate_columnstore_compression(value: str | None) -> str | None:
    """Validate columnstore DATA_COMPRESSION before it reaches generated DDL."""

    if value is None or not str(value).strip():
        return None
    normalized = str(value).strip().upper()
    if normalized not in _SUPPORTED_COLUMNSTORE_COMPRESSIONS:
        supported = ", ".join(sorted(_SUPPORTED_COLUMNSTORE_COMPRESSIONS))
        raise ValueError(
            f"unsupported columnstore index compression {value!r}; use one of {supported}"
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
