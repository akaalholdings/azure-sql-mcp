from __future__ import annotations

import logging
import math
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from .connection import AzureSqlExecutor
from .index_recommendations import build_create_index_statement
from .index_recommendations import split_index_columns
from .observability import sanitize_error_message
from .query_text import strip_query_store_parameter_declarations
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


@dataclass(frozen=True)
class IndexCandidate:
    schema: str
    table: str
    key_columns: tuple[str, ...]
    include_columns: tuple[str, ...]


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
                normalized_sql = strip_query_store_parameter_declarations(sql_text)
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
        seen: dict[tuple[str, str, tuple[str, ...]], _RawCandidate] = {}

        # From plan XML missing indexes
        for query_row in workload:
            for mi in query_row.get("missing_indexes", []):
                key_cols = mi.get("equality_columns", []) + mi.get("inequality_columns", [])
                if not key_cols:
                    continue
                sig = (mi.get("schema", "dbo"), mi.get("table", ""), tuple(key_cols))
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
            sig = (dmv.get("schema", "dbo"), dmv.get("table", ""), tuple(key_cols))
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
        SELECT SUM(p.rows) AS row_count
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
        existing: set[tuple[str, str, tuple[str, ...]]],
    ) -> list[_RawCandidate]:
        """Remove candidates whose key columns match or are a prefix of an existing index."""
        result: list[_RawCandidate] = []
        for rc in candidates:
            candidate_keys = tuple(rc.key_columns)
            covered = False
            for ex_schema, ex_table, ex_cols in existing:
                if rc.schema != ex_schema or rc.table != ex_table:
                    continue
                # Candidate is covered if its key columns are a prefix of existing
                if _is_prefix(candidate_keys, ex_cols):
                    covered = True
                    break
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
                    if _is_prefix(tuple(rc.key_columns), tuple(wider.key_columns)):
                        # rc is subsumed by wider — merge metrics into wider
                        wider.impact_pct = max(wider.impact_pct, rc.impact_pct)
                        wider.statement_subtree_cost += rc.statement_subtree_cost
                        wider.total_cpu_us += rc.total_cpu_us
                        wider.total_executions += rc.total_executions
                        wider.query_ids |= rc.query_ids
                        wider.include_columns = _merge_unique(
                            wider.include_columns, rc.include_columns,
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
                create_index_sql=build_create_index_statement(
                    schema_name=rc.schema,
                    table_name=rc.table,
                    key_columns=rc.key_columns,
                    include_columns=clean_include,
                ),
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
    ) -> set[tuple[str, str, tuple[str, ...]]]:
        query = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            i.index_id,
            ic.is_included_column,
            ic.key_ordinal,
            c.name AS column_name
        FROM sys.indexes AS i
        INNER JOIN sys.tables AS t ON i.object_id = t.object_id
        INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
        INNER JOIN sys.index_columns AS ic
            ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns AS c
            ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.is_hypothetical = 0 AND i.index_id > 0
        ORDER BY s.name, t.name, i.index_id, ic.key_ordinal
        """
        rows = await self.executor.fetch_all(database_name, query)
        by_index: dict[tuple[str, str, Any], list[tuple[int, str]]] = {}
        for row in rows:
            if row.get("is_included_column"):
                continue
            key = (
                row.get("schema_name") or "dbo",
                row.get("table_name") or "",
                row.get("index_id"),
            )
            by_index.setdefault(key, []).append(
                (int(row.get("key_ordinal") or 0), row.get("column_name") or ""),
            )

        signatures: set[tuple[str, str, tuple[str, ...]]] = set()
        for (schema_name, table_name, _), columns in by_index.items():
            ordered = tuple(
                col for _, col in sorted(columns, key=lambda x: x[0]) if col
            )
            if ordered:
                signatures.add((schema_name, table_name, ordered))
        return signatures

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
