"""Rule-based anti-pattern detection over showplan XML (estimated or actual).

Each finding names a stable rule code, a severity, the plan node, the numbers
from the plan that triggered it, the optimizer pattern family it belongs to,
and a direction for the fix. On an actual plan with operator times a finding
also carries its operator's self elapsed time, that time's share of the
statement's elapsed time, and a benefit: the share of elapsed time the fix can
address, with the basis stated. Otherwise findings rank by the operator's share
of estimated cost, which is an estimate in every plan. Shares are weighted by
the statement's part of the whole plan, so a trivial statement in a batch does
not outrank the expensive one. Row estimates are compared per execution.
Runtime rules (spills, estimate gaps, thread skew, grant waste) apply only to
actual plans. Findings are leads to measure, not measured gains.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from collections.abc import Collection
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from .plan_digest import TOP_OPERATORS
from .plan_digest import TREE_BUDGET
from .plan_digest import build_plan_digest
from .plan_tree import ParsedPlan
from .plan_tree import PlanNode
from .plan_tree import PlanParseError
from .plan_tree import ce_guess_fraction
from .plan_tree import clean_text
from .plan_tree import cross_join_check
from .plan_tree import cursor_operation
from .plan_tree import estimate_check
from .plan_tree import estimate_origins
from .plan_tree import is_true
from .plan_tree import parse_plan
from .plan_tree import partition_access
from .plan_tree import predicate_variables
from .plan_tree import query_time_stats
from .plan_tree import residual_predicate
from .plan_tree import statement_elapsed_ms
from .plan_tree import statement_kind
from .plan_tree import statistics_used
from .plan_tree import subtree
from .plan_tree import unquote
from .showplan_access import TableAccess
from .showplan_access import classify_predicate
from .showplan_access import eager_spool_index
from .showplan_access import parse_plan_access

NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"
_Q = f"{{{NS}}}"

SEVERITY_ORDER = {"high": 3, "medium": 2, "low": 1, "info": 0}
LOOKUP_EXECUTION_FLOOR = 100
NESTED_LOOP_EXECUTION_FLOOR = 10_000
ESTIMATE_GAP_FACTOR = 10.0
ESTIMATE_GAP_ROW_FLOOR = 1_000
SCAN_TABLE_ROW_FLOOR = 10_000
# Grant sizes follow the PerformanceMonitor calibration: a finding from 1 GB, critical from 4 GB.
# Azure SQL Database tiers differ in size, so a share of MaxQueryMemory also counts.
LARGE_GRANT_KB = 1024 * 1024
CRITICAL_GRANT_KB = 4 * 1024 * 1024
WASTED_GRANT_FLOOR_KB = 65_536
UDF_TIME_SHARE_FLOOR = 0.1
# The default cost threshold for parallelism: a cheaper plan is never made parallel,
# so its NonParallelPlanReason changes nothing. The database setting is not in the plan.
PARALLELISM_COST_FLOOR = 5.0
# A seek that reads at least this fraction of the table tests a residual LIKE on most rows, as a scan does.
WIDE_SEEK_READ_FRACTION = 0.5
# An operator below this share of the statement is not worth a shape finding.
HARM_SHARE_FLOOR = 0.05
# On a timed plan, medium and high findings that can address at least this share lead the list.
HOTSPOT_SHARE_FLOOR = 0.05
SEGMENT_READ_FLOOR = 10
ROWS_READ_RATIO = 100.0
ROWGROUP_ROWS = 1_048_576
# Row mode costs little on a few thousand rows; batch mode pays off from about here.
BATCH_MODE_ROW_FLOOR = 100_000
LATE_FILTER_ROW_FLOOR = 10_000
LATE_FILTER_KEEP_FRACTION = 0.1
# PerformanceMonitor calibration for an ineffective lazy spool.
SPOOL_REBIND_FLOOR = 100
SPOOL_REWIND_RATIO = 5.0
REPEATED_EXECUTION_FLOOR = 100
DEEP_OFFSET_ROWS = 10_000
FAN_OUT_ROW_FLOOR = 10_000
BARE_SCAN_ROW_FLOOR = 100_000
BARE_SCAN_SHARE_FLOOR = 0.25
SCAN_MISESTIMATE_SHARE = 0.5
SCAN_MISESTIMATE_SELECTIVITY = 0.1
# Showplan cuts StatementText near 4,000 characters.
TRUNCATED_TEXT_CHARS = 3_990
MAX_FINDINGS = 50
_UDF = f"{_Q}UserDefinedFunction"
_COLUMN_REFERENCE = f"{_Q}ColumnReference"
_SCALAR = f"{_Q}ScalarOperator"
_PREDICATE_TAGS = {f"{_Q}{name}" for name in ("Predicate", "SeekPredicates", "ProbeResidual", "BuildResidual", "Residual")}
# Where a conversion changes access, join, grouping or order, and so the plan.
_PLAN_SHAPING_TAGS = _PREDICATE_TAGS | {
    f"{_Q}{name}"
    for name in ("HashKeysBuild", "HashKeysProbe", "InnerSideJoinColumns", "OuterSideJoinColumns", "GroupBy", "OrderBy")
}
_SCAN_OPERATORS = frozenset({"Clustered Index Scan", "Index Scan", "Table Scan"})
_PASS_THROUGH_OPERATORS = frozenset({"Compute Scalar", "Parallelism", "Segment", "Sequence Project"})
_MEMORY_CONSUMERS = frozenset({"Sort", "Hash Match", "Window Aggregate"})
_NAME_PART = re.compile(r"\[((?:[^\]]|\]\])*)\]")
_OPTIMIZE_FOR_UNKNOWN = re.compile(r"OPTIMIZE\s+FOR\s+(?:UNKNOWN|\([^)]*\bUNKNOWN\b)", re.IGNORECASE)
_DECLARE_CURSOR = re.compile(r"\bDECLARE\s+\S+\s+(?:(?:INSENSITIVE|SCROLL)\s+)*CURSOR\b(?P<options>.*?)\bFOR\b", re.IGNORECASE | re.DOTALL)
_WILDCARD_FIX = (
    "Search by prefix (LIKE 'abc%') if the requirement allows. For an ends-with search, index a "
    "computed REVERSE() column and search it by prefix; for a contains search, use full-text search."
)
_COMPUTED_COLUMN_INDEX = (
    "index a deterministic computed column on the expression (every writer then needs the required "
    "SET options, for example QUOTED_IDENTIFIER and ANSI_NULLS ON when a module was created, or its "
    "INSERT, UPDATE or DELETE fails with Msg 1934; adds write cost)"
)


@dataclass
class PlanFinding:
    rule: str
    severity: str
    family: str
    message: str
    fix: str
    node_id: int | None = None
    statement_index: int = 0
    evidence: dict[str, Any] = field(default_factory=dict)
    estimated_cost_share: float | None = None
    self_elapsed_ms: float | None = None
    elapsed_share: float | None = None
    # Elapsed time the fix can address, on a plan that timed its operators.
    benefit_ms: float | None = None
    benefit_share: float | None = None
    benefit_basis: str | None = None
    statement_weight: float = 1.0

    @property
    def impact_share(self) -> float:
        """Measured share when the plan timed its operators, else the estimated share."""

        if self.elapsed_share is not None:
            return self.elapsed_share
        return self.estimated_cost_share or 0.0

    @property
    def plan_share(self) -> float | None:
        """The finding's share of the whole plan: its statement's weight times its share there."""

        share = self.benefit_share if self.benefit_share is not None else self.estimated_cost_share
        return None if share is None else self.statement_weight * share

    def as_dict(self) -> dict[str, Any]:
        plan_share = self.plan_share
        return {
            "rule": self.rule,
            "severity": self.severity,
            "family": self.family,
            "node_id": self.node_id,
            "statement_index": self.statement_index,
            "message": self.message,
            "fix": self.fix,
            "evidence": self.evidence,
            "estimated_cost_share": (
                round(self.estimated_cost_share, 4) if self.estimated_cost_share is not None else None
            ),
            "self_elapsed_ms": self.self_elapsed_ms,
            "elapsed_share": round(self.elapsed_share, 4) if self.elapsed_share is not None else None,
            "benefit_ms": round(self.benefit_ms, 1) if self.benefit_ms is not None else None,
            "benefit_share": round(self.benefit_share, 4) if self.benefit_share is not None else None,
            "benefit_basis": self.benefit_basis,
            "statement_weight": round(self.statement_weight, 4),
            "plan_share": round(plan_share, 4) if plan_share is not None else None,
        }


def analyze_plan(
    plan: str | ParsedPlan,
    *,
    in_flight: bool = False,
    partition_columns: Mapping[str, str] | None = None,
    statement_indexes: Collection[int] | None = None,
    unique_keys: Mapping[str, Collection[Collection[str]]] | None = None,
) -> dict[str, Any]:
    """Run every rule over a showplan and return sorted findings.

    ``plan`` is showplan XML, or a ParsedPlan so one tool call parses once.
    ``in_flight`` marks a running request's plan: its counts are partial, so
    overestimates and grant waste on unfinished operators are not reported.
    ``partition_columns`` maps ``schema.table`` to its partitioning column (from
    the catalog); the plan does not name it. ``statement_indexes`` limits the
    analysis to those statements of a batch plan; their indexes are kept.
    ``unique_keys`` maps ``schema.table`` to the column lists of its unique keys
    (from the catalog; the plan does not say an index is unique). With it, a
    sniffed parameter used only for an equality seek of a whole unique key is
    not reported: that seek returns one row whatever the value.
    """

    try:
        parsed = parse_plan(plan)
    except PlanParseError as exc:
        return _empty(str(exc))
    actual = parsed.root.find(f".//{_Q}RunTimeInformation") is not None
    access_summary = parse_plan_access(parsed)
    wanted = [
        (index, item)
        for index, item in enumerate(parsed.statements)
        if statement_indexes is None or index in statement_indexes
    ]
    timed = any(node.has_timing for _, item in wanted for node in item.nodes)
    # A statement's weight is its part of the plan: of elapsed time when the plan
    # timed its operators, else of estimated cost.
    sizes = [
        (statement_elapsed_ms(item.query_plan, item.nodes) or 0.0)
        if timed
        else _plan_cost(item.statement, item.query_plan)
        for _, item in wanted
    ]
    total = sum(sizes)
    findings: list[PlanFinding] = []
    statements_out: list[dict[str, Any]] = []
    seen_statements: set[int] = set()
    partitioning = {key.strip().lower(): value for key, value in (partition_columns or {}).items()}
    unique = {
        key.strip().lower(): [frozenset(str(column).lower() for column in columns) for columns in keys]
        for key, keys in (unique_keys or {}).items()
    }
    for (index, item), size in zip(wanted, sizes):
        weight = size / total if total > 0 else 1.0
        accesses = access_summary.statements[index].accesses if index < len(access_summary.statements) else []
        statement_findings: list[PlanFinding] = []
        if item.query_plan is not None:
            rules = _StatementRules(
                index,
                item.statement,
                item.query_plan,
                item.nodes,
                accesses,
                actual=actual,
                in_flight=in_flight,
                weight=weight,
                partition_columns=partitioning,
                first_plan=id(item.statement) not in seen_statements,
                unique_keys=unique,
            )
            statement_findings = rules.run()
        seen_statements.add(id(item.statement))
        findings.extend(statement_findings)
        operation_type, cursor_type = cursor_operation(item.statement, item.query_plan)
        statements_out.append(
            {
                "statement_index": index,
                "statement_type": (item.statement.get("StatementType") or "").upper(),
                "statement_kind": statement_kind(item.statement),
                "cursor_operation": operation_type,
                "cursor_actual_type": cursor_type,
                "statement_cost": _float(item.statement.get("StatementSubTreeCost")),
                "elapsed_ms": statement_elapsed_ms(item.query_plan, item.nodes) if actual else None,
                "weight": round(weight, 4),
                "query_hash": item.statement.get("QueryHash"),
                "finding_count": len(statement_findings),
            }
        )
    findings.sort(key=lambda finding: _rank(finding, timed))
    counts: dict[str, int] = {}
    families: dict[str, int] = {}
    for finding in findings:
        counts[finding.severity] = counts.get(finding.severity, 0) + 1
        families[finding.family] = families.get(finding.family, 0) + 1
    result: dict[str, Any] = {
        "plan_kind": "actual" if actual else "estimated",
        "parse_error": None,
        "ranking_basis": "self_elapsed_share" if timed else "estimated_cost_share",
        "statements": statements_out,
        "finding_counts": counts,
        "families": families,
        "findings": [finding.as_dict() for finding in findings[:MAX_FINDINGS]],
        "truncated": len(findings) > MAX_FINDINGS,
        "note": (
            "Findings are leads. On a plan that timed its operators, medium and high findings "
            f"that can address at least {HOTSPOT_SHARE_FLOOR:.0%} of the plan's elapsed time come first, "
            "largest benefit first (benefit_share, with its basis); the rest follow by severity. "
            "Otherwise findings rank by severity, then by share of estimated cost. Cost is an "
            "estimate in every plan. Measure any change with a benchmark before claiming a gain."
        ),
    }
    if in_flight:
        result["in_flight"] = True
        result["in_flight_note"] = (
            "Counts and times are partial and still growing: overestimates and unused memory on "
            "operators that have not finished are not reported."
        )
    return result


def read_plan(
    plan_xml: str,
    *,
    include_digest: bool = True,
    in_flight: bool = False,
    partition_columns: Mapping[str, str] | None = None,
    statement_indexes: Collection[int] | None = None,
    pin_plan_hash: str | None = None,
    top_n: int = TOP_OPERATORS,
    tree_budget: int = TREE_BUDGET,
    unique_keys: Mapping[str, Collection[Collection[str]]] | None = None,
) -> dict[str, Any]:
    """Parse a showplan once, then run the rules and build the digest from that one parse.

    Pure and synchronous, with no shared state: call it through ``asyncio.to_thread``
    for a large plan. Returns ``analysis`` (as analyze_plan), ``digest`` (as
    build_plan_digest, or None) and ``parsed`` (the ParsedPlan for a later
    describe_plan_node or parse_plan_access; None when the XML did not parse).
    """

    try:
        parsed: ParsedPlan | None = parse_plan(plan_xml)
    except PlanParseError:
        parsed = None
    source: str | ParsedPlan = parsed if parsed is not None else plan_xml
    return {
        "parsed": parsed,
        "analysis": analyze_plan(
            source,
            in_flight=in_flight,
            partition_columns=partition_columns,
            statement_indexes=statement_indexes,
            unique_keys=unique_keys,
        ),
        "digest": (
            build_plan_digest(
                source, top_n=top_n, tree_budget=tree_budget, pin_plan_hash=pin_plan_hash, in_flight=in_flight
            )
            if include_digest
            else None
        ),
    }


def _empty(reason: str) -> dict[str, Any]:
    return {
        "plan_kind": None,
        "parse_error": reason,
        "statements": [],
        "finding_counts": {},
        "families": {},
        "findings": [],
        "truncated": False,
    }


def _plan_cost(statement: ET.Element, query_plan: ET.Element | None) -> float:
    """Estimated cost of one plan. A cursor's plans share one statement, so each uses its root's cost."""

    root = query_plan.find(f"{_Q}RelOp") if query_plan is not None else None
    root_cost = _float(root.get("EstimatedTotalSubtreeCost")) if root is not None else 0.0
    if statement.tag == f"{_Q}StmtCursor":
        return root_cost
    return _float(statement.get("StatementSubTreeCost")) or root_cost


def _rank(finding: PlanFinding, timed: bool) -> tuple[Any, ...]:
    severity = SEVERITY_ORDER.get(finding.severity, 0)
    share = finding.plan_share or 0.0
    if timed and finding.benefit_share is not None and severity >= SEVERITY_ORDER["medium"] and share >= HOTSPOT_SHARE_FLOOR:
        return (0, -share, -severity, finding.rule)
    return (1, -severity, -share, finding.rule)


class _StatementRules:
    """One statement's plan and the shares every rule grades by."""

    def __init__(
        self,
        index: int,
        statement: ET.Element,
        query_plan: ET.Element,
        nodes: list[PlanNode],
        accesses: list[TableAccess],
        *,
        actual: bool,
        in_flight: bool,
        weight: float,
        partition_columns: Mapping[str, str],
        first_plan: bool,
        unique_keys: Mapping[str, list[frozenset[str]]] | None = None,
    ) -> None:
        self.index = index
        self.statement = statement
        self.query_plan = query_plan
        self.nodes = nodes
        self.accesses = accesses
        self.actual = actual
        self.in_flight = in_flight
        self.weight = weight
        self.partition_columns = partition_columns
        self.unique_keys = unique_keys or {}
        # A cursor statement has one plan per operation: text rules run once.
        self.first_plan = first_plan
        self.findings: list[PlanFinding] = []
        self.by_id = {node.node_id: node for node in nodes}
        self.access_by_node = {access.node_id: access for access in accesses}
        self.statement_cost = _float(statement.get("StatementSubTreeCost"))
        self.cost_shares = (
            {node.node_id: node.own_cost / self.statement_cost for node in nodes} if self.statement_cost > 0 else {}
        )
        self.timed = any(node.has_timing for node in nodes)
        self.elapsed = statement_elapsed_ms(query_plan, nodes) if self.timed else None
        self.time_shares = (
            {node.node_id: min(1.0, (node.self_elapsed_ms or 0.0) / self.elapsed) for node in nodes}
            if self.elapsed
            else {}
        )

    def run(self) -> list[PlanFinding]:
        _access_rules(self)
        _statement_warning_rules(self)
        for node in self.nodes:
            _operator_rules(node, self)
        _columnstore_rules(self)
        _partition_rules(self)
        _shape_rules(self)
        _memory_rules(self)
        if self.first_plan:
            _text_rules(self)
        if self.actual:
            _estimate_rules(self)
            for node in self.nodes:
                _runtime_rules(node, self)
            _spill_occurred(self)
        return self.findings

    def impact(self, node_id: int | None, cost_share: float) -> float:
        """An operator's share of the whole plan: measured self time when timed, else estimated cost."""

        if node_id is not None and node_id in self.time_shares:
            return self.time_shares[node_id] * self.weight
        return cost_share * self.weight

    def share_of(self, members: Sequence[PlanNode]) -> float:
        """Several operators' share of the whole plan, on the same basis as ``impact``."""

        if self.elapsed:
            return min(1.0, sum(node.self_elapsed_ms or 0.0 for node in members) / self.elapsed) * self.weight
        if self.statement_cost > 0:
            return min(1.0, sum(node.own_cost for node in members) / self.statement_cost) * self.weight
        return 0.0

    def settled(self, node: PlanNode) -> bool:
        """The operator's counts are final: always, except on an unfinished operator of an in-flight plan."""

        return not self.in_flight or node.finished

    def add(
        self,
        rule: str,
        severity: str,
        family: str,
        message: str,
        fix: str,
        *,
        node_id: int | None = None,
        evidence: dict[str, Any] | None = None,
        cost_share: float | None = None,
        benefit_nodes: Sequence[PlanNode] | None = None,
        benefit_ms: float | None = None,
        benefit_basis: str | None = None,
    ) -> None:
        """``benefit_nodes`` are the operators whose self time the fix can address (default: the node itself)."""

        if cost_share is None and node_id is not None:
            cost_share = self.cost_shares.get(node_id)
        node = self.by_id.get(node_id) if node_id is not None else None
        benefit_share: float | None = None
        if self.elapsed:
            if benefit_ms is None:
                members = benefit_nodes if benefit_nodes is not None else ([node] if node is not None else [])
                if members:
                    benefit_ms = sum(member.self_elapsed_ms or 0.0 for member in members)
                    benefit_basis = benefit_basis or (
                        f"self time of node {members[0].node_id}"
                        if len(members) == 1
                        else "self time of nodes " + ", ".join(str(member.node_id) for member in members[:8])
                    )
            if benefit_ms is not None:
                benefit_ms = min(benefit_ms, self.elapsed)
                benefit_share = benefit_ms / self.elapsed
        else:
            benefit_ms = benefit_basis = None
        if self.in_flight:
            evidence = {**(evidence or {}), "in_flight": True}
        self.findings.append(
            PlanFinding(
                rule=rule,
                severity=severity,
                family=family,
                message=message,
                fix=fix,
                node_id=node_id,
                statement_index=self.index,
                evidence=evidence or {},
                estimated_cost_share=cost_share,
                self_elapsed_ms=(
                    round(node.self_elapsed_ms, 1)
                    if self.elapsed and node is not None and node.self_elapsed_ms is not None
                    else None
                ),
                elapsed_share=self.time_shares.get(node_id) if node_id is not None else None,
                benefit_ms=benefit_ms,
                benefit_share=benefit_share,
                benefit_basis=benefit_basis,
                statement_weight=self.weight,
            )
        )


def _label(node: PlanNode) -> str:
    return f"{node.label} (node {node.node_id})"


def _access_rules(rules: _StatementRules) -> None:
    add, impact, by_id = rules.add, rules.impact, rules.by_id
    for access in rules.accesses:
        table = clean_text(f"{access.schema}.{access.table}", 200)
        share = impact(access.node_id, access.cost_share)
        columnstore = _is_columnstore(access)
        seek_columns = set(access.seek_eq_columns) | set(access.seek_range_columns)
        for column, kind in access.nonsargable.items():
            if kind == "udf":
                continue  # scalar_udf reports the function and the column it wraps
            column_name = clean_text(column, 128)
            implicit = kind == "convert_implicit"
            wildcard = kind == "leading_wildcard"
            identity = kind == "arithmetic_identity"
            wrapper = (
                "an implicit conversion"
                if implicit
                else "a leading-wildcard LIKE"
                if wildcard
                else "an identity expression (+ 0, - 0 or * 1)"
                if identity
                else clean_text(kind.replace("function:", "").upper(), 60)
            )
            # ``column + 0`` beside a seek on another column is written on purpose:
            # it keeps a sniffed plan from seeking an index on this column.
            steering = identity and column not in seek_columns and bool(seek_columns)
            # A dynamic seek (CAST to date via GetRangeThroughConvert, an implicit
            # conversion via GetRangeWithMismatchedTypes) still seeks a range on
            # the column and re-checks the wrapped predicate as a residual.
            dynamic = column in access.dynamic_seek_columns
            seeked = dynamic or column in seek_columns
            if steering:
                seek_on = ", ".join(clean_text(item, 128) or "" for item in sorted(seek_columns))
                message = (
                    f"{table}.{column_name} is wrapped in {wrapper}, so no index can seek it; this access "
                    f"seeks on {seek_on} instead. That is the usual purpose of such an expression: it steers "
                    f"the plan away from an index on {column_name}."
                )
                fix = (
                    "Keep it only if it is there on purpose. It disables every index seek and the histogram "
                    f"estimate on {column_name} for this predicate, for every parameter value. To test "
                    "whether it is still needed, benchmark the statement without it across the parameter cases."
                )
            elif dynamic:
                message = (
                    f"{table}.{column_name} is wrapped in {wrapper}, but the index still seeks a range on it "
                    "(dynamic seek) and re-checks the wrapped predicate."
                )
                fix = (
                    "Bind the parameter or variable as the column's exact type: the seek becomes exact "
                    "and the estimate improves."
                    if implicit
                    else "A bare-column half-open range, or a parameter typed as the column, gives an exact "
                    "seek and a better estimate."
                )
            elif seeked:
                message = (
                    f"{table}.{column_name} is wrapped in {wrapper} in a residual predicate; the index still "
                    "seeks on the bare column, and the wrapped predicate filters the rows that seek returns."
                )
                fix = (
                    "Bind the parameter or variable as the column's exact type."
                    if implicit
                    else "Usually acceptable. If the residual discards most of the rows the seek reads, rewrite "
                    f"it with the column bare, or {_COMPUTED_COLUMN_INDEX}."
                )
            elif wildcard:
                message = (
                    f"{table}.{column_name} is searched with LIKE and a leading wildcard: there is no prefix to "
                    "seek, so every row is read and tested."
                )
                fix = _WILDCARD_FIX + (
                    " The column is also under an implicit conversion: type the pattern as the column "
                    "(for a varchar column, '...' rather than N'...')."
                    if column in access.implicit_conversion_columns
                    else ""
                )
            elif columnstore:
                message = (
                    f"{table}.{column_name} is wrapped in {wrapper} inside the columnstore scan's predicate, so "
                    "the scan cannot skip rowgroups on it and tests the expression on every row."
                )
                fix = (
                    "Bind the parameter or variable as the column's exact type."
                    if implicit
                    else "Compare the bare column (move the work to the constant or parameter side), or store "
                    "the computed value in a real column at load time: a columnstore index cannot hold a "
                    "persisted computed column."
                )
            else:
                message = (
                    f"{table}.{column_name} is wrapped in {wrapper} inside the predicate, so an index on "
                    "the column cannot seek it as written."
                )
                fix = (
                    "Bind the parameter or variable as the column's exact type."
                    if implicit
                    else "Rewrite the predicate so the column is bare (move the work to the parameter side, "
                    f"or use a half-open range for date buckets), or {_COMPUTED_COLUMN_INDEX}."
                )
            add(
                "implicit_conversion_on_column" if implicit else "non_sargable_predicate",
                "low" if seeked or steering else _share_severity(share, high=0.3, medium=0.05),
                "predicates",
                message,
                fix,
                node_id=access.node_id,
                cost_share=access.cost_share,
                evidence={
                    "table": table,
                    "column": column_name,
                    "wrapper": kind,
                    **({"deliberate_steering": steering} if identity else {}),
                    "operation": access.operation,
                    "dynamic_seek": dynamic,
                    "rewrite_pattern": (
                        "implicit_conversion_on_column"
                        if implicit
                        else "leading_wildcard_like"
                        if wildcard
                        else "function_on_column"
                    ),
                },
            )
        node = by_id.get(access.node_id)
        rows_read = _float(node.element.get("EstimatedRowsRead")) if node is not None else 0.0
        cardinality = node.table_cardinality if node is not None else 0.0
        wildcard_fix = _WILDCARD_FIX[0].lower() + _WILDCARD_FIX[1:]
        for column in access.residual_wildcard_columns:
            column_name = clean_text(column, 128)
            seek_on = ", ".join(clean_text(item, 128) or "" for item in access.seek_eq_columns + access.seek_range_columns)
            severity = "low"
            if access.operation in {"lookup", "rid_lookup"}:
                # The lookup fetches one row per outer row: the index that feeds it limits the rows.
                feeder = clean_text(access.paired_index, 128)
                message = (
                    f"{table}.{column_name} is searched with LIKE and a leading wildcard in a residual predicate on "
                    f"the {access.operation.replace('_', ' ')}: {feeder or 'the index access that feeds it'} limits "
                    "the rows, and the LIKE is tested on each row the lookup fetches."
                )
                fix = (
                    f"Add {column_name} as an INCLUDE column to {feeder or 'the index that feeds the lookup'}, so "
                    "the LIKE filters the rows before the lookups run (or let review_workload_indexes design it); "
                    "otherwise " + wildcard_fix
                )
            elif cardinality > 0 and rows_read >= cardinality * WIDE_SEEK_READ_FRACTION:
                severity = _share_severity(share, high=0.3, medium=0.05)
                message = (
                    f"{table}.{column_name} is searched with LIKE and a leading wildcard in a residual predicate: the "
                    f"seek on {seek_on or 'the index'} reads about {rows_read:,.0f} of {cardinality:,.0f} rows, so "
                    "the LIKE is tested on most of the table."
                )
                fix = "Make the seek more selective, or " + wildcard_fix
            else:
                message = (
                    f"{table}.{column_name} is searched with LIKE and a leading wildcard in a residual predicate: "
                    f"the {'seek on ' + seek_on if seek_on else access.operation.replace('_', ' ')} limits the "
                    "rows, and the LIKE filters the rows it returns."
                )
                fix = (
                    "Usually acceptable. If the LIKE discards most of the rows the seek reads, make the seek more "
                    "selective; otherwise " + wildcard_fix
                )
            add(
                "non_sargable_predicate",
                severity,
                "predicates",
                message,
                fix,
                node_id=access.node_id,
                cost_share=access.cost_share,
                evidence={
                    "table": table,
                    "column": column_name,
                    "wrapper": "leading_wildcard",
                    "operation": access.operation,
                    "dynamic_seek": False,
                    "rewrite_pattern": "leading_wildcard_like",
                },
            )
        # An actual plan counts the lookups that ran: an underestimated outer
        # input can run a lookup the optimizer expected once 200,000 times, and
        # an overestimated one can run it far less often (or never).
        actual_executions = node.actual_executions if node is not None and node.has_runtime else None
        measured = actual_executions is not None
        timed = node is not None and node.has_timing
        executions = actual_executions if actual_executions is not None else access.estimated_executions
        if access.operation in {"lookup", "rid_lookup"} and executions >= LOOKUP_EXECUTION_FLOOR:
            fetched = [clean_text(column, 128) or "" for column in access.output_columns]
            runs = (
                f"{actual_executions:,.0f} times (estimated {access.estimated_executions:,.0f})"
                if actual_executions is not None
                else f"about {access.estimated_executions:,.0f} times"
            )
            add(
                "rid_lookup" if access.operation == "rid_lookup" else "key_lookup",
                (
                    _measured_lookup_severity(executions, access.estimated_executions, access.cost_share)
                    if measured and not timed
                    else _share_severity(share, high=0.3, medium=0.1)
                ),
                "indexes",
                f"{access.operation.replace('_', ' ').title()} on {table} runs {runs} to fetch "
                f"{', '.join(fetched) or 'row data'}.",
                (
                    f"Add {', '.join(fetched)} as INCLUDE columns to "
                    f"{clean_text(access.paired_index, 128)} (or let review_workload_indexes design it), "
                    "then benchmark."
                    if access.paired_index and fetched
                    else "Cover the lookup with an index that includes the fetched columns, then benchmark."
                ),
                node_id=access.node_id,
                cost_share=access.cost_share,
                # A covering index removes the lookup and the loop that drives it.
                benefit_nodes=_lookup_and_loop(node),
                benefit_basis=(
                    f"self time of the lookup (node {access.node_id}) and its driving Nested Loops "
                    f"(node {node.parent.node_id})"
                    if node is not None and node.parent is not None and node.parent.physical_op == "Nested Loops"
                    else None
                ),
                evidence={
                    "table": table,
                    "estimated_executions": access.estimated_executions,
                    "actual_executions": actual_executions,
                    "measured": measured,
                    "fetched_columns": fetched,
                    "feeding_index": access.paired_index,
                },
            )
        seekable = [c for c, kind in access.residual.items() if kind in {"eq", "range"}]
        # A seek index does not help a columnstore scan, and a table of a few thousand
        # rows is read whole in a handful of pages either way.
        small = 0 < access.table_cardinality < SCAN_TABLE_ROW_FLOOR
        if access.operation in {"scan", "heap_scan"} and seekable and share >= 0.05 and not columnstore and not small:
            add(
                "scan_with_seekable_predicate",
                _share_severity(share, high=0.4, medium=0.1),
                "indexes",
                f"{table} is scanned while filtering on {', '.join(clean_text(c, 128) or '' for c in seekable)}; "
                "an index keyed on those columns could seek instead.",
                "Design the index from the access pattern (equality columns, then one range "
                "column, includes for output) with review_workload_indexes, then benchmark.",
                node_id=access.node_id,
                cost_share=access.cost_share,
                evidence={
                    "table": table,
                    "residual_predicates": dict(access.residual),
                    "estimated_rows": access.estimated_rows,
                    "index_used": access.index_name,
                },
            )


def _value_cannot_change_plan(rules: _StatementRules, parameter: str) -> bool:
    """Whether no value of this parameter could have given another plan.

    A trivial plan was not chosen by cost, so no sniffed value shaped it. And
    an equality seek of a whole unique key returns one row for any value: when
    every use of the parameter is such a seek, the estimate cannot differ.
    Uniqueness comes from the catalog (``unique_keys``), never from the plan.
    """

    if (rules.statement.get("StatementOptmLevel") or "").upper() == "TRIVIAL":
        return True
    if not rules.unique_keys or not parameter:
        return False
    uses = [
        reference
        for reference in rules.query_plan.iter(f"{_Q}ColumnReference")
        if reference.get("Column") == parameter and not reference.get("ParameterCompiledValue")
    ]
    seek_uses = 0
    for access in rules.accesses:
        node = rules.by_id.get(access.node_id)
        if node is None:
            continue
        in_equality_seek = [
            reference
            for prefix in node.element.iter(f"{_Q}Prefix")
            if (prefix.get("ScanType") or "").upper() == "EQ"
            for reference in prefix.iter(f"{_Q}ColumnReference")
            if reference.get("Column") == parameter
        ]
        if not in_equality_seek:
            continue
        sought = {column.lower() for column in access.seek_eq_columns}
        keys = rules.unique_keys.get(f"{access.schema}.{access.table}".lower(), [])
        if not any(key and key <= sought for key in keys):
            return False
        seek_uses += len(in_equality_seek)
    return bool(uses) and seek_uses == len(uses)


def _statement_warning_rules(rules: _StatementRules) -> None:
    statement, query_plan, add, actual = rules.statement, rules.query_plan, rules.add, rules.actual
    for convert in query_plan.findall(f"{_Q}Warnings/{_Q}PlanAffectingConvert"):
        _plan_affecting_convert(rules, convert)
    requests = [
        (_float(group.get("Impact")), index)
        for group in query_plan.findall(f"{_Q}MissingIndexes/{_Q}MissingIndexGroup")
        for index in group.findall(f"{_Q}MissingIndex")
    ]
    per_table: dict[str | None, int] = {}
    for _, index in requests:
        name = clean_text(f"{unquote(index.get('Schema'))}.{unquote(index.get('Table'))}", 200)
        per_table[name] = per_table.get(name, 0) + 1
    for impact, index in requests:
        columns: dict[str, list[str]] = {"EQUALITY": [], "INEQUALITY": [], "INCLUDE": []}
        for column_group in index.findall(f"{_Q}ColumnGroup"):
            usage = (column_group.get("Usage") or "").upper()
            columns.setdefault(usage, []).extend(
                clean_text(unquote(column.get("Name")), 128) or "" for column in column_group.findall(f"{_Q}Column")
            )
        table = clean_text(f"{unquote(index.get('Schema'))}.{unquote(index.get('Table'))}", 200)
        # PerformanceMonitor calibration: over 4 keys, over 5 includes, several requests
        # for one table, and an impact under 25% is not worth a lead.
        keys = len(columns["EQUALITY"]) + len(columns["INEQUALITY"])
        wide = len(columns["INCLUDE"]) > 5
        quality: list[str] = []
        if keys > 4:
            quality.append(f"{keys} key columns: more than an index usually needs")
        if wide:
            quality.append(f"{len(columns['INCLUDE'])} include columns: the include list is wide")
        if per_table[table] > 1:
            quality.append(f"{per_table[table]} requests for this table in one statement: design one index for them")
        add(
            "missing_index_hint",
            "info" if impact < 25 else "medium" if impact >= 50 else "low",
            "indexes",
            f"The optimizer suggests an index on {table} (estimated impact {impact:.0f}%)"
            + (f"; {'; '.join(quality)}." if quality else "."),
            "Treat it as a lead: merge it with existing indexes and other queries' needs "
            "(review_workload_indexes) instead of creating it verbatim.",
            evidence={
                "impact_pct": impact,
                "equality": columns["EQUALITY"],
                "inequality": columns["INEQUALITY"],
                "include": columns["INCLUDE"],
                "wide_include_list": wide,
                "quality_flags": quality,
            },
        )
    for name, rule, text in (
        ("ColumnsWithNoStatistics", "column_without_statistics", "No statistics exist on {column}; the estimate is a guess."),
        ("ColumnsWithStaleStatistics", "stale_statistics", "Statistics on {column} were stale when the plan compiled."),
    ):
        for column in query_plan.findall(f".//{_Q}Warnings/{_Q}{name}/{_Q}ColumnReference"):
            table_name = clean_text(unquote(column.get("Table")), 128)
            column_name = clean_text(unquote(column.get("Column")), 128)
            add(
                rule,
                "medium",
                "cardinality",
                text.format(column=f"{table_name}.{column_name}"),
                (
                    "Check AUTO_CREATE_STATISTICS and create statistics on the column if it is off."
                    if rule == "column_without_statistics"
                    else "Update the statistics (check_statistics_health) and compare the new plan."
                ),
                evidence={"table": table_name, "column": column_name},
            )
    if query_plan.find(f"{_Q}UnmatchedIndexes") is not None:
        add(
            "unmatched_filtered_index",
            "medium",
            "indexes",
            "A filtered index could not be used because the query is parameterized.",
            "Use a literal for the filter predicate, OPTION (RECOMPILE), or an unfiltered index.",
        )
    reason = clean_text(query_plan.get("NonParallelPlanReason"), 120)
    root = query_plan.find(f"{_Q}RelOp")
    # A cursor's plans share one statement: each plan's own root cost decides.
    plan_cost = _float(root.get("EstimatedTotalSubtreeCost")) if root is not None else 0.0
    plan_cost = plan_cost or _float(statement.get("StatementSubTreeCost"))
    if (
        reason
        and reason not in {"MaxDOPSetToOne", "EstimatedDOPIsOne", "NoParallelPlansInDesktopOrExpressEdition"}
        and plan_cost >= PARALLELISM_COST_FLOOR
    ):
        add(
            "non_parallel_plan_reason",
            "medium" if "UserDefinedFunction" in reason or "TableVariable" in reason else "info",
            "parallelism",
            f"The plan could not go parallel: {reason}.",
            (
                "A FAST_FORWARD cursor never runs in parallel: rewrite the loop as a set-based statement, "
                "or declare the cursor STATIC if its populate query must go parallel."
                if reason == "NoParallelFastForwardCursor"
                else "Remove the blocker (for example a non-inlinable scalar UDF, which often reports "
                "CouldNotGenerateValidParallelPlan) if the query is CPU-bound and large."
            ),
            evidence={"non_parallel_plan_reason": reason},
        )
    abort = statement.get("StatementOptmEarlyAbortReason")
    if abort in {"TimeOut", "MemoryLimitExceeded"}:
        add(
            "optimizer_early_abort",
            "medium",
            "compile",
            f"The optimizer stopped early ({abort}); the chosen plan may be far from the best.",
            "Simplify the query (fewer joins per statement, split into steps) before tuning indexes.",
            evidence={"reason": abort},
        )
    compile_cpu = _float(query_plan.get("CompileCPU"))
    if compile_cpu >= 1000:
        add(
            "high_compile_cpu",
            "medium",
            "compile",
            f"Compiling this statement used {compile_cpu:,.0f} ms of CPU.",
            "Avoid recompiling it on every call (plan reuse) or simplify the statement.",
            evidence={"compile_cpu_ms": compile_cpu, "compile_time_ms": _float(query_plan.get("CompileTime"))},
        )
    for parameter in query_plan.findall(f"{_Q}ParameterList/{_Q}ColumnReference"):
        compiled = clean_text(parameter.get("ParameterCompiledValue"), 100)
        runtime = clean_text(parameter.get("ParameterRuntimeValue"), 100)
        name = clean_text(parameter.get("Column"), 128)
        if (
            actual
            and compiled is not None
            and runtime is not None
            and compiled != runtime
            and not _value_cannot_change_plan(rules, parameter.get("Column") or "")
        ):
            add(
                "sniffed_parameter_differs",
                "info",
                "cardinality",
                f"{name} was compiled for {compiled} but ran with {runtime}.",
                "Check parameter sensitivity with get_query_parameter_buckets before changing anything.",
                evidence={"parameter": name, "compiled": compiled, "runtime": runtime},
            )
    times = query_time_stats(query_plan)
    if times is not None and times.elapsed_ms and times.udf_elapsed_ms:
        share = times.udf_elapsed_ms / times.elapsed_ms
        if share >= UDF_TIME_SHARE_FLOOR:
            add(
                "scalar_udf_time",
                "high" if share >= 0.5 else "medium",
                "udf",
                f"Scalar UDFs took {share:.0%} of the statement's elapsed time "
                f"({times.udf_elapsed_ms:,.0f} of {times.elapsed_ms:,.0f} ms). No operator "
                "shows this time, so self times understate the functions.",
                "Inline the function logic, or make the function inlinable (compatibility "
                "level 150+) and confirm the plan changes.",
                evidence={
                    "udf_elapsed_ms": times.udf_elapsed_ms,
                    "udf_cpu_ms": times.udf_cpu_ms,
                    "elapsed_ms": times.elapsed_ms,
                    "udf_elapsed_share": round(share, 4),
                },
                benefit_ms=times.udf_elapsed_ms,
                benefit_basis="scalar UDF elapsed time (QueryTimeStats)",
            )
    grant = query_plan.find(f"{_Q}MemoryGrantInfo")
    if grant is not None and actual:
        # GrantWaitTime is in seconds (showplan XSD, MemoryGrantType).
        wait_s = _float(grant.get("GrantWaitTime"))
        if wait_s > 0:
            add(
                "memory_grant_wait",
                "high",
                "memory",
                f"The query waited {wait_s:,.0f} s for its memory grant (RESOURCE_SEMAPHORE).",
                "Reduce the grant (fix row estimates, avoid wide sorts) or reduce concurrency.",
                evidence={
                    "grant_wait_ms": wait_s * 1000,
                    "grant_wait_s": wait_s,
                    "granted_kb": _float(grant.get("GrantedMemory")),
                },
                benefit_ms=wait_s * 1000,
                benefit_basis="time spent waiting for the memory grant",
            )
    for warning in query_plan.findall(f"{_Q}Warnings/{_Q}MemoryGrantWarning"):
        kind = clean_text(warning.get("GrantWarningKind"), 80) or ""
        # An unused grant is graded by excessive_memory_grant, which knows the size that matters.
        if kind.replace(" ", "").lower() == "excessivegrant" and _float(warning.get("GrantedMemory")) < LARGE_GRANT_KB:
            continue
        add(
            "memory_grant_warning",
            "medium",
            "memory",
            f"Memory grant warning: {kind}.",
            "Fix the row estimate that sized the grant.",
            evidence={key: clean_text(value, 100) for key, value in warning.attrib.items()},
        )


def _operator_rules(node: PlanNode, rules: _StatementRules) -> None:
    add, actual, table_access = rules.add, rules.actual, rules.access_by_node.get(node.node_id)
    relop = node.element
    physical = node.physical_op
    logical = node.logical_op
    warnings = relop.find(f"{_Q}Warnings")
    access = relop.find(f"{_Q}IndexScan")
    if access is None:
        access = relop.find(f"{_Q}TableScan")
    if access is not None:
        obj = access.find(f"{_Q}Object")
        table = clean_text(unquote(obj.get("Table")), 128) if obj is not None else None
        if table and table.startswith("@"):
            # One row is the fixed guess of a statement compiled before the table variable
            # was filled. A larger estimate is a real count: deferred compilation
            # (compatibility level 150+), OPTION (RECOMPILE), or a table-valued parameter.
            guessed = node.estimate_rows <= 1
            add(
                "table_variable",
                "medium" if guessed else "low",
                "cardinality",
                (
                    f"Table variable {table} (node {node.node_id}) is read with the fixed guess of "
                    f"{node.estimate_rows:,.0f} row(s): the statement compiled before the table variable was "
                    "filled, so joins and memory grants are sized for one row."
                    if guessed
                    else f"Table variable {table} (node {node.node_id}) is read with an estimate of "
                    f"{node.estimate_rows:,.0f} rows: the row count was known at compile time, but a table "
                    "variable has no column statistics, so a predicate or join on its columns can still be "
                    "misestimated."
                ),
                (
                    "Run the statement under compatibility level 150 or higher (table variable deferred "
                    "compilation) or add OPTION (RECOMPILE); measure before moving the rows to a temporary table."
                    if guessed
                    else "No change is needed for the row count. Only if an actual plan shows a row_estimate_gap "
                    "on a join or filter over its columns, measure a temporary table, which has column statistics."
                ),
                node_id=node.node_id,
                evidence={"table": table, "estimated_rows": node.estimate_rows, "fixed_guess": guessed},
            )
    spool = eager_spool_index(relop)
    if spool is not None or (physical == "Index Spool" and logical == "Eager Spool"):
        table = clean_text(f"{spool.schema}.{spool.table}", 200) if spool is not None and spool.table else None
        keys = [clean_text(c, 128) or "" for c in (spool.eq_columns + spool.range_columns)] if spool else []
        includes = [clean_text(c, 128) or "" for c in spool.include_columns] if spool else []
        add(
            "eager_index_spool",
            "high",
            "indexes",
            (
                f"The optimizer builds a temporary index on {table} ({', '.join(keys)})"
                + (f" INCLUDE ({', '.join(includes)})" if includes else "")
                + " on every execution, because no permanent index fits. The spool also "
                "suppresses the missing-index request: this spool is the request."
                if table and keys
                else "The optimizer builds a temporary index (eager index spool) on every execution."
            ),
            "Create a permanent index with the spool's keys and includes (review_workload_indexes "
            "designs it against existing indexes), then benchmark. Check for a large overestimate "
            "on the spool's input first.",
            node_id=node.node_id,
            evidence={
                "table": table,
                "key_columns": keys,
                "include_columns": includes,
                "estimated_rows_per_execution": node.estimate_rows,
                "estimated_executions": node.estimated_executions,
            },
        )
    if physical == "Row Count Spool":
        add(
            "row_count_spool",
            "medium",
            "predicates",
            "A row count spool usually means NOT IN against a nullable column.",
            "Rewrite as NOT EXISTS (check NULL semantics first) or make the column NOT NULL.",
            node_id=node.node_id,
            evidence={"rewrite_pattern": "not_in_nullable_column"},
        )
    if warnings is not None and is_true(warnings.get("NoJoinPredicate")):
        _no_join_predicate(node, add)
    merge = relop.find(f"{_Q}Merge")
    if merge is not None and is_true(merge.get("ManyToMany")):
        add(
            "many_to_many_merge_join",
            "medium",
            "joins",
            "A many-to-many merge join builds a worktable in tempdb.",
            "Make one join input unique (a unique index or DISTINCT on the right key) if the semantics allow.",
            node_id=node.node_id,
        )
    if physical == "Nested Loops" and len(node.children) >= 2:
        inner = node.children[1]
        measured = actual and inner.has_runtime
        executions = inner.actual_executions if measured else inner.estimated_executions
        if executions >= NESTED_LOOP_EXECUTION_FLOOR:
            add(
                "nested_loops_high_inner_executions",
                "medium",
                "joins",
                f"The inner side of a nested loops join runs about {executions:,.0f} times"
                + (
                    f" (estimated {inner.estimated_executions:,.0f})."
                    if measured
                    else "."
                ),
                "Check the outer row estimate (the first input, node "
                f"{node.children[0].node_id}); an index on the inner join key or a hash join may fit better.",
                node_id=node.node_id,
                evidence={
                    "inner_executions": executions,
                    "estimated_inner_executions": inner.estimated_executions,
                    "outer_node_id": node.children[0].node_id,
                    "actual": measured,
                },
            )
    udfs = [element for element in node.own_elements if element.tag == _UDF]
    if udfs:
        _scalar_udf(node, udfs, add, table_access)
    if physical == "Table-valued function":
        function = relop.find(f"{_Q}TableValuedFunction/{_Q}Object")
        schema = unquote(function.get("Schema")) if function is not None else None
        # STRING_SPLIT, OPENJSON, GENERATE_SERIES and the system functions are engine
        # functions: the plan names them without a schema, and nothing can be inlined.
        if function is not None and schema and schema.lower() != "sys":
            name = clean_text(f"{schema}.{unquote(function.get('Table'))}", 200)
            # 100 rows (1 under the legacy estimator) is the fixed guess; another
            # number means interleaved execution supplied the real count.
            guessed = node.estimate_rows in (1.0, 100.0)
            add(
                "multi_statement_tvf",
                "medium" if guessed else "low",
                "cardinality",
                (
                    f"Multi-statement table-valued function {name} (node {node.node_id}) supplies rows with "
                    f"the fixed guess of {node.estimate_rows:,.0f} row(s)."
                    if guessed
                    else f"Multi-statement table-valued function {name} (node {node.node_id}) is estimated at "
                    f"{node.estimate_rows:,.0f} rows (interleaved execution supplied the count), but its result "
                    "has no column statistics."
                ),
                "Replace it with an inline table-valued function so the optimizer sees through it; interleaved "
                "execution (compatibility level 140+) corrects only the row count.",
                node_id=node.node_id,
                evidence={"function": name, "estimated_rows": node.estimate_rows, "fixed_guess": guessed},
            )


def _scalar_udf(node: PlanNode, udfs: list[ET.Element], add, access: TableAccess | None) -> None:
    """A UserDefinedFunction element is a call that was not inlined, on any operator.

    ``wrapped_columns`` lists table columns (schema.table.column) that a UDF wraps in a
    predicate on this operator's own input, which an index on the column cannot seek.
    """

    own = {id(element) for element in node.own_elements}
    # A table access reads its own columns from the access record (outer references
    # excluded). A Filter or join reads only what its inputs output; anything else in
    # its predicate is an outer reference. An Assert checks a constraint on rows being
    # written, which nothing searches.
    inputs = {
        _column_key(ref)
        for child in node.children
        for ref in child.element.iterfind(f"{_Q}OutputList/{_COLUMN_REFERENCE}")
    }
    read_predicates = access is None and node.physical_op != "Assert"
    in_predicate = False
    wrapped: set[str] = set()
    for container in node.own_elements:
        if container.tag not in _PREDICATE_TAGS:
            continue
        for udf in container.iter(_UDF):
            if id(udf) not in own:
                continue  # inside a subquery's own operator
            in_predicate = True
            if container.tag == f"{_Q}SeekPredicates" or not read_predicates:
                continue  # a seek key holds the function on the value side, not on the column
            wrapped.update(
                _qualified_column(ref)
                for ref in udf.iter(_COLUMN_REFERENCE)
                if id(ref) in own and ref.get("Table") and _column_key(ref) in inputs
            )
    if access is not None:
        wrapped.update(
            clean_text(f"{access.schema}.{access.table}.{column}", 300) or ""
            for column, kind in access.nonsargable.items()
            if kind == "udf"
        )
    wrapped.discard("")
    names = sorted({clean_text(_object_name(udf.get("FunctionName")), 200) or "" for udf in udfs})
    message = f"Scalar UDF {', '.join(names)} runs row by row and is not inlined."
    if wrapped:
        message += (
            f" It wraps {', '.join(sorted(wrapped))} inside a predicate, so an index on the column "
            "cannot seek it as written (non-SARGable)."
        )
    if in_predicate and any(not is_true(udf.get("IsClrFunction")) for udf in udfs):
        message += " A T-SQL scalar UDF also keeps the whole plan serial."
    add(
        "scalar_udf",
        "medium",
        "udf",
        message,
        "Inline the logic, or make the function inlinable (compatibility level 150+) and confirm the plan changes.",
        node_id=node.node_id,
        evidence={
            "functions": names,
            "in_predicate": in_predicate,
            "wrapped_columns": sorted(wrapped),
            "rewrite_pattern": "scalar_udf_in_predicate" if in_predicate else "scalar_udf_in_select_list",
        },
    )


def _column_key(ref: ET.Element) -> tuple[str | None, ...]:
    return tuple(unquote(ref.get(name)) for name in ("Schema", "Table", "Alias", "Column"))


def _qualified_column(ref: ET.Element) -> str:
    parts = (unquote(ref.get(name)) for name in ("Schema", "Table", "Column"))
    return clean_text(".".join(part for part in parts if part), 300) or ""


def _object_name(value: str | None) -> str:
    """``[db].[dbo].[fn]`` as ``db.dbo.fn``."""

    parts = [part.replace("]]", "]") for part in _NAME_PART.findall(value or "")]
    return ".".join(parts) if parts else unquote(value) or ""


def _no_join_predicate(node: PlanNode, add) -> None:
    """A NoJoinPredicate warning is often benign; grade it from the join's inputs."""

    check = cross_join_check(node)
    severity, message = {
        "correlated": (
            "info",
            "SQL Server flagged no join predicate, but outer references feed the inner side "
            f"({', '.join(check.outer_references)}), so there is nothing to compare. Usually benign.",
        ),
        "implied_predicate": (
            "low",
            "SQL Server flagged no join predicate, but both inputs are filtered on the same value "
            f"({', '.join(check.shared_values)}): the optimizer dropped a join predicate it could imply. "
            "Usually benign.",
        ),
        "no_multiplication": (
            "low",
            "SQL Server flagged no join predicate, but one input has at most one row, so the join "
            "cannot multiply rows.",
        ),
        "not_multiplied": (
            "low",
            "SQL Server flagged no join predicate, but the output did not multiply: this is not an "
            "accidental cross join.",
        ),
        "multiplied": (
            "high",
            "A join with no join predicate emitted the product of its inputs: every row joined to "
            "every row.",
        ),
    }.get(
        check.verdict,
        (
            "medium",
            "A join has no join predicate. Unless both inputs are filtered on the same value (an "
            "implied predicate the optimizer removed), every row joins to every row.",
        ),
    )
    add(
        "no_join_predicate",
        severity,
        "joins",
        message,
        (
            "No change needed if the verdict is benign; otherwise add the missing join condition "
            "(check ON and WHERE for a typo). Compare the predicates of the two inputs."
        ),
        node_id=node.node_id,
        evidence={
            "verdict": check.verdict,
            "outer_references": list(check.outer_references),
            "shared_values": list(check.shared_values),
            "outer_rows": check.outer_rows,
            "inner_rows_per_execution": check.inner_rows,
            "output_rows": check.output_rows,
            "measured": check.measured,
            "input_node_ids": [child.node_id for child in node.children[:2]],
        },
    )


def _runtime_rules(node: PlanNode, rules: _StatementRules) -> None:
    add = rules.add
    if not node.has_runtime:
        return
    warnings = node.element.find(f"{_Q}Warnings")
    if warnings is not None:
        spill = warnings.find(f"{_Q}SpillToTempDb")
        if spill is not None:
            details = warnings.find(f"{_Q}SortSpillDetails")
            if details is None:
                details = warnings.find(f"{_Q}HashSpillDetails")
            add(
                "tempdb_spill",
                "high" if _float(spill.get("SpillLevel")) >= 2 else "medium",
                "memory",
                f"{node.physical_op} spilled to tempdb (level {clean_text(spill.get('SpillLevel'), 10)}).",
                "Fix the row estimate that sized the memory grant, or reduce the rows and columns sorted or hashed.",
                node_id=node.node_id,
                evidence={
                    "spill_level": clean_text(spill.get("SpillLevel"), 10),
                    "writes_to_tempdb": (
                        clean_text(details.get("WritesToTempDb"), 30) if details is not None else None
                    ),
                },
            )
        elif warnings.find(f"{_Q}SpillOccurred") is not None:
            add(
                "tempdb_spill",
                "medium",
                "memory",
                f"{_label(node)} spilled to tempdb; this plan source does not record the spill level.",
                "Fix the row estimate that sized the memory grant, or reduce the rows and columns sorted or hashed.",
                node_id=node.node_id,
                evidence={"spill_level": None, "writes_to_tempdb": None},
            )
        exchange = warnings.find(f"{_Q}ExchangeSpillDetails")
        if exchange is not None:
            add(
                "exchange_spill",
                "medium",
                "parallelism",
                f"A parallel exchange spilled to tempdb ({_float(exchange.get('WritesToTempDb')):,.0f} writes).",
                "Look for an order-preserving exchange or skewed threads feeding a merge join; "
                "removing the ordering requirement usually removes the spill.",
                node_id=node.node_id,
                evidence={"writes_to_tempdb": _float(exchange.get("WritesToTempDb"))},
            )
    worker_rows = [counter.rows for counter in node.threads if counter.thread > 0]
    total_rows = sum(worker_rows)
    # A running operator's threads are at different points of their input.
    if len(worker_rows) >= 2 and total_rows >= 10_000 and rules.settled(node):
        # With n threads max/average can never exceed n, so judge skew by the
        # busiest thread's share against an even split.
        threads = len(worker_rows)
        busiest = max(worker_rows)
        share = busiest / total_rows
        skewed = (
            share >= 0.85
            if threads == 2
            else busiest >= 2.5 * total_rows / threads and share >= 0.5
        )
        if skewed:
            add(
                "parallel_thread_skew",
                "medium",
                "parallelism",
                f"One of {threads} parallel threads handled {share:.0%} of the rows "
                f"({busiest:,.0f} of {total_rows:,.0f}).",
                "Skewed data or a skewed partitioning column leaves threads idle; review the distribution.",
                node_id=node.node_id,
                evidence={
                    "max_thread_rows": busiest,
                    "total_rows": total_rows,
                    "busiest_thread_share": round(share, 3),
                    "threads": threads,
                },
            )


def _measured_lookup_severity(actual: float, estimated: float, cost_share: float) -> str:
    """Grade a lookup by its measured count when the plan did not time its operators.

    The estimated cost share was sized for the estimated count, so it holds only
    while the lookup ran within ESTIMATE_GAP_FACTOR of that count.
    """

    if actual >= estimated * ESTIMATE_GAP_FACTOR:
        return "high" if actual >= LOOKUP_EXECUTION_FLOOR * ESTIMATE_GAP_FACTOR else "medium"
    if actual * ESTIMATE_GAP_FACTOR <= estimated:
        return "low"
    return _share_severity(cost_share, high=0.3, medium=0.1)


def _share_severity(share: float, *, high: float, medium: float) -> str:
    if share >= high:
        return "high"
    if share >= medium:
        return "medium"
    return "low"


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _is_columnstore(access: TableAccess) -> bool:
    return (access.storage or "").lower() == "columnstore"


def _actual_mode(node: PlanNode) -> str | None:
    return next((counter.mode for counter in node.threads if counter.mode), None)


def _lookup_and_loop(node: PlanNode | None) -> list[PlanNode] | None:
    if node is None:
        return None
    if node.parent is not None and node.parent.physical_op == "Nested Loops":
        return [node, node.parent]
    return [node]


_QUALIFIED_NAME = re.compile(r"(?:\[(?:[^\]]|\]\])*\]\s*\.\s*)+\[(?:[^\]]|\]\])*\]")


def _plan_affecting_convert(rules: _StatementRules, convert: ET.Element) -> None:
    """SQL Server raises this warning for a conversion anywhere in the statement.

    ``Seek Plan`` always matters. ``Cardinality Estimate`` matters only when the
    converted column sits in a predicate, a join key, a grouping or an order: a
    conversion in the output list alone changes nothing.
    """

    issue = clean_text(convert.get("ConvertIssue"), 60) or ""
    raw = convert.get("Expression") or ""
    expression = clean_text(raw, 300)
    explicit = raw.lstrip().upper().startswith(("CONVERT(", "CAST(", "TRY_CONVERT(", "TRY_CAST("))
    position = None if issue == "Seek Plan" else _convert_position(rules.nodes, raw)
    if position == "output_only":
        rules.add(
            "plan_affecting_convert",
            "info",
            "predicates",
            f"SQL Server reports a type conversion ({issue}): {expression or 'expression not shown'}. In this plan "
            "the conversion is only in an output expression: it does not change the access path, a join, a "
            "grouping or the estimates.",
            "No change is needed for performance.",
            evidence={"convert_issue": issue, "expression": expression, "position": position, "explicit": explicit},
        )
        return
    rules.add(
        "plan_affecting_convert",
        "high" if issue == "Seek Plan" else "medium",
        "predicates",
        f"The optimizer reports a type conversion that affects the plan ({issue}): "
        f"{expression or 'expression not shown'}.",
        (
            "Remove the explicit CONVERT or CAST from the column, or move it to the constant or parameter side."
            if explicit
            else "Make the parameter, variable, or join column types match so the conversion disappears."
        ),
        evidence={"convert_issue": issue, "expression": expression, "position": position, "explicit": explicit},
    )


def _convert_position(nodes: list[PlanNode], expression: str) -> str | None:
    """Where the plan applies the conversion: ``plan_shaping``, ``output_only``, or None when not found."""

    wanted: set[tuple[str, str]] = set()
    for name in _QUALIFIED_NAME.findall(expression):
        parts = [part.replace("]]", "]").lower() for part in _NAME_PART.findall(name)]
        wanted.add((parts[-2], parts[-1]))
    if not wanted:
        return None

    def converts_wanted(convert: ET.Element) -> bool:
        return any(
            ((unquote(ref.get("Table")) or "").lower(), (unquote(ref.get("Column")) or "").lower()) in wanted
            or ((unquote(ref.get("Alias")) or "").lower(), (unquote(ref.get("Column")) or "").lower()) in wanted
            for ref in convert.iter(_COLUMN_REFERENCE)
        )

    found = False
    defined: set[str] = set()
    shaping_references: set[str] = set()
    for node in nodes:
        shaping: set[int] = set()
        for container in node.own_elements:
            if container.tag not in _PLAN_SHAPING_TAGS:
                continue
            shaping.update(id(convert) for convert in container.iter(f"{_Q}Convert"))
            shaping_references.update(
                ref.get("Column") or "" for ref in container.iter(_COLUMN_REFERENCE) if not ref.get("Table")
            )
        for element in node.own_elements:
            if element.tag == f"{_Q}Convert" and converts_wanted(element):
                if id(element) in shaping:
                    return "plan_shaping"
                found = True
            elif element.tag == f"{_Q}DefinedValue":
                target = element.find(_COLUMN_REFERENCE)
                if target is not None and any(converts_wanted(convert) for convert in element.iter(f"{_Q}Convert")):
                    defined.add(target.get("Column") or "")
    # A Compute Scalar can define the converted value that a predicate, key or sort above it uses.
    if defined & shaping_references:
        return "plan_shaping"
    return "output_only" if found else None


def _columnstore_rules(rules: _StatementRules) -> None:
    add = rules.add
    for access in rules.accesses:
        node = rules.by_id.get(access.node_id)
        if node is None or not _is_columnstore(access):
            continue
        table = clean_text(f"{access.schema}.{access.table}", 200)
        index = clean_text(access.index_name, 128)
        share = rules.impact(access.node_id, access.cost_share)
        columns = [clean_text(column, 128) or "" for column in dict.fromkeys([*access.residual, *access.nonsargable])]
        measured = node.has_runtime
        rows = node.actual_rows if measured else None
        rows_read = node.actual_rows_read if measured else None
        reads, skips = node.segment_reads, node.segment_skips
        if columns and measured and rules.settled(node) and share >= HARM_SHARE_FLOOR:
            evidence = {
                "table": table,
                "index": index,
                "predicate_columns": columns,
                "segments_read": reads,
                "segments_skipped": skips,
                "rows": rows,
                "rows_read": rows_read,
            }
            fix = (
                f"Align the rowgroups with {', '.join(columns)}: rebuild the columnstore index with ORDER "
                f"({', '.join(columns)}) (an ordered columnstore index), or load the rows in that order, or "
                "partition the table by that column. Then check segments skipped on a new actual plan."
            )
            if reads is not None and reads >= SEGMENT_READ_FLOOR and not skips:
                add(
                    "columnstore_no_segment_elimination",
                    _share_severity(share, high=0.3, medium=0.1),
                    "batch_mode",
                    f"{_label(node)} read {reads:,.0f} rowgroups of {table} and skipped none while filtering on "
                    f"{', '.join(columns)}"
                    + (f" ({rows_read:,.0f} rows read to return {rows:,.0f})" if rows_read else "")
                    + ": the minimum and maximum values of each rowgroup overlap the filter, so no rowgroup "
                    "is eliminated.",
                    fix,
                    node_id=node.node_id,
                    cost_share=access.cost_share,
                    evidence=evidence,
                )
            elif rows_read is not None and rows_read >= ROWGROUP_ROWS and rows_read >= ROWS_READ_RATIO * max(rows or 0.0, 1.0):
                add(
                    "columnstore_low_selectivity_scan",
                    _share_severity(share, high=0.3, medium=0.1),
                    "batch_mode",
                    f"{_label(node)} read {rows_read:,.0f} rows of {table} to return {rows:,.0f} while filtering on "
                    f"{', '.join(columns)}"
                    + (f" ({skips:,.0f} of {reads + (skips or 0.0):,.0f} rowgroups skipped)" if reads is not None else "")
                    + ": most rowgroups hold a few matching rows, so they are all read.",
                    fix,
                    node_id=node.node_id,
                    cost_share=access.cost_share,
                    evidence=evidence,
                )
        actual_mode = _actual_mode(node)
        mode = actual_mode or node.estimated_mode
        processed = (rows_read or rows or 0.0) if measured else (node.estimated_rows_read or node.estimate_rows)
        if mode == "Row" and processed >= BATCH_MODE_ROW_FLOOR:
            members = [node]
            while members[-1].parent is not None and (_actual_mode(members[-1].parent) or members[-1].parent.estimated_mode) != "Batch":
                members.append(members[-1].parent)
            hinted = "DISALLOW_BATCH_MODE" in (rules.statement.get("StatementText") or "").upper()
            add(
                "columnstore_row_mode",
                _share_severity(rules.share_of(members), high=0.3, medium=HARM_SHARE_FLOOR) if rules.timed else "medium",
                "batch_mode",
                f"{_label(node)} reads columnstore index {index} on {table} in row mode ({processed:,.0f} rows"
                + (", although the plan was compiled for batch mode" if node.estimated_mode == "Batch" else "")
                + "): the rows are handled one at a time, not in batches, and the operators above it run "
                "in row mode too.",
                (
                    "Remove USE HINT('DISALLOW_BATCH_MODE') from the statement, then compare."
                    if hinted
                    else "Find what keeps this statement out of batch mode: a database compatibility level below "
                    "130, a cursor, a hint that disallows batch mode, or a construct that only runs in row mode. "
                    "Change it, confirm EstimatedExecutionMode is Batch on the scan, then compare elapsed time."
                ),
                node_id=node.node_id,
                cost_share=access.cost_share,
                benefit_nodes=members,
                benefit_basis="self time of the scan and the row mode operators above it",
                evidence={
                    "table": table,
                    "index": index,
                    "estimated_mode": node.estimated_mode or None,
                    "actual_mode": actual_mode,
                    "rows": processed,
                    "batch_mode_disallowed_by_hint": hinted,
                },
            )
        elif mode == "Batch":
            consumer = node.parent
            while consumer is not None and (consumer.is_exchange or consumer.physical_op == "Compute Scalar"):
                consumer = consumer.parent
            flowing = node.actual_rows if measured else node.estimate_rows
            if consumer is None or flowing < BATCH_MODE_ROW_FLOOR:
                continue
            consumer_actual = _actual_mode(consumer)
            # An operator compiled for batch mode that ran in row mode is batch_mode_fallback.
            if (consumer_actual or consumer.estimated_mode) != "Row" or consumer.estimated_mode == "Batch":
                continue
            add(
                "columnstore_feeds_row_mode",
                # Without times this is a lead: an estimated plan cannot show aggregate
                # pushdown, which keeps the rows out of the row mode operator.
                _share_severity(rules.share_of([consumer]), high=0.3, medium=HARM_SHARE_FLOOR) if rules.timed else "low",
                "batch_mode",
                f"{_label(consumer)} runs in row mode on the {flowing:,.0f} rows that the batch mode columnstore "
                f"scan of {table} (node {node.node_id}) returns: the batches are split into single rows at this "
                "operator.",
                "Nested Loops, Merge Join and Stream Aggregate have no batch mode; Hash Match, Sort and Window "
                "Aggregate do. Rewrite so the scan feeds a batch mode operator (for example aggregate before the "
                "join), then compare.",
                node_id=consumer.node_id,
                evidence={
                    "table": table,
                    "scan_node_id": node.node_id,
                    "rows_from_scan": flowing,
                    "consumer_estimated_mode": consumer.estimated_mode or None,
                    "consumer_actual_mode": consumer_actual,
                },
            )
    for node in rules.nodes:
        access = rules.access_by_node.get(node.node_id)
        if node.estimated_mode == "Batch" and _actual_mode(node) == "Row" and (access is None or not _is_columnstore(access)):
            rows_in = max([node.actual_rows, *(child.actual_rows for child in node.children if child.has_runtime)])
            if rows_in < BATCH_MODE_ROW_FLOOR:
                continue
            add(
                "batch_mode_fallback",
                (
                    _share_severity(rules.impact(node.node_id, rules.cost_shares.get(node.node_id, 0.0)), high=0.3, medium=HARM_SHARE_FLOOR)
                    if rules.timed
                    else "medium"
                ),
                "batch_mode",
                f"{_label(node)} was compiled for batch mode but ran in row mode on {rows_in:,.0f} rows.",
                "Batch mode was dropped at run time for this operator. Check it for a spill or a memory grant "
                "warning on this run, and compare with another run (explain_query with analyze=true): a run that "
                "keeps batch mode handles these rows in batches.",
                node_id=node.node_id,
                evidence={"estimated_mode": "Batch", "actual_mode": "Row", "rows": rows_in},
            )
        if node.physical_op == "Filter":
            _columnstore_filter(rules, node)


def _columnstore_filter(rules: _StatementRules, node: PlanNode) -> None:
    """A Filter directly above a columnstore scan holds a predicate the scan could not evaluate."""

    predicate = node.element.find(f"{_Q}Filter/{_Q}Predicate")
    scan = node.children[0] if node.children else None
    while scan is not None and scan.physical_op == "Compute Scalar" and scan.children:
        scan = scan.children[0]
    access = rules.access_by_node.get(scan.node_id) if scan is not None else None
    if scan is None or access is None or predicate is None or not _is_columnstore(access):
        return
    residual, nonsargable = classify_predicate(predicate, access.schema, access.table, access.alias)
    columns = [clean_text(column, 128) or "" for column in dict.fromkeys([*nonsargable, *residual])]
    if not columns:
        return
    measured = node.has_runtime and scan.has_runtime and rules.settled(node)
    rows_in = scan.actual_rows if measured else scan.estimate_rows
    rows_out = node.actual_rows if measured else node.estimate_rows
    if rows_in < BATCH_MODE_ROW_FLOOR or rows_out > rows_in * 0.5:
        return
    table = clean_text(f"{access.schema}.{access.table}", 200)
    wrappers = sorted(set(nonsargable.values()))
    rules.add(
        "columnstore_predicate_not_pushed",
        (
            _share_severity(rules.impact(node.node_id, rules.cost_shares.get(node.node_id, 0.0)), high=0.3, medium=HARM_SHARE_FLOOR)
            if rules.timed
            else "medium"
            if rows_in >= ROWGROUP_ROWS
            else "low"
        ),
        "batch_mode",
        f"{_label(node)} tests a predicate on {table}.{', '.join(columns)} "
        + (f"({', '.join(wrapper.replace('function:', '').upper() for wrapper in wrappers)} around the column) " if wrappers else "")
        + f"on the {rows_in:,.0f} rows that the columnstore scan (node {scan.node_id}) returns, and keeps "
        f"{rows_out:,.0f}: the predicate was not pushed into the scan, so the scan cannot skip rowgroups or "
        "filter rows for it.",
        "Compare the bare column (move the function to the constant side, or use a collation that makes it "
        "unnecessary), or store the computed value in a real column at load time and filter on that: a "
        "columnstore index cannot hold a persisted computed column.",
        node_id=node.node_id,
        evidence={
            "table": table,
            "scan_node_id": scan.node_id,
            "columns": columns,
            "wrappers": wrappers,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "measured": measured,
            "predicate": residual_predicate(node),
            "rewrite_pattern": "function_on_column" if wrappers else None,
        },
    )


def _partition_rules(rules: _StatementRules) -> None:
    """A partitioned object read without a partition seek key, although a predicate could have bounded it.

    A bare comparison on the partitioning column always gives a seek key on the
    partition id. So when the catalog column is not given, only a wrapped
    column can be the partitioning column, and the finding says "if".
    """

    for access in rules.accesses:
        node = rules.by_id.get(access.node_id)
        partitions = partition_access(node) if node is not None else None
        if node is None or partitions is None or partitions.seek_key:
            continue
        if partitions.accessed is not None and partitions.accessed <= 1:
            continue
        known = rules.partition_columns.get(f"{access.schema}.{access.table}".lower())
        wrapped = {column: kind for column, kind in access.nonsargable.items() if kind != "leading_wildcard"}
        if known:
            candidates = [column for column in [*wrapped, *access.residual] if column.lower() == known.strip("[]").lower()]
        else:
            candidates = list(wrapped)
        if not candidates:
            continue
        table = clean_text(f"{access.schema}.{access.table}", 200)
        column = clean_text(candidates[0], 128)
        kind = wrapped.get(candidates[0])
        wrapper = (
            "an implicit conversion"
            if kind == "convert_implicit"
            else clean_text(kind.replace("function:", "").upper(), 60)
            if kind
            else None
        )
        read = f"all {partitions.accessed:,} partitions" if partitions.accessed is not None else "every partition"
        rewrite = (
            f"Compare the bare column: for one day use a half-open range ({column} >= @from AND {column} < @to), "
            "with the parameter typed as the column. The access then gets a seek key on the partition id and "
            "reads only the matching partitions."
        )
        if known:
            message = (
                f"{_label(node)} read {read} of {table} with no partition seek key. Its predicate on the "
                f"partitioning column {column} "
                + (f"wraps it in {wrapper}, which prevents partition elimination." if wrapper else "did not bound the partitions.")
            )
            fix = rewrite
        else:
            message = (
                f"{_label(node)} read {read} of {table} with no partition seek key. Its predicate wraps {column} "
                f"in {wrapper}. If {column} is the partitioning column, the wrapper is what prevents partition "
                "elimination."
            )
            fix = f"Check the partitioning column of {table} (get_object_details). If it is {column}: " + rewrite
        rules.add(
            "no_partition_elimination",
            # Without the catalog column this is a lead, not a measured loss.
            _share_severity(rules.impact(access.node_id, access.cost_share), high=0.3, medium=HARM_SHARE_FLOOR) if known else "low",
            "partitioning",
            message,
            fix,
            node_id=node.node_id,
            cost_share=access.cost_share,
            evidence={
                "table": table,
                "partitions_accessed": partitions.accessed,
                "partition_ranges": list(partitions.ranges),
                "candidate_columns": [clean_text(item, 128) for item in candidates],
                "partitioning_column_known": bool(known),
                "partitioning_column": column if known else None,
                "wrapper": kind,
                "rewrite_pattern": "function_on_column" if kind else None,
            },
        )


def _shape_rules(rules: _StatementRules) -> None:
    for access in rules.accesses:
        _predicate_patterns(rules, access)
        _bare_scan(rules, access)
    for node in rules.nodes:
        physical = node.physical_op
        if physical == "Filter":
            _late_filter(rules, node)
        elif physical in {"Table Spool", "Index Spool"}:
            _lazy_spool(rules, node)
        elif physical == "Merge Interval":
            _or_join(rules, node)
        elif physical == "Top":
            _deep_offset(rules, node)
            _top_above_scan(rules, node)
        if _is_distinct(node):
            _distinct_fan_out(rules, node)
        if is_true(node.element.get("IsAdaptive")):
            _adaptive_join(rules, node)


def _variable(operand: ET.Element) -> str | None:
    """The ``@name`` an operand is, when it is nothing else."""

    identifier = operand.find(f"{_Q}Identifier")
    if identifier is None or len(operand) != 1:
        return None
    ref = identifier.find(_COLUMN_REFERENCE)
    name = (ref.get("Column") or "") if ref is not None and not ref.get("Table") else ""
    return name if name.startswith("@") else None


def _is_null(operand: ET.Element) -> bool:
    const = operand.find(f"{_Q}Const")
    return const is not None and (const.get("ConstValue") or "").strip("() ").upper() == "NULL"


def _table_columns(element: ET.Element, access: TableAccess) -> list[str]:
    return [
        unquote(ref.get("Column")) or ""
        for ref in element.iter(_COLUMN_REFERENCE)
        if unquote(ref.get("Table")) == access.table and (unquote(ref.get("Schema")) or access.schema) == access.schema
    ]


def _catch_all_parameters(predicate: ET.Element, access: TableAccess) -> dict[str, list[str]]:
    """Parameters used as ``@p IS NULL OR column = @p`` or ``column = ISNULL(@p, column)``, with their columns."""

    found: dict[str, list[str]] = {}
    for logical in predicate.iter(f"{_Q}Logical"):
        if (logical.get("Operation") or "").upper() != "OR":
            continue
        operands = logical.findall(_SCALAR)
        null_tested: set[str] = set()
        for operand in operands:
            compare = operand.find(f"{_Q}Compare")
            if compare is None or (compare.get("CompareOp") or "").upper() != "IS":
                continue
            sides = compare.findall(_SCALAR)
            if len(sides) == 2:
                for value, other in ((sides[0], sides[1]), (sides[1], sides[0])):
                    name = _variable(value)
                    if name and _is_null(other):
                        null_tested.add(name)
        for operand in operands:
            used = {ref.get("Column") for ref in operand.iter(_COLUMN_REFERENCE) if not ref.get("Table")}
            columns = _table_columns(operand, access)
            for name in sorted(null_tested & used):
                if columns:
                    found.setdefault(name, []).extend(column for column in columns if column not in found.get(name, []))
    for function in predicate.iter(f"{_Q}Intrinsic"):
        if (function.get("FunctionName") or "").lower() != "isnull":
            continue
        names = [ref.get("Column") or "" for ref in function.iter(_COLUMN_REFERENCE) if not ref.get("Table")]
        columns = _table_columns(function, access)
        for name in names:
            if name.startswith("@") and columns:
                found.setdefault(name, []).extend(column for column in columns if column not in found.get(name, []))
    return found


def _or_across_columns(predicate: ET.Element, access: TableAccess) -> list[str]:
    """Columns of an OR whose every branch compares one bare column of the table with a value."""

    for logical in predicate.iter(f"{_Q}Logical"):
        if (logical.get("Operation") or "").upper() != "OR":
            continue
        columns: list[str] = []
        for operand in logical.findall(_SCALAR):
            compare = operand.find(f"{_Q}Compare")
            sides = compare.findall(_SCALAR) if compare is not None else []
            bare = [
                column
                for side in sides
                if len(side) == 1
                and side.find(f"{_Q}Identifier") is not None
                and len(found := _table_columns(side, access)) == 1
                and (column := found[0])
            ]
            valued = sum(1 for side in sides if not _table_columns(side, access))
            if len(sides) != 2 or len(bare) != 1 or valued != 1:
                columns = []
                break
            columns.append(bare[0])
        distinct = list(dict.fromkeys(columns))
        if len(distinct) >= 2:
            return distinct
    return []


def _predicate_patterns(rules: _StatementRules, access: TableAccess) -> None:
    node = rules.by_id.get(access.node_id)
    if node is None:
        return
    holder = node.element.find(f"{_Q}IndexScan")
    if holder is None:
        holder = node.element.find(f"{_Q}TableScan")
    predicate = holder.find(f"{_Q}Predicate") if holder is not None else None
    if predicate is None:
        return
    table = clean_text(f"{access.schema}.{access.table}", 200)
    share = rules.impact(access.node_id, access.cost_share)
    scanned = access.operation in {"scan", "heap_scan"}
    catch_all = _catch_all_parameters(predicate, access)
    if catch_all:
        parameters = [clean_text(name, 128) or "" for name in catch_all]
        columns = [clean_text(column, 128) or "" for column in dict.fromkeys(c for found in catch_all.values() for c in found)]
        rules.add(
            "catch_all_predicate",
            _share_severity(share, high=0.3, medium=HARM_SHARE_FLOOR) if scanned else "low",
            "predicates",
            f"{_label(node)} filters {table} with the catch-all pattern (@p IS NULL OR column = @p) on "
            f"{', '.join(parameters)}: one cached plan must serve every mix of supplied and missing parameters, "
            f"so it cannot seek on {', '.join(columns)}.",
            "Add OPTION (RECOMPILE) when the statement does not run many times a second: the optimizer then "
            "drops the branches of the NULL parameters. Otherwise build the statement with only the supplied "
            "predicates (sp_executesql, still parameterized), or write one statement per common combination.",
            node_id=node.node_id,
            cost_share=access.cost_share,
            evidence={
                "table": table,
                "parameters": parameters,
                "columns": columns,
                "operation": access.operation,
                "rewrite_pattern": "catch_all_optional_parameter",
            },
        )
        return
    columns = [clean_text(column, 128) or "" for column in _or_across_columns(predicate, access)]
    small = 0 < access.table_cardinality < SCAN_TABLE_ROW_FLOOR
    if columns and scanned and share >= HARM_SHARE_FLOOR and not small and not _is_columnstore(access):
        rules.add(
            "or_across_columns",
            _share_severity(share, high=0.3, medium=0.1),
            "predicates",
            f"{_label(node)} scans {table} for a predicate that ORs different columns ({', '.join(columns)}): "
            "one index seek cannot serve an OR across columns.",
            "Rewrite as UNION ALL of one query per column, each excluding the rows an earlier branch returns, "
            "with an index on each column; or index each column and check for an index union plan. Then compare.",
            node_id=node.node_id,
            cost_share=access.cost_share,
            evidence={"table": table, "columns": columns, "rewrite_pattern": "or_across_columns"},
        )


def _bare_scan(rules: _StatementRules, access: TableAccess) -> None:
    """A whole rowstore table read with no predicate (PerformanceMonitor: narrow index up to 3 columns, else columnstore)."""

    node = rules.by_id.get(access.node_id)
    if node is None or access.operation not in {"scan", "heap_scan"} or _is_columnstore(access):
        return
    if (access.index_kind or "").lower() not in {"clustered", "heap"} or access.spool_node_id is not None:
        return
    # An ordered scan supplies an order, and a row goal stops early: neither reads the table for its own sake.
    if access.residual or access.nonsargable or access.ordered or node.estimate_rows_without_row_goal is not None:
        return
    cardinality = node.table_cardinality
    if cardinality < BARE_SCAN_ROW_FLOOR:
        return
    if node.has_runtime:
        if not rules.settled(node) or node.actual_rows < cardinality * 0.5:
            return
    elif node.estimate_rows < cardinality * 0.5:
        return
    share = rules.impact(access.node_id, access.cost_share)
    if share < BARE_SCAN_SHARE_FLOOR:
        return
    table = clean_text(f"{access.schema}.{access.table}", 200)
    columns = [clean_text(column, 128) or "" for column in access.output_columns]
    narrow = len(columns) <= 3
    rules.add(
        "bare_scan",
        # A design lead: the plan cannot show that a narrower index would be read instead.
        "low",
        "indexes",
        f"{_label(node)} reads all {cardinality:,.0f} rows of {table} with no predicate to return "
        + (
            f"{len(columns)} column(s) ({', '.join(columns)}). A nonclustered index on those columns is "
            "narrower to scan than the whole table."
            if columns and narrow
            else "no column (a count or an existence check). Any nonclustered index is narrower to scan "
            "than the whole table."
            if narrow
            else f"{len(columns)} columns. If this is an analytic read (an aggregate or a report over most of "
            "the table), a nonclustered columnstore index reads only the columns used, compressed and in batch mode."
        ),
        "First confirm that the statement needs every row. If it does, benchmark the index on a copy "
        "(benchmark_index_candidate) and weigh its write cost.",
        node_id=node.node_id,
        cost_share=access.cost_share,
        evidence={
            "table": table,
            "table_cardinality": cardinality,
            "output_columns": columns,
            "suggestion": "narrow_nonclustered_index" if narrow else "nonclustered_columnstore_index",
        },
    )


def _late_filter(rules: _StatementRules, node: PlanNode) -> None:
    """A Filter that discards most of the rows a join below it produced.

    A HAVING or a window-function filter reads computed columns only and cannot
    move lower; a startup filter decides whether its subtree runs at all.
    """

    element = node.element.find(f"{_Q}Filter")
    predicate = element.find(f"{_Q}Predicate") if element is not None else None
    if element is None or predicate is None or not node.children or is_true(element.get("StartupExpression")):
        return
    child = node.children[0]
    below = subtree(child)
    if not any("join" in item.logical_op.lower() for item in below):
        return
    if not any(ref.get("Table") for ref in predicate.iter(_COLUMN_REFERENCE)):
        return
    measured = node.has_runtime and child.has_runtime
    if measured and not rules.settled(node):
        return
    rows_in = child.actual_rows if measured else child.estimate_rows
    rows_out = node.actual_rows if measured else node.estimate_rows
    if rows_in < LATE_FILTER_ROW_FLOOR or rows_out > rows_in * LATE_FILTER_KEEP_FRACTION:
        return
    share = rules.share_of(below)
    if share < 0.1:
        return
    null_test = any(
        (compare.get("CompareOp") or "").upper() == "IS" and any(_is_null(side) for side in compare.findall(_SCALAR))
        for compare in predicate.iter(f"{_Q}Compare")
    )
    rules.add(
        "late_filter",
        _share_severity(share, high=0.5, medium=0.1),
        "predicates",
        f"{_label(node)} keeps {rows_out:,.0f} of the {rows_in:,.0f} rows that {_label(child)} produces: "
        f"{residual_predicate(node) or 'predicate not shown'}. The predicate is applied after the join, so the "
        f"work below it ({share:.0%} of the plan) mostly produces rows that are discarded.",
        "Make the predicate apply at a table access: compare bare columns of one table, with no function, no "
        "OR across tables and no expression over columns of both join inputs."
        + (
            " For LEFT JOIN ... WHERE column IS NULL, write NOT EXISTS: the optimizer then uses an anti semi join."
            if null_test
            else ""
        ),
        node_id=node.node_id,
        benefit_nodes=below,
        benefit_basis=f"self time of the subtree under the Filter (node {child.node_id} and below), whose rows it discards",
        evidence={
            "rows_in": rows_in,
            "rows_out": rows_out,
            "measured": measured,
            "subtree_root_node_id": child.node_id,
            "predicate": residual_predicate(node),
            "rewrite_pattern": "left_join_is_null" if null_test else "late_filter",
        },
    )


def _lazy_spool(rules: _StatementRules, node: PlanNode) -> None:
    # A spool with no input reads another spool's rows (PrimaryNodeId).
    if "Lazy" not in node.logical_op or not node.children or not node.has_runtime:
        return
    rebinds, rewinds = node.actual_rebinds, node.actual_rewinds or 0.0
    if rebinds is None or rebinds <= SPOOL_REBIND_FLOOR or rewinds >= SPOOL_REWIND_RATIO * rebinds:
        return
    outer = node.parent.children[0].node_id if node.parent is not None and node.parent.children else None
    rules.add(
        "lazy_spool_ineffective",
        _share_severity(rules.impact(node.node_id, rules.cost_shares.get(node.node_id, 0.0)), high=0.3, medium=HARM_SHARE_FLOOR),
        "joins",
        f"{_label(node)} was reloaded {rebinds:,.0f} times (rebinds) and reused its cached rows {rewinds:,.0f} "
        "times (rewinds): the cache is rarely hit, so the spool mostly adds writes to tempdb.",
        "The outer input rarely repeats the correlated value in sequence. An index on the inner side's "
        "correlated column lets the optimizer drop the spool"
        + (f"; check the outer row estimate (node {outer})." if outer is not None else "."),
        node_id=node.node_id,
        evidence={"rebinds": rebinds, "rewinds": rewinds, "outer_node_id": outer},
    )


def _or_join(rules: _StatementRules, node: PlanNode) -> None:
    """Merge Interval over Constant Scan branches, run for every outer row: an OR in a join or correlation."""

    below = subtree(node)
    branches = max((len(item.children) for item in below if item.physical_op == "Concatenation"), default=0)
    if branches < 2 or sum(1 for item in below if item.physical_op == "Constant Scan") < 2:
        return
    measured = node.has_runtime
    executions = node.actual_executions if measured else node.estimated_executions
    if executions < REPEATED_EXECUTION_FLOOR:
        return
    loop = node.parent if node.parent is not None and node.parent.physical_op == "Nested Loops" else None
    members = subtree(loop) if loop is not None else below
    target = rules.access_by_node.get(loop.children[1].node_id) if loop is not None and len(loop.children) > 1 else None
    table = clean_text(f"{target.schema}.{target.table}", 200) if target is not None else None
    rules.add(
        "join_or_predicate",
        _share_severity(rules.share_of(members), high=0.3, medium=HARM_SHARE_FLOOR),
        "joins",
        f"{_label(node)} builds seek ranges from {branches} Constant Scan branches and ran {executions:,.0f} times"
        + (f" to seek {table}" if table else "")
        + ": an OR (or IN) between join or correlated predicates is expanded into one range per branch for "
        "every outer row.",
        "Rewrite the OR as UNION ALL of one join per branch (add a predicate so the branches do not return "
        "the same row twice), with an index for each branch; then compare the plans.",
        node_id=node.node_id,
        benefit_nodes=members,
        benefit_basis=(
            f"self time of the Nested Loops (node {loop.node_id}) that the ranges drive, and everything below it"
            if loop is not None
            else None
        ),
        evidence={
            "branches": branches,
            "executions": executions,
            "measured": measured,
            "table": table,
            "rewrite_pattern": "or_in_join_predicate",
        },
    )


def _row_count_value(query_plan: ET.Element, scalar: ET.Element) -> float | None:
    """A TOP or OFFSET row count: a constant, or the value of the parameter that holds it."""

    const = scalar.find(f"{_Q}Const")
    text = const.get("ConstValue") if const is not None else None
    name = _variable(scalar)
    if name:
        for parameter in query_plan.findall(f"{_Q}ParameterList/{_COLUMN_REFERENCE}"):
            if parameter.get("Column") == name:
                text = parameter.get("ParameterRuntimeValue") or parameter.get("ParameterCompiledValue")
    match = re.fullmatch(r"\(*(\d+)\)*", (text or "").strip())
    return float(match.group(1)) if match else None


def _deep_offset(rules: _StatementRules, node: PlanNode) -> None:
    top = node.element.find(f"{_Q}Top")
    offset = top.find(f"{_Q}OffsetExpression/{_Q}ScalarOperator") if top is not None else None
    if offset is None or not node.children:
        return
    depth = _row_count_value(rules.query_plan, offset)
    child = node.children[0]
    measured = node.has_runtime and child.has_runtime and rules.settled(node)
    if measured:
        depth = child.actual_rows - node.actual_rows
    if depth is None or depth < DEEP_OFFSET_ROWS:
        return
    below = subtree(node)
    share = rules.share_of(below)
    rules.add(
        "deep_offset_paging",
        "high" if share >= 0.3 and depth >= 10 * DEEP_OFFSET_ROWS else "medium" if share >= HARM_SHARE_FLOOR else "low",
        "paging",
        f"{_label(node)} skips {depth:,.0f} rows before it returns its page (OFFSET): every skipped row is still "
        "read, joined and sorted below it, so each later page costs more.",
        "Page by key, not by position (keyset paging): WHERE the sort key is past the last key of the previous "
        "page, ORDER BY the sort key, TOP (n), with an index on the sort key. Every page then costs the same.",
        node_id=node.node_id,
        benefit_nodes=below,
        benefit_basis=f"self time of node {node.node_id} and everything below it, which produces the skipped rows",
        evidence={"rows_skipped": depth, "measured": measured, "rewrite_pattern": "deep_offset_paging"},
    )


def _top_above_scan(rules: _StatementRules, node: PlanNode) -> None:
    """TOP over a filtered scan, run once per outer row of a nested loop (PerformanceMonitor: critical there)."""

    scan = node.children[0] if node.children else None
    while scan is not None and scan.physical_op == "Compute Scalar" and scan.children:
        scan = scan.children[0]
    access = rules.access_by_node.get(scan.node_id) if scan is not None else None
    if scan is None or access is None or scan.physical_op not in _SCAN_OPERATORS:
        return
    if not (access.residual or access.nonsargable):
        return
    loop, current = None, node
    while current.parent is not None and loop is None:
        parent = current.parent
        if parent.physical_op == "Nested Loops" and len(parent.children) > 1 and parent.children[1] is current:
            loop = parent
        current = parent
    measured = node.has_runtime
    executions = node.actual_executions if measured else node.estimated_executions
    if loop is None or executions < REPEATED_EXECUTION_FLOOR:
        return
    table = clean_text(f"{access.schema}.{access.table}", 200)
    columns = [clean_text(column, 128) or "" for column in dict.fromkeys([*access.residual, *access.nonsargable])]
    rows_read = scan.actual_rows_read if scan.has_runtime else None
    rules.add(
        "top_above_scan",
        _share_severity(rules.share_of([node, scan]), high=0.3, medium=HARM_SHARE_FLOOR),
        "indexes",
        f"{_label(node)} sits on {_label(scan)} of {table} on the inner side of Nested Loops (node {loop.node_id}) "
        f"and ran {executions:,.0f} times: each run scans until it finds its rows"
        + (f" ({rows_read:,.0f} rows read in total)." if rows_read else "."),
        f"Create an index on {table} keyed on {', '.join(columns)}, then the ORDER BY column of the TOP, so each "
        "run seeks and stops (review_workload_indexes designs it against the existing indexes).",
        node_id=node.node_id,
        benefit_nodes=[node, scan],
        benefit_basis=f"self time of the Top (node {node.node_id}) and the scan under it (node {scan.node_id})",
        evidence={
            "table": table,
            "scan_node_id": scan.node_id,
            "loop_node_id": loop.node_id,
            "executions": executions,
            "measured": measured,
            "filter_columns": columns,
            "rows_read": rows_read,
        },
    )


def _is_distinct(node: PlanNode) -> bool:
    """DISTINCT, or GROUP BY with no aggregate: the operator only removes duplicates."""

    if node.logical_op in {"Distinct Sort", "Flow Distinct"}:
        return True
    if node.logical_op == "Aggregate" and node.physical_op in {"Hash Match", "Stream Aggregate"}:
        # Any defined value is something computed over the group (COUNT, SUM, ANY).
        return not any(element.tag == f"{_Q}DefinedValue" for element in node.own_elements)
    return False


def _distinct_fan_out(rules: _StatementRules, node: PlanNode) -> None:
    """A join multiplies one table's rows and a DISTINCT above it removes the copies again."""

    join = node.children[0] if node.children else None
    while join is not None and "join" not in join.logical_op.lower():
        if join.physical_op not in _PASS_THROUGH_OPERATORS | {"Sort"} or not join.children:
            return
        join = join.children[0]
    if join is None or len(join.children) < 2:
        return
    tables = {
        (unquote(ref.get("Schema")), unquote(ref.get("Table")))
        for ref in node.element.iterfind(f"{_Q}OutputList/{_COLUMN_REFERENCE}")
        if ref.get("Table")
    }
    if len(tables) != 1:
        return
    schema, table = next(iter(tables))
    side = next(
        (
            child
            for child in join.children[:2]
            if any(
                (access := rules.access_by_node.get(item.node_id)) is not None
                and access.table == table
                and (schema is None or access.schema == schema)
                for item in subtree(child)
            )
        ),
        None,
    )
    measured = node.has_runtime and join.has_runtime and side is not None and side.has_runtime
    if side is None or (measured and not rules.settled(node)):
        return
    # The inner side's estimate is per execution, so only the outer side compares without counters.
    if not measured and join.physical_op == "Nested Loops" and side is not join.children[0]:
        return
    if measured:
        joined, kept, supplied = join.actual_rows, node.actual_rows, side.actual_rows
    else:
        joined, kept, supplied = join.estimate_rows, node.estimate_rows, side.estimate_rows
    if joined < FAN_OUT_ROW_FLOOR or joined < 10 * max(kept, 1.0) or joined < 2 * max(supplied, 1.0):
        return
    name = clean_text(f"{schema}.{table}" if schema else table, 200)
    rules.add(
        "distinct_over_join_fanout",
        _share_severity(rules.share_of([node, join]), high=0.3, medium=HARM_SHARE_FLOOR) if rules.timed else "low",
        "joins",
        f"{_label(node)} reduces the {joined:,.0f} rows of {_label(join)} to {kept:,.0f} and returns columns of "
        f"{name} only: the join multiplies the {supplied:,.0f} rows of {name}, and the DISTINCT removes the "
        "copies again.",
        "If the other table is only tested for a match, replace the join with EXISTS (a semi join stops at the "
        "first match and returns each row once) and drop the DISTINCT. If values from it are needed, aggregate "
        "it to one row per join key before the join.",
        node_id=node.node_id,
        benefit_nodes=[node, join],
        benefit_basis=f"self time of the duplicate removal (node {node.node_id}) and the join under it (node {join.node_id})",
        evidence={
            "table": name,
            "join_node_id": join.node_id,
            "rows_joined": joined,
            "rows_kept": kept,
            "rows_from_table": supplied,
            "measured": measured,
            "rewrite_pattern": "distinct_over_join_fanout",
        },
    )


def _adaptive_join(rules: _StatementRules, node: PlanNode) -> None:
    planned = clean_text(node.element.get("EstimatedJoinType"), 40) or ""
    ran = next((clean_text(counter.join_type, 40) or "" for counter in node.threads if counter.join_type), "")
    if not planned or not ran or planned.replace(" ", "").lower() == ran.replace(" ", "").lower():
        return
    build = node.children[0].actual_rows if node.children and node.children[0].has_runtime else None
    rules.add(
        "adaptive_join_switched",
        "info",
        "joins",
        f"{_label(node)} was planned as {planned} and ran as {ran}: the build input had "
        f"{build if build is None else format(build, ',.0f')} rows against a threshold of "
        f"{_float(node.element.get('AdaptiveThresholdRows')):,.0f}.",
        "No change needed: the adaptive join chose the cheaper strategy for this run. A large difference from "
        "the estimate still points at the build input's row estimate.",
        node_id=node.node_id,
        evidence={
            "estimated_join_type": planned,
            "actual_join_type": ran,
            "threshold_rows": _float(node.element.get("AdaptiveThresholdRows")),
            "build_rows": build,
        },
    )


def _memory_rules(rules: _StatementRules) -> None:
    grant = rules.query_plan.find(f"{_Q}MemoryGrantInfo")
    if grant is None or not rules.actual or grant.get("GrantedMemory") is None:
        return
    granted = _float(grant.get("GrantedMemory"))
    used = _float(grant.get("MaxUsedMemory")) if grant.get("MaxUsedMemory") is not None else None
    maximum = _float(grant.get("MaxQueryMemory"))
    of_maximum = granted / maximum if maximum > 0 else None
    feedback = clean_text(grant.get("IsMemoryGrantFeedbackAdjusted"), 60)
    evidence: dict[str, Any] = {
        "granted_kb": granted,
        "max_used_kb": used,
        "max_query_memory_kb": maximum or None,
        "share_of_max_query_memory": round(of_maximum, 3) if of_maximum is not None else None,
        "grant_feedback": feedback,
        "last_requested_kb": _float(grant.get("LastRequestedMemory")) or None,
    }
    relative = f" ({of_maximum:.0%} of the most one query may get)" if of_maximum is not None else ""
    if used is not None and used < granted * 0.1:
        # A running query has not used its memory yet. A small unused grant starves nobody.
        sizeable = granted >= LARGE_GRANT_KB or (
            of_maximum is not None and of_maximum >= 0.1 and granted >= WASTED_GRANT_FLOOR_KB
        )
        if rules.in_flight or not sizeable:
            return
        rules.add(
            "excessive_memory_grant",
            "medium" if granted >= LARGE_GRANT_KB or (of_maximum or 0.0) >= 0.25 else "low",
            "memory",
            f"The query was granted {granted / 1024:,.0f} MB{relative} but used {used / 1024:,.0f} MB."
            + (
                " Memory grant feedback is already adjusting it: measure again after a few executions."
                if (feedback or "").lower().startswith("yes")
                else " Memory grant feedback is disabled for this plan."
                if "disabled" in (feedback or "").lower()
                else ""
            ),
            "Fix the overestimate (statistics, predicates) so concurrent queries are not starved.",
            evidence=evidence,
        )
        return
    large = granted >= LARGE_GRANT_KB or (
        of_maximum is not None and of_maximum >= 0.5 and granted >= WASTED_GRANT_FLOOR_KB
    )
    if not large:
        return
    consumers = sorted(
        (node for node in rules.nodes if node.physical_op in _MEMORY_CONSUMERS),
        key=lambda node: -_input_bytes(node),
    )[:3]
    names = [f"{_label(node)}, about {_input_bytes(node) / 1048576:,.0f} MB of input" for node in consumers]
    evidence["consumers"] = [
        {"node_id": node.node_id, "operator": node.label, "estimated_input_mb": round(_input_bytes(node) / 1048576, 1)}
        for node in consumers
    ]
    rules.add(
        "large_memory_grant",
        "high" if granted >= CRITICAL_GRANT_KB or (of_maximum or 0.0) >= 0.9 else "medium",
        "memory",
        f"The query was granted {granted / 1024:,.0f} MB of memory{relative}"
        + (f" and used {used / 1024:,.0f} MB" if used is not None else "")
        + (f". Largest consumers by estimated input: {'; '.join(names)}." if names else "."),
        "Give the consumers less to hold: fewer and narrower columns into the sort or hash (no SELECT *, no "
        "(max) columns), an index that supplies the order, or a smaller build input. Queries wait for memory "
        "(RESOURCE_SEMAPHORE) behind a grant of this size.",
        node_id=consumers[0].node_id if consumers else None,
        # Memory is the cost here, not this operator's time.
        benefit_nodes=[],
        evidence=evidence,
    )


def _input_bytes(node: PlanNode) -> float:
    """Estimated bytes a sort or hash holds: its input, or the build input of a join."""

    source = node.children[0] if node.children else node
    return source.estimate_rows * max(1.0, source.estimated_executions) * source.avg_row_size


def _text_rules(rules: _StatementRules) -> None:
    """Rules that read the statement text; a text cut by the showplan can hide what they look for."""

    statement = rules.statement
    text = statement.get("StatementText") or ""
    hint = _OPTIMIZE_FOR_UNKNOWN.search(text)
    if hint:
        rules.add(
            "optimize_for_unknown",
            "info",
            "cardinality",
            "The statement uses OPTIMIZE FOR UNKNOWN: the optimizer estimates from average density, not from "
            "the parameter values, so every value gets the same plan.",
            "Keep it only if the values are skewed and no single plan suits them; check with "
            "get_query_parameter_buckets. For a few very different cases, measure OPTION (RECOMPILE) or "
            "OPTIMIZE FOR a typical value.",
            evidence={"hint": clean_text(hint.group(0), 80)},
        )
    if len(text) >= TRUNCATED_TEXT_CHARS:
        rules.add(
            "truncated_statement_text",
            "info",
            "compile",
            f"The plan holds only the first {len(text):,} characters of the statement text, so checks that read "
            "the text (hints, cursor options, OPTIMIZE FOR) can miss what follows.",
            "Read the full text from Query Store (sys.query_store_query_text) before judging hints or options.",
            evidence={"statement_text_chars": len(text)},
        )
    if statement.tag != f"{_Q}StmtCursor":
        return
    cursor = statement.find(f"{_Q}CursorPlan")
    actual_type = clean_text(cursor.get("CursorActualType"), 40) if cursor is not None else None
    if (actual_type or "").lower() == "dynamic":
        rules.add(
            "dynamic_cursor",
            "low",
            "cursor",
            "The cursor is DYNAMIC: every fetch runs the query again to see changes made since the last one.",
            "Replace the loop with a set-based statement. If a cursor is required, declare it LOCAL FAST_FORWARD "
            "(or STATIC READ_ONLY when the result is read more than once).",
            evidence={
                "cursor_actual_type": actual_type,
                "cursor_requested_type": clean_text(cursor.get("CursorRequestedType"), 40) if cursor is not None else None,
            },
        )
    declared = _DECLARE_CURSOR.search(text)
    if declared and not re.search(r"\bLOCAL\b", declared.group("options"), re.IGNORECASE):
        rules.add(
            "cursor_without_local",
            "info",
            "cursor",
            "The cursor is declared without LOCAL. Unless the database option CURSOR_DEFAULT is LOCAL (not in "
            "the plan), it is global to the connection: it outlives the batch and its name can collide.",
            "Declare the cursor LOCAL.",
            evidence={"declaration": clean_text(declared.group(0), 120)},
        )


def _unsniffed_variables(rules: _StatementRules, node: PlanNode) -> list[str]:
    """Variables in the operator's predicates whose value the optimizer did not have at compile time."""

    listed = {
        parameter.get("Column"): parameter
        for parameter in rules.query_plan.findall(f"{_Q}ParameterList/{_COLUMN_REFERENCE}")
    }
    return [
        clean_text(name, 128) or ""
        for name in predicate_variables(node)
        if name not in listed or listed[name].get("ParameterCompiledValue") is None
    ]


def _gap_rows(node: PlanNode) -> float:
    check = estimate_check(node)
    return max(node.actual_rows, check.estimated_per_execution * max(1.0, check.executions)) if check else 0.0


def _estimate_rules(rules: _StatementRules) -> None:
    """One finding per misestimate, at the lowest operator where it first appears."""

    origins = estimate_origins(rules.nodes, factor_floor=ESTIMATE_GAP_FACTOR)
    by_identity = {id(node): node for node in rules.nodes}
    inherited: dict[int, list[PlanNode]] = {}
    for key, origin in origins.items():
        if origin.propagated:
            inherited.setdefault(origin.origin_node_id, []).append(by_identity[key])
    statistics = statistics_used(rules.query_plan)
    for node in rules.nodes:
        origin = origins.get(id(node))
        check = estimate_check(node)
        if origin is None or origin.propagated or check is None:
            continue
        # A running operator has not produced its rows yet; an underestimate only grows.
        if check.direction == "over" and not rules.settled(node):
            continue
        followers = inherited.get(node.node_id, [])
        if not any(_gap_rows(member) >= ESTIMATE_GAP_ROW_FLOOR for member in [node, *followers]):
            continue
        actual_rows = node.actual_rows
        guess = ce_guess_fraction(node)
        evidence: dict[str, Any] = {
            "estimated_rows_per_execution": round(check.estimated_per_execution, 3),
            "actual_rows_per_execution": round(check.actual_per_execution, 3),
            "executions": check.executions,
            "estimated_executions": node.estimated_executions,
            "actual_rows": actual_rows,
            "direction": check.direction,
            "factor": round(check.factor, 1),
            "propagated_node_ids": sorted(follower.node_id for follower in followers),
        }
        if origin.amplifies_node_id is not None:
            evidence["amplifies_node_id"] = origin.amplifies_node_id
        tables = {
            (unquote(element.get("Schema")), unquote(element.get("Table")))
            for element in node.own_elements
            if element.tag == f"{_Q}Object" and element.get("Table")
        }
        used = [
            {
                "statistics": item.statistics,
                "modification_count": item.modification_count,
                "sampling_percent": item.sampling_percent,
                "last_update": item.last_update,
            }
            for item in statistics
            if (item.schema, item.table) in tables
        ][:5]
        if used:
            evidence["statistics"] = used
        gap = (
            f"estimated {check.estimated_per_execution:,.0f} rows per execution but produced "
            f"{check.actual_per_execution:,.0f} ({check.direction}estimated {check.factor:,.0f}x over "
            f"{check.executions:,.0f} execution(s))"
        )
        suffix = ""
        if guess is not None:
            evidence["estimate_matches_fixed_guess"] = round(guess, 4)
            suffix += (
                f" The estimate is {guess:.1%} of the table, a fixed-guess fraction: the "
                "optimizer had no usable statistics for this predicate."
            )
        if followers:
            suffix += (
                f" The same error carries up through node(s) {', '.join(str(item) for item in evidence['propagated_node_ids'])}"
                ": fix it here."
            )
        access = rules.access_by_node.get(node.node_id)
        variables = _unsniffed_variables(rules, node) if access is not None else []
        share = rules.impact(node.node_id, rules.cost_shares.get(node.node_id, 0.0))
        severity = "high" if check.factor >= 100 else "medium"
        without_goal = node.estimate_rows_without_row_goal
        if without_goal is not None and without_goal > node.estimate_rows and check.direction == "under":
            evidence["estimated_rows_without_row_goal"] = without_goal
            rules.add(
                "row_goal_misestimate",
                severity,
                "row_goal",
                f"{_label(node)} was planned under a row goal: the estimate was cut from {without_goal:,.0f} to "
                f"{node.estimate_rows:,.0f} rows because the optimizer expected to stop early, but it {gap}."
                + suffix,
                "The plan was chosen to return its first rows fast, and they did not come early. Measure OPTION "
                "(USE HINT('DISABLE_OPTIMIZER_ROWGOAL')) (keep it with a Query Store hint if it helps), or give "
                "the query an index that returns the wanted rows in order.",
                node_id=node.node_id,
                evidence=evidence,
            )
        elif variables:
            evidence["local_variables"] = variables
            evidence["rewrite_pattern"] = "local_variable_predicate"
            table = clean_text(f"{access.schema}.{access.table}", 200) if access is not None else None
            rules.add(
                "local_variable_estimate",
                severity,
                "cardinality",
                f"{_label(node)} filters {table} with {', '.join(variables)}, whose value the optimizer did not "
                f"have at compile time (a local variable, OPTIMIZE FOR UNKNOWN, or parameter sniffing off): it {gap}."
                + suffix,
                "Pass the value as a parameter (sp_executesql or a procedure parameter) so the optimizer reads "
                "the histogram for it, or add OPTION (RECOMPILE) to this statement if it runs rarely.",
                node_id=node.node_id,
                evidence=evidence,
            )
        elif (
            access is not None
            and access.operation in {"scan", "heap_scan"}
            and check.direction == "over"
            and share >= SCAN_MISESTIMATE_SHARE
            and node.table_cardinality > 0
            and check.actual_per_execution < node.table_cardinality * SCAN_MISESTIMATE_SELECTIVITY
        ):
            table = clean_text(f"{access.schema}.{access.table}", 200)
            evidence["table_cardinality"] = node.table_cardinality
            evidence["selectivity"] = round(check.actual_per_execution / node.table_cardinality, 6)
            rules.add(
                "scan_cardinality_misestimate",
                "high",
                "cardinality",
                f"{_label(node)} on {table} {gap}: {check.actual_per_execution / node.table_cardinality:.2%} of "
                f"the table. With the true count a seek would be far cheaper than this scan, which is "
                f"{share:.0%} of the plan." + suffix,
                "Fix the estimate first: update the statistics on the filter columns (check_statistics_health) "
                "and compare the plan. If it still scans, design an index on the filter columns with "
                "review_workload_indexes.",
                node_id=node.node_id,
                evidence=evidence,
            )
        else:
            rules.add(
                "row_estimate_gap",
                severity,
                "cardinality",
                f"{node.label} {gap}." + suffix,
                "Find why the estimate is wrong (stale statistics, correlated predicates, parameter "
                "sensitivity, table variables, local variables) before changing indexes.",
                node_id=node.node_id,
                evidence=evidence,
            )


def _spill_occurred(rules: _StatementRules) -> None:
    """A last actual plan records only that a spill happened, not where (lightweight profiling)."""

    if rules.query_plan.find(f"{_Q}Warnings/{_Q}SpillOccurred") is None:
        return
    if any(finding.rule == "tempdb_spill" for finding in rules.findings):
        return
    candidates = [node for node in rules.nodes if node.physical_op in _MEMORY_CONSUMERS]
    rules.add(
        "tempdb_spill",
        "medium",
        "memory",
        "A sort or hash spilled to tempdb during this execution. This plan source does not record which "
        "operator or how deep"
        + (f"; the candidates are {', '.join(_label(node) for node in candidates[:5])}." if candidates else "."),
        "Capture an actual plan with explain_query (analyze=true) to see the operator and the spill level, then "
        "fix the row estimate that sized its memory grant.",
        evidence={"operator_known": False, "candidate_node_ids": [node.node_id for node in candidates[:10]]},
    )
