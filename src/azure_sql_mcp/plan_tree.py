"""Showplan operator tree with self-time attribution.

The timing rules follow Erik Darling's ``sqlserver-query-plans`` plugin
(github.com/erikdarlingdata/claude-plugins, ``extract.py``), MIT License,
Copyright (c) 2026 Erik Darling; see NOTICE.

Row mode reports elapsed and CPU time cumulatively: an operator's numbers
include its whole subtree, so ranking raw times always crowns the root. Batch
mode reports each operator on its own. Exchange operators accumulate time spent
waiting on other operators, and thread 0 of a parallel plan is the
coordinator, whose elapsed time is the wall clock of the whole branch. Self
time is therefore computed per thread, never across threads, and an exchange's
self time is advisory.

Cost is an estimate in every plan, including actual plans. Every string read
from a plan is untrusted: ``clean_text`` flattens it to one bounded line.
"""

from __future__ import annotations

import math
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from dataclasses import field

NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"
_Q = f"{{{NS}}}"
_RELOP = f"{_Q}RelOp"

MAX_PLAN_CHARS = 64 * 1024 * 1024
MAX_PLAN_DEPTH = 1_000
EXCHANGE_LOGICAL_OPS = frozenset({"Gather Streams", "Distribute Streams", "Repartition Streams"})
# Selectivities the optimizer falls back on when it has no usable statistics.
# Which predicate yields which fraction depends on the cardinality estimator
# version, so report the fingerprint and never name the guess.
CE_GUESS_BANDS = ((0.29, 0.31), (0.155, 0.175), (0.098, 0.102), (0.088, 0.092), (0.009, 0.011))

_LINE_BREAKS = (0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x85, 0x2028, 0x2029)
_TEXT_TRANSLATION: dict[int, str | None] = {code: None for code in range(0x20)}
_TEXT_TRANSLATION[0x7F] = None
_TEXT_TRANSLATION.update({code: None for code in range(0x80, 0xA0)})
# Bidirectional overrides can reorder what a reader sees.
_TEXT_TRANSLATION.update({code: None for code in (*range(0x202A, 0x202F), *range(0x2066, 0x206A))})
_TEXT_TRANSLATION.update({code: " " for code in _LINE_BREAKS})
_SCAN_TYPES = {"EQ": "=", "GE": ">=", "GT": ">", "LE": "<=", "LT": "<", "NE": "<>"}


class PlanParseError(ValueError):
    """The input is not a usable showplan. ``str(error)`` is a stable reason."""


def unquote(identifier: str | None) -> str | None:
    """Turn a showplan identifier such as ``[dbo]`` into ``dbo``."""

    if identifier is None:
        return None
    value = identifier.strip()
    if value.startswith("[") and value.endswith("]") and len(value) >= 2:
        return value[1:-1].replace("]]", "]")
    return value


def clean_text(value: object, limit: int | None = 300) -> str | None:
    """Flatten a plan-derived string to one bounded line.

    XML character references survive attribute parsing, so a crafted plan can
    carry newlines or control characters in object names and predicates.
    """

    if value is None:
        return None
    text = " ".join(str(value).translate(_TEXT_TRANSLATION).split())
    if limit is not None and len(text) > limit:
        text = text[: max(0, limit - 3)].rstrip() + "..."
    return text


def parse_showplan(plan_xml: str | None) -> ET.Element:
    """Parse showplan XML from Query Store, a DMV, or a caller, refusing hostile input."""

    text = plan_xml or ""
    if len(text) > MAX_PLAN_CHARS:
        raise PlanParseError("plan_too_large")
    text = text.lstrip("﻿").strip()
    if not text:
        raise PlanParseError("plan_unavailable")
    if text.startswith("<?xml"):
        # The prolog may claim utf-16 for text that is already decoded.
        end = text.find("?>")
        text = text[end + 2 :].lstrip() if end >= 0 else text
    if "<!DOCTYPE" in text or "<!ENTITY" in text:
        raise PlanParseError("plan_rejected: showplan XML never declares a DTD or entities")
    try:
        return ET.fromstring(text)
    except ET.ParseError as exc:
        raise PlanParseError(f"plan_unparseable: {exc}") from exc


@dataclass(frozen=True)
class ThreadCounters:
    thread: int
    rows: float
    executions: float
    rows_read: float | None
    elapsed_ms: float | None
    cpu_ms: float | None
    logical_reads: float | None
    mode: str


@dataclass(eq=False)
class PlanNode:
    element: ET.Element
    node_id: int
    physical_op: str
    logical_op: str
    depth: int
    parent: PlanNode | None
    estimate_rows: float
    estimate_rebinds: float
    estimate_rewinds: float
    subtree_cost: float
    table_cardinality: float
    estimate_rows_without_row_goal: float | None
    avg_row_size: float
    parallel: bool
    estimated_mode: str
    threads: tuple[ThreadCounters, ...]
    children: list[PlanNode] = field(default_factory=list)
    self_elapsed_ms: float | None = None
    self_cpu_ms: float | None = None
    _own_elements: list[ET.Element] | None = field(default=None, repr=False)

    @property
    def has_runtime(self) -> bool:
        return bool(self.threads)

    @property
    def has_timing(self) -> bool:
        return any(counter.elapsed_ms is not None for counter in self.threads)

    @property
    def work_threads(self) -> tuple[ThreadCounters, ...]:
        """Threads that did row work; thread 0 is the coordinator when others exist."""

        workers = tuple(counter for counter in self.threads if counter.thread > 0)
        return workers or self.threads

    @property
    def actual_rows(self) -> float:
        return sum(counter.rows for counter in self.threads)

    @property
    def actual_executions(self) -> float:
        return sum(counter.executions for counter in self.threads)

    @property
    def actual_rows_read(self) -> float | None:
        values = [counter.rows_read for counter in self.threads if counter.rows_read is not None]
        return sum(values) if values else None

    @property
    def logical_reads(self) -> float | None:
        values = [counter.logical_reads for counter in self.threads if counter.logical_reads is not None]
        return sum(values) if values else None

    @property
    def elapsed_ms(self) -> float:
        """Cumulative in row mode; the slowest worker thread."""

        return max((counter.elapsed_ms or 0.0 for counter in self.work_threads), default=0.0)

    @property
    def cpu_ms(self) -> float:
        """Cumulative in row mode; summed over threads."""

        return sum(counter.cpu_ms or 0.0 for counter in self.threads)

    @property
    def mode(self) -> str:
        actual = next((counter.mode for counter in self.threads if counter.mode), "")
        return actual or self.estimated_mode

    @property
    def is_parallelism(self) -> bool:
        return self.physical_op == "Parallelism"

    @property
    def is_exchange(self) -> bool:
        return self.is_parallelism or self.logical_op in EXCHANGE_LOGICAL_OPS

    @property
    def label(self) -> str:
        if self.logical_op and self.logical_op != self.physical_op:
            return f"{self.physical_op} ({self.logical_op})"
        return self.physical_op

    @property
    def row_goal(self) -> bool:
        """A row goal (TOP, FAST n, EXISTS) reduced the estimate."""

        return self.estimate_rows_without_row_goal is not None

    @property
    def estimated_executions(self) -> float:
        return 1.0 + self.estimate_rebinds + self.estimate_rewinds

    @property
    def logical_executions(self) -> float | None:
        """Executions comparable with the per-execution ``EstimateRows``.

        A parallel operator that each worker ran once is one logical execution:
        its estimate covers all threads, so dividing by the thread count would
        invent an overestimate.
        """

        if not self.threads:
            return None
        workers = self.work_threads
        if len(workers) > 1 and all(counter.executions <= 1 for counter in workers):
            return 1.0 if any(counter.executions > 0 for counter in workers) else 0.0
        return self.actual_executions

    @property
    def own_cost(self) -> float:
        """Estimated cost of this operator alone. Still an estimate, in every plan."""

        return max(0.0, self.subtree_cost - sum(child.subtree_cost for child in self.children))

    @property
    def own_elements(self) -> list[ET.Element]:
        """Descendant elements that belong to this operator, in document order."""

        if self._own_elements is None:
            found: list[ET.Element] = []
            stack = list(reversed(list(self.element)))
            while stack:
                element = stack.pop()
                if element.tag == _RELOP:
                    continue
                found.append(element)
                stack.extend(reversed(list(element)))
            self._own_elements = found
        return self._own_elements


@dataclass(frozen=True)
class EstimateCheck:
    estimated_per_execution: float
    actual_per_execution: float
    executions: float
    factor: float
    direction: str


@dataclass(frozen=True)
class StatementTimes:
    elapsed_ms: float | None
    cpu_ms: float | None
    udf_elapsed_ms: float | None
    udf_cpu_ms: float | None


@dataclass(frozen=True)
class CrossJoinCheck:
    """Why a join carries a NoJoinPredicate warning, from its inputs.

    ``verdict`` is one of: ``correlated`` (outer references feed the inner
    side), ``implied_predicate`` (both inputs pinned to the same value, so the
    optimizer dropped a redundant join predicate), ``no_multiplication`` (an
    input has at most one row), ``multiplied`` (output is the product of the
    inputs), ``not_multiplied``, or ``possible_cross_join`` (estimated plan).
    """

    verdict: str
    outer_references: tuple[str, ...]
    shared_values: tuple[str, ...]
    outer_rows: float | None
    inner_rows: float | None
    output_rows: float | None
    measured: bool


def statements(root: ET.Element) -> list[tuple[ET.Element, ET.Element | None]]:
    """Every ``StmtSimple`` with its ``QueryPlan`` (None when the statement has none)."""

    return [(statement, statement.find(f"{_Q}QueryPlan")) for statement in root.iter(f"{_Q}StmtSimple")]


def build_tree(query_plan: ET.Element | None) -> list[PlanNode]:
    """Operators of one statement in document order, with self times attributed."""

    if query_plan is None:
        return []
    root_element = query_plan.find(_RELOP)
    if root_element is None:
        return []
    root = _node(root_element, None, 0)
    nodes: list[PlanNode] = []
    stack = [root]
    while stack:
        node = stack.pop()
        if node.depth > MAX_PLAN_DEPTH:
            raise PlanParseError("plan_too_deep")
        nodes.append(node)
        node.children = [_node(child, node, node.depth + 1) for child in child_relops(node.element)]
        stack.extend(reversed(node.children))
    _attribute_self_times(nodes)
    return nodes


def child_relops(relop: ET.Element) -> list[ET.Element]:
    """Direct child operators in document order: the first child of a join is its outer input."""

    found: list[ET.Element] = []
    stack = list(reversed(list(relop)))
    while stack:
        element = stack.pop()
        if element.tag == _RELOP:
            found.append(element)
            continue
        stack.extend(reversed(list(element)))
    return found


def query_time_stats(query_plan: ET.Element | None) -> StatementTimes | None:
    stats = query_plan.find(f"{_Q}QueryTimeStats") if query_plan is not None else None
    if stats is None:
        return None
    return StatementTimes(
        elapsed_ms=_number(stats.get("ElapsedTime")),
        cpu_ms=_number(stats.get("CpuTime")),
        udf_elapsed_ms=_number(stats.get("UdfElapsedTime")),
        udf_cpu_ms=_number(stats.get("UdfCpuTime")),
    )


def statement_elapsed_ms(query_plan: ET.Element | None, nodes: list[PlanNode]) -> float | None:
    """Wall clock for the statement: QueryTimeStats, else the root's cumulative elapsed."""

    times = query_time_stats(query_plan)
    if times is not None and times.elapsed_ms:
        return times.elapsed_ms
    if nodes and nodes[0].has_timing and nodes[0].elapsed_ms > 0:
        return nodes[0].elapsed_ms
    return None


def estimate_check(node: PlanNode) -> EstimateCheck | None:
    """Compare rows per execution with ``EstimateRows``, which is per execution.

    ``ActualRows`` is a total over every execution and thread, so comparing it
    with the estimate makes the inner side of every nested loop look wrong.
    """

    if not node.has_runtime or node.is_exchange:
        return None
    executions = node.logical_executions
    if not executions:
        return None
    actual = node.actual_rows / executions
    estimated = node.estimate_rows
    return EstimateCheck(
        estimated_per_execution=estimated,
        actual_per_execution=actual,
        executions=executions,
        factor=max(actual, estimated) / max(1.0, min(actual, estimated)),
        direction="under" if actual > estimated else "over",
    )


def ce_guess_fraction(node: PlanNode) -> float | None:
    """The estimate's fraction of the table when it lands on a fixed-guess band."""

    if node.table_cardinality <= 0:
        return None
    fraction = node.estimate_rows / node.table_cardinality
    if any(low <= fraction <= high for low, high in CE_GUESS_BANDS):
        return fraction
    return None


def is_eager_index_spool(node: PlanNode) -> bool:
    """Only an Index Spool; an eager Table Spool is Halloween protection in DML plans."""

    return node.physical_op == "Index Spool" and "Eager" in node.logical_op


def cross_join_check(node: PlanNode) -> CrossJoinCheck:
    """Triage a NoJoinPredicate warning: correlated, implied, or a real cross join.

    A nested loop's inner ``ActualRows`` is already a total over every
    execution, so its rows per execution are what multiply the outer input.
    """

    references = tuple(outer_references(node))
    if references or len(node.children) < 2:
        return CrossJoinCheck("correlated" if references else "possible_cross_join", references, (), None, None, None, False)
    outer, inner = node.children[0], node.children[1]
    shared = tuple(sorted(_pinned_values(outer) & _pinned_values(inner)))[:5]
    measured = node.has_runtime and outer.has_runtime and inner.has_runtime
    nested = node.physical_op == "Nested Loops"
    if measured:
        outer_rows = outer.actual_rows
        executions = inner.logical_executions or 0.0
        inner_rows = (inner.actual_rows / executions if executions else 0.0) if nested else inner.actual_rows
        output_rows: float | None = node.actual_rows
    else:
        outer_rows, inner_rows, output_rows = outer.estimate_rows, inner.estimate_rows, node.estimate_rows
    if shared:
        verdict = "implied_predicate"
    elif min(outer_rows, inner_rows) <= 1:
        verdict = "no_multiplication"
    elif not measured:
        verdict = "possible_cross_join"
    else:
        verdict = "multiplied" if (output_rows or 0.0) >= outer_rows * inner_rows / 2 else "not_multiplied"
    return CrossJoinCheck(verdict, references, shared, outer_rows, inner_rows, output_rows, measured)


def node_objects(node: PlanNode) -> list[str]:
    labels: list[str] = []
    for element in node.own_elements:
        if element.tag != f"{_Q}Object":
            continue
        name = ".".join(part for part in (unquote(element.get("Schema")), unquote(element.get("Table"))) if part)
        if not name:
            continue
        index = unquote(element.get("Index"))
        alias = unquote(element.get("Alias"))
        label = name + (f".{index}" if index else "") + (f" AS {alias}" if alias else "")
        labels.append(clean_text(label, 200) or "")
    return list(dict.fromkeys(labels))


def seek_predicates(node: PlanNode, *, limit: int = 8) -> list[str]:
    """``column op expression`` for each seek key, new and legacy formats."""

    found: list[str] = []
    for element in node.own_elements:
        local = _local(element.tag)
        if local not in {"Prefix", "StartRange", "EndRange"}:
            continue
        columns = element.find(f"{_Q}RangeColumns")
        expressions = element.find(f"{_Q}RangeExpressions")
        names = [_column_label(ref) for ref in columns.findall(f"{_Q}ColumnReference")] if columns is not None else []
        values = (
            [scalar.get("ScalarString") or "" for scalar in expressions.findall(f"{_Q}ScalarOperator")]
            if expressions is not None
            else []
        )
        if not names:
            continue
        scan_type = element.get("ScanType") or "EQ"
        operator = _SCAN_TYPES.get(scan_type, scan_type)
        found.append(clean_text(f"{local}: {', '.join(names)} {operator} {', '.join(values)}".strip(), 300) or "")
        if len(found) >= limit:
            break
    return found


def residual_predicate(node: PlanNode) -> str | None:
    for wanted in ("Predicate", "ProbeResidual", "Residual"):
        for element in node.own_elements:
            if _local(element.tag) != wanted:
                continue
            scalar = element.find(f"{_Q}ScalarOperator")
            if scalar is not None and scalar.get("ScalarString"):
                return clean_text(scalar.get("ScalarString"), 300)
    return None


def outer_references(node: PlanNode) -> list[str]:
    for element in node.own_elements:
        if element.tag == f"{_Q}OuterReferences":
            return [_column_label(ref) for ref in element.findall(f"{_Q}ColumnReference")]
    return []


def output_columns(node: PlanNode, *, limit: int = 40) -> list[str]:
    output = node.element.find(f"{_Q}OutputList")
    if output is None:
        return []
    return [_column_label(ref) for ref in output.findall(f"{_Q}ColumnReference")][:limit]


def scan_order(node: PlanNode) -> str | None:
    for element in node.own_elements:
        ordered = element.get("Ordered")
        if ordered is None:
            continue
        direction = element.get("ScanDirection") or ""
        text = "ordered" if ordered in {"1", "true"} else "unordered"
        return f"{text} {direction.lower()}".strip()
    return None


def warning_texts(element: ET.Element | None) -> list[str]:
    """What SQL Server already said, from one ``Warnings`` element."""

    warnings = element.find(f"{_Q}Warnings") if element is not None else None
    if warnings is None:
        return []
    found: list[str] = []
    if warnings.get("NoJoinPredicate") in {"1", "true"}:
        found.append("No join predicate (often benign: check outer references and both inputs)")
    for attribute, text in (
        ("SpatialGuess", "Spatial index selectivity guessed"),
        ("UnmatchedIndexes", "Filtered index not matched because of parameterization"),
        ("FullUpdateForOnlineIndexBuild", "Full update for online index build"),
    ):
        if warnings.get(attribute) in {"1", "true"}:
            found.append(text)
    for convert in warnings.findall(f"{_Q}PlanAffectingConvert"):
        found.append(f"Implicit conversion [{convert.get('ConvertIssue') or '?'}]: {convert.get('Expression') or ''}")
    spill = warnings.find(f"{_Q}SpillToTempDb")
    level = spill.get("SpillLevel") if spill is not None else None
    spilled_threads = spill.get("SpilledThreadCount") if spill is not None else None
    details = warnings.findall(f"{_Q}SortSpillDetails") + warnings.findall(f"{_Q}HashSpillDetails")
    for detail in details:
        kind = "Sort" if detail.tag.endswith("SortSpillDetails") else "Hash"
        prefix = f"{kind} spill" + (f" level {level}, {spilled_threads or '?'} thread(s)" if level else "")
        found.append(
            f"{prefix}: granted {_number(detail.get('GrantedMemoryKb')) or 0:,.0f} KB, "
            f"used {_number(detail.get('UsedMemoryKb')) or 0:,.0f} KB, "
            f"{_number(detail.get('WritesToTempDb')) or 0:,.0f} writes, "
            f"{_number(detail.get('ReadsFromTempDb')) or 0:,.0f} reads"
        )
    if spill is not None and not details:
        found.append(f"Spill to tempdb, level {level or '?'}, {spilled_threads or '?'} thread(s)")
    for detail in warnings.findall(f"{_Q}ExchangeSpillDetails"):
        found.append(f"Exchange spill: {_number(detail.get('WritesToTempDb')) or 0:,.0f} writes to tempdb")
    if warnings.find(f"{_Q}SpillOccurred") is not None:
        found.append("Spill occurred during execution")
    grant = warnings.find(f"{_Q}MemoryGrantWarning")
    if grant is not None:
        found.append(
            f"Memory grant [{grant.get('GrantWarningKind') or '?'}]: "
            f"requested {(_number(grant.get('RequestedMemory')) or 0) / 1024:,.0f} MB, "
            f"granted {(_number(grant.get('GrantedMemory')) or 0) / 1024:,.0f} MB, "
            f"used {(_number(grant.get('MaxUsedMemory')) or 0) / 1024:,.0f} MB"
        )
    for name, text in (
        ("ColumnsWithNoStatistics", "No statistics on"),
        ("ColumnsWithStaleStatistics", "Stale statistics on"),
    ):
        columns = warnings.find(f"{_Q}{name}")
        if columns is not None:
            names = [_column_label(ref) for ref in columns.findall(f"{_Q}ColumnReference")]
            found.append(f"{text}: {', '.join(filter(None, names))}")
    for wait in warnings.findall(f"{_Q}Wait"):
        found.append(f"Wait {wait.get('WaitType') or '?'}: {wait.get('WaitTime') or '?'} ms")
    return [text for item in found if (text := clean_text(item, 400))]


def _node(element: ET.Element, parent: PlanNode | None, depth: int) -> PlanNode:
    threads: list[ThreadCounters] = []
    runtime = element.find(f"{_Q}RunTimeInformation")
    if runtime is not None:
        for counter in runtime.findall(f"{_Q}RunTimeCountersPerThread"):
            threads.append(
                ThreadCounters(
                    thread=int(_number(counter.get("Thread")) or 0),
                    rows=_number(counter.get("ActualRows")) or 0.0,
                    executions=_number(counter.get("ActualExecutions")) or 0.0,
                    rows_read=_number(counter.get("ActualRowsRead")),
                    elapsed_ms=_number(counter.get("ActualElapsedms")),
                    cpu_ms=_number(counter.get("ActualCPUms")),
                    logical_reads=_number(counter.get("ActualLogicalReads")),
                    mode=counter.get("ActualExecutionMode") or "",
                )
            )
    return PlanNode(
        element=element,
        node_id=int(_number(element.get("NodeId")) or 0),
        physical_op=clean_text(element.get("PhysicalOp"), 80) or "",
        logical_op=clean_text(element.get("LogicalOp"), 80) or "",
        depth=depth,
        parent=parent,
        estimate_rows=_number(element.get("EstimateRows")) or 0.0,
        estimate_rebinds=_number(element.get("EstimateRebinds")) or 0.0,
        estimate_rewinds=_number(element.get("EstimateRewinds")) or 0.0,
        subtree_cost=_number(element.get("EstimatedTotalSubtreeCost")) or 0.0,
        table_cardinality=_number(element.get("TableCardinality")) or 0.0,
        estimate_rows_without_row_goal=_number(element.get("EstimateRowsWithoutRowGoal")),
        avg_row_size=_number(element.get("AvgRowSize")) or 0.0,
        parallel=element.get("Parallel") in {"1", "true"},
        estimated_mode=element.get("EstimatedExecutionMode") or "",
        threads=tuple(threads),
    )


def _attribute_self_times(nodes: list[PlanNode]) -> None:
    """Fill ``self_elapsed_ms`` and ``self_cpu_ms``; children are visited before parents."""

    if not any(node.has_timing for node in nodes):
        return
    contribution: dict[str, dict[PlanNode, float]] = {"elapsed": {}, "cpu": {}}
    batch_total: dict[str, dict[PlanNode, float]] = {"elapsed": {}, "cpu": {}}
    by_thread: dict[str, dict[PlanNode, dict[int, float]]] = {"elapsed": {}, "cpu": {}}
    batch_by_thread: dict[str, dict[PlanNode, dict[int, float]]] = {"elapsed": {}, "cpu": {}}
    for node in reversed(nodes):
        for key in ("elapsed", "cpu"):
            batch_total[key][node] = _value(node, key) + sum(
                batch_total[key][child] if _batch(child) else contribution[key][child]
                for child in node.children
                if not child.is_parallelism
            )
            contribution[key][node] = _contribution(node, key, contribution[key], batch_total[key])
            batch_by_thread[key][node] = _batch_threads(node, key, by_thread[key], batch_by_thread[key])
            by_thread[key][node] = _child_threads(node, key, by_thread[key], batch_by_thread[key])
    for node in nodes:
        node.self_elapsed_ms = _self_elapsed(node, contribution["elapsed"], by_thread["elapsed"])
        node.self_cpu_ms = _self_cpu(node, contribution["cpu"], by_thread["cpu"])


def _batch(node: PlanNode) -> bool:
    return node.mode == "Batch" and node.has_runtime


def _value(node: PlanNode, key: str) -> float:
    return node.elapsed_ms if key == "elapsed" else node.cpu_ms


def _thread_value(counter: ThreadCounters, key: str) -> float:
    return (counter.elapsed_ms if key == "elapsed" else counter.cpu_ms) or 0.0


def _contribution(
    node: PlanNode, key: str, contribution: dict[PlanNode, float], batch_total: dict[PlanNode, float]
) -> float:
    """Time this operator adds to its parent's cumulative total."""

    if node.is_parallelism and node.children:
        return max(contribution[child] for child in node.children)
    if _batch(node):
        # Batch operators pipeline, so a contiguous batch zone adds up. CPU
        # follows the same rule as elapsed here.
        return batch_total[node]
    own = _value(node, key)
    if own > 0:
        return own
    # A pass-through operator with no runtime stats (Compute Scalar): look
    # through it to the descendants that have them.
    return sum(contribution[child] for child in node.children)


def _batch_threads(
    node: PlanNode,
    key: str,
    by_thread: dict[PlanNode, dict[int, float]],
    batch_by_thread: dict[PlanNode, dict[int, float]],
) -> dict[int, float]:
    totals = {counter.thread: _thread_value(counter, key) for counter in node.work_threads}
    for child in node.children:
        if child.is_parallelism:
            continue  # zone boundary
        _merge(totals, batch_by_thread[child] if _batch(child) else by_thread[child])
    return totals


def _child_threads(
    node: PlanNode,
    key: str,
    by_thread: dict[PlanNode, dict[int, float]],
    batch_by_thread: dict[PlanNode, dict[int, float]],
) -> dict[int, float]:
    """What this operator contributes to its parent, per thread."""

    if node.is_parallelism and node.children:
        # Exchange times are unreliable: follow the dominant branch.
        busiest = max(node.children, key=lambda child: _value(child, key))
        return dict(by_thread[busiest])
    if _batch(node):
        return dict(batch_by_thread[node])
    if node.has_runtime and _value(node, key) > 0:
        return {counter.thread: _thread_value(counter, key) for counter in node.work_threads}
    totals: dict[int, float] = {}
    for child in node.children:
        _merge(totals, by_thread[child])
    return totals


def _per_thread_self(node: PlanNode, key: str, by_thread: dict[PlanNode, dict[int, float]]) -> float:
    """Subtract within each thread, never across threads, and keep the slowest thread."""

    children: dict[int, float] = {}
    for child in node.children:
        _merge(children, by_thread[child])
    return max(
        (
            max(0.0, _thread_value(counter, key) - children.get(counter.thread, 0.0))
            for counter in node.work_threads
        ),
        default=0.0,
    )


def _self_elapsed(
    node: PlanNode, contribution: dict[PlanNode, float], by_thread: dict[PlanNode, dict[int, float]]
) -> float:
    if not node.has_runtime or node.elapsed_ms <= 0:
        return 0.0
    if node.mode == "Batch":
        return node.elapsed_ms
    if node.is_exchange:
        workers = [counter.elapsed_ms or 0.0 for counter in node.threads if counter.thread > 0]
        if not workers:
            return 0.0
        return max(0.0, max(workers) - sum(contribution[child] for child in node.children))
    if len(node.threads) > 1:
        return _per_thread_self(node, "elapsed", by_thread)
    return max(0.0, node.elapsed_ms - sum(contribution[child] for child in node.children))


def _self_cpu(
    node: PlanNode, contribution: dict[PlanNode, float], by_thread: dict[PlanNode, dict[int, float]]
) -> float:
    if not node.has_runtime or node.cpu_ms <= 0:
        return 0.0
    if node.mode == "Batch":
        return node.cpu_ms
    if len(node.threads) > 1:
        return _per_thread_self(node, "cpu", by_thread)
    return max(0.0, node.cpu_ms - sum(contribution[child] for child in node.children))


def _pinned_values(top: PlanNode) -> set[str]:
    """Constants and variables a subtree's seeks and filters compare against.

    0, 1, NULL and empty strings are left out: two inputs filtering unrelated
    flags on the same trivial literal would otherwise look like an implied join.
    """

    values: set[str] = set()
    stack = [top]
    while stack:
        node = stack.pop()
        stack.extend(node.children)
        for element in node.own_elements:
            if _local(element.tag) not in {"Predicate", "SeekPredicates", "SeekPredicateNew", "SeekPredicate"}:
                continue
            inner = list(element)
            while inner:
                item = inner.pop()
                if item.tag == _RELOP:
                    continue
                inner.extend(list(item))
                if item.tag == f"{_Q}Const":
                    value = (item.get("ConstValue") or "").strip("()")
                    if value and value.upper() not in {"0", "1", "NULL", "''", "N''"}:
                        values.add(clean_text(value, 80) or "")
                elif item.tag == f"{_Q}ColumnReference" and not item.get("Table"):
                    column = item.get("Column") or ""
                    if column.startswith("@"):
                        values.add(clean_text(column, 80) or "")
    values.discard("")
    return values


def _merge(target: dict[int, float], source: dict[int, float]) -> None:
    for thread, value in source.items():
        target[thread] = target.get(thread, 0.0) + value


def _column_label(reference: ET.Element) -> str:
    column = unquote(reference.get("Column")) or ""
    table = unquote(reference.get("Table")) or unquote(reference.get("Alias"))
    return clean_text(f"{table}.{column}" if table else column, 160) or ""


def _local(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def _number(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    return number if math.isfinite(number) else None
