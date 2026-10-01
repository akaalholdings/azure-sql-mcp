"""Rule-based anti-pattern detection over showplan XML (estimated or actual).

Each finding names a stable rule code, a severity, the plan node, the numbers
from the plan that triggered it, the operator's share of the statement's
estimated cost, the optimizer pattern family it belongs to, and a direction for
the fix. Runtime rules (spills, estimate gaps, thread skew, grant waste) apply
only to actual plans. Findings are leads to measure, not measured gains.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from .showplan_access import parse_plan_access
from .showplan_access import unquote

NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"
_Q = f"{{{NS}}}"

SEVERITY_ORDER = {"high": 3, "medium": 2, "low": 1, "info": 0}
LOOKUP_EXECUTION_FLOOR = 100
NESTED_LOOP_EXECUTION_FLOOR = 10_000
ESTIMATE_GAP_FACTOR = 10.0
ESTIMATE_GAP_ROW_FLOOR = 1_000
SCAN_TABLE_ROW_FLOOR = 10_000
EXCESSIVE_GRANT_KB = 1024 * 1024
MAX_FINDINGS = 50


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

    def as_dict(self) -> dict[str, Any]:
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
        }


def analyze_plan(plan_xml: str) -> dict[str, Any]:
    """Run every rule over a showplan and return sorted findings."""

    text = (plan_xml or "").strip()
    if text.startswith("<?xml"):
        text = text[text.find("?>") + 2 :].lstrip()
    if not text:
        return _empty("plan_unavailable")
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        return _empty(f"plan_unparseable: {exc}")

    actual = root.find(f".//{_Q}RunTimeInformation") is not None
    access_summary = parse_plan_access(text)
    findings: list[PlanFinding] = []
    statements_out: list[dict[str, Any]] = []
    for index, statement in enumerate(root.iter(f"{_Q}StmtSimple")):
        accesses = (
            access_summary.statements[index].accesses
            if index < len(access_summary.statements)
            else []
        )
        statement_findings = _statement_findings(index, statement, accesses, actual)
        findings.extend(statement_findings)
        statements_out.append(
            {
                "statement_index": index,
                "statement_type": (statement.get("StatementType") or "").upper(),
                "statement_cost": _float(statement.get("StatementSubTreeCost")),
                "query_hash": statement.get("QueryHash"),
                "finding_count": len(statement_findings),
            }
        )
    findings.sort(
        key=lambda item: (
            -SEVERITY_ORDER.get(item.severity, 0),
            -(item.estimated_cost_share or 0.0),
            item.rule,
        )
    )
    counts: dict[str, int] = {}
    families: dict[str, int] = {}
    for item in findings:
        counts[item.severity] = counts.get(item.severity, 0) + 1
        families[item.family] = families.get(item.family, 0) + 1
    return {
        "plan_kind": "actual" if actual else "estimated",
        "parse_error": None,
        "statements": statements_out,
        "finding_counts": counts,
        "families": families,
        "findings": [item.as_dict() for item in findings[:MAX_FINDINGS]],
        "truncated": len(findings) > MAX_FINDINGS,
        "note": (
            "Findings are leads ranked by severity and the operator's share of estimated "
            "cost. Measure any change with a benchmark before claiming a gain."
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


def _statement_findings(
    index: int, statement: ET.Element, accesses: list[Any], actual: bool
) -> list[PlanFinding]:
    findings: list[PlanFinding] = []
    query_plan = statement.find(f"{_Q}QueryPlan")
    statement_cost = _float(statement.get("StatementSubTreeCost"))
    if query_plan is None:
        return findings
    relops = list(query_plan.iter(f"{_Q}RelOp"))
    share = _cost_shares(relops, statement_cost)

    def add(
        rule: str,
        severity: str,
        family: str,
        message: str,
        fix: str,
        *,
        node: ET.Element | None = None,
        evidence: dict[str, Any] | None = None,
        cost_share: float | None = None,
    ) -> None:
        node_id = int(_float(node.get("NodeId"))) if node is not None else None
        if cost_share is None and node_id is not None:
            cost_share = share.get(node_id)
        findings.append(
            PlanFinding(
                rule=rule,
                severity=severity,
                family=family,
                message=message,
                fix=fix,
                node_id=node_id,
                statement_index=index,
                evidence=evidence or {},
                estimated_cost_share=cost_share,
            )
        )

    _access_rules(accesses, add)
    _statement_warning_rules(statement, query_plan, add, actual)
    for relop in relops:
        _operator_rules(relop, add, actual)
    if actual:
        for relop in relops:
            _runtime_rules(relop, add)
    return findings


def _access_rules(accesses: list[Any], add) -> None:
    for access in accesses:
        table = f"{access.schema}.{access.table}"
        for column, kind in access.nonsargable.items():
            add(
                "implicit_conversion_on_column" if kind == "convert_implicit" else "non_sargable_predicate",
                _share_severity(access.cost_share, high=0.3, medium=0.05),
                "predicates",
                (
                    f"{table}.{column} is wrapped in an implicit conversion, so no index can seek on it."
                    if kind == "convert_implicit"
                    else f"{table}.{column} is wrapped in {kind.replace('function:', '').upper()} "
                    "inside the predicate, so no index can seek on it."
                ),
                (
                    "Bind the parameter or variable as the column's exact type."
                    if kind == "convert_implicit"
                    else "Rewrite the predicate so the column is bare (move the work to the "
                    "parameter side, or use a half-open range for date buckets)."
                ),
                cost_share=access.cost_share,
                evidence={"table": table, "column": column, "wrapper": kind, "operation": access.operation},
            )
        if access.operation in {"lookup", "rid_lookup"} and access.estimated_executions >= LOOKUP_EXECUTION_FLOOR:
            add(
                "rid_lookup" if access.operation == "rid_lookup" else "key_lookup",
                _share_severity(access.cost_share, high=0.3, medium=0.1),
                "indexes",
                f"{access.operation.replace('_', ' ').title()} on {table} runs about "
                f"{access.estimated_executions:,.0f} times to fetch "
                f"{', '.join(access.output_columns) or 'row data'}.",
                (
                    f"Add {', '.join(access.output_columns)} as INCLUDE columns to "
                    f"{access.paired_index} (or let review_workload_indexes design it), then benchmark."
                    if access.paired_index and access.output_columns
                    else "Cover the lookup with an index that includes the fetched columns, then benchmark."
                ),
                cost_share=access.cost_share,
                evidence={
                    "table": table,
                    "estimated_executions": access.estimated_executions,
                    "fetched_columns": list(access.output_columns),
                    "feeding_index": access.paired_index,
                },
            )
        seekable = [c for c, kind in access.residual.items() if kind in {"eq", "range"}]
        if access.operation in {"scan", "heap_scan"} and seekable and access.cost_share >= 0.05:
            add(
                "scan_with_seekable_predicate",
                _share_severity(access.cost_share, high=0.4, medium=0.1),
                "indexes",
                f"{table} is scanned while filtering on {', '.join(seekable)}; an index keyed on "
                "those columns could seek instead.",
                "Design the index from the access pattern (equality columns, then one range "
                "column, includes for output) with review_workload_indexes, then benchmark.",
                cost_share=access.cost_share,
                evidence={
                    "table": table,
                    "residual_predicates": dict(access.residual),
                    "estimated_rows": access.estimated_rows,
                    "index_used": access.index_name,
                },
            )


def _statement_warning_rules(statement: ET.Element, query_plan: ET.Element, add, actual: bool) -> None:
    for convert in query_plan.findall(f"{_Q}Warnings/{_Q}PlanAffectingConvert"):
        issue = convert.get("ConvertIssue") or ""
        add(
            "plan_affecting_convert",
            "high" if issue == "Seek Plan" else "medium",
            "predicates",
            f"The optimizer reports a type conversion that affects the plan ({issue}): "
            f"{convert.get('Expression') or 'expression not shown'}.",
            "Make the parameter, variable, or join column types match so the conversion disappears.",
            evidence={"convert_issue": issue, "expression": convert.get("Expression")},
        )
    for group in query_plan.findall(f"{_Q}MissingIndexes/{_Q}MissingIndexGroup"):
        impact = _float(group.get("Impact"))
        for index in group.findall(f"{_Q}MissingIndex"):
            columns: dict[str, list[str]] = {"EQUALITY": [], "INEQUALITY": [], "INCLUDE": []}
            for column_group in index.findall(f"{_Q}ColumnGroup"):
                usage = (column_group.get("Usage") or "").upper()
                columns.setdefault(usage, []).extend(
                    unquote(column.get("Name")) or "" for column in column_group.findall(f"{_Q}Column")
                )
            wide = len(columns.get("INCLUDE", [])) > 8
            add(
                "missing_index_hint",
                "medium" if impact >= 50 else "low",
                "indexes",
                f"The optimizer suggests an index on {unquote(index.get('Schema'))}."
                f"{unquote(index.get('Table'))} (estimated impact {impact:.0f}%)"
                + ("; the include list is wide." if wide else "."),
                "Treat it as a lead: merge it with existing indexes and other queries' needs "
                "(review_workload_indexes) instead of creating it verbatim.",
                evidence={
                    "impact_pct": impact,
                    "equality": columns.get("EQUALITY", []),
                    "inequality": columns.get("INEQUALITY", []),
                    "include": columns.get("INCLUDE", []),
                    "wide_include_list": wide,
                },
            )
    for column in query_plan.findall(f".//{_Q}Warnings/{_Q}ColumnsWithNoStatistics/{_Q}ColumnReference"):
        add(
            "column_without_statistics",
            "medium",
            "cardinality",
            f"No statistics exist on {unquote(column.get('Table'))}.{unquote(column.get('Column'))}; "
            "the estimate is a guess.",
            "Check AUTO_CREATE_STATISTICS and create statistics on the column if it is off.",
            evidence={"table": unquote(column.get("Table")), "column": unquote(column.get("Column"))},
        )
    if query_plan.find(f"{_Q}UnmatchedIndexes") is not None:
        add(
            "unmatched_filtered_index",
            "medium",
            "indexes",
            "A filtered index could not be used because the query is parameterized.",
            "Use a literal for the filter predicate, OPTION (RECOMPILE), or an unfiltered index.",
        )
    reason = query_plan.get("NonParallelPlanReason")
    if reason and reason not in {"MaxDOPSetToOne", "EstimatedDOPIsOne", "NoParallelPlansInDesktopOrExpressEdition"}:
        add(
            "non_parallel_plan_reason",
            "medium" if "UserDefinedFunction" in reason or "TableVariable" in reason else "info",
            "parallelism",
            f"The plan could not go parallel: {reason}.",
            "Remove the blocker (for example a non-inlinable scalar UDF) if the query is CPU-bound and large.",
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
        compiled = parameter.get("ParameterCompiledValue")
        runtime = parameter.get("ParameterRuntimeValue")
        if actual and compiled is not None and runtime is not None and compiled != runtime:
            add(
                "sniffed_parameter_differs",
                "info",
                "cardinality",
                f"{parameter.get('Column')} was compiled for {compiled} but ran with {runtime}.",
                "Check parameter sensitivity with get_query_parameter_buckets before changing anything.",
                evidence={"parameter": parameter.get("Column"), "compiled": compiled, "runtime": runtime},
            )
    grant = query_plan.find(f"{_Q}MemoryGrantInfo")
    if grant is not None and actual:
        granted = _float(grant.get("GrantedMemory"))
        used = _float(grant.get("MaxUsedMemory"))
        wait = _float(grant.get("GrantWaitTime"))
        if wait > 0:
            add(
                "memory_grant_wait",
                "high",
                "memory",
                f"The query waited {wait:,.0f} ms for its memory grant.",
                "Reduce the grant (fix row estimates, avoid wide sorts) or reduce concurrency.",
                evidence={"grant_wait_ms": wait, "granted_kb": granted},
            )
        if granted >= EXCESSIVE_GRANT_KB and used < granted * 0.1:
            add(
                "excessive_memory_grant",
                "medium",
                "memory",
                f"The query was granted {granted / 1024:,.0f} MB but used {used / 1024:,.0f} MB.",
                "Fix the overestimate (statistics, predicates) so concurrent queries are not starved.",
                evidence={"granted_kb": granted, "max_used_kb": used},
            )
    for warning in query_plan.findall(f"{_Q}Warnings/{_Q}MemoryGrantWarning"):
        add(
            "memory_grant_warning",
            "medium",
            "memory",
            f"Memory grant warning: {warning.get('GrantWarningKind')}.",
            "Fix the row estimate that sized the grant.",
            evidence=dict(warning.attrib),
        )


def _operator_rules(relop: ET.Element, add, actual: bool) -> None:
    physical = relop.get("PhysicalOp") or ""
    logical = relop.get("LogicalOp") or ""
    warnings = relop.find(f"{_Q}Warnings")
    access = relop.find(f"{_Q}IndexScan")
    if access is None:
        access = relop.find(f"{_Q}TableScan")
    if access is not None:
        obj = access.find(f"{_Q}Object")
        table = unquote(obj.get("Table")) if obj is not None else None
        if table and table.startswith("@"):
            add(
                "table_variable",
                "medium",
                "cardinality",
                f"Table variable {table} is read with an estimate of "
                f"{_float(relop.get('EstimateRows')):,.0f} row(s); that estimate is often far from reality.",
                "Use a temporary table with statistics, or confirm table variable deferred "
                "compilation (compatibility level 150+) gives accurate estimates.",
                node=relop,
                evidence={"table": table, "estimated_rows": _float(relop.get("EstimateRows"))},
            )
    if physical == "Index Spool" and logical == "Eager Spool":
        add(
            "eager_index_spool",
            "high",
            "indexes",
            "The optimizer builds a temporary index (eager index spool) for every execution.",
            "Create a permanent index on the spool's seek columns, then benchmark.",
            node=relop,
        )
    if physical == "Row Count Spool":
        add(
            "row_count_spool",
            "medium",
            "predicates",
            "A row count spool usually means NOT IN against a nullable column.",
            "Rewrite as NOT EXISTS (check NULL semantics first) or make the column NOT NULL.",
            node=relop,
        )
    if warnings is not None and warnings.get("NoJoinPredicate") in {"1", "true"}:
        add(
            "no_join_predicate",
            "high",
            "joins",
            "A join has no join predicate: every row joins to every row.",
            "Add the missing join condition; check for a typo in ON or WHERE.",
            node=relop,
        )
    merge = relop.find(f"{_Q}Merge")
    if merge is not None and merge.get("ManyToMany") in {"1", "true"}:
        add(
            "many_to_many_merge_join",
            "medium",
            "joins",
            "A many-to-many merge join builds a worktable in tempdb.",
            "Make one join input unique (a unique index or DISTINCT on the right key) if the semantics allow.",
            node=relop,
        )
    nested = relop.find(f"{_Q}NestedLoops")
    if nested is not None:
        children = [child for child in nested if child.tag == f"{_Q}RelOp"]
        if len(children) >= 2:
            inner = children[1]
            executions = _executions(inner, actual)
            if executions >= NESTED_LOOP_EXECUTION_FLOOR:
                add(
                    "nested_loops_high_inner_executions",
                    "medium",
                    "joins",
                    f"The inner side of a nested loops join runs about {executions:,.0f} times.",
                    "Check the outer row estimate; an index on the inner join key or a hash join may fit better.",
                    node=relop,
                    evidence={"inner_executions": executions, "actual": actual},
                )
    if relop.find(f".//{_Q}UserDefinedFunction") is not None and physical == "Compute Scalar":
        names = sorted(
            {
                unquote(udf.get("FunctionName")) or ""
                for udf in relop.iter(f"{_Q}UserDefinedFunction")
            }
        )
        add(
            "scalar_udf",
            "medium",
            "udf",
            f"Scalar UDF {', '.join(names)} runs row by row and is not inlined.",
            "Inline the logic, or make the function inlinable (compatibility level 150+) and confirm the plan changes.",
            node=relop,
            evidence={"functions": names},
        )
    if physical == "Table-valued function":
        add(
            "multi_statement_tvf",
            "medium",
            "cardinality",
            "A table-valued function supplies rows with a fixed guess for their count.",
            "Replace it with an inline table-valued function, or rely on interleaved execution (compatibility level 140+).",
            node=relop,
            evidence={"estimated_rows": _float(relop.get("EstimateRows"))},
        )


def _runtime_rules(relop: ET.Element, add) -> None:
    counters = relop.findall(f"{_Q}RunTimeInformation/{_Q}RunTimeCountersPerThread")
    if not counters:
        return
    actual_rows = sum(_float(c.get("ActualRows")) for c in counters)
    executions = sum(_float(c.get("ActualExecutions")) for c in counters) or 1.0
    estimated = _float(relop.get("EstimateRows")) * max(1.0, _float(relop.get("EstimateRebinds")) + _float(relop.get("EstimateRewinds")) + 1.0)
    high = max(actual_rows, estimated)
    low = max(1.0, min(actual_rows, estimated))
    if high >= ESTIMATE_GAP_ROW_FLOOR and high / low >= ESTIMATE_GAP_FACTOR:
        add(
            "row_estimate_gap",
            "high" if high / low >= 100 else "medium",
            "cardinality",
            f"{relop.get('PhysicalOp')} produced {actual_rows:,.0f} rows against an estimate of {estimated:,.0f}.",
            "Find why the estimate is wrong (stale statistics, correlated predicates, parameter "
            "sensitivity, table variables) before changing indexes.",
            node=relop,
            evidence={"actual_rows": actual_rows, "estimated_rows": estimated, "actual_executions": executions},
        )
    warnings = relop.find(f"{_Q}Warnings")
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
                f"{relop.get('PhysicalOp')} spilled to tempdb (level {spill.get('SpillLevel')}).",
                "Fix the row estimate that sized the memory grant, or reduce the rows and columns sorted or hashed.",
                node=relop,
                evidence={
                    "spill_level": spill.get("SpillLevel"),
                    "writes_to_tempdb": details.get("WritesToTempDb") if details is not None else None,
                },
            )
    worker_rows = [
        _float(c.get("ActualRows"))
        for c in counters
        if c.get("Thread") not in {None, "0"}
    ]
    total_rows = sum(worker_rows)
    if len(worker_rows) >= 2 and total_rows >= 10_000:
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
                node=relop,
                evidence={
                    "max_thread_rows": busiest,
                    "total_rows": total_rows,
                    "busiest_thread_share": round(share, 3),
                    "threads": threads,
                },
            )


def _cost_shares(relops: list[ET.Element], statement_cost: float) -> dict[int, float]:
    shares: dict[int, float] = {}
    if statement_cost <= 0:
        return shares
    for relop in relops:
        children = _child_relops(relop)
        own = max(
            0.0,
            _float(relop.get("EstimatedTotalSubtreeCost"))
            - sum(_float(child.get("EstimatedTotalSubtreeCost")) for child in children),
        )
        shares[int(_float(relop.get("NodeId")))] = own / statement_cost
    return shares


def _child_relops(relop: ET.Element) -> list[ET.Element]:
    children: list[ET.Element] = []
    stack = list(relop)
    while stack:
        element = stack.pop()
        if element.tag == f"{_Q}RelOp":
            children.append(element)
            continue
        stack.extend(list(element))
    return children


def _executions(relop: ET.Element, actual: bool) -> float:
    if actual:
        counters = relop.findall(f"{_Q}RunTimeInformation/{_Q}RunTimeCountersPerThread")
        if counters:
            return sum(_float(c.get("ActualExecutions")) for c in counters)
    return _float(relop.get("EstimateRebinds")) + _float(relop.get("EstimateRewinds")) + 1.0


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
