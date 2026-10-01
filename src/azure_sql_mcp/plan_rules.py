"""Rule-based anti-pattern detection over showplan XML (estimated or actual).

Each finding names a stable rule code, a severity, the plan node, the numbers
from the plan that triggered it, the optimizer pattern family it belongs to,
and a direction for the fix. On an actual plan with operator times a finding
also carries its operator's self elapsed time and that time's share of the
statement's elapsed time, and ranking uses the share; otherwise findings rank
by the operator's share of estimated cost, which is an estimate in every plan.
Row estimates are compared per execution. Runtime rules (spills, estimate gaps,
thread skew, grant waste) apply only to actual plans. Findings are leads to
measure, not measured gains.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from .plan_tree import PlanNode
from .plan_tree import PlanParseError
from .plan_tree import build_tree
from .plan_tree import ce_guess_fraction
from .plan_tree import clean_text
from .plan_tree import cross_join_check
from .plan_tree import estimate_check
from .plan_tree import parse_showplan
from .plan_tree import query_time_stats
from .plan_tree import statement_elapsed_ms
from .plan_tree import statements
from .plan_tree import unquote
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
EXCESSIVE_GRANT_KB = 1024 * 1024
UDF_TIME_SHARE_FLOOR = 0.1
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
    self_elapsed_ms: float | None = None
    elapsed_share: float | None = None

    @property
    def impact_share(self) -> float:
        """Measured share when the plan timed its operators, else the estimated share."""

        if self.elapsed_share is not None:
            return self.elapsed_share
        return self.estimated_cost_share or 0.0

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
            "self_elapsed_ms": self.self_elapsed_ms,
            "elapsed_share": round(self.elapsed_share, 4) if self.elapsed_share is not None else None,
        }


def analyze_plan(plan_xml: str) -> dict[str, Any]:
    """Run every rule over a showplan and return sorted findings."""

    try:
        root = parse_showplan(plan_xml)
        actual = root.find(f".//{_Q}RunTimeInformation") is not None
        access_summary = parse_plan_access(plan_xml)
        findings: list[PlanFinding] = []
        statements_out: list[dict[str, Any]] = []
        timed = False
        for index, (statement, query_plan) in enumerate(statements(root)):
            nodes = build_tree(query_plan)
            timed = timed or any(node.has_timing for node in nodes)
            accesses = (
                access_summary.statements[index].accesses
                if index < len(access_summary.statements)
                else []
            )
            statement_findings = _statement_findings(index, statement, query_plan, nodes, accesses, actual)
            findings.extend(statement_findings)
            statements_out.append(
                {
                    "statement_index": index,
                    "statement_type": (statement.get("StatementType") or "").upper(),
                    "statement_cost": _float(statement.get("StatementSubTreeCost")),
                    "elapsed_ms": statement_elapsed_ms(query_plan, nodes) if actual else None,
                    "query_hash": statement.get("QueryHash"),
                    "finding_count": len(statement_findings),
                }
            )
    except PlanParseError as exc:
        return _empty(str(exc))
    findings.sort(key=lambda item: (-SEVERITY_ORDER.get(item.severity, 0), -item.impact_share, item.rule))
    counts: dict[str, int] = {}
    families: dict[str, int] = {}
    for item in findings:
        counts[item.severity] = counts.get(item.severity, 0) + 1
        families[item.family] = families.get(item.family, 0) + 1
    return {
        "plan_kind": "actual" if actual else "estimated",
        "parse_error": None,
        "ranking_basis": "self_elapsed_share" if timed else "estimated_cost_share",
        "statements": statements_out,
        "finding_counts": counts,
        "families": families,
        "findings": [item.as_dict() for item in findings[:MAX_FINDINGS]],
        "truncated": len(findings) > MAX_FINDINGS,
        "note": (
            "Findings are leads ranked by severity, then by the operator's share of elapsed "
            "time (self time, children excluded) when the plan timed its operators, otherwise "
            "by its share of estimated cost. Cost is an estimate in every plan. Measure any "
            "change with a benchmark before claiming a gain."
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
    index: int,
    statement: ET.Element,
    query_plan: ET.Element | None,
    nodes: list[PlanNode],
    accesses: list[Any],
    actual: bool,
) -> list[PlanFinding]:
    findings: list[PlanFinding] = []
    if query_plan is None:
        return findings
    statement_cost = _float(statement.get("StatementSubTreeCost"))
    by_id = {node.node_id: node for node in nodes}
    cost_shares = {node.node_id: node.own_cost / statement_cost for node in nodes} if statement_cost > 0 else {}
    elapsed = statement_elapsed_ms(query_plan, nodes) if any(node.has_timing for node in nodes) else None
    time_shares = (
        {node.node_id: min(1.0, (node.self_elapsed_ms or 0.0) / elapsed) for node in nodes} if elapsed else {}
    )

    def impact(node_id: int | None, cost_share: float) -> float:
        if node_id is not None and node_id in time_shares:
            return time_shares[node_id]
        return cost_share

    def add(
        rule: str,
        severity: str,
        family: str,
        message: str,
        fix: str,
        *,
        node_id: int | None = None,
        evidence: dict[str, Any] | None = None,
        cost_share: float | None = None,
    ) -> None:
        if cost_share is None and node_id is not None:
            cost_share = cost_shares.get(node_id)
        node = by_id.get(node_id) if node_id is not None else None
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
                self_elapsed_ms=(
                    round(node.self_elapsed_ms, 1)
                    if elapsed and node is not None and node.self_elapsed_ms is not None
                    else None
                ),
                elapsed_share=time_shares.get(node_id) if node_id is not None else None,
            )
        )

    _access_rules(accesses, add, impact)
    _statement_warning_rules(statement, query_plan, add, actual)
    for node in nodes:
        _operator_rules(node, add, actual)
    if actual:
        for node in nodes:
            _runtime_rules(node, add)
    return findings


def _access_rules(accesses: list[Any], add, impact) -> None:
    for access in accesses:
        table = clean_text(f"{access.schema}.{access.table}", 200)
        share = impact(access.node_id, access.cost_share)
        for column, kind in access.nonsargable.items():
            column_name = clean_text(column, 128)
            add(
                "implicit_conversion_on_column" if kind == "convert_implicit" else "non_sargable_predicate",
                _share_severity(share, high=0.3, medium=0.05),
                "predicates",
                (
                    f"{table}.{column_name} is wrapped in an implicit conversion, so no index can seek on it."
                    if kind == "convert_implicit"
                    else f"{table}.{column_name} is wrapped in "
                    f"{clean_text(kind.replace('function:', '').upper(), 60)} "
                    "inside the predicate, so no index can seek on it."
                ),
                (
                    "Bind the parameter or variable as the column's exact type."
                    if kind == "convert_implicit"
                    else "Rewrite the predicate so the column is bare (move the work to the "
                    "parameter side, or use a half-open range for date buckets)."
                ),
                node_id=access.node_id,
                cost_share=access.cost_share,
                evidence={"table": table, "column": column_name, "wrapper": kind, "operation": access.operation},
            )
        if access.operation in {"lookup", "rid_lookup"} and access.estimated_executions >= LOOKUP_EXECUTION_FLOOR:
            fetched = [clean_text(column, 128) or "" for column in access.output_columns]
            add(
                "rid_lookup" if access.operation == "rid_lookup" else "key_lookup",
                _share_severity(share, high=0.3, medium=0.1),
                "indexes",
                f"{access.operation.replace('_', ' ').title()} on {table} runs about "
                f"{access.estimated_executions:,.0f} times to fetch "
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
                evidence={
                    "table": table,
                    "estimated_executions": access.estimated_executions,
                    "fetched_columns": fetched,
                    "feeding_index": access.paired_index,
                },
            )
        seekable = [c for c, kind in access.residual.items() if kind in {"eq", "range"}]
        if access.operation in {"scan", "heap_scan"} and seekable and share >= 0.05:
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


def _statement_warning_rules(statement: ET.Element, query_plan: ET.Element, add, actual: bool) -> None:
    for convert in query_plan.findall(f"{_Q}Warnings/{_Q}PlanAffectingConvert"):
        issue = clean_text(convert.get("ConvertIssue"), 60) or ""
        expression = clean_text(convert.get("Expression"), 300)
        add(
            "plan_affecting_convert",
            "high" if issue == "Seek Plan" else "medium",
            "predicates",
            f"The optimizer reports a type conversion that affects the plan ({issue}): "
            f"{expression or 'expression not shown'}.",
            "Make the parameter, variable, or join column types match so the conversion disappears.",
            evidence={"convert_issue": issue, "expression": expression},
        )
    for group in query_plan.findall(f"{_Q}MissingIndexes/{_Q}MissingIndexGroup"):
        impact = _float(group.get("Impact"))
        for index in group.findall(f"{_Q}MissingIndex"):
            columns: dict[str, list[str]] = {"EQUALITY": [], "INEQUALITY": [], "INCLUDE": []}
            for column_group in index.findall(f"{_Q}ColumnGroup"):
                usage = (column_group.get("Usage") or "").upper()
                columns.setdefault(usage, []).extend(
                    clean_text(unquote(column.get("Name")), 128) or ""
                    for column in column_group.findall(f"{_Q}Column")
                )
            wide = len(columns.get("INCLUDE", [])) > 8
            table = clean_text(f"{unquote(index.get('Schema'))}.{unquote(index.get('Table'))}", 200)
            add(
                "missing_index_hint",
                "medium" if impact >= 50 else "low",
                "indexes",
                f"The optimizer suggests an index on {table} (estimated impact {impact:.0f}%)"
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
    if reason and reason not in {"MaxDOPSetToOne", "EstimatedDOPIsOne", "NoParallelPlansInDesktopOrExpressEdition"}:
        add(
            "non_parallel_plan_reason",
            "medium" if "UserDefinedFunction" in reason or "TableVariable" in reason else "info",
            "parallelism",
            f"The plan could not go parallel: {reason}.",
            "Remove the blocker (for example a non-inlinable scalar UDF, which often reports "
            "CouldNotGenerateValidParallelPlan) if the query is CPU-bound and large.",
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
        if actual and compiled is not None and runtime is not None and compiled != runtime:
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
            f"Memory grant warning: {clean_text(warning.get('GrantWarningKind'), 80)}.",
            "Fix the row estimate that sized the grant.",
            evidence={key: clean_text(value, 100) for key, value in warning.attrib.items()},
        )


def _operator_rules(node: PlanNode, add, actual: bool) -> None:
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
            add(
                "table_variable",
                "medium",
                "cardinality",
                f"Table variable {table} is read with an estimate of "
                f"{node.estimate_rows:,.0f} row(s); that estimate is often far from reality.",
                "Use a temporary table with statistics, or confirm table variable deferred "
                "compilation (compatibility level 150+) gives accurate estimates.",
                node_id=node.node_id,
                evidence={"table": table, "estimated_rows": node.estimate_rows},
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
        )
    if warnings is not None and warnings.get("NoJoinPredicate") in {"1", "true"}:
        _no_join_predicate(node, add)
    merge = relop.find(f"{_Q}Merge")
    if merge is not None and merge.get("ManyToMany") in {"1", "true"}:
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
    if physical == "Compute Scalar" and any(
        element.tag == f"{_Q}UserDefinedFunction" for element in node.own_elements
    ):
        names = sorted(
            {
                clean_text(unquote(element.get("FunctionName")), 200) or ""
                for element in node.own_elements
                if element.tag == f"{_Q}UserDefinedFunction"
            }
        )
        add(
            "scalar_udf",
            "medium",
            "udf",
            f"Scalar UDF {', '.join(names)} runs row by row and is not inlined.",
            "Inline the logic, or make the function inlinable (compatibility level 150+) and confirm the plan changes.",
            node_id=node.node_id,
            evidence={"functions": names},
        )
    if physical == "Table-valued function":
        add(
            "multi_statement_tvf",
            "medium",
            "cardinality",
            "A table-valued function supplies rows with a fixed guess for their count.",
            "Replace it with an inline table-valued function, or rely on interleaved execution (compatibility level 140+).",
            node_id=node.node_id,
            evidence={"estimated_rows": node.estimate_rows},
        )


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


def _runtime_rules(node: PlanNode, add) -> None:
    if not node.has_runtime:
        return
    check = estimate_check(node)
    if check is not None and check.factor >= ESTIMATE_GAP_FACTOR:
        actual_rows = node.actual_rows
        estimated_total = check.estimated_per_execution * max(1.0, check.executions)
        if max(actual_rows, estimated_total) >= ESTIMATE_GAP_ROW_FLOOR:
            guess = ce_guess_fraction(node)
            evidence: dict[str, Any] = {
                "estimated_rows_per_execution": round(check.estimated_per_execution, 3),
                "actual_rows_per_execution": round(check.actual_per_execution, 3),
                "executions": check.executions,
                "estimated_executions": node.estimated_executions,
                "actual_rows": actual_rows,
                "direction": check.direction,
                "factor": round(check.factor, 1),
            }
            message = (
                f"{node.label} estimated {check.estimated_per_execution:,.0f} rows per execution "
                f"but produced {check.actual_per_execution:,.0f} ({check.direction}estimated "
                f"{check.factor:,.0f}x over {check.executions:,.0f} execution(s))."
            )
            if guess is not None:
                evidence["estimate_matches_fixed_guess"] = round(guess, 4)
                message += (
                    f" The estimate is {guess:.1%} of the table, a fixed-guess fraction: the "
                    "optimizer had no usable statistics for this predicate."
                )
            add(
                "row_estimate_gap",
                "high" if check.factor >= 100 else "medium",
                "cardinality",
                message,
                "Find why the estimate is wrong (stale statistics, correlated predicates, parameter "
                "sensitivity, table variables, local variables) before changing indexes.",
                node_id=node.node_id,
                evidence=evidence,
            )
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
                node_id=node.node_id,
                evidence={
                    "max_thread_rows": busiest,
                    "total_rows": total_rows,
                    "busiest_thread_share": round(share, 3),
                    "threads": threads,
                },
            )


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
