"""Extract per-table access patterns from showplan XML.

Query Store stores the compiled (estimated) plan for every captured statement.
This module turns that plan into one record per table access: which index was
used and how (seek, scan, lookup), which columns the seek consumed, which
columns were filtered as a residual predicate and how (equality, range,
non-SARGable), which columns the operator had to output, which columns a sort
or aggregate above it needed in order, and the operator's share of the
statement's estimated cost.

The records are evidence for index design. They never claim runtime facts: a
stored plan carries estimates only.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Iterator

from .plan_tree import PlanParseError
from .plan_tree import is_true
from .plan_tree import parse_showplan
from .plan_tree import unquote

NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"
_Q = f"{{{NS}}}"

_TABLE_ACCESS_ELEMENTS = {"IndexScan", "TableScan"}
_DML_ELEMENTS = {"Update", "SimpleUpdate", "CreateIndex"}
_SARGABLE_COMPARE = {"EQ", "IS", "LT", "LE", "GT", "GE"}
_RANGE_COMPARE = {"LT", "LE", "GT", "GE"}
_WRAPPER_ELEMENTS = {
    "Convert": "convert",
    "Intrinsic": "intrinsic",
    "Arithmetic": "arithmetic",
    # ScalarType children; "UDF" is a statement-level element, never inside a predicate.
    "UserDefinedFunction": "udf",
    "UserDefinedAggregate": "udf",
    "IF": "case",
    "Aggregate": "aggregate",
}


@dataclass(frozen=True)
class ColumnRef:
    schema: str | None
    table: str | None
    alias: str | None
    column: str

    @property
    def is_table_column(self) -> bool:
        return bool(self.table) and not self.column.startswith("@")


@dataclass
class MissingIndexHint:
    schema: str
    table: str
    equality: tuple[str, ...]
    inequality: tuple[str, ...]
    include: tuple[str, ...]
    impact: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "table": self.table,
            "equality_columns": list(self.equality),
            "inequality_columns": list(self.inequality),
            "include_columns": list(self.include),
            "impact_pct": self.impact,
        }


@dataclass
class TableAccess:
    node_id: int
    physical_op: str
    logical_op: str
    schema: str
    table: str
    alias: str | None
    index_name: str | None
    index_kind: str | None
    storage: str | None
    operation: str
    seek_eq_columns: tuple[str, ...] = ()
    seek_range_columns: tuple[str, ...] = ()
    # Range columns whose bounds are computed at run time (dynamic seek).
    dynamic_seek_columns: tuple[str, ...] = ()
    residual: dict[str, str] = field(default_factory=dict)
    nonsargable: dict[str, str] = field(default_factory=dict)
    implicit_conversion_columns: tuple[str, ...] = ()
    output_columns: tuple[str, ...] = ()
    order_columns: tuple[tuple[str, str], ...] = ()
    join_columns: tuple[str, ...] = ()
    estimated_rows: float = 0.0
    estimated_executions: float = 1.0
    own_cost: float = 0.0
    cost_share: float = 0.0
    paired_index: str | None = None
    paired_node_id: int | None = None
    ordered: bool = False
    forced_index: bool = False
    spool_node_id: int | None = None
    spool_eq_columns: tuple[str, ...] = ()
    spool_range_columns: tuple[str, ...] = ()

    @property
    def residual_eq_columns(self) -> tuple[str, ...]:
        return tuple(column for column, kind in self.residual.items() if kind == "eq")

    @property
    def residual_range_columns(self) -> tuple[str, ...]:
        return tuple(column for column, kind in self.residual.items() if kind == "range")

    def as_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "physical_op": self.physical_op,
            "operation": self.operation,
            "schema": self.schema,
            "table": self.table,
            "alias": self.alias,
            "index_name": self.index_name,
            "index_kind": self.index_kind,
            "storage": self.storage,
            "seek_eq_columns": list(self.seek_eq_columns),
            "seek_range_columns": list(self.seek_range_columns),
            "residual_predicates": dict(self.residual),
            "nonsargable_predicates": dict(self.nonsargable),
            "implicit_conversion_columns": list(self.implicit_conversion_columns),
            "output_columns": list(self.output_columns),
            "order_columns": [
                {"column": column, "direction": direction}
                for column, direction in self.order_columns
            ],
            "join_columns": list(self.join_columns),
            "estimated_rows": self.estimated_rows,
            "estimated_executions": self.estimated_executions,
            "estimated_cost_share": round(self.cost_share, 6),
            "paired_index": self.paired_index,
            "forced_index": self.forced_index,
            "eager_spool": (
                {
                    "node_id": self.spool_node_id,
                    "eq_columns": list(self.spool_eq_columns),
                    "range_columns": list(self.spool_range_columns),
                }
                if self.spool_node_id is not None
                else None
            ),
        }


@dataclass(frozen=True)
class SpoolIndex:
    """The index an eager index spool builds on every execution.

    The optimizer builds it because no permanent index fits, and the spool
    suppresses the missing-index request, so the spool is the request.
    """

    node_id: int
    schema: str | None
    table: str | None
    eq_columns: tuple[str, ...]
    range_columns: tuple[str, ...]
    include_columns: tuple[str, ...]
    own_cost: float


@dataclass
class DmlTarget:
    schema: str
    table: str
    operation: str
    indexes_maintained: tuple[str, ...]


@dataclass
class StatementAccess:
    statement_type: str
    statement_cost: float
    query_hash: str | None
    plan_hash: str | None
    accesses: list[TableAccess] = field(default_factory=list)
    missing_indexes: list[MissingIndexHint] = field(default_factory=list)
    dml_targets: list[DmlTarget] = field(default_factory=list)


@dataclass
class PlanAccessSummary:
    statements: list[StatementAccess] = field(default_factory=list)
    parse_error: str | None = None

    @property
    def accesses(self) -> Iterator[TableAccess]:
        for statement in self.statements:
            yield from statement.accesses


def parse_plan_access(plan_xml: str) -> PlanAccessSummary:
    try:
        root = parse_showplan(plan_xml)
    except PlanParseError as exc:
        return PlanAccessSummary(parse_error=str(exc))
    summary = PlanAccessSummary()
    for statement in root.iter(f"{_Q}StmtSimple"):
        summary.statements.append(_parse_statement(statement))
    return summary


def _parse_statement(statement: ET.Element) -> StatementAccess:
    result = StatementAccess(
        statement_type=(statement.get("StatementType") or "").upper(),
        statement_cost=_float(statement.get("StatementSubTreeCost")),
        query_hash=statement.get("QueryHash"),
        plan_hash=statement.get("QueryPlanHash"),
    )
    query_plan = statement.find(f"{_Q}QueryPlan")
    if query_plan is None:
        return result
    result.missing_indexes = _missing_indexes(query_plan)
    root_op = query_plan.find(f"{_Q}RelOp")
    if root_op is None:
        return result
    parents: dict[ET.Element, ET.Element] = {}
    for parent in root_op.iter():
        for child in parent:
            parents[child] = parent
    statement_cost = result.statement_cost or _float(root_op.get("EstimatedTotalSubtreeCost"))
    for relop in root_op.iter(f"{_Q}RelOp"):
        access_element = _first_child(relop, _TABLE_ACCESS_ELEMENTS)
        if access_element is not None:
            access = _parse_access(relop, access_element, statement_cost)
            if access is not None:
                result.accesses.append(access)
        dml_element = _first_child(relop, _DML_ELEMENTS)
        if dml_element is not None:
            target = _parse_dml(relop, dml_element)
            if target is not None:
                result.dml_targets.append(target)
    _attach_order_and_join_columns(root_op, result.accesses)
    _pair_lookups(root_op, result.accesses)
    _attach_eager_spools(root_op, result.accesses, statement_cost)
    return result


def _parse_access(
    relop: ET.Element, element: ET.Element, statement_cost: float
) -> TableAccess | None:
    obj = element.find(f"{_Q}Object")
    if obj is None:
        return None
    schema = unquote(obj.get("Schema"))
    table = unquote(obj.get("Table"))
    if not schema or not table:
        return None
    alias = unquote(obj.get("Alias"))
    index_name = unquote(obj.get("Index"))
    index_kind = obj.get("IndexKind") or ("Heap" if element.tag == f"{_Q}TableScan" else None)
    storage = obj.get("Storage") or element.get("Storage")
    physical_op = relop.get("PhysicalOp") or ""
    is_lookup = is_true(element.get("Lookup")) or physical_op in {"Key Lookup", "RID Lookup"}
    seek_eq, seek_range = _seek_columns(element)
    operation = _operation(physical_op, element, index_kind, is_lookup, bool(seek_eq or seek_range))

    def belongs(ref: ColumnRef) -> bool:
        return _same_table(ref, schema, table, alias)

    residual: dict[str, str] = {}
    nonsargable: dict[str, str] = {}
    implicit: list[str] = []
    predicate = element.find(f"{_Q}Predicate")
    if predicate is not None:
        _classify_predicate(predicate, belongs, residual, nonsargable, implicit)
    seek_columns = set(seek_eq) | set(seek_range)
    for column in list(residual):
        if column in seek_columns and residual[column] == "eq":
            residual.pop(column)

    outputs = [
        ref.column
        for ref in _column_refs(relop.find(f"{_Q}OutputList"))
        if belongs(ref) and not _is_internal_column(ref.column)
    ]
    executions = _float(relop.get("EstimateRebinds")) + _float(relop.get("EstimateRewinds")) + 1.0
    own_cost = max(
        0.0,
        _float(relop.get("EstimatedTotalSubtreeCost"))
        - sum(
            _float(child.get("EstimatedTotalSubtreeCost"))
            for child in _child_relops(relop)
        ),
    )
    return TableAccess(
        node_id=int(_float(relop.get("NodeId"))),
        physical_op=physical_op,
        logical_op=relop.get("LogicalOp") or "",
        schema=schema,
        table=table,
        alias=alias,
        index_name=index_name,
        index_kind=index_kind,
        storage=storage,
        operation=operation,
        seek_eq_columns=tuple(seek_eq),
        seek_range_columns=tuple(seek_range),
        dynamic_seek_columns=tuple(_dynamic_seek_columns(element)),
        residual=residual,
        nonsargable=nonsargable,
        implicit_conversion_columns=tuple(dict.fromkeys(implicit)),
        output_columns=tuple(dict.fromkeys(outputs)),
        estimated_rows=_float(relop.get("EstimateRows")),
        estimated_executions=executions,
        own_cost=own_cost,
        cost_share=(own_cost / statement_cost) if statement_cost > 0 else 0.0,
        ordered=is_true(element.get("Ordered")),
        forced_index=is_true(element.get("ForcedIndex")),
    )


def _operation(
    physical_op: str,
    element: ET.Element,
    index_kind: str | None,
    is_lookup: bool,
    has_seek: bool,
) -> str:
    if is_lookup:
        heap = (index_kind or "").lower() == "heap" or physical_op == "RID Lookup"
        return "rid_lookup" if heap else "lookup"
    if element.tag == f"{_Q}TableScan":
        return "heap_scan"
    if has_seek or "Seek" in physical_op:
        return "seek"
    return "scan"


def _seek_columns(element: ET.Element) -> tuple[list[str], list[str]]:
    seek_predicates = element.find(f"{_Q}SeekPredicates")
    if seek_predicates is None:
        return [], []
    return _key_columns(seek_predicates)


def _dynamic_seek_columns(element: ET.Element) -> list[str]:
    """Range columns bounded by an internal expression column.

    GetRangeThroughConvert, GetRangeWithMismatchedTypes and LikeRange* compute the
    bounds in a Compute Scalar (Expr1003, ...) that the seek range reads; a range
    bounded by a parameter or constant is a plain seek.
    """

    seek_predicates = element.find(f"{_Q}SeekPredicates")
    if seek_predicates is None:
        return []
    dynamic: list[str] = []
    for part in seek_predicates.iter():
        if part.tag not in {f"{_Q}StartRange", f"{_Q}EndRange"}:
            continue
        columns = _column_refs(part.find(f"{_Q}RangeColumns"))
        expressions = part.find(f"{_Q}RangeExpressions")
        bounds = expressions.findall(f"{_Q}ScalarOperator") if expressions is not None else []
        for ref, bound in zip(columns, bounds):
            computed = any(
                not inner.table and inner.column.lower().startswith("expr") for inner in _column_refs(bound)
            )
            if computed and not _is_internal_column(ref.column) and ref.column not in dynamic:
                dynamic.append(ref.column)
    return dynamic


def _key_columns(seek_predicates: ET.Element) -> tuple[list[str], list[str]]:
    eq: list[str] = []
    ranged: list[str] = []
    containers = list(seek_predicates.iter(f"{_Q}SeekKeys")) + list(
        seek_predicates.iter(f"{_Q}SeekPredicate")
    )
    for keys in containers:
        for part in keys:
            columns = [
                ref.column
                for ref in _column_refs(part.find(f"{_Q}RangeColumns"))
                if not _is_internal_column(ref.column)
            ]
            if part.tag == f"{_Q}Prefix" and (part.get("ScanType") or "EQ") == "EQ":
                for column in columns:
                    if column not in eq:
                        eq.append(column)
            elif part.tag in {f"{_Q}Prefix", f"{_Q}StartRange", f"{_Q}EndRange"}:
                if columns:
                    last = columns[-1]
                    for column in columns[:-1]:
                        if column not in eq:
                            eq.append(column)
                    if last not in ranged and last not in eq:
                        ranged.append(last)
    return eq, ranged


def _classify_predicate(
    predicate: ET.Element,
    belongs,
    residual: dict[str, str],
    nonsargable: dict[str, str],
    implicit: list[str],
) -> None:
    scalar = predicate.find(f"{_Q}ScalarOperator")
    if scalar is None:
        return
    _classify_scalar(scalar, belongs, residual, nonsargable, implicit, under_or=False)


_KIND_RANK = {"eq": 3, "range": 2, "other": 1}


def _record(residual: dict[str, str], column: str, kind: str) -> None:
    current = residual.get(column)
    if current is None or _KIND_RANK[kind] > _KIND_RANK[current]:
        residual[column] = kind


def _record_wrapper(nonsargable: dict[str, str], column: str, wrapper: str) -> None:
    # The first wrapper wins, but a scalar UDF wrap never hides another wrapper on the
    # same column: scalar_udf reports the function, the other wrapper keeps its finding.
    if nonsargable.get(column) in {None, "udf"}:
        nonsargable[column] = wrapper


def _classify_scalar(
    scalar: ET.Element,
    belongs,
    residual: dict[str, str],
    nonsargable: dict[str, str],
    implicit: list[str],
    *,
    under_or: bool,
) -> None:
    for child in scalar:
        tag = _local(child.tag)
        if tag == "Logical":
            operation = (child.get("Operation") or "").upper()
            operands = child.findall(f"{_Q}ScalarOperator")
            if operation == "OR":
                in_column = _in_list_column(operands, belongs)
                if in_column is not None:
                    _record(residual, in_column, "eq")
                    continue
                for operand in operands:
                    _classify_scalar(operand, belongs, residual, nonsargable, implicit, under_or=True)
            else:
                for operand in operands:
                    _classify_scalar(operand, belongs, residual, nonsargable, implicit, under_or=under_or)
        elif tag == "Compare":
            _classify_compare(child, belongs, residual, nonsargable, implicit, under_or=under_or)
        elif tag == "Intrinsic" and (child.get("FunctionName") or "").lower() == "like":
            for operand_column, wrapped in _operand_columns(child, belongs):
                if wrapped is None:
                    _record(residual, operand_column, "other")
                else:
                    _record_wrapper(nonsargable, operand_column, wrapped)
        else:
            for ref, wrapper in _wrapped_columns(child, belongs, wrapper=None):
                if wrapper is not None:
                    _record_wrapper(nonsargable, ref, wrapper)
                    if wrapper == "convert_implicit":
                        implicit.append(ref)
                else:
                    _record(residual, ref, "other")


def _classify_compare(
    compare: ET.Element,
    belongs,
    residual: dict[str, str],
    nonsargable: dict[str, str],
    implicit: list[str],
    *,
    under_or: bool,
) -> None:
    op = (compare.get("CompareOp") or "").upper()
    operands = compare.findall(f"{_Q}ScalarOperator")
    for index, operand in enumerate(operands):
        other_operands = [item for position, item in enumerate(operands) if position != index]
        direct = _direct_column(operand)
        if direct is not None and belongs(direct):
            if under_or or op not in _SARGABLE_COMPARE:
                _record(residual, direct.column, "other")
            elif any(_references_same_alias_column(other, belongs) for other in other_operands):
                _record(residual, direct.column, "other")
            else:
                _record(residual, direct.column, "range" if op in _RANGE_COMPARE else "eq")
            continue
        for column, wrapper in _wrapped_columns(operand, belongs, wrapper=None):
            if wrapper is None:
                _record(residual, column, "other")
            else:
                _record_wrapper(nonsargable, column, wrapper)
                if wrapper == "convert_implicit":
                    implicit.append(column)


def _in_list_column(operands: list[ET.Element], belongs) -> str | None:
    column: str | None = None
    if len(operands) < 2:
        return None
    for operand in operands:
        compare = operand.find(f"{_Q}Compare")
        if compare is None or (compare.get("CompareOp") or "").upper() != "EQ":
            return None
        refs = [
            ref
            for side in compare.findall(f"{_Q}ScalarOperator")
            if (ref := _direct_column(side)) is not None and belongs(ref)
        ]
        if len(refs) != 1:
            return None
        if column is None:
            column = refs[0].column
        elif column != refs[0].column:
            return None
    return column


def _direct_column(operand: ET.Element) -> ColumnRef | None:
    identifier = operand.find(f"{_Q}Identifier")
    if identifier is None or len(operand) != 1:
        return None
    reference = identifier.find(f"{_Q}ColumnReference")
    if reference is None:
        return None
    return _ref(reference)


def _references_same_alias_column(operand: ET.Element, belongs) -> bool:
    return any(belongs(ref) for ref in _column_refs(operand))


def _operand_columns(element: ET.Element, belongs) -> list[tuple[str, str | None]]:
    results: list[tuple[str, str | None]] = []
    for operand in element.findall(f"{_Q}ScalarOperator"):
        direct = _direct_column(operand)
        if direct is not None and belongs(direct):
            results.append((direct.column, None))
            continue
        results.extend(_wrapped_columns(operand, belongs, wrapper=None))
    return results


def _wrapped_columns(element: ET.Element, belongs, *, wrapper: str | None) -> list[tuple[str, str | None]]:
    """Return target-table columns under ``element`` with the nearest wrapper kind."""

    results: list[tuple[str, str | None]] = []
    tag = _local(element.tag)
    current = wrapper
    if tag in _WRAPPER_ELEMENTS and current is None:
        if tag == "Convert" and is_true(element.get("Implicit")):
            current = "convert_implicit"
        elif tag == "Intrinsic":
            current = f"function:{(element.get('FunctionName') or 'unknown').lower()}"
        else:
            current = _WRAPPER_ELEMENTS[tag]
    if tag == "ColumnReference":
        ref = _ref(element)
        if belongs(ref) and not _is_internal_column(ref.column):
            results.append((ref.column, current))
        return results
    for child in element:
        results.extend(_wrapped_columns(child, belongs, wrapper=current))
    return results


def _parse_dml(relop: ET.Element, element: ET.Element) -> DmlTarget | None:
    objects = element.findall(f"{_Q}Object")
    if not objects:
        return None
    schema = unquote(objects[0].get("Schema"))
    table = unquote(objects[0].get("Table"))
    if not schema or not table:
        return None
    physical_op = (relop.get("PhysicalOp") or "").lower()
    operation = (
        "insert" if "insert" in physical_op
        else "delete" if "delete" in physical_op
        else "update"
    )
    indexes = tuple(
        name for obj in objects if (name := unquote(obj.get("Index"))) is not None
    )
    return DmlTarget(schema=schema, table=table, operation=operation, indexes_maintained=indexes)


def _attach_order_and_join_columns(root_op: ET.Element, accesses: list[TableAccess]) -> None:
    by_node = {access.node_id: access for access in accesses}
    for relop in root_op.iter(f"{_Q}RelOp"):
        order_refs: list[tuple[ColumnRef, str]] = []
        join_refs: list[ColumnRef] = []
        sort = relop.find(f"{_Q}Sort")
        if sort is not None:
            for column in sort.findall(f"{_Q}OrderBy/{_Q}OrderByColumn"):
                reference = column.find(f"{_Q}ColumnReference")
                if reference is not None:
                    direction = "ASC" if is_true(column.get("Ascending", "1")) else "DESC"
                    order_refs.append((_ref(reference), direction))
        stream = relop.find(f"{_Q}StreamAggregate")
        if stream is not None:
            for reference in stream.findall(f"{_Q}GroupBy/{_Q}ColumnReference"):
                order_refs.append((_ref(reference), "ASC"))
        for keys_path in (
            f"{_Q}Hash/{_Q}HashKeysBuild",
            f"{_Q}Hash/{_Q}HashKeysProbe",
            f"{_Q}Merge/{_Q}InnerSideJoinColumns",
            f"{_Q}Merge/{_Q}OuterSideJoinColumns",
        ):
            for reference in relop.findall(f"{keys_path}/{_Q}ColumnReference"):
                join_refs.append(_ref(reference))
        if not order_refs and not join_refs:
            continue
        for descendant in relop.iter(f"{_Q}RelOp"):
            access = by_node.get(int(_float(descendant.get("NodeId"))))
            if access is None or descendant is relop:
                continue
            order = tuple(
                (ref.column, direction)
                for ref, direction in order_refs
                if _same_table(ref, access.schema, access.table, access.alias)
            )
            if order:
                # Pre-order traversal: a deeper (nearer) sort or aggregate wins.
                access.order_columns = order
            joins = tuple(
                ref.column
                for ref in join_refs
                if _same_table(ref, access.schema, access.table, access.alias)
            )
            if joins:
                access.join_columns = tuple(dict.fromkeys(access.join_columns + joins))


def _pair_lookups(root_op: ET.Element, accesses: list[TableAccess]) -> None:
    """Link each key or RID lookup to the nonclustered access that fed it."""

    by_node = {access.node_id: access for access in accesses}
    for relop in root_op.iter(f"{_Q}RelOp"):
        nested = relop.find(f"{_Q}NestedLoops")
        if nested is None:
            continue
        inside = [
            by_node[node_id]
            for descendant in relop.iter(f"{_Q}RelOp")
            if descendant is not relop
            and (node_id := int(_float(descendant.get("NodeId")))) in by_node
        ]
        lookups = [item for item in inside if item.operation in {"lookup", "rid_lookup"}]
        for lookup in lookups:
            if lookup.paired_index is not None:
                continue
            feeders = [
                item
                for item in inside
                if item.operation in {"seek", "scan"}
                and item.table == lookup.table
                and item.schema == lookup.schema
                and item.alias == lookup.alias
                and (item.index_kind or "").lower() == "nonclustered"
            ]
            if feeders:
                lookup.paired_index = feeders[0].index_name
                lookup.paired_node_id = feeders[0].node_id


def eager_spool_index(relop: ET.Element) -> SpoolIndex | None:
    """Keys and includes of an eager index spool; None for any other operator."""

    if relop.get("PhysicalOp") != "Index Spool" or "Eager" not in (relop.get("LogicalOp") or ""):
        return None
    spool = _first_child(relop, {"Spool"})
    if spool is None:
        return None
    eq: list[str] = []
    ranged: list[str] = []
    schema: str | None = None
    table: str | None = None
    for container in spool:
        if _local(container.tag) not in {"SeekPredicateNew", "SeekPredicate"}:
            continue
        container_eq, container_range = _key_columns(container)
        eq.extend(column for column in container_eq if column not in eq)
        ranged.extend(column for column in container_range if column not in ranged and column not in eq)
        for ref in _column_refs(container.find(f".//{_Q}RangeColumns")):
            schema = schema or ref.schema
            table = table or ref.table
    if not eq and not ranged:
        return None
    keys = set(eq) | set(ranged)
    includes = [
        ref.column
        for ref in _column_refs(relop.find(f"{_Q}OutputList"))
        if ref.table == table and not _is_internal_column(ref.column) and ref.column not in keys
    ]
    own_cost = max(
        0.0,
        _float(relop.get("EstimatedTotalSubtreeCost"))
        - sum(_float(child.get("EstimatedTotalSubtreeCost")) for child in _child_relops(relop)),
    )
    return SpoolIndex(
        node_id=int(_float(relop.get("NodeId"))),
        schema=schema,
        table=table,
        eq_columns=tuple(eq),
        range_columns=tuple(ranged),
        include_columns=tuple(dict.fromkeys(includes)),
        own_cost=own_cost,
    )


def _attach_eager_spools(root_op: ET.Element, accesses: list[TableAccess], statement_cost: float) -> None:
    """Give the scan feeding an eager index spool the spool's keys and cost.

    A permanent index with those keys removes both the scan and the spool.
    """

    by_node = {access.node_id: access for access in accesses}
    for relop in root_op.iter(f"{_Q}RelOp"):
        spool = eager_spool_index(relop)
        if spool is None or not spool.table:
            continue
        feeder = next(
            (
                by_node[node_id]
                for descendant in relop.iter(f"{_Q}RelOp")
                if descendant is not relop
                and (node_id := int(_float(descendant.get("NodeId")))) in by_node
                and by_node[node_id].table == spool.table
                and (spool.schema is None or by_node[node_id].schema == spool.schema)
            ),
            None,
        )
        if feeder is None or feeder.operation not in {"scan", "heap_scan"}:
            continue
        feeder.spool_node_id = spool.node_id
        feeder.spool_eq_columns = spool.eq_columns
        feeder.spool_range_columns = spool.range_columns
        feeder.output_columns = tuple(dict.fromkeys(feeder.output_columns + spool.include_columns))
        feeder.own_cost += spool.own_cost
        feeder.cost_share = feeder.own_cost / statement_cost if statement_cost > 0 else 0.0


def _missing_indexes(query_plan: ET.Element) -> list[MissingIndexHint]:
    hints: list[MissingIndexHint] = []
    for group in query_plan.findall(f"{_Q}MissingIndexes/{_Q}MissingIndexGroup"):
        impact = _float(group.get("Impact"))
        for index in group.findall(f"{_Q}MissingIndex"):
            schema = unquote(index.get("Schema"))
            table = unquote(index.get("Table"))
            if not schema or not table:
                continue
            groups: dict[str, list[str]] = {"EQUALITY": [], "INEQUALITY": [], "INCLUDE": []}
            for column_group in index.findall(f"{_Q}ColumnGroup"):
                usage = (column_group.get("Usage") or "").upper()
                for column in column_group.findall(f"{_Q}Column"):
                    name = unquote(column.get("Name"))
                    if name and usage in groups:
                        groups[usage].append(name)
            hints.append(
                MissingIndexHint(
                    schema=schema,
                    table=table,
                    equality=tuple(groups["EQUALITY"]),
                    inequality=tuple(groups["INEQUALITY"]),
                    include=tuple(groups["INCLUDE"]),
                    impact=impact,
                )
            )
    return hints


def _same_table(ref: ColumnRef, schema: str, table: str, alias: str | None) -> bool:
    if not ref.table or ref.table != table:
        return False
    if ref.schema and ref.schema != schema:
        return False
    if alias and ref.alias and ref.alias != alias:
        return False
    return True


def _ref(element: ET.Element) -> ColumnRef:
    return ColumnRef(
        schema=unquote(element.get("Schema")),
        table=unquote(element.get("Table")),
        alias=unquote(element.get("Alias")),
        column=unquote(element.get("Column")) or "",
    )


def _column_refs(element: ET.Element | None) -> list[ColumnRef]:
    if element is None:
        return []
    return [_ref(reference) for reference in element.iter(f"{_Q}ColumnReference")]


def _is_internal_column(column: str) -> bool:
    lowered = column.lower()
    return (
        not column
        or column.startswith("@")
        or lowered.startswith(("expr", "bmk", "uniq", "chk", "ptnid", "rowid", "partitionid"))
    )


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


def _first_child(relop: ET.Element, local_names: set[str]) -> ET.Element | None:
    for child in relop:
        if _local(child.tag) in local_names:
            return child
    return None


def _local(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
