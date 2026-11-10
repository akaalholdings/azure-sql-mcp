from __future__ import annotations

import re
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from sqlglot import exp
from sqlglot import parse
from sqlglot.errors import ParseError

from .param_binding import _mask_parameter_non_code
from .safe_sql import read_only_batch_branches
from .safe_sql import extract_hint_inventory


_CLOCK_FUNCTIONS = frozenset(
    {
        "CURRENT_TIMESTAMP",
        "GETDATE",
        "GETUTCDATE",
        "SYSDATETIME",
        "SYSDATETIMEOFFSET",
        "SYSUTCDATETIME",
    }
)
_ROW_VOLATILE_FUNCTIONS = frozenset({"NEWID", "RAND", "CRYPT_GEN_RANDOM"})
# Table hints that replace the statement's isolation level for one table. The
# snapshot comparison cannot cover them: a dirty or lock-skipping read returns
# whatever other sessions are doing at that moment.
_ISOLATION_TABLE_HINTS = frozenset(
    {
        "HOLDLOCK",
        "NOLOCK",
        "READCOMMITTED",
        "READCOMMITTEDLOCK",
        "READPAST",
        "READUNCOMMITTED",
        "REPEATABLEREAD",
        "SERIALIZABLE",
        "SNAPSHOT",
    }
)
_PLAN_FORCING_TABLE_HINTS = frozenset({"FORCESCAN", "FORCESEEK", "INDEX", "NOEXPAND"})
# Owner decision: these change when a plan is compiled, not which plan shape is allowed.
_COMPILE_OPTIONS = frozenset({"OPTIMIZE FOR", "RECOMPILE"})
# A tie gives peers the same value in these functions, so tie order cannot change the result.
_TIE_SAFE_WINDOW_FUNCTIONS = (exp.Rank, exp.DenseRank, exp.PercentRank, exp.CumeDist)
_FRAMED_AGGREGATES = (exp.Sum, exp.Avg, exp.Count, exp.Min, exp.Max)


@dataclass(frozen=True, slots=True)
class EquivalencePreflight:
    """Static facts that determine whether direct snapshot proof is meaningful."""

    volatile_functions: tuple[str, ...] = ()
    statement_stable_clock_functions: tuple[str, ...] = ()
    row_volatile_functions: tuple[str, ...] = ()
    nonrepeatable_table_sample_count: int = 0
    unordered_row_limit_count: int = 0
    ordered_row_limit_without_total_order_count: int = 0
    window_order_without_total_order_count: int = 0
    risk_codes: tuple[str, ...] = ()
    details: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    # The outer ORDER BY of each statement that has no row limit. A tie there
    # is not a risk (the same rows come back); it explains an order-only mismatch.
    result_orders: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    @property
    def direct_snapshot_supported(self) -> bool:
        return not self.risk_codes

    @property
    def classification(self) -> str:
        return (
            "direct_snapshot"
            if self.direct_snapshot_supported
            else "proof_contract_required"
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_version": 1,
            "classification": self.classification,
            "direct_snapshot_supported": self.direct_snapshot_supported,
            "risk_codes": list(self.risk_codes),
            "volatile_functions": list(self.volatile_functions),
            "statement_stable_clock_functions": list(
                self.statement_stable_clock_functions
            ),
            "row_volatile_functions": list(self.row_volatile_functions),
            "nonrepeatable_table_sample_count": (
                self.nonrepeatable_table_sample_count
            ),
            "unordered_row_limit_count": self.unordered_row_limit_count,
            "ordered_row_limit_without_total_order_count": (
                self.ordered_row_limit_without_total_order_count
            ),
            "window_order_without_total_order_count": (
                self.window_order_without_total_order_count
            ),
            "deterministic_proof_contract_available": False,
            "details": [dict(item) for item in self.details],
            "performance_sql_must_remain_unchanged": True,
            "proxy_scope": (
                "not_applicable"
                if self.direct_snapshot_supported
                else (
                    "not available in equivalence contract version 1; "
                    "performance-only"
                )
            ),
        }


def _without_query_options(sql: str) -> str:
    """Blank every OPTION (...) clause, keeping all other text at its position."""

    code = _mask_parameter_non_code(sql).upper()
    text = list(sql)
    for match in re.finditer(r"\bOPTION\s*\(", code):
        depth = 0
        for index in range(match.end() - 1, len(code)):
            depth += (code[index] == "(") - (code[index] == ")")
            if depth == 0:
                text[match.start() : index + 1] = " " * (index + 1 - match.start())
                break
    return "".join(text)


def parseable_sql(sql: str) -> str:
    """Return SQL text the analysis can parse, or raise ValueError.

    The parser does not know every query hint (OPTIMIZE FOR with values, USE
    HINT). A query hint steers the plan and never changes the rows, so when
    the full text does not parse, the analysis reads it without its OPTION
    clauses. The text that is executed and measured is never changed.
    """

    for text in (sql, _without_query_options(sql)):
        try:
            parse(text, read="tsql")
        except ParseError:
            continue
        return text
    raise ValueError("SQL could not be parsed for equivalence preflight.")


def _parse_statements(sql: str) -> list[exp.Expr]:
    return [statement for statement in parse(parseable_sql(sql), read="tsql") if statement]


def _batch_selects(sql: str) -> list[str] | None:
    """The SELECT of each branch when the text is a read-only branching batch."""

    branches = read_only_batch_branches(sql)
    return [branch.select_sql for branch in branches] if branches else None


def _merged_branch_preflights(selects: list[str]) -> EquivalencePreflight:
    """One preflight for a branching batch: a risk in any branch is a risk of the batch."""

    parts = [analyze_equivalence_preflight(select) for select in selects]

    def merged(name: str) -> tuple[str, ...]:
        return tuple(dict.fromkeys(item for part in parts for item in getattr(part, name)))

    return EquivalencePreflight(
        volatile_functions=merged("volatile_functions"),
        statement_stable_clock_functions=merged("statement_stable_clock_functions"),
        row_volatile_functions=merged("row_volatile_functions"),
        nonrepeatable_table_sample_count=sum(part.nonrepeatable_table_sample_count for part in parts),
        unordered_row_limit_count=sum(part.unordered_row_limit_count for part in parts),
        ordered_row_limit_without_total_order_count=sum(
            part.ordered_row_limit_without_total_order_count for part in parts
        ),
        window_order_without_total_order_count=sum(
            part.window_order_without_total_order_count for part in parts
        ),
        risk_codes=merged("risk_codes"),
        details=tuple(
            {**detail, "branch": ordinal}
            for ordinal, part in enumerate(parts, start=1)
            for detail in part.details
        ),
        result_orders=tuple(
            {**detail, "branch": ordinal}
            for ordinal, part in enumerate(parts, start=1)
            for detail in part.result_orders
        ),
    )


def analyze_equivalence_preflight(sql: str) -> EquivalencePreflight:
    """Detect SQL shapes that cannot prove semantics by sequential snapshots.

    This intentionally does not rewrite SQL. A performance run must continue to
    measure the production-shaped statement, while any deterministic proof is a
    separately declared and scoped contract. A read-only branching batch is
    analyzed branch by branch.
    """

    selects = _batch_selects(sql)
    if selects is not None:
        return _merged_branch_preflights(selects)
    statements = _parse_statements(sql)

    clock_functions: set[str] = set()
    row_volatile_functions: set[str] = set()
    nonrepeatable_table_samples: list[dict[str, Any]] = []
    unordered_limits: list[dict[str, Any]] = []
    ordered_limits_without_total_order: list[dict[str, Any]] = []
    window_orders_without_total_order: list[dict[str, Any]] = []

    isolation_hints: set[str] = set()
    result_orders: list[dict[str, Any]] = []

    for statement_ordinal, statement in enumerate(statements, start=1):
        select_ordinal = 0
        window_ordinal = 0
        cte_names = _statement_ctes(statement)
        result_select = _set_leaves(statement)[-1]
        if (
            isinstance(statement, exp.Query)
            and isinstance(result_select, exp.Select)
            and result_select.args.get("order") is not None
            and result_select.args.get("limit") is None
            and result_select.args.get("offset") is None
        ):
            result_order = _total_order_requirement(
                result_select,
                result_select.args["order"].expressions,
                cte_names,
                resolve_projection=True,
            )
            if result_order is not None:
                result_orders.append(
                    {
                        "statement_ordinal": statement_ordinal,
                        "risk": "result_order_ties",
                        "total_order": result_order,
                    }
                )
        for node in statement.walk():
            function_name = _function_name(node)
            if function_name in _CLOCK_FUNCTIONS:
                clock_functions.add(function_name)
            elif function_name in _ROW_VOLATILE_FUNCTIONS:
                row_volatile_functions.add(function_name)

            if isinstance(node, exp.TableSample) and node.args.get("seed") is None:
                nonrepeatable_table_samples.append(
                    {
                        "statement_ordinal": statement_ordinal,
                        "risk": "nonrepeatable_table_sample",
                    }
                )

            if isinstance(node, exp.WithTableHint):
                isolation_hints.update(
                    name
                    for name in (hint.name.upper() for hint in node.expressions)
                    if name in _ISOLATION_TABLE_HINTS
                )

            if isinstance(node, exp.Window):
                window_ordinal += 1
                if node.args.get("order") is not None and not _window_is_tie_safe(node):
                    select = node.find_ancestor(exp.Select)
                    order = node.args["order"]
                    keys = [*(node.args.get("partition_by") or ()), *order.expressions]
                    total_order = (
                        _total_order_requirement(select, keys, cte_names, resolve_projection=False)
                        if select is not None
                        else {"provable": False, "unprovable_reason": "no enclosing SELECT"}
                    )
                    if total_order is not None:
                        window_orders_without_total_order.append(
                            {
                                "statement_ordinal": statement_ordinal,
                                "window_ordinal": window_ordinal,
                                "risk": "window_order_total_order_unproven",
                                "proof_requirement": (
                                    "verify that window ORDER BY keys form a unique "
                                    "total order"
                                ),
                                "total_order": total_order,
                            }
                        )

            if not isinstance(node, exp.Select):
                continue
            select_ordinal += 1
            limit = node.args.get("limit")
            if limit is None and node.args.get("offset") is None:
                continue
            if isinstance(node.parent, exp.Exists):
                # EXISTS only asks whether a row exists; TOP cannot change that.
                continue
            order = node.args.get("order")
            if order is None:
                unordered_limits.append(
                    {
                        "statement_ordinal": statement_ordinal,
                        "select_ordinal": select_ordinal,
                        "risk": "unordered_row_limit",
                    }
                )
                continue
            limit_options = limit.args.get("limit_options") if limit is not None else None
            if limit_options is not None and limit_options.args.get("with_ties"):
                # WITH TIES returns every row tied with the last one.
                continue
            total_order = _total_order_requirement(
                node,
                order.expressions,
                cte_names,
                resolve_projection=True,
            )
            if total_order is not None:
                ordered_limits_without_total_order.append(
                    {
                        "statement_ordinal": statement_ordinal,
                        "select_ordinal": select_ordinal,
                        "risk": "row_limit_total_order_unproven",
                        "proof_requirement": (
                            "verify that ORDER BY keys form a unique total order"
                        ),
                        "total_order": total_order,
                    }
                )

    # The parser does not see every spelling of a table hint; the token scan does.
    isolation_hints.update(statement_hint_profile(sql)["isolation_table_hints"])

    risk_codes: list[str] = []
    details: list[dict[str, Any]] = []
    if clock_functions:
        risk_codes.append("statement_stable_clock")
        details.append(
            {
                "risk": "statement_stable_clock",
                "functions": sorted(clock_functions),
                "proof_requirement": (
                    "freeze one typed clock value for both proof statements"
                ),
            }
        )
    if row_volatile_functions:
        risk_codes.append("row_volatile_function")
        details.append(
            {
                "risk": "row_volatile_function",
                "functions": sorted(row_volatile_functions),
                "proof_requirement": (
                    "direct result equivalence is unsupported without an "
                    "owner-approved invariant"
                ),
            }
        )
    if nonrepeatable_table_samples:
        risk_codes.append("nonrepeatable_table_sample")
        details.extend(nonrepeatable_table_samples)
    if unordered_limits:
        risk_codes.append("unordered_row_limit")
        details.extend(unordered_limits)
    if ordered_limits_without_total_order:
        risk_codes.append("row_limit_total_order_unproven")
        details.extend(ordered_limits_without_total_order)
    if window_orders_without_total_order:
        risk_codes.append("window_order_total_order_unproven")
        details.extend(window_orders_without_total_order)
    if isolation_hints:
        risk_codes.append("isolation_table_hint")
        details.append(
            {
                "risk": "isolation_table_hint",
                "hints": sorted(isolation_hints),
                "proof_requirement": (
                    "remove the isolation table hint; a result match under it holds "
                    "only while no other session writes"
                ),
            }
        )

    volatile = tuple(sorted(clock_functions | row_volatile_functions))
    return EquivalencePreflight(
        volatile_functions=volatile,
        statement_stable_clock_functions=tuple(sorted(clock_functions)),
        row_volatile_functions=tuple(sorted(row_volatile_functions)),
        nonrepeatable_table_sample_count=len(nonrepeatable_table_samples),
        unordered_row_limit_count=len(unordered_limits),
        ordered_row_limit_without_total_order_count=len(
            ordered_limits_without_total_order
        ),
        window_order_without_total_order_count=len(
            window_orders_without_total_order
        ),
        risk_codes=tuple(risk_codes),
        details=tuple(details),
        result_orders=tuple(result_orders),
    )


def _window_is_tie_safe(window: exp.Window) -> bool:
    """Whether tied rows get the same value, so their relative order is irrelevant."""

    function = window.this
    if isinstance(function, _TIE_SAFE_WINDOW_FUNCTIONS):
        return True
    if not isinstance(function, _FRAMED_AGGREGATES):
        return False
    # With ORDER BY and no frame the default is RANGE, which includes all peers.
    spec = window.args.get("spec")
    return spec is None or str(spec.args.get("kind") or "").upper() == "RANGE"


def _select_sources(
    select: exp.Select,
    cte_names: Mapping[str, Any] | set[str],
) -> tuple[list[dict[str, str]], str | None]:
    """Base tables of one SELECT, and why its rows cannot be keyed from the catalog."""

    if isinstance(select.parent, exp.SetOperation):
        return [], "the row limit applies to a set operation, whose rows have no catalog key"
    from_clause = select.args.get("from") or select.args.get("from_")
    items = [from_clause.this] if from_clause is not None else []
    items += [join.this for join in select.args.get("joins") or []]
    sources: list[dict[str, str]] = []
    for item in items:
        if isinstance(item, exp.Lateral) and _is_single_row_subquery(item.this):
            continue  # APPLY (SELECT TOP (1) ...) adds at most one row per outer row
        if not isinstance(item, exp.Table):
            kind = "table function" if isinstance(item, exp.Lateral) else "derived table"
            return [], f"a {kind} in FROM has no catalog key"
        if isinstance(item.this, exp.Parameter):
            return [], "a table variable in FROM has no catalog key the proof can read"
        if not isinstance(item.this, exp.Identifier):
            return [], "a table function in FROM has no catalog key"
        if item.this.args.get("temporary") or item.name.startswith("#"):
            return [], "a temporary table in FROM has no catalog key the proof can read"
        if item.catalog:
            return [], "a table in another database is not inspected"
        if not item.db and item.name.casefold() in cte_names:
            return [], "a common table expression in FROM has no catalog key"
        if not item.db:
            return [], f"table {item.name} is not schema-qualified"
        sources.append(
            {"schema": item.db, "table": item.name, "alias": item.alias_or_name or item.name}
        )
    if not sources:
        return [], "the SELECT reads no base table"
    return sources, None


def _is_single_row_subquery(node: exp.Expr | None) -> bool:
    select = node.this if isinstance(node, exp.Subquery) else None
    limit = select.args.get("limit") if isinstance(select, exp.Select) else None
    if not isinstance(limit, exp.Limit):
        return False
    options = limit.args.get("limit_options")
    count = limit.args.get("expression")
    return (
        isinstance(count, exp.Literal)
        and not count.is_string
        and str(count.this) == "1"
        and not (options is not None and options.args.get("with_ties"))
    )


def _source_of(column: exp.Column, sources: list[dict[str, str]]) -> dict[str, str] | None:
    qualifier = column.table.casefold()
    if not qualifier:
        return sources[0] if len(sources) == 1 else None
    return next(
        (
            source
            for source in sources
            if qualifier in {source["alias"].casefold(), source["table"].casefold()}
        ),
        None,
    )


class _NoRowKey(Exception):
    """The rows of a relation cannot be tied to a key the proof can check."""


@dataclass(slots=True)
class _FromItem:
    """One FROM or JOIN item of a SELECT, as the total-order proof sees it."""

    names: frozenset[str]  # casefolded names a column can be qualified with
    paths: set[str]  # every source under this item
    resolve: Callable[[str], tuple[str, str] | None]  # column name -> (source, column)
    label: str  # how a reason names this item
    selects_star: bool = False


@dataclass(slots=True)
class _RowIdentity:
    """What identifies one row of a SELECT.

    A plain SELECT returns one row per combination of one row from each FROM
    item, so its rows are identified by the base-table rows under it. A derived
    table or CTE that is a plain SELECT is flattened into its own base tables;
    one that returns distinct rows (UNION, INTERSECT, EXCEPT, SELECT DISTINCT)
    is one source whose key is all of its columns.
    """

    tables: list[dict[str, Any]] = field(default_factory=list)
    unique_relations: list[dict[str, Any]] = field(default_factory=list)
    equalities: list[dict[str, Any]] = field(default_factory=list)
    items: list[_FromItem] = field(default_factory=list)

    def item_for(self, column: exp.Column) -> _FromItem | None:
        qualifier = column.table.casefold()
        if not qualifier:
            return self.items[0] if len(self.items) == 1 else None
        return next((item for item in self.items if qualifier in item.names), None)

    def resolve(self, column: exp.Column) -> tuple[str, str] | None:
        item = self.item_for(column)
        return item.resolve(column.name) if item is not None else None

    def base_column(self, target: tuple[str, str] | None) -> dict[str, str] | None:
        """The base-table column a resolved source column is a direct copy of."""

        if target is None:
            return None
        path, column = target
        for table in self.tables:
            if table["alias"] == path:
                return {"schema": table["schema"], "table": table["table"], "column": column}
        for relation in self.unique_relations:
            if relation["alias"] == path:
                return relation["lineage"].get(column.casefold())
        return None

    def why_unresolved(self, column: exp.Column) -> str:
        item = self.item_for(column)
        if item is None or not item.label:
            return f"ordering column {column.name} is not qualified with its table alias"
        name = f"{column.table}.{column.name}" if column.table else column.name
        if item.selects_star:
            return (
                f"{item.label} selects *, so ordering column {name} cannot be traced to a "
                "base table column"
            )
        return (
            f"ordering column {name} of {item.label} is an expression or is not in its "
            "select list, so it cannot be traced to a base table column"
        )


def _statement_ctes(statement: exp.Expr) -> dict[str, exp.CTE | None]:
    """Casefolded CTE name -> its definition; None when the name is defined twice."""

    ctes: dict[str, exp.CTE | None] = {}
    for cte in statement.find_all(exp.CTE):
        name = (cte.alias_or_name or "").casefold()
        if name:
            ctes[name] = None if name in ctes else cte
    return ctes


def _set_leaves(query: exp.Expr) -> list[exp.Expr]:
    """The SELECT blocks of a query, left to right through its set operations."""

    if isinstance(query, (exp.Subquery, exp.Paren)) and not query.args.get("alias"):
        return _set_leaves(query.this)
    if isinstance(query, exp.SetOperation):
        return [*_set_leaves(query.this), *_set_leaves(query.expression)]
    return [query]


def _selects_star(select: exp.Select) -> bool:
    return any(
        isinstance(item, exp.Star) or isinstance(item.this, exp.Star)
        for item in select.expressions
    )


def _output_names(select: exp.Select, declared: Sequence[exp.Expr]) -> list[str | None]:
    if declared:
        return [identifier.name for identifier in declared]
    return [
        item.alias if isinstance(item, exp.Alias) else item.name if isinstance(item, exp.Column) else None
        for item in select.expressions
    ]


def _column_equalities(condition: exp.Expr | None) -> Iterator[tuple[exp.Column, exp.Column]]:
    """Column = column terms that every row passing the condition satisfies."""

    pending = [condition] if condition is not None else []
    while pending:
        node = pending.pop()
        if isinstance(node, exp.Paren):
            pending.append(node.this)
        elif isinstance(node, exp.And):
            pending.extend((node.this, node.expression))
        elif (
            isinstance(node, exp.EQ)
            and isinstance(node.this, exp.Column)
            and isinstance(node.expression, exp.Column)
        ):
            yield node.this, node.expression


def _distinct_rows_relation(
    query: exp.Expr,
    declared: Sequence[exp.Expr],
    path: str,
    label: str,
    ctes: Mapping[str, exp.CTE | None],
    active: frozenset[str],
) -> dict[str, Any]:
    """A relation that returns distinct rows: all of its columns are its key."""

    leaves = [leaf for leaf in _set_leaves(query) if isinstance(leaf, exp.Select)]
    if len(leaves) != len(_set_leaves(query)):
        raise _NoRowKey(f"{label} is not a SELECT, so its rows have no key")
    if any(_selects_star(leaf) for leaf in leaves):
        raise _NoRowKey(f"{label} selects *, so the columns that make its rows distinct cannot be named")
    names = _output_names(leaves[0], declared)
    if any(name is None for name in names) or len({str(name).casefold() for name in names}) != len(names):
        raise _NoRowKey(
            f"{label} has an unnamed or repeated column, so the ORDER BY cannot name "
            "every column that makes its rows distinct"
        )
    copies: list[list[dict[str, str] | None]] = []
    for ordinal, leaf in enumerate(leaves):
        try:
            identity = _row_identity(leaf, ctes, f"{path}#{ordinal}.", active)
        except _NoRowKey:
            copies.append([None] * len(names))
            continue
        copies.append(
            [
                identity.base_column(identity.resolve(item.unalias()))
                if isinstance(item.unalias(), exp.Column)
                else None
                for item in leaf.expressions
            ]
        )
    lineage: dict[str, dict[str, str]] = {}
    for position, name in enumerate(names):
        column_copies = [
            leaf_copies[position] if position < len(leaf_copies) else None
            for leaf_copies in copies
        ]
        first = column_copies[0]
        if first is not None and all(
            copy is not None
            and {key: value.casefold() for key, value in copy.items()}
            == {key: value.casefold() for key, value in first.items()}
            for copy in column_copies
        ):
            lineage[str(name).casefold()] = first
    return {
        "alias": path,
        "relation": label,
        "kind": "distinct_rows",
        "key_columns": [str(name) for name in names],
        "order_columns": [],
        "lineage": lineage,
    }


def _from_item(
    node: exp.Expr,
    identity: _RowIdentity,
    ctes: Mapping[str, exp.CTE | None],
    prefix: str,
    active: frozenset[str],
    outer_label: str,
) -> _FromItem:
    query: exp.Expr | None = None
    declared: Sequence[exp.Expr] = ()
    alias = node.alias_or_name or ""
    label = ""
    if isinstance(node, exp.Table):
        if node.args.get("pivots"):
            raise _NoRowKey("a PIVOT or UNPIVOT in FROM has no catalog key")
        if isinstance(node.this, exp.Parameter):
            raise _NoRowKey("a table variable in FROM has no catalog key the proof can read")
        if not isinstance(node.this, exp.Identifier):
            raise _NoRowKey("a table function in FROM has no catalog key")
        if node.this.args.get("temporary") or node.name.startswith("#"):
            raise _NoRowKey("a temporary table in FROM has no catalog key the proof can read")
        if node.catalog:
            raise _NoRowKey("a table in another database is not inspected")
        name = node.name.casefold()
        if not node.db and name in ctes:
            cte = ctes[name]
            label = f"common table expression {node.name}"
            if cte is None:
                raise _NoRowKey(f"{label} is defined more than once in the statement")
            if name in active:
                raise _NoRowKey(f"{label} is recursive, so its rows have no catalog key")
            active = active | {name}
            query = cte.this
            cte_alias = cte.args.get("alias")
            declared = cte_alias.args.get("columns") or () if cte_alias is not None else ()
        elif not node.db:
            raise _NoRowKey(f"table {node.name} is not schema-qualified")
        elif node.args.get("version") or node.args.get("when"):
            raise _NoRowKey(
                f"{node.db}.{node.name} is read FOR SYSTEM_TIME, which can return several "
                "versions of one key"
            )
        else:
            path = f"{prefix}{alias or node.name}"
            identity.tables.append(
                {
                    "schema": node.db,
                    "table": node.name,
                    "alias": path,
                    **({"relation": outer_label} if outer_label else {}),
                }
            )
            return _FromItem(
                names=frozenset({alias.casefold(), name}),
                paths={path},
                resolve=lambda column: (path, column),
                label="",
            )
    elif isinstance(node, exp.Subquery) and alias:
        label = f"derived table {alias}"
        query = node.this
        table_alias = node.args.get("alias")
        declared = table_alias.args.get("columns") or () if table_alias is not None else ()
    if query is None:
        kind = "table function" if isinstance(node, exp.Lateral) else "derived table"
        raise _NoRowKey(f"a {kind} in FROM has no catalog key")

    path = f"{prefix}{alias}"
    top = query
    while isinstance(top, (exp.Subquery, exp.Paren)) and not top.args.get("alias"):
        top = top.this
    if isinstance(top, exp.SetOperation) and not top.args.get("distinct"):
        raise _NoRowKey(
            f"{label} uses UNION ALL, which can return the same row twice, so no column "
            "of it is a key"
        )
    if isinstance(top, exp.SetOperation) or (
        isinstance(top, exp.Select) and top.args.get("distinct") is not None
    ):
        relation = _distinct_rows_relation(query, declared, path, label, ctes, active)
        identity.unique_relations.append(relation)
        names = {str(name).casefold(): str(name) for name in relation["key_columns"]}
        return _FromItem(
            names=frozenset({alias.casefold()}),
            paths={path},
            resolve=lambda column: (path, names[column.casefold()])
            if column.casefold() in names
            else None,
            label=label,
        )
    if not isinstance(top, exp.Select):
        raise _NoRowKey(f"{label} is not a SELECT, so its rows have no key")

    inner = _row_identity(top, ctes, f"{path}.", active, label)
    identity.tables.extend(inner.tables)
    identity.unique_relations.extend(inner.unique_relations)
    identity.equalities.extend(inner.equalities)
    star = _selects_star(top)
    outputs: dict[str, tuple[str, str] | None] = {}
    if not star:
        for name, item in zip(_output_names(top, declared), top.expressions):
            expression = item.unalias()
            if name is not None:
                outputs[name.casefold()] = (
                    inner.resolve(expression) if isinstance(expression, exp.Column) else None
                )
    return _FromItem(
        names=frozenset({alias.casefold()}),
        paths={source["alias"] for source in (*inner.tables, *inner.unique_relations)},
        resolve=lambda column: outputs.get(column.casefold()),
        label=label,
        selects_star=star,
    )


def _row_identity(
    select: exp.Select,
    ctes: Mapping[str, exp.CTE | None],
    prefix: str = "",
    active: frozenset[str] = frozenset(),
    label: str = "",
) -> _RowIdentity:
    """Sources that identify one row of a SELECT, or raise _NoRowKey with the reason."""

    identity = _RowIdentity()
    from_clause = select.args.get("from") or select.args.get("from_")
    entries: list[tuple[exp.Expr, exp.Join | None]] = (
        [(from_clause.this, None)] if from_clause is not None else []
    )
    entries += [(join.this, join) for join in select.args.get("joins") or []]
    joined: list[tuple[exp.Join, _FromItem]] = []
    for node, join in entries:
        if isinstance(node, exp.Lateral) and _is_single_row_subquery(node.this):
            continue  # APPLY (SELECT TOP (1) ...) adds at most one row per outer row
        item = _from_item(node, identity, ctes, prefix, active, label)
        identity.items.append(item)
        if join is not None:
            joined.append((join, item))
    if not identity.items:
        raise _NoRowKey("the SELECT reads no base table")

    def add(left: exp.Column, right: exp.Column, joined_item: _FromItem | None) -> None:
        source, target = identity.resolve(left), identity.resolve(right)
        if source is None or target is None:
            return
        text = f"{left.sql(dialect='tsql')} = {right.sql(dialect='tsql')}"
        direction = "both"
        if joined_item is not None:
            # LEFT JOIN: a row of the preserved side fixes the joined column,
            # but unmatched rows all carry NULL there, so not the reverse.
            if (source[0] in joined_item.paths) == (target[0] in joined_item.paths):
                return
            if source[0] in joined_item.paths:
                source, target = target, source
            direction = "forward"
        identity.equalities.append(
            {
                "left": {"alias": source[0], "column": source[1]},
                "right": {"alias": target[0], "column": target[1]},
                "direction": direction,
                "text": text,
            }
        )

    sides = {str(join.side or "").upper() for join, _item in joined}
    # RIGHT and FULL joins NULL-extend the earlier sources: no ON equality is used.
    if not sides & {"RIGHT", "FULL"}:
        for join, item in joined:
            for left, right in _column_equalities(join.args.get("on")):
                add(left, right, item if str(join.side or "").upper() == "LEFT" else None)
    where = select.args.get("where")
    for left, right in _column_equalities(where.this if where is not None else None):
        add(left, right, None)
    return identity


def _set_operation_order(
    select: exp.Select,
    keys: list[exp.Expr],
    ctes: Mapping[str, exp.CTE | None],
) -> dict[str, Any]:
    """Total-order requirement for an ORDER BY that applies to a whole set operation."""

    top: exp.Expr = select
    while isinstance(top.parent, exp.SetOperation):
        top = top.parent
    limit = select.args.get("limit")
    if _set_leaves(top)[-1] is not select or isinstance(limit, exp.Limit):
        return {
            "provable": False,
            "unprovable_reason": (
                "the row limit applies to one branch of a set operation, whose rows have "
                "no catalog key"
            ),
        }
    if not top.args.get("distinct"):
        return {
            "provable": False,
            "unprovable_reason": (
                "the row limit applies to a set operation with UNION ALL, which can return "
                "the same row twice, so no column of it is a key"
            ),
        }
    try:
        relation = _distinct_rows_relation(top, (), "", "the set operation", ctes, frozenset())
    except _NoRowKey as exc:
        return {"provable": False, "unprovable_reason": str(exc)}
    names = relation["key_columns"]
    folded = {name.casefold(): name for name in names}
    for key in keys:
        expression = key.this if isinstance(key, exp.Ordered) else key
        name: str | None = None
        if isinstance(expression, exp.Literal) and expression.is_int:
            position = int(expression.name)
            name = names[position - 1] if 1 <= position <= len(names) else None
        elif isinstance(expression, exp.Column) and not expression.table:
            name = folded.get(expression.name.casefold())
        if name is not None and name not in relation["order_columns"]:
            relation["order_columns"].append(name)
    return {"provable": True, "tables": [], "unique_relations": [relation]}


_STATEMENT_CONSTANT_NODES = (
    exp.Literal,
    exp.Parameter,
    exp.Placeholder,
    exp.Var,
    exp.Neg,
    exp.Paren,
    exp.Cast,
    exp.DataType,
    exp.DataTypeParam,
    exp.Identifier,
    exp.Boolean,
)


def _pinned_columns(select: exp.Select) -> list[exp.Column]:
    """Columns the WHERE clause fixes to one value in every returned row.

    Only ``column = <literal, parameter or variable>`` at the top level of an
    AND chain counts. A range, an OR, a comparison with another column or with
    a function call fixes nothing.
    """

    where = select.args.get("where")
    pending = [where.this] if where is not None else []
    pinned: list[exp.Column] = []
    while pending:
        node = pending.pop()
        if isinstance(node, exp.Paren):
            pending.append(node.this)
        elif isinstance(node, exp.And):
            pending.extend((node.this, node.expression))
        elif isinstance(node, exp.EQ):
            for column, value in ((node.this, node.expression), (node.expression, node.this)):
                if isinstance(column, exp.Column) and all(
                    isinstance(item, _STATEMENT_CONSTANT_NODES) for item in value.walk()
                ):
                    pinned.append(column)
    return pinned


def inline_relation(sql: str, schema: str, name: str, body: str) -> str | None:
    """Read a view as its definition: each reference becomes ``(body) AS alias``.

    For analysis only, never for execution. A column of the view then
    resolves to the base column it copies, so an ORDER BY on the view and one
    on its base table are compared as the same key. Table hints on the
    reference (NOEXPAND) are dropped. Returns None when the statement does
    not read the relation or either text cannot be parsed as one statement.
    """

    try:
        statements = _parse_statements(sql)
        definitions = _parse_statements(body)
    except (ParseError, ValueError):
        return None
    if len(statements) != 1 or len(definitions) != 1:
        return None
    definition = definitions[0]
    if not isinstance(definition, exp.Query):
        return None
    replaced = 0

    def inline(node: exp.Expr) -> exp.Expr:
        nonlocal replaced
        if (
            isinstance(node, exp.Table)
            and node.name.casefold() == name.casefold()
            and (node.db or "").casefold() == schema.casefold()
            and not node.catalog
        ):
            replaced += 1
            return exp.Subquery(
                this=definition.copy(),
                alias=exp.TableAlias(this=exp.to_identifier(node.alias or node.name)),
            )
        return node

    inlined = statements[0].transform(inline)
    return inlined.sql(dialect="tsql") if replaced else None


def _expression_identity(expression: exp.Expr, *, ignore_qualifiers: bool) -> str:
    """One text for an expression, so two spellings of it compare equal.

    Identifier case and quoting do not change T-SQL name resolution.
    ``ignore_qualifiers`` drops table qualifiers: valid only where every
    column can come from one source.
    """

    node = expression.copy()
    while isinstance(node, exp.Paren):
        node = node.this
    for column in list(node.find_all(exp.Column)):
        if ignore_qualifiers:
            for part in ("table", "db", "catalog"):
                column.set(part, None)
    for identifier in node.find_all(exp.Identifier):
        identifier.set("this", identifier.name.casefold())
        identifier.set("quoted", False)
    return node.sql(dialect="tsql")


def _total_order_requirement(
    select: exp.Select,
    keys: list[exp.Expr],
    cte_names: Mapping[str, exp.CTE | None],
    *,
    resolve_projection: bool,
) -> dict[str, Any] | None:
    """What the ordering keys must cover for the row order to have no ties.

    Returns None when the ordering is deterministic from the SQL alone: the
    SELECT is grouped and the keys cover every grouping column. Otherwise the
    result lists the sources that identify a row (base tables, and relations
    that return distinct rows) with the plain columns the keys contribute to
    each, and the column equalities that hold in every row. The catalog decides
    whether those cover a unique NOT NULL key of every base table.
    """

    if resolve_projection and isinstance(select.parent, exp.SetOperation):
        # T-SQL applies a trailing ORDER BY to the whole set operation.
        return _set_operation_order(select, keys, cte_names)

    projections = list(select.expressions)
    resolved: list[exp.Expr] = []
    for key in keys:
        expression = key.this if isinstance(key, exp.Ordered) else key
        if resolve_projection and not any(
            isinstance(item, exp.Star) or isinstance(item.this, exp.Star) for item in projections
        ):
            if isinstance(expression, exp.Literal) and expression.is_int:
                position = int(expression.name)
                if 1 <= position <= len(projections):
                    expression = projections[position - 1].unalias()
            elif isinstance(expression, exp.Column) and not expression.table:
                expression = next(
                    (
                        item.this
                        for item in projections
                        if isinstance(item, exp.Alias)
                        and item.alias.casefold() == expression.name.casefold()
                    ),
                    expression,
                )
        resolved.append(expression)
    key_columns = [item for item in resolved if isinstance(item, exp.Column)]
    # A column the WHERE clause fixes to one value is the same in every row,
    # so it orders nothing and may count as an ordering column: a unique key
    # (a, b) with a = @x leaves no two rows tied on ORDER BY b.
    pinned = _pinned_columns(select)

    group = select.args.get("group")
    if group is not None:
        grouped = list(group.expressions)
        # One row per distinct value of the GROUP BY expressions: keys that
        # cover each of them, as the expression itself or through its alias,
        # leave no two rows tied. With one source, a qualifier adds nothing.
        one_source = not select.args.get("joins")
        ordered = {
            _expression_identity(item, ignore_qualifiers=one_source)
            for item in (*resolved, *pinned)
        }
        if grouped and all(
            _expression_identity(item, ignore_qualifiers=one_source) in ordered
            for item in grouped
        ):
            return None
        return {
            "provable": False,
            "unprovable_reason": (
                "the rows are groups; the ordering keys do not include every GROUP BY column"
            ),
        }

    try:
        identity = _row_identity(select, cte_names)
    except _NoRowKey as exc:
        return {"provable": False, "unprovable_reason": str(exc)}
    sources = [*identity.tables, *identity.unique_relations]
    for source in sources:
        source["order_columns"] = []
    by_alias = {source["alias"]: source for source in sources}
    for column in key_columns:
        target = identity.resolve(column)
        if target is None:
            return {"provable": False, "unprovable_reason": identity.why_unresolved(column)}
        if target[1] not in by_alias[target[0]]["order_columns"]:
            by_alias[target[0]]["order_columns"].append(target[1])
    for column in pinned:
        target = identity.resolve(column)
        if target is not None and target[1] not in by_alias[target[0]]["order_columns"]:
            by_alias[target[0]]["order_columns"].append(target[1])
    requirement: dict[str, Any] = {"provable": True, "tables": identity.tables}
    if identity.unique_relations:
        requirement["unique_relations"] = identity.unique_relations
    if identity.equalities:
        requirement["equalities"] = identity.equalities
    return requirement


def _hint_items(code: str, opening: int) -> list[str]:
    """Split the parenthesized list that opens at ``opening`` on top-level commas."""

    depth = 0
    start = opening + 1
    items: list[str] = []
    for index in range(opening, len(code)):
        character = code[index]
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                items.append(code[start:index])
                break
        elif character == "," and depth == 1:
            items.append(code[start:index])
            start = index + 1
    return [item.strip() for item in items if item.strip()]


def statement_hint_profile(sql: str) -> dict[str, Any]:
    """Classify the hints a statement carries, without parsing it.

    A candidate with a plan-forcing hint is ``hinted``: its gain belongs to the
    hint, so it is a plan-enforcement proposal and not a plain rewrite winner.
    RECOMPILE and OPTIMIZE FOR are compile options and do not mark it hinted.
    The read-only validator's scanner is the single source, so the proof gate
    and the admission gate cannot disagree about a hint.
    """

    return extract_hint_inventory(sql).as_profile()


def _statement_sources(statement: exp.Expr, select: exp.Select | None) -> list[dict[str, str]]:
    if select is None:
        return []
    cte_names = {
        cte.alias_or_name.casefold() for cte in statement.find_all(exp.CTE) if cte.alias_or_name
    }
    return _select_sources(select, cte_names)[0]


def _not_null_precondition(
    statement: exp.Expr,
    column: exp.Expr | None,
    select: exp.Select | None,
) -> dict[str, Any] | None:
    if not isinstance(column, exp.Column):
        return None
    source = _source_of(column, _statement_sources(statement, select))
    if source is None:
        return None
    return {
        "kind": "not_null",
        "schema_name": source["schema"],
        "table_name": source["table"],
        "columns": [column.name],
    }


def _conjuncts(condition: exp.Expr | None) -> list[exp.Expr]:
    """The terms of a top-level AND chain."""

    pending = [condition] if condition is not None else []
    terms: list[exp.Expr] = []
    while pending:
        node = pending.pop()
        if isinstance(node, exp.Paren):
            pending.append(node.this)
        elif isinstance(node, exp.And):
            pending.extend((node.expression, node.this))
        else:
            terms.append(node)
    return terms


def _single_table_rows(select: exp.Expr | None) -> tuple[tuple[str, str], list[exp.Expr]] | None:
    """The table and the filter terms of a subquery that reads one table, plainly.

    None for a join, a derived table, GROUP BY, HAVING, DISTINCT or a row
    limit, and for a filter that names a column of another query (a
    correlation): two such subqueries cannot be compared term by term.
    """

    if not isinstance(select, exp.Select) or select.args.get("joins"):
        return None
    if any(select.args.get(name) for name in ("group", "having", "distinct", "limit", "offset")):
        return None
    from_clause = select.args.get("from") or select.args.get("from_")
    table = from_clause.this if from_clause is not None else None
    if not isinstance(table, exp.Table) or table.catalog:
        return None
    where = select.args.get("where")
    terms = _conjuncts(where.this if where is not None else None)
    own = {table.name.casefold(), (table.alias_or_name or table.name).casefold()}
    if any(
        column.table and column.table.casefold() not in own
        for term in terms
        for column in term.find_all(exp.Column)
    ):
        return None
    return ((table.db or "dbo").casefold(), table.name.casefold()), terms


def _null_existence_tests(statement: exp.Expr) -> list[dict[str, Any]]:
    """Each ``NOT EXISTS (SELECT ... WHERE ... column IS NULL)`` of the statement.

    ``filter`` is the other filter terms of the subquery, as comparable text;
    None when the test is not a term of its WHERE clause's AND chain or its
    subquery is not a plain read of one table.
    """

    tests: list[dict[str, Any]] = []
    for node in statement.find_all(exp.Exists):
        if not isinstance(node.parent, exp.Not) or not isinstance(node.this, exp.Select):
            continue
        null_columns = [
            term.this
            for term in _conjuncts((node.this.args.get("where") or exp.Where()).args.get("this"))
            if isinstance(term, exp.Is)
            and isinstance(term.this, exp.Column)
            and isinstance(term.expression, exp.Null)
        ]
        if not null_columns:
            continue
        from_clause = node.this.args.get("from") or node.this.args.get("from_")
        table = from_clause.this if from_clause is not None else None
        if not isinstance(table, exp.Table):
            continue
        ancestor = node.parent.parent
        while isinstance(ancestor, (exp.And, exp.Paren)):
            ancestor = ancestor.parent
        rows = _single_table_rows(node.this) if isinstance(ancestor, exp.Where) else None
        for column in null_columns:
            tests.append(
                {
                    "table": ((table.db or "dbo").casefold(), table.name.casefold()),
                    "column": column.name.casefold(),
                    "filter": (
                        frozenset(
                            _expression_identity(term, ignore_qualifiers=True)
                            for term in rows[1]
                            if not (isinstance(term, exp.Is) and term.this is column)
                        )
                        if rows is not None
                        else None
                    ),
                }
            )
    return tests


def _not_in_subquery_rows(subquery: exp.Expr | None) -> dict[str, Any] | None:
    """The table, the column and the filter of a NOT IN subquery that reads one table."""

    rows = _single_table_rows(subquery)
    if rows is None or not isinstance(subquery, exp.Select) or len(subquery.expressions) != 1:
        return None
    column = subquery.expressions[0]
    if not isinstance(column, exp.Column):
        return None
    return {
        "table": rows[0],
        "column": column.name.casefold(),
        "filter": frozenset(
            _expression_identity(term, ignore_qualifiers=True) for term in rows[1]
        ),
    }


_NEEDED_UNLESS_NULL_HANDLED = "the statement handles NULL"


def _not_in_preconditions(
    removed: Sequence[Mapping[str, Any]],
    null_tests: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any] | None], list[dict[str, Any]]]:
    """NOT NULL preconditions of removed NOT IN predicates, less what the rewrite tests itself.

    NOT IN returns no row when its subquery yields a NULL. A rewrite that
    keeps ``AND NOT EXISTS (the same rows WHERE column IS NULL)`` returns no
    row then either, so it does not need the subquery column to be NOT NULL.
    A NULL test that cannot be matched to the subquery leaves the
    precondition in place, marked as needed unless the statement handles NULL.
    The tested value on the outer side stays a precondition.
    """

    preconditions: list[dict[str, Any] | None] = []
    handled: list[dict[str, Any]] = []
    for item in removed:
        subquery_column, tested_value = item["preconditions"]
        rows = item["rows"]
        subquery_filter = rows["filter"] if rows is not None else None
        tests = (
            [
                test
                for test in null_tests
                if test["table"] == rows["table"] and test["column"] == rows["column"]
            ]
            if rows is not None and subquery_column is not None
            else []
        )
        if subquery_column is not None and any(
            test["filter"] is not None and test["filter"] == subquery_filter for test in tests
        ):
            handled.append(
                {
                    **subquery_column,
                    "how": (
                        "the statement returns no row when this column IS NULL in the "
                        "rows the NOT IN subquery read: NOT EXISTS (... IS NULL) over "
                        "the same table and filter"
                    ),
                }
            )
        elif subquery_column is not None and tests:
            preconditions.append(
                {
                    **subquery_column,
                    "needed_unless": _NEEDED_UNLESS_NULL_HANDLED,
                    "unless_reason": (
                        "the statement tests this column for NULL, but the filter of that "
                        "test could not be matched to the filter of the NOT IN subquery, "
                        "so it is not shown to cover the same rows"
                    ),
                }
            )
        else:
            preconditions.append(subquery_column)
        preconditions.append(tested_value)
    return preconditions, handled


def _shape(statements: list[exp.Expr]) -> dict[str, Any]:
    """Count the constructs whose meaning depends on NULLs, duplicates or missing rows."""

    not_in: list[dict[str, Any]] = []
    counted: dict[str, dict[str, Any] | None] = {}
    shape: dict[str, Any] = {
        "distinct": 0,
        "semi_joins": 0,
        "joins": 0,
        "outer_joins": 0,
        "count_star": 0,
    }
    for statement in statements:
        for node in statement.walk():
            if isinstance(node, exp.Select) and node.args.get("distinct") is not None:
                shape["distinct"] += 1
            elif isinstance(node, exp.Exists):
                shape["semi_joins"] += 1
            elif isinstance(node, exp.In) and node.args.get("query") is not None:
                shape["semi_joins"] += 1
                if isinstance(node.parent, exp.Not):
                    subquery = node.args["query"].this
                    projections = subquery.expressions if isinstance(subquery, exp.Select) else []
                    not_in.append(
                        {
                            "preconditions": [
                                _not_null_precondition(
                                    statement,
                                    projections[0] if len(projections) == 1 else None,
                                    subquery if isinstance(subquery, exp.Select) else None,
                                ),
                                _not_null_precondition(
                                    statement, node.this, node.find_ancestor(exp.Select)
                                ),
                            ],
                            "rows": _not_in_subquery_rows(subquery),
                        }
                    )
            elif isinstance(node, exp.Join) and not isinstance(node.this, exp.Lateral):
                shape["joins"] += 1
                if str(node.side or "").upper() in {"LEFT", "RIGHT", "FULL"}:
                    shape["outer_joins"] += 1
            elif isinstance(node, exp.Count):
                argument = node.this
                if isinstance(argument, exp.Column):
                    counted[f"{argument.table}.{argument.name}".casefold()] = (
                        _not_null_precondition(
                            statement, argument, node.find_ancestor(exp.Select)
                        )
                    )
                elif not isinstance(argument, exp.Distinct):
                    shape["count_star"] += 1  # COUNT(*) and COUNT(<constant>) count rows
    shape["not_in"] = not_in
    shape["null_tests"] = [
        test for statement in statements for test in _null_existence_tests(statement)
    ]
    shape["counted_columns"] = counted
    return shape


def _requirement(
    code: str,
    reason: str,
    required_kinds: list[str],
    preconditions: list[dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    concrete = [item for item in preconditions or [] if item is not None]
    resolved = bool(preconditions) and len(concrete) == len(preconditions or [])
    return {
        "code": code,
        "reason": reason,
        "required_kinds": sorted(required_kinds),
        "resolved": resolved,
        "preconditions": concrete if resolved else [],
    }


def rewrite_requirements(baseline_sql: str, candidate_sql: str) -> list[dict[str, Any]]:
    """Name the data properties a rewrite depends on, from how its shape changed.

    These rewrites return the same rows on a data set that happens to have no
    NULL, no duplicate and no orphan row, and different rows on one that does.
    A result match cannot tell the two apart; only an enforced constraint can.
    The list is a floor, not a ceiling: an empty list does not prove a rewrite safe.
    """

    candidate_selects = _batch_selects(candidate_sql)
    baseline_selects = _batch_selects(baseline_sql)
    if candidate_selects is not None or baseline_selects is not None:
        # Each branch replaces the whole baseline statement for some parameter values.
        if baseline_selects is None:
            pairs = [(baseline_sql, select) for select in candidate_selects or ()]
        elif candidate_selects is None:
            pairs = [(select, candidate_sql) for select in baseline_selects]
        elif len(baseline_selects) == len(candidate_selects):
            pairs = list(zip(baseline_selects, candidate_selects))
        else:
            pairs = []
        by_code: dict[str, dict[str, Any]] = {}
        for baseline_select, candidate_select in pairs:
            for requirement in rewrite_requirements(baseline_select, candidate_select):
                by_code.setdefault(requirement["code"], requirement)
        return list(by_code.values())
    baseline = _shape(_parse_statements(baseline_sql))
    candidate = _shape(_parse_statements(candidate_sql))

    requirements: list[dict[str, Any]] = []
    if len(baseline["not_in"]) != len(candidate["not_in"]):
        with_not_in, without = (
            (baseline, candidate)
            if len(baseline["not_in"]) > len(candidate["not_in"])
            else (candidate, baseline)
        )
        preconditions, handled = _not_in_preconditions(
            with_not_in["not_in"], without["null_tests"]
        )
        requirement = _requirement(
            "not_in_null_semantics",
            "NOT IN returns no row when the subquery yields a NULL and drops a row whose "
            "tested value is NULL; NOT EXISTS and an anti-join keep those rows. The two are "
            "equal only when both compared columns are NOT NULL, or when the statement "
            "tests the NULL case itself.",
            ["not_null"],
            preconditions,
        )
        if handled:
            requirement["handled_by_statement"] = handled
        requirements.append(requirement)
    changed_counts = set(baseline["counted_columns"]) ^ set(candidate["counted_columns"])
    if changed_counts and baseline["count_star"] != candidate["count_star"]:
        both = {**candidate["counted_columns"], **baseline["counted_columns"]}
        requirements.append(
            _requirement(
                "count_null_semantics",
                "COUNT(column) skips NULLs and COUNT(*) counts every row. The two are equal "
                "only when the column is NOT NULL.",
                ["not_null"],
                [both[name] for name in sorted(changed_counts)],
            )
        )
    if baseline["distinct"] > candidate["distinct"]:
        requirements.append(
            _requirement(
                "distinct_removed",
                "DISTINCT was removed. The results are equal only when the joins cannot "
                "return a duplicate row, which needs a unique key on the joined columns.",
                ["unique"],
            )
        )
    semi_delta = candidate["semi_joins"] - baseline["semi_joins"]
    join_delta = candidate["joins"] - baseline["joins"]
    if semi_delta * join_delta < 0:
        requirements.append(
            _requirement(
                "exists_join_swap",
                "EXISTS or IN returns each outer row once; a JOIN returns it once per match. "
                "The two are equal only when the joined columns are a unique key.",
                ["unique"],
            )
        )
    if baseline["outer_joins"] != candidate["outer_joins"]:
        requirements.append(
            _requirement(
                "outer_join_changed",
                "An outer join keeps rows that have no match; an inner join or a removed join "
                "does not. The two are equal only when a trusted foreign key on a NOT NULL "
                "column guarantees a match.",
                ["trusted_foreign_key", "not_null"],
            )
        )
    return requirements


def has_outer_literal_top_zero(sql: str) -> bool:
    """Return whether the final query has an outer literal ``TOP (0)``.

    This is deliberately narrower than proving that an arbitrary predicate
    returns no rows. Parameterized limits, non-zero limits, and limits that
    occur only inside a nested query are not accepted.
    """

    if _batch_selects(sql) is not None:
        return False
    statements = _parse_statements(sql)
    if not statements:
        return False
    if any(not isinstance(statement, exp.Declare) for statement in statements[:-1]):
        return False

    outer_limit = statements[-1].args.get("limit")
    if not isinstance(outer_limit, exp.Limit):
        return False
    limit_expression = outer_limit.args.get("expression")
    return (
        isinstance(limit_expression, exp.Literal)
        and not limit_expression.is_string
        and str(limit_expression.this) == "0"
    )


OrderKeys = tuple[tuple[str, bool], ...]


def outer_order_by_keys(
    sql: str,
    relation: tuple[str, str, str] | None = None,
) -> tuple[OrderKeys | None, ...]:
    """Return the outer ORDER BY keys of each result-returning statement.

    Each entry is ``None`` when that statement has no outer ORDER BY. An
    ORDER BY inside a derived table, CTE, TOP, or window does not count:
    only the outermost one promises an order to the caller. A read-only
    branching batch has one entry per branch. ``relation`` (schema, name,
    SELECT body) is a view the statement reads: it is read as its definition
    (``inline_relation``), in every branch of a batch.
    """

    selects = _batch_selects(sql)
    if selects is not None:
        return tuple(
            keys for select in selects for keys in outer_order_by_keys(select, relation)
        )
    if relation is not None:
        sql = inline_relation(sql, *relation) or sql
    statements = _parse_statements(sql)
    orders: list[OrderKeys | None] = []
    for statement in statements:
        if not isinstance(statement, exp.Query) or statement.args.get("into"):
            continue
        order = statement.args.get("order")
        last_member = statement
        while order is None and isinstance(last_member, exp.SetOperation):
            # sqlglot attaches a T-SQL UNION's trailing ORDER BY to the last
            # member; T-SQL applies it to the whole result.
            last_member = last_member.expression
            order = last_member.args.get("order")
        if not isinstance(order, exp.Order) or not order.expressions:
            orders.append(None)
            continue
        scope = last_member if isinstance(last_member, exp.Select) and last_member is statement else None
        ctes = _statement_ctes(statement)
        orders.append(
            tuple(_order_key(ordered, scope, ctes) for ordered in order.expressions)
        )
    return tuple(orders)


def resolve_compare_order(sql: str) -> bool:
    """Return whether result order is contractual for this SQL."""

    try:
        return any(keys is not None for keys in outer_order_by_keys(sql))
    except ValueError:
        # Unparseable SQL keeps the stricter comparison; an order-only
        # difference is reported as unproven, never as a result difference.
        return True


def effective_compare_order(sql: str, requested: bool | None) -> bool:
    """Resolve ``compare_order``; an outer ORDER BY cannot be waived."""

    if resolve_compare_order(sql):
        return True
    return bool(requested)


def order_contract_risk(
    baseline_sql: str,
    candidate_sql: str,
    *,
    candidate_relation: tuple[str, str, str] | None = None,
) -> dict[str, Any] | None:
    """Return a blocking risk when the candidate weakens the baseline order.

    A sandbox run can return key order by accident, so result comparison
    cannot detect a dropped or changed outer ORDER BY. The candidate must keep
    the baseline keys, in order and direction, as a prefix of its own keys.
    ``candidate_relation`` is a view the candidate reads, as (schema, name,
    SELECT body): its columns are then the expressions they store.
    """

    candidate_orders = outer_order_by_keys(candidate_sql, candidate_relation)
    baseline_orders = outer_order_by_keys(baseline_sql)
    if _batch_selects(candidate_sql) is not None or _batch_selects(baseline_sql) is not None:
        # Any branch can be the one that runs, so every candidate branch must
        # keep the order every baseline branch promises.
        required = list(dict.fromkeys(keys for keys in baseline_orders if keys is not None))
        if not required:
            return None
        reason = None
        branch = None
        # removed: a branch returns rows in no promised order, which no
        # comparison can show. keys_differ: every branch still orders its rows.
        change = "keys_differ"
        if len(required) > 1:
            reason = (
                "baseline branches have different outer ORDER BY keys, so the order "
                "contract of the batch cannot be compared"
            )
        if any(candidate_keys is None for candidate_keys in candidate_orders):
            change = "removed"
        for ordinal, candidate_keys in enumerate(candidate_orders, start=1):
            if reason is not None:
                break
            if candidate_keys is None:
                reason = f"candidate branch {ordinal} removes the baseline outer ORDER BY"
            elif candidate_keys[: len(required[0])] != required[0]:
                reason = (
                    f"candidate branch {ordinal} outer ORDER BY does not start with the "
                    "baseline keys"
                )
            branch = ordinal if reason is not None else None
        if reason is None:
            return None
        return {
            "risk": "order_contract_changed",
            "source": "baseline_vs_candidate",
            "result_set_ordinal": 1,
            **({"branch": branch} if branch is not None else {}),
            "change": change,
            "reason": reason,
            "proof_requirement": (
                "keep the baseline outer ORDER BY keys and directions as a "
                "prefix of the outer ORDER BY of every candidate branch"
            ),
        }
    for ordinal, baseline_keys in enumerate(
        baseline_orders,
        start=1,
    ):
        if baseline_keys is None:
            continue
        candidate_keys = (
            candidate_orders[ordinal - 1]
            if ordinal <= len(candidate_orders)
            else None
        )
        if candidate_keys is None:
            reason = "candidate removes the baseline outer ORDER BY"
        elif candidate_keys[: len(baseline_keys)] != baseline_keys:
            reason = "candidate outer ORDER BY does not start with the baseline keys"
        else:
            continue
        return {
            "risk": "order_contract_changed",
            "source": "baseline_vs_candidate",
            "result_set_ordinal": ordinal,
            # removed: no comparison can show an order that is not asked for.
            # keys_differ: the candidate still orders its rows; the keys could
            # not be matched as text (another alias, a derived table).
            "change": "removed" if candidate_keys is None else "keys_differ",
            "reason": reason,
            "proof_requirement": (
                "keep the baseline outer ORDER BY keys and directions as a "
                "prefix of the candidate outer ORDER BY"
            ),
        }
    return None


def _order_sources(select: exp.Select) -> dict[str, str] | None:
    """Map each FROM/JOIN source name or alias to the table it names."""

    sources: dict[str, str] = {}
    from_clause = select.args.get("from") or select.args.get("from_")
    items = [from_clause.this] if from_clause is not None else []
    items += [join.this for join in select.args.get("joins") or []]
    for item in items:
        if isinstance(item, exp.Table):
            target = ".".join(
                part.casefold() for part in (item.catalog, item.db, item.name) if part
            )
            sources[(item.alias_or_name or item.name).casefold()] = target
            sources.setdefault(target, target)
        elif item is not None and item.alias_or_name:
            # Derived table or other source: its alias is its identity.
            sources[item.alias_or_name.casefold()] = item.alias_or_name.casefold()
        else:
            return None
    return sources


def _order_relations(
    select: exp.Select,
    ctes: Mapping[str, exp.CTE | None],
) -> dict[str, exp.Select]:
    """The plain SELECT behind each derived table or CTE in the FROM clause, by alias."""

    relations: dict[str, exp.Select] = {}
    from_clause = select.args.get("from") or select.args.get("from_")
    items = [from_clause.this] if from_clause is not None else []
    items += [join.this for join in select.args.get("joins") or []]
    for item in items:
        inner: exp.Expr | None = None
        if isinstance(item, exp.Subquery):
            inner = item.this
        elif isinstance(item, exp.Table) and not item.db:
            cte = ctes.get(item.name.casefold())
            inner = cte.this if cte is not None else None
        if (
            isinstance(inner, exp.Select)
            and item.alias_or_name
            and not any(
                isinstance(projection, exp.Star) or isinstance(projection.this, exp.Star)
                for projection in inner.expressions
            )
        ):
            relations[item.alias_or_name.casefold()] = inner
    return relations


def _resolve_order_expression(
    expression: exp.Expr,
    select: exp.Select,
    ctes: Mapping[str, exp.CTE | None],
) -> exp.Expr:
    """Rewrite an ORDER BY key to the column it binds to, when that is certain.

    T-SQL binds an ORDER BY ordinal to the select list, an unqualified name to a
    select-list name first, and a qualifier to its FROM source. A column of a
    derived table or CTE that is a direct copy of a base-table column resolves
    to that base column, so a rewrite that only moves the rows through such a
    relation keeps the same key. Anything that cannot be resolved for certain
    is returned unchanged, so the comparison stays at least as strict as plain
    text.
    """

    projections = list(select.expressions)
    if any(isinstance(item, exp.Star) or isinstance(item.this, exp.Star) for item in projections):
        return expression
    if isinstance(expression, exp.Literal) and expression.is_int:
        position = int(expression.name)
        if 1 <= position <= len(projections):
            expression = projections[position - 1].unalias().copy()
        else:
            return expression
    elif isinstance(expression, exp.Column) and not expression.table:
        named = [
            item
            for item in projections
            if isinstance(item, (exp.Alias, exp.Column))
            and item.alias_or_name.casefold() == expression.name.casefold()
        ]
        if len(named) == 1:
            expression = named[0].unalias().copy()
    sources = _order_sources(select)
    if not sources:
        return expression
    targets = set(sources.values())
    try:
        identity: _RowIdentity | None = _row_identity(select, ctes)
    except _NoRowKey:
        identity = None
    relations = _order_relations(select, ctes)

    def qualify(node: exp.Expr) -> exp.Expr:
        if not isinstance(node, exp.Column) or isinstance(node.this, exp.Star):
            return node
        base = identity.base_column(identity.resolve(node)) if identity is not None else None
        if base is not None:
            return exp.Column(
                this=exp.to_identifier(base["column"]),
                table=exp.to_identifier(f"{base['schema']}.{base['table']}".casefold()),
            )
        # A column of a derived table, CTE or inlined view that is an
        # expression there (the alias of a GROUP BY expression) is that
        # expression: ordering by the column orders by its values.
        inner = (
            relations.get(node.table.casefold())
            if node.table
            else next(iter(relations.values()))
            if len(sources) == len(relations) == 1
            else None
        )
        if inner is not None:
            named = [
                item
                for item in inner.expressions
                if isinstance(item, (exp.Alias, exp.Column))
                and item.alias_or_name.casefold() == node.name.casefold()
            ]
            if len(named) == 1 and not any(
                isinstance(part, (exp.AggFunc, exp.Window, exp.Subquery))
                for part in named[0].walk()
            ):
                return exp.Paren(
                    this=_resolve_order_expression(named[0].unalias().copy(), inner, ctes)
                )
        qualifier = ".".join(
            part.casefold() for part in (node.catalog, node.db, node.table) if part
        )
        if qualifier:
            target = sources.get(qualifier) or sources.get(node.table.casefold())
        else:
            target = next(iter(targets)) if len(targets) == 1 else None
        if target is None:
            return node
        # One opaque identifier: the resolved source is compared, not re-parsed.
        return exp.Column(this=exp.to_identifier(node.name), table=exp.to_identifier(target))

    return expression.transform(qualify)


def _order_key(
    ordered: exp.Expr,
    select: exp.Select | None = None,
    ctes: Mapping[str, exp.CTE | None] | None = None,
) -> tuple[str, bool]:
    expression = ordered.this.copy() if isinstance(ordered, exp.Ordered) else ordered.copy()
    if select is not None:
        expression = _resolve_order_expression(expression, select, ctes or {})
    # Parentheses around a whole key or a whole operand change nothing.
    expression = expression.transform(
        lambda node: node.this
        if isinstance(node, exp.Paren)
        and isinstance(node.this, (exp.Column, exp.Func, exp.Cast, exp.Paren))
        else node
    )
    # Identifier case and quoting do not change T-SQL name resolution.
    for identifier in expression.find_all(exp.Identifier):
        identifier.set("this", identifier.name.casefold())
        identifier.set("quoted", False)
    return (
        expression.sql(dialect="tsql"),
        bool(isinstance(ordered, exp.Ordered) and ordered.args.get("desc")),
    )


def _function_name(node: exp.Expr) -> str | None:
    if isinstance(node, exp.CurrentTimestampLTZ):
        return "SYSDATETIMEOFFSET"
    if isinstance(node, exp.CurrentTimestamp):
        return "CURRENT_TIMESTAMP"
    if isinstance(node, exp.Uuid):
        return "NEWID"
    if isinstance(node, exp.Rand):
        return "RAND" if node.args.get("this") is None else None
    if isinstance(node, exp.Anonymous):
        name = str(node.name or "").strip().upper()
        return name or None
    return None
