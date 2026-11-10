from __future__ import annotations

import re
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from sqlglot import exp
from sqlglot import parse
from sqlglot.errors import ParseError

from .param_binding import _mask_parameter_non_code


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


def analyze_equivalence_preflight(sql: str) -> EquivalencePreflight:
    """Detect SQL shapes that cannot prove semantics by sequential snapshots.

    This intentionally does not rewrite SQL. A performance run must continue to
    measure the production-shaped statement, while any deterministic proof is a
    separately declared and scoped contract.
    """

    statements = _parse_statements(sql)

    clock_functions: set[str] = set()
    row_volatile_functions: set[str] = set()
    nonrepeatable_table_samples: list[dict[str, Any]] = []
    unordered_limits: list[dict[str, Any]] = []
    ordered_limits_without_total_order: list[dict[str, Any]] = []
    window_orders_without_total_order: list[dict[str, Any]] = []

    isolation_hints: set[str] = set()

    for statement_ordinal, statement in enumerate(statements, start=1):
        select_ordinal = 0
        window_ordinal = 0
        cte_names = {
            cte.alias_or_name.casefold()
            for cte in statement.find_all(exp.CTE)
            if cte.alias_or_name
        }
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
    cte_names: set[str],
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


def _total_order_requirement(
    select: exp.Select,
    keys: list[exp.Expr],
    cte_names: set[str],
    *,
    resolve_projection: bool,
) -> dict[str, Any] | None:
    """What the ordering keys must cover for the row order to have no ties.

    Returns None when the ordering is deterministic from the SQL alone: the
    SELECT is grouped and the keys cover every grouping column. Otherwise the
    result lists, for each base table, the plain columns the keys contribute;
    the catalog decides whether they cover a unique NOT NULL key.
    """

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

    group = select.args.get("group")
    if group is not None:
        grouped = list(group.expressions)
        ordered = {(item.table.casefold(), item.name.casefold()) for item in key_columns}
        if grouped and all(
            isinstance(item, exp.Column)
            and (item.table.casefold(), item.name.casefold()) in ordered
            for item in grouped
        ):
            return None
        return {
            "provable": False,
            "unprovable_reason": (
                "the rows are groups; the ordering keys do not include every GROUP BY column"
            ),
        }

    sources, reason = _select_sources(select, cte_names)
    if reason is not None:
        return {"provable": False, "unprovable_reason": reason}
    columns_by_alias: dict[str, list[str]] = {source["alias"]: [] for source in sources}
    for column in key_columns:
        source = _source_of(column, sources)
        if source is None:
            return {
                "provable": False,
                "unprovable_reason": (
                    f"ordering column {column.name} is not qualified with its table alias"
                ),
            }
        if column.name not in columns_by_alias[source["alias"]]:
            columns_by_alias[source["alias"]].append(column.name)
    return {
        "provable": True,
        "tables": [
            {**source, "order_columns": columns_by_alias[source["alias"]]}
            for source in sources
        ],
    }


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
    """

    code = _mask_parameter_non_code(sql).upper()
    plan_forcing: set[str] = set()
    isolation: set[str] = set()
    compile_options: set[str] = set()
    for match in re.finditer(r"\bOPTION\s*\(", code):
        for item in _hint_items(code, match.end() - 1):
            name = " ".join(re.match(r"[A-Z_ ]*", item).group(0).split())  # type: ignore[union-attr]
            compile_option = next(
                (option for option in _COMPILE_OPTIONS if name.startswith(option)),
                None,
            )
            if compile_option is not None:
                compile_options.add(compile_option)
            elif name:
                plan_forcing.add(name)
    for match in re.finditer(r"\bWITH\s*\(", code):
        for item in _hint_items(code, match.end() - 1):
            word = re.match(r"[A-Z_]*", item).group(0)  # type: ignore[union-attr]
            if word in _ISOLATION_TABLE_HINTS:
                isolation.add(word)
            elif word in _PLAN_FORCING_TABLE_HINTS:
                plan_forcing.add(word)
    # The deprecated form: FROM dbo.T (NOLOCK), without WITH.
    for match in re.finditer(r"\(\s*(NOLOCK|READUNCOMMITTED|READPAST)\s*\)", code):
        isolation.add(match.group(1))
    for match in re.finditer(r"\b(LOOP|HASH|MERGE|REMOTE)\s+JOIN\b", code):
        plan_forcing.add(f"{match.group(1)} JOIN")
    return {
        "hinted": bool(plan_forcing),
        "plan_forcing_hints": sorted(plan_forcing),
        "isolation_table_hints": sorted(isolation),
        "compile_options": sorted(compile_options),
    }


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


def _shape(statements: list[exp.Expr]) -> dict[str, Any]:
    """Count the constructs whose meaning depends on NULLs, duplicates or missing rows."""

    not_in: list[list[dict[str, Any] | None]] = []
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
                        [
                            _not_null_precondition(
                                statement,
                                projections[0] if len(projections) == 1 else None,
                                subquery if isinstance(subquery, exp.Select) else None,
                            ),
                            _not_null_precondition(
                                statement, node.this, node.find_ancestor(exp.Select)
                            ),
                        ]
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

    baseline = _shape(_parse_statements(baseline_sql))
    candidate = _shape(_parse_statements(candidate_sql))

    requirements: list[dict[str, Any]] = []
    if len(baseline["not_in"]) != len(candidate["not_in"]):
        longer = max(baseline["not_in"], candidate["not_in"], key=len)
        requirements.append(
            _requirement(
                "not_in_null_semantics",
                "NOT IN returns no row when the subquery yields a NULL and drops a row whose "
                "tested value is NULL; NOT EXISTS and an anti-join keep those rows. The two are "
                "equal only when both compared columns are NOT NULL.",
                ["not_null"],
                [item for pair in longer for item in pair],
            )
        )
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


def outer_order_by_keys(sql: str) -> tuple[OrderKeys | None, ...]:
    """Return the outer ORDER BY keys of each result-returning statement.

    Each entry is ``None`` when that statement has no outer ORDER BY. An
    ORDER BY inside a derived table, CTE, TOP, or window does not count:
    only the outermost one promises an order to the caller.
    """

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
        orders.append(
            tuple(_order_key(ordered, scope) for ordered in order.expressions)
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
) -> dict[str, Any] | None:
    """Return a blocking risk when the candidate weakens the baseline order.

    A sandbox run can return key order by accident, so result comparison
    cannot detect a dropped or changed outer ORDER BY. The candidate must keep
    the baseline keys, in order and direction, as a prefix of its own keys.
    """

    candidate_orders = outer_order_by_keys(candidate_sql)
    for ordinal, baseline_keys in enumerate(
        outer_order_by_keys(baseline_sql),
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


def _resolve_order_expression(expression: exp.Expr, select: exp.Select) -> exp.Expr:
    """Rewrite an ORDER BY key to the column it binds to, when that is certain.

    T-SQL binds an ORDER BY ordinal to the select list, an unqualified name to a
    select-list alias first, and a qualifier to its FROM source. Anything that
    cannot be resolved for certain is returned unchanged, so the comparison
    stays at least as strict as plain text.
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
        for item in projections:
            if isinstance(item, exp.Alias) and item.alias.casefold() == expression.name.casefold():
                expression = item.this.copy()
                break
    sources = _order_sources(select)
    if not sources:
        return expression
    targets = set(sources.values())

    def qualify(node: exp.Expr) -> exp.Expr:
        if not isinstance(node, exp.Column) or isinstance(node.this, exp.Star):
            return node
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


def _order_key(ordered: exp.Expr, select: exp.Select | None = None) -> tuple[str, bool]:
    expression = ordered.this.copy() if isinstance(ordered, exp.Ordered) else ordered.copy()
    if select is not None:
        expression = _resolve_order_expression(expression, select)
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
