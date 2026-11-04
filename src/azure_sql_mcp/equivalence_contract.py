from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any

from sqlglot import exp
from sqlglot import parse
from sqlglot.errors import ParseError


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


def analyze_equivalence_preflight(sql: str) -> EquivalencePreflight:
    """Detect SQL shapes that cannot prove semantics by sequential snapshots.

    This intentionally does not rewrite SQL. A performance run must continue to
    measure the production-shaped statement, while any deterministic proof is a
    separately declared and scoped contract.
    """

    try:
        statements = [statement for statement in parse(sql, read="tsql") if statement]
    except ParseError as exc:
        raise ValueError("SQL could not be parsed for equivalence preflight.") from exc

    clock_functions: set[str] = set()
    row_volatile_functions: set[str] = set()
    nonrepeatable_table_samples: list[dict[str, Any]] = []
    unordered_limits: list[dict[str, Any]] = []
    ordered_limits_without_total_order: list[dict[str, Any]] = []
    window_orders_without_total_order: list[dict[str, Any]] = []

    for statement_ordinal, statement in enumerate(statements, start=1):
        select_ordinal = 0
        window_ordinal = 0
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

            if isinstance(node, exp.Window):
                window_ordinal += 1
                if node.args.get("order") is not None:
                    window_orders_without_total_order.append(
                        {
                            "statement_ordinal": statement_ordinal,
                            "window_ordinal": window_ordinal,
                            "risk": "window_order_total_order_unproven",
                            "proof_requirement": (
                                "verify that window ORDER BY keys form a unique "
                                "total order"
                            ),
                        }
                    )

            if not isinstance(node, exp.Select):
                continue
            select_ordinal += 1
            if node.args.get("limit") is not None:
                if node.args.get("order") is None:
                    unordered_limits.append(
                        {
                            "statement_ordinal": statement_ordinal,
                            "select_ordinal": select_ordinal,
                            "risk": "unordered_row_limit",
                        }
                    )
                else:
                    ordered_limits_without_total_order.append(
                        {
                            "statement_ordinal": statement_ordinal,
                            "select_ordinal": select_ordinal,
                            "risk": "row_limit_total_order_unproven",
                            "proof_requirement": (
                                "verify that ORDER BY keys form a unique total order"
                            ),
                        }
                    )

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


def has_outer_literal_top_zero(sql: str) -> bool:
    """Return whether the final query has an outer literal ``TOP (0)``.

    This is deliberately narrower than proving that an arbitrary predicate
    returns no rows. Parameterized limits, non-zero limits, and limits that
    occur only inside a nested query are not accepted.
    """

    try:
        statements = [statement for statement in parse(sql, read="tsql") if statement]
    except ParseError as exc:
        raise ValueError("SQL could not be parsed for equivalence preflight.") from exc
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

    try:
        statements = [statement for statement in parse(sql, read="tsql") if statement]
    except ParseError as exc:
        raise ValueError("SQL could not be parsed for equivalence preflight.") from exc
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
