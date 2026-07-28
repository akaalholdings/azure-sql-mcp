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
