"""Validation for Query Store hint strings (``sys.sp_query_store_set_hints``).

The hints string is the one free-text surface in the plan-enforcement toolset, so it is
validated against a strict allowlist grammar before it is ever sent to the engine:

- The whole string must be a single ``OPTION ( hint [, hint]* )`` clause.
- Each hint must match one of the documented, Query Store-supported hint shapes below.
- No statement terminators, comments, or stray quoting anywhere; ``OPTIMIZE FOR`` values
  and ``USE HINT`` names are confined to tight sub-grammars.

This is deliberately narrower than what ``sp_query_store_set_hints`` itself would accept:
anything not on the list is rejected here with a clear error rather than passed through.
"""

from __future__ import annotations

import re

# Documented USE HINT names (Azure SQL Database). Compatibility-level variants are
# matched by pattern below rather than enumerated.
_USE_HINT_NAMES = frozenset(
    {
        "ASSUME_JOIN_PREDICATE_DEPENDS_ON_FILTERS",
        "ASSUME_MIN_SELECTIVITY_FOR_FILTER_ESTIMATES",
        "ASSUME_FULL_INDEPENDENCE_FOR_FILTER_ESTIMATES",
        "ASSUME_PARTIAL_CORRELATION_FOR_FILTER_ESTIMATES",
        "DISABLE_BATCH_MODE_ADAPTIVE_JOINS",
        "DISABLE_BATCH_MODE_MEMORY_GRANT_FEEDBACK",
        "DISABLE_DEFERRED_COMPILATION_TV",
        "DISABLE_INTERLEAVED_EXECUTION_TVF",
        "DISABLE_OPTIMIZED_NESTED_LOOP",
        "DISABLE_OPTIMIZER_ROWGOAL",
        "DISABLE_PARAMETER_SNIFFING",
        "DISABLE_ROW_MODE_MEMORY_GRANT_FEEDBACK",
        "DISABLE_TSQL_SCALAR_UDF_INLINING",
        "DISALLOW_BATCH_MODE",
        "ENABLE_HIST_AMENDMENT_FOR_ASC_KEYS",
        "ENABLE_QUERY_OPTIMIZER_HOTFIXES",
        "FORCE_DEFAULT_CARDINALITY_ESTIMATION",
        "FORCE_LEGACY_CARDINALITY_ESTIMATION",
        "QUERY_PLAN_PROFILE",
    }
)
_COMPAT_LEVEL_PATTERN = re.compile(r"^QUERY_OPTIMIZER_COMPATIBILITY_LEVEL_\d{3}$")

# OPTIMIZE FOR (@p = <literal> | @p UNKNOWN, ...): literals are tight — numbers or
# single-quoted strings with no embedded quotes/terminators.
_OPTIMIZE_FOR_VALUE = r"(?:UNKNOWN|=\s*(?:N?'[^'\r\n;]*'|[+-]?\d+(?:\.\d+)?))"
_OPTIMIZE_FOR_ARG = rf"@\w+\s+{_OPTIMIZE_FOR_VALUE}"

# One regex per allowed hint shape (case-insensitive, anchored per-hint).
_HINT_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(rf"^{pattern}$", re.IGNORECASE)
    for pattern in (
        r"RECOMPILE",
        r"OPTIMIZE\s+FOR\s+UNKNOWN",
        rf"OPTIMIZE\s+FOR\s*\(\s*{_OPTIMIZE_FOR_ARG}(?:\s*,\s*{_OPTIMIZE_FOR_ARG})*\s*\)",
        r"MAXDOP\s+\d+",
        r"MAX_GRANT_PERCENT\s*=\s*\d+(?:\.\d+)?",
        r"MIN_GRANT_PERCENT\s*=\s*\d+(?:\.\d+)?",
        r"FAST\s+\d+",
        r"MAXRECURSION\s+\d+",
        r"KEEP\s+PLAN",
        r"KEEPFIXED\s+PLAN",
        r"FORCE\s+ORDER",
        r"EXPAND\s+VIEWS",
        r"IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX",
        r"NO_PERFORMANCE_SPOOL",
        r"(?:HASH|ORDER)\s+GROUP",
        r"(?:CONCAT|HASH|MERGE)\s+UNION",
        r"(?:LOOP|MERGE|HASH)\s+JOIN",
        r"USE\s+HINT\s*\(\s*'[A-Za-z0-9_]+'(?:\s*,\s*'[A-Za-z0-9_]+')*\s*\)",
    )
)

_OPTION_WRAPPER = re.compile(r"^OPTION\s*\((.*)\)$", re.IGNORECASE | re.DOTALL)
_FORBIDDEN = re.compile(r";|--|/\*|\*/|\[|\]")
_USE_HINT_INNER = re.compile(r"'([A-Za-z0-9_]+)'")


def _split_top_level(body: str) -> list[str]:
    """Split on commas that are not inside parentheses."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(depth - 1, 0)
        if char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    parts.append("".join(current).strip())
    return [part for part in parts if part]


def validate_query_hints(hints: str) -> str:
    """Validate and normalize a Query Store hints string.

    Returns the trimmed string on success; raises ``ValueError`` with the reason on
    failure. The returned value is what should be passed to ``sp_query_store_set_hints``.
    """
    if not isinstance(hints, str) or not hints.strip():
        raise ValueError("query_hints must be a non-empty OPTION(...) clause.")
    trimmed = hints.strip()

    if _FORBIDDEN.search(trimmed):
        raise ValueError(
            "query_hints must not contain statement terminators, comments, or brackets."
        )

    wrapper = _OPTION_WRAPPER.match(trimmed)
    if not wrapper:
        raise ValueError("query_hints must be a single OPTION(...) clause, e.g. OPTION(RECOMPILE).")

    body = wrapper.group(1).strip()
    if not body:
        raise ValueError("OPTION(...) must contain at least one hint.")

    for hint in _split_top_level(body):
        if not any(pattern.match(hint) for pattern in _HINT_PATTERNS):
            raise ValueError(f"unsupported or malformed query hint: {hint!r}")
        if re.match(r"^USE\s+HINT", hint, re.IGNORECASE):
            for name in _USE_HINT_INNER.findall(hint):
                upper = name.upper()
                if upper not in _USE_HINT_NAMES and not _COMPAT_LEVEL_PATTERN.match(upper):
                    raise ValueError(f"unsupported USE HINT name: {name!r}")

    return trimmed
