from __future__ import annotations

import re
from collections import Counter
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from typing import cast

from sqlglot import exp
from sqlglot import parse
from sqlglot.errors import ParseError

from .equivalence_contract import EquivalencePreflight
from .equivalence_contract import _CLOCK_FUNCTIONS
from .equivalence_contract import _ROW_VOLATILE_FUNCTIONS
from .equivalence_contract import analyze_equivalence_preflight

MAX_VIEW_DEPENDENCY_DEPTH = 8

ViewDefinitionResolver = Callable[
    [str, str, str],
    Awaitable[Mapping[str, Any] | Sequence[Mapping[str, Any]] | str | None],
]
ExecutorCompatibleFetcher = Callable[
    [str, str, Sequence[Any] | None],
    Awaitable[Sequence[Mapping[str, Any]]],
]

_VIEW_DEFINITION_QUERY = """
SELECT
    s.name AS schema_name,
    o.name AS object_name,
    o.type AS object_type_code,
    o.type_desc AS object_type,
    m.definition,
    CASE
        WHEN OBJECTPROPERTYEX(o.object_id, 'IsEncrypted') = 1 THEN 1
        ELSE 0
    END AS is_encrypted
FROM sys.objects AS o
INNER JOIN sys.schemas AS s
    ON s.schema_id = o.schema_id
LEFT JOIN sys.sql_modules AS m
    ON m.object_id = o.object_id
WHERE o.object_id = CASE
    WHEN ? = N'' THEN OBJECT_ID(QUOTENAME(?))
    ELSE OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?))
END
"""

_CREATE_VIEW_HEADER = re.compile(
    r"^\s*(?:CREATE|ALTER)\s+(?:OR\s+ALTER\s+)?VIEW\b.*?\bAS\s+",
    re.IGNORECASE | re.DOTALL,
)
_VIEW_TYPE_VALUES = frozenset({"V", "VIEW", "SQL_VIEW"})
_DYNAMIC_EXTERNAL_SOURCE = "dynamic_external_source"
_NON_VIEW_TYPE_VALUES = frozenset(
    {
        "AF",
        "FN",
        "FS",
        "FT",
        "IF",
        "P",
        "PROCEDURE",
        "SQL_INLINE_TABLE_VALUED_FUNCTION",
        "SQL_SCALAR_FUNCTION",
        "SQL_STORED_PROCEDURE",
        "SQL_TABLE_VALUED_FUNCTION",
        "TABLE",
        "U",
        "USER_TABLE",
    }
)


@dataclass(frozen=True, slots=True)
class FunctionVerdict:
    function: str
    source: str
    count: int
    verdict: str
    reason: str
    category: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "function": self.function,
            "source": self.source,
            "count": self.count,
            "verdict": self.verdict,
            "reason": self.reason,
            "category": self.category,
        }


@dataclass(frozen=True, slots=True)
class DatabaseEquivalencePreflight:
    """Database-aware equivalence facts without retaining SQL definitions."""

    analysis_coverage: Mapping[str, Any]
    risk_codes: tuple[str, ...] = ()
    risks: tuple[Mapping[str, Any], ...] = ()
    function_verdicts: tuple[FunctionVerdict, ...] = ()
    resolved_view_dependencies: tuple[Mapping[str, Any], ...] = ()
    unresolved_dependencies: tuple[Mapping[str, Any], ...] = ()
    contract_version: int = 2

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
        coverage = dict(self.analysis_coverage)
        functions = [verdict.as_dict() for verdict in self.function_verdicts]
        resolved_dependencies = [dict(item) for item in self.resolved_view_dependencies]
        return {
            "contract_version": self.contract_version,
            "classification": self.classification,
            "direct_snapshot_supported": self.direct_snapshot_supported,
            "coverage_complete": bool(coverage.get("complete")),
            "analysis_scope": "query_and_recursive_view_definitions",
            "analysis_coverage": coverage,
            "risk_codes": list(self.risk_codes),
            "risks": [dict(item) for item in self.risks],
            "functions": [dict(item) for item in functions],
            "function_verdicts": functions,
            "resolved_dependencies": [dict(item) for item in resolved_dependencies],
            "resolved_view_dependencies": resolved_dependencies,
            "unresolved_dependencies": [
                dict(item) for item in self.unresolved_dependencies
            ],
        }


@dataclass(frozen=True, slots=True)
class _ObjectReference:
    schema_name: str
    object_name: str
    source: str
    catalog_name: str = ""

    @property
    def key(self) -> tuple[str, str, str]:
        return (
            self.catalog_name,
            self.schema_name,
            self.object_name,
        )


@dataclass(frozen=True, slots=True)
class _Resolution:
    status: str
    definition: str | None = None
    schema_name: str | None = None
    object_name: str | None = None


class EquivalencePreflightService:
    """Run direct analysis and recursively inspect database view definitions.

    The identifier resolver receives (database_name, schema_name, object_name)
    and returns a definition string or metadata mapping. A mapping should
    include object_type and may include definition, is_encrypted, or
    accessible. Returning a table/object type skips view traversal.

    The executor may provide fetch_all or be a callback accepting
    (database_name, query, params). Only metadata is queried through this path.
    """

    def __init__(
        self,
        resolver: ViewDefinitionResolver | Any | None = None,
        *,
        executor: Any | None = None,
        max_depth: int = MAX_VIEW_DEPENDENCY_DEPTH,
    ) -> None:
        if resolver is not None and executor is not None:
            raise ValueError("Provide resolver or executor, not both.")
        if not isinstance(max_depth, int) or isinstance(max_depth, bool):
            raise ValueError("max_depth must be an integer from 0 through 8.")
        if not 0 <= max_depth <= MAX_VIEW_DEPENDENCY_DEPTH:
            raise ValueError("max_depth must be an integer from 0 through 8.")
        if executor is None and _has_fetch_all(resolver):
            executor = resolver
            resolver = None
        self.resolver = resolver
        self.executor = executor
        self.max_depth = max_depth

    async def analyze(
        self,
        database_name: str,
        sql: str,
    ) -> DatabaseEquivalencePreflight:
        direct = analyze_equivalence_preflight(sql)
        observations = _collect_function_observations(sql, "query")
        risk_codes: list[str] = list(direct.risk_codes)
        risks: list[dict[str, Any]] = _static_risk_summaries(direct, "query")
        _add_function_risks(risk_codes, risks, observations)
        unresolved: list[dict[str, Any]] = []
        resolved: list[dict[str, Any]] = []
        analyzed_views: set[tuple[str, str, str]] = set()
        resolution_cache: dict[tuple[str, str, str], _Resolution] = {}
        references = _referenced_objects(sql, "query")

        for reference in references:
            await self._inspect_reference(
                database_name,
                reference,
                depth=1,
                path=(),
                risk_codes=risk_codes,
                risks=risks,
                observations=observations,
                resolved=resolved,
                unresolved=unresolved,
                analyzed_views=analyzed_views,
                resolution_cache=resolution_cache,
            )

        unique_unresolved = _unique_mappings(unresolved)
        unique_resolved = _unique_mappings(resolved)
        coverage = {
            "status": "complete" if not unique_unresolved else "incomplete",
            "complete": not unique_unresolved,
            "max_depth": self.max_depth,
            "referenced_object_count": len(references),
            "resolved_view_count": len(unique_resolved),
            "analyzed_view_count": len(analyzed_views),
            "unresolved_dependency_count": len(unique_unresolved),
        }
        return DatabaseEquivalencePreflight(
            analysis_coverage=coverage,
            risk_codes=tuple(_unique_strings(risk_codes)),
            risks=tuple(_unique_mappings(risks)),
            function_verdicts=tuple(_build_function_verdicts(observations)),
            resolved_view_dependencies=tuple(unique_resolved),
            unresolved_dependencies=tuple(unique_unresolved),
        )

    async def _inspect_reference(
        self,
        database_name: str,
        reference: _ObjectReference,
        *,
        depth: int,
        path: tuple[tuple[str, str, str], ...],
        risk_codes: list[str],
        risks: list[dict[str, Any]],
        observations: list[tuple[str, str, str, str, str]],
        resolved: list[dict[str, Any]],
        unresolved: list[dict[str, Any]],
        analyzed_views: set[tuple[str, str, str]],
        resolution_cache: dict[tuple[str, str, str], _Resolution],
    ) -> None:
        if reference.catalog_name:
            _mark_unresolved(
                unresolved,
                risk_codes,
                risks,
                reference,
                depth,
                "cross_database_dependency",
                "cross-database and linked-server dependencies are not inspected",
            )
            return
        if reference.key in path:
            _mark_unresolved(
                unresolved,
                risk_codes,
                risks,
                reference,
                depth,
                "cyclic_view_dependency",
                "cyclic view dependency detected",
            )
            return
        if reference.key in analyzed_views:
            return
        if reference.key not in resolution_cache:
            resolution_cache[reference.key] = await self._resolve(
                database_name,
                reference,
            )
        resolution = resolution_cache[reference.key]
        if resolution.status == "non_view":
            return
        canonical_reference = _ObjectReference(
            resolution.schema_name or reference.schema_name,
            resolution.object_name or reference.object_name,
            reference.source,
        )
        if canonical_reference.key in path:
            _mark_unresolved(
                unresolved,
                risk_codes,
                risks,
                canonical_reference,
                depth,
                "cyclic_view_dependency",
                "cyclic view dependency detected",
            )
            return
        if canonical_reference.key in analyzed_views:
            return
        if depth > self.max_depth and resolution.status == "resolved":
            _mark_unresolved(
                unresolved,
                risk_codes,
                risks,
                canonical_reference,
                depth,
                "view_dependency_depth_exceeded",
                "maximum view dependency depth exceeded",
            )
            return
        if resolution.status != "resolved" or resolution.definition is None:
            reason_by_status = {
                "encrypted": "view definition is encrypted and cannot be inspected",
                "inaccessible": "view definition could not be read",
                "unresolved": "view dependency could not be resolved",
            }
            reason = reason_by_status.get(
                resolution.status,
                "view dependency could not be analyzed",
            )
            _mark_unresolved(
                unresolved,
                risk_codes,
                risks,
                reference,
                depth,
                f"{resolution.status}_view_dependency",
                reason,
            )
            return

        body = _view_definition_body(resolution.definition)
        source_name = _qualified_name(canonical_reference)
        try:
            view_preflight = analyze_equivalence_preflight(body)
            view_references = _referenced_objects(
                body,
                source_name,
                default_schema=canonical_reference.schema_name,
            )
            view_observations = _collect_function_observations(
                body,
                source_name,
            )
        except (ParseError, ValueError):
            _mark_unresolved(
                unresolved,
                risk_codes,
                risks,
                reference,
                depth,
                "invalid_view_definition",
                "view definition could not be parsed for equivalence analysis",
            )
            return

        resolved.append(
            {
                "schema_name": canonical_reference.schema_name,
                "object_name": canonical_reference.object_name,
                "depth": depth,
                "source": reference.source,
            }
        )
        analyzed_views.add(canonical_reference.key)
        observations.extend(view_observations)
        _add_function_risks(risk_codes, risks, view_observations)
        for risk in _static_risk_summaries(
            view_preflight,
            source_name,
        ):
            _add_risk(
                risk_codes,
                risks,
                str(risk["risk"]),
                str(risk["source"]),
                str(risk["reason"]),
            )
        risk_codes.extend(view_preflight.risk_codes)

        next_path = (*path, canonical_reference.key)
        for view_reference in view_references:
            await self._inspect_reference(
                database_name,
                view_reference,
                depth=depth + 1,
                path=next_path,
                risk_codes=risk_codes,
                risks=risks,
                observations=observations,
                resolved=resolved,
                unresolved=unresolved,
                analyzed_views=analyzed_views,
                resolution_cache=resolution_cache,
            )

    async def _resolve(
        self,
        database_name: str,
        reference: _ObjectReference,
    ) -> _Resolution:
        try:
            if self.resolver is not None:
                value = await self.resolver(
                    database_name,
                    reference.schema_name,
                    reference.object_name,
                )
            elif self.executor is not None:
                value = await _fetch_with_executor(
                    self.executor,
                    database_name,
                    reference.schema_name,
                    reference.object_name,
                )
            else:
                return _Resolution("unresolved")
        except Exception:
            return _Resolution("inaccessible")
        return _normalize_resolution(
            value,
            fallback_schema=reference.schema_name,
            fallback_object=reference.object_name,
        )


async def analyze_database_equivalence_preflight(
    sql: str,
    database_name: str,
    resolver: ViewDefinitionResolver | Any | None = None,
    *,
    executor: Any | None = None,
    max_depth: int = MAX_VIEW_DEPENDENCY_DEPTH,
) -> DatabaseEquivalencePreflight:
    """Convenience wrapper for database-aware asynchronous preflight analysis."""

    return await EquivalencePreflightService(
        resolver,
        executor=executor,
        max_depth=max_depth,
    ).analyze(database_name, sql)


def _has_fetch_all(value: Any) -> bool:
    return value is not None and callable(getattr(value, "fetch_all", None))


async def _fetch_with_executor(
    executor: Any,
    database_name: str,
    schema_name: str,
    object_name: str,
) -> Sequence[Mapping[str, Any]]:
    params = [schema_name, object_name, schema_name, object_name]
    fetch_all = getattr(executor, "fetch_all", None)
    if callable(fetch_all):
        fetch = cast(Callable[..., Awaitable[Sequence[Mapping[str, Any]]]], fetch_all)
        return await fetch(database_name, _VIEW_DEFINITION_QUERY, params=params)
    if callable(executor):
        fetch = cast(
            Callable[..., Awaitable[Sequence[Mapping[str, Any]]]],
            executor,
        )
        return await fetch(database_name, _VIEW_DEFINITION_QUERY, params)
    raise TypeError("executor must provide fetch_all or be an async callback")


def _normalize_resolution(
    value: Any,
    *,
    fallback_schema: str,
    fallback_object: str,
) -> _Resolution:
    if isinstance(value, str):
        if not fallback_schema:
            return _Resolution("unresolved")
        return _Resolution(
            "resolved",
            value if value.strip() else None,
            fallback_schema,
            fallback_object,
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if not value:
            return _Resolution("unresolved")
        value = value[0]
    if not isinstance(value, Mapping):
        return _Resolution("unresolved")

    status = str(value.get("status") or "").strip().casefold()
    if status in {"encrypted", "is_encrypted"} or _as_bool(
        value.get("is_encrypted", value.get("encrypted"))
    ):
        return _Resolution("encrypted")
    if status in {"inaccessible", "forbidden", "permission_denied"} or (
        "accessible" in value and not _as_bool(value.get("accessible"))
    ):
        return _Resolution("inaccessible")
    if status in {"unresolved", "not_found", "missing"}:
        return _Resolution("unresolved")

    object_type = (
        str(
            value.get("object_type")
            or value.get("type_desc")
            or value.get("type")
            or value.get("object_type_code")
            or ""
        )
        .strip()
        .upper()
    )
    schema_name = str(value.get("schema_name") or fallback_schema).strip()
    object_name = str(value.get("object_name") or fallback_object).strip()
    definition = value.get("definition")
    if object_type in _NON_VIEW_TYPE_VALUES:
        return _Resolution("non_view", None, schema_name or None, object_name)
    if object_type and object_type not in _VIEW_TYPE_VALUES:
        return _Resolution("unresolved")
    if not isinstance(definition, str) or not definition.strip():
        return _Resolution("unresolved")
    if not schema_name:
        return _Resolution("unresolved")
    return _Resolution("resolved", definition, schema_name, object_name)


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "y"}
    return bool(value)


def _view_definition_body(definition: str) -> str:
    return _CREATE_VIEW_HEADER.sub("", definition, count=1).strip()


def _qualified_name(reference: _ObjectReference) -> str:
    return f"{reference.schema_name}.{reference.object_name}"


def _referenced_objects(
    sql: str,
    source: str,
    *,
    default_schema: str = "",
) -> list[_ObjectReference]:
    statements = [
        statement for statement in parse(sql, read="tsql") if statement is not None
    ]
    cte_names = {
        cte.alias_or_name
        for statement in statements
        for cte in statement.find_all(exp.CTE)
        if cte.alias_or_name
    }
    references: list[_ObjectReference] = []
    seen: set[tuple[str, str, str]] = set()
    for statement in statements:
        for table in statement.find_all(exp.Table):
            if (
                table.name in cte_names
                and not table.db
                and not table.catalog
            ):
                continue
            _add_reference(
                references,
                seen,
                _ObjectReference(
                    table.db or default_schema,
                    table.name,
                    source,
                    _table_catalog_name(table),
                ),
            )
    return references


def _table_catalog_name(table: exp.Table) -> str:
    catalog_expression = table.args.get("catalog")
    if catalog_expression is not None and not isinstance(
        catalog_expression,
        exp.Identifier,
    ):
        return _DYNAMIC_EXTERNAL_SOURCE
    if table.catalog:
        return table.catalog
    if isinstance(table.this, exp.Anonymous):
        return _DYNAMIC_EXTERNAL_SOURCE
    return ""


def _add_reference(
    references: list[_ObjectReference],
    seen: set[tuple[str, str, str]],
    reference: _ObjectReference,
) -> None:
    if reference.key not in seen:
        references.append(reference)
        seen.add(reference.key)


def _collect_function_observations(
    sql: str,
    source: str,
) -> list[tuple[str, str, str, str, str]]:
    observations: list[tuple[str, str, str, str, str]] = []
    for statement in (
        statement for statement in parse(sql, read="tsql") if statement is not None
    ):
        for node in statement.walk():
            observation = _function_observation(node, sql, source)
            if observation is not None:
                observations.append(observation)
    return observations


def _function_observation(
    node: exp.Expr,
    sql: str,
    source: str,
) -> tuple[str, str, str, str, str] | None:
    name: str | None = None
    category: str | None = None
    verdict = "proof_required"
    reason = ""
    if isinstance(node, exp.CurrentTimestampLTZ):
        name = _source_function_name(node, sql) or "SYSDATETIMEOFFSET"
        category = "clock"
        reason = "statement-stable clock values can differ across proof statements"
    elif isinstance(node, exp.CurrentTimestamp):
        name = _source_function_name(node, sql) or "CURRENT_TIMESTAMP"
        category = "clock"
        reason = "statement-stable clock values can differ across proof statements"
    elif isinstance(node, exp.Uuid):
        name = "NEWID"
        category = "volatile"
        reason = "row-volatile function has no deterministic equivalence contract"
    elif isinstance(node, exp.Rand):
        name = "RAND"
        seed = node.args.get("this")
        if _is_literal_seed(seed):
            category = "safely_seeded"
            verdict = "safe"
            reason = "RAND uses a literal seed and is repeatable"
        else:
            category = "volatile"
            reason = (
                "RAND is unseeded or uses a non-literal seed and is not "
                "safe for direct equivalence"
            )
    elif isinstance(node, exp.Anonymous):
        name = str(node.name or "").strip().upper() or None
        if name in _CLOCK_FUNCTIONS:
            category = "clock"
            reason = "statement-stable clock values can differ across proof statements"
        elif name in _ROW_VOLATILE_FUNCTIONS:
            category = "volatile"
            reason = "row-volatile function has no deterministic equivalence contract"
    if name is None or category is None:
        return None
    return name, source, category, verdict, reason


def _source_function_name(node: exp.Expr, sql: str) -> str | None:
    start = node.meta.get("start")
    end = node.meta.get("end")
    if not isinstance(start, int) or not isinstance(end, int):
        return None
    value = sql[start : end + 1].strip().upper()
    return value if re.fullmatch(r"[A-Z_][A-Z0-9_]*", value) else None


def _is_literal_seed(seed: exp.Expr | None) -> bool:
    if isinstance(seed, exp.Literal):
        return not bool(getattr(seed, "is_string", False))
    if isinstance(seed, (exp.Neg, exp.Paren)):
        return _is_literal_seed(seed.this)
    return False


def _build_function_verdicts(
    observations: Sequence[tuple[str, str, str, str, str]],
) -> list[FunctionVerdict]:
    counts: Counter[tuple[str, str, str, str, str]] = Counter(observations)
    return [
        FunctionVerdict(
            function=function,
            source=source,
            count=count,
            verdict=verdict,
            reason=reason,
            category=category,
        )
        for (function, source, category, verdict, reason), count in sorted(
            counts.items(),
            key=lambda item: (item[0][1], item[0][0], item[0][2], item[0][3]),
        )
    ]


def _static_risk_summaries(
    preflight: EquivalencePreflight,
    source: str,
) -> list[dict[str, Any]]:
    reasons = {
        "statement_stable_clock": "statement-stable clock values can differ across proof statements",
        "row_volatile_function": "row-volatile function has no deterministic equivalence contract",
        "nonrepeatable_table_sample": "TABLESAMPLE is not repeatable without a seed",
        "unordered_row_limit": "unordered row limits do not define a stable result boundary",
        "row_limit_total_order_unproven": "ORDER BY keys have not been proven to form a unique total order",
        "window_order_total_order_unproven": "window ORDER BY keys have not been proven to form a unique total order",
    }
    return [
        {
            "risk": risk_code,
            "source": source,
            "reason": reasons.get(risk_code, "equivalence risk detected"),
        }
        for risk_code in preflight.risk_codes
    ]


def _add_function_risks(
    risk_codes: list[str],
    risks: list[dict[str, Any]],
    observations: Sequence[tuple[str, str, str, str, str]],
) -> None:
    for verdict in _build_function_verdicts(observations):
        if verdict.verdict == "safe":
            continue
        risk_code = (
            "statement_stable_clock"
            if verdict.category == "clock"
            else "row_volatile_function"
        )
        _add_risk(
            risk_codes,
            risks,
            risk_code,
            verdict.source,
            verdict.reason,
            function=verdict.function,
        )


def _add_risk(
    risk_codes: list[str],
    risks: list[dict[str, Any]],
    risk_code: str,
    source: str,
    reason: str,
    **extra: Any,
) -> None:
    risk_codes.append(risk_code)
    item: dict[str, Any] = {"risk": risk_code, "source": source, "reason": reason}
    item.update(extra)
    risks.append(item)


def _mark_unresolved(
    unresolved: list[dict[str, Any]],
    risk_codes: list[str],
    risks: list[dict[str, Any]],
    reference: _ObjectReference,
    depth: int,
    reason_code: str,
    reason: str,
) -> None:
    unresolved.append(
        {
            "schema_name": reference.schema_name,
            "object_name": reference.object_name,
            **(
                {"catalog_name": reference.catalog_name}
                if reference.catalog_name
                else {}
            ),
            "depth": depth,
            "source": reference.source,
            "reason": reason_code,
            "message": reason,
        }
    )
    _add_risk(risk_codes, risks, reason_code, reference.source, reason)


def _unique_strings(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _unique_mappings(values: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    unique: list[Mapping[str, Any]] = []
    seen: set[tuple[tuple[str, str], ...]] = set()
    for value in values:
        identity = tuple(sorted((str(key), repr(item)) for key, item in value.items()))
        if identity not in seen:
            unique.append(dict(value))
            seen.add(identity)
    return unique


__all__ = [
    "DatabaseEquivalencePreflight",
    "EquivalencePreflightService",
    "ExecutorCompatibleFetcher",
    "FunctionVerdict",
    "MAX_VIEW_DEPENDENCY_DEPTH",
    "ViewDefinitionResolver",
    "analyze_database_equivalence_preflight",
]
