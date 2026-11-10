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

from .catalog_constraints import CatalogConstraintService
from .catalog_constraints import TableConstraintFacts
from .catalog_constraints import _folded
from .catalog_constraints import deterministic_order_key
from .catalog_constraints import order_tiebreaker
from .equivalence_contract import EquivalencePreflight
from .equivalence_contract import _CLOCK_FUNCTIONS
from .equivalence_contract import _ROW_VOLATILE_FUNCTIONS
from .equivalence_contract import analyze_equivalence_preflight
from .equivalence_contract import parseable_sql
from .safe_sql import read_only_batch_branches

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
_CREATE_INLINE_FUNCTION_HEADER = re.compile(
    r"^\s*(?:CREATE|ALTER)\s+(?:OR\s+ALTER\s+)?FUNCTION\b.*?"
    r"\bRETURNS\s+TABLE\b.*?\bAS\s+RETURN\b",
    re.IGNORECASE | re.DOTALL,
)
_CREATE_MULTI_STATEMENT_FUNCTION = re.compile(
    r"^\s*(?:CREATE|ALTER)\s+(?:OR\s+ALTER\s+)?FUNCTION\b.*?"
    r"\bRETURNS\s+@\w+\s+TABLE\b.*?\bAS\s+BEGIN\b(?P<body>.*)\bEND\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_VIEW_TYPE_VALUES = frozenset({"V", "VIEW", "SQL_VIEW"})
_TABLE_FUNCTION_TYPE_VALUES = frozenset(
    {
        "IF",
        "TF",
        "SQL_INLINE_TABLE_VALUED_FUNCTION",
        "SQL_TABLE_VALUED_FUNCTION",
    }
)
_DYNAMIC_EXTERNAL_SOURCE = "dynamic_external_source"
# Rowset functions that read another server or a file. Azure SQL Database has
# no linked servers, but the source is still outside this database.
_EXTERNAL_ROWSET_FUNCTIONS = frozenset(
    {"OPENDATASOURCE", "OPENQUERY", "OPENROWSET", "OPENXML"}
)
# Built-in rowset functions whose rows depend only on their arguments.
_DETERMINISTIC_BUILTIN_ROWSET_FUNCTIONS = frozenset({"STRING_SPLIT"})
_NON_VIEW_TYPE_VALUES = frozenset(
    {
        "AF",
        "FN",
        "FS",
        "FT",
        "P",
        "PROCEDURE",
        "SQL_SCALAR_FUNCTION",
        "SQL_STORED_PROCEDURE",
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
    total_order_proofs: tuple[Mapping[str, Any], ...] = ()
    total_order_gaps: tuple[Mapping[str, Any], ...] = ()
    # The outer ORDER BY of a statement without a row limit. A tie there is
    # not a risk; it is why an order-only mismatch can be noise.
    result_order_proofs: tuple[Mapping[str, Any], ...] = ()
    result_order_ties: tuple[Mapping[str, Any], ...] = ()
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
            "total_order_proofs": [dict(item) for item in self.total_order_proofs],
            "total_order_gaps": [dict(item) for item in self.total_order_gaps],
            "result_order_proofs": [dict(item) for item in self.result_order_proofs],
            "result_order_ties": [dict(item) for item in self.result_order_ties],
        }


@dataclass(frozen=True, slots=True)
class _ObjectReference:
    schema_name: str
    object_name: str
    source: str
    catalog_name: str = ""
    is_function: bool = False
    # `name.nodes(...)` is a schema function or the XML nodes() method on a
    # column; only the catalog can tell.
    xml_method_candidate: bool = False

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
    missing: bool = False


class EquivalencePreflightService:
    """Run direct analysis and recursively inspect database view definitions.

    The identifier resolver receives (database_name, schema_name, object_name)
    and returns a definition string or metadata mapping. A mapping should
    include object_type and may include definition, is_encrypted, or
    accessible. Returning a table/object type skips view traversal.

    The executor may provide fetch_all or be a callback accepting
    (database_name, query, params). Only metadata is queried through this path.

    The catalog reader proves a TOP, OFFSET or window order deterministic when
    its keys cover a unique NOT NULL key. It defaults to a reader on the
    executor; without one, such an order stays unproven.
    """

    def __init__(
        self,
        resolver: ViewDefinitionResolver | Any | None = None,
        *,
        executor: Any | None = None,
        max_depth: int = MAX_VIEW_DEPENDENCY_DEPTH,
        catalog: Any | None = None,
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
        if catalog is None and _has_fetch_all(executor):
            catalog = CatalogConstraintService(executor)
        self.catalog = catalog

    async def analyze(
        self,
        database_name: str,
        sql: str,
    ) -> DatabaseEquivalencePreflight:
        branches = read_only_batch_branches(sql)
        if branches:
            return _merge_branch_preflights(
                [await self.analyze(database_name, branch.select_sql) for branch in branches]
            )
        sql = parseable_sql(sql)
        direct = analyze_equivalence_preflight(sql)
        observations = _collect_function_observations(sql, "query")
        facts_by_table: dict[tuple[str, str], TableConstraintFacts] = {}
        total_order_proofs, total_order_gaps = await self._prove_total_orders(
            database_name,
            direct.details,
            facts_by_table,
        )
        result_order_proofs, result_order_ties = await self._prove_total_orders(
            database_name,
            direct.result_orders,
            facts_by_table,
        )
        unproven_orders = {str(gap["risk"]) for gap in total_order_gaps}
        risk_codes: list[str] = [
            code
            for code in direct.risk_codes
            if code not in _TOTAL_ORDER_RISKS or code in unproven_orders
        ]
        risks: list[dict[str, Any]] = [
            _with_ties(risk, total_order_gaps)
            for risk in _static_risk_summaries(direct, "query")
            if risk["risk"] in risk_codes
        ]
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
            total_order_proofs=tuple(total_order_proofs),
            total_order_gaps=tuple(total_order_gaps),
            result_order_proofs=tuple(result_order_proofs),
            result_order_ties=tuple(result_order_ties),
        )

    async def _prove_total_orders(
        self,
        database_name: str,
        details: Sequence[Mapping[str, Any]],
        facts_by_table: dict[tuple[str, str], TableConstraintFacts],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Split each ordered row limit, window order and result order into proven or still tied.

        The ordering is total when it fixes one row of every source under the
        SELECT. A base table is fixed when the known columns cover a unique NOT
        NULL key; a relation of distinct rows when they cover all its columns.
        A column equality that holds in every row carries a fixed column to the
        other side, which is how a join back on the key is proven.
        """

        proofs: list[dict[str, Any]] = []
        gaps: list[dict[str, Any]] = []
        for detail in details:
            requirement = detail.get("total_order")
            if not isinstance(requirement, Mapping):
                continue
            instance = {
                key: detail[key]
                for key in ("risk", "statement_ordinal", "select_ordinal", "window_ordinal")
                if key in detail
            }
            clause = (
                "window ORDER BY"
                if detail["risk"] == "window_order_total_order_unproven"
                else "ORDER BY"
            )
            consequence = {
                "window_order_total_order_unproven": "so their order inside the window is not defined",
                "result_order_ties": "so their order in the result is not defined",
            }.get(str(detail["risk"]), "so which of them the row limit keeps is not defined")
            if not requirement.get("provable"):
                gaps.append({**instance, "tie": str(requirement.get("unprovable_reason"))})
                continue
            tables = [dict(table) for table in requirement["tables"]]
            relations = [dict(item) for item in requirement.get("unique_relations") or ()]
            if tables and self.catalog is None:
                gaps.append({**instance, "tie": "no catalog reader is configured"})
                continue
            instance_gaps: list[dict[str, Any]] = []
            facts_by_alias: dict[str, TableConstraintFacts] = {}
            for table in tables:
                identity = (str(table["schema"]).casefold(), str(table["table"]).casefold())
                if identity not in facts_by_table:
                    facts_by_table[identity] = await self.catalog.table_facts(  # type: ignore[union-attr]
                        database_name,
                        table["schema"],
                        table["table"],
                    )
                facts = facts_by_table[identity]
                name = f"{table['schema']}.{table['table']}"
                if not facts.exists:
                    instance_gaps.append(
                        {**instance, "table": name, "tie": f"{name} could not be read from the catalog"}
                    )
                elif facts.object_type != "USER_TABLE":
                    instance_gaps.append(
                        {
                            **instance,
                            "table": name,
                            "tie": f"{name} is not a base table, so it has no key to order by",
                        }
                    )
                else:
                    facts_by_alias[table["alias"]] = facts
            if instance_gaps:
                gaps.extend(instance_gaps)
                continue

            known: dict[str, list[str]] = {
                source["alias"]: list(source["order_columns"]) for source in (*tables, *relations)
            }
            ordered_directly = {alias: _folded(columns) for alias, columns in known.items()}
            fixed: dict[str, Any] = {}

            def base(endpoint: Mapping[str, Any]) -> tuple[str, str, str] | None:
                alias = endpoint["alias"]
                table = next((item for item in tables if item["alias"] == alias), None)
                if table is not None:
                    return (table["schema"], table["table"], endpoint["column"])
                relation = next(item for item in relations if item["alias"] == alias)
                copy = relation["lineage"].get(str(endpoint["column"]).casefold())
                return (copy["schema"], copy["table"], copy["column"]) if copy else None

            def type_of(column: tuple[str, str, str]) -> str | None:
                facts = facts_by_table.get((column[0].casefold(), column[1].casefold()))
                return (facts.column_types or {}).get(column[2].casefold()) if facts else None

            usable: list[Mapping[str, Any]] = []
            unusable: dict[str, str] = {}  # source alias -> why its join could not be used
            for equality in requirement.get("equalities") or ():
                left, right = base(equality["left"]), base(equality["right"])
                if left is None or right is None:
                    why = "a joined column is not a direct copy of a base table column"
                elif tuple(part.casefold() for part in left) == tuple(
                    part.casefold() for part in right
                ):
                    why = ""  # the same column of the same table: no conversion
                elif type_of(left) is None or type_of(right) is None:
                    why = "the column types could not be read"
                elif type_of(left) != type_of(right):
                    why = (
                        "the column types differ, so an implicit conversion can match "
                        "several key values"
                    )
                else:
                    why = ""
                if not why:
                    usable.append(equality)
                    continue
                for endpoint in (equality["left"], equality["right"]):
                    unusable.setdefault(
                        endpoint["alias"], f"join {equality['text']} was not used: {why}"
                    )

            def is_known(endpoint: Mapping[str, Any]) -> bool:
                return endpoint["alias"] in fixed or str(endpoint["column"]).casefold() in _folded(
                    known[endpoint["alias"]]
                )

            progress = True
            while progress:
                progress = False
                for table in tables:
                    alias = table["alias"]
                    key = (
                        None
                        if alias in fixed
                        else deterministic_order_key(facts_by_alias[alias], known[alias])
                    )
                    if key is not None:
                        fixed[alias] = key
                        progress = True
                for relation in relations:
                    alias = relation["alias"]
                    if alias not in fixed and _folded(relation["key_columns"]) <= _folded(known[alias]):
                        fixed[alias] = relation
                        progress = True
                for equality in usable:
                    pairs = [(equality["left"], equality["right"])]
                    if equality["direction"] == "both":
                        pairs.append((equality["right"], equality["left"]))
                    for source, target in pairs:
                        if is_known(source) and not is_known(target):
                            known[target["alias"]].append(target["column"])
                            progress = True

            keys: list[dict[str, Any]] = []
            for table in tables:
                alias = table["alias"]
                facts = facts_by_alias[alias]
                name = f"{table['schema']}.{table['table']}"
                relation_label = table.get("relation")
                nested = {"relation": relation_label} if relation_label else {}
                if alias in fixed:
                    key = fixed[alias]
                    keys.append(
                        {
                            "table": name,
                            "key_name": key.name,
                            "key_kind": key.kind,
                            "key_columns": list(key.columns),
                            **nested,
                            **(
                                {}
                                if _folded(key.columns) <= ordered_directly[alias]
                                else {"reached_through_join": True}
                            ),
                        }
                    )
                    continue
                columns = known[alias]
                shown = (
                    f"{name} (read as {alias.rsplit('.', 1)[-1]} in {relation_label})"
                    if relation_label
                    else name
                )
                tiebreaker = order_tiebreaker(facts, columns)
                if tiebreaker is None:
                    fix = f"{name} has no unique NOT NULL key; no ordering of its columns is deterministic"
                elif relation_label:
                    fix = (
                        f"select {', '.join(tiebreaker['missing_columns'])} "
                        f"({tiebreaker['key_kind']} {tiebreaker['key_name']}) in {relation_label} "
                        f"and append it to the {clause}"
                    )
                else:
                    fix = (
                        f"append {', '.join(tiebreaker['missing_columns'])} "
                        f"({tiebreaker['key_kind']} {tiebreaker['key_name']}) to the {clause}"
                    )
                instance_gaps.append(
                    {
                        **instance,
                        "table": name,
                        **nested,
                        "tie": (
                            f"rows of {shown} with the same ({', '.join(columns)}) are tied, "
                            f"{consequence}"
                            if columns
                            else f"the ordering has no column of {shown}, so its rows are tied"
                        ),
                        "tiebreaker": fix,
                        "tiebreaker_columns": (
                            tiebreaker["missing_columns"] if tiebreaker is not None else []
                        ),
                        **({"join_not_used": unusable[alias]} if alias in unusable else {}),
                    }
                )
            for relation in relations:
                alias = relation["alias"]
                label = relation["relation"]
                if alias in fixed:
                    keys.append(
                        {
                            "relation": label,
                            "key_kind": relation["kind"],
                            "key_columns": list(relation["key_columns"]),
                            **(
                                {}
                                if _folded(relation["key_columns"]) <= ordered_directly[alias]
                                else {"reached_through_join": True}
                            ),
                        }
                    )
                    continue
                missing = [
                    column
                    for column in relation["key_columns"]
                    if column.casefold() not in _folded(known[alias])
                ]
                instance_gaps.append(
                    {
                        **instance,
                        "relation": label,
                        "tie": (
                            f"{label} returns distinct rows, so only all of its columns "
                            f"({', '.join(relation['key_columns'])}) form a key; the ordering "
                            f"fixes ({', '.join(known[alias])})"
                        ),
                        "tiebreaker": f"append {', '.join(missing)} of {label} to the {clause}",
                        "tiebreaker_columns": missing,
                        **({"join_not_used": unusable[alias]} if alias in unusable else {}),
                    }
                )
            if instance_gaps:
                gaps.extend(instance_gaps)
            else:
                proofs.append({**instance, "keys": keys})
        return proofs, gaps

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
        if reference.xml_method_candidate and (
            resolution.missing or resolution.status == "non_view"
        ):
            return  # no such function: SQL binds nodes() to the XML column
        is_function = reference.is_function or resolution.status == "function"
        if resolution.status == "non_view" and not is_function:
            return
        canonical_reference = _ObjectReference(
            resolution.schema_name or reference.schema_name,
            resolution.object_name or reference.object_name,
            reference.source,
            is_function=is_function,
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
        if depth > self.max_depth and resolution.status in {"resolved", "function"}:
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
        readable_statuses = {"resolved", "function"} if is_function else {"resolved"}
        if resolution.status not in readable_statuses or resolution.definition is None:
            kind = "function" if is_function else "view"
            reason_by_status = {
                "encrypted": f"{kind} definition is encrypted and cannot be inspected",
                "inaccessible": f"{kind} definition could not be read",
                "unresolved": f"{kind} dependency could not be resolved",
            }
            reason = reason_by_status.get(
                resolution.status,
                f"{kind} dependency could not be analyzed",
            )
            # A function name that resolves to a table or CLR object has no
            # readable T-SQL body.
            status = (
                "unresolved"
                if is_function and resolution.status not in reason_by_status
                else resolution.status
            )
            _mark_unresolved(
                unresolved,
                risk_codes,
                risks,
                reference,
                depth,
                f"{status}_{kind}_dependency",
                reason,
            )
            return

        source_name = _qualified_name(canonical_reference)
        try:
            body = (
                _function_definition_body(resolution.definition)
                if is_function
                else _view_definition_body(resolution.definition)
            )
            if body is None:
                raise ValueError("function definition is not a T-SQL table function")
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
            kind = "function" if is_function else "view"
            _mark_unresolved(
                unresolved,
                risk_codes,
                risks,
                reference,
                depth,
                f"invalid_{kind}_definition",
                f"{kind} definition could not be parsed for equivalence analysis",
            )
            return

        resolved.append(
            {
                "schema_name": canonical_reference.schema_name,
                "object_name": canonical_reference.object_name,
                "depth": depth,
                "source": reference.source,
                **(
                    {"dependency_type": "table_valued_function"}
                    if is_function
                    else {}
                ),
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


def _merge_branch_preflights(
    parts: Sequence[DatabaseEquivalencePreflight],
) -> DatabaseEquivalencePreflight:
    """One preflight for a branching batch: a risk in any branch is a risk of the batch."""

    def tagged(name: str) -> tuple[Mapping[str, Any], ...]:
        return tuple(
            {**item, "branch": ordinal}
            for ordinal, part in enumerate(parts, start=1)
            for item in getattr(part, name)
        )

    counts = (
        "referenced_object_count",
        "resolved_view_count",
        "analyzed_view_count",
        "unresolved_dependency_count",
    )
    complete = all(part.analysis_coverage.get("complete") for part in parts)
    return DatabaseEquivalencePreflight(
        analysis_coverage={
            "status": "complete" if complete else "incomplete",
            "complete": complete,
            "max_depth": max(int(part.analysis_coverage.get("max_depth") or 0) for part in parts),
            **{
                name: sum(int(part.analysis_coverage.get(name) or 0) for part in parts)
                for name in counts
            },
            "branch_count": len(parts),
        },
        risk_codes=tuple(_unique_strings([code for part in parts for code in part.risk_codes])),
        risks=tagged("risks"),
        function_verdicts=tuple(
            dict.fromkeys(verdict for part in parts for verdict in part.function_verdicts)
        ),
        resolved_view_dependencies=tuple(
            _unique_mappings([item for part in parts for item in part.resolved_view_dependencies])
        ),
        unresolved_dependencies=tuple(
            _unique_mappings([item for part in parts for item in part.unresolved_dependencies])
        ),
        total_order_proofs=tagged("total_order_proofs"),
        total_order_gaps=tagged("total_order_gaps"),
        result_order_proofs=tagged("result_order_proofs"),
        result_order_ties=tagged("result_order_ties"),
    )


_TOTAL_ORDER_RISKS = frozenset(
    {"row_limit_total_order_unproven", "window_order_total_order_unproven"}
)


def _with_ties(
    risk: dict[str, Any],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Put the exact tie and its tiebreaker into the risk the agent reads first."""

    ties = [gap for gap in gaps if gap.get("risk") == risk["risk"]]
    if not ties:
        return risk
    text = "; ".join(
        f"{gap['tie']}; fix: {gap['tiebreaker']}" if gap.get("tiebreaker") else str(gap["tie"])
        for gap in ties
    )
    return {**risk, "reason": f"{risk['reason']}: {text}"}


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
    if value is None:
        return _Resolution("unresolved", missing=True)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if not value:
            return _Resolution("unresolved", missing=True)
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
        return _Resolution("unresolved", missing=True)

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
    if object_type in _TABLE_FUNCTION_TYPE_VALUES:
        if not isinstance(definition, str) or not definition.strip() or not schema_name:
            return _Resolution("unresolved")
        return _Resolution("function", definition, schema_name, object_name)
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


def _function_definition_body(definition: str) -> str | None:
    """Return the T-SQL body of an inline or multi-statement table function."""

    inline = _CREATE_INLINE_FUNCTION_HEADER.match(definition)
    if inline is not None:
        return definition[inline.end() :].strip()
    multi_statement = _CREATE_MULTI_STATEMENT_FUNCTION.match(definition)
    if multi_statement is not None:
        body = multi_statement.group("body").strip()
        # An opaque statement could hide a volatile call from the analysis.
        if any(
            isinstance(node, exp.Command)
            for statement in parse(body, read="tsql")
            if statement is not None
            for node in statement.walk()
        ):
            return None
        return body
    return None


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
        source_aliases = _source_aliases(statement)
        for table in statement.find_all(exp.Table):
            if isinstance(table.this, exp.Parameter):
                continue  # table variable: its rows come from this batch
            if isinstance(table.this, exp.Anonymous):
                if isinstance(table.args.get("db"), exp.Parameter) and (
                    table.this.name.casefold() == "nodes"
                ):
                    continue  # @xml.nodes(): shreds a variable from this batch
                catalog_name = _table_catalog_name(table)
                reference = _function_reference(
                    table.this.name,
                    [part for part in (catalog_name, table.db) if part],
                    source,
                    default_schema=default_schema,
                    dynamic_catalog=catalog_name == _DYNAMIC_EXTERNAL_SOURCE,
                    source_aliases=source_aliases,
                )
                if reference is not None:
                    _add_reference(references, seen, reference)
                continue
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
        for lateral in statement.find_all(exp.Lateral):
            call = lateral.this
            prefix: list[str] | None = []
            if isinstance(call, exp.Dot):
                prefix = _identifier_parts(call.this)
                call = call.expression
            if isinstance(call, exp.Anonymous) and prefix is not None:
                reference = _function_reference(
                    call.name,
                    prefix,
                    source,
                    default_schema=default_schema,
                    source_aliases=source_aliases,
                )
                if reference is not None:
                    _add_reference(references, seen, reference)
    return references


def _identifier_parts(node: exp.Expr) -> list[str] | None:
    if isinstance(node, exp.Identifier):
        return [node.name]
    if isinstance(node, exp.Dot):
        left = _identifier_parts(node.this)
        right = _identifier_parts(node.expression)
        return None if left is None or right is None else [*left, *right]
    return None


def _source_aliases(statement: exp.Expr) -> frozenset[str]:
    """Casefolded names a column reference can be qualified with."""

    names = {alias.name for alias in statement.find_all(exp.TableAlias)}
    names.update(
        table.name
        for table in statement.find_all(exp.Table)
        if isinstance(table.this, exp.Identifier)
    )
    return frozenset(name.casefold() for name in names if name)


def _function_reference(
    function_name: str,
    prefix: Sequence[str],
    source: str,
    *,
    default_schema: str,
    dynamic_catalog: bool = False,
    source_aliases: frozenset[str] = frozenset(),
) -> _ObjectReference | None:
    """Map a table-valued function call in FROM, JOIN, or APPLY to a reference."""

    name = str(function_name or "").strip()
    if name.upper() in _EXTERNAL_ROWSET_FUNCTIONS or dynamic_catalog:
        # Arguments can carry connection strings, so no name is retained.
        return _ObjectReference(
            prefix[-1] if prefix else "",
            "",
            source,
            _DYNAMIC_EXTERNAL_SOURCE,
        )
    if name.casefold() == "nodes":
        if len(prefix) >= 2 and prefix[0].casefold() in source_aliases:
            return None  # alias.column.nodes(): shreds a value already read
        if len(prefix) == 1:
            return _ObjectReference(
                prefix[0],
                name,
                source,
                is_function=True,
                xml_method_candidate=True,
            )
    if not prefix and name.upper() in _DETERMINISTIC_BUILTIN_ROWSET_FUNCTIONS:
        return None
    return _ObjectReference(
        prefix[-1] if prefix else default_schema,
        name,
        source,
        ".".join(prefix[:-1]),
        is_function=True,
    )


def _table_catalog_name(table: exp.Table) -> str:
    catalog_expression = table.args.get("catalog")
    if catalog_expression is not None and not isinstance(
        catalog_expression,
        exp.Identifier,
    ):
        return _DYNAMIC_EXTERNAL_SOURCE
    if table.catalog:
        return table.catalog
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
        "isolation_table_hint": (
            "an isolation table hint (for example NOLOCK or READPAST) reads outside the "
            "comparison snapshot, so a result match holds only while no other session writes"
        ),
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
