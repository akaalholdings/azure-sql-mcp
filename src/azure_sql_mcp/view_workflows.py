"""Reviewed, reversible CREATE VIEW and ALTER VIEW workflows for Azure SQL."""

from __future__ import annotations

import hashlib
import re
import secrets
from dataclasses import dataclass
from dataclasses import replace
from typing import Any

import sqlglot
from sqlglot import exp

from .admin_policy import AdminAction
from .admin_policy import AdminPolicy
from .database_policy import DatabasePolicy
from .database_policy import DatabasePolicySet
from .safe_sql import strip_literals_and_comments

VIEW_DEFINITION_SQL = """
SELECT
    s.name AS schema_name,
    v.name AS view_name,
    v.object_id,
    m.definition,
    m.is_schema_bound,
    m.uses_ansi_nulls,
    m.uses_quoted_identifier
FROM sys.views AS v
INNER JOIN sys.schemas AS s
    ON s.schema_id = v.schema_id
LEFT JOIN sys.sql_modules AS m
    ON m.object_id = v.object_id
WHERE s.name = ?
  AND v.name = ?
"""

VIEW_DEPENDENCIES_SQL = """
SELECT
    COALESCE(d.referenced_database_name, '') AS referenced_database_name,
    COALESCE(d.referenced_schema_name, rs.name, '') AS referenced_schema_name,
    COALESCE(d.referenced_entity_name, '') AS referenced_entity_name
FROM sys.sql_expression_dependencies AS d
INNER JOIN sys.views AS v
    ON v.object_id = d.referencing_id
INNER JOIN sys.schemas AS s
    ON s.schema_id = v.schema_id
LEFT JOIN sys.schemas AS rs
    ON rs.schema_id = d.referenced_schema_id
WHERE s.name = ?
  AND v.name = ?
ORDER BY referenced_database_name, referenced_schema_name, referenced_entity_name
"""

VIEW_INDEXES_SQL = """
SELECT COUNT_BIG(*) AS index_count
FROM sys.indexes AS i
INNER JOIN sys.views AS v
    ON v.object_id = i.object_id
INNER JOIN sys.schemas AS s
    ON s.schema_id = v.schema_id
WHERE s.name = ?
  AND v.name = ?
  AND i.index_id > 0
"""

VIEW_MARKER_SQL = """
SELECT
    ep.name,
    CONVERT(nvarchar(max), ep.value) AS marker_value
FROM sys.extended_properties AS ep
WHERE ep.class = 1
  AND ep.major_id = OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?), N'V')
  AND ep.minor_id = 0
  AND LEFT(ep.name, ?) = ?
ORDER BY ep.name
"""

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_@$#]*$")
_PRODUCTION_ENVIRONMENTS = frozenset({"production", "prod", "live"})
_MARKER_NAME_PREFIX = "AzureSqlMcp_View_v1_"
_MARKER_VALUE_PATTERN = re.compile(r"^[0-9a-f]{48}$")
_IDEMPOTENCY_DIGEST_PATTERN = re.compile(r"^idempotency-v1:[0-9a-f]{64}$")
_VIEW_HEADER = re.compile(
    r"^\s*(?:(?:--[^\r\n]*(?:\r\n|\r|\n)|/\*.*?\*/\s*)*)"
    r"(?:CREATE(?:\s+OR\s+ALTER)?|ALTER)\s+VIEW\b",
    re.IGNORECASE | re.DOTALL,
)

_VIEW_ATTRIBUTES = frozenset({"ENCRYPTION", "SCHEMABINDING", "VIEW_METADATA"})
_SQL_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_@$#]*")
_INDEXED_VIEW_SET_OPTIONS = (
    ("ANSI_NULLS", "ON"),
    ("QUOTED_IDENTIFIER", "ON"),
    ("ANSI_PADDING", "ON"),
    ("ANSI_WARNINGS", "ON"),
    ("ARITHABORT", "ON"),
    ("CONCAT_NULL_YIELDS_NULL", "ON"),
    ("NUMERIC_ROUNDABORT", "OFF"),
)


class ViewWorkflowError(ValueError):
    """Base error for invalid, stale, or unverifiable view changes."""


class ViewDefinitionError(ViewWorkflowError):
    """Raised when a view body is not an actionable Azure SQL definition."""


class ViewPolicyError(PermissionError, ViewWorkflowError):
    """Raised when a view apply is not allowed by local policy."""


class ViewVerificationError(ViewWorkflowError):
    """Raised when a changed view cannot be proven to match its contract."""


@dataclass(frozen=True)
class ViewLegalityReport:
    valid: bool
    errors: tuple[str, ...]
    dependencies: tuple[str, ...]
    schema_bound: bool
    normalized_definition: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "errors": list(self.errors),
            "dependencies": list(self.dependencies),
            "schema_bound": self.schema_bound,
            "normalized_definition": self.normalized_definition,
        }


@dataclass(frozen=True)
class ViewHeader:
    """Validated, replayable state from an existing view header."""

    leading_comments: str = ""
    column_list: tuple[str, ...] = ()
    attributes: tuple[str, ...] = ()
    raw_suffix: str = ""

    @property
    def schema_bound(self) -> bool:
        return "SCHEMABINDING" in self.attributes

    def as_dict(self) -> dict[str, Any]:
        return {
            "leading_comments": self.leading_comments,
            "column_list": list(self.column_list),
            "attributes": list(self.attributes),
            "raw_suffix": self.raw_suffix,
        }


@dataclass(frozen=True)
class _ParsedViewDefinition:
    header: ViewHeader
    body: str


@dataclass(frozen=True)
class ViewSnapshot:
    database_name: str
    schema_name: str
    view_name: str
    exists: bool
    object_id: int | None = None
    definition: str | None = None
    definition_fingerprint: str | None = None
    dependencies: tuple[str, ...] = ()
    schema_bound: bool = False
    uses_ansi_nulls: bool = True
    uses_quoted_identifier: bool = True
    header: ViewHeader | None = None
    index_count: int = 0
    marker_name: str | None = None
    marker_present: bool = False
    marker_value: str | None = None
    reserved_marker_names: tuple[str, ...] = ()
    dispatch_proof: dict[str, str] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "schema_name",
            _canonical_identifier(self.schema_name, "schema_name"),
        )
        object.__setattr__(
            self,
            "view_name",
            _canonical_identifier(self.view_name, "view_name"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "database_name": self.database_name,
            "schema_name": self.schema_name,
            "view_name": self.view_name,
            "exists": self.exists,
            "object_id": self.object_id,
            "definition": self.definition,
            "definition_fingerprint": self.definition_fingerprint,
            "dependencies": list(self.dependencies),
            "schema_bound": self.schema_bound,
            "uses_ansi_nulls": self.uses_ansi_nulls,
            "uses_quoted_identifier": self.uses_quoted_identifier,
            "header": self.header.as_dict() if self.header is not None else None,
            "index_count": self.index_count,
            "marker_name": self.marker_name,
            "marker_present": self.marker_present,
            "marker_value": self.marker_value,
            "reserved_marker_names": list(self.reserved_marker_names),
        }


@dataclass(frozen=True)
class ViewChangeRequest:
    database_name: str
    schema_name: str
    view_name: str
    definition: str
    operation: str = "auto"
    reviewed_intent: bool = False
    idempotency_key: str | None = None
    indexed_view: bool = False
    schema_bound: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "schema_name",
            _canonical_identifier(self.schema_name, "schema_name"),
        )
        object.__setattr__(
            self,
            "view_name",
            _canonical_identifier(self.view_name, "view_name"),
        )
        if not isinstance(self.database_name, str) or not self.database_name.strip():
            raise ViewDefinitionError("database_name must not be empty")
        if self.operation.casefold() not in {"auto", "create", "alter"}:
            raise ViewDefinitionError("operation must be auto, create, or alter")
        if not isinstance(self.reviewed_intent, bool):
            raise ViewDefinitionError("reviewed_intent must be a boolean")
        if self.indexed_view and not self.schema_bound:
            raise ViewDefinitionError(
                "indexed_view requires schema_bound=True for Azure SQL legality"
            )


@dataclass(frozen=True)
class PreparedViewChange:
    request: ViewChangeRequest
    operation: str
    apply_sql: str | None
    rollback_sql: str
    prior: ViewSnapshot
    target_fingerprint: str
    target_dependencies: tuple[str, ...]
    legality: ViewLegalityReport
    marker_name: str
    marker_value: str

    @property
    def idempotency_key(self) -> str | None:
        return self.request.idempotency_key

    @property
    def target_uses_ansi_nulls(self) -> bool:
        return _target_ansi_nulls(self.request, self.prior)

    @property
    def target_uses_quoted_identifier(self) -> bool:
        return _target_quoted_identifier(self.request, self.prior)

    def as_dict(self) -> dict[str, Any]:
        return {
            "database_name": self.request.database_name,
            "schema_name": self.request.schema_name,
            "view_name": self.request.view_name,
            "operation": self.operation,
            "apply_sql": self.apply_sql,
            "rollback_sql": self.rollback_sql,
            "prior": self.prior.as_dict(),
            "target_fingerprint": self.target_fingerprint,
            "target_dependencies": list(self.target_dependencies),
            "marker_name": self.marker_name,
            "target_uses_ansi_nulls": self.target_uses_ansi_nulls,
            "target_uses_quoted_identifier": self.target_uses_quoted_identifier,
            "legality": self.legality.as_dict(),
            "reviewed_intent": self.request.reviewed_intent,
            "idempotency_key": self.request.idempotency_key,
        }


@dataclass(frozen=True)
class ViewVerification:
    verified: bool
    definition_verified: bool
    dependencies_verified: bool
    module_options_verified: bool
    marker_verified: bool
    workflow_commit_proven: bool
    actual: ViewSnapshot
    expected_fingerprint: str
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "verified": self.verified,
            "definition_verified": self.definition_verified,
            "dependencies_verified": self.dependencies_verified,
            "module_options_verified": self.module_options_verified,
            "marker_verified": self.marker_verified,
            "workflow_commit_proven": self.workflow_commit_proven,
            "actual": self.actual.as_dict(),
            "expected_fingerprint": self.expected_fingerprint,
            "reason": self.reason,
        }


def validate_view_definition(
    definition: str,
    *,
    indexed_view: bool = False,
    schema_bound: bool = False,
) -> ViewLegalityReport:
    """Validate one SELECT-shaped view body without querying schema metadata."""

    text = str(definition or "").strip()
    errors: list[str] = []
    if not text:
        errors.append("view definition must not be empty")
    if _VIEW_HEADER.match(text):
        errors.append("definition must be the SELECT body, not CREATE/ALTER VIEW")
    if re.search(
        r"(?im)^\s*GO(?:\s+\d+)?\s*$",
        strip_literals_and_comments(text),
    ):
        errors.append("batch separator GO is not allowed in a view definition")

    statement: Any = None
    if not errors:
        try:
            statements = sqlglot.parse(text, read="tsql")
        except Exception as exc:
            errors.append(f"definition is not parseable T-SQL: {type(exc).__name__}")
        else:
            if len(statements) != 1:
                errors.append("exactly one SELECT statement is required")
            else:
                statement = statements[0]
                if statement.key not in {"select", "union", "intersect", "except"}:
                    errors.append("view definition must be a SELECT-shaped statement")
                elif any(table.catalog for table in statement.find_all(exp.Table)):
                    errors.append(
                        "Azure SQL Database views cannot use direct three- or "
                        "four-part cross-database object names; expose remote "
                        "objects through local external tables"
                    )

    upper = strip_literals_and_comments(text).upper()
    if re.search(r"\bINTO\b", upper):
        errors.append("SELECT INTO is not legal in a view")
    if re.search(r"\bOPTION\s*\(", upper):
        errors.append("query OPTION hints are not legal in a view")
    if re.search(r"\bORDER\s+BY\b", upper) and not re.search(
        r"\bTOP\b|\bOFFSET\b", upper
    ):
        errors.append("ORDER BY requires TOP or OFFSET in an Azure SQL view")
    if re.search(r"(?:^|\s)#\w+", text):
        errors.append("temporary tables are not legal in a view")

    if schema_bound or indexed_view:
        errors.extend(_validate_schema_bound_view(statement, upper))
    if indexed_view:
        if not schema_bound:
            errors.append("indexed views require WITH SCHEMABINDING")
        errors.extend(_validate_indexed_view(statement, upper))

    dependencies = extract_view_dependencies(text)
    return ViewLegalityReport(
        valid=not errors,
        errors=tuple(dict.fromkeys(errors)),
        dependencies=dependencies,
        schema_bound=schema_bound,
        normalized_definition=_normalize_view_body(text),
    )


def require_valid_view_definition(
    definition: str,
    *,
    indexed_view: bool = False,
    schema_bound: bool = False,
) -> ViewLegalityReport:
    report = validate_view_definition(
        definition,
        indexed_view=indexed_view,
        schema_bound=schema_bound,
    )
    if not report.valid:
        raise ViewDefinitionError("; ".join(report.errors))
    return report


def _validate_schema_bound_view(statement: Any, upper: str) -> list[str]:
    """Apply the conservative subset we can prove safe without catalog metadata."""

    errors: list[str] = []
    if statement is None:
        return ["schema-bound/indexed view definition could not be analyzed"]

    cte_names = {
        cte.alias_or_name.casefold()
        for cte in statement.find_all(exp.CTE)
        if cte.alias_or_name
    }
    if statement.find(exp.With) is not None or cte_names:
        errors.append("CTEs are not supported in schema-bound/indexed views")
    if statement.find(exp.Subquery) is not None:
        errors.append("subqueries are not supported in schema-bound/indexed views")

    for star in statement.find_all(exp.Star):
        parent = star.parent
        if not (
            isinstance(parent, exp.Count)
            and bool(parent.args.get("big_int"))
        ):
            errors.append("SELECT * is not supported in schema-bound/indexed views")
            break

    object_names: set[str] = set()
    for table in statement.find_all(exp.Table):
        table_name = table.name.casefold()
        if table_name in cte_names:
            continue
        if isinstance(table.this, exp.Anonymous):
            errors.append(
                "inline table-valued functions are not supported in "
                "schema-bound/indexed views"
            )
            continue
        if table.args.get("version") is not None:
            errors.append("temporal FOR SYSTEM_TIME references are not supported")
        parts = [part for part in (table.catalog, table.db, table.name) if part]
        if len(parts) != 2:
            errors.append(
                "schema-bound/indexed view objects must use exactly two-part "
                "local names (<schema>.<object>)"
            )
        object_key = ".".join(parts).casefold()
        if object_key in object_names:
            errors.append("self-joins are not supported in schema-bound/indexed views")
        object_names.add(object_key)

    if any(column.catalog for column in statement.find_all(exp.Column)):
        errors.append(
            "schema-bound/indexed view column references cannot use a database qualifier"
        )

    for pattern, message in (
        (r"\bUNION(?:\s+ALL)?\b|\bEXCEPT\b|\bINTERSECT\b", "set operators are not supported in schema-bound/indexed views"),
        (r"\b(?:LEFT|RIGHT|FULL|OUTER)\s+JOIN\b", "outer joins are not supported in schema-bound/indexed views"),
        (r"\b(?:CROSS|OUTER)\s+APPLY\b|\bAPPLY\b", "APPLY is not supported in schema-bound/indexed views"),
        (r"\bDISTINCT\b", "DISTINCT is not supported in schema-bound/indexed views"),
        (r"\bHAVING\b", "HAVING is not supported in schema-bound/indexed views"),
        (r"\b(?:TOP|OFFSET)\b", "TOP and OFFSET are not supported in schema-bound/indexed views"),
        (r"\bWITH\s*\(", "table hints are not supported in schema-bound/indexed views"),
        (r"\bOVER\s*\(", "window functions are not supported in schema-bound/indexed views"),
        (r"\b(?:PIVOT|UNPIVOT|TABLESAMPLE)\b", "unsupported table operators are not supported in schema-bound/indexed views"),
        (r"\b(?:CONTAINS|FREETEXT|CONTAINSTABLE|FREETEXTTABLE)\b", "full-text predicates are not supported in schema-bound/indexed views"),
        (r"\bFOR\s+(?:XML|JSON|SYSTEM_TIME)\b", "FOR clauses are not supported in schema-bound/indexed views"),
        (r"\b(?:FLOAT|REAL)\b", "imprecise FLOAT/REAL expressions are not supported"),
        (r"\b(?:FORMAT|AT\s+TIME\s+ZONE)\b", "nondeterministic expressions are not supported"),
    ):
        if re.search(pattern, upper):
            errors.append(message)

    if re.search(
        r"\b(?:CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME|CURRENT_USER|"
        r"GETDATE|GETUTCDATE|NEWID|NEWSEQUENTIALID|RAND|SESSION_USER|"
        r"SUSER_ID|SUSER_NAME|SUSER_SNAME|SYSDATETIME(?:OFFSET)?|"
        r"SYSUTCDATETIME|SYSTEM_USER|USER_NAME)\b",
        upper,
    ):
        errors.append("nondeterministic functions are not supported")
    if re.search(r"@@[A-Z_]+", upper):
        errors.append("session and server variables are not supported")
    if re.search(r"\b(?:MIN|MAX|AVG|STDEV|STDEVP|VAR|VARP)\s*\(", upper):
        errors.append("MIN/MAX/AVG/STDEV/VAR aggregates are not supported")

    group_by = statement.find(exp.Group) is not None or bool(
        re.search(r"\bGROUP\s+BY\b", upper)
    )
    if group_by and not any(
        isinstance(count.this, exp.Star) and bool(count.args.get("big_int"))
        for count in statement.find_all(exp.Count)
    ):
        errors.append("GROUP BY requires COUNT_BIG(*)")
    return errors


def _validate_indexed_view(statement: Any, upper: str) -> list[str]:
    if statement is None:
        return ["indexed view definition could not be analyzed"]
    errors: list[str] = []
    for count in statement.find_all(exp.Count):
        if not bool(count.args.get("big_int")):
            errors.append("COUNT is not supported in an indexed view; use COUNT_BIG")
        elif not isinstance(count.this, exp.Star):
            errors.append("indexed views require COUNT_BIG(*) when counting rows")
    for total in statement.find_all(exp.Sum):
        if not isinstance(total.this, exp.Coalesce):
            errors.append(
                "SUM in an indexed view must use ISNULL/COALESCE so its "
                "expression is provably non-null"
            )
    if re.search(r"\b(?:MIN|MAX|AVG|STDEV|STDEVP|VAR|VARP)\s*\(", upper):
        errors.append("MIN/MAX/AVG/STDEV/VAR aggregates are not supported")

    # sqlglot gives known built-ins dedicated nodes. Anonymous calls are either
    # user-defined or syntax this workflow cannot prove deterministic.
    for function in statement.find_all(exp.Anonymous):
        errors.append(
            f"function {function.name or '<unknown>'} is unsupported or its "
            "indexed-view determinism cannot be proven"
        )
    return errors


def extract_view_dependencies(definition: str) -> tuple[str, ...]:
    body = _extract_view_body(definition)
    try:
        statement = sqlglot.parse_one(body, read="tsql")
    except Exception:
        return ()
    cte_names = {
        cte.alias_or_name.casefold()
        for cte in statement.find_all(exp.CTE)
        if cte.alias_or_name
    }
    dependencies: set[str] = set()
    for table in statement.find_all(exp.Table):
        if isinstance(table.this, exp.Anonymous):
            continue
        if table.name.casefold() in cte_names:
            continue
        parts = [part for part in (table.catalog, table.db, table.name) if part]
        if parts:
            dependencies.add(".".join(parts).casefold())
    table_valued_functions = {
        id(table.this): table
        for table in statement.find_all(exp.Table)
        if isinstance(table.this, exp.Anonymous)
    }
    for function in statement.find_all(exp.Anonymous):
        table = table_valued_functions.get(id(function))
        if table is not None:
            parts = [part for part in (table.catalog, table.db, function.name) if part]
        else:
            if not (
                isinstance(function.parent, exp.Dot)
                and function.parent.expression is function
            ):
                continue
            qualifier = _qualified_identifier_parts(function.parent.this)
            parts = [*qualifier, function.name]
        if parts:
            dependencies.add(".".join(parts).casefold())
    return tuple(sorted(dependencies))


def build_view_statement(
    operation: str,
    schema_name: str,
    view_name: str,
    definition: str,
    *,
    schema_bound: bool = False,
    indexed_view: bool = False,
) -> str:
    operation = operation.casefold()
    if operation not in {"create", "alter"}:
        raise ViewDefinitionError("view operation must be create or alter")
    schema_name = _canonical_identifier(schema_name, "schema_name")
    view_name = _canonical_identifier(view_name, "view_name")
    require_valid_view_definition(
        definition,
        indexed_view=indexed_view,
        schema_bound=schema_bound,
    )
    options = " WITH SCHEMABINDING" if schema_bound else ""
    body = str(definition).strip().rstrip(";").strip()
    return (
        f"{operation.upper()} VIEW {_quote_identifier(schema_name)}."
        f"{_quote_identifier(view_name)}{options} AS {body};"
    )


def build_view_execution_batch(
    statement: str,
    *,
    uses_ansi_nulls: bool,
    uses_quoted_identifier: bool,
    indexed_view: bool = False,
    preserve_module_options: bool = False,
) -> str:
    """Execute module DDL as its own nested batch under exact SET options."""

    escaped = statement.replace("'", "''")
    if indexed_view:
        indexed_options = list(_INDEXED_VIEW_SET_OPTIONS)
        if preserve_module_options:
            indexed_options[0] = (
                "ANSI_NULLS",
                "ON" if uses_ansi_nulls else "OFF",
            )
            indexed_options[1] = (
                "QUOTED_IDENTIFIER",
                "ON" if uses_quoted_identifier else "OFF",
            )
        set_statements = [
            f"SET {name} {value};" for name, value in indexed_options
        ]
    else:
        ansi_nulls = "ON" if uses_ansi_nulls else "OFF"
        quoted_identifier = "ON" if uses_quoted_identifier else "OFF"
        set_statements = [
            f"SET ANSI_NULLS {ansi_nulls};",
            f"SET QUOTED_IDENTIFIER {quoted_identifier};",
        ]
    return (
        "\n".join(set_statements)
        + f"\nEXEC sys.sp_executesql N'{escaped}';"
    )


def _build_fenced_view_execution_batch(
    statement: str,
    *,
    database_name: str,
    schema_name: str,
    view_name: str,
    expected_exists: bool,
    expected_object_id: int | None,
    expected_definition: str | None,
    expected_index_count: int,
    expected_schema_bound: bool | None,
    expected_uses_ansi_nulls: bool | None,
    expected_uses_quoted_identifier: bool | None,
    marker_name: str,
    expected_marker_present: bool,
    expected_marker_value: str | None,
    expected_reserved_marker_count: int,
    marker_action: str,
    marker_value: str,
    uses_ansi_nulls: bool,
    uses_quoted_identifier: bool,
    indexed_view: bool = False,
    preserve_module_options: bool = False,
) -> str:
    """Build one atomic, database-side fenced view mutation.

    Existing targets are first mutated through their private extended-property
    marker.  That metadata DDL takes the target object's schema-modification
    lock and the transaction holds it while the exact catalog guards are
    repeated and the requested view DDL runs.  An absent target cannot be
    locked by object, so its second absence check is followed by CREATE VIEW;
    a concurrent creator therefore causes CREATE to fail instead of being
    adopted.  The exact module text is fenced by its SHA-256 over SQL
    Server's nvarchar representation, avoiding an unbounded SQL string
    literal in the guard.
    """

    schema_name = _canonical_identifier(schema_name, "schema_name")
    view_name = _canonical_identifier(view_name, "view_name")
    if expected_index_count < 0:
        raise ViewWorkflowError("expected view index count must be non-negative")
    if expected_exists and expected_object_id is None:
        raise ViewWorkflowError("an existing expected view requires an object id")
    if not expected_exists and (
        expected_object_id is not None or expected_definition is not None
    ):
        raise ViewWorkflowError("an absent expected view cannot contain object data")
    if marker_action not in {"add", "drop", "drop_with_view"}:
        raise ViewWorkflowError("view marker action is invalid")
    _validate_marker_name(marker_name)
    if expected_reserved_marker_count < 0:
        raise ViewWorkflowError(
            "expected reserved view marker count must be non-negative"
        )
    if marker_action == "add" and not marker_value:
        raise ViewWorkflowError("view marker value must not be empty")
    if marker_action in {"drop", "drop_with_view"} and not expected_marker_present:
        raise ViewWorkflowError("view marker drop requires an existing marker")
    if marker_action in {"drop", "drop_with_view"} and expected_reserved_marker_count < 1:
        raise ViewWorkflowError(
            "view marker drop requires a reserved marker inventory"
        )

    set_statements = _view_set_statements(
        uses_ansi_nulls=uses_ansi_nulls,
        uses_quoted_identifier=uses_quoted_identifier,
        indexed_view=indexed_view,
        preserve_module_options=preserve_module_options,
    )
    expected_hash = _definition_sql_hash(expected_definition)
    expected_hash_sql = f"0x{expected_hash}" if expected_hash else "NULL"
    expected_object_sql = (
        str(expected_object_id) if expected_object_id is not None else "NULL"
    )
    resource = _sql_string_literal(
        "AzureSqlMcp.ViewWorkflow:"
        + hashlib.sha256(
            ":".join(
                (
                    database_name.casefold(),
                    schema_name.casefold(),
                    view_name.casefold(),
                )
            ).encode("utf-8")
        ).hexdigest()
    )
    schema_literal = _sql_string_literal(schema_name)
    view_literal = _sql_string_literal(view_name)
    marker_name_literal = _sql_string_literal(marker_name)
    marker_prefix_literal = _sql_string_literal(_MARKER_NAME_PREFIX)
    expected_marker_value_literal = _sql_string_literal(expected_marker_value or "")
    expected_schema_bound_sql = _sql_bit_literal(expected_schema_bound)
    expected_ansi_nulls_sql = _sql_bit_literal(expected_uses_ansi_nulls)
    expected_quoted_identifier_sql = _sql_bit_literal(expected_uses_quoted_identifier)
    locked_marker_present = marker_action == "add"
    locked_marker_value = marker_value if locked_marker_present else None
    locked_reserved_marker_count = expected_reserved_marker_count + (
        1 if marker_action == "add" else -1
    )
    locked_marker_value_literal = _sql_string_literal(locked_marker_value or "")
    escaped = statement.replace("'", "''")
    fence_comment = (
        "-- AzureSqlMcp view fence "
        f"expected_exists={int(expected_exists)} "
        f"expected_object_id={expected_object_sql} "
        f"expected_index_count={expected_index_count} "
        f"expected_definition_sha256={expected_hash or 'NULL'} "
        f"expected_schema_bound={expected_schema_bound_sql} "
        f"expected_uses_ansi_nulls={expected_ansi_nulls_sql} "
        f"expected_uses_quoted_identifier={expected_quoted_identifier_sql} "
        f"expected_marker_present={int(expected_marker_present)} "
        f"expected_marker_value={expected_marker_value or 'NULL'} "
        f"expected_reserved_marker_count={expected_reserved_marker_count} "
        f"marker_name={marker_name} marker_action={marker_action}"
    )
    return "\n".join(
        [
            *set_statements,
            "SET XACT_ABORT ON;",
            "BEGIN TRANSACTION;",
            "DECLARE @lock_result int;",
            "EXEC @lock_result = sys.sp_getapplock "
            f"@Resource = N'{resource}', @LockMode = N'Exclusive', "
            "@LockOwner = N'Transaction', @LockTimeout = 0;",
            "IF @lock_result < 0",
            "BEGIN",
            "    ROLLBACK TRANSACTION;",
            "    THROW 51000, N'view mutation lock could not be acquired', 1;",
            "END;",
            fence_comment,
            "DECLARE @actual_exists bit = 0;",
            "DECLARE @actual_object_id int = NULL;",
            "DECLARE @actual_definition nvarchar(max) = NULL;",
            "DECLARE @actual_is_schema_bound bit = NULL;",
            "DECLARE @actual_uses_ansi_nulls bit = NULL;",
            "DECLARE @actual_uses_quoted_identifier bit = NULL;",
            "DECLARE @actual_index_count bigint = 0;",
            "SELECT",
            "    @actual_exists = 1,",
            "    @actual_object_id = v.object_id,",
            "    @actual_definition = m.definition,",
            "    @actual_is_schema_bound = m.is_schema_bound,",
            "    @actual_uses_ansi_nulls = m.uses_ansi_nulls,",
            "    @actual_uses_quoted_identifier = m.uses_quoted_identifier",
            "FROM sys.views AS v",
            "INNER JOIN sys.schemas AS s",
            "    ON s.schema_id = v.schema_id",
            "LEFT JOIN sys.sql_modules AS m",
            "    ON m.object_id = v.object_id",
            f"WHERE s.name = N'{schema_literal}' AND v.name = N'{view_literal}';",
            "SELECT @actual_index_count = COUNT_BIG(*)",
            "FROM sys.indexes AS i",
            "WHERE i.object_id = @actual_object_id AND i.index_id > 0;",
            "DECLARE @actual_marker_present bit = 0;",
            "DECLARE @actual_marker_value nvarchar(max) = NULL;",
            "DECLARE @actual_reserved_marker_count bigint = 0;",
            "SELECT",
            "    @actual_marker_present = 1,",
            "    @actual_marker_value = CONVERT(nvarchar(max), ep.value)",
            "FROM sys.extended_properties AS ep",
            "WHERE ep.class = 1",
            "  AND ep.major_id = @actual_object_id",
            "  AND ep.minor_id = 0",
            f"  AND ep.name = N'{marker_name_literal}';",
            "SELECT @actual_reserved_marker_count = COUNT_BIG(*)",
            "FROM sys.extended_properties AS ep",
            "WHERE ep.class = 1",
            "  AND ep.major_id = @actual_object_id",
            "  AND ep.minor_id = 0",
            f"  AND LEFT(ep.name, {len(_MARKER_NAME_PREFIX)}) "
            f"= N'{marker_prefix_literal}';",
            "DECLARE @actual_definition_sha256 varbinary(32) = NULL;",
            "IF @actual_definition IS NOT NULL",
            "    SET @actual_definition_sha256 = HASHBYTES(",
            "        'SHA2_256', CONVERT(nvarchar(max), @actual_definition)",
            "    );",
            f"IF @actual_exists <> {int(expected_exists)}",
            f"   OR ISNULL(@actual_object_id, -2147483648) <> ISNULL({expected_object_sql}, -2147483648)",
            f"   OR @actual_index_count <> {expected_index_count}",
            f"   OR ISNULL(@actual_definition_sha256, 0x) <> ISNULL({expected_hash_sql}, 0x)",
            f"   OR ISNULL(CONVERT(int, @actual_is_schema_bound), -1) <> ISNULL(CONVERT(int, {expected_schema_bound_sql}), -1)",
            f"   OR ISNULL(CONVERT(int, @actual_uses_ansi_nulls), -1) <> ISNULL(CONVERT(int, {expected_ansi_nulls_sql}), -1)",
            f"   OR ISNULL(CONVERT(int, @actual_uses_quoted_identifier), -1) <> ISNULL(CONVERT(int, {expected_quoted_identifier_sql}), -1)",
            f"   OR @actual_marker_present <> {int(expected_marker_present)}",
            f"   OR ISNULL(@actual_marker_value, N'') <> N'{expected_marker_value_literal}'",
            f"   OR @actual_reserved_marker_count <> {expected_reserved_marker_count}",
            "BEGIN",
            "    ROLLBACK TRANSACTION;",
            "    THROW 51001, N'view mutation precondition no longer matches', 1;",
            "END;",
            *(
                [
                    "DECLARE @marker_result int;",
                    _build_marker_mutation_sql(
                        marker_action,
                        marker_name=marker_name,
                        marker_value=marker_value,
                        schema_name=schema_name,
                        view_name=view_name,
                        capture_return_code=True,
                    ),
                    "IF @marker_result <> 0",
                    "BEGIN",
                    "    ROLLBACK TRANSACTION;",
                    "    THROW 51002, N'view mutation marker lock could not be acquired', 1;",
                    "END;",
                ]
                if expected_exists
                else []
            ),
            "SET @actual_exists = 0;",
            "SET @actual_object_id = NULL;",
            "SET @actual_definition = NULL;",
            "SET @actual_is_schema_bound = NULL;",
            "SET @actual_uses_ansi_nulls = NULL;",
            "SET @actual_uses_quoted_identifier = NULL;",
            "SET @actual_index_count = 0;",
            "SELECT",
            "    @actual_exists = 1,",
            "    @actual_object_id = v.object_id,",
            "    @actual_definition = m.definition,",
            "    @actual_is_schema_bound = m.is_schema_bound,",
            "    @actual_uses_ansi_nulls = m.uses_ansi_nulls,",
            "    @actual_uses_quoted_identifier = m.uses_quoted_identifier",
            "FROM sys.views AS v",
            "INNER JOIN sys.schemas AS s",
            "    ON s.schema_id = v.schema_id",
            "LEFT JOIN sys.sql_modules AS m",
            "    ON m.object_id = v.object_id",
            f"WHERE s.name = N'{schema_literal}' AND v.name = N'{view_literal}';",
            "SELECT @actual_index_count = COUNT_BIG(*)",
            "FROM sys.indexes AS i",
            "WHERE i.object_id = @actual_object_id AND i.index_id > 0;",
            "SET @actual_marker_present = 0;",
            "SET @actual_marker_value = NULL;",
            "SET @actual_reserved_marker_count = 0;",
            "SELECT",
            "    @actual_marker_present = 1,",
            "    @actual_marker_value = CONVERT(nvarchar(max), ep.value)",
            "FROM sys.extended_properties AS ep",
            "WHERE ep.class = 1",
            "  AND ep.major_id = @actual_object_id",
            "  AND ep.minor_id = 0",
            f"  AND ep.name = N'{marker_name_literal}';",
            "SELECT @actual_reserved_marker_count = COUNT_BIG(*)",
            "FROM sys.extended_properties AS ep",
            "WHERE ep.class = 1",
            "  AND ep.major_id = @actual_object_id",
            "  AND ep.minor_id = 0",
            f"  AND LEFT(ep.name, {len(_MARKER_NAME_PREFIX)}) = N'{marker_prefix_literal}';",
            "SET @actual_definition_sha256 = NULL;",
            "IF @actual_definition IS NOT NULL",
            "    SET @actual_definition_sha256 = HASHBYTES(",
            "        'SHA2_256', CONVERT(nvarchar(max), @actual_definition)",
            "    );",
            f"IF @actual_exists <> {int(expected_exists)}",
            f"   OR ISNULL(@actual_object_id, -2147483648) <> ISNULL({expected_object_sql}, -2147483648)",
            f"   OR @actual_index_count <> {expected_index_count}",
            f"   OR ISNULL(@actual_definition_sha256, 0x) <> ISNULL({expected_hash_sql}, 0x)",
            f"   OR ISNULL(CONVERT(int, @actual_is_schema_bound), -1) <> ISNULL(CONVERT(int, {expected_schema_bound_sql}), -1)",
            f"   OR ISNULL(CONVERT(int, @actual_uses_ansi_nulls), -1) <> ISNULL(CONVERT(int, {expected_ansi_nulls_sql}), -1)",
            f"   OR ISNULL(CONVERT(int, @actual_uses_quoted_identifier), -1) <> ISNULL(CONVERT(int, {expected_quoted_identifier_sql}), -1)",
            f"   OR @actual_marker_present <> {int(locked_marker_present if expected_exists else expected_marker_present)}",
            f"   OR ISNULL(@actual_marker_value, N'') <> N'{locked_marker_value_literal if expected_exists else expected_marker_value_literal}'",
            f"   OR @actual_reserved_marker_count <> {locked_reserved_marker_count if expected_exists else expected_reserved_marker_count}",
            "BEGIN",
            "    ROLLBACK TRANSACTION;",
            "    THROW 51001, N'view mutation precondition no longer matches', 1;",
            "END;",
            f"EXEC sys.sp_executesql N'{escaped}';",
            *(
                [
                    _build_marker_mutation_sql(
                        marker_action,
                        marker_name=marker_name,
                        marker_value=marker_value,
                        schema_name=schema_name,
                        view_name=view_name,
                    )
                ]
                if not expected_exists and marker_action == "add"
                else []
            ),
            "COMMIT TRANSACTION;",
        ]
    )


def _sql_bit_literal(value: bool | None) -> str:
    if value is None:
        return "NULL"
    return "1" if value else "0"


def _view_set_statements(
    *,
    uses_ansi_nulls: bool,
    uses_quoted_identifier: bool,
    indexed_view: bool,
    preserve_module_options: bool,
) -> list[str]:
    if indexed_view:
        indexed_options = list(_INDEXED_VIEW_SET_OPTIONS)
        if preserve_module_options:
            indexed_options[0] = (
                "ANSI_NULLS",
                "ON" if uses_ansi_nulls else "OFF",
            )
            indexed_options[1] = (
                "QUOTED_IDENTIFIER",
                "ON" if uses_quoted_identifier else "OFF",
            )
        return [f"SET {name} {value};" for name, value in indexed_options]
    return [
        f"SET ANSI_NULLS {'ON' if uses_ansi_nulls else 'OFF'};",
        f"SET QUOTED_IDENTIFIER {'ON' if uses_quoted_identifier else 'OFF'};",
    ]


def _definition_sql_hash(definition: str | None) -> str | None:
    if definition is None:
        return None
    return hashlib.sha256(definition.encode("utf-16le")).hexdigest()


def _sql_string_literal(value: str) -> str:
    return str(value).replace("'", "''")


def _validate_marker_name(marker_name: str) -> str:
    prefix = _MARKER_NAME_PREFIX
    token = str(marker_name)[len(prefix) :]
    if not str(marker_name).startswith(prefix) or not _MARKER_VALUE_PATTERN.fullmatch(
        token
    ):
        raise ViewWorkflowError("view marker name is not an MCP-generated property")
    return token


def _validate_reserved_marker_inventory(names: tuple[str, ...]) -> None:
    for name in names:
        if not _MARKER_VALUE_PATTERN.fullmatch(
            str(name)[len(_MARKER_NAME_PREFIX) :]
        ) or not str(name).startswith(_MARKER_NAME_PREFIX):
            raise ViewWorkflowError(
                "view marker inventory contains an unsupported MCP marker name"
            )


def _new_marker_name() -> str:
    return _MARKER_NAME_PREFIX + secrets.token_hex(24)


def _view_receipt_identity(
    request: ViewChangeRequest,
    *,
    operation: str,
    target_fingerprint: str,
    prior: ViewSnapshot,
) -> str:
    material = ":".join(
        (
            request.database_name.casefold(),
            request.schema_name.casefold(),
            request.view_name.casefold(),
            operation,
            target_fingerprint,
            prior.definition_fingerprint or "absent",
            _view_idempotency_identity(request.idempotency_key),
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _view_idempotency_identity(value: str | None) -> str:
    if value is None:
        return ""
    if _IDEMPOTENCY_DIGEST_PATTERN.fullmatch(value):
        return value
    digest = hashlib.sha256(
        f"idempotency-v1:view-change.intent:{value}".encode("utf-8")
    ).hexdigest()
    return f"idempotency-v1:{digest}"


def _build_marker_value(
    request: ViewChangeRequest,
    *,
    operation: str,
    target_fingerprint: str,
    prior: ViewSnapshot,
    marker_name: str,
) -> str:
    token = _validate_marker_name(marker_name)
    return ":".join(
        (
            "v1",
            token,
            _view_receipt_identity(
                request,
                operation=operation,
                target_fingerprint=target_fingerprint,
                prior=prior,
            ),
            target_fingerprint,
        )
    )


def _validate_prepared_marker(
    request: ViewChangeRequest,
    *,
    operation: str,
    target_fingerprint: str,
    prior: ViewSnapshot,
    marker_name: str,
    marker_value: str,
) -> None:
    _validate_marker_name(marker_name)
    expected = _build_marker_value(
        request,
        operation=operation,
        target_fingerprint=target_fingerprint,
        prior=prior,
        marker_name=marker_name,
    )
    if marker_value != expected:
        raise ViewWorkflowError("durable view marker identity is invalid")
    if prior.marker_name != marker_name:
        raise ViewWorkflowError("durable view marker target is inconsistent")
    if operation != "noop" and prior.reserved_marker_names:
        raise ViewWorkflowError(
            "reserved Azure SQL MCP view marker already exists; exact workflow "
            "ownership cannot be established or safely restored"
        )


def _build_marker_mutation_sql(
    action: str,
    *,
    marker_name: str,
    marker_value: str,
    schema_name: str,
    view_name: str,
    capture_return_code: bool = False,
) -> str:
    _validate_marker_name(marker_name)
    schema_name = _canonical_identifier(schema_name, "schema_name")
    view_name = _canonical_identifier(view_name, "view_name")
    quoted_name = _sql_string_literal(marker_name)
    quoted_schema = _sql_string_literal(schema_name)
    quoted_view = _sql_string_literal(view_name)
    execute_prefix = "EXEC @marker_result = " if capture_return_code else "EXEC "
    if action == "add":
        return (
            f"{execute_prefix}sys.sp_addextendedproperty "
            f"@name = N'{quoted_name}', "
            f"@value = N'{_sql_string_literal(marker_value)}', "
            "@level0type = N'SCHEMA', "
            f"@level0name = N'{quoted_schema}', "
            "@level1type = N'VIEW', "
            f"@level1name = N'{quoted_view}';"
        )
    if action == "drop":
        return (
            f"{execute_prefix}sys.sp_dropextendedproperty "
            f"@name = N'{quoted_name}', "
            "@level0type = N'SCHEMA', "
            f"@level0name = N'{quoted_schema}', "
            "@level1type = N'VIEW', "
            f"@level1name = N'{quoted_view}';"
        )
    if action == "drop_with_view":
        return (
            f"{execute_prefix}sys.sp_dropextendedproperty "
            f"@name = N'{quoted_name}', "
            "@level0type = N'SCHEMA', "
            f"@level0name = N'{quoted_schema}', "
            "@level1type = N'VIEW', "
            f"@level1name = N'{quoted_view}';"
        )
    raise ViewWorkflowError("view marker action is invalid")


def view_definition_fingerprint(
    definition: str,
    *,
    schema_bound: bool | None = None,
    uses_ansi_nulls: bool = True,
    uses_quoted_identifier: bool = True,
) -> str:
    """Fingerprint the body, binding state, and persisted module SET options.

    A body-only definition has no header, so its binding state defaults to
    unbound. A module definition derives the default from its validated header.
    Callers capturing catalog state should pass the catalog value explicitly.
    """

    parsed = _parse_view_module(definition)
    effective_schema_bound = (
        parsed.header.schema_bound
        if schema_bound is None and parsed is not None
        else bool(schema_bound)
    )
    header_identity = _normalized_view_header_identity(
        parsed.header if parsed is not None else None,
        schema_bound=effective_schema_bound,
    )
    normalized = _normalize_view_body(definition)
    material = (
        f"header={header_identity!r}"
        f"\x00schema_bound={int(effective_schema_bound)}"
        f"\x00ansi_nulls={int(uses_ansi_nulls)}"
        f"\x00quoted_identifier={int(uses_quoted_identifier)}"
        f"\x00{normalized}"
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def prepared_view_change_state(prepared: PreparedViewChange) -> dict[str, Any]:
    """Serialize the exact raw-SQL state required for restart-safe rollback."""

    request = prepared.request
    prior = prepared.prior
    return {
        "state_version": 1,
        "request": {
            "database_name": request.database_name,
            "schema_name": request.schema_name,
            "view_name": request.view_name,
            "definition": request.definition,
            "operation": request.operation,
            "reviewed_intent": request.reviewed_intent,
            "idempotency_key": request.idempotency_key,
            "indexed_view": request.indexed_view,
            "schema_bound": request.schema_bound,
        },
        "operation": prepared.operation,
        "prior": {
            "database_name": prior.database_name,
            "schema_name": prior.schema_name,
            "view_name": prior.view_name,
            "exists": prior.exists,
            "object_id": prior.object_id,
            "definition": prior.definition,
            "dependencies": list(prior.dependencies),
            "schema_bound": prior.schema_bound,
            "uses_ansi_nulls": prior.uses_ansi_nulls,
            "uses_quoted_identifier": prior.uses_quoted_identifier,
            "index_count": prior.index_count,
            "marker_name": prior.marker_name,
            "marker_present": prior.marker_present,
            "marker_value": prior.marker_value,
            "reserved_marker_names": list(prior.reserved_marker_names),
        },
        "marker_name": prepared.marker_name,
        "marker_value": prepared.marker_value,
    }


def prepared_view_change_from_state(payload: Any) -> PreparedViewChange:
    """Rebuild and revalidate a durable view intent without trusting stored SQL."""

    if not isinstance(payload, dict) or payload.get("state_version") != 1:
        raise ViewWorkflowError("unsupported durable view change state")
    request_payload = payload.get("request")
    prior_payload = payload.get("prior")
    if not isinstance(request_payload, dict) or not isinstance(prior_payload, dict):
        raise ViewWorkflowError("durable view change state is incomplete")
    try:
        request = ViewChangeRequest(
            database_name=str(request_payload["database_name"]),
            schema_name=str(request_payload["schema_name"]),
            view_name=str(request_payload["view_name"]),
            definition=str(request_payload["definition"]),
            operation=str(request_payload.get("operation") or "auto"),
            reviewed_intent=_state_bool(
                request_payload.get("reviewed_intent", False),
                "reviewed_intent",
            ),
            idempotency_key=(
                str(request_payload["idempotency_key"])
                if request_payload.get("idempotency_key") is not None
                else None
            ),
            indexed_view=_state_bool(
                request_payload.get("indexed_view", False),
                "indexed_view",
            ),
            schema_bound=_state_bool(
                request_payload.get("schema_bound", False),
                "schema_bound",
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ViewWorkflowError("durable view request is invalid") from exc

    prior = _view_snapshot_from_state(prior_payload)
    if (
        prior.database_name.casefold() != request.database_name.casefold()
        or prior.schema_name.casefold() != request.schema_name.casefold()
        or prior.view_name.casefold() != request.view_name.casefold()
    ):
        raise ViewWorkflowError("durable view prior state belongs to another object")

    legality = require_valid_view_definition(
        request.definition,
        indexed_view=request.indexed_view,
        schema_bound=request.schema_bound,
    )
    operation = str(payload.get("operation") or "").casefold()
    if operation not in {"create", "alter", "noop"}:
        raise ViewWorkflowError("durable view operation is invalid")
    target_fingerprint = view_definition_fingerprint(
        request.definition,
        schema_bound=request.schema_bound,
        uses_ansi_nulls=_target_ansi_nulls(request, prior),
        uses_quoted_identifier=_target_quoted_identifier(request, prior),
    )
    marker_name = str(payload.get("marker_name") or "")
    marker_value = str(payload.get("marker_value") or "")
    _validate_prepared_marker(
        request,
        operation=operation,
        target_fingerprint=target_fingerprint,
        prior=prior,
        marker_name=marker_name,
        marker_value=marker_value,
    )
    if operation == "create" and prior.exists:
        raise ViewWorkflowError("durable CREATE VIEW state has an existing prior object")
    if operation == "alter" and not prior.exists:
        raise ViewWorkflowError("durable ALTER VIEW state has no prior object")
    if operation == "noop" and (
        not prior.exists or prior.definition_fingerprint != target_fingerprint
    ):
        raise ViewWorkflowError("durable no-op view state does not match its target")
    if prior.exists and prior.index_count > 0 and operation != "noop":
        raise ViewWorkflowError(
            "cannot alter a view with existing indexes; exact index rollback is not implemented"
        )

    apply_sql = None
    if operation != "noop":
        apply_sql = _build_fenced_view_execution_batch(
            build_view_statement(
                operation,
                request.schema_name,
                request.view_name,
                request.definition,
                schema_bound=request.schema_bound,
                indexed_view=request.indexed_view,
            ),
            database_name=request.database_name,
            schema_name=request.schema_name,
            view_name=request.view_name,
            expected_exists=prior.exists,
            expected_object_id=prior.object_id,
            expected_definition=prior.definition,
            expected_index_count=prior.index_count,
            expected_schema_bound=prior.schema_bound if prior.exists else None,
            expected_uses_ansi_nulls=(
                prior.uses_ansi_nulls if prior.exists else None
            ),
            expected_uses_quoted_identifier=(
                prior.uses_quoted_identifier if prior.exists else None
            ),
            marker_name=marker_name,
            expected_marker_present=prior.marker_present,
            expected_marker_value=prior.marker_value,
            expected_reserved_marker_count=len(prior.reserved_marker_names),
            marker_action="add",
            marker_value=marker_value,
            uses_ansi_nulls=_target_ansi_nulls(request, prior),
            uses_quoted_identifier=_target_quoted_identifier(request, prior),
            indexed_view=request.indexed_view,
        )
    rollback_sql = (
        _build_snapshot_restore_statement(prior, indexed_view=request.indexed_view)
        if prior.exists
        else (
            f"DROP VIEW {_quote_identifier(request.schema_name)}."
            f"{_quote_identifier(request.view_name)};"
        )
    )
    return PreparedViewChange(
        request=request,
        operation=operation,
        apply_sql=apply_sql,
        rollback_sql=rollback_sql,
        prior=prior,
        target_fingerprint=target_fingerprint,
        target_dependencies=legality.dependencies,
        legality=legality,
        marker_name=marker_name,
        marker_value=marker_value,
    )


def view_apply_receipt(snapshot: ViewSnapshot) -> dict[str, Any]:
    if (
        not snapshot.exists
        or snapshot.object_id is None
        or snapshot.definition is None
        or snapshot.definition_fingerprint is None
        or snapshot.marker_name is None
        or not snapshot.marker_present
        or snapshot.marker_value is None
        or snapshot.reserved_marker_names != (snapshot.marker_name,)
    ):
        raise ViewWorkflowError(
            "a confirmed workflow-marked view snapshot is required for an apply receipt"
        )
    if snapshot.dispatch_proof is not None:
        _validate_dispatch_proof(snapshot.dispatch_proof, snapshot)
    receipt = {
        "database_name": snapshot.database_name,
        "schema_name": snapshot.schema_name,
        "view_name": snapshot.view_name,
        "object_id": snapshot.object_id,
        "definition": snapshot.definition,
        "definition_fingerprint": snapshot.definition_fingerprint,
        "dependencies": list(snapshot.dependencies),
        "schema_bound": snapshot.schema_bound,
        "uses_ansi_nulls": snapshot.uses_ansi_nulls,
        "uses_quoted_identifier": snapshot.uses_quoted_identifier,
        "index_count": snapshot.index_count,
        "marker_name": snapshot.marker_name,
        "marker_present": snapshot.marker_present,
        "marker_value": snapshot.marker_value,
        "reserved_marker_names": list(snapshot.reserved_marker_names),
    }
    if snapshot.dispatch_proof is not None:
        receipt["dispatch_proof"] = dict(snapshot.dispatch_proof)
    return receipt


def view_snapshot_from_receipt(payload: Any) -> ViewSnapshot:
    if not isinstance(payload, dict):
        raise ViewWorkflowError("durable view apply receipt is invalid")
    try:
        definition_value = payload.get("definition")
        if not isinstance(definition_value, str) or not definition_value.strip():
            raise ViewWorkflowError(
                "durable view apply receipt has no exact committed definition"
            )
        parsed = _parse_view_module(definition_value)
        if parsed is None:
            raise ViewWorkflowError(
                "durable view apply receipt has an unsupported definition header"
            )
        snapshot = ViewSnapshot(
            database_name=str(payload["database_name"]),
            schema_name=str(payload["schema_name"]),
            view_name=str(payload["view_name"]),
            exists=True,
            object_id=int(payload["object_id"]),
            definition=definition_value,
            definition_fingerprint=str(payload["definition_fingerprint"]),
            dependencies=tuple(str(item) for item in payload.get("dependencies", ())),
            schema_bound=_state_bool(
                payload["schema_bound"],
                "receipt schema_bound",
            ),
            uses_ansi_nulls=_state_bool(
                payload["uses_ansi_nulls"],
                "receipt uses_ansi_nulls",
            ),
            uses_quoted_identifier=_state_bool(
                payload["uses_quoted_identifier"],
                "receipt uses_quoted_identifier",
            ),
            index_count=_non_negative_int(
                payload["index_count"],
                "receipt index_count",
            ),
            marker_name=str(payload["marker_name"]),
            marker_present=_state_bool(
                payload["marker_present"],
                "receipt marker_present",
            ),
            marker_value=(
                str(payload["marker_value"])
                if payload.get("marker_value") is not None
                else None
            ),
            reserved_marker_names=tuple(
                sorted(
                    {
                        str(item)
                        for item in payload.get("reserved_marker_names", ())
                    }
                )
            ),
            header=parsed.header,
            dispatch_proof=(
                _parse_dispatch_proof(payload["dispatch_proof"])
                if payload.get("dispatch_proof") is not None
                else None
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ViewWorkflowError("durable view apply receipt is invalid") from exc
    if not snapshot.definition_fingerprint:
        raise ViewWorkflowError("durable view apply receipt has no target fingerprint")
    if not isinstance(snapshot.definition, str):
        raise ViewWorkflowError("durable view apply receipt has no definition")
    if view_definition_fingerprint(
        snapshot.definition,
        schema_bound=snapshot.schema_bound,
        uses_ansi_nulls=snapshot.uses_ansi_nulls,
        uses_quoted_identifier=snapshot.uses_quoted_identifier,
    ) != snapshot.definition_fingerprint:
        raise ViewWorkflowError("durable view apply receipt definition is inconsistent")
    _require_azure_sql_module_options(snapshot)
    if (
        snapshot.marker_name is None
        or not snapshot.marker_present
        or snapshot.marker_value is None
    ):
        raise ViewWorkflowError("durable view apply receipt has no workflow marker")
    _validate_marker_name(snapshot.marker_name)
    _validate_reserved_marker_inventory(snapshot.reserved_marker_names)
    if snapshot.reserved_marker_names != (snapshot.marker_name,):
        raise ViewWorkflowError(
            "durable view apply receipt has an ambiguous marker inventory"
        )
    return snapshot


def _parse_dispatch_proof(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ViewWorkflowError(
            "durable view apply receipt has no committed dispatch proof"
        )
    proof = {str(key): str(item) for key, item in value.items()}
    required = {"version", "audit_id", "sql_hash", "object_id", "target_fingerprint"}
    if set(proof) != required or proof.get("version") != "1":
        raise ViewWorkflowError("durable view dispatch proof is invalid")
    if not proof["audit_id"].strip() or not re.fullmatch(
        r"[0-9a-f]{64}", proof["sql_hash"]
    ):
        raise ViewWorkflowError("durable view dispatch proof is invalid")
    return proof


def _validate_dispatch_proof(
    proof: dict[str, str],
    snapshot: ViewSnapshot,
) -> None:
    parsed = _parse_dispatch_proof(proof)
    if (
        parsed["object_id"] != str(snapshot.object_id)
        or parsed["target_fingerprint"] != str(snapshot.definition_fingerprint)
    ):
        raise ViewWorkflowError("view dispatch proof does not match the receipt target")


class ViewWorkflowService:
    """Prepare and apply one reviewed view mutation with exact rollback."""

    def __init__(
        self,
        executor,
        database_policy: DatabasePolicySet | DatabasePolicy,
        admin_policy: AdminPolicy,
    ) -> None:
        self.executor = executor
        self.database_policy = database_policy
        self.admin_policy = admin_policy
        self._applied_receipts: dict[str, ViewSnapshot] = {}

    async def capture_view(
        self,
        database_name: str,
        schema_name: str,
        view_name: str,
        *,
        marker_name: str | None = None,
    ) -> ViewSnapshot:
        schema_name = _canonical_identifier(schema_name, "schema_name")
        view_name = _canonical_identifier(view_name, "view_name")
        rows = await self.executor.fetch_all(
            database_name,
            VIEW_DEFINITION_SQL,
            params=[schema_name, view_name],
        )
        if not rows:
            return ViewSnapshot(
                database_name,
                schema_name,
                view_name,
                False,
                marker_name=marker_name,
            )
        row = rows[0]
        definition = row.get("definition")
        if definition is None:
            raise ViewWorkflowError(
                f"view {schema_name}.{view_name} exists but its definition is unavailable"
            )
        definition_text = str(definition)
        parsed = _parse_view_module(definition_text)
        if parsed is None:
            raise ViewWorkflowError(
                f"view {schema_name}.{view_name} has an unsupported definition header"
            )
        dependencies = await self._capture_dependencies(
            database_name,
            schema_name,
            view_name,
        )
        index_count = await self._capture_index_count(
            database_name,
            schema_name,
            view_name,
        )
        catalog_schema_bound = _as_bool(row.get("is_schema_bound"))
        schema_bound = (
            catalog_schema_bound
            if catalog_schema_bound is not None
            else parsed.header.schema_bound
        )
        uses_ansi_nulls = _as_bool(row.get("uses_ansi_nulls"))
        uses_quoted_identifier = _as_bool(row.get("uses_quoted_identifier"))
        if uses_ansi_nulls is None or uses_quoted_identifier is None:
            raise ViewWorkflowError(
                f"view {schema_name}.{view_name} module SET options are unavailable"
            )
        object_id = _as_int(row.get("object_id"))
        if object_id is None:
            raise ViewWorkflowError(
                f"view {schema_name}.{view_name} object identity is unavailable"
            )
        marker_present = False
        marker_value = None
        reserved_marker_names: tuple[str, ...] = ()
        if marker_name is not None:
            _validate_marker_name(marker_name)
            marker_rows = await self.executor.fetch_all(
                database_name,
                VIEW_MARKER_SQL,
                params=[
                    schema_name,
                    view_name,
                    len(_MARKER_NAME_PREFIX),
                    _MARKER_NAME_PREFIX,
                ],
            )
            reserved_marker_names = tuple(
                sorted(
                    {
                        str(row.get("name"))
                        for row in marker_rows
                        if row.get("name") is not None
                    }
                )
            )
            _validate_reserved_marker_inventory(reserved_marker_names)
            exact_marker = next(
                (
                    row
                    for row in marker_rows
                    if str(row.get("name")) == marker_name
                ),
                None,
            )
            if exact_marker is not None:
                marker_present = True
                value = exact_marker.get("marker_value")
                marker_value = str(value) if value is not None else None
        return ViewSnapshot(
            database_name=database_name,
            schema_name=schema_name,
            view_name=view_name,
            exists=True,
            object_id=object_id,
            definition=definition_text,
            definition_fingerprint=view_definition_fingerprint(
                definition_text,
                schema_bound=schema_bound,
                uses_ansi_nulls=uses_ansi_nulls,
                uses_quoted_identifier=uses_quoted_identifier,
            ),
            dependencies=dependencies,
            schema_bound=schema_bound,
            uses_ansi_nulls=uses_ansi_nulls,
            uses_quoted_identifier=uses_quoted_identifier,
            header=parsed.header,
            index_count=index_count,
            marker_name=marker_name,
            marker_present=marker_present,
            marker_value=marker_value,
            reserved_marker_names=reserved_marker_names,
        )

    async def prepare(self, request: ViewChangeRequest) -> PreparedViewChange:
        legality = require_valid_view_definition(
            request.definition,
            indexed_view=request.indexed_view,
            schema_bound=request.schema_bound,
        )
        marker_name = _new_marker_name()
        prior = await self.capture_view(
            request.database_name,
            request.schema_name,
            request.view_name,
            marker_name=marker_name,
        )
        _require_azure_sql_module_options(prior)
        requested_operation = request.operation.casefold()
        if requested_operation == "auto":
            operation = "alter" if prior.exists else "create"
        else:
            operation = requested_operation
        target_fingerprint = view_definition_fingerprint(
            request.definition,
            schema_bound=request.schema_bound,
            uses_ansi_nulls=_target_ansi_nulls(request, prior),
            uses_quoted_identifier=_target_quoted_identifier(request, prior),
        )
        if prior.exists and not prior.definition:
            raise ViewWorkflowError("exact prior definition capture is required")
        if prior.exists and prior.definition_fingerprint == target_fingerprint:
            operation = "noop"
        if operation != "noop" and prior.reserved_marker_names:
            raise ViewWorkflowError(
                "reserved Azure SQL MCP view marker already exists "
                f"({', '.join(prior.reserved_marker_names)}); exact workflow "
                "ownership cannot be established or safely restored"
            )
        if prior.exists and prior.index_count > 0 and operation != "noop":
            raise ViewWorkflowError(
                "cannot alter a view with existing indexes; exact index rollback is not implemented"
            )
        if operation == "create" and prior.exists:
            raise ViewWorkflowError(
                "CREATE VIEW would replace an existing different definition"
            )
        if operation == "alter" and not prior.exists:
            raise ViewWorkflowError("ALTER VIEW requires an existing view")
        marker_value = _build_marker_value(
            request,
            operation=operation,
            target_fingerprint=target_fingerprint,
            prior=prior,
            marker_name=marker_name,
        )

        apply_sql = None
        if operation != "noop":
            apply_sql = _build_fenced_view_execution_batch(
                build_view_statement(
                    operation,
                    request.schema_name,
                    request.view_name,
                    request.definition,
                    schema_bound=request.schema_bound,
                    indexed_view=request.indexed_view,
                ),
                database_name=request.database_name,
                schema_name=request.schema_name,
                view_name=request.view_name,
                expected_exists=prior.exists,
                expected_object_id=prior.object_id,
                expected_definition=prior.definition,
                expected_index_count=prior.index_count,
                expected_schema_bound=prior.schema_bound if prior.exists else None,
                expected_uses_ansi_nulls=(
                    prior.uses_ansi_nulls if prior.exists else None
                ),
                expected_uses_quoted_identifier=(
                    prior.uses_quoted_identifier if prior.exists else None
                ),
                marker_name=marker_name,
                expected_marker_present=prior.marker_present,
                expected_marker_value=prior.marker_value,
                expected_reserved_marker_count=len(prior.reserved_marker_names),
                marker_action="add",
                marker_value=marker_value,
                uses_ansi_nulls=_target_ansi_nulls(request, prior),
                uses_quoted_identifier=_target_quoted_identifier(request, prior),
                indexed_view=request.indexed_view,
            )
        if prior.exists:
            rollback_sql = _build_snapshot_restore_statement(
                prior,
                indexed_view=request.indexed_view,
            )
        else:
            rollback_sql = (
                f"DROP VIEW {_quote_identifier(request.schema_name)}."
                f"{_quote_identifier(request.view_name)};"
            )
        return PreparedViewChange(
            request=request,
            operation=operation,
            apply_sql=apply_sql,
            rollback_sql=rollback_sql,
            prior=prior,
            target_fingerprint=target_fingerprint,
            target_dependencies=legality.dependencies,
            legality=legality,
            marker_name=marker_name,
            marker_value=marker_value,
        )

    async def prepare_view_change(self, request: ViewChangeRequest) -> PreparedViewChange:
        return await self.prepare(request)

    async def preview(self, prepared: PreparedViewChange) -> dict[str, Any]:
        return {
            "status": "dry_run",
            "dry_run": True,
            "apply_allowed": self._apply_policy_reason(prepared.request.database_name)
            is None,
            "policy_reason": self._apply_policy_reason(prepared.request.database_name),
            **prepared.as_dict(),
        }

    async def preview_view_change(self, prepared: PreparedViewChange) -> dict[str, Any]:
        return await self.preview(prepared)

    async def apply(self, prepared: PreparedViewChange) -> dict[str, Any]:
        self._require_apply_policy(prepared.request.database_name)
        request = prepared.request
        if not request.reviewed_intent:
            raise ViewPolicyError("view apply requires reviewed_intent=True")
        if not request.idempotency_key or not request.idempotency_key.strip():
            raise ViewPolicyError("view apply requires a non-empty idempotency_key")
        if prepared.operation == "noop":
            current = await self.capture_view(
                request.database_name,
                request.schema_name,
                request.view_name,
                marker_name=prepared.marker_name,
            )
            if not _same_snapshot_identity(current, prepared.prior):
                raise ViewWorkflowError(
                    "view changed after preview; prepare a new reviewed change"
                )
            verification = await self.verify(prepared)
            if not verification.verified:
                raise ViewVerificationError(
                    "no-op view verification failed; prepare a new reviewed change"
                )
            return {
                "status": "already_applied",
                "workflow_applied": False,
                "apply_receipt": None,
                "verification": verification.as_dict(),
            }

        current = await self.capture_view(
            request.database_name,
            request.schema_name,
            request.view_name,
            marker_name=prepared.marker_name,
        )
        if current.exists and current.index_count > 0:
            raise ViewWorkflowError(
                "cannot alter a view with existing indexes; exact index rollback is not implemented"
            )
        if (
            current.exists
            and current.definition_fingerprint == prepared.target_fingerprint
            and _dependencies_match(prepared.target_dependencies, current.dependencies)
        ):
            receipt_key = self._receipt_key(prepared)
            receipt = self._applied_receipts.get(receipt_key)
            workflow_applied = (
                receipt is not None
                and receipt.object_id is not None
                and receipt.object_id == current.object_id
            )
            if receipt is not None and not workflow_applied:
                self._applied_receipts.pop(receipt_key, None)
            verification = await self.verify(prepared)
            if not verification.verified:
                return {
                    "status": "hold",
                    "workflow_applied": False,
                    "apply_receipt": None,
                    "verification": verification.as_dict(),
                    "reason": (
                        "an identical target exists, but its Azure SQL MCP marker "
                        "is absent or does not match; ownership was not adopted"
                    ),
                }
            if receipt is None:
                receipt = verification.actual
                self._applied_receipts[receipt_key] = receipt
            return {
                "status": "already_applied",
                "workflow_applied": True,
                "apply_receipt": (
                    view_apply_receipt(receipt) if receipt is not None else None
                ),
                "verification": verification.as_dict(),
            }
        if not _same_snapshot_identity(current, prepared.prior):
            raise ViewWorkflowError(
                "view changed after preview; prepare a new reviewed change"
            )

        action = AdminAction(
            tool_name="view_workflow",
            database_name=request.database_name,
            action_type="query",
            sql=prepared.apply_sql or "",
            rollback_sql=prepared.rollback_sql,
            trusted_generated=True,
            reviewed_intent=True,
            idempotency_key=request.idempotency_key,
            exactly_once=True,
            policy_verified=True,
            non_production=True,
            verification_required=True,
        )
        execution = await self._execute_once(action)
        verification = await self.verify(prepared)
        dispatch_proof = _build_dispatch_proof(
            execution,
            action.sql,
            prepared,
            object_id=verification.actual.object_id,
        )
        confirmed = replace(verification.actual, dispatch_proof=dispatch_proof)
        self._applied_receipts[self._receipt_key(prepared)] = confirmed
        if not verification.verified:
            try:
                rollback = await self.rollback(prepared)
            except Exception as exc:
                raise ViewVerificationError(
                    f"view verification failed and exact rollback failed: {exc}"
                ) from exc
            raise ViewVerificationError(
                "view verification failed; exact rollback completed: "
                f"{rollback['status']}"
            )
        return {
            "status": "completed",
            "workflow_applied": True,
            "apply_receipt": view_apply_receipt(confirmed),
            "mutation_proof": dict(dispatch_proof),
            "verification": verification.as_dict(),
        }

    async def apply_prepared_view_change(
        self,
        prepared: PreparedViewChange,
    ) -> dict[str, Any]:
        return await self.apply(prepared)

    async def verify(self, prepared: PreparedViewChange) -> ViewVerification:
        actual = await self.capture_view(
            prepared.request.database_name,
            prepared.request.schema_name,
            prepared.request.view_name,
            marker_name=prepared.marker_name,
        )
        definition_verified = (
            actual.exists
            and actual.definition is not None
            and view_definition_fingerprint(
                actual.definition,
                schema_bound=actual.schema_bound,
                uses_ansi_nulls=prepared.target_uses_ansi_nulls,
                uses_quoted_identifier=prepared.target_uses_quoted_identifier,
            )
            == prepared.target_fingerprint
        )
        dependencies_verified = _dependencies_match(
            prepared.target_dependencies,
            actual.dependencies,
        )
        module_options_verified = (
            actual.exists
            and actual.uses_ansi_nulls == prepared.target_uses_ansi_nulls
            and actual.uses_quoted_identifier
            == prepared.target_uses_quoted_identifier
        )
        marker_verified = (
            prepared.operation == "noop"
            or (
                actual.marker_name == prepared.marker_name
                and actual.marker_present
                and actual.marker_value == prepared.marker_value
                and actual.reserved_marker_names == (prepared.marker_name,)
            )
        )
        verified = (
            definition_verified
            and dependencies_verified
            and module_options_verified
            and marker_verified
        )
        workflow_commit_proven = (
            prepared.operation != "noop"
            and verified
            and marker_verified
        )
        if verified:
            reason = "definition, dependencies, and module SET options match"
        elif not marker_verified:
            reason = "workflow marker is missing or does not match"
        elif not definition_verified:
            reason = "definition mismatch"
        elif not dependencies_verified:
            reason = "dependency mismatch"
        else:
            reason = "module SET option mismatch"
        return ViewVerification(
            verified=verified,
            definition_verified=definition_verified,
            dependencies_verified=dependencies_verified,
            module_options_verified=module_options_verified,
            marker_verified=marker_verified,
            workflow_commit_proven=workflow_commit_proven,
            actual=actual,
            expected_fingerprint=prepared.target_fingerprint,
            reason=reason,
        )

    async def verify_view_change(self, prepared: PreparedViewChange) -> ViewVerification:
        return await self.verify(prepared)

    async def rollback(self, prepared: PreparedViewChange) -> dict[str, Any]:
        self._require_apply_policy(prepared.request.database_name)
        request = prepared.request
        receipt_key = self._receipt_key(prepared)
        receipt = self._applied_receipts.get(receipt_key)
        if receipt is None:
            raise ViewWorkflowError(
                "rollback fencing failed; this process has no confirmed apply receipt"
            )
        current = await self.capture_view(
            request.database_name,
            request.schema_name,
            request.view_name,
            marker_name=prepared.marker_name,
        )
        if current.exists and current.index_count > 0:
            raise ViewWorkflowError(
                "rollback is fenced because the workflow view has an existing index"
            )
        if (
            not current.exists
            or current.object_id is None
            or receipt.object_id is None
            or current.object_id != receipt.object_id
            or current.definition_fingerprint != receipt.definition_fingerprint
            or current.definition_fingerprint != prepared.target_fingerprint
            or current.marker_name != prepared.marker_name
            or not current.marker_present
            or current.marker_value != prepared.marker_value
        ):
            raise ViewWorkflowError(
                "rollback fencing failed; current view is not the confirmed workflow object"
            )
        rollback_module = (
            _build_snapshot_restore_module(prepared.prior)
            if prepared.prior.exists
            else (
                f"DROP VIEW {_quote_identifier(request.schema_name)}."
                f"{_quote_identifier(request.view_name)};"
            )
        )
        rollback_sql = _build_fenced_view_execution_batch(
            rollback_module,
            database_name=request.database_name,
            schema_name=request.schema_name,
            view_name=request.view_name,
            expected_exists=True,
            expected_object_id=receipt.object_id,
            expected_definition=(
                receipt.definition or _build_target_catalog_definition(prepared)
            ),
            expected_index_count=receipt.index_count,
            expected_schema_bound=receipt.schema_bound,
            expected_uses_ansi_nulls=receipt.uses_ansi_nulls,
            expected_uses_quoted_identifier=receipt.uses_quoted_identifier,
            marker_name=prepared.marker_name,
            expected_marker_present=True,
            expected_marker_value=prepared.marker_value,
            expected_reserved_marker_count=1,
            marker_action=("drop" if prepared.prior.exists else "drop_with_view"),
            marker_value=prepared.marker_value,
            uses_ansi_nulls=prepared.prior.uses_ansi_nulls,
            uses_quoted_identifier=prepared.prior.uses_quoted_identifier,
            indexed_view=request.indexed_view,
            preserve_module_options=request.indexed_view,
        )
        action = AdminAction(
            tool_name="view_workflow_rollback",
            database_name=request.database_name,
            action_type="query",
            sql=rollback_sql,
            rollback_sql=prepared.apply_sql or prepared.rollback_sql,
            trusted_generated=True,
            reviewed_intent=True,
            idempotency_key=f"{request.idempotency_key}:rollback",
            exactly_once=True,
            policy_verified=True,
            non_production=True,
            verification_required=True,
        )
        await self._execute_once(action)
        after = await self.capture_view(
            request.database_name,
            request.schema_name,
            request.view_name,
            marker_name=prepared.marker_name,
        )
        restored = _snapshot_matches_prior(after, prepared.prior)
        if not restored:
            raise ViewVerificationError("exact prior view state was not restored")
        self._applied_receipts.pop(receipt_key, None)
        return {"status": "rolled_back", "snapshot": after.as_dict()}

    async def rollback_view_change(self, prepared: PreparedViewChange) -> dict[str, Any]:
        return await self.rollback(prepared)

    def register_apply_receipt(
        self,
        prepared: PreparedViewChange,
        receipt: ViewSnapshot,
    ) -> None:
        request = prepared.request
        if (
            not receipt.exists
            or receipt.object_id is None
            or receipt.definition_fingerprint != prepared.target_fingerprint
            or receipt.database_name.casefold() != request.database_name.casefold()
            or receipt.schema_name.casefold() != request.schema_name.casefold()
            or receipt.view_name.casefold() != request.view_name.casefold()
            or receipt.marker_name != prepared.marker_name
            or not receipt.marker_present
            or receipt.marker_value != prepared.marker_value
            or receipt.reserved_marker_names != (prepared.marker_name,)
        ):
            raise ViewWorkflowError("view apply receipt does not match the prepared target")
        if receipt.dispatch_proof is not None:
            _validate_dispatch_proof(receipt.dispatch_proof, receipt)
        self._applied_receipts[self._receipt_key(prepared)] = receipt

    async def prior_state_restored(
        self,
        prepared: PreparedViewChange,
    ) -> tuple[bool, ViewSnapshot]:
        current = await self.capture_view(
            prepared.request.database_name,
            prepared.request.schema_name,
            prepared.request.view_name,
            marker_name=prepared.marker_name,
        )
        return _snapshot_matches_prior(current, prepared.prior), current

    async def _capture_dependencies(
        self,
        database_name: str,
        schema_name: str,
        view_name: str,
    ) -> tuple[str, ...]:
        rows = await self.executor.fetch_all(
            database_name,
            VIEW_DEPENDENCIES_SQL,
            params=[schema_name, view_name],
        )
        dependencies = {
            _dependency_name(
                row.get("referenced_database_name"),
                row.get("referenced_schema_name"),
                row.get("referenced_entity_name"),
            )
            for row in rows
            if row.get("referenced_entity_name")
        }
        return tuple(sorted(dependencies))

    async def _capture_index_count(
        self,
        database_name: str,
        schema_name: str,
        view_name: str,
    ) -> int:
        rows = await self.executor.fetch_all(
            database_name,
            VIEW_INDEXES_SQL,
            params=[schema_name, view_name],
        )
        if not rows:
            return 0
        count = _as_int(rows[0].get("index_count"))
        if count is None or count < 0:
            raise ViewWorkflowError("view index metadata is unavailable")
        return count

    async def _execute_once(self, action: AdminAction) -> dict[str, Any]:
        execute_once = getattr(self.admin_policy, "execute_exactly_once", None)
        if execute_once is not None:
            return await execute_once(action, self.executor, dry_run=False)
        return await self.admin_policy.execute(action, self.executor, dry_run=False)

    def _apply_policy_reason(self, database_name: str) -> str | None:
        policy = self._database_policy(database_name)
        if policy is None or not policy.configured:
            return "database is not configured in the local policy"
        if policy.environment.strip().casefold() in _PRODUCTION_ENVIRONMENTS:
            return "view DDL is restricted to non-production environments"
        if not policy.allow_read:
            return "view apply requires policy allow_read=True for verification"
        if not policy.allow_view_apply:
            return "local policy does not allow view apply"
        return None

    def _require_apply_policy(self, database_name: str) -> None:
        reason = self._apply_policy_reason(database_name)
        if reason:
            raise ViewPolicyError(reason)

    def _database_policy(self, database_name: str) -> DatabasePolicy | None:
        if isinstance(self.database_policy, DatabasePolicySet):
            return self.database_policy.policy_for(database_name)
        return self.database_policy

    @staticmethod
    def _receipt_key(prepared: PreparedViewChange) -> str:
        return _view_receipt_identity(
            prepared.request,
            operation=prepared.operation,
            target_fingerprint=prepared.target_fingerprint,
            prior=prepared.prior,
        )


def _validate_identifier(value: str, field_name: str) -> None:
    _canonical_identifier(value, field_name)


def _canonical_identifier(value: str, field_name: str) -> str:
    cleaned = str(value).strip()
    if len(cleaned) >= 2 and cleaned[0] == "[" and cleaned[-1] == "]":
        cleaned = cleaned[1:-1]
    if not _IDENTIFIER.fullmatch(cleaned):
        raise ViewDefinitionError(f"{field_name} must be one SQL identifier")
    return cleaned


def _require_azure_sql_module_options(snapshot: ViewSnapshot) -> None:
    if snapshot.exists and not snapshot.uses_ansi_nulls:
        raise ViewWorkflowError(
            "Azure SQL Database exposes ANSI_NULLS as always ON; an existing "
            "legacy ANSI_NULLS OFF view cannot be applied or exactly rolled back"
        )


def _target_ansi_nulls(request: ViewChangeRequest, prior: ViewSnapshot) -> bool:
    _require_azure_sql_module_options(prior)
    return True


def _target_quoted_identifier(
    request: ViewChangeRequest,
    prior: ViewSnapshot,
) -> bool:
    return True if request.indexed_view or not prior.exists else prior.uses_quoted_identifier


def _build_dispatch_proof(
    execution: dict[str, Any],
    sql: str,
    prepared: PreparedViewChange,
    *,
    object_id: int | None,
) -> dict[str, str]:
    status = str(execution.get("status") or "")
    audit_id = str(execution.get("audit_id") or "").strip()
    if status != "completed" or not audit_id:
        raise ViewWorkflowError(
            "view mutation returned no completed dispatch proof after DDL"
        )
    if object_id is None:
        raise ViewWorkflowError("view mutation returned no confirmed object identity")
    return {
        "version": "1",
        "audit_id": audit_id,
        "sql_hash": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "object_id": str(object_id),
        "target_fingerprint": prepared.target_fingerprint,
    }


def _quote_identifier(value: str) -> str:
    cleaned = _canonical_identifier(value, "identifier")
    return f"[{cleaned}]"


def _extract_view_body(definition: str) -> str:
    parsed = _parse_view_module(definition)
    if parsed is not None:
        return _strip_statement_terminator(parsed.body).strip()
    return _strip_statement_terminator(str(definition)).strip()


def _parse_view_module(definition: str) -> _ParsedViewDefinition | None:
    """Parse a SQL Server view module without inspecting the SELECT body for headers."""

    text = str(definition)
    core_start = _consume_leading_trivia(text)
    if not re.match(r"(?:CREATE|ALTER)\b", text[core_start:], re.IGNORECASE):
        return None

    position = core_start
    position = _consume_keyword(text, position, "CREATE", "ALTER")
    if _matches_keyword(text, position, "OR"):
        position = _consume_keyword(text, position, "OR")
        position = _consume_keyword(text, position, "ALTER")
    position = _consume_keyword(text, position, "VIEW")

    _, position = _consume_sql_identifier(text, position, "view name")
    view_name_end = position
    position = _skip_space(text, position)
    if position < len(text) and text[position] == ".":
        _, position = _consume_sql_identifier(text, position + 1, "view name")
        view_name_end = position
        position = _skip_space(text, position)
    if position < len(text) and text[position] == ".":
        raise ViewWorkflowError("view header has more than one qualifier")

    column_list: tuple[str, ...] = ()
    option_start = view_name_end
    if position < len(text) and text[position] == "(":
        end = _consume_parenthesized(text, position)
        column_list = _parse_column_list(text[position + 1 : end - 1])
        position = _skip_space(text, end)

    attributes: list[str] = []
    if _matches_keyword(text, position, "WITH"):
        position = _consume_keyword(text, position, "WITH")
        while True:
            position = _skip_space(text, position)
            attribute, position = _consume_sql_word(text, position, "view attribute")
            attribute = attribute.upper()
            if attribute not in _VIEW_ATTRIBUTES:
                raise ViewWorkflowError(
                    f"view header attribute {attribute!r} cannot be faithfully restored"
                )
            if attribute in attributes:
                raise ViewWorkflowError(
                    f"view header attribute {attribute!r} is duplicated"
                )
            attributes.append(attribute)
            position = _skip_space(text, position)
            if position >= len(text) or text[position] != ",":
                break
            position += 1
    position = _skip_space(text, position)
    as_position = _consume_keyword(text, position, "AS")
    if not text[as_position:].strip():
        raise ViewWorkflowError("view definition body is empty")

    header = ViewHeader(
        leading_comments=text[:core_start],
        column_list=column_list,
        attributes=tuple(attributes),
        raw_suffix=text[option_start:position],
    )
    return _ParsedViewDefinition(header=header, body=text[as_position:])


def _view_snapshot_from_state(payload: dict[str, Any]) -> ViewSnapshot:
    try:
        exists = _state_bool(payload["exists"], "prior exists")
        database_name = str(payload["database_name"])
        schema_name = str(payload["schema_name"])
        view_name = str(payload["view_name"])
        object_id = _as_int(payload.get("object_id"))
        dependencies = tuple(str(item) for item in payload.get("dependencies", ()))
        schema_bound = _state_bool(
            payload.get("schema_bound", False),
            "prior schema_bound",
        )
        uses_ansi_nulls = _state_bool(
            payload["uses_ansi_nulls"],
            "prior uses_ansi_nulls",
        )
        uses_quoted_identifier = _state_bool(
            payload["uses_quoted_identifier"],
            "prior uses_quoted_identifier",
        )
        marker_name_value = payload.get("marker_name")
        marker_present = _state_bool(
            payload.get("marker_present", False),
            "prior marker_present",
        )
        marker_value = payload.get("marker_value")
        if marker_name_value is not None and not isinstance(marker_name_value, str):
            raise ViewWorkflowError("durable prior view marker name is invalid")
        if marker_value is not None and not isinstance(marker_value, str):
            raise ViewWorkflowError("durable prior view marker value is invalid")
        reserved_marker_names_value = payload.get("reserved_marker_names", ())
        if not isinstance(reserved_marker_names_value, (list, tuple)):
            raise ViewWorkflowError("durable prior view marker inventory is invalid")
        reserved_marker_names = tuple(
            sorted({str(item) for item in reserved_marker_names_value if str(item)})
        )
        _validate_reserved_marker_inventory(reserved_marker_names)
        marker_name = marker_name_value
        definition_value = payload.get("definition")
    except (KeyError, TypeError, ValueError) as exc:
        raise ViewWorkflowError("durable prior view state is invalid") from exc
    if not exists:
        if definition_value is not None or object_id is not None:
            raise ViewWorkflowError("absent durable view state contains object data")
        return ViewSnapshot(
            database_name,
            schema_name,
            view_name,
            False,
            marker_name=marker_name,
            marker_present=marker_present,
            marker_value=marker_value,
            reserved_marker_names=reserved_marker_names,
        )
    if not isinstance(definition_value, str) or not definition_value.strip():
        raise ViewWorkflowError("existing durable view state has no exact definition")
    parsed = _parse_view_module(definition_value)
    if parsed is None:
        raise ViewWorkflowError("durable prior view definition header is unsupported")
    snapshot = ViewSnapshot(
        database_name=database_name,
        schema_name=schema_name,
        view_name=view_name,
        exists=True,
        object_id=object_id,
        definition=definition_value,
        definition_fingerprint=view_definition_fingerprint(
            definition_value,
            schema_bound=schema_bound,
            uses_ansi_nulls=uses_ansi_nulls,
            uses_quoted_identifier=uses_quoted_identifier,
        ),
        dependencies=dependencies,
        schema_bound=schema_bound,
        uses_ansi_nulls=uses_ansi_nulls,
        uses_quoted_identifier=uses_quoted_identifier,
        header=parsed.header,
        index_count=_non_negative_int(payload.get("index_count", 0), "prior index_count"),
        marker_name=marker_name,
        marker_present=marker_present,
        marker_value=marker_value,
        reserved_marker_names=reserved_marker_names,
    )
    _require_azure_sql_module_options(snapshot)
    return snapshot


def _state_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ViewWorkflowError(f"durable view {field_name} must be a boolean")
    return value


def _consume_leading_trivia(text: str) -> int:
    position = 0
    while True:
        position = _skip_space(text, position)
        if text.startswith("--", position):
            newline = text.find("\n", position + 2)
            if newline < 0:
                return len(text)
            position = newline + 1
            continue
        if text.startswith("/*", position):
            end = text.find("*/", position + 2)
            if end < 0:
                raise ViewWorkflowError("unterminated leading view comment")
            position = end + 2
            continue
        return position


def _skip_space(text: str, position: int) -> int:
    while position < len(text) and text[position].isspace():
        position += 1
    return position


def _matches_keyword(text: str, position: int, keyword: str) -> bool:
    position = _skip_space(text, position)
    end = position + len(keyword)
    return (
        text[position:end].casefold() == keyword.casefold()
        and (end == len(text) or not (text[end].isalnum() or text[end] in "_@$#"))
    )


def _consume_keyword(text: str, position: int, *keywords: str) -> int:
    position = _skip_space(text, position)
    for keyword in keywords:
        if _matches_keyword(text, position, keyword):
            return position + len(keyword)
    expected = " or ".join(keywords)
    raise ViewWorkflowError(f"view header expected {expected}")


def _consume_sql_word(text: str, position: int, label: str) -> tuple[str, int]:
    position = _skip_space(text, position)
    match = _SQL_WORD.match(text, position)
    if match is None:
        raise ViewWorkflowError(f"view header expected a {label}")
    return match.group(0), match.end()


def _consume_sql_identifier(
    text: str,
    position: int,
    label: str,
) -> tuple[str, int]:
    position = _skip_space(text, position)
    if position >= len(text):
        raise ViewWorkflowError(f"view header expected a {label}")
    if text[position] == "[":
        end = position + 1
        while end < len(text):
            if text[end] != "]":
                end += 1
            elif end + 1 < len(text) and text[end + 1] == "]":
                end += 2
            else:
                return text[position : end + 1], end + 1
        raise ViewWorkflowError(f"view header has an unterminated {label}")
    if text[position] == '"':
        end = position + 1
        while end < len(text):
            if text[end] != '"':
                end += 1
            elif end + 1 < len(text) and text[end + 1] == '"':
                end += 2
            else:
                return text[position : end + 1], end + 1
        raise ViewWorkflowError(f"view header has an unterminated {label}")
    return _consume_sql_word(text, position, label)


def _consume_parenthesized(text: str, position: int) -> int:
    depth = 0
    cursor = position
    while cursor < len(text):
        character = text[cursor]
        if character == "[":
            _, cursor = _consume_sql_identifier(text, cursor, "column name")
            continue
        if character == '"':
            _, cursor = _consume_sql_identifier(text, cursor, "column name")
            continue
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return cursor + 1
        cursor += 1
    raise ViewWorkflowError("view header has an unterminated column list")


def _parse_column_list(content: str) -> tuple[str, ...]:
    columns = _split_sql_list(content)
    if not columns or any(not column.strip() for column in columns):
        raise ViewWorkflowError("view header has an empty column name")
    parsed: list[str] = []
    for column in columns:
        token = column.strip()
        _, end = _consume_sql_identifier(token, 0, "column name")
        if _skip_space(token, end) != len(token):
            raise ViewWorkflowError(
                "view column list contains syntax that cannot be faithfully restored"
            )
        parsed.append(token)
    return tuple(parsed)


def _split_sql_list(content: str) -> list[str]:
    values: list[str] = []
    start = 0
    cursor = 0
    while cursor < len(content):
        character = content[cursor]
        if character == "[":
            _, cursor = _consume_sql_identifier(content, cursor, "column name")
            continue
        if character == '"':
            _, cursor = _consume_sql_identifier(content, cursor, "column name")
            continue
        if character == ",":
            values.append(content[start:cursor])
            start = cursor + 1
        cursor += 1
    values.append(content[start:])
    return values


def _strip_statement_terminator(text: str) -> str:
    stripped = text.rstrip()
    if stripped.endswith(";"):
        return stripped[:-1]
    return text


def _build_snapshot_restore_statement(
    snapshot: ViewSnapshot,
    *,
    indexed_view: bool = False,
) -> str:
    return build_view_execution_batch(
        _build_snapshot_restore_module(snapshot),
        uses_ansi_nulls=snapshot.uses_ansi_nulls,
        uses_quoted_identifier=snapshot.uses_quoted_identifier,
        indexed_view=indexed_view,
        preserve_module_options=indexed_view,
    )


def _build_snapshot_restore_module(snapshot: ViewSnapshot) -> str:
    if not snapshot.exists or snapshot.definition is None or snapshot.header is None:
        raise ViewWorkflowError("exact prior view header capture is required")
    _require_azure_sql_module_options(snapshot)
    parsed = _parse_view_module(snapshot.definition)
    if parsed is None:
        raise ViewWorkflowError("exact prior view header capture is required")
    return (
        f"{snapshot.header.leading_comments}ALTER VIEW "
        f"{_quote_identifier(snapshot.schema_name)}.{_quote_identifier(snapshot.view_name)}"
        f"{snapshot.header.raw_suffix}AS{_strip_statement_terminator(parsed.body)};"
    )


def _build_target_catalog_definition(prepared: PreparedViewChange) -> str:
    request = prepared.request
    # SQL Server exposes altered module text using the CREATE form in its
    # catalog.  This is also the form used by the fake executor and gives the
    # rollback fence a deterministic exact-definition hash after restart.
    return build_view_statement(
        "create",
        request.schema_name,
        request.view_name,
        request.definition,
        schema_bound=request.schema_bound,
        indexed_view=request.indexed_view,
    )


def _normalized_view_header_identity(
    header: ViewHeader | None,
    *,
    schema_bound: bool,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if header is None:
        column_list: tuple[str, ...] = ()
        attributes: set[str] = set()
    else:
        column_list = tuple(
            re.sub(r"\s+", " ", column.strip())
            for column in header.column_list
        )
        attributes = {attribute.upper() for attribute in header.attributes}
    if schema_bound:
        attributes.add("SCHEMABINDING")
    return column_list, tuple(sorted(attributes))


def _normalize_view_body(definition: str) -> str:
    text = _extract_view_body(definition)
    normalized: list[str] = []
    pending_space = False
    position = 0
    while position < len(text):
        if text.startswith("--", position):
            newline = text.find("\n", position + 2)
            position = len(text) if newline < 0 else newline + 1
            pending_space = True
            continue
        if text.startswith("/*", position):
            end = text.find("*/", position + 2)
            position = len(text) if end < 0 else end + 2
            pending_space = True
            continue
        character = text[position]
        if character.isspace():
            pending_space = True
            position += 1
            continue
        if character in {"'", "[", '"'}:
            if pending_space and normalized:
                normalized.append(" ")
            pending_space = False
            end = _consume_quoted_token(text, position, character)
            normalized.append(text[position:end])
            position = end
            continue
        if pending_space and normalized:
            normalized.append(" ")
        pending_space = False
        normalized.append(character)
        position += 1
    return "".join(normalized).strip()


def _consume_quoted_token(text: str, position: int, delimiter: str) -> int:
    closing = "]" if delimiter == "[" else delimiter
    cursor = position + 1
    while cursor < len(text):
        if text[cursor] != closing:
            cursor += 1
            continue
        if cursor + 1 < len(text) and text[cursor + 1] == closing:
            cursor += 2
            continue
        return cursor + 1
    return len(text)


def _qualified_identifier_parts(node: Any) -> list[str]:
    if isinstance(node, exp.Identifier):
        return [node.name]
    if isinstance(node, exp.Dot):
        return _qualified_identifier_parts(node.this) + _qualified_identifier_parts(
            node.expression
        )
    return []


def _dependency_name(database: Any, schema: Any, entity: Any) -> str:
    database_text = str(database or "").strip()
    schema_text = str(schema or "").strip()
    entity_text = str(entity or "").strip()
    return ".".join(
        part
        for part in (database_text, schema_text, entity_text)
        if part
    ).casefold()


def _dependencies_match(expected: tuple[str, ...], actual: tuple[str, ...]) -> bool:
    remaining = {
        str(item).strip().strip("[]").casefold()
        for item in actual
    }
    for expected_item in expected:
        expected_name = str(expected_item).strip().strip("[]").casefold()
        if "." in expected_name:
            if expected_name not in remaining:
                return False
            remaining.remove(expected_name)
            continue
        matches = {item for item in remaining if _dependency_leaf(item) == expected_name}
        if len(matches) != 1:
            return False
        remaining.remove(matches.pop())
    return not remaining


def _dependency_leaf(value: str) -> str:
    return str(value).strip().strip("[]").split(".")[-1].casefold()


def _same_snapshot_identity(current: ViewSnapshot, prior: ViewSnapshot) -> bool:
    if current.exists != prior.exists:
        return False
    if not prior.exists:
        return True
    return (
        current.object_id is not None
        and current.object_id == prior.object_id
        and current.definition_fingerprint == prior.definition_fingerprint
        and _normalized_view_header_identity(
            current.header,
            schema_bound=current.schema_bound,
        )
        == _normalized_view_header_identity(
            prior.header,
            schema_bound=prior.schema_bound,
        )
        and _dependencies_match(prior.dependencies, current.dependencies)
        and current.uses_ansi_nulls == prior.uses_ansi_nulls
        and current.uses_quoted_identifier == prior.uses_quoted_identifier
        and current.index_count == prior.index_count
        and _marker_snapshot_matches(current, prior)
    )


def _snapshot_matches_prior(current: ViewSnapshot, prior: ViewSnapshot) -> bool:
    if not prior.exists:
        return not current.exists
    return (
        current.exists
        and current.object_id is not None
        and current.object_id == prior.object_id
        and current.definition_fingerprint == prior.definition_fingerprint
        and _normalized_view_header_identity(
            current.header,
            schema_bound=current.schema_bound,
        )
        == _normalized_view_header_identity(
            prior.header,
            schema_bound=prior.schema_bound,
        )
        and _dependencies_match(prior.dependencies, current.dependencies)
        and current.uses_ansi_nulls == prior.uses_ansi_nulls
        and current.uses_quoted_identifier == prior.uses_quoted_identifier
        and current.index_count == prior.index_count
        and _marker_snapshot_matches(current, prior)
    )


def _marker_snapshot_matches(
    current: ViewSnapshot,
    expected: ViewSnapshot,
) -> bool:
    if expected.marker_name is None:
        return True
    return (
        current.marker_name == expected.marker_name
        and current.marker_present == expected.marker_present
        and current.marker_value == expected.marker_value
        and current.reserved_marker_names == expected.reserved_marker_names
    )


def _as_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _non_negative_int(value: Any, field_name: str) -> int:
    parsed = _as_int(value)
    if parsed is None or parsed < 0:
        raise ViewWorkflowError(f"durable view {field_name} must be a non-negative integer")
    return parsed


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"1", "true", "yes", "y"}:
            return True
        if normalized in {"0", "false", "no", "n"}:
            return False
    return None


__all__ = [
    "PreparedViewChange",
    "VIEW_DEFINITION_SQL",
    "VIEW_DEPENDENCIES_SQL",
    "VIEW_INDEXES_SQL",
    "ViewChangeRequest",
    "ViewDefinitionError",
    "ViewHeader",
    "ViewLegalityReport",
    "ViewPolicyError",
    "ViewSnapshot",
    "ViewVerification",
    "ViewWorkflowError",
    "ViewVerificationError",
    "ViewWorkflowService",
    "build_view_execution_batch",
    "build_view_statement",
    "extract_view_dependencies",
    "prepared_view_change_from_state",
    "prepared_view_change_state",
    "require_valid_view_definition",
    "validate_view_definition",
    "view_apply_receipt",
    "view_definition_fingerprint",
    "view_snapshot_from_receipt",
]
