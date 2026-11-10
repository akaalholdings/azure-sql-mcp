"""Leased indexed-view experiments: validation, names, DDL, catalog proof and cleanup.

An indexed view (a schema-bound view with a unique clustered index) can answer
a warehouse aggregate from a few thousand stored rows. This module is the
engine for testing one as a leased, cleaned-up object, the way a test index is
tested: it plans the experiment from a caller's view definition, builds the
SQL batches, and judges what the catalog shows. It runs no write itself. The
tool layer sends the batches through the admin lane and keeps the lease.

Order of use (see ``ViewExperimentPlan``):

1. ``require_view_experiment_policy``: the database policy allows it.
2. ``plan_view_experiment``: static findings, names, DDL. Refuse when
   ``plan.valid`` is false; no statement has run.
3. ``read_view_catalog_state`` then ``name_in_use_blocker``: the name is free.
4. ``build_view_experiment_create_batch``: view, owner markers and indexes in
   one transaction.
5. ``read_view_catalog_state`` then ``verify_view_experiment``: the built
   objects are the requested ones; ``view_index_size`` reports rows and size.
6. ``build_test_view_drop_batch``: guarded by the owner markers.
7. ``reconcile_view_experiment`` after a build or a drop that failed or was
   interrupted.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from dataclasses import replace
from typing import Any
from typing import Mapping
from typing import Sequence

import sqlglot
from sqlglot import exp

from .connection import AzureSqlExecutor
from .database_policy import WORKFLOW_TEST_INDEX_PREFIX
from .database_policy import DatabasePolicy
from .database_policy import extends_test_index_prefix
from .index_metadata import MAX_GENERATED_TEST_INDEX_NAME_LENGTH
from .index_metadata import ExistingIndex
from .index_metadata import TableColumnFacts
from .index_metadata import collect_existing_indexes
from .index_metadata import generated_test_object_name
from .index_metadata import parse_candidate_key
from .index_optimizer import INDEX_KIND_ROWSTORE
from .index_optimizer import INDEX_KIND_VIEW_CLUSTERED
from .index_optimizer import TEST_INDEX_DROP_LOCK_TIMEOUT_MS
from .index_optimizer import IndexCandidate
from .index_optimizer import build_index_candidate_statement
from .index_optimizer import classify_create_index_failure
from .index_optimizer import expected_index_definition_matches
from .safe_sql import SafeSqlValidator
from .safe_sql import strip_literals_and_comments
from .view_workflows import ViewWorkflowError
from .view_workflows import _parse_view_module
from .view_workflows import validate_view_definition

# The markers a leased test index carries (server.TEST_INDEX_OWNER_PROPERTY and
# TEST_INDEX_DEFINITION_PROPERTY), written here on the view itself.
VIEW_OWNER_PROPERTY = "AzureSqlMcpLeaseOwner"
VIEW_DEFINITION_PROPERTY = "AzureSqlMcpDefinitionFingerprint"
LEASE_OBJECT_KIND = "indexed_view"
MAX_VIEW_EXTRA_INDEXES = 3
# The session options the engine requires to create an indexed view, to build
# its index, to write to its base tables and to read it through its index.
INDEXED_VIEW_SET_OPTIONS = (
    ("ANSI_NULLS", "ON"),
    ("QUOTED_IDENTIFIER", "ON"),
    ("ANSI_PADDING", "ON"),
    ("ANSI_WARNINGS", "ON"),
    ("ARITHABORT", "ON"),
    ("CONCAT_NULL_YIELDS_NULL", "ON"),
    ("NUMERIC_ROUNDABORT", "OFF"),
)
READ_MODE_NOEXPAND = "noexpand"
READ_MODE_AUTOMATIC = "automatic"
# The build batch raises this number with the engine's own number in the text.
VIEW_BUILD_ERROR_NUMBER = 50020
_VIEW_BUILD_ROLLED_BACK = "The test view build was rolled back."

VIEW_EXPERIMENT_WRITE_OVERHEAD_WARNING = (
    "An indexed view is maintained inside every INSERT, UPDATE, DELETE and MERGE on its "
    "base tables, in the same transaction, for as long as it exists. This experiment "
    "builds the view and its index for the test window only and drops both before it "
    "returns; if the call is interrupted, the lease drops them later. While the view "
    "exists, a session that writes to a base table must run with ANSI_NULLS, "
    "QUOTED_IDENTIFIER, ANSI_PADDING, ANSI_WARNINGS, ARITHABORT and "
    "CONCAT_NULL_YIELDS_NULL ON and NUMERIC_ROUNDABORT OFF, or its statement fails with "
    "error 1934. The index build is offline: it is expected to hold a shared lock on each "
    "base table until it commits, so writes to the base tables wait while it runs and "
    "reads do not."
)
VIEW_EXPERIMENT_WRITE_COST_NOTE = (
    "The write cost of this view was not measured: this tool runs no data change. Before "
    "a production change, measure INSERT, UPDATE and DELETE on each base table with and "
    "without the view on a copy of the data."
)
# Observed on Azure SQL Database with an indexed view in place. A
# recommendation of an indexed view on a loaded table must say this.
VIEW_BASE_TABLE_RESTRICTIONS = (
    "While an indexed view exists, these statements on its base tables fail: TRUNCATE "
    "TABLE (error 3729), ALTER TABLE ... ALTER COLUMN on a column the view reads (error "
    "4922), and ALTER TABLE ... SWITCH on a partitioned base table when the view is not "
    "partition-aligned (error 11401). An indexed view on a fact table therefore blocks "
    "TRUNCATE TABLE and partition SWITCH loads on that table, and it is maintained on "
    "every write to it."
)
VIEW_EXPERIMENT_CAVEATS = (
    "WITH SCHEMABINDING blocks a change to any base table column the view reads until "
    "the view is dropped.",
    "WITH (NOEXPAND) makes the optimizer read the view index and use its statistics. "
    "Without the hint the optimizer may match the view on its own or may not: check the "
    "plan before you rely on automatic matching.",
)

# The seven options as the session reports them. Each value is 1, 0 or NULL.
SESSION_SET_OPTIONS_SQL = (
    "SELECT\n"
    + ",\n".join(
        f"    CONVERT(int, SESSIONPROPERTY('{name}')) AS {name.lower()}"
        for name, _value in INDEXED_VIEW_SET_OPTIONS
    )
    + ";"
)

# One row for the object of that name, a view or not; no row when the name is
# free. Parameters: owner property name, definition property name, schema, name.
TEST_VIEW_CATALOG_SQL = """
SELECT
    o.object_id,
    o.type AS object_type,
    s.name AS schema_name,
    o.name AS object_name,
    m.definition,
    m.is_schema_bound,
    m.uses_ansi_nulls,
    m.uses_quoted_identifier,
    CONVERT(nvarchar(4000), owner_marker.value) AS owner_marker,
    CONVERT(nvarchar(4000), definition_marker.value) AS definition_marker
FROM sys.objects AS o
INNER JOIN sys.schemas AS s
    ON s.schema_id = o.schema_id
LEFT JOIN sys.sql_modules AS m
    ON m.object_id = o.object_id
LEFT JOIN sys.extended_properties AS owner_marker
    ON owner_marker.class = 1
    AND owner_marker.major_id = o.object_id
    AND owner_marker.minor_id = 0
    AND owner_marker.name = ?
LEFT JOIN sys.extended_properties AS definition_marker
    ON definition_marker.class = 1
    AND definition_marker.major_id = o.object_id
    AND definition_marker.minor_id = 0
    AND definition_marker.name = ?
WHERE s.name = ?
  AND o.name = ?
"""

_PLAIN_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_OWNER_TOKEN = re.compile(r"^[A-Za-z0-9_.:-]{16,200}$")

# Messages of view_workflows.validate_view_definition, by a stable part of the
# text: (text, finding code, severity). A message that matches none is an
# error with the code view_rule, so a new rule there still refuses.
_RULE_CODES: tuple[tuple[str, str, str], ...] = (
    ("must not be empty", "empty_definition", "error"),
    ("batch separator GO", "batch_separator", "error"),
    ("not parseable T-SQL", "not_parseable", "error"),
    ("exactly one SELECT statement", "not_a_single_select", "error"),
    ("must be a SELECT-shaped statement", "not_a_single_select", "error"),
    ("cross-database object names", "cross_database_name", "error"),
    ("database qualifier", "cross_database_name", "error"),
    ("SELECT INTO", "select_into", "error"),
    ("OPTION hints", "option_clause", "error"),
    ("ORDER BY requires", "order_by", "error"),
    ("temporary tables", "temporary_table", "error"),
    ("could not be analyzed", "not_analyzed", "error"),
    ("CTEs are not supported", "common_table_expression", "error"),
    ("subqueries are not supported", "subquery", "error"),
    ("SELECT * is not supported", "select_star", "error"),
    ("table-valued functions", "table_valued_function", "error"),
    ("FOR SYSTEM_TIME", "temporal_reference", "error"),
    ("two-part", "two_part_names", "error"),
    ("self-joins", "self_join", "error"),
    ("set operators", "set_operator", "error"),
    ("outer joins", "outer_join", "error"),
    ("APPLY is not supported", "apply", "error"),
    ("DISTINCT is not supported", "distinct", "error"),
    ("HAVING is not supported", "having", "error"),
    ("TOP and OFFSET", "top_or_offset", "error"),
    ("table hints", "table_hint", "error"),
    ("window functions", "window_function", "error"),
    ("unsupported table operators", "table_operator", "error"),
    ("full-text predicates", "full_text_predicate", "error"),
    ("FOR clauses", "for_clause", "error"),
    # A float column may be in an indexed view, though not in its key: the
    # engine decides.
    ("imprecise FLOAT/REAL", "imprecise_expression", "warning"),
    ("nondeterministic", "nondeterministic_function", "error"),
    ("session and server variables", "session_variable", "error"),
    ("MIN/MAX/AVG", "unsupported_aggregate", "error"),
    ("GROUP BY requires COUNT_BIG(*)", "count_big_required", "error"),
    ("use COUNT_BIG", "count_instead_of_count_big", "error"),
    ("COUNT_BIG of an expression", "count_big_star", "error"),
    # The parser does not know the function, so its determinism is not known
    # here. The engine checks it when the index is built.
    ("determinism cannot be proven", "function_determinism_unproven", "warning"),
)
# Replaced by the catalog-aware check in _sum_findings.
_SUM_RULE_TEXT = "SUM in an indexed view must use ISNULL/COALESCE"


@dataclass(frozen=True)
class ViewFinding:
    """One result of a check on a test view. ``severity`` error refuses the experiment;
    a warning names what only the engine can decide."""

    code: str
    message: str
    severity: str = "error"
    check: str = "static-check"

    def as_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "severity": self.severity,
            "check": self.check,
            "message": self.message,
        }


@dataclass(frozen=True)
class ViewDefinitionCheck:
    """A caller's view definition, read for use as a leased test view."""

    body: str
    column_list: tuple[str, ...] = ()
    # Schema and name of a CREATE VIEW header, when the caller sent one.
    requested_name: tuple[str, ...] = ()
    output_columns: tuple[str, ...] = ()
    # "schema.table" as written, in FROM order.
    base_tables: tuple[str, ...] = ()
    grouped: bool = False
    # Output columns that are GROUP BY expressions, in select order.
    group_by_columns: tuple[str, ...] = ()
    # GROUP BY expressions that no output column returns.
    unselected_group_by: tuple[str, ...] = ()
    normalized_body: str = ""
    findings: tuple[ViewFinding, ...] = ()

    @property
    def valid(self) -> bool:
        return not any(finding.severity == "error" for finding in self.findings)


@dataclass(frozen=True)
class ViewIndexSpec:
    """One more nonclustered index on the test view."""

    key_columns: tuple[str, ...]
    include_columns: tuple[str, ...] = ()
    is_unique: bool = False


@dataclass(frozen=True)
class ViewExperimentPlan:
    """Everything the tool layer needs to run one indexed-view experiment."""

    schema: str
    view_name: str
    definition: ViewDefinitionCheck
    create_view_statement: str
    # None when the key could not be planned (see ``findings``).
    clustered_index: IndexCandidate | None
    extra_indexes: tuple[IndexCandidate, ...]
    test_index_name_prefixes: tuple[str, ...]
    maxdop: int | None = None
    findings: tuple[ViewFinding, ...] = ()
    notes: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return self.clustered_index is not None and not any(
            finding.severity == "error" for finding in self.findings
        )

    @property
    def qualified_name(self) -> str:
        return f"[{self.schema}].[{self.view_name}]"

    @property
    def indexes(self) -> tuple[IndexCandidate, ...]:
        clustered = (self.clustered_index,) if self.clustered_index is not None else ()
        return (*clustered, *self.extra_indexes)

    @property
    def definition_fingerprint(self) -> str:
        """One value for the view and its indexes: the lease's object fingerprint
        and the definition marker on the view."""

        payload = {
            "object_kind": LEASE_OBJECT_KIND,
            "schema": self.schema,
            "view": self.view_name,
            "create_view_sha256": _sha256(self.create_view_statement),
            "indexes": [index.definition_fingerprint for index in self.indexes],
        }
        return _sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")))

    def index_statements(self) -> list[str]:
        """CREATE INDEX statements: the unique clustered index first."""

        return [
            build_index_candidate_statement(index, maxdop=self.maxdop)
            for index in self.indexes
        ]

    def setup_statements(self) -> list[str]:
        """The exact statements of the design, for the answer: the SET options
        (one batch), CREATE VIEW (its own batch), then each CREATE INDEX."""

        return [
            "\n".join(_set_option_statements()),
            self.create_view_statement,
            *self.index_statements(),
        ]

    def teardown_statements(self) -> list[str]:
        """DROP VIEW drops the view's indexes with it."""

        return [f"DROP VIEW {self.qualified_name};"]

    def lease_metadata(self) -> dict[str, Any]:
        """Lease metadata of the experiment, before the build is sent.

        The tool layer adds its private ``lease_owner_fence``. There is no
        ``target_table`` key on purpose: test index cleanup then cannot take
        this lease for an index lease, and leaves it for the view cleanup.
        """

        clustered = self.clustered_index
        return {
            "object_kind": LEASE_OBJECT_KIND,
            "target_schema": self.schema,
            "target_view": self.view_name,
            "base_tables": list(self.definition.base_tables),
            "create_view_sha256": _sha256(self.create_view_statement),
            "clustered_index_name": clustered.index_name if clustered else None,
            "clustered_index_key": list(clustered.key_columns) if clustered else [],
            "extra_indexes": [
                {
                    "index_name": index.index_name,
                    "key_columns": list(index.key_columns),
                    "include_columns": list(index.include_columns),
                    "is_unique": index.is_unique,
                }
                for index in self.extra_indexes
            ],
            "marker_definition_fingerprint": self.definition_fingerprint,
            "fingerprint_provenance": "candidate_intent",
            "create_dispatch_state": "pre_dispatch",
        }

    def as_dict(self) -> dict[str, Any]:
        clustered = self.clustered_index
        return {
            "view": f"{self.schema}.{self.view_name}",
            "base_tables": list(self.definition.base_tables),
            "clustered_index_name": clustered.index_name if clustered else None,
            "clustered_index_key": list(clustered.key_columns) if clustered else [],
            "extra_index_names": [index.index_name for index in self.extra_indexes],
            "definition_fingerprint": self.definition_fingerprint,
            "valid": self.valid,
            "findings": [finding.as_dict() for finding in self.findings],
            "notes": list(self.notes),
            "setup_statements": self.setup_statements() if self.valid else [],
            "teardown_statements": self.teardown_statements(),
            "write_overhead_warning": VIEW_EXPERIMENT_WRITE_OVERHEAD_WARNING,
            "base_table_restrictions": VIEW_BASE_TABLE_RESTRICTIONS,
            "write_cost": VIEW_EXPERIMENT_WRITE_COST_NOTE,
            "caveats": list(VIEW_EXPERIMENT_CAVEATS),
        }


@dataclass(frozen=True)
class ViewCatalogState:
    """What the catalog holds under a test view name."""

    object_id: int | None = None
    # sys.objects.type without padding: V for a view. None when the name is free.
    object_type: str | None = None
    definition: str | None = None
    is_schema_bound: bool | None = None
    uses_ansi_nulls: bool | None = None
    uses_quoted_identifier: bool | None = None
    owner_marker: str | None = None
    definition_marker: str | None = None
    indexes: tuple[ExistingIndex, ...] = ()

    @property
    def exists(self) -> bool:
        return self.object_id is not None

    def owned_by(self, owner_token: str, definition_fingerprint: str) -> bool:
        """Whether both markers of the build transaction are on a view of this name."""

        return (
            self.object_type == "V"
            and bool(owner_token)
            and self.owner_marker == owner_token
            and self.definition_marker == definition_fingerprint
        )


@dataclass(frozen=True)
class ViewExperimentVerification:
    matches: bool
    mismatches: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {"matches": self.matches, "mismatches": list(self.mismatches)}


@dataclass(frozen=True)
class ViewLeaseReconcile:
    """The lease status after a failed or interrupted step, and whether to drop."""

    lease_status: str
    drop_view: bool
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "lease_status": self.lease_status,
            "drop_view": self.drop_view,
            "reason": self.reason,
        }


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


def view_experiment_policy_blockers(policy: DatabasePolicy) -> tuple[str, ...]:
    """Why the database policy does not allow an indexed-view experiment.

    The experiment needs three existing permissions, each for what it does:
    it builds and drops temporary indexes under a lease (``allow_test_indexes``
    on a non-production entry), it runs CREATE VIEW and DROP VIEW
    (``allow_view_apply``), and the first index of a view cannot be built
    online, so the build blocks writers of the base tables
    (``allow_offline_test_index``).
    """

    blockers: list[str] = []
    if not policy.configured:
        return ("The database has no entry in the database policy.",)
    if not policy.is_non_production:
        blockers.append(
            "The policy environment is classed "
            f"{policy.environment_class}; an indexed-view experiment needs a known "
            "non-production label."
        )
    if not policy.allow_test_indexes:
        blockers.append(
            "allow_test_indexes is false: the experiment builds a temporary index under a lease."
        )
    if not policy.allow_view_apply:
        blockers.append(
            "allow_view_apply is false: the experiment runs CREATE VIEW and DROP VIEW."
        )
    if not policy.allow_offline_test_index:
        blockers.append(
            "allow_offline_test_index is false: the first index of a view cannot be built "
            "online, and the build is expected to hold a shared lock on each base table, "
            "so writes to the base tables wait while it is built."
        )
    return tuple(blockers)


INDEXED_VIEW_DESIGNS_FORBIDDEN = (
    "allow_indexed_view_designs is false in the database policy: indexed views are "
    "not a design option for this database, so none is tested or proposed. Use a "
    "rewrite, a rowstore index or a nonclustered columnstore index."
)


def require_indexed_view_designs_allowed(policy: DatabasePolicy) -> None:
    """Refuse a new indexed-view experiment where the policy forbids the design.

    Kept apart from ``require_view_experiment_policy``: the drop of a leased
    view reads that one, and a view built before the field was set to false
    must still be dropped.
    """

    if policy.configured and not policy.allow_indexed_view_designs:
        raise PermissionError(
            f"{INDEXED_VIEW_DESIGNS_FORBIDDEN} Database: {policy.database_name}. "
            "No query and no DDL was run."
        )


def require_view_experiment_policy(policy: DatabasePolicy) -> None:
    blockers = view_experiment_policy_blockers(policy)
    if blockers:
        raise PermissionError(
            "The database policy does not allow an indexed-view experiment on "
            f"{policy.database_name}. " + " ".join(blockers) + " No query and no DDL was run."
        )


# ---------------------------------------------------------------------------
# Names
# ---------------------------------------------------------------------------


def view_name_prefixes(test_index_name_prefixes: Sequence[str]) -> tuple[str, ...]:
    """The view name prefix of each allowed test index prefix: ``v_`` in place of ``IX_``.

    ``IX_Testing_`` gives ``v_Testing_``; a prefix that does not start with
    ``IX_`` gets ``v_`` in front. No policy field of its own: a database that
    allows test indexes under a prefix allows test views under its twin.
    """

    prefixes: list[str] = []
    for prefix in test_index_name_prefixes:
        stem = prefix[3:] if prefix[:3].casefold() == "ix_" else prefix
        prefixes.append(f"v_{stem}")
    return tuple(dict.fromkeys(prefixes))


def is_test_view_name(name: str | None, test_index_name_prefixes: Sequence[str]) -> bool:
    """A plain identifier under an allowed test view prefix, within the name length."""

    if not name or not _PLAIN_IDENTIFIER.fullmatch(name):
        return False
    if len(name) > MAX_GENERATED_TEST_INDEX_NAME_LENGTH:
        return False
    folded = name.casefold()
    return any(
        folded.startswith(prefix.casefold()) and len(name) > len(prefix)
        for prefix in view_name_prefixes(test_index_name_prefixes)
    )


def _require_test_view_target(
    schema: str, view_name: str, test_index_name_prefixes: Sequence[str]
) -> None:
    if not _PLAIN_IDENTIFIER.fullmatch(schema or ""):
        raise ValueError(f"schema_name must be a plain identifier; got {schema!r}.")
    if not is_test_view_name(view_name, test_index_name_prefixes):
        prefixes = " or ".join(view_name_prefixes(test_index_name_prefixes))
        raise ValueError(
            f"view name must be a plain identifier that starts with {prefixes}. This engine "
            "creates and drops only test views under these prefixes, so it can never "
            "touch an application view."
        )


def _usable_name_text(text: str | None) -> str:
    usable = re.match(r"[A-Za-z0-9_]*", str(text or "").strip())
    return usable.group().strip("_") if usable is not None else ""


# ---------------------------------------------------------------------------
# Definition checks
# ---------------------------------------------------------------------------


def validate_test_view_definition(
    definition: str,
    *,
    table_columns: Mapping[str, Sequence[TableColumnFacts]] | None = None,
) -> ViewDefinitionCheck:
    """Check a view definition for use as an indexed test view, without the database.

    ``definition`` is the SELECT body, or a whole ``CREATE VIEW ... WITH
    SCHEMABINDING AS SELECT ...`` statement (its name is only a request).
    ``table_columns`` maps a base table as ``schema.table`` to its columns
    (collect_table_column_facts); with it, a SUM over a NOT NULL column is
    accepted as written and a SUM over a nullable column is refused. Every
    finding is a static check. What cannot be checked here is a warning, and
    the engine decides when the index is built.
    """

    text = str(definition or "")
    findings: list[ViewFinding] = []
    body = text
    column_list: tuple[str, ...] = ()
    requested_name: tuple[str, ...] = ()
    try:
        module = _parse_view_module(text)
    except ViewWorkflowError as exc:
        findings.append(ViewFinding("view_header", f"The CREATE VIEW header is not valid: {exc}."))
        module = None
    if module is not None:
        body = module.body
        column_list = tuple(_bare_identifier(column) for column in module.header.column_list)
        requested_name = tuple(_bare_identifier(part) for part in module.name_parts)
        keyword = text[len(module.header.leading_comments) : module.keyword_end]
        if re.fullmatch(r"\s*CREATE\s*", keyword, re.IGNORECASE) is None:
            findings.append(
                ViewFinding(
                    "create_view_only",
                    "The experiment creates a new view and never alters one: write CREATE "
                    "VIEW, or pass the SELECT body only.",
                )
            )
        if not module.header.schema_bound:
            findings.append(
                ViewFinding(
                    "schemabinding_required",
                    "An indexed view needs WITH SCHEMABINDING in its CREATE VIEW header.",
                )
            )
        other = [name for name in module.header.attributes if name != "SCHEMABINDING"]
        if other:
            findings.append(
                ViewFinding(
                    "view_attribute",
                    f"The view attribute {', '.join(other)} is not used for a test view: "
                    "keep WITH SCHEMABINDING only.",
                )
            )
    body = body.strip().rstrip(";").strip()

    report = validate_view_definition(body, indexed_view=True, schema_bound=True)
    for message in report.errors:
        if message.startswith(_SUM_RULE_TEXT):
            continue
        code, severity = next(
            ((code, severity) for part, code, severity in _RULE_CODES if part in message),
            ("view_rule", "error"),
        )
        findings.append(ViewFinding(code, _sentence(message), severity))

    statement = _parse_select(body)
    if statement is None:
        return ViewDefinitionCheck(
            body=body,
            column_list=column_list,
            requested_name=requested_name,
            normalized_body=report.normalized_definition,
            findings=_distinct(findings),
        )

    try:
        SafeSqlValidator().validate_read_only(body)
    except ValueError as exc:
        findings.append(
            ViewFinding("not_read_only", f"The view body is not one read-only SELECT: {exc}")
        )
    code = strip_literals_and_comments(body)
    if "?" in body:
        findings.append(
            ViewFinding(
                "question_mark",
                "The view body holds a question mark. The build batch uses ? as its "
                "parameter marker, so the body cannot hold one, not even in a literal.",
            )
        )
    if re.search(r"(?<![@\w])@[A-Za-z_#]", code):
        findings.append(
            ViewFinding(
                "variable",
                "The view body holds a variable or parameter. A view takes none: filter "
                "by the column in the statement that reads the view.",
            )
        )
    if re.search(r"\b(?:ROLLUP|CUBE|GROUPING\s+SETS)\b", code, re.IGNORECASE):
        findings.append(
            ViewFinding(
                "grouping_extension",
                "ROLLUP, CUBE and GROUPING SETS are not allowed in an indexed view.",
            )
        )

    selects = list(statement.selects)
    names = [select.alias_or_name for select in selects]
    if column_list:
        if len(column_list) != len(selects):
            findings.append(
                ViewFinding(
                    "column_list_length",
                    f"The view column list names {len(column_list)} columns and the SELECT "
                    f"returns {len(selects)}.",
                )
            )
        names = list(column_list[: len(selects)]) + names[len(column_list) :]
    for position, name in enumerate(names, start=1):
        if not name:
            findings.append(
                ViewFinding(
                    "unnamed_column",
                    f"Select item {position} has no column name: give the expression an alias.",
                )
            )
    folded_names = [name.casefold() for name in names if name]
    repeated = sorted({name for name in folded_names if folded_names.count(name) > 1})
    if repeated:
        findings.append(
            ViewFinding(
                "duplicate_column", f"The view returns the column {repeated[0]} more than once."
            )
        )

    group = statement.args.get("group")
    group_sql = [_expression_key(node) for node in (group.expressions if group else ())]
    grouped = group is not None
    group_by_columns: list[str] = []
    selected_group_sql: set[str] = set()
    for name, select in zip(names, selects, strict=False):
        key = _expression_key(select.unalias())
        if key in group_sql:
            selected_group_sql.add(key)
            if name:
                group_by_columns.append(name)
    unselected = tuple(
        node.sql(dialect="tsql")
        for node in (group.expressions if group else ())
        if _expression_key(node) not in selected_group_sql
    )
    findings.extend(_sum_findings(statement, table_columns))
    base_tables = tuple(
        dict.fromkeys(
            f"{table.db}.{table.name}"
            for table in statement.find_all(exp.Table)
            if table.db and table.name and not isinstance(table.this, exp.Anonymous)
        )
    )
    return ViewDefinitionCheck(
        body=body,
        column_list=column_list,
        requested_name=requested_name,
        output_columns=tuple(names),
        base_tables=base_tables,
        grouped=grouped,
        group_by_columns=tuple(group_by_columns),
        unselected_group_by=unselected,
        normalized_body=report.normalized_definition,
        findings=_distinct(findings),
    )


def _parse_select(body: str) -> Any:
    try:
        statements = sqlglot.parse(body, read="tsql")
    except Exception:  # noqa: BLE001 - the parser raises several types; reported as a finding
        return None
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        return None
    return statements[0]


def _expression_key(node: Any) -> str:
    return node.sql(dialect="tsql", normalize=True).casefold()


def _bare_identifier(value: str) -> str:
    text = str(value).strip()
    if len(text) >= 2 and text[0] == "[" and text[-1] == "]":
        return text[1:-1].replace("]]", "]")
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        return text[1:-1].replace('""', '"')
    return text


def _sentence(message: str) -> str:
    text = message.strip()
    text = text[:1].upper() + text[1:]
    return text if text.endswith(".") else f"{text}."


def _distinct(findings: Sequence[ViewFinding]) -> tuple[ViewFinding, ...]:
    return tuple(dict.fromkeys(findings))


def _sum_findings(
    statement: Any,
    table_columns: Mapping[str, Sequence[TableColumnFacts]] | None,
) -> list[ViewFinding]:
    """The engine refuses an indexed view whose SUM reads a nullable expression (error 8662)."""

    sources: dict[str, dict[str, TableColumnFacts]] = {}
    catalog = {key.casefold(): columns for key, columns in (table_columns or {}).items()}
    for table in statement.find_all(exp.Table):
        columns = catalog.get(f"{table.db}.{table.name}".casefold())
        if columns is not None:
            sources[(table.alias or table.name).casefold()] = {
                column.name.casefold(): column for column in columns
            }
    findings: list[ViewFinding] = []
    for total in statement.find_all(exp.Sum):
        argument = total.this
        state, culprit = _nullability(argument, sources)
        text = argument.sql(dialect="tsql")
        if state == "nullable":
            findings.append(
                ViewFinding(
                    "sum_over_nullable_expression",
                    f"SUM({text}) reads a nullable expression ({culprit}). The engine "
                    "refuses the index of such a view (error 8662). Write "
                    f"SUM({_isnull_form(argument)}).",
                )
            )
        elif state == "unknown":
            findings.append(
                ViewFinding(
                    "sum_nullability_unknown",
                    f"SUM({text}) could not be proven NOT NULL here ({culprit}). The engine "
                    "refuses the index of a view whose SUM reads a nullable expression "
                    f"(error 8662). If an input can be NULL, write SUM(ISNULL({text}, 0)).",
                    "warning",
                )
            )
    return findings


def _isnull_form(argument: Any) -> str:
    """The ISNULL spelling the engine types as NOT NULL: ISNULL(x, 0), or the
    arguments of a two-argument COALESCE moved into ISNULL."""

    if (
        isinstance(argument, exp.Coalesce)
        and not argument.args.get("is_null")
        and len(argument.expressions) == 1
    ):
        first = argument.this.sql(dialect="tsql")
        return f"ISNULL({first}, {argument.expressions[0].sql(dialect='tsql')})"
    return f"ISNULL({argument.sql(dialect='tsql')}, 0)"


def _nullability(
    node: Any, sources: Mapping[str, Mapping[str, TableColumnFacts]]
) -> tuple[str, str]:
    """("not_null" | "nullable" | "unknown", what decided it)."""

    if isinstance(node, exp.Paren | exp.Neg):
        return _nullability(node.this, sources)
    if isinstance(node, exp.Null):
        return "nullable", "NULL literal"
    if isinstance(node, exp.Literal):
        return "not_null", "literal"
    if isinstance(node, exp.Column):
        return _column_nullability(node, sources)
    if isinstance(node, exp.Coalesce) and not node.args.get("is_null"):
        # COALESCE is a CASE to the engine: nullable when an argument is.
        if _combined_nullability((node.this, *node.expressions), sources)[0] == "not_null":
            return "not_null", "all COALESCE arguments are NOT NULL"
        return "nullable", "the engine types COALESCE over a nullable argument as nullable"
    if isinstance(node, exp.Coalesce):
        replacement = node.expressions[0] if node.expressions else None
        if replacement is not None and _nullability(replacement, sources)[0] == "not_null":
            return "not_null", "ISNULL"
        return "unknown", "the ISNULL replacement value is not a NOT NULL value"
    if isinstance(node, exp.TryCast) or (isinstance(node, exp.Convert) and node.args.get("safe")):
        return "unknown", "TRY_CAST and TRY_CONVERT return NULL when the conversion fails"
    if isinstance(node, exp.Cast):
        return _nullability(node.this, sources)
    if isinstance(node, exp.Convert):
        return _nullability(node.expression, sources)
    if isinstance(node, exp.Add | exp.Sub | exp.Mul | exp.Div | exp.Mod):
        return _combined_nullability((node.this, node.expression), sources)
    if isinstance(node, exp.Case):
        default = node.args.get("default")
        if default is None:
            return "nullable", "CASE without ELSE returns NULL"
        branches = [branch.args["true"] for branch in node.args.get("ifs", [])]
        return _combined_nullability((*branches, default), sources)
    return "unknown", f"{node.sql(dialect='tsql')} is not a column, literal or plain arithmetic"


def _combined_nullability(
    nodes: Sequence[Any], sources: Mapping[str, Mapping[str, TableColumnFacts]]
) -> tuple[str, str]:
    states = [_nullability(node, sources) for node in nodes]
    for wanted in ("nullable", "unknown"):
        found = next((state for state in states if state[0] == wanted), None)
        if found is not None:
            return found
    return "not_null", "all inputs are NOT NULL"


def _column_nullability(
    column: Any, sources: Mapping[str, Mapping[str, TableColumnFacts]]
) -> tuple[str, str]:
    name = column.name.casefold()
    label = column.sql(dialect="tsql")
    if column.table:
        owners = [sources[column.table.casefold()]] if column.table.casefold() in sources else []
    else:
        owners = [columns for columns in sources.values() if name in columns]
    if len(owners) != 1 or name not in owners[0]:
        return "unknown", f"the nullability of {label} was not read from the catalog"
    nullable = owners[0][name].is_nullable
    if nullable is None:
        return "unknown", f"the nullability of {label} was not read from the catalog"
    if nullable:
        return "nullable", f"{label} is nullable"
    return "not_null", f"{label} is NOT NULL"


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


def plan_view_experiment(
    definition: str,
    *,
    clustered_index_key: Sequence[str] | None = None,
    extra_indexes: Sequence[ViewIndexSpec] = (),
    schema_name: str | None = None,
    view_name: str | None = None,
    index_name_prefix: str = WORKFLOW_TEST_INDEX_PREFIX,
    name_tag: str = "",
    test_index_name_prefixes: Sequence[str] = (WORKFLOW_TEST_INDEX_PREFIX,),
    table_columns: Mapping[str, Sequence[TableColumnFacts]] | None = None,
    maxdop: int | None = None,
) -> ViewExperimentPlan:
    """Plan one indexed-view experiment: findings, names and DDL. Runs nothing.

    ``view_name`` (or the name in a CREATE VIEW header) is used as given when
    it is under an allowed test view prefix; any other name gives a generated
    one, ``<view prefix>[<tag>_]<FirstBaseTable>_<8 hex>``, and a note. Index
    names follow the test index rule under ``index_name_prefix``, with the
    view name in the place of the table name. ``clustered_index_key`` may be
    left out for a view with GROUP BY: the key is then every GROUP BY column
    the view returns. ``index_name_prefix`` and ``name_tag`` come from the
    tool layer's prefix resolution, ``test_index_name_prefixes`` and
    ``maxdop`` from the database policy. Raises ValueError when
    ``index_name_prefix`` is not an allowed prefix or an extension of one.
    """

    allowed = tuple(test_index_name_prefixes)
    if not extends_test_index_prefix(index_name_prefix, allowed):
        raise ValueError(
            f"index_name_prefix {index_name_prefix!r} is not an allowed test index prefix ("
            + ", ".join(allowed)
            + ") and does not extend one."
        )
    checked = validate_test_view_definition(definition, table_columns=table_columns)
    findings = list(checked.findings)
    notes: list[str] = []
    header_name = checked.requested_name
    schema = schema_name or (header_name[0] if len(header_name) == 2 else "dbo")
    if not _PLAIN_IDENTIFIER.fullmatch(schema):
        findings.append(
            ViewFinding("schema_name", f"The schema name {schema!r} is not a plain identifier.")
        )
        schema = "dbo"
    requested = (view_name or (header_name[-1] if header_name else "")).strip()
    view = requested
    if not is_test_view_name(requested, allowed):
        base_table = checked.base_tables[0].split(".")[-1] if checked.base_tables else "View"
        tag = _usable_name_text(requested)[:24] or name_tag
        view = generated_test_object_name(
            view_name_prefixes((index_name_prefix,))[0],
            [*([tag] if tag else []), base_table],
            _sha256(
                json.dumps(
                    [schema.casefold(), list(checked.column_list), checked.normalized_body],
                    separators=(",", ":"),
                )
            ),
        )
        if requested:
            notes.append(
                f"The requested view name {requested} is not under a test view prefix ("
                + ", ".join(view_name_prefixes(allowed))
                + f"). The view is created as {schema}.{view}: read that name in "
                "candidate_sql."
            )

    columns = ""
    if checked.column_list:
        columns = " (" + ", ".join(_quote(name) for name in checked.column_list) + ")"
    create_view = (
        f"CREATE VIEW [{schema}].[{view}]{columns} WITH SCHEMABINDING AS\n{checked.body}"
    )

    key, key_findings, key_notes = _clustered_key(checked, clustered_index_key)
    findings.extend(key_findings)
    notes.extend(key_notes)
    clustered: IndexCandidate | None = None
    extras: list[IndexCandidate] = []
    if key:
        clustered = _named(
            IndexCandidate(
                schema=schema,
                table=view,
                key_columns=key,
                include_columns=(),
                is_unique=True,
                index_kind=INDEX_KIND_VIEW_CLUSTERED,
            ),
            index_name_prefix,
            name_tag,
        )
    if len(extra_indexes) > MAX_VIEW_EXTRA_INDEXES:
        findings.append(
            ViewFinding(
                "too_many_indexes",
                f"A test view takes at most {MAX_VIEW_EXTRA_INDEXES} nonclustered indexes.",
            )
        )
    for spec in extra_indexes[:MAX_VIEW_EXTRA_INDEXES]:
        spec_findings = _index_column_findings(
            checked, (*spec.key_columns, *spec.include_columns), "nonclustered index"
        )
        if not spec.key_columns:
            spec_findings.append(
                ViewFinding("index_key_required", "A nonclustered index needs a key column.")
            )
        findings.extend(spec_findings)
        if not spec_findings:
            extras.append(
                _named(
                    IndexCandidate(
                        schema=schema,
                        table=view,
                        key_columns=tuple(spec.key_columns),
                        include_columns=tuple(spec.include_columns),
                        is_unique=spec.is_unique,
                        index_kind=INDEX_KIND_ROWSTORE,
                    ),
                    index_name_prefix,
                    name_tag,
                )
            )
    names = [index.index_name for index in (clustered, *extras) if index is not None]
    if len(set(names)) != len(names):
        findings.append(
            ViewFinding("duplicate_index", "The same index definition is listed twice.")
        )
    plan = ViewExperimentPlan(
        schema=schema,
        view_name=view,
        definition=checked,
        create_view_statement=create_view,
        clustered_index=clustered,
        extra_indexes=tuple(extras),
        test_index_name_prefixes=allowed,
        maxdop=maxdop,
        findings=_distinct(findings),
        notes=tuple(notes),
    )
    if plan.valid:
        try:
            plan.index_statements()
        except ValueError as exc:
            return replace(
                plan,
                findings=(*plan.findings, ViewFinding("index_definition", _sentence(str(exc)))),
            )
    return plan


def _named(candidate: IndexCandidate, prefix: str, tag: str) -> IndexCandidate:
    """Name a view index by the test index rule: prefix, view, key columns, hash."""

    name = generated_test_object_name(
        prefix,
        [
            *([tag] if tag else []),
            candidate.table,
            *(_key_name(key) for key in candidate.key_columns),
        ],
        candidate.definition_fingerprint,
    )
    return replace(candidate, index_name=name)


def _key_name(column: str) -> str:
    try:
        return parse_candidate_key(column).name
    except ValueError:
        return str(column)


def _quote(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _index_column_findings(
    checked: ViewDefinitionCheck, columns: Sequence[str], what: str
) -> list[ViewFinding]:
    findings: list[ViewFinding] = []
    output = {name.casefold() for name in checked.output_columns if name}
    for column in columns:
        try:
            name = parse_candidate_key(column).name
        except ValueError:
            findings.append(
                ViewFinding(
                    "index_column_name",
                    f"{column!r} is not a column name with an optional ASC or DESC.",
                )
            )
            continue
        if name.casefold() not in output:
            findings.append(
                ViewFinding(
                    "index_column_not_in_view",
                    f"The {what} column {name} is not a column the view returns ("
                    + ", ".join(name for name in checked.output_columns if name)
                    + ").",
                )
            )
    return findings


def _clustered_key(
    checked: ViewDefinitionCheck, requested: Sequence[str] | None
) -> tuple[tuple[str, ...], list[ViewFinding], list[str]]:
    """The key of the unique clustered index, its findings and notes."""

    notes: list[str] = []
    if not requested:
        if checked.grouped and checked.group_by_columns and not checked.unselected_group_by:
            notes.append(
                "No clustered_index_key was given: the key is every GROUP BY column the "
                "view returns (" + ", ".join(checked.group_by_columns) + ")."
            )
            return checked.group_by_columns, [], notes
        if not checked.output_columns:
            return (), [], notes  # the definition findings already refuse the view
        return (
            (),
            [
                ViewFinding(
                    "clustered_index_key_required",
                    "Give clustered_index_key: the view columns that identify one row. "
                    "Only a view whose GROUP BY columns are all returned gets a default key.",
                )
            ],
            notes,
        )
    findings = _index_column_findings(checked, requested, "clustered index key")
    if findings:
        return (), findings, notes
    key_names = [parse_candidate_key(column).name.casefold() for column in requested]
    grouped = {name.casefold() for name in checked.group_by_columns}
    for column, name in zip(requested, key_names, strict=True):
        if checked.grouped and name not in grouped:
            findings.append(
                ViewFinding(
                    "key_column_not_grouped",
                    f"The key column {parse_candidate_key(column).name} is not a GROUP BY "
                    "column. The key of the unique clustered index of a view with GROUP BY "
                    "can hold GROUP BY columns only.",
                )
            )
    if checked.grouped:
        missing = [
            name for name in checked.group_by_columns if name.casefold() not in set(key_names)
        ]
        missing.extend(checked.unselected_group_by)
        if missing:
            findings.append(
                ViewFinding(
                    "key_misses_group_by_column",
                    "The key does not hold every GROUP BY expression (missing: "
                    + ", ".join(missing)
                    + "). Unless those depend on the key columns, two view rows share a "
                    "key and the build fails with error 1505 after it has read the base "
                    "tables.",
                    "warning",
                )
            )
    return tuple(requested), findings, notes


# ---------------------------------------------------------------------------
# The statement that reads the view
# ---------------------------------------------------------------------------


def view_candidate_findings(
    candidate_sql: str, plan: ViewExperimentPlan, *, read_mode: str = READ_MODE_NOEXPAND
) -> tuple[ViewFinding, ...]:
    """Check the candidate statement against the planned view, before any run.

    ``noexpand``: the statement must read the test view by its two-part name
    and with ``WITH (NOEXPAND)`` on every reference. ``automatic``: the
    statement must not name the view; the unchanged statement is run and the
    plan shows whether the optimizer matched the view.
    """

    if read_mode not in {READ_MODE_NOEXPAND, READ_MODE_AUTOMATIC}:
        raise ValueError(f"read_mode must be {READ_MODE_NOEXPAND} or {READ_MODE_AUTOMATIC}.")
    try:
        statement = sqlglot.parse_one(candidate_sql, read="tsql")
    except Exception:  # noqa: BLE001 - the parser raises several types
        return (
            ViewFinding(
                "candidate_not_parsed",
                "The candidate statement could not be parsed here, so its use of "
                f"{plan.schema}.{plan.view_name} was not checked.",
                "warning",
            ),
        )
    findings: list[ViewFinding] = []
    view = plan.view_name.casefold()
    requested = {
        name.casefold() for name in plan.definition.requested_name[-1:] if name
    } - {view}
    references = 0
    for table in statement.find_all(exp.Table):
        name = table.name.casefold()
        hints = {
            str(hint.name).upper()
            for group in table.args.get("hints") or ()
            for hint in group.expressions
        }
        if name == view:
            references += 1
            if table.db.casefold() != plan.schema.casefold():
                findings.append(
                    ViewFinding(
                        "view_name_not_two_part",
                        f"Read the test view by its two-part name {plan.schema}."
                        f"{plan.view_name}.",
                    )
                )
            if read_mode == READ_MODE_NOEXPAND and "NOEXPAND" not in hints:
                findings.append(
                    ViewFinding(
                        "noexpand_missing",
                        f"Add WITH (NOEXPAND) after {plan.schema}.{plan.view_name} (and its "
                        "alias). Without it the optimizer may expand the view and the "
                        "measurement is not of the view index.",
                    )
                )
        elif "NOEXPAND" in hints:
            findings.append(
                ViewFinding(
                    "noexpand_on_other_object",
                    f"WITH (NOEXPAND) is allowed on the test view only, not on {table.name}.",
                )
            )
        elif name in requested:
            findings.append(
                ViewFinding(
                    "view_renamed",
                    f"The statement reads {table.name}, and the view is created as "
                    f"{plan.schema}.{plan.view_name}. Read that name.",
                )
            )
    if read_mode == READ_MODE_NOEXPAND and references == 0:
        findings.append(
            ViewFinding(
                "view_not_read",
                f"The candidate statement does not read {plan.schema}.{plan.view_name}. "
                f"For read_mode {READ_MODE_AUTOMATIC}, pass the unchanged statement.",
            )
        )
    if read_mode == READ_MODE_AUTOMATIC and references:
        findings.append(
            ViewFinding(
                "view_read_in_automatic_mode",
                "Automatic matching is tested with the unchanged statement: it must not "
                f"name {plan.schema}.{plan.view_name}.",
            )
        )
    return _distinct(findings)


def session_set_option_blockers(row: Mapping[str, Any]) -> tuple[str, ...]:
    """The indexed-view SET options a session has wrong (``SESSION_SET_OPTIONS_SQL`` row).

    A session with a wrong option cannot read the view through its index, and
    its writes to the base tables fail while the view exists. ARITHABORT OFF
    is not reported when ANSI_WARNINGS is ON: at compatibility level 90 or
    higher the engine then treats ARITHABORT as ON, and an ODBC session
    starts that way.
    """

    def is_on(name: str) -> bool | None:
        value = row.get(name.lower())
        return None if value is None else int(value) == 1

    blockers: list[str] = []
    for name, wanted in INDEXED_VIEW_SET_OPTIONS:
        if name == "ARITHABORT" and is_on("ANSI_WARNINGS") and is_on(name) is False:
            continue
        if is_on(name) is not (wanted == "ON"):
            blockers.append(f"{name} must be {wanted}")
    return tuple(blockers)


# ---------------------------------------------------------------------------
# Build and drop batches
# ---------------------------------------------------------------------------


# @@OPTIONS bits: 64 is ARITHABORT, 16384 is XACT_ABORT.
_OPTIONS_BEFORE = "DECLARE @options_before int = @@OPTIONS;"
_RESTORE_ARITHABORT = "IF (@options_before & 64) = 0 SET ARITHABORT OFF;"
_RESTORE_XACT_ABORT = "IF (@options_before & 16384) = 0 SET XACT_ABORT OFF;"
# The value of a new session. The executor sets its own lock timeout in front
# of each read, so no caller depends on the value left here.
_RESTORE_LOCK_TIMEOUT = "SET LOCK_TIMEOUT -1;"


def _set_option_statements() -> list[str]:
    return [f"SET {name} {value};" for name, value in INDEXED_VIEW_SET_OPTIONS]


def _nested_batch(statement: str) -> str:
    text = statement.rstrip().rstrip(";").replace("'", "''")
    return f"    EXEC sys.sp_executesql N'{text}';"


def _marker_statement(schema: str, view: str, property_name: str) -> str:
    return (
        "    EXEC sys.sp_addextendedproperty "
        f"@name = N'{property_name}', @value = ?, "
        f"@level0type = N'SCHEMA', @level0name = N'{schema}', "
        f"@level1type = N'VIEW', @level1name = N'{view}';"
    )


def build_view_experiment_create_batch(plan: ViewExperimentPlan) -> str:
    """One batch that builds the view, its owner markers and its indexes, or nothing.

    Parameters, in order: the lease owner token, ``plan.definition_fingerprint``.
    The batch sets the seven indexed-view options first, so they hold for
    every nested statement whatever the pooled connection had. CREATE VIEW
    must be the first statement of a batch, so it runs through
    sp_executesql, as each CREATE INDEX does. One transaction: a failure
    leaves no object. The CATCH block rolls back and raises error 50020
    whose text carries the engine's error number and message.

    Session state: the pool does not reset a session, so on both paths the
    batch puts ARITHABORT and XACT_ABORT back to what they were. The other
    six options stay as set: an ODBC session already has those values, and
    ANSI_NULLS and QUOTED_IDENTIFIER cannot be set conditionally.
    """

    if not plan.valid:
        raise ValueError("The view experiment plan has findings that refuse it.")
    _require_test_view_target(plan.schema, plan.view_name, plan.test_index_name_prefixes)
    for index in plan.indexes:
        name = index.index_name or ""
        if not _PLAIN_IDENTIFIER.fullmatch(name) or not name.casefold().startswith(
            tuple(prefix.casefold() for prefix in plan.test_index_name_prefixes)
        ):
            raise ValueError(f"index name {name!r} is not under an allowed test index prefix.")
    return "\n".join(
        [
            _OPTIONS_BEFORE,
            *_set_option_statements(),
            "SET XACT_ABORT ON;",
            "BEGIN TRY",
            "    BEGIN TRANSACTION;",
            _nested_batch(plan.create_view_statement),
            _marker_statement(plan.schema, plan.view_name, VIEW_OWNER_PROPERTY),
            _marker_statement(plan.schema, plan.view_name, VIEW_DEFINITION_PROPERTY),
            *(_nested_batch(statement) for statement in plan.index_statements()),
            "    COMMIT TRANSACTION;",
            "END TRY",
            "BEGIN CATCH",
            "    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;",
            f"    {_RESTORE_ARITHABORT}",
            f"    {_RESTORE_XACT_ABORT}",
            "    DECLARE @build_error nvarchar(2048) = CONCAT(",
            f"        N'{_VIEW_BUILD_ROLLED_BACK} Engine error ', ERROR_NUMBER(),",
            "        N': ', ERROR_MESSAGE());",
            f"    THROW {VIEW_BUILD_ERROR_NUMBER}, @build_error, 1;",
            "END CATCH;",
            _RESTORE_ARITHABORT,
            _RESTORE_XACT_ABORT,
        ]
    )


def build_test_view_drop_batch(
    schema: str,
    view_name: str,
    *,
    test_index_name_prefixes: Sequence[str] = (WORKFLOW_TEST_INDEX_PREFIX,),
    lock_timeout_ms: int = TEST_INDEX_DROP_LOCK_TIMEOUT_MS,
) -> str:
    """DROP VIEW (which drops its indexes) only for a view this lease created.

    Parameters, in order: the lease owner token, the expected definition
    fingerprint or NULL, the expected object_id or NULL. Inside one
    transaction the batch reads the owner marker of the view under a held
    lock and raises before the DROP when it is absent or different: 50012
    view missing, 50014 another object_id, 50011 owner marker, 50015
    definition marker. The name must be under a test view prefix.

    DROP VIEW takes Sch-M on the view and Sch-S on each base table (observed
    locks). So it waits, up to ``lock_timeout_ms`` and then fails with error
    1222, only for a session that uses the view or holds an open write
    transaction on a base table; a plain reader of a base table does not
    block it. On both paths the batch puts the lock timeout back to -1 and
    XACT_ABORT back to what it was.
    """

    _require_test_view_target(schema, view_name, test_index_name_prefixes)
    if not 1 <= int(lock_timeout_ms) <= 60_000:
        raise ValueError("lock_timeout_ms must be between 1 and 60000")

    def marker_check(property_name: str, variable: str) -> list[str]:
        return [
            "        SELECT 1",
            "        FROM sys.extended_properties AS ep WITH (UPDLOCK, HOLDLOCK)",
            "        WHERE ep.class = 1",
            "          AND ep.major_id = @object_id",
            "          AND ep.minor_id = 0",
            f"          AND ep.name = N'{property_name}'",
            "          AND CONVERT(nvarchar(4000), ep.value) COLLATE Latin1_General_100_BIN2",
            f"              = {variable} COLLATE Latin1_General_100_BIN2",
        ]

    return "\n".join(
        [
            _OPTIONS_BEFORE,
            "SET XACT_ABORT ON;",
            f"SET LOCK_TIMEOUT {int(lock_timeout_ms)};",
            "BEGIN TRY",
            "    BEGIN TRANSACTION;",
            "    DECLARE @owner_marker nvarchar(4000) = ?;",
            "    DECLARE @expected_definition nvarchar(4000) = ?;",
            "    DECLARE @expected_object_id int = ?;",
            f"    DECLARE @object_id int = OBJECT_ID(N'[{schema}].[{view_name}]', N'V');",
            "    IF @object_id IS NULL",
            "        THROW 50012, 'Test view is missing.', 1;",
            "    IF @expected_object_id IS NOT NULL AND @object_id <> @expected_object_id",
            "        THROW 50014, 'Test view object identity mismatch.', 1;",
            "    IF NOT EXISTS (",
            *marker_check(VIEW_OWNER_PROPERTY, "@owner_marker"),
            "    )",
            "        THROW 50011, 'Test view ownership marker mismatch.', 1;",
            "    IF @expected_definition IS NOT NULL AND NOT EXISTS (",
            *marker_check(VIEW_DEFINITION_PROPERTY, "@expected_definition"),
            "    )",
            "        THROW 50015, 'Test view definition marker mismatch.', 1;",
            f"    DROP VIEW [{schema}].[{view_name}];",
            "    COMMIT TRANSACTION;",
            "END TRY",
            "BEGIN CATCH",
            "    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;",
            f"    {_RESTORE_LOCK_TIMEOUT}",
            f"    {_RESTORE_XACT_ABORT}",
            "    THROW;",
            "END CATCH;",
            _RESTORE_LOCK_TIMEOUT,
            _RESTORE_XACT_ABORT,
        ]
    )


def view_experiment_batch_params(
    plan: ViewExperimentPlan, owner_token: str
) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
    """(create parameters, drop parameters) of one experiment for its owner token."""

    if not _OWNER_TOKEN.fullmatch(owner_token or ""):
        raise ValueError("A view experiment needs a private lease-owner token.")
    fingerprint = plan.definition_fingerprint
    return (owner_token, fingerprint), (owner_token, fingerprint, None)


def view_build_failure(error: BaseException) -> dict[str, Any]:
    """Classify a failed build batch: rolled back for certain, or outcome unknown.

    ``outcome`` is ``definite_rollback`` when the batch's own CATCH block
    reported the failure (it rolls back first) and no sign of a lost
    connection, timeout or cancel is present; then ``engine_error_number``
    is the engine's number from the message. Otherwise ``unknown``, with
    ``reason`` ``connection_or_timeout`` when the transport failed: the batch
    may still be running and may commit.
    """

    base = classify_create_index_failure(error)
    transport = base.get("reason") == "connection_or_timeout"
    engine_number: int | None = None
    current: BaseException | None = error
    seen: set[int] = set()
    rolled_back = False
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        for value in (*getattr(current, "args", ()), getattr(current, "ddbc_error", None)):
            if isinstance(value, str) and _VIEW_BUILD_ROLLED_BACK in value:
                rolled_back = True
                number = re.search(r"Engine error (\d{1,6})", value)
                engine_number = int(number.group(1)) if number else engine_number
        current = current.__cause__ or current.__context__
    if rolled_back and not transport:
        return {
            "outcome": "definite_rollback",
            "engine_error_number": engine_number,
            "reason": "engine_error",
        }
    return {
        "outcome": "unknown",
        "engine_error_number": engine_number,
        "reason": "connection_or_timeout" if transport else "error_not_proven_to_roll_back",
    }


# ---------------------------------------------------------------------------
# Catalog proof, size, lease reconcile
# ---------------------------------------------------------------------------


async def read_view_catalog_state(
    executor: AzureSqlExecutor,
    database_name: str,
    *,
    schema_name: str,
    view_name: str,
) -> ViewCatalogState:
    """Read what the catalog holds under a test view name: at most two reads."""

    rows = await executor.fetch_all(
        database_name,
        TEST_VIEW_CATALOG_SQL,
        params=[VIEW_OWNER_PROPERTY, VIEW_DEFINITION_PROPERTY, schema_name, view_name],
        sql_origin="server_built",
    )
    if not rows:
        return ViewCatalogState()
    state = view_catalog_state_from_row(rows[0])
    if state.object_type != "V":
        return state
    indexes = await collect_existing_indexes(
        executor, database_name, schema_name=schema_name, table_name=view_name
    )
    return replace(state, indexes=tuple(indexes))


def view_catalog_state_from_row(row: Mapping[str, Any]) -> ViewCatalogState:
    def flag(name: str) -> bool | None:
        value = row.get(name)
        return None if value is None else bool(value)

    def text(name: str) -> str | None:
        value = row.get(name)
        return None if value is None else str(value)

    object_id = row.get("object_id")
    return ViewCatalogState(
        object_id=int(object_id) if object_id is not None else None,
        # sys.objects.type is char(2): 'V ' arrives padded.
        object_type=(text("object_type") or "").strip().upper() or None,
        definition=text("definition"),
        is_schema_bound=flag("is_schema_bound"),
        uses_ansi_nulls=flag("uses_ansi_nulls"),
        uses_quoted_identifier=flag("uses_quoted_identifier"),
        owner_marker=text("owner_marker"),
        definition_marker=text("definition_marker"),
    )


def name_in_use_blocker(plan: ViewExperimentPlan, state: ViewCatalogState) -> str | None:
    """Why the planned view name cannot be created now; None when it is free."""

    if not state.exists:
        return None
    kind = "a view" if state.object_type == "V" else "an object that is not a view"
    marked = (
        " It carries a test view owner marker: an earlier experiment left it, and its "
        "lease must clean it up first."
        if state.owner_marker
        else " It has no test view owner marker, so this tool will not drop it."
    )
    return (
        f"{plan.schema}.{plan.view_name} is already the name of {kind}.{marked} "
        "No query and no DDL was run."
    )


def _module_text(value: str | None) -> str:
    return (value or "").strip().rstrip(";").strip()


def verify_view_experiment(
    plan: ViewExperimentPlan, state: ViewCatalogState, *, owner_token: str
) -> ViewExperimentVerification:
    """Whether the catalog holds exactly the planned view and indexes, built by this lease."""

    mismatches: list[str] = []
    if state.object_type != "V":
        return ViewExperimentVerification(False, ("view_missing",))
    if state.owner_marker != owner_token or not owner_token:
        mismatches.append("owner_marker")
    if state.definition_marker != plan.definition_fingerprint:
        mismatches.append("definition_marker")
    if _module_text(state.definition) != _module_text(plan.create_view_statement):
        mismatches.append("view_definition")
    if state.is_schema_bound is not True:
        mismatches.append("schema_binding")
    if state.uses_ansi_nulls is not True or state.uses_quoted_identifier is not True:
        mismatches.append("module_set_options")
    expected = {str(index.index_name).casefold(): index for index in plan.indexes}
    seen: set[str] = set()
    for actual in state.indexes:
        wanted = expected.get(actual.name.casefold())
        if wanted is None:
            mismatches.append(f"unexpected_index:{actual.name}")
            continue
        seen.add(actual.name.casefold())
        if not expected_index_definition_matches(actual, wanted):
            mismatches.append(f"index_definition:{actual.name}")
    mismatches.extend(
        f"index_missing:{index.index_name}"
        for name, index in expected.items()
        if name not in seen
    )
    return ViewExperimentVerification(not mismatches, tuple(mismatches))


def view_index_size(plan: ViewExperimentPlan, state: ViewCatalogState) -> dict[str, Any]:
    """Rows and size of the built view indexes, from the catalog state already read.

    ``rows`` is the row count of the clustered index: the rows the view
    stores. Sizes are reserved pages (8 KB each).
    """

    def pages(index: ExistingIndex) -> int | None:
        counts = [count for _number, count in index.partition_page_counts]
        return None if not counts or any(count is None for count in counts) else sum(
            int(count or 0) for count in counts
        )

    def rows(index: ExistingIndex) -> int | None:
        counts = [count for _number, count in index.partition_row_counts]
        return None if not counts or any(count is None for count in counts) else sum(
            int(count or 0) for count in counts
        )

    def megabytes(page_count: int | None) -> float | None:
        return None if page_count is None else round(page_count * 8 / 1024, 2)

    clustered_name = (
        str(plan.clustered_index.index_name).casefold() if plan.clustered_index else ""
    )
    indexes = [
        {
            "index_name": index.name,
            "index_type": index.index_type,
            "rows": rows(index),
            "size_mb": megabytes(pages(index)),
        }
        for index in state.indexes
    ]
    clustered = next(
        (index for index in state.indexes if index.name.casefold() == clustered_name), None
    )
    all_pages = [pages(index) for index in state.indexes]
    total = None if not all_pages or None in all_pages else sum(int(p or 0) for p in all_pages)
    return {
        "view": f"{plan.schema}.{plan.view_name}",
        "rows": rows(clustered) if clustered is not None else None,
        "size_mb": megabytes(total),
        "indexes": indexes,
        "write_cost": VIEW_EXPERIMENT_WRITE_COST_NOTE,
    }


def reconcile_view_experiment(
    *,
    step: str,
    state: ViewCatalogState | None,
    owner_token: str,
    definition_fingerprint: str,
    transport_failure: bool = False,
) -> ViewLeaseReconcile:
    """The lease status after a build or a drop that failed or was interrupted.

    ``step`` is ``build`` or ``drop``. ``state`` is a fresh catalog read; None
    when that read failed. ``transport_failure``: the step ended with a lost
    connection, a timeout or a cancel (``view_build_failure`` reason
    ``connection_or_timeout``), so its batch may still be running.

    - Catalog not read: ``cleanup_required``. Nothing is known.
    - A view with this lease's markers: ``cleanup_required`` and
      ``drop_view``; the guarded drop may run (again).
    - After a drop, no view of this lease: ``cleaned``.
    - After a build, no object of that name and the engine reported the
      failure: released as ``create_failed``. Nothing is left.
    - After a build, no object of that name and a transport failure:
      ``cleanup_required``. The build may still commit; one catalog read does
      not prove it will not.
    - After a build, an object of that name without this lease's markers: not
      ours, never dropped. The build writes the view and its markers in one
      transaction, so nothing of this lease exists or can still commit:
      ``create_failed``.
    """

    if step not in {"build", "drop"}:
        raise ValueError("step must be build or drop.")
    if state is None:
        return ViewLeaseReconcile("cleanup_required", False, "catalog_not_read")
    if state.owned_by(owner_token, definition_fingerprint):
        return ViewLeaseReconcile("cleanup_required", True, "owned_view_in_catalog")
    if step == "drop":
        return ViewLeaseReconcile("cleaned", False, "owned_view_not_in_catalog")
    if state.exists:
        return ViewLeaseReconcile("create_failed", False, "name_held_by_object_not_owned")
    if transport_failure:
        return ViewLeaseReconcile(
            "cleanup_required", False, "outcome_unknown_after_transport_failure"
        )
    return ViewLeaseReconcile("create_failed", False, "nothing_in_catalog")


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
