"""DMV column contract for Azure SQL Database (test support, not a test module).

``tests/fixtures/azure_dmv/columns.json`` holds the column sets that Microsoft
Learn documents for each Azure SQL Database DMV the code reads. Owner captures
in ``tests/fixtures/azure_dmv/live/<tier>.json`` (``scripts/capture_dmv_columns.py``)
are authoritative for their tier; the documented sets are the fallback.

``StrictDmvExecutor`` parses every statement with sqlglot (T-SQL) and raises an
error shaped like the driver's ``Invalid column name 'x'. (207)`` when a statement
names a column of a contract DMV that is not in the set, or that the page marks as
internal use only. A statement sqlglot cannot parse fails unless it is on
``UNPARSED_ALLOWLIST`` with a reason, so a parser gap can never hide drift.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.scope import Scope
from sqlglot.optimizer.scope import traverse_scope

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "azure_dmv"
COLUMNS_PATH = FIXTURE_DIR / "columns.json"
LIVE_DIR = FIXTURE_DIR / "live"

# Statement needle -> why sqlglot may not parse it. Empty: every statement parses.
UNPARSED_ALLOWLIST: dict[str, str] = {}


@dataclass(frozen=True)
class DmvContract:
    columns: frozenset[str]
    internal: frozenset[str] = frozenset()


class DmvColumnError(Exception):
    """Raised like SQL Server error 207 for a column the DMV does not have."""


class UnparsedStatementError(AssertionError):
    """A statement the contract could not check."""


def documented_contracts() -> dict[str, DmvContract]:
    payload = json.loads(COLUMNS_PATH.read_text())
    return {
        name.lower(): DmvContract(
            columns=frozenset(column.lower() for column in entry["columns"]),
            internal=frozenset(column.lower() for column in entry.get("internal", ())),
        )
        for name, entry in payload["dmvs"].items()
    }


def documented_entries() -> dict[str, dict[str, Any]]:
    return json.loads(COLUMNS_PATH.read_text())["dmvs"]


def live_contracts(path: Path) -> dict[str, DmvContract]:
    """Column sets from one owner capture; internal markers still come from the docs."""

    documented = documented_contracts()
    captured = json.loads(path.read_text())["dmvs"]
    contracts = dict(documented)
    for name, entry in captured.items():
        if "columns" not in entry:
            continue
        key = name.lower()
        internal = documented[key].internal if key in documented else frozenset()
        contracts[key] = DmvContract(frozenset(column.lower() for column in entry["columns"]), internal)
    return contracts


def documented_row(dmv: str, **values: Any) -> dict[str, Any]:
    """One row shaped like ``SELECT *`` on the DMV: every documented column, unset ones NULL."""

    entry = documented_entries()[dmv]
    columns = entry["columns"]
    unknown = set(values) - set(columns)
    if unknown:
        raise KeyError(f"{dmv} documents no column {sorted(unknown)}")
    return {column: values.get(column) for column in columns}


def _source_name(source: Any) -> str | None:
    """``sys.<object>`` for a table, table-valued function or APPLY source."""

    node = source
    if isinstance(source, Scope):
        if not isinstance(source.expression, exp.Lateral):
            return None
        node = source.expression.this
    if isinstance(node, exp.Table):
        if isinstance(node.this, exp.Anonymous):
            return f"{node.db}.{node.this.name}".lower()
        return f"{node.db}.{node.name}".lower()
    if isinstance(node, exp.Dot) and isinstance(node.expression, exp.Anonymous):
        return f"{node.this.name}.{node.expression.name}".lower()
    return None


def _plain_column(projection: exp.Expression) -> str | None:
    node = projection.this if isinstance(projection, exp.Alias) else projection
    while isinstance(node, (exp.Cast, exp.TryCast)):
        node = node.this
    return node.name.lower() if isinstance(node, exp.Column) else None


class StrictDmvExecutor:
    """Fake executor that enforces the DMV column contract on every statement.

    ``rows`` maps a DMV to the rows it holds; each key must be a documented
    column. ``SELECT *`` returns every documented column, and a projection of plain
    (optionally cast) columns returns those columns. ``responses`` are
    ``(needle, rows | Exception)`` pairs checked first, for aggregates and joins.
    Every violation is kept in ``violations`` even when the caller swallows the error.
    """

    def __init__(
        self,
        rows: Mapping[str, list[dict[str, Any]]] | None = None,
        responses: Sequence[tuple[str, Any]] = (),
        *,
        contracts: Mapping[str, DmvContract] | None = None,
        allowlist: Mapping[str, str] | None = None,
    ) -> None:
        self.contracts = dict(contracts or documented_contracts())
        self.rows: dict[str, list[dict[str, Any]]] = {}
        for dmv, dmv_rows in (rows or {}).items():
            key = dmv.lower()
            for row in dmv_rows:
                unknown = {column.lower() for column in row} - self.contracts[key].columns
                if unknown:
                    raise KeyError(f"fixture row for {dmv} uses undocumented columns {sorted(unknown)}")
            self.rows[key] = [{column.lower(): value for column, value in row.items()} for row in dmv_rows]
        self.responses = list(responses)
        self.allowlist = dict(UNPARSED_ALLOWLIST if allowlist is None else allowlist)
        self.calls: list[tuple[str, str, tuple[Any, ...] | None]] = []
        self.violations: list[str] = []

    async def fetch_all(
        self,
        database_name: str,
        query: str,
        params: Sequence[Any] | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        self.calls.append((database_name, query, tuple(params) if params is not None else None))
        trees = self.check(query)
        for needle, result in self.responses:
            if needle in query:
                if isinstance(result, BaseException):
                    raise result
                return result
        return self._rows_for(trees[-1] if trees else None)

    def check(self, query: str) -> list[exp.Expression]:
        """Parse ``query`` and raise on the first column outside the contract."""

        try:
            trees = [tree for tree in sqlglot.parse(query, read="tsql") if tree is not None]
            scopes = [scope for tree in trees for scope in traverse_scope(tree)]
        except SqlglotError as exc:
            for needle, reason in self.allowlist.items():
                if needle in query and reason:
                    return []
            self.violations.append(f"unparsed: {exc}")
            raise UnparsedStatementError(f"sqlglot could not parse a statement: {exc}\n{query}") from exc
        for scope in scopes:
            self._check_scope(scope)
        return trees

    def _check_scope(self, scope: Scope) -> None:
        names = {alias: _source_name(source) for alias, source in scope.sources.items()}
        contract_sources = {alias: name for alias, name in names.items() if name in self.contracts}
        only_contracts = bool(names) and len(contract_sources) == len(names)
        for column in scope.columns:
            if isinstance(column.this, exp.Star):
                continue
            name = column.name.lower()
            if column.table:
                dmv = contract_sources.get(column.table)
                allowed = [self.contracts[dmv]] if dmv else []
            elif only_contracts:
                allowed = [self.contracts[dmv] for dmv in contract_sources.values()]
            else:
                allowed = []
            if not allowed:
                continue
            if any(name in contract.internal for contract in allowed) or not any(
                name in contract.columns for contract in allowed
            ):
                self.violations.append(name)
                raise DmvColumnError(f"Invalid column name '{column.name}'. (207)")

    def _rows_for(self, tree: exp.Expression | None) -> list[dict[str, Any]]:
        if not isinstance(tree, exp.Select):
            return []
        scope = traverse_scope(tree)[-1]
        names = [_source_name(source) for source in scope.sources.values()]
        if len(names) != 1 or names[0] not in self.contracts:
            return []
        dmv = names[0]
        assert dmv is not None
        stored = self.rows.get(dmv, [])
        if tree.is_star:
            documented = sorted(self.contracts[dmv].columns)
            return [{column: row.get(column) for column in documented} for row in stored]
        output: list[tuple[str, str]] = []
        for projection in tree.expressions:
            source_column = _plain_column(projection)
            if source_column is None:
                return []
            output.append((projection.alias_or_name, source_column))
        return [{alias: row.get(column) for alias, column in output} for row in stored]
