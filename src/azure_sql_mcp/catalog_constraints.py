"""Constraint and key facts read from the Azure SQL Database catalog.

An equivalence proof may rely on a data property (a column has no NULL, a key
is unique, a foreign key holds, a CHECK holds) only when the catalog enforces
it. This module is the one reader of those facts. It reads metadata only, with
parameterized object names, and it never reads table rows.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

PRECONDITION_KINDS = ("not_null", "unique", "trusted_foreign_key", "trusted_check")

_OBJECT_FILTER = "OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?))"

_COLUMNS_QUERY = f"""
SELECT
    o.type_desc AS object_type,
    c.name AS column_name,
    c.is_nullable
FROM sys.objects AS o
INNER JOIN sys.columns AS c
    ON c.object_id = o.object_id
WHERE o.object_id = {_OBJECT_FILTER}
ORDER BY c.column_id
"""

_UNIQUE_KEYS_QUERY = f"""
SELECT
    i.name AS index_name,
    i.is_primary_key,
    i.is_unique_constraint,
    i.has_filter,
    i.is_disabled,
    i.is_hypothetical,
    c.name AS column_name
FROM sys.indexes AS i
INNER JOIN sys.index_columns AS ic
    ON ic.object_id = i.object_id
   AND ic.index_id = i.index_id
INNER JOIN sys.columns AS c
    ON c.object_id = ic.object_id
   AND c.column_id = ic.column_id
WHERE i.object_id = {_OBJECT_FILTER}
  AND i.is_unique = 1
  AND ic.key_ordinal > 0
ORDER BY i.index_id, ic.key_ordinal
"""

_FOREIGN_KEYS_QUERY = f"""
SELECT
    fk.name AS constraint_name,
    fk.is_disabled,
    fk.is_not_trusted,
    pc.name AS column_name,
    rs.name AS referenced_schema,
    rt.name AS referenced_table,
    rc.name AS referenced_column
FROM sys.foreign_keys AS fk
INNER JOIN sys.foreign_key_columns AS fkc
    ON fkc.constraint_object_id = fk.object_id
INNER JOIN sys.columns AS pc
    ON pc.object_id = fkc.parent_object_id
   AND pc.column_id = fkc.parent_column_id
INNER JOIN sys.columns AS rc
    ON rc.object_id = fkc.referenced_object_id
   AND rc.column_id = fkc.referenced_column_id
INNER JOIN sys.tables AS rt
    ON rt.object_id = fk.referenced_object_id
INNER JOIN sys.schemas AS rs
    ON rs.schema_id = rt.schema_id
WHERE fk.parent_object_id = {_OBJECT_FILTER}
ORDER BY fk.object_id, fkc.constraint_column_id
"""

_CHECK_CONSTRAINTS_QUERY = f"""
SELECT
    cc.name AS constraint_name,
    cc.is_disabled,
    cc.is_not_trusted,
    cc.definition
FROM sys.check_constraints AS cc
WHERE cc.parent_object_id = {_OBJECT_FILTER}
ORDER BY cc.name
"""


@dataclass(frozen=True, slots=True)
class UniqueKey:
    name: str
    kind: str  # primary_key, unique_constraint or unique_index
    columns: tuple[str, ...]
    has_filter: bool = False
    is_disabled: bool = False
    is_hypothetical: bool = False

    @property
    def usable(self) -> bool:
        """A filtered key is unique only inside its filter; the others do not exist for queries."""

        return not (self.has_filter or self.is_disabled or self.is_hypothetical)


@dataclass(frozen=True, slots=True)
class ForeignKey:
    name: str
    column_pairs: tuple[tuple[str, str], ...]  # (referencing column, referenced column)
    referenced_schema: str
    referenced_table: str
    is_disabled: bool = False
    is_not_trusted: bool = False


@dataclass(frozen=True, slots=True)
class CheckConstraint:
    name: str
    definition: str
    is_disabled: bool = False
    is_not_trusted: bool = False


@dataclass(frozen=True, slots=True)
class TableConstraintFacts:
    schema_name: str
    table_name: str
    exists: bool = False
    object_type: str | None = None
    nullable: Mapping[str, bool] | None = None  # casefolded column name -> is_nullable
    unique_keys: tuple[UniqueKey, ...] = ()
    foreign_keys: tuple[ForeignKey, ...] = ()
    check_constraints: tuple[CheckConstraint, ...] = ()
    read_error: str | None = None

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}"

    def is_not_null(self, column: str) -> bool:
        return (self.nullable or {}).get(column.casefold()) is False


class CatalogConstraintService:
    """Read constraint facts for one table through the metadata executor."""

    def __init__(self, executor: Any) -> None:
        self.executor = executor

    async def table_facts(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
    ) -> TableConstraintFacts:
        params = [schema_name, table_name]
        try:
            columns = await self._fetch(database_name, _COLUMNS_QUERY, params)
            if not columns:
                return TableConstraintFacts(schema_name, table_name)
            key_rows = await self._fetch(database_name, _UNIQUE_KEYS_QUERY, params)
            foreign_key_rows = await self._fetch(database_name, _FOREIGN_KEYS_QUERY, params)
            check_rows = await self._fetch(database_name, _CHECK_CONSTRAINTS_QUERY, params)
        except Exception as exc:
            # An unreadable catalog proves nothing; the caller reports the gap.
            return TableConstraintFacts(
                schema_name,
                table_name,
                read_error=type(exc).__name__,
            )
        return TableConstraintFacts(
            schema_name=schema_name,
            table_name=table_name,
            exists=True,
            object_type=str(columns[0].get("object_type") or "") or None,
            nullable={
                str(row["column_name"]).casefold(): bool(row.get("is_nullable"))
                for row in columns
                if row.get("column_name") is not None
            },
            unique_keys=_unique_keys(key_rows),
            foreign_keys=_foreign_keys(foreign_key_rows),
            check_constraints=tuple(
                CheckConstraint(
                    name=str(row.get("constraint_name") or ""),
                    definition=str(row.get("definition") or ""),
                    is_disabled=bool(row.get("is_disabled")),
                    is_not_trusted=bool(row.get("is_not_trusted")),
                )
                for row in check_rows
            ),
        )

    async def _fetch(
        self,
        database_name: str,
        query: str,
        params: Sequence[Any],
    ) -> list[Mapping[str, Any]]:
        rows = await self.executor.fetch_all(database_name, query, params=list(params))
        return [row for row in rows or () if isinstance(row, Mapping)]


def _unique_keys(rows: Sequence[Mapping[str, Any]]) -> tuple[UniqueKey, ...]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("index_name") or "")
        entry = grouped.setdefault(name, {"row": row, "columns": []})
        if row.get("column_name") is not None:
            entry["columns"].append(str(row["column_name"]))
    keys = []
    for name, entry in grouped.items():
        row = entry["row"]
        kind = (
            "primary_key"
            if row.get("is_primary_key")
            else "unique_constraint"
            if row.get("is_unique_constraint")
            else "unique_index"
        )
        keys.append(
            UniqueKey(
                name=name,
                kind=kind,
                columns=tuple(entry["columns"]),
                has_filter=bool(row.get("has_filter")),
                is_disabled=bool(row.get("is_disabled")),
                is_hypothetical=bool(row.get("is_hypothetical")),
            )
        )
    return tuple(keys)


def _foreign_keys(rows: Sequence[Mapping[str, Any]]) -> tuple[ForeignKey, ...]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("constraint_name") or "")
        entry = grouped.setdefault(name, {"row": row, "pairs": []})
        entry["pairs"].append(
            (str(row.get("column_name") or ""), str(row.get("referenced_column") or ""))
        )
    return tuple(
        ForeignKey(
            name=name,
            column_pairs=tuple(entry["pairs"]),
            referenced_schema=str(entry["row"].get("referenced_schema") or ""),
            referenced_table=str(entry["row"].get("referenced_table") or ""),
            is_disabled=bool(entry["row"].get("is_disabled")),
            is_not_trusted=bool(entry["row"].get("is_not_trusted")),
        )
        for name, entry in grouped.items()
    )


def _folded(columns: Sequence[Any]) -> set[str]:
    return {str(column).casefold() for column in columns}


def _order_keys(facts: TableConstraintFacts) -> list[UniqueKey]:
    """Usable unique keys with no nullable column, primary key first."""

    keys = [
        key
        for key in facts.unique_keys
        if key.usable and key.columns and all(facts.is_not_null(column) for column in key.columns)
    ]
    return sorted(keys, key=lambda key: (key.kind != "primary_key", len(key.columns)))


def deterministic_order_key(
    facts: TableConstraintFacts,
    columns: Sequence[str],
) -> UniqueKey | None:
    """Return a unique NOT NULL key the ordering columns cover, if the catalog has one.

    Ordering by a superset of such a key leaves no two rows of the table tied,
    so TOP, OFFSET and ROW_NUMBER pick the same rows on every execution.
    """

    if not facts.exists:
        return None
    ordered = _folded(columns)
    return next(
        (key for key in _order_keys(facts) if _folded(key.columns) <= ordered),
        None,
    )


def order_tiebreaker(
    facts: TableConstraintFacts,
    columns: Sequence[str],
) -> dict[str, Any] | None:
    """Name the key columns to append so the ordering becomes deterministic."""

    if not facts.exists:
        return None
    ordered = _folded(columns)
    keys = _order_keys(facts)
    if not keys:
        return None
    key = min(
        keys,
        key=lambda item: len([column for column in item.columns if column.casefold() not in ordered]),
    )
    return {
        "key_name": key.name,
        "key_kind": key.kind,
        "key_columns": list(key.columns),
        "missing_columns": [
            column for column in key.columns if column.casefold() not in ordered
        ],
    }


def _normalized_predicate(text: str) -> str:
    """Compare a predicate with a catalog definition: no brackets, parentheses, space or case."""

    return re.sub(r"[\[\]()\s]", "", text).casefold()


def verify_precondition(
    facts: TableConstraintFacts,
    item: Mapping[str, Any],
) -> dict[str, Any]:
    """Check one declared precondition against catalog facts.

    The result says only that the constraint exists and is enforced. Whether
    it is the constraint the rewrite depends on stays the declarer's claim.
    """

    kind = str(item.get("kind") or "").strip().casefold()
    columns = [str(column) for column in item.get("columns") or ()]
    result: dict[str, Any] = {
        "kind": kind,
        "schema_name": facts.schema_name,
        "table_name": facts.table_name,
        "columns": columns,
        "verified": False,
    }

    def failed(reason: str) -> dict[str, Any]:
        result["reason"] = reason
        return result

    def passed(reason: str, **evidence: Any) -> dict[str, Any]:
        result.update({"verified": True, "reason": reason, **evidence})
        return result

    if kind not in PRECONDITION_KINDS:
        return failed(f"kind must be one of {', '.join(PRECONDITION_KINDS)}.")
    if not facts.exists:
        return failed(
            f"{facts.qualified_name} could not be read from the catalog"
            + (f" ({facts.read_error})." if facts.read_error else ".")
        )

    if kind == "not_null":
        if not columns:
            return failed("not_null needs at least one column.")
        nullable = [column for column in columns if not facts.is_not_null(column)]
        if nullable:
            return failed(
                f"{', '.join(nullable)} in {facts.qualified_name} is nullable or does not exist."
            )
        return passed("every column is declared NOT NULL.")

    if kind == "unique":
        if not columns:
            return failed("unique needs at least one column.")
        declared = _folded(columns)
        key = next(
            (
                key
                for key in facts.unique_keys
                if key.usable and key.columns and _folded(key.columns) <= declared
            ),
            None,
        )
        if key is None:
            return failed(
                f"{facts.qualified_name} has no enabled, unfiltered unique key within "
                f"({', '.join(columns)})."
            )
        return passed(
            "an enabled, unfiltered unique key covers the columns.",
            constraint_name=key.name,
            constraint_kind=key.kind,
        )

    if kind == "trusted_foreign_key":
        referenced_columns = [str(column) for column in item.get("referenced_columns") or ()]
        if not columns or len(columns) != len(referenced_columns):
            return failed("trusted_foreign_key needs matching columns and referenced_columns.")
        wanted_pairs = {
            (left.casefold(), right.casefold())
            for left, right in zip(columns, referenced_columns)
        }
        wanted_table = (
            str(item.get("referenced_schema") or "").casefold(),
            str(item.get("referenced_table") or "").casefold(),
        )
        matches = [
            key
            for key in facts.foreign_keys
            if {(left.casefold(), right.casefold()) for left, right in key.column_pairs}
            == wanted_pairs
            and (key.referenced_schema.casefold(), key.referenced_table.casefold()) == wanted_table
        ]
        if not matches:
            return failed(
                f"{facts.qualified_name} has no foreign key on exactly these column pairs "
                "to the named table."
            )
        trusted = next(
            (key for key in matches if not key.is_disabled and not key.is_not_trusted),
            None,
        )
        if trusted is None:
            return failed(
                f"foreign key {matches[0].name} is disabled or not trusted "
                "(is_not_trusted = 1): existing rows were never checked."
            )
        return passed("the foreign key is enabled and trusted.", constraint_name=trusted.name)

    constraint_name = str(item.get("constraint_name") or "").casefold()
    expected = _normalized_predicate(str(item.get("expected_predicate") or ""))
    if not constraint_name or not expected:
        return failed("trusted_check needs constraint_name and expected_predicate.")
    check = next(
        (
            constraint
            for constraint in facts.check_constraints
            if constraint.name.casefold() == constraint_name
        ),
        None,
    )
    if check is None:
        return failed(f"{facts.qualified_name} has no CHECK constraint with that name.")
    if check.is_disabled or check.is_not_trusted:
        return failed(f"CHECK constraint {check.name} is disabled or not trusted.")
    if _normalized_predicate(check.definition) != expected:
        return failed(
            f"CHECK constraint {check.name} does not have the expected predicate."
        )
    return passed(
        "the CHECK constraint is enabled, trusted and has the expected predicate.",
        constraint_name=check.name,
    )


__all__ = [
    "PRECONDITION_KINDS",
    "CatalogConstraintService",
    "CheckConstraint",
    "ForeignKey",
    "TableConstraintFacts",
    "UniqueKey",
    "deterministic_order_key",
    "order_tiebreaker",
    "verify_precondition",
]
