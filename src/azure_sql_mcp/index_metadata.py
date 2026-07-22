"""Full-fidelity existing-index metadata and semantic coverage checks."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable

from .connection import AzureSqlExecutor


EXISTING_INDEX_METADATA_SQL = """
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.index_id,
    i.name AS index_name,
    i.type_desc AS index_type,
    i.is_unique,
    i.is_primary_key,
    i.is_unique_constraint,
    i.is_disabled,
    i.has_filter,
    i.filter_definition,
    i.fill_factor,
    ic.index_column_id,
    ic.key_ordinal,
    ic.is_included_column,
    ic.is_descending_key,
    ic.partition_ordinal,
    c.name AS column_name,
    ds.name AS data_space_name,
    ds.type_desc AS data_space_type,
    p.partition_number,
    p.data_compression_desc,
    COALESCE(us.user_seeks, 0) AS user_seeks,
    COALESCE(us.user_scans, 0) AS user_scans,
    COALESCE(us.user_lookups, 0) AS user_lookups,
    COALESCE(us.user_updates, 0) AS user_updates,
    CONVERT(varchar(33), SYSUTCDATETIME(), 127) AS collected_at_utc
FROM sys.indexes AS i
INNER JOIN sys.tables AS t
    ON i.object_id = t.object_id
INNER JOIN sys.schemas AS s
    ON t.schema_id = s.schema_id
LEFT JOIN sys.index_columns AS ic
    ON i.object_id = ic.object_id
    AND i.index_id = ic.index_id
LEFT JOIN sys.columns AS c
    ON ic.object_id = c.object_id
    AND ic.column_id = c.column_id
LEFT JOIN sys.data_spaces AS ds
    ON i.data_space_id = ds.data_space_id
LEFT JOIN sys.partitions AS p
    ON i.object_id = p.object_id
    AND i.index_id = p.index_id
LEFT JOIN sys.dm_db_index_usage_stats AS us
    ON i.object_id = us.object_id
    AND i.index_id = us.index_id
    AND us.database_id = DB_ID()
WHERE i.is_hypothetical = 0
  AND i.index_id > 0
ORDER BY
    s.name,
    t.name,
    i.index_id,
    ic.key_ordinal,
    ic.index_column_id,
    p.partition_number
"""

_DIRECTION_PATTERN = re.compile(r"^(.*?)(?:\s+(ASC|DESC))?$", re.IGNORECASE)


@dataclass(frozen=True)
class IndexKeyColumn:
    name: str
    direction: str = "ASC"

    def __post_init__(self) -> None:
        name = _clean_identifier(self.name)
        direction = self.direction.upper()
        if not name:
            raise ValueError("index key column name must not be empty")
        if direction not in {"ASC", "DESC"}:
            raise ValueError("index key direction must be ASC or DESC")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "direction", direction)

    def as_dict(self) -> dict[str, str]:
        return {"name": self.name, "direction": self.direction}


@dataclass(frozen=True)
class ExistingIndex:
    schema: str
    table: str
    index_id: int
    name: str
    index_type: str
    key_columns: tuple[IndexKeyColumn, ...]
    include_columns: tuple[str, ...] = ()
    nonkey_columns: tuple[str, ...] = ()
    filter_definition: str | None = None
    is_unique: bool = False
    is_primary_key: bool = False
    is_unique_constraint: bool = False
    is_disabled: bool = False
    fill_factor: int = 0
    partition_columns: tuple[str, ...] = ()
    data_space_name: str | None = None
    data_space_type: str | None = None
    partition_compression: tuple[tuple[int, str], ...] = ()
    usage: dict[str, int] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "table": self.table,
            "index_id": self.index_id,
            "name": self.name,
            "index_type": self.index_type,
            "key_columns": [column.as_dict() for column in self.key_columns],
            "include_columns": list(self.include_columns),
            "nonkey_columns": list(self.nonkey_columns),
            "filter_definition": self.filter_definition,
            "is_unique": self.is_unique,
            "is_primary_key": self.is_primary_key,
            "is_unique_constraint": self.is_unique_constraint,
            "is_disabled": self.is_disabled,
            "fill_factor": self.fill_factor,
            "partition_columns": list(self.partition_columns),
            "data_space_name": self.data_space_name,
            "data_space_type": self.data_space_type,
            "partition_compression": [
                {"partition_number": number, "compression": compression}
                for number, compression in self.partition_compression
            ],
            "usage": dict(self.usage),
            "provenance": dict(self.provenance),
        }


async def collect_existing_indexes(
    executor: AzureSqlExecutor,
    database_name: str,
) -> list[ExistingIndex]:
    rows = await executor.fetch_all(database_name, EXISTING_INDEX_METADATA_SQL)
    return parse_existing_index_rows(rows)


def parse_existing_index_rows(rows: Iterable[dict[str, Any]]) -> list[ExistingIndex]:
    grouped: dict[tuple[str, str, int], dict[str, Any]] = {}
    for row in rows:
        index_id = _as_int(row.get("index_id"))
        if index_id <= 0:
            continue
        schema = str(row.get("schema_name") or "dbo")
        table = str(row.get("table_name") or "")
        key = (schema, table, index_id)
        item = grouped.setdefault(
            key,
            {
                "row": dict(row),
                "keys": {},
                "includes": {},
                "nonkeys": {},
                "partitions": set(),
                "partition_columns": {},
            },
        )
        column_name = str(row.get("column_name") or "")
        index_column_id = _as_int(row.get("index_column_id"))
        key_ordinal = _as_int(row.get("key_ordinal"))
        if column_name and bool(row.get("is_included_column")):
            item["includes"][index_column_id] = _clean_identifier(column_name)
        elif column_name and key_ordinal > 0:
            item["keys"][key_ordinal] = IndexKeyColumn(
                column_name,
                "DESC" if bool(row.get("is_descending_key")) else "ASC",
            )
        elif column_name:
            item["nonkeys"][index_column_id] = _clean_identifier(column_name)
        partition_ordinal = _as_int(row.get("partition_ordinal"))
        if column_name and partition_ordinal > 0:
            item["partition_columns"][partition_ordinal] = _clean_identifier(column_name)
        partition_number = _as_int(row.get("partition_number"))
        compression = row.get("data_compression_desc")
        if partition_number > 0 and compression:
            item["partitions"].add((partition_number, str(compression)))

    indexes: list[ExistingIndex] = []
    for (schema, table, index_id), item in grouped.items():
        row = item["row"]
        keys = tuple(item["keys"][ordinal] for ordinal in sorted(item["keys"]))
        indexes.append(
            ExistingIndex(
                schema=schema,
                table=table,
                index_id=index_id,
                name=str(row.get("index_name") or f"index_{index_id}"),
                index_type=str(row.get("index_type") or "UNKNOWN"),
                key_columns=keys,
                include_columns=tuple(
                    item["includes"][ordinal] for ordinal in sorted(item["includes"])
                ),
                nonkey_columns=tuple(
                    item["nonkeys"][ordinal] for ordinal in sorted(item["nonkeys"])
                ),
                filter_definition=(
                    str(row["filter_definition"])
                    if row.get("filter_definition") is not None
                    else None
                ),
                is_unique=bool(row.get("is_unique")),
                is_primary_key=bool(row.get("is_primary_key")),
                is_unique_constraint=bool(row.get("is_unique_constraint")),
                is_disabled=bool(row.get("is_disabled")),
                fill_factor=_as_int(row.get("fill_factor")),
                partition_columns=tuple(
                    item["partition_columns"][ordinal]
                    for ordinal in sorted(item["partition_columns"])
                ),
                data_space_name=(
                    str(row["data_space_name"])
                    if row.get("data_space_name") is not None
                    else None
                ),
                data_space_type=(
                    str(row["data_space_type"])
                    if row.get("data_space_type") is not None
                    else None
                ),
                partition_compression=tuple(sorted(item["partitions"])),
                usage={
                    "user_seeks": _as_int(row.get("user_seeks")),
                    "user_scans": _as_int(row.get("user_scans")),
                    "user_lookups": _as_int(row.get("user_lookups")),
                    "user_updates": _as_int(row.get("user_updates")),
                },
                provenance={
                    "source": "sys.indexes/sys.index_columns/sys.partitions",
                    "collected_at_utc": row.get("collected_at_utc"),
                },
            )
        )
    return sorted(
        indexes,
        key=lambda index: (index.schema.casefold(), index.table.casefold(), index.index_id),
    )


def coerce_existing_indexes(
    values: Iterable[ExistingIndex | tuple[str, str, tuple[str, ...]]],
) -> list[ExistingIndex]:
    result: list[ExistingIndex] = []
    for ordinal, value in enumerate(values, start=1):
        if isinstance(value, ExistingIndex):
            result.append(value)
            continue
        schema, table, columns = value
        result.append(
            ExistingIndex(
                schema=schema,
                table=table,
                index_id=-ordinal,
                name=f"legacy_index_{ordinal}",
                index_type="UNKNOWN",
                key_columns=tuple(parse_candidate_key(column) for column in columns),
                provenance={"source": "legacy_signature"},
            )
        )
    return result


def existing_index_covers_candidate(
    index: ExistingIndex,
    *,
    schema: str,
    table: str,
    key_columns: Iterable[str],
    include_columns: Iterable[str] = (),
    filter_definition: str | None = None,
) -> bool:
    if index.is_disabled:
        return False
    if index.schema.casefold() != schema.casefold() or index.table.casefold() != table.casefold():
        return False

    candidate_keys = tuple(parse_candidate_key(column) for column in key_columns)
    if len(candidate_keys) > len(index.key_columns):
        return False
    for candidate, existing in zip(candidate_keys, index.key_columns, strict=False):
        if candidate.name.casefold() != existing.name.casefold():
            return False
        if candidate.direction != existing.direction:
            return False

    candidate_filter = _normalize_filter(filter_definition)
    existing_filter = _normalize_filter(index.filter_definition)
    if candidate_filter != existing_filter:
        return False

    available = {
        column.name.casefold() for column in index.key_columns
    } | {column.casefold() for column in index.include_columns}
    required = {_clean_identifier(column).casefold() for column in include_columns if column}
    return required.issubset(available)


def parse_candidate_key(value: str) -> IndexKeyColumn:
    match = _DIRECTION_PATTERN.fullmatch(str(value).strip())
    if match is None:
        raise ValueError(f"invalid candidate key column: {value!r}")
    return IndexKeyColumn(
        _clean_identifier(match.group(1)),
        (match.group(2) or "ASC").upper(),
    )


def _clean_identifier(value: str) -> str:
    return str(value).strip().strip("[]").strip()


def _normalize_filter(value: str | None) -> str | None:
    if value is None or not str(value).strip():
        return None
    return " ".join(str(value).split()).casefold()


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
