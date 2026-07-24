"""Full-fidelity existing-index metadata and semantic coverage checks."""

from __future__ import annotations

import hashlib
import json
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
    kc.name AS constraint_name,
    kc.type_desc AS constraint_type,
    ic.index_column_id,
    ic.key_ordinal,
    ic.is_included_column,
    ic.is_descending_key,
    ic.partition_ordinal,
    c.name AS column_name,
    ds.name AS data_space_name,
    ds.type_desc AS data_space_type,
    ps.name AS partition_scheme_name,
    pf.name AS partition_function_name,
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
LEFT JOIN sys.partition_schemes AS ps
    ON i.data_space_id = ps.data_space_id
LEFT JOIN sys.partition_functions AS pf
    ON ps.function_id = pf.function_id
LEFT JOIN sys.key_constraints AS kc
    ON kc.parent_object_id = i.object_id
    AND kc.unique_index_id = i.index_id
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

_DIRECTION_PATTERN = re.compile(
    r"^(?:\[(?P<bracketed>[^\]\r\n]+)\]|"
    r"(?P<plain>[A-Za-z_][A-Za-z0-9_]*))"
    r"(?:\s+(?P<direction>ASC|DESC))?$",
    re.IGNORECASE,
)


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
    constraint_name: str | None = None
    constraint_type: str | None = None
    is_disabled: bool = False
    fill_factor: int = 0
    partition_columns: tuple[str, ...] = ()
    data_space_name: str | None = None
    data_space_type: str | None = None
    partition_scheme_name: str | None = None
    partition_function_name: str | None = None
    partition_compression: tuple[tuple[int, str], ...] = ()
    usage: dict[str, int] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)

    @property
    def definition_fingerprint(self) -> str:
        return index_definition_fingerprint(
            schema=self.schema,
            table=self.table,
            index_type=self.index_type,
            key_columns=self.key_columns,
            include_columns=self.include_columns,
            filter_definition=self.filter_definition,
            is_unique=self.is_unique,
            is_primary_key=self.is_primary_key,
            is_unique_constraint=self.is_unique_constraint,
            constraint_name=self.constraint_name,
            constraint_type=self.constraint_type,
            is_disabled=self.is_disabled,
            fill_factor=self.fill_factor,
            partition_columns=self.partition_columns,
            data_space_name=self.data_space_name,
            data_space_type=self.data_space_type,
            partition_scheme_name=self.partition_scheme_name,
            partition_function_name=self.partition_function_name,
            partition_compression=self.partition_compression,
        )

    @property
    def fingerprint(self) -> str:
        """Stable fingerprint of the complete observed index definition."""

        return self.definition_fingerprint

    @property
    def ownership_fingerprint(self) -> str:
        """Stable name/table/definition token suitable for lease fencing."""

        return index_ownership_fingerprint(
            self.schema,
            self.table,
            self.name,
            self.definition_fingerprint,
        )

    @property
    def ownership(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "table": self.table,
            "index_name": self.name,
            "index_id": self.index_id,
            "constraint_name": self.constraint_name,
            "definition_fingerprint": self.definition_fingerprint,
            "ownership_fingerprint": self.ownership_fingerprint,
        }

    @property
    def is_unused(self) -> bool:
        return sum(
            int(self.usage.get(metric, 0) or 0)
            for metric in ("user_seeks", "user_scans", "user_lookups")
        ) == 0

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
            "constraint_name": self.constraint_name,
            "constraint_type": self.constraint_type,
            "is_disabled": self.is_disabled,
            "fill_factor": self.fill_factor,
            "partition_columns": list(self.partition_columns),
            "data_space_name": self.data_space_name,
            "data_space_type": self.data_space_type,
            "partition_scheme_name": self.partition_scheme_name,
            "partition_function_name": self.partition_function_name,
            "partition_compression": [
                {"partition_number": number, "compression": compression}
                for number, compression in self.partition_compression
            ],
            "usage": dict(self.usage),
            "provenance": dict(self.provenance),
            "definition_fingerprint": self.definition_fingerprint,
            "fingerprint": self.fingerprint,
            "ownership": self.ownership,
            "is_unused": self.is_unused,
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
                is_unique=_as_bool(row.get("is_unique")),
                is_primary_key=_as_bool(row.get("is_primary_key")),
                is_unique_constraint=_as_bool(row.get("is_unique_constraint")),
                constraint_name=(
                    str(row["constraint_name"])
                    if row.get("constraint_name") is not None
                    else None
                ),
                constraint_type=(
                    str(row["constraint_type"])
                    if row.get("constraint_type") is not None
                    else None
                ),
                is_disabled=_as_bool(row.get("is_disabled")),
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
                partition_scheme_name=(
                    str(row["partition_scheme_name"])
                    if row.get("partition_scheme_name") is not None
                    else None
                ),
                partition_function_name=(
                    str(row["partition_function_name"])
                    if row.get("partition_function_name") is not None
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
                    "constraint_source": "sys.key_constraints",
                    "data_space_source": "sys.data_spaces/sys.partition_schemes",
                },
            )
        )
    return sorted(
        indexes,
        key=lambda index: (index.schema, index.table, index.index_id),
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
    is_unique: bool = False,
    data_space_name: str | None = None,
    partition_columns: Iterable[str] = (),
) -> bool:
    if index.is_disabled:
        return False
    if _clean_identifier(index.schema) != _clean_identifier(schema) or _clean_identifier(
        index.table
    ) != _clean_identifier(table):
        return False

    candidate_keys = tuple(parse_candidate_key(column) for column in key_columns)
    if len(candidate_keys) > len(index.key_columns):
        return False
    for candidate, existing in zip(candidate_keys, index.key_columns, strict=False):
        if candidate.name != existing.name:
            return False
        if candidate.direction != existing.direction:
            return False

    candidate_filter = _normalize_filter(filter_definition)
    existing_filter = _normalize_filter(index.filter_definition)
    if candidate_filter != existing_filter:
        return False
    if is_unique and not index.is_unique:
        return False
    if data_space_name is not None and (
        _clean_identifier(data_space_name)
        != _clean_identifier(index.data_space_name or "")
    ):
        return False
    required_partitions = tuple(_clean_identifier(column) for column in partition_columns)
    if required_partitions and tuple(index.partition_columns) != required_partitions:
        return False

    available = {column.name for column in index.key_columns} | set(index.include_columns)
    required = {_clean_identifier(column) for column in include_columns if column}
    return required.issubset(available)


def parse_candidate_key(value: str) -> IndexKeyColumn:
    match = _DIRECTION_PATTERN.fullmatch(str(value).strip())
    if match is None:
        raise ValueError(f"invalid candidate key column: {value!r}")
    return IndexKeyColumn(
        match.group("bracketed") or match.group("plain") or "",
        (match.group("direction") or "ASC").upper(),
    )


def _clean_identifier(value: str) -> str:
    return str(value).strip().strip("[]").strip()


def _normalize_filter(value: str | None) -> str | None:
    if value is None or not str(value).strip():
        return None
    text = str(value).strip()
    normalized: list[str] = []
    pending_space = False
    index = 0

    def append_text(fragment: str) -> None:
        nonlocal pending_space
        if pending_space and normalized:
            normalized.append(" ")
        pending_space = False
        normalized.append(fragment)

    while index < len(text):
        char = text[index]
        if char == "'" or (
            char in {"N", "n"}
            and index + 1 < len(text)
            and text[index + 1] == "'"
        ):
            start = index
            if char in {"N", "n"}:
                index += 1
            index += 1
            while index < len(text):
                if text[index] == "'":
                    if index + 1 < len(text) and text[index + 1] == "'":
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            append_text(text[start:index])
        elif char in {"[", '"'}:
            closing = "]" if char == "[" else '"'
            start = index
            index += 1
            while index < len(text):
                if text[index] == closing:
                    if index + 1 < len(text) and text[index + 1] == closing:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            append_text(text[start:index])
        elif char == "-" and index + 1 < len(text) and text[index + 1] == "-":
            index += 2
            while index < len(text) and text[index] not in "\r\n":
                index += 1
            pending_space = True
        elif char == "/" and index + 1 < len(text) and text[index + 1] == "*":
            index += 2
            depth = 1
            while index < len(text) and depth:
                if text.startswith("/*", index):
                    depth += 1
                    index += 2
                elif text.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            pending_space = True
        elif char.isspace():
            pending_space = True
            index += 1
        else:
            append_text(char)
            index += 1
    return "".join(normalized)


def normalize_index_definition(value: str | None) -> str | None:
    """Normalize only whitespace/comments; preserve SQL spelling and literals."""

    return _normalize_filter(value)


def index_definition_payload(
    *,
    schema: str,
    table: str,
    index_type: str = "NONCLUSTERED",
    key_columns: Iterable[IndexKeyColumn | str] = (),
    include_columns: Iterable[str] = (),
    filter_definition: str | None = None,
    is_unique: bool = False,
    is_primary_key: bool = False,
    is_unique_constraint: bool = False,
    constraint_name: str | None = None,
    constraint_type: str | None = None,
    is_disabled: bool = False,
    fill_factor: int = 0,
    partition_columns: Iterable[str] = (),
    data_space_name: str | None = None,
    data_space_type: str | None = None,
    partition_scheme_name: str | None = None,
    partition_function_name: str | None = None,
    partition_compression: Iterable[tuple[int, str]] = (),
) -> dict[str, Any]:
    parsed_keys = tuple(
        column if isinstance(column, IndexKeyColumn) else parse_candidate_key(column)
        for column in key_columns
    )
    return {
        "schema": _clean_identifier(schema),
        "table": _clean_identifier(table),
        "index_type": str(index_type).strip().upper(),
        "key_columns": [column.as_dict() for column in parsed_keys],
        "include_columns": [_clean_identifier(column) for column in include_columns],
        "filter_definition": normalize_index_definition(filter_definition),
        "is_unique": bool(is_unique),
        "is_primary_key": bool(is_primary_key),
        "is_unique_constraint": bool(is_unique_constraint),
        "constraint_name": _clean_identifier(constraint_name) if constraint_name else None,
        "constraint_type": str(constraint_type).strip().upper() if constraint_type else None,
        "is_disabled": bool(is_disabled),
        "fill_factor": int(fill_factor),
        "partition_columns": [_clean_identifier(column) for column in partition_columns],
        "data_space_name": _clean_identifier(data_space_name) if data_space_name else None,
        "data_space_type": str(data_space_type).strip().upper() if data_space_type else None,
        "partition_scheme_name": (
            _clean_identifier(partition_scheme_name) if partition_scheme_name else None
        ),
        "partition_function_name": (
            _clean_identifier(partition_function_name) if partition_function_name else None
        ),
        "partition_compression": [
            {"partition_number": int(number), "compression": normalized_compression}
            for number, compression in sorted(partition_compression)
            if (normalized_compression := str(compression).strip().upper()) != "NONE"
        ],
    }


def index_definition_fingerprint(**definition: Any) -> str:
    payload = index_definition_payload(**definition)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def index_ownership_fingerprint(
    schema: str,
    table: str,
    index_name: str,
    definition_fingerprint: str,
) -> str:
    payload = {
        "schema": _clean_identifier(schema),
        "table": _clean_identifier(table),
        "index_name": _clean_identifier(index_name),
        "definition_fingerprint": str(definition_fingerprint),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def index_definition_matches(
    index: ExistingIndex,
    *,
    schema: str,
    table: str,
    key_columns: Iterable[IndexKeyColumn | str],
    include_columns: Iterable[str] = (),
    filter_definition: str | None = None,
    is_unique: bool = False,
    index_type: str = "NONCLUSTERED",
    data_space_name: str | None = None,
    partition_columns: Iterable[str] = (),
) -> bool:
    expected = index_definition_fingerprint(
        schema=schema,
        table=table,
        index_type=index_type,
        key_columns=key_columns,
        include_columns=include_columns,
        filter_definition=filter_definition,
        is_unique=is_unique,
        is_primary_key=False,
        is_unique_constraint=False,
        is_disabled=False,
        fill_factor=index.fill_factor,
        partition_columns=partition_columns,
        data_space_name=data_space_name,
        data_space_type=index.data_space_type,
        partition_scheme_name=index.partition_scheme_name,
        partition_function_name=index.partition_function_name,
        partition_compression=index.partition_compression,
    )
    return index.definition_fingerprint == expected


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "y"}
    return bool(value)


__all__ = [
    "EXISTING_INDEX_METADATA_SQL",
    "ExistingIndex",
    "IndexKeyColumn",
    "collect_existing_indexes",
    "coerce_existing_indexes",
    "existing_index_covers_candidate",
    "index_definition_fingerprint",
    "index_definition_matches",
    "index_definition_payload",
    "index_ownership_fingerprint",
    "normalize_index_definition",
    "parse_candidate_key",
    "parse_existing_index_rows",
]
