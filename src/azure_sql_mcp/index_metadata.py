"""Full-fidelity existing-index metadata and semantic coverage checks."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any, Iterable

from .connection import AzureSqlExecutor


EXISTING_INDEX_METADATA_SQL = """
SELECT
    o.object_id,
    o.type AS parent_object_type_code,
    o.type_desc AS parent_object_type,
    s.name AS schema_name,
    o.name AS table_name,
    i.index_id,
    i.name AS index_name,
    i.type AS index_type_code,
    i.type_desc AS index_type,
    i.is_unique,
    i.is_primary_key,
    i.is_unique_constraint,
    i.is_disabled,
    i.is_hypothetical,
    i.auto_created AS auto_created,
    i.has_filter,
    i.filter_definition,
    i.is_padded,
    i.ignore_dup_key,
    i.allow_row_locks,
    i.allow_page_locks,
    i.optimize_for_sequential_key,
    i.suppress_dup_key_messages,
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
    p.rows AS partition_row_count,
    pss.reserved_page_count AS partition_page_count,
    p.data_compression_desc,
    p.xml_compression_desc,
    st.no_recompute AS statistics_no_recompute,
    st.is_incremental AS statistics_incremental,
    us.user_seeks AS user_seeks,
    us.user_scans AS user_scans,
    us.user_lookups AS user_lookups,
    us.user_updates AS user_updates,
    CONVERT(varchar(33), SYSUTCDATETIME(), 127) AS collected_at_utc
FROM sys.indexes AS i
INNER JOIN sys.objects AS o
    ON i.object_id = o.object_id
INNER JOIN sys.schemas AS s
    ON o.schema_id = s.schema_id
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
LEFT JOIN sys.stats AS st
    ON st.object_id = i.object_id
    AND st.stats_id = i.index_id
LEFT JOIN sys.partitions AS p
    ON i.object_id = p.object_id
    AND i.index_id = p.index_id
LEFT JOIN sys.dm_db_partition_stats AS pss
    ON pss.object_id = p.object_id
    AND pss.index_id = p.index_id
    AND pss.partition_number = p.partition_number
LEFT JOIN sys.dm_db_index_usage_stats AS us
    ON i.object_id = us.object_id
    AND i.index_id = us.index_id
    AND us.database_id = DB_ID()
WHERE i.index_id > 0
  AND o.type IN ('U', 'V')
ORDER BY
    s.name,
    o.name,
    i.index_id,
    ic.key_ordinal,
    ic.index_column_id,
    p.partition_number
"""

ENGINE_START_TIME_SQL = """
SELECT
    CONVERT(varchar(33), sqlserver_start_time, 127) AS engine_start_time_utc,
    CONVERT(
        varchar(128),
        COALESCE(SERVERPROPERTY('ServerName'), @@SERVERNAME, 'azure-sql-database')
    ) AS engine_identity
FROM sys.dm_os_sys_info
"""

DATABASE_INCARNATION_SQL = """
SELECT CONVERT(varchar(128), physical_database_name) AS database_incarnation_identity
FROM sys.databases
WHERE name = DB_NAME()
"""

INDEX_PROTECTION_METADATA_SQL = """
SELECT
    TOP (10000)
    i.object_id,
    i.index_id,
    i.type AS index_type_code,
    i.type_desc AS index_type,
    i.is_unique,
    i.is_primary_key,
    i.is_unique_constraint,
    i.is_disabled,
    i.is_hypothetical,
    i.auto_created AS auto_created,
    CASE WHEN ds.type = 'PS' THEN 1 ELSE 0 END AS partition_switch_dependency,
    o.type AS parent_object_type_code,
    CASE WHEN o.type = 'V' THEN 1 ELSE 0 END AS is_indexed_view,
    fk.object_id AS child_foreign_key_id,
    fk.parent_object_id AS child_object_id,
    fk.referenced_object_id AS referenced_object_id,
    fk.key_index_id AS referenced_key_index_id,
    fkc.constraint_column_id AS child_constraint_column_ordinal,
    fkc.parent_column_id AS child_column_id,
    fkc.referenced_column_id AS referenced_column_id,
    ic.key_ordinal AS child_index_key_ordinal,
    CASE WHEN ep.major_id IS NULL THEN 0 ELSE 1 END AS has_index_extended_properties
FROM sys.indexes AS i
INNER JOIN sys.objects AS o
    ON o.object_id = i.object_id
LEFT JOIN sys.data_spaces AS ds
    ON ds.data_space_id = i.data_space_id
LEFT JOIN sys.foreign_keys AS fk
    ON fk.parent_object_id = i.object_id
    OR fk.referenced_object_id = i.object_id
LEFT JOIN sys.foreign_key_columns AS fkc
    ON fkc.constraint_object_id = fk.object_id
LEFT JOIN sys.index_columns AS ic
    ON ic.object_id = fkc.parent_object_id
    AND ic.index_id = i.index_id
    AND ic.column_id = fkc.parent_column_id
LEFT JOIN sys.extended_properties AS ep
    ON ep.class = 7
    AND ep.major_id = i.object_id
    AND ep.minor_id = i.index_id
WHERE i.index_id > 0
  AND o.type IN ('U', 'V')
ORDER BY i.object_id, i.index_id, fk.object_id,
    fkc.constraint_column_id
"""

_DIRECTION_PATTERN = re.compile(
    r"^(?:\[(?P<bracketed>(?:[^\]\r\n]|\]\])+)\]|"
    r"(?P<plain>[A-Za-z_][A-Za-z0-9_]*))"
    r"(?:\s+(?P<direction>ASC|DESC))?$",
    re.IGNORECASE,
)

_SPECIALIST_INDEX_TYPES = {
    3: "XML",
    4: "SPATIAL",
    5: "CLUSTERED COLUMNSTORE",
    6: "NONCLUSTERED COLUMNSTORE",
    7: "NONCLUSTERED HASH",
    9: "JSON",
}

_REVERSIBLE_DEFINITION_VERSION = 1
_REVERSIBLE_DEFINITION_FINGERPRINT_VERSION = "reversible_definition_fingerprint_v1"


@dataclass(frozen=True)
class IndexKeyColumn:
    name: str
    direction: str = "ASC"

    def __post_init__(self) -> None:
        # Values on this type are catalog identifiers. Preserve their spelling
        # exactly; delimited user input is parsed before construction.
        name = str(self.name)
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
    is_unique: bool | None = False
    is_primary_key: bool | None = False
    is_unique_constraint: bool | None = False
    constraint_name: str | None = None
    constraint_type: str | None = None
    is_disabled: bool | None = False
    fill_factor: int = 0
    partition_columns: tuple[str, ...] = ()
    data_space_name: str | None = None
    data_space_type: str | None = None
    partition_scheme_name: str | None = None
    partition_function_name: str | None = None
    partition_compression: tuple[tuple[int, str], ...] = ()
    usage: dict[str, int | None] = field(default_factory=dict)
    usage_context: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)
    object_id: int | None = None
    parent_object_type: str | None = None
    parent_object_type_code: str | None = None
    index_type_code: int | None = None
    is_hypothetical: bool | None = None
    is_auto_created: bool | None = None
    has_filter: bool | None = None
    is_padded: bool | None = None
    ignore_dup_key: bool | None = None
    allow_row_locks: bool | None = None
    allow_page_locks: bool | None = None
    optimize_for_sequential_key: bool | None = None
    suppress_dup_key_messages: bool | None = None
    statistics_no_recompute: bool | None = None
    statistics_incremental: bool | None = None
    xml_compression: tuple[tuple[int, str], ...] = ()
    partition_row_counts: tuple[tuple[int, int | None], ...] = ()
    partition_page_counts: tuple[tuple[int, int | None], ...] = ()
    protection_evidence: dict[str, Any] = field(default_factory=dict)
    has_index_extended_properties: bool | None = None

    @property
    def definition_fingerprint(self) -> str:
        return self.reversible_definition_fingerprint_v1

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
    def reversible_definition(self) -> dict[str, Any]:
        """Return the complete, versioned definition consumed by reverse DDL."""

        return reversible_index_definition_payload(self)

    @property
    def reversible_definition_fingerprint_v1(self) -> str:
        return reversible_index_definition_fingerprint(self.reversible_definition)

    @property
    def reversibility_blockers(self) -> tuple[str, ...]:
        blockers: list[str] = []
        required = {
            "object_id": self.object_id,
            "parent_object_type_code": self.parent_object_type_code,
            "index_type_code": self.index_type_code,
            "is_unique": self.is_unique,
            "is_primary_key": self.is_primary_key,
            "is_unique_constraint": self.is_unique_constraint,
            "is_disabled": self.is_disabled,
            "is_hypothetical": self.is_hypothetical,
            "is_auto_created": self.is_auto_created,
            "has_filter": self.has_filter,
            "is_padded": self.is_padded,
            "ignore_dup_key": self.ignore_dup_key,
            "allow_row_locks": self.allow_row_locks,
            "allow_page_locks": self.allow_page_locks,
            "statistics_no_recompute": self.statistics_no_recompute,
            "statistics_incremental": self.statistics_incremental,
        }
        for field_name, value in required.items():
            if value is None:
                blockers.append(f"{field_name}_unavailable")
        if self.optimize_for_sequential_key is None:
            blockers.append("optimize_for_sequential_key_unavailable")
        if self.suppress_dup_key_messages is None:
            blockers.append("suppress_dup_key_messages_unavailable")
        if self.has_filter is True and self.filter_definition is None:
            blockers.append("filtered_predicate_unavailable")
        if self.has_filter is False and self.filter_definition is not None:
            blockers.append("filter_metadata_inconsistent")
        if not self.name:
            blockers.append("index_name_unavailable")
        if not self.schema or not self.table:
            blockers.append("parent_identity_unavailable")
        if not self.key_columns:
            blockers.append("key_columns_unavailable")
        if self.data_space_type is None or self.data_space_name is None:
            blockers.append("data_space_unavailable")
        if (self.data_space_type or "").upper() == "PARTITION_SCHEME":
            if not self.partition_scheme_name or not self.partition_columns:
                blockers.append("partition_placement_unavailable")
        if (
            (self.data_space_type or "").upper()
            in {"PARTITION_SCHEME", "PARTITION SCHEME"}
            and not self.partition_compression
        ):
            blockers.append("partition_compression_unavailable")
        return tuple(dict.fromkeys(blockers))

    @property
    def is_reversible(self) -> bool:
        return not self.reversibility_blockers

    @property
    def is_unused(self) -> bool | None:
        read_counters = [
            self.usage.get(metric)
            for metric in ("user_seeks", "user_scans", "user_lookups")
        ]
        if any(
            value is not None and int(value) > 0
            for value in read_counters
        ):
            return False
        if any(value is None for value in read_counters):
            return None
        if self.usage_context.get("availability") != "available":
            return None
        if self.usage_context.get("coverage") != "covered":
            return None
        return True

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
            "usage_context": dict(self.usage_context),
            "provenance": dict(self.provenance),
            "object_id": self.object_id,
            "parent_object_type": self.parent_object_type,
            "parent_object_type_code": self.parent_object_type_code,
            "index_type_code": self.index_type_code,
            "is_hypothetical": self.is_hypothetical,
            "is_auto_created": self.is_auto_created,
            "has_filter": self.has_filter,
            "is_padded": self.is_padded,
            "ignore_dup_key": self.ignore_dup_key,
            "allow_row_locks": self.allow_row_locks,
            "allow_page_locks": self.allow_page_locks,
            "optimize_for_sequential_key": self.optimize_for_sequential_key,
            "suppress_dup_key_messages": self.suppress_dup_key_messages,
            "statistics_no_recompute": self.statistics_no_recompute,
            "statistics_incremental": self.statistics_incremental,
            "xml_compression": [
                {"partition_number": number, "compression": compression}
                for number, compression in self.xml_compression
            ],
            "partition_row_counts": [
                {"partition_number": number, "rows": count}
                for number, count in self.partition_row_counts
            ],
            "partition_page_counts": [
                {"partition_number": number, "pages": count}
                for number, count in self.partition_page_counts
            ],
            "protection_evidence": dict(self.protection_evidence),
            "has_index_extended_properties": self.has_index_extended_properties,
            "reversible_definition": self.reversible_definition,
            "reversible_definition_fingerprint_v1": self.reversible_definition_fingerprint_v1,
            "reversibility_blockers": list(self.reversibility_blockers),
            "is_reversible": self.is_reversible,
            "definition_fingerprint": self.definition_fingerprint,
            "fingerprint": self.fingerprint,
            "ownership": self.ownership,
            "is_unused": self.is_unused,
        }


async def collect_existing_indexes(
    executor: AzureSqlExecutor,
    database_name: str,
    *,
    observation_window_minutes: int | None = None,
) -> list[ExistingIndex]:
    rows = await executor.fetch_all(database_name, EXISTING_INDEX_METADATA_SQL)
    usage_context = await _collect_usage_context(
        executor,
        database_name,
        observation_window_minutes=observation_window_minutes,
    )
    protection_evidence = await _collect_protection_evidence(
        executor,
        database_name,
    )
    return parse_existing_index_rows(
        rows,
        observation_window_minutes=observation_window_minutes,
        usage_context=usage_context,
        protection_evidence=protection_evidence,
    )


def parse_existing_index_rows(
    rows: Iterable[dict[str, Any]],
    *,
    observation_window_minutes: int | None = None,
    usage_context: dict[str, Any] | None = None,
    protection_evidence: dict[tuple[int, int], dict[str, Any]] | None = None,
) -> list[ExistingIndex]:
    rows = list(rows)
    observed_at_utc = next(
        (
            row.get("collected_at_utc")
            for row in rows
            if row.get("collected_at_utc") is not None
        ),
        None,
    )
    resolved_usage_context = _resolve_usage_context(
        usage_context or {},
        observed_at_utc=observed_at_utc,
        observation_window_minutes=observation_window_minutes,
    )
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
                "xml_partitions": set(),
                "partition_rows": {},
                "partition_pages": {},
                "partition_columns": {},
            },
        )
        column_name = str(row.get("column_name") or "")
        index_column_id = _as_int(row.get("index_column_id"))
        key_ordinal = _as_int(row.get("key_ordinal"))
        if column_name and _as_bool(row.get("is_included_column")):
            item["includes"][index_column_id] = column_name
        elif column_name and key_ordinal > 0:
            item["keys"][key_ordinal] = IndexKeyColumn(
                column_name,
                "DESC" if _as_bool(row.get("is_descending_key")) else "ASC",
            )
        elif column_name:
            item["nonkeys"][index_column_id] = column_name
        partition_ordinal = _as_int(row.get("partition_ordinal"))
        if column_name and partition_ordinal > 0:
            item["partition_columns"][partition_ordinal] = column_name
        partition_number = _as_int(row.get("partition_number"))
        compression = row.get("data_compression_desc")
        if partition_number > 0 and compression:
            item["partitions"].add((partition_number, str(compression)))
        xml_compression = row.get("xml_compression_desc")
        if partition_number > 0 and xml_compression:
            item["xml_partitions"].add((partition_number, str(xml_compression)))
        if partition_number > 0:
            item["partition_rows"][partition_number] = _as_optional_int(
                row.get("partition_row_count")
            )
            item["partition_pages"][partition_number] = _as_optional_int(
                row.get("partition_page_count")
            )

    indexes: list[ExistingIndex] = []
    for (schema, table, index_id), item in grouped.items():
        row = item["row"]
        keys = tuple(item["keys"][ordinal] for ordinal in sorted(item["keys"]))
        protection_payload = _index_protection_payload(
            row,
            (protection_evidence or {}).get(
                (
                    _as_optional_int(row.get("object_id")) or 0,
                    index_id,
                ),
                (protection_evidence or {}).get(
                    (0, 0),
                    {
                        "coverage": "unavailable",
                        "blockers": ["protection_metadata_unavailable"],
                    },
                ),
            ),
        )
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
                is_unique=_as_optional_bool(row.get("is_unique")),
                is_primary_key=_as_optional_bool(row.get("is_primary_key")),
                is_unique_constraint=_as_optional_bool(row.get("is_unique_constraint")),
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
                is_disabled=_as_optional_bool(row.get("is_disabled")),
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
                    "user_seeks": _as_optional_int(row.get("user_seeks")),
                    "user_scans": _as_optional_int(row.get("user_scans")),
                    "user_lookups": _as_optional_int(row.get("user_lookups")),
                    "user_updates": _as_optional_int(row.get("user_updates")),
                },
                usage_context=dict(resolved_usage_context),
                provenance={
                    "source": "sys.indexes/sys.index_columns/sys.partitions",
                    "collected_at_utc": row.get("collected_at_utc"),
                    "constraint_source": "sys.key_constraints",
                    "data_space_source": "sys.data_spaces/sys.partition_schemes",
                    "usage_context": dict(resolved_usage_context),
                },
                object_id=_as_optional_int(row.get("object_id")),
                parent_object_type=(
                    str(row["parent_object_type"])
                    if row.get("parent_object_type") is not None
                    else None
                ),
                parent_object_type_code=(
                    str(row["parent_object_type_code"])
                    if row.get("parent_object_type_code") is not None
                    else None
                ),
                index_type_code=_as_optional_int(row.get("index_type_code")),
                is_hypothetical=_as_optional_bool(row.get("is_hypothetical")),
                is_auto_created=_as_optional_bool(
                    row.get("auto_created")
                ),
                has_filter=_as_optional_bool(row.get("has_filter")),
                is_padded=_as_optional_bool(row.get("is_padded")),
                ignore_dup_key=_as_optional_bool(row.get("ignore_dup_key")),
                allow_row_locks=_as_optional_bool(row.get("allow_row_locks")),
                allow_page_locks=_as_optional_bool(row.get("allow_page_locks")),
                optimize_for_sequential_key=_as_optional_bool(
                    row.get("optimize_for_sequential_key")
                ),
                suppress_dup_key_messages=_as_optional_bool(
                    row.get("suppress_dup_key_messages")
                ),
                statistics_no_recompute=_as_optional_bool(
                    row.get("statistics_no_recompute")
                ),
                statistics_incremental=_as_optional_bool(
                    row.get("statistics_incremental")
                ),
                xml_compression=tuple(sorted(item["xml_partitions"])),
                partition_row_counts=tuple(sorted(item["partition_rows"].items())),
                partition_page_counts=tuple(sorted(item["partition_pages"].items())),
                protection_evidence=protection_payload,
                has_index_extended_properties=_as_optional_bool(
                    protection_payload.get("has_index_extended_properties")
                ),
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
    key_columns: Iterable[IndexKeyColumn | str],
    include_columns: Iterable[str] = (),
    filter_definition: str | None = None,
    is_unique: bool = False,
    data_space_name: str | None = None,
    partition_columns: Iterable[str] = (),
    exact_catalog_names: bool = False,
) -> bool:
    if index.is_disabled:
        return False
    normalize_identifier = (
        _exact_catalog_identifier if exact_catalog_names else _clean_identifier
    )
    if normalize_identifier(index.schema) != normalize_identifier(
        schema
    ) or normalize_identifier(index.table) != normalize_identifier(table):
        return False

    candidate_keys = tuple(
        column
        if isinstance(column, IndexKeyColumn)
        else IndexKeyColumn(_exact_catalog_identifier(column), "ASC")
        if exact_catalog_names
        else _candidate_key_column(column)
        for column in key_columns
    )
    if len(candidate_keys) > len(index.key_columns):
        return False
    for candidate, existing in zip(candidate_keys, index.key_columns, strict=False):
        if normalize_identifier(candidate.name) != normalize_identifier(existing.name):
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
        normalize_identifier(data_space_name)
        != normalize_identifier(index.data_space_name or "")
    ):
        return False
    required_partitions = tuple(
        normalize_identifier(column) for column in partition_columns
    )
    existing_partitions = tuple(
        normalize_identifier(column) for column in index.partition_columns
    )
    if required_partitions and existing_partitions != required_partitions:
        return False

    available = {
        normalize_identifier(column.name) for column in index.key_columns
    } | {normalize_identifier(column) for column in index.include_columns}
    required = {
        normalize_identifier(column) for column in include_columns if column
    }
    return required.issubset(available)


def parse_candidate_key(value: str) -> IndexKeyColumn:
    match = _DIRECTION_PATTERN.fullmatch(str(value).strip())
    if match is None:
        raise ValueError(f"invalid candidate key column: {value!r}")
    return IndexKeyColumn(
        (match.group("bracketed") or match.group("plain") or "").replace(
            "]]", "]"
        ),
        (match.group("direction") or "ASC").upper(),
    )


def _candidate_key_column(value: IndexKeyColumn | str) -> IndexKeyColumn:
    if isinstance(value, IndexKeyColumn):
        return value
    try:
        return parse_candidate_key(value)
    except ValueError:
        # Missing-index sources return exact catalog names without a direction.
        # Keep the established simple/bracketed direction grammar, but never
        # infer a direction by splitting an otherwise unrestricted identifier.
        return IndexKeyColumn(_catalog_identifier(value), "ASC")


def _clean_identifier(value: str) -> str:
    return _catalog_identifier(value)


def _catalog_identifier(value: str) -> str:
    text = str(value).strip()
    if len(text) >= 2 and text[0] == "[":
        index = 1
        while index < len(text):
            if text[index] != "]":
                index += 1
                continue
            if index + 1 < len(text) and text[index + 1] == "]":
                index += 2
                continue
            if index == len(text) - 1:
                return text[1:-1].replace("]]", "]")
            break
    return text


def _exact_catalog_identifier(value: Any) -> str:
    return str(value)


def _specialist_index_type(
    index_type_code: int | None,
    index_type: Any,
) -> str | None:
    if index_type_code in {1, 2}:
        return None
    description = str(index_type or "").strip().upper()
    if description and description not in {"CLUSTERED", "NONCLUSTERED", "UNKNOWN"}:
        return description
    if index_type_code in _SPECIALIST_INDEX_TYPES:
        return _SPECIALIST_INDEX_TYPES[index_type_code]
    if index_type_code is not None:
        return f"INDEX TYPE {index_type_code}"
    return None


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
        "key_columns": [
            {
                "name": _clean_identifier(column.name),
                "direction": column.direction,
            }
            for column in parsed_keys
        ],
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


def reversible_index_definition_payload(index: ExistingIndex) -> dict[str, Any]:
    """Serialize the exact catalog definition used for reversible DDL.

    This intentionally has a separate schema from ``index_definition_payload``.
    The latter is a legacy lease token and must not change when a catalog option
    is added to the rollback contract.
    """

    return {
        "version": _REVERSIBLE_DEFINITION_VERSION,
        "object_id": _as_optional_int(index.object_id),
        "parent_object_type": _canonical_text(index.parent_object_type),
        "parent_object_type_code": _canonical_text(index.parent_object_type_code),
        "schema": _exact_catalog_identifier(index.schema),
        "table": _exact_catalog_identifier(index.table),
        "index_id": int(index.index_id),
        "index_name": _exact_catalog_identifier(index.name),
        "index_type": str(index.index_type).strip().upper(),
        "index_type_code": _as_optional_int(index.index_type_code),
        "is_primary_key": _canonical_optional_bool(index.is_primary_key),
        "is_unique_constraint": _canonical_optional_bool(index.is_unique_constraint),
        "constraint_name": _canonical_optional_identifier(index.constraint_name),
        "constraint_type": _canonical_optional_upper(index.constraint_type),
        "is_disabled": _canonical_optional_bool(index.is_disabled),
        "is_hypothetical": _canonical_optional_bool(index.is_hypothetical),
        "is_auto_created": _canonical_optional_bool(index.is_auto_created),
        "key_columns": [
            {
                "name": _exact_catalog_identifier(column.name),
                "direction": str(column.direction).strip().upper(),
            }
            for column in index.key_columns
        ],
        "include_columns": [
            _exact_catalog_identifier(column) for column in index.include_columns
        ],
        "nonkey_columns": [
            _exact_catalog_identifier(column) for column in index.nonkey_columns
        ],
        "filter": {
            "has_filter": _canonical_optional_bool(index.has_filter),
            "definition": index.filter_definition,
        },
        "is_unique": _canonical_optional_bool(index.is_unique),
        "is_padded": _canonical_optional_bool(index.is_padded),
        "fill_factor": int(index.fill_factor),
        "ignore_dup_key": _canonical_optional_bool(index.ignore_dup_key),
        "statistics_no_recompute": _canonical_optional_bool(
            index.statistics_no_recompute
        ),
        "statistics_incremental": _canonical_optional_bool(
            index.statistics_incremental
        ),
        "allow_row_locks": _canonical_optional_bool(index.allow_row_locks),
        "allow_page_locks": _canonical_optional_bool(index.allow_page_locks),
        "optimize_for_sequential_key": _canonical_optional_bool(
            index.optimize_for_sequential_key
        ),
        "suppress_dup_key_messages": _canonical_optional_bool(
            index.suppress_dup_key_messages
        ),
        "data_space": {
            "name": _canonical_optional_identifier(index.data_space_name),
            "type": _canonical_optional_upper(index.data_space_type),
            "partition_scheme": _canonical_optional_identifier(
                index.partition_scheme_name
            ),
            "partition_function": _canonical_optional_identifier(
                index.partition_function_name
            ),
            "partition_columns": [
                _exact_catalog_identifier(column) for column in index.partition_columns
            ],
        },
        "partition_compression": _partition_compression_payload(
            index.partition_compression
        ),
        "xml_compression": _partition_compression_payload(index.xml_compression),
    }


def reversible_index_definition_fingerprint(
    definition: ExistingIndex | Mapping[str, Any],
) -> str:
    """Hash an existing index or its persisted reversible definition payload."""

    payload = (
        reversible_index_definition_payload(definition)
        if isinstance(definition, ExistingIndex)
        else definition
    )
    encoded = json.dumps(
        {
            "version": _REVERSIBLE_DEFINITION_FINGERPRINT_VERSION,
            "definition": payload,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_text(value: str | None) -> str | None:
    return str(value).strip() if value is not None else None


def _canonical_optional_bool(value: bool | None) -> bool | None:
    return bool(value) if value is not None else None


def _canonical_optional_identifier(value: str | None) -> str | None:
    return _exact_catalog_identifier(value) if value is not None else None


def _canonical_optional_upper(value: str | None) -> str | None:
    return str(value).strip().upper() if value is not None else None


def _partition_compression_payload(
    values: Iterable[tuple[int, str]],
) -> list[dict[str, int | str]]:
    normalized = sorted(
        (int(number), str(compression).strip().upper())
        for number, compression in values
    )
    return [
        {"partition_number": number, "compression": compression}
        for number, compression in normalized
    ]


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


async def _collect_usage_context(
    executor: AzureSqlExecutor,
    database_name: str,
    *,
    observation_window_minutes: int | None,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "source": "sys.dm_db_index_usage_stats",
        "counter_epoch_source": "sys.dm_os_sys_info.sqlserver_start_time",
        "availability": "unavailable",
        "coverage": "unavailable",
    }
    if observation_window_minutes is not None:
        context["requested_window_minutes"] = observation_window_minutes
    rows = await executor.fetch_all(database_name, ENGINE_START_TIME_SQL)
    if not rows:
        return context
    engine_start_time = rows[0].get("engine_start_time_utc")
    if engine_start_time is None:
        return context
    context.update(
        {
            "availability": "available",
            "counter_epoch_utc": engine_start_time,
            "engine_start_time_utc": engine_start_time,
            "engine_identity": rows[0].get("engine_identity"),
        }
    )
    return context


async def _collect_protection_evidence(
    executor: AzureSqlExecutor,
    database_name: str,
) -> dict[tuple[int, int], dict[str, Any]]:
    """Collect relationship and catalog protection evidence fail-closed."""

    rows = await executor.fetch_all(database_name, INDEX_PROTECTION_METADATA_SQL)

    return parse_protection_evidence(rows)


def parse_protection_evidence(
    rows: Iterable[dict[str, Any]],
    *,
    max_rows: int = 10_000,
) -> dict[tuple[int, int], dict[str, Any]]:
    """Parse the fixed protection query without weakening failed coverage."""

    rows = list(rows)

    if len(rows) >= max_rows:
        return {
            (0, 0): {
                "coverage": "incomplete",
                "blockers": ["protection_metadata_cap_reached"],
            }
        }

    evidence: dict[tuple[int, int], dict[str, Any]] = {}
    for raw_row in rows:
        row = dict(raw_row)
        object_id = _as_optional_int(row.get("object_id"))
        index_id = _as_optional_int(row.get("index_id"))
        if object_id is None or index_id is None or index_id <= 0:
            return {
                (0, 0): {
                    "coverage": "incomplete",
                    "blockers": ["malformed_protection_metadata"],
                }
            }
        key = (object_id, index_id)
        item = evidence.setdefault(
            key,
            {
                "object_id": object_id,
                "index_id": index_id,
                "coverage": "complete",
                "referenced_foreign_key_key_index_ids": set(),
                "child_foreign_key_support": {},
                "has_index_extended_properties": False,
                "partition_switch_dependency": _as_optional_bool(
                    row.get("partition_switch_dependency")
                ),
            },
        )
        item["primary_key"] = _as_bool(row.get("is_primary_key"))
        item["unique_constraint"] = _as_bool(row.get("is_unique_constraint"))
        item["standalone_unique"] = _as_bool(row.get("is_unique")) and not (
            _as_bool(row.get("is_primary_key"))
            or _as_bool(row.get("is_unique_constraint"))
        )
        item["clustered"] = _as_optional_int(row.get("index_type_code")) in {1, 5}
        item["indexed_view"] = _as_bool(row.get("is_indexed_view"))
        item["disabled"] = _as_bool(row.get("is_disabled"))
        item["hypothetical"] = _as_bool(row.get("is_hypothetical"))
        item["auto_created"] = _as_bool(
            row.get("auto_created")
        )
        specialist_type = _specialist_index_type(
            _as_optional_int(row.get("index_type_code")),
            row.get("index_type"),
        )
        if specialist_type:
            item["specialist_type"] = specialist_type
        else:
            item.setdefault("specialist_type", None)
        partition_switch_dependency = _as_optional_bool(
            row.get("partition_switch_dependency")
        )
        if partition_switch_dependency is None:
            data_space_type = str(
                row.get("data_space_type") or row.get("data_space_type_desc") or ""
            ).upper()
            if data_space_type:
                partition_switch_dependency = data_space_type in {
                    "PS",
                    "PARTITION_SCHEME",
                    "PARTITION SCHEME",
                }
            else:
                item["coverage"] = "incomplete"
                item.setdefault("blockers", []).append(
                    "partition_switch_dependency_unavailable"
                )
        if partition_switch_dependency is True or item["partition_switch_dependency"] is None:
            item["partition_switch_dependency"] = partition_switch_dependency
        item["has_index_extended_properties"] = item[
            "has_index_extended_properties"
        ] or _as_bool(row.get("has_index_extended_properties"))
        item["extended_properties"] = item["has_index_extended_properties"]

        foreign_key_id = _as_optional_int(row.get("child_foreign_key_id"))
        if foreign_key_id is None:
            continue
        child_object_id = _as_optional_int(row.get("child_object_id"))
        referenced_object_id = _as_optional_int(row.get("referenced_object_id"))
        referenced_key_index_id = _as_optional_int(row.get("referenced_key_index_id"))
        if (
            referenced_object_id == object_id
            and referenced_key_index_id == index_id
        ):
            item["referenced_foreign_key_key_index_ids"].add(foreign_key_id)
        if child_object_id != object_id:
            continue
        child = item["child_foreign_key_support"].setdefault(
            foreign_key_id,
            {
                "foreign_key_id": foreign_key_id,
                "child_object_id": child_object_id,
                "constraint_ordinals": [],
                "index_key_ordinals": [],
            },
        )
        constraint_ordinal = _as_optional_int(
            row.get("child_constraint_column_ordinal")
        )
        index_ordinal = _as_optional_int(row.get("child_index_key_ordinal"))
        if constraint_ordinal is None:
            child["metadata_incomplete"] = True
            item["coverage"] = "incomplete"
            item.setdefault("blockers", []).append(
                "child_foreign_key_metadata_incomplete"
            )
        else:
            child["constraint_ordinals"].append(constraint_ordinal)
        if index_ordinal is not None:
            child["index_key_ordinals"].append(index_ordinal)

    for item in evidence.values():
        child_support: list[dict[str, Any]] = []
        for support in item["child_foreign_key_support"].values():
            constraint_ordinals = support.pop("constraint_ordinals")
            index_ordinals = support.pop("index_key_ordinals")
            support["leading_key_supported"] = bool(
                not support.pop("metadata_incomplete", False)
                and constraint_ordinals
                and sorted(constraint_ordinals) == list(
                    range(1, len(constraint_ordinals) + 1)
                )
                and index_ordinals == constraint_ordinals
            )
            support["constraint_ordinals"] = constraint_ordinals
            support["index_key_ordinals"] = index_ordinals
            if support["leading_key_supported"]:
                child_support.append(support)
        item["child_foreign_key_support"] = sorted(
            child_support, key=lambda value: value["foreign_key_id"]
        )
        item["referenced_foreign_key_key_index_ids"] = sorted(
            item["referenced_foreign_key_key_index_ids"]
        )
        item["referenced_foreign_keys"] = [
            {
                "foreign_key_id": foreign_key_id,
                "key_index_id": item.get("index_id"),
            }
            for foreign_key_id in item["referenced_foreign_key_key_index_ids"]
        ]
        item["extended_properties"] = item["has_index_extended_properties"]
        item["automatic_tuning"] = item.get("auto_created")
        item["hinted_or_forced_plan"] = None
        item.setdefault("specialist_type", None)
        item["safe_to_remove"] = item.get("coverage") == "complete"
    return evidence


def _resolve_usage_context(
    usage_context: dict[str, Any],
    *,
    observed_at_utc: Any,
    observation_window_minutes: int | None,
) -> dict[str, Any]:
    context = dict(usage_context)
    requested_window = (
        observation_window_minutes
        if observation_window_minutes is not None
        else context.get("requested_window_minutes")
    )
    if requested_window is not None:
        context["requested_window_minutes"] = requested_window
    if context.get("availability") != "available":
        context.setdefault("coverage", "unavailable")
        return context
    if context.get("coverage") in {"covered", "partial"} and context.get(
        "coverage_window_evaluated"
    ) is True:
        return context

    engine_start = _parse_utc_timestamp(
        context.get("counter_epoch_utc") or context.get("engine_start_time_utc")
    )
    observed_at = _parse_utc_timestamp(observed_at_utc)
    if engine_start is None or observed_at is None:
        context["coverage"] = "unknown"
        return context
    if requested_window is None:
        context["coverage"] = "covered"
        return context
    try:
        requested_minutes = int(requested_window)
    except (TypeError, ValueError):
        context["coverage"] = "unknown"
        return context
    observation_start = observed_at - timedelta(minutes=max(requested_minutes, 0))
    context["coverage"] = (
        "covered" if engine_start <= observation_start else "partial"
    )
    context["coverage_window_evaluated"] = True
    context["observed_at_utc"] = observed_at_utc
    return context


def _parse_utc_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        timestamp = value
    elif value is None:
        return None
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            timestamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _as_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"1", "true", "yes", "y"}:
            return True
        if normalized in {"0", "false", "no", "n"}:
            return False
        return None
    if isinstance(value, (bool, int)):
        return bool(value)
    return None


def _index_protection_payload(
    row: dict[str, Any],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(evidence)
    if payload.get("coverage") == "covered":
        payload["coverage"] = "complete"
    is_unique = _as_bool(row.get("is_unique"))
    is_primary_key = _as_bool(row.get("is_primary_key"))
    is_unique_constraint = _as_bool(row.get("is_unique_constraint"))
    index_type_code = _as_optional_int(row.get("index_type_code"))
    parent_type = str(
        row.get("parent_object_type_code")
        or row.get("parent_object_type")
        or ""
    ).upper()
    payload.setdefault("primary_key", is_primary_key)
    payload.setdefault("unique_constraint", is_unique_constraint)
    payload.setdefault(
        "standalone_unique",
        is_unique and not is_primary_key and not is_unique_constraint,
    )
    payload.setdefault("clustered", index_type_code in {1, 5})
    payload.setdefault("indexed_view", parent_type in {"V", "VIEW"})
    payload.setdefault("disabled", _as_bool(row.get("is_disabled")))
    payload.setdefault("hypothetical", _as_bool(row.get("is_hypothetical")))
    payload.setdefault(
        "auto_created",
        _as_bool(row.get("auto_created")),
    )
    specialist_type = _specialist_index_type(
        index_type_code,
        row.get("index_type"),
    )
    if specialist_type and not payload.get("specialist_type"):
        payload["specialist_type"] = specialist_type
    else:
        payload.setdefault("specialist_type", None)
    payload.setdefault("has_index_extended_properties", False)
    payload.setdefault(
        "extended_properties", payload["has_index_extended_properties"]
    )
    payload.setdefault("referenced_foreign_key_key_index_ids", [])
    payload.setdefault("referenced_foreign_keys", [])
    payload["automatic_tuning"] = payload.get("auto_created")
    payload.setdefault("hinted_or_forced_plan", None)
    if "partition_switch_dependency" not in payload:
        data_space_type = str(
            row.get("data_space_type") or row.get("data_space_type_desc") or ""
        ).upper()
        payload["partition_switch_dependency"] = (
            data_space_type in {"PS", "PARTITION_SCHEME", "PARTITION SCHEME"}
            if data_space_type
            else None
        )
    payload["safe_to_remove"] = payload.get("coverage") == "complete"
    if payload.get("coverage") != "complete":
        payload.setdefault("blockers", ["protection_metadata_unavailable"])
    return payload


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "y"}
    return bool(value)


__all__ = [
    "DATABASE_INCARNATION_SQL",
    "ENGINE_START_TIME_SQL",
    "EXISTING_INDEX_METADATA_SQL",
    "INDEX_PROTECTION_METADATA_SQL",
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
    "parse_protection_evidence",
    "reversible_index_definition_payload",
    "reversible_index_definition_fingerprint",
]
