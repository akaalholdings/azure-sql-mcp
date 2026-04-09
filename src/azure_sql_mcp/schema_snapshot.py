from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Sequence

from .connection import AzureSqlExecutor

FUNCTION_TYPE_CODES = ("FN", "IF", "TF", "FS", "FT", "AF")


@dataclass(frozen=True)
class ColumnDef:
    name: str
    data_type: str
    max_length: int
    precision: int
    scale: int
    is_nullable: bool
    default_definition: str | None
    computed_definition: str | None = None
    is_persisted: bool = False


@dataclass(frozen=True)
class IndexDef:
    name: str
    index_type: str
    is_unique: bool
    is_primary_key: bool
    key_columns: tuple[str, ...]
    included_columns: tuple[str, ...]
    filter_definition: str | None = None
    data_compression: str = "NONE"


@dataclass(frozen=True)
class ConstraintDef:
    name: str
    constraint_type: str
    columns: tuple[str, ...]
    referenced_schema: str | None
    referenced_table: str | None
    referenced_columns: tuple[str, ...] | None
    definition: str | None
    delete_action: str | None = None
    update_action: str | None = None
    is_not_trusted: bool = False


@dataclass(frozen=True)
class SequenceDef:
    schema_name: str
    name: str
    data_type: str
    start_value: int
    increment: int
    min_value: int
    max_value: int
    is_cycling: bool
    current_value: int


@dataclass(frozen=True)
class TriggerDef:
    schema_name: str
    table_name: str
    name: str
    is_instead_of: bool
    is_disabled: bool
    events: str
    definition: str | None


@dataclass(frozen=True)
class TableSnapshot:
    schema_name: str
    table_name: str
    columns: tuple[ColumnDef, ...]
    indexes: tuple[IndexDef, ...]
    constraints: tuple[ConstraintDef, ...]
    triggers: tuple[TriggerDef, ...] = ()
    data_compression: str = "NONE"


@dataclass(frozen=True)
class ProgrammableObjectSnapshot:
    schema_name: str
    object_name: str
    object_type: str
    definition: str | None


@dataclass(frozen=True)
class SchemaSnapshot:
    database_name: str
    captured_at: str
    tables: dict[tuple[str, str], TableSnapshot]
    views: dict[tuple[str, str], ProgrammableObjectSnapshot]
    procedures: dict[tuple[str, str], ProgrammableObjectSnapshot]
    functions: dict[tuple[str, str], ProgrammableObjectSnapshot]
    sequences: dict[tuple[str, str], SequenceDef] | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "database_name": self.database_name,
            "captured_at": self.captured_at,
            "tables": {
                f"{schema}.{table}": _table_to_dict(snapshot)
                for (schema, table), snapshot in sorted(self.tables.items())
            },
            "views": {
                f"{schema}.{name}": _programmable_to_dict(snapshot)
                for (schema, name), snapshot in sorted(self.views.items())
            },
            "procedures": {
                f"{schema}.{name}": _programmable_to_dict(snapshot)
                for (schema, name), snapshot in sorted(self.procedures.items())
            },
            "functions": {
                f"{schema}.{name}": _programmable_to_dict(snapshot)
                for (schema, name), snapshot in sorted(self.functions.items())
            },
        }
        if self.sequences:
            result["sequences"] = {
                f"{schema}.{name}": asdict(seq)
                for (schema, name), seq in sorted(self.sequences.items())
            }
        return result


def _table_to_dict(snapshot: TableSnapshot) -> dict[str, Any]:
    return asdict(snapshot)


def _programmable_to_dict(
    snapshot: ProgrammableObjectSnapshot,
) -> dict[str, Any]:
    return asdict(snapshot)


def _schema_scope_clause(schema_names: Sequence[str]) -> tuple[str, list[Any]]:
    if not schema_names:
        return "1 = 0", []
    placeholders = ", ".join("?" for _ in schema_names)
    return f"s.name IN ({placeholders})", list(schema_names)


def _normalize_schema_names(schema_filter: Sequence[str] | None) -> list[str]:
    if schema_filter is None:
        return []
    return sorted({schema.strip() for schema in schema_filter if schema and schema.strip()})


async def capture_snapshot(
    executor: AzureSqlExecutor,
    database_name: str,
    schema_filter: Sequence[str] | None = None,
) -> SchemaSnapshot:
    schema_names = await _fetch_schema_names(executor, database_name, schema_filter)
    captured_at = datetime.now(timezone.utc).isoformat()
    if not schema_names:
        return SchemaSnapshot(
            database_name=database_name,
            captured_at=captured_at,
            tables={},
            views={},
            procedures={},
            functions={},
        )

    (
        columns_rows, index_rows, constraint_rows,
        object_rows, sequence_rows, trigger_rows,
    ) = await _fetch_snapshot_rows(
        executor,
        database_name,
        schema_names,
    )

    tables = _build_tables(columns_rows, index_rows, constraint_rows, trigger_rows)
    views, procedures, functions = _build_programmable_objects(object_rows)
    sequences = _build_sequences(sequence_rows)

    return SchemaSnapshot(
        database_name=database_name,
        captured_at=captured_at,
        tables=tables,
        views=views,
        procedures=procedures,
        functions=functions,
        sequences=sequences or None,
    )


async def _fetch_schema_names(
    executor: AzureSqlExecutor,
    database_name: str,
    schema_filter: Sequence[str] | None,
) -> list[str]:
    schema_names = _normalize_schema_names(schema_filter)
    query = """
    SELECT s.name AS schema_name
    FROM sys.schemas AS s
    WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
    """
    params: list[Any] = []
    if schema_names:
        clause, clause_params = _schema_scope_clause(schema_names)
        query += f"\n      AND {clause}"
        params.extend(clause_params)
    query += "\n    ORDER BY s.name"

    rows = await executor.fetch_all(database_name, query, params=params or None)
    if schema_names:
        allowed = set(schema_names)
        return [row["schema_name"] for row in rows if row["schema_name"] in allowed]
    return [row["schema_name"] for row in rows]


async def _fetch_snapshot_rows(
    executor: AzureSqlExecutor,
    database_name: str,
    schema_names: Sequence[str],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    clause, params = _schema_scope_clause(schema_names)
    constraint_params = params * 4

    columns_query = f"""
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        c.name AS column_name,
        ty.name AS data_type,
        c.max_length,
        c.precision,
        c.scale,
        c.is_nullable,
        dc.definition AS default_definition,
        cc.definition AS computed_definition,
        CAST(ISNULL(cc.is_persisted, 0) AS BIT) AS is_persisted
    FROM sys.tables AS t
    INNER JOIN sys.schemas AS s
        ON t.schema_id = s.schema_id
    INNER JOIN sys.columns AS c
        ON t.object_id = c.object_id
    INNER JOIN sys.types AS ty
        ON c.user_type_id = ty.user_type_id
    LEFT JOIN sys.default_constraints AS dc
        ON c.default_object_id = dc.object_id
    LEFT JOIN sys.computed_columns AS cc
        ON c.object_id = cc.object_id
       AND c.column_id = cc.column_id
    WHERE {clause}
    ORDER BY s.name, t.name, c.column_id
    """

    indexes_query = f"""
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        i.name AS index_name,
        i.type_desc AS index_type,
        i.is_unique,
        i.is_primary_key,
        ic.key_ordinal,
        ic.is_included_column,
        ic.index_column_id AS index_column_ordinal,
        c.name AS column_name,
        i.filter_definition,
        p.data_compression_desc AS data_compression
    FROM sys.indexes AS i
    INNER JOIN sys.tables AS t
        ON i.object_id = t.object_id
    INNER JOIN sys.schemas AS s
        ON t.schema_id = s.schema_id
    INNER JOIN sys.index_columns AS ic
        ON i.object_id = ic.object_id
       AND i.index_id = ic.index_id
    INNER JOIN sys.columns AS c
        ON ic.object_id = c.object_id
       AND ic.column_id = c.column_id
    LEFT JOIN sys.partitions AS p
        ON i.object_id = p.object_id
       AND i.index_id = p.index_id
       AND p.partition_number = 1
    WHERE {clause}
      AND i.name IS NOT NULL
      AND i.is_primary_key = 0
      AND i.is_unique_constraint = 0
    ORDER BY s.name, t.name, i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id
    """

    constraints_query = f"""
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        kc.name AS constraint_name,
        CASE kc.type
            WHEN 'PK' THEN 'PRIMARY_KEY'
            WHEN 'UQ' THEN 'UNIQUE'
        END AS constraint_type,
        ic.key_ordinal AS constraint_column_ordinal,
        c.name AS column_name,
        NULL AS referenced_schema,
        NULL AS referenced_table,
        NULL AS referenced_column,
        NULL AS definition,
        NULL AS delete_action,
        NULL AS update_action,
        CAST(0 AS BIT) AS is_not_trusted
    FROM sys.key_constraints AS kc
    INNER JOIN sys.tables AS t
        ON kc.parent_object_id = t.object_id
    INNER JOIN sys.schemas AS s
        ON t.schema_id = s.schema_id
    INNER JOIN sys.index_columns AS ic
        ON kc.parent_object_id = ic.object_id
       AND kc.unique_index_id = ic.index_id
    INNER JOIN sys.columns AS c
        ON ic.object_id = c.object_id
       AND ic.column_id = c.column_id
    WHERE {clause}

    UNION ALL

    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        fk.name AS constraint_name,
        'FOREIGN_KEY' AS constraint_type,
        fkc.constraint_column_id AS constraint_column_ordinal,
        pc.name AS column_name,
        rs.name AS referenced_schema,
        rt.name AS referenced_table,
        rc.name AS referenced_column,
        NULL AS definition,
        fk.delete_referential_action_desc AS delete_action,
        fk.update_referential_action_desc AS update_action,
        fk.is_not_trusted
    FROM sys.foreign_keys AS fk
    INNER JOIN sys.tables AS t
        ON fk.parent_object_id = t.object_id
    INNER JOIN sys.schemas AS s
        ON t.schema_id = s.schema_id
    INNER JOIN sys.foreign_key_columns AS fkc
        ON fk.object_id = fkc.constraint_object_id
    INNER JOIN sys.columns AS pc
        ON fkc.parent_object_id = pc.object_id
       AND fkc.parent_column_id = pc.column_id
    INNER JOIN sys.tables AS rt
        ON fkc.referenced_object_id = rt.object_id
    INNER JOIN sys.schemas AS rs
        ON rt.schema_id = rs.schema_id
    INNER JOIN sys.columns AS rc
        ON fkc.referenced_object_id = rc.object_id
       AND fkc.referenced_column_id = rc.column_id
    WHERE {clause}

    UNION ALL

    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        cc.name AS constraint_name,
        'CHECK' AS constraint_type,
        1 AS constraint_column_ordinal,
        CASE WHEN cc.parent_column_id > 0 THEN col.name ELSE NULL END AS column_name,
        NULL AS referenced_schema,
        NULL AS referenced_table,
        NULL AS referenced_column,
        cc.definition AS definition,
        NULL AS delete_action,
        NULL AS update_action,
        cc.is_not_trusted
    FROM sys.check_constraints AS cc
    INNER JOIN sys.tables AS t
        ON cc.parent_object_id = t.object_id
    INNER JOIN sys.schemas AS s
        ON t.schema_id = s.schema_id
    LEFT JOIN sys.columns AS col
        ON cc.parent_object_id = col.object_id
       AND cc.parent_column_id = col.column_id
    WHERE {clause}

    UNION ALL

    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        dc.name AS constraint_name,
        'DEFAULT' AS constraint_type,
        1 AS constraint_column_ordinal,
        c.name AS column_name,
        NULL AS referenced_schema,
        NULL AS referenced_table,
        NULL AS referenced_column,
        dc.definition AS definition,
        NULL AS delete_action,
        NULL AS update_action,
        CAST(0 AS BIT) AS is_not_trusted
    FROM sys.default_constraints AS dc
    INNER JOIN sys.tables AS t
        ON dc.parent_object_id = t.object_id
    INNER JOIN sys.schemas AS s
        ON t.schema_id = s.schema_id
    INNER JOIN sys.columns AS c
        ON dc.parent_object_id = c.object_id
       AND dc.parent_column_id = c.column_id
    WHERE {clause}
    ORDER BY schema_name, table_name, constraint_name
    """

    objects_query = f"""
    SELECT
        s.name AS schema_name,
        o.name AS object_name,
        o.type AS object_type,
        m.definition AS definition
    FROM sys.objects AS o
    INNER JOIN sys.schemas AS s
        ON o.schema_id = s.schema_id
    LEFT JOIN sys.sql_modules AS m
        ON o.object_id = m.object_id
    WHERE {clause}
      AND o.type IN ('V', 'P', {", ".join(repr(code) for code in FUNCTION_TYPE_CODES)})
    ORDER BY s.name, o.name
    """

    sequences_query = f"""
    SELECT
        s.name AS schema_name,
        seq.name AS sequence_name,
        ty.name AS data_type,
        CAST(seq.start_value AS BIGINT) AS start_value,
        CAST(seq.increment AS BIGINT) AS increment,
        CAST(seq.minimum_value AS BIGINT) AS min_value,
        CAST(seq.maximum_value AS BIGINT) AS max_value,
        seq.is_cycling,
        CAST(seq.current_value AS BIGINT) AS current_value
    FROM sys.sequences AS seq
    INNER JOIN sys.schemas AS s
        ON seq.schema_id = s.schema_id
    INNER JOIN sys.types AS ty
        ON seq.user_type_id = ty.user_type_id
    WHERE {clause}
    ORDER BY s.name, seq.name
    """

    triggers_query = f"""
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        tr.name AS trigger_name,
        tr.is_instead_of_trigger AS is_instead_of,
        tr.is_disabled,
        STUFF((
            SELECT ', ' + te.type_desc
            FROM sys.trigger_events AS te
            WHERE te.object_id = tr.object_id
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS events,
        m.definition
    FROM sys.triggers AS tr
    INNER JOIN sys.tables AS t
        ON tr.parent_id = t.object_id
    INNER JOIN sys.schemas AS s
        ON t.schema_id = s.schema_id
    LEFT JOIN sys.sql_modules AS m
        ON tr.object_id = m.object_id
    WHERE {clause}
      AND tr.parent_class = 1
    ORDER BY s.name, t.name, tr.name
    """

    return await asyncio.gather(
        executor.fetch_all(database_name, columns_query, params=params),
        executor.fetch_all(database_name, indexes_query, params=params),
        executor.fetch_all(database_name, constraints_query, params=constraint_params),
        executor.fetch_all(database_name, objects_query, params=params),
        executor.fetch_all(database_name, sequences_query, params=params),
        executor.fetch_all(database_name, triggers_query, params=params),
    )


def _build_tables(
    columns_rows: Sequence[dict[str, Any]],
    index_rows: Sequence[dict[str, Any]],
    constraint_rows: Sequence[dict[str, Any]],
    trigger_rows: Sequence[dict[str, Any]] | None = None,
) -> dict[tuple[str, str], TableSnapshot]:
    table_columns: dict[tuple[str, str], list[ColumnDef]] = defaultdict(list)
    for row in columns_rows:
        key = (row["schema_name"], row["table_name"])
        table_columns[key].append(
            ColumnDef(
                name=row["column_name"],
                data_type=row["data_type"],
                max_length=int(row["max_length"]),
                precision=int(row["precision"]),
                scale=int(row["scale"]),
                is_nullable=bool(row["is_nullable"]),
                default_definition=row.get("default_definition"),
                computed_definition=row.get("computed_definition"),
                is_persisted=bool(row.get("is_persisted", False)),
            )
        )

    table_indexes: dict[tuple[str, str], list[IndexDef]] = defaultdict(list)
    index_groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in index_rows:
        table_key = (row["schema_name"], row["table_name"])
        index_key = (row["schema_name"], row["table_name"], row["index_name"])
        group = index_groups.setdefault(
            index_key,
            {
                "schema_name": row["schema_name"],
                "table_name": row["table_name"],
                "name": row["index_name"],
                "index_type": row["index_type"],
                "is_unique": bool(row["is_unique"]),
                "is_primary_key": bool(row["is_primary_key"]),
                "key_columns": [],
                "included_columns": [],
                "filter_definition": row.get("filter_definition"),
                "data_compression": row.get("data_compression", "NONE") or "NONE",
            },
        )
        column_name = row["column_name"]
        if row["is_included_column"]:
            group["included_columns"].append((int(row["index_column_ordinal"]), column_name))
        else:
            group["key_columns"].append((int(row["key_ordinal"]), column_name))

    for (schema_name, table_name, _), group in index_groups.items():
        table_key = (schema_name, table_name)
        key_columns = tuple(
            column_name
            for _, column_name in sorted(group["key_columns"], key=lambda item: item[0])
        )
        included_columns = tuple(
            column_name
            for _, column_name in sorted(group["included_columns"], key=lambda item: item[0])
        )
        table_indexes[table_key].append(
            IndexDef(
                name=group["name"],
                index_type=group["index_type"],
                is_unique=group["is_unique"],
                is_primary_key=group["is_primary_key"],
                key_columns=key_columns,
                included_columns=included_columns,
                filter_definition=group.get("filter_definition"),
                data_compression=group.get("data_compression", "NONE"),
            )
        )

    table_constraints: dict[tuple[str, str], list[ConstraintDef]] = defaultdict(list)
    constraint_groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in constraint_rows:
        table_key = (row["schema_name"], row["table_name"])
        constraint_key = (
            row["schema_name"],
            row["table_name"],
            row["constraint_name"],
        )
        group = constraint_groups.setdefault(
            constraint_key,
            {
                "name": row["constraint_name"],
                "constraint_type": row["constraint_type"],
                "columns": [],
                "referenced_schema": row.get("referenced_schema"),
                "referenced_table": row.get("referenced_table"),
                "referenced_columns": [],
                "definition": row.get("definition"),
                "delete_action": row.get("delete_action"),
                "update_action": row.get("update_action"),
                "is_not_trusted": bool(row.get("is_not_trusted", False)),
            },
        )
        column_name = row.get("column_name")
        if column_name:
            group["columns"].append(
                (int(row.get("constraint_column_ordinal") or 0), column_name)
            )
        if row.get("referenced_column"):
            group["referenced_columns"].append(
                (int(row.get("constraint_column_ordinal") or 0), row["referenced_column"])
            )

    for (schema_name, table_name, _), group in constraint_groups.items():
        table_key = (schema_name, table_name)
        referenced_columns = (
            tuple(
                column_name
                for _, column_name in sorted(group["referenced_columns"], key=lambda item: item[0])
            )
            or None
        )
        table_constraints[table_key].append(
            ConstraintDef(
                name=group["name"],
                constraint_type=group["constraint_type"],
                columns=tuple(
                    column_name
                    for _, column_name in sorted(group["columns"], key=lambda item: item[0])
                ),
                referenced_schema=group["referenced_schema"],
                referenced_table=group["referenced_table"],
                referenced_columns=referenced_columns,
                definition=group["definition"],
                delete_action=group.get("delete_action"),
                update_action=group.get("update_action"),
                is_not_trusted=group.get("is_not_trusted", False),
            )
        )

    table_triggers: dict[tuple[str, str], list[TriggerDef]] = defaultdict(list)
    for row in (trigger_rows or []):
        table_key = (row["schema_name"], row["table_name"])
        table_triggers[table_key].append(
            TriggerDef(
                schema_name=row["schema_name"],
                table_name=row["table_name"],
                name=row["trigger_name"],
                is_instead_of=bool(row.get("is_instead_of", False)),
                is_disabled=bool(row.get("is_disabled", False)),
                events=row.get("events") or "",
                definition=row.get("definition"),
            )
        )

    table_keys = (
        set(table_columns) | set(table_indexes)
        | set(table_constraints) | set(table_triggers)
    )
    tables: dict[tuple[str, str], TableSnapshot] = {}
    for key in sorted(table_keys):
        schema_name, table_name = key
        tables[key] = TableSnapshot(
            schema_name=schema_name,
            table_name=table_name,
            columns=tuple(table_columns.get(key, [])),
            indexes=tuple(table_indexes.get(key, [])),
            constraints=tuple(table_constraints.get(key, [])),
            triggers=tuple(table_triggers.get(key, [])),
        )
    return tables


def _build_sequences(
    sequence_rows: Sequence[dict[str, Any]],
) -> dict[tuple[str, str], SequenceDef]:
    sequences: dict[tuple[str, str], SequenceDef] = {}
    for row in sequence_rows:
        key = (row["schema_name"], row["sequence_name"])
        sequences[key] = SequenceDef(
            schema_name=row["schema_name"],
            name=row["sequence_name"],
            data_type=row["data_type"],
            start_value=int(row.get("start_value", 0)),
            increment=int(row.get("increment", 1)),
            min_value=int(row.get("min_value", 0)),
            max_value=int(row.get("max_value", 0)),
            is_cycling=bool(row.get("is_cycling", False)),
            current_value=int(row.get("current_value", 0)),
        )
    return sequences


def _build_programmable_objects(
    object_rows: Sequence[dict[str, Any]],
) -> tuple[
    dict[tuple[str, str], ProgrammableObjectSnapshot],
    dict[tuple[str, str], ProgrammableObjectSnapshot],
    dict[tuple[str, str], ProgrammableObjectSnapshot],
]:
    views: dict[tuple[str, str], ProgrammableObjectSnapshot] = {}
    procedures: dict[tuple[str, str], ProgrammableObjectSnapshot] = {}
    functions: dict[tuple[str, str], ProgrammableObjectSnapshot] = {}

    for row in object_rows:
        object_type = str(row["object_type"]).strip()
        schema_name = row["schema_name"]
        object_name = row["object_name"]
        snapshot = ProgrammableObjectSnapshot(
            schema_name=schema_name,
            object_name=object_name,
            object_type=_normalize_object_type(object_type),
            definition=row.get("definition"),
        )
        key = (schema_name, object_name)
        if object_type == "V":
            views[key] = snapshot
        elif object_type == "P":
            procedures[key] = snapshot
        else:
            functions[key] = snapshot

    return views, procedures, functions


def _normalize_object_type(object_type: str) -> str:
    object_type = object_type.strip()
    if object_type == "V":
        return "VIEW"
    if object_type == "P":
        return "PROCEDURE"
    return "FUNCTION"
