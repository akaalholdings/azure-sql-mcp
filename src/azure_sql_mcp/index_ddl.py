"""Exact, non-executing reverse DDL for fully observed user indexes."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from .index_metadata import ExistingIndex
from .index_metadata import IndexKeyColumn


_REQUIRED_SET_OPTIONS = (
    "SET ANSI_NULLS ON;",
    "SET QUOTED_IDENTIFIER ON;",
    "SET ANSI_PADDING ON;",
    "SET ANSI_WARNINGS ON;",
    "SET ARITHABORT ON;",
    "SET CONCAT_NULL_YIELDS_NULL ON;",
    "SET NUMERIC_ROUNDABORT OFF;",
)


def quote_identifier(identifier: str) -> str:
    """Quote one SQL Server identifier without changing its legal contents."""

    if not isinstance(identifier, str) or identifier == "":
        raise ValueError("identifier must not be empty")
    return f"[{identifier.replace(']', ']]')}]"


def render_reverse_index_ddl(index: ExistingIndex) -> dict[str, Any]:
    """Render executable recreation SQL only for a complete safe definition."""

    blockers = list(index.reversibility_blockers)
    if not index.name:
        blockers.append("index_name_unavailable")
    if not index.schema or not index.table:
        blockers.append("parent_identity_unavailable")
    if not index.key_columns:
        blockers.append("key_columns_unavailable")
    if not index.data_space_name:
        blockers.append("data_space_unavailable")
    if index.parent_object_type_code not in {None, "U"}:
        blockers.append("parent_is_not_user_table")
    if index.parent_object_type_code is None and index.parent_object_type not in {
        None,
        "USER_TABLE",
        "TABLE",
    }:
        blockers.append("parent_object_type_unsupported")
    if index.index_type_code != 2:
        blockers.append("index_type_is_not_nonclustered_rowstore")
    if index.index_type.upper() not in {"NONCLUSTERED", "NONCLUSTERED INDEX"}:
        blockers.append("index_type_is_not_nonclustered_rowstore")
    if index.is_disabled:
        blockers.append("index_is_disabled")
    if index.is_hypothetical:
        blockers.append("index_is_hypothetical")
    if index.is_auto_created:
        blockers.append("index_is_auto_created")
    if index.suppress_dup_key_messages:
        blockers.append("suppress_dup_key_messages_unsupported")
    if index.is_primary_key:
        blockers.append("index_is_primary_key")
    if index.is_unique_constraint:
        blockers.append("index_is_unique_constraint")
    if index.constraint_name is not None or index.constraint_type is not None:
        blockers.append("index_constraint_identity_present")
    if index.fill_factor < 0 or index.fill_factor > 100:
        blockers.append("fill_factor_invalid")

    if index.data_space_type is not None:
        data_space_type = index.data_space_type.upper()
        if data_space_type in {"PARTITION_SCHEME", "PARTITION SCHEME"}:
            if len(index.partition_columns) != 1:
                blockers.append("partition_column_count_not_one")
            if not index.partition_scheme_name:
                blockers.append("partition_scheme_name_unavailable")
            if not index.partition_compression:
                blockers.append("partition_compression_unavailable")
        elif data_space_type not in {"FILEGROUP", "ROWS", "FG"}:
            blockers.append("data_space_type_unsupported")

    if index.has_filter is True and not index.filter_definition:
        blockers.append("filtered_predicate_unavailable")
    if index.has_filter is False and index.filter_definition is not None:
        blockers.append("filter_metadata_inconsistent")
    blockers = list(dict.fromkeys(blockers))
    base = {
        "executable": not blockers,
        "ddl": None,
        "reverse_ddl": None,
        "drop_ddl": None,
        "blockers": blockers,
        "index_identity": {
            "object_id": index.object_id,
            "schema": index.schema,
            "table": index.table,
            "index_id": index.index_id,
            "index_name": index.name,
        },
        "reversible_definition_fingerprint_v1": index.reversible_definition_fingerprint_v1,
    }
    if blockers:
        return base

    key_sql = ", ".join(
        f"{quote_identifier(column.name)} {column.direction}"
        for column in index.key_columns
    )
    unique = "UNIQUE " if index.is_unique else ""
    include_sql = (
        "\nINCLUDE ("
        + ", ".join(quote_identifier(column) for column in index.include_columns)
        + ")"
        if index.include_columns
        else ""
    )
    filter_sql = f"\nWHERE {index.filter_definition}" if index.has_filter else ""
    with_sql = ", ".join(
        (
            f"PAD_INDEX = {_on_off(index.is_padded)}",
            f"FILLFACTOR = {index.fill_factor}",
            f"IGNORE_DUP_KEY = {_on_off(index.ignore_dup_key)}",
            f"STATISTICS_NORECOMPUTE = {_on_off(index.statistics_no_recompute)}",
            f"STATISTICS_INCREMENTAL = {_on_off(index.statistics_incremental)}",
            f"ALLOW_ROW_LOCKS = {_on_off(index.allow_row_locks)}",
            f"ALLOW_PAGE_LOCKS = {_on_off(index.allow_page_locks)}",
            f"OPTIMIZE_FOR_SEQUENTIAL_KEY = {_on_off(index.optimize_for_sequential_key)}",
        )
    )
    placement = _placement_sql(index)
    create_sql = (
        "\n".join(_REQUIRED_SET_OPTIONS)
        + "\nCREATE "
        + unique
        + "NONCLUSTERED INDEX "
        + quote_identifier(index.name)
        + " ON "
        + quote_identifier(index.schema)
        + "."
        + quote_identifier(index.table)
        + " ("
        + key_sql
        + ")"
        + include_sql
        + filter_sql
        + "\nWITH ("
        + with_sql
        + ")"
        + placement
        + ";"
    )
    compression_sql = _compression_sql(index)
    base["ddl"] = create_sql + ("\n" + compression_sql if compression_sql else "")
    base["reverse_ddl"] = base["ddl"]
    base["drop_ddl"] = (
        "DROP INDEX "
        + quote_identifier(index.name)
        + " ON "
        + quote_identifier(index.schema)
        + "."
        + quote_identifier(index.table)
        + ";"
    )
    return base


def render_exact_reverse_index_ddl(index: ExistingIndex) -> dict[str, Any]:
    """Explicit alias for callers that want to distinguish this from temp DDL."""

    return render_reverse_index_ddl(index)


def render_reverse_index_definition(definition: Mapping[str, Any]) -> dict[str, Any]:
    """Render reverse DDL from the persisted reversible definition only."""

    if definition.get("version") != 1:
        return _malformed_reverse_result("definition_version_unsupported")

    filter_payload = definition.get("filter")
    filter_payload = filter_payload if isinstance(filter_payload, Mapping) else {}
    data_space = definition.get("data_space")
    data_space = data_space if isinstance(data_space, Mapping) else {}

    def pairs(name: str) -> tuple[tuple[int, str], ...] | None:
        values = definition.get(name, ())
        if not isinstance(values, (list, tuple)):
            return None
        result: list[tuple[int, str]] = []
        for value in values:
            if not isinstance(value, Mapping):
                return None
            try:
                result.append(
                    (int(value["partition_number"]), str(value["compression"]))
                )
            except (KeyError, TypeError, ValueError):
                return None
        return tuple(sorted(result))

    key_values = definition.get("key_columns", ())
    keys: list[IndexKeyColumn] = []
    if isinstance(key_values, (list, tuple)):
        for value in key_values:
            if not isinstance(value, Mapping):
                return _malformed_reverse_result("key_definition_malformed")
            try:
                name = value["name"]
                direction = value["direction"]
                if not isinstance(name, str) or not isinstance(direction, str):
                    return _malformed_reverse_result("key_definition_malformed")
                keys.append(IndexKeyColumn(name, direction))
            except (KeyError, TypeError, ValueError):
                return _malformed_reverse_result("key_definition_malformed")
    else:
        return _malformed_reverse_result("key_definition_malformed")

    include_values = definition.get("include_columns", ())
    if not isinstance(include_values, (list, tuple)):
        return _malformed_reverse_result("include_definition_malformed")
    if any(not isinstance(value, str) or not value for value in include_values):
        return _malformed_reverse_result("include_definition_malformed")
    partition_columns = data_space.get("partition_columns", ()) or ()
    if not isinstance(partition_columns, (list, tuple)):
        return _malformed_reverse_result("partition_definition_malformed")
    if any(not isinstance(value, str) or not value for value in partition_columns):
        return _malformed_reverse_result("partition_definition_malformed")

    partition_compression = pairs("partition_compression")
    xml_compression = pairs("xml_compression")
    if partition_compression is None or xml_compression is None:
        return _malformed_reverse_result("compression_definition_malformed")

    index = ExistingIndex(
        schema=str(definition.get("schema") or ""),
        table=str(definition.get("table") or ""),
        index_id=int(definition.get("index_id") or 0),
        name=str(definition.get("index_name") or ""),
        index_type=str(definition.get("index_type") or ""),
        key_columns=tuple(keys),
        include_columns=tuple(str(value) for value in include_values),
        filter_definition=(
            str(filter_payload.get("definition"))
            if filter_payload.get("definition") is not None
            else None
        ),
        is_unique=definition.get("is_unique"),
        is_primary_key=definition.get("is_primary_key"),
        is_unique_constraint=definition.get("is_unique_constraint"),
        constraint_name=definition.get("constraint_name"),
        constraint_type=definition.get("constraint_type"),
        is_disabled=definition.get("is_disabled"),
        fill_factor=int(definition.get("fill_factor") or 0),
        partition_columns=tuple(str(value) for value in partition_columns),
        data_space_name=(
            str(data_space.get("name")) if data_space.get("name") is not None else None
        ),
        data_space_type=(
            str(data_space.get("type")) if data_space.get("type") is not None else None
        ),
        partition_scheme_name=(
            str(data_space.get("partition_scheme"))
            if data_space.get("partition_scheme") is not None
            else None
        ),
        partition_function_name=(
            str(data_space.get("partition_function"))
            if data_space.get("partition_function") is not None
            else None
        ),
        partition_compression=partition_compression,
        is_hypothetical=definition.get("is_hypothetical"),
        is_auto_created=definition.get("is_auto_created"),
        has_filter=filter_payload.get("has_filter"),
        is_padded=definition.get("is_padded"),
        ignore_dup_key=definition.get("ignore_dup_key"),
        allow_row_locks=definition.get("allow_row_locks"),
        allow_page_locks=definition.get("allow_page_locks"),
        optimize_for_sequential_key=definition.get("optimize_for_sequential_key"),
        suppress_dup_key_messages=definition.get("suppress_dup_key_messages"),
        statistics_no_recompute=definition.get("statistics_no_recompute"),
        statistics_incremental=definition.get("statistics_incremental"),
        xml_compression=xml_compression,
        object_id=definition.get("object_id"),
        parent_object_type=definition.get("parent_object_type"),
        parent_object_type_code=definition.get("parent_object_type_code"),
        index_type_code=definition.get("index_type_code"),
    )
    result = render_reverse_index_ddl(index)
    stored_blockers = definition.get("reversibility_blockers")
    if stored_blockers is not None and not isinstance(stored_blockers, (list, tuple)):
        return _malformed_reverse_result("reversibility_blockers_malformed")
    if isinstance(stored_blockers, (list, tuple)):
        blockers = [str(value) for value in stored_blockers if str(value)]
        if blockers:
            result["blockers"] = list(dict.fromkeys([*result["blockers"], *blockers]))
            result["executable"] = False
            result["ddl"] = None
            result["reverse_ddl"] = None
            result["drop_ddl"] = None
    return result


def _malformed_reverse_result(blocker: str) -> dict[str, Any]:
    return {
        "executable": False,
        "ddl": None,
        "reverse_ddl": None,
        "drop_ddl": None,
        "blockers": [blocker],
    }


def _placement_sql(index: ExistingIndex) -> str:
    data_space_type = (index.data_space_type or "").upper()
    if data_space_type in {"PARTITION_SCHEME", "PARTITION SCHEME"}:
        return (
            "\nON "
            + quote_identifier(index.partition_scheme_name or "")
            + " ("
            + quote_identifier(index.partition_columns[0])
            + ")"
        )
    return "\nON " + quote_identifier(index.data_space_name or "")


def _compression_sql(index: ExistingIndex) -> str:
    data_by_partition = dict(index.partition_compression)
    xml_by_partition = dict(index.xml_compression)
    partition_numbers = sorted(set(data_by_partition) | set(xml_by_partition))
    if not partition_numbers:
        return ""
    groups: dict[tuple[str, str | None], list[int]] = defaultdict(list)
    for number in partition_numbers:
        data_compression = data_by_partition.get(number, "NONE").upper()
        xml_compression = xml_by_partition.get(number)
        groups[
            (data_compression, xml_compression.upper() if xml_compression else None)
        ].append(number)
    statements: list[str] = []
    for (data_compression, xml_compression), numbers in sorted(groups.items()):
        options = [f"DATA_COMPRESSION = {data_compression}"]
        if xml_compression is not None:
            options.append(f"XML_COMPRESSION = {xml_compression}")
        for number in numbers:
            statements.append(
                "ALTER INDEX "
                + quote_identifier(index.name)
                + " ON "
                + quote_identifier(index.schema)
                + "."
                + quote_identifier(index.table)
                + f" REBUILD PARTITION = {number} WITH ({', '.join(options)});"
            )
    return "\n".join(statements)


def _on_off(value: bool | None) -> str:
    return "ON" if value else "OFF"


_REMOVAL_CANDIDATE_STATES = frozenset({"drop_candidate", "consolidate_candidate"})


def render_drop_index_ddl(
    source: ExistingIndex | Mapping[str, Any],
) -> str | None:
    """Render the exact DROP inverse for a complete persisted definition.

    A review subject must carry a reversible definition.  This keeps a
    malformed or incomplete subject from producing a plausible-looking DROP
    statement that has no exact rollback evidence.
    """

    if isinstance(source, ExistingIndex):
        rendered = render_reverse_index_ddl(source)
    elif isinstance(source, Mapping):
        if _subject_reversibility_blockers(source):
            return None
        definition = _reversible_definition(source)
        if definition is None:
            return None
        rendered = render_reverse_index_definition(definition)
    else:
        return None
    if rendered.get("executable") is not True:
        return None
    drop_ddl = rendered.get("drop_ddl")
    return drop_ddl if isinstance(drop_ddl, str) else None


def render_proposed_drop_ddl(subject: Mapping[str, Any]) -> str | None:
    """Render a candidate-only DROP for either removal recommendation state."""

    if str(subject.get("state", "")) not in _REMOVAL_CANDIDATE_STATES:
        return None
    return render_drop_index_ddl(subject)


def render_candidate_rollback(subject: Mapping[str, Any]) -> str | None:
    """Render exact rollback SQL only for an index-removal candidate.

    The returned SQL is deliberately raw so an artifact writer can apply its
    own comment prefix exactly once.  Use
    :func:`render_inert_candidate_rollback` when writing a recommend-only
    artifact directly.
    """

    if str(subject.get("state", "")) not in _REMOVAL_CANDIDATE_STATES:
        return None
    if _subject_reversibility_blockers(subject):
        return None
    definition = _reversible_definition(subject)
    if definition is None:
        return None
    rendered = render_reverse_index_definition(definition)
    reverse_ddl = rendered.get("reverse_ddl")
    if rendered.get("executable") is not True or not isinstance(reverse_ddl, str):
        return None
    return reverse_ddl


def render_inert_candidate_rollback(subject: Mapping[str, Any]) -> str | None:
    """Render candidate rollback SQL as comments, never executable SQL."""

    rollback = render_candidate_rollback(subject)
    return _comment_sql(rollback) if rollback is not None else None


def render_inert_proposed_drop(
    subject: Mapping[str, Any],
    *,
    surviving_index: ExistingIndex | Mapping[str, Any] | str | None = None,
) -> str | None:
    """Render a proposed DROP and optional survivor note as comments."""

    drop_ddl = render_proposed_drop_ddl(subject)
    if drop_ddl is None:
        return None
    lines: list[str] = []
    if str(subject.get("state", "")) == "consolidate_candidate":
        survivor = surviving_index
        if survivor is None:
            candidate_survivor = subject.get("surviving_covering_index")
            if isinstance(candidate_survivor, (ExistingIndex, Mapping, str)):
                survivor = candidate_survivor
        survivor_reference = _survivor_reference(survivor)
        if survivor_reference is not None:
            lines.append(f"Surviving covering index: {survivor_reference}.")
    lines.append(drop_ddl)
    return _comment_sql("\n".join(lines))


def render_validation_selects(subject: Mapping[str, Any]) -> str:
    """Render concrete, commented catalog checks for one review subject."""

    identity = _subject_identity(subject)
    state = str(subject.get("state", "observe"))
    lines = [
        f"Validation for {state} subject {subject.get('subject_id', '')};",
        "Review the returned metadata and coverage before any separately authorised change.",
    ]
    if identity["object_id"] is not None and identity["index_id"] is not None:
        object_id = identity["object_id"]
        index_id = identity["index_id"]
        lines.extend(
            [
                "SELECT",
                "    DB_NAME() AS database_name,",
                "    s.name AS schema_name,",
                "    o.name AS table_name,",
                "    i.name AS index_name,",
                "    i.index_id,",
                "    i.type AS index_type_code,",
                "    i.type_desc AS index_type,",
                "    i.is_unique,",
                "    i.is_primary_key,",
                "    i.is_unique_constraint,",
                "    i.is_disabled,",
                "    i.is_hypothetical,",
                "    i.auto_created,",
                "    i.has_filter,",
                "    i.filter_definition,",
                "    i.is_padded,",
                "    i.ignore_dup_key,",
                "    i.allow_row_locks,",
                "    i.allow_page_locks,",
                "    i.optimize_for_sequential_key,",
                "    i.suppress_dup_key_messages,",
                "    i.fill_factor,",
                "    ds.name AS data_space_name,",
                "    ds.type_desc AS data_space_type",
                "FROM sys.indexes AS i",
                "INNER JOIN sys.objects AS o ON o.object_id = i.object_id",
                "INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id",
                "LEFT JOIN sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id",
                f"WHERE i.object_id = {object_id} AND i.index_id = {index_id};",
                "",
                "SELECT",
                "    ic.index_column_id,",
                "    ic.key_ordinal,",
                "    ic.is_included_column,",
                "    ic.is_descending_key,",
                "    ic.partition_ordinal,",
                "    c.name AS column_name",
                "FROM sys.index_columns AS ic",
                "LEFT JOIN sys.columns AS c",
                "    ON c.object_id = ic.object_id AND c.column_id = ic.column_id",
                f"WHERE ic.object_id = {object_id} AND ic.index_id = {index_id}",
                "ORDER BY ic.is_included_column, ic.key_ordinal, ic.index_column_id;",
                "",
                "SELECT",
                "    user_seeks,",
                "    user_scans,",
                "    user_lookups,",
                "    user_updates,",
                "    last_user_seek,",
                "    last_user_scan,",
                "    last_user_lookup",
                "FROM sys.dm_db_index_usage_stats",
                "WHERE database_id = DB_ID()",
                f"  AND object_id = {object_id} AND index_id = {index_id};",
            ]
        )
    elif identity["schema"] and identity["table"]:
        schema_literal = _sql_string_literal(identity["schema"])
        table_literal = _sql_string_literal(identity["table"])
        lines.extend(
            [
                "SELECT",
                "    DB_NAME() AS database_name,",
                "    s.name AS schema_name,",
                "    o.name AS table_name,",
                "    i.name AS index_name,",
                "    i.index_id,",
                "    i.type AS index_type_code,",
                "    i.type_desc AS index_type,",
                "    i.is_unique,",
                "    i.has_filter,",
                "    i.filter_definition,",
                "    i.fill_factor,",
                "    ds.name AS data_space_name,",
                "    ds.type_desc AS data_space_type",
                "FROM sys.indexes AS i",
                "INNER JOIN sys.objects AS o ON o.object_id = i.object_id",
                "INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id",
                "LEFT JOIN sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id",
                f"WHERE s.name = {schema_literal} AND o.name = {table_literal}",
                "  AND i.index_id > 0",
                "ORDER BY i.index_id, i.name;",
            ]
        )
    else:
        lines.append(
            "No stable schema, table, object, and index identity was recorded."
        )

    expected_keys = _subject_key_columns(subject)
    if expected_keys:
        lines.append(
            "Expected key columns: "
            + ", ".join(
                quote_identifier(name) + " " + direction
                for name, direction in expected_keys
            )
            + "."
        )
    expected_includes = _subject_include_columns(subject)
    if expected_includes:
        lines.append(
            "Expected included columns: "
            + ", ".join(quote_identifier(name) for name in expected_includes)
            + "."
        )
    return _comment_sql("\n".join(lines))


def _reversible_definition(source: Mapping[str, Any]) -> Mapping[str, Any] | None:
    direct = source.get("reversible_definition")
    if isinstance(direct, Mapping):
        return direct
    definition = source.get("definition")
    if isinstance(definition, Mapping):
        nested = definition.get("reversible_definition")
        if isinstance(nested, Mapping):
            return nested
    if "index_name" in source and "key_columns" in source:
        return source
    return None


def _subject_reversibility_blockers(source: Mapping[str, Any]) -> tuple[str, ...]:
    blockers: list[str] = []
    containers: list[Mapping[str, Any]] = [source]
    definition = source.get("definition")
    if isinstance(definition, Mapping):
        containers.append(definition)
    for container in containers:
        value = container.get("reversibility_blockers")
        if value is None:
            continue
        if not isinstance(value, (list, tuple)):
            return ("reversibility_blockers_malformed",)
        blockers.extend(str(item) for item in value if str(item))
    return tuple(dict.fromkeys(blockers))


def _subject_identity(subject: Mapping[str, Any]) -> dict[str, Any]:
    definition = _reversible_definition(subject)
    values = definition if definition is not None else subject

    def value(*names: str) -> Any:
        for name in names:
            if name in values and values[name] is not None:
                return values[name]
        return None

    object_id = value("object_id")
    index_id = value("index_id")
    if not isinstance(object_id, int) or isinstance(object_id, bool) or object_id < 0:
        object_id = None
    if not isinstance(index_id, int) or isinstance(index_id, bool) or index_id < 0:
        index_id = None
    return {
        "schema": value("schema_name", "schema"),
        "table": value("table_name", "table"),
        "index_name": value("index_name", "name"),
        "object_id": object_id,
        "index_id": index_id,
    }


def _survivor_reference(
    source: ExistingIndex | Mapping[str, Any] | str | None,
) -> str | None:
    if isinstance(source, ExistingIndex):
        identity = {
            "schema": source.schema,
            "table": source.table,
            "index_name": source.name,
        }
    elif isinstance(source, Mapping):
        identity = _subject_identity(source)
    elif isinstance(source, str) and source:
        return quote_identifier(source)
    else:
        return None
    if not all(
        isinstance(identity.get(key), str) and identity[key]
        for key in ("schema", "table", "index_name")
    ):
        return None
    return ".".join(
        quote_identifier(str(identity[key]))
        for key in ("schema", "table", "index_name")
    )


def _subject_key_columns(subject: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    definition = _reversible_definition(subject)
    values = definition if definition is not None else subject
    raw_values = values.get("key_columns", ())
    if not isinstance(raw_values, (list, tuple)):
        return ()
    result: list[tuple[str, str]] = []
    for raw in raw_values:
        if isinstance(raw, Mapping):
            name = raw.get("name")
            direction = str(raw.get("direction", "ASC")).upper()
        else:
            # Missing-index subjects store exact catalog names. A legal name may
            # itself end in ASC or DESC, so only structured keys carry direction.
            name = str(raw)
            direction = "ASC"
        if isinstance(name, str) and name and direction in {"ASC", "DESC"}:
            result.append((name, direction))
    return tuple(result)


def _subject_include_columns(subject: Mapping[str, Any]) -> tuple[str, ...]:
    definition = _reversible_definition(subject)
    values = definition if definition is not None else subject
    raw_values = values.get("include_columns", ())
    if not isinstance(raw_values, (list, tuple)):
        return ()
    return tuple(value for value in raw_values if isinstance(value, str) and value)


def _sql_string_literal(value: Any) -> str:
    return "N'" + str(value).replace("'", "''") + "'"


def _comment_sql(value: str) -> str:
    return "\n".join("-- " + line if line else "--" for line in value.splitlines())


__all__ = [
    "quote_identifier",
    "render_candidate_rollback",
    "render_exact_reverse_index_ddl",
    "render_drop_index_ddl",
    "render_inert_candidate_rollback",
    "render_inert_proposed_drop",
    "render_proposed_drop_ddl",
    "render_reverse_index_definition",
    "render_reverse_index_ddl",
    "render_validation_selects",
]
