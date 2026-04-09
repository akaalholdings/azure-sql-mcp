from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from .schema_diff import DiffCategory
from .schema_diff import DiffType
from .schema_diff import SchemaDifference
from .schema_snapshot import ColumnDef
from .schema_snapshot import ConstraintDef
from .schema_snapshot import IndexDef
from .schema_snapshot import ProgrammableObjectSnapshot
from .schema_snapshot import SchemaSnapshot
from .schema_snapshot import SequenceDef
from .schema_snapshot import TableSnapshot
from .schema_snapshot import TriggerDef

DEFAULT_CONSTRAINT_TYPE = "DEFAULT"
FOREIGN_KEY_CONSTRAINT_TYPE = "FOREIGN_KEY"
PRIMARY_KEY_CONSTRAINT_TYPE = "PRIMARY_KEY"
UNIQUE_CONSTRAINT_TYPE = "UNIQUE"
CHECK_CONSTRAINT_TYPE = "CHECK"

TYPE_LENGTH_TYPES = {"char", "nchar", "varchar", "nvarchar", "binary", "varbinary"}
TYPE_PRECISION_SCALE_TYPES = {"decimal", "numeric"}
TYPE_SCALE_TYPES = {"datetime2", "datetimeoffset", "time"}


def generate_migration_script(
    differences: list[SchemaDifference],
    source_snapshot: SchemaSnapshot,
    target_snapshot: SchemaSnapshot,
) -> str:
    added_tables = {
        (diff.schema_name, diff.object_name)
        for diff in differences
        if diff.category == DiffCategory.TABLE and diff.diff_type == DiffType.ADDED
    }
    removed_tables = {
        (diff.schema_name, diff.object_name)
        for diff in differences
        if diff.category == DiffCategory.TABLE and diff.diff_type == DiffType.REMOVED
    }
    added_columns = _collect_column_names(differences, DiffType.ADDED)
    removed_columns = _collect_column_names(differences, DiffType.REMOVED)

    drop_constraint_statements: list[tuple[str, str]] = []
    drop_index_statements: list[tuple[str, str]] = []
    drop_trigger_statements: list[tuple[str, str]] = []
    drop_programmable_statements: list[tuple[str, str]] = []
    drop_sequence_statements: list[tuple[str, str]] = []
    drop_table_statements: list[tuple[str, str]] = []
    alter_table_statements: list[tuple[str, str]] = []
    create_table_statements: list[tuple[str, str]] = []
    create_programmable_statements: list[tuple[str, str]] = []
    create_index_statements: list[tuple[str, str]] = []
    create_constraint_statements: list[tuple[str, str]] = []
    create_sequence_statements: list[tuple[str, str]] = []
    create_trigger_statements: list[tuple[str, str]] = []

    for diff in differences:
        table_key = _table_key_for_diff(diff)
        if table_key and table_key in added_tables and diff.category != DiffCategory.TABLE:
            continue
        if table_key and table_key in removed_tables and diff.category != DiffCategory.TABLE:
            continue

        if diff.category == DiffCategory.TABLE:
            if diff.diff_type == DiffType.ADDED:
                table = target_snapshot.tables[(diff.schema_name, diff.object_name)]
                create_table_statements.append(
                    (
                        f"Create table [{table.schema_name}].[{table.table_name}]",
                        _render_create_table_statement(table),
                    )
                )
                for index in table.indexes:
                    create_index_statements.append(
                        (
                            f"Create index [{table.schema_name}].[{table.table_name}].[{index.name}]",
                            _render_create_index_statement(table.schema_name, table.table_name, index),
                        )
                    )
                for constraint in table.constraints:
                    if constraint.constraint_type == FOREIGN_KEY_CONSTRAINT_TYPE:
                        create_constraint_statements.append(
                            (
                                f"Add foreign key [{table.schema_name}].[{table.table_name}].[{constraint.name}]",
                                _render_add_constraint_statement(table, constraint),
                            )
                        )
                continue

            if diff.diff_type == DiffType.REMOVED:
                drop_table_statements.append(
                    (
                        f"Drop table [{diff.schema_name}].[{diff.object_name}]",
                        f"DROP TABLE {quote_qualified_name(diff.schema_name, diff.object_name)};",
                    )
                )
                continue

        if diff.category == DiffCategory.COLUMN:
            if diff.diff_type == DiffType.ADDED:
                table_name, column_name = _split_object_name(diff.object_name)
                table = target_snapshot.tables[(diff.schema_name, table_name)]
                column = _get_column(table, column_name)
                alter_table_statements.append(
                    (
                        f"Add column [{diff.schema_name}].[{table_name}].[{column_name}]",
                        _render_add_column_statement(table, column),
                    )
                )
                continue

            if diff.diff_type == DiffType.REMOVED:
                table_name, column_name = _split_object_name(diff.object_name)
                alter_table_statements.append(
                    (
                        f"Drop column [{diff.schema_name}].[{table_name}].[{column_name}]",
                        f"ALTER TABLE {quote_qualified_name(diff.schema_name, table_name)} "
                        f"DROP COLUMN {quote_identifier(column_name)};",
                    )
                )
                continue

            table_name, column_name = _split_object_name(diff.object_name)
            source_column = diff.source_definition
            target_column = diff.target_definition
            assert isinstance(source_column, ColumnDef)
            assert isinstance(target_column, ColumnDef)
            alter_table_statements.append(
                (
                    f"Alter column [{diff.schema_name}].[{table_name}].[{column_name}]",
                    _render_alter_column_statement(diff.schema_name, table_name, source_column, target_column),
                )
            )
            continue

        if diff.category == DiffCategory.INDEX:
            table_name, index_name = _split_object_name(diff.object_name)
            if diff.diff_type == DiffType.REMOVED:
                source_index = diff.source_definition
                assert isinstance(source_index, IndexDef)
                drop_index_statements.append(
                    (
                        f"Drop index [{diff.schema_name}].[{table_name}].[{index_name}]",
                        _render_drop_index_statement(diff.schema_name, table_name, source_index),
                    )
                )
            elif diff.diff_type == DiffType.ADDED:
                target_index = diff.target_definition
                assert isinstance(target_index, IndexDef)
                create_index_statements.append(
                    (
                        f"Create index [{diff.schema_name}].[{table_name}].[{index_name}]",
                        _render_create_index_statement(diff.schema_name, table_name, target_index),
                    )
                )
            else:
                source_index = diff.source_definition
                target_index = diff.target_definition
                assert isinstance(source_index, IndexDef)
                assert isinstance(target_index, IndexDef)
                drop_index_statements.append(
                    (
                        f"Drop index [{diff.schema_name}].[{table_name}].[{index_name}]",
                        _render_drop_index_statement(diff.schema_name, table_name, source_index),
                    )
                )
                create_index_statements.append(
                    (
                        f"Recreate index [{diff.schema_name}].[{table_name}].[{index_name}]",
                        _render_create_index_statement(diff.schema_name, table_name, target_index),
                    )
                )
            continue

        if diff.category == DiffCategory.CONSTRAINT:
            table_name, constraint_name = _split_object_name(diff.object_name)
            if (
                diff.diff_type == DiffType.ADDED
                and diff.target_definition is not None
                and _is_default_constraint(diff.target_definition)
                and _constraint_touches_added_or_removed_column(
                    diff.schema_name,
                    table_name,
                    diff.target_definition,
                    added_columns,
                    removed_columns,
                )
            ):
                continue
            if (
                diff.diff_type == DiffType.REMOVED
                and diff.source_definition is not None
                and _is_default_constraint(diff.source_definition)
                and _constraint_touches_added_or_removed_column(
                    diff.schema_name,
                    table_name,
                    diff.source_definition,
                    added_columns,
                    removed_columns,
                )
            ):
                continue

            if diff.diff_type == DiffType.REMOVED:
                source_constraint = diff.source_definition
                assert isinstance(source_constraint, ConstraintDef)
                drop_constraint_statements.append(
                    (
                        f"Drop constraint [{diff.schema_name}].[{table_name}].[{constraint_name}]",
                        _render_drop_constraint_statement(diff.schema_name, table_name, constraint_name),
                    )
                )
            elif diff.diff_type == DiffType.ADDED:
                target_constraint = diff.target_definition
                assert isinstance(target_constraint, ConstraintDef)
                create_constraint_statements.append(
                    (
                        f"Add constraint [{diff.schema_name}].[{table_name}].[{constraint_name}]",
                        _render_add_constraint_statement_from_constraint(
                            diff.schema_name,
                            table_name,
                            target_constraint,
                        ),
                    )
                )
            else:
                source_constraint = diff.source_definition
                target_constraint = diff.target_definition
                assert isinstance(source_constraint, ConstraintDef)
                assert isinstance(target_constraint, ConstraintDef)
                drop_constraint_statements.append(
                    (
                        f"Drop constraint [{diff.schema_name}].[{table_name}].[{constraint_name}]",
                        _render_drop_constraint_statement(diff.schema_name, table_name, constraint_name),
                    )
                )
                create_constraint_statements.append(
                    (
                        f"Recreate constraint [{diff.schema_name}].[{table_name}].[{constraint_name}]",
                        _render_add_constraint_statement_from_constraint(
                            diff.schema_name,
                            table_name,
                            target_constraint,
                        ),
                    )
                )
            continue

        if diff.category == DiffCategory.TRIGGER:
            table_name, trigger_name = _split_object_name(diff.object_name)
            if diff.diff_type == DiffType.REMOVED:
                drop_trigger_statements.append(
                    (
                        f"Drop trigger [{diff.schema_name}].[{trigger_name}]",
                        f"DROP TRIGGER {quote_qualified_name(diff.schema_name, trigger_name)};",
                    )
                )
            elif diff.diff_type == DiffType.ADDED:
                target_trigger = diff.target_definition
                assert isinstance(target_trigger, TriggerDef)
                if target_trigger.definition:
                    create_trigger_statements.append(
                        (
                            f"Create trigger [{diff.schema_name}].[{trigger_name}]",
                            _ensure_statement_terminator(target_trigger.definition.strip()),
                        )
                    )
            else:
                target_trigger = diff.target_definition
                assert isinstance(target_trigger, TriggerDef)
                if target_trigger.definition:
                    altered = re.sub(
                        r"^\s*CREATE\s+",
                        "ALTER ",
                        target_trigger.definition,
                        count=1,
                        flags=re.IGNORECASE,
                    )
                    create_trigger_statements.append(
                        (
                            f"Alter trigger [{diff.schema_name}].[{trigger_name}]",
                            _ensure_statement_terminator(altered.strip()),
                        )
                    )
            continue

        if diff.category == DiffCategory.SEQUENCE:
            if diff.diff_type == DiffType.REMOVED:
                drop_sequence_statements.append(
                    (
                        f"Drop sequence [{diff.schema_name}].[{diff.object_name}]",
                        f"DROP SEQUENCE {quote_qualified_name(diff.schema_name, diff.object_name)};",
                    )
                )
            elif diff.diff_type == DiffType.ADDED:
                target_seq = diff.target_definition
                assert isinstance(target_seq, SequenceDef)
                create_sequence_statements.append(
                    (
                        f"Create sequence [{diff.schema_name}].[{diff.object_name}]",
                        _render_create_sequence(target_seq),
                    )
                )
            else:
                target_seq = diff.target_definition
                assert isinstance(target_seq, SequenceDef)
                create_sequence_statements.append(
                    (
                        f"Alter sequence [{diff.schema_name}].[{diff.object_name}]",
                        _render_alter_sequence(target_seq),
                    )
                )
            continue

        if diff.category in {DiffCategory.VIEW, DiffCategory.PROCEDURE, DiffCategory.FUNCTION}:
            if diff.diff_type == DiffType.REMOVED:
                source_object = diff.source_definition
                assert isinstance(source_object, ProgrammableObjectSnapshot)
                drop_programmable_statements.append(
                    (
                        f"Drop {diff.category.value} [{diff.schema_name}].[{diff.object_name}]",
                        _render_drop_programmable_statement(source_object),
                    )
                )
            elif diff.diff_type == DiffType.ADDED:
                target_object = diff.target_definition
                assert isinstance(target_object, ProgrammableObjectSnapshot)
                create_programmable_statements.append(
                    (
                        f"Create {diff.category.value} [{diff.schema_name}].[{diff.object_name}]",
                        _render_programmable_create_statement(target_object),
                    )
                )
            else:
                target_object = diff.target_definition
                assert isinstance(target_object, ProgrammableObjectSnapshot)
                create_programmable_statements.append(
                    (
                        f"Alter {diff.category.value} [{diff.schema_name}].[{diff.object_name}]",
                        _render_programmable_alter_statement(target_object),
                    )
                )
            continue

    lines = [
        "SET XACT_ABORT ON;",
        "BEGIN TRANSACTION;",
        "",
    ]

    statements_in_order = (
        ("Dropping triggers", drop_trigger_statements),
        ("Dropping constraints", drop_constraint_statements),
        ("Dropping indexes", drop_index_statements),
        ("Dropping programmable objects", drop_programmable_statements),
        ("Dropping sequences", drop_sequence_statements),
        ("Dropping tables", drop_table_statements),
        ("Creating sequences", create_sequence_statements),
        ("Creating tables", create_table_statements),
        ("Altering tables", alter_table_statements),
        ("Creating or altering programmable objects", create_programmable_statements),
        ("Creating indexes", create_index_statements),
        ("Creating constraints", create_constraint_statements),
        ("Creating or altering triggers", create_trigger_statements),
    )

    for section_name, statements in statements_in_order:
        if not statements:
            continue
        lines.append(f"-- {section_name}")
        for label, statement in statements:
            _append_statement(lines, label, statement)
        lines.append("")

    if len(lines) == 3:
        lines.append("PRINT N'No schema changes detected.';")

    lines.append("COMMIT TRANSACTION;")
    return "\n".join(lines)


def _append_statement(lines: list[str], label: str, statement: str) -> None:
    lines.append(f"-- {label}")
    lines.append(f"PRINT N'{_escape_sql_string(label)}';")
    lines.append(statement.rstrip())


def _collect_column_names(
    differences: list[SchemaDifference],
    diff_type: DiffType,
) -> dict[tuple[str, str], set[str]]:
    collected: dict[tuple[str, str], set[str]] = defaultdict(set)
    for diff in differences:
        if diff.category != DiffCategory.COLUMN or diff.diff_type != diff_type:
            continue
        table_name, column_name = _split_object_name(diff.object_name)
        collected[(diff.schema_name, table_name)].add(column_name)
    return collected


def _table_key_for_diff(diff: SchemaDifference) -> tuple[str, str] | None:
    if diff.category == DiffCategory.TABLE:
        return (diff.schema_name, diff.object_name)
    if diff.category in {
        DiffCategory.COLUMN,
        DiffCategory.INDEX,
        DiffCategory.CONSTRAINT,
    }:
        table_name, _ = _split_object_name(diff.object_name)
        return (diff.schema_name, table_name)
    return None


def _constraint_touches_added_or_removed_column(
    schema_name: str,
    table_name: str,
    constraint: ConstraintDef,
    added_columns: dict[tuple[str, str], set[str]],
    removed_columns: dict[tuple[str, str], set[str]],
) -> bool:
    columns = set(constraint.columns)
    for key, column_names in added_columns.items():
        if key == (schema_name, table_name) and columns.intersection(column_names):
            return True
    for key, column_names in removed_columns.items():
        if key == (schema_name, table_name) and columns.intersection(column_names):
            return True
    return False


def _render_create_table_statement(table: TableSnapshot) -> str:
    lines = [f"CREATE TABLE {quote_qualified_name(table.schema_name, table.table_name)} ("]
    body_lines: list[str] = []

    for column in table.columns:
        body_lines.append(f"    {render_column_definition(column)}")

    for constraint in _ordered_table_constraints(table.constraints):
        if constraint.constraint_type == FOREIGN_KEY_CONSTRAINT_TYPE:
            continue
        body_lines.append(f"    {_render_table_constraint_fragment(constraint)}")

    for index, line in enumerate(body_lines):
        suffix = "," if index < len(body_lines) - 1 else ""
        lines.append(f"{line}{suffix}")

    lines.append(");")
    return "\n".join(lines)


def _ordered_table_constraints(constraints: tuple[ConstraintDef, ...]) -> list[ConstraintDef]:
    priority = {
        PRIMARY_KEY_CONSTRAINT_TYPE: 0,
        UNIQUE_CONSTRAINT_TYPE: 1,
        CHECK_CONSTRAINT_TYPE: 2,
        DEFAULT_CONSTRAINT_TYPE: 3,
        FOREIGN_KEY_CONSTRAINT_TYPE: 4,
    }
    return sorted(
        constraints,
        key=lambda constraint: (priority.get(constraint.constraint_type, 99), constraint.name),
    )


def _render_table_constraint_fragment(constraint: ConstraintDef) -> str:
    if constraint.constraint_type == PRIMARY_KEY_CONSTRAINT_TYPE:
        columns = ", ".join(quote_identifier(column) for column in constraint.columns)
        return (
            f"CONSTRAINT {quote_identifier(constraint.name)} PRIMARY KEY ({columns})"
        )
    if constraint.constraint_type == UNIQUE_CONSTRAINT_TYPE:
        columns = ", ".join(quote_identifier(column) for column in constraint.columns)
        return f"CONSTRAINT {quote_identifier(constraint.name)} UNIQUE ({columns})"
    if constraint.constraint_type == CHECK_CONSTRAINT_TYPE:
        definition = constraint.definition or "1 = 1"
        return f"CONSTRAINT {quote_identifier(constraint.name)} CHECK {definition}"
    if constraint.constraint_type == DEFAULT_CONSTRAINT_TYPE:
        column_name = constraint.columns[0] if constraint.columns else "UnknownColumn"
        default_definition = constraint.definition or "(NULL)"
        return (
            f"CONSTRAINT {quote_identifier(constraint.name)} DEFAULT {default_definition} "
            f"FOR {quote_identifier(column_name)}"
        )
    raise ValueError(f"Unsupported constraint type: {constraint.constraint_type}")


def _render_add_constraint_statement(
    table: TableSnapshot,
    constraint: ConstraintDef,
) -> str:
    return _render_add_constraint_statement_from_constraint(
        table.schema_name,
        table.table_name,
        constraint,
    )


def _render_add_constraint_statement_from_constraint(
    schema_name: str,
    table_name: str,
    constraint: ConstraintDef,
) -> str:
    if constraint.constraint_type == FOREIGN_KEY_CONSTRAINT_TYPE:
        columns = ", ".join(quote_identifier(column) for column in constraint.columns)
        referenced_columns = ", ".join(
            quote_identifier(column)
            for column in (constraint.referenced_columns or ())
        )
        nocheck = "WITH NOCHECK " if constraint.is_not_trusted else ""
        stmt = (
            f"ALTER TABLE {quote_qualified_name(schema_name, table_name)} "
            f"{nocheck}ADD CONSTRAINT {quote_identifier(constraint.name)} FOREIGN KEY ({columns}) "
            f"REFERENCES {quote_qualified_name(constraint.referenced_schema or 'dbo', constraint.referenced_table or 'UnknownTable')} "
            f"({referenced_columns})"
        )
        cascade_parts: list[str] = []
        if constraint.delete_action and constraint.delete_action != "NO_ACTION":
            cascade_parts.append(f"ON DELETE {constraint.delete_action.replace('_', ' ')}")
        if constraint.update_action and constraint.update_action != "NO_ACTION":
            cascade_parts.append(f"ON UPDATE {constraint.update_action.replace('_', ' ')}")
        if cascade_parts:
            stmt += " " + " ".join(cascade_parts)
        return stmt + ";"
    if constraint.constraint_type == PRIMARY_KEY_CONSTRAINT_TYPE:
        columns = ", ".join(quote_identifier(column) for column in constraint.columns)
        return (
            f"ALTER TABLE {quote_qualified_name(schema_name, table_name)} "
            f"ADD CONSTRAINT {quote_identifier(constraint.name)} PRIMARY KEY ({columns});"
        )
    if constraint.constraint_type == UNIQUE_CONSTRAINT_TYPE:
        columns = ", ".join(quote_identifier(column) for column in constraint.columns)
        return (
            f"ALTER TABLE {quote_qualified_name(schema_name, table_name)} "
            f"ADD CONSTRAINT {quote_identifier(constraint.name)} UNIQUE ({columns});"
        )
    if constraint.constraint_type == CHECK_CONSTRAINT_TYPE:
        definition = constraint.definition or "1 = 1"
        nocheck = "WITH NOCHECK " if constraint.is_not_trusted else ""
        return (
            f"ALTER TABLE {quote_qualified_name(schema_name, table_name)} "
            f"{nocheck}ADD CONSTRAINT {quote_identifier(constraint.name)} CHECK {definition};"
        )
    if constraint.constraint_type == DEFAULT_CONSTRAINT_TYPE:
        column_name = constraint.columns[0] if constraint.columns else "UnknownColumn"
        default_definition = constraint.definition or "(NULL)"
        return (
            f"ALTER TABLE {quote_qualified_name(schema_name, table_name)} "
            f"ADD CONSTRAINT {quote_identifier(constraint.name)} DEFAULT {default_definition} "
            f"FOR {quote_identifier(column_name)};"
        )
    raise ValueError(f"Unsupported constraint type: {constraint.constraint_type}")


def _render_drop_constraint_statement(
    schema_name: str,
    table_name: str,
    constraint_name: str,
) -> str:
    return (
        f"ALTER TABLE {quote_qualified_name(schema_name, table_name)} "
        f"DROP CONSTRAINT {quote_identifier(constraint_name)};"
    )


def _render_add_column_statement(table: TableSnapshot, column: ColumnDef) -> str:
    default_clause = ""
    if column.default_definition:
        default_clause = f" DEFAULT {column.default_definition}"
    return (
        f"ALTER TABLE {quote_qualified_name(table.schema_name, table.table_name)} "
        f"ADD {render_column_definition(column, include_default=False)}{default_clause};"
    )


def _render_alter_column_statement(
    schema_name: str,
    table_name: str,
    source_column: ColumnDef,
    target_column: ColumnDef,
) -> str:
    column_definition = render_column_type(target_column)
    nullability = "NULL" if target_column.is_nullable else "NOT NULL"
    return (
        f"ALTER TABLE {quote_qualified_name(schema_name, table_name)} "
        f"ALTER COLUMN {quote_identifier(target_column.name)} {column_definition} {nullability};"
    )


def _render_create_index_statement(
    schema_name: str,
    table_name: str,
    index: IndexDef,
) -> str:
    parts = ["CREATE"]
    if index.is_unique:
        parts.append("UNIQUE")
    index_type = index.index_type.upper()
    if "XML" not in index_type and "SPATIAL" not in index_type:
        parts.append(index.index_type)
    else:
        parts.append(index.index_type)
    parts.append("INDEX")
    parts.append(quote_identifier(index.name))
    parts.append(f"ON {quote_qualified_name(schema_name, table_name)}")
    parts.append(
        "("
        + ", ".join(quote_identifier(column) for column in index.key_columns)
        + ")"
    )
    if index.included_columns:
        parts.append(
            "INCLUDE ("
            + ", ".join(quote_identifier(column) for column in index.included_columns)
            + ")"
        )
    if index.filter_definition:
        parts.append(f"WHERE {index.filter_definition}")
    with_options: list[str] = []
    if index.data_compression and index.data_compression != "NONE":
        with_options.append(f"DATA_COMPRESSION = {index.data_compression}")
    if with_options:
        parts.append("WITH (" + ", ".join(with_options) + ")")
    return " ".join(parts) + ";"


def _render_drop_index_statement(
    schema_name: str,
    table_name: str,
    index: IndexDef,
) -> str:
    return (
        f"DROP INDEX {quote_identifier(index.name)} "
        f"ON {quote_qualified_name(schema_name, table_name)};"
    )


def _render_programmable_create_statement(
    obj: ProgrammableObjectSnapshot,
) -> str:
    definition = obj.definition
    if not definition:
        return (
            f"-- Definition unavailable for {obj.object_type.lower()} "
            f"{quote_qualified_name(obj.schema_name, obj.object_name)}"
        )
    return _ensure_statement_terminator(definition.strip())


def _render_programmable_alter_statement(
    obj: ProgrammableObjectSnapshot,
) -> str:
    definition = obj.definition
    if not definition:
        return (
            f"-- Definition unavailable for {obj.object_type.lower()} "
            f"{quote_qualified_name(obj.schema_name, obj.object_name)}"
        )
    altered = re.sub(
        r"^\s*CREATE\s+OR\s+ALTER\s+",
        "ALTER ",
        definition,
        count=1,
        flags=re.IGNORECASE,
    )
    if altered == definition:
        altered = re.sub(
            r"^\s*CREATE\s+",
            "ALTER ",
            definition,
            count=1,
            flags=re.IGNORECASE,
        )
    return _ensure_statement_terminator(altered.strip())


def _render_drop_programmable_statement(obj: ProgrammableObjectSnapshot) -> str:
    object_type = obj.object_type.upper()
    if object_type == "VIEW":
        drop_prefix = "DROP VIEW"
    elif object_type == "PROCEDURE":
        drop_prefix = "DROP PROCEDURE"
    else:
        drop_prefix = "DROP FUNCTION"
    return f"{drop_prefix} {quote_qualified_name(obj.schema_name, obj.object_name)};"


def render_column_definition(column: ColumnDef, include_default: bool = True) -> str:
    if column.computed_definition:
        persisted = " PERSISTED" if column.is_persisted else ""
        return f"{quote_identifier(column.name)} AS {column.computed_definition}{persisted}"
    column_type = render_column_type(column)
    nullability = "NULL" if column.is_nullable else "NOT NULL"
    default_clause = ""
    if include_default and column.default_definition:
        default_clause = f" DEFAULT {column.default_definition}"
    return f"{quote_identifier(column.name)} {column_type} {nullability}{default_clause}"


def render_column_type(column: ColumnDef) -> str:
    data_type = column.data_type.lower()
    if data_type in TYPE_PRECISION_SCALE_TYPES:
        return f"{column.data_type}({column.precision}, {column.scale})"
    if data_type in TYPE_SCALE_TYPES:
        return f"{column.data_type}({column.scale})"
    if data_type in TYPE_LENGTH_TYPES:
        if column.max_length == -1:
            return f"{column.data_type}(max)"
        length = column.max_length
        if data_type.startswith("n"):
            length = max(length // 2, 0)
        return f"{column.data_type}({length})"
    return column.data_type


def quote_identifier(identifier: str) -> str:
    return f"[{identifier.replace(']', ']]')}]"


def quote_qualified_name(schema_name: str, object_name: str) -> str:
    return f"{quote_identifier(schema_name)}.{quote_identifier(object_name)}"


def _get_column(table: TableSnapshot, column_name: str) -> ColumnDef:
    for column in table.columns:
        if column.name == column_name:
            return column
    raise KeyError(f"Column {column_name!r} not found in {table.schema_name}.{table.table_name}")


def _split_object_name(object_name: str) -> tuple[str, str]:
    if "." not in object_name:
        raise ValueError(f"Expected table-qualified object name, got {object_name!r}")
    return object_name.split(".", 1)


def _is_default_constraint(constraint: ConstraintDef) -> bool:
    return constraint.constraint_type == DEFAULT_CONSTRAINT_TYPE


def _ensure_statement_terminator(statement: str) -> str:
    if statement.rstrip().endswith(";"):
        return statement
    return statement.rstrip() + ";"


def _render_create_sequence(seq: SequenceDef) -> str:
    cycling = "CYCLE" if seq.is_cycling else "NO CYCLE"
    return (
        f"CREATE SEQUENCE {quote_qualified_name(seq.schema_name, seq.name)} "
        f"AS {seq.data_type} "
        f"START WITH {seq.start_value} "
        f"INCREMENT BY {seq.increment} "
        f"MINVALUE {seq.min_value} "
        f"MAXVALUE {seq.max_value} "
        f"{cycling};"
    )


def _render_alter_sequence(seq: SequenceDef) -> str:
    cycling = "CYCLE" if seq.is_cycling else "NO CYCLE"
    return (
        f"ALTER SEQUENCE {quote_qualified_name(seq.schema_name, seq.name)} "
        f"INCREMENT BY {seq.increment} "
        f"MINVALUE {seq.min_value} "
        f"MAXVALUE {seq.max_value} "
        f"{cycling};"
    )


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")
