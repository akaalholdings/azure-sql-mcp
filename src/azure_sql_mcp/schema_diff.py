from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from enum import Enum
import re
from typing import Any

from .schema_snapshot import ProgrammableObjectSnapshot
from .schema_snapshot import SchemaSnapshot
from .schema_snapshot import SequenceDef
from .schema_snapshot import TableSnapshot

WHITESPACE_PATTERN = re.compile(r"\s+")


class DiffType(str, Enum):
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"


class DiffCategory(str, Enum):
    TABLE = "table"
    COLUMN = "column"
    INDEX = "index"
    CONSTRAINT = "constraint"
    TRIGGER = "trigger"
    SEQUENCE = "sequence"
    VIEW = "view"
    PROCEDURE = "procedure"
    FUNCTION = "function"


CATEGORY_ORDER = {
    DiffCategory.TABLE: 0,
    DiffCategory.COLUMN: 1,
    DiffCategory.INDEX: 2,
    DiffCategory.CONSTRAINT: 3,
    DiffCategory.TRIGGER: 4,
    DiffCategory.SEQUENCE: 5,
    DiffCategory.VIEW: 6,
    DiffCategory.PROCEDURE: 7,
    DiffCategory.FUNCTION: 8,
}

DIFF_TYPE_ORDER = {
    DiffType.ADDED: 0,
    DiffType.MODIFIED: 1,
    DiffType.REMOVED: 2,
}


@dataclass(frozen=True)
class SchemaDifference:
    diff_type: DiffType
    category: DiffCategory
    schema_name: str
    object_name: str
    detail: str
    source_definition: Any | None = None
    target_definition: Any | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "diff_type": self.diff_type.value,
            "category": self.category.value,
            "schema_name": self.schema_name,
            "object_name": self.object_name,
            "detail": self.detail,
            "source_definition": _serialize_value(self.source_definition)
            if self.source_definition is not None
            else None,
            "target_definition": _serialize_value(self.target_definition)
            if self.target_definition is not None
            else None,
        }


def compare_snapshots(
    source: SchemaSnapshot,
    target: SchemaSnapshot,
) -> list[SchemaDifference]:
    differences: list[SchemaDifference] = []

    source_tables = source.tables
    target_tables = target.tables

    source_table_keys = set(source_tables)
    target_table_keys = set(target_tables)

    for schema_name, table_name in sorted(target_table_keys - source_table_keys):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.ADDED,
                category=DiffCategory.TABLE,
                schema_name=schema_name,
                object_name=table_name,
                detail=f"Table [{schema_name}].[{table_name}] exists in target but not in source",
                target_definition=target_tables[(schema_name, table_name)],
            )
        )

    for schema_name, table_name in sorted(source_table_keys - target_table_keys):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.REMOVED,
                category=DiffCategory.TABLE,
                schema_name=schema_name,
                object_name=table_name,
                detail=f"Table [{schema_name}].[{table_name}] exists in source but not in target",
                source_definition=source_tables[(schema_name, table_name)],
            )
        )

    for key in sorted(source_table_keys & target_table_keys):
        source_table = source_tables[key]
        target_table = target_tables[key]
        differences.extend(_compare_table(source_table, target_table))

    differences.extend(_compare_sequences(
        source.sequences or {},
        target.sequences or {},
    ))
    differences.extend(_compare_programmable_group(source.views, target.views, DiffCategory.VIEW))
    differences.extend(
        _compare_programmable_group(
            source.procedures,
            target.procedures,
            DiffCategory.PROCEDURE,
        )
    )
    differences.extend(
        _compare_programmable_group(
            source.functions,
            target.functions,
            DiffCategory.FUNCTION,
        )
    )

    return sorted(
        differences,
        key=lambda item: (
            CATEGORY_ORDER[item.category],
            item.schema_name,
            item.object_name,
            DIFF_TYPE_ORDER[item.diff_type],
            item.detail,
        ),
    )


def _compare_table(
    source_table: TableSnapshot,
    target_table: TableSnapshot,
) -> list[SchemaDifference]:
    differences: list[SchemaDifference] = []

    source_columns = {column.name: column for column in source_table.columns}
    target_columns = {column.name: column for column in target_table.columns}

    for column_name in sorted(target_columns.keys() - source_columns.keys()):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.ADDED,
                category=DiffCategory.COLUMN,
                schema_name=target_table.schema_name,
                object_name=f"{target_table.table_name}.{column_name}",
                detail=f"Column {column_name} exists in target but not in source",
                target_definition=target_columns[column_name],
            )
        )

    for column_name in sorted(source_columns.keys() - target_columns.keys()):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.REMOVED,
                category=DiffCategory.COLUMN,
                schema_name=source_table.schema_name,
                object_name=f"{source_table.table_name}.{column_name}",
                detail=f"Column {column_name} exists in source but not in target",
                source_definition=source_columns[column_name],
            )
        )

    for column_name in sorted(source_columns.keys() & target_columns.keys()):
        source_column = source_columns[column_name]
        target_column = target_columns[column_name]
        changes = []
        if source_column.data_type != target_column.data_type:
            changes.append(
                f"data_type: {source_column.data_type} -> {target_column.data_type}"
            )
        if source_column.max_length != target_column.max_length:
            changes.append(
                f"max_length: {source_column.max_length} -> {target_column.max_length}"
            )
        if source_column.precision != target_column.precision:
            changes.append(
                f"precision: {source_column.precision} -> {target_column.precision}"
            )
        if source_column.scale != target_column.scale:
            changes.append(f"scale: {source_column.scale} -> {target_column.scale}")
        if source_column.is_nullable != target_column.is_nullable:
            changes.append(
                f"is_nullable: {source_column.is_nullable} -> {target_column.is_nullable}"
            )
        if _normalize_definition(source_column.default_definition) != _normalize_definition(
            target_column.default_definition
        ):
            changes.append(
                "default_definition: "
                f"{source_column.default_definition or 'NULL'} -> "
                f"{target_column.default_definition or 'NULL'}"
            )
        if _normalize_definition(source_column.computed_definition) != _normalize_definition(
            target_column.computed_definition
        ):
            changes.append(
                "computed_definition: "
                f"{source_column.computed_definition or 'NULL'} -> "
                f"{target_column.computed_definition or 'NULL'}"
            )
        if source_column.is_persisted != target_column.is_persisted:
            changes.append(
                f"is_persisted: {source_column.is_persisted} -> {target_column.is_persisted}"
            )

        if changes:
            differences.append(
                SchemaDifference(
                    diff_type=DiffType.MODIFIED,
                    category=DiffCategory.COLUMN,
                    schema_name=source_table.schema_name,
                    object_name=f"{source_table.table_name}.{column_name}",
                    detail="; ".join(changes),
                    source_definition=source_column,
                    target_definition=target_column,
                )
            )

    source_indexes = {index.name: index for index in source_table.indexes}
    target_indexes = {index.name: index for index in target_table.indexes}

    for index_name in sorted(target_indexes.keys() - source_indexes.keys()):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.ADDED,
                category=DiffCategory.INDEX,
                schema_name=target_table.schema_name,
                object_name=f"{target_table.table_name}.{index_name}",
                detail=f"Index {index_name} exists in target but not in source",
                target_definition=target_indexes[index_name],
            )
        )

    for index_name in sorted(source_indexes.keys() - target_indexes.keys()):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.REMOVED,
                category=DiffCategory.INDEX,
                schema_name=source_table.schema_name,
                object_name=f"{source_table.table_name}.{index_name}",
                detail=f"Index {index_name} exists in source but not in target",
                source_definition=source_indexes[index_name],
            )
        )

    for index_name in sorted(source_indexes.keys() & target_indexes.keys()):
        source_index = source_indexes[index_name]
        target_index = target_indexes[index_name]
        changes = []
        if source_index.index_type != target_index.index_type:
            changes.append(
                f"index_type: {source_index.index_type} -> {target_index.index_type}"
            )
        if source_index.is_unique != target_index.is_unique:
            changes.append(
                f"is_unique: {source_index.is_unique} -> {target_index.is_unique}"
            )
        if source_index.is_primary_key != target_index.is_primary_key:
            changes.append(
                "is_primary_key: "
                f"{source_index.is_primary_key} -> {target_index.is_primary_key}"
            )
        if source_index.key_columns != target_index.key_columns:
            changes.append(
                f"key_columns: {source_index.key_columns} -> {target_index.key_columns}"
            )
        if source_index.included_columns != target_index.included_columns:
            changes.append(
                "included_columns: "
                f"{source_index.included_columns} -> {target_index.included_columns}"
            )
        if _normalize_definition(source_index.filter_definition) != _normalize_definition(
            target_index.filter_definition
        ):
            changes.append(
                "filter_definition: "
                f"{source_index.filter_definition or 'NULL'} -> "
                f"{target_index.filter_definition or 'NULL'}"
            )
        if source_index.data_compression != target_index.data_compression:
            changes.append(
                f"data_compression: {source_index.data_compression} -> {target_index.data_compression}"
            )

        if changes:
            differences.append(
                SchemaDifference(
                    diff_type=DiffType.MODIFIED,
                    category=DiffCategory.INDEX,
                    schema_name=source_table.schema_name,
                    object_name=f"{source_table.table_name}.{index_name}",
                    detail="; ".join(changes),
                    source_definition=source_index,
                    target_definition=target_index,
                )
            )

    source_constraints = {constraint.name: constraint for constraint in source_table.constraints}
    target_constraints = {constraint.name: constraint for constraint in target_table.constraints}

    for constraint_name in sorted(target_constraints.keys() - source_constraints.keys()):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.ADDED,
                category=DiffCategory.CONSTRAINT,
                schema_name=target_table.schema_name,
                object_name=f"{target_table.table_name}.{constraint_name}",
                detail=f"Constraint {constraint_name} exists in target but not in source",
                target_definition=target_constraints[constraint_name],
            )
        )

    for constraint_name in sorted(source_constraints.keys() - target_constraints.keys()):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.REMOVED,
                category=DiffCategory.CONSTRAINT,
                schema_name=source_table.schema_name,
                object_name=f"{source_table.table_name}.{constraint_name}",
                detail=f"Constraint {constraint_name} exists in source but not in target",
                source_definition=source_constraints[constraint_name],
            )
        )

    for constraint_name in sorted(source_constraints.keys() & target_constraints.keys()):
        source_constraint = source_constraints[constraint_name]
        target_constraint = target_constraints[constraint_name]
        changes = []
        if source_constraint.constraint_type != target_constraint.constraint_type:
            changes.append(
                "constraint_type: "
                f"{source_constraint.constraint_type} -> {target_constraint.constraint_type}"
            )
        if source_constraint.columns != target_constraint.columns:
            changes.append(
                f"columns: {source_constraint.columns} -> {target_constraint.columns}"
            )
        if source_constraint.referenced_schema != target_constraint.referenced_schema:
            changes.append(
                "referenced_schema: "
                f"{source_constraint.referenced_schema} -> {target_constraint.referenced_schema}"
            )
        if source_constraint.referenced_table != target_constraint.referenced_table:
            changes.append(
                "referenced_table: "
                f"{source_constraint.referenced_table} -> {target_constraint.referenced_table}"
            )
        if source_constraint.referenced_columns != target_constraint.referenced_columns:
            changes.append(
                "referenced_columns: "
                f"{source_constraint.referenced_columns} -> {target_constraint.referenced_columns}"
            )
        if _normalize_definition(source_constraint.definition) != _normalize_definition(
            target_constraint.definition
        ):
            changes.append(
                "definition: "
                f"{source_constraint.definition or 'NULL'} -> {target_constraint.definition or 'NULL'}"
            )
        if source_constraint.delete_action != target_constraint.delete_action:
            changes.append(
                f"delete_action: {source_constraint.delete_action} -> {target_constraint.delete_action}"
            )
        if source_constraint.update_action != target_constraint.update_action:
            changes.append(
                f"update_action: {source_constraint.update_action} -> {target_constraint.update_action}"
            )
        if source_constraint.is_not_trusted != target_constraint.is_not_trusted:
            changes.append(
                f"is_not_trusted: {source_constraint.is_not_trusted} -> {target_constraint.is_not_trusted}"
            )

        if changes:
            differences.append(
                SchemaDifference(
                    diff_type=DiffType.MODIFIED,
                    category=DiffCategory.CONSTRAINT,
                    schema_name=source_table.schema_name,
                    object_name=f"{source_table.table_name}.{constraint_name}",
                    detail="; ".join(changes),
                    source_definition=source_constraint,
                    target_definition=target_constraint,
                )
            )

    differences.extend(_compare_triggers(source_table, target_table))

    return differences


def _compare_triggers(
    source_table: TableSnapshot,
    target_table: TableSnapshot,
) -> list[SchemaDifference]:
    differences: list[SchemaDifference] = []
    source_triggers = {trigger.name: trigger for trigger in source_table.triggers}
    target_triggers = {trigger.name: trigger for trigger in target_table.triggers}

    for trigger_name in sorted(target_triggers.keys() - source_triggers.keys()):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.ADDED,
                category=DiffCategory.TRIGGER,
                schema_name=target_table.schema_name,
                object_name=f"{target_table.table_name}.{trigger_name}",
                detail=f"Trigger {trigger_name} exists in target but not in source",
                target_definition=target_triggers[trigger_name],
            )
        )

    for trigger_name in sorted(source_triggers.keys() - target_triggers.keys()):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.REMOVED,
                category=DiffCategory.TRIGGER,
                schema_name=source_table.schema_name,
                object_name=f"{source_table.table_name}.{trigger_name}",
                detail=f"Trigger {trigger_name} exists in source but not in target",
                source_definition=source_triggers[trigger_name],
            )
        )

    for trigger_name in sorted(source_triggers.keys() & target_triggers.keys()):
        source_trigger = source_triggers[trigger_name]
        target_trigger = target_triggers[trigger_name]
        changes = []
        if _normalize_definition(source_trigger.definition) != _normalize_definition(
            target_trigger.definition
        ):
            changes.append("definition changed")
        if source_trigger.is_disabled != target_trigger.is_disabled:
            changes.append(
                f"is_disabled: {source_trigger.is_disabled} -> {target_trigger.is_disabled}"
            )
        if source_trigger.is_instead_of != target_trigger.is_instead_of:
            changes.append(
                f"is_instead_of: {source_trigger.is_instead_of} -> {target_trigger.is_instead_of}"
            )
        if source_trigger.events != target_trigger.events:
            changes.append(
                f"events: {source_trigger.events} -> {target_trigger.events}"
            )
        if changes:
            differences.append(
                SchemaDifference(
                    diff_type=DiffType.MODIFIED,
                    category=DiffCategory.TRIGGER,
                    schema_name=source_table.schema_name,
                    object_name=f"{source_table.table_name}.{trigger_name}",
                    detail="; ".join(changes),
                    source_definition=source_trigger,
                    target_definition=target_trigger,
                )
            )

    return differences


def _compare_sequences(
    source_sequences: dict[tuple[str, str], SequenceDef],
    target_sequences: dict[tuple[str, str], SequenceDef],
) -> list[SchemaDifference]:
    differences: list[SchemaDifference] = []
    source_keys = set(source_sequences)
    target_keys = set(target_sequences)

    for schema_name, seq_name in sorted(target_keys - source_keys):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.ADDED,
                category=DiffCategory.SEQUENCE,
                schema_name=schema_name,
                object_name=seq_name,
                detail=f"Sequence [{schema_name}].[{seq_name}] exists in target but not in source",
                target_definition=target_sequences[(schema_name, seq_name)],
            )
        )

    for schema_name, seq_name in sorted(source_keys - target_keys):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.REMOVED,
                category=DiffCategory.SEQUENCE,
                schema_name=schema_name,
                object_name=seq_name,
                detail=f"Sequence [{schema_name}].[{seq_name}] exists in source but not in target",
                source_definition=source_sequences[(schema_name, seq_name)],
            )
        )

    for schema_name, seq_name in sorted(source_keys & target_keys):
        source_seq = source_sequences[(schema_name, seq_name)]
        target_seq = target_sequences[(schema_name, seq_name)]
        changes = []
        if source_seq.data_type != target_seq.data_type:
            changes.append(f"data_type: {source_seq.data_type} -> {target_seq.data_type}")
        if source_seq.increment != target_seq.increment:
            changes.append(f"increment: {source_seq.increment} -> {target_seq.increment}")
        if source_seq.min_value != target_seq.min_value:
            changes.append(f"min_value: {source_seq.min_value} -> {target_seq.min_value}")
        if source_seq.max_value != target_seq.max_value:
            changes.append(f"max_value: {source_seq.max_value} -> {target_seq.max_value}")
        if source_seq.is_cycling != target_seq.is_cycling:
            changes.append(f"is_cycling: {source_seq.is_cycling} -> {target_seq.is_cycling}")
        if changes:
            differences.append(
                SchemaDifference(
                    diff_type=DiffType.MODIFIED,
                    category=DiffCategory.SEQUENCE,
                    schema_name=schema_name,
                    object_name=seq_name,
                    detail="; ".join(changes),
                    source_definition=source_seq,
                    target_definition=target_seq,
                )
            )

    return differences


def _compare_programmable_group(
    source_objects: dict[tuple[str, str], ProgrammableObjectSnapshot],
    target_objects: dict[tuple[str, str], ProgrammableObjectSnapshot],
    category: DiffCategory,
) -> list[SchemaDifference]:
    differences: list[SchemaDifference] = []

    source_keys = set(source_objects)
    target_keys = set(target_objects)

    for schema_name, object_name in sorted(target_keys - source_keys):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.ADDED,
                category=category,
                schema_name=schema_name,
                object_name=object_name,
                detail=f"{category.value.title()} [{schema_name}].[{object_name}] exists in target but not in source",
                target_definition=target_objects[(schema_name, object_name)],
            )
        )

    for schema_name, object_name in sorted(source_keys - target_keys):
        differences.append(
            SchemaDifference(
                diff_type=DiffType.REMOVED,
                category=category,
                schema_name=schema_name,
                object_name=object_name,
                detail=f"{category.value.title()} [{schema_name}].[{object_name}] exists in source but not in target",
                source_definition=source_objects[(schema_name, object_name)],
            )
        )

    for schema_name, object_name in sorted(source_keys & target_keys):
        source_definition = source_objects[(schema_name, object_name)].definition or ""
        target_definition = target_objects[(schema_name, object_name)].definition or ""
        if _normalize_definition(source_definition) != _normalize_definition(target_definition):
            differences.append(
                SchemaDifference(
                    diff_type=DiffType.MODIFIED,
                    category=category,
                    schema_name=schema_name,
                    object_name=object_name,
                    detail=f"{category.value.title()} definition changed",
                    source_definition=source_objects[(schema_name, object_name)],
                    target_definition=target_objects[(schema_name, object_name)],
                )
            )

    return differences


def _normalize_definition(value: str | None) -> str:
    if not value:
        return ""
    return WHITESPACE_PATTERN.sub(" ", value).strip()


def _serialize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return {key: _serialize_value(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_serialize_value(item) for item in value]
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    return value

