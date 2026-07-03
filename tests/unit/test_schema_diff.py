from __future__ import annotations

from typing import Any

from azure_sql_mcp.schema_diff import DiffCategory
from azure_sql_mcp.schema_diff import DiffType
from azure_sql_mcp.schema_diff import compare_snapshots
from azure_sql_mcp.schema_snapshot import ColumnDef
from azure_sql_mcp.schema_snapshot import ConstraintDef
from azure_sql_mcp.schema_snapshot import IndexDef
from azure_sql_mcp.schema_snapshot import ProgrammableObjectSnapshot
from azure_sql_mcp.schema_snapshot import SchemaSnapshot
from azure_sql_mcp.schema_snapshot import SequenceDef
from azure_sql_mcp.schema_snapshot import TableSnapshot
from azure_sql_mcp.schema_snapshot import TriggerDef


def make_snapshot(
    *,
    database_name: str,
    orders_total_amount_default: str,
    orders_total_amount_precision: int,
    orders_total_amount_scale: int,
    orders_total_amount_index_columns: tuple[str, ...],
    orders_total_amount_index_includes: tuple[str, ...],
    orders_view_definition: str,
    include_fk: bool,
    include_audit_log: bool,
    include_legacy: bool,
    include_customer_id: bool,
    include_old_code: bool,
    include_new_code: bool,
) -> SchemaSnapshot:
    orders_columns = [
        ColumnDef("OrderId", "int", 4, 10, 0, False, None),
        ColumnDef(
            "TotalAmount",
            "decimal",
            5,
            orders_total_amount_precision,
            orders_total_amount_scale,
            False,
            orders_total_amount_default,
        ),
    ]
    if include_old_code:
        orders_columns.append(ColumnDef("OldCode", "nvarchar", 20, 0, 0, True, None))
    if include_customer_id:
        orders_columns.append(ColumnDef("CustomerId", "int", 4, 10, 0, False, None))
    if include_new_code:
        orders_columns.append(ColumnDef("NewCode", "nvarchar", 40, 0, 0, True, "(N'X')"))

    orders_constraints = [
        ConstraintDef(
            name="PK_Orders",
            constraint_type="PRIMARY_KEY",
            columns=("OrderId",),
            referenced_schema=None,
            referenced_table=None,
            referenced_columns=None,
            definition=None,
        ),
        ConstraintDef(
            name="DF_Orders_TotalAmount",
            constraint_type="DEFAULT",
            columns=("TotalAmount",),
            referenced_schema=None,
            referenced_table=None,
            referenced_columns=None,
            definition=orders_total_amount_default,
        ),
        ConstraintDef(
            name="CK_Orders_TotalAmount",
            constraint_type="CHECK",
            columns=("TotalAmount",),
            referenced_schema=None,
            referenced_table=None,
            referenced_columns=None,
            definition="[TotalAmount] >= 0",
        ),
    ]
    if include_fk:
        orders_constraints.append(
            ConstraintDef(
                name="FK_Orders_Customers",
                constraint_type="FOREIGN_KEY",
                columns=("CustomerId",),
                referenced_schema="dbo",
                referenced_table="Customers",
                referenced_columns=("CustomerId",),
                definition=None,
            )
        )

    orders_indexes = [
        IndexDef(
            name="IX_Orders_TotalAmount",
            index_type="NONCLUSTERED",
            is_unique=False,
            is_primary_key=False,
            key_columns=orders_total_amount_index_columns,
            included_columns=orders_total_amount_index_includes,
        )
    ]

    tables = {
        ("dbo", "Orders"): TableSnapshot(
            schema_name="dbo",
            table_name="Orders",
            columns=tuple(orders_columns),
            indexes=tuple(orders_indexes),
            constraints=tuple(orders_constraints),
        ),
        ("dbo", "Customers"): TableSnapshot(
            schema_name="dbo",
            table_name="Customers",
            columns=(
                ColumnDef("CustomerId", "int", 4, 10, 0, False, None),
                ColumnDef("CustomerName", "nvarchar", 200, 0, 0, False, None),
            ),
            indexes=(),
            constraints=(
                ConstraintDef(
                    name="PK_Customers",
                    constraint_type="PRIMARY_KEY",
                    columns=("CustomerId",),
                    referenced_schema=None,
                    referenced_table=None,
                    referenced_columns=None,
                    definition=None,
                ),
            ),
        ),
    }
    if include_audit_log:
        tables[("dbo", "AuditLog")] = TableSnapshot(
            schema_name="dbo",
            table_name="AuditLog",
            columns=(
                ColumnDef("AuditId", "int", 4, 10, 0, False, None),
                ColumnDef("Message", "nvarchar", 200, 0, 0, False, None),
            ),
            indexes=(),
            constraints=(
                ConstraintDef(
                    name="PK_AuditLog",
                    constraint_type="PRIMARY_KEY",
                    columns=("AuditId",),
                    referenced_schema=None,
                    referenced_table=None,
                    referenced_columns=None,
                    definition=None,
                ),
            ),
        )
    if include_legacy:
        tables[("dbo", "Legacy")] = TableSnapshot(
            schema_name="dbo",
            table_name="Legacy",
            columns=(
                ColumnDef("LegacyId", "int", 4, 10, 0, False, None),
            ),
            indexes=(),
            constraints=(),
        )

    return SchemaSnapshot(
        database_name=database_name,
        captured_at="2026-03-30T00:00:00Z",
        tables=tables,
        views={
            ("dbo", "vw_Orders"): ProgrammableObjectSnapshot(
                schema_name="dbo",
                object_name="vw_Orders",
                object_type="VIEW",
                definition=orders_view_definition,
            )
        },
        procedures={},
        functions={},
    )


def test_compare_snapshots_detects_deterministic_schema_changes() -> None:
    source = make_snapshot(
        database_name="source",
        orders_total_amount_default="((0))",
        orders_total_amount_precision=18,
        orders_total_amount_scale=2,
        orders_total_amount_index_columns=("TotalAmount",),
        orders_total_amount_index_includes=(),
        orders_view_definition="CREATE VIEW dbo.vw_Orders AS SELECT OrderId FROM dbo.Orders",
        include_fk=False,
        include_audit_log=False,
        include_legacy=True,
        include_customer_id=False,
        include_old_code=True,
        include_new_code=False,
    )
    target = make_snapshot(
        database_name="target",
        orders_total_amount_default="((1))",
        orders_total_amount_precision=18,
        orders_total_amount_scale=4,
        orders_total_amount_index_columns=("TotalAmount", "CustomerId"),
        orders_total_amount_index_includes=("NewCode",),
        orders_view_definition=(
            "CREATE VIEW dbo.vw_Orders AS SELECT OrderId, TotalAmount FROM dbo.Orders"
        ),
        include_fk=True,
        include_audit_log=True,
        include_legacy=False,
        include_customer_id=True,
        include_old_code=False,
        include_new_code=True,
    )

    differences = compare_snapshots(source, target)

    assert [difference.category.value for difference in differences] == [
        "table",
        "table",
        "column",
        "column",
        "column",
        "column",
        "index",
        "constraint",
        "constraint",
        "view",
    ]
    assert differences[0].diff_type is DiffType.ADDED
    assert differences[0].object_name == "AuditLog"
    assert differences[1].diff_type is DiffType.REMOVED
    assert differences[1].object_name == "Legacy"
    assert any(
        difference.category is DiffCategory.COLUMN
        and difference.object_name == "Orders.TotalAmount"
        and "scale: 2 -> 4" in difference.detail
        for difference in differences
    )
    assert any(
        difference.category is DiffCategory.INDEX
        and difference.object_name == "Orders.IX_Orders_TotalAmount"
        and "key_columns: ('TotalAmount',) -> ('TotalAmount', 'CustomerId')" in difference.detail
        for difference in differences
    )
    assert any(
        difference.category is DiffCategory.CONSTRAINT
        and difference.object_name == "Orders.FK_Orders_Customers"
        and difference.diff_type is DiffType.ADDED
        for difference in differences
    )
    assert any(
        difference.category is DiffCategory.VIEW
        and difference.diff_type is DiffType.MODIFIED
        for difference in differences
    )


def test_compare_snapshots_ignores_whitespace_only_definition_changes() -> None:
    source = make_snapshot(
        database_name="source",
        orders_total_amount_default="((0))",
        orders_total_amount_precision=18,
        orders_total_amount_scale=2,
        orders_total_amount_index_columns=("TotalAmount",),
        orders_total_amount_index_includes=(),
        orders_view_definition="CREATE VIEW dbo.vw_Orders AS SELECT OrderId FROM dbo.Orders",
        include_fk=False,
        include_audit_log=False,
        include_legacy=False,
        include_customer_id=False,
        include_old_code=True,
        include_new_code=False,
    )
    target = make_snapshot(
        database_name="target",
        orders_total_amount_default="((0))",
        orders_total_amount_precision=18,
        orders_total_amount_scale=2,
        orders_total_amount_index_columns=("TotalAmount",),
        orders_total_amount_index_includes=(),
        orders_view_definition="""
            CREATE   VIEW   dbo.vw_Orders
            AS
            SELECT OrderId FROM dbo.Orders
        """,
        include_fk=False,
        include_audit_log=False,
        include_legacy=False,
        include_customer_id=False,
        include_old_code=True,
        include_new_code=False,
    )

    differences = compare_snapshots(source, target)

    assert differences == []


def _empty_snapshot(**overrides: Any) -> SchemaSnapshot:
    defaults: dict[str, Any] = {
        "database_name": "testdb",
        "captured_at": "2026-04-01T00:00:00Z",
        "tables": {},
        "views": {},
        "procedures": {},
        "functions": {},
        "sequences": None,
    }
    defaults.update(overrides)
    return SchemaSnapshot(**defaults)


def test_filtered_index_diff_detected() -> None:
    """17.1: Filtered index WHERE clause changes are detected."""
    source = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (), (
            IndexDef("IX_A", "NONCLUSTERED", False, False, ("Col1",), (),
                     filter_definition="[IsActive] = 1"),
        ), ()),
    })
    target = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (), (
            IndexDef("IX_A", "NONCLUSTERED", False, False, ("Col1",), (),
                     filter_definition="[IsActive] = 1 AND [IsDeleted] = 0"),
        ), ()),
    })

    diffs = compare_snapshots(source, target)
    assert len(diffs) == 1
    assert diffs[0].category is DiffCategory.INDEX
    assert "filter_definition" in diffs[0].detail


def test_index_compression_diff_detected() -> None:
    """17.2: Index compression changes are detected."""
    source = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (), (
            IndexDef("IX_A", "NONCLUSTERED", False, False, ("Col1",), (),
                     data_compression="NONE"),
        ), ()),
    })
    target = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (), (
            IndexDef("IX_A", "NONCLUSTERED", False, False, ("Col1",), (),
                     data_compression="PAGE"),
        ), ()),
    })

    diffs = compare_snapshots(source, target)
    assert len(diffs) == 1
    assert "data_compression: NONE -> PAGE" in diffs[0].detail


def test_fk_cascade_rules_diff_detected() -> None:
    """17.3: FK cascade action changes are detected."""
    source = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (), (), (
            ConstraintDef("FK_T_Parent", "FOREIGN_KEY", ("ParentId",),
                          "dbo", "Parent", ("Id",), None,
                          delete_action="NO_ACTION", update_action="NO_ACTION"),
        )),
    })
    target = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (), (), (
            ConstraintDef("FK_T_Parent", "FOREIGN_KEY", ("ParentId",),
                          "dbo", "Parent", ("Id",), None,
                          delete_action="CASCADE", update_action="SET_NULL"),
        )),
    })

    diffs = compare_snapshots(source, target)
    assert len(diffs) == 1
    assert "delete_action: NO_ACTION -> CASCADE" in diffs[0].detail
    assert "update_action: NO_ACTION -> SET_NULL" in diffs[0].detail


def test_nocheck_state_diff_detected() -> None:
    """17.4: WITH NOCHECK state changes are detected."""
    source = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (), (), (
            ConstraintDef("CK_T_Val", "CHECK", ("Val",), None, None, None,
                          "[Val] > 0", is_not_trusted=False),
        )),
    })
    target = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (), (), (
            ConstraintDef("CK_T_Val", "CHECK", ("Val",), None, None, None,
                          "[Val] > 0", is_not_trusted=True),
        )),
    })

    diffs = compare_snapshots(source, target)
    assert len(diffs) == 1
    assert "is_not_trusted" in diffs[0].detail


def test_sequence_add_remove_modify() -> None:
    """17.5: Sequence diffs are detected."""
    source = _empty_snapshot(sequences={
        ("dbo", "SeqA"): SequenceDef("dbo", "SeqA", "int", 1, 1, 1, 2147483647, False, 100),
        ("dbo", "SeqOld"): SequenceDef("dbo", "SeqOld", "int", 1, 1, 1, 2147483647, False, 50),
    })
    target = _empty_snapshot(sequences={
        ("dbo", "SeqA"): SequenceDef("dbo", "SeqA", "bigint", 1, 10, 1, 9223372036854775807, True, 100),
        ("dbo", "SeqNew"): SequenceDef("dbo", "SeqNew", "int", 1, 1, 1, 2147483647, False, 1),
    })

    diffs = compare_snapshots(source, target)
    categories = [(d.diff_type.value, d.object_name) for d in diffs if d.category is DiffCategory.SEQUENCE]
    assert ("added", "SeqNew") in categories
    assert ("removed", "SeqOld") in categories
    assert ("modified", "SeqA") in categories


def test_trigger_add_remove_modify() -> None:
    """17.6: Trigger diffs are detected."""
    source = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (), (), (),
            triggers=(
                TriggerDef("dbo", "T", "trg_Old", False, False, "INSERT", "CREATE TRIGGER trg_Old ON dbo.T FOR INSERT AS SELECT 1"),
                TriggerDef("dbo", "T", "trg_Mod", False, False, "INSERT", "CREATE TRIGGER trg_Mod ON dbo.T FOR INSERT AS SELECT 1"),
            )),
    })
    target = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (), (), (),
            triggers=(
                TriggerDef("dbo", "T", "trg_Mod", False, True, "INSERT, UPDATE", "CREATE TRIGGER trg_Mod ON dbo.T FOR INSERT AS SELECT 2"),
                TriggerDef("dbo", "T", "trg_New", False, False, "DELETE", "CREATE TRIGGER trg_New ON dbo.T FOR DELETE AS SELECT 1"),
            )),
    })

    diffs = compare_snapshots(source, target)
    trigger_diffs = [d for d in diffs if d.category is DiffCategory.TRIGGER]
    assert len(trigger_diffs) == 3
    types = {(d.diff_type.value, d.object_name.split(".")[-1]) for d in trigger_diffs}
    assert ("added", "trg_New") in types
    assert ("removed", "trg_Old") in types
    assert ("modified", "trg_Mod") in types


def test_computed_column_diff_detected() -> None:
    """17.7: Computed column changes are detected."""
    source = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
            ColumnDef("FullName", "nvarchar", -1, 0, 0, True, None,
                      computed_definition="[FirstName] + ' ' + [LastName]", is_persisted=False),
        ), (), ()),
    })
    target = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
            ColumnDef("FullName", "nvarchar", -1, 0, 0, True, None,
                      computed_definition="[FirstName] + ' ' + [LastName]", is_persisted=True),
        ), (), ()),
    })

    diffs = compare_snapshots(source, target)
    assert len(diffs) == 1
    assert "is_persisted: False -> True" in diffs[0].detail
