from __future__ import annotations

from azure_sql_mcp.ddl_generator import generate_migration_script
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


def test_generate_migration_script_orders_and_renders_conservative_ddl() -> None:
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
    script = generate_migration_script(differences, source, target)

    assert script.startswith("SET XACT_ABORT ON;\nBEGIN TRANSACTION;")
    assert script.rstrip().endswith("COMMIT TRANSACTION;")
    assert "PRINT N'Create table [dbo].[AuditLog]';" in script
    assert "CREATE TABLE [dbo].[AuditLog]" in script
    assert "ALTER TABLE [dbo].[Orders] ADD [CustomerId] int NOT NULL;" in script
    assert "ALTER TABLE [dbo].[Orders] ADD [NewCode] nvarchar(20) NULL DEFAULT (N'X');" in script
    assert "ALTER TABLE [dbo].[Orders] DROP COLUMN [OldCode];" in script
    assert "ALTER TABLE [dbo].[Orders] ALTER COLUMN [TotalAmount] decimal(18, 4) NOT NULL;" in script
    assert "DROP INDEX [IX_Orders_TotalAmount] ON [dbo].[Orders];" in script
    assert "CREATE NONCLUSTERED INDEX [IX_Orders_TotalAmount] ON [dbo].[Orders]" in script
    assert "ALTER VIEW dbo.vw_Orders AS SELECT OrderId, TotalAmount FROM dbo.Orders;" in script
    assert "ALTER TABLE [dbo].[Orders] ADD CONSTRAINT [FK_Orders_Customers] FOREIGN KEY ([CustomerId]) REFERENCES [dbo].[Customers] ([CustomerId]);" in script
    assert "DROP TABLE [dbo].[Legacy];" in script

    assert script.index("DROP CONSTRAINT [DF_Orders_TotalAmount]") < script.index(
        "DROP INDEX [IX_Orders_TotalAmount]"
    )
    assert script.index("DROP INDEX [IX_Orders_TotalAmount]") < script.index(
        "DROP TABLE [dbo].[Legacy]"
    )
    assert script.index("DROP TABLE [dbo].[Legacy]") < script.index("CREATE TABLE [dbo].[AuditLog]")
    assert script.index("CREATE TABLE [dbo].[AuditLog]") < script.index(
        "ALTER TABLE [dbo].[Orders] ADD [CustomerId]"
    )
    assert script.index("ALTER TABLE [dbo].[Orders] ADD [CustomerId]") < script.index(
        "ALTER VIEW dbo.vw_Orders"
    )
    assert script.index("ALTER VIEW dbo.vw_Orders") < script.index(
        "CREATE NONCLUSTERED INDEX [IX_Orders_TotalAmount]"
    )
    assert script.index("CREATE NONCLUSTERED INDEX [IX_Orders_TotalAmount]") < script.index(
        "ALTER TABLE [dbo].[Orders] ADD CONSTRAINT [FK_Orders_Customers]"
    )


def _empty_snapshot(**overrides) -> SchemaSnapshot:
    defaults = {
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


def test_filtered_index_ddl_includes_where_clause() -> None:
    """17.1: Filtered index DDL includes WHERE clause."""
    source = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
        ), (), ()),
    })
    target = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
        ), (
            IndexDef("IX_Active", "NONCLUSTERED", False, False, ("Id",), (),
                     filter_definition="[IsActive] = 1"),
        ), ()),
    })

    diffs = compare_snapshots(source, target)
    script = generate_migration_script(diffs, source, target)

    assert "WHERE [IsActive] = 1" in script
    assert "CREATE NONCLUSTERED INDEX [IX_Active]" in script


def test_index_compression_ddl_includes_data_compression() -> None:
    """17.2: Index DDL includes DATA_COMPRESSION."""
    source = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
        ), (), ()),
    })
    target = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
        ), (
            IndexDef("IX_Compressed", "NONCLUSTERED", False, False, ("Id",), (),
                     data_compression="PAGE"),
        ), ()),
    })

    diffs = compare_snapshots(source, target)
    script = generate_migration_script(diffs, source, target)

    assert "WITH (DATA_COMPRESSION = PAGE)" in script


def test_fk_cascade_ddl_includes_on_delete_update() -> None:
    """17.3: FK DDL includes ON DELETE/UPDATE cascade actions."""
    source = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
            ColumnDef("ParentId", "int", 4, 10, 0, False, None),
        ), (), ()),
        ("dbo", "Parent"): TableSnapshot("dbo", "Parent", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
        ), (), (
            ConstraintDef("PK_Parent", "PRIMARY_KEY", ("Id",), None, None, None, None),
        )),
    })
    target = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
            ColumnDef("ParentId", "int", 4, 10, 0, False, None),
        ), (), (
            ConstraintDef("FK_T_Parent", "FOREIGN_KEY", ("ParentId",),
                          "dbo", "Parent", ("Id",), None,
                          delete_action="CASCADE", update_action="SET_NULL"),
        )),
        ("dbo", "Parent"): TableSnapshot("dbo", "Parent", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
        ), (), (
            ConstraintDef("PK_Parent", "PRIMARY_KEY", ("Id",), None, None, None, None),
        )),
    })

    diffs = compare_snapshots(source, target)
    script = generate_migration_script(diffs, source, target)

    assert "ON DELETE CASCADE" in script
    assert "ON UPDATE SET NULL" in script


def test_nocheck_constraint_ddl_uses_with_nocheck() -> None:
    """17.4: Untrusted constraints use WITH NOCHECK."""
    source = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
            ColumnDef("Val", "int", 4, 10, 0, False, None),
        ), (), ()),
    })
    target = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
            ColumnDef("Val", "int", 4, 10, 0, False, None),
        ), (), (
            ConstraintDef("CK_T_Val", "CHECK", ("Val",), None, None, None,
                          "[Val] > 0", is_not_trusted=True),
        )),
    })

    diffs = compare_snapshots(source, target)
    script = generate_migration_script(diffs, source, target)

    assert "WITH NOCHECK ADD CONSTRAINT [CK_T_Val]" in script


def test_sequence_create_and_drop_ddl() -> None:
    """17.5: Sequence create/drop DDL."""
    source = _empty_snapshot(sequences={
        ("dbo", "SeqOld"): SequenceDef("dbo", "SeqOld", "int", 1, 1, 1, 2147483647, False, 50),
    })
    target = _empty_snapshot(sequences={
        ("dbo", "SeqNew"): SequenceDef("dbo", "SeqNew", "bigint", 100, 5, 1, 9223372036854775807, True, 100),
    })

    diffs = compare_snapshots(source, target)
    script = generate_migration_script(diffs, source, target)

    assert "DROP SEQUENCE [dbo].[SeqOld];" in script
    assert "CREATE SEQUENCE [dbo].[SeqNew] AS bigint START WITH 100 INCREMENT BY 5" in script
    assert "CYCLE" in script
    # Drops come before creates
    assert script.index("DROP SEQUENCE") < script.index("CREATE SEQUENCE")


def test_trigger_create_and_drop_ddl() -> None:
    """17.6: Trigger create/drop DDL."""
    source = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (), (), (),
            triggers=(
                TriggerDef("dbo", "T", "trg_Old", False, False, "INSERT",
                           "CREATE TRIGGER trg_Old ON dbo.T FOR INSERT AS SELECT 1"),
            )),
    })
    target = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (), (), (),
            triggers=(
                TriggerDef("dbo", "T", "trg_New", False, False, "DELETE",
                           "CREATE TRIGGER trg_New ON dbo.T FOR DELETE AS SELECT 1"),
            )),
    })

    diffs = compare_snapshots(source, target)
    script = generate_migration_script(diffs, source, target)

    assert "DROP TRIGGER [dbo].[trg_Old];" in script
    assert "CREATE TRIGGER trg_New ON dbo.T FOR DELETE AS SELECT 1;" in script
    # Triggers drop before create
    assert script.index("DROP TRIGGER") < script.index("CREATE TRIGGER trg_New")


def test_computed_column_ddl_includes_as_expression() -> None:
    """17.7: Computed column DDL includes AS expression and PERSISTED."""
    source = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
        ), (), ()),
    })
    target = _empty_snapshot(tables={
        ("dbo", "T"): TableSnapshot("dbo", "T", (
            ColumnDef("Id", "int", 4, 10, 0, False, None),
            ColumnDef("FullName", "nvarchar", -1, 0, 0, True, None,
                      computed_definition="[First] + ' ' + [Last]", is_persisted=True),
        ), (), ()),
    })

    diffs = compare_snapshots(source, target)
    script = generate_migration_script(diffs, source, target)

    assert "[FullName] AS [First] + ' ' + [Last] PERSISTED" in script
