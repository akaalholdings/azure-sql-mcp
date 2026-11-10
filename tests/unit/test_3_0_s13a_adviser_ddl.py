"""S13a F5: adviser DDL for a clustered columnstore table has no ONLINE build option.

Observed on Azure SQL Database: on a table whose clustered index is columnstore
(sys.indexes type 5), CREATE NONCLUSTERED INDEX ... WITH (ONLINE = ON) fails
with error 35352 and ALTER TABLE ... ADD CONSTRAINT ... WITH (ONLINE = ON) fails
with error 1750; ALTER INDEX ... REBUILD WITH (ONLINE = ON) works. A rowstore
table with a nonclustered columnstore index (type 6) accepts all three online.
DDL that carries a refused option fails when the DBA runs it; DDL that drops
the option without need blocks writers for the whole build.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from azure_sql_mcp.index_advisor import build_index_advice
from azure_sql_mcp.index_ddl import without_online_create
from azure_sql_mcp.index_review import _actionable_subject_sql
from azure_sql_mcp.index_review import _columnstore_tables
from tests.unit.test_index_advisor import _index
from tests.unit.test_index_advisor import _inputs
from tests.unit.test_index_advisor import _orders_indexes
from tests.unit.test_index_advisor import _query


def _recs(report: dict[str, Any], action: str) -> list[dict[str, Any]]:
    return [rec for rec in report["recommendations"] if rec["action"] == action]


def _nonclustered_columnstore(*, disabled: bool = False) -> Any:
    ncci = _index("Sales", "Orders", "NCCI_Orders", ("OrderDate",), index_id=7, type_code=6)
    return replace(ncci, index_type="NONCLUSTERED COLUMNSTORE", is_disabled=disabled)


def _clustered_columnstore() -> Any:
    cci = _index("Sales", "Orders", "CCI_Orders", ("OrderDate",), index_id=8, type_code=5)
    return replace(cci, index_type="CLUSTERED COLUMNSTORE")


@pytest.mark.parametrize(
    ("ddl", "expected"),
    [
        (
            "CREATE NONCLUSTERED INDEX [IX_A] ON [dbo].[T] ([a] ASC) WITH (ONLINE = ON);",
            "CREATE NONCLUSTERED INDEX [IX_A] ON [dbo].[T] ([a] ASC);",
        ),
        (
            "CREATE NONCLUSTERED INDEX [IX_A] ON [dbo].[T] ([a] ASC)\n"
            "WITH (DROP_EXISTING = ON, ONLINE = ON, PAD_INDEX = OFF)\nON [PRIMARY];",
            "CREATE NONCLUSTERED INDEX [IX_A] ON [dbo].[T] ([a] ASC)\n"
            "WITH (DROP_EXISTING = ON, PAD_INDEX = OFF)\nON [PRIMARY];",
        ),
        (
            "CREATE UNIQUE NONCLUSTERED INDEX [IX_A] ON [dbo].[T] ([a] ASC) WITH (DATA_COMPRESSION "
            "= PAGE, ONLINE = ON (WAIT_AT_LOW_PRIORITY (MAX_DURATION = 1 MINUTES, "
            "ABORT_AFTER_WAIT = SELF)), MAXDOP = 2);",
            "CREATE UNIQUE NONCLUSTERED INDEX [IX_A] ON [dbo].[T] ([a] ASC) WITH (DATA_COMPRESSION "
            "= PAGE, MAXDOP = 2);",
        ),
        (
            "-- CREATE NONCLUSTERED INDEX [IX_A] ON [dbo].[T] ([a] ASC)\n"
            "-- WITH (ONLINE = ON, PAD_INDEX = OFF)\n-- ON [PRIMARY];",
            "-- CREATE NONCLUSTERED INDEX [IX_A] ON [dbo].[T] ([a] ASC)\n"
            "-- WITH (PAD_INDEX = OFF)\n-- ON [PRIMARY];",
        ),
        (
            # A ';' inside a filter literal does not end the statement.
            "CREATE NONCLUSTERED INDEX [IX_A] ON [dbo].[T] ([a] ASC) WHERE [b] = 'x;y' "
            "WITH (ONLINE = ON) ON [PRIMARY];",
            "CREATE NONCLUSTERED INDEX [IX_A] ON [dbo].[T] ([a] ASC) WHERE [b] = 'x;y' "
            "ON [PRIMARY];",
        ),
        (
            # Error 1750 on a clustered columnstore table.
            "ALTER TABLE [dbo].[T] ADD CONSTRAINT [UQ_T_a] UNIQUE NONCLUSTERED ([a] ASC) "
            "WITH (ONLINE = ON);",
            "ALTER TABLE [dbo].[T] ADD CONSTRAINT [UQ_T_a] UNIQUE NONCLUSTERED ([a] ASC);",
        ),
        (
            "ALTER TABLE [dbo].[T] ADD CONSTRAINT [PK_T] PRIMARY KEY NONCLUSTERED ([a] ASC) "
            "WITH (FILLFACTOR = 90, ONLINE = ON);",
            "ALTER TABLE [dbo].[T] ADD CONSTRAINT [PK_T] PRIMARY KEY NONCLUSTERED ([a] ASC) "
            "WITH (FILLFACTOR = 90);",
        ),
    ],
)
def test_online_option_is_removed_from_create_index_and_add_constraint(
    ddl: str, expected: str
) -> None:
    assert without_online_create(ddl) == expected


def test_rebuild_and_drop_statements_keep_their_form() -> None:
    # A rebuild works online on a clustered columnstore table; a DROP has no option.
    untouched = (
        "DROP INDEX [IX_A] ON [dbo].[T];\n"
        "ALTER INDEX [IX_B] ON [dbo].[T] REBUILD WITH (ONLINE = ON);\n"
        "ALTER INDEX [CCI_T] ON [dbo].[T] REBUILD WITH (ONLINE = ON);\n"
        "ALTER TABLE [dbo].[T] DROP CONSTRAINT [UQ_T_a];\n"
        "ALTER TABLE [dbo].[T] REBUILD WITH (ONLINE = ON);"
    )
    mixed = untouched + "\nCREATE INDEX [IX_B] ON [dbo].[T] ([b]) WITH (ONLINE = ON);"

    assert without_online_create(untouched) == untouched
    assert without_online_create(mixed) == untouched + "\nCREATE INDEX [IX_B] ON [dbo].[T] ([b]);"


def _widen_and_drop(existing: list[Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    unused = _index("Sales", "Orders", "IX_Orders_Status", ("Status",), index_id=5, reads=0)
    report = build_index_advice(
        _inputs(
            [_query(1, "seek_residual_lookup_sort.xml", cpu_us=9_000_000)],
            [*existing, unused],
            references={("sales", "orders", "ix_orders_status"): 0},
            window_days=40,
        )
    )
    return _recs(report, "widen_index")[0], _recs(report, "drop_index")[0]


def test_adviser_ddl_on_a_clustered_columnstore_table_is_offline_and_says_so() -> None:
    widen, drop = _widen_and_drop([*_orders_indexes(), _clustered_columnstore()])

    for rec in (widen, drop):
        assert "ONLINE" not in (rec["ddl"] or "")
        assert "ONLINE" not in rec["rollback_ddl"]
        assert rec["offline_build"]["reason"] == "clustered_columnstore_table"
        assert "clustered columnstore" in rec["offline_build"]["note"]
        assert "blocks all writes" in rec["offline_build"]["note"]
    assert "CREATE NONCLUSTERED INDEX" in widen["ddl"] and "DROP_EXISTING = ON" in widen["ddl"]
    assert "CREATE NONCLUSTERED INDEX" in drop["rollback_ddl"]


@pytest.mark.parametrize(
    "extra",
    [[], [_nonclustered_columnstore()], [_nonclustered_columnstore(disabled=True)]],
    ids=["rowstore_only", "nonclustered_columnstore", "disabled_nonclustered_columnstore"],
)
def test_adviser_ddl_stays_online_without_a_clustered_columnstore_index(
    extra: list[Any],
) -> None:
    widen, drop = _widen_and_drop([*_orders_indexes(), *extra])

    assert widen["ddl"].count("ONLINE = ON") == 1
    assert drop["rollback_ddl"].count("ONLINE = ON") == 1
    assert "offline_build" not in widen and "offline_build" not in drop


def _review_subjects() -> list[dict[str, Any]]:
    nonclustered_columnstore = {
        "subject_kind": "existing_index",
        "subject_id": "index:2:7",
        "schema_name": "dw",
        "table_name": "DimUser",
        "state": "keep",
        "definition": {
            "index_type": "NONCLUSTERED COLUMNSTORE",
            "reversible_definition": {"index_type_code": 6, "is_disabled": False},
        },
    }
    columnstore = {
        "subject_kind": "existing_index",
        "subject_id": "index:1:1",
        "schema_name": "dw",
        "table_name": "FactVote",
        "state": "keep",
        "definition": {
            "index_type": "CLUSTERED COLUMNSTORE",
            "reversible_definition": {"index_type_code": 5, "is_disabled": False},
        },
    }
    create = {
        "subject_kind": "missing_index",
        "subject_id": "missing:1",
        "candidate_fingerprint": "0123456789abcdef0123",
        "schema_name": "dw",
        "table_name": "FactVote",
        "state": "create_candidate",
        "key_columns": ["PostKey"],
        "include_columns": ["VoteTypeKey"],
    }
    rowstore = {**create, "subject_id": "missing:2", "table_name": "DimUser"}
    return [columnstore, create, rowstore, nonclustered_columnstore]


def test_index_review_create_candidate_is_offline_only_on_a_clustered_columnstore_table() -> None:
    columnstore, create, rowstore, nonclustered_columnstore = _review_subjects()
    # dw.DimUser has a nonclustered columnstore index only: its build stays online.
    tables = _columnstore_tables([columnstore, create, rowstore, nonclustered_columnstore])

    assert tables == {("dw", "FactVote")}
    offline_ddl, _ = _actionable_subject_sql(create, tables)
    online_ddl, _ = _actionable_subject_sql(rowstore, tables)
    assert offline_ddl is not None and "ONLINE" not in offline_ddl.splitlines()[-1]
    assert "offline" in offline_ddl and "blocks all writes" in offline_ddl
    assert all(line.startswith("--") for line in offline_ddl.splitlines())
    assert online_ddl is not None and online_ddl.endswith("WITH (ONLINE = ON);")
