"""S13b-1 A: a nonclustered columnstore index as a leased test index.

A warehouse aggregate over a wide rowstore fact table is often fixed by a
nonclustered columnstore index, not by a B-tree. The index benchmark could
build B-trees only, so that design was never measured. The same candidate
type now carries the kind, and the DDL, the definition fingerprint, the
generated name and the check of the built index follow it. The refusals run
before any query: the engine allows one columnstore index per table, and
some column types cannot be held in one.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from azure_sql_mcp.index_metadata import TABLE_COLUMN_FACTS_SQL
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import IndexKeyColumn
from azure_sql_mcp.index_metadata import TableColumnFacts
from azure_sql_mcp.index_metadata import collect_table_column_facts
from azure_sql_mcp.index_metadata import is_test_index_name
from azure_sql_mcp.index_optimizer import INDEX_KIND_NONCLUSTERED_COLUMNSTORE
from azure_sql_mcp.index_optimizer import INDEX_KIND_ROWSTORE
from azure_sql_mcp.index_optimizer import INDEX_KIND_VIEW_CLUSTERED
from azure_sql_mcp.index_optimizer import IndexCandidate
from azure_sql_mcp.index_optimizer import build_index_candidate_statement
from azure_sql_mcp.index_optimizer import build_test_index_statement
from azure_sql_mcp.index_optimizer import columnstore_index_blockers
from azure_sql_mcp.index_optimizer import expected_index_definition_matches
from azure_sql_mcp.server import generated_test_index_name


def _ncci(**fields: Any) -> IndexCandidate:
    values: dict[str, Any] = {
        "schema": "dw",
        "table": "FactSale",
        "key_columns": ("DateKey", "ProductKey", "Amount"),
        "include_columns": (),
        "index_name": "IX_Testing_FactSale_DateKey_ProductKey_Amount_0a1b2c3d",
        "index_kind": INDEX_KIND_NONCLUSTERED_COLUMNSTORE,
    }
    values.update(fields)
    return IndexCandidate(**values)


def _built(candidate: IndexCandidate, **fields: Any) -> ExistingIndex:
    """The catalog row set of a built columnstore index: its columns are nonkey columns."""

    values: dict[str, Any] = {
        "schema": candidate.schema,
        "table": candidate.table,
        "index_id": 7,
        "name": str(candidate.index_name),
        "index_type": "NONCLUSTERED COLUMNSTORE",
        "index_type_code": 6,
        "key_columns": (),
        "include_columns": tuple(candidate.key_columns),
        "filter_definition": None,
        "is_unique": False,
        "data_space_name": "PRIMARY",
        "data_space_type": "ROWS_FILEGROUP",
        "partition_compression": ((1, "COLUMNSTORE"),),
    }
    values.update(fields)
    return ExistingIndex(**values)


def _column(name: str, type_name: str = "int", **fields: Any) -> TableColumnFacts:
    return TableColumnFacts(name=name, type_name=type_name, **fields)


FACT_COLUMNS = (
    _column("SaleKey", "bigint"),
    _column("DateKey"),
    _column("ProductKey"),
    _column("Amount", "decimal"),
    _column("Note", "nvarchar", max_length=-1),
    _column("Payload", "xml"),
    _column("RowVer", "timestamp"),
    _column("Margin", "decimal", is_computed=True),
    _column("Promo", "int", is_sparse=True),
    _column("Region", "geography"),
    _column("Attributes", "json"),
    _column("Embedding", "vector", max_length=20),
    _column("ObjectName", "sysname", max_length=256),
)


# --- DDL ---------------------------------------------------------------------


def test_columnstore_ddl_lists_the_columns_and_builds_online() -> None:
    ddl = build_test_index_statement(_ncci(), online=True, maxdop=4)

    assert ddl == (
        "CREATE NONCLUSTERED COLUMNSTORE INDEX "
        "[IX_Testing_FactSale_DateKey_ProductKey_Amount_0a1b2c3d] "
        "ON [dw].[FactSale] ([DateKey], [ProductKey], [Amount]) "
        "WITH (ONLINE = ON (WAIT_AT_LOW_PRIORITY (MAX_DURATION = 1 MINUTES, "
        "ABORT_AFTER_WAIT = SELF)), MAXDOP = 4);"
    )
    assert " ASC" not in ddl and "INCLUDE" not in ddl


def test_an_online_columnstore_build_waits_at_low_priority_as_a_rowstore_build_does() -> None:
    # Observed on Azure SQL Database: the engine accepts the clause for this
    # statement. Without it the build can head a blocking chain on its last lock.
    wait = (
        "ONLINE = ON (WAIT_AT_LOW_PRIORITY (MAX_DURATION = 1 MINUTES, ABORT_AFTER_WAIT = SELF))"
    )
    rowstore = replace(_ncci(), index_kind=INDEX_KIND_ROWSTORE)

    assert wait in build_test_index_statement(_ncci(), online=True)
    assert wait in build_test_index_statement(rowstore, online=True)
    assert "MAX_DURATION = 5 MINUTES" in build_index_candidate_statement(
        _ncci(), online=True, low_priority_wait_minutes=5
    )
    assert "WAIT_AT_LOW_PRIORITY" not in build_index_candidate_statement(
        _ncci(), online=False, low_priority_wait_minutes=5
    )


def test_columnstore_ddl_carries_filter_compression_and_partition_alignment() -> None:
    candidate = _ncci(
        filter_definition="[DateKey] >= 20240101",
        compression="COLUMNSTORE_ARCHIVE",
        data_space_name="ps_FactSale",
        partition_columns=("DateKey",),
        partition_scheme_name="ps_FactSale",
    )

    ddl = build_test_index_statement(candidate, online=False, allow_offline=True)

    assert ddl.endswith(
        "([DateKey], [ProductKey], [Amount]) WHERE [DateKey] >= 20240101 "
        "WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE) ON [ps_FactSale]([DateKey]);"
    )
    assert "ONLINE" not in ddl


def test_an_offline_columnstore_build_needs_the_policy_as_a_rowstore_build_does() -> None:
    with pytest.raises(PermissionError, match="offline"):
        build_test_index_statement(_ncci(), online=False, allow_offline=False)


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"key_columns": ()}, "at least one column"),
        ({"key_columns": ("DateKey DESC",)}, "ASC or DESC"),
        ({"include_columns": ("Amount",)}, "INCLUDE"),
        ({"is_unique": True}, "unique"),
        ({"compression": "PAGE"}, "COLUMNSTORE"),
        ({"key_columns": ("DateKey", "[datekey]")}, "twice"),
        ({"filter_definition": "DateKey IN (SELECT 1)"}, "filtered index predicate"),
    ],
)
def test_columnstore_ddl_refuses_what_the_engine_syntax_does_not_have(
    fields: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        build_index_candidate_statement(_ncci(**fields))


def test_rowstore_compression_names_are_still_refused_for_a_rowstore_index() -> None:
    rowstore = _ncci(index_kind=INDEX_KIND_ROWSTORE, compression="COLUMNSTORE")

    with pytest.raises(ValueError, match="rowstore index compression"):
        build_index_candidate_statement(rowstore)


def test_a_clustered_index_is_never_built_through_the_test_index_path() -> None:
    # The unique clustered kind exists for a leased test view only. On a table
    # it would rebuild the whole table.
    clustered = _ncci(index_kind=INDEX_KIND_VIEW_CLUSTERED, is_unique=True)

    with pytest.raises(ValueError, match="test view"):
        build_test_index_statement(clustered, online=False, allow_offline=True)


def test_an_unknown_index_kind_is_refused() -> None:
    with pytest.raises(ValueError, match="index_kind"):
        build_index_candidate_statement(_ncci(index_kind="clustered_columnstore"))


# --- fingerprint and name -----------------------------------------------------


def test_the_kind_is_part_of_the_definition_fingerprint() -> None:
    columnstore = _ncci()
    rowstore = replace(columnstore, index_kind=INDEX_KIND_ROWSTORE)

    assert columnstore.definition_fingerprint != rowstore.definition_fingerprint
    assert columnstore.as_dict()["index_kind"] == "nonclustered_columnstore"
    # The default compression of a columnstore index is COLUMNSTORE.
    assert (
        replace(columnstore, compression="COLUMNSTORE").definition_fingerprint
        == columnstore.definition_fingerprint
    )
    assert (
        replace(columnstore, compression="COLUMNSTORE_ARCHIVE").definition_fingerprint
        != columnstore.definition_fingerprint
    )


def test_a_rowstore_fingerprint_is_the_one_it_had_before_the_kind_existed() -> None:
    # Open leases and replayed requests compare this value.
    rowstore = IndexCandidate(
        schema="dbo",
        table="Orders",
        key_columns=("CustomerId", "OrderDate DESC"),
        include_columns=("Total",),
    )

    assert rowstore.index_kind == INDEX_KIND_ROWSTORE
    assert rowstore.definition_fingerprint == (
        "6da9a26ce074b6a1dceca153cc700865b99cfd363d2030910ac28257d897e56c"
    )


def test_the_existing_generator_names_a_columnstore_test_index() -> None:
    candidate = _ncci(index_name=None)

    name = generated_test_index_name("IX_Testing_", candidate)

    assert name == (
        "IX_Testing_FactSale_DateKey_ProductKey_Amount_" + candidate.definition_fingerprint[:8]
    )
    assert is_test_index_name(name)
    # A B-tree on the same columns is another definition, so another name.
    assert name != generated_test_index_name(
        "IX_Testing_", replace(candidate, index_kind=INDEX_KIND_ROWSTORE)
    )


# --- the built index against the request ---------------------------------------


def test_a_built_columnstore_index_matches_its_request() -> None:
    candidate = _ncci()

    assert expected_index_definition_matches(_built(candidate), candidate)
    # Column order has no meaning in a columnstore index.
    assert expected_index_definition_matches(
        _built(candidate, include_columns=("Amount", "DateKey", "ProductKey")), candidate
    )


@pytest.mark.parametrize(
    "catalog",
    [
        {"index_type": "NONCLUSTERED", "index_type_code": 2},
        {"index_type": "CLUSTERED COLUMNSTORE", "index_type_code": 5},
        {"include_columns": ("DateKey", "ProductKey")},
        {"include_columns": ("DateKey", "ProductKey", "Amount", "CustomerKey")},
        {"filter_definition": "([DateKey]>=(20240101))"},
        {"is_disabled": True},
        {"partition_compression": ((1, "COLUMNSTORE_ARCHIVE"),)},
        {"name": "NCCI_FactSale"},
    ],
    ids=[
        "b_tree",
        "clustered_columnstore",
        "column_missing",
        "column_added",
        "filter_added",
        "disabled",
        "other_compression",
        "other_name",
    ],
)
def test_a_different_index_in_the_catalog_is_not_taken_for_the_test_index(
    catalog: dict[str, Any],
) -> None:
    # The lease drops only what it can prove it built.
    candidate = _ncci()

    assert not expected_index_definition_matches(_built(candidate, **catalog), candidate)


def test_a_filtered_archive_columnstore_index_matches_the_catalog_spelling() -> None:
    candidate = _ncci(
        filter_definition="[DateKey] >= 20240101", compression="COLUMNSTORE_ARCHIVE"
    )
    built = _built(
        candidate,
        filter_definition="([DateKey]>=(20240101))",
        partition_compression=((1, "COLUMNSTORE_ARCHIVE"),),
    )

    assert expected_index_definition_matches(built, candidate)


def test_an_aligned_columnstore_index_may_hold_the_partitioning_column_it_did_not_list() -> None:
    # The engine adds the partitioning column to an aligned index.
    candidate = _ncci(
        key_columns=("ProductKey", "Amount"),
        data_space_name="ps_FactSale",
        partition_columns=("DateKey",),
        partition_scheme_name="ps_FactSale",
        partition_function_name="pf_FactSale",
    )
    placement: dict[str, Any] = {
        "data_space_name": "ps_FactSale",
        "data_space_type": "PARTITION_SCHEME",
        "partition_scheme_name": "ps_FactSale",
        "partition_function_name": "pf_FactSale",
        "partition_columns": ("DateKey",),
        "partition_compression": ((1, "COLUMNSTORE"), (2, "COLUMNSTORE")),
    }

    assert expected_index_definition_matches(
        _built(candidate, include_columns=("ProductKey", "Amount", "DateKey"), **placement),
        candidate,
    )
    assert expected_index_definition_matches(
        _built(candidate, include_columns=("ProductKey", "Amount"), **placement), candidate
    )
    assert not expected_index_definition_matches(
        _built(candidate, include_columns=("ProductKey", "Amount", "SaleKey"), **placement),
        candidate,
    )


def test_a_rowstore_request_does_not_match_a_columnstore_index_of_that_name() -> None:
    candidate = _ncci()
    rowstore_request = replace(candidate, index_kind=INDEX_KIND_ROWSTORE)

    assert not expected_index_definition_matches(_built(candidate), rowstore_request)


# --- refusals before any run ----------------------------------------------------


def _existing(name: str, index_type: str, type_code: int, **fields: Any) -> ExistingIndex:
    return ExistingIndex(
        schema="dw",
        table="FactSale",
        index_id=type_code,
        name=name,
        index_type=index_type,
        index_type_code=type_code,
        key_columns=(IndexKeyColumn("SaleKey"),) if type_code in {1, 2} else (),
        **fields,
    )


def test_a_table_without_a_columnstore_index_and_with_plain_columns_is_accepted() -> None:
    pk = _existing("PK_FactSale", "CLUSTERED", 1, is_unique=True)

    assert columnstore_index_blockers(
        _ncci(), existing_indexes=[pk], table_columns=FACT_COLUMNS
    ) == ()


@pytest.mark.parametrize(
    ("index_type", "type_code", "disabled"),
    [
        ("CLUSTERED COLUMNSTORE", 5, False),
        ("NONCLUSTERED COLUMNSTORE", 6, False),
        ("NONCLUSTERED COLUMNSTORE", 6, True),
    ],
)
def test_a_second_columnstore_index_on_a_table_is_refused(
    index_type: str, type_code: int, disabled: bool
) -> None:
    current = _existing("CS_FactSale", index_type, type_code, is_disabled=disabled)

    blockers = columnstore_index_blockers(
        _ncci(), existing_indexes=[current], table_columns=FACT_COLUMNS
    )

    assert len(blockers) == 1
    assert "dw.FactSale already has the columnstore index CS_FactSale" in blockers[0]
    assert "one columnstore index" in blockers[0] and "35339" in blockers[0]


def test_a_columnstore_index_of_another_table_does_not_block() -> None:
    other = replace(_existing("CS_Other", "NONCLUSTERED COLUMNSTORE", 6), table="FactReturn")

    assert columnstore_index_blockers(
        _ncci(), existing_indexes=[other], table_columns=FACT_COLUMNS
    ) == ()


@pytest.mark.parametrize(
    ("column", "reason", "engine_error"),
    [
        ("Note", "nvarchar(max)", "35343"),
        ("Payload", "xml", "35343"),
        ("RowVer", "timestamp", "35343"),
        ("Margin", "computed column", "35307"),
        ("Promo", "sparse column", "35309"),
        ("Region", "geography", "35343"),
        ("Attributes", "json", "35343"),
        ("NoSuchColumn", "does not exist", ""),
    ],
)
def test_a_column_a_nonclustered_columnstore_index_cannot_hold_is_named(
    column: str, reason: str, engine_error: str
) -> None:
    blockers = columnstore_index_blockers(
        _ncci(key_columns=("DateKey", column)), existing_indexes=[], table_columns=FACT_COLUMNS
    )

    assert len(blockers) == 1
    assert column in blockers[0] and reason in blockers[0] and engine_error in blockers[0]


def test_a_vector_and_a_sysname_column_are_accepted() -> None:
    # Observed on Azure SQL Database: the engine builds the index on both.
    assert columnstore_index_blockers(
        _ncci(key_columns=("DateKey", "Embedding", "ObjectName")),
        existing_indexes=[],
        table_columns=FACT_COLUMNS,
    ) == ()


def test_the_column_read_names_a_system_type_by_its_own_name() -> None:
    # vector and json are system types stored as another type: the base type
    # join read vector as varbinary. Only an alias type reads as its base type.
    sql = " ".join(TABLE_COLUMN_FACTS_SQL.split())

    assert (
        "CASE WHEN ut.is_user_defined = 1 AND ut.is_assembly_type = 0 "
        "THEN COALESCE(bt.name, ut.name) ELSE ut.name END AS type_name"
    ) in sql


def test_every_blocker_is_reported_in_one_answer() -> None:
    current = _existing("CS_FactSale", "NONCLUSTERED COLUMNSTORE", 6)

    blockers = columnstore_index_blockers(
        _ncci(key_columns=("Note", "Payload", "[datekey]")),
        existing_indexes=[current],
        table_columns=FACT_COLUMNS,
    )

    assert len(blockers) == 3


def test_blockers_are_for_the_columnstore_kind_only() -> None:
    with pytest.raises(ValueError, match="nonclustered_columnstore"):
        columnstore_index_blockers(
            _ncci(index_kind=INDEX_KIND_ROWSTORE), existing_indexes=[], table_columns=()
        )


# --- the column read -------------------------------------------------------------


class _Executor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, str, list[Any]]] = []

    async def fetch_all(
        self, database_name: str, sql: str, params: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        self.calls.append((database_name, sql, list(params or [])))
        return self.rows


@pytest.mark.asyncio
async def test_the_column_read_is_one_scoped_catalog_query() -> None:
    executor = _Executor(
        [
            {
                "column_name": "Note",
                "type_name": "nvarchar",
                "max_length": -1,
                "is_nullable": True,
                "is_computed": False,
                "is_sparse": 0,
            },
            {
                "column_name": "Margin",
                "type_name": "decimal",
                "max_length": 9,
                "is_computed": 1,
                "is_sparse": None,
            },
        ]
    )

    facts = await collect_table_column_facts(
        executor, "appdb", schema_name="dw", table_name="Fact]Sale"  # type: ignore[arg-type]
    )

    assert executor.calls == [("appdb", TABLE_COLUMN_FACTS_SQL, ["[dw].[Fact]]Sale]"])]
    assert facts == [
        TableColumnFacts("Note", "nvarchar", max_length=-1, is_nullable=True),
        TableColumnFacts("Margin", "decimal", max_length=9, is_computed=True),
    ]
    assert "OBJECT_ID(?)" in TABLE_COLUMN_FACTS_SQL
