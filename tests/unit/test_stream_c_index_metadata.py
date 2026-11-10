"""Server cost of collect_existing_indexes: concurrent reads, one-object scope,
an existence check, and no index x column x partition cross product."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from azure_sql_mcp import index_metadata
from azure_sql_mcp.index_metadata import EXISTING_INDEX_METADATA_SQL
from azure_sql_mcp.index_metadata import INDEX_PROTECTION_METADATA_SQL
from azure_sql_mcp.index_metadata import collect_existing_indexes
from azure_sql_mcp.index_metadata import parse_existing_index_rows


class RecordingExecutor:
    def __init__(self, failures: dict[str, Exception] | None = None, delays: dict[str, float] | None = None) -> None:
        self.failures = failures or {}
        self.delays = delays or {}
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.in_flight = 0
        self.peak_in_flight = 0

    async def fetch_all(self, database_name: str, query: str, **kwargs: Any) -> list[dict[str, Any]]:
        if "FROM sys.dm_os_sys_info" in query:
            key = "engine_start"
        elif "child_foreign_key_ids" in query:
            key = "protection"
        elif "FROM sys.indexes" in query:
            key = "metadata" if "sys.index_columns" in query else "exists"
        else:
            raise AssertionError(f"unexpected query: {query[:80]}")
        self.calls.append((key, query, kwargs))
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self.delays.get(key, 0.01))
        finally:
            self.in_flight -= 1
        if key in self.failures:
            raise self.failures[key]
        if key == "engine_start":
            return [{"engine_start_time_utc": "2026-08-01T00:00:00", "engine_identity": "srv"}]
        return []

    def call(self, key: str) -> tuple[str, dict[str, Any]]:
        return next((query, kwargs) for name, query, kwargs in self.calls if name == key)


# ---------------------------------------------------------------- #62


@pytest.mark.asyncio
async def test_the_three_index_metadata_reads_run_at_the_same_time() -> None:
    # Index metadata, engine start time and protection evidence need nothing
    # from each other; in sequence the caller waited for the sum of the three.
    executor = RecordingExecutor()

    await collect_existing_indexes(executor, "appdb")  # type: ignore[arg-type]

    assert [key for key, _query, _kwargs in executor.calls] == ["metadata", "engine_start", "protection"]
    assert executor.peak_in_flight == 3


@pytest.mark.asyncio
async def test_the_first_failure_in_read_order_is_raised() -> None:
    # Callers show this error to the user. The protection read fails first in
    # time here; the metadata error is still the one raised, as in sequence.
    executor = RecordingExecutor(
        failures={"metadata": PermissionError("metadata denied"), "protection": PermissionError("protection denied")},
        delays={"metadata": 0.03, "protection": 0.001},
    )

    with pytest.raises(PermissionError, match="metadata denied"):
        await collect_existing_indexes(executor, "appdb")  # type: ignore[arg-type]


# ---------------------------------------------------------------- #66 (a)


@pytest.mark.asyncio
async def test_one_table_scope_is_a_parameterized_object_id_predicate() -> None:
    # The benchmark flow needs the indexes of one table. Reading the whole
    # catalog (and the correlated foreign-key subqueries) for it was O(all
    # indexes in the database) per check.
    executor = RecordingExecutor()

    await collect_existing_indexes(
        executor,  # type: ignore[arg-type]
        "appdb",
        schema_name="Sales",
        table_name="Order]Lines",
    )

    name = "[Sales].[Order]]Lines]"
    query, kwargs = executor.call("protection")
    assert "AND i.object_id = OBJECT_ID(?)" in query
    assert query.count("?") == 1
    assert kwargs == {"params": [name]}
    # The metadata query filters the index and both detail branches itself,
    # so the scope does not depend on the optimizer pushing the filter down.
    query, kwargs = executor.call("metadata")
    assert "AND i.object_id = OBJECT_ID(?)" in query
    assert "WHERE ic.object_id = OBJECT_ID(?)" in query
    assert "WHERE p.object_id = OBJECT_ID(?)" in query
    assert query.count("?") == 3
    assert kwargs == {"params": [name, name, name]}
    assert "Order]Lines" not in query
    assert executor.call("engine_start")[1] == {}


@pytest.mark.asyncio
async def test_database_wide_read_sends_no_parameters() -> None:
    executor = RecordingExecutor()

    await collect_existing_indexes(executor, "appdb")  # type: ignore[arg-type]

    assert executor.call("metadata") == (EXISTING_INDEX_METADATA_SQL, {})
    assert executor.call("protection") == (INDEX_PROTECTION_METADATA_SQL, {})
    # index_review runs both constants inside its own transaction, with no
    # parameters.
    assert "?" not in EXISTING_INDEX_METADATA_SQL
    assert "?" not in INDEX_PROTECTION_METADATA_SQL


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", [{"schema_name": "Sales"}, {"table_name": "Orders"}])
async def test_one_table_scope_needs_schema_and_table(scope: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="schema_name and table_name"):
        await collect_existing_indexes(RecordingExecutor(), "appdb", **scope)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_index_exists_is_one_small_read() -> None:
    class Executor(RecordingExecutor):
        async def fetch_all(self, database_name: str, query: str, **kwargs: Any) -> list[dict[str, Any]]:
            await super().fetch_all(database_name, query, **kwargs)
            return [{"index_exists": 1}] if kwargs["params"][1] == "IX_Present" else []

    executor = Executor()

    present = await index_metadata.index_exists(
        executor,  # type: ignore[arg-type]
        "appdb",
        schema_name="Sales",
        table_name="Orders",
        index_name="IX_Present",
    )
    absent = await index_metadata.index_exists(
        executor,  # type: ignore[arg-type]
        "appdb",
        schema_name="Sales",
        table_name="Orders",
        index_name="IX_Gone",
    )

    assert (present, absent) == (True, False)
    assert [key for key, _query, _kwargs in executor.calls] == ["exists", "exists"]
    query, kwargs = executor.call("exists")
    assert " ".join(query.split()) == (
        "SELECT 1 AS index_exists FROM sys.indexes WHERE object_id = OBJECT_ID(?) AND name = ?"
    )
    assert kwargs == {"params": ["[Sales].[Orders]", "IX_Present"]}


# ---------------------------------------------------------------- #66 (b)

_NO_COLUMN = {
    "index_column_id": None,
    "key_ordinal": None,
    "is_included_column": None,
    "is_descending_key": None,
    "partition_ordinal": None,
    "column_name": None,
}
_NO_PARTITION = {
    "partition_number": None,
    "partition_row_count": None,
    "partition_page_count": None,
    "data_compression_desc": None,
    "xml_compression_desc": None,
}


def _index(
    table: str,
    index_id: int,
    name: str,
    type_code: int,
    type_desc: str,
    columns: list[dict[str, Any]],
    partitions: list[dict[str, Any]],
    **overrides: Any,
) -> dict[str, Any]:
    base = {
        "object_id": 100 if table == "FactOrders" else 200,
        "parent_object_type_code": "U ",
        "parent_object_type": "USER_TABLE",
        "schema_name": "Sales",
        "table_name": table,
        "index_id": index_id,
        "index_name": name,
        "index_type_code": type_code,
        "index_type": type_desc,
        "is_unique": False,
        "is_primary_key": False,
        "is_unique_constraint": False,
        "is_disabled": False,
        "is_hypothetical": False,
        "auto_created": False,
        "has_filter": False,
        "filter_definition": None,
        "is_padded": False,
        "ignore_dup_key": False,
        "allow_row_locks": True,
        "allow_page_locks": True,
        "optimize_for_sequential_key": False,
        "suppress_dup_key_messages": False,
        "fill_factor": 0,
        "constraint_name": None,
        "constraint_type": None,
        "data_space_name": "ps_OrderDate" if len(partitions) > 1 else "PRIMARY",
        "data_space_type": "PARTITION_SCHEME" if len(partitions) > 1 else "ROWS_FILEGROUP",
        "partition_scheme_name": "ps_OrderDate" if len(partitions) > 1 else None,
        "partition_function_name": "pf_OrderDate" if len(partitions) > 1 else None,
        "statistics_no_recompute": False,
        "statistics_incremental": False,
        "statistics_updated_utc": "2026-09-01T00:00:00",
        "user_seeks": 12,
        "user_scans": 3,
        "user_lookups": 0,
        "user_updates": 40,
        "collected_at_utc": "2026-10-01T10:00:00Z",
    }
    base.update(overrides)
    return {"base": base, "columns": columns, "partitions": partitions}


def _column(ordinal: int, name: str, *, key: int = 0, included: bool = False, partition: int = 0) -> dict[str, Any]:
    return {
        "index_column_id": ordinal,
        "key_ordinal": key,
        "is_included_column": included,
        "is_descending_key": False,
        "partition_ordinal": partition,
        "column_name": name,
    }


def _partitions(count: int, *, columnstore: bool = False) -> list[dict[str, Any]]:
    return [
        {
            "partition_number": number,
            "partition_row_count": 1_000_000 + number,
            "partition_page_count": 4_000 + number,
            # The newest partitions are not yet compressed.
            "data_compression_desc": "COLUMNSTORE" if columnstore else ("NONE" if number > count - 5 else "PAGE"),
            "xml_compression_desc": "OFF",
        }
        for number in range(1, count + 1)
    ]


def _catalog() -> list[dict[str, Any]]:
    """A 365-partition fact table: a clustered columnstore index (where
    sys.index_columns lists every table column) and five rowstore indexes;
    plus a plain table and a disabled index that has no partition rows."""

    fact_columns = [f"Measure{position:03d}" for position in range(1, 100)]
    catalog = [
        _index(
            "FactOrders",
            1,
            "CCI_FactOrders",
            5,
            "CLUSTERED COLUMNSTORE",
            [_column(1, "OrderDate", included=True, partition=1)]
            + [_column(position + 2, name, included=True) for position, name in enumerate(fact_columns)],
            _partitions(365, columnstore=True),
        )
    ]
    for number in range(2, 7):
        catalog.append(
            _index(
                "FactOrders",
                number,
                f"IX_FactOrders_{number}",
                2,
                "NONCLUSTERED",
                [
                    _column(1, f"Measure{number:03d}", key=1),
                    _column(2, "OrderDate", key=2, partition=1),
                    _column(3, "Measure050", key=3),
                ]
                + [_column(position, f"Measure{position + 60:03d}", included=True) for position in range(4, 9)],
                _partitions(365),
            )
        )
    catalog.append(
        _index(
            "Customers",
            1,
            "PK_Customers",
            1,
            "CLUSTERED",
            [_column(1, "CustomerID", key=1)],
            _partitions(1),
            is_unique=True,
            is_primary_key=True,
            constraint_name="PK_Customers",
            constraint_type="PRIMARY_KEY_CONSTRAINT",
        )
    )
    catalog.append(
        _index(
            "Customers",
            2,
            "IX_Customers_Disabled",
            2,
            "NONCLUSTERED",
            [_column(1, "Region", key=1), _column(2, "Name", included=True)],
            [],
            is_disabled=True,
        )
    )
    return catalog


def _order(row: dict[str, Any]) -> tuple[Any, ...]:
    # The query's ORDER BY; SQL Server sorts NULL first.
    def nulls_first(value: Any) -> tuple[int, Any]:
        return (0, 0) if value is None else (1, value)

    return (
        row["schema_name"],
        row["table_name"],
        row["index_id"],
        nulls_first(row["key_ordinal"]),
        nulls_first(row["index_column_id"]),
        nulls_first(row["partition_number"]),
    )


def _cross_product_rows(catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """What the old query returned: every column row joined to every partition row."""

    rows = [
        {**index["base"], **column, **partition}
        for index in catalog
        for column in index["columns"] or [_NO_COLUMN]
        for partition in index["partitions"] or [_NO_PARTITION]
    ]
    return sorted(rows, key=_order)


def _detail_rows(catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """What the new query returns: column rows, then partition rows."""

    rows: list[dict[str, Any]] = []
    for index in catalog:
        details = [{**column, **_NO_PARTITION} for column in index["columns"]] + [
            {**_NO_COLUMN, **partition} for partition in index["partitions"]
        ]
        rows.extend({**index["base"], **detail} for detail in details or [{**_NO_COLUMN, **_NO_PARTITION}])
    return sorted(rows, key=_order)


def test_partitions_do_not_multiply_the_column_rows() -> None:
    catalog = _catalog()
    old_rows = _cross_product_rows(catalog)
    new_rows = _detail_rows(catalog)

    # 100 x 365 for the columnstore index alone.
    assert len(old_rows) == sum(
        max(len(index["columns"]), 1) * max(len(index["partitions"]), 1) for index in catalog
    ) == 51_103
    assert len(new_rows) == sum(len(index["columns"]) + len(index["partitions"]) for index in catalog) == 2_334

    parsed = parse_existing_index_rows(new_rows)
    assert parsed == parse_existing_index_rows(old_rows)
    assert [index.as_dict() for index in parsed] == [
        index.as_dict() for index in parse_existing_index_rows(old_rows)
    ]
    # Row order carries no meaning.
    assert parse_existing_index_rows(list(reversed(new_rows))) == parsed

    by_name = {index.name: index for index in parsed}
    columnstore = by_name["CCI_FactOrders"]
    assert len(columnstore.include_columns) == 100
    assert columnstore.partition_columns == ("OrderDate",)
    assert len(columnstore.partition_compression) == 365
    assert columnstore.partition_row_counts[-1] == (365, 1_000_365)
    assert columnstore.partition_page_counts[0] == (1, 4_001)
    rowstore = by_name["IX_FactOrders_2"]
    assert [column.name for column in rowstore.key_columns] == ["Measure002", "OrderDate", "Measure050"]
    assert len(rowstore.include_columns) == 5
    assert rowstore.partition_compression[0] == (1, "PAGE")
    assert rowstore.partition_compression[-1] == (365, "NONE")
    assert rowstore.xml_compression[0] == (1, "OFF")
    disabled = by_name["IX_Customers_Disabled"]
    assert disabled.partition_compression == () and disabled.include_columns == ("Name",)


def _table_names(node: exp.Expression) -> set[str]:
    return {table.name for table in node.find_all(exp.Table)}


@pytest.mark.parametrize("scoped", [False, True])
def test_metadata_sql_reads_columns_and_partitions_as_separate_rows(scoped: bool) -> None:
    sql = index_metadata.existing_index_metadata_sql(scoped=scoped)
    tree = sqlglot.parse_one(sql, read="tsql")

    [union] = list(tree.find_all(exp.Union))
    columns, partitions = _table_names(union.left), _table_names(union.right)
    assert "index_columns" in columns and "partitions" not in columns
    assert {"partitions", "dm_db_partition_stats"} <= partitions and "index_columns" not in partitions
    # Outside the detail rows, the partition views are not joined to the index.
    outer = _table_names(tree) - columns - partitions
    assert "indexes" in outer and not {"index_columns", "partitions", "dm_db_partition_stats"} & outer
    # Both kinds of row carry the same columns, in the query's column order.
    assert [p.alias_or_name for p in union.left.selects] == [p.alias_or_name for p in union.right.selects]


def test_metadata_sql_keeps_its_result_columns() -> None:
    tree = sqlglot.parse_one(EXISTING_INDEX_METADATA_SQL, read="tsql")

    assert [projection.alias_or_name for projection in tree.selects] == [
        "object_id",
        "parent_object_type_code",
        "parent_object_type",
        "schema_name",
        "table_name",
        "index_id",
        "index_name",
        "index_type_code",
        "index_type",
        "is_unique",
        "is_primary_key",
        "is_unique_constraint",
        "is_disabled",
        "is_hypothetical",
        "auto_created",
        "has_filter",
        "filter_definition",
        "is_padded",
        "ignore_dup_key",
        "allow_row_locks",
        "allow_page_locks",
        "optimize_for_sequential_key",
        "suppress_dup_key_messages",
        "fill_factor",
        "constraint_name",
        "constraint_type",
        "index_column_id",
        "key_ordinal",
        "is_included_column",
        "is_descending_key",
        "partition_ordinal",
        "column_store_order_ordinal",
        "column_name",
        "data_space_name",
        "data_space_type",
        "partition_scheme_name",
        "partition_function_name",
        "partition_number",
        "partition_row_count",
        "partition_page_count",
        "data_compression_desc",
        "xml_compression_desc",
        "statistics_no_recompute",
        "statistics_incremental",
        "statistics_updated_utc",
        "user_seeks",
        "user_scans",
        "user_lookups",
        "user_updates",
        "collected_at_utc",
    ]
