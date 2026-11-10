"""An ordered nonclustered columnstore test index (S14 item 4).

On a rowstore table with a date-range predicate, an ordered nonclustered
columnstore index with archive compression read about 45 percent fewer pages
than a plain one: ORDER makes the row groups disjoint on the range column, so
more of them are eliminated. The test index tools could not build that form.

Observed on Azure SQL Database (coordinator, 2026-11-10): ORDER (...) is
accepted with ONLINE and WAIT_AT_LOW_PRIORITY, an ORDER column that is not in
the index is engine error 1911, and sys.index_columns.column_store_order_ordinal
holds the order of a built index.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import existing_index_metadata_sql
from azure_sql_mcp.index_metadata import parse_existing_index_rows
from azure_sql_mcp.index_optimizer import INDEX_KIND_NONCLUSTERED_COLUMNSTORE
from azure_sql_mcp.index_optimizer import INDEX_KIND_ROWSTORE
from azure_sql_mcp.index_optimizer import IndexCandidate
from azure_sql_mcp.index_optimizer import build_test_index_statement
from azure_sql_mcp.index_optimizer import expected_index_definition_matches
from azure_sql_mcp.server import TEST_INDEX_PREFIX
from azure_sql_mcp.server import generated_test_index_name
from tests.unit import test_3_0_s13b2_columnstore_kind as columnstore_kind
from tests.unit import test_3_0_wiring2_index_set as index_set
from tests.unit.test_index_benchmark_workflow import _profile

NAME = "IX_Testing_FactSale_DateKey_ProductKey_Amount_0a1b2c3d"


def _ncci(**fields: Any) -> IndexCandidate:
    values: dict[str, Any] = {
        "schema": "dw",
        "table": "FactSale",
        "key_columns": ("DateKey", "ProductKey", "Amount"),
        "include_columns": (),
        "index_name": NAME,
        "index_kind": INDEX_KIND_NONCLUSTERED_COLUMNSTORE,
    }
    values.update(fields)
    return IndexCandidate(**values)


def _built(candidate: IndexCandidate, **fields: Any) -> ExistingIndex:
    values: dict[str, Any] = {
        "schema": candidate.schema,
        "table": candidate.table,
        "index_id": 7,
        "name": str(candidate.index_name),
        "index_type": "NONCLUSTERED COLUMNSTORE",
        "key_columns": (),
        "include_columns": tuple(candidate.key_columns),
        "partition_compression": ((1, candidate.compression or "COLUMNSTORE"),),
        "columnstore_order_columns": tuple(candidate.order_columns),
    }
    values.update(fields)
    return ExistingIndex(**values)


def test_the_ddl_emits_order_after_the_column_list_with_archive_compression() -> None:
    candidate = _ncci(order_columns=("DateKey", "[ProductKey]"), compression="COLUMNSTORE_ARCHIVE")

    ddl = build_test_index_statement(candidate, online=True, maxdop=2)

    assert ddl == (
        f"CREATE NONCLUSTERED COLUMNSTORE INDEX [{NAME}] "
        "ON [dw].[FactSale] ([DateKey], [ProductKey], [Amount]) "
        "ORDER ([DateKey], [ProductKey]) "
        "WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE, ONLINE = ON (WAIT_AT_LOW_PRIORITY "
        "(MAX_DURATION = 1 MINUTES, ABORT_AFTER_WAIT = SELF)), MAXDOP = 2);"
    )


def test_a_filtered_ordered_index_puts_order_before_the_filter() -> None:
    candidate = _ncci(order_columns=("DateKey",), filter_definition="[DateKey] >= 20240101")

    ddl = build_test_index_statement(candidate, online=False, allow_offline=True)

    assert "([DateKey], [ProductKey], [Amount]) ORDER ([DateKey]) WHERE [DateKey] >= 20240101" in ddl


def test_an_index_without_order_columns_has_the_ddl_it_always_had() -> None:
    assert "ORDER" not in build_test_index_statement(_ncci(), online=True)


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"order_columns": ("CustomerKey",)}, "not in the index column list"),
        ({"order_columns": ("DateKey DESC",)}, "ASC or DESC"),
        ({"order_columns": ("DateKey", "[datekey]")}, "twice"),
        (
            {"order_columns": ("DateKey",), "index_kind": INDEX_KIND_ROWSTORE},
            "nonclustered_columnstore",
        ),
    ],
)
def test_order_columns_the_engine_would_refuse_are_refused_before_any_ddl(
    fields: dict[str, Any], message: str
) -> None:
    # Engine error 1911 for a column outside the index: refused here, by name.
    with pytest.raises(ValueError, match=message):
        build_test_index_statement(_ncci(**fields), online=True)


def test_the_order_is_part_of_the_definition_fingerprint_and_not_of_the_name() -> None:
    plain = _ncci(index_name=None)
    by_date = replace(plain, order_columns=("DateKey",))
    by_product = replace(plain, order_columns=("ProductKey",))

    assert len({plain.definition_fingerprint, by_date.definition_fingerprint,
                by_product.definition_fingerprint}) == 3
    assert replace(plain, order_columns=("DateKey", "ProductKey")).definition_fingerprint != (
        replace(plain, order_columns=("ProductKey", "DateKey")).definition_fingerprint
    )
    # The readable part of the name is the table and the columns, as before.
    assert generated_test_index_name("IX_Testing_", by_date) == (
        "IX_Testing_FactSale_DateKey_ProductKey_Amount_" + by_date.definition_fingerprint[:8]
    )
    assert by_date.as_dict()["order_columns"] == ["DateKey"]
    assert "order_columns" not in plain.as_dict()


def test_a_built_index_is_verified_against_the_requested_order() -> None:
    candidate = _ncci(order_columns=("DateKey", "ProductKey"), compression="COLUMNSTORE_ARCHIVE")

    assert expected_index_definition_matches(_built(candidate), candidate)
    # Another order, no order, or an order nobody asked for: not the index of the lease.
    for other in [("ProductKey", "DateKey"), ("DateKey",), ()]:
        assert not expected_index_definition_matches(
            _built(candidate, columnstore_order_columns=other), candidate
        )
    assert not expected_index_definition_matches(
        _built(_ncci(), columnstore_order_columns=("DateKey",)), _ncci()
    )


def test_the_catalog_read_takes_the_order_from_column_store_order_ordinal() -> None:
    for sql in (existing_index_metadata_sql(), existing_index_metadata_sql(scoped=True)):
        assert "ic.column_store_order_ordinal" in sql
        assert "NULL AS column_store_order_ordinal" in sql

    def row(column: str, column_id: int, order_ordinal: int) -> dict[str, Any]:
        return {
            "schema_name": "dw", "table_name": "FactSale", "index_id": 7, "index_name": NAME,
            "index_type": "NONCLUSTERED COLUMNSTORE", "index_column_id": column_id,
            "key_ordinal": 0, "is_included_column": 1, "column_name": column,
            "column_store_order_ordinal": order_ordinal,
        }

    [index] = parse_existing_index_rows(
        [row("DateKey", 1, 2), row("ProductKey", 2, 1), row("Amount", 3, 0)]
    )

    assert index.columnstore_order_columns == ("ProductKey", "DateKey")
    assert index.as_dict()["columnstore_order_columns"] == ["ProductKey", "DateKey"]


# --- the tools ----------------------------------------------------------------------

ORDERED_NCCI = {
    "table_name": "Comments",
    "kind": "nonclustered_columnstore",
    "key_columns": ["PostTypeKey", "Score"],
    "order_columns": ["PostTypeKey"],
    "data_compression": "COLUMNSTORE_ARCHIVE",
}


class _OrderedSandbox(columnstore_kind._ColumnstoreSandbox):
    """The catalog double: a built columnstore index keeps the order it was created with."""

    async def _create(self, _database: str, schema: str, table: str, index_name: str,
                      key_columns: list[str], include_columns: list[str] | None, online: bool,
                      **kwargs: Any) -> dict[str, str]:
        result = await super()._create(
            _database, schema, table, index_name, key_columns, include_columns, online, **kwargs
        )
        order = tuple(kwargs.get("order_columns") or ())
        self.indexes[index_name] = replace(
            self.indexes[index_name], columnstore_order_columns=order
        )
        candidate = IndexCandidate(
            schema=schema, table=table, key_columns=tuple(key_columns), include_columns=(),
            index_name=index_name, compression=kwargs.get("data_compression"),
            index_kind=INDEX_KIND_NONCLUSTERED_COLUMNSTORE, order_columns=order,
        )
        self.markers[index_name] = (str(kwargs["ownership_proof"]), candidate.definition_fingerprint)
        return result


@pytest.mark.asyncio
async def test_an_ordered_archive_columnstore_index_is_built_verified_and_dropped(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = index_set._app(server_config_factory)
    sandbox = _OrderedSandbox(app, monkeypatch)
    session_id, candidate_id = index_set._session(app, columnstore_kind.SQL)
    index_name = generated_test_index_name(
        TEST_INDEX_PREFIX,
        IndexCandidate(
            schema="dbo", table="Comments", key_columns=("PostTypeKey", "Score"),
            include_columns=(), compression="COLUMNSTORE_ARCHIVE",
            index_kind=INDEX_KIND_NONCLUSTERED_COLUMNSTORE, order_columns=("PostTypeKey",),
        ),
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(600) for _ in range(3)],
            *[index_set._plan(index_name, elapsed_ms=60) for _ in range(3)],
            *[_profile(600) for _ in range(3)],
        ]
    )

    result = await index_set._benchmark(
        app, session_id, candidate_id, sql=columnstore_kind.SQL, indexes=[ORDERED_NCCI]
    )

    assert result["classification"] == "improved", result["reason"]
    assert sandbox.created == [index_name] and sandbox.dropped == [index_name]
    create = sandbox.create_calls[0]
    assert create["order_columns"] == ["PostTypeKey"]
    assert create["data_compression"] == "COLUMNSTORE_ARCHIVE"
    assert "([PostTypeKey], [Score]) ORDER ([PostTypeKey]) WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE" in (
        result["index_ddl"]
    )
    assert result["indexes"][0]["order_columns"] == ["PostTypeKey"]
    assert result["lease"]["status"] == "cleaned"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ({**ORDERED_NCCI, "order_columns": ["CreationDate"]}, "not in the index column list"),
        (
            {"table_name": "Comments", "key_columns": ["PostTypeKey"], "order_columns": ["PostTypeKey"]},
            "nonclustered_columnstore",
        ),
    ],
)
async def test_order_columns_that_cannot_be_built_cost_no_execution(
    server_config_factory, monkeypatch: pytest.MonkeyPatch, spec: dict[str, Any], message: str
) -> None:
    app = index_set._app(server_config_factory)
    sandbox = _OrderedSandbox(app, monkeypatch)
    session_id, candidate_id = index_set._session(app, columnstore_kind.SQL)
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ValueError, match=message):
        await index_set._benchmark(
            app, session_id, candidate_id, sql=columnstore_kind.SQL, indexes=[spec]
        )

    assert sandbox.created == [] and app.plans.profile_query.await_count == 0


@pytest.mark.asyncio
async def test_the_tools_take_order_columns_in_every_form(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = index_set._app(server_config_factory)
    _OrderedSandbox(app, monkeypatch)
    session_id, candidate_id = index_set._session(app, columnstore_kind.SQL)
    app._benchmark_index_candidate = AsyncMock(return_value={"classification": "neutral"})  # type: ignore[method-assign]
    base = {
        "session_id": session_id, "candidate_id": candidate_id,
        "sql": columnstore_kind.SQL, "idempotency_key": "k",
    }

    await app.mcp._tool_manager.call_tool(
        "benchmark_index_candidate", {**base, "indexes": [ORDERED_NCCI]}
    )
    as_set = app._benchmark_index_candidate.await_args.kwargs
    await app.mcp._tool_manager.call_tool(
        "benchmark_index_candidate",
        {**base, "schema_name": "dbo", "table_name": "Comments",
         "key_columns": ["PostTypeKey", "Score"], "index_kind": "nonclustered_columnstore",
         "order_columns": ["Score"], "data_compression": "COLUMNSTORE_ARCHIVE"},
    )
    single = app._benchmark_index_candidate.await_args.kwargs

    assert as_set["indexes"][0]["order_columns"] == ["PostTypeKey"]
    assert single["order_columns"] == ["Score"]


@pytest.mark.asyncio
async def test_create_test_index_previews_the_ordered_archive_form(server_config_factory) -> None:
    app = index_set._app(server_config_factory)

    preview = await app._create_test_index(
        "appdb", "dbo", "Comments", "IX_Testing_Comments_ordered",
        ["PostTypeKey", "Score"], None, True, True,
        index_kind="nonclustered_columnstore",
        order_columns=["PostTypeKey"],
        data_compression="COLUMNSTORE_ARCHIVE",
    )

    assert "ORDER ([PostTypeKey]) WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE" in str(preview)
    with pytest.raises(ValueError, match="not in the index column list"):
        await app._create_test_index(
            "appdb", "dbo", "Comments", "IX_Testing_Comments_ordered",
            ["PostTypeKey", "Score"], None, True, True,
            index_kind="nonclustered_columnstore", order_columns=["Text"],
        )
