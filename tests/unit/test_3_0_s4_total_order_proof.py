"""TOP, OFFSET and window order are provable only on a catalog-verified unique NOT NULL key."""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.catalog_constraints import CatalogConstraintService
from azure_sql_mcp.catalog_constraints import TableConstraintFacts
from azure_sql_mcp.catalog_constraints import UniqueKey
from azure_sql_mcp.equivalence_preflight import EquivalencePreflightService


def _facts(table: str, **overrides: Any) -> TableConstraintFacts:
    values: dict[str, Any] = {
        "schema_name": "dbo",
        "table_name": table,
        "exists": True,
        "object_type": "USER_TABLE",
        "nullable": {"id": False, "score": False, "postid": False, "slug": True},
        "unique_keys": (UniqueKey(name=f"PK_{table}", kind="primary_key", columns=("Id",)),),
    }
    values.update(overrides)
    return TableConstraintFacts(**values)


class FakeCatalog:
    def __init__(self, tables: dict[str, TableConstraintFacts]) -> None:
        self.tables = tables
        self.reads: list[tuple[str, str, str]] = []

    async def table_facts(self, database_name: str, schema_name: str, table_name: str):
        self.reads.append((database_name, schema_name, table_name))
        return self.tables.get(
            table_name.casefold(), TableConstraintFacts(schema_name, table_name)
        )


async def _tables_only(_database: str, _schema: str, _name: str) -> dict[str, Any]:
    return {"object_type": "USER_TABLE"}


async def _analyze(sql: str, catalog: Any) -> dict[str, Any]:
    service = EquivalencePreflightService(_tables_only, catalog=catalog)
    return (await service.analyze("appdb", sql)).as_dict()


@pytest.mark.asyncio
async def test_top_ordered_by_the_primary_key_is_proven_from_the_catalog() -> None:
    catalog = FakeCatalog({"posts": _facts("Posts")})

    result = await _analyze(
        "SELECT TOP (10) p.Id, p.Score FROM dbo.Posts AS p ORDER BY p.Id DESC", catalog
    )

    assert result["direct_snapshot_supported"] is True
    assert result["risk_codes"] == []
    assert result["total_order_proofs"] == [
        {
            "risk": "row_limit_total_order_unproven",
            "statement_ordinal": 1,
            "select_ordinal": 1,
            "keys": [
                {
                    "table": "dbo.Posts",
                    "key_name": "PK_Posts",
                    "key_kind": "primary_key",
                    "key_columns": ["Id"],
                }
            ],
        }
    ]


@pytest.mark.asyncio
async def test_top_ordered_by_a_non_unique_column_names_the_tie_and_the_tiebreaker() -> None:
    catalog = FakeCatalog({"posts": _facts("Posts")})

    result = await _analyze(
        "SELECT TOP (10) p.Id, p.Score FROM dbo.Posts AS p ORDER BY p.Score DESC", catalog
    )

    assert result["direct_snapshot_supported"] is False
    assert result["risk_codes"] == ["row_limit_total_order_unproven"]
    gap = result["total_order_gaps"][0]
    assert gap["table"] == "dbo.Posts"
    assert gap["tie"] == (
        "rows of dbo.Posts with the same (Score) are tied, so which of them the row limit "
        "keeps is not defined"
    )
    assert gap["tiebreaker_columns"] == ["Id"]
    assert gap["tiebreaker"] == "append Id (primary_key PK_Posts) to the ORDER BY"
    # The agent reads the risk list first: the tie must be there, not only in a side list.
    risk = next(
        item for item in result["risks"] if item["risk"] == "row_limit_total_order_unproven"
    )
    assert gap["tie"] in risk["reason"]
    assert gap["tiebreaker"] in risk["reason"]


@pytest.mark.asyncio
async def test_appending_the_key_as_tiebreaker_makes_the_order_provable() -> None:
    catalog = FakeCatalog({"posts": _facts("Posts")})

    result = await _analyze(
        "SELECT p.Id FROM dbo.Posts AS p ORDER BY p.Score DESC, p.Id "
        "OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY",
        catalog,
    )

    assert result["direct_snapshot_supported"] is True


@pytest.mark.asyncio
async def test_a_nullable_unique_column_is_not_accepted_as_the_ordering_key() -> None:
    catalog = FakeCatalog(
        {
            "posts": _facts(
                "Posts",
                unique_keys=(UniqueKey(name="UQ_Slug", kind="unique_index", columns=("Slug",)),),
            )
        }
    )

    result = await _analyze(
        "SELECT TOP (5) p.Slug FROM dbo.Posts AS p ORDER BY p.Slug", catalog
    )

    assert result["direct_snapshot_supported"] is False
    assert "no unique NOT NULL key" in result["total_order_gaps"][0]["tiebreaker"]


@pytest.mark.asyncio
async def test_a_join_is_proven_only_when_every_table_contributes_a_key() -> None:
    catalog = FakeCatalog({"posts": _facts("Posts"), "comments": _facts("Comments")})
    sql = (
        "SELECT TOP (10) p.Id, c.Id FROM dbo.Posts AS p "
        "JOIN dbo.Comments AS c ON c.PostId = p.Id ORDER BY p.Id{extra}"
    )

    one_side = await _analyze(sql.format(extra=""), catalog)
    both_sides = await _analyze(sql.format(extra=", c.Id"), catalog)

    assert one_side["direct_snapshot_supported"] is False
    assert one_side["total_order_gaps"][0]["table"] == "dbo.Comments"
    assert "no column of dbo.Comments" in one_side["total_order_gaps"][0]["tie"]
    assert both_sides["direct_snapshot_supported"] is True


@pytest.mark.asyncio
async def test_row_number_is_proven_when_partition_and_order_cover_the_key() -> None:
    catalog = FakeCatalog({"posts": _facts("Posts")})
    sql = (
        "SELECT ROW_NUMBER() OVER (PARTITION BY p.OwnerUserId ORDER BY {order}) AS rn "
        "FROM dbo.Posts AS p"
    )

    tied = await _analyze(sql.format(order="p.Score DESC"), catalog)
    keyed = await _analyze(sql.format(order="p.Score DESC, p.Id"), catalog)

    assert tied["risk_codes"] == ["window_order_total_order_unproven"]
    assert tied["total_order_gaps"][0]["tiebreaker"] == (
        "append Id (primary_key PK_Posts) to the window ORDER BY"
    )
    assert keyed["direct_snapshot_supported"] is True


@pytest.mark.asyncio
async def test_a_view_or_an_unreadable_table_proves_nothing() -> None:
    catalog = FakeCatalog({"posts": _facts("Posts", object_type="VIEW")})

    view = await _analyze("SELECT TOP (5) p.Id FROM dbo.Posts AS p ORDER BY p.Id", catalog)
    missing = await _analyze("SELECT TOP (5) a.Id FROM dbo.Archive AS a ORDER BY a.Id", catalog)

    assert view["direct_snapshot_supported"] is False
    assert "not a base table" in view["total_order_gaps"][0]["tie"]
    assert missing["direct_snapshot_supported"] is False
    assert "could not be read" in missing["total_order_gaps"][0]["tie"]


@pytest.mark.asyncio
async def test_one_proven_limit_does_not_clear_another_unproven_one() -> None:
    catalog = FakeCatalog({"posts": _facts("Posts"), "comments": _facts("Comments")})

    result = await _analyze(
        "SELECT TOP (5) p.Id FROM dbo.Posts AS p WHERE p.Id IN "
        "(SELECT TOP (100) c.PostId FROM dbo.Comments AS c ORDER BY c.Score DESC) "
        "ORDER BY p.Id",
        catalog,
    )

    assert result["risk_codes"] == ["row_limit_total_order_unproven"]
    assert len(result["total_order_proofs"]) == 1
    assert len(result["total_order_gaps"]) == 1


@pytest.mark.asyncio
async def test_without_a_catalog_reader_an_ordered_limit_stays_unproven() -> None:
    service = EquivalencePreflightService(_tables_only)

    result = (
        await service.analyze("appdb", "SELECT TOP (5) p.Id FROM dbo.Posts AS p ORDER BY p.Id")
    ).as_dict()

    assert result["direct_snapshot_supported"] is False
    assert result["total_order_gaps"][0]["tie"] == "no catalog reader is configured"


@pytest.mark.asyncio
async def test_catalog_is_read_once_per_table_and_only_when_a_total_order_is_needed() -> None:
    catalog = FakeCatalog({"posts": _facts("Posts")})

    await _analyze("SELECT p.Id FROM dbo.Posts AS p WHERE p.Score > 5", catalog)
    assert catalog.reads == []

    await _analyze(
        "SELECT TOP (5) p.Id, ROW_NUMBER() OVER (ORDER BY p.Id) AS rn "
        "FROM dbo.Posts AS p ORDER BY p.Id",
        catalog,
    )
    assert catalog.reads == [("appdb", "dbo", "Posts")]


def test_an_executor_backed_service_reads_the_catalog_through_that_executor() -> None:
    class Executor:
        async def fetch_all(self, *_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
            return []

    executor = Executor()
    service = EquivalencePreflightService(executor=executor)

    assert isinstance(service.catalog, CatalogConstraintService)
    assert service.catalog.executor is executor


@pytest.mark.asyncio
async def test_database_preflight_accepts_a_query_hint_the_parser_does_not_know() -> None:
    catalog = FakeCatalog({"posts": _facts("Posts")})

    result = await _analyze(
        "SELECT TOP (5) p.Id FROM dbo.Posts AS p WHERE p.Score > @s ORDER BY p.Id "
        "OPTION (OPTIMIZE FOR (@s = 10))",
        catalog,
    )

    assert result["direct_snapshot_supported"] is True
    assert result["total_order_proofs"][0]["keys"][0]["key_name"] == "PK_Posts"
