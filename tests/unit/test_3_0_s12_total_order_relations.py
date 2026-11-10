"""Total-order proof through derived tables, CTEs, set operations and key joins.

The standard fixes for deep paging and for OR across two columns page or union
the keys in a derived table and join back on the primary key. A DBA needs the
product to prove those rewrites; when it cannot, the answer must name the
relation that has no key.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.catalog_constraints import CatalogConstraintService
from azure_sql_mcp.catalog_constraints import TableConstraintFacts
from azure_sql_mcp.catalog_constraints import UniqueKey
from azure_sql_mcp.equivalence_preflight import EquivalencePreflightService

INT = "56/4/10/0/"
NVARCHAR_40 = "231/80/0/0/Latin1_General_CI_AS"


def _facts(table: str, **overrides: Any) -> TableConstraintFacts:
    values: dict[str, Any] = {
        "schema_name": "dbo",
        "table_name": table,
        "exists": True,
        "object_type": "USER_TABLE",
        "nullable": {
            "id": False,
            "creationdate": False,
            "posttypeid": False,
            "postid": False,
            "owneruserid": True,
            "lasteditoruserid": True,
            "userid": True,
            "score": False,
        },
        "unique_keys": (UniqueKey(name=f"PK_{table}", kind="primary_key", columns=("Id",)),),
        "column_types": {
            "id": INT,
            "postid": INT,
            "owneruserid": INT,
            "lasteditoruserid": INT,
            "userid": INT,
            "score": INT,
        },
    }
    values.update(overrides)
    return TableConstraintFacts(**values)


class FakeCatalog:
    def __init__(self, *tables: TableConstraintFacts) -> None:
        self.tables = {facts.table_name.casefold(): facts for facts in tables}

    async def table_facts(self, _database: str, schema_name: str, table_name: str):
        return self.tables.get(
            table_name.casefold(), TableConstraintFacts(schema_name, table_name)
        )


async def _tables_only(_database: str, _schema: str, _name: str) -> dict[str, Any]:
    return {"object_type": "USER_TABLE"}


async def _analyze(sql: str, *tables: TableConstraintFacts) -> dict[str, Any]:
    service = EquivalencePreflightService(_tables_only, catalog=FakeCatalog(*tables))
    return (await service.analyze("appdb", sql)).as_dict()


# The deep-paging rewrite from the first live comparison (task T6).
PAGING_REWRITE = """
SELECT p.Id, p.Title, p.CreationDate, p.AnswerCount
FROM (SELECT p0.Id, p0.CreationDate FROM dbo.Posts AS p0 WHERE p0.PostTypeId = 1
      ORDER BY p0.CreationDate DESC, p0.Id DESC OFFSET @skip ROWS FETCH NEXT 25 ROWS ONLY) AS k
JOIN dbo.Posts AS p ON p.Id = k.Id
ORDER BY p.CreationDate DESC, p.Id DESC
"""


@pytest.mark.asyncio
async def test_paging_rewrite_proves_the_inner_page_and_the_outer_result_order() -> None:
    result = await _analyze(PAGING_REWRITE, _facts("Posts"))

    assert result["direct_snapshot_supported"] is True
    assert result["risk_codes"] == []
    # The inner page: OFFSET over one table, ordered by a superset of its primary key.
    assert [proof["keys"][0]["key_name"] for proof in result["total_order_proofs"]] == [
        "PK_Posts"
    ]
    # The outer ORDER BY: the join back on Id cannot duplicate a row, so
    # (CreationDate, Id) of p orders the 25 rows without a tie. An order-only
    # difference between baseline and rewrite is then a real difference.
    assert result["result_order_ties"] == []
    outer = result["result_order_proofs"][0]
    assert outer["statement_ordinal"] == 1
    assert {key["table"] for key in outer["keys"]} == {"dbo.Posts"}
    assert any(key.get("relation") == "derived table k" for key in outer["keys"])


@pytest.mark.asyncio
async def test_row_limit_on_the_join_back_is_proven_through_the_key_join() -> None:
    # Same rewrite with the page taken outside: the ORDER BY names only p, and
    # the row of k is fixed by p.Id = k.Id on the same key column.
    result = await _analyze(
        "SELECT TOP (25) p.Id, p.Title FROM "
        "(SELECT p0.Id FROM dbo.Posts AS p0 WHERE p0.PostTypeId = 1) AS k "
        "JOIN dbo.Posts AS p ON p.Id = k.Id ORDER BY p.CreationDate DESC, p.Id DESC",
        _facts("Posts"),
    )

    assert result["direct_snapshot_supported"] is True
    keys = result["total_order_proofs"][0]["keys"]
    assert {"table": "dbo.Posts", "key_name": "PK_Posts", "key_kind": "primary_key",
            "key_columns": ["Id"]} in keys
    inner = next(key for key in keys if key.get("relation") == "derived table k")
    assert inner["reached_through_join"] is True


@pytest.mark.asyncio
async def test_ordering_by_columns_of_a_derived_table_uses_the_base_table_key() -> None:
    result = await _analyze(
        "SELECT TOP (25) k.Id, k.Created FROM "
        "(SELECT p0.Id, p0.CreationDate AS Created FROM dbo.Posts AS p0 "
        " WHERE p0.PostTypeId = 1) AS k ORDER BY k.Created DESC, k.Id DESC",
        _facts("Posts"),
    )

    assert result["direct_snapshot_supported"] is True
    assert result["total_order_proofs"][0]["keys"] == [
        {
            "table": "dbo.Posts",
            "key_name": "PK_Posts",
            "key_kind": "primary_key",
            "key_columns": ["Id"],
            "relation": "derived table k",
        }
    ]


@pytest.mark.asyncio
async def test_a_cte_of_keys_joined_back_is_proven_like_a_derived_table() -> None:
    result = await _analyze(
        "WITH k AS (SELECT p0.Id FROM dbo.Posts AS p0 WHERE p0.PostTypeId = 1 "
        "ORDER BY p0.CreationDate DESC, p0.Id DESC OFFSET @skip ROWS FETCH NEXT 25 ROWS ONLY) "
        "SELECT TOP (25) p.Id, p.Title FROM k JOIN dbo.Posts AS p ON p.Id = k.Id "
        "ORDER BY p.CreationDate DESC, p.Id DESC",
        _facts("Posts"),
    )

    assert result["direct_snapshot_supported"] is True
    assert len(result["total_order_proofs"]) == 2
    assert any(
        key.get("relation") == "common table expression k"
        for proof in result["total_order_proofs"]
        for key in proof["keys"]
    )


# The OR-across-two-columns rewrite from the live comparison (task T5).
UNION_REWRITE = (
    "SELECT TOP (50) p.Id, p.Title FROM "
    "(SELECT p1.Id FROM dbo.Posts AS p1 WHERE p1.OwnerUserId = @u "
    " UNION{all} SELECT p2.Id FROM dbo.Posts AS p2 WHERE p2.LastEditorUserId = @u) AS k "
    "JOIN dbo.Posts AS p ON p.Id = k.Id ORDER BY p.Id DESC"
)


@pytest.mark.asyncio
async def test_union_of_key_seeks_joined_back_is_proven() -> None:
    result = await _analyze(UNION_REWRITE.format(all=""), _facts("Posts"))

    assert result["direct_snapshot_supported"] is True
    keys = result["total_order_proofs"][0]["keys"]
    assert {
        "relation": "derived table k",
        "key_kind": "distinct_rows",
        "key_columns": ["Id"],
        "reached_through_join": True,
    } in keys


@pytest.mark.asyncio
async def test_union_all_is_named_as_the_relation_that_can_repeat_a_row() -> None:
    # A post the user both owns and last edited comes back twice from UNION ALL,
    # so p.Id no longer orders the joined rows.
    result = await _analyze(UNION_REWRITE.format(all=" ALL"), _facts("Posts"))

    assert result["risk_codes"] == ["row_limit_total_order_unproven"]
    tie = result["total_order_gaps"][0]["tie"]
    assert "derived table k" in tie
    assert "UNION ALL" in tie
    assert "same row twice" in tie


@pytest.mark.asyncio
async def test_a_union_ordered_by_all_of_its_columns_needs_no_catalog_key() -> None:
    sql = (
        "SELECT p1.Id FROM dbo.Posts AS p1 WHERE p1.OwnerUserId = @u UNION "
        "SELECT p2.Id FROM dbo.Posts AS p2 WHERE p2.LastEditorUserId = @u "
        "ORDER BY Id DESC OFFSET 0 ROWS FETCH NEXT 50 ROWS ONLY"
    )

    result = await _analyze(sql)

    assert result["direct_snapshot_supported"] is True
    assert result["total_order_proofs"][0]["keys"] == [
        {"relation": "the set operation", "key_kind": "distinct_rows", "key_columns": ["Id"]}
    ]


@pytest.mark.asyncio
async def test_a_union_ordered_by_part_of_its_columns_names_the_missing_ones() -> None:
    result = await _analyze(
        "SELECT TOP (10) k.Id FROM "
        "(SELECT p1.Id, p1.Score FROM dbo.Posts AS p1 UNION "
        " SELECT c.PostId, c.Score FROM dbo.Comments AS c) AS k ORDER BY k.Id",
        _facts("Posts"),
        _facts("Comments"),
    )

    assert result["risk_codes"] == ["row_limit_total_order_unproven"]
    gap = result["total_order_gaps"][0]
    assert gap["relation"] == "derived table k"
    assert "only all of its columns (Id, Score)" in gap["tie"]
    assert gap["tiebreaker_columns"] == ["Score"]


@pytest.mark.asyncio
async def test_a_join_to_a_lookup_table_on_its_key_does_not_need_its_key_in_the_order() -> None:
    # One user per post: the LEFT JOIN on the primary key of Users cannot fan out.
    sql = (
        "SELECT TOP (50) p.Id, u.DisplayName FROM dbo.Posts AS p "
        "LEFT JOIN dbo.Users AS u ON u.Id = p.OwnerUserId "
        "ORDER BY p.CreationDate DESC, p.Id DESC"
    )

    result = await _analyze(sql, _facts("Posts"), _facts("Users"))

    assert result["direct_snapshot_supported"] is True
    users = next(
        key for key in result["total_order_proofs"][0]["keys"] if key["table"] == "dbo.Users"
    )
    assert users["reached_through_join"] is True


@pytest.mark.asyncio
async def test_a_key_join_is_not_used_when_the_column_types_differ() -> None:
    # '1' and '01' both equal the integer 1: an implicit conversion can match
    # several rows of the lookup table, so the join proves nothing.
    sql = (
        "SELECT TOP (50) p.Id FROM dbo.Posts AS p "
        "JOIN dbo.Users AS u ON u.Id = p.OwnerUserId ORDER BY p.Id"
    )

    result = await _analyze(
        sql, _facts("Posts"), _facts("Users", column_types={"id": NVARCHAR_40})
    )

    assert result["risk_codes"] == ["row_limit_total_order_unproven"]
    gap = result["total_order_gaps"][0]
    assert gap["table"] == "dbo.Users"
    assert "p.OwnerUserId = u.Id" in gap["join_not_used"] or (
        "u.Id = p.OwnerUserId" in gap["join_not_used"]
    )
    assert "types differ" in gap["join_not_used"]


@pytest.mark.asyncio
async def test_a_left_join_key_does_not_order_the_preserved_table() -> None:
    # Every post without an owner has u.Id NULL: ordering by u.Id ties them all.
    sql = (
        "SELECT TOP (50) p.Id FROM dbo.Posts AS p "
        "LEFT JOIN dbo.Users AS u ON u.Id = p.OwnerUserId ORDER BY u.Id"
    )

    result = await _analyze(sql, _facts("Posts"), _facts("Users"))

    assert result["risk_codes"] == ["row_limit_total_order_unproven"]
    assert result["total_order_gaps"][0]["table"] == "dbo.Posts"


@pytest.mark.asyncio
async def test_a_fan_out_join_inside_the_derived_table_is_still_a_gap() -> None:
    # One post has many comments: k repeats p0.Id, so k.Id is not a key of k.
    result = await _analyze(
        "SELECT TOP (25) k.Id FROM "
        "(SELECT p0.Id FROM dbo.Posts AS p0 JOIN dbo.Comments AS c ON c.PostId = p0.Id) AS k "
        "ORDER BY k.Id",
        _facts("Posts"),
        _facts("Comments"),
    )

    assert result["risk_codes"] == ["row_limit_total_order_unproven"]
    gap = result["total_order_gaps"][0]
    assert gap["table"] == "dbo.Comments"
    assert gap["relation"] == "derived table k"
    assert "read as c in derived table k" in gap["tie"]
    assert gap["tiebreaker"] == (
        "select Id (primary_key PK_Comments) in derived table k and append it to the ORDER BY"
    )


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        (
            "SELECT TOP (5) k.Total FROM (SELECT p0.Score + 1 AS Total FROM dbo.Posts AS p0) AS k "
            "ORDER BY k.Total",
            "ordering column k.Total of derived table k is an expression",
        ),
        (
            "SELECT TOP (5) k.Id FROM (SELECT * FROM dbo.Posts AS p0) AS k ORDER BY k.Id",
            "derived table k selects *",
        ),
        (
            "WITH r AS (SELECT p0.Id, p0.ParentId FROM dbo.Posts AS p0 WHERE p0.ParentId IS NULL "
            "UNION ALL SELECT c.Id, c.ParentId FROM dbo.Posts AS c JOIN r ON r.Id = c.ParentId) "
            "SELECT TOP (5) r.Id FROM r ORDER BY r.Id",
            "common table expression r uses UNION ALL",
        ),
        (
            "SELECT TOP (5) k.Id FROM (SELECT h.Id FROM dbo.Posts FOR SYSTEM_TIME ALL AS h) AS k "
            "ORDER BY k.Id",
            "FOR SYSTEM_TIME",
        ),
    ],
)
@pytest.mark.asyncio
async def test_an_unprovable_relation_is_named_with_the_reason(sql: str, reason: str) -> None:
    result = await _analyze(sql, _facts("Posts"))

    assert result["risk_codes"] == ["row_limit_total_order_unproven"]
    assert reason in result["total_order_gaps"][0]["tie"]


@pytest.mark.asyncio
async def test_a_tied_outer_order_without_a_row_limit_is_reported_but_is_not_a_risk() -> None:
    result = await _analyze(
        "SELECT p.Id, p.Score FROM dbo.Posts AS p ORDER BY p.Score DESC", _facts("Posts")
    )

    # No row limit: the same rows come back whatever the tie order, so the
    # snapshot comparison stays allowed. The tie explains an order-only mismatch.
    assert result["direct_snapshot_supported"] is True
    assert result["result_order_proofs"] == []
    tie = result["result_order_ties"][0]
    assert tie["table"] == "dbo.Posts"
    assert "their order in the result is not defined" in tie["tie"]
    assert tie["tiebreaker_columns"] == ["Id"]


@pytest.mark.asyncio
async def test_catalog_column_types_are_read_for_the_join_check() -> None:
    class Executor:
        async def fetch_all(self, _database: str, query: str, params: Any = None):
            if "sys.columns AS c" in query and "sys.indexes" not in query:
                return [
                    {
                        "object_type": "USER_TABLE",
                        "column_name": "Id",
                        "is_nullable": 0,
                        "system_type_id": 56,
                        "max_length": 4,
                        "precision": 10,
                        "scale": 0,
                        "collation_name": None,
                    },
                    {
                        "object_type": "USER_TABLE",
                        "column_name": "DisplayName",
                        "is_nullable": 0,
                        "system_type_id": 231,
                        "max_length": 80,
                        "precision": 0,
                        "scale": 0,
                        "collation_name": "Latin1_General_CI_AS",
                    },
                ]
            return []

    facts = await CatalogConstraintService(Executor()).table_facts("appdb", "dbo", "Users")

    assert facts.column_types == {"id": INT, "displayname": NVARCHAR_40}
