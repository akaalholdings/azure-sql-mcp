"""Catalog constraint facts: the only source a proof may cite for keys and nullability."""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.catalog_constraints import CatalogConstraintService
from azure_sql_mcp.catalog_constraints import CheckConstraint
from azure_sql_mcp.catalog_constraints import ForeignKey
from azure_sql_mcp.catalog_constraints import TableConstraintFacts
from azure_sql_mcp.catalog_constraints import UniqueKey
from azure_sql_mcp.catalog_constraints import deterministic_order_key
from azure_sql_mcp.catalog_constraints import order_tiebreaker
from azure_sql_mcp.catalog_constraints import verify_precondition


def _posts(**overrides: Any) -> TableConstraintFacts:
    values: dict[str, Any] = {
        "schema_name": "dbo",
        "table_name": "Posts",
        "exists": True,
        "object_type": "USER_TABLE",
        "nullable": {"id": False, "owneruserid": True, "score": False, "slug": True},
        "unique_keys": (
            UniqueKey(name="PK_Posts", kind="primary_key", columns=("Id",)),
            UniqueKey(name="UQ_Posts_Slug", kind="unique_index", columns=("Slug",)),
        ),
        "foreign_keys": (
            ForeignKey(
                name="FK_Posts_Users",
                column_pairs=(("OwnerUserId", "Id"),),
                referenced_schema="dbo",
                referenced_table="Users",
            ),
        ),
        "check_constraints": (
            CheckConstraint(name="CK_Posts_Score", definition="([Score]>=(0))"),
        ),
    }
    values.update(overrides)
    return TableConstraintFacts(**values)


class CatalogExecutor:
    def __init__(self, rows_by_marker: dict[str, list[dict[str, Any]]]) -> None:
        self.rows_by_marker = rows_by_marker
        self.calls: list[tuple[str, str, tuple[Any, ...]]] = []

    async def fetch_all(self, database_name: str, query: str, params=None):
        self.calls.append((database_name, query, tuple(params or ())))
        for marker, rows in self.rows_by_marker.items():
            if marker in query:
                return rows
        return []


@pytest.mark.asyncio
async def test_table_facts_are_read_with_parameters_and_never_with_inlined_names() -> None:
    executor = CatalogExecutor(
        {
            "sys.foreign_keys": [
                {
                    "constraint_name": "FK_Posts_Users",
                    "is_disabled": 0,
                    "is_not_trusted": 1,
                    "column_name": "OwnerUserId",
                    "referenced_schema": "dbo",
                    "referenced_table": "Users",
                    "referenced_column": "Id",
                }
            ],
            "sys.check_constraints": [
                {
                    "constraint_name": "CK_Posts_Score",
                    "is_disabled": 0,
                    "is_not_trusted": 0,
                    "definition": "([Score]>=(0))",
                }
            ],
            "sys.index_columns": [
                {
                    "index_name": "PK_Posts",
                    "is_primary_key": 1,
                    "is_unique_constraint": 0,
                    "has_filter": 0,
                    "is_disabled": 0,
                    "is_hypothetical": 0,
                    "column_name": "Id",
                },
                {
                    "index_name": "UQ_Posts_Active",
                    "is_primary_key": 0,
                    "is_unique_constraint": 0,
                    "has_filter": 1,
                    "is_disabled": 0,
                    "is_hypothetical": 0,
                    "column_name": "Slug",
                },
            ],
            "sys.columns": [
                {"object_type": "USER_TABLE", "column_name": "Id", "is_nullable": 0},
                {"object_type": "USER_TABLE", "column_name": "Slug", "is_nullable": 1},
            ],
        }
    )

    facts = await CatalogConstraintService(executor).table_facts(
        "appdb", "dbo", "Posts'; DROP TABLE dbo.Posts;--"
    )

    assert facts.exists
    assert facts.nullable == {"id": False, "slug": True}
    assert [key.name for key in facts.unique_keys] == ["PK_Posts", "UQ_Posts_Active"]
    assert facts.unique_keys[1].has_filter is True
    assert facts.foreign_keys[0].is_not_trusted is True
    assert facts.check_constraints[0].definition == "([Score]>=(0))"
    for _database, query, params in executor.calls:
        assert "DROP TABLE" not in query
        assert params == ("dbo", "Posts'; DROP TABLE dbo.Posts;--")


@pytest.mark.asyncio
async def test_unknown_object_and_unreadable_catalog_prove_nothing() -> None:
    missing = await CatalogConstraintService(CatalogExecutor({})).table_facts(
        "appdb", "dbo", "NoSuchTable"
    )

    class Denied:
        async def fetch_all(self, *_args: Any, **_kwargs: Any):
            raise PermissionError("VIEW DEFINITION denied")

    denied = await CatalogConstraintService(Denied()).table_facts("appdb", "dbo", "Posts")

    for facts in (missing, denied):
        assert facts.exists is False
        assert deterministic_order_key(facts, ["Id"]) is None
        assert verify_precondition(
            facts,
            {"kind": "not_null", "schema_name": "dbo", "table_name": "Posts", "columns": ["Id"]},
        )["verified"] is False
    assert denied.read_error == "PermissionError"


def test_order_is_deterministic_only_on_a_usable_unique_not_null_key() -> None:
    facts = _posts()

    assert deterministic_order_key(facts, ["Score", "ID"]).name == "PK_Posts"
    # Score alone has ties; Slug is unique but nullable, so it is not accepted.
    assert deterministic_order_key(facts, ["Score"]) is None
    assert deterministic_order_key(facts, ["Slug"]) is None


@pytest.mark.parametrize(
    "flag",
    ["has_filter", "is_disabled", "is_hypothetical"],
)
def test_filtered_disabled_and_hypothetical_keys_do_not_prove_uniqueness(flag: str) -> None:
    facts = _posts(
        unique_keys=(
            UniqueKey(name="UX", kind="unique_index", columns=("Id",), **{flag: True}),
        )
    )

    assert deterministic_order_key(facts, ["Id"]) is None
    assert verify_precondition(
        facts,
        {"kind": "unique", "schema_name": "dbo", "table_name": "Posts", "columns": ["Id"]},
    )["verified"] is False


def test_tiebreaker_names_the_key_columns_missing_from_the_order() -> None:
    tiebreaker = order_tiebreaker(_posts(), ["Score"])

    assert tiebreaker == {
        "key_name": "PK_Posts",
        "key_kind": "primary_key",
        "key_columns": ["Id"],
        "missing_columns": ["Id"],
    }
    # A table with no usable NOT NULL unique key has no tiebreaker to offer.
    heap = _posts(unique_keys=())
    assert order_tiebreaker(heap, ["Score"]) is None


def test_not_null_precondition_fails_on_a_nullable_or_unknown_column() -> None:
    facts = _posts()
    base = {"kind": "not_null", "schema_name": "dbo", "table_name": "Posts"}

    assert verify_precondition(facts, {**base, "columns": ["Id", "Score"]})["verified"] is True
    nullable = verify_precondition(facts, {**base, "columns": ["OwnerUserId"]})
    assert nullable["verified"] is False
    assert "OwnerUserId" in nullable["reason"]
    assert verify_precondition(facts, {**base, "columns": ["Nope"]})["verified"] is False
    assert verify_precondition(facts, {**base, "columns": []})["verified"] is False


def test_foreign_key_precondition_requires_trust_and_the_exact_column_pairs() -> None:
    item = {
        "kind": "trusted_foreign_key",
        "schema_name": "dbo",
        "table_name": "Posts",
        "columns": ["OwnerUserId"],
        "referenced_schema": "dbo",
        "referenced_table": "Users",
        "referenced_columns": ["Id"],
    }

    assert verify_precondition(_posts(), item)["verified"] is True
    # WITH NOCHECK leaves is_not_trusted = 1: the optimizer may not rely on it, nor may a proof.
    untrusted = _posts(
        foreign_keys=(
            ForeignKey(
                name="FK_Posts_Users",
                column_pairs=(("OwnerUserId", "Id"),),
                referenced_schema="dbo",
                referenced_table="Users",
                is_not_trusted=True,
            ),
        )
    )
    result = verify_precondition(untrusted, item)
    assert result["verified"] is False
    assert "not trusted" in result["reason"]
    assert verify_precondition(_posts(), {**item, "referenced_table": "Accounts"})["verified"] is False


def test_check_precondition_must_match_the_expected_predicate_not_only_exist() -> None:
    item = {
        "kind": "trusted_check",
        "schema_name": "dbo",
        "table_name": "Posts",
        "constraint_name": "CK_Posts_Score",
        "expected_predicate": "Score >= 0",
    }

    assert verify_precondition(_posts(), item)["verified"] is True
    # Some trusted CHECK existing is not evidence for a different predicate.
    wrong = verify_precondition(_posts(), {**item, "expected_predicate": "Score > 10"})
    assert wrong["verified"] is False
    assert verify_precondition(_posts(), {**item, "expected_predicate": ""})["verified"] is False


def test_unknown_precondition_kind_is_rejected_not_passed() -> None:
    result = verify_precondition(
        _posts(),
        {"kind": "trust_me", "schema_name": "dbo", "table_name": "Posts"},
    )

    assert result["verified"] is False
    assert "kind" in result["reason"]
