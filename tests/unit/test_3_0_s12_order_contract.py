"""The order contract compares resolved base columns, not alias text.

A rewrite that pages keys in a derived table keeps the caller's order. Flagging
it as order_contract_changed stops the proof before any row is compared.
"""

from __future__ import annotations

import pytest

from azure_sql_mcp.equivalence_contract import order_contract_risk
from azure_sql_mcp.equivalence_contract import outer_order_by_keys

BASELINE = (
    "SELECT p.Id, p.Title, p.CreationDate, p.AnswerCount FROM dbo.Posts AS p "
    "WHERE p.PostTypeId = 1 ORDER BY p.CreationDate DESC, p.Id DESC "
    "OFFSET @skip ROWS FETCH NEXT 25 ROWS ONLY"
)
PAGED_KEYS = (
    "(SELECT p0.Id, p0.CreationDate FROM dbo.Posts AS p0 WHERE p0.PostTypeId = 1 "
    "ORDER BY p0.CreationDate DESC, p0.Id DESC OFFSET @skip ROWS FETCH NEXT 25 ROWS ONLY)"
)
BASE_KEYS = ((("dbo.posts.creationdate", True), ("dbo.posts.id", True)),)


def test_join_back_rewrite_from_the_live_comparison_keeps_the_order_contract() -> None:
    # Regression guard for the exact statement pair of task T6. This pair already
    # passed before stream S12; the forms below are the ones that were flagged.
    candidate = (
        "SELECT p.Id, p.Title, p.CreationDate, p.AnswerCount FROM "
        f"{PAGED_KEYS} AS k JOIN dbo.Posts AS p ON p.Id = k.Id "
        "ORDER BY p.CreationDate DESC, p.Id DESC"
    )

    assert order_contract_risk(BASELINE, candidate) is None


@pytest.mark.parametrize(
    "candidate",
    [
        # The alias p now names the derived table of paged rows.
        "SELECT p.Id, p.Title, p.CreationDate, p.AnswerCount FROM "
        "(SELECT p0.Id, p0.Title, p0.CreationDate, p0.AnswerCount FROM dbo.Posts AS p0 "
        " WHERE p0.PostTypeId = 1 ORDER BY p0.CreationDate DESC, p0.Id DESC "
        " OFFSET @skip ROWS FETCH NEXT 25 ROWS ONLY) AS p "
        "ORDER BY p.CreationDate DESC, p.Id DESC",
        # Ordered by the key relation, whose columns are the same base columns.
        "SELECT p.Id, p.Title, p.CreationDate, p.AnswerCount FROM "
        f"{PAGED_KEYS} AS k JOIN dbo.Posts AS p ON p.Id = k.Id "
        "ORDER BY k.CreationDate DESC, k.Id DESC",
        # The same through a CTE, with a renamed column.
        "WITH k AS (SELECT p0.Id, p0.CreationDate AS Created FROM dbo.Posts AS p0 "
        "WHERE p0.PostTypeId = 1 ORDER BY p0.CreationDate DESC, p0.Id DESC "
        "OFFSET @skip ROWS FETCH NEXT 25 ROWS ONLY) "
        "SELECT p.Id, p.Title, p.CreationDate, p.AnswerCount FROM k "
        "JOIN dbo.Posts AS p ON p.Id = k.Id ORDER BY k.Created DESC, k.Id DESC",
        # Unqualified names bind to the select list, which names p.CreationDate and p.Id.
        "SELECT p.Id, p.Title, p.CreationDate, p.AnswerCount FROM "
        f"{PAGED_KEYS} AS k JOIN dbo.Posts AS p ON p.Id = k.Id "
        "ORDER BY CreationDate DESC, Id DESC",
    ],
)
def test_same_base_columns_and_directions_are_the_same_order_contract(candidate: str) -> None:
    assert outer_order_by_keys(candidate) == BASE_KEYS
    assert order_contract_risk(BASELINE, candidate) is None


@pytest.mark.parametrize(
    "candidate",
    [
        # Direction changed on the base column.
        "SELECT k.Id FROM (SELECT p0.Id, p0.CreationDate FROM dbo.Posts AS p0) AS k "
        "ORDER BY k.CreationDate ASC, k.Id DESC",
        # The derived column is an expression, not the base column.
        "SELECT k.Id FROM (SELECT p0.Id, DATEADD(DAY, 1, p0.CreationDate) AS CreationDate "
        "FROM dbo.Posts AS p0) AS k ORDER BY k.CreationDate DESC, k.Id DESC",
        # The derived column is another base column under the old name.
        "SELECT k.Id FROM (SELECT p0.Id, p0.LastActivityDate AS CreationDate "
        "FROM dbo.Posts AS p0) AS k ORDER BY k.CreationDate DESC, k.Id DESC",
        # Another table's column with the same name.
        "SELECT k.Id FROM (SELECT c.Id, c.CreationDate FROM dbo.Comments AS c) AS k "
        "ORDER BY k.CreationDate DESC, k.Id DESC",
    ],
)
def test_a_changed_order_is_still_flagged_through_a_derived_table(candidate: str) -> None:
    risk = order_contract_risk(BASELINE, candidate)

    assert risk is not None
    assert risk["risk"] == "order_contract_changed"
