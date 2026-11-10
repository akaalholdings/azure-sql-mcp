"""A NOT NULL precondition is not demanded for a NULL case the statement tests itself.

O07: NOT IN on a nullable column was rewritten to NOT EXISTS plus an explicit
NULL-existence test, which is equal to NOT IN for every data state. Finalize
still answered not deployment-ready, "UserId in dbo.Comments is nullable".
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.equivalence_contract import rewrite_requirements
from tests.unit.test_3_0_s4_proof_scopes import FakeCatalog
from tests.unit.test_3_0_s4_proof_scopes import _improved_finalist
from tests.unit.test_3_0_s4_proof_scopes import _table
from tests.unit.test_performance_workflows import _service

NOT_IN = (
    "SELECT u.Id, u.DisplayName FROM dbo.Users AS u WHERE u.Reputation > 100 AND u.Id NOT IN ("
    "SELECT c.UserId FROM dbo.Comments AS c "
    "WHERE c.CreationDate >= '2013-10-01' AND c.CreationDate < '2013-11-01')"
)
_ANTI_JOIN = (
    "SELECT candidate.Id, candidate.DisplayName FROM dbo.Users AS candidate "
    "WHERE candidate.Reputation > 100 AND NOT EXISTS ("
    "SELECT 1 FROM dbo.Comments AS c WHERE c.UserId = candidate.Id "
    "AND c.CreationDate >= '2013-10-01' AND c.CreationDate < '2013-11-01')"
)
PLAIN_NOT_EXISTS = _ANTI_JOIN
# NOT IN returns no row when the subquery yields a NULL: the second test keeps that.
NULL_HANDLED = _ANTI_JOIN + (
    " AND NOT EXISTS (SELECT 1 FROM dbo.Comments AS n "
    "WHERE n.CreationDate >= '2013-10-01' AND n.CreationDate < '2013-11-01' "
    "AND n.UserId IS NULL)"
)
# A NULL test over other rows than the NOT IN subquery read.
NULL_TEST_ON_OTHER_ROWS = _ANTI_JOIN + (
    " AND NOT EXISTS (SELECT 1 FROM dbo.Comments AS n "
    "WHERE n.CreationDate >= '2014-01-01' AND n.UserId IS NULL)"
)
USERS_ID = {"kind": "not_null", "schema_name": "dbo", "table_name": "Users", "columns": ["Id"]}
COMMENTS_USERID = {
    "kind": "not_null", "schema_name": "dbo", "table_name": "Comments", "columns": ["UserId"],
}


def _not_in(candidate: str) -> dict[str, Any]:
    [requirement] = [
        item
        for item in rewrite_requirements(NOT_IN, candidate)
        if item["code"] == "not_in_null_semantics"
    ]
    return requirement


def test_an_explicit_null_test_over_the_same_rows_removes_the_precondition() -> None:
    requirement = _not_in(NULL_HANDLED)

    # The tested value still must not be NULL; the subquery column is handled.
    assert requirement["preconditions"] == [USERS_ID]
    assert requirement["resolved"] is True
    [handled] = requirement["handled_by_statement"]
    assert {key: handled[key] for key in COMMENTS_USERID} == COMMENTS_USERID
    assert "IS NULL" in handled["how"]


def test_a_plain_not_exists_still_needs_both_columns_not_null() -> None:
    requirement = _not_in(PLAIN_NOT_EXISTS)

    assert requirement["preconditions"] == [COMMENTS_USERID, USERS_ID]
    assert "handled_by_statement" not in requirement


def test_a_null_test_that_cannot_be_matched_is_needed_unless_the_statement_handles_null() -> None:
    requirement = _not_in(NULL_TEST_ON_OTHER_ROWS)

    conditional = requirement["preconditions"][0]
    assert {key: conditional[key] for key in COMMENTS_USERID} == COMMENTS_USERID
    assert conditional["needed_unless"] == "the statement handles NULL"
    assert "filter" in conditional["unless_reason"]
    assert requirement["preconditions"][1] == USERS_ID


def _catalog() -> FakeCatalog:
    return FakeCatalog(
        {
            "users": _table("Users", {"id": False, "reputation": False, "displayname": True}),
            "comments": _table("Comments", {"userid": True, "creationdate": False}),
        }
    )


async def _finalized(candidate: str) -> dict[str, Any]:
    service, _store, _plans, _executor = _service()
    service.catalog = _catalog()
    session_id, candidate_id, _result = await _improved_finalist(service, NOT_IN, candidate)
    verification = await service.verify_logical_preconditions("appdb", session_id, candidate_id)
    return service.finalize_session(
        session_id,
        selected_candidate_id=candidate_id,
        stopping_reason="measured",
        precondition_verification=verification,
    )


@pytest.mark.asyncio
async def test_finalize_accepts_a_rewrite_that_handles_the_null_case_itself() -> None:
    final = await _finalized(NULL_HANDLED)

    assert final["selected_candidate_proof_scope"] == "constraint_backed"
    assert final["deployment_ready"] is True
    assert final["deployment_blockers"] == []


@pytest.mark.asyncio
async def test_finalize_still_blocks_a_plain_not_exists_on_a_nullable_column() -> None:
    final = await _finalized(PLAIN_NOT_EXISTS)

    assert final["deployment_ready"] is False
    assert [item["code"] for item in final["deployment_blockers"]] == ["not_in_null_semantics"]
    assert "nullable" in final["deployment_blockers"][0]["reason"]


@pytest.mark.asyncio
async def test_finalize_names_a_conditional_precondition_instead_of_a_hard_blocker() -> None:
    final = await _finalized(NULL_TEST_ON_OTHER_ROWS)

    # Not proven, and said as what it is: needed unless the statement handles NULL.
    assert final["deployment_ready"] is False
    assert [item["code"] for item in final["deployment_blockers"]] == ["snapshot_match_only"]
    [conditional] = final["equivalence_basis"]["conditional_preconditions"]
    assert conditional["code"] == "not_in_null_semantics"
    assert conditional["needed_unless"] == "the statement handles NULL"
    assert conditional["table_name"] == "Comments" and conditional["columns"] == ["UserId"]
    assert conditional["unless_reason"]
