"""Rewrites that return equal rows on a small data set but are not equal in general.

In every case the test database returns identical rows for both statements and
the candidate is faster. That is exactly the situation a DBA must not trust:
the data happens to hold no NULL, no duplicate and no tie. Each case must end
not deployment_ready, and the response must name why.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.catalog_constraints import TableConstraintFacts
from azure_sql_mcp.catalog_constraints import UniqueKey
from azure_sql_mcp.equivalence_preflight import EquivalencePreflightService
from tests.unit.test_performance_workflows import _service


class FakeCatalog:
    """A schema where nothing the rewrites rely on is enforced."""

    def __init__(self) -> None:
        self.tables = {
            name.casefold(): TableConstraintFacts(
                schema_name="dbo",
                table_name=name,
                exists=True,
                object_type="USER_TABLE",
                nullable={"id": False, "userid": True, "lasteditoruserid": True, "score": False},
                unique_keys=(UniqueKey(name=f"PK_{name}", kind="primary_key", columns=("Id",)),),
            )
            for name in ("Users", "Badges", "Posts")
        }

    async def table_facts(self, _database: str, schema_name: str, table_name: str):
        return self.tables.get(
            table_name.casefold(), TableConstraintFacts(schema_name, table_name)
        )


async def _tables_only(_database: str, _schema: str, _name: str) -> dict[str, Any]:
    return {"object_type": "USER_TABLE"}


CASES = [
    pytest.param(
        "SELECT u.Id FROM dbo.Users AS u "
        "WHERE u.Id NOT IN (SELECT b.UserId FROM dbo.Badges AS b)",
        "SELECT u.Id FROM dbo.Users AS u WHERE NOT EXISTS "
        "(SELECT 1 FROM dbo.Badges AS candidate WHERE candidate.UserId = u.Id)",
        "not_in_null_semantics",
        "UserId in dbo.Badges is nullable",
        id="not-in-with-nulls",
    ),
    pytest.param(
        "SELECT DISTINCT u.Id FROM dbo.Users AS u JOIN dbo.Badges AS b ON b.UserId = u.Id",
        "SELECT u.Id FROM dbo.Users AS u "
        "JOIN dbo.Badges AS candidate ON candidate.UserId = u.Id",
        "distinct_removed",
        "duplicate row",
        id="distinct-removed-with-join-fan-out",
    ),
    pytest.param(
        "SELECT u.Id FROM dbo.Users AS u WHERE EXISTS "
        "(SELECT 1 FROM dbo.Badges AS b WHERE b.UserId = u.Id)",
        "SELECT u.Id FROM dbo.Users AS u "
        "JOIN dbo.Badges AS candidate ON candidate.UserId = u.Id",
        "exists_join_swap",
        "once per match",
        id="inner-join-replacing-exists",
    ),
    pytest.param(
        "SELECT COUNT(p.LastEditorUserId) AS edits FROM dbo.Posts AS p",
        "SELECT COUNT(*) AS edits FROM dbo.Posts AS candidate",
        "count_null_semantics",
        "LastEditorUserId in dbo.Posts is nullable",
        id="count-column-versus-count-star",
    ),
    pytest.param(
        "SELECT p.Id FROM dbo.Posts AS p WHERE p.Score > 10 ORDER BY p.Id",
        "SELECT candidate.Id FROM dbo.Posts AS candidate WHERE candidate.Score > 10",
        "order_contract_changed",
        "removes the baseline outer ORDER BY",
        id="implicit-ordering",
    ),
    pytest.param(
        "SELECT TOP (10) p.Id FROM dbo.Posts AS p ORDER BY p.Score DESC",
        "SELECT TOP (10) candidate.Id FROM dbo.Posts AS candidate ORDER BY candidate.Score DESC",
        "row_limit_total_order_unproven",
        "append Id (primary_key PK_Posts) to the ORDER BY",
        id="top-ordered-by-a-column-with-ties",
    ),
    pytest.param(
        "SELECT TOP (10) p.Id FROM dbo.Posts AS p WHERE p.Score > 10",
        "SELECT TOP (10) candidate.Id FROM dbo.Posts AS candidate WHERE candidate.Score > 10",
        "unordered_row_limit",
        "unordered row limits",
        id="top-without-order-by",
    ),
    pytest.param(
        "SELECT p.Id FROM dbo.Posts AS p WHERE p.Score > 10",
        "SELECT candidate.Id FROM dbo.Posts AS candidate WITH (NOLOCK) "
        "WHERE candidate.Score > 10",
        "isolation_table_hint",
        "isolation table hint",
        id="nolock",
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("baseline", "candidate", "code", "named_reason"), CASES)
async def test_data_dependent_rewrite_is_never_deployment_ready(
    baseline: str,
    candidate: str,
    code: str,
    named_reason: str,
) -> None:
    catalog = FakeCatalog()
    preflight = EquivalencePreflightService(_tables_only, catalog=catalog)

    async def analyzer(database_name: str, sql: str) -> dict[str, Any]:
        return (await preflight.analyze(database_name, sql)).as_dict()

    service, _store, _plans, _executor = _service(equivalence_analyzer=analyzer)
    service.catalog = catalog
    case = service.start_case("appdb", baseline)
    session = service.start_session(case.case_id, "appdb")
    session_id = session["session_id"]
    candidate_id = service.add_candidate(session_id, candidate, strategy="predicate")[
        "candidate_id"
    ]

    finalist = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", baseline, candidate, phase="finalist"
    )
    # The rewrite is faster and, where it could be compared, returned the same rows.
    assert finalist["classification"] in {"improved", "performance_only"}
    selection_scope = (
        "proven" if finalist["classification"] == "improved" else "performance_only"
    )
    verification = await service.verify_logical_preconditions(
        "appdb", session_id, candidate_id, None
    )
    final = service.finalize_session(
        session_id,
        selected_candidate_id=candidate_id,
        stopping_reason="fastest measured candidate",
        selection_scope=selection_scope,
        precondition_verification=verification,
    )

    assert final["deployment_ready"] is False
    assert final["selected_candidate_proof_scope"] in {"snapshot_matched", "performance_only"}
    blocker = next(item for item in final["deployment_blockers"] if item["code"] == code)
    assert named_reason in blocker["reason"]
