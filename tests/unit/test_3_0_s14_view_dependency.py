"""A view whose definition starts with a comment is not a cycle (S14 item 6a).

compare_query_results refused the rewrite of a statement that reads a plain
view with "cyclic view dependency detected". The catalog keeps a module as it
was created, with the comment above CREATE VIEW. The header was then not
removed, the view's own name was read as a table of its body, and the walk
found the view inside itself.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.equivalence_preflight import EquivalencePreflightService
from tests.unit.test_performance_workflows import SnapshotExecutor
from tests.unit.test_performance_workflows import _service

BODY = (
    "SELECT p.Id, p.OwnerUserId, u.DisplayName FROM dbo.Posts AS p "
    "JOIN dbo.Users AS u ON u.Id = p.OwnerUserId"
)
STATEMENT = (
    "SELECT v.Id, p.Score FROM dbo.vPostOwner AS v "
    "JOIN dbo.Posts AS p ON p.Id = v.Id WHERE v.OwnerUserId = 5 ORDER BY v.Id"
)
REWRITE = (
    "SELECT candidate.Id, candidate.Score FROM dbo.Posts AS candidate "
    "JOIN dbo.Users AS u ON u.Id = candidate.OwnerUserId "
    "WHERE candidate.OwnerUserId = 5 ORDER BY candidate.Id"
)
DEFINITIONS = {
    "line_comment": "-- owner of each post\r\nCREATE VIEW dbo.vPostOwner AS " + BODY,
    "block_comment": "/* owner of each post\n   AS of 2026 */\nCREATE VIEW [dbo].[vPostOwner]\nAS\n" + BODY,
    "several_comments": "\n-- one\n/* two */\n  -- three\nCREATE OR ALTER VIEW dbo.vPostOwner AS " + BODY,
    "no_comment": "CREATE VIEW dbo.vPostOwner AS " + BODY,
}


def _resolver(definitions: dict[str, str]):
    async def resolve(_database: str, schema: str, name: str) -> dict[str, Any]:
        definition = definitions.get(name.casefold())
        if definition is None:
            return {"schema_name": schema, "object_name": name, "object_type": "USER_TABLE"}
        return {
            "schema_name": schema, "object_name": name, "object_type": "VIEW",
            "definition": definition, "is_encrypted": 0,
        }

    return resolve


@pytest.mark.asyncio
@pytest.mark.parametrize("form", list(DEFINITIONS))
async def test_a_statement_that_joins_a_view_to_one_of_its_tables_has_no_cycle(form: str) -> None:
    service = EquivalencePreflightService(_resolver({"vpostowner": DEFINITIONS[form]}))

    result = (await service.analyze("appdb", STATEMENT)).as_dict()

    assert result["unresolved_dependencies"] == []
    assert "cyclic_view_dependency" not in result["risk_codes"]
    assert [item["object_name"] for item in result["resolved_dependencies"]] == ["vPostOwner"]


@pytest.mark.asyncio
async def test_a_view_that_does_read_itself_through_another_view_is_still_a_cycle() -> None:
    service = EquivalencePreflightService(
        _resolver(
            {
                "va": "-- first\nCREATE VIEW dbo.vA AS SELECT b.Id FROM dbo.vB AS b",
                "vb": "/* second */ CREATE VIEW dbo.vB AS SELECT a.Id FROM dbo.vA AS a",
            }
        )
    )

    result = (await service.analyze("appdb", "SELECT a.Id FROM dbo.vA AS a")).as_dict()

    assert result["unresolved_dependencies"][0]["reason"] == "cyclic_view_dependency"


@pytest.mark.asyncio
async def test_the_rewrite_of_a_statement_over_such_a_view_is_compared() -> None:
    preflight = EquivalencePreflightService(
        _resolver({"vpostowner": DEFINITIONS["line_comment"]})
    )

    async def analyzer(database_name: str, sql: str) -> dict[str, Any]:
        return (await preflight.analyze(database_name, sql)).as_dict()

    rows = [{"Id": 1, "Score": 3}, {"Id": 2, "Score": 9}]
    executor = SnapshotExecutor(baseline_rows=rows, candidate_rows=rows)
    service, _store, _plans, _executor = _service(
        executor=executor, equivalence_analyzer=analyzer
    )

    result = await service.compare_query_results("appdb", STATEMENT, REWRITE)

    assert len(executor.sessions) == 1
    assert result["status"] == "match"
    assert "cyclic" not in str(result)
