"""Decision D2: a match on today's data is never, by itself, ready for deployment."""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.catalog_constraints import TableConstraintFacts
from azure_sql_mcp.catalog_constraints import UniqueKey
from azure_sql_mcp.performance_workflows import PerformanceWorkflowService
from azure_sql_mcp.performance_workflows import deployment_readiness
from tests.unit.test_performance_workflows import _service


class FakeCatalog:
    def __init__(self, tables: dict[str, TableConstraintFacts]) -> None:
        self.tables = tables

    async def table_facts(self, _database: str, schema_name: str, table_name: str):
        return self.tables.get(
            table_name.casefold(), TableConstraintFacts(schema_name, table_name)
        )


def _table(name: str, nullable: dict[str, bool], *keys: UniqueKey) -> TableConstraintFacts:
    return TableConstraintFacts(
        schema_name="dbo",
        table_name=name,
        exists=True,
        object_type="USER_TABLE",
        nullable=nullable,
        unique_keys=keys,
    )


NOT_IN = "SELECT u.Id FROM dbo.Users AS u WHERE u.Id NOT IN (SELECT b.UserId FROM dbo.Badges AS b)"
NOT_EXISTS = (
    "SELECT u.Id FROM dbo.Users AS u WHERE NOT EXISTS "
    "(SELECT 1 FROM dbo.Badges AS candidate WHERE candidate.UserId = u.Id)"
)
SARGABLE_BASELINE = "SELECT p.Id FROM dbo.Posts AS p WHERE YEAR(p.CreationDate) = 2024"
SARGABLE_CANDIDATE = (
    "SELECT candidate.Id FROM dbo.Posts AS candidate "
    "WHERE candidate.CreationDate >= '20240101' AND candidate.CreationDate < '20250101'"
)


async def _improved_finalist(
    service: PerformanceWorkflowService,
    baseline: str,
    candidate: str,
) -> tuple[str, str, dict[str, Any]]:
    case = service.start_case("appdb", baseline)
    session = service.start_session(case.case_id, "appdb")
    created = service.add_candidate(session["session_id"], candidate, strategy="predicate")
    result = await service.benchmark_candidate(
        session["session_id"],
        created["candidate_id"],
        "appdb",
        baseline,
        candidate,
        phase="finalist",
    )
    assert result["classification"] == "improved", result["reason"]
    return session["session_id"], created["candidate_id"], result


def _blocker_codes(result: dict[str, Any]) -> list[str]:
    return [item["code"] for item in result["deployment_blockers"]]


@pytest.mark.asyncio
async def test_a_snapshot_match_alone_is_not_deployment_ready() -> None:
    service, _store, _plans, _executor = _service()
    session_id, candidate_id, _ = await _improved_finalist(
        service, SARGABLE_BASELINE, SARGABLE_CANDIDATE
    )

    final = service.finalize_session(
        session_id,
        selected_candidate_id=candidate_id,
        stopping_reason="best measured rewrite",
    )

    assert final["selected_candidate_proof_scope"] == "snapshot_matched"
    assert final["semantic_equivalence"] == "snapshot_matched"
    assert final["deployment_ready"] is False
    assert _blocker_codes(final) == ["snapshot_match_only"]
    assert final["equivalence_basis"]["precondition_basis"] == "not_declared"
    assert final["equivalence_basis"]["tested_parameter_cases"] == [
        {"parameter_case": "default", "row_count": 1, "snapshot_isolation_verified": True}
    ]


@pytest.mark.asyncio
async def test_attesting_that_no_precondition_exists_does_not_buy_readiness() -> None:
    service, _store, _plans, _executor = _service()
    service.catalog = FakeCatalog({})
    session_id, candidate_id, _ = await _improved_finalist(
        service, SARGABLE_BASELINE, SARGABLE_CANDIDATE
    )

    verification = await service.verify_logical_preconditions(
        "appdb", session_id, candidate_id, []
    )
    final = service.finalize_session(
        session_id,
        selected_candidate_id=candidate_id,
        stopping_reason="best measured rewrite",
        precondition_verification=verification,
    )

    assert verification["precondition_basis"] == "attested_none"
    assert final["selected_candidate_proof_scope"] == "snapshot_matched"
    assert final["deployment_ready"] is False


@pytest.mark.asyncio
async def test_benchmark_names_what_the_rewrite_depends_on() -> None:
    service, _store, _plans, _executor = _service()

    _session, _candidate, finalist = await _improved_finalist(service, NOT_IN, NOT_EXISTS)

    assert [item["code"] for item in finalist["rewrite_requirements"]] == [
        "not_in_null_semantics"
    ]


@pytest.mark.asyncio
async def test_not_in_rewrite_on_a_nullable_column_is_named_and_blocked() -> None:
    service, _store, _plans, _executor = _service()
    service.catalog = FakeCatalog(
        {
            "users": _table("Users", {"id": False}),
            "badges": _table("Badges", {"userid": True}),
        }
    )
    session_id, candidate_id, _ = await _improved_finalist(service, NOT_IN, NOT_EXISTS)

    verification = await service.verify_logical_preconditions(
        "appdb", session_id, candidate_id, None
    )
    final = service.finalize_session(
        session_id,
        selected_candidate_id=candidate_id,
        stopping_reason="best measured rewrite",
        precondition_verification=verification,
    )

    assert final["selected_candidate_proof_scope"] == "snapshot_matched"
    assert final["deployment_ready"] is False
    blocker = next(
        item for item in final["deployment_blockers"] if item["code"] == "not_in_null_semantics"
    )
    assert "UserId in dbo.Badges is nullable" in blocker["reason"]


@pytest.mark.asyncio
async def test_not_in_rewrite_backed_by_not_null_columns_is_constraint_backed() -> None:
    service, _store, _plans, _executor = _service()
    service.catalog = FakeCatalog(
        {
            "users": _table("Users", {"id": False}),
            "badges": _table("Badges", {"userid": False}),
        }
    )
    session_id, candidate_id, _ = await _improved_finalist(service, NOT_IN, NOT_EXISTS)

    verification = await service.verify_logical_preconditions(
        "appdb", session_id, candidate_id, None
    )
    final = service.finalize_session(
        session_id,
        selected_candidate_id=candidate_id,
        stopping_reason="best measured rewrite",
        precondition_verification=verification,
    )

    assert final["selected_candidate_proof_scope"] == "constraint_backed"
    assert final["deployment_ready"] is True
    assert final["deployment_blockers"] == []
    assert final["automatic_deployment_approved"] is False
    assert final["equivalence_basis"]["precondition_basis"] == "verified_constraints"


@pytest.mark.asyncio
async def test_a_hinted_candidate_is_not_ready_even_when_constraint_backed() -> None:
    service, _store, _plans, _executor = _service()
    service.catalog = FakeCatalog(
        {
            "users": _table("Users", {"id": False}),
            "badges": _table("Badges", {"userid": False}),
        }
    )
    hinted = NOT_EXISTS + " OPTION (MAXDOP 1)"
    session_id, candidate_id, _ = await _improved_finalist(service, NOT_IN, hinted)

    verification = await service.verify_logical_preconditions(
        "appdb", session_id, candidate_id, None
    )
    final = service.finalize_session(
        session_id,
        selected_candidate_id=candidate_id,
        stopping_reason="best measured rewrite",
        precondition_verification=verification,
    )

    assert final["selected_candidate_proof_scope"] == "constraint_backed"
    assert final["deployment_ready"] is False
    assert _blocker_codes(final) == ["hinted_candidate"]
    assert "MAXDOP" in final["deployment_blockers"][0]["reason"]


@pytest.mark.asyncio
async def test_failed_parity_blocks_a_constraint_backed_candidate() -> None:
    service, _store, _plans, _executor = _service()
    service.catalog = FakeCatalog(
        {
            "users": _table("Users", {"id": False}),
            "badges": _table("Badges", {"userid": False}),
        }
    )
    session_id, candidate_id, _ = await _improved_finalist(service, NOT_IN, NOT_EXISTS)

    verification = await service.verify_logical_preconditions(
        "appdb", session_id, candidate_id, None
    )
    final = service.finalize_session(
        session_id,
        selected_candidate_id=candidate_id,
        stopping_reason="best measured rewrite",
        precondition_verification=verification,
        parity_status="failed",
    )

    assert final["deployment_ready"] is False
    assert _blocker_codes(final) == ["parity_failed"]


DISTINCT_BASELINE = (
    "SELECT DISTINCT u.Id FROM dbo.Users AS u JOIN dbo.Badges AS b ON b.UserId = u.Id"
)
DISTINCT_CANDIDATE = (
    "SELECT u.Id FROM dbo.Users AS u JOIN dbo.Badges AS candidate ON candidate.UserId = u.Id"
)


@pytest.mark.asyncio
async def test_removed_distinct_needs_a_declared_and_verified_unique_key() -> None:
    service, _store, _plans, _executor = _service()
    service.catalog = FakeCatalog(
        {
            "badges": _table(
                "Badges",
                {"userid": False},
                UniqueKey(name="UQ_Badges_User", kind="unique_index", columns=("UserId",)),
            ),
        }
    )
    session_id, candidate_id, _ = await _improved_finalist(
        service, DISTINCT_BASELINE, DISTINCT_CANDIDATE
    )

    wrong_kind = await service.verify_logical_preconditions(
        "appdb",
        session_id,
        candidate_id,
        [{"kind": "not_null", "schema_name": "dbo", "table_name": "Badges", "columns": ["UserId"]}],
    )
    right_kind = await service.verify_logical_preconditions(
        "appdb",
        session_id,
        candidate_id,
        [{"kind": "unique", "schema_name": "dbo", "table_name": "Badges", "columns": ["UserId"]}],
    )

    # A true but unrelated fact does not cover a duplicate-row risk.
    assert wrong_kind["constraint_backed"] is False
    assert wrong_kind["requirements"][0]["missing_kinds"] == ["unique"]
    assert right_kind["constraint_backed"] is True
    assert right_kind["relevance"] == "catalog-verified, agent-declared relevance"


@pytest.mark.asyncio
async def test_a_declared_precondition_that_the_catalog_refutes_stops_finalization() -> None:
    service, _store, _plans, _executor = _service()
    service.catalog = FakeCatalog({"badges": _table("Badges", {"userid": False})})
    session_id, candidate_id, _ = await _improved_finalist(
        service, DISTINCT_BASELINE, DISTINCT_CANDIDATE
    )
    verification = await service.verify_logical_preconditions(
        "appdb",
        session_id,
        candidate_id,
        [{"kind": "unique", "schema_name": "dbo", "table_name": "Badges", "columns": ["UserId"]}],
    )

    with pytest.raises(ValueError, match="no enabled, unfiltered unique key"):
        service.finalize_session(
            session_id,
            selected_candidate_id=candidate_id,
            stopping_reason="best measured rewrite",
            precondition_verification=verification,
        )

    # The session stays open so the agent can correct the claim or pick another candidate.
    assert service.get_session(session_id)["session"]["status"] == "finalist_validation"


@pytest.mark.asyncio
async def test_verification_of_one_candidate_cannot_back_another() -> None:
    service, _store, _plans, _executor = _service()
    service.catalog = FakeCatalog(
        {
            "users": _table("Users", {"id": False}),
            "badges": _table("Badges", {"userid": False}),
        }
    )
    session_id, candidate_id, _ = await _improved_finalist(service, NOT_IN, NOT_EXISTS)
    verification = await service.verify_logical_preconditions(
        "appdb", session_id, candidate_id, None
    )

    with pytest.raises(ValueError, match="another candidate"):
        service.finalize_session(
            session_id,
            selected_candidate_id=candidate_id,
            stopping_reason="best measured rewrite",
            precondition_verification={**verification, "candidate_id": "candidate-other"},
        )


def test_readiness_needs_a_backed_scope_no_hint_and_no_failed_parity() -> None:
    ready = deployment_readiness(proof_scope="constraint_backed")
    index_only = deployment_readiness(proof_scope="unchanged_sql")
    matched = deployment_readiness(proof_scope="snapshot_matched")
    nothing = deployment_readiness(proof_scope=None)

    assert ready == {"deployment_ready": True, "deployment_blockers": []}
    assert index_only["deployment_ready"] is True
    assert matched["deployment_ready"] is False
    assert nothing == {"deployment_ready": False, "deployment_blockers": []}
    # Parity that was not evaluated is not a failure; only a failed check blocks.
    assert deployment_readiness(
        proof_scope="unchanged_sql", parity_status="not_evaluated"
    )["deployment_ready"] is True


def test_an_index_on_unchanged_sql_is_ready_without_preconditions() -> None:
    """An index cannot change the rows a statement returns, so no constraint is needed."""

    from azure_sql_mcp.performance_contracts import EvidenceEnvelopeV1

    service, store, _plans, _executor = _service()
    sql = "SELECT id FROM dbo.Items WHERE owner = 7"
    case = service.start_case("appdb", sql)
    session_id = service.start_session(case.case_id, "appdb")["session_id"]
    candidate_id = service.add_candidate(session_id, sql, strategy="index")["candidate_id"]
    service.sessions.start_screening(session_id)
    service.sessions.mark_candidate_finalist(session_id, candidate_id)
    evidence = store.create_evidence(
        EvidenceEnvelopeV1(
            kind="index_finalist",
            query_fingerprint=case.query_fingerprint,
            database_fingerprint=case.database_fingerprint,
            observed_execution_count=15,
            metrics={"classification": "improved"},
            metadata={
                "session_id": session_id,
                "candidate_id": candidate_id,
                "phase": "finalist",
                "proof_scope": "aba_result_stability",
                "equivalence": [
                    {
                        "status": "match",
                        "proven_for_parameter_case": True,
                        "same_sql": True,
                        "plan_used_expected_index": True,
                    }
                ],
            },
        )
    )
    service.sessions.record_candidate_result(
        session_id,
        candidate_id,
        state="improved",
        finalist_runs=5,
        parameter_cases=1,
        executions=15,
        evidence_ids=(evidence.evidence_id,),
    )

    final = service.finalize_session(
        session_id,
        selected_candidate_id=candidate_id,
        stopping_reason="index proven by A-B-A",
    )

    assert final["selected_candidate_proof_scope"] == "unchanged_sql"
    assert final["deployment_ready"] is True
    assert final["equivalence_basis"]["precondition_basis"] == "not_applicable"


@pytest.mark.asyncio
async def test_a_match_on_zero_rows_proves_nothing() -> None:
    """Parameter values that return no row make any two statements look equal."""

    from tests.unit.test_performance_workflows import SnapshotExecutor

    service, _store, _plans, _executor = _service(
        executor=SnapshotExecutor(baseline_rows=[], candidate_rows=[])
    )
    case = service.start_case("appdb", SARGABLE_BASELINE)
    session_id = service.start_session(case.case_id, "appdb")["session_id"]
    candidate_id = service.add_candidate(session_id, SARGABLE_CANDIDATE, strategy="predicate")[
        "candidate_id"
    ]

    finalist = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", SARGABLE_BASELINE, SARGABLE_CANDIDATE,
        phase="finalist",
    )

    assert finalist["equivalence"][0]["vacuous"] is True
    assert finalist["classification"] == "inconclusive"
    assert "zero rows" in finalist["reason"]


@pytest.mark.asyncio
async def test_result_comparison_reports_a_snapshot_match_not_a_proof() -> None:
    service, _store, _plans, _executor = _service()

    result = await service.compare_query_results("appdb", NOT_IN, NOT_EXISTS)

    assert result["status"] == "match"
    assert result["equivalence_scope"] == "snapshot_matched_for_parameter_case"
    assert [item["code"] for item in result["rewrite_requirements"]] == ["not_in_null_semantics"]
