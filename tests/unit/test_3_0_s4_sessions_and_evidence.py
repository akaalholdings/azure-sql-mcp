"""Session contract, evidence verdict and response size."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.artifacts import ExplainPlanArtifact
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.performance_store import ContractNotFoundError
from azure_sql_mcp.performance_store import PerformanceStore
from azure_sql_mcp.performance_workflows import profile_payload
from azure_sql_mcp.plans import PlansService
from azure_sql_mcp.plans import ProfiledPlanResult
from azure_sql_mcp.tuning_sessions import DEFAULT_EXECUTIONS
from azure_sql_mcp.tuning_sessions import lineage_execution_budget
from azure_sql_mcp.tuning_sessions import session_failure_diagnostic
from tests.unit.test_performance_workflows import _deep_policy
from tests.unit.test_performance_workflows import _service
from tests.unit.test_performance_workflows import _typed_binder

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "showplans"
BASELINE = "SELECT id FROM dbo.Items WHERE owner = @p"
CANDIDATE = "SELECT id FROM dbo.Items AS candidate WHERE owner = @p"
FOUR_CASES = [
    {"name": name, "values": {"p": value}, "types": {"p": "int"}}
    for name, value in (("common", 1), ("rare", 2), ("null", None), ("boundary", 0))
]


def _read_only_policy() -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {"environment": "production", "allow_read": True},
            },
        }
    )


def test_an_unknown_session_id_is_a_not_found_caller_error() -> None:
    """Review finding 41: a garbled id must not read as an Azure SQL failure."""

    service, _store, _plans, _executor = _service()

    with pytest.raises(ContractNotFoundError) as raised:
        service.get_session("session-does-not-exist")

    diagnostic = session_failure_diagnostic(raised.value)
    assert diagnostic is not None
    assert diagnostic["diagnostic_code"] == "not_found"
    assert diagnostic["retryable"] is False
    assert "another process" in diagnostic["message"]
    # The redactor replaces quoted text; the message must survive it.
    assert "'" not in str(raised.value)
    assert "session-does-not-exist" in str(raised.value)


def test_an_id_from_another_process_state_is_not_found() -> None:
    service, _store, _plans, _executor = _service()
    other = PerformanceStore(db_path=":memory:")
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")
    session = service.start_session(case.case_id, "appdb")

    with pytest.raises(ContractNotFoundError) as raised:
        other.get_session(session["session_id"])

    assert session_failure_diagnostic(raised.value)["diagnostic_code"] == "not_found"


@pytest.mark.asyncio
async def test_an_expired_session_says_expired() -> None:
    current = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
    service, _store, _plans, _executor = _service(clock=lambda: current[0])
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")
    session = service.start_session(case.case_id, "appdb")
    candidate = service.add_candidate(
        session["session_id"], "SELECT id FROM dbo.Items AS candidate", strategy="predicate"
    )
    current[0] += timedelta(hours=1)

    with pytest.raises(Exception) as raised:
        await service.benchmark_candidate(
            session["session_id"],
            candidate["candidate_id"],
            "appdb",
            "SELECT id FROM dbo.Items",
            "SELECT id FROM dbo.Items AS candidate",
        )

    diagnostic = session_failure_diagnostic(raised.value)
    assert diagnostic is not None
    assert diagnostic["diagnostic_code"] == "session_expired"


def test_a_database_error_is_not_claimed_as_a_session_error() -> None:
    assert session_failure_diagnostic(RuntimeError("login failed")) is None


def test_default_budget_is_120_and_the_policy_caps_it() -> None:
    service, _store, _plans, _executor = _service()
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")

    capped = service.start_session(case.case_id, "appdb")
    service.database_policy = _deep_policy()
    other_case = service.start_case("appdb", "SELECT id FROM dbo.Orders")
    default = service.start_session(other_case.case_id, "appdb")

    assert DEFAULT_EXECUTIONS == 120
    # The test policy allows 80 per session; a default must not ask for more.
    assert capped["execution_limit"] == 80
    assert default["execution_limit"] == 120


def test_an_explicit_budget_over_the_policy_is_refused_not_trimmed() -> None:
    service, _store, _plans, _executor = _service()
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")

    with pytest.raises(PermissionError, match="exceeds the local database policy"):
        service.start_session(case.case_id, "appdb", execution_limit=500)


@pytest.mark.asyncio
async def test_default_budget_covers_the_rewrite_plus_index_lineage_for_four_cases() -> None:
    """Screen four cases, validate four, then prove an index on the rewrite."""

    service, store, _plans, _executor = _service(binder=_typed_binder())
    service.database_policy = _deep_policy()
    case = service.start_case("appdb", BASELINE, parameter_cases=FOUR_CASES)
    session = service.start_session(case.case_id, "appdb")
    plan = lineage_execution_budget(4)

    assert plan == {
        "rewrite_screening": 32,
        "rewrite_finalist": 56,
        "index_child_finalist": 60,
        "total": 148,
    }
    assert session["execution_limit"] == 148
    assert session["budget"]["rewrite_plus_index_lineage"] == {**plan, "fits": True}

    candidate = service.add_candidate(session["session_id"], CANDIDATE, strategy="predicate")
    for phase in ("screening", "finalist"):
        await service.benchmark_candidate(
            session["session_id"], candidate["candidate_id"], "appdb", BASELINE, CANDIDATE,
            phase=phase, parameter_cases=FOUR_CASES,
        )
    used = store.execution_budget_usage(session["session_id"])
    assert used["consumed"] == plan["rewrite_screening"] + plan["rewrite_finalist"]
    assert used["remaining"] >= plan["index_child_finalist"]


def test_a_budget_too_small_for_the_lineage_says_so() -> None:
    service, _store, _plans, _executor = _service(binder=_typed_binder())
    case = service.start_case("appdb", BASELINE, parameter_cases=FOUR_CASES)

    session = service.start_session(case.case_id, "appdb")

    assert session["execution_limit"] == 80
    assert session["budget"]["rewrite_plus_index_lineage"]["fits"] is False


def test_tune_query_gets_a_reason_instead_of_an_error_on_a_read_only_policy() -> None:
    """Review finding 43: the case and its evidence are still useful without a session."""

    service, _store, _plans, _executor = _service()
    service.database_policy = _read_only_policy()
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")

    result = service.start_session_if_permitted(case.case_id, "appdb")

    assert result["session_id"] is None
    assert result["session_available"] is False
    assert "allow_benchmark" in result["policy_reason"]


def test_a_session_is_sized_to_a_small_policy() -> None:
    service, _store, _plans, _executor = _service()
    service.database_policy = DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "max_benchmark_executions": 10,
                }
            },
        }
    )
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")

    result = service.start_session_if_permitted(case.case_id, "appdb")

    assert result["session_available"] is True
    assert result["execution_limit"] == 10


@pytest.mark.asyncio
async def test_active_evidence_is_refused_before_any_database_read() -> None:
    service, _store, plans, _executor = _service()
    service.database_policy = _read_only_policy()
    sql = "SELECT id FROM dbo.Items"
    case = service.start_case("appdb", sql)
    collector = AsyncMock(return_value={"available": True})

    with pytest.raises(PermissionError):
        await service.collect_case_evidence(
            case.case_id, "appdb", sql, {"query_store": collector},
            window_minutes=15, execute_query=True,
        )

    collector.assert_not_awaited()
    assert plans.calls == []


@pytest.mark.asyncio
async def test_read_only_evidence_works_on_a_read_only_policy() -> None:
    service, _store, _plans, _executor = _service()
    service.database_policy = _read_only_policy()
    sql = "SELECT id FROM dbo.Items"
    case = service.start_case("appdb", sql)

    result = await service.collect_case_evidence(
        case.case_id, "appdb", sql,
        {"query_store": AsyncMock(return_value={"available": True})},
        window_minutes=15,
    )

    assert result["outcome"] == "healthy"


@pytest.mark.asyncio
async def test_database_wide_noise_does_not_make_the_query_actionable() -> None:
    """Review finding 47: a stale statistic on another table says nothing about this query."""

    service, _store, _plans, _executor = _service()
    sql = "SELECT id FROM dbo.Items"
    case = service.start_case("appdb", sql)

    result = await service.collect_case_evidence(
        case.case_id, "appdb", sql,
        {
            "query_store_history": AsyncMock(return_value={"available": True, "rows": []}),
            "statistics": AsyncMock(return_value={"stale_statistics_count": 3}),
            "blocking": AsyncMock(return_value={"blocking_session_count": 2}),
        },
        window_minutes=15,
    )

    assert result["outcome"] == "healthy"
    assert result["outcome_basis"]["query_scoped_sections"] == ["query_store_history"]
    assert result["outcome_basis"]["database_context_actionable"] is True
    assert result["outcome_basis"]["database_context_sections"] == ["blocking", "statistics"]


def _fixture_profile(name: str) -> ProfiledPlanResult:
    raw_xml = (FIXTURES / name).read_text(encoding="utf-8")
    summary = PlansService(None, None).summarize_showplan_xml(raw_xml)  # type: ignore[arg-type]
    return ProfiledPlanResult(
        plan=ExplainPlanArtifact(
            database_name="appdb", analyze=True, summary=summary, raw_xml=raw_xml
        ),
        result_sets=[
            QueryResult(columns=("id",), rows=[{"id": 1}], column_type_signatures=("int",))
        ],
        elapsed_wall_ms=120.0,
        user_query_executions=1,
        truncated=False,
        metric_provenance="test",
    )


def test_the_actual_plan_result_carries_the_digest_and_the_findings() -> None:
    """The agent must see the slow operator without running the query a second time."""

    payload = profile_payload(_fixture_profile("heap_scan_nonsargable.xml"))

    assert payload["plan_digest"]["contract"] == "plan_digest_v1"
    assert payload["plan_digest"]["statements"][0]["top_operators"]
    assert payload["plan_findings"]["findings"]
    # Ranked by cost when the plan has no operator times, and it says so.
    assert payload["plan_digest"]["ranking_basis"] in {"self_elapsed_ms", "estimated_self_cost"}


@pytest.mark.asyncio
async def test_a_plan_finding_on_the_query_makes_the_case_actionable() -> None:
    service, _store, plans, _executor = _service()
    sql = "SELECT id FROM dbo.Items"
    case = service.start_case("appdb", sql)
    plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        return_value=_fixture_profile("heap_scan_nonsargable.xml")
    )

    result = await service.collect_case_evidence(
        case.case_id, "appdb", sql,
        {"query_store_history": AsyncMock(return_value={"available": True, "rows": []})},
        window_minutes=15, execute_query=True,
    )

    assert result["outcome"] == "actionable"
    assert result["outcome_basis"]["profile_findings"] > 0
    assert result["profile"]["plan_digest"]["statements"]


@pytest.mark.asyncio
async def test_an_auto_bound_case_is_benchmarked_by_name() -> None:
    """The agent never saw the bound values, so it cannot send them back."""

    service, _store, _plans, _executor = _service(binder=_typed_binder())
    case = service.start_case("appdb", BASELINE, parameter_cases=FOUR_CASES[:2])
    session = service.start_session(case.case_id, "appdb")
    candidate = service.add_candidate(session["session_id"], CANDIDATE, strategy="predicate")

    by_default = await service.benchmark_candidate(
        session["session_id"], candidate["candidate_id"], "appdb", BASELINE, CANDIDATE,
    )

    assert [item["parameter_case"] for item in by_default["parameter_results"]] == [
        "common",
        "rare",
    ]


@pytest.mark.asyncio
async def test_a_case_registered_in_another_process_needs_its_values_again() -> None:
    service, store, _plans, _executor = _service(binder=_typed_binder())
    case = service.start_case("appdb", BASELINE, parameter_cases=FOUR_CASES[:1])
    session = service.start_session(case.case_id, "appdb")
    candidate = service.add_candidate(session["session_id"], CANDIDATE, strategy="predicate")
    service._registered_parameter_cases.clear()  # a restart: values are never persisted

    with pytest.raises(ValueError, match="not held by this MCP process"):
        await service.benchmark_candidate(
            session["session_id"], candidate["candidate_id"], "appdb", BASELINE, CANDIDATE,
            parameter_cases=[{"name": "common"}],
        )
    # Nothing in the stored case could have rebuilt the values.
    receipts = store.get_performance_case(case.case_id).metadata["parameter_case_receipts"]
    assert [receipt["values_persisted"] for receipt in receipts] == [False]


@pytest.mark.asyncio
async def test_session_summary_does_not_grow_with_every_evidence_record() -> None:
    """Backlog: get_tuning_session after every result made tokens grow O(N^2)."""

    service, _store, _plans, _executor = _service(binder=_typed_binder())
    service.database_policy = _deep_policy()
    case = service.start_case("appdb", BASELINE, parameter_cases=FOUR_CASES)
    session = service.start_session(case.case_id, "appdb", max_candidates=20, execution_limit=1000)
    session_id = session["session_id"]
    sizes: list[int] = []
    for ordinal in range(6):
        sql = CANDIDATE.replace("candidate", f"candidate{ordinal}")
        candidate_id = service.add_candidate(session_id, sql, strategy="predicate")[
            "candidate_id"
        ]
        await service.benchmark_candidate(
            session_id, candidate_id, "appdb", BASELINE, sql, parameter_cases=FOUR_CASES
        )
        sizes.append(len(json.dumps(service.get_session(session_id), default=str)))

    summary = service.get_session(session_id)
    full = service.get_session(session_id, view="full")

    assert "evidence" not in summary
    assert summary["evidence_count"] == 6
    assert len(summary["latest_results"]) == 6
    latest = summary["latest_results"][-1]
    assert latest["classification"] == "promising"
    assert latest["parameter_cases"][0] == {
        "parameter_case": "common",
        "baseline": 100.0,
        "candidate": 50.0,
        "verdict": "improved",
    }
    assert len(full["evidence"]) == 6
    per_candidate = (sizes[-1] - sizes[0]) / 5
    full_size = len(json.dumps(full, default=str))
    # Each added candidate costs a compact row, a small fraction of its evidence record.
    assert per_candidate < 2_500
    assert sizes[-1] < full_size / 4


@pytest.mark.asyncio
async def test_plan_delta_is_compact_and_ranked_by_self_time() -> None:
    """Review finding 97: 8-9 KB of cost-ranked operators per case pointed at the wrong one."""

    service, _store, plans, _executor = _service()
    plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        return_value=_fixture_profile("seek_residual_lookup_sort.xml")
    )
    sql = "SELECT id FROM dbo.Items"
    case = service.start_case("appdb", sql)
    session = service.start_session(case.case_id, "appdb")
    rewrite = "SELECT id FROM dbo.Items AS candidate"
    candidate = service.add_candidate(session["session_id"], rewrite, strategy="predicate")

    result = await service.benchmark_candidate(
        session["session_id"], candidate["candidate_id"], "appdb", sql, rewrite
    )

    delta = result["parameter_results"][0]["plan_delta"]
    assert delta["plan_changed"] is False
    assert len(delta["baseline"]["top_operators"]) <= 3
    assert set(delta["baseline"]["top_operators"][0]) <= {
        "node_id", "operator", "object", "self_elapsed_ms", "estimated_cost_share", "rows",
    }
    assert len(json.dumps(delta)) < 2_000


def test_session_evidence_is_read_without_loading_other_sessions() -> None:
    service, store, _plans, _executor = _service()
    loaded: list[str] = []
    original = store._row_to_contract

    def counting(row: Any, contract_cls: Any, identifier: str) -> Any:
        loaded.append(identifier)
        return original(row, contract_cls, identifier)

    from azure_sql_mcp.performance_contracts import EvidenceEnvelopeV1

    for session_id in ("session-a", "session-b", "session-b"):
        store.create_evidence(
            EvidenceEnvelopeV1(source="azure-sql-mcp", metadata={"session_id": session_id})
        )
    store._row_to_contract = counting  # type: ignore[method-assign]

    found = store.list_evidence_for_session("session-a")

    assert len(found) == 1
    assert len(loaded) == 1
