from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.artifacts import ExplainPlanArtifact
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.performance_store import PerformanceStore
from azure_sql_mcp.performance_workflows import PerformanceWorkflowService
from azure_sql_mcp.performance_workflows import classify_benchmark
from azure_sql_mcp.performance_workflows import compare_result_sets
from azure_sql_mcp.plans import ProfiledPlanResult
from azure_sql_mcp.safe_sql import SafeSqlValidator
from azure_sql_mcp.tuning_sessions import TuningSessionStateMachine


def _policy(max_executions: int = 80) -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "allow_test_indexes": True,
                    "allow_plan_apply": False,
                    "max_benchmark_executions": max_executions,
                }
            },
        }
    )


def _profile(elapsed_ms: float, *, marker: str) -> ProfiledPlanResult:
    summary = {
        "statement_count": 1,
        "operator_count": 1,
        "actual_metrics": {
            "actual_cpu_ms": elapsed_ms / 2,
            "actual_elapsed_ms": elapsed_ms,
            "actual_rows": 1,
            "actual_logical_reads": None,
            "actual_physical_reads": None,
            "query_metric_source": "showplan_query_time_stats",
            "read_metric_source": "not_available_as_reliable_query_total",
        },
        "warnings": [],
        "missing_indexes": [],
        "top_operators": [{"physical_op": marker}],
    }
    return ProfiledPlanResult(
        plan=ExplainPlanArtifact(
            database_name="appdb",
            analyze=True,
            summary=summary,
            raw_xml=f"<ShowPlanXML marker='{marker}' />",
        ),
        result_sets=[
            QueryResult(
                columns=("id",),
                rows=[{"id": 1}],
                column_type_signatures=("synthetic-int",),
            )
        ],
        elapsed_wall_ms=elapsed_ms,
        user_query_executions=1,
        truncated=False,
        metric_provenance="test",
    )


class SnapshotExecutor:
    def __init__(
        self,
        baseline_rows: list[dict[str, Any]] | None = None,
        candidate_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.baseline_rows = baseline_rows or [{"id": 1}]
        self.candidate_rows = candidate_rows or [{"id": 1}]
        self.sessions: list[list[str]] = []

    async def execute_session_exactly_once(
        self,
        _database_name: str,
        statements: list[str],
        *,
        max_rows: int | None = None,
    ) -> list[list[QueryResult]]:
        self.sessions.append(statements)
        assert max_rows is not None
        return [
            [],
            [],
            [
                QueryResult(
                    columns=("id",),
                    rows=self.baseline_rows,
                    column_type_signatures=("synthetic-int",),
                )
            ],
            [
                QueryResult(
                    columns=("id",),
                    rows=self.candidate_rows,
                    column_type_signatures=("synthetic-int",),
                )
            ],
            [],
        ]


class RoutedPlans:
    def __init__(self, *, baseline_ms: float = 100, candidate_ms: float = 50) -> None:
        self.baseline_ms = baseline_ms
        self.candidate_ms = candidate_ms
        self.calls: list[str] = []

    async def profile_query(self, _database_name: str, sql: str) -> ProfiledPlanResult:
        self.calls.append(sql)
        is_candidate = "candidate" in sql.casefold()
        return _profile(
            self.candidate_ms if is_candidate else self.baseline_ms,
            marker="candidate" if is_candidate else "baseline",
        )


def _service(
    *,
    executor: SnapshotExecutor | None = None,
    plans: RoutedPlans | None = None,
    binder=None,
) -> tuple[PerformanceWorkflowService, PerformanceStore, RoutedPlans, SnapshotExecutor]:
    store = PerformanceStore(db_path=":memory:")
    actual_plans = plans or RoutedPlans()
    actual_executor = executor or SnapshotExecutor()
    service = PerformanceWorkflowService(
        executor=actual_executor,  # type: ignore[arg-type]
        plans=actual_plans,  # type: ignore[arg-type]
        validator=SafeSqlValidator(),
        store=store,
        sessions=TuningSessionStateMachine(store),
        database_policy=_policy(),
        row_limit=50,
        parameter_binder=binder,
    )
    return service, store, actual_plans, actual_executor


def _start(
    service: PerformanceWorkflowService,
    *,
    baseline: str = "SELECT id FROM dbo.Items",
    candidate: str = "SELECT id FROM dbo.Items AS candidate",
    parameter_cases: list[dict[str, Any]] | None = None,
) -> tuple[str, str]:
    case = service.start_case("appdb", baseline, parameter_cases=parameter_cases)
    session = service.start_session(case.case_id)
    tuning_candidate = service.add_candidate(
        session["session_id"],
        candidate,
        strategy="predicate",
    )
    return session["session_id"], tuning_candidate["candidate_id"]


@pytest.mark.asyncio
async def test_measured_sample_executes_each_query_exactly_once() -> None:
    service, _store, plans, executor = _service()
    session_id, candidate_id = _start(service)

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
    )

    assert result["classification"] == "improved"
    assert result["executions"] == 8
    assert len(plans.calls) == 6
    assert len(executor.sessions) == 1
    assert result["equivalence"][0]["proven_for_parameter_case"] is True


@pytest.mark.asyncio
async def test_losing_candidate_does_not_end_session_or_erase_next_win() -> None:
    plans = RoutedPlans(baseline_ms=100, candidate_ms=140)
    service, store, _plans, _executor = _service(plans=plans)
    session_id, first_id = _start(service)

    first = await service.benchmark_candidate(
        session_id,
        first_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
    )
    assert first["classification"] == "regressed"
    assert store.get_session(session_id).status == "screening"

    second_sql = "SELECT id FROM dbo.Items AS candidate WHERE 1 = 1"
    second = service.add_candidate(session_id, second_sql, strategy="combined")
    plans.candidate_ms = 45
    result = await service.benchmark_candidate(
        session_id,
        second["candidate_id"],
        "appdb",
        "SELECT id FROM dbo.Items",
        second_sql,
        runs_override=3,
    )

    assert result["classification"] == "improved"
    assert result["session_continues"] is True
    assert store.get_candidate(first_id).state == "regressed"
    assert store.get_candidate(second["candidate_id"]).state == "screening"

    calls_before_retry = len(plans.calls)
    with pytest.raises(ValueError, match="already been measured"):
        await service.benchmark_candidate(
            session_id,
            second["candidate_id"],
            "appdb",
            "SELECT id FROM dbo.Items",
            second_sql,
            runs_override=3,
        )
    assert len(plans.calls) == calls_before_retry


@pytest.mark.asyncio
async def test_candidate_failure_is_inconclusive_and_session_continues() -> None:
    service, store, plans, _executor = _service()
    session_id, candidate_id = _start(service)
    plans.profile_query = AsyncMock(side_effect=TimeoutError("synthetic timeout"))  # type: ignore[method-assign]

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
    )

    assert result["state"] == "inconclusive"
    assert result["session_continues"] is True
    assert store.get_candidate(candidate_id).state == "inconclusive"


@pytest.mark.asyncio
async def test_common_rare_null_and_boundary_parameter_buckets_are_all_measured() -> None:
    bound_cases: list[tuple[str, Any]] = []

    async def bind(_database: str, sql: str, values: dict[str, Any]) -> str:
        value = values["p"]
        bound_cases.append((sql, value))
        literal = "NULL" if value is None else str(value)
        return f"DECLARE @p int = {literal}; {sql}"

    parameter_cases = [
        {"name": "common", "values": {"p": 1}},
        {"name": "rare", "values": {"p": 999999}},
        {"name": "NULL", "values": {"p": None}},
        {"name": "boundary", "values": {"p": 0}},
    ]
    service, _store, _plans, _executor = _service(binder=bind)
    baseline = "SELECT id FROM dbo.Items WHERE value = @p"
    candidate = "SELECT id FROM dbo.Items AS candidate WHERE value = @p"
    session_id, candidate_id = _start(
        service,
        baseline=baseline,
        candidate=candidate,
        parameter_cases=parameter_cases,
    )

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate,
        parameter_cases=parameter_cases,
        runs_override=3,
    )

    assert [item["parameter_case"] for item in result["parameter_results"]] == [
        "common",
        "rare",
        "NULL",
        "boundary",
    ]
    assert result["executions"] == 32
    assert len(bound_cases) == 8


@pytest.mark.asyncio
async def test_four_bucket_screening_and_finalist_fit_exactly_in_default_budget() -> None:
    async def bind(_database: str, sql: str, values: dict[str, Any]) -> str:
        value = values["p"]
        literal = "NULL" if value is None else str(value)
        return f"DECLARE @p int = {literal}; {sql}"

    parameter_cases = [
        {"name": "common", "values": {"p": 1}},
        {"name": "rare", "values": {"p": 999999}},
        {"name": "NULL", "values": {"p": None}},
        {"name": "boundary", "values": {"p": 0}},
    ]
    service, store, _plans, _executor = _service(binder=bind)
    baseline = "SELECT id FROM dbo.Items WHERE value = @p"
    candidate = "SELECT id FROM dbo.Items AS candidate WHERE value = @p"
    session_id, candidate_id = _start(
        service,
        baseline=baseline,
        candidate=candidate,
        parameter_cases=parameter_cases,
    )

    screening = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate,
        phase="screening",
        parameter_cases=parameter_cases,
    )
    finalist = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate,
        phase="finalist",
        parameter_cases=parameter_cases,
    )

    assert screening["executions"] == 32
    assert finalist["executions"] == 48
    stored = store.get_candidate(candidate_id)
    assert stored.executions == 80
    assert stored.parameter_cases == 4
    assert stored.state == "improved"


@pytest.mark.asyncio
async def test_duplicate_and_order_sensitive_comparison_fails_closed() -> None:
    executor = SnapshotExecutor(
        baseline_rows=[{"id": 1}, {"id": 1}, {"id": 2}],
        candidate_rows=[{"id": 1}, {"id": 2}, {"id": 2}],
    )
    service, _store, _plans, _executor = _service(executor=executor)

    duplicate_mismatch = await service.compare_query_results(
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items",
        compare_order=False,
    )
    assert duplicate_mismatch["status"] == "mismatch"

    executor.baseline_rows = [{"id": 1}, {"id": 2}]
    executor.candidate_rows = [{"id": 2}, {"id": 1}]
    ordered = await service.compare_query_results(
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items",
        compare_order=True,
    )
    unordered = await service.compare_query_results(
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items",
        compare_order=False,
    )
    assert ordered["status"] == "mismatch"
    assert unordered["status"] == "match"
    assert executor.sessions[-1][-1] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"


@pytest.mark.asyncio
async def test_failed_snapshot_comparison_charges_reserved_execution_pair() -> None:
    service, _store, _plans, executor = _service()
    executor.execute_session_exactly_once = AsyncMock(  # type: ignore[method-assign]
        side_effect=TimeoutError("synthetic snapshot timeout")
    )

    result = await service.compare_query_results(
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
    )

    assert result["status"] == "inconclusive"
    assert result["executions"] == 2
    assert result["execution_count_is_conservative"] is True


def test_truncated_result_comparison_is_never_proven() -> None:
    baseline = QueryResult(
        columns=("id",),
        rows=[{"id": 1}, {"id": 2}],
        column_type_signatures=("synthetic-int",),
    )
    candidate = QueryResult(
        columns=("id",),
        rows=[{"id": 1}, {"id": 2}],
        column_type_signatures=("synthetic-int",),
    )

    result = compare_result_sets(
        baseline,
        candidate,
        row_limit=1,
        compare_order=True,
        same_snapshot=True,
    )

    assert result["status"] == "inconclusive"
    assert result["proven_for_parameter_case"] is False


def test_ordered_comparison_is_type_sensitive() -> None:
    baseline = QueryResult(
        columns=("value",),
        rows=[{"value": 1}],
        column_type_signatures=("synthetic-int",),
    )
    candidate = QueryResult(
        columns=("value",),
        rows=[{"value": 1.0}],
        column_type_signatures=("synthetic-float",),
    )

    result = compare_result_sets(
        baseline,
        candidate,
        row_limit=10,
        compare_order=True,
        same_snapshot=True,
    )

    assert result["status"] == "mismatch"
    assert result["types_match"] is False


@pytest.mark.asyncio
async def test_incomplete_triage_evidence_is_never_healthy() -> None:
    service, store, _plans, _executor = _service()
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")

    async def available() -> dict[str, Any]:
        return {"status": "ok"}

    async def unavailable() -> dict[str, Any]:
        raise PermissionError("synthetic missing permission")

    result = await service.collect_case_evidence(
        case.case_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        {"resource": available, "query_store": unavailable},
        window_minutes=60,
    )

    assert result["outcome"] == "partial"
    assert result["incomplete_evidence_can_be_healthy"] is False
    persisted = store.get_evidence(result["evidence"]["evidence_id"])
    assert persisted.metadata["outcome"] == "partial"
    assert "SELECT id" not in persisted.to_json()


@pytest.mark.asyncio
async def test_nested_unavailable_or_truncated_evidence_is_partial() -> None:
    service, _store, _plans, _executor = _service()
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")

    async def nested_gap() -> dict[str, Any]:
        return {
            "resource": {"available": False},
            "rows": {"truncated": True},
        }

    result = await service.collect_case_evidence(
        case.case_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        {"combined": nested_gap},
        window_minutes=60,
    )

    assert result["outcome"] == "partial"
    section = result["sections"]["combined"]
    assert section["available"] is True
    assert section["complete"] is False
    assert section["truncated"] is True
    assert {gap["reason"] for gap in section["evidence_gaps"]} == {
        "truncated",
        "unavailable",
    }


def test_finalize_marks_unresolved_candidates_inconclusive() -> None:
    service, _store, _plans, _executor = _service()
    session_id, candidate_id = _start(service)

    result = service.finalize_session(
        session_id,
        selected_candidate_id=None,
        stopping_reason="evidence budget exhausted",
    )

    assert result["session"]["status"] == "completed"
    assert result["leaderboard"][0]["candidate_id"] == candidate_id
    assert result["leaderboard"][0]["state"] == "inconclusive"


def test_classification_uses_the_selected_objective() -> None:
    parameter_results = [
        {
            "baseline": {
                "elapsed_ms": 100.0,
                "cpu_ms": 20.0,
                "noise_ratio": 0.0,
            },
            "candidate": {
                "elapsed_ms": 50.0,
                "cpu_ms": 40.0,
                "noise_ratio": 0.0,
            },
        }
    ]
    equivalence = [{"status": "match"}]

    elapsed, _ = classify_benchmark(
        parameter_results,
        equivalence,
        objective="elapsed_time",
    )
    cpu, _ = classify_benchmark(
        parameter_results,
        equivalence,
        objective="cpu",
    )

    assert elapsed == "improved"
    assert cpu == "regressed"
