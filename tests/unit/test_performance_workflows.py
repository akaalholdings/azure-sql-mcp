from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import Mock
from uuid import UUID

import pytest

from azure_sql_mcp.artifacts import ExplainPlanArtifact
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.connection import StatementDispatchPrevented
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.equivalence_contract import analyze_equivalence_preflight
from azure_sql_mcp.param_binding import ParameterExecutionContract
from azure_sql_mcp.param_binding import SP_EXECUTESQL_CONTROL_INPUT_SIZES
from azure_sql_mcp.param_binding import SqlParameterType
from azure_sql_mcp.param_binding import TypedParameter
from azure_sql_mcp.performance_contracts import EvidenceEnvelopeV1
from azure_sql_mcp.performance_contracts import PerformanceCaseV1
from azure_sql_mcp.performance_store import IdempotencyConflictError
from azure_sql_mcp.performance_store import PerformanceStore
from azure_sql_mcp.performance_workflows import PerformanceWorkflowService
from azure_sql_mcp.performance_workflows import aggregate_samples
from azure_sql_mcp.performance_workflows import classify_benchmark
from azure_sql_mcp.performance_workflows import compare_result_collections
from azure_sql_mcp.performance_workflows import compare_result_sets
from azure_sql_mcp.performance_workflows import extract_profile_metrics
from azure_sql_mcp.performance_workflows import profile_result_fingerprint
from azure_sql_mcp.performance_workflows import parameter_case_fingerprint
from azure_sql_mcp.performance_workflows import parameter_case_receipt
from azure_sql_mcp.plans import ProfiledPlanResult
from azure_sql_mcp.query_identity import legacy_database_fingerprint
from azure_sql_mcp.query_identity import legacy_query_fingerprint
from azure_sql_mcp.safe_sql import SafeSqlValidator
from azure_sql_mcp.tuning_sessions import InvalidTransitionError
from azure_sql_mcp.tuning_sessions import TuningBudgetExceeded
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


def _deep_policy() -> DatabasePolicySet:
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
                    "max_benchmark_executions": 80,
                    "max_tuning_candidates": 60,
                    "max_tuning_session_executions": 2000,
                    "max_tuning_session_minutes": 360,
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


def test_plan_fingerprint_excludes_per_execution_metrics() -> None:
    first = extract_profile_metrics(_profile(100, marker="same-plan"))
    second = extract_profile_metrics(_profile(25, marker="same-plan"))
    changed = extract_profile_metrics(_profile(25, marker="different-plan"))

    assert first["plan_fingerprint"] == second["plan_fingerprint"]
    assert changed["plan_fingerprint"] != first["plan_fingerprint"]


@pytest.mark.asyncio
async def test_active_case_matching_rejects_legacy_fingerprints() -> None:
    service, store, _plans, _executor = _service()
    sql = "SELECT id FROM dbo.Items"
    case = store.create_performance_case(
        PerformanceCaseV1(
            query_fingerprint=legacy_query_fingerprint(sql),
            database_fingerprint=legacy_database_fingerprint("appdb"),
        )
    )

    with pytest.raises(ValueError, match="SQL fingerprint"):
        await service.collect_case_evidence(
            case.case_id,
            "appdb",
            sql,
            {"query_store": AsyncMock(return_value={"available": True})},
            window_minutes=15,
        )


@pytest.mark.asyncio
async def test_explicit_server_binding_allows_legacy_case_recovery() -> None:
    service, store, _plans, _executor = _service(allow_legacy_state=True)
    sql = "SELECT id FROM dbo.Items"
    case = store.create_performance_case(
        PerformanceCaseV1(
            query_fingerprint=legacy_query_fingerprint(sql),
            database_fingerprint=legacy_database_fingerprint("appdb"),
        )
    )

    result = await service.collect_case_evidence(
        case.case_id,
        "appdb",
        sql,
        {"query_store": AsyncMock(return_value={"available": True})},
        window_minutes=15,
    )

    assert result["outcome"] == "healthy"


class SnapshotExecutor:
    def __init__(
        self,
        baseline_rows: list[dict[str, Any]] | None = None,
        candidate_rows: list[dict[str, Any]] | None = None,
        *,
        baseline_columns: tuple[str, ...] = ("id",),
        candidate_columns: tuple[str, ...] = ("id",),
        baseline_type_signatures: tuple[str, ...] = ("synthetic-int",),
        candidate_type_signatures: tuple[str, ...] = ("synthetic-int",),
    ) -> None:
        self.baseline_rows = (
            [{"id": 1}] if baseline_rows is None else baseline_rows
        )
        self.candidate_rows = (
            [{"id": 1}] if candidate_rows is None else candidate_rows
        )
        self.baseline_columns = baseline_columns
        self.candidate_columns = candidate_columns
        self.baseline_type_signatures = baseline_type_signatures
        self.candidate_type_signatures = candidate_type_signatures
        self.sessions: list[list[str]] = []
        self.session_parameters: list[Any] = []
        self.session_input_sizes: list[Any] = []
        self.candidate_dispatches = 0
        self.baseline_completed = False

    async def execute_session_exactly_once(
        self,
        _database_name: str,
        statements: list[str],
        *,
        max_rows: int | None = None,
        statement_params=None,
        statement_input_sizes=None,
        before_statement_dispatch=None,
    ) -> list[list[QueryResult]]:
        self.sessions.append(statements)
        self.session_parameters.append(statement_params)
        self.session_input_sizes.append(statement_input_sizes)
        assert max_rows is not None
        assert statement_params is not None
        results = [
            [],
            [],
            [
                QueryResult(
                    columns=self.baseline_columns,
                    rows=self.baseline_rows,
                    column_type_signatures=self.baseline_type_signatures,
                )
            ],
            [
                QueryResult(
                    columns=self.candidate_columns,
                    rows=self.candidate_rows,
                    column_type_signatures=self.candidate_type_signatures,
                )
            ],
            [],
            [],
        ]
        if before_statement_dispatch is not None:
            before_statement_dispatch(0)
            before_statement_dispatch(1)
            before_statement_dispatch(2)
        self.baseline_completed = True
        if before_statement_dispatch is not None:
            try:
                before_statement_dispatch(3)
            except Exception as exc:
                raise StatementDispatchPrevented(3, exc) from exc
        self.candidate_dispatches += 1
        return results


class DispatchFailureExecutor(SnapshotExecutor):
    def __init__(self, fail_at_statement: int) -> None:
        super().__init__()
        self.fail_at_statement = fail_at_statement

    async def execute_session_exactly_once(
        self,
        _database_name: str,
        statements: list[str],
        *,
        max_rows: int | None = None,
        statement_params=None,
        statement_input_sizes=None,
        before_statement_dispatch=None,
    ) -> list[list[QueryResult]]:
        assert max_rows is not None
        assert statement_params is not None
        assert before_statement_dispatch is not None
        for statement_index in range(self.fail_at_statement + 1):
            before_statement_dispatch(statement_index)
        raise RuntimeError(
            f"synthetic statement {self.fail_at_statement} failure"
        )


class RoutedPlans:
    def __init__(self, *, baseline_ms: float = 100, candidate_ms: float = 50) -> None:
        self.baseline_ms = baseline_ms
        self.candidate_ms = candidate_ms
        self.calls: list[str] = []
        self.parameterized_contracts: list[ParameterExecutionContract] = []

    async def profile_query(self, _database_name: str, sql: str) -> ProfiledPlanResult:
        self.calls.append(sql)
        is_candidate = "candidate" in sql.casefold()
        return _profile(
            self.candidate_ms if is_candidate else self.baseline_ms,
            marker="candidate" if is_candidate else "baseline",
        )

    async def profile_parameterized_query(
        self,
        database_name: str,
        contract: ParameterExecutionContract,
    ) -> ProfiledPlanResult:
        self.parameterized_contracts.append(contract)
        return await self.profile_query(database_name, contract.sql_text)


def _typed_binder(
    bound_cases: list[tuple[str, Any]] | None = None,
):
    async def bind(
        _database: str,
        sql: str,
        parameter_case: dict[str, Any],
    ) -> ParameterExecutionContract:
        value = parameter_case["values"]["p"]
        if bound_cases is not None:
            bound_cases.append((sql, value))
        return ParameterExecutionContract(
            sql_text=sql,
            bucket_id=str(parameter_case["name"]),
            parameters=(
                TypedParameter(
                    name="@p",
                    sql_type=SqlParameterType.from_sql(
                        str(parameter_case["types"]["p"])
                    ),
                    value=value,
                    provenance="synthetic_test_case",
                ),
            ),
            provenance="synthetic_test_case",
        )

    return bind


def _service(
    *,
    executor: SnapshotExecutor | None = None,
    plans: RoutedPlans | None = None,
    binder=None,
    allow_legacy_state: bool = False,
    equivalence_analyzer=None,
    clock=None,
) -> tuple[PerformanceWorkflowService, PerformanceStore, RoutedPlans, SnapshotExecutor]:
    store = PerformanceStore(db_path=":memory:")
    actual_plans = plans or RoutedPlans()
    actual_executor = executor or SnapshotExecutor()
    service = PerformanceWorkflowService(
        executor=actual_executor,  # type: ignore[arg-type]
        plans=actual_plans,  # type: ignore[arg-type]
        validator=SafeSqlValidator(),
        store=store,
        sessions=TuningSessionStateMachine(store, clock=clock),
        database_policy=_policy(),
        row_limit=50,
        parameter_binder=binder,
        allow_legacy_state=allow_legacy_state,
        equivalence_analyzer=equivalence_analyzer,
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
    session = service.start_session(case.case_id, "appdb")
    tuning_candidate = service.add_candidate(
        session["session_id"],
        candidate,
        strategy="predicate",
    )
    return session["session_id"], tuning_candidate["candidate_id"]


def test_start_session_accepts_policy_authorized_multi_hour_budget() -> None:
    service, store, _plans, _executor = _service()
    service.database_policy = _deep_policy()
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")

    session = service.start_session(
        case.case_id,
        "appdb",
        max_candidates=60,
        execution_limit=2000,
        time_limit_minutes=360,
        idempotency_key="deep-session",
    )

    assert session["max_candidates"] == 60
    assert session["execution_limit"] == 2000
    assert session["time_limit_seconds"] == 21_600
    assert session["replay_metadata"]["request_fingerprint"]
    store.close()


def test_expired_session_view_hides_actionable_budget_but_keeps_lifecycle_state() -> None:
    current = [datetime(2026, 1, 1, tzinfo=timezone.utc)]

    def clock() -> datetime:
        return current[0]

    service, store, _plans, _executor = _service(clock=clock)
    try:
        session_id, candidate_id = _start(service)
        service.sessions.start_screening(session_id)
        current[0] += timedelta(seconds=20 * 60 + 1)

        view = service.get_session(session_id)
        assert view["session"]["status"] == "screening"
        assert view["session"]["effective_status"] == "expired"
        assert view["session"]["deadline_exceeded"] is True
        assert view["session"]["accepts_new_work"] is False
        assert view["session"]["accepts_finalization"] is True
        assert view["session"]["available"] is False
        assert view["budget"]["actionable"] is False
        assert view["budget"]["executions_remaining"] == 0
        assert view["budget"]["raw_executions_remaining"] == 80

        with pytest.raises(TuningBudgetExceeded):
            service.sessions.ensure_dispatch_allowed(session_id)
        with pytest.raises(TuningBudgetExceeded):
            service.add_candidate(session_id, "SELECT id FROM dbo.Other", strategy="join")

        service.sessions.record_candidate_result(
            session_id,
            candidate_id,
            state="inconclusive",
            failure_code="late_timeout",
        )
        finalized = service.finalize_session(
            session_id,
            selected_candidate_id=None,
            stopping_reason="late terminal result recorded",
        )
        assert finalized["session"]["status"] == "completed"
        assert finalized["session"]["effective_status"] == "completed"
        assert finalized["session"]["availability_reason"] is None
        assert store.get_session(session_id).status == "completed"
    finally:
        store.close()


def test_exhausted_session_budget_reports_no_new_work() -> None:
    service, store, _plans, _executor = _service()
    try:
        case = service.start_case("appdb", "SELECT id FROM dbo.Items")
        session = service.start_session(
            case.case_id,
            "appdb",
            max_candidates=1,
            execution_limit=1,
        )
        candidate = service.add_candidate(
            session["session_id"],
            "SELECT id FROM dbo.Items AS candidate",
            strategy="predicate",
        )
        store.reserve_execution_attempts(
            session["session_id"],
            candidate["candidate_id"],
            1,
            "b" * 64,
            owner_reference="budget-exhaustion-test",
        )

        view = service.get_session(session["session_id"])

        assert view["budget"]["candidate_slots_remaining"] == 0
        assert view["budget"]["executions_remaining"] == 0
        assert view["budget"]["accepts_candidate_creation"] is False
        assert view["budget"]["accepts_execution"] is False
        assert view["budget"]["accepts_new_work"] is False
        assert view["session"]["accepts_new_work"] is False
        assert view["session"]["availability_reason"] == "budget_exhausted"
    finally:
        store.close()


def test_full_session_with_only_terminal_candidates_reports_no_actionable_work() -> None:
    service, store, _plans, _executor = _service()
    try:
        case = service.start_case("appdb", "SELECT id FROM dbo.Items")
        session = service.start_session(
            case.case_id,
            "appdb",
            max_candidates=1,
            execution_limit=80,
        )
        candidate = service.add_candidate(
            session["session_id"],
            "SELECT id FROM dbo.Items AS candidate",
            strategy="predicate",
        )
        service.sessions.start_screening(session["session_id"])
        service.sessions.record_candidate_result(
            session["session_id"],
            candidate["candidate_id"],
            state="inconclusive",
            failure_code="test_terminal",
        )

        view = service.get_session(session["session_id"])

        assert view["budget"]["candidate_slots_remaining"] == 0
        assert view["budget"]["executions_remaining"] == 80
        assert view["budget"]["accepts_candidate_creation"] is False
        assert view["budget"]["accepts_execution"] is False
        assert view["budget"]["accepts_new_work"] is False
        assert view["session"]["accepts_new_work"] is False
        assert view["session"]["availability_reason"] == "no_actionable_candidate"
    finally:
        store.close()


def test_start_session_rejects_budget_above_policy_without_fallback() -> None:
    service, store, _plans, _executor = _service()
    service.database_policy = _deep_policy()
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")

    with pytest.raises(
        PermissionError,
        match=r"requested 60 candidates, 2000 executions, and 361 minutes.*360 minutes",
    ):
        service.start_session(
            case.case_id,
            "appdb",
            max_candidates=60,
            execution_limit=2000,
            time_limit_minutes=361,
        )

    stored_sessions = store._connection.execute(
        "SELECT COUNT(*) AS session_count FROM tuning_sessions"
    ).fetchone()
    assert stored_sessions is not None
    assert stored_sessions["session_count"] == 0
    store.close()


def test_query_store_identity_is_explicit_and_part_of_case_idempotency() -> None:
    service, store, _plans, _executor = _service()
    try:
        first = service.start_case(
            "appdb",
            "SELECT id FROM dbo.Items",
            query_store_query_id=42,
            idempotency_key="query-store-case",
        )
        replay = service.start_case(
            "appdb",
            "SELECT id FROM dbo.Items",
            query_store_query_id=42,
            idempotency_key="query-store-case",
        )

        assert first.query_store_query_id == 42
        assert replay == first
        assert '"query_store_query_id":42' in first.to_json()
        with pytest.raises(IdempotencyConflictError, match="different request"):
            service.start_case(
                "appdb",
                "SELECT id FROM dbo.Items",
                query_store_query_id=43,
                idempotency_key="query-store-case",
            )
    finally:
        store.close()


@pytest.mark.asyncio
async def test_query_store_collection_passes_only_stored_identity_and_replays() -> None:
    service, store, _plans, _executor = _service()
    calls: list[int] = []

    async def collect_by_query_id(query_store_query_id: int) -> dict[str, Any]:
        calls.append(query_store_query_id)
        return {
            "available": True,
            "query_id": query_store_query_id,
            "distinct_compiled_parameter_set_count": 1,
            "distinct_compiled_parameter_sets": [
                [{"name": "@P1", "compiled_value": "(private-compiled-value)"}]
            ],
            "buckets": [
                {
                    "compiled_parameters": [
                        {
                            "name": "@P1",
                            "data_type": "int",
                            "compiled_value": "(private-compiled-value)",
                            "runtime_value": "(private-runtime-value)",
                        }
                    ]
                }
            ],
        }

    case = service.start_case(
        "appdb",
        "SELECT id FROM dbo.Items",
        query_store_query_id=42,
    )
    try:
        first = await service.collect_case_evidence(
            case.case_id,
            "appdb",
            "SELECT id FROM dbo.Items",
            {"query_store_history": collect_by_query_id},
            window_minutes=15,
            query_store_query_id=42,
            idempotency_key="query-store-evidence",
        )
        replay = await service.collect_case_evidence(
            case.case_id,
            "appdb",
            "SELECT id FROM dbo.Items",
            {"query_store_history": collect_by_query_id},
            window_minutes=15,
            query_store_query_id=42,
            idempotency_key="query-store-evidence",
        )

        assert calls == [42]
        assert first["evidence"]["metadata"]["query_store_query_id"] == 42
        query_store_data = first["sections"]["query_store_history"]["data"]
        assert query_store_data["distinct_compiled_parameter_set_count"] == 1
        assert "distinct_compiled_parameter_sets" not in query_store_data
        assert query_store_data["buckets"][0]["compiled_parameters"][0] == {
            "name": "@P1",
            "data_type": "int",
        }
        assert replay["recovered_from_durable_evidence"] is True
        with pytest.raises(ValueError, match="conflicts"):
            await service.collect_case_evidence(
                case.case_id,
                "appdb",
                "SELECT id FROM dbo.Items",
                {"query_store_history": collect_by_query_id},
                window_minutes=15,
                query_store_query_id=43,
                idempotency_key="query-store-evidence-other",
            )
    finally:
        store.close()


def test_parameter_fingerprint_v1_and_value_free_receipt_are_compatible() -> None:
    parameter_case = {
        "name": "common",
        "values": {"p": 42},
        "types": {"p": "int"},
        "weight": 1.0,
    }

    assert parameter_case_fingerprint(parameter_case) == (
        "1b69372360307f1e5a4e4f08c9c09ada986f22994bd476ec0673b590dd46989b"
    )
    receipt = parameter_case_receipt(parameter_case)
    assert receipt["name"] == "common"
    assert receipt["value_parameter_names"] == ["p"]
    assert receipt["type_parameter_names"] == ["p"]
    assert receipt["parameter_types"] == {"p": "int"}
    assert receipt["weight"] == 1.0
    assert receipt["template"] == {
        "name": "common",
        "values": {"p": "<caller-retained value; not persisted>"},
        "types": {"p": "int"},
        "weight": 1.0,
    }
    assert receipt["fingerprint_v1"] == parameter_case_fingerprint(parameter_case)
    assert receipt["values_persisted"] is False
    assert receipt["matching_rules"]["parameter_name_spelling"] == "exact"
    assert receipt["matching_rules"]["sql_type_spelling"] == "exact"
    assert "42" not in json.dumps(receipt, sort_keys=True)


def test_case_returns_reusable_value_free_parameter_contract() -> None:
    service, store, _plans, _executor = _service()
    try:
        case = service.start_case(
            "appdb",
            "SELECT id FROM dbo.Items WHERE id = @P1",
            parameter_cases=[
                {
                    "name": "common",
                    "values": {"P1": 42},
                    "types": {"P1": "int"},
                    "weight": 1.0,
                }
            ],
        )

        payload = service.get_case(case.case_id)
        metadata = payload["case"]["metadata"]
        assert metadata["canonical_parameter_case_template"]["values"] == {
            "<exact parameter name>": "<caller-retained value>"
        }
        assert metadata["parameter_case_templates"][0]["types"] == {
            "P1": "int"
        }
        assert metadata["parameter_case_receipts"][0]["fingerprint_v1"] == (
            case.parameter_case_fingerprints[0]
        )
        assert metadata["parameter_case_receipts"][0]["values_persisted"] is False
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        assert '"values":{"P1":42}' not in serialized
    finally:
        store.close()


def test_parameter_case_mismatch_explains_missing_typed_input() -> None:
    service, store, _plans, _executor = _service()
    try:
        with pytest.raises(ValueError, match="missing SQL types for @p"):
            service.start_case(
                "appdb",
                "SELECT id FROM dbo.Items WHERE id = @p",
                parameter_cases=[{"name": "common", "values": {"p": 42}}],
            )
    finally:
        store.close()


def test_parameter_case_mismatch_explains_value_change_without_persisting_values() -> None:
    service, store, _plans, _executor = _service()
    parameter_case = {
        "name": "common",
        "values": {"p": 42},
        "types": {"p": "int"},
    }
    baseline = "SELECT id FROM dbo.Items WHERE id = @p"
    candidate = "SELECT id FROM dbo.Items AS candidate WHERE id = @p"
    case = service.start_case(
        "appdb",
        baseline,
        parameter_cases=[parameter_case],
    )
    session = service.start_session(case.case_id, "appdb")
    tuning_candidate = service.add_candidate(
        session["session_id"],
        candidate,
        strategy="predicate",
    )
    changed_case = {**parameter_case, "values": {"p": 43}}
    try:
        with pytest.raises(
            ValueError,
            match=r"index 0.*received .* expected .*values are not persisted",
        ):
            asyncio.run(
                service.benchmark_candidate(
                    session["session_id"],
                    tuning_candidate["candidate_id"],
                    "appdb",
                    baseline,
                    candidate,
                    parameter_cases=[changed_case],
                )
            )
        serialized = json.dumps(case.to_dict(), sort_keys=True)
        assert '"p": 42' not in serialized
        assert '"p": 43' not in serialized
    finally:
        store.close()


def test_get_session_reconciles_unattached_candidate_evidence_read_only() -> None:
    service, store, _plans, _executor = _service()
    session_id, candidate_id = _start(service)
    session = store.get_session(session_id)
    case = store.get_performance_case(session.performance_case_id)

    def create_session_evidence(
        *,
        kind: str,
        evidence_candidate_id: str,
        evidence_database_fingerprint: str | None = None,
    ) -> EvidenceEnvelopeV1:
        return store.create_evidence(
            EvidenceEnvelopeV1(
                kind=kind,
                query_fingerprint=case.query_fingerprint,
                database_fingerprint=(
                    evidence_database_fingerprint
                    or case.database_fingerprint
                ),
                metrics={"classification": "neutral"},
                metadata={
                    "session_id": session_id,
                    "candidate_id": evidence_candidate_id,
                    "phase": "screening",
                },
            )
        )

    attached = create_session_evidence(
        kind="tuning_screening",
        evidence_candidate_id=candidate_id,
    )
    service.sessions.start_screening(session_id)
    service.sessions.record_candidate_result(
        session_id,
        candidate_id,
        state="neutral",
        evidence_ids=(attached.evidence_id,),
    )
    unattached = create_session_evidence(
        kind="index_screening",
        evidence_candidate_id=candidate_id,
    )
    create_session_evidence(
        kind="index_screening",
        evidence_candidate_id="candidate-not-in-session",
    )
    create_session_evidence(
        kind="index_screening",
        evidence_candidate_id=candidate_id,
        evidence_database_fingerprint="f" * 64,
    )
    current_session = store.get_session(session_id)
    store.save_session(
        replace(
            current_session,
            metadata={},
            version=current_session.version + 1,
        ),
        expected_version=current_session.version,
    )

    result = service.get_session(session_id)

    assert {
        evidence["evidence_id"]
        for evidence in result["evidence"]
    } == {attached.evidence_id, unattached.evidence_id}
    assert result["evidence_reconciliation"] == {
        "attached_count": 1,
        "unattached_count": 1,
        "attached_evidence_ids": [attached.evidence_id],
        "unattached_evidence_ids": [unattached.evidence_id],
        "reconciliation_required": True,
    }
    assert result["leaderboard"][0]["evidence_ids"] == [attached.evidence_id]
    assert store.get_candidate(candidate_id).evidence_ids == (
        attached.evidence_id,
    )


def test_start_case_rejects_idempotency_key_reuse_for_different_query() -> None:
    service, _store, _plans, _executor = _service()
    service.start_case(
        "appdb",
        "SELECT id FROM dbo.Items",
        idempotency_key="case-request",
    )

    with pytest.raises(IdempotencyConflictError, match="different request"):
        service.start_case(
            "appdb",
            "SELECT status FROM dbo.Items",
            idempotency_key="case-request",
        )


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
        prove_equivalence=True,
    )

    assert result["classification"] == "improved"
    assert result["decision_basis"] == "observed_range_separation_v1"
    assert result["parameter_results"][0]["decision_basis"] == (
        "observed_range_separation_v1"
    )
    assert result["parameter_results"][0]["comparison_margin"][
        "conservative_gain"
    ] == 50.0
    assert result["executions"] == 8
    assert len(plans.calls) == 6
    assert len(executor.sessions) == 1
    assert result["equivalence"][0]["proven_for_parameter_case"] is True


@pytest.mark.asyncio
async def test_benchmark_idempotency_key_is_not_persisted_raw() -> None:
    service, store, _plans, _executor = _service()
    session_id, candidate_id = _start(service)

    await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
        idempotency_key="caller-secret-key",
    )

    rows = store._connection.execute(
        "SELECT idempotency_key FROM operation_idempotency"
    ).fetchall()
    assert rows
    assert all("caller-secret-key" not in row["idempotency_key"] for row in rows)


def test_candidate_idempotency_replays_before_duplicate_rejection() -> None:
    service, _store, _plans, _executor = _service()
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")
    session = service.start_session(case.case_id, "appdb")
    candidate_sql = "SELECT id FROM dbo.Items AS candidate"

    first = service.add_candidate(
        session["session_id"],
        candidate_sql,
        strategy="predicate",
        idempotency_key="candidate-replay",
    )
    replay = service.add_candidate(
        session["session_id"],
        candidate_sql,
        strategy="predicate",
        idempotency_key="candidate-replay",
    )

    assert replay == first
    with pytest.raises(IdempotencyConflictError, match="different request"):
        service.add_candidate(
            session["session_id"],
            "SELECT status FROM dbo.Items AS candidate",
            strategy="predicate",
            idempotency_key="candidate-replay",
        )


@pytest.mark.asyncio
async def test_committed_case_evidence_replays_before_collectors_or_query() -> None:
    service, store, plans, _executor = _service()
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")
    collector = AsyncMock(
        return_value={
            "status": "ok",
            "logical_database_guid": UUID(
                "12345678-1234-5678-1234-567812345678"
            ),
            "last_updated_date_utc": datetime(
                2026,
                7,
                28,
                10,
                11,
                12,
                tzinfo=timezone.utc,
            ),
            "configured_value": Decimal("123.4500"),
        }
    )

    first = await service.collect_case_evidence(
        case.case_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        {"query_store": collector},
        window_minutes=15,
        execute_query=True,
        idempotency_key="case-evidence-replay",
    )
    replay = await service.collect_case_evidence(
        case.case_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        {"query_store": collector},
        window_minutes=15,
        execute_query=True,
        idempotency_key="case-evidence-replay",
    )

    assert replay["evidence"]["evidence_id"] == first["evidence"]["evidence_id"]
    assert replay["evidence"] == first["evidence"]
    assert replay["sections"] == first["sections"]
    assert first["sections"]["query_store"]["data"] == {
        "status": "ok",
        "logical_database_guid": "12345678-1234-5678-1234-567812345678",
        "last_updated_date_utc": "2026-07-28T10:11:12+00:00",
        "configured_value": "123.4500",
    }
    assert replay["recovered_from_durable_evidence"] is True
    assert collector.await_count == 1
    assert plans.calls == ["SELECT id FROM dbo.Items"]
    assert store.get_performance_case(case.case_id).baseline_evidence_ids == (
        first["evidence"]["evidence_id"],
    )


@pytest.mark.asyncio
async def test_session_deadline_is_checked_before_each_query_dispatch() -> None:
    service, store, plans, _executor = _service()
    session_id, candidate_id = _start(service)
    original_profile = plans.profile_query
    calls = 0

    async def expire_after_first(database_name: str, sql: str) -> ProfiledPlanResult:
        nonlocal calls
        calls += 1
        result = await original_profile(database_name, sql)
        if calls == 1:
            current = store.get_session(session_id)
            store.save_session(
                replace(
                    current,
                    deadline_at_utc=(
                        datetime.now(timezone.utc) - timedelta(seconds=1)
                    ).isoformat(),
                    version=current.version + 1,
                ),
                expected_version=current.version,
            )
        return result

    plans.profile_query = expire_after_first  # type: ignore[method-assign]

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
    )

    assert result["state"] == "inconclusive"
    assert result["executions"] == 1
    assert plans.calls == ["SELECT id FROM dbo.Items"]
    assert store.get_candidate(candidate_id).state == "inconclusive"
    reservation = store.get_execution_reservation(result["execution_reservation_id"])
    assert reservation["status"] == "completed"
    assert reservation["dispatched_attempt_count"] == 1


@pytest.mark.asyncio
async def test_snapshot_deadline_is_checked_after_baseline_before_candidate() -> None:
    service, store, plans, executor = _service()
    session_id, candidate_id = _start(service)
    checks = 0

    original_ensure = service.sessions.ensure_dispatch_allowed

    def expire_on_candidate_check(checked_session_id: str) -> Any:
        nonlocal checks
        checks += 1
        if checks == 9:
            current = store.get_session(session_id)
            store.save_session(
                replace(
                    current,
                    deadline_at_utc=(
                        datetime.now(timezone.utc) - timedelta(seconds=1)
                    ).isoformat(),
                    version=current.version + 1,
                ),
                expected_version=current.version,
            )
        return original_ensure(checked_session_id)

    service.sessions.ensure_dispatch_allowed = expire_on_candidate_check  # type: ignore[method-assign]

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
        prove_equivalence=True,
    )

    assert result["classification"] == "inconclusive"
    assert result["executions"] == 7
    assert result["equivalence"][0]["executions"] == 1
    assert checks == 9
    assert executor.baseline_completed is True
    assert executor.candidate_dispatches == 0
    assert plans.calls == [
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        "SELECT id FROM dbo.Items AS candidate",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
    ]
    reservation = store.get_execution_reservation(result["execution_reservation_id"])
    assert reservation["dispatched_attempt_count"] == 7


@pytest.mark.asyncio
async def test_equivalence_reuses_one_canonical_typed_parameter_bucket() -> None:
    bound_cases: list[tuple[str, Any]] = []
    service, _store, plans, _executor = _service(
        binder=_typed_binder(bound_cases)
    )
    baseline = "SELECT id FROM dbo.Items WHERE id = @p"
    candidate = "SELECT id FROM dbo.Items AS candidate WHERE id = @p"
    session_id, candidate_id = _start(
        service,
        baseline=baseline,
        candidate=candidate,
        parameter_cases=[
            {"name": "common", "values": {"p": 42}, "types": {"p": "int"}}
        ],
    )

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate,
        parameter_cases=[
            {"name": "common", "values": {"p": 42}, "types": {"p": "int"}}
        ],
        runs_override=2,
        prove_equivalence=True,
    )

    assert result["equivalence"][0]["status"] == "match"
    assert bound_cases == [(baseline, 42)]
    first_baseline, first_candidate = plans.parameterized_contracts[:2]
    assert first_baseline.bucket_id == first_candidate.bucket_id == "common"
    assert first_baseline.parameters[0] is first_candidate.parameters[0]


@pytest.mark.asyncio
async def test_finalized_idempotent_reservation_is_not_executed_again() -> None:
    service, _store, plans, executor = _service()
    session_id, candidate_id = _start(service)
    service.sessions.start_screening(session_id)
    service.store.reserve_execution_attempts = Mock(
        return_value={
            "reservation_id": "execution-existing",
            "status": "completed",
            "version": 1,
        }
    )

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
        idempotency_key="same-request",
    )

    assert result["failure_code"] == "benchmark_request_already_finalized"
    assert result["executions"] == 0
    assert plans.calls == []
    assert executor.sessions == []


@pytest.mark.asyncio
async def test_reserved_idempotent_request_is_not_executed_again() -> None:
    service, _store, plans, executor = _service()
    session_id, candidate_id = _start(service)
    service.store.reserve_execution_attempts = Mock(
        return_value={
            "reservation_id": "execution-in-flight",
            "status": "reserved",
            "version": 0,
            "replayed": True,
        }
    )

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
        idempotency_key="same-in-flight-request",
    )

    assert result["failure_code"] == "benchmark_request_reconciliation_required"
    assert result["executions"] == 0
    assert plans.calls == []
    assert executor.sessions == []


@pytest.mark.asyncio
async def test_execution_reservation_is_not_completed_before_evidence_is_durable() -> None:
    service, store, _plans, _executor = _service()
    session_id, candidate_id = _start(service)
    store.create_evidence = Mock(side_effect=RuntimeError("synthetic persistence failure"))

    with pytest.raises(RuntimeError, match="persistence failure"):
        await service.benchmark_candidate(
            session_id,
            candidate_id,
            "appdb",
            "SELECT id FROM dbo.Items",
            "SELECT id FROM dbo.Items AS candidate",
            runs_override=3,
            idempotency_key="evidence-fails",
        )

    row = store._connection.execute(
        "SELECT status FROM execution_reservations WHERE candidate_id = ?",
        (candidate_id,),
    ).fetchone()
    assert row is not None
    assert row["status"] == "reserved"
    candidate = store.get_candidate(candidate_id)
    assert candidate.evidence_ids == ()
    assert candidate.executions == 0


@pytest.mark.asyncio
async def test_retry_recovers_evidence_commit_crash_without_rerunning_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, store, plans, executor = _service()
    session_id, candidate_id = _start(service)
    original_record = service.sessions.record_candidate_result

    def fail_after_evidence(*_args, **_kwargs):
        raise RuntimeError("synthetic post-evidence crash")

    monkeypatch.setattr(
        service.sessions,
        "record_candidate_result",
        fail_after_evidence,
    )
    with pytest.raises(RuntimeError, match="post-evidence crash"):
        await service.benchmark_candidate(
            session_id,
            candidate_id,
            "appdb",
            "SELECT id FROM dbo.Items",
            "SELECT id FROM dbo.Items AS candidate",
            runs_override=3,
            idempotency_key="recover-after-evidence",
        )

    dispatched_calls = len(plans.calls)
    assert dispatched_calls == 6
    reservation = store._connection.execute(
        "SELECT status FROM execution_reservations WHERE candidate_id = ?",
        (candidate_id,),
    ).fetchone()
    assert reservation is not None
    assert reservation["status"] == "reserved"

    monkeypatch.setattr(
        service.sessions,
        "record_candidate_result",
        original_record,
    )
    recovered = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
        idempotency_key="recover-after-evidence",
    )

    assert recovered["classification"] == "promising"
    assert recovered["decision_basis"] == "observed_range_separation_v1"
    persisted = store.get_evidence(recovered["evidence_id"])
    assert persisted.metrics["decision_basis"] == recovered["decision_basis"]
    assert persisted.metrics["parameter_results"] == recovered[
        "parameter_results"
    ]
    assert recovered["recovered_from_durable_evidence"] is True
    assert recovered["reservation_status"] == "completed"
    assert recovered["executions"] == 6
    assert len(plans.calls) == dispatched_calls
    assert executor.sessions == []
    candidate = store.get_candidate(candidate_id)
    assert candidate.executions == 6
    assert candidate.evidence_ids == (recovered["evidence_id"],)


@pytest.mark.asyncio
async def test_completed_session_rejects_exact_benchmark_replay_before_side_effects() -> None:
    service, store, plans, executor = _service()
    session_id, candidate_id = _start(service)
    baseline = "SELECT id FROM dbo.Items"
    candidate_sql = "SELECT id FROM dbo.Items AS candidate"
    await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate_sql,
        runs_override=3,
        idempotency_key="completed-exact-replay",
    )
    service.finalize_session(
        session_id,
        selected_candidate_id=None,
        stopping_reason="benchmark complete",
    )
    plan_calls = list(plans.calls)
    executor_sessions = list(executor.sessions)
    reservation_count = store._connection.execute(
        "SELECT COUNT(*) AS count FROM execution_reservations"
    ).fetchone()
    evidence_count = store._connection.execute(
        "SELECT COUNT(*) AS count FROM evidence_envelopes"
    ).fetchone()
    assert reservation_count is not None
    assert evidence_count is not None
    service.validator.validate_read_only = Mock(  # type: ignore[method-assign]
        side_effect=AssertionError("SQL validation must not run")
    )
    store.get_idempotent_execution_reservation = Mock(  # type: ignore[method-assign]
        side_effect=AssertionError("replay lookup must not run")
    )

    with pytest.raises(
        InvalidTransitionError,
        match=r"get_tuning_session.*terminal sessions cannot benchmark or replay",
    ):
        await service.benchmark_candidate(
            session_id,
            candidate_id,
            "appdb",
            baseline,
            candidate_sql,
            runs_override=3,
            idempotency_key="completed-exact-replay",
        )

    assert service.validator.validate_read_only.call_count == 0
    assert store.get_idempotent_execution_reservation.call_count == 0
    assert plans.calls == plan_calls
    assert executor.sessions == executor_sessions
    assert store._connection.execute(
        "SELECT COUNT(*) AS count FROM execution_reservations"
    ).fetchone()["count"] == reservation_count["count"]
    assert store._connection.execute(
        "SELECT COUNT(*) AS count FROM evidence_envelopes"
    ).fetchone()["count"] == evidence_count["count"]


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
        prove_equivalence=True,
    )
    assert first["classification"] == "regressed"
    assert store.get_session(session_id).status == "screening"

    second_sql = "SELECT id FROM dbo.Items AS candidate WHERE 1 = 1"
    second = service.add_candidate(session_id, second_sql, strategy="predicate")
    plans.candidate_ms = 45
    result = await service.benchmark_candidate(
        session_id,
        second["candidate_id"],
        "appdb",
        "SELECT id FROM dbo.Items",
        second_sql,
        runs_override=3,
        prove_equivalence=True,
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
        idempotency_key="failed-benchmark",
    )

    assert result["state"] == "inconclusive"
    assert result["session_continues"] is True
    assert result["reservation_status"] == "completed"
    evidence = store.get_evidence(result["evidence_id"])
    assert evidence.metrics["classification"] == "inconclusive"
    assert evidence.metadata["benchmark_failed"] is True
    assert evidence.observed_execution_count == 1
    candidate = store.get_candidate(candidate_id)
    assert candidate.state == "inconclusive"
    assert candidate.evidence_ids == (evidence.evidence_id,)

    replay = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
        idempotency_key="failed-benchmark",
    )

    assert replay["evidence_id"] == evidence.evidence_id
    assert replay["recovered_from_durable_evidence"] is True
    assert plans.profile_query.await_count == 1


@pytest.mark.asyncio
async def test_known_failure_detail_replays_without_sql_or_sensitive_data() -> None:
    service, store, plans, _executor = _service()
    session_id, candidate_id = _start(service)
    plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=Exception(
            "[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
            "Procedure expects parameter '@statement' of type "
            "'ntext/nchar/nvarchar'. (214)",
            "SELECT SecretValue FROM dbo.PrivateTable "
            "where token='do-not-return'",
        )
    )
    baseline = "SELECT id FROM dbo.Items"
    candidate = "SELECT id FROM dbo.Items AS candidate"

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate,
        runs_override=3,
        idempotency_key="known-failure-detail",
    )

    evidence = store.get_evidence(result["evidence_id"])
    diagnostic = result["failure_diagnostic"]
    assert diagnostic["diagnostic_code"] == (
        "sp_executesql_unicode_control_argument"
    )
    assert diagnostic["sqlstate"] == "42000"
    assert diagnostic["native_error_code"] == 214
    assert evidence.metadata["failure_diagnostic"] == diagnostic
    serialized = evidence.to_json()
    assert "SELECT SecretValue" not in serialized
    assert "PrivateTable" not in serialized
    assert "do-not-return" not in serialized

    replay = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate,
        runs_override=3,
        idempotency_key="known-failure-detail",
    )
    assert replay["failure_diagnostic"] == diagnostic
    assert replay["evidence_id"] == result["evidence_id"]
    assert replay["recovered_from_durable_evidence"] is True
    assert plans.profile_query.await_count == 1


@pytest.mark.asyncio
async def test_zero_dispatch_failure_is_durable_and_releases_reservation() -> None:
    binder = AsyncMock(side_effect=ValueError("synthetic binding failure"))
    service, store, plans, _executor = _service(binder=binder)
    baseline = "SELECT id FROM dbo.Items WHERE id = @p"
    candidate = "SELECT id FROM dbo.Items AS candidate WHERE id = @p"
    parameter_cases = [
        {"name": "common", "values": {"p": 42}, "types": {"p": "int"}}
    ]
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
        runs_override=2,
        idempotency_key="zero-dispatch-failure",
    )

    assert result["state"] == "inconclusive"
    assert result["executions"] == 0
    assert result["reservation_status"] == "released"
    assert plans.calls == []
    evidence = store.get_evidence(result["evidence_id"])
    assert evidence.observed_execution_count == 0
    assert store.get_candidate(candidate_id).evidence_ids == (
        evidence.evidence_id,
    )


@pytest.mark.asyncio
async def test_early_rewrite_failure_charges_only_dispatched_executions() -> None:
    service, store, plans, _executor = _service()
    session_id, candidate_id = _start(service)
    plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            _profile(100, marker="baseline"),
            TimeoutError("synthetic early failure"),
        ]
    )

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
        idempotency_key="partial-dispatch-failure",
    )

    assert result["state"] == "inconclusive"
    assert result["executions"] == 2
    assert result["reservation_status"] == "completed"
    assert store.get_candidate(candidate_id).executions == 2
    reservation = store.get_execution_reservation(result["execution_reservation_id"])
    assert reservation["attempt_count"] == 6
    assert reservation["dispatched_attempt_count"] == 2
    assert store.execution_budget_usage(session_id)["remaining"] == 78
    assert plans.profile_query.await_count == 2
    evidence = store.get_evidence(result["evidence_id"])
    assert evidence.observed_execution_count == 2
    assert evidence.metadata["benchmark_failed"] is True


@pytest.mark.asyncio
async def test_failure_receipt_reconciles_after_candidate_persistence_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, store, plans, _executor = _service()
    session_id, candidate_id = _start(service)
    plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            _profile(100, marker="baseline"),
            TimeoutError("synthetic early failure"),
        ]
    )
    original_record = service.sessions.record_candidate_result
    monkeypatch.setattr(
        service.sessions,
        "record_candidate_result",
        Mock(side_effect=RuntimeError("synthetic candidate persistence crash")),
    )

    with pytest.raises(RuntimeError, match="candidate persistence crash"):
        await service.benchmark_candidate(
            session_id,
            candidate_id,
            "appdb",
            "SELECT id FROM dbo.Items",
            "SELECT id FROM dbo.Items AS candidate",
            runs_override=3,
            idempotency_key="failure-receipt-crash",
        )

    reservation_row = store._connection.execute(
        "SELECT reservation_id FROM execution_reservations WHERE candidate_id = ?",
        (candidate_id,),
    ).fetchone()
    assert reservation_row is not None
    reservation = store.get_execution_reservation(
        reservation_row["reservation_id"]
    )
    assert reservation["status"] == "reserved"
    assert store.get_candidate(candidate_id).evidence_ids == ()
    evidence_row = store._connection.execute(
        "SELECT evidence_id FROM evidence_envelopes"
    ).fetchone()
    assert evidence_row is not None

    monkeypatch.setattr(
        service.sessions,
        "record_candidate_result",
        original_record,
    )
    replay = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
        idempotency_key="failure-receipt-crash",
    )

    assert replay["evidence_id"] == evidence_row["evidence_id"]
    assert replay["executions"] == 2
    assert replay["reservation_status"] == "completed"
    assert replay["recovered_from_durable_evidence"] is True
    assert plans.profile_query.await_count == 2


@pytest.mark.asyncio
async def test_expired_reservation_without_receipt_recovers_conservatively(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, store, plans, _executor = _service()
    session_id, candidate_id = _start(service)
    plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=TimeoutError("synthetic query failure")
    )
    original_create_evidence = store.create_evidence
    monkeypatch.setattr(
        store,
        "create_evidence",
        Mock(side_effect=RuntimeError("synthetic receipt crash")),
    )

    with pytest.raises(RuntimeError, match="receipt crash"):
        await service.benchmark_candidate(
            session_id,
            candidate_id,
            "appdb",
            "SELECT id FROM dbo.Items",
            "SELECT id FROM dbo.Items AS candidate",
            runs_override=3,
            idempotency_key="expired-without-receipt",
        )

    reservation_row = store._connection.execute(
        "SELECT reservation_id FROM execution_reservations WHERE candidate_id = ?",
        (candidate_id,),
    ).fetchone()
    assert reservation_row is not None
    store._connection.execute(
        """
        UPDATE execution_reservations
        SET expires_at_utc = '2000-01-01T00:00:00+00:00'
        WHERE reservation_id = ?
        """,
        (reservation_row["reservation_id"],),
    )
    monkeypatch.setattr(store, "create_evidence", original_create_evidence)

    replay = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
        runs_override=3,
        idempotency_key="expired-without-receipt",
    )

    assert replay["state"] == "inconclusive"
    assert replay["failure_code"] == "benchmark_request_expired"
    assert replay["executions"] == 6
    assert replay["reservation_status"] == "completed"
    evidence = store.get_evidence(replay["evidence_id"])
    assert evidence.observed_execution_count == 6
    assert evidence.metadata["execution_count_is_conservative"] is True
    candidate = store.get_candidate(candidate_id)
    assert candidate.state == "inconclusive"
    assert candidate.executions == 6
    assert plans.profile_query.await_count == 1


@pytest.mark.asyncio
async def test_cancelled_rewrite_charges_uncertain_dispatched_execution() -> None:
    service, store, plans, _executor = _service()
    session_id, candidate_id = _start(service)
    plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            _profile(100, marker="baseline"),
            asyncio.CancelledError(),
        ]
    )

    with pytest.raises(asyncio.CancelledError):
        await service.benchmark_candidate(
            session_id,
            candidate_id,
            "appdb",
            "SELECT id FROM dbo.Items",
            "SELECT id FROM dbo.Items AS candidate",
            runs_override=3,
        )

    assert store.get_candidate(candidate_id).executions == 2
    reservation_row = store._connection.execute(
        "SELECT reservation_id FROM execution_reservations WHERE candidate_id = ?",
        (candidate_id,),
    ).fetchone()
    assert reservation_row is not None
    reservation = store.get_execution_reservation(reservation_row["reservation_id"])
    assert reservation["status"] == "completed"
    assert reservation["dispatched_attempt_count"] == 2
    assert store.execution_budget_usage(session_id)["remaining"] == 78


@pytest.mark.asyncio
async def test_common_rare_null_and_boundary_parameter_buckets_are_all_measured() -> None:
    bound_cases: list[tuple[str, Any]] = []

    parameter_cases = [
        {"name": "common", "values": {"p": 1}, "types": {"p": "int"}},
        {"name": "rare", "values": {"p": 999999}, "types": {"p": "int"}},
        {"name": "NULL", "values": {"p": None}, "types": {"p": "int"}},
        {"name": "boundary", "values": {"p": 0}, "types": {"p": "int"}},
    ]
    service, _store, _plans, _executor = _service(
        binder=_typed_binder(bound_cases)
    )
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
    assert result["executions"] == 24
    assert result["equivalence_deferred"] is True
    assert len(bound_cases) == 4


@pytest.mark.asyncio
async def test_adaptive_screening_and_four_bucket_finalist_fit_default_budget() -> None:
    parameter_cases = [
        {"name": "common", "values": {"p": 1}, "types": {"p": "int"}},
        {"name": "rare", "values": {"p": 999999}, "types": {"p": "int"}},
        {"name": "NULL", "values": {"p": None}, "types": {"p": "int"}},
        {"name": "boundary", "values": {"p": 0}, "types": {"p": "int"}},
    ]
    service, store, _plans, _executor = _service(binder=_typed_binder())
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
        parameter_cases=parameter_cases[:1],
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

    assert screening["executions"] == 6
    assert finalist["executions"] == 48
    stored = store.get_candidate(candidate_id)
    assert stored.executions == 54
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
    assert unordered["snapshot_isolation_verified"] is True
    statements = executor.sessions[-1]
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL SNAPSHOT"
    assert "@@TRANCOUNT" in statements[1]
    assert "transaction_isolation_level = 5" in statements[1]
    assert "THROW 51000" in statements[1]
    assert statements[-1] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"


@pytest.mark.asyncio
async def test_volatile_comparison_requires_a_proof_contract_before_dispatch() -> None:
    service, _store, _plans, executor = _service()

    result = await service.compare_query_results(
        "appdb",
        "SELECT NEWID() AS value",
        "SELECT NEWID() AS value",
    )

    assert result["status"] == "proof_contract_required"
    assert result["executions"] == 0
    assert result["equivalence_preflight"]["baseline"]["risk_codes"] == [
        "row_volatile_function"
    ]
    assert executor.sessions == []


@pytest.mark.asyncio
async def test_outer_top_zero_comparison_returns_shape_match() -> None:
    executor = SnapshotExecutor(baseline_rows=[], candidate_rows=[])
    service, _store, _plans, _executor = _service(executor=executor)

    result = await service.compare_query_results(
        "appdb",
        "SELECT TOP (0) id FROM dbo.Items",
        "SELECT TOP 0 id FROM dbo.Items AS candidate",
    )

    assert result["status"] == "match"
    assert result["comparison_scope"] == "statically_zero_row"
    assert result["executions"] == 2
    assert result["equivalence_preflight"]["baseline"]["risk_codes"] == [
        "unordered_row_limit"
    ]
    assert result["equivalence_preflight"]["candidate"]["risk_codes"] == [
        "unordered_row_limit"
    ]
    assert len(executor.sessions) == 1


@pytest.mark.asyncio
async def test_outer_top_zero_comparison_returns_shape_type_mismatch() -> None:
    executor = SnapshotExecutor(
        baseline_rows=[],
        candidate_rows=[],
        candidate_type_signatures=("synthetic-bigint",),
    )
    service, _store, _plans, _executor = _service(executor=executor)

    result = await service.compare_query_results(
        "appdb",
        "SELECT TOP (0) id FROM dbo.Items",
        "SELECT TOP (0) id FROM dbo.Items AS candidate",
    )

    assert result["status"] == "mismatch"
    assert result["comparison_scope"] == "statically_zero_row"
    assert result["result_sets"][0]["types_match"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("baseline_sql", "candidate_sql"),
    [
        (
            "SELECT TOP (0) id FROM dbo.Items",
            "SELECT TOP (1) id FROM dbo.Items AS candidate",
        ),
        (
            "SELECT TOP (@row_count) id FROM dbo.Items",
            "SELECT TOP (@row_count) id FROM dbo.Items AS candidate",
        ),
        (
            "SELECT id FROM (SELECT TOP (0) id FROM dbo.Items) AS picked",
            (
                "SELECT id FROM "
                "(SELECT TOP (0) id FROM dbo.Items AS candidate) AS picked"
            ),
        ),
        (
            "SELECT TOP (1) id FROM dbo.Items",
            "SELECT TOP (1) id FROM dbo.Items AS candidate",
        ),
    ],
)
async def test_top_zero_shape_scope_rejects_unsupported_variants(
    baseline_sql: str,
    candidate_sql: str,
) -> None:
    service, _store, _plans, executor = _service()

    result = await service.compare_query_results(
        "appdb",
        baseline_sql,
        candidate_sql,
    )

    assert result["status"] == "proof_contract_required"
    assert result["executions"] == 0
    assert executor.sessions == []


@pytest.mark.asyncio
async def test_comparison_uses_injected_database_aware_preflight() -> None:
    calls: list[tuple[str, str]] = []

    async def analyze(database_name: str, sql: str) -> dict[str, Any]:
        calls.append((database_name, sql))
        return {
            "direct_snapshot_supported": False,
            "risk_codes": ["clock_function_in_view"],
            "resolved_view_dependencies": ["dbo.ReportView"],
        }

    service, _store, _plans, executor = _service(
        equivalence_analyzer=analyze,
    )

    result = await service.compare_query_results(
        "appdb",
        "SELECT id FROM dbo.ReportView",
        "SELECT id FROM dbo.ReportView",
    )

    assert result["status"] == "proof_contract_required"
    assert calls == [
        ("appdb", "SELECT id FROM dbo.ReportView"),
        ("appdb", "SELECT id FROM dbo.ReportView"),
    ]
    assert result["equivalence_preflight"]["baseline"][
        "resolved_view_dependencies"
    ] == ["dbo.ReportView"]
    assert executor.sessions == []


@pytest.mark.asyncio
async def test_benchmark_uses_injected_database_aware_preflight() -> None:
    calls: list[tuple[str, str]] = []

    async def analyze(database_name: str, sql: str) -> dict[str, Any]:
        calls.append((database_name, sql))
        return {
            "direct_snapshot_supported": True,
            "risk_codes": [],
            "resolved_view_dependencies": [],
        }

    service, _store, _plans, _executor = _service(
        equivalence_analyzer=analyze,
    )
    baseline = "SELECT id FROM dbo.Items"
    candidate_sql = "SELECT id FROM dbo.Items AS candidate"
    session_id, candidate_id = _start(
        service,
        baseline=baseline,
        candidate=candidate_sql,
    )

    await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate_sql,
        phase="screening",
    )

    assert calls == [
        ("appdb", baseline),
        ("appdb", candidate_sql),
    ]


@pytest.mark.asyncio
async def test_volatile_finalist_runs_performance_samples_without_snapshot_dispatch() -> None:
    baseline = "SELECT NEWID() AS value FROM dbo.Items"
    candidate = "SELECT NEWID() AS value FROM dbo.Items AS candidate"
    service, store, plans, executor = _service()
    session_id, candidate_id = _start(
        service,
        baseline=baseline,
        candidate=candidate,
    )

    screening = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate,
        phase="screening",
    )

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate,
        phase="finalist",
    )

    assert screening["classification"] == "promising"
    assert result["classification"] == "performance_only"
    assert result["executions"] == 10
    assert result["proof_scope"] == "performance_only"
    assert result["equivalence"] == []
    assert len(plans.calls) == 16
    assert executor.sessions == []
    reservation_count = store._connection.execute(
        "SELECT COUNT(*) AS count FROM execution_reservations"
    ).fetchone()
    assert reservation_count is not None
    assert reservation_count["count"] == 2


@pytest.mark.asyncio
async def test_volatile_screening_is_explicitly_performance_only() -> None:
    baseline = "SELECT NEWID() AS value"
    candidate_sql = "SELECT NEWID() AS value FROM (VALUES (1)) AS candidate(id)"
    service, store, _plans, _executor = _service()
    session_id, candidate_id = _start(
        service,
        baseline=baseline,
        candidate=candidate_sql,
    )

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate_sql,
        phase="screening",
        prove_equivalence=False,
        idempotency_key="volatile-performance-only",
    )

    assert result["classification"] == "promising"
    assert result["performance_classification"] == "promising"
    assert result["durable_state"] == "screening"
    assert result["proof_scope"] == "performance_only"
    assert result["equivalence_deferred"] is True
    evidence = store.get_evidence(result["evidence_id"])
    assert evidence.metrics["classification"] == "promising"
    assert evidence.metrics["performance_classification"] == "promising"
    assert evidence.metadata["proof_scope"] == "performance_only"
    assert evidence.metadata["equivalence_preflight"]["baseline"][
        "direct_snapshot_supported"
    ] is False
    stored_candidate = service.sessions.get_candidate(candidate_id)
    assert stored_candidate.state == "screening"
    assert stored_candidate.failure_code is None

    replay = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate_sql,
        phase="screening",
        prove_equivalence=False,
        idempotency_key="volatile-performance-only",
    )

    assert replay["classification"] == "promising"
    assert replay["performance_classification"] == "promising"
    assert replay["durable_state"] == "screening"
    assert replay["evidence_id"] == result["evidence_id"]
    assert replay["recovered_from_durable_evidence"] is True
    assert len(_plans.calls) == 6


@pytest.mark.asyncio
async def test_unordered_top_screening_remains_finalist_eligible_and_runs_full_workload() -> None:
    baseline = "SELECT TOP (1) id FROM dbo.Items"
    candidate_sql = "SELECT TOP (1) id FROM dbo.Items AS candidate"
    service, store, plans, executor = _service()
    session_id, candidate_id = _start(
        service,
        baseline=baseline,
        candidate=candidate_sql,
    )

    screening = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate_sql,
        phase="screening",
    )
    finalist = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate_sql,
        phase="finalist",
    )

    assert screening["classification"] == "promising"
    assert screening["durable_state"] == "screening"
    assert screening["equivalence_preflight"]["baseline"][
        "direct_snapshot_supported"
    ] is False
    assert finalist["classification"] == "performance_only"
    assert finalist["performance_classification"] == "improved"
    assert finalist["proof_scope"] == "performance_only"
    assert finalist["equivalence"] == []
    assert finalist["executions"] == 10
    stored = store.get_candidate(candidate_id)
    assert stored.finalist_runs == 5
    assert stored.state == "performance_only"
    assert stored.failure_code is None
    assert len(plans.calls) == 16
    assert executor.sessions == []


@pytest.mark.asyncio
async def test_performance_only_finalist_requires_explicit_selection_scope() -> None:
    baseline = "SELECT TOP (1) id FROM dbo.Items"
    candidate_sql = "SELECT TOP (1) id FROM dbo.Items AS candidate"
    service, _store, _plans, _executor = _service()
    session_id, candidate_id = _start(
        service,
        baseline=baseline,
        candidate=candidate_sql,
    )

    await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate_sql,
        phase="finalist",
    )

    with pytest.raises(ValueError, match="Selection scope proven"):
        service.finalize_session(
            session_id,
            selected_candidate_id=candidate_id,
            stopping_reason="choose the measured winner",
        )

    result = service.finalize_session(
        session_id,
        selected_candidate_id=candidate_id,
        stopping_reason="choose the measured winner",
        selection_scope="performance_only",
    )

    assert result["selection_scope"] == "performance_only"
    assert result["selected_candidate_classification"] == "performance_only"
    assert result["selected_candidate_proof_scope"] == "performance_only"
    assert result["semantic_equivalence"] == "unproven"
    assert result["deployment_ready"] is False
    assert result["automatic_deployment_approved"] is False
    assert result["session"]["selected_candidate_id"] == candidate_id
    assert result["session"]["status"] == "completed"


def test_performance_only_selection_rejects_zero_execution_finalist_receipt() -> None:
    service, store, _plans, _executor = _service()
    session_id, candidate_id = _start(service)
    case = store.get_performance_case(
        store.get_session(session_id).performance_case_id
    )
    service.sessions.start_screening(session_id)
    service.sessions.mark_candidate_finalist(session_id, candidate_id)
    evidence = store.create_evidence(
        EvidenceEnvelopeV1(
            kind="tuning_finalist",
            query_fingerprint=case.query_fingerprint,
            database_fingerprint=case.database_fingerprint,
            observed_execution_count=0,
            metrics={
                "classification": "performance_only",
                "performance_classification": "improved",
            },
            metadata={
                "session_id": session_id,
                "candidate_id": candidate_id,
                "phase": "finalist",
                "proof_scope": "performance_only",
                "equivalence_deferred": True,
                "equivalence": [],
            },
        )
    )
    service.sessions.record_candidate_result(
        session_id,
        candidate_id,
        state="performance_only",
        finalist_runs=1,
        executions=0,
        evidence_ids=(evidence.evidence_id,),
    )

    with pytest.raises(ValueError, match="nonzero execution count"):
        service.finalize_session(
            session_id,
            selected_candidate_id=candidate_id,
            stopping_reason="zero execution regression",
            selection_scope="performance_only",
        )


def test_performance_only_selection_rejects_incomplete_nonzero_finalist_receipt() -> None:
    service, store, _plans, _executor = _service()
    session_id, candidate_id = _start(service)
    case = store.get_performance_case(
        store.get_session(session_id).performance_case_id
    )
    service.sessions.start_screening(session_id)
    service.sessions.mark_candidate_finalist(session_id, candidate_id)
    reservation_owner = "incomplete-finalist-owner"
    reservation = store.reserve_execution_attempts(
        session_id,
        candidate_id,
        10,
        "c" * 64,
        owner_reference=reservation_owner,
    )
    evidence = store.create_evidence(
        EvidenceEnvelopeV1(
            kind="tuning_finalist",
            query_fingerprint=case.query_fingerprint,
            database_fingerprint=case.database_fingerprint,
            observed_execution_count=2,
            metrics={
                "classification": "performance_only",
                "performance_classification": "improved",
                "parameter_results": [
                    {
                        "parameter_case": "unparameterized",
                        "baseline": {"elapsed_ms": 100.0},
                        "candidate": {"elapsed_ms": 50.0},
                    }
                ],
            },
            metadata={
                "session_id": session_id,
                "candidate_id": candidate_id,
                "phase": "finalist",
                "proof_scope": "performance_only",
                "equivalence_deferred": True,
                "equivalence": [],
                "execution_reservation_id": reservation["reservation_id"],
            },
        )
    )
    service.sessions.record_candidate_result(
        session_id,
        candidate_id,
        state="performance_only",
        finalist_runs=5,
        parameter_cases=1,
        executions=2,
        evidence_ids=(evidence.evidence_id,),
    )
    store.complete_execution_attempts(
        reservation["reservation_id"],
        dispatched_attempt_count=2,
        owner_reference=reservation_owner,
        expected_version=reservation["version"],
    )

    with pytest.raises(ValueError, match="nonzero execution count"):
        service.finalize_session(
            session_id,
            selected_candidate_id=candidate_id,
            stopping_reason="incomplete finalist regression",
            selection_scope="performance_only",
        )


@pytest.mark.asyncio
async def test_exact_legacy_proof_contract_candidate_is_recovered_for_finalist() -> None:
    baseline = "SELECT TOP (1) id FROM dbo.Items"
    candidate_sql = "SELECT TOP (1) id FROM dbo.Items AS candidate"
    service, store, plans, _executor = _service()
    session_id, candidate_id = _start(
        service,
        baseline=baseline,
        candidate=candidate_sql,
    )
    case = store.get_performance_case(
        store.get_session(session_id).performance_case_id
    )
    _install_legacy_proof_contract_receipt(
        service,
        store,
        case,
        session_id,
        candidate_id,
        baseline,
        candidate_sql,
    )

    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        baseline,
        candidate_sql,
        phase="finalist",
    )

    assert result["classification"] == "performance_only"
    assert result["proof_scope"] == "performance_only"
    assert result["executions"] == 10
    stored = store.get_candidate(candidate_id)
    assert stored.state == "performance_only"
    assert stored.failure_code is None
    assert stored.executions == 16
    assert len(plans.calls) == 10


def _install_legacy_proof_contract_receipt(
    service: PerformanceWorkflowService,
    store: PerformanceStore,
    case: PerformanceCaseV1,
    session_id: str,
    candidate_id: str,
    baseline: str,
    candidate_sql: str,
    *,
    mutation: str | None = None,
) -> None:
    service.sessions.start_screening(session_id)
    reservation_owner = "legacy-proof-contract-owner"
    reservation = store.reserve_execution_attempts(
        session_id,
        candidate_id,
        6,
        "a" * 64,
        owner_reference=reservation_owner,
    )
    parameter_results = [
        {
            "parameter_case": "unparameterized",
            "weight": 1.0,
            "baseline": {"elapsed_ms": 100.0},
            "candidate": {"elapsed_ms": 50.0},
            "plan_delta": {},
        }
    ]
    metrics = {
        "classification": "proof_contract_required",
        "performance_classification": "promising",
        "objective": "elapsed_time",
        "parameter_results": parameter_results,
    }
    metadata = {
        "session_id": session_id,
        "candidate_id": candidate_id,
        "equivalence": [],
        "equivalence_deferred": True,
        "equivalence_preflight": {
            "baseline": analyze_equivalence_preflight(baseline).as_dict(),
            "candidate": analyze_equivalence_preflight(candidate_sql).as_dict(),
        },
        "proof_scope": "performance_only",
        "phase": "screening",
        "reason": (
            "Performance screening improved, but this MCP contract has no "
            "deterministic proof input for this SQL shape; the candidate was "
            "not promoted."
        ),
        "performance_reason": (
            "screening signal improved beyond noise; finalist equivalence is "
            "still required"
        ),
        "execution_reservation_id": reservation["reservation_id"],
        "sql_persisted": False,
    }
    candidate_executions = 6
    dispatched_executions = 6
    if mutation == "missing_objective":
        metrics.pop("objective")
    elif mutation == "missing_preflight":
        metadata.pop("equivalence_preflight")
    elif mutation == "missing_parameter_results":
        metrics["parameter_results"] = []
    elif mutation == "candidate_counter_mismatch":
        candidate_executions = 5
    elif mutation == "reservation_counter_mismatch":
        dispatched_executions = 5
    old_evidence = store.create_evidence(
        EvidenceEnvelopeV1(
            source="azure-sql-mcp",
            kind="tuning_screening",
            query_fingerprint=case.query_fingerprint,
            database_fingerprint=case.database_fingerprint,
            parameters_fingerprint="b" * 64,
            observed_execution_count=6,
            metrics=metrics,
            metadata=metadata,
        )
    )
    service.sessions.record_candidate_result(
        session_id,
        candidate_id,
        state="inconclusive",
        screen_runs=3,
        parameter_cases=1,
        executions=candidate_executions,
        evidence_ids=(old_evidence.evidence_id,),
        failure_code="proof_contract_required",
    )
    store.complete_execution_attempts(
        reservation["reservation_id"],
        dispatched_attempt_count=dispatched_executions,
        owner_reference=reservation_owner,
        expected_version=reservation["version"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "missing_objective",
        "missing_preflight",
        "missing_parameter_results",
        "candidate_counter_mismatch",
        "reservation_counter_mismatch",
    ],
)
async def test_legacy_recovery_rejects_near_match_receipts(
    mutation: str,
) -> None:
    baseline = "SELECT TOP (1) id FROM dbo.Items"
    candidate_sql = "SELECT TOP (1) id FROM dbo.Items AS candidate"
    service, store, plans, _executor = _service()
    session_id, candidate_id = _start(
        service,
        baseline=baseline,
        candidate=candidate_sql,
    )
    case = store.get_performance_case(
        store.get_session(session_id).performance_case_id
    )
    _install_legacy_proof_contract_receipt(
        service,
        store,
        case,
        session_id,
        candidate_id,
        baseline,
        candidate_sql,
        mutation=mutation,
    )

    with pytest.raises(ValueError, match="terminal benchmark"):
        await service.benchmark_candidate(
            session_id,
            candidate_id,
            "appdb",
            baseline,
            candidate_sql,
            phase="finalist",
        )

    assert plans.calls == []
    assert store.get_candidate(candidate_id).state == "inconclusive"


def test_combined_strategy_is_available_for_a_multi_family_rewrite() -> None:
    service, _store, _plans, _executor = _service()
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")
    session = service.start_session(case.case_id, "appdb")

    candidate = service.add_candidate(
        session["session_id"],
        "SELECT id FROM dbo.Items AS candidate",
        strategy="combined",
    )

    assert candidate["strategy"] == "combined"
    assert candidate["rewrite_artifact_ref"] is None
    assert "lineage" not in candidate["metadata"]


@pytest.mark.asyncio
async def test_rewrite_plus_index_requires_and_persists_a_proven_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_sql = "SELECT id FROM dbo.Items AS candidate"
    service, store, _plans, _executor = _service()
    session_id, parent_id = _start(service, candidate=parent_sql)

    await service.benchmark_candidate(
        session_id,
        parent_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        parent_sql,
        phase="screening",
        prove_equivalence=False,
    )
    finalist = await service.benchmark_candidate(
        session_id,
        parent_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        parent_sql,
        phase="finalist",
    )
    assert finalist["classification"] == "improved"

    child = service.add_candidate(
        session_id,
        parent_sql,
        strategy="rewrite_plus_index",
        artifact_ref=f"candidate:{parent_id}",
        idempotency_key="rewrite-plus-index-child",
    )

    assert child["metadata"]["lineage"]["parent_candidate_id"] == parent_id
    assert child["metadata"]["lineage"]["parent_evidence_id"] == finalist[
        "evidence_id"
    ]
    assert store.get_session(session_id).status == "finalist_validation"

    monkeypatch.setattr(
        service.sessions,
        "get_candidate",
        Mock(side_effect=AssertionError("parent state must not be revalidated")),
    )
    replay = service.add_candidate(
        session_id,
        parent_sql,
        strategy="rewrite_plus_index",
        artifact_ref=f"candidate:{parent_id}",
        idempotency_key="rewrite-plus-index-child",
    )

    assert replay == child


@pytest.mark.asyncio
async def test_parameterized_result_comparison_uses_one_typed_case() -> None:
    bound_cases: list[tuple[str, Any]] = []
    service, _store, _plans, executor = _service(
        binder=_typed_binder(bound_cases)
    )
    parameter_case = {
        "name": "common",
        "values": {"p": 42},
        "types": {"p": "int"},
        "weight": 1.0,
    }

    result = await service.compare_query_results(
        "appdb",
        "SELECT id FROM dbo.Items WHERE id = @p",
        "SELECT id FROM dbo.Items WHERE id = @p",
        parameter_case=parameter_case,
    )

    assert result["status"] == "match"
    assert result["parameter_case"] == "common"
    assert bound_cases == [
        ("SELECT id FROM dbo.Items WHERE id = @p", 42)
    ]
    params = executor.session_parameters[-1]
    assert params[2][1] == "@p int"
    assert params[3][1] == "@p int"
    input_sizes = executor.session_input_sizes[-1]
    assert input_sizes[2] == SP_EXECUTESQL_CONTROL_INPUT_SIZES
    assert input_sizes[3] == SP_EXECUTESQL_CONTROL_INPUT_SIZES


@pytest.mark.asyncio
async def test_parameterized_result_comparison_requires_a_typed_case() -> None:
    service, _store, _plans, _executor = _service(binder=_typed_binder())

    with pytest.raises(ValueError, match="requires one exact typed parameter_case"):
        await service.compare_query_results(
            "appdb",
            "SELECT id FROM dbo.Items WHERE id = @p",
            "SELECT id FROM dbo.Items WHERE id = @p",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fail_at_statement", "expected_executions"),
    [(0, 0), (1, 0), (2, 1), (3, 2)],
)
async def test_snapshot_failure_reports_exact_hook_dispatch_count(
    fail_at_statement: int,
    expected_executions: int,
) -> None:
    executor = DispatchFailureExecutor(fail_at_statement)
    service, _store, _plans, _executor = _service(executor=executor)

    result = await service.compare_query_results(
        "appdb",
        "SELECT id FROM dbo.Items",
        "SELECT id FROM dbo.Items AS candidate",
    )

    assert result["status"] == "inconclusive"
    assert result["same_snapshot"] is False
    assert result["snapshot_isolation_verified"] is False
    assert result["proven_for_parameter_case"] is False
    assert result["executions"] == expected_executions
    assert result["execution_count_is_conservative"] is False


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


def test_duplicate_output_names_without_positional_rows_are_inconclusive() -> None:
    baseline = QueryResult(
        columns=("value", "value"),
        rows=[{"value": 1}],
        column_type_signatures=("synthetic-int", "synthetic-int"),
    )
    candidate = QueryResult(
        columns=("value", "value"),
        rows=[{"value": 2}],
        column_type_signatures=("synthetic-int", "synthetic-int"),
    )

    result = compare_result_sets(
        baseline,
        candidate,
        row_limit=10,
        compare_order=True,
        same_snapshot=True,
    )

    assert result["status"] == "inconclusive"
    assert result["rows_match"] is None


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


def test_later_result_set_difference_fails_equivalence() -> None:
    baseline = [
        QueryResult(
            columns=("id",),
            rows=[{"id": 1}],
            column_type_signatures=("synthetic-int",),
        ),
        QueryResult(
            columns=("detail",),
            rows=[{"detail": "baseline"}],
            column_type_signatures=("synthetic-text",),
        ),
    ]
    candidate = [
        baseline[0],
        QueryResult(
            columns=("detail",),
            rows=[{"detail": "candidate"}],
            column_type_signatures=("synthetic-text",),
        ),
    ]

    result = compare_result_collections(
        baseline,
        candidate,
        row_limit=10,
        compare_order=True,
        same_snapshot=True,
    )

    assert result["status"] == "mismatch"
    assert result["proven_for_parameter_case"] is False
    assert result["baseline_result_set_count"] == 2
    assert result["result_sets"][1]["status"] == "mismatch"


def test_profile_fingerprint_includes_every_result_set() -> None:
    baseline = _profile(100, marker="baseline")
    candidate = _profile(100, marker="baseline")
    baseline.result_sets.append(
        QueryResult(
            columns=("detail",),
            rows=[{"detail": "baseline"}],
            column_type_signatures=("synthetic-text",),
        )
    )
    candidate.result_sets.append(
        QueryResult(
            columns=("detail",),
            rows=[{"detail": "candidate"}],
            column_type_signatures=("synthetic-text",),
        )
    )

    baseline_fingerprint = profile_result_fingerprint(
        baseline,
        compare_order=True,
    )
    candidate_fingerprint = profile_result_fingerprint(
        candidate,
        compare_order=True,
    )

    assert baseline_fingerprint["result_set_count"] == 2
    assert baseline_fingerprint["fingerprint"] != candidate_fingerprint["fingerprint"]


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


@pytest.mark.asyncio
async def test_query_store_status_object_is_not_hashed_as_actionable_status() -> None:
    service, _store, _plans, _executor = _service()
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")

    async def query_store_status() -> dict[str, Any]:
        return {
            "enabled": True,
            "status": {
                "actual_state_desc": "READ_WRITE",
                "desired_state_desc": "READ_WRITE",
                "readonly_reason": 0,
            },
        }

    result = await service.collect_case_evidence(
        case.case_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        {"query_store": query_store_status},
        window_minutes=15,
    )

    assert result["outcome"] == "healthy"


@pytest.mark.asyncio
async def test_active_parameterized_evidence_uses_the_typed_execution_contract() -> None:
    bound_cases: list[tuple[str, Any]] = []
    binder = _typed_binder(bound_cases)
    service, store, plans, _executor = _service(binder=binder)
    parameter_case = {
        "name": "common",
        "values": {"p": 98765432123456789},
        "types": {"p": "bigint"},
        "weight": 1.0,
    }
    sql = "SELECT id FROM dbo.Items WHERE id = @p"
    case = service.start_case("appdb", sql, parameter_cases=[parameter_case])
    contract = await binder("appdb", sql, parameter_case)

    async def available() -> dict[str, Any]:
        return {"status": "ok"}

    result = await service.collect_case_evidence(
        case.case_id,
        "appdb",
        sql,
        {"resource": available},
        window_minutes=60,
        execute_query=True,
        execution_contract=contract,
    )

    assert result["profile"]["user_query_executions"] == 1
    assert plans.calls == [sql]
    assert bound_cases == [(sql, 98765432123456789)]
    persisted = store.get_evidence(result["evidence"]["evidence_id"]).to_json()
    assert "98765432123456789" not in persisted
    assert sql not in persisted


@pytest.mark.asyncio
async def test_active_parameterized_evidence_fails_without_a_typed_case() -> None:
    service, _store, _plans, _executor = _service()
    sql = "SELECT id FROM dbo.Items WHERE id = @p"
    parameter_case = {
        "name": "common",
        "values": {"p": 42},
        "types": {"p": "int"},
        "weight": 1.0,
    }
    case = service.start_case("appdb", sql, parameter_cases=[parameter_case])

    with pytest.raises(ValueError, match="requires one explicit typed parameter case"):
        await service.collect_case_evidence(
            case.case_id,
            "appdb",
            sql,
            {},
            window_minutes=60,
            execute_query=True,
        )


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


def _objective_aggregate(
    objective: str,
    values: list[float],
) -> dict[str, Any]:
    metric_name = {
        "elapsed_time": "elapsed_ms",
        "cpu": "cpu_ms",
        "logical_reads": "logical_reads",
        "physical_reads": "physical_reads",
    }[objective]
    source_field = {
        "elapsed_time": ("elapsed_source", "client_wall_clock"),
        "cpu": ("cpu_source", "showplan_query_time_stats"),
        "logical_reads": ("read_source", "statistics_io_table_messages"),
        "physical_reads": ("read_source", "statistics_io_table_messages"),
    }[objective]
    return aggregate_samples(
        [
            {
                metric_name: value,
                source_field[0]: source_field[1],
            }
            for value in values
        ]
    )


def test_classification_accepts_large_separation_despite_relative_candidate_noise() -> None:
    parameter_results = [
        {
            "parameter_case": "reported_case",
            "weight": 1.0,
            "baseline": _objective_aggregate(
                "elapsed_time",
                [157_900.0, 158_035.0, 158_100.0],
            ),
            "candidate": _objective_aggregate(
                "elapsed_time",
                [1_500.0, 1_598.0, 2_300.0],
            ),
        }
    ]

    state, _reason = classify_benchmark(
        parameter_results,
        [{"status": "match"}],
        objective="elapsed_time",
    )

    assert state == "improved"
    assert parameter_results[0]["candidate"]["noise_ratio"] > 0.25
    assert parameter_results[0]["decision_basis"] == (
        "observed_range_separation_v1"
    )
    assert parameter_results[0]["comparison_margin"] == {
        "objective": "elapsed_time",
        "metric": "elapsed_ms",
        "baseline_min": 157_900.0,
        "baseline_max": 158_100.0,
        "candidate_min": 1_500.0,
        "candidate_max": 2_300.0,
        "conservative_gain": 155_600.0,
        "conservative_gain_ratio": pytest.approx(155_600.0 / 158_035.0),
        "conservative_regression": -156_600.0,
        "conservative_regression_ratio": pytest.approx(
            -156_600.0 / 158_035.0
        ),
    }


def test_classification_treats_overlapping_observed_ranges_as_neutral() -> None:
    parameter_results = [
        {
            "baseline": _objective_aggregate(
                "elapsed_time",
                [100.0, 110.0, 120.0],
            ),
            "candidate": _objective_aggregate(
                "elapsed_time",
                [90.0, 110.0, 130.0],
            ),
        }
    ]

    state, _reason = classify_benchmark(
        parameter_results,
        [{"status": "match"}],
    )

    assert state == "neutral"
    assert parameter_results[0]["comparison_margin"]["conservative_gain"] == (
        -30.0
    )
    assert parameter_results[0]["comparison_margin"][
        "conservative_regression"
    ] == -30.0


def test_classification_fails_closed_without_observed_ranges() -> None:
    parameter_results = [
        {
            "baseline": {
                "elapsed_ms": 100.0,
                "sample_count": 3,
                "metric_sources": {"elapsed_ms": ["client_wall_clock"]},
            },
            "candidate": {
                "elapsed_ms": 50.0,
                "sample_count": 3,
                "metric_sources": {"elapsed_ms": ["client_wall_clock"]},
            },
        }
    ]

    state, reason = classify_benchmark(
        parameter_results,
        [{"status": "match"}],
    )

    assert state == "inconclusive"
    assert "observed ranges" in reason
    assert "comparison_margin" not in parameter_results[0]


def test_classification_rejects_materially_separated_bucket_regression() -> None:
    parameter_results = [
        {
            "weight": 0.01,
            "baseline": _objective_aggregate(
                "elapsed_time",
                [100.0, 101.0, 102.0],
            ),
            "candidate": _objective_aggregate(
                "elapsed_time",
                [120.0, 121.0, 122.0],
            ),
        },
        {
            "weight": 0.99,
            "baseline": _objective_aggregate(
                "elapsed_time",
                [1_000.0, 1_001.0, 1_002.0],
            ),
            "candidate": _objective_aggregate(
                "elapsed_time",
                [100.0, 101.0, 102.0],
            ),
        },
    ]

    state, _reason = classify_benchmark(
        parameter_results,
        [{"status": "match"}],
    )

    assert state == "regressed"


@pytest.mark.parametrize(
    ("objective", "baseline_values", "candidate_values"),
    [
        ("elapsed_time", [100.0, 101.0], [70.0, 71.0]),
        ("cpu", [100.0, 101.0], [70.0, 71.0]),
        ("logical_reads", [1_000.0, 1_001.0], [700.0, 701.0]),
        ("physical_reads", [100.0, 101.0], [70.0, 71.0]),
    ],
)
def test_classification_weights_conservative_gain_for_every_objective(
    objective: str,
    baseline_values: list[float],
    candidate_values: list[float],
) -> None:
    parameter_results = [
        {
            "weight": 9.0,
            "baseline": _objective_aggregate(objective, baseline_values),
            "candidate": _objective_aggregate(objective, candidate_values),
        },
        {
            "weight": 1.0,
            "baseline": _objective_aggregate(objective, baseline_values),
            "candidate": _objective_aggregate(objective, baseline_values),
        },
    ]

    state, _reason = classify_benchmark(
        parameter_results,
        [{"status": "match"}],
        objective=objective,
    )

    assert state == "improved"
    assert all(
        result["decision_basis"] == "observed_range_separation_v1"
        for result in parameter_results
    )


def test_classification_uses_the_selected_objective() -> None:
    parameter_results = [
        {
            "baseline": {
                "elapsed_ms": 100.0,
                "cpu_ms": 20.0,
                "noise_ratio": 0.0,
                "sample_count": 3,
                "spread": {
                    "elapsed_ms": {"min": 100.0, "max": 100.0},
                    "cpu_ms": {"min": 20.0, "max": 20.0},
                },
                "metric_sources": {
                    "elapsed_ms": ["client_wall_clock"],
                    "cpu_ms": ["showplan_query_time_stats"],
                    "reads": [],
                },
            },
            "candidate": {
                "elapsed_ms": 50.0,
                "cpu_ms": 40.0,
                "noise_ratio": 0.0,
                "sample_count": 3,
                "spread": {
                    "elapsed_ms": {"min": 50.0, "max": 50.0},
                    "cpu_ms": {"min": 40.0, "max": 40.0},
                },
                "metric_sources": {
                    "elapsed_ms": ["client_wall_clock"],
                    "cpu_ms": ["showplan_query_time_stats"],
                    "reads": [],
                },
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


def test_classification_requires_runtime_snapshot_attestation_when_requested() -> None:
    parameter_results = [
        {
            "baseline": _objective_aggregate(
                "elapsed_time",
                [100.0, 100.0, 100.0],
            ),
            "candidate": _objective_aggregate(
                "elapsed_time",
                [50.0, 50.0, 50.0],
            ),
        }
    ]

    missing, _ = classify_benchmark(
        parameter_results,
        [{"status": "match", "same_snapshot": True}],
        require_snapshot_attestation=True,
    )
    verified, _ = classify_benchmark(
        parameter_results,
        [
            {
                "status": "match",
                "same_snapshot": True,
                "snapshot_isolation_verified": True,
            }
        ],
        require_snapshot_attestation=True,
    )

    assert missing == "inconclusive"
    assert verified == "improved"


def test_classification_rejects_missing_or_mixed_metric_provenance() -> None:
    parameter_results = [
        {
            "baseline": {
                "elapsed_ms": 100.0,
                "noise_ratio": 0.0,
                "sample_count": 3,
                "metric_sources": {"elapsed_ms": ["client_wall_clock"]},
            },
            "candidate": {
                "elapsed_ms": 50.0,
                "noise_ratio": 0.0,
                "sample_count": 3,
                "metric_sources": {
                    "elapsed_ms": ["client_wall_clock", "synthetic_other_source"]
                },
            },
        }
    ]

    state, reason = classify_benchmark(
        parameter_results,
        [{"status": "match"}],
        objective="elapsed_time",
    )

    assert state == "inconclusive"
    assert "provenance" in reason
