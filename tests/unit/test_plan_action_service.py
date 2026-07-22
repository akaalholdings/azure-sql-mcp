from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.performance_contracts import PerformanceCaseV1
from azure_sql_mcp.performance_store import PerformanceStore
from azure_sql_mcp.plan_action_service import PlanActionService
from azure_sql_mcp.tuning_sessions import TuningSessionStateMachine


def _policy() -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_benchmark": False,
                    "allow_test_indexes": False,
                    "allow_plan_apply": True,
                    "max_benchmark_executions": 0,
                }
            },
        }
    )


def _state(
    force_plan_id: int | None,
    hints: str | None,
    *,
    ownership: str = "manual",
) -> dict[str, Any]:
    return {
        "force_plan_id": force_plan_id,
        "query_store_hints": hints,
        "ownership": ownership,
        "captured": True,
    }


class StateExecutor:
    def __init__(self, states: list[dict[str, Any]]) -> None:
        self.states = list(states)
        self.statements: list[list[str]] = []

    async def execute_session(
        self,
        _database_name: str,
        statements: list[str],
        *,
        max_rows: int | None = None,
    ) -> list[list[QueryResult]]:
        assert max_rows == 10
        self.statements.append(statements)
        state = self.states.pop(0)
        forcing_type = "AUTO" if state["ownership"] == "automatic" else "MANUAL"
        forced = (
            [
                QueryResult(
                    columns=("plan_id", "plan_forcing_type_desc"),
                    rows=[
                        {
                            "plan_id": state["force_plan_id"],
                            "plan_forcing_type_desc": forcing_type,
                        }
                    ],
                )
            ]
            if state["force_plan_id"] is not None
            else []
        )
        hints = (
            [
                QueryResult(
                    columns=("query_hint_text",),
                    rows=[{"query_hint_text": state["query_store_hints"]}],
                )
            ]
            if state["query_store_hints"] is not None
            else []
        )
        automatic = (
            [QueryResult(columns=("automatic_owner",), rows=[{"automatic_owner": 1}])]
            if state["ownership"] == "automatic"
            else []
        )
        return [[], [], forced, hints, automatic, []]


class RecordingAdminPolicy:
    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self.actions: list[Any] = []
        self.fail_on_call = fail_on_call

    async def execute(self, action, _executor, *, dry_run: bool, max_rows=None):
        assert dry_run is False
        self.actions.append(action)
        if self.fail_on_call == len(self.actions):
            raise TimeoutError("synthetic uncertain database response")
        return {"status": "completed", "tool_name": action.tool_name}


class BlockingAdminPolicy(RecordingAdminPolicy):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(self, action, _executor, *, dry_run: bool, max_rows=None):
        assert dry_run is False
        self.actions.append(action)
        self.started.set()
        await self.release.wait()
        return {"status": "completed", "tool_name": action.tool_name}


def _baseline(*, count: int = 40) -> dict[str, Any]:
    return {
        "count_executions": count,
        "avg_duration": 100.0,
        "avg_cpu_time": 40.0,
        "avg_logical_io_reads": 1000.0,
        "evidence": {
            "source": "query-store",
            "provenance": "capture-v1",
            "environment": "test",
            "database_name": "appdb",
            "query_id": 42,
            "window_start": "2026-07-15T08:00:00Z",
            "window_end": "2026-07-15T09:00:00Z",
            "post_change": False,
            "parameter_buckets": ["common", "rare"],
            "truncated": False,
        },
    }


def _candidate(*, duration: float, count: int = 40) -> dict[str, Any]:
    return {
        "count_executions": count,
        "avg_duration": duration,
        "avg_cpu_time": 30.0,
        "avg_logical_io_reads": 800.0,
        "evidence": {
            "source": "query-store",
            "provenance": "capture-v1",
            "environment": "test",
            "database_name": "appdb",
            "query_id": 42,
            "window_start": "2026-07-15T09:00:00Z",
            "window_end": "2026-07-15T10:00:00Z",
            "post_change": True,
            "parameter_buckets": ["common", "rare"],
            "truncated": False,
        },
    }


def _service(
    server_config_factory,
    states: list[dict[str, Any]],
) -> tuple[PlanActionService, PerformanceStore, RecordingAdminPolicy, str]:
    config = server_config_factory(
        access_mode=AccessMode.UNRESTRICTED,
        write_policy=WritePolicy.APPLY,
        profile=McpProfile.ENFORCER_APPLY,
        plan_apply_kill_switch=False,
        performance_state_dir=":memory:",
    )
    store = PerformanceStore(db_path=":memory:")
    case = store.create_performance_case(
        PerformanceCaseV1(
            case_id="case-plan-action",
            query_fingerprint="query-fingerprint",
        )
    )
    session = TuningSessionStateMachine(store).create_session(case)
    admin = RecordingAdminPolicy()
    service = PlanActionService(
        config=config,
        executor=StateExecutor(states),  # type: ignore[arg-type]
        admin_policy=admin,  # type: ignore[arg-type]
        database_policy=_policy(),
        store=store,
    )
    return service, store, admin, session.session_id


@pytest.mark.asyncio
async def test_prepared_apply_verifies_and_restores_exact_prior_state(
    server_config_factory,
) -> None:
    prior = _state(3, "OPTION(MAXDOP 2)")
    applied = _state(7, "OPTION(MAXDOP 2)")
    service, store, admin, session_id = _service(
        server_config_factory,
        [prior, prior, applied, applied, applied, prior],
    )

    prepared = await service.prepare(
        "appdb",
        session_id=session_id,
        candidate_id=None,
        operation="force_plan",
        query_id=42,
        plan_id=7,
        query_hints=None,
        evidence=_baseline(),
        reviewed_by="operator",
        reason="reviewed regression",
        idempotency_key="change-42-7",
    )
    intent_id = prepared["intent"]["intent_id"]
    assert prepared["prepared"] is True

    applied_result = await service.apply(
        "appdb",
        intent_id,
        authorization_reference="approved-change",
    )
    assert applied_result["confirmed"] is True

    verified = await service.verify(
        "appdb",
        intent_id,
        candidate_evidence=_candidate(duration=150.0),
        authorization_reference="approved-rollback",
    )

    assert verified["decision"] == "rollback"
    assert verified["confirmed"] is True
    assert store.get_plan_action_intent(intent_id).status == "rolled_back"
    assert len(admin.actions) == 2
    restore = admin.actions[1]
    assert "sp_query_store_unforce_plan" in restore.sql
    assert "sp_query_store_clear_hints" in restore.sql
    assert "sp_query_store_set_hints" in restore.sql
    assert "sp_query_store_force_plan" in restore.sql
    assert restore.params == (42, 7, 42, "OPTION(MAXDOP 2)", 42, 42, 3)


@pytest.mark.asyncio
async def test_insufficient_verification_evidence_holds_without_rollback(
    server_config_factory,
) -> None:
    prior = _state(None, None)
    applied = _state(7, None)
    service, store, admin, session_id = _service(
        server_config_factory,
        [prior, prior, applied, applied],
    )
    prepared = await service.prepare(
        "appdb",
        session_id=session_id,
        candidate_id=None,
        operation="force_plan",
        query_id=42,
        plan_id=7,
        query_hints=None,
        evidence=_baseline(),
        reviewed_by="operator",
        reason="reviewed regression",
        idempotency_key="change-hold",
    )
    intent_id = prepared["intent"]["intent_id"]
    await service.apply(
        "appdb",
        intent_id,
        authorization_reference="approved-change",
    )

    result = await service.verify(
        "appdb",
        intent_id,
        candidate_evidence=_candidate(duration=70.0, count=2),
        authorization_reference="unused-unless-rollback",
    )

    assert result["decision"] == "hold"
    assert store.get_plan_action_intent(intent_id).status == "hold"
    assert len(admin.actions) == 1


@pytest.mark.asyncio
async def test_automatic_tuning_ownership_is_detected_and_never_applied(
    server_config_factory,
) -> None:
    service, _store, admin, session_id = _service(
        server_config_factory,
        [_state(3, None, ownership="automatic")],
    )

    prepared = await service.prepare(
        "appdb",
        session_id=session_id,
        candidate_id=None,
        operation="force_plan",
        query_id=42,
        plan_id=7,
        query_hints=None,
        evidence=_baseline(),
        reviewed_by="operator",
        reason="reviewed regression",
        idempotency_key="automatic-owner",
    )

    assert prepared["prepared"] is False
    assert prepared["intent"]["status"] == "rejected"
    with pytest.raises(PermissionError, match="Only a prepared intent"):
        await service.apply(
            "appdb",
            prepared["intent"]["intent_id"],
            authorization_reference="must-not-apply",
        )
    assert admin.actions == []
    ownership_query = service.executor.statements[0][4]  # type: ignore[attr-defined]
    assert "$.planForceDetails.queryId" in ownership_query
    assert "execute_action_initiated_by = 'System'" in ownership_query


@pytest.mark.asyncio
async def test_kill_switch_blocks_prepared_apply(server_config_factory) -> None:
    service, _store, admin, session_id = _service(
        server_config_factory,
        [_state(None, None)],
    )
    prepared = await service.prepare(
        "appdb",
        session_id=session_id,
        candidate_id=None,
        operation="force_plan",
        query_id=42,
        plan_id=7,
        query_hints=None,
        evidence=_baseline(),
        reviewed_by="operator",
        reason="reviewed regression",
        idempotency_key="kill-switch",
    )
    service.config = replace(service.config, plan_apply_kill_switch=True)

    with pytest.raises(PermissionError, match="kill switch"):
        await service.apply(
            "appdb",
            prepared["intent"]["intent_id"],
            authorization_reference="blocked",
        )
    assert admin.actions == []


@pytest.mark.asyncio
async def test_prepared_intent_redacts_sql_and_rejects_evidence_hash_drift(
    server_config_factory,
) -> None:
    service, store, admin, session_id = _service(
        server_config_factory,
        [_state(None, None)],
    )
    evidence = _baseline()
    evidence["sql"] = "SELECT synthetic_value FROM dbo.SyntheticTable"
    prepared = await service.prepare(
        "appdb",
        session_id=session_id,
        candidate_id=None,
        operation="force_plan",
        query_id=42,
        plan_id=7,
        query_hints=None,
        evidence=evidence,
        reviewed_by="operator",
        reason="reviewed regression",
        idempotency_key="evidence-hash",
    )
    intent = store.get_plan_action_intent(prepared["intent"]["intent_id"])
    assert "sql" not in intent.metadata["baseline_evidence"]

    metadata = dict(intent.metadata)
    baseline = dict(metadata["baseline_evidence"])
    baseline["avg_duration"] = 999.0
    metadata["baseline_evidence"] = baseline
    store.save_plan_action_intent(
        replace(intent, metadata=metadata, version=intent.version + 1)
    )

    with pytest.raises(RuntimeError, match="evidence hash"):
        await service.apply(
            "appdb",
            intent.intent_id,
            authorization_reference="approved-change",
        )
    assert admin.actions == []


@pytest.mark.asyncio
async def test_verification_holds_when_automatic_tuning_takes_ownership(
    server_config_factory,
) -> None:
    prior = _state(None, None)
    applied = _state(7, None)
    automatic = _state(7, None, ownership="automatic")
    service, store, admin, session_id = _service(
        server_config_factory,
        [prior, prior, applied, automatic],
    )
    prepared = await service.prepare(
        "appdb",
        session_id=session_id,
        candidate_id=None,
        operation="force_plan",
        query_id=42,
        plan_id=7,
        query_hints=None,
        evidence=_baseline(),
        reviewed_by="operator",
        reason="reviewed regression",
        idempotency_key="ownership-change",
    )
    await service.apply(
        "appdb",
        prepared["intent"]["intent_id"],
        authorization_reference="approved-change",
    )

    result = await service.verify(
        "appdb",
        prepared["intent"]["intent_id"],
        candidate_evidence=_candidate(duration=150.0),
        authorization_reference="must-not-rollback-engine-state",
    )

    assert result["decision"] == "hold"
    assert store.get_plan_action_intent(prepared["intent"]["intent_id"]).status == "hold"
    assert len(admin.actions) == 1


@pytest.mark.asyncio
async def test_uncertain_apply_is_durable_unknown_and_cannot_retry(
    server_config_factory,
) -> None:
    service, store, admin, session_id = _service(
        server_config_factory,
        [_state(None, None), _state(None, None)],
    )
    admin.fail_on_call = 1
    prepared = await service.prepare(
        "appdb",
        session_id=session_id,
        candidate_id=None,
        operation="force_plan",
        query_id=42,
        plan_id=7,
        query_hints=None,
        evidence=_baseline(),
        reviewed_by="operator",
        reason="reviewed regression",
        idempotency_key="uncertain-apply",
    )
    intent_id = prepared["intent"]["intent_id"]

    result = await service.apply(
        "appdb",
        intent_id,
        authorization_reference="approved-change",
    )

    assert result["confirmed"] is False
    assert store.get_plan_action_intent(intent_id).status == "unknown"
    replay = await service.apply(
        "appdb",
        intent_id,
        authorization_reference="must-not-retry",
    )
    assert replay["reconciliation_required"] is True
    assert replay["confirmed"] is False
    assert len(admin.actions) == 1


@pytest.mark.asyncio
async def test_uncertain_rollback_is_durable_unknown(
    server_config_factory,
) -> None:
    prior = _state(3, "OPTION(MAXDOP 2)")
    applied = _state(7, "OPTION(MAXDOP 2)")
    service, store, admin, session_id = _service(
        server_config_factory,
        [prior, prior, applied, applied],
    )
    admin.fail_on_call = 2
    prepared = await service.prepare(
        "appdb",
        session_id=session_id,
        candidate_id=None,
        operation="force_plan",
        query_id=42,
        plan_id=7,
        query_hints=None,
        evidence=_baseline(),
        reviewed_by="operator",
        reason="reviewed regression",
        idempotency_key="uncertain-rollback",
    )
    intent_id = prepared["intent"]["intent_id"]
    applied_result = await service.apply(
        "appdb",
        intent_id,
        authorization_reference="approved-change",
    )
    assert applied_result["confirmed"] is True

    result = await service.rollback(
        "appdb",
        intent_id,
        authorization_reference="approved-rollback",
    )

    assert result["confirmed"] is False
    assert store.get_plan_action_intent(intent_id).status == "unknown"
    assert len(admin.actions) == 2


@pytest.mark.asyncio
async def test_concurrent_apply_uses_one_durable_mutation_claim(
    server_config_factory,
) -> None:
    prior = _state(None, None)
    applied = _state(7, None)
    service, store, _admin, session_id = _service(
        server_config_factory,
        [prior, prior, applied],
    )
    blocker = BlockingAdminPolicy()
    service.admin_policy = blocker  # type: ignore[assignment]
    prepared = await service.prepare(
        "appdb",
        session_id=session_id,
        candidate_id=None,
        operation="force_plan",
        query_id=42,
        plan_id=7,
        query_hints=None,
        evidence=_baseline(),
        reviewed_by="operator",
        reason="reviewed regression",
        idempotency_key="concurrent-apply",
    )
    intent_id = prepared["intent"]["intent_id"]

    first_task = asyncio.create_task(
        service.apply(
            "appdb",
            intent_id,
            authorization_reference="approved-change",
        )
    )
    await blocker.started.wait()
    replay = await service.apply(
        "appdb",
        intent_id,
        authorization_reference="duplicate-call",
    )
    blocker.release.set()
    first = await first_task

    assert replay["reconciliation_required"] is True
    assert replay["intent"]["status"] == "applying"
    assert first["confirmed"] is True
    assert store.get_plan_action_intent(intent_id).status == "applied"
    assert len(blocker.actions) == 1
