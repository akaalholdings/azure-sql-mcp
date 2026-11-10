from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.performance_contracts import PerformanceCaseV1
from azure_sql_mcp.performance_store import IdempotencyConflictError
from azure_sql_mcp.performance_store import PerformanceStore
from azure_sql_mcp.plan_action_service import PlanActionService
from azure_sql_mcp.query_identity import legacy_database_fingerprint
from azure_sql_mcp.query_identity import server_database_identity
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
    force_failures: int | None = None,
    failure_reason: str | None = None,
) -> dict[str, Any]:
    state: dict[str, Any] = {
        "force_plan_id": force_plan_id,
        "query_store_hints": hints,
        "ownership": ownership,
        "captured": True,
    }
    if force_failures is not None:
        state["force_failure_count"] = force_failures
        state["last_force_failure_reason_desc"] = failure_reason
    return state


def _qs_row(
    window: str,
    plan_id: int,
    count: int,
    duration: float,
    *,
    cpu: float = 30.0,
    reads: float = 800.0,
    max_duration: float | None = None,
    stdev: float | None = None,
) -> dict[str, Any]:
    """One per-plan Query Store aggregate row as the MCP collector reads it."""

    row = {
        "window_name": window,
        "plan_id": plan_id,
        "count_executions": count,
        "total_duration": duration * count,
        "total_cpu_time": cpu * count,
        "total_logical_io_reads": reads * count,
        "max_duration": max_duration if max_duration is not None else duration * 1.5,
    }
    if stdev is not None:
        # Sum of squares from one interval: n * (stdev^2 + avg^2).
        row["total_duration_sq"] = count * (stdev**2 + duration**2)
    return row


def _baseline_rows(plan_id: int = 3) -> list[dict[str, Any]]:
    return [_qs_row("baseline", plan_id, 40, 100.0, cpu=40.0, reads=1000.0)]


def _improved_rows(plan_id: int = 7) -> list[dict[str, Any]]:
    return [*_baseline_rows(), _qs_row("post", plan_id, 40, 70.0)]


class StateExecutor:
    def __init__(
        self,
        states: list[dict[str, Any]],
        runtime_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.states = list(states)
        self.runtime_rows = list(runtime_rows or [])
        self.statements: list[list[str]] = []
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []
        # Force precheck reads. The default is a healthy target: the plan
        # belongs to query 42, ran an hour ago, and the query ran one plan.
        self.plan_row: dict[str, Any] | None = {
            "query_id": 42,
            "plan_type_desc": "Compiled Plan",
            "force_failure_count": 0,
            "last_force_failure_reason_desc": "NONE",
            "minutes_since_last_execution": 60,
            "has_option_recompile": 0,
            "plan_uses_temp_table": 0,
            "query_store_state": "READ_WRITE",
        }
        self.variant_rows: list[dict[str, Any]] = []
        self.bucket_rows: list[dict[str, Any]] | None = None
        self.precheck_error: Exception | None = None

    async def fetch_all(
        self,
        _database_name: str,
        query: str,
        params: Any = None,
        *,
        max_rows: int | None = None,
    ) -> list[dict[str, Any]]:
        assert max_rows is not None
        if "minutes_since_last_execution" in query:
            if self.precheck_error is not None:
                raise self.precheck_error
            plan_id = tuple(params or ())[0]
            return [] if self.plan_row is None else [{"plan_id": plan_id, **self.plan_row}]
        if "query_store_query_variant" in query:
            return [dict(row) for row in self.variant_rows]
        if "avg_rowcount" in query:
            if self.bucket_rows is None:
                return [{"plan_id": 7, "executions": 100, "avg_rowcount": 10.0}]
            return [dict(row) for row in self.bucket_rows]
        self.fetches.append((query, tuple(params or ())))
        return [dict(row) for row in self.runtime_rows]

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
                            "force_failure_count": state.get("force_failure_count", 0),
                            "last_force_failure_reason_desc": state.get(
                                "last_force_failure_reason_desc"
                            ),
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
        "per_bucket": {
            "common": {"avg_duration": 100.0},
            "rare": {"avg_duration": 100.0},
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
        "per_bucket": {
            "common": {"avg_duration": duration},
            "rare": {"avg_duration": duration},
        },
    }


def _service(
    server_config_factory,
    states: list[dict[str, Any]],
    runtime_rows: list[dict[str, Any]] | None = None,
    intent_ttl: timedelta | None = None,
    **config_overrides: Any,
) -> tuple[PlanActionService, PerformanceStore, RecordingAdminPolicy, str]:
    config = server_config_factory(
        access_mode=AccessMode.UNRESTRICTED,
        write_policy=WritePolicy.APPLY,
        profile=McpProfile.ENFORCER_APPLY,
        plan_apply_kill_switch=False,
        performance_state_dir=":memory:",
        **config_overrides,
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
    ttl_kwargs = {} if intent_ttl is None else {"intent_ttl": intent_ttl}
    service = PlanActionService(
        config=config,
        executor=StateExecutor(states, runtime_rows),  # type: ignore[arg-type]
        admin_policy=admin,  # type: ignore[arg-type]
        database_policy=_policy(),
        store=store,
        **ttl_kwargs,
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
        runtime_rows=[*_baseline_rows(), _qs_row("post", 7, 40, 150.0)],
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
    assert prepared["intent"]["metadata"]["database_fingerprint"] == (
        server_database_identity(service.config.server, "appdb")
    )

    applied_result = await service.apply(
        "appdb",
        intent_id,
        authorization_reference="approved-change",
    )
    assert applied_result["confirmed"] is True
    _age_applied_intent(store, intent_id)

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
    for action in admin.actions:
        assert action.exactly_once is True
        assert action.reviewed_intent is True
        assert action.idempotency_key
        assert action.rollback_sql
        assert action.policy_verified is True
        assert action.verification_required is True


@pytest.mark.asyncio
async def test_plan_action_identity_rejects_same_database_on_different_server(
    server_config_factory,
) -> None:
    service, store, admin, session_id = _service(
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
        idempotency_key="server-boundary",
    )
    intent_id = prepared["intent"]["intent_id"]
    assert store.get_plan_action_intent(intent_id).metadata["database_fingerprint"] == (
        server_database_identity(service.config.server, "appdb")
    )

    service.config = replace(service.config, server="other.database.windows.net")
    with pytest.raises(PermissionError, match="database fingerprint"):
        await service.apply(
            "appdb",
            intent_id,
            authorization_reference="approved-change",
        )
    assert admin.actions == []


@pytest.mark.asyncio
async def test_plan_action_identity_accepts_mixed_case_server_and_database(
    server_config_factory,
) -> None:
    service, _store, admin, session_id = _service(
        server_config_factory,
        [_state(None, None), _state(None, None), _state(7, None)],
    )
    service.config = replace(service.config, server="Server.Database.Windows.Net")

    prepared = await service.prepare(
        "AppDb",
        session_id=session_id,
        candidate_id=None,
        operation="force_plan",
        query_id=42,
        plan_id=7,
        query_hints=None,
        evidence=_baseline(),
        reviewed_by="operator",
        reason="reviewed regression",
        idempotency_key="mixed-case-target",
    )
    intent_id = prepared["intent"]["intent_id"]

    assert prepared["intent"]["metadata"]["database_fingerprint"] == (
        server_database_identity("server.database.windows.net", "appdb")
    )
    applied = await service.apply(
        "aPpDb",
        intent_id,
        authorization_reference="approved-change",
    )

    assert applied["confirmed"] is True
    assert len(admin.actions) == 1


@pytest.mark.asyncio
async def test_legacy_plan_intent_database_fingerprint_cannot_be_applied(
    server_config_factory,
) -> None:
    service, store, admin, session_id = _service(
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
        idempotency_key="legacy-plan-target",
    )
    intent = store.get_plan_action_intent(prepared["intent"]["intent_id"])
    legacy_intent = replace(
        intent,
        metadata={
            **intent.metadata,
            "database_fingerprint": legacy_database_fingerprint("appdb"),
        },
        version=intent.version + 1,
    )
    store.save_plan_action_intent(
        legacy_intent,
        expected_version=intent.version,
    )

    with pytest.raises(PermissionError, match="database fingerprint"):
        service._validated_intent("appdb", intent.intent_id)

    assert admin.actions == []


@pytest.mark.asyncio
async def test_explicit_server_binding_migrates_legacy_plan_intent_identity(
    server_config_factory,
) -> None:
    service, store, admin, session_id = _service(
        server_config_factory,
        [_state(None, None)],
        legacy_state_server_binding="server.database.windows.net",
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
        idempotency_key="legacy-plan-migration",
    )
    intent = store.get_plan_action_intent(prepared["intent"]["intent_id"])
    legacy_intent = replace(
        intent,
        metadata={
            **intent.metadata,
            "database_fingerprint": legacy_database_fingerprint("appdb"),
        },
        version=intent.version + 1,
    )
    store.save_plan_action_intent(
        legacy_intent,
        expected_version=intent.version,
    )

    migrated = service._validated_intent("appdb", intent.intent_id)

    assert migrated.metadata["database_fingerprint"] == server_database_identity(
        service.config.server,
        "appdb",
    )
    assert (
        migrated.metadata["identity_migration"]
        == "legacy-database-to-server-bound-v1"
    )
    assert admin.actions == []


@pytest.mark.asyncio
async def test_prepare_rejects_idempotency_key_reuse_for_different_action(
    server_config_factory,
) -> None:
    prior = _state(None, None)
    service, _store, _admin, session_id = _service(
        server_config_factory,
        [prior, prior],
    )
    await service.prepare(
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
        idempotency_key="same-plan-action",
    )

    with pytest.raises(IdempotencyConflictError, match="different request"):
        await service.prepare(
            "appdb",
            session_id=session_id,
            candidate_id=None,
            operation="force_plan",
            query_id=42,
            plan_id=8,
            query_hints=None,
            evidence=_baseline(),
            reviewed_by="operator",
            reason="reviewed regression",
            idempotency_key="same-plan-action",
        )


@pytest.mark.asyncio
async def test_insufficient_verification_evidence_holds_without_rollback(
    server_config_factory,
) -> None:
    prior = _state(None, None)
    applied = _state(7, None)
    service, store, admin, session_id = _service(
        server_config_factory,
        [prior, prior, applied, applied],
        runtime_rows=[*_baseline_rows(), _qs_row("post", 7, 2, 70.0)],
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
async def test_manual_apply_recommendation_is_not_automatic_ownership(
    server_config_factory,
) -> None:
    """Microsoft documents state reason AutomaticTuningOptionNotEnabled as
    'apply the recommendation manually': automatic tuning is off and will not
    act on it. Every other Active FORCE_LAST_GOOD_PLAN row stays engine-owned,
    and a NULL reason must not open the gate (NULL <> 'x' is not true)."""
    service, _store, _admin, session_id = _service(
        server_config_factory,
        [_state(None, None)],
    )

    await service.prepare(
        "appdb",
        session_id=session_id,
        candidate_id=None,
        operation="force_plan",
        query_id=42,
        plan_id=7,
        query_hints=None,
        evidence=_baseline(),
        reviewed_by="operator",
        reason="manual-apply recommendation",
        idempotency_key="manual-apply-owner",
    )

    ownership_query = " ".join(
        service.executor.statements[0][4].split()  # type: ignore[attr-defined]
    )
    assert (
        "(type = 'FORCE_LAST_GOOD_PLAN' "
        "AND JSON_VALUE(state, '$.currentValue') = 'Active' "
        "AND ISNULL(JSON_VALUE(state, '$.reason'), '') "
        "<> 'AutomaticTuningOptionNotEnabled')"
    ) in ownership_query
    # Rows the engine executed or is verifying stay engine-owned.
    assert "IN ('Active', 'Verifying', 'Success')" in ownership_query
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


def _age_applied_intent(store: PerformanceStore, intent_id: str) -> None:
    """Verify runs after the post-apply window has elapsed, as it does in use.

    Without this a verify microseconds after apply gets a zero-length window on a
    coarse clock (Windows before Python 3.13 ticks about every 15 ms), and holds
    with "windows must have positive duration", whatever the evidence says.
    """

    latest = store.get_plan_action_intent(intent_id)
    applied = datetime.fromisoformat(str(latest.metadata["applied_at_utc"]))
    store.save_plan_action_intent(
        replace(
            latest,
            metadata={**latest.metadata, "applied_at_utc": (applied - timedelta(hours=1)).isoformat()},
            version=latest.version + 1,
        ),
        expected_version=latest.version,
    )


async def _applied_force_plan(
    server_config_factory,
    *,
    verify_state: dict[str, Any] | None = None,
    runtime_rows: list[dict[str, Any]] | None = None,
    applied_state: dict[str, Any] | None = None,
    extra_states: list[dict[str, Any]] | None = None,
) -> tuple[PlanActionService, PerformanceStore, RecordingAdminPolicy, str]:
    prior = _state(None, None)
    applied = applied_state or _state(7, None, force_failures=0)
    service, store, admin, session_id = _service(
        server_config_factory,
        [prior, prior, applied, verify_state or applied, *(extra_states or [])],
        runtime_rows=runtime_rows,
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
        idempotency_key="verify-evidence",
    )
    intent_id = prepared["intent"]["intent_id"]
    applied_result = await service.apply(
        "appdb",
        intent_id,
        authorization_reference="approved-change",
    )
    assert applied_result["confirmed"] is True
    _age_applied_intent(store, intent_id)
    return service, store, admin, intent_id


@pytest.mark.asyncio
async def test_caller_numbers_alone_never_keep_a_forced_plan(
    server_config_factory,
) -> None:
    # The agent can pass numbers from another plan, another window, or invent
    # them. With no Query Store executions since apply there is nothing to keep.
    service, store, admin, intent_id = await _applied_force_plan(
        server_config_factory,
        runtime_rows=_baseline_rows(),
    )

    result = await service.verify(
        "appdb",
        intent_id,
        candidate_evidence=_candidate(duration=70.0),
        authorization_reference="unused-unless-rollback",
    )

    assert result["decision"] == "hold"
    assert store.get_plan_action_intent(intent_id).status == "hold"
    assert len(admin.actions) == 1
    executor = service.executor
    assert isinstance(executor, StateExecutor)
    query, params = executor.fetches[-1]
    assert "sys.query_store_runtime_stats" in query
    applied_at = store.get_plan_action_intent(intent_id).metadata["applied_at_utc"]
    assert params[0] == (
        datetime.fromisoformat(applied_at)
        .astimezone(timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%S.%f")
    )
    assert result["evidence"]["caller_evidence_used_for_keep"] is False


@pytest.mark.asyncio
async def test_verify_keeps_on_query_store_evidence_and_reports_missing_buckets(
    server_config_factory,
) -> None:
    # Keep needs the forced plan's own baseline: plan 7 ran before the apply,
    # so its tail after the apply is compared with its own tail.
    service, store, _admin, intent_id = await _applied_force_plan(
        server_config_factory,
        runtime_rows=[
            _qs_row("baseline", 7, 40, 100.0, cpu=40.0, reads=1000.0),
            _qs_row("post", 7, 40, 70.0),
        ],
    )

    result = await service.verify(
        "appdb",
        intent_id,
        authorization_reference="unused-unless-rollback",
    )

    assert result["decision"] == "keep"
    assert result["evidence"]["post_plans_without_baseline"] == []
    assert store.get_plan_action_intent(intent_id).status == "kept"
    evidence = result["evidence"]
    assert evidence["source"] == "mcp_query_store"
    assert evidence["forced_plan_execution_share"] == 1.0
    assert evidence["post"]["count_executions"] == 40
    assert evidence["post"]["avg_duration"] == 70.0
    assert evidence["baseline"]["avg_duration"] == 100.0
    # Query Store has no runtime parameter values: say so, never invent buckets.
    assert evidence["parameter_buckets"] == "unavailable"
    assert [row["plan_id"] for row in evidence["per_plan"]] == [7, 7]


@pytest.mark.asyncio
async def test_verify_holds_when_forced_plan_did_not_run_most_executions(
    server_config_factory,
) -> None:
    # Forcing can silently fail (NO_INDEX, NO_PLAN): the query keeps running on
    # another plan, so the post numbers say nothing about the forced plan.
    service, store, admin, intent_id = await _applied_force_plan(
        server_config_factory,
        runtime_rows=[
            *_baseline_rows(),
            _qs_row("post", 7, 10, 50.0),
            _qs_row("post", 9, 90, 60.0),
        ],
    )

    result = await service.verify(
        "appdb",
        intent_id,
        candidate_evidence=_candidate(duration=60.0),
        authorization_reference="unused-unless-rollback",
    )

    assert result["decision"] == "hold"
    intent = store.get_plan_action_intent(intent_id)
    assert intent.status == "hold"
    assert "forced plan" in intent.metadata["verification"]["reason"].lower()
    assert result["evidence"]["forced_plan_execution_share"] == 0.1
    assert len(admin.actions) == 1


@pytest.mark.asyncio
async def test_verify_holds_when_force_failures_rise_after_apply(
    server_config_factory,
) -> None:
    service, store, admin, intent_id = await _applied_force_plan(
        server_config_factory,
        verify_state=_state(7, None, force_failures=3, failure_reason="NO_INDEX"),
        runtime_rows=_improved_rows(),
    )

    result = await service.verify(
        "appdb",
        intent_id,
        candidate_evidence=_candidate(duration=70.0),
        authorization_reference="unused-unless-rollback",
    )

    assert result["decision"] == "hold"
    intent = store.get_plan_action_intent(intent_id)
    assert intent.status == "hold"
    assert "NO_INDEX" in intent.metadata["verification"]["reason"]
    assert len(admin.actions) == 1


@pytest.mark.asyncio
async def test_verify_holds_when_tail_regresses_inside_better_average(
    server_config_factory,
) -> None:
    # 990 small runs 100 -> 50 ms, 10 large runs 100 -> 2000 ms: the average
    # improves 30.5% but the slowest executions got 13x worse.
    service, store, _admin, intent_id = await _applied_force_plan(
        server_config_factory,
        runtime_rows=[
            _qs_row("baseline", 3, 1000, 100.0, cpu=40.0, reads=1000.0, max_duration=150.0),
            _qs_row("post", 7, 1000, 69.5, max_duration=2000.0),
        ],
    )

    result = await service.verify(
        "appdb",
        intent_id,
        authorization_reference="unused-unless-rollback",
    )

    assert result["decision"] == "hold"
    assert store.get_plan_action_intent(intent_id).status == "hold"


@pytest.mark.asyncio
async def test_verify_compares_tail_with_forced_plans_own_baseline(
    server_config_factory,
) -> None:
    # The classic force: plan 7 served small/medium values (max 120), the
    # sniffed plan 9 ran the large ones at 5000. After forcing 7, medium goes
    # 100 -> 1500. The all-plan baseline max (5000) hides that; plan 7's own
    # baseline max (120) does not.
    service, store, admin, intent_id = await _applied_force_plan(
        server_config_factory,
        runtime_rows=[
            _qs_row("baseline", 7, 950, 52.63, max_duration=120.0),
            _qs_row("baseline", 9, 50, 5000.0, max_duration=5000.0),
            _qs_row("post", 7, 1000, 135.0, max_duration=1500.0),
        ],
    )

    result = await service.verify(
        "appdb",
        intent_id,
        authorization_reference="unused-unless-rollback",
    )

    assert result["decision"] == "hold"
    verification = store.get_plan_action_intent(intent_id).metadata["verification"]
    assert "max_duration" in verification["reason"]
    assert verification["regressed_metrics"] == ["max_duration"]
    assert result["evidence"]["tail_baseline_basis"] == "forced_plan"
    assert result["evidence"]["baseline"]["max_duration"] == 120.0
    assert len(admin.actions) == 1


@pytest.mark.asyncio
async def test_verify_compares_spread_with_forced_plans_own_baseline(
    server_config_factory,
) -> None:
    # Max is unchanged, but plan 7 now runs a wider spread than it did for the
    # values it served before; across all plans the baseline spread was huge.
    service, store, _admin, intent_id = await _applied_force_plan(
        server_config_factory,
        runtime_rows=[
            _qs_row("baseline", 7, 950, 50.0, max_duration=5000.0, stdev=10.0),
            _qs_row("baseline", 9, 50, 3000.0, max_duration=5000.0, stdev=500.0),
            _qs_row("post", 7, 1000, 60.0, max_duration=5000.0, stdev=200.0),
        ],
    )

    result = await service.verify(
        "appdb",
        intent_id,
        authorization_reference="unused-unless-rollback",
    )

    assert result["decision"] == "hold"
    verification = store.get_plan_action_intent(intent_id).metadata["verification"]
    assert verification["regressed_metrics"] == ["stdev_duration"]
    assert result["evidence"]["baseline"]["stdev_duration"] == pytest.approx(10.0)
    assert result["evidence"]["post"]["stdev_duration"] == pytest.approx(200.0)


@pytest.mark.asyncio
async def test_verify_holds_when_forced_plan_has_no_baseline_of_its_own(
    server_config_factory,
) -> None:
    # The common force: only the sniffed plan 3 ran in the 24 h baseline, and
    # plan 7 comes from older history. Plan 3's outliers (max 5000) set the
    # tail baseline, so medium values going 100 -> 1500 under plan 7 stay
    # under it. With no per-parameter stats, this must not be a keep.
    service, store, admin, intent_id = await _applied_force_plan(
        server_config_factory,
        runtime_rows=[
            _qs_row("baseline", 3, 1000, 300.0, max_duration=5000.0),
            _qs_row("post", 7, 1000, 135.0, max_duration=1500.0),
        ],
    )

    result = await service.verify(
        "appdb",
        intent_id,
        authorization_reference="unused-unless-rollback",
    )

    assert result["decision"] == "hold"
    reason = store.get_plan_action_intent(intent_id).metadata["verification"]["reason"]
    assert "did not run in the baseline window" in reason
    assert "get_query_parameter_buckets" in reason
    assert result["evidence"]["post_plans_without_baseline"] == [7]
    assert len(admin.actions) == 1


@pytest.mark.asyncio
async def test_verify_holds_for_bucket_check_when_baseline_ran_several_plans(
    server_config_factory,
) -> None:
    # Several baseline plans mean a parameter-sensitive query, and Query Store
    # has no per-parameter runtime stats: a better blend is not enough to keep.
    service, store, admin, intent_id = await _applied_force_plan(
        server_config_factory,
        runtime_rows=[
            _qs_row("baseline", 3, 30, 100.0, max_duration=400.0),
            _qs_row("baseline", 9, 10, 300.0, max_duration=400.0),
            _qs_row("post", 7, 40, 70.0, max_duration=200.0),
        ],
    )

    result = await service.verify(
        "appdb",
        intent_id,
        authorization_reference="unused-unless-rollback",
    )

    assert result["decision"] == "hold"
    intent = store.get_plan_action_intent(intent_id)
    assert intent.status == "hold"
    reason = intent.metadata["verification"]["reason"]
    assert "2 plans" in reason
    assert "get_query_parameter_buckets" in reason
    assert result["evidence"]["baseline_plan_count"] == 2
    assert len(admin.actions) == 1


@pytest.mark.asyncio
async def test_caller_regression_context_downgrades_query_store_keep(
    server_config_factory,
) -> None:
    service, store, admin, intent_id = await _applied_force_plan(
        server_config_factory,
        runtime_rows=_improved_rows(),
    )

    result = await service.verify(
        "appdb",
        intent_id,
        candidate_evidence=_candidate(duration=150.0),
        authorization_reference="unused-unless-rollback",
    )

    assert result["decision"] == "hold"
    assert store.get_plan_action_intent(intent_id).status == "hold"
    assert len(admin.actions) == 1


def _expire(store: PerformanceStore, intent_id: str, **changes: Any) -> None:
    intent = store.get_plan_action_intent(intent_id)
    metadata = dict(intent.metadata)
    for key, value in changes.items():
        if value is None:
            metadata.pop(key, None)
        else:
            metadata[key] = value
    created = changes.get("created_at_utc")
    metadata.pop("created_at_utc", None)
    store.save_plan_action_intent(
        replace(
            intent,
            metadata=metadata,
            created_at_utc=created or intent.created_at_utc,
            version=intent.version + 1,
        ),
        expected_version=intent.version,
    )


async def _prepare_only(service: PlanActionService, session_id: str) -> dict[str, Any]:
    return await service.prepare(
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
        idempotency_key="expiry",
    )


@pytest.mark.asyncio
async def test_prepare_sets_24_hour_expiry_and_apply_refuses_expired_intent(
    server_config_factory,
) -> None:
    # Evidence from last month says nothing about today's workload; an old
    # intent must be prepared again, not applied.
    service, store, admin, session_id = _service(
        server_config_factory,
        [_state(None, None)],
    )
    prepared = await _prepare_only(service, session_id)
    intent = prepared["intent"]
    expires_at = datetime.fromisoformat(intent["metadata"]["expires_at_utc"])
    created_at = datetime.fromisoformat(intent["created_at_utc"])
    assert expires_at - created_at == timedelta(hours=24)
    assert prepared["expires_at_utc"] == intent["metadata"]["expires_at_utc"]

    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    _expire(store, intent["intent_id"], expires_at_utc=past)
    result = await service.apply(
        "appdb",
        intent["intent_id"],
        authorization_reference="approved-change",
    )

    assert result["confirmed"] is False
    stored = store.get_plan_action_intent(intent["intent_id"])
    assert stored.status == "hold"
    assert stored.metadata["apply_confirmation"] == "intent_expired"
    assert admin.actions == []


@pytest.mark.asyncio
async def test_rollback_never_touches_plan_state_for_an_intent_that_was_never_applied(
    server_config_factory,
) -> None:
    # An expired intent is held without any change. Later a DBA forces plan 9.
    # Rolling back the held intent must not unforce the DBA's plan: this
    # intent applied nothing, so it has nothing to restore.
    dba_forced = _state(9, None)
    service, store, admin, session_id = _service(
        server_config_factory,
        [_state(None, None), dba_forced, dba_forced],
    )
    prepared = await _prepare_only(service, session_id)
    intent_id = prepared["intent"]["intent_id"]
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    _expire(store, intent_id, expires_at_utc=past)
    await service.apply("appdb", intent_id, authorization_reference="approved-change")
    assert store.get_plan_action_intent(intent_id).status == "hold"

    result = await service.rollback(
        "appdb", intent_id, authorization_reference="approved-rollback"
    )

    assert result["confirmed"] is False
    assert result["not_applied"] is True
    assert admin.actions == []
    assert store.get_plan_action_intent(intent_id).status == "hold"


@pytest.mark.asyncio
async def test_rollback_holds_when_plan_state_changed_after_apply(
    server_config_factory,
) -> None:
    # The intent forced plan 7. A DBA then forced plan 9 by hand. Restoring
    # the prior state would undo the DBA's change, not this intent's.
    service, store, admin, intent_id = await _applied_force_plan(
        server_config_factory,
        verify_state=_state(9, None),
    )

    result = await service.rollback(
        "appdb", intent_id, authorization_reference="approved-rollback"
    )

    assert result["confirmed"] is False
    assert result["decision"] == "hold"
    intent = store.get_plan_action_intent(intent_id)
    assert intent.status == "hold"
    assert intent.metadata["rollback_confirmation"] == "state_changed"
    assert len(admin.actions) == 1  # only the original apply


@pytest.mark.asyncio
async def test_apply_refuses_legacy_intent_without_expiry_once_ttl_passed(
    server_config_factory,
) -> None:
    service, store, admin, session_id = _service(
        server_config_factory,
        [_state(None, None)],
    )
    prepared = await _prepare_only(service, session_id)
    old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    _expire(store, prepared["intent"]["intent_id"], expires_at_utc=None, created_at_utc=old)

    result = await service.apply(
        "appdb",
        prepared["intent"]["intent_id"],
        authorization_reference="approved-change",
    )

    assert result["confirmed"] is False
    assert store.get_plan_action_intent(prepared["intent"]["intent_id"]).status == "hold"
    assert admin.actions == []


@pytest.mark.asyncio
async def test_intent_ttl_is_configurable(server_config_factory) -> None:
    service, _store, _admin, session_id = _service(
        server_config_factory,
        [_state(None, None)],
        intent_ttl=timedelta(hours=2),
    )

    intent = (await _prepare_only(service, session_id))["intent"]

    expires_at = datetime.fromisoformat(intent["metadata"]["expires_at_utc"])
    created_at = datetime.fromisoformat(intent["created_at_utc"])
    assert expires_at - created_at == timedelta(hours=2)


@pytest.mark.asyncio
async def test_verify_rollback_reports_regressed_metrics(server_config_factory) -> None:
    # Duration improves 30% but reads double: the operator must learn the
    # rollback came from reads, not time, or they will re-force the same plan.
    service, store, _admin, intent_id = await _applied_force_plan(
        server_config_factory,
        runtime_rows=[
            _qs_row("baseline", 7, 40, 100.0, cpu=40.0, reads=1000.0),
            _qs_row("post", 7, 40, 70.0, cpu=30.0, reads=2000.0),
        ],
        extra_states=[_state(7, None, force_failures=0), _state(None, None)],
    )

    result = await service.verify(
        "appdb", intent_id, authorization_reference="approved-rollback"
    )

    assert result["decision"] == "rollback"
    assert result["confirmed"] is True
    assert result["regressed_metrics"] == ["avg_logical_io_reads"]
    intent = store.get_plan_action_intent(intent_id)
    assert intent.status == "rolled_back"
    verification = intent.metadata["verification"]
    assert verification["action"] == "rollback"
    assert verification["regressed_metrics"] == ["avg_logical_io_reads"]
    assert verification["evidence"]["post"]["count_executions"] == 40


@pytest.mark.asyncio
async def test_verify_rollback_hold_keeps_regressed_metrics(server_config_factory) -> None:
    # Automatic tuning took the query after verify chose rollback: the intent
    # holds, and the reason for the attempted rollback must survive the hold.
    service, store, _admin, intent_id = await _applied_force_plan(
        server_config_factory,
        runtime_rows=[
            _qs_row("baseline", 7, 40, 100.0, cpu=40.0, reads=1000.0),
            _qs_row("post", 7, 40, 70.0, cpu=30.0, reads=2000.0),
        ],
        extra_states=[_state(7, None, ownership="automatic")],
    )

    result = await service.verify(
        "appdb", intent_id, authorization_reference="approved-rollback"
    )

    assert result["decision"] == "rollback"
    assert result["confirmed"] is False
    intent = store.get_plan_action_intent(intent_id)
    assert intent.status == "hold"
    assert intent.metadata["rollback_confirmation"] == "ownership_changed"
    assert intent.metadata["verification"]["regressed_metrics"] == [
        "avg_logical_io_reads"
    ]


@pytest.mark.asyncio
async def test_state_errors_keep_the_status_word_after_redaction(
    server_config_factory,
) -> None:
    # The MCP error sanitizer masks single-quoted text, so a quoted status
    # reaches the caller as '[REDACTED]' and the operator cannot see why.
    from azure_sql_mcp.observability import sanitize_error_message

    service, _store, _admin, session_id = _service(
        server_config_factory, [_state(None, None)]
    )
    intent_id = (await _prepare_only(service, session_id))["intent"]["intent_id"]

    with pytest.raises(ValueError) as rollback_error:
        await service.rollback(
            "appdb", intent_id, authorization_reference="approved-rollback"
        )
    with pytest.raises(ValueError) as verify_error:
        await service.verify(
            "appdb", intent_id, authorization_reference="approved-rollback"
        )

    for error in (rollback_error.value, verify_error.value):
        message = sanitize_error_message(str(error))
        assert "prepared" in message
        assert "[REDACTED]" not in message


class ConcurrentWriterAdminPolicy(RecordingAdminPolicy):
    """Another writer bumps the intent row while the force call runs."""

    def __init__(self, store: PerformanceStore, intent_id: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.store = store
        self.intent_id = intent_id

    async def execute(self, action, _executor, *, dry_run: bool, max_rows=None):
        latest = self.store.get_plan_action_intent(self.intent_id)
        self.store.save_plan_action_intent(
            replace(latest, status="hold", version=latest.version + 1),
            expected_version=latest.version,
        )
        return await super().execute(action, _executor, dry_run=dry_run, max_rows=max_rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("dispatch_fails", [False, True])
async def test_apply_reports_outcome_unknown_when_dispatched_result_cannot_be_recorded(
    server_config_factory,
    dispatch_fails: bool,
) -> None:
    # The force call was sent. A lost status write must not read as a plain
    # state-store error (safe to retry): the plan may be forced already.
    from azure_sql_mcp.incident_log import describe_exception, tool_failure_code
    from azure_sql_mcp.plan_action_service import PlanActionOutcomeUnknownError

    service, store, _admin, session_id = _service(
        server_config_factory,
        [_state(None, None), _state(None, None), _state(7, None)],
    )
    intent_id = (await _prepare_only(service, session_id))["intent"]["intent_id"]
    writer = ConcurrentWriterAdminPolicy(
        store, intent_id, fail_on_call=1 if dispatch_fails else None
    )
    service.admin_policy = writer  # type: ignore[assignment]

    with pytest.raises(PlanActionOutcomeUnknownError) as raised:
        await service.apply("appdb", intent_id, authorization_reference="approved-change")

    assert len(writer.actions) == 1
    assert tool_failure_code(raised.value) == "verification_failed"
    described = describe_exception(raised.value)
    assert (described["category"], described["priority"]) == ("outcome_unknown", "P1")
    assert store.get_plan_action_intent(intent_id).status == "hold"


@pytest.mark.asyncio
async def test_apply_records_unknown_when_the_confirm_read_fails(server_config_factory) -> None:
    # The force call was sent and the store works; only the confirm read failed.
    # The intent must say so (unknown, confirm_read_failed), not stay "applying".
    service, store, _admin, session_id = _service(
        server_config_factory,
        [_state(None, None), _state(None, None)],  # no state left for the confirm read
    )
    intent_id = (await _prepare_only(service, session_id))["intent"]["intent_id"]

    result = await service.apply("appdb", intent_id, authorization_reference="approved-change")

    assert result["confirmed"] is False
    intent = store.get_plan_action_intent(intent_id)
    assert intent.status == "unknown"
    assert intent.metadata["apply_confirmation"] == "confirm_read_failed"
    assert intent.metadata["apply_error_type"] == "IndexError"


@pytest.mark.asyncio
async def test_rollback_records_unknown_when_the_confirm_read_fails(server_config_factory) -> None:
    service, store, admin, intent_id = await _applied_force_plan(server_config_factory)

    result = await service.rollback(
        "appdb", intent_id, authorization_reference="approved-change", reason="operator"
    )

    assert len(admin.actions) == 2  # force, then the restore was sent
    assert result["confirmed"] is False
    intent = store.get_plan_action_intent(intent_id)
    assert intent.status == "unknown"
    assert intent.metadata["rollback_confirmation"] == "confirm_read_failed"


@pytest.mark.asyncio
async def test_rollback_reports_outcome_unknown_when_the_result_cannot_be_recorded(
    server_config_factory,
) -> None:
    # The restore was sent, then another writer changed the intent row, so the
    # rolled_back status cannot be saved. That must not read as a refusal.
    from azure_sql_mcp.incident_log import tool_failure_code
    from azure_sql_mcp.plan_action_service import PlanActionOutcomeUnknownError

    service, store, _admin, intent_id = await _applied_force_plan(
        server_config_factory, extra_states=[_state(None, None)]
    )
    service.admin_policy = ConcurrentWriterAdminPolicy(store, intent_id)  # type: ignore[assignment]

    with pytest.raises(PlanActionOutcomeUnknownError) as raised:
        await service.rollback(
            "appdb", intent_id, authorization_reference="approved-change", reason="operator"
        )

    assert tool_failure_code(raised.value) == "verification_failed"
