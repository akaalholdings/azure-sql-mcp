"""Force precheck, enforcement guardrails and definite apply failures."""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

import pytest

from azure_sql_mcp import plan_action_service
from tests.unit.test_plan_action_service import RecordingAdminPolicy
from tests.unit.test_plan_action_service import StateExecutor
from tests.unit.test_plan_action_service import _age_applied_intent
from tests.unit.test_plan_action_service import _baseline
from tests.unit.test_plan_action_service import _baseline_rows
from tests.unit.test_plan_action_service import _qs_row
from tests.unit.test_plan_action_service import _service
from tests.unit.test_plan_action_service import _state

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)


async def _prepare(service: Any, session_id: str, *, query_id: int = 42, key: str = "k") -> dict[str, Any]:
    return await service.prepare(
        "appdb",
        session_id=session_id,
        candidate_id=None,
        operation="force_plan",
        query_id=query_id,
        plan_id=7,
        query_hints=None,
        evidence=_baseline(),
        reviewed_by="operator",
        reason="reviewed regression",
        idempotency_key=key,
    )


def _executor(service: Any) -> StateExecutor:
    return service.executor


def _codes(items: list[dict[str, Any]]) -> list[str]:
    return [item["code"] for item in items]


@pytest.mark.asyncio
async def test_healthy_force_target_passes_the_precheck(server_config_factory) -> None:
    service, _store, _admin, session_id = _service(server_config_factory, [_state(None, None)])

    prepared = await _prepare(service, session_id)

    assert prepared["prepared"] is True
    assert prepared["force_precheck"]["status"] == "pass"


@pytest.mark.asyncio
async def test_force_is_blocked_for_a_parameter_sensitive_query(server_config_factory) -> None:
    # Plan 7 serves 10-row calls and plan 9 serves 50,000-row calls. Each plan
    # is right for its own values; forcing either one breaks the other range.
    service, _store, _admin, session_id = _service(server_config_factory, [_state(None, None)])
    _executor(service).bucket_rows = [
        {"plan_id": 7, "executions": 900, "avg_rowcount": 10.0},
        {"plan_id": 9, "executions": 40, "avg_rowcount": 50_000.0},
    ]

    prepared = await _prepare(service, session_id)

    assert prepared["prepared"] is False
    assert prepared["intent"]["status"] == "rejected"
    assert _codes(prepared["force_precheck"]["blockers"]) == ["parameter_sensitive_query"]
    assert "parameter" in prepared["intent"]["metadata"]["rejection_reason"]


@pytest.mark.asyncio
async def test_two_plans_with_the_same_row_counts_are_a_regression_not_sensitivity(
    server_config_factory,
) -> None:
    # A regressed plan next to the last good plan returns the same rows: this
    # is the case forcing exists for and it must not be blocked.
    service, _store, _admin, session_id = _service(server_config_factory, [_state(None, None)])
    _executor(service).bucket_rows = [
        {"plan_id": 7, "executions": 900, "avg_rowcount": 10.0},
        {"plan_id": 9, "executions": 40, "avg_rowcount": 12.0},
    ]

    prepared = await _prepare(service, session_id)

    assert prepared["prepared"] is True


@pytest.mark.asyncio
async def test_force_is_blocked_when_the_plan_last_ran_more_than_four_days_ago(
    server_config_factory,
) -> None:
    service, _store, _admin, session_id = _service(server_config_factory, [_state(None, None)])
    plan_row = _executor(service).plan_row
    assert plan_row is not None
    plan_row["minutes_since_last_execution"] = 5 * 24 * 60

    prepared = await _prepare(service, session_id)

    assert prepared["prepared"] is False
    assert _codes(prepared["force_precheck"]["blockers"]) == ["plan_not_run_recently"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("plan_row", "code"),
    [
        (None, "target_plan_not_found"),
        ({"query_id": 77}, "plan_belongs_to_another_query"),
    ],
)
async def test_force_is_blocked_when_the_target_plan_is_not_a_plan_of_the_query(
    server_config_factory, plan_row: dict[str, Any] | None, code: str
) -> None:
    # The engine refuses this pair at apply. Prepare must say so, not hand back
    # an intent that can never be applied.
    service, _store, _admin, session_id = _service(server_config_factory, [_state(None, None)])
    executor = _executor(service)
    executor.plan_row = None if plan_row is None else {**(executor.plan_row or {}), **plan_row}

    prepared = await _prepare(service, session_id)

    assert prepared["prepared"] is False
    assert _codes(prepared["force_precheck"]["blockers"]) == [code]


@pytest.mark.asyncio
async def test_precheck_warns_without_blocking(server_config_factory) -> None:
    service, _store, _admin, session_id = _service(server_config_factory, [_state(None, None)])
    executor = _executor(service)
    assert executor.plan_row is not None
    executor.plan_row.update(
        has_option_recompile=1,
        plan_uses_temp_table=1,
        query_store_state="READ_ONLY",
        force_failure_count=3,
        last_force_failure_reason_desc="NO_PLAN",
    )
    executor.variant_rows = [{"parent_query_id": 12, "dispatcher_plan_id": 900}]

    prepared = await _prepare(service, session_id)

    precheck = prepared["force_precheck"]
    assert prepared["prepared"] is True
    assert precheck["status"] == "warn"
    assert sorted(_codes(precheck["warnings"])) == [
        "option_recompile",
        "prior_force_failures",
        "psp_variant",
        "query_store_read_only",
        "temp_table_in_plan",
    ]
    variant = next(item for item in precheck["warnings"] if item["code"] == "psp_variant")
    assert variant["parent_query_id"] == 12
    # The warnings stay on the durable intent for the reviewer.
    assert prepared["intent"]["metadata"]["force_precheck"]["status"] == "warn"


@pytest.mark.asyncio
async def test_precheck_that_cannot_be_read_blocks(server_config_factory) -> None:
    service, _store, _admin, session_id = _service(server_config_factory, [_state(None, None)])
    _executor(service).precheck_error = RuntimeError("Query Store read failed")

    prepared = await _prepare(service, session_id)

    assert prepared["prepared"] is False
    assert _codes(prepared["force_precheck"]["blockers"]) == ["precheck_unavailable"]


def _event(kind: str, hours_ago: float, *, query_id: int = 42, intent: str = "i", **payload: Any) -> dict[str, Any]:
    return {
        "event_type": f"plan_action.{kind}",
        "payload": {
            "query_id": query_id,
            "intent_id": intent,
            "attempt": 1,
            "at_utc": (NOW - timedelta(hours=hours_ago)).isoformat(),
            **payload,
        },
    }


def test_guardrails_allow_a_first_action() -> None:
    assert plan_action_service.evaluate_guardrails([], query_id=42, now=NOW)["allowed"] is True


def test_query_cooldown_blocks_a_second_action_within_24_hours() -> None:
    blocked = plan_action_service.evaluate_guardrails([_event("dispatched", 23)], query_id=42, now=NOW)
    other_query = plan_action_service.evaluate_guardrails([_event("dispatched", 23)], query_id=43, now=NOW)
    later = plan_action_service.evaluate_guardrails([_event("dispatched", 25)], query_id=42, now=NOW)

    assert blocked["allowed"] is False
    assert blocked["blocked_by"] == "query_cooldown"
    assert blocked["retry_after_utc"] == (NOW + timedelta(hours=1)).isoformat()
    assert other_query["allowed"] is True
    assert later["allowed"] is True


def test_daily_cap_blocks_the_fourth_action_on_a_database() -> None:
    events = [_event("dispatched", hours, query_id=query, intent=f"i{query}")
              for query, hours in ((1, 20), (2, 10), (3, 2))]

    blocked = plan_action_service.evaluate_guardrails(events, query_id=42, now=NOW)

    assert blocked["blocked_by"] == "daily_action_cap"
    assert blocked["actions_last_24h"] == 3
    # The oldest of the three leaves the 24-hour window in 4 hours.
    assert blocked["retry_after_utc"] == (NOW + timedelta(hours=4)).isoformat()


def test_two_failed_actions_start_a_168_hour_cooldown() -> None:
    events = [
        _event("dispatched", 100, intent="a"),
        _event("failed", 99, intent="a", failure="rolled_back_after_regression", state_changed=True),
        _event("dispatched", 50, intent="b"),
        _event("failed", 49, intent="b", failure="force_failed", state_changed=True),
    ]

    blocked = plan_action_service.evaluate_guardrails(events, query_id=42, now=NOW)
    one_failure = plan_action_service.evaluate_guardrails(events[:2], query_id=42, now=NOW)

    assert blocked["blocked_by"] == "failure_cooldown"
    assert blocked["failed_actions"] == 2
    assert blocked["retry_after_utc"] == (NOW + timedelta(hours=168 - 49)).isoformat()
    assert one_failure["allowed"] is True


def test_an_apply_that_changed_nothing_does_not_start_the_cooldown() -> None:
    # The engine refused the action, so nothing was changed: the retry after
    # the cause is fixed must not wait a day or use up the daily cap.
    events = [
        _event("dispatched", 1, intent="a"),
        _event("failed", 1, intent="a", failure="apply_failed", state_changed=False),
    ]

    result = plan_action_service.evaluate_guardrails(events, query_id=42, now=NOW)

    assert result["allowed"] is True
    assert result["actions_last_24h"] == 0
    # Two refused applies must not lock the query for a week either.
    assert result["failed_actions"] == 0


def test_a_retry_that_succeeds_after_a_refused_apply_is_an_action() -> None:
    # Attempt 1 changed nothing; attempt 2 of the same intent forced the plan.
    events = [
        _event("dispatched", 2, intent="a"),
        _event("failed", 2, intent="a", failure="apply_failed", state_changed=False),
        _event("dispatched", 1, intent="a", attempt=3),
    ]

    result = plan_action_service.evaluate_guardrails(events, query_id=42, now=NOW)

    assert result["blocked_by"] == "query_cooldown"
    assert result["actions_last_24h"] == 1


@pytest.mark.asyncio
async def test_apply_is_refused_inside_the_query_cooldown(server_config_factory) -> None:
    # First intent is applied. A second intent for the same query within the
    # day is refused before any state is read or changed, and stays prepared.
    prior, applied = _state(None, None), _state(7, None)
    service, store, admin, session_id = _service(
        server_config_factory, [prior, prior, applied, applied]
    )
    first = await _prepare(service, session_id, key="first")
    assert (await service.apply("appdb", first["intent"]["intent_id"], authorization_reference="a"))[
        "confirmed"
    ] is True
    second = await _prepare(service, session_id, key="second")

    result = await service.apply(
        "appdb", second["intent"]["intent_id"], authorization_reference="a"
    )

    assert result["confirmed"] is False
    assert result["guardrail"]["blocked_by"] == "query_cooldown"
    assert store.get_plan_action_intent(second["intent"]["intent_id"]).status == "prepared"
    assert len(admin.actions) == 1
    assert second["guardrails"]["blocked_by"] == "query_cooldown"


@pytest.mark.asyncio
async def test_unforce_is_never_delayed_by_the_guardrails(server_config_factory) -> None:
    # The forced plan turned out wrong an hour after the force. Removing it
    # must not wait for the cooldown that limits new interventions.
    prior, forced = _state(None, None), _state(7, None)
    service, store, admin, session_id = _service(
        server_config_factory, [prior, prior, forced, forced, forced, prior]
    )
    first = await _prepare(service, session_id, key="force")
    await service.apply("appdb", first["intent"]["intent_id"], authorization_reference="a")
    unforce = await service.prepare(
        "appdb",
        session_id=session_id,
        candidate_id=None,
        operation="unforce_plan",
        query_id=42,
        plan_id=7,
        query_hints=None,
        evidence=_baseline(),
        reviewed_by="operator",
        reason="forced plan is wrong",
        idempotency_key="unforce",
    )

    result = await service.apply(
        "appdb", unforce["intent"]["intent_id"], authorization_reference="a"
    )

    assert result["confirmed"] is True
    assert len(admin.actions) == 2
    assert "force_precheck" not in unforce


class RefusingAdminPolicy(RecordingAdminPolicy):
    async def execute(self, action, _executor, *, dry_run: bool, max_rows=None):
        self.actions.append(action)
        raise RuntimeError("Msg 12406: plan is not found in the Query Store for query (12406)")


@pytest.mark.asyncio
async def test_definite_apply_failure_returns_the_intent_to_prepared(server_config_factory) -> None:
    # The engine refused the force and the control state is unchanged. The
    # intent must not sit in "unknown" forever: nothing was applied.
    prior = _state(None, None)
    service, store, _admin, session_id = _service(server_config_factory, [prior, prior, prior])
    service.admin_policy = RefusingAdminPolicy()
    prepared = await _prepare(service, session_id)
    intent_id = prepared["intent"]["intent_id"]

    result = await service.apply("appdb", intent_id, authorization_reference="a")

    stored = store.get_plan_action_intent(intent_id)
    assert result["confirmed"] is False
    assert result["apply_failed"] is True
    assert stored.status == "prepared"
    assert stored.metadata["apply_confirmation"] == "failed_no_change"
    assert stored.metadata["apply_error_type"] == "RuntimeError"
    assert "applied_at_utc" not in stored.metadata

    # The cause is fixed: the same intent applies now, with no cooldown.
    service.admin_policy = RecordingAdminPolicy()
    _executor(service).states.extend([prior, _state(7, None), _state(7, None)])
    retried = await service.apply("appdb", intent_id, authorization_reference="a")

    assert retried["confirmed"] is True
    # The retry forced the plan, so the query is now inside its cooldown.
    again = await _prepare(service, session_id, key="again")
    assert again["guardrails"]["blocked_by"] == "query_cooldown"


@pytest.mark.asyncio
async def test_rollback_after_a_regression_counts_as_a_failed_action(server_config_factory) -> None:
    # Verification rolled the forced plan back because it was slower. Two such
    # failures for one query start the 168-hour cooldown.
    prior, applied = _state(None, None), _state(7, None)
    service, store, _admin, session_id = _service(
        server_config_factory,
        [prior, prior, applied, applied, applied, prior],
        runtime_rows=[*_baseline_rows(), _qs_row("post", 7, 40, 150.0)],
    )
    prepared = await _prepare(service, session_id)
    intent_id = prepared["intent"]["intent_id"]
    await service.apply("appdb", intent_id, authorization_reference="a")
    _age_applied_intent(store, intent_id)

    verified = await service.verify("appdb", intent_id, authorization_reference="a")

    assert verified["decision"] == "rollback"
    database_fingerprint = prepared["intent"]["metadata"]["database_fingerprint"]
    failed = [
        event["payload"]
        for event in store.list_events(
            aggregate_type="plan_action_guardrail", aggregate_id=database_fingerprint
        )
        if event["event_type"] == "plan_action.failed"
    ]
    assert [(item["query_id"], item["failure"]) for item in failed] == [
        (42, "rolled_back_after_regression")
    ]


@pytest.mark.asyncio
async def test_apply_failure_stays_unknown_when_the_state_did_change(server_config_factory) -> None:
    # The error came back but the plan is forced: the outcome is not a clean
    # failure and the intent must not go back to prepared.
    prior, applied = _state(None, None), _state(7, None)
    service, store, _admin, session_id = _service(server_config_factory, [prior, prior, applied])
    service.admin_policy = RefusingAdminPolicy()
    prepared = await _prepare(service, session_id)

    result = await service.apply(
        "appdb", prepared["intent"]["intent_id"], authorization_reference="a"
    )

    assert result["confirmed"] is False
    assert store.get_plan_action_intent(prepared["intent"]["intent_id"]).status == "unknown"
