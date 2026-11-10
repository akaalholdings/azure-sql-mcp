"""diagnose_database: scored facts, causes that need corroboration, and honest gaps.

A list of findings is what a bare model already produces from raw DMVs. These
tests pin what a DBA needs instead: bars that do not fire on an idle database,
an unreadable source that is never an all-clear, and a cause only when two
facts agree.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.database_diagnosis import MAX_FACTS
from azure_sql_mcp.database_diagnosis import DatabaseDiagnosisService
from azure_sql_mcp.database_diagnosis import diagnose_findings
from azure_sql_mcp.database_diagnosis import score_facts


def _by_code(evidence: dict[str, Any], **options: Any) -> dict[str, Any]:
    return {fact.code: fact for fact in score_facts(evidence, **options)}


def _wait(wait_type: str, ratio: float, level: str, share: float, bar: float | None = None) -> dict[str, Any]:
    return {
        "wait_type": wait_type,
        "pct_of_total": share,
        "wait_seconds_per_observed_second": ratio,
        "absolute_level": level,
        "absolute_bar": bar,
    }


_INTERVAL = {"mode": "interval", "sample_seconds": 15}
_CUMULATIVE = {"mode": "cumulative", "since_utc": "2026-11-01T00:00:00"}
_LIMITS = {"service_objective": {"service_objective": "GP_Gen5_4"}, "governance_limits": {"cpu_limit": 4}}


def _cpu_history(avg: float, peak: float, p95: float | None = None, minutes_above_95: float | None = 0.0) -> dict[str, Any]:
    return {
        "source": "recent",
        "summary": {
            "avg_cpu_percent": {
                "avg": avg,
                "max": peak,
                "p95": p95 if p95 is not None else peak,
                "total_samples": 240,
                "minutes_above_80": 0.0,
                "minutes_above_95": minutes_above_95,
            }
        },
    }


def _top_query(share_of_capacity: float, *, vcores: int = 4, minutes: int = 60, **extra: Any) -> dict[str, Any]:
    cpu_us = share_of_capacity * vcores * minutes * 60 * 1_000_000
    return {
        "sample_query_id": 9,
        "query_hash": "0x0A",
        "executions": 4_000,
        "total_cpu_us": cpu_us,
        "avg_logical_io_reads": 400_000,
        "avg_rowcount": 40,
        "distinct_query_count": 1,
        "unparameterized_query_count": 0,
        **extra,
    }


def _idle_chain(blocked: int, wait_ms: int) -> dict[str, Any]:
    return {
        "head_blocker_session_id": 61,
        "head_blocker_state": "sleeping_open_transaction",
        "head_blocker": {"program_name": "billing-api", "open_transaction_count": 1, "idle_seconds": 840},
        "blocked_session_ids": list(range(100, 100 + blocked)),
        "total_blocked_count": blocked,
        "chain_depth": 1,
        "max_wait_time_ms": wait_ms,
        "victim_split": {"reader": 0, "writer": blocked, "range": 0, "schema": 0, "non_lock": 0},
    }


# --- Facts: absolute bars ---------------------------------------------------


def test_idle_database_has_no_actionable_finding() -> None:
    # 2% CPU, a 250% regression worth 60 ms in total, one query with half of the
    # top-five CPU, and 200 ms of PAGEIOLATCH in 15 s. Relative shares called all
    # of this a problem; none of it is.
    evidence = {
        "resource_history": _cpu_history(2.0, 6.0),
        "resource_limits": _LIMITS,
        "waits": {"top_waits": [_wait("PAGEIOLATCH_SH", 0.0133, "below_bar", 92.0, 0.25)], "window": _INTERVAL},
        "sessions": {"blocking_chains": [], "sessions": []},
        "top_queries": [_top_query(0.01), _top_query(0.01, query_hash="0x0B")],
        "regressions": {
            "window": {"recent_minutes": 60},
            "regressions": [{"query_id": 3, "regression_pct": 250.0, "plan_changed": True, "extra_cost": 60.0}],
        },
    }

    facts = score_facts(evidence)

    assert facts, "an idle database still reports its numbers"
    assert {fact.severity for fact in facts} == {"info"}
    assert diagnose_findings(evidence) == []


def test_a_wait_is_judged_on_its_absolute_bar_not_its_share() -> None:
    facts = _by_code(
        {
            "waits": {
                "top_waits": [
                    _wait("LCK_M_X", 0.01, "below_bar", 78.0, 0.10),
                    _wait("PAGEIOLATCH_SH", 0.40, "above_bar", 22.0, 0.25),
                ],
                "window": _INTERVAL,
            }
        }
    )

    assert "lock_waits" not in facts and "dominant_wait" not in facts
    io = facts["io_latch_waits"]
    assert (io.severity, io.value, io.unit) == ("medium", 0.40, "wait_seconds_per_observed_second")
    assert io.bar["kind"] == "absolute" and io.bar["above_bar"] == 0.25


def test_cumulative_waits_are_inferred_and_a_sample_is_verified() -> None:
    def fact(window: dict[str, Any]) -> Any:
        return _by_code({"waits": {"top_waits": [_wait("LCK_M_S", 0.2, "above_bar", 60.0, 0.05)], "window": window}})[
            "lock_waits"
        ]

    assert fact(_INTERVAL).basis == "verified"
    assert fact(_CUMULATIVE).basis == "inferred"
    assert "2026-11-01" in fact(_CUMULATIVE).window


def test_share_is_only_a_tie_breaker_when_no_absolute_bar_can_be_applied() -> None:
    facts = _by_code(
        {"waits": {"top_waits": [_wait("HADR_SYNC_COMMIT", 0.3, "no_bar", 64.0)], "window": _INTERVAL}}
    )

    lead = facts["dominant_wait"]
    assert lead.severity == "low"
    assert lead.bar["kind"] == "relative_share"


@pytest.mark.parametrize(
    ("blocked", "depth", "severity"),
    [(2, 1, "low"), (6, 1, "medium"), (30, 1, "high"), (2, 3, "medium"), (2, 8, "high")],
)
def test_blocking_is_scored_on_victims_and_depth(blocked: int, depth: int, severity: str) -> None:
    chain = {
        "head_blocker_session_id": 51,
        "head_blocker_state": "running",
        "blocked_session_ids": [60],
        "total_blocked_count": blocked,
        "chain_depth": depth,
        "max_wait_time_ms": 500,
    }

    assert _by_code({"sessions": {"blocking_chains": [chain]}})["active_blocking"].severity == severity


def test_one_session_blocked_for_a_minute_is_a_long_lock_wait() -> None:
    facts = _by_code({"sessions": {"blocking_chains": [_idle_chain(1, 70_000)]}})

    assert facts["active_blocking"].severity == "low"
    assert facts["long_lock_wait"].severity == "high"
    assert facts["idle_head_blocker"].detail["program_name"] == "billing-api"


def test_dominant_query_is_measured_against_cpu_capacity() -> None:
    busy = _cpu_history(88.0, 100.0)

    scored = _by_code({"resource_history": busy, "resource_limits": _LIMITS, "top_queries": [_top_query(0.30)]})
    small = _by_code({"resource_history": busy, "resource_limits": _LIMITS, "top_queries": [_top_query(0.04)]})

    fact = scored["dominant_query"]
    assert fact.severity == "high"
    assert fact.detail["share_of_cpu_capacity_pct"] == 30.0
    assert fact.detail["shape"] == "frequent_heavy"
    # 4% of capacity is the largest of the top five and still not a bad actor.
    assert small["dominant_query"].severity == "info"


def test_without_the_cpu_limit_the_share_of_top_queries_is_only_a_lead() -> None:
    fact = _by_code({"resource_history": _cpu_history(88.0, 100.0), "top_queries": [_top_query(0.30)]})[
        "dominant_query"
    ]

    assert (fact.severity, fact.basis) == ("low", "inferred")
    assert fact.bar["kind"] == "relative_share"


def test_regression_severity_is_the_time_it_adds_per_hour() -> None:
    def severity(extra_ms: float) -> str:
        return _by_code(
            {
                "regressions": {
                    "window": {"recent_minutes": 60},
                    "regressions": [{"query_id": 9, "regression_pct": 300.0, "extra_cost": extra_ms}],
                }
            }
        )["query_regression"].severity

    assert severity(900_000.0) == "high"  # 900 s of added run time an hour
    assert severity(90_000.0) == "medium"
    assert severity(5_000.0) == "info"


def test_new_sources_become_facts() -> None:
    facts = _by_code(
        {
            "memory_pressure": {
                "findings": [
                    {
                        "code": "memory_grant_waiters",
                        "severity": "medium",
                        "message": "2 queries wait for a grant.",
                        "evidence": {"waiter_count": 2, "window": "point_in_time"},
                        "next_tools": ["get_memory_grants"],
                    },
                    {
                        "code": "forced_memory_grants",
                        "severity": "low",
                        "message": "9 forced grants since the engine started.",
                        "evidence": {"forced_grant_count_since_engine_start": 9, "window": "cumulative_since_engine_start"},
                        "next_tools": ["get_memory_grants"],
                    },
                ]
            },
            "elastic_pool": {
                "pressure": [{"dimension": "cpu", "metric": "avg_cpu_percent", "level": "critical", "p95_percent": 98.0}]
            },
            "columnstore_health": {"totals": {"index_count": 4, "flagged_index_count": 2}, "worst_indexes": []},
            "failed_executions": {
                "rising": [{"query_id": 42, "query_hash": "0x2A", "severity": "high", "rise_ratio": 6.0, "recent_failed_executions": 30}],
                "summary": {"severity": "high", "rise_ratio": 6.0},
            },
            "locking_state": {"read_committed_snapshot": "off", "optimized_locking": "on"},
            "plan_cache": {"total_plans": 40_000, "total_single_use_plans": 33_000},
            "memory_grants": {"pending_count": 0, "likely_spilling_count": 3},
        }
    )

    assert facts["memory_grant_waiters"].basis == "verified"
    assert facts["forced_memory_grants"].basis == "inferred"
    assert facts["elastic_pool_pressure"].severity == "high"
    assert facts["elastic_pool_pressure"].detail["dimensions"] == ["cpu"]
    assert facts["columnstore_maintenance_debt"].severity == "low"
    assert facts["failed_executions_rising"].detail["query_ids"] == [42]
    assert facts["read_committed_snapshot_off"].severity == "info"
    assert facts["single_use_plan_share"].value == 82.5
    assert facts["likely_spilling"].basis == "inferred"


# --- The service ------------------------------------------------------------


def _service(**overrides: Any) -> DatabaseDiagnosisService:
    resource = AsyncMock()
    resource.get_resource_stats_history = AsyncMock(return_value=_cpu_history(10.0, 20.0))
    resource.get_resource_limits = AsyncMock(return_value=_LIMITS)
    waits = AsyncMock()
    waits.get_wait_stats = AsyncMock(return_value={"top_waits": [], "window": _INTERVAL})
    sessions = AsyncMock()
    sessions.get_active_sessions = AsyncMock(return_value={"blocking_chains": [], "sessions": []})
    store = AsyncMock()
    store.get_top_queries = AsyncMock(return_value=[])
    trends = AsyncMock()
    trends.regressions = AsyncMock(return_value={"regressions": []})
    trends.failed_executions = AsyncMock(return_value={"rising": [], "summary": {"severity": None}})
    pvs = AsyncMock()
    pvs.get_version_store_stats = AsyncMock(return_value={"findings": []})
    services: dict[str, Any] = {
        "resource_governance": resource,
        "wait_stats": waits,
        "sessions": sessions,
        "query_store": store,
        "query_store_trends": trends,
        "version_store": pvs,
    }
    services.update(overrides)
    return DatabaseDiagnosisService(**services)


def _reader(method: str, payload: Any) -> AsyncMock:
    service = AsyncMock()
    setattr(service, method, AsyncMock(return_value=payload))
    return service


@pytest.mark.asyncio
async def test_response_leads_with_the_cause_then_leads_then_facts() -> None:
    service = _service(
        sessions=_reader("get_active_sessions", {"blocking_chains": [_idle_chain(12, 45_000)], "sessions": []}),
        elastic_pool=_reader(
            "get_pool_pressure",
            {"pressure": [{"dimension": "storage", "level": "warning", "used_storage_percent": 84.0}]},
        ),
    )

    result = await service.diagnose("appdb", sample_seconds=15)

    keys = list(result)
    assert keys.index("conclusion") < keys.index("stories") < keys.index("leads") < keys.index("facts")
    (story,) = result["stories"]
    assert story["code"] == "abandoned_transaction"
    assert "abandoned transaction" in result["conclusion"]
    assert "session 61" in story["cause"].lower() and "billing-api" in story["cause"]
    fact_ids = {fact["id"] for fact in result["facts"]}
    assert set(story["supporting_facts"]) <= fact_ids
    assert all(fact["basis"] in {"verified", "inferred"} for fact in result["facts"])
    # The pool storage warning has nothing to do with the blocking: it stays a lead.
    assert [lead["code"] for lead in result["leads"]] == ["elastic_pool_pressure"]
    # The flat list stays for callers that read it, and points at the facts.
    assert {finding["fact_id"] for finding in result["findings"]} <= fact_ids
    assert result["verdict"] == "findings"


@pytest.mark.asyncio
async def test_cpu_bad_actor_needs_the_pressure_and_the_query() -> None:
    busy = AsyncMock()
    busy.get_resource_stats_history = AsyncMock(return_value=_cpu_history(90.0, 100.0, minutes_above_95=22.0))
    busy.get_resource_limits = AsyncMock(return_value=_LIMITS)
    store = AsyncMock()
    store.get_top_queries = AsyncMock(return_value=[_top_query(0.30), _top_query(0.02, query_hash="0x0B")])

    with_query = await _service(resource_governance=busy, query_store=store).diagnose("appdb")
    without_query = await _service(resource_governance=busy).diagnose("appdb")

    story = with_query["stories"][0]
    assert story["code"] == "cpu_bad_actor"
    assert story["next_tools"][0] == {"tool": "analyze_query_plan", "arguments": {"query_id": 9}}
    assert "plan" in story["fix"]
    # CPU pressure alone names no cause.
    assert without_query["stories"] == []
    (lead,) = without_query["leads"]
    assert lead["code"] == "cpu_pressure"
    assert lead["possible_causes"][0]["story"] == "cpu_bad_actor"
    store.get_top_queries.assert_awaited_once_with("appdb", "cpu", 60, 5, group_by="query_hash")


@pytest.mark.asyncio
async def test_an_unreadable_source_is_an_explicit_fact_and_never_an_all_clear() -> None:
    sessions = AsyncMock()
    sessions.get_active_sessions = AsyncMock(side_effect=PermissionError("VIEW DATABASE STATE denied"))
    trends = AsyncMock()
    trends.regressions = AsyncMock(
        return_value={
            "regressions": [],
            "result_status": "precondition",
            "result_status_reason": "Query Store is OFF.",
            "remediation": "ALTER DATABASE CURRENT SET QUERY_STORE = ON;",
        }
    )
    trends.failed_executions = AsyncMock(return_value={"rising": [], "summary": {}})

    result = await _service(sessions=sessions, query_store_trends=trends).diagnose("appdb")

    unread = {fact["code"]: fact for fact in result["facts"] if fact["state"] == "not_observed"}
    assert set(unread) == {"sessions_not_observed", "regressions_not_observed"}
    assert unread["regressions_not_observed"]["detail"]["remediation"].startswith("ALTER DATABASE")
    assert unread["sessions_not_observed"]["severity"] == "unknown"
    assert result["verdict"] == "partial"
    assert "not an all-clear" in result["conclusion"]
    assert result["stories"] == [] and result["leads"] == []


@pytest.mark.asyncio
async def test_every_source_read_and_quiet_is_a_stated_no_findings() -> None:
    result = await _service().diagnose("appdb")

    assert result["verdict"] == "no_findings"
    assert result["findings"] == []
    assert "nothing is above its bar" in result["conclusion"]
    assert not [fact for fact in result["facts"] if fact["state"] == "not_observed"]


@pytest.mark.asyncio
async def test_a_source_that_does_not_apply_is_not_a_gap() -> None:
    pool = _reader("get_pool_pressure", {"pressure": [], "result_status": "not_supported", "result_status_reason": "Not pooled."})

    result = await _service(elastic_pool=pool).diagnose("appdb")

    assert result["sources"]["elastic_pool"] == "not_supported"
    assert result["verdict"] == "no_findings"
    assert result["gaps"] == []


@pytest.mark.asyncio
async def test_optional_sources_are_read_only_when_wired() -> None:
    plain = await _service().diagnose("appdb")
    wired = await _service(
        memory_pressure=_reader("get_memory_pressure", {"findings": []}),
        columnstore_health=_reader("get_columnstore_health", {"totals": {"flagged_index_count": 0}}),
        tempdb_memory=_reader("get_memory_grants", {"pending_count": 0, "likely_spilling_count": 0}),
        plan_cache=_reader("get_plan_cache_analysis", {"total_plans": 10, "total_single_use_plans": 9}),
    ).diagnose("appdb")

    new = {"memory_pressure", "columnstore_health", "memory_grants", "plan_cache"}
    assert not new & set(plain["sources"])
    assert new <= set(wired["sources"])
    assert "failed_executions" in plain["sources"]


@pytest.mark.asyncio
async def test_a_slow_source_times_out_alone() -> None:
    async def never(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        await asyncio.sleep(30)
        return {}

    slow = AsyncMock()
    slow.get_columnstore_health = never

    result = await _service(columnstore_health=slow, source_timeout_seconds=0.05).diagnose("appdb")

    assert result["sources"]["columnstore_health"] == "unavailable"
    assert any(gap.startswith("columnstore_health:") and "timed out" in gap for gap in result["gaps"])
    assert result["sources"]["waits"] == "ok"
    assert result["verdict"] == "partial"


@pytest.mark.asyncio
async def test_the_resource_window_is_reported_as_read_not_as_asked() -> None:
    # Without master a 24-hour request reads only the last hour of resource
    # history, while Query Store covers 24 hours. The fact must say which.
    resource = AsyncMock()
    resource.get_resource_stats_history = AsyncMock(
        return_value={
            **_cpu_history(92.0, 100.0, minutes_above_95=30.0),
            "result_status": "precondition",
            "result_status_reason": "A 1440-minute window needs sys.resource_stats.",
            "remediation": "Add master to AZURE_SQL_ALLOWED_DATABASES.",
        }
    )
    resource.get_resource_limits = AsyncMock(return_value=_LIMITS)

    result = await _service(resource_governance=resource).diagnose("appdb", window_minutes=1440)

    cpu = next(fact for fact in result["facts"] if fact["code"] == "cpu_pressure")
    assert cpu["severity"] == "high"
    assert cpu["window"].startswith("last 60 min")
    assert result["context"]["windows"]["resource_history_minutes"] == 60
    assert result["context"]["windows"]["query_store_minutes"] == 1440
    assert any(gap.startswith("resource_history:") for gap in result["gaps"])


@pytest.mark.asyncio
async def test_the_response_stays_bounded() -> None:
    chains = [
        {
            "head_blocker_session_id": head,
            "head_blocker_state": "running",
            "blocked_session_ids": [head + 1],
            "total_blocked_count": 1,
            "chain_depth": 1,
            "max_wait_time_ms": 100,
        }
        for head in range(200, 400, 2)
    ]
    pool = _reader(
        "get_pool_pressure",
        {"pressure": [{"dimension": f"d{index}", "level": "warning", "p95_percent": 81.0} for index in range(60)]},
    )

    result = await _service(
        sessions=_reader("get_active_sessions", {"blocking_chains": chains, "sessions": []}), elastic_pool=pool
    ).diagnose("appdb")

    assert len(result["facts"]) <= MAX_FACTS
    assert result["omitted"]["facts"] > 0
    assert len(result["leads"]) <= 8
    assert len(json.dumps(result)) < 40_000


@pytest.mark.asyncio
async def test_baseline_says_whether_the_level_is_new_for_this_hour() -> None:
    busy = AsyncMock()
    busy.get_resource_stats_history = AsyncMock(return_value=_cpu_history(90.0, 100.0, minutes_above_95=22.0))
    busy.get_resource_limits = AsyncMock(return_value=_LIMITS)
    store = AsyncMock()
    store.get_top_queries = AsyncMock(return_value=[_top_query(0.30)])
    baseline = AsyncMock()
    baseline.resource_bands = AsyncMock(
        return_value={
            "baseline_status": "ok",
            "metrics": {
                "avg_cpu_percent": {
                    "judgement": "abnormal",
                    "peak": 100.0,
                    "z_peak": 21.0,
                    "band_kind": "hour_of_day",
                    "band": {"median": 22.0, "upper": 25.0},
                },
                "avg_data_io_percent": {"judgement": "normal"},
            },
        }
    )

    without = await _service(resource_governance=busy, query_store=store).diagnose("appdb", master_available=True)
    with_band = await _service(resource_governance=busy, query_store=store, baseline=baseline).diagnose(
        "appdb", master_available=True
    )

    codes = {fact["code"]: fact for fact in with_band["facts"]}
    assert codes["cpu_abnormal_for_hour"]["basis"] == "inferred"
    assert codes["cpu_abnormal_for_hour"]["bar"]["median"] == 22.0
    # The same incident is more certain once the level is known to be new for the hour.
    assert with_band["stories"][0]["confidence"] > without["stories"][0]["confidence"]
    baseline.resource_bands.assert_awaited_once_with("appdb", window_minutes=60, master_available=True)


@pytest.mark.asyncio
async def test_a_missing_baseline_is_reported_and_does_not_hide_a_clean_result() -> None:
    baseline = AsyncMock()
    baseline.resource_bands = AsyncMock(
        return_value={
            "metrics": {},
            "result_status": "precondition",
            "result_status_reason": "A baseline needs sys.resource_stats, which is read from master.",
            "remediation": "Add master to AZURE_SQL_ALLOWED_DATABASES.",
        }
    )

    result = await _service(baseline=baseline).diagnose("appdb")

    (unread,) = [fact for fact in result["facts"] if fact["state"] == "not_observed"]
    assert unread["code"] == "baseline_not_observed"
    assert any(gap.startswith("baseline:") for gap in result["gaps"])
    # The baseline is context: no pressure can hide behind it.
    assert result["verdict"] == "no_findings"
