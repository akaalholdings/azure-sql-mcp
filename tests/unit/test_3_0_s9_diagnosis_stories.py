"""Stories: a cause needs corroboration, a lone fact is a lead.

A DBA acts on a cause. A wrong cause sends them to fix the wrong thing during an
incident, so every story here has a fixture that makes it fire and a near-miss
with one corroborating fact removed, which must stay a lead.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.diagnosis_stories import CONFIDENCE_CEILING
from azure_sql_mcp.diagnosis_stories import MAX_LEADS
from azure_sql_mcp.diagnosis_stories import RULES
from azure_sql_mcp.diagnosis_stories import Fact
from azure_sql_mcp.diagnosis_stories import build_stories


def _fact(code: str, source: str, severity: str = "medium", **fields: Any) -> Fact:
    return Fact(code=code, source=source, database=fields.pop("database", "appdb"), severity=severity, **fields)


def _numbered(facts: list[Fact]) -> list[Fact]:
    for index, fact in enumerate(facts, start=1):
        fact.id = f"F{index}"
    return facts


# story code -> (facts that make it fire, code of the corroborating fact to remove)
STORY_FIXTURES: dict[str, tuple[list[Fact], str]] = {
    "abandoned_transaction": (
        [
            _fact(
                "idle_head_blocker",
                "sessions",
                detail={"session_id": 61, "program_name": "billing-api", "idle_seconds": 840, "blocked": 12},
            ),
            _fact("long_lock_wait", "sessions", value=45_000, unit="ms"),
            _fact("active_blocking", "sessions", detail={"blocked": 12, "head": 61}),
        ],
        "long_lock_wait",
    ),
    "memory_pressure_driving_io": (
        [
            _fact("io_latch_waits", "waits", detail={"dimensions": ["data_io"]}),
            _fact("memory_grant_waiters", "memory_pressure", value=3, unit="queries"),
            _fact("likely_spilling", "memory_grants", "low", basis="inferred"),
        ],
        "memory_grant_waiters",
    ),
    "log_rate_throttling": (
        [
            _fact("log_rate_governor_waits", "waits", "high", detail={"dimensions": ["log_write"]}),
            _fact("log_write_pressure", "resource_history", "high", value=99.0, unit="percent_of_limit"),
            _fact(
                "dominant_writer",
                "sessions",
                "info",
                detail={"session_id": 77, "command": "ALTER INDEX", "share_pct": 92.0},
            ),
        ],
        "log_write_pressure",
    ),
    "parallelism_exhausting_workers": (
        [
            _fact("threadpool_waits", "waits", detail={"dimensions": ["workers"]}),
            _fact("parallelism_waits", "waits"),
            _fact("worker_pressure", "resource_history", "high", value=97.0, unit="percent_of_limit"),
        ],
        "parallelism_waits",
    ),
    "blocking_holding_workers": (
        [
            _fact("threadpool_waits", "waits", detail={"dimensions": ["workers"]}),
            _fact("active_blocking", "sessions", "high", detail={"blocked": 40, "head": 51}),
        ],
        "active_blocking",
    ),
    "cpu_bad_actor": (
        [
            _fact("cpu_pressure", "resource_history", "high", value=96.0, unit="percent_of_limit"),
            _fact(
                "dominant_query",
                "top_queries",
                detail={"query_id": 9, "query_hash": "0x0A", "share_of_cpu_capacity_pct": 41.0},
            ),
            _fact("scheduler_waits", "waits", detail={"dimensions": ["cpu"]}),
        ],
        "dominant_query",
    ),
    "timeouts_from_regression": (
        [
            _fact(
                "failed_executions_rising",
                "failed_executions",
                "high",
                value=6.0,
                unit="times_baseline_rate",
                detail={"query_ids": [42, 43]},
            ),
            _fact(
                "query_regression",
                "regressions",
                "high",
                detail={"query_ids": [42], "query_id": 42, "regression_pct": 310.0},
            ),
        ],
        "query_regression",
    ),
    "plan_cache_churn": (
        [
            _fact("compile_waits", "waits"),
            _fact("single_use_plan_share", "plan_cache", value=83.0, unit="percent_of_cached_plans"),
        ],
        "single_use_plan_share",
    ),
    "readers_blocked_by_writers": (
        [
            _fact("reader_victims_majority", "sessions", value=80.0, unit="percent_of_blocked_sessions"),
            _fact("read_committed_snapshot_off", "locking_state", "info"),
        ],
        "read_committed_snapshot_off",
    ),
    "pool_limit_reached": (
        [
            _fact(
                "elastic_pool_pressure",
                "elastic_pool",
                "high",
                value=98.0,
                unit="percent_of_pool_limit",
                detail={"dimensions": ["cpu"], "dimension": "cpu"},
            ),
            _fact("scheduler_waits", "waits", detail={"dimensions": ["cpu"]}),
        ],
        "scheduler_waits",
    ),
}


def test_every_rule_has_a_fire_and_a_near_miss_fixture() -> None:
    assert {rule.code for rule in RULES} == set(STORY_FIXTURES)


def test_every_rule_needs_two_different_facts() -> None:
    # One fact alone is a lead. A rule with a single required check would turn
    # any lone symptom into a cause.
    for rule in RULES:
        required = [check for check in rule.checks if check.required]
        assert len(required) >= 2, rule.code
        assert rule.checks[0].required, rule.code


@pytest.mark.parametrize("code", sorted(STORY_FIXTURES))
def test_story_fires_when_its_facts_corroborate(code: str) -> None:
    facts, _ = STORY_FIXTURES[code]

    stories, leads = build_stories(_numbered([Fact(**vars(fact)) for fact in facts]))

    story = next(item for item in stories if item["code"] == code)
    assert len(story["supporting_facts"]) >= 2
    assert story["cause"].endswith(".")
    assert 0.2 < story["confidence"] <= CONFIDENCE_CEILING
    assert story["next_tools"] and all("tool" in step for step in story["next_tools"])
    # A supporting fact is not also reported as an unexplained lead.
    assert not {lead["fact_id"] for lead in leads} & set(story["supporting_facts"])


@pytest.mark.parametrize("code", sorted(STORY_FIXTURES))
def test_story_stays_a_lead_when_one_corroborating_fact_is_missing(code: str) -> None:
    facts, removed = STORY_FIXTURES[code]
    kept = _numbered([Fact(**vars(fact)) for fact in facts if fact.code != removed])

    stories, leads = build_stories(kept)

    assert code not in {story["code"] for story in stories}
    symptom = kept[0]
    lead = next(item for item in leads if item["fact_id"] == symptom.id)
    # A THREADPOOL lead can become one of two causes: each is named with what it lacks.
    possible = {item["story"]: item for item in lead["possible_causes"]}
    assert possible[code]["missing"]


def test_confidence_grows_with_corroboration_and_never_reaches_one() -> None:
    facts, _ = STORY_FIXTURES["abandoned_transaction"]
    full = [Fact(**vars(fact)) for fact in facts] + [
        _fact("version_cleanup_held_by_long_transaction", "version_store"),
        _fact("lock_waits", "waits"),
    ]
    two_facts = [Fact(**vars(fact)) for fact in facts[:2]]

    (strong,), _ = build_stories(_numbered(full))
    (weak,), _ = build_stories(_numbered(two_facts))

    assert weak["confidence"] < strong["confidence"] <= CONFIDENCE_CEILING < 1.0


def test_an_inferred_fact_counts_for_less_than_a_verified_one() -> None:
    # Cumulative counters say a wait happened, not that it is happening now.
    def facts(basis: str) -> list[Fact]:
        return _numbered(
            [
                _fact("compile_waits", "waits", basis=basis),
                _fact("single_use_plan_share", "plan_cache"),
            ]
        )

    (verified,), _ = build_stories(facts("verified"))
    (inferred,), _ = build_stories(facts("inferred"))

    assert inferred["confidence"] < verified["confidence"]
    assert any("sample_seconds" in hint for hint in inferred["raise_confidence"])


def test_story_names_what_was_checked_and_what_could_not_be_read() -> None:
    facts, _ = STORY_FIXTURES["cpu_bad_actor"]
    kept = _numbered([Fact(**vars(fact)) for fact in facts if fact.code != "scheduler_waits"])

    (read,), _ = build_stories(kept)
    (unread,), _ = build_stories(kept, unread_sources={"waits"})

    # Waits were read and showed no CPU queue: say so. Unread waits are not evidence against.
    assert any("queue for CPU" in item["check"] for item in read["checked_not_supporting"])
    assert not any("queue for CPU" in item["check"] for item in unread["checked_not_supporting"])
    assert any("waits" in hint for hint in unread["raise_confidence"])


def test_timeouts_need_the_same_query_to_have_regressed() -> None:
    facts = _numbered(
        [
            _fact("failed_executions_rising", "failed_executions", "high", detail={"query_ids": [42]}),
            _fact("query_regression", "regressions", "high", detail={"query_ids": [7], "query_id": 7}),
        ]
    )

    stories, leads = build_stories(facts)

    assert stories == []
    assert {lead["fact_id"] for lead in leads} == {"F1", "F2"}


def test_pool_story_needs_the_wait_on_the_same_resource() -> None:
    facts = _numbered(
        [
            _fact("elastic_pool_pressure", "elastic_pool", "high", detail={"dimensions": ["log_write"]}),
            _fact("scheduler_waits", "waits", detail={"dimensions": ["cpu"]}),
        ]
    )

    stories, _ = build_stories(facts)

    assert stories == []


def test_an_incident_never_spans_two_databases() -> None:
    facts = _numbered(
        [
            _fact("cpu_pressure", "resource_history", "high", database="sales"),
            _fact("dominant_query", "top_queries", database="billing", detail={"query_id": 9}),
        ]
    )

    stories, leads = build_stories(facts)

    assert stories == []
    assert len(leads) == 2


def test_info_and_unread_facts_are_not_leads() -> None:
    facts = _numbered(
        [
            _fact("top_wait", "waits", "info"),
            _fact("sessions_not_observed", "sessions", "unknown", state="not_observed"),
        ]
    )

    assert build_stories(facts) == ([], [])


def test_leads_are_bounded_and_keep_the_most_severe() -> None:
    facts = _numbered(
        [_fact(f"columnstore_maintenance_debt_{index}", "columnstore_health", "low") for index in range(12)]
        + [_fact("cpu_pressure", "resource_history", "high")]
    )

    _, leads = build_stories(facts)

    assert len(leads) == MAX_LEADS
    assert leads[0]["code"] == "cpu_pressure"
