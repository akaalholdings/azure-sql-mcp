"""Capacity verdict: under-provisioned, over-provisioned or right-sized, with the evidence.

A client asks "should we scale?". The wrong answer costs money both ways:
scaling up a database whose limit is the log rate buys nothing, and scaling
down on three quiet days breaks month-end. The verdict names the binding
dimension, needs enough samples, and never quotes a price.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from datetime import timedelta
from typing import Any

import pytest

from azure_sql_mcp.azure_tier import classify_service_tier
from azure_sql_mcp.capacity_assessment import MIN_SAMPLES_FOR_VERDICT
from azure_sql_mcp.capacity_assessment import CapacityAssessmentService
from azure_sql_mcp.capacity_assessment import assess_capacity
from azure_sql_mcp.capacity_assessment import detect_pauses
from azure_sql_mcp.capacity_assessment import storage_headroom
from tests.azure_dmv_contract import StrictDmvExecutor

START = datetime(2026, 10, 1, 0, 0, 0)
PROVISIONED = classify_service_tier("GeneralPurpose", "GP_Gen5_8", None)
SERVERLESS = classify_service_tier("GeneralPurpose", "GP_S_Gen5_4", None)


def _rows(
    days: float,
    *,
    cpu: float | Callable[[int], float] = 20.0,
    io: float = 10.0,
    log: float | Callable[[int], float] = 5.0,
    workers: float = 3.0,
    storage_mb: Callable[[int], float] | None = None,
    skip: Callable[[int], bool] | None = None,
    sku: str | Callable[[int], str] = "GP_Gen5_8",
    cpu_limit: float | None = 8,
    dtu_limit: int | None = None,
) -> list[dict[str, Any]]:
    def pick(value: Any, index: int) -> Any:
        return value(index) if callable(value) else value

    rows = []
    for index in range(int(days * 288)):
        if skip is not None and skip(index):
            continue
        rows.append(
            {
                "end_time": START + timedelta(minutes=5 * (index + 1)),
                "sku": pick(sku, index),
                "cpu_limit": cpu_limit,
                "dtu_limit": dtu_limit,
                "avg_cpu_percent": pick(cpu, index),
                "avg_data_io_percent": io,
                "avg_log_write_percent": pick(log, index),
                "max_worker_percent": workers,
                "max_session_percent": 2.0,
                "storage_in_megabytes": storage_mb(index) if storage_mb else 50_000.0,
            }
        )
    return rows


def _waiting(waiters: int = 0, timeouts: int = 0, granted_mb: float = 100.0, available_mb: float = 900.0) -> dict[str, Any]:
    return {
        "resource_semaphores": {
            "result_status": "ok",
            "totals": {
                "waiter_count": waiters,
                "timeout_error_count_since_engine_start": timeouts,
                "forced_grant_count_since_engine_start": 0,
                "granted_memory_mb": granted_mb,
                "available_memory_mb": available_mb,
            },
        }
    }


def test_sustained_cpu_is_under_provisioned_and_names_cpu() -> None:
    busy_hours = lambda index: 96.0 if index % 288 < 60 else 35.0  # noqa: E731 - five hours a day at the limit

    result = assess_capacity(_rows(14, cpu=busy_hours), database_name="appdb", tier=PROVISIONED, memory=_waiting())

    assert result["verdict"] == "under_provisioned"
    assert result["binding_dimension"] == "cpu"
    assert result["dimensions"]["cpu"]["p95"] == 96.0
    assert "8 vCores" in result["tier_change"]


def test_log_bound_database_is_not_told_to_add_cpu() -> None:
    # Log writes at the cap for eight hours a day while CPU sits at 40%.
    capped = lambda index: 100.0 if index % 288 < 96 else 10.0  # noqa: E731

    result = assess_capacity(_rows(14, cpu=40.0, log=capped), database_name="appdb", tier=PROVISIONED, memory=_waiting())

    assert result["verdict"] == "under_provisioned"
    assert result["binding_dimension"] == "log_write"
    assert "log rate" in result["tier_change"]
    assert result["dimensions"]["cpu"]["p95"] == 40.0


def test_idle_database_is_over_provisioned_only_with_a_week_of_history() -> None:
    idle = {"cpu": 2.0, "io": 1.0, "log": 1.0}

    two_weeks = assess_capacity(_rows(14, **idle), database_name="appdb", tier=PROVISIONED, memory=_waiting())
    three_days = assess_capacity(_rows(3, **idle), database_name="appdb", tier=PROVISIONED, memory=_waiting())

    assert two_weeks["verdict"] == "over_provisioned"
    assert "smaller" in two_weeks["tier_change"]
    # Three quiet days do not cover a business cycle.
    assert three_days["verdict"] == "insufficient_evidence"
    assert three_days["missing"] == {"days_covered": 3.0, "days_needed": 7}


def test_a_grant_waiter_blocks_a_scale_down_and_reads_as_under_provisioned() -> None:
    result = assess_capacity(_rows(14, cpu=3.0), database_name="appdb", tier=PROVISIONED, memory=_waiting(waiters=2))

    assert result["verdict"] == "under_provisioned"
    assert result["binding_dimension"] == "memory_grants"


def test_heavy_grant_use_keeps_an_idle_cpu_database_right_sized() -> None:
    result = assess_capacity(
        _rows(14, cpu=3.0), database_name="appdb", tier=PROVISIONED, memory=_waiting(granted_mb=800.0, available_mb=200.0)
    )

    assert result["verdict"] == "right_sized"


def test_unobserved_memory_is_a_stated_caveat_on_a_scale_down() -> None:
    result = assess_capacity(_rows(14, cpu=2.0), database_name="appdb", tier=PROVISIONED, memory=None)

    assert result["verdict"] == "over_provisioned"
    assert "memory_grants" in result["not_observed"]
    assert any("memory grant" in caveat.lower() for caveat in result["caveats"])


def test_too_few_samples_give_no_verdict() -> None:
    rows = _rows(1, cpu=99.0)[: MIN_SAMPLES_FOR_VERDICT - 1]

    result = assess_capacity(rows, database_name="appdb", tier=PROVISIONED, memory=_waiting())

    assert result["verdict"] == "insufficient_evidence"
    assert result["missing"] == {"samples": MIN_SAMPLES_FOR_VERDICT - 1, "samples_needed": MIN_SAMPLES_FOR_VERDICT}


def test_only_the_current_service_objective_is_judged() -> None:
    # Pegged on 4 vCores for a week, then scaled to 8 and calm since.
    half = 7 * 288
    rows = _rows(
        14,
        cpu=lambda index: 97.0 if index < half else 30.0,
        sku=lambda index: "GP_Gen5_4" if index < half else "GP_Gen5_8",
    )

    result = assess_capacity(rows, database_name="appdb", tier=PROVISIONED, memory=_waiting())

    assert result["verdict"] == "right_sized"
    assert result["dimensions"]["cpu"]["max"] == 30.0
    assert [(event["from"], event["to"]) for event in result["scale_events"]] == [("GP_Gen5_4", "GP_Gen5_8")]


def test_serverless_reports_auto_pause_and_is_not_called_over_provisioned() -> None:
    # Active one hour in four: the gaps are pauses.
    rows = _rows(14, cpu=4.0, sku="GP_S_Gen5_4", cpu_limit=4, skip=lambda index: index % 48 >= 12)

    result = assess_capacity(
        rows, database_name="appdb", tier=SERVERLESS, memory=_waiting(), recent_memory_percent=22.0
    )

    assert result["verdict"] == "right_sized"
    serverless = result["serverless"]
    assert serverless["pause_count"] >= 80
    assert serverless["paused_share_pct"] == pytest.approx(75.0, abs=1.0)
    assert serverless["memory_reclaim"] == "likely"
    assert "auto-pause" in result["tier_change"]


def test_detect_pauses_ignores_one_missing_sample() -> None:
    rows = _rows(1, skip=lambda index: index == 100)

    assert detect_pauses(rows)["pause_count"] == 0


def test_storage_growth_gives_days_to_the_maximum_size() -> None:
    # 2 GB a day; 40 GB of headroom at the end of the window.
    rows = _rows(10, storage_mb=lambda index: 40_000.0 + 2_048.0 * index / 288)
    used_now = rows[-1]["storage_in_megabytes"]

    headroom = storage_headroom(rows, max_size_mb=used_now + 40_960.0)

    assert headroom["growth_mb_per_day"] == pytest.approx(2_048.0, rel=0.01)
    assert headroom["days_to_max"] == pytest.approx(20.0, abs=0.5)


def test_storage_near_the_maximum_is_the_binding_dimension() -> None:
    rows = _rows(14, cpu=30.0, storage_mb=lambda _index: 95_000.0)

    result = assess_capacity(rows, database_name="appdb", tier=PROVISIONED, memory=_waiting(), max_size_mb=100_000.0)

    assert result["verdict"] == "under_provisioned"
    assert result["binding_dimension"] == "storage"
    assert "maximum size" in result["tier_change"]


def test_dtu_database_states_its_limit_in_dtus() -> None:
    rows = _rows(14, cpu=93.0, sku="S3", cpu_limit=None, dtu_limit=100)

    result = assess_capacity(
        rows, database_name="appdb", tier=classify_service_tier("Standard", "S3", None), memory=_waiting()
    )

    assert result["limits"] == {"service_objective": "S3", "dtu_limit": 100}
    assert "100 DTUs" in result["tier_change"]


def test_the_verdict_never_quotes_a_price() -> None:
    result = assess_capacity(_rows(14, cpu=2.0), database_name="appdb", tier=PROVISIONED, memory=_waiting())
    text = json.dumps({key: value for key, value in result.items() if key != "pricing"}).lower()

    assert "prices are not read" in result["pricing"].lower()
    assert not any(word in text for word in ("$", "usd", "per month", "price", "cheaper"))


@pytest.mark.asyncio
async def test_master_gets_no_verdict() -> None:
    result = await CapacityAssessmentService(StrictDmvExecutor()).assess("master", master_available=True)

    assert result["verdict"] is None
    assert result["result_status"] == "not_supported"


@pytest.mark.asyncio
async def test_without_master_the_history_is_a_precondition_with_the_fix() -> None:
    result = await CapacityAssessmentService(StrictDmvExecutor()).assess("appdb", master_available=False)

    assert result["verdict"] is None
    assert result["result_status"] == "precondition"
    assert "AZURE_SQL_ALLOWED_DATABASES" in result["remediation"]


@pytest.mark.asyncio
async def test_service_reads_only_documented_columns_and_judges_the_rows() -> None:
    executor = StrictDmvExecutor(
        rows={"sys.resource_stats": _rows(8, cpu=92.0)},
        responses=[
            ("'ServiceObjective'", [{"edition": "GeneralPurpose", "service_objective": "GP_Gen5_8", "elastic_pool_name": None}]),
            ("MaxSizeInBytes", [{"max_size_bytes": 500 * 1024 * 1024 * 1024}]),
        ],
    )

    result = await CapacityAssessmentService(executor).assess("appdb", master_available=True)

    assert executor.violations == []
    assert result["verdict"] == "under_provisioned"
    assert result["storage"]["max_size_mb"] == 512_000.0
    assert "memory_grants" in result["not_observed"]
    history_calls = [call for call in executor.calls if "sys.resource_stats" in call[1]]
    assert [(call[0], call[2]) for call in history_calls] == [("master", ("appdb", 14 * 24 * 60))]


@pytest.mark.asyncio
async def test_pooled_database_is_sent_to_the_pool() -> None:
    executor = StrictDmvExecutor(
        rows={"sys.resource_stats": _rows(8, cpu=92.0, sku="ElasticPool")},
        responses=[
            ("'ServiceObjective'", [{"edition": "Standard", "service_objective": "ElasticPool", "elastic_pool_name": "pool-a"}]),
        ],
    )

    result = await CapacityAssessmentService(executor).assess("appdb", master_available=True)

    assert result["verdict"] == "pool_scoped"
    assert result["tier_change"] is None
    assert result["dimensions"]["cpu"]["p95"] == 92.0
