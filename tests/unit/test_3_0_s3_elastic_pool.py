"""Elastic pool pressure is read from inside the pooled database.

A pooled database can sit at 20% of its own cap while the pool is pegged by a
neighbour. Before this collector nothing read the pool, so the diagnosis said
"no pressure" during a pool-wide stall.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from typing import Any

import pytest

from azure_sql_mcp.elastic_pool import ElasticPoolService
from tests.azure_dmv_contract import StrictDmvExecutor

POOL = "sys.dm_elastic_pool_resource_stats"
POOLED = [{"edition": "GeneralPurpose", "service_objective": "ElasticPool", "elastic_pool_name": "pool-a"}]
SINGLE = [{"edition": "GeneralPurpose", "service_objective": "GP_Gen5_4", "elastic_pool_name": None}]
END = datetime(2026, 11, 10, 12, 0, 0)


def pool_rows(count: int = 120, **overrides: Any) -> list[dict[str, Any]]:
    """Newest first, one sample every 15 seconds."""

    rows = []
    for index in range(count):
        row: dict[str, Any] = {
            "end_time": END - timedelta(seconds=15 * index),
            "avg_cpu_percent": 20.0,
            "avg_data_io_percent": 5.0,
            "avg_log_write_percent": 3.0,
            "max_worker_percent": 4.0,
            "max_session_percent": 1.0,
            "avg_instance_cpu_percent": 25.0,
            "avg_instance_memory_percent": 99.0,
            "avg_edtu_percent": None,
            "instance_vcores": 8,
            "used_storage_mb": 409_600,
            "allocated_storage_mb": 450_000,
            "storage_limit_mb": 1_048_576,
            "used_storage_percent": 39.06,
            "allocated_storage_percent": 42.92,
        }
        row.update(overrides)
        rows.append(row)
    return rows


def executor_for(rows: Any, tier: Any = POOLED) -> StrictDmvExecutor:
    return StrictDmvExecutor(responses=[("'Edition'", tier), (POOL, rows)])


@pytest.mark.asyncio
async def test_pegged_pool_cpu_is_reported_against_the_pool_limit() -> None:
    executor = executor_for(pool_rows(avg_cpu_percent=98.0))

    result = await ElasticPoolService(executor).get_pool_pressure("appdb")  # type: ignore[arg-type]

    assert executor.violations == []
    assert result["result_status"] == "ok"
    assert result["elastic_pool_name"] == "pool-a"
    assert result["source_dmv"] == POOL
    cpu = result["usage_percent_of_pool_limit"]["avg_cpu_percent"]
    assert cpu["avg"] == 98.0 and cpu["p95"] == 98.0
    assert cpu["minutes_above_95"] == 30.0
    assert result["pressure"] == [
        {"dimension": "cpu", "metric": "avg_cpu_percent", "level": "critical", "p95_percent": 98.0, "max_percent": 98.0, "minutes_above_95": 30.0}
    ]
    assert result["limits"] == {"instance_vcores": 8, "storage_limit_mb": 1_048_576}
    window = result["collection_window"]
    assert window["sample_count"] == 120 and window["sample_seconds"] == 15
    assert window["start_utc"] == "2026-11-10T11:30:15" and window["end_utc"] == "2026-11-10T12:00:00"


@pytest.mark.parametrize(
    ("column", "dimension"),
    [
        ("avg_data_io_percent", "data_io"),
        ("avg_log_write_percent", "log_write"),
        ("max_worker_percent", "workers"),
        ("max_session_percent", "sessions"),
    ],
)
@pytest.mark.asyncio
async def test_each_governed_dimension_is_compared_with_the_pool_limit(column: str, dimension: str) -> None:
    result = await ElasticPoolService(executor_for(pool_rows(**{column: 85.0}))).get_pool_pressure("appdb")  # type: ignore[arg-type]

    (pressure,) = result["pressure"]
    assert (pressure["dimension"], pressure["level"]) == (dimension, "warning")


@pytest.mark.asyncio
async def test_quiet_pool_with_full_memory_has_no_pressure() -> None:
    # Instance memory at 99% is the buffer pool, not pressure.
    result = await ElasticPoolService(executor_for(pool_rows())).get_pool_pressure("appdb")  # type: ignore[arg-type]

    assert result["pressure"] == []
    assert result["instance"]["avg_instance_memory_percent"]["avg"] == 99.0
    assert result["instance"]["memory_note"].startswith("Information only.")


@pytest.mark.parametrize(("used_percent", "level"), [(79.0, None), (85.0, "warning"), (93.0, "critical")])
@pytest.mark.asyncio
async def test_pool_storage_is_compared_with_the_pool_storage_limit(used_percent: float, level: str | None) -> None:
    result = await ElasticPoolService(executor_for(pool_rows(used_storage_percent=used_percent))).get_pool_pressure("appdb")  # type: ignore[arg-type]

    assert result["storage"]["used_storage_percent"] == used_percent
    assert result["storage"]["storage_limit_mb"] == 1_048_576
    levels = [item["level"] for item in result["pressure"] if item["dimension"] == "storage"]
    assert levels == ([level] if level else [])


@pytest.mark.asyncio
async def test_a_single_database_is_not_supported_and_the_pool_view_is_not_read() -> None:
    executor = executor_for(pool_rows(), tier=SINGLE)

    result = await ElasticPoolService(executor).get_pool_pressure("appdb")  # type: ignore[arg-type]

    assert result["result_status"] == "not_supported"
    assert "not in an elastic pool" in result["result_status_reason"]
    assert not any(POOL in query for _, query, _ in executor.calls)


@pytest.mark.asyncio
async def test_refused_pool_read_states_the_permission_path() -> None:
    # The pool view needs VIEW SERVER STATE on every tier: no database grant opens it.
    executor = executor_for(RuntimeError("VIEW SERVER STATE permission was denied on object 'server'. (300)"))

    result = await ElasticPoolService(executor).get_pool_pressure("appdb")  # type: ignore[arg-type]

    assert result["result_status"] == "unavailable"
    assert result["pressure"] == []
    assert "VIEW SERVER STATE" in result["result_status_reason"]
    assert "ALTER SERVER ROLE ##MS_ServerStateReader##" in result["remediation"]
    assert "GRANT VIEW DATABASE STATE" not in result["remediation"]
    assert "##MS_ServerStateReader##" in result["permission_path"]


@pytest.mark.asyncio
async def test_pooled_database_with_no_samples_is_empty() -> None:
    result = await ElasticPoolService(executor_for([])).get_pool_pressure("appdb")  # type: ignore[arg-type]

    assert result["result_status"] == "empty"
    assert result["pressure"] == []


@pytest.mark.asyncio
async def test_window_is_capped_at_the_view_retention() -> None:
    executor = executor_for(pool_rows())

    result = await ElasticPoolService(executor).get_pool_pressure("appdb", window_minutes=600)  # type: ignore[arg-type]

    pool_call = next(call for call in executor.calls if POOL in call[1])
    assert pool_call[2] == (40,)
    assert result["collection_window"]["window_minutes"] == 40
