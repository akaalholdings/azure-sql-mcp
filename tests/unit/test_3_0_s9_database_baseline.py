"""Baselines computed on demand from the history Azure keeps.

"CPU is at 90%" means nothing until it is compared with the same hour on other
days: a nightly batch at 90% is normal, the same level at 14:00 is an incident.
The band is robust (median and MAD), an anomaly needs both the peak and the
mean to move, and a short history is reported as insufficient, never as normal.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from datetime import timedelta
from typing import Any

import pytest

from azure_sql_mcp.database_baseline import FLAT_MIN_SAMPLES
from azure_sql_mcp.database_baseline import QUERY_HISTORY_SQL
from azure_sql_mcp.database_baseline import DatabaseBaselineService
from azure_sql_mcp.database_baseline import judge_window
from azure_sql_mcp.database_baseline import modified_z
from azure_sql_mcp.database_baseline import pick_band
from tests.azure_dmv_contract import StrictDmvExecutor

END = datetime(2026, 10, 15, 15, 0, 0)  # a Thursday


def _samples(days: float, value: Callable[[datetime], float], *, end: datetime = END) -> list[tuple[datetime, float]]:
    count = int(days * 288)
    return [(stamp, value(stamp)) for stamp in (end - timedelta(minutes=5 * index) for index in range(count - 1, -1, -1))]


def _usual(stamp: datetime) -> float:
    """A nightly batch at 02:00 and a quiet day, with a little noise."""

    noise = 2.0 if stamp.minute % 10 else -2.0
    return (90.0 if stamp.hour == 2 else 20.0) + noise


def _split(samples: list[tuple[datetime, float]], window_start: datetime) -> tuple[list[Any], list[Any]]:
    return (
        [item for item in samples if item[0] <= window_start],
        [item for item in samples if item[0] > window_start],
    )


def test_the_nightly_batch_is_normal_at_night_and_abnormal_in_the_afternoon() -> None:
    night_end = datetime(2026, 10, 15, 3, 0, 0)
    night = _samples(14, _usual, end=night_end)
    afternoon = _samples(14, lambda stamp: 90.0 if stamp > END - timedelta(hours=1) else _usual(stamp))

    at_night = judge_window(*_split(night, night_end - timedelta(hours=1)))
    in_the_afternoon = judge_window(*_split(afternoon, END - timedelta(hours=1)))

    assert at_night["judgement"] == "normal"
    assert at_night["band_kind"] == "hour_of_day"
    assert at_night["band"]["median"] == pytest.approx(90.0, abs=2.0)
    assert in_the_afternoon["judgement"] == "abnormal"
    assert in_the_afternoon["band"]["median"] == pytest.approx(20.0, abs=2.0)
    assert in_the_afternoon["z_peak"] >= 3.5 and in_the_afternoon["z_mean"] >= 3.5


def test_one_spike_is_not_an_anomaly_because_the_mean_did_not_move() -> None:
    spike_at = END - timedelta(minutes=30)
    samples = _samples(14, lambda stamp: 96.0 if stamp == spike_at else _usual(stamp))

    judged = judge_window(*_split(samples, END - timedelta(hours=1)))

    assert judged["peak"] == 96.0
    assert judged["judgement"] != "abnormal"


def test_a_large_z_score_on_a_tiny_move_is_normal() -> None:
    # 1% to 4% is twenty robust sigmas and three points: nobody is paged for it.
    samples = _samples(14, lambda stamp: 4.0 if stamp > END - timedelta(hours=1) else 1.0 + (0.1 if stamp.minute % 10 else 0.0))

    judged = judge_window(*_split(samples, END - timedelta(hours=1)))

    assert judged["z_mean"] >= 3.5
    assert judged["judgement"] == "normal"


def test_a_sustained_move_short_of_an_anomaly_is_high() -> None:
    # Seven points above the usual level: clear of the band, short of the 10-point floor.
    samples = _samples(14, lambda stamp: 27.0 if stamp > END - timedelta(hours=1) else _usual(stamp))

    judged = judge_window(*_split(samples, END - timedelta(hours=1)))

    assert judged["peak"] - judged["band"]["median"] < 10.0
    assert judged["judgement"] == "high"


def test_a_thin_baseline_needs_a_stricter_absolute_bar() -> None:
    def run(level: float) -> dict[str, Any]:
        samples = _samples(2, lambda stamp: level if stamp > END - timedelta(hours=1) else _usual(stamp))
        return judge_window(*_split(samples, END - timedelta(hours=1)))

    assert run(60.0)["band_kind"] == "flat"
    # Two days cannot say what is usual at 14:00, so 60% is not called an anomaly.
    assert run(60.0)["judgement"] == "high"
    assert run(92.0)["judgement"] == "abnormal"


def test_too_little_history_is_insufficient_and_says_how_much_is_missing() -> None:
    samples = _samples(2 / 24, lambda _stamp: 95.0)  # two hours in total

    judged = judge_window(*_split(samples, END - timedelta(hours=1)))

    assert judged["judgement"] == "insufficient_history"
    assert judged["missing"] == {"baseline_samples": 12, "samples_needed": FLAT_MIN_SAMPLES}


def test_hour_of_week_is_used_only_when_three_weeks_exist() -> None:
    four_weeks = [item for item in _samples(28, _usual) if item[0] <= END - timedelta(hours=1)]
    two_weeks = [item for item in _samples(14, _usual) if item[0] <= END - timedelta(hours=1)]

    assert pick_band(four_weeks, END)["band_kind"] == "hour_of_week"
    # sys.resource_stats keeps 14 days: two Thursdays are not three.
    assert pick_band(two_weeks, END)["band_kind"] == "hour_of_day"


def test_modified_z_handles_a_baseline_without_spread() -> None:
    assert modified_z(20.0, 20.0, 0.0) == 0.0
    assert modified_z(21.0, 20.0, 0.0) > 3.5
    assert modified_z(30.0, 20.0, 2.0) == pytest.approx(5.0)


# --- The service ------------------------------------------------------------


def _resource_rows(cpu: Callable[[datetime], float], days: float = 14) -> list[dict[str, Any]]:
    return [
        {
            "end_time": stamp,
            "sku": "GP_Gen5_4",
            "cpu_limit": 4,
            "avg_cpu_percent": value,
            "avg_data_io_percent": 5.0,
            "avg_log_write_percent": 3.0,
            "max_worker_percent": 2.0,
            "max_session_percent": 1.0,
        }
        for stamp, value in _samples(days, cpu)
    ]


@pytest.mark.asyncio
async def test_resource_bands_judge_each_metric_for_the_hour() -> None:
    incident = lambda stamp: 88.0 if stamp > END - timedelta(hours=1) else _usual(stamp)  # noqa: E731
    executor = StrictDmvExecutor(rows={"sys.resource_stats": _resource_rows(incident)})

    result = await DatabaseBaselineService(executor).resource_bands("appdb", window_minutes=60, master_available=True)

    assert executor.violations == []
    assert result["result_status"] == "ok"
    assert result["baseline_status"] == "ok"
    cpu = result["metrics"]["avg_cpu_percent"]
    assert cpu["judgement"] == "abnormal"
    # The incident hour is judged against earlier days, not against itself.
    assert cpu["band"]["median"] < 30.0
    assert result["metrics"]["avg_data_io_percent"]["judgement"] == "normal"
    assert result["window"]["samples"] == 12
    (call,) = [call for call in executor.calls if "sys.resource_stats" in call[1]]
    assert (call[0], call[2]) == ("master", ("appdb", 14 * 24 * 60))


@pytest.mark.asyncio
async def test_a_past_window_is_judged_against_the_days_before_it() -> None:
    executor = StrictDmvExecutor(rows={"sys.resource_stats": _resource_rows(_usual)})

    result = await DatabaseBaselineService(executor).resource_bands(
        "appdb", window_minutes=60, as_of_utc="2026-10-15T03:00:00Z", master_available=True
    )

    cpu = result["metrics"]["avg_cpu_percent"]
    assert result["window"]["end_utc"].startswith("2026-10-15T03:00:00")
    assert cpu["peak"] >= 88.0 and cpu["judgement"] == "normal"


@pytest.mark.asyncio
async def test_short_history_is_reported_as_insufficient() -> None:
    executor = StrictDmvExecutor(rows={"sys.resource_stats": _resource_rows(lambda _stamp: 97.0, days=2 / 24)})

    result = await DatabaseBaselineService(executor).resource_bands("appdb", master_available=True)

    assert result["baseline_status"] == "insufficient_history"
    assert result["metrics"]["avg_cpu_percent"]["judgement"] == "insufficient_history"
    assert result["history"]["days_needed_for_hour_of_day"] == 10
    assert "not a statement that the level is normal" in result["result_status_reason"]


@pytest.mark.asyncio
async def test_without_master_the_baseline_is_a_precondition() -> None:
    result = await DatabaseBaselineService(StrictDmvExecutor()).resource_bands("appdb", master_available=False)

    assert result["result_status"] == "precondition"
    assert "AZURE_SQL_ALLOWED_DATABASES" in result["remediation"]


def _query_rows(query_id: int, duration_ms: Callable[[datetime], float], days: int = 20, executions: int = 200) -> list[dict[str, Any]]:
    rows = []
    for index in range(days * 24, -1, -1):
        start = END - timedelta(hours=1) - timedelta(hours=index)
        rows.append(
            {
                "query_id": query_id,
                "interval_start_utc": start,
                "executions": executions,
                "total_duration_us": duration_ms(start) * 1000.0 * executions,
                "total_cpu_us": duration_ms(start) * 500.0 * executions,
            }
        )
    return rows


class _QueryExecutor:
    def __init__(self, rows: list[dict[str, Any]]):
        self.rows = rows
        self.calls: list[tuple[str, str, tuple[Any, ...]]] = []

    async def fetch_all(self, database_name: str, query: str, params: Any = None, **_: Any) -> list[dict[str, Any]]:
        self.calls.append((database_name, query, tuple(params or ())))
        return self.rows


@pytest.mark.asyncio
async def test_query_bands_separate_a_new_slowdown_from_a_query_that_is_always_slow() -> None:
    last_hour = END - timedelta(hours=1)
    rows = (
        _query_rows(7, lambda start: 400.0 if start >= last_hour else 20.0 + start.day % 3)
        + _query_rows(8, lambda start: 400.0 + start.day % 5)
        + _query_rows(9, lambda start: 900.0 if start >= last_hour else 30.0, days=1)
        + _query_rows(10, lambda start: 900.0 if start >= last_hour else 30.0, executions=2)
    )
    executor = _QueryExecutor(rows)

    result = await DatabaseBaselineService(executor).query_bands("appdb", window_minutes=60, query_ids=[7, 8, 9, 10])

    judged = {item["query_id"]: item for item in result["queries"]}
    assert judged[7]["judgement"] == "abnormal"
    assert judged[7]["avg_duration_ms"]["band"]["median"] < 30.0
    assert judged[8]["judgement"] == "normal"
    assert judged[9]["judgement"] == "insufficient_history"
    assert judged[10]["judgement"] == "too_few_executions"
    # The newest first: what is abnormal leads.
    assert result["queries"][0]["query_id"] == 7
    (call,) = executor.calls
    assert call[2] == (30, 7, 8, 9, 10)


@pytest.mark.asyncio
async def test_query_bands_default_to_the_top_cpu_queries_of_the_window() -> None:
    class _Store:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, ...]] = []

        async def get_top_queries(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            self.calls.append((args, kwargs))
            return [{"query_id": 7}]

    store = _Store()
    executor = _QueryExecutor(_query_rows(7, lambda _start: 20.0))

    result = await DatabaseBaselineService(executor, store).query_bands("appdb", window_minutes=120, top=3)

    assert store.calls == [(("appdb", "cpu", 120, 3), {"group_by": "query"})]
    assert [item["query_id"] for item in result["queries"]] == [7]


def test_query_history_sql_reads_documented_query_store_columns() -> None:
    executor = StrictDmvExecutor()

    executor.check(QUERY_HISTORY_SQL.format(placeholders="?, ?"))

    assert executor.violations == []
