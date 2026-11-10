"""Baselines computed on demand from the history Azure already keeps. No collector.

``sys.resource_stats`` (master, 14 days of 5-minute samples) gives a band per
resource metric for the hour of day, and for the hour of the week when three
weeks exist. Query Store runtime statistics give the same per query. A window is
judged against the band of its own hour: normal, high, or abnormal. The band is
the median and the median absolute deviation (MAD), so one bad night does not
move it. An anomaly needs a modified z-score of 3.5 on both the peak and the
mean, and a move large enough to matter. A history too short for a band is
``insufficient_history`` with what is missing, never "normal".
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from datetime import timedelta
from statistics import median
from typing import Any

from .azure_tier import gated_read_unavailable
from .capacity_assessment import MAX_WINDOW_DAYS
from .capacity_assessment import RESOURCE_SAMPLES_SQL
from .capacity_assessment import RESOURCE_STATS_VIEW
from .capacity_assessment import SAMPLE_MINUTES
from .connection import AzureSqlExecutor
from .result_status import ResultStatus
from .result_status import status_payload
from .time_windows import parse_as_of

Sample = tuple[datetime, float]

# Gap review 2C item 12 (calibrated): hour x weekday needs 10 samples over 3
# days, hour only needs 10 samples over 10 days, else one flat band. An anomaly
# is a modified z of 3.5 or more on both the peak and the mean plus a magnitude
# floor; "worse" needs a move of 25% or more beyond one robust sigma.
HOUR_OF_WEEK_MIN_SAMPLES = 10
HOUR_OF_WEEK_MIN_DAYS = 3
HOUR_OF_DAY_MIN_SAMPLES = 10
HOUR_OF_DAY_MIN_DAYS = 10
MODIFIED_Z_BAR = 3.5
WORSE_MOVE_SHARE = 0.25
# Iglewicz and Hoaglin: sigma = MAD / 0.6745, or 1.2533 x the mean absolute
# deviation when the MAD is zero.
MAD_TO_SIGMA = 1.0 / 0.6745
MEAN_AD_TO_SIGMA = 1.253314
Z_WITHOUT_SPREAD = 99.0
# Magnitude floors. Judgment constants: 10 points of a limit is the smallest move
# worth an anomaly, 5 points the smallest worth "high".
MAGNITUDE_FLOOR_POINTS = 10.0
HIGH_FLOOR_SHARE = 0.5
# A flat band is a thin baseline: twice the floor, and the level must also reach
# the 80% health band (gap review 2E). Judgment constants.
THIN_FLOOR_MULTIPLE = 2.0
THIN_ABSOLUTE_BAR = 80.0
# Three hours of 5-minute samples, or 36 Query Store intervals. Judgment constant.
FLAT_MIN_SAMPLES = 36
# Per-query floors: 10 ms or 10 executions, and a quarter of the median. The
# window needs 10 executions (the regression reader's default) to be judged.
QUERY_DURATION_FLOOR_MS = 10.0
QUERY_EXECUTIONS_FLOOR = 10.0
QUERY_MIN_EXECUTIONS = 10
QUERY_HISTORY_DAYS = 30
MAX_QUERIES = 20

RESOURCE_METRICS = (
    "avg_cpu_percent",
    "avg_data_io_percent",
    "avg_log_write_percent",
    "max_worker_percent",
    "max_session_percent",
)

# Parameters: history days, then one query_id per placeholder.
QUERY_HISTORY_SQL = """
SELECT
    p.query_id,
    CAST(rsi.start_time AS datetime2(0)) AS interval_start_utc,
    SUM(rs.count_executions) AS executions,
    SUM(rs.avg_duration * rs.count_executions) AS total_duration_us,
    SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu_us
FROM sys.query_store_runtime_stats AS rs
INNER JOIN sys.query_store_runtime_stats_interval AS rsi
    ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
INNER JOIN sys.query_store_plan AS p
    ON p.plan_id = rs.plan_id
WHERE rsi.start_time >= DATEADD(DAY, -?, SYSUTCDATETIME())
  AND p.query_id IN ({placeholders})
GROUP BY p.query_id, rsi.start_time
ORDER BY p.query_id, rsi.start_time
"""

_JUDGEMENT_RANK = {"abnormal": 3, "high": 2, "normal": 1, "insufficient_history": 0, "too_few_executions": 0}


def _timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def robust_sigma(values: list[float]) -> tuple[float, float]:
    """The median and a robust standard deviation of ``values``."""

    center = median(values)
    deviations = [abs(value - center) for value in values]
    mad = median(deviations)
    if mad > 0:
        return center, mad * MAD_TO_SIGMA
    return center, MEAN_AD_TO_SIGMA * sum(deviations) / len(deviations)


def modified_z(value: float, center: float, sigma: float) -> float:
    """Distance from the median in robust sigmas; a band without spread has no scale."""

    if sigma > 0:
        return (value - center) / sigma
    return Z_WITHOUT_SPREAD if value > center else 0.0


def pick_band(baseline: list[Sample], at: datetime, *, shift_minutes: int = SAMPLE_MINUTES) -> dict[str, Any] | None:
    """The band for the hour of ``at``: hour of week, else hour of day, else flat, else None.

    A resource sample is stamped with its end time, so it is bucketed by its
    start (``shift_minutes`` earlier).
    """

    shifted = [(stamp - timedelta(minutes=shift_minutes), value) for stamp, value in baseline]
    same_hour = [(stamp, value) for stamp, value in shifted if stamp.hour == at.hour]
    same_weekday = [(stamp, value) for stamp, value in same_hour if stamp.weekday() == at.weekday()]
    for kind, bucket, min_samples, min_days in (
        ("hour_of_week", same_weekday, HOUR_OF_WEEK_MIN_SAMPLES, HOUR_OF_WEEK_MIN_DAYS),
        ("hour_of_day", same_hour, HOUR_OF_DAY_MIN_SAMPLES, HOUR_OF_DAY_MIN_DAYS),
        ("flat", shifted, FLAT_MIN_SAMPLES, 1),
    ):
        days = len({stamp.date() for stamp, _ in bucket})
        if len(bucket) < min_samples or days < min_days:
            continue
        center, sigma = robust_sigma([value for _, value in bucket])
        return {
            "band_kind": kind,
            "median": round(center, 2),
            "robust_sigma": round(sigma, 3),
            "upper": round(center + sigma, 2),
            "samples": len(bucket),
            "days": days,
        }
    return None


def _judge(peak: float, mean: float, band: Mapping[str, Any], floor_points: float, floor_share: float, thin_bar: float | None) -> dict[str, Any]:
    center, sigma = float(band["median"]), float(band["robust_sigma"])
    thin = band["band_kind"] == "flat"
    floor = max(floor_points, floor_share * center) * (THIN_FLOOR_MULTIPLE if thin else 1.0)
    z_peak, z_mean = modified_z(peak, center, sigma), modified_z(mean, center, sigma)
    abnormal = (
        z_peak >= MODIFIED_Z_BAR
        and z_mean >= MODIFIED_Z_BAR
        and peak - center >= floor
        and mean - center >= floor
        and (not thin or thin_bar is None or peak >= thin_bar)
    )
    high = (
        mean > center + sigma
        and mean >= center * (1.0 + WORSE_MOVE_SHARE)
        and mean - center >= floor_points * HIGH_FLOOR_SHARE
    )
    return {
        "judgement": "abnormal" if abnormal else "high" if high else "normal",
        "peak": round(peak, 2),
        "mean": round(mean, 2),
        "z_peak": round(z_peak, 2),
        "z_mean": round(z_mean, 2),
        "band_kind": band["band_kind"],
        "band": {key: band[key] for key in ("median", "upper", "robust_sigma", "samples", "days")},
    }


def judge_window(
    baseline: list[Sample],
    window: list[Sample],
    *,
    shift_minutes: int = SAMPLE_MINUTES,
    floor_points: float = MAGNITUDE_FLOOR_POINTS,
    floor_share: float = 0.0,
    thin_bar: float | None = THIN_ABSOLUTE_BAR,
) -> dict[str, Any]:
    """Judge the samples of a window against the band of each hour; return the worst hour.

    ``baseline`` must hold only samples from before the window, so an incident
    is never part of its own band.
    """

    hours: dict[datetime, list[float]] = {}
    for stamp, value in window:
        start = stamp - timedelta(minutes=shift_minutes)
        hours.setdefault(start.replace(minute=0, second=0, microsecond=0), []).append(value)
    judged: list[dict[str, Any]] = []
    for hour, values in sorted(hours.items()):
        band = pick_band(baseline, hour, shift_minutes=shift_minutes)
        if band is None:
            continue
        judged.append(
            {
                **_judge(max(values), sum(values) / len(values), band, floor_points, floor_share, thin_bar),
                "hour_utc": hour.isoformat(),
            }
        )
    if not judged:
        return {
            "judgement": "insufficient_history",
            "missing": {"baseline_samples": len(baseline), "samples_needed": FLAT_MIN_SAMPLES},
        }
    worst = max(judged, key=lambda item: (_JUDGEMENT_RANK[item["judgement"]], item["z_mean"]))
    return {**worst, "hours_judged": len(judged)}


class DatabaseBaselineService:
    def __init__(self, executor: AzureSqlExecutor, query_store: Any = None):
        self.executor = executor
        self.query_store = query_store

    async def resource_bands(
        self,
        database_name: str,
        *,
        window_minutes: int = 60,
        as_of_utc: str | None = None,
        master_available: bool = False,
    ) -> dict[str, Any]:
        """Judge each resource metric of a window against the band for its hour."""

        window_minutes = max(SAMPLE_MINUTES, min(int(window_minutes), 7 * 24 * 60))
        base: dict[str, Any] = {
            "database_name": database_name,
            "source": f"{RESOURCE_STATS_VIEW} (master), 5-minute samples, about {MAX_WINDOW_DAYS} days",
            "metrics": {},
        }
        if not master_available:
            return {
                **base,
                **status_payload(
                    ResultStatus.PRECONDITION,
                    f"A baseline needs the 5-minute history in {RESOURCE_STATS_VIEW}, which is read from master.",
                    remediation="Add master to AZURE_SQL_ALLOWED_DATABASES; the login needs access to master.",
                ),
            }
        try:
            rows = await self.executor.fetch_all(
                "master", RESOURCE_SAMPLES_SQL, params=[database_name, MAX_WINDOW_DAYS * 24 * 60]
            )
        except Exception as exc:
            blocked = await gated_read_unavailable(
                self.executor, database_name, RESOURCE_STATS_VIEW, exc, site="database_baseline.resource"
            )
            if blocked is None:
                raise
            return {**base, **blocked}
        stamped = sorted(
            ((stamp, row) for row in rows if (stamp := _timestamp(row.get("end_time"))) is not None),
            key=lambda item: item[0],
        )
        if not stamped:
            return {
                **base,
                "baseline_status": "insufficient_history",
                **status_payload(
                    ResultStatus.EMPTY,
                    f"{RESOURCE_STATS_VIEW} holds no sample for {database_name}. The name must match the "
                    "name in master exactly.",
                ),
            }
        as_of = parse_as_of(as_of_utc)
        # Without as_of the newest sample ends the window: the view lags by minutes,
        # and the client clock is not the database's clock.
        end = as_of.replace(tzinfo=None) if as_of else stamped[-1][0]
        start = end - timedelta(minutes=window_minutes)
        before = [(stamp, row) for stamp, row in stamped if stamp <= start]
        inside = [(stamp, row) for stamp, row in stamped if start < stamp <= end]
        metrics: dict[str, Any] = {}
        for metric in RESOURCE_METRICS:
            baseline = [(stamp, _number(row[metric])) for stamp, row in before if row.get(metric) is not None]
            window = [(stamp, _number(row[metric])) for stamp, row in inside if row.get(metric) is not None]
            if window:
                metrics[metric] = judge_window(baseline, window)
        days = (before[-1][0] - before[0][0]).total_seconds() / 86400.0 if len(before) > 1 else 0.0
        judged = [item for item in metrics.values() if item["judgement"] != "insufficient_history"]
        payload: dict[str, Any] = {
            **base,
            "window": {
                "start_utc": start.isoformat(),
                "end_utc": end.isoformat(),
                "minutes": window_minutes,
                "samples": len(inside),
            },
            "history": {
                "baseline_samples": len(before),
                "days_covered": round(days, 1),
                "days_needed_for_hour_of_day": HOUR_OF_DAY_MIN_DAYS,
                "weeks_needed_for_hour_of_week": HOUR_OF_WEEK_MIN_DAYS,
            },
            "baseline_status": "ok" if judged else "insufficient_history",
            "metrics": metrics,
            "method": {
                "band": "median and MAD of the same hour on earlier days",
                "abnormal": "modified z >= 3.5 on the peak and the mean, and 10 points above the median",
                "high": "mean above one robust sigma and 25% above the median",
            },
        }
        if not inside:
            payload.update(
                status_payload(ResultStatus.EMPTY, "The window holds no sample, so nothing can be judged.")
            )
        elif not judged:
            payload.update(
                status_payload(
                    ResultStatus.OK,
                    f"insufficient_history: {len(before)} sample(s) over {round(days, 1):g} day(s) before "
                    f"the window; {FLAT_MIN_SAMPLES} samples are needed for any band and "
                    f"{HOUR_OF_DAY_MIN_DAYS} days for a band per hour. This is not a statement that the "
                    "level is normal.",
                )
            )
        else:
            abnormal = [name for name, item in metrics.items() if item["judgement"] == "abnormal"]
            payload.update(
                status_payload(
                    ResultStatus.OK,
                    f"{len(abnormal)} of {len(metrics)} metric(s) abnormal for this hour.",
                )
            )
        return payload

    async def query_bands(
        self,
        database_name: str,
        *,
        window_minutes: int = 60,
        query_ids: list[int] | None = None,
        top: int = 5,
        history_days: int = QUERY_HISTORY_DAYS,
    ) -> dict[str, Any]:
        """Judge the duration and the call count of each query against its band for the hour.

        ``query_ids`` defaults to the top CPU queries of the window. The newest
        intervals that cover ``window_minutes`` (one hour at least) are judged
        against the earlier intervals of the same hour.
        """

        window_minutes = max(60, min(int(window_minutes), 7 * 24 * 60))
        history_days = max(1, min(int(history_days), 90))
        if query_ids is None:
            if self.query_store is None:
                raise ValueError("query_ids is required when no Query Store service is wired.")
            top_rows = await self.query_store.get_top_queries(
                database_name, "cpu", window_minutes, max(1, min(int(top), MAX_QUERIES)), group_by="query"
            )
            query_ids = [int(row["query_id"]) for row in top_rows if row.get("query_id") is not None]
        query_ids = [int(query_id) for query_id in query_ids][:MAX_QUERIES]
        payload: dict[str, Any] = {
            "database_name": database_name,
            "source": "Query Store runtime statistics per interval",
            "window_minutes": window_minutes,
            "history_days": history_days,
            "queries": [],
        }
        if not query_ids:
            return {**payload, **status_payload(ResultStatus.EMPTY, "No query ran in the window.")}
        rows = await self.executor.fetch_all(
            database_name,
            QUERY_HISTORY_SQL.format(placeholders=", ".join("?" for _ in query_ids)),
            params=[history_days, *query_ids],
        )
        by_query: dict[int, list[tuple[datetime, Mapping[str, Any]]]] = {}
        for row in rows:
            stamp = _timestamp(row.get("interval_start_utc"))
            if stamp is not None and row.get("query_id") is not None:
                by_query.setdefault(int(row["query_id"]), []).append((stamp, row))
        newest = max((stamp for series in by_query.values() for stamp, _ in series), default=None)
        for query_id in query_ids:
            series = sorted(by_query.get(query_id, []), key=lambda item: item[0])
            payload["queries"].append(_judge_query(query_id, series, newest, window_minutes))
        payload["queries"].sort(key=lambda item: -_JUDGEMENT_RANK[item["judgement"]])
        abnormal = sum(1 for item in payload["queries"] if item["judgement"] == "abnormal")
        payload.update(
            status_payload(
                ResultStatus.OK,
                f"{abnormal} of {len(query_ids)} query/queries abnormal for this hour.",
            )
        )
        return payload


def _judge_query(
    query_id: int,
    series: list[tuple[datetime, Mapping[str, Any]]],
    newest: datetime | None,
    window_minutes: int,
) -> dict[str, Any]:
    if not series or newest is None:
        return {
            "query_id": query_id,
            "judgement": "insufficient_history",
            "missing": {"baseline_samples": 0, "samples_needed": FLAT_MIN_SAMPLES},
        }
    start = newest - timedelta(minutes=window_minutes)
    before = [(stamp, row) for stamp, row in series if stamp <= start]
    inside = [(stamp, row) for stamp, row in series if stamp > start]
    executions = sum(_number(row.get("executions")) for _, row in inside)
    if executions < QUERY_MIN_EXECUTIONS:
        return {
            "query_id": query_id,
            "judgement": "too_few_executions",
            "executions_in_window": int(executions),
            "executions_needed": QUERY_MIN_EXECUTIONS,
        }

    def duration_ms(row: Mapping[str, Any]) -> float:
        return _number(row.get("total_duration_us")) / max(_number(row.get("executions")), 1.0) / 1000.0

    def judged(value: Any, floor_points: float) -> dict[str, Any]:
        return judge_window(
            [(stamp, value(row)) for stamp, row in before if _number(row.get("executions")) > 0],
            [(stamp, value(row)) for stamp, row in inside if _number(row.get("executions")) > 0],
            shift_minutes=0,
            floor_points=floor_points,
            floor_share=WORSE_MOVE_SHARE,
            thin_bar=None,
        )

    duration = judged(duration_ms, QUERY_DURATION_FLOOR_MS)
    calls = judged(lambda row: _number(row.get("executions")), QUERY_EXECUTIONS_FLOOR)
    worst = max(duration["judgement"], calls["judgement"], key=lambda name: _JUDGEMENT_RANK[name])
    return {
        "query_id": query_id,
        "judgement": worst,
        "executions_in_window": int(executions),
        "avg_duration_ms": duration,
        "executions_per_interval": calls,
    }
