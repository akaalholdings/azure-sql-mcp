"""Capacity verdict from the history Azure keeps: under-provisioned, over-provisioned or right-sized.

Reads the raw 5-minute samples of ``sys.resource_stats`` (master, about 14 days)
and judges the current service objective on CPU, data IO, log rate, workers,
sessions, memory grants and storage headroom. The verdict names the binding
dimension and the tier change. It never reads or states a price (decision D8):
the caller brings the prices.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .azure_tier import TierContext
from .azure_tier import gated_read_unavailable
from .azure_tier import read_dmv
from .azure_tier import read_tier_context
from .connection import AzureSqlExecutor
from .incident_log import note_exception
from .result_status import ResultStatus
from .result_status import status_payload

MAX_WINDOW_DAYS = 14
SAMPLE_MINUTES = 5
RESOURCE_STATS_VIEW = "sys.resource_stats"

# The one raw reader of sys.resource_stats. Parameters: database name, minutes.
RESOURCE_SAMPLES_SQL = """
SELECT
    end_time,
    sku,
    cpu_limit,
    dtu_limit,
    avg_cpu_percent,
    avg_data_io_percent,
    avg_log_write_percent,
    max_worker_percent,
    max_session_percent,
    storage_in_megabytes
FROM sys.resource_stats
WHERE database_name = ?
  AND end_time >= DATEADD(MINUTE, -?, GETUTCDATE())
ORDER BY end_time
"""

MAX_SIZE_SQL = """
SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS bigint) AS max_size_bytes
"""

RECENT_MEMORY_SQL = """
SELECT CAST(AVG(avg_memory_usage_percent) AS DECIMAL(5, 2)) AS avg_memory_usage_percent
FROM sys.dm_db_resource_stats
"""

# Gap review 2C item 15 (calibrated): under-provisioned at p95 CPU above 85%, any
# grant waiter, grant timeout or forced grant, or workers above 80%;
# over-provisioned at average CPU under 15%, maximum under 40% and grant use
# under 50%; 12 or more samples; no verdict for master.
MIN_SAMPLES_FOR_VERDICT = 12
UNDER_CPU_P95 = 85.0
UNDER_WORKERS_P95 = 80.0
OVER_CPU_AVG = 15.0
OVER_CPU_MAX = 40.0
OVER_GRANT_USE = 0.5
# The gap review asks for log rate, data IO and sessions as well and gives no
# bars. Judgment: the CPU bar for the two rates, the worker bar for sessions.
UNDER_DATA_IO_P95 = 85.0
UNDER_LOG_WRITE_P95 = 85.0
UNDER_SESSIONS_P95 = 80.0
# Judgment: half the compute doubles every percentage, so a scale-down needs each
# other dimension to peak under 40% as well.
OVER_OTHER_MAX = 40.0
# Design W11c step 11: under seven days covered, a scale-down is not judged.
MIN_DAYS_FOR_SCALE_DOWN = 7
# elastic_pool.py: storage is critical at 90% used.
STORAGE_UNDER_PERCENT = 90.0
# Judgment: three missing 5-minute samples in a row is a pause, one is noise.
PAUSE_GAP_MINUTES = 15
FREQUENT_PAUSES_PER_DAY = 3.0
# Judgment: on serverless, memory use under half of the limit means the cache was reclaimed.
SERVERLESS_RECLAIM_MEMORY_PERCENT = 50.0

PRICING_NOTE = (
    "Prices are not read by this tool. Take the price of the current and the target service "
    "objective from the Azure pricing page or the client's agreement, with the date."
)

# dimension -> (column, under-provisioned p95 bar)
_DIMENSIONS: dict[str, tuple[str, float]] = {
    "cpu": ("avg_cpu_percent", UNDER_CPU_P95),
    "data_io": ("avg_data_io_percent", UNDER_DATA_IO_P95),
    "log_write": ("avg_log_write_percent", UNDER_LOG_WRITE_P95),
    "workers": ("max_worker_percent", UNDER_WORKERS_P95),
    "sessions": ("max_session_percent", UNDER_SESSIONS_P95),
}


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


def percentile(ordered: list[float], fraction: float) -> float:
    """Nearest-rank percentile of an ascending list (the rule summarize_resource_metric uses)."""

    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def summarize(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    return {
        "avg": round(sum(ordered) / len(ordered), 2),
        "p50": round(percentile(ordered, 0.50), 2),
        "p95": round(percentile(ordered, 0.95), 2),
        "p99": round(percentile(ordered, 0.99), 2),
        "max": round(ordered[-1], 2),
        "samples": len(ordered),
    }


def _ordered(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (row for row in rows if _timestamp(row.get("end_time")) is not None),
        key=lambda row: _timestamp(row.get("end_time")) or datetime.min,
    )


def current_segment(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """The samples since the last change of service objective, and the changes seen.

    A percentage before a scale operation is a percentage of a different limit,
    so only the current objective is judged.
    """

    events: list[dict[str, Any]] = []
    start = 0
    for index in range(1, len(rows)):
        before, after = rows[index - 1], rows[index]
        changed = any(before.get(key) != after.get(key) for key in ("sku", "cpu_limit", "dtu_limit"))
        if not changed:
            continue
        stamp = _timestamp(after.get("end_time"))
        events.append(
            {"at_utc": stamp.isoformat() if stamp else None, "from": before.get("sku"), "to": after.get("sku")}
        )
        start = index
    return rows[start:], events


def detect_pauses(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Gaps in the 5-minute samples: a paused serverless database writes no sample."""

    stamps = [stamp for stamp in (_timestamp(row.get("end_time")) for row in rows) if stamp is not None]
    paused_minutes = 0.0
    count = 0
    for before, after in zip(stamps, stamps[1:]):
        gap = (after - before).total_seconds() / 60.0
        if gap > PAUSE_GAP_MINUTES:
            count += 1
            paused_minutes += gap - SAMPLE_MINUTES
    span = (stamps[-1] - stamps[0]).total_seconds() / 60.0 if len(stamps) > 1 else 0.0
    return {
        "pause_count": count,
        "paused_minutes": round(paused_minutes),
        "paused_share_pct": round(100.0 * paused_minutes / span, 1) if span else 0.0,
        "pauses_per_day": round(count / (span / 1440.0), 2) if span else 0.0,
    }


def storage_headroom(rows: list[dict[str, Any]], max_size_mb: float | None) -> dict[str, Any] | None:
    """Used data space against the maximum size, and the days left at the growth of the window."""

    points = [
        (stamp, _number(row.get("storage_in_megabytes")))
        for row in rows
        if (stamp := _timestamp(row.get("end_time"))) is not None and row.get("storage_in_megabytes") is not None
    ]
    if not points or not max_size_mb:
        return None
    (first_at, first_mb), (last_at, last_mb) = points[0], points[-1]
    days = (last_at - first_at).total_seconds() / 86400.0
    growth = (last_mb - first_mb) / days if days > 0 else 0.0
    headroom = max(max_size_mb - last_mb, 0.0)
    return {
        "used_mb": round(last_mb, 1),
        "max_size_mb": round(max_size_mb, 1),
        "used_pct": round(100.0 * last_mb / max_size_mb, 1),
        "growth_mb_per_day": round(growth, 1),
        "days_to_max": round(headroom / growth, 1) if growth > 0 else None,
    }


def _memory_state(memory: Mapping[str, Any] | None) -> dict[str, Any] | None:
    semaphores = (memory or {}).get("resource_semaphores") or {}
    totals = semaphores.get("totals")
    if not isinstance(totals, Mapping):
        return None
    granted = _number(totals.get("granted_memory_mb"))
    available = _number(totals.get("available_memory_mb"))
    return {
        "waiters": int(_number(totals.get("waiter_count"))),
        "timeouts_since_engine_start": int(_number(totals.get("timeout_error_count_since_engine_start"))),
        "forced_grants_since_engine_start": int(_number(totals.get("forced_grant_count_since_engine_start"))),
        "grant_use": round(granted / (granted + available), 2) if granted + available > 0 else 0.0,
    }


def _limit_text(limits: Mapping[str, Any]) -> str:
    if limits.get("cpu_limit_vcores"):
        return f"{_number(limits['cpu_limit_vcores']):g} vCores"
    if limits.get("dtu_limit"):
        return f"{_number(limits['dtu_limit']):g} DTUs"
    return "the current limit"


def _tier_change(dimension: str, summary: Mapping[str, Any], limits: Mapping[str, Any], storage: Any) -> str:
    limit = _limit_text(limits)
    if dimension == "cpu":
        return (
            f"More compute than {limit}: p95 CPU is {summary['p95']}% of the limit. Before the change, "
            "check that the log rate and data IO of the target objective also cover their own use."
        )
    if dimension == "log_write":
        return (
            f"A higher log rate, not more CPU: p95 log write is {summary['p95']}% of the log rate limit "
            f"on {limit}. The log rate rises with vCores only up to a fixed cap per tier, so read the "
            "documented log rate of the target objective first. Hyperscale has the highest cap."
        )
    if dimension == "data_io":
        return (
            f"More data IO: p95 is {summary['p95']}% of the IOPS limit on {limit}. IOPS rise with vCores; "
            "Business Critical and Hyperscale give more IOPS per vCore than General Purpose."
        )
    if dimension in {"workers", "sessions"}:
        return (
            f"More {dimension} come with more compute than {limit}, but a worker or session peak is "
            "usually blocking, parallelism or a connection leak. Rule those out first."
        )
    if dimension == "memory_grants":
        return (
            f"More memory comes with more compute than {limit}. First check that the grants are sized "
            "from good statistics; an oversized grant queues the others."
        )
    return (
        f"Raise the maximum size, or move to a tier with a larger maximum: data space is "
        f"{storage['used_pct']}% of the maximum size."
    )


def assess_capacity(
    rows: list[dict[str, Any]],
    *,
    database_name: str,
    tier: TierContext | None = None,
    max_size_mb: float | None = None,
    memory: Mapping[str, Any] | None = None,
    recent_memory_percent: float | None = None,
) -> dict[str, Any]:
    """Judge the current service objective from 5-minute samples. Pure: no reads."""

    ordered, events = current_segment(_ordered(rows))
    stamps = [stamp for row in ordered if (stamp := _timestamp(row.get("end_time"))) is not None]
    days = (stamps[-1] - stamps[0]).total_seconds() / 86400.0 if len(stamps) > 1 else 0.0
    latest = ordered[-1] if ordered else {}
    limits: dict[str, Any] = {"service_objective": latest.get("sku") or (tier.service_objective if tier else None)}
    if latest.get("cpu_limit"):
        limits["cpu_limit_vcores"] = latest["cpu_limit"]
    elif latest.get("dtu_limit"):
        limits["dtu_limit"] = latest["dtu_limit"]
    dimensions = {
        name: summarize([_number(row.get(column)) for row in ordered if row.get(column) is not None])
        for name, (column, _) in _DIMENSIONS.items()
        if any(row.get(column) is not None for row in ordered)
    }
    grants = _memory_state(memory)
    storage = storage_headroom(ordered, max_size_mb)
    serverless = bool(tier and tier.compute_model == "serverless")
    not_observed = [
        name for name, seen in (("memory_grants", grants), ("storage", storage)) if seen is None
    ]
    result: dict[str, Any] = {
        "database_name": database_name,
        "verdict": None,
        "binding_dimension": None,
        "reasons": [],
        "tier_change": None,
        "caveats": [],
        "limits": limits,
        "dimensions": dimensions,
        "memory_grants": grants,
        "storage": storage,
        "serverless": None,
        "scale_events": events,
        "window": {
            "samples": len(ordered),
            "sample_minutes": SAMPLE_MINUTES,
            "days_covered": round(days, 1),
            "start_utc": stamps[0].isoformat() if stamps else None,
            "end_utc": stamps[-1].isoformat() if stamps else None,
            "scope": "since the last change of service objective",
        },
        "not_observed": not_observed,
        "pricing": PRICING_NOTE,
    }
    if serverless:
        pauses = detect_pauses(ordered)
        result["serverless"] = {
            **pauses,
            "basis": "inferred from gaps in the 5-minute samples",
            "recent_memory_usage_percent": recent_memory_percent,
            "memory_reclaim": (
                "not_observed"
                if recent_memory_percent is None
                else "likely"
                if recent_memory_percent < SERVERLESS_RECLAIM_MEMORY_PERCENT
                else "not_seen"
            ),
        }
    if len(ordered) < MIN_SAMPLES_FOR_VERDICT:
        result["verdict"] = "insufficient_evidence"
        result["missing"] = {"samples": len(ordered), "samples_needed": MIN_SAMPLES_FOR_VERDICT}
        return result

    # --- Under-provisioned: any dimension at its bar ---
    reasons: list[str] = result["reasons"]
    breaching = [
        (summary["p95"], name)
        for name, summary in dimensions.items()
        if summary["p95"] > _DIMENSIONS[name][1]
    ]
    for p95, name in sorted(breaching, reverse=True):
        reasons.append(f"p95 {name} is {p95}% of its limit (bar {_DIMENSIONS[name][1]:g}%).")
    binding = max(breaching)[1] if breaching else None
    if grants is not None and (
        grants["waiters"] or grants["timeouts_since_engine_start"] or grants["forced_grants_since_engine_start"]
    ):
        reasons.append(
            f"Memory grants: {grants['waiters']} waiting now, {grants['timeouts_since_engine_start']} "
            f"timeout(s) and {grants['forced_grants_since_engine_start']} forced grant(s) since the "
            "engine started."
        )
        binding = binding or "memory_grants"
    if storage is not None and storage["used_pct"] >= STORAGE_UNDER_PERCENT:
        reasons.append(f"Data space is {storage['used_pct']}% of the maximum size.")
        binding = binding or "storage"
    if storage is not None and storage["days_to_max"] is not None:
        result["caveats"].append(
            f"At the growth of this window the maximum size is reached in about {storage['days_to_max']:g} days."
        )
    if tier is not None and tier.pooled:
        result["verdict"] = "pool_scoped"
        result["caveats"].append(
            f"The database is in elastic pool {tier.elastic_pool_name or '(name not readable)'}: the "
            "percentages are of its per-database limit, and capacity is decided for the pool. Read "
            "the pool pressure."
        )
        return result
    if binding is not None:
        result["verdict"] = "under_provisioned"
        result["binding_dimension"] = binding
        result["tier_change"] = _tier_change(binding, dimensions.get(binding, {}), limits, storage)
        return result

    # --- Over-provisioned: every dimension far below its limit, for a week ---
    cpu = dimensions.get("cpu")
    idle = (
        cpu is not None
        and cpu["avg"] < OVER_CPU_AVG
        and cpu["max"] < OVER_CPU_MAX
        and all(summary["max"] < OVER_OTHER_MAX for name, summary in dimensions.items() if name != "cpu")
        and (grants is None or grants["grant_use"] < OVER_GRANT_USE)
    )
    if serverless:
        pauses = result["serverless"]
        result["verdict"] = "right_sized"
        result["tier_change"] = (
            "No change of maximum vCores. Serverless bills the vCores used, so the levers are the "
            "minimum vCores and the auto-pause delay"
            + (
                f": {pauses['pause_count']} auto-pause(s) in the window ({pauses['pauses_per_day']:g} a "
                "day). Each resume takes about a minute and the first connection fails with error "
                "40613, so a latency-sensitive application needs a longer delay or auto-pause off."
                if pauses["pauses_per_day"] >= FREQUENT_PAUSES_PER_DAY
                else "; no frequent auto-pause was seen in the window."
            )
        )
        return result
    if not idle:
        result["verdict"] = "right_sized"
        return result
    if days < MIN_DAYS_FOR_SCALE_DOWN:
        result["verdict"] = "insufficient_evidence"
        result["missing"] = {"days_covered": round(days, 1), "days_needed": MIN_DAYS_FOR_SCALE_DOWN}
        return result
    assert cpu is not None
    result["verdict"] = "over_provisioned"
    result["binding_dimension"] = "cpu"
    reasons.append(
        f"CPU averages {cpu['avg']}% and never passes {cpu['max']}% of {_limit_text(limits)} in "
        f"{round(days, 1):g} days; no other dimension passes {OVER_OTHER_MAX:g}%."
    )
    result["tier_change"] = (
        f"A smaller service objective than {_limit_text(limits)}. At half the compute the CPU peak "
        f"would be about {min(2 * cpu['max'], 100):g}% of the new limit. Go down one step, then judge "
        "again after a full business cycle (month-end included)."
    )
    if grants is None:
        result["caveats"].append(
            "Memory grant use was not observed. Check that no query waits for a memory grant before "
            "the scale-down."
        )
    return result


class CapacityAssessmentService:
    def __init__(self, executor: AzureSqlExecutor, memory_pressure: Any = None):
        self.executor = executor
        self.memory_pressure = memory_pressure

    async def assess(
        self,
        database_name: str,
        *,
        window_days: int = MAX_WINDOW_DAYS,
        master_available: bool = False,
    ) -> dict[str, Any]:
        """Read the history and return the capacity verdict for one user database."""

        window_days = max(1, min(int(window_days), MAX_WINDOW_DAYS))
        base: dict[str, Any] = {"database_name": database_name, "verdict": None, "pricing": PRICING_NOTE}
        if database_name.strip().casefold() == "master":
            return {
                **base,
                **status_payload(
                    ResultStatus.NOT_SUPPORTED,
                    "master holds no user workload and has no service objective to size. Name a user database.",
                ),
            }
        if not master_available:
            return {
                **base,
                **status_payload(
                    ResultStatus.PRECONDITION,
                    f"A capacity verdict needs the 5-minute history in {RESOURCE_STATS_VIEW}, which is "
                    "read from master.",
                    remediation="Add master to AZURE_SQL_ALLOWED_DATABASES; the login needs access to master.",
                ),
            }
        try:
            rows = await self.executor.fetch_all(
                "master", RESOURCE_SAMPLES_SQL, params=[database_name, window_days * 24 * 60]
            )
        except Exception as exc:
            blocked = await gated_read_unavailable(
                self.executor, database_name, RESOURCE_STATS_VIEW, exc, site="capacity_assessment.history"
            )
            if blocked is None:
                raise
            return {**base, **blocked}
        tier: TierContext | None = None
        try:
            tier = (await read_tier_context(self.executor, database_name)).tier
        except Exception as exc:
            note_exception(exc, "capacity_assessment.tier")
        size_rows, _ = await read_dmv(
            self.executor, database_name, "DATABASEPROPERTYEX", MAX_SIZE_SQL, site="capacity_assessment.max_size"
        )
        max_size_bytes = _number(size_rows[0].get("max_size_bytes")) if size_rows else 0.0
        memory = None
        if self.memory_pressure is not None:
            try:
                memory = await self.memory_pressure.get_memory_pressure(database_name, 1)
            except Exception as exc:
                note_exception(exc, "capacity_assessment.memory")
        recent_memory = None
        if tier is not None and tier.compute_model == "serverless":
            memory_rows, _ = await read_dmv(
                self.executor,
                database_name,
                "sys.dm_db_resource_stats",
                RECENT_MEMORY_SQL,
                site="capacity_assessment.recent_memory",
            )
            if memory_rows and memory_rows[0].get("avg_memory_usage_percent") is not None:
                recent_memory = _number(memory_rows[0]["avg_memory_usage_percent"])
        result = assess_capacity(
            rows,
            database_name=database_name,
            tier=tier,
            max_size_mb=max_size_bytes / (1024 * 1024) if max_size_bytes > 0 else None,
            memory=memory,
            recent_memory_percent=recent_memory,
        )
        if tier is None:
            result["not_observed"].append("service_objective")
        result["source"] = f"{RESOURCE_STATS_VIEW} (master), 5-minute samples"
        if not rows:
            result.update(
                status_payload(
                    ResultStatus.EMPTY,
                    f"{RESOURCE_STATS_VIEW} holds no sample for {database_name} in the last {window_days} "
                    "day(s). The database name must match the name in master exactly.",
                )
            )
        else:
            result.update(
                status_payload(
                    ResultStatus.OK,
                    f"Verdict {result['verdict']} from {result['window']['samples']} sample(s) over "
                    f"{result['window']['days_covered']:g} day(s).",
                )
            )
        return result
