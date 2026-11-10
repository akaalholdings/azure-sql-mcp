"""One-call database diagnosis: score the facts, link them into a cause, name what is missing.

Each source (resource use against limits, waits, sessions, Query Store, memory,
elastic pool, columnstore, locking state, version store) becomes scored facts:
a stable code, a value with its unit, the absolute bar it was judged against,
a severity, and its source and window. ``diagnosis_stories`` links facts into a
cause when two of them agree; a fact alone is a lead. A source that cannot be
read becomes an explicit ``not_observed`` fact and is never turned into
"healthy".
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from typing import Any

from .application_patterns import query_shape
from .diagnosis_stories import MAX_LEADS
from .diagnosis_stories import Fact
from .diagnosis_stories import build_stories
from .incident_log import note_exception
from .locking_state import read_locking_state
from .observability import sanitize_error_message
from .resource_governance import RECENT_RETENTION_MINUTES
from .result_status import ResultStatus
from .result_status import status_payload
from .wait_stats import classify_wait

SEVERITY_RANK = {"high": 3, "medium": 2, "low": 1, "info": 0, "unknown": -1}

LOG_GOVERNOR_WAITS = {
    "LOG_RATE_GOVERNOR",
    "POOL_LOG_RATE_GOVERNOR",
    "HADR_THROTTLE_LOG_RATE_GOVERNOR",
    "INSTANCE_LOG_RATE_GOVERNOR",
}

# --- Bars. Each number names where it comes from. ---------------------------

# Resource use as a percentage of the governed limit. 80/95 are the health bands
# of the gap review (2E) and of health.py and elastic_pool.py; five minutes
# above the line is their "sustained" rule. An average of 60 is the bar this
# module has always used for "elevated"; design W11a-16 proposes 50 and asks for
# a live calibration first.
PRESSURE_AVG_HIGH = 80.0
PRESSURE_AVG_ELEVATED = 60.0
PRESSURE_P95_HIGH = 95.0
PRESSURE_P95_ELEVATED = 80.0
PRESSURE_SUSTAINED_MINUTES = 5.0
# Peak bars, used alone only when the source gives no time above the line.
PRESSURE_PEAK_HIGH = 95.0
PRESSURE_PEAK_ELEVATED = 85.0
# health.py: below 20 samples the nearest-rank p95 is the single maximum.
PRESSURE_MIN_P95_SAMPLES = 20
PRESSURE_BAR = {
    "kind": "absolute",
    "unit": "percent_of_limit",
    "medium": "average >= 60, p95 >= 80, or 5 minutes above 80",
    "high": "average >= 80, p95 >= 95, or 5 minutes above 95",
}

# A wait whose bar has no "high" level is high at four times its bar. Judgment
# constant; the bars themselves are calibrated in wait_stats.py.
WAIT_HIGH_MULTIPLE = 4.0
# The log rate governor has no calibrated bar. Any governor wait in a live sample
# is throttling now; a cumulative figure is scored against the poison-wait bar
# of RESOURCE_SEMAPHORE (0.01 wait seconds per observed second).
GOVERNOR_CUMULATIVE_BAR = 0.01
# Relative share, used only when no absolute bar can be applied to the top wait.
DOMINANT_WAIT_SHARE_PCT = 30.0
WAIT_TOP_N = 25

# Gap review 2C item 13 (calibrated): chain depth 3 to 8, victims 5 to 25.
BLOCKED_MEDIUM = 5
BLOCKED_HIGH = 25
CHAIN_DEPTH_MEDIUM = 3
CHAIN_DEPTH_HIGH = 8
# Gap review 2E: a single lock wait of 10 s is a warning, 60 s is critical.
LOCK_WAIT_MEDIUM_MS = 10_000
LOCK_WAIT_HIGH_MS = 60_000
READER_MAJORITY_SHARE = 0.5
# sessions.PILEUP_MIN_REQUESTS (3) makes a pile-up; 10 on one statement is a queue. Judgment.
PILEUP_MEDIUM_REQUESTS = 10
# One request with half of the page writes of all running requests, and at least
# 1,000 pages written. Judgment constants; they name a writer, they do not score it.
DOMINANT_WRITER_SHARE = 0.5
DOMINANT_WRITER_MIN_WRITES = 1_000
MAINTENANCE_COMMANDS = ("ALTER INDEX", "CREATE INDEX", "DBCC", "BULK INSERT", "UPDATE STATISTIC", "SELECT INTO")

# Design W11a-16: a query is dominant at 10% of the CPU capacity of the window
# (vCores x seconds), a bad actor at 25%.
DOMINANT_QUERY_CAPACITY_PCT = 10.0
DOMINANT_QUERY_CAPACITY_HIGH_PCT = 25.0
# Relative tie-breaker when the CPU limit is unknown (the bar this module used before).
DOMINANT_QUERY_TOP_SHARE_PCT = 40.0
# Gap review 2B item 8: the share of measured CPU that the top queries explain is
# suspect under 0.5 or over 1.1.
ATTRIBUTION_LOW = 0.5
ATTRIBUTION_HIGH = 1.1
# One statement with 100 or more literal variants, half of them unparameterized. Judgment.
VARIANT_FAMILY_MIN_QUERIES = 100
VARIANT_FAMILY_UNPARAMETERIZED_SHARE = 0.5

# Design W11a-16: a regression counts by the run time it adds per hour.
REGRESSION_MIN_PCT = 50.0
REGRESSION_ADDED_SECONDS_PER_HOUR = 60.0
REGRESSION_ADDED_SECONDS_PER_HOUR_HIGH = 600.0

# plan_cache.py warns at half of the cached plans being single-use; below 1,000
# cached plans the share says nothing (judgment).
SINGLE_USE_PLAN_SHARE = 0.5
SINGLE_USE_MIN_PLANS = 1_000
# Gap review 2B item 6: five pending memory grants is severe.
PENDING_GRANTS_HIGH = 5

MAX_FACTS = 40

_UNREAD = frozenset({ResultStatus.UNAVAILABLE.value, ResultStatus.PRECONDITION.value})
# Context sources describe the database; an unread one is reported and does not
# make the verdict partial, because no pressure can hide behind it.
_CONTEXT_SOURCES = frozenset({"baseline", "locking_state"})
_ALL_SOURCES = (
    "resource_history",
    "resource_limits",
    "waits",
    "sessions",
    "top_queries",
    "regressions",
    "failed_executions",
    "version_store",
    "memory_pressure",
    "memory_grants",
    "elastic_pool",
    "columnstore_health",
    "locking_state",
    "plan_cache",
    "baseline",
)

# metric, fact code, title, dimension, next tools
_RESOURCE_RULES: tuple[tuple[str, str, str, str, list[dict[str, Any]]], ...] = (
    (
        "avg_cpu_percent",
        "cpu_pressure",
        "CPU is near the database's limit",
        "cpu",
        [
            {"tool": "get_top_queries", "arguments": {"sort_by": "cpu"}},
            {"tool": "get_query_store_regressions", "arguments": {"metric": "cpu"}},
        ],
    ),
    (
        "avg_data_io_percent",
        "data_io_pressure",
        "Data I/O is near the database's limit",
        "data_io",
        [
            {"tool": "get_top_queries", "arguments": {"sort_by": "logical_io"}},
            {"tool": "review_workload_indexes", "arguments": {"objective": "logical_reads"}},
        ],
    ),
    (
        "avg_log_write_percent",
        "log_write_pressure",
        "Log writes are near the log rate limit",
        "log_write",
        [
            {"tool": "get_resource_limits", "arguments": {}},
            {"tool": "get_open_transactions", "arguments": {}},
        ],
    ),
    (
        "max_worker_percent",
        "worker_pressure",
        "Worker threads are near the database's limit",
        "workers",
        [
            {"tool": "get_active_sessions", "arguments": {}},
            {"tool": "get_currently_waiting_tasks", "arguments": {}},
        ],
    ),
)


def _tools(*steps: tuple[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"tool": tool, "arguments": arguments} for tool, arguments in steps]


# fact code, test on the wait type, dimension, what it means, next tools
_WAIT_FAMILIES: tuple[tuple[str, Callable[[str], bool], str | None, str, list[dict[str, Any]]], ...] = (
    (
        "threadpool_waits",
        lambda wait: wait == "THREADPOOL",
        "workers",
        "Requests wait for a worker thread",
        _tools(("get_active_sessions", {}), ("get_currently_waiting_tasks", {})),
    ),
    (
        "compile_waits",
        lambda wait: wait == "RESOURCE_SEMAPHORE_QUERY_COMPILE",
        None,
        "Queries wait for memory to compile",
        _tools(("get_plan_cache_analysis", {})),
    ),
    (
        "memory_grant_waits",
        lambda wait: wait == "RESOURCE_SEMAPHORE",
        None,
        "Queries wait for a memory grant",
        _tools(("get_memory_grants", {})),
    ),
    (
        "scheduler_waits",
        lambda wait: wait == "SOS_SCHEDULER_YIELD",
        "cpu",
        "Requests queue for CPU",
        _tools(("get_top_queries", {"sort_by": "cpu"})),
    ),
    (
        "log_write_waits",
        lambda wait: wait == "WRITELOG",
        "log_write",
        "Commits wait for the log write",
        _tools(("get_resource_stats_history", {})),
    ),
    (
        "lock_waits",
        lambda wait: wait.startswith("LCK_M_"),
        None,
        "Sessions wait on locks",
        _tools(("get_active_sessions", {}), ("get_lock_details", {})),
    ),
    (
        "io_latch_waits",
        lambda wait: wait.startswith("PAGEIOLATCH_"),
        "data_io",
        "Queries wait for data pages to be read",
        _tools(("review_workload_indexes", {"objective": "logical_reads"}), ("get_io_stats", {})),
    ),
    (
        "parallelism_waits",
        lambda wait: wait.startswith("CX"),
        None,
        "Parallel queries wait on their own threads",
        _tools(("get_top_queries", {"sort_by": "cpu"})),
    ),
    (
        "latch_waits",
        lambda wait: wait.startswith(("LATCH_", "PAGELATCH_")),
        None,
        "Sessions wait on in-memory latches",
        _tools(("get_tempdb_usage", {})),
    ),
)
_LEVEL_RANK = {"high": 2, "above_bar": 1}


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _rank(severity: str) -> int:
    return SEVERITY_RANK.get(severity, 0)


def _named_tools(names: Any) -> list[Any]:
    return [{"tool": name, "arguments": {}} if isinstance(name, str) else name for name in names or []]


class DatabaseDiagnosisService:
    def __init__(
        self,
        *,
        resource_governance: Any,
        wait_stats: Any,
        sessions: Any,
        query_store: Any,
        query_store_trends: Any,
        version_store: Any,
        memory_pressure: Any = None,
        tempdb_memory: Any = None,
        elastic_pool: Any = None,
        columnstore_health: Any = None,
        plan_cache: Any = None,
        baseline: Any = None,
        executor: Any = None,
        source_timeout_seconds: float | None = None,
    ):
        self.resource_governance = resource_governance
        self.wait_stats = wait_stats
        self.sessions = sessions
        self.query_store = query_store
        self.query_store_trends = query_store_trends
        self.version_store = version_store
        self.memory_pressure = memory_pressure
        self.tempdb_memory = tempdb_memory
        self.elastic_pool = elastic_pool
        self.columnstore_health = columnstore_health
        self.plan_cache = plan_cache
        self.baseline = baseline
        self.executor = executor
        self.source_timeout_seconds = source_timeout_seconds

    def _sources(
        self,
        database_name: str,
        window_minutes: int,
        sample_seconds: int,
        master_available: bool,
    ) -> dict[str, Awaitable[Any]]:
        sources: dict[str, Awaitable[Any]] = {
            "resource_history": self.resource_governance.get_resource_stats_history(
                database_name, window_minutes, master_available=master_available
            ),
            "resource_limits": self.resource_governance.get_resource_limits(database_name),
            "waits": self.wait_stats.get_wait_stats(database_name, WAIT_TOP_N, sample_seconds=sample_seconds),
            "sessions": self.sessions.get_active_sessions(database_name, 200),
            "top_queries": self.query_store.get_top_queries(
                database_name, "cpu", window_minutes, 5, group_by="query_hash"
            ),
            "regressions": self.query_store_trends.regressions(
                database_name, recent_minutes=window_minutes, top=5
            ),
            "failed_executions": self.query_store_trends.failed_executions(
                database_name, recent_minutes=window_minutes, top=5
            ),
            "version_store": self.version_store.get_version_store_stats(database_name),
        }
        # Each optional source is read only when the server wires its service.
        if self.memory_pressure is not None:
            sources["memory_pressure"] = self.memory_pressure.get_memory_pressure(database_name, 5)
        if self.tempdb_memory is not None:
            sources["memory_grants"] = self.tempdb_memory.get_memory_grants(database_name)
        if self.elastic_pool is not None:
            sources["elastic_pool"] = self.elastic_pool.get_pool_pressure(database_name, window_minutes)
        if self.columnstore_health is not None:
            sources["columnstore_health"] = self.columnstore_health.get_columnstore_health(database_name, 3)
        if self.executor is not None:
            sources["locking_state"] = read_locking_state(self.executor, database_name)
        if self.plan_cache is not None:
            sources["plan_cache"] = self.plan_cache.get_plan_cache_analysis(database_name)
        if self.baseline is not None:
            sources["baseline"] = self.baseline.resource_bands(
                database_name, window_minutes=window_minutes, master_available=master_available
            )
        return sources

    async def _bounded(self, read: Awaitable[Any], extra_seconds: float) -> Any:
        if not self.source_timeout_seconds:
            return await read
        timeout = float(self.source_timeout_seconds) + extra_seconds
        try:
            return await asyncio.wait_for(read, timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"timed out after {timeout:g} s") from exc

    async def diagnose(
        self,
        database_name: str,
        *,
        window_minutes: int = 60,
        sample_seconds: int = 0,
        master_available: bool = False,
    ) -> dict[str, Any]:
        window_minutes = max(5, min(int(window_minutes), 7 * 24 * 60))
        sources = self._sources(database_name, window_minutes, sample_seconds, master_available)
        results = await asyncio.gather(
            *(
                self._bounded(read, float(sample_seconds) if name == "waits" else 0.0)
                for name, read in sources.items()
            ),
            return_exceptions=True,
        )
        evidence: dict[str, Any] = {}
        source_status: dict[str, str] = {}
        gaps: list[str] = []
        unread_facts: list[Fact] = []
        for name, result in zip(sources, results, strict=True):
            remediation = None
            if isinstance(result, BaseException):
                note_exception(result, f"database_diagnosis.{name}")
                status = ResultStatus.UNAVAILABLE.value
                reason = sanitize_error_message(str(result))
            else:
                read = _mapping(result)
                status = str(read.get("result_status") or ResultStatus.OK.value)
                reason = str(read.get("result_status_reason") or status)
                remediation = read.get("remediation")
                evidence[name] = result
            source_status[name] = status
            if status in _UNREAD:
                gaps.append(f"{name}: {reason}")
                unread_facts.append(_not_observed(name, database_name, status, reason, remediation))

        facts = score_facts(evidence, database_name=database_name, window_minutes=window_minutes)
        facts.extend(unread_facts)
        facts.sort(key=lambda fact: (0 if fact.actionable else 1 if fact.state == "not_observed" else 2, -_rank(fact.severity)))
        for index, fact in enumerate(facts, start=1):
            fact.id = f"F{index}"

        unread = {name for name, status in source_status.items() if status in _UNREAD}
        if _mapping(evidence.get("locking_state")).get("read_committed_snapshot") == "unknown":
            unread.add("locking_state")
        not_read = unread | (set(_ALL_SOURCES) - set(sources))
        stories, leads = build_stories(facts, unread_sources=not_read)
        actionable = [fact for fact in facts if fact.actionable]
        explained = {fact_id for story in stories for fact_id in story["supporting_facts"]}
        open_leads = sum(1 for fact in actionable if fact.id not in explained)
        pressure_gaps = sorted(unread - _CONTEXT_SOURCES)
        read_sources = [name for name, status in source_status.items() if status not in _UNREAD]

        if actionable:
            verdict = "findings"
        else:
            verdict = "partial" if pressure_gaps else "no_findings"
        limits = _mapping(evidence.get("resource_limits"))
        history = _mapping(evidence.get("resource_history"))
        waits = _mapping(evidence.get("waits"))
        payload: dict[str, Any] = {
            "database_name": database_name,
            "verdict": verdict,
            "conclusion": _conclusion(stories, leads, pressure_gaps),
            "stories": stories,
            "leads": leads,
            "facts": [fact.as_dict() for fact in facts[:MAX_FACTS]],
            "findings": [_legacy_finding(fact) for fact in actionable],
            "sources": source_status,
            "gaps": gaps,
            "context": {
                "service_objective": limits.get("service_objective"),
                "cpu_limit_vcores": _mapping(limits.get("governance_limits")).get("cpu_limit"),
                "windows": {
                    "requested_minutes": window_minutes,
                    "resource_history_minutes": _resource_window_minutes(history, window_minutes),
                    "resource_history_source": history.get("source"),
                    "query_store_minutes": window_minutes,
                    "waits": _mapping(waits.get("window")).get("mode"),
                    "sessions": "point_in_time",
                },
                "basis": {
                    "verified": "Read from the database in this call, for the window the fact names.",
                    "inferred": "Derived: a cumulative counter, a proxy, a share, or a link between facts.",
                },
            },
            "window_minutes": window_minutes,
            "sample_seconds": sample_seconds,
        }
        omitted = {"facts": max(0, len(facts) - MAX_FACTS), "leads": max(0, open_leads - len(leads))}
        if any(omitted.values()):
            payload["omitted"] = omitted
        if not read_sources:
            payload.update(
                status_payload(
                    ResultStatus.UNAVAILABLE,
                    "No diagnostic source could be read; see gaps.",
                )
            )
        else:
            payload.update(
                status_payload(
                    ResultStatus.OK,
                    f"Read {len(read_sources)} of {len(sources)} sources; {len(stories)} cause(s), "
                    f"{open_leads} lead(s).",
                )
            )
        return payload


def _conclusion(stories: list[dict[str, Any]], leads: list[dict[str, Any]], gaps: list[str]) -> str:
    unread = (
        f" {len(gaps)} source(s) could not be read ({', '.join(gaps)}), so this is not an all-clear."
        if gaps
        else ""
    )
    if stories:
        top = stories[0]
        return f"{top['cause']} Confidence {top['confidence']:.2f}.{unread}"
    if leads:
        return (
            f"No cause is established. {len(leads)} lead(s), each a fact without a second fact to "
            f"corroborate it. Strongest: {leads[0]['message']}{unread}"
        )
    if gaps:
        return f"No pressure was found in the sources that could be read.{unread}"
    return "No pressure was found: every source was read and nothing is above its bar."


def _not_observed(
    source: str, database: str, status: str, reason: str, remediation: Any
) -> Fact:
    detail: dict[str, Any] = {"result_status": status}
    if remediation:
        detail["remediation"] = remediation
    return Fact(
        code=f"{source}_not_observed",
        source=source,
        database=database,
        severity="unknown",
        state="not_observed",
        message=reason,
        detail=detail,
    )


def _legacy_finding(fact: Fact) -> dict[str, Any]:
    finding = {
        "code": fact.code,
        "severity": fact.severity,
        "message": fact.message,
        "evidence": fact.evidence if fact.evidence is not None else fact.detail,
        "next_tools": fact.next_tools,
    }
    if fact.id:
        finding["fact_id"] = fact.id
    return finding


def diagnose_findings(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    """The flat list: one finding per fact that is at or above its bar."""

    return [_legacy_finding(fact) for fact in score_facts(evidence) if fact.actionable]


def score_facts(
    evidence: Mapping[str, Any], *, database_name: str = "", window_minutes: int = 60
) -> list[Fact]:
    """Turn the source results into scored facts. Unread sources are added by the caller."""

    history = _mapping(evidence.get("resource_history"))
    facts = _resource_facts(history, window_minutes)
    cpu = next((fact for fact in facts if fact.code == "cpu_pressure"), None)
    facts.extend(_wait_facts(_mapping(evidence.get("waits"))))
    facts.extend(_session_facts(_mapping(evidence.get("sessions"))))
    facts.extend(
        _top_query_facts(
            evidence.get("top_queries"),
            _mapping(evidence.get("resource_limits")),
            cpu,
            window_minutes,
            _resource_window_minutes(history, window_minutes),
        )
    )
    facts.extend(_regression_facts(_mapping(evidence.get("regressions")), window_minutes))
    facts.extend(_failed_execution_facts(_mapping(evidence.get("failed_executions"))))
    facts.extend(_version_store_facts(_mapping(evidence.get("version_store"))))
    facts.extend(_memory_facts(_mapping(evidence.get("memory_pressure"))))
    facts.extend(_memory_grant_facts(_mapping(evidence.get("memory_grants"))))
    facts.extend(_pool_facts(_mapping(evidence.get("elastic_pool"))))
    facts.extend(_columnstore_facts(_mapping(evidence.get("columnstore_health"))))
    facts.extend(_locking_facts(_mapping(evidence.get("locking_state"))))
    facts.extend(_plan_cache_facts(_mapping(evidence.get("plan_cache"))))
    facts.extend(_baseline_facts(_mapping(evidence.get("baseline"))))
    for fact in facts:
        fact.database = database_name
    return facts


def _fact(code: str, source: str, severity: str, message: str, **fields: Any) -> Fact:
    return Fact(code=code, source=source, database="", severity=severity, message=message, **fields)


def _resource_window_minutes(history: Mapping[str, Any], window_minutes: int) -> int:
    """The minutes the resource history covers: the recent view holds about one hour."""

    if history.get("source") == "recent":
        return min(window_minutes, RECENT_RETENTION_MINUTES)
    return window_minutes


def _pressure_severity(stats: Mapping[str, Any], avg: float, peak: float) -> str:
    samples = stats.get("total_samples")
    p95 = _number(stats.get("p95")) if samples is not None and samples >= PRESSURE_MIN_P95_SAMPLES else 0.0
    above_95, above_80 = stats.get("minutes_above_95"), stats.get("minutes_above_80")
    timed = above_95 is not None
    if (
        avg >= PRESSURE_AVG_HIGH
        or p95 >= PRESSURE_P95_HIGH
        or _number(above_95) >= PRESSURE_SUSTAINED_MINUTES
        or (not timed and peak >= PRESSURE_PEAK_HIGH)
    ):
        return "high"
    if (
        avg >= PRESSURE_AVG_ELEVATED
        or p95 >= PRESSURE_P95_ELEVATED
        or _number(above_80) >= PRESSURE_SUSTAINED_MINUTES
        or (not timed and peak >= PRESSURE_PEAK_ELEVATED)
    ):
        return "medium"
    # A short touch of the limit is visible and is not sustained pressure.
    return "low" if peak >= PRESSURE_PEAK_HIGH else "info"


def _resource_facts(history: Mapping[str, Any], window_minutes: int) -> list[Fact]:
    summary = _mapping(history.get("summary"))
    recent = history.get("source") != "long_term"
    window = f"last {_resource_window_minutes(history, window_minutes)} min, " + (
        "15-second samples" if recent else "5-minute samples in hourly buckets"
    )
    facts = []
    for metric, code, title, dimension, next_tools in _RESOURCE_RULES:
        stats = summary.get(metric)
        if not isinstance(stats, dict):
            continue
        avg = _number(stats.get("avg"))
        peak = _number(stats.get("max"))
        # Long windows summarize hourly buckets: the bucket maximum (max_*) is
        # the 5-minute peak; the highest hourly average hides a short saturation.
        peak_stats = summary.get(metric.replace("avg_", "max_", 1))
        if metric.startswith("avg_") and isinstance(peak_stats, dict):
            peak = max(peak, _number(peak_stats.get("max")))
        facts.append(
            _fact(
                code,
                "resource_history",
                _pressure_severity(stats, avg, peak),
                f"{title} (average {avg:.0f}%, peak {peak:.0f}% of the limit).",
                value=peak,
                unit="percent_of_limit",
                bar=PRESSURE_BAR,
                window=window,
                detail={
                    "metric": metric,
                    "avg_pct": avg,
                    "p95_pct": stats.get("p95"),
                    "minutes_above_95": stats.get("minutes_above_95"),
                    "dimensions": [dimension],
                },
                evidence={"metric": metric, "avg_pct": avg, "max_pct": peak, "source": history.get("source")},
                next_tools=next_tools,
            )
        )
    return facts


def _wait_window(waits: Mapping[str, Any]) -> tuple[bool, str]:
    window = _mapping(waits.get("window"))
    if window.get("mode") == "cumulative":
        since = window.get("since_utc")
        return True, f"cumulative since {since}" if since else "cumulative since the counters started"
    return False, f"{window.get('sample_seconds') or '?'} s sample"


def _wait_facts(waits: Mapping[str, Any]) -> list[Fact]:
    top = [row for row in waits.get("top_waits") or [] if isinstance(row, Mapping)]
    if not top:
        return []
    cumulative, window = _wait_window(waits)
    basis = "inferred" if cumulative else "verified"
    facts: list[Fact] = []
    governor = [row for row in top if row.get("wait_type") in LOG_GOVERNOR_WAITS]
    if governor:
        ratio = max((_number(row.get("wait_seconds_per_observed_second")) for row in governor), default=0.0)
        known = any(row.get("wait_seconds_per_observed_second") is not None for row in governor)
        if not cumulative:
            severity = "high"
        else:
            severity = "medium" if not known or ratio >= GOVERNOR_CUMULATIVE_BAR else "low"
        facts.append(
            _fact(
                "log_rate_governor_waits",
                "waits",
                severity,
                "Queries are waiting on the log rate governor: log generation is hitting "
                "the service objective's cap.",
                value=ratio if known else None,
                unit="wait_seconds_per_observed_second",
                bar={
                    "kind": "absolute",
                    "note": "No calibrated bar: any governor wait in a live sample is throttling.",
                    "cumulative_medium": GOVERNOR_CUMULATIVE_BAR,
                },
                window=window,
                basis=basis,
                detail={"wait_types": [row.get("wait_type") for row in governor], "dimensions": ["log_write"]},
                evidence={"waits": [row.get("wait_type") for row in governor]},
                next_tools=_tools(
                    ("get_resource_limits", {}), ("get_resource_stats_history", {"window_minutes": 60})
                ),
            )
        )
    for code, matches, dimension, title, next_tools in _WAIT_FAMILIES:
        members = [row for row in top if matches(str(row.get("wait_type") or ""))]
        over = [row for row in members if row.get("absolute_level") in _LEVEL_RANK]
        if not over:
            continue
        best = max(
            over,
            key=lambda row: (
                _LEVEL_RANK[row["absolute_level"]],
                _number(row.get("wait_seconds_per_observed_second")),
            ),
        )
        ratio = _number(best.get("wait_seconds_per_observed_second"))
        bar = _number(best.get("absolute_bar"))
        high = best["absolute_level"] == "high" or (bar > 0 and ratio >= WAIT_HIGH_MULTIPLE * bar)
        detail: dict[str, Any] = {"wait_types": [row.get("wait_type") for row in over]}
        if dimension:
            detail["dimensions"] = [dimension]
        facts.append(
            _fact(
                code,
                "waits",
                "high" if high else "medium",
                f"{title}: {best.get('wait_type')} is at {ratio:g} wait seconds per observed second "
                f"(bar {bar:g}).",
                value=ratio,
                unit="wait_seconds_per_observed_second",
                bar={"kind": "absolute", "above_bar": best.get("absolute_bar"), "calibration": "SQL Server fleet"},
                window=window,
                basis=basis,
                detail=detail,
                next_tools=next_tools,
            )
        )
    if facts:
        return facts
    leader = top[0]
    wait_type = str(leader.get("wait_type") or "")
    share = _number(leader.get("pct_of_total"))
    category = str(leader.get("category") or classify_wait(wait_type))
    unjudged = leader.get("absolute_level") in {None, "no_bar", "unknown_observed_time"}
    if unjudged and share >= DOMINANT_WAIT_SHARE_PCT:
        # The tie-breaker: no absolute bar could be applied, so the share is all there is.
        return [
            _fact(
                "dominant_wait",
                "waits",
                "low",
                f"{wait_type} ({category}) accounts for {share:.0f}% of waits"
                + (" since the counters reset." if cumulative else " in the sample."),
                value=share,
                unit="percent_of_wait_time",
                bar={"kind": "relative_share", "low": DOMINANT_WAIT_SHARE_PCT},
                window=window,
                basis="inferred",
                detail={"wait_type": wait_type, "category": category},
                evidence={
                    "wait_type": wait_type,
                    "category": category,
                    "pct_of_total": share,
                    "window": waits.get("window"),
                },
                next_tools=_wait_next_tools(category),
            )
        ]
    return [
        _fact(
            "top_wait",
            "waits",
            "info",
            f"{wait_type} is the largest wait and is below its bar.",
            value=leader.get("wait_seconds_per_observed_second"),
            unit="wait_seconds_per_observed_second",
            window=window,
            basis=basis,
            detail={"wait_type": wait_type, "pct_of_total": share},
        )
    ]


def _wait_next_tools(category: str) -> list[dict[str, Any]]:
    mapping = {
        "CPU": [{"tool": "get_top_queries", "arguments": {"sort_by": "cpu"}}],
        "I/O": [{"tool": "review_workload_indexes", "arguments": {"objective": "logical_reads"}}],
        "Lock": [{"tool": "get_active_sessions", "arguments": {}}, {"tool": "get_lock_details", "arguments": {}}],
        "Memory": [{"tool": "get_memory_grants", "arguments": {}}],
        "Log I/O": [{"tool": "get_resource_stats_history", "arguments": {}}],
        "Parallelism": [{"tool": "get_top_queries", "arguments": {"sort_by": "cpu"}}],
        "Latch": [{"tool": "get_tempdb_usage", "arguments": {}}],
    }
    return mapping.get(category, [{"tool": "get_query_wait_stats", "arguments": {}}])


def _blocked_count(chain: Mapping[str, Any]) -> int:
    total = chain.get("total_blocked_count")
    return int(total) if total is not None else len(chain.get("blocked_session_ids") or [])


def _blocking_severity(blocked: int, depth: int) -> str:
    if blocked >= BLOCKED_HIGH or depth >= CHAIN_DEPTH_HIGH:
        return "high"
    if blocked >= BLOCKED_MEDIUM or depth >= CHAIN_DEPTH_MEDIUM:
        return "medium"
    return "low"


def _session_facts(sessions: Mapping[str, Any]) -> list[Fact]:
    facts: list[Fact] = []
    lock_tools = _tools(("get_lock_details", {}), ("get_open_transactions", {}))
    chains = [chain for chain in sessions.get("blocking_chains") or [] if isinstance(chain, Mapping)]
    if chains:
        blocked = sum(_blocked_count(chain) for chain in chains)
        depth = max(int(_number(chain.get("chain_depth"))) or 1 for chain in chains)
        head = chains[0].get("head_blocker_session_id")
        severity = _blocking_severity(blocked, depth)
        max_wait_ms = max(int(_number(chain.get("max_wait_time_ms"))) for chain in chains)
        facts.append(
            _fact(
                "active_blocking",
                "sessions",
                severity,
                f"{len(chains)} blocking chain(s) right now; {blocked} session(s) blocked, "
                f"head blocker session {head}.",
                value=blocked,
                unit="blocked_sessions",
                bar={"kind": "absolute", "medium": "5 blocked or depth 3", "high": "25 blocked or depth 8"},
                window="point_in_time",
                detail={"blocked": blocked, "head": head, "chains": len(chains), "depth": depth},
                evidence={
                    "chains": len(chains),
                    "blocked_sessions": blocked,
                    "head_blockers": [chain.get("head_blocker_session_id") for chain in chains[:5]],
                },
                next_tools=lock_tools,
            )
        )
        if max_wait_ms >= LOCK_WAIT_MEDIUM_MS:
            facts.append(
                _fact(
                    "long_lock_wait",
                    "sessions",
                    "high" if max_wait_ms >= LOCK_WAIT_HIGH_MS else "medium",
                    f"A blocked session has waited {max_wait_ms / 1000:.0f} s on a lock.",
                    value=max_wait_ms,
                    unit="ms",
                    bar={"kind": "absolute", "medium": LOCK_WAIT_MEDIUM_MS, "high": LOCK_WAIT_HIGH_MS},
                    window="point_in_time",
                    next_tools=lock_tools,
                )
            )
        idle = [chain for chain in chains if chain.get("head_blocker_state") == "sleeping_open_transaction"]
        if idle:
            chain = max(idle, key=_blocked_count)
            who = _mapping(chain.get("head_blocker"))
            facts.append(
                _fact(
                    "idle_head_blocker",
                    "sessions",
                    "high" if _blocked_count(chain) >= BLOCKED_HIGH else "medium",
                    f"Head blocker session {chain.get('head_blocker_session_id')} is idle with an open "
                    f"transaction and blocks {_blocked_count(chain)} session(s).",
                    value=_blocked_count(chain),
                    unit="blocked_sessions",
                    window="point_in_time",
                    detail={
                        "session_id": chain.get("head_blocker_session_id"),
                        "program_name": who.get("program_name"),
                        "idle_seconds": who.get("idle_seconds"),
                        "blocked": _blocked_count(chain),
                    },
                    next_tools=_tools(("get_open_transactions", {})),
                )
            )
        readers = sum(int(_number(_mapping(chain.get("victim_split")).get("reader"))) for chain in chains)
        if blocked >= BLOCKED_MEDIUM and readers / blocked >= READER_MAJORITY_SHARE:
            facts.append(
                _fact(
                    "reader_victims_majority",
                    "sessions",
                    severity,
                    f"{readers} of {blocked} blocked sessions want shared locks: readers wait behind writers.",
                    value=round(100.0 * readers / blocked, 1),
                    unit="percent_of_blocked_sessions",
                    bar={"kind": "absolute", "medium": "5 blocked and half of them readers"},
                    window="point_in_time",
                    next_tools=_tools(("get_database_configuration", {})),
                )
            )
    pileups = sessions.get("pileups") or []
    if pileups:
        top = _mapping(pileups[0])
        count = int(_number(top.get("request_count")))
        facts.append(
            _fact(
                "query_pileup",
                "sessions",
                "medium" if count >= PILEUP_MEDIUM_REQUESTS else "low",
                f"{count} requests run query hash {top.get('query_hash')} at the same time.",
                value=count,
                unit="requests",
                bar={"kind": "absolute", "low": 3, "medium": PILEUP_MEDIUM_REQUESTS},
                window="point_in_time",
                detail={
                    "query_hash": top.get("query_hash"),
                    "blocked_request_count": top.get("blocked_request_count"),
                },
                next_tools=_tools(("get_active_sessions", {})),
            )
        )
    requests = [row for row in sessions.get("sessions") or [] if isinstance(row, Mapping)]
    parallel = [row for row in requests if _number(row.get("dop")) > 1]
    if parallel:
        facts.append(
            _fact(
                "parallel_requests",
                "sessions",
                "info",
                f"{len(parallel)} request(s) run in parallel now.",
                value=sum(int(_number(row.get("parallel_worker_count"))) for row in parallel),
                unit="parallel_workers",
                window="point_in_time",
                detail={"requests": len(parallel)},
            )
        )
    writer = _dominant_writer(requests)
    if writer is not None:
        facts.append(writer)
    return facts


def _dominant_writer(requests: list[Mapping[str, Any]]) -> Fact | None:
    total = sum(_number(row.get("writes")) for row in requests)
    if total <= 0:
        return None
    top = max(requests, key=lambda row: _number(row.get("writes")))
    writes = _number(top.get("writes"))
    command = str(top.get("command") or "")
    maintenance = command.upper().startswith(MAINTENANCE_COMMANDS)
    if writes < DOMINANT_WRITER_MIN_WRITES or (writes / total < DOMINANT_WRITER_SHARE and not maintenance):
        return None
    return _fact(
        "dominant_writer",
        "sessions",
        "info",
        f"Session {top.get('session_id')} ({command}) does {100.0 * writes / total:.0f}% of the page "
        "writes of the running requests.",
        value=writes,
        unit="pages_written",
        window="point_in_time",
        detail={
            "session_id": top.get("session_id"),
            "command": command,
            "share_pct": round(100.0 * writes / total, 1),
            "query_hash": top.get("query_hash"),
        },
    )


def _top_query_facts(
    top_queries: Any,
    limits: Mapping[str, Any],
    cpu: Fact | None,
    window_minutes: int,
    resource_minutes: int,
) -> list[Fact]:
    rows = [row for row in top_queries if isinstance(row, Mapping)] if isinstance(top_queries, list) else []
    total = sum(_number(row.get("total_cpu_us")) for row in rows)
    if not rows or total <= 0:
        return []
    facts: list[Fact] = []
    leader = rows[0]
    leader_cpu = _number(leader.get("total_cpu_us"))
    query_id = leader.get("query_id", leader.get("sample_query_id"))
    top_share = round(100.0 * leader_cpu / total, 1)
    shape = query_shape(leader, window_minutes)
    detail: dict[str, Any] = {
        "query_id": query_id,
        "query_hash": leader.get("query_hash"),
        "share_of_top_queries_pct": top_share,
        "shape": shape["shape"],
        "_shape_advice": shape["advice"],
        "executions": shape["executions"],
        "cpu_ms_per_execution": shape["cpu_ms_per_execution"],
    }
    cpu_limit = _number(_mapping(limits.get("governance_limits")).get("cpu_limit"))
    if cpu_limit > 0:
        capacity_us = cpu_limit * window_minutes * 60 * 1_000_000
        share = round(100.0 * leader_cpu / capacity_us, 1)
        detail["share_of_cpu_capacity_pct"] = share
        if share >= DOMINANT_QUERY_CAPACITY_HIGH_PCT:
            severity = "high"
        else:
            severity = "medium" if share >= DOMINANT_QUERY_CAPACITY_PCT else "info"
        basis = "verified"
        bar: dict[str, Any] = {
            "kind": "absolute",
            "unit": "percent_of_cpu_capacity",
            "medium": DOMINANT_QUERY_CAPACITY_PCT,
            "high": DOMINANT_QUERY_CAPACITY_HIGH_PCT,
        }
        message = (
            f"Query hash {leader.get('query_hash')} (query {query_id}) uses {share:g}% of the CPU "
            f"capacity of the window ({cpu_limit:g} vCores)."
        )
        value: float = share
        unit = "percent_of_cpu_capacity"
        # Attribution is honest only when both figures cover the same minutes.
        if cpu is not None and resource_minutes == window_minutes and _number(cpu.detail.get("avg_pct")) > 0:
            measured_us = capacity_us * _number(cpu.detail.get("avg_pct")) / 100.0
            explained = total / measured_us
            detail["top_queries_explain_measured_cpu_pct"] = round(100.0 * explained, 1)
            if explained < ATTRIBUTION_LOW:
                detail["attribution_note"] = (
                    "The top queries explain under half of the measured CPU: the load is spread, or it "
                    "is not in Query Store."
                )
            elif explained > ATTRIBUTION_HIGH:
                detail["attribution_note"] = (
                    "The top queries add up to more CPU than was measured: the two windows do not line up."
                )
    else:
        severity = "low" if top_share >= DOMINANT_QUERY_TOP_SHARE_PCT else "info"
        basis = "inferred"
        bar = {"kind": "relative_share", "low": DOMINANT_QUERY_TOP_SHARE_PCT}
        message = (
            f"Query {query_id} uses {top_share:.0f}% of the CPU among the top five queries in the window."
        )
        value, unit = top_share, "percent_of_top_queries_cpu"
    if cpu is not None and _rank(cpu.severity) < _rank("medium"):
        # Without CPU pressure the largest query is only the largest query.
        severity = "info"
    facts.append(
        _fact(
            "dominant_query",
            "top_queries",
            severity,
            message,
            value=value,
            unit=unit,
            bar=bar,
            window=f"last {window_minutes} min of Query Store",
            basis=basis,
            detail=detail,
            evidence={
                "query_id": query_id,
                "plan_id": leader.get("plan_id"),
                "query_hash": leader.get("query_hash"),
                "share_of_top5_cpu_pct": top_share,
            },
            next_tools=_tools(
                ("analyze_query_plan", {"query_id": query_id}), ("get_query_store_trend", {"query_id": query_id})
            ),
        )
    )
    for row in rows:
        variants = _number(row.get("distinct_query_count"))
        unparameterized = _number(row.get("unparameterized_query_count"))
        if variants < VARIANT_FAMILY_MIN_QUERIES or unparameterized / variants < VARIANT_FAMILY_UNPARAMETERIZED_SHARE:
            continue
        facts.append(
            _fact(
                "unparameterized_query_family",
                "top_queries",
                "medium",
                f"Query hash {row.get('query_hash')} has {variants:.0f} literal variants in Query Store; "
                f"{unparameterized:.0f} are not parameterized.",
                value=variants,
                unit="query_ids",
                bar={"kind": "absolute", "medium": VARIANT_FAMILY_MIN_QUERIES},
                window=f"last {window_minutes} min of Query Store",
                detail={"query_hash": row.get("query_hash"), "distinct_query_count": int(variants)},
                next_tools=_tools(("get_plan_cache_analysis", {})),
            )
        )
        break
    return facts


def _regression_facts(regressions: Mapping[str, Any], window_minutes: int) -> list[Fact]:
    facts = []
    recent_minutes = _number(_mapping(regressions.get("window")).get("recent_minutes")) or window_minutes
    timed = str(regressions.get("metric") or "duration") in {"duration", "cpu"}
    for item in (regressions.get("regressions") or [])[:3]:
        pct = _number(item.get("regression_pct"))
        if pct < REGRESSION_MIN_PCT:
            continue
        query_id = item.get("query_id")
        added = _number(item.get("extra_cost")) / 1000.0 / (recent_minutes / 60.0) if timed else None
        if added is None:
            severity = "medium" if pct >= 200 or item.get("plan_changed") else "low"
        elif added >= REGRESSION_ADDED_SECONDS_PER_HOUR_HIGH:
            severity = "high"
        else:
            severity = "medium" if added >= REGRESSION_ADDED_SECONDS_PER_HOUR else "info"
        facts.append(
            _fact(
                "query_regression",
                "regressions",
                severity,
                f"Query {query_id} is {pct:.0f}% slower than its baseline"
                + (" and runs a new plan." if item.get("plan_changed") else "."),
                value=round(added, 1) if added is not None else pct,
                unit="added_seconds_per_hour" if added is not None else "percent_slower",
                bar={
                    "kind": "absolute",
                    "unit": "added_seconds_per_hour",
                    "medium": REGRESSION_ADDED_SECONDS_PER_HOUR,
                    "high": REGRESSION_ADDED_SECONDS_PER_HOUR_HIGH,
                },
                window=f"last {recent_minutes:g} min against the Query Store baseline",
                detail={
                    "query_id": query_id,
                    "query_ids": [query_id],
                    "regression_pct": pct,
                    "plan_changed": bool(item.get("plan_changed")),
                },
                evidence={
                    "query_id": query_id,
                    "regression_pct": pct,
                    "new_plan_ids": item.get("new_plan_ids"),
                    "extra_cost": item.get("extra_cost"),
                },
                next_tools=_tools(
                    ("get_query_store_trend", {"query_id": query_id}),
                    ("analyze_query_plan", {"query_id": query_id}),
                    ("plan_health_review", {}),
                ),
            )
        )
    return facts


def _failed_execution_facts(failed: Mapping[str, Any]) -> list[Fact]:
    rising = [item for item in failed.get("rising") or [] if isinstance(item, Mapping)]
    summary = _mapping(failed.get("summary"))
    top = rising[0] if rising else summary
    severity = top.get("severity")
    if severity not in {"high", "medium"}:
        return []
    query_ids = [item.get("query_id") for item in rising[:5]]
    subject = f"query {top.get('query_id')}" if rising else "the database"
    return [
        _fact(
            "failed_executions_rising",
            "failed_executions",
            str(severity),
            f"Aborted or failed runs of {subject} are at {_number(top.get('rise_ratio')):g} times the "
            f"baseline rate ({_number(top.get('recent_failed_executions')):g} in the window).",
            value=_number(top.get("rise_ratio")),
            unit="times_baseline_rate",
            bar={"kind": "absolute", "medium": "2x with 3 or more failed runs", "high": "5x"},
            window="recent window against the Query Store baseline",
            detail={"query_ids": query_ids, "query_hash": top.get("query_hash")},
            # Database-wide rise with no single query: the top queries carry the failed counts.
            next_tools=_tools(
                ("get_query_store_trend", {"query_id": top.get("query_id")})
                if rising
                else ("get_top_queries", {"sort_by": "executions"}),
            ),
        )
    ]


def _passed_through(findings: Any, source: str) -> list[Fact]:
    """Collector findings become facts; the collector owns the bar and the wording."""

    facts = []
    for item in findings or []:
        if not isinstance(item, Mapping):
            continue
        evidence = dict(_mapping(item.get("evidence")))
        cumulative = str(evidence.get("window") or "").startswith("cumulative")
        counts = [value for key, value in evidence.items() if key.endswith("_count") or "count_" in key]
        facts.append(
            _fact(
                str(item.get("code") or source),
                source,
                str(item.get("severity") or "info"),
                str(item.get("message") or ""),
                value=_number(counts[0]) if counts else None,
                unit="count" if counts else None,
                window=evidence.get("window") or "point_in_time",
                basis="inferred" if cumulative else "verified",
                evidence=evidence,
                next_tools=_named_tools(item.get("next_tools")),
            )
        )
    return facts


def _version_store_facts(version_store: Mapping[str, Any]) -> list[Fact]:
    return _passed_through(version_store.get("findings"), "version_store")


def _memory_facts(memory: Mapping[str, Any]) -> list[Fact]:
    return _passed_through(memory.get("findings"), "memory_pressure")


def _memory_grant_facts(grants: Mapping[str, Any]) -> list[Fact]:
    facts = []
    pending = int(_number(grants.get("pending_count")))
    if pending:
        facts.append(
            _fact(
                "pending_memory_grants",
                "memory_grants",
                "high" if pending >= PENDING_GRANTS_HIGH else "medium",
                f"{pending} query/queries wait for a memory grant right now.",
                value=pending,
                unit="queries",
                bar={"kind": "absolute", "medium": 1, "high": PENDING_GRANTS_HIGH},
                window="point_in_time",
                next_tools=_tools(("get_memory_grants", {})),
            )
        )
    spilling = int(_number(grants.get("likely_spilling_count")))
    if spilling:
        facts.append(
            _fact(
                "likely_spilling",
                "memory_grants",
                "low",
                f"{spilling} running query/queries use 95% or more of their memory grant and are likely "
                "to spill to tempdb.",
                value=spilling,
                unit="queries",
                window="point_in_time",
                basis="inferred",
                next_tools=_tools(("get_memory_grants", {})),
            )
        )
    return facts


def _pool_facts(pool: Mapping[str, Any]) -> list[Fact]:
    facts = []
    for item in pool.get("pressure") or []:
        if not isinstance(item, Mapping):
            continue
        dimension = str(item.get("dimension"))
        value = item.get("p95_percent", item.get("used_storage_percent"))
        facts.append(
            _fact(
                "elastic_pool_pressure",
                "elastic_pool",
                "high" if item.get("level") == "critical" else "medium",
                f"The elastic pool is at {_number(value):g}% of its {dimension} limit.",
                value=_number(value),
                unit="percent_of_pool_limit",
                bar={"kind": "absolute", "medium": "p95 >= 80", "high": "p95 >= 95 or 5 minutes above 95"},
                window="pool history, up to 40 min",
                detail={"dimension": dimension, "dimensions": [dimension]},
                next_tools=_tools(("get_resource_limits", {})),
            )
        )
    return facts


def _columnstore_facts(columnstore: Mapping[str, Any]) -> list[Fact]:
    totals = _mapping(columnstore.get("totals"))
    flagged = int(_number(totals.get("flagged_index_count")))
    if not flagged:
        return []
    worst = [
        f"{index.get('schema_name')}.{index.get('table_name')}.{index.get('index_name')}"
        for index in (columnstore.get("worst_indexes") or [])[:3]
        if isinstance(index, Mapping) and index.get("flags")
    ]
    return [
        _fact(
            "columnstore_maintenance_debt",
            "columnstore_health",
            "low",
            f"{flagged} columnstore index(es) have deleted rows over 20% or undersized row groups.",
            value=flagged,
            unit="indexes",
            bar={"kind": "absolute", "low": "20% deleted rows, or row groups under 102,400 rows"},
            window="point_in_time",
            detail={"worst_indexes": worst, "deleted_rows_percent": totals.get("deleted_rows_percent")},
        )
    ]


def _locking_facts(locking: Mapping[str, Any]) -> list[Fact]:
    if locking.get("read_committed_snapshot") != "off":
        return []
    return [
        _fact(
            "read_committed_snapshot_off",
            "locking_state",
            "info",
            "READ_COMMITTED_SNAPSHOT is off: readers take shared locks and wait behind writers.",
            window="point_in_time",
            detail={"optimized_locking": locking.get("optimized_locking")},
        )
    ]


def _plan_cache_facts(plan_cache: Mapping[str, Any]) -> list[Fact]:
    total = _number(plan_cache.get("total_plans"))
    single = _number(plan_cache.get("total_single_use_plans"))
    if total <= 0:
        return []
    share = single / total
    churn = total >= SINGLE_USE_MIN_PLANS and share >= SINGLE_USE_PLAN_SHARE
    return [
        _fact(
            "single_use_plan_share",
            "plan_cache",
            "medium" if churn else "info",
            f"{single:.0f} of {total:.0f} cached plans were used once.",
            value=round(100.0 * share, 1),
            unit="percent_of_cached_plans",
            bar={"kind": "absolute", "medium": "50% of 1,000 or more cached plans"},
            window="point_in_time",
            next_tools=_tools(("get_plan_cache_analysis", {})),
        )
    ]


def _baseline_facts(baseline: Mapping[str, Any]) -> list[Fact]:
    facts = []
    normal: dict[str, Any] = {}
    dimensions = {metric: dimension for metric, _, _, dimension, _ in _RESOURCE_RULES}
    for metric, judged in _mapping(baseline.get("metrics")).items():
        if not isinstance(judged, Mapping) or metric not in dimensions:
            continue
        if judged.get("judgement") != "abnormal":
            normal[metric] = judged.get("judgement")
            continue
        facts.append(
            _fact(
                f"{dimensions[metric]}_abnormal_for_hour",
                "baseline",
                "medium",
                f"{metric} peaked at {_number(judged.get('peak')):g}% and is abnormal for this hour "
                f"(usual median {_number(_mapping(judged.get('band')).get('median')):g}%).",
                value=_number(judged.get("peak")),
                unit="percent_of_limit",
                bar={"kind": "baseline_band", **_mapping(judged.get("band"))},
                window=str(judged.get("band_kind") or "baseline"),
                basis="inferred",
                detail={"dimensions": [dimensions[metric]], "z_peak": judged.get("z_peak")},
                next_tools=_tools(("get_resource_stats_history", {})),
            )
        )
    if normal:
        facts.append(
            _fact(
                "baseline_for_hour",
                "baseline",
                "info",
                "How the window compares with the usual level for this hour.",
                window="baseline",
                basis="inferred",
                detail=normal,
            )
        )
    return facts


__all__ = [
    "MAX_FACTS",
    "MAX_LEADS",
    "DatabaseDiagnosisService",
    "diagnose_findings",
    "score_facts",
]
