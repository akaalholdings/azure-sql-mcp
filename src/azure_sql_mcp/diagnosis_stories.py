"""Stories: link scored facts into a cause, or leave them as leads.

A fact is one scored observation (a wait above its bar, a blocking chain, a
query that uses a quarter of the CPU). A story is a small, explicit rule that
says which facts together make a cause. A story needs corroboration: every rule
has at least two required checks on different facts, so one fact alone stays a
lead. Confidence is a number built from the share of checks that matched and
the length of the matched chain, and it never reaches 1.0.

Facts of different databases are never linked: an incident does not span two
databases.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Collection
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Any

SEVERITY_RANK = {"high": 3, "medium": 2, "low": 1, "info": 0, "unknown": -1}

# Confidence = base + share weight x (matched checks / all checks) + depth weight
# x (matched links / full depth). The three weights are the calibrated formula in
# the gap review (2C item 11); it sums to 1.0, so the ceiling keeps a story below
# certainty.
BASE_CONFIDENCE = 0.20
MATCHED_SHARE_WEIGHT = 0.48
PATH_DEPTH_WEIGHT = 0.32
CONFIDENCE_CEILING = 0.95
# Three facts linked to the symptom earn the whole depth term. Judgment: the
# longest chain in the rules below has three links.
FULL_PATH_DEPTH = 3
# A check matched only by an inferred fact (a cumulative counter, a proxy) counts
# half. Judgment constant, not calibrated.
INFERRED_CHECK_WEIGHT = 0.5

MAX_STORIES = 5
MAX_LEADS = 8
MAX_POSSIBLE_CAUSES = 3

_INFERRED_HINTS = {
    "waits": (
        "Re-run with sample_seconds=15 so the waits are measured now, not since the "
        "counters started."
    ),
    "memory_pressure": (
        "The grant timeout and forced grant counts are totals since the engine started; "
        "get_memory_grants shows what waits now."
    ),
    "memory_grants": "A grant that is fully used suggests a spill; analyze_query_plan on that session confirms it.",
    "top_queries": "The CPU limit could not be read, so the share is relative to the top queries only.",
    "baseline": "The band is statistical; a longer history narrows it.",
}


@dataclass
class Fact:
    """One scored observation from one source of one database."""

    code: str
    source: str
    database: str
    severity: str = "info"
    state: str = "observed"  # observed | not_observed
    value: float | None = None
    unit: str | None = None
    bar: dict[str, Any] | None = None
    window: str | None = None
    basis: str = "verified"  # verified: read in this call; inferred: derived or cumulative
    message: str = ""
    detail: dict[str, Any] = field(default_factory=dict)
    next_tools: list[Any] = field(default_factory=list)
    evidence: dict[str, Any] | None = None
    id: str = ""

    @property
    def actionable(self) -> bool:
        return self.state == "observed" and SEVERITY_RANK.get(self.severity, 0) >= SEVERITY_RANK["low"]

    def as_dict(self) -> dict[str, Any]:
        """The compact form for a response: the number, its bar, and where it came from."""

        item: dict[str, Any] = {
            "id": self.id,
            "code": self.code,
            "state": self.state,
            "severity": self.severity,
            "source": self.source,
            "basis": self.basis,
        }
        for key in ("value", "unit", "bar", "window"):
            if getattr(self, key) is not None:
                item[key] = getattr(self, key)
        # Keys that start with "_" feed a story's wording and are not repeated here.
        shown = {key: value for key, value in self.detail.items() if not key.startswith("_")}
        if shown:
            item["detail"] = shown
        if self.state == "not_observed":
            item["reason"] = self.message
        return item


@dataclass(frozen=True)
class Check:
    """One link of a story: any of ``codes`` observed at ``min_severity`` or above."""

    name: str
    codes: tuple[str, ...]
    sources: tuple[str, ...]
    min_severity: str = "medium"
    required: bool = False
    raise_hint: str = ""


Picked = Mapping[str, Fact]


@dataclass(frozen=True)
class StoryRule:
    code: str
    # checks[0] is the symptom. Required checks must all match for a cause.
    checks: tuple[Check, ...]
    cause: Callable[[Picked], str]
    fix: Callable[[Picked], str]
    next_tools: Callable[[Picked], list[dict[str, Any]]]
    # Detail key (a list) that the facts of the required checks must share.
    join_on: str | None = None
    join_label: str = ""


def _need(name: str, codes: tuple[str, ...], sources: tuple[str, ...], min_severity: str = "medium") -> Check:
    return Check(name, codes, sources, min_severity, required=True)


def _extra(
    name: str,
    codes: tuple[str, ...],
    sources: tuple[str, ...],
    hint: str,
    min_severity: str = "medium",
) -> Check:
    return Check(name, codes, sources, min_severity, required=False, raise_hint=hint)


def _text(value: str) -> Callable[[Picked], str]:
    return lambda _picked: value


def _tools(*steps: tuple[str, dict[str, Any]]) -> Callable[[Picked], list[dict[str, Any]]]:
    return lambda _picked: [{"tool": tool, "arguments": dict(arguments)} for tool, arguments in steps]


def _detail(picked: Picked, code: str, key: str, default: Any = "?") -> Any:
    fact = picked.get(code)
    value = fact.detail.get(key) if fact else None
    return default if value is None else value


def _abandoned_cause(picked: Picked) -> str:
    program = _detail(picked, "idle_head_blocker", "program_name", None)
    return (
        f"Session {_detail(picked, 'idle_head_blocker', 'session_id')}"
        + (f" ({program})" if program else "")
        + f" holds locks in an open transaction while idle for "
        f"{_detail(picked, 'idle_head_blocker', 'idle_seconds')} s and blocks "
        f"{_detail(picked, 'idle_head_blocker', 'blocked')} session(s): an abandoned transaction."
    )


def _log_cause(picked: Picked) -> str:
    text = "Log generation is at the log rate cap and requests wait on the log rate governor"
    if "dominant_writer" in picked:
        return (
            f"{text}; session {_detail(picked, 'dominant_writer', 'session_id')} "
            f"({_detail(picked, 'dominant_writer', 'command')}) does "
            f"{_detail(picked, 'dominant_writer', 'share_pct')}% of the writes of the running requests."
        )
    return f"{text}. No single running request was seen doing most of the writes."


def _cpu_cause(picked: Picked) -> str:
    cpu = picked.get("cpu_pressure")
    return (
        f"Query hash {_detail(picked, 'dominant_query', 'query_hash')} uses "
        f"{_detail(picked, 'dominant_query', 'share_of_cpu_capacity_pct')}% of the CPU capacity in the window "
        f"while CPU is at {cpu.value if cpu and cpu.value is not None else '?'}% of its limit: "
        "one statement is the bad actor."
    )


def _cpu_fix(picked: Picked) -> str:
    advice = _detail(picked, "dominant_query", "_shape_advice", None)
    return advice or (
        "Read its plan before any scale-up: analyze_query_plan names the operator that burns the CPU."
    )


def _query_tools(code: str) -> Callable[[Picked], list[dict[str, Any]]]:
    def build(picked: Picked) -> list[dict[str, Any]]:
        query_id = _detail(picked, code, "query_id", None)
        arguments = {"query_id": query_id} if query_id is not None else {}
        return [
            {"tool": "analyze_query_plan", "arguments": dict(arguments)},
            {"tool": "get_query_store_trend", "arguments": dict(arguments)},
        ]

    return build


def _timeout_cause(picked: Picked) -> str:
    rise = picked.get("failed_executions_rising")
    return (
        f"Query {_detail(picked, 'query_regression', 'query_id')} is "
        f"{_detail(picked, 'query_regression', 'regression_pct')}% slower than its baseline and its aborted or "
        f"failed runs are at {rise.value if rise and rise.value is not None else '?'} times the baseline rate: "
        "the timeouts follow the regression."
    )


def _churn_cause(picked: Picked) -> str:
    text = "Statements are compiled again and again: compile waits are above the bar and "
    share = picked.get("single_use_plan_share")
    if share is not None:
        return f"{text}{share.value}% of the cached plans were used once."
    return (
        f"{text}one statement has "
        f"{_detail(picked, 'unparameterized_query_family', 'distinct_query_count')} literal variants in Query Store."
    )


def _pool_cause(picked: Picked) -> str:
    pool = picked.get("elastic_pool_pressure")
    return (
        f"The elastic pool is at its {_detail(picked, 'elastic_pool_pressure', 'dimension', 'resource')} limit "
        f"({pool.value if pool and pool.value is not None else '?'}% of the pool limit) and this database waits "
        "on that resource: the pool, not this database's own limit, is the constraint."
    )


_THREADPOOL = ("threadpool_waits", "worker_pressure")
_LOCK_WAITING = ("long_lock_wait", "lock_waits")

RULES: tuple[StoryRule, ...] = (
    StoryRule(
        code="abandoned_transaction",
        checks=(
            _need("the head blocker is idle with an open transaction", ("idle_head_blocker",), ("sessions",), "low"),
            _need("sessions wait on its locks for 10 s or more, or lock waits are above the bar", _LOCK_WAITING, ("sessions", "waits")),
            _extra(
                "the chain is wide or deep",
                ("active_blocking",),
                ("sessions",),
                "The chain is small now; get_active_sessions again in a minute shows whether it grows.",
            ),
            _extra(
                "version cleanup is held by a long transaction",
                ("version_cleanup_held_by_long_transaction",),
                ("version_store",),
                "get_open_transactions shows how long the head blocker's transaction has been open.",
            ),
        ),
        cause=_abandoned_cause,
        fix=_text(
            "Fix the code path that leaves the transaction open: commit or roll back on every exit and "
            "never wait for a user or a remote call inside a transaction. Ending the session is a DBA "
            "decision and rolls back its work."
        ),
        next_tools=_tools(("get_open_transactions", {}), ("get_lock_details", {})),
    ),
    StoryRule(
        code="memory_pressure_driving_io",
        checks=(
            _need("data page reads wait above the bar (PAGEIOLATCH)", ("io_latch_waits",), ("waits",)),
            _need(
                "queries wait for a memory grant",
                ("memory_grant_waiters", "pending_memory_grants", "memory_grant_waits"),
                ("memory_pressure", "memory_grants", "waits"),
            ),
            _extra(
                "queries spill to tempdb",
                ("likely_spilling", "forced_memory_grants"),
                ("memory_grants", "memory_pressure"),
                "get_memory_grants shows whether running queries use their whole grant and spill.",
                "low",
            ),
            _extra(
                "data IO is near its limit",
                ("data_io_pressure",),
                ("resource_history",),
                "get_resource_stats_history shows whether data IO reaches its limit when the grants queue.",
            ),
        ),
        cause=_text(
            "Queries wait for memory grants, so sorts and hashes run on disk and data pages are read "
            "again: memory pressure is driving the IO waits."
        ),
        fix=_text(
            "Shrink the grants first: fix the largest sorts and hashes (statistics, an index that removes "
            "the sort). More IOPS does not remove a spill."
        ),
        next_tools=_tools(("get_memory_grants", {}), ("get_top_queries", {"sort_by": "memory"})),
    ),
    StoryRule(
        code="log_rate_throttling",
        checks=(
            _need("requests wait on the log rate governor", ("log_rate_governor_waits",), ("waits",)),
            _need("log writes are near the log rate limit", ("log_write_pressure",), ("resource_history",)),
            _extra(
                "one running request does most of the writes",
                ("dominant_writer",),
                ("sessions",),
                "get_open_transactions ranks open transactions by log bytes and names the writer.",
                "info",
            ),
        ),
        cause=_log_cause,
        fix=_text(
            "Write less log per second: smaller batches, fewer indexes on the target, index maintenance "
            "outside the load. A larger service objective helps only where its log rate cap is higher; "
            "check the cap before scaling."
        ),
        next_tools=_tools(("get_resource_limits", {}), ("get_open_transactions", {})),
    ),
    StoryRule(
        code="parallelism_exhausting_workers",
        checks=(
            _need("requests wait for a worker thread, or workers are near the limit", _THREADPOOL, ("waits", "resource_history")),
            _need("parallelism waits are above the bar", ("parallelism_waits",), ("waits",)),
            _extra(
                "workers are near the limit",
                ("worker_pressure",),
                ("resource_history",),
                "get_resource_stats_history shows max_worker_percent for the same minutes.",
            ),
            _extra(
                "parallel requests are running now",
                ("parallel_requests",),
                ("sessions",),
                "get_active_sessions shows dop and parallel_worker_count per request.",
                "info",
            ),
        ),
        cause=_text(
            "Parallel queries take the worker threads: requests wait for a worker while parallelism "
            "waits are above the bar."
        ),
        fix=_text(
            "Lower MAXDOP for the database or fix the parallel plans. Do not scale up first: the same "
            "plans take the new workers too."
        ),
        next_tools=_tools(("get_active_sessions", {}), ("get_database_configuration", {})),
    ),
    StoryRule(
        code="blocking_holding_workers",
        checks=(
            _need("requests wait for a worker thread, or workers are near the limit", _THREADPOOL, ("waits", "resource_history")),
            _need("a wide or deep blocking chain is active", ("active_blocking",), ("sessions",)),
            _extra(
                "the head blocker is idle with an open transaction",
                ("idle_head_blocker",),
                ("sessions",),
                "get_active_sessions shows the state of the head blocker.",
                "low",
            ),
        ),
        cause=lambda picked: (
            f"Blocked sessions hold the worker threads: {_detail(picked, 'active_blocking', 'blocked')} "
            f"session(s) wait behind head blocker {_detail(picked, 'active_blocking', 'head')}, and new "
            "requests wait for a worker."
        ),
        fix=_text(
            "Clear the head blocker's transaction at its source; the workers come back when the chain "
            "drains. More workers only lets more sessions queue behind the same lock."
        ),
        next_tools=_tools(("get_active_sessions", {}), ("get_lock_details", {})),
    ),
    StoryRule(
        code="cpu_bad_actor",
        checks=(
            _need("CPU is near its limit", ("cpu_pressure",), ("resource_history",)),
            _need("one query hash uses 10% or more of the CPU capacity", ("dominant_query",), ("top_queries",)),
            _extra(
                "requests queue for CPU (SOS_SCHEDULER_YIELD above the bar)",
                ("scheduler_waits",),
                ("waits",),
                "get_wait_stats with sample_seconds=15 shows whether requests queue for CPU now.",
            ),
            _extra(
                "CPU is abnormal for this hour",
                ("cpu_abnormal_for_hour",),
                ("baseline",),
                "A baseline for this hour (needs master in the allowlist) shows whether this CPU level is new.",
            ),
            _extra(
                "the same statement runs many times at once",
                ("query_pileup",),
                ("sessions",),
                "get_active_sessions pileups shows whether the statement is stacked up now.",
                "low",
            ),
        ),
        cause=_cpu_cause,
        fix=_cpu_fix,
        next_tools=_query_tools("dominant_query"),
    ),
    StoryRule(
        code="timeouts_from_regression",
        checks=(
            _need("aborted or failed executions are rising", ("failed_executions_rising",), ("failed_executions",)),
            _need("a query is slower than its baseline", ("query_regression",), ("regressions",)),
        ),
        cause=_timeout_cause,
        fix=_text(
            "Restore the earlier behaviour of this query: plan_health_review says whether forcing the "
            "earlier plan is safe. Leave the client timeout as it is until the query is fixed."
        ),
        next_tools=_query_tools("query_regression"),
        join_on="query_ids",
        join_label="the same query in both facts",
    ),
    StoryRule(
        code="plan_cache_churn",
        checks=(
            _need("compile waits are above the bar", ("compile_waits",), ("waits",)),
            _need(
                "most cached plans are single-use, or one statement has many literal variants",
                ("single_use_plan_share", "unparameterized_query_family"),
                ("plan_cache", "top_queries"),
            ),
            _extra(
                "CPU is near its limit",
                ("cpu_pressure",),
                ("resource_history",),
                "get_resource_stats_history shows whether the compiles cost CPU capacity.",
            ),
        ),
        cause=_churn_cause,
        fix=_text(
            "Parameterize the statements in the application. Forced parameterization for the database is "
            "the no-deploy lever; test it first, because it changes plans for every statement."
        ),
        next_tools=_tools(
            ("get_plan_cache_analysis", {}),
            ("get_top_queries", {"sort_by": "executions"}),
        ),
    ),
    StoryRule(
        code="readers_blocked_by_writers",
        checks=(
            _need("most blocked sessions want shared locks", ("reader_victims_majority",), ("sessions",)),
            _need("READ_COMMITTED_SNAPSHOT is off", ("read_committed_snapshot_off",), ("locking_state",), "info"),
            _extra(
                "sessions wait on locks for 10 s or more, or lock waits are above the bar",
                _LOCK_WAITING,
                ("sessions", "waits"),
                "get_wait_stats with sample_seconds=15 shows whether lock waits are above the bar.",
            ),
        ),
        cause=lambda picked: (
            f"Readers wait behind writers: {picked['reader_victims_majority'].value}% of the blocked "
            "sessions want shared locks and READ_COMMITTED_SNAPSHOT is off."
        ),
        fix=_text(
            "Turn READ_COMMITTED_SNAPSHOT on after testing the code that depends on blocking reads. It "
            "removes reader-writer blocking only; writers still block writers."
        ),
        next_tools=_tools(("get_database_configuration", {}), ("get_lock_details", {})),
    ),
    StoryRule(
        code="pool_limit_reached",
        checks=(
            _need("the elastic pool is near a pool limit", ("elastic_pool_pressure",), ("elastic_pool",)),
            _need(
                "this database waits on the same resource",
                ("scheduler_waits", "io_latch_waits", "log_write_waits", "log_rate_governor_waits", "threadpool_waits"),
                ("waits",),
            ),
        ),
        cause=_pool_cause,
        fix=_text(
            "Find which databases use the pool (per-database pool metrics in the portal), then move the "
            "heavy one out or raise the pool limit. Tuning this database alone does not free the pool."
        ),
        next_tools=_tools(("get_resource_limits", {}), ("get_resource_stats_history", {})),
        join_on="dimensions",
        join_label="a wait on the resource the pool is short of",
    ),
)


def _rank(severity: str) -> int:
    return SEVERITY_RANK.get(severity, 0)


def _strongest(facts: list[Fact]) -> Fact:
    return max(facts, key=lambda fact: (_rank(fact.severity), fact.basis == "verified"))


@dataclass
class _Match:
    rule: StoryRule
    matched: list[tuple[Check, list[Fact]]]
    missing_required: list[str]
    unmatched_optional: list[Check]

    @property
    def fired(self) -> bool:
        return not self.missing_required


def _match(rule: StoryRule, observed: list[Fact]) -> _Match:
    used: set[int] = set()
    matched: list[tuple[Check, list[Fact]]] = []
    missing: list[str] = []
    optional: list[Check] = []
    for check in rule.checks:
        candidates = [
            fact
            for fact in observed
            if fact.code in check.codes
            and _rank(fact.severity) >= _rank(check.min_severity)
            and id(fact) not in used
        ]
        if candidates:
            matched.append((check, candidates))
            used.update(id(fact) for fact in candidates)
        elif check.required:
            missing.append(check.name)
        else:
            optional.append(check)
    if rule.join_on and not missing:
        required = [(check, facts) for check, facts in matched if check.required]
        keys = [
            {key for fact in facts for key in fact.detail.get(rule.join_on) or []} for _, facts in required
        ]
        common = set.intersection(*keys) if keys else set()
        if not common:
            missing.append(rule.join_label)
        else:
            matched = [
                (
                    check,
                    [fact for fact in facts if common & set(fact.detail.get(rule.join_on) or [])]
                    if check.required
                    else facts,
                )
                for check, facts in matched
            ]
    return _Match(rule, matched, missing, optional)


def _story(match: _Match, observed: list[Fact], unread_sources: Collection[str]) -> dict[str, Any]:
    rule = match.rule
    supporting = [fact for _, facts in match.matched for fact in facts]
    picked: dict[str, Fact] = {}
    for _, facts in match.matched:
        for code in {fact.code for fact in facts}:
            picked[code] = _strongest([fact for fact in facts if fact.code == code])
    weight = sum(
        1.0 if any(fact.basis == "verified" for fact in facts) else INFERRED_CHECK_WEIGHT
        for _, facts in match.matched
    )
    depth = min(len(match.matched) - 1, FULL_PATH_DEPTH) / FULL_PATH_DEPTH
    confidence = BASE_CONFIDENCE + MATCHED_SHARE_WEIGHT * weight / len(rule.checks) + PATH_DEPTH_WEIGHT * depth
    not_supporting: list[dict[str, Any]] = []
    raise_confidence: list[str] = []
    for check in match.unmatched_optional:
        if set(check.sources) <= set(unread_sources):
            raise_confidence.append(f"{check.name}: {', '.join(check.sources)} was not read in this call.")
            continue
        below = [fact for fact in observed if fact.code in check.codes]
        entry: dict[str, Any] = {
            "check": check.name,
            "result": "below the bar" if below else "not found",
        }
        if below:
            entry["fact_id"] = _strongest(below).id
        not_supporting.append(entry)
        if check.raise_hint:
            raise_confidence.append(check.raise_hint)
    for source in sorted({fact.source for fact in supporting if fact.basis == "inferred"}):
        raise_confidence.append(
            _INFERRED_HINTS.get(source, f"The {source} fact is inferred; its next tool confirms it.")
        )
    required_facts = [fact for check, facts in match.matched if check.required for fact in facts]
    return {
        "id": "",
        "code": rule.code,
        "cause": rule.cause(picked),
        "confidence": round(min(confidence, CONFIDENCE_CEILING), 2),
        "severity": _strongest(required_facts).severity,
        "basis": "inferred",
        "supporting_facts": [fact.id for fact in supporting],
        "checked_not_supporting": not_supporting,
        "raise_confidence": raise_confidence,
        "fix": rule.fix(picked),
        "next_tools": rule.next_tools(picked),
    }


def _for_database(
    facts: list[Fact], unread_sources: Collection[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    observed = [fact for fact in facts if fact.state == "observed"]
    stories: list[dict[str, Any]] = []
    explained: set[int] = set()
    possible: dict[int, list[dict[str, Any]]] = {}
    for rule in RULES:
        match = _match(rule, observed)
        if match.fired:
            stories.append(_story(match, observed, unread_sources))
            explained.update(id(fact) for _, facts in match.matched for fact in facts)
            continue
        for check, check_facts in match.matched:
            if not check.required:
                continue
            for fact in check_facts:
                possible.setdefault(id(fact), []).append(
                    {"story": rule.code, "missing": list(match.missing_required)}
                )
    leads: list[dict[str, Any]] = []
    for fact in observed:
        if not fact.actionable or id(fact) in explained:
            continue
        causes = possible.get(id(fact), [])[:MAX_POSSIBLE_CAUSES]
        leads.append(
            {
                "fact_id": fact.id,
                "code": fact.code,
                "severity": fact.severity,
                "basis": fact.basis,
                "message": fact.message,
                "why_a_lead": (
                    "A cause needs a second fact, and it is missing; see possible_causes."
                    if causes
                    else "One fact with nothing in this call that corroborates it."
                ),
                "possible_causes": causes,
                "next_tools": list(fact.next_tools),
            }
        )
    return stories, leads


def build_stories(
    facts: list[Fact], *, unread_sources: Collection[str] = ()
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (stories, leads): causes ranked by confidence, then uncorroborated facts.

    ``unread_sources`` names the sources that were not read (unreadable, or not
    wired), so a check on them is reported as not observed and never as
    evidence against a cause.
    """

    stories: list[dict[str, Any]] = []
    leads: list[dict[str, Any]] = []
    for database in sorted({fact.database for fact in facts}):
        scoped = [fact for fact in facts if fact.database == database]
        found, open_leads = _for_database(scoped, unread_sources)
        stories.extend(found)
        leads.extend(open_leads)
    stories.sort(key=lambda story: (-story["confidence"], -_rank(story["severity"])))
    stories = stories[:MAX_STORIES]
    for index, story in enumerate(stories, start=1):
        story["id"] = f"S{index}"
    leads.sort(key=lambda lead: -_rank(lead["severity"]))
    return stories, leads[:MAX_LEADS]
