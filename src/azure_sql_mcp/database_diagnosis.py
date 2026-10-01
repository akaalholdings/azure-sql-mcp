"""One-call database diagnosis: gather evidence, rank findings, name next steps.

Reads several sources at once (resource use against limits, waits, blocking,
top Query Store consumers, regressions, version store), applies simple,
explained rules, and returns findings ordered by severity. Every finding names
the tool calls that drill into it. A source that cannot be read is reported as a
gap and never turned into "healthy".
"""

from __future__ import annotations

import asyncio
from typing import Any

from .observability import sanitize_error_message
from .result_status import ResultStatus
from .result_status import status_payload
from .wait_stats import classify_wait

SEVERITY_RANK = {"high": 3, "medium": 2, "low": 1, "info": 0}

LOG_GOVERNOR_WAITS = {
    "LOG_RATE_GOVERNOR",
    "POOL_LOG_RATE_GOVERNOR",
    "HADR_THROTTLE_LOG_RATE_GOVERNOR",
    "INSTANCE_LOG_RATE_GOVERNOR",
}

_RESOURCE_RULES: tuple[tuple[str, str, str, list[dict[str, Any]]], ...] = (
    (
        "avg_cpu_percent",
        "cpu_pressure",
        "CPU is near the database's limit",
        [
            {"tool": "get_top_queries", "arguments": {"sort_by": "cpu"}},
            {"tool": "get_query_store_regressions", "arguments": {"metric": "cpu"}},
        ],
    ),
    (
        "avg_data_io_percent",
        "data_io_pressure",
        "Data I/O is near the database's limit",
        [
            {"tool": "get_top_queries", "arguments": {"sort_by": "logical_io"}},
            {"tool": "review_workload_indexes", "arguments": {"objective": "logical_reads"}},
        ],
    ),
    (
        "avg_log_write_percent",
        "log_write_pressure",
        "Log writes are near the log rate limit",
        [
            {"tool": "get_resource_limits", "arguments": {}},
            {"tool": "get_top_queries", "arguments": {"sort_by": "total_duration"}},
        ],
    ),
    (
        "max_worker_percent",
        "worker_pressure",
        "Worker threads are near the database's limit",
        [
            {"tool": "get_active_sessions", "arguments": {}},
            {"tool": "get_currently_waiting_tasks", "arguments": {}},
        ],
    ),
)


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
    ):
        self.resource_governance = resource_governance
        self.wait_stats = wait_stats
        self.sessions = sessions
        self.query_store = query_store
        self.query_store_trends = query_store_trends
        self.version_store = version_store

    async def diagnose(
        self,
        database_name: str,
        *,
        window_minutes: int = 60,
        sample_seconds: int = 0,
        master_available: bool = False,
    ) -> dict[str, Any]:
        window_minutes = max(5, min(int(window_minutes), 7 * 24 * 60))
        sources = {
            "resource_history": self.resource_governance.get_resource_stats_history(
                database_name, window_minutes, master_available=master_available
            ),
            "resource_limits": self.resource_governance.get_resource_limits(database_name),
            "waits": self.wait_stats.get_wait_stats(
                database_name, 10, sample_seconds=sample_seconds
            ),
            "sessions": self.sessions.get_active_sessions(database_name, 200),
            "top_queries": self.query_store.get_top_queries(
                database_name, "cpu", window_minutes, 5
            ),
            "regressions": self.query_store_trends.regressions(
                database_name, recent_minutes=window_minutes, top=5
            ),
            "version_store": self.version_store.get_version_store_stats(database_name),
        }
        results = await asyncio.gather(*sources.values(), return_exceptions=True)
        evidence: dict[str, Any] = {}
        source_status: dict[str, str] = {}
        gaps: list[str] = []
        for name, result in zip(sources, results, strict=True):
            if isinstance(result, BaseException):
                source_status[name] = "unavailable"
                gaps.append(f"{name}: {sanitize_error_message(str(result))}")
                continue
            status = result.get("result_status") if isinstance(result, dict) else None
            source_status[name] = str(status or "ok")
            if status in {"unavailable", "precondition", "not_supported"}:
                reason = result.get("result_status_reason") if isinstance(result, dict) else None
                gaps.append(f"{name}: {reason or status}")
            evidence[name] = result
        findings = diagnose_findings(evidence)
        findings.sort(key=lambda item: -SEVERITY_RANK.get(item["severity"], 0))
        read = [name for name, status in source_status.items() if status in {"ok", "empty"}]
        payload: dict[str, Any] = {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "sample_seconds": sample_seconds,
            "verdict": (
                "findings" if findings else ("no_findings" if len(read) == len(sources) else "partial")
            ),
            "findings": findings,
            "sources": source_status,
            "facts": _facts(evidence),
            "gaps": gaps,
        }
        if not read:
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
                    f"Read {len(read)} of {len(sources)} sources; {len(findings)} finding(s).",
                )
            )
        return payload


def diagnose_findings(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    findings.extend(_resource_findings(evidence.get("resource_history") or {}))
    findings.extend(_wait_findings(evidence.get("waits") or {}))
    findings.extend(_blocking_findings(evidence.get("sessions") or {}))
    findings.extend(_top_query_findings(evidence.get("top_queries")))
    findings.extend(_regression_findings(evidence.get("regressions") or {}))
    findings.extend(_version_store_findings(evidence.get("version_store") or {}))
    return findings


def _resource_findings(history: dict[str, Any]) -> list[dict[str, Any]]:
    summary = history.get("summary") or {}
    findings = []
    for metric, code, title, next_tools in _RESOURCE_RULES:
        stats = summary.get(metric)
        if not isinstance(stats, dict):
            continue
        avg = float(stats.get("avg") or 0.0)
        peak = float(stats.get("max") or 0.0)
        if peak >= 95 or avg >= 80:
            severity = "high"
        elif peak >= 85 or avg >= 60:
            severity = "medium"
        else:
            continue
        findings.append(
            _finding(
                code,
                severity,
                f"{title} (average {avg:.0f}%, peak {peak:.0f}% of the limit).",
                evidence={"metric": metric, "avg_pct": avg, "max_pct": peak, "source": history.get("source")},
                next_tools=next_tools,
            )
        )
    return findings


def _wait_findings(waits: dict[str, Any]) -> list[dict[str, Any]]:
    top = waits.get("top_waits") or []
    if not top:
        return []
    findings = []
    governor = [row for row in top if row.get("wait_type") in LOG_GOVERNOR_WAITS]
    if governor:
        findings.append(
            _finding(
                "log_rate_governor_waits",
                "high",
                "Queries are waiting on the log rate governor: log generation is hitting "
                "the service objective's cap.",
                evidence={"waits": [row.get("wait_type") for row in governor]},
                next_tools=[
                    {"tool": "get_resource_limits", "arguments": {}},
                    {"tool": "get_resource_stats_history", "arguments": {"window_minutes": 60}},
                ],
            )
        )
    leader = top[0]
    share = float(leader.get("pct_of_total") or 0.0)
    wait_type = str(leader.get("wait_type") or "")
    if share >= 30 and wait_type not in LOG_GOVERNOR_WAITS:
        category = classify_wait(wait_type)
        findings.append(
            _finding(
                "dominant_wait",
                "medium",
                f"{wait_type} ({category}) accounts for {share:.0f}% of waits"
                + (" since the counters reset." if (waits.get("window") or {}).get("mode") == "cumulative" else " in the sample."),
                evidence={"wait_type": wait_type, "category": category, "pct_of_total": share, "window": waits.get("window")},
                next_tools=_wait_next_tools(category),
            )
        )
    return findings


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


def _blocking_findings(sessions: dict[str, Any]) -> list[dict[str, Any]]:
    chains = sessions.get("blocking_chains") or []
    if not chains:
        return []
    blocked = sum(len(chain.get("blocked_sessions") or []) for chain in chains)
    head = chains[0].get("head_blocker_session_id")
    return [
        _finding(
            "active_blocking",
            "high" if blocked >= 5 else "medium",
            f"{len(chains)} blocking chain(s) right now; {blocked} session(s) blocked, "
            f"head blocker session {head}.",
            evidence={
                "chains": len(chains),
                "blocked_sessions": blocked,
                "head_blockers": [chain.get("head_blocker_session_id") for chain in chains[:5]],
            },
            next_tools=[
                {"tool": "get_lock_details", "arguments": {}},
                {"tool": "get_open_transactions", "arguments": {}},
            ],
        )
    ]


def _top_query_findings(top_queries: Any) -> list[dict[str, Any]]:
    rows = top_queries if isinstance(top_queries, list) else []
    total = sum(float(row.get("total_cpu_us") or 0.0) for row in rows)
    if not rows or total <= 0:
        return []
    leader = rows[0]
    share = 100.0 * float(leader.get("total_cpu_us") or 0.0) / total
    if share < 40:
        return []
    query_id = leader.get("query_id")
    return [
        _finding(
            "dominant_query",
            "medium",
            f"Query {query_id} uses {share:.0f}% of the CPU among the top five queries in the window.",
            evidence={"query_id": query_id, "plan_id": leader.get("plan_id"), "share_of_top5_cpu_pct": round(share, 1)},
            next_tools=[
                {"tool": "analyze_query_plan", "arguments": {"query_id": query_id}},
                {"tool": "get_query_store_trend", "arguments": {"query_id": query_id}},
            ],
        )
    ]


def _regression_findings(regressions: dict[str, Any]) -> list[dict[str, Any]]:
    findings = []
    for item in (regressions.get("regressions") or [])[:3]:
        pct = float(item.get("regression_pct") or 0.0)
        if pct < 50:
            continue
        query_id = item.get("query_id")
        findings.append(
            _finding(
                "query_regression",
                "high" if pct >= 200 or item.get("plan_changed") else "medium",
                f"Query {query_id} is {pct:.0f}% slower than its baseline"
                + (" and runs a new plan." if item.get("plan_changed") else "."),
                evidence={
                    "query_id": query_id,
                    "regression_pct": pct,
                    "new_plan_ids": item.get("new_plan_ids"),
                    "extra_cost": item.get("extra_cost"),
                },
                next_tools=[
                    {"tool": "get_query_store_trend", "arguments": {"query_id": query_id}},
                    {"tool": "analyze_query_plan", "arguments": {"query_id": query_id}},
                    {"tool": "plan_health_review", "arguments": {}},
                ],
            )
        )
    return findings


def _version_store_findings(version_store: dict[str, Any]) -> list[dict[str, Any]]:
    findings = []
    for item in version_store.get("findings") or []:
        if item.get("severity") not in {"high", "medium"}:
            continue
        findings.append(
            _finding(
                item.get("code", "version_store"),
                item["severity"],
                str(item.get("message") or ""),
                evidence=item.get("evidence") or {},
                next_tools=[{"tool": tool, "arguments": {}} for tool in item.get("next_tools") or []],
            )
        )
    return findings


def _finding(
    code: str,
    severity: str,
    message: str,
    *,
    evidence: dict[str, Any],
    next_tools: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "message": message,
        "evidence": evidence,
        "next_tools": next_tools,
    }


def _facts(evidence: dict[str, Any]) -> dict[str, Any]:
    history = evidence.get("resource_history") or {}
    limits = evidence.get("resource_limits") or {}
    waits = evidence.get("waits") or {}
    sessions = evidence.get("sessions") or {}
    top = evidence.get("top_queries")
    regressions = evidence.get("regressions") or {}
    pvs = (evidence.get("version_store") or {}).get("persistent_version_store") or {}
    return {
        "resource_summary": history.get("summary"),
        "resource_source": history.get("source"),
        "service_objective": limits.get("service_objective"),
        "top_waits": [
            {key: row.get(key) for key in ("wait_type", "category", "wait_time_ms", "pct_of_total")}
            for row in (waits.get("top_waits") or [])[:5]
        ],
        "wait_window": waits.get("window"),
        "blocking_chains": len(sessions.get("blocking_chains") or []),
        "active_sessions": sessions.get("active_session_count"),
        "top_queries_by_cpu": [
            {key: row.get(key) for key in ("query_id", "plan_id", "executions", "total_cpu_us")}
            for row in (top if isinstance(top, list) else [])[:5]
        ],
        "regressions": [
            {key: item.get(key) for key in ("query_id", "regression_pct", "plan_changed")}
            for item in (regressions.get("regressions") or [])[:5]
        ],
        "version_store_mb": pvs.get("size_mb"),
    }
