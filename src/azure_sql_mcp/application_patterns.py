"""Application-side patterns: bad actor by shape, pile-ups, chatty callers, row-by-row calls.

The shape of a query decides the fix. A statement that runs very often and is
cheap each time is fixed in the application (fewer calls); a statement that runs
seldom and is expensive each time is fixed in its plan. Query Store gives the
counts per query_hash; the live requests give pile-ups and callers. Query Store
does not record which program ran a statement, so callers come only from the
requests running at the time of the call.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

from .incident_log import note_exception
from .observability import sanitize_error_message
from .result_status import ResultStatus
from .result_status import status_payload

# Gap review 2C item 14 (calibrated): only queries with 100 or more executions are
# scored; cost per execution runs from 50 ms to 2,000 ms of CPU, or from 5,000 to
# 250,000 logical reads.
MIN_EXECUTIONS_FOR_SEVERITY = 100
CPU_MS_PER_EXECUTION_BAR = 50.0
CPU_MS_PER_EXECUTION_HEAVY = 2_000.0
READS_PER_EXECUTION_BAR = 5_000.0
READS_PER_EXECUTION_HEAVY = 250_000.0
# Execution-count tiers above the calibrated floor of 100. Judgment constants.
EXECUTION_TIER_2 = 1_000
EXECUTION_TIER_3 = 10_000
# One call a second, sustained, is "frequent". Judgment constant.
FREQUENT_EXECUTIONS_PER_MINUTE = 60.0
# Design W11c step 12: 50 executions a second that return 1.5 rows or fewer each.
ROW_BY_ROW_EXECUTIONS_PER_SECOND = 50.0
ROW_BY_ROW_MAX_ROWS_PER_EXECUTION = 1.5
# One program with 80% of at least 5 running requests. Judgment constants.
CHATTY_CALLER_SHARE = 0.8
CHATTY_CALLER_MIN_REQUESTS = 5
# sessions.PILEUP_MIN_REQUESTS (3) makes a pile-up; 10 on one statement is a queue.
PILEUP_MEDIUM_REQUESTS = 10

_ADVICE = {
    "frequent_light": (
        "Each call is cheap; the count is the cost. Make fewer calls: cache the result, fetch a set in "
        "one statement, or batch. Tuning its plan gains little."
    ),
    "rare_heavy": (
        "Each run is expensive. Tune its plan: analyze_query_plan names the operator, and "
        "review_workload_indexes the index that removes the reads."
    ),
    "frequent_heavy": (
        "It is expensive and it runs often. Tune its plan first, because every run gains; then ask "
        "whether it must run this often."
    ),
    "moderate": "Neither the call count nor the cost per call stands out.",
}
_SEVERITY_BY_SCORE = ((4, "high"), (2, "medium"), (1, "low"))


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _query_id(row: Mapping[str, Any]) -> Any:
    return row.get("query_id", row.get("sample_query_id"))


def query_shape(row: Mapping[str, Any], window_minutes: int) -> dict[str, Any]:
    """Classify one top-queries row as frequent_light, rare_heavy, frequent_heavy or moderate."""

    executions = _number(row.get("executions"))
    cpu_ms = _number(row.get("total_cpu_us")) / executions / 1000.0 if executions else 0.0
    reads = _number(row.get("avg_logical_io_reads"))
    per_minute = executions / max(1, int(window_minutes))
    if cpu_ms >= CPU_MS_PER_EXECUTION_HEAVY or reads >= READS_PER_EXECUTION_HEAVY:
        cost = 2
    elif cpu_ms >= CPU_MS_PER_EXECUTION_BAR or reads >= READS_PER_EXECUTION_BAR:
        cost = 1
    else:
        cost = 0
    if cost == 2:
        shape = "frequent_heavy" if executions >= MIN_EXECUTIONS_FOR_SEVERITY else "rare_heavy"
    elif cost == 0 and per_minute >= FREQUENT_EXECUTIONS_PER_MINUTE:
        shape = "frequent_light"
    else:
        shape = "moderate"
    tier = 0
    if executions >= MIN_EXECUTIONS_FOR_SEVERITY:
        tier = 1 + (executions >= EXECUTION_TIER_2) + (executions >= EXECUTION_TIER_3)
    severity = next((name for floor, name in _SEVERITY_BY_SCORE if tier * cost >= floor), None)
    return {
        "shape": shape,
        "severity": severity,
        "executions": int(executions),
        "executions_per_minute": round(per_minute, 1),
        "cpu_ms_per_execution": round(cpu_ms, 2),
        "logical_reads_per_execution": round(reads, 1),
        "rows_per_execution": round(_number(row.get("avg_rowcount")), 2),
        "advice": _ADVICE[shape],
    }


def row_by_row(row: Mapping[str, Any], window_minutes: int) -> dict[str, Any] | None:
    """The row-by-row signature in Query Store: a very high rate of calls that return about one row."""

    executions = _number(row.get("executions"))
    per_second = executions / (max(1, int(window_minutes)) * 60.0)
    rows = row.get("avg_rowcount")
    if rows is None or per_second < ROW_BY_ROW_EXECUTIONS_PER_SECOND:
        return None
    if _number(rows) > ROW_BY_ROW_MAX_ROWS_PER_EXECUTION:
        return None
    return {
        "executions_per_second": round(per_second, 1),
        "rows_per_execution": round(_number(rows), 2),
        "executions": int(executions),
    }


def chatty_caller(load_by_caller: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The program that issues most of the running requests, or None. Host names are dropped."""

    by_program: dict[Any, int] = {}
    for caller in load_by_caller:
        program = caller.get("program_name")
        by_program[program] = by_program.get(program, 0) + int(_number(caller.get("request_count")))
    total = sum(by_program.values())
    if total < CHATTY_CALLER_MIN_REQUESTS:
        return None
    program, count = max(by_program.items(), key=lambda item: item[1])
    if count / total < CHATTY_CALLER_SHARE:
        return None
    return {"program_name": program, "request_count": count, "share_pct": round(100.0 * count / total, 1)}


def _pattern(
    code: str,
    severity: str,
    message: str,
    evidence: dict[str, Any],
    advice: str,
    next_tools: list[dict[str, Any]],
    basis: str = "verified",
) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "basis": basis,
        "message": message,
        "evidence": evidence,
        "advice": advice,
        "next_tools": next_tools,
    }


def query_patterns(rows: list[dict[str, Any]], window_minutes: int) -> list[dict[str, Any]]:
    """Row-by-row and shape patterns from top-queries rows grouped by query_hash."""

    patterns: list[dict[str, Any]] = []
    seen: set[Any] = set()
    for row in rows:
        key = row.get("query_hash") or _query_id(row)
        if key in seen:
            continue
        seen.add(key)
        shape = query_shape(row, window_minutes)
        identity = {"query_hash": row.get("query_hash"), "query_id": _query_id(row)}
        plan_tools = [{"tool": "analyze_query_plan", "arguments": {"query_id": _query_id(row)}}]
        loop = row_by_row(row, window_minutes)
        if loop is not None:
            patterns.append(
                _pattern(
                    "row_by_row_calls",
                    "medium",
                    f"Query hash {row.get('query_hash')} runs {loop['executions_per_second']:,.0f} times a "
                    f"second and returns {loop['rows_per_execution']} row(s) a call: a loop in the application "
                    "that fetches one row at a time.",
                    {**identity, **loop, "cpu_ms_per_execution": shape["cpu_ms_per_execution"]},
                    "Replace the loop with one set-based statement (a join, an IN list or a table-valued "
                    "parameter). The statement itself is already cheap.",
                    [{"tool": "get_query_store_trend", "arguments": {"query_id": _query_id(row)}}],
                    basis="inferred",
                )
            )
            continue
        if shape["shape"] == "frequent_light":
            patterns.append(
                _pattern(
                    "frequent_light_query",
                    "low",
                    f"Query hash {row.get('query_hash')} runs {shape['executions_per_minute']:,.0f} times a "
                    f"minute at {shape['cpu_ms_per_execution']} ms of CPU each.",
                    {**identity, **shape},
                    shape["advice"],
                    [{"tool": "get_query_store_trend", "arguments": {"query_id": _query_id(row)}}],
                )
            )
        elif shape["shape"] in {"rare_heavy", "frequent_heavy"}:
            patterns.append(
                _pattern(
                    f"{shape['shape']}_query",
                    shape["severity"] or "low",
                    f"Query hash {row.get('query_hash')} costs {shape['cpu_ms_per_execution']:,.0f} ms of CPU "
                    f"and {shape['logical_reads_per_execution']:,.0f} logical reads a run, "
                    f"{shape['executions']:,} run(s) in the window.",
                    {**identity, **shape},
                    shape["advice"],
                    plan_tools,
                )
            )
    return patterns


def session_patterns(sessions: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Pile-ups on one query hash and a chatty caller, from the running requests."""

    patterns: list[dict[str, Any]] = []
    for pileup in (sessions.get("pileups") or [])[:3]:
        count = int(_number(pileup.get("request_count")))
        patterns.append(
            _pattern(
                "query_pileup",
                "medium" if count >= PILEUP_MEDIUM_REQUESTS else "low",
                f"{count} requests run query hash {pileup.get('query_hash')} at the same time; "
                f"{int(_number(pileup.get('blocked_request_count')))} of them are blocked.",
                {
                    "query_hash": pileup.get("query_hash"),
                    "request_count": count,
                    "blocked_request_count": pileup.get("blocked_request_count"),
                    "window": "point_in_time",
                },
                "When they are blocked, find the head blocker. When they run, the statement is too slow "
                "for its call rate: tune it or limit the callers.",
                [{"tool": "get_active_sessions", "arguments": {}}],
            )
        )
    caller = chatty_caller(sessions.get("load_by_caller") or [])
    if caller is not None:
        patterns.append(
            _pattern(
                "chatty_caller",
                "low",
                f"Program {caller['program_name']} issues {caller['share_pct']}% of the "
                f"{caller['request_count']} or more requests running now.",
                {**caller, "window": "point_in_time"},
                "Start with this program: its connection pool size and its call pattern set the load.",
                [{"tool": "get_active_sessions", "arguments": {}}],
            )
        )
    return patterns


_SEVERITY_ORDER = {"high": 0, "medium": 1, "low": 2}


class ApplicationPatternService:
    def __init__(self, *, query_store: Any, sessions: Any):
        self.query_store = query_store
        self.sessions = sessions

    async def analyze(self, database_name: str, *, window_minutes: int = 60, top: int = 20) -> dict[str, Any]:
        """Patterns in how the application calls the database, each with the fix that fits it."""

        window_minutes = max(5, int(window_minutes))
        top = max(1, min(int(top), 50))
        reads = {
            "top_by_executions": self.query_store.get_top_queries(
                database_name, "executions", window_minutes, top, group_by="query_hash"
            ),
            "top_by_cpu": self.query_store.get_top_queries(
                database_name, "cpu", window_minutes, top, group_by="query_hash"
            ),
            "sessions": self.sessions.get_active_sessions(database_name, 200),
        }
        results = await asyncio.gather(*reads.values(), return_exceptions=True)
        sources: dict[str, str] = {}
        gaps: list[str] = []
        rows: list[dict[str, Any]] = []
        sessions: Mapping[str, Any] = {}
        for name, result in zip(reads, results, strict=True):
            if isinstance(result, BaseException):
                note_exception(result, f"application_patterns.{name}")
                sources[name] = ResultStatus.UNAVAILABLE.value
                gaps.append(f"{name}: {sanitize_error_message(str(result))}")
                continue
            status = result.get("result_status") if isinstance(result, Mapping) else None
            sources[name] = str(status or ResultStatus.OK.value)
            if status in {"unavailable", "precondition"}:
                gaps.append(f"{name}: {result.get('result_status_reason') or status}")
                continue
            if isinstance(result, list):
                rows.extend(result)
            elif isinstance(result, Mapping):
                sessions = result
        patterns = query_patterns(rows, window_minutes) + session_patterns(sessions)
        patterns.sort(key=lambda item: _SEVERITY_ORDER.get(item["severity"], 3))
        payload: dict[str, Any] = {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "patterns": patterns,
            "sources": sources,
            "gaps": gaps,
            "caller_scope": "Callers and pile-ups are the requests running at the time of the call.",
        }
        if len(gaps) == len(reads):
            payload.update(status_payload(ResultStatus.UNAVAILABLE, "No source could be read; see gaps."))
        elif gaps:
            payload.update(
                status_payload(
                    ResultStatus.OK,
                    f"{len(patterns)} pattern(s) from the readable sources. {len(gaps)} source(s) could "
                    "not be read, so this is not an all-clear; see gaps.",
                )
            )
        elif patterns:
            payload.update(status_payload(ResultStatus.OK, f"{len(patterns)} pattern(s) found."))
        else:
            payload.update(
                status_payload(ResultStatus.EMPTY, "No application pattern is above its bar in the window.")
            )
        return payload
