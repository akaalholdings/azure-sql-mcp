"""Result-status contract: tell an agent which kind of "nothing" a tool returned.

Every tool result carries ``result_status``:

- ``ok``: the source was read and returned data.
- ``empty``: the source was read successfully and held no matching rows. This is a
  true negative (for example, no deadlocks were captured in the readable window).
- ``unavailable``: the source exists on this engine but could not be read now
  (permission refused, timeout, transient failure). It is not an all-clear.
- ``not_supported``: the source does not exist on Azure SQL Database. Nothing can
  be enabled to obtain it; do not send anyone to fix it.
- ``precondition``: the source can exist, but a setup step is missing (an Extended
  Events session, Query Store READ_WRITE, master access). ``remediation`` names
  the exact step.

Services set an explicit status when they know better than the generic rule.
Otherwise ``empty`` is inferred only from a tool's declared primary row keys, never
from incidental lists such as ``warnings``.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import Any

RESULT_STATUS_KEY = "result_status"
RESULT_STATUS_REASON_KEY = "result_status_reason"
REMEDIATION_KEY = "remediation"


class ResultStatus(str, Enum):
    OK = "ok"
    EMPTY = "empty"
    UNAVAILABLE = "unavailable"
    NOT_SUPPORTED = "not_supported"
    PRECONDITION = "precondition"


RESULT_STATUS_VALUES = frozenset(status.value for status in ResultStatus)

# Tool name -> top-level keys that hold the tool's primary rows. A result is
# ``empty`` only when every declared key is present and empty.
PRIMARY_ROW_KEYS: dict[str, tuple[str, ...]] = {
    "get_active_sessions": ("sessions",),
    "get_top_queries": ("rows",),
    "analyze_index_recommendations": (
        "missing_indexes",
        "automatic_tuning_recommendations",
    ),
    "get_wait_stats": ("top_waits",),
    "get_query_wait_stats": ("query_wait_stats",),
    "get_currently_waiting_tasks": ("waiting_tasks",),
    "get_lock_details": ("locks",),
    "get_open_transactions": ("transactions",),
    "get_deadlock_history": ("deadlocks",),
    "get_tempdb_usage": ("sessions",),
    "get_memory_grants": ("grants",),
    "get_io_stats": ("files",),
    "get_resource_stats_history": ("history",),
    "check_statistics_health": ("all_stats",),
    "get_plan_cache_analysis": ("distribution",),
    "get_query_compilation_stats": ("queries",),
    "detect_parameter_sniffing": ("queries",),
    "detect_regressed_queries": ("recommendations",),
    "get_forced_plans": ("forced_plans",),
    "review_plan_enforcement": ("recommended_actions",),
    "list_learning_candidates": ("candidates",),
    "recall_lessons": ("lessons",),
    "get_query_store_trend": ("buckets",),
    "get_query_store_regressions": ("regressions",),
    "analyze_query_plan": ("findings",),
}

_DEFAULT_EMPTY_REASON = (
    "The source was read successfully and held no matching rows in the readable "
    "window. This is a true negative, not a collection failure."
)


def status_payload(
    status: ResultStatus,
    reason: str,
    *,
    remediation: str | None = None,
) -> dict[str, Any]:
    """Return the status fields a service merges into its payload."""

    payload: dict[str, Any] = {
        RESULT_STATUS_KEY: status.value,
        RESULT_STATUS_REASON_KEY: reason,
    }
    if remediation:
        payload[REMEDIATION_KEY] = remediation
    return payload


def infer_result_status(tool_name: str, payload: Mapping[str, Any]) -> ResultStatus:
    keys = PRIMARY_ROW_KEYS.get(tool_name)
    if not keys:
        return ResultStatus.OK
    values = [payload.get(key) for key in keys]
    if all(isinstance(value, list) and not value for value in values):
        return ResultStatus.EMPTY
    return ResultStatus.OK


def apply_result_status(tool_name: str, payload: Any) -> Any:
    """Stamp ``result_status`` on a dict result unless the service already did."""

    if not isinstance(payload, dict):
        return payload
    existing = payload.get(RESULT_STATUS_KEY)
    if isinstance(existing, str) and existing in RESULT_STATUS_VALUES:
        return payload
    status = infer_result_status(tool_name, payload)
    result = dict(payload)
    result[RESULT_STATUS_KEY] = status.value
    if status is ResultStatus.EMPTY and RESULT_STATUS_REASON_KEY not in result:
        result[RESULT_STATUS_REASON_KEY] = _DEFAULT_EMPTY_REASON
    return result
