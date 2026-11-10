from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .azure_tier import gated_read_unavailable
from .connection import AzureSqlExecutor
from .incident_log import note_exception
from .query_store import _RELATIVE_WINDOW_PREDICATE
from .result_status import ResultStatus
from .result_status import status_payload

# Benign waits to exclude — these are background/idle waits that inflate totals
# without indicating real performance problems.
BENIGN_WAITS: frozenset[str] = frozenset(
    {
        "BROKER_EVENTHANDLER",
        "BROKER_RECEIVE_WAITFOR",
        "BROKER_TASK_STOP",
        "BROKER_TO_FLUSH",
        "BROKER_TRANSMITTER",
        "CHECKPOINT_QUEUE",
        "CHKPT",
        "CLR_AUTO_EVENT",
        "CLR_MANUAL_EVENT",
        "DIRTY_PAGE_POLL",
        "DISPATCHER_QUEUE_SEMAPHORE",
        "FT_IFTS_SCHEDULER_IDLE_WAIT",
        "FT_IFTSHC_MUTEX",
        "HADR_FILESTREAM_IOMGR_IOCOMPLETION",
        "HADR_WORK_QUEUE",
        "LAZYWRITER_SLEEP",
        "LOGMGR_QUEUE",
        "ONDEMAND_TASK_QUEUE",
        "PREEMPTIVE_OS_AUTHENTICATIONOPS",
        "PREEMPTIVE_OS_GETPROCADDRESS",
        "QDS_ASYNC_QUEUE",
        "QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP",
        "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP",
        "QDS_SHUTDOWN_QUEUE",
        "REQUEST_FOR_DEADLOCK_SEARCH",
        "SLEEP_BPOOL_FLUSH",
        "SLEEP_DBSTARTUP",
        "SLEEP_DCOMSTARTUP",
        "SLEEP_MASTERDBREADY",
        "SLEEP_MASTERMDREADY",
        "SLEEP_MASTERUPGRADED",
        "SLEEP_MSDBSTARTUP",
        "SLEEP_SYSTEMTASK",
        "SLEEP_TASK",
        "SP_SERVER_DIAGNOSTICS_SLEEP",
        "SQLTRACE_BUFFER_FLUSH",
        "SQLTRACE_INCREMENTAL_FLUSH_SLEEP",
        "WAIT_XTP_HOST_WAIT",
        "WAIT_XTP_OFFLINE_CKPT_NEW_LOG",
        "WAITFOR",
        "XE_DISPATCHER_WAIT",
        "XE_TIMER_EVENT",
    }
)

# Wait type → category mapping for Azure SQL DB
WAIT_CATEGORY_MAP: dict[str, str] = {
    "CXPACKET": "Parallelism",
    "CXCONSUMER": "Parallelism",
    "CXSYNC_PORT": "Parallelism",
    "CXSYNC_CONSUMER": "Parallelism",
    "SOS_SCHEDULER_YIELD": "CPU",
    "THREADPOOL": "CPU",
    "RESOURCE_SEMAPHORE": "Memory",
    "RESOURCE_SEMAPHORE_QUERY_COMPILE": "Memory",
    "CMEMTHREAD": "Memory",
    "PAGEIOLATCH_SH": "I/O",
    "PAGEIOLATCH_EX": "I/O",
    "PAGEIOLATCH_UP": "I/O",
    "PAGEIOLATCH_DT": "I/O",
    "PAGEIOLATCH_NL": "I/O",
    "PAGEIOLATCH_KP": "I/O",
    "IO_COMPLETION": "I/O",
    "ASYNC_IO_COMPLETION": "I/O",
    "WRITE_COMPLETION": "I/O",
    "WRITELOG": "Log I/O",
    "LOGBUFFER": "Log I/O",
    "LOG_RATE_GOVERNOR": "Log I/O",
    "POOL_LOG_RATE_GOVERNOR": "Log I/O",
    "HADR_THROTTLE_LOG_RATE_GOVERNOR": "Log I/O",
    "INSTANCE_LOG_RATE_GOVERNOR": "Log I/O",
    "LCK_M_SCH_S": "Lock",
    "LCK_M_SCH_M": "Lock",
    "LCK_M_S": "Lock",
    "LCK_M_U": "Lock",
    "LCK_M_X": "Lock",
    "LCK_M_IS": "Lock",
    "LCK_M_IU": "Lock",
    "LCK_M_IX": "Lock",
    "LCK_M_SIU": "Lock",
    "LCK_M_SIX": "Lock",
    "LCK_M_UIX": "Lock",
    "LCK_M_BU": "Lock",
    "LCK_M_RS_S": "Lock",
    "LCK_M_RS_U": "Lock",
    "LCK_M_RIn_NL": "Lock",
    "LCK_M_RIn_S": "Lock",
    "LCK_M_RIn_U": "Lock",
    "LCK_M_RIn_X": "Lock",
    "LCK_M_RX_S": "Lock",
    "LCK_M_RX_U": "Lock",
    "LCK_M_RX_X": "Lock",
    "PAGELATCH_SH": "Latch",
    "PAGELATCH_EX": "Latch",
    "PAGELATCH_UP": "Latch",
    "PAGELATCH_DT": "Latch",
    "PAGELATCH_NL": "Latch",
    "PAGELATCH_KP": "Latch",
    "LATCH_SH": "Latch",
    "LATCH_EX": "Latch",
    "LATCH_UP": "Latch",
    "LATCH_DT": "Latch",
    "LATCH_NL": "Latch",
    "LATCH_KP": "Latch",
    "ASYNC_NETWORK_IO": "Network",
    "OLEDB": "Network",
    "EXTERNAL_SCRIPT_NETWORK_IOF": "Network",
    "SE_REPL_CATCHUP_THROTTLE": "Replication",
    "SE_REPL_COMMIT_ACK": "Replication",
    "SE_REPL_COMMIT_TURN": "Replication",
    "SE_REPL_ROLLBACK_ACK": "Replication",
    "SE_REPL_SLOW_SECONDARY_THROTTLE": "Replication",
}

# Root cause recommendations per category
CATEGORY_RECOMMENDATIONS: dict[str, str] = {
    "CPU": "Tune expensive queries, check for missing indexes, consider scaling up compute tier.",
    "I/O": "Check for missing indexes (scans), large sorts/spills, or under-provisioned storage IOPS.",
    "Log I/O": "Reduce transaction log volume: batch smaller commits, check LOG_RATE_GOVERNOR (Azure throttling), consider scaling tier.",
    "Lock": "Investigate blocking chains (get_active_sessions), shorten transactions, use READ COMMITTED SNAPSHOT.",
    "Latch": "Page latch contention often indicates hot pages — check last-page insert patterns on identity columns.",
    "Memory": "RESOURCE_SEMAPHORE means queries waiting for memory grants. Check for large sorts/hashes, reduce query concurrency, or scale up.",
    "Network": "ASYNC_NETWORK_IO means waiting for client to consume results. Check client-side processing or network latency.",
    "Parallelism": "High CXPACKET/CXCONSUMER indicates parallel query skew. Check for stale stats or consider MAXDOP tuning.",
    "Replication": "Geo-replication or readable secondary commit acknowledgement delay. Check secondary health.",
}


# THREADPOOL is a worker-thread limit, not CPU: the category value stays for
# compatibility, the advice does not send anyone to scale up first.
WAIT_RECOMMENDATIONS: dict[str, str] = {
    "THREADPOOL": (
        "Requests are waiting for a worker thread (error 10928 when it lasts). With parallelism "
        "waits beside it, lower MAXDOP or fix the parallel plans; with blocking beside it, the "
        "blocked sessions hold the workers, so clear the head blocker. Check get_active_sessions "
        "and max_worker_percent before any change of service objective."
    ),
}


@dataclass(frozen=True)
class WaitBar:
    """Absolute bar for one wait type, in wait seconds per observed second."""

    above_bar: float
    high: float | None = None
    # THREADPOOL counts only when the waits are also long on average.
    min_avg_wait_ms: float | None = None


# Calibrated list from the gap review (2C item 10): wait time divided by observed
# time. Calibrated on a SQL Server fleet; the log governor and worker caps of
# Azure SQL Database have not been checked against it.
_EXACT_WAIT_BARS: dict[str, WaitBar] = {
    "SOS_SCHEDULER_YIELD": WaitBar(0.75),
    "RESOURCE_SEMAPHORE": WaitBar(0.01, 0.10),
    "RESOURCE_SEMAPHORE_QUERY_COMPILE": WaitBar(0.01, 0.10),
    "WRITELOG": WaitBar(0.25, 0.50),
    "LCK_M_S": WaitBar(0.05),
    "LCK_M_IS": WaitBar(0.05),
    "LCK_M_SCH_M": WaitBar(0.01, 0.10),
    "PAGELATCH_UP": WaitBar(round(10 / 3600, 5)),  # 10 seconds per hour
    "THREADPOOL": WaitBar(0.25, min_avg_wait_ms=1000),  # 15 minutes per hour and 1 s average
}
_PREFIX_WAIT_BARS: tuple[tuple[str, WaitBar], ...] = (
    ("LCK_M_R", WaitBar(0.01, 0.10)),  # range locks
    ("LCK_M_", WaitBar(0.10)),
    ("CX", WaitBar(0.25)),
    ("PAGEIOLATCH_", WaitBar(0.25)),
    ("LATCH_", WaitBar(0.25)),
)
ABSOLUTE_BARS_NOTE = {
    "unit": "wait_seconds_per_observed_second",
    "meaning": (
        "Wait time divided by observed time (sample_seconds, or the time since the counters "
        "started). absolute_level compares it with the bar for that wait type; pct_of_total is "
        "only the share among the waits seen."
    ),
    "calibration": (
        "Bars come from a SQL Server fleet calibration and are not yet checked against Azure SQL "
        "Database log-rate and worker governance."
    ),
}


def absolute_wait_bar(wait_type: str) -> WaitBar | None:
    """The calibrated absolute bar for ``wait_type``, or None when there is none."""

    if wait_type in _EXACT_WAIT_BARS:
        return _EXACT_WAIT_BARS[wait_type]
    for prefix, bar in _PREFIX_WAIT_BARS:
        if wait_type.startswith(prefix):
            return bar
    return None


def absolute_wait_level(wait_type: str, ratio: float | None, avg_wait_ms: float | None) -> str:
    """no_bar, unknown_observed_time, below_bar, above_bar or high."""

    bar = absolute_wait_bar(wait_type)
    if bar is None:
        return "no_bar"
    if ratio is None:
        return "unknown_observed_time"
    if bar.min_avg_wait_ms is not None and (avg_wait_ms or 0.0) < bar.min_avg_wait_ms:
        return "below_bar"
    if bar.high is not None and ratio >= bar.high:
        return "high"
    return "above_bar" if ratio >= bar.above_bar else "below_bar"


def classify_wait(wait_type: str) -> str:
    """Return the category for a wait type, or 'Other' if unknown."""
    if wait_type in WAIT_CATEGORY_MAP:
        return WAIT_CATEGORY_MAP[wait_type]
    # Prefix-based classification for families not individually listed
    if wait_type.startswith("LCK_M_"):
        return "Lock"
    if wait_type.startswith("PAGEIOLATCH_"):
        return "I/O"
    if wait_type.startswith("PAGELATCH_"):
        return "Latch"
    if wait_type.startswith("LATCH_"):
        return "Latch"
    if wait_type.startswith("PREEMPTIVE_"):
        return "Preemptive"
    return "Other"


MAX_WAIT_SAMPLE_SECONDS = 30

WAIT_SNAPSHOT_SQL = """
SELECT
    wait_type,
    waiting_tasks_count,
    wait_time_ms,
    max_wait_time_ms,
    signal_wait_time_ms,
    wait_time_ms - signal_wait_time_ms AS resource_wait_time_ms
FROM sys.dm_db_wait_stats
WHERE waiting_tasks_count > 0
ORDER BY wait_time_ms DESC
"""

WAIT_STATS_DMV = "sys.dm_db_wait_stats"
WAITING_TASKS_DMV = "sys.dm_os_waiting_tasks"
QUERY_WAIT_STATS_VIEW = "sys.query_store_wait_stats"

WAIT_CAPTURE_STATE_SQL = """
SELECT
    actual_state_desc,
    wait_stats_capture_mode_desc
FROM sys.database_query_store_options
"""

COUNTER_EPOCH_SQL = """
SELECT
    CONVERT(varchar(33), sqlserver_start_time, 127) AS engine_start_time_utc,
    DATEDIFF(SECOND, sqlserver_start_time, SYSDATETIME()) AS seconds_since_engine_start
FROM sys.dm_os_sys_info
"""


def wait_deltas(
    first: list[dict[str, Any]], second: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], bool]:
    """Return per-wait deltas between two snapshots, largest wait time first.

    A counter that went backwards means the counters were reset between the
    snapshots; the second snapshot is then the best available interval figure.
    """

    before = {row.get("wait_type"): row for row in first}
    reset = False
    deltas: list[dict[str, Any]] = []
    for row in second:
        wait_type = row.get("wait_type")
        prior = before.get(wait_type, {})
        tasks = int(row.get("waiting_tasks_count") or 0) - int(prior.get("waiting_tasks_count") or 0)
        wait_ms = int(row.get("wait_time_ms") or 0) - int(prior.get("wait_time_ms") or 0)
        signal_ms = int(row.get("signal_wait_time_ms") or 0) - int(prior.get("signal_wait_time_ms") or 0)
        if tasks < 0 or wait_ms < 0 or signal_ms < 0:
            reset = True
            break
        if tasks > 0 or wait_ms > 0:
            deltas.append(
                {
                    "wait_type": wait_type,
                    "waiting_tasks_count": tasks,
                    "wait_time_ms": wait_ms,
                    "signal_wait_time_ms": signal_ms,
                    "resource_wait_time_ms": wait_ms - signal_ms,
                }
            )
    if reset:
        deltas = [
            {
                "wait_type": row.get("wait_type"),
                "waiting_tasks_count": int(row.get("waiting_tasks_count") or 0),
                "wait_time_ms": int(row.get("wait_time_ms") or 0),
                "signal_wait_time_ms": int(row.get("signal_wait_time_ms") or 0),
                "resource_wait_time_ms": int(row.get("wait_time_ms") or 0)
                - int(row.get("signal_wait_time_ms") or 0),
            }
            for row in second
            if int(row.get("waiting_tasks_count") or 0) > 0
        ]
    deltas.sort(key=lambda row: row["wait_time_ms"], reverse=True)
    return deltas, reset


class WaitStatsService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_wait_stats(
        self,
        database_name: str,
        top_n: int = 20,
        *,
        sample_seconds: int = 0,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    ) -> dict[str, Any]:
        """Top waits from sys.dm_db_wait_stats with category mapping and recommendations.

        ``sample_seconds=0`` returns counters accumulated since the last reset
        (failover, scaling, restart, or a manual clear), which can dilute a recent
        problem. A positive value takes two snapshots that many seconds apart and
        returns only the waits that accrued in between.
        """
        sample_seconds = max(0, min(int(sample_seconds), MAX_WAIT_SAMPLE_SECONDS))
        observed_seconds: int | None
        try:
            first = await self.executor.fetch_all(database_name, WAIT_SNAPSHOT_SQL)
        except Exception as exc:
            blocked = await gated_read_unavailable(
                self.executor, database_name, WAIT_STATS_DMV, exc, site="wait_stats.snapshot"
            )
            if blocked is None:
                raise
            return {
                "database_name": database_name,
                "total_wait_time_ms": 0,
                "top_waits": [],
                "categories": [],
                **blocked,
            }
        if sample_seconds == 0:
            rows = first
            window, observed_seconds = await self._cumulative_window(database_name)
        else:
            await sleep(sample_seconds)
            second = await self.executor.fetch_all(database_name, WAIT_SNAPSHOT_SQL)
            rows, reset = wait_deltas(first, second)
            window = {
                "mode": "interval",
                "sample_seconds": sample_seconds,
                "counter_reset_detected": reset,
            }
            observed_seconds = sample_seconds

        def per_observed_second(wait_ms: Any) -> float | None:
            if not observed_seconds:
                return None
            return round(float(wait_ms or 0) / (observed_seconds * 1000.0), 4)

        # Filter out benign waits
        filtered = [r for r in rows if r.get("wait_type") not in BENIGN_WAITS]

        total_wait_ms = sum(r.get("wait_time_ms", 0) for r in filtered) or 1

        waits: list[dict[str, Any]] = []
        for row in filtered[:top_n]:
            wait_type = row.get("wait_type", "")
            wait_ms = row.get("wait_time_ms", 0)
            category = classify_wait(wait_type)
            tasks = row.get("waiting_tasks_count") or 0
            avg_wait_ms = round(wait_ms / tasks, 1) if tasks else None
            ratio = per_observed_second(wait_ms)
            bar = absolute_wait_bar(wait_type)
            waits.append(
                {
                    **row,
                    "category": category,
                    "pct_of_total": round(wait_ms / total_wait_ms * 100, 2),
                    "avg_wait_ms": avg_wait_ms,
                    "wait_seconds_per_observed_second": ratio,
                    "absolute_bar": bar.above_bar if bar else None,
                    "absolute_level": absolute_wait_level(wait_type, ratio, avg_wait_ms),
                    "recommendation": WAIT_RECOMMENDATIONS.get(
                        wait_type, CATEGORY_RECOMMENDATIONS.get(category, "")
                    ),
                }
            )

        # Aggregate by category
        category_totals: dict[str, dict[str, Any]] = {}
        for w in waits:
            cat = w["category"]
            if cat not in category_totals:
                category_totals[cat] = {
                    "category": cat,
                    "total_wait_time_ms": 0,
                    "total_tasks": 0,
                    "recommendation": CATEGORY_RECOMMENDATIONS.get(cat, ""),
                }
            category_totals[cat]["total_wait_time_ms"] += w.get("wait_time_ms", 0)
            category_totals[cat]["total_tasks"] += w.get("waiting_tasks_count", 0)

        for cat_info in category_totals.values():
            cat_info["pct_of_total"] = round(
                cat_info["total_wait_time_ms"] / total_wait_ms * 100, 2
            )
            cat_info["wait_seconds_per_observed_second"] = per_observed_second(
                cat_info["total_wait_time_ms"]
            )

        categories_sorted = sorted(
            category_totals.values(),
            key=lambda c: c["total_wait_time_ms"],
            reverse=True,
        )

        return {
            "database_name": database_name,
            "source_dmv": WAIT_STATS_DMV,
            "total_wait_time_ms": total_wait_ms,
            "observed_seconds": observed_seconds,
            "absolute_bars": ABSOLUTE_BARS_NOTE,
            "top_waits": waits,
            "categories": categories_sorted,
            "window": window,
        }

    async def _cumulative_window(self, database_name: str) -> tuple[dict[str, Any], int | None]:
        """The cumulative window and its length in seconds (None when the epoch is unreadable)."""

        window: dict[str, Any] = {
            "mode": "cumulative",
            "since_utc": None,
            "note": (
                "Counters accumulate since the last reset (failover, scaling, restart, "
                "or a manual clear) and can dilute a recent problem. Pass "
                "sample_seconds for a live interval."
            ),
        }
        try:
            rows = await self.executor.fetch_all(database_name, COUNTER_EPOCH_SQL)
        except Exception:
            return window, None
        if rows and rows[0].get("engine_start_time_utc"):
            window["since_utc"] = rows[0]["engine_start_time_utc"]
        seconds = rows[0].get("seconds_since_engine_start") if rows else None
        return window, int(seconds) if seconds else None

    async def get_query_wait_stats(
        self,
        database_name: str,
        window_minutes: int = 60,
        top_n: int = 20,
    ) -> dict[str, Any]:
        """Per-query wait breakdown from Query Store wait stats.

        An interval counts when it ends inside the window (the same predicate as
        the other Query Store tools), so the closed interval that holds most of
        the window is not dropped. ``interval_coverage`` is the time the returned
        rows really cover.
        """
        # avg_query_wait_time_ms is per execution, so total / avg is the row's
        # execution count: SUM(total) / SUM(total / avg) is the execution-weighted
        # mean, SUM(avg * executions) / SUM(executions).
        query = f"""
        SELECT TOP (?)
            q.query_id,
            qt.query_sql_text,
            ws.wait_category_desc,
            SUM(ws.total_query_wait_time_ms) AS total_wait_time_ms,
            SUM(ws.total_query_wait_time_ms)
                / NULLIF(SUM(ws.total_query_wait_time_ms / NULLIF(ws.avg_query_wait_time_ms, 0)), 0)
                AS weighted_avg_wait_ms,
            MAX(ws.max_query_wait_time_ms) AS max_wait_time_ms,
            MIN(rsi.start_time) AS first_interval_start_utc,
            MAX(rsi.end_time) AS last_interval_end_utc
        FROM sys.query_store_wait_stats AS ws
        INNER JOIN sys.query_store_plan AS p
            ON ws.plan_id = p.plan_id
        INNER JOIN sys.query_store_query AS q
            ON p.query_id = q.query_id
        INNER JOIN sys.query_store_query_text AS qt
            ON q.query_text_id = qt.query_text_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON ws.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE {_RELATIVE_WINDOW_PREDICATE}
        GROUP BY q.query_id, qt.query_sql_text, ws.wait_category_desc
        ORDER BY total_wait_time_ms DESC
        """

        try:
            rows = await self.executor.fetch_all(
                database_name, query, params=[int(top_n), int(window_minutes)]
            )
        except Exception as exc:
            blocked = await gated_read_unavailable(
                self.executor, database_name, QUERY_WAIT_STATS_VIEW, exc, site="wait_stats.query_waits"
            )
            if blocked is None:
                raise
            return {
                "database_name": database_name,
                "window_minutes": window_minutes,
                "query_wait_stats": [],
                **blocked,
            }
        starts = [row["first_interval_start_utc"] for row in rows if row.get("first_interval_start_utc")]
        ends = [row["last_interval_end_utc"] for row in rows if row.get("last_interval_end_utc")]
        payload: dict[str, Any] = {
            "database_name": database_name,
            "source_dmv": QUERY_WAIT_STATS_VIEW,
            "window_minutes": window_minutes,
            "interval_coverage": {
                "start_utc": min(starts) if starts else None,
                "end_utc": max(ends) if ends else None,
            },
            "query_wait_stats": rows,
        }
        if not rows:
            payload.update(await self._empty_wait_capture_status(database_name))
        return payload

    async def _empty_wait_capture_status(self, database_name: str) -> dict[str, Any]:
        """Why no Query Store wait row came back: nothing waited, or nothing was captured."""

        try:
            options = await self.executor.fetch_all(database_name, WAIT_CAPTURE_STATE_SQL)
        except Exception as exc:
            note_exception(exc, "wait_stats.capture_state")
            options = []
        row = options[0] if options else {}
        state = str(row.get("actual_state_desc") or "unknown")
        capture = str(row.get("wait_stats_capture_mode_desc") or "unknown")
        result: dict[str, Any] = {
            "capture": {"query_store_state": state, "wait_stats_capture_mode": capture}
        }
        if state.upper() == "OFF":
            result.update(
                status_payload(
                    ResultStatus.PRECONDITION,
                    "Query Store is off, so no query waits are recorded.",
                    remediation="ALTER DATABASE CURRENT SET QUERY_STORE = ON;",
                )
            )
        elif capture.upper() == "OFF":
            result.update(
                status_payload(
                    ResultStatus.PRECONDITION,
                    "Query Store wait capture is off, so no query waits are recorded. An empty "
                    "result does not mean that nothing waited.",
                    remediation="ALTER DATABASE CURRENT SET QUERY_STORE (WAIT_STATS_CAPTURE_MODE = ON);",
                )
            )
        return result

    async def get_currently_waiting_tasks(
        self,
        database_name: str,
    ) -> dict[str, Any]:
        """Active waits right now from sys.dm_os_waiting_tasks."""
        query = """
        SELECT
            wt.session_id,
            wt.wait_type,
            wt.wait_duration_ms,
            wt.blocking_session_id,
            wt.resource_description,
            r.command,
            r.status AS request_status,
            r.cpu_time AS cpu_time_ms,
            r.total_elapsed_time AS elapsed_time_ms,
            SUBSTRING(
                st.text,
                (r.statement_start_offset / 2) + 1,
                (
                    CASE r.statement_end_offset
                        WHEN -1 THEN DATALENGTH(st.text)
                        ELSE r.statement_end_offset
                    END - r.statement_start_offset
                ) / 2 + 1
            ) AS current_statement
        FROM sys.dm_os_waiting_tasks AS wt
        INNER JOIN sys.dm_exec_requests AS r
            ON wt.session_id = r.session_id
        INNER JOIN sys.dm_exec_sessions AS s
            ON wt.session_id = s.session_id
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
        WHERE wt.session_id != @@SPID
          AND s.is_user_process = 1
        ORDER BY wt.wait_duration_ms DESC
        """
        try:
            rows = await self.executor.fetch_all(database_name, query)
        except Exception as exc:
            blocked = await gated_read_unavailable(
                self.executor, database_name, WAITING_TASKS_DMV, exc, site="wait_stats.waiting_tasks"
            )
            if blocked is None:
                raise
            return {
                "database_name": database_name,
                "currently_waiting_count": 0,
                "waiting_tasks": [],
                **blocked,
            }

        for row in rows:
            wait_type = row.get("wait_type", "")
            category = classify_wait(wait_type)
            row["category"] = category
            row["recommendation"] = WAIT_RECOMMENDATIONS.get(
                wait_type, CATEGORY_RECOMMENDATIONS.get(category, "")
            )

        return {
            "database_name": database_name,
            "source_dmv": WAITING_TASKS_DMV,
            "collection_window": {"mode": "point_in_time"},
            "currently_waiting_count": len(rows),
            "waiting_tasks": rows,
        }
