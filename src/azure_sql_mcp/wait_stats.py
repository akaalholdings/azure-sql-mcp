from __future__ import annotations

from typing import Any

from .connection import AzureSqlExecutor

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


class WaitStatsService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_wait_stats(
        self,
        database_name: str,
        top_n: int = 20,
    ) -> dict[str, Any]:
        """Top waits from sys.dm_db_wait_stats with category mapping and recommendations."""
        query = """
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
        rows = await self.executor.fetch_all(database_name, query)

        # Filter out benign waits
        filtered = [r for r in rows if r.get("wait_type") not in BENIGN_WAITS]

        total_wait_ms = sum(r.get("wait_time_ms", 0) for r in filtered) or 1

        waits: list[dict[str, Any]] = []
        for row in filtered[:top_n]:
            wait_type = row.get("wait_type", "")
            wait_ms = row.get("wait_time_ms", 0)
            category = classify_wait(wait_type)
            waits.append(
                {
                    **row,
                    "category": category,
                    "pct_of_total": round(wait_ms / total_wait_ms * 100, 2),
                    "recommendation": CATEGORY_RECOMMENDATIONS.get(category, ""),
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

        categories_sorted = sorted(
            category_totals.values(),
            key=lambda c: c["total_wait_time_ms"],
            reverse=True,
        )

        return {
            "database_name": database_name,
            "total_wait_time_ms": total_wait_ms,
            "top_waits": waits,
            "categories": categories_sorted,
        }

    async def get_query_wait_stats(
        self,
        database_name: str,
        window_minutes: int = 60,
        top_n: int = 20,
    ) -> dict[str, Any]:
        """Per-query wait breakdown from Query Store wait stats."""
        query = """
        SELECT TOP ({top_n})
            q.query_id,
            qt.query_sql_text,
            ws.wait_category_desc,
            SUM(ws.total_query_wait_time_ms) AS total_wait_time_ms,
            SUM(ws.avg_query_wait_time_ms * ws.total_query_wait_time_ms)
                / NULLIF(SUM(ws.total_query_wait_time_ms), 0) AS weighted_avg_wait_ms,
            MAX(ws.max_query_wait_time_ms) AS max_wait_time_ms
        FROM sys.query_store_wait_stats AS ws
        INNER JOIN sys.query_store_plan AS p
            ON ws.plan_id = p.plan_id
        INNER JOIN sys.query_store_query AS q
            ON p.query_id = q.query_id
        INNER JOIN sys.query_store_query_text AS qt
            ON q.query_text_id = qt.query_text_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON ws.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE rsi.start_time >= DATEADD(MINUTE, -{window_minutes}, GETUTCDATE())
        GROUP BY q.query_id, qt.query_sql_text, ws.wait_category_desc
        ORDER BY total_wait_time_ms DESC
        """.format(top_n=int(top_n), window_minutes=int(window_minutes))

        rows = await self.executor.fetch_all(database_name, query)
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "query_wait_stats": rows,
        }

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
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
        WHERE wt.session_id != @@SPID
          AND wt.session_id > 50
        ORDER BY wt.wait_duration_ms DESC
        """
        rows = await self.executor.fetch_all(database_name, query)

        for row in rows:
            wait_type = row.get("wait_type", "")
            category = classify_wait(wait_type)
            row["category"] = category
            row["recommendation"] = CATEGORY_RECOMMENDATIONS.get(category, "")

        return {
            "database_name": database_name,
            "currently_waiting_count": len(rows),
            "waiting_tasks": rows,
        }
