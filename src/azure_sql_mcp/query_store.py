from __future__ import annotations

from typing import Any

from .connection import AzureSqlExecutor

SORT_BY_EXPRESSIONS = {
    "total_duration": "SUM(rs.avg_duration * rs.count_executions)",
    "avg_duration": "AVG(rs.avg_duration)",
    "cpu": "SUM(rs.avg_cpu_time * rs.count_executions)",
    "executions": "SUM(rs.count_executions)",
    "logical_io": "SUM(rs.avg_logical_io_reads * rs.count_executions)",
    "physical_io": "SUM(rs.avg_physical_io_reads * rs.count_executions)",
    "memory": "AVG(rs.avg_query_max_used_memory)",
}


class QueryStoreService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_status(self, database_name: str) -> dict[str, Any]:
        query = """
        SELECT
            desired_state_desc,
            actual_state_desc,
            readonly_reason,
            current_storage_size_mb,
            max_storage_size_mb,
            query_capture_mode_desc,
            wait_stats_capture_mode_desc
        FROM sys.database_query_store_options
        """
        rows = await self.executor.fetch_all(database_name, query)
        if not rows:
            return {
                "enabled": False,
                "message": "Query Store options were not returned.",
            }
        row = rows[0]
        return {
            "enabled": row["actual_state_desc"] not in {"OFF", "ERROR"},
            "status": row,
        }

    async def get_top_queries(
        self,
        database_name: str,
        sort_by: str,
        window_minutes: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        query = self._build_top_queries_query(sort_by)
        params = [window_minutes, limit] if sort_by == "resource_blend" else [limit, window_minutes]
        return await self.executor.fetch_all(
            database_name,
            query,
            params=params,
        )

    def _build_top_queries_query(self, sort_by: str) -> str:
        if sort_by == "resource_blend":
            return """
            WITH QueryMetrics AS (
                SELECT
                    q.query_id,
                    p.plan_id,
                    qt.query_sql_text,
                    SUM(rs.count_executions) AS executions,
                    SUM(rs.avg_duration * rs.count_executions) AS total_duration_us,
                    AVG(rs.avg_duration) AS avg_duration_us,
                    SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu_us,
                    SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
                    SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_io_reads,
                    AVG(rs.avg_query_max_used_memory) AS avg_query_max_used_memory,
                    CAST(MAX(rsi.end_time) AS datetime2(7)) AS last_seen_utc
                FROM sys.query_store_query_text AS qt
                INNER JOIN sys.query_store_query AS q
                    ON qt.query_text_id = q.query_text_id
                INNER JOIN sys.query_store_plan AS p
                    ON q.query_id = p.query_id
                INNER JOIN sys.query_store_runtime_stats AS rs
                    ON p.plan_id = rs.plan_id
                INNER JOIN sys.query_store_runtime_stats_interval AS rsi
                    ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
                WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
                GROUP BY q.query_id, p.plan_id, qt.query_sql_text
            ),
            MaxMetrics AS (
                SELECT
                    MAX(total_cpu_us) AS max_cpu_us,
                    MAX(total_logical_io_reads) AS max_logical_io_reads,
                    MAX(total_duration_us) AS max_duration_us
                FROM QueryMetrics
            )
            SELECT TOP (?)
                qm.*,
                (
                    COALESCE(CAST(qm.total_cpu_us AS FLOAT) / NULLIF(mm.max_cpu_us, 0), 0.0)
                    + COALESCE(
                        CAST(qm.total_logical_io_reads AS FLOAT)
                        / NULLIF(mm.max_logical_io_reads, 0),
                        0.0
                    )
                    + COALESCE(
                        CAST(qm.total_duration_us AS FLOAT) / NULLIF(mm.max_duration_us, 0),
                        0.0
                    )
                ) / 3.0 AS resource_blend_score
            FROM QueryMetrics AS qm
            CROSS JOIN MaxMetrics AS mm
            ORDER BY resource_blend_score DESC, qm.total_cpu_us DESC
            """

        order_expression = SORT_BY_EXPRESSIONS.get(sort_by)
        if order_expression is None:
            supported = ", ".join(
                [
                    "total_duration",
                    "avg_duration",
                    "cpu",
                    "executions",
                    "logical_io",
                    "physical_io",
                    "memory",
                    "resource_blend",
                ]
            )
            raise ValueError(f"Unsupported sort_by. Use {supported}.")

        return f"""
        SELECT TOP (?)
            q.query_id,
            p.plan_id,
            qt.query_sql_text,
            SUM(rs.count_executions) AS executions,
            SUM(rs.avg_duration * rs.count_executions) AS total_duration_us,
            AVG(rs.avg_duration) AS avg_duration_us,
            SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu_us,
            SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
            SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_io_reads,
            AVG(rs.avg_query_max_used_memory) AS avg_query_max_used_memory,
            CAST(MAX(rsi.end_time) AS datetime2(7)) AS last_seen_utc
        FROM sys.query_store_query_text AS qt
        INNER JOIN sys.query_store_query AS q
            ON qt.query_text_id = q.query_text_id
        INNER JOIN sys.query_store_plan AS p
            ON q.query_id = p.query_id
        INNER JOIN sys.query_store_runtime_stats AS rs
            ON p.plan_id = rs.plan_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
        GROUP BY q.query_id, p.plan_id, qt.query_sql_text
        ORDER BY {order_expression} DESC
        """
