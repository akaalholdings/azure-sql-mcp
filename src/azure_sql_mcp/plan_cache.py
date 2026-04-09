from __future__ import annotations

from typing import Any

from .connection import AzureSqlExecutor


class PlanCacheService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def check_statistics_health(
        self,
        database_name: str,
        stale_days: int = 7,
        mod_pct_threshold: float = 20.0,
    ) -> dict[str, Any]:
        """Check statistics currency using sys.dm_db_stats_properties."""
        query = """
        SELECT
            OBJECT_SCHEMA_NAME(s.object_id) AS schema_name,
            OBJECT_NAME(s.object_id) AS table_name,
            s.name AS stats_name,
            s.auto_created,
            sp.last_updated,
            sp.rows AS total_rows,
            sp.rows_sampled,
            sp.modification_counter,
            CASE WHEN sp.rows > 0
                 THEN CAST(sp.modification_counter * 100.0 / sp.rows AS DECIMAL(10, 2))
                 ELSE 0
            END AS modification_pct,
            CASE WHEN sp.rows > 0 AND sp.rows_sampled > 0
                 THEN CAST(sp.rows_sampled * 100.0 / sp.rows AS DECIMAL(10, 2))
                 ELSE 0
            END AS sample_pct,
            DATEDIFF(DAY, sp.last_updated, GETDATE()) AS days_since_update
        FROM sys.stats AS s
        CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
        INNER JOIN sys.objects AS o ON s.object_id = o.object_id
        WHERE o.is_ms_shipped = 0
          AND sp.rows > 0
        ORDER BY modification_pct DESC
        """
        rows = await self.executor.fetch_all(database_name, query)

        stale: list[dict[str, Any]] = []
        low_sample: list[dict[str, Any]] = []
        high_modification: list[dict[str, Any]] = []

        for row in rows:
            days = row.get("days_since_update") or 0
            mod_pct = float(row.get("modification_pct") or 0)
            sample_pct = float(row.get("sample_pct") or 0)

            if days > stale_days:
                stale.append(row)
            if mod_pct > mod_pct_threshold:
                high_modification.append(row)
            if sample_pct < 10 and (row.get("total_rows") or 0) > 1000:
                low_sample.append(row)

        return {
            "database_name": database_name,
            "total_stats": len(rows),
            "stale_count": len(stale),
            "high_modification_count": len(high_modification),
            "low_sample_count": len(low_sample),
            "stale_stats": stale,
            "high_modification_stats": high_modification,
            "low_sample_stats": low_sample,
            "all_stats": rows,
        }

    async def get_plan_cache_analysis(
        self,
        database_name: str,
    ) -> dict[str, Any]:
        """Plan cache health: type distribution, single-use bloat, top plans by size."""
        distribution_query = """
        SELECT
            objtype AS plan_type,
            COUNT(*) AS plan_count,
            SUM(CAST(size_in_bytes AS BIGINT)) / (1024.0 * 1024.0) AS total_size_mb,
            SUM(usecounts) AS total_use_counts,
            SUM(CASE WHEN usecounts = 1 THEN 1 ELSE 0 END) AS single_use_count,
            SUM(CASE WHEN usecounts = 1 THEN CAST(size_in_bytes AS BIGINT) ELSE 0 END)
                / (1024.0 * 1024.0) AS single_use_size_mb
        FROM sys.dm_exec_cached_plans
        GROUP BY objtype
        ORDER BY total_size_mb DESC
        """
        top_plans_query = """
        SELECT TOP 20
            cp.objtype AS plan_type,
            cp.usecounts,
            cp.size_in_bytes / 1024.0 AS size_kb,
            cp.cacheobjtype,
            SUBSTRING(st.text, 1, 200) AS sql_text_preview
        FROM sys.dm_exec_cached_plans AS cp
        CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) AS st
        ORDER BY cp.size_in_bytes DESC
        """

        distribution = await self.executor.fetch_all(database_name, distribution_query)
        top_plans = await self.executor.fetch_all(database_name, top_plans_query)

        total_plans = sum(r.get("plan_count", 0) for r in distribution)
        total_single_use = sum(r.get("single_use_count", 0) for r in distribution)
        total_size_mb = sum(float(r.get("total_size_mb", 0) or 0) for r in distribution)
        single_use_size = sum(
            float(r.get("single_use_size_mb", 0) or 0) for r in distribution
        )

        warnings: list[dict[str, Any]] = []
        if total_plans > 0 and total_single_use / total_plans > 0.5:
            warnings.append(
                {
                    "type": "single_use_plan_bloat",
                    "message": (
                        f"{total_single_use}/{total_plans} plans ({round(total_single_use / total_plans * 100)}%) "
                        f"are single-use, consuming {round(single_use_size, 1)} MB. "
                        "Consider enabling 'optimize for ad hoc workloads'."
                    ),
                }
            )

        return {
            "database_name": database_name,
            "total_plans": total_plans,
            "total_single_use_plans": total_single_use,
            "total_size_mb": round(total_size_mb, 2),
            "single_use_size_mb": round(single_use_size, 2),
            "distribution": distribution,
            "top_plans_by_size": top_plans,
            "warnings": warnings,
        }

    async def get_query_compilation_stats(
        self,
        database_name: str,
        top_n: int = 20,
    ) -> dict[str, Any]:
        """Identify excessively recompiled queries from sys.dm_exec_query_stats."""
        query = """
        SELECT TOP ({top_n})
            qs.query_hash,
            qs.plan_generation_num AS recompile_count,
            qs.execution_count,
            CASE WHEN qs.execution_count > 0
                 THEN CAST(qs.plan_generation_num * 1.0 / qs.execution_count AS DECIMAL(10, 4))
                 ELSE 0
            END AS recompile_ratio,
            qs.creation_time,
            qs.last_execution_time,
            qs.total_elapsed_time / 1000.0 AS total_elapsed_ms,
            qs.total_worker_time / 1000.0 AS total_cpu_ms,
            qs.total_logical_reads,
            SUBSTRING(st.text, 1, 300) AS sql_text_preview
        FROM sys.dm_exec_query_stats AS qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
        WHERE qs.plan_generation_num > 1
        ORDER BY qs.plan_generation_num DESC
        """.format(top_n=int(top_n))

        rows = await self.executor.fetch_all(database_name, query)

        excessive = [
            r
            for r in rows
            if (float(r.get("recompile_ratio") or 0)) > 0.5
            and (r.get("execution_count") or 0) > 10
        ]

        return {
            "database_name": database_name,
            "recompiled_query_count": len(rows),
            "excessive_recompile_count": len(excessive),
            "queries": rows,
            "warnings": (
                [
                    {
                        "type": "excessive_recompilation",
                        "message": (
                            f"{len(excessive)} queries recompile more than 50% of executions. "
                            "Check for schema changes, temp table usage, or RECOMPILE hints."
                        ),
                    }
                ]
                if excessive
                else []
            ),
        }
