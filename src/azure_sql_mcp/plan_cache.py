from __future__ import annotations

from typing import Any

from .connection import AzureSqlExecutor

RECOMPILING_MIN_GENERATIONS = 10
RECOMPILING_WINDOW_MINUTES = 60
# Single-use plans are bloat only when they are most of the compiled plans and
# hold enough memory to matter. A small or idle cache is mostly single-use plans.
SINGLE_USE_BLOAT_SHARE = 0.5
SINGLE_USE_BLOAT_MIN_MB = 100.0
# Azure SQL Database has no server option: the setting is database scoped.
AD_HOC_PLAN_STUBS_SQL = "ALTER DATABASE SCOPED CONFIGURATION SET OPTIMIZE_FOR_AD_HOC_WORKLOADS = ON;"
AD_HOC_SETTING_SQL = """
SELECT CAST(value AS nvarchar(20)) AS value
FROM sys.database_scoped_configurations
WHERE name = N'OPTIMIZE_FOR_AD_HOC_WORKLOADS'
"""


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
        """Plan cache health: type distribution, single-use bloat, top plans by size.

        A plan is a compiled plan. The cache also holds parse trees (views) and
        plan stubs, which are entries but not plans: they count in
        ``total_cache_entries`` and ``total_size_mb`` only. Single-use plans are
        reported as bloat when they are over ``SINGLE_USE_BLOAT_SHARE`` of the
        compiled plans and hold at least ``SINGLE_USE_BLOAT_MIN_MB``.
        """
        distribution_query = """
        SELECT
            objtype AS plan_type,
            COUNT(*) AS plan_count,
            SUM(CASE WHEN cacheobjtype = 'Compiled Plan' THEN 1 ELSE 0 END) AS compiled_plan_count,
            SUM(CAST(size_in_bytes AS BIGINT)) / (1024.0 * 1024.0) AS total_size_mb,
            SUM(CAST(usecounts AS BIGINT)) AS total_use_counts,
            SUM(CASE WHEN usecounts = 1 AND cacheobjtype = 'Compiled Plan' THEN 1 ELSE 0 END) AS single_use_count,
            SUM(
                CASE
                    WHEN usecounts = 1 AND cacheobjtype = 'Compiled Plan' THEN CAST(size_in_bytes AS BIGINT)
                    ELSE 0
                END
            ) / (1024.0 * 1024.0) AS single_use_size_mb
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
        setting = await self.executor.fetch_all(database_name, AD_HOC_SETTING_SQL)
        # None when the setting row is not there: then it is not known to be on.
        stubs_on = str(setting[0].get("value") or "").strip().upper() in {"1", "ON", "TRUE"} if setting else None

        total_entries = sum(r.get("plan_count") or 0 for r in distribution)
        total_plans = sum(r.get("compiled_plan_count") or 0 for r in distribution)
        total_single_use = sum(r.get("single_use_count") or 0 for r in distribution)
        total_size_mb = sum(float(r.get("total_size_mb", 0) or 0) for r in distribution)
        single_use_size = sum(
            float(r.get("single_use_size_mb", 0) or 0) for r in distribution
        )

        single_use_share = total_single_use / total_plans if total_plans else 0.0
        warnings: list[dict[str, Any]] = []
        if single_use_share > SINGLE_USE_BLOAT_SHARE and single_use_size >= SINGLE_USE_BLOAT_MIN_MB:
            found = (
                f"{total_single_use} of {total_plans} compiled plans ({round(single_use_share * 100)}%) "
                f"were used once and hold {round(single_use_size, 1)} MB. "
            )
            parameterize = (
                "parameterize them in the application. Forced parameterization changes plans for every "
                "statement, so test it first."
            )
            if stubs_on:
                # The stub setting cannot shrink this further: do not recommend it again.
                warnings.append(
                    {
                        "type": "single_use_plan_bloat",
                        "message": (
                            f"{found}OPTIMIZE_FOR_AD_HOC_WORKLOADS is already on, so these statements "
                            f"compile again and again: {parameterize}"
                        ),
                    }
                )
            else:
                warnings.append(
                    {
                        "type": "single_use_plan_bloat",
                        "message": (
                            f"{found}First step: keep a plan stub for a batch until it runs a second time "
                            "(the database scoped setting in remediation). If statements still compile again "
                            f"and again, {parameterize}"
                        ),
                        "remediation": AD_HOC_PLAN_STUBS_SQL,
                    }
                )

        return {
            "database_name": database_name,
            "total_cache_entries": total_entries,
            "total_plans": total_plans,
            "total_single_use_plans": total_single_use,
            "single_use_plan_pct": round(single_use_share * 100, 1),
            "total_size_mb": round(total_size_mb, 2),
            "single_use_size_mb": round(single_use_size, 2),
            "single_use_bloat_rule": {
                "min_share_of_compiled_plans": SINGLE_USE_BLOAT_SHARE,
                "min_single_use_size_mb": SINGLE_USE_BLOAT_MIN_MB,
            },
            "optimize_for_ad_hoc_workloads": stubs_on,
            "distribution": distribution,
            "top_plans_by_size": top_plans,
            "warnings": warnings,
        }

    async def get_query_compilation_stats(
        self,
        database_name: str,
        top_n: int = 20,
    ) -> dict[str, Any]:
        """Rank cached statements by recompiles and flag the ones recompiling now.

        plan_generation_num counts every recompile of the statement, while
        execution_count restarts at each compile, so the two never form a
        ratio. A statement is recompiling now when it has at least
        ``RECOMPILING_MIN_GENERATIONS`` plan generations and its current plan
        was compiled in the last ``RECOMPILING_WINDOW_MINUTES`` minutes.
        OPTION (RECOMPILE) statements are never cached and do not appear here.
        """
        query = """
        SELECT TOP ({top_n})
            qs.query_hash,
            qs.plan_generation_num AS recompile_count,
            qs.execution_count,
            qs.creation_time,
            DATEDIFF(MINUTE, qs.creation_time, GETDATE()) AS minutes_since_last_compile,
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

        for row in rows:
            minutes = row.get("minutes_since_last_compile")
            row["recompiling_now"] = (
                (row.get("recompile_count") or 0) >= RECOMPILING_MIN_GENERATIONS
                and minutes is not None
                and minutes <= RECOMPILING_WINDOW_MINUTES
            )
        excessive = [r for r in rows if r["recompiling_now"]]

        return {
            "database_name": database_name,
            "recompiled_query_count": len(rows),
            "excessive_recompile_count": len(excessive),
            "recompiling_now_rule": {
                "min_plan_generations": RECOMPILING_MIN_GENERATIONS,
                "compiled_within_minutes": RECOMPILING_WINDOW_MINUTES,
            },
            "queries": rows,
            "warnings": (
                [
                    {
                        "type": "excessive_recompilation",
                        "message": (
                            f"{len(excessive)} cached statement(s) have many plan generations "
                            "and compiled again in the last "
                            f"{RECOMPILING_WINDOW_MINUTES} minutes. "
                            "Check for temp table or schema changes, statistics updates, "
                            "or changing SET options."
                        ),
                    }
                ]
                if excessive
                else []
            ),
        }
