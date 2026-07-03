from __future__ import annotations

import json
from typing import Any

from .connection import AzureSqlExecutor


class QueryRegressionService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def detect_parameter_sniffing(
        self,
        database_name: str,
        variance_threshold: float = 10.0,
        window_minutes: int = 1440,
        top_n: int = 20,
    ) -> dict[str, Any]:
        """Find queries with multiple plans where perf varies wildly (parameter sniffing indicator)."""
        query = """
        SELECT TOP ({top_n})
            q.query_id,
            qt.query_sql_text,
            COUNT(DISTINCT p.plan_id) AS plan_count,
            MIN(rs.avg_duration / 1000.0) AS best_avg_duration_ms,
            MAX(rs.avg_duration / 1000.0) AS worst_avg_duration_ms,
            CASE WHEN MIN(rs.avg_duration) > 0
                 THEN MAX(rs.avg_duration) * 1.0 / MIN(rs.avg_duration)
                 ELSE 0
            END AS duration_variance_ratio,
            MIN(rs.avg_cpu_time / 1000.0) AS best_avg_cpu_ms,
            MAX(rs.avg_cpu_time / 1000.0) AS worst_avg_cpu_ms,
            SUM(rs.count_executions) AS total_executions,
            MIN(p.plan_id) AS best_plan_id,
            MAX(p.plan_id) AS worst_plan_id
        FROM sys.query_store_query AS q
        INNER JOIN sys.query_store_query_text AS qt
            ON q.query_text_id = qt.query_text_id
        INNER JOIN sys.query_store_plan AS p
            ON q.query_id = p.query_id
        INNER JOIN sys.query_store_runtime_stats AS rs
            ON p.plan_id = rs.plan_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE rsi.start_time >= DATEADD(MINUTE, -{window_minutes}, GETUTCDATE())
        GROUP BY q.query_id, qt.query_sql_text
        HAVING COUNT(DISTINCT p.plan_id) > 1
           AND CASE WHEN MIN(rs.avg_duration) > 0
                    THEN MAX(rs.avg_duration) * 1.0 / MIN(rs.avg_duration)
                    ELSE 0
               END >= {threshold}
        ORDER BY duration_variance_ratio DESC
        """.format(
            top_n=int(top_n),
            window_minutes=int(window_minutes),
            threshold=float(variance_threshold),
        )

        rows = await self.executor.fetch_all(database_name, query)
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "variance_threshold": variance_threshold,
            "affected_query_count": len(rows),
            "queries": rows,
        }

    async def detect_regressed_queries(
        self,
        database_name: str,
        window_minutes: int = 1440,
    ) -> dict[str, Any]:
        """Surface automatic tuning regression recommendations."""
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")
        query = """
        WITH TuningRecommendations AS (
            SELECT
                reason,
                score,
                JSON_VALUE(state, '$.currentValue') AS current_state,
                JSON_VALUE(state, '$.reason') AS state_reason,
                JSON_VALUE(details, '$.implementationDetails.script') AS tuning_script,
                JSON_VALUE(details, '$.queryId') AS query_id,
                JSON_VALUE(details, '$.regressedPlanId') AS regressed_plan_id,
                JSON_VALUE(details, '$.recommendedPlanId') AS recommended_plan_id,
                JSON_VALUE(details, '$.estimatedCpuGain') AS estimated_cpu_gain,
                JSON_VALUE(details, '$.estimatedDurationGain') AS estimated_duration_gain,
                details
            FROM sys.dm_db_tuning_recommendations
        ),
        RecentPlanActivity AS (
            SELECT
                p.query_id,
                p.plan_id,
                MAX(rsi.end_time) AS last_seen_utc,
                SUM(rs.count_executions) AS recent_execution_count
            FROM sys.query_store_plan AS p
            INNER JOIN sys.query_store_runtime_stats AS rs
                ON p.plan_id = rs.plan_id
            INNER JOIN sys.query_store_runtime_stats_interval AS rsi
                ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
            WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
            GROUP BY p.query_id, p.plan_id
        )
        SELECT
            tr.reason,
            tr.score,
            tr.current_state,
            tr.state_reason,
            tr.tuning_script,
            tr.query_id,
            tr.regressed_plan_id,
            tr.recommended_plan_id,
            tr.estimated_cpu_gain,
            tr.estimated_duration_gain,
            rpa.last_seen_utc,
            rpa.recent_execution_count,
            tr.details
        FROM TuningRecommendations AS tr
        LEFT JOIN RecentPlanActivity AS rpa
            ON rpa.query_id = TRY_CONVERT(bigint, tr.query_id)
           AND rpa.plan_id = TRY_CONVERT(bigint, tr.recommended_plan_id)
        WHERE rpa.last_seen_utc IS NOT NULL
        ORDER BY tr.score DESC
        """
        rows = await self.executor.fetch_all(database_name, query, params=[int(window_minutes)])

        # Parse the details JSON for richer output
        for row in rows:
            details = row.get("details")
            if isinstance(details, str):
                try:
                    row["details"] = json.loads(details)
                except (json.JSONDecodeError, TypeError):
                    pass

        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "recommendation_count": len(rows),
            "recommendations": rows,
        }

    async def compare_query_plans(
        self,
        database_name: str,
        query_id: int,
        plan_id_a: int | None = None,
        plan_id_b: int | None = None,
    ) -> dict[str, Any]:
        """Compare two plans for a query. If plan IDs not given, uses best/worst by duration."""
        if plan_id_a is not None and plan_id_b is not None:
            plans_query = """
            SELECT
                p.plan_id,
                p.query_id,
                p.is_forced_plan,
                p.force_failure_count,
                rs.avg_duration / 1000.0 AS avg_duration_ms,
                rs.avg_cpu_time / 1000.0 AS avg_cpu_ms,
                rs.avg_logical_io_reads,
                rs.avg_physical_io_reads,
                rs.count_executions,
                rs.first_execution_time,
                rs.last_execution_time,
                CAST(p.query_plan AS NVARCHAR(MAX)) AS query_plan_xml
            FROM sys.query_store_plan AS p
            INNER JOIN sys.query_store_runtime_stats AS rs
                ON p.plan_id = rs.plan_id
            WHERE p.query_id = {query_id}
              AND p.plan_id IN ({plan_a}, {plan_b})
            ORDER BY p.plan_id
            """.format(
                query_id=int(query_id),
                plan_a=int(plan_id_a),
                plan_b=int(plan_id_b),
            )
        else:
            plans_query = """
            WITH PlanStats AS (
                SELECT
                    p.plan_id,
                    p.query_id,
                    p.is_forced_plan,
                    p.force_failure_count,
                    rs.avg_duration / 1000.0 AS avg_duration_ms,
                    rs.avg_cpu_time / 1000.0 AS avg_cpu_ms,
                    rs.avg_logical_io_reads,
                    rs.avg_physical_io_reads,
                    rs.count_executions,
                    rs.first_execution_time,
                    rs.last_execution_time,
                    CAST(p.query_plan AS NVARCHAR(MAX)) AS query_plan_xml,
                    ROW_NUMBER() OVER (ORDER BY rs.avg_duration ASC) AS best_rank,
                    ROW_NUMBER() OVER (ORDER BY rs.avg_duration DESC) AS worst_rank
                FROM sys.query_store_plan AS p
                INNER JOIN sys.query_store_runtime_stats AS rs
                    ON p.plan_id = rs.plan_id
                WHERE p.query_id = {query_id}
            )
            SELECT * FROM PlanStats
            WHERE best_rank = 1 OR worst_rank = 1
            ORDER BY avg_duration_ms ASC
            """.format(query_id=int(query_id))

        rows = await self.executor.fetch_all(database_name, plans_query)

        plans: list[dict[str, Any]] = []
        for row in rows:
            plan_xml = row.pop("query_plan_xml", None) or ""
            row["plan_xml_length"] = len(plan_xml)
            # Extract top operators from plan XML (lightweight parse)
            row["top_operators"] = self._extract_top_operators(plan_xml)
            plans.append(row)

        comparison: dict[str, Any] = {}
        if len(plans) == 2:
            a, b = plans[0], plans[1]
            comparison = {
                "duration_ratio": round(
                    (b.get("avg_duration_ms") or 1)
                    / max(a.get("avg_duration_ms") or 1, 0.001),
                    2,
                ),
                "cpu_ratio": round(
                    (b.get("avg_cpu_ms") or 1)
                    / max(a.get("avg_cpu_ms") or 1, 0.001),
                    2,
                ),
                "io_ratio": round(
                    (b.get("avg_logical_io_reads") or 1)
                    / max(a.get("avg_logical_io_reads") or 1, 0.001),
                    2,
                ),
            }

        return {
            "database_name": database_name,
            "query_id": query_id,
            "plans": plans,
            "comparison": comparison,
        }

    async def get_forced_plans(
        self,
        database_name: str,
        window_minutes: int = 1440,
    ) -> dict[str, Any]:
        """List all forced plans with execution stats and staleness check."""
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")
        query = """
        WITH ForcedPlanStats AS (
            SELECT
                p.plan_id,
                p.query_id,
                qt.query_sql_text,
                p.is_forced_plan,
                p.force_failure_count,
                p.last_force_failure_reason_desc,
                MAX(rs.avg_duration) / 1000.0 AS avg_duration_ms,
                MAX(rs.avg_cpu_time) / 1000.0 AS avg_cpu_ms,
                MAX(rs.avg_logical_io_reads) AS avg_logical_io_reads,
                SUM(rs.count_executions) AS count_executions,
                SUM(
                    CASE
                        WHEN rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
                        THEN rs.count_executions
                        ELSE 0
                    END
                ) AS recent_execution_count,
                MAX(rs.last_execution_time) AS last_execution_time
            FROM sys.query_store_plan AS p
            INNER JOIN sys.query_store_query AS q
                ON p.query_id = q.query_id
            INNER JOIN sys.query_store_query_text AS qt
                ON q.query_text_id = qt.query_text_id
            LEFT JOIN sys.query_store_runtime_stats AS rs
                ON p.plan_id = rs.plan_id
            LEFT JOIN sys.query_store_runtime_stats_interval AS rsi
                ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
            WHERE p.is_forced_plan = 1
            GROUP BY
                p.plan_id,
                p.query_id,
                qt.query_sql_text,
                p.is_forced_plan,
                p.force_failure_count,
                p.last_force_failure_reason_desc
        )
        SELECT
            plan_id,
            query_id,
            query_sql_text,
            is_forced_plan,
            force_failure_count,
            last_force_failure_reason_desc,
            avg_duration_ms,
            avg_cpu_ms,
            avg_logical_io_reads,
            count_executions,
            recent_execution_count,
            last_execution_time,
            DATEDIFF(DAY, last_execution_time, SYSUTCDATETIME()) AS days_since_last_exec
        FROM ForcedPlanStats
        ORDER BY last_execution_time DESC
        """
        rows = await self.executor.fetch_all(database_name, query, params=[int(window_minutes)])

        stale = [r for r in rows if (r.get("days_since_last_exec") or 0) > 7]
        failing = [r for r in rows if (r.get("force_failure_count") or 0) > 0]

        warnings: list[dict[str, Any]] = []
        if stale:
            warnings.append(
                {
                    "type": "stale_forced_plans",
                    "message": (
                        f"{len(stale)} forced plan(s) haven't executed in over 7 days — "
                        "may be stale or the query pattern changed"
                    ),
                    "plan_ids": [r.get("plan_id") for r in stale],
                }
            )
        if failing:
            warnings.append(
                {
                    "type": "failing_forced_plans",
                    "message": (
                        f"{len(failing)} forced plan(s) have force failures — "
                        "the optimizer cannot use the forced plan"
                    ),
                    "plan_ids": [r.get("plan_id") for r in failing],
                }
            )

        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "forced_plan_count": len(rows),
            "stale_count": len(stale),
            "failing_count": len(failing),
            "forced_plans": rows,
            "warnings": warnings,
        }

    async def get_query_parameter_buckets(
        self,
        database_name: str,
        query_id: int,
    ) -> dict[str, Any]:
        """Extract compiled parameter values per Query Store plan for one query.

        Each plan's SHOWPLAN XML carries the parameter values the plan was compiled
        with (``ParameterCompiledValue``). Grouped with per-plan runtime stats, those
        values are the **parameter buckets** a tuning pass must test: each distinct
        compiled set produced a distinct plan shape in production.
        """
        if query_id <= 0:
            raise ValueError("query_id must be greater than 0.")
        query = """
        SELECT
            p.plan_id,
            CAST(p.query_plan AS nvarchar(max)) AS query_plan_xml,
            p.is_forced_plan,
            SUM(rs.count_executions) AS executions,
            SUM(rs.avg_duration * rs.count_executions)
                / NULLIF(SUM(rs.count_executions), 0) / 1000.0 AS avg_duration_ms,
            MAX(rs.last_execution_time) AS last_execution_time
        FROM sys.query_store_plan AS p
        LEFT JOIN sys.query_store_runtime_stats AS rs
            ON p.plan_id = rs.plan_id
        WHERE p.query_id = ?
        GROUP BY p.plan_id, CAST(p.query_plan AS nvarchar(max)), p.is_forced_plan
        ORDER BY SUM(rs.count_executions) DESC
        """
        rows = await self.executor.fetch_all(database_name, query, params=[int(query_id)])

        buckets: list[dict[str, Any]] = []
        seen_sets: set[tuple] = set()
        distinct_parameter_sets: list[list[dict[str, Any]]] = []
        for row in rows:
            plan_xml = row.pop("query_plan_xml", None) or ""
            parameters = self._extract_compiled_parameters(plan_xml)
            bucket = {**row, "parameters": parameters}
            buckets.append(bucket)
            if parameters:
                key = tuple(
                    (p["name"], p.get("compiled_value")) for p in parameters
                )
                if key not in seen_sets:
                    seen_sets.add(key)
                    distinct_parameter_sets.append(parameters)

        return {
            "database_name": database_name,
            "query_id": query_id,
            "plan_count": len(buckets),
            "buckets": buckets,
            "distinct_parameter_sets": distinct_parameter_sets,
            "note": (
                "Each distinct compiled parameter set produced its own plan in "
                "production — test at least these buckets, plus boundary/NULL/empty "
                "cases the history cannot show. Compiled values reflect compile time, "
                "not every runtime value."
            ),
        }

    @staticmethod
    def _extract_compiled_parameters(plan_xml: str) -> list[dict[str, Any]]:
        """Pull ParameterList entries (name, type, compiled value) from SHOWPLAN XML."""
        if not plan_xml:
            return []
        import xml.etree.ElementTree as ET

        try:
            root = ET.fromstring(plan_xml)
        except ET.ParseError:
            return []

        ns = "{http://schemas.microsoft.com/sqlserver/2004/07/showplan}"
        parameters: list[dict[str, Any]] = []
        seen: set[str] = set()
        for param_list in root.iter(f"{ns}ParameterList"):
            for column in param_list.iter(f"{ns}ColumnReference"):
                name = column.get("Column")
                if not name or name in seen:
                    continue
                seen.add(name)
                parameters.append(
                    {
                        "name": name,
                        "data_type": column.get("ParameterDataType"),
                        "compiled_value": column.get("ParameterCompiledValue"),
                        "runtime_value": column.get("ParameterRuntimeValue"),
                    }
                )
        return parameters

    @staticmethod
    def _extract_top_operators(plan_xml: str) -> list[str]:
        """Extract physical operator names from plan XML (lightweight)."""
        if not plan_xml:
            return []
        import xml.etree.ElementTree as ET

        try:
            root = ET.fromstring(plan_xml)
        except ET.ParseError:
            return []

        operators: list[str] = []
        for rel_op in root.iter("{http://schemas.microsoft.com/sqlserver/2004/07/showplan}RelOp"):
            phys = rel_op.get("PhysicalOp")
            if phys and phys not in operators:
                operators.append(phys)
            if len(operators) >= 10:
                break
        return operators
