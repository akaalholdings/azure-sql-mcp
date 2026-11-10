from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .connection import AzureSqlExecutor

# Raw DMV read: no JSON paths, joins or filters, so no row can be lost in SQL.
_TUNING_RECOMMENDATIONS_SQL = """
SELECT
    type,
    reason,
    score,
    state,
    details,
    is_executable_action,
    is_revertable_action,
    execute_action_initiated_by,
    revert_action_initiated_by,
    valid_since,
    last_refresh
FROM sys.dm_db_tuning_recommendations
"""

# Window activity per named plan. The LEFT JOIN keeps a plan that did not run,
# so is_forced_plan is known for it; a plan purged from Query Store has no row.
_PLAN_ACTIVITY_SQL = """
SELECT
    p.plan_id,
    p.query_id,
    p.is_forced_plan,
    MAX(rsi.end_time) AS last_seen_utc,
    SUM(rs.count_executions) AS recent_execution_count
FROM sys.query_store_plan AS p
LEFT JOIN (
    sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi
        ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
       AND rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
)
    ON rs.plan_id = p.plan_id
WHERE p.plan_id IN ({placeholders})
GROUP BY p.plan_id, p.query_id, p.is_forced_plan
"""

_PLAN_ID_CHUNK = 1000



# Plans are ranked on per-plan weighted averages; the text is joined after the
# aggregation, so nvarchar(max) is not a GROUP BY key. Averages use Regular
# runs only (execution_type 0); Aborted (3, timeout or cancel) and Exception (4)
# runs are counted per plan. A plan whose failed runs are 10% or more of its
# runs has material failures: it never ranks best and it ranks worst first, so
# a plan that only times out (no Regular average) is still the worst plan.
_PARAMETER_SNIFFING_SQL = """
WITH plan_avg AS (
    SELECT
        p.query_id,
        rs.plan_id,
        SUM(CASE WHEN rs.execution_type = 0 THEN rs.count_executions ELSE 0 END) AS executions,
        SUM(CASE WHEN rs.execution_type = 3 THEN rs.count_executions ELSE 0 END) AS aborted_executions,
        SUM(CASE WHEN rs.execution_type = 4 THEN rs.count_executions ELSE 0 END) AS exception_executions,
        SUM(CASE WHEN rs.execution_type = 0 THEN rs.avg_duration * rs.count_executions END)
            / NULLIF(SUM(CASE WHEN rs.execution_type = 0 THEN rs.count_executions END), 0) AS avg_duration_us,
        SUM(CASE WHEN rs.execution_type = 0 THEN rs.avg_cpu_time * rs.count_executions END)
            / NULLIF(SUM(CASE WHEN rs.execution_type = 0 THEN rs.count_executions END), 0) AS avg_cpu_us
    FROM sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi
        ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
    INNER JOIN sys.query_store_plan AS p
        ON p.plan_id = rs.plan_id
    WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
    GROUP BY p.query_id, rs.plan_id
    HAVING SUM(rs.count_executions) > 0
),
plan_health AS (
    SELECT
        plan_avg.*,
        CASE
            WHEN avg_duration_us IS NULL
              OR (aborted_executions + exception_executions) * 10
                 >= executions + aborted_executions + exception_executions
            THEN 1 ELSE 0
        END AS material_failures
    FROM plan_avg
),
ranked AS (
    SELECT
        query_id,
        plan_id,
        executions,
        aborted_executions,
        exception_executions,
        material_failures,
        avg_duration_us,
        avg_cpu_us,
        ROW_NUMBER() OVER (
            PARTITION BY query_id
            ORDER BY material_failures ASC, avg_duration_us ASC, plan_id ASC
        ) AS best_rn,
        ROW_NUMBER() OVER (
            PARTITION BY query_id
            ORDER BY material_failures DESC,
                     CASE WHEN avg_duration_us IS NULL THEN 0 ELSE 1 END,
                     avg_duration_us DESC, plan_id DESC
        ) AS worst_rn,
        COUNT(*) OVER (PARTITION BY query_id) AS plan_count,
        SUM(executions) OVER (PARTITION BY query_id) AS total_executions
    FROM plan_health
)
SELECT TOP (?)
    b.query_id,
    qt.query_sql_text,
    b.plan_count,
    b.avg_duration_us / 1000.0 AS best_avg_duration_ms,
    w.avg_duration_us / 1000.0 AS worst_avg_duration_ms,
    -- A Regular-only average of a failing plan omits its timeouts: no ratio.
    CASE WHEN w.material_failures = 0
        THEN w.avg_duration_us * 1.0 / NULLIF(b.avg_duration_us, 0)
    END AS duration_variance_ratio,
    b.avg_cpu_us / 1000.0 AS best_avg_cpu_ms,
    w.avg_cpu_us / 1000.0 AS worst_avg_cpu_ms,
    b.total_executions,
    b.plan_id AS best_plan_id,
    w.plan_id AS worst_plan_id,
    b.executions AS best_plan_executions,
    w.executions AS worst_plan_executions,
    b.aborted_executions AS best_plan_aborted_executions,
    b.exception_executions AS best_plan_exception_executions,
    b.material_failures AS best_plan_material_failures,
    w.aborted_executions AS worst_plan_aborted_executions,
    w.exception_executions AS worst_plan_exception_executions,
    w.material_failures AS worst_plan_material_failures
FROM ranked AS b
INNER JOIN ranked AS w
    ON w.query_id = b.query_id
   AND w.worst_rn = 1
INNER JOIN sys.query_store_query AS q
    ON q.query_id = b.query_id
INNER JOIN sys.query_store_query_text AS qt
    ON qt.query_text_id = q.query_text_id
WHERE b.best_rn = 1
  AND b.plan_count > 1
  AND b.material_failures = 0
  AND b.avg_duration_us > 0
  AND (w.material_failures = 1 OR w.avg_duration_us >= ? * b.avg_duration_us)
ORDER BY w.material_failures DESC, duration_variance_ratio DESC
"""

# One row per plan: aggregate runtime stats first, then join the plan (and its
# XML) only for the rows that are returned.
_COMPARE_PLANS_SQL = """
WITH plan_stats AS (
    -- execution_type: 0 Regular, 3 Aborted (timeout or cancel), 4 Exception.
    -- Averages are execution-weighted over Regular runs only.
    SELECT
        rs.plan_id,
        SUM(CASE WHEN rs.execution_type = 0 THEN rs.count_executions ELSE 0 END) AS count_executions,
        SUM(CASE WHEN rs.execution_type = 3 THEN rs.count_executions ELSE 0 END) AS aborted_executions,
        SUM(CASE WHEN rs.execution_type = 4 THEN rs.count_executions ELSE 0 END) AS exception_executions,
        SUM(CASE WHEN rs.execution_type = 0 THEN rs.avg_duration * rs.count_executions END)
            / NULLIF(SUM(CASE WHEN rs.execution_type = 0 THEN rs.count_executions END), 0)
            / 1000.0 AS avg_duration_ms,
        SUM(CASE WHEN rs.execution_type = 0 THEN rs.avg_cpu_time * rs.count_executions END)
            / NULLIF(SUM(CASE WHEN rs.execution_type = 0 THEN rs.count_executions END), 0)
            / 1000.0 AS avg_cpu_ms,
        SUM(CASE WHEN rs.execution_type = 0 THEN rs.avg_logical_io_reads * rs.count_executions END)
            / NULLIF(SUM(CASE WHEN rs.execution_type = 0 THEN rs.count_executions END), 0)
            AS avg_logical_io_reads,
        SUM(CASE WHEN rs.execution_type = 0 THEN rs.avg_physical_io_reads * rs.count_executions END)
            / NULLIF(SUM(CASE WHEN rs.execution_type = 0 THEN rs.count_executions END), 0)
            AS avg_physical_io_reads,
        MIN(rs.first_execution_time) AS first_execution_time,
        MAX(rs.last_execution_time) AS last_execution_time
    FROM sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_plan AS p
        ON p.plan_id = rs.plan_id
    WHERE p.query_id = ?
      {plan_filter}
    GROUP BY rs.plan_id
),
plan_health AS (
    -- Same rule as detect_parameter_sniffing: no Regular average, or 10% or
    -- more aborted/exception runs, is a material failure.
    SELECT
        plan_stats.*,
        CASE
            WHEN avg_duration_ms IS NULL
              OR (aborted_executions + exception_executions) * 10
                 >= count_executions + aborted_executions + exception_executions
            THEN 1 ELSE 0
        END AS material_failures
    FROM plan_stats
),
ranked AS (
    SELECT
        plan_health.*,
        -- A failing plan never ranks best and ranks worst first.
        ROW_NUMBER() OVER (
            ORDER BY material_failures ASC, avg_duration_ms ASC, plan_id ASC
        ) AS best_rank,
        ROW_NUMBER() OVER (
            ORDER BY material_failures DESC,
                     CASE WHEN avg_duration_ms IS NULL THEN 0 ELSE 1 END,
                     avg_duration_ms DESC, plan_id DESC
        ) AS worst_rank
    FROM plan_health
)
SELECT
    p.plan_id,
    p.query_id,
    p.is_forced_plan,
    p.force_failure_count,
    r.avg_duration_ms,
    r.avg_cpu_ms,
    r.avg_logical_io_reads,
    r.avg_physical_io_reads,
    r.count_executions,
    r.aborted_executions,
    r.exception_executions,
    r.material_failures,
    r.first_execution_time,
    r.last_execution_time,
    CAST(p.query_plan AS NVARCHAR(MAX)) AS query_plan_xml
FROM ranked AS r
INNER JOIN sys.query_store_plan AS p
    ON p.plan_id = r.plan_id
{rank_filter}
ORDER BY {order_by}
"""


# Averages are execution-weighted over Regular runs; max_duration_ms is the
# single slowest run. Runtime statistics age out of Query Store, so the last
# execution falls back to the time kept on the plan row.
_FORCED_PLAN_STATS_CTE = """
WITH ForcedPlanStats AS (
    SELECT
        p.plan_id,
        p.query_id,
        qt.query_sql_text,
        p.is_forced_plan,
        p.plan_forcing_type_desc,
        p.force_failure_count,
        p.last_force_failure_reason_desc,
        SUM(CASE WHEN rs.execution_type = 0 THEN rs.avg_duration * rs.count_executions END)
            / NULLIF(SUM(CASE WHEN rs.execution_type = 0 THEN rs.count_executions END), 0)
            / 1000.0 AS avg_duration_ms,
        SUM(CASE WHEN rs.execution_type = 0 THEN rs.avg_cpu_time * rs.count_executions END)
            / NULLIF(SUM(CASE WHEN rs.execution_type = 0 THEN rs.count_executions END), 0)
            / 1000.0 AS avg_cpu_ms,
        SUM(CASE WHEN rs.execution_type = 0 THEN rs.avg_logical_io_reads * rs.count_executions END)
            / NULLIF(SUM(CASE WHEN rs.execution_type = 0 THEN rs.count_executions END), 0)
            AS avg_logical_io_reads,
        MAX(rs.max_duration) / 1000.0 AS max_duration_ms,
        SUM(rs.count_executions) AS count_executions,
        SUM(CASE WHEN rs.execution_type = 3 THEN rs.count_executions ELSE 0 END)
            AS aborted_executions,
        SUM(CASE WHEN rs.execution_type = 4 THEN rs.count_executions ELSE 0 END)
            AS exception_executions,
        SUM(
            CASE
                WHEN rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
                THEN rs.count_executions
                ELSE 0
            END
        ) AS recent_execution_count,
        COALESCE(MAX(rs.last_execution_time), MAX(p.last_execution_time)) AS last_execution_time
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
        p.plan_forcing_type_desc,
        p.force_failure_count,
        p.last_force_failure_reason_desc
)
"""
_FORCED_PLANS_SQL = (
    _FORCED_PLAN_STATS_CTE
    + """SELECT
    f.plan_id,
    f.query_id,
    f.query_sql_text,
    f.is_forced_plan,
    f.plan_forcing_type_desc,
    f.force_failure_count,
    f.last_force_failure_reason_desc,
    f.avg_duration_ms,
    f.avg_cpu_ms,
    f.avg_logical_io_reads,
    f.max_duration_ms,
    f.count_executions,
    f.aborted_executions,
    f.exception_executions,
    f.recent_execution_count,
    ISNULL(qa.query_recent_execution_count, 0) AS query_recent_execution_count,
    f.last_execution_time,
    DATEDIFF(DAY, f.last_execution_time, SYSUTCDATETIME()) AS days_since_last_exec
FROM ForcedPlanStats AS f
OUTER APPLY (
    -- Executions of any plan of the query: the query runs while the
    -- forced plan does not when forcing is failing now.
    SELECT SUM(rs2.count_executions) AS query_recent_execution_count
    FROM sys.query_store_plan AS p2
    INNER JOIN sys.query_store_runtime_stats AS rs2
        ON rs2.plan_id = p2.plan_id
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi2
        ON rs2.runtime_stats_interval_id = rsi2.runtime_stats_interval_id
    WHERE p2.query_id = f.query_id
      AND rsi2.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
) AS qa
ORDER BY f.last_execution_time DESC
"""
)


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
        """Find queries with multiple plans where perf varies wildly (parameter sniffing indicator).

        Each plan's averages are weighted by executions over Regular runs only,
        then the plans are ranked: best/worst are the fastest and slowest plan,
        so one timeout row or one slow interval cannot set the ratio. A plan
        with material aborted/exception runs never ranks best and ranks worst
        first; with no Regular runs its averages and the ratio are None.
        """
        query = _PARAMETER_SNIFFING_SQL
        params: list[object] = [int(window_minutes), int(top_n), float(variance_threshold)]

        rows = await self.executor.fetch_all(database_name, query, params=params)
        for row in rows:
            for key in ("best_plan_material_failures", "worst_plan_material_failures"):
                if key in row:
                    row[key] = bool(row[key])
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
        """Surface automatic tuning regression recommendations.

        Every DMV row is returned. JSON is parsed in Python (the ids live under
        $.planForceDetails), then each row is annotated with the window activity
        of its regressed and recommended plans. A regression is live when the
        regressed plan ran in the window, even if the last-good plan did not.
        """
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")
        rows = await self.executor.fetch_all(database_name, _TUNING_RECOMMENDATIONS_SQL)
        recommendations = [parse_tuning_recommendation(row) for row in rows]

        plan_ids = sorted(
            {
                plan_id
                for rec in recommendations
                for plan_id in (rec["regressed_plan_id"], rec["recommended_plan_id"])
                if plan_id is not None
            }
        )
        activity: dict[int, dict[str, Any]] = {}
        # One placeholder per id (any compat level, unlike OPENJSON); chunked
        # to stay far below the 2100-parameter limit.
        for start in range(0, len(plan_ids), _PLAN_ID_CHUNK):
            chunk = plan_ids[start : start + _PLAN_ID_CHUNK]
            query = _PLAN_ACTIVITY_SQL.format(placeholders=", ".join("?" for _ in chunk))
            for row in await self.executor.fetch_all(
                database_name, query, params=[int(window_minutes), *chunk]
            ):
                plan_id = _int_or_none(row.get("plan_id"))
                if plan_id is not None:
                    activity[plan_id] = row

        for rec in recommendations:
            _annotate_plan_activity(rec, activity)
        # Live regressions first, then the engine's score.
        recommendations.sort(
            key=lambda rec: (not rec["live"], -(_float_or_none(rec["score"]) or 0.0))
        )

        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "recommendation_count": len(recommendations),
            "recommendations": recommendations,
        }

    async def compare_query_plans(
        self,
        database_name: str,
        query_id: int,
        plan_id_a: int | None = None,
        plan_id_b: int | None = None,
    ) -> dict[str, Any]:
        """Compare two plans for a query. If plan IDs not given, uses best/worst by duration.

        Runtime stats are aggregated per plan first (execution-weighted, Regular
        runs only; aborted and exception counts are separate fields), so each
        plan is one row and its XML is read once. Plans come back in the
        caller's a/b order (best, worst by default) and each ratio is
        compared_plan_id / baseline_plan_id.
        """
        if (plan_id_a is None) != (plan_id_b is None):
            raise ValueError("Give both plan_id_a and plan_id_b, or neither (best vs worst).")
        if plan_id_a is not None and plan_id_b is not None:
            plans_query = _COMPARE_PLANS_SQL.format(
                plan_filter="AND p.plan_id IN (?, ?)",
                rank_filter="",
                order_by="r.plan_id",
            )
            params: list[object] = [int(query_id), int(plan_id_a), int(plan_id_b)]
        else:
            plans_query = _COMPARE_PLANS_SQL.format(
                plan_filter="",
                rank_filter="WHERE r.best_rank = 1 OR r.worst_rank = 1",
                order_by="r.best_rank",
            )
            params = [int(query_id)]

        rows = await self.executor.fetch_all(database_name, plans_query, params=params)

        plans: list[dict[str, Any]] = []
        for row in rows:
            plan_xml = row.pop("query_plan_xml", None) or ""
            if "material_failures" in row:
                row["material_failures"] = bool(row["material_failures"])
            row["plan_xml_length"] = len(plan_xml)
            # Extract top operators from plan XML (lightweight parse)
            row["top_operators"] = self._extract_top_operators(plan_xml)
            plans.append(row)
        if plan_id_a is not None and plan_id_b is not None:
            # The SQL sorts by plan_id; put the rows in the caller's a/b order.
            by_id = {int(plan["plan_id"]): plan for plan in plans}
            plans = [by_id[i] for i in dict.fromkeys((int(plan_id_a), int(plan_id_b))) if i in by_id]

        comparison: dict[str, Any] = {}
        if len(plans) == 2:
            a, b = plans[0], plans[1]
            # A Regular-only average of a failing plan omits its timeouts, so
            # a ratio would understate it: no ratios then.
            failing = [p["plan_id"] for p in plans if p.get("material_failures")]
            metrics = (
                ("duration_ratio", "avg_duration_ms"),
                ("cpu_ratio", "avg_cpu_ms"),
                ("io_ratio", "avg_logical_io_reads"),
            )
            comparison = {
                name: None if failing else _plan_ratio(a, b, column) for name, column in metrics
            }
            # Each ratio is compared / baseline.
            comparison["baseline_plan_id"] = a["plan_id"]
            comparison["compared_plan_id"] = b["plan_id"]
            # A plan with only aborted/exception runs has no Regular average,
            # so its ratios are None; it is not 1 ms (it may always time out).
            without_regular = [p["plan_id"] for p in plans if p.get("avg_duration_ms") is None]
            if without_regular:
                comparison["plans_without_regular_runs"] = without_regular
            if failing:
                comparison["plans_with_material_failures"] = failing

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
        query = _FORCED_PLANS_SQL
        rows = await self.executor.fetch_all(
            database_name, query, params=[int(window_minutes), int(window_minutes)]
        )

        for row in rows:
            # No runtime statistics and no last execution time on the plan row:
            # the age is unknown, which is not the same as zero days.
            row["last_execution_unknown"] = row.get("days_since_last_exec") is None
        stale = [
            r for r in rows if r["last_execution_unknown"] or r["days_since_last_exec"] > 7
        ]
        failing = [r for r in rows if (r.get("force_failure_count") or 0) > 0]

        warnings: list[dict[str, Any]] = []
        if stale:
            warnings.append(
                {
                    "type": "stale_forced_plans",
                    "message": (
                        f"{len(stale)} forced plan(s) haven't executed in over 7 days, or "
                        "have no execution left in Query Store — may be stale or the "
                        "query pattern changed"
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


def parse_tuning_recommendation(row: Mapping[str, Any]) -> dict[str, Any]:
    """Flatten one raw sys.dm_db_tuning_recommendations row.

    ``state`` is JSON {currentValue, reason}. ``details`` is JSON with the ids
    and pre-detection stats under ``planForceDetails`` (CPU averages in
    microseconds) and the script under ``implementationDetails``. Unreadable
    JSON yields None fields, never an error; the raw text stays in ``details``.
    """
    state = _json_object(row.get("state"))
    parsed_details = _load_json(row.get("details"))
    details = parsed_details if isinstance(parsed_details, dict) else {}
    force = _json_object(details.get("planForceDetails"))
    implementation = _json_object(details.get("implementationDetails"))

    regressed_exec = _int_or_none(force.get("regressedPlanExecutionCount"))
    recommended_exec = _int_or_none(force.get("recommendedPlanExecutionCount"))
    regressed_cpu = _float_or_none(force.get("regressedPlanCpuTimeAverage"))
    recommended_cpu = _float_or_none(force.get("recommendedPlanCpuTimeAverage"))
    # Microsoft's samples read *ErrorCount; the column reference says *AbortedCount.
    regressed_errors = _int_or_none(
        force.get("regressedPlanErrorCount", force.get("regressedPlanAbortedCount"))
    )
    recommended_errors = _int_or_none(
        force.get("recommendedPlanErrorCount", force.get("recommendedPlanAbortedCount"))
    )

    estimated_cpu_gain = None
    if (
        regressed_exec is not None
        and recommended_exec is not None
        and regressed_cpu is not None
        and recommended_cpu is not None
    ):
        # Documented estimate: total executions times the per-execution CPU
        # difference, microseconds to seconds.
        estimated_cpu_gain = (
            (regressed_exec + recommended_exec) * (regressed_cpu - recommended_cpu) / 1_000_000
        )
    error_prone = (
        regressed_errors > recommended_errors
        if regressed_errors is not None and recommended_errors is not None
        else None
    )
    current_state = state.get("currentValue")

    return {
        "type": row.get("type"),
        "reason": row.get("reason"),
        "score": row.get("score"),
        "current_state": current_state if isinstance(current_state, str) else None,
        "state_reason": state.get("reason"),
        "tuning_script": implementation.get("script"),
        "query_id": _int_or_none(force.get("queryId")),
        "regressed_plan_id": _int_or_none(force.get("regressedPlanId")),
        "recommended_plan_id": _int_or_none(force.get("recommendedPlanId")),
        "regressed_plan_execution_count": regressed_exec,
        "recommended_plan_execution_count": recommended_exec,
        "regressed_plan_error_count": regressed_errors,
        "recommended_plan_error_count": recommended_errors,
        "regressed_plan_cpu_time_average_us": regressed_cpu,
        "recommended_plan_cpu_time_average_us": recommended_cpu,
        "estimated_cpu_gain": estimated_cpu_gain,
        "estimated_cpu_gain_unit": "cpu_seconds",
        # Not in the DMV; kept as null for output compatibility.
        "estimated_duration_gain": None,
        "error_prone": error_prone,
        "is_executable_action": row.get("is_executable_action"),
        "is_revertable_action": row.get("is_revertable_action"),
        "execute_action_initiated_by": row.get("execute_action_initiated_by"),
        "revert_action_initiated_by": row.get("revert_action_initiated_by"),
        "valid_since": row.get("valid_since"),
        "last_refresh": row.get("last_refresh"),
        "details": parsed_details if parsed_details is not None else row.get("details"),
    }


def _annotate_plan_activity(
    recommendation: dict[str, Any],
    activity: Mapping[int, Mapping[str, Any]],
) -> None:
    regressed_id = recommendation["regressed_plan_id"]
    recommended_id = recommendation["recommended_plan_id"]
    regressed = activity.get(regressed_id) if regressed_id is not None else None
    recommended = activity.get(recommended_id) if recommended_id is not None else None
    regressed_runs = _int_or_none((regressed or {}).get("recent_execution_count")) or 0
    recommended_runs = _int_or_none((recommended or {}).get("recent_execution_count")) or 0
    seen = [
        row["last_seen_utc"]
        for row in (regressed, recommended)
        if row is not None and row.get("last_seen_utc") is not None
    ]
    recommendation["regressed_plan_recent_execution_count"] = regressed_runs
    recommendation["recommended_plan_recent_execution_count"] = recommended_runs
    recommendation["recent_execution_count"] = regressed_runs + recommended_runs
    recommendation["last_seen_utc"] = max(seen) if seen else None
    # None: the plan is no longer in Query Store, so forcing it would fail.
    recommendation["recommended_plan_is_forced"] = (
        bool(recommended.get("is_forced_plan")) if recommended is not None else None
    )
    # Live: the regressed plan is still running in the window.
    recommendation["live"] = regressed_runs > 0


def _load_json(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None


def _json_object(value: Any) -> dict[str, Any]:
    loaded = _load_json(value)
    return loaded if isinstance(loaded, dict) else {}


def _int_or_none(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _plan_ratio(a: Mapping[str, Any], b: Mapping[str, Any], key: str) -> float | None:
    """b / a for one metric; None when either plan has no Regular-run average."""
    a_value, b_value = _float_or_none(a.get(key)), _float_or_none(b.get(key))
    if a_value is None or b_value is None:
        return None
    return round((b_value or 1) / max(a_value or 1, 0.001), 2)
