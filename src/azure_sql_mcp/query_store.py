from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any

from .connection import AzureSqlExecutor


QUERY_STORE_SHOWPLAN_NAMESPACE = {
    "sp": "http://schemas.microsoft.com/sqlserver/2004/07/showplan"
}


def _validate_query_hash(query_hash: str) -> str:
    normalized_hash = (query_hash or "").strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{16}", normalized_hash):
        raise ValueError("query_hash must be a 0x-prefixed hex string for binary(8).")
    return normalized_hash

def _escape_like_pattern(value: str) -> str:
    """Escape LIKE wildcards so query text matches as a literal substring.

    Query text routinely contains % and _ (LIKE clauses, identifiers); left
    unescaped they turn the fingerprint into a wildcard pattern.
    """
    return (
        value.replace("[", "[[]")
        .replace("%", "[%]")
        .replace("_", "[_]")
    )


def _weighted_runtime_average(metric: str) -> str:
    """Return a count-weighted Query Store runtime-stat average expression."""

    allowed_metrics = {
        "avg_cpu_time",
        "avg_duration",
        "avg_logical_io_reads",
        "avg_physical_io_reads",
        "avg_query_max_used_memory",
        "avg_rowcount",
    }
    if metric not in allowed_metrics:
        raise ValueError(f"Unsupported Query Store runtime metric: {metric}")
    return (
        f"SUM(rs.{metric} * rs.count_executions) "
        "/ NULLIF(SUM(rs.count_executions), 0)"
    )


_WEIGHTED_AVG_DURATION = _weighted_runtime_average("avg_duration")
_WEIGHTED_AVG_CPU = _weighted_runtime_average("avg_cpu_time")
_WEIGHTED_AVG_LOGICAL_READS = _weighted_runtime_average("avg_logical_io_reads")
_WEIGHTED_AVG_PHYSICAL_READS = _weighted_runtime_average("avg_physical_io_reads")
_WEIGHTED_AVG_MEMORY = _weighted_runtime_average("avg_query_max_used_memory")
_WEIGHTED_AVG_ROWS = _weighted_runtime_average("avg_rowcount")


SORT_BY_EXPRESSIONS = {
    "total_duration": "SUM(rs.avg_duration * rs.count_executions)",
    "avg_duration": _WEIGHTED_AVG_DURATION,
    "cpu": "SUM(rs.avg_cpu_time * rs.count_executions)",
    "executions": "SUM(rs.count_executions)",
    "logical_io": "SUM(rs.avg_logical_io_reads * rs.count_executions)",
    "physical_io": "SUM(rs.avg_physical_io_reads * rs.count_executions)",
    "memory": _WEIGHTED_AVG_MEMORY,
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

    async def resolve_query_identity(
        self,
        database_name: str,
        sql: str,
    ) -> dict[str, Any]:
        """Resolve exact Query Store text to one stable query identity.

        Similar-text matching is deliberately excluded. Multiple exact
        identities are returned as ambiguous rather than silently selecting
        one context/settings combination.
        """

        if not isinstance(sql, str) or not sql:
            raise ValueError("sql must not be empty.")
        rows = await self.executor.fetch_all(
            database_name,
            """
            SELECT TOP (20)
                q.query_id,
                CONVERT(varchar(18), q.query_hash, 1) AS query_hash,
                q.context_settings_id,
                q.object_id
            FROM sys.query_store_query_text AS qt
            INNER JOIN sys.query_store_query AS q
                ON q.query_text_id = qt.query_text_id
            WHERE DATALENGTH(qt.query_sql_text)
                    = DATALENGTH(CAST(? AS nvarchar(max)))
              AND qt.query_sql_text COLLATE Latin1_General_100_BIN2
                    = CAST(? AS nvarchar(max)) COLLATE Latin1_General_100_BIN2
            ORDER BY q.last_execution_time DESC, q.query_id DESC
            """,
            params=[sql, sql],
        )
        if not rows:
            return {
                "status": "not_found",
                "identity_kind": "exact_query_store_text",
                "matches": [],
            }
        identities = {
            (int(row["query_id"]), str(row["query_hash"])) for row in rows
        }
        if len(identities) != 1:
            return {
                "status": "ambiguous",
                "identity_kind": "exact_query_store_text",
                "matches": rows,
            }
        query_id, query_hash = next(iter(identities))
        return {
            "status": "resolved",
            "identity_kind": "query_id",
            "query_id": query_id,
            "query_hash": query_hash,
            "matches": rows,
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

    async def get_query_history(
        self,
        database_name: str,
        *,
        query_id: int | None = None,
        query_hash: str | None = None,
        sql: str | None = None,
        window_minutes: int = 1440,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Resolve Query Store evidence by stable identity before fuzzy text.

        Query ID is preferred when present, then query hash, and only then the
        text matcher. This ordering prevents a nearby SQL string from silently
        becoming the evidence source for a different query identity.
        """
        if query_id is not None:
            return await self.get_query_history_by_id(
                database_name,
                query_id,
                window_minutes=window_minutes,
                limit=limit,
            )
        if query_hash:
            return await self.get_query_history_by_hash(
                database_name,
                query_hash,
                window_minutes=window_minutes,
                limit=limit,
            )
        if sql is not None:
            return await self.get_query_history_by_text(
                database_name,
                sql,
                window_minutes=window_minutes,
                limit=limit,
            )
        raise ValueError("query_id, query_hash, or sql is required.")

    async def get_query_history_by_id(
        self,
        database_name: str,
        query_id: int,
        window_minutes: int = 1440,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Return Query Store history for one stable ``query_id``."""
        self._validate_window_and_limit(window_minutes, limit)
        if query_id <= 0:
            raise ValueError("query_id must be greater than 0.")
        query = f"""
        SELECT TOP (?)
            q.query_id,
            q.query_hash,
            p.plan_id,
            p.query_plan_hash,
            q.query_parameterization_type_desc,
            p.plan_type_desc,
            p.is_forced_plan,
            p.force_failure_count,
            p.last_force_failure_reason_desc,
            qt.query_sql_text,
            SUM(rs.count_executions) AS executions,
            SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS total_duration_ms,
            {_WEIGHTED_AVG_DURATION} / 1000.0 AS avg_duration_ms,
            SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS total_cpu_ms,
            {_WEIGHTED_AVG_CPU} / 1000.0 AS avg_cpu_ms,
            SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
            SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_io_reads,
            {_WEIGHTED_AVG_LOGICAL_READS} AS avg_logical_io_reads,
            {_WEIGHTED_AVG_PHYSICAL_READS} AS avg_physical_io_reads,
            {_WEIGHTED_AVG_MEMORY} AS avg_query_max_used_memory,
            MIN(rs.avg_rowcount) AS min_avg_rowcount,
            MAX(rs.avg_rowcount) AS max_avg_rowcount,
            {_WEIGHTED_AVG_ROWS} AS avg_rowcount,
            MIN(rsi.start_time) AS first_seen_utc,
            MAX(rsi.end_time) AS last_seen_utc
        FROM sys.query_store_query AS q
        INNER JOIN sys.query_store_query_text AS qt
            ON qt.query_text_id = q.query_text_id
        INNER JOIN sys.query_store_plan AS p
            ON q.query_id = p.query_id
        INNER JOIN sys.query_store_runtime_stats AS rs
            ON p.plan_id = rs.plan_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
          AND q.query_id = ?
        GROUP BY
            q.query_id,
            q.query_hash,
            p.plan_id,
            p.query_plan_hash,
            q.query_parameterization_type_desc,
            p.plan_type_desc,
            p.is_forced_plan,
            p.force_failure_count,
            p.last_force_failure_reason_desc,
            qt.query_sql_text
        ORDER BY total_cpu_ms DESC, total_duration_ms DESC
        """
        rows = await self.executor.fetch_all(
            database_name,
            query,
            params=[limit, window_minutes, int(query_id)],
        )
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "query_id": query_id,
            "identity_kind": "query_id",
            "matches": rows,
        }

    async def get_query_history_by_hash(
        self,
        database_name: str,
        query_hash: str,
        window_minutes: int = 1440,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Query Store history matched by query_hash (from SHOWPLAN XML).

        Hash matching survives parameter renaming (@CustomerId vs @P1) and
        whitespace differences that defeat text matching.
        """
        normalized_hash = _validate_query_hash(query_hash)
        self._validate_window_and_limit(window_minutes, limit)

        query = f"""
        SELECT TOP (?)
            q.query_id,
            p.plan_id,
            p.is_forced_plan,
            p.force_failure_count,
            p.last_force_failure_reason_desc,
            qt.query_sql_text,
            SUM(rs.count_executions) AS executions,
            SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS total_duration_ms,
            {_WEIGHTED_AVG_DURATION} / 1000.0 AS avg_duration_ms,
            SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS total_cpu_ms,
            {_WEIGHTED_AVG_CPU} / 1000.0 AS avg_cpu_ms,
            SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
            SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_io_reads,
            {_WEIGHTED_AVG_LOGICAL_READS} AS avg_logical_io_reads,
            {_WEIGHTED_AVG_PHYSICAL_READS} AS avg_physical_io_reads,
            {_WEIGHTED_AVG_MEMORY} AS avg_query_max_used_memory,
            {_WEIGHTED_AVG_ROWS} AS avg_rowcount,
            MIN(rsi.start_time) AS first_seen_utc,
            MAX(rsi.end_time) AS last_seen_utc
        FROM sys.query_store_query AS q
        INNER JOIN sys.query_store_query_text AS qt
            ON qt.query_text_id = q.query_text_id
        INNER JOIN sys.query_store_plan AS p
            ON q.query_id = p.query_id
        INNER JOIN sys.query_store_runtime_stats AS rs
            ON p.plan_id = rs.plan_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
          AND q.query_hash = CONVERT(BINARY(8), ?, 1)
        GROUP BY
            q.query_id,
            p.plan_id,
            p.is_forced_plan,
            p.force_failure_count,
            p.last_force_failure_reason_desc,
            qt.query_sql_text
        ORDER BY total_cpu_ms DESC, total_duration_ms DESC
        """
        rows = await self.executor.fetch_all(
            database_name,
            query,
            params=[limit, window_minutes, normalized_hash],
        )
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "query_hash": normalized_hash,
            "identity_kind": "query_hash",
            "matches": rows,
        }

    async def get_query_history_by_text(
        self,
        database_name: str,
        sql: str,
        window_minutes: int = 1440,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Best-effort Query Store history for SQL text similar to the supplied query."""
        fingerprint = " ".join(sql.split())[:1000]
        if not fingerprint:
            raise ValueError("sql must not be empty.")
        self._validate_window_and_limit(window_minutes, limit)

        query = f"""
        SELECT TOP (?)
            q.query_id,
            p.plan_id,
            p.is_forced_plan,
            p.force_failure_count,
            p.last_force_failure_reason_desc,
            qt.query_sql_text,
            SUM(rs.count_executions) AS executions,
            SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS total_duration_ms,
            {_WEIGHTED_AVG_DURATION} / 1000.0 AS avg_duration_ms,
            SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS total_cpu_ms,
            {_WEIGHTED_AVG_CPU} / 1000.0 AS avg_cpu_ms,
            SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
            SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_io_reads,
            {_WEIGHTED_AVG_LOGICAL_READS} AS avg_logical_io_reads,
            {_WEIGHTED_AVG_PHYSICAL_READS} AS avg_physical_io_reads,
            {_WEIGHTED_AVG_MEMORY} AS avg_query_max_used_memory,
            {_WEIGHTED_AVG_ROWS} AS avg_rowcount,
            MIN(rsi.start_time) AS first_seen_utc,
            MAX(rsi.end_time) AS last_seen_utc
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
          AND (
                qt.query_sql_text LIKE '%' + ? + '%'
             OR ? LIKE '%' + LEFT(qt.query_sql_text, 1000) + '%'
          )
        GROUP BY
            q.query_id,
            p.plan_id,
            p.is_forced_plan,
            p.force_failure_count,
            p.last_force_failure_reason_desc,
            qt.query_sql_text
        ORDER BY total_cpu_ms DESC, total_duration_ms DESC
        """
        rows = await self.executor.fetch_all(
            database_name,
            query,
            params=[limit, window_minutes, _escape_like_pattern(fingerprint), fingerprint],
        )
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "fingerprint_length": len(fingerprint),
            "identity_kind": "fuzzy_text",
            "matches": rows,
        }

    async def get_parameter_runtime_buckets(
        self,
        database_name: str,
        *,
        query_id: int | None = None,
        query_hash: str | None = None,
        window_minutes: int = 1440,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Expose compiled-parameter and runtime buckets for stable identity.

        Query Store does not record every runtime parameter value. The returned
        bucket therefore distinguishes compiled values from the runtime stats
        observed for the corresponding plan/interval; it never labels an
        inferred value as a runtime value.
        """
        self._validate_window_and_limit(window_minutes, limit)
        if query_id is None and query_hash is None:
            raise ValueError("query_id or query_hash is required.")
        if query_id is not None and query_id <= 0:
            raise ValueError("query_id must be greater than 0.")

        if query_id is not None:
            identity_clause = "q.query_id = ?"
            identity_params: list[Any] = [int(query_id)]
            identity_payload = {"query_id": int(query_id), "identity_kind": "query_id"}
        else:
            normalized_hash = _validate_query_hash(str(query_hash))
            identity_clause = "q.query_hash = CONVERT(BINARY(8), ?, 1)"
            identity_params = [normalized_hash]
            identity_payload = {
                "query_hash": normalized_hash,
                "identity_kind": "query_hash",
            }

        query = f"""
        SELECT TOP (?)
            q.query_id,
            q.query_hash,
            p.plan_id,
            p.query_plan_hash,
            q.query_parameterization_type_desc,
            p.plan_type_desc,
            CAST(p.query_plan AS nvarchar(max)) AS query_plan_xml,
            rs.execution_type_desc,
            SUM(rs.count_executions) AS executions,
            MIN(rs.avg_duration) / 1000.0 AS min_avg_duration_ms,
            MAX(rs.avg_duration) / 1000.0 AS max_avg_duration_ms,
            {_WEIGHTED_AVG_DURATION} / 1000.0 AS avg_duration_ms,
            {_WEIGHTED_AVG_CPU} / 1000.0 AS avg_cpu_ms,
            {_WEIGHTED_AVG_LOGICAL_READS} AS avg_logical_io_reads,
            {_WEIGHTED_AVG_PHYSICAL_READS} AS avg_physical_io_reads,
            {_WEIGHTED_AVG_ROWS} AS avg_rowcount,
            MIN(rsi.start_time) AS first_seen_utc,
            MAX(rsi.end_time) AS last_seen_utc
        FROM sys.query_store_query AS q
        INNER JOIN sys.query_store_plan AS p
            ON q.query_id = p.query_id
        INNER JOIN sys.query_store_runtime_stats AS rs
            ON p.plan_id = rs.plan_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
          AND {identity_clause}
        GROUP BY
            q.query_id,
            q.query_hash,
            p.plan_id,
            p.query_plan_hash,
            q.query_parameterization_type_desc,
            p.plan_type_desc,
            CAST(p.query_plan AS nvarchar(max)),
            rs.execution_type_desc
        ORDER BY executions DESC, avg_duration_ms DESC
        """
        rows = await self.executor.fetch_all(
            database_name,
            query,
            params=[limit, window_minutes, *identity_params],
        )
        buckets: list[dict[str, Any]] = []
        distinct_parameter_sets: set[tuple[tuple[str, Any], ...]] = set()
        for raw_row in rows:
            row = dict(raw_row)
            plan_xml = row.pop("query_plan_xml", None) or ""
            parameters = self._extract_plan_parameters(str(plan_xml))
            row["compiled_parameters"] = parameters
            runtime_parameters = [
                parameter
                for parameter in parameters
                if parameter.get("runtime_value") is not None
            ]
            row["runtime_parameters"] = runtime_parameters
            row["runtime_parameter_values_observed"] = bool(runtime_parameters)
            row["runtime_bucket_source"] = "query_store_runtime_stats_by_plan_and_interval"
            row["runtime_bucket_key"] = {
                "plan_id": row.get("plan_id"),
                "execution_type": row.get("execution_type_desc"),
            }
            buckets.append(row)
            key = tuple(
                (parameter["name"], parameter.get("compiled_value"))
                for parameter in parameters
            )
            if key:
                distinct_parameter_sets.add(key)

        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            **identity_payload,
            "buckets": buckets,
            "bucket_count": len(buckets),
            "plan_count": len({bucket.get("plan_id") for bucket in buckets}),
            "distinct_compiled_parameter_sets": [
                [{"name": name, "compiled_value": value} for name, value in key]
                for key in sorted(distinct_parameter_sets, key=repr)
            ],
            "runtime_values_note": (
                "Query Store runtime stats are grouped by plan and interval; "
                "compiled plan values are not runtime value samples."
            ),
        }

    async def get_query_parameter_buckets(
        self,
        database_name: str,
        query_id: int,
    ) -> dict[str, Any]:
        """Compatibility alias for the stable query-id bucket API."""
        return await self.get_parameter_runtime_buckets(
            database_name,
            query_id=query_id,
        )

    @staticmethod
    def _extract_plan_parameters(plan_xml: str) -> list[dict[str, Any]]:
        if not plan_xml.strip():
            return []
        try:
            root = ET.fromstring(plan_xml)
        except ET.ParseError:
            return []
        parameters: list[dict[str, Any]] = []
        for node in root.findall(
            ".//sp:ParameterList/sp:ColumnReference",
            QUERY_STORE_SHOWPLAN_NAMESPACE,
        ):
            name = node.attrib.get("Column")
            if not name:
                continue
            parameters.append(
                {
                    "name": name,
                    "data_type": node.attrib.get("ParameterDataType"),
                    "compiled_value": node.attrib.get("ParameterCompiledValue"),
                    "runtime_value": node.attrib.get("ParameterRuntimeValue"),
                }
            )
        return parameters

    @staticmethod
    def _validate_window_and_limit(window_minutes: int, limit: int) -> None:
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")
        if limit <= 0:
            raise ValueError("limit must be greater than 0.")

    def _build_top_queries_query(self, sort_by: str) -> str:
        if sort_by == "resource_blend":
            return f"""
            WITH QueryMetrics AS (
                SELECT
                    q.query_id,
                    p.plan_id,
                    qt.query_sql_text,
                    SUM(rs.count_executions) AS executions,
                    SUM(rs.avg_duration * rs.count_executions) AS total_duration_us,
                    {_WEIGHTED_AVG_DURATION} AS avg_duration_us,
                    SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu_us,
                    SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
                    SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_io_reads,
                    {_WEIGHTED_AVG_LOGICAL_READS} AS avg_logical_io_reads,
                    {_WEIGHTED_AVG_PHYSICAL_READS} AS avg_physical_io_reads,
                    {_WEIGHTED_AVG_MEMORY} AS avg_query_max_used_memory,
                    {_WEIGHTED_AVG_ROWS} AS avg_rowcount,
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
            {_WEIGHTED_AVG_DURATION} AS avg_duration_us,
            SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu_us,
            SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
            SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_io_reads,
            {_WEIGHTED_AVG_LOGICAL_READS} AS avg_logical_io_reads,
            {_WEIGHTED_AVG_PHYSICAL_READS} AS avg_physical_io_reads,
            {_WEIGHTED_AVG_MEMORY} AS avg_query_max_used_memory,
            {_WEIGHTED_AVG_ROWS} AS avg_rowcount,
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
