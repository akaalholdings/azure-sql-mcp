from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

from .connection import AzureSqlExecutor
from .index_recommendations import build_create_index_statement
from .index_recommendations import split_index_columns
from .observability import sanitize_error_message
from .query_text import strip_query_store_parameter_declarations
from .safe_sql import SafeSqlValidator

SHOWPLAN_NAMESPACE = {"sp": "http://schemas.microsoft.com/sqlserver/2004/07/showplan"}


class QueryIndexAnalysisService:
    """Analyze specific SQL queries for missing-index recommendations via execution plans."""

    def __init__(self, executor: AzureSqlExecutor, validator: SafeSqlValidator):
        self.executor = executor
        self.validator = validator

    async def analyze_queries(
        self,
        database_name: str,
        queries: list[str],
    ) -> dict[str, Any]:
        if len(queries) > 10:
            raise ValueError("Maximum 10 queries per analysis.")
        if not queries:
            raise ValueError("At least one query is required.")

        all_recommendations: list[dict[str, Any]] = []
        query_details: list[dict[str, Any]] = []
        existing_indexes = await self._get_existing_indexes(database_name)

        for sql in queries:
            normalized_sql = strip_query_store_parameter_declarations(sql)
            validated = self.validator.validate_read_only(normalized_sql)
            plan_xml = await self._get_estimated_plan(database_name, validated.normalized_sql)
            missing = self._extract_missing_indexes(plan_xml)
            query_details.append({
                "sql": validated.normalized_sql,
                "missing_index_count": len(missing),
            })
            for rec in missing:
                rec["source_query"] = validated.normalized_sql
            all_recommendations.extend(missing)

        consolidated = self._consolidate_recommendations(
            all_recommendations,
            existing_indexes=existing_indexes,
        )
        return {
            "database_name": database_name,
            "queries_analyzed": len(queries),
            "query_details": query_details,
            "recommendations": consolidated,
        }

    async def analyze_workload(
        self,
        database_name: str,
        window_minutes: int = 60,
        top_n: int = 20,
    ) -> dict[str, Any]:
        """Pull top resource-heavy queries from Query Store and analyze for index recommendations."""
        top_queries = await self._get_top_workload_queries(
            database_name, window_minutes, top_n
        )
        existing_indexes = await self._get_existing_indexes(database_name)

        all_recommendations: list[dict[str, Any]] = []
        analyzed_queries: list[dict[str, Any]] = []

        for row in top_queries:
            sql_text = row.get("query_sql_text", "")
            if not sql_text or not sql_text.strip():
                continue
            try:
                normalized_sql = strip_query_store_parameter_declarations(sql_text)
                validated = self.validator.validate_read_only(normalized_sql)
                plan_xml = await self._get_estimated_plan(database_name, validated.normalized_sql)
                missing = self._extract_missing_indexes(plan_xml)
                for rec in missing:
                    rec["source_query"] = validated.normalized_sql
                    rec["query_id"] = row.get("query_id")
                    rec["plan_id"] = row.get("plan_id")
                    rec["query_total_cpu_us"] = self._to_float(row.get("total_cpu_us"))
                    rec["query_total_duration_us"] = self._to_float(
                        row.get("total_duration_us")
                    )
                    rec["query_total_logical_io"] = self._to_float(
                        row.get("total_logical_io_reads")
                    )
                    rec["query_executions"] = self._to_float(row.get("executions"))
                all_recommendations.extend(missing)
                analyzed_queries.append({
                    "query_id": row.get("query_id"),
                    "sql": validated.normalized_sql[:200],
                    "missing_index_count": len(missing),
                })
            except Exception as exc:
                analyzed_queries.append({
                    "query_id": row.get("query_id"),
                    "sql": sql_text[:200],
                    "missing_index_count": 0,
                    "error": sanitize_error_message(str(exc)),
                })

        # Cross-reference with DMV missing indexes
        dmv_recommendations = await self._get_dmv_missing_indexes(database_name)

        consolidated = self._consolidate_recommendations(
            all_recommendations,
            existing_indexes=existing_indexes,
            dmv_recommendations=dmv_recommendations,
        )
        for rank, rec in enumerate(consolidated, start=1):
            rec["rank"] = rank

        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "queries_analyzed": len(analyzed_queries),
            "analyzed_queries": analyzed_queries,
            "recommendations": consolidated,
            "dmv_recommendations": dmv_recommendations,
        }

    async def _get_estimated_plan(self, database_name: str, sql: str) -> str:
        per_statement_results = await self.executor.execute_session(
            database_name,
            ["SET SHOWPLAN_XML ON", sql, "SET SHOWPLAN_XML OFF"],
        )
        plan_results = per_statement_results[1] if len(per_statement_results) > 1 else []
        for result in plan_results:
            for row in result.rows:
                for value in row.values():
                    if isinstance(value, str) and value.lstrip().startswith("<ShowPlanXML"):
                        return value
        raise RuntimeError("No SHOWPLAN XML returned.")

    def _extract_missing_indexes(self, plan_xml: str) -> list[dict[str, Any]]:
        root = ET.fromstring(plan_xml)
        results: list[dict[str, Any]] = []
        for group in root.findall(".//sp:MissingIndexGroup", SHOWPLAN_NAMESPACE):
            impact = float(group.attrib.get("Impact", "0"))
            for idx in group.findall("sp:MissingIndex", SHOWPLAN_NAMESPACE):
                schema = idx.attrib.get("Schema", "[dbo]").strip("[]")
                table = idx.attrib.get("Table", "").strip("[]")
                equality_cols = self._extract_column_group(idx, "EQUALITY")
                inequality_cols = self._extract_column_group(idx, "INEQUALITY")
                include_cols = self._extract_column_group(idx, "INCLUDE")
                key_columns = equality_cols + inequality_cols
                if not key_columns:
                    continue
                create_sql = build_create_index_statement(
                    schema_name=schema,
                    table_name=table,
                    key_columns=key_columns,
                    include_columns=include_cols,
                )
                results.append({
                    "schema": schema,
                    "table": table,
                    "equality_columns": equality_cols,
                    "inequality_columns": inequality_cols,
                    "include_columns": include_cols,
                    "impact_pct": impact,
                    "create_index_sql": create_sql,
                })
        return results

    def _extract_column_group(self, index_node, usage: str) -> list[str]:
        columns: list[str] = []
        for cg in index_node.findall("sp:ColumnGroup", SHOWPLAN_NAMESPACE):
            if cg.attrib.get("Usage") == usage:
                for col in cg.findall("sp:Column", SHOWPLAN_NAMESPACE):
                    name = col.attrib.get("Name", "").strip("[]")
                    if name:
                        columns.append(name)
        return columns

    def _consolidate_recommendations(
        self,
        recommendations: list[dict[str, Any]],
        *,
        existing_indexes: set[tuple[str, str, tuple[str, ...]]] | None = None,
        dmv_recommendations: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Merge recommendations for the same table + key columns."""
        seen: dict[tuple[str, str, tuple[str, ...]], dict[str, Any]] = {}
        dmv_lookup = self._build_dmv_lookup(dmv_recommendations or [])

        for rec in recommendations:
            signature = self._recommendation_signature(rec)
            if existing_indexes and signature in existing_indexes:
                continue

            if signature in seen:
                existing = seen[signature]
                existing["impact_pct"] = max(existing["impact_pct"], rec["impact_pct"])
                existing["cumulative_impact_pct"] = round(
                    existing.get("cumulative_impact_pct", 0.0) + rec.get("impact_pct", 0.0),
                    2,
                )
                existing["query_total_cpu_us"] += self._to_float(rec.get("query_total_cpu_us"))
                existing["query_total_duration_us"] += self._to_float(
                    rec.get("query_total_duration_us")
                )
                existing["query_total_logical_io"] += self._to_float(
                    rec.get("query_total_logical_io")
                )
                existing["query_executions"] += self._to_float(rec.get("query_executions"))
                existing["source_queries"].add(rec.get("source_query", ""))
                existing["query_ids"].update(
                    query_id
                    for query_id in [rec.get("query_id")]
                    if query_id is not None
                )
                existing["include_columns"] = self._merge_unique_columns(
                    existing.get("include_columns", []),
                    rec.get("include_columns", []),
                )
            else:
                aggregate = dict(rec)
                aggregate["include_columns"] = self._merge_unique_columns(
                    rec.get("include_columns", []),
                    [],
                )
                aggregate["source_queries"] = {
                    rec.get("source_query", "")
                } - {""}
                aggregate["query_ids"] = {
                    query_id
                    for query_id in [rec.get("query_id")]
                    if query_id is not None
                }
                aggregate["query_total_cpu_us"] = self._to_float(rec.get("query_total_cpu_us"))
                aggregate["query_total_duration_us"] = self._to_float(
                    rec.get("query_total_duration_us")
                )
                aggregate["query_total_logical_io"] = self._to_float(
                    rec.get("query_total_logical_io")
                )
                aggregate["query_executions"] = self._to_float(rec.get("query_executions"))
                aggregate["cumulative_impact_pct"] = round(rec.get("impact_pct", 0.0), 2)
                seen[signature] = aggregate

        result = list(seen.values())
        for rec in result:
            rec["affected_queries"] = len(rec.pop("source_queries", set())) or 1
            rec["query_ids"] = sorted(rec.get("query_ids", set()))
            rec["key_columns"] = rec.get("equality_columns", []) + rec.get(
                "inequality_columns", []
            )
            rec["create_index_sql"] = build_create_index_statement(
                schema_name=rec["schema"],
                table_name=rec["table"],
                key_columns=rec["key_columns"],
                include_columns=rec.get("include_columns", []),
            )
            rec["impact_score"] = round(
                (
                    rec.get("impact_pct", 0.0) / 100.0
                    * max(rec.get("query_total_cpu_us", 0.0), 0.0)
                    * max(rec.get("query_executions", 1.0), 1.0)
                )
                / 1_000_000.0,
                2,
            )
            dmv_match = dmv_lookup.get(self._recommendation_signature(rec))
            if dmv_match:
                rec["dmv_match"] = {
                    "user_seeks": dmv_match.get("user_seeks"),
                    "avg_user_impact": dmv_match.get("avg_user_impact"),
                    "estimated_improvement": dmv_match.get("estimated_improvement"),
                }
                rec["estimated_size_kb"] = dmv_match.get("estimated_size_kb")
            else:
                rec["estimated_size_kb"] = None

        result.sort(
            key=lambda r: (r.get("impact_score", 0.0), r.get("impact_pct", 0.0)),
            reverse=True,
        )
        return result

    def _same_index(self, a: dict[str, Any], b: dict[str, Any]) -> bool:
        return self._recommendation_signature(a) == self._recommendation_signature(b)

    async def _get_top_workload_queries(
        self, database_name: str, window_minutes: int, top_n: int
    ) -> list[dict[str, Any]]:
        query = """
        SELECT TOP (?)
            q.query_id,
            p.plan_id,
            qt.query_sql_text,
            SUM(rs.count_executions) AS executions,
            SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu_us,
            SUM(rs.avg_duration * rs.count_executions) AS total_duration_us,
            SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads
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
        ORDER BY SUM(rs.avg_cpu_time * rs.count_executions) DESC
        """
        return await self.executor.fetch_all(
            database_name, query, params=[top_n, window_minutes]
        )

    async def _get_dmv_missing_indexes(self, database_name: str) -> list[dict[str, Any]]:
        query = """
        SELECT TOP (20)
            OBJECT_SCHEMA_NAME(mid.object_id) AS schema_name,
            OBJECT_NAME(mid.object_id) AS table_name,
            mid.equality_columns,
            mid.inequality_columns,
            mid.included_columns,
            migs.user_seeks,
            migs.avg_total_user_cost,
            migs.avg_user_impact,
            CAST(NULL AS BIGINT) AS estimated_size_kb,
            CAST(
                (migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans))
                AS DECIMAL(18, 2)
            ) AS estimated_improvement
        FROM sys.dm_db_missing_index_details AS mid
        INNER JOIN sys.dm_db_missing_index_groups AS mig
            ON mid.index_handle = mig.index_handle
        INNER JOIN sys.dm_db_missing_index_group_stats AS migs
            ON mig.index_group_handle = migs.group_handle
        WHERE mid.database_id = DB_ID()
        ORDER BY estimated_improvement DESC
        """
        rows = await self.executor.fetch_all(database_name, query)
        for row in rows:
            row["schema"] = row.get("schema_name") or "dbo"
            row["table"] = row.get("table_name") or ""
            row["equality_columns"] = split_index_columns(row.get("equality_columns"))
            row["inequality_columns"] = split_index_columns(row.get("inequality_columns"))
            row["include_columns"] = split_index_columns(row.get("included_columns"))
            row["create_index_sql"] = build_create_index_statement(
                schema_name=row["schema"],
                table_name=row["table"],
                key_columns=row["equality_columns"] + row["inequality_columns"],
                include_columns=row["include_columns"],
            )
        return rows

    async def _get_existing_indexes(
        self,
        database_name: str,
    ) -> set[tuple[str, str, tuple[str, ...]]]:
        query = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            i.index_id,
            i.name AS index_name,
            ic.is_included_column,
            ic.key_ordinal,
            c.name AS column_name
        FROM sys.indexes AS i
        INNER JOIN sys.tables AS t
            ON i.object_id = t.object_id
        INNER JOIN sys.schemas AS s
            ON t.schema_id = s.schema_id
        INNER JOIN sys.index_columns AS ic
            ON i.object_id = ic.object_id
            AND i.index_id = ic.index_id
        INNER JOIN sys.columns AS c
            ON ic.object_id = c.object_id
            AND ic.column_id = c.column_id
        WHERE i.is_hypothetical = 0
            AND i.index_id > 0
        ORDER BY s.name, t.name, i.index_id, ic.key_ordinal, ic.index_column_id
        """
        rows = await self.executor.fetch_all(database_name, query)
        by_index: dict[tuple[str, str, Any], list[tuple[int, str]]] = {}
        for row in rows:
            if row.get("is_included_column"):
                continue
            key = (
                row.get("schema_name") or "dbo",
                row.get("table_name") or "",
                row.get("index_id"),
            )
            by_index.setdefault(key, []).append(
                (int(row.get("key_ordinal") or 0), row.get("column_name") or "")
            )

        signatures: set[tuple[str, str, tuple[str, ...]]] = set()
        for (schema_name, table_name, _index_id), columns in by_index.items():
            ordered_columns = tuple(
                column_name
                for _, column_name in sorted(columns, key=lambda item: item[0])
                if column_name
            )
            if ordered_columns:
                signatures.add((schema_name, table_name, ordered_columns))
        return signatures

    def _recommendation_signature(
        self,
        recommendation: dict[str, Any],
    ) -> tuple[str, str, tuple[str, ...]]:
        key_columns = tuple(
            recommendation.get("equality_columns", [])
            + recommendation.get("inequality_columns", [])
        )
        return (
            recommendation.get("schema", "dbo"),
            recommendation.get("table", ""),
            key_columns,
        )

    def _build_dmv_lookup(
        self,
        dmv_recommendations: list[dict[str, Any]],
    ) -> dict[tuple[str, str, tuple[str, ...]], dict[str, Any]]:
        return {
            self._recommendation_signature(recommendation): recommendation
            for recommendation in dmv_recommendations
        }

    def _merge_unique_columns(
        self,
        first: list[str],
        second: list[str],
    ) -> list[str]:
        merged: list[str] = []
        for column in [*first, *second]:
            if column and column not in merged:
                merged.append(column)
        return merged

    def _to_float(self, value: Any) -> float:
        if value is None:
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
