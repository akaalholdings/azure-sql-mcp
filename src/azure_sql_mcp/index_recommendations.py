from __future__ import annotations

import json
import re
from typing import Any

from .connection import AzureSqlExecutor
from .safe_sql import SafeSqlValidator

NON_IDENTIFIER = re.compile(r"[^A-Za-z0-9_]+")


def make_index_name(table_name: str, key_columns: str) -> str:
    suffix = NON_IDENTIFIER.sub("_", key_columns).strip("_")
    return f"IX_{table_name}_{suffix}"[:128]


def split_index_columns(raw_columns: str | None) -> list[str]:
    if not raw_columns:
        return []

    columns: list[str] = []
    for chunk in raw_columns.split(","):
        column = chunk.strip().strip("[]")
        if column and column not in columns:
            columns.append(column)
    return columns


def build_create_index_statement(
    schema_name: str,
    table_name: str,
    key_columns: list[str],
    include_columns: list[str] | None = None,
) -> str:
    if not key_columns:
        return "-- No key columns were available for this recommendation."

    index_name = make_index_name(table_name, "_".join(key_columns))
    include_clause = ""
    if include_columns:
        include_clause = (
            " INCLUDE ("
            + ", ".join(f"[{column}]" for column in include_columns)
            + ")"
        )
    return (
        f"CREATE INDEX [{index_name}] ON [{schema_name}].[{table_name}] "
        + "("
        + ", ".join(f"[{column}]" for column in key_columns)
        + ")"
        + include_clause
        + ";"
    )


class IndexRecommendationService:
    def __init__(
        self,
        executor: AzureSqlExecutor,
        validator: SafeSqlValidator | None = None,
    ):
        self.executor = executor
        self.validator = validator or SafeSqlValidator()

    async def analyze(self, database_name: str) -> dict[str, Any]:
        missing_indexes = await self._get_missing_indexes(database_name)
        tuning_recommendations = await self._get_tuning_recommendations(database_name)
        return {
            "missing_indexes": missing_indexes,
            "automatic_tuning_recommendations": tuning_recommendations,
        }

    async def analyze_workload_indexes(
        self,
        database_name: str,
        window_minutes: int = 60,
        top_n: int = 20,
    ) -> dict[str, Any]:
        from .query_index_analysis import QueryIndexAnalysisService

        service = QueryIndexAnalysisService(self.executor, self.validator)
        return await service.analyze_workload(
            database_name=database_name,
            window_minutes=window_minutes,
            top_n=top_n,
        )

    async def _get_missing_indexes(self, database_name: str) -> list[dict[str, Any]]:
        query = """
        SELECT
            OBJECT_SCHEMA_NAME(mid.object_id) AS schema_name,
            OBJECT_NAME(mid.object_id) AS table_name,
            mid.equality_columns,
            mid.inequality_columns,
            mid.included_columns,
            migs.user_seeks,
            migs.user_scans,
            migs.avg_total_user_cost,
            migs.avg_user_impact,
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
            row["suggested_create_index"] = self._build_create_index_statement(row)
        return rows

    async def _get_tuning_recommendations(self, database_name: str) -> list[dict[str, Any]]:
        query = """
        SELECT
            reason,
            score,
            state,
            details
        FROM sys.dm_db_tuning_recommendations
        """
        rows = await self.executor.fetch_all(database_name, query)
        recommendations = []
        for row in rows:
            details = row.get("details")
            parsed_details: Any = details
            if isinstance(details, str):
                try:
                    parsed_details = json.loads(details)
                except json.JSONDecodeError:
                    parsed_details = details
            recommendations.append(
                {
                    "reason": row.get("reason"),
                    "score": row.get("score"),
                    "state": row.get("state"),
                    "details": parsed_details,
                }
            )
        return recommendations

    def _build_create_index_statement(self, row: dict[str, Any]) -> str:
        schema_name = row.get("schema_name") or "dbo"
        table_name = row.get("table_name") or "unknown_table"
        equality_columns = split_index_columns(row.get("equality_columns"))
        inequality_columns = split_index_columns(row.get("inequality_columns"))
        include_columns = split_index_columns(row.get("included_columns"))

        key_columns = equality_columns + inequality_columns
        return build_create_index_statement(
            schema_name=schema_name,
            table_name=table_name,
            key_columns=key_columns,
            include_columns=include_columns,
        )
