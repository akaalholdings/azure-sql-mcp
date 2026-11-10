from __future__ import annotations

import math
from collections.abc import Callable
from typing import Any

import mcp.types as types
from mcp.server.fastmcp import FastMCP

from .config import ServerConfig

# The tools each prompt tells the agent to call. A prompt is registered only
# when the active profile exposes all of them, so no step names a missing tool.
PROMPT_TOOLS: dict[str, tuple[str, ...]] = {
    "analyze-slow-queries": (
        "get_top_queries",
        "analyze_query_plan",
        "get_query_store_trend",
        "review_workload_indexes",
    ),
    "review-index-health": ("review_workload_indexes",),
    "explore-schema": (
        "list_schemas",
        "get_table_stats",
        "get_object_details",
        "get_dependencies",
        "search_objects",
    ),
    "compare-schemas": ("compare_schemas", "generate_migration_script"),
    "troubleshoot-performance": (
        "diagnose_database",
        "get_top_queries",
        "analyze_query_plan",
        "review_workload_indexes",
        "get_active_sessions",
    ),
}


def register_prompts(mcp: FastMCP, config: ServerConfig) -> None:
    """Register the MCP prompt templates the active profile can run."""

    def prompt(name: str, description: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            if all(config.is_tool_enabled(tool) for tool in PROMPT_TOOLS[name]):
                mcp.prompt(name, description=description)(func)
            return func

        return decorator

    @prompt(
        "analyze-slow-queries",
        description="Investigate slow-running queries using Query Store and execution plans.",
    )
    def analyze_slow_queries(
        database_name: str,
        window_minutes: int = 60,
    ) -> list[types.PromptMessage]:
        """Guide analysis of slow queries using Query Store and execution plans."""
        config.validate_database_name(database_name)
        lookback_days = max(1, math.ceil(window_minutes / 1440))
        return [
            types.PromptMessage(
                role="user",
                content=types.TextContent(
                    type="text",
                    text=(
                        f"Analyze slow queries in the '{database_name}' database "
                        f"over the last {window_minutes} minutes.\n\n"
                        "Steps:\n"
                        f"1. Call get_top_queries with database_name='{database_name}', "
                        f"sort_by='avg_duration', window_minutes={window_minutes}, "
                        "and limit=10. Read result_status first: precondition means "
                        "Query Store is off or read-only. Keep each query_id.\n"
                        f"2. For the top 3 queries, call analyze_query_plan with "
                        f"database_name='{database_name}' and the query_id. It reads the "
                        "stored plan with its measured runtime; do not send the query "
                        "text again. A stored plan is estimated: it shows shape and "
                        "estimates. Add last_actual=true for the actual row counts of "
                        "the last run.\n"
                        f"3. Call get_query_store_trend with database_name='{database_name}' "
                        "and the query_id to see when the query changed and whether a "
                        "new plan appeared.\n"
                        f"4. If a finding points to a missing or weak index, call "
                        f"review_workload_indexes with database_name='{database_name}', "
                        f"lookback_days={lookback_days}, objective='duration', and "
                        "query_ids set to those query ids. A missing-index hint is a "
                        "lead, not a design.\n"
                        "5. Summarize, for each query: the slow operator or mechanism, "
                        "the execution count, and one fix with its risk and rollback. "
                        "Say what could not be read."
                    ),
                ),
            )
        ]

    @prompt(
        "review-index-health",
        description=(
            "Review indexes from the workload: missing, overlapping, and unused "
            "indexes with inert DDL and rollback."
        ),
    )
    def review_index_health(
        database_name: str,
    ) -> list[types.PromptMessage]:
        """Guide a workload-driven review of indexes."""
        config.validate_database_name(database_name)
        return [
            types.PromptMessage(
                role="user",
                content=types.TextContent(
                    type="text",
                    text=(
                        f"Review the indexes of the '{database_name}' database from its "
                        "workload.\n\n"
                        "Steps:\n"
                        f"1. Call review_workload_indexes with database_name='{database_name}' "
                        "and lookback_days=7. When an index removal is in question use "
                        "lookback_days=40: a drop is rated high only with 35 days of "
                        "Query Store history.\n"
                        "2. Read result_status, then gaps. The gaps state how much of the "
                        "workload was analysed. A precondition result holds a remediation "
                        "statement for a DBA: show it, do not run it.\n"
                        "3. For each table, highest workload share first, report how "
                        "queries reach it and each recommendation: action, keys, includes, "
                        "supporting query ids, confidence, blockers, write impact, DDL "
                        "and rollback DDL.\n"
                        "4. Report rewrite opportunities separately: no index fixes a "
                        "non-sargable predicate or an implicit conversion.\n"
                        "5. Produce a prioritized plan: create, extend, widen, consolidate, "
                        "or drop. The DDL is inert advice for change control. Label the "
                        "estimated benefit as an estimate, and say that a create, extend "
                        "or widen must be measured on a non-production copy first."
                    ),
                ),
            )
        ]

    @prompt(
        "explore-schema",
        description="Explore a database schema: tables, relationships, and key objects.",
    )
    def explore_schema(
        database_name: str,
        schema_name: str = "dbo",
    ) -> list[types.PromptMessage]:
        """Guide interactive exploration of a database schema."""
        config.validate_database_name(database_name)
        return [
            types.PromptMessage(
                role="user",
                content=types.TextContent(
                    type="text",
                    text=(
                        f"Explore the '{schema_name}' schema in the '{database_name}' "
                        "database.\n\n"
                        "Steps:\n"
                        f"1. Call list_schemas with database_name='{database_name}' "
                        "to confirm the available schemas.\n"
                        f"2. Call get_table_stats with database_name='{database_name}' and "
                        f"schema_name='{schema_name}' to list the tables with their row "
                        "counts and sizes.\n"
                        f"3. For the largest and the most relevant tables, call "
                        f"get_object_details with database_name='{database_name}', "
                        f"schema_name='{schema_name}', the table name and "
                        "object_type='table' to inspect columns, constraints, and "
                        "indexes.\n"
                        f"4. Call get_dependencies for key tables or procedures to "
                        "understand relationships.\n"
                        f"5. Call search_objects with database_name='{database_name}', "
                        "pattern='%' and object_type='view', then 'procedure' and "
                        f"'function'. Keep the rows whose schema is '{schema_name}', then "
                        "summarize the schema layout and design patterns."
                    ),
                ),
            )
        ]

    @prompt(
        "compare-schemas",
        description="Compare schemas between two databases to find differences.",
    )
    def compare_schemas(
        source_database: str,
        target_database: str,
    ) -> list[types.PromptMessage]:
        """Guide a schema comparison between two databases."""
        config.validate_database_name(source_database)
        config.validate_database_name(target_database)
        return [
            types.PromptMessage(
                role="user",
                content=types.TextContent(
                    type="text",
                    text=(
                        f"Compare schemas between '{source_database}' (source) and "
                        f"'{target_database}' (target).\n\n"
                        "Steps:\n"
                        f"1. Call compare_schemas with source_database='{source_database}' "
                        f"and target_database='{target_database}' to get the structured diff.\n"
                        f"2. Review the grouped differences and summarize added, removed, "
                        "and modified objects by category.\n"
                        f"3. Call generate_migration_script with source_database='{source_database}' "
                        f"and target_database='{target_database}' to draft the migration SQL.\n"
                        "4. Inspect the script for destructive changes, dependency ordering, "
                        "and rollout risks.\n"
                        "5. Call out breaking changes and recommended next steps before execution."
                    ),
                ),
            )
        ]

    @prompt(
        "troubleshoot-performance",
        description="Performance troubleshooting: ranked findings, top queries, plans, indexes, and blocking.",
    )
    def troubleshoot_performance(
        database_name: str,
    ) -> list[types.PromptMessage]:
        """Guide performance troubleshooting from one ranked diagnosis."""
        config.validate_database_name(database_name)
        return [
            types.PromptMessage(
                role="user",
                content=types.TextContent(
                    type="text",
                    text=(
                        f"Troubleshoot performance issues in the '{database_name}' "
                        "database.\n\n"
                        "Steps:\n"
                        f"1. Call diagnose_database with database_name='{database_name}'. "
                        "During a live incident add sample_seconds=15. Read the verdict, "
                        "the findings (ordered by severity) and the gaps: a partial "
                        "verdict is not an all-clear. Follow the next tool calls each "
                        "finding names where the active profile has them.\n"
                        f"2. Call get_top_queries with database_name='{database_name}', "
                        "sort_by='resource_blend', window_minutes=60, and limit=10 "
                        "to identify resource-heavy recent queries. Keep each query_id.\n"
                        f"3. For the top 2-3 queries, call analyze_query_plan with "
                        f"database_name='{database_name}' and the query_id to read the "
                        "stored plan and its findings.\n"
                        f"4. Call review_workload_indexes with "
                        f"database_name='{database_name}', lookback_days=1, "
                        "top_queries=20, and query_ids set to those query ids for "
                        "workload-driven index recommendations.\n"
                        f"5. Call get_active_sessions with database_name='{database_name}' "
                        "and read its blocking chains. Do not ask it for plans during an "
                        "incident.\n"
                        "6. Summarize critical issues, quick wins, and longer-term "
                        "remediation steps, each with its evidence, risk and rollback."
                    ),
                ),
            )
        ]
