from __future__ import annotations

import mcp.types as types
from mcp.server.fastmcp import FastMCP

from .config import ServerConfig


def register_prompts(mcp: FastMCP, config: ServerConfig) -> None:
    """Register MCP prompt templates for guided workflows."""

    @mcp.prompt(
        "analyze-slow-queries",
        description="Investigate slow-running queries using Query Store and execution plans.",
    )
    def analyze_slow_queries(
        database_name: str,
        window_minutes: int = 60,
    ) -> list[types.PromptMessage]:
        """Guide analysis of slow queries using Query Store and execution plans."""
        config.validate_database_name(database_name)
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
                        "and limit=10 to find the slowest queries.\n"
                        f"2. For the top 3 queries, call explain_query with "
                        f"database_name='{database_name}' and the query text to "
                        "inspect execution plans.\n"
                        f"3. Call analyze_index_recommendations with "
                        f"database_name='{database_name}' if plan warnings suggest "
                        "missing or inefficient indexes.\n"
                        "4. Summarize the slowest queries, execution counts, plan "
                        "warnings, and concrete remediation steps."
                    ),
                ),
            )
        ]

    @mcp.prompt(
        "review-index-health",
        description="Review index health: fragmentation, unused indexes, duplicates, and missing indexes.",
    )
    def review_index_health(
        database_name: str,
    ) -> list[types.PromptMessage]:
        """Guide a review of index health and recommendations."""
        config.validate_database_name(database_name)
        return [
            types.PromptMessage(
                role="user",
                content=types.TextContent(
                    type="text",
                    text=(
                        f"Perform an index health review for the '{database_name}' database.\n\n"
                        "Steps:\n"
                        f"1. Call analyze_db_health with database_name='{database_name}' "
                        "and health_type='index' to check fragmentation, duplicate "
                        "indexes, and unused indexes.\n"
                        f"2. Call analyze_index_recommendations with "
                        f"database_name='{database_name}' to collect missing-index "
                        "and tuning recommendations.\n"
                        f"3. Use get_table_stats with database_name='{database_name}' "
                        "if you need to prioritize large tables first.\n"
                        "4. Produce a prioritized action plan: create, rebuild, "
                        "reorganize, or drop."
                    ),
                ),
            )
        ]

    @mcp.prompt(
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
                        f"2. Call list_objects with database_name='{database_name}', "
                        f"schema_name='{schema_name}', object_type='table' to list "
                        "tables.\n"
                        f"3. For important tables, call get_object_details with "
                        f"database_name='{database_name}', schema_name='{schema_name}', "
                        "and the table name to inspect columns, constraints, and "
                        "indexes.\n"
                        f"4. Call get_dependencies for key tables or procedures to "
                        "understand relationships.\n"
                        f"5. Repeat list_objects for views and procedures in the same "
                        "schema, then summarize the schema layout and design patterns."
                    ),
                ),
            )
        ]

    @mcp.prompt(
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

    @mcp.prompt(
        "troubleshoot-performance",
        description="Comprehensive performance troubleshooting: health, queries, indexes, and resources.",
    )
    def troubleshoot_performance(
        database_name: str,
    ) -> list[types.PromptMessage]:
        """Guide comprehensive performance troubleshooting."""
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
                        f"1. Call analyze_db_health with database_name='{database_name}' "
                        "and health_type='all' for a broad health snapshot.\n"
                        f"2. Call get_top_queries with database_name='{database_name}', "
                        "sort_by='resource_blend', window_minutes=60, and limit=10 "
                        "to identify resource-heavy recent queries.\n"
                        f"3. For the top 2-3 queries, call explain_query with "
                        f"database_name='{database_name}' to inspect execution plans.\n"
                        f"4. Call analyze_workload_indexes with "
                        f"database_name='{database_name}', window_minutes=60, and "
                        "top_n=20 for workload-driven index recommendations.\n"
                        f"5. Call get_active_sessions with database_name='{database_name}' "
                        "to check for blocking or long-running work.\n"
                        "6. Summarize critical issues, quick wins, and longer-term "
                        "remediation steps."
                    ),
                ),
            )
        ]
