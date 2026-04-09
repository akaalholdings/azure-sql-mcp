from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

import mcp.types as types
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from .artifacts import ErrorPayload
from .artifacts import ExplainPlanArtifact
from .artifacts import json_text
from .auth import AzureSqlAuthenticator
from .capabilities import CapabilityService
from .config import AccessMode
from .config import ServerConfig
from .config import TransportMode
from .config import load_server_config
from .connection import AzureSqlExecutor
from .connection_pool import ConnectionPool
from .health import HealthService
from .index_optimizer import IndexOptimizer
from .index_recommendations import IndexRecommendationService
from .introspection import IntrospectionService
from .lock_diagnostics import LockDiagnosticsService
from .logging_config import configure_logging
from .observability import sanitize_error_message
from .param_binding import ParameterBindingService
from .plan_cache import PlanCacheService
from .plans import PlansService
from .prompts import register_prompts
from .query_index_analysis import QueryIndexAnalysisService
from .query_regression import QueryRegressionService
from .query_store import QueryStoreService
from .resource_governance import ResourceGovernanceService
from .resources import register_resources
from .safe_sql import SafeSqlValidator
from .schema_compare import SchemaCompareService
from .sessions import SessionsService
from .tempdb_memory import TempdbMemoryService
from .wait_stats import WaitStatsService

ResponseType = list[types.TextContent | types.ImageContent | types.EmbeddedResource]

logger = logging.getLogger(__name__)


class AzureSqlMcpApplication:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.mcp = FastMCP("azure-sql-mcp")

        authenticator = AzureSqlAuthenticator(config)
        pool = ConnectionPool(config, authenticator)
        executor = AzureSqlExecutor(config, authenticator, pool)
        validator = SafeSqlValidator()

        self.pool = pool

        self.executor = executor
        self.validator = validator
        self.introspection = IntrospectionService(executor)
        self.query_store = QueryStoreService(executor)
        self.plans = PlansService(executor, validator)
        self.recommendations = IndexRecommendationService(executor)
        self.query_index_analysis = QueryIndexAnalysisService(executor, validator)
        self.health = HealthService(executor, self.query_store)
        self.sessions = SessionsService(executor)
        self.schema_compare = SchemaCompareService(executor)
        self.index_optimizer = IndexOptimizer(executor, validator)
        self.wait_stats = WaitStatsService(executor)
        self.lock_diagnostics = LockDiagnosticsService(executor)
        self.tempdb_memory = TempdbMemoryService(executor)
        self.resource_governance = ResourceGovernanceService(executor)
        self.plan_cache = PlanCacheService(executor)
        self.query_regression = QueryRegressionService(executor)
        self.param_binding = ParameterBindingService(executor)
        self.capabilities = CapabilityService(
            executor,
            self.query_store,
            self.plans,
            self.recommendations,
        )

        self._register_tools()
        self._prune_disabled_tools()
        register_resources(self.mcp, self.config, self.introspection)
        register_prompts(self.mcp, self.config)

    def _prune_disabled_tools(self) -> None:
        """Remove tools that are not in the configured tool_groups."""
        from .config import ToolGroup

        if ToolGroup.ALL in self.config.tool_groups:
            return
        registered = [t.name for t in self.mcp._tool_manager.list_tools()]
        for name in registered:
            if not self.config.is_tool_enabled(name):
                self.mcp.remove_tool(name)
                logger.debug("Pruned tool '%s' (not in active tool groups)", name)

    def _register_tools(self) -> None:
        @self.mcp.tool(
            description="List the configured Azure SQL databases available to this MCP server.",
            annotations=ToolAnnotations(
                title="List Databases",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def list_databases() -> ResponseType:
            return self._format_response(
                {
                    "server": self.config.server,
                    "default_database": self.config.default_database,
                    "allowed_databases": list(self.config.allowed_databases),
                }
            )

        @self.mcp.tool(
            description="Probe Azure SQL capabilities and permission-sensitive features for a database.",
            annotations=ToolAnnotations(
                title="Check Capabilities",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def check_capabilities(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "check_capabilities",
                database_name,
                self.capabilities.check,
            )

        @self.mcp.tool(
            description="List schemas in the selected Azure SQL database.",
            annotations=ToolAnnotations(
                title="List Schemas",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def list_schemas(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "list_schemas",
                database_name,
                self.introspection.list_schemas,
            )

        @self.mcp.tool(
            description="List objects in a schema. Supports table, view, procedure, function, and index.",
            annotations=ToolAnnotations(
                title="List Objects",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def list_objects(
            schema_name: str = Field(description="Schema name."),
            object_type: str = Field(
                default="table",
                description="Object type: table, view, procedure, function, or index.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "list_objects",
                database_name,
                lambda resolved_database: self.introspection.list_objects(
                    resolved_database,
                    schema_name,
                    object_type,
                ),
            )

        @self.mcp.tool(
            description="Search for database objects by name pattern across all schemas.",
            annotations=ToolAnnotations(
                title="Search Objects",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def search_objects(
            pattern: str = Field(
                description="SQL LIKE pattern, for example '%User%' or 'Order%'.",
            ),
            object_type: str | None = Field(
                default=None,
                description="Optional filter: table, view, procedure, or function.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "search_objects",
                database_name,
                lambda resolved_database: self.introspection.search_objects(
                    resolved_database,
                    pattern,
                    object_type,
                ),
            )

        @self.mcp.tool(
            description="Show detailed information about a schema object.",
            annotations=ToolAnnotations(
                title="Get Object Details",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_object_details(
            schema_name: str = Field(description="Schema name."),
            object_name: str = Field(description="Object name."),
            object_type: str = Field(
                default="table",
                description="Object type: table, view, procedure, function, or index.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_object_details",
                database_name,
                lambda resolved_database: self.introspection.get_object_details(
                    resolved_database,
                    schema_name,
                    object_name,
                    object_type,
                ),
            )

        @self.mcp.tool(
            description="Get dependency graph for a database object: what it references and what references it.",
            annotations=ToolAnnotations(
                title="Get Dependencies",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_dependencies(
            schema_name: str = Field(description="Schema name."),
            object_name: str = Field(description="Object name."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_dependencies",
                database_name,
                lambda resolved_database: self.introspection.get_dependencies(
                    resolved_database,
                    schema_name,
                    object_name,
                ),
            )

        @self.mcp.tool(
            description="Get approximate row counts and storage sizes for tables.",
            annotations=ToolAnnotations(
                title="Get Table Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_table_stats(
            schema_name: str | None = Field(
                default=None,
                description="Optional schema filter. Omit for all schemas.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_table_stats",
                database_name,
                lambda resolved_database: self.introspection.get_table_stats(
                    resolved_database,
                    schema_name,
                ),
            )

        @self.mcp.tool(
            description="Capture a point-in-time schema snapshot for a database.",
            annotations=ToolAnnotations(
                title="Capture Schema Snapshot",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def capture_schema_snapshot(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
            schema_filter: str | None = Field(
                default=None,
                description="Comma-separated schema names to capture. Defaults to all user schemas.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "capture_schema_snapshot",
                database_name,
                lambda resolved_database: self.schema_compare.capture_schema_snapshot(
                    resolved_database,
                    schema_filter,
                ),
            )

        @self.mcp.tool(
            description="Compare schemas between two databases and return all differences.",
            annotations=ToolAnnotations(
                title="Compare Schemas",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def compare_schemas(
            source_database: str = Field(description="Source database name."),
            target_database: str = Field(description="Target database name."),
            schema_filter: str | None = Field(
                default=None,
                description="Comma-separated schema names to compare. Defaults to all user schemas.",
            ),
        ) -> ResponseType:
            return await self._run_database_pair_tool(
                "compare_schemas",
                source_database,
                target_database,
                lambda resolved_source, resolved_target: self.schema_compare.compare_schemas(
                    resolved_source,
                    resolved_target,
                    schema_filter,
                ),
            )

        @self.mcp.tool(
            description="Generate a T-SQL migration script to transform source schema to match target.",
            annotations=ToolAnnotations(
                title="Generate Migration Script",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def generate_migration_script(
            source_database: str = Field(description="Source database name."),
            target_database: str = Field(description="Target database name."),
            schema_filter: str | None = Field(
                default=None,
                description="Comma-separated schema names to compare. Defaults to all user schemas.",
            ),
        ) -> ResponseType:
            async def _generate_script_payload(
                resolved_source: str,
                resolved_target: str,
            ) -> dict[str, Any]:
                script = await self.schema_compare.generate_migration_script(
                    resolved_source,
                    resolved_target,
                    schema_filter,
                )
                return {
                    "source_database": resolved_source,
                    "target_database": resolved_target,
                    "schema_filter": schema_filter,
                    "migration_script": script,
                }

            return await self._run_database_pair_tool(
                "generate_migration_script",
                source_database,
                target_database,
                _generate_script_payload,
            )

        @self.mcp.tool(
            description="List active sessions and running queries, including blocking information.",
            annotations=ToolAnnotations(
                title="Get Active Sessions",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_active_sessions(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_active_sessions",
                database_name,
                self.sessions.get_active_sessions,
            )

        @self.mcp.tool(
            description="Execute a read-only SQL query in restricted mode.",
            annotations=ToolAnnotations(
                title="Execute SQL",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def execute_sql(
            sql: str = Field(description="Read-only SQL to execute."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "execute_sql",
                database_name,
                lambda resolved_database: self._execute_safe_sql(resolved_database, sql),
            )

        @self.mcp.tool(
            description="Generate an estimated or actual execution plan for a read-only SQL query.",
            annotations=ToolAnnotations(
                title="Explain Query",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def explain_query(
            sql: str = Field(description="Read-only SQL query to explain."),
            analyze: bool = Field(
                default=False,
                description="When true, executes the query and returns an actual plan.",
            ),
            hypothetical_indexes: list[dict[str, Any]] | None = Field(
                default=None,
                description=(
                    "Reserved for future use. Hypothetical index analysis is disabled "
                    "on this read-only tool for safety."
                ),
            ),
            auto_bind_params: bool = Field(
                default=False,
                description=(
                    "When true, automatically detects @param placeholders and binds "
                    "them using column statistics or type-based fallback values."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "explain_query",
                database_name,
                lambda resolved_database: self._explain_query(
                    resolved_database,
                    sql,
                    analyze,
                    hypothetical_indexes,
                    auto_bind_params,
                ),
            )

        @self.mcp.tool(
            name="get_top_queries",
            description="List top queries from Query Store.",
            annotations=ToolAnnotations(
                title="Get Top Queries",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_top_queries(
            sort_by: str = Field(
                default="total_duration",
                description=(
                    "Sort by total_duration, avg_duration, cpu, executions, "
                    "logical_io, physical_io, memory, or resource_blend."
                ),
            ),
            window_minutes: int = Field(
                default=60,
                description="How far back to look in Query Store, in minutes.",
            ),
            limit: int = Field(default=10, description="Maximum number of rows to return."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_top_queries",
                database_name,
                lambda resolved_database: self._get_top_queries(
                    resolved_database,
                    sort_by,
                    window_minutes,
                    limit,
                ),
            )

        @self.mcp.tool(
            description="Analyze up to 10 SQL queries and recommend optimal indexes based on execution plans.",
            annotations=ToolAnnotations(
                title="Analyze Query Indexes",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def analyze_query_indexes(
            queries: list[str] = Field(
                description="List of SQL SELECT queries to analyze (max 10)."
            ),
            auto_bind_params: bool = Field(
                default=False,
                description=(
                    "When true, automatically binds @param placeholders in queries "
                    "using column statistics before analyzing."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "analyze_query_indexes",
                database_name,
                lambda resolved_database: self._analyze_query_indexes(
                    resolved_database,
                    queries,
                    auto_bind_params,
                ),
            )

        @self.mcp.tool(
            description="Analyze the database workload to identify resource-intensive queries and recommend optimal indexes.",
            annotations=ToolAnnotations(
                title="Analyze Workload Indexes",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def analyze_workload_indexes(
            window_minutes: int = Field(
                default=60,
                description="How far back to look in Query Store, in minutes.",
            ),
            top_n: int = Field(
                default=20,
                description="Number of top queries to analyze.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "analyze_workload_indexes",
                database_name,
                lambda resolved_database: self.recommendations.analyze_workload_indexes(
                    resolved_database,
                    window_minutes,
                    top_n,
                ),
            )

        @self.mcp.tool(
            description="Analyze missing-index and automatic tuning recommendations.",
            annotations=ToolAnnotations(
                title="Analyze Index Recommendations",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def analyze_index_recommendations(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "analyze_index_recommendations",
                database_name,
                self.recommendations.analyze,
            )

        @self.mcp.tool(
            description=(
                "Run the index optimization engine: analyzes workload from Query Store, "
                "generates index candidates from execution plans and DMVs, estimates sizes, "
                "scores using Pareto optimization (read benefit vs size vs write overhead), "
                "and returns budget-constrained ranked recommendations."
            ),
            annotations=ToolAnnotations(
                title="Optimize Indexes",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def optimize_indexes(
            window_minutes: int = Field(
                default=60,
                description="Query Store lookback window in minutes.",
            ),
            top_n: int = Field(
                default=30,
                description="Number of top queries to analyze.",
            ),
            budget_mb: float | None = Field(
                default=None,
                description="Max total index size in MB. None = unlimited.",
            ),
            alpha: float = Field(
                default=1.5,
                description="Size penalty weight (higher = prefer smaller indexes).",
            ),
            beta: float = Field(
                default=0.5,
                description="Write penalty weight (higher = avoid indexes on write-heavy tables).",
            ),
            min_improvement_pct: float = Field(
                default=5.0,
                description="Minimum impact percentage to include a candidate.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "optimize_indexes",
                database_name,
                lambda db: self.index_optimizer.optimize(
                    db, window_minutes, top_n, budget_mb, alpha, beta, min_improvement_pct,
                ),
            )

        # --- Phase 9: Wait Statistics ---

        @self.mcp.tool(
            description=(
                "Get top wait statistics from sys.dm_db_wait_stats with category mapping "
                "(CPU, I/O, Lock, Memory, Network, etc.), benign wait filtering, and root-cause recommendations."
            ),
            annotations=ToolAnnotations(
                title="Get Wait Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_wait_stats(
            top_n: int = Field(default=20, description="Number of top waits to return."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_wait_stats",
                database_name,
                lambda db: self.wait_stats.get_wait_stats(db, top_n),
            )

        @self.mcp.tool(
            description=(
                "Get per-query wait breakdown from Query Store wait stats. "
                "Ties waits to specific queries: 'this query is slow because of X'."
            ),
            annotations=ToolAnnotations(
                title="Get Query Wait Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_query_wait_stats(
            window_minutes: int = Field(
                default=60, description="Query Store lookback window in minutes."
            ),
            top_n: int = Field(default=20, description="Number of top results."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_query_wait_stats",
                database_name,
                lambda db: self.wait_stats.get_query_wait_stats(db, window_minutes, top_n),
            )

        @self.mcp.tool(
            description=(
                "Get currently waiting tasks right now — real-time view of what is blocked "
                "from sys.dm_os_waiting_tasks with SQL text and wait category."
            ),
            annotations=ToolAnnotations(
                title="Get Currently Waiting Tasks",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_currently_waiting_tasks(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_currently_waiting_tasks",
                database_name,
                self.wait_stats.get_currently_waiting_tasks,
            )

        # --- Phase 10: Lock & Transaction Diagnostics ---

        @self.mcp.tool(
            description=(
                "Get current lock details from sys.dm_tran_locks with owning session, "
                "lock mode (S, X, IX, IS, U, Sch-M), resource type, and SQL text."
            ),
            annotations=ToolAnnotations(
                title="Get Lock Details",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_lock_details(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_lock_details",
                database_name,
                self.lock_diagnostics.get_lock_details,
            )

        @self.mcp.tool(
            description=(
                "Get open transactions with duration, type (read/write, read-only), "
                "log bytes used, and warnings for long-running or idle-in-transaction."
            ),
            annotations=ToolAnnotations(
                title="Get Open Transactions",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_open_transactions(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_open_transactions",
                database_name,
                self.lock_diagnostics.get_open_transactions,
            )

        @self.mcp.tool(
            description=(
                "Get recent deadlock history from system_health extended events session. "
                "Parses deadlock XML to show victim, participants, resources, and SQL text."
            ),
            annotations=ToolAnnotations(
                title="Get Deadlock History",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_deadlock_history(
            max_events: int = Field(
                default=10, description="Maximum number of deadlock events to return."
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_deadlock_history",
                database_name,
                lambda db: self.lock_diagnostics.get_deadlock_history(db, max_events),
            )

        # --- Phase 11: Tempdb & Memory Grant Diagnostics ---

        @self.mcp.tool(
            description=(
                "Get per-session tempdb consumption: user/internal object allocations "
                "and net usage in MB."
            ),
            annotations=ToolAnnotations(
                title="Get Tempdb Usage",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_tempdb_usage(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_tempdb_usage",
                database_name,
                self.tempdb_memory.get_tempdb_usage,
            )

        @self.mcp.tool(
            description=(
                "Get tempdb space breakdown: version store, user objects, internal objects, "
                "free space. Useful for diagnosing version store bloat under snapshot isolation."
            ),
            annotations=ToolAnnotations(
                title="Get Tempdb Space Breakdown",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_tempdb_space_breakdown(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_tempdb_space_breakdown",
                database_name,
                self.tempdb_memory.get_tempdb_space_breakdown,
            )

        @self.mcp.tool(
            description=(
                "Get active and pending memory grants. Identifies queries waiting for memory "
                "(RESOURCE_SEMAPHORE) and queries likely spilling to tempdb."
            ),
            annotations=ToolAnnotations(
                title="Get Memory Grants",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_memory_grants(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_memory_grants",
                database_name,
                self.tempdb_memory.get_memory_grants,
            )

        # --- Phase 12: I/O & Azure Resource Governance ---

        @self.mcp.tool(
            description=(
                "Get per-file I/O stats: read/write latency, throughput, stall times. "
                "Warns when average latency exceeds 20ms threshold."
            ),
            annotations=ToolAnnotations(
                title="Get I/O Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_io_stats(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_io_stats",
                database_name,
                self.resource_governance.get_io_stats,
            )

        @self.mcp.tool(
            description=(
                "Get Azure resource governance limits: max CPU%, IOPS, log rate, memory, "
                "workers, and current service tier/objective."
            ),
            annotations=ToolAnnotations(
                title="Get Resource Limits",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_resource_limits(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_resource_limits",
                database_name,
                self.resource_governance.get_resource_limits,
            )

        @self.mcp.tool(
            description=(
                "Get resource utilization history (15-sec granularity) from sys.dm_db_resource_stats. "
                "Shows CPU, data I/O, log write, memory trends with sustained pressure warnings."
            ),
            annotations=ToolAnnotations(
                title="Get Resource Stats History",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_resource_stats_history(
            window_minutes: int = Field(
                default=60, description="How far back to look, in minutes."
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_resource_stats_history",
                database_name,
                lambda db: self.resource_governance.get_resource_stats_history(db, window_minutes),
            )

        # --- Phase 13: Statistics & Plan Cache ---

        @self.mcp.tool(
            description=(
                "Check statistics health: stale stats, high modification counts, "
                "low sample rates. Flags stats needing UPDATE STATISTICS."
            ),
            annotations=ToolAnnotations(
                title="Check Statistics Health",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def check_statistics_health(
            stale_days: int = Field(
                default=7, description="Flag stats not updated in this many days."
            ),
            mod_pct_threshold: float = Field(
                default=20.0,
                description="Flag stats where modification_counter exceeds this % of rows.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "check_statistics_health",
                database_name,
                lambda db: self.plan_cache.check_statistics_health(
                    db, stale_days, mod_pct_threshold
                ),
            )

        @self.mcp.tool(
            description=(
                "Analyze plan cache: plan type distribution, single-use plan bloat, "
                "top plans by size. Detects ad-hoc query pollution."
            ),
            annotations=ToolAnnotations(
                title="Get Plan Cache Analysis",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_plan_cache_analysis(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_plan_cache_analysis",
                database_name,
                self.plan_cache.get_plan_cache_analysis,
            )

        @self.mcp.tool(
            description=(
                "Identify excessively recompiled queries from sys.dm_exec_query_stats. "
                "Flags queries where recompile ratio exceeds 50%."
            ),
            annotations=ToolAnnotations(
                title="Get Query Compilation Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_query_compilation_stats(
            top_n: int = Field(default=20, description="Number of top recompiled queries."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_query_compilation_stats",
                database_name,
                lambda db: self.plan_cache.get_query_compilation_stats(db, top_n),
            )

        # --- Phase 14: Parameter Sniffing & Query Regression ---

        @self.mcp.tool(
            description=(
                "Detect parameter sniffing: queries with multiple plans where "
                "MAX(avg_duration) / MIN(avg_duration) exceeds threshold (default 10x)."
            ),
            annotations=ToolAnnotations(
                title="Detect Parameter Sniffing",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def detect_parameter_sniffing(
            variance_threshold: float = Field(
                default=10.0,
                description="Min ratio of worst/best plan duration to flag (e.g., 10 = 10x worse).",
            ),
            window_minutes: int = Field(
                default=1440,
                description="Query Store lookback window in minutes (default 24 hours).",
            ),
            top_n: int = Field(default=20, description="Max number of results."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "detect_parameter_sniffing",
                database_name,
                lambda db: self.query_regression.detect_parameter_sniffing(
                    db, variance_threshold, window_minutes, top_n
                ),
            )

        @self.mcp.tool(
            description=(
                "Surface automatic tuning regression recommendations from "
                "sys.dm_db_tuning_recommendations with plan forcing scripts."
            ),
            annotations=ToolAnnotations(
                title="Detect Regressed Queries",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def detect_regressed_queries(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "detect_regressed_queries",
                database_name,
                self.query_regression.detect_regressed_queries,
            )

        @self.mcp.tool(
            description=(
                "Compare two query plans side-by-side: operators, duration, CPU, I/O. "
                "If plan IDs not specified, compares best vs worst plan by duration."
            ),
            annotations=ToolAnnotations(
                title="Compare Query Plans",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def compare_query_plans(
            query_id: int = Field(description="Query Store query_id."),
            plan_id_a: int | None = Field(
                default=None, description="First plan_id to compare (optional, uses best by default)."
            ),
            plan_id_b: int | None = Field(
                default=None, description="Second plan_id to compare (optional, uses worst by default)."
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "compare_query_plans",
                database_name,
                lambda db: self.query_regression.compare_query_plans(
                    db, query_id, plan_id_a, plan_id_b
                ),
            )

        @self.mcp.tool(
            description=(
                "List all forced plans with execution stats and staleness warnings. "
                "Identifies forced plans that haven't executed recently or have force failures."
            ),
            annotations=ToolAnnotations(
                title="Get Forced Plans",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_forced_plans(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_forced_plans",
                database_name,
                self.query_regression.get_forced_plans,
            )

        @self.mcp.tool(
            description=(
                "Analyze Azure SQL database health. Supports index, buffer, connection, "
                "constraint, replication, identity, query_store, tuning, resource, storage, "
                "and all."
            ),
            annotations=ToolAnnotations(
                title="Analyze DB Health",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def analyze_db_health(
            health_type: str = Field(
                default="all",
                description=(
                    "Health type: index, buffer, connection, constraint, replication, "
                    "identity, query_store, tuning, resource, storage, or all."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "analyze_db_health",
                database_name,
                lambda resolved_database: self.health.analyze(
                    resolved_database,
                    health_type,
                ),
            )

        if self.config.access_mode == AccessMode.UNRESTRICTED:
            @self.mcp.tool(
                description="Execute unrestricted T-SQL. This can be destructive.",
                annotations=ToolAnnotations(
                    title="Execute Unrestricted T-SQL",
                    readOnlyHint=False,
                    destructiveHint=True,
                    idempotentHint=False,
                    openWorldHint=True,
                ),
            )
            async def execute_tsql_unrestricted(
                sql: str = Field(description="T-SQL to execute."),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "execute_tsql_unrestricted",
                    database_name,
                    lambda resolved_database: self._execute_unrestricted_sql(
                        resolved_database,
                        sql,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Rebuild or reorganize a table index. "
                    "REBUILD recreates the index (fixes fragmentation, updates stats). "
                    "REORGANIZE is lighter-weight (online, defragments leaf level only). "
                    "Use ONLINE=True (default) to avoid blocking queries during rebuild."
                ),
                annotations=ToolAnnotations(
                    title="Rebuild Index",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def rebuild_index(
                schema_name: str = Field(description="Schema name (e.g. 'dbo')."),
                table_name: str = Field(description="Table name."),
                index_name: str = Field(description="Index name to rebuild/reorganize."),
                operation: str = Field(
                    default="REBUILD",
                    description="REBUILD or REORGANIZE.",
                ),
                online: bool = Field(
                    default=True,
                    description="Use ONLINE=ON for rebuild (avoids blocking). Ignored for REORGANIZE.",
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "rebuild_index",
                    database_name,
                    lambda db: self._rebuild_index(
                        db, schema_name, table_name, index_name, operation, online,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Update statistics for a table or specific statistic. "
                    "Refreshes query optimizer cost estimates. Use after large data changes "
                    "or when stale statistics are detected by health checks."
                ),
                annotations=ToolAnnotations(
                    title="Update Statistics",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def update_statistics(
                schema_name: str = Field(description="Schema name (e.g. 'dbo')."),
                table_name: str = Field(description="Table name."),
                stat_name: str | None = Field(
                    default=None,
                    description="Optional statistic name. If omitted, updates all statistics on the table.",
                ),
                sample_percent: int | None = Field(
                    default=None,
                    description="Sample percentage (1-100). If omitted, SQL Server chooses automatically.",
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "update_statistics",
                    database_name,
                    lambda db: self._update_statistics(
                        db, schema_name, table_name, stat_name, sample_percent,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Force or unforce a query plan in Query Store. "
                    "Forcing pins a specific plan to a query, preventing regressions. "
                    "Unforcing releases the pin so the optimizer can choose freely."
                ),
                annotations=ToolAnnotations(
                    title="Force/Unforce Query Plan",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def force_query_plan(
                query_id: int = Field(description="Query Store query_id."),
                plan_id: int = Field(description="Query Store plan_id."),
                unforce: bool = Field(
                    default=False,
                    description="Set True to unforce (release) the plan instead of forcing it.",
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "force_query_plan",
                    database_name,
                    lambda db: self._force_query_plan(
                        db, query_id, plan_id, unforce,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Terminate a user session (SPID). Use to resolve blocking chains "
                    "or kill runaway queries. Cannot kill system sessions."
                ),
                annotations=ToolAnnotations(
                    title="Kill Session",
                    readOnlyHint=False,
                    destructiveHint=True,
                    idempotentHint=False,
                    openWorldHint=False,
                ),
            )
            async def kill_session(
                session_id: int = Field(description="Session ID (SPID) to terminate."),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "kill_session",
                    database_name,
                    lambda db: self._kill_session(db, session_id),
                )

    async def _run_tool(
        self,
        tool_name: str,
        database_name: str | None,
        callback,
    ) -> ResponseType:
        requested_database = (database_name or self.config.default_database).strip()
        correlation_id = str(uuid.uuid4())
        started_at = time.monotonic()
        try:
            resolved_database = self.config.validate_database_name(database_name)
            logger.info(
                "Running tool",
                extra={
                    "tool_name": tool_name,
                    "database_name": resolved_database,
                    "correlation_id": correlation_id,
                },
            )
            payload = await asyncio.wait_for(
                callback(resolved_database),
                timeout=self.config.tool_timeout_seconds,
            )
            duration_ms = self._duration_ms(started_at)
            row_count = self._estimate_row_count(payload)
            log_extra: dict[str, Any] = {
                "tool_name": tool_name,
                "database_name": resolved_database,
                "correlation_id": correlation_id,
                "duration_ms": duration_ms,
                "row_count": row_count,
            }
            if row_count > 10_000:
                logger.warning(
                    "Tool returned large result set (%d rows)",
                    row_count,
                    extra=log_extra,
                )
            else:
                logger.info("Tool completed", extra=log_extra)
            return self._format_response(payload)
        except asyncio.TimeoutError:
            duration_ms = self._duration_ms(started_at)
            logger.warning(
                "Tool timed out",
                extra={
                    "tool_name": tool_name,
                    "database_name": requested_database,
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                },
            )
            return self._format_error(
                "timeout",
                f"Tool '{tool_name}' timed out after {self.config.tool_timeout_seconds}s.",
            )
        except Exception as exc:
            duration_ms = self._duration_ms(started_at)
            sanitized_error = sanitize_error_message(str(exc))
            logger.error(
                "Tool failed",
                extra={
                    "tool_name": tool_name,
                    "database_name": requested_database,
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                    "error_type": type(exc).__name__,
                    "error": sanitized_error,
                },
            )
            return self._format_error("tool_error", sanitized_error)

    async def _run_database_pair_tool(
        self,
        tool_name: str,
        source_database: str,
        target_database: str,
        callback,
    ) -> ResponseType:
        requested_label = f"{source_database.strip()} -> {target_database.strip()}"
        correlation_id = str(uuid.uuid4())
        started_at = time.monotonic()
        try:
            resolved_source = self.config.validate_database_name(source_database)
            resolved_target = self.config.validate_database_name(target_database)
            database_label = f"{resolved_source} -> {resolved_target}"
            logger.info(
                "Running tool",
                extra={
                    "tool_name": tool_name,
                    "database_name": database_label,
                    "correlation_id": correlation_id,
                },
            )
            payload = await asyncio.wait_for(
                callback(resolved_source, resolved_target),
                timeout=self.config.tool_timeout_seconds,
            )
            duration_ms = self._duration_ms(started_at)
            logger.info(
                "Tool completed",
                extra={
                    "tool_name": tool_name,
                    "database_name": database_label,
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                },
            )
            return self._format_response(payload)
        except asyncio.TimeoutError:
            duration_ms = self._duration_ms(started_at)
            logger.warning(
                "Tool timed out",
                extra={
                    "tool_name": tool_name,
                    "database_name": requested_label,
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                },
            )
            return self._format_error(
                "timeout",
                f"Tool '{tool_name}' timed out after {self.config.tool_timeout_seconds}s.",
            )
        except Exception as exc:
            duration_ms = self._duration_ms(started_at)
            sanitized_error = sanitize_error_message(str(exc))
            logger.error(
                "Tool failed",
                extra={
                    "tool_name": tool_name,
                    "database_name": requested_label,
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                    "error_type": type(exc).__name__,
                    "error": sanitized_error,
                },
            )
            return self._format_error("tool_error", sanitized_error)

    async def _execute_safe_sql(self, database_name: str, sql: str) -> dict[str, Any]:
        validated = self.validator.validate_read_only(sql)
        # Fetch at most row_limit + 1 rows to detect truncation without loading entire result
        fetch_limit = self.config.row_limit + 1
        rows = await self.executor.fetch_all(
            database_name, validated.normalized_sql, max_rows=fetch_limit,
        )
        return self._truncate_rows(
            {
                "database_name": database_name,
                "normalized_sql": validated.normalized_sql,
                "rows": rows,
            }
        )

    async def _execute_unrestricted_sql(
        self,
        database_name: str,
        sql: str,
    ) -> dict[str, Any]:
        # Fetch at most row_limit + 1 rows per result set to prevent OOM
        fetch_limit = self.config.row_limit + 1
        results = await self.executor.execute_batches(
            database_name, sql, max_rows=fetch_limit,
        )
        payload = []
        for result in results:
            payload.append(self._truncate_rows({"rows": result.rows}))
        return {"database_name": database_name, "result_sets": payload}

    async def _rebuild_index(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        index_name: str,
        operation: str,
        online: bool,
    ) -> dict[str, Any]:
        op = operation.strip().upper()
        if op not in ("REBUILD", "REORGANIZE"):
            raise ValueError("operation must be REBUILD or REORGANIZE")
        quoted_schema = self._quote_identifier(schema_name)
        quoted_table = self._quote_identifier(table_name)
        quoted_index = self._quote_identifier(index_name)
        qualified_index = f"{quoted_schema}.{quoted_table}.{quoted_index}"
        if op == "REBUILD":
            online_clause = " WITH (ONLINE = ON)" if online else ""
            sql = f"ALTER INDEX {quoted_index} ON {quoted_schema}.{quoted_table} REBUILD{online_clause}"
        else:
            sql = f"ALTER INDEX {quoted_index} ON {quoted_schema}.{quoted_table} REORGANIZE"
        await self.executor.execute_non_query(database_name, sql)
        return {
            "database_name": database_name,
            "operation": op,
            "index": qualified_index,
            "online": online if op == "REBUILD" else None,
            "status": "completed",
        }

    async def _update_statistics(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        stat_name: str | None,
        sample_percent: int | None,
    ) -> dict[str, Any]:
        quoted_schema = self._quote_identifier(schema_name)
        quoted_table_name = self._quote_identifier(table_name)
        qualified_table = f"{quoted_schema}.{quoted_table_name}"
        if stat_name:
            target = f"{qualified_table} {self._quote_identifier(stat_name)}"
        else:
            target = qualified_table
        sql = f"UPDATE STATISTICS {target}"
        if sample_percent is not None:
            if not 1 <= sample_percent <= 100:
                raise ValueError("sample_percent must be between 1 and 100")
            sql += f" WITH SAMPLE {sample_percent} PERCENT"
        await self.executor.execute_non_query(database_name, sql)
        return {
            "database_name": database_name,
            "table": qualified_table,
            "statistic": stat_name or "(all)",
            "sample_percent": sample_percent,
            "status": "completed",
        }

    async def _force_query_plan(
        self,
        database_name: str,
        query_id: int,
        plan_id: int,
        unforce: bool,
    ) -> dict[str, Any]:
        if unforce:
            sql = "EXEC sp_query_store_unforce_plan @query_id = ?, @plan_id = ?"
        else:
            sql = "EXEC sp_query_store_force_plan @query_id = ?, @plan_id = ?"
        await self.executor.execute_non_query(database_name, sql, params=[query_id, plan_id])
        return {
            "database_name": database_name,
            "query_id": query_id,
            "plan_id": plan_id,
            "action": "unforced" if unforce else "forced",
            "status": "completed",
        }

    async def _kill_session(
        self,
        database_name: str,
        session_id: int,
    ) -> dict[str, Any]:
        if session_id <= 50:
            raise ValueError(
                f"Cannot kill session {session_id}: system sessions (SPID <= 50) are protected."
            )
        # KILL cannot be parameterized — use string formatting with validated int
        sql = f"KILL {int(session_id)}"
        await self.executor.execute_non_query(database_name, sql)
        return {
            "database_name": database_name,
            "session_id": session_id,
            "status": "kill_issued",
            "note": "Session termination initiated. Rollback of active transactions may take time.",
        }

    async def _explain_query(
        self,
        database_name: str,
        sql: str,
        analyze: bool,
        hypothetical_indexes: list[dict[str, Any]] | None = None,
        auto_bind_params: bool = False,
    ) -> dict[str, Any]:
        if hypothetical_indexes:
            raise ValueError(
                "Hypothetical index analysis is disabled on explain_query for safety. "
                "Use analyze_query_indexes/analyze_workload_indexes for read-only index insights."
            )
        effective_sql = sql
        binding_info: dict[str, Any] | None = None
        if auto_bind_params:
            binding_info = await self.param_binding.bind_parameters(
                database_name, sql,
            )
            if binding_info.get("parameters"):
                effective_sql = binding_info["bound_sql"]

        artifact = await self.plans.explain_query(
            database_name,
            effective_sql,
            analyze,
            hypothetical_indexes=hypothetical_indexes,
        )
        result = self._artifact_to_dict(artifact)
        if binding_info and binding_info.get("parameters"):
            result["parameter_binding"] = {
                "original_sql": binding_info["original_sql"],
                "parameters": binding_info["parameters"],
            }
        return result

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        value = identifier.strip()
        if not value:
            raise ValueError("Identifier cannot be empty.")
        return f"[{value.replace(']', ']]')}]"

    async def _analyze_query_indexes(
        self,
        database_name: str,
        queries: list[str],
        auto_bind_params: bool = False,
    ) -> dict[str, Any]:
        effective_queries = queries
        if auto_bind_params:
            bound_queries: list[str] = []
            for query in queries:
                binding = await self.param_binding.bind_parameters(database_name, query)
                if binding.get("parameters"):
                    bound_queries.append(binding["bound_sql"])
                else:
                    bound_queries.append(query)
            effective_queries = bound_queries
        return await self.query_index_analysis.analyze_queries(
            database_name, effective_queries,
        )

    async def _get_top_queries(
        self,
        database_name: str,
        sort_by: str,
        window_minutes: int,
        limit: int,
    ) -> dict[str, Any]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0.")
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")

        status = await self.query_store.get_status(database_name)
        rows = await self.query_store.get_top_queries(
            database_name,
            sort_by,
            window_minutes,
            limit,
        )
        return self._truncate_rows(
            {
                "database_name": database_name,
                "query_store_status": status,
                "sort_by": sort_by,
                "window_minutes": window_minutes,
                "rows": rows,
            }
        )

    def _artifact_to_dict(self, artifact: ExplainPlanArtifact) -> dict[str, Any]:
        return artifact.as_dict()

    def _truncate_rows(self, payload: dict[str, Any]) -> dict[str, Any]:
        rows = payload.get("rows")
        if isinstance(rows, list):
            payload["row_count"] = len(rows)
            payload["truncated"] = len(rows) > self.config.row_limit
            payload["rows"] = rows[: self.config.row_limit]
        return payload

    def _format_response(self, payload: Any) -> ResponseType:
        return [types.TextContent(type="text", text=json_text(payload))]

    def _format_error(self, code: str, message: str) -> ResponseType:
        return self._format_response(ErrorPayload(code=code, message=message).as_dict())

    @staticmethod
    def _duration_ms(started_at: float) -> float:
        return round((time.monotonic() - started_at) * 1000, 2)

    @staticmethod
    def _estimate_row_count(payload: Any) -> int:
        if isinstance(payload, dict):
            for value in payload.values():
                if isinstance(value, list):
                    return len(value)
        if isinstance(payload, list):
            return len(payload)
        return 0

    async def run(self) -> None:
        self.mcp.settings.host = self.config.transport.host
        self.mcp.settings.port = self.config.transport.port

        try:
            if self.config.transport.mode == TransportMode.STDIO:
                await self.mcp.run_stdio_async()
            elif self.config.transport.mode == TransportMode.SSE:
                await self.mcp.run_sse_async()
            else:
                await self.mcp.run_streamable_http_async()
        finally:
            try:
                await self.pool.close_all()
            except Exception as exc:
                logger.error(
                    "Failed to close connection pool during shutdown.",
                    extra={
                        "error_type": type(exc).__name__,
                        "error": sanitize_error_message(str(exc)),
                    },
                )


async def async_main(argv: list[str] | None = None) -> None:
    config = load_server_config(argv)
    configure_logging(config.log_level, config.log_format)
    app = AzureSqlMcpApplication(config)
    await app.run()


def main(argv: list[str] | None = None) -> None:
    asyncio.run(async_main(argv))


if __name__ == "__main__":
    main()
