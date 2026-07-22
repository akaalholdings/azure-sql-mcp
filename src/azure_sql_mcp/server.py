from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
import uuid
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import AnyHttpUrl
from pydantic import Field

from .admin_policy import AdminAction
from .admin_policy import AdminPolicy
from .artifact_store import ArtifactStore
from .artifacts import ErrorPayload
from .artifacts import ExplainPlanArtifact
from .auth import AzureSqlAuthenticator
from .capabilities import CapabilityService
from .config import AccessMode
from .config import McpProfile
from .config import ServerConfig
from .config import TransportMode
from .config import load_server_config
from .connection import AzureSqlExecutor
from .connection_pool import ConnectionPool
from .database_policy import load_database_policy_or_deny
from .diagnostics import DiagnosticQueryService
from .health import HealthService
from .index_optimizer import IndexOptimizer
from .index_recommendations import IndexRecommendationService
from .introspection import IntrospectionService
from .lock_diagnostics import LockDiagnosticsService
from .logging_config import configure_logging
from .observability import sanitize_error_message
from .param_binding import detect_parameters
from .param_binding import ParameterBindingService
from .performance_contracts import EvidenceEnvelopeV1
from .performance_store import PerformanceStore
from .performance_workflows import PerformanceWorkflowService
from .performance_workflows import aggregate_samples
from .performance_workflows import classify_benchmark
from .performance_workflows import compare_plan_summaries_payload
from .performance_workflows import database_fingerprint
from .performance_workflows import extract_profile_metrics
from .performance_workflows import fingerprint_json
from .plan_action_service import PlanActionService
from .plan_cache import PlanCacheService
from .plan_enforcement import PlanEnforcementService
from .plans import PlansService
from .prompts import register_prompts
from .query_index_analysis import QueryIndexAnalysisService
from .query_hints import validate_query_hints
from .query_regression import QueryRegressionService
from .query_store import QueryStoreService
from .resource_governance import ResourceGovernanceService
from .resources import register_resources
from .safe_sql import SafeSqlValidator
from .schema_compare import SchemaCompareService
from .sessions import SessionsService
from .tempdb_memory import TempdbMemoryService
from .tuning_sessions import TuningSessionStateMachine
from .transport_auth import StaticBearerTokenVerifier
from .wait_stats import WaitStatsService

ResponseType = dict[str, Any]

logger = logging.getLogger(__name__)

# Disposable test indexes (create_test_index / drop_test_index) are namespaced by this
# prefix; the drop tool refuses anything outside it so real indexes are untouchable.
TEST_INDEX_PREFIX = "IX_Testing_"
_PLAIN_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _auth_settings(config: ServerConfig) -> AuthSettings:
    host = config.transport.host
    if host in {"0.0.0.0", "::"}:
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    resource_url = AnyHttpUrl(f"http://{host}:{config.transport.port}")
    return AuthSettings(
        issuer_url=resource_url,
        resource_server_url=resource_url,
        required_scopes=["azure-sql-mcp"],
    )


class AzureSqlMcpApplication:
    def __init__(self, config: ServerConfig):
        self.config = config
        token_verifier = (
            StaticBearerTokenVerifier(config.mcp_bearer_token)
            if config.mcp_bearer_token
            else None
        )
        self.mcp = FastMCP(
            "azure-sql-mcp",
            token_verifier=token_verifier,
            auth=_auth_settings(config) if token_verifier else None,
        )

        authenticator = AzureSqlAuthenticator(config)
        pool = ConnectionPool(config, authenticator)
        executor = AzureSqlExecutor(config, authenticator, pool)
        validator = SafeSqlValidator()

        self.pool = pool

        self.executor = executor
        self.validator = validator
        self.artifacts = ArtifactStore()
        self.admin_policy = AdminPolicy(config)
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
        self.diagnostics = DiagnosticQueryService(executor)
        self.plan_cache = PlanCacheService(executor)
        self.query_regression = QueryRegressionService(executor)
        self.plan_enforcement = PlanEnforcementService(
            executor,
            self.query_regression,
            self.admin_policy,
        )
        self.param_binding = ParameterBindingService(executor)
        self.database_policy = load_database_policy_or_deny(
            config.database_policy_file
        )
        self.performance_store = (
            PerformanceStore(db_path=":memory:")
            if config.performance_state_dir == ":memory:"
            else PerformanceStore(config.performance_state_dir)
        )
        self.tuning_sessions = TuningSessionStateMachine(self.performance_store)
        self.performance_workflows = PerformanceWorkflowService(
            executor=executor,
            plans=self.plans,
            validator=validator,
            store=self.performance_store,
            sessions=self.tuning_sessions,
            database_policy=self.database_policy,
            row_limit=config.row_limit,
            parameter_binder=self._bind_performance_parameters,
        )
        self.plan_actions = PlanActionService(
            config=config,
            executor=executor,
            admin_policy=self.admin_policy,
            database_policy=self.database_policy,
            store=self.performance_store,
        )
        self.capabilities = CapabilityService(
            executor,
            self.query_store,
            self.plans,
            self.recommendations,
        )

        self._register_tools()
        self._prune_disabled_tools()
        register_resources(self.mcp, self.config, self.introspection, self.artifacts)
        register_prompts(self.mcp, self.config)

    def _prune_disabled_tools(self) -> None:
        """Remove tools that are not in the configured tool_groups."""
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
            limit: int = Field(
                default=200,
                description="Maximum sessions to return (longest-running first, max 1000).",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_active_sessions",
                database_name,
                lambda db: self.sessions.get_active_sessions(db, limit),
            )

        @self.mcp.tool(
            description=(
                "Execute a read-only SQL query in restricted mode. The query may be "
                "preceded by DECLARE @var and SET @var = ... statements, followed by "
                "exactly one SELECT."
            ),
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
            parameter_values: dict[str, Any] | None = Field(
                default=None,
                description=(
                    "Optional JSON parameter values keyed by name (for example "
                    "{'CustomerId': 42}); values are safely rendered as T-SQL literals."
                ),
            ),
            include_raw_xml: bool = Field(
                default=False,
                description=(
                    "When true, includes raw SHOWPLAN XML inline. Defaults to False; "
                    "use raw_xml_resource_uri for token-safe retrieval."
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
                    include_raw_xml,
                    parameter_values,
                ),
            )

        @self.mcp.tool(
            description=(
                "Collect a structured single-query tuning evidence pack: actual or "
                "estimated plan summary, bounded result sample, Query Store history, "
                "index recommendations, waits, and statistics health."
            ),
            annotations=ToolAnnotations(
                title="Tune Query",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def tune_query(
            sql: str = Field(description="Read-only SQL query to tune."),
            analyze: bool = Field(
                default=True,
                description="When true, execute the query to capture an actual plan.",
            ),
            auto_bind_params: bool = Field(
                default=True,
                description="Bind @param placeholders from column statistics where possible.",
            ),
            parameter_values: dict[str, Any] | None = Field(
                default=None,
                description=(
                    "Explicit parameter values for representative execution; prefer this "
                    "over heuristic statistics/type fallback binding."
                ),
            ),
            include_raw_xml: bool = Field(
                default=False,
                description="Include raw SHOWPLAN XML inline. Defaults to token-safe artifact URI only.",
            ),
            window_minutes: int = Field(
                default=1440,
                description="Query Store lookback window for history evidence.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "tune_query",
                database_name,
                lambda resolved_database: self._tune_query(
                    resolved_database,
                    sql,
                    analyze,
                    auto_bind_params,
                    include_raw_xml,
                    window_minutes,
                    parameter_values,
                ),
            )

        @self.mcp.tool(
            description=(
                "Benchmark a baseline query against a proposed rewrite using the same "
                "read-only execution path, actual-plan summaries, bounded row samples, "
                "and a sample equivalence check."
            ),
            annotations=ToolAnnotations(
                title="Benchmark Query Rewrite",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def benchmark_query_rewrite(
            baseline_sql: str = Field(description="Original read-only SQL query."),
            rewrite_sql: str = Field(description="Candidate semantically equivalent rewrite."),
            analyze: bool = Field(
                default=True,
                description="When true, execute both queries to capture actual plans.",
            ),
            auto_bind_params: bool = Field(
                default=True,
                description="Bind @param placeholders from column statistics where possible.",
            ),
            parameter_values: dict[str, Any] | None = Field(
                default=None,
                description="Explicit parameter values for both baseline and rewrite.",
            ),
            compare_order: bool = Field(
                default=True,
                description=(
                    "Compare result rows in returned order. Set false only when the "
                    "query contract does not require ordering."
                ),
            ),
            include_raw_xml: bool = Field(
                default=False,
                description="Include raw SHOWPLAN XML inline. Defaults to artifact URI only.",
            ),
            runs: int = Field(
                default=3,
                description=(
                    "Screening executions per side (1-3). Metrics "
                    "become per-run medians with min/max spread, so a single lucky run "
                    "cannot masquerade as a win. Run 1 is typically cold-cache."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "benchmark_query_rewrite",
                database_name,
                lambda resolved_database: self._benchmark_query_rewrite(
                    resolved_database,
                    baseline_sql,
                    rewrite_sql,
                    analyze,
                    auto_bind_params,
                    include_raw_xml,
                    runs,
                    parameter_values,
                    compare_order,
                ),
            )

        @self.mcp.tool(
            description=(
                "Create a durable, redacted performance case. SQL is fingerprinted "
                "and is not written to the MCP state database."
            ),
            annotations=ToolAnnotations(
                title="Start Performance Case",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def start_performance_case(
            sql: str = Field(description="Baseline read-only SELECT-shaped SQL."),
            parameter_cases: list[dict[str, Any]] | None = Field(
                default=None,
                description=(
                    "Up to four named parameter cases, for example common, rare, NULL, "
                    "and boundary. Values are fingerprinted, not persisted."
                ),
            ),
            objective: str = Field(
                default="elapsed_time",
                description="Primary tuning objective recorded with the case.",
            ),
            idempotency_key: str | None = Field(
                default=None,
                description="Optional caller-generated idempotency key.",
            ),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "start_performance_case",
                database_name,
                lambda db: self._start_performance_case(
                    db,
                    sql,
                    parameter_cases,
                    objective,
                    idempotency_key,
                ),
            )

        @self.mcp.tool(
            description=(
                "Collect Azure SQL resource, Query Store, wait, blocking, statistics, "
                "parameter-sensitivity, and regression evidence for a performance case."
            ),
            annotations=ToolAnnotations(
                title="Collect Performance Evidence",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def collect_performance_evidence(
            case_id: str = Field(description="Performance case identifier."),
            sql: str = Field(description="The same baseline SQL used to create the case."),
            window_minutes: int = Field(default=60, ge=1),
            execute_query: bool = Field(
                default=False,
                description=(
                    "Capture one actual-plan sample. Requires database benchmark policy."
                ),
            ),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "collect_performance_evidence",
                database_name,
                lambda db: self._collect_performance_evidence(
                    db,
                    case_id,
                    sql,
                    window_minutes,
                    execute_query,
                    idempotency_key,
                ),
            )

        @self.mcp.tool(
            description="Get a redacted performance case, its evidence envelopes, and event history.",
            annotations=ToolAnnotations(
                title="Get Performance Case",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def get_performance_case(
            case_id: str = Field(description="Performance case identifier."),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "get_performance_case",
                database_name,
                lambda db: self._get_performance_case(db, case_id),
            )

        @self.mcp.tool(
            description=(
                "Start a durable iterative tuning session with default budgets of 10 "
                "candidates, 80 executions, and 20 minutes."
            ),
            annotations=ToolAnnotations(
                title="Start Tuning Session",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def start_tuning_session(
            case_id: str = Field(description="Performance case identifier."),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "start_tuning_session",
                database_name,
                lambda db: self._start_tuning_session(db, case_id, idempotency_key),
            )

        @self.mcp.tool(
            description=(
                "Add one concrete rewrite or index experiment to a tuning session. "
                "Only its fingerprint and optional artifact reference are persisted."
            ),
            annotations=ToolAnnotations(
                title="Add Tuning Candidate",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def add_tuning_candidate(
            session_id: str = Field(description="Tuning session identifier."),
            candidate_sql: str = Field(description="Concrete read-only candidate SQL."),
            strategy: str = Field(
                description=(
                    "Candidate family: predicate, join, aggregation, cardinality, index, or combined."
                )
            ),
            artifact_ref: str | None = Field(default=None),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "add_tuning_candidate",
                database_name,
                lambda db: self._add_tuning_candidate(
                    db,
                    session_id,
                    candidate_sql,
                    strategy,
                    artifact_ref,
                    idempotency_key,
                ),
            )

        @self.mcp.tool(
            description=(
                "Benchmark one rewrite candidate with interleaved, exactly-once samples "
                "and snapshot-consistent duplicate-aware result comparison."
            ),
            annotations=ToolAnnotations(
                title="Benchmark Tuning Candidate",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def benchmark_tuning_candidate(
            session_id: str = Field(description="Tuning session identifier."),
            candidate_id: str = Field(description="Candidate identifier."),
            baseline_sql: str = Field(description="Baseline read-only SQL."),
            candidate_sql: str = Field(description="Candidate read-only SQL."),
            phase: str = Field(default="screening", description="screening or finalist"),
            parameter_cases: list[dict[str, Any]] | None = Field(default=None),
            compare_order: bool = Field(default=True),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "benchmark_tuning_candidate",
                database_name,
                lambda db: self.performance_workflows.benchmark_candidate(
                    session_id,
                    candidate_id,
                    db,
                    baseline_sql,
                    candidate_sql,
                    phase=phase,
                    parameter_cases=parameter_cases,
                    compare_order=compare_order,
                    idempotency_key=idempotency_key,
                ),
            )

        @self.mcp.tool(
            description=(
                "Benchmark a leased disposable index in a policy-allowlisted sandbox. "
                "Cleanup is automatic and cleanup failures are durable."
            ),
            annotations=ToolAnnotations(
                title="Benchmark Index Candidate",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=False,
                openWorldHint=False,
            ),
        )
        async def benchmark_index_candidate(
            session_id: str = Field(description="Tuning session identifier."),
            candidate_id: str = Field(description="Index candidate identifier."),
            sql: str = Field(description="Read-only query measured before and after the index."),
            schema_name: str = Field(description="Target schema."),
            table_name: str = Field(description="Target table."),
            key_columns: list[str] = Field(description="Ordered key columns, optionally ASC/DESC."),
            include_columns: list[str] | None = Field(default=None),
            parameter_cases: list[dict[str, Any]] | None = Field(
                default=None,
                description="The same named parameter cases recorded on the performance case.",
            ),
            phase: str = Field(default="screening", description="screening or finalist"),
            online: bool = Field(default=True),
            lease_minutes: int = Field(default=30, ge=5, le=120),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "benchmark_index_candidate",
                database_name,
                lambda db: self._benchmark_index_candidate(
                    db,
                    session_id,
                    candidate_id,
                    sql,
                    schema_name,
                    table_name,
                    key_columns,
                    include_columns,
                    phase,
                    online,
                    lease_minutes,
                    idempotency_key,
                    parameter_cases,
                ),
            )

        @self.mcp.tool(
            description=(
                "Finalize a tuning session with the winning candidate, complete leaderboard, "
                "rejected experiments, and explicit stopping reason."
            ),
            annotations=ToolAnnotations(
                title="Finalize Tuning Session",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def finalize_tuning_session(
            session_id: str = Field(description="Tuning session identifier."),
            selected_candidate_id: str | None = Field(default=None),
            stopping_reason: str = Field(description="Why the optimizer stopped."),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "finalize_tuning_session",
                database_name,
                lambda db: self._finalize_tuning_session(
                    db,
                    session_id,
                    selected_candidate_id,
                    stopping_reason,
                    idempotency_key,
                ),
            )

        @self.mcp.tool(
            description=(
                "Compare two read-only query results in one snapshot. The result is proven "
                "only when the complete bounded results, duplicates, shape, and required order match."
            ),
            annotations=ToolAnnotations(
                title="Compare Query Results",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def compare_query_results(
            baseline_sql: str = Field(description="Baseline read-only SQL."),
            candidate_sql: str = Field(description="Candidate read-only SQL."),
            compare_order: bool = Field(default=True),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "compare_query_results",
                database_name,
                lambda db: self.performance_workflows.compare_query_results(
                    db,
                    baseline_sql,
                    candidate_sql,
                    compare_order=compare_order,
                ),
            )

        @self.mcp.tool(
            description="Compare arbitrary redacted execution-plan summaries and sourced metrics.",
            annotations=ToolAnnotations(
                title="Compare Plan Summaries",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def compare_plan_summaries(
            baseline_summary: dict[str, Any] = Field(description="Baseline plan summary."),
            candidate_summary: dict[str, Any] = Field(description="Candidate plan summary."),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "compare_plan_summaries",
                database_name,
                lambda db: self._compare_plan_summaries(
                    db,
                    baseline_summary,
                    candidate_summary,
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
                min_length=1,
                max_length=10,
                description="List of SQL SELECT queries to analyze (max 10).",
            ),
            auto_bind_params: bool = Field(
                default=False,
                description=(
                    "When true, automatically binds @param placeholders in queries "
                    "using column statistics before analyzing."
                ),
            ),
            parameter_values: dict[str, Any] | None = Field(
                default=None,
                description="Explicit parameter values for the supplied queries.",
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
                    parameter_values,
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
            limit: int = Field(
                default=200,
                description="Maximum lock rows to return (waiting locks first, max 1000).",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_lock_details",
                database_name,
                lambda db: self.lock_diagnostics.get_lock_details(db, limit),
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
            limit: int = Field(
                default=100,
                description="Maximum transactions to return (oldest first, max 1000).",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_open_transactions",
                database_name,
                lambda db: self.lock_diagnostics.get_open_transactions(db, limit),
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
            limit: int = Field(
                default=200,
                description="Maximum sessions to return (largest tempdb consumers first, max 1000).",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_tempdb_usage",
                database_name,
                lambda db: self.tempdb_memory.get_tempdb_usage(db, limit),
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

        @self.mcp.tool(
            description=(
                "Get MCP server connection pool statistics: per-database acquire/"
                "release/discard counts, peak utilization, and possible connection "
                "leaks. Diagnoses MCP-side slowness without touching the database."
            ),
            annotations=ToolAnnotations(
                title="Get Connection Pool Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=False,
            ),
        )
        async def get_connection_pool_stats() -> ResponseType:
            return self._format_response(
                {
                    "server": self.config.server,
                    "pool_size_per_database": self.config.pool_size,
                    "metrics": self.pool.get_metrics(),
                    "possible_leaks": self.pool.check_leaked_connections(),
                }
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

        # --- Phase 22: Azure SQL Diagnostic Query Parity ---

        @self.mcp.tool(
            description=(
                "Get Azure SQL database configuration inventory: version, read-only "
                "instance settings, database properties, scoped configurations, Query Store, "
                "automatic tuning, geo-replication links, and Azure DB properties."
            ),
            annotations=ToolAnnotations(
                title="Get Database Configuration",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_database_configuration(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_database_configuration",
                database_name,
                self.diagnostics.get_database_configuration,
            )

        @self.mcp.tool(
            description=(
                "Get Azure SQL storage diagnostics: database/file size, log usage, "
                "VLF counts, last VLF status, and high-usage warnings."
            ),
            annotations=ToolAnnotations(
                title="Get Storage Diagnostics",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_storage_diagnostics(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_storage_diagnostics",
                database_name,
                self.diagnostics.get_storage_diagnostics,
            )

        @self.mcp.tool(
            description=(
                "Get connection diagnostics: connection counts by client IP, session "
                "summary, and optional bounded input-buffer text for current database sessions."
            ),
            annotations=ToolAnnotations(
                title="Get Connection Diagnostics",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_connection_diagnostics(
            limit: int = Field(default=50, description="Maximum rows per detail section."),
            include_input_buffer: bool = Field(
                default=False,
                description=(
                    "Include sys.dm_exec_input_buffer details when available. This can expose "
                    "sensitive SQL text and literals, so it is disabled by default."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_connection_diagnostics",
                database_name,
                lambda db: self.diagnostics.get_connection_diagnostics(
                    db,
                    limit=limit,
                    include_input_buffer=include_input_buffer,
                ),
            )

        @self.mcp.tool(
            description=(
                "Get top cached statements from sys.dm_exec_query_stats. Returns "
                "bounded text previews and plan-cache metrics without raw plan XML."
            ),
            annotations=ToolAnnotations(
                title="Get Top Cached Queries",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_top_cached_queries(
            sort_by: str = Field(
                default="total_worker_time",
                description=(
                    "Sort by execution_count, total_worker_time, avg_worker_time, "
                    "total_elapsed_time, avg_elapsed_time, total_logical_reads, "
                    "total_physical_reads, or total_logical_writes."
                ),
            ),
            limit: int = Field(default=25, description="Maximum cached queries to return."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_top_cached_queries",
                database_name,
                lambda db: self.diagnostics.get_top_cached_queries(db, sort_by, limit),
            )

        @self.mcp.tool(
            description=(
                "Get cached stored procedure and UDF execution statistics. Returns "
                "bounded routine metrics and missing-index flags without raw plan XML."
            ),
            annotations=ToolAnnotations(
                title="Get Cached Routine Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_cached_routine_stats(
            routine_type: str = Field(
                default="all",
                description="Routine type: all, procedure, or function.",
            ),
            sort_by: str = Field(
                default="total_worker_time",
                description=(
                    "Sort by execution_count, total_worker_time, avg_worker_time, "
                    "total_elapsed_time, avg_elapsed_time, total_logical_reads, "
                    "total_physical_reads, or total_logical_writes."
                ),
            ),
            limit: int = Field(default=25, description="Maximum routines per section."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_cached_routine_stats",
                database_name,
                lambda db: self.diagnostics.get_cached_routine_stats(
                    db,
                    routine_type=routine_type,
                    sort_by=sort_by,
                    limit=limit,
                ),
            )

        @self.mcp.tool(
            description=(
                "Get object and index diagnostics: write-heavy nonclustered indexes, "
                "read/write usage, buffer footprint, volatile stats, columnstore row groups, "
                "index lock waits, and resumable index rebuilds."
            ),
            annotations=ToolAnnotations(
                title="Get Object Index Diagnostics",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_object_index_diagnostics(
            schema_name: str | None = Field(
                default=None,
                description="Optional schema filter.",
            ),
            table_name: str | None = Field(
                default=None,
                description="Optional table filter.",
            ),
            limit: int = Field(default=25, description="Maximum rows per detail section."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_object_index_diagnostics",
                database_name,
                lambda db: self.diagnostics.get_object_index_diagnostics(
                    db,
                    schema_name=schema_name,
                    table_name=table_name,
                    limit=limit,
                ),
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
            window_minutes: int = Field(
                default=1440,
                ge=1,
                description="Query Store lookback window in minutes (default 24 hours).",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "detect_regressed_queries",
                database_name,
                lambda db: self.query_regression.detect_regressed_queries(
                    db, window_minutes
                ),
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
            window_minutes: int = Field(
                default=1440,
                ge=1,
                description="Query Store lookback window in minutes (default 24 hours).",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_forced_plans",
                database_name,
                lambda db: self.query_regression.get_forced_plans(db, window_minutes),
            )

        @self.mcp.tool(
            description=(
                "Extract the compiled parameter values behind each Query Store plan for "
                "one query — the parameter buckets a tuning pass must test. Each distinct "
                "compiled set produced its own plan shape in production; pair with "
                "boundary/NULL/empty cases the history cannot show."
            ),
            annotations=ToolAnnotations(
                title="Get Query Parameter Buckets",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def get_query_parameter_buckets(
            query_id: int = Field(description="Query Store query_id."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_query_parameter_buckets",
                database_name,
                lambda db: self.query_regression.get_query_parameter_buckets(db, query_id),
            )

        @self.mcp.tool(
            description=(
                "Review Query Store health, parameter sensitivity, regressions, "
                "forced-plan failures, and ranked force/unforce candidates."
            ),
            annotations=ToolAnnotations(
                title="Plan Health Review",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def plan_health_review(
            window_minutes: int = Field(
                default=1440,
                description="Query Store lookback window in minutes.",
            ),
            top_n: int = Field(default=20, description="Maximum ranked findings to return."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "plan_health_review",
                database_name,
                lambda db: self._plan_health_review(
                    db,
                    window_minutes=window_minutes,
                    top_n=top_n,
                ),
            )

        @self.mcp.tool(
            description=(
                "Preview one plan-enforcement cycle. This tool is permanently read-only; "
                "use the prepared plan-action workflow for reviewed mutations."
            ),
            annotations=ToolAnnotations(
                title="Plan Enforcer Tick",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def plan_enforcer_tick(
            window_minutes: int = Field(
                default=1440,
                description="Query Store lookback window in minutes.",
            ),
            max_actions: int = Field(
                default=1,
                description="Maximum force/unforce actions to preview or apply.",
            ),
            dry_run: bool = Field(
                default=True,
                description="Compatibility flag. False is rejected; preview is permanent.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            if not dry_run:
                return self._format_error(
                    "preview_only",
                    "plan_enforcer_tick is permanently preview-only; prepare a reviewed action.",
                )
            return await self._run_tool(
                "plan_enforcer_tick",
                database_name,
                lambda db: self.plan_enforcement.tick(
                    db,
                    window_minutes=window_minutes,
                    max_actions=max_actions,
                    dry_run=True,
                ),
            )

        @self.mcp.tool(
            description=(
                "Review Query Store regressions and forced-plan health, then rank "
                "read-only candidate plan force/unforce actions."
            ),
            annotations=ToolAnnotations(
                title="Review Plan Enforcement",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def review_plan_enforcement(
            window_minutes: int = Field(
                default=1440,
                description="Query Store lookback window in minutes.",
            ),
            top_n: int = Field(default=20, description="Maximum ranked actions to return."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "review_plan_enforcement",
                database_name,
                lambda db: self.plan_enforcement.review(
                    db,
                    window_minutes=window_minutes,
                    top_n=top_n,
                ),
            )

        @self.mcp.tool(
            description=(
                "Preview an exact reversible Query Store force/unforce action and "
                "record an audit entry without executing it."
            ),
            annotations=ToolAnnotations(
                title="Dry Run Plan Action",
                readOnlyHint=False,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def dry_run_plan_action(
            action: str = Field(description="'force' or 'unforce'."),
            query_id: int = Field(description="Query Store query_id."),
            plan_id: int = Field(description="Query Store plan_id."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "dry_run_plan_action",
                database_name,
                lambda db: self.plan_enforcement.dry_run_action(
                    db,
                    action=action,
                    query_id=query_id,
                    plan_id=plan_id,
                ),
            )

        @self.mcp.tool(
            description=(
                "Capture exact Query Store control state and persist a reviewed, redacted "
                "plan-action intent. Automatic Tuning ownership is review-only."
            ),
            annotations=ToolAnnotations(
                title="Prepare Plan Action",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def prepare_plan_action(
            session_id: str = Field(description="Shared tuning session identifier."),
            operation: str = Field(
                description="force_plan, unforce_plan, set_hints, or clear_hints."
            ),
            query_id: int = Field(description="Query Store query_id."),
            evidence: dict[str, Any] = Field(description="Reviewed pre-change evidence window."),
            reviewed_by: str = Field(description="Human reviewer identifier."),
            reason: str = Field(description="Reviewed reason for the action."),
            idempotency_key: str = Field(description="Unique idempotency key."),
            candidate_id: str | None = Field(default=None),
            plan_id: int | None = Field(default=None),
            query_hints: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "prepare_plan_action",
                database_name,
                lambda db: self.plan_actions.prepare(
                    db,
                    session_id=session_id,
                    candidate_id=candidate_id,
                    operation=operation,
                    query_id=query_id,
                    plan_id=plan_id,
                    query_hints=query_hints,
                    evidence=evidence,
                    reviewed_by=reviewed_by,
                    reason=reason,
                    idempotency_key=idempotency_key,
                ),
            )

        @self.mcp.tool(
            description=(
                "Apply one prepared intent after rechecking server policy, database policy, "
                "kill switch, explicit authorization, ownership, and exact prior state."
            ),
            annotations=ToolAnnotations(
                title="Apply Prepared Plan Action",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def apply_prepared_plan_action(
            intent_id: str = Field(description="Prepared plan-action intent identifier."),
            authorization_reference: str = Field(
                description="Explicit per-action authorization or change reference."
            ),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "apply_prepared_plan_action",
                database_name,
                lambda db: self.plan_actions.apply(
                    db,
                    intent_id,
                    authorization_reference=authorization_reference,
                ),
            )

        @self.mcp.tool(
            description=(
                "Verify non-overlapping pre/post Query Store windows with matching parameter "
                "buckets. Regression restores the exact prior force and hint state."
            ),
            annotations=ToolAnnotations(
                title="Verify Plan Action",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def verify_plan_action(
            intent_id: str = Field(description="Applied plan-action intent identifier."),
            candidate_evidence: dict[str, Any] = Field(
                description="Post-change Query Store evidence window."
            ),
            authorization_reference: str = Field(
                description="Authorization used if verification requires rollback."
            ),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "verify_plan_action",
                database_name,
                lambda db: self.plan_actions.verify(
                    db,
                    intent_id,
                    candidate_evidence=candidate_evidence,
                    authorization_reference=authorization_reference,
                ),
            )

        @self.mcp.tool(
            description="Restore the exact force-plan and Query Store hint state captured at review.",
            annotations=ToolAnnotations(
                title="Rollback Plan Action",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def rollback_plan_action(
            intent_id: str = Field(description="Plan-action intent identifier."),
            authorization_reference: str = Field(
                description="Explicit rollback authorization or change reference."
            ),
            reason: str = Field(default="explicit rollback requested"),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "rollback_plan_action",
                database_name,
                lambda db: self.plan_actions.rollback(
                    db,
                    intent_id,
                    authorization_reference=authorization_reference,
                    reason=reason,
                ),
            )

        @self.mcp.tool(
            description=(
                "Analyze operational Azure SQL database health. Query-performance triage "
                "belongs to collect_performance_evidence, which uses resource, Query Store, "
                "wait, blocking, statistics, parameter-sensitivity, and regression evidence."
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
                    "Health type: connection, constraint, replication, identity, query_store, "
                    "tuning, resource, storage, statistics, or all."
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
                description=(
                    "Preview a Query Store force/unforce action. Direct execution is "
                    "blocked; use prepare_plan_action and apply_prepared_plan_action."
                ),
                annotations=ToolAnnotations(
                    title="Apply Plan Action",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def apply_plan_action(
                action: str = Field(description="'force' or 'unforce'."),
                query_id: int = Field(description="Query Store query_id."),
                plan_id: int = Field(description="Query Store plan_id."),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Must remain true. Direct plan actions are permanently preview-only."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "apply_plan_action",
                    database_name,
                    lambda db: self.plan_enforcement.apply_action(
                        db,
                        action=action,
                        query_id=query_id,
                        plan_id=plan_id,
                        dry_run=dry_run,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Run DBA T-SQL against an allowlisted initial database. Direct or "
                    "statically recoverable DROP DATABASE statements are rejected, but SQL "
                    "assembled only at runtime cannot be proven or blocked. Each call makes "
                    "one submission with no retry, uses an isolated connection that is discarded, "
                    "and drains every result set. GO is a client batch separator, not T-SQL, "
                    "and is unsupported."
                ),
                annotations=ToolAnnotations(
                    title="Execute Unrestricted T-SQL",
                    readOnlyHint=False,
                    destructiveHint=True,
                    idempotentHint=False,
                    openWorldHint=True,
                ),
            )
            async def execute_tsql_unrestricted(
                sql: str = Field(
                    description=(
                        "One T-SQL batch to execute. Do not include the client-side GO separator. "
                        "DROP DATABASE is rejected when directly or statically recoverable."
                    )
                ),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Preview and audit without execution by default. Set False only "
                        "with AZURE_SQL_WRITE_POLICY=apply."
                    ),
                ),
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
                        dry_run,
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
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Preview and audit without execution by default. Set False only "
                        "with AZURE_SQL_WRITE_POLICY=apply."
                    ),
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
                        db, schema_name, table_name, index_name, operation, online, dry_run,
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
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Preview and audit without execution by default. Set False only "
                        "with AZURE_SQL_WRITE_POLICY=apply."
                    ),
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
                        db, schema_name, table_name, stat_name, sample_percent, dry_run,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Preview forcing or unforcing a Query Store plan. Direct execution "
                    "is blocked; use the reviewed prepared plan-action workflow."
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
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Must remain true. Direct force/unforce is permanently preview-only."
                    ),
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
                        db, query_id, plan_id, unforce, dry_run,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Preview validated Query Store hints for a query. Direct execution is "
                    "blocked; use the reviewed prepared plan-action workflow."
                ),
                annotations=ToolAnnotations(
                    title="Set Query Store Hints",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def set_query_store_hints(
                query_id: int = Field(description="Query Store query_id."),
                query_hints: str = Field(
                    description=(
                        "Hints as a single OPTION(...) clause, e.g. OPTION(RECOMPILE) or "
                        "OPTION(OPTIMIZE FOR (@p = 42)). Validated against an allowlist."
                    ),
                ),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Must remain true. Direct Query Store hint changes are preview-only."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "set_query_store_hints",
                    database_name,
                    lambda db: self._set_query_store_hints(
                        db, query_id, query_hints, dry_run,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Preview clearing Query Store hints. Direct execution is blocked; "
                    "use the reviewed prepared plan-action workflow."
                ),
                annotations=ToolAnnotations(
                    title="Clear Query Store Hints",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def clear_query_store_hints(
                query_id: int = Field(description="Query Store query_id."),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Must remain true. Direct Query Store hint changes are preview-only."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "clear_query_store_hints",
                    database_name,
                    lambda db: self._clear_query_store_hints(db, query_id, dry_run),
                )

            @self.mcp.tool(
                description=(
                    "Preview a disposable test index definition. Direct creation is blocked; "
                    "use benchmark_index_candidate for a leased sandbox measurement."
                ),
                annotations=ToolAnnotations(
                    title="Create Test Index",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=False,
                    openWorldHint=False,
                ),
            )
            async def create_test_index(
                schema_name: str = Field(description="Schema name (e.g. 'dbo')."),
                table_name: str = Field(description="Table name."),
                index_name: str = Field(
                    description="Index name — must start with the IX_Testing_ prefix.",
                ),
                key_columns: list[str] = Field(
                    description=(
                        "Key columns in order; each may carry an ASC/DESC suffix, "
                        "e.g. ['ShipDate', 'StatusCode DESC']."
                    ),
                ),
                include_columns: list[str] | None = Field(
                    default=None,
                    description="Optional INCLUDE columns (plain column names).",
                ),
                online: bool = Field(
                    default=True,
                    description="Use ONLINE=ON (avoids blocking during the build).",
                ),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Must remain true. Direct test-index creation is preview-only."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "create_test_index",
                    database_name,
                    lambda db: self._create_test_index(
                        db, schema_name, table_name, index_name,
                        key_columns, include_columns, online, dry_run,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Preview dropping a namespaced test index. Direct removal is blocked; "
                    "leased cleanup belongs to benchmark_index_candidate."
                ),
                annotations=ToolAnnotations(
                    title="Drop Test Index",
                    readOnlyHint=False,
                    destructiveHint=True,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def drop_test_index(
                schema_name: str = Field(description="Schema name (e.g. 'dbo')."),
                table_name: str = Field(description="Table name."),
                index_name: str = Field(
                    description="Index name — must start with the IX_Testing_ prefix.",
                ),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Must remain true. Direct test-index removal is preview-only."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "drop_test_index",
                    database_name,
                    lambda db: self._drop_test_index(
                        db, schema_name, table_name, index_name, dry_run,
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
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Preview and audit without execution by default. Set False only "
                        "with AZURE_SQL_WRITE_POLICY=apply."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "kill_session",
                    database_name,
                    lambda db: self._kill_session(db, session_id, dry_run),
                )

    async def _bind_performance_parameters(
        self,
        database_name: str,
        sql: str,
        values: dict[str, Any] | Any,
    ) -> str:
        if not isinstance(values, dict):
            values = dict(values)
        binding = await self.param_binding.bind_parameters(
            database_name,
            sql,
            parameter_values=values,
        )
        bound_sql = binding.get("bound_sql")
        if not isinstance(bound_sql, str) or not bound_sql.strip():
            raise ValueError("Parameter binding did not produce executable SQL.")
        return bound_sql

    async def _start_performance_case(
        self,
        database_name: str,
        sql: str,
        parameter_cases: list[dict[str, Any]] | None,
        objective: str,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        performance_case = self.performance_workflows.start_case(
            database_name,
            sql,
            parameter_cases=parameter_cases,
            metadata={"objective": objective, "raw_sql_persisted": False},
            idempotency_key=idempotency_key,
        )
        return performance_case.to_dict()

    async def _collect_performance_evidence(
        self,
        database_name: str,
        case_id: str,
        sql: str,
        window_minutes: int,
        execute_query: bool,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        collectors: dict[str, Callable[[], Awaitable[Any]]] = {
            "resource_limits": lambda: self.resource_governance.get_resource_limits(
                database_name
            ),
            "resource_history": lambda: self.resource_governance.get_resource_stats_history(
                database_name,
                window_minutes,
            ),
            "query_store_status": lambda: self.query_store.get_status(database_name),
            "query_store_history": lambda: self.query_store.get_query_history_by_text(
                database_name,
                sql,
                window_minutes=window_minutes,
                limit=20,
            ),
            "waits": lambda: self.wait_stats.get_wait_stats(database_name, top_n=20),
            "blocking": lambda: self.lock_diagnostics.get_lock_details(
                database_name,
                limit=200,
            ),
            "open_transactions": lambda: self.lock_diagnostics.get_open_transactions(
                database_name,
                limit=100,
            ),
            "statistics": lambda: self.plan_cache.check_statistics_health(database_name),
            "parameter_sensitivity": lambda: self.query_regression.detect_parameter_sniffing(
                database_name,
                window_minutes=window_minutes,
                top_n=20,
            ),
            "regressions": lambda: self.query_regression.detect_regressed_queries(
                database_name,
                window_minutes=window_minutes,
            ),
        }
        return await self.performance_workflows.collect_case_evidence(
            case_id,
            database_name,
            sql,
            collectors,
            window_minutes=window_minutes,
            execute_query=execute_query,
            idempotency_key=idempotency_key,
        )

    async def _get_performance_case(
        self,
        database_name: str,
        case_id: str,
    ) -> dict[str, Any]:
        payload = self.performance_workflows.get_case(case_id)
        case = payload["case"]
        if case.get("database_fingerprint") != database_fingerprint(database_name):
            raise PermissionError("Performance case belongs to another database.")
        return payload

    async def _start_tuning_session(
        self,
        database_name: str,
        case_id: str,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        case = self.performance_store.get_performance_case(case_id)
        if case.database_fingerprint != database_fingerprint(database_name):
            raise PermissionError("Performance case belongs to another database.")
        return self.performance_workflows.start_session(
            case_id,
            idempotency_key=idempotency_key,
        )

    async def _add_tuning_candidate(
        self,
        database_name: str,
        session_id: str,
        candidate_sql: str,
        strategy: str,
        artifact_ref: str | None,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        session = self.tuning_sessions.get_session(session_id)
        case = self.performance_store.get_performance_case(session.performance_case_id)
        if case.database_fingerprint != database_fingerprint(database_name):
            raise PermissionError("Tuning session belongs to another database.")
        return self.performance_workflows.add_candidate(
            session_id,
            candidate_sql,
            strategy=strategy,
            artifact_ref=artifact_ref,
            idempotency_key=idempotency_key,
        )

    async def _finalize_tuning_session(
        self,
        database_name: str,
        session_id: str,
        selected_candidate_id: str | None,
        stopping_reason: str,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        session = self.tuning_sessions.get_session(session_id)
        case = self.performance_store.get_performance_case(session.performance_case_id)
        if case.database_fingerprint != database_fingerprint(database_name):
            raise PermissionError("Tuning session belongs to another database.")
        return self.performance_workflows.finalize_session(
            session_id,
            selected_candidate_id=selected_candidate_id,
            stopping_reason=stopping_reason,
            idempotency_key=idempotency_key,
        )

    async def _compare_plan_summaries(
        self,
        _database_name: str,
        baseline_summary: dict[str, Any],
        candidate_summary: dict[str, Any],
    ) -> dict[str, Any]:
        return compare_plan_summaries_payload(baseline_summary, candidate_summary)

    async def _benchmark_index_candidate(
        self,
        database_name: str,
        session_id: str,
        candidate_id: str,
        sql: str,
        schema_name: str,
        table_name: str,
        key_columns: list[str],
        include_columns: list[str] | None,
        phase: str,
        online: bool,
        lease_minutes: int,
        idempotency_key: str | None,
        parameter_cases: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        if self.config.profile != McpProfile.SANDBOX:
            raise PermissionError("Index benchmarking requires the sandbox MCP profile.")
        policy = self.database_policy.require(database_name)
        if policy.environment.casefold() in {"production", "prod", "live"}:
            raise PermissionError("Temporary indexes are prohibited in production policy entries.")
        if not policy.allow_test_indexes:
            raise PermissionError("Database policy does not permit temporary indexes.")
        if phase not in {"screening", "finalist"}:
            raise ValueError("phase must be screening or finalist.")
        normalized_sql = self.validator.validate_read_only(sql).normalized_sql
        session = self.tuning_sessions.get_session(session_id)
        candidate = self.tuning_sessions.get_candidate(candidate_id)
        case = self.performance_store.get_performance_case(session.performance_case_id)
        if candidate.session_id != session_id:
            raise ValueError("Candidate does not belong to the tuning session.")
        if candidate.is_terminal:
            raise ValueError("Index candidate already has a terminal benchmark result.")
        if candidate.rewrite_fingerprint != self._sql_fingerprint(normalized_sql):
            raise ValueError("Index candidate SQL fingerprint does not match.")
        if case.query_fingerprint != self._sql_fingerprint(normalized_sql):
            raise ValueError("Index benchmark SQL does not match the performance case.")
        if case.database_fingerprint != database_fingerprint(database_name):
            raise PermissionError("Tuning session belongs to another database.")
        if not key_columns:
            raise ValueError("key_columns must contain at least one column.")
        if phase == "screening" and candidate.screen_runs:
            raise ValueError("Index candidate screening has already been measured.")
        if phase == "finalist" and candidate.finalist_runs:
            raise ValueError("Index candidate finalist validation has already been measured.")

        cases = self.performance_workflows._normalize_parameter_cases(parameter_cases)
        if len(cases) > session.parameter_case_limit:
            raise ValueError(
                f"parameter_cases exceeds the session limit of {session.parameter_case_limit}."
            )
        supplied_case_fingerprints = tuple(
            fingerprint_json(parameter_case["values"])
            for parameter_case in cases
        )
        if supplied_case_fingerprints != case.parameter_case_fingerprints:
            raise ValueError(
                "Index benchmark parameter cases do not match the performance case."
            )
        bound_cases = [
            {
                "name": parameter_case["name"],
                "values_fingerprint": fingerprint_json(parameter_case["values"]),
                "sql": await self.performance_workflows._bind_case(
                    database_name,
                    normalized_sql,
                    parameter_case,
                ),
            }
            for parameter_case in cases
        ]

        runs = (
            session.screen_runs_per_candidate
            if phase == "screening"
            else session.finalist_runs_per_candidate
        )
        requested_executions = len(cases) * (runs * 2 + 2)
        if not policy.can_benchmark(requested_executions):
            raise PermissionError("Database policy does not permit this benchmark count.")
        already_executed = sum(
            item.executions for item in self.tuning_sessions.list_candidates(session_id)
        )
        if already_executed + requested_executions > session.execution_limit:
            raise ValueError("The tuning session execution budget would be exceeded.")
        if phase == "screening":
            self.tuning_sessions.start_screening(session_id)
        else:
            self.tuning_sessions.mark_candidate_finalist(session_id, candidate_id)

        schema = self._validate_plain_identifier(schema_name, "schema_name")
        table = self._validate_plain_identifier(table_name, "table_name")
        object_fingerprint = fingerprint_json(
            {
                "schema": schema.casefold(),
                "table": table.casefold(),
                "key_columns": key_columns,
                "include_columns": include_columns or [],
            }
        )
        index_name = f"{TEST_INDEX_PREFIX}{object_fingerprint[:16]}"
        lease_id = (
            f"lease-{fingerprint_json({'session': session_id, 'candidate': candidate_id, 'key': idempotency_key})[:32]}"
        )
        try:
            existing_lease = self.performance_store.get_index_lease(lease_id)
        except KeyError:
            existing_lease = None
        if existing_lease is not None:
            if existing_lease["status"] in {
                "pending_create",
                "active",
                "cleanup_pending",
                "cleanup_required",
            }:
                raise PermissionError(
                    "This index benchmark lease still requires cleanup or reconciliation."
                )
            raise ValueError(
                "This index benchmark idempotency key was already used; inspect the existing evidence."
            )
        open_leases = self.performance_store.list_open_index_leases(
            database_fingerprint=case.database_fingerprint
        )
        conflicting = [
            lease
            for lease in open_leases
            if lease["lease_id"] != lease_id
        ]
        if conflicting:
            raise PermissionError(
                "An earlier temporary-index lease still requires cleanup before another test."
            )
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=lease_minutes)
        lease = self.performance_store.create_index_lease(
            lease_id=lease_id,
            database_fingerprint=case.database_fingerprint or "",
            session_id=session_id,
            candidate_id=candidate_id,
            index_name=index_name,
            object_fingerprint=object_fingerprint,
            expires_at_utc=expires_at.isoformat(),
            metadata={
                "phase": phase,
                "sql_persisted": False,
                "target_schema": schema,
                "target_table": table,
            },
        )

        measured_executions = 0
        measurements: list[dict[str, Any]] = [
            {
                "parameter_case": bound_case["name"],
                "sql": bound_case["sql"],
                "baseline_samples": [],
                "candidate_samples": [],
                "baseline_plan": {},
                "candidate_plan": {},
            }
            for bound_case in bound_cases
        ]
        cleanup_error: str | None = None
        created = False
        create_attempted = False
        benchmark_error: str | None = None
        equivalence: list[dict[str, Any]] = []
        try:
            for measurement in measurements:
                for _ in range(runs):
                    measured_executions += 1
                    profiled = await self.plans.profile_query(
                        database_name,
                        str(measurement["sql"]),
                    )
                    if profiled.user_query_executions != 1:
                        raise RuntimeError(
                            "Profiled samples must execute the user query exactly once."
                        )
                    measurement["baseline_samples"].append(
                        extract_profile_metrics(profiled)
                    )
                    measurement["baseline_plan"] = profiled.plan.summary
            create_attempted = True
            await self._create_test_index(
                database_name,
                schema,
                table,
                index_name,
                key_columns,
                include_columns,
                online,
                dry_run=False,
                workflow_managed=True,
            )
            created = True
            lease = self.performance_store.update_index_lease(
                lease_id,
                status="active",
                metadata={"created_at_utc": datetime.now(timezone.utc).isoformat()},
            )
            for measurement in measurements:
                for _ in range(runs):
                    measured_executions += 1
                    profiled = await self.plans.profile_query(
                        database_name,
                        str(measurement["sql"]),
                    )
                    if profiled.user_query_executions != 1:
                        raise RuntimeError(
                            "Profiled samples must execute the user query exactly once."
                        )
                    measurement["candidate_samples"].append(
                        extract_profile_metrics(profiled)
                    )
                    measurement["candidate_plan"] = profiled.plan.summary
            for measurement in measurements:
                measured_executions += 2
                comparison = dict(
                    await self.performance_workflows.compare_query_results(
                        database_name,
                        str(measurement["sql"]),
                        str(measurement["sql"]),
                        compare_order=True,
                    )
                )
                if int(comparison.get("executions", 0)) != 2:
                    raise RuntimeError(
                        "Snapshot comparisons must reserve exactly two query executions."
                    )
                comparison["parameter_case"] = measurement["parameter_case"]
                equivalence.append(comparison)
        except asyncio.CancelledError:
            benchmark_error = "timeout"
        except Exception as exc:
            benchmark_error = type(exc).__name__
        finally:
            if created or create_attempted:
                try:
                    self.performance_store.update_index_lease(
                        lease_id,
                        status="cleanup_pending",
                    )
                    if created or await self._temporary_index_exists(
                        database_name,
                        schema,
                        table,
                        index_name,
                    ):
                        await self._drop_test_index(
                            database_name,
                            schema,
                            table,
                            index_name,
                            dry_run=False,
                            workflow_managed=True,
                        )
                    lease = self.performance_store.update_index_lease(
                        lease_id,
                        status="cleaned",
                        metadata={"cleaned_at_utc": datetime.now(timezone.utc).isoformat()},
                    )
                except Exception as exc:
                    cleanup_error = type(exc).__name__
                    lease = self.performance_store.update_index_lease(
                        lease_id,
                        status="cleanup_required",
                        metadata={"cleanup_error_type": cleanup_error},
                    )
            elif benchmark_error:
                lease = self.performance_store.update_index_lease(
                    lease_id,
                    status="create_failed",
                    metadata={"failure_type": benchmark_error},
                )

        parameter_results = [
            {
                "parameter_case": measurement["parameter_case"],
                "baseline": aggregate_samples(measurement["baseline_samples"]),
                "candidate": aggregate_samples(measurement["candidate_samples"]),
                "plan_delta": compare_plan_summaries_payload(
                    measurement["baseline_plan"],
                    measurement["candidate_plan"],
                ),
            }
            for measurement in measurements
        ]
        if cleanup_error:
            state, reason = (
                "cleanup_required",
                "benchmark completed but the temporary index could not be removed",
            )
        elif benchmark_error or not all(
            measurement["candidate_samples"] for measurement in measurements
        ):
            state, reason = (
                "inconclusive",
                "index candidate failed; reject this candidate and continue",
            )
        else:
            objective = str(case.metadata.get("objective") or "elapsed_time")
            state, reason = classify_benchmark(
                parameter_results,
                equivalence,
                objective=objective,
            )

        candidate_plans = [
            measurement["candidate_plan"]
            for measurement in measurements
            if measurement["candidate_plan"]
        ]

        evidence = self.performance_store.create_evidence(
            EvidenceEnvelopeV1(
                source="azure-sql-mcp",
                kind=f"index_{phase}",
                query_fingerprint=case.query_fingerprint,
                database_fingerprint=case.database_fingerprint,
                parameters_fingerprint=fingerprint_json(
                    [
                        {
                            "name": bound_case["name"],
                            "values_fingerprint": bound_case["values_fingerprint"],
                        }
                        for bound_case in bound_cases
                    ]
                ),
                plan_fingerprint=(
                    fingerprint_json(candidate_plans) if candidate_plans else None
                ),
                observed_execution_count=measured_executions,
                metrics={
                    "classification": state,
                    "objective": str(case.metadata.get("objective") or "elapsed_time"),
                    "parameter_results": parameter_results,
                },
                metadata={
                    "session_id": session_id,
                    "candidate_id": candidate_id,
                    "lease_id": lease_id,
                    "lease_status": lease["status"],
                    "equivalence": equivalence,
                },
            ),
            idempotency_key=(f"{idempotency_key}:evidence" if idempotency_key else None),
        )
        durable_state = state
        if phase == "screening" and state == "improved":
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="screening",
                screen_runs=runs,
                parameter_cases=len(cases),
                executions=measured_executions,
                evidence_ids=(evidence.evidence_id,),
                idempotency_key=idempotency_key,
            )
            durable_state = updated.state
        else:
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state=state,
                screen_runs=runs if phase == "screening" else 0,
                finalist_runs=runs if phase == "finalist" else 0,
                parameter_cases=len(cases),
                executions=measured_executions,
                evidence_ids=(evidence.evidence_id,),
                failure_code=(
                    state
                    if state in {"inconclusive", "cleanup_required", "equivalence_failed"}
                    else None
                ),
                idempotency_key=idempotency_key,
            )
        return {
            "session_id": session_id,
            "candidate_id": candidate_id,
            "classification": state,
            "objective": str(case.metadata.get("objective") or "elapsed_time"),
            "durable_state": durable_state,
            "reason": reason,
            "phase": phase,
            "executions": measured_executions,
            "metrics": parameter_results[0],
            "parameter_results": parameter_results,
            "equivalence": equivalence,
            "lease": lease,
            "index_ddl": self._test_index_ddl(
                schema,
                table,
                index_name,
                key_columns,
                include_columns,
                online,
            ),
            "rollback_ddl": f"DROP INDEX [{index_name}] ON [{schema}].[{table}]",
            "session_continues": True,
        }

    @staticmethod
    def _sql_fingerprint(sql: str) -> str:
        normalized = " ".join(sql.split()).casefold()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def _test_index_ddl(
        self,
        schema: str,
        table: str,
        index_name: str,
        key_columns: list[str],
        include_columns: list[str] | None,
        online: bool,
    ) -> str:
        key_parts: list[str] = []
        for raw_column in key_columns:
            name, _, direction = raw_column.strip().partition(" ")
            name = self._validate_plain_identifier(name, "key column")
            direction = direction.strip().upper()
            if direction and direction not in {"ASC", "DESC"}:
                raise ValueError("Index key direction must be ASC or DESC.")
            key_parts.append(f"[{name}] {direction}".strip())
        includes = [
            f"[{self._validate_plain_identifier(column, 'include column')}]"
            for column in (include_columns or [])
        ]
        ddl = (
            f"CREATE NONCLUSTERED INDEX [{index_name}] ON [{schema}].[{table}] "
            f"({', '.join(key_parts)})"
        )
        if includes:
            ddl += f" INCLUDE ({', '.join(includes)})"
        if online:
            ddl += " WITH (ONLINE = ON)"
        return ddl

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
        dry_run: bool = True,
    ) -> dict[str, Any]:
        # Fetch at most row_limit + 1 rows per result set to prevent OOM
        fetch_limit = self.config.row_limit + 1
        action = AdminAction(
            tool_name="execute_tsql_unrestricted",
            database_name=database_name,
            action_type="query",
            sql=sql,
        )
        payload = await self.admin_policy.execute(
            action,
            self.executor,
            dry_run=dry_run,
            max_rows=fetch_limit,
        )
        for result_set in payload.get("result_sets", []):
            if isinstance(result_set, dict):
                self._truncate_rows(result_set)
        return payload

    async def _rebuild_index(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        index_name: str,
        operation: str,
        online: bool,
        dry_run: bool = True,
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
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="rebuild_index",
                database_name=database_name,
                action_type="maintenance",
                sql=sql,
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "operation": op,
            "index": qualified_index,
            "online": online if op == "REBUILD" else None,
        })
        return payload

    async def _update_statistics(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        stat_name: str | None,
        sample_percent: int | None,
        dry_run: bool = True,
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
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="update_statistics",
                database_name=database_name,
                action_type="maintenance",
                sql=sql,
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "table": qualified_table,
            "statistic": stat_name or "(all)",
            "sample_percent": sample_percent,
        })
        return payload

    async def _force_query_plan(
        self,
        database_name: str,
        query_id: int,
        plan_id: int,
        unforce: bool,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if not dry_run:
            raise PermissionError(
                "Direct force/unforce is preview-only; use the prepared plan-action workflow."
            )
        if query_id <= 0:
            raise ValueError("query_id must be greater than 0.")
        if plan_id <= 0:
            raise ValueError("plan_id must be greater than 0.")
        if unforce:
            sql = "EXEC sp_query_store_unforce_plan @query_id = ?, @plan_id = ?"
            rollback_sql = (
                "EXEC sp_query_store_force_plan "
                f"@query_id = {int(query_id)}, @plan_id = {int(plan_id)}"
            )
        else:
            sql = "EXEC sp_query_store_force_plan @query_id = ?, @plan_id = ?"
            rollback_sql = (
                "EXEC sp_query_store_unforce_plan "
                f"@query_id = {int(query_id)}, @plan_id = {int(plan_id)}"
            )
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="force_query_plan",
                database_name=database_name,
                action_type="query_store",
                sql=sql,
                params=(int(query_id), int(plan_id)),
                rollback_sql=rollback_sql,
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "query_id": query_id,
            "plan_id": plan_id,
            "action": "unforced" if unforce else "forced",
        })
        return payload

    async def _set_query_store_hints(
        self,
        database_name: str,
        query_id: int,
        query_hints: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if not dry_run:
            raise PermissionError(
                "Direct Query Store hint changes are preview-only; use the prepared workflow."
            )
        if query_id <= 0:
            raise ValueError("query_id must be greater than 0.")
        validated_hints = validate_query_hints(query_hints)
        # The driver binds str parameters as varchar, but the proc requires
        # nvarchar — route the hints through an nvarchar variable.
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="set_query_store_hints",
                database_name=database_name,
                action_type="query_store",
                sql=(
                    "DECLARE @hints nvarchar(max) = ?;\n"
                    "EXEC sys.sp_query_store_set_hints @query_id = ?, @query_hints = @hints"
                ),
                params=(validated_hints, int(query_id)),
                rollback_sql=(
                    f"EXEC sys.sp_query_store_clear_hints @query_id = {int(query_id)}"
                ),
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "query_id": query_id,
            "query_hints": validated_hints,
            "action": "hints_set",
        })
        return payload

    async def _clear_query_store_hints(
        self,
        database_name: str,
        query_id: int,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if not dry_run:
            raise PermissionError(
                "Direct Query Store hint changes are preview-only; use the prepared workflow."
            )
        if query_id <= 0:
            raise ValueError("query_id must be greater than 0.")
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="clear_query_store_hints",
                database_name=database_name,
                action_type="query_store",
                sql="EXEC sys.sp_query_store_clear_hints @query_id = ?",
                params=(int(query_id),),
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "query_id": query_id,
            "action": "hints_cleared",
        })
        return payload

    async def _create_test_index(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        index_name: str,
        key_columns: list[str],
        include_columns: list[str] | None,
        online: bool,
        dry_run: bool = True,
        workflow_managed: bool = False,
    ) -> dict[str, Any]:
        if not dry_run and not workflow_managed:
            raise PermissionError(
                "Direct test-index creation is preview-only; use benchmark_index_candidate."
            )
        if not dry_run:
            self._require_test_index_database(database_name)
        index = self._validate_test_index_name(index_name)
        schema = self._validate_plain_identifier(schema_name, "schema_name")
        table = self._validate_plain_identifier(table_name, "table_name")
        if not key_columns:
            raise ValueError("key_columns must contain at least one column.")

        key_parts = []
        for column in key_columns:
            name, _, direction = column.strip().partition(" ")
            name = self._validate_plain_identifier(name, "key column")
            direction = direction.strip().upper()
            if direction and direction not in ("ASC", "DESC"):
                raise ValueError(f"key column direction must be ASC or DESC, got {direction!r}.")
            key_parts.append(f"[{name}] {direction}".strip())

        include_parts = [
            f"[{self._validate_plain_identifier(column, 'include column')}]"
            for column in (include_columns or [])
        ]

        sql = (
            f"CREATE NONCLUSTERED INDEX [{index}] ON [{schema}].[{table}] "
            f"({', '.join(key_parts)})"
        )
        if include_parts:
            sql += f" INCLUDE ({', '.join(include_parts)})"
        if online:
            sql += " WITH (ONLINE = ON)"
        rollback_sql = f"DROP INDEX [{index}] ON [{schema}].[{table}]"

        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="create_test_index",
                database_name=database_name,
                action_type="test_index",
                sql=sql,
                rollback_sql=rollback_sql,
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "index": f"[{schema}].[{table}].[{index}]",
            "key_columns": key_columns,
            "include_columns": include_columns or [],
            "online": online,
            "action": "test_index_created",
            "note": "Disposable test index — drop it with drop_test_index after measuring.",
        })
        return payload

    async def _drop_test_index(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        index_name: str,
        dry_run: bool = True,
        workflow_managed: bool = False,
    ) -> dict[str, Any]:
        if not dry_run and not workflow_managed:
            raise PermissionError(
                "Direct test-index removal is preview-only; use benchmark_index_candidate."
            )
        if not dry_run:
            self._require_test_index_database(database_name)
        index = self._validate_test_index_name(index_name)
        schema = self._validate_plain_identifier(schema_name, "schema_name")
        table = self._validate_plain_identifier(table_name, "table_name")
        sql = f"DROP INDEX [{index}] ON [{schema}].[{table}]"
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="drop_test_index",
                database_name=database_name,
                action_type="test_index",
                sql=sql,
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "index": f"[{schema}].[{table}].[{index}]",
            "action": "test_index_dropped",
        })
        return payload

    def _require_test_index_database(self, database_name: str) -> None:
        """Require the named sandbox profile and local database policy."""
        if self.config.profile != McpProfile.SANDBOX:
            raise PermissionError("Temporary index DDL requires the sandbox MCP profile.")
        policy = self.database_policy.require(database_name)
        if not policy.allow_test_indexes:
            raise PermissionError("Database policy does not permit temporary indexes.")
        if policy.environment.casefold() in {"production", "prod", "live"}:
            raise PermissionError("Temporary indexes are prohibited in production.")

    @staticmethod
    def _validate_plain_identifier(identifier: str, label: str) -> str:
        value = (identifier or "").strip()
        if not _PLAIN_IDENTIFIER.match(value):
            raise ValueError(
                f"{label} must be a plain identifier (letters, digits, underscores); got {value!r}."
            )
        return value

    @classmethod
    def _validate_test_index_name(cls, index_name: str) -> str:
        value = cls._validate_plain_identifier(index_name, "index_name")
        if not value.upper().startswith(TEST_INDEX_PREFIX.upper()):
            raise ValueError(
                f"index_name must start with the test prefix {TEST_INDEX_PREFIX!r} — "
                "these tools only manage disposable test indexes."
            )
        return value

    async def _kill_session(
        self,
        database_name: str,
        session_id: int,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if session_id <= 50:
            raise ValueError(
                f"Cannot kill session {session_id}: system sessions (SPID <= 50) are protected."
            )
        # KILL cannot be parameterized — use string formatting with validated int
        sql = f"KILL {int(session_id)}"
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="kill_session",
                database_name=database_name,
                action_type="session",
                sql=sql,
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "session_id": session_id,
            "note": "Session termination initiated. Rollback of active transactions may take time.",
        })
        if payload["status"] == "completed":
            payload["status"] = "kill_issued"
        return payload

    async def _tune_query(
        self,
        database_name: str,
        sql: str,
        analyze: bool,
        auto_bind_params: bool,
        include_raw_xml: bool,
        window_minutes: int,
        parameter_values: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")
        effective_sql, binding_info = await self._prepare_query(
            database_name,
            sql,
            auto_bind_params,
            parameter_values,
        )
        performance_case = self.performance_workflows.start_case(
            database_name,
            effective_sql,
            metadata={
                "objective": "elapsed_time",
                "compatibility_tool": "tune_query",
                "raw_sql_persisted": False,
            },
        )
        evidence = await self._collect_performance_evidence(
            database_name,
            performance_case.case_id,
            effective_sql,
            window_minutes,
            analyze,
            None,
        )
        session = self.performance_workflows.start_session(
            performance_case.case_id,
        )
        return {
            "database_name": database_name,
            "performance_case_id": performance_case.case_id,
            "tuning_session_id": session["session_id"],
            "query_hash": self._sql_fingerprint(effective_sql),
            "analyze": analyze,
            "parameter_binding": binding_info,
            "evidence": evidence,
            "session": session,
            "next_step": (
                "Produce concrete rewrites, add each with add_tuning_candidate, then "
                "benchmark them. Missing plan evidence does not block static rewrites."
            ),
            "raw_xml_included": False,
            "raw_xml_note": (
                "Durable tuning state stores plan fingerprints and summaries, not raw XML."
                if include_raw_xml
                else None
            ),
            "scripts": {
                "rollback": "-- No database changes were applied by tune_query.",
                "deploy": "-- Deploy only the finalist that passes equivalence and benchmark gates.",
            },
        }

    async def _benchmark_query_rewrite(
        self,
        database_name: str,
        baseline_sql: str,
        rewrite_sql: str,
        analyze: bool,
        auto_bind_params: bool,
        include_raw_xml: bool,
        runs: int = 1,
        parameter_values: dict[str, Any] | None = None,
        compare_order: bool = True,
    ) -> dict[str, Any]:
        if not analyze:
            raise ValueError("benchmark_query_rewrite requires analyze=true for measured results.")
        if not 1 <= runs <= 3:
            raise ValueError("Compatibility screening runs must be between 1 and 3.")
        baseline_effective, baseline_binding = await self._prepare_query(
            database_name,
            baseline_sql,
            auto_bind_params,
            parameter_values,
        )
        rewrite_effective, rewrite_binding = await self._prepare_query(
            database_name,
            rewrite_sql,
            auto_bind_params,
            parameter_values,
        )
        performance_case = self.performance_workflows.start_case(
            database_name,
            baseline_effective,
            metadata={
                "objective": "elapsed_time",
                "compatibility_tool": "benchmark_query_rewrite",
            },
        )
        session = self.performance_workflows.start_session(performance_case.case_id)
        candidate = self.performance_workflows.add_candidate(
            session["session_id"],
            rewrite_effective,
            strategy="rewrite",
        )
        benchmark = await self.performance_workflows.benchmark_candidate(
            session["session_id"],
            candidate["candidate_id"],
            database_name,
            baseline_effective,
            rewrite_effective,
            phase="screening",
            compare_order=compare_order,
            runs_override=runs,
        )
        benchmark.update({
            "database_name": database_name,
            "analyze": True,
            "performance_case_id": performance_case.case_id,
            "tuning_session_id": session["session_id"],
            "winning_sql": rewrite_sql if benchmark.get("classification") == "improved" else None,
            "parameter_binding": {
                "baseline": baseline_binding,
                "rewrite": rewrite_binding,
            },
            "raw_xml_included": False,
            "raw_xml_note": (
                "Raw XML is omitted from durable workflow responses."
                if include_raw_xml
                else None
            ),
            "scripts": {
                "rollback": "-- No database changes were applied by benchmark_query_rewrite.",
                "deploy": "-- Deploy the accepted rewrite in application code after full equivalence proof.",
            },
        })
        return benchmark

    async def _plan_health_review(
        self,
        database_name: str,
        *,
        window_minutes: int,
        top_n: int,
    ) -> dict[str, Any]:
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")
        if top_n <= 0:
            raise ValueError("top_n must be greater than 0.")

        enforcement = await self.plan_enforcement.review(
            database_name,
            window_minutes=window_minutes,
            top_n=top_n,
        )
        sniffing = await self.query_regression.detect_parameter_sniffing(
            database_name,
            window_minutes=window_minutes,
            top_n=top_n,
        )
        return {
            "database_name": database_name,
            "mode": "review",
            "window_minutes": window_minutes,
            "top_n": top_n,
            "plan_enforcement": enforcement,
            "parameter_sniffing": {
                "affected_query_count": sniffing.get("affected_query_count", 0),
                "queries": sniffing.get("queries", [])[:top_n],
            },
        }

    async def _metadata_inventory(
        self,
        database_name: str,
        table_references: list[dict[str, str | None]],
    ) -> dict[str, Any]:
        details = []
        unresolved = []
        schema_name_set: set[str] = set()
        for ref in table_references:
            schema_name = ref.get("schema")
            if isinstance(schema_name, str) and schema_name:
                schema_name_set.add(schema_name)
        schema_names = sorted(schema_name_set)
        table_stats: dict[str, Any] = {}
        for schema_name in schema_names:
            table_stats[schema_name] = await self.introspection.get_table_stats(
                database_name,
                schema_name,
            )

        for ref in table_references:
            schema_name = ref.get("schema")
            table_name = ref.get("table")
            if not schema_name or not table_name:
                unresolved.append(
                    {
                        "reference": ref,
                        "reason": "schema is not explicit; use get_object_details after resolving it",
                    }
                )
                continue
            try:
                details.append(
                    await self.introspection.get_object_details(
                        database_name,
                        schema_name,
                        table_name,
                        "table",
                    )
                )
            except Exception as exc:
                unresolved.append(
                    {
                        "reference": ref,
                        "reason": sanitize_error_message(str(exc)),
                    }
                )

        return {
            "table_references": table_references,
            "object_details": details,
            "table_stats_by_schema": table_stats,
            "unresolved": unresolved,
        }

    async def _query_store_history_for_plan(
        self,
        database_name: str,
        original_sql: str,
        plan: dict[str, Any],
        window_minutes: int,
    ) -> dict[str, Any]:
        """Query Store history for tune_query evidence.

        Prefer query_hash from the captured plan — it survives parameter
        renaming (@CustomerId vs @P1). Fall back to text matching with the
        ORIGINAL sql; the auto-bound DECLARE batch never matches stored text.
        """
        summary = plan.get("summary")
        statements = summary.get("statements") if isinstance(summary, dict) else None
        query_hash = None
        if isinstance(statements, list) and statements:
            first = statements[0]
            if isinstance(first, dict):
                query_hash = first.get("query_hash")

        if isinstance(query_hash, str) and query_hash.lower().startswith("0x"):
            history = await self.query_store.get_query_history_by_hash(
                database_name,
                query_hash,
                window_minutes=window_minutes,
                limit=10,
            )
            if history.get("matches"):
                history["matched_by"] = "query_hash"
                return history

        history = await self.query_store.get_query_history_by_text(
            database_name,
            original_sql,
            window_minutes=window_minutes,
            limit=10,
        )
        history["matched_by"] = "text"
        return history

    async def _prepare_query(
        self,
        database_name: str,
        sql: str,
        auto_bind_params: bool,
        parameter_values: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any] | None]:
        if not auto_bind_params and not parameter_values:
            return sql, None
        binding_info = await self.param_binding.bind_parameters(
            database_name, sql, parameter_values=parameter_values,
        )
        if binding_info.get("parameters"):
            return str(binding_info["bound_sql"]), {
                "original_sql": binding_info["original_sql"],
                "parameters": binding_info["parameters"],
            }
        return sql, None

    @staticmethod
    async def _optional_payload(callback) -> dict[str, Any]:
        try:
            return {"ok": True, "data": await callback()}
        except Exception as exc:
            return {"ok": False, "error": sanitize_error_message(str(exc))}

    async def _optional_evidence(
        self,
        callback: Callable[[], Awaitable[Any]],
    ) -> Any:
        try:
            return await callback()
        except Exception as exc:
            return {
                "available": False,
                "error": sanitize_error_message(str(exc)),
            }

    @staticmethod
    def _sql_hash(sql: str) -> str:
        normalized = " ".join(sql.split()).lower()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]

    async def _explain_query(
        self,
        database_name: str,
        sql: str,
        analyze: bool,
        hypothetical_indexes: list[dict[str, Any]] | None = None,
        auto_bind_params: bool = False,
        include_raw_xml: bool = False,
        parameter_values: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if hypothetical_indexes:
            raise ValueError(
                "Hypothetical index analysis is disabled on explain_query for safety. "
                "Use analyze_query_indexes/analyze_workload_indexes for read-only index insights."
            )
        effective_sql = sql
        binding_info: dict[str, Any] | None = None
        if auto_bind_params or parameter_values:
            binding_info = await self.param_binding.bind_parameters(
                database_name, sql, parameter_values=parameter_values,
            )
            if binding_info.get("parameters"):
                effective_sql = binding_info["bound_sql"]

        artifact = await self.plans.explain_query(
            database_name,
            effective_sql,
            analyze,
            hypothetical_indexes=hypothetical_indexes,
        )
        result = self._artifact_to_dict(artifact, include_raw_xml=include_raw_xml)
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
        parameter_values: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        effective_queries = queries
        if auto_bind_params or parameter_values:
            normalized_values: dict[str, Any] = {}
            for raw_name, value in (parameter_values or {}).items():
                name = str(raw_name).lstrip("@").strip().casefold()
                if not name:
                    raise ValueError("explicit parameter name must not be empty")
                if name in normalized_values:
                    raise ValueError(f"duplicate explicit parameter name: {name}")
                normalized_values[name] = value
            detected_by_query = [
                {name.casefold() for name in detect_parameters(query)}
                for query in queries
            ]
            detected_any = set().union(*detected_by_query) if detected_by_query else set()
            unknown_names = sorted(set(normalized_values) - detected_any)
            if unknown_names:
                raise ValueError(
                    "explicit value supplied for unknown parameter(s): "
                    + ", ".join(unknown_names)
                )
            bound_queries: list[str] = []
            for query, detected_names in zip(queries, detected_by_query, strict=True):
                query_values = {
                    name: value
                    for name, value in normalized_values.items()
                    if name in detected_names
                }
                binding = await self.param_binding.bind_parameters(
                    database_name, query, parameter_values=query_values,
                )
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

    def _artifact_to_dict(
        self,
        artifact: ExplainPlanArtifact,
        *,
        include_raw_xml: bool = False,
    ) -> dict[str, Any]:
        raw_xml_resource = self.artifacts.put_text(
            kind="showplan-xml",
            text=artifact.raw_xml,
            mime_type="application/xml",
            metadata={
                "database_name": artifact.database_name,
                "analyze": artifact.analyze,
            },
        )
        return artifact.as_dict(
            include_raw_xml=include_raw_xml,
            raw_xml_resource=raw_xml_resource,
        )

    def _truncate_rows(self, payload: dict[str, Any]) -> dict[str, Any]:
        rows = payload.get("rows")
        if isinstance(rows, list):
            # Fetches use row_limit + 1 to detect truncation; row_count must
            # describe the rows actually returned, not the sentinel row.
            payload["truncated"] = len(rows) > self.config.row_limit
            payload["rows"] = rows[: self.config.row_limit]
            payload["row_count"] = len(payload["rows"])
        return payload

    def _format_response(self, payload: Any) -> ResponseType:
        if isinstance(payload, dict):
            return payload
        return {"result": payload}

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

    async def _cleanup_expired_index_leases(self) -> dict[str, int]:
        """Retry expired temporary-index cleanup before a sandbox accepts work."""

        summary = {"examined": 0, "cleaned": 0, "cleanup_required": 0}
        if self.config.profile != McpProfile.SANDBOX:
            return summary
        now = datetime.now(timezone.utc)
        databases_by_fingerprint = {
            database_fingerprint(name): name for name in self.config.allowed_databases
        }
        for lease in self.performance_store.list_open_index_leases():
            try:
                expires_at = datetime.fromisoformat(
                    str(lease["expires_at_utc"]).replace("Z", "+00:00")
                )
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
            except (TypeError, ValueError):
                expires_at = now
            if expires_at.astimezone(timezone.utc) > now:
                continue
            summary["examined"] += 1
            database_name = databases_by_fingerprint.get(lease["database_fingerprint"])
            metadata = lease.get("metadata") or {}
            schema_name = metadata.get("target_schema")
            table_name = metadata.get("target_table")
            try:
                if (
                    not database_name
                    or not isinstance(schema_name, str)
                    or not isinstance(table_name, str)
                ):
                    raise ValueError("expired lease has no resolvable cleanup target")
                self._require_test_index_database(database_name)
                self.performance_store.update_index_lease(
                    lease["lease_id"],
                    status="cleanup_pending",
                    metadata={"recovery_attempted_at_utc": now.isoformat()},
                )
                if await self._temporary_index_exists(
                    database_name,
                    schema_name,
                    table_name,
                    lease["index_name"],
                ):
                    await self._drop_test_index(
                        database_name,
                        schema_name,
                        table_name,
                        lease["index_name"],
                        dry_run=False,
                        workflow_managed=True,
                    )
                self.performance_store.update_index_lease(
                    lease["lease_id"],
                    status="cleaned",
                    metadata={
                        "recovered_at_startup": True,
                        "cleaned_at_utc": datetime.now(timezone.utc).isoformat(),
                    },
                )
                summary["cleaned"] += 1
            except Exception as exc:
                self.performance_store.update_index_lease(
                    lease["lease_id"],
                    status="cleanup_required",
                    metadata={
                        "recovery_error_type": type(exc).__name__,
                        "recovery_attempted_at_utc": now.isoformat(),
                    },
                )
                summary["cleanup_required"] += 1
        return summary

    async def _temporary_index_exists(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        index_name: str,
    ) -> bool:
        rows = await self.executor.fetch_all(
            database_name,
            """
            SELECT TOP (1) 1 AS found
            FROM sys.indexes AS i
            INNER JOIN sys.tables AS t ON t.object_id = i.object_id
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE i.name = ? AND s.name = ? AND t.name = ?
            """,
            (index_name, schema_name, table_name),
        )
        return bool(rows)

    async def run(self) -> None:
        self.mcp.settings.host = self.config.transport.host
        self.mcp.settings.port = self.config.transport.port

        try:
            cleanup = await self._cleanup_expired_index_leases()
            if cleanup["examined"]:
                logger.info("Reconciled expired temporary-index leases.", extra=cleanup)
            if self.config.transport.mode == TransportMode.STDIO:
                await self.mcp.run_stdio_async()
            elif self.config.transport.mode == TransportMode.SSE:
                await self.mcp.run_sse_async()
            else:
                await self.mcp.run_streamable_http_async()
        finally:
            try:
                self.performance_store.close()
            except Exception as exc:
                logger.error(
                    "Failed to close performance state store during shutdown.",
                    extra={
                        "error_type": type(exc).__name__,
                        "error": sanitize_error_message(str(exc)),
                    },
                )
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
