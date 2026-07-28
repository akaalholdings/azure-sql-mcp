from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import re
import time
import uuid
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from typing import Any
from typing import NoReturn

from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
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
from .candidate_lineage import combined_parent_id
from .candidate_lineage import validate_combined_parent
from .config import AccessMode
from .config import McpProfile
from .config import ServerConfig
from .config import TransportMode
from .config import load_server_config
from .connection import AzureSqlExecutor
from .connection_pool import ConnectionPool
from .database_policy import load_database_policy_or_deny
from .diagnostics import DiagnosticQueryService
from .equivalence_contract import analyze_equivalence_preflight
from .health import HealthService
from .index_optimizer import IndexCandidate
from .index_optimizer import IndexOptimizer
from .index_optimizer import build_index_candidate_statement
from .index_optimizer import expected_index_definition_matches
from .index_optimizer import verify_plan_uses_index
from .index_metadata import collect_existing_indexes
from .index_metadata import existing_index_covers_candidate
from .index_recommendations import IndexRecommendationService
from .introspection import IntrospectionService
from .lock_diagnostics import LockDiagnosticsService
from .logging_config import configure_logging
from .observability import sanitize_error_message
from .param_binding import detect_parameters
from .param_binding import ParameterExecutionContract
from .param_binding import ParameterBindingService
from .performance_contracts import EvidenceEnvelopeV1
from .performance_store import ContractNotFoundError
from .performance_store import IdempotencyConflictError
from .performance_store import PerformanceStore
from .performance_workflows import PerformanceWorkflowService
from .performance_workflows import aggregate_samples
from .performance_workflows import classify_benchmark
from .performance_workflows import compare_plan_summaries_payload
from .performance_workflows import database_fingerprint
from .performance_workflows import database_fingerprint_matches
from .performance_workflows import extract_profile_metrics
from .performance_workflows import fingerprint_json
from .performance_workflows import fingerprint_text
from .performance_workflows import fingerprint_text_matches
from .performance_workflows import parameter_case_fingerprint
from .performance_workflows import profile_result_fingerprint
from .plan_action_service import PlanActionService
from .plan_cache import PlanCacheService
from .plan_enforcement import PlanEnforcementService
from .plans import PlansService
from .platform_capabilities import PlatformCapabilitiesService
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
from .view_workflows import PreparedViewChange
from .view_workflows import ViewChangeRequest
from .view_workflows import ViewWorkflowService
from .view_workflows import prepared_view_change_from_state
from .view_workflows import prepared_view_change_state
from .view_workflows import view_apply_receipt
from .view_workflows import view_snapshot_from_receipt

ResponseType = dict[str, Any]

logger = logging.getLogger(__name__)

# Disposable test indexes (create_test_index / drop_test_index) are namespaced by this
# prefix; the drop tool refuses anything outside it so real indexes are untouchable.
TEST_INDEX_PREFIX = "IX_Testing_"
TEST_INDEX_OWNER_PROPERTY = "AzureSqlMcpLeaseOwner"
TEST_INDEX_DEFINITION_PROPERTY = "AzureSqlMcpDefinitionFingerprint"
_PLAIN_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_INDEX_OWNER_PROOF = re.compile(r"^[A-Za-z0-9_.:-]{16,200}$")
_IDEMPOTENCY_DIGEST_PATTERN = re.compile(r"^idempotency-v1:[0-9a-f]{64}$")
_PENDING_INDEX_OWNERSHIP_SQL = """
SELECT
    i.index_id,
    CONVERT(nvarchar(4000), owner_marker.value) AS owner_marker,
    CONVERT(nvarchar(4000), definition_marker.value) AS definition_marker
FROM sys.indexes AS i WITH (UPDLOCK, HOLDLOCK)
INNER JOIN sys.tables AS t WITH (UPDLOCK, HOLDLOCK)
    ON t.object_id = i.object_id
INNER JOIN sys.schemas AS s WITH (UPDLOCK, HOLDLOCK)
    ON s.schema_id = t.schema_id
LEFT JOIN sys.extended_properties AS owner_marker WITH (UPDLOCK, HOLDLOCK)
    ON owner_marker.class = 7
    AND owner_marker.major_id = i.object_id
    AND owner_marker.minor_id = i.index_id
    AND owner_marker.name = ?
LEFT JOIN sys.extended_properties AS definition_marker WITH (UPDLOCK, HOLDLOCK)
    ON definition_marker.class = 7
    AND definition_marker.major_id = i.object_id
    AND definition_marker.minor_id = i.index_id
    AND definition_marker.name = ?
WHERE s.name = ?
  AND t.name = ?
  AND i.name = ?
  AND i.index_id > 0
"""
_SESSION_WORKFLOW_TOOLS = frozenset(
    {
        "benchmark_tuning_candidate",
        "benchmark_index_candidate",
        "benchmark_query_rewrite",
        "apply_prepared_view_change",
        "verify_view_change",
        "rollback_view_change",
    }
)
_EVIDENCE_WORKFLOW_TOOLS = frozenset(
    {"collect_performance_evidence", "tune_query"}
)


def _view_change_request_fingerprint(
    database_fingerprint_value: str,
    request: ViewChangeRequest,
) -> str:
    return fingerprint_json(
        {
            "database_fingerprint": database_fingerprint_value,
            "database_name": request.database_name,
            "schema_name": request.schema_name,
            "view_name": request.view_name,
            "definition": request.definition,
            "operation": request.operation.casefold(),
            "reviewed_intent": request.reviewed_intent,
            "idempotency_key": _view_idempotency_identity(request.idempotency_key),
            "indexed_view": request.indexed_view,
            "schema_bound": request.schema_bound,
        }
    )


def _view_idempotency_identity(value: str | None) -> str | None:
    if value is None:
        return None
    if _IDEMPOTENCY_DIGEST_PATTERN.fullmatch(value):
        return value
    digest = hashlib.sha256(
        f"idempotency-v1:view-change.intent:{value}".encode("utf-8")
    ).hexdigest()
    return f"idempotency-v1:{digest}"


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
        self._startup_timestamp = datetime.now(timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        try:
            self._package_version = package_version("azure-sql-mcp")
        except PackageNotFoundError:  # pragma: no cover - source-only execution
            self._package_version = "unknown"
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
        self.platform_capabilities = PlatformCapabilitiesService(executor)
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
            collector_timeout_seconds=config.query_timeout_seconds + 5,
            comparison_row_limit=config.comparison_row_limit,
            server_name=config.server,
            allow_legacy_state=bool(config.legacy_state_server_binding),
        )
        self.plan_actions = PlanActionService(
            config=config,
            executor=executor,
            admin_policy=self.admin_policy,
            database_policy=self.database_policy,
            store=self.performance_store,
        )
        self.view_workflows = ViewWorkflowService(
            executor,
            self.database_policy,
            self.admin_policy,
        )
        self._prepared_view_changes: dict[str, PreparedViewChange] = {}
        self.capabilities = CapabilityService(
            executor,
            self.query_store,
            self.plans,
            self.recommendations,
        )

        self._register_tools()
        self._prune_disabled_tools()
        self._enforce_strict_tool_argument_models()
        register_resources(self.mcp, self.config, self.introspection, self.artifacts)
        register_prompts(self.mcp, self.config)

    def _prune_disabled_tools(self) -> None:
        """Remove tools that are not in the configured tool_groups."""
        registered = [t.name for t in self.mcp._tool_manager.list_tools()]
        for name in registered:
            if not self.config.is_tool_enabled(name):
                self.mcp.remove_tool(name)
                logger.debug("Pruned tool '%s' (not in active tool groups)", name)

    def _enforce_strict_tool_argument_models(self) -> None:
        """Make FastMCP's generated argument models reject unknown fields."""

        for tool in self.mcp._tool_manager.list_tools():
            argument_model = tool.fn_metadata.arg_model
            argument_model.model_config["extra"] = "forbid"
            argument_model.model_rebuild(force=True)
            tool.parameters = argument_model.model_json_schema()

    def _register_tools(self) -> None:
        @self.mcp.tool(
            description=(
                "Return DB-free runtime identity, registered-tool capabilities, and "
                "contract fingerprints for this MCP server process."
            ),
            annotations=ToolAnnotations(
                title="Check Runtime Status",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def check_runtime_status() -> ResponseType:
            return self._format_response(self._runtime_status())

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
                self._check_database_capabilities,
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
                    "{'CustomerId': 42}). Values are passed as driver parameters "
                    "and are not returned in the response."
                ),
            ),
            parameter_types: dict[str, str] | None = Field(
                default=None,
                description=(
                    "Optional declared SQL types keyed by parameter name, for example "
                    "{'CustomerId': 'bigint'}. Supply exact types for faithful compilation."
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
                    parameter_types,
                ),
            )

        @self.mcp.tool(
            description=(
                "Compatibility initializer only: open a performance case/session and "
                "collect evidence. It does not generate or benchmark a rewrite and must "
                "not be treated as a completed optimization. Named optimizer profiles "
                "use the explicit case/session workflow instead."
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
            parameter_types: dict[str, str] | None = Field(
                default=None,
                description="Optional exact SQL types for the supplied parameter values.",
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
                    parameter_types,
                ),
            )

        @self.mcp.tool(
            description=(
                "Benchmark a baseline query against a proposed rewrite using the same "
                "typed read-only execution path, interleaved actual-plan samples, and "
                "complete bounded snapshot equivalence."
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
            parameter_types: dict[str, str] | None = Field(
                default=None,
                description="Optional exact SQL types shared by baseline and rewrite.",
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
                    "Screening executions per side (2-3). Metrics "
                    "become per-run medians with min/max spread, so a single lucky run "
                    "cannot masquerade as a win. The workflow does not clear Azure SQL caches."
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
                    parameter_types,
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
            parameter_case: dict[str, Any] | None = Field(
                default=None,
                description=(
                    "For active evidence on parameterized SQL, one named case containing "
                    "an exact values object and exact SQL types object."
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
                    parameter_case,
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
                "Start a durable iterative tuning session. Defaults are 10 candidates, "
                "80 executions, and 20 minutes; explicit multi-hour budgets are accepted "
                "when the local database policy permits them."
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
            max_candidates: int = Field(
                default=10,
                ge=1,
                description="Maximum candidate experiments for this session.",
            ),
            execution_limit: int = Field(
                default=80,
                ge=1,
                description="Maximum measured query executions across the session.",
            ),
            time_limit_minutes: int = Field(
                default=20,
                ge=1,
                description="Wall-clock session budget in minutes; may span hours.",
            ),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "start_tuning_session",
                database_name,
                lambda db: self._start_tuning_session(
                    db,
                    case_id,
                    max_candidates,
                    execution_limit,
                    time_limit_minutes,
                    idempotency_key,
                ),
            )

        @self.mcp.tool(
            description=(
                "Resume a durable tuning session with its complete redacted leaderboard, "
                "evidence, events, and remaining budgets. Raw SQL is never persisted."
            ),
            annotations=ToolAnnotations(
                title="Get Tuning Session",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def get_tuning_session(
            session_id: str = Field(description="Tuning session identifier."),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "get_tuning_session",
                database_name,
                lambda db: self._get_tuning_session(db, session_id),
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
            artifact_ref: str | None = Field(
                default=None,
                description=(
                    "Durable artifact reference. Combined candidates require "
                    "candidate:<proven-parent-id>."
                ),
            ),
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
            runs: int | None = Field(
                default=None,
                ge=2,
                le=5,
                description=(
                    "Optional paired runs per parameter case. Screening is capped by "
                    "the session screening limit; finalists by the finalist limit."
                ),
            ),
            prove_equivalence: bool | None = Field(
                default=None,
                description=(
                    "Defaults false for broad screening and true for finalists. "
                    "Finalists cannot disable full snapshot equivalence."
                ),
            ),
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
                    runs_override=runs,
                    prove_equivalence=prove_equivalence,
                    idempotency_key=idempotency_key,
                ),
                deadline_provider=lambda: self.tuning_sessions.get_session(
                    session_id
                ).deadline_at_utc,
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
            filter_definition: str | None = Field(
                default=None,
                description="Optional filtered-index predicate; no SQL statements or comments.",
            ),
            is_unique: bool = Field(default=False),
            parameter_cases: list[dict[str, Any]] | None = Field(
                default=None,
                description="The same named parameter cases recorded on the performance case.",
            ),
            phase: str = Field(default="screening", description="screening or finalist"),
            online: bool = Field(default=True),
            compare_order: bool = Field(
                default=True,
                description="Preserve ordered-result semantics during A-B-A stability checks.",
            ),
            lease_minutes: int = Field(default=30, ge=5, le=120),
            idempotency_key: str = Field(
                description="Caller-generated key used to fence retries and cleanup."
            ),
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
                    filter_definition,
                    is_unique,
                    phase,
                    online,
                    compare_order,
                    lease_minutes,
                    idempotency_key,
                    parameter_cases,
                ),
                deadline_provider=lambda: self.tuning_sessions.get_session(
                    session_id
                ).deadline_at_utc,
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
            parameter_case: dict[str, Any] | None = Field(
                default=None,
                description=(
                    "Required for parameterized SQL: one named case with exact values "
                    "and declared SQL types for every parameter."
                ),
            ),
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
                    parameter_case=parameter_case,
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
            description=(
                "Prepare and preview a reversible CREATE VIEW or ALTER VIEW change. "
                "Optimizer preparations are process-local previews; sandbox "
                "preparations become restart-safe only with explicit raw-SQL state opt-in."
            ),
            annotations=ToolAnnotations(
                title="Prepare View Change",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def prepare_view_change(
            schema_name: str = Field(description="Target schema."),
            view_name: str = Field(description="Target view."),
            definition: str = Field(
                description="Complete SELECT-shaped view body, without CREATE/ALTER VIEW."
            ),
            operation: str = Field(default="auto", description="auto, create, or alter"),
            schema_bound: bool = Field(default=False),
            indexed_view: bool = Field(default=False),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "prepare_view_change",
                database_name,
                lambda db: self._prepare_view_change(
                    db,
                    schema_name,
                    view_name,
                    definition,
                    operation,
                    schema_bound,
                    indexed_view,
                    idempotency_key,
                ),
            )

        @self.mcp.tool(
            description=(
                "Apply one durable prepared view change in the sandbox profile after "
                "explicit review, raw-SQL state opt-in, and non-production policy approval."
            ),
            annotations=ToolAnnotations(
                title="Apply Prepared View Change",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def apply_prepared_view_change(
            change_id: str = Field(description="Prepared view change identifier."),
            reviewed_intent: bool = Field(
                description="Must be true to confirm this exact prepared definition."
            ),
            idempotency_key: str = Field(
                description="Caller-stable key for this exact apply."
            ),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "apply_prepared_view_change",
                database_name,
                lambda db: self._apply_prepared_view_change(
                    db,
                    change_id,
                    reviewed_intent,
                    idempotency_key,
                ),
            )

        @self.mcp.tool(
            description=(
                "Verify that a prepared view change has the expected definition "
                "and dependency set."
            ),
            annotations=ToolAnnotations(
                title="Verify View Change",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def verify_view_change(
            change_id: str = Field(description="Prepared view change identifier."),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "verify_view_change",
                database_name,
                lambda db: self._verify_view_change(db, change_id),
            )

        @self.mcp.tool(
            description=(
                "Restore the exact prior definition for a workflow-owned view change. "
                "Rollback is fenced against unrelated current definitions."
            ),
            annotations=ToolAnnotations(
                title="Rollback View Change",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def rollback_view_change(
            change_id: str = Field(description="Prepared view change identifier."),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "rollback_view_change",
                database_name,
                lambda db: self._rollback_view_change(db, change_id),
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
            parameter_types: dict[str, str] | None = Field(
                default=None,
                description=(
                    "Optional exact SQL types keyed by parameter name and shared "
                    "across the supplied queries."
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
                    parameter_values,
                    parameter_types,
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
                self._raise_tool_error(
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

    def _runtime_status(self) -> dict[str, Any]:
        tools = sorted(
            self.mcp._tool_manager.list_tools(),
            key=lambda tool: tool.name,
        )
        tool_names = [tool.name for tool in tools]
        schema_material = [
            {
                "name": tool.name,
                "input_schema": tool.parameters,
                "output_schema": tool.output_schema,
            }
            for tool in tools
        ]
        schema_fingerprint = hashlib.sha256(
            json.dumps(
                schema_material,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        contracts = {
            "strict_arguments": True,
            "mcp_errors": True,
        }
        config_fingerprint = self.config.sanitized_config_fingerprint()
        runtime_fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "startup_timestamp": self._startup_timestamp,
                    "package_version": self._package_version,
                    "tool_schema_fingerprint": schema_fingerprint,
                    "sanitized_config_fingerprint": config_fingerprint,
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return {
            "startup_timestamp": self._startup_timestamp,
            "package_version": self._package_version,
            "profile": (
                self.config.profile.value if self.config.profile is not None else None
            ),
            "transport": self.config.transport.mode.value,
            "tool_count": len(tool_names),
            "tool_names": tool_names,
            "contracts": contracts,
            "strict_argument_models": contracts["strict_arguments"],
            "mcp_tool_errors": contracts["mcp_errors"],
            "runtime_fingerprint": runtime_fingerprint,
            "tool_schema_fingerprint": schema_fingerprint,
            "sanitized_config_fingerprint": config_fingerprint,
        }

    async def _bind_performance_parameters(
        self,
        database_name: str,
        sql: str,
        parameter_case: dict[str, Any] | Any,
    ) -> ParameterExecutionContract:
        case = (
            parameter_case
            if isinstance(parameter_case, dict)
            else dict(parameter_case)
        )
        values = dict(case.get("values") or {})
        types = dict(case.get("types") or {})
        detected = {name.casefold() for name in detect_parameters(sql)}
        value_names = {str(name).lstrip("@").casefold() for name in values}
        type_names = {str(name).lstrip("@").casefold() for name in types}
        if value_names != detected or type_names != detected:
            raise ValueError(
                "Every query parameter requires one explicit value and SQL type "
                "in each benchmark case."
            )
        bucket = await self.param_binding.build_parameter_bucket(
            database_name,
            sql,
            parameter_values=values,
            parameter_types=types,
            bucket_id=str(case.get("name") or "default"),
            label=str(case.get("name") or "default"),
            provenance="explicit_performance_case",
        )
        return self.param_binding.build_execution_contract(
            sql,
            bucket,
            provenance="typed_sp_executesql",
        )

    async def _check_database_capabilities(
        self,
        database_name: str,
    ) -> dict[str, Any]:
        checks, platform = await asyncio.gather(
            self.capabilities.check(database_name),
            self._optional_payload(
                lambda: self.platform_capabilities.get_summary(database_name)
            ),
        )
        policy = self.database_policy.policy_for(database_name)
        return {
            **checks,
            "azure_sql_database": platform,
            "mcp_contract": {
                "contract_version": 1,
                "performance_tuning": 1,
                "durable_view_change": 1,
                "prepared_plan_action": 1,
            },
            "local_tuning_policy": {
                "configured": policy.configured,
                "environment": policy.environment,
                "allow_read": policy.allow_read,
                "allow_benchmark": policy.allow_benchmark,
                "allow_test_indexes": policy.allow_test_indexes,
                "allow_view_apply": policy.allow_view_apply,
                "allow_plan_apply": policy.allow_plan_apply,
                "max_benchmark_executions": policy.max_benchmark_executions,
                "max_tuning_candidates": policy.max_tuning_candidates,
                "max_tuning_session_executions": (
                    policy.max_tuning_session_executions
                ),
                "max_tuning_session_minutes": policy.max_tuning_session_minutes,
            },
        }

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
            metadata={
                "objective": objective,
                "raw_sql_persisted": False,
                "equivalence_preflight": analyze_equivalence_preflight(sql).as_dict(),
            },
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
        parameter_case: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        execution_contract: ParameterExecutionContract | None = None
        if execute_query and parameter_case is not None:
            execution_contract = await self._bind_performance_parameters(
                database_name,
                sql,
                parameter_case,
            )
        collectors: dict[str, Callable[[], Awaitable[Any]]] = {
            "resource_limits": lambda: self.resource_governance.get_resource_limits(
                database_name
            ),
            "resource_history": lambda: self.resource_governance.get_resource_stats_history(
                database_name,
                window_minutes,
            ),
            "query_store_status": lambda: self.query_store.get_status(database_name),
            "query_store_history": lambda: self._collect_query_store_evidence(
                database_name,
                sql,
                window_minutes,
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
            execution_contract=execution_contract,
            idempotency_key=idempotency_key,
        )

    async def _collect_query_store_evidence(
        self,
        database_name: str,
        sql: str,
        window_minutes: int,
    ) -> dict[str, Any]:
        identity = await self.query_store.resolve_query_identity(database_name, sql)
        if identity.get("status") != "resolved":
            return {
                "available": False,
                "complete": False,
                "status": "inconclusive",
                "reason": "exact Query Store identity was not uniquely resolved",
                "identity": identity,
                "fuzzy_match_used": False,
            }
        query_id = int(identity["query_id"])
        history, parameter_buckets = await asyncio.gather(
            self.query_store.get_query_history_by_id(
                database_name,
                query_id,
                window_minutes=window_minutes,
                limit=20,
            ),
            self.query_store.get_parameter_runtime_buckets(
                database_name,
                query_id=query_id,
                window_minutes=window_minutes,
                limit=50,
            ),
        )

        def coverage(
            result: Any,
            rows_key: str,
            limit: int,
        ) -> dict[str, Any]:
            rows = result.get(rows_key) if isinstance(result, Mapping) else None
            row_count = len(rows) if isinstance(rows, list) else 0
            available = row_count > 0 and (
                not isinstance(result, Mapping) or result.get("available") is not False
            )
            complete = available and row_count < limit
            if isinstance(result, Mapping):
                complete = complete and result.get("complete") is not False
                complete = complete and result.get("truncated") is not True
            return {
                "available": available,
                "complete": complete,
                "row_count": row_count,
                "limit": limit,
            }

        history_coverage = coverage(history, "matches", 20)
        bucket_coverage = coverage(parameter_buckets, "buckets", 50)
        parameter_buckets_required = bool(detect_parameters(sql))
        bucket_coverage["required"] = parameter_buckets_required
        evidence_available = bool(
            history_coverage["available"]
            or (
                parameter_buckets_required
                and bucket_coverage["available"]
            )
        )
        evidence_complete = bool(
            history_coverage["complete"]
            and (
                not parameter_buckets_required
                or bucket_coverage["complete"]
            )
        )
        if evidence_complete:
            status = "resolved"
            reason = None
        elif evidence_available:
            status = "partial"
            reason = (
                "Query Store history or required parameter-bucket evidence is incomplete"
            )
        else:
            status = "inconclusive"
            reason = (
                "Query Store history is empty"
                if not parameter_buckets_required
                else "Query Store history and parameter-bucket evidence are empty"
            )
        return {
            "available": evidence_available,
            "complete": evidence_complete,
            "status": status,
            "reason": reason,
            "identity": {
                "query_id": query_id,
                "query_hash": identity.get("query_hash"),
                "identity_kind": "query_id",
            },
            "history": history,
            "parameter_buckets": parameter_buckets,
            "coverage": {
                "history": history_coverage,
                "parameter_buckets": bucket_coverage,
            },
            "fuzzy_match_used": False,
        }

    async def _get_performance_case(
        self,
        database_name: str,
        case_id: str,
    ) -> dict[str, Any]:
        payload = self.performance_workflows.get_case(case_id)
        case = payload["case"]
        if not database_fingerprint_matches(
            str(case.get("database_fingerprint") or ""),
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise PermissionError("Performance case belongs to another database.")
        return payload

    async def _start_tuning_session(
        self,
        database_name: str,
        case_id: str,
        max_candidates: int,
        execution_limit: int,
        time_limit_minutes: int,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        case = self.performance_store.get_performance_case(case_id)
        if not database_fingerprint_matches(
            case.database_fingerprint or "",
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise PermissionError("Performance case belongs to another database.")
        return self.performance_workflows.start_session(
            case_id,
            database_name,
            max_candidates=max_candidates,
            execution_limit=execution_limit,
            time_limit_minutes=time_limit_minutes,
            idempotency_key=idempotency_key,
        )

    async def _get_tuning_session(
        self,
        database_name: str,
        session_id: str,
    ) -> dict[str, Any]:
        payload = self.performance_workflows.get_session(session_id)
        session = payload["session"]
        if not database_fingerprint_matches(
            str(
                session.get("metadata", {}).get("database_fingerprint")
                or ""
            ),
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise PermissionError("Tuning session belongs to another database.")
        return payload

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
        if not database_fingerprint_matches(
            case.database_fingerprint or "",
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
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
        if not database_fingerprint_matches(
            case.database_fingerprint or "",
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
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

    async def _prepare_view_change(
        self,
        database_name: str,
        schema_name: str,
        view_name: str,
        definition: str,
        operation: str,
        schema_bound: bool,
        indexed_view: bool,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        request = ViewChangeRequest(
            database_name=database_name,
            schema_name=schema_name,
            view_name=view_name,
            definition=definition,
            operation=operation,
            idempotency_key=idempotency_key,
            indexed_view=indexed_view,
            schema_bound=schema_bound,
        )
        schema_name = request.schema_name
        view_name = request.view_name
        intent: dict[str, Any] | None = None
        durable = (
            self.config.profile == McpProfile.SANDBOX
            and self.config.persist_view_sql_state
            and self.config.performance_state_dir != ":memory:"
        )
        if durable:
            if not idempotency_key or not idempotency_key.strip():
                raise ValueError(
                    "Durable sandbox view preparation requires an idempotency key."
                )
            normalized_idempotency_key = _view_idempotency_identity(idempotency_key)
            assert normalized_idempotency_key is not None
            request = replace(
                request,
                idempotency_key=normalized_idempotency_key,
            )
            database_fp = database_fingerprint(database_name, self.config.server)
            intent = self.performance_store.get_idempotent_view_change_intent(
                database_fingerprint=database_fp,
                idempotency_key=normalized_idempotency_key,
            )
            if intent is not None:
                prepared = prepared_view_change_from_state(intent["payload"])
                if _view_change_request_fingerprint(database_fp, prepared.request) != (
                    _view_change_request_fingerprint(database_fp, request)
                ):
                    raise IdempotencyConflictError(
                        "View idempotency key was reused for a different request."
                    )
                change_id = str(intent["change_id"])
            else:
                prepared = await self.view_workflows.prepare_view_change(request)
                change_id = "view-" + fingerprint_json(
                    {
                        "database": database_fp,
                        "schema": schema_name,
                        "view": view_name,
                        "operation": prepared.operation,
                        "target": prepared.target_fingerprint,
                        "prior": prepared.prior.definition_fingerprint,
                        "idempotency_key": normalized_idempotency_key,
                    }
                )[:32]
                state = prepared_view_change_state(prepared)
                intent = self.performance_store.create_view_change_intent(
                    change_id=change_id,
                    database_fingerprint=database_fp,
                    request_fingerprint=fingerprint_json(state),
                    payload=state,
                    raw_sql_persistence_authorized=True,
                )
                prepared = prepared_view_change_from_state(intent["payload"])
        else:
            prepared = await self.view_workflows.prepare_view_change(request)
            change_id = "view-" + fingerprint_json(
                {
                    "database": database_fingerprint(
                        database_name,
                        self.config.server,
                    ),
                    "schema": schema_name,
                    "view": view_name,
                    "operation": prepared.operation,
                    "target": prepared.target_fingerprint,
                    "prior": prepared.prior.definition_fingerprint,
                    "idempotency_key": idempotency_key,
                }
            )[:32]
        existing = self._prepared_view_changes.get(change_id)
        if existing is not None and (
            existing.target_fingerprint != prepared.target_fingerprint
            or existing.prior.definition_fingerprint
            != prepared.prior.definition_fingerprint
        ):
            raise ValueError("Prepared view change identifier conflict.")
        self._prepared_view_changes[change_id] = prepared
        preview = await self.view_workflows.preview_view_change(prepared)
        return {
            "change_id": change_id,
            "process_local": not durable,
            "durable": durable,
            "restart_requires_reprepare": not durable,
            "raw_view_sql_persisted": durable,
            "intent_status": intent["status"] if intent is not None else None,
            **preview,
        }

    def _prepared_view_change(
        self,
        database_name: str,
        change_id: str,
    ) -> PreparedViewChange:
        if self.config.persist_view_sql_state:
            try:
                prepared, _intent = self._durable_view_change(
                    database_name,
                    change_id,
                )
            except ContractNotFoundError:
                pass
            else:
                return prepared
        prepared = self._prepared_view_changes.get(change_id)
        if prepared is None:
            raise ValueError(
                "Unknown view change; prepare it in this MCP process."
            )
        if prepared.request.database_name.casefold() != database_name.casefold():
            raise PermissionError("Prepared view change belongs to another database.")
        return prepared

    def _durable_view_change(
        self,
        database_name: str,
        change_id: str,
    ) -> tuple[PreparedViewChange, dict[str, Any]]:
        intent = self.performance_store.get_view_change_intent(change_id)
        if not database_fingerprint_matches(
            str(intent["database_fingerprint"]),
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise PermissionError("Prepared view change belongs to another database.")
        if fingerprint_json(intent["payload"]) != intent["request_fingerprint"]:
            raise ValueError("Durable view change payload fingerprint does not match.")
        prepared = prepared_view_change_from_state(intent["payload"])
        if prepared.request.database_name.casefold() != database_name.casefold():
            raise PermissionError("Prepared view change belongs to another database.")
        self._prepared_view_changes[change_id] = prepared
        return prepared, intent

    def _require_durable_view_change(
        self,
        database_name: str,
        change_id: str,
    ) -> tuple[PreparedViewChange, dict[str, Any]]:
        if (
            not self.config.persist_view_sql_state
            or self.config.performance_state_dir == ":memory:"
        ):
            raise PermissionError(
                "View apply and rollback require "
                "AZURE_SQL_PERSIST_VIEW_SQL_STATE=true so exact rollback survives restart."
            )
        return self._durable_view_change(database_name, change_id)

    async def _reconcile_durable_view_change(
        self,
        change_id: str,
        prepared: PreparedViewChange,
        intent: dict[str, Any],
    ) -> dict[str, Any]:
        verification = await self.view_workflows.verify_view_change(prepared)
        if intent.get("receipt") is None:
            if getattr(prepared, "operation", None) == "noop":
                prior_restored, current = await self.view_workflows.prior_state_restored(
                    prepared
                )
                if prior_restored:
                    updated = self.performance_store.update_view_change_intent(
                        change_id,
                        status="already_applied",
                        expected_version=int(intent["version"]),
                        raw_sql_persistence_authorized=True,
                    )
                    return {
                        "change_id": change_id,
                        "status": "already_applied",
                        "intent_status": updated["status"],
                        "workflow_applied": False,
                        "prior_state_restored": True,
                        "current": current.as_dict(),
                        "verification": verification.as_dict(),
                        "reason": (
                            "the unchanged prior snapshot proves this interrupted "
                            "no-op performed no mutation"
                        ),
                    }
            if verification.verified and verification.workflow_commit_proven:
                receipt = view_apply_receipt(verification.actual)
                self.view_workflows.register_apply_receipt(
                    prepared,
                    verification.actual,
                )
                updated = self.performance_store.update_view_change_intent(
                    change_id,
                    status="applied",
                    expected_version=int(intent["version"]),
                    raw_sql_persistence_authorized=True,
                    receipt=receipt,
                )
                return {
                    "change_id": change_id,
                    "status": "reconciled_applied",
                    "intent_status": updated["status"],
                    "workflow_applied": True,
                    "verification": verification.as_dict(),
                    "reason": (
                        "the exact workflow marker and target state prove that "
                        "the interrupted view change committed"
                    ),
                }
            updated = self.performance_store.update_view_change_intent(
                change_id,
                status="hold",
                expected_version=int(intent["version"]),
                raw_sql_persistence_authorized=True,
            )
            return {
                "change_id": change_id,
                "status": "hold",
                "intent_status": updated["status"],
                "workflow_applied": False,
                "verification": verification.as_dict(),
                "reason": (
                    "the target definition may have been applied externally, but "
                    "there is no durable dispatch/commit receipt for this workflow; "
                    "rollback ownership was not adopted"
                ),
            }
        if verification.verified:
            receipt_snapshot = view_snapshot_from_receipt(intent["receipt"])
            self.view_workflows.register_apply_receipt(prepared, receipt_snapshot)
            updated = self.performance_store.update_view_change_intent(
                change_id,
                status="applied",
                expected_version=int(intent["version"]),
                raw_sql_persistence_authorized=True,
            )
            return {
                "change_id": change_id,
                "status": "reconciled_applied",
                "intent_status": updated["status"],
                "workflow_applied": True,
                "verification": verification.as_dict(),
            }
        prior_restored, current = await self.view_workflows.prior_state_restored(
            prepared
        )
        updated = self.performance_store.update_view_change_intent(
            change_id,
            status="hold",
            expected_version=int(intent["version"]),
            raw_sql_persistence_authorized=True,
        )
        return {
            "change_id": change_id,
            "status": "hold",
            "intent_status": updated["status"],
            "workflow_applied": intent.get("receipt") is not None,
            "prior_state_restored": prior_restored,
            "current": current.as_dict(),
            "verification": verification.as_dict(),
            "reason": (
                "interrupted view apply is not at the prepared target; "
                "the DDL was not replayed"
            ),
        }

    async def _apply_prepared_view_change(
        self,
        database_name: str,
        change_id: str,
        reviewed_intent: bool,
        idempotency_key: str,
    ) -> dict[str, Any]:
        if self.config.profile != McpProfile.SANDBOX:
            raise PermissionError("View apply requires the sandbox MCP profile.")
        if not reviewed_intent:
            raise PermissionError("reviewed_intent must be true for view apply.")
        prepared, intent = self._require_durable_view_change(
            database_name,
            change_id,
        )
        if intent["status"] in {"applying", "hold"}:
            return await self._reconcile_durable_view_change(
                change_id,
                prepared,
                intent,
            )
        if intent["status"] == "applied":
            receipt = intent.get("receipt")
            if receipt is None:
                raise ValueError("Durable applied view intent has no receipt.")
            self.view_workflows.register_apply_receipt(
                prepared,
                view_snapshot_from_receipt(receipt),
            )
            verification = await self.view_workflows.verify_view_change(prepared)
            if not verification.verified:
                intent = self.performance_store.update_view_change_intent(
                    change_id,
                    status="hold",
                    expected_version=int(intent["version"]),
                    raw_sql_persistence_authorized=True,
                )
                return {
                    "change_id": change_id,
                    "status": "hold",
                    "intent_status": intent["status"],
                    "workflow_applied": True,
                    "verification": verification.as_dict(),
                    "reason": (
                        "the workflow-owned view no longer verifies; "
                        "the durable rollback receipt was retained"
                    ),
                }
            return {
                "change_id": change_id,
                "status": "already_applied",
                "intent_status": "applied",
                "workflow_applied": True,
                "verification": verification.as_dict(),
            }
        if intent["status"] == "already_applied":
            verification = await self.view_workflows.verify_view_change(prepared)
            if not verification.verified:
                intent = self.performance_store.update_view_change_intent(
                    change_id,
                    status="hold",
                    expected_version=int(intent["version"]),
                    raw_sql_persistence_authorized=True,
                )
                return {
                    "change_id": change_id,
                    "status": "hold",
                    "intent_status": intent["status"],
                    "workflow_applied": False,
                    "verification": verification.as_dict(),
                    "reason": "the externally applied view no longer verifies",
                }
            return {
                "change_id": change_id,
                "status": "already_applied",
                "intent_status": "already_applied",
                "workflow_applied": False,
                "verification": verification.as_dict(),
            }
        if intent["status"] == "rolled_back":
            raise ValueError("Rolled-back view intent cannot be applied again.")
        if intent["status"] != "prepared":
            raise ValueError(f"View intent is not applyable from {intent['status']!r}.")
        normalized_idempotency_key = _view_idempotency_identity(idempotency_key)
        existing_key = prepared.request.idempotency_key
        if existing_key and existing_key != normalized_idempotency_key:
            raise ValueError(
                "Apply idempotency key does not match the prepared change."
            )
        prepared = replace(
            prepared,
            request=replace(
                prepared.request,
                reviewed_intent=True,
                idempotency_key=normalized_idempotency_key,
            ),
        )
        self._prepared_view_changes[change_id] = prepared
        intent = self.performance_store.update_view_change_intent(
            change_id,
            status="applying",
            expected_version=int(intent["version"]),
            raw_sql_persistence_authorized=True,
        )
        result = await self.view_workflows.apply_prepared_view_change(prepared)
        verification = await self.view_workflows.verify_view_change(prepared)
        workflow_applied = result.get("workflow_applied") is True
        if workflow_applied:
            receipt_payload = result.get("apply_receipt")
            receipt_snapshot = view_snapshot_from_receipt(receipt_payload)
            self.view_workflows.register_apply_receipt(
                prepared,
                receipt_snapshot,
            )
            receipt = view_apply_receipt(receipt_snapshot)
            status = "applied" if verification.verified else "hold"
        else:
            receipt = None
            status = "already_applied" if verification.verified else "hold"
        intent = self.performance_store.update_view_change_intent(
            change_id,
            status=status,
            expected_version=int(intent["version"]),
            raw_sql_persistence_authorized=True,
            receipt=receipt,
        )
        response = {
            "change_id": change_id,
            "intent_status": intent["status"],
            **result,
        }
        if status == "hold":
            response.update(
                {
                    "status": "hold",
                    "verification": verification.as_dict(),
                    "reason": (
                        "post-apply verification failed; durable ownership and "
                        "the exact rollback receipt were retained"
                        if workflow_applied
                        else "the target could not be verified"
                    ),
                }
            )
        return response

    async def _verify_view_change(
        self,
        database_name: str,
        change_id: str,
    ) -> dict[str, Any]:
        prepared = self._prepared_view_change(database_name, change_id)
        try:
            durable_prepared, intent = self._durable_view_change(
                database_name,
                change_id,
            )
        except ContractNotFoundError:
            intent = None
        else:
            prepared = durable_prepared
            if intent["status"] in {"applying", "hold"}:
                return await self._reconcile_durable_view_change(
                    change_id,
                    prepared,
                    intent,
                )
            if intent["status"] == "applied" and intent.get("receipt") is not None:
                self.view_workflows.register_apply_receipt(
                    prepared,
                    view_snapshot_from_receipt(intent["receipt"]),
                )
        verification = await self.view_workflows.verify_view_change(prepared)
        return {
            "change_id": change_id,
            "intent_status": intent["status"] if intent is not None else None,
            **verification.as_dict(),
        }

    async def _rollback_view_change(
        self,
        database_name: str,
        change_id: str,
    ) -> dict[str, Any]:
        if self.config.profile != McpProfile.SANDBOX:
            raise PermissionError("View rollback requires the sandbox MCP profile.")
        prepared, intent = self._require_durable_view_change(
            database_name,
            change_id,
        )
        if not prepared.request.idempotency_key:
            raise ValueError("Apply the reviewed view change before rollback.")
        if intent["status"] == "rolled_back":
            restored, current = await self.view_workflows.prior_state_restored(prepared)
            if not restored:
                raise ValueError("Rolled-back view intent no longer matches prior state.")
            return {
                "change_id": change_id,
                "status": "already_rolled_back",
                "intent_status": "rolled_back",
                "snapshot": current.as_dict(),
            }
        if intent["status"] == "applying" or (
            intent["status"] == "hold" and intent.get("receipt") is None
        ):
            reconciliation = await self._reconcile_durable_view_change(
                change_id,
                prepared,
                intent,
            )
            if reconciliation["intent_status"] != "applied":
                raise ValueError(
                    "Interrupted view apply is not at the prepared target; rollback is held."
                )
            prepared, intent = self._durable_view_change(database_name, change_id)
        if intent["status"] not in {"applied", "hold"}:
            raise ValueError(
                "Only a workflow-applied durable view intent can be rolled back."
            )
        receipt = intent.get("receipt")
        if receipt is None:
            raise ValueError("Durable applied view intent has no receipt.")
        self.view_workflows.register_apply_receipt(
            prepared,
            view_snapshot_from_receipt(receipt),
        )
        restored, current = await self.view_workflows.prior_state_restored(prepared)
        if restored:
            updated = self.performance_store.update_view_change_intent(
                change_id,
                status="rolled_back",
                expected_version=int(intent["version"]),
                raw_sql_persistence_authorized=True,
            )
            return {
                "change_id": change_id,
                "status": "already_rolled_back",
                "intent_status": updated["status"],
                "snapshot": current.as_dict(),
            }
        result = await self.view_workflows.rollback_view_change(prepared)
        updated = self.performance_store.update_view_change_intent(
            change_id,
            status="rolled_back",
            expected_version=int(intent["version"]),
            raw_sql_persistence_authorized=True,
        )
        return {
            "change_id": change_id,
            "intent_status": updated["status"],
            **result,
        }

    def _recover_index_benchmark_result(
        self,
        *,
        session_id: str,
        candidate_id: str,
        phase: str,
        runs: int,
        parameter_case_count: int,
        reservation: Mapping[str, Any],
        reservation_owner: str,
        request_fingerprint: str,
        evidence_operation_key: str,
        benchmark_operation_key: str,
        index_ddl: str,
        rollback_ddl: str,
        index_definition_fingerprint: str,
    ) -> dict[str, Any]:
        """Finish committed index evidence without repeating DDL or query work."""

        evidence = self.performance_store.get_idempotent_evidence(
            evidence_operation_key,
            request_fingerprint=request_fingerprint,
        )
        if evidence is None:
            finalized = reservation["status"] != "reserved"
            return {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "classification": "inconclusive",
                "reason": (
                    "This idempotent index benchmark has a durable reservation "
                    "but no committed result. Reconcile its lease; no DDL or query "
                    "was rerun."
                ),
                "failure_code": (
                    "index_benchmark_request_already_finalized"
                    if finalized
                    else "index_benchmark_request_reconciliation_required"
                ),
                "executions": 0,
                "execution_reservation_id": reservation["reservation_id"],
                "reservation_status": reservation["status"],
                "session_continues": True,
            }

        metadata = dict(evidence.metadata)
        if (
            metadata.get("session_id") != session_id
            or metadata.get("candidate_id") != candidate_id
            or metadata.get("phase") != phase
            or metadata.get("execution_reservation_id")
            != reservation["reservation_id"]
        ):
            raise RuntimeError(
                "Committed index evidence does not match its execution reservation."
            )
        metrics = dict(evidence.metrics)
        state = str(metrics.get("classification") or "")
        if not state:
            raise RuntimeError("Committed index evidence has no classification.")
        performance_classification = str(
            metrics.get("performance_classification") or state
        )
        candidate_state = (
            "inconclusive" if state == "proof_contract_required" else state
        )
        measured_executions = int(evidence.observed_execution_count)
        if phase == "screening" and candidate_state in {"promising", "improved"}:
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="screening",
                screen_runs=runs,
                parameter_cases=parameter_case_count,
                executions=measured_executions,
                evidence_ids=(evidence.evidence_id,),
                idempotency_key=benchmark_operation_key,
            )
            durable_state = updated.state
        else:
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state=candidate_state,
                screen_runs=runs if phase == "screening" else 0,
                finalist_runs=runs if phase == "finalist" else 0,
                parameter_cases=parameter_case_count,
                executions=measured_executions,
                evidence_ids=(evidence.evidence_id,),
                failure_code=(
                    state
                    if candidate_state
                    in {"inconclusive", "cleanup_required", "equivalence_failed"}
                    else None
                ),
                idempotency_key=benchmark_operation_key,
            )
            durable_state = updated.state

        reservation_status = str(reservation["status"])
        if reservation_status == "reserved":
            reservation_update = (
                self.performance_store.complete_execution_attempts
                if measured_executions
                else self.performance_store.release_execution_attempts
            )
            finalized_reservation = reservation_update(
                str(reservation["reservation_id"]),
                dispatched_attempt_count=measured_executions,
                owner_reference=reservation_owner,
                expected_version=int(reservation["version"]),
            )
            reservation_status = str(finalized_reservation["status"])

        lease_id = metadata.get("lease_id")
        if not isinstance(lease_id, str) or not lease_id:
            raise RuntimeError("Committed index evidence has no lease identity.")
        lease = self.performance_store.get_index_lease(lease_id)
        public_lease = self._public_index_lease(lease)
        parameter_results = metrics.get("parameter_results")
        result_rows = parameter_results if isinstance(parameter_results, list) else []
        equivalence = metadata.get("equivalence")
        return {
            "session_id": session_id,
            "candidate_id": candidate_id,
            "classification": state,
            "performance_classification": performance_classification,
            "objective": metrics.get("objective"),
            "durable_state": durable_state,
            "reason": metadata.get("reason")
            or "Recovered committed index evidence without rerunning work.",
            "phase": phase,
            "executions": measured_executions,
            "metrics": result_rows[0] if result_rows else {},
            "parameter_results": result_rows,
            "equivalence": equivalence if isinstance(equivalence, list) else [],
            "equivalence_preflight": metadata.get("equivalence_preflight"),
            "proof_scope": metadata.get("proof_scope"),
            "lineage": metadata.get("lineage"),
            "evidence_id": evidence.evidence_id,
            "lease": public_lease,
            "index_definition_fingerprint": metadata.get(
                "index_definition_fingerprint",
                index_definition_fingerprint,
            ),
            "index_ddl": index_ddl,
            "rollback_ddl": rollback_ddl,
            "execution_reservation_id": reservation["reservation_id"],
            "reservation_status": reservation_status,
            "recovered_from_durable_evidence": True,
            "session_continues": True,
        }

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
        filter_definition: str | None,
        is_unique: bool,
        phase: str,
        online: bool,
        compare_order: bool,
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
        cleanup = await self._cleanup_expired_index_leases()
        if cleanup["cleanup_required"]:
            return {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "classification": "cleanup_required",
                "reason": (
                    "Expired temporary-index cleanup is unresolved; no new DDL "
                    "or benchmark query was dispatched."
                ),
                "failure_code": "expired_index_cleanup_required",
                "executions": 0,
                "cleanup": cleanup,
                "session_continues": False,
            }
        if phase not in {"screening", "finalist"}:
            raise ValueError("phase must be screening or finalist.")
        normalized_sql = self.validator.validate_read_only(sql).execution_sql
        session = self.tuning_sessions.get_session(session_id)
        candidate = self.tuning_sessions.get_candidate(candidate_id)
        case = self.performance_store.get_performance_case(session.performance_case_id)
        if candidate.session_id != session_id:
            raise ValueError("Candidate does not belong to the tuning session.")
        if candidate.strategy not in {"index", "combined"}:
            raise ValueError(
                "Index benchmarking requires an index or combined candidate."
            )
        if not fingerprint_text_matches(
            candidate.rewrite_fingerprint,
            normalized_sql,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise ValueError("Index candidate SQL fingerprint does not match.")
        lineage: dict[str, Any] | None = None
        if candidate.strategy == "index":
            if not fingerprint_text_matches(
                case.query_fingerprint,
                normalized_sql,
                allow_legacy=bool(self.config.legacy_state_server_binding),
            ):
                raise ValueError(
                    "Index benchmark SQL does not match the performance case."
                )
        else:
            if phase != "finalist":
                raise ValueError(
                    "Combined candidates run only as finalist marginal index experiments."
                )
            parent = self.tuning_sessions.get_candidate(combined_parent_id(candidate))
            parent_evidence = [
                self.performance_store.get_evidence(evidence_id)
                for evidence_id in parent.evidence_ids
            ]
            lineage = validate_combined_parent(
                candidate,
                parent,
                parent_evidence,
            )
            if dict(candidate.metadata.get("lineage") or {}) != lineage:
                raise ValueError(
                    "Combined candidate durable lineage does not match its proven parent."
                )
        if not database_fingerprint_matches(
            case.database_fingerprint or "",
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise PermissionError("Tuning session belongs to another database.")
        if not key_columns:
            raise ValueError("key_columns must contain at least one column.")
        equivalence_preflight = analyze_equivalence_preflight(
            normalized_sql
        ).as_dict()
        direct_snapshot_supported = bool(
            equivalence_preflight["direct_snapshot_supported"]
        )
        if not direct_snapshot_supported and phase == "finalist":
            return {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "classification": "proof_contract_required",
                "durable_state": candidate.state,
                "reason": (
                    "This MCP contract has no deterministic proof input for this "
                    "SQL shape; finalist validation cannot proceed."
                ),
                "phase": phase,
                "executions": 0,
                "equivalence": [],
                "equivalence_preflight": equivalence_preflight,
                "proof_scope": "performance_only",
                "lineage": lineage,
                "session_continues": True,
            }
        cases = self.performance_workflows._normalize_parameter_cases(parameter_cases)
        if len(cases) > session.parameter_case_limit:
            raise ValueError(
                f"parameter_cases exceeds the session limit of {session.parameter_case_limit}."
            )
        supplied_case_fingerprints = tuple(
            parameter_case_fingerprint(parameter_case) for parameter_case in cases
        )
        registered_case_fingerprints = set(case.parameter_case_fingerprints)
        if (
            len(set(supplied_case_fingerprints)) != len(supplied_case_fingerprints)
            or any(
                fingerprint not in registered_case_fingerprints
                for fingerprint in supplied_case_fingerprints
            )
        ):
            raise ValueError(
                "Index benchmark parameter cases must be an unchanged subset "
                "of the performance case."
            )
        if phase == "finalist" and set(supplied_case_fingerprints) != (
            registered_case_fingerprints
        ):
            raise ValueError(
                "Finalist index validation must cover every registered parameter case."
            )
        runs = (
            session.screen_runs_per_candidate
            if phase == "screening"
            else session.finalist_runs_per_candidate
        )
        requested_executions = len(cases) * runs * 3
        if not policy.can_benchmark(requested_executions):
            raise PermissionError("Database policy does not permit this benchmark count.")
        if not idempotency_key or not idempotency_key.strip():
            raise ValueError("Index benchmarking requires an idempotency key.")
        owner_fingerprint = fingerprint_json(
            {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "phase": phase,
                "idempotency_key": idempotency_key,
            }
        )
        execution_owner = f"index-execution-{owner_fingerprint[:32]}"
        index_operation_key = fingerprint_json(
            {"operation": "index-benchmark-result-key", "key": idempotency_key}
        )
        index_evidence_key = fingerprint_json(
            {"operation": "index-benchmark-evidence-key", "key": idempotency_key}
        )
        create_operation_key = fingerprint_json(
            {"operation": "index-benchmark-create-key", "key": idempotency_key}
        )
        cleanup_operation_key = fingerprint_json(
            {"operation": "index-benchmark-cleanup-key", "key": idempotency_key}
        )

        schema = self._validate_plain_identifier(schema_name, "schema_name")
        table = self._validate_plain_identifier(table_name, "table_name")

        request_fingerprint = fingerprint_json(
            {
                "operation": "index-benchmark-v2",
                "session_id": session_id,
                "candidate_id": candidate_id,
                "database_name": database_name,
                "sql": normalized_sql,
                "schema": schema,
                "table": table,
                "key_columns": list(key_columns),
                "include_columns": list(include_columns or ()),
                "filter_definition": filter_definition,
                "is_unique": is_unique,
                "phase": phase,
                "parameter_cases": supplied_case_fingerprints,
                "runs": runs,
                "online": online,
                "compare_order": compare_order,
                "lease_minutes": lease_minutes,
                "equivalence_preflight": equivalence_preflight,
                "lineage": lineage,
            }
        )
        self.performance_store.bind_index_benchmark_request(
            session_id,
            candidate_id,
            phase,
            request_fingerprint,
            idempotency_key=idempotency_key,
        )
        existing_reservation = (
            self.performance_store.get_idempotent_execution_reservation(
                session_id,
                candidate_id,
                request_fingerprint,
                idempotency_key=idempotency_key,
                owner_reference=execution_owner,
            )
        )
        if existing_reservation is None and candidate.is_terminal:
            if candidate.executions == 0:
                return {
                    "session_id": session_id,
                    "candidate_id": candidate_id,
                    "classification": candidate.state,
                    "durable_state": candidate.state,
                    "reason": (
                        "Replayed the durable zero-execution candidate result; "
                        "no catalog lookup, DDL, or benchmark query was dispatched."
                    ),
                    "executions": 0,
                    "session_continues": candidate.state != "cleanup_required",
                    "replayed_zero_execution_result": True,
                }
            raise ValueError("Index candidate already has a terminal benchmark result.")
        if (
            existing_reservation is None
            and not candidate.is_terminal
            and phase == "screening"
            and candidate.screen_runs
        ):
            raise ValueError("Index candidate screening has already been measured.")
        if (
            existing_reservation is None
            and not candidate.is_terminal
            and phase == "finalist"
            and candidate.finalist_runs
        ):
            raise ValueError("Index candidate finalist validation has already been measured.")

        bound_cases = [
            {
                "name": parameter_case["name"],
                "parameter_case_fingerprint": parameter_case_fingerprint(
                    parameter_case
                ),
                "weight": parameter_case["weight"],
                "execution_contract": await self.performance_workflows._bind_case(
                    database_name,
                    normalized_sql,
                    parameter_case,
                ),
            }
            for parameter_case in cases
        ]
        schema, table = await self._resolve_canonical_table_identity(
            database_name,
            schema,
            table,
        )
        provisional_index = IndexCandidate(
            schema=schema,
            table=table,
            key_columns=tuple(key_columns),
            include_columns=tuple(include_columns or ()),
            filter_definition=filter_definition,
            is_unique=is_unique,
        )
        index_name = f"{TEST_INDEX_PREFIX}{provisional_index.definition_fingerprint[:16]}"
        index_candidate = replace(provisional_index, index_name=index_name)
        object_fingerprint = index_candidate.definition_fingerprint
        index_ddl = build_index_candidate_statement(index_candidate, online=online)
        rollback_ddl = f"DROP INDEX [{index_name}] ON [{schema}].[{table}];"
        if existing_reservation is not None:
            return self._recover_index_benchmark_result(
                session_id=session_id,
                candidate_id=candidate_id,
                phase=phase,
                runs=runs,
                parameter_case_count=len(cases),
                reservation=existing_reservation,
                reservation_owner=execution_owner,
                request_fingerprint=request_fingerprint,
                evidence_operation_key=index_evidence_key,
                benchmark_operation_key=index_operation_key,
                index_ddl=index_ddl,
                rollback_ddl=rollback_ddl,
                index_definition_fingerprint=object_fingerprint,
            )

        existing_indexes = await collect_existing_indexes(self.executor, database_name)
        name_conflict = next(
            (
                existing
                for existing in existing_indexes
                if existing.schema == schema
                and existing.table == table
                and existing.name == index_name
            ),
            None,
        )
        if name_conflict is not None:
            if not candidate.is_terminal:
                if phase == "screening":
                    self.tuning_sessions.start_screening(session_id)
                else:
                    self.tuning_sessions.mark_candidate_finalist(
                        session_id,
                        candidate_id,
                    )
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="inconclusive",
                parameter_cases=len(cases),
                executions=0,
                failure_code="name_conflict",
                idempotency_key=index_operation_key,
            )
            return {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "classification": "inconclusive",
                "durable_state": updated.state,
                "reason": (
                    "The generated temporary index name already exists with an "
                    "unowned observed definition."
                ),
                "failure_code": "name_conflict",
                "existing_index": name_conflict.as_dict(),
                "executions": 0,
                "index_ddl": index_ddl,
                "rollback_ddl": rollback_ddl,
                "session_continues": True,
            }
        covering_indexes = [
            existing
            for existing in existing_indexes
            if existing_index_covers_candidate(
                existing,
                schema=schema,
                table=table,
                key_columns=key_columns,
                include_columns=include_columns or (),
                filter_definition=filter_definition,
                is_unique=is_unique,
            )
        ]
        if covering_indexes:
            if not candidate.is_terminal:
                if phase == "screening":
                    self.tuning_sessions.start_screening(session_id)
                else:
                    self.tuning_sessions.mark_candidate_finalist(
                        session_id,
                        candidate_id,
                    )
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="neutral",
                parameter_cases=len(cases),
                executions=0,
                failure_code=None,
                idempotency_key=index_operation_key,
            )
            return {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "classification": "neutral",
                "durable_state": updated.state,
                "reason": "an existing enabled index already covers this candidate",
                "existing_indexes": [index.as_dict() for index in covering_indexes],
                "executions": 0,
                "index_ddl": index_ddl,
                "rollback_ddl": rollback_ddl,
                "session_continues": True,
            }

        execution_reservation = self.performance_store.reserve_execution_attempts(
            session_id,
            candidate_id,
            requested_executions,
            request_fingerprint,
            idempotency_key=idempotency_key,
            owner_reference=execution_owner,
        )
        if (
            execution_reservation["status"] != "reserved"
            or execution_reservation.get("replayed") is True
        ):
            return self._recover_index_benchmark_result(
                session_id=session_id,
                candidate_id=candidate_id,
                phase=phase,
                runs=runs,
                parameter_case_count=len(cases),
                reservation=execution_reservation,
                reservation_owner=execution_owner,
                request_fingerprint=request_fingerprint,
                evidence_operation_key=index_evidence_key,
                benchmark_operation_key=index_operation_key,
                index_ddl=index_ddl,
                rollback_ddl=rollback_ddl,
                index_definition_fingerprint=object_fingerprint,
            )
        lease_id = (
            f"lease-{fingerprint_json({'session': session_id, 'candidate': candidate_id, 'key': idempotency_key})[:32]}"
        )
        lease_owner = f"index-lease-{owner_fingerprint[:32]}"
        lease: dict[str, Any] | None = None
        pre_dispatch_reservation_finalized = False

        def finalize_pre_dispatch_reservation() -> None:
            nonlocal pre_dispatch_reservation_finalized
            if pre_dispatch_reservation_finalized:
                return
            self.performance_store.release_execution_attempts(
                execution_reservation["reservation_id"],
                owner_reference=execution_owner,
                expected_version=execution_reservation["version"],
            )
            pre_dispatch_reservation_finalized = True

        try:
            session_remaining_seconds = self._require_tuning_session_time(session_id)
            effective_lease_minutes = lease_minutes
            if math.isfinite(session_remaining_seconds):
                effective_lease_minutes = max(
                    lease_minutes,
                    int(session_remaining_seconds / 60) + 2,
                )
            expires_at = datetime.now(timezone.utc) + timedelta(
                minutes=effective_lease_minutes
            )
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
                    "requested_lease_minutes": lease_minutes,
                    "protected_lease_minutes": effective_lease_minutes,
                    "lease_owner_fence": lease_owner,
                    "sql_persisted": False,
                    "target_schema": schema,
                    "target_table": table,
                    "key_columns": list(key_columns),
                    "include_columns": list(include_columns or ()),
                    "filter_definition": filter_definition,
                    "is_unique": is_unique,
                    "definition_fingerprint": object_fingerprint,
                    "marker_definition_fingerprint": object_fingerprint,
                    "fingerprint_provenance": "candidate_intent",
                    "create_dispatch_state": "pre_dispatch",
                    "equivalence_preflight": equivalence_preflight,
                    "lineage": lineage,
                },
                owner_reference=lease_owner,
                request_fingerprint=fingerprint_json(
                    {
                        "lease_id": lease_id,
                        "definition": object_fingerprint,
                        "key": idempotency_key,
                    }
                ),
            )
            if lease["status"] != "pending_create":
                finalize_pre_dispatch_reservation()
                return {
                    "session_id": session_id,
                    "candidate_id": candidate_id,
                    "classification": (
                        "cleanup_required"
                        if lease["status"] == "cleanup_required"
                        else "inconclusive"
                    ),
                    "reason": (
                        "This idempotent index lease already has durable state. "
                        "Retrieve or reconcile that lease; no DDL or query was rerun."
                    ),
                    "failure_code": "index_lease_already_exists",
                    "executions": 0,
                    "lease": {
                        "lease_id": lease["lease_id"],
                        "status": lease["status"],
                        "version": lease["version"],
                    },
                    "session_continues": lease["status"] != "cleanup_required",
                }
            if phase == "screening":
                self.tuning_sessions.start_screening(session_id)
            else:
                self.tuning_sessions.mark_candidate_finalist(session_id, candidate_id)
        except BaseException:
            finalize_pre_dispatch_reservation()
            if lease is not None:
                self.performance_store.update_index_lease(
                    lease_id,
                    status="create_failed",
                    metadata={"failure_type": "workflow_transition_failed"},
                    owner_reference=lease_owner,
                    expected_version=lease["version"],
                )
            raise

        measured_executions = 0
        measurements: list[dict[str, Any]] = [
            {
                "parameter_case": bound_case["name"],
                "weight": bound_case["weight"],
                "execution_contract": bound_case["execution_contract"],
                "baseline_before_samples": [],
                "baseline_after_samples": [],
                "candidate_samples": [],
                "baseline_before_results": [],
                "baseline_after_results": [],
                "candidate_results": [],
                "baseline_before_plan": {},
                "baseline_after_plan": {},
                "candidate_plan": {},
                "plan_use": [],
            }
            for bound_case in bound_cases
        ]
        cleanup_error: str | None = None
        create_attempted = False
        ownership_recorded = False
        benchmark_error: str | None = None
        cancelled = False
        equivalence: list[dict[str, Any]] = []

        async def record_observed_index(
            observed_index: Any,
            *,
            observation: str,
        ) -> bool:
            nonlocal lease, ownership_recorded
            if observed_index is None or not expected_index_definition_matches(
                observed_index,
                index_candidate,
            ):
                return False
            if lease is None:
                raise RuntimeError("Temporary index lease was not created.")
            await self._verify_pending_index_ownership(
                database_name,
                lease,
                observed_index,
                lease_owner,
            )
            lease = self.performance_store.update_index_lease(
                lease_id,
                status="active",
                metadata={
                    "created_at_utc": datetime.now(timezone.utc).isoformat(),
                    "observed_definition_fingerprint": (
                        observed_index.definition_fingerprint
                    ),
                    "fingerprint_provenance": "observed_existing_index",
                    "creation_observation": observation,
                },
                owner_reference=lease_owner,
                expected_version=lease["version"],
            )
            ownership_recorded = True
            return True

        async def reconcile_create_outcome() -> None:
            nonlocal lease
            if not create_attempted or ownership_recorded or lease is None:
                return
            lease = self.performance_store.update_index_lease(
                lease_id,
                status="cleanup_required",
                metadata={
                    "ownership_status": "uncertain_after_create_dispatch",
                    "ownership_proof": "none",
                    "cleanup_policy": "do_not_adopt_or_drop",
                },
                owner_reference=lease_owner,
                expected_version=lease["version"],
            )

        try:
            for measurement in measurements:
                for _ in range(runs):
                    self._require_tuning_session_time(session_id)
                    measured_executions += 1
                    profiled = await self.performance_workflows._profile_execution_contract(
                        database_name,
                        measurement["execution_contract"],
                        max_result_rows=self.config.comparison_row_limit,
                    )
                    if profiled.user_query_executions != 1:
                        raise RuntimeError(
                            "Profiled samples must execute the user query exactly once."
                        )
                    measurement["baseline_before_samples"].append(
                        extract_profile_metrics(profiled)
                    )
                    measurement["baseline_before_results"].append(
                        profile_result_fingerprint(
                            profiled,
                            compare_order=compare_order,
                        )
                    )
                    measurement["baseline_before_plan"] = profiled.plan.summary
            self._require_tuning_session_time(session_id)
            if lease is None:
                raise RuntimeError("Temporary index lease was not created.")
            lease = self.performance_store.update_index_lease(
                lease_id,
                status="pending_create",
                metadata={
                    "create_dispatch_state": "dispatched",
                    "create_dispatched_at_utc": datetime.now(
                        timezone.utc
                    ).isoformat(),
                },
                owner_reference=lease_owner,
                expected_version=lease["version"],
            )
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
                filter_definition=filter_definition,
                is_unique=is_unique,
                idempotency_key=create_operation_key,
                ownership_proof=lease_owner,
            )
            observed_indexes = await collect_existing_indexes(
                self.executor,
                database_name,
            )
            observed_index = next(
                (
                    existing
                    for existing in observed_indexes
                    if existing.schema == schema
                    and existing.table == table
                    and existing.name == index_name
                ),
                None,
            )
            if not await record_observed_index(
                observed_index,
                observation="post_create",
            ):
                raise RuntimeError(
                    "Created index definition could not be verified exactly."
                )
            for measurement in measurements:
                for _ in range(runs):
                    self._require_tuning_session_time(session_id)
                    measured_executions += 1
                    profiled = await self.performance_workflows._profile_execution_contract(
                        database_name,
                        measurement["execution_contract"],
                        max_result_rows=self.config.comparison_row_limit,
                    )
                    if profiled.user_query_executions != 1:
                        raise RuntimeError(
                            "Profiled samples must execute the user query exactly once."
                        )
                    measurement["candidate_samples"].append(
                        extract_profile_metrics(profiled)
                    )
                    measurement["candidate_results"].append(
                        profile_result_fingerprint(
                            profiled,
                            compare_order=compare_order,
                        )
                    )
                    measurement["candidate_plan"] = profiled.plan.summary
                    measurement["plan_use"].append(
                        verify_plan_uses_index(
                            profiled.plan.raw_xml,
                            index_name,
                        ).as_dict()
                    )
        except asyncio.CancelledError:
            benchmark_error = "timeout"
            cancelled = True
            try:
                await reconcile_create_outcome()
            except Exception:
                logger.exception(
                    "Unable to durably reconcile temporary index ownership",
                    extra={"lease_id": lease_id},
                )
        except Exception as exc:
            benchmark_error = type(exc).__name__
            try:
                await reconcile_create_outcome()
            except Exception:
                logger.exception(
                    "Unable to durably reconcile temporary index ownership",
                    extra={"lease_id": lease_id},
                )
        finally:
            if ownership_recorded:
                try:
                    lease = self.performance_store.update_index_lease(
                        lease_id,
                        status="cleanup_pending",
                        owner_reference=lease_owner,
                        expected_version=lease["version"],
                    )
                    current_indexes = await collect_existing_indexes(
                        self.executor,
                        database_name,
                    )
                    current_index = next(
                        (
                            existing
                            for existing in current_indexes
                            if existing.schema == schema
                            and existing.table == table
                            and existing.name == index_name
                        ),
                        None,
                    )
                    if current_index is not None:
                        lease_metadata = lease.get("metadata") or {}
                        observed_fingerprint = lease_metadata.get(
                            "observed_definition_fingerprint"
                        )
                        verification_fingerprint = (
                            observed_fingerprint
                            if isinstance(observed_fingerprint, str)
                            and observed_fingerprint
                            else str(lease["object_fingerprint"])
                        )
                        owned_definition = (
                            current_index.definition_fingerprint
                            == verification_fingerprint
                            and expected_index_definition_matches(
                                current_index,
                                index_candidate,
                            )
                        )
                        if not owned_definition:
                            raise RuntimeError(
                                "Temporary index name now identifies an unowned definition."
                            )
                    if current_index is not None:
                        await self._drop_test_index(
                            database_name,
                            schema,
                            table,
                            index_name,
                            dry_run=False,
                            workflow_managed=True,
                            idempotency_key=cleanup_operation_key,
                            ownership_proof=lease_owner,
                            expected_definition_fingerprint=str(
                                lease["object_fingerprint"]
                            ),
                            expected_index_id=current_index.index_id,
                        )
                    remaining_indexes = await collect_existing_indexes(
                        self.executor,
                        database_name,
                    )
                    if any(
                        existing.schema == schema
                        and existing.table == table
                        and existing.name == index_name
                        for existing in remaining_indexes
                    ):
                        raise RuntimeError("Temporary index removal was not verified.")
                    lease = self.performance_store.update_index_lease(
                        lease_id,
                        status="cleaned",
                        metadata={"cleaned_at_utc": datetime.now(timezone.utc).isoformat()},
                        owner_reference=lease_owner,
                        expected_version=lease["version"],
                    )
                except asyncio.CancelledError:
                    cancelled = True
                    cleanup_error = "cancelled"
                    try:
                        lease = self.performance_store.update_index_lease(
                            lease_id,
                            status="cleanup_required",
                            metadata={
                                "cleanup_error_type": "cancelled",
                                "cleanup_policy": "retry_only_with_observed_ownership",
                            },
                            owner_reference=lease_owner,
                            expected_version=lease["version"],
                        )
                    except Exception:
                        logger.exception(
                            "Unable to persist cancellation during temporary-index cleanup",
                            extra={"lease_id": lease_id},
                        )
                except Exception as exc:
                    cleanup_error = type(exc).__name__
                    lease = self.performance_store.update_index_lease(
                        lease_id,
                        status="cleanup_required",
                        metadata={"cleanup_error_type": cleanup_error},
                        owner_reference=lease_owner,
                        expected_version=lease["version"],
                    )
            elif benchmark_error:
                if lease["status"] != "cleanup_required":
                    lease = self.performance_store.update_index_lease(
                        lease_id,
                        status="create_failed",
                        metadata={"failure_type": benchmark_error},
                        owner_reference=lease_owner,
                        expected_version=lease["version"],
                    )

        if not cleanup_error and not benchmark_error and lease["status"] == "cleaned":
            try:
                for measurement in measurements:
                    for _ in range(runs):
                        self._require_tuning_session_time(session_id)
                        measured_executions += 1
                        profiled = (
                            await self.performance_workflows._profile_execution_contract(
                                database_name,
                                measurement["execution_contract"],
                                max_result_rows=self.config.comparison_row_limit,
                            )
                        )
                        if profiled.user_query_executions != 1:
                            raise RuntimeError(
                                "Profiled samples must execute the user query exactly once."
                            )
                        measurement["baseline_after_samples"].append(
                            extract_profile_metrics(profiled)
                        )
                        measurement["baseline_after_results"].append(
                            profile_result_fingerprint(
                                profiled,
                                compare_order=compare_order,
                            )
                        )
                        measurement["baseline_after_plan"] = profiled.plan.summary
            except asyncio.CancelledError:
                benchmark_error = "timeout"
                cancelled = True
            except Exception as exc:
                benchmark_error = type(exc).__name__

        for measurement in measurements:
            result_evidence = [
                *measurement["baseline_before_results"],
                *measurement["candidate_results"],
                *measurement["baseline_after_results"],
            ]
            complete_results = all(
                len(measurement[result_key]) == runs
                and all(
                    item.get("complete") is True
                    for item in measurement[result_key]
                )
                for result_key in (
                    "baseline_before_results",
                    "candidate_results",
                    "baseline_after_results",
                )
            )
            fingerprints = {
                str(item["fingerprint"])
                for item in result_evidence
                if item.get("fingerprint")
            }
            plan_used = bool(measurement["plan_use"]) and all(
                item.get("used") is True for item in measurement["plan_use"]
            )
            stable = complete_results and len(fingerprints) == 1
            equivalence.append(
                {
                    "parameter_case": measurement["parameter_case"],
                    "status": (
                        "match"
                        if stable
                        else "inconclusive"
                        if not complete_results
                        else "mismatch"
                    ),
                    "proven_for_parameter_case": stable,
                    "same_sql": True,
                    "same_snapshot": False,
                    "basis": (
                        "unchanged SQL plus complete A-B-A result stability"
                    ),
                    "plan_used_expected_index": plan_used,
                    "result_evidence": result_evidence,
                }
            )

        parameter_results = [
            {
                "parameter_case": measurement["parameter_case"],
                "weight": measurement["weight"],
                "baseline": aggregate_samples(
                    [
                        *measurement["baseline_before_samples"],
                        *measurement["baseline_after_samples"],
                    ]
                ),
                "candidate": aggregate_samples(measurement["candidate_samples"]),
                "plan_delta": compare_plan_summaries_payload(
                    measurement["baseline_before_plan"],
                    measurement["candidate_plan"],
                ),
                "baseline_after_plan_delta": compare_plan_summaries_payload(
                    measurement["baseline_before_plan"],
                    measurement["baseline_after_plan"],
                ),
                "plan_use": measurement["plan_use"],
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
        elif not all(
            comparison.get("plan_used_expected_index") is True
            for comparison in equivalence
        ):
            state, reason = (
                "inconclusive",
                "the expected temporary index was not used in every measured bucket",
            )
        else:
            objective = str(case.metadata.get("objective") or "elapsed_time")
            state, reason = classify_benchmark(
                parameter_results,
                equivalence,
                objective=objective,
                require_equivalence=direct_snapshot_supported,
            )

        performance_classification = state
        performance_reason = reason
        candidate_state = state
        if (
            phase == "screening"
            and not direct_snapshot_supported
            and state in {"promising", "improved"}
        ):
            state = "proof_contract_required"
            candidate_state = "inconclusive"
            reason = (
                "Performance screening improved, but this MCP contract has no "
                "deterministic proof input for this SQL shape; the candidate was "
                "not promoted."
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
                            "parameter_case_fingerprint": bound_case[
                                "parameter_case_fingerprint"
                            ],
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
                    "performance_classification": performance_classification,
                    "objective": str(case.metadata.get("objective") or "elapsed_time"),
                    "parameter_results": parameter_results,
                },
                metadata={
                    "session_id": session_id,
                    "candidate_id": candidate_id,
                    "lease_id": lease_id,
                    "lease_status": lease["status"],
                    "index_definition_fingerprint": object_fingerprint,
                    "execution_reservation_id": execution_reservation[
                        "reservation_id"
                    ],
                    "equivalence": equivalence,
                    "equivalence_preflight": equivalence_preflight,
                    "proof_scope": (
                        "aba_result_stability"
                        if direct_snapshot_supported
                        else "performance_only"
                    ),
                    "lineage": lineage,
                    "phase": phase,
                    "reason": reason,
                    "performance_reason": performance_reason,
                },
            ),
            idempotency_key=index_evidence_key,
            request_fingerprint=request_fingerprint,
        )
        durable_state = candidate_state
        if phase == "screening" and candidate_state in {"promising", "improved"}:
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="screening",
                screen_runs=runs,
                parameter_cases=len(cases),
                executions=measured_executions,
                evidence_ids=(evidence.evidence_id,),
                idempotency_key=index_operation_key,
            )
            durable_state = updated.state
        else:
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state=candidate_state,
                screen_runs=runs if phase == "screening" else 0,
                finalist_runs=runs if phase == "finalist" else 0,
                parameter_cases=len(cases),
                executions=measured_executions,
                evidence_ids=(evidence.evidence_id,),
                failure_code=(
                    state
                    if candidate_state
                    in {"inconclusive", "cleanup_required", "equivalence_failed"}
                    else None
                ),
                idempotency_key=index_operation_key,
            )
        reservation_update = (
            self.performance_store.complete_execution_attempts
            if measured_executions
            else self.performance_store.release_execution_attempts
        )
        reservation_update(
            execution_reservation["reservation_id"],
            dispatched_attempt_count=measured_executions,
            owner_reference=execution_owner,
            expected_version=execution_reservation["version"],
        )
        public_lease = self._public_index_lease(lease)
        result = {
            "session_id": session_id,
            "candidate_id": candidate_id,
            "classification": state,
            "performance_classification": performance_classification,
            "objective": str(case.metadata.get("objective") or "elapsed_time"),
            "durable_state": durable_state,
            "reason": reason,
            "phase": phase,
            "executions": measured_executions,
            "metrics": parameter_results[0],
            "parameter_results": parameter_results,
            "equivalence": equivalence,
            "equivalence_preflight": equivalence_preflight,
            "proof_scope": (
                "aba_result_stability"
                if direct_snapshot_supported
                else "performance_only"
            ),
            "lineage": lineage,
            "evidence_id": evidence.evidence_id,
            "lease": public_lease,
            "index_definition_fingerprint": object_fingerprint,
            "index_ddl": index_ddl,
            "rollback_ddl": rollback_ddl,
            "session_continues": True,
        }
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def _run_tool(
        self,
        tool_name: str,
        database_name: str | None,
        callback,
        *,
        deadline_provider: Callable[[], str | None] | None = None,
    ) -> ResponseType:
        requested_database = (database_name or self.config.default_database).strip()
        correlation_id = str(uuid.uuid4())
        started_at = time.monotonic()
        timeout_seconds = self.config.tool_timeout_seconds
        try:
            resolved_database = self.config.validate_database_name(database_name)
            timeout_seconds = self._timeout_for_tool(
                tool_name,
                resolved_database,
            )
            if deadline_provider is not None:
                deadline_at_utc = deadline_provider()
                if deadline_at_utc:
                    deadline = datetime.fromisoformat(
                        deadline_at_utc.replace("Z", "+00:00")
                    )
                    if deadline.tzinfo is None:
                        deadline = deadline.replace(tzinfo=timezone.utc)
                    remaining = (
                        deadline.astimezone(timezone.utc)
                        - datetime.now(timezone.utc)
                    ).total_seconds()
                    timeout_seconds = min(timeout_seconds, max(0.0, remaining))
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
                timeout=timeout_seconds,
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
        except ToolError:
            raise
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
            self._raise_tool_error(
                "timeout",
                f"Tool '{tool_name}' timed out after {timeout_seconds}s.",
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
            self._raise_tool_error("tool_error", sanitized_error)

    def _timeout_for_tool(
        self,
        tool_name: str,
        database_name: str | None = None,
    ) -> float:
        if tool_name in _SESSION_WORKFLOW_TOOLS:
            per_request_executions = (
                self.database_policy.policy_for(
                    database_name
                ).max_benchmark_executions
                if database_name
                else 0
            )
            benchmark_timeout = (
                self.config.query_timeout_seconds
                * max(1, per_request_executions)
                + 5 * 60
            )
            return max(
                self.config.tool_timeout_seconds,
                21 * 60,
                benchmark_timeout,
            )
        if tool_name in _EVIDENCE_WORKFLOW_TOOLS:
            return max(
                self.config.tool_timeout_seconds,
                self.config.query_timeout_seconds + 60,
            )
        return self.config.tool_timeout_seconds

    def _require_tuning_session_time(self, session_id: str) -> float:
        """Fence a new benchmark dispatch against the durable session deadline."""

        session = self.tuning_sessions.get_session(session_id)
        if not session.deadline_at_utc:
            return float("inf")
        deadline = datetime.fromisoformat(
            session.deadline_at_utc.replace("Z", "+00:00")
        )
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        remaining = (
            deadline.astimezone(timezone.utc) - datetime.now(timezone.utc)
        ).total_seconds()
        if remaining <= 0:
            raise TimeoutError("The tuning session time budget has expired.")
        return remaining

    async def _resolve_canonical_table_identity(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
    ) -> tuple[str, str]:
        """Resolve catalog spelling before generating any table DDL."""

        rows = await self.executor.fetch_all(
            database_name,
            """
            SELECT TOP (2)
                s.name AS schema_name,
                t.name AS table_name
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE LOWER(s.name) = LOWER(?)
              AND LOWER(t.name) = LOWER(?)
            ORDER BY s.name, t.name
            """,
            params=[schema_name, table_name],
            max_rows=2,
        )
        if len(rows) != 1:
            raise ValueError(
                "The target table could not be resolved to one canonical Azure SQL "
                "catalog object; no index DDL was dispatched."
            )
        resolved_schema = self._validate_plain_identifier(
            str(rows[0].get("schema_name") or ""),
            "catalog schema_name",
        )
        resolved_table = self._validate_plain_identifier(
            str(rows[0].get("table_name") or ""),
            "catalog table_name",
        )
        return resolved_schema, resolved_table

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
        except ToolError:
            raise
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
            self._raise_tool_error(
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
            self._raise_tool_error("tool_error", sanitized_error)

    async def _execute_safe_sql(self, database_name: str, sql: str) -> dict[str, Any]:
        validated = self.validator.validate_read_only(sql)
        # Fetch at most row_limit + 1 rows to detect truncation without loading entire result
        fetch_limit = self.config.row_limit + 1
        rows = await self.executor.fetch_all(
            database_name, validated.execution_sql, max_rows=fetch_limit,
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
        filter_definition: str | None = None,
        is_unique: bool = False,
        idempotency_key: str | None = None,
        ownership_proof: str | None = None,
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

        candidate = IndexCandidate(
            schema=schema,
            table=table,
            key_columns=tuple(key_columns),
            include_columns=tuple(include_columns or ()),
            filter_definition=filter_definition,
            is_unique=is_unique,
            index_name=index,
        )
        sql = build_index_candidate_statement(candidate, online=online)
        rollback_sql = f"DROP INDEX [{index}] ON [{schema}].[{table}];"
        if workflow_managed and not idempotency_key:
            raise ValueError("Workflow-managed index creation requires an idempotency key.")
        if workflow_managed and (
            ownership_proof is None
            or not _INDEX_OWNER_PROOF.fullmatch(ownership_proof)
        ):
            raise ValueError(
                "Workflow-managed index creation requires a private lease-owner token."
            )
        params: tuple[Any, ...] = ()
        rollback_params: tuple[Any, ...] = ()
        if workflow_managed:
            definition_fingerprint = candidate.definition_fingerprint
            params = (ownership_proof, definition_fingerprint)
            rollback_params = (ownership_proof, definition_fingerprint, None)
            sql = (
                "SET XACT_ABORT ON;\n"
                "BEGIN TRANSACTION;\n"
                f"{sql.rstrip().rstrip(';')};\n"
                "EXEC sys.sp_addextendedproperty "
                f"@name = N'{TEST_INDEX_OWNER_PROPERTY}', "
                "@value = ?, "
                "@level0type = N'SCHEMA', "
                f"@level0name = N'{schema}', "
                "@level1type = N'TABLE', "
                f"@level1name = N'{table}', "
                "@level2type = N'INDEX', "
                f"@level2name = N'{index}';\n"
                "EXEC sys.sp_addextendedproperty "
                f"@name = N'{TEST_INDEX_DEFINITION_PROPERTY}', "
                "@value = ?, "
                "@level0type = N'SCHEMA', "
                f"@level0name = N'{schema}', "
                "@level1type = N'TABLE', "
                f"@level1name = N'{table}', "
                "@level2type = N'INDEX', "
                f"@level2name = N'{index}';\n"
                "COMMIT TRANSACTION;"
            )
            rollback_sql = self._build_guarded_index_drop_sql(schema, table, index)

        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="create_test_index",
                database_name=database_name,
                action_type="test_index",
                sql=sql,
                params=params,
                rollback_sql=rollback_sql,
                rollback_params=rollback_params,
                trusted_generated=True,
                reviewed_intent=workflow_managed,
                idempotency_key=idempotency_key,
                exactly_once=workflow_managed,
                policy_verified=workflow_managed,
                non_production=workflow_managed,
                verification_required=workflow_managed,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "index": f"[{schema}].[{table}].[{index}]",
            "key_columns": key_columns,
            "include_columns": include_columns or [],
            "filter_definition": filter_definition,
            "is_unique": is_unique,
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
        idempotency_key: str | None = None,
        ownership_proof: str | None = None,
        expected_definition_fingerprint: str | None = None,
        expected_index_id: int | None = None,
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
        sql = f"DROP INDEX [{index}] ON [{schema}].[{table}];"
        if workflow_managed and not idempotency_key:
            raise ValueError("Workflow-managed index cleanup requires an idempotency key.")
        params: tuple[Any, ...] = ()
        if workflow_managed:
            if ownership_proof is None or not _INDEX_OWNER_PROOF.fullmatch(ownership_proof):
                raise ValueError(
                    "Workflow-managed index cleanup requires a private lease-owner token."
                )
            if expected_definition_fingerprint is not None and not re.fullmatch(
                r"[0-9a-fA-F]{64}", expected_definition_fingerprint
            ):
                raise ValueError("Expected index definition fingerprint is invalid.")
            if expected_index_id is not None and expected_index_id <= 0:
                raise ValueError("Expected index_id must be greater than 0.")
            params = (
                ownership_proof,
                expected_definition_fingerprint,
                expected_index_id,
            )
            sql = self._build_guarded_index_drop_sql(schema, table, index)
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="drop_test_index",
                database_name=database_name,
                action_type="test_index",
                sql=sql,
                params=params,
                trusted_generated=True,
                reviewed_intent=workflow_managed,
                idempotency_key=idempotency_key,
                exactly_once=workflow_managed,
                policy_verified=workflow_managed,
                non_production=workflow_managed,
                verification_required=workflow_managed,
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
    def _build_guarded_index_drop_sql(
        schema: str,
        table: str,
        index: str,
    ) -> str:
        return (
            "SET XACT_ABORT ON;\n"
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;\n"
            "BEGIN TRY\n"
            "    BEGIN TRANSACTION;\n"
            f"    DECLARE @object_id int = OBJECT_ID(N'{schema}.{table}', N'U');\n"
            "    IF @object_id IS NULL\n"
            "        THROW 50002, 'Temporary index target table is missing.', 1;\n"
            "    DECLARE @table_lock_probe bit;\n"
            f"    SELECT TOP (1) @table_lock_probe = 1 FROM [{schema}].[{table}] WITH (TABLOCK, HOLDLOCK);\n"
            "    DECLARE @owner_marker nvarchar(4000) = ?;\n"
            "    DECLARE @expected_definition nvarchar(4000) = ?;\n"
            "    DECLARE @expected_index_id int = ?;\n"
            "    DECLARE @index_id int;\n"
            "    SELECT @index_id = i.index_id\n"
            "    FROM sys.indexes AS i WITH (UPDLOCK, HOLDLOCK)\n"
            "    INNER JOIN sys.tables AS t WITH (UPDLOCK, HOLDLOCK)\n"
            "        ON t.object_id = i.object_id\n"
            "    INNER JOIN sys.schemas AS s WITH (UPDLOCK, HOLDLOCK)\n"
            "        ON s.schema_id = t.schema_id\n"
            f"    WHERE i.object_id = @object_id AND s.name = N'{schema}'\n"
            f"      AND t.name = N'{table}' AND i.name = N'{index}';\n"
            "    IF @index_id IS NULL\n"
            "        THROW 50003, 'Temporary index object is missing.', 1;\n"
            "    IF @expected_index_id IS NOT NULL AND @index_id <> @expected_index_id\n"
            "        THROW 50004, 'Temporary index object identity mismatch.', 1;\n"
            "    IF NOT EXISTS (\n"
            "        SELECT 1\n"
            "        FROM sys.extended_properties AS ep WITH (UPDLOCK, HOLDLOCK)\n"
            "        WHERE ep.class = 7\n"
            "          AND ep.major_id = @object_id\n"
            "          AND ep.minor_id = @index_id\n"
            f"          AND ep.name = N'{TEST_INDEX_OWNER_PROPERTY}'\n"
            "          AND CONVERT(nvarchar(4000), ep.value) COLLATE Latin1_General_100_BIN2\n"
            "              = @owner_marker COLLATE Latin1_General_100_BIN2\n"
            "    )\n"
            "        THROW 50001, 'Temporary index ownership marker mismatch.', 1;\n"
            "    IF @expected_definition IS NOT NULL AND NOT EXISTS (\n"
            "        SELECT 1\n"
            "        FROM sys.extended_properties AS ep WITH (UPDLOCK, HOLDLOCK)\n"
            "        WHERE ep.class = 7\n"
            "          AND ep.major_id = @object_id\n"
            "          AND ep.minor_id = @index_id\n"
            f"          AND ep.name = N'{TEST_INDEX_DEFINITION_PROPERTY}'\n"
            "          AND CONVERT(nvarchar(4000), ep.value) COLLATE Latin1_General_100_BIN2\n"
            "              = @expected_definition COLLATE Latin1_General_100_BIN2\n"
            "    )\n"
            "        THROW 50005, 'Temporary index definition mismatch.', 1;\n"
            f"    DROP INDEX [{index}] ON [{schema}].[{table}];\n"
            "    COMMIT TRANSACTION;\n"
            "    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\n"
            "END TRY\n"
            "BEGIN CATCH\n"
            "    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;\n"
            "    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\n"
            "    THROW;\n"
            "END CATCH;"
        )

    @staticmethod
    def _public_index_lease(lease: Mapping[str, Any]) -> dict[str, Any]:
        public = dict(lease)
        public.pop("owner_token", None)
        public.pop("fencing_token", None)
        metadata = public.get("metadata")
        if isinstance(metadata, Mapping):
            safe_metadata = dict(metadata)
            safe_metadata.pop("lease_owner_fence", None)
            public["metadata"] = safe_metadata
        return public

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
        parameter_types: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")
        parameter_case, binding_info = await self._compatibility_parameter_case(
            database_name,
            sql,
            auto_bind_params,
            parameter_values,
            parameter_types,
        )
        performance_case = self.performance_workflows.start_case(
            database_name,
            sql,
            parameter_cases=[parameter_case] if parameter_case is not None else None,
            metadata={
                "objective": "elapsed_time",
                "compatibility_tool": "tune_query",
                "raw_sql_persisted": False,
            },
        )
        evidence = await self._collect_performance_evidence(
            database_name,
            performance_case.case_id,
            sql,
            window_minutes,
            analyze,
            None,
            parameter_case,
        )
        session = self.performance_workflows.start_session(
            performance_case.case_id,
            database_name,
        )
        return {
            "database_name": database_name,
            "performance_case_id": performance_case.case_id,
            "tuning_session_id": session["session_id"],
            "query_hash": fingerprint_text(sql),
            "analyze": analyze,
            "parameter_binding": binding_info,
            "evidence": evidence,
            "session": session,
            "workflow_complete": False,
            "candidate_required": True,
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
        runs: int = 3,
        parameter_values: dict[str, Any] | None = None,
        compare_order: bool = True,
        parameter_types: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if not analyze:
            raise ValueError("benchmark_query_rewrite requires analyze=true for measured results.")
        if not 2 <= runs <= 3:
            raise ValueError("Compatibility screening runs must be between 2 and 3.")
        parameter_case, binding_info = await self._compatibility_parameter_case(
            database_name,
            baseline_sql,
            auto_bind_params,
            parameter_values,
            parameter_types,
        )
        parameter_cases = [parameter_case] if parameter_case is not None else None
        if parameter_case is not None:
            await self._bind_performance_parameters(
                database_name,
                rewrite_sql,
                parameter_case,
            )
        performance_case = self.performance_workflows.start_case(
            database_name,
            baseline_sql,
            parameter_cases=parameter_cases,
            metadata={
                "objective": "elapsed_time",
                "compatibility_tool": "benchmark_query_rewrite",
            },
        )
        session = self.performance_workflows.start_session(
            performance_case.case_id,
            database_name,
        )
        candidate = self.performance_workflows.add_candidate(
            session["session_id"],
            rewrite_sql,
            strategy="predicate",
        )
        benchmark = await self.performance_workflows.benchmark_candidate(
            session["session_id"],
            candidate["candidate_id"],
            database_name,
            baseline_sql,
            rewrite_sql,
            phase="screening",
            parameter_cases=parameter_cases,
            compare_order=compare_order,
            runs_override=runs,
            prove_equivalence=True,
        )
        benchmark.update({
            "database_name": database_name,
            "analyze": True,
            "performance_case_id": performance_case.case_id,
            "tuning_session_id": session["session_id"],
            "winning_sql": rewrite_sql if benchmark.get("classification") == "improved" else None,
            "parameter_binding": {
                "baseline": binding_info,
                "rewrite": binding_info,
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
        """Return Query Store evidence only after stable identity resolution."""
        summary = plan.get("summary")
        statements = summary.get("statements") if isinstance(summary, dict) else None
        query_hash = None
        if isinstance(statements, list) and statements:
            first = statements[0]
            if isinstance(first, dict):
                query_hash = first.get("query_hash")

        identity = await self.query_store.resolve_query_identity(
            database_name,
            original_sql,
        )
        if identity.get("status") != "resolved":
            return {
                "database_name": database_name,
                "window_minutes": window_minutes,
                "matches": [],
                "status": "inconclusive",
                "reason": "exact Query Store identity was not uniquely resolved",
                "identity": identity,
                "plan_query_hash": query_hash,
                "matched_by": "none",
                "fuzzy_match_used": False,
            }
        history = await self.query_store.get_query_history_by_id(
            database_name,
            int(identity["query_id"]),
            window_minutes=window_minutes,
            limit=10,
        )
        history["matched_by"] = "query_id"
        history["fuzzy_match_used"] = False
        history["plan_query_hash"] = query_hash
        history["identity_query_hash"] = identity.get("query_hash")
        history["query_hash_corroborated"] = (
            str(query_hash).casefold()
            == str(identity.get("query_hash")).casefold()
            if query_hash is not None and identity.get("query_hash") is not None
            else None
        )
        if history["query_hash_corroborated"] is False:
            history["status"] = "inconclusive"
            history["matches"] = []
            history["reason"] = (
                "the exact Query Store identity resolved to a different query hash "
                "than the captured plan"
            )
        return history

    async def _compatibility_parameter_case(
        self,
        database_name: str,
        sql: str,
        auto_bind_params: bool,
        parameter_values: dict[str, Any] | None,
        parameter_types: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        detected = detect_parameters(sql)
        detected_names = {name.casefold() for name in detected}
        supplied_names = {
            str(name).lstrip("@").casefold()
            for name in (parameter_values or {})
        }
        if detected and not auto_bind_params and supplied_names != detected_names:
            raise ValueError(
                "Parameterized execution with auto_bind_params=false requires "
                "one explicit value for every query parameter."
            )
        if not detected and not parameter_values and not parameter_types:
            return None, None

        bucket = await self.param_binding.build_parameter_bucket(
            database_name,
            sql,
            parameter_values=parameter_values,
            parameter_types=parameter_types,
            bucket_id="compatibility",
            label="compatibility",
            provenance=(
                "compatibility_auto_binding"
                if auto_bind_params
                else "compatibility_explicit_binding"
            ),
        )
        if not bucket.parameters:
            return None, None

        parameter_case = {
            "name": "compatibility",
            "values": {
                parameter.name.lstrip("@"): parameter.value
                for parameter in bucket.parameters
            },
            "types": {
                parameter.name.lstrip("@"): parameter.sql_type.sql_declaration
                for parameter in bucket.parameters
            },
            "weight": 1.0,
        }
        binding_summary = {
            "bucket_id": bucket.bucket_id,
            "provenance": bucket.provenance,
            "values_redacted": True,
            "parameters": [
                {
                    "name": parameter.name,
                    "data_type": parameter.sql_type.sql_declaration,
                    "provenance": parameter.provenance,
                    "provenance_detail": dict(parameter.provenance_detail),
                }
                for parameter in bucket.parameters
            ],
        }
        return parameter_case, binding_summary

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
        parameter_types: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if hypothetical_indexes:
            raise ValueError(
                "Hypothetical index analysis is disabled on explain_query for safety. "
                "Use analyze_query_indexes/analyze_workload_indexes for read-only index insights."
            )
        parameter_case, binding_info = await self._compatibility_parameter_case(
            database_name,
            sql,
            auto_bind_params,
            parameter_values,
            parameter_types,
        )
        if parameter_case is not None:
            contract = await self._bind_performance_parameters(
                database_name,
                sql,
                parameter_case,
            )
            artifact = await self.plans.explain_parameterized_query(
                database_name,
                contract,
                analyze=analyze,
            )
        else:
            if detect_parameters(sql):
                raise ValueError(
                    "Parameterized SQL requires explicit values or auto_bind_params=true."
                )
            artifact = await self.plans.explain_query(
                database_name,
                sql,
                analyze,
                hypothetical_indexes=hypothetical_indexes,
            )
        result = self._artifact_to_dict(artifact, include_raw_xml=include_raw_xml)
        if binding_info is not None:
            result["parameter_binding"] = binding_info
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
        parameter_types: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        normalized_values: dict[str, Any] = {}
        for raw_name, value in (parameter_values or {}).items():
            name = str(raw_name).lstrip("@").strip().casefold()
            if not name:
                raise ValueError("explicit parameter name must not be empty")
            if name in normalized_values:
                raise ValueError(f"duplicate explicit parameter name: {name}")
            normalized_values[name] = value
        normalized_types: dict[str, str] = {}
        for raw_name, value in (parameter_types or {}).items():
            name = str(raw_name).lstrip("@").strip().casefold()
            if not name:
                raise ValueError("explicit parameter name must not be empty")
            if name in normalized_types:
                raise ValueError(f"duplicate explicit parameter type: {name}")
            normalized_types[name] = str(value)
        detected_by_query = [
            {name.casefold() for name in detect_parameters(query)}
            for query in queries
        ]
        detected_any = set().union(*detected_by_query) if detected_by_query else set()
        unknown_names = sorted(
            (set(normalized_values) | set(normalized_types)) - detected_any
        )
        if unknown_names:
            raise ValueError(
                "explicit value or type supplied for unknown parameter(s): "
                + ", ".join(unknown_names)
            )

        contracts: list[ParameterExecutionContract] = []
        binding_summaries: list[dict[str, Any] | None] = []
        for ordinal, (query, detected_names) in enumerate(
            zip(queries, detected_by_query, strict=True),
            start=1,
        ):
            if not detected_names:
                contracts.append(
                    ParameterExecutionContract(
                        sql_text=query,
                        bucket_id=f"index-analysis-{ordinal}",
                        parameters=(),
                        provenance="unparameterized_index_analysis",
                    )
                )
                binding_summaries.append(None)
                continue
            query_values = {
                name: value
                for name, value in normalized_values.items()
                if name in detected_names
            }
            query_types = {
                name: value
                for name, value in normalized_types.items()
                if name in detected_names
            }
            if not auto_bind_params and set(query_values) != detected_names:
                raise ValueError(
                    "Parameterized index analysis with auto_bind_params=false "
                    "requires one explicit value for every query parameter."
                )
            bucket = await self.param_binding.build_parameter_bucket(
                database_name,
                query,
                parameter_values=query_values,
                parameter_types=query_types,
                bucket_id=f"index-analysis-{ordinal}",
                provenance=(
                    "index_analysis_auto_binding"
                    if auto_bind_params
                    else "index_analysis_explicit_binding"
                ),
            )
            contracts.append(
                self.param_binding.build_execution_contract(
                    query,
                    bucket,
                    provenance="typed_index_analysis",
                )
            )
            binding_summaries.append(
                {
                    "bucket_id": bucket.bucket_id,
                    "values_redacted": True,
                    "parameters": [
                        {
                            "name": parameter.name,
                            "data_type": parameter.sql_type.sql_declaration,
                            "provenance": parameter.provenance,
                        }
                        for parameter in bucket.parameters
                    ],
                }
            )
        result = await self.query_index_analysis.analyze_queries(
            database_name,
            queries,
            execution_contracts=contracts,
        )
        result["parameter_binding"] = binding_summaries
        return result

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

    @staticmethod
    def _raise_tool_error(code: str, message: str) -> NoReturn:
        payload = ErrorPayload(code=code, message=message).as_dict()
        raise ToolError(
            json.dumps(
                payload,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
        )

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

    async def _verify_pending_index_ownership(
        self,
        database_name: str,
        lease: Mapping[str, Any],
        current_index: Any,
        ownership_proof: str,
    ) -> None:
        """Prove an uncertain CREATE belongs to this lease before cleanup."""

        metadata = lease.get("metadata") or {}
        key_columns = metadata.get("key_columns")
        include_columns = metadata.get("include_columns")
        filter_definition = metadata.get("filter_definition")
        is_unique = metadata.get("is_unique")

        schema_name = metadata.get("target_schema")
        table_name = metadata.get("target_table")
        index_name = lease.get("index_name")
        object_fingerprint = str(lease.get("object_fingerprint") or "")
        marker_fingerprint = str(
            metadata.get("marker_definition_fingerprint") or object_fingerprint
        )
        if not isinstance(schema_name, str) or not schema_name:
            raise ValueError("pending CREATE lease has no target schema")
        if not isinstance(table_name, str) or not table_name:
            raise ValueError("pending CREATE lease has no target table")
        if not isinstance(index_name, str) or not index_name:
            raise ValueError("pending CREATE lease has no resolvable target")
        if marker_fingerprint != object_fingerprint:
            raise ValueError("pending CREATE lease marker fingerprint is inconsistent")
        persisted_structure = any(
            value is not None
            for value in (key_columns, include_columns, filter_definition, is_unique)
        )
        if persisted_structure:
            if not isinstance(key_columns, (list, tuple)) or not key_columns:
                raise ValueError("pending CREATE lease key structure is invalid")
            if not all(isinstance(column, str) for column in key_columns):
                raise ValueError("pending CREATE lease key structure is invalid")
            if not isinstance(include_columns, (list, tuple)) or not all(
                isinstance(column, str) for column in include_columns
            ):
                raise ValueError("pending CREATE lease include structure is invalid")
            if filter_definition is not None and not isinstance(
                filter_definition, str
            ):
                raise ValueError("pending CREATE lease filter structure is invalid")
            if not isinstance(is_unique, bool):
                raise ValueError("pending CREATE lease uniqueness is invalid")
            candidate = IndexCandidate(
                schema=schema_name,
                table=table_name,
                key_columns=tuple(key_columns),
                include_columns=tuple(include_columns),
                filter_definition=filter_definition,
                is_unique=is_unique,
                index_name=index_name,
            )
        else:
            candidate = IndexCandidate(
                schema=current_index.schema,
                table=current_index.table,
                key_columns=tuple(
                    f"[{column.name}] {column.direction}"
                    for column in current_index.key_columns
                ),
                include_columns=tuple(current_index.include_columns),
                filter_definition=current_index.filter_definition,
                is_unique=current_index.is_unique,
                index_name=current_index.name,
            )
        if candidate.definition_fingerprint != marker_fingerprint:
            raise ValueError("pending CREATE lease structure does not match its marker")
        if not expected_index_definition_matches(current_index, candidate):
            raise ValueError(
                "pending CREATE index structure does not match its persisted candidate"
            )

        rows = await self.executor.fetch_all(
            database_name,
            _PENDING_INDEX_OWNERSHIP_SQL,
            params=(
                TEST_INDEX_OWNER_PROPERTY,
                TEST_INDEX_DEFINITION_PROPERTY,
                schema_name,
                table_name,
                index_name,
            ),
        )
        if len(rows) != 1:
            raise ValueError("pending CREATE ownership marker lookup was ambiguous")
        row = rows[0]
        raw_index_id = row.get("index_id")
        if raw_index_id is None:
            raise ValueError("pending CREATE ownership marker has no index identity")
        try:
            marker_index_id = int(str(raw_index_id))
        except (TypeError, ValueError) as exc:
            raise ValueError("pending CREATE ownership marker has no index identity") from exc
        if marker_index_id <= 0 or marker_index_id != current_index.index_id:
            raise ValueError("pending CREATE ownership marker index identity mismatch")
        if row.get("owner_marker") != ownership_proof:
            raise ValueError("pending CREATE ownership marker is missing or mismatched")
        if row.get("definition_marker") != marker_fingerprint:
            raise ValueError(
                "pending CREATE definition marker is missing or mismatched"
            )

    async def _cleanup_expired_index_leases(self) -> dict[str, int]:
        """Retry expired temporary-index cleanup before a sandbox accepts work."""

        summary = {"examined": 0, "cleaned": 0, "cleanup_required": 0}
        if self.config.profile != McpProfile.SANDBOX:
            return summary
        now = datetime.now(timezone.utc)
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
            if self._index_lease_has_live_session(lease, now):
                # A short/legacy lease must not let another MCP process drop an
                # index while its durable tuning session can still dispatch work.
                continue
            summary["examined"] += 1
            database_name = next(
                (
                    name
                    for name in self.config.allowed_databases
                    if database_fingerprint_matches(
                        str(lease["database_fingerprint"]),
                        name,
                        self.config.server,
                        allow_legacy=bool(self.config.legacy_state_server_binding),
                    )
                ),
                None,
            )
            metadata = lease.get("metadata") or {}
            schema_name = metadata.get("target_schema")
            table_name = metadata.get("target_table")
            ownership_proof = metadata.get("lease_owner_fence")
            try:
                if (
                    not database_name
                    or not isinstance(schema_name, str)
                    or not isinstance(table_name, str)
                ):
                    raise ValueError("expired lease has no resolvable cleanup target")
                if not isinstance(ownership_proof, str) or not _INDEX_OWNER_PROOF.fullmatch(
                    ownership_proof
                ):
                    raise ValueError(
                        "expired lease has no private ownership marker token"
                    )
                self._require_test_index_database(database_name)
                updated_lease = self.performance_store.recover_index_lease(
                    lease["lease_id"],
                    status="cleanup_pending",
                    metadata={"recovery_attempted_at_utc": now.isoformat()},
                    expected_version=lease["version"],
                )
                current_indexes = await collect_existing_indexes(
                    self.executor,
                    database_name,
                )
                current_index = next(
                    (
                        index
                        for index in current_indexes
                        if index.schema == schema_name
                        and index.table == table_name
                        and index.name == str(lease["index_name"])
                    ),
                    None,
                )
                if current_index is not None:
                    if lease["status"] == "pending_create" and metadata.get(
                        "create_dispatch_state"
                    ) != "dispatched":
                        raise RuntimeError(
                            "pre-dispatch temporary-index lease has an unexpected "
                            "visible index"
                        )
                    if lease["status"] == "pending_create":
                        await self._verify_pending_index_ownership(
                            database_name,
                            lease,
                            current_index,
                            ownership_proof,
                        )
                    else:
                        observed_fingerprint = metadata.get(
                            "observed_definition_fingerprint"
                        )
                        verification_fingerprint = (
                            observed_fingerprint
                            if isinstance(observed_fingerprint, str)
                            and observed_fingerprint
                            else str(lease["object_fingerprint"])
                        )
                        if (
                            metadata.get("fingerprint_provenance")
                            != "observed_existing_index"
                            or current_index.definition_fingerprint
                            != verification_fingerprint
                            or current_index.is_disabled
                        ):
                            raise RuntimeError(
                                "expired lease index definition no longer matches its "
                                "workflow-owned fingerprint"
                            )
                    await self._drop_test_index(
                        database_name,
                        schema_name,
                        table_name,
                        lease["index_name"],
                        dry_run=False,
                        workflow_managed=True,
                        idempotency_key=(
                            f"lease-recovery:{lease['lease_id']}:"
                            f"{updated_lease['version']}"
                        ),
                        ownership_proof=ownership_proof,
                        expected_definition_fingerprint=str(
                            lease["object_fingerprint"]
                        ),
                        expected_index_id=current_index.index_id,
                    )
                elif lease["status"] == "pending_create":
                    if metadata.get("create_dispatch_state") != "pre_dispatch":
                        raise RuntimeError(
                            "temporary-index lease has an uncertain create outcome"
                        )
                self.performance_store.recover_index_lease(
                    lease["lease_id"],
                    status="cleaned",
                    metadata={
                        "recovered_at_startup": True,
                        "cleaned_at_utc": datetime.now(timezone.utc).isoformat(),
                    },
                    expected_version=updated_lease["version"],
                )
                summary["cleaned"] += 1
            except Exception as exc:
                try:
                    current_lease = self.performance_store.get_index_lease(
                        lease["lease_id"]
                    )
                    self.performance_store.recover_index_lease(
                        lease["lease_id"],
                        status="cleanup_required",
                        metadata={
                            "recovery_error_type": type(exc).__name__,
                            "recovery_attempted_at_utc": now.isoformat(),
                        },
                        expected_version=current_lease["version"],
                    )
                except Exception:
                    logger.exception(
                        "Failed to record expired temporary-index cleanup failure",
                        extra={"lease_id": lease["lease_id"]},
                    )
                summary["cleanup_required"] += 1
        return summary

    def _index_lease_has_live_session(
        self,
        lease: Mapping[str, Any],
        now: datetime,
    ) -> bool:
        if lease.get("status") not in {"pending_create", "active"}:
            return False
        try:
            session = self.tuning_sessions.get_session(str(lease["session_id"]))
            if session.status not in {"screening", "finalist_validation"}:
                return False
            if not session.deadline_at_utc:
                return True
            deadline = datetime.fromisoformat(
                session.deadline_at_utc.replace("Z", "+00:00")
            )
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            return deadline.astimezone(timezone.utc) > now
        except (ContractNotFoundError, KeyError, TypeError, ValueError):
            return False

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
