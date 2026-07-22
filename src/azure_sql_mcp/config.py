from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from enum import Enum


class AuthMode(str, Enum):
    ENTRA_DEFAULT = "entra-default"
    SERVICE_PRINCIPAL = "service-principal"
    INTERACTIVE = "interactive"
    SQL_PASSWORD = "sql-password"


class AccessMode(str, Enum):
    RESTRICTED = "restricted"
    UNRESTRICTED = "unrestricted"


class WritePolicy(str, Enum):
    DISABLED = "disabled"
    REVIEW = "review"
    APPLY = "apply"


class ToolGroup(str, Enum):
    CORE = "core"
    PERFORMANCE = "performance"
    SCHEMA = "schema"
    ADMIN = "admin"
    ALL = "all"


class McpProfile(str, Enum):
    TRIAGE = "triage"
    OPTIMIZER = "optimizer"
    SANDBOX = "sandbox"
    ENFORCER_REVIEW = "enforcer-review"
    ENFORCER_APPLY = "enforcer-apply"


# Tool name → group mapping.  Tools not listed here are always registered.
TOOL_GROUPS: dict[str, ToolGroup] = {
    # core: essential query & introspection (always in "all", registered by default)
    "list_databases": ToolGroup.CORE,
    "check_capabilities": ToolGroup.CORE,
    "list_schemas": ToolGroup.CORE,
    "list_objects": ToolGroup.CORE,
    "search_objects": ToolGroup.CORE,
    "get_object_details": ToolGroup.CORE,
    "get_dependencies": ToolGroup.CORE,
    "get_table_stats": ToolGroup.CORE,
    "execute_sql": ToolGroup.CORE,
    "explain_query": ToolGroup.CORE,
    "tune_query": ToolGroup.CORE,
    "benchmark_query_rewrite": ToolGroup.CORE,
    "start_performance_case": ToolGroup.CORE,
    "collect_performance_evidence": ToolGroup.CORE,
    "get_performance_case": ToolGroup.CORE,
    "start_tuning_session": ToolGroup.CORE,
    "add_tuning_candidate": ToolGroup.CORE,
    "benchmark_tuning_candidate": ToolGroup.CORE,
    "benchmark_index_candidate": ToolGroup.CORE,
    "finalize_tuning_session": ToolGroup.CORE,
    "compare_query_results": ToolGroup.CORE,
    "compare_plan_summaries": ToolGroup.CORE,
    "analyze_db_health": ToolGroup.CORE,
    "get_top_queries": ToolGroup.CORE,
    "analyze_index_recommendations": ToolGroup.CORE,
    # performance: deep diagnostics & tuning
    "analyze_query_indexes": ToolGroup.PERFORMANCE,
    "analyze_workload_indexes": ToolGroup.PERFORMANCE,
    "optimize_indexes": ToolGroup.PERFORMANCE,
    "get_wait_stats": ToolGroup.PERFORMANCE,
    "get_query_wait_stats": ToolGroup.PERFORMANCE,
    "get_currently_waiting_tasks": ToolGroup.PERFORMANCE,
    "get_lock_details": ToolGroup.PERFORMANCE,
    "get_open_transactions": ToolGroup.PERFORMANCE,
    "get_deadlock_history": ToolGroup.PERFORMANCE,
    "get_tempdb_usage": ToolGroup.PERFORMANCE,
    "get_tempdb_space_breakdown": ToolGroup.PERFORMANCE,
    "get_memory_grants": ToolGroup.PERFORMANCE,
    "get_io_stats": ToolGroup.PERFORMANCE,
    "get_resource_limits": ToolGroup.PERFORMANCE,
    "get_resource_stats_history": ToolGroup.PERFORMANCE,
    "get_connection_pool_stats": ToolGroup.PERFORMANCE,
    "get_database_configuration": ToolGroup.PERFORMANCE,
    "get_storage_diagnostics": ToolGroup.PERFORMANCE,
    "get_connection_diagnostics": ToolGroup.PERFORMANCE,
    "get_top_cached_queries": ToolGroup.PERFORMANCE,
    "get_cached_routine_stats": ToolGroup.PERFORMANCE,
    "get_object_index_diagnostics": ToolGroup.PERFORMANCE,
    "check_statistics_health": ToolGroup.PERFORMANCE,
    "get_plan_cache_analysis": ToolGroup.PERFORMANCE,
    "get_query_compilation_stats": ToolGroup.PERFORMANCE,
    "detect_parameter_sniffing": ToolGroup.PERFORMANCE,
    "detect_regressed_queries": ToolGroup.PERFORMANCE,
    "get_query_parameter_buckets": ToolGroup.PERFORMANCE,
    "compare_query_plans": ToolGroup.PERFORMANCE,
    "get_forced_plans": ToolGroup.PERFORMANCE,
    "plan_health_review": ToolGroup.PERFORMANCE,
    "plan_enforcer_tick": ToolGroup.PERFORMANCE,
    "review_plan_enforcement": ToolGroup.PERFORMANCE,
    "dry_run_plan_action": ToolGroup.PERFORMANCE,
    "get_active_sessions": ToolGroup.PERFORMANCE,
    # schema: schema comparison & migration
    "capture_schema_snapshot": ToolGroup.SCHEMA,
    "compare_schemas": ToolGroup.SCHEMA,
    "generate_migration_script": ToolGroup.SCHEMA,
    # admin: write/destructive operations (also requires UNRESTRICTED mode)
    "execute_tsql_unrestricted": ToolGroup.ADMIN,
    "rebuild_index": ToolGroup.ADMIN,
    "update_statistics": ToolGroup.ADMIN,
    "force_query_plan": ToolGroup.ADMIN,
    "set_query_store_hints": ToolGroup.ADMIN,
    "clear_query_store_hints": ToolGroup.ADMIN,
    "create_test_index": ToolGroup.ADMIN,
    "drop_test_index": ToolGroup.ADMIN,
    "apply_plan_action": ToolGroup.ADMIN,
    "prepare_plan_action": ToolGroup.PERFORMANCE,
    "apply_prepared_plan_action": ToolGroup.ADMIN,
    "verify_plan_action": ToolGroup.ADMIN,
    "rollback_plan_action": ToolGroup.ADMIN,
    "kill_session": ToolGroup.ADMIN,
}


_TUNING_TOOLS = frozenset(
    {
        "tune_query",
        "benchmark_query_rewrite",
        "start_tuning_session",
        "add_tuning_candidate",
        "benchmark_tuning_candidate",
        "finalize_tuning_session",
        "compare_query_results",
        "compare_plan_summaries",
    }
)
_INDEX_WORKFLOW_TOOLS = frozenset({"benchmark_index_candidate"})
_ENFORCER_REVIEW_TOOLS = frozenset(
    {
        "plan_health_review",
        "plan_enforcer_tick",
        "review_plan_enforcement",
        "dry_run_plan_action",
        "prepare_plan_action",
    }
)
_ENFORCER_APPLY_TOOLS = frozenset(
    {"apply_prepared_plan_action", "verify_plan_action", "rollback_plan_action"}
)
_DIRECT_ADMIN_TOOLS = frozenset(
    {
        "apply_plan_action",
        "force_query_plan",
        "set_query_store_hints",
        "clear_query_store_hints",
        "create_test_index",
        "drop_test_index",
    }
)


class TransportMode(str, Enum):
    STDIO = "stdio"
    SSE = "sse"
    STREAMABLE_HTTP = "streamable-http"


@dataclass(frozen=True)
class TransportConfig:
    mode: TransportMode
    host: str
    port: int


@dataclass(frozen=True)
class ServerConfig:
    server: str
    default_database: str
    allowed_databases: tuple[str, ...]
    auth_mode: AuthMode
    access_mode: AccessMode
    query_timeout_seconds: int
    row_limit: int
    pool_size: int
    max_retries: int
    tool_timeout_seconds: int
    log_format: str
    username: str | None
    password: str | None
    trust_server_certificate: bool
    tenant_id: str | None
    client_id: str | None
    client_secret: str | None
    transport: TransportConfig
    tool_groups: frozenset[ToolGroup]
    log_level: str
    mcp_bearer_token: str | None
    write_policy: WritePolicy
    audit_dir: str
    audit_full_sql: bool
    remote_admin_enabled: bool
    profile: McpProfile | None = None
    database_policy_file: str | None = None
    performance_state_dir: str = "~/.azure-sql-mcp/state"
    plan_apply_kill_switch: bool = True

    def validate_database_name(self, database_name: str | None) -> str:
        """Resolve a database against the allowlist, case-insensitively.

        Azure SQL database names are case-insensitive; the canonical allowlist
        spelling is returned so downstream code sees one consistent name.
        """
        candidate = (database_name or self.default_database).strip()
        for allowed in self.allowed_databases:
            if candidate.casefold() == allowed.casefold():
                return allowed
        raise ValueError(
            f"Database '{candidate}' is not in AZURE_SQL_ALLOWED_DATABASES."
        )

    def is_tool_enabled(self, tool_name: str) -> bool:
        """Check whether a tool should be registered based on configured tool_groups."""
        group = TOOL_GROUPS.get(tool_name)
        if self.profile is not None:
            if tool_name in _DIRECT_ADMIN_TOOLS:
                return False
            if tool_name in _TUNING_TOOLS and self.profile not in {
                McpProfile.OPTIMIZER,
                McpProfile.SANDBOX,
            }:
                return False
            if (
                tool_name in _INDEX_WORKFLOW_TOOLS
                and self.profile != McpProfile.SANDBOX
            ):
                return False
            if tool_name in _ENFORCER_REVIEW_TOOLS and self.profile not in {
                McpProfile.ENFORCER_REVIEW,
                McpProfile.ENFORCER_APPLY,
            }:
                return False
            if (
                tool_name in _ENFORCER_APPLY_TOOLS
                and self.profile != McpProfile.ENFORCER_APPLY
            ):
                return False
            if group == ToolGroup.ADMIN and tool_name not in _ENFORCER_APPLY_TOOLS:
                return False
        if group is None:
            # Unknown tools are always registered
            return True
        if group == ToolGroup.ADMIN and self.access_mode != AccessMode.UNRESTRICTED:
            return False
        if (
            group == ToolGroup.ADMIN
            and self.transport.mode != TransportMode.STDIO
            and not self.remote_admin_enabled
        ):
            return False
        if ToolGroup.ALL in self.tool_groups:
            return True
        return group in self.tool_groups


def parse_csv(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def env_or_arg(namespace: argparse.Namespace, name: str, default: str | None = None) -> str | None:
    value = getattr(namespace, name)
    if value is not None:
        return value
    env_name = name.upper()
    return os.getenv(env_name, default)


def positive_int(raw_value: str | None, default: int, field_name: str) -> int:
    if raw_value is None or raw_value == "":
        return default
    value = int(raw_value)
    if value <= 0:
        raise ValueError(f"{field_name} must be greater than 0.")
    return value


def parse_bool(raw_value: str | None, default: bool = False) -> bool:
    if raw_value is None or raw_value == "":
        return default
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {raw_value!r}.")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Azure SQL Database MCP Server")
    parser.add_argument("--azure-sql-server", dest="azure_sql_server")
    parser.add_argument("--azure-sql-default-database", dest="azure_sql_default_database")
    parser.add_argument("--azure-sql-allowed-databases", dest="azure_sql_allowed_databases")
    parser.add_argument(
        "--azure-sql-auth-mode",
        dest="azure_sql_auth_mode",
        choices=[mode.value for mode in AuthMode],
    )
    parser.add_argument(
        "--azure-sql-access-mode",
        dest="azure_sql_access_mode",
        choices=[mode.value for mode in AccessMode],
    )
    parser.add_argument("--azure-sql-query-timeout-seconds", dest="azure_sql_query_timeout_seconds")
    parser.add_argument("--azure-sql-row-limit", dest="azure_sql_row_limit")
    parser.add_argument("--azure-sql-pool-size", dest="azure_sql_pool_size")
    parser.add_argument("--azure-sql-max-retries", dest="azure_sql_max_retries")
    parser.add_argument("--azure-sql-tool-timeout-seconds", dest="azure_sql_tool_timeout_seconds")
    parser.add_argument(
        "--log-format",
        default=os.getenv("AZURE_SQL_LOG_FORMAT", "text"),
        choices=["text", "json"],
    )
    parser.add_argument("--azure-sql-username", dest="azure_sql_username")
    parser.add_argument("--azure-sql-password", dest="azure_sql_password")
    parser.add_argument(
        "--azure-sql-trust-server-certificate",
        dest="azure_sql_trust_server_certificate",
    )
    parser.add_argument("--azure-tenant-id", dest="azure_tenant_id")
    parser.add_argument("--azure-client-id", dest="azure_client_id")
    parser.add_argument("--azure-client-secret", dest="azure_client_secret")
    parser.add_argument("--azure-sql-mcp-bearer-token", dest="azure_sql_mcp_bearer_token")
    parser.add_argument(
        "--azure-sql-write-policy",
        dest="azure_sql_write_policy",
        choices=[policy.value for policy in WritePolicy],
    )
    parser.add_argument("--azure-sql-audit-dir", dest="azure_sql_audit_dir")
    parser.add_argument("--azure-sql-audit-full-sql", dest="azure_sql_audit_full_sql")
    parser.add_argument("--azure-sql-enable-remote-admin", dest="azure_sql_enable_remote_admin")
    parser.add_argument(
        "--transport",
        choices=[mode.value for mode in TransportMode],
        default=os.getenv("AZURE_SQL_TRANSPORT", TransportMode.STDIO.value),
    )
    parser.add_argument("--host", default=os.getenv("AZURE_SQL_HOST", "127.0.0.1"))
    parser.add_argument("--port", default=os.getenv("AZURE_SQL_PORT", "8000"))
    parser.add_argument("--log-level", default=os.getenv("AZURE_SQL_LOG_LEVEL", "INFO"))
    parser.add_argument(
        "--azure-sql-tool-groups",
        dest="azure_sql_tool_groups",
        help="Comma-separated tool groups to expose: core,performance,schema,admin,all (default: all)",
    )
    parser.add_argument(
        "--azure-sql-profile",
        dest="azure_sql_profile",
        choices=[profile.value for profile in McpProfile],
    )
    parser.add_argument(
        "--azure-sql-database-policy-file",
        dest="azure_sql_database_policy_file",
    )
    parser.add_argument(
        "--azure-sql-performance-state-dir",
        dest="azure_sql_performance_state_dir",
    )
    parser.add_argument(
        "--azure-sql-plan-apply-kill-switch",
        dest="azure_sql_plan_apply_kill_switch",
    )
    return parser


def load_server_config(argv: list[str] | None = None) -> ServerConfig:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    server = env_or_arg(args, "azure_sql_server")
    default_database = env_or_arg(args, "azure_sql_default_database")
    allowed_databases = parse_csv(env_or_arg(args, "azure_sql_allowed_databases"))
    auth_mode = AuthMode(
        env_or_arg(args, "azure_sql_auth_mode", AuthMode.ENTRA_DEFAULT.value)
    )
    access_mode = AccessMode(
        env_or_arg(args, "azure_sql_access_mode", AccessMode.RESTRICTED.value)
    )
    query_timeout_seconds = positive_int(
        env_or_arg(args, "azure_sql_query_timeout_seconds"),
        default=30,
        field_name="AZURE_SQL_QUERY_TIMEOUT_SECONDS",
    )
    row_limit = positive_int(
        env_or_arg(args, "azure_sql_row_limit"),
        default=200,
        field_name="AZURE_SQL_ROW_LIMIT",
    )
    pool_size = positive_int(
        env_or_arg(args, "azure_sql_pool_size"),
        default=5,
        field_name="AZURE_SQL_POOL_SIZE",
    )
    max_retries = int(env_or_arg(args, "azure_sql_max_retries") or "3")
    if max_retries < 0:
        raise ValueError("AZURE_SQL_MAX_RETRIES must be >= 0.")
    tool_timeout_raw = env_or_arg(args, "azure_sql_tool_timeout_seconds")
    tool_timeout_seconds = (
        positive_int(tool_timeout_raw, default=0, field_name="AZURE_SQL_TOOL_TIMEOUT_SECONDS")
        if tool_timeout_raw
        else query_timeout_seconds + 15
    )
    if tool_timeout_seconds < query_timeout_seconds:
        raise ValueError(
            "AZURE_SQL_TOOL_TIMEOUT_SECONDS must be >= AZURE_SQL_QUERY_TIMEOUT_SECONDS: "
            "the outer tool timeout would cancel every query before its own timeout."
        )

    if not server:
        raise ValueError("AZURE_SQL_SERVER is required.")
    if not default_database:
        raise ValueError("AZURE_SQL_DEFAULT_DATABASE is required.")
    if not allowed_databases:
        raise ValueError("AZURE_SQL_ALLOWED_DATABASES must contain at least one database.")
    if default_database not in allowed_databases:
        raise ValueError("AZURE_SQL_DEFAULT_DATABASE must be included in AZURE_SQL_ALLOWED_DATABASES.")

    username = env_or_arg(args, "azure_sql_username")
    password = env_or_arg(args, "azure_sql_password")
    trust_server_certificate = parse_bool(
        env_or_arg(args, "azure_sql_trust_server_certificate")
    )
    tenant_id = env_or_arg(args, "azure_tenant_id")
    client_id = env_or_arg(args, "azure_client_id")
    client_secret = env_or_arg(args, "azure_client_secret")

    if auth_mode == AuthMode.SQL_PASSWORD and (not username or not password):
        raise ValueError(
            "AZURE_SQL_USERNAME and AZURE_SQL_PASSWORD are required for sql-password auth."
        )
    if auth_mode == AuthMode.SERVICE_PRINCIPAL and (
        not tenant_id or not client_id or not client_secret
    ):
        raise ValueError(
            "AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET are required for service-principal auth."
        )

    transport = TransportConfig(
        mode=TransportMode(args.transport),
        host=args.host,
        port=positive_int(args.port, default=8000, field_name="port"),
    )

    mcp_bearer_token = env_or_arg(args, "azure_sql_mcp_bearer_token")
    if transport.mode != TransportMode.STDIO and not mcp_bearer_token:
        raise ValueError(
            "AZURE_SQL_MCP_BEARER_TOKEN is required for sse and streamable-http transports."
        )

    log_format = args.log_format.lower()
    if log_format not in {"text", "json"}:
        raise ValueError("AZURE_SQL_LOG_FORMAT must be 'text' or 'json'.")

    tool_groups_raw = env_or_arg(args, "azure_sql_tool_groups") or "all"
    tool_groups = frozenset(
        ToolGroup(g.strip().lower())
        for g in tool_groups_raw.split(",")
        if g.strip()
    )
    write_policy_raw = env_or_arg(args, "azure_sql_write_policy")
    if access_mode == AccessMode.RESTRICTED:
        write_policy = WritePolicy.DISABLED
    elif write_policy_raw:
        write_policy = WritePolicy(write_policy_raw)
    else:
        write_policy = WritePolicy.REVIEW
    audit_dir = (
        env_or_arg(args, "azure_sql_audit_dir")
        or os.path.expanduser("~/.azure-sql-mcp/audit")
    )
    audit_full_sql = parse_bool(env_or_arg(args, "azure_sql_audit_full_sql"))
    remote_admin_enabled = parse_bool(env_or_arg(args, "azure_sql_enable_remote_admin"))
    profile_raw = env_or_arg(args, "azure_sql_profile")
    profile = McpProfile(profile_raw) if profile_raw else None
    database_policy_file = env_or_arg(args, "azure_sql_database_policy_file")
    performance_state_dir = (
        env_or_arg(
            args,
            "azure_sql_performance_state_dir",
            "~/.azure-sql-mcp/state",
        )
        or "~/.azure-sql-mcp/state"
    )
    plan_apply_kill_switch = parse_bool(
        env_or_arg(args, "azure_sql_plan_apply_kill_switch"),
        default=True,
    )
    read_only_profiles = {
        McpProfile.TRIAGE,
        McpProfile.OPTIMIZER,
        McpProfile.ENFORCER_REVIEW,
    }
    write_profiles = {McpProfile.SANDBOX, McpProfile.ENFORCER_APPLY}
    if (
        profile is not None
        and profile in read_only_profiles
        and access_mode != AccessMode.RESTRICTED
    ):
        raise ValueError(f"AZURE_SQL_PROFILE={profile.value} requires restricted access mode.")
    if profile is not None and profile in write_profiles:
        if transport.mode != TransportMode.STDIO:
            raise ValueError(f"AZURE_SQL_PROFILE={profile.value} is local stdio only.")
        if access_mode != AccessMode.UNRESTRICTED:
            raise ValueError(f"AZURE_SQL_PROFILE={profile.value} requires unrestricted access mode.")
        if write_policy != WritePolicy.APPLY:
            raise ValueError(f"AZURE_SQL_PROFILE={profile.value} requires write policy apply.")
    if (
        transport.mode != TransportMode.STDIO
        and not remote_admin_enabled
        and write_policy == WritePolicy.APPLY
    ):
        raise ValueError(
            "AZURE_SQL_ENABLE_REMOTE_ADMIN=1 is required to use "
            "AZURE_SQL_WRITE_POLICY=apply over sse or streamable-http transports."
        )

    return ServerConfig(
        server=server,
        default_database=default_database,
        allowed_databases=allowed_databases,
        auth_mode=auth_mode,
        access_mode=access_mode,
        query_timeout_seconds=query_timeout_seconds,
        row_limit=row_limit,
        pool_size=pool_size,
        max_retries=max_retries,
        tool_timeout_seconds=tool_timeout_seconds,
        log_format=log_format,
        username=username,
        password=password,
        trust_server_certificate=trust_server_certificate,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        transport=transport,
        tool_groups=tool_groups,
        log_level=args.log_level.upper(),
        mcp_bearer_token=mcp_bearer_token,
        write_policy=write_policy,
        audit_dir=audit_dir,
        audit_full_sql=audit_full_sql,
        remote_admin_enabled=remote_admin_enabled,
        profile=profile,
        database_policy_file=database_policy_file,
        performance_state_dir=performance_state_dir,
        plan_apply_kill_switch=plan_apply_kill_switch,
    )
