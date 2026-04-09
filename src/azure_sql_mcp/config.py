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


class ToolGroup(str, Enum):
    CORE = "core"
    PERFORMANCE = "performance"
    SCHEMA = "schema"
    ADMIN = "admin"
    ALL = "all"


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
    "check_statistics_health": ToolGroup.PERFORMANCE,
    "get_plan_cache_analysis": ToolGroup.PERFORMANCE,
    "get_query_compilation_stats": ToolGroup.PERFORMANCE,
    "detect_parameter_sniffing": ToolGroup.PERFORMANCE,
    "detect_regressed_queries": ToolGroup.PERFORMANCE,
    "compare_query_plans": ToolGroup.PERFORMANCE,
    "get_forced_plans": ToolGroup.PERFORMANCE,
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
    "kill_session": ToolGroup.ADMIN,
}


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
    tenant_id: str | None
    client_id: str | None
    client_secret: str | None
    transport: TransportConfig
    tool_groups: frozenset[ToolGroup]
    log_level: str

    def validate_database_name(self, database_name: str | None) -> str:
        candidate = (database_name or self.default_database).strip()
        if candidate not in self.allowed_databases:
            raise ValueError(
                f"Database '{candidate}' is not in AZURE_SQL_ALLOWED_DATABASES."
            )
        return candidate

    def is_tool_enabled(self, tool_name: str) -> bool:
        """Check whether a tool should be registered based on configured tool_groups."""
        if ToolGroup.ALL in self.tool_groups:
            return True
        group = TOOL_GROUPS.get(tool_name)
        if group is None:
            # Unknown tools are always registered
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
    parser.add_argument("--azure-tenant-id", dest="azure_tenant_id")
    parser.add_argument("--azure-client-id", dest="azure_client_id")
    parser.add_argument("--azure-client-secret", dest="azure_client_secret")
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

    log_format = args.log_format.lower()
    if log_format not in {"text", "json"}:
        raise ValueError("AZURE_SQL_LOG_FORMAT must be 'text' or 'json'.")

    tool_groups_raw = env_or_arg(args, "azure_sql_tool_groups") or "all"
    tool_groups = frozenset(
        ToolGroup(g.strip().lower())
        for g in tool_groups_raw.split(",")
        if g.strip()
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
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        transport=transport,
        tool_groups=tool_groups,
        log_level=args.log_level.upper(),
    )
