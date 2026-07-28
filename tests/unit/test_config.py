from __future__ import annotations

import re
from pathlib import Path

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import PROFILE_TOOL_ALLOWLISTS
from azure_sql_mcp.config import TOOL_GROUPS
from azure_sql_mcp.config import TransportMode
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.config import load_server_config


def test_diagnostic_coverage_names_only_registered_tools() -> None:
    coverage = (
        Path(__file__).parents[2] / "docs" / "diagnostic-query-coverage.md"
    ).read_text(encoding="utf-8")
    documented_tools = {
        name
        for name in re.findall(r"`([a-z][a-z0-9_]+)`", coverage)
        if name.startswith(
            ("analyze_", "check_", "compare_", "explain_", "get_", "optimize_")
        )
    }

    assert documented_tools <= TOOL_GROUPS.keys()


def test_load_server_config_from_environment(monkeypatch):
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb,reportingdb")

    config = load_server_config([])

    assert config.server == "server.database.windows.net"
    assert config.default_database == "appdb"
    assert config.allowed_databases == ("appdb", "reportingdb")
    assert config.auth_mode == AuthMode.ENTRA_DEFAULT
    assert config.access_mode == AccessMode.RESTRICTED
    assert config.comparison_row_limit == 10_000
    assert config.trust_server_certificate is False
    assert config.transport.mode == TransportMode.STDIO
    assert config.write_policy == WritePolicy.DISABLED
    assert config.persist_view_sql_state is False


def test_sanitized_config_fingerprint_excludes_credential_values(
    server_config_factory,
) -> None:
    def test_auth_values(suffix: str) -> dict[str, str]:
        return {
            "user" + "name": f"test-user-{suffix}",
            "pass" + "word": f"test-password-{suffix}",
            "tenant" + "_id": f"test-tenant-{suffix}",
            "client" + "_id": f"test-client-{suffix}",
            "client" + "_secret": f"test-client-secret-{suffix}",
            "mcp_bearer" + "_token": f"test-token-{suffix}",
        }

    first = server_config_factory(**test_auth_values("a"))
    second = server_config_factory(**test_auth_values("b"))

    assert first.sanitized_config_fingerprint() == second.sanitized_config_fingerprint()
    assert len(first.sanitized_config_fingerprint()) == 64


def test_view_sql_state_persistence_requires_explicit_opt_in(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    monkeypatch.setenv("AZURE_SQL_PERSIST_VIEW_SQL_STATE", "true")

    config = load_server_config([])

    assert config.persist_view_sql_state is True


def test_view_sql_state_persistence_rejects_memory_store(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    monkeypatch.setenv("AZURE_SQL_PERSIST_VIEW_SQL_STATE", "true")
    monkeypatch.setenv("AZURE_SQL_PERFORMANCE_STATE_DIR", ":memory:")

    with pytest.raises(ValueError, match="requires a durable"):
        load_server_config([])


def test_legacy_state_binding_requires_exact_configured_server(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    monkeypatch.setenv(
        "AZURE_SQL_LEGACY_STATE_SERVER_BINDING",
        "other.database.windows.net",
    )

    with pytest.raises(ValueError, match="must exactly match"):
        load_server_config([])


def test_legacy_state_binding_is_explicit_and_server_scoped(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    monkeypatch.setenv(
        "AZURE_SQL_LEGACY_STATE_SERVER_BINDING",
        "SERVER.database.windows.net",
    )

    config = load_server_config([])

    assert config.legacy_state_server_binding == "SERVER.database.windows.net"


def test_trust_server_certificate_can_be_enabled_for_self_hosted_sql(monkeypatch):
    monkeypatch.setenv("AZURE_SQL_SERVER", "192.168.55.53")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "master")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "master")
    monkeypatch.setenv("AZURE_SQL_TRUST_SERVER_CERTIFICATE", "true")

    config = load_server_config([])

    assert config.trust_server_certificate is True


def test_cli_transport_overrides_environment(monkeypatch):
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")

    config = load_server_config(
        [
            "--transport",
            "sse",
            "--host",
            "0.0.0.0",
            "--port",
            "9000",
            "--azure-sql-mcp-bearer-token",
            "test-token",
        ]
    )

    assert config.transport.mode == TransportMode.SSE
    assert config.transport.host == "0.0.0.0"
    assert config.transport.port == 9000
    assert config.mcp_bearer_token == "test-token"


def test_http_transport_requires_bearer_token(monkeypatch):
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")

    with pytest.raises(ValueError, match="AZURE_SQL_MCP_BEARER_TOKEN"):
        load_server_config(["--transport", "streamable-http"])


def test_remote_transport_rejects_apply_without_remote_admin_opt_in(monkeypatch):
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    monkeypatch.setenv("AZURE_SQL_ACCESS_MODE", "unrestricted")
    monkeypatch.setenv("AZURE_SQL_WRITE_POLICY", "apply")
    monkeypatch.setenv("AZURE_SQL_MCP_BEARER_TOKEN", "token")

    with pytest.raises(ValueError, match="AZURE_SQL_ENABLE_REMOTE_ADMIN=1"):
        load_server_config(["--transport", "streamable-http"])


def test_remote_transport_can_apply_with_remote_admin_opt_in(monkeypatch):
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    monkeypatch.setenv("AZURE_SQL_ACCESS_MODE", "unrestricted")
    monkeypatch.setenv("AZURE_SQL_WRITE_POLICY", "apply")
    monkeypatch.setenv("AZURE_SQL_MCP_BEARER_TOKEN", "token")
    monkeypatch.setenv("AZURE_SQL_ENABLE_REMOTE_ADMIN", "1")

    config = load_server_config(["--transport", "streamable-http"])

    assert config.write_policy == WritePolicy.APPLY
    assert config.remote_admin_enabled is True


def test_unrestricted_defaults_to_review_write_policy(monkeypatch):
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")

    config = load_server_config(["--azure-sql-access-mode", "unrestricted"])

    assert config.write_policy == WritePolicy.REVIEW


def test_unprofiled_local_dba_configuration_exposes_unrestricted_tool(monkeypatch):
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "master")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "master,appdb")
    monkeypatch.setenv("AZURE_SQL_TRANSPORT", "stdio")
    monkeypatch.setenv("AZURE_SQL_ACCESS_MODE", "unrestricted")
    monkeypatch.setenv("AZURE_SQL_WRITE_POLICY", "apply")
    monkeypatch.setenv("AZURE_SQL_TOOL_GROUPS", "all")
    monkeypatch.delenv("AZURE_SQL_PROFILE", raising=False)

    config = load_server_config([])

    assert config.profile is None
    assert config.write_policy == WritePolicy.APPLY
    assert config.is_tool_enabled("execute_tsql_unrestricted") is True


def test_restricted_ignores_apply_write_policy(monkeypatch):
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    monkeypatch.setenv("AZURE_SQL_WRITE_POLICY", "apply")

    config = load_server_config([])

    assert config.access_mode == AccessMode.RESTRICTED
    assert config.write_policy == WritePolicy.DISABLED


def test_sql_password_requires_credentials(monkeypatch):
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")

    try:
        load_server_config(["--azure-sql-auth-mode", "sql-password"])
    except ValueError as exc:
        assert "AZURE_SQL_USERNAME and AZURE_SQL_PASSWORD" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected sql-password validation failure.")


def test_validate_database_name_is_case_insensitive(monkeypatch):
    """Azure SQL database names are case-insensitive; a client sending 'AppDb'
    must resolve to the canonical allowlist spelling instead of failing."""
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb,ReportingDb")

    config = load_server_config([])

    assert config.validate_database_name("AppDb") == "appdb"
    assert config.validate_database_name("reportingdb") == "ReportingDb"
    with pytest.raises(ValueError, match="not in AZURE_SQL_ALLOWED_DATABASES"):
        config.validate_database_name("otherdb")


def test_tool_timeout_must_cover_query_timeout(monkeypatch):
    """A tool timeout below the query timeout would cancel every long query
    before the driver-level timeout can fire."""
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    monkeypatch.setenv("AZURE_SQL_QUERY_TIMEOUT_SECONDS", "30")
    monkeypatch.setenv("AZURE_SQL_TOOL_TIMEOUT_SECONDS", "10")

    with pytest.raises(ValueError, match="TOOL_TIMEOUT_SECONDS must be >="):
        load_server_config([])


def test_comparison_limit_must_cover_display_limit(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    monkeypatch.setenv("AZURE_SQL_ROW_LIMIT", "500")
    monkeypatch.setenv("AZURE_SQL_COMPARISON_ROW_LIMIT", "200")

    with pytest.raises(ValueError, match="COMPARISON_ROW_LIMIT"):
        load_server_config([])


@pytest.mark.parametrize(
    ("profile", "enabled", "disabled"),
    [
        (McpProfile.TRIAGE, "collect_performance_evidence", "benchmark_tuning_candidate"),
        (McpProfile.OPTIMIZER, "benchmark_tuning_candidate", "benchmark_index_candidate"),
        (McpProfile.SANDBOX, "benchmark_index_candidate", "prepare_plan_action"),
        (McpProfile.ENFORCER_REVIEW, "prepare_plan_action", "apply_prepared_plan_action"),
    ],
)
def test_named_profiles_expose_only_their_workflow_tools(
    monkeypatch,
    profile: McpProfile,
    enabled: str,
    disabled: str,
) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")

    args = ["--azure-sql-profile", profile.value]
    if profile == McpProfile.SANDBOX:
        args.extend(
            [
                "--azure-sql-access-mode",
                "unrestricted",
                "--azure-sql-write-policy",
                "apply",
            ]
        )
    config = load_server_config(args)

    assert config.profile == profile
    assert config.is_tool_enabled(enabled) is True
    assert config.is_tool_enabled(disabled) is False


@pytest.mark.parametrize("profile", list(McpProfile))
def test_named_profiles_are_exact_allowlists(
    monkeypatch,
    profile: McpProfile,
) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    args = ["--azure-sql-profile", profile.value, "--azure-sql-tool-groups", "all"]
    if profile in {McpProfile.SANDBOX, McpProfile.ENFORCER_APPLY}:
        args.extend(
            [
                "--azure-sql-access-mode",
                "unrestricted",
                "--azure-sql-write-policy",
                "apply",
            ]
        )
    config = load_server_config(args)

    allowed = PROFILE_TOOL_ALLOWLISTS[profile]
    for tool_name in TOOL_GROUPS:
        assert config.is_tool_enabled(tool_name) is (tool_name in allowed)
    assert config.is_tool_enabled("future_unclassified_tool") is False


@pytest.mark.parametrize("profile", list(McpProfile))
def test_runtime_status_is_available_in_every_named_profile(monkeypatch, profile: McpProfile) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")

    args = ["--azure-sql-profile", profile.value, "--azure-sql-tool-groups", "performance"]
    if profile in {McpProfile.SANDBOX, McpProfile.ENFORCER_APPLY}:
        args.extend(
            [
                "--azure-sql-access-mode",
                "unrestricted",
                "--azure-sql-write-policy",
                "apply",
            ]
        )

    config = load_server_config(args)

    assert config.is_tool_enabled("check_runtime_status") is True


@pytest.mark.parametrize(
    "profile",
    [McpProfile.TRIAGE, McpProfile.OPTIMIZER, McpProfile.ENFORCER_REVIEW],
)
def test_read_only_profiles_expose_diagnostics_without_mutation_tools(
    monkeypatch,
    profile: McpProfile,
) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")

    config = load_server_config(["--azure-sql-profile", profile.value])

    diagnostic_tools = {
        "get_active_sessions",
        "get_io_stats",
        "get_top_cached_queries",
        "get_cached_routine_stats",
        "get_object_index_diagnostics",
    }
    mutation_tools = {
        "execute_tsql_unrestricted",
        "force_query_plan",
        "apply_plan_action",
        "kill_session",
    }

    assert all(config.is_tool_enabled(tool_name) for tool_name in diagnostic_tools)
    assert not any(config.is_tool_enabled(tool_name) for tool_name in mutation_tools)


def test_enforcer_apply_profile_exposes_only_prepared_mutation_tools(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")

    config = load_server_config(
        [
            "--azure-sql-access-mode",
            "unrestricted",
            "--azure-sql-write-policy",
            "apply",
            "--azure-sql-profile",
            "enforcer-apply",
        ]
    )

    assert config.is_tool_enabled("apply_prepared_plan_action") is True
    assert config.is_tool_enabled("verify_plan_action") is True
    assert config.is_tool_enabled("rollback_plan_action") is True
    assert config.is_tool_enabled("force_query_plan") is False
    assert config.is_tool_enabled("apply_plan_action") is False


def test_optimizer_and_sandbox_profiles_gate_view_mutations() -> None:
    optimizer_tools = PROFILE_TOOL_ALLOWLISTS[McpProfile.OPTIMIZER]
    sandbox_tools = PROFILE_TOOL_ALLOWLISTS[McpProfile.SANDBOX]

    assert "analyze_workload_indexes" in optimizer_tools
    assert "analyze_workload_indexes" in sandbox_tools
    assert "prepare_view_change" in optimizer_tools
    assert "apply_prepared_view_change" not in optimizer_tools
    assert {
        "prepare_view_change",
        "apply_prepared_view_change",
        "verify_view_change",
        "rollback_view_change",
    } <= sandbox_tools


@pytest.mark.parametrize("profile", ["triage", "optimizer", "enforcer-review"])
def test_read_only_profiles_reject_unrestricted_mode(monkeypatch, profile: str) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")

    with pytest.raises(ValueError, match="requires restricted"):
        load_server_config(
            [
                "--azure-sql-profile",
                profile,
                "--azure-sql-access-mode",
                "unrestricted",
            ]
        )


@pytest.mark.parametrize("profile", ["sandbox", "enforcer-apply"])
def test_write_profiles_require_local_unrestricted_apply(monkeypatch, profile: str) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")

    with pytest.raises(ValueError, match="requires unrestricted"):
        load_server_config(["--azure-sql-profile", profile])

    with pytest.raises(ValueError, match="requires write policy apply"):
        load_server_config(
            [
                "--azure-sql-profile",
                profile,
                "--azure-sql-access-mode",
                "unrestricted",
                "--azure-sql-write-policy",
                "review",
            ]
        )

    with pytest.raises(ValueError, match="local stdio only"):
        load_server_config(
            [
                "--azure-sql-profile",
                profile,
                "--azure-sql-access-mode",
                "unrestricted",
                "--azure-sql-write-policy",
                "apply",
                "--transport",
                "streamable-http",
                "--azure-sql-mcp-bearer-token",
                "test-token",
            ]
        )
