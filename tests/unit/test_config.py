from __future__ import annotations

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import TransportMode
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.config import load_server_config


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
    assert config.trust_server_certificate is False
    assert config.transport.mode == TransportMode.STDIO
    assert config.write_policy == WritePolicy.DISABLED


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
