from __future__ import annotations

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import AuthMode
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
    assert config.transport.mode == TransportMode.STDIO
    assert config.write_policy == WritePolicy.DISABLED


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
