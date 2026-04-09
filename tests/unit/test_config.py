from __future__ import annotations

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.config import TransportMode
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


def test_cli_transport_overrides_environment(monkeypatch):
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")

    config = load_server_config(
        ["--transport", "sse", "--host", "0.0.0.0", "--port", "9000"]
    )

    assert config.transport.mode == TransportMode.SSE
    assert config.transport.host == "0.0.0.0"
    assert config.transport.port == 9000


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

