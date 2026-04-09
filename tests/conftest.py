from __future__ import annotations

from collections.abc import Callable

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.config import ServerConfig
from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.config import TransportConfig
from azure_sql_mcp.config import TransportMode


@pytest.fixture
def server_config_factory() -> Callable[..., ServerConfig]:
    def factory(**overrides) -> ServerConfig:
        config = {
            "server": "server.database.windows.net",
            "default_database": "appdb",
            "allowed_databases": ("appdb", "reportingdb"),
            "auth_mode": AuthMode.ENTRA_DEFAULT,
            "access_mode": AccessMode.RESTRICTED,
            "query_timeout_seconds": 30,
            "row_limit": 200,
            "pool_size": 5,
            "max_retries": 3,
            "tool_timeout_seconds": 45,
            "log_format": "text",
            "username": None,
            "password": None,
            "tenant_id": None,
            "client_id": None,
            "client_secret": None,
            "transport": TransportConfig(
                mode=TransportMode.STDIO,
                host="127.0.0.1",
                port=8000,
            ),
            "tool_groups": frozenset({ToolGroup.ALL}),
            "log_level": "INFO",
        }
        config.update(overrides)
        return ServerConfig(**config)

    return factory


@pytest.fixture
def sample_server_config(server_config_factory: Callable[..., ServerConfig]) -> ServerConfig:
    return server_config_factory()
