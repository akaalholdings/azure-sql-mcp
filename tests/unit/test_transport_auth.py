from __future__ import annotations

import pytest
import httpx

from azure_sql_mcp.config import TransportConfig
from azure_sql_mcp.config import TransportMode
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.transport_auth import StaticBearerTokenVerifier


@pytest.mark.asyncio
async def test_static_bearer_token_verifier_accepts_exact_token() -> None:
    verifier = StaticBearerTokenVerifier("test-token")

    token = await verifier.verify_token("test-token")

    assert token is not None
    assert token.client_id == "azure-sql-mcp-static-bearer"
    assert token.scopes == ["azure-sql-mcp"]


@pytest.mark.asyncio
async def test_static_bearer_token_verifier_rejects_wrong_token() -> None:
    verifier = StaticBearerTokenVerifier("test-token")

    assert await verifier.verify_token("wrong-token") is None


@pytest.mark.asyncio
async def test_streamable_http_app_enforces_bearer_token(server_config_factory) -> None:
    config = server_config_factory(
        mcp_bearer_token="test-token",
        transport=TransportConfig(
            mode=TransportMode.STREAMABLE_HTTP,
            host="127.0.0.1",
            port=8000,
        ),
    )
    app = AzureSqlMcpApplication(config).mcp.streamable_http_app()
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://127.0.0.1:8000",
    ) as client:
        missing = await client.get("/mcp")
        wrong = await client.get("/mcp", headers={"Authorization": "Bearer wrong-token"})
        valid = await client.get(
            "/mcp",
            headers={"Authorization": "Bearer test-token"},
        )

    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert valid.status_code != 401
