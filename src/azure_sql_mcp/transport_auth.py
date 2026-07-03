from __future__ import annotations

import hmac

from mcp.server.auth.provider import AccessToken
from mcp.server.auth.provider import TokenVerifier


class StaticBearerTokenVerifier(TokenVerifier):
    """Minimal bearer verifier for MCP HTTP transports behind a shared secret."""

    def __init__(self, expected_token: str):
        self._expected_token = expected_token

    async def verify_token(self, token: str) -> AccessToken | None:
        if not hmac.compare_digest(token, self._expected_token):
            return None
        return AccessToken(
            token=token,
            client_id="azure-sql-mcp-static-bearer",
            scopes=["azure-sql-mcp"],
        )
