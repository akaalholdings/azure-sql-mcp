from __future__ import annotations

import struct
from dataclasses import dataclass

import pytest

from azure_sql_mcp import auth as auth_module
from azure_sql_mcp.auth import AZURE_SQL_SCOPE
from azure_sql_mcp.auth import SQL_ACCESS_TOKEN_ATTRIBUTE
from azure_sql_mcp.auth import AzureSqlAuthenticator
from azure_sql_mcp.auth import obfuscate_secret
from azure_sql_mcp.config import AuthMode


@dataclass
class FakeAccessToken:
    token: str


class FakeCredential:
    def __init__(self, token: str = "fake-token", **kwargs):
        self.token = token
        self.kwargs = kwargs
        self.scopes: list[str] = []

    def get_token(self, scope: str) -> FakeAccessToken:
        self.scopes.append(scope)
        return FakeAccessToken(self.token)


def _make_authenticator(server_config_factory, **overrides) -> AzureSqlAuthenticator:
    return AzureSqlAuthenticator(server_config_factory(**overrides))


def _decode_token_payload(raw_payload: bytes) -> str:
    encoded_length = struct.unpack("<I", raw_payload[:4])[0]
    token_bytes = raw_payload[4:]
    assert encoded_length == len(token_bytes)
    return token_bytes.decode("utf-16-le")


def test_obfuscate_secret_redacts_password_values():
    assert obfuscate_secret(None) is None
    assert obfuscate_secret("UID=sa;PWD=secret;Password=another;") == "UID=sa;PWD=****;Password=****;"


def test_build_connection_arguments_uses_sql_password_without_token(server_config_factory):
    authenticator = _make_authenticator(
        server_config_factory,
        auth_mode=AuthMode.SQL_PASSWORD,
        username="sa",
        password="P@ssw0rd!",
    )

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("get_access_token should not be called for SQL password auth.")

    authenticator.get_access_token = fail_if_called  # type: ignore[method-assign]

    arguments = authenticator.build_connection_arguments("appdb")

    assert "DATABASE=appdb;" in arguments.connection_string
    assert "UID=sa;" in arguments.connection_string
    assert "PWD=P@ssw0rd!;" in arguments.connection_string
    assert "DRIVER=" not in arguments.connection_string
    assert "Connection Timeout=" not in arguments.connection_string
    assert arguments.attrs_before is None


@pytest.mark.parametrize(
    ("auth_mode", "patch_target", "patch_kwargs"),
    [
        (AuthMode.ENTRA_DEFAULT, "DefaultAzureCredential", {}),
        (
            AuthMode.INTERACTIVE,
            "InteractiveBrowserCredential",
            {"tenant_id": "tenant-1", "client_id": "client-1"},
        ),
        (
            AuthMode.SERVICE_PRINCIPAL,
            "ClientSecretCredential",
            {
                "tenant_id": "tenant-1",
                "client_id": "client-1",
                "client_secret": "secret-1",
            },
        ),
    ],
)
def test_build_connection_arguments_packs_tokens_for_entra_modes(
    monkeypatch,
    server_config_factory,
    auth_mode,
    patch_target,
    patch_kwargs,
):
    created_credentials: list[FakeCredential] = []

    def credential_factory(**kwargs):
        credential = FakeCredential(token=f"{auth_mode.value}-token", **kwargs)
        created_credentials.append(credential)
        return credential

    monkeypatch.setattr(auth_module, patch_target, credential_factory)

    authenticator = _make_authenticator(
        server_config_factory,
        auth_mode=auth_mode,
        **patch_kwargs,
    )
    arguments = authenticator.build_connection_arguments("appdb")

    assert created_credentials
    credential = created_credentials[0]
    assert credential.scopes == [AZURE_SQL_SCOPE]
    assert arguments.attrs_before is not None
    assert SQL_ACCESS_TOKEN_ATTRIBUTE in arguments.attrs_before
    assert _decode_token_payload(arguments.attrs_before[SQL_ACCESS_TOKEN_ATTRIBUTE]) == (
        f"{auth_mode.value}-token"
    )
    assert "SERVER=tcp:server.database.windows.net,1433;" in arguments.connection_string
    assert "DATABASE=appdb;" in arguments.connection_string
    assert "DRIVER=" not in arguments.connection_string
    assert "Connection Timeout=" not in arguments.connection_string


def test_get_access_token_rejects_sql_password_mode(server_config_factory):
    authenticator = _make_authenticator(
        server_config_factory,
        auth_mode=AuthMode.SQL_PASSWORD,
        username="sa",
        password="P@ssw0rd!",
    )

    with pytest.raises(ValueError, match="SQL password auth mode"):
        authenticator.get_access_token()
