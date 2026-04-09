from __future__ import annotations

import re
import struct
from dataclasses import dataclass

from azure.identity import ClientSecretCredential
from azure.identity import DefaultAzureCredential
from azure.identity import InteractiveBrowserCredential

from .config import AuthMode
from .config import ServerConfig

AZURE_SQL_SCOPE = "https://database.windows.net/.default"
SQL_ACCESS_TOKEN_ATTRIBUTE = 1256


@dataclass(frozen=True)
class ConnectionArguments:
    connection_string: str
    attrs_before: dict[int, bytes] | None = None


def obfuscate_secret(value: str | None) -> str | None:
    if value is None:
        return None
    redacted = re.sub(r"(PWD=)([^;]+)", r"\1****", value, flags=re.IGNORECASE)
    redacted = re.sub(r"(Password=)([^;]+)", r"\1****", redacted, flags=re.IGNORECASE)
    return redacted


class AzureSqlAuthenticator:
    def __init__(self, config: ServerConfig):
        self.config = config
        self._credential = None

    def build_connection_arguments(self, database_name: str) -> ConnectionArguments:
        connection_string = (
            f"SERVER=tcp:{self.config.server},1433;"
            f"DATABASE={database_name};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
        )

        if self.config.auth_mode == AuthMode.SQL_PASSWORD:
            connection_string += (
                f"UID={self.config.username};"
                f"PWD={self.config.password};"
            )
            return ConnectionArguments(connection_string=connection_string)

        token = self.get_access_token()
        token_bytes = token.encode("utf-16-le")
        token_struct = struct.pack(
            f"<I{len(token_bytes)}s",
            len(token_bytes),
            token_bytes,
        )
        return ConnectionArguments(
            connection_string=connection_string,
            attrs_before={SQL_ACCESS_TOKEN_ATTRIBUTE: token_struct},
        )

    def get_access_token(self) -> str:
        if self.config.auth_mode == AuthMode.SQL_PASSWORD:
            raise ValueError("SQL password auth mode does not use access tokens.")

        credential = self._get_or_create_credential()
        token = credential.get_token(AZURE_SQL_SCOPE)
        return token.token

    def _get_or_create_credential(self):
        if self._credential is not None:
            return self._credential

        if self.config.auth_mode == AuthMode.ENTRA_DEFAULT:
            self._credential = DefaultAzureCredential()
        elif self.config.auth_mode == AuthMode.INTERACTIVE:
            self._credential = InteractiveBrowserCredential(
                tenant_id=self.config.tenant_id,
                client_id=self.config.client_id,
            )
        elif self.config.auth_mode == AuthMode.SERVICE_PRINCIPAL:
            self._credential = ClientSecretCredential(
                tenant_id=self.config.tenant_id or "",
                client_id=self.config.client_id or "",
                client_secret=self.config.client_secret or "",
            )
        else:
            raise ValueError(f"Unsupported auth mode: {self.config.auth_mode}")

        return self._credential
