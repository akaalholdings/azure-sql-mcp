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
APPLICATION_INTENTS = ("read_write", "read_only")


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


def _odbc_value(value: str) -> str:
    """Quote a connection-string value that ODBC would otherwise split.

    A ';' ends an attribute and a leading '{' starts a quoted value, so such a
    value goes in braces with each '}' doubled. Other values are unchanged.
    """

    if not any(character in value for character in ";{}") and value == value.strip():
        return value
    return "{" + value.replace("}", "}}") + "}"


class AzureSqlAuthenticator:
    def __init__(self, config: ServerConfig):
        self.config = config
        self._credential = None

    def build_connection_arguments(
        self,
        database_name: str,
        *,
        application_intent: str | None = None,
    ) -> ConnectionArguments:
        """Build the connection string and token for one database.

        ``application_intent="read_only"`` asks Azure SQL for a readable
        replica (read scale-out). A database with no readable replica answers
        on its primary.
        """

        if application_intent is not None and application_intent not in APPLICATION_INTENTS:
            raise ValueError(
                f"application_intent must be one of: {', '.join(APPLICATION_INTENTS)}."
            )
        connection_string = (
            f"SERVER=tcp:{self.config.server},1433;"
            f"DATABASE={database_name};"
            "Encrypt=yes;"
            f"TrustServerCertificate={'yes' if self.config.trust_server_certificate else 'no'};"
        )
        if application_intent == "read_only":
            connection_string += "ApplicationIntent=ReadOnly;"

        if self.config.auth_mode == AuthMode.SQL_PASSWORD:
            connection_string += (
                f"UID={_odbc_value(self.config.username or '')};"
                f"PWD={_odbc_value(self.config.password or '')};"
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
