from __future__ import annotations

import os
from typing import Iterator

import mssql_python
import pytest

from azure_sql_mcp.auth import ConnectionArguments
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.config import load_server_config
from azure_sql_mcp.server import AzureSqlMcpApplication

pytestmark = pytest.mark.skipif(
    not os.getenv("AZURE_SQL_SERVER"),
    reason="AZURE_SQL_SERVER not set",
)

TEST_SCHEMA_NAME = "mcp_integration"


def _is_local_sql_server(app: AzureSqlMcpApplication) -> bool:
    return (
        app.config.auth_mode == AuthMode.SQL_PASSWORD
        and app.config.server.lower() in {"localhost", "127.0.0.1", "sqlserver"}
    )


def _patch_local_sql_server_connection(app: AzureSqlMcpApplication) -> None:
    if not _is_local_sql_server(app):
        return

    authenticator = app.executor.authenticator
    original = authenticator.build_connection_arguments

    def build_connection_arguments(database_name: str) -> ConnectionArguments:
        arguments = original(database_name)
        connection_string = arguments.connection_string.replace(
            "Encrypt=yes;",
            "Encrypt=no;",
        ).replace(
            "TrustServerCertificate=no;",
            "TrustServerCertificate=yes;",
        )
        return ConnectionArguments(
            connection_string=connection_string,
            attrs_before=arguments.attrs_before,
        )

    authenticator.build_connection_arguments = build_connection_arguments  # type: ignore[method-assign]


def _connect(app: AzureSqlMcpApplication, database_name: str):
    arguments = app.executor.authenticator.build_connection_arguments(database_name)
    return mssql_python.connect(
        arguments.connection_string,
        attrs_before=arguments.attrs_before or {},
        autocommit=True,
        timeout=app.config.query_timeout_seconds,
    )


def _prepare_schema(app: AzureSqlMcpApplication, database_name: str) -> None:
    statements = [
        f"IF SCHEMA_ID(N'{TEST_SCHEMA_NAME}') IS NULL EXEC(N'CREATE SCHEMA [{TEST_SCHEMA_NAME}]')",
        f"IF OBJECT_ID(N'[{TEST_SCHEMA_NAME}].[vw_OrderSummary]', N'V') IS NOT NULL DROP VIEW [{TEST_SCHEMA_NAME}].[vw_OrderSummary]",
        f"IF OBJECT_ID(N'[{TEST_SCHEMA_NAME}].[usp_GetOrders]', N'P') IS NOT NULL DROP PROCEDURE [{TEST_SCHEMA_NAME}].[usp_GetOrders]",
        f"IF OBJECT_ID(N'[{TEST_SCHEMA_NAME}].[Orders]', N'U') IS NOT NULL DROP TABLE [{TEST_SCHEMA_NAME}].[Orders]",
        f"IF OBJECT_ID(N'[{TEST_SCHEMA_NAME}].[Customers]', N'U') IS NOT NULL DROP TABLE [{TEST_SCHEMA_NAME}].[Customers]",
        (
            f"CREATE TABLE [{TEST_SCHEMA_NAME}].[Customers] ("
            "[CustomerId] INT NOT NULL PRIMARY KEY, "
            "[CustomerName] NVARCHAR(100) NOT NULL)"
        ),
        (
            f"CREATE TABLE [{TEST_SCHEMA_NAME}].[Orders] ("
            "[OrderId] INT NOT NULL PRIMARY KEY, "
            "[CustomerId] INT NOT NULL, "
            "[TotalAmount] DECIMAL(10, 2) NOT NULL, "
            "[CreatedAt] DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(), "
            f"CONSTRAINT [FK_{TEST_SCHEMA_NAME}_Orders_Customers] FOREIGN KEY ([CustomerId]) "
            f"REFERENCES [{TEST_SCHEMA_NAME}].[Customers]([CustomerId]))"
        ),
        f"CREATE INDEX [IX_{TEST_SCHEMA_NAME}_Orders_CustomerId] ON [{TEST_SCHEMA_NAME}].[Orders]([CustomerId])",
        (
            f"INSERT INTO [{TEST_SCHEMA_NAME}].[Customers] ([CustomerId], [CustomerName]) "
            "VALUES (1, N'Alice'), (2, N'Bob')"
        ),
        (
            f"INSERT INTO [{TEST_SCHEMA_NAME}].[Orders] ([OrderId], [CustomerId], [TotalAmount]) "
            "VALUES (100, 1, 25.50), (101, 2, 40.00)"
        ),
        (
            f"CREATE VIEW [{TEST_SCHEMA_NAME}].[vw_OrderSummary] AS "
            f"SELECT o.[OrderId], c.[CustomerName], o.[TotalAmount] "
            f"FROM [{TEST_SCHEMA_NAME}].[Orders] AS o "
            f"INNER JOIN [{TEST_SCHEMA_NAME}].[Customers] AS c ON o.[CustomerId] = c.[CustomerId]"
        ),
        (
            f"CREATE OR ALTER PROCEDURE [{TEST_SCHEMA_NAME}].[usp_GetOrders] "
            "AS BEGIN "
            f"SELECT [OrderId], [CustomerId], [TotalAmount] FROM [{TEST_SCHEMA_NAME}].[Orders] "
            "ORDER BY [OrderId]; "
            "END"
        ),
    ]

    with _connect(app, database_name) as connection:
        cursor = connection.cursor()
        for statement in statements:
            cursor.execute(statement)
        cursor.close()


def _cleanup_schema(app: AzureSqlMcpApplication, database_name: str) -> None:
    statements = [
        f"IF OBJECT_ID(N'[{TEST_SCHEMA_NAME}].[vw_OrderSummary]', N'V') IS NOT NULL DROP VIEW [{TEST_SCHEMA_NAME}].[vw_OrderSummary]",
        f"IF OBJECT_ID(N'[{TEST_SCHEMA_NAME}].[usp_GetOrders]', N'P') IS NOT NULL DROP PROCEDURE [{TEST_SCHEMA_NAME}].[usp_GetOrders]",
        f"IF OBJECT_ID(N'[{TEST_SCHEMA_NAME}].[Orders]', N'U') IS NOT NULL DROP TABLE [{TEST_SCHEMA_NAME}].[Orders]",
        f"IF OBJECT_ID(N'[{TEST_SCHEMA_NAME}].[Customers]', N'U') IS NOT NULL DROP TABLE [{TEST_SCHEMA_NAME}].[Customers]",
    ]

    with _connect(app, database_name) as connection:
        cursor = connection.cursor()
        for statement in statements:
            cursor.execute(statement)
        cursor.close()


@pytest.fixture(scope="session")
def integration_app() -> Iterator[AzureSqlMcpApplication]:
    config = load_server_config([])
    app = AzureSqlMcpApplication(config)
    _patch_local_sql_server_connection(app)
    yield app


@pytest.fixture(scope="session")
def integration_database_name(integration_app: AzureSqlMcpApplication) -> str:
    return integration_app.config.default_database


@pytest.fixture(scope="session")
def prepared_test_schema(
    integration_app: AzureSqlMcpApplication,
    integration_database_name: str,
) -> Iterator[str]:
    try:
        _prepare_schema(integration_app, integration_database_name)
    except Exception as exc:  # pragma: no cover - integration-only failure path
        pytest.skip(f"Could not prepare integration test schema: {exc}")
    try:
        yield TEST_SCHEMA_NAME
    finally:
        try:
            _cleanup_schema(integration_app, integration_database_name)
        except Exception:
            pass
