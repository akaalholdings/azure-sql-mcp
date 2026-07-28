from __future__ import annotations

from azure_sql_mcp.observability import (
    compute_query_hash,
    extract_sql_error_info,
    redact_sql_literals,
    sanitize_error_message,
)


class TestExtractSqlErrorInfo:
    def test_sqlstate_from_string(self):
        exc = Exception("[42S02] Table not found (Error 208)")
        info = extract_sql_error_info(exc)
        assert info["sqlstate"] == "42S02"
        assert info["native_error_code"] == 208

    def test_nested_exception(self):
        inner = Exception("[HY000] something went wrong")
        outer = Exception("Wrapper")
        outer.__cause__ = inner
        info = extract_sql_error_info(outer)
        assert info["sqlstate"] == "HY000"

    def test_no_sql_info(self):
        exc = Exception("generic error")
        info = extract_sql_error_info(exc)
        assert info == {}

    def test_tuple_args(self):
        exc = Exception(("08S01", "Connection lost"))
        info = extract_sql_error_info(exc)
        assert info["sqlstate"] == "08S01"


class TestComputeQueryHash:
    def test_deterministic(self):
        h1 = compute_query_hash("SELECT 1 FROM t")
        h2 = compute_query_hash("SELECT 1 FROM t")
        assert h1 == h2

    def test_whitespace_normalized(self):
        h1 = compute_query_hash("SELECT  1   FROM   t")
        h2 = compute_query_hash("select 1 from t")
        assert h1 == h2

    def test_different_queries_different_hash(self):
        h1 = compute_query_hash("SELECT 1")
        h2 = compute_query_hash("SELECT 2")
        assert h1 != h2


class TestSanitizeErrorMessage:
    def test_strips_connection_string(self):
        msg = "Error connecting: Server=myserver.database.windows.net;User ID=admin;Password=secret123;"
        sanitized = sanitize_error_message(msg)
        assert "secret123" not in sanitized
        assert "admin" not in sanitized
        assert "myserver.database.windows.net" not in sanitized

    def test_strips_server_name(self):
        msg = "Cannot connect to prod-sql-01.database.windows.net"
        sanitized = sanitize_error_message(msg)
        assert "prod-sql-01.database.windows.net" not in sanitized
        assert "[server]" in sanitized

    def test_strips_ip(self):
        msg = "Connection refused by 10.0.1.5 on port 1433"
        sanitized = sanitize_error_message(msg)
        assert "10.0.1.5" not in sanitized
        assert "[ip]" in sanitized

    def test_strips_uid_pwd_connection_tokens(self):
        msg = "SERVER=tcp:prod.database.windows.net;DATABASE=appdb;UID=sa;PWD=secret!;"
        sanitized = sanitize_error_message(msg)
        assert "UID=sa" not in sanitized
        assert "PWD=secret!" not in sanitized
        assert "prod.database.windows.net" not in sanitized

    def test_preserves_generic_messages(self):
        msg = "Timeout expired while waiting for query"
        assert sanitize_error_message(msg) == msg

    def test_preserves_apostrophes_in_generic_message(self):
        msg = "Can't connect because the session isn't available"
        assert sanitize_error_message(msg) == msg

    def test_strips_sql_literals(self):
        sanitized = sanitize_error_message("Incorrect syntax near N'SuperSecret-123!'")
        assert "SuperSecret-123!" not in sanitized
        assert "N'[REDACTED]'" in sanitized

    def test_preserves_unquoted_state_codes_while_redacting_sql_literals(self):
        sanitized = sanitize_error_message(
            "Session session-1 is cancelled; expected one of "
            "[finalist_validation, screening]. Incorrect syntax near N'SuperSecret-123!'"
        )
        assert "cancelled" in sanitized
        assert "finalist_validation" in sanitized
        assert "screening" in sanitized
        assert "SuperSecret-123!" not in sanitized
        assert "N'[REDACTED]'" in sanitized


class TestRedactSqlLiterals:
    def test_redacts_quoted_values_and_preserves_identifiers(self):
        sql = 'EXEC "dbo"."RecordValues" @one = N\'private\', @two = \'it\'\'s secret\''
        assert redact_sql_literals(sql) == (
            'EXEC "dbo"."RecordValues" @one = N\'[REDACTED]\', @two = \'[REDACTED]\''
        )

    def test_redacts_line_and_nested_block_comments(self):
        sql = "SELECT 1 -- token=private\n/* outer /* inner */ secret */ SELECT 2"
        assert redact_sql_literals(sql) == (
            "SELECT 1 --[REDACTED]\n/*[REDACTED]*/ SELECT 2"
        )

    def test_redacts_unterminated_literal(self):
        assert redact_sql_literals("SELECT 'never closed") == "SELECT '[REDACTED]'"
