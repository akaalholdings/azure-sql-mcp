from __future__ import annotations

from azure_sql_mcp.query_text import strip_query_store_parameter_declarations


def test_strip_query_store_parameter_declarations_removes_leading_declarations() -> None:
    sql = "(@P1 nvarchar(30), @P2 decimal(18,2))SELECT * FROM sys.objects"
    assert strip_query_store_parameter_declarations(sql) == "SELECT * FROM sys.objects"


def test_strip_query_store_parameter_declarations_keeps_normal_sql() -> None:
    sql = "SELECT TOP 1 name FROM sys.objects"
    assert strip_query_store_parameter_declarations(sql) == sql


def test_strip_query_store_parameter_declarations_keeps_parenthesized_non_query_store_sql() -> None:
    sql = "(SELECT 1)"
    assert strip_query_store_parameter_declarations(sql) == sql


def test_strip_query_store_parameter_declarations_requires_select_or_with_after_prefix() -> None:
    sql = "(@P1 int)UPDATE dbo.t SET c = 1"
    assert strip_query_store_parameter_declarations(sql) == sql
