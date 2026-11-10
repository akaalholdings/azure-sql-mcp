"""Live correction of the S13a rule: only a clustered columnstore index forces an offline build.

Observed on Azure SQL Database: CREATE NONCLUSTERED INDEX ... WITH (ONLINE = ON)
fails with error 35352 on a table whose clustered index is columnstore
(sys.indexes type 5). A rowstore table with a nonclustered columnstore index
(type 6) accepts the online build, so an offline build there would block
writers for nothing.
"""

from __future__ import annotations

import re

from mssql_python.exceptions import ProgrammingError

from azure_sql_mcp.connection import AdminBatchOutcomeUnknownError
from azure_sql_mcp.observability import extract_failure_diagnostic
from azure_sql_mcp.server import _COLUMNSTORE_OFFLINE_BUILD_NOTE
from azure_sql_mcp.server import _TEST_INDEX_TABLE_SQL
from azure_sql_mcp.server import _TestIndexTable
from azure_sql_mcp.tool_guides import get_tool_guide

ONLINE_REFUSED = (
    "create statement failed because the operation cannot be performed online on a "
    "table with a columnstore index. Perform the operation without specifying the "
    "ONLINE option or drop (or disable) the columnstore index before performing the "
    "operation using the ONLINE option."
)


def test_table_read_counts_only_a_clustered_columnstore_index() -> None:
    sql = " ".join(_TEST_INDEX_TABLE_SQL.split())

    assert "cs.type = 5" in sql
    assert "has_clustered_columnstore_index" in sql
    # Type 6 (nonclustered columnstore) accepts an online B-tree build.
    assert not re.search(r"type IN \(5, 6\)", sql)
    assert "is_disabled" not in sql


def test_the_fact_and_the_note_say_clustered_columnstore() -> None:
    table = _TestIndexTable(schema="dw", table="FactVote", has_clustered_columnstore_index=True)

    assert table.has_clustered_columnstore_index is True
    assert "clustered columnstore index" in _COLUMNSTORE_OFFLINE_BUILD_NOTE


def test_refused_online_build_returns_engine_error_35352() -> None:
    """mssql-python gives no error number; the agent needs it to look the rule up."""

    cause = ProgrammingError(
        driver_error="Syntax error or access violation",
        ddbc_error=f"[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]{ONLINE_REFUSED}",
    )
    try:
        raise AdminBatchOutcomeUnknownError(str(cause)) from cause
    except AdminBatchOutcomeUnknownError as wrapped:
        diagnostic = extract_failure_diagnostic(wrapped, engine_text=True)

    assert diagnostic["native_error_code"] == 35352
    assert diagnostic["engine_message"] == ONLINE_REFUSED


def test_error_number_is_not_guessed_for_caller_sql() -> None:
    error = RuntimeError(f"[42000] [Microsoft][SQL Server]{ONLINE_REFUSED}")

    assert "native_error_code" not in extract_failure_diagnostic(error)


def test_guides_name_the_clustered_columnstore_rule() -> None:
    text = " ".join(get_tool_guide("benchmark_index_candidate")["guide"].split())

    assert "clustered columnstore index is built offline" in text
