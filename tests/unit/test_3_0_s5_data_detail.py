"""Data-detail switch (PerformanceMonitor gap review, section 2D item 16).

A customer decides how much of their data may reach an agent's context: all of
it, statement shapes without literals, or metadata only. The decision must hold
for every tool, so it is applied in one place, and text that carries a secret is
withheld at every level.
"""

from __future__ import annotations

import pytest

from azure_sql_mcp.observability import DATA_DETAIL_LEVELS
from azure_sql_mcp.observability import apply_data_detail
from azure_sql_mcp.observability import contains_credential_pattern
from azure_sql_mcp.observability import mask_sql_literals
from azure_sql_mcp.observability import normalize_data_detail

STATEMENT = (
    "(@P1 nvarchar(4000),@P2 int)SELECT o.OrderId FROM dbo.[Orders 2024] AS o "
    "WHERE o.Email = N'a@b.example' AND o.Total > 1500.50 AND o.Region = @P1 "
    "AND o.Flags = 0x1F -- ticket 8841"
)


def test_levels_are_the_three_documented_ones_and_default_to_full() -> None:
    assert DATA_DETAIL_LEVELS == ("full", "mask_literals", "metadata_only")
    assert normalize_data_detail(None) == "full"
    assert normalize_data_detail(" Mask_Literals ") == "mask_literals"
    with pytest.raises(ValueError, match="full, mask_literals, metadata_only"):
        normalize_data_detail("partial")


def test_masking_removes_every_literal_and_keeps_the_shape() -> None:
    masked = mask_sql_literals(STATEMENT)

    assert masked == (
        "(@P1 nvarchar(4000),@P2 int)SELECT o.OrderId FROM dbo.[Orders 2024] AS o "
        "WHERE o.Email = '?' AND o.Total > ? AND o.Region = @P1 "
        "AND o.Flags = ? --?"
    )
    # The declared parameter types are the evidence for an implicit conversion
    # and hold no data; identifiers with digits are not literals.
    assert "nvarchar(4000)" in masked and "[Orders 2024]" in masked


def test_masking_handles_escapes_comments_and_identifiers() -> None:
    assert mask_sql_literals("SELECT 'it''s', \"col 9\", t1.c2 /* 4111-1111 */ FROM t1") == (
        "SELECT '?', \"col 9\", t1.c2 /*?*/ FROM t1"
    )
    assert mask_sql_literals("SELECT TOP (10) 1e3, -5, x FROM t WHERE y IN (1, 2)") == (
        "SELECT TOP (?) ?, -?, x FROM t WHERE y IN (?, ?)"
    )


@pytest.mark.parametrize(
    "text",
    [
        "CREATE LOGIN app WITH PASSWORD = 'S3cret!'",
        "ALTER USER app WITH PASSWORD='x'",
        "CREATE DATABASE SCOPED CREDENTIAL c WITH IDENTITY = 'SAS', SECRET = 'sv=2024'",
        "SELECT * FROM OPENROWSET(BULK 'https://acct.blob.core.windows.net/x', FORMAT='CSV') AS r",
        "EXEC sp_x @conn = 'Server=tcp:h;Database=d;User ID=u;Pwd=abc;'",
        "OPEN SYMMETRIC KEY k DECRYPTION BY PASSWORD N'p'",
    ],
)
def test_credential_text_is_recognised(text: str) -> None:
    assert contains_credential_pattern(text) is True


@pytest.mark.parametrize(
    "text",
    [
        "SELECT PasswordHash, LoginCount FROM dbo.Users WHERE UserId = @id",
        "SELECT * FROM dbo.Logins AS l WHERE l.LastLogin > @since",
        "SELECT secret_id FROM dbo.Secrets",
    ],
)
def test_column_names_that_mention_passwords_are_not_credentials(text: str) -> None:
    assert contains_credential_pattern(text) is False


def _payload() -> dict[str, object]:
    return {
        "database_name": "appdb",
        "rows": [
            {
                "query_id": 42,
                "query_hash": "0x1A2B3C4D5E6F7A8B",
                "query_sql_text": STATEMENT,
                "executions": 900,
            },
            {"query_id": 43, "query_sql_text": "CREATE LOGIN app WITH PASSWORD = 'S3cret!'"},
        ],
        "plan_digest": {
            "runtime": {"row_counts": True},
            "statements": [
                {
                    "statement_text": "SELECT 1 WHERE Status = 'Open'",
                    "parameters": [
                        {"name": "@P1", "compiled": "N'EMEA'", "runtime": "N'APAC'", "status": "differs"}
                    ],
                    "findings": [{"predicate": "[o].[Total]>(1500.50)", "node_id": 3}],
                }
            ],
        },
        "columns": [
            {
                "column_name": "Region",
                "value_picks": {"common": {"value": "EMEA", "equal_rows": 900}},
                "histogram": {"steps": [{"range_high_key": "EMEA", "equal_rows": 900}]},
            }
        ],
        "settings": [{"name": "MAXDOP", "value": 8}],
        "query_plan_xml": "<ShowPlanXML><StmtSimple StatementText=\"SELECT 'x'\"/></ShowPlanXML>",
    }


def test_full_returns_everything_except_credential_text() -> None:
    original = _payload()

    result = apply_data_detail(_payload(), "full")

    assert result["rows"][0] == original["rows"][0]  # type: ignore[index]
    assert result["plan_digest"] == original["plan_digest"]
    assert result["rows"][1]["query_sql_text"] == "[withheld: credential pattern]"  # type: ignore[index]
    assert "data_detail" not in result


def test_mask_literals_masks_text_predicates_and_parameter_values() -> None:
    result = apply_data_detail(_payload(), "mask_literals")

    row = result["rows"][0]
    assert row["query_sql_text"] == mask_sql_literals(STATEMENT)
    # Identity and numbers that are measurements are untouched.
    assert (row["query_id"], row["query_hash"], row["executions"]) == (42, "0x1A2B3C4D5E6F7A8B", 900)
    statement = result["plan_digest"]["statements"][0]
    assert statement["statement_text"] == "SELECT ? WHERE Status = '?'"
    assert statement["parameters"][0] == {
        "name": "@P1",
        "compiled": "[masked]",
        "runtime": "[masked]",
        "status": "differs",
    }
    assert statement["findings"][0] == {"predicate": "[o].[Total]>(?)", "node_id": 3}
    # "runtime" is a parameter value only inside a parameter list.
    assert result["plan_digest"]["runtime"] == {"row_counts": True}
    column = result["columns"][0]
    assert column["value_picks"]["common"] == {"value": "[masked]", "equal_rows": 900}
    assert column["histogram"]["steps"][0] == {"range_high_key": "[masked]", "equal_rows": 900}
    # A configuration value is metadata, not a parameter value.
    assert result["settings"] == [{"name": "MAXDOP", "value": 8}]
    # Raw plan XML holds statement text, predicates and compiled values.
    assert result["query_plan_xml"] == "[withheld: data detail is mask_literals]"
    assert result["rows"][1]["query_sql_text"] == "[withheld: credential pattern]"
    assert result["data_detail"] == "mask_literals"


def test_metadata_only_withholds_text_and_predicates() -> None:
    result = apply_data_detail(_payload(), "metadata_only")

    assert result["rows"][0]["query_sql_text"] == "[withheld: data detail is metadata_only]"
    assert result["rows"][0]["query_id"] == 42
    statement = result["plan_digest"]["statements"][0]
    assert statement["statement_text"] == "[withheld: data detail is metadata_only]"
    assert statement["findings"][0]["predicate"] == "[withheld: data detail is metadata_only]"
    assert statement["parameters"][0]["compiled"] == "[masked]"
    assert result["columns"][0]["column_name"] == "Region"
    assert result["data_detail"] == "metadata_only"


def test_the_input_is_not_changed_and_other_types_pass_through() -> None:
    payload = _payload()
    snapshot = repr(payload)

    apply_data_detail(payload, "metadata_only")

    assert repr(payload) == snapshot
    assert apply_data_detail("plain text", "metadata_only") == "plain text"
    assert apply_data_detail(None, "mask_literals") is None
    with pytest.raises(ValueError):
        apply_data_detail({}, "everything")
