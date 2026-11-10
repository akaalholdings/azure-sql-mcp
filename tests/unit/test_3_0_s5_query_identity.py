"""Query Store identity helpers (review finding 53).

An application sends `SELECT ... WHERE c = @c` through sp_executesql, and Query
Store keeps it as `(@c int)SELECT ...`. A byte-exact comparison never matches,
so the history, the regressions and the application's real parameter types are
lost for almost every parameterized query. These helpers give the Query Store
reader one way to compare text and one way to read the hashes from a plan.
"""

from __future__ import annotations

import pytest

from azure_sql_mcp.query_identity import normalize_query_hash
from azure_sql_mcp.query_identity import normalize_statement_text
from azure_sql_mcp.query_identity import plan_query_hashes
from azure_sql_mcp.query_identity import statement_match_kind
from azure_sql_mcp.query_text import parameterized_query_store_text
from azure_sql_mcp.query_text import split_parameter_declarations

_NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"


def _plan(*statements: str) -> str:
    return (
        f'<ShowPlanXML xmlns="{_NS}" Version="1.564"><BatchSequence><Batch>'
        f"<Statements>{''.join(statements)}</Statements>"
        "</Batch></BatchSequence></ShowPlanXML>"
    )


def test_split_reads_the_declared_types_the_application_sent() -> None:
    # nvarchar(4000) against a varchar column is the classic production
    # CONVERT_IMPLICIT scan: the declared type must survive, not be re-guessed.
    parameters, statement = split_parameter_declarations(
        "(@Status nvarchar(4000),@CustomerId int, @Amount decimal(18,2))"
        "SELECT o.OrderId FROM dbo.Orders AS o WHERE o.Status = @Status"
    )

    assert parameters == (
        ("@Status", "nvarchar(4000)"),
        ("@CustomerId", "int"),
        ("@Amount", "decimal(18,2)"),
    )
    assert statement == "SELECT o.OrderId FROM dbo.Orders AS o WHERE o.Status = @Status"


def test_split_handles_writes_and_auto_parameterized_names() -> None:
    # Simple and forced parameterization name the parameters @1, @2; a
    # parameterized UPDATE has the same prefix as a SELECT.
    parameters, statement = split_parameter_declarations(
        "(@1 int,@2 varchar(8000))UPDATE dbo.Orders SET Status = @2 WHERE OrderId = @1"
    )

    assert parameters == (("@1", "int"), ("@2", "varchar(8000)"))
    assert statement.startswith("UPDATE dbo.Orders")


@pytest.mark.parametrize(
    "sql",
    [
        "(SELECT 1)",
        "(SELECT @a) UNION ALL SELECT 2",
        "(@a) SELECT 1",
        "(@a int",
        "(@a int, 'x')SELECT 1",
        "(@a int; DROP TABLE t)SELECT 1",
        "SELECT 1",
    ],
)
def test_split_leaves_text_that_is_not_a_declaration_list(sql: str) -> None:
    assert split_parameter_declarations(sql) == ((), sql)


def test_normalized_text_ignores_the_prefix_layout_and_comments() -> None:
    stored = (
        "(@c int)SELECT o.OrderId\r\n\tFROM dbo.Orders AS o -- hot path\r\n"
        "WHERE o.CustomerId = @c;"
    )
    submitted = (
        "SELECT o.OrderId FROM dbo.Orders AS o /* tuned */ WHERE o.CustomerId = @c"
    )

    assert normalize_statement_text(stored) == normalize_statement_text(submitted)
    assert normalize_statement_text(stored) == (
        "SELECT o.OrderId FROM dbo.Orders AS o WHERE o.CustomerId = @c"
    )


def test_normalized_text_never_touches_literals_or_delimited_names() -> None:
    # Two statements that differ inside a literal or a delimited name are
    # different queries; merging them would attach the wrong history.
    assert normalize_statement_text("SELECT 'a  b'") != normalize_statement_text(
        "SELECT 'a b'"
    )
    assert normalize_statement_text("SELECT '--x' AS c") == "SELECT '--x' AS c"
    assert normalize_statement_text("SELECT [a  b] FROM t") == "SELECT [a  b] FROM t"
    assert normalize_statement_text('SELECT "a /* b" FROM t') == 'SELECT "a /* b" FROM t'
    assert normalize_statement_text("SELECT N'it''s  x'") == "SELECT N'it''s  x'"


def test_normalized_text_keeps_case_unless_asked() -> None:
    # On a case-sensitive collation dbo.Orders and dbo.orders are two tables.
    assert normalize_statement_text("select 1 from T") != normalize_statement_text(
        "SELECT 1 FROM t"
    )
    assert normalize_statement_text(
        "select 'Ab' from T", fold_case=True
    ) == normalize_statement_text("SELECT 'Ab' FROM t", fold_case=True)
    assert normalize_statement_text(
        "SELECT 'Ab'", fold_case=True
    ) != normalize_statement_text("SELECT 'ab'", fold_case=True)


def test_a_comment_cannot_join_two_tokens() -> None:
    assert normalize_statement_text("SELECT 1/**/FROM t") == "SELECT 1 FROM t"
    assert normalize_statement_text("SELECT a/* x */+/* y */b") == "SELECT a + b"


def test_normalize_rejects_empty_text() -> None:
    with pytest.raises(ValueError):
        normalize_statement_text("  ")


def test_match_kind_names_the_weakest_step_that_was_needed() -> None:
    body = "SELECT o.OrderId FROM dbo.Orders AS o WHERE o.CustomerId = @c"

    assert statement_match_kind(body, body) == "exact_text"
    assert statement_match_kind(body, "(@c int)" + body) == "parameter_prefix"
    assert statement_match_kind(body, "(@c int)" + body.replace(" FROM", "\r\n  FROM")) == (
        "normalized_text"
    )
    assert statement_match_kind(body.lower(), "(@c int)" + body) == "case_insensitive_text"
    assert statement_match_kind(body, body.replace("@c", "@d")) is None
    assert statement_match_kind(body, body.replace("Orders", "Returns")) is None


def test_parameterized_text_is_the_form_query_store_keeps_for_sp_executesql() -> None:
    text = parameterized_query_store_text(
        "SELECT 1 WHERE @a = @b", "@a int, @b varchar(20)"
    )

    assert text == "(@a int, @b varchar(20))SELECT 1 WHERE @a = @b"
    assert split_parameter_declarations(text) == (
        (("@a", "int"), ("@b", "varchar(20)")),
        "SELECT 1 WHERE @a = @b",
    )
    assert parameterized_query_store_text("SELECT 1", "") == "SELECT 1"


def test_query_hash_is_validated_and_printed_one_way() -> None:
    assert normalize_query_hash("0x1a2b3c4d5e6f7a8b") == "0x1A2B3C4D5E6F7A8B"
    assert normalize_query_hash(" 0X1A2B3C4D5E6F7A8B ") == "0x1A2B3C4D5E6F7A8B"
    assert normalize_query_hash(bytes.fromhex("1a2b3c4d5e6f7a8b")) == "0x1A2B3C4D5E6F7A8B"


@pytest.mark.parametrize(
    "value",
    [
        "1A2B3C4D5E6F7A8B",
        "0x1A2B",
        "0x1A2B3C4D5E6F7A8B00",
        "0x1A2B3C4D5E6F7A8G",
        "0x1A2B3C4D5E6F7A8B; DROP TABLE t",
        # The engine reports zero for statements that have no query hash.
        "0x0000000000000000",
        b"\x01\x02",
        None,
        1234,
    ],
)
def test_query_hash_rejects_anything_that_is_not_binary_8(value: object) -> None:
    with pytest.raises(ValueError):
        normalize_query_hash(value)


def test_plan_hashes_come_back_per_statement_in_document_order() -> None:
    plan = _plan(
        '<StmtSimple StatementId="1" StatementType="ASSIGN" StatementText="DECLARE @x int = 1"/>',
        '<StmtSimple StatementId="2" StatementType="SELECT" '
        'QueryHash="0xaabbccddeeff0011" QueryPlanHash="0x1122334455667788"/>',
        '<StmtCond StatementId="3" StatementType="COND">'
        '<Then><Statements><StmtSimple StatementId="4" StatementType="UPDATE" '
        'QueryHash="0x0102030405060708" QueryPlanHash="0x8877665544332211"/>'
        "</Statements></Then></StmtCond>",
    )

    hashes = plan_query_hashes(plan)

    assert hashes == [
        {
            "statement_id": 2,
            "statement_type": "SELECT",
            "query_hash": "0xAABBCCDDEEFF0011",
            "query_plan_hash": "0x1122334455667788",
        },
        {
            "statement_id": 4,
            "statement_type": "UPDATE",
            "query_hash": "0x0102030405060708",
            "query_plan_hash": "0x8877665544332211",
        },
    ]


def test_plan_hashes_drop_values_that_are_not_hashes() -> None:
    # Plan XML is data. A hash that is not binary(8) must never reach a
    # CONVERT(binary(8), ?, 1) lookup, and a zero hash matches nothing real.
    plan = _plan(
        '<StmtSimple StatementId="1" StatementType="SELECT" '
        'QueryHash="0x1; DROP" QueryPlanHash="0x1122334455667788"/>',
        '<StmtSimple StatementId="2" StatementType="SELECT" '
        'QueryHash="0x0000000000000000" QueryPlanHash="not-a-hash"/>',
    )

    assert plan_query_hashes(plan) == [
        {
            "statement_id": 1,
            "statement_type": "SELECT",
            "query_hash": None,
            "query_plan_hash": "0x1122334455667788",
        }
    ]


@pytest.mark.parametrize("plan", ["", "<not-xml", "<ShowPlanXML/>", "<a xmlns='urn:x'/>"])
def test_plan_hashes_reject_text_that_is_not_a_showplan(plan: str) -> None:
    with pytest.raises(ValueError):
        plan_query_hashes(plan)
