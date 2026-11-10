"""An IF guard of a read-only branching batch may hold deterministic scalar expressions.

O02: the natural guard for "values that convert differently" is binary,
``@code COLLATE Latin1_General_BIN2 NOT LIKE N'%[^0-9A-Za-z-]%'``. The gate
refused COLLATE and CONVERT in a guard, so the agent used a weaker guard that
depends on the default collation of the database. The grammar stays closed:
no subquery, no object name, no write and no other function is admitted.
"""

from __future__ import annotations

import pytest

from azure_sql_mcp.safe_sql import SafeSqlValidator

SEEK = "SELECT a.Id FROM dbo.Accounts AS a WHERE a.AccountCode = CONVERT(varchar(20), @code)"
SCAN = "SELECT a.Id FROM dbo.Accounts AS a WHERE a.AccountCode = @code"


def _batch(guard: str) -> str:
    return f"IF {guard}\n    {SEEK};\nELSE\n    {SCAN};"


@pytest.mark.parametrize(
    "guard",
    [
        "@code IS NULL OR @code COLLATE Latin1_General_BIN2 NOT LIKE N'%[^0-9A-Za-z-]%'",
        "CONVERT(varchar(20), @code) = @code",
        "CAST(@code AS varchar(20)) = @code AND @m BETWEEN 1 AND 12",
        "TRY_CONVERT(int, @code) IS NOT NULL",
        "TRY_CAST(@code AS decimal(18, 2)) > 0",
        "@code LIKE '%!_%' ESCAPE '!'",
        "PATINDEX('%[^0-9]%', @code COLLATE Latin1_General_BIN2) = 0",
        "CONVERT(nvarchar(max), @code) COLLATE SQL_Latin1_General_CP1_CS_AS = N'ABC'",
        "LEN(@code) <= 20 AND ISNULL(@m, 0) > 0",
    ],
)
def test_a_guard_of_deterministic_scalar_expressions_on_parameters_is_admitted(guard: str) -> None:
    validated = SafeSqlValidator().validate_read_only_candidate(_batch(guard))

    assert [branch.kind for branch in validated.branches] == ["select", "select"]
    assert validated.branches[0].conditions == ((guard, True),)
    assert validated.execution_sql == _batch(guard)


@pytest.mark.parametrize(
    "guard",
    [
        # A subquery, in any position.
        "EXISTS (SELECT 1 FROM dbo.Accounts)",
        "(SELECT COUNT(*) FROM dbo.Accounts) > 0",
        "CONVERT(int, (SELECT MAX(Id) FROM dbo.Accounts)) > 0",
        "@code COLLATE (SELECT 1) = 'a'",
        # A function that reads the catalog, the clock or user code.
        "OBJECT_ID('dbo.Accounts') IS NOT NULL",
        "dbo.fn_is_valid(@code) = 1",
        "GETDATE() > @d",
        "NEWID() IS NOT NULL",
        # A column or object name.
        "a.AccountCode = @code",
        "CONVERT(varchar(20), AccountCode) = @code",
        # CONVERT used as a name, not as a call.
        "CONVERT = 1",
    ],
)
def test_a_guard_that_reads_data_or_calls_another_function_is_still_refused(guard: str) -> None:
    with pytest.raises(ValueError):
        SafeSqlValidator().validate_read_only_candidate(_batch(guard))


def test_a_branch_that_writes_is_still_refused_under_an_admitted_guard() -> None:
    batch = (
        "IF CONVERT(varchar(20), @code) = @code\n"
        "    DELETE FROM dbo.Accounts WHERE AccountCode = @code;\n"
        f"ELSE\n    {SCAN};"
    )

    with pytest.raises(ValueError):
        SafeSqlValidator().validate_read_only_candidate(batch)


def test_exec_of_user_code_is_still_refused_under_an_admitted_guard() -> None:
    batch = f"IF @code COLLATE Latin1_General_BIN2 LIKE 'A%'\n    EXEC dbo.DoWork @code;\nELSE\n    {SCAN};"

    with pytest.raises(ValueError):
        SafeSqlValidator().validate_read_only_candidate(batch)


def test_a_declared_variable_may_take_a_converted_parameter() -> None:
    batch = (
        "DECLARE @code_v varchar(20) = TRY_CONVERT(varchar(20), @code);\n"
        "IF @code_v = @code\n"
        f"    {SEEK};\nELSE\n    {SCAN};"
    )

    validated = SafeSqlValidator().validate_read_only_candidate(batch)

    assert validated.local_variables == ("@code_v",)
    with pytest.raises(ValueError):
        SafeSqlValidator().validate_read_only_candidate(
            batch.replace("TRY_CONVERT(varchar(20), @code)", "(SELECT TOP (1) AccountCode FROM dbo.Accounts)")
        )


# --- a statement the parser cannot read says where it stopped (O07) ---------------


@pytest.mark.parametrize(
    ("sql", "near"),
    [
        # A shell or JSON layer stripped the quotes of the pattern.
        ("SELECT i.name FROM sys.indexes AS i WHERE i.name LIKE IX[_]bench[_]O07[_]%", "[_]"),
        # The quotes were doubled on the way in.
        ("SELECT i.name FROM sys.indexes AS i WHERE i.name LIKE ''IX[_]bench%''", "IX"),
        (
            "SELECT COUNT(*) FROM dbo.Comments AS c "
            "WHERE c.CreationDate >= CONVERT(datetime, 2013-10-01T00:00:00, 126)",
            "T00",
        ),
    ],
)
def test_an_unreadable_select_is_refused_with_the_place_and_a_quoting_hint(sql: str, near: str) -> None:
    """O07: three refusals with one fixed sentence and no hint which construct blocked it."""

    with pytest.raises(ValueError) as refusal:
        SafeSqlValidator().validate_read_only(sql)

    message = str(refusal.value)
    assert "could not be statically analyzed" in message
    assert "line 1, column" in message
    assert near in message
    assert "quotes" in message


def test_the_same_catalog_queries_with_their_quotes_are_admitted() -> None:
    validator = SafeSqlValidator()

    for sql in (
        "SELECT i.name FROM sys.indexes AS i WHERE i.name LIKE 'IX[_]bench[_]O07[_]%'",
        "SELECT i.name FROM sys.indexes AS i WHERE i.name LIKE 'IX_bench_O07%'",
        "SELECT COUNT(*) FROM dbo.Comments AS c "
        "WHERE c.CreationDate >= CONVERT(datetime, '2013-10-01T00:00:00', 126)",
    ):
        assert validator.validate_read_only(sql).execution_sql == sql
