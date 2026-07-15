from __future__ import annotations

import pytest

from azure_sql_mcp.safe_sql import SafeSqlValidator


@pytest.fixture
def validator():
    return SafeSqlValidator()


def test_accepts_single_select(validator):
    validated = validator.validate_read_only("SELECT TOP 1 name FROM sys.objects;")
    assert "SELECT TOP 1 name FROM sys.objects" in validated.normalized_sql


def test_accepts_cte_and_sys_catalog(validator):
    validated = validator.validate_read_only(
        "WITH names AS (SELECT name FROM sys.objects) SELECT TOP 1 name FROM names"
    )
    assert "WITH names AS" in validated.normalized_sql


def test_allows_openjson(validator):
    validated = validator.validate_read_only("SELECT * FROM OPENJSON('[1,2]')")
    assert "OPENJSON" in validated.normalized_sql


def test_allows_common_read_only_functions(validator):
    validated = validator.validate_read_only(
        "SELECT TOP 1 GETDATE(), COALESCE(name, '') FROM sys.objects ORDER BY name"
    )
    assert validated.normalized_sql


@pytest.mark.parametrize(
    "sql, match",
    [
        ("INSERT INTO dbo.Items VALUES (1)", "Restricted mode only supports SELECT queries."),
        ("SELECT 1; SELECT 2;", "Exactly one SQL statement is allowed."),
        ("EXEC dbo.DoThing", "EXEC is not allowed in restricted mode."),
        ("SELECT 1 INTO dbo.NewTable FROM sys.objects", "SELECT INTO is not allowed in restricted mode."),
        ("SELECT * INTO #temp FROM sys.objects", "Temporary table references are not allowed in restricted mode."),
        ("SELECT * FROM OPENROWSET(...)", "External rowset access is not allowed in restricted mode."),
        ("SELECT 1 GO SELECT 2", "Batch separators such as GO are not allowed."),
        ("DBCC CHECKDB", "DBCC commands are not allowed in restricted mode."),
        ("SELECT xp_cmdshell('dir')", "Function 'xp_cmdshell' is not allowed in restricted mode."),
        (
            "SELECT sp_OACreate('Shell.Application')",
            "Function 'sp_oacreate' is not allowed in restricted mode.",
        ),
        (
            "SELECT * FROM [LinkedServer].[DB].[dbo].[T]",
            "Cross-database and linked-server references are not allowed in restricted mode.",
        ),
        ("EX/**/ECUTE sp_who", "Invalid T-SQL:"),
        # Phase 16: SQL Validation v2
        ("WAITFOR DELAY '00:00:05'", "WAITFOR is not allowed in restricted mode"),
        ("SELECT 1 WAITFOR TIME '12:00:00'", "WAITFOR is not allowed in restricted mode"),
        ("EXECUTE AS USER = 'dbo'", "EXECUTE AS is not allowed in restricted mode"),
        (
            "SELECT * FROM dbo.t WITH (UPDLOCK)",
            "Locking hints .* are not allowed in restricted mode",
        ),
        (
            "SELECT * FROM dbo.t WITH (XLOCK)",
            "Locking hints .* are not allowed in restricted mode",
        ),
        (
            "SELECT * FROM dbo.t WITH (TABLOCKX)",
            "Locking hints .* are not allowed in restricted mode",
        ),
        (
            "sp_executesql N'SELECT 1'",
            "sp_executesql is not allowed in restricted mode",
        ),
        (
            ";WITH cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte) SELECT * FROM cte OPTION (MAXRECURSION 0)",
            "MAXRECURSION 0 .* is not allowed in restricted mode",
        ),
    ],
)
def test_rejects_unsafe_sql(validator, sql, match):
    with pytest.raises(ValueError, match=match):
        validator.validate_read_only(sql)


def test_allows_normal_with_hint(validator):
    """WITH (NOLOCK) should not be blocked."""
    validated = validator.validate_read_only("SELECT * FROM sys.objects WITH (NOLOCK)")
    assert validated.normalized_sql


def test_allows_maxrecursion_nonzero(validator):
    """MAXRECURSION with a positive value should be allowed."""
    validated = validator.validate_read_only(
        "WITH cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 10) "
        "SELECT * FROM cte OPTION (MAXRECURSION 100)"
    )
    assert validated.normalized_sql


def test_allows_declare_set_prefix_before_select(validator):
    """Auto-bound parameterized queries ship as DECLARE/SET + SELECT batches."""
    validated = validator.validate_read_only(
        "DECLARE @UserId int;\n"
        "SET @UserId = 42;\n\n"
        "SELECT * FROM dbo.Users WHERE UserId = @UserId"
    )
    assert "DECLARE" in validated.normalized_sql.upper()
    assert "SELECT" in validated.normalized_sql.upper()


def test_allows_declare_with_inline_default(validator):
    validated = validator.validate_read_only(
        "DECLARE @Cutoff datetime2(7) = SYSDATETIME(); SELECT * FROM dbo.Orders WHERE CreatedAt >= @Cutoff"
    )
    assert validated.normalized_sql


def test_validated_prefix_batch_revalidates(validator):
    """The normalized batch must itself pass validation (idempotent round-trip)."""
    validated = validator.validate_read_only(
        "DECLARE @n int; SET @n = 5; SELECT TOP (@n) name FROM sys.objects"
    )
    revalidated = validator.validate_read_only(validated.normalized_sql)
    assert revalidated.normalized_sql


@pytest.mark.parametrize(
    "sql, match",
    [
        (
            "SET NOCOUNT ON; SELECT 1",
            "Only DECLARE and SET @variable",
        ),
        (
            "SET SHOWPLAN_XML ON; SELECT 1",
            "Only DECLARE and SET @variable",
        ),
        (
            "DECLARE @x int; DELETE FROM dbo.Users",
            "Restricted mode only supports SELECT queries.",
        ),
        (
            "DECLARE @x int; SET @x = 1; SELECT 1; SELECT 2",
            "Only DECLARE and SET @variable",
        ),
        (
            "SELECT 1; DECLARE @x int",
            "Only DECLARE and SET @variable",
        ),
        (
            "SET @x = (SELECT TOP 1 id FROM [Other].[dbo].[T]); SELECT @x",
            "Cross-database and linked-server references are not allowed in restricted mode.",
        ),
    ],
)
def test_rejects_unsafe_prefix_batches(validator, sql, match):
    with pytest.raises(ValueError, match=match):
        validator.validate_read_only(sql)


def test_extract_table_references_from_bound_batch(validator):
    refs = validator.extract_table_references(
        "DECLARE @UserId int;\nSET @UserId = 42;\n\n"
        "SELECT * FROM dbo.Users WHERE UserId = @UserId"
    )
    assert refs == [{"schema": "dbo", "table": "Users"}]


def test_extract_table_references_excludes_cte_names(validator):
    refs = validator.extract_table_references(
        """
        WITH recent AS (
            SELECT * FROM dbo.Orders WHERE CreatedAt >= '20260101'
        )
        SELECT c.CustomerId
        FROM recent AS r
        JOIN sales.Customers AS c ON c.CustomerId = r.CustomerId
        """
    )

    assert refs == [
        {"schema": "dbo", "table": "Orders"},
        {"schema": "sales", "table": "Customers"},
    ]


@pytest.mark.parametrize(
    "sql",
    [
        # Words that trip text rules when they appear as string data, not code.
        "SELECT * FROM dbo.Notes WHERE body = 'time to go home'",
        "SELECT * FROM dbo.Items WHERE tag = 'item#1'",
        "SELECT * FROM dbo.Logs WHERE message = 'please execute the plan'",
        "SELECT * FROM dbo.Jobs WHERE description = 'waitfor approval'",
        "SELECT 1 AS n -- EXEC in a comment is not code",
    ],
)
def test_text_rules_ignore_literals_and_comments(validator, sql):
    assert validator.validate_read_only(sql).normalized_sql


def test_text_rules_still_reject_code_outside_literals(validator):
    with pytest.raises(ValueError, match="EXEC is not allowed"):
        validator.validate_read_only("EXEC dbo.DoThing 'safe string'")
    with pytest.raises(ValueError, match="Temporary table references"):
        validator.validate_read_only("SELECT * FROM #tmp")
