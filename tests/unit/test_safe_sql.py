from __future__ import annotations

import pytest

from azure_sql_mcp.safe_sql import AdmissionStatus
from azure_sql_mcp.safe_sql import SafeSqlValidator
from azure_sql_mcp.safe_sql import StaticAnalysisStatus
from azure_sql_mcp.safe_sql import UnsupportedSqlError
from azure_sql_mcp.safe_sql import strip_literals_and_comments


@pytest.fixture
def validator():
    return SafeSqlValidator()


def test_accepts_single_select(validator):
    validated = validator.validate_read_only("SELECT TOP 1 name FROM sys.objects;")
    assert "SELECT TOP 1 name FROM sys.objects" in validated.normalized_sql


def test_validation_preserves_exact_sql_for_execution_and_identity(validator):
    sql = "\nSELECT name  FROM sys.objects WHERE name = 'MiXeD  Value'  \n"

    validated = validator.validate_read_only(sql)

    assert validated.submitted_sql == sql
    assert validated.execution_sql == sql
    assert validated.normalized_sql == sql.strip()
    assert "MiXeD  Value" in validated.execution_sql


def test_safe_but_unanalyzable_tsql_has_distinct_admission_outcome(validator):
    sql = "SELECT * FROM dbo.Items FOR JSON PATH, INCLUDE_NULL_VALUES"

    outcome = validator.analyze_read_only(sql)

    assert outcome.analysis_status is StaticAnalysisStatus.UNSUPPORTED
    assert outcome.admission_status is AdmissionStatus.NOT_ADMITTED
    assert outcome.submitted_sql == sql
    with pytest.raises(UnsupportedSqlError):
        validator.validate_read_only(sql)


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


# T-SQL does not need a semicolon between statements, and an unbracketed
# reserved keyword can never be an alias. sqlglot does accept one as an alias
# ("SELECT 1 AS DELETE FROM dbo.t"), so these batches parse as one SELECT while
# SQL Server runs a second, writing statement on an autocommit connection.
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1 DELETE FROM dbo.t WHERE id < 100",
        "SELECT @x DELETE FROM dbo.t",
        "DECLARE @x int = 1; SELECT @x DELETE FROM dbo.t",
        "SELECT (SELECT COUNT(*) FROM dbo.t) DELETE FROM dbo.t WHERE id < 100",
        "SELECT 1 DELETE FROM dbo.t COMMIT",
        "SELECT 1 RETURN",
        # Rejected today only because sqlglot fails to parse them; the policy
        # must not depend on parser quirks that can change between versions.
        "SELECT 1 TRUNCATE TABLE dbo.t",
        "SELECT 1 DROP TABLE dbo.t",
        "SELECT 1 INSERT dbo.t VALUES (1)",
        "SELECT 1 USE master",
        "SELECT 1 SET ROWCOUNT 1",
        "SELECT 1 GRANT SELECT ON dbo.t TO public",
        "SELECT 1 BEGIN TRAN",
        "SELECT 1 ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 1",
        "SELECT 1 MERGE dbo.t AS t USING dbo.s AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.v = s.v;",
        "SELECT 1 DECLARE c CURSOR FOR SELECT 1",
        # Compatibility forms of keywords are folded before matching.
        "SELECT ＤＥＬＥＴＥ FROM dbo.t",
    ],
)
def test_rejects_statement_keywords_hidden_after_select(validator, sql):
    with pytest.raises(ValueError, match="Statement keyword .* is not allowed"):
        validator.validate_read_only(sql)


@pytest.mark.parametrize(
    "sql",
    [
        # A '/*' inside a string must not hide the code that follows it.
        "SELECT '/*', 1 DELETE FROM dbo.t --*/'",
        # A '/*' inside a line comment must not hide the next line.
        "SELECT 1 -- /*\nDELETE FROM dbo.t -- */",
    ],
)
def test_comment_and_literal_tricks_do_not_hide_code(validator, sql):
    with pytest.raises(ValueError, match="Statement keyword 'DELETE' is not allowed"):
        validator.validate_read_only(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM dbo.t WITH (ROWLOCK, XLOCK)",
        "SELECT * FROM dbo.t WITH (ROWLOCK, UPDLOCK)",
        "SELECT * FROM dbo.t (XLOCK)",
        "SELECT * FROM dbo.t -- /*\nWITH (TABLOCKX) -- */",
        # Under RCSI these take shared locks that block writers for the whole read.
        "SELECT * FROM dbo.t WITH (TABLOCK)",
        "SELECT * FROM dbo.t WITH (NOLOCK, HOLDLOCK)",
        "SELECT * FROM dbo.t WITH (SERIALIZABLE)",
        "SELECT * FROM dbo.t WITH (REPEATABLEREAD)",
        "SELECT * FROM dbo.t WITH (READCOMMITTEDLOCK)",
        # Quoted hint names: fail closed rather than rely on how SQL Server reads them.
        "SELECT * FROM dbo.t WITH ([UPDLOCK])",
        'SELECT * FROM dbo.t WITH ("TABLOCK")',
        "SELECT * FROM dbo.t ([XLOCK])",
        "SELECT * FROM dbo.t WITH (NOLOCK, [HOLDLOCK])",
    ],
)
def test_rejects_locking_hints_in_any_position(validator, sql):
    with pytest.raises(ValueError, match="Locking hints .* are not allowed in restricted mode"):
        validator.validate_read_only(sql)


def test_rejects_sequence_side_effects(validator):
    with pytest.raises(ValueError, match="NEXT VALUE FOR"):
        validator.validate_read_only("SELECT NEXT VALUE FOR dbo.OrderNumbers")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT RAND(42)",
        "SELECT c FROM dbo.t WHERE RAND(123) < 0.1",
        "WITH x AS (SELECT RAND(9) AS r) SELECT r FROM x",
        "DECLARE @s int = 7; SELECT RAND(@s)",
    ],
)
def test_rejects_seeded_rand(validator, sql):
    # A seed resets the connection's generator, and pooled connections are reused.
    with pytest.raises(ValueError, match="RAND\\(seed\\)"):
        validator.validate_read_only(sql)


def test_allows_unseeded_rand(validator):
    assert validator.validate_read_only("SELECT RAND() AS r").normalized_sql


@pytest.mark.parametrize("separator", ["\x0b", "\x0c", "\x85", " ", " "])
def test_unusual_line_separators_do_not_hide_code_in_line_comments(validator, separator):
    # SQL Server ends a -- comment at CR or LF. Ending it earlier as well only
    # exposes more text to the keyword gate, so it can never admit more.
    with pytest.raises(ValueError, match="Statement keyword 'DROP'"):
        validator.validate_read_only(f"SELECT 1 --c{separator}DROP TABLE dbo.t")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM dbo.t ORDER BY id OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY",
        "SELECT * FROM dbo.a INNER MERGE JOIN dbo.b ON a.id = b.id",
        "SELECT id FROM dbo.a UNION SELECT id FROM dbo.b OPTION (MERGE UNION)",
        'SELECT [DELETE], "UPDATE" FROM dbo.t',
        "SELECT 'DELETE FROM dbo.t' AS note -- DROP TABLE dbo.t",
        "DECLARE @delete int = 1; SET @delete = 2; SELECT @delete AS [Update]",
        "SELECT x.updated_at, x.is_deleted, x.created_by FROM dbo.t AS x",
        "SELECT * FROM dbo.t AS t WHERE t.Status = N'it''s /* not a comment'",
        "SELECT * FROM dbo.t WITH (NOLOCK)",
    ],
)
def test_keyword_gate_allows_read_only_uses(validator, sql):
    assert validator.validate_read_only(sql).normalized_sql


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 'unterminated",
        "SELECT 1 /* unterminated",
        "SELECT [unterminated FROM dbo.t",
    ],
)
def test_rejects_unterminated_tokens(validator, sql):
    with pytest.raises(ValueError, match="Unterminated"):
        validator.validate_read_only(sql)


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT 1 -- /*\nDELETE", "SELECT 1  \nDELETE"),
        ("/* a /* nested */ still comment */SELECT 1", " SELECT 1"),
        ("SELECT '/*' , 1", "SELECT ? , 1"),
        ("SELECT [a--b], N'x'", "SELECT [a--b], ?"),
    ],
)
def test_strip_literals_and_comments_follows_tsql_lexing(sql, expected):
    assert strip_literals_and_comments(sql) == expected
