# Phase 7: SQL Validation Hardening

## Problem

Our current SQL validator uses a **blacklist** approach: it blocks known-dangerous AST node types (INSERT, UPDATE, DELETE, etc.) and uses regex pre-checks for EXEC, GO, OPENROWSET, and temp tables. A whitelist approach (allowing only known-safe functions) is inherently more secure, but may be too restrictive for T-SQL's rich function surface.

We should strengthen the current approach to block known bypass vectors.

---

## Current Validation Flow

```
1. Regex pre-checks (GO, EXEC, OPENROWSET, temp tables)
2. Parse with sqlglot (T-SQL dialect)
3. Enforce single statement
4. Check top-level node is SELECT/UNION/EXCEPT/INTERSECT
5. Walk AST for banned nodes (INSERT, UPDATE, DELETE, etc.)
6. Check for INTO and temp table references
7. Return normalized SQL
```

## Known Bypass Risks

### Risk 1: Comment-Embedded Keywords
```sql
-- EX/**/ECUTE could bypass regex but sqlglot handles it correctly
-- This is already safe due to AST parsing, but regex gives false confidence
```

### Risk 2: Dangerous Functions in SELECT Context

Some T-SQL functions can have side effects even in a SELECT:
```sql
SELECT xp_cmdshell('whoami')                    -- OS command execution
SELECT * FROM OPENROWSET(BULK 'C:\file', ...)   -- File system access
SELECT sp_OACreate('...')                        -- COM object creation
```

Our regex catches OPENROWSET, but `xp_cmdshell` and `sp_OA*` are not explicitly blocked.

### Risk 3: Linked Server Access
```sql
SELECT * FROM [LinkedServer].[Database].[Schema].[Table]
```
Four-part names can access linked servers. Our validator doesn't check for this.

### Risk 4: Dynamic SQL via String Functions
```sql
-- sqlglot parses this as a SELECT, but it could theoretically be used
-- for information gathering beyond the intended scope
SELECT name FROM sys.server_principals
```
System catalog access is allowed -- this may be intentional for introspection, but should be documented as a design decision.

---

## Recommended Enhancements

### 7A. Block Dangerous System Procedures and Extended Procedures

Add an AST-level check for dangerous function calls:

```python
BLOCKED_FUNCTIONS = frozenset({
    # Extended stored procedures
    "xp_cmdshell", "xp_fileexist", "xp_fixeddrives", "xp_getfiledetails",
    "xp_dirtree", "xp_subdirs", "xp_regread", "xp_regwrite",
    "xp_servicecontrol", "xp_loginconfig", "xp_msver",
    # OLE Automation
    "sp_oacreate", "sp_oamethod", "sp_oagetproperty",
    "sp_oasetproperty", "sp_oadestroy", "sp_oageterrorinfo",
    # Mail
    "sp_send_dbmail", "xp_sendmail",
    # Dangerous system procs accessible via SELECT
    "fn_get_sql", "fn_servershareddrives",
})

def _check_dangerous_functions(self, statement):
    for node in statement.walk():
        if isinstance(node, exp.Anonymous):  # Function calls
            func_name = str(node.this).lower()
            if func_name in BLOCKED_FUNCTIONS:
                raise ValueError(
                    f"Function '{func_name}' is not allowed in restricted mode."
                )
```

### 7B. Block Linked Server / Four-Part Names

```python
def _check_four_part_names(self, statement):
    for node in statement.walk():
        if isinstance(node, exp.Table):
            parts = []
            if node.args.get("catalog"):
                parts.append(str(node.args["catalog"]))
            if node.args.get("db"):
                parts.append(str(node.args["db"]))
            if len(parts) > 0:
                raise ValueError(
                    "Cross-database and linked server references are not allowed "
                    "in restricted mode."
                )
```

### 7C. Block DBCC Commands

DBCC commands can modify database state:

```python
DBCC_PATTERN = re.compile(r"\bDBCC\b", re.IGNORECASE)

# Add to validate_read_only before parsing:
if DBCC_PATTERN.search(candidate):
    raise ValueError("DBCC commands are not allowed in restricted mode.")
```

### 7D. Add Bypass Attempt Tests

Expand `tests/unit/test_safe_sql.py`:

```python
class TestBypassAttempts:
    def test_rejects_xp_cmdshell_in_select(self):
        validator = SafeSqlValidator()
        with pytest.raises(ValueError, match="xp_cmdshell"):
            validator.validate_read_only("SELECT xp_cmdshell('dir')")

    def test_rejects_four_part_name(self):
        validator = SafeSqlValidator()
        with pytest.raises(ValueError, match="linked server"):
            validator.validate_read_only(
                "SELECT * FROM [Server].[DB].[dbo].[Table]"
            )

    def test_rejects_dbcc(self):
        validator = SafeSqlValidator()
        with pytest.raises(ValueError, match="DBCC"):
            validator.validate_read_only("DBCC CHECKDB")

    def test_rejects_sp_oacreate(self):
        validator = SafeSqlValidator()
        with pytest.raises(ValueError, match="sp_oacreate"):
            validator.validate_read_only("SELECT sp_OACreate('Shell.Application')")

    def test_comment_embedded_exec_still_blocked(self):
        """Verify that comment tricks don't bypass the AST check."""
        validator = SafeSqlValidator()
        # sqlglot should parse this correctly or reject it
        with pytest.raises(ValueError):
            validator.validate_read_only("EX/**/ECUTE sp_who")

    def test_allows_safe_system_catalog_queries(self):
        """System catalog access is intentionally allowed for introspection."""
        validator = SafeSqlValidator()
        result = validator.validate_read_only(
            "SELECT name FROM sys.objects WHERE type = 'U'"
        )
        assert result.normalized_sql  # Should succeed

    def test_allows_common_functions(self):
        """Ensure legitimate functions aren't blocked."""
        validator = SafeSqlValidator()
        for sql in [
            "SELECT GETDATE()",
            "SELECT COUNT(*) FROM sys.tables",
            "SELECT ISNULL(name, '') FROM sys.schemas",
            "SELECT TOP 10 * FROM sys.columns ORDER BY column_id",
            "SELECT STRING_AGG(name, ',') FROM sys.schemas",
        ]:
            result = validator.validate_read_only(sql)
            assert result.normalized_sql
```

### 7E. Document Security Model

Add a section to the README documenting the security model:

- Restricted mode allows: `SELECT`, CTEs, UNION/EXCEPT/INTERSECT, system catalog queries
- Restricted mode blocks: DML, DDL, EXEC, OPENROWSET, temp tables, DBCC, dangerous functions, linked server access
- The validator is defense-in-depth (regex pre-check + AST validation)
- System catalog access (sys.*) is intentionally allowed for schema introspection
- SQL injection is mitigated by parameterized queries for all dynamic values

---

## Files to Change

- **Modify:** `src/azure_sql_mcp/safe_sql.py` - Add function blocklist, four-part name check, DBCC check
- **Extend:** `tests/unit/test_safe_sql.py` - Add bypass attempt tests
- **Modify:** `README.md` - Document security model

## Verification

1. Run all existing safe_sql tests to ensure no regressions
2. Run new bypass attempt tests
3. Verify legitimate queries still work (system catalog, common functions, CTEs, subqueries)
4. Test with a real Azure SQL database to confirm no false positives on common query patterns
