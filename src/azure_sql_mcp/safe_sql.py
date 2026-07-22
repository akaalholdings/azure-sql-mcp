from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from sqlglot import exp
from sqlglot import parse
from sqlglot.errors import ParseError

GO_PATTERN = re.compile(r"(^|\s)GO(\s|$)", re.IGNORECASE)
EXEC_PATTERN = re.compile(r"\bEXEC(?:UTE)?\b", re.IGNORECASE)
OPENROWSET_PATTERN = re.compile(
    r"\b(OPENROWSET|OPENQUERY|OPENDATASOURCE)\b",
    re.IGNORECASE,
)
DBCC_PATTERN = re.compile(r"\bDBCC\b", re.IGNORECASE)
TEMP_TABLE_PATTERN = re.compile(r"#[A-Za-z0-9_]+")
WAITFOR_PATTERN = re.compile(r"\bWAITFOR\b", re.IGNORECASE)
EXECUTE_AS_PATTERN = re.compile(r"\bEXECUTE\s+AS\b", re.IGNORECASE)
SP_EXECUTESQL_PATTERN = re.compile(r"\bsp_executesql\b", re.IGNORECASE)
DANGEROUS_HINTS_PATTERN = re.compile(
    r"\bWITH\s*\(\s*(UPDLOCK|XLOCK|TABLOCKX)\b",
    re.IGNORECASE,
)
MAXRECURSION_ZERO_PATTERN = re.compile(
    r"\bMAXRECURSION\s+0\b",
    re.IGNORECASE,
)
_LINE_COMMENT_PATTERN = re.compile(r"--[^\r\n]*")
_BLOCK_COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)
_STRING_LITERAL_PATTERN = re.compile(r"N?'(?:''|[^'])*'", re.IGNORECASE)

BLOCKED_FUNCTIONS = frozenset(
    {
        # Extended stored procedures
        "xp_cmdshell",
        "xp_dirtree",
        "xp_fileexist",
        "xp_fixeddrives",
        "xp_getfiledetails",
        "xp_loginconfig",
        "xp_msver",
        "xp_regread",
        "xp_regwrite",
        "xp_servicecontrol",
        "xp_subdirs",
        # OLE Automation / COM access
        "sp_oacreate",
        "sp_oadestroy",
        "sp_oageterrorinfo",
        "sp_oagetproperty",
        "sp_oamethod",
        "sp_oasetproperty",
        # Mail / external notification
        "sp_send_dbmail",
        "xp_sendmail",
        # Dangerous metadata / filesystem helpers
        "fn_get_sql",
        "fn_servershareddrives",
    }
)


def strip_literals_and_comments(sql: str) -> str:
    """Replace comments and string literals so text-rule scans only see code.

    Keyword patterns (GO, EXEC, WAITFOR, #temp, ...) must not fire on words
    that merely appear inside string data or comments.
    """
    without_block_comments = _BLOCK_COMMENT_PATTERN.sub(" ", sql)
    without_comments = _LINE_COMMENT_PATTERN.sub(" ", without_block_comments)
    return _STRING_LITERAL_PATTERN.sub("?", without_comments)


@dataclass(frozen=True)
class ValidatedQuery:
    normalized_sql: str


class SafeSqlValidator:
    def validate_read_only(self, sql: str) -> ValidatedQuery:
        """Validate a read-only batch: optional DECLARE / SET @variable statements
        followed by exactly one SELECT-style statement.

        The variable prefix exists so parameterized queries can be executed with
        bound values (T-SQL variables are batch-scoped, so the DECLARE/SET block
        and the query must ship as a single batch).
        """
        candidate = sql.strip()
        if not candidate:
            raise ValueError("SQL cannot be empty.")
        self._check_text_rules(candidate)

        try:
            statements = parse(candidate, read="tsql")
        except ParseError as exc:
            raise ValueError(f"Invalid T-SQL: {exc}") from exc

        if not statements:
            raise ValueError("Invalid T-SQL: parser returned no statements.")

        *prefix, final = statements
        for statement in prefix:
            if statement is None:
                raise ValueError("Invalid T-SQL: parser returned an empty statement.")
            self._check_prefix_statement(statement)
        if final is None:
            raise ValueError("Invalid T-SQL: parser returned an empty statement.")
        self._check_statement(final)

        normalized = ";\n".join(
            statement.sql(dialect="tsql") for statement in statements if statement
        )
        return ValidatedQuery(normalized_sql=normalized)

    def extract_table_references(self, sql: str) -> list[dict[str, str | None]]:
        """Return base table references from a validated read-only statement."""
        validated = self.validate_read_only(sql)
        statements = [
            statement
            for statement in parse(validated.normalized_sql, read="tsql")
            if statement is not None
        ]
        if not statements:
            raise ValueError("Invalid T-SQL: parser returned an empty statement.")
        cte_names = {
            str(cte.alias_or_name).lower()
            for statement in statements
            for cte in statement.find_all(exp.CTE)
            if cte.alias_or_name
        }

        seen: set[tuple[str | None, str]] = set()
        positions: dict[tuple[str | None, str], int] = {}
        references: list[dict[str, str | None]] = []
        normalized_lookup = validated.normalized_sql.lower()
        all_tables = (
            table for statement in statements for table in statement.find_all(exp.Table)
        )
        for table in all_tables:
            table_name = str(table.this).strip("[]")
            if not table_name or table_name.lower() in cte_names:
                continue
            schema_name = table.args.get("db")
            schema = str(schema_name).strip("[]") if schema_name is not None else None
            key = (schema, table_name)
            if key in seen:
                continue
            seen.add(key)
            needle = f"{schema}.{table_name}" if schema else table_name
            position = normalized_lookup.find(needle.lower())
            positions[key] = position if position >= 0 else len(normalized_lookup)
            references.append(
                {
                    "schema": schema,
                    "table": table_name,
                }
            )
        return sorted(
            references,
            key=lambda ref: positions[(ref.get("schema"), str(ref["table"]))],
        )

    def _check_text_rules(self, sql: str) -> None:
        # Scan with literals and comments removed: a WHERE clause comparing
        # against 'go home' or 'item#1' is data, not a batch separator or a
        # temp table. Actual EXEC/DML in code positions is still caught here
        # and again by the AST walk.
        candidate = strip_literals_and_comments(sql)
        if GO_PATTERN.search(candidate):
            raise ValueError("Batch separators such as GO are not allowed.")
        if DBCC_PATTERN.search(candidate):
            raise ValueError("DBCC commands are not allowed in restricted mode.")
        if EXECUTE_AS_PATTERN.search(candidate):
            raise ValueError("EXECUTE AS is not allowed in restricted mode (privilege escalation risk).")
        if EXEC_PATTERN.search(candidate):
            raise ValueError("EXEC is not allowed in restricted mode.")
        if OPENROWSET_PATTERN.search(candidate):
            raise ValueError("External rowset access is not allowed in restricted mode.")
        if TEMP_TABLE_PATTERN.search(candidate):
            raise ValueError("Temporary table references are not allowed in restricted mode.")
        if WAITFOR_PATTERN.search(candidate):
            raise ValueError("WAITFOR is not allowed in restricted mode (DoS risk).")
        if SP_EXECUTESQL_PATTERN.search(candidate):
            raise ValueError("sp_executesql is not allowed in restricted mode (dynamic SQL risk).")
        if DANGEROUS_HINTS_PATTERN.search(candidate):
            raise ValueError(
                "Locking hints (UPDLOCK, XLOCK, TABLOCKX) are not allowed in restricted mode."
            )
        if MAXRECURSION_ZERO_PATTERN.search(candidate):
            raise ValueError(
                "MAXRECURSION 0 (unlimited recursion) is not allowed in restricted mode."
            )

    def _check_statement(self, statement: Any) -> None:
        if not isinstance(statement, (exp.Select, exp.Union, exp.Except, exp.Intersect)):
            raise ValueError("Restricted mode only supports SELECT queries.")
        self._check_tree(statement)

    def _check_prefix_statement(self, statement: Any) -> None:
        """Allow only DECLARE and SET @variable statements before the final SELECT."""
        if isinstance(statement, exp.Declare):
            self._check_tree(statement)
            return
        if isinstance(statement, exp.Set) and self._is_variable_assignment(statement):
            self._check_tree(statement)
            return
        raise ValueError(
            "Exactly one SQL statement is allowed. Only DECLARE and SET @variable "
            "assignments may precede the final SELECT statement."
        )

    @staticmethod
    def _is_variable_assignment(statement: exp.Set) -> bool:
        """True only for `SET @var = ...`; session options like SET NOCOUNT ON
        parse with a Column (not a Parameter) on the left and are rejected."""
        items = statement.expressions
        if not items:
            return False
        for item in items:
            assignment = item.this if isinstance(item, exp.SetItem) else None
            if not isinstance(assignment, exp.EQ):
                return False
            if not isinstance(assignment.this, exp.Parameter):
                return False
        return True

    def _check_tree(self, statement: Any) -> None:
        banned_nodes = (
            exp.Insert,
            exp.Update,
            exp.Delete,
            exp.Merge,
            exp.Create,
            exp.Drop,
            exp.Alter,
            exp.Command,
            exp.Transaction,
        )
        for node in statement.walk():
            if isinstance(node, banned_nodes):
                raise ValueError(
                    f"Restricted mode rejected unsafe SQL node: {node.__class__.__name__}."
                )

            if isinstance(node, exp.Into):
                raise ValueError("SELECT INTO is not allowed in restricted mode.")

            if isinstance(node, exp.Table):
                self._check_table_reference(node)

            if isinstance(node, exp.Anonymous):
                self._check_function_call(node)

    def _check_table_reference(self, table: exp.Table) -> None:
        if table.args.get("catalog") is not None:
            raise ValueError(
                "Cross-database and linked-server references are not allowed in restricted mode."
            )
        if str(table.this).startswith("#"):
            raise ValueError("Temporary table references are not allowed in restricted mode.")

    def _check_function_call(self, function: exp.Anonymous) -> None:
        function_name = str(function.this).lower()
        if function_name in BLOCKED_FUNCTIONS:
            raise ValueError(
                f"Function '{function_name}' is not allowed in restricted mode."
            )
