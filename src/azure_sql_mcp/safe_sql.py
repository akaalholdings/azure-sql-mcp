from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
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
# Zero however it is written: 0, 00, 0.0, 0e0, 0x0.
MAXRECURSION_ZERO_PATTERN = re.compile(
    r"\bMAXRECURSION\s+(?:0+(?![0-9])|0[xX])",
    re.IGNORECASE,
)
_READ_ONLY_PREFIX_PATTERN = re.compile(
    r"^\s*(?:SELECT|WITH|DECLARE)\b",
    re.IGNORECASE,
)
_CODE_WORD_PATTERN = re.compile(r"[\w@#$]+")
_LINE_COMMENT_ENDS = "\r\n\x0b\x0c\x85  "

# Table hints that take or hold locks a plain read does not. Under RCSI (the
# Azure SQL Database default) an unhinted SELECT takes no shared locks; these
# make the read block writers for its whole duration.
LOCKING_HINTS = frozenset(
    {
        "UPDLOCK",
        "XLOCK",
        "TABLOCKX",
        "TABLOCK",
        "HOLDLOCK",
        "SERIALIZABLE",
        "REPEATABLEREAD",
        "READCOMMITTEDLOCK",
    }
)

# T-SQL reserved keywords that start a statement and never occur inside a
# read-only SELECT. A reserved keyword cannot be an unbracketed alias in SQL
# Server, so one of these in code always starts another statement, even with
# no semicolon before it. sqlglot accepts them as aliases, so this check must
# not rely on the parse tree.
STATEMENT_KEYWORDS = frozenset(
    {
        "ALTER", "BACKUP", "BEGIN", "BREAK", "BULK", "CHECKPOINT", "CLOSE",
        "COMMIT", "CONTINUE", "CREATE", "DBCC", "DEALLOCATE", "DELETE", "DENY",
        "DISK", "DROP", "DUMP", "ERRLVL", "EXEC", "EXECUTE", "EXIT", "GOTO",
        "GRANT", "IF", "INSERT", "KILL", "LINENO", "LOAD", "OPEN", "PRINT",
        "RAISERROR", "READTEXT", "RECONFIGURE", "RESTORE", "RETURN", "REVERT",
        "REVOKE", "ROLLBACK", "SAVE", "SETUSER", "SHUTDOWN", "TRAN",
        "TRANSACTION", "TRUNCATE", "UPDATE", "UPDATETEXT", "WAITFOR", "WHILE",
        "WRITETEXT",
    }
)
# Reserved keywords that are statements in general but legal inside a read-only
# batch when followed by one of these words (or by a @variable).
_CONDITIONAL_KEYWORDS = {
    "MERGE": frozenset({"JOIN", "UNION"}),  # join and union hints
    "USE": frozenset({"HINT", "PLAN"}),  # OPTION (USE HINT(...)/USE PLAN ...)
    "SET": frozenset(),  # only SET @variable = ...
    "DECLARE": frozenset(),  # only DECLARE @variable ...
}

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
        # External storage readers: they take a URL, so the engine would
        # contact a host or path built from database content.
        "fn_xe_file_target_read_file",
        "fn_xe_telemetry_blob_target_read_file",
        "fn_get_audit_file",
        "fn_get_audit_file_v2",
    }
)
_BLOCKED_FUNCTION_WORDS = frozenset(name.upper() for name in BLOCKED_FUNCTIONS)


@dataclass(frozen=True)
class _LexedSql:
    code: str  # comments -> " ", string literals -> "?", quoted identifiers kept
    words: str  # as code, but quoted identifiers blanked for keyword scans
    terminated: bool
    pieces: tuple[str, ...] = ()  # the parts of code, in order
    spans: tuple[tuple[int, int], ...] = ()  # where each part is in the input


def _lex(sql: str) -> _LexedSql:
    """Scan T-SQL once, left to right, the way SQL Server's lexer does.

    Comments, string literals and quoted identifiers are recognized in the
    order they start, so a '/*' inside a string or a line comment cannot hide
    the code after it. Block comments nest, as they do in SQL Server.
    """
    code: list[str] = []
    words: list[str] = []
    spans: list[tuple[int, int]] = []
    terminated = True
    i, n = 0, len(sql)
    while i < n:
        if sql.startswith("--", i):
            # SQL Server ends a line comment at CR or LF. Ending it at other
            # line separators too only exposes more text to the keyword gate.
            end = i + 2
            while end < n and sql[end] not in _LINE_COMMENT_ENDS:
                end += 1
            code.append(" ")
            words.append(" ")
            spans.append((i, end))
        elif sql.startswith("/*", i):
            depth, end = 1, i + 2
            while end < n and depth:
                if sql.startswith("/*", end):
                    depth, end = depth + 1, end + 2
                elif sql.startswith("*/", end):
                    depth, end = depth - 1, end + 2
                else:
                    end += 1
            terminated = terminated and depth == 0
            code.append(" ")
            words.append(" ")
            spans.append((i, end))
        elif sql[i] in "'[\"":
            close = "]" if sql[i] == "[" else sql[i]
            end = i + 1
            while (end := sql.find(close, end)) >= 0 and sql.startswith(close * 2, end):
                end += 2
            if end < 0:
                terminated, end = False, n
            else:
                end += 1
            start = i
            if sql[i] == "'":
                # N'...' is one literal: drop a standalone N prefix.
                if code and code[-1] in ("N", "n") and not (
                    len(code) > 1 and (code[-2][-1:].isalnum() or code[-2][-1:] in "_@#$")
                ):
                    code.pop()
                    words.pop()
                    start = spans.pop()[0]
                code.append("?")
                words.append("?")
            else:
                code.append(sql[i:end])
                words.append(" ")
            spans.append((start, end))
        else:
            end = i + 1
            code.append(sql[i])
            words.append(sql[i])
            spans.append((i, end))
        i = end
    return _LexedSql("".join(code), "".join(words), terminated, tuple(code), tuple(spans))


def strip_literals_and_comments(sql: str) -> str:
    """Replace comments and string literals so text-rule scans only see code.

    Keyword patterns (GO, EXEC, WAITFOR, #temp, ...) must not fire on words
    that merely appear inside string data or comments.
    """
    return _lex(sql).code


def _code_words(sql: str, *, include_quoted: bool = False) -> list[str]:
    """Upper-cased words outside literals and comments.

    Words inside quoted identifiers are skipped unless include_quoted is set.
    NFKC folds compatibility forms (for example full-width letters) so they
    cannot disguise a keyword.
    """
    lexed = _lex(sql)
    words = unicodedata.normalize("NFKC", lexed.code if include_quoted else lexed.words)
    return [word.upper() for word in _CODE_WORD_PATTERN.findall(words)]


@dataclass(frozen=True)
class _Token:
    kind: str  # "word", "quoted", "string" or "punct"
    text: str  # a word upper-cased and NFKC-folded; a quoted name without its delimiters
    start: int  # offsets in the input text
    end: int


def _tokenize(lexed: _LexedSql) -> list[_Token]:
    """Tokens of the code that ``_lex`` found, with their place in the input."""

    tokens: list[_Token] = []
    word: list[str] = []
    word_start = word_end = 0

    def flush() -> None:
        if word:
            text = unicodedata.normalize("NFKC", "".join(word)).upper()
            tokens.append(_Token("word", text, word_start, word_end))
            word.clear()

    for piece, (start, end) in zip(lexed.pieces, lexed.spans):
        length = end - start
        if length == 1 and (piece.isalnum() or piece in "_@#$"):
            if not word:
                word_start = start
            word.append(piece)
            word_end = end
            continue
        flush()
        if length == 1:
            if not piece.isspace():
                tokens.append(_Token("punct", piece, start, end))
        elif piece == "?":
            tokens.append(_Token("string", piece, start, end))
        elif piece != " ":  # a comment is " "
            name = unicodedata.normalize("NFKC", piece[1:-1]).upper()
            tokens.append(_Token("quoted", name, start, end))
    flush()
    return tokens


# Table hints that change what a read sees or which locks it takes.
ISOLATION_TABLE_HINTS = frozenset(
    {
        "NOLOCK", "READUNCOMMITTED", "READPAST", "READCOMMITTED", "READCOMMITTEDLOCK",
        "REPEATABLEREAD", "SERIALIZABLE", "HOLDLOCK", "SNAPSHOT", "UPDLOCK", "XLOCK",
        "TABLOCK", "TABLOCKX",
    }
)
# Table hints that force an access path or a lock granularity.
PLAN_FORCING_TABLE_HINTS = frozenset(
    {
        "INDEX", "FORCESEEK", "FORCESCAN", "NOEXPAND", "ROWLOCK", "PAGLOCK", "NOWAIT",
        "SPATIAL_WINDOW_MAX_CELLS",
    }
)
# Query hints that change when a plan is compiled, not which plan shape is
# allowed. A candidate that only carries these is not "hinted".
COMPILE_QUERY_HINTS = frozenset({"RECOMPILE", "OPTIMIZE FOR"})
PLAN_FORCING_QUERY_HINTS = frozenset(
    {
        "MAXDOP", "FORCE ORDER", "LOOP JOIN", "HASH JOIN", "MERGE JOIN", "CONCAT UNION",
        "HASH UNION", "MERGE UNION", "HASH GROUP", "ORDER GROUP", "FAST", "USE HINT",
        "USE PLAN", "TABLE HINT", "QUERYTRACEON", "MAX_GRANT_PERCENT", "MIN_GRANT_PERCENT",
        "NO_PERFORMANCE_SPOOL", "EXPAND VIEWS", "IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX",
        "ROBUST PLAN", "KEEP PLAN", "KEEPFIXED PLAN", "MAXRECURSION", "PARAMETERIZATION",
        "DISABLE_OPTIMIZED_PLAN_FORCING", "FORCE EXTERNALPUSHDOWN",
        "DISABLE EXTERNALPUSHDOWN", "FORCE SCALEOUTEXECUTION", "DISABLE SCALEOUTEXECUTION",
    }
)
_JOIN_HINT_WORDS = frozenset({"LOOP", "HASH", "MERGE", "REMOTE"})
UNRECOGNIZED_QUERY_HINT = "UNRECOGNIZED QUERY HINT"
UNRECOGNIZED_TABLE_HINT = "UNRECOGNIZED TABLE HINT"

# USE HINT names a statement may carry through the read-only gate. Each one
# changes an optimizer choice only. This list is the statement-admission list;
# the Query Store hint list in query_hints.py is separate and may be narrower.
STATEMENT_USE_HINT_NAMES = frozenset(
    {
        "ASSUME_JOIN_PREDICATE_DEPENDS_ON_FILTERS",
        "ASSUME_MIN_SELECTIVITY_FOR_FILTER_ESTIMATES",
        "ASSUME_FULL_INDEPENDENCE_FOR_FILTER_ESTIMATES",
        "ASSUME_PARTIAL_CORRELATION_FOR_FILTER_ESTIMATES",
        "DISABLE_BATCH_MODE_ADAPTIVE_JOINS",
        "DISABLE_BATCH_MODE_MEMORY_GRANT_FEEDBACK",
        "DISABLE_DEFERRED_COMPILATION_TV",
        "DISABLE_INTERLEAVED_EXECUTION_TVF",
        "DISABLE_OPTIMIZED_NESTED_LOOP",
        "DISABLE_OPTIMIZER_ROWGOAL",
        "DISABLE_PARAMETER_SNIFFING",
        "DISABLE_ROW_MODE_MEMORY_GRANT_FEEDBACK",
        "DISABLE_TSQL_SCALAR_UDF_INLINING",
        "DISALLOW_BATCH_MODE",
        "ENABLE_HIST_AMENDMENT_FOR_ASC_KEYS",
        "ENABLE_QUERY_OPTIMIZER_HOTFIXES",
        "FORCE_DEFAULT_CARDINALITY_ESTIMATION",
        "FORCE_LEGACY_CARDINALITY_ESTIMATION",
        "QUERY_PLAN_PROFILE",
    }
)
_COMPATIBILITY_LEVEL_HINT = re.compile(r"QUERY_OPTIMIZER_COMPATIBILITY_LEVEL_\d{3}")
_USE_HINT_NAME = re.compile(r"[A-Za-z0-9_]+")
_OPTIMIZE_FOR_VARIABLE = re.compile(r"@[A-Z_#][A-Z0-9_@#$]*")
_UNSIGNED_INTEGER = re.compile(r"\d+")
_HEX_LITERAL = re.compile(r"0X[0-9A-F]*")


@dataclass(frozen=True)
class HintInventory:
    """The hints one statement carries, as keywords only.

    No object, index or variable name and no literal value is kept, so the
    inventory can be stored. ``trailing_option`` is the (start, end) offset of
    the OPTION clause that ends the statement, or None.
    """

    isolation: tuple[str, ...] = ()
    plan_forcing: tuple[str, ...] = ()
    compile: tuple[str, ...] = ()
    unknown: tuple[str, ...] = ()
    query_hints: tuple[str, ...] = ()  # the OPTION items, in the order written
    use_hint_names: tuple[str, ...] = ()
    trailing_option: tuple[int, int] | None = None

    @property
    def hinted(self) -> bool:
        """True when a hint forces a plan shape. A hint this scanner does not
        know counts: a hinted candidate is never a plain deployable winner."""

        return bool(self.plan_forcing or self.unknown)

    def as_profile(self) -> dict[str, Any]:
        return {
            "hinted": self.hinted,
            "plan_forcing_hints": sorted({*self.plan_forcing, *self.unknown}),
            "isolation_table_hints": list(self.isolation),
            "compile_options": list(self.compile),
        }


def _is_punct(token: _Token, text: str) -> bool:
    return token.kind == "punct" and token.text == text


def _matching_close(tokens: list[_Token], opening: int) -> int | None:
    depth = 0
    for index in range(opening, len(tokens)):
        if _is_punct(tokens[index], "("):
            depth += 1
        elif _is_punct(tokens[index], ")"):
            depth -= 1
            if depth == 0:
                return index
    return None


def _split_items(tokens: list[_Token]) -> list[list[_Token]]:
    """Split the tokens of one parenthesized list on its top-level commas."""

    items: list[list[_Token]] = [[]]
    depth = 0
    for token in tokens:
        if _is_punct(token, "("):
            depth += 1
        elif _is_punct(token, ")"):
            depth -= 1
        elif depth == 0 and _is_punct(token, ","):
            items.append([])
            continue
        items[-1].append(token)
    return items


def _table_hint_words(tokens: list[_Token]) -> list[str]:
    """Words at the top level of a table-hint list. A name after ``=`` and
    anything in nested parentheses (index and column names) is left out."""

    words: list[str] = []
    depth = 0
    value_next = False
    for token in tokens:
        if _is_punct(token, "("):
            depth += 1
        elif _is_punct(token, ")"):
            depth -= 1
        elif depth:
            continue
        elif _is_punct(token, "="):
            value_next = True
        elif token.kind in ("word", "quoted"):
            if not value_next:
                words.append(token.text)
            value_next = False
    return words


def _trailing_option(tokens: list[_Token]) -> tuple[int, int] | None:
    """Token indexes (OPTION, closing parenthesis) of the clause that ends the text."""

    end = len(tokens)
    while end and _is_punct(tokens[end - 1], ";"):
        end -= 1
    if not end or not _is_punct(tokens[end - 1], ")"):
        return None
    depth = 0
    for index, token in enumerate(tokens[:end]):
        if _is_punct(token, "("):
            if (
                depth == 0
                and index
                and tokens[index - 1].kind == "word"
                and tokens[index - 1].text == "OPTION"
                and _matching_close(tokens, index) == end - 1
            ):
                return index - 1, end - 1
            depth += 1
        elif _is_punct(token, ")"):
            depth -= 1
            if depth < 0:
                return None
    return None


def extract_hint_inventory(sql: str) -> HintInventory:
    """Inventory the table hints, join hints and query hints of a statement.

    Built on the validator's own lexer, so a hint word inside a string, a
    comment or a nested comment is never counted and one in code never missed.
    Used to mark a candidate as hinted and to find the trailing OPTION clause.
    """

    lexed = _lex(sql)
    tokens = _tokenize(lexed)
    isolation: set[str] = set()
    plan_forcing: set[str] = set()
    compile_options: set[str] = set()
    unknown: set[str] = set()
    query_hints: list[str] = []
    use_hint_names: list[str] = []

    def add_table_hints(words: list[str], *, strict: bool) -> None:
        for word in words:
            if word in ISOLATION_TABLE_HINTS:
                isolation.add(word)
            elif word in PLAN_FORCING_TABLE_HINTS:
                plan_forcing.add(word)
            elif strict:
                unknown.add(UNRECOGNIZED_TABLE_HINT)

    def add_query_hint(item: list[_Token]) -> None:
        leading: list[str] = []
        for token in item:
            if token.kind != "word":
                break
            leading.append(token.text)
        name = next(
            (
                candidate
                for candidate in (" ".join(leading[:2]), " ".join(leading[:1]))
                if candidate in COMPILE_QUERY_HINTS or candidate in PLAN_FORCING_QUERY_HINTS
            ),
            UNRECOGNIZED_QUERY_HINT,
        )
        if name in COMPILE_QUERY_HINTS:
            compile_options.add(name)
        elif name in PLAN_FORCING_QUERY_HINTS:
            plan_forcing.add(name)
        else:
            unknown.add(name)
        if name not in query_hints:
            query_hints.append(name)
        if name == "USE HINT":
            for token in item:
                if token.kind == "string":
                    hint_name = _string_literal_value(sql, token)
                    if _USE_HINT_NAME.fullmatch(hint_name) and hint_name.upper() not in use_hint_names:
                        use_hint_names.append(hint_name.upper())
        elif name == "TABLE HINT":
            arguments = _split_items(item[3:-1]) if len(item) > 3 else []
            for argument in arguments[1:]:  # the first names the table
                add_table_hints(_table_hint_words(argument), strict=True)

    for index, token in enumerate(tokens):
        if token.kind == "word" and token.text in _JOIN_HINT_WORDS:
            following = tokens[index + 1] if index + 1 < len(tokens) else None
            if following is not None and following.kind == "word" and following.text == "JOIN":
                plan_forcing.add(f"{token.text} JOIN")
        if not _is_punct(token, "("):
            continue
        close = _matching_close(tokens, index)
        if close is None:
            continue
        inner = tokens[index + 1 : close]
        previous = tokens[index - 1] if index else None
        keyword = previous.text if previous is not None and previous.kind == "word" else None
        if keyword == "OPTION":
            for item in _split_items(inner):
                if item:
                    add_query_hint(item)
        elif keyword == "WITH":
            # OPENJSON(...) WITH (...) declares columns; a table reference
            # WITH (...) lists hints. Known hint words count in both.
            declares_columns = index >= 2 and _is_punct(tokens[index - 2], ")")
            add_table_hints(_table_hint_words(inner), strict=not declares_columns)
        else:
            # The old syntax without WITH: FROM dbo.t (NOLOCK).
            words = _table_hint_words(inner)
            if words and all(
                word in ISOLATION_TABLE_HINTS or word in PLAN_FORCING_TABLE_HINTS
                for word in words
            ):
                add_table_hints(words, strict=False)

    trailing = _trailing_option(tokens)
    return HintInventory(
        isolation=tuple(sorted(isolation)),
        plan_forcing=tuple(sorted(plan_forcing)),
        compile=tuple(sorted(compile_options)),
        unknown=tuple(sorted(unknown)),
        query_hints=tuple(query_hints),
        use_hint_names=tuple(use_hint_names),
        trailing_option=(
            None if trailing is None else (tokens[trailing[0]].start, tokens[trailing[1]].end)
        ),
    )


def _string_literal_value(sql: str, token: _Token) -> str:
    text = sql[token.start : token.end]
    if text[:1] in ("N", "n"):
        text = text[1:]
    return text[1:-1].replace("''", "'")


def _is_plain_literal(tokens: list[_Token]) -> bool:
    """One string, NULL, or a number with an optional sign. No expression."""

    if len(tokens) == 1 and tokens[0].kind == "string":
        return True
    if tokens and tokens[0].kind == "punct" and tokens[0].text in "+-":
        tokens = tokens[1:]
    texts = [token.text for token in tokens]
    if any(token.kind != "word" for token in tokens[::2]):
        return False
    if len(texts) == 1:
        return (
            texts[0] == "NULL"
            or _UNSIGNED_INTEGER.fullmatch(texts[0]) is not None
            or _HEX_LITERAL.fullmatch(texts[0]) is not None
        )
    return (
        len(texts) == 3
        and _is_punct(tokens[1], ".")
        and _UNSIGNED_INTEGER.fullmatch(texts[0]) is not None
        and _UNSIGNED_INTEGER.fullmatch(texts[2]) is not None
    )


def _is_admitted_query_hint(sql: str, item: list[_Token]) -> bool:
    """True for one query hint the read-only gate admits in a trailing OPTION.

    The list is closed: RECOMPILE, OPTIMIZE FOR, MAXDOP, FORCE ORDER, the
    LOOP/MERGE/HASH JOIN hints and USE HINT with listed names. None of them
    can write or change the rows a SELECT returns. A delimited name, a
    non-ASCII word or any token outside the shapes below is refused.
    """

    for token in item:
        if token.kind == "quoted":
            return False
        if token.kind == "word" and not sql[token.start : token.end].isascii():
            return False
    shape = tuple("'" if token.kind == "string" else token.text for token in item)
    kinds = tuple(token.kind for token in item)
    if shape in (("RECOMPILE",), ("FORCE", "ORDER"), ("OPTIMIZE", "FOR", "UNKNOWN")):
        return kinds == ("word",) * len(shape)
    if len(shape) == 2 and kinds == ("word", "word"):
        if shape[0] in ("LOOP", "MERGE", "HASH") and shape[1] == "JOIN":
            return True
        return shape[0] == "MAXDOP" and re.fullmatch(r"\d{1,5}", shape[1]) is not None
    if len(item) < 5 or not _is_punct(item[2], "(") or not _is_punct(item[-1], ")"):
        return False
    arguments = item[3:-1]
    if any(_is_punct(token, "(") or _is_punct(token, ")") for token in arguments):
        return False
    if shape[:2] == ("USE", "HINT") and kinds[:2] == ("word", "word"):
        for argument in _split_items(arguments):
            if len(argument) != 1 or argument[0].kind != "string":
                return False
            name = _string_literal_value(sql, argument[0])
            if _USE_HINT_NAME.fullmatch(name) is None or not (
                name.upper() in STATEMENT_USE_HINT_NAMES
                or _COMPATIBILITY_LEVEL_HINT.fullmatch(name.upper())
            ):
                return False
        return True
    if shape[:2] == ("OPTIMIZE", "FOR") and kinds[:2] == ("word", "word"):
        for argument in _split_items(arguments):
            if (
                len(argument) < 2
                or argument[0].kind != "word"
                or _OPTIMIZE_FOR_VARIABLE.fullmatch(argument[0].text) is None
            ):
                return False
            value = argument[1:]
            if len(value) == 1 and value[0].kind == "word" and value[0].text == "UNKNOWN":
                continue
            if not (_is_punct(value[0], "=") and _is_plain_literal(value[1:])):
                return False
        return True
    return False


def _admitted_option_body(sql: str) -> str | None:
    """The statement text in front of an admitted trailing OPTION clause.

    None when the text does not end with one OPTION clause whose every hint
    is admitted.
    """

    lexed = _lex(sql)
    tokens = _tokenize(lexed)
    trailing = _trailing_option(tokens)
    if trailing is None:
        return None
    option_index, close_index = trailing
    items = _split_items(tokens[option_index + 2 : close_index])
    if not all(item and _is_admitted_query_hint(sql, item) for item in items):
        return None
    # A second OPTION in front of the clause is not T-SQL the gate understands.
    if any(token.kind == "word" and token.text == "OPTION" for token in tokens[:option_index]):
        return None
    return sql[: tokens[option_index].start]


@dataclass(frozen=True)
class ValidatedQuery:
    """A read-only query with both exact and compatibility SQL spellings.

    ``execution_sql`` is the text that must be sent to the driver.  It is
    intentionally not generated by sqlglot.  ``normalized_sql`` remains for
    callers using the original API and is now the trimmed exact text rather
    than a parser re-emission.  ``analysis_sql`` is only a parser artifact;
    for a statement admitted with a trailing OPTION clause the parser could
    not read, it is the statement without that clause.  ``statement_hints``
    names the query hints of the OPTION clause, without their values.
    """

    normalized_sql: str
    submitted_sql: str = ""
    execution_sql: str = ""
    analysis_sql: str = ""
    analysis_status: str = "supported"
    admission_status: str = "admitted"
    statement_hints: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        submitted = self.submitted_sql or self.normalized_sql
        execution = self.execution_sql or submitted
        object.__setattr__(self, "submitted_sql", submitted)
        object.__setattr__(self, "execution_sql", execution)
        object.__setattr__(self, "analysis_sql", self.analysis_sql or self.normalized_sql)


class StaticAnalysisStatus(StrEnum):
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    REJECTED = "rejected"


class AdmissionStatus(StrEnum):
    ADMITTED = "admitted"
    NOT_ADMITTED = "not_admitted"


@dataclass(frozen=True)
class StaticAnalysisOutcome:
    """The non-throwing result of a static-analysis/admission attempt."""

    analysis_status: StaticAnalysisStatus
    admission_status: AdmissionStatus
    reason: str | None = None
    submitted_sql: str = ""


class UnsupportedSqlError(ValueError):
    """Raised when SQL is not statically understood well enough to admit."""

    analysis_status = StaticAnalysisStatus.UNSUPPORTED
    admission_status = AdmissionStatus.NOT_ADMITTED

    def __init__(self, message: str, *, submitted_sql: str) -> None:
        super().__init__(message)
        self.submitted_sql = submitted_sql


class SafeSqlValidator:
    def validate_read_only(self, sql: str) -> ValidatedQuery:
        """Validate a read-only batch: optional DECLARE / SET @variable statements
        followed by exactly one SELECT-style statement.

        The variable prefix exists so parameterized queries can be executed with
        bound values (T-SQL variables are batch-scoped, so the DECLARE/SET block
        and the query must ship as a single batch).
        """
        if not isinstance(sql, str):
            raise ValueError("SQL must be a string.")
        candidate = sql.strip()
        if not candidate:
            raise ValueError("SQL cannot be empty.")
        self._check_text_rules(candidate)

        try:
            statements = parse(candidate, read="tsql")
        except ParseError as exc:
            self._check_statement_keywords(candidate)
            under_option = self._parse_under_trailing_option(candidate)
            if under_option is None:
                if _READ_ONLY_PREFIX_PATTERN.match(candidate):
                    raise UnsupportedSqlError(
                        "T-SQL could not be statically analyzed; admission is not allowed.",
                        submitted_sql=sql,
                    ) from exc
                raise ValueError(f"Invalid T-SQL: {exc}") from exc
            # The same checks as for any other statement follow.
            statements = under_option

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
        self._check_statement_keywords(candidate)

        analysis_sql = ";\n".join(
            statement.sql(dialect="tsql") for statement in statements if statement
        )
        return ValidatedQuery(
            normalized_sql=candidate,
            submitted_sql=sql,
            execution_sql=sql,
            analysis_sql=analysis_sql,
            statement_hints=extract_hint_inventory(candidate).query_hints,
        )

    @staticmethod
    def _parse_under_trailing_option(candidate: str) -> list[Any] | None:
        """Parse the statement in front of an admitted trailing OPTION clause.

        The parser cannot read OPTIMIZE FOR (@p = value) or USE HINT(...).
        When the text ends with one OPTION clause whose every hint is on the
        admitted list, the text in front of it is parsed and checked like any
        other statement. None means the text is not of that form.
        """

        body = _admitted_option_body(candidate)
        if body is None or not body.strip():
            return None
        try:
            return parse(body, read="tsql")
        except ParseError:
            return None

    def analyze_read_only(self, sql: str) -> StaticAnalysisOutcome:
        """Return a distinct admission outcome without admitting SQL.

        This is useful to orchestration code that needs to report the
        difference between unsafe SQL, invalid SQL, and safe-but-unsupported
        T-SQL without ever executing an unadmitted statement.
        """

        submitted = sql if isinstance(sql, str) else ""
        try:
            self.validate_read_only(sql)
        except UnsupportedSqlError as exc:
            return StaticAnalysisOutcome(
                analysis_status=StaticAnalysisStatus.UNSUPPORTED,
                admission_status=AdmissionStatus.NOT_ADMITTED,
                reason=str(exc),
                submitted_sql=exc.submitted_sql,
            )
        except ValueError as exc:
            return StaticAnalysisOutcome(
                analysis_status=StaticAnalysisStatus.REJECTED,
                admission_status=AdmissionStatus.NOT_ADMITTED,
                reason=str(exc),
                submitted_sql=submitted,
            )
        return StaticAnalysisOutcome(
            analysis_status=StaticAnalysisStatus.SUPPORTED,
            admission_status=AdmissionStatus.ADMITTED,
            submitted_sql=submitted,
        )

    def extract_table_references(self, sql: str) -> list[dict[str, str | None]]:
        """Return base table references from a validated read-only statement."""
        validated = self.validate_read_only(sql)
        execution_sql = validated.execution_sql.strip()
        try:
            parsed = parse(execution_sql, read="tsql")
        except ParseError:
            # Admitted above, so this is the trailing-OPTION form.
            parsed = self._parse_under_trailing_option(execution_sql) or []
        statements = [statement for statement in parsed if statement is not None]
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
        lexed = _lex(sql)
        if not lexed.terminated:
            raise ValueError("Unterminated string literal, quoted identifier, or comment.")
        candidate = lexed.code
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
        # Quoted hint names ([UPDLOCK], "TABLOCK") count too: fail closed.
        code_words = _code_words(sql, include_quoted=True)
        # The parse-tree check below does not see a delimited function name
        # such as [sys].[fn_get_sql](...), so the name itself is refused.
        blocked = sorted(_BLOCKED_FUNCTION_WORDS.intersection(code_words))
        if blocked:
            raise ValueError(
                f"Function '{blocked[0].lower()}' is not allowed in restricted mode."
            )
        hints = sorted(LOCKING_HINTS.intersection(code_words))
        if hints:
            raise ValueError(
                f"Locking hints ({', '.join(hints)}) are not allowed in restricted mode."
            )
        if MAXRECURSION_ZERO_PATTERN.search(candidate):
            raise ValueError(
                "MAXRECURSION 0 (unlimited recursion) is not allowed in restricted mode."
            )

    def _check_statement_keywords(self, sql: str) -> None:
        words = _code_words(sql)
        for position, word in enumerate(words):
            allowed_next = _CONDITIONAL_KEYWORDS.get(word)
            if word not in STATEMENT_KEYWORDS and allowed_next is None:
                continue
            if allowed_next is not None:
                following = words[position + 1] if position + 1 < len(words) else ""
                if following in allowed_next:
                    continue
                if word in ("SET", "DECLARE") and following.startswith("@"):
                    continue
            raise ValueError(
                f"Statement keyword '{word}' is not allowed in restricted mode. Only one "
                "SELECT statement, optionally preceded by DECLARE/SET @variable, may run."
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

            if isinstance(node, exp.NextValueFor):
                raise ValueError(
                    "NEXT VALUE FOR changes sequence state and is not allowed in restricted mode."
                )

            if isinstance(node, exp.Rand) and node.this is not None:
                raise ValueError(
                    "RAND(seed) reseeds the connection's random number generator and is not "
                    "allowed in restricted mode."
                )

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
