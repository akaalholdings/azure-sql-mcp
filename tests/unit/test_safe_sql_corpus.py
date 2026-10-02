"""Regression gate: the 2026-10-02 validator attack corpus, pinned.

Every input keeps the admission decision it had when the fixture was made.
Any change to _lex or to admission that flips a decision fails here and must
be reviewed case by case. Independently of the pinned decisions, no admitted
input may carry a side-effect construct in code.

"cases" holds the sweep rows as typed. Some of them only describe an input
(escape text, a note, a placeholder); "materialized" holds those real inputs.
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp.safe_sql import LOCKING_HINTS
from azure_sql_mcp.safe_sql import STATEMENT_KEYWORDS
from azure_sql_mcp.safe_sql import AdmissionStatus
from azure_sql_mcp.safe_sql import SafeSqlValidator
from azure_sql_mcp.safe_sql import _code_words
from azure_sql_mcp.safe_sql import strip_literals_and_comments

CORPUS = Path(__file__).resolve().parents[1] / "fixtures" / "safe_sql_attack_corpus.json"
SOURCE_INPUT_COUNT = 1422

# The validator's lexer also ends a -- comment at VT, FF, NEL, U+2028 and
# U+2029 so its keyword gate sees more text. SQL Server ends it only at CR or
# LF, so text after those characters is still comment and runs nothing.
# Judge side effects the way SQL Server reads the batch.
_NOT_LINE_ENDS_IN_SQL_SERVER = str.maketrans(dict.fromkeys("\x0b\x0c\x85\u2028\u2029", " "))
_SEEDED_RAND = re.compile(r"\bRAND\s*\(\s*[^\s)]", re.IGNORECASE)

# Written out here, not imported from safe_sql: a word removed from the
# validator's sets must still be caught by this check.
_KEYWORDS_THAT_START_A_STATEMENT = frozenset(
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
_HINTS_THAT_TAKE_LOCKS = frozenset(
    {
        "UPDLOCK", "XLOCK", "TABLOCK", "TABLOCKX", "HOLDLOCK", "SERIALIZABLE",
        "REPEATABLEREAD", "READCOMMITTEDLOCK",
    }
)


def _load_fixture() -> dict[str, Any]:
    return json.loads(CORPUS.read_text(encoding="utf-8"))


def _materialize(entry: dict[str, Any]) -> str:
    if "sql" in entry:
        return entry["sql"]
    return entry["prefix"] + entry["repeat"] * entry["count"] + entry["suffix"]


def _load_cases() -> list[dict[str, Any]]:
    """The sweep rows as typed, plus the real inputs that some rows only describe."""
    fixture = _load_fixture()
    materialized = [
        {"sql": _materialize(entry), "admitted": entry["admitted"]}
        for entry in fixture["materialized"]
    ]
    return fixture["cases"] + materialized


def _side_effect_markers(sql: str) -> list[str]:
    """Side-effect constructs in code (not in literals, comments or [names])."""
    as_sql_server_reads_it = sql.translate(_NOT_LINE_ENDS_IN_SQL_SERVER)
    words = _code_words(as_sql_server_reads_it)
    markers = sorted(_KEYWORDS_THAT_START_A_STATEMENT.intersection(words))
    markers += sorted(_HINTS_THAT_TAKE_LOCKS.intersection(words))
    if any(words[i : i + 3] == ["NEXT", "VALUE", "FOR"] for i in range(len(words))):
        markers.append("NEXT VALUE FOR")
    code = unicodedata.normalize("NFKC", strip_literals_and_comments(as_sql_server_reads_it))
    if _SEEDED_RAND.search(code):
        markers.append("RAND(seed)")
    return markers


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT 1 UPDATE dbo.t SET c = 1", ["UPDATE"]),
        ("SELECT * FROM dbo.t WITH (TABLOCK)", ["TABLOCK"]),
        ("SELECT NEXT/**/VALUE/**/FOR dbo.s", ["NEXT VALUE FOR"]),
        ("SELECT RAND (@seed)", ["RAND(seed)"]),
        ("SELECT RAND()", []),
        ("SELECT 'UPDATE t WITH (TABLOCK)', RAND(/* no seed */)", []),
        ("SELECT [DELETE] FROM dbo.t -- NEXT VALUE FOR dbo.s", []),
        ("SELECT 1 --c\nSELECT NEXT VALUE FOR dbo.s", ["NEXT VALUE FOR"]),
        ("SELECT 1 --c\u2028SELECT NEXT VALUE FOR dbo.s", []),
    ],
)
def test_side_effect_markers_follow_sql_server_lexing(sql, expected):
    assert _side_effect_markers(sql) == expected


def test_validator_word_sets_keep_every_word_this_check_uses():
    # A word removed from the validator must fail here, not pass silently
    # through a check that reads the same set.
    assert not _KEYWORDS_THAT_START_A_STATEMENT - STATEMENT_KEYWORDS
    assert not _HINTS_THAT_TAKE_LOCKS - LOCKING_HINTS


def test_corpus_fixture_keeps_all_sweep_inputs_without_duplicates():
    sweep_rows = _load_fixture()["cases"]
    sqls = [case["sql"] for case in _load_cases()]

    assert len(sweep_rows) >= SOURCE_INPUT_COUNT
    assert len(set(sqls)) == len(sqls)


def test_each_materialized_input_traces_to_its_sweep_row():
    fixture = _load_fixture()
    sweep_sqls = {case["sql"] for case in fixture["cases"]}

    assert fixture["materialized"]
    assert [e["from"] for e in fixture["materialized"] if e["from"] not in sweep_sqls] == []


# Some sweep rows hold "\x85" as four characters, an English note or a
# "<30000 'x' chars>" placeholder. The gate must run the input they describe.
@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("SELECT 1 --c\x80DROP TABLE t", id="C1 control in comment"),
        pytest.param("SELECT 1 --c\tINSERT INTO t VALUES(1)", id="TAB in comment"),
        pytest.param("SELECT 1 --c\x85SELECT 2 INTO t", id="SELECT INTO after NEL"),
        pytest.param("SELECT 1 --c\x85ALTER SEQUENCE s RESTART WITH 1", id="ALTER after NEL"),
        pytest.param("SELECT 1 --c\x85EXEC xp_cmdshell 'dir'", id="xp_cmdshell after NEL"),
        pytest.param(
            "SELECT 1 --" + "x" * 30000 + "\u2029INSERT INTO t VALUES(1)",
            id="long comment then PS",
        ),
        pytest.param("SELECT " + "+".join(["1"] * 5000), id="5000-term expression"),
        pytest.param("SELECT 1 /* a /* b */ c */ , 2", id="nested comment, note removed"),
    ],
)
def test_corpus_runs_the_real_input_a_sweep_row_describes(sql):
    assert sql in {case["sql"] for case in _load_cases()}


def test_every_corpus_input_keeps_its_pinned_admission_decision():
    validator = SafeSqlValidator()
    flipped = []
    for case in _load_cases():
        outcome = validator.analyze_read_only(case["sql"])
        admitted = outcome.admission_status is AdmissionStatus.ADMITTED
        if admitted != case["admitted"]:
            flipped.append((case["sql"][:200], case["admitted"], admitted, outcome.reason))

    assert not flipped, (
        f"{len(flipped)} corpus decision(s) changed (sql, pinned, now, reason). Review each "
        f"one; if the change is intended, update its 'admitted' value in {CORPUS.name}: "
        f"{flipped!r}"
    )


def test_no_admitted_corpus_input_has_a_side_effect_construct():
    unsafe = [
        (case["sql"][:200], markers)
        for case in _load_cases()
        if case["admitted"] and (markers := _side_effect_markers(case["sql"]))
    ]

    assert not unsafe, f"admitted inputs with side-effect constructs: {unsafe!r}"
