"""One hint scanner, and admission of a trailing OPTION clause.

The tuning harness must be able to run a candidate with OPTION (RECOMPILE),
OPTIMIZE FOR or USE HINT, the remedies an expert tries first. The read-only
gate may admit them only as a trailing clause of a SELECT, from an explicit
list of hints that cannot write or change results. Everything else that the
gate rejected before must still be rejected.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from azure_sql_mcp.safe_sql import AdmissionStatus
from azure_sql_mcp.safe_sql import SafeSqlValidator
from azure_sql_mcp.safe_sql import UnsupportedSqlError
from azure_sql_mcp.safe_sql import extract_hint_inventory

CORPUS = Path(__file__).resolve().parents[1] / "fixtures" / "safe_sql_attack_corpus.json"


# --- the inventory ------------------------------------------------------------


def test_inventory_separates_isolation_plan_forcing_and_compile_hints() -> None:
    inventory = extract_hint_inventory(
        "SELECT p.Id FROM dbo.Posts AS p WITH (NOLOCK, INDEX(IX_Posts_Owner)) "
        "INNER LOOP JOIN dbo.Users AS u WITH (FORCESEEK) ON u.Id = p.OwnerUserId "
        "WHERE p.OwnerUserId = @owner "
        "OPTION (RECOMPILE, MAXDOP 1, OPTIMIZE FOR (@owner = 7), "
        "USE HINT('FORCE_LEGACY_CARDINALITY_ESTIMATION'))"
    )

    assert inventory.as_profile() == {
        "hinted": True,
        "plan_forcing_hints": ["FORCESEEK", "INDEX", "LOOP JOIN", "MAXDOP", "USE HINT"],
        "isolation_table_hints": ["NOLOCK"],
        "compile_options": ["OPTIMIZE FOR", "RECOMPILE"],
    }
    assert inventory.query_hints == ("RECOMPILE", "MAXDOP", "OPTIMIZE FOR", "USE HINT")
    assert inventory.use_hint_names == ("FORCE_LEGACY_CARDINALITY_ESTIMATION",)


def test_compile_options_alone_do_not_make_a_candidate_hinted() -> None:
    # RECOMPILE and OPTIMIZE FOR are the standard parameter-sensitivity
    # rewrites; a candidate that uses them can still be deployment-ready.
    compile_only = extract_hint_inventory(
        "SELECT p.Id FROM dbo.Posts AS p WHERE p.OwnerUserId = @o "
        "OPTION (RECOMPILE, OPTIMIZE FOR UNKNOWN)"
    )
    forced = extract_hint_inventory("SELECT p.Id FROM dbo.Posts AS p OPTION (MAXDOP 1)")

    assert compile_only.hinted is False
    assert compile_only.compile == ("OPTIMIZE FOR", "RECOMPILE")
    assert forced.hinted is True


def test_hint_words_in_literals_comments_and_names_are_not_hints() -> None:
    inventory = extract_hint_inventory(
        "SELECT p.Id, 'WITH (NOLOCK) OPTION (MAXDOP 1)' AS note "
        "/* FORCESEEK /* WITH (NOLOCK) */ INNER HASH JOIN */ "
        "FROM dbo.[Hash Join] AS p -- WITH (READPAST)\n"
        "WHERE p.Title = N'LOOP JOIN' OPTION (RECOMPILE)"
    )

    assert inventory.hinted is False
    assert inventory.isolation == ()
    assert inventory.compile == ("RECOMPILE",)


@pytest.mark.parametrize(
    ("sql", "isolation", "plan_forcing"),
    [
        # Hints separated by spaces: deprecated, and still accepted by the engine.
        ("SELECT a FROM dbo.t WITH (NOLOCK FORCESEEK)", ("NOLOCK",), ("FORCESEEK",)),
        ("SELECT a FROM dbo.t WITH (INDEX = IX_a)", (), ("INDEX",)),
        ("SELECT a FROM dbo.t WITH (FORCESEEK(IX_a(a)), READPAST)", ("READPAST",), ("FORCESEEK",)),
        # The old syntax without WITH.
        ("SELECT a FROM dbo.t AS x (HOLDLOCK)", ("HOLDLOCK",), ()),
        ("SELECT a FROM dbo.t x (NOLOCK, INDEX(1))", ("NOLOCK",), ("INDEX",)),
        # A table hint given as a query hint.
        (
            "SELECT a FROM dbo.t OPTION (TABLE HINT(t, NOLOCK, INDEX(IX_a)))",
            ("NOLOCK",),
            ("INDEX", "TABLE HINT"),
        ),
        # A delimited hint name: counted, so the check fails closed.
        ("SELECT a FROM dbo.t WITH ([NOLOCK])", ("NOLOCK",), ()),
        ("SELECT a FROM dbo.t WITH (UPDLOCK, ROWLOCK)", ("UPDLOCK",), ("ROWLOCK",)),
        ("SELECT a FROM dbo.t x LEFT MERGE JOIN dbo.u y ON x.a = y.a", (), ("MERGE JOIN",)),
        ("SELECT a FROM dbo.t OPTION (QUERYTRACEON 8649, FORCE ORDER)", (), ("FORCE ORDER", "QUERYTRACEON")),
    ],
)
def test_inventory_finds_each_hint_form(sql, isolation, plan_forcing) -> None:
    inventory = extract_hint_inventory(sql)

    assert inventory.isolation == isolation
    assert inventory.plan_forcing == plan_forcing
    assert inventory.hinted is bool(plan_forcing)


def test_unrecognized_hints_count_as_hinted_and_carry_no_text() -> None:
    # A hint this scanner does not know may force a plan: fail closed. The
    # inventory is persisted, so it must hold keywords only.
    inventory = extract_hint_inventory(
        "SELECT a FROM dbo.t WITH (SECRET_TABLE_HINT) WHERE a = @c "
        "OPTION (SECRET_QUERY_HINT 5, OPTIMIZE FOR (@c = 424242), "
        "USE HINT('not a hint name'))"
    )

    assert inventory.hinted is True
    assert inventory.unknown == ("UNRECOGNIZED QUERY HINT", "UNRECOGNIZED TABLE HINT")
    dumped = json.dumps([inventory.as_profile(), inventory.query_hints, inventory.use_hint_names])
    for secret in ("SECRET", "424242", "not a hint name"):
        assert secret not in dumped
    assert "IX_SECRET" not in json.dumps(
        extract_hint_inventory("SELECT a FROM dbo.t WITH (INDEX(IX_Secret))").as_profile()
    ).upper()


def test_openjson_schema_and_cte_column_lists_are_not_table_hints() -> None:
    inventory = extract_hint_inventory(
        "WITH c (a, b) AS (SELECT 1, 2) "
        "SELECT c.a, j.id FROM c CROSS APPLY OPENJSON(@doc) WITH (id int '$.id', name sysname) AS j"
    )

    assert inventory.hinted is False
    assert inventory.unknown == ()


@pytest.mark.parametrize(
    ("sql", "clause"),
    [
        ("SELECT 1 OPTION (RECOMPILE)", "OPTION (RECOMPILE)"),
        ("SELECT 1 OPTION(MAXDOP 1) ; ", "OPTION(MAXDOP 1)"),
        (
            "SELECT a FROM dbo.t WHERE a IN (SELECT 1) OPTION (OPTIMIZE FOR (@c = N')'))",
            "OPTION (OPTIMIZE FOR (@c = N')'))",
        ),
        ("SELECT a FROM (SELECT 1 AS a OPTION (RECOMPILE)) AS q", None),
        ("SELECT 'OPTION (RECOMPILE)'", None),
        ("SELECT 1 OPTION (RECOMPILE) SELECT 2", None),
        ("SELECT 1 -- OPTION (RECOMPILE)", None),
    ],
)
def test_trailing_option_span_is_the_exact_clause_text(sql, clause) -> None:
    span = extract_hint_inventory(sql).trailing_option

    assert (None if span is None else sql[span[0] : span[1]]) == clause


# --- admission ----------------------------------------------------------------

ADMITTED_WITH_OPTION = [
    "SELECT a FROM dbo.t WHERE a = @c OPTION (OPTIMIZE FOR (@c = 5))",
    "SELECT a FROM dbo.t WHERE a = @c OPTION (OPTIMIZE FOR (@c UNKNOWN))",
    "SELECT a FROM dbo.t WHERE a = @c OPTION (OPTIMIZE FOR (@c = N'x', @d = -1.5))",
    "SELECT a FROM dbo.t OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT a FROM dbo.t OPTION (USE HINT('QUERY_OPTIMIZER_COMPATIBILITY_LEVEL_150'))",
    "SELECT a FROM dbo.t OPTION (RECOMPILE, USE HINT('DISABLE_PARAMETER_SNIFFING', 'DISALLOW_BATCH_MODE'))",
    "SELECT a FROM dbo.t x JOIN dbo.u y ON x.a = y.a "
    "OPTION (MAXDOP 1, FORCE ORDER, LOOP JOIN, OPTIMIZE FOR (@c = 5));",
    "DECLARE @c int = 5; SELECT a FROM dbo.t WHERE a = @c OPTION (OPTIMIZE FOR (@c = 5))",
    "WITH q AS (SELECT a FROM dbo.t) SELECT a FROM q UNION ALL SELECT a FROM dbo.u "
    "OPTION (HASH JOIN, USE HINT('FORCE_LEGACY_CARDINALITY_ESTIMATION'))",
]


@pytest.mark.parametrize("sql", ADMITTED_WITH_OPTION)
def test_select_with_an_allowlisted_trailing_option_is_admitted(sql: str) -> None:
    validated = SafeSqlValidator().validate_read_only(sql)

    # The text sent to the engine is the caller's exact text.
    assert validated.execution_sql == sql
    assert "OPTION" not in validated.analysis_sql.upper()
    assert validated.statement_hints
    assert not any(character.isdigit() or character == "'" for hint in validated.statement_hints for character in hint)


def test_statement_hints_name_the_kinds_without_values() -> None:
    validated = SafeSqlValidator().validate_read_only(
        "SELECT a FROM dbo.t WHERE a = @c "
        "OPTION (RECOMPILE, OPTIMIZE FOR (@c = 424242), USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))"
    )

    assert validated.statement_hints == ("RECOMPILE", "OPTIMIZE FOR", "USE HINT")
    # A clause the parser reads on its own is reported the same way.
    parsed = SafeSqlValidator().validate_read_only("SELECT a FROM dbo.t OPTION (MAXDOP 2)")
    assert parsed.statement_hints == ("MAXDOP",)
    assert SafeSqlValidator().validate_read_only("SELECT 1").statement_hints == ()


def test_table_references_are_read_from_a_hinted_statement() -> None:
    # Preflight and index analysis need the tables of a hinted candidate.
    references = SafeSqlValidator().extract_table_references(
        "SELECT p.Id FROM dbo.Posts AS p JOIN dbo.Users AS u ON u.Id = p.OwnerUserId "
        "OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))"
    )

    assert references == [
        {"schema": "dbo", "table": "Posts"},
        {"schema": "dbo", "table": "Users"},
    ]


STILL_REJECTED = [
    # Hint names that are not on the list.
    "SELECT a FROM dbo.t OPTION (USE HINT('NOT_A_HINT'))",
    "SELECT a FROM dbo.t OPTION (USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE'))",
    "SELECT a FROM dbo.t OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL' + 'X'))",
    "SELECT a FROM dbo.t OPTION (USE HINT(@name))",
    "SELECT a FROM dbo.t OPTION (USE HINT())",
    # Query hints that are not on the list, alone or next to a listed one.
    "SELECT a FROM dbo.t OPTION (TABLE HINT(t, PAGLOCK))",
    "SELECT a FROM dbo.t OPTION (TABLE HINT(t, INDEX(1)))",
    "SELECT a FROM dbo.t OPTION (TABLE HINT(t, NOLOCK), USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT a FROM dbo.t OPTION (QUERYTRACEON 8649, USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT a FROM dbo.t OPTION (USE PLAN N'<p/>', USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT a FROM dbo.t OPTION (FOR TIMESTAMP AS OF '2020')",
    "WITH r AS (SELECT 1 AS n) SELECT n FROM r "
    "OPTION (MAXRECURSION 0, USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    # Values that are not plain literals.
    "SELECT a FROM dbo.t WHERE a = @c OPTION (OPTIMIZE FOR (@c = (SELECT MAX(a) FROM dbo.t)))",
    "SELECT a FROM dbo.t WHERE a = @c OPTION (OPTIMIZE FOR (@c = dbo.f(1)))",
    "SELECT a FROM dbo.t WHERE a = @c OPTION (OPTIMIZE FOR (@c = 1 + 1))",
    "SELECT a FROM dbo.t WHERE a = @c OPTION (OPTIMIZE FOR (@@SPID = 1))",
    "SELECT a FROM dbo.t OPTION (MAXDOP (SELECT 1), USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT a FROM dbo.t OPTION ([RECOMPILE], USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT a FROM dbo.t OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'),)",
    "SELECT a FROM dbo.t OPTION ()",
    # The clause is not the end of one SELECT.
    "SELECT a FROM dbo.t OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL')) DELETE FROM dbo.t",
    "SELECT a FROM dbo.t OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL')); DROP TABLE dbo.t",
    "SELECT a FROM dbo.t OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL')) SELECT 2",
    "SELECT a FROM dbo.t OPTION (RECOMPILE) OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT a FROM (SELECT 1 AS a OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))) AS q",
    "SELECT 1 SELECT a FROM dbo.t OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    # The statement under the clause is not an admitted SELECT.
    "UPDATE dbo.t SET a = 1 OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "DELETE FROM dbo.t OPTION (OPTIMIZE FOR (@c = 5))",
    "SELECT a INTO dbo.copy FROM dbo.t OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT a FROM dbo.t WITH (TABLOCKX) OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT a FROM #t OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT a FROM otherdb.dbo.t OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT NEXT VALUE FOR dbo.s OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT a FROM OPENROWSET('x', 'y', 'z') OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "EXEC dbo.p OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT a FROM dbo.t WHERE OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
]


@pytest.mark.parametrize("sql", STILL_REJECTED)
def test_everything_else_with_an_option_clause_is_still_rejected(sql: str) -> None:
    outcome = SafeSqlValidator().analyze_read_only(sql)

    assert outcome.admission_status is AdmissionStatus.NOT_ADMITTED


def test_unparsed_option_clause_keeps_the_unsupported_error_class() -> None:
    # Callers tell "unsafe" from "not understood" by the error class.
    with pytest.raises(UnsupportedSqlError):
        SafeSqlValidator().validate_read_only(
            "SELECT a FROM dbo.t OPTION (USE HINT('NOT_A_HINT'))"
        )


@pytest.mark.parametrize("zero", ["00", "000", "0x0", "0e0"])
def test_unlimited_recursion_is_rejected_however_zero_is_written(zero: str) -> None:
    # MAXRECURSION 0 lets a recursive CTE run until the query timeout.
    outcome = SafeSqlValidator().analyze_read_only(
        f"WITH c AS (SELECT 1 AS n) SELECT n FROM c OPTION (MAXRECURSION {zero})"
    )

    assert outcome.admission_status is AdmissionStatus.NOT_ADMITTED


# --- the attack corpus --------------------------------------------------------

# The only corpus inputs whose decision changes: a SELECT that ends with an
# allowlisted OPTION clause, which the parser could not read before.
NEWLY_ADMITTED = {
    "SELECT * FROM dbo.t OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT * FROM dbo.t OPTION (USE HINT('FORCE_LEGACY_CARDINALITY_ESTIMATION'))",
    "SELECT 1 OPTION (USE HINT('FORCE_LEGACY_CARDINALITY_ESTIMATION'))",
    "SELECT * FROM t OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT 1 OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL'))",
    "SELECT * FROM t OPTION (USE HINT('FORCE_LEGACY_CARDINALITY_ESTIMATION'))",
}

# The decision each of those inputs had on 2026-10-02, before this change.
PINNED_BEFORE = dict.fromkeys(NEWLY_ADMITTED, False)


def _corpus_cases() -> list[dict]:
    fixture = json.loads(CORPUS.read_text(encoding="utf-8"))
    materialized = [
        {
            "sql": entry.get("sql") or entry["prefix"] + entry["repeat"] * entry["count"] + entry["suffix"],
            "admitted": entry["admitted"],
        }
        for entry in fixture["materialized"]
    ]
    return fixture["cases"] + materialized


def test_attack_corpus_admits_nothing_new_except_the_listed_option_selects() -> None:
    validator = SafeSqlValidator()
    cases = _corpus_cases()
    assert len(cases) >= 1422

    newly_admitted = set()
    newly_rejected = []
    for case in cases:
        before = PINNED_BEFORE.get(case["sql"], case["admitted"])
        now = validator.analyze_read_only(case["sql"]).admission_status is AdmissionStatus.ADMITTED
        if now and not before:
            newly_admitted.add(case["sql"])
        elif before and not now:
            newly_rejected.append(case["sql"][:200])

    assert newly_admitted == NEWLY_ADMITTED
    assert newly_rejected == []
    for sql in NEWLY_ADMITTED:
        inventory = extract_hint_inventory(sql)
        assert inventory.trailing_option is not None
        body = sql[: inventory.trailing_option[0]]
        # The statement under the clause was admitted before, on its own.
        assert validator.analyze_read_only(body).admission_status is AdmissionStatus.ADMITTED
        assert inventory.query_hints == ("USE HINT",)


@pytest.mark.parametrize(
    "clause",
    [
        "OPTION (OPTIMIZE FOR (@c = 5))",
        "OPTION (RECOMPILE, OPTIMIZE FOR (@p UNKNOWN), MAXDOP 1, FORCE ORDER, HASH JOIN)",
    ],
)
def test_an_admitted_option_clause_never_admits_a_statement_rejected_on_its_own(
    clause: str,
) -> None:
    # The clause adds hints to a statement; it must not carry one past the gate.
    validator = SafeSqlValidator()
    admitted = []
    for case in _corpus_cases():
        if PINNED_BEFORE.get(case["sql"], case["admitted"]):
            continue
        for joined in (f"{case['sql']} {clause}", f"{case['sql']}\n{clause}"):
            if validator.analyze_read_only(joined).admission_status is AdmissionStatus.ADMITTED:
                admitted.append(joined[:200])

    assert admitted == []
