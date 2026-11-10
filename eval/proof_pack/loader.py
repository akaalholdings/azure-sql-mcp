"""Loader for the proof adversarial pack.

Each pair in pairs.json is a baseline query and a rewrite that return the same
rows on one small data set (`equal_on`) and different rows on another
(`differs_on`). Both data sets obey the declared schema, so the rewrite is not
equivalent in general. A proof that accepts a pair is wrong.

The pairs marked `portable` use only SQL that SQLite and Azure SQL Database
read the same way. `run_pair` runs those in an in-memory SQLite database, so
the expected rows are checked without a server.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any

FORMAT_VERSION = 1
PACK_FILE = Path(__file__).resolve().parent / "pairs.json"
STATES = ("equal_on", "differs_on")
# The same kinds as catalog_constraints.PRECONDITION_KINDS in the server.
PRECONDITION_KINDS = ("not_null", "unique", "trusted_foreign_key", "trusted_check")

Rows = list[tuple[Any, ...]]


def load_proof_pack(path: Path = PACK_FILE) -> dict[str, Any]:
    """Read and validate the pack. An invalid pack raises ValueError."""
    pack = json.loads(path.read_text(encoding="utf-8"))
    problems = validate_proof_pack(pack)
    if problems:
        raise ValueError("\n".join(problems))
    return pack


def expected_rows(pair: Mapping[str, Any], state: str) -> tuple[Rows, Rows]:
    """(baseline rows, rewrite rows) that the pair returns on one data set."""
    expected = pair["expected"]
    if state == "equal_on":
        rows = _tuples(expected["equal_rows"])
        return rows, list(rows)
    if state == "differs_on":
        return _tuples(expected["baseline_rows"]), _tuples(expected["rewrite_rows"])
    raise ValueError(f"unknown state {state!r}; use one of {STATES}")


def ddl_statements(pack: Mapping[str, Any]) -> list[str]:
    """T-SQL that creates the pack's schema and tables."""
    statements = [f"CREATE SCHEMA {pack['schema']};"]
    for table, spec in pack["tables"].items():
        lines = [
            f"    {column['name']} {column['type']} {'NULL' if column['nullable'] else 'NOT NULL'}"
            for column in spec["columns"]
        ]
        name = table.split(".")[-1]
        lines.append(f"    CONSTRAINT PK_{name} PRIMARY KEY ({', '.join(spec['primary_key'])})")
        statements.append(f"CREATE TABLE {table} (\n" + ",\n".join(lines) + "\n);")
    return statements


def load_statements(pack: Mapping[str, Any], pair: Mapping[str, Any], state: str) -> list[str]:
    """T-SQL that empties every pack table and inserts one data set of a pair."""
    statements = [f"DELETE FROM {table};" for table in pack["tables"]]
    for table, rows in _state(pair, state).items():
        columns = ", ".join(column["name"] for column in pack["tables"][table]["columns"])
        for row in rows:
            values = ", ".join(_literal(value) for value in row)
            statements.append(f"INSERT INTO {table} ({columns}) VALUES ({values});")
    return statements


def run_pair(pack: Mapping[str, Any], pair: Mapping[str, Any], state: str) -> tuple[Rows, Rows]:
    """Run baseline and rewrite of a portable pair in SQLite on one data set."""
    if not pair.get("portable"):
        raise ValueError(f"{pair['id']} is not portable: run it on Azure SQL Database")
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute(f"ATTACH DATABASE ':memory:' AS {pack['schema']}")
        for table, spec in pack["tables"].items():
            columns = ", ".join(column["name"] for column in spec["columns"])
            connection.execute(f"CREATE TABLE {table} ({columns})")
        for table, rows in _state(pair, state).items():
            marks = ", ".join("?" for _ in pack["tables"][table]["columns"])
            connection.executemany(f"INSERT INTO {table} VALUES ({marks})", rows)
        parameters = pair.get("parameters") or {}
        baseline = connection.execute(pair["baseline_sql"], parameters).fetchall()
        rewrite = connection.execute(pair["rewrite_sql"], parameters).fetchall()
    finally:
        connection.close()
    return baseline, rewrite


def validate_proof_pack(pack: Mapping[str, Any]) -> list[str]:
    """Return every problem in the pack. An empty list means valid."""
    problems: list[str] = []
    if pack.get("format_version") != FORMAT_VERSION:
        problems.append(f"format_version must be {FORMAT_VERSION}")
    tables = pack.get("tables") or {}
    seen: set[str] = set()
    for pair in pack.get("pairs") or []:
        name = str(pair.get("id") or "<pair>")
        if name in seen:
            problems.append(f"{name}: the id is used twice")
        seen.add(name)
        found = _pair_problems(pair, tables)
        problems += [f"{name}: {problem}" for problem in found]
        if not found and pair.get("portable"):
            problems += [f"{name}: {problem}" for problem in _execution_problems(pack, pair)]
    if not seen:
        problems.append("the pack has no pairs")
    return problems


def _pair_problems(pair: Mapping[str, Any], tables: Mapping[str, Any]) -> list[str]:
    problems = [
        f"missing `{field}`"
        for field in ("id", "baseline_sql", "rewrite_sql", "reason", "tells_apart", "expected")
        if not pair.get(field) or (isinstance(pair[field], str) and not pair[field].strip())
    ]
    if not isinstance(pair.get("would_be_equivalent_if"), list):
        problems.append("`would_be_equivalent_if` must be a list; use [] and `no_constraint_because` when no constraint helps")
    if not pair.get("portable") and not str(pair.get("not_portable_because") or "").strip():
        problems.append("a pair that is not portable must say why in `not_portable_because`")
    for state in STATES:
        if not isinstance(pair.get(state), dict):
            problems.append(f"missing data set `{state}`")
    if set(pair.get("parameter_types") or {}) != set(pair.get("parameters") or {}):
        problems.append("`parameter_types` must give a T-SQL type for each parameter")
    if problems:
        return problems
    for condition in pair["would_be_equivalent_if"]:
        problems += _condition_problems(condition, tables)
    if not pair["would_be_equivalent_if"] and not str(pair.get("no_constraint_because") or "").strip():
        problems.append("say in `no_constraint_because` why no constraint makes the pair equivalent")
    for state in STATES:
        problems += [f"{state}: {problem}" for problem in _data_problems(pair[state], tables)]
    baseline, rewrite = expected_rows(pair, "differs_on")
    if _ordered(baseline) == _ordered(rewrite):
        problems.append("the expected rows on differs_on are the same: the data does not tell the pair apart")
    return problems


def _condition_problems(condition: Mapping[str, Any], tables: Mapping[str, Any]) -> list[str]:
    kind, table = condition.get("kind"), str(condition.get("table"))
    if kind not in PRECONDITION_KINDS:
        return [f"precondition kind must be one of {PRECONDITION_KINDS}"]
    spec = tables.get(table)
    if spec is None:
        return [f"a precondition names the unknown table {table!r}"]
    columns = list(condition.get("columns") or [])
    known = {column["name"]: column for column in spec["columns"]}
    if any(column not in known for column in columns):
        return [f"a precondition names a column that {table} does not have"]
    # A constraint the schema already has cannot be what the pair lacks.
    held = False
    if kind == "not_null":
        held = bool(columns) and all(not known[column]["nullable"] for column in columns)
    elif kind == "unique":
        keys = [spec["primary_key"], *(spec.get("unique") or [])]
        held = any(set(key) <= set(columns) for key in keys)
    elif kind == "trusted_foreign_key":
        held = any(key["columns"] == columns for key in spec.get("foreign_keys") or [])
    else:
        held = condition.get("definition") in (spec.get("checks") or [])
    if held:
        return [f"the schema already enforces {kind} on {table} ({', '.join(columns)}), so it cannot be the missing constraint"]
    return []


def _data_problems(state: Mapping[str, Any], tables: Mapping[str, Any]) -> list[str]:
    problems: list[str] = []
    for table, rows in state.items():
        spec = tables.get(table)
        if spec is None:
            problems.append(f"unknown table {table!r}")
            continue
        names = [column["name"] for column in spec["columns"]]
        if any(len(row) != len(names) for row in rows):
            problems.append(f"{table}: each row needs {len(names)} columns")
            continue
        for index, column in enumerate(spec["columns"]):
            if not column["nullable"] and any(row[index] is None for row in rows):
                problems.append(f"{table}.{column['name']} is NOT NULL but the data holds a NULL")
        for key in [spec["primary_key"], *(spec.get("unique") or [])]:
            values = [tuple(row[names.index(column)] for column in key) for row in rows]
            if len(set(values)) != len(values):
                problems.append(f"{table}: duplicate value for the key ({', '.join(key)})")
    return problems


def _execution_problems(pack: Mapping[str, Any], pair: Mapping[str, Any]) -> list[str]:
    problems: list[str] = []
    for state in STATES:
        try:
            actual = run_pair(pack, pair, state)
        except sqlite3.Error as error:
            return [f"{state}: SQLite cannot run the pair: {error}"]
        for label, got, want in zip(("baseline", "rewrite"), actual, expected_rows(pair, state)):
            if _ordered(got) != _ordered(want):
                problems.append(f"{state}: {label} rows are not the expected rows: got {_ordered(got)}")
        if state == "differs_on" and _ordered(actual[0]) == _ordered(actual[1]):
            problems.append("differs_on: both queries return the same rows: the data does not tell the pair apart")
        if state == "equal_on" and not actual[0]:
            problems.append("equal_on: the baseline returns no rows, and a match on no rows proves nothing")
    return problems


def _state(pair: Mapping[str, Any], state: str) -> Mapping[str, Any]:
    if state not in STATES:
        raise ValueError(f"unknown state {state!r}; use one of {STATES}")
    return pair[state]


def _tuples(rows: list[list[Any]]) -> Rows:
    return [tuple(row) for row in rows]


def _ordered(rows: Rows) -> Rows:
    return sorted(rows, key=repr)


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"
