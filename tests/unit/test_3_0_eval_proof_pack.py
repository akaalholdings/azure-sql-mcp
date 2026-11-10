"""The proof adversarial pack: pairs that match on one data set and differ on another.

Each pair is a rewrite that returns the same rows as its baseline today and
different rows after a change the schema allows. A proof that calls such a
pair equivalent because today's rows match would let a wrong rewrite reach
production. The pack is only useful if each pair really behaves as it says,
so the portable pairs are executed here, in SQLite, on both data sets.
"""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "eval" / "proof_pack" / "loader.py"


def _load():
    spec = importlib.util.spec_from_file_location("eval_proof_pack_loader", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


loader = _load()
PACK: dict[str, Any] = loader.load_proof_pack()
PAIRS = {pair["id"]: pair for pair in PACK["pairs"]}
PORTABLE = [pair for pair in PACK["pairs"] if pair["portable"]]


def _sorted(rows: list[tuple]) -> list[tuple]:
    return sorted(rows, key=repr)


def _broken(pair_id: str, **changes: Any) -> dict[str, Any]:
    pack = copy.deepcopy(PACK)
    pair = next(item for item in pack["pairs"] if item["id"] == pair_id)
    pair.update(changes)
    pack["pairs"] = [pair]
    return pack


def test_shipped_pack_is_valid() -> None:
    assert loader.validate_proof_pack(PACK) == []
    assert len(PAIRS) == len(PACK["pairs"]) >= 12


@pytest.mark.parametrize("pair", PORTABLE, ids=lambda pair: pair["id"])
def test_pair_matches_on_the_first_data_set_and_differs_on_the_second(pair: dict[str, Any]) -> None:
    baseline, rewrite = loader.run_pair(PACK, pair, "equal_on")
    expected_baseline, expected_rewrite = loader.expected_rows(pair, "equal_on")
    assert _sorted(baseline) == _sorted(rewrite) == _sorted(expected_baseline) == _sorted(expected_rewrite)
    assert baseline, "a match on zero rows proves nothing"

    baseline, rewrite = loader.run_pair(PACK, pair, "differs_on")
    expected_baseline, expected_rewrite = loader.expected_rows(pair, "differs_on")
    assert _sorted(baseline) == _sorted(expected_baseline)
    assert _sorted(rewrite) == _sorted(expected_rewrite)
    assert _sorted(baseline) != _sorted(rewrite)


def test_pack_holds_the_cases_the_release_gate_names() -> None:
    # NOT IN against NOT EXISTS with a NULL, JOIN to EXISTS with duplicates, and a removed DISTINCT.
    for pair_id in ("not_in_null_in_list", "not_in_null_outer_value", "join_to_exists_duplicates", "distinct_removed", "row_number_drops_ties"):
        assert pair_id in PAIRS, pair_id


def test_most_pairs_are_executed_and_the_rest_say_why_not() -> None:
    not_portable = [pair for pair in PACK["pairs"] if not pair["portable"]]

    assert len(PORTABLE) >= 12
    assert len(not_portable) <= 3
    for pair in not_portable:
        assert pair["not_portable_because"].strip(), pair["id"]


@pytest.mark.parametrize("pair", PACK["pairs"], ids=lambda pair: pair["id"])
def test_pair_states_the_reason_and_the_data_that_tells_it_apart(pair: dict[str, Any]) -> None:
    assert pair["reason"].strip()
    assert pair["tells_apart"].strip()
    assert pair["equal_on"] != pair["differs_on"]
    for condition in pair["would_be_equivalent_if"]:
        assert condition["kind"] in loader.PRECONDITION_KINDS
    if not pair["would_be_equivalent_if"]:
        assert pair["no_constraint_because"].strip(), pair["id"]


def test_every_parameter_has_a_type_for_the_live_run() -> None:
    # Untyped parameters would be bound as nvarchar and change the plan, or the comparison, on the server.
    with_parameters = [pair for pair in PACK["pairs"] if pair["parameters"]]

    assert with_parameters
    for pair in PACK["pairs"]:
        assert set(pair["parameter_types"]) == set(pair["parameters"]), pair["id"]
        for name in pair["parameters"]:
            assert f"@{name}" in pair["baseline_sql"], pair["id"]
    untyped = _broken("or_to_union_all", parameter_types={})
    assert any("type" in problem for problem in loader.validate_proof_pack(untyped))


def test_no_pair_is_ever_deployment_ready() -> None:
    assert PACK["invariant"] == {"max_proof_scope": "snapshot_matched", "deployment_ready": False}


def test_expected_rows_feed_a_fake_executor() -> None:
    pair = PAIRS["not_in_null_in_list"]

    assert loader.expected_rows(pair, "equal_on") == ([(1,), (3,)], [(1,), (3,)])
    assert loader.expected_rows(pair, "differs_on") == ([], [(1,), (3,)])
    with pytest.raises(ValueError, match="state"):
        loader.expected_rows(pair, "tomorrow")


def test_run_pair_refuses_a_pair_that_sqlite_cannot_run() -> None:
    not_portable = next(pair for pair in PACK["pairs"] if not pair["portable"])

    with pytest.raises(ValueError, match="not portable"):
        loader.run_pair(PACK, not_portable, "equal_on")


def test_ddl_declares_the_schema_the_pairs_rely_on() -> None:
    ddl = "\n".join(loader.ddl_statements(PACK))

    assert ddl.startswith("CREATE SCHEMA pp;")
    assert "CREATE TABLE pp.Suppressions" in ddl
    # The nullable column is the whole point of the NOT IN pairs.
    assert "Email varchar(100) NULL" in ddl
    assert "CONSTRAINT PK_Customers PRIMARY KEY (CustomerID)" in ddl
    assert "REFERENCES" not in ddl, "no foreign key is declared: the join-removal pairs depend on that"


def test_load_statements_reset_the_tables_and_insert_the_state() -> None:
    pair = PAIRS["not_in_null_in_list"]

    statements = loader.load_statements(PACK, pair, "differs_on")

    assert statements[: len(PACK["tables"])] == [f"DELETE FROM {table};" for table in PACK["tables"]]
    assert "INSERT INTO pp.Suppressions (SuppressionID, Email) VALUES (2, NULL);" in statements
    assert "INSERT INTO pp.Customers (CustomerID, Email, Tier, ReferredByCustomerID) VALUES (1, 'a@x.test', 'S', NULL);" in statements


def test_load_statements_escape_quotes() -> None:
    pack = copy.deepcopy(PACK)
    pair = pack["pairs"][0]
    pair["equal_on"] = {"pp.Regions": [[1, "O'Brien's"]]}

    assert "INSERT INTO pp.Regions (RegionID, RegionName) VALUES (1, 'O''Brien''s');" in loader.load_statements(pack, pair, "equal_on")


# ------------------------------------------------------------------ validation


def test_validation_rejects_a_pair_that_the_data_does_not_tell_apart() -> None:
    pair = PAIRS["distinct_removed"]
    pack = _broken("distinct_removed", differs_on=pair["equal_on"])

    problems = loader.validate_proof_pack(pack)

    assert any("does not tell" in problem for problem in problems)


def test_validation_rejects_an_equivalent_rewrite() -> None:
    # A pair whose rewrite is the baseline would teach the proof to refuse a correct rewrite.
    pair = PAIRS["join_to_exists_duplicates"]
    pack = _broken("join_to_exists_duplicates", rewrite_sql=pair["baseline_sql"])

    assert any("rewrite rows" in problem for problem in loader.validate_proof_pack(pack))


def test_validation_rejects_expected_rows_that_the_queries_do_not_return() -> None:
    pair = copy.deepcopy(PAIRS["not_in_null_in_list"])
    pair["expected"]["equal_rows"] = [[1], [2], [3]]
    pack = _broken("not_in_null_in_list", expected=pair["expected"])

    assert any("equal_on" in problem and "expected" in problem for problem in loader.validate_proof_pack(pack))


def test_validation_rejects_a_missing_constraint_that_the_schema_has() -> None:
    # CustomerID is the primary key. If a pair needed that to be equivalent, it would be equivalent.
    pack = _broken(
        "distinct_removed",
        would_be_equivalent_if=[{"kind": "unique", "table": "pp.Customers", "columns": ["CustomerID"]}],
    )

    assert any("already" in problem for problem in loader.validate_proof_pack(pack))


@pytest.mark.parametrize(
    ("state", "needle"),
    [
        ({"pp.Customers": [[1, "a@x.test", None, None]]}, "NOT NULL"),
        ({"pp.Customers": [[1, "a@x.test", "S", None], [1, "b@x.test", "S", None]]}, "duplicate"),
        ({"pp.Customers": [[1, "a@x.test"]]}, "columns"),
        ({"pp.Nowhere": [[1]]}, "unknown table"),
    ],
    ids=["null-in-not-null-column", "duplicate-primary-key", "short-row", "unknown-table"],
)
def test_validation_rejects_data_the_schema_would_refuse(state: dict[str, Any], needle: str) -> None:
    # Data that cannot be loaded on the bench database tells nothing apart there.
    pack = _broken("distinct_removed", differs_on=state)

    assert any(needle in problem for problem in loader.validate_proof_pack(pack))


def test_load_proof_pack_raises_on_an_invalid_file(tmp_path: Path) -> None:
    pack = _broken("distinct_removed", reason="")
    path = tmp_path / "pairs.json"
    path.write_text(json.dumps(pack), encoding="utf-8")

    with pytest.raises(ValueError, match="reason"):
        loader.load_proof_pack(path)
