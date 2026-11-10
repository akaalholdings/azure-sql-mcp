"""Large results and TOP without ORDER BY are compared (S14 items 6b and 6c).

6b: a comparison stopped at the comparison row limit (10,000 rows by default)
and returned "inconclusive", so a rewrite of a statement that returns more
rows could never be shown to match. Rows above the limit are now compared by
a running digest that the driver boundary computes while it fetches in
batches: one for the ordered stream, one that ignores order.

6c: a statement with TOP and no ORDER BY was not compared at all. Both row
sets are now compared as multisets; a match is a tested match at most,
because the engine may choose other rows on another run.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from azure_sql_mcp.connection import ROW_DIGEST_BLOCK_ROWS
from azure_sql_mcp.connection import ROW_DIGEST_FETCH_ROWS
from azure_sql_mcp.connection import AzureSqlExecutor
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.connection import RowStreamDigest
from azure_sql_mcp.performance_workflows import compare_result_sets
from azure_sql_mcp.performance_workflows import label_comparison
from azure_sql_mcp.performance_workflows import preflight_gap_codes
from tests.unit.test_performance_workflows import RoutedPlans
from tests.unit.test_performance_workflows import SnapshotExecutor
from tests.unit.test_performance_workflows import _service

LIMIT = 10


class _Cursor:
    """A driver cursor over ``total`` rows that records the size of every fetch."""

    description = [("id",)]

    def __init__(self, total: int) -> None:
        self.pending = [(value,) for value in range(total)]
        self.fetch_sizes: list[int] = []

    def fetchmany(self, size: int) -> list[tuple[int]]:
        self.fetch_sizes.append(size)
        batch, self.pending = self.pending[:size], self.pending[size:]
        return batch

    def nextset(self) -> bool:
        return False


def _fetched(values: list[int]) -> QueryResult:
    """What the driver boundary returns for these rows at a row limit of LIMIT."""

    digest = RowStreamDigest()
    for value in values:
        digest.add((value,))
    return QueryResult(
        columns=("id",),
        rows=[{"id": value} for value in values[: LIMIT + 1]],
        column_type_signatures=("synthetic-int",),
        row_digest=digest.summary() if len(values) > LIMIT else None,
    )


def _compare(baseline: list[int], candidate: list[int], *, compare_order: bool = True) -> dict[str, Any]:
    return compare_result_sets(
        _fetched(baseline), _fetched(candidate),
        row_limit=LIMIT, compare_order=compare_order, same_snapshot=True,
    )


# --- 6b: the driver boundary -------------------------------------------------------


def test_rows_above_the_limit_are_fetched_in_batches_and_digested_not_held(
    sample_server_config,
) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    total = 3 * ROW_DIGEST_FETCH_ROWS + 7
    cursor = _Cursor(total)

    [result] = executor._consume_batches(
        cursor, max_rows=LIMIT + 1, stop_on_cap=False, digest_beyond_cap=True
    )

    # Memory is bounded: the first limit + 1 rows are kept, the rest pass through.
    assert len(result.rows) == LIMIT + 1
    assert max(cursor.fetch_sizes) == ROW_DIGEST_FETCH_ROWS and cursor.pending == []
    assert result.row_digest is not None
    assert result.row_digest["row_count"] == total
    expected = RowStreamDigest()
    for value in range(total):
        expected.add((value,))
    assert result.row_digest == expected.summary()


def test_a_result_within_the_limit_has_no_digest_and_one_fetch(sample_server_config) -> None:
    executor = AzureSqlExecutor(sample_server_config, MagicMock(), MagicMock())
    cursor = _Cursor(LIMIT)

    [result] = executor._consume_batches(
        cursor, max_rows=LIMIT + 1, stop_on_cap=False, digest_beyond_cap=True
    )

    assert result.row_digest is None and cursor.fetch_sizes == [LIMIT + 1]


# --- 6b: the comparison --------------------------------------------------------------


def test_a_large_result_in_the_same_order_matches_and_says_how_it_was_compared() -> None:
    rows = list(range(5_000))

    result = _compare(rows, rows)

    assert result["status"] == "match"
    assert result["rows_match"] is True
    assert result["proven_for_parameter_case"] is True
    assert result["compared_by_digest"] is True
    assert result["truncated"] is False
    assert (result["baseline_row_count"], result["candidate_row_count"]) == (5_000, 5_000)
    assert "more rows than the comparison row limit of 10" in result["digest_note"]
    label_comparison(result, [], True)
    assert (result["ordered_rows_match"], result["multiset_match"]) == (True, True)


def test_the_same_large_rows_in_another_order_are_an_order_only_mismatch() -> None:
    rows = list(range(5_000))
    reordered = [*rows[:4_000], *reversed(rows[4_000:])]

    result = _compare(rows, reordered)

    assert result["status"] == "order_only_mismatch"
    label_comparison(result, [], True)
    assert (result["ordered_rows_match"], result["multiset_match"]) == (False, True)
    # Without an outer ORDER BY the same rows are a match.
    assert _compare(rows, reordered, compare_order=False)["status"] == "match"


def test_a_row_that_differs_above_the_limit_is_a_mismatch_with_its_position_and_no_value() -> None:
    rows = list(range(5_000))
    changed = list(rows)
    changed[3_456] = 987_654_321

    result = _compare(rows, changed)

    assert result["status"] == "mismatch"
    block_start = 3_456 // ROW_DIGEST_BLOCK_ROWS * ROW_DIGEST_BLOCK_ROWS
    assert result["first_differences"]["first_differing_row_range"] == {
        "from": block_start + 1,
        "to": block_start + ROW_DIGEST_BLOCK_ROWS,
    }
    assert "987654321" not in str(result)
    # A difference inside the rows that are held is still named exactly.
    early = list(rows)
    early[4] = -1
    assert _compare(rows, early)["first_differences"]["first_differing_row"] == 5


def test_another_row_count_above_the_limit_is_a_mismatch_with_both_counts() -> None:
    result = _compare(list(range(5_000)), list(range(4_990)))

    assert result["status"] == "mismatch"
    assert (result["baseline_row_count"], result["candidate_row_count"]) == (5_000, 4_990)
    # The shorter stream ends inside a block: a changed row in that block, or
    # the first row that only the longer stream has.
    assert result["first_differences"]["first_differing_row_range"] == {"from": 4_001, "to": 4_991}
    # One side within the limit, the other above it.
    short = _compare(list(range(LIMIT)), list(range(5_000)))
    assert short["status"] == "mismatch"
    assert (short["baseline_row_count"], short["candidate_row_count"]) == (LIMIT, 5_000)


def test_a_duplicate_swapped_for_another_row_is_not_hidden_by_the_multiset_digest() -> None:
    rows = list(range(5_000))
    swapped = [*rows[:-1], 17]

    assert _compare(rows, swapped, compare_order=False)["status"] == "mismatch"


def test_a_truncated_result_without_a_digest_is_inconclusive_as_before() -> None:
    def plain(values: list[int]) -> QueryResult:
        return QueryResult(
            columns=("id",), rows=[{"id": value} for value in values],
            column_type_signatures=("synthetic-int",),
        )

    result = compare_result_sets(
        plain(list(range(LIMIT + 1))), plain(list(range(LIMIT + 1))),
        row_limit=LIMIT, compare_order=True, same_snapshot=True,
    )

    assert result["status"] == "inconclusive" and result["truncated"] is True


# --- 6c: TOP without ORDER BY ------------------------------------------------------

TOP = "SELECT TOP (3) i.id FROM dbo.Items AS i WHERE i.kind = 5"
TOP_REWRITE = "SELECT TOP (3) candidate.id FROM dbo.Items AS candidate WHERE candidate.kind = 5"


def _rows(*values: int) -> list[dict[str, Any]]:
    return [{"id": value} for value in values]


def test_an_unordered_row_limit_no_longer_blocks_the_comparison() -> None:
    gaps, blocking = preflight_gap_codes(
        {"baseline": {"risk_codes": ["unordered_row_limit"]}, "candidate": {"risk_codes": []}}
    )

    assert gaps == ["unordered_row_limit"] and blocking == []


@pytest.mark.asyncio
async def test_top_without_order_by_is_compared_as_a_multiset_and_is_a_tested_match() -> None:
    executor = SnapshotExecutor(baseline_rows=_rows(1, 2, 3), candidate_rows=_rows(3, 1, 2))
    service, _store, _plans, _executor = _service(executor=executor)

    result = await service.compare_query_results("appdb", TOP, TOP_REWRITE)

    assert len(executor.sessions) == 1
    assert result["status"] == "match"
    assert result["multiset_match"] is True
    assert result["tested_match"] is True
    assert result["proof_scope"] == "snapshot_test"
    assert result["proven_for_parameter_case"] is False
    assert any("TOP without ORDER BY" in caveat for caveat in result["caveats"])


@pytest.mark.asyncio
async def test_other_rows_under_top_without_order_by_are_reported_and_may_be_the_row_choice() -> None:
    executor = SnapshotExecutor(baseline_rows=_rows(1, 2, 3), candidate_rows=_rows(1, 2, 4))
    service, _store, _plans, _executor = _service(executor=executor)

    result = await service.compare_query_results("appdb", TOP, TOP_REWRITE)

    assert result["status"] == "mismatch"
    assert result["mismatch_may_be_row_choice"] is True
    assert "ORDER BY" in result["reason"]


@pytest.mark.asyncio
async def test_another_row_count_under_top_without_order_by_is_a_plain_mismatch() -> None:
    executor = SnapshotExecutor(baseline_rows=_rows(1, 2, 3), candidate_rows=_rows(1, 2))
    service, _store, _plans, _executor = _service(executor=executor)

    result = await service.compare_query_results("appdb", TOP, TOP_REWRITE)

    assert result["status"] == "mismatch"
    assert "mismatch_may_be_row_choice" not in result
    assert (result["baseline_row_count"], result["candidate_row_count"]) == (3, 2)


@pytest.mark.asyncio
async def test_a_finalist_with_top_and_no_order_by_is_compared_and_never_proven() -> None:
    executor = SnapshotExecutor(baseline_rows=_rows(1, 2, 3), candidate_rows=_rows(2, 3, 1))
    service, _store, _plans, _executor = _service(
        plans=RoutedPlans(baseline_ms=100, candidate_ms=20), executor=executor
    )
    case = service.start_case("appdb", TOP)
    session_id = service.start_session(case.case_id, "appdb")["session_id"]
    candidate = service.add_candidate(session_id, TOP_REWRITE, strategy="predicate")

    result = await service.benchmark_candidate(
        session_id, candidate["candidate_id"], "appdb", TOP, TOP_REWRITE, phase="finalist"
    )

    assert len(executor.sessions) == 1
    comparison = result["equivalence"][0]
    assert comparison["status"] == "match" and comparison["tested_match"] is True
    assert comparison["proven_for_parameter_case"] is False
    assert result["classification"] == "performance_only"
