"""Index portfolio review: findings 50 and 51, partition reasons, purge-stable ids, cost.

The removal gate itself is unchanged: 35 days, readable-replica caution,
constraint and foreign-key protection.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest

from azure_sql_mcp.index_review import IndexReviewNotFoundError
from azure_sql_mcp.index_review import IndexReviewService
from azure_sql_mcp.index_review import IndexReviewSnapshotV1
from azure_sql_mcp.index_review import database_fingerprint
from azure_sql_mcp.index_review import review_index_portfolio
from tests.unit.test_index_review import _BASE_TIME
from tests.unit.test_index_review import _history
from tests.unit.test_index_review import _index
from tests.unit.test_index_review import _service_over
from tests.unit.test_index_review import _snapshot


def _subject(
    ordinal: int,
    keys: tuple[str, ...],
    *,
    reads: int = 0,
    table: str = "Orders",
    includes: tuple[str, ...] = (),
) -> dict[str, Any]:
    """One nonclustered index of dbo.<table> with its own identity and keys."""

    subject = _index(name=f"IX_{table}_{'_'.join(keys)}", reads=reads)
    subject["subject_id"] = f"index:{table}:{ordinal}"
    subject["index_id"] = ordinal
    subject["table_name"] = table
    definition = subject["definition"]["reversible_definition"]
    definition["index_id"] = ordinal
    definition["table"] = table
    definition["key_columns"] = [{"name": key, "direction": "ASC"} for key in keys]
    definition["include_columns"] = list(includes)
    subject["definition_fingerprint"] = f"definition-{table}-{ordinal}"
    subject["subject_fingerprint"] = f"subject-{table}-{ordinal}"
    return subject


def _daily(days: int, make_subjects) -> list[IndexReviewSnapshotV1]:
    return [_snapshot(day, subjects=tuple(make_subjects(day))) for day in range(days)]


def _states(review) -> dict[str, dict[str, Any]]:
    return {str(subject["index_name"]): dict(subject) for subject in review.subjects}


# --- partitioned indexes appear with their reason ---------------------------


def test_unused_index_on_a_partition_scheme_is_kept_with_the_partition_reason() -> None:
    """It used to say only protected_or_used, the same as a primary key or an index in use."""

    def subjects(day: int) -> list[dict[str, Any]]:
        subject = _index()
        subject["protections"]["partition_switch_dependency"] = True
        return [subject]

    subject = review_index_portfolio("appdb", _daily(40, subjects)).subjects[0]

    assert subject["state"] == "keep"
    assert subject["reason_codes"] == ["protected_or_used", "partition_switch_dependency"]

    # An index in use or a constraint keeps the plain reason: the partition is not why it stays.
    def partitioned_and_read(day: int) -> list[dict[str, Any]]:
        subject = _index(reads=day)
        subject["protections"]["partition_switch_dependency"] = True
        return [subject]

    read = review_index_portfolio("appdb", _daily(40, partitioned_and_read)).subjects[0]
    assert read["reason_codes"] == ["protected_or_used"]
    primary = review_index_portfolio("appdb", _daily(40, lambda day: [_index(protected=True)])).subjects[0]
    assert primary["reason_codes"] == ["protected_or_used"]


# --- 51: stacks of prefix indexes --------------------------------------------


def test_stack_of_unused_prefix_indexes_is_dropped_whole() -> None:
    """IX(a), IX(a,b), IX(a,b,c), none read in 40 days: the widest one costs the most and goes too."""

    def subjects(day: int) -> list[dict[str, Any]]:
        return [_subject(11, ("a",)), _subject(12, ("a", "b")), _subject(13, ("a", "b", "c"))]

    states = _states(review_index_portfolio("appdb", _daily(40, subjects)))

    assert {name: subject["state"] for name, subject in states.items()} == {
        "IX_Orders_a": "drop_candidate",
        "IX_Orders_a_b": "drop_candidate",
        "IX_Orders_a_b_c": "drop_candidate",
    }
    assert all("survivor_reference" not in subject for subject in states.values())


def test_survivor_of_a_consolidation_is_never_itself_proposed_for_removal() -> None:
    """The widest index is read, so it stays; both narrower ones name it, not each other."""

    def subjects(day: int) -> list[dict[str, Any]]:
        return [
            _subject(11, ("a",)),
            _subject(12, ("a", "b")),
            _subject(13, ("a", "b", "c"), reads=day * 3),
        ]

    states = _states(review_index_portfolio("appdb", _daily(40, subjects)))

    assert states["IX_Orders_a_b_c"]["state"] == "keep"
    for name in ("IX_Orders_a", "IX_Orders_a_b"):
        assert states[name]["state"] == "consolidate_candidate"
        assert states[name]["survivor_reference"] == "[dbo].[Orders].[IX_Orders_a_b_c]"


def test_nearest_kept_index_is_the_survivor() -> None:
    """The middle index is younger than the window, so it is observed and stays: it is the survivor."""

    def subjects(day: int) -> list[dict[str, Any]]:
        stack = [_subject(11, ("a",)), _subject(13, ("a", "b", "c"), reads=day)]
        if day >= 20:
            stack.append(_subject(12, ("a", "b")))
        return stack

    states = _states(review_index_portfolio("appdb", _daily(40, subjects)))

    assert states["IX_Orders_a_b"]["state"] == "observe"
    assert states["IX_Orders_a"]["state"] == "consolidate_candidate"
    assert states["IX_Orders_a"]["survivor_reference"] == "[dbo].[Orders].[IX_Orders_a_b]"


# --- 50: a stale review id cannot open the actionable projection -------------


def _real_fingerprint(snapshots: list[IndexReviewSnapshotV1]) -> list[IndexReviewSnapshotV1]:
    fingerprint = database_fingerprint("server.database.windows.net", "appdb")
    return [replace(snapshot, database_fingerprint=fingerprint) for snapshot in snapshots]


@pytest.mark.asyncio
async def test_stale_review_id_does_not_resolve_to_the_actionable_projection() -> None:
    snapshots = _real_fingerprint(_history(40))
    last = _BASE_TIME + timedelta(days=39)
    service = _service_over(snapshots, clock=lambda: last + timedelta(days=10))

    stale = await service.review_portfolio("appdb")
    assert stale.observation["history_status"] == "stale"
    assert stale.overall_state == "inconclusive" and stale.subjects == ()

    explicit = await service.review_portfolio("appdb", as_of_run_id=stale.as_of_run_id)
    assert explicit.overall_state == "actionable"
    # One id, one result.
    assert stale.review_id != explicit.review_id
    with pytest.raises(IndexReviewNotFoundError):
        await service.get_review("appdb", stale.review_id)


@pytest.mark.asyncio
async def test_review_of_an_old_capture_is_marked_historical_with_its_age() -> None:
    """Asking for an old run by id is allowed; the answer must say how old its evidence is."""

    snapshots = _real_fingerprint(_history(40))
    last = _BASE_TIME + timedelta(days=39)
    old = _service_over(snapshots, clock=lambda: last + timedelta(days=10))
    explicit = await old.review_portfolio("appdb", as_of_run_id=snapshots[-1].run_id)

    assert explicit.observation["history_status"] == "historical"
    assert explicit.observation["stale_hours"] == pytest.approx(240.0)
    fetched = await old.get_review("appdb", explicit.review_id)
    assert fetched.observation["history_status"] == "historical"

    fresh = _service_over(snapshots, clock=lambda: last + timedelta(hours=3))
    current = await fresh.review_portfolio("appdb")
    assert "history_status" not in current.observation
    assert current.review_id == explicit.review_id


# --- review ids survive a retention purge ------------------------------------


def test_review_id_does_not_change_when_captures_older_than_the_window_are_purged() -> None:
    history = _history(70)
    full = review_index_portfolio("appdb", history)

    # Keep the 35-day window and the 48 hours before it that the gate can read.
    purged = review_index_portfolio("appdb", history[-38:])

    assert purged.review_id == full.review_id
    assert [dict(subject) for subject in purged.subjects] == [dict(subject) for subject in full.subjects]

    # A capture missing inside the window is different evidence, so a different id.
    without_one = review_index_portfolio("appdb", history[:50] + history[51:])
    assert without_one.review_id != full.review_id


# --- cost grows with the number of indexes, not with its square --------------


class _CountingSubjects(tuple):
    """A capture's subject list that counts how often it is walked."""

    walks = 0

    def __iter__(self):
        self.walks += 1
        return super().__iter__()


def test_each_capture_is_indexed_once_instead_of_searched_once_per_index() -> None:
    """2,000 indexes over 40 captures took 23 s: every index searched every capture's subject list."""

    def subjects(day: int) -> list[dict[str, Any]]:
        return [_subject(ordinal, (f"c{ordinal}",), table=f"T{ordinal // 5}") for ordinal in range(30)]

    history = _daily(40, subjects)
    expected = review_index_portfolio("appdb", history)
    for snapshot in history:
        object.__setattr__(snapshot, "subjects", _CountingSubjects(snapshot.subjects))

    review = review_index_portfolio("appdb", history)

    assert review.as_dict() == expected.as_dict()
    # 30 walks per capture before the change: one for each index.
    assert max(snapshot.subjects.walks for snapshot in history) <= 3


# --- per-table memo for the candidate reads ----------------------------------


def test_candidate_sizing_reads_each_table_once() -> None:
    calls: list[str] = []

    class Session:
        def fetch_all(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
            if "write_ratio" in sql:
                calls.append("write_ratio")
                return [{"write_ratio": 0.25}]
            if "dm_db_partition_stats" in sql:
                calls.append("row_count")
                return [{"row_count": 1000}]
            calls.append("widths:" + ",".join(params[3:]))
            return [{"column_name": name, "max_length": 4} for name in params[3:]]

    def candidate(*columns: str) -> dict[str, Any]:
        return {
            "schema_name": "dbo",
            "table_name": "Orders",
            "equality_columns": list(columns),
            "inequality_columns": [],
            "include_columns": [],
        }

    memo: dict[tuple[str, ...], Any] = {}
    session = Session()
    first = IndexReviewService._estimate_candidate_size(session, candidate("a", "b"), memo=memo)  # type: ignore[arg-type]
    second = IndexReviewService._estimate_candidate_size(session, candidate("b", "c"), memo=memo)  # type: ignore[arg-type]
    third = IndexReviewService._estimate_candidate_size(session, candidate("a", "c"), memo=memo)  # type: ignore[arg-type]
    ratios = [
        IndexReviewService._collect_candidate_write_ratio(session, candidate("a"), memo=memo)  # type: ignore[arg-type]
        for _ in range(3)
    ]

    assert first == second == third and first is not None
    assert ratios == [0.25, 0.25, 0.25]
    # One row count and one write ratio for the table; a column's width is read once.
    assert calls == ["row_count", "widths:a,b", "widths:c", "write_ratio"]
    # Without a memo every call reads again, as before.
    assert IndexReviewService._estimate_candidate_size(session, candidate("a", "b")) == first  # type: ignore[arg-type]
    assert calls[-2:] == ["row_count", "widths:a,b"]

