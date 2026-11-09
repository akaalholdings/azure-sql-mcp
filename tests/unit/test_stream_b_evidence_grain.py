"""INDEX_EVIDENCE_QUERY is read at plan grain (review findings 19 and 20).

One row per plan x runtime interval, each with the full plan XML, filled the
10,000-row cap with about 12 hourly plans over 35 days. Coverage was then
"capped" on any real database and no removal verdict could be reached.
"""

from __future__ import annotations

import re
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

import azure_sql_mcp.index_review as index_review
from azure_sql_mcp.index_review import MAX_CAPTURE_ROWS
from azure_sql_mcp.index_review import QUERY_STORE_WINDOW_SQL
from azure_sql_mcp.index_review import CaptureContext
from azure_sql_mcp.index_review import IndexReviewService
from azure_sql_mcp.index_review import _snapshot_params
from azure_sql_mcp.query_store import INDEX_EVIDENCE_QUERY
from azure_sql_mcp.query_store import QueryStoreService
from tests.unit.test_index_history_repository import _RealCaptureExecutor
from tests.unit.test_index_history_repository import _RealDatabaseSession
from tests.unit.test_query_regression import QueryStoreSqlite
from tests.unit.test_query_regression import _translate_tsql
from tests.unit.test_query_store import sqlite_index_evidence_query

WINDOW_MINUTES = 35 * 1440

PLAN_XML = """
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence><Batch><Statements>
    <StmtSimple QueryPlanHash="0x0102030405060708" StatementSubTreeCost="2.5">
      <QueryPlan><RelOp PhysicalOp="Index Seek" LogicalOp="Index Seek">
        <IndexScan><Object Database="[appdb]" Schema="[Sales]" Table="[Orders]" Index="[IX_Orders_CustomerID]" /></IndexScan>
        <MissingIndexes><MissingIndexGroup Impact="90"><MissingIndex Database="[appdb]" Schema="[Sales]" Table="[Orders]">
          <ColumnGroup Usage="EQUALITY"><Column Name="[SalespersonPersonID]" /></ColumnGroup>
          <ColumnGroup Usage="INCLUDE"><Column Name="[OrderDate]" /></ColumnGroup>
        </MissingIndex></MissingIndexGroup></MissingIndexes>
      </RelOp></QueryPlan>
    </StmtSimple>
  </Statements></Batch></BatchSequence>
</ShowPlanXML>
"""


def _store(
    plans: int, intervals: int, *, first_interval_id: int = 1, interval_minutes: int = 60
) -> QueryStoreSqlite:
    store = QueryStoreSqlite()
    top = datetime.now(UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    interval_ids = range(first_interval_id, first_interval_id + intervals)
    for offset, interval_id in enumerate(interval_ids):
        start = top - timedelta(minutes=interval_minutes * (intervals - offset))
        store.add_interval(interval_id, start, minutes=interval_minutes)
    for plan_id in range(1, plans + 1):
        store.insert("query_store_query", query_id=plan_id, query_text_id=plan_id, is_internal_query=0)
        store.insert(
            "query_store_plan",
            plan_id=plan_id,
            query_id=plan_id,
            query_plan=PLAN_XML,
            is_forced_plan=0,
            query_plan_hash="0x0102030405060708",
        )
    store.conn.executemany(
        "INSERT INTO sys.query_store_runtime_stats "
        "(plan_id, runtime_stats_interval_id, execution_type, count_executions, avg_cpu_time) "
        "VALUES (?, ?, 0, 10, 5.0)",
        [(plan_id, interval_id) for plan_id in range(1, plans + 1) for interval_id in interval_ids],
    )
    return store


def _run_evidence_query(store: QueryStoreSqlite, params: list) -> list[dict]:
    sql, bound = _translate_tsql(sqlite_index_evidence_query(), list(params))
    return [dict(row) for row in store.conn.execute(sql, bound).fetchall()]


class _QueryStoreSession:
    """Runs the real evidence SQL text on SQLite for the capture collector."""

    def __init__(self, store: QueryStoreSqlite, intervals: int) -> None:
        self.store = store
        self.intervals = intervals

    def fetch_all(self, sql, params=None):
        if "sys.database_query_store_options" in sql:
            return [
                {
                    "actual_state_desc": "READ_WRITE",
                    "query_capture_mode_desc": "ALL",
                    "stale_query_threshold_days": 60,
                }
            ]
        if sql == QUERY_STORE_WINDOW_SQL:
            return [
                {
                    "window_start_utc": "2026-09-29T10:00:00Z",
                    "window_end_utc": "2026-11-03T09:00:00Z",
                    "runtime_interval_count": self.intervals,
                }
            ]
        if sql == INDEX_EVIDENCE_QUERY:
            return _run_evidence_query(self.store, list(params))
        raise AssertionError(sql[:80])


def _context() -> CaptureContext:
    return CaptureContext("appdb", "fp", "run-1", "key", "request", "2026-11-03T10:00:00Z", 35)


def _count_parses(monkeypatch) -> list[int]:
    parsed_plan_ids: list[int] = []
    real = index_review.parse_showplan_index_evidence

    def counting(raw_xml, **kwargs):
        parsed_plan_ids.append(kwargs["plan_id"])
        return real(raw_xml, **kwargs)

    monkeypatch.setattr(index_review, "parse_showplan_index_evidence", counting)
    return parsed_plan_ids


def test_evidence_query_returns_one_row_per_plan_and_keeps_plans_without_runtime() -> None:
    # A stored plan with no runtime in the window still proves which index a
    # forced or rarely-run query needs, so the LEFT JOIN must keep it.
    store = _store(plans=2, intervals=3)
    store.conn.execute("DELETE FROM sys.query_store_runtime_stats WHERE plan_id = 2")
    # Aborted runs are a second runtime row in the same interval.
    store.add_runtime(1, 2, 4, 50.0, execution_type=3)

    rows = _run_evidence_query(store, [100, WINDOW_MINUTES])

    assert [row["plan_id"] for row in rows] == [1, 2]
    busy, idle = rows
    assert busy["execution_count"] == 34
    assert busy["total_cpu_us"] == pytest.approx(10 * 5.0 * 3 + 4 * 50.0)
    assert sorted(busy["positive_runtime_interval_ids"].split(",")) == ["1", "2", "3"]
    assert busy["zero_execution_runtime_interval_ids"] is None
    assert busy["query_plan_xml"] == PLAN_XML
    assert idle["execution_count"] == 0
    assert idle["positive_runtime_interval_ids"] is None
    assert idle["last_seen_utc"] is None
    assert idle["query_plan_xml"] == PLAN_XML


def test_twenty_hourly_plans_over_35_days_stay_under_the_cap_and_parse_once(monkeypatch) -> None:
    # 20 plans x 840 hourly intervals was 16,800 rows: over the 10,000 cap, so
    # 8 plans were cut off and every plan XML was parsed up to 840 times.
    intervals = 35 * 24
    session = _QueryStoreSession(_store(plans=20, intervals=intervals), intervals)
    parsed_plan_ids = _count_parses(monkeypatch)

    query_store, references, candidates = IndexReviewService._collect_query_store(session, _context())

    coverage = query_store["coverage"]
    assert coverage["capped"] is False
    assert "query_store_plan_cap_reached" not in coverage["blockers"]
    assert coverage["status"] == "complete"
    assert coverage["eligible"] == coverage["scanned"] == 20
    assert sorted(parsed_plan_ids) == list(range(1, 21))
    assert sorted(reference["plan_id"] for reference in references) == list(range(1, 21))
    for reference in references:
        assert reference["execution_count"] == 10 * intervals
        assert reference["runtime_interval_ids"] == list(range(1, intervals + 1))
    [candidate] = candidates
    assert candidate["plan_ids"] == list(range(1, 21))
    assert candidate["positive_runtime_interval_ids"] == list(range(1, intervals + 1))
    assert candidate["recurring"] is True


def test_the_cap_counts_plans() -> None:
    class Session(_QueryStoreSession):
        def fetch_all(self, sql, params=None):
            if sql == INDEX_EVIDENCE_QUERY:
                assert list(params) == [MAX_CAPTURE_ROWS + 1, WINDOW_MINUTES]
                return [
                    {"query_id": plan_id, "plan_id": plan_id, "query_plan_xml": "", "execution_count": 0}
                    for plan_id in range(1, MAX_CAPTURE_ROWS + 2)
                ]
            return super().fetch_all(sql, params)

    query_store, _references, _candidates = IndexReviewService._collect_query_store(
        Session(QueryStoreSqlite(), 840), _context()
    )

    assert query_store["coverage"]["capped"] is True
    assert query_store["coverage"]["scanned"] == MAX_CAPTURE_ROWS
    assert "query_store_plan_cap_reached" in query_store["coverage"]["blockers"]


def test_interval_ids_are_aggregated_as_varchar_max_and_survive_past_8000_bytes(monkeypatch) -> None:
    # STRING_AGG over plain varchar fails with error 9829 once the result
    # passes 8,000 bytes: about 1,150 seven-digit interval ids.
    aggregates = re.findall(r"STRING_AGG\((.*?),\s*','\)", INDEX_EVIDENCE_QUERY, flags=re.DOTALL)
    assert len(aggregates) == 2
    for argument in aggregates:
        assert "AS varchar(max))" in argument

    # 15-minute Query Store intervals: 2,001 of them fit in the 35-day window.
    intervals = 2_001
    first_id = 1_000_000
    store = _store(plans=1, intervals=intervals, first_interval_id=first_id, interval_minutes=15)
    [row] = _run_evidence_query(store, [10, WINDOW_MINUTES])
    assert len(row["positive_runtime_interval_ids"]) > 8_000

    parsed_plan_ids = _count_parses(monkeypatch)
    _query_store, [reference], [candidate] = IndexReviewService._collect_query_store(
        _QueryStoreSession(store, intervals), _context()
    )

    expected = list(range(first_id, first_id + intervals))
    assert parsed_plan_ids == [1]
    assert reference["runtime_interval_ids"] == expected
    assert candidate["runtime_interval_ids"] == expected
    assert candidate["positive_runtime_interval_ids"] == expected
    assert candidate["request_count"] == intervals


def test_plan_xml_is_read_once_per_plan_after_top_and_is_not_a_group_key() -> None:
    normalized = " ".join(INDEX_EVIDENCE_QUERY.split())
    select_list, rest = normalized.split("SELECT TOP (?)", 1)
    ranked, _joined_after_top = rest.split(") AS ranked", 1)
    plan_text = re.compile(r"\bquery_plan\b")

    assert plan_text.findall(normalized) == ["query_plan"]
    assert "plan_text.query_plan AS query_plan_xml" in select_list
    assert not plan_text.search(ranked)
    assert "INNER JOIN sys.query_store_plan AS plan_text ON plan_text.plan_id = ranked.plan_id" in normalized
    for group_by in re.findall(r"GROUP BY (.*?)\)", normalized):
        assert "query_plan" not in group_by


# --- the stored snapshot does not change with the grain ----------------------

_LAST_SEEN = {1: "2026-11-01T01:00:00", 2: "2026-11-01T02:00:00", 3: "2026-11-01T03:00:00"}


def _plan_interval_rows() -> list[dict]:
    """The grain read before 2.6.4: one row for each plan x runtime interval."""

    def row(plan_id, interval_id, executions, *, forced=False, last_seen=None):
        return {
            "query_id": 40 + plan_id,
            "plan_id": plan_id,
            "query_plan_hash": "0x0102030405060708",
            "is_forced_plan": forced,
            "query_plan_xml": PLAN_XML,
            "runtime_stats_interval_id": interval_id,
            "execution_count": executions,
            "total_cpu_us": executions * 5,
            "last_seen_utc": last_seen,
        }

    return [
        row(7, 1, 4, forced=True, last_seen=_LAST_SEEN[1]),
        row(7, 2, 0, forced=True, last_seen=_LAST_SEEN[2]),
        row(7, 3, 6, forced=True, last_seen=_LAST_SEEN[3]),
        row(8, 2, 9, last_seen=_LAST_SEEN[2]),
        # Stored plan with no runtime in the window.
        row(9, None, 0),
    ]


def _plan_rows() -> list[dict]:
    """The same Query Store content at plan grain."""

    def row(plan_id, positive, zero, executions, *, forced=False, last_seen=None):
        return {
            "query_id": 40 + plan_id,
            "plan_id": plan_id,
            "query_plan_hash": "0x0102030405060708",
            "is_forced_plan": forced,
            "query_plan_xml": PLAN_XML,
            "positive_runtime_interval_ids": positive,
            "zero_execution_runtime_interval_ids": zero,
            "execution_count": executions,
            "total_cpu_us": executions * 5,
            "last_seen_utc": last_seen,
        }

    return [
        row(7, "1,3", "2", 10, forced=True, last_seen=_LAST_SEEN[3]),
        row(8, "2", None, 9, last_seen=_LAST_SEEN[2]),
        row(9, None, None, 0),
    ]


def _stored_capture(evidence_rows: list[dict]):
    class Session(_RealDatabaseSession):
        def fetch_all(self, statement, params=None):
            if statement == INDEX_EVIDENCE_QUERY:
                return [dict(row) for row in evidence_rows]
            return super().fetch_all(statement, params)

    capture = IndexReviewService(None, None)._collect_capture(
        Session(_RealCaptureExecutor()), _context(), readable_replicas="none"
    )
    stored_rows = [_snapshot_params(capture.run, subject) for subject in capture.snapshot.subjects]
    return capture, stored_rows


def _without_parser_fingerprint(capture, *, plan_id: int) -> tuple[list[dict], list[str]]:
    removed: list[str] = []
    subjects = []
    for subject in capture.snapshot.subjects:
        references = []
        for reference in subject["query_store_references"]:
            reference = dict(reference)
            if reference.get("plan_id") == plan_id:
                removed.append(reference.pop("plan_fingerprint"))
            references.append(reference)
        subjects.append({**subject, "query_store_references": references})
    return subjects, removed


def _stored_row(rows: list[list], subject_kind: str) -> list:
    [row] = [row for row in rows if row[3] == subject_kind]
    return row


def test_plan_grain_rows_store_the_same_snapshot_as_plan_interval_rows() -> None:
    # recurring_executed needs two intervals WITH executions, and the stored
    # subjects carry the interval ids. Both must survive the grain change, or
    # create verdicts and stored history silently change meaning.
    old_capture, old_rows = _stored_capture(_plan_interval_rows())
    new_capture, new_rows = _stored_capture(_plan_rows())

    old_subjects, old_parser_fingerprints = _without_parser_fingerprint(old_capture, plan_id=7)
    new_subjects, new_parser_fingerprints = _without_parser_fingerprint(new_capture, plan_id=7)
    assert new_subjects == old_subjects
    # The parser's plan_fingerprint digests the runtime facts it was given. It
    # was the digest of the first plan x interval row; it is now the digest of
    # the plan totals. No gate reads it. Plans with one interval keep theirs.
    assert len(old_parser_fingerprints) == len(new_parser_fingerprints) == 1
    assert _stored_row(new_rows, "missing_index") == _stored_row(old_rows, "missing_index")
    assert _stored_row(new_rows, "existing_index")[1:20] == _stored_row(old_rows, "existing_index")[1:20]
    assert _stored_row(new_rows, "existing_index")[21:] == _stored_row(old_rows, "existing_index")[21:]
    assert new_capture.run.snapshot_set_fingerprint == old_capture.run.snapshot_set_fingerprint
    assert new_capture.run.inventory_fingerprint == old_capture.run.inventory_fingerprint

    missing = next(
        subject for subject in new_capture.snapshot.subjects if subject["subject_kind"] == "missing_index"
    )
    assert missing["runtime_interval_ids"] == [1, 2, 3]
    assert missing["positive_runtime_interval_ids"] == [1, 2, 3]
    assert missing["recurring_executed"] is True
    assert missing["aggregates"]["request_count"] == 5
    assert missing["aggregates"]["query_store_executions"] == 19
    existing = next(
        subject for subject in new_capture.snapshot.subjects if subject["subject_kind"] == "existing_index"
    )
    by_plan = {reference["plan_id"]: reference for reference in existing["query_store_references"]}
    assert by_plan[7]["runtime_interval_ids"] == [1, 2, 3]
    assert by_plan[7]["execution_count"] == 10
    assert by_plan[9]["runtime_interval_ids"] == []

    # Only the row counters differ: they now count plans.
    old_query_store = dict(old_capture.run.query_store)
    new_query_store = dict(new_capture.run.query_store)
    old_coverage = dict(old_query_store.pop("coverage"))
    new_coverage = dict(new_query_store.pop("coverage"))
    assert (old_coverage.pop("eligible"), old_coverage.pop("scanned")) == (5, 5)
    assert (new_coverage.pop("eligible"), new_coverage.pop("scanned")) == (3, 3)
    assert new_coverage == old_coverage
    assert new_query_store == old_query_store


def test_one_interval_with_executions_is_not_recurring_at_plan_grain() -> None:
    # Interval 2 has a runtime row but no executions: it is an observed
    # interval, not a second executed one.
    rows = [{**_plan_rows()[0], "positive_runtime_interval_ids": "1", "zero_execution_runtime_interval_ids": "2"}]

    capture, _stored = _stored_capture(rows)

    missing = next(
        subject for subject in capture.snapshot.subjects if subject["subject_kind"] == "missing_index"
    )
    assert missing["runtime_interval_ids"] == [1, 2]
    assert missing["positive_runtime_interval_ids"] == [1]
    assert missing["recurring_executed"] is False


class _RowsExecutor:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, list]] = []

    async def fetch_all(self, database_name, query, params=None):
        self.calls.append((query, list(params or [])))
        if query == INDEX_EVIDENCE_QUERY:
            return [dict(row) for row in self.rows]
        return []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method", ["get_index_executed_plan_references", "get_missing_index_candidate_requests"]
)
async def test_query_store_service_reads_plan_grain_rows_like_plan_interval_rows(method: str) -> None:
    async def read(rows: list[dict]) -> dict:
        executor = _RowsExecutor(rows)
        result = await getattr(QueryStoreService(executor), method)("appdb", window_minutes=60, limit=10)
        assert executor.calls[0][1] == [11, 60]
        return result

    old = await read(_plan_interval_rows())
    new = await read(_plan_rows())
    # See the snapshot test: the parser digest of the multi-interval plan.
    for result in (old, new):
        for reference in result.get("index_references", []):
            if reference["plan_id"] == 7:
                del reference["plan_fingerprint"]

    assert (old["coverage"].pop("eligible"), old["coverage"].pop("scanned")) == (5, 5)
    assert (new["coverage"].pop("eligible"), new["coverage"].pop("scanned")) == (3, 3)
    assert new == old
    if method == "get_missing_index_candidate_requests":
        [candidate] = new["missing_index_candidates"]
        assert candidate["runtime_interval_ids"] == [1, 2, 3]
        assert candidate["request_count"] == 5
        assert candidate["recurring"] is True


@pytest.mark.asyncio
async def test_plan_with_executions_but_no_interval_is_malformed_at_plan_grain() -> None:
    row = {**_plan_rows()[1], "positive_runtime_interval_ids": None}

    result = await QueryStoreService(_RowsExecutor([row])).get_index_executed_plan_references("appdb")

    assert result["index_references"] == []
    assert "query_store_identity_or_interval_malformed" in result["coverage"]["blockers"]
