"""Compact portfolio-review response (review finding 24).

review_index_portfolio returned about 14 KB per index: the whole review, a
pretty-printed JSON copy of it, and validation SQL for every 'keep' index. At
100 indexes that is over 1 MB, which an agent truncates (losing the drop
candidates) or reads at the cost of its context.
"""

from __future__ import annotations

import json

import pytest

from azure_sql_mcp.index_review import IndexReviewV1
from azure_sql_mcp.index_review import render_index_review_artifacts
from azure_sql_mcp.index_review import review_index_portfolio
from tests.unit.test_index_review import _index
from tests.unit.test_index_review import _snapshot

ACTIONABLE_STATES = {"create_candidate", "consolidate_candidate", "drop_candidate"}


def _subject(number: int, *, key_column: str | None = None, **kwargs) -> dict:
    subject = _index(**kwargs)
    index_id = number + 10
    name = f"IX_Orders_{number:03d}"
    subject.update(
        subject_id=f"index:100:{index_id}",
        index_id=index_id,
        index_name=name,
        subject_fingerprint=f"subject-{name}",
        definition_fingerprint=f"definition-{number}",
    )
    subject["definition"]["reversible_definition"].update(
        index_id=index_id,
        index_name=name,
        key_columns=[{"name": key_column or f"Column{number}", "direction": "ASC"}],
    )
    return subject


def _create_candidate() -> dict:
    return {
        "subject_kind": "missing_index",
        "subject_id": "missing:candidate-1",
        "subject_fingerprint": "candidate-1",
        "candidate_fingerprint": "candidate-1",
        "schema_name": "dbo",
        "table_name": "Orders",
        "key_columns": ["SalespersonId"],
        "include_columns": ["OrderDate"],
        "current_score": 10,
        "runtime_interval_ids": [1, 2],
        "positive_runtime_interval_ids": [1, 2],
        "statement_subtree_cost": 1,
        "execution_count": 2,
        "impact_pct": 10,
        "estimated_size_mb": 1,
        "table_write_ratio": 0.1,
        "query_store_complete": True,
        "covered_by": [],
        "projected_database_storage_percent": 50,
        "coverage": {"query_store": "complete", "malformed": []},
    }


def _portfolio_subjects() -> tuple[dict, ...]:
    """100 subjects: 60 keep, 35 observe, 3 drop, 1 consolidate, 1 create."""

    subjects = [_subject(number, protected=True) for number in range(59)]
    # Number 59 is kept as the canonical copy of a duplicate; 99 duplicates it.
    subjects.append(_subject(59, key_column="Shared"))
    for number in range(60, 95):
        observed = _subject(number)
        observed["coverage"]["query_store"] = "incomplete"
        subjects.append(observed)
    subjects.extend(_subject(number) for number in range(95, 98))
    subjects.append(_subject(99, key_column="Shared"))
    subjects.append(_create_candidate())
    return tuple(subjects)


@pytest.fixture(scope="module")
def review() -> IndexReviewV1:
    subjects = _portfolio_subjects()
    return review_index_portfolio(
        "appdb", [_snapshot(day, subjects=subjects) for day in range(91)]
    )


def _json_bytes(value: object) -> int:
    return len(json.dumps(value, ensure_ascii=False).encode("utf-8"))


def test_portfolio_fixture_has_five_actionable_subjects_of_one_hundred(review) -> None:
    counts = review.observation["state_counts"]

    assert len(review.subjects) == 100
    assert counts["keep"] == 60
    assert counts["observe"] == 35
    assert (counts["drop_candidate"], counts["consolidate_candidate"], counts["create_candidate"]) == (
        3,
        1,
        1,
    )


def test_compact_review_is_bounded_and_keeps_every_actionable_ddl_and_rollback(review) -> None:
    from azure_sql_mcp.index_review import compact_index_review

    full = review.as_dict()
    artifacts = render_index_review_artifacts(review)
    tool_response_before = {**full, "artifacts": artifacts}

    compact = compact_index_review(review)

    assert _json_bytes(compact) < 12_000
    assert _json_bytes(compact) * 50 < _json_bytes(tool_response_before)
    assert "subjects" not in compact
    assert compact["review_id"] == review.review_id
    assert compact["overall_state"] == review.overall_state == "partial"
    assert compact["state_counts"] == review.observation["state_counts"]
    assert compact["subject_count"] == 100
    assert compact["omitted_subjects"] == {"total": 95, "by_state": {"keep": 60, "observe": 35}}
    assert compact["blockers"] == [
        {"code": "query_store_coverage_complete", "subject_count": 35},
    ]

    by_id = {subject["subject_id"]: subject for subject in full["subjects"]}
    actionable = compact["actionable_subjects"]
    assert [item["subject_id"] for item in actionable] == sorted(
        subject_id for subject_id, subject in by_id.items() if subject["state"] in ACTIONABLE_STATES
    )
    assert len(actionable) == 5
    artifact_for_state = {
        "create_candidate": "create-candidates.sql",
        "consolidate_candidate": "consolidation-candidates.sql",
        "drop_candidate": "drop-candidates.sql",
    }
    for item in actionable:
        source = by_id[item["subject_id"]]
        assert item["state"] == source["state"]
        assert item["reason_codes"] == source["reason_codes"]
        # The DDL and its rollback are the same inert text the artifact files
        # carry: commented, and never dropped by the compaction.
        assert item["ddl"] and item["ddl"] in artifacts[artifact_for_state[item["state"]]]
        assert item["rollback"] and item["rollback"] in artifacts["rollback.sql"]
        for text in (item["ddl"], item["rollback"]):
            assert all(line.startswith("--") for line in text.splitlines())
    consolidate = next(item for item in actionable if item["state"] == "consolidate_candidate")
    assert consolidate["survivor_reference"] == by_id[consolidate["subject_id"]]["survivor_reference"]
    assert "IX_Orders_059" in consolidate["survivor_reference"]
    create = next(item for item in actionable if item["state"] == "create_candidate")
    assert "CREATE NONCLUSTERED INDEX" in create["ddl"]
    assert "DROP INDEX" in create["rollback"]


def test_compact_review_accepts_a_stored_review_mapping(review) -> None:
    from azure_sql_mcp.index_review import compact_index_review

    assert compact_index_review(review.as_dict()) == compact_index_review(review)


def test_artifacts_can_leave_out_the_json_copy_and_non_actionable_subjects(review) -> None:
    everything = render_index_review_artifacts(review)

    trimmed = render_index_review_artifacts(review, include_json=False, actionable_only=True)

    assert set(everything) - set(trimmed) == {"index-review.json"}
    assert everything["validation.sql"].count("-- Validation for ") == 100
    assert trimmed["validation.sql"].count("-- Validation for ") == 5
    for state in ("keep", "observe"):
        assert f"-- Validation for {state} subject" not in trimmed["validation.sql"]
    assert trimmed["index-review.md"].count("| `index:") + trimmed["index-review.md"].count(
        "| `missing:"
    ) == 5
    assert "Not listed: 95 subjects (keep 60, observe 35)." in trimmed["index-review.md"]
    for name in ("create-candidates.sql", "consolidation-candidates.sql", "drop-candidates.sql", "rollback.sql"):
        assert trimmed[name] == everything[name]
    assert _json_bytes(trimmed) * 20 < _json_bytes(everything)


def test_json_copy_stays_available_for_file_export(review) -> None:
    artifacts = render_index_review_artifacts(review)

    assert json.loads(artifacts["index-review.json"]) == json.loads(json.dumps(review.as_dict()))
    assert render_index_review_artifacts(review, include_json=False, actionable_only=False)[
        "validation.sql"
    ] == artifacts["validation.sql"]


def test_subject_detail_returns_the_full_gate_for_one_subject(review) -> None:
    from azure_sql_mcp.index_review import index_review_subjects

    observed = next(subject for subject in review.subjects if subject["state"] == "observe")

    detail = index_review_subjects(review, subject_ids=[observed["subject_id"], "index:1:1"])

    assert detail["review_id"] == review.review_id
    assert detail["subject_count"] == 100
    assert detail["matched_count"] == 1
    assert detail["unknown_subject_ids"] == ["index:1:1"]
    [subject] = detail["subjects"]
    assert subject == dict(observed)
    assert subject["removal_gate"]["blockers"] == ["query_store_coverage_complete"]
    assert subject["definition"]["reversible_definition"]["index_name"] == observed["index_name"]
    assert subject["protections"]["coverage"] == "complete"


def test_subject_detail_pages_through_every_subject_once(review) -> None:
    from azure_sql_mcp.index_review import index_review_subjects

    seen: list[str] = []
    offset: int | None = 0
    pages = 0
    while offset is not None:
        page = index_review_subjects(review, offset=offset, limit=30)
        assert page["matched_count"] == 100
        assert len(page["subjects"]) <= 30
        seen.extend(subject["subject_id"] for subject in page["subjects"])
        offset = page["next_offset"]
        pages += 1

    assert pages == 4
    assert seen == [subject["subject_id"] for subject in review.subjects]


@pytest.mark.parametrize(("offset", "limit"), [(-1, 10), (0, 0), (0, 101)])
def test_subject_detail_rejects_an_unbounded_page(review, offset: int, limit: int) -> None:
    from azure_sql_mcp.index_review import index_review_subjects

    with pytest.raises(ValueError):
        index_review_subjects(review, offset=offset, limit=limit)
