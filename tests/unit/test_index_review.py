from __future__ import annotations

import json
import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from azure_sql_mcp.config import LEARNING_TOOL_NAMES
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import TOOL_GROUPS
from azure_sql_mcp.config import TransportMode
from azure_sql_mcp.config import load_server_config
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.index_review import CONTRACT_CHECKS
from azure_sql_mcp.index_review import CONTRACT_DEFAULTS
from azure_sql_mcp.index_review import CONTRACT_SCHEMA_FINGERPRINT
from azure_sql_mcp.index_review import CaptureContext
from azure_sql_mcp.index_review import RUN_CONTRACT_COLUMNS
from azure_sql_mcp.index_review import SNAPSHOT_CONTRACT_COLUMNS
from azure_sql_mcp.index_review import IndexReviewIntegrityError
from azure_sql_mcp.index_review import IndexReviewRunV1
from azure_sql_mcp.index_review import IndexReviewSchemaError
from azure_sql_mcp.index_review import IndexReviewService
from azure_sql_mcp.index_review import IndexReviewSnapshotV1
from azure_sql_mcp.index_review import MAX_CAPTURE_ROWS
from azure_sql_mcp.index_review import _column_names
from azure_sql_mcp.index_review import _redacted_index
from azure_sql_mcp.index_review import daily_idempotency_key
from azure_sql_mcp.index_review import idempotency_key_hash
from azure_sql_mcp.index_review import parse_review_id
from azure_sql_mcp.index_review import render_index_review_artifacts
from azure_sql_mcp.index_review import review_index_portfolio
from azure_sql_mcp.index_review import validate_contract_probe


def _reversible(
    *,
    name: str = "IX_Orders_CustomerId",
    index_type: str = "NONCLUSTERED",
    unique: bool = False,
    blockers: list[str] | None = None,
) -> dict[str, object]:
    return {
        "version": 1,
        "object_id": 100,
        "parent_object_type": "USER_TABLE",
        "parent_object_type_code": "U",
        "schema": "dbo",
        "table": "Orders",
        "index_id": 2,
        "index_name": name,
        "index_type": index_type,
        "index_type_code": 2 if index_type == "NONCLUSTERED" else 5,
        "is_primary_key": False,
        "is_unique_constraint": False,
        "constraint_name": None,
        "constraint_type": None,
        "is_disabled": False,
        "is_hypothetical": False,
        "is_auto_created": False,
        "key_columns": [{"name": "CustomerId", "direction": "ASC"}],
        "include_columns": [],
        "filter": {"has_filter": False, "definition": None},
        "is_unique": unique,
        "is_padded": False,
        "fill_factor": 90,
        "ignore_dup_key": False,
        "statistics_no_recompute": False,
        "statistics_incremental": False,
        "allow_row_locks": True,
        "allow_page_locks": True,
        "optimize_for_sequential_key": False,
        "suppress_dup_key_messages": False,
        "data_space": {
            "name": "PRIMARY",
            "type": "ROWS",
            "partition_scheme": None,
            "partition_function": None,
            "partition_columns": [],
        },
        "partition_compression": [],
        "xml_compression": [],
    }


def _index(
    *,
    name: str = "IX_Orders_CustomerId",
    fingerprint: str = "definition-1",
    index_type: str = "NONCLUSTERED",
    protected: bool = False,
    referenced: bool = False,
    reads: int = 0,
    updates: int = 10,
) -> dict[str, object]:
    index_id = 2 if name == "IX_Orders_CustomerId" else {"IX_A": 3, "IX_B": 4}.get(name, 5)
    protections = {
        "coverage": "complete",
        "primary_key": protected,
        "unique_constraint": False,
        "indexed_view": False,
        "clustered": index_type == "CLUSTERED",
        "disabled": False,
        "hypothetical": False,
        "auto_created": False,
        "safe_to_remove": True,
        "automatic_tuning": False,
        "specialist_type": None,
        "has_index_extended_properties": False,
        "extended_properties": False,
        "hinted_or_forced_plan": False,
        "partition_switch_dependency": False,
        "referenced_foreign_key_key_index_ids": [1] if referenced else [],
        "child_foreign_key_support": [],
    }
    subject = {
        "subject_kind": "existing_index",
        "subject_id": f"index:100:{index_id}",
        "schema_name": "dbo",
        "table_name": "Orders",
        "object_id": 100,
        "index_id": index_id,
        "index_name": name,
        "definition_fingerprint": fingerprint,
        "definition": {
            "reversible_definition": {
                **_reversible(
                name=name,
                index_type=index_type,
                ),
                "index_id": index_id,
            },
            "reversibility_blockers": [],
        },
        "counters": {
            "user_seeks": reads,
            "user_scans": 0,
            "user_lookups": 0,
            "user_updates": updates,
        },
        "counter_epoch_fingerprint": "epoch-1",
        "query_store_references": [],
        "protections": protections,
        "coverage": {
            "query_store": "complete",
            "hint": "complete",
            "dependency": "complete",
            "protection": "complete",
            "usage": "covered",
            "malformed": [],
        },
        "size_pages": 20,
        "size_bytes": 20 * 8192,
        "write_burden": updates,
    }
    subject["subject_fingerprint"] = f"subject-{name}"
    return subject


def _snapshot(
    ordinal: int,
    *,
    index: dict[str, object] | None = None,
    subjects: tuple[dict[str, object], ...] | None = None,
    epoch: str = "epoch-1",
    engine: str = "a" * 64,
    query_store: dict[str, object] | None = None,
) -> IndexReviewSnapshotV1:
    observed = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=ordinal)
    selected = subjects if subjects is not None else ((index or _index()),)
    coverage = {
        "query_store": {"status": "complete"},
        "hints": "complete",
        "dependency": "complete",
        "protection": "complete",
    }
    return IndexReviewSnapshotV1(
        run_id=f"run-{ordinal}",
        snapshot_id=f"snapshot-{ordinal}",
        database_name="appdb",
        database_fingerprint="db-1",
        observed_at_utc=observed.isoformat().replace("+00:00", "Z"),
        counter_epoch_fingerprint=epoch,
        engine_fingerprint=engine,
        engine_identity="azure-sql-database",
        engine_start_time_utc="2025-12-01T00:00:00Z",
        database_incarnation_fingerprint="b" * 64,
        database_incarnation_identity="physical-db-1",
        subjects=selected,
        query_store=query_store or {"enabled": True, "complete": True},
        coverage=coverage,
    )


def _captured_index() -> SimpleNamespace:
    protection = dict(_index()["protections"])
    return SimpleNamespace(
        object_id=100,
        index_id=2,
        schema="dbo",
        table="Orders",
        name="IX_Orders_CustomerId",
        protection_evidence=protection,
        usage={
            "user_seeks": 0,
            "user_scans": 0,
            "user_lookups": 0,
            "user_updates": 10,
        },
        partition_page_counts=((1, 20),),
        reversible_definition=_reversible(),
        reversible_definition_fingerprint_v1="c" * 64,
        definition_fingerprint="c" * 64,
        reversibility_blockers=(),
        usage_context={
            "coverage": "covered",
            "counter_epoch_fingerprint": "epoch-1",
        },
        provenance={"collected_at_utc": "2026-01-01T00:00:00Z"},
    )


def _policy(*, allow_write: bool = False, extension: int = 0) -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_index_history_write": allow_write,
                    "business_cycle_extension_days": extension,
                }
            },
        }
    )


def _probe_rows() -> list[list[dict[str, object]]]:
    columns = []
    for table_name, specs in (
        ("IndexReviewRun", RUN_CONTRACT_COLUMNS),
        ("IndexReviewSnapshot", SNAPSHOT_CONTRACT_COLUMNS),
    ):
        for name, data_type, max_length, nullable in specs:
            columns.append(
                {
                    "TableName": table_name,
                    "ColumnName": name,
                "DataType": data_type,
                "MaxLength": max_length,
                "IsNullable": nullable,
                "PrecisionValue": {"datetime2": 27, "int": 10, "bigint": 19}.get(data_type, 0),
                "ScaleValue": 7 if data_type == "datetime2" else 0,
            }
            )
    indexes = []
    for table, name, columns_for_index, primary in (
        ("IndexReviewRun", "PK_IndexReviewRun", ("RunId",), True),
        (
            "IndexReviewRun",
            "UQ_IndexReviewRun_Database_Idempotency",
            ("DatabaseFingerprint", "IdempotencyKeyHash"),
            False,
        ),
        ("IndexReviewSnapshot", "PK_IndexReviewSnapshot", ("SnapshotId",), True),
        (
            "IndexReviewSnapshot",
            "UQ_IndexReviewSnapshot_Run_Subject",
            ("RunId", "SubjectId"),
            False,
        ),
    ):
        for ordinal, column in enumerate(columns_for_index, start=1):
            indexes.append(
                {
                    "TableName": table,
                    "IndexName": name,
                    "IsPrimaryKey": primary,
                    "IsUnique": True,
                    "KeyOrdinal": ordinal,
                    "ColumnName": column,
                }
            )
    foreign_keys = [
        {
            "ChildTableName": "IndexReviewSnapshot",
            "ParentTableName": "IndexReviewRun",
            "ColumnOrdinal": 1,
            "ChildColumnName": "RunId",
            "ParentColumnName": "RunId",
        }
    ]
    permissions = [
        {
            "TableName": table,
            "SelectState": 1,
            "InsertState": 1,
            "UpdateState": 0,
            "DeleteState": 0,
            "AlterState": 0,
            "ControlState": 0,
        }
        for table in ("IndexReviewRun", "IndexReviewSnapshot")
    ]
    constraints = [
        {"TableName": table, "ConstraintName": name, "ConstraintType": "DEFAULT", "Definition": definition}
        for table, name, definition in CONTRACT_DEFAULTS
    ] + [
        {"TableName": table, "ConstraintName": name, "ConstraintType": "CHECK", "Definition": definition}
        for table, name, definition in CONTRACT_CHECKS
    ]
    return [columns, indexes, foreign_keys, constraints, permissions]


def test_contract_probe_requires_exact_schema_and_minimum_permissions() -> None:
    result = validate_contract_probe(_probe_rows())
    assert result == result.__class__(CONTRACT_SCHEMA_FINGERPRINT, True, True, True)

    with pytest.raises(Exception):
        validate_contract_probe(_probe_rows()[:-1])

    bad = _probe_rows()
    bad[0][0]["DataType"] = "int"
    with pytest.raises(Exception):
        validate_contract_probe(bad)

    denied = _probe_rows()
    denied[4][0]["SelectState"] = None
    denied_result = validate_contract_probe(denied)
    assert denied_result.allow_read is False
    assert denied_result.allow_write is False


@pytest.mark.parametrize(
    "permission",
    [
        "UpdateState",
        "DeleteState",
        "AlterState",
        "ControlState",
        "ExecuteState",
        "ReferencesState",
        "ViewDefinitionState",
        "TakeOwnershipState",
    ],
)
def test_contract_probe_reports_broader_permissions_without_rejecting_them(
    permission: str,
) -> None:
    probe = _probe_rows()
    probe[4][0][permission] = 1

    result = validate_contract_probe(probe)

    assert result.allow_read is True
    assert result.allow_write is True
    assert result.dangerous_permissions_absent is False


@pytest.mark.parametrize(
    ("remaining_table", "expected_message"),
    [
        (
            None,
            "Index history tables are missing: dbatools.IndexReviewRun, "
            "dbatools.IndexReviewSnapshot.",
        ),
        (
            "IndexReviewRun",
            "Index history table is missing: dbatools.IndexReviewSnapshot.",
        ),
    ],
)
def test_contract_probe_identifies_missing_history_tables(
    remaining_table: str | None,
    expected_message: str,
) -> None:
    probe = _probe_rows()
    probe[0] = [
        row
        for row in probe[0]
        if remaining_table is not None and row["TableName"] == remaining_table
    ]

    with pytest.raises(
        IndexReviewSchemaError, match=rf"^{re.escape(expected_message)}$"
    ):
        validate_contract_probe(probe)


@pytest.mark.parametrize(
    ("constraint_name", "definition"),
    [
        ("CK_IndexReviewRun_ContractVersion", "ContractVersion = '2.3.1'"),
        ("DF_IndexReviewRun_CreatedAtUtc", "GETUTCDATE()"),
    ],
)
def test_contract_probe_rejects_definition_drift(
    constraint_name: str, definition: str
) -> None:
    probe = _probe_rows()
    row = next(row for row in probe[3] if row["ConstraintName"] == constraint_name)
    row["Definition"] = definition
    with pytest.raises(Exception):
        validate_contract_probe(probe)


def test_contract_probe_ignores_only_harmless_sql_formatting() -> None:
    probe = _probe_rows()
    default = next(
        row for row in probe[3] if row["ConstraintName"] == "DF_IndexReviewRun_CreatedAtUtc"
    )
    default["Definition"] = " ( SYSUTCDATETIME ( ) ) "
    check = next(
        row for row in probe[3] if row["ConstraintName"] == "CK_IndexReviewRun_ContractVersion"
    )
    check["Definition"] = "( [ContractVersion] = ( '2.3.0' ) )"
    assert validate_contract_probe(probe).schema_fingerprint == CONTRACT_SCHEMA_FINGERPRINT


def test_contract_probe_preserves_logical_grouping() -> None:
    probe = _probe_rows()
    check = next(
        row
        for row in probe[3]
        if row["ConstraintName"] == "CK_IndexReviewRun_EngineEpochIdentity"
    )
    check["Definition"] = (
        "EngineFingerprint IS NULL AND EngineIdentity IS NULL AND "
        "(EngineStartTimeUtc IS NULL OR EngineFingerprint IS NOT NULL) AND "
        "EngineIdentity IS NOT NULL AND EngineStartTimeUtc IS NOT NULL"
    )
    with pytest.raises(Exception):
        validate_contract_probe(probe)


def test_redaction_rejects_query_material_but_allows_exact_reversible_filter() -> None:
    subject = _index()
    subject["definition"]["reversible_definition"]["filter"] = {
        "has_filter": True,
        "definition": "[Status] = N'active'",
    }
    snapshot = _snapshot(1, index=subject)
    assert "Status" in snapshot.as_dict()["subjects"][0]["definition"]["reversible_definition"]["filter"]["definition"]

    subject["query_text"] = "SELECT secret"
    with pytest.raises(IndexReviewIntegrityError):
        _snapshot(2, index=subject)

    malformed_filter = _index()
    malformed_filter["definition"] = {"filter": {"definition": "[Status] = 1"}}
    with pytest.raises(IndexReviewIntegrityError):
        _snapshot(3, index=malformed_filter)

    for key in ("query_sql_text", "statement_text", "module_definition", "query_plan"):
        raw_subject = _index()
        raw_subject[key] = "secret material"
        with pytest.raises(IndexReviewIntegrityError):
            _snapshot(4, index=raw_subject)

    nested_payloads = {
        "definition": {"query_text": "SELECT secret"},
        "protections": {"parameters": "@secret"},
        "aggregates": {"query_plan_xml": "<ShowPlanXML/>"},
        "coverage": {"plan_xml": "<ShowPlanXML/>"},
    }
    for field, nested in nested_payloads.items():
        raw_subject = _index()
        if field == "definition":
            raw_subject[field]["reversible_definition"].update(nested)
        else:
            raw_subject.setdefault(field, {}).update(nested)
        with pytest.raises(IndexReviewIntegrityError):
            _snapshot(5, index=raw_subject)

    raw_reference = _index()
    raw_reference["query_store_references"] = [{"parameters": "@secret"}]
    with pytest.raises(IndexReviewIntegrityError):
        _snapshot(6, index=raw_reference)

    with pytest.raises(IndexReviewIntegrityError):
        _snapshot(7, query_store={"coverage": {"query_plan_xml": "<ShowPlanXML/>"}})

    bad_code = _index()
    bad_code["definition"]["reversibility_blockers"] = ["DROP INDEX IX_Orders_CustomerId"]
    with pytest.raises(IndexReviewIntegrityError):
        _snapshot(8, index=bad_code)


def test_daily_key_and_hash_are_deterministic_and_raw_key_is_not_the_hash() -> None:
    moment = datetime(2026, 8, 28, 23, 30, tzinfo=timezone.utc)
    fingerprint = "A" * 64
    key = daily_idempotency_key(fingerprint, moment)
    assert key == f"index-review:{'a' * 64}:2026-08-28"
    assert idempotency_key_hash(fingerprint, key) != key
    assert idempotency_key_hash(fingerprint, key) == idempotency_key_hash("a" * 64, key)


@pytest.mark.parametrize("count", [90, 91])
def test_drop_gate_requires_90_distinct_observation_days(count: int) -> None:
    snapshots = [_snapshot(day) for day in range(count)]
    review = review_index_portfolio("appdb", snapshots)
    state = review.subjects[0]["state"]
    assert state == ("drop_candidate" if count == 91 else "observe")


def test_drop_gate_requires_complete_engine_and_database_identity() -> None:
    snapshots = [
        replace(
            _snapshot(day),
            engine_fingerprint=None,
            engine_identity=None,
            engine_start_time_utc=None,
            database_incarnation_fingerprint=None,
            database_incarnation_identity=None,
        )
        for day in range(91)
    ]
    review = review_index_portfolio("appdb", snapshots)
    subject = review.subjects[0]
    removal_gate = subject["removal_gate"]

    assert subject["state"] == "observe"
    assert review.overall_state == "inconclusive"
    assert removal_gate["gates"]["stable_engine_and_database"] is False
    assert "stable_engine_and_database" in removal_gate["blockers"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("specialist_type", "XML"),
        ("has_index_extended_properties", True),
        ("extended_properties", True),
        ("automatic_tuning", True),
        ("safe_to_remove", False),
    ],
)
def test_specialist_or_uncertain_metadata_never_reaches_drop_candidate(
    field: str,
    value: object,
) -> None:
    index = _index()
    protections = index["protections"]
    assert isinstance(protections, dict)
    protections[field] = value

    review = review_index_portfolio(
        "appdb",
        [_snapshot(day, index=index) for day in range(91)],
    )
    subject = review.subjects[0]
    removal_gate = subject["removal_gate"]

    assert subject["state"] == "observe"
    assert removal_gate["gates"]["not_protected"] is False
    assert "not_protected" in removal_gate["blockers"]


@pytest.mark.parametrize(
    ("protection_field", "definition_field"),
    [("disabled", "is_disabled"), ("hypothetical", "is_hypothetical")],
)
def test_disabled_and_hypothetical_indexes_require_specialist_observation(
    protection_field: str,
    definition_field: str,
) -> None:
    index = _index()
    index["protections"][protection_field] = True
    index["definition"]["reversible_definition"][definition_field] = True

    review = review_index_portfolio(
        "appdb",
        [_snapshot(day, index=index) for day in range(91)],
    )

    assert review.subjects[0]["state"] == "observe"
    assert review.subjects[0]["removal_gate"]["gates"]["not_protected"] is False


def test_business_cycle_extension_and_gap_are_fail_closed() -> None:
    snapshots = [_snapshot(day) for day in range(99)]
    extended = review_index_portfolio(
        "appdb",
        snapshots,
        business_cycle_extension_days=10,
    )
    assert extended.subjects[0]["state"] == "observe"

    with_gap = snapshots[:45] + [_snapshot(48 + day) for day in range(55)]
    assert review_index_portfolio("appdb", with_gap).subjects[0]["state"] == "observe"


def test_epoch_counter_definition_protection_and_special_type_gates() -> None:
    reset = [_snapshot(day, epoch="epoch-2" if day == 45 else "epoch-1") for day in range(90)]
    assert review_index_portfolio("appdb", reset).subjects[0]["state"] == "observe"

    protected = [_snapshot(day, index=_index(protected=True)) for day in range(90)]
    assert review_index_portfolio("appdb", protected).subjects[0]["state"] == "keep"

    special = [_snapshot(day, index=_index(index_type="CLUSTERED COLUMNSTORE")) for day in range(90)]
    assert review_index_portfolio("appdb", special).subjects[0]["state"] == "observe"

    unsupported_reverse = _index()
    unsupported_reverse["definition"]["reversibility_blockers"] = [
        "suppress_dup_key_messages_unsupported"
    ]
    assert review_index_portfolio(
        "appdb",
        [_snapshot(day, index=unsupported_reverse) for day in range(90)],
    ).subjects[0]["state"] == "observe"

    filtered = _index()
    filtered["definition"]["reversible_definition"]["filter"] = {
        "has_filter": True,
        "definition": "[Status] = 1",
    }
    assert review_index_portfolio(
        "appdb",
        [_snapshot(day, index=filtered) for day in range(90)],
    ).subjects[0]["state"] == "observe"

    partitioned = _index()
    partitioned["definition"]["reversible_definition"]["data_space"] = {
        "name": "ps_Orders",
        "type": "PARTITION_SCHEME",
        "partition_scheme": "ps_Orders",
        "partition_function": "pf_Orders",
        "partition_columns": ["OrderDate"],
    }
    assert review_index_portfolio(
        "appdb",
        [_snapshot(day, index=partitioned) for day in range(90)],
    ).subjects[0]["state"] == "observe"


def test_consolidation_requires_strict_coverage_and_independent_drop_gate() -> None:
    first = _index(name="IX_A")
    second = _index(name="IX_B")
    second["definition"]["reversible_definition"]["include_columns"] = ["CreatedAt"]
    snapshots = [_snapshot(day, subjects=(first, second)) for day in range(91)]
    subjects = {
        item["index_name"]: item
        for item in review_index_portfolio("appdb", snapshots).subjects
    }
    states = {name: item["state"] for name, item in subjects.items()}
    assert states == {"IX_A": "consolidate_candidate", "IX_B": "keep"}
    assert subjects["IX_A"]["overlap_relation"] == "strict_coverage"
    assert subjects["IX_A"]["reason_codes"] == ["strict_coverage_overlap"]

    blocked = [
        _snapshot(
            day,
            subjects=(first, _index(name="IX_B", reads=1 if day == 89 else 0)),
        )
        for day in range(91)
    ]
    blocked_states = {
        item["index_name"]: item["state"]
        for item in review_index_portfolio("appdb", blocked).subjects
    }
    assert blocked_states["IX_B"] == "observe"


def test_exact_duplicate_consolidation_emits_explicit_relation_and_reason() -> None:
    first = _index(name="IX_A")
    second = _index(name="IX_B")

    review = review_index_portfolio(
        "appdb",
        [_snapshot(day, subjects=(first, second)) for day in range(91)],
    )
    subjects = {item["index_name"]: item for item in review.subjects}

    assert subjects["IX_A"]["state"] == "keep"
    assert subjects["IX_B"]["state"] == "consolidate_candidate"
    assert subjects["IX_B"]["overlap_relation"] == "exact_duplicate"
    assert subjects["IX_B"]["reason_codes"] == ["exact_duplicate_definition"]


def test_create_requires_recurring_executed_query_store_and_storage_headroom() -> None:
    candidate = {
        "subject_kind": "missing_index",
        "subject_id": "missing:candidate-1",
        "subject_fingerprint": "candidate-1",
        "schema_name": "dbo",
        "table_name": "Orders",
        "key_columns": ["CustomerId"],
        "include_columns": [],
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
        "projected_database_storage_percent": 89.9,
        "coverage": {"query_store": "complete", "malformed": []},
    }
    snapshot = _snapshot(1, subjects=(candidate,))
    result = review_index_portfolio("appdb", [snapshot])
    assert result.subjects[0]["state"] == "create_candidate"

    candidate["projected_database_storage_percent"] = 90
    assert review_index_portfolio("appdb", [_snapshot(2, subjects=(candidate,))]).subjects[0]["state"] == "observe"


def test_query_store_rows_merge_references_and_aggregate_candidates_by_plan(
    monkeypatch,
) -> None:
    evidence_rows = [
        {
            "query_id": 1,
            "plan_id": 11,
            "query_plan_xml": "seek",
            "execution_count": 3,
            "runtime_stats_interval_id": 1,
            "statement_subtree_cost": 2.5,
            "estimated_index_size_mb": 1.0,
            "table_write_ratio": 0.1,
            "last_seen_utc": "2026-04-01T00:00:00Z",
        },
        {
            "query_id": 1,
            "plan_id": 11,
            "query_plan_xml": "scan",
            "execution_count": 0,
            "runtime_stats_interval_id": 2,
            "statement_subtree_cost": 2.5,
            "estimated_index_size_mb": 1.0,
            "table_write_ratio": 0.1,
            "last_seen_utc": "2026-04-02T00:00:00Z",
            "is_forced_plan": True,
        },
        {
            "query_id": 1,
            "plan_id": 12,
            "query_plan_xml": "plan-12",
            "execution_count": 5,
            "runtime_stats_interval_id": 2,
            "statement_subtree_cost": 7.5,
            "estimated_index_size_mb": 2.0,
            "table_write_ratio": 0.2,
            "last_seen_utc": "2026-04-03T00:00:00Z",
        },
    ]

    class Session:
        def fetch_all(self, sql, params=None):
            if "sys.database_query_store_options" in sql:
                return [
                    {
                        "actual_state_desc": "READ_WRITE",
                        "query_capture_mode_desc": "ALL",
                        "stale_query_threshold_days": 90,
                    }
                ]
            if (
                "sys.query_store_runtime_stats_interval" in sql
                and params is not None
                and len(params) == 1
            ):
                return [
                    {
                        "window_start_utc": datetime(
                            2026, 1, 1, tzinfo=timezone.utc
                        ),
                        "window_end_utc": datetime(
                            2026, 4, 3, tzinfo=timezone.utc
                        ),
                        "runtime_interval_count": 2,
                    }
                ]
            return evidence_rows

    def parse_plan(plan_xml, **kwargs):
        plan_id = kwargs["plan_id"]
        execution_count = kwargs["execution_count"]
        interval_ids = kwargs["runtime_interval_ids"]
        operator_kind = "Index Seek" if plan_xml == "seek" else "Index Scan"
        return {
            "coverage": {"malformed": 0, "blockers": []},
            "index_references": (
                [
                    {
                        "query_id": 1,
                        "plan_id": 11,
                        "database_name": "appdb",
                        "schema_name": "dbo",
                        "object_name": "Orders",
                        "index_name": "IX_Orders_CustomerId",
                        "execution_count": execution_count,
                        "runtime_interval_ids": interval_ids,
                        "operator_kind": operator_kind,
                        "operator_kinds": [operator_kind],
                        "last_seen": kwargs["last_seen"],
                        "is_forced_plan": kwargs["is_forced_plan"],
                    }
                ]
                if plan_id == 11
                else []
            ),
            "missing_index_candidates": [
                {
                    "candidate_signature": "candidate-1",
                    "database_name": "appdb",
                    "schema_name": "dbo",
                    "object_name": "Orders",
                    "query_id": 1,
                    "plan_id": plan_id,
                    "runtime_interval_ids": interval_ids,
                    "execution_count": execution_count,
                    "impact_pct": 40.0,
                    "equality_columns": ["CustomerId"],
                    "inequality_columns": [],
                    "include_columns": ["CreatedAt"],
                    "last_seen": kwargs["last_seen"],
                    "is_forced_plan": kwargs["is_forced_plan"],
                }
            ],
        }

    monkeypatch.setattr(
        "azure_sql_mcp.index_review.parse_showplan_index_evidence",
        parse_plan,
    )
    context = CaptureContext(
        database_name="appdb",
        database_fingerprint="db-1",
        run_id="run-1",
        idempotency_key_hash="key-1",
        request_fingerprint="request-1",
        observed_at_utc="2026-04-03T00:00:00Z",
        minimum_observation_days=90,
    )

    query_store, references, candidates = IndexReviewService._collect_query_store(
        Session(),
        context,
    )

    assert references == [
        {
            "query_id": 1,
            "plan_id": 11,
            "database_name": "appdb",
            "schema_name": "dbo",
            "object_name": "Orders",
            "index_name": "IX_Orders_CustomerId",
            "execution_count": 3,
            "runtime_interval_ids": [1, 2],
            "operator_kind": "Multiple",
            "operator_kinds": ["Index Scan", "Index Seek"],
            "last_seen": "2026-04-02T00:00:00Z",
            "is_forced_plan": True,
        }
    ]
    candidate = candidates[0]
    assert candidate["execution_count"] == 8
    assert candidate["statement_subtree_cost"] == 10
    assert candidate["runtime_interval_ids"] == [1, 2]
    assert candidate["positive_runtime_interval_ids"] == [1, 2]
    assert candidate["recurring"] is True
    assert candidate["current_score"] is None
    assert candidate["scoring_blockers"] == [
        "estimated_size_mb_conflicting",
        "write_ratio_conflicting",
    ]
    assert query_store["runtime_window"]["window_start_utc"] == "2026-01-01T00:00:00Z"
    assert query_store["runtime_window"]["window_end_utc"] == "2026-04-03T00:00:00Z"
    json.dumps(query_store)
    assert query_store["complete"] is False


def test_candidate_size_uses_shared_page_math_and_exact_candidate_columns() -> None:
    calls = []

    class Session:
        def fetch_all(self, sql, params=None):
            calls.append((sql, params))
            if len(calls) == 1:
                return [{"row_count": 1000}]
            return [
                {"column_name": "CustomerId", "max_length": 4},
                {"column_name": "CreatedAt", "max_length": 8},
            ]

    size = IndexReviewService._estimate_candidate_size(
        Session(),
        {
            "schema_name": "dbo",
            "table_name": "Orders",
            "equality_columns": ["CustomerId"],
            "inequality_columns": [],
            "include_columns": ["CreatedAt", "CustomerId"],
        },
    )

    assert size == 27034
    assert calls[0][1] == [1, "dbo", "Orders"]
    assert calls[1][1] == [257, "dbo", "Orders", "CustomerId", "CreatedAt"]


def test_candidate_size_and_write_ratio_fail_closed_on_incomplete_inputs() -> None:
    class MissingWidthSession:
        def fetch_all(self, sql, params=None):
            if "sys.dm_db_partition_stats" in sql:
                return [{"row_count": 1000}]
            return [{"column_name": "CustomerId", "max_length": 4}]

    assert (
        IndexReviewService._estimate_candidate_size(
            MissingWidthSession(),
            {
                "schema_name": "dbo",
                "table_name": "Orders",
                "equality_columns": ["CustomerId"],
                "inequality_columns": [],
                "include_columns": ["CreatedAt"],
            },
        )
        is None
    )

    calls = []

    class WriteRatioSession:
        def fetch_all(self, sql, params=None):
            calls.append((sql, params))
            return [{"write_ratio": 0.5}]

    assert (
        IndexReviewService._collect_candidate_write_ratio(
            WriteRatioSession(),
            {"schema_name": "dbo", "table_name": "Orders"},
        )
        == 0.5
    )
    assert calls[0][1] == [1, "dbo", "Orders"]
    assert "THEN 0.5" in calls[0][0]


def test_storage_collection_uses_bigint_before_aggregation_and_handles_large_database() -> None:
    calls: list[str] = []

    class Session:
        def fetch_all(self, sql, params=None):
            calls.append(sql)
            return [
                {
                    "max_size_bytes": 10 * 1024**3,
                    "used_size_bytes": 3 * 1024**3,
                }
            ]

    storage = IndexReviewService._collect_storage(Session())

    assert "SUM(CONVERT(bigint, size)) * CONVERT(bigint, 8192)" in calls[0]
    assert "WHERE type = 0" in calls[0]
    assert storage == {
        "coverage": "complete",
        "max_size_bytes": 10 * 1024**3,
        "used_size_bytes": 3 * 1024**3,
        "used_percent": 30.0,
    }


def test_storage_collection_rejects_nonpositive_limit_and_negative_allocation() -> None:
    class Session:
        def __init__(self, row):
            self.row = row

        def fetch_all(self, sql, params=None):
            return [self.row]

    assert IndexReviewService._collect_storage(
        Session({"max_size_bytes": -1, "used_size_bytes": 1024})
    ) == {
        "coverage": "incomplete",
        "max_size_bytes": None,
        "used_size_bytes": 1024,
        "used_percent": None,
    }
    assert IndexReviewService._collect_storage(
        Session({"max_size_bytes": 1024, "used_size_bytes": -1})
    ) == {
        "coverage": "incomplete",
        "max_size_bytes": 1024,
        "used_size_bytes": None,
        "used_percent": None,
    }


def test_resolved_hint_is_persisted_as_protection_and_prevents_drop_candidate() -> None:
    subject = _redacted_index(
        _captured_index(),
        hint_coverage={"status": "complete"},
        query_store_coverage={"status": "complete"},
        hint_evidence=(
            {
                "resolved_indexes": [
                    {
                        "object_id": 100,
                        "index_id": 2,
                        "index_name": "IX_Orders_CustomerId",
                    }
                ]
            },
        ),
    )

    assert subject["protections"]["hinted_or_forced_plan"] is True
    review = review_index_portfolio(
        "appdb",
        [_snapshot(day, index=subject) for day in range(91)],
    )
    assert review.subjects[0]["state"] == "keep"


def test_hint_row_cap_is_detected_and_removal_fails_closed() -> None:
    rows = [
        {"retained_query_text": "SELECT 1"}
        for _ in range(MAX_CAPTURE_ROWS)
    ] + [
        {"retained_query_text": "SELECT 1 WITH (INDEX(IX_Orders_CustomerId))"}
    ]

    class Session:
        calls = 0

        def fetch_all(self, sql, params=None):
            self.calls += 1
            return rows if self.calls == 1 else []

    coverage, evidence = IndexReviewService._collect_hints(
        Session(),
        [_captured_index()],
        observation_window_minutes=90 * 1440,
    )
    subject = _redacted_index(
        _captured_index(),
        hint_coverage=coverage,
        query_store_coverage={"status": "complete"},
        hint_evidence=evidence,
    )

    assert coverage["status"] == "incomplete"
    assert coverage["sources"]["query_store_text"]["capped"] is True
    assert "query_store_text_cap_reached" in coverage["blockers"]
    assert subject["protections"]["hinted_or_forced_plan"] is None
    review = review_index_portfolio(
        "appdb",
        [_snapshot(day, index=subject) for day in range(91)],
    )
    assert review.subjects[0]["state"] == "observe"


def test_forced_query_store_reference_keeps_index_and_incomplete_coverage_observes() -> None:
    subject = _index()
    subject["query_store_references"] = [{"plan_id": 44, "is_forced_plan": True}]
    assert review_index_portfolio("appdb", [_snapshot(day, index=subject) for day in range(90)]).subjects[0]["state"] == "observe"

    incomplete = _index()
    incomplete["coverage"]["query_store"] = "incomplete"
    assert review_index_portfolio("appdb", [_snapshot(day, index=incomplete) for day in range(90)]).overall_state == "inconclusive"


def test_review_selector_is_parseable_and_artifacts_are_exactly_seven_inert_files() -> None:
    review = review_index_portfolio("appdb", [_snapshot(1)])
    selector = parse_review_id(review.review_id)
    assert selector["minimum_days"] == 90
    assert selector["as_of"]
    artifacts = render_index_review_artifacts(review)
    assert set(artifacts) == {
        "index-review.json",
        "index-review.md",
        "create-candidates.sql",
        "consolidation-candidates.sql",
        "drop-candidates.sql",
        "rollback.sql",
        "validation.sql",
    }
    assert "safe_to_drop" not in artifacts["index-review.json"]
    assert "drop_candidate" in artifacts["index-review.md"] or "observe" in artifacts["index-review.md"]
    assert "DROP INDEX" not in artifacts["drop-candidates.sql"]
    assert all(line.startswith("--") for line in artifacts["drop-candidates.sql"].splitlines())


def test_delimited_candidate_identifiers_are_preserved_and_exactly_quoted() -> None:
    columns = _column_names(
        "[Order Date], [Amount]]Gross], [Last, First], [År]"
    )
    assert columns == ["Order Date", "Amount]Gross", "Last, First", "År"]

    artifacts = render_index_review_artifacts(
        {
            "database_name": "appdb",
            "review_id": "review-1",
            "overall_state": "actionable",
            "subjects": [
                {
                    "subject_id": "missing:quoted",
                    "subject_kind": "missing_index",
                    "subject_fingerprint": "candidate-quoted",
                    "candidate_fingerprint": "candidate-quoted",
                    "schema_name": "sales data",
                    "table_name": "Order] Lines",
                    "key_columns": columns,
                    "include_columns": ["Résumé, Text"],
                    "state": "create_candidate",
                    "reason_codes": [],
                }
            ],
        }
    )

    ddl = artifacts["create-candidates.sql"]
    assert "[sales data].[Order]] Lines]" in ddl
    assert (
        "([Order Date] ASC, [Amount]]Gross] ASC, [Last, First] ASC, [År] ASC)"
        in ddl
    )
    assert "INCLUDE ([Résumé, Text])" in ddl
    assert all(line.startswith("--") for line in ddl.splitlines())


@pytest.mark.asyncio
async def test_get_review_reconstructs_deterministically_after_restart() -> None:
    snapshots = [_snapshot(day) for day in range(90)]
    run_pairs = []
    for snapshot in snapshots:
        run_pairs.append(
            (
                IndexReviewRunV1(
                    snapshot.run_id,
                    "appdb",
                    "db-1",
                    f"key-{snapshot.run_id}",
                    f"request-{snapshot.run_id}",
                    snapshot.observed_at_utc,
                    snapshot.counter_epoch_fingerprint,
                    snapshot.inventory_fingerprint,
                    "qs-1",
                    engine_fingerprint=snapshot.engine_fingerprint,
                    subject_count=len(snapshot.subjects),
                    snapshot_set_fingerprint=snapshot.snapshot_fingerprint,
                ),
                snapshot,
            )
        )

    class Repository:
        async def list_history(self, database_name):
            assert database_name == "appdb"
            return run_pairs

    executor = SimpleNamespace(config=SimpleNamespace(server="server.database.windows.net"))
    service = IndexReviewService(executor, Repository(), database_policy=_policy())
    first = review_index_portfolio("appdb", snapshots)
    restored = await service.get_review("appdb", first.review_id)
    assert restored.review_id == first.review_id
    assert restored.as_dict() == first.as_dict()


def test_index_review_profile_contains_six_base_tools_and_recall_only_learning(server_config_factory) -> None:
    config = server_config_factory(profile=McpProfile.INDEX_REVIEW)
    base = {
        "check_runtime_status",
        "list_databases",
        "check_capabilities",
        "capture_index_review_snapshot",
        "review_index_portfolio",
        "get_index_review",
    }
    enabled = {name for name in base | set(LEARNING_TOOL_NAMES) if config.is_tool_enabled(name)}
    assert enabled == base | {"recall_lessons"}
    assert config.is_tool_enabled("execute_sql") is False


def test_index_review_remote_surface_is_exactly_six_base_tools(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    monkeypatch.setenv("AZURE_SQL_MCP_BEARER_TOKEN", "test-bearer")
    config = load_server_config(
        [
            "--azure-sql-profile", "index-review",
            "--transport", TransportMode.SSE.value,
            "--azure-sql-tool-groups", "all",
        ]
    )
    candidates = set(TOOL_GROUPS) | set(LEARNING_TOOL_NAMES) | {"check_runtime_status"}
    enabled = {name for name in candidates if config.is_tool_enabled(name)}
    assert enabled == {
        "check_runtime_status",
        "list_databases",
        "check_capabilities",
        "capture_index_review_snapshot",
        "review_index_portfolio",
        "get_index_review",
    }
