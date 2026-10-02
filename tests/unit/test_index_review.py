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
from azure_sql_mcp.index_review import MIN_OBSERVATION_DAYS
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


_BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _utc(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _snapshot(
    ordinal: int,
    *,
    index: dict[str, object] | None = None,
    subjects: tuple[dict[str, object], ...] | None = None,
    epoch: str = "epoch-1",
    engine: str = "a" * 64,
    engine_start: str = "2025-12-01T00:00:00Z",
    database_incarnation: str = "physical-db-1",
    query_store: dict[str, object] | None = None,
    query_store_coverage: dict[str, object] | None = None,
) -> IndexReviewSnapshotV1:
    observed = _BASE_TIME + timedelta(days=ordinal)
    selected = subjects if subjects is not None else ((index or _index()),)
    coverage = {
        "query_store": query_store_coverage
        or {"status": "complete", "stale_query_threshold_days": 90},
        "hints": "complete",
        "dependency": "complete",
        "protection": "complete",
    }
    return IndexReviewSnapshotV1(
        run_id=f"run-{ordinal}",
        snapshot_id=f"snapshot-{ordinal}",
        database_name="appdb",
        database_fingerprint="db-1",
        observed_at_utc=_utc(observed),
        counter_epoch_fingerprint=epoch,
        engine_fingerprint=engine,
        engine_identity="azure-sql-database",
        engine_start_time_utc=engine_start,
        database_incarnation_fingerprint=(
            "b" * 64 if database_incarnation == "physical-db-1" else "c" * 64
        ),
        database_incarnation_identity=database_incarnation,
        subjects=selected,
        query_store=query_store
        or {"enabled": True, "complete": True, "stale_query_threshold_days": 90},
        coverage=coverage,
    )


# Azure SQL Database defaults: AUTO capture and a 30-day stale threshold. A
# 2.6.x capture over a 35-day window records both as Query Store blockers.
_AZURE_DEFAULT_QUERY_STORE = {
    "enabled": True,
    "complete": False,
    "state": "READ_WRITE",
    "capture_mode": "AUTO",
    "stale_query_threshold_days": 30,
    "retention_days": 30,
    "window_minutes": 35 * 1440,
}
_AZURE_DEFAULT_QUERY_STORE_COVERAGE = {
    "status": "incomplete",
    "blockers": [
        "query_store_capture_mode_not_all",
        "query_store_stale_threshold_insufficient",
    ],
    "malformed": 0,
    "capped": False,
    "stale_query_threshold_days": 30,
    "retention_days": 30,
    "requested_window_minutes": 35 * 1440,
}


def _history(
    days: int,
    *,
    restarts: dict[int, float] | None = None,
    missing: frozenset[int] = frozenset(),
    seeks=lambda day: 0,
    azure_default_query_store: bool = False,
) -> list[IndexReviewSnapshotV1]:
    """Daily captures; ``restarts`` maps a capture day to the hours before
    that capture when the engine restarted (failover, scale, pause/resume).
    ``seeks`` returns the cumulative user_seeks counter seen by that capture,
    or None when sys.dm_db_index_usage_stats has no row for the index."""

    epoch = 1
    engine_start = datetime(2025, 12, 1, tzinfo=timezone.utc)
    snapshots = []
    for day in range(days):
        observed = _BASE_TIME + timedelta(days=day)
        if restarts and day in restarts:
            epoch += 1
            engine_start = observed - timedelta(hours=restarts[day])
        if day in missing:
            continue
        index = _index(reads=0)
        value = seeks(day)
        if value is None:
            index["counters"] = {key: None for key in index["counters"]}
        else:
            index["counters"]["user_seeks"] = value
        index["counter_epoch_fingerprint"] = f"epoch-{epoch}"
        if azure_default_query_store:
            index["coverage"]["query_store"] = "incomplete"
        snapshots.append(
            _snapshot(
                day,
                index=index,
                epoch=f"epoch-{epoch}",
                engine=f"{epoch:x}" * 64,
                engine_start=_utc(engine_start),
                query_store=(
                    dict(_AZURE_DEFAULT_QUERY_STORE)
                    if azure_default_query_store
                    else None
                ),
                query_store_coverage=(
                    dict(_AZURE_DEFAULT_QUERY_STORE_COVERAGE)
                    if azure_default_query_store
                    else None
                ),
            )
        )
    return snapshots


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


def test_owner_removal_window_is_35_days() -> None:
    # Owner decision 2026-10-02: 35 days always holds one full month-end
    # plus a buffer for close jobs and one missed capture.
    assert MIN_OBSERVATION_DAYS == 35


@pytest.mark.parametrize("count", [35, 36])
def test_drop_gate_needs_a_snapshot_at_or_before_the_window_start(count: int) -> None:
    snapshots = [_snapshot(day) for day in range(count)]
    review = review_index_portfolio("appdb", snapshots)
    subject = review.subjects[0]
    observation = subject["removal_gate"]["observation"]

    assert review.minimum_observation_days == 35
    assert observation["window_start_utc"] == _utc(
        _BASE_TIME + timedelta(days=count - 1 - 35)
    )
    if count == 36:
        assert subject["state"] == "drop_candidate"
        assert observation["anchor_observed_at_utc"] == _utc(_BASE_TIME)
        assert observation["subject_younger_than_window"] is False
    else:
        # Day 0 is one day after the window start: nothing proves the first
        # day of the window, so the index is too young to judge.
        assert subject["state"] == "observe"
        assert "continuous_usable_days" in subject["reason_codes"]
        assert observation["subject_younger_than_window"] is True


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


@pytest.mark.parametrize(("count", "state"), [(95, "observe"), (96, "drop_candidate")])
def test_business_cycle_extension_lengthens_the_trailing_window(
    count: int, state: str
) -> None:
    # A quarter-end database extends the 35-day window per database (+60).
    review = review_index_portfolio(
        "appdb",
        [_snapshot(day) for day in range(count)],
        business_cycle_extension_days=60,
    )

    assert review.minimum_observation_days == 95
    assert review.subjects[0]["state"] == state


def test_reads_before_the_trailing_window_do_not_block_removal() -> None:
    # Seeks on day 10 only; the 35-day window of the day-50 run starts on day
    # 15, so whole-history evidence must not keep this index forever.
    snapshots = _history(51, seeks=lambda day: 3 if day >= 10 else 0)

    subject = review_index_portfolio("appdb", snapshots).subjects[0]

    assert subject["state"] == "drop_candidate"
    assert subject["removal_gate"]["observation"]["window_read_count"] == 0


def test_counter_epoch_change_without_engine_start_change_observes_only_inside_window() -> None:
    def history(reset_day: int) -> list[IndexReviewSnapshotV1]:
        snapshots = []
        for day in range(90):
            index = _index()
            epoch = "epoch-2" if day >= reset_day else "epoch-1"
            index["counter_epoch_fingerprint"] = epoch
            snapshots.append(_snapshot(day, index=index, epoch=epoch))
        return snapshots

    inside = review_index_portfolio("appdb", history(70)).subjects[0]
    assert inside["state"] == "observe"
    assert "stable_counter_epoch" in inside["reason_codes"]

    outside = review_index_portfolio("appdb", history(20)).subjects[0]
    assert outside["state"] == "drop_candidate"


def test_physical_database_change_inside_window_observes() -> None:
    def history(change_day: int) -> list[IndexReviewSnapshotV1]:
        return [
            _snapshot(
                day,
                database_incarnation=(
                    "physical-db-2" if day >= change_day else "physical-db-1"
                ),
            )
            for day in range(60)
        ]

    inside = review_index_portfolio("appdb", history(40)).subjects[0]
    assert inside["state"] == "observe"
    assert "stable_engine_and_database" in inside["reason_codes"]
    assert inside["removal_gate"]["observation"]["physical_database_change"] is True

    assert (
        review_index_portfolio("appdb", history(10)).subjects[0]["state"]
        == "drop_candidate"
    )


def test_failover_then_clean_unused_days_yields_drop_candidate() -> None:
    # Failover one hour before the day-20 capture: one unobserved hour, then
    # a new counter epoch that starts at zero and stays at zero.
    snapshots = _history(40, restarts={20: 1})

    subject = review_index_portfolio("appdb", snapshots).subjects[0]
    observation = subject["removal_gate"]["observation"]

    assert subject["state"] == "drop_candidate"
    assert [segment["boundary"] for segment in observation["segments"]] == [
        "window_anchor",
        "engine_start",
    ]
    assert observation["segments"][1]["began_in_window"] is True
    assert observation["unobserved_hours_total"] == pytest.approx(23.0)
    assert observation["max_unobserved_hours"] == pytest.approx(23.0)
    assert observation["coverage_pct"] == pytest.approx(100 * (1 - 23 / 840), abs=0.001)
    assert observation["gaps"] == [
        {
            "from_utc": _utc(_BASE_TIME + timedelta(days=19)),
            "to_utc": _utc(_BASE_TIME + timedelta(days=20, hours=-1)),
            "hours": pytest.approx(23.0),
            "left_run_id": "run-19",
            "right_run_id": "run-20",
            "reason": "engine_start",
        }
    ]


def test_one_missed_capture_inside_an_epoch_is_tolerated() -> None:
    # Counters are cumulative inside an epoch, so a missed day loses nothing.
    snapshots = _history(37, missing=frozenset({17}))

    subject = review_index_portfolio("appdb", snapshots).subjects[0]
    observation = subject["removal_gate"]["observation"]

    assert subject["state"] == "drop_candidate"
    assert observation["unobserved_hours_total"] == 0
    assert observation["coverage_pct"] == 100


@pytest.mark.parametrize(
    "seeks",
    [lambda day: 0, lambda day: 50 if 10 <= day < 20 else 0],
    ids=["unused", "reads_hidden_by_a_silent_reset"],
)
def test_capture_gap_over_48_hours_inside_one_epoch_observes(seeks) -> None:
    # Only a counter decrease reveals a reset with no engine start change,
    # and sparse captures miss it: seeks on days 10-19, then a silent reset to
    # zero before the day-29 capture. Captures at most 48 h apart bound that
    # blind spot like any other unobserved interval.
    snapshots = [
        snapshot
        for snapshot in _history(37, seeks=seeks, azure_default_query_store=True)
        if snapshot.run_id in {"run-0", "run-1", "run-29", "run-36"}
    ]

    subject = review_index_portfolio("appdb", snapshots).subjects[0]
    observation = subject["removal_gate"]["observation"]

    assert subject["state"] == "observe"
    assert subject["reason_codes"] == ["continuous_usable_days"]
    assert observation["max_gap_hours"] == pytest.approx(672.0)
    assert observation["no_gap_over_48_hours"] is False
    assert observation["enough_observation_days"] is False


@pytest.mark.parametrize(
    ("unusable_days", "state"),
    [(frozenset({20}), "drop_candidate"), (frozenset({20, 21, 22}), "observe")],
)
def test_captures_without_usable_counters_count_as_missed(
    unusable_days: frozenset[int], state: str
) -> None:
    # A capture whose usage read failed cannot reveal a reset, so the 48 h
    # bound runs between captures with usable counters, not raw captures.
    snapshots = []
    for snapshot in _history(37):
        if int(snapshot.run_id.removeprefix("run-")) in unusable_days:
            item = dict(snapshot.subjects[0])
            item["coverage"] = {**item["coverage"], "usage": "unavailable"}
            snapshot = replace(snapshot, subjects=(item,))
        snapshots.append(snapshot)

    subject = review_index_portfolio("appdb", snapshots).subjects[0]
    observation = subject["removal_gate"]["observation"]

    assert subject["state"] == state
    assert observation["max_gap_hours"] == pytest.approx(24.0)
    assert observation["max_usable_gap_hours"] == pytest.approx(
        24.0 * (len(unusable_days) + 1)
    )
    if state == "observe":
        assert subject["reason_codes"] == ["continuous_usable_days"]


def test_reads_hidden_by_a_reset_keep_the_index() -> None:
    # The old epoch shows 7 old seeks and no growth. After the failover the
    # new epoch's first counters already show 3 seeks: the absolute counters
    # of an epoch that began in the window expose reads a delta would hide.
    snapshots = _history(
        40,
        restarts={20: 6},
        seeks=lambda day: 3 if day >= 20 else 7,
    )

    subject = review_index_portfolio("appdb", snapshots).subjects[0]
    gate = subject["removal_gate"]

    assert subject["state"] == "keep"
    assert subject["reason_codes"] == ["protected_or_used"]
    assert gate["gates"]["zero_seek_scan_lookup_deltas"] is False
    assert gate["gates"]["counters_never_decrease"] is True
    assert [segment["read_count"] for segment in gate["observation"]["segments"]] == [0, 3]


def test_counter_decrease_without_engine_start_change_observes() -> None:
    # A decrease with the same sqlserver_start_time is a reset of unknown
    # cause and time: reads before it are lost, so the gate stays closed.
    snapshots = _history(40, seeks=lambda day: 2 if day >= 30 else 5)

    subject = review_index_portfolio("appdb", snapshots).subjects[0]

    assert subject["state"] == "observe"
    assert "counters_never_decrease" in subject["reason_codes"]


def test_reads_in_one_epoch_keep_the_index_when_another_epoch_decreased() -> None:
    # Any read evidence in the window means keep. Epoch 1 shows 4 seeks; the
    # unknown reset inside epoch 2 must not turn proven reads into observe.
    def seeks(day: int) -> int:
        if day < 20:
            return 4 if day >= 10 else 0
        return 3 if day < 30 else 1

    subject = review_index_portfolio(
        "appdb", _history(40, restarts={20: 1}, seeks=seeks)
    ).subjects[0]

    assert [
        segment["read_count"]
        for segment in subject["removal_gate"]["observation"]["segments"]
    ] == [4, None]
    assert subject["state"] == "keep"
    assert subject["reason_codes"] == ["protected_or_used"]


def test_reads_proven_before_an_unexplained_decrease_keep_the_index() -> None:
    # One epoch: seeks grow from 0 to 50 on day 10, then fall back to 0 on
    # day 20 with no engine start change. The decrease hides how many reads
    # the segment saw, but the growth already proved reads in the window.
    snapshots = _history(37, seeks=lambda day: 50 if 10 <= day < 20 else 0)

    subject = review_index_portfolio("appdb", snapshots).subjects[0]
    (segment,) = subject["removal_gate"]["observation"]["segments"]

    assert segment["read_count"] is None
    assert segment["proven_read_count"] == 50
    assert subject["state"] == "keep"
    assert subject["reason_codes"] == ["protected_or_used"]


def test_unobserved_interval_over_48_hours_across_a_reset_observes() -> None:
    # Days 21 and 22 were not captured and the engine restarted 4 hours
    # before the day-23 capture: reads from day 20 to the restart are lost.
    blocked = _history(40, restarts={23: 4}, missing=frozenset({21, 22}))
    subject = review_index_portfolio("appdb", blocked).subjects[0]

    assert subject["state"] == "observe"
    assert "continuous_usable_days" in subject["reason_codes"]
    assert subject["removal_gate"]["observation"]["max_unobserved_hours"] == pytest.approx(68.0)

    # An early restart does not make the same 72 h capture gap safe: on Azure
    # an engine can start well before the database fails over onto it, so
    # reads on the old primary after the restart are lost too.
    early = _history(40, restarts={23: 64}, missing=frozenset({21, 22}))
    early_subject = review_index_portfolio("appdb", early).subjects[0]
    early_observation = early_subject["removal_gate"]["observation"]

    assert early_subject["state"] == "observe"
    assert early_subject["reason_codes"] == ["continuous_usable_days"]
    assert early_observation["max_unobserved_hours"] == pytest.approx(8.0)
    assert early_observation["max_usable_gap_hours"] == pytest.approx(72.0)


def test_usage_coverage_below_95_percent_observes() -> None:
    # Three restarts, each 20 hours after the previous capture: no single
    # interval is over 48 h, but 60 of 840 window hours are unobserved.
    snapshots = _history(36, restarts={10: 4, 20: 4, 30: 4})

    subject = review_index_portfolio("appdb", snapshots).subjects[0]
    observation = subject["removal_gate"]["observation"]

    assert subject["state"] == "observe"
    assert "continuous_usable_days" in subject["reason_codes"]
    assert observation["max_unobserved_hours"] == pytest.approx(20.0)
    assert observation["coverage_pct"] == pytest.approx(100 * (1 - 60 / 840), abs=0.001)


@pytest.mark.parametrize(("missed_day", "state"), [(25, "drop_candidate"), (11, "observe")])
def test_failover_next_to_a_missed_capture_exceeds_35_day_coverage(
    missed_day: int, state: str
) -> None:
    # Business Critical failover: the new primary's sqlserver_start_time is
    # weeks old, so the whole capture gap before it is unobserved. On a
    # 35-day window 95% coverage allows 42 h in total, tighter than the 48 h
    # single-interval limit. A missed capture right before the failover makes
    # a 48 h blind spot, so the index stays in observe while it is in the window.
    snapshots = _history(
        40,
        restarts={12: 24 * 40},
        missing=frozenset({missed_day}),
        azure_default_query_store=True,
    )

    subject = review_index_portfolio("appdb", snapshots).subjects[0]
    observation = subject["removal_gate"]["observation"]

    assert subject["state"] == state
    assert [gap["reason"] for gap in observation["gaps"]] == ["engine_start_time_unknown"]
    if state == "observe":
        assert subject["reason_codes"] == ["continuous_usable_days"]
        assert observation["max_unobserved_hours"] == pytest.approx(48.0)
        assert observation["coverage_pct"] == pytest.approx(100 * (1 - 48 / 840), abs=0.001)


def test_index_with_no_usage_row_counts_as_zero_reads() -> None:
    # sys.dm_db_index_usage_stats has no row until the first use after an
    # engine start; with a known start time that means zero reads.
    unused = review_index_portfolio("appdb", _history(36, seeks=lambda day: None))
    assert unused.subjects[0]["state"] == "drop_candidate"
    assert unused.subjects[0]["removal_gate"]["read_count"] == 0

    first_seek = review_index_portfolio(
        "appdb", _history(36, seeks=lambda day: 1 if day >= 30 else None)
    )
    assert first_seek.subjects[0]["state"] == "keep"


def test_missing_usage_row_without_engine_start_is_not_zero() -> None:
    snapshots = [
        replace(snapshot, engine_start_time_utc=None)
        for snapshot in _history(36, seeks=lambda day: None)
    ]

    subject = review_index_portfolio("appdb", snapshots).subjects[0]

    assert subject["state"] == "observe"
    assert "complete_usable_counter_coverage" in subject["reason_codes"]


def test_index_read_only_at_month_end_is_kept() -> None:
    # Seeks only on Jan 31, Feb 28 and Mar 31. Every 35-day window holds a
    # month-end, so every review from the first full window on keeps it.
    month_ends = {30, 58, 89}
    snapshots = _history(
        100,
        seeks=lambda day: sum(5 for month_end in month_ends if day >= month_end),
    )

    states = {
        review_index_portfolio("appdb", snapshots, as_of_run_id=f"run-{day}")
        .subjects[0]["state"]
        for day in range(35, 100)
    }

    assert states == {"keep"}


def test_default_azure_query_store_settings_after_failover_and_missed_day_drop() -> None:
    # Live check 14: AUTO capture, 30-day stale threshold, one failover and
    # one missed daily capture. Daily captures chain Query Store coverage
    # across the 35-day window, so the unused index reaches drop_candidate.
    snapshots = _history(
        40,
        restarts={12: 2},
        missing=frozenset({25}),
        azure_default_query_store=True,
    )

    review = review_index_portfolio("appdb", snapshots)
    subject = review.subjects[0]

    assert subject["state"] == "drop_candidate"
    assert subject["removal_gate"]["observation"]["query_store_chain_complete"] is True
    assert subject["removal_gate"]["observation"]["query_store_advisory"] is True

    used = _history(
        40,
        restarts={12: 2},
        missing=frozenset({25}),
        seeks=lambda day: 2 if day >= 30 else 0,
        azure_default_query_store=True,
    )
    assert review_index_portfolio("appdb", used).subjects[0]["state"] == "keep"


def test_query_store_advisory_blockers_need_chained_retention() -> None:
    # One epoch, no reads, but no capture between day 3 and day 36: a 30-day
    # stale threshold cannot cover a 33-day capture gap.
    snapshots = [
        snapshot
        for snapshot in _history(40, azure_default_query_store=True)
        if snapshot.run_id in {f"run-{day}" for day in (0, 1, 2, 3, 36, 37, 38, 39)}
    ]

    subject = review_index_portfolio("appdb", snapshots).subjects[0]

    assert subject["state"] == "observe"
    assert "query_store_coverage_complete" in subject["reason_codes"]
    assert subject["removal_gate"]["observation"]["query_store_chain_complete"] is False


def test_hard_query_store_blockers_are_never_advisory() -> None:
    capped = dict(_AZURE_DEFAULT_QUERY_STORE_COVERAGE)
    capped["blockers"] = [*capped["blockers"], "query_store_plan_cap_reached"]
    capped["capped"] = True
    snapshots = [
        replace(snapshot, coverage={**snapshot.coverage, "query_store": capped})
        if snapshot.run_id == "run-30"
        else snapshot
        for snapshot in _history(40, azure_default_query_store=True)
    ]

    subject = review_index_portfolio("appdb", snapshots).subjects[0]

    assert subject["state"] == "observe"
    assert "query_store_coverage_complete" in subject["reason_codes"]


def test_query_store_retention_shorter_than_window_is_reported_as_gap() -> None:
    short = review_index_portfolio("appdb", _history(40, azure_default_query_store=True))
    assert short.observation["window_start_utc"] == _utc(_BASE_TIME + timedelta(days=4))
    assert short.observation["query_store_retention_days"] == 30
    assert short.observation["gaps"] == [
        {
            "from_utc": _utc(_BASE_TIME + timedelta(days=4)),
            "to_utc": _utc(_BASE_TIME + timedelta(days=9)),
            "hours": 120.0,
            "reason": "query_store_retention_shorter_than_window",
        }
    ]

    covered = review_index_portfolio("appdb", _history(40))
    assert covered.observation["query_store_retention_days"] == 90
    assert covered.observation["gaps"] == []


def test_executed_query_store_reference_counts_only_inside_the_window() -> None:
    def history(last_seen_day: int) -> list[IndexReviewSnapshotV1]:
        snapshots = []
        for day in range(60):
            index = _index()
            if day >= last_seen_day:
                index["query_store_references"] = [
                    {
                        "query_id": 7,
                        "plan_id": 70,
                        "execution_count": 4,
                        "last_seen": _utc(_BASE_TIME + timedelta(days=last_seen_day)),
                    }
                ]
            snapshots.append(_snapshot(day, index=index))
        return snapshots

    inside = review_index_portfolio("appdb", history(40)).subjects[0]
    assert inside["state"] == "keep"

    before_window = review_index_portfolio("appdb", history(10)).subjects[0]
    assert before_window["state"] == "drop_candidate"


def test_epoch_counter_definition_protection_and_special_type_gates() -> None:
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
    assert selector["minimum_days"] == 35
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


def _service_over(
    snapshots: list[IndexReviewSnapshotV1],
    *,
    clock=None,
) -> IndexReviewService:
    run_pairs = [
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
        for snapshot in snapshots
    ]

    class Repository:
        async def list_history(self, database_name):
            assert database_name == "appdb"
            return run_pairs

    executor = SimpleNamespace(config=SimpleNamespace(server="server.database.windows.net"))
    return IndexReviewService(
        executor, Repository(), database_policy=_policy(), clock=clock
    )


@pytest.mark.asyncio
async def test_get_review_reconstructs_deterministically_after_restart() -> None:
    snapshots = [_snapshot(day) for day in range(90)]
    service = _service_over(snapshots)
    first = review_index_portfolio("appdb", snapshots)
    restored = await service.get_review("appdb", first.review_id)
    assert restored.review_id == first.review_id
    assert restored.as_dict() == first.as_dict()


@pytest.mark.asyncio
async def test_recheck_review_id_is_bounded_and_round_trips() -> None:
    snapshots = [_snapshot(day) for day in range(40)]
    service = _service_over(
        snapshots, clock=lambda: _BASE_TIME + timedelta(days=39, hours=1)
    )
    base = await service.review_portfolio("appdb", as_of_run_id="run-36")

    recheck = await service.review_portfolio("appdb", prior_review_id=base.review_id)

    assert len(recheck.review_id) < 200
    assert parse_review_id(recheck.review_id)["prior"] is not None
    assert recheck.prior_review_id == base.review_id
    assert recheck.prior_base_run_id == "run-36"
    assert recheck.subjects[0]["state"] == "drop_candidate"
    assert recheck.subjects[0]["recheck"] == {
        "is_recheck": True,
        "prior_state": "drop_candidate",
        "transition": "unchanged",
    }
    # The prior review only drives the transition; the gate still reads the
    # full history instead of the three runs after the prior review.
    assert recheck.observation["snapshot_count"] == 40
    restored = await service.get_review("appdb", recheck.review_id)
    assert restored.as_dict() == recheck.as_dict()


@pytest.mark.asyncio
async def test_review_id_issued_by_2_6_0_does_not_resolve_to_new_advice() -> None:
    # 2.6.0 issued this id for the history below (90-day window) with state
    # observe. The 2.6.1 gate would rebuild the same selector as
    # drop_candidate, so an id from the earlier gate must fail closed.
    issued_by_2_6_0 = (
        "ir1.a=n2imnneve3wf6plg6mnxrsx4xp.d=90.p=4lleekiekkwu5f2d345jch2j76"
        ".db=nakqz7cfxvrkpilgxntqwctrvy.as=mkhykjzlfw26kfiqgm76yqwddh.pr=-"
        ".h=3cpzwqchkevd7shft7y7pi7dnm.s=t7tjjofgmoghejrxh2vauzhwjz"
    )
    service = _service_over(_history(100, restarts={80: 2}))

    with pytest.raises(IndexReviewIntegrityError):
        await service.get_review("appdb", issued_by_2_6_0)


def test_compact_run_selector_prefix_must_be_unambiguous(monkeypatch) -> None:
    history = [(SimpleNamespace(run_id=f"run-{day}"), None) for day in (1, 2)]
    monkeypatch.setattr(
        "azure_sql_mcp.index_review._compact", lambda value: "abcdefghij" + value[-1]
    )

    assert IndexReviewService._find_compact_run(history, "abcdefghij2") == history[1]
    with pytest.raises(IndexReviewIntegrityError):
        IndexReviewService._find_compact_run(history, "abcdefghij")


def test_index_review_profile_contains_base_tools_and_recall_only_learning(server_config_factory) -> None:
    config = server_config_factory(profile=McpProfile.INDEX_REVIEW)
    base = {
        "check_runtime_status",
        "list_databases",
        "check_capabilities",
        "capture_index_review_snapshot",
        "review_index_portfolio",
        "get_index_review",
        "review_workload_indexes",
        "check_statistics_health",
        "get_top_queries",
        "get_query_store_trend",
    }
    enabled = {name for name in base | set(LEARNING_TOOL_NAMES) if config.is_tool_enabled(name)}
    assert enabled == base | {"recall_lessons"}
    assert config.is_tool_enabled("execute_sql") is False


def test_index_review_remote_surface_is_exactly_the_base_tools(monkeypatch) -> None:
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
        "review_workload_indexes",
        "check_statistics_health",
        "get_top_queries",
        "get_query_store_trend",
    }
