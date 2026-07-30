from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from azure_sql_mcp.artifacts import ExplainPlanArtifact
from azure_sql_mcp.candidate_lineage import validate_combined_parent_request
from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import parse_candidate_key
from azure_sql_mcp.index_optimizer import IndexCandidate
from azure_sql_mcp.performance_contracts import EvidenceEnvelopeV1
from azure_sql_mcp.plans import ProfiledPlanResult
from azure_sql_mcp.performance_store import IdempotencyConflictError
from azure_sql_mcp.performance_workflows import database_fingerprint
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.tuning_sessions import InvalidTransitionError


def _policy() -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "sandbox",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "allow_test_indexes": True,
                    "allow_plan_apply": False,
                    "max_benchmark_executions": 80,
                }
            },
        }
    )


def _profile(
    elapsed_ms: float,
    *,
    index_name: str | None = None,
    result_value: int = 1,
) -> ProfiledPlanResult:
    summary = {
        "statement_count": 1,
        "operator_count": 1,
        "actual_metrics": {
            "actual_cpu_ms": elapsed_ms / 2,
            "actual_elapsed_ms": elapsed_ms,
            "actual_rows": 1,
            "actual_logical_reads": None,
            "actual_physical_reads": None,
            "query_metric_source": "showplan_query_time_stats",
            "read_metric_source": "not_available_as_reliable_query_total",
        },
        "warnings": [],
        "missing_indexes": [],
        "top_operators": [],
    }
    raw_xml = (
        (
            '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">'
            f'<IndexScan Index="[dbo].[{index_name}]" />'
            "</ShowPlanXML>"
        )
        if index_name
        else (
            '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" />'
        )
    )
    return ProfiledPlanResult(
        plan=ExplainPlanArtifact(
            database_name="appdb",
            analyze=True,
            summary=summary,
            raw_xml=raw_xml,
        ),
        result_sets=[
            QueryResult(
                columns=("id",),
                rows=[{"id": result_value}],
                column_type_signatures=("synthetic-int",),
                positional_rows=((result_value,),),
            )
        ],
        elapsed_wall_ms=elapsed_ms,
        user_query_executions=1,
        truncated=False,
        metric_provenance="test",
    )


def _install_catalog(
    monkeypatch: pytest.MonkeyPatch,
    *,
    key_columns: list[str],
    include_columns: list[str] | None = None,
    filter_definition: str | None = None,
    is_unique: bool = False,
    index_type: str = "NONCLUSTERED",
    data_space_name: str | None = None,
    data_space_type: str | None = None,
    partition_scheme_name: str | None = None,
    partition_function_name: str | None = None,
    partition_columns: tuple[str, ...] = (),
    partition_compression: tuple[tuple[int, str], ...] = (),
    sequence: tuple[str, ...] = ("empty", "present", "present", "empty"),
) -> tuple[str, AsyncMock]:
    provisional = IndexCandidate(
        schema="dbo",
        table="Items",
        key_columns=tuple(key_columns),
        include_columns=tuple(include_columns or ()),
        filter_definition=filter_definition,
        is_unique=is_unique,
    )
    index_name = f"IX_Testing_{provisional.definition_fingerprint[:16]}"
    existing = ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name=index_name,
        index_type=index_type,
        key_columns=tuple(parse_candidate_key(column) for column in key_columns),
        include_columns=tuple(include_columns or ()),
        filter_definition=filter_definition,
        is_unique=is_unique,
        data_space_name=data_space_name,
        data_space_type=data_space_type,
        partition_scheme_name=partition_scheme_name,
        partition_function_name=partition_function_name,
        partition_columns=partition_columns,
        partition_compression=partition_compression,
    )
    catalog = AsyncMock(
        side_effect=[
            [] if state == "empty" else [existing]
            for state in sequence
        ]
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        catalog,
    )
    return index_name, catalog


def _app(server_config_factory) -> AzureSqlMcpApplication:
    app = AzureSqlMcpApplication(
        server_config_factory(
            access_mode=AccessMode.UNRESTRICTED,
            write_policy=WritePolicy.APPLY,
            profile=McpProfile.SANDBOX,
            performance_state_dir=":memory:",
        )
    )
    policy = _policy()
    app.database_policy = policy
    app.performance_workflows.database_policy = policy

    async def fetch_rows(
        _database_name: str,
        sql: str,
        *,
        params: tuple[object, ...] = (),
        **_kwargs: object,
    ) -> list[dict[str, object]]:
        if "sys.sql_modules" in sql and "object_type_code" in sql:
            return [
                {
                    "schema_name": "dbo",
                    "object_name": "Items",
                    "object_type_code": "U",
                    "object_type": "USER_TABLE",
                    "definition": None,
                    "is_encrypted": 0,
                }
            ]
        if "owner_marker" in sql and "definition_marker" in sql:
            index_name = str(params[-1])
            lease = next(
                item
                for item in app.performance_store.list_open_index_leases()
                if item["index_name"] == index_name
            )
            metadata = lease["metadata"]
            return [
                {
                    "index_id": 2,
                    "owner_marker": metadata["lease_owner_fence"],
                    "definition_marker": lease["object_fingerprint"],
                }
            ]
        return [{"schema_name": "dbo", "table_name": "Items"}]

    app.executor.fetch_all = AsyncMock(
        side_effect=fetch_rows
    )  # type: ignore[method-assign]
    return app


def _candidate(
    app: AzureSqlMcpApplication,
    sql: str,
    *,
    parameter_cases: list[dict[str, object]] | None = None,
) -> tuple[str, str]:
    case = app.performance_workflows.start_case(
        "appdb",
        sql,
        parameter_cases=parameter_cases,
    )
    session = app.performance_workflows.start_session(case.case_id, "appdb")
    candidate = app.performance_workflows.add_candidate(
        session["session_id"],
        sql,
        strategy="index",
    )
    return session["session_id"], candidate["candidate_id"]


def _lineage_candidate(
    app: AzureSqlMcpApplication,
    *,
    baseline_sql: str,
    rewrite_sql: str,
    child_strategy: str = "rewrite_plus_index",
    parent_scope: str = "proven",
) -> tuple[str, str, str]:
    case = app.performance_workflows.start_case("appdb", baseline_sql)
    session = app.performance_workflows.start_session(case.case_id, "appdb")
    session_id = session["session_id"]
    parent = app.performance_workflows.add_candidate(
        session_id,
        rewrite_sql,
        strategy="predicate",
    )
    parent_id = parent["candidate_id"]
    app.tuning_sessions.start_screening(session_id)
    app.tuning_sessions.mark_candidate_finalist(session_id, parent_id)
    performance_only = parent_scope == "performance_only"
    proof = app.performance_store.create_evidence(
        EvidenceEnvelopeV1(
            source="azure-sql-mcp",
            kind="tuning_finalist",
            query_fingerprint=case.query_fingerprint,
            database_fingerprint=case.database_fingerprint,
            observed_execution_count=12,
            metrics=(
                {
                    "classification": "performance_only",
                    "performance_classification": "improved",
                }
                if performance_only
                else {"classification": "improved"}
            ),
            metadata={
                "session_id": session_id,
                "candidate_id": parent_id,
                "phase": "finalist",
                "proof_scope": (
                    "performance_only"
                    if performance_only
                    else "direct_snapshot"
                ),
                "equivalence_deferred": performance_only,
                "equivalence": (
                    []
                    if performance_only
                    else [
                        {
                            "status": "match",
                            "proven_for_parameter_case": True,
                            "same_snapshot": True,
                            "snapshot_isolation_verified": True,
                        }
                    ]
                ),
            },
        )
    )
    app.tuning_sessions.record_candidate_result(
        session_id,
        parent_id,
        state="performance_only" if performance_only else "improved",
        finalist_runs=5,
        parameter_cases=1,
        executions=12,
        evidence_ids=(proof.evidence_id,),
    )
    if child_strategy == "combined":
        parent_candidate = app.tuning_sessions.get_candidate(parent_id)
        lineage = validate_combined_parent_request(
            session_id=session_id,
            rewrite_fingerprint=parent_candidate.rewrite_fingerprint or "",
            parent_reference=f"candidate:{parent_id}",
            parent=parent_candidate,
            evidence=[proof],
        )
        child = app.tuning_sessions.add_candidate(
            session_id,
            strategy="combined",
            rewrite_fingerprint=parent_candidate.rewrite_fingerprint,
            rewrite_artifact_ref=f"candidate:{parent_id}",
            metadata={"lineage": lineage},
        )
        child_id = child.candidate_id
    else:
        child = app.performance_workflows.add_candidate(
            session_id,
            rewrite_sql,
            strategy=child_strategy,
            artifact_ref=f"candidate:{parent_id}",
        )
        child_id = child["candidate_id"]
    return session_id, parent_id, child_id


def _set_session_deadline(
    app: AzureSqlMcpApplication,
    session_id: str,
    deadline_at_utc: str | None,
) -> None:
    row = app.performance_store._connection.execute(
        "SELECT payload FROM tuning_sessions WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    assert row is not None
    payload = json.loads(row["payload"])
    payload["deadline_at_utc"] = deadline_at_utc
    app.performance_store._connection.execute(
        "UPDATE tuning_sessions SET payload = ? WHERE session_id = ?",
        (json.dumps(payload, sort_keys=True), session_id),
    )
    app.performance_store._connection.commit()


@pytest.mark.asyncio
async def test_unproven_combined_parent_stops_before_index_ddl(
    server_config_factory,
) -> None:
    app = _app(server_config_factory)
    baseline_sql = "SELECT id FROM dbo.Items"
    rewrite_sql = "SELECT id FROM dbo.Items AS candidate"
    case = app.performance_workflows.start_case("appdb", baseline_sql)
    session = app.performance_workflows.start_session(case.case_id, "appdb")
    parent = app.performance_workflows.add_candidate(
        session["session_id"],
        rewrite_sql,
        strategy="predicate",
    )
    child = app.tuning_sessions.add_candidate(
        session["session_id"],
        strategy="combined",
        rewrite_fingerprint=parent["rewrite_fingerprint"],
        rewrite_artifact_ref=f"candidate:{parent['candidate_id']}",
        metadata={
            "lineage": {
                "parent_candidate_id": parent["candidate_id"],
                "parent_evidence_id": "missing-evidence",
            }
        },
    )
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="completed finalist"):
        await app._benchmark_index_candidate(
            "appdb",
            session["session_id"],
            child.candidate_id,
            rewrite_sql,
            "dbo",
            "Items",
            ["id"],
            None,
            None,
            False,
            "finalist",
            True,
            True,
            30,
            "unproven-combined-parent",
        )

    app._create_test_index.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("child_strategy", ["rewrite_plus_index", "combined"])
async def test_proven_lineage_candidate_runs_marginal_aba(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
    child_strategy: str,
) -> None:
    app = _app(server_config_factory)
    baseline_sql = "SELECT id FROM dbo.Items"
    rewrite_sql = "SELECT id FROM dbo.Items AS candidate"
    session_id, parent_id, child_id = _lineage_candidate(
        app,
        baseline_sql=baseline_sql,
        rewrite_sql=rewrite_sql,
        child_strategy=child_strategy,
    )
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["id"],
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(5)],
            *[_profile(50, index_name=index_name) for _ in range(5)],
            *[_profile(100) for _ in range(5)],
        ]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        child_id,
        rewrite_sql,
        "dbo",
        "Items",
        ["id"],
        None,
        None,
        False,
        "finalist",
        True,
        True,
        30,
        f"proven-{child_strategy}-parent",
    )

    assert result["classification"] == "improved"
    assert result["proof_scope"] == "aba_result_stability"
    assert result["lineage"]["parent_candidate_id"] == parent_id
    assert result["lineage"]["parent_equivalence"] == "proven"
    stored = app.performance_store.get_candidate(child_id)
    evidence = app.performance_store.get_evidence(stored.evidence_ids[-1])
    assert evidence.metadata["lineage"]["parent_candidate_id"] == parent_id
    assert evidence.metadata["proof_scope"] == "aba_result_stability"
    assert result["equivalence"][0]["plan_used_expected_index"] is True
    app._create_test_index.assert_awaited_once()
    app._drop_test_index.assert_awaited_once()


@pytest.mark.asyncio
async def test_rewrite_plus_index_child_of_performance_only_parent_stays_unproven(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    baseline_sql = "SELECT id FROM dbo.Items"
    rewrite_sql = "SELECT id FROM dbo.Items AS candidate"
    session_id, parent_id, child_id = _lineage_candidate(
        app,
        baseline_sql=baseline_sql,
        rewrite_sql=rewrite_sql,
        parent_scope="performance_only",
    )
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["id"],
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(5)],
            *[_profile(50, index_name=index_name) for _ in range(5)],
            *[_profile(100) for _ in range(5)],
        ]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        child_id,
        rewrite_sql,
        "dbo",
        "Items",
        ["id"],
        None,
        None,
        False,
        "finalist",
        True,
        True,
        30,
        "performance-only-rewrite-plus-index-parent",
    )

    assert result["classification"] == "performance_only"
    assert result["proof_scope"] == "performance_only"
    assert result["lineage"]["parent_candidate_id"] == parent_id
    assert result["lineage"]["parent_equivalence"] == "unproven"
    stored = app.tuning_sessions.get_candidate(child_id)
    assert stored.state == "performance_only"
    assert stored.failure_code is None


@pytest.mark.asyncio
async def test_volatile_index_screening_reports_performance_only(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT TOP (1) id, NEWID() AS token FROM dbo.Items"
    session_id, candidate_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["id"],
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[
                _profile(50, index_name=index_name, result_value=2)
                for _ in range(3)
            ],
            *[_profile(100) for _ in range(3)],
        ]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["id"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "volatile-index-screening",
    )

    assert result["classification"] == "promising"
    assert result["decision_basis"] == "observed_range_separation_v1"
    assert result["parameter_results"][0]["decision_basis"] == (
        "observed_range_separation_v1"
    )
    assert result["durable_state"] == "screening"
    assert result["proof_scope"] == "performance_only"
    assert result["equivalence"][0]["status"] == "mismatch"
    assert result["equivalence_preflight"]["direct_snapshot_supported"] is False
    evidence_metrics = app.performance_store.get_evidence(
        result["evidence_id"]
    ).metrics
    assert evidence_metrics["performance_classification"] == "promising"
    assert evidence_metrics["decision_basis"] == "observed_range_separation_v1"
    stored_candidate = app.tuning_sessions.get_candidate(candidate_id)
    assert stored_candidate.state == "screening"
    assert stored_candidate.failure_code is None
    app._create_test_index.assert_awaited_once()
    app._drop_test_index.assert_awaited_once()

    replay = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["id"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "volatile-index-screening",
    )

    assert replay["classification"] == "promising"
    assert replay["performance_classification"] == "promising"
    assert replay["decision_basis"] == "observed_range_separation_v1"
    assert replay["durable_state"] == "screening"
    assert replay["evidence_id"] == result["evidence_id"]
    assert replay["recovered_from_durable_evidence"] is True
    assert app.plans.profile_query.await_count == 9
    app._create_test_index.assert_awaited_once()
    app._drop_test_index.assert_awaited_once()


@pytest.mark.asyncio
async def test_volatile_index_finalist_runs_complete_performance_workload(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT TOP (1) id, NEWID() AS token FROM dbo.Items"
    session_id, candidate_id = _candidate(app, sql)
    app._cleanup_expired_index_leases = AsyncMock(  # type: ignore[method-assign]
        return_value={
            "expired": 1,
            "cleaned": 1,
            "cleanup_required": 0,
        }
    )
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["id"],
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(5)],
            *[_profile(50, index_name=index_name) for _ in range(5)],
            *[_profile(100) for _ in range(5)],
        ]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["id"],
        None,
        None,
        False,
        "finalist",
        True,
        True,
        30,
        "volatile-index-finalist",
    )

    assert result["classification"] == "performance_only"
    assert result["performance_classification"] == "improved"
    assert result["durable_state"] == "performance_only"
    assert result["proof_scope"] == "performance_only"
    assert result["executions"] == 15
    app._cleanup_expired_index_leases.assert_awaited_once()
    app._create_test_index.assert_awaited_once()
    app._drop_test_index.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "catalog_kwargs",
    [
        {"index_type": "CLUSTERED"},
        {
            "index_type": "NONCLUSTERED",
            "data_space_name": "ps_Items",
            "data_space_type": "PARTITION_SCHEME",
            "partition_scheme_name": "ps_Items",
            "partition_function_name": "pf_Items",
            "partition_columns": ("status",),
            "partition_compression": ((1, "PAGE"),),
        },
    ],
)
async def test_existing_same_name_index_is_terminal_name_conflict_without_work(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
    catalog_kwargs: dict[str, object],
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["status"],
        sequence=("present",),
        **catalog_kwargs,
    )
    app.performance_store.reserve_execution_attempts = Mock()
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "same-name-conflict",
    )

    assert index_name == result["existing_index"]["name"]
    assert result["classification"] == "inconclusive"
    assert result["durable_state"] == "inconclusive"
    assert result["failure_code"] == "name_conflict"
    assert result["executions"] == 0
    assert result["session_continues"] is True
    app.performance_store.reserve_execution_attempts.assert_not_called()
    app._create_test_index.assert_not_awaited()
    app._drop_test_index.assert_not_awaited()
    app.plans.profile_query.assert_not_awaited()


@pytest.mark.asyncio
async def test_finalized_index_reservation_does_not_rerun_query_or_ddl(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[]),
    )
    app.performance_store.reserve_execution_attempts = Mock(
        return_value={
            "reservation_id": "execution-existing",
            "status": "completed",
            "version": 1,
        }
    )
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        ["id"],
        None,
        False,
        "screening",
        True,
        True,
        30,
        "same-index-request",
    )

    assert result["failure_code"] == "index_benchmark_request_already_finalized"
    assert result["executions"] == 0
    app._create_test_index.assert_not_awaited()
    app.plans.profile_query.assert_not_awaited()


@pytest.mark.asyncio
async def test_in_flight_index_reservation_does_not_rerun_query_or_ddl(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[]),
    )
    app.performance_store.reserve_execution_attempts = Mock(
        return_value={
            "reservation_id": "execution-in-flight",
            "status": "reserved",
            "version": 0,
            "replayed": True,
        }
    )
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        ["id"],
        None,
        False,
        "screening",
        True,
        True,
        30,
        "same-index-request",
    )

    assert result["failure_code"] == "index_benchmark_request_reconciliation_required"
    assert result["executions"] == 0
    app._create_test_index.assert_not_awaited()
    app.plans.profile_query.assert_not_awaited()


@pytest.mark.asyncio
async def test_index_retry_recovers_post_evidence_crash_without_query_or_ddl(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    index_name, catalog = _install_catalog(
        monkeypatch,
        key_columns=["status"],
    )
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[_profile(40, index_name=index_name) for _ in range(3)],
            *[_profile(100) for _ in range(3)],
        ]
    )
    original_record = app.tuning_sessions.record_candidate_result

    def fail_after_evidence(*_args, **_kwargs):
        raise RuntimeError("synthetic index post-evidence crash")

    monkeypatch.setattr(
        app.tuning_sessions,
        "record_candidate_result",
        fail_after_evidence,
    )
    with pytest.raises(RuntimeError, match="post-evidence crash"):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            None,
            None,
            False,
            "screening",
            True,
            True,
            30,
            "recover-index-evidence",
        )

    assert app.plans.profile_query.await_count == 9
    assert catalog.await_count == 4
    app._create_test_index.assert_awaited_once()
    app._drop_test_index.assert_awaited_once()

    orphaned = await app._get_tuning_session("appdb", session_id)
    assert [item["evidence_id"] for item in orphaned["evidence"]] == [
        app.performance_store.list_evidence_for_session(session_id)[0].evidence_id
    ]
    assert orphaned["evidence_reconciliation"] == {
        "attached_count": 0,
        "unattached_count": 1,
        "attached_evidence_ids": [],
        "unattached_evidence_ids": [
            app.performance_store.list_evidence_for_session(session_id)[0].evidence_id
        ],
        "reconciliation_required": True,
    }

    monkeypatch.setattr(
        app.tuning_sessions,
        "record_candidate_result",
        original_record,
    )
    recovered = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "recover-index-evidence",
    )

    assert recovered["classification"] == "improved"
    assert recovered["recovered_from_durable_evidence"] is True
    assert recovered["reservation_status"] == "completed"
    assert recovered["lease"]["status"] == "cleaned"
    assert app.plans.profile_query.await_count == 9
    assert catalog.await_count == 4
    app._create_test_index.assert_awaited_once()
    app._drop_test_index.assert_awaited_once()
    attached = await app._get_tuning_session("appdb", session_id)
    assert [item["evidence_id"] for item in attached["evidence"]] == [
        recovered["evidence_id"]
    ]
    assert attached["evidence_reconciliation"] == {
        "attached_count": 1,
        "unattached_count": 0,
        "attached_evidence_ids": [recovered["evidence_id"]],
        "unattached_evidence_ids": [],
        "reconciliation_required": False,
    }


@pytest.mark.asyncio
async def test_covering_index_result_is_idempotent_without_ddl_or_queries(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    covering = ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name="IX_Items_status",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("status"),),
        include_columns=("id",),
    )
    catalog = AsyncMock(return_value=[covering])
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        catalog,
    )
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    first = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        ["id"],
        None,
        False,
        "screening",
        True,
        True,
        30,
        "covering-index",
    )
    replay = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        ["id"],
        None,
        False,
        "screening",
        True,
        True,
        30,
        "covering-index",
    )

    assert first["classification"] == "neutral"
    assert replay["classification"] == "neutral"
    assert replay["durable_state"] == "neutral"
    assert catalog.await_count == 1
    app._create_test_index.assert_not_awaited()
    app.plans.profile_query.assert_not_awaited()

    with pytest.raises(IdempotencyConflictError, match="different idempotency key"):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            ["id"],
            None,
            False,
            "screening",
            True,
            True,
            30,
            "different-covering-index-key",
        )

    with pytest.raises(IdempotencyConflictError, match="different request"):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status", "id"],
            None,
            None,
            False,
            "screening",
            True,
            True,
            30,
            "covering-index",
        )

    app.tuning_sessions.complete_session(
        session_id,
        stopping_reason="terminal replay regression",
    )
    app._cleanup_expired_index_leases = AsyncMock(  # type: ignore[method-assign]
        return_value={"examined": 0, "cleaned": 0, "cleanup_required": 0}
    )
    with pytest.raises(
        InvalidTransitionError,
        match=r"completed.*get_tuning_session",
    ):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            ["id"],
            None,
            False,
            "screening",
            True,
            True,
            30,
            "covering-index",
        )
    app._cleanup_expired_index_leases.assert_not_awaited()
    assert catalog.await_count == 1
    app._create_test_index.assert_not_awaited()
    app.plans.profile_query.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["completed", "cancelled"])
async def test_terminal_session_rejects_index_benchmark_before_any_work(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
    terminal_status: str,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    if terminal_status == "completed":
        app.tuning_sessions.complete_session(
            session_id,
            stopping_reason="terminal session regression",
        )
    else:
        app.tuning_sessions.cancel_session(
            session_id,
            stopping_reason="terminal session regression",
        )

    app._cleanup_expired_index_leases = AsyncMock(  # type: ignore[method-assign]
        return_value={"examined": 0, "cleaned": 0, "cleanup_required": 0}
    )
    app._check_equivalence_preflight = AsyncMock()  # type: ignore[method-assign]
    app.performance_workflows._bind_case = AsyncMock()  # type: ignore[method-assign]
    app._resolve_canonical_table_identity = AsyncMock()  # type: ignore[method-assign]
    app.performance_store.bind_index_benchmark_request = Mock()  # type: ignore[method-assign]
    app.performance_store.reserve_execution_attempts = Mock()  # type: ignore[method-assign]
    app.performance_store.create_index_lease = Mock()  # type: ignore[method-assign]
    app.performance_store.create_evidence = Mock()  # type: ignore[method-assign]
    catalog = AsyncMock()
    monkeypatch.setattr("azure_sql_mcp.server.collect_existing_indexes", catalog)
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]
    fetch_all = AsyncMock()
    app.executor.fetch_all = fetch_all  # type: ignore[method-assign]

    with pytest.raises(
        InvalidTransitionError,
        match=rf"{terminal_status}.*get_tuning_session",
    ):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            None,
            None,
            False,
            "screening",
            True,
            True,
            30,
            f"{terminal_status}-session",
        )

    app._cleanup_expired_index_leases.assert_not_awaited()
    app._check_equivalence_preflight.assert_not_awaited()
    app.performance_workflows._bind_case.assert_not_awaited()
    app._resolve_canonical_table_identity.assert_not_awaited()
    app.performance_store.bind_index_benchmark_request.assert_not_called()
    app.performance_store.reserve_execution_attempts.assert_not_called()
    app.performance_store.create_index_lease.assert_not_called()
    app.performance_store.create_evidence.assert_not_called()
    catalog.assert_not_awaited()
    app._create_test_index.assert_not_awaited()
    app._drop_test_index.assert_not_awaited()
    app.plans.profile_query.assert_not_awaited()
    fetch_all.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminal_race_before_index_create_prevents_ddl_and_releases_budget(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    catalog = AsyncMock(return_value=[])
    monkeypatch.setattr("azure_sql_mcp.server.collect_existing_indexes", catalog)
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]

    profile_count = 0

    async def complete_after_baseline(*_args, **_kwargs):
        nonlocal profile_count
        profile_count += 1
        if profile_count == 3:
            app.tuning_sessions.complete_session(
                session_id,
                stopping_reason="completed during baseline",
            )
        return _profile(100)

    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=complete_after_baseline
    )

    with pytest.raises(
        InvalidTransitionError,
        match=r"completed.*get_tuning_session",
    ):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            None,
            None,
            False,
            "screening",
            True,
            True,
            30,
            "terminal-before-create",
        )

    app._create_test_index.assert_not_awaited()
    app._drop_test_index.assert_not_awaited()
    assert app.plans.profile_query.await_count == 3
    reservation_row = app.performance_store._connection.execute(
        "SELECT reservation_id FROM execution_reservations WHERE candidate_id = ?",
        (candidate_id,),
    ).fetchone()
    assert reservation_row is not None
    reservation = app.performance_store.get_execution_reservation(
        reservation_row["reservation_id"]
    )
    assert reservation["status"] in {"completed", "released"}
    assert reservation["dispatched_attempt_count"] == 3
    assert app.performance_store.list_open_index_leases() == []
    assert app.performance_store.list_evidence_for_session(session_id) == []


@pytest.mark.asyncio
async def test_terminal_race_after_index_create_still_cleans_owned_index(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    provisional = IndexCandidate(
        schema="dbo",
        table="Items",
        key_columns=("status",),
        include_columns=(),
    )
    index_name = f"IX_Testing_{provisional.definition_fingerprint[:16]}"
    existing = ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name=index_name,
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("status"),),
    )
    catalog_count = 0

    async def terminate_after_create(*_args, **_kwargs):
        nonlocal catalog_count
        catalog_count += 1
        if catalog_count == 1:
            return []
        if catalog_count == 2:
            app.tuning_sessions.complete_session(
                session_id,
                stopping_reason="completed after index create",
            )
            return [existing]
        if catalog_count == 3:
            return [existing]
        return []

    catalog = AsyncMock(side_effect=terminate_after_create)
    monkeypatch.setattr("azure_sql_mcp.server.collect_existing_indexes", catalog)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(100) for _ in range(3)]
    )

    with pytest.raises(
        InvalidTransitionError,
        match=r"completed.*get_tuning_session",
    ):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            None,
            None,
            False,
            "screening",
            True,
            True,
            30,
            "terminal-after-create",
        )

    app._create_test_index.assert_awaited_once()
    app._drop_test_index.assert_awaited_once()
    assert app.plans.profile_query.await_count == 3
    assert catalog.await_count == 4
    assert app.performance_store.list_open_index_leases() == []
    reservation_row = app.performance_store._connection.execute(
        "SELECT reservation_id FROM execution_reservations WHERE candidate_id = ?",
        (candidate_id,),
    ).fetchone()
    assert reservation_row is not None
    reservation = app.performance_store.get_execution_reservation(
        reservation_row["reservation_id"]
    )
    assert reservation["status"] in {"completed", "released"}
    assert reservation["dispatched_attempt_count"] == 3
    assert app.performance_store.list_evidence_for_session(session_id) == []


@pytest.mark.asyncio
async def test_first_index_slower_second_index_faster_and_session_continues(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, first_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    first_index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["status"],
        include_columns=["id"],
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[_profile(140, index_name=first_index_name) for _ in range(3)],
            *[_profile(100) for _ in range(3)],
        ]
    )

    first = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        first_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        ["id"],
        None,
        False,
        "screening",
        True,
        True,
        30,
        "first-index",
    )
    assert first["classification"] == "regressed"
    assert first["session_continues"] is True
    assert first["lease"]["status"] == "cleaned"
    assert "lease_owner_fence" not in first["lease"].get("metadata", {})

    second = app.performance_workflows.add_candidate(
        session_id,
        sql,
        strategy="index",
        idempotency_key="second-index-candidate",
    )
    second_index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["status", "id"],
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[_profile(45, index_name=second_index_name) for _ in range(3)],
            *[_profile(100) for _ in range(3)],
        ]
    )
    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        second["candidate_id"],
        sql,
        "dbo",
        "Items",
        ["status", "id"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "second-index",
    )

    assert result["classification"] == "improved"
    assert result["durable_state"] == "screening"
    assert result["session_continues"] is True
    assert app.tuning_sessions.get_candidate(first_id).state == "regressed"
    assert app.tuning_sessions.get_candidate(second["candidate_id"]).state == "screening"
    assert app._drop_test_index.await_count == 2


@pytest.mark.asyncio
async def test_index_benchmark_is_inconclusive_without_expected_index_use(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    _index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["status"],
        include_columns=["id"],
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[_profile(50) for _ in range(3)],
            *[_profile(100) for _ in range(3)],
        ]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        ["id"],
        None,
        False,
        "screening",
        True,
        True,
        30,
        "missing-index-plan",
    )

    assert result["classification"] == "inconclusive"
    assert result["equivalence"][0]["plan_used_expected_index"] is False


@pytest.mark.asyncio
async def test_index_benchmark_rejects_unstable_result_fingerprints(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["status"],
        include_columns=["id"],
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[_profile(50, index_name=index_name, result_value=2) for _ in range(3)],
            *[_profile(100) for _ in range(3)],
        ]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        ["id"],
        None,
        False,
        "screening",
        True,
        True,
        30,
        "unstable-result-fingerprint",
    )

    assert result["classification"] == "equivalence_failed"
    assert result["equivalence"][0]["status"] == "mismatch"
    assert result["equivalence"][0]["plan_used_expected_index"] is True


@pytest.mark.asyncio
async def test_partial_aba_samples_never_prove_equivalence(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("synthetic create failure")
    )
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]
    _install_catalog(
        monkeypatch,
        key_columns=["status"],
        sequence=("empty", "empty", "empty"),
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(100) for _ in range(3)]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "partial-aba",
    )

    assert result["classification"] == "inconclusive"
    assert result["equivalence"][0]["status"] == "inconclusive"
    assert result["equivalence"][0]["proven_for_parameter_case"] is False


@pytest.mark.asyncio
async def test_failed_create_never_drops_unowned_same_name_index(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("synthetic create failure")
    )
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]
    _install_catalog(
        monkeypatch,
        key_columns=["status"],
        index_type="CLUSTERED",
        sequence=("empty", "present"),
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(100) for _ in range(3)]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "failed-create-unowned",
    )

    assert result["classification"] == "inconclusive"
    assert result["lease"]["status"] == "cleanup_required"
    app._drop_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_index_cleanup_failure_is_durable_and_blocks_silent_success(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(side_effect=RuntimeError("synthetic cleanup"))  # type: ignore[method-assign]
    index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["status"],
        sequence=("empty", "present", "present"),
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[_profile(50, index_name=index_name) for _ in range(3)],
        ]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "cleanup-failure",
    )

    assert result["classification"] == "cleanup_required"
    assert result["lease"]["status"] == "cleanup_required"
    assert app.tuning_sessions.get_candidate(candidate_id).state == "cleanup_required"

    replay = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "cleanup-failure",
    )
    assert replay["recovered_from_durable_evidence"] is True
    assert replay["classification"] == "cleanup_required"
    assert replay["reservation_status"] == "completed"
    app._create_test_index.assert_awaited_once()
    app._drop_test_index.assert_awaited_once()

    with pytest.raises(IdempotencyConflictError, match="different.*idempotency key"):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            None,
            None,
            False,
            "screening",
            True,
            True,
            30,
            "different-cleanup-request",
        )


@pytest.mark.asyncio
async def test_index_candidate_timeout_is_inconclusive_without_create(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[]),
    )
    app.plans.profile_query = AsyncMock(side_effect=TimeoutError("synthetic timeout"))  # type: ignore[method-assign]

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "timeout-index",
    )

    assert result["classification"] == "inconclusive"
    assert result["lease"]["status"] == "create_failed"
    assert result["executions"] == 1
    assert app.tuning_sessions.get_candidate(candidate_id).executions == 1
    reservation_row = app.performance_store._connection.execute(
        "SELECT reservation_id FROM execution_reservations WHERE candidate_id = ?",
        (candidate_id,),
    ).fetchone()
    assert reservation_row is not None
    reservation = app.performance_store.get_execution_reservation(
        reservation_row["reservation_id"]
    )
    assert reservation["attempt_count"] == 9
    assert reservation["dispatched_attempt_count"] == 1
    assert app.performance_store.execution_budget_usage(session_id)["remaining"] == 79
    app._create_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_session_is_rejected_before_index_reservation(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    _set_session_deadline(
        app,
        session_id,
        (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[]),
    )
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(TimeoutError, match="time budget has expired"):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            None,
            None,
            False,
            "screening",
            True,
            True,
            30,
            "expired-before-dispatch",
        )

    reservation_row = app.performance_store._connection.execute(
        "SELECT reservation_id FROM execution_reservations WHERE candidate_id = ?",
        (candidate_id,),
    ).fetchone()
    assert reservation_row is None
    assert app.performance_store.execution_budget_usage(session_id)["remaining"] == 80
    app._create_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_no_deadline_infinity_path_releases_pre_dispatch_reservation(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    _set_session_deadline(app, session_id, None)
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        app.performance_store,
        "create_index_lease",
        Mock(side_effect=RuntimeError("synthetic lease failure")),
    )
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="synthetic lease failure"):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            None,
            None,
            False,
            "screening",
            True,
            True,
            30,
            "legacy-no-deadline",
        )

    reservation_row = app.performance_store._connection.execute(
        "SELECT reservation_id FROM execution_reservations WHERE candidate_id = ?",
        (candidate_id,),
    ).fetchone()
    assert reservation_row is not None
    reservation = app.performance_store.get_execution_reservation(
        reservation_row["reservation_id"]
    )
    assert reservation["status"] == "released"
    assert reservation["dispatched_attempt_count"] == 0
    assert app.performance_store.execution_budget_usage(session_id)["remaining"] == 80
    app._create_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelled_index_benchmark_charges_uncertain_dispatched_execution(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[]),
    )
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(100), asyncio.CancelledError()]
    )

    with pytest.raises(asyncio.CancelledError):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            None,
            None,
            False,
            "screening",
            True,
            True,
            30,
            "cancelled-index",
        )

    assert app.tuning_sessions.get_candidate(candidate_id).executions == 2
    reservation_row = app.performance_store._connection.execute(
        "SELECT reservation_id FROM execution_reservations WHERE candidate_id = ?",
        (candidate_id,),
    ).fetchone()
    assert reservation_row is not None
    reservation = app.performance_store.get_execution_reservation(
        reservation_row["reservation_id"]
    )
    assert reservation["status"] == "completed"
    assert reservation["dispatched_attempt_count"] == 2
    assert app.performance_store.execution_budget_usage(session_id)["remaining"] == 78
    app._create_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_uncertain_index_create_is_reconciled_and_cleaned(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(100) for _ in range(3)]
    )
    app._create_test_index = AsyncMock(  # type: ignore[method-assign]
        side_effect=TimeoutError("synthetic uncertain create")
    )
    _install_catalog(
        monkeypatch,
        key_columns=["status"],
        sequence=("empty", "present", "present", "empty"),
    )
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "uncertain-index-create",
    )

    assert result["classification"] == "inconclusive"
    assert result["lease"]["status"] == "cleanup_required"
    app._drop_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_successful_create_never_adopts_an_unmarked_structural_match(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(100) for _ in range(3)]
    )
    app._create_test_index = AsyncMock(  # type: ignore[method-assign]
        return_value={"status": "completed"}
    )
    _install_catalog(
        monkeypatch,
        key_columns=["status"],
        sequence=("empty", "present"),
    )

    async def unmarked_rows(
        _database_name: str,
        sql_text: str,
        **_kwargs: object,
    ) -> list[dict[str, object]]:
        if "owner_marker" in sql_text and "definition_marker" in sql_text:
            return [
                {
                    "index_id": 2,
                    "owner_marker": None,
                    "definition_marker": None,
                }
            ]
        return [{"schema_name": "dbo", "table_name": "Items"}]

    app.executor.fetch_all = AsyncMock(side_effect=unmarked_rows)  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "unmarked-successful-create",
    )

    assert result["classification"] == "inconclusive"
    assert result["lease"]["status"] == "cleanup_required"
    app._drop_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_index_candidate_measures_all_recorded_parameter_buckets(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = @p"
    parameter_cases: list[dict[str, object]] = [
        {"name": "common", "values": {"p": 1}, "types": {"p": "int"}},
        {"name": "rare", "values": {"p": 999999}, "types": {"p": "int"}},
        {"name": "NULL", "values": {"p": None}, "types": {"p": "int"}},
        {"name": "boundary", "values": {"p": 0}, "types": {"p": "int"}},
    ]
    session_id, candidate_id = _candidate(
        app,
        sql,
        parameter_cases=parameter_cases,
    )

    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["status"],
        include_columns=["id"],
    )
    app.plans.profile_parameterized_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(12)],
            *[_profile(50, index_name=index_name) for _ in range(12)],
            *[_profile(100) for _ in range(12)],
        ]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        ["id"],
        None,
        False,
        "screening",
        True,
        True,
        30,
        "parameterized-index",
        parameter_cases,
    )

    assert result["classification"] == "improved"
    assert result["executions"] == 36
    assert [
        item["parameter_case"] for item in result["parameter_results"]
    ] == ["common", "rare", "NULL", "boundary"]
    assert [
        receipt["name"] for receipt in result["parameter_case_receipts"]
    ] == ["common", "rare", "NULL", "boundary"]
    assert all(
        receipt["values_persisted"] is False
        for receipt in result["parameter_case_receipts"]
    )
    evidence = app.performance_store.get_evidence(result["evidence_id"])
    assert evidence.metadata["parameter_case_receipts"] == (
        result["parameter_case_receipts"]
    )
    assert "999999" not in evidence.to_json()
    replay = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        ["id"],
        None,
        False,
        "screening",
        True,
        True,
        30,
        "parameterized-index",
        parameter_cases,
    )
    assert replay["parameter_case_receipts"] == result["parameter_case_receipts"]
    assert replay["classification"] == result["classification"]
    assert replay["evidence_id"] == result["evidence_id"]
    assert app.tuning_sessions.get_candidate(candidate_id).parameter_cases == 4


@pytest.mark.asyncio
async def test_index_parameter_case_mismatch_is_value_free_and_actionable(
    server_config_factory,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = @p"
    registered = [
        {
            "name": "common",
            "values": {"p": 1},
            "types": {"p": "int"},
            "weight": 1.0,
        }
    ]
    session_id, candidate_id = _candidate(
        app,
        sql,
        parameter_cases=registered,
    )
    changed = [
        {
            "name": "common",
            "values": {"p": 999999},
            "types": {"p": "int"},
            "weight": 1.0,
        }
    ]

    with pytest.raises(ValueError) as error:
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            None,
            None,
            False,
            "screening",
            True,
            True,
            30,
            "index-parameter-mismatch",
            changed,
        )

    message = str(error.value)
    assert "Parameter case index 0" in message
    assert "fingerprint" in message
    assert "999999" not in message


@pytest.mark.asyncio
async def test_index_cleanup_uses_marker_fingerprint_when_catalog_fingerprint_differs(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["status"],
        data_space_name="PRIMARY",
        sequence=("empty", "present", "present", "empty"),
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[_profile(50, index_name=index_name) for _ in range(3)],
            *[_profile(100) for _ in range(3)],
        ]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "marker-fingerprint-cleanup",
    )

    lease = result["lease"]
    marker_fingerprint = result["index_definition_fingerprint"]
    assert lease["object_fingerprint"] == marker_fingerprint
    assert lease["metadata"]["marker_definition_fingerprint"] == marker_fingerprint
    assert lease["metadata"]["observed_definition_fingerprint"] != marker_fingerprint
    drop_call = app._drop_test_index.await_args
    assert drop_call is not None
    assert drop_call.kwargs["expected_definition_fingerprint"] == marker_fingerprint


@pytest.mark.asyncio
async def test_expired_index_lease_is_reconciled_on_sandbox_startup(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    existing = ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name="IX_Testing_recovery",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("status"),),
    )
    app.performance_store.create_index_lease(
        lease_id="lease-expired",
        database_fingerprint=database_fingerprint("appdb", app.config.server),
        session_id="session-recovery",
        candidate_id="candidate-recovery",
        index_name="IX_Testing_recovery",
        object_fingerprint=existing.definition_fingerprint,
        expires_at_utc=(datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        metadata={
            "target_schema": "dbo",
            "target_table": "Items",
            "fingerprint_provenance": "observed_existing_index",
            "lease_owner_fence": "index-lease-recovery-owner-1234",
        },
    )
    app.performance_store.update_index_lease(
        "lease-expired",
        status="active",
        object_fingerprint=existing.definition_fingerprint,
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[existing]),
    )
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]

    summary = await app._cleanup_expired_index_leases()

    assert summary == {"examined": 1, "cleaned": 1, "cleanup_required": 0}
    assert app.performance_store.get_index_lease("lease-expired")["status"] == "cleaned"
    app._drop_test_index.assert_awaited_once()
    assert app._drop_test_index.await_args.kwargs["ownership_proof"] == (
        "index-lease-recovery-owner-1234"
    )


@pytest.mark.asyncio
async def test_expired_unobserved_index_lease_fails_closed(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    candidate = IndexCandidate(
        schema="dbo",
        table="Items",
        key_columns=("status",),
        include_columns=(),
        index_name="IX_Testing_unobserved",
    )
    existing = ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name="IX_Testing_unobserved",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("status"),),
    )
    app.performance_store.create_index_lease(
        lease_id="lease-unobserved",
        database_fingerprint=database_fingerprint("appdb", app.config.server),
        session_id="session-recovery",
        candidate_id="candidate-recovery",
        index_name=existing.name,
        object_fingerprint=candidate.definition_fingerprint,
        expires_at_utc=(datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        metadata={
            "target_schema": "dbo",
            "target_table": "Items",
            "fingerprint_provenance": "candidate_intent",
        },
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[existing]),
    )
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]

    summary = await app._cleanup_expired_index_leases()

    assert summary == {"examined": 1, "cleaned": 0, "cleanup_required": 1}
    assert (
        app.performance_store.get_index_lease("lease-unobserved")["status"]
        == "cleanup_required"
    )
    app._drop_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_pre_dispatch_pending_create_without_visible_index_is_clean(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    app.performance_store.create_index_lease(
        lease_id="lease-pending-no-index",
        database_fingerprint=database_fingerprint("appdb", app.config.server),
        session_id="session-recovery",
        candidate_id="candidate-recovery",
        index_name="IX_Testing_pending_no_index",
        object_fingerprint="candidate-fingerprint",
        expires_at_utc=(datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        metadata={
            "target_schema": "dbo",
            "target_table": "Items",
            "fingerprint_provenance": "candidate_intent",
            "lease_owner_fence": "index-lease-pre-dispatch-owner-1234",
            "create_dispatch_state": "pre_dispatch",
        },
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[]),
    )
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]

    summary = await app._cleanup_expired_index_leases()

    assert summary == {"examined": 1, "cleaned": 1, "cleanup_required": 0}
    assert (
        app.performance_store.get_index_lease("lease-pending-no-index")["status"]
        == "cleaned"
    )
    app._drop_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_dispatched_pending_create_without_visible_index_remains_uncertain(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    app.performance_store.create_index_lease(
        lease_id="lease-dispatched-no-index",
        database_fingerprint=database_fingerprint("appdb", app.config.server),
        session_id="session-recovery",
        candidate_id="candidate-recovery",
        index_name="IX_Testing_dispatched_no_index",
        object_fingerprint="candidate-fingerprint",
        expires_at_utc=(datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        metadata={
            "target_schema": "dbo",
            "target_table": "Items",
            "fingerprint_provenance": "candidate_intent",
            "lease_owner_fence": "index-lease-dispatched-owner-1234",
            "create_dispatch_state": "dispatched",
            "create_dispatched_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[]),
    )
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]

    summary = await app._cleanup_expired_index_leases()

    assert summary == {"examined": 1, "cleaned": 0, "cleanup_required": 1}
    assert (
        app.performance_store.get_index_lease("lease-dispatched-no-index")["status"]
        == "cleanup_required"
    )
    app._drop_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_pending_create_requires_marker_and_structural_ownership_proof(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    candidate = IndexCandidate(
        schema="dbo",
        table="Items",
        key_columns=("status",),
        include_columns=(),
        index_name="IX_Testing_pending_owned",
    )
    existing = ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name=candidate.index_name or "",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("status"),),
        data_space_name="PRIMARY",
    )
    owner = "index-lease-pending-owned-1234"
    app.performance_store.create_index_lease(
        lease_id="lease-pending-owned",
        database_fingerprint=database_fingerprint("appdb", app.config.server),
        session_id="session-recovery",
        candidate_id="candidate-recovery",
        index_name=existing.name,
        object_fingerprint=candidate.definition_fingerprint,
        expires_at_utc=(datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        metadata={
            "target_schema": "dbo",
            "target_table": "Items",
            "key_columns": ["status"],
            "include_columns": [],
            "filter_definition": None,
            "is_unique": False,
            "marker_definition_fingerprint": candidate.definition_fingerprint,
            "lease_owner_fence": owner,
            "create_dispatch_state": "dispatched",
        },
        owner_reference=owner,
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[existing]),
    )
    app.executor.fetch_all = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            {
                "index_id": existing.index_id,
                "owner_marker": owner,
                "definition_marker": candidate.definition_fingerprint,
            }
        ]
    )
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]

    summary = await app._cleanup_expired_index_leases()

    assert summary == {"examined": 1, "cleaned": 1, "cleanup_required": 0}
    assert app.performance_store.get_index_lease("lease-pending-owned")["status"] == (
        "cleaned"
    )
    app._drop_test_index.assert_awaited_once()
    drop_call = app._drop_test_index.await_args
    assert drop_call is not None
    assert drop_call.kwargs["expected_index_id"] == 2
    assert drop_call.kwargs["expected_definition_fingerprint"] == (
        candidate.definition_fingerprint
    )


@pytest.mark.asyncio
async def test_expired_pending_create_never_adopts_unmarked_structural_match(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    candidate = IndexCandidate(
        schema="dbo",
        table="Items",
        key_columns=("status",),
        include_columns=(),
        index_name="IX_Testing_pending_unmarked",
    )
    existing = ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name=candidate.index_name or "",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("status"),),
    )
    owner = "index-lease-pending-unmarked-1234"
    app.performance_store.create_index_lease(
        lease_id="lease-pending-unmarked",
        database_fingerprint=database_fingerprint("appdb", app.config.server),
        session_id="session-recovery",
        candidate_id="candidate-recovery",
        index_name=existing.name,
        object_fingerprint=candidate.definition_fingerprint,
        expires_at_utc=(datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        metadata={
            "target_schema": "dbo",
            "target_table": "Items",
            "key_columns": ["status"],
            "include_columns": [],
            "filter_definition": None,
            "is_unique": False,
            "marker_definition_fingerprint": candidate.definition_fingerprint,
            "lease_owner_fence": owner,
            "create_dispatch_state": "dispatched",
        },
        owner_reference=owner,
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[existing]),
    )
    app.executor.fetch_all = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            {
                "index_id": existing.index_id,
                "owner_marker": None,
                "definition_marker": None,
            }
        ]
    )
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]

    summary = await app._cleanup_expired_index_leases()

    assert summary == {"examined": 1, "cleaned": 0, "cleanup_required": 1}
    assert app.performance_store.get_index_lease("lease-pending-unmarked")["status"] == (
        "cleanup_required"
    )
    app._drop_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_long_lived_sandbox_reconciles_expired_lease_before_new_benchmark(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    existing = ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name="IX_Testing_recovery_runtime",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("status"),),
    )
    app.performance_store.create_index_lease(
        lease_id="lease-expired-runtime",
        database_fingerprint=database_fingerprint("appdb", app.config.server),
        session_id="session-recovery",
        candidate_id="candidate-recovery",
        index_name=existing.name,
        object_fingerprint=existing.definition_fingerprint,
        expires_at_utc=(datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        metadata={
            "target_schema": "dbo",
            "target_table": "Items",
            "fingerprint_provenance": "observed_existing_index",
        },
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[existing]),
    )
    app._drop_test_index = AsyncMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("synthetic cleanup failure")
    )
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "dbo",
        "Items",
        ["status"],
        ["id"],
        None,
        False,
        "screening",
        True,
        True,
        30,
        "new-index-after-expiry",
    )

    assert result["classification"] == "cleanup_required"
    assert result["failure_code"] == "expired_index_cleanup_required"
    assert result["executions"] == 0
    app._create_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_lease_with_live_session_is_fenced_from_cleanup(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app.tuning_sessions.start_screening(session_id)
    existing = ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name="IX_Testing_live_lease",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("status"),),
    )
    app.performance_store.create_index_lease(
        lease_id="lease-live-session",
        database_fingerprint=database_fingerprint("appdb", app.config.server),
        session_id=session_id,
        candidate_id=candidate_id,
        index_name=existing.name,
        object_fingerprint=existing.definition_fingerprint,
        expires_at_utc=(datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        metadata={
            "target_schema": "dbo",
            "target_table": "Items",
            "fingerprint_provenance": "observed_existing_index",
        },
    )
    app.performance_store.update_index_lease(
        "lease-live-session",
        status="active",
        object_fingerprint=existing.definition_fingerprint,
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(return_value=[existing]),
    )
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]

    summary = await app._cleanup_expired_index_leases()

    assert summary == {"examined": 0, "cleaned": 0, "cleanup_required": 0}
    assert app.performance_store.get_index_lease("lease-live-session")["status"] == "active"
    app._drop_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_index_benchmark_resolves_catalog_spelling_before_ddl(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["status"],
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[_profile(50, index_name=index_name) for _ in range(3)],
            *[_profile(100) for _ in range(3)],
        ]
    )

    result = await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        "DBO",
        "items",
        ["status"],
        None,
        None,
        False,
        "screening",
        True,
        True,
        30,
        "canonical-index-target",
    )

    assert result["classification"] == "improved"
    app._create_test_index.assert_awaited_once()
    assert app._create_test_index.await_args.args[1:3] == ("dbo", "Items")


@pytest.mark.asyncio
async def test_index_cleanup_cancellation_is_durable_and_re_raised(
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = _candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(side_effect=asyncio.CancelledError())  # type: ignore[method-assign]
    index_name, _ = _install_catalog(
        monkeypatch,
        key_columns=["status"],
        sequence=("empty", "present", "present"),
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[_profile(50, index_name=index_name) for _ in range(3)],
        ]
    )

    with pytest.raises(asyncio.CancelledError):
        await app._benchmark_index_candidate(
            "appdb",
            session_id,
            candidate_id,
            sql,
            "dbo",
            "Items",
            ["status"],
            None,
            None,
            False,
            "screening",
            True,
            True,
            30,
            "cancelled-cleanup",
        )

    lease = app.performance_store.list_open_index_leases()[0]
    assert lease["status"] == "cleanup_required"
    assert app.tuning_sessions.get_candidate(candidate_id).state == "cleanup_required"
