"""S13a F1, F3, F7: test index builds on columnstore and partitioned tables,
the lease after a failed build, and a failure result that names its cause.

Observed on a 112M-row partitioned clustered columnstore table: the online
build was refused by the engine, the agent saw only ``sql_execution_failed``,
and the lease blocked every later test index for an hour.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock

import pytest
from mssql_python.exceptions import OperationalError
from mssql_python.exceptions import ProgrammingError

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.connection import AdminBatchOutcomeUnknownError
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import parse_candidate_key
from azure_sql_mcp.index_optimizer import IndexCandidate
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.server import generated_test_index_name
from tests.unit.test_index_benchmark_workflow import _app
from tests.unit.test_index_benchmark_workflow import _candidate
from tests.unit.test_index_benchmark_workflow import _install_catalog
from tests.unit.test_index_benchmark_workflow import _profile

SQL = "SELECT id FROM dbo.Items"
ONLINE_REFUSED = (
    "create statement failed because the operation cannot be performed online on a "
    "table with a columnstore index. Perform the operation without specifying the "
    "ONLINE option or drop (or disable) the columnstore index before performing the "
    "operation using the ONLINE option."
)
PARTITIONED = {
    "partition_scheme_name": "ps_Items",
    "partition_function_name": "pf_Items",
    "partitioning_column": "CreatedDateKey",
}


def _sandbox_app(server_config_factory, **policy_fields: Any) -> AzureSqlMcpApplication:
    app = _app(server_config_factory)
    policy = DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "sandbox",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "allow_test_indexes": True,
                    "max_benchmark_executions": 80,
                    **policy_fields,
                }
            },
        }
    )
    app.database_policy = policy
    app.performance_workflows.database_policy = policy
    return app


def _table_facts(app: AzureSqlMcpApplication, **facts: Any) -> None:
    """Make the table read of the index workflow return these catalog facts."""

    inner = app.executor.fetch_all.side_effect

    async def fetch(database_name: str, sql: str, **kwargs: Any) -> list[dict[str, Any]]:
        rows = await inner(database_name, sql, **kwargs)
        if "has_clustered_columnstore_index" in sql:
            return [{**rows[0], "has_clustered_columnstore_index": 0, **facts}]
        return rows

    app.executor.fetch_all = AsyncMock(side_effect=fetch)  # type: ignore[method-assign]


def _engine_failure(text: str) -> Exception:
    cause = ProgrammingError(
        driver_error="Syntax error or access violation",
        ddbc_error=f"[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]{text}",
    )
    try:
        raise AdminBatchOutcomeUnknownError(str(cause)) from cause
    except AdminBatchOutcomeUnknownError as wrapped:
        return wrapped


async def _benchmark(
    app: AzureSqlMcpApplication,
    session_id: str,
    candidate_id: str,
    *,
    online: bool = True,
    key: str = "s13a-key",
) -> dict[str, Any]:
    return await app._benchmark_index_candidate(
        "appdb", session_id, candidate_id, SQL, "dbo", "Items", ["id"], None, None, False,
        "screening", online, True, 30, key, None,
    )


def _full_run(index_name: str) -> list[Any]:
    return [
        *[_profile(100) for _ in range(3)],
        *[_profile(50, index_name=index_name) for _ in range(3)],
        *[_profile(100) for _ in range(3)],
    ]


# --- F1: columnstore table -----------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("online", [True, False])
async def test_columnstore_table_is_refused_before_any_execution_when_offline_is_not_allowed(
    server_config_factory, monkeypatch: pytest.MonkeyPatch, online: bool
) -> None:
    # The engine refuses the online build and the policy refuses the offline
    # one: the agent must learn both rules before a baseline run is spent.
    app = _sandbox_app(server_config_factory)
    _table_facts(app, has_clustered_columnstore_index=1)
    session_id, candidate_id = _candidate(app, SQL)
    _, catalog = _install_catalog(monkeypatch, key_columns=["id"])
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(PermissionError) as refusal:
        await _benchmark(app, session_id, candidate_id, online=online)

    message = str(refusal.value)
    assert "columnstore" in message and "online" in message
    assert "allow_offline_test_index" in message
    assert "dbo.Items" in message
    app.plans.profile_query.assert_not_awaited()
    app._create_test_index.assert_not_awaited()
    assert app.performance_store.list_open_index_leases() == []
    # Nothing was bound: the same candidate works once the policy allows it.
    assert app.tuning_sessions.get_candidate(candidate_id).is_terminal is False


@pytest.mark.asyncio
async def test_columnstore_table_builds_offline_and_says_so_when_policy_allows(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _sandbox_app(server_config_factory, allow_offline_test_index=True)
    _table_facts(app, has_clustered_columnstore_index=1)
    session_id, candidate_id = _candidate(app, SQL)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(monkeypatch, key_columns=["id"])
    app.plans.profile_query = AsyncMock(side_effect=_full_run(index_name))  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id, online=True)

    create = app._create_test_index.await_args
    assert create.args[6] is False  # online
    assert "ONLINE" not in result["index_ddl"]
    assert result["offline_build"]["reason"] == "clustered_columnstore_table"
    assert result["offline_build"]["tables"] == ["dbo.Items"]
    assert "blocks all writes" in result["offline_build"]["note"]
    assert result["indexes"][0]["online"] is False
    assert result["lease"]["status"] == "cleaned"


@pytest.mark.asyncio
async def test_rowstore_table_still_builds_online_with_no_offline_note(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _sandbox_app(server_config_factory, allow_offline_test_index=True)
    _table_facts(app)
    session_id, candidate_id = _candidate(app, SQL)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(monkeypatch, key_columns=["id"])
    app.plans.profile_query = AsyncMock(side_effect=_full_run(index_name))  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    assert app._create_test_index.await_args.args[6] is True
    assert "ONLINE = ON" in result["index_ddl"]
    assert "offline_build" not in result
    assert result["indexes"][0]["online"] is True


@pytest.mark.asyncio
async def test_preview_of_a_test_index_on_a_columnstore_table_never_shows_online(
    server_config_factory,
) -> None:
    allowed = _sandbox_app(server_config_factory, allow_offline_test_index=True)
    refused = _sandbox_app(server_config_factory)
    columnstore = await allowed._resolve_canonical_table_identity("appdb", "dbo", "Items")
    columnstore = type(columnstore)(schema="dbo", table="Items", has_clustered_columnstore_index=True)

    preview = await allowed._create_test_index(
        "appdb", "dbo", "Items", "IX_Testing_s13a", ["id"], None, True,
        dry_run=True, table_facts=columnstore,
    )

    assert "ONLINE" not in preview["sql_preview"]
    assert preview["online"] is False
    assert preview["offline_build"]["reason"] == "clustered_columnstore_table"
    with pytest.raises(PermissionError, match="allow_offline_test_index"):
        await refused._create_test_index(
            "appdb", "dbo", "Items", "IX_Testing_s13a", ["id"], None, True,
            dry_run=True, table_facts=columnstore,
        )


@pytest.mark.asyncio
async def test_table_read_returns_columnstore_and_partition_facts(server_config_factory) -> None:
    app = _sandbox_app(server_config_factory)
    _table_facts(app, has_clustered_columnstore_index=1, **PARTITIONED)

    table = await app._resolve_canonical_table_identity("appdb", "DBO", "items")

    assert (table.schema, table.table) == ("dbo", "Items")
    assert table.has_clustered_columnstore_index is True
    assert table.partition_scheme == "ps_Items"
    assert table.partition_function == "pf_Items"
    assert table.partition_column == "CreatedDateKey"
    sql = app.executor.fetch_all.await_args.args[1]
    # Live check: only a clustered columnstore index (type 5) refuses an online build.
    assert "cs.type = 5" in sql


# --- F7: partitioned table and explicit failure cause ----------------------------


def _aligned_index(index_name: str) -> ExistingIndex:
    # What the catalog holds after CREATE INDEX on a partitioned table: the
    # index is on the table's partition scheme, partitioned by its column.
    return ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name=index_name,
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("id"),),
        include_columns=(),
        filter_definition=None,
        is_unique=False,
        data_space_name="ps_Items",
        data_space_type="PARTITION_SCHEME",
        partition_scheme_name="ps_Items",
        partition_function_name="pf_Items",
        partition_columns=("CreatedDateKey",),
        partition_compression=((1, "NONE"), (2, "NONE")),
    )


@pytest.mark.asyncio
async def test_test_index_on_a_partitioned_table_is_verified_and_measured(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The built index is aligned with the table's partitions. Comparing it with
    # an unpartitioned request failed every build after the whole build time.
    app = _sandbox_app(server_config_factory)
    _table_facts(app, **PARTITIONED)
    session_id, candidate_id = _candidate(app, SQL)
    index_name = generated_test_index_name(
        "IX_Testing_",
        IndexCandidate(
            schema="dbo",
            table="Items",
            key_columns=("id",),
            include_columns=(),
            data_space_name="ps_Items",
            partition_columns=("CreatedDateKey",),
            partition_scheme_name="ps_Items",
            partition_function_name="pf_Items",
        ),
    )
    built = _aligned_index(index_name)
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(side_effect=[[], [built], [built], []]),
    )
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(side_effect=_full_run(index_name))  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    assert result["indexes"][0]["built"] is True
    assert result["indexes"][0]["index_name"] == index_name
    assert "ON [ps_Items]([CreatedDateKey])" in result["index_ddl"]
    assert result["lease"]["status"] == "cleaned"
    assert "failure_diagnostic" not in result
    assert app._create_test_index.await_args.kwargs["table_facts"].partition_scheme == "ps_Items"


@pytest.mark.asyncio
async def test_workflow_create_ddl_is_aligned_with_the_table_partitions(
    server_config_factory,
) -> None:
    app = _sandbox_app(server_config_factory)
    _table_facts(app, **PARTITIONED)
    table = await app._resolve_canonical_table_identity("appdb", "dbo", "Items")

    preview = await app._create_test_index(
        "appdb", "dbo", "Items", "IX_Testing_s13a", ["id"], None, True,
        dry_run=True, table_facts=table,
    )

    assert preview["sql_preview"].rstrip().endswith("ON [ps_Items]([CreatedDateKey]);")


@pytest.mark.asyncio
async def test_a_built_index_that_does_not_match_the_request_is_named_as_the_cause(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _sandbox_app(server_config_factory)
    _table_facts(app)
    session_id, candidate_id = _candidate(app, SQL)
    index_name, _ = _install_catalog(monkeypatch, key_columns=["id"])
    other = ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name=index_name,
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("status"),),
        include_columns=(),
        filter_definition=None,
        is_unique=False,
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(side_effect=[[], [other], []]),
    )
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(side_effect=[_profile(100) for _ in range(3)])  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    diagnostic = result["failure_diagnostic"]
    assert diagnostic["failed_step"] == "verify_created_index"
    assert diagnostic["cause_class"] == "definition_mismatch"
    assert result["indexes"][0]["built"] is False
    assert result["lease"]["status"] == "cleaned"


@pytest.mark.asyncio
async def test_a_build_that_hits_the_statement_timeout_reports_the_limit(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _sandbox_app(server_config_factory)
    _table_facts(app)
    session_id, candidate_id = _candidate(app, SQL)
    timeout = OperationalError(
        driver_error="Timeout expired",
        ddbc_error="[Microsoft][ODBC Driver 18 for SQL Server]Query timeout expired",
    )
    app._create_test_index = AsyncMock(  # type: ignore[method-assign]
        side_effect=AdminBatchOutcomeUnknownError(str(timeout))
    )
    app._create_test_index.side_effect.__cause__ = timeout
    _install_catalog(monkeypatch, key_columns=["id"], sequence=("empty", "empty"))
    app.plans.profile_query = AsyncMock(side_effect=[_profile(100) for _ in range(3)])  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    diagnostic = result["failure_diagnostic"]
    assert diagnostic["failed_step"] == "create_index"
    assert diagnostic["cause_class"] == "timeout"
    assert diagnostic["timeout_seconds"] == app.config.query_timeout_seconds
    assert diagnostic["timeout_setting"] == "AZURE_SQL_QUERY_TIMEOUT_SECONDS"
    # The build may still be running on the server: the lease stays.
    assert result["lease"]["status"] == "cleanup_required"


@pytest.mark.asyncio
async def test_a_lock_wait_abort_is_named_with_its_wait_limits(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _sandbox_app(server_config_factory)
    _table_facts(app)
    session_id, candidate_id = _candidate(app, SQL)
    app._create_test_index = AsyncMock(  # type: ignore[method-assign]
        side_effect=_engine_failure("Lock request time out period exceeded.")
    )
    _install_catalog(monkeypatch, key_columns=["id"], sequence=("empty", "empty"))
    app.plans.profile_query = AsyncMock(side_effect=[_profile(100) for _ in range(3)])  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    diagnostic = result["failure_diagnostic"]
    assert diagnostic["failed_step"] == "create_index"
    assert diagnostic["cause_class"] == "lock_wait_abort"
    assert diagnostic["native_error_code"] == 1222
    assert diagnostic["low_priority_wait_seconds"] == 60
    assert diagnostic["engine_message"] == "Lock request time out period exceeded."


@pytest.mark.asyncio
async def test_a_failed_measurement_names_its_step_without_engine_text(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The measured statement is the caller's SQL: its engine text is not returned.
    app = _sandbox_app(server_config_factory)
    _table_facts(app)
    session_id, candidate_id = _candidate(app, SQL)
    _install_catalog(monkeypatch, key_columns=["id"], sequence=("empty",))
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=_engine_failure("Conversion failed when converting the value 'x'.")
    )

    result = await _benchmark(app, session_id, candidate_id)

    diagnostic = result["failure_diagnostic"]
    assert diagnostic["failed_step"] == "baseline_before"
    assert "engine_message" not in diagnostic
    app._create_test_index.assert_not_awaited()


# --- F2 and F3: a failed build -----------------------------------------------------


@pytest.mark.asyncio
async def test_failed_build_returns_the_engine_message_and_frees_the_lease(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The engine reported the error, so the batch ended; the catalog shows no
    # index. Holding the lease blocked every later test index in the database.
    app = _sandbox_app(server_config_factory)
    _table_facts(app)
    session_id, candidate_id = _candidate(app, SQL)
    app._create_test_index = AsyncMock(side_effect=_engine_failure(ONLINE_REFUSED))  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]
    _install_catalog(monkeypatch, key_columns=["id"], sequence=("empty", "empty", "empty"))
    app.plans.profile_query = AsyncMock(side_effect=[_profile(100) for _ in range(3)])  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    diagnostic = result["failure_diagnostic"]
    assert diagnostic["diagnostic_code"] == "sql_execution_failed"
    assert diagnostic["failed_step"] == "create_index"
    assert diagnostic["cause_class"] == "engine_error"
    assert diagnostic["engine_message"] == ONLINE_REFUSED
    assert result["create_failure"]["index_in_catalog"] is False
    assert result["lease"]["status"] == "create_failed"
    assert app.performance_store.list_open_index_leases() == []
    app._drop_test_index.assert_not_awaited()

    # The next test index in the same database is not refused with lease_conflict.
    next_candidate = app.performance_workflows.add_candidate(session_id, SQL, strategy="index")
    index_name, _ = _install_catalog(monkeypatch, key_columns=["id"])
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(side_effect=_full_run(index_name))  # type: ignore[method-assign]
    second = await _benchmark(app, session_id, next_candidate["candidate_id"], key="s13a-second")
    assert second["lease"]["status"] == "cleaned"


@pytest.mark.asyncio
async def test_failed_build_keeps_cleanup_required_when_the_index_exists(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _sandbox_app(server_config_factory)
    _table_facts(app)
    session_id, candidate_id = _candidate(app, SQL)
    index_name, _ = _install_catalog(monkeypatch, key_columns=["id"])
    unowned = ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name=index_name,
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("status"),),
        include_columns=(),
        filter_definition=None,
        is_unique=False,
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(side_effect=[[], [unowned]]),
    )
    app._create_test_index = AsyncMock(side_effect=_engine_failure(ONLINE_REFUSED))  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(side_effect=[_profile(100) for _ in range(3)])  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    assert result["lease"]["status"] == "cleanup_required"
    assert result["create_failure"]["index_in_catalog"] is True
    app._drop_test_index.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_build_keeps_cleanup_required_when_the_catalog_check_fails(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _sandbox_app(server_config_factory)
    _table_facts(app)
    session_id, candidate_id = _candidate(app, SQL)
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(side_effect=[[], RuntimeError("catalog read failed")]),
    )
    app._create_test_index = AsyncMock(side_effect=_engine_failure(ONLINE_REFUSED))  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(side_effect=[_profile(100) for _ in range(3)])  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    assert result["lease"]["status"] == "cleanup_required"
    assert result["create_failure"]["index_in_catalog"] is None
    assert len(app.performance_store.list_open_index_leases()) == 1


@pytest.mark.asyncio
async def test_a_lost_connection_keeps_the_lease_even_when_the_catalog_shows_no_index(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The server may still be building: an absent index now proves nothing.
    app = _sandbox_app(server_config_factory)
    _table_facts(app)
    session_id, candidate_id = _candidate(app, SQL)
    app._create_test_index = AsyncMock(  # type: ignore[method-assign]
        side_effect=ConnectionError("Communication link failure")
    )
    _install_catalog(monkeypatch, key_columns=["id"], sequence=("empty", "empty"))
    app.plans.profile_query = AsyncMock(side_effect=[_profile(100) for _ in range(3)])  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id)

    assert result["lease"]["status"] == "cleanup_required"
    assert result["failure_diagnostic"]["cause_class"] == "connection_lost"
    assert result["create_failure"]["index_in_catalog"] is False


@pytest.mark.asyncio
async def test_a_failed_drop_returns_the_engine_message_with_cleanup_required(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The DBA who must drop the index by hand needs the engine's reason.
    app = _sandbox_app(server_config_factory)
    _table_facts(app)
    session_id, candidate_id = _candidate(app, SQL)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(  # type: ignore[method-assign]
        side_effect=_engine_failure("Lock request time out period exceeded.")
    )
    index_name, _ = _install_catalog(
        monkeypatch, key_columns=["id"], sequence=("empty", "present", "present")
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[_profile(50, index_name=index_name) for _ in range(3)],
        ]
    )

    result = await _benchmark(app, session_id, candidate_id)

    assert result["classification"] == "cleanup_required"
    assert result["lease"]["status"] == "cleanup_required"
    failure = result["cleanup_failure"]
    assert failure["failed_step"] == "drop_index"
    assert failure["engine_message"] == "Lock request time out period exceeded."
    assert f"DROP INDEX [{index_name}]" in result["rollback_ddl"]


@pytest.mark.asyncio
async def test_create_test_index_tool_reads_the_table_and_previews_an_offline_build(
    server_config_factory,
) -> None:
    app = AzureSqlMcpApplication(server_config_factory(access_mode=AccessMode.UNRESTRICTED))
    app.database_policy = _sandbox_app(
        server_config_factory, allow_offline_test_index=True
    ).database_policy
    app.executor.fetch_all = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            {"schema_name": "dbo", "table_name": "Items", "has_clustered_columnstore_index": 1, **PARTITIONED}
        ]
    )

    response = await app.mcp._tool_manager.call_tool(
        "create_test_index",
        {
            "schema_name": "dbo",
            "table_name": "Items",
            "index_name": "IX_Testing_s13a",
            "key_columns": ["id"],
            "database_name": "appdb",
        },
    )
    preview = json.loads(response) if isinstance(response, str) else response

    assert "ONLINE" not in preview["sql_preview"]
    assert preview["sql_preview"].rstrip().endswith("ON [ps_Items]([CreatedDateKey]);")
    assert preview["offline_build"]["reason"] == "clustered_columnstore_table"
    assert preview["index_name"] == "IX_Testing_s13a"
