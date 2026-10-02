"""Server wiring of the incident log: the dispatch hook, swallow sites, lifecycle."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import threading
import time
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from mcp import types
from mcp.server.fastmcp.exceptions import ToolError
from mcp.shared.exceptions import McpError
from mcp.shared.memory import create_connected_server_and_client_session
from mssql_python.exceptions import ProgrammingError

from azure_sql_mcp import incident_log
from azure_sql_mcp.config import IncidentSettings
from azure_sql_mcp.config import ServerConfig
from azure_sql_mcp.connection_pool import ConnectionPool
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.incident_log import IncidentLog
from azure_sql_mcp.incident_log import render_markdown
from azure_sql_mcp.incident_log import summarize_backlog
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.server import async_main
from azure_sql_mcp.param_binding import ParameterBindingService
from tests.unit.test_database_diagnosis import _service as diagnosis_service
from tests.unit.test_health import build_service as health_service
from tests.unit.test_index_benchmark_workflow import _app as index_app
from tests.unit.test_index_benchmark_workflow import _candidate as index_candidate
from tests.unit.test_index_benchmark_workflow import _install_catalog as install_index_catalog
from tests.unit.test_index_benchmark_workflow import _profile as index_profile
from tests.unit.test_incident_log import fake_context
from tests.unit.test_incident_log import read_records
from tests.unit.test_performance_workflows import _service as workflow_service
from tests.unit.test_performance_workflows import _start as workflow_start
from tests.unit.test_server import make_config


def sentinel_config(tmp_path: Path, *, enabled: bool = True, **overrides) -> ServerConfig:
    return replace(
        make_config(tmp_path),
        server="sentinelsrv.database.windows.net",
        default_database="SentinelDb",
        allowed_databases=("SentinelDb",),
        performance_state_dir=str(tmp_path / "state"),
        incident=IncidentSettings(enabled=enabled),
        **overrides,
    )


def sentinel_app(tmp_path: Path, **kwargs) -> AzureSqlMcpApplication:
    app = AzureSqlMcpApplication(sentinel_config(tmp_path, **kwargs))
    app.database_policy = DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {"SentinelDb": {"environment": "test", "allow_read": True}},
        }
    )
    return app


def records(app: AzureSqlMcpApplication) -> list[dict]:
    assert app.incidents.directory is not None
    return read_records(app.incidents.directory)


def missing_table() -> ProgrammingError:
    return ProgrammingError(
        "Base table or view not found",
        "[Microsoft][SQL Server]Invalid object name 'SENTINEL_T'. (208)",
    )


# --- the dispatch hook ---------------------------------------------------------


@pytest.mark.asyncio
async def test_payloads_and_errors_are_identical_with_the_log_on_and_off(
    tmp_path: Path,
) -> None:
    outcomes = []
    for name, enabled in (("on", True), ("off", False)):
        app = sentinel_app(tmp_path / name, enabled=enabled)
        app.executor.fetch_all = AsyncMock(
            side_effect=[[{"n": 1}], [{"n": 1}], missing_table()]
        )
        manager = app.mcp._tool_manager
        seen: list[object] = [
            await manager.call_tool("execute_sql", {"sql": "SELECT 1 AS n"}),
            await manager.call_tool(
                "execute_sql", {"sql": "SELECT 1 AS n"}, convert_result=True
            ),
        ]
        for tool, arguments in (
            ("execute_sql", {"sql": "SELECT n FROM SENTINEL_T WHERE n = 'SENTINEL_V'"}),
            ("execute_sql", {"sql": 7}),
            ("list_databases", {"unexpected": "SENTINEL_V"}),
            ("query_sentineldb_sales", {}),
        ):
            with pytest.raises(ToolError) as error:
                await manager.call_tool(tool, arguments)
            seen.append(
                (
                    str(error.value),
                    type(error.value.__cause__),
                    type(error.value.__context__),
                )
            )
        outcomes.append(seen)
        app.incidents.close()

    assert outcomes[0] == outcomes[1]
    assert (tmp_path / "on" / "state" / "incidents").is_dir()
    assert not (tmp_path / "off" / "state" / "incidents").exists()


@pytest.mark.asyncio
async def test_tool_failure_record_keeps_codes_and_drops_names_and_values(
    tmp_path: Path,
) -> None:
    app = sentinel_app(tmp_path)
    app.executor.fetch_all = AsyncMock(side_effect=missing_table())

    with pytest.raises(ToolError):
        await app.mcp._tool_manager.call_tool(
            "execute_sql",
            {"sql": "SELECT n FROM SENTINEL_T WHERE n = 'SENTINEL_V'"},
            context=fake_context(),
        )

    [record] = records(app)
    assert (record["kind"], record["tool"]) == ("tool_error", "execute_sql")
    assert (record["category"], record["priority"]) == ("caller_error", "P4")
    assert record["error"]["native_error_code"] == 208
    assert record["call"]["argument_keys"] == ["sql"]
    assert record["call"]["budget_s"] == app.config.tool_timeout_seconds
    assert record["session"]["client"] == {"name": "copilot-cli", "version": "1.2.3"}
    assert "sentinel" not in json.dumps(record).lower()


@pytest.mark.asyncio
async def test_rejected_sql_literals_and_comments_reach_no_record_or_export(
    tmp_path: Path,
) -> None:
    app = sentinel_app(tmp_path)
    for sql in (
        "SELECT name FROM dbo.Employees OPTION ('Zebulon Quartermaine 078-05-1120' RECOMPILE)",
        "SELECT a FROM CROSS /* step -> <"
        + "~" * 110
        + "> Zebulon Quartermaine salary 250000 */ x",
    ):
        with pytest.raises(ToolError):
            await app.mcp._tool_manager.call_tool(
                "execute_sql", {"sql": sql}, context=fake_context()
            )

    option, _comment = records(app)
    assert (option["category"], option["priority"]) == ("caller_error", "P4")
    backlog = app.incidents.backlog(
        performance_db=None, min_priority="P4", include_summaries=True
    )
    for text in (
        json.dumps(records(app)),
        render_markdown(backlog),
        json.dumps(summarize_backlog(backlog)),
    ):
        for private in ("zebulon", "quartermaine", "salary", "250000", "1120", "~~"):
            assert private not in text.lower()


@pytest.mark.asyncio
async def test_unknown_tools_are_drift_and_only_package_tool_names_are_kept(
    tmp_path: Path,
) -> None:
    app = sentinel_app(tmp_path)

    for name in ("query_sentineldb_sales", "kill_session"):
        with pytest.raises(ToolError, match="Unknown tool"):
            await app.mcp._tool_manager.call_tool(name, {})

    hallucinated, pruned = records(app)
    assert (hallucinated["category"], hallucinated["priority"]) == ("contract_drift", "P3")
    assert hallucinated["error"]["code"] == "unknown_tool"
    # A profile-pruned package tool is a real drift signal; any other name is
    # agent text that may hold a database or table name.
    assert (hallucinated["tool"], pruned["tool"]) == ("unregistered", "kill_session")
    assert "sentinel" not in json.dumps(hallucinated).lower()


@pytest.mark.asyncio
async def test_timeout_is_recorded_with_the_tool_budget(tmp_path: Path) -> None:
    app = sentinel_app(tmp_path, tool_timeout_seconds=0.05)

    async def never_returns(*_args, **_kwargs):
        await asyncio.sleep(5)

    app.executor.fetch_all = never_returns

    with pytest.raises(ToolError, match="timed out"):
        await app.mcp._tool_manager.call_tool("execute_sql", {"sql": "SELECT 1 AS n"})

    [record] = records(app)
    assert (record["kind"], record["category"], record["priority"]) == (
        "tool_timeout",
        "timeout",
        "P3",
    )
    assert record["call"]["budget_s"] == 0.05


@pytest.mark.asyncio
async def test_only_a_client_cancel_notification_is_tagged_client(tmp_path: Path) -> None:
    app = sentinel_app(tmp_path)
    app.incidents.slow_seconds = 0  # every cancel here is a long one
    started = asyncio.Event()
    request_ids: list[object] = []

    async def wait_forever(*_args, **_kwargs):
        request_ids.append(app.mcp.get_context().request_context.request_id)
        started.set()
        await asyncio.sleep(30)

    app.executor.fetch_all = wait_forever
    arguments = {"sql": "SELECT 1 AS n"}
    async with create_connected_server_and_client_session(app.mcp) as client:
        call = asyncio.create_task(client.call_tool("execute_sql", arguments))
        await started.wait()
        await client.send_notification(
            types.ClientNotification(
                types.CancelledNotification(
                    params=types.CancelledNotificationParams(requestId=request_ids[0])
                )
            )
        )
        with pytest.raises(McpError, match="Request cancelled"):
            await call
    # The transport stops under a running call: the server cancels it, not the client.
    started.clear()
    async with create_connected_server_and_client_session(app.mcp) as client:
        call = asyncio.create_task(client.call_tool("execute_sql", arguments))
        await started.wait()
        call.cancel()

    cancels = [record for record in records(app) if record["kind"] == "tool_cancelled"]
    assert [(record["cause"], record["priority"]) for record in cancels] == [
        ("client", "P2"),
        ("unknown", "P4"),
    ]


@pytest.mark.asyncio
async def test_unavailable_result_is_returned_unchanged_and_recorded_as_degraded(
    tmp_path: Path,
) -> None:
    app = sentinel_app(tmp_path)
    reason = "VIEW DATABASE STATE permission was denied in SentinelDb."
    app.version_store.get_version_store_stats = AsyncMock(
        return_value={"result_status": "unavailable", "result_status_reason": reason}
    )

    result = await app.mcp._tool_manager.call_tool("get_version_store_stats", {})

    assert result == {"result_status": "unavailable", "result_status_reason": reason}
    [record] = records(app)
    assert (record["kind"], record["priority"]) == ("degraded_result", "P4")
    assert "Sentinel" not in json.dumps(record)


@pytest.mark.asyncio
async def test_incident_log_failures_never_change_a_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = sentinel_app(tmp_path)
    manager = app.mcp._tool_manager
    # One failure inside end(), one inside begin().
    for target, name in ((incident_log, "describe_exception"), (IncidentLog, "_session_info")):
        app.executor.fetch_all = AsyncMock(side_effect=[[{"n": 1}], missing_table()])
        with monkeypatch.context() as patched:
            patched.setattr(target, name, Mock(side_effect=RuntimeError))
            result = await manager.call_tool(
                "execute_sql", {"sql": "SELECT 1 AS n"}, context=fake_context()
            )
            with pytest.raises(ToolError) as error:
                await manager.call_tool(
                    "execute_sql", {"sql": "SELECT 1 AS n"}, context=fake_context()
                )

        assert result["rows"] == [{"n": 1}]
        assert str(error.value).startswith(
            'Error executing tool execute_sql: {"code":"tool_error"'
        )


@pytest.mark.asyncio
async def test_third_identical_failure_in_one_session_records_one_agent_loop(
    tmp_path: Path,
) -> None:
    app = sentinel_app(tmp_path)
    app.executor.fetch_all = AsyncMock(side_effect=[missing_table() for _ in range(4)])
    context = fake_context()

    for _ in range(4):
        with pytest.raises(ToolError):
            await app.mcp._tool_manager.call_tool(
                "execute_sql", {"sql": "SELECT n FROM SENTINEL_T"}, context=context
            )

    loops = [record for record in records(app) if record["kind"] == "agent_loop"]
    assert len(loops) == 1
    assert loops[0]["loop"]["repeat_count"] == 3


# --- swallowed partial failures -------------------------------------------------


@pytest.mark.asyncio
async def test_optional_payload_failure_inside_an_ok_result_is_recorded(
    tmp_path: Path,
) -> None:
    app = sentinel_app(tmp_path)
    app.capabilities.check = AsyncMock(return_value={"query_store": "on"})
    app.platform_capabilities.get_summary = AsyncMock(
        side_effect=TypeError("'NoneType' object is not subscriptable")
    )

    result = await app.mcp._tool_manager.call_tool("check_capabilities", {})

    assert result["azure_sql_database"]["ok"] is False
    [record] = records(app)
    assert (record["kind"], record["site"], record["tool"]) == (
        "swallowed_exception",
        "server.optional_payload",
        "check_capabilities",
    )
    assert (record["category"], record["priority"]) == ("product_bug", "P1")


@pytest.mark.asyncio
async def test_optional_evidence_failure_is_recorded(tmp_path: Path) -> None:
    app = sentinel_app(tmp_path)

    async def broken() -> None:
        raise AttributeError("'NoneType' object has no attribute 'get'")

    call = app.incidents.begin("collect_performance_evidence", {})
    evidence = await app._optional_evidence(broken)
    app.incidents.end(call, result={"result_status": "ok"})

    assert evidence["available"] is False
    [record] = records(app)
    assert (record["site"], record["tool"]) == (
        "server.optional_evidence",
        "collect_performance_evidence",
    )


def missing_column() -> ProgrammingError:
    return ProgrammingError(
        "Column not found", "[Microsoft][SQL Server]Invalid column name 'SENTINEL_C'. (207)"
    )


@pytest.mark.asyncio
async def test_service_gap_and_unavailable_failures_are_recorded(tmp_path: Path) -> None:
    app = sentinel_app(tmp_path)
    manager = app.mcp._tool_manager
    app.executor.fetch_all = AsyncMock(
        side_effect=[
            [{"persistent_version_store_size_kb": 1}],
            missing_column(),
            TypeError("'NoneType' object is not subscriptable"),
            missing_column(),
            TypeError("'NoneType' object is not subscriptable"),
        ]
    )

    partial = await manager.call_tool("get_version_store_stats", {}, context=fake_context())
    unavailable = await manager.call_tool("get_version_store_stats", {}, context=fake_context())

    assert (partial["result_status"], len(partial["gaps"])) == ("ok", 3)
    assert unavailable["result_status"] == "unavailable"
    swallowed = [record for record in records(app) if record["kind"] == "swallowed_exception"]
    assert [(r["site"], r["error"]["class"], r["priority"]) for r in swallowed] == [
        ("version_store.optional", "ProgrammingError", "P2"),
        ("version_store.optional", "TypeError", "P1"),
        ("version_store.optional", "ProgrammingError", "P2"),
        ("version_store.pvs_stats", "TypeError", "P1"),
    ]
    assert {r["tool"] for r in swallowed} == {"get_version_store_stats"}
    assert "SENTINEL" not in json.dumps(swallowed)


@pytest.mark.asyncio
async def test_workload_index_review_source_failures_are_recorded(tmp_path: Path) -> None:
    app = sentinel_app(tmp_path)
    app.executor.fetch_all = AsyncMock(side_effect=TypeError("'NoneType' object is not iterable"))

    result = await app.mcp._tool_manager.call_tool("review_workload_indexes", {})

    assert result["result_status"] == "unavailable"
    sites = [r["site"] for r in records(app) if r["kind"] == "swallowed_exception"]
    assert sites == [
        "workload_index_advisor.query_store_options",
        "workload_index_advisor.existing_indexes",
    ]


@pytest.mark.asyncio
async def test_diagnosis_source_failure_is_recorded_per_source(tmp_path: Path) -> None:
    app = sentinel_app(tmp_path)
    service = diagnosis_service()
    service.wait_stats.get_wait_stats = AsyncMock(side_effect=KeyError("wait_type"))

    call = app.incidents.begin("diagnose_database", {})
    result = await service.diagnose("SentinelDb")
    app.incidents.end(call, result=result)

    assert result["sources"]["waits"] == "unavailable"
    [record] = records(app)
    assert (record["site"], record["category"], record["priority"]) == (
        "database_diagnosis.waits",
        "product_bug",
        "P1",
    )


@pytest.mark.asyncio
async def test_learning_terminal_link_failure_is_recorded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = sentinel_app(tmp_path)
    assert app.learning_service is not None
    monkeypatch.setattr(
        app.learning_service.store,
        "get_decision",
        Mock(side_effect=sqlite3.OperationalError("database is locked")),
    )

    call = app.incidents.begin("benchmark_tuning_candidate", {})
    result = await app._attach_learning_terminal_link(
        "benchmark_tuning_candidate", "SentinelDb", "decision-x", {"status": "ok"}
    )
    app.incidents.end(call, result=result)

    assert result["learning_link_status"] == "failed"
    [record] = records(app)
    assert (record["site"], record["category"], record["priority"]) == (
        "server.learning_terminal_link",
        "state_store",
        "P1",
    )


# A product bug in these swallowed paths reads as "nothing proves out" unless
# it is recorded: one TypeError per site must become one P1 record.
BUG = TypeError("'NoneType' object is not subscriptable")


async def swallowed_sites(tmp_path: Path, tool: str, action) -> list[tuple[str, str, str]]:
    log = IncidentLog(tmp_path / "incidents", server_version="2.6.0")
    call = log.begin(tool, {})
    await action()
    log.end(call, result={"result_status": "ok"})
    return sorted(
        (record["site"], record["error"]["class"], record["priority"])
        for record in read_records(tmp_path / "incidents")
        if record["kind"] == "swallowed_exception"
    )


@pytest.mark.asyncio
async def test_evidence_collector_failures_and_timeouts_are_recorded(tmp_path: Path) -> None:
    service, _store, _plans, _executor = workflow_service()
    service.collector_timeout_seconds = 0.05
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")

    async def hang() -> None:
        await asyncio.sleep(5)

    async def collect() -> None:
        result = await service.collect_case_evidence(
            case.case_id,
            "appdb",
            "SELECT id FROM dbo.Items",
            {"query_store": AsyncMock(side_effect=BUG), "waits": hang},
            window_minutes=15,
            execute_query=False,
            idempotency_key="evidence-1",
        )
        assert {section["available"] for section in result["sections"].values()} == {False}

    assert await swallowed_sites(tmp_path, "collect_performance_evidence", collect) == [
        ("performance_workflows.evidence_collector", "TimeoutError", "P3"),
        ("performance_workflows.evidence_collector", "TypeError", "P1"),
    ]


@pytest.mark.asyncio
async def test_snapshot_comparison_failure_is_recorded(tmp_path: Path) -> None:
    service, _store, _plans, executor = workflow_service()
    executor.execute_session_exactly_once = AsyncMock(side_effect=BUG)  # type: ignore[method-assign]

    async def compare() -> None:
        result = await service.compare_query_results(
            "appdb", "SELECT id FROM dbo.Items", "SELECT id FROM dbo.Items AS candidate"
        )
        assert result["status"] == "inconclusive"

    assert await swallowed_sites(tmp_path, "compare_query_results", compare) == [
        ("performance_workflows.snapshot_comparison", "TypeError", "P1")
    ]


@pytest.mark.asyncio
async def test_benchmark_failure_receipt_is_recorded(tmp_path: Path) -> None:
    service, _store, plans, _executor = workflow_service()
    session_id, candidate_id = workflow_start(service)
    plans.profile_query = AsyncMock(side_effect=BUG)  # type: ignore[method-assign]

    async def benchmark() -> None:
        result = await service.benchmark_candidate(
            session_id,
            candidate_id,
            "appdb",
            "SELECT id FROM dbo.Items",
            "SELECT id FROM dbo.Items AS candidate",
            runs_override=3,
            idempotency_key="benchmark-1",
        )
        assert result["failure_code"] == "TypeError"

    assert await swallowed_sites(tmp_path, "benchmark_tuning_candidate", benchmark) == [
        ("performance_workflows.benchmark", "TypeError", "P1")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("profiles", "site"),
    [
        ([BUG], "server.index_benchmark"),
        (
            [*[index_profile(100) for _ in range(3)], *[index_profile(50) for _ in range(3)], BUG],
            "server.index_benchmark_after",
        ),
    ],
)
async def test_index_benchmark_measurement_failures_are_recorded(
    tmp_path: Path,
    server_config_factory,
    monkeypatch: pytest.MonkeyPatch,
    profiles,
    site,
) -> None:
    app = index_app(server_config_factory)
    sql = "SELECT id FROM dbo.Items WHERE status = 1"
    session_id, candidate_id = index_candidate(app, sql)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    install_index_catalog(monkeypatch, key_columns=["status"])
    app.plans.profile_query = AsyncMock(side_effect=profiles)  # type: ignore[method-assign]

    async def benchmark() -> None:
        result = await app._benchmark_index_candidate(
            "appdb", session_id, candidate_id, sql, "dbo", "Items", ["status"],
            None, None, False, "screening", True, True, 30, f"key-{site}",
        )
        assert result["classification"] == "inconclusive"

    assert await swallowed_sites(tmp_path, "benchmark_index_candidate", benchmark) == [
        (site, "TypeError", "P1")
    ]


@pytest.mark.asyncio
async def test_unresolved_metadata_reference_failure_is_recorded(tmp_path: Path) -> None:
    app = sentinel_app(tmp_path / "app")
    app.introspection.get_table_stats = AsyncMock(return_value={})  # type: ignore[method-assign]
    app.introspection.get_object_details = AsyncMock(side_effect=BUG)  # type: ignore[method-assign]

    async def inventory() -> None:
        result = await app._metadata_inventory("SentinelDb", [{"schema": "dbo", "table": "T"}])
        assert len(result["unresolved"]) == 1

    assert await swallowed_sites(tmp_path, "analyze_query_plan", inventory) == [
        ("server.metadata_inventory", "TypeError", "P1")
    ]


@pytest.mark.asyncio
async def test_health_check_and_source_failures_are_recorded(tmp_path: Path) -> None:
    service, _executor, _query_store = health_service(
        errors=[("SELECT 1", BUG), ("sys.dm_user_db_resource_governance", BUG)]
    )

    async def checks() -> None:
        failed = await service._run_check("index", "appdb", AsyncMock(side_effect=BUG))
        assert failed["status"] == "warning"
        assert (await service._fetch_optional_rows("appdb", "SELECT 1"))[0] == []
        assert await service._fetch_governance_limits("appdb") == {}

    assert await swallowed_sites(tmp_path, "analyze_db_health", checks) == [
        ("health.check", "TypeError", "P1"),
        ("health.governance_limits", "TypeError", "P1"),
        ("health.optional_rows", "TypeError", "P1"),
    ]


@pytest.mark.asyncio
async def test_parameter_binding_lookup_failures_are_recorded(tmp_path: Path) -> None:
    type_row = {
        "table_name": "Orders",
        "schema_name": "dbo",
        "data_type": "int",
        "max_length": 4,
        "precision": 10,
        "scale": 0,
        "stats_id": 1,
    }

    class FailingExecutor:
        def __init__(self, failing: str) -> None:
            self.failing = failing

        async def fetch_all(self, _database_name, query, params=None):
            if self.failing in query:
                raise BUG
            return [type_row] if "sys.stats_columns" in query else []

    async def bind() -> None:
        for failing in ("sys.stats_columns", "dm_db_stats_histogram"):
            service = ParameterBindingService(FailingExecutor(failing))  # type: ignore[arg-type]
            await service.bind_parameters("appdb", "SELECT * FROM Orders WHERE Id = @Id")

    assert await swallowed_sites(tmp_path, "detect_parameter_sniffing", bind) == [
        ("param_binding.column_type", "TypeError", "P1"),
        ("param_binding.histogram", "TypeError", "P1"),
    ]


def test_pool_circuit_breaker_and_leaked_connections_are_recorded(tmp_path: Path) -> None:
    config = sentinel_config(tmp_path)
    log = IncidentLog.from_config(config)
    pool = ConnectionPool(config, Mock())

    call = log.begin("get_connection_pool_stats", {})
    for _ in range(5):
        pool._record_failure("SentinelDb")
    pool._leases[1] = ("SentinelDb", time.monotonic() - 3600, "stack")
    leaked = pool.check_leaked_connections()
    log.end(call, result={"result_status": "ok"})

    assert len(leaked) == 1
    opened, leak = read_records(log.directory)
    assert (opened["site"], opened["category"], opened["priority"]) == (
        "connection_pool.circuit_open",
        "environment",
        "P3",
    )
    assert (leak["site"], leak["category"]) == (
        "connection_pool.leaked_connection",
        "product_bug",
    )
    assert "Sentinel" not in json.dumps([opened, leak])


def test_the_watchdog_records_each_leaked_connection_once_without_a_stats_call(
    tmp_path: Path,
) -> None:
    app = sentinel_app(tmp_path)
    app.pool._leases[1] = ("SentinelDb", time.monotonic() - 3600, "stack")
    app.pool._leases[2] = ("SentinelDb", time.monotonic(), "stack")  # still young
    def leaks() -> list[dict]:
        return [r for r in records(app) if r.get("site") == "connection_pool.leaked_connection"]

    app.incidents.start()
    try:
        for _ in range(2):  # on its own thread, like the watchdog
            ticker = threading.Thread(target=app.incidents.tick)
            ticker.start()
            ticker.join()
        assert [(r["kind"], r["category"]) for r in leaks()] == [("condition", "product_bug")]
        assert len(app.pool.check_leaked_connections()) == 1  # the stats tool sees it too
    finally:
        app.incidents.close()

    assert len(leaks()) == 1
    assert "Sentinel" not in json.dumps(leaks())


# --- process lifecycle ----------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_failure_is_recorded_and_reraised(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AZURE_SQL_INCIDENT_DIR", str(tmp_path / "incidents"))
    monkeypatch.delenv("AZURE_SQL_INCIDENT_LOG", raising=False)
    failure = ValueError("AZURE_SQL_SERVER is required.")
    monkeypatch.setattr(
        "azure_sql_mcp.server.load_server_config", Mock(side_effect=failure)
    )

    with pytest.raises(ValueError) as raised:
        await async_main([])

    assert raised.value is failure
    [record] = read_records(tmp_path / "incidents")
    assert (record["kind"], record["phase"]) == ("startup_failure", "startup")


@pytest.mark.asyncio
async def test_startup_failure_follows_the_incident_flags_and_never_writes_when_off(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    for name in list(os.environ):
        if name.startswith("AZURE_SQL_"):
            monkeypatch.delenv(name)
    custom = tmp_path / "custom"
    memory = ["--azure-sql-performance-state-dir", ":memory:"]

    with pytest.raises(ValueError, match="AZURE_SQL_SERVER"):
        await async_main(
            ["--azure-sql-incident-log", "off", "--azure-sql-incident-dir", str(custom), *memory]
        )
    with pytest.raises(SystemExit):
        await async_main(["--help"])
    assert list(home.iterdir()) == [] and not custom.exists()

    with pytest.raises(ValueError, match="AZURE_SQL_SERVER"):
        await async_main(["--azure-sql-incident-dir", str(custom), *memory])
    [record] = read_records(custom)
    assert record["kind"] == "startup_failure"
    assert list(home.iterdir()) == []


@pytest.mark.asyncio
async def test_cancelling_the_server_tags_in_flight_calls_as_shutdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = sentinel_app(tmp_path)
    started = asyncio.Event()

    async def wait_forever(*_args, **_kwargs):
        started.set()
        await asyncio.sleep(30)

    async def transport() -> None:
        # Like the MCP server, each request runs in its own task.
        await asyncio.create_task(
            app.mcp._tool_manager.call_tool(
                "execute_sql", {"sql": "SELECT 1 AS n"}, context=fake_context()
            )
        )

    app.executor.fetch_all = wait_forever
    monkeypatch.setattr(app.mcp, "run_stdio_async", transport)
    monkeypatch.setattr(app.pool, "close_all", AsyncMock())
    server = asyncio.create_task(app.run())
    await started.wait()
    server.cancel()  # what asyncio.run does on Ctrl+C or a host SIGINT

    with pytest.raises(asyncio.CancelledError):
        await server

    [record] = [record for record in records(app) if record["kind"] == "tool_cancelled"]
    assert (record["cause"], record["priority"]) == ("shutdown", "P4")


@pytest.mark.asyncio
async def test_run_starts_the_log_before_cleanup_and_closes_it_first(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = sentinel_app(tmp_path)
    order: list[str] = []

    def spy(owner: object, name: str, label: str) -> None:
        real = getattr(owner, name)

        def wrapper(*args, **kwargs):
            order.append(label)
            return real(*args, **kwargs)

        monkeypatch.setattr(owner, name, wrapper)

    spy(app.incidents, "start", "incidents.start")
    spy(app.incidents, "mark_shutting_down", "incidents.mark_shutting_down")
    spy(app.incidents, "close", "incidents.close")
    spy(app.performance_store, "close", "performance_store.close")
    monkeypatch.setattr(
        app,
        "_cleanup_expired_index_leases",
        AsyncMock(side_effect=lambda: order.append("cleanup") or {"examined": 0}),
    )
    monkeypatch.setattr(
        app.mcp,
        "run_stdio_async",
        AsyncMock(side_effect=lambda: order.append("transport")),
    )
    monkeypatch.setattr(app.pool, "close_all", AsyncMock())

    await app.run()

    assert order == [
        "incidents.start",
        "cleanup",
        "transport",
        "incidents.mark_shutting_down",
        "incidents.close",
        "performance_store.close",
    ]
    assert not any(
        thread.name == "azure-sql-mcp-incident-watchdog" for thread in threading.enumerate()
    )


@pytest.mark.asyncio
async def test_server_exit_by_exception_is_recorded_and_reraised(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = sentinel_app(tmp_path)
    failure = BrokenPipeError("stdout closed")
    monkeypatch.setattr(app.mcp, "run_stdio_async", AsyncMock(side_effect=failure))
    monkeypatch.setattr(app.pool, "close_all", AsyncMock())

    with pytest.raises(BrokenPipeError) as raised:
        await app.run()

    assert raised.value is failure
    [record] = records(app)
    assert (record["kind"], record["phase"]) == ("server_exit", "run")
