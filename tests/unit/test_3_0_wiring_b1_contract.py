"""B1 wiring: the error contract, the response finishers and the status tools.

Each test calls a registered tool through the tool manager, so it fails when
server.py stops calling the module function it covers.
"""

from __future__ import annotations

import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.tuning_sessions import InvalidTransitionError
from azure_sql_mcp.tuning_sessions import TuningBudgetExceeded
from tests.unit.test_server import make_config
from tests.unit.test_server import make_read_policy


async def _call(app: AzureSqlMcpApplication, name: str, arguments: dict[str, Any]) -> Any:
    return await app.mcp._tool_manager.call_tool(name, arguments)


async def _error(app: AzureSqlMcpApplication, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    with pytest.raises(ToolError) as error:
        await _call(app, name, arguments)
    text = str(error.value)
    return json.loads(text[text.index("{") :])


@pytest.fixture
def app(tmp_path: Path) -> AzureSqlMcpApplication:
    return AzureSqlMcpApplication(make_config(tmp_path))


@pytest.mark.asyncio
async def test_unknown_session_id_is_not_found_and_not_a_database_failure(
    app: AzureSqlMcpApplication,
) -> None:
    # A wrong id used to reach the agent as sql_execution_failed with the
    # message '[REDACTED]', so it blamed the database and retried.
    payload = await _error(app, "get_tuning_session", {"session_id": "ts-40613-missing"})

    assert payload["code"] == "not_found"
    assert "ts-40613-missing" not in json.dumps(payload)
    assert "REDACTED" not in payload["message"]
    diagnostic = payload["details"]["failure_diagnostic"]
    assert diagnostic["diagnostic_code"] == "not_found"
    assert diagnostic["retryable"] is False
    assert "transient" not in diagnostic


@pytest.mark.asyncio
async def test_session_state_and_budget_failures_carry_their_own_codes(
    app: AzureSqlMcpApplication,
) -> None:
    # The agent must tell "wrong state" from "budget spent" without parsing text.
    app._get_tuning_session = AsyncMock(  # type: ignore[method-assign]
        side_effect=InvalidTransitionError("Session status completed is not in allowed statuses.")
    )
    state = await _error(app, "get_tuning_session", {"session_id": "s"})
    app._get_tuning_session = AsyncMock(  # type: ignore[method-assign]
        side_effect=TuningBudgetExceeded("Execution limit 80 would be exceeded.")
    )
    budget = await _error(app, "get_tuning_session", {"session_id": "s"})

    assert state["code"] == "invalid_session_state"
    assert state["message"] == "Session status completed is not in allowed statuses."
    assert "get_tuning_session" in state["details"]["next_action"]
    assert budget["code"] == "session_budget_exceeded"
    assert budget["details"]["failure_diagnostic"]["retryable"] is False


@pytest.mark.asyncio
async def test_compile_error_names_the_identifier_to_the_client_but_not_the_log(
    app: AzureSqlMcpApplication,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # "Invalid column name '[REDACTED]'" cannot be fixed; the name is schema, not data.
    app.database_policy = make_read_policy("appdb")
    app.executor.fetch_all = AsyncMock(
        side_effect=RuntimeError("Invalid column name 'Scroe'. Conversion failed for 'secret-row-value'.")
    )
    with caplog.at_level(logging.ERROR, logger="azure_sql_mcp.server"):
        payload = await _error(app, "execute_sql", {"sql": "SELECT Scroe FROM dbo.Posts"})

    assert "'Scroe'" in payload["message"]
    assert "secret-row-value" not in payload["message"]
    logged = [record for record in caplog.records if record.getMessage() == "Tool failed"]
    assert logged and "Scroe" not in logged[0].error  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_next_tools_outside_the_profile_are_listed_as_unavailable(
    tmp_path: Path,
) -> None:
    # A triage agent told to call start_tuning_session wastes a call on a
    # tool its profile does not register.
    app = AzureSqlMcpApplication(replace(make_config(tmp_path), profile=McpProfile.TRIAGE))
    app.database_diagnosis.diagnose = AsyncMock(
        return_value={
            "verdict": "pressure",
            "findings": [
                {"code": "x", "next_tools": ["get_wait_stats", "start_tuning_session"]}
            ],
        }
    )

    result = await _call(app, "diagnose_database", {})

    finding = result["findings"][0]
    assert finding["next_tools"] == ["get_wait_stats"]
    assert finding["unavailable_next_tools"] == [
        {"tool": "start_tuning_session", "available_in_profiles": ["optimizer", "sandbox"]}
    ]


@pytest.mark.asyncio
async def test_data_detail_masks_literals_in_every_tool_response(tmp_path: Path) -> None:
    # The switch is a promise to the data owner; it must not depend on which tool ran.
    app = AzureSqlMcpApplication(replace(make_config(tmp_path), data_detail="mask_literals"))
    app.database_policy = make_read_policy("appdb")
    app.executor.fetch_all = AsyncMock(return_value=[{"n": 1}])

    result = await _call(
        app, "execute_sql", {"sql": "SELECT 1 AS n FROM dbo.Users WHERE Email = 'a@example.test'"}
    )

    assert result["data_detail"] == "mask_literals"
    assert "a@example.test" not in result["normalized_sql"]
    assert "dbo.Users" in result["normalized_sql"]


@pytest.mark.asyncio
async def test_data_detail_withholds_plan_xml_read_as_a_resource(tmp_path: Path) -> None:
    app = AzureSqlMcpApplication(replace(make_config(tmp_path), data_detail="metadata_only"))
    artifact = app.artifacts.put_text(
        kind="showplan-xml",
        text="<ShowPlanXML><Stmt StatementText=\"SELECT 'x'\"/></ShowPlanXML>",
        mime_type="application/xml",
        metadata={},
    )

    contents = await app.mcp.read_resource(artifact["resource_uri"])

    text = "".join(str(item.content) for item in contents)
    assert "StatementText" not in text
    assert "metadata_only" in text


@pytest.mark.asyncio
async def test_runtime_status_reports_counts_and_names_only_on_request(
    app: AzureSqlMcpApplication,
) -> None:
    # 100+ tool names in the first call of every session cost context for nothing.
    status = await _call(app, "check_runtime_status", {})
    named = await _call(app, "check_runtime_status", {"include_tool_names": True})

    assert "tool_names" not in status
    assert status["tool_count"] == sum(status["tool_counts_by_group"].values())
    assert status["tool_counts_by_group"]["core"] > 10
    assert status["data_detail"] == "full"
    assert status["query_cancellation"] == app.executor.cancellation_report()
    assert named["tool_count"] == len(named["tool_names"])
    assert status["tool_schema_fingerprint"] == named["tool_schema_fingerprint"]


def _stub_capabilities(app: AzureSqlMcpApplication, platform: dict[str, Any]) -> None:
    operators = [{"node_id": index, "physical_op": "Clustered Index Scan"} for index in range(40)]
    plan_detail = {
        "statement_count": 1,
        "operators": operators,
        "top_operators": operators[:5],
        "statements": [{"statement_text": "SELECT TOP 1 * FROM [dbo].[Posts]"}],
        "warnings": [],
    }
    app.capabilities.check = AsyncMock(
        return_value={
            "database_name": "appdb",
            "checks": {
                "connectivity": {"available": True, "detail": {"database_name": "appdb"}},
                "query_store": {"available": True, "detail": {"enabled": True}},
                "estimated_plan": {"available": True, "detail": dict(plan_detail)},
                "actual_plan": {"available": True, "detail": dict(plan_detail)},
            },
        }
    )
    app.platform_capabilities.get_summary = AsyncMock(return_value=platform)
    app.query_store.get_replica_dimension = AsyncMock(
        return_value={"database_name": "appdb", "available": False, "replicas": []}
    )


@pytest.mark.asyncio
async def test_capabilities_do_not_return_the_probe_plan(app: AzureSqlMcpApplication) -> None:
    # The probe proves the permission; its operators say nothing about the user's query.
    _stub_capabilities(app, {"platform_supported": True, "platform": "azure_sql_database"})

    # Stream S13b-2: the default is now the summary; the probe details are in full.
    result = await _call(app, "check_capabilities", {"detail": "full"})

    for name in ("estimated_plan", "actual_plan"):
        check = result["checks"][name]
        assert check["available"] is True
        assert "operators" not in json.dumps(check)
        assert check["detail"] == {"probe": "plan_returned", "statement_count": 1}
    assert result["checks"]["connectivity"]["detail"] == {"database_name": "appdb"}
    assert result["query_store_replicas"]["data"] == {
        "database_name": "appdb",
        "available": False,
        "replicas": [],
    }
    assert result["result_status"] == "ok"
    assert len(json.dumps(result)) < 4000


@pytest.mark.asyncio
async def test_capabilities_stamp_not_supported_for_another_engine(
    app: AzureSqlMcpApplication,
) -> None:
    # A SQL Server box answers every probe; without the stamp the agent
    # applies Azure limits and advice to it.
    _stub_capabilities(
        app,
        {"platform_supported": False, "engine_edition": 3, "platform": "sql_server_enterprise"},
    )

    result = await _call(app, "check_capabilities", {})

    assert result["result_status"] == "not_supported"
    assert "EngineEdition 3" in result["result_status_reason"]


def test_pool_is_built_with_the_policy_read_intents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Without this a database marked read_only in policy still connects to the primary.
    policy = DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {"environment": "test", "allow_read": True, "application_intent": "read_only"}
            },
        }
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.load_database_policy_or_deny", lambda _path: policy
    )

    app = AzureSqlMcpApplication(make_config(tmp_path))

    assert app.pool.application_intent("appdb") == "read_only"


@pytest.mark.asyncio
async def test_execute_sql_runs_caller_sql_tagged_and_inside_the_rollback_envelope(
    app: AzureSqlMcpApplication,
) -> None:
    # The validator is the first gate; the envelope is the second if it is ever wrong.
    app.database_policy = make_read_policy("appdb")
    app.executor.fetch_all = AsyncMock(return_value=[{"n": 1}])

    await _call(app, "execute_sql", {"sql": "SELECT 1 AS n"})

    kwargs = app.executor.fetch_all.await_args.kwargs
    assert kwargs["sql_origin"] == "caller_supplied"
    assert kwargs["read_only_envelope"] is True
