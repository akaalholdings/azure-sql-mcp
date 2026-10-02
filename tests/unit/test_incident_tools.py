"""The DB-free incident tools: report_stuck, export_incident_backlog, runtime status."""

from __future__ import annotations

import json
import re
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from azure_sql_mcp.config import TransportConfig
from azure_sql_mcp.config import TransportMode
from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_incident_capture import records
from tests.unit.test_incident_capture import sentinel_app
from tests.unit.test_incident_log import fake_context
from tests.unit.test_server import make_config

INCIDENT_TOOLS = {"report_stuck", "export_incident_backlog"}
SUMMARY_KEYS = {"fingerprint", "title", "priority", "category", "count", "last_seen_utc"}


def report(**overrides) -> dict:
    arguments = {
        "skill": "sql-optimizer",
        "skill_version": "2.4.0",
        "blocker_kind": "repeated_tool_failure",
        "last_tool": "execute_sql",
        "summary": "execute_sql keeps failing on `SELECT * FROM SentinelDb.dbo.Customers` with 208",
    }
    arguments.update(overrides)
    return arguments


def test_incident_tools_are_local_stdio_only(tmp_path: Path) -> None:
    local = AzureSqlMcpApplication(make_config(tmp_path)).mcp._tool_manager._tools
    remote = AzureSqlMcpApplication(
        replace(
            make_config(tmp_path),
            transport=TransportConfig(
                mode=TransportMode.STREAMABLE_HTTP, host="127.0.0.1", port=8000
            ),
            mcp_bearer_token="token",
        )
    ).mcp._tool_manager._tools

    assert INCIDENT_TOOLS <= set(local)
    assert not INCIDENT_TOOLS & set(remote)
    stuck = local["report_stuck"].annotations
    assert (stuck.readOnlyHint, stuck.destructiveHint, stuck.openWorldHint) == (
        False,
        False,
        False,
    )
    export = local["export_incident_backlog"].annotations
    assert (export.readOnlyHint, export.idempotentHint, export.openWorldHint) == (
        True,
        True,
        False,
    )


@pytest.mark.asyncio
async def test_report_stuck_records_a_redacted_report_and_returns_what_was_kept(
    tmp_path: Path,
) -> None:
    app = sentinel_app(tmp_path)
    manager = app.mcp._tool_manager
    context = fake_context()
    arguments = report(related_ids=["case-" + "a" * 32, "SentinelDb.dbo.Customers"])

    result = await manager.call_tool("report_stuck", arguments, context=context)

    assert (result["result_status"], result["recorded"], result["coalesced"]) == (
        "ok",
        True,
        False,
    )
    assert re.fullmatch(r"[0-9a-f]{32}", result["incident_id"])
    assert (result["skill_version_expected"], result["skill_version_mismatch"]) == (
        "2.5.0",
        True,
    )
    assert "Sentinel" not in result["summary"] and "`" not in result["summary"]
    assert "Stop retrying that exact call" in result["next_step"]
    [record] = records(app)
    assert (record["kind"], record["priority"]) == ("agent_report", "P2")
    assert record["report"]["related_ids"] == ["case-" + "a" * 32]
    assert record["session"]["client"] == {"name": "copilot-cli", "version": "1.2.3"}
    assert "Sentinel" not in json.dumps(record)

    again = await manager.call_tool("report_stuck", arguments, context=context)
    assert (again["result_status"], again["recorded"], again["coalesced"]) == (
        "ok",
        False,
        True,
    )
    assert again["incident_id"] == result["incident_id"]
    assert len(records(app)) == 1


@pytest.mark.asyncio
async def test_report_stuck_keeps_only_package_tool_names_and_known_skills(
    tmp_path: Path,
) -> None:
    app = sentinel_app(tmp_path)
    manager = app.mcp._tool_manager

    result = await manager.call_tool(
        "report_stuck",
        report(skill="none", skill_version=None, last_tool="get_sentineldb_rows"),
    )

    assert (result["skill_version_expected"], result["skill_version_mismatch"]) == (
        None,
        False,
    )
    [record] = records(app)
    assert record["report"]["last_tool"] == "unregistered"
    assert "sentinel" not in json.dumps(record).lower()
    with pytest.raises(ToolError, match="invalid_arguments"):
        await manager.call_tool("report_stuck", report(skill="sentineldb-skill"))


@pytest.mark.asyncio
async def test_report_stuck_is_unavailable_not_an_error_when_the_log_is_off(
    tmp_path: Path,
) -> None:
    for app in (
        sentinel_app(tmp_path / "off", enabled=False),
        AzureSqlMcpApplication(make_config(tmp_path / "memory")),
    ):
        result = await app.mcp._tool_manager.call_tool("report_stuck", report())

        assert (result["result_status"], result["recorded"]) == ("unavailable", False)
        assert "incident log is off" in result["result_status_reason"]


@pytest.mark.asyncio
async def test_export_returns_titles_counts_priorities_and_fingerprints_by_default(
    tmp_path: Path,
) -> None:
    app = sentinel_app(tmp_path)
    manager = app.mcp._tool_manager
    app.version_store.get_version_store_stats = AsyncMock(
        side_effect=TypeError("'NoneType' object is not subscriptable")
    )
    for _ in range(2):
        with pytest.raises(ToolError):
            await manager.call_tool("get_version_store_stats", {})
    await manager.call_tool("report_stuck", report(), context=fake_context())

    summary = await manager.call_tool("export_incident_backlog", {})

    assert summary["result_status"] == "ok"
    assert [set(item) for item in summary["items"]] == [SUMMARY_KEYS, SUMMARY_KEYS]
    bug, blocker = summary["items"]
    assert (bug["priority"], bug["category"], bug["count"]) == ("P1", "product_bug", 2)
    assert blocker["category"] == "agent_report"
    assert "summar" not in json.dumps(summary["items"])

    details = await manager.call_tool(
        "export_incident_backlog", {"include_details": True, "min_priority": "P4"}
    )
    reported = next(item for item in details["items"] if item["category"] == "agent_report")
    assert reported["summaries"] and "example" in reported
    assert "Sentinel" not in json.dumps(details)


@pytest.mark.asyncio
async def test_export_is_empty_without_incidents_and_unavailable_when_off(
    tmp_path: Path,
) -> None:
    empty = await sentinel_app(tmp_path / "on").mcp._tool_manager.call_tool(
        "export_incident_backlog", {}
    )
    off = await sentinel_app(tmp_path / "off", enabled=False).mcp._tool_manager.call_tool(
        "export_incident_backlog", {}
    )

    assert (empty["result_status"], empty["items"]) == ("empty", [])
    assert off["result_status"] == "unavailable"
    assert "incident log is off" in off["result_status_reason"]


@pytest.mark.asyncio
async def test_runtime_status_reports_incident_log_state(tmp_path: Path) -> None:
    durable = await sentinel_app(tmp_path).mcp._tool_manager.call_tool(
        "check_runtime_status", {}
    )
    memory = await AzureSqlMcpApplication(make_config(tmp_path)).mcp._tool_manager.call_tool(
        "check_runtime_status", {}
    )

    assert durable["incident_log"] == {
        "enabled": True,
        "reason": None,
        "orphan_detection": "journal",
        "retention_days": 30,
        "slow_seconds": 60,
        "records_written": 0,
        "capped_day": None,
    }
    assert (memory["incident_log"]["enabled"], memory["incident_log"]["reason"]) == (
        False,
        "no_durable_state_dir",
    )
    assert "sentinel" not in json.dumps(durable).lower()


@pytest.mark.asyncio
async def test_report_stuck_stores_the_same_redaction_the_export_applies(
    tmp_path: Path,
) -> None:
    app = sentinel_app(tmp_path)
    expected = {
        "execute_sql fails on select e.ssn, e.salary from employees e join salaries s "
        "on s.emp = e.id": "[sql-like text withheld]",
        "execute_sql keeps failing on exec usp_GetSalary @last = Quartermaine": (
            "[sql-like text withheld]"
        ),
        "get_top_queries shows Zebulon Quartermaine blocking the Patients table": (
            "get_top_queries shows [ident] [ident] blocking the [ident] table"
        ),
        "After two tries tune_query fails on PatientDiagnoses. Azure SQL Query Store "
        "shows nothing": "After two tries tune_query fails on [ident]. Azure SQL Query "
        "Store shows nothing",
    }

    replies = [
        await app.mcp._tool_manager.call_tool(
            "report_stuck", report(summary=summary), context=fake_context()
        )
        for summary in expected
    ]

    assert [reply["summary"] for reply in replies] == list(expected.values())
    stored = [record["report"]["summary"] for record in records(app)]
    assert stored == list(expected.values())
    backlog = app.incidents.backlog(
        performance_db=None, min_priority="P4", include_summaries=True
    )
    assert sorted(text for item in backlog["items"] for text in item["summaries"]) == sorted(
        set(expected.values())
    )


@pytest.mark.asyncio
async def test_report_stuck_keeps_no_free_text_but_the_summary(tmp_path: Path) -> None:
    app = sentinel_app(tmp_path)
    manager = app.mcp._tool_manager
    for field in ("expected", "observed"):
        with pytest.raises(ToolError, match="invalid_arguments"):
            await manager.call_tool(
                "report_stuck",
                report(**{field: "diagnosis rows for Zebulon Quartermaine with HIV status"}),
            )

    await manager.call_tool(
        "report_stuck",
        report(
            related_ids=[
                "zebulon_quartermaine-" + "0" * 32,
                "case-" + "a" * 32,
                "lesson-" + "b" * 32,
                "c" * 32,
            ]
        ),
        context=fake_context(),
    )

    [report_record] = [r for r in records(app) if r["kind"] == "agent_report"]
    assert report_record["report"]["related_ids"] == [
        "case-" + "a" * 32,
        "lesson-" + "b" * 32,
        "c" * 32,
    ]
    assert not {"expected", "observed"} & set(report_record["report"])
    blob = json.dumps(records(app)).lower()
    for private in ("zebulon", "quartermaine", "hiv"):
        assert private not in blob


@pytest.mark.asyncio
async def test_rate_limited_reports_write_nothing_about_the_incident_tools(
    tmp_path: Path,
) -> None:
    app = sentinel_app(tmp_path)
    manager = app.mcp._tool_manager
    context = fake_context()
    tools = sorted(manager._tools)[:25]  # distinct blockers, one session

    results = [
        await manager.call_tool(
            "report_stuck",
            report(last_tool=tool, summary="The same call keeps failing the same way."),
            context=context,
        )
        for tool in tools
    ]

    assert [result["recorded"] for result in results].count(True) == 20
    limited = results[-1]
    assert (limited["result_status"], limited["recorded"], limited["reason"]) == (
        "ok",
        False,
        "rate_limited",
    )
    assert {record["kind"] for record in records(app)} == {"agent_report"}
