"""B4 wiring: one plan parse, catalog partition columns, and a compact explain response."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp import server as server_module
from azure_sql_mcp.artifacts import ExplainPlanArtifact
from azure_sql_mcp.plan_sources import PlanSource
from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_server import make_config

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "showplans"
PARTITIONED = (FIXTURES / "partitioned_scan_no_elimination.xml").read_text()
NO_OPERATORS = (
    '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564">'
    "<BatchSequence><Batch><Statements>"
    '<StmtSimple StatementText="SET NOCOUNT ON" StatementId="1" StatementType="SET ON/OFF" />'
    "</Statements></Batch></BatchSequence></ShowPlanXML>"
)


async def _call(app: AzureSqlMcpApplication, name: str, arguments: dict[str, Any]) -> Any:
    return await app.mcp._tool_manager.call_tool(name, arguments)


@pytest.fixture
def app(tmp_path: Path) -> AzureSqlMcpApplication:
    application = AzureSqlMcpApplication(make_config(tmp_path))
    application.executor.fetch_all = AsyncMock(return_value=[])
    return application


def _artifact(app: AzureSqlMcpApplication, xml: str, *, analyze: bool = True) -> ExplainPlanArtifact:
    summary = app.plans.summarize_showplan_xml(xml)
    if analyze:
        metrics = summary.setdefault("actual_metrics", {})
        metrics["measured_wall_elapsed_ms"] = 220.5
        metrics["measured_wall_elapsed_source"] = "client_wall_clock"
    return ExplainPlanArtifact(database_name="appdb", analyze=analyze, summary=summary, raw_xml=xml)


@pytest.mark.asyncio
async def test_analyze_query_plan_parses_the_showplan_once(
    app: AzureSqlMcpApplication, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Rules and digest each parsed a multi-megabyte plan; four parses on the
    # event loop stalled every other tool call.
    parses: list[int] = []
    real = server_module.read_plan

    def counting(plan: Any, **kwargs: Any) -> dict[str, Any]:
        parses.append(1)
        return real(plan, **kwargs)

    monkeypatch.setattr(server_module, "read_plan", counting)
    monkeypatch.setattr(
        server_module, "analyze_plan", lambda *_a, **_k: pytest.fail("second parse: analyze_plan")
    )
    monkeypatch.setattr(
        server_module, "build_plan_digest", lambda *_a, **_k: pytest.fail("second parse: digest")
    )

    result = await _call(app, "analyze_query_plan", {"plan_xml": PARTITIONED})

    assert parses == [1]
    assert result["digest"]["statement_count"] == 1
    assert result["result_status"] == "ok"


@pytest.mark.asyncio
async def test_partition_finding_is_graded_from_the_catalog_column(
    app: AzureSqlMcpApplication,
) -> None:
    # Without the catalog the rule can only say "if OrderDate is the
    # partitioning column"; the DBA needs the measured loss, not a lead.
    app.executor.fetch_all = AsyncMock(
        return_value=[
            {"schema_name": "dbo", "table_name": "FactOrders", "partitioning_column": "OrderDate"}
        ]
    )

    result = await _call(app, "analyze_query_plan", {"plan_xml": PARTITIONED})

    [finding] = [item for item in result["findings"] if item["rule"] == "no_partition_elimination"]
    assert finding["evidence"]["partitioning_column_known"] is True
    assert finding["evidence"]["partitioning_column"] == "OrderDate"
    assert finding["severity"] != "low"
    assert app.executor.fetch_all.await_args.kwargs["params"] == ["dbo", "FactOrders"]


@pytest.mark.asyncio
async def test_a_refused_partition_catalog_read_keeps_the_finding_as_a_lead(
    app: AzureSqlMcpApplication,
) -> None:
    app.executor.fetch_all = AsyncMock(side_effect=RuntimeError("permission denied"))

    result = await _call(app, "analyze_query_plan", {"plan_xml": PARTITIONED})

    [finding] = [item for item in result["findings"] if item["rule"] == "no_partition_elimination"]
    assert finding["evidence"]["partitioning_column_known"] is False
    assert finding["severity"] == "low"


@pytest.mark.asyncio
async def test_live_session_plan_is_read_as_in_flight(app: AzureSqlMcpApplication) -> None:
    # A running request's counts are partial: "overestimate" on an operator
    # that has not finished is noise that sends the agent the wrong way.
    app.plan_sources.live = AsyncMock(
        return_value=PlanSource(PARTITIONED, {"source": "live_session", "session_id": 77})
    )

    result = await _call(app, "analyze_query_plan", {"session_id": 77})

    assert result["in_flight"] is True
    assert "in_flight_note" in result


@pytest.mark.asyncio
async def test_plan_with_no_operators_is_empty_not_ok(app: AzureSqlMcpApplication) -> None:
    # "ok" with no findings read as "plan is clean"; there was no plan to judge.
    result = await _call(app, "analyze_query_plan", {"plan_xml": NO_OPERATORS})

    assert result["result_status"] == "empty"
    assert "no operators" in result["result_status_reason"]


@pytest.mark.asyncio
async def test_explain_query_keeps_ranking_basis_and_note_with_the_findings(
    app: AzureSqlMcpApplication,
) -> None:
    # A finding list without its ranking basis is read as "first is slowest".
    app.plans.explain_query = AsyncMock(return_value=_artifact(app, PARTITIONED))

    result = await _call(app, "explain_query", {"sql": "SELECT 1 FROM dbo.FactOrders", "analyze": True})

    assert result["plan_findings"]["ranking_basis"] in {"self_elapsed_share", "estimated_cost_share"}
    assert "note" in result["plan_findings"]


@pytest.mark.asyncio
async def test_explain_actual_returns_logical_reads_when_only_the_plan_has_them(
    app: AzureSqlMcpApplication,
) -> None:
    # Agents quoted "rows touched" because reads were null, then ran their own
    # DMV query to get the number a tuning report leads with.
    artifact = _artifact(app, PARTITIONED)
    artifact.summary["actual_metrics"]["actual_logical_reads"] = None
    artifact.summary["actual_metrics"]["read_metric_source"] = "not_available_as_reliable_query_total"
    app.plans.explain_query = AsyncMock(return_value=artifact)

    result = await _call(app, "explain_query", {"sql": "SELECT 1 FROM dbo.FactOrders", "analyze": True})

    metrics = result["summary"]["actual_metrics"]
    assert metrics["actual_logical_reads"] == 994800
    assert metrics["read_metric_source"] == "showplan_operator_runtime_counters"


@pytest.mark.asyncio
async def test_explain_actual_names_the_client_clock_and_reports_server_time(
    app: AzureSqlMcpApplication,
) -> None:
    # 220 ms of network and fetch beside 0 ms of server time was read as "the query takes 220 ms".
    app.plans.explain_query = AsyncMock(return_value=_artifact(app, PARTITIONED))

    result = await _call(app, "explain_query", {"sql": "SELECT 1 FROM dbo.FactOrders", "analyze": True})

    assert result["execution_ms"] == 220.5
    assert result["server_elapsed_ms"] == 18000
    assert result["metric_provenance"]["execution_ms"] == "client_wall_clock"
    assert result["metric_provenance"]["server_elapsed_ms"] == "showplan_query_time_stats"
    assert "network" in result["metric_provenance"]["note"]


@pytest.mark.asyncio
async def test_explain_default_does_not_repeat_what_the_digest_holds(
    app: AzureSqlMcpApplication,
) -> None:
    # The same statement text, hashes, parameters and times came back three times.
    app.plans.explain_query = AsyncMock(return_value=_artifact(app, PARTITIONED))

    compact = await _call(app, "explain_query", {"sql": "SELECT 1 FROM dbo.FactOrders", "analyze": True})
    full = await _call(
        app,
        "explain_query",
        {"sql": "SELECT 1 FROM dbo.FactOrders", "analyze": True, "include_operator_summary": True},
    )

    summary = compact["summary"]
    for repeated in ("statements", "parameters"):
        assert repeated not in summary
        assert repeated in full["summary"]
    assert "statement_metrics" not in summary["actual_metrics"]
    assert "statement_metrics" in full["summary"]["actual_metrics"]
    # Empty lists and null counters say nothing.
    for empty in ("warnings", "spills", "implicit_conversions", "missing_indexes", "memory_grants"):
        assert empty not in summary
    assert "actual_physical_reads" not in summary["actual_metrics"]
    assert compact["plan_digest"]["statements"][0]["query_hash"] == "0x4D5E6F708192A3B4"
    assert len(json.dumps(compact["summary"])) < len(json.dumps(full["summary"])) / 2


@pytest.mark.asyncio
async def test_explain_names_a_supplied_parameter_the_sql_does_not_reference(
    app: AzureSqlMcpApplication,
) -> None:
    # "requires one explicit value for every query parameter" sent the agent
    # looking for a missing value when it had passed one too many.
    with pytest.raises(Exception) as error:
        await _call(
            app,
            "explain_query",
            {
                "sql": "SELECT 1 FROM dbo.FactOrders AS f WHERE f.OrderId = @id",
                "parameter_values": {"id": 5, "day": "2026-01-01"},
            },
        )

    assert "not referenced" in str(error.value)
    assert "@day" in str(error.value)
