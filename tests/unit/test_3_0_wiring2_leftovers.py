"""Wiring 2, item 6: what the first wiring pass listed as not done."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp import query_store as query_store_module
from azure_sql_mcp import server_instructions
from azure_sql_mcp import tool_guides
from azure_sql_mcp.performance_workflows import BENCHMARK_MARKER_PREFIX
from azure_sql_mcp.performance_workflows import mark_benchmark_sql
from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_server import make_config


async def _call(app: AzureSqlMcpApplication, name: str, arguments: dict[str, Any]) -> Any:
    return await app.mcp._tool_manager.call_tool(name, arguments)


@pytest.fixture
def app(tmp_path: Path) -> AzureSqlMcpApplication:
    return AzureSqlMcpApplication(make_config(tmp_path))


def _tool(app: AzureSqlMcpApplication, name: str) -> Any:
    return next(tool for tool in app.mcp._tool_manager.list_tools() if tool.name == name)


@pytest.mark.asyncio
async def test_trend_and_regressions_can_be_read_for_one_replica_role(
    app: AzureSqlMcpApplication,
) -> None:
    # A read-only workload on a secondary was blended into the primary's
    # history, so its regression showed as a smaller change of the whole.
    app.query_store_trends.trend = AsyncMock(return_value={"buckets": []})  # type: ignore[method-assign]
    app.query_store_trends.regressions = AsyncMock(  # type: ignore[method-assign]
        return_value={"regressions": []}
    )

    await _call(app, "get_query_store_trend", {"query_id": 7, "replica_role": "secondary"})
    await _call(app, "get_query_store_regressions", {"replica_role": "geo_secondary"})

    assert app.query_store_trends.trend.await_args.kwargs["replica_role"] == "secondary"
    assert app.query_store_trends.regressions.await_args.kwargs["replica_role"] == "geo_secondary"
    with pytest.raises(Exception, match="replica_role"):
        await _call(app, "get_query_store_trend", {"replica_role": "tertiary"})


def test_the_benchmark_marker_predicate_matches_what_the_benchmark_sends() -> None:
    # One spelling of the marker in two modules: a silent drift would put the
    # tool's own benchmark runs back into the top queries.
    assert query_store_module.BENCHMARK_MARKER_PREFIX == BENCHMARK_MARKER_PREFIX
    marked = mark_benchmark_sql("SELECT 1", "session", "baseline")
    assert marked.startswith(query_store_module.BENCHMARK_MARKER_PREFIX)
    predicate = query_store_module.benchmark_statement_predicate("qt.query_sql_text")
    assert f"qt.query_sql_text LIKE N'{BENCHMARK_MARKER_PREFIX}%'" in predicate
    # A parameterized statement is stored behind its declaration block.
    assert f"qt.query_sql_text LIKE N'(@%){BENCHMARK_MARKER_PREFIX}%'" in predicate


@pytest.mark.parametrize("group_by", ["plan", "query", "query_hash"])
def test_top_queries_leave_out_statements_the_benchmark_tools_ran(
    app: AzureSqlMcpApplication, group_by: str
) -> None:
    # After a tuning session the product's own A-B runs were the top
    # consumers, and the query_hash roll-up added them to the application's.
    sql = app.query_store._build_top_queries_query("cpu", group_by)

    inner = sql.split("GROUP BY", 1)[0]
    assert "NOT EXISTS" in inner
    assert f"LIKE N'{BENCHMARK_MARKER_PREFIX}%'" in inner
    # Still one parameter marker for TOP and one for the window.
    assert sql.count("?") == 2


@pytest.mark.asyncio
async def test_query_identity_candidates_leave_out_benchmark_statements(
    app: AzureSqlMcpApplication,
) -> None:
    # A benchmark copy has the query_hash of the statement it measures: with
    # the copies in, a hash lookup of a tuned query is always ambiguous.
    app.executor.fetch_all = AsyncMock(return_value=[])  # type: ignore[method-assign]

    await app.query_store.resolve_query_identity(
        "appdb", "SELECT 1", query_hash="0x1234567890ABCDEF"
    )

    for call in app.executor.fetch_all.await_args_list:
        assert f"NOT LIKE N'{BENCHMARK_MARKER_PREFIX}%'" in call.args[1]


def _plan_with_hash(query_hash: str) -> Any:
    from azure_sql_mcp.artifacts import ExplainPlanArtifact

    return ExplainPlanArtifact(
        database_name="appdb",
        analyze=False,
        summary={"statements": [{"query_hash": query_hash}]},
        raw_xml="<ShowPlanXML />",
    )


@pytest.mark.asyncio
async def test_query_store_evidence_resolves_the_query_by_the_hash_of_its_plan(
    app: AzureSqlMcpApplication,
) -> None:
    # Text identity fails on a change of whitespace or casing; the case then
    # had no Query Store history at all although the query was in the store.
    app.plans.explain_query = AsyncMock(return_value=_plan_with_hash("0x1234567890ABCDEF"))  # type: ignore[method-assign]
    app.query_store.resolve_query_identity = AsyncMock(  # type: ignore[method-assign]
        return_value={
            "status": "resolved",
            "query_id": 42,
            "query_hash": "0x1234567890ABCDEF",
            "matched_by": "query_hash",
        }
    )
    app.query_store.get_query_history_by_id = AsyncMock(return_value={"matches": [{}]})  # type: ignore[method-assign]
    app.query_store.get_parameter_runtime_buckets = AsyncMock(return_value={"buckets": []})  # type: ignore[method-assign]

    evidence = await app._collect_query_store_evidence("appdb", "select  1 from dbo.Posts", 60)

    assert app.query_store.resolve_query_identity.await_args.kwargs["query_hash"] == (
        "0x1234567890ABCDEF"
    )
    assert evidence["identity"]["query_id"] == 42
    assert evidence["identity"]["identity_provenance"] == "plan_query_hash"
    assert evidence["identity"]["identity_verified_against_sql"] is True


@pytest.mark.asyncio
async def test_a_plan_that_cannot_be_compiled_falls_back_to_text_identity(
    app: AzureSqlMcpApplication,
) -> None:
    app.plans.explain_query = AsyncMock(side_effect=RuntimeError("SHOWPLAN denied"))  # type: ignore[method-assign]
    app.query_store.resolve_query_identity = AsyncMock(  # type: ignore[method-assign]
        return_value={"status": "not_found", "matches": []}
    )

    evidence = await app._collect_query_store_evidence("appdb", "SELECT 1 FROM dbo.Posts", 60)

    assert app.query_store.resolve_query_identity.await_args.kwargs["query_hash"] is None
    assert evidence["available"] is False
    assert evidence["identity"]["identity_provenance"] == "exact_sql_text"


@pytest.mark.asyncio
async def test_the_contract_version_is_the_package_version(app: AzureSqlMcpApplication) -> None:
    # A literal fell behind the package and told the agent it spoke to an
    # older server than check_runtime_status named.
    app.capabilities.check = AsyncMock(return_value={"query_store": True})  # type: ignore[method-assign]
    app.platform_capabilities.get_summary = AsyncMock(  # type: ignore[method-assign]
        return_value={"platform": "azure_sql_database_paas"}
    )

    contract = (await app._check_database_capabilities("appdb"))["mcp_contract"]

    assert contract["contract_version"] == app._runtime_status()["package_version"]
    assert contract["branching_batch"] == 1
    assert contract["index_set_experiment"] == 1
    assert contract["slow_baseline_cases"] == 1


@pytest.mark.parametrize(
    ("tool", "fields"),
    [
        (
            "get_query_compilation_stats",
            ["recompile_count", "recompiling_now", "excessive_recompile_count"],
        ),
        (
            "get_forced_plans",
            ["force_failure_count", "plan_forcing_type_desc", "query_recent_execution_count"],
        ),
        ("diagnose_database", ["conclusion", "stories", "leads", "facts", "gaps"]),
    ],
)
def test_tool_descriptions_name_the_fields_the_tool_returns(
    app: AzureSqlMcpApplication, tool: str, fields: list[str]
) -> None:
    # A description that names a field the tool does not return (a "recompile
    # ratio") makes the agent report a number that is not there.
    description = _tool(app, tool).description
    for name in fields:
        assert name in description
    assert "recompile ratio exceeds" not in description


def test_guides_cover_the_branching_batch_and_the_index_set(
    app: AzureSqlMcpApplication,
) -> None:
    names = {tool.name for tool in app.mcp._tool_manager.list_tools()}
    for workflow in ("branching_batch", "index_set_experiment"):
        guide = tool_guides.get_tool_guide(workflow)
        assert guide["found"] is True
        assert guide["kind"] == "workflow"
        # A guide that names a tool that does not exist sends the agent to an error.
        assert set(guide["next_tools"]) <= names
    listed = {entry["name"]: entry["kind"] for entry in tool_guides.list_tool_guides()}
    assert listed["branching_batch"] == "workflow"


@pytest.mark.parametrize("name", [entry["name"] for entry in tool_guides.list_tool_guides()])
def test_every_argument_a_guide_names_is_an_argument_of_a_registered_tool(
    app: AzureSqlMcpApplication, name: str
) -> None:
    # "phase=finalist" or "indexes=" in a guide is copied into a call. An
    # argument that no tool takes is refused as an unknown field.
    arguments = {
        parameter
        for tool in app.mcp._tool_manager.list_tools()
        for parameter in tool.parameters.get("properties", {})
    }
    cited = set(re.findall(r"(?<![A-Za-z_@])([a-z][a-z_]+)=", tool_guides.get_tool_guide(name)["guide"]))

    assert sorted(cited - arguments) == []


def test_instructions_give_the_slow_query_workflow_in_at_most_twelve_lines() -> None:
    text = server_instructions.SERVER_INSTRUCTIONS
    start = text.index("- One slow query")
    end = text.index("- Plan stability:")
    workflow = text[start:end].strip().splitlines()

    assert len(workflow) <= 12
    body = " ".join(workflow)
    for phrase in (
        "profile null",
        "indexes=",
        "IF / ELSE",
        "slow_baseline_cases",
        "benchmark_index_candidate",
        "finalize_tuning_session",
    ):
        assert phrase in body
