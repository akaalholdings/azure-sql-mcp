"""Wiring 2, items 4 and 5: catalog unique keys for the plan rules, and compact results."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_3_0_s12_plan_findings import _access
from tests.unit.test_3_0_s12_plan_findings import _counters
from tests.unit.test_3_0_s12_plan_findings import _eq_seek
from tests.unit.test_3_0_s12_plan_findings import _key_lookup_plan
from tests.unit.test_3_0_s12_plan_findings import _plan
from tests.unit.test_server import make_config


async def _call(app: AzureSqlMcpApplication, name: str, arguments: dict[str, Any]) -> Any:
    return await app.mcp._tool_manager.call_tool(name, arguments)


class _Catalog:
    """The catalog reads of the constraint service, by table."""

    def __init__(self, unique_keys: dict[str, list[tuple[str, list[str], dict[str, Any]]]]) -> None:
        self.unique_keys = unique_keys
        self.key_reads: list[str] = []

    async def fetch_all(self, _database: str, sql: str, *, params: Any = (), **_kwargs: Any) -> Any:
        if "partition_ordinal" in sql:
            return []
        table = str(list(params)[1]) if len(list(params)) == 2 else ""
        if "c.is_nullable" in sql:
            return [{"object_type": "USER_TABLE", "column_name": "Id", "is_nullable": False}]
        if "i.is_unique = 1" in sql:
            self.key_reads.append(table)
            return [
                {"index_name": name, "column_name": column, **flags}
                for name, columns, flags in self.unique_keys.get(table, [])
                for column in columns
            ]
        return []


@pytest.fixture
def app(tmp_path: Path) -> AzureSqlMcpApplication:
    return AzureSqlMcpApplication(make_config(tmp_path))


def _sniff_findings(result: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in result["findings"] if item["rule"] == "sniffed_parameter_differs"]


@pytest.mark.asyncio
async def test_a_seek_of_a_whole_unique_key_is_not_a_sniffed_parameter_finding(
    app: AzureSqlMcpApplication,
) -> None:
    # T4, the trap task: one row by primary key. The finding sent the agent
    # after a parameter problem that cannot exist on a single-row seek.
    catalog = _Catalog({"Users": [("PK_Users", ["Id"], {"is_primary_key": True})]})
    app.executor.fetch_all = catalog.fetch_all  # type: ignore[method-assign]

    result = await _call(app, "analyze_query_plan", {"plan_xml": _key_lookup_plan(level="FULL")})

    assert _sniff_findings(result) == []
    assert catalog.key_reads == ["Users"]


@pytest.mark.asyncio
async def test_a_filtered_unique_index_does_not_clear_the_finding(
    app: AzureSqlMcpApplication,
) -> None:
    # A filtered key is unique only inside its filter: the seek can return many rows.
    catalog = _Catalog({"Users": [("UQ_Users_Active", ["Id"], {"has_filter": True})]})
    app.executor.fetch_all = catalog.fetch_all  # type: ignore[method-assign]

    result = await _call(app, "analyze_query_plan", {"plan_xml": _key_lookup_plan(level="FULL")})

    assert len(_sniff_findings(result)) == 1


@pytest.mark.asyncio
async def test_an_unreadable_catalog_keeps_the_finding(app: AzureSqlMcpApplication) -> None:
    app.executor.fetch_all = AsyncMock(side_effect=RuntimeError("permission denied"))  # type: ignore[method-assign]

    result = await _call(app, "analyze_query_plan", {"plan_xml": _key_lookup_plan(level="FULL")})

    assert len(_sniff_findings(result)) == 1
    assert result["result_status"] == "ok"


@pytest.mark.asyncio
async def test_a_plan_without_compiled_parameter_values_reads_no_keys(
    app: AzureSqlMcpApplication,
) -> None:
    # The rule can only fire where a parameter was sniffed: no other plan pays
    # for catalog reads.
    catalog = _Catalog({})
    app.executor.fetch_all = catalog.fetch_all  # type: ignore[method-assign]
    seek = _access(
        0, "Users", "u", "PK_Users", seek=_eq_seek("Users", "Id", "u", "@Id"),
        runtime=_counters(1, reads=3), rows=1, cost=0.0033,
    )

    await _call(app, "analyze_query_plan", {"plan_xml": _plan(seek, cost=0.0033)})

    assert catalog.key_reads == []


@pytest.mark.asyncio
async def test_key_reads_are_bounded_to_five_tables(app: AzureSqlMcpApplication) -> None:
    catalog = _Catalog({})
    app.executor.fetch_all = catalog.fetch_all  # type: ignore[method-assign]
    seeks = "".join(
        _access(
            node, f"T{node}", f"t{node}", f"PK_T{node}",
            seek=_eq_seek(f"T{node}", "Id", f"t{node}", "@Id"),
            runtime=_counters(1, reads=3), rows=1, cost=0.0033,
        )
        for node in range(7)
    )
    parameters = (
        '<ColumnReference Column="@Id" ParameterDataType="int" '
        'ParameterCompiledValue="(22656)" ParameterRuntimeValue="(1144035)" />'
    )

    union = (
        '<RelOp NodeId="100" PhysicalOp="Concatenation" LogicalOp="Concatenation" '
        f'EstimateRows="7" EstimatedTotalSubtreeCost="0.02"><Concat>{seeks}</Concat></RelOp>'
    )

    await _call(
        app,
        "analyze_query_plan",
        {"plan_xml": _plan(union, cost=0.02, level="FULL", parameters=parameters)},
    )

    assert len(catalog.key_reads) == 5


# --- Item 5: compact results ---


def _recommendation(action: str, table: str, *query_ids: int) -> dict[str, Any]:
    return {
        "action": action,
        "table": table,
        "supporting_queries": [{"query_id": query_id, "plan_id": 1} for query_id in query_ids],
    }


@pytest.mark.asyncio
async def test_recommendations_that_serve_the_requested_queries_come_first(
    app: AzureSqlMcpApplication,
) -> None:
    # T1: four suggestions from other queries came back and the agent had to
    # work out that none was about the statement it was tuning.
    app.workload_index_advisor.review = AsyncMock(  # type: ignore[method-assign]
        return_value={
            "recommendations": [
                _recommendation("create_index", "dbo.Votes", 11),
                _recommendation("drop_index", "dbo.Badges"),
                _recommendation("create_index", "dbo.Posts", 11, 42),
            ]
        }
    )

    # Stream S13b-2: the recommendations of other queries are counted and left
    # out by default (they were most of a 16 to 34 KB report); they come back,
    # after the ones that serve the query, with include_other_recommendations.
    result = await _call(app, "review_workload_indexes", {"query_ids": [42]})
    everything = await _call(
        app,
        "review_workload_indexes",
        {"query_ids": [42], "include_other_recommendations": True},
    )

    assert [item["table"] for item in result["recommendations"]] == ["dbo.Posts"]
    assert result["requested_queries"]["other_recommendations_omitted"] == 2
    assert [item["table"] for item in everything["recommendations"]] == [
        "dbo.Posts",
        "dbo.Votes",
        "dbo.Badges",
    ]
    assert [item["serves_requested_query"] for item in everything["recommendations"]] == [
        True,
        False,
        False,
    ]
    assert result["requested_queries"]["recommendations_for_them"] == 1


@pytest.mark.asyncio
async def test_no_recommendation_for_the_requested_query_is_said_plainly(
    app: AzureSqlMcpApplication,
) -> None:
    app.workload_index_advisor.review = AsyncMock(  # type: ignore[method-assign]
        return_value={"recommendations": [_recommendation("create_index", "dbo.Votes", 11)]}
    )

    result = await _call(app, "review_workload_indexes", {"query_ids": [42, 43]})

    assert result["requested_queries"]["recommendations_for_them"] == 0
    assert "No recommendation serves query id 42, 43" in result["requested_queries"]["message"]
    assert "1 recommendation" in result["requested_queries"]["message"]

    plain = await _call(app, "review_workload_indexes", {})
    assert "requested_queries" not in plain
    assert "serves_requested_query" not in plain["recommendations"][0]


def _evidence_payload() -> dict[str, Any]:
    return {
        "performance_case_id": "case-1",
        "outcome": "collected",
        "evidence": {"evidence_id": "evidence-1", "kind": "collected"},
        "sections": {
            "query_store": {"available": True, "complete": True, "data": {"rows": [{}] * 400}},
            "waits": {"available": False, "complete": False, "error": "permission denied"},
        },
        "profile": {
            "metrics": {"elapsed_ms": 31, "cpu_ms": 30, "logical_reads": 24698},
            "plan_findings": {"findings": [{"rule": "scan"}]},
        },
    }


@pytest.mark.asyncio
async def test_tune_query_returns_the_evidence_summary_unless_full_is_asked(
    app: AzureSqlMcpApplication,
) -> None:
    # The evidence block was 250 KB and the agent used only its id.
    app.executor.fetch_all = AsyncMock(return_value=[])  # type: ignore[method-assign]
    app._collect_performance_evidence = AsyncMock(  # type: ignore[method-assign]
        return_value=_evidence_payload()
    )
    arguments = {"sql": "SELECT p.Id FROM dbo.Posts AS p", "analyze": False}

    summary = await _call(app, "tune_query", arguments)
    full = await _call(app, "tune_query", {**arguments, "detail": "full"})

    assert summary["evidence"]["evidence_id"] == "evidence-1"
    assert summary["evidence"]["sections_unavailable"] == ["waits"]
    assert summary["evidence"]["profile_metrics"]["logical_reads"] == 24698
    assert "sections" not in summary["evidence"]
    assert summary["detail"] == "summary"
    assert len(full["evidence"]["sections"]["query_store"]["data"]["rows"]) == 400


@pytest.mark.asyncio
async def test_a_compact_benchmark_result_leads_with_the_decision(
    app: AzureSqlMcpApplication,
) -> None:
    # About ten numbers decide a candidate; they came inside 32 KB.
    from azure_sql_mcp import server as server_module

    payload = {
        "classification": "improved",
        "objective": "logical_reads",
        "phase": "screening",
        "executions": 8,
        "proof_scope": "direct_snapshot",
        "equivalence": [{"status": "match", "parameter_case": "heavy"}],
        "parameter_results": [
            {
                "parameter_case": "heavy",
                "baseline": {"sample_count": 3, "elapsed_ms": 31, "cpu_ms": 30, "logical_reads": 24698},
                "candidate": {"sample_count": 3, "elapsed_ms": 1, "cpu_ms": 0, "logical_reads": 5},
                "comparison_margin": {
                    "bucket_verdict": "improved",
                    "baseline_median": 24698,
                    "candidate_median": 5,
                },
            }
        ],
    }

    compact = server_module._compact_benchmark_payload(payload)

    [case] = compact["decision"]["cases"]
    assert case["parameter_case"] == "heavy"
    assert case["speedup"] == 4939.6
    assert case["candidate"]["logical_reads"] == 5
    assert compact["decision"]["equivalence"]["status"] == "match"
    assert compact["decision"]["classification"] == "improved"
