"""Tool responses carry what a decision needs by default; the full form is opt-in.

Each default here was megabytes on a real database: the raw operator list of a
plan, a pretty-printed copy of an index review, the stored snapshot of every
index. The agent could not read them, so the answer was lost or truncated.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.artifacts import ExplainPlanArtifact
from azure_sql_mcp.index_review import CaptureResult
from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_server import make_config
from tests.unit.test_stream_b_review_compact import review  # noqa: F401  (pytest fixture)


@pytest.fixture
def app(tmp_path: Path) -> AzureSqlMcpApplication:
    return AzureSqlMcpApplication(make_config(tmp_path))


def _plan_artifact() -> ExplainPlanArtifact:
    operators = [{"node_id": node, "physical_op": "Index Seek"} for node in range(50)]
    return ExplainPlanArtifact(
        database_name="appdb",
        analyze=False,
        summary={
            "statement_count": 1,
            "operator_count": 50,
            "warnings": [],
            "missing_indexes": [],
            "operators": operators,
            "top_operators": operators[:5],
        },
        raw_xml="<ShowPlanXML />",
    )


@pytest.mark.asyncio
async def test_explain_query_returns_no_raw_operator_list_by_default(
    app: AzureSqlMcpApplication,
) -> None:
    app.plans.explain_query = AsyncMock(return_value=_plan_artifact())  # type: ignore[method-assign]

    payload = await app._explain_query("appdb", "SELECT 1", analyze=False)

    # The digest ranks operators by self time. A second list ranked by
    # estimated cost under the same name contradicted it.
    assert "operators" not in payload["summary"]
    assert "top_operators" not in payload["summary"]
    assert payload["summary"]["operator_count"] == 50
    assert payload["summary"]["statement_count"] == 1


@pytest.mark.asyncio
async def test_explain_query_full_operator_list_names_its_ranking(
    app: AzureSqlMcpApplication,
) -> None:
    app.plans.explain_query = AsyncMock(return_value=_plan_artifact())  # type: ignore[method-assign]

    payload = await app._explain_query(
        "appdb", "SELECT 1", analyze=False, include_operator_summary=True
    )

    assert len(payload["summary"]["operators"]) == 50
    assert "top_operators" not in payload["summary"]
    assert len(payload["summary"]["top_operators_by_estimated_cost"]) == 5


@pytest.mark.asyncio
async def test_portfolio_review_is_compact_by_default(
    app: AzureSqlMcpApplication,
    review,  # noqa: F811
) -> None:
    app.index_review.review_portfolio = AsyncMock(return_value=review)  # type: ignore[method-assign]

    payload = await app._review_index_portfolio("appdb", None, None)

    # 100 subjects, 5 actionable: the full form of this review is over 1 MB.
    assert len(json.dumps(payload)) < 40_000
    assert payload["subject_count"] == 100
    assert len(payload["actionable_subjects"]) == 5
    assert all(item["ddl"] and item["rollback"] for item in payload["actionable_subjects"])
    assert "subjects" not in payload
    assert "index-review.json" not in payload["artifacts"]
    assert payload["omitted_subjects"]["total"] == 95


@pytest.mark.asyncio
async def test_portfolio_review_full_form_is_opt_in(
    app: AzureSqlMcpApplication,
    review,  # noqa: F811
) -> None:
    app.index_review.review_portfolio = AsyncMock(return_value=review)  # type: ignore[method-assign]

    payload = await app._review_index_portfolio("appdb", None, None, include_full_review=True)

    assert len(payload["subjects"]) == 100
    assert "index-review.json" in payload["artifacts"]


@pytest.mark.asyncio
async def test_get_index_review_returns_full_detail_for_named_subjects(
    app: AzureSqlMcpApplication,
    review,  # noqa: F811
) -> None:
    app.index_review.get_review = AsyncMock(return_value=review)  # type: ignore[method-assign]
    compact = await app._get_index_review("appdb", "review-1")
    wanted = compact["actionable_subjects"][0]["subject_id"]

    payload = await app._get_index_review("appdb", "review-1", subject_ids=[wanted, "no-such"])

    # The gate that decides a drop stays reachable for any one subject.
    (subject,) = payload["subjects"]
    assert subject["subject_id"] == wanted
    assert "removal_gate" in subject or "definition" in subject
    assert payload["unknown_subject_ids"] == ["no-such"]
    assert "artifacts" not in payload


@pytest.mark.asyncio
async def test_capture_returns_a_summary_and_does_not_read_the_snapshot_back(
    app: AzureSqlMcpApplication,
) -> None:
    summary = {"status": "captured", "run_id": "run-1", "subject_count": 2000}
    result = AsyncMock(spec=CaptureResult)
    result.as_summary = lambda: summary
    result.as_dict = lambda: {"snapshot": {"subjects": ["x"] * 2000}}
    app.index_review.capture_snapshot = AsyncMock(return_value=result)  # type: ignore[method-assign]

    payload = await app._capture_index_review_snapshot("appdb", None)

    assert payload == summary
    app.index_review.capture_snapshot.assert_awaited_once_with(
        "appdb", idempotency_key=None, include_snapshot=False
    )


@pytest.mark.asyncio
async def test_capture_can_return_the_stored_snapshot_on_request(
    app: AzureSqlMcpApplication,
) -> None:
    result = AsyncMock(spec=CaptureResult)
    result.as_dict = lambda: {"snapshot": {"subjects": ["x"]}}
    app.index_review.capture_snapshot = AsyncMock(return_value=result)  # type: ignore[method-assign]

    payload = await app._capture_index_review_snapshot("appdb", None, include_snapshot=True)

    assert payload == {"snapshot": {"subjects": ["x"]}}
    app.index_review.capture_snapshot.assert_awaited_once_with(
        "appdb", idempotency_key=None, include_snapshot=True
    )


def test_capture_gets_time_for_its_catalog_and_query_store_reads(
    app: AzureSqlMcpApplication,
) -> None:
    # The default budget is one query timeout plus 15 s. A capture reads the
    # catalog, Query Store and DMVs and then writes: on a large database every
    # attempt timed out and rolled back, so nothing was ever stored.
    assert app._timeout_for_tool("capture_index_review_snapshot") >= (
        app.config.query_timeout_seconds * 6
    )


@pytest.mark.asyncio
async def test_review_workload_indexes_passes_the_tuned_query_ids(
    app: AzureSqlMcpApplication,
) -> None:
    # The optimizer reviews indexes for one query. Without its id the review
    # only sees the top queries, so a query ranked below them got advice
    # designed for other queries.
    app.workload_index_advisor.review = AsyncMock(return_value={"recommendations": []})  # type: ignore[method-assign]

    await app.mcp._tool_manager.call_tool(
        "review_workload_indexes", {"database_name": "appdb", "query_ids": [140, 141]}
    )

    assert app.workload_index_advisor.review.await_args.kwargs["query_ids"] == [140, 141]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "target", "arguments"),
    [
        ("analyze_workload_indexes", "recommendations.analyze_workload_indexes", {}),
        ("optimize_indexes", "index_optimizer.optimize", {}),
    ],
)
async def test_index_tools_return_every_index_only_on_request(
    app: AzureSqlMcpApplication,
    tool: str,
    target: str,
    arguments: dict,
) -> None:
    owner_name, method = target.split(".")
    owner = getattr(app, owner_name)
    setattr(owner, method, AsyncMock(return_value={"existing_indexes": []}))

    await app.mcp._tool_manager.call_tool(tool, {"database_name": "appdb", **arguments})
    assert getattr(owner, method).await_args.kwargs["include_all_existing_indexes"] is False

    await app.mcp._tool_manager.call_tool(
        tool, {"database_name": "appdb", "include_all_existing_indexes": True, **arguments}
    )
    assert getattr(owner, method).await_args.kwargs["include_all_existing_indexes"] is True


@pytest.mark.asyncio
async def test_analyze_query_indexes_passes_the_full_list_switch(
    app: AzureSqlMcpApplication,
) -> None:
    app.query_index_analysis.analyze_queries = AsyncMock(return_value={})  # type: ignore[method-assign]

    await app._analyze_query_indexes(
        "appdb", ["SELECT 1"], False, None, None, include_all_existing_indexes=True
    )

    assert (
        app.query_index_analysis.analyze_queries.await_args.kwargs["include_all_existing_indexes"]
        is True
    )
