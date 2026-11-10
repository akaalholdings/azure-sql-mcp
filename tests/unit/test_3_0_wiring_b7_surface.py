"""B7 wiring: the new guide and statistics tools, compact payloads, one
parameter shape, the decision text gate and the short server instructions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from azure_sql_mcp.learning_contracts import ContractValidationError
from azure_sql_mcp.learning_contracts import DecisionRecordV1
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.server import _compact_benchmark_payload
from azure_sql_mcp.server_instructions import SERVER_INSTRUCTIONS
from azure_sql_mcp.performance_workflows import aggregate_samples
from tests.unit.test_server import make_config
from tests.unit.test_server import make_read_policy


async def _call(app: AzureSqlMcpApplication, name: str, arguments: dict[str, Any]) -> Any:
    return await app.mcp._tool_manager.call_tool(name, arguments)


@pytest.fixture
def app(tmp_path: Path) -> AzureSqlMcpApplication:
    application = AzureSqlMcpApplication(make_config(tmp_path))
    application.database_policy = make_read_policy("appdb")
    return application


@pytest.mark.asyncio
async def test_tool_guide_lists_guides_and_returns_one(app: AzureSqlMcpApplication) -> None:
    # Long guidance lives behind one call, not in every tool description.
    listing = await _call(app, "get_tool_guide", {})
    guide = await _call(app, "get_tool_guide", {"name": "explain_query"})
    unknown = await _call(app, "get_tool_guide", {"name": "no_such_tool"})

    assert any(entry["name"] == "explain_query" for entry in listing["guides"])
    assert guide["found"] is True and guide["name"] == "explain_query"
    assert unknown["found"] is False and "explain_query" in unknown["available"]


@pytest.mark.asyncio
async def test_column_statistics_tool_reads_the_service_and_needs_allow_read(
    app: AzureSqlMcpApplication, tmp_path: Path
) -> None:
    # Histogram values are data values: the same read gate as the catalog tools.
    app.column_statistics.get_column_statistics = AsyncMock(
        return_value={"result_status": "ok", "columns": []}
    )

    await _call(
        app,
        "get_column_statistics",
        {"schema_name": "dbo", "table_name": "Posts", "columns": ["OwnerUserId"], "max_steps": 10},
    )

    app.column_statistics.get_column_statistics.assert_awaited_once_with(
        "appdb", "dbo", "Posts", ["OwnerUserId"], include_histogram=True, max_steps=10, top_values=5
    )
    denied = AzureSqlMcpApplication(make_config(tmp_path))
    denied.column_statistics.get_column_statistics = AsyncMock()
    with pytest.raises(ToolError):
        await _call(denied, "get_column_statistics", {"schema_name": "dbo", "table_name": "Posts"})
    denied.column_statistics.get_column_statistics.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_sql_with_no_rows_is_empty(app: AzureSqlMcpApplication) -> None:
    # "ok" with zero rows was read as "the tool returned something I missed".
    app.executor.fetch_all = AsyncMock(return_value=[])

    result = await _call(app, "execute_sql", {"sql": "SELECT 1 AS n WHERE 1 = 0"})

    assert result["result_status"] == "empty"
    assert result["row_count"] == 0


def _evidence_payload() -> dict[str, Any]:
    rows = [{"wait_type": f"W{index}", "wait_time_ms": index, "pad": "x" * 200} for index in range(400)]
    return {
        "performance_case_id": "case-1",
        "outcome": "healthy",
        "evidence": {"evidence_id": "ev-1", "kind": "baseline", "metrics": {"sections": rows}},
        "sections": {
            "waits": {"available": True, "captured_at_utc": "2026-11-10T00:00:00Z", "gaps": [], "data": {"top_waits": rows}},
            "regressions": {"available": False, "error": "permission denied", "gaps": ["regressions"]},
        },
        "profile": None,
        "outcome_basis": {"query_scoped": ["query_store_history"]},
        "incomplete_evidence_can_be_healthy": False,
    }


@pytest.mark.asyncio
async def test_collect_evidence_returns_a_summary_unless_full_is_asked(
    app: AzureSqlMcpApplication,
) -> None:
    # 250 KB came back and the agent used only the evidence id.
    app._collect_performance_evidence = AsyncMock(return_value=_evidence_payload())  # type: ignore[method-assign]
    arguments = {"case_id": "case-1", "sql": "SELECT 1"}

    summary = await _call(app, "collect_performance_evidence", arguments)
    full = await _call(app, "collect_performance_evidence", {**arguments, "detail": "full"})

    assert summary["evidence"]["evidence_id"] == "ev-1"
    assert summary["outcome"] == "healthy"
    assert summary["sections"]["waits"]["row_counts"] == {"top_waits": 400}
    assert summary["sections"]["regressions"]["available"] is False
    assert summary["sections"]["regressions"]["error"] == "permission denied"
    assert len(json.dumps(summary)) < 2500
    assert len(full["sections"]["waits"]["data"]["top_waits"]) == 400


def _benchmark_payload() -> dict[str, Any]:
    samples = [
        {"elapsed_ms": 100.0, "client_elapsed_ms": 120.0, "cpu_ms": 50.0, "logical_reads": 800.0,
         "physical_reads": 0.0, "actual_rows": 10}
    ] * 5
    fast = [{**sample, "elapsed_ms": 20.0, "logical_reads": 90.0} for sample in samples]
    result = {
        "parameter_case": "common",
        "weight": 1.0,
        "baseline": aggregate_samples(samples),
        "candidate": aggregate_samples(fast),
        "plan_delta": {"plan_changed": True, "baseline": None, "candidate": None},
        "decision_basis": "per_bucket_median_shift_v2",
    }
    return {
        "session_id": "s",
        "candidate_id": "c",
        "classification": "improved",
        "objective": "elapsed_time",
        "executions": 14,
        "metrics": result,
        "parameter_results": [result],
        "parameter_case_receipts": [{"name": "common", "fingerprint": "f" * 64}],
        "equivalence": [
            {"parameter_case": "common", "status": "match", "proven_for_parameter_case": True,
             "result_evidence": [{"digest": "d" * 64}] * 10}
        ],
        "equivalence_preflight": {
            "classification": "direct_snapshot", "direct_snapshot_supported": True,
            "risk_codes": [], "functions": [], "resolved_dependencies": [{"x": 1}] * 20,
        },
        "proof_scope": "snapshot_matched",
        "evidence_id": "ev-9",
    }


def test_compact_benchmark_keeps_the_decision_numbers_and_drops_the_spread() -> None:
    # About ten numbers decide a candidate; 32 KB of spread and digests hid them.
    full = _benchmark_payload()

    compact = _compact_benchmark_payload(full)

    case = compact["parameter_results"][0]
    assert case["baseline"]["elapsed_ms"] == 100.0
    assert case["candidate"]["elapsed_ms"] == 20.0
    assert case["candidate"]["logical_reads"] == 90.0
    assert case["baseline"]["sample_count"] == 5
    assert "spread" not in case["baseline"] and "run_values" not in case["candidate"]
    assert "metrics" not in compact
    assert "result_evidence" not in compact["equivalence"][0]
    assert compact["equivalence"][0]["status"] == "match"
    assert compact["equivalence_preflight"] == {
        "classification": "direct_snapshot", "direct_snapshot_supported": True, "risk_codes": [],
    }
    assert compact["evidence_id"] == "ev-9"
    assert len(json.dumps(compact)) < len(json.dumps(full)) / 4


@pytest.mark.asyncio
async def test_benchmark_tool_is_compact_by_default_with_the_headline_first(
    app: AzureSqlMcpApplication,
) -> None:
    app.tuning_sessions.get_session = lambda _id: type("S", (), {"deadline_at_utc": None})()  # type: ignore[method-assign]
    app.performance_workflows.benchmark_candidate = AsyncMock(return_value=_benchmark_payload())
    arguments = {"session_id": "s", "candidate_id": "c", "baseline_sql": "SELECT 1", "candidate_sql": "SELECT 1"}

    compact = await _call(app, "benchmark_tuning_candidate", arguments)
    full = await _call(app, "benchmark_tuning_candidate", {**arguments, "detail": "full"})

    assert next(iter(compact)) == "headline"
    assert compact["headline"]["relative_improvement_pct"] == 80.0
    assert "spread" not in compact["parameter_results"][0]["baseline"]
    assert "spread" in full["parameter_results"][0]["baseline"]
    assert full["headline"]["relative_improvement_pct"] == 80.0


def _decision(**overrides: Any) -> DecisionRecordV1:
    values: dict[str, Any] = {
        "skill": "sql-optimizer",
        "skill_version": "2.5.0",
        "learning_key": "paging-keyset",
        "consumed_evidence_refs": ("evidence-1",),
        "subject_kind": "query",
        "subject_fingerprint": "f" * 64,
        "tactic": "Create an index on the join column, then join back to the table with a derived table.",
        "expected_result": {"summary": "Fewer reads; the update path is unchanged."},
        "confidence": 0.7,
        "uncertainty": {"summary": "Selectivity of the filter on the dropped column."},
        "evaluator_fingerprint": "e" * 64,
        "runtime_fingerprint": "a" * 64,
        "runtime_compatibility_fingerprint": "b" * 64,
        "tool_schema_fingerprint": "c" * 64,
        "sanitized_config_fingerprint": "d" * 64,
        "scope": {"database_fingerprint": "g" * 64},
    }
    values.update(overrides)
    return DecisionRecordV1(**values)


def test_decision_text_accepts_ordinary_words() -> None:
    # "index", "join", "table", "with", "create", "update" and "drop" are how
    # a DBA writes a tactic; refusing them made the learning loop unusable.
    decision = _decision()

    assert "join back to the table" in decision.tactic


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT p.Id FROM dbo.Posts AS p WHERE p.OwnerUserId = 22656",
        "select top (10) Id from Posts",
        "SELECT 1",
        "UPDATE dbo.Users SET Reputation = 0 WHERE Id = 5",
        "DELETE FROM dbo.Votes WHERE PostId = 7",
        "INSERT INTO dbo.Badges (UserId) VALUES (1)",
        "EXEC sp_executesql N'x'",
        "DECLARE @id int = 5",
        "CREATE INDEX IX_Posts_Owner ON dbo.Posts (OwnerUserId)",
        "DROP TABLE dbo.Posts",
        "WITH keys AS (SELECT Id FROM dbo.Posts) SELECT Id FROM keys",
        "MERGE dbo.T AS t USING dbo.S AS s ON t.Id = s.Id WHEN MATCHED THEN DELETE",
    ],
)
def test_decision_text_still_refuses_sql_statements(statement: str) -> None:
    # Statement text carries literals: customer data must not enter the learning store.
    with pytest.raises(ContractValidationError):
        _decision(tactic=f"Tried this: {statement}")
    with pytest.raises(ContractValidationError):
        _decision(expected_result={"summary": statement})


def test_decision_fingerprints_keep_the_strict_rule() -> None:
    with pytest.raises(ContractValidationError):
        _decision(subject_fingerprint="select the fingerprint")


def test_record_decision_accepts_a_text_summary_for_result_and_uncertainty(
    app: AzureSqlMcpApplication,
) -> None:
    # The skill reference describes both as summaries; the tool refused a string.
    properties = app.mcp._tool_manager._tools["record_decision"].parameters["properties"]

    for name in ("expected_result", "uncertainty"):
        accepted = {option.get("type") for option in properties[name]["anyOf"]}
        assert accepted == {"object", "string"}


@pytest.mark.asyncio
async def test_explain_query_accepts_the_parameter_case_shape(app: AzureSqlMcpApplication) -> None:
    # One shape for parameter values in every tool: no relearning per tool.
    app._explain_query = AsyncMock(return_value={"summary": {}, "plan_kind": "estimated"})  # type: ignore[method-assign]

    await _call(
        app,
        "explain_query",
        {
            "sql": "SELECT 1 FROM dbo.Posts AS p WHERE p.Id = @id",
            "parameter_case": {"name": "common", "values": {"id": 5}, "types": {"id": "int"}, "weight": 1},
        },
    )

    args = app._explain_query.await_args.args
    assert {"id": 5} in args and {"id": "int"} in args


@pytest.mark.asyncio
async def test_compare_query_results_accepts_the_flat_parameter_spelling(
    app: AzureSqlMcpApplication,
) -> None:
    app.performance_workflows.compare_query_results = AsyncMock(return_value={"status": "match"})

    await _call(
        app,
        "compare_query_results",
        {
            "baseline_sql": "SELECT 1 FROM dbo.Posts AS p WHERE p.Id = @id",
            "candidate_sql": "SELECT 1 FROM dbo.Posts AS p WHERE p.Id = @id",
            "parameter_values": {"id": 5},
            "parameter_types": {"id": "int"},
        },
    )

    case = app.performance_workflows.compare_query_results.await_args.kwargs["parameter_case"]
    assert case["values"] == {"id": 5} and case["types"] == {"id": "int"}


@pytest.mark.asyncio
async def test_two_parameter_spellings_at_once_are_refused(app: AzureSqlMcpApplication) -> None:
    with pytest.raises(ToolError, match="parameter_case or parameter_values"):
        await _call(
            app,
            "explain_query",
            {
                "sql": "SELECT 1 FROM dbo.Posts AS p WHERE p.Id = @id",
                "parameter_values": {"id": 5},
                "parameter_case": {"name": "c", "values": {"id": 6}, "types": {"id": "int"}, "weight": 1},
            },
        )


def test_instructions_name_the_new_tools_and_stay_short() -> None:
    # The instructions are read at every session start: each line costs every user.
    for tool in (
        "get_tool_guide",
        "get_column_statistics",
        "get_failed_executions",
        "get_diagnostic_history",
        "get_memory_pressure",
        "assess_capacity",
    ):
        assert tool in SERVER_INSTRUCTIONS
    for field in ("include_tool_names", "detail=full", "confirmation_token", "group_by"):
        assert field in SERVER_INSTRUCTIONS
    assert len(SERVER_INSTRUCTIONS) < 8200
