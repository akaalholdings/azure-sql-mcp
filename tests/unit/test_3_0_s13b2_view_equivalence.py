"""The one-call experiments always compare results (live defect after commit 14eddcb).

Live: an indexed view took 8,981 ms CPU and 81,230 reads to 0 ms and 2 reads,
and the result said "not_compared: this SQL shape has no deterministic
comparison", because the candidate orders by a column of the view and the
original by a column of the table. A bare model compares the two result sets
and reports "identical".
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.catalog_constraints import TableConstraintFacts
from azure_sql_mcp.catalog_constraints import UniqueKey
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.equivalence_contract import inline_relation
from azure_sql_mcp.equivalence_contract import order_contract_risk
from azure_sql_mcp.equivalence_preflight import EquivalencePreflightService
from azure_sql_mcp.index_metadata import TABLE_COLUMN_FACTS_SQL
from azure_sql_mcp.index_metadata import TableColumnFacts
from azure_sql_mcp.performance_workflows import compact_benchmark_result
from azure_sql_mcp.performance_workflows import compare_result_sets
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.server import _compact_benchmark_payload
from azure_sql_mcp.view_experiments import plan_view_experiment
from tests.unit.test_3_0_s4_total_order_proof import FakeCatalog
from tests.unit.test_3_0_s4_total_order_proof import _facts
from tests.unit.test_3_0_s4_total_order_proof import _tables_only
from tests.unit.test_3_0_s13b2_indexed_view_tool import _app
from tests.unit.test_3_0_s13b2_indexed_view_tool import _ViewSandbox
from tests.unit.test_index_benchmark_workflow import _profile

VIEW = "v_Testing_FactPostMonth"
ORIGINAL = (
    "SELECT p.PostTypeKey, COUNT_BIG(*) AS PostCount, "
    "SUM(ISNULL(CAST(p.Score AS bigint), 0)) AS TotalScore FROM dw.FactPost AS p "
    "WHERE p.CreationDateKey / 100 = 201310 GROUP BY p.PostTypeKey ORDER BY p.PostTypeKey"
)
CANDIDATE = (
    f"SELECT v.PostTypeKey, v.PostCount, v.TotalScore FROM dw.{VIEW} AS v WITH (NOEXPAND) "
    "WHERE v.YearMonth = 201310 ORDER BY v.PostTypeKey"
)
BODY = (
    "SELECT p.CreationDateKey / 100 AS YearMonth, p.PostTypeKey, COUNT_BIG(*) AS PostCount, "
    "SUM(ISNULL(CAST(p.Score AS bigint), 0)) AS TotalScore FROM dw.FactPost AS p "
    "GROUP BY p.CreationDateKey / 100, p.PostTypeKey"
)
FACT_POST = {
    "dw.FactPost": [
        TableColumnFacts("CreationDateKey", "int", is_nullable=False),
        TableColumnFacts("PostTypeKey", "int", is_nullable=False),
        TableColumnFacts("Score", "int", is_nullable=False),
    ]
}


# --- the order analysis ----------------------------------------------------------------


def _view_facts() -> dict[tuple[str, str], TableConstraintFacts]:
    return {
        ("dw", VIEW.casefold()): TableConstraintFacts(
            schema_name="dw", table_name=VIEW, exists=True, object_type="USER_TABLE",
            nullable={"yearmonth": False, "posttypekey": False},
            unique_keys=(
                UniqueKey(name="IX_Testing_view", kind="unique_index", columns=("YearMonth", "PostTypeKey")),
            ),
        )
    }


@pytest.mark.asyncio
async def test_the_unique_key_of_the_planned_view_proves_the_order_of_the_candidate() -> None:
    """ORDER BY PostTypeKey with YearMonth fixed by an equality leaves no two rows tied."""

    service = EquivalencePreflightService(_tables_only, catalog=FakeCatalog({}))

    proven = (await service.analyze("appdb", CANDIDATE, declared_tables=_view_facts())).as_dict()
    tied = (
        await service.analyze(
            "appdb", CANDIDATE.replace("WHERE v.YearMonth = 201310 ", ""), declared_tables=_view_facts()
        )
    ).as_dict()

    assert proven["risk_codes"] == [] and proven["direct_snapshot_supported"] is True
    assert proven["unresolved_dependencies"] == []
    assert proven["result_order_proofs"][0]["keys"][0]["key_columns"] == ["YearMonth", "PostTypeKey"]
    # Without the equality two months tie on PostTypeKey.
    assert tied["result_order_ties"] and "YearMonth" in str(tied["result_order_ties"])


@pytest.mark.asyncio
async def test_a_column_fixed_by_an_equality_counts_as_an_ordering_column() -> None:
    """D08: WHERE UserId = @u ... ORDER BY ValidFrom with a unique key (UserId, ValidFrom)."""

    catalog = FakeCatalog(
        {
            "dimuser": _facts(
                "DimUser",
                nullable={"userid": False, "validfrom": False},
                unique_keys=(
                    UniqueKey(name="UQ_DimUser", kind="unique_index", columns=("UserId", "ValidFrom")),
                ),
            )
        }
    )
    service = EquivalencePreflightService(_tables_only, catalog=catalog)
    pinned = "SELECT TOP (1) d.Band FROM dbo.DimUser AS d WHERE d.UserId = @u ORDER BY d.ValidFrom DESC"

    fixed = (await service.analyze("appdb", pinned)).as_dict()
    ranged = (await service.analyze("appdb", pinned.replace("d.UserId = @u", "d.UserId > @u"))).as_dict()
    either = (
        await service.analyze("appdb", pinned.replace("d.UserId = @u", "d.UserId = @u OR d.Band = 1"))
    ).as_dict()

    assert fixed["risk_codes"] == []
    # A range or an OR fixes nothing.
    assert ranged["risk_codes"] == ["row_limit_total_order_unproven"]
    assert either["risk_codes"] == ["row_limit_total_order_unproven"]


@pytest.mark.asyncio
async def test_an_original_ordered_by_its_whole_group_by_key_is_a_total_order() -> None:
    service = EquivalencePreflightService(_tables_only, catalog=FakeCatalog({}))

    result = (await service.analyze("appdb", ORIGINAL)).as_dict()

    assert result["risk_codes"] == [] and result["result_order_ties"] == []


def test_the_view_read_as_its_definition_keeps_the_order_contract() -> None:
    """v.PostTypeKey is the table's PostTypeKey: the alias of the view is not a change."""

    assert order_contract_risk(ORIGINAL, CANDIDATE)["change"] == "keys_differ"

    inlined = inline_relation(CANDIDATE, "dw", VIEW, BODY)

    assert inlined is not None and "NOEXPAND" not in inlined and VIEW not in inlined
    assert order_contract_risk(ORIGINAL, inlined) is None
    # A candidate that orders by another column is still a changed contract.
    other = inline_relation(CANDIDATE.replace("ORDER BY v.PostTypeKey", "ORDER BY v.PostCount"), "dw", VIEW, BODY)
    assert order_contract_risk(ORIGINAL, other)["risk"] == "order_contract_changed"
    assert inline_relation(ORIGINAL, "dw", VIEW, BODY) is None


# --- first differences, without row values ------------------------------------------------


def _rows(*rows: tuple[int, int]) -> QueryResult:
    return QueryResult(
        columns=("PostTypeKey", "PostCount"),
        rows=[{"PostTypeKey": key, "PostCount": count} for key, count in rows],
        column_type_signatures=("synthetic-int", "synthetic-bigint"),
    )


def test_a_mismatch_names_the_first_difference_without_any_row_value() -> None:
    result = compare_result_sets(
        _rows((1, 500), (2, 731), (3, 90)), _rows((1, 500), (2, 9999), (4, 90)),
        row_limit=50, compare_order=True, same_snapshot=True,
    )

    assert result["status"] == "mismatch"
    assert result["first_differences"] == {
        "rows_only_in_baseline": 2,
        "rows_only_in_candidate": 2,
        "first_differing_row": 2,
        "first_differing_columns": ["PostCount"],
    }
    assert "9999" not in str(result) and "731" not in str(result)


def test_a_match_has_no_difference_block() -> None:
    result = compare_result_sets(
        _rows((1, 500)), _rows((1, 500)), row_limit=50, compare_order=True, same_snapshot=True
    )

    assert result["status"] == "match" and "first_differences" not in result


# --- the view benchmark ---------------------------------------------------------------------


class _FactPostSandbox(_ViewSandbox):
    def __init__(self, app: AzureSqlMcpApplication, monkeypatch: pytest.MonkeyPatch) -> None:
        super().__init__(app, monkeypatch)
        self.plan = plan_view_experiment(
            BODY, view_name=VIEW, schema_name="dw",
            clustered_index_key=["YearMonth", "PostTypeKey"], table_columns=FACT_POST,
        )
        self.candidate_rows: list[dict[str, Any]] = [{"id": 1}, {"id": 2}]
        self.snapshot_error: BaseException | None = None
        inner = app.executor.fetch_all.side_effect

        async def fetch(database: str, sql: str, *, params: Any = (), **kwargs: Any) -> list[dict[str, Any]]:
            if sql == TABLE_COLUMN_FACTS_SQL:
                return [
                    {"column_name": column.name, "type_name": column.type_name, "max_length": 4,
                     "is_nullable": 0, "is_computed": 0, "is_sparse": 0}
                    for column in FACT_POST["dw.FactPost"]
                ]
            if "sys.sql_modules" in sql and "object_type_code" in sql:
                return [{"schema_name": "dw", "object_name": "FactPost", "object_type_code": "U",
                         "object_type": "USER_TABLE", "definition": None, "is_encrypted": 0}]
            return await inner(database, sql, params=params, **kwargs)

        app.executor.fetch_all = AsyncMock(side_effect=fetch)  # type: ignore[method-assign]
        app.executor.execute_session_exactly_once = AsyncMock(side_effect=self._compare)  # type: ignore[method-assign]

    async def _compare(self, _database: str, _statements: list[str], **kwargs: Any) -> list[list[QueryResult]]:
        self.events.append("compare")
        hook = kwargs.get("before_statement_dispatch")
        for index in range(4):
            if hook is not None:
                hook(index)
        if self.snapshot_error is not None:
            raise self.snapshot_error

        def result(rows: list[dict[str, Any]]) -> list[QueryResult]:
            return [QueryResult(columns=("id",), rows=rows, column_type_signatures=("synthetic-int",))]

        return [[], [], result([{"id": 1}, {"id": 2}]), result(self.candidate_rows), [], []]


async def _run_view(
    app: AzureSqlMcpApplication,
    sandbox: _FactPostSandbox,
    *,
    candidate_sql: str = CANDIDATE,
    phase: str = "finalist",
) -> dict[str, Any]:
    case = app.performance_workflows.start_case("appdb", ORIGINAL)
    session_id = app.performance_workflows.start_session(case.case_id, "appdb")["session_id"]
    candidate = app.performance_workflows.add_candidate(
        session_id, candidate_sql, strategy="rewrite_plus_index"
    )
    clustered = str(sandbox.plan.clustered_index.index_name)

    async def profile(_database: str, sql: str, **_kwargs: Any) -> Any:
        reads_view = VIEW in sql
        return _profile(4 if reads_view else 900, index_name=clustered if reads_view else None)

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]
    return await app._benchmark_indexed_view_candidate(
        "appdb", session_id, candidate["candidate_id"], BODY,
        sql=ORIGINAL, candidate_sql=candidate_sql, view_name=VIEW, schema_name="dw",
        clustered_index_key=["YearMonth", "PostTypeKey"], phase=phase, idempotency_key="live-case",
    )


@pytest.mark.asyncio
async def test_the_live_statements_are_compared_as_ordered_rows_and_proven_for_the_cases(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _FactPostSandbox(app, monkeypatch)

    result = await _run_view(app, sandbox)

    assert sandbox.events == ["build_view", "compare", "drop_view"]
    assert result["classification"] == "improved", result["reason"]
    assert result["proof_scope"] == "direct_snapshot"
    comparison = result["equivalence"][0]
    assert comparison["status"] == "match"
    assert comparison["comparison_basis"] == "ordered_rows"
    assert comparison["proven_for_parameter_case"] is True
    decision = _compact_benchmark_payload(result)["decision"]
    assert decision["equivalence"] == {
        "status": "match",
        "proof_scope": "direct_snapshot",
        "deferred": False,
        "risk_codes": [],
        "rewrite_requirements": decision["equivalence"]["rewrite_requirements"],
        "comparison_basis": "ordered_rows",
    }


@pytest.mark.asyncio
async def test_a_screening_run_compares_the_results_too(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The build is the cost: no call builds the view and leaves the results uncompared."""

    app = _app(server_config_factory)
    sandbox = _FactPostSandbox(app, monkeypatch)

    result = await _run_view(app, sandbox, phase="screening")

    assert sandbox.events == ["build_view", "compare", "drop_view"]
    assert result["equivalence"][0]["status"] == "match"
    assert result["classification"] == "promising"
    assert result["executions"] == 9 + 2


@pytest.mark.asyncio
async def test_a_changed_order_is_still_compared_and_reported_as_a_tested_match(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _FactPostSandbox(app, monkeypatch)
    sandbox.candidate_rows = [{"id": 2}, {"id": 1}]
    by_count = CANDIDATE.replace("ORDER BY v.PostTypeKey", "ORDER BY v.PostCount")

    result = await _run_view(app, sandbox, candidate_sql=by_count)

    assert "compare" in sandbox.events
    comparison = result["equivalence"][0]
    assert comparison["status"] == "match"
    assert comparison["comparison_basis"] == "multiset_with_duplicate_counts"
    assert comparison["ordered_rows_match"] is False
    assert comparison["proven_for_parameter_case"] is False
    # The rows are the same; the order contract is not shown, so this is a test.
    assert result["classification"] == "performance_only"
    decision = _compact_benchmark_payload(result)["decision"]["equivalence"]
    assert decision["status"] == "match"
    assert decision["proof_scope"] == "snapshot_test"
    assert decision["deferred"] is False
    assert decision["risk_codes"] == ["order_contract_changed"]
    assert "tested" in result["reason"] and "order_contract_changed" in result["reason"]


@pytest.mark.asyncio
async def test_other_rows_fail_the_candidate_with_the_first_differences(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _FactPostSandbox(app, monkeypatch)
    sandbox.candidate_rows = [{"id": 1}, {"id": 3}]

    result = await _run_view(app, sandbox)

    assert result["classification"] == "equivalence_failed"
    comparison = result["equivalence"][0]
    assert comparison["status"] == "mismatch"
    assert comparison["first_differences"]["first_differing_row"] == 2
    assert comparison["first_differences"]["first_differing_columns"] == ["id"]
    summary = _compact_benchmark_payload(result)
    assert summary["equivalence"][0]["first_differences"]["rows_only_in_candidate"] == 1
    assert result["lease"]["status"] == "cleaned"


@pytest.mark.asyncio
async def test_a_comparison_that_fails_says_why_and_is_not_called_a_match(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _FactPostSandbox(app, monkeypatch)
    sandbox.snapshot_error = RuntimeError("snapshot isolation is not allowed in this database")

    result = await _run_view(app, sandbox)

    comparison = result["equivalence"][0]
    assert comparison["status"] == "inconclusive"
    assert comparison["reason"] == "snapshot comparison could not complete"
    assert comparison["error_type"] == "RuntimeError"
    assert result["classification"] == "inconclusive"
    assert result["lease"]["status"] == "cleaned"


def test_the_decision_block_shows_a_tested_match_as_snapshot_test_not_deferred() -> None:
    decision = compact_benchmark_result(
        {
            "classification": "performance_only",
            "proof_scope": "performance_only",
            "equivalence_deferred": True,
            "parameter_results": [],
            "equivalence": [
                {"parameter_case": "default", "status": "match", "proof_scope": "snapshot_test",
                 "comparison_basis": "multiset_with_duplicate_counts",
                 "proven_for_parameter_case": False}
            ],
        }
    )["decision"]["equivalence"]

    assert decision["status"] == "match"
    assert decision["proof_scope"] == "snapshot_test"
    assert decision["deferred"] is False
    assert decision["comparison_basis"] == "multiset_with_duplicate_counts"
