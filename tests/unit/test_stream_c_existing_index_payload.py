"""The legacy index tools list only the existing indexes of the tables they
touch, in a compact form, and look each table up once per run."""

from __future__ import annotations

import copy
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from azure_sql_mcp import index_metadata
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import IndexKeyColumn
from azure_sql_mcp.index_optimizer import IndexOptimizer
from azure_sql_mcp.query_index_analysis import QueryIndexAnalysisService
from azure_sql_mcp.safe_sql import SafeSqlValidator
from tests.unit.test_index_optimizer import FakeExecutor

COMPACT_KEYS = {
    "schema",
    "table",
    "name",
    "index_type",
    "key_columns",
    "include_columns",
    "filter_definition",
    "is_unique",
    "is_disabled",
    "usage",
    "partition_count",
    "compression",
}
# About 4.4 KB per index in full; 2,000 of them were about 8.9 MB.
PAYLOAD_LIMIT_BYTES = 20_000


def _table(number: int) -> str:
    return f"T{number:04d}"


def _synthetic_indexes(tables: int = 400, per_table: int = 5, partitions: int = 12) -> list[ExistingIndex]:
    indexes: list[ExistingIndex] = []
    for number in range(tables):
        for index_id in range(1, per_table + 1):
            numbers = range(1, partitions + 1)
            indexes.append(
                ExistingIndex(
                    schema="dbo",
                    table=_table(number),
                    index_id=index_id,
                    name=f"IX_{_table(number)}_{index_id}",
                    index_type="CLUSTERED" if index_id == 1 else "NONCLUSTERED",
                    key_columns=(IndexKeyColumn(f"Key{index_id}"), IndexKeyColumn("CreatedAt", "DESC")),
                    include_columns=("Status", "Total") if index_id > 1 else (),
                    is_unique=index_id == 1,
                    usage={"user_seeks": 10, "user_scans": 1, "user_lookups": 0, "user_updates": 5},
                    usage_context={"availability": "available", "coverage": "covered"},
                    object_id=1000 + number,
                    index_type_code=1 if index_id == 1 else 2,
                    data_space_name="ps_Date",
                    data_space_type="PARTITION_SCHEME",
                    partition_scheme_name="ps_Date",
                    partition_columns=("CreatedAt",),
                    partition_compression=tuple(
                        (n, "NONE" if n > partitions - 2 else "PAGE") for n in numbers
                    ),
                    xml_compression=tuple((n, "OFF") for n in numbers),
                    partition_row_counts=tuple((n, 1_000 * n) for n in numbers),
                    partition_page_counts=tuple((n, 10 * n) for n in numbers),
                )
            )
    return indexes


def _plan(*, missing_table: str, missing_column: str, read_table: str) -> str:
    return (
        '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">'
        "<BatchSequence><Batch><Statements>"
        '<StmtSimple StatementType="SELECT" StatementSubTreeCost="12.5"><QueryPlan>'
        '<MissingIndexes><MissingIndexGroup Impact="80.0">'
        f'<MissingIndex Database="[appdb]" Schema="[dbo]" Table="[{missing_table}]">'
        f'<ColumnGroup Usage="EQUALITY"><Column Name="[{missing_column}]" /></ColumnGroup>'
        "</MissingIndex></MissingIndexGroup></MissingIndexes>"
        '<RelOp NodeId="0" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan">'
        "<IndexScan>"
        f'<Object Database="[appdb]" Schema="[dbo]" Table="[{read_table}]" Index="[IX_{read_table}_1]" IndexKind="Clustered" />'
        "</IndexScan></RelOp>"
        "</QueryPlan></StmtSimple>"
        "</Statements></Batch></BatchSequence></ShowPlanXML>"
    )


def _names(payload: dict[str, Any]) -> set[str]:
    return {index["name"] for index in payload["existing_indexes"]}


def _expected(*tables: int) -> set[str]:
    return {f"IX_{_table(number)}_{index_id}" for number in tables for index_id in range(1, 6)}


def _assert_compact(payload: dict[str, Any], *tables: int) -> None:
    assert _names(payload) == _expected(*tables)
    assert all(set(index) == COMPACT_KEYS for index in payload["existing_indexes"])
    assert payload["existing_indexes_total"] == 2_000
    assert payload["existing_indexes_omitted"] == 2_000 - 5 * len(tables)
    assert "get_object_index_diagnostics" in payload["existing_indexes_note"]
    assert len(json.dumps(payload, default=str).encode()) < PAYLOAD_LIMIT_BYTES


# ---------------------------------------------------------------- #23


def test_compact_index_keeps_what_an_agent_reads_and_one_compression_summary() -> None:
    [index] = _synthetic_indexes(tables=1, per_table=1)

    payload = index_metadata.existing_index_response([index], [("DBO", "t0000")])

    assert payload["existing_indexes"] == [
        {
            "schema": "dbo",
            "table": "T0000",
            "name": "IX_T0000_1",
            "index_type": "CLUSTERED",
            "key_columns": [{"name": "Key1", "direction": "ASC"}, {"name": "CreatedAt", "direction": "DESC"}],
            "include_columns": [],
            "filter_definition": None,
            "is_unique": True,
            "is_disabled": False,
            "usage": {"user_seeks": 10, "user_scans": 1, "user_lookups": 0, "user_updates": 5},
            "partition_count": 12,
            "compression": "mixed: NONE x2, PAGE x10",
        }
    ]
    assert (payload["existing_indexes_total"], payload["existing_indexes_omitted"]) == (1, 0)


def test_full_metadata_stays_available_on_request() -> None:
    indexes = _synthetic_indexes(tables=3)

    payload = index_metadata.existing_index_response(indexes, [], full=True)

    assert payload["existing_indexes"] == [index.as_dict() for index in indexes]
    assert (payload["existing_indexes_total"], payload["existing_indexes_omitted"]) == (15, 0)


def _analysis_service(indexes: list[ExistingIndex], plan_xml: str) -> QueryIndexAnalysisService:
    executor = MagicMock()
    executor.config.row_limit = 200
    service = QueryIndexAnalysisService(executor, SafeSqlValidator())
    service._get_existing_indexes = AsyncMock(return_value=indexes)  # type: ignore[method-assign]
    service._get_estimated_plan = AsyncMock(return_value=plan_xml)  # type: ignore[method-assign]
    return service


@pytest.mark.asyncio
async def test_analyze_queries_lists_only_indexes_of_the_tables_it_touched() -> None:
    # On a database with 2,000 indexes the response was about 8.9 MB (2M
    # tokens), nearly all of it indexes of tables the query never touched.
    indexes = _synthetic_indexes()
    assert len(indexes) == 2_000
    assert len(json.dumps([index.as_dict() for index in indexes]).encode()) > 4_000_000
    service = _analysis_service(indexes, _plan(missing_table=_table(3), missing_column="Region", read_table=_table(7)))

    payload = await service.analyze_queries("appdb", ["SELECT Region FROM dbo.T0003"])

    # T0003 is named by the recommendation, T0007 is read by the analysed plan.
    assert [(rec["table"], rec["key_columns"]) for rec in payload["recommendations"]] == [("T0003", ["Region"])]
    _assert_compact(payload, 3, 7)


@pytest.mark.asyncio
async def test_index_that_covers_a_filtered_hint_is_still_listed() -> None:
    # The hint on T0005 is dropped because IX_T0005_2 covers it. The agent must
    # still see that index, or the empty recommendation list has no reason.
    indexes = _synthetic_indexes()
    service = _analysis_service(indexes, _plan(missing_table=_table(5), missing_column="Key2", read_table=_table(5)))

    payload = await service.analyze_queries("appdb", ["SELECT Key2 FROM dbo.T0005"])

    assert payload["recommendations"] == []
    assert payload["hints_removed_by_filtering_or_consolidation"] == 1
    _assert_compact(payload, 5)


@pytest.mark.asyncio
async def test_analyze_queries_returns_every_index_in_full_on_request() -> None:
    indexes = _synthetic_indexes(tables=4)
    service = _analysis_service(indexes, _plan(missing_table=_table(1), missing_column="Region", read_table=_table(1)))

    payload = await service.analyze_queries(
        "appdb", ["SELECT Region FROM dbo.T0001"], include_all_existing_indexes=True
    )

    assert payload["existing_indexes"] == [index.as_dict() for index in indexes]
    assert payload["existing_indexes_omitted"] == 0


@pytest.mark.asyncio
async def test_analyze_workload_lists_only_indexes_of_the_tables_it_touched() -> None:
    indexes = _synthetic_indexes()
    service = _analysis_service(indexes, _plan(missing_table=_table(3), missing_column="Region", read_table=_table(7)))
    service.param_binding.prepare_query_store_text = AsyncMock(  # type: ignore[method-assign]
        side_effect=lambda _database, sql: sql
    )
    service._get_top_workload_queries = AsyncMock(  # type: ignore[method-assign]
        return_value=[{"query_id": 42, "plan_id": 7, "query_sql_text": "SELECT Region FROM dbo.T0003"}]
    )
    # A DMV request is returned to the agent too, so its table counts.
    service._get_dmv_missing_indexes = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            {"schema": "dbo", "table": _table(9), "equality_columns": ["Region"], "inequality_columns": [], "include_columns": []}
        ]
    )

    payload = await service.analyze_workload("appdb")

    assert [rec["table"] for rec in payload["recommendations"]] == ["T0003"]
    _assert_compact(payload, 3, 7, 9)


class _ColumnFilteringExecutor(FakeExecutor):
    """Returns widths only for the columns asked for, as the IN list does."""

    # FakeExecutor has no config, so the estimated-plan read failed and every
    # workload query was skipped.
    config = SimpleNamespace(row_limit=200)

    async def fetch_all(self, database_name, query, params=None):
        rows = await super().fetch_all(database_name, query, params)
        if "AS write_ratio" in query:
            # FakeExecutor matches this read as the index catalog read.
            return [{"write_ratio": self.write_ratio_val}]
        if "c.max_length" in query:
            return [row for row in rows if row["column_name"] in params[2:]]
        # The optimizer edits the rows it reads; each run gets its own.
        return copy.deepcopy(rows)


def _dmv_row(table: str, column: str, include: str) -> dict[str, Any]:
    return {
        "schema_name": "dbo",
        "table_name": table,
        "equality_columns": f"[{column}]",
        "inequality_columns": None,
        "included_columns": f"[{include}]",
        "user_seeks": 50,
        "user_scans": 0,
        "avg_total_user_cost": 10.0,
        "avg_user_impact": 80.0,
        "estimated_improvement": 400.0,
    }


@pytest.mark.asyncio
async def test_optimize_indexes_lists_only_indexes_of_the_tables_it_touched() -> None:
    indexes = _synthetic_indexes()
    executor = _ColumnFilteringExecutor(
        workload_rows=[
            {
                "query_id": 1,
                "plan_id": 10,
                "query_sql_text": "SELECT Region FROM dbo.T0003",
                "executions": 100,
                "total_cpu_us": 5_000_000,
                "total_duration_us": 6_000_000,
                "total_logical_io_reads": 1_000,
            }
        ],
        dmv_rows=[_dmv_row(_table(9), "CustomerId", "Status")],
        plan_xml=_plan(missing_table=_table(3), missing_column="CustomerId", read_table=_table(7)),
    )
    optimizer = IndexOptimizer(executor=executor, validator=SafeSqlValidator())  # type: ignore[arg-type]
    optimizer._get_existing_indexes = AsyncMock(return_value=indexes)  # type: ignore[method-assign]

    result = await optimizer.optimize("appdb")
    payload = result.as_dict()

    assert {selected["table"] for selected in payload["selected_indexes"]} == {"T0003", "T0009"}
    assert payload["existing_indexes_count"] == 2_000
    _assert_compact(payload, 3, 7, 9)

    full = (await optimizer.optimize("appdb", include_all_existing_indexes=True)).as_dict()
    assert full["existing_indexes"] == [index.as_dict() for index in indexes]
    assert full["existing_indexes_omitted"] == 0


# ---------------------------------------------------------------- #61


def _three_candidates_on_one_table() -> _ColumnFilteringExecutor:
    return _ColumnFilteringExecutor(
        dmv_rows=[
            _dmv_row("Orders", "CustomerId", "Status"),
            _dmv_row("Orders", "CreatedAt", "Status"),
            _dmv_row("Orders", "Status", "CustomerId"),
        ],
        row_count=100_000,
    )


@pytest.mark.asyncio
async def test_optimize_reads_each_table_once_for_all_its_candidates() -> None:
    # Three candidates on one table repeated the same row count, write ratio
    # and column width reads: three round trips per candidate.
    executor = _three_candidates_on_one_table()
    optimizer = IndexOptimizer(executor=executor, validator=SafeSqlValidator())  # type: ignore[arg-type]

    result = await optimizer.optimize("appdb")

    assert result.candidates_after_consolidation == 3
    queries = [(query, params) for _database, query, params in executor.fetch_history]
    row_count_reads = [params for query, params in queries if "SUM(p.row_count)" in query]
    write_ratio_reads = [params for query, params in queries if "AS write_ratio" in query]
    width_reads = [params for query, params in queries if "c.max_length" in query]
    assert row_count_reads == [["dbo", "Orders"]]
    assert write_ratio_reads == [["dbo", "Orders"]]
    # Each column's width is asked for once; the later candidates reuse it.
    assert [params[2:] for params in width_reads] == [["CustomerId", "Status"], ["CreatedAt"]]

    # The memo does not change the estimates: each equals a read of its own.
    fresh = IndexOptimizer(executor=_three_candidates_on_one_table(), validator=SafeSqlValidator())  # type: ignore[arg-type]
    assert len(result.selected_indexes) == 3
    for selected in result.selected_indexes:
        columns = [*selected.candidate.key_columns, *selected.candidate.include_columns]
        assert selected.estimated_size_mb == await fresh._estimate_index_size("appdb", "dbo", "Orders", columns)
        assert selected.estimated_size_mb > 0
        assert selected.write_ratio == 0.3


@pytest.mark.asyncio
async def test_optimize_does_not_share_lookups_between_runs() -> None:
    # Row counts and write ratios move between calls; the memo lives for one run.
    executor = _ColumnFilteringExecutor(dmv_rows=[_dmv_row("Orders", "CustomerId", "Status")])
    optimizer = IndexOptimizer(executor=executor, validator=SafeSqlValidator())  # type: ignore[arg-type]

    await optimizer.optimize("appdb")
    await optimizer.optimize("appdb")

    row_count_reads = [query for _database, query, _params in executor.fetch_history if "SUM(p.row_count)" in query]
    assert len(row_count_reads) == 2
