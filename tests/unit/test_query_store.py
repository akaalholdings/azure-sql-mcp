from __future__ import annotations

import pytest

from azure_sql_mcp.query_store import QueryStoreService


class FakeExecutor:
    def __init__(self, rows=None):
        self.query = None
        self.params = None
        self.rows = rows or []

    async def fetch_all(self, database_name, query, params=None):
        self.query = query
        self.params = params
        return self.rows


@pytest.mark.asyncio
async def test_top_queries_uses_requested_sort_expression():
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_top_queries("appdb", "cpu", 30, 5)

    assert "ORDER BY SUM(rs.avg_cpu_time * rs.count_executions) DESC" in executor.query
    assert executor.params == [5, 30]


@pytest.mark.asyncio
async def test_top_queries_supports_extended_sort_metrics():
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_top_queries("appdb", "logical_io", 45, 10)

    assert "SUM(rs.avg_logical_io_reads * rs.count_executions)" in executor.query
    assert "avg_query_max_used_memory" in executor.query
    assert "CAST(MAX(rsi.end_time) AS datetime2(7)) AS last_seen_utc" in executor.query
    assert executor.params == [10, 45]


@pytest.mark.asyncio
async def test_top_queries_uses_resource_blend_query_shape():
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_top_queries("appdb", "resource_blend", 60, 7)

    assert "WITH QueryMetrics AS" in executor.query
    assert "resource_blend_score" in executor.query
    assert "MAX(total_logical_io_reads) AS max_logical_io_reads" in executor.query
    assert "CAST(MAX(rsi.end_time) AS datetime2(7)) AS last_seen_utc" in executor.query
    assert executor.params == [60, 7]


@pytest.mark.asyncio
async def test_top_queries_rejects_unknown_sort():
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    with pytest.raises(ValueError, match="resource_blend"):
        await service.get_top_queries("appdb", "bad-sort", 30, 5)


@pytest.mark.asyncio
async def test_exact_query_identity_uses_binary_length_aware_comparison() -> None:
    executor = FakeExecutor()
    service = QueryStoreService(executor)
    sql = "SELECT N'MiXeD  value' "

    await service.resolve_query_identity("appdb", sql)

    assert "DATALENGTH(qt.query_sql_text)" in executor.query
    assert "Latin1_General_100_BIN2" in executor.query
    assert executor.params == [sql, sql]


@pytest.mark.asyncio
async def test_query_history_escapes_like_wildcards():
    """Query text routinely contains % and _; the fingerprint must match as a
    literal substring, not as a wildcard pattern."""
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_query_history_by_text(
        "appdb",
        "SELECT * FROM t WHERE name LIKE '%foo_bar%'",
    )

    escaped_fingerprint = executor.params[2]
    assert "[%]" in escaped_fingerprint
    assert "[_]" in escaped_fingerprint
    # The reversed containment clause still receives the raw fingerprint value.
    assert executor.params[3] == "SELECT * FROM t WHERE name LIKE '%foo_bar%'"


@pytest.mark.asyncio
async def test_query_history_by_hash_converts_hex_string():
    """Hash matching survives parameter renaming (@CustomerId vs @P1) that
    defeats text matching; the hex string must be converted to BINARY(8)."""
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    result = await service.get_query_history_by_hash(
        "appdb", "0x90FC7E5399EA52A5", window_minutes=60, limit=5,
    )

    assert "CONVERT(BINARY(8), ?, 1)" in executor.query
    assert executor.params == [5, 60, "0x90FC7E5399EA52A5"]
    assert result["matches"] == []
    assert result["query_hash"] == "0x90FC7E5399EA52A5"


@pytest.mark.asyncio
async def test_query_history_by_hash_rejects_non_hex():
    service = QueryStoreService(FakeExecutor())
    with pytest.raises(ValueError, match="0x-prefixed"):
        await service.get_query_history_by_hash("appdb", "DROP TABLE x")


@pytest.mark.asyncio
async def test_query_history_prefers_query_id_over_hash_and_fuzzy_text() -> None:
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    result = await service.get_query_history(
        "appdb",
        query_id=42,
        query_hash="0x1111111111111111",
        sql="SELECT * FROM Orders",
        window_minutes=60,
        limit=5,
    )

    assert result["identity_kind"] == "query_id"
    assert "q.query_id = ?" in executor.query
    assert "LIKE '%'" not in executor.query
    assert executor.params == [5, 60, 42]


@pytest.mark.asyncio
async def test_query_history_by_id_omits_variant_column_from_plan_catalog() -> None:
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_query_history_by_id("appdb", 42, window_minutes=60, limit=5)

    query = executor.query.lower()
    assert "p.query_variant_query_id" not in query
    assert "query_variant_query_id" not in query
    assert query.count("q.query_parameterization_type_desc") == 2
    assert "p.query_parameterization_type_desc" not in query
    assert "and q.query_id = ?" in query
    assert executor.params == [5, 60, 42]


@pytest.mark.asyncio
async def test_query_history_averages_are_execution_weighted_and_zero_safe() -> None:
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_query_history_by_id("appdb", 42, window_minutes=60, limit=5)

    assert (
        "SUM(rs.avg_duration * rs.count_executions) "
        "/ NULLIF(SUM(rs.count_executions), 0)"
    ) in executor.query
    assert (
        "SUM(rs.avg_cpu_time * rs.count_executions) "
        "/ NULLIF(SUM(rs.count_executions), 0)"
    ) in executor.query
    assert "SUM(rs.avg_logical_io_reads * rs.count_executions)" in executor.query
    assert "SUM(rs.avg_rowcount * rs.count_executions)" in executor.query
    assert "AVG(rs.avg_duration)" not in executor.query
    assert "AVG(rs.avg_cpu_time)" not in executor.query


@pytest.mark.asyncio
async def test_parameter_runtime_buckets_preserve_compiled_values_and_provenance() -> None:
    plan_xml = """\
    <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
      <BatchSequence><Batch><Statements><StmtSimple><QueryPlan><ParameterList>
        <ColumnReference Column="@CustomerId" ParameterDataType="int"
                         ParameterCompiledValue="(1)" ParameterRuntimeValue="(42)" />
      </ParameterList></QueryPlan></StmtSimple></Statements></Batch></BatchSequence>
    </ShowPlanXML>
    """
    executor = FakeExecutor(
        rows=[
            {
                "query_id": 42,
                "plan_id": 7,
                "query_plan_xml": plan_xml,
                "executions": 10,
                "avg_duration_ms": 12.5,
            }
        ]
    )
    service = QueryStoreService(executor)

    result = await service.get_parameter_runtime_buckets(
        "appdb",
        query_id=42,
        window_minutes=60,
        limit=5,
    )

    bucket = result["buckets"][0]
    assert bucket["compiled_parameters"][0]["compiled_value"] == "(1)"
    assert bucket["compiled_parameters"][0]["runtime_value"] == "(42)"
    assert bucket["runtime_parameter_values_observed"] is True
    assert bucket["runtime_bucket_source"] == (
        "query_store_runtime_stats_by_plan_and_interval"
    )
    assert result["distinct_compiled_parameter_sets"] == [
        [{"name": "@CustomerId", "compiled_value": "(1)"}]
    ]
    assert result["distinct_compiled_parameter_set_count"] == 1


@pytest.mark.asyncio
async def test_parameter_runtime_buckets_omits_variant_column_from_plan_catalog() -> None:
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    result = await service.get_parameter_runtime_buckets(
        "appdb",
        query_id=42,
        window_minutes=60,
        limit=5,
    )

    query = executor.query.lower()
    assert "p.query_variant_query_id" not in query
    assert "query_variant_query_id" not in query
    assert query.count("q.query_parameterization_type_desc") == 2
    assert "p.query_parameterization_type_desc" not in query
    assert "and q.query_id = ?" in query
    assert result["query_id"] == 42
    assert executor.params == [5, 60, 42]


@pytest.mark.asyncio
async def test_parameter_buckets_weight_runtime_averages_by_execution_count() -> None:
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_parameter_runtime_buckets(
        "appdb", query_id=42, window_minutes=60, limit=5
    )

    assert (
        "SUM(rs.avg_duration * rs.count_executions) "
        "/ NULLIF(SUM(rs.count_executions), 0)"
    ) in executor.query
    assert "SUM(rs.avg_logical_io_reads * rs.count_executions)" in executor.query
    assert "SUM(rs.avg_rowcount * rs.count_executions)" in executor.query
    assert "AVG(rs.avg_duration)" not in executor.query
    assert "AVG(rs.avg_logical_io_reads)" not in executor.query
