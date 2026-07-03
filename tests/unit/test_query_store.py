from __future__ import annotations

import pytest

from azure_sql_mcp.query_store import QueryStoreService


class FakeExecutor:
    def __init__(self):
        self.query = None
        self.params = None

    async def fetch_all(self, database_name, query, params=None):
        self.query = query
        self.params = params
        return []


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
