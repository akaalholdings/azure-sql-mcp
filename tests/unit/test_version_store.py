from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.version_store import VersionStoreService


class RoutingExecutor:
    def __init__(self, **responses: Any) -> None:
        self.responses = {
            "pvs": [{"database_id": 5, "persistent_version_store_size_kb": 30 * 1024 * 1024, "current_aborted_transaction_count": 2}],
            "space": [{"data_used_mb": 100_000.0, "data_allocated_mb": 120_000.0}],
            "transactions": [],
            "snapshots": [],
        }
        self.responses.update(responses)

    async def fetch_all(self, database_name: str, query: str, *args: Any, **kwargs: Any):
        if "persistent_version_store_stats" in query:
            key = "pvs"
        elif "FILEPROPERTY" in query:
            key = "space"
        elif "dm_tran_active_snapshot_database_transactions" in query:
            key = "snapshots"
        else:
            key = "transactions"
        result = self.responses[key]
        if isinstance(result, Exception):
            raise result
        return result


@pytest.mark.asyncio
async def test_large_store_held_by_a_long_transaction_is_flagged() -> None:
    executor = RoutingExecutor(
        transactions=[
            {"transaction_id": 1, "transaction_name": "user_transaction", "age_seconds": 7200, "session_id": 77, "login_name": "etl", "host_name": "batch01", "program_name": "loader", "session_status": "sleeping"}
        ]
    )

    result = await VersionStoreService(executor).get_version_store_stats("appdb")

    assert result["persistent_version_store"]["size_mb"] == 30720.0
    assert result["persistent_version_store"]["pct_of_used_data"] == 30.72
    codes = {finding["code"]: finding for finding in result["findings"]}
    assert codes["persistent_version_store_large"]["severity"] == "high"
    held = codes["version_cleanup_held_by_long_transaction"]
    assert held["severity"] == "high"
    assert "Session 77" in held["message"]
    assert "aborted_transactions_awaiting_cleanup" in codes


@pytest.mark.asyncio
async def test_long_snapshot_reader_also_holds_cleanup() -> None:
    executor = RoutingExecutor(
        pvs=[{"persistent_version_store_size_kb": 1024}],
        snapshots=[{"session_id": 91, "transaction_id": 5, "elapsed_time_seconds": 3600, "is_snapshot": 1}],
    )

    result = await VersionStoreService(executor).get_version_store_stats("appdb")

    held = next(f for f in result["findings"] if f["code"] == "version_cleanup_held_by_long_transaction")
    assert held["severity"] == "medium"
    assert "snapshot transaction" in held["message"]


@pytest.mark.asyncio
async def test_unreadable_store_is_unavailable_not_healthy() -> None:
    result = await VersionStoreService(RoutingExecutor(pvs=PermissionError("denied"))).get_version_store_stats("appdb")

    assert result["result_status"] == "unavailable"
    assert "VIEW DATABASE STATE" in result["result_status_reason"]


@pytest.mark.asyncio
async def test_optional_reads_degrade_to_gaps() -> None:
    executor = RoutingExecutor(space=PermissionError("denied"), transactions=PermissionError("denied"))

    result = await VersionStoreService(executor).get_version_store_stats("appdb")

    assert result["persistent_version_store"]["pct_of_used_data"] is None
    assert len(result["gaps"]) == 2
