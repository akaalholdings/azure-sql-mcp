from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.health import HealthService


class FakeExecutor:
    def __init__(
        self,
        responses: list[tuple[str, list[dict[str, Any]]]] | None = None,
        errors: list[tuple[str, Exception]] | None = None,
    ):
        self.responses = responses or []
        self.errors = errors or []
        self.calls: list[tuple[str, str, tuple[Any, ...] | None]] = []

    async def fetch_all(
        self,
        database_name: str,
        query: str,
        params: list[Any] | tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        normalized_params = tuple(params) if params is not None else None
        self.calls.append((database_name, query, normalized_params))

        for needle, error in self.errors:
            if needle in query:
                raise error

        for needle, rows in self.responses:
            if needle in query:
                return rows

        return []


class FakeQueryStoreService:
    def __init__(self, payload: dict[str, Any] | None = None):
        self.payload = payload or {
            "enabled": True,
            "status": {
                "actual_state_desc": "READ_WRITE",
                "desired_state_desc": "READ_WRITE",
            },
        }
        self.calls: list[str] = []

    async def get_status(self, database_name: str) -> dict[str, Any]:
        self.calls.append(database_name)
        return self.payload


def build_service(
    responses: list[tuple[str, list[dict[str, Any]]]] | None = None,
    errors: list[tuple[str, Exception]] | None = None,
    query_store_payload: dict[str, Any] | None = None,
) -> tuple[HealthService, FakeExecutor, FakeQueryStoreService]:
    executor = FakeExecutor(responses=responses, errors=errors)
    query_store = FakeQueryStoreService(payload=query_store_payload)
    return HealthService(executor, query_store), executor, query_store


def assert_threshold_payload(check: dict[str, Any]) -> None:
    assert check["status"] in {"pass", "warning", "critical"}
    assert isinstance(check["details"], dict)
    assert isinstance(check["thresholds"], dict)
    assert isinstance(check["findings"], list)


@pytest.mark.asyncio
async def test_analyze_all_includes_phase_two_checks_and_threshold_payloads():
    service, executor, query_store = build_service(
        responses=[
            (
                "Buffer cache hit ratio",
                [
                    {
                        "buffer_cache_hit_ratio": 92.3,
                        "page_life_expectancy_seconds": 280,
                    }
                ],
            ),
            (
                "Page life expectancy",
                [
                    {
                        "buffer_cache_hit_ratio": 92.3,
                        "page_life_expectancy_seconds": 280,
                    }
                ],
            ),
            (
                "sys.dm_exec_sessions",
                [
                    {
                        "total_sessions": 95,
                        "active_requests": 3,
                        "idle_with_open_transaction": 21,
                        "idle_sessions": 40,
                        "session_limit": 100,
                    }
                ],
            ),
            (
                "sys.foreign_keys",
                [
                    {
                        "schema_name": "dbo",
                        "table_name": "Orders",
                        "constraint_name": "FK_Orders_Customers",
                        "constraint_type": "FOREIGN_KEY",
                        "is_disabled": 0,
                        "row_count": 125000,
                    }
                ],
            ),
            (
                "COUNT(*)",
                [
                    {
                        "schema_name": "dbo",
                        "table_name": "Orders",
                        "constraint_name": "FK_Orders_Customers",
                        "constraint_type": "FOREIGN_KEY",
                        "is_disabled": 0,
                        "row_count": 125000,
                    }
                ],
            ),
            (
                "sys.partitions",
                [
                    {
                        "schema_name": "dbo",
                        "table_name": "Orders",
                        "constraint_name": "FK_Orders_Customers",
                        "constraint_type": "FOREIGN_KEY",
                        "is_disabled": 0,
                        "row_count": 125000,
                    }
                ],
            ),
            (
                "sys.check_constraints",
                [
                    {
                        "schema_name": "dbo",
                        "table_name": "Orders",
                        "constraint_name": "CK_Orders_Status",
                        "constraint_type": "CHECK",
                        "is_disabled": 0,
                        "row_count": 125000,
                    }
                ],
            ),
            (
                "sys.dm_geo_replication_link_status",
                [
                    {
                        "replication_group": "rg-1",
                        "partner_server": "secondary.database.windows.net",
                        "partner_database": "appdb",
                        "role_desc": "PRIMARY",
                        "replication_state_desc": "CATCH_UP",
                        "synchronization_health_desc": "NOT_HEALTHY",
                        "replication_lag_sec": 45,
                        "last_replication": "2026-03-30T12:00:00Z",
                    }
                ],
            ),
            (
                "sys.identity_columns",
                [
                    {
                        "schema_name": "dbo",
                        "table_name": "Events",
                        "column_name": "EventId",
                        "data_type": "int",
                        "seed_value": 1,
                        "increment_value": 1,
                        "last_value": 2050000000,
                        "max_value": 2147483647,
                        "pct_used": 95.6,
                    }
                ],
            ),
            (
                "IndexKeyCols",
                [
                    {
                        "schema_name": "dbo",
                        "table_name": "Orders",
                        "index_a": "IX_Orders_CustomerId",
                        "type_a": "NONCLUSTERED",
                        "index_b": "IX_Orders_CustomerId_Alt",
                        "type_b": "NONCLUSTERED",
                        "key_columns": "CustomerId",
                    }
                ],
            ),
            (
                "sys.dm_db_index_physical_stats",
                [
                    {
                        "schema_name": "dbo",
                        "table_name": "Orders",
                        "index_name": "IX_Orders_CreatedAt",
                        "avg_fragmentation_in_percent": 68.5,
                        "page_count": 4200,
                    }
                ],
            ),
            (
                "sys.dm_db_index_usage_stats",
                [
                    {
                        "schema_name": "dbo",
                        "table_name": "Orders",
                        "index_name": "IX_Orders_Unused",
                        "user_seeks": 0,
                        "user_scans": 0,
                        "user_lookups": 0,
                        "user_updates": 14,
                    }
                ],
            ),
            (
                "sys.database_automatic_tuning_options",
                [
                    {
                        "name": "CREATE_INDEX",
                        "desired_state_desc": "ON",
                        "actual_state_desc": "ON",
                        "reason_desc": None,
                    }
                ],
            ),
            (
                "sys.dm_db_tuning_recommendations",
                [{"recommendation_count": 2}],
            ),
            (
                "sys.dm_db_resource_stats",
                [
                    {
                        "end_time": "2026-03-30T12:00:00Z",
                        "avg_cpu_percent": 11.2,
                        "avg_data_io_percent": 14.8,
                        "avg_log_write_percent": 8.1,
                        "avg_memory_usage_percent": 52.0,
                        "xtp_storage_percent": 0.0,
                        "max_worker_percent": 4.0,
                        "max_session_percent": 10.0,
                        "dtu_limit": 100,
                    }
                ],
            ),
            (
                "sys.database_files",
                [
                    {
                        "name": "appdb",
                        "type_desc": "ROWS",
                        "size_mb": 512.0,
                        "max_size_mb": 2048.0,
                    }
                ],
            ),
        ],
        query_store_payload={
            "enabled": True,
            "status": {
                "actual_state_desc": "READ_WRITE",
                "desired_state_desc": "READ_WRITE",
            },
        },
    )

    payload = await service.analyze("appdb", "all")

    assert payload["database_name"] == "appdb"
    checks = payload["checks"]
    expected_checks = {
        "index",
        "query_store",
        "tuning",
        "resource",
        "storage",
        "buffer",
        "connection",
        "constraint",
        "replication",
        "identity",
        "statistics",
    }
    assert expected_checks.issubset(checks)
    assert query_store.calls == ["appdb"]


@pytest.mark.asyncio
async def test_fetch_optional_rows_sanitizes_driver_errors():
    service, _, _ = build_service(
        errors=[
            (
                "SELECT 1",
                RuntimeError(
                    "SERVER=tcp:prod.database.windows.net;DATABASE=appdb;UID=sa;PWD=secret!;"
                ),
            )
        ]
    )

    rows, error = await service._fetch_optional_rows("appdb", "SELECT 1")

    assert rows == []
    assert error is not None
    assert "secret!" not in error
    assert "UID=sa" not in error
    assert "prod.database.windows.net" not in error


@pytest.mark.asyncio
async def test_threshold_evaluation_for_buffer_and_connection_checks():
    service, _, _ = build_service(
        responses=[
            (
                "Buffer cache hit ratio",
                [
                    {
                        "buffer_cache_hit_ratio": 89.9,
                        "page_life_expectancy_seconds": 250,
                    }
                ],
            ),
            (
                "Page life expectancy",
                [
                    {
                        "buffer_cache_hit_ratio": 89.9,
                        "page_life_expectancy_seconds": 250,
                    }
                ],
            ),
            (
                "sys.dm_exec_sessions",
                [
                    {
                        "total_sessions": 81,
                        "active_requests": 2,
                        "idle_with_open_transaction": 0,
                        "idle_sessions": 79,
                        "session_limit": 100,
                    }
                ],
            ),
        ]
    )

    buffer_payload = await service.analyze("appdb", "buffer")
    connection_payload = await service.analyze("appdb", "connection")

    buffer_check = buffer_payload["checks"]["buffer"]
    connection_check = connection_payload["checks"]["connection"]

    assert buffer_check["status"] == "critical"
    assert any("buffer cache hit ratio" in finding.lower() for finding in buffer_check["findings"])
    assert any("page life expectancy" in finding.lower() for finding in buffer_check["findings"])

    assert connection_check["status"] == "warning"
    assert any("session" in finding.lower() for finding in connection_check["findings"])


@pytest.mark.asyncio
async def test_duplicate_indexes_are_reported_as_a_warning():
    service, _, _ = build_service(
        responses=[
            (
                "IndexKeyCols",
                [
                    {
                        "schema_name": "dbo",
                        "table_name": "Orders",
                        "index_a": "IX_Orders_CustomerId",
                        "type_a": "NONCLUSTERED",
                        "index_b": "IX_Orders_CustomerId_Alt",
                        "type_b": "NONCLUSTERED",
                        "key_columns": "CustomerId",
                    }
                ],
            ),
            (
                "sys.dm_db_index_physical_stats",
                [],
            ),
            (
                "sys.dm_db_index_usage_stats",
                [],
            ),
        ]
    )

    payload = await service.analyze("appdb", "index")
    index_check = payload["checks"]["index"]

    assert index_check["status"] == "warning"
    assert any("duplicate" in finding.lower() for finding in index_check["findings"])


@pytest.mark.asyncio
async def test_unavailable_dmv_degrades_gracefully():
    service, _, _ = build_service(
        errors=[
            (
                "Buffer cache hit ratio",
                RuntimeError("The requested DMV is not available in this tier."),
            ),
        ]
    )

    payload = await service.analyze("appdb", "buffer")
    buffer_check = payload["checks"]["buffer"]

    assert buffer_check["status"] == "warning"
    assert buffer_check["findings"]
    assert "unavailable" in " ".join(str(item).lower() for item in buffer_check["findings"])


@pytest.mark.asyncio
async def test_statistics_health_warns_on_stale_and_high_modification():
    service, _, _ = build_service(
        responses=[
            (
                "sys.dm_db_stats_properties",
                [
                    {
                        "schema_name": "dbo",
                        "table_name": "Orders",
                        "stats_id": 1,
                        "statistics_name": "IX_Orders_Date",
                        "last_updated": "2026-03-01",
                        "total_rows": 100000,
                        "modification_counter": 25000,
                        "modification_pct": 25.0,
                    },
                    {
                        "schema_name": "dbo",
                        "table_name": "Customers",
                        "stats_id": 2,
                        "statistics_name": "_WA_Sys_Name",
                        "last_updated": "2026-03-20",
                        "total_rows": 50000,
                        "modification_counter": 100,
                        "modification_pct": 0.2,
                    },
                ],
            ),
        ]
    )

    payload = await service.analyze("appdb", "statistics")
    stats_check = payload["checks"]["statistics"]

    assert_threshold_payload(stats_check)
    assert stats_check["status"] == "warning"
    assert stats_check["details"]["high_modification_count"] == 1
    assert stats_check["details"]["stale_count"] == 2


@pytest.mark.asyncio
async def test_resource_health_governance_comparison_warns():
    service, _, _ = build_service(
        responses=[
            (
                "sys.dm_db_resource_stats",
                [
                    {
                        "end_time": "2026-03-30T12:00:00Z",
                        "avg_cpu_percent": 85.0,
                        "avg_data_io_percent": 90.0,
                        "avg_log_write_percent": 10.0,
                        "avg_memory_usage_percent": 82.0,
                        "xtp_storage_percent": 0.0,
                        "max_worker_percent": 4.0,
                        "max_session_percent": 10.0,
                        "dtu_limit": 100,
                    }
                ],
            ),
            (
                "sys.dm_user_db_resource_governance",
                [
                    {
                        "primary_max_cpu_percent": 100,
                        "primary_max_log_rate_per_db_in_bytes_per_second": 50000000,
                        "primary_group_max_io": 5000,
                        "pool_max_io": 10000,
                        "max_db_memory": 2097152,
                        "max_workers_per_query": 64,
                        "checkpoint_rate_mbps": 200,
                    }
                ],
            ),
        ]
    )

    payload = await service.analyze("appdb", "resource")
    resource_check = payload["checks"]["resource"]

    assert_threshold_payload(resource_check)
    assert resource_check["details"].get("governance_limits")
    findings_text = " ".join(resource_check["findings"])
    assert "governance" in findings_text.lower() or "peaked" in findings_text.lower()
