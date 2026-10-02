from __future__ import annotations

from datetime import UTC
from datetime import datetime
from typing import Any

import pytest

from azure_sql_mcp.health import HealthService
from tests.azure_dmv_contract import StrictDmvExecutor
from tests.azure_dmv_contract import documented_row


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
        "query_store",
        "tuning",
        "resource",
        "storage",
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
async def test_threshold_evaluation_for_connection_check():
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
                    }
                ],
            ),
            ("max_sessions", [{"max_sessions": 100, "primary_group_max_workers": 50}]),
        ]
    )

    connection_payload = await service.analyze("appdb", "connection")

    connection_check = connection_payload["checks"]["connection"]

    assert connection_check["status"] == "warning"
    assert connection_check["findings"] == ["Session usage is 81.0% of the limit (81/100)."]


@pytest.mark.parametrize("retired_check", ["index", "buffer"])
def test_legacy_query_health_classifiers_are_rejected(retired_check: str):
    service, _, _ = build_service()

    with pytest.raises(ValueError, match="collect_performance_evidence"):
        service._parse_requested_checks(retired_check)


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


GOVERNANCE_DMV = "sys.dm_user_db_resource_governance"


def governance_row(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "database_id": 5,
        "slo_name": "SQLDB_OP_GP_GEN5_2",
        "cpu_limit": 2,
        "primary_group_max_cpu": 50.0,
        "primary_max_log_rate": 50331648,
        "pool_max_log_rate": 104857600,
        "instance_max_log_rate": 125829120,
        "primary_group_max_io": 640,
        "primary_group_max_workers": 200,
        "max_sessions": 30000,
        "max_db_memory": 9741328,
        "checkpoint_rate_mbps": 200,
        "replica_role": 0,
    }
    values.update(overrides)
    return documented_row(GOVERNANCE_DMV, **values)


def resource_rows(count: int = 240, **metrics: float) -> list[dict[str, Any]]:
    """``count`` 15-second samples, newest first; ``metrics`` override the idle defaults."""

    rows = []
    for index in range(count):
        row = {
            "end_time": f"2026-10-02T10:{59 - index // 4:02d}:{45 - 15 * (index % 4):02d}",
            "avg_cpu_percent": 12.0,
            "avg_data_io_percent": 3.0,
            "avg_log_write_percent": 2.0,
            "avg_memory_usage_percent": 40.0,
            "avg_instance_memory_percent": 45.0,
            "max_worker_percent": 1.0,
            "max_session_percent": 1.0,
            "xtp_storage_percent": 0.0,
            "avg_instance_cpu_percent": 15.0,
        }
        row.update(metrics)
        rows.append(row)
    return rows


def issued(executor: FakeExecutor) -> str:
    return "\n".join(query for _, query, _ in executor.calls)


@pytest.mark.asyncio
async def test_resource_check_does_not_double_normalize_cpu():
    # avg_cpu_percent is already a percentage of the CPU limit; dividing by
    # primary_group_max_cpu again reported 120% of a limit that was not reached.
    service, _, _ = build_service(
        responses=[
            ("sys.dm_db_resource_stats", resource_rows(avg_cpu_percent=60.0)),
            (GOVERNANCE_DMV, [governance_row(primary_group_max_cpu=50.0)]),
        ]
    )

    check = (await service.analyze("appdb", "resource"))["checks"]["resource"]

    assert_threshold_payload(check)
    assert check["status"] == "pass"
    assert not any("governance limit" in finding for finding in check["findings"])


@pytest.mark.asyncio
async def test_sustained_pressure_is_reported_in_absolute_units():
    service, _, _ = build_service(
        responses=[
            ("sys.dm_db_resource_stats", resource_rows(avg_log_write_percent=98.0, avg_cpu_percent=85.0)),
            (GOVERNANCE_DMV, [governance_row()]),
        ]
    )

    check = (await service.analyze("appdb", "resource"))["checks"]["resource"]

    assert check["status"] == "critical"
    findings = " ".join(check["findings"])
    assert "Log write" in findings and "98.0% of 48.0 MB/s" in findings
    assert "of 2 vCores" in findings
    assert check["details"]["governance_limits"]["effective_log_rate_cap_mb_per_sec"] == 48.0


@pytest.mark.asyncio
async def test_governance_limits_project_documented_columns_for_this_database():
    service, executor, _ = build_service(responses=[(GOVERNANCE_DMV, [governance_row()])])

    limits = await service._fetch_governance_limits("appdb")

    assert "WHERE database_id = DB_ID()" in issued(executor)
    assert limits["primary_max_log_rate"] == 50331648
    assert limits["max_log_rate_mb_per_sec"] == 48.0
    assert limits["pool_max_log_rate_mb_per_sec"] == 100.0
    assert limits["max_sessions"] == 30000
    # Documented as internal use only.
    assert "max_db_memory" not in limits and "checkpoint_rate_mbps" not in limits


@pytest.mark.asyncio
async def test_warm_cache_memory_is_not_pressure():
    # Microsoft documents memory near 100% after ramp-up as expected on Azure SQL Database.
    service, _, _ = build_service(
        responses=[
            ("sys.dm_db_resource_stats", resource_rows(avg_memory_usage_percent=99.2, avg_instance_memory_percent=98.0)),
            (GOVERNANCE_DMV, [governance_row()]),
        ]
    )

    check = (await service.analyze("appdb", "resource"))["checks"]["resource"]

    assert check["status"] == "pass"
    assert check["findings"] == []
    informational = check["details"]["informational"]
    assert {item["metric"] for item in informational} == {"avg_memory_usage_percent", "avg_instance_memory_percent"}
    assert all("expected" in item["note"] for item in informational)


@pytest.mark.asyncio
async def test_resource_check_reads_the_full_hour():
    rows = resource_rows()
    rows[0]["avg_cpu_percent"] = 96.0
    service, executor, _ = build_service(responses=[("sys.dm_db_resource_stats", rows)])

    check = (await service.analyze("appdb", "resource"))["checks"]["resource"]

    assert "TOP (12)" not in issued(executor)
    assert "DATEADD(MINUTE, -60" in issued(executor)
    # One 15-second spike is reported but never escalates on its own.
    assert check["status"] == "pass"
    assert check["details"]["peak_usage"]["avg_cpu_percent"] == 96.0
    assert check["details"]["summary"]["avg_cpu_percent"]["p95"] == 12.0
    assert len(check["details"]["recent_intervals"]) == 12


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [8, 19])
async def test_short_window_spike_does_not_escalate(count: int):
    # After a failover, scale operation or serverless resume the view holds only a
    # few minutes; below 20 samples the nearest-rank p95 is the single maximum.
    rows = resource_rows(count=count)
    rows[0]["avg_cpu_percent"] = 99.0
    service, _, _ = build_service(responses=[("sys.dm_db_resource_stats", rows)])

    check = (await service.analyze("appdb", "resource"))["checks"]["resource"]

    assert check["status"] == "pass"
    assert check["findings"] == []
    assert check["details"]["peak_usage"]["avg_cpu_percent"] == 99.0
    notes = [item for item in check["details"]["informational"] if item["metric"] == "sample_window"]
    assert notes and f"{count} samples" in notes[0]["note"]


@pytest.mark.asyncio
async def test_sustained_pressure_in_a_short_window_reports_the_real_window():
    service, _, _ = build_service(
        responses=[("sys.dm_db_resource_stats", resource_rows(count=40, avg_cpu_percent=97.0))]
    )

    check = (await service.analyze("appdb", "resource"))["checks"]["resource"]

    assert check["status"] == "critical"
    assert any("over the last 10.0 min" in finding for finding in check["findings"])
    assert not any("hour" in finding for finding in check["findings"])


@pytest.mark.asyncio
async def test_recent_intervals_keep_the_dtu_and_cpu_limits():
    # 2.6.0 returned dtu_limit in every recent_intervals row; 2.6.1 only adds fields.
    sample = documented_row(
        "sys.dm_db_resource_stats",
        end_time="2026-10-02T10:59:45",
        avg_cpu_percent=12.0,
        dtu_limit=100,
    )
    executor = StrictDmvExecutor({"sys.dm_db_resource_stats": [sample]})
    service = HealthService(executor, FakeQueryStoreService())  # type: ignore[arg-type]

    check = (await service.analyze("appdb", "resource"))["checks"]["resource"]

    row = check["details"]["recent_intervals"][0]
    assert row["dtu_limit"] == 100
    assert "cpu_limit" in row


@pytest.mark.asyncio
async def test_five_minutes_at_the_limit_is_critical_even_with_a_low_p95():
    rows = resource_rows()
    for row in rows[:20]:
        row["max_worker_percent"] = 100.0
    service, _, _ = build_service(responses=[("sys.dm_db_resource_stats", rows)])

    check = (await service.analyze("appdb", "resource"))["checks"]["resource"]

    assert check["status"] == "critical"
    assert any("Workers" in finding and "5.0 min" in finding for finding in check["findings"])


@pytest.mark.asyncio
async def test_replication_suspended_link_is_critical():
    link = documented_row(
        "sys.dm_geo_replication_link_status",
        partner_database="appdb",
        role_desc="PRIMARY",
        replication_state_desc="SUSPENDED",
        replication_lag_sec=0,
    )
    service, _, _ = build_service(responses=[("sys.dm_geo_replication_link_status", [link])])

    check = (await service.analyze("appdb", "replication"))["checks"]["replication"]

    assert check["status"] == "critical"
    assert any("SUSPENDED" in finding for finding in check["findings"])


@pytest.mark.asyncio
async def test_replication_unreadable_is_never_pass():
    service, _, _ = build_service(
        errors=[("sys.dm_geo_replication_link_status", PermissionError("VIEW DATABASE STATE permission denied"))]
    )

    check = (await service.analyze("appdb", "replication"))["checks"]["replication"]

    assert check["status"] == "warning"
    assert check["details"]["available"] is False
    assert any("this is not a pass" in finding for finding in check["findings"])


@pytest.mark.asyncio
async def test_unacknowledged_commits_on_the_primary_are_critical():
    link = documented_row(
        "sys.dm_geo_replication_link_status",
        partner_database="appdb",
        role_desc="PRIMARY",
        replication_state_desc="CATCH_UP",
        replication_lag_sec=5,
        last_replication=datetime(2026, 10, 2, 10, 0, tzinfo=UTC),
        last_commit=datetime(2026, 10, 2, 10, 10, tzinfo=UTC),
    )
    service, _, _ = build_service(responses=[("sys.dm_geo_replication_link_status", [link])])

    check = (await service.analyze("appdb", "replication"))["checks"]["replication"]

    assert check["status"] == "critical"
    assert any("600" in finding for finding in check["findings"])


@pytest.mark.asyncio
async def test_seeding_link_is_information_not_a_failure():
    link = documented_row(
        "sys.dm_geo_replication_link_status",
        partner_database="appdb",
        role_desc="PRIMARY",
        replication_state_desc="SEEDING",
        replication_lag_sec=None,
    )
    service, _, _ = build_service(responses=[("sys.dm_geo_replication_link_status", [link])])

    check = (await service.analyze("appdb", "replication"))["checks"]["replication"]

    assert check["status"] == "pass"
    assert check["details"]["informational"]


@pytest.mark.asyncio
async def test_not_healthy_secondary_replica_is_critical():
    replica = {"synchronization_health_desc": "NOT_HEALTHY", "secondary_lag_seconds": 40, "redo_queue_size": 0}
    service, executor, _ = build_service(responses=[("sys.dm_database_replica_states", [replica])])

    check = (await service.analyze("appdb", "replication"))["checks"]["replication"]

    assert "is_local = 0" in issued(executor)
    assert "availability_groups" not in issued(executor)
    assert check["status"] == "critical"
    assert check["details"]["replicas"] == [replica]


@pytest.mark.asyncio
async def test_connection_session_limit_from_max_sessions():
    service, _, _ = build_service(
        responses=[
            ("AS idle_with_open_transaction", [{"total_sessions": 300, "active_requests": 4, "idle_sessions": 290}]),
            ("max_sessions", [{"max_sessions": 30000, "primary_group_max_workers": 200}]),
            ("sys.dm_db_resource_stats", resource_rows()),
        ]
    )

    check = (await service.analyze("appdb", "connection"))["checks"]["connection"]

    assert check["status"] == "pass"
    assert check["details"]["session_limit"] == 30000
    assert check["details"]["worker_limit"] == 200
    assert check["details"]["session_limit_utilization_percent"] == 1.0
    assert check["details"]["max_worker_percent_peak"] == 1.0


@pytest.mark.asyncio
async def test_connection_worker_pressure_comes_from_resource_stats():
    service, _, _ = build_service(
        responses=[
            ("AS idle_with_open_transaction", [{"total_sessions": 40}]),
            ("max_sessions", [{"max_sessions": 30000, "primary_group_max_workers": 200}]),
            ("sys.dm_db_resource_stats", resource_rows(max_worker_percent=97.0)),
        ]
    )

    check = (await service.analyze("appdb", "connection"))["checks"]["connection"]

    assert check["status"] == "critical"
    assert any("of 200 workers" in finding for finding in check["findings"])


@pytest.mark.asyncio
async def test_connection_limits_permission_failure_keeps_the_check_with_a_hint():
    service, _, _ = build_service(
        responses=[
            ("AS idle_with_open_transaction", [{"total_sessions": 40}]),
            ("sys.dm_db_resource_stats", resource_rows()),
        ],
        errors=[("max_sessions", PermissionError("VIEW SERVER STATE permission denied"))],
    )

    check = (await service.analyze("appdb", "connection"))["checks"]["connection"]

    # Session and worker pressure still come from sys.dm_db_resource_stats.
    assert check["status"] == "pass"
    assert check["details"]["session_limit"] is None
    assert "##MS_ServerStateReader##" in check["details"]["session_limit_unavailable_reason"]


@pytest.mark.asyncio
async def test_connection_with_no_readable_limit_source_is_not_a_pass():
    service, _, _ = build_service(
        responses=[("AS idle_with_open_transaction", [{"total_sessions": 40}])],
        errors=[
            ("max_sessions", PermissionError("denied")),
            ("sys.dm_db_resource_stats", PermissionError("denied")),
        ],
    )

    check = (await service.analyze("appdb", "connection"))["checks"]["connection"]

    assert check["status"] == "warning"
    assert any("this is not a pass" in finding for finding in check["findings"])
    assert len(check["findings"]) == 1


@pytest.mark.asyncio
async def test_connection_with_unreadable_pressure_is_not_a_pass():
    # max_sessions (30000) almost never trips; worker percent is the signal for
    # worker exhaustion, so losing it must not read as a pass.
    service, _, _ = build_service(
        responses=[
            ("AS idle_with_open_transaction", [{"total_sessions": 40}]),
            ("max_sessions", [{"max_sessions": 30000, "primary_group_max_workers": 200}]),
        ],
        errors=[("sys.dm_db_resource_stats", TimeoutError("Query timeout expired"))],
    )

    check = (await service.analyze("appdb", "connection"))["checks"]["connection"]

    assert check["status"] == "warning"
    assert "Query timeout expired" in check["details"]["pressure_unavailable_reason"]
    assert any("worker" in finding and "this is not a pass" in finding for finding in check["findings"])
    assert check["details"]["session_limit"] == 30000
