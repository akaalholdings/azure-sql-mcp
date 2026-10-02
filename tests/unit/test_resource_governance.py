from __future__ import annotations

import pytest

from azure_sql_mcp.resource_governance import ResourceGovernanceService
from azure_sql_mcp.resource_governance import log_rate_caps
from azure_sql_mcp.resource_governance import summarize_resource_metric
from tests.azure_dmv_contract import documented_row


class FakeExecutor:
    def __init__(self, results_by_call: list[list[dict]] | None = None):
        self._calls: list[list[dict]] = results_by_call or [[]]
        self._call_idx = 0

    async def fetch_all(self, database_name: str, query: str) -> list[dict]:
        if self._call_idx < len(self._calls):
            result = self._calls[self._call_idx]
            self._call_idx += 1
            return result
        return []


@pytest.mark.asyncio
async def test_get_io_stats_warns_on_high_latency():
    rows = [
        {"file_name": "data", "file_type": "ROWS", "physical_name": "/data/file.mdf", "file_size_mb": 1024, "num_of_reads": 10000, "num_of_writes": 5000, "read_mb": 500, "write_mb": 200, "avg_read_latency_ms": 25.0, "avg_write_latency_ms": 5.0, "io_stall_read_ms": 250000, "io_stall_write_ms": 25000, "total_io_stall_ms": 275000},
        {"file_name": "log", "file_type": "LOG", "physical_name": "/data/file.ldf", "file_size_mb": 256, "num_of_reads": 100, "num_of_writes": 20000, "read_mb": 5, "write_mb": 1000, "avg_read_latency_ms": 2.0, "avg_write_latency_ms": 3.0, "io_stall_read_ms": 200, "io_stall_write_ms": 60000, "total_io_stall_ms": 60200},
    ]
    service = ResourceGovernanceService(FakeExecutor([rows]))
    result = await service.get_io_stats("testdb")

    assert len(result["files"]) == 2
    assert len(result["warnings"]) == 1  # data file read latency > 20ms
    assert result["warnings"][0]["type"] == "high_read_latency"
    assert result["warnings"][0]["file"] == "data"


@pytest.mark.asyncio
async def test_get_io_stats_no_warnings():
    rows = [
        {"file_name": "data", "file_type": "ROWS", "physical_name": "f", "file_size_mb": 100, "num_of_reads": 100, "num_of_writes": 50, "read_mb": 10, "write_mb": 5, "avg_read_latency_ms": 5.0, "avg_write_latency_ms": 3.0, "io_stall_read_ms": 500, "io_stall_write_ms": 150, "total_io_stall_ms": 650},
    ]
    service = ResourceGovernanceService(FakeExecutor([rows]))
    result = await service.get_io_stats("testdb")
    assert result["warnings"] == []


GOVERNANCE_DMV = "sys.dm_user_db_resource_governance"


def governance_row(**overrides) -> dict:
    values = {
        "database_id": 5,
        "slo_name": "SQLDB_OP_GP_GEN5_2",
        "cpu_limit": 2,
        "primary_group_max_cpu": 100.0,
        "primary_max_log_rate": 50331648,
        "pool_max_log_rate": 104857600,
        "instance_max_log_rate": 125829120,
        "primary_group_max_io": 640,
        "max_sessions": 30000,
    }
    values.update(overrides)
    return documented_row(GOVERNANCE_DMV, **values)


@pytest.mark.asyncio
async def test_get_resource_limits_documented_caps():
    slo_rows = [
        {"edition": "GeneralPurpose", "service_objective": "GP_Gen5_2", "elastic_pool_name": None},
    ]
    service = ResourceGovernanceService(FakeExecutor([[governance_row()], slo_rows]))
    result = await service.get_resource_limits("testdb")

    assert result["service_objective"]["service_objective"] == "GP_Gen5_2"
    limits = result["governance_limits"]
    assert limits["primary_group_max_cpu"] == 100.0  # SELECT * stays raw
    assert limits["max_log_rate_mb_per_sec"] == 48.0  # 50331648 / 1048576
    assert limits["pool_max_log_rate_mb_per_sec"] == 100.0
    assert limits["instance_max_log_rate_mb_per_sec"] == 120.0
    assert limits["effective_log_rate_cap_mb_per_sec"] == 48.0
    assert "result_status" not in result


def test_log_rate_caps_take_the_smallest_known_cap():
    assert log_rate_caps({"primary_max_log_rate": 0, "pool_max_log_rate": 31457280, "instance_max_log_rate": None}) == {
        "max_log_rate_mb_per_sec": None,
        "pool_max_log_rate_mb_per_sec": 30.0,
        "instance_max_log_rate_mb_per_sec": None,
        "effective_log_rate_cap_mb_per_sec": 30.0,
    }
    assert log_rate_caps({})["effective_log_rate_cap_mb_per_sec"] is None


@pytest.mark.asyncio
async def test_get_resource_limits_permission_denied_is_unavailable_with_hint():
    executor = RecordingExecutor(
        {
            "dm_user_db_resource_governance": PermissionError("VIEW SERVER STATE permission denied"),
            "DATABASEPROPERTYEX": [
                {"edition": "GeneralPurpose", "service_objective": "ElasticPool", "elastic_pool_name": "pool-a"}
            ],
        }
    )

    result = await ResourceGovernanceService(executor).get_resource_limits("appdb")

    assert result["result_status"] == "unavailable"
    assert "##MS_ServerStateReader##" in result["result_status_reason"]
    assert "elastic pool" in result["result_status_reason"]
    assert result["service_objective"]["service_objective"] == "ElasticPool"
    assert result["governance_limits"] == {}
    assert result["available"] is False


@pytest.mark.asyncio
async def test_unreadable_governance_keeps_a_performance_case_from_turning_healthy():
    # get_resource_limits used to raise here, which the case recorded as a gap. The
    # unavailable payload must stay a gap: missing limits are not an all-clear.
    from tests.unit.test_performance_workflows import _service

    limits = ResourceGovernanceService(
        RecordingExecutor(
            {
                "dm_user_db_resource_governance": PermissionError("permission denied"),
                "DATABASEPROPERTYEX": [
                    {"edition": "GeneralPurpose", "service_objective": "ElasticPool", "elastic_pool_name": "pool-a"}
                ],
            }
        )
    )
    service, _, _, _ = _service()
    case = service.start_case("appdb", "SELECT id FROM dbo.Items")

    result = await service.collect_case_evidence(
        case.case_id,
        "appdb",
        "SELECT id FROM dbo.Items",
        {"resource_limits": lambda: limits.get_resource_limits("appdb")},
        window_minutes=15,
    )

    section = result["sections"]["resource_limits"]
    assert section["available"] is False
    assert section["complete"] is False
    assert result["outcome"] != "healthy"


@pytest.mark.asyncio
async def test_service_objective_is_read_without_owner_rights():
    # sys.database_service_objectives shows no row to a non-owner; DATABASEPROPERTYEX does.
    executor = RecordingExecutor({"dm_user_db_resource_governance": [governance_row()]})

    await ResourceGovernanceService(executor).get_resource_limits("appdb")

    assert any("DATABASEPROPERTYEX" in query for _, query, _ in executor.calls)


@pytest.mark.asyncio
async def test_memory_is_informational_not_a_warning():
    rows = [
        {"end_time": f"2026-04-01T10:{i:02d}:00", "avg_cpu_percent": 12.0, "avg_memory_usage_percent": 99.0,
         "avg_instance_memory_percent": 97.0, "max_worker_percent": 2.0}
        for i in range(40)
    ]
    service = ResourceGovernanceService(FakeExecutor([rows]))

    result = await service.get_resource_stats_history("testdb", window_minutes=60)

    assert result["warnings"] == []
    assert result["summary"]["avg_memory_usage_percent"]["max"] == 99.0  # the data stays
    informational = {item["type"]: item for item in result["informational"]}
    assert set(informational) == {"memory_avg_memory_usage_percent", "memory_avg_instance_memory_percent"}
    assert "expected" in informational["memory_avg_memory_usage_percent"]["message"]


def test_summarize_resource_metric_reports_p95_and_time_above():
    values = [10.0] * 200 + [85.0] * 20 + [99.0] * 20

    summary = summarize_resource_metric(values, sample_seconds=15)

    assert summary["p95"] == 99.0
    assert summary["max"] == 99.0
    assert summary["samples_above_80_pct"] == 40
    assert summary["minutes_above_80"] == 10.0
    assert summary["minutes_above_95"] == 5.0
    assert summarize_resource_metric([50.0], sample_seconds=None)["minutes_above_80"] is None


@pytest.mark.asyncio
async def test_recent_history_reads_every_documented_pressure_column():
    executor = RecordingExecutor({"dm_db_resource_stats": [{"avg_cpu_percent": 5.0, "max_session_percent": 90.0}]})

    result = await ResourceGovernanceService(executor).get_resource_stats_history("appdb", 60)

    query = executor.calls[0][1]
    for column in ("xtp_storage_percent", "avg_instance_memory_percent", "max_session_percent"):
        assert column in query
    assert result["summary"]["max_session_percent"]["p95"] == 90.0
    assert result["summary"]["avg_cpu_percent"]["minutes_above_80"] == 0.0


@pytest.mark.asyncio
async def test_get_resource_stats_history_warns_sustained_pressure():
    # Create 10 samples, 7 with CPU > 80% → should warn (70% > 30%)
    rows = []
    for i in range(10):
        cpu = 85.0 if i < 7 else 30.0
        rows.append({
            "end_time": f"2026-04-01T10:{i:02d}:00",
            "avg_cpu_percent": cpu,
            "avg_data_io_percent": 10.0,
            "avg_log_write_percent": 5.0,
            "avg_memory_usage_percent": 40.0,
            "max_worker_percent": 20.0,
            "max_session_percent": 15.0,
            "avg_instance_cpu_percent": cpu,
        })
    service = ResourceGovernanceService(FakeExecutor([rows]))
    result = await service.get_resource_stats_history("testdb", window_minutes=60)

    assert result["sample_count"] == 10
    assert "avg_cpu_percent" in result["summary"]
    assert result["summary"]["avg_cpu_percent"]["samples_above_80_pct"] == 7

    warning_types = [w["type"] for w in result["warnings"]]
    assert "sustained_avg_cpu_percent" in warning_types
    # Other metrics should not warn
    assert "sustained_avg_data_io_percent" not in warning_types


@pytest.mark.asyncio
async def test_get_resource_stats_history_no_warnings():
    rows = [
        {"end_time": "2026-04-01T10:00:00", "avg_cpu_percent": 20.0, "avg_data_io_percent": 5.0, "avg_log_write_percent": 2.0, "avg_memory_usage_percent": 30.0, "max_worker_percent": 10.0, "max_session_percent": 5.0, "avg_instance_cpu_percent": 20.0},
    ]
    service = ResourceGovernanceService(FakeExecutor([rows]))
    result = await service.get_resource_stats_history("testdb")
    assert result["warnings"] == []


class RecordingExecutor:
    def __init__(self, responses: dict[str, object] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list[tuple[str, str, list | None]] = []

    async def fetch_all(self, database_name, query, params=None, **kwargs):
        self.calls.append((database_name, query, list(params) if params is not None else None))
        for marker, result in self.responses.items():
            if marker in query:
                if isinstance(result, Exception):
                    raise result
                return result
        return []


@pytest.mark.asyncio
async def test_governance_limits_are_scoped_to_the_current_database():
    executor = RecordingExecutor({"dm_user_db_resource_governance": [{"database_id": 5, "slo_name": "GP_S_Gen5_2"}]})

    result = await ResourceGovernanceService(executor).get_resource_limits("appdb")

    governance_query = next(q for _, q, _ in executor.calls if "dm_user_db_resource_governance" in q)
    # In an elastic pool the view lists every pool database.
    assert "WHERE database_id = DB_ID()" in governance_query
    assert result["governance_limits"]["slo_name"] == "GP_S_Gen5_2"


@pytest.mark.asyncio
async def test_unreadable_service_objective_is_a_warning_not_silence():
    executor = RecordingExecutor(
        {
            "dm_user_db_resource_governance": [{"database_id": 5}],
            "database_service_objectives": PermissionError("denied"),
        }
    )

    result = await ResourceGovernanceService(executor).get_resource_limits("appdb")

    assert [w["type"] for w in result["warnings"]] == ["service_objective_unavailable"]


@pytest.mark.asyncio
async def test_short_windows_read_recent_samples_from_the_database():
    executor = RecordingExecutor({"dm_db_resource_stats": [{"avg_cpu_percent": 12.5}]})

    result = await ResourceGovernanceService(executor).get_resource_stats_history("appdb", 45)

    assert result["source"] == "recent"
    assert result["granularity"] == "15_second_samples"
    assert executor.calls[0][0] == "appdb"
    assert "result_status" not in result


@pytest.mark.asyncio
async def test_long_windows_read_hourly_buckets_from_master():
    executor = RecordingExecutor({"sys.resource_stats": [{"hour_start_utc": "2026-09-30T10:00:00", "avg_cpu_percent": 40.0, "max_worker_percent": 12.0}]})

    result = await ResourceGovernanceService(executor).get_resource_stats_history(
        "appdb", 7 * 24 * 60, master_available=True
    )

    database, query, params = executor.calls[0]
    assert database == "master"
    assert "GROUP BY DATEADD(HOUR" in query
    assert params == ["appdb", 10080]
    assert result["source"] == "long_term"
    assert result["summary"]["max_worker_percent"]["max"] == 12.0


@pytest.mark.asyncio
async def test_long_window_without_master_is_a_precondition_with_recent_data():
    executor = RecordingExecutor({"dm_db_resource_stats": [{"avg_cpu_percent": 5.0}]})

    result = await ResourceGovernanceService(executor).get_resource_stats_history("appdb", 1440)

    assert result["result_status"] == "precondition"
    assert "master" in result["remediation"]
    assert result["source"] == "recent"
    assert all(database == "appdb" for database, _, _ in executor.calls)


@pytest.mark.asyncio
async def test_resource_history_rejects_unknown_sources_and_clamps_windows():
    service = ResourceGovernanceService(RecordingExecutor())
    with pytest.raises(ValueError):
        await service.get_resource_stats_history("appdb", 60, source="weekly")

    result = await service.get_resource_stats_history("appdb", 10**9, master_available=True)
    assert result["window_minutes"] == 20160
