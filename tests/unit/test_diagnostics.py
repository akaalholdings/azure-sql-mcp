from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from azure_sql_mcp.diagnostics import DiagnosticQueryService


class FakeExecutor:
    def __init__(
        self,
        results_by_call: list[list[dict[str, Any]]] | None = None,
        errors: list[tuple[str, Exception]] | None = None,
    ):
        self._results = results_by_call or []
        self._call_idx = 0
        self.errors = errors or []
        self.calls: list[tuple[str, str, tuple[Any, ...] | None]] = []

    async def fetch_all(
        self,
        database_name: str,
        query: str,
        params: Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        normalized_params = tuple(params) if params is not None else None
        self.calls.append((database_name, query, normalized_params))
        for needle, error in self.errors:
            if needle in query:
                raise error
        if self._call_idx < len(self._results):
            rows = self._results[self._call_idx]
            self._call_idx += 1
            return rows
        return []


@pytest.mark.asyncio
async def test_get_database_configuration_collects_expected_sections() -> None:
    executor = FakeExecutor(
        [
            [{"server_name": "sql", "sql_version": "Microsoft SQL Azure"}],
            [{"name": "cost threshold", "value_in_use": 5}],
            [{"database_name": "appdb", "compatibility_level": 170}],
            [{"name": "MAXDOP", "value": 8}],
            [{"actual_state_desc": "READ_WRITE"}],
            [{"name": "FORCE_LAST_GOOD_PLAN", "actual_state_desc": "ON"}],
            [{"partner_database": "appdb-secondary"}],
            [{"database_edition": "GeneralPurpose", "service_objective": "GP_Gen5_2"}],
        ]
    )
    service = DiagnosticQueryService(executor)

    result = await service.get_database_configuration("appdb")

    assert result["database_name"] == "appdb"
    assert result["version"]["rows"][0]["server_name"] == "sql"
    assert result["database_properties"]["rows"][0]["compatibility_level"] == 170
    assert result["database_scoped_configurations"]["rows"][0]["name"] == "MAXDOP"
    assert result["query_store_options"]["available"] is True
    assert result["automatic_tuning_options"]["rows"][0]["name"] == "FORCE_LAST_GOOD_PLAN"
    assert result["geo_replication_links"]["rows"][0]["partner_database"] == "appdb-secondary"
    assert result["azure_properties"]["rows"][0]["service_objective"] == "GP_Gen5_2"
    assert result["coverage"]["status"] == "complete"
    assert len(executor.calls) == 8


@pytest.mark.asyncio
async def test_get_storage_diagnostics_warns_on_log_vlf_and_file_usage() -> None:
    executor = FakeExecutor(
        [
            [{"database_size_mb": 900.0}],
            [
                {
                    "file_name": "appdb",
                    "total_size_mb": 1000.0,
                    "available_space_mb": 40.0,
                }
            ],
            [{"used_log_space_percent": 96.0}],
            [{"vlf_count": 250}],
            [{"vlf_status": 2}],
        ]
    )
    service = DiagnosticQueryService(executor)

    result = await service.get_storage_diagnostics("appdb")

    assert result["database_size"]["rows"][0]["database_size_mb"] == 900.0
    assert result["file_space"]["rows"][0]["file_name"] == "appdb"
    assert result["log_space"]["rows"][0]["used_log_space_percent"] == 96.0
    assert result["vlf_counts"]["rows"][0]["vlf_count"] == 250
    assert {warning["type"] for warning in result["warnings"]} == {
        "critical_log_space_usage",
        "high_vlf_count",
        "critical_file_usage",
    }
    assert result["coverage"]["status"] == "complete"


@pytest.mark.asyncio
async def test_get_connection_diagnostics_includes_ip_counts_and_input_buffer() -> None:
    executor = FakeExecutor(
        [
            [{"client_net_address": "10.0.0.4", "connection_count": 3}],
            [{"total_sessions": 4, "running_sessions": 1}],
            [{"session_id": 55, "input_buffer": "SELECT 1"}],
        ]
    )
    service = DiagnosticQueryService(executor)

    result = await service.get_connection_diagnostics(
        "appdb",
        limit=500,
        include_input_buffer=True,
    )

    assert result["limit"] == 100
    assert result["connection_counts_by_ip"]["rows"][0]["client_net_address"] == "10.0.0.4"
    assert result["input_buffers"]["rows"][0]["input_buffer"] == "SELECT 1"
    assert "TOP (100)" in executor.calls[0][1]
    assert "sys.dm_exec_input_buffer" in executor.calls[2][1]


@pytest.mark.asyncio
async def test_get_connection_diagnostics_can_skip_input_buffer() -> None:
    executor = FakeExecutor(
        [
            [{"client_net_address": "10.0.0.4", "connection_count": 3}],
            [{"total_sessions": 4, "running_sessions": 1}],
        ]
    )
    service = DiagnosticQueryService(executor)

    result = await service.get_connection_diagnostics("appdb")

    assert result["input_buffers"]["skipped"] is True
    assert result["coverage"]["status"] == "partial"
    assert result["coverage"]["skipped_sections"] == ["input_buffers"]
    assert len(executor.calls) == 2


@pytest.mark.asyncio
async def test_get_top_cached_queries_clamps_limit_and_omits_raw_plan_xml() -> None:
    executor = FakeExecutor(
        [
            [
                {
                    "query_hash": "0x01",
                    "execution_count": 20,
                    "has_missing_index": True,
                    "query_text_preview": "SELECT * FROM dbo.Orders",
                }
            ]
        ]
    )
    service = DiagnosticQueryService(executor)

    result = await service.get_top_cached_queries(
        "appdb",
        sort_by="total_logical_reads",
        limit=500,
    )

    assert result["limit"] == 100
    assert result["cached_queries"]["rows"][0]["has_missing_index"] is True
    assert result["coverage"]["status"] == "complete"
    assert "TOP (100)" in executor.calls[0][1]
    assert "ORDER BY qs.total_logical_reads DESC" in executor.calls[0][1]
    assert "query_plan AS" not in executor.calls[0][1]
    assert "query_plan" not in result["cached_queries"]["rows"][0]


@pytest.mark.asyncio
async def test_get_top_cached_queries_rejects_unknown_sort() -> None:
    service = DiagnosticQueryService(FakeExecutor())

    with pytest.raises(ValueError, match="Unsupported sort_by"):
        await service.get_top_cached_queries("appdb", sort_by="bad")


@pytest.mark.asyncio
async def test_get_cached_routine_stats_returns_proc_and_function_sections() -> None:
    executor = FakeExecutor(
        [
            [{"routine_name": "GetOrders", "routine_type": "PROCEDURE"}],
            [{"routine_name": "NormalizeSku", "routine_type": "SQL_SCALAR_FUNCTION"}],
        ]
    )
    service = DiagnosticQueryService(executor)

    result = await service.get_cached_routine_stats(
        "appdb",
        routine_type="all",
        sort_by="execution_count",
        limit=25,
    )

    assert result["procedures"]["rows"][0]["routine_name"] == "GetOrders"
    assert result["functions"]["rows"][0]["routine_name"] == "NormalizeSku"
    assert result["coverage"]["status"] == "complete"
    assert "sys.dm_exec_procedure_stats" in executor.calls[0][1]
    assert "sys.dm_exec_function_stats" in executor.calls[1][1]
    assert "query_plan AS" not in executor.calls[0][1]
    assert "query_plan AS" not in executor.calls[1][1]


@pytest.mark.asyncio
async def test_get_cached_routine_stats_can_scope_to_procedures() -> None:
    executor = FakeExecutor([[{"routine_name": "GetOrders"}]])
    service = DiagnosticQueryService(executor)

    result = await service.get_cached_routine_stats("appdb", routine_type="procedure")

    assert result["procedures"]["row_count"] == 1
    assert result["functions"]["skipped"] is True
    assert len(executor.calls) == 1


@pytest.mark.asyncio
async def test_get_object_index_diagnostics_collects_sections_and_handles_optional_errors() -> None:
    executor = FakeExecutor(
        [
            [{"table_name": "Orders", "temporal_type_desc": "NON_TEMPORAL_TABLE"}],
            [{"index_name": "IX_Orders_CustomerId", "user_updates": 100}],
            [{"index_name": "IX_Orders_CustomerId", "user_reads": 90}],
            [{"index_name": "IX_Orders_CustomerId", "user_updates": 100}],
            [{"stats_name": "IX_Orders_CustomerId", "modification_counter": 40}],
            [{"index_name": "CCI_Orders", "deleted_rows_pct": 12.5}],
            [{"index_name": "IX_Orders_CustomerId", "row_lock_wait_count": 2}],
            [{"index_name": "IX_Orders_CustomerId", "percent_complete": 50}],
        ],
        errors=[("sys.dm_os_buffer_descriptors", RuntimeError("permission denied"))],
    )
    service = DiagnosticQueryService(executor)

    result = await service.get_object_index_diagnostics(
        "appdb",
        schema_name="dbo",
        table_name="Orders",
        limit=25,
    )

    assert result["table_properties"]["rows"][0]["table_name"] == "Orders"
    assert result["bad_nonclustered_indexes"]["rows"][0]["user_updates"] == 100
    assert result["index_usage_reads"]["rows"][0]["user_reads"] == 90
    assert result["buffer_usage"]["available"] is False
    assert "permission denied" in result["buffer_usage"]["error"]
    assert result["coverage"]["status"] == "partial"
    assert result["coverage"]["unavailable_sections"] == ["buffer_usage"]
    assert result["volatile_statistics"]["rows"][0]["stats_name"] == "IX_Orders_CustomerId"
    assert result["columnstore_physical_stats"]["rows"][0]["deleted_rows_pct"] == 12.5
    assert result["lock_waits"]["rows"][0]["row_lock_wait_count"] == 2
    assert result["resumable_index_operations"]["rows"][0]["percent_complete"] == 50
    assert executor.calls[0][2] == ("dbo", "dbo", "Orders", "Orders")
