"""Columnstore health per index: row group quality, deleted rows, delta stores, trims.

Columnstore on the fact tables is fixed on the platforms this serves, so "rebuild
or not" and "why are scans slow" need row group facts per index, bounded to the
worst few with totals for the rest.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.columnstore_health import ColumnstoreHealthService
from tests.azure_dmv_contract import StrictDmvExecutor

DMV = "sys.dm_db_column_store_row_group_physical_stats"
FULL = 1_048_576


def group(
    table: str,
    state: str,
    trim: str | None,
    count: int,
    total_rows: int,
    deleted_rows: int = 0,
    size_bytes: int = 0,
    *,
    object_id: int | None = None,
    index_name: str | None = None,
) -> dict[str, Any]:
    return {
        "object_id": object_id if object_id is not None else abs(hash(table)) % 10_000,
        "index_id": 1,
        "schema_name": "dbo",
        "table_name": table,
        "index_name": index_name or f"CCI_{table}",
        "index_type": "CLUSTERED COLUMNSTORE",
        "state_desc": state,
        "trim_reason_desc": trim,
        "row_group_count": count,
        "total_rows": total_rows,
        "deleted_rows": deleted_rows,
        "size_in_bytes": size_bytes,
    }


def executor_for(rows: Any) -> StrictDmvExecutor:
    return StrictDmvExecutor(responses=[(DMV, rows)])


async def health(rows: Any, **kwargs: Any) -> dict[str, Any]:
    return await ColumnstoreHealthService(executor_for(rows)).get_columnstore_health("appdb", **kwargs)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_one_index_reports_states_rows_per_group_deletes_and_delta_stores() -> None:
    rows = [
        group("FactSales", "COMPRESSED", "NO_TRIM", 90, 90 * FULL, deleted_rows=30 * FULL, size_bytes=9_000 * 1_048_576),
        group("FactSales", "COMPRESSED", "DICTIONARY_SIZE", 10, 10 * 200_000),
        group("FactSales", "OPEN", None, 1, 50_000),
        group("FactSales", "CLOSED", None, 2, 2 * FULL),
        group("FactSales", "TOMBSTONE", None, 3, 0),
    ]

    result = await health(rows)

    assert result["result_status"] == "ok"
    assert result["source_dmv"] == DMV
    assert result["collection_window"] == {"mode": "point_in_time"}
    (index,) = result["worst_indexes"]
    assert index["table_name"] == "FactSales" and index["index_name"] == "CCI_FactSales"
    assert index["row_groups_by_state"] == {"COMPRESSED": 100, "OPEN": 1, "CLOSED": 2, "TOMBSTONE": 3}
    assert index["compressed_row_groups"] == 100
    assert index["compressed_rows"] == 90 * FULL + 2_000_000
    assert index["avg_rows_per_compressed_row_group"] == round((90 * FULL + 2_000_000) / 100)
    assert index["deleted_rows"] == 30 * FULL
    assert index["deleted_rows_percent"] == round(100 * 30 * FULL / (90 * FULL + 2_000_000), 2)
    assert index["open_delta_stores"] == 1 and index["closed_delta_stores"] == 2
    assert index["delta_store_rows"] == 50_000 + 2 * FULL
    assert index["trim_reasons"] == {"DICTIONARY_SIZE": 10}
    assert index["size_mb"] == 9000.0
    assert set(index["flags"]) == {"deleted_rows_20_percent_or_more", "dictionary_size_trims"}


@pytest.mark.asyncio
async def test_healthy_index_has_no_flag() -> None:
    rows = [
        group("FactOrders", "COMPRESSED", "NO_TRIM", 50, 50 * FULL, deleted_rows=FULL),
        # The tail of an index build is always short; it is not a defect.
        group("FactOrders", "COMPRESSED", "RESIDUAL_ROW_GROUP", 1, 300_000),
        group("FactOrders", "OPEN", None, 1, 10_000),
    ]

    result = await health(rows)

    (index,) = result["worst_indexes"]
    assert index["flags"] == []
    assert index["trim_reasons"] == {"RESIDUAL_ROW_GROUP": 1}
    assert result["totals"]["flagged_index_count"] == 0


@pytest.mark.asyncio
async def test_small_row_groups_are_flagged_only_when_the_index_could_fill_one() -> None:
    trickle = [group("FactClicks", "COMPRESSED", "BULKLOAD", 400, 400 * 20_000)]
    tiny = [group("DimDate", "COMPRESSED", "RESIDUAL_ROW_GROUP", 1, 7_000)]

    flagged = (await health(trickle))["worst_indexes"][0]
    small_table = (await health(tiny))["worst_indexes"][0]

    assert flagged["avg_rows_per_compressed_row_group"] == 20_000
    assert "undersized_row_groups" in flagged["flags"]
    # 7,000 rows cannot fill a row group; flagging it would ask for a pointless rebuild.
    assert small_table["flags"] == []


@pytest.mark.asyncio
async def test_memory_limited_compression_is_flagged() -> None:
    rows = [
        group("FactWide", "COMPRESSED", "MEMORY_LIMITATION", 40, 40 * 300_000),
        group("FactWide", "COMPRESSED", "NO_TRIM", 10, 10 * FULL),
    ]

    (index,) = (await health(rows))["worst_indexes"]

    assert "memory_limited_trims" in index["flags"]
    assert index["trim_reasons"] == {"MEMORY_LIMITATION": 40}


@pytest.mark.asyncio
async def test_output_is_bounded_to_the_worst_indexes_with_totals_for_all() -> None:
    rows: list[dict[str, Any]] = []
    for number in range(30):
        deleted = FULL * number // 30  # index 29 has the most deleted rows
        rows.append(group(f"Fact{number:02d}", "COMPRESSED", "NO_TRIM", 10, 10 * FULL, deleted_rows=10 * deleted, object_id=number))

    result = await health(rows, worst_n=5)

    assert [index["table_name"] for index in result["worst_indexes"]] == ["Fact29", "Fact28", "Fact27", "Fact26", "Fact25"]
    assert result["totals"]["index_count"] == 30
    assert result["totals"]["compressed_rows"] == 30 * 10 * FULL
    assert result["totals"]["row_groups_by_state"] == {"COMPRESSED": 300}
    assert result["truncated"] is True and result["worst_n"] == 5
    assert "deleted" in result["ranked_by"]


@pytest.mark.asyncio
async def test_worst_n_is_clamped() -> None:
    result = await health([group("FactA", "COMPRESSED", "NO_TRIM", 1, FULL)], worst_n=10_000)

    assert result["worst_n"] == 50


@pytest.mark.asyncio
async def test_no_columnstore_rows_is_empty_and_says_what_can_hide_them() -> None:
    result = await health([])

    assert result["result_status"] == "empty"
    assert result["worst_indexes"] == []
    assert "CONTROL" in result["result_status_reason"]


@pytest.mark.asyncio
async def test_refused_read_is_unavailable_with_remediation() -> None:
    result = await health(RuntimeError("VIEW DATABASE STATE permission was denied in database 'appdb'. (262)"))

    assert result["result_status"] == "unavailable"
    assert "GRANT VIEW DATABASE STATE" in result["remediation"]
    assert result["worst_indexes"] == []


@pytest.mark.asyncio
async def test_read_is_one_aggregate_on_documented_columns_and_can_target_a_table() -> None:
    executor = StrictDmvExecutor()
    service = ColumnstoreHealthService(executor)  # type: ignore[arg-type]

    await service.get_columnstore_health("appdb")
    await service.get_columnstore_health("appdb", schema_name="dbo", table_name="FactSales")

    assert executor.violations == []
    assert len(executor.calls) == 2
    everything, one_table = executor.calls
    assert "GROUP BY" in everything[1] and everything[2] is None
    assert one_table[2] == ("dbo", "FactSales")
