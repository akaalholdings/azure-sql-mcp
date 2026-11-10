"""Columnstore health per index from the row group physical statistics.

One aggregate read of ``sys.dm_db_column_store_row_group_physical_stats`` grouped
by index, state and trim reason. The view returns only tables the caller has
CONTROL on. Output is bounded: the worst indexes plus totals over all of them.

Dictionary pressure is read from the ``DICTIONARY_SIZE`` trim reason, which is the
engine's own statement that a dictionary cut a row group short.
"""

from __future__ import annotations

from typing import Any

from .azure_tier import read_dmv
from .connection import AzureSqlExecutor
from .result_status import ResultStatus
from .result_status import status_payload

ROW_GROUP_DMV = "sys.dm_db_column_store_row_group_physical_stats"

ROW_GROUP_SQL = """
SELECT
    rg.object_id,
    rg.index_id,
    OBJECT_SCHEMA_NAME(rg.object_id) AS schema_name,
    OBJECT_NAME(rg.object_id) AS table_name,
    i.name AS index_name,
    i.type_desc AS index_type,
    rg.state_desc,
    rg.trim_reason_desc,
    COUNT_BIG(*) AS row_group_count,
    SUM(CAST(rg.total_rows AS bigint)) AS total_rows,
    SUM(CAST(ISNULL(rg.deleted_rows, 0) AS bigint)) AS deleted_rows,
    SUM(CAST(rg.size_in_bytes AS bigint)) AS size_in_bytes
FROM sys.dm_db_column_store_row_group_physical_stats AS rg
INNER JOIN sys.indexes AS i
    ON i.object_id = rg.object_id
   AND i.index_id = rg.index_id
INNER JOIN sys.objects AS o
    ON o.object_id = rg.object_id
WHERE o.is_ms_shipped = 0{object_filter}
GROUP BY rg.object_id, rg.index_id, i.name, i.type_desc, rg.state_desc, rg.trim_reason_desc
"""

OBJECT_FILTER = """
  AND OBJECT_SCHEMA_NAME(rg.object_id) = ?
  AND OBJECT_NAME(rg.object_id) = ?"""

MAX_WORST_N = 50
ROW_GROUP_MAX_ROWS = 1_048_576
# Microsoft Learn: remove deleted rows when 20% or more of a row group is deleted.
DELETED_ROWS_PERCENT_BAR = 20.0
# A bulk load below 102,400 rows goes to a delta store; compressed row groups
# that average less than that were cut short.
UNDERSIZED_AVG_ROWS = 102_400
RANKED_BY = "flag count, then deleted_rows_percent, then size_mb (all descending)"
_BYTES_PER_MB = 1024 * 1024


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _add(counter: dict[str, int], key: str, amount: int) -> None:
    counter[key] = counter.get(key, 0) + amount


def summarize_columnstore_indexes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fold (index, state, trim reason) aggregate rows into one entry per index."""

    indexes: dict[tuple[Any, Any], dict[str, Any]] = {}
    for row in rows:
        key = (row.get("object_id"), row.get("index_id"))
        index = indexes.setdefault(
            key,
            {
                "schema_name": row.get("schema_name"),
                "table_name": row.get("table_name"),
                "index_name": row.get("index_name"),
                "index_type": row.get("index_type"),
                "row_groups_by_state": {},
                "compressed_row_groups": 0,
                "compressed_rows": 0,
                "deleted_rows": 0,
                "open_delta_stores": 0,
                "closed_delta_stores": 0,
                "delta_store_rows": 0,
                "trim_reasons": {},
                "size_bytes": 0,
            },
        )
        state = str(row.get("state_desc") or "UNKNOWN")
        count = _int(row.get("row_group_count"))
        total_rows = _int(row.get("total_rows"))
        _add(index["row_groups_by_state"], state, count)
        index["size_bytes"] += _int(row.get("size_in_bytes"))
        if state == "COMPRESSED":
            index["compressed_row_groups"] += count
            index["compressed_rows"] += total_rows
            index["deleted_rows"] += _int(row.get("deleted_rows"))
            trim = row.get("trim_reason_desc")
            if trim and trim != "NO_TRIM":
                _add(index["trim_reasons"], str(trim), count)
        elif state in {"OPEN", "CLOSED"}:
            index["open_delta_stores" if state == "OPEN" else "closed_delta_stores"] += count
            index["delta_store_rows"] += total_rows

    summaries: list[dict[str, Any]] = []
    for index in indexes.values():
        compressed_groups = index["compressed_row_groups"]
        compressed_rows = index["compressed_rows"]
        index["avg_rows_per_compressed_row_group"] = (
            round(compressed_rows / compressed_groups) if compressed_groups else None
        )
        index["deleted_rows_percent"] = (
            round(100.0 * index["deleted_rows"] / compressed_rows, 2) if compressed_rows else None
        )
        index["size_mb"] = round(index.pop("size_bytes") / _BYTES_PER_MB, 1)
        flags: list[str] = []
        if (index["deleted_rows_percent"] or 0.0) >= DELETED_ROWS_PERCENT_BAR:
            flags.append("deleted_rows_20_percent_or_more")
        if (
            compressed_rows >= ROW_GROUP_MAX_ROWS
            and index["avg_rows_per_compressed_row_group"] is not None
            and index["avg_rows_per_compressed_row_group"] < UNDERSIZED_AVG_ROWS
        ):
            flags.append("undersized_row_groups")
        if index["trim_reasons"].get("DICTIONARY_SIZE"):
            flags.append("dictionary_size_trims")
        if index["trim_reasons"].get("MEMORY_LIMITATION"):
            flags.append("memory_limited_trims")
        index["flags"] = flags
        summaries.append(index)
    summaries.sort(
        key=lambda item: (-len(item["flags"]), -(item["deleted_rows_percent"] or 0.0), -item["size_mb"])
    )
    return summaries


def _totals(indexes: list[dict[str, Any]]) -> dict[str, Any]:
    states: dict[str, int] = {}
    trims: dict[str, int] = {}
    for index in indexes:
        for state, count in index["row_groups_by_state"].items():
            _add(states, state, count)
        for reason, count in index["trim_reasons"].items():
            _add(trims, reason, count)
    compressed_rows = sum(index["compressed_rows"] for index in indexes)
    deleted_rows = sum(index["deleted_rows"] for index in indexes)
    return {
        "index_count": len(indexes),
        "flagged_index_count": sum(1 for index in indexes if index["flags"]),
        "row_groups_by_state": states,
        "compressed_rows": compressed_rows,
        "deleted_rows": deleted_rows,
        "deleted_rows_percent": round(100.0 * deleted_rows / compressed_rows, 2) if compressed_rows else None,
        "delta_store_rows": sum(index["delta_store_rows"] for index in indexes),
        "trim_reasons": trims,
        "size_mb": round(sum(index["size_mb"] for index in indexes), 1),
    }


class ColumnstoreHealthService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_columnstore_health(
        self,
        database_name: str,
        worst_n: int = 10,
        *,
        schema_name: str | None = None,
        table_name: str | None = None,
    ) -> dict[str, Any]:
        """Row group health of every columnstore index, or of one table's indexes."""

        worst_n = max(1, min(int(worst_n), MAX_WORST_N))
        one_table = bool(schema_name and table_name)
        query = ROW_GROUP_SQL.format(object_filter=OBJECT_FILTER if one_table else "")
        params = [schema_name, table_name] if one_table else None
        rows, gap = await read_dmv(
            self.executor, database_name, ROW_GROUP_DMV, query, params, site="columnstore_health.row_groups"
        )
        payload: dict[str, Any] = {
            "database_name": database_name,
            "source_dmv": ROW_GROUP_DMV,
            "collection_window": {"mode": "point_in_time"},
            "worst_n": worst_n,
            "ranked_by": RANKED_BY,
            "thresholds": {
                "deleted_rows_percent": DELETED_ROWS_PERCENT_BAR,
                "undersized_avg_rows_per_compressed_row_group": UNDERSIZED_AVG_ROWS,
                "row_group_max_rows": ROW_GROUP_MAX_ROWS,
            },
            "truncated": False,
            "totals": _totals([]),
            "worst_indexes": [],
        }
        if gap is not None:
            payload.update(gap)
            return payload
        indexes = summarize_columnstore_indexes(rows)
        if not indexes:
            payload.update(
                status_payload(
                    ResultStatus.EMPTY,
                    "No columnstore row group is visible. Either no columnstore index matches, or "
                    "the principal lacks CONTROL on the tables: the view hides tables without it.",
                )
            )
            return payload
        payload["totals"] = _totals(indexes)
        payload["truncated"] = len(indexes) > worst_n
        payload["worst_indexes"] = indexes[:worst_n]
        payload.update(
            status_payload(
                ResultStatus.OK,
                f"{len(indexes)} columnstore index(es); {payload['totals']['flagged_index_count']} flagged.",
            )
        )
        return payload
