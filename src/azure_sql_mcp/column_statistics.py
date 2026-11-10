"""Column statistics for parameter-bucket selection, read without sampling rows.

For a table and its columns this returns, per column, the statistics object
that leads with it, a bounded histogram, skew measures and the values a DBA
tests a parameter-sensitive query with: the most common value, a rare value,
NULL and the two boundaries.

Values of a column with Dynamic Data Masking or a sensitivity classification
are withheld (decision D5): the query returns NULL for their keys, so they
never leave the database. When the login cannot read classifications at all,
every value is withheld, because an empty classification list then proves
nothing. Counts and ratios are still returned.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from .connection import AzureSqlExecutor
from .incident_log import note_exception
from .param_binding import column_parameter_type
from .result_status import REMEDIATION_KEY
from .result_status import ResultStatus
from .result_status import status_payload

MAX_COLUMNS = 20
MAX_HISTOGRAM_STEPS = 200
DEFAULT_HISTOGRAM_STEPS = 40
DEFAULT_TOP_VALUES = 5
MAX_VALUE_CHARS = 512

# One row per statistics object that leads with the column, or one row with a
# NULL stats_id when none does. A histogram describes only the leading column.
COLUMN_STATISTICS_SQL = """
SELECT
    c.column_id,
    c.name AS column_name,
    TYPE_NAME(c.system_type_id) AS data_type,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.collation_name,
    c.is_masked,
    st.stats_id,
    st.name AS stats_name,
    st.auto_created,
    st.has_filter,
    st.filter_definition,
    sp.rows,
    sp.rows_sampled,
    sp.steps,
    sp.modification_counter,
    CONVERT(varchar(33), sp.last_updated, 127) AS last_updated_utc
FROM sys.columns AS c
LEFT JOIN sys.stats_columns AS sc
    ON sc.object_id = c.object_id
   AND sc.column_id = c.column_id
   AND sc.stats_column_id = 1
LEFT JOIN sys.stats AS st
    ON st.object_id = sc.object_id
   AND st.stats_id = sc.stats_id
OUTER APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) AS sp
WHERE c.object_id = OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?))
ORDER BY c.column_id, st.stats_id
"""

# Always one row or more: can_read says whether an empty list means "none".
CLASSIFICATION_SQL = """
SELECT
    p.can_read,
    cls.minor_id AS column_id,
    cls.label,
    cls.information_type
FROM (
    SELECT HAS_PERMS_BY_NAME(
        DB_NAME(), N'DATABASE', N'VIEW ANY SENSITIVITY CLASSIFICATION'
    ) AS can_read
) AS p
LEFT JOIN sys.sensitivity_classifications AS cls
    ON cls.class = 1
   AND cls.major_id = OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?))
"""

ROW_COUNT_SQL = """
SELECT SUM(ps.row_count) AS row_count
FROM sys.dm_db_partition_stats AS ps
WHERE ps.object_id = OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?))
  AND ps.index_id IN (0, 1)
"""

# range_high_key is sql_variant, which the driver cannot fetch: CONVERT it
# server-side (style 121 keeps date and time keys ISO-formatted). The first
# IN list holds the statistics whose values are withheld; 0 is never a
# stats_id, so the list is valid when it is otherwise empty.
HISTOGRAM_SQL_TEMPLATE = """
SELECT
    st.stats_id,
    h.step_number,
    CASE WHEN st.stats_id IN (0{withheld}) THEN NULL
         ELSE CONVERT(nvarchar(4000), h.range_high_key, 121)
    END AS range_high_key,
    CASE WHEN h.range_high_key IS NULL THEN 1 ELSE 0 END AS is_null_key,
    h.range_rows,
    h.equal_rows,
    h.distinct_range_rows,
    h.average_range_rows
FROM sys.stats AS st
CROSS APPLY sys.dm_db_stats_histogram(st.object_id, st.stats_id) AS h
WHERE st.object_id = OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?))
  AND st.stats_id IN ({stats})
ORDER BY st.stats_id, h.step_number
"""

_CLASSIFICATION_REMEDIATION = (
    "GRANT VIEW ANY SENSITIVITY CLASSIFICATION TO [<mcp login or user>]; "
    "Without it the server cannot tell which columns are classified, so it "
    "withholds every histogram value."
)
_NO_STATISTICS_REASON = (
    "No statistics object leads with the requested columns. The engine creates "
    "one the first time a query filters on a column (AUTO_CREATE_STATISTICS), "
    "or a DBA creates one with CREATE STATISTICS."
)
_HISTOGRAM_UNREADABLE_REASON = (
    "sys.dm_db_stats_histogram returned no rows for a statistics object that has "
    "steps. The login needs SELECT on the column, or membership in db_owner or "
    "db_ddladmin, to read a histogram."
)


def summarize_histogram(
    steps: Sequence[dict[str, Any]],
    *,
    max_steps: int,
    top_values: int,
    values_withheld: bool = False,
) -> dict[str, Any]:
    """Turn histogram steps, in key order, into picks, skew measures and a bounded list.

    ``distribution`` holds counts and ratios. ``value_picks`` holds the values
    for the usual parameter buckets, and is None when values are withheld.
    ``heavy_hitters`` are the keys with the most equal rows. ``histogram``
    keeps at most ``max_steps`` steps, the ones that cover the most rows.
    """

    null_steps = [step for step in steps if step.get("is_null_key")]
    value_steps = [step for step in steps if not step.get("is_null_key")]
    histogram_rows = sum(_rows(step, "range_rows") + _rows(step, "equal_rows") for step in steps)
    null_rows = sum(_rows(step, "equal_rows") for step in null_steps)
    keyed = [step for step in value_steps if _rows(step, "equal_rows") > 0]
    estimated_distinct = sum(_rows(step, "distinct_range_rows") for step in value_steps) + len(keyed)
    max_equal_rows = max((_rows(step, "equal_rows") for step in keyed), default=0.0)
    average_rows = (
        (histogram_rows - null_rows) / estimated_distinct if estimated_distinct else None
    )
    distribution = {
        "histogram_rows": _number(histogram_rows),
        "null_rows": _number(null_rows),
        "null_fraction": _ratio(null_rows, histogram_rows),
        "estimated_distinct": _number(estimated_distinct),
        "density": (1 / estimated_distinct) if estimated_distinct else None,
        "average_rows_per_value": average_rows,
        "max_equal_rows": _number(max_equal_rows),
        "top_value_share": _ratio(max_equal_rows, histogram_rows),
        # How many times the heaviest value exceeds the average value.
        "skew_ratio": (max_equal_rows / average_rows) if average_rows else None,
    }

    by_weight = sorted(keyed, key=lambda step: _rows(step, "equal_rows"), reverse=True)
    heavy_hitters = [
        {
            "value": None if values_withheld else _value(step),
            "equal_rows": _number(_rows(step, "equal_rows")),
            "share": _ratio(_rows(step, "equal_rows"), histogram_rows),
        }
        for step in by_weight[: max(0, top_values)]
    ]

    value_picks: dict[str, Any] | None = None
    if not values_withheld and value_steps:
        rare = min(
            (step for step in keyed if _rows(step, "equal_rows") >= 1),
            key=lambda step: _rows(step, "equal_rows"),
            default=None,
        )
        value_picks = {
            "common": _pick(by_weight[0]) if by_weight else None,
            "rare": _pick(rare) if rare is not None else None,
            "boundary_low": {"value": _value(value_steps[0])},
            "boundary_high": {"value": _value(value_steps[-1])},
            "null": {"rows": _number(null_rows)} if null_steps else None,
        }

    limit = max(1, min(max_steps, MAX_HISTOGRAM_STEPS))
    kept = steps
    if len(steps) > limit:
        heaviest = sorted(
            range(len(steps)),
            key=lambda index: _rows(steps[index], "range_rows") + _rows(steps[index], "equal_rows"),
            reverse=True,
        )[:limit]
        kept = [steps[index] for index in sorted(heaviest)]
    histogram = {
        "steps_total": len(steps),
        "steps_returned": len(kept),
        "truncated": len(kept) < len(steps),
        "steps": [
            {
                "range_high_key": None if values_withheld else _value(step),
                "is_null_key": bool(step.get("is_null_key")),
                "range_rows": _number(_rows(step, "range_rows")),
                "equal_rows": _number(_rows(step, "equal_rows")),
                "distinct_range_rows": _number(_rows(step, "distinct_range_rows")),
                "average_range_rows": _number(_rows(step, "average_range_rows")),
            }
            for step in kept
        ],
    }
    return {
        "distribution": distribution,
        "value_picks": value_picks,
        "heavy_hitters": heavy_hitters,
        "histogram": histogram,
    }


class ColumnStatisticsService:
    """Reads leading-column statistics and histograms for one table."""

    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_column_statistics(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        columns: Sequence[str] | None = None,
        *,
        include_histogram: bool = True,
        max_steps: int = DEFAULT_HISTOGRAM_STEPS,
        top_values: int = DEFAULT_TOP_VALUES,
    ) -> dict[str, Any]:
        """Return statistics, skew and value picks for up to 20 columns.

        ``columns`` None reads the columns that lead a statistics object.
        Raises ValueError when the table or a named column is not found.
        """

        requested = [str(name) for name in columns or []]
        if len(requested) > MAX_COLUMNS:
            raise ValueError(f"At most {MAX_COLUMNS} columns can be read in one call.")
        if not 1 <= max_steps <= MAX_HISTOGRAM_STEPS:
            raise ValueError(f"max_steps must be between 1 and {MAX_HISTOGRAM_STEPS}.")
        if not 0 <= top_values <= 20:
            raise ValueError("top_values must be between 0 and 20.")

        target = [schema_name, table_name]
        catalog, classification, row_count_rows = await asyncio.gather(
            self.executor.fetch_all(database_name, COLUMN_STATISTICS_SQL, params=target),
            self._optional_read(database_name, CLASSIFICATION_SQL, target, "classification"),
            self._optional_read(database_name, ROW_COUNT_SQL, target, "row_count"),
        )
        if not catalog:
            raise ValueError("The table was not found in this database.")

        by_column: dict[int, list[dict[str, Any]]] = {}
        for row in catalog:
            by_column.setdefault(int(row["column_id"]), []).append(row)
        selected = _select_columns(by_column, requested)

        gaps: list[str] = []
        classified, classification_readable = _classified_columns(classification)
        if not classification_readable:
            gaps.append(
                "Sensitivity classifications could not be read, so every histogram "
                "value is withheld."
            )
        row_count = row_count_rows[0].get("row_count") if row_count_rows else None
        if row_count_rows is None:
            gaps.append("The table row count could not be read.")

        chosen: dict[int, dict[str, Any]] = {}
        withheld: dict[int, str] = {}
        for column_id in selected:
            rows = by_column[column_id]
            reason = _withheld_reason(rows[0], column_id in classified, classification_readable)
            if reason is not None:
                withheld[column_id] = reason
            leading = [row for row in rows if row.get("stats_id") is not None]
            if leading:
                chosen[column_id] = _preferred_statistic(leading)

        histograms = await self._read_histograms(
            database_name,
            target,
            stats_ids=[int(row["stats_id"]) for row in chosen.values() if _steps(row) > 0],
            withheld_stats_ids=[
                int(chosen[column_id]["stats_id"]) for column_id in withheld if column_id in chosen
            ],
        )

        results: list[dict[str, Any]] = []
        for column_id in selected:
            rows = by_column[column_id]
            statistic = chosen.get(column_id)
            steps = histograms.get(int(statistic["stats_id"]), []) if statistic else []
            results.append(
                _column_result(
                    rows,
                    statistic,
                    steps,
                    values_withheld=withheld.get(column_id),
                    include_histogram=include_histogram,
                    max_steps=max_steps,
                    top_values=top_values,
                )
            )

        payload: dict[str, Any] = {
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "row_count": row_count,
            "columns": results,
            "gaps": gaps,
        }
        statuses = {result["status"] for result in results}
        if statuses == {"no_leading_statistics"}:
            payload.update(status_payload(ResultStatus.EMPTY, _NO_STATISTICS_REASON))
        elif "histogram_unreadable" in statuses and "ok" not in statuses:
            payload.update(status_payload(ResultStatus.UNAVAILABLE, _HISTOGRAM_UNREADABLE_REASON))
        else:
            if "histogram_unreadable" in statuses:
                gaps.append(_HISTOGRAM_UNREADABLE_REASON)
            payload.update(status_payload(ResultStatus.OK, "Column statistics were read."))
        if not classification_readable:
            payload[REMEDIATION_KEY] = _CLASSIFICATION_REMEDIATION
        return payload

    async def _optional_read(
        self,
        database_name: str,
        query: str,
        params: list[Any],
        what: str,
    ) -> list[dict[str, Any]] | None:
        try:
            return await self.executor.fetch_all(database_name, query, params=params)
        except Exception as exc:
            note_exception(exc, f"column_statistics.{what}")
            return None

    async def _read_histograms(
        self,
        database_name: str,
        target: list[Any],
        *,
        stats_ids: list[int],
        withheld_stats_ids: list[int],
    ) -> dict[int, list[dict[str, Any]]]:
        if not stats_ids:
            return {}
        withheld = [stats_id for stats_id in withheld_stats_ids if stats_id in stats_ids]
        query = HISTOGRAM_SQL_TEMPLATE.format(
            withheld="".join(", ?" for _ in withheld),
            stats=", ".join("?" for _ in stats_ids),
        )
        rows = await self.executor.fetch_all(
            database_name,
            query,
            params=[*withheld, *target, *stats_ids],
        )
        histograms: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            histograms.setdefault(int(row["stats_id"]), []).append(row)
        return histograms


def _select_columns(
    by_column: dict[int, list[dict[str, Any]]],
    requested: list[str],
) -> list[int]:
    names = {str(rows[0]["column_name"]).casefold(): column_id for column_id, rows in by_column.items()}
    if requested:
        missing = [name for name in requested if name.casefold() not in names]
        if missing:
            raise ValueError(
                f"{len(missing)} requested column(s) were not found on the table."
            )
        return list(dict.fromkeys(names[name.casefold()] for name in requested))
    leading = [
        column_id
        for column_id, rows in by_column.items()
        if any(row.get("stats_id") is not None for row in rows)
    ]
    return sorted(leading or by_column)[:MAX_COLUMNS]


def _classified_columns(
    classification: list[dict[str, Any]] | None,
) -> tuple[set[int], bool]:
    """Column ids with a classification, and whether the list can be trusted."""

    if not classification or not all(row.get("can_read") for row in classification):
        return set(), False
    return {int(row["column_id"]) for row in classification if row.get("column_id") is not None}, True


def _withheld_reason(
    row: dict[str, Any],
    classified: bool,
    classification_readable: bool,
) -> str | None:
    if row.get("is_masked"):
        return "masked"
    if classified:
        return "classified"
    if not classification_readable:
        return "classification_unreadable"
    return None


def _preferred_statistic(leading: list[dict[str, Any]]) -> dict[str, Any]:
    """Unfiltered first, then the highest sample rate, then the newest."""

    def sample_rate(row: dict[str, Any]) -> float:
        rows = float(row.get("rows") or 0)
        return float(row.get("rows_sampled") or 0) / rows if rows else 0.0

    newest_first = sorted(
        leading, key=lambda row: str(row.get("last_updated_utc") or ""), reverse=True
    )
    # min keeps the first of equal items, so the newest wins a tie.
    return min(newest_first, key=lambda row: (bool(row.get("has_filter")), -sample_rate(row)))


def _column_result(
    rows: list[dict[str, Any]],
    statistic: dict[str, Any] | None,
    steps: list[dict[str, Any]],
    *,
    values_withheld: str | None,
    include_histogram: bool,
    max_steps: int,
    top_values: int,
) -> dict[str, Any]:
    first = rows[0]
    result: dict[str, Any] = {
        "column_name": first["column_name"],
        "data_type": column_parameter_type(first) or first.get("data_type"),
        "is_nullable": bool(first.get("is_nullable")),
        "collation_name": first.get("collation_name"),
        "values_withheld": values_withheld,
        "status": "ok",
        "statistics": None,
        "other_leading_statistics": [],
        "distribution": None,
        "value_picks": None,
        "heavy_hitters": [],
    }
    if statistic is None:
        result["status"] = "no_leading_statistics"
        return result
    stats_rows = float(statistic.get("rows") or 0)
    result["statistics"] = {
        "name": statistic.get("stats_name"),
        "stats_id": statistic.get("stats_id"),
        "auto_created": bool(statistic.get("auto_created")),
        "has_filter": bool(statistic.get("has_filter")),
        "filter_definition": statistic.get("filter_definition"),
        "rows": statistic.get("rows"),
        "rows_sampled": statistic.get("rows_sampled"),
        "sample_percent": (
            round(float(statistic.get("rows_sampled") or 0) * 100 / stats_rows, 2)
            if stats_rows
            else None
        ),
        "steps": statistic.get("steps"),
        "rows_modified_since_update": statistic.get("modification_counter"),
        "last_updated_utc": statistic.get("last_updated_utc"),
    }
    result["other_leading_statistics"] = [
        row.get("stats_name")
        for row in rows
        if row.get("stats_id") is not None and row.get("stats_id") != statistic.get("stats_id")
    ]
    if not steps:
        result["status"] = "histogram_unreadable" if _steps(statistic) > 0 else "empty_statistics"
        return result
    summary = summarize_histogram(
        steps,
        max_steps=max_steps,
        top_values=top_values,
        values_withheld=values_withheld is not None,
    )
    result["distribution"] = summary["distribution"]
    result["value_picks"] = summary["value_picks"]
    result["heavy_hitters"] = summary["heavy_hitters"]
    if include_histogram:
        result["histogram"] = summary["histogram"]
    return result


def _steps(row: dict[str, Any]) -> int:
    return int(row.get("steps") or 0)


def _rows(step: dict[str, Any], key: str) -> float:
    value = step.get(key)
    return float(value) if value is not None else 0.0


def _number(value: float) -> int | float:
    return int(value) if float(value).is_integer() else round(value, 2)


def _ratio(part: float, whole: float) -> float | None:
    return round(part / whole, 6) if whole else None


def _value(step: dict[str, Any]) -> str | None:
    value = step.get("range_high_key")
    if value is None:
        return None
    text = str(value)
    return text if len(text) <= MAX_VALUE_CHARS else text[:MAX_VALUE_CHARS] + "..."


def _pick(step: dict[str, Any]) -> dict[str, Any]:
    return {"value": _value(step), "equal_rows": _number(_rows(step, "equal_rows"))}
