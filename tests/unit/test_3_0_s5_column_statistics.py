"""Column statistics as a first-class read (design W7 step 10, decision D5).

To test a parameter-sensitive query a DBA picks a common value, a rare value,
NULL and the boundaries from the column's histogram. Without this read an agent
guesses values, and a benchmark on a guessed value proves nothing about the
value that hurts in production.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.column_statistics import ColumnStatisticsService
from azure_sql_mcp.column_statistics import summarize_histogram
from azure_sql_mcp.introspection import IntrospectionService


def _step(
    key: str | None,
    equal_rows: float,
    range_rows: float = 0,
    distinct_range_rows: float = 0,
    *,
    stats_id: int = 2,
    is_null_key: bool | None = None,
) -> dict[str, Any]:
    return {
        "stats_id": stats_id,
        "range_high_key": key,
        "is_null_key": (key is None) if is_null_key is None else is_null_key,
        "range_rows": range_rows,
        "equal_rows": equal_rows,
        "distinct_range_rows": distinct_range_rows,
        "average_range_rows": (range_rows / distinct_range_rows) if distinct_range_rows else 1,
    }


STEPS = [
    _step(None, 50),
    _step("10", 900),
    _step("20", 3, range_rows=40, distinct_range_rows=8),
    _step("99", 7, range_rows=0, distinct_range_rows=0),
]


def test_summary_gives_the_four_buckets_a_dba_tests() -> None:
    summary = summarize_histogram(STEPS, max_steps=50, top_values=2)

    assert summary["value_picks"] == {
        "common": {"value": "10", "equal_rows": 900},
        "rare": {"value": "20", "equal_rows": 3},
        "boundary_low": {"value": "10"},
        "boundary_high": {"value": "99"},
        "null": {"rows": 50},
    }
    assert summary["heavy_hitters"] == [
        {"value": "10", "equal_rows": 900, "share": 0.9},
        {"value": "99", "equal_rows": 7, "share": 0.007},
    ]


def test_summary_measures_skew_from_the_histogram() -> None:
    distribution = summarize_histogram(STEPS, max_steps=50, top_values=2)["distribution"]

    # 3 keys with rows plus 8 distinct values inside the one range.
    assert distribution["estimated_distinct"] == 11
    assert distribution["histogram_rows"] == 1000
    assert distribution["null_rows"] == 50
    assert distribution["null_fraction"] == 0.05
    assert distribution["density"] == pytest.approx(1 / 11)
    assert distribution["average_rows_per_value"] == pytest.approx(950 / 11)
    assert distribution["max_equal_rows"] == 900
    assert distribution["top_value_share"] == 0.9
    # The heaviest value returns about ten times the average value's rows:
    # one plan cannot be right for both.
    assert distribution["skew_ratio"] == pytest.approx(900 / (950 / 11))


def test_histogram_is_bounded_and_keeps_the_heaviest_steps_in_key_order() -> None:
    steps = [_step(str(index), equal_rows=index) for index in range(1, 201)]

    histogram = summarize_histogram(steps, max_steps=5, top_values=3)["histogram"]

    assert histogram["steps_total"] == 200
    assert histogram["steps_returned"] == 5
    assert histogram["truncated"] is True
    assert [step["range_high_key"] for step in histogram["steps"]] == [
        "196", "197", "198", "199", "200",
    ]


def test_withheld_values_keep_the_counts_and_drop_every_key() -> None:
    # D5: a masked or classified column never shows its values. The SQL already
    # returns NULL keys for it; is_null_key still marks the real NULL step.
    steps = [
        _step(None, 50, is_null_key=True),
        _step(None, 900, is_null_key=False),
        _step(None, 50, is_null_key=False),
    ]

    summary = summarize_histogram(steps, max_steps=50, top_values=2, values_withheld=True)

    assert summary["value_picks"] is None
    assert summary["heavy_hitters"] == [
        {"value": None, "equal_rows": 900, "share": 0.9},
        {"value": None, "equal_rows": 50, "share": 0.05},
    ]
    assert all(step["range_high_key"] is None for step in summary["histogram"]["steps"])
    assert summary["distribution"]["null_rows"] == 50
    assert summary["distribution"]["estimated_distinct"] == 2


class CatalogExecutor:
    def __init__(
        self,
        catalog: list[dict[str, Any]],
        *,
        histogram: list[dict[str, Any]] | None = None,
        classification: list[dict[str, Any]] | Exception | None = None,
        row_count: int | None = 1000,
    ) -> None:
        self.catalog = catalog
        self.histogram = histogram or []
        self.classification = (
            [{"can_read": 1, "column_id": None, "label": None, "information_type": None}]
            if classification is None
            else classification
        )
        self.row_count = row_count
        self.calls: list[tuple[str, list[Any]]] = []

    async def fetch_all(self, database_name: str, query: str, params=None):
        self.calls.append((query, list(params or [])))
        if "sys.dm_db_stats_histogram" in query:
            return self.histogram
        if "sys.sensitivity_classifications" in query:
            if isinstance(self.classification, Exception):
                raise self.classification
            return self.classification
        if "sys.dm_db_partition_stats" in query:
            return [{"row_count": self.row_count}]
        return self.catalog

    def histogram_call(self) -> tuple[str, list[Any]]:
        return next(call for call in self.calls if "sys.dm_db_stats_histogram" in call[0])


def _catalog_row(
    column_id: int,
    name: str,
    *,
    stats_id: int | None = 2,
    data_type: str = "int",
    is_masked: bool = False,
    steps: int | None = 4,
    has_filter: bool = False,
    rows_sampled: int = 1000,
    stats_name: str | None = None,
) -> dict[str, Any]:
    return {
        "column_id": column_id,
        "column_name": name,
        "data_type": data_type,
        "max_length": 4,
        "precision": 10,
        "scale": 0,
        "is_nullable": True,
        "collation_name": None,
        "is_masked": is_masked,
        "stats_id": stats_id,
        "stats_name": stats_name or (f"IX_{name}" if stats_id else None),
        "auto_created": False,
        "has_filter": has_filter,
        "filter_definition": "([Status]='Open')" if has_filter else None,
        "rows": 1000 if stats_id else None,
        "rows_sampled": rows_sampled if stats_id else None,
        "steps": steps if stats_id else None,
        "modification_counter": 12 if stats_id else None,
        "last_updated_utc": "2026-11-01T02:00:00" if stats_id else None,
    }


@pytest.mark.asyncio
async def test_service_returns_picks_for_a_column_with_leading_statistics() -> None:
    executor = CatalogExecutor([_catalog_row(3, "CustomerId")], histogram=STEPS)
    service = ColumnStatisticsService(executor)  # type: ignore[arg-type]

    result = await service.get_column_statistics("appdb", "dbo", "Orders", ["customerid"])

    assert result["result_status"] == "ok"
    assert result["row_count"] == 1000
    column = result["columns"][0]
    assert column["column_name"] == "CustomerId"
    assert column["data_type"] == "int"
    assert column["values_withheld"] is None
    assert column["statistics"]["name"] == "IX_CustomerId"
    assert column["statistics"]["sample_percent"] == 100.0
    assert column["value_picks"]["common"] == {"value": "10", "equal_rows": 900}
    assert column["distribution"]["skew_ratio"] > 10
    # The histogram read asks only for statistics that lead with the column.
    # Parameter order: withheld statistics ids, schema, table, statistics ids.
    assert executor.histogram_call()[1] == ["dbo", "Orders", 2]
    catalog_query = executor.calls[0][0]
    assert "sc.stats_column_id = 1" in catalog_query


@pytest.mark.asyncio
async def test_unfiltered_fully_sampled_statistics_are_preferred() -> None:
    # A filtered histogram describes a subset; a 1% sample misses rare values.
    executor = CatalogExecutor(
        [
            _catalog_row(3, "CustomerId", stats_id=2, has_filter=True, stats_name="IX_Open"),
            _catalog_row(3, "CustomerId", stats_id=5, rows_sampled=10, stats_name="_WA_Sys"),
            _catalog_row(3, "CustomerId", stats_id=7, stats_name="IX_Full"),
        ],
        histogram=[{**step, "stats_id": 7} for step in STEPS],
    )
    service = ColumnStatisticsService(executor)  # type: ignore[arg-type]

    result = await service.get_column_statistics("appdb", "dbo", "Orders", ["CustomerId"])

    column = result["columns"][0]
    assert column["statistics"]["name"] == "IX_Full"
    assert column["other_leading_statistics"] == ["IX_Open", "_WA_Sys"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("catalog_kwargs", "classification", "expected"),
    [
        ({"is_masked": True}, None, "masked"),
        (
            {},
            [{"can_read": 1, "column_id": 3, "label": "Confidential", "information_type": "Contact"}],
            "classified",
        ),
        # The login cannot see classifications: an empty list is not "none".
        (
            {},
            [{"can_read": 0, "column_id": None, "label": None, "information_type": None}],
            "classification_unreadable",
        ),
        ({}, RuntimeError("permission denied"), "classification_unreadable"),
    ],
)
async def test_protected_columns_never_ask_the_database_for_their_values(
    catalog_kwargs: dict[str, Any],
    classification: Any,
    expected: str,
) -> None:
    executor = CatalogExecutor(
        [_catalog_row(3, "Email", data_type="int", **catalog_kwargs)],
        histogram=[
            _step(None, 50, is_null_key=True),
            _step(None, 900, is_null_key=False),
            _step(None, 50, is_null_key=False),
        ],
        classification=classification,
    )
    service = ColumnStatisticsService(executor)  # type: ignore[arg-type]

    result = await service.get_column_statistics("appdb", "dbo", "Users", ["Email"])

    column = result["columns"][0]
    assert column["values_withheld"] == expected
    assert column["value_picks"] is None
    assert column["distribution"]["top_value_share"] == 0.9
    # The statistics id is passed in the withheld list, so the CASE in the
    # query returns NULL instead of the key: the value never leaves the database.
    query, params = executor.histogram_call()
    assert "CASE WHEN st.stats_id IN (0, ?) THEN NULL" in query
    assert params == [2, "dbo", "Users", 2]
    if expected == "classification_unreadable":
        assert "VIEW ANY SENSITIVITY CLASSIFICATION" in result["remediation"]


@pytest.mark.asyncio
async def test_no_leading_statistics_is_empty_not_ok() -> None:
    executor = CatalogExecutor([_catalog_row(4, "Notes", stats_id=None, data_type="int")])
    service = ColumnStatisticsService(executor)  # type: ignore[arg-type]

    result = await service.get_column_statistics("appdb", "dbo", "Orders", ["Notes"])

    assert result["result_status"] == "empty"
    assert result["columns"][0]["status"] == "no_leading_statistics"
    assert result["columns"][0]["value_picks"] is None
    assert not any("sys.dm_db_stats_histogram" in query for query, _ in executor.calls)


@pytest.mark.asyncio
async def test_steps_without_histogram_rows_is_unavailable_not_empty() -> None:
    # The statistic has 4 steps but the histogram came back empty: a missing
    # permission, never a column with no data.
    executor = CatalogExecutor([_catalog_row(3, "CustomerId", steps=4)], histogram=[])
    service = ColumnStatisticsService(executor)  # type: ignore[arg-type]

    result = await service.get_column_statistics("appdb", "dbo", "Orders", ["CustomerId"])

    assert result["result_status"] == "unavailable"
    assert result["columns"][0]["status"] == "histogram_unreadable"


@pytest.mark.asyncio
async def test_unknown_table_or_column_is_refused_without_reading_histograms() -> None:
    service = ColumnStatisticsService(CatalogExecutor([]))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="not found"):
        await service.get_column_statistics("appdb", "dbo", "Missing", None)

    executor = CatalogExecutor([_catalog_row(3, "CustomerId")])
    service = ColumnStatisticsService(executor)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="not found"):
        await service.get_column_statistics("appdb", "dbo", "Orders", ["Nope"])


@pytest.mark.asyncio
async def test_default_reads_only_columns_that_lead_a_statistic_and_caps_the_request() -> None:
    executor = CatalogExecutor(
        [_catalog_row(1, "Id"), _catalog_row(2, "Notes", stats_id=None)], histogram=STEPS
    )
    service = ColumnStatisticsService(executor)  # type: ignore[arg-type]

    result = await service.get_column_statistics("appdb", "dbo", "Orders")

    assert [column["column_name"] for column in result["columns"]] == ["Id"]
    with pytest.raises(ValueError, match="At most"):
        await service.get_column_statistics(
            "appdb", "dbo", "Orders", [f"c{index}" for index in range(30)]
        )


# --- sys.sql_modules inlining facts for functions ----------------------------


class OneRowExecutor:
    def __init__(self, row: dict[str, Any]) -> None:
        self.row = row
        self.queries: list[str] = []

    async def fetch_all(self, database_name: str, query: str, params=None):
        self.queries.append(query)
        return [dict(self.row)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("is_inlineable", "inline_type", "status"),
    [(True, True, "inlineable"), (False, False, "not_inlineable"), (True, False, "inlining_off")],
)
async def test_function_details_say_whether_a_scalar_udf_can_be_inlined(
    is_inlineable: bool,
    inline_type: bool,
    status: str,
) -> None:
    # A scalar UDF that is not inlined runs once per row and forces a serial
    # plan; its time appears in no operator. The agent must see this up front.
    executor = OneRowExecutor(
        {
            "schema_name": "dbo",
            "object_name": "fn_NetPrice",
            "object_type": "SQL_SCALAR_FUNCTION",
            "definition": "CREATE FUNCTION ...",
            "is_inlineable": is_inlineable,
            "inline_type": inline_type,
        }
    )
    service = IntrospectionService(executor)  # type: ignore[arg-type]

    details = await service.get_object_details("appdb", "dbo", "fn_NetPrice", "function")

    assert "m.is_inlineable" in executor.queries[0] and "m.inline_type" in executor.queries[0]
    assert details["is_inlineable"] is is_inlineable
    assert details["inline_type"] is inline_type
    assert details["scalar_udf_inlining"]["status"] == status


@pytest.mark.asyncio
async def test_inlining_facts_are_not_attached_to_procedures() -> None:
    executor = OneRowExecutor(
        {
            "schema_name": "dbo",
            "object_name": "usp_GetOrders",
            "object_type": "SQL_STORED_PROCEDURE",
            "definition": "CREATE PROCEDURE ...",
            "is_inlineable": False,
            "inline_type": False,
        }
    )
    service = IntrospectionService(executor)  # type: ignore[arg-type]

    details = await service.get_object_details("appdb", "dbo", "usp_GetOrders", "procedure")

    assert "scalar_udf_inlining" not in details
