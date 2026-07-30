from __future__ import annotations

from dataclasses import replace

import pytest

from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import collect_existing_indexes
from azure_sql_mcp.index_metadata import EXISTING_INDEX_METADATA_SQL
from azure_sql_mcp.index_metadata import IndexKeyColumn
from azure_sql_mcp.index_metadata import existing_index_covers_candidate
from azure_sql_mcp.index_metadata import normalize_index_definition
from azure_sql_mcp.index_metadata import parse_existing_index_rows


def _rows() -> list[dict[str, object]]:
    base: dict[str, object] = {
        "schema_name": "dbo",
        "table_name": "Orders",
        "index_id": 7,
        "index_name": "IX_Orders_Status_Date",
        "index_type": "NONCLUSTERED",
        "is_unique": True,
        "is_primary_key": False,
        "is_unique_constraint": True,
        "is_disabled": False,
        "has_filter": True,
        "filter_definition": "[Status] = (1)",
        "fill_factor": 90,
        "data_space_name": "ps_OrderDate",
        "data_space_type": "PARTITION_SCHEME",
        "partition_number": 1,
        "data_compression_desc": "PAGE",
        "user_seeks": 10,
        "user_scans": 2,
        "user_lookups": 1,
        "user_updates": 3,
        "collected_at_utc": "2026-07-15T10:00:00Z",
    }
    return [
        {
            **base,
            "index_column_id": 1,
            "key_ordinal": 1,
            "is_included_column": False,
            "is_descending_key": False,
            "partition_ordinal": 0,
            "column_name": "Status",
        },
        {
            **base,
            "index_column_id": 2,
            "key_ordinal": 2,
            "is_included_column": False,
            "is_descending_key": True,
            "partition_ordinal": 1,
            "column_name": "OrderDate",
        },
        {
            **base,
            "index_column_id": 3,
            "key_ordinal": 0,
            "is_included_column": True,
            "is_descending_key": False,
            "partition_ordinal": 0,
            "column_name": "Total",
        },
    ]


def test_parser_preserves_full_index_metadata() -> None:
    [index] = parse_existing_index_rows(_rows())

    assert [item.as_dict() for item in index.key_columns] == [
        {"name": "Status", "direction": "ASC"},
        {"name": "OrderDate", "direction": "DESC"},
    ]
    assert index.include_columns == ("Total",)
    assert index.filter_definition == "[Status] = (1)"
    assert index.is_unique is True
    assert index.is_unique_constraint is True
    assert index.is_disabled is False
    assert index.partition_columns == ("OrderDate",)
    assert index.data_space_name == "ps_OrderDate"
    assert index.partition_compression == ((1, "PAGE"),)
    assert index.usage["user_updates"] == 3
    assert index.provenance["source"].startswith("sys.indexes")


def test_parser_preserves_missing_usage_counters_as_unavailable() -> None:
    [index] = parse_existing_index_rows(
        [{**_rows()[0], "user_seeks": None, "user_scans": None, "user_lookups": None}]
    )

    assert index.usage["user_seeks"] is None
    assert index.usage["user_scans"] is None
    assert index.usage["user_lookups"] is None
    assert index.is_unused is None


def test_usage_query_preserves_null_dmv_counters() -> None:
    assert "us.user_seeks AS user_seeks" in EXISTING_INDEX_METADATA_SQL
    assert "us.user_scans AS user_scans" in EXISTING_INDEX_METADATA_SQL
    assert "us.user_lookups AS user_lookups" in EXISTING_INDEX_METADATA_SQL
    assert "COALESCE(us.user_seeks" not in EXISTING_INDEX_METADATA_SQL


def test_coverage_requires_keys_directions_includes_filter_and_enabled_state() -> None:
    index = ExistingIndex(
        schema="dbo",
        table="Orders",
        index_id=2,
        name="IX",
        index_type="NONCLUSTERED",
        key_columns=(
            IndexKeyColumn("Status", "ASC"),
            IndexKeyColumn("OrderDate", "DESC"),
        ),
        include_columns=("Total",),
        filter_definition="[Status] = (1)",
    )

    assert existing_index_covers_candidate(
        index,
        schema="dbo",
        table="Orders",
        key_columns=("Status ASC", "OrderDate DESC"),
        include_columns=("Total",),
        filter_definition=" [Status]   = (1) ",
    )
    assert not existing_index_covers_candidate(
        index,
        schema="dbo",
        table="Orders",
        key_columns=("Status ASC", "OrderDate ASC"),
        include_columns=("Total",),
        filter_definition="[Status] = (1)",
    )
    assert not existing_index_covers_candidate(
        index,
        schema="dbo",
        table="Orders",
        key_columns=("Status ASC",),
        include_columns=("MissingCover",),
        filter_definition="[Status] = (1)",
    )
    assert not existing_index_covers_candidate(
        index,
        schema="dbo",
        table="Orders",
        key_columns=("Status ASC",),
        include_columns=("Total",),
        filter_definition=None,
    )
    assert not existing_index_covers_candidate(
        replace(index, is_disabled=True),
        schema="dbo",
        table="Orders",
        key_columns=("Status ASC",),
        include_columns=("Total",),
        filter_definition="[Status] = (1)",
    )


def test_filter_normalization_preserves_identifier_and_literal_spelling() -> None:
    normalized = normalize_index_definition(
        "[STATUS] = N'MiXeD  Value' AND note = 'SELECT' /* DROP */"
    )

    assert normalized == "[STATUS] = N'MiXeD  Value' AND note = 'SELECT'"
    assert normalize_index_definition("[STATUS] = 1") != normalize_index_definition(
        "[status] = 1"
    )
    assert normalize_index_definition("Status = 1") != normalize_index_definition(
        "status = 1"
    )
    assert normalize_index_definition("status = 1 /* comment */") == "status = 1"
    assert normalize_index_definition("status = 'MiXeD'") != (
        "status = 'mixed'"
    )


def test_unique_constraint_and_covering_index_remain_visible() -> None:
    [index] = parse_existing_index_rows(_rows())
    payload = index.as_dict()

    assert payload["is_unique"] is True
    assert payload["is_unique_constraint"] is True
    assert payload["include_columns"] == ["Total"]
    assert payload["filter_definition"] == "[Status] = (1)"


def test_keyless_columnstore_index_remains_in_inventory() -> None:
    row = {
        **_rows()[0],
        "index_id": 9,
        "index_name": "CCI_Orders",
        "index_type": "CLUSTERED COLUMNSTORE",
        "index_column_id": 1,
        "key_ordinal": 0,
        "is_included_column": False,
        "is_descending_key": False,
        "partition_ordinal": 0,
        "column_name": "Status",
    }

    [index] = parse_existing_index_rows([row])

    assert index.name == "CCI_Orders"
    assert index.index_type == "CLUSTERED COLUMNSTORE"
    assert index.key_columns == ()
    assert index.nonkey_columns == ("Status",)


def test_definition_and_ownership_fingerprints_are_stable_and_exposed() -> None:
    [index] = parse_existing_index_rows(_rows())

    assert index.definition_fingerprint == index.fingerprint
    assert index.ownership_fingerprint
    assert index.ownership["index_name"] == index.name
    assert index.ownership["definition_fingerprint"] == index.fingerprint
    assert index.as_dict()["ownership"]["ownership_fingerprint"] == index.ownership_fingerprint


def test_unused_index_is_identifiable_without_treating_disabled_as_usable() -> None:
    [index] = parse_existing_index_rows(
        [
            {
                **_rows()[0],
                "user_seeks": 0,
                "user_scans": 0,
                "user_lookups": 0,
                "user_updates": 12,
            }
        ],
        observation_window_minutes=60,
        usage_context={
            "availability": "available",
            "counter_epoch_utc": "2026-07-01T00:00:00Z",
        },
    )

    assert index.is_unused is True
    assert replace(index, is_disabled=True).is_unused is True


@pytest.mark.parametrize(
    ("counter_epoch", "usage", "expected"),
    [
        ("2026-07-01T00:00:00Z", {"user_seeks": 0, "user_scans": 0, "user_lookups": 0}, True),
        ("2026-07-15T09:30:00Z", {"user_seeks": 0, "user_scans": 0, "user_lookups": 0}, None),
        ("2026-07-15T09:30:00Z", {"user_seeks": 1, "user_scans": 0, "user_lookups": 0}, False),
    ],
)
def test_is_unused_reflects_observation_coverage(
    counter_epoch: str,
    usage: dict[str, int],
    expected: bool | None,
) -> None:
    [index] = parse_existing_index_rows(
        [{**_rows()[0], **usage}],
        observation_window_minutes=60,
        usage_context={
            "availability": "available",
            "counter_epoch_utc": counter_epoch,
        },
    )

    assert index.is_unused is expected


def test_is_unused_is_unknown_when_counter_epoch_probe_fails() -> None:
    [index] = parse_existing_index_rows(
        [{**_rows()[0], "user_seeks": 0, "user_scans": 0, "user_lookups": 0}],
        observation_window_minutes=60,
        usage_context={"availability": "unavailable", "coverage": "unavailable"},
    )

    assert index.usage_context["availability"] == "unavailable"
    assert index.is_unused is None


@pytest.mark.asyncio
async def test_collect_existing_indexes_degrades_engine_start_probe_failure() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_all(self, database_name: str, query: str):
            self.calls += 1
            if self.calls == 1:
                return [
                    {
                        **_rows()[0],
                        "user_seeks": 0,
                        "user_scans": 0,
                        "user_lookups": 0,
                    }
                ]
            raise PermissionError("VIEW SERVER STATE denied")

    [index] = await collect_existing_indexes(
        Executor(),
        "appdb",
        observation_window_minutes=60,
    )

    assert index.usage_context["availability"] == "unavailable"
    assert index.is_unused is None
