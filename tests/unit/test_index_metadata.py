from __future__ import annotations

from dataclasses import replace

from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import IndexKeyColumn
from azure_sql_mcp.index_metadata import existing_index_covers_candidate
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
