from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import DATABASE_INCARNATION_SQL
from azure_sql_mcp.index_metadata import ENGINE_START_TIME_SQL
from azure_sql_mcp.index_metadata import INDEX_PROTECTION_METADATA_SQL
from azure_sql_mcp.index_metadata import collect_existing_indexes
from azure_sql_mcp.index_metadata import EXISTING_INDEX_METADATA_SQL
from azure_sql_mcp.index_metadata import IndexKeyColumn
from azure_sql_mcp.index_metadata import reversible_index_definition_payload
from azure_sql_mcp.index_metadata import existing_index_covers_candidate
from azure_sql_mcp.index_metadata import normalize_index_definition
from azure_sql_mcp.index_metadata import parse_existing_index_rows
from azure_sql_mcp.index_metadata import parse_protection_evidence
from azure_sql_mcp.index_metadata import reversible_index_definition_fingerprint


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


def test_catalog_parser_preserves_directional_and_delimited_identifier_spelling() -> None:
    row = {
        **_complete_rows()[0],
        "schema_name": "[sales]",
        "table_name": "[Orders]",
        "index_name": "[IX Status]",
        "column_name": "Status ASC",
        "partition_ordinal": 0,
    }

    [index] = parse_existing_index_rows([row])

    assert index.schema == "[sales]"
    assert index.table == "[Orders]"
    assert index.name == "[IX Status]"
    assert index.key_columns == (IndexKeyColumn("Status ASC", "DESC"),)
    assert index.reversible_definition["schema"] == "[sales]"
    assert index.reversible_definition["table"] == "[Orders]"
    assert index.reversible_definition["index_name"] == "[IX Status]"
    assert index.reversible_definition["key_columns"] == [
        {"name": "Status ASC", "direction": "DESC"}
    ]


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
    assert "i.auto_created AS auto_created" in EXISTING_INDEX_METADATA_SQL
    assert "i.is_auto_created" not in EXISTING_INDEX_METADATA_SQL


def test_azure_sql_identity_queries_use_physical_database_and_server_epoch() -> None:
    assert "sys.databases" in DATABASE_INCARNATION_SQL
    assert "physical_database_name" in DATABASE_INCARNATION_SQL
    assert "name = DB_NAME()" in DATABASE_INCARNATION_SQL
    assert "sys.database_recovery_status" not in DATABASE_INCARNATION_SQL
    assert "database_guid" not in DATABASE_INCARNATION_SQL

    assert "sqlserver_start_time" in ENGINE_START_TIME_SQL
    assert "COALESCE(SERVERPROPERTY('ServerName'), @@SERVERNAME, 'azure-sql-database')" in ENGINE_START_TIME_SQL
    assert "MachineName" not in ENGINE_START_TIME_SQL


def test_protection_catalog_fields_are_translated_to_persisted_contract_values() -> None:
    [evidence] = list(
        parse_protection_evidence(
            [
                {
                    "object_id": 101,
                    "index_id": 2,
                    "index_type_code": 2,
                    "is_unique": 0,
                    "is_primary_key": 0,
                    "is_unique_constraint": 0,
                    "is_disabled": 0,
                    "is_hypothetical": 0,
                    "auto_created": 1,
                    "partition_switch_dependency": 0,
                    "is_indexed_view": 0,
                    "has_index_extended_properties": 0,
                }
            ]
        ).values()
    )
    assert evidence["coverage"] == "complete"
    assert evidence["automatic_tuning"] is True
    assert evidence["partition_switch_dependency"] is False
    assert "CASE WHEN ds.type = 'PS'" in INDEX_PROTECTION_METADATA_SQL
    assert "i.type_desc AS index_type" in INDEX_PROTECTION_METADATA_SQL


def test_only_a_complete_leading_child_foreign_key_is_protected() -> None:
    base = {
        "object_id": 101,
        "index_id": 2,
        "index_type_code": 2,
        "index_type": "NONCLUSTERED",
        "is_unique": 0,
        "is_primary_key": 0,
        "is_unique_constraint": 0,
        "is_disabled": 0,
        "is_hypothetical": 0,
        "auto_created": 0,
        "partition_switch_dependency": 0,
        "is_indexed_view": 0,
        "has_index_extended_properties": 0,
        "child_foreign_key_id": 7001,
        "child_object_id": 101,
    }
    unrelated = parse_protection_evidence(
        [
            {
                **base,
                "child_constraint_column_ordinal": 1,
                "child_index_key_ordinal": None,
            },
            {
                **base,
                "child_constraint_column_ordinal": 2,
                "child_index_key_ordinal": None,
            },
        ]
    )[(101, 2)]
    supporting = parse_protection_evidence(
        [
            {
                **base,
                "child_constraint_column_ordinal": 1,
                "child_index_key_ordinal": 1,
            },
            {
                **base,
                "child_constraint_column_ordinal": 2,
                "child_index_key_ordinal": 2,
            },
        ]
    )[(101, 2)]

    assert unrelated["coverage"] == "complete"
    assert unrelated["safe_to_remove"] is True
    assert unrelated["child_foreign_key_support"] == []
    assert supporting["child_foreign_key_support"] == [
        {
            "foreign_key_id": 7001,
            "child_object_id": 101,
            "leading_key_supported": True,
            "constraint_ordinals": [1, 2],
            "index_key_ordinals": [1, 2],
        }
    ]


def _complete_rows() -> list[dict[str, object]]:
    base = {
        "schema_name": "sales",
        "table_name": "Order]Header",
        "object_id": 101,
        "parent_object_type": "USER_TABLE",
        "parent_object_type_code": "U",
        "index_id": 2,
        "index_name": "IX Order]Header",
        "index_type_code": 2,
        "index_type": "NONCLUSTERED",
        "is_unique": True,
        "is_primary_key": False,
        "is_unique_constraint": False,
        "is_disabled": False,
        "is_hypothetical": False,
        "auto_created": False,
        "has_filter": True,
        "filter_definition": "[Status] = N'O''Reilly' AND [Code] <> N']'",
        "is_padded": True,
        "ignore_dup_key": True,
        "allow_row_locks": False,
        "allow_page_locks": True,
        "optimize_for_sequential_key": True,
        "suppress_dup_key_messages": False,
        "statistics_no_recompute": True,
        "statistics_incremental": False,
        "fill_factor": 87,
        "data_space_name": "ps Order]Date",
        "data_space_type": "PARTITION_SCHEME",
        "partition_scheme_name": "ps Order]Date",
        "partition_function_name": "pf Order]Date",
        "collected_at_utc": "2026-07-15T10:00:00Z",
        "user_seeks": 0,
        "user_scans": 0,
        "user_lookups": 0,
        "user_updates": 0,
    }
    return [
        {
            **base,
            "index_column_id": 1,
            "key_ordinal": 1,
            "is_included_column": False,
            "is_descending_key": True,
            "partition_ordinal": 1,
            "column_name": "Order]Date",
            "partition_number": 1,
            "data_compression_desc": "PAGE",
            "xml_compression_desc": "OFF",
            "partition_row_count": 10,
            "partition_page_count": 20,
        },
        {
            **base,
            "index_column_id": 2,
            "key_ordinal": 2,
            "is_included_column": False,
            "is_descending_key": False,
            "partition_ordinal": 0,
            "column_name": "Code]Part",
            "partition_number": 2,
            "data_compression_desc": "ROW",
            "xml_compression_desc": "ON",
            "partition_row_count": 11,
            "partition_page_count": 21,
        },
        {
            **base,
            "index_column_id": 3,
            "key_ordinal": 0,
            "is_included_column": True,
            "is_descending_key": False,
            "partition_ordinal": 0,
            "column_name": "Amount]Gross",
            "partition_number": 1,
            "data_compression_desc": "PAGE",
            "xml_compression_desc": "OFF",
            "partition_row_count": 10,
            "partition_page_count": 20,
        },
    ]


def test_reversible_metadata_is_separate_from_legacy_lease_fingerprint() -> None:
    [index] = parse_existing_index_rows(_complete_rows())
    changed = replace(index, allow_page_locks=not index.allow_page_locks)

    assert index.definition_fingerprint != changed.definition_fingerprint
    assert index.reversible_definition_fingerprint_v1 != (
        changed.reversible_definition_fingerprint_v1
    )
    assert index.reversible_definition["schema"] == "sales"
    assert index.reversible_definition["table"] == "Order]Header"
    assert index.reversible_definition["filter"]["definition"].endswith("N']'")
    assert index.is_reversible is True


def test_parser_collects_options_partition_stats_xml_compression_and_identity() -> None:
    [index] = parse_existing_index_rows(_complete_rows())

    assert index.object_id == 101
    assert index.parent_object_type_code == "U"
    assert index.index_type_code == 2
    assert index.is_auto_created is False
    assert index.has_filter is True
    assert index.is_padded is True
    assert index.ignore_dup_key is True
    assert index.allow_row_locks is False
    assert index.allow_page_locks is True
    assert index.optimize_for_sequential_key is True
    assert index.suppress_dup_key_messages is False
    assert index.statistics_no_recompute is True
    assert index.statistics_incremental is False
    assert index.partition_compression == ((1, "PAGE"), (2, "ROW"))
    assert index.xml_compression == ((1, "OFF"), (2, "ON"))
    assert index.has_index_extended_properties is False
    assert index.partition_row_counts == ((1, 10), (2, 11))
    assert index.partition_page_counts == ((1, 20), (2, 21))
    assert reversible_index_definition_payload(index)["data_space"]["partition_columns"] == [
        "Order]Date"
    ]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("is_unique", False),
        ("is_primary_key", True),
        ("is_unique_constraint", True),
        ("constraint_name", "UQ_OrderHeader"),
        ("constraint_type", "UNIQUE_CONSTRAINT"),
        ("is_disabled", True),
        ("is_hypothetical", True),
        ("is_auto_created", True),
        ("is_padded", False),
        ("fill_factor", 88),
        ("ignore_dup_key", False),
        ("statistics_no_recompute", False),
        ("statistics_incremental", True),
        ("allow_row_locks", True),
        ("allow_page_locks", False),
        ("optimize_for_sequential_key", False),
        ("suppress_dup_key_messages", True),
        ("index_type", "CLUSTERED"),
        ("index_type_code", 1),
        ("has_filter", False),
        ("filter_definition", "[Status] = (2)"),
        ("schema", "archive"),
        ("table", "OrderHeaderArchive"),
        ("index_id", 3),
        ("name", "IX_OrderHeader_Archive"),
        ("object_id", 102),
        ("parent_object_type", "VIEW"),
        ("parent_object_type_code", "V"),
        ("data_space_name", "PRIMARY"),
        ("data_space_type", "FILEGROUP"),
        ("partition_scheme_name", "ps_Other"),
        ("partition_function_name", "pf_Other"),
        ("partition_columns", ("Code]Part",)),
        ("partition_compression", ((1, "ROW"), (2, "ROW"))),
        ("xml_compression", ((1, "ON"), (2, "OFF"))),
        ("nonkey_columns", ("Other]Column",)),
    ],
)
def test_reversible_fingerprint_includes_each_identity_and_reversible_option(
    field: str,
    value: object,
) -> None:
    [index] = parse_existing_index_rows(_complete_rows())

    changed = replace(index, **{field: value})

    assert index.reversible_definition_fingerprint_v1 != (
        changed.reversible_definition_fingerprint_v1
    )


def test_reversible_fingerprint_preserves_key_order_and_direction() -> None:
    [index] = parse_existing_index_rows(_complete_rows())

    reordered = replace(index, key_columns=tuple(reversed(index.key_columns)))
    direction_changed = replace(
        index,
        key_columns=(
            IndexKeyColumn(index.key_columns[0].name, "ASC"),
            index.key_columns[1],
        ),
    )

    assert index.reversible_definition["key_columns"] == [
        {"name": "Order]Date", "direction": "DESC"},
        {"name": "Code]Part", "direction": "ASC"},
    ]
    assert index.reversible_definition_fingerprint_v1 != (
        reordered.reversible_definition_fingerprint_v1
    )
    assert index.reversible_definition_fingerprint_v1 != (
        direction_changed.reversible_definition_fingerprint_v1
    )


def test_reversible_payload_and_fingerprint_have_canonical_parity() -> None:
    [index] = parse_existing_index_rows(_complete_rows())
    payload = index.reversible_definition
    expected = hashlib.sha256(
        json.dumps(
            {
                "version": "reversible_definition_fingerprint_v1",
                "definition": payload,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()

    assert reversible_index_definition_payload(index) == payload
    assert reversible_index_definition_fingerprint(payload) == expected
    assert reversible_index_definition_fingerprint(index) == expected
    assert index.reversible_definition_fingerprint_v1 == expected


def test_reversible_payload_is_stable_for_catalog_row_and_partition_order() -> None:
    [index] = parse_existing_index_rows(_complete_rows())
    [reordered_rows] = parse_existing_index_rows(list(reversed(_complete_rows())))
    manually_reordered = replace(
        index,
        partition_compression=tuple(reversed(index.partition_compression)),
        xml_compression=tuple(reversed(index.xml_compression)),
    )

    assert index.reversible_definition == reordered_rows.reversible_definition
    assert index.reversible_definition_fingerprint_v1 == (
        reordered_rows.reversible_definition_fingerprint_v1
    )
    assert index.definition_fingerprint == reordered_rows.definition_fingerprint
    assert index.reversible_definition == manually_reordered.reversible_definition


def test_filtered_predicate_unavailable_is_not_reversible() -> None:
    [index] = parse_existing_index_rows(
        [{**_complete_rows()[0], "filter_definition": None}]
    )

    assert index.is_reversible is False
    assert "filtered_predicate_unavailable" in index.reversibility_blockers


def test_protection_evidence_preserves_fk_roles_and_composite_order() -> None:
    rows = _complete_rows()
    evidence = {
        (101, 2): {
            "coverage": "covered",
            "primary_key": False,
            "unique_constraint": False,
            "standalone_unique": True,
            "clustered": False,
            "indexed_view": False,
            "referenced_foreign_key_key_index_ids": [7001],
            "child_foreign_key_support": [
                {
                    "foreign_key_id": 7002,
                    "leading_key_supported": True,
                    "constraint_ordinals": [1, 2],
                    "index_key_ordinals": [1, 2],
                }
            ],
            "automatic_tuning": False,
            "hinted_or_forced_plan": False,
            "partition_switch_dependency": False,
            "has_index_extended_properties": True,
            "disabled": False,
            "hypothetical": False,
            "specialist_type": None,
        }
    }

    [index] = parse_existing_index_rows(rows, protection_evidence=evidence)

    assert index.protection_evidence["referenced_foreign_key_key_index_ids"] == [7001]
    assert index.protection_evidence["child_foreign_key_support"][0][
        "leading_key_supported"
    ] is True
    assert index.protection_evidence["has_index_extended_properties"] is True


@pytest.mark.parametrize(
    ("field", "value"),
    [("is_indexed_view", True), ("auto_created", True), ("index_type_code", 5)],
)
def test_special_protection_metadata_is_explicit(field: str, value: object) -> None:
    row = {**_complete_rows()[0], field: value}
    if field == "is_indexed_view":
        row["parent_object_type_code"] = "V"
    if field == "index_type_code":
        row["index_type"] = "CLUSTERED COLUMNSTORE"
    [index] = parse_existing_index_rows([row])

    assert index.protection_evidence["indexed_view"] is (field == "is_indexed_view")
    if field == "auto_created":
        assert index.protection_evidence["auto_created"] is True
    if field == "index_type_code":
        assert index.protection_evidence["specialist_type"] == "CLUSTERED COLUMNSTORE"


@pytest.mark.parametrize(
    ("index_type_code", "index_type", "expected"),
    [
        (3, None, "XML"),
        (4, None, "SPATIAL"),
        (5, None, "CLUSTERED COLUMNSTORE"),
        (6, None, "NONCLUSTERED COLUMNSTORE"),
        (7, None, "NONCLUSTERED HASH"),
        (9, None, "JSON"),
        (42, "CUSTOM SPECIALIST", "CUSTOM SPECIALIST"),
    ],
)
def test_protection_parser_preserves_specialist_index_types(
    index_type_code: int,
    index_type: str | None,
    expected: str,
) -> None:
    evidence = parse_protection_evidence(
        [
            {
                "object_id": 101,
                "index_id": 2,
                "index_type_code": index_type_code,
                "index_type": index_type,
                "partition_switch_dependency": 0,
            }
        ]
    )[(101, 2)]

    assert evidence["specialist_type"] == expected


def test_existing_index_metadata_replaces_falsey_specialist_placeholder() -> None:
    row = {
        **_complete_rows()[0],
        "index_type_code": 6,
        "index_type": "NONCLUSTERED COLUMNSTORE",
    }
    [index] = parse_existing_index_rows(
        [row],
        protection_evidence={
            (101, 2): {
                "coverage": "complete",
                "specialist_type": None,
                "partition_switch_dependency": False,
            }
        },
    )

    assert index.protection_evidence["specialist_type"] == "NONCLUSTERED COLUMNSTORE"


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


@pytest.mark.parametrize(
    "column_name",
    ("Order Date", "Line]Number", "Last, First", "År Månad", "Order Date DESC"),
)
def test_covering_match_treats_unrestricted_candidate_names_as_exact_identifiers(
    column_name: str,
) -> None:
    index = ExistingIndex(
        schema="dbo",
        table="Order Details",
        index_id=2,
        name="IX_Exact_Name",
        index_type="NONCLUSTERED",
        key_columns=(IndexKeyColumn(column_name, "ASC"),),
    )

    assert existing_index_covers_candidate(
        index,
        schema="dbo",
        table="Order Details",
        key_columns=(column_name,),
    )
    assert not existing_index_covers_candidate(
        replace(index, key_columns=(IndexKeyColumn(column_name, "DESC"),)),
        schema="dbo",
        table="Order Details",
        key_columns=(column_name,),
    )


def test_covering_match_preserves_explicit_bracketed_and_structured_directions() -> None:
    index = ExistingIndex(
        schema="dbo",
        table="Orders",
        index_id=2,
        name="IX_Explicit_Direction",
        index_type="NONCLUSTERED",
        key_columns=(IndexKeyColumn("Order]Date", "DESC"),),
        include_columns=("Last, First", "Månad Summa", "Line]Total"),
    )

    assert existing_index_covers_candidate(
        index,
        schema="dbo",
        table="Orders",
        key_columns=("[Order]]Date] DESC",),
        include_columns=("Last, First", "Månad Summa", "[Line]]Total]"),
    )
    assert existing_index_covers_candidate(
        index,
        schema="dbo",
        table="Orders",
        key_columns=(IndexKeyColumn("Order]Date", "DESC"),),
        include_columns=("Last, First",),
    )


def test_exact_catalog_coverage_does_not_parse_directional_looking_names() -> None:
    plain = ExistingIndex(
        schema="dbo",
        table="Orders",
        index_id=2,
        name="IX_Plain",
        index_type="NONCLUSTERED",
        key_columns=(IndexKeyColumn("Status", "ASC"),),
        include_columns=("Payload",),
    )
    directional_name = replace(
        plain,
        name="IX_Directional_Name",
        key_columns=(IndexKeyColumn("Status ASC", "ASC"),),
        include_columns=("[Payload]",),
    )

    assert existing_index_covers_candidate(
        plain,
        schema="dbo",
        table="Orders",
        key_columns=("Status ASC",),
        include_columns=("Payload",),
    )
    assert not existing_index_covers_candidate(
        plain,
        schema="dbo",
        table="Orders",
        key_columns=("Status ASC",),
        include_columns=("Payload",),
        exact_catalog_names=True,
    )
    assert existing_index_covers_candidate(
        directional_name,
        schema="dbo",
        table="Orders",
        key_columns=("Status ASC",),
        include_columns=("[Payload]",),
        exact_catalog_names=True,
    )
    assert not existing_index_covers_candidate(
        directional_name,
        schema="dbo",
        table="Orders",
        key_columns=("Status ASC",),
        include_columns=("Payload",),
        exact_catalog_names=True,
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
async def test_collect_existing_indexes_does_not_hide_engine_start_permission_failure() -> None:
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

    with pytest.raises(PermissionError, match="VIEW SERVER STATE denied"):
        await collect_existing_indexes(
            Executor(),
            "appdb",
            observation_window_minutes=60,
        )
