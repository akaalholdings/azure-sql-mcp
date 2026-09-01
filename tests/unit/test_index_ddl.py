from __future__ import annotations

from azure_sql_mcp.index_ddl import quote_identifier
from azure_sql_mcp.index_ddl import render_candidate_rollback
from azure_sql_mcp.index_ddl import render_exact_reverse_index_ddl
from azure_sql_mcp.index_ddl import render_drop_index_ddl
from azure_sql_mcp.index_ddl import render_inert_candidate_rollback
from azure_sql_mcp.index_ddl import render_inert_proposed_drop
from azure_sql_mcp.index_ddl import render_proposed_drop_ddl
from azure_sql_mcp.index_ddl import render_reverse_index_ddl
from azure_sql_mcp.index_ddl import render_reverse_index_definition
from azure_sql_mcp.index_ddl import render_validation_selects
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import IndexKeyColumn


def _index(**changes: object) -> ExistingIndex:
    values: dict[str, object] = {
        "schema": "sales]schema",
        "table": "Order]Header",
        "index_id": 2,
        "name": "IX Order]Header [active]",
        "index_type": "NONCLUSTERED",
        "key_columns": (
            IndexKeyColumn("Order]Date", "DESC"),
            IndexKeyColumn("Customer]Id", "ASC"),
        ),
        "include_columns": ("Amount]Gross", "Status"),
        "filter_definition": "[Status] = N'O''Reilly' AND [Code] <> N']'",
        "is_unique": True,
        "is_disabled": False,
        "fill_factor": 87,
        "partition_columns": ("Order]Date",),
        "data_space_name": "ps Order]Date",
        "data_space_type": "PARTITION_SCHEME",
        "partition_scheme_name": "ps Order]Date",
        "partition_compression": ((1, "PAGE"), (2, "ROW"), (3, "PAGE")),
        "xml_compression": ((1, "OFF"), (2, "ON"), (3, "OFF")),
        "object_id": 101,
        "parent_object_type": "USER_TABLE",
        "parent_object_type_code": "U",
        "index_type_code": 2,
        "is_hypothetical": False,
        "is_auto_created": False,
        "has_filter": True,
        "is_padded": True,
        "ignore_dup_key": True,
        "allow_row_locks": False,
        "allow_page_locks": True,
        "optimize_for_sequential_key": True,
        "suppress_dup_key_messages": False,
        "statistics_no_recompute": True,
        "statistics_incremental": False,
    }
    values.update(changes)
    return ExistingIndex(**values)  # type: ignore[arg-type]


def test_quote_identifier_escapes_closing_brackets() -> None:
    assert quote_identifier("a]b") == "[a]]b]"


def test_exact_reverse_ddl_round_trips_all_definition_options() -> None:
    result = render_exact_reverse_index_ddl(_index())
    ddl = result["ddl"]

    assert result["executable"] is True
    assert result["blockers"] == []
    assert "SET ANSI_NULLS ON;" in ddl
    assert "CREATE UNIQUE NONCLUSTERED INDEX [IX Order]]Header [active]]]" in ddl
    assert "ON [sales]]schema].[Order]]Header]" in ddl
    assert "[Order]]Date] DESC, [Customer]]Id] ASC" in ddl
    assert "INCLUDE ([Amount]]Gross], [Status])" in ddl
    assert "WHERE [Status] = N'O''Reilly' AND [Code] <> N']'" in ddl
    assert "PAD_INDEX = ON" in ddl
    assert "FILLFACTOR = 87" in ddl
    assert "IGNORE_DUP_KEY = ON" in ddl
    assert "STATISTICS_NORECOMPUTE = ON" in ddl
    assert "STATISTICS_INCREMENTAL = OFF" in ddl
    assert "ALLOW_ROW_LOCKS = OFF" in ddl
    assert "ALLOW_PAGE_LOCKS = ON" in ddl
    assert "OPTIMIZE_FOR_SEQUENTIAL_KEY = ON" in ddl
    assert "SUPPRESS_DUP_KEY_MESSAGES" not in ddl
    assert "ON [ps Order]]Date] ([Order]]Date])" in ddl
    assert "REBUILD PARTITIONS" not in ddl
    assert (
        "REBUILD PARTITION = 1 WITH (DATA_COMPRESSION = PAGE, XML_COMPRESSION = OFF)"
        in ddl
    )
    assert (
        "REBUILD PARTITION = 2 WITH (DATA_COMPRESSION = ROW, XML_COMPRESSION = ON)"
        in ddl
    )
    assert (
        "REBUILD PARTITION = 3 WITH (DATA_COMPRESSION = PAGE, XML_COMPRESSION = OFF)"
        in ddl
    )


def test_reverse_ddl_refuses_incomplete_or_unsupported_metadata() -> None:
    for changed, blocker in (
        ({"filter_definition": None}, "filtered_predicate_unavailable"),
        ({"index_type_code": 5}, "index_type_is_not_nonclustered_rowstore"),
        ({"parent_object_type_code": "V"}, "parent_is_not_user_table"),
        ({"is_auto_created": True}, "index_is_auto_created"),
        ({"is_disabled": True}, "index_is_disabled"),
        ({"suppress_dup_key_messages": None}, "suppress_dup_key_messages_unavailable"),
    ):
        result = render_reverse_index_ddl(_index(**changed))
        assert result["executable"] is False
        assert result["ddl"] is None
        assert blocker in result["blockers"]

    result = render_reverse_index_ddl(_index(suppress_dup_key_messages=True))
    assert result["executable"] is False
    assert result["ddl"] is None
    assert "suppress_dup_key_messages_unsupported" in result["blockers"]


def test_persisted_reversible_definition_reconstructs_exact_ddl() -> None:
    index = _index()
    direct = render_reverse_index_ddl(index)
    restored = render_reverse_index_definition(index.reversible_definition)
    assert restored["executable"] is True
    assert restored["reverse_ddl"] == direct["reverse_ddl"]


def _subject(index: ExistingIndex, state: str) -> dict[str, object]:
    return {
        "state": state,
        "subject_id": f"index:{index.object_id}:{index.index_id}",
        "definition": {"reversible_definition": index.reversible_definition},
    }


def test_drop_rendering_is_the_exact_quoted_inverse_for_reversible_definition() -> None:
    result = render_drop_index_ddl(_index())

    assert result == (
        "DROP INDEX [IX Order]]Header [active]]] ON [sales]]schema].[Order]]Header];"
    )


def test_persisted_blockers_and_malformed_coverage_fail_closed() -> None:
    definition = _index().reversible_definition
    definition["reversibility_blockers"] = ["coverage_incomplete"]
    result = render_reverse_index_definition(definition)
    assert result["executable"] is False
    assert result["ddl"] is None
    assert result["reverse_ddl"] is None
    assert result["drop_ddl"] is None
    assert result["blockers"] == ["coverage_incomplete"]

    malformed_include = _index().reversible_definition
    malformed_include["include_columns"] = "not-a-column-list"
    malformed_result = render_reverse_index_definition(malformed_include)
    assert malformed_result["executable"] is False
    assert malformed_result["blockers"] == ["include_definition_malformed"]

    subject = _subject(_index(), "drop_candidate")
    subject["definition"]["reversibility_blockers"] = ["coverage_incomplete"]
    assert render_proposed_drop_ddl(subject) is None
    assert render_candidate_rollback(subject) is None


def test_rollback_is_exact_and_only_for_removal_candidates() -> None:
    index = _index()
    expected = render_reverse_index_ddl(index)["reverse_ddl"]
    for state in ("drop_candidate", "consolidate_candidate"):
        subject = _subject(index, state)
        assert render_candidate_rollback(subject) == expected
        inert = render_inert_candidate_rollback(subject)
        assert inert is not None
        assert all(line.startswith("--") for line in inert.splitlines())
        assert "CREATE UNIQUE NONCLUSTERED INDEX" in inert

    for state in ("create_candidate", "keep", "observe"):
        subject = _subject(index, state)
        assert render_candidate_rollback(subject) is None
        assert render_inert_candidate_rollback(subject) is None


def test_consolidation_drop_is_candidate_only_and_remains_inert() -> None:
    subject = _subject(_index(), "consolidate_candidate")
    subject["surviving_covering_index"] = "IX Covering"

    proposed = render_proposed_drop_ddl(subject)
    inert = render_inert_proposed_drop(subject)
    assert proposed == (
        "DROP INDEX [IX Order]]Header [active]]] ON [sales]]schema].[Order]]Header];"
    )
    assert inert is not None
    assert "-- Surviving covering index: [IX Covering]." in inert
    assert "-- DROP INDEX [IX Order]]Header [active]]] ON" in inert
    assert all(line.startswith("--") for line in inert.splitlines())
    assert "safe to drop" not in inert.lower()

    for state in ("keep", "observe"):
        assert render_proposed_drop_ddl(_subject(_index(), state)) is None
        assert render_inert_proposed_drop(_subject(_index(), state)) is None


def test_validation_selects_are_concrete_commented_and_deterministic() -> None:
    subject = _subject(_index(), "drop_candidate")
    first = render_validation_selects(subject)
    second = render_validation_selects(subject)

    assert first == second
    assert "-- SELECT" in first
    assert "-- FROM sys.indexes AS i" in first
    assert "-- WHERE i.object_id = 101 AND i.index_id = 2;" in first
    assert "-- FROM sys.index_columns AS ic" in first
    assert "-- FROM sys.dm_db_index_usage_stats" in first
    assert "-- Expected key columns: [Order]]Date] DESC, [Customer]]Id] ASC." in first
    assert "-- Expected included columns: [Amount]]Gross], [Status]." in first
    assert all(line.startswith("--") for line in first.splitlines())
    assert "safe to drop" not in first.lower()


def test_validation_for_create_candidate_uses_escaped_table_literals() -> None:
    subject = {
        "state": "create_candidate",
        "subject_id": "missing:candidate-1",
        "schema_name": "sales]schema",
        "table_name": "Order'Header",
        "key_columns": ["Amount]Gross DESC"],
        "include_columns": ["Status]Code"],
    }

    result = render_validation_selects(subject)

    assert "-- WHERE s.name = N'sales]schema' AND o.name = N'Order''Header'" in result
    assert "-- Expected key columns: [Amount]]Gross DESC] ASC." in result
    assert "-- Expected included columns: [Status]]Code]." in result
    assert all(line.startswith("--") for line in result.splitlines())
