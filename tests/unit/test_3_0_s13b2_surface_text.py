"""Server text and argument names an agent reads or reaches for (friction F26, P10).

Each refused call here cost a round trip: the description named a kind the
code refused, and the refusal did not name the accepted fields.
"""

from __future__ import annotations

import inspect
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.catalog_constraints import PRECONDITION_KINDS
from azure_sql_mcp.catalog_constraints import TableConstraintFacts
from azure_sql_mcp.catalog_constraints import UniqueKey
from azure_sql_mcp.catalog_constraints import verify_precondition
from azure_sql_mcp.index_optimizer import IndexCandidate
from azure_sql_mcp.server import _BUILT_INDEX_SQL
from azure_sql_mcp.tool_guides import MAX_GUIDE_CHARS
from azure_sql_mcp.tool_guides import get_tool_guide
from azure_sql_mcp.tool_guides import list_tool_guides
from tests.unit.test_index_benchmark_workflow import _app


def _facts() -> TableConstraintFacts:
    return TableConstraintFacts(
        schema_name="dbo",
        table_name="Users",
        exists=True,
        object_type="USER_TABLE",
        nullable={"id": False, "displayname": True},
        unique_keys=(
            UniqueKey(name="PK_Users", columns=("Id",), kind="primary_key"),
        ),
    )


def _tool_field(app: Any, tool: str, field: str) -> str:
    properties = app.mcp._tool_manager.get_tool(tool).parameters["properties"]
    return " ".join(str(properties[field].get("description", "")).split())


# --- logical preconditions --------------------------------------------------------


def test_finalize_description_lists_exactly_the_kinds_the_code_accepts(
    server_config_factory,
) -> None:
    text = _tool_field(_app(server_config_factory), "finalize_tuning_session", "logical_preconditions")

    for kind in PRECONDITION_KINDS:
        assert kind in text, kind
    # The exact field names of each kind, so the first call is accepted.
    for field in (
        "schema_name", "table_name", "columns", "referenced_schema", "referenced_table",
        "referenced_columns", "constraint_name", "expected_predicate",
    ):
        assert field in text, field
    assert "kind: not_null | unique | trusted_foreign_key | trusted_check" in text


def test_the_kind_and_column_spellings_agents_reached_for_are_accepted() -> None:
    """D01 and D05: unique_key, column_name and column were each refused once."""

    by_alias = verify_precondition(_facts(), {"kind": "unique_key", "column_name": "Id"})
    by_list = verify_precondition(_facts(), {"kind": "not_null", "column_names": ["Id"]})
    by_one = verify_precondition(_facts(), {"kind": "not_null", "column": "Id"})

    assert by_alias["verified"] is True and by_alias["kind"] == "unique"
    assert by_alias["columns"] == ["Id"]
    assert by_list["verified"] is True
    assert by_one["verified"] is True


def test_a_nullable_column_is_still_refuted_under_an_alias() -> None:
    result = verify_precondition(_facts(), {"kind": "not_null", "column": "DisplayName"})

    assert result["verified"] is False
    assert "DisplayName" in result["reason"]


def test_two_column_spellings_in_one_precondition_are_refused_not_guessed() -> None:
    result = verify_precondition(
        _facts(), {"kind": "not_null", "columns": ["Id"], "column": "DisplayName"}
    )

    assert result["verified"] is False
    assert "one of columns, column_names, column_name, column" in result["reason"]


def test_an_unknown_kind_names_the_accepted_kinds_and_fields() -> None:
    result = verify_precondition(_facts(), {"kind": "primary_key", "columns": ["Id"]})

    assert result["verified"] is False
    for kind in PRECONDITION_KINDS:
        assert kind in result["reason"]
    assert "schema_name, table_name, columns" in result["reason"]


# --- guides -------------------------------------------------------------------------


def _all_guide_text() -> dict[str, str]:
    return {
        entry["name"]: " ".join(get_tool_guide(entry["name"])["guide"].split())
        for entry in list_tool_guides()
    }


def test_no_guide_sends_every_index_candidate_through_the_workload_review() -> None:
    """O22: a 34 KB review for a one-statement index whose design the plan already gave."""

    guides = _all_guide_text()

    assert "Take keys and includes from review_workload_indexes" not in guides[
        "benchmark_index_candidate"
    ]
    design = guides["index_design"]
    assert "from the plan" in design
    assert "equality columns" in design and "ORDER BY" in design
    for name, text in guides.items():
        assert "Design the index with review_workload_indexes" not in text, name


def test_the_review_guide_says_when_the_workload_review_is_the_right_tool() -> None:
    text = _all_guide_text()["review_workload_indexes"]

    assert "whole index set" in text
    assert "single statement" in text and "from the plan" in text


def test_guides_state_the_parent_rule_and_the_one_call_path() -> None:
    guides = _all_guide_text()
    text = guides["index_set_experiment"]

    assert "candidate_sql" in text
    assert "whose parent is the measured rewrite" not in text
    one_call = guides["rewrite_with_indexes"]
    assert "candidate_sql" in one_call and "rewrite_plus_index" in one_call
    assert "neutral" in one_call and "matched" in one_call
    assert "one call" in one_call


def test_every_guide_stays_within_the_size_limit() -> None:
    for entry in list_tool_guides():
        assert len(get_tool_guide(entry["name"])["guide"]) <= MAX_GUIDE_CHARS, entry["name"]


# --- built index facts on a partitioned table ---------------------------------------


def test_depth_of_a_partitioned_index_is_read_per_partition() -> None:
    """Live: INDEXPROPERTY(..., 'IndexDepth') is NULL for a partitioned index."""

    sql = " ".join(_BUILT_INDEX_SQL.split())

    assert "sys.dm_db_index_physical_stats" in sql
    assert "MAX(" in sql and "index_depth" in sql
    assert "'LIMITED'" in sql


@pytest.mark.asyncio
async def test_a_missing_depth_alone_does_not_say_the_size_could_not_be_read(
    server_config_factory,
) -> None:
    app = _app(server_config_factory)

    async def fetch(_database: str, _sql: str, **_kwargs: Any) -> list[dict[str, Any]]:
        return [{"index_depth": None, "used_pages": 2560, "row_count": 16_000_000}]

    app.executor.fetch_all = fetch  # type: ignore[method-assign]
    member = SimpleNamespace(
        schema="dw",
        table="FactPost",
        index_name="IX_Testing_x",
        candidate=IndexCandidate(
            schema="dw", table="FactPost", key_columns=("PostId",), include_columns=()
        ),
    )

    facts = await app._built_index_facts("appdb", member)

    assert facts["size_mb"] == 20.0
    assert facts["row_count"] == 16_000_000
    assert facts["index_depth"] is None
    assert facts["facts_note"] == "The B-tree depth of the built index could not be read."


def test_built_index_facts_reads_depth_from_the_physical_stats_parameters() -> None:
    # The statement takes schema, table and index name once each, in that order.
    assert _BUILT_INDEX_SQL.count("?") == 3
    assert inspect.isfunction(get_tool_guide)


# --- argument names agents reached for (F26) -----------------------------------------


async def _call(app: Any, name: str, arguments: dict[str, Any]) -> Any:
    return await app.mcp._tool_manager.call_tool(name, arguments)


@pytest.mark.asyncio
async def test_an_unknown_argument_is_refused_with_the_accepted_names(server_config_factory) -> None:
    """About seven refused calls: the refusal did not say which names the tool takes."""

    app = _app(server_config_factory)

    with pytest.raises(Exception) as refusal:
        await _call(app, "get_column_statistics", {"schema_name": "dbo", "table": "Users"})

    payload = json.loads(str(refusal.value))
    details = payload["details"]
    assert "table_name" in details["accepted_arguments"]
    assert "columns" in details["accepted_arguments"]
    unknown = next(issue for issue in details["issues"] if issue["path"] == "table")
    assert unknown["did_you_mean"] == "table_name"


@pytest.mark.asyncio
async def test_get_object_details_takes_a_two_part_name_or_table_name(server_config_factory) -> None:
    app = _app(server_config_factory)
    app._get_object_details = AsyncMock(return_value={"name": "FactPost"})  # type: ignore[method-assign]

    await _call(app, "get_object_details", {"object_name": "dw.FactPost"})
    two_part = app._get_object_details.await_args.args
    await _call(app, "get_object_details", {"schema_name": "dw", "table_name": "FactPost"})
    by_table = app._get_object_details.await_args.args
    await _call(app, "get_object_details", {"object_name": "[dw].[FactPost]"})
    bracketed = app._get_object_details.await_args.args

    assert two_part[1:3] == by_table[1:3] == bracketed[1:3] == ("dw", "FactPost")


@pytest.mark.asyncio
async def test_get_object_details_without_a_schema_says_what_to_pass(server_config_factory) -> None:
    app = _app(server_config_factory)
    app._get_object_details = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(Exception, match="schema_name"):
        await _call(app, "get_object_details", {"object_name": "FactPost"})
    with pytest.raises(Exception, match="object_name or table_name, not both"):
        await _call(
            app, "get_object_details",
            {"schema_name": "dw", "object_name": "FactPost", "table_name": "DimUser"},
        )

    app._get_object_details.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_column_statistics_takes_column_names_and_column_name(server_config_factory) -> None:
    app = _app(server_config_factory)
    app.column_statistics.get_column_statistics = AsyncMock(return_value={"columns": []})  # type: ignore[method-assign]
    base = {"schema_name": "dbo", "table_name": "Users"}

    await _call(app, "get_column_statistics", {**base, "column_names": ["DisplayName", "Reputation"]})
    plural = app.column_statistics.get_column_statistics.await_args.args
    await _call(app, "get_column_statistics", {**base, "column_name": "DisplayName"})
    single = app.column_statistics.get_column_statistics.await_args.args

    assert plural[3] == ["DisplayName", "Reputation"]
    assert single[3] == ["DisplayName"]
    with pytest.raises(Exception, match="one of columns, column_names, column_name"):
        await _call(
            app, "get_column_statistics", {**base, "columns": ["Id"], "column_name": "DisplayName"}
        )


@pytest.mark.asyncio
async def test_get_table_stats_takes_a_table_name(server_config_factory) -> None:
    app = _app(server_config_factory)
    rows = [
        {"schema_name": "dw", "table_name": "FactPost", "approximate_row_count": 16_000_000},
        {"schema_name": "dw", "table_name": "DimUser", "approximate_row_count": 2_300_000},
        {"schema_name": "dbo", "table_name": "FactPost", "approximate_row_count": 5},
    ]
    app.introspection.get_table_stats = AsyncMock(  # type: ignore[method-assign]
        side_effect=lambda _db, schema: [row for row in rows if schema in (None, row["schema_name"])]
    )

    one = await _call(app, "get_table_stats", {"table_name": "dw.factpost"})
    by_schema = await _call(app, "get_table_stats", {"schema_name": "dw", "table_name": "DimUser"})
    everywhere = await _call(app, "get_table_stats", {"table_name": "FactPost"})

    def names(result: Any) -> list[tuple[str, str]]:
        return [(row["schema_name"], row["table_name"]) for row in result["result"]]

    assert names(one) == [("dw", "FactPost")]
    assert names(by_schema) == [("dw", "DimUser")]
    assert names(everywhere) == [("dw", "FactPost"), ("dbo", "FactPost")]
