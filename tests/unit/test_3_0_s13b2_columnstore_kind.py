"""A nonclustered columnstore test index in benchmark_index_candidate (friction F01, P2 wiring).

D05: the right design for an aggregate scan of a rowstore fact was a
nonclustered columnstore index, and the tool could build only B-trees, so the
answer was delivered unmeasured.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.index_metadata import TABLE_COLUMN_FACTS_SQL
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import parse_candidate_key
from azure_sql_mcp.index_optimizer import IndexCandidate
from azure_sql_mcp.server import TEST_INDEX_PREFIX
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.server import generated_test_index_name
from tests.unit.test_3_0_wiring2_index_set import _Sandbox
from tests.unit.test_3_0_wiring2_index_set import _app
from tests.unit.test_3_0_wiring2_index_set import _benchmark
from tests.unit.test_3_0_wiring2_index_set import _plan
from tests.unit.test_3_0_wiring2_index_set import _session
from tests.unit.test_index_benchmark_workflow import _profile

SQL = (
    "SELECT c.PostTypeKey, SUM(c.Score) AS TotalScore FROM dbo.Comments AS c "
    "GROUP BY c.PostTypeKey"
)
NCCI = {
    "table_name": "Comments",
    "kind": "nonclustered_columnstore",
    "key_columns": ["PostTypeKey", "Score"],
}
COLUMNS = {
    "PostTypeKey": ("int", 4),
    "Score": ("int", 4),
    "Text": ("nvarchar", -1),
}


class _ColumnstoreSandbox(_Sandbox):
    """The catalog double, with table columns and columnstore indexes."""

    def __init__(self, app: AzureSqlMcpApplication, monkeypatch: pytest.MonkeyPatch) -> None:
        super().__init__(app, monkeypatch)
        self.kinds: dict[str, str] = {}

    async def _create(self, _database: str, schema: str, table: str, index_name: str,
                      key_columns: list[str], include_columns: list[str] | None, online: bool,
                      **kwargs: Any) -> dict[str, str]:
        kind = str(kwargs.get("index_kind") or "rowstore")
        if kind == "rowstore":
            return await super()._create(
                _database, schema, table, index_name, key_columns, include_columns, online, **kwargs
            )
        self.create_calls.append({"index_name": index_name, "online": online, **kwargs})
        compression = kwargs.get("data_compression")
        candidate = IndexCandidate(
            schema=schema, table=table, key_columns=tuple(key_columns), include_columns=(),
            filter_definition=kwargs.get("filter_definition"), index_name=index_name,
            compression=compression, index_kind=kind,
        )
        self.indexes[index_name] = ExistingIndex(
            schema=schema, table=table, index_id=self.next_index_id, name=index_name,
            index_type="NONCLUSTERED COLUMNSTORE",
            key_columns=(),
            include_columns=tuple(parse_candidate_key(column).name for column in key_columns),
            filter_definition=kwargs.get("filter_definition"),
            partition_compression=((1, compression or "COLUMNSTORE"),),
        )
        self.next_index_id += 1
        self.markers[index_name] = (str(kwargs["ownership_proof"]), candidate.definition_fingerprint)
        self.kinds[index_name] = kind
        self.created.append(index_name)
        return {"status": "completed"}

    async def _fetch(self, _database: str, sql: str, *, params: Any = (), **_kwargs: Any) -> list[dict[str, Any]]:
        if sql == TABLE_COLUMN_FACTS_SQL:
            return [
                {"column_name": name, "type_name": type_name, "max_length": length,
                 "is_nullable": 0, "is_computed": 0, "is_sparse": 0}
                for name, (type_name, length) in COLUMNS.items()
            ]
        if "IndexDepth" in sql and self.kinds.get(str(params[-1])):
            return [{"index_depth": None, "used_pages": 640, "row_count": 22_400_000}]
        if "column_store_row_groups" in sql:
            return []
        return await super()._fetch(_database, sql, params=params, **_kwargs)


def _name(spec: dict[str, Any]) -> str:
    return generated_test_index_name(
        TEST_INDEX_PREFIX,
        IndexCandidate(
            schema="dbo", table=spec["table_name"], key_columns=tuple(spec["key_columns"]),
            include_columns=(), compression=None, index_kind="nonclustered_columnstore",
        ),
    )


@pytest.mark.asyncio
async def test_a_nonclustered_columnstore_index_is_built_measured_and_dropped(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _ColumnstoreSandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL)
    index_name = _name(NCCI)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(600) for _ in range(3)],
            *[_plan(index_name, elapsed_ms=60) for _ in range(3)],
            *[_profile(600) for _ in range(3)],
        ]
    )

    result = await _benchmark(app, session_id, candidate_id, sql=SQL, indexes=[NCCI])

    assert result["classification"] == "improved", result["reason"]
    assert sandbox.created == [index_name] and sandbox.dropped == [index_name]
    assert sandbox.create_calls[0]["index_kind"] == "nonclustered_columnstore"
    assert "CREATE NONCLUSTERED COLUMNSTORE INDEX" in result["index_ddl"]
    assert "([PostTypeKey], [Score])" in result["index_ddl"]
    built = result["indexes"][0]
    assert built["index_kind"] == "nonclustered_columnstore"
    assert built["built"] is True and built["size_mb"] == 5.0
    # A columnstore index has no B-tree: no depth, and no note that blames a failed read.
    assert built["index_depth"] is None and "facts_note" not in built
    assert result["lease"]["status"] == "cleaned"


@pytest.mark.asyncio
async def test_a_rowstore_index_keeps_its_name_ddl_and_request_when_kind_is_left_out(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _ColumnstoreSandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL)
    spec = {"table_name": "Comments", "key_columns": ["PostTypeKey"], "include_columns": ["Score"]}
    index_name = generated_test_index_name(
        TEST_INDEX_PREFIX,
        IndexCandidate(schema="dbo", table="Comments", key_columns=("PostTypeKey",),
                       include_columns=("Score",)),
    )
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(600) for _ in range(3)],
            *[_plan(index_name, elapsed_ms=60) for _ in range(3)],
            *[_profile(600) for _ in range(3)],
        ]
    )

    result = await _benchmark(app, session_id, candidate_id, sql=SQL, indexes=[spec])

    assert sandbox.created == [index_name]
    assert "index_kind" not in sandbox.create_calls[0]
    assert result["indexes"][0]["index_kind"] == "rowstore"
    assert result["index_ddl"].startswith("CREATE NONCLUSTERED INDEX")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ({**NCCI, "key_columns": ["PostTypeKey", "Text"]}, "Text"),
        ({**NCCI, "include_columns": ["Score"]}, "include"),
        ({**NCCI, "is_unique": True}, "unique"),
        ({**NCCI, "data_compression": "PAGE"}, "COLUMNSTORE or COLUMNSTORE_ARCHIVE"),
        ({"table_name": "Comments", "key_columns": ["Score"], "data_compression": "COLUMNSTORE"},
         "NONE, ROW or PAGE"),
        ({**NCCI, "kind": "clustered_columnstore"}, "kind"),
    ],
)
async def test_a_columnstore_definition_the_engine_would_refuse_costs_no_execution(
    server_config_factory, monkeypatch: pytest.MonkeyPatch, spec: dict[str, Any], message: str
) -> None:
    app = _app(server_config_factory)
    sandbox = _ColumnstoreSandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL)
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ValueError, match=f"(?i){message}"):
        await _benchmark(app, session_id, candidate_id, sql=SQL, indexes=[spec])

    app.plans.profile_query.assert_not_awaited()
    assert sandbox.create_calls == []
    # Nothing was bound: the corrected request works on the same candidate.
    assert app.tuning_sessions.get_candidate(candidate_id).is_terminal is False


@pytest.mark.asyncio
async def test_a_table_that_already_has_a_columnstore_index_is_refused_before_any_run(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _ColumnstoreSandbox(app, monkeypatch)
    sandbox.indexes["NCCI_Comments"] = ExistingIndex(
        schema="dbo", table="Comments", index_id=5, name="NCCI_Comments",
        index_type="NONCLUSTERED COLUMNSTORE", key_columns=(), include_columns=("Score",),
    )
    session_id, candidate_id = _session(app, SQL)
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ValueError) as refusal:
        await _benchmark(app, session_id, candidate_id, sql=SQL, indexes=[NCCI])

    assert "NCCI_Comments" in str(refusal.value)
    assert "No query and no DDL was run" in str(refusal.value)
    app.plans.profile_query.assert_not_awaited()
    assert sandbox.create_calls == []


@pytest.mark.asyncio
async def test_replaces_is_refused_for_a_columnstore_index(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    _ColumnstoreSandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL)

    with pytest.raises(ValueError, match="replaces"):
        await _benchmark(
            app, session_id, candidate_id, sql=SQL, indexes=[NCCI], replaces="IX_Comments_Old"
        )


@pytest.mark.asyncio
async def test_the_tool_accepts_kind_in_an_indexes_entry_and_index_kind_for_one_index(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    _ColumnstoreSandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, SQL)
    app._benchmark_index_candidate = AsyncMock(return_value={"classification": "neutral"})  # type: ignore[method-assign]
    base = {"session_id": session_id, "candidate_id": candidate_id, "sql": SQL, "idempotency_key": "k"}

    await app.mcp._tool_manager.call_tool(
        "benchmark_index_candidate",
        {**base, "indexes": [{**NCCI, "data_compression": "COLUMNSTORE_ARCHIVE"}]},
    )
    as_set = app._benchmark_index_candidate.await_args.kwargs
    await app.mcp._tool_manager.call_tool(
        "benchmark_index_candidate",
        {**base, "schema_name": "dbo", "table_name": "Comments",
         "key_columns": ["PostTypeKey", "Score"], "index_kind": "nonclustered_columnstore"},
    )
    single = app._benchmark_index_candidate.await_args.kwargs

    assert as_set["indexes"][0]["kind"] == "nonclustered_columnstore"
    assert as_set["indexes"][0]["data_compression"] == "COLUMNSTORE_ARCHIVE"
    assert single["index_kind"] == "nonclustered_columnstore"


@pytest.mark.asyncio
async def test_a_columnstore_index_reports_no_depth_even_when_the_catalog_says_zero(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Live: the depth read returns 0 for a columnstore index, which is not a depth."""

    app = _app(server_config_factory)
    sandbox = _ColumnstoreSandbox(app, monkeypatch)
    inner = sandbox._fetch

    async def fetch(_database: str, sql: str, *, params: Any = (), **kwargs: Any) -> list[dict[str, Any]]:
        if "IndexDepth" in sql:
            return [{"index_depth": 0, "used_pages": 640, "row_count": 22_400_000}]
        return await inner(_database, sql, params=params, **kwargs)

    app.executor.fetch_all = AsyncMock(side_effect=fetch)  # type: ignore[method-assign]
    session_id, candidate_id = _session(app, SQL)
    index_name = _name(NCCI)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(600) for _ in range(3)],
            *[_plan(index_name, elapsed_ms=60) for _ in range(3)],
            *[_profile(600) for _ in range(3)],
        ]
    )

    result = await _benchmark(app, session_id, candidate_id, sql=SQL, indexes=[NCCI])

    assert result["indexes"][0]["index_depth"] is None
    assert "facts_note" not in result["indexes"][0]
