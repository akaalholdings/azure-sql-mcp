"""A test index that is not partition-aligned (coordinator item, live evidence).

On a rowstore fact partitioned by year, the winning design for TOP (20) ...
ORDER BY PostId DESC was one B-tree across all partitions: 81,230 reads down
to 3 or 4. Since the aligned build became the rule the tool could not test
that design.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.index_advisor import NON_ALIGNED_INDEX_WARNING
from azure_sql_mcp.index_advisor import build_index_advice
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import parse_candidate_key
from azure_sql_mcp.index_optimizer import IndexCandidate
from azure_sql_mcp.server import generated_test_index_name
from tests.unit.test_3_0_s8_partitioned import _clustered
from tests.unit.test_3_0_s13a_index_build import PARTITIONED
from tests.unit.test_3_0_s13a_index_build import SQL
from tests.unit.test_3_0_s13a_index_build import _aligned_index
from tests.unit.test_3_0_s13a_index_build import _full_run
from tests.unit.test_3_0_s13a_index_build import _sandbox_app
from tests.unit.test_3_0_s13a_index_build import _table_facts
from tests.unit.test_index_advisor import _inputs
from tests.unit.test_index_advisor import _query
from tests.unit.test_index_benchmark_workflow import _candidate
from tests.unit.test_index_benchmark_workflow import _install_catalog

NON_ALIGNED = {"table_name": "Items", "key_columns": ["id"], "partition_aligned": False}


def _name(**placement: Any) -> str:
    return generated_test_index_name(
        "IX_Testing_",
        IndexCandidate(
            schema="dbo", table="Items", key_columns=("id",), include_columns=(), **placement
        ),
    )


def _on_primary(index_name: str) -> ExistingIndex:
    return ExistingIndex(
        schema="dbo", table="Items", index_id=2, name=index_name, index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("id"),), include_columns=(),
        data_space_name="PRIMARY", data_space_type="ROWS_FILEGROUP",
    )


async def _benchmark(app: Any, session_id: str, candidate_id: str, **options: Any) -> dict[str, Any]:
    return await app._benchmark_index_candidate(
        "appdb", session_id, candidate_id, SQL, None, None, None, None, None, False,
        "screening", True, True, 30, "non-aligned-key", None, **options,
    )


@pytest.mark.asyncio
async def test_a_non_aligned_index_is_built_on_primary_verified_and_reported(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _sandbox_app(server_config_factory)
    _table_facts(app, **PARTITIONED)
    session_id, candidate_id = _candidate(app, SQL)
    index_name = _name(data_space_name="PRIMARY")
    built = _on_primary(index_name)
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(side_effect=[[], [built], [built], []]),
    )
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(side_effect=_full_run(index_name))  # type: ignore[method-assign]
    leases: list[dict[str, Any]] = []
    update = app.performance_store.update_index_lease
    monkeypatch.setattr(
        app.performance_store,
        "update_index_lease",
        lambda *args, **kwargs: leases.append(update(*args, **kwargs)) or leases[-1],
    )

    result = await _benchmark(app, session_id, candidate_id, indexes=[NON_ALIGNED])

    # One B-tree on the only filegroup, not one per partition.
    assert result["index_ddl"].rstrip().endswith("ON [PRIMARY];")
    assert "ps_Items" not in result["index_ddl"]
    facts = app._create_test_index.await_args.kwargs["table_facts"]
    assert facts.placement() == {"data_space_name": "PRIMARY"}
    assert result["indexes"][0]["index_name"] == index_name
    assert result["indexes"][0]["built"] is True
    assert result["indexes"][0]["partition_aligned"] is False
    assert result["lease"]["status"] == "cleaned"
    assert result["non_aligned_index_warning"] == NON_ALIGNED_INDEX_WARNING
    for phrase in ("SWITCH", "dropped or disabled", "partition elimination", "one ordered B-tree"):
        assert phrase in NON_ALIGNED_INDEX_WARNING, phrase
    # The lease record says so too: recovery rebuilds the definition from it.
    assert any(
        lease["metadata"].get("partition_aligned") is False
        and lease["metadata"].get("data_space") == "PRIMARY"
        for lease in leases
    )


@pytest.mark.asyncio
async def test_a_non_aligned_request_does_not_accept_an_aligned_build(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verification compares with the requested placement: PRIMARY, no partition column."""

    app = _sandbox_app(server_config_factory)
    _table_facts(app, **PARTITIONED)
    session_id, candidate_id = _candidate(app, SQL)
    index_name = _name(data_space_name="PRIMARY")
    aligned = _aligned_index(index_name)
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(side_effect=[[], [aligned], [aligned], [aligned], [], []]),
    )
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(side_effect=_full_run(index_name))  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id, indexes=[NON_ALIGNED])

    assert result["classification"] == "inconclusive"
    assert result["failure_diagnostic"]["cause_class"] == "definition_mismatch"


@pytest.mark.asyncio
async def test_the_default_stays_partition_aligned(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _sandbox_app(server_config_factory)
    _table_facts(app, **PARTITIONED)
    session_id, candidate_id = _candidate(app, SQL)
    index_name = _name(
        data_space_name="ps_Items", partition_columns=("CreatedDateKey",),
        partition_scheme_name="ps_Items", partition_function_name="pf_Items",
    )
    built = _aligned_index(index_name)
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(side_effect=[[], [built], [built], []]),
    )
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(side_effect=_full_run(index_name))  # type: ignore[method-assign]

    result = await _benchmark(
        app, session_id, candidate_id, indexes=[{"table_name": "Items", "key_columns": ["id"]}]
    )

    assert "ON [ps_Items]([CreatedDateKey])" in result["index_ddl"]
    assert result["indexes"][0]["partition_aligned"] is True
    assert "non_aligned_index_warning" not in result


@pytest.mark.asyncio
async def test_on_a_table_that_is_not_partitioned_the_field_is_ignored_with_a_note(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _sandbox_app(server_config_factory)
    _table_facts(app)
    session_id, candidate_id = _candidate(app, SQL)
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    index_name, _ = _install_catalog(monkeypatch, key_columns=["id"])
    app.plans.profile_query = AsyncMock(side_effect=_full_run(index_name))  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id, indexes=[NON_ALIGNED])

    # The same index, name and DDL as without the field.
    assert result["indexes"][0]["index_name"] == index_name
    assert "ON [PRIMARY]" not in result["index_ddl"]
    assert "dbo.Items is not partitioned" in result["partition_aligned_note"]
    assert "non_aligned_index_warning" not in result
    assert result["lease"]["status"] == "cleaned"


@pytest.mark.asyncio
async def test_a_partitioned_clustered_columnstore_table_refuses_before_any_execution(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The engine does not accept a non-aligned nonclustered index on such a table."""

    app = _sandbox_app(server_config_factory, allow_offline_test_index=True)
    _table_facts(app, has_clustered_columnstore_index=1, **PARTITIONED)
    session_id, candidate_id = _candidate(app, SQL)
    _install_catalog(monkeypatch, key_columns=["id"])
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ValueError) as refusal:
        await _benchmark(app, session_id, candidate_id, indexes=[NON_ALIGNED])

    message = str(refusal.value)
    assert "expected engine refusal" in message
    assert "clustered columnstore" in message and "partition-aligned" in message
    assert "No query and no DDL was run" in message
    app.plans.profile_query.assert_not_awaited()
    app._create_test_index.assert_not_awaited()
    assert app.performance_store.list_open_index_leases() == []
    assert app.tuning_sessions.get_candidate(candidate_id).is_terminal is False


@pytest.mark.asyncio
async def test_the_tool_takes_partition_aligned_in_both_argument_forms(
    server_config_factory,
) -> None:
    app = _sandbox_app(server_config_factory)
    session_id, candidate_id = _candidate(app, SQL)
    app._benchmark_index_candidate = AsyncMock(return_value={"classification": "neutral"})  # type: ignore[method-assign]
    base = {"session_id": session_id, "candidate_id": candidate_id, "sql": SQL, "idempotency_key": "k"}

    await app.mcp._tool_manager.call_tool(
        "benchmark_index_candidate", {**base, "indexes": [NON_ALIGNED]}
    )
    as_set = app._benchmark_index_candidate.await_args.kwargs
    await app.mcp._tool_manager.call_tool(
        "benchmark_index_candidate",
        {**base, "schema_name": "dbo", "table_name": "Items", "key_columns": ["id"],
         "partition_aligned": False},
    )
    single = app._benchmark_index_candidate.await_args.kwargs

    assert as_set["indexes"][0]["partition_aligned"] is False
    assert single["partition_aligned"] is False


# --- the workload review ---------------------------------------------------------------


def _create(report: dict[str, Any]) -> dict[str, Any]:
    [rec] = [rec for rec in report["recommendations"] if rec["action"] == "create_index"]
    return rec


def test_the_review_proposes_a_non_aligned_index_only_with_the_same_warning() -> None:
    query = _query(1, "hash_join_stream_aggregate.xml", cpu_us=9_000_000)

    rec = _create(build_index_advice(_inputs([query], [_clustered(column="ExpectedDeliveryDate")])))

    alternative = rec["partitioning"]["non_aligned_alternative"]
    assert alternative["ddl"].endswith("ON [PRIMARY];")
    assert alternative["warning"] == NON_ALIGNED_INDEX_WARNING
    assert "partition_aligned" in alternative["test_with"]


@pytest.mark.asyncio
async def test_a_non_aligned_columnstore_index_is_refused_before_any_execution(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Live: a columnstore index ON [PRIMARY] on a partitioned table is engine error 35316."""

    app = _sandbox_app(server_config_factory)
    _table_facts(app, **PARTITIONED)
    session_id, candidate_id = _candidate(app, SQL)
    _install_catalog(monkeypatch, key_columns=["id"])
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ValueError) as refusal:
        await _benchmark(
            app, session_id, candidate_id,
            indexes=[{**NON_ALIGNED, "kind": "nonclustered_columnstore"}],
        )

    assert "35316" in str(refusal.value) and "partition-aligned" in str(refusal.value)
    app.plans.profile_query.assert_not_awaited()
    app._create_test_index.assert_not_awaited()
