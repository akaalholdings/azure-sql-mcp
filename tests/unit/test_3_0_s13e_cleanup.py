"""A held test object that could not be dropped cannot be missed, and is dropped later.

O15 (live): finalize could not drop a held index on dbo.Users (the route to
the database changed and open connections failed). It answered result_status
ok and deployment_ready true, with ``dropped: false`` and no reason, and
promised that the next benchmark call would retry. Three later benchmark
calls did not: one measured both arms with the leftover in place and said
nothing, two refused. No tool could drop it. A leftover test index on a
shared table changes the plans of every other session.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_3_0_s13d_session_held import ORIGINAL
from tests.unit.test_3_0_s13d_session_held import REWRITE
from tests.unit.test_3_0_s13d_session_held import IndexSandbox
from tests.unit.test_3_0_s13d_session_held import _bench
from tests.unit.test_3_0_s13d_session_held import _candidate
from tests.unit.test_3_0_s13d_session_held import _session
from tests.unit.test_index_benchmark_workflow import _app


def _link_failure() -> BaseException:
    return ConnectionError("[08S01] [Microsoft][ODBC Driver 18 for SQL Server]Communication link failure")


async def _held_index(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> tuple[AzureSqlMcpApplication, IndexSandbox, str, str]:
    """An app whose first session holds one test index on dbo.Items."""

    monkeypatch.setattr("azure_sql_mcp.server._LEASE_DROP_BACKOFF_SECONDS", (0.0, 0.0))
    app = _app(server_config_factory)
    app.pool.discard_idle = AsyncMock(return_value=0)  # type: ignore[method-assign]
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    candidate_id = _candidate(app, session_id, ORIGINAL, "index")
    kept = await _bench(app, session_id, candidate_id, "k1", keep_for_session=True)
    return app, sandbox, session_id, kept["indexes"][0]["index_name"]


async def _finalize(app: AzureSqlMcpApplication, session_id: str) -> dict[str, Any]:
    return await app._finalize_tuning_session("appdb", session_id, None, "measured", None)


def _drops(sandbox: IndexSandbox) -> int:
    return len([event for event in sandbox.events if event.startswith("drop:")])


@pytest.mark.asyncio
async def test_finalize_retries_a_failed_drop_on_a_fresh_connection_and_then_reports_it_dropped(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, sandbox, session_id, index_name = await _held_index(server_config_factory, monkeypatch)
    sandbox.drop_errors.extend([_link_failure(), _link_failure()])

    final = await _finalize(app, session_id)

    assert _drops(sandbox) == 3
    # Idle pooled connections are closed before each new attempt.
    assert app.pool.discard_idle.await_count == 2
    [item] = final["held_test_objects_dropped"]
    assert item["index_name"] == index_name and item["dropped"] is True
    assert item["attempts"] == 3
    assert "cleanup_required" not in final
    assert final.get("result_status") != "precondition"
    assert sandbox.indexes == {}
    assert app.performance_store.list_open_index_leases() == []


@pytest.mark.asyncio
async def test_a_drop_that_keeps_failing_cannot_be_read_as_a_clean_finish(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, sandbox, session_id, index_name = await _held_index(server_config_factory, monkeypatch)
    sandbox.drop_errors.extend([_link_failure()] * 5)

    final = await _finalize(app, session_id)

    assert _drops(sandbox) == 3  # bounded
    ddl = f"DROP INDEX [{index_name}] ON [dbo].[Items];"
    assert final["cleanup_required"] is True
    assert final["result_status"] == "precondition"
    assert index_name in final["result_status_reason"]
    assert final["remediation"] == ddl
    [item] = final["held_test_objects_dropped"]
    assert item["dropped"] is False
    assert item["remediation"] == ddl
    assert item["failure"]["failed_step"] == "drop_index"
    assert item["failure"]["cause_class"] == "connection_lost"
    assert "retry" in final["held_test_objects_note"]
    assert list(sandbox.indexes) == [index_name]
    [lease] = app.performance_store.list_open_index_leases()
    assert lease["status"] == "cleanup_required"
    # The owner token of the lease never leaves the server.
    assert lease["metadata"]["lease_owner_fence"] not in str(final)


@pytest.mark.asyncio
async def test_finalize_again_on_a_finalized_session_retries_the_drop_and_is_idempotent(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, sandbox, session_id, index_name = await _held_index(server_config_factory, monkeypatch)
    sandbox.drop_errors.extend([_link_failure()] * 3)
    assert (await _finalize(app, session_id))["cleanup_required"] is True

    again = await _finalize(app, session_id)

    assert again["already_finalized"] is True
    [item] = again["held_test_objects_dropped"]
    assert item["index_name"] == index_name and item["dropped"] is True
    assert "cleanup_required" not in again
    assert sandbox.indexes == {}
    assert app.performance_store.list_open_index_leases() == []

    third = await _finalize(app, session_id)

    assert third["already_finalized"] is True
    assert third["held_test_objects_dropped"] == []
    assert "cleanup_required" not in third


@pytest.mark.asyncio
async def test_the_next_index_benchmark_drops_the_leftover_first_and_says_so(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The promise of the finalize note: the next benchmark call retries the drop."""

    app, sandbox, session_id, index_name = await _held_index(server_config_factory, monkeypatch)
    sandbox.drop_errors.extend([_link_failure()] * 3)
    await _finalize(app, session_id)
    assert list(sandbox.indexes) == [index_name]
    second_session, _case = _session(app)
    candidate_id = _candidate(app, second_session, ORIGINAL, "index")
    sandbox.events.clear()

    result = await _bench(app, second_session, candidate_id, "k2")

    # Dropped before the baseline ran; then the index was built again and dropped.
    assert sandbox.events[0] == f"drop:{index_name}"
    assert result["classification"] in {"promising", "improved"}, result["reason"]
    assert [item["index_name"] for item in result["cleanup_before_run"]["dropped"]] == [index_name]
    assert sandbox.profiled[-9:-6] == ["original"] * 3
    assert sandbox.indexes == {}
    assert app.performance_store.list_open_index_leases() == []


@pytest.mark.asyncio
async def test_no_benchmark_and_no_new_session_runs_on_a_database_that_holds_a_leftover(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, sandbox, session_id, index_name = await _held_index(server_config_factory, monkeypatch)
    sandbox.drop_errors.extend([_link_failure()] * 3)
    await _finalize(app, session_id)
    second_session, case_id = _session(app)
    index_candidate = _candidate(app, second_session, ORIGINAL, "index")
    rewrite_candidate = _candidate(app, second_session, REWRITE, "predicate")
    runs = len(sandbox.profiled)
    ddl = f"DROP INDEX [{index_name}] ON [dbo].[Items];"

    async def still_failing(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise _link_failure()

    app._drop_test_index = AsyncMock(side_effect=still_failing)  # type: ignore[method-assign]

    refused = [
        await _bench(app, second_session, index_candidate, "k2"),
        await app._benchmark_tuning_candidate(
            "appdb", second_session, rewrite_candidate, ORIGINAL, REWRITE, phase="finalist"
        ),
        await app._start_tuning_session("appdb", case_id, None, None, None, None),
    ]

    for result in refused:
        assert result["result_status"] == "precondition"
        assert result["cleanup_required"] is True
        assert result["remediation"] == ddl
        assert result["cleanup"]["remaining"][0]["failure"]["cause_class"] == "connection_lost"
        assert "session_id" not in result or result["executions"] == 0
    assert refused[0]["classification"] == refused[1]["classification"] == "cleanup_required"
    assert len(sandbox.profiled) == runs
    # No second session was opened by the refused start.
    assert "session" not in refused[2]


@pytest.mark.asyncio
async def test_the_rewrite_benchmark_drops_a_leftover_before_it_measures(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O15 step 3: both arms were measured with the leftover index in place."""

    app, sandbox, session_id, index_name = await _held_index(server_config_factory, monkeypatch)
    sandbox.drop_errors.extend([_link_failure()] * 3)
    await _finalize(app, session_id)
    second_session, _case = _session(app)
    candidate_id = _candidate(app, second_session, REWRITE, "predicate")
    sandbox.events.clear()

    result = await app._benchmark_tuning_candidate(
        "appdb", second_session, candidate_id, ORIGINAL, REWRITE, phase="screening"
    )

    assert sandbox.events[0] == f"drop:{index_name}"
    assert sandbox.indexes == {}
    assert [item["index_name"] for item in result["cleanup_before_run"]["dropped"]] == [index_name]
    assert "classification" in result and result.get("cleanup_required") is not True


@pytest.mark.asyncio
async def test_a_leftover_that_carries_this_servers_marker_is_dropped_not_reported_as_unowned(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O15 step 4: 'name already exists with an unowned observed definition'."""

    app, sandbox, session_id, index_name = await _held_index(server_config_factory, monkeypatch)
    [lease] = app.performance_store.list_open_index_leases()
    # The store believes the lease is closed; the catalog still holds the index.
    pending = app.performance_store.recover_index_lease(
        lease["lease_id"], status="cleanup_pending", expected_version=lease["version"]
    )
    app.performance_store.recover_index_lease(
        lease["lease_id"], status="cleaned", expected_version=pending["version"]
    )
    second_session, _case = _session(app)
    same_definition = _candidate(app, second_session, ORIGINAL, "index")
    sandbox.events.clear()

    result = await _bench(app, second_session, same_definition, "k2")

    assert result.get("failure_code") != "name_conflict"
    assert sandbox.events[0] == f"drop:{index_name}"
    assert [item["index_name"] for item in result["leftovers_dropped"]] == [index_name]
    assert result["classification"] in {"promising", "improved"}, result["reason"]
    assert sandbox.indexes == {}


@pytest.mark.asyncio
async def test_an_index_with_another_owners_marker_is_never_dropped(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, sandbox, session_id, index_name = await _held_index(server_config_factory, monkeypatch)
    [lease] = app.performance_store.list_open_index_leases()
    pending = app.performance_store.recover_index_lease(
        lease["lease_id"], status="cleanup_pending", expected_version=lease["version"]
    )
    app.performance_store.recover_index_lease(
        lease["lease_id"], status="cleaned", expected_version=pending["version"]
    )
    # Another server built an index of this name: its marker is not ours.
    sandbox.markers[index_name] = ("index-lease-someone-else-0123456789ab", sandbox.markers[index_name][1])
    second_session, _case = _session(app)
    same_definition = _candidate(app, second_session, ORIGINAL, "index")
    sandbox.events.clear()

    result = await _bench(app, second_session, same_definition, "k2")

    assert result["failure_code"] == "name_conflict"
    assert sandbox.events == []
    assert list(sandbox.indexes) == [index_name]


@pytest.mark.asyncio
async def test_a_rewrite_benchmark_and_a_comparison_warn_about_a_test_index_they_do_not_hold(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A test index of another session on a table the statements read changes both arms."""

    app, sandbox, holder_session, index_name = await _held_index(server_config_factory, monkeypatch)
    other_session, _case = _session(app)

    warned = await app._test_objects_on_read_tables(
        "appdb", [ORIGINAL, REWRITE], own_session_id=other_session
    )
    own = await app._test_objects_on_read_tables(
        "appdb", [ORIGINAL, REWRITE], own_session_id=holder_session
    )
    comparison = await app._test_objects_on_read_tables("appdb", [ORIGINAL, REWRITE])

    [item] = warned["objects"]
    assert item["index_name"] == index_name and item["name"] == "dbo.Items"
    assert item["held_by_session"] == holder_session
    assert "plans" in warned["warning"]
    # The session that holds it is told through held_test_objects_present instead.
    assert own is None
    assert comparison is not None and comparison["objects"][0]["index_name"] == index_name
    await _finalize(app, holder_session)
    assert await app._test_objects_on_read_tables("appdb", [ORIGINAL, REWRITE]) is None


@pytest.mark.asyncio
async def test_a_kept_index_says_that_no_run_followed_a_drop(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O15: aba_result_stability was reported although the held index had no after leg."""

    app = _app(server_config_factory)
    IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    kept_candidate = _candidate(app, session_id, ORIGINAL, "index")

    kept = await _bench(app, session_id, kept_candidate, "k1", keep_for_session=True)

    assert kept["proof_scope"] == "aba_result_stability"
    assert "before the build and with the index" in kept["proof_scope_note"]
    await _finalize(app, session_id)
    second_session, _case = _session(app)
    dropped = await _bench(app, second_session, _candidate(app, second_session, ORIGINAL, "index"), "k2")
    assert "proof_scope_note" not in dropped


def test_the_session_headline_says_cleanup_required() -> None:
    from azure_sql_mcp.tool_contracts import add_tool_headline

    payload = {"session": {"session_id": "session-1", "status": "completed"}}
    clean = add_tool_headline("finalize_tuning_session", dict(payload))
    leftover = add_tool_headline("finalize_tuning_session", {**payload, "cleanup_required": True})

    assert "cleanup_required" not in clean["headline"]
    assert leftover["headline"]["cleanup_required"] is True


@pytest.mark.asyncio
async def test_the_view_benchmark_is_refused_too_while_a_leftover_cannot_be_dropped(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datetime import datetime
    from datetime import timedelta
    from datetime import timezone

    from tests.unit.test_3_0_s13b2_indexed_view_tool import BODY
    from tests.unit.test_3_0_s13b2_indexed_view_tool import _ViewSandbox
    from tests.unit.test_3_0_s13b2_indexed_view_tool import _app as _view_app
    from tests.unit.test_3_0_s13b2_indexed_view_tool import _candidate_sql
    from tests.unit.test_3_0_s13b2_indexed_view_tool import _run
    from tests.unit.test_3_0_s13b2_indexed_view_tool import _session as _view_session

    app = _view_app(server_config_factory)
    sandbox = _ViewSandbox(app, monkeypatch)
    session_id, candidate_id = _view_session(app, _candidate_sql())
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]
    case = app.performance_store.get_performance_case(
        app.tuning_sessions.get_session(session_id).performance_case_id
    )
    # A lease of an earlier run whose cleanup failed. It has no owner token,
    # so this server cannot prove the index is its own and must not drop it.
    lease = app.performance_store.create_index_lease(
        lease_id="lease-earlier",
        database_fingerprint=case.database_fingerprint or "",
        session_id="session-earlier",
        candidate_id="candidate-earlier",
        index_name="IX_Testing_Posts_OwnerUserId_0a1b2c3d",
        object_fingerprint="a" * 64,
        expires_at_utc=(datetime.now(timezone.utc) + timedelta(minutes=20)).isoformat(),
        metadata={"target_schema": "dbo", "target_table": "Posts"},
    )
    pending = app.performance_store.recover_index_lease(
        lease["lease_id"], status="cleanup_pending", expected_version=lease["version"]
    )
    app.performance_store.recover_index_lease(
        lease["lease_id"], status="cleanup_required", expected_version=pending["version"]
    )

    result = await _run(app, session_id, candidate_id)

    assert result["classification"] == "cleanup_required"
    assert result["result_status"] == "precondition"
    assert result["remediation"] == "DROP INDEX [IX_Testing_Posts_OwnerUserId_0a1b2c3d] ON [dbo].[Posts];"
    assert result["cleanup"]["remaining"][0]["failure"]["cause_class"] == "internal_error"
    assert sandbox.events == []
    app.plans.profile_query.assert_not_awaited()
    assert BODY  # the view was never built
