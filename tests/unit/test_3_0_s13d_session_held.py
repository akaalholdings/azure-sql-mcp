"""Session-held test objects (keep_for_session).

D05 and O01: the bare model builds a test index once and measures several
variants against it. The product rebuilt the index (7 to 15 minutes on a
22M-row table) for every benchmark call, and dropped a measured view before
its results could be compared. A call can now leave what it built under the
session's one lease; a later call of the session reuses a matching object,
finalize drops everything, and an expired hold is dropped by the reconcile.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.index_optimizer import ExistingIndex
from azure_sql_mcp.index_optimizer import IndexCandidate
from azure_sql_mcp.index_optimizer import parse_candidate_key
from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_3_0_s13b2_indexed_view_tool import BODY
from tests.unit.test_3_0_s13b2_indexed_view_tool import ORIGINAL as VIEW_ORIGINAL
from tests.unit.test_3_0_s13b2_indexed_view_tool import VIEW
from tests.unit.test_3_0_s13b2_indexed_view_tool import _ViewSandbox
from tests.unit.test_3_0_s13b2_indexed_view_tool import _app as _view_app
from tests.unit.test_3_0_s13b2_indexed_view_tool import _candidate_sql as _view_sql
from tests.unit.test_index_benchmark_workflow import _app
from tests.unit.test_index_benchmark_workflow import _profile

ORIGINAL = "SELECT id FROM dbo.Items"
REWRITE = "SELECT id FROM dbo.Items AS candidate"
SECOND_REWRITE = "SELECT id FROM dbo.Items AS second_candidate"


class IndexSandbox:
    """A catalog that shows a test index from its CREATE until its DROP.

    Each index keeps the owner marker and the definition marker its CREATE
    wrote, as the extended properties of the real batch do.
    """

    def __init__(
        self,
        app: AzureSqlMcpApplication,
        monkeypatch: pytest.MonkeyPatch,
        *,
        fallback_fetch: Any = None,
    ) -> None:
        self.app = app
        self.fallback_fetch = fallback_fetch
        self.indexes: dict[str, ExistingIndex] = {}
        self.markers: dict[str, tuple[str, str]] = {}
        self.events: list[str] = []
        self.profiled: list[str] = []
        self.create_errors: list[BaseException] = []
        # Each DROP raises the next of these before it removes anything.
        self.drop_errors: list[BaseException] = []
        app._create_test_index = AsyncMock(side_effect=self._create)  # type: ignore[method-assign]
        app._drop_test_index = AsyncMock(side_effect=self._drop)  # type: ignore[method-assign]
        app.executor.fetch_all = AsyncMock(side_effect=self._fetch)  # type: ignore[method-assign]
        app.executor.execute_session_exactly_once = AsyncMock(side_effect=self._snapshot)  # type: ignore[method-assign]
        app.plans.profile_query = AsyncMock(side_effect=self._profile)  # type: ignore[method-assign]
        monkeypatch.setattr("azure_sql_mcp.server.collect_existing_indexes", self._catalog)

    async def _catalog(self, _executor: Any, _database: str, **kwargs: Any) -> list[ExistingIndex]:
        return [
            index
            for index in self.indexes.values()
            if index.table == kwargs.get("table_name", index.table)
        ]

    async def _create(
        self,
        _database: str,
        schema: str,
        table: str,
        index_name: str,
        key_columns: list[str],
        include_columns: list[str] | None,
        _online: bool,
        **options: Any,
    ) -> dict[str, Any]:
        self.events.append(f"create:{index_name}")
        if self.create_errors:
            raise self.create_errors.pop(0)
        candidate = IndexCandidate(
            schema=schema,
            table=table,
            key_columns=tuple(key_columns),
            include_columns=tuple(include_columns or ()),
            filter_definition=options.get("filter_definition"),
            is_unique=bool(options.get("is_unique", False)),
        )
        self.indexes[index_name] = ExistingIndex(
            schema=schema,
            table=table,
            index_id=2 + len(self.indexes),
            name=index_name,
            index_type="NONCLUSTERED",
            key_columns=tuple(parse_candidate_key(column) for column in key_columns),
            include_columns=tuple(include_columns or ()),
            filter_definition=options.get("filter_definition"),
            is_unique=bool(options.get("is_unique", False)),
        )
        self.markers[index_name] = (str(options["ownership_proof"]), candidate.definition_fingerprint)
        return {"status": "completed"}

    async def _drop(self, _database: str, _schema: str, _table: str, index_name: str, **options: Any) -> dict[str, Any]:
        self.events.append(f"drop:{index_name}")
        if self.drop_errors:
            raise self.drop_errors.pop(0)
        # The guarded DROP checks the owner marker before it drops.
        if options["ownership_proof"] != self.markers[index_name][0]:
            raise RuntimeError("Temporary index ownership marker mismatch. (50001)")
        del self.indexes[index_name]
        del self.markers[index_name]
        return {"status": "completed"}

    async def _fetch(self, _database: str, sql: str, *, params: Any = (), **_kwargs: Any) -> list[dict[str, Any]]:
        index_markers = "owner_marker" in sql and "definition_marker" in sql and "sys.indexes" in sql
        if self.fallback_fetch is not None and not (index_markers and str(params[-1]) in self.indexes):
            return await self.fallback_fetch(_database, sql, params=params)
        if "sys.sql_modules" in sql and "object_type_code" in sql:
            return [
                {"schema_name": "dbo", "object_name": "Items", "object_type_code": "U",
                 "object_type": "USER_TABLE", "definition": None, "is_encrypted": 0}
            ]
        if "owner_marker" in sql and "definition_marker" in sql:
            name = str(params[-1])
            if name not in self.indexes:
                return []
            owner, definition = self.markers[name]
            return [
                {"index_id": self.indexes[name].index_id, "owner_marker": owner,
                 "definition_marker": definition}
            ]
        return [{"schema_name": "dbo", "table_name": "Items"}]

    async def _snapshot(self, _database: str, _statements: list[str], **kwargs: Any) -> list[list[QueryResult]]:
        self.events.append("compare")
        hook = kwargs.get("before_statement_dispatch")
        for index in range(4):
            if hook is not None:
                hook(index)
        rows = [QueryResult(columns=("id",), rows=[{"id": 1}], column_type_signatures=("synthetic-int",))]
        return [[], [], rows, rows, [], []]

    async def _profile(self, _database: str, sql: str, **_kwargs: Any) -> Any:
        rewrite = "candidate" in sql
        self.profiled.append("rewrite" if rewrite else "original")
        used = next(iter(self.indexes), None)
        # The original is slow without an index; anything is fast with one.
        return _profile(20 if used else 100, index_name=used)


def _session(app: AzureSqlMcpApplication) -> tuple[str, str]:
    case = app.performance_workflows.start_case("appdb", ORIGINAL)
    session = app.performance_workflows.start_session(case.case_id, "appdb")
    return session["session_id"], case.case_id


def _candidate(app: AzureSqlMcpApplication, session_id: str, sql: str, strategy: str) -> str:
    return app.performance_workflows.add_candidate(session_id, sql, strategy=strategy)["candidate_id"]


async def _bench(
    app: AzureSqlMcpApplication,
    session_id: str,
    candidate_id: str,
    key: str,
    *,
    key_columns: list[str] | None = None,
    candidate_sql: str | None = None,
    phase: str = "screening",
    **options: Any,
) -> dict[str, Any]:
    return await app._benchmark_index_candidate(
        "appdb", session_id, candidate_id, ORIGINAL, "dbo", "Items", key_columns or ["id"],
        None, None, False, phase, True, True, 30, key, None,
        candidate_sql=candidate_sql, **options,
    )


def _held(app: AzureSqlMcpApplication) -> list[dict[str, Any]]:
    return [
        lease
        for lease in app.performance_store.list_open_index_leases()
        if lease["metadata"].get("held_for_session") is True
    ]


@pytest.mark.asyncio
async def test_the_default_still_drops_the_index_before_the_call_returns(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    candidate_id = _candidate(app, session_id, ORIGINAL, "index")

    result = await _bench(app, session_id, candidate_id, "k1")

    assert sandbox.indexes == {}
    assert result["lease"]["status"] == "cleaned"
    objects = result["test_objects"]
    assert [item["index_name"] for item in objects["built"]] == [result["indexes"][0]["index_name"]]
    assert objects["reused"] == []
    assert objects["still_held"] == []
    assert _held(app) == []


@pytest.mark.asyncio
async def test_a_kept_index_is_reused_by_the_next_call_without_a_rebuild(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D05: the index alone, then the rewrite with the same index, under one build."""

    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    first = _candidate(app, session_id, ORIGINAL, "index")
    second = _candidate(app, session_id, REWRITE, "rewrite_plus_index")

    kept = await _bench(app, session_id, first, "k1", keep_for_session=True)

    index_name = kept["indexes"][0]["index_name"]
    assert kept["classification"] in {"promising", "improved"}, kept["reason"]
    assert list(sandbox.indexes) == [index_name]
    assert sandbox.events == [f"create:{index_name}"]
    assert kept["lease"]["status"] == "active"
    assert [item["index_name"] for item in kept["test_objects"]["still_held"]] == [index_name]
    assert kept["test_objects"]["hold_expires_at_utc"]
    # No run followed a drop: the baseline is the runs before the build.
    assert kept["parameter_results"][0]["runs"]["baseline_after"] == 0
    assert kept["equivalence"][0]["status"] == "match"
    runs_before = len(sandbox.profiled)

    reused = await _bench(
        app, session_id, second, "k2", candidate_sql=REWRITE, phase="finalist"
    )

    assert reused["classification"] == "improved", reused["reason"]
    # No second build; the default call drops what it used when it returns.
    assert sandbox.events == [f"create:{index_name}", "compare", f"drop:{index_name}"]
    assert [item["index_name"] for item in reused["test_objects"]["reused"]] == [index_name]
    assert reused["test_objects"]["built"] == []
    assert reused["test_objects"]["still_held"] == []
    # The original was not run again while the index existed: its baseline is
    # the one this session measured before the build.
    assert sandbox.profiled[runs_before:] == ["rewrite"] * 5
    case = reused["parameter_results"][0]
    assert case["baseline_source"] == "session_before_hold"
    assert case["baseline"]["elapsed_ms"] == 100
    assert case["candidate"]["elapsed_ms"] == 20
    assert reused["equivalence"][0]["status"] == "match"
    assert sandbox.indexes == {}
    assert app.performance_store.list_open_index_leases() == []


@pytest.mark.asyncio
async def test_an_index_that_does_not_match_a_held_one_is_built(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    first = _candidate(app, session_id, ORIGINAL, "index")
    second = _candidate(app, session_id, REWRITE, "rewrite_plus_index")

    kept = await _bench(app, session_id, first, "k1", keep_for_session=True)
    held_name = kept["indexes"][0]["index_name"]

    other = await _bench(
        app, session_id, second, "k2", key_columns=["status"], candidate_sql=REWRITE,
        keep_for_session=True,
    )

    built_name = other["indexes"][0]["index_name"]
    assert built_name != held_name
    assert [item["index_name"] for item in other["test_objects"]["built"]] == [built_name]
    assert other["test_objects"]["reused"] == []
    # One lease carries both objects of the session.
    assert sorted(item["index_name"] for item in other["test_objects"]["still_held"]) == sorted(
        [held_name, built_name]
    )
    assert len(app.performance_store.list_open_index_leases()) == 1
    assert sorted(sandbox.indexes) == sorted([held_name, built_name])


@pytest.mark.asyncio
async def test_finalize_drops_everything_the_session_holds_and_reports_it(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    candidate_id = _candidate(app, session_id, ORIGINAL, "index")
    kept = await _bench(app, session_id, candidate_id, "k1", keep_for_session=True)
    index_name = kept["indexes"][0]["index_name"]

    view = await app._get_tuning_session("appdb", session_id)

    held = view["held_test_objects"]
    assert [item["index_name"] for item in held] == [index_name]
    assert held[0]["kind"] == "index"
    assert held[0]["age_seconds"] >= 0
    assert held[0]["expires_at_utc"] == kept["test_objects"]["hold_expires_at_utc"]

    final = await app._finalize_tuning_session(
        "appdb", session_id, None, "measured", None
    )

    assert [item["index_name"] for item in final["held_test_objects_dropped"]] == [index_name]
    assert sandbox.indexes == {}
    assert app.performance_store.list_open_index_leases() == []
    assert "held_test_objects" not in await app._get_tuning_session("appdb", session_id)


@pytest.mark.asyncio
async def test_an_expired_hold_is_dropped_by_the_reconcile_although_the_session_is_open(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    candidate_id = _candidate(app, session_id, ORIGINAL, "index")
    await _bench(app, session_id, candidate_id, "k1", keep_for_session=True)
    lease = _held(app)[0]
    assert sandbox.indexes

    # The hold is bounded by lease_minutes from the end of the call.
    expires = datetime.fromisoformat(lease["expires_at_utc"])
    assert timedelta(minutes=29) < expires - datetime.now(timezone.utc) <= timedelta(minutes=30)
    app.performance_store._connection.execute(
        "UPDATE index_leases SET expires_at_utc = ? WHERE lease_id = ?",
        ((datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(), lease["lease_id"]),
    )

    summary = await app._cleanup_expired_index_leases()

    assert summary == {"examined": 1, "cleaned": 1, "cleanup_required": 0}
    assert sandbox.indexes == {}
    assert app.performance_store.list_open_index_leases() == []


@pytest.mark.asyncio
async def test_a_crash_between_calls_leaves_a_lease_that_a_new_process_cleans(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    candidate_id = _candidate(app, session_id, ORIGINAL, "index")
    await _bench(app, session_id, candidate_id, "k1", keep_for_session=True)
    lease = _held(app)[0]

    # A new process knows only the durable lease and the catalog.
    assert lease["status"] == "active"
    member = lease["metadata"]["members"][0]
    assert member["index_name"] in sandbox.indexes
    assert member["fingerprint_provenance"] == "observed_existing_index"
    app.performance_store._connection.execute(
        "UPDATE index_leases SET expires_at_utc = ? WHERE lease_id = ?",
        ((datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(), lease["lease_id"]),
    )
    summary = await app._cleanup_expired_index_leases(
        database_fingerprint=lease["database_fingerprint"]
    )

    assert summary["cleaned"] == 1
    assert sandbox.indexes == {}


@pytest.mark.asyncio
async def test_a_case_with_no_baseline_from_before_the_hold_is_refused_before_any_run(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With a held index in place the original cannot give a clean baseline."""

    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    first = _candidate(app, session_id, ORIGINAL, "index")
    second = _candidate(app, session_id, REWRITE, "rewrite_plus_index")
    await _bench(app, session_id, first, "k1", keep_for_session=True)
    runs = len(sandbox.profiled)

    # compare_order differs, so the kept baseline does not fit this call.
    with pytest.raises(ValueError, match="no baseline measured before"):
        await app._benchmark_index_candidate(
            "appdb", session_id, second, ORIGINAL, "dbo", "Items", ["id"], None, None, False,
            "screening", True, False, 30, "k2", None, candidate_sql=REWRITE,
            plan_reuse="first_case",
        )

    assert len(sandbox.profiled) == runs
    assert len(_held(app)) == 1


@pytest.mark.asyncio
async def test_a_held_view_and_a_held_index_share_one_lease_and_finalize_drops_both(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _view_app(server_config_factory)
    views = _ViewSandbox(app, monkeypatch)
    case = app.performance_workflows.start_case("appdb", VIEW_ORIGINAL)
    session_id = app.performance_workflows.start_session(case.case_id, "appdb")["session_id"]
    view_candidate = app.performance_workflows.add_candidate(
        session_id, _view_sql(), strategy="rewrite_plus_index"
    )["candidate_id"]
    clustered = str(views.plan.clustered_index.index_name)

    async def profile(_database: str, sql: str, **_kwargs: Any) -> Any:
        reads_view = VIEW in sql
        return _profile(4 if reads_view else 900, index_name=clustered if reads_view else None)

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]

    kept = await app._benchmark_indexed_view_candidate(
        "appdb", session_id, view_candidate, BODY, sql=VIEW_ORIGINAL, candidate_sql=_view_sql(),
        view_name=VIEW, phase="screening", idempotency_key="view-1", keep_for_session=True,
    )

    assert kept["classification"] in {"promising", "improved"}, kept["reason"]
    assert views.events == ["build_view", "compare"]
    assert views.state.exists
    assert kept["lease"]["status"] == "active"
    assert kept["test_objects"]["still_held"][0]["kind"] == "indexed_view"
    assert kept["test_objects"]["still_held"][0]["name"] == f"dbo.{VIEW}"

    # A second view call with the same definition reuses the view: no build.
    again = app.performance_workflows.add_candidate(
        session_id, _view_sql() + " OPTION (MAXDOP 1)", strategy="rewrite_plus_index"
    )["candidate_id"]
    reused = await app._benchmark_indexed_view_candidate(
        "appdb", session_id, again, BODY, sql=VIEW_ORIGINAL,
        candidate_sql=_view_sql() + " OPTION (MAXDOP 1)", view_name=VIEW, phase="screening",
        idempotency_key="view-2", keep_for_session=True,
    )

    assert views.events == ["build_view", "compare", "compare"]
    assert reused["test_objects"]["reused"][0]["name"] == f"dbo.{VIEW}"
    assert reused["test_objects"]["built"] == []
    assert reused["parameter_results"][0]["baseline_source"] == "session_before_hold"
    assert len(app.performance_store.list_open_index_leases()) == 1

    # An index on the base table, kept too: the same lease now lists both.
    indexes = IndexSandbox(app, monkeypatch, fallback_fetch=views._fetch)
    app.executor.execute_session_exactly_once = AsyncMock(side_effect=views._snapshot)  # type: ignore[method-assign]

    async def profile_with_index(_database: str, sql: str, **_kwargs: Any) -> Any:
        if VIEW in sql:
            return _profile(4, index_name=clustered)
        return _profile(30, index_name=next(iter(indexes.indexes), None))

    app.plans.profile_query = AsyncMock(side_effect=profile_with_index)  # type: ignore[method-assign]
    index_candidate = app.performance_workflows.add_candidate(
        session_id, VIEW_ORIGINAL, strategy="index"
    )["candidate_id"]
    with_index = await app._benchmark_index_candidate(
        "appdb", session_id, index_candidate, VIEW_ORIGINAL, "dbo", "Posts", ["OwnerUserId"],
        None, None, False, "screening", True, None, 30, "index-1", None, keep_for_session=True,
    )

    assert with_index["classification"] in {"promising", "improved"}, with_index["reason"]
    assert sorted(item["kind"] for item in with_index["test_objects"]["still_held"]) == [
        "index",
        "indexed_view",
    ]
    assert len(app.performance_store.list_open_index_leases()) == 1
    assert views.state.exists and len(indexes.indexes) == 1

    final = await app._finalize_tuning_session("appdb", session_id, None, "measured", None)

    assert sorted(item["kind"] for item in final["held_test_objects_dropped"]) == [
        "index",
        "indexed_view",
    ]
    assert all(item["dropped"] for item in final["held_test_objects_dropped"])
    assert "drop_view" in views.events
    assert not views.state.exists
    assert indexes.indexes == {}
    assert app.performance_store.list_open_index_leases() == []


@pytest.mark.asyncio
async def test_an_expired_hold_of_a_view_is_dropped_by_the_reconcile(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _view_app(server_config_factory)
    views = _ViewSandbox(app, monkeypatch)
    case = app.performance_workflows.start_case("appdb", VIEW_ORIGINAL)
    session_id = app.performance_workflows.start_session(case.case_id, "appdb")["session_id"]
    candidate_id = app.performance_workflows.add_candidate(
        session_id, _view_sql(), strategy="rewrite_plus_index"
    )["candidate_id"]
    clustered = str(views.plan.clustered_index.index_name)

    async def profile(_database: str, sql: str, **_kwargs: Any) -> Any:
        reads_view = VIEW in sql
        return _profile(4 if reads_view else 900, index_name=clustered if reads_view else None)

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]
    await app._benchmark_indexed_view_candidate(
        "appdb", session_id, candidate_id, BODY, sql=VIEW_ORIGINAL, candidate_sql=_view_sql(),
        view_name=VIEW, phase="screening", idempotency_key="view-1", keep_for_session=True,
    )
    lease = _held(app)[0]
    app.performance_store._connection.execute(
        "UPDATE index_leases SET expires_at_utc = ? WHERE lease_id = ?",
        ((datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(), lease["lease_id"]),
    )

    summary = await app._cleanup_expired_index_leases()

    assert summary["cleaned"] == 1
    assert not views.state.exists
    assert app.performance_store.list_open_index_leases() == []


@pytest.mark.asyncio
async def test_the_session_view_lists_a_held_object_only_while_it_is_held(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    candidate_id = _candidate(app, session_id, ORIGINAL, "index")

    assert "held_test_objects" not in await app._get_tuning_session("appdb", session_id)
    kept = await _bench(app, session_id, candidate_id, "k1", keep_for_session=True)

    payload = app._with_held_objects_present("appdb", session_id, {"classification": "neutral"})

    # A rewrite benchmark in this session says that the index was in place.
    present = payload["held_test_objects_present"]
    assert present["objects"][0]["index_name"] == kept["indexes"][0]["index_name"]
    assert "lease_owner_fence" not in str(await app._get_tuning_session("appdb", session_id))
    assert kept["lease"]["metadata"].get("lease_owner_fence") is None


@pytest.mark.asyncio
async def test_a_failed_build_with_keep_leaves_nothing_and_closes_the_lease(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    sandbox.create_errors.append(RuntimeError("Invalid column name 'nope'. (207)"))
    session_id, _case = _session(app)
    candidate_id = _candidate(app, session_id, ORIGINAL, "index")

    result = await _bench(app, session_id, candidate_id, "k1", keep_for_session=True)

    assert result["classification"] == "inconclusive"
    assert result["test_objects"]["still_held"] == []
    assert sandbox.indexes == {}
    assert app.performance_store.list_open_index_leases() == []


@pytest.mark.asyncio
async def test_a_call_that_stops_before_its_first_statement_gives_the_hold_back_its_expiry(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """While a call uses a hold the open session protects the lease; a call that
    never started must not leave that protection on an idle hold."""

    app = _app(server_config_factory)
    sandbox = IndexSandbox(app, monkeypatch)
    session_id, _case = _session(app)
    first = _candidate(app, session_id, ORIGINAL, "index")
    second = _candidate(app, session_id, REWRITE, "rewrite_plus_index")
    await _bench(app, session_id, first, "k1", keep_for_session=True)

    def refuse(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("the candidate could not start")

    monkeypatch.setattr(app.tuning_sessions, "start_screening", refuse)
    with pytest.raises(RuntimeError, match="could not start"):
        await _bench(app, session_id, second, "k2", candidate_sql=REWRITE)

    [lease] = _held(app)
    assert lease["status"] == "active"
    assert lease["metadata"]["hold_state"] == "idle"
    expires = datetime.fromisoformat(lease["expires_at_utc"])
    assert expires - datetime.now(timezone.utc) <= timedelta(minutes=30)
    assert len(sandbox.indexes) == 1
