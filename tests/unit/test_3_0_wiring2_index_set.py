"""Wiring 2, item 2: an index experiment an agent can use, under the same lease rules.

Tasks T1, T3 and T5 of the first live comparison: the tool tested one index
under a name it chose, so a two-index design or a named design could not be
measured, and agents fell back to raw DDL. A set is one candidate under one
lease: every index is created or none is left, and every one is dropped on
every path.
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.database_policy import DatabasePolicy
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.database_policy import DatabasePolicyValidationError
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import parse_candidate_key
from azure_sql_mcp.index_optimizer import IndexCandidate
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.server import TEST_INDEX_PREFIX
from azure_sql_mcp.server import generated_test_index_name
from tests.unit.test_index_benchmark_workflow import _profile
from tests.unit.test_index_benchmark_workflow import _set_session_deadline

SQL = (
    "SELECT DISTINCT u.Id FROM dbo.Badges AS b JOIN dbo.Users AS u ON u.Id = b.UserId "
    "WHERE b.Name = N'Teacher'"
)
BADGES = {"table_name": "Badges", "key_columns": ["Name", "UserId"]}
USERS = {"table_name": "Users", "key_columns": ["Id"], "include_columns": ["Reputation"]}


class _ProcessDied(BaseException):
    """The MCP process stopped: nothing after this point runs or is written."""


def _policy(**overrides: Any) -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "development",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "allow_test_indexes": True,
                    "max_benchmark_executions": 80,
                    **overrides,
                }
            },
        }
    )


class _Sandbox:
    """A database catalog: an index exists once its CREATE batch committed.

    The CREATE batch commits the index and its two ownership markers together,
    as the real batch does. ``fail`` maps an index ordinal (order of creation)
    to what its CREATE raises; ``commit_before_failure`` says whether the
    batch committed before the error reached the client.
    """

    def __init__(self, app: AzureSqlMcpApplication, monkeypatch: pytest.MonkeyPatch) -> None:
        self.app = app
        self.indexes: dict[str, ExistingIndex] = {}
        self.markers: dict[str, tuple[str, str]] = {}
        self.created: list[str] = []
        self.dropped: list[str] = []
        self.create_calls: list[dict[str, Any]] = []
        self.fail: dict[int, BaseException] = {}
        self.commit_before_failure = True
        self.catalog_failures: list[BaseException] = []
        self.foreign_markers: set[int] = set()
        self.dead = False
        self.next_index_id = 2
        app._create_test_index = AsyncMock(side_effect=self._create)  # type: ignore[method-assign]
        app._drop_test_index = AsyncMock(side_effect=self._drop)  # type: ignore[method-assign]
        app.executor.fetch_all = AsyncMock(side_effect=self._fetch)  # type: ignore[method-assign]
        monkeypatch.setattr("azure_sql_mcp.server.collect_existing_indexes", self._catalog)

    def _alive(self) -> None:
        if self.dead:
            raise _ProcessDied

    async def _catalog(
        self, _executor: Any, _database: str, *, schema_name: str, table_name: str
    ) -> list[ExistingIndex]:
        self._alive()
        if self.catalog_failures:
            raise self.catalog_failures.pop(0)
        return [
            index
            for index in self.indexes.values()
            if index.schema == schema_name and index.table == table_name
        ]

    async def _create(
        self,
        _database: str,
        schema: str,
        table: str,
        index_name: str,
        key_columns: list[str],
        include_columns: list[str] | None,
        online: bool,
        **kwargs: Any,
    ) -> dict[str, str]:
        self._alive()
        ordinal = len(self.create_calls)
        self.create_calls.append({"index_name": index_name, "online": online, **kwargs})
        failure = self.fail.get(ordinal)
        if failure is None or self.commit_before_failure:
            compression = kwargs.get("data_compression")
            candidate = IndexCandidate(
                schema=schema,
                table=table,
                key_columns=tuple(key_columns),
                include_columns=tuple(include_columns or ()),
                filter_definition=kwargs.get("filter_definition"),
                is_unique=bool(kwargs.get("is_unique")),
                index_name=index_name,
                compression=compression,
            )
            self.indexes[index_name] = ExistingIndex(
                schema=schema,
                table=table,
                index_id=self.next_index_id,
                name=index_name,
                index_type="NONCLUSTERED",
                key_columns=tuple(parse_candidate_key(column) for column in key_columns),
                include_columns=tuple(include_columns or ()),
                filter_definition=kwargs.get("filter_definition"),
                is_unique=bool(kwargs.get("is_unique")),
                partition_compression=((1, compression),) if compression else (),
            )
            self.next_index_id += 1
            owner = (
                "index-lease-someone-else-1234"
                if ordinal in self.foreign_markers
                else str(kwargs["ownership_proof"])
            )
            self.markers[index_name] = (owner, candidate.definition_fingerprint)
            self.created.append(index_name)
        if failure is not None:
            if isinstance(failure, _ProcessDied):
                self.dead = True
            raise failure
        return {"status": "completed"}

    async def _drop(
        self, _database: str, _schema: str, _table: str, index_name: str, **kwargs: Any
    ) -> dict[str, str]:
        self._alive()
        owner, definition = self.markers[index_name]
        # The guarded DROP rechecks both markers and the index_id.
        assert kwargs["ownership_proof"] == owner
        assert kwargs["expected_definition_fingerprint"] == definition
        assert kwargs["expected_index_id"] == self.indexes[index_name].index_id
        del self.indexes[index_name]
        self.dropped.append(index_name)
        return {"status": "completed"}

    async def _fetch(
        self, _database: str, sql: str, *, params: Any = (), **_kwargs: Any
    ) -> list[dict[str, Any]]:
        self._alive()
        if "sys.sql_modules" in sql and "object_type_code" in sql:
            # Every object of these tests is a user table.
            return [{"object_type_code": "U", "object_type": "USER_TABLE", "is_encrypted": 0}]
        if "owner_marker" in sql and "definition_marker" in sql:
            index = self.indexes.get(str(params[-1]))
            if index is None:
                return []
            owner, definition = self.markers[index.name]
            return [
                {"index_id": index.index_id, "owner_marker": owner, "definition_marker": definition}
            ]
        if "IndexDepth" in sql:
            index = self.indexes[str(params[-1])]
            compressed = bool(index.partition_compression)
            return [
                {
                    "index_depth": 3 if compressed else 4,
                    "used_pages": 1280 if compressed else 2560,
                    "row_count": 7_600_000,
                }
            ]
        if "sys.tables" in sql and len(params) == 2:
            return [{"schema_name": "dbo", "table_name": str(params[1])}]
        return []


def _app(server_config_factory, policy: DatabasePolicySet | None = None) -> AzureSqlMcpApplication:
    app = AzureSqlMcpApplication(
        server_config_factory(
            access_mode=AccessMode.UNRESTRICTED,
            write_policy=WritePolicy.APPLY,
            profile=McpProfile.SANDBOX,
            performance_state_dir=":memory:",
        )
    )
    app.database_policy = policy or _policy()
    app.performance_workflows.database_policy = app.database_policy
    return app


def _session(
    app: AzureSqlMcpApplication,
    sql: str = SQL,
    *,
    parameter_cases: list[dict[str, Any]] | None = None,
) -> tuple[str, str]:
    case = app.performance_workflows.start_case("appdb", sql, parameter_cases=parameter_cases)
    session = app.performance_workflows.start_session(case.case_id, "appdb")
    candidate = app.performance_workflows.add_candidate(
        session["session_id"], sql, strategy="index"
    )
    return session["session_id"], candidate["candidate_id"]


def _names(prefix: str = TEST_INDEX_PREFIX, *specs: dict[str, Any]) -> list[str]:
    return [
        generated_test_index_name(
            prefix,
            IndexCandidate(
                schema="dbo",
                table=spec["table_name"],
                key_columns=tuple(spec["key_columns"]),
                include_columns=tuple(spec.get("include_columns") or ()),
                compression=spec.get("data_compression"),
            ),
        )
        for spec in specs
    ]


def _plan(*index_names: str, elapsed_ms: float = 20.0) -> Any:
    profile = _profile(elapsed_ms, index_name=index_names[0] if index_names else None)
    if len(index_names) > 1:
        scans = "".join(f'<IndexScan Index="[dbo].[{name}]" />' for name in index_names)
        object.__setattr__(
            profile.plan,
            "raw_xml",
            '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">'
            f"{scans}</ShowPlanXML>",
        )
    return profile


async def _benchmark(
    app: AzureSqlMcpApplication,
    session_id: str,
    candidate_id: str,
    *,
    sql: str = SQL,
    key: str = "set-key",
    phase: str = "screening",
    parameter_cases: list[dict[str, Any]] | None = None,
    **options: Any,
) -> dict[str, Any]:
    single = options.pop("single", None) or {}
    return await app._benchmark_index_candidate(
        "appdb",
        session_id,
        candidate_id,
        sql,
        single.get("schema_name"),
        single.get("table_name"),
        single.get("key_columns"),
        single.get("include_columns"),
        None,
        False,
        phase,
        True,
        None,
        30,
        key,
        parameter_cases,
        **options,
    )


def _open_leases(app: AzureSqlMcpApplication) -> list[dict[str, Any]]:
    return app.performance_store.list_open_index_leases()


@pytest.mark.asyncio
async def test_a_two_index_set_on_two_tables_is_one_candidate_under_one_lease(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # T3: the design needs one index on each of two tables. Tested one at a
    # time neither helps, so the agent measured the pair with raw DDL.
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app)
    badges, users = _names(TEST_INDEX_PREFIX, BADGES, USERS)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(4000) for _ in range(3)],
            *[_plan(badges, users) for _ in range(3)],
            *[_profile(4000) for _ in range(3)],
        ]
    )

    result = await _benchmark(app, session_id, candidate_id, indexes=[BADGES, USERS])

    assert result["classification"] == "improved"
    assert sandbox.created == [badges, users]
    assert sorted(sandbox.dropped) == sorted([badges, users])
    assert sandbox.indexes == {}
    # One lease for the set: the gate that allows one experiment per database
    # sees the pair as one experiment.
    assert result["lease"]["status"] == "cleaned"
    assert [member["index_name"] for member in result["lease"]["metadata"]["members"]] == [
        badges,
        users,
    ]
    assert _open_leases(app) == []
    report = {item["index_name"]: item for item in result["indexes"]}
    assert report[badges]["built"] is True
    assert report[badges]["table"] == "Badges"
    assert report[badges]["size_mb"] == 20.0
    assert report[badges]["index_depth"] == 4
    assert report[users]["used_in_cases"] == ["default"]
    assert result["index_ddl"].count("CREATE NONCLUSTERED INDEX") == 2
    assert result["rollback_ddl"].count("DROP INDEX") == 2
    assert "unused_indexes" not in result


@pytest.mark.asyncio
async def test_an_index_of_the_set_that_no_plan_used_is_named(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The gain is real, but one index did not earn its write cost.
    app = _app(server_config_factory)
    _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app)
    badges, users = _names(TEST_INDEX_PREFIX, BADGES, USERS)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(4000) for _ in range(3)],
            *[_plan(badges) for _ in range(3)],
            *[_profile(4000) for _ in range(3)],
        ]
    )

    result = await _benchmark(app, session_id, candidate_id, indexes=[BADGES, USERS])

    assert result["classification"] == "improved"
    assert result["unused_indexes"] == [users]
    assert users in result["reason"]


@pytest.mark.asyncio
async def test_a_definite_create_failure_on_the_second_index_leaves_no_index(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # All created or none left: the first index must not outlive a set that
    # could not be built, or it changes every later baseline on the table.
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    sandbox.fail[1] = RuntimeError(
        "[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Lock request "
        "time out period exceeded. (1222) (SQLExecDirectW)"
    )
    sandbox.commit_before_failure = False
    session_id, candidate_id = _session(app)
    badges, users = _names(TEST_INDEX_PREFIX, BADGES, USERS)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(4000) for _ in range(3)]
    )

    result = await _benchmark(app, session_id, candidate_id, indexes=[BADGES, USERS])

    assert result["classification"] == "inconclusive"
    assert result["create_failure"]["outcome"] == "definite_rollback"
    assert result["create_failure"]["index_name"] == users
    assert sandbox.created == [badges]
    assert sandbox.dropped == [badges]
    assert sandbox.indexes == {}
    assert result["lease"]["status"] == "cleaned"
    assert _open_leases(app) == []
    assert app.plans.profile_query.await_count == 3
    assert [item["built"] for item in result["indexes"]] == [True, False]
    assert result["session_continues"] is True


@pytest.mark.asyncio
async def test_a_lost_reply_on_the_second_create_is_proven_by_its_marker_and_both_are_dropped(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    sandbox.fail[1] = TimeoutError("reply lost after commit")
    session_id, candidate_id = _session(app)
    names = _names(TEST_INDEX_PREFIX, BADGES, USERS)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(4000) for _ in range(3)]
    )

    result = await _benchmark(app, session_id, candidate_id, indexes=[BADGES, USERS])

    assert result["classification"] == "inconclusive"
    assert sorted(sandbox.dropped) == sorted(names)
    assert sandbox.indexes == {}
    assert result["lease"]["status"] == "cleaned"
    assert _open_leases(app) == []


@pytest.mark.asyncio
async def test_an_unprovable_second_create_keeps_the_lease_open_and_still_drops_the_first(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The second index exists but its marker names another owner: it is never
    # dropped, the lease stays open for a person, and the first index, which
    # is proven ours, does not stay behind with it.
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    sandbox.fail[1] = TimeoutError("reply lost after commit")
    sandbox.foreign_markers.add(1)
    session_id, candidate_id = _session(app)
    badges, users = _names(TEST_INDEX_PREFIX, BADGES, USERS)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(4000) for _ in range(3)]
    )

    result = await _benchmark(app, session_id, candidate_id, indexes=[BADGES, USERS])

    assert sandbox.dropped == [badges]
    assert list(sandbox.indexes) == [users]
    assert result["lease"]["status"] == "cleanup_required"
    [lease] = _open_leases(app)
    members = {member["index_name"]: member for member in lease["metadata"]["members"]}
    assert members[users]["cleanup_policy"] == "do_not_adopt_or_drop"
    assert "cleaned_at_utc" in members[badges]

    # The open lease gates the database exactly as a one-index lease does.
    second = app.performance_workflows.add_candidate(
        session_id, SQL, strategy="index", idempotency_key="second"
    )
    with pytest.raises(Exception, match="already has active lease"):
        await _benchmark(
            app, session_id, second["candidate_id"], key="second-key", indexes=[BADGES]
        )
    assert len(sandbox.create_calls) == 2


@pytest.mark.asyncio
async def test_a_cancel_between_the_creates_drops_the_created_index_and_is_re_raised(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The cancel arrives after the first CREATE committed and before the
    # second was sent. The first index is proven by its marker and dropped.
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app)
    badges, users = _names(TEST_INDEX_PREFIX, BADGES, USERS)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(4000) for _ in range(3)]
    )
    real_catalog = sandbox._catalog
    reads = 0

    async def cancel_after_first_create(*args: Any, **kwargs: Any) -> Any:
        nonlocal reads
        reads += 1
        if reads == 3:  # two pre-create table reads, then the post-create read
            raise asyncio.CancelledError
        return await real_catalog(*args, **kwargs)

    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes", cancel_after_first_create
    )

    with pytest.raises(asyncio.CancelledError):
        await _benchmark(app, session_id, candidate_id, indexes=[BADGES, USERS])

    assert sandbox.created == [badges]
    assert sandbox.dropped == [badges]
    assert sandbox.indexes == {}
    assert _open_leases(app) == []
    assert app.tuning_sessions.get_candidate(candidate_id).state == "inconclusive"


@pytest.mark.asyncio
async def test_a_cancel_during_the_second_create_drops_the_first_and_keeps_the_lease_open(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The second CREATE was sent and its outcome is unknown: the lease stays
    # open for it, and the first index is still dropped.
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    sandbox.fail[1] = asyncio.CancelledError()
    sandbox.commit_before_failure = False
    session_id, candidate_id = _session(app)
    badges, users = _names(TEST_INDEX_PREFIX, BADGES, USERS)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(4000) for _ in range(3)]
    )

    with pytest.raises(asyncio.CancelledError):
        await _benchmark(app, session_id, candidate_id, indexes=[BADGES, USERS])

    assert sandbox.dropped == [badges]
    assert sandbox.indexes == {}
    [lease] = _open_leases(app)
    assert lease["status"] == "cleanup_required"
    members = {member["index_name"]: member for member in lease["metadata"]["members"]}
    assert members[users]["ownership_status"] == "uncertain_after_create_dispatch"


def _expire(app: AzureSqlMcpApplication, lease_id: str) -> None:
    app.performance_store._connection.execute(
        "UPDATE index_leases SET expires_at_utc = ? WHERE lease_id = ?",
        ((datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(), lease_id),
    )
    app.performance_store._connection.commit()
    # The session of the dead process has passed its deadline too.
    _set_session_deadline(
        app,
        app.performance_store.get_index_lease(lease_id)["session_id"],
        (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
    )


async def _crash_during_second_create(
    server_config_factory, monkeypatch: pytest.MonkeyPatch, *, second_committed: bool
) -> tuple[AzureSqlMcpApplication, _Sandbox, list[str]]:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    sandbox.fail[1] = _ProcessDied()
    sandbox.commit_before_failure = second_committed
    session_id, candidate_id = _session(app)
    names = _names(TEST_INDEX_PREFIX, BADGES, USERS)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[_profile(4000) for _ in range(3)]
    )
    real_update = app.performance_store.update_index_lease

    def update_unless_dead(*args: Any, **kwargs: Any) -> Any:
        if sandbox.dead:
            raise _ProcessDied
        return real_update(*args, **kwargs)

    monkeypatch.setattr(app.performance_store, "update_index_lease", update_unless_dead)
    with pytest.raises(_ProcessDied):
        await _benchmark(app, session_id, candidate_id, indexes=[BADGES, USERS])
    # A new process: it can read and write again, and the lease has expired.
    sandbox.dead = False
    monkeypatch.setattr(app.performance_store, "update_index_lease", real_update)
    [lease] = _open_leases(app)
    assert lease["status"] == "pending_create"
    _expire(app, lease["lease_id"])
    return app, sandbox, names


@pytest.mark.asyncio
async def test_crash_recovery_drops_every_index_of_the_set_that_its_markers_prove(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The process died while the second CREATE ran, and that CREATE
    # committed. The next process must find both indexes from the one lease.
    app, sandbox, names = await _crash_during_second_create(
        server_config_factory, monkeypatch, second_committed=True
    )
    assert sorted(sandbox.indexes) == sorted(names)

    summary = await app._cleanup_expired_index_leases()

    assert summary == {"examined": 1, "cleaned": 1, "cleanup_required": 0}
    assert sorted(sandbox.dropped) == sorted(names)
    assert sandbox.indexes == {}
    assert _open_leases(app) == []


@pytest.mark.asyncio
async def test_crash_recovery_drops_the_first_index_when_the_second_outcome_is_unknown(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The second CREATE was sent and is not visible: it may still commit, so
    # the lease stays open. The first index is proven and is dropped now.
    app, sandbox, names = await _crash_during_second_create(
        server_config_factory, monkeypatch, second_committed=False
    )
    badges, _users = names

    summary = await app._cleanup_expired_index_leases()

    assert summary == {"examined": 1, "cleaned": 0, "cleanup_required": 1}
    assert sandbox.dropped == [badges]
    [lease] = _open_leases(app)
    assert lease["status"] == "cleanup_required"


@pytest.mark.asyncio
async def test_crash_recovery_never_drops_an_index_of_the_set_with_a_foreign_marker(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, sandbox, names = await _crash_during_second_create(
        server_config_factory, monkeypatch, second_committed=True
    )
    badges, users = names
    sandbox.markers[users] = ("index-lease-someone-else-1234", sandbox.markers[users][1])

    summary = await app._cleanup_expired_index_leases()

    assert summary["cleanup_required"] == 1
    assert sandbox.dropped == [badges]
    assert list(sandbox.indexes) == [users]
    assert _open_leases(app)[0]["status"] == "cleanup_required"


@pytest.mark.asyncio
async def test_a_leftover_test_index_on_any_table_of_the_set_stops_the_run_before_ddl(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    sandbox.indexes["IX_Testing_0123456789abcdef"] = ExistingIndex(
        schema="dbo",
        table="Users",
        index_id=9,
        name="IX_Testing_0123456789abcdef",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("Reputation"),),
    )
    session_id, candidate_id = _session(app)
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id, indexes=[BADGES, USERS])

    assert result["failure_code"] == "leftover_test_index"
    assert result["remediation"] == "DROP INDEX [IX_Testing_0123456789abcdef] ON [dbo].[Users];"
    assert sandbox.create_calls == []
    app.plans.profile_query.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_name_prefix_the_database_policy_lists_is_used(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # T1: the engagement required its own index name, the tool generated its
    # own, so no test index was built and the answer was not measured.
    # S13a: an unlisted prefix is no longer refused (owner decision); it falls
    # back to the policy prefix with a note, see test_3_0_s13a_index_prefix.py.
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app)

    app.database_policy = _policy(test_index_name_prefixes=["IX_Bench_"])
    [badges] = _names("IX_Bench_", BADGES)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(4000) for _ in range(3)],
            *[_plan(badges) for _ in range(3)],
            *[_profile(4000) for _ in range(3)],
        ]
    )

    result = await _benchmark(
        app, session_id, candidate_id, indexes=[BADGES], index_name_prefix="IX_Bench_"
    )

    assert result["classification"] == "improved"
    # The caller chose the prefix; the rest of the name is still generated.
    assert re.fullmatch(r"IX_Bench_Badges_\w+_[0-9a-f]{8}", sandbox.created[0])
    assert "index_name_note" not in result
    assert sandbox.dropped == sandbox.created
    assert result["lease"]["index_name"] == badges


@pytest.mark.asyncio
async def test_a_leftover_index_under_a_policy_prefix_is_found_by_the_precheck(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory, _policy(test_index_name_prefixes=["IX_Bench_"]))
    sandbox = _Sandbox(app, monkeypatch)
    for name in ("IX_Bench_0123456789abcdef", "IX_Bench_OrdersByDate"):
        sandbox.indexes[name] = ExistingIndex(
            schema="dbo",
            table="Badges",
            index_id=9,
            name=name,
            index_type="NONCLUSTERED",
            key_columns=(parse_candidate_key("Date"),),
        )
    session_id, candidate_id = _session(app)

    result = await _benchmark(app, session_id, candidate_id, indexes=[BADGES])

    # Only a name of the generated form is a leftover: an application index
    # that shares the prefix is not offered for a DROP.
    assert result["leftover_test_indexes"] == ["IX_Bench_0123456789abcdef"]
    assert sandbox.create_calls == []


def test_the_guarded_ddl_refuses_a_prefix_the_policy_does_not_list() -> None:
    assert AzureSqlMcpApplication._validate_test_index_name(
        "IX_Bench_0123456789abcdef", ("IX_Testing_", "IX_Bench_")
    )
    with pytest.raises(ValueError, match="test prefix"):
        AzureSqlMcpApplication._validate_test_index_name("IX_Bench_0123456789abcdef")


def test_policy_prefixes_are_validated_and_default_to_the_workflow_prefix() -> None:
    assert DatabasePolicy.from_mapping(
        "appdb", {"environment": "dev", "allow_read": True}
    ).test_index_name_prefixes == (TEST_INDEX_PREFIX,)
    for bad in (["IX"], [], ["IX_A_", "ix_a_"], ["IX Bench"], "IX_Bench_"):
        with pytest.raises(DatabasePolicyValidationError):
            DatabasePolicy.from_mapping(
                "appdb",
                {"environment": "dev", "allow_read": True, "test_index_name_prefixes": bad},
            )
    with pytest.raises(DatabasePolicyValidationError, match="test_index_maxdop"):
        DatabasePolicy.from_mapping(
            "appdb", {"environment": "dev", "allow_read": True, "test_index_maxdop": 65}
        )


@pytest.mark.asyncio
async def test_policy_maxdop_and_offline_reach_the_test_index_ddl(
    server_config_factory,
) -> None:
    # An index build at the database default DOP can take every core of a
    # shared test database; the policy is where its owner caps it.
    app = _app(server_config_factory, _policy(test_index_maxdop=2))

    preview = await app._create_test_index(
        "appdb", "dbo", "Posts", "IX_Testing_preview", ["OwnerUserId"], None, True,
        data_compression="ROW",
    )

    assert "MAXDOP = 2" in preview["sql_preview"]
    assert "DATA_COMPRESSION = ROW" in preview["sql_preview"]
    with pytest.raises(PermissionError, match="offline"):
        await app._create_test_index(
            "appdb", "dbo", "Posts", "IX_Testing_preview", ["OwnerUserId"], None, False
        )
    app.database_policy = _policy(allow_offline_test_index=True)
    offline = await app._create_test_index(
        "appdb", "dbo", "Posts", "IX_Testing_preview", ["OwnerUserId"], None, False
    )
    assert "ONLINE" not in offline["sql_preview"]


POSTS_SQL = (
    "SELECT TOP (10) p.Id, p.Title FROM dbo.Posts AS p WHERE p.OwnerUserId = 22656 "
    "ORDER BY p.Score DESC, p.Id"
)
POSTS = {
    "schema_name": "dbo",
    "table_name": "Posts",
    "key_columns": ["OwnerUserId", "Score DESC", "Id"],
    "include_columns": ["Title"],
}


@pytest.mark.asyncio
async def test_row_compression_is_built_and_its_depth_and_size_are_reported(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # T1: the bare model won by noticing that ROW compression kept the index
    # one level shallower. The product could neither build nor see that.
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, POSTS_SQL)
    [compressed] = _names(TEST_INDEX_PREFIX, {**POSTS, "data_compression": "ROW"})
    [plain] = _names(TEST_INDEX_PREFIX, POSTS)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(31) for _ in range(3)],
            *[_plan(compressed, elapsed_ms=1) for _ in range(3)],
            *[_profile(31) for _ in range(3)],
        ]
    )

    result = await _benchmark(
        app, session_id, candidate_id, sql=POSTS_SQL, single=POSTS, data_compression="ROW"
    )

    assert compressed != plain
    assert sandbox.create_calls[0]["data_compression"] == "ROW"
    assert "DATA_COMPRESSION = ROW" in result["index_ddl"]
    [report] = result["indexes"]
    assert report["data_compression"] == "ROW"
    assert report["index_depth"] == 3
    assert report["size_mb"] == 10.0
    assert result["lease"]["metadata"]["data_compression"] == "ROW"
    assert sandbox.indexes == {}


@pytest.mark.asyncio
async def test_a_replacement_candidate_is_hinted_and_labelled_hinted_evidence(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # With the old index still on the table the optimizer may keep choosing
    # it, and the test would measure nothing. The hint measures the new index;
    # it never proves the optimizer will choose it.
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    sandbox.indexes["IX_Posts_OwnerUserId"] = ExistingIndex(
        schema="dbo",
        table="Posts",
        index_id=7,
        name="IX_Posts_OwnerUserId",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("OwnerUserId"),),
    )
    session_id, candidate_id = _session(app, POSTS_SQL)
    [name] = _names(TEST_INDEX_PREFIX, POSTS)
    sent: list[str] = []

    async def profile(_database: str, sql: str, **_kwargs: Any) -> Any:
        sent.append(sql)
        hinted = "WITH (INDEX(" in sql
        return _plan(name, elapsed_ms=1) if hinted else _profile(31)

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="existing index"):
        await _benchmark(
            app, session_id, candidate_id, sql=POSTS_SQL, single=POSTS, replaces="IX_Missing"
        )
    assert sandbox.create_calls == []

    result = await _benchmark(
        app,
        session_id,
        candidate_id,
        sql=POSTS_SQL,
        single=POSTS,
        replaces="IX_Posts_OwnerUserId",
        phase="finalist",
    )

    candidate_runs = [sql for sql in sent if "WITH (INDEX(" in sql]
    assert len(candidate_runs) == 5
    assert all(f"WITH (INDEX([{name}]))" in sql for sql in candidate_runs)
    assert result["proof_scope"] == "hinted_evidence"
    assert result["deployment_ready"] is False
    assert result["classification"] == "performance_only"
    assert result["replacement_experiment"]["replaces"] == ["IX_Posts_OwnerUserId"]
    assert result["equivalence"][0]["same_sql"] is False
    # The replaced index is still there; only the test index was dropped.
    assert list(sandbox.indexes) == ["IX_Posts_OwnerUserId"]


OR_SQL = (
    "SELECT p.Id, p.Title FROM dbo.Posts AS p "
    "WHERE p.OwnerUserId = @u OR p.LastEditorUserId = @u"
)
OR_CASES = [
    {"name": "active", "values": {"u": 22656}, "types": {"u": "int"}, "weight": 1.0},
    {"name": "scan", "values": {"u": 5}, "types": {"u": "int"}, "weight": 1.0},
]
EDITOR = {"table_name": "Posts", "key_columns": ["LastEditorUserId"]}
EDITOR_WITH_ID = {"table_name": "Posts", "key_columns": ["LastEditorUserId", "Id"]}


@pytest.mark.asyncio
async def test_a_slow_baseline_case_runs_once_and_later_index_candidates_reuse_it(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # T5: one case is a 150 s scan. Three runs before and three after per
    # candidate made the measured workflow unusable, so no session was opened.
    app = _app(server_config_factory)
    _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, OR_SQL, parameter_cases=OR_CASES)
    runs: list[tuple[str, bool]] = []

    def scripted(index_name: str):
        async def profile(_database: str, contract: Any, **_kwargs: Any) -> Any:
            indexed = index_name in app_indexes()
            runs.append((contract.bucket_id, indexed))
            if indexed:
                return _plan(index_name, elapsed_ms=2)
            return _profile(150_000 if contract.bucket_id == "scan" else 110)

        return profile

    def app_indexes() -> set[str]:
        return set(sandbox_indexes)

    sandbox_indexes = app._create_test_index.side_effect.__self__.indexes
    [first_name] = _names(TEST_INDEX_PREFIX, EDITOR)
    app.plans.profile_parameterized_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=scripted(first_name)
    )

    first = await _benchmark(
        app,
        session_id,
        candidate_id,
        sql=OR_SQL,
        parameter_cases=OR_CASES,
        indexes=[EDITOR],
        slow_baseline_cases={"scan": 150},
    )

    assert first["classification"] == "improved"
    assert [run for run in runs if run == ("scan", False)] == [("scan", False)]
    assert len([run for run in runs if run == ("active", False)]) == 6
    scan = next(item for item in first["parameter_results"] if item["parameter_case"] == "scan")
    assert scan["baseline_source"] == "single_run"
    assert scan["baseline_single_sample"] is True
    assert "150 seconds" in scan["baseline_note"]
    assert first["executions"] == 3 * 3 + 1 + 3
    assert first["equivalence"][1]["status"] == "match"

    second = app.performance_workflows.add_candidate(
        session_id, OR_SQL, strategy="index", idempotency_key="second"
    )
    [second_name] = _names(TEST_INDEX_PREFIX, EDITOR_WITH_ID)
    runs.clear()
    app.plans.profile_parameterized_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=scripted(second_name)
    )

    result = await _benchmark(
        app,
        session_id,
        second["candidate_id"],
        sql=OR_SQL,
        key="second-key",
        parameter_cases=OR_CASES,
        indexes=[EDITOR_WITH_ID],
        slow_baseline_cases={"scan": 150},
    )

    assert ("scan", False) not in runs
    scan = next(item for item in result["parameter_results"] if item["parameter_case"] == "scan")
    assert scan["baseline_source"] == "session_single_run"
    assert scan["baseline_evidence_id"] == first["evidence_id"]
    assert scan["baseline"]["elapsed_ms"] == 150_000
    assert result["classification"] == "improved"
    assert result["executions"] == 3 * 3 + 3


async def _call(app: AzureSqlMcpApplication, name: str, arguments: dict[str, Any]) -> Any:
    return await app.mcp._tool_manager.call_tool(name, arguments)


@pytest.mark.asyncio
async def test_the_tool_takes_a_set_and_refuses_a_mixed_or_oversized_request(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    session_id, candidate_id = _session(app)
    badges, users = _names(TEST_INDEX_PREFIX, BADGES, USERS)
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(4000) for _ in range(3)],
            *[_plan(badges, users) for _ in range(3)],
            *[_profile(4000) for _ in range(3)],
        ]
    )
    base = {
        "session_id": session_id,
        "candidate_id": candidate_id,
        "sql": SQL,
        "idempotency_key": "tool-key",
    }

    with pytest.raises(Exception, match="not both"):
        await _call(
            app,
            "benchmark_index_candidate",
            {**base, "indexes": [BADGES], "table_name": "Badges", "schema_name": "dbo"},
        )
    with pytest.raises(Exception, match="1 to 4"):
        await _call(app, "benchmark_index_candidate", {**base, "indexes": [BADGES] * 5})
    with pytest.raises(Exception, match="fill_factor"):
        await _call(
            app,
            "benchmark_index_candidate",
            {**base, "indexes": [{**BADGES, "fill_factor": 70}]},
        )
    assert sandbox.create_calls == []

    result = await _call(
        app, "benchmark_index_candidate", {**base, "indexes": [BADGES, USERS]}
    )

    assert result["classification"] == "improved"
    assert [item["index_name"] for item in result["indexes"]] == [badges, users]
    assert result["indexes"][0]["index_depth"] == 4


@pytest.mark.asyncio
async def test_the_rewrite_benchmark_tool_passes_slow_baseline_cases(
    server_config_factory,
) -> None:
    app = _app(server_config_factory)
    session_id, candidate_id = _session(app, OR_SQL, parameter_cases=OR_CASES)
    app.performance_workflows.benchmark_candidate = AsyncMock(  # type: ignore[method-assign]
        return_value={"classification": "improved", "parameter_results": []}
    )

    await _call(
        app,
        "benchmark_tuning_candidate",
        {
            "session_id": session_id,
            "candidate_id": candidate_id,
            "baseline_sql": OR_SQL,
            "candidate_sql": OR_SQL,
            "slow_baseline_cases": {"scan": 150},
        },
    )

    kwargs = app.performance_workflows.benchmark_candidate.await_args.kwargs
    assert kwargs["slow_baseline_cases"] == {"scan": 150}


BRANCHING_SQL = (
    "IF @u IS NULL SELECT p.Id FROM dbo.Posts AS p ORDER BY p.Id "
    "ELSE SELECT p.Id FROM dbo.Posts AS p WHERE p.LastEditorUserId = @u ORDER BY p.Id"
)


@pytest.mark.asyncio
async def test_an_index_is_measured_for_a_branching_batch(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # T2: the fix was branches plus indexes. The index tool took one SELECT
    # only, so the indexes of the branches were measured with raw DDL.
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    cases = [{"name": "editor", "values": {"u": 5}, "types": {"u": "int"}, "weight": 1.0}]
    session_id, candidate_id = _session(app, BRANCHING_SQL, parameter_cases=cases)
    [name] = _names(TEST_INDEX_PREFIX, EDITOR)
    sent: list[str] = []

    async def profile(_database: str, contract: Any, **_kwargs: Any) -> Any:
        sent.append(contract.sql_text)
        return _plan(name, elapsed_ms=2) if sandbox.indexes else _profile(900)

    app.plans.profile_parameterized_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=profile
    )

    result = await _benchmark(
        app,
        session_id,
        candidate_id,
        sql=BRANCHING_SQL,
        parameter_cases=cases,
        indexes=[EDITOR],
    )

    assert result["classification"] in {"improved", "promising"}
    assert all(sql.split("*/", 1)[-1].strip() == BRANCHING_SQL for sql in sent)
    assert sandbox.indexes == {}


@pytest.mark.asyncio
async def test_a_set_that_repeats_an_existing_index_is_refused_before_any_ddl(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Building a copy of an index the table has measures nothing and doubles
    # its write cost for the length of the test.
    app = _app(server_config_factory)
    sandbox = _Sandbox(app, monkeypatch)
    sandbox.indexes["IX_Badges_Name_UserId"] = ExistingIndex(
        schema="dbo",
        table="Badges",
        index_id=5,
        name="IX_Badges_Name_UserId",
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key("Name"), parse_candidate_key("UserId")),
    )
    session_id, candidate_id = _session(app)
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    result = await _benchmark(app, session_id, candidate_id, indexes=[BADGES, USERS])

    assert result["classification"] == "inconclusive"
    assert result["failure_code"] == "index_set_partly_covered"
    assert "dbo.Badges (Name, UserId)" in result["reason"]
    assert sandbox.create_calls == []
    assert _open_leases(app) == []
    app.plans.profile_query.assert_not_awaited()

    # The same columns under ROW compression are another index: that test runs.
    second = app.performance_workflows.add_candidate(
        session_id, SQL, strategy="index", idempotency_key="compressed"
    )
    [compressed] = _names(TEST_INDEX_PREFIX, {**BADGES, "data_compression": "ROW"})
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(4000) for _ in range(3)],
            *[_plan(compressed) for _ in range(3)],
            *[_profile(4000) for _ in range(3)],
        ]
    )

    measured = await _benchmark(
        app,
        session_id,
        second["candidate_id"],
        key="compressed-key",
        indexes=[{**BADGES, "data_compression": "ROW"}],
    )

    assert measured["classification"] == "improved"
    assert measured["indexes"][0]["index_depth"] == 3
    assert list(sandbox.indexes) == ["IX_Badges_Name_UserId"]
