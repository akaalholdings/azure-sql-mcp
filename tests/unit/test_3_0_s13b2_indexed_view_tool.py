"""benchmark_indexed_view_candidate: a leased indexed view measured against the original.

O01, D01 and D08: the bare model built a schema-bound view with a unique
clustered index and measured it; the product could only recommend one. The
engine is S13b-1's ``view_experiments``; these tests hold the tool layer: the
gates, the order of the steps, the lease on every path and the result.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.index_metadata import TABLE_COLUMN_FACTS_SQL
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.view_experiments import INDEXED_VIEW_SET_OPTIONS
from azure_sql_mcp.view_experiments import SESSION_SET_OPTIONS_SQL
from azure_sql_mcp.view_experiments import TEST_VIEW_CATALOG_SQL
from azure_sql_mcp.view_experiments import VIEW_EXPERIMENT_WRITE_OVERHEAD_WARNING
from azure_sql_mcp.view_experiments import ViewCatalogState
from azure_sql_mcp.view_experiments import plan_view_experiment
from tests.unit.test_3_0_s13b1_view_experiments import BODY
from tests.unit.test_3_0_s13b1_view_experiments import POSTS
from tests.unit.test_3_0_s13b1_view_experiments import _built_index
from tests.unit.test_index_benchmark_workflow import _profile

ORIGINAL = (
    "SELECT p.OwnerUserId, COUNT_BIG(*) AS PostCount FROM dbo.Posts AS p "
    "WHERE p.PostTypeId = 1 GROUP BY p.OwnerUserId"
)
VIEW = "v_Testing_PostAgg"


def _candidate_sql(view: str = VIEW) -> str:
    return (
        f"SELECT v.OwnerUserId, SUM(v.PostCount) AS PostCount FROM dbo.{view} AS v "
        "WITH (NOEXPAND) GROUP BY v.OwnerUserId"
    )


def _policy(**overrides: Any) -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "bench",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "allow_test_indexes": True,
                    "allow_view_apply": True,
                    "allow_offline_test_index": True,
                    "max_benchmark_executions": 80,
                    **overrides,
                }
            },
        }
    )


class _ViewSandbox:
    """A catalog in which the build batch creates the view and the drop batch removes it."""

    def __init__(self, app: AzureSqlMcpApplication, monkeypatch: pytest.MonkeyPatch) -> None:
        self.app = app
        self.plan = plan_view_experiment(BODY, view_name=VIEW, table_columns=POSTS)
        self.state = ViewCatalogState()
        self.events: list[str] = []
        self.actions: list[Any] = []
        self.build_error: BaseException | None = None
        app.executor.fetch_all = AsyncMock(side_effect=self._fetch)  # type: ignore[method-assign]
        app.admin_policy.execute = AsyncMock(side_effect=self._admin)  # type: ignore[method-assign]
        app.executor.execute_session_exactly_once = AsyncMock(side_effect=self._snapshot)  # type: ignore[method-assign]
        monkeypatch.setattr(
            "azure_sql_mcp.view_experiments.collect_existing_indexes", self._view_indexes
        )
        monkeypatch.setattr("azure_sql_mcp.server.collect_existing_indexes", self._view_indexes)

    async def _view_indexes(self, _executor: Any, _database: str, **_kwargs: Any) -> list[Any]:
        return list(self.state.indexes)

    async def _fetch(self, _database: str, sql: str, *, params: Any = (), **_kwargs: Any) -> list[dict[str, Any]]:
        if sql == TABLE_COLUMN_FACTS_SQL:
            return [
                {"column_name": column.name, "type_name": column.type_name,
                 "max_length": column.max_length, "is_nullable": column.is_nullable,
                 "is_computed": 0, "is_sparse": 0}
                for column in POSTS["dbo.Posts"]
            ]
        if sql == SESSION_SET_OPTIONS_SQL:
            return [{name.lower(): 1 if value == "ON" else 0 for name, value in INDEXED_VIEW_SET_OPTIONS}]
        if "sys.sql_modules" in sql and "object_type_code" in sql:
            return [
                {"schema_name": "dbo", "object_name": "Posts", "object_type_code": "U",
                 "object_type": "USER_TABLE", "definition": None, "is_encrypted": 0}
            ]
        if sql == TEST_VIEW_CATALOG_SQL:
            if not self.state.exists:
                return []
            return [
                {
                    "object_id": self.state.object_id,
                    "object_type": "V ",
                    "definition": self.state.definition,
                    "is_schema_bound": 1,
                    "uses_ansi_nulls": 1,
                    "uses_quoted_identifier": 1,
                    "owner_marker": self.state.owner_marker,
                    "definition_marker": self.state.definition_marker,
                }
            ]
        return [{"schema_name": "dbo", "table_name": "Posts"}]

    async def _admin(self, action: Any, _executor: Any, *, dry_run: bool, **_kwargs: Any) -> dict[str, Any]:
        assert dry_run is False
        self.actions.append(action)
        if "DROP VIEW" in action.sql and "CREATE VIEW" not in action.sql:
            self.events.append("drop_view")
            assert action.params[0] == self.state.owner_marker
            self.state = ViewCatalogState()
            return {"status": "completed"}
        self.events.append("build_view")
        if self.build_error is not None:
            raise self.build_error
        self.state = ViewCatalogState(
            object_id=1001,
            object_type="V",
            definition=self.plan.create_view_statement,
            is_schema_bound=True,
            uses_ansi_nulls=True,
            uses_quoted_identifier=True,
            owner_marker=str(action.params[0]),
            definition_marker=str(action.params[1]),
            indexes=tuple(
                _built_index(index, position)
                for position, index in enumerate(self.plan.indexes, start=1)
            ),
        )
        return {"status": "completed"}

    async def _snapshot(self, _database: str, _statements: list[str], **kwargs: Any) -> list[list[QueryResult]]:
        self.events.append("compare")
        hook = kwargs.get("before_statement_dispatch")
        for index in range(4):
            if hook is not None:
                hook(index)
        rows = [QueryResult(columns=("id",), rows=[{"id": 1}], column_type_signatures=("synthetic-int",))]
        return [[], [], rows, rows, [], []]


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


def _session(app: AzureSqlMcpApplication, candidate_sql: str) -> tuple[str, str]:
    case = app.performance_workflows.start_case("appdb", ORIGINAL)
    session = app.performance_workflows.start_session(case.case_id, "appdb")
    candidate = app.performance_workflows.add_candidate(
        session["session_id"], candidate_sql, strategy="rewrite_plus_index"
    )
    return session["session_id"], candidate["candidate_id"]


def _profiles(app: AzureSqlMcpApplication, sandbox: _ViewSandbox) -> list[str]:
    seen: list[str] = []
    clustered = str(sandbox.plan.clustered_index.index_name)

    async def profile(_database: str, sql: str, **_kwargs: Any) -> Any:
        reads_view = VIEW in sql
        seen.append("view" if reads_view else "original")
        return _profile(4 if reads_view else 900, index_name=clustered if reads_view else None)

    app.plans.profile_query = AsyncMock(side_effect=profile)  # type: ignore[method-assign]
    return seen


async def _run(app: AzureSqlMcpApplication, session_id: str, candidate_id: str, **options: Any) -> dict[str, Any]:
    arguments: dict[str, Any] = {
        "sql": ORIGINAL,
        "candidate_sql": _candidate_sql(),
        "view_name": VIEW,
        "phase": "finalist",
        "idempotency_key": "view-key",
    }
    arguments.update(options)
    return await app._benchmark_indexed_view_candidate(
        "appdb", session_id, candidate_id, BODY, **arguments
    )


@pytest.mark.asyncio
async def test_an_indexed_view_is_built_measured_compared_and_dropped(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _ViewSandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, _candidate_sql())
    seen = _profiles(app, sandbox)

    result = await _run(app, session_id, candidate_id)

    assert result["classification"] == "improved", result["reason"]
    # The original before and after, the view statement while the view existed.
    assert seen == ["original"] * 5 + ["view"] * 5 + ["original"] * 5
    assert sandbox.events == ["build_view", "compare", "drop_view"]
    assert result["executions"] == 15 + 2
    assert result["lease"]["status"] == "cleaned"
    assert app.performance_store.list_open_index_leases() == []
    assert result["proof_scope"] == "direct_snapshot"
    assert result["equivalence"][0]["status"] == "match"
    assert result["equivalence"][0]["plan_used_expected_index"] is True
    view = result["view"]
    assert view["view"] == f"dbo.{VIEW}"
    assert view["write_overhead_warning"] == VIEW_EXPERIMENT_WRITE_OVERHEAD_WARNING
    assert any("CREATE VIEW" in statement for statement in view["setup_statements"])
    assert view["teardown_statements"] == [f"DROP VIEW [dbo].[{VIEW}];"]
    assert result["built"]["rows"] == 41_250
    assert result["verification"] == {"matches": True, "mismatches": []}
    # The private owner token never leaves the server.
    owner = str(sandbox.actions[0].params[0])
    assert owner not in str(result)
    # Both admin actions carry the exact inverse statement, so the gate needs no exemption.
    build, drop = sandbox.actions
    assert build.tool_name == drop.tool_name == "benchmark_indexed_view_candidate"
    assert "DROP VIEW" in build.rollback_sql
    assert "CREATE VIEW" in drop.rollback_sql
    assert build.exactly_once and drop.exactly_once

    final = app.performance_workflows.finalize_session(
        session_id, selected_candidate_id=candidate_id, stopping_reason="measured"
    )
    assert final["selected_candidate_proof_scope"] == "snapshot_matched"


@pytest.mark.asyncio
async def test_preview_returns_the_plan_and_runs_nothing(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _ViewSandbox(app, monkeypatch)
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    result = await app._benchmark_indexed_view_candidate(
        "appdb", None, None, BODY, preview=True
    )

    assert result["preview"] is True
    assert result["view"].startswith("dbo.v_Testing_")
    assert result["valid"] is True and result["setup_statements"]
    assert sandbox.events == []
    app.plans.profile_query.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["allow_view_apply", "allow_offline_test_index", "allow_test_indexes"])
async def test_each_missing_policy_permission_refuses_before_any_statement(
    server_config_factory, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    app = _app(server_config_factory, _policy(**{field: False}))
    sandbox = _ViewSandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, _candidate_sql())
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(PermissionError):
        await _run(app, session_id, candidate_id)

    assert sandbox.events == []
    app.plans.profile_query.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_candidate_that_does_not_read_the_view_with_noexpand_is_refused_by_name(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _ViewSandbox(app, monkeypatch)
    without_hint = _candidate_sql().replace(" WITH (NOEXPAND)", "")
    session_id, candidate_id = _session(app, without_hint)
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="noexpand_missing"):
        await _run(app, session_id, candidate_id, candidate_sql=without_hint)

    assert sandbox.events == []
    app.plans.profile_query.assert_not_awaited()
    assert app.tuning_sessions.get_candidate(candidate_id).is_terminal is False


@pytest.mark.asyncio
async def test_a_build_the_engine_refuses_leaves_nothing_and_frees_the_lease(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _app(server_config_factory)
    sandbox = _ViewSandbox(app, monkeypatch)
    sandbox.build_error = RuntimeError(
        "[42000] [Microsoft][SQL Server]The test view build was rolled back. Engine error "
        "1940: Cannot create index on view because the underlying object has a different owner."
    )
    session_id, candidate_id = _session(app, _candidate_sql())
    _profiles(app, sandbox)

    result = await _run(app, session_id, candidate_id)

    assert result["classification"] == "inconclusive"
    assert sandbox.events == ["build_view"]
    assert result["lease"]["status"] == "create_failed"
    assert result["failure_diagnostic"]["failed_step"] == "build_view"
    assert result["failure_diagnostic"]["native_error_code"] == 1940
    assert "Cannot create index on view" in result["failure_diagnostic"]["engine_message"]
    assert app.performance_store.list_open_index_leases() == []
    assert result["session_continues"] is True


@pytest.mark.asyncio
async def test_an_expired_view_lease_is_cleaned_with_a_guarded_drop_and_its_inverse(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A process that died after the build must not leave the view on the base table."""

    app = _app(server_config_factory)
    sandbox = _ViewSandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, _candidate_sql())
    plan = sandbox.plan
    case = app.performance_store.get_performance_case(
        app.tuning_sessions.get_session(session_id).performance_case_id
    )
    owner = "index-lease-0123456789abcdef0123456789abcdef"
    app.performance_store.create_index_lease(
        lease_id="lease-view-expired",
        database_fingerprint=case.database_fingerprint or "",
        session_id=session_id,
        candidate_id=candidate_id,
        index_name=str(plan.clustered_index.index_name),
        object_fingerprint=plan.definition_fingerprint,
        expires_at_utc=(datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
        metadata={
            **plan.lease_metadata(),
            "lease_owner_fence": owner,
            "create_dispatch_state": "dispatched",
        },
        owner_reference=owner,
        request_fingerprint="expired-view-lease",
    )
    sandbox.state = ViewCatalogState(
        object_id=1001, object_type="V", definition=plan.create_view_statement,
        is_schema_bound=True, uses_ansi_nulls=True, uses_quoted_identifier=True,
        owner_marker=owner, definition_marker=plan.definition_fingerprint,
        indexes=tuple(_built_index(index, n) for n, index in enumerate(plan.indexes, start=1)),
    )
    monkeypatch.setattr(app, "_index_lease_has_live_session", lambda *_args: False)

    summary = await app._cleanup_expired_index_leases()

    assert summary["cleaned"] == 1 and summary["cleanup_required"] == 0
    assert sandbox.events == ["drop_view"]
    drop = sandbox.actions[0]
    assert drop.tool_name == "benchmark_indexed_view_candidate"
    assert drop.params == (owner, plan.definition_fingerprint, 1001)
    # The inverse is rebuilt from the catalog: the lease keeps no SQL text.
    assert "CREATE VIEW" in drop.rollback_sql and "CREATE UNIQUE CLUSTERED INDEX" in drop.rollback_sql
    assert app.performance_store.list_open_index_leases() == []


@pytest.mark.asyncio
async def test_capabilities_say_whether_a_view_experiment_is_allowed(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    allowed = _app(server_config_factory)
    refused = _app(server_config_factory, _policy(allow_view_apply=False))

    yes = allowed._local_view_experiment_capability(allowed.database_policy.policy_for("appdb"))
    no = refused._local_view_experiment_capability(refused.database_policy.policy_for("appdb"))

    assert yes == {
        "allowed": True,
        "designs_forbidden": False,
        "blockers": [],
        "test_view_name_prefixes": ["v_Testing_"],
    }
    assert no["allowed"] is False
    assert "allow_view_apply" in no["blockers"][0]


def test_the_tool_is_registered_for_the_sandbox_profile_only(server_config_factory) -> None:
    sandbox = _app(server_config_factory)
    tool = sandbox.mcp._tool_manager.get_tool("benchmark_indexed_view_candidate")

    assert tool is not None
    properties = tool.parameters["properties"]
    for name in ("view_definition", "candidate_sql", "clustered_index_key", "read_mode",
                 "preview", "runs", "parameter_cases", "idempotency_key"):
        assert name in properties, name
    assert properties["read_mode"]["enum"] == ["noexpand", "automatic"]


@pytest.mark.asyncio
async def test_a_build_that_lost_its_connection_keeps_the_lease_for_cleanup(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The build may still commit after the catalog was read: one look proves nothing."""

    app = _app(server_config_factory)
    sandbox = _ViewSandbox(app, monkeypatch)
    sandbox.build_error = ConnectionError("TCP Provider: connection was closed")
    session_id, candidate_id = _session(app, _candidate_sql())
    _profiles(app, sandbox)

    result = await _run(app, session_id, candidate_id)

    assert result["classification"] == "cleanup_required"
    assert result["lease"]["status"] == "cleanup_required"
    assert result["remediation"] == f"DROP VIEW [dbo].[{VIEW}];"
    assert result["session_continues"] is False
    assert sandbox.events == ["build_view"]
