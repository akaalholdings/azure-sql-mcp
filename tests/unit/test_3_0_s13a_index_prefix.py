"""S13a F4: test index names. A call never fails because of the name.

An engagement tags its test indexes per task (IX_bench_D04_). The policy lists
the family (IX_bench_). A prefix that extends the family is used; any other
prefix falls back to the policy prefix with a note. The generated name says
what the index is (table and key columns), so a DBA who meets it in
sys.indexes can tell, and it always starts with an allowed prefix: that is
what the drop guard and lease cleanup check before they touch an index.
"""

from __future__ import annotations

import random
import re
import string
from dataclasses import replace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.database_policy import MAX_CALLER_TEST_INDEX_PREFIX_LENGTH
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.database_policy import extends_test_index_prefix
from azure_sql_mcp.index_advisor import build_index_advice
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import existing_index_covers_candidate
from azure_sql_mcp.index_metadata import parse_candidate_key
from azure_sql_mcp.index_optimizer import IndexCandidate
from azure_sql_mcp.index_metadata import is_test_index_name
from azure_sql_mcp.server import MAX_GENERATED_TEST_INDEX_NAME_LENGTH
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.server import _is_generated_test_index_name
from azure_sql_mcp.server import generated_test_index_name
from tests.unit.test_index_advisor import _index as _advisor_index
from tests.unit.test_index_advisor import _inputs as _advisor_inputs
from tests.unit.test_index_advisor import _orders_indexes
from tests.unit.test_index_benchmark_workflow import _app
from tests.unit.test_index_benchmark_workflow import _candidate
from tests.unit.test_index_benchmark_workflow import _profile

SQL = "SELECT id FROM dbo.Items"
ALLOWED = ("IX_Testing_", "IX_bench_")


def _bench_policy(**fields: Any) -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "bench",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "allow_test_indexes": True,
                    "max_benchmark_executions": 80,
                    "test_index_name_prefixes": ["IX_bench_"],
                    **fields,
                }
            },
        }
    )


def _bench_app(server_config_factory, **fields: Any) -> AzureSqlMcpApplication:
    app = _app(server_config_factory)
    policy = _bench_policy(**fields)
    app.database_policy = policy
    app.performance_workflows.database_policy = policy
    return app


def _index(name: str, key: str = "id") -> ExistingIndex:
    return ExistingIndex(
        schema="dbo",
        table="Items",
        index_id=2,
        name=name,
        index_type="NONCLUSTERED",
        key_columns=(parse_candidate_key(key),),
        include_columns=(),
        filter_definition=None,
        is_unique=False,
    )


async def _benchmark(app: AzureSqlMcpApplication, prefix: str | None) -> dict[str, Any]:
    session_id, candidate_id = _candidate(app, SQL)
    return await app._benchmark_index_candidate(
        "appdb", session_id, candidate_id, SQL, "dbo", "Items", ["id"], None, None, False,
        "screening", True, True, 30, "prefix-key", None, index_name_prefix=prefix,
    )


@pytest.mark.parametrize("prefix", ["IX_bench_", "IX_bench_D04_", "ix_BENCH_d04_", "IX_Testing_T1_"])
def test_a_prefix_that_is_or_extends_an_allowed_prefix_is_accepted(prefix: str) -> None:
    assert extends_test_index_prefix(prefix, ALLOWED) is True


@pytest.mark.parametrize(
    "prefix",
    [
        "IX_prod_",  # another family
        "IX_bench",  # shorter than the allowed prefix
        "PK_IX_bench_D04_",  # holds the allowed prefix, does not start with it
        "IX_bench_D04-",  # not letters, digits, underscore
        "IX_bench_D04 ",
        "IX_bench_]; DROP TABLE dbo.Items; --",
        "IX_bench_" + "x" * MAX_CALLER_TEST_INDEX_PREFIX_LENGTH,  # too long
        "",
    ],
)
def test_a_prefix_outside_the_allowed_families_does_not_extend_one(prefix: str) -> None:
    assert extends_test_index_prefix(prefix, ALLOWED) is False


def test_the_longest_prefix_keeps_the_generated_name_inside_the_engine_limit() -> None:
    longest = "IX_bench_" + "x" * (MAX_CALLER_TEST_INDEX_PREFIX_LENGTH - len("IX_bench_"))

    assert extends_test_index_prefix(longest, ALLOWED) is True
    assert len(longest) + 16 <= 128


def test_server_uses_an_extended_prefix_as_given(server_config_factory) -> None:
    app = _bench_app(server_config_factory)
    policy = app.database_policy.require("appdb")

    assert app._resolve_test_index_prefix(policy, None) == ("IX_Testing_", "")
    assert app._resolve_test_index_prefix(policy, "IX_bench_D04_") == ("IX_bench_D04_", "")


@pytest.mark.parametrize(
    ("requested", "tag"),
    [
        ("IX_prod_D04_", "IX_prod_D04"),  # another family: kept as a tag
        ("D04-x", "D04"),  # the leading identifier text only
        ("]; DROP TABLE dbo.Items; --", ""),  # nothing usable
        ("", ""),
        ("IX_bench_" + "x" * 80, "x" * 24),  # extends the family but is too long
    ],
)
def test_server_never_refuses_a_prefix_it_cannot_use(
    server_config_factory, requested: str, tag: str
) -> None:
    # Refusing cost a whole benchmark call for a cosmetic argument.
    app = _bench_app(server_config_factory)
    policy = app.database_policy.require("appdb")

    assert app._resolve_test_index_prefix(policy, requested) == ("IX_bench_", tag)


def test_generated_name_says_table_and_key_columns_and_is_one_per_definition() -> None:
    candidate = IndexCandidate(
        schema="dw",
        table="FactVote",
        key_columns=("PostKey", "[Vote Date] DESC"),
        include_columns=("VoteTypeKey",),
    )
    name = generated_test_index_name("IX_bench_D04_", candidate)

    assert name == f"IX_bench_D04_FactVote_PostKey_Vote_Date_{candidate.definition_fingerprint[:8]}"
    # The same definition gives the same name; another definition another name.
    assert generated_test_index_name("IX_bench_D04_", candidate) == name
    wider = IndexCandidate(
        schema="dw",
        table="FactVote",
        key_columns=("PostKey", "[Vote Date] DESC"),
        include_columns=("VoteTypeKey", "UserKey"),
    )
    assert generated_test_index_name("IX_bench_D04_", wider) != name
    assert generated_test_index_name("IX_bench_", candidate, "T7").startswith(
        "IX_bench_T7_FactVote_PostKey_"
    )


def test_generated_name_is_cut_to_the_length_limit_and_keeps_its_hash() -> None:
    candidate = IndexCandidate(
        schema="dbo",
        table="T" * 120,
        key_columns=tuple(f"Column{number}" for number in range(16)),
        include_columns=(),
    )
    name = generated_test_index_name("IX_bench_", candidate)

    assert len(name) == MAX_GENERATED_TEST_INDEX_NAME_LENGTH == 100
    assert name.endswith("_" + candidate.definition_fingerprint[:8])


def test_any_prefix_input_gives_a_safe_name_under_an_allowed_prefix(
    server_config_factory,
) -> None:
    # The invariant the drop guard relies on: whatever the caller sends, the
    # name starts with an allowed prefix, is a plain identifier and is short.
    app = _bench_app(server_config_factory)
    policy = app.database_policy.require("appdb")
    allowed = app._allowed_test_index_prefixes(policy)
    rng = random.Random(13)
    alphabet = string.ascii_letters + string.digits + "_-.;'\"[]() \t\n%*é漢"
    inputs = [
        None, "", " ", "_", "IX_bench_", "ix_BENCH_x_", "IX_Testing_", "IX_bench", "IX_",
        "IX_bench_" + "y" * 55, "IX_bench_" + "y" * 56, "x" * 500, "'; DROP INDEX x; --",
        *(
            rng.choice(["", "IX_bench_", "IX_Testing_", "ix_bench_", "PK_"])
            + "".join(rng.choice(alphabet) for _ in range(rng.randrange(0, 90)))
            for _ in range(2000)
        ),
    ]
    candidates = [
        IndexCandidate(schema="dbo", table="Items", key_columns=("id",), include_columns=()),
        IndexCandidate(
            schema="dw",
            table="Fact_" + "W" * 110,
            key_columns=("[Odd Name]]x] DESC", "B" * 100),
            include_columns=(),
        ),
        IndexCandidate(schema="dbo", table="_", key_columns=("_",), include_columns=()),
    ]

    for requested in inputs:
        prefix, tag = app._resolve_test_index_prefix(policy, requested)
        for candidate in candidates:
            name = generated_test_index_name(prefix, candidate, tag)
            assert name.casefold().startswith(tuple(item.casefold() for item in allowed)), requested
            assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name), requested
            assert len(name) <= MAX_GENERATED_TEST_INDEX_NAME_LENGTH, requested
            assert name.endswith("_" + candidate.definition_fingerprint[:8]), requested
            # The guards that drop, reconcile and report leftovers accept it.
            assert app._validate_test_index_name(name, allowed) == name
            assert _is_generated_test_index_name(name, allowed), requested


def test_names_of_the_new_and_the_old_scheme_are_both_recognised_as_test_indexes() -> None:
    # The leftover check and the drop guard must see both, or a lost run
    # leaves an index nothing will ever report or drop.
    for name in (
        "IX_bench_FactVote_PostKey_0123abcd",  # new scheme
        "IX_bench_D04_FactVote_PostKey_0123abcd",  # new scheme, extended prefix
        "IX_bench_0123456789abcdef",  # scheme before 3.0
        "IX_bench_D04_0123456789abcdef",
        "IX_Testing_0123456789abcdef",
        "IX_Testing_Items_id_0123abcd",
    ):
        assert _is_generated_test_index_name(name, ALLOWED), name
        assert AzureSqlMcpApplication._validate_test_index_name(name, ALLOWED) == name
    # An application index that shares the prefix is not a generated name.
    assert not _is_generated_test_index_name("IX_bench_Orders_Status", ALLOWED)
    assert not _is_generated_test_index_name("IX_prod_FactVote_PostKey_0123abcd", ALLOWED)
    assert not _is_generated_test_index_name("IX_prod_D04_0123456789abcdef", ALLOWED)
    with pytest.raises(ValueError, match="test prefix"):
        AzureSqlMcpApplication._validate_test_index_name("IX_prod_FactVote_PostKey_0123abcd", ALLOWED)


def test_advisers_hide_workflow_test_indexes_of_both_schemes() -> None:
    assert is_test_index_name("IX_Testing_0123456789abcdef")
    assert is_test_index_name("IX_Testing_Items_id_0123abcd")
    assert is_test_index_name("IX_Testing_D04_Items_id_0123abcd")


@pytest.mark.asyncio
async def test_benchmark_names_creates_and_drops_the_index_under_the_extended_prefix(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _bench_app(server_config_factory)
    name = generated_test_index_name(
        "IX_bench_D04_",
        IndexCandidate(schema="dbo", table="Items", key_columns=("id",), include_columns=()),
    )
    assert name.startswith("IX_bench_D04_Items_id_")
    built = _index(name)
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(side_effect=[[], [built], [built], []]),
    )
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[_profile(50, index_name=name) for _ in range(3)],
            *[_profile(100) for _ in range(3)],
        ]
    )

    result = await _benchmark(app, "IX_bench_D04_")

    # Every place that reports the index gives the full generated name.
    assert result["indexes"][0]["index_name"] == name
    assert result["lease"]["index_name"] == name
    assert f"[{name}]" in result["index_ddl"] and f"[{name}]" in result["rollback_ddl"]
    assert "index_name_note" not in result
    assert result["lease"]["status"] == "cleaned"
    assert app._create_test_index.await_args.args[3] == name
    assert app._drop_test_index.await_args.args[3] == name


@pytest.mark.asyncio
async def test_benchmark_with_an_unlisted_prefix_runs_under_the_policy_prefix_and_says_so(
    server_config_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _bench_app(server_config_factory)
    name = generated_test_index_name(
        "IX_bench_",
        IndexCandidate(schema="dbo", table="Items", key_columns=("id",), include_columns=()),
        "IX_prod_D04",
    )
    built = _index(name)
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes",
        AsyncMock(side_effect=[[], [built], [built], []]),
    )
    app._create_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app._drop_test_index = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            *[_profile(100) for _ in range(3)],
            *[_profile(50, index_name=name) for _ in range(3)],
            *[_profile(100) for _ in range(3)],
        ]
    )

    result = await _benchmark(app, "IX_prod_D04_")

    assert name.startswith("IX_bench_IX_prod_D04_Items_id_")
    assert result["indexes"][0]["index_name"] == name
    assert result["lease"]["status"] == "cleaned"
    note = result["index_name_note"]
    assert "not in test_index_name_prefixes" in note and name in note
    assert app._drop_test_index.await_args.args[3] == name


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "leftover_name",
    ["IX_bench_D03_0123456789abcdef", "IX_bench_D03_Items_status_0123abcd"],
)
async def test_leftover_of_the_old_or_the_new_scheme_still_stops_the_benchmark(
    server_config_factory, monkeypatch: pytest.MonkeyPatch, leftover_name: str
) -> None:
    app = _bench_app(server_config_factory)
    leftover = _index(leftover_name, key="status")
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes", AsyncMock(return_value=[leftover])
    )
    app._create_test_index = AsyncMock()  # type: ignore[method-assign]
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    result = await _benchmark(app, "IX_bench_D04_")

    assert result["failure_code"] == "leftover_test_index"
    assert result["leftover_test_indexes"] == [leftover_name]
    app._create_test_index.assert_not_awaited()
    app.plans.profile_query.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name", ["IX_bench_D04_0123456789abcdef", "IX_bench_D04_Items_id_0123abcd"]
)
async def test_guarded_drop_accepts_names_of_both_schemes_and_nothing_outside_the_prefixes(
    server_config_factory, name: str
) -> None:
    # Lease cleanup and expired-lease recovery both drop through this guard.
    app = _bench_app(server_config_factory)

    preview = await app._drop_test_index("appdb", "dbo", "Items", name, dry_run=True)

    assert f"DROP INDEX [{name}] ON [dbo].[Items]" in preview["sql_preview"]
    with pytest.raises(ValueError, match="test prefix"):
        await app._drop_test_index("appdb", "dbo", "Items", "IX_Items_Status", dry_run=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name", ["IX_bench_D04_0123456789abcdef", "IX_bench_D04_Items_id_0123abcd"]
)
async def test_expired_lease_of_either_naming_scheme_is_dropped_on_recovery(
    server_config_factory, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    # A lease written before the naming change must still be cleaned after it.
    app = _bench_app(server_config_factory)
    session_id, candidate_id = _candidate(app, SQL)
    case = app.performance_store.get_performance_case(
        app.tuning_sessions.get_session(session_id).performance_case_id
    )
    owner = "index-lease-0123456789abcdef0123456789abcdef"
    fingerprint = IndexCandidate(
        schema="dbo", table="Items", key_columns=("id",), include_columns=()
    ).definition_fingerprint
    lease = app.performance_store.create_index_lease(
        lease_id="lease-s13a",
        database_fingerprint=case.database_fingerprint or "",
        session_id=session_id,
        candidate_id=candidate_id,
        index_name=name,
        object_fingerprint=fingerprint,
        expires_at_utc="2000-01-01T00:00:00+00:00",
        metadata={
            "lease_owner_fence": owner,
            "target_schema": "dbo",
            "target_table": "Items",
            "create_dispatch_state": "dispatched",
            "fingerprint_provenance": "observed_existing_index",
            "observed_definition_fingerprint": _index(name).definition_fingerprint,
        },
        owner_reference=owner,
    )
    app.performance_store.update_index_lease(
        "lease-s13a", status="active", owner_reference=owner, expected_version=lease["version"]
    )
    monkeypatch.setattr(
        "azure_sql_mcp.server.collect_existing_indexes", AsyncMock(return_value=[_index(name)])
    )
    app.admin_policy.execute = AsyncMock(return_value={"status": "completed"})  # type: ignore[method-assign]

    summary = await app._cleanup_expired_index_leases()

    assert summary == {"examined": 1, "cleaned": 1, "cleanup_required": 0}
    dropped = app.admin_policy.execute.await_args.args[0]
    assert f"DROP INDEX [{name}]" in dropped.sql
    assert app.performance_store.list_open_index_leases() == []


# --- create_test_index and drop_test_index with an explicit name --------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("requested", "final"),
    [
        ("IX_Orders_Status", "IX_bench_IX_Orders_Status"),
        ("my index; DROP TABLE x", "IX_bench_my"),
        ("Orders-by-date", "IX_bench_Orders"),
        ("N" * 300, "IX_bench_" + "N" * 91),
    ],
)
async def test_preview_with_a_name_outside_the_prefixes_is_renamed_not_refused(
    server_config_factory, requested: str, final: str
) -> None:
    app = _bench_app(server_config_factory)

    preview = await app._create_test_index(
        "appdb", "dbo", "Orders", requested, ["Status"], None, True, dry_run=True
    )

    assert preview["index_name"] == final
    assert len(final) <= MAX_GENERATED_TEST_INDEX_NAME_LENGTH
    assert f"CREATE NONCLUSTERED INDEX [{final}] ON [dbo].[Orders]" in preview["sql_preview"]
    assert preview["rollback_sql"] == f"DROP INDEX [{final}] ON [dbo].[Orders];"
    assert "not under" in preview["index_name_note"] and final in preview["index_name_note"]


@pytest.mark.asyncio
async def test_preview_with_no_usable_name_gets_a_generated_one(server_config_factory) -> None:
    app = _bench_app(server_config_factory)

    preview = await app._create_test_index(
        "appdb", "dbo", "Orders", "]; --", ["Status"], None, True, dry_run=True
    )

    assert re.fullmatch(r"IX_bench_Orders_Status_[0-9a-f]{8}", preview["index_name"])
    assert preview["index_name"] in preview["index_name_note"]


@pytest.mark.asyncio
async def test_preview_with_a_name_under_an_allowed_prefix_is_kept(server_config_factory) -> None:
    app = _bench_app(server_config_factory)

    preview = await app._create_test_index(
        "appdb", "dbo", "Orders", "IX_bench_Orders_Status", ["Status"], None, True, dry_run=True
    )

    assert preview["index_name"] == "IX_bench_Orders_Status"
    assert "index_name_note" not in preview


@pytest.mark.asyncio
async def test_the_leased_build_never_renames_and_still_refuses_a_foreign_name(
    server_config_factory,
) -> None:
    # The workflow generates its names; a foreign name there is a defect, and
    # the lease must hold the name that was built.
    app = _bench_app(server_config_factory)
    app.admin_policy.execute = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="test prefix"):
        await app._create_test_index(
            "appdb", "dbo", "Orders", "IX_Orders_Status", ["Status"], None, True,
            dry_run=False, workflow_managed=True, idempotency_key="k",
            ownership_proof="index-lease-0123456789abcdef",
        )
    app.admin_policy.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_drop_refuses_a_name_outside_the_prefixes_and_says_which_it_accepts(
    server_config_factory,
) -> None:
    # This refusal is what stops the tool dropping an application index.
    app = _bench_app(server_config_factory)

    with pytest.raises(ValueError) as refusal:
        await app._drop_test_index("appdb", "dbo", "Orders", "IX_Orders_Status", dry_run=True)

    message = str(refusal.value)
    assert "test prefix" in message
    assert "IX_Testing_" in message and "IX_bench_" in message
    assert "drop" in message.lower()


# --- advisers hide test indexes under policy prefixes ---------------------------------


def test_policy_prefix_names_are_test_indexes_only_in_their_generated_form() -> None:
    policy = ("IX_bench_",)

    assert is_test_index_name("IX_bench_Items_id_0123abcd", policy)
    assert is_test_index_name("IX_bench_D04_Items_id_0123abcd", policy)
    assert is_test_index_name("IX_bench_0123456789abcdef", policy)
    # An application index may share the prefix: it stays part of the design.
    assert not is_test_index_name("IX_bench_OrdersByDate", policy)
    # Without the policy prefixes only the built-in names are known.
    assert not is_test_index_name("IX_bench_Items_id_0123abcd")
    assert is_test_index_name("IX_Testing_anything") and is_test_index_name("IX_Tuning_w1")


def test_a_leftover_under_a_policy_prefix_never_covers_a_candidate() -> None:
    leftover = _index("IX_bench_Items_id_0123abcd")
    candidate = {"schema": "dbo", "table": "Items", "key_columns": ["id"]}

    assert existing_index_covers_candidate(leftover, **candidate) is True
    assert (
        existing_index_covers_candidate(
            leftover, **candidate, test_index_name_prefixes=("IX_bench_",)
        )
        is False
    )


def test_adviser_reports_a_leftover_under_a_policy_prefix_and_keeps_it_out_of_the_design() -> None:
    # Counted as a real index, the leftover absorbed or hid a recommendation.
    orphan = _advisor_index(
        "Sales", "Orders", "IX_bench_Orders_CustomerID_0123abcd",
        ("CustomerID", "OrderDate"), index_id=9, reads=0,
    )
    inputs = _advisor_inputs([], [*_orders_indexes(), orphan])
    blind = build_index_advice(inputs)
    inputs.settings = replace(inputs.settings, test_index_name_prefixes=("IX_bench_",))
    aware = build_index_advice(inputs)

    def leftovers(report: dict[str, Any]) -> list[str]:
        return [
            rec["index_name"]
            for rec in report["recommendations"]
            if "leftover_test_index" in rec["reason_codes"]
        ]

    assert leftovers(blind) == []
    assert leftovers(aware) == ["IX_bench_Orders_CustomerID_0123abcd"]


@pytest.mark.asyncio
async def test_review_workload_indexes_passes_the_policy_prefixes_to_the_adviser(
    server_config_factory,
) -> None:
    app = _bench_app(server_config_factory)
    app.workload_index_advisor.review = AsyncMock(return_value={"recommendations": []})  # type: ignore[method-assign]

    await app.mcp._tool_manager.call_tool("review_workload_indexes", {"database_name": "appdb"})

    assert app.workload_index_advisor.review.await_args.kwargs["test_index_name_prefixes"] == (
        "IX_bench_",
    )
