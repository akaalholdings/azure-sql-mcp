"""S13b-1 B: the engine of a leased indexed-view experiment.

A bare model with a plain SQL tool beat the product on warehouse tasks: it
created a schema-bound view, built the unique clustered index on it, read it
WITH (NOEXPAND) and measured the gain. The product could not build that as a
leased test object. These tests hold the rules that make it safe to do so:
the refusals come before any statement runs and name the exact rule, only a
view under a test prefix that carries this lease's owner marker is ever
dropped, a failed build leaves nothing and frees the lease, and every result
carries the write overhead warning.
"""

from __future__ import annotations

import random
import re
import string
from dataclasses import replace
from typing import Any

import pytest

from azure_sql_mcp.database_policy import DatabasePolicy
from azure_sql_mcp.index_metadata import MAX_GENERATED_TEST_INDEX_NAME_LENGTH
from azure_sql_mcp.index_metadata import ExistingIndex
from azure_sql_mcp.index_metadata import IndexKeyColumn
from azure_sql_mcp.index_metadata import TableColumnFacts
from azure_sql_mcp.index_metadata import is_test_index_name
from azure_sql_mcp.index_optimizer import IndexCandidate
from azure_sql_mcp.performance_store import LeaseConflictError
from azure_sql_mcp.performance_store import PerformanceStore
from azure_sql_mcp.server import TEST_INDEX_DEFINITION_PROPERTY
from azure_sql_mcp.server import TEST_INDEX_OWNER_PROPERTY
from azure_sql_mcp.server import generated_test_index_name
from azure_sql_mcp.view_experiments import INDEXED_VIEW_SET_OPTIONS
from azure_sql_mcp.view_experiments import SESSION_SET_OPTIONS_SQL
from azure_sql_mcp.view_experiments import TEST_VIEW_CATALOG_SQL
from azure_sql_mcp.view_experiments import VIEW_BASE_TABLE_RESTRICTIONS
from azure_sql_mcp.view_experiments import VIEW_DEFINITION_PROPERTY
from azure_sql_mcp.view_experiments import VIEW_EXPERIMENT_WRITE_COST_NOTE
from azure_sql_mcp.view_experiments import VIEW_EXPERIMENT_WRITE_OVERHEAD_WARNING
from azure_sql_mcp.view_experiments import VIEW_OWNER_PROPERTY
from azure_sql_mcp.view_experiments import ViewCatalogState
from azure_sql_mcp.view_experiments import ViewExperimentPlan
from azure_sql_mcp.view_experiments import ViewIndexSpec
from azure_sql_mcp.view_experiments import build_test_view_drop_batch
from azure_sql_mcp.view_experiments import build_view_experiment_create_batch
from azure_sql_mcp.view_experiments import is_test_view_name
from azure_sql_mcp.view_experiments import name_in_use_blocker
from azure_sql_mcp.view_experiments import plan_view_experiment
from azure_sql_mcp.view_experiments import read_view_catalog_state
from azure_sql_mcp.view_experiments import reconcile_view_experiment
from azure_sql_mcp.view_experiments import require_view_experiment_policy
from azure_sql_mcp.view_experiments import session_set_option_blockers
from azure_sql_mcp.view_experiments import validate_test_view_definition
from azure_sql_mcp.view_experiments import verify_view_experiment
from azure_sql_mcp.view_experiments import view_build_failure
from azure_sql_mcp.view_experiments import view_candidate_findings
from azure_sql_mcp.view_experiments import view_experiment_batch_params
from azure_sql_mcp.view_experiments import view_experiment_policy_blockers
from azure_sql_mcp.view_experiments import view_index_size
from azure_sql_mcp.view_experiments import view_name_prefixes
from azure_sql_mcp.view_workflows import validate_view_definition

BODY = (
    "SELECT p.OwnerUserId, CONVERT(date, p.CreationDate) AS CreationDay,\n"
    "       COUNT_BIG(*) AS PostCount, SUM(p.Score) AS TotalScore\n"
    "FROM dbo.Posts AS p\n"
    "WHERE p.PostTypeId = 1\n"
    "GROUP BY p.OwnerUserId, CONVERT(date, p.CreationDate)"
)
POSTS = {
    "dbo.Posts": [
        TableColumnFacts("OwnerUserId", "int", is_nullable=True),
        TableColumnFacts("CreationDate", "datetime", is_nullable=False),
        TableColumnFacts("PostTypeId", "int", is_nullable=False),
        TableColumnFacts("Score", "int", is_nullable=False),
        TableColumnFacts("ViewCount", "int", is_nullable=True),
    ]
}
OWNER = "lease-owner-0123456789abcdef"
OTHER_OWNER = "lease-owner-fedcba9876543210"


def _plan(definition: str = BODY, **arguments: Any) -> ViewExperimentPlan:
    arguments.setdefault("table_columns", POSTS)
    return plan_view_experiment(definition, **arguments)


def _codes(findings: Any, severity: str | None = None) -> list[str]:
    return [
        finding.code for finding in findings if severity is None or finding.severity == severity
    ]


# --- policy ---------------------------------------------------------------------


def _policy(**fields: Any) -> DatabasePolicy:
    values: dict[str, Any] = {
        "database_name": "appdb",
        "environment": "bench",
        "allow_read": True,
        "allow_test_indexes": True,
        "allow_view_apply": True,
        "allow_offline_test_index": True,
    }
    values.update(fields)
    return DatabasePolicy(**values)


def test_a_policy_with_the_three_permissions_allows_the_experiment() -> None:
    assert view_experiment_policy_blockers(_policy()) == ()
    require_view_experiment_policy(_policy())


@pytest.mark.parametrize(
    ("fields", "named"),
    [
        ({"allow_test_indexes": False}, "allow_test_indexes"),
        ({"allow_view_apply": False}, "allow_view_apply"),
        ({"allow_offline_test_index": False}, "allow_offline_test_index"),
        ({"environment": "production"}, "non-production"),
    ],
)
def test_each_missing_permission_refuses_and_is_named(fields: dict[str, Any], named: str) -> None:
    # CREATE VIEW, a leased index and an offline build are three separate consents.
    blockers = view_experiment_policy_blockers(_policy(**fields))

    assert len(blockers) == 1 and named in blockers[0]
    with pytest.raises(PermissionError, match="No query and no DDL was run"):
        require_view_experiment_policy(_policy(**fields))


def test_a_database_without_a_policy_entry_is_refused() -> None:
    assert view_experiment_policy_blockers(DatabasePolicy.denied("appdb")) == (
        "The database has no entry in the database policy.",
    )


# --- names ----------------------------------------------------------------------


def test_view_prefixes_are_the_twins_of_the_test_index_prefixes() -> None:
    assert view_name_prefixes(("IX_Testing_", "IX_bench_", "ix_lab_", "Tmp_idx_")) == (
        "v_Testing_",
        "v_bench_",
        "v_lab_",
        "v_Tmp_idx_",
    )


def test_generated_names_say_what_the_objects_are_and_repeat_for_one_definition() -> None:
    plan = _plan()
    again = _plan()

    assert re.fullmatch(r"v_Testing_Posts_[0-9a-f]{8}", plan.view_name)
    assert plan.clustered_index is not None
    assert plan.clustered_index.index_name == (
        f"IX_Testing_{plan.view_name}_OwnerUserId_CreationDay_"
        + plan.clustered_index.definition_fingerprint[:8]
    )
    assert (plan.view_name, plan.clustered_index.index_name) == (
        again.view_name,
        again.clustered_index.index_name if again.clustered_index else None,
    )
    # Another definition is another view name.
    assert _plan(BODY.replace("= 1", "= 2")).view_name != plan.view_name


def test_view_index_names_follow_the_rule_of_the_test_index_generator() -> None:
    plan = _plan(index_name_prefix="IX_bench_D08_", test_index_name_prefixes=("IX_bench_",))
    clustered = plan.clustered_index
    assert clustered is not None

    assert clustered.index_name == generated_test_index_name(
        "IX_bench_D08_", replace(clustered, index_name=None)
    )
    assert plan.view_name.startswith("v_bench_D08_")
    assert is_test_index_name(clustered.index_name, ("IX_bench_",))


def test_a_caller_name_under_a_test_view_prefix_is_used_as_given() -> None:
    plan = _plan(view_name="v_Testing_PostsPerDay")

    assert plan.view_name == "v_Testing_PostsPerDay"
    assert plan.notes == (
        "No clustered_index_key was given: the key is every GROUP BY column the view "
        "returns (OwnerUserId, CreationDay).",
    )


def test_a_caller_name_outside_the_prefixes_is_replaced_and_the_result_says_so() -> None:
    plan = _plan("CREATE VIEW rpt.vPostAgg WITH SCHEMABINDING AS\n" + BODY + ";")

    assert plan.valid
    assert plan.schema == "rpt"
    assert re.fullmatch(r"v_Testing_vPostAgg_Posts_[0-9a-f]{8}", plan.view_name)
    assert f"The view is created as rpt.{plan.view_name}" in plan.notes[0]
    assert "vPostAgg is not under a test view prefix (v_Testing_)" in plan.notes[0]


def test_an_index_prefix_outside_the_policy_is_refused() -> None:
    with pytest.raises(ValueError, match="not an allowed test index prefix"):
        _plan(index_name_prefix="IX_Sales_", test_index_name_prefixes=("IX_Testing_",))


def test_any_name_input_gives_names_the_engine_and_the_drop_guard_accept() -> None:
    # A call never fails on a name, and no generated name leaves the test prefixes.
    generator = random.Random(13)
    alphabet = string.ascii_letters + string.digits + "_ -.[]'\";%"
    allowed = ("IX_Testing_", "IX_bench_")
    for _ in range(400):
        requested = "".join(generator.choices(alphabet, k=generator.randint(0, 140)))
        tag = "".join(generator.choices(string.ascii_letters + "_", k=generator.randint(0, 24)))
        plan = _plan(
            view_name=requested,
            name_tag=tag.strip("_"),
            index_name_prefix=generator.choice(["IX_Testing_", "IX_bench_", "IX_bench_T7_"]),
            test_index_name_prefixes=allowed,
        )

        assert plan.valid, plan.findings
        assert is_test_view_name(plan.view_name, allowed)
        for name in (plan.view_name, *(index.index_name for index in plan.indexes)):
            assert name is not None
            assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name)
            assert len(name) <= MAX_GENERATED_TEST_INDEX_NAME_LENGTH
        build_view_experiment_create_batch(plan)
        build_test_view_drop_batch(plan.schema, plan.view_name, test_index_name_prefixes=allowed)


# --- static checks of the definition ----------------------------------------------


def test_a_legal_grouped_view_has_no_finding() -> None:
    checked = validate_test_view_definition(BODY, table_columns=POSTS)

    assert checked.findings == ()
    assert checked.output_columns == ("OwnerUserId", "CreationDay", "PostCount", "TotalScore")
    assert checked.group_by_columns == ("OwnerUserId", "CreationDay")
    assert checked.base_tables == ("dbo.Posts",)


@pytest.mark.parametrize(
    ("definition", "code"),
    [
        ("CREATE VIEW dbo.v AS " + BODY, "schemabinding_required"),
        ("ALTER VIEW dbo.v WITH SCHEMABINDING AS " + BODY, "create_view_only"),
        ("CREATE VIEW dbo.v WITH SCHEMABINDING, ENCRYPTION AS " + BODY, "view_attribute"),
        (BODY + "; DROP TABLE dbo.Posts", "not_a_single_select"),
        (BODY.replace("dbo.Posts", "Posts"), "two_part_names"),
        (BODY.replace("dbo.Posts", "otherdb.dbo.Posts"), "cross_database_name"),
        (BODY.replace("COUNT_BIG(*) AS PostCount, ", ""), "count_big_required"),
        (BODY.replace("COUNT_BIG(*)", "COUNT(*)"), "count_instead_of_count_big"),
        (
            "SELECT p.Id, u.Reputation FROM dbo.Posts AS p "
            "LEFT JOIN dbo.Users AS u ON u.Id = p.OwnerUserId",
            "outer_join",
        ),
        ("SELECT DISTINCT p.OwnerUserId FROM dbo.Posts AS p", "distinct"),
        (
            "SELECT p.Id FROM dbo.Posts AS p WHERE p.Id IN (SELECT v.PostId FROM dbo.Votes AS v)",
            "subquery",
        ),
        ("SELECT p.Id, GETDATE() AS AsOf FROM dbo.Posts AS p", "nondeterministic_function"),
        ("SELECT TOP (10) p.Id FROM dbo.Posts AS p", "top_or_offset"),
        (
            "SELECT p.Id FROM dbo.Posts AS p UNION ALL SELECT c.Id FROM dbo.Comments AS c",
            "set_operator",
        ),
        ("SELECT p.* FROM dbo.Posts AS p", "select_star"),
        ("SELECT p.OwnerUserId, MAX(p.Score) AS Best FROM dbo.Posts AS p GROUP BY p.OwnerUserId",
         "unsupported_aggregate"),
        ("SELECT p.Id, p.Score + 1 FROM dbo.Posts AS p", "unnamed_column"),
        ("SELECT p.Id FROM dbo.Posts AS p WHERE p.OwnerUserId = @UserId", "variable"),
        ("SELECT p.Id FROM dbo.Posts AS p WHERE p.Title LIKE '%?%'", "question_mark"),
        ("SELECT p.Id FROM dbo.Posts AS p INNER JOIN dbo.Posts AS q ON q.Id = p.ParentId",
         "self_join"),
    ],
)
def test_each_static_rule_refuses_with_its_own_finding(definition: str, code: str) -> None:
    checked = validate_test_view_definition(definition, table_columns=POSTS)

    assert code in _codes(checked.findings, "error"), checked.findings
    assert not checked.valid
    assert all(finding.check == "static-check" for finding in checked.findings)
    assert all(finding.message.endswith(".") for finding in checked.findings)


def test_a_refused_definition_gives_no_build_batch() -> None:
    plan = _plan("SELECT DISTINCT p.OwnerUserId FROM dbo.Posts AS p", clustered_index_key=["OwnerUserId"])

    assert not plan.valid
    assert plan.as_dict()["setup_statements"] == []
    with pytest.raises(ValueError, match="findings that refuse"):
        build_view_experiment_create_batch(plan)


def test_sum_over_a_not_null_column_is_accepted_as_written() -> None:
    # The engine accepts it; demanding ISNULL here made the tested view differ
    # from the one a DBA would write.
    assert validate_test_view_definition(BODY, table_columns=POSTS).valid
    assert "ISNULL" not in _plan().create_view_statement


def test_sum_over_a_nullable_column_is_refused_with_the_fix() -> None:
    checked = validate_test_view_definition(
        BODY.replace("SUM(p.Score)", "SUM(p.ViewCount)"), table_columns=POSTS
    )

    finding = next(f for f in checked.findings if f.code == "sum_over_nullable_expression")
    assert finding.severity == "error"
    assert "p.ViewCount is nullable" in finding.message
    assert "SUM(ISNULL(p.ViewCount, 0))" in finding.message and "8662" in finding.message


@pytest.mark.parametrize(
    ("expression", "state"),
    [
        ("SUM(ISNULL(p.ViewCount, 0))", None),
        ("SUM(p.Score * 2 + 1)", None),
        ("SUM(CONVERT(bigint, p.Score))", None),
        ("SUM(CASE WHEN p.PostTypeId = 1 THEN 1 ELSE 0 END)", None),
        ("SUM(CASE WHEN p.PostTypeId = 1 THEN p.Score END)", "sum_over_nullable_expression"),
        ("SUM(p.Score + p.ViewCount)", "sum_over_nullable_expression"),
        ("SUM(COALESCE(p.ViewCount, 0))", "sum_over_nullable_expression"),
        ("SUM(COALESCE(p.Score, 0))", None),
        ("SUM(TRY_CONVERT(int, p.Score))", "sum_nullability_unknown"),
        ("SUM(ISNULL(p.ViewCount, p.ViewCount))", "sum_nullability_unknown"),
    ],
)
def test_sum_nullability_is_decided_from_the_expression_and_the_catalog(
    expression: str, state: str | None
) -> None:
    checked = validate_test_view_definition(
        BODY.replace("SUM(p.Score)", expression), table_columns=POSTS
    )

    assert _codes(checked.findings) == ([state] if state else [])


def test_sum_over_coalesce_is_refused_and_the_text_gives_the_isnull_form() -> None:
    # Observed: the engine types COALESCE over a nullable column as nullable and
    # refuses the index with 8662; SUM(ISNULL(col, 0)) is accepted.
    checked = validate_test_view_definition(
        BODY.replace("SUM(p.Score)", "SUM(COALESCE(p.ViewCount, 0))"), table_columns=POSTS
    )
    without_catalog = validate_test_view_definition(
        BODY.replace("SUM(p.Score)", "SUM(COALESCE(p.ViewCount, 0))")
    )

    for result in (checked, without_catalog):
        finding = next(f for f in result.findings if f.code == "sum_over_nullable_expression")
        assert finding.severity == "error" and finding.check == "static-check"
        assert "SUM(ISNULL(p.ViewCount, 0))" in finding.message and "8662" in finding.message
        assert not result.valid


def test_count_big_of_a_column_is_accepted_beside_count_big_star() -> None:
    # Observed: accepted with COUNT_BIG(*) in the select list; refused with
    # 10138 without it.
    beside = BODY.replace("SUM(p.Score) AS TotalScore", "COUNT_BIG(p.ViewCount) AS Viewed")
    alone = beside.replace("COUNT_BIG(*) AS PostCount,", "")

    assert validate_test_view_definition(beside, table_columns=POSTS).findings == ()
    assert validate_view_definition(beside, indexed_view=True, schema_bound=True).valid
    refused = validate_test_view_definition(alone, table_columns=POSTS)
    finding = next(f for f in refused.findings if f.code == "count_big_star")
    assert finding.severity == "error" and "10138" in finding.message


def test_a_view_with_an_aggregate_and_no_group_by_can_be_indexed() -> None:
    # Observed: the engine builds a unique clustered index on the one stored row.
    plan = _plan(
        "SELECT COUNT_BIG(*) AS n, SUM(p.Score) AS s FROM dbo.Posts AS p",
        clustered_index_key=["n"],
    )

    assert plan.findings == ()
    assert plan.valid


def test_without_catalog_facts_a_plain_sum_is_a_warning_and_the_engine_decides() -> None:
    checked = validate_test_view_definition(BODY)

    assert _codes(checked.findings, "warning") == ["sum_nullability_unknown"]
    assert checked.valid


def test_a_function_the_parser_does_not_know_is_left_to_the_engine() -> None:
    checked = validate_test_view_definition(
        "SELECT p.Id, dbo.ScoreBand(p.Score) AS Band FROM dbo.Posts AS p", table_columns=POSTS
    )

    assert _codes(checked.findings) == ["function_determinism_unproven"]
    assert checked.valid


# --- the clustered index key ---------------------------------------------------------


def test_a_view_without_group_by_needs_a_key_from_the_caller() -> None:
    body = "SELECT p.Id, p.OwnerUserId, p.Score FROM dbo.Posts AS p WHERE p.PostTypeId = 1"

    assert _codes(_plan(body).findings, "error") == ["clustered_index_key_required"]
    keyed = _plan(body, clustered_index_key=["Id"])
    assert keyed.valid and keyed.clustered_index is not None
    assert keyed.clustered_index.key_columns == ("Id",)


@pytest.mark.parametrize(
    ("key", "code", "severity"),
    [
        (["NoSuchColumn"], "index_column_not_in_view", "error"),
        (["OwnerUserId", "PostCount"], "key_column_not_grouped", "error"),
        (["OwnerUserId"], "key_misses_group_by_column", "warning"),
        (["OwnerUserId; DROP"], "index_column_name", "error"),
    ],
)
def test_a_key_the_engine_would_refuse_or_fail_on_is_named_before_the_build(
    key: list[str], code: str, severity: str
) -> None:
    plan = _plan(clustered_index_key=key)

    assert code in _codes(plan.findings, severity)
    assert plan.valid is (severity == "warning")


def test_extra_indexes_must_name_view_columns_and_are_capped() -> None:
    plan = _plan(extra_indexes=[ViewIndexSpec(("CreationDay",), ("NoSuchColumn",))])
    assert _codes(plan.findings, "error") == ["index_column_not_in_view"]

    many = _plan(extra_indexes=[ViewIndexSpec((name,)) for name in ("CreationDay",) * 4])
    assert "too_many_indexes" in _codes(many.findings, "error")


# --- build batch ------------------------------------------------------------------------


def test_the_build_batch_sets_the_seven_options_then_builds_everything_in_one_transaction() -> None:
    plan = _plan(extra_indexes=[ViewIndexSpec(("CreationDay",), ("PostCount",))], maxdop=4)
    batch = build_view_experiment_create_batch(plan)
    lines = batch.splitlines()

    assert lines[0] == "DECLARE @options_before int = @@OPTIONS;"
    assert lines[1:8] == [f"SET {name} {value};" for name, value in INDEXED_VIEW_SET_OPTIONS]
    assert dict(INDEXED_VIEW_SET_OPTIONS) == {
        "ANSI_NULLS": "ON",
        "QUOTED_IDENTIFIER": "ON",
        "ANSI_PADDING": "ON",
        "ANSI_WARNINGS": "ON",
        "ARITHABORT": "ON",
        "CONCAT_NULL_YIELDS_NULL": "ON",
        "NUMERIC_ROUNDABORT": "OFF",
    }
    order = [
        batch.index("BEGIN TRANSACTION;"),
        batch.index(f"EXEC sys.sp_executesql N'CREATE VIEW [dbo].[{plan.view_name}] WITH SCHEMABINDING AS"),
        batch.index(f"@name = N'{VIEW_OWNER_PROPERTY}', @value = ?"),
        batch.index(f"@name = N'{VIEW_DEFINITION_PROPERTY}', @value = ?"),
        batch.index("EXEC sys.sp_executesql N'CREATE UNIQUE CLUSTERED INDEX"),
        batch.index("EXEC sys.sp_executesql N'CREATE NONCLUSTERED INDEX"),
        batch.index("COMMIT TRANSACTION;"),
        batch.index("IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;"),
        batch.index("THROW 50020, @build_error, 1;"),
    ]
    assert order == sorted(order)
    assert batch.count("BEGIN TRANSACTION") == 1 and batch.count("?") == 2
    assert "@level1type = N'VIEW'" in batch and "WITH (MAXDOP = 4)" in batch
    assert "ONLINE" not in batch
    create_params, drop_params = view_experiment_batch_params(plan, OWNER)
    assert create_params == (OWNER, plan.definition_fingerprint)
    assert drop_params == (OWNER, plan.definition_fingerprint, None)


RESTORE_ARITHABORT = "IF (@options_before & 64) = 0 SET ARITHABORT OFF;"
RESTORE_XACT_ABORT = "IF (@options_before & 16384) = 0 SET XACT_ABORT OFF;"


def test_the_build_batch_puts_back_the_session_options_it_changed_on_both_paths() -> None:
    # The pool does not reset a session. A measurement on a connection that
    # kept ARITHABORT ON would compile under another plan cache key.
    batch = build_view_experiment_create_batch(_plan())
    catch = batch[batch.index("BEGIN CATCH") : batch.index("END CATCH;")]
    after = batch[batch.index("END CATCH;") :]

    for part in (catch, after):
        assert RESTORE_ARITHABORT in part and RESTORE_XACT_ABORT in part
    assert catch.index(RESTORE_XACT_ABORT) < catch.index("THROW 50020")
    assert catch.index("ROLLBACK TRANSACTION;") < catch.index(RESTORE_ARITHABORT)


def test_the_drop_batch_puts_back_the_lock_timeout_and_xact_abort_on_both_paths() -> None:
    batch = build_test_view_drop_batch("dbo", "v_Testing_Posts_0a1b2c3d")
    catch = batch[batch.index("BEGIN CATCH") : batch.index("END CATCH;")]
    after = batch[batch.index("END CATCH;") :]

    assert batch.splitlines()[0] == "DECLARE @options_before int = @@OPTIONS;"
    for part in (catch, after):
        assert "SET LOCK_TIMEOUT -1;" in part and RESTORE_XACT_ABORT in part
    assert catch.index("SET LOCK_TIMEOUT -1;") < catch.index("THROW;")


def test_the_view_is_marked_with_the_property_names_of_a_test_index() -> None:
    assert (VIEW_OWNER_PROPERTY, VIEW_DEFINITION_PROPERTY) == (
        TEST_INDEX_OWNER_PROPERTY,
        TEST_INDEX_DEFINITION_PROPERTY,
    )


def test_a_quote_in_the_view_body_cannot_end_the_nested_batch() -> None:
    plan = _plan(BODY.replace("p.PostTypeId = 1", "p.Tags = 'it''s'"))
    batch = build_view_experiment_create_batch(plan)

    assert "p.Tags = ''it''''s''" in batch
    assert "p.Tags = 'it''s'" in plan.create_view_statement


def test_the_build_batch_refuses_a_name_outside_the_test_prefixes() -> None:
    plan = _plan()
    with pytest.raises(ValueError, match="starts with v_Testing_"):
        build_view_experiment_create_batch(replace(plan, view_name="vSalesSummary"))
    assert plan.clustered_index is not None
    foreign_index = replace(plan.clustered_index, index_name="PK_Sales")
    with pytest.raises(ValueError, match="not under an allowed test index prefix"):
        build_view_experiment_create_batch(replace(plan, clustered_index=foreign_index))


def test_an_owner_token_is_required_for_the_batch_parameters() -> None:
    with pytest.raises(ValueError, match="lease-owner token"):
        view_experiment_batch_params(_plan(), "short")


# --- guarded drop -------------------------------------------------------------------------


def test_the_drop_checks_the_owner_marker_in_the_catalog_before_it_drops() -> None:
    batch = build_test_view_drop_batch("dbo", "v_Testing_Posts_0a1b2c3d")

    order = [
        batch.index("BEGIN TRANSACTION;"),
        batch.index("OBJECT_ID(N'[dbo].[v_Testing_Posts_0a1b2c3d]', N'V')"),
        batch.index("THROW 50012"),
        batch.index(f"ep.name = N'{VIEW_OWNER_PROPERTY}'"),
        batch.index("THROW 50011, 'Test view ownership marker mismatch.', 1;"),
        batch.index(f"ep.name = N'{VIEW_DEFINITION_PROPERTY}'"),
        batch.index("THROW 50015"),
        batch.index("DROP VIEW [dbo].[v_Testing_Posts_0a1b2c3d];"),
        batch.index("COMMIT TRANSACTION;"),
    ]
    assert order == sorted(order)
    assert batch.count("?") == 3 and batch.count("DROP ") == 1
    assert "WITH (UPDLOCK, HOLDLOCK)" in batch and "ep.class = 1" in batch
    assert "SET LOCK_TIMEOUT 5000;" in batch
    assert "ALTER" not in batch


@pytest.mark.parametrize(
    ("schema", "view"),
    [
        ("dbo", "vSalesSummary"),
        ("dbo", "v_Testing_"),
        ("dbo", "v_Testing_x]; DROP TABLE dbo.Posts; --"),
        ("dbo]; DROP", "v_Testing_Posts_0a1b2c3d"),
        ("dbo", "v_bench_Posts_0a1b2c3d"),
    ],
)
def test_the_drop_is_refused_for_any_name_outside_the_test_view_prefixes(
    schema: str, view: str
) -> None:
    with pytest.raises(ValueError):
        build_test_view_drop_batch(schema, view)


def test_a_policy_prefix_extends_the_names_the_drop_accepts() -> None:
    batch = build_test_view_drop_batch(
        "dbo", "v_bench_Posts_0a1b2c3d", test_index_name_prefixes=("IX_Testing_", "IX_bench_")
    )

    assert "DROP VIEW [dbo].[v_bench_Posts_0a1b2c3d];" in batch


# --- the built objects against the request ---------------------------------------------------


def _built_index(candidate: IndexCandidate, index_id: int, **fields: Any) -> ExistingIndex:
    clustered = index_id == 1
    values: dict[str, Any] = {
        "schema": candidate.schema,
        "table": candidate.table,
        "index_id": index_id,
        "name": str(candidate.index_name),
        "index_type": "CLUSTERED" if clustered else "NONCLUSTERED",
        "key_columns": tuple(IndexKeyColumn(name) for name in candidate.key_columns),
        "include_columns": tuple(candidate.include_columns),
        "is_unique": candidate.is_unique,
        "data_space_name": "PRIMARY",
        "data_space_type": "ROWS_FILEGROUP",
        "partition_compression": ((1, "NONE"),),
        "partition_row_counts": ((1, 41_250),),
        "partition_page_counts": ((1, 256 if clustered else 64),),
    }
    values.update(fields)
    return ExistingIndex(**values)


def _built_state(plan: ViewExperimentPlan, **fields: Any) -> ViewCatalogState:
    values: dict[str, Any] = {
        "object_id": 1001,
        "object_type": "V",
        # The catalog keeps the module text as it was sent.
        "definition": plan.create_view_statement,
        "is_schema_bound": True,
        "uses_ansi_nulls": True,
        "uses_quoted_identifier": True,
        "owner_marker": OWNER,
        "definition_marker": plan.definition_fingerprint,
        "indexes": tuple(
            _built_index(index, position) for position, index in enumerate(plan.indexes, start=1)
        ),
    }
    values.update(fields)
    return ViewCatalogState(**values)


def test_the_built_view_and_indexes_match_the_plan() -> None:
    plan = _plan(extra_indexes=[ViewIndexSpec(("CreationDay",), ("PostCount",))])

    verification = verify_view_experiment(plan, _built_state(plan), owner_token=OWNER)

    assert verification.as_dict() == {"matches": True, "mismatches": []}


def test_each_difference_between_catalog_and_plan_is_named() -> None:
    plan = _plan(extra_indexes=[ViewIndexSpec(("CreationDay",), ("PostCount",))])
    clustered, extra = plan.indexes
    cases: dict[str, dict[str, Any]] = {
        "owner_marker": {"owner_marker": OTHER_OWNER},
        "definition_marker": {"definition_marker": "0" * 64},
        "view_definition": {"definition": plan.create_view_statement.replace("= 1", "= 2")},
        "schema_binding": {"is_schema_bound": False},
        "module_set_options": {"uses_quoted_identifier": False},
        f"index_missing:{extra.index_name}": {"indexes": (_built_index(clustered, 1),)},
        f"index_definition:{clustered.index_name}": {
            "indexes": (_built_index(clustered, 1, is_unique=False), _built_index(extra, 2))
        },
        "unexpected_index:IX_Other": {
            "indexes": (
                _built_index(clustered, 1),
                _built_index(extra, 2),
                _built_index(replace(extra, index_name="IX_Other"), 3),
            )
        },
    }
    for mismatch, fields in cases.items():
        verification = verify_view_experiment(
            plan, _built_state(plan, **fields), owner_token=OWNER
        )
        assert verification.mismatches == (mismatch,), mismatch

    missing = verify_view_experiment(plan, ViewCatalogState(), owner_token=OWNER)
    assert missing.mismatches == ("view_missing",)


def test_size_and_row_count_come_from_the_built_view_index() -> None:
    plan = _plan(extra_indexes=[ViewIndexSpec(("CreationDay",), ("PostCount",))])

    size = view_index_size(plan, _built_state(plan))

    assert size["rows"] == 41_250
    assert size["size_mb"] == 2.5  # (256 + 64) pages of 8 KB
    assert [index["size_mb"] for index in size["indexes"]] == [2.0, 0.5]
    # Write cost is out of scope: no data change is allowed. The result says so.
    assert size["write_cost"] == VIEW_EXPERIMENT_WRITE_COST_NOTE
    assert "not measured" in VIEW_EXPERIMENT_WRITE_COST_NOTE


def test_the_plan_result_carries_the_write_overhead_warning_and_exact_statements() -> None:
    plan = _plan()
    result = plan.as_dict()

    assert result["write_overhead_warning"] == VIEW_EXPERIMENT_WRITE_OVERHEAD_WARNING
    assert "every INSERT, UPDATE, DELETE and MERGE on its base tables" in result[
        "write_overhead_warning"
    ]
    assert "for the test window only" in result["write_overhead_warning"]
    # Writers wait for the build and readers do not: inferred from the locks, so "expected".
    assert "expected to hold a shared lock on each base table" in result["write_overhead_warning"]
    assert result["base_table_restrictions"] == VIEW_BASE_TABLE_RESTRICTIONS
    for engine_error in ("3729", "4922", "11401"):
        assert engine_error in VIEW_BASE_TABLE_RESTRICTIONS
    assert "blocks TRUNCATE TABLE and partition SWITCH loads" in VIEW_BASE_TABLE_RESTRICTIONS
    assert "maintained on every write" in VIEW_BASE_TABLE_RESTRICTIONS
    assert result["setup_statements"][1] == plan.create_view_statement
    assert result["setup_statements"][2].startswith("CREATE UNIQUE CLUSTERED INDEX [IX_Testing_")
    assert result["teardown_statements"] == [f"DROP VIEW [dbo].[{plan.view_name}];"]


# --- lease ------------------------------------------------------------------------------------


def test_a_view_lease_uses_the_test_index_lease_store_and_excludes_a_second_experiment() -> None:
    plan = _plan(extra_indexes=[ViewIndexSpec(("CreationDay",))])
    assert plan.clustered_index is not None and plan.clustered_index.index_name is not None
    metadata = plan.lease_metadata()
    # No target_table: test index cleanup cannot take this lease for an index lease.
    assert "target_table" not in metadata

    with PerformanceStore(db_path=":memory:") as store:
        lease = store.create_index_lease(
            lease_id="lease-view-1",
            database_fingerprint="db-fingerprint",
            session_id="session-1",
            candidate_id="candidate-1",
            index_name=plan.clustered_index.index_name,
            object_fingerprint=plan.definition_fingerprint,
            expires_at_utc="2026-11-10T12:00:00+00:00",
            metadata=metadata,
        )
        kept = lease["metadata"]
        assert kept["object_kind"] == "indexed_view"
        assert (kept["target_schema"], kept["target_view"]) == ("dbo", plan.view_name)
        assert kept["clustered_index_name"] == plan.clustered_index.index_name
        assert kept["marker_definition_fingerprint"] == plan.definition_fingerprint
        assert kept["extra_indexes"][0]["index_name"] == plan.extra_indexes[0].index_name
        assert lease["status"] == "pending_create"
        with pytest.raises(LeaseConflictError):
            store.create_index_lease(
                lease_id="lease-index-2",
                database_fingerprint="db-fingerprint",
                session_id="session-1",
                candidate_id="candidate-2",
                index_name="IX_Testing_Posts_Score_0a1b2c3d",
                object_fingerprint="f" * 64,
                expires_at_utc="2026-11-10T12:00:00+00:00",
            )


def _reconcile(step: str, state: ViewCatalogState | None, **arguments: Any) -> tuple[str, bool]:
    plan = _plan()
    outcome = reconcile_view_experiment(
        step=step,
        state=state(plan) if callable(state) else state,
        owner_token=OWNER,
        definition_fingerprint=plan.definition_fingerprint,
        **arguments,
    )
    return outcome.lease_status, outcome.drop_view


def test_a_failed_build_that_left_nothing_frees_the_lease() -> None:
    assert _reconcile("build", ViewCatalogState()) == ("create_failed", False)


def test_a_lost_connection_keeps_the_lease_even_when_the_catalog_shows_no_view() -> None:
    # The build batch may still be running on the server and can commit later.
    assert _reconcile("build", ViewCatalogState(), transport_failure=True) == (
        "cleanup_required",
        False,
    )


def test_an_unreadable_catalog_keeps_cleanup_required() -> None:
    assert _reconcile("build", None) == ("cleanup_required", False)
    assert _reconcile("drop", None) == ("cleanup_required", False)


def test_an_owned_view_left_in_the_catalog_is_dropped_under_the_lease() -> None:
    assert _reconcile("build", _built_state) == ("cleanup_required", True)  # type: ignore[arg-type]
    assert _reconcile("drop", _built_state) == ("cleanup_required", True)  # type: ignore[arg-type]


def test_a_view_of_that_name_that_the_lease_did_not_create_is_never_dropped() -> None:
    plan = _plan()
    foreign = _built_state(plan, owner_marker=None, definition_marker=None)
    other_lease = _built_state(plan, owner_marker=OTHER_OWNER)
    table = ViewCatalogState(object_id=7, object_type="U")

    for state in (foreign, other_lease, table):
        assert _reconcile("build", state) == ("create_failed", False)
        assert not state.owned_by(OWNER, plan.definition_fingerprint)
    assert "will not drop it" in str(name_in_use_blocker(plan, foreign))
    assert "its lease must clean it up first" in str(name_in_use_blocker(plan, other_lease))
    assert "not a view" in str(name_in_use_blocker(plan, table))
    assert name_in_use_blocker(plan, ViewCatalogState()) is None


def test_a_drop_is_proven_by_the_catalog() -> None:
    assert _reconcile("drop", ViewCatalogState()) == ("cleaned", False)


def test_reconcile_knows_two_steps_only() -> None:
    with pytest.raises(ValueError, match="build or drop"):
        _reconcile("verify", ViewCatalogState())


# --- build failure -------------------------------------------------------------------------------


class _DriverError(Exception):
    def __init__(self, text: str) -> None:
        super().__init__(text)
        self.ddbc_error = text


def test_an_engine_error_of_the_build_is_a_proven_rollback_with_the_engine_number() -> None:
    error = _DriverError(
        "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]The test view build was rolled "
        "back. Engine error 8662: Cannot create the clustered index on view because the view "
        "references an unknown value (SUM aggregate of nullable expression)."
    )

    assert view_build_failure(error) == {
        "outcome": "definite_rollback",
        "engine_error_number": 8662,
        "reason": "engine_error",
    }


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("statement timeout"),
        ConnectionError("connection reset"),
        _DriverError("Communication link failure"),
    ],
)
def test_a_transport_failure_of_the_build_is_an_unknown_outcome(error: Exception) -> None:
    failure = view_build_failure(error)

    assert failure["outcome"] == "unknown"
    assert failure["reason"] == "connection_or_timeout"


def test_an_error_the_batch_did_not_report_is_not_called_a_rollback() -> None:
    failure = view_build_failure(RuntimeError("driver failed before the batch ran"))

    assert failure == {
        "outcome": "unknown",
        "engine_error_number": None,
        "reason": "error_not_proven_to_roll_back",
    }


# --- the statement that reads the view --------------------------------------------------------------


def test_a_candidate_that_reads_the_view_with_noexpand_is_accepted() -> None:
    plan = _plan()
    sql = (
        f"SELECT v.CreationDay, v.PostCount FROM dbo.{plan.view_name} AS v WITH (NOEXPAND) "
        "WHERE v.OwnerUserId = @UserId"
    )

    assert view_candidate_findings(sql, plan) == ()


def test_candidate_mistakes_are_named_before_any_run() -> None:
    plan = _plan("CREATE VIEW dbo.vPostAgg WITH SCHEMABINDING AS " + BODY)
    view = plan.view_name
    cases = {
        f"SELECT v.PostCount FROM dbo.{view} AS v": "noexpand_missing",
        f"SELECT v.PostCount FROM {view} AS v WITH (NOEXPAND)": "view_name_not_two_part",
        "SELECT v.PostCount FROM dbo.vPostAgg AS v WITH (NOEXPAND)": "noexpand_on_other_object",
        "SELECT v.PostCount FROM dbo.vPostAgg AS v": "view_renamed",
        "SELECT p.Id FROM dbo.Posts AS p": "view_not_read",
    }
    for sql, code in cases.items():
        assert code in _codes(view_candidate_findings(sql, plan), "error"), sql

    renamed = view_candidate_findings("SELECT v.PostCount FROM dbo.vPostAgg AS v", plan)
    assert f"dbo.{view}" in renamed[0].message


def test_automatic_matching_runs_the_unchanged_statement() -> None:
    plan = _plan()
    unchanged = "SELECT p.OwnerUserId, COUNT_BIG(*) FROM dbo.Posts AS p GROUP BY p.OwnerUserId"

    assert view_candidate_findings(unchanged, plan, read_mode="automatic") == ()
    assert _codes(
        view_candidate_findings(
            f"SELECT v.PostCount FROM dbo.{plan.view_name} AS v", plan, read_mode="automatic"
        )
    ) == ["view_read_in_automatic_mode"]
    with pytest.raises(ValueError, match="read_mode"):
        view_candidate_findings(unchanged, plan, read_mode="expand")


def test_a_session_with_a_wrong_set_option_is_named() -> None:
    right = {name.lower(): 1 if value == "ON" else 0 for name, value in INDEXED_VIEW_SET_OPTIONS}

    assert session_set_option_blockers(right) == ()
    assert session_set_option_blockers(
        {**right, "quoted_identifier": 0, "numeric_roundabort": 1}
    ) == ("QUOTED_IDENTIFIER must be ON", "NUMERIC_ROUNDABORT must be OFF")
    assert session_set_option_blockers({**right, "ansi_nulls": None}) == ("ANSI_NULLS must be ON",)
    # An ODBC session starts with ARITHABORT OFF and ANSI_WARNINGS ON; the engine
    # then treats ARITHABORT as ON, so that session must not be refused.
    assert session_set_option_blockers({**right, "arithabort": 0}) == ()
    assert session_set_option_blockers({**right, "arithabort": 0, "ansi_warnings": 0}) == (
        "ANSI_WARNINGS must be ON",
        "ARITHABORT must be ON",
    )
    for name, _value in INDEXED_VIEW_SET_OPTIONS:
        assert f"SESSIONPROPERTY('{name}')) AS {name.lower()}" in SESSION_SET_OPTIONS_SQL


# --- the catalog read ------------------------------------------------------------------------------------


class _Executor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, list[Any]]] = []

    async def fetch_all(
        self, database_name: str, sql: str, params: list[Any] | None = None, **_: Any
    ) -> list[dict[str, Any]]:
        self.calls.append((sql, list(params or [])))
        return self.rows if sql == TEST_VIEW_CATALOG_SQL else []


@pytest.mark.asyncio
async def test_a_free_name_reads_as_no_object() -> None:
    executor = _Executor([])

    state = await read_view_catalog_state(
        executor, "appdb", schema_name="dbo", view_name="v_Testing_Posts_0a1b2c3d"  # type: ignore[arg-type]
    )

    assert not state.exists
    assert executor.calls == [
        (
            TEST_VIEW_CATALOG_SQL,
            [VIEW_OWNER_PROPERTY, VIEW_DEFINITION_PROPERTY, "dbo", "v_Testing_Posts_0a1b2c3d"],
        )
    ]


@pytest.mark.asyncio
async def test_a_table_of_that_name_is_reported_and_its_indexes_are_not_read() -> None:
    executor = _Executor([{"object_id": 7, "object_type": "U "}])

    state = await read_view_catalog_state(
        executor, "appdb", schema_name="dbo", view_name="v_Testing_Posts_0a1b2c3d"  # type: ignore[arg-type]
    )

    assert state.exists and state.object_type == "U"
    assert len(executor.calls) == 1


@pytest.mark.asyncio
async def test_a_view_row_carries_its_markers_and_module_options() -> None:
    executor = _Executor(
        [
            {
                "object_id": 1001,
                "object_type": "V ",
                "definition": "CREATE VIEW ...",
                "is_schema_bound": 1,
                "uses_ansi_nulls": True,
                "uses_quoted_identifier": 1,
                "owner_marker": OWNER,
                "definition_marker": "a" * 64,
            }
        ]
    )

    state = await read_view_catalog_state(
        executor, "appdb", schema_name="dbo", view_name="v_Testing_Posts_0a1b2c3d"  # type: ignore[arg-type]
    )

    assert state.owned_by(OWNER, "a" * 64)
    assert not state.owned_by(OTHER_OWNER, "a" * 64)
    assert not state.owned_by(OWNER, "b" * 64)
    assert (state.is_schema_bound, state.uses_ansi_nulls, state.uses_quoted_identifier) == (
        True,
        True,
        True,
    )
    # The index read of the view followed the object read.
    assert len(executor.calls) > 1
