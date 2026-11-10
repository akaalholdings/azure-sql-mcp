"""Static proof contract: which ties, hints and rewrite shapes a snapshot match cannot cover."""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.equivalence_contract import analyze_equivalence_preflight
from azure_sql_mcp.equivalence_contract import rewrite_requirements
from azure_sql_mcp.equivalence_contract import statement_hint_profile


def _detail(sql: str, risk: str) -> dict[str, Any]:
    preflight = analyze_equivalence_preflight(sql)
    return next(item for item in preflight.details if item.get("risk") == risk)


def test_ordered_top_names_the_table_and_columns_a_total_order_must_cover() -> None:
    detail = _detail(
        "SELECT TOP (10) p.Id, p.Score FROM dbo.Posts AS p ORDER BY p.Score DESC",
        "row_limit_total_order_unproven",
    )

    assert detail["total_order"] == {
        "provable": True,
        "tables": [
            {"schema": "dbo", "table": "Posts", "alias": "p", "order_columns": ["Score"]}
        ],
    }


def test_join_needs_a_key_from_every_joined_table() -> None:
    detail = _detail(
        "SELECT TOP (10) p.Id, c.Id FROM dbo.Posts AS p "
        "JOIN dbo.Comments AS c ON c.PostId = p.Id ORDER BY p.Id, c.Score",
        "row_limit_total_order_unproven",
    )

    assert [
        (table["table"], table["order_columns"]) for table in detail["total_order"]["tables"]
    ] == [("Posts", ["Id"]), ("Comments", ["Score"])]


def test_select_list_alias_and_ordinal_resolve_to_the_base_column() -> None:
    by_alias = _detail(
        "SELECT TOP (5) p.Id AS post_id FROM dbo.Posts AS p ORDER BY post_id",
        "row_limit_total_order_unproven",
    )
    by_ordinal = _detail(
        "SELECT TOP (5) p.Id FROM dbo.Posts AS p ORDER BY 1",
        "row_limit_total_order_unproven",
    )

    assert by_alias["total_order"]["tables"][0]["order_columns"] == ["Id"]
    assert by_ordinal["total_order"]["tables"][0]["order_columns"] == ["Id"]


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        (
            # Stream S12 proves a CTE or derived table that is a plain SELECT of
            # base columns; these two still have no key.
            "WITH recent AS (SELECT Id FROM dbo.Posts UNION ALL SELECT Id FROM dbo.Archive) "
            "SELECT TOP (5) Id FROM recent ORDER BY Id",
            "common table expression",
        ),
        (
            "SELECT TOP (5) d.Id FROM (SELECT * FROM dbo.Posts) AS d ORDER BY d.Id",
            "derived table",
        ),
        (
            "SELECT TOP (5) t.Id FROM #staged AS t ORDER BY t.Id",
            "temporary table",
        ),
        (
            "SELECT Id FROM dbo.Posts UNION ALL SELECT Id FROM dbo.Archive "
            "ORDER BY Id OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY",
            "set operation",
        ),
    ],
)
def test_sources_without_catalog_keys_are_named_as_unprovable(sql: str, reason: str) -> None:
    detail = _detail(sql, "row_limit_total_order_unproven")

    assert detail["total_order"]["provable"] is False
    assert reason in detail["total_order"]["unprovable_reason"]


def test_offset_without_fetch_is_a_row_limit() -> None:
    preflight = analyze_equivalence_preflight(
        "SELECT p.Id FROM dbo.Posts AS p ORDER BY p.Score OFFSET 20 ROWS"
    )

    assert preflight.risk_codes == ("row_limit_total_order_unproven",)


def test_top_inside_exists_does_not_change_the_result() -> None:
    preflight = analyze_equivalence_preflight(
        "SELECT u.Id FROM dbo.Users AS u WHERE EXISTS "
        "(SELECT TOP (1) 1 FROM dbo.Posts AS p WHERE p.OwnerUserId = u.Id)"
    )

    assert preflight.direct_snapshot_supported


def test_top_with_ties_returns_the_same_rows_whatever_the_tie_order() -> None:
    preflight = analyze_equivalence_preflight(
        "SELECT TOP (10) WITH TIES p.Id FROM dbo.Posts AS p ORDER BY p.Score DESC"
    )

    assert preflight.direct_snapshot_supported


def test_ordering_groups_by_their_grouping_columns_is_deterministic() -> None:
    by_group = analyze_equivalence_preflight(
        "SELECT TOP (5) p.OwnerUserId, COUNT(*) AS posts FROM dbo.Posts AS p "
        "GROUP BY p.OwnerUserId ORDER BY p.OwnerUserId"
    )
    # Two owners with the same count tie, so the fifth row is not defined.
    by_count = analyze_equivalence_preflight(
        "SELECT TOP (5) p.OwnerUserId, COUNT(*) AS posts FROM dbo.Posts AS p "
        "GROUP BY p.OwnerUserId ORDER BY COUNT(*) DESC"
    )

    assert by_group.direct_snapshot_supported
    assert by_count.risk_codes == ("row_limit_total_order_unproven",)


@pytest.mark.parametrize(
    "window",
    [
        "RANK() OVER (ORDER BY p.Score DESC)",
        "DENSE_RANK() OVER (PARTITION BY p.OwnerUserId ORDER BY p.Score)",
        "SUM(p.Score) OVER (ORDER BY p.CreationDate)",
        "COUNT(*) OVER (ORDER BY p.CreationDate RANGE UNBOUNDED PRECEDING)",
    ],
)
def test_windows_that_give_tied_rows_the_same_value_need_no_total_order(window: str) -> None:
    preflight = analyze_equivalence_preflight(f"SELECT p.Id, {window} AS v FROM dbo.Posts AS p")

    assert preflight.direct_snapshot_supported


@pytest.mark.parametrize(
    "window",
    [
        "ROW_NUMBER() OVER (ORDER BY p.Score DESC)",
        "NTILE(4) OVER (ORDER BY p.Score)",
        "LAG(p.Score) OVER (ORDER BY p.CreationDate)",
        "SUM(p.Score) OVER (ORDER BY p.CreationDate ROWS UNBOUNDED PRECEDING)",
    ],
)
def test_windows_that_split_tied_rows_still_need_a_total_order(window: str) -> None:
    preflight = analyze_equivalence_preflight(f"SELECT p.Id, {window} AS v FROM dbo.Posts AS p")

    assert preflight.risk_codes == ("window_order_total_order_unproven",)


def test_window_total_order_counts_partition_and_order_columns() -> None:
    detail = _detail(
        "SELECT ROW_NUMBER() OVER (PARTITION BY p.OwnerUserId ORDER BY p.Score DESC) AS rn "
        "FROM dbo.Posts AS p",
        "window_order_total_order_unproven",
    )

    assert detail["total_order"]["tables"][0]["order_columns"] == ["OwnerUserId", "Score"]


def test_dirty_read_hint_blocks_snapshot_proof() -> None:
    preflight = analyze_equivalence_preflight(
        "SELECT p.Id FROM dbo.Posts AS p WITH (NOLOCK) WHERE p.OwnerUserId = 7"
    )

    assert preflight.risk_codes == ("isolation_table_hint",)
    assert _detail(
        "SELECT p.Id FROM dbo.Posts AS p WITH (NOLOCK) WHERE p.OwnerUserId = 7",
        "isolation_table_hint",
    )["hints"] == ["NOLOCK"]


def test_hint_profile_separates_plan_forcing_isolation_and_compile_options() -> None:
    profile = statement_hint_profile(
        "SELECT p.Id FROM dbo.Posts AS p WITH (NOLOCK, INDEX(IX_Posts_Owner)) "
        "INNER LOOP JOIN dbo.Users AS u WITH (FORCESEEK) ON u.Id = p.OwnerUserId "
        "WHERE p.OwnerUserId = @owner "
        "OPTION (RECOMPILE, MAXDOP 1, OPTIMIZE FOR (@owner = 7), "
        "USE HINT('FORCE_LEGACY_CARDINALITY_ESTIMATION'))"
    )

    assert profile == {
        "hinted": True,
        "plan_forcing_hints": ["FORCESEEK", "INDEX", "LOOP JOIN", "MAXDOP", "USE HINT"],
        "isolation_table_hints": ["NOLOCK"],
        "compile_options": ["OPTIMIZE FOR", "RECOMPILE"],
    }


def test_hint_words_in_literals_comments_and_names_are_not_hints() -> None:
    profile = statement_hint_profile(
        "SELECT p.Id, 'WITH (NOLOCK) OPTION (MAXDOP 1)' AS note /* FORCESEEK */ "
        "FROM dbo.[Hash Join] AS p WHERE p.Title = N'LOOP JOIN' OPTION (RECOMPILE)"
    )

    assert profile["hinted"] is False
    assert profile["isolation_table_hints"] == []
    assert profile["compile_options"] == ["RECOMPILE"]


def _codes(baseline: str, candidate: str) -> list[str]:
    return [item["code"] for item in rewrite_requirements(baseline, candidate)]


def test_not_in_to_not_exists_requires_both_compared_columns_not_null() -> None:
    requirements = rewrite_requirements(
        "SELECT u.Id FROM dbo.Users AS u "
        "WHERE u.Id NOT IN (SELECT b.UserId FROM dbo.Badges AS b)",
        "SELECT u.Id FROM dbo.Users AS u "
        "WHERE NOT EXISTS (SELECT 1 FROM dbo.Badges AS b WHERE b.UserId = u.Id)",
    )

    assert [item["code"] for item in requirements] == ["not_in_null_semantics"]
    assert requirements[0]["resolved"] is True
    assert requirements[0]["preconditions"] == [
        {"kind": "not_null", "schema_name": "dbo", "table_name": "Badges", "columns": ["UserId"]},
        {"kind": "not_null", "schema_name": "dbo", "table_name": "Users", "columns": ["Id"]},
    ]
    assert "NULL" in requirements[0]["reason"]


def test_count_column_versus_count_star_requires_the_column_not_null() -> None:
    requirements = rewrite_requirements(
        "SELECT COUNT(p.LastEditorUserId) FROM dbo.Posts AS p WHERE p.OwnerUserId = 7",
        "SELECT COUNT(*) FROM dbo.Posts AS p WHERE p.OwnerUserId = 7",
    )

    assert [item["code"] for item in requirements] == ["count_null_semantics"]
    assert requirements[0]["preconditions"] == [
        {
            "kind": "not_null",
            "schema_name": "dbo",
            "table_name": "Posts",
            "columns": ["LastEditorUserId"],
        }
    ]


def test_removed_distinct_requires_a_declared_unique_key() -> None:
    requirements = rewrite_requirements(
        "SELECT DISTINCT u.Id FROM dbo.Users AS u JOIN dbo.Badges AS b ON b.UserId = u.Id",
        "SELECT u.Id FROM dbo.Users AS u JOIN dbo.Badges AS b ON b.UserId = u.Id",
    )

    assert [item["code"] for item in requirements] == ["distinct_removed"]
    assert requirements[0]["resolved"] is False
    assert requirements[0]["required_kinds"] == ["unique"]
    assert "duplicate" in requirements[0]["reason"]


def test_exists_replaced_by_join_requires_a_declared_unique_key() -> None:
    assert _codes(
        "SELECT u.Id FROM dbo.Users AS u WHERE EXISTS "
        "(SELECT 1 FROM dbo.Badges AS b WHERE b.UserId = u.Id)",
        "SELECT u.Id FROM dbo.Users AS u JOIN dbo.Badges AS b ON b.UserId = u.Id",
    ) == ["exists_join_swap"]


def test_outer_join_turned_inner_requires_a_trusted_foreign_key_and_not_null() -> None:
    requirements = rewrite_requirements(
        "SELECT p.Id FROM dbo.Posts AS p LEFT JOIN dbo.Users AS u ON u.Id = p.OwnerUserId",
        "SELECT p.Id FROM dbo.Posts AS p JOIN dbo.Users AS u ON u.Id = p.OwnerUserId",
    )

    assert [item["code"] for item in requirements] == ["outer_join_changed"]
    assert requirements[0]["required_kinds"] == ["not_null", "trusted_foreign_key"]


def test_a_rewrite_that_keeps_the_logical_shape_has_no_requirement() -> None:
    assert _codes(
        "SELECT p.Id FROM dbo.Posts AS p WHERE YEAR(p.CreationDate) = 2024",
        "SELECT p.Id FROM dbo.Posts AS p "
        "WHERE p.CreationDate >= '20240101' AND p.CreationDate < '20250101'",
    ) == []


def test_optimize_for_unknown_is_a_compile_option_not_a_plan_forcing_hint() -> None:
    profile = statement_hint_profile(
        "SELECT p.Id FROM dbo.Posts AS p WHERE p.OwnerUserId = @owner "
        "OPTION (OPTIMIZE FOR UNKNOWN)"
    )

    assert profile["hinted"] is False
    assert profile["compile_options"] == ["OPTIMIZE FOR"]


def test_dirty_read_hint_in_the_old_syntax_without_with_is_still_found() -> None:
    sql = "SELECT p.Id FROM dbo.Posts p (NOLOCK) WHERE p.OwnerUserId = 7"

    assert statement_hint_profile(sql)["isolation_table_hints"] == ["NOLOCK"]
    assert "isolation_table_hint" in analyze_equivalence_preflight(sql).risk_codes


@pytest.mark.parametrize(
    "option",
    ["OPTIMIZE FOR (@owner = 7)", "USE HINT('DISABLE_PARAMETER_SNIFFING')"],
)
def test_a_query_hint_the_parser_does_not_know_does_not_stop_the_analysis(option: str) -> None:
    """A candidate with OPTIMIZE FOR could not be benchmarked at all: preflight raised."""

    plain = f"SELECT p.Id FROM dbo.Posts AS p WHERE p.OwnerUserId = @owner OPTION ({option})"
    limited = (
        "SELECT TOP (5) p.Id FROM dbo.Posts AS p "
        f"WHERE p.OwnerUserId = @owner OPTION ({option})"
    )

    assert analyze_equivalence_preflight(plain).direct_snapshot_supported
    # The hint is set aside for parsing only; the risks of the statement are still found.
    assert analyze_equivalence_preflight(limited).risk_codes == ("unordered_row_limit",)
    assert rewrite_requirements(plain, plain) == []


def test_sql_that_is_wrong_apart_from_its_hints_is_still_rejected() -> None:
    with pytest.raises(ValueError, match="could not be parsed"):
        analyze_equivalence_preflight("SELECT FROM WHERE OPTION (OPTIMIZE FOR (@p = 1))")
