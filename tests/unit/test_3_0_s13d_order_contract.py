"""ORDER BY through an alias of a GROUP BY expression is the same ordering contract.

O01: the baseline ordered by the alias ``AwardDay`` of its one GROUP BY
expression and the candidate by the view column ``v.AwardDay`` that stores
that expression. The preflight reported ``result_order_ties`` for the
baseline and ``order_contract_changed`` for the pair, so the fastest design
(4 reads) left the workflow unproven.
"""

from __future__ import annotations

import pytest

from azure_sql_mcp.equivalence_contract import analyze_equivalence_preflight
from azure_sql_mcp.equivalence_contract import order_contract_risk
from azure_sql_mcp.performance_workflows import order_contract_removed
from azure_sql_mcp.performance_workflows import preflight_gap_codes

BASELINE = (
    "SELECT CONVERT(date, b.Date) AS AwardDay, COUNT(*) AS Awards "
    "FROM dbo.Badges AS b WHERE b.Name = @name AND MONTH(b.Date) = @m "
    "GROUP BY CONVERT(date, b.Date) ORDER BY AwardDay"
)
VIEW_BODY = (
    "SELECT b.Name, CONVERT(date, b.Date) AS AwardDay, COUNT_BIG(*) AS Awards "
    "FROM dbo.Badges AS b GROUP BY b.Name, CONVERT(date, b.Date)"
)
CANDIDATE = (
    "SELECT v.AwardDay, v.Awards FROM dw.v_bench_BadgeDay AS v WITH (NOEXPAND) "
    "WHERE v.Name = @name AND MONTH(v.AwardDay) = @m ORDER BY v.AwardDay"
)
BRANCHING_CANDIDATE = (
    "IF @m BETWEEN 1 AND 12\n"
    "    SELECT v.AwardDay, v.Awards FROM dw.v_bench_BadgeDay AS v WITH (NOEXPAND) "
    "WHERE v.Name = @name AND MONTH(v.AwardDay) = @m ORDER BY v.AwardDay;\n"
    "ELSE\n"
    "    SELECT v.AwardDay, v.Awards FROM dw.v_bench_BadgeDay AS v WITH (NOEXPAND) "
    "WHERE v.Name = @name AND 1 = 0 ORDER BY v.AwardDay;"
)


RELATION = ("dw", "v_bench_BadgeDay", VIEW_BODY)


def test_an_order_by_on_the_alias_of_the_only_group_by_expression_is_a_total_order() -> None:
    preflight = analyze_equivalence_preflight(BASELINE)

    # No entry: the order is total from the SQL alone, no catalog proof is needed.
    assert preflight.result_orders == ()


def test_an_order_by_on_the_group_by_expression_itself_is_a_total_order() -> None:
    preflight = analyze_equivalence_preflight(
        "SELECT CONVERT(date, b.Date) AS AwardDay, COUNT(*) AS Awards FROM dbo.Badges AS b "
        "GROUP BY CONVERT(date, b.Date) ORDER BY CONVERT(date, b.Date)"
    )

    assert preflight.result_orders == ()


def test_an_unqualified_order_by_column_covers_a_qualified_group_by_column_of_one_table() -> None:
    preflight = analyze_equivalence_preflight(
        "SELECT b.Name, COUNT(*) AS Awards FROM dbo.Badges AS b GROUP BY b.Name ORDER BY Name"
    )

    assert preflight.result_orders == ()


def test_an_order_by_that_misses_a_group_by_expression_still_has_ties() -> None:
    preflight = analyze_equivalence_preflight(
        "SELECT b.Name, CONVERT(date, b.Date) AS AwardDay, COUNT(*) AS Awards "
        "FROM dbo.Badges AS b GROUP BY b.Name, CONVERT(date, b.Date) ORDER BY AwardDay"
    )

    assert [item["risk"] for item in preflight.result_orders] == ["result_order_ties"]
    assert "GROUP BY" in preflight.result_orders[0]["total_order"]["unprovable_reason"]


def test_a_view_column_that_stores_the_group_by_expression_keeps_the_order_contract() -> None:
    assert order_contract_risk(BASELINE, CANDIDATE, candidate_relation=RELATION) is None
    # Without the definition of the view its column is only a name.
    assert order_contract_risk(BASELINE, CANDIDATE) is not None


def test_a_branching_batch_that_reads_the_view_keeps_the_order_contract() -> None:
    assert (
        order_contract_risk(BASELINE, BRANCHING_CANDIDATE, candidate_relation=RELATION) is None
    )


def test_a_view_column_of_another_expression_is_still_a_changed_contract() -> None:
    candidate = (
        "SELECT v.AwardDay, v.Awards FROM dw.v_bench_BadgeDay AS v WITH (NOEXPAND) "
        "WHERE v.Name = @name ORDER BY v.Awards"
    )

    risk = order_contract_risk(BASELINE, candidate, candidate_relation=RELATION)

    assert risk is not None and risk["change"] == "keys_differ"


@pytest.mark.parametrize(
    ("candidate", "change"),
    [
        (
            "IF @m > 0 SELECT b.Name FROM dbo.Badges AS b ORDER BY b.Id; "
            "ELSE SELECT b.Name FROM dbo.Badges AS b ORDER BY b.Id;",
            "keys_differ",
        ),
        (
            "IF @m > 0 SELECT b.Name FROM dbo.Badges AS b; "
            "ELSE SELECT b.Name FROM dbo.Badges AS b ORDER BY b.Name;",
            "removed",
        ),
    ],
)
def test_a_branching_candidate_says_whether_the_order_was_removed_or_only_differs(
    candidate: str, change: str
) -> None:
    """Both are compared; only a removed ORDER BY gets the caveat that no order is promised."""

    baseline = "SELECT b.Name FROM dbo.Badges AS b ORDER BY b.Name"

    risk = order_contract_risk(baseline, candidate)

    assert risk is not None and risk["change"] == change
    preflight = {
        "baseline": {"risk_codes": []},
        "candidate": {"risk_codes": [risk["risk"]], "details": [risk]},
    }
    _gaps, blocking = preflight_gap_codes(preflight)
    assert blocking == []
    assert order_contract_removed(preflight) is (change == "removed")
