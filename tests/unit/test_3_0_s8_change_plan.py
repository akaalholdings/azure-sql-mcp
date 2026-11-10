"""The client change plan: an ordered, inert plan built from one index review.

Create first, observe, remove last. The order matters to a client: a drop
that goes in before its replacement is proven is how an index change becomes
an incident.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.index_advisor import ForeignKeyInfo
from azure_sql_mcp.index_advisor import build_index_advice
from azure_sql_mcp.index_advisor import build_index_change_plan
from tests.unit.test_index_advisor import _index
from tests.unit.test_index_advisor import _inputs
from tests.unit.test_index_advisor import _orders_indexes
from tests.unit.test_index_advisor import _query

UNUSED = ("sales", "orders", "ix_orders_status")


def _report(**kwargs: Any) -> dict[str, Any]:
    existing = [
        *_orders_indexes(),
        _index("Sales", "OrderLines", "PK_Sales_OrderLines", ("OrderLineID",), index_id=1, type_code=1, primary_key=True),
        _index("Sales", "Orders", "IX_Orders_Status", ("Status",), index_id=5, reads=0, updates=90_000),
    ]
    queries = [
        _query(1, "seek_residual_lookup_sort.xml", cpu_us=900_000),
        _query(2, "clustered_scan_residuals.xml", cpu_us=2_000_000),
    ]
    return build_index_advice(_inputs(queries, existing, references={UNUSED: 0}, window_days=40, **kwargs))


def test_plan_creates_first_observes_then_removes_last() -> None:
    report = _report()
    plan = build_index_change_plan(report)

    assert plan["recommend_only"] is True
    phases = [step["phase"] for step in plan["steps"]]
    assert phases == ["create", "change", "observe", "remove"]
    assert [step["step"] for step in plan["steps"]] == [1, 2, 3, 4]
    create, change, observe, remove = plan["steps"]

    assert create["action"] == "create_index" and create["statement"].startswith("CREATE NONCLUSTERED INDEX")
    assert create["rollback"].startswith("DROP INDEX")
    assert change["action"] == "widen_index" and "DROP_EXISTING = ON" in change["statement"]

    # What to observe and for how long.
    assert observe["observe_days"] == 7
    assert observe["depends_on"] == [1, 2]
    assert observe["query_ids"] == [1, 2]
    assert "20%" in observe["rollback_trigger"]

    # The drop waits for the observation and carries its own evidence window.
    assert remove["action"] == "drop_index" and remove["statement"].endswith("DROP INDEX [IX_Orders_Status] ON [Sales].[Orders];")
    assert remove["depends_on"] == [3]
    assert "ONLINE = ON" in remove["rollback"]
    assert remove["evidence_window"] == {
        "usage_counter_days": 61.0,
        "plan_reference_days": 40.0,
        "required_days": 35,
    }
    assert remove["recheck_before_acting"] is True


def test_every_change_step_states_both_risks_and_its_evidence_window() -> None:
    plan = build_index_change_plan(_report())

    for step in plan["steps"]:
        if step["phase"] == "observe":
            continue
        assert step["recommendation_id"].startswith("R")
        assert step["risk_of_acting"] and all(isinstance(item, str) for item in step["risk_of_acting"])
        assert step["risk_of_not_acting"]
        assert step["evidence_window"]
    create = plan["steps"][0]
    assert create["evidence_window"]["query_store_days"] == 40.0
    assert "analysed workload" in create["risk_of_not_acting"]
    assert "90000 write" in plan["steps"][-1]["risk_of_not_acting"]


def test_removal_without_an_executable_statement_is_not_a_step() -> None:
    """A drop pinned by a hint has no DDL: the plan lists it with what must happen first."""

    report = _report(hint_references={UNUSED: ["module object_id 77"]})
    plan = build_index_change_plan(report)

    assert [step["phase"] for step in plan["steps"]] == ["create", "change", "observe"]
    (blocked,) = plan["not_planned"]
    assert blocked["index_name"] == "IX_Orders_Status"
    assert "index_named_by_hint_or_forced_plan" in blocked["blockers"]
    assert blocked["prerequisites"]


def test_removals_alone_need_no_observation_step_but_are_watched_after() -> None:
    existing = [*_orders_indexes(), _index("Sales", "Orders", "IX_Orders_Status", ("Status",), index_id=5, reads=0)]
    report = build_index_advice(_inputs([], existing, references={UNUSED: 0}, window_days=40))
    plan = build_index_change_plan(report, soak_days=14)

    (remove,) = plan["steps"]
    assert remove["phase"] == "remove" and remove["depends_on"] == []
    assert remove["observe_after_days"] == 14


def test_leads_and_reviews_are_questions_not_steps() -> None:
    report = _report(foreign_keys=[ForeignKeyInfo("FK_Orders_Salesperson", "Sales", "Orders", ("SalespersonPersonID",), "dbo", "People")])
    plan = build_index_change_plan(report)
    step_ids = {step.get("recommendation_id") for step in plan["steps"]}
    lead_ids = {item["recommendation_id"] for item in plan["open_questions"]}

    assert not step_ids & lead_ids
    assert lead_ids == {
        rec["id"] for rec in report["recommendations"] if rec["action"] == "review_index"
    }
    assert len(lead_ids) == 1
    assert "FK_Orders_Salesperson" in plan["open_questions"][0]["question"]
    assert plan["summary"]["steps"] == 4
    assert plan["summary"]["estimated_size_added_mb"] is not None


def test_soak_shorter_than_one_day_is_refused() -> None:
    with pytest.raises(ValueError, match="soak_days"):
        build_index_change_plan(_report(), soak_days=0)
