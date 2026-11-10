"""Test-index DDL safety, the replacement experiment and the multi-query proof set.

The advisor recommends extend, widen and consolidate. A benchmark that only
adds an extra index cannot prove them: the old index is still there, so the
queries it serves never show a regression.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from azure_sql_mcp.index_optimizer import ExperimentAssessment
from azure_sql_mcp.index_optimizer import IndexCandidate
from azure_sql_mcp.index_optimizer import ProofQuery
from azure_sql_mcp.index_optimizer import ProofQueryResult
from azure_sql_mcp.index_optimizer import assess_proof_set
from azure_sql_mcp.index_optimizer import bounded_table_lock_probe_sql
from azure_sql_mcp.index_optimizer import build_index_candidate_statement
from azure_sql_mcp.index_optimizer import build_test_index_statement
from azure_sql_mcp.index_optimizer import classify_create_index_failure
from azure_sql_mcp.index_optimizer import hint_candidate_index
from azure_sql_mcp.index_optimizer import plan_proof_set
from azure_sql_mcp.index_optimizer import plan_replacement_experiment
from azure_sql_mcp.index_optimizer import proof_queries_from_recommendation
from azure_sql_mcp.connection import AdminBatchOutcomeUnknownError
from tests.unit.test_index_advisor import _index

CANDIDATE = IndexCandidate(
    schema="Sales",
    table="Orders",
    key_columns=("CustomerID", "OrderDate"),
    include_columns=("Status",),
    index_name="IX_Testing_0123456789abcdef",
)


# --- item 4: test-index DDL safety -----------------------------------------


def test_online_test_index_waits_at_low_priority_and_aborts_itself_with_policy_maxdop() -> None:
    """A build that waits for its final lock at normal priority queues every reader and writer behind it."""

    sql = build_test_index_statement(CANDIDATE, maxdop=2)

    assert (
        "WITH (ONLINE = ON (WAIT_AT_LOW_PRIORITY (MAX_DURATION = 1 MINUTES, "
        "ABORT_AFTER_WAIT = SELF)), MAXDOP = 2)"
    ) in sql
    assert sql.startswith("CREATE NONCLUSTERED INDEX [IX_Testing_0123456789abcdef] ON [Sales].[Orders]")


def test_maxdop_is_left_to_the_database_when_policy_sets_none() -> None:
    sql = build_test_index_statement(CANDIDATE)
    assert "WAIT_AT_LOW_PRIORITY" in sql and "MAXDOP" not in sql


@pytest.mark.parametrize("maxdop", [0, -1, 65])
def test_maxdop_outside_the_engine_range_is_refused(maxdop: int) -> None:
    with pytest.raises(ValueError, match="maxdop"):
        build_test_index_statement(CANDIDATE, maxdop=maxdop)


def test_offline_test_index_is_refused_unless_policy_allows_it() -> None:
    """An offline build blocks every writer of the table for the whole build."""

    with pytest.raises(PermissionError, match="offline"):
        build_test_index_statement(CANDIDATE, online=False)

    sql = build_test_index_statement(CANDIDATE, online=False, allow_offline=True, maxdop=2)
    assert "ONLINE" not in sql and "WITH (MAXDOP = 2)" in sql


def test_the_review_statement_builder_keeps_its_output() -> None:
    assert build_index_candidate_statement(CANDIDATE, online=True).endswith("INCLUDE ([Status]) WITH (ONLINE = ON);")


def _driver_error(sqlstate: str, text: str) -> Exception:
    return RuntimeError(sqlstate, f"[{sqlstate}] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]{text}")


@pytest.mark.parametrize(
    ("sqlstate", "text", "number", "reason"),
    [
        ("42000", "Lock request time out period exceeded. (1222) (SQLExecDirectW)", 1222, "lock_timeout_or_low_priority_abort"),
        ("42000", "Column name 'stauts' does not exist in the target table or view. (1911) (SQLExecDirectW)", 1911, "compile_error"),
        ("23000", "The CREATE UNIQUE INDEX statement terminated because a duplicate key was found. (1505) (SQLExecDirectW)", 1505, "duplicate_key"),
        ("42S02", "Invalid object name 'Sales.Ordrs'. (208) (SQLExecDirectW)", 208, "compile_error"),
    ],
)
def test_a_server_reported_create_failure_is_a_definite_rollback(
    sqlstate: str, text: str, number: int, reason: str
) -> None:
    """These errors mean the CREATE never committed: the lease must not go to cleanup_required."""

    error = AdminBatchOutcomeUnknownError("wrapped")
    error.__cause__ = _driver_error(sqlstate, text)

    verdict = classify_create_index_failure(error)

    assert verdict == {"outcome": "definite_rollback", "error_number": number, "reason": reason}


@pytest.mark.parametrize(
    "error",
    [
        _driver_error("08S01", "Communication link failure (10054) (SQLExecDirectW)"),
        _driver_error("HYT00", "Query timeout expired (0) (SQLExecDirectW)"),
        # A lock timeout reported together with a lost connection proves nothing.
        _driver_error("08S01", "Lock request time out period exceeded. (1222); TCP Provider: connection reset (10054)"),
        _driver_error("42000", "Some error this classifier does not list. (50000) (SQLExecDirectW)"),
        TimeoutError("statement deadline"),
    ],
)
def test_transport_loss_timeouts_and_unlisted_errors_stay_outcome_unknown(error: Exception) -> None:
    assert classify_create_index_failure(error)["outcome"] == "unknown"


def test_drop_side_lock_probe_is_bounded_by_a_short_lock_timeout() -> None:
    sql = bounded_table_lock_probe_sql("Sales", "Order]Lines")
    assert sql == (
        "SET LOCK_TIMEOUT 5000;\n"
        "SELECT TOP (1) @table_lock_probe = 1 FROM [Sales].[Order]]Lines] WITH (TABLOCK, HOLDLOCK);"
    )
    with pytest.raises(ValueError, match="lock_timeout_ms"):
        bounded_table_lock_probe_sql("Sales", "Orders", lock_timeout_ms=0)


# --- item 3: replacement experiment ----------------------------------------


def _old(name: str = "IX_Orders_CustomerID", **kwargs: Any):
    return _index("Sales", "Orders", name, ("CustomerID",), index_id=5, **kwargs)


def test_table_hint_replacement_forces_the_new_index_and_is_labelled_hinted_evidence() -> None:
    experiment = plan_replacement_experiment(CANDIDATE, [_old()], hide_method="table_hint")

    assert experiment.replaces == ("IX_Orders_CustomerID",)
    assert experiment.setup_sql == () and experiment.teardown_sql == ()
    assert experiment.candidate_hint == "INDEX([IX_Testing_0123456789abcdef])"
    # A hinted measurement shows that the index can serve the query, never that it will be chosen.
    assert experiment.proof_scope == "hinted_evidence"
    assert experiment.as_dict()["sandbox_only"] is True


def test_hint_goes_on_every_reference_of_the_table_and_nothing_else_changes() -> None:
    sql = (
        "WITH recent AS (SELECT o.CustomerID FROM Sales.Orders AS o WHERE o.OrderDate >= @d)\n"
        "SELECT c.CustomerName  FROM Sales.Customers c JOIN recent r ON r.CustomerID = c.CustomerID\n"
        "WHERE EXISTS (SELECT 1 FROM [Sales].[Orders] x WHERE x.CustomerID = c.CustomerID)"
    )
    hint = " WITH (INDEX([IX_Testing_0123456789abcdef]))"

    assert hint_candidate_index(sql, CANDIDATE) == (
        f"WITH recent AS (SELECT o.CustomerID FROM Sales.Orders AS o{hint} WHERE o.OrderDate >= @d)\n"
        "SELECT c.CustomerName  FROM Sales.Customers c JOIN recent r ON r.CustomerID = c.CustomerID\n"
        f"WHERE EXISTS (SELECT 1 FROM [Sales].[Orders] x{hint} WHERE x.CustomerID = c.CustomerID)"
    )


def test_hint_is_refused_when_it_cannot_be_placed_safely() -> None:
    with pytest.raises(ValueError, match="does not reference"):
        hint_candidate_index("SELECT 1 FROM Sales.Customers", CANDIDATE)
    with pytest.raises(ValueError, match="already has a table hint"):
        hint_candidate_index("SELECT 1 FROM Sales.Orders WITH (NOLOCK)", CANDIDATE)
    # A common table expression named like the table is not the table.
    with pytest.raises(ValueError, match="does not reference"):
        hint_candidate_index("WITH Orders AS (SELECT 1 AS x) SELECT x FROM Orders", CANDIDATE)
    # Another schema's table of the same name is not the table either.
    with pytest.raises(ValueError, match="does not reference"):
        hint_candidate_index("SELECT 1 FROM Archive.Orders", CANDIDATE)


def test_disable_in_sandbox_hides_the_old_index_and_restores_it() -> None:
    experiment = plan_replacement_experiment(CANDIDATE, [_old()], hide_method="disable_in_sandbox")

    assert experiment.setup_sql == ("ALTER INDEX [IX_Orders_CustomerID] ON [Sales].[Orders] DISABLE;",)
    assert experiment.teardown_sql == ("ALTER INDEX [IX_Orders_CustomerID] ON [Sales].[Orders] REBUILD;",)
    assert experiment.candidate_hint is None
    assert experiment.proof_scope == "sandbox_replacement"


@pytest.mark.parametrize(
    ("index", "reason"),
    [
        (_index("Sales", "Orders", "PK_Orders", ("OrderID",), index_id=1, type_code=1, primary_key=True), "clustered"),
        (_index("Sales", "Orders", "UQ_Orders", ("CustomerID",), index_id=5, unique=True), "unique"),
        (
            _index(
                "Sales", "Orders", "IX_FK", ("CustomerID",), index_id=5,
                protection={"coverage": "complete", "child_foreign_key_support": [{"foreign_key_id": 9, "leading_key_supported": True}]},
            ),
            "foreign key",
        ),
        (_index("Sales", "Orders", "IX_Unknown", ("CustomerID",), index_id=5, protection={"coverage": "incomplete"}), "protection metadata"),
    ],
)
def test_disable_is_refused_for_an_index_that_enforces_or_supports_a_constraint(index: Any, reason: str) -> None:
    """Disabling a unique index also disables the foreign keys that reference it; that is not an experiment."""

    with pytest.raises(ValueError, match=reason):
        plan_replacement_experiment(CANDIDATE, [index], hide_method="disable_in_sandbox")
    # The table hint changes no object, so it stays available.
    assert plan_replacement_experiment(CANDIDATE, [index], hide_method="table_hint").replaces == (index.name,)


def test_replacement_needs_a_named_candidate_and_indexes_of_the_same_table() -> None:
    with pytest.raises(ValueError, match="index_name"):
        plan_replacement_experiment(replace(CANDIDATE, index_name=None), [_old()])
    with pytest.raises(ValueError, match="Sales.Orders"):
        plan_replacement_experiment(CANDIDATE, [_index("Sales", "OrderLines", "IX_Lines", ("OrderID",), index_id=5)])
    with pytest.raises(ValueError, match="at least one"):
        plan_replacement_experiment(CANDIDATE, [])
    with pytest.raises(ValueError, match="hide_method"):
        plan_replacement_experiment(CANDIDATE, [_old()], hide_method="drop")


# --- item 3: multi-query proof set -----------------------------------------


RECOMMENDATION = {
    "action": "widen_index",
    "validation": {
        "proof_set": [
            {"query_id": 11, "plan_id": 110, "share_of_recommendation_pct": 70.0},
            {"query_id": 12, "plan_id": 120, "share_of_recommendation_pct": 20.0},
        ],
        "regression_set": [
            {"query_id": 21, "plan_id": 210, "workload_share_pct": 4.0},
            {"query_id": 22, "plan_id": 220, "workload_share_pct": 1.0},
            # A query in both sets is measured once, as a proof query.
            {"query_id": 12, "plan_id": 120, "workload_share_pct": 0.5},
        ],
    },
}


def test_proof_queries_come_from_the_recommendation_with_their_roles() -> None:
    queries = proof_queries_from_recommendation(RECOMMENDATION)
    assert [(q.query_id, q.plan_id, q.role) for q in queries] == [
        (11, 110, "proof"),
        (12, 120, "proof"),
        (21, 210, "regression"),
        (22, 220, "regression"),
    ]


def test_proof_set_plan_fits_the_execution_budget_by_dropping_regression_queries_last_first() -> None:
    queries = proof_queries_from_recommendation(RECOMMENDATION)

    full = plan_proof_set(queries, runs_per_phase=3, max_executions=120)
    assert full["executions_required"] == 4 * 3 * 3
    assert full["not_measured"] == []

    tight = plan_proof_set(queries, runs_per_phase=3, max_executions=30)
    assert [q["query_id"] for q in tight["queries"]] == [11, 12, 21]
    assert tight["executions_required"] == 27
    assert tight["not_measured"] == [{"query_id": 22, "role": "regression", "reason": "execution_budget"}]

    # The queries the recommendation claims to serve are never dropped silently.
    with pytest.raises(ValueError, match="proof queries"):
        plan_proof_set(queries, runs_per_phase=3, max_executions=17)


def _result(query_id: int, role: str, status: str, improvement: float | None) -> ProofQueryResult:
    return ProofQueryResult(
        query=ProofQuery(query_id=query_id, plan_id=query_id * 10, role=role),
        assessment=ExperimentAssessment(
            status=status,
            baseline_equivalent=status != "incomplete",
            candidate_wins=status == "winner",
            baseline_median_ms=100.0,
            candidate_median_ms=None if improvement is None else 100.0 - improvement,
            improvement_pct=improvement,
            reason="",
        ),
    )


def test_proof_set_is_proven_only_when_every_proof_query_wins_and_no_regression_query_slows() -> None:
    proven = assess_proof_set(
        [_result(11, "proof", "winner", 60.0), _result(12, "proof", "winner", 30.0), _result(21, "regression", "loser", -4.0)],
        expected=proof_queries_from_recommendation(RECOMMENDATION)[:3],
    )
    assert proven["status"] == "proven"
    assert proven["failed_query_ids"] == []


def test_one_proof_query_that_does_not_win_rejects_the_recommendation() -> None:
    verdict = assess_proof_set([_result(11, "proof", "winner", 60.0), _result(12, "proof", "loser", 1.0)])
    assert verdict["status"] == "rejected"
    assert verdict["failed_query_ids"] == [12]


def test_a_regression_query_that_slows_past_the_rollback_bar_rejects_the_recommendation() -> None:
    verdict = assess_proof_set([_result(11, "proof", "winner", 60.0), _result(21, "regression", "loser", -35.0)])
    assert verdict["status"] == "rejected"
    assert verdict["failed_query_ids"] == [21]
    assert "20" in verdict["reason"]


def test_an_unmeasured_or_inconclusive_query_leaves_the_set_inconclusive() -> None:
    inconclusive = assess_proof_set([_result(11, "proof", "winner", 60.0), _result(12, "proof", "inconclusive", None)])
    assert inconclusive["status"] == "inconclusive"

    missing = assess_proof_set(
        [_result(11, "proof", "winner", 60.0)], expected=proof_queries_from_recommendation(RECOMMENDATION)
    )
    assert missing["status"] == "inconclusive"
    assert missing["unmeasured_query_ids"] == [12, 21, 22]

    assert assess_proof_set([])["status"] == "inconclusive"


# --- the advisor names what a recommendation replaces ------------------------


def test_widen_and_consolidate_recommendations_name_the_indexes_they_replace() -> None:
    """Without this the proof path can only add the new index beside the old one."""

    from azure_sql_mcp.index_advisor import build_index_advice
    from tests.unit.test_index_advisor import _inputs
    from tests.unit.test_index_advisor import _orders_indexes
    from tests.unit.test_index_advisor import _query

    widen = build_index_advice(
        _inputs([_query(1, "seek_residual_lookup_sort.xml", cpu_us=9_000_000)], _orders_indexes())
    )
    rec = next(item for item in widen["recommendations"] if item["action"] == "widen_index")
    assert rec["validation"]["replaces"] == ["FK_Sales_Orders_CustomerID"]
    queries = proof_queries_from_recommendation(rec)
    assert [(query.query_id, query.role) for query in queries] == [(1, "proof")]

    existing = _orders_indexes() + [
        _index("Sales", "Orders", "IX_A", ("OrderDate",), index_id=5, reads=5, includes=("Comments",)),
        _index("Sales", "Orders", "IX_B", ("OrderDate",), index_id=6, reads=50, includes=("Status",)),
    ]
    consolidate = build_index_advice(_inputs([], existing))
    rec = next(item for item in consolidate["recommendations"] if item["action"] == "consolidate_index")
    # The survivor is rebuilt with the absorbed column, so both old definitions go.
    assert rec["validation"]["replaces"] == [rec["index_name"], rec["merge_into"]]
