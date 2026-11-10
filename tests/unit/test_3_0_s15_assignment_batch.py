"""Assignments in front of the result-producing statement of a read-only batch (S15 item 0).

A bare model won benchmark tasks with two batch forms the server refused: a
key read into a variable by SELECT in front of the main statement, and a probe
in front of an IF / ELSE. The proof and benchmark tools could not measure
them. The batch grammar now admits declarations and assignments first, then
one SELECT or one IF / ELSE tree. Every read-only refusal still holds inside
such a batch.
"""

from __future__ import annotations

import pytest

from azure_sql_mcp.safe_sql import MAX_BATCH_ASSIGNMENTS
from azure_sql_mcp.safe_sql import SafeSqlValidator

SHAPE_A = (
    "DECLARE @TypeKey tinyint; "
    "SELECT @TypeKey = t.PostTypeKey FROM dw.DimPostType AS t WHERE t.PostTypeName = @type; "
    "SELECT d.CalendarYear, COUNT_BIG(*) AS n FROM dw.FactPost AS p "
    "JOIN dw.DimDate AS d ON d.DateKey = p.CreationDateKey "
    "WHERE p.PostTypeKey = @TypeKey GROUP BY d.CalendarYear ORDER BY d.CalendarYear "
    "OPTION (OPTIMIZE FOR (@TypeKey = 1));"
)
LOOP_BRANCH = (
    "SELECT u.Id, COUNT_BIG(*) AS n FROM dbo.Users AS u JOIN dbo.Badges AS b ON b.UserId = u.Id "
    "WHERE u.Reputation >= @rep GROUP BY u.Id OPTION (LOOP JOIN)"
)
HASH_BRANCH = LOOP_BRANCH.replace("LOOP JOIN", "HASH JOIN")
SHAPE_B = (
    "DECLARE @n int; "
    "SELECT @n = COUNT(*) FROM (SELECT TOP (2001) u.Id FROM dbo.Users AS u "
    "WHERE u.Reputation >= @rep) AS x; "
    f"IF @n <= 2000 {LOOP_BRANCH} ELSE {HASH_BRANCH};"
)


def _validate(sql: str):
    return SafeSqlValidator().validate_read_only_candidate(sql)


def test_a_key_read_into_a_variable_in_front_of_one_select_is_admitted() -> None:
    query = _validate(SHAPE_A)

    assert query.execution_sql == SHAPE_A
    # The case parameter stays a parameter of the batch; the declared variable is local.
    assert query.parameters == ("@type",)
    assert query.local_variables == ("@TypeKey",)
    [branch] = query.branches
    assert branch.path == "batch" and branch.conditions == ()
    assert branch.select_sql.startswith("SELECT d.CalendarYear")
    assert query.assignment_statements == (
        "SELECT @TypeKey = t.PostTypeKey FROM dw.DimPostType AS t WHERE t.PostTypeName = @type",
    )
    assert "OPTIMIZE FOR" in query.statement_hints


def test_a_probe_in_front_of_an_if_else_is_admitted() -> None:
    query = _validate(SHAPE_B)

    assert query.parameters == ("@rep",)
    assert query.local_variables == ("@n",)
    assert [branch.path for branch in query.branches] == ["if.then", "if.else"]
    assert query.branches[0].conditions == (("@n <= 2000", True),)
    assert query.branches[0].select_sql == LOOP_BRANCH
    assert len(query.assignment_statements) == 1


def test_set_from_a_scalar_subquery_and_several_assignments_are_admitted() -> None:
    query = _validate(
        "DECLARE @lo int, @hi int; DECLARE @n bigint = (SELECT COUNT_BIG(*) FROM dbo.Users AS u); "
        "SELECT @lo = MIN(u.Reputation), @hi = MAX(u.Reputation) FROM dbo.Users AS u "
        "WHERE u.Reputation >= @rep; "
        "SET @n = (SELECT COUNT_BIG(*) FROM dbo.Badges AS b WHERE b.UserId = @lo); "
        "IF @n > 100 SELECT @hi AS hi ELSE SELECT @lo AS lo;"
    )

    assert query.local_variables == ("@lo", "@hi", "@n")
    assert query.parameters == ("@rep",)
    assert len(query.assignment_statements) == 3
    assert len(query.branches) == 2


def test_one_select_with_a_plain_declare_prefix_is_still_the_ordinary_form() -> None:
    query = _validate("DECLARE @k int = 5; SELECT p.Id FROM dbo.Posts AS p WHERE p.Score = @k;")

    assert query.branches == ()
    assert query.assignment_statements == ()


@pytest.mark.parametrize(
    ("statement", "message"),
    [
        # An assignment SELECT that also returns a column: a mixed result set.
        (
            "SELECT @TypeKey = t.PostTypeKey, t.PostTypeName FROM dw.DimPostType AS t;",
            "every select-list item",
        ),
        # A variable the batch did not declare (a parameter of the case).
        ("SELECT @type = t.PostTypeName FROM dw.DimPostType AS t;", "declared local variable"),
        # A function that is refused everywhere.
        (
            "SELECT @TypeKey = COUNT(*) FROM sys.fn_get_audit_file(N'x', NULL, NULL) AS f;",
            "fn_get_audit_file",
        ),
        ("SELECT @TypeKey = COUNT(*) FROM OPENROWSET(BULK N'x', SINGLE_BLOB) AS f;", "rowset"),
        ("SELECT @TypeKey = t.PostTypeKey INTO dbo.Kept FROM dw.DimPostType AS t;", "INTO"),
        ("SELECT @TypeKey = t.PostTypeKey FROM otherdb.dw.DimPostType AS t;", "Cross-database"),
        ("SELECT @TypeKey = t.PostTypeKey FROM dw.DimPostType AS t WITH (UPDLOCK);", "UPDLOCK"),
        # DML between the assignments.
        ("UPDATE dw.DimPostType SET PostTypeName = N'x';", "UPDATE"),
        ("DELETE FROM dw.DimPostType;", "DELETE"),
        ("INSERT INTO dw.DimPostType (PostTypeName) VALUES (N'x');", "INSERT"),
        # User code, temp tables, table variables, SET options.
        ("EXEC dbo.usp_Refresh;", "only sys.sp_executesql of a string literal"),
        ("SELECT @TypeKey = t.k FROM #keys AS t;", "Temporary table"),
        ("DECLARE @keys TABLE (k int);", "scalar type"),
        ("SET NOCOUNT ON;", "SET"),
        ("SET @TypeKey = dbo.fn_UserCode(1);", "SET value"),
    ],
)
def test_each_forbidden_statement_is_still_refused_inside_the_batch(
    statement: str, message: str
) -> None:
    batch = SHAPE_A.replace(
        "SELECT @TypeKey = t.PostTypeKey FROM dw.DimPostType AS t WHERE t.PostTypeName = @type; ",
        "SELECT @TypeKey = t.PostTypeKey FROM dw.DimPostType AS t WHERE t.PostTypeName = @type; "
        + statement
        + " ",
    )

    with pytest.raises(ValueError, match=message):
        _validate(batch)


def test_an_assignment_select_without_its_semicolon_is_refused() -> None:
    batch = SHAPE_B.replace("AS x; IF", "AS x IF")

    with pytest.raises(ValueError, match="semicolon"):
        _validate(batch)


def test_a_second_result_producing_select_is_refused() -> None:
    with pytest.raises(ValueError, match="text after the batch"):
        _validate(SHAPE_A + " SELECT 1 AS extra;")


def test_the_number_of_assignment_statements_is_capped() -> None:
    assert MAX_BATCH_ASSIGNMENTS == 8
    assignment = "SELECT @n = COUNT(*) FROM dbo.Users AS u; "
    allowed = "DECLARE @n int; " + assignment * 8 + "SELECT @n AS n;"
    too_many = "DECLARE @n int; " + assignment * 9 + "SELECT @n AS n;"

    assert len(_validate(allowed).assignment_statements) == 8
    with pytest.raises(ValueError, match="at most 8"):
        _validate(too_many)


def test_the_single_statement_gate_does_not_admit_the_batch() -> None:
    # execute_sql and every other one-statement path keep their gate.
    with pytest.raises(ValueError):
        SafeSqlValidator().validate_read_only(SHAPE_A)


# --- plan capture: one statement plan per executed statement --------------------------

from types import SimpleNamespace  # noqa: E402
from typing import Any  # noqa: E402

from azure_sql_mcp.connection import ProfiledExecution  # noqa: E402
from azure_sql_mcp.connection import QueryResult  # noqa: E402
from azure_sql_mcp.param_binding import ParameterExecutionContract  # noqa: E402
from azure_sql_mcp.param_binding import SqlParameterType  # noqa: E402
from azure_sql_mcp.param_binding import TypedParameter  # noqa: E402
from azure_sql_mcp.param_binding import detect_parameters  # noqa: E402
from azure_sql_mcp.plan_digest import build_plan_digest  # noqa: E402
from azure_sql_mcp.plans import PlansService  # noqa: E402

ASSIGNMENT_B = (
    "SELECT @n = COUNT(*) FROM (SELECT TOP (2001) u.Id FROM dbo.Users AS u "
    "WHERE u.Reputation >= @rep) AS x"
)


def _statement_plan(text: str, *, cpu: int, elapsed: int, rows: int) -> str:
    """The document SET STATISTICS XML returns for one executed statement."""

    return (
        '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">'
        "<BatchSequence><Batch><Statements>"
        f'<StmtSimple StatementText="{text}" StatementType="SELECT" StatementSubTreeCost="0.5">'
        f'<QueryPlan><QueryTimeStats CpuTime="{cpu}" ElapsedTime="{elapsed}"/>'
        '<RelOp NodeId="0" PhysicalOp="Index Seek" LogicalOp="Index Seek" EstimateRows="50" '
        'EstimatedTotalSubtreeCost="0.5"><RunTimeInformation>'
        f'<RunTimeCountersPerThread Thread="0" ActualRows="{rows}" ActualExecutions="1" '
        'ActualLogicalReads="7"/></RunTimeInformation><IndexScan/></RelOp>'
        "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"
    )


class _BatchExecutor:
    """Returns the plan of each executed statement after its result set, as the engine does."""

    def __init__(self, executed: list[tuple[str, int, int, int]]) -> None:
        self.config = SimpleNamespace(row_limit=200)
        self.executed = executed
        self.sent: list[Any] = []

    def _result_sets(self) -> list[QueryResult]:
        *assignments, final = self.executed
        sets = [
            QueryResult(
                columns=("plan",),
                rows=[{"plan": _statement_plan(text, cpu=cpu, elapsed=elapsed, rows=rows)}],
            )
            for text, cpu, elapsed, rows in assignments
        ]
        text, cpu, elapsed, rows = final
        sets.append(QueryResult(columns=("Id",), rows=[{"Id": 1}, {"Id": 2}]))
        sets.append(
            QueryResult(
                columns=("plan",),
                rows=[{"plan": _statement_plan(text, cpu=cpu, elapsed=elapsed, rows=rows)}],
            )
        )
        return sets

    async def execute_profiled_read_only(self, _database: str, query: str, **kwargs: Any):
        self.sent.append((query, kwargs.get("params")))
        return ProfiledExecution(result_sets=self._result_sets(), elapsed_wall_ms=40.0)

    async def execute_session(self, _database: str, statements: list[str], **_kwargs: Any):
        self.sent.append(statements[1])
        return [[], self._result_sets(), []]


def _rep_contract(sql: str) -> ParameterExecutionContract:
    return ParameterExecutionContract(
        sql_text=sql,
        bucket_id="rare",
        parameters=(TypedParameter("@rep", SqlParameterType("int"), 200000, "test"),),
        provenance="candidate",
    )


@pytest.mark.asyncio
async def test_the_actual_plan_of_a_probe_batch_is_the_whole_batch_and_names_its_branch() -> None:
    # The probe is the longer plan in a real run: the result statement must
    # still be found, and the probe's cost must count.
    executor = _BatchExecutor([(ASSIGNMENT_B, 30, 34, 1), (HASH_BRANCH, 120, 150, 2)])
    service = PlansService(executor=executor, validator=SafeSqlValidator())  # type: ignore[arg-type]

    profiled = await service.profile_parameterized_query("appdb", _rep_contract(SHAPE_B))

    # One typed call of the whole batch: @rep is its parameter, @n is local.
    query, params = executor.sent[0]
    assert query.startswith("EXEC sys.sp_executesql")
    assert params[0] == SHAPE_B
    assert detect_parameters(SHAPE_B) == ["rep"]
    batch = profiled.plan.summary["batch"]
    assert batch["branch_count"] == 2
    assert batch["executed_branch"]["ordinal"] == 2
    assert batch["executed_branch"]["conditions"] == [
        {"condition": "@n <= 2000", "is_true": False}
    ]
    assert batch["statements_executed"] == 2
    assert batch["result_statement"] == 2
    assert "assignment" in batch["metric_note"]
    metrics = profiled.plan.summary["actual_metrics"]
    # The cost of the case is the cost of the batch: the probe counts.
    assert metrics["actual_cpu_ms"] == 150
    assert metrics["actual_elapsed_ms"] == 184
    assert metrics["query_metric_source"] == "showplan_query_time_stats"
    assert metrics["actual_logical_reads"] == 14
    # The rows are those of the result-producing statement, the last one.
    assert metrics["actual_rows"] == 2
    # The batch returns one result set: the plan documents are not rows.
    assert [result.columns for result in profiled.result_sets] == [("Id",)]
    digest = build_plan_digest(profiled.plan.raw_xml)
    assert digest["statement_count"] == 2
    assert [entry["statement_index"] for entry in digest["statements"]] == [0, 1]


@pytest.mark.asyncio
async def test_explain_with_analyze_reads_a_key_lookup_batch_as_one_candidate() -> None:
    final_select = SafeSqlValidator().validate_read_only_candidate(SHAPE_A).branches[0].select_sql
    assignment = SafeSqlValidator().validate_read_only_candidate(SHAPE_A).assignment_statements[0]
    executor = _BatchExecutor([(assignment, 1, 2, 1), (final_select, 200, 240, 15)])
    service = PlansService(executor=executor, validator=SafeSqlValidator())  # type: ignore[arg-type]

    artifact = await service.explain_query("appdb", SHAPE_A, analyze=True)

    assert artifact.summary["batch"]["branch_count"] == 1
    assert artifact.summary["batch"]["executed_branch"]["ordinal"] == 1
    assert artifact.summary["batch"]["statements_executed"] == 2
    assert artifact.summary["actual_metrics"]["actual_cpu_ms"] == 201
    assert artifact.summary["statement_count"] == 2


@pytest.mark.asyncio
async def test_an_if_else_batch_with_no_assignment_keeps_its_one_statement_metrics() -> None:
    batch = f"IF @rep > 1000 {LOOP_BRANCH} ELSE {HASH_BRANCH};"
    executor = _BatchExecutor([(LOOP_BRANCH, 20, 25, 2)])
    service = PlansService(executor=executor, validator=SafeSqlValidator())  # type: ignore[arg-type]

    profiled = await service.profile_parameterized_query("appdb", _rep_contract(batch))

    assert profiled.plan.summary["batch"] == {
        "branch_count": 2,
        "executed_branch": {
            "ordinal": 1,
            "path": "if.then",
            "conditions": [{"condition": "@rep > 1000", "is_true": True}],
            "kind": "select",
        },
    }
    assert profiled.plan.summary["actual_metrics"]["actual_cpu_ms"] == 20


# --- the comparison and benchmark tools take the batch as one candidate ------------------

from tests.unit.test_3_0_s4_measurement import BASELINE  # noqa: E402
from tests.unit.test_3_0_s4_measurement import CASES  # noqa: E402
from tests.unit.test_3_0_s4_measurement import ScriptedPlans  # noqa: E402
from tests.unit.test_3_0_s4_measurement import _session  # noqa: E402
from tests.unit.test_performance_workflows import _service  # noqa: E402
from tests.unit.test_performance_workflows import _typed_binder  # noqa: E402

PROBE_BATCH = (
    "DECLARE @n int; "
    "SELECT @n = COUNT(*) FROM (SELECT TOP (11) i.id FROM dbo.Items AS i WHERE i.owner = @p) AS x; "
    "IF @n <= 10 SELECT id FROM dbo.Items AS candidate WHERE owner = @p OPTION (LOOP JOIN) "
    "ELSE SELECT id FROM dbo.Items AS candidate WHERE owner = @p OPTION (HASH JOIN);"
)


@pytest.mark.asyncio
async def test_a_probe_batch_is_compared_with_the_original_in_one_snapshot() -> None:
    service, _store, _plans, executor = _service(binder=_typed_binder())

    comparison = await service.compare_query_results(
        "appdb", BASELINE, PROBE_BATCH, parameter_case=CASES[0]
    )

    assert comparison["status"] == "match", comparison
    # The whole batch is the candidate statement of the snapshot session, sent
    # as one typed call: the parameter of the case is its only parameter.
    statements = executor.sessions[0]
    parameters = executor.session_parameters[0]
    assert PROBE_BATCH in (statements[3], *(parameters[3] or ()))


@pytest.mark.asyncio
async def test_a_probe_batch_is_benchmarked_as_one_candidate() -> None:
    plans = ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 100)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (candidate_id,) = _session(service, PROBE_BATCH)

    result = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, PROBE_BATCH,
        phase="finalist", parameter_cases=CASES,
    )

    assert result["classification"] in {"improved", "performance_only"}, result["reason"]
    assert [item["status"] for item in result["equivalence"]] == ["match", "match"]
    # Every candidate execution ran the whole batch text, assignments included.
    candidate_runs = [sql for side, _case, sql in plans.calls if side == "candidate"]
    assert candidate_runs and all(PROBE_BATCH in sql for sql in candidate_runs)
