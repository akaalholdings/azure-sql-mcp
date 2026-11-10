"""The read-only gate admits one closed multi-statement form: the branching batch.

The standard fix for catch-all optional parameters is IF / ELSE with one SELECT
per branch, or parameterized dynamic SQL. Without this form the product cannot
prove or measure that fix. The grammar is closed, so every test below that
hides a write, a second statement or built dynamic SQL must stay refused.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from azure_sql_mcp.safe_sql import SafeSqlValidator
from azure_sql_mcp.safe_sql import read_only_batch_branches

# Task T2 of the first live comparison: catch-all optional parameters.
CATCH_ALL_BASELINE = (
    "SELECT TOP (50) c.Id, c.PostId, c.UserId, c.Score, c.CreationDate FROM dbo.Comments AS c "
    "WHERE (@UserId IS NULL OR c.UserId = @UserId) AND (@PostId IS NULL OR c.PostId = @PostId) "
    "ORDER BY c.CreationDate DESC, c.Id DESC"
)
SELECT_LIST = "SELECT TOP (50) c.Id, c.PostId, c.UserId, c.Score, c.CreationDate FROM dbo.Comments AS c"
ORDER = "ORDER BY c.CreationDate DESC, c.Id DESC"
IF_ELSE_FIX = f"""
IF @UserId IS NOT NULL AND @PostId IS NOT NULL
    {SELECT_LIST} WHERE c.UserId + 0 = @UserId AND c.PostId = @PostId {ORDER};
ELSE IF @UserId IS NOT NULL
    {SELECT_LIST} WHERE c.UserId = @UserId {ORDER};
ELSE IF @PostId IS NOT NULL
BEGIN
    {SELECT_LIST} WHERE c.PostId = @PostId {ORDER};
END
ELSE
    {SELECT_LIST} {ORDER};
"""
DYNAMIC_FIX = (
    "IF @UserId IS NOT NULL\n"
    "    EXEC sys.sp_executesql\n"
    "        N'SELECT TOP (50) c.Id, c.Score FROM dbo.Comments AS c WHERE c.UserId = @u "
    "ORDER BY c.CreationDate DESC, c.Id DESC',\n"
    "        N'@u int', @u = @UserId;\n"
    "ELSE\n"
    f"    {SELECT_LIST} WHERE c.PostId = @PostId {ORDER};"
)


def test_if_else_fix_for_catch_all_parameters_is_admitted_as_one_candidate() -> None:
    validator = SafeSqlValidator()
    with pytest.raises(ValueError, match="only supports SELECT|not allowed"):
        validator.validate_read_only(IF_ELSE_FIX)

    validated = validator.validate_read_only_candidate(IF_ELSE_FIX)

    assert validated.execution_sql == IF_ELSE_FIX
    assert [branch.kind for branch in validated.branches] == ["select"] * 4
    assert [branch.path for branch in validated.branches] == [
        "if.then",
        "if.else.if.then",
        "if.else.if.else.if.then",
        "if.else.if.else.if.else",
    ]
    assert validated.branches[1].conditions == (
        ("@UserId IS NOT NULL AND @PostId IS NOT NULL", False),
        ("@UserId IS NOT NULL", True),
    )
    # Each branch is the exact SELECT text, and it passes the single-SELECT gate.
    for branch in validated.branches:
        assert branch.select_sql.startswith("SELECT TOP (50)")
        assert branch.select_sql.endswith("c.Id DESC")
        validator.validate_read_only(branch.standalone_sql)
    assert "c.UserId + 0 = @UserId" in validated.branches[0].select_sql
    assert validated.parameters == ("@UserId", "@PostId")


def test_parameterized_dynamic_sql_branch_is_admitted_with_its_declared_parameters() -> None:
    validated = SafeSqlValidator().validate_read_only_candidate(DYNAMIC_FIX)

    dynamic, static = validated.branches
    assert dynamic.kind == "sp_executesql"
    assert dynamic.select_sql.startswith("SELECT TOP (50) c.Id, c.Score FROM dbo.Comments")
    assert dynamic.dynamic_parameters == (("@u", "int"),)
    assert dynamic.standalone_sql.startswith("DECLARE @u int;\nSELECT TOP (50)")
    assert static.kind == "select"


def test_local_variables_from_parameters_and_literals_are_kept_for_each_branch() -> None:
    sql = (
        "DECLARE @from datetime2(0) = @since, @top int = 25;\n"
        "SET @top = @top + 1;\n"
        "IF @since IS NULL SELECT TOP (@top) p.Id FROM dbo.Posts AS p ORDER BY p.Id\n"
        "ELSE SELECT TOP (@top) p.Id FROM dbo.Posts AS p WHERE p.CreationDate >= @from ORDER BY p.Id"
    )

    validated = SafeSqlValidator().validate_read_only_candidate(sql)

    assert validated.parameters == ("@since",)
    assert validated.local_variables == ("@from", "@top")
    assert validated.branches[1].standalone_sql.startswith("DECLARE @from datetime2(0) = @since")
    assert validated.branches[1].standalone_sql.endswith("ORDER BY p.Id")


def test_a_single_select_is_validated_exactly_as_before() -> None:
    validator = SafeSqlValidator()

    validated = validator.validate_read_only_candidate(CATCH_ALL_BASELINE)

    assert validated == validator.validate_read_only(CATCH_ALL_BASELINE)
    assert validated.branches == ()
    assert read_only_batch_branches(CATCH_ALL_BASELINE) is None
    assert len(read_only_batch_branches(IF_ELSE_FIX) or ()) == 4


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        # A branch that hides a write.
        ("IF @a = 1 SELECT 1 ELSE DELETE FROM dbo.Posts", "ELSE branch"),
        ("IF @a = 1 SELECT 1 ELSE SELECT 1; DELETE FROM dbo.Posts", "after the batch"),
        ("IF @a = 1 UPDATE dbo.Posts SET Score = 0 ELSE SELECT 1", "IF condition|branch"),
        ("IF @a = 1 SELECT p.Id INTO dbo.Copy FROM dbo.Posts AS p ELSE SELECT 1", "INTO"),
        ("IF @a = 1 BEGIN SELECT 1; DELETE FROM dbo.Posts END", "one statement"),
        ("IF @a = 1 BEGIN TRANSACTION SELECT 1", "BEGIN"),
        ("IF @a = 1 SELECT 1 ELSE SELECT NEXT VALUE FOR dbo.Seq", "NEXT VALUE FOR"),
        ("IF @a = 1 SELECT 1 FROM dbo.Posts WITH (UPDLOCK) ELSE SELECT 2", "Locking hints"),
        # A dynamic string built by concatenation.
        (
            "IF @a = 1 EXEC sys.sp_executesql N'SELECT 1 FROM dbo.Posts WHERE Id = ' + @id "
            "ELSE SELECT 1",
            "one string literal",
        ),
        (
            "DECLARE @s nvarchar(max) = N'SELECT ' + @col + N' FROM dbo.Posts';\n"
            "IF @a = 1 EXEC sys.sp_executesql @s ELSE SELECT 1",
            "one string literal",
        ),
        # EXEC of a variable, of a string, of another procedure.
        ("IF @a = 1 EXEC (@s) ELSE SELECT 1", "sp_executesql"),
        ("IF @a = 1 EXEC @s ELSE SELECT 1", "sp_executesql"),
        ("IF @a = 1 EXEC ('DELETE FROM dbo.Posts') ELSE SELECT 1", "sp_executesql"),
        ("IF @a = 1 EXEC dbo.PurgePosts ELSE SELECT 1", "sp_executesql"),
        ("IF @a = 1 EXEC master.sys.sp_executesql N'SELECT 1' ELSE SELECT 1", "sp_executesql"),
        ("IF @a = 1 EXEC [sys].[sp_executesql] N'SELECT 1' ELSE SELECT 1", "sp_executesql"),
        ("IF @a = 1 EXEC @rc = sys.sp_executesql N'SELECT 1' ELSE SELECT 1", "sp_executesql"),
        # The dynamic statement itself must be one read-only SELECT.
        ("IF @a = 1 EXEC sys.sp_executesql N'DELETE FROM dbo.Posts' ELSE SELECT 1", "dynamic"),
        (
            "IF @a = 1 EXEC sys.sp_executesql N'SELECT 1; DROP TABLE dbo.Posts' ELSE SELECT 1",
            "dynamic",
        ),
        (
            "IF @a = 1 EXEC sys.sp_executesql N'EXEC sys.sp_executesql N''SELECT 1''' ELSE SELECT 1",
            "dynamic",
        ),
        (
            "IF @a = 1 EXEC sys.sp_executesql N'IF 1 = 1 SELECT 1' ELSE SELECT 1",
            "dynamic",
        ),
        (
            "IF @a = 1 EXEC sys.sp_executesql N'SELECT 1 WHERE 1 = @x', N'@y int', @y = 1 "
            "ELSE SELECT 1",
            "does not declare",
        ),
        (
            "IF @a = 1 EXEC sys.sp_executesql N'SELECT @x', N'@x int OUTPUT', @x = @a OUTPUT "
            "ELSE SELECT 1",
            "parameter list",
        ),
        (
            "IF @a = 1 EXEC sys.sp_executesql N'SELECT 1', N'@x int', N'DROP TABLE dbo.Posts', 2 "
            "ELSE SELECT 1",
            "more arguments",
        ),
        ("IF @a = 1 EXEC sys.sp_executesql N'SELECT 1' WITH RECOMPILE ELSE SELECT 1", "after"),
        # WAITFOR and other statements.
        ("IF @a = 1 WAITFOR DELAY '00:00:10' ELSE SELECT 1", "WAITFOR"),
        ("IF @a = 1 SELECT 1 ELSE BEGIN WAITFOR DELAY '00:00:10' END", "WAITFOR"),
        ("WHILE 1 = 1 SELECT 1", "only supports SELECT|not allowed"),
        ("IF @a = 1 SELECT 1 ELSE PRINT 'x'", "ELSE branch"),
        ("IF @a = 1 SELECT 1 ELSE SET @a = 2", "ELSE branch"),
        ("IF @a = 1 SELECT 1 ELSE RETURN", "ELSE branch"),
        ("IF @a = 1 SELECT 1 ELSE SELECT 1 FROM #work", "Temporary table"),
        ("IF @a = 1 SELECT 1\nGO\nDELETE FROM dbo.Posts", "GO"),
        # The condition is over parameters and literals only.
        ("IF EXISTS (SELECT 1 FROM dbo.Posts) SELECT 1 ELSE SELECT 2", "IF condition"),
        ("IF dbo.CanPurge(@a) = 1 SELECT 1 ELSE SELECT 2", "IF condition"),
        ("IF @@ROWCOUNT > 0 SELECT 1 ELSE SELECT 2", "IF condition"),
        ("IF @a = (SELECT MAX(Id) FROM dbo.Posts) SELECT 1 ELSE SELECT 2", "IF condition"),
        ("IF SELECT 1", "IF condition"),
        # Prefix statements are scalar DECLARE and SET @variable only.
        ("DECLARE @t TABLE (Id int); IF @a = 1 SELECT 1 ELSE SELECT 2", "DECLARE"),
        ("DECLARE @x int = (SELECT MAX(Id) FROM dbo.Posts); IF @a = 1 SELECT 1", "DECLARE"),
        ("DECLARE @x int = dbo.Fn(1); IF @a = 1 SELECT 1", "DECLARE"),
        ("SET NOCOUNT ON; IF @a = 1 SELECT 1 ELSE SELECT 2", "SET"),
        ("SET @x = NEXT VALUE FOR dbo.Seq; IF @a = 1 SELECT 1", "SET"),
        # A line separator SQL Server does not treat as a line end could hide
        # the statement string in a comment and run a later argument instead.
        (
            "IF @a = 1 EXEC sys.sp_executesql --c\x0c N'SELECT 1', N'@x nvarchar(99)',\n"
            "N'DROP TABLE dbo.Posts'",
            "line separator",
        ),
    ],
)
def test_anything_outside_the_branching_batch_grammar_is_refused(sql: str, reason: str) -> None:
    validator = SafeSqlValidator()

    with pytest.raises(ValueError, match=reason):
        validator.validate_read_only_candidate(sql)
    assert read_only_batch_branches(sql) is None


def test_case_expressions_do_not_end_a_branch() -> None:
    sql = (
        "IF @a = 1 SELECT CASE WHEN p.Score > 0 THEN 'up' ELSE 'down' END AS vote "
        "FROM dbo.Posts AS p ELSE SELECT (CASE WHEN 1 = 1 THEN 'x' END) AS vote"
    )

    validated = SafeSqlValidator().validate_read_only_candidate(sql)

    assert len(validated.branches) == 2
    assert validated.branches[0].select_sql.endswith("FROM dbo.Posts AS p")


def test_nesting_and_branch_count_are_bounded() -> None:
    deep = "IF @a = 1 " * 40 + "SELECT 1"

    with pytest.raises(ValueError, match="nested"):
        SafeSqlValidator().validate_read_only_candidate(deep)


CORPUS = Path(__file__).resolve().parents[1] / "fixtures" / "safe_sql_attack_corpus.json"


def test_attack_corpus_admits_nothing_new_outside_the_grammar() -> None:
    validator = SafeSqlValidator()
    inputs = [case["sql"] for case in json.loads(CORPUS.read_text(encoding="utf-8"))["cases"]]
    newly_admitted: list[str] = []
    changed_verdict: list[str] = []
    for sql in inputs:
        try:
            before = validator.validate_read_only(sql)
        except ValueError:
            before = None
        try:
            after = validator.validate_read_only_candidate(sql)
        except ValueError:
            after = None
        if before is not None and after != before:
            changed_verdict.append(sql)
        if before is None and after is not None:
            # Admitted only as the closed grammar: it has parsed branches.
            assert after.branches, sql
            newly_admitted.append(sql)

    assert len(inputs) > 1000
    assert changed_verdict == []
    # Every corpus input the gate refused before stream S12 is still refused.
    assert newly_admitted == []


# --- The proof, comparison and plan paths take the batch as one candidate. ---

from types import SimpleNamespace  # noqa: E402
from typing import Any  # noqa: E402

from azure_sql_mcp.connection import ProfiledExecution  # noqa: E402
from azure_sql_mcp.connection import QueryResult  # noqa: E402
from azure_sql_mcp.equivalence_contract import analyze_equivalence_preflight  # noqa: E402
from azure_sql_mcp.equivalence_contract import effective_compare_order  # noqa: E402
from azure_sql_mcp.equivalence_contract import order_contract_risk  # noqa: E402
from azure_sql_mcp.equivalence_contract import rewrite_requirements  # noqa: E402
from azure_sql_mcp.param_binding import ParameterExecutionContract  # noqa: E402
from azure_sql_mcp.param_binding import SqlParameterType  # noqa: E402
from azure_sql_mcp.param_binding import TypedParameter  # noqa: E402
from azure_sql_mcp.plans import PlansService  # noqa: E402


def test_every_branch_must_keep_the_baseline_order_contract() -> None:
    assert effective_compare_order(IF_ELSE_FIX, None) is True
    assert order_contract_risk(CATCH_ALL_BASELINE, IF_ELSE_FIX) is None

    weaker = IF_ELSE_FIX.replace(
        f"WHERE c.UserId = @UserId {ORDER}", "WHERE c.UserId = @UserId ORDER BY c.Id DESC"
    )
    risk = order_contract_risk(CATCH_ALL_BASELINE, weaker)

    assert risk is not None
    assert risk["risk"] == "order_contract_changed"
    assert risk["branch"] == 2


def test_static_preflight_reads_a_batch_branch_by_branch() -> None:
    preflight = analyze_equivalence_preflight(IF_ELSE_FIX)

    # Four TOP (50) ... ORDER BY branches: four ordered row limits to prove.
    assert preflight.risk_codes == ("row_limit_total_order_unproven",)
    assert [detail["branch"] for detail in preflight.details] == [1, 2, 3, 4]
    assert all(detail["total_order"]["provable"] for detail in preflight.details)


def test_a_branch_that_changes_null_semantics_is_still_a_rewrite_requirement() -> None:
    baseline = (
        "SELECT p.Id FROM dbo.Posts AS p WHERE (@u IS NULL OR p.OwnerUserId = @u) "
        "AND p.Id NOT IN (SELECT c.PostId FROM dbo.Comments AS c)"
    )
    candidate = (
        "IF @u IS NULL SELECT p.Id FROM dbo.Posts AS p WHERE NOT EXISTS "
        "(SELECT 1 FROM dbo.Comments AS c WHERE c.PostId = p.Id) "
        "ELSE SELECT p.Id FROM dbo.Posts AS p WHERE p.OwnerUserId = @u AND p.Id NOT IN "
        "(SELECT c.PostId FROM dbo.Comments AS c)"
    )

    assert [item["code"] for item in rewrite_requirements(baseline, candidate)] == [
        "not_in_null_semantics"
    ]


def _plan(statement_text: str) -> str:
    return (
        '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">'
        "<BatchSequence><Batch><Statements>"
        f'<StmtSimple StatementText="{statement_text}" StatementType="SELECT" '
        'StatementSubTreeCost="0.01"><QueryPlan><RelOp NodeId="0" '
        'PhysicalOp="Index Seek" LogicalOp="Index Seek" EstimateRows="50" '
        'EstimatedTotalSubtreeCost="0.01"><IndexScan/></RelOp></QueryPlan></StmtSimple>'
        "</Statements></Batch></BatchSequence></ShowPlanXML>"
    )


class _Executor:
    def __init__(self, plan_for: Any) -> None:
        self.config = SimpleNamespace(row_limit=200)
        self.plan_for = plan_for
        self.sent: list[Any] = []

    async def execute_session(self, _database: str, statements: list[str], **_kwargs: Any):
        self.sent.append(statements[1])
        plan = QueryResult(columns=("plan",), rows=[{"plan": self.plan_for(statements[1])}])
        return [[], [plan], []]

    async def execute_profiled_read_only(self, _database: str, query: str, **kwargs: Any):
        self.sent.append((query, kwargs.get("params")))
        rows = QueryResult(columns=("Id",), rows=[{"Id": 1}])
        plan = QueryResult(columns=("plan",), rows=[{"plan": self.plan_for(query)}])
        return ProfiledExecution(result_sets=[rows, plan], elapsed_wall_ms=4.0)


def _contract(sql: str) -> ParameterExecutionContract:
    return ParameterExecutionContract(
        sql_text=sql,
        bucket_id="user-only",
        parameters=(
            TypedParameter("@UserId", SqlParameterType("int"), 1144035, "test"),
            TypedParameter("@PostId", SqlParameterType("int"), None, "test"),
        ),
        provenance="candidate",
    )


@pytest.mark.asyncio
async def test_actual_plan_of_a_batch_names_the_branch_the_parameter_case_selected() -> None:
    branch_select = SafeSqlValidator().validate_read_only_candidate(IF_ELSE_FIX).branches[1]
    executor = _Executor(lambda _sql: _plan(branch_select.select_sql))
    service = PlansService(executor=executor, validator=SafeSqlValidator())  # type: ignore[arg-type]

    profiled = await service.profile_parameterized_query("appdb", _contract(IF_ELSE_FIX))

    # The whole batch is sent once, as one typed sp_executesql call.
    query, params = executor.sent[0]
    assert query.startswith("EXEC sys.sp_executesql")
    assert params[0] == IF_ELSE_FIX
    assert profiled.user_query_executions == 1
    assert profiled.plan.summary["batch"] == {
        "branch_count": 4,
        "executed_branch": {
            "ordinal": 2,
            "path": "if.else.if.then",
            "conditions": [
                {"condition": "@UserId IS NOT NULL AND @PostId IS NOT NULL", "is_true": False},
                {"condition": "@UserId IS NOT NULL", "is_true": True},
            ],
            "kind": "select",
        },
    }


@pytest.mark.asyncio
async def test_estimated_plans_come_one_per_branch_and_nothing_is_executed() -> None:
    executor = _Executor(lambda sql: _plan("branch"))
    service = PlansService(executor=executor, validator=SafeSqlValidator())  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="explain_batch_branches"):
        await service.explain_parameterized_query("appdb", _contract(IF_ELSE_FIX), analyze=False)
    assert executor.sent == []

    plans = await service.explain_batch_branches("appdb", IF_ELSE_FIX, contract=_contract(IF_ELSE_FIX))

    assert [item["branch"]["path"] for item in plans] == [
        "if.then",
        "if.else.if.then",
        "if.else.if.else.if.then",
        "if.else.if.else.if.else",
    ]
    assert all(item["plan"].query_executed is False for item in plans)
    # Each branch is compiled alone, with only the parameters it uses.
    assert executor.sent[1].startswith("DECLARE @UserId int;")
    assert "@PostId" not in executor.sent[1]
    assert "OPTIMIZE FOR (@UserId = 1144035)" in executor.sent[1]
    assert not executor.sent[3].startswith("DECLARE")


# --- The comparison and the benchmark take the batch as one candidate. ---

from tests.unit.test_3_0_s4_measurement import ScriptedPlans  # noqa: E402
from tests.unit.test_performance_workflows import _service  # noqa: E402
from tests.unit.test_performance_workflows import _typed_binder  # noqa: E402

OPTIONAL_BASELINE = "SELECT id FROM dbo.Items WHERE (@p IS NULL OR owner = @p) ORDER BY id"
OPTIONAL_FIX = (
    "IF @p IS NULL SELECT id FROM dbo.Items AS candidate ORDER BY id "
    "ELSE SELECT id FROM dbo.Items AS candidate WHERE owner = @p ORDER BY id"
)
OPTIONAL_CASES = [
    {"name": "one-owner", "values": {"p": 7}, "types": {"p": "int"}, "weight": 1.0},
]


@pytest.mark.asyncio
async def test_a_branching_batch_is_compared_and_benchmarked_in_a_session() -> None:
    plans = ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 100)
    service, _store, _plans, executor = _service(plans=plans, binder=_typed_binder())
    case = service.start_case("appdb", OPTIONAL_BASELINE, parameter_cases=OPTIONAL_CASES)
    session_id = service.start_session(case.case_id, "appdb")["session_id"]
    candidate_id = service.add_candidate(session_id, OPTIONAL_FIX, strategy="predicate")[
        "candidate_id"
    ]

    comparison = await service.compare_query_results(
        "appdb", OPTIONAL_BASELINE, OPTIONAL_FIX, parameter_case=OPTIONAL_CASES[0]
    )
    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        OPTIONAL_BASELINE,
        OPTIONAL_FIX,
        phase="finalist",
        parameter_cases=OPTIONAL_CASES,
    )

    assert comparison["status"] == "match"
    assert comparison["order_compared"] is True
    assert result["classification"] == "improved"
    assert result["proof_scope"] == "direct_snapshot"
    # The batch text reached the measured path whole: one candidate, one execution each.
    sent = [sql for side, _case, sql in plans.calls if side == "candidate"]
    assert all(sql.split("*/", 1)[-1].strip() == OPTIONAL_FIX for sql in sent)
    assert executor.sessions


@pytest.mark.asyncio
async def test_a_batch_branch_that_drops_the_order_is_stopped_before_any_execution() -> None:
    plans = ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 100)
    service, _store, _plans, executor = _service(plans=plans, binder=_typed_binder())
    unordered = OPTIONAL_FIX.replace("WHERE owner = @p ORDER BY id", "WHERE owner = @p")

    comparison = await service.compare_query_results(
        "appdb", OPTIONAL_BASELINE, unordered, parameter_case=OPTIONAL_CASES[0]
    )

    assert comparison["status"] == "proof_contract_required"
    assert comparison["executions"] == 0
    assert "order_contract_changed" in comparison["equivalence_preflight"]["candidate"]["risk_codes"]
    assert executor.sessions == []
