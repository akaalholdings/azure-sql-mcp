"""Wiring 2, item 1: a read-only branching batch is one candidate at every proof tool.

Task T2 of the first live comparison: the standard fix for catch-all optional
parameters is IF / ELSE with one SELECT per branch. The tools refused it, so
the agent measured nothing through the product.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.connection import ProfiledExecution
from azure_sql_mcp.connection import QueryResult
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.param_binding import detect_parameters
from azure_sql_mcp.safe_sql import SafeSqlValidator
from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_server import make_config

_COLUMNS = "c.Id, c.PostId, c.UserId, c.Score, c.CreationDate"
_ORDER = "ORDER BY c.CreationDate DESC, c.Id DESC"
BASELINE = (
    f"SELECT TOP (50) {_COLUMNS} FROM dbo.Comments AS c\n"
    "WHERE (@UserId IS NULL OR c.UserId = @UserId) AND (@PostId IS NULL OR c.PostId = @PostId)\n"
    f"{_ORDER}"
)
USER_BRANCH = (
    f"SELECT TOP (50) {_COLUMNS} FROM dbo.Comments AS c\n"
    f"    WHERE c.UserId = @UserId {_ORDER};"
)
IF_ELSE_FIX = (
    "IF @UserId IS NOT NULL AND @PostId IS NOT NULL\n"
    f"    SELECT TOP (50) {_COLUMNS} FROM dbo.Comments AS c\n"
    f"    WHERE c.UserId + 0 = @UserId AND c.PostId = @PostId {_ORDER};\n"
    "ELSE IF @UserId IS NOT NULL\n"
    f"    {USER_BRANCH}\n"
    "ELSE IF @PostId IS NOT NULL\n"
    f"    SELECT TOP (50) {_COLUMNS} FROM dbo.Comments AS c\n"
    f"    WHERE c.PostId = @PostId {_ORDER};\n"
    "ELSE\n"
    f"    SELECT TOP (50) {_COLUMNS} FROM dbo.Comments AS c\n"
    f"    {_ORDER};"
)
DYNAMIC_BRANCH = (
    f"EXEC sys.sp_executesql N'SELECT TOP (50) {_COLUMNS} FROM dbo.Comments AS c "
    f"WHERE c.UserId = @u {_ORDER}', N'@u int', @u = @UserId;"
)
DYNAMIC_FIX = IF_ELSE_FIX.replace(USER_BRANCH, DYNAMIC_BRANCH)
REFUSED = "IF @UserId IS NULL SELECT 1 ELSE DELETE FROM dbo.Comments"
USER_ONLY = {
    "parameter_values": {"UserId": 1144035, "PostId": None},
    "parameter_types": {"UserId": "int", "PostId": "int"},
}
USER_ONLY_CASE = {
    "name": "user-only",
    "values": USER_ONLY["parameter_values"],
    "types": USER_ONLY["parameter_types"],
    "weight": 1.0,
}


def _plan(statement_text: str, table: str = "Comments") -> str:
    text = statement_text.replace("&", "&amp;").replace("<", "&lt;").replace('"', "&quot;")
    return (
        '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">'
        "<BatchSequence><Batch><Statements>"
        f'<StmtSimple StatementText="{text}" StatementType="SELECT" '
        'StatementSubTreeCost="0.01"><QueryPlan><RelOp NodeId="0" '
        'PhysicalOp="Index Seek" LogicalOp="Index Seek" EstimateRows="50" '
        'EstimatedTotalSubtreeCost="0.01"><IndexScan>'
        f'<Object Schema="[dbo]" Table="[{table}]" Index="[IX_Comments_UserId]"/>'
        "</IndexScan></RelOp></QueryPlan></StmtSimple>"
        "</Statements></Batch></BatchSequence></ShowPlanXML>"
    )


class _Database:
    """Records what reached the database: compiled texts and executed batches."""

    def __init__(self, executed_statement: str = "") -> None:
        self.compiled: list[str] = []
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.executed_statement = executed_statement

    async def execute_session(self, _database: str, statements: list[str], **_kwargs: Any) -> Any:
        self.compiled.append(statements[1])
        plan = QueryResult(columns=("plan",), rows=[{"plan": _plan(statements[1])}])
        return [[], [plan], []]

    async def execute_profiled_read_only(self, _database: str, query: str, **kwargs: Any) -> Any:
        self.executed.append((query, tuple(kwargs.get("params") or ())))
        rows = QueryResult(columns=("Id",), rows=[{"Id": 1}])
        plan = QueryResult(columns=("plan",), rows=[{"plan": _plan(self.executed_statement)}])
        return ProfiledExecution(result_sets=[rows, plan], elapsed_wall_ms=4.0)


async def _call(app: AzureSqlMcpApplication, name: str, arguments: dict[str, Any]) -> Any:
    return await app.mcp._tool_manager.call_tool(name, arguments)


def _install(app: AzureSqlMcpApplication, database: _Database) -> None:
    app.executor.execute_session = database.execute_session  # type: ignore[method-assign]
    app.executor.execute_profiled_read_only = (  # type: ignore[method-assign]
        database.execute_profiled_read_only
    )
    app.executor.fetch_all = AsyncMock(return_value=[])  # type: ignore[method-assign]


@pytest.fixture
def app(tmp_path: Path) -> AzureSqlMcpApplication:
    application = AzureSqlMcpApplication(make_config(tmp_path))
    policy = DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "appdb": {
                    "environment": "development",
                    "allow_read": True,
                    "allow_benchmark": True,
                    "max_benchmark_executions": 80,
                }
            },
        }
    )
    application.database_policy = policy
    application.performance_workflows.database_policy = policy
    return application


@pytest.mark.asyncio
async def test_estimated_explain_of_a_batch_returns_one_plan_per_branch(
    app: AzureSqlMcpApplication,
) -> None:
    # The agent must see the plan each branch gets before it measures any of
    # them; "use analyze=true" would have run a 150 s scan to learn a shape.
    database = _Database()
    _install(app, database)

    result = await _call(app, "explain_query", {"sql": IF_ELSE_FIX, **USER_ONLY})

    assert result["analyze"] is False
    assert result["query_executed"] is False
    assert database.executed == []
    assert result["summary"]["batch"] == {"branch_count": 4}
    assert [branch["ordinal"] for branch in result["branches"]] == [1, 2, 3, 4]
    second = result["branches"][1]
    assert second["condition"] == (
        "NOT (@UserId IS NOT NULL AND @PostId IS NOT NULL) AND (@UserId IS NOT NULL)"
    )
    assert second["plan_digest"]["statement_count"] == 1
    assert "findings" in second["plan_findings"]
    # Each branch was compiled alone, for the parameters it uses.
    assert len(database.compiled) == 4
    assert "@PostId" not in database.compiled[1]
    assert "OPTIMIZE FOR (@UserId = 1144035)" in database.compiled[1]
    # Compact: no per-operator summary, and no raw plan text inline.
    assert "summary" not in second
    assert "raw_xml" not in second
    assert result["headline"]["plan_kind"] == "estimated"


@pytest.mark.asyncio
async def test_actual_explain_of_a_batch_names_the_branch_the_parameters_selected(
    app: AzureSqlMcpApplication,
) -> None:
    # One execution, and the response says which branch it measured: without
    # that the agent cannot tie the numbers to a SELECT.
    database = _Database(executed_statement=USER_BRANCH.rstrip(";"))
    _install(app, database)

    result = await _call(
        app, "explain_query", {"sql": IF_ELSE_FIX, "analyze": True, **USER_ONLY}
    )

    [(query, params)] = database.executed
    assert query.startswith("EXEC sys.sp_executesql")
    assert params[0] == IF_ELSE_FIX
    assert params[2:] == (1144035, None)
    batch = result["summary"]["batch"]
    assert batch["branch_count"] == 4
    assert batch["executed_branch"]["ordinal"] == 2
    assert batch["executed_branch"]["path"] == "if.else.if.then"


def test_a_named_sp_executesql_argument_is_not_a_parameter_of_the_batch() -> None:
    # "@u = @UserId" names an argument of the inner call. Asking the caller
    # for a value of @u refused the dynamic form of the fix at the binder.
    assert detect_parameters(DYNAMIC_FIX) == ["UserId", "PostId"]
    # A name that is also used as a variable is still a parameter.
    both = DYNAMIC_FIX.replace("@u = @UserId", "@u = @u").replace(
        "IF @UserId IS NOT NULL AND", "IF @u IS NOT NULL AND"
    )
    assert "u" in detect_parameters(both)
    # A single SELECT is read exactly as before.
    assert detect_parameters("SELECT @a = c.Id, @b = c.Score FROM dbo.Comments AS c") == ["a", "b"]


@pytest.mark.asyncio
async def test_the_binder_binds_the_dynamic_sql_form_with_the_callers_parameters(
    app: AzureSqlMcpApplication,
) -> None:
    database = _Database(
        executed_statement=f"SELECT TOP (50) {_COLUMNS} FROM dbo.Comments AS c "
        f"WHERE c.UserId = @u {_ORDER}"
    )
    _install(app, database)

    result = await _call(
        app, "explain_query", {"sql": DYNAMIC_FIX, "analyze": True, **USER_ONLY}
    )

    [(_query, params)] = database.executed
    assert params[0] == DYNAMIC_FIX
    assert params[1] == "@UserId int, @PostId int"
    assert result["summary"]["batch"]["executed_branch"]["kind"] == "sp_executesql"

    estimated = await _call(app, "explain_query", {"sql": DYNAMIC_FIX, **USER_ONLY})
    dynamic = estimated["branches"][1]
    assert dynamic["kind"] == "sp_executesql"
    assert dynamic["estimated_plan_basis"]["basis"] == "typed_variables_without_values"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "arguments"),
    [
        ("explain_query", {"sql": REFUSED, "parameter_values": {"UserId": 1}}),
        ("check_equivalence_preflight", {"sql": REFUSED}),
        ("start_performance_case", {"sql": REFUSED}),
        ("tune_query", {"sql": REFUSED, "parameter_values": {"UserId": 1}}),
        (
            "compare_query_results",
            {"baseline_sql": BASELINE, "candidate_sql": REFUSED, "parameter_case": USER_ONLY_CASE},
        ),
        (
            "benchmark_query_rewrite",
            {"baseline_sql": BASELINE, "rewrite_sql": REFUSED, **USER_ONLY},
        ),
    ],
)
async def test_a_batch_with_a_write_branch_stays_refused_at_every_entry_point(
    app: AzureSqlMcpApplication, tool: str, arguments: dict[str, Any]
) -> None:
    database = _Database()
    _install(app, database)

    with pytest.raises(Exception, match="read-only branching batch|only supports SELECT"):
        await _call(app, tool, arguments)

    assert database.executed == []
    assert database.compiled == []


@pytest.mark.asyncio
async def test_execute_sql_keeps_the_single_statement_gate(app: AzureSqlMcpApplication) -> None:
    # execute_sql returns rows of one statement; a batch there has no proof
    # workflow around it and no branch label on its result.
    _install(app, _Database())

    with pytest.raises(Exception, match="only supports SELECT"):
        await _call(app, "execute_sql", {"sql": IF_ELSE_FIX})


@pytest.mark.asyncio
async def test_preflight_reads_a_batch_branch_by_branch(app: AzureSqlMcpApplication) -> None:
    _install(app, _Database())

    result = await _call(app, "check_equivalence_preflight", {"sql": IF_ELSE_FIX})

    # Every branch is read: a gap in any one of them is named with its branch.
    assert sorted({gap["branch"] for gap in result["total_order_gaps"]}) == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_a_batch_opens_a_case_and_becomes_a_session_candidate(
    app: AzureSqlMcpApplication,
) -> None:
    # T2: no case or session could be opened, so the product's proof and
    # measurement were not used at all.
    _install(app, _Database())

    case = await _call(
        app,
        "start_performance_case",
        {"sql": BASELINE, "parameter_cases": [USER_ONLY_CASE]},
    )
    session = await _call(app, "start_tuning_session", {"case_id": case["case_id"]})
    candidate = await _call(
        app,
        "add_tuning_candidate",
        {
            "session_id": session["session_id"],
            "candidate_sql": IF_ELSE_FIX,
            "strategy": "predicate",
        },
    )
    batch_case = await _call(
        app,
        "start_performance_case",
        {"sql": IF_ELSE_FIX, "parameter_cases": [USER_ONLY_CASE]},
    )

    assert candidate["candidate_id"]
    assert batch_case["case_id"] != case["case_id"]


def test_the_batch_gate_is_the_one_safe_sql_owns() -> None:
    # The server adds no grammar of its own: what it admits is what the gate admits.
    validator = SafeSqlValidator()
    assert len(validator.validate_read_only_candidate(DYNAMIC_FIX).branches) == 4
    with pytest.raises(ValueError):
        validator.validate_read_only_candidate(REFUSED)


# --- The server's binder inside the measured workflow. ---

from tests.unit.test_3_0_s4_measurement import ScriptedPlans  # noqa: E402
from tests.unit.test_performance_workflows import _service  # noqa: E402

OPTIONAL_BASELINE = "SELECT id FROM dbo.Items WHERE (@owner IS NULL OR owner = @owner) ORDER BY id"
OPTIONAL_DYNAMIC_FIX = (
    "IF @owner IS NULL SELECT id FROM dbo.Items AS candidate ORDER BY id "
    "ELSE EXEC sys.sp_executesql N'SELECT id FROM dbo.Items AS candidate "
    "WHERE owner = @o ORDER BY id', N'@o int', @o = @owner;"
)
OPTIONAL_CASES = [
    {"name": "one-owner", "values": {"owner": 7}, "types": {"owner": "int"}, "weight": 1.0},
]


@pytest.mark.asyncio
async def test_the_server_binder_carries_the_dynamic_form_through_a_measured_session(
    app: AzureSqlMcpApplication,
) -> None:
    # The binder the tools use, not a test double: it refused the batch
    # because it asked the caller for a value of the inner argument @o.
    plans = ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 100)
    service, _store, _plans, _executor = _service(
        plans=plans, binder=app._bind_performance_parameters
    )
    case = service.start_case("appdb", OPTIONAL_BASELINE, parameter_cases=OPTIONAL_CASES)
    session_id = service.start_session(case.case_id, "appdb")["session_id"]
    candidate_id = service.add_candidate(
        session_id, OPTIONAL_DYNAMIC_FIX, strategy="predicate"
    )["candidate_id"]

    comparison = await service.compare_query_results(
        "appdb", OPTIONAL_BASELINE, OPTIONAL_DYNAMIC_FIX, parameter_case=OPTIONAL_CASES[0]
    )
    result = await service.benchmark_candidate(
        session_id,
        candidate_id,
        "appdb",
        OPTIONAL_BASELINE,
        OPTIONAL_DYNAMIC_FIX,
        phase="finalist",
        parameter_cases=OPTIONAL_CASES,
    )

    assert comparison["status"] == "match"
    assert result["classification"] == "improved"
    assert result["proof_scope"] == "direct_snapshot"
    sent = [sql for side, _case, sql in plans.calls if side == "candidate"]
    assert sent and all(sql.split("*/", 1)[-1].strip() == OPTIONAL_DYNAMIC_FIX for sql in sent)
