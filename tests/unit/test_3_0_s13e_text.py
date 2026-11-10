"""Refusals that say what to do, hints an explain can take, and guides that match the tools.

O05: a cross-case run after the finalist was refused with one sentence and no
route. O15: explain_query refused OPTION (USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE'))
and blamed stripped quotes. D12: the instructions named a tool the profile did
not have.
"""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.config import PROFILE_TOOL_ALLOWLISTS
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.safe_sql import SafeSqlValidator
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.server_instructions import SERVER_INSTRUCTIONS
from azure_sql_mcp.tool_guides import MAX_GUIDE_CHARS
from azure_sql_mcp.tool_guides import get_tool_guide
from tests.unit.test_3_0_s4_measurement import BASELINE
from tests.unit.test_3_0_s4_measurement import CANDIDATE
from tests.unit.test_3_0_s4_measurement import CASES
from tests.unit.test_3_0_s4_measurement import ScriptedPlans
from tests.unit.test_3_0_s4_measurement import _session
from tests.unit.test_performance_workflows import _service
from tests.unit.test_performance_workflows import _typed_binder

SELECT = "SELECT u.Id FROM dbo.Users AS u WHERE u.DisplayName LIKE '%john%'"


@pytest.mark.asyncio
async def test_a_second_finalist_is_refused_with_the_call_that_adds_a_run() -> None:
    """S14: a cross case after the finalist is now measured (test_3_0_s14_measure_after_finalist).

    What is still refused is a plain second finalist; the refusal names the argument.
    """

    plans = ScriptedPlans(lambda side, _case, _call: 400 if side == "baseline" else 100)
    service, _store, _plans, _executor = _service(plans=plans, binder=_typed_binder())
    session_id, (candidate_id,) = _session(service, CANDIDATE)
    first = await service.benchmark_candidate(
        session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
        phase="finalist", parameter_cases=CASES,
    )
    assert first["classification"] == "improved", first["reason"]

    with pytest.raises(ValueError) as refusal:
        await service.benchmark_candidate(
            session_id, candidate_id, "appdb", BASELINE, CANDIDATE,
            phase="finalist", parameter_cases=CASES,
        )

    message = str(refusal.value)
    assert "terminal benchmark result (improved)" in message
    assert "measurement_only" in message and "cross_case" in message

    # Registering the same statement again still works: a second candidate.
    again = service.add_candidate(session_id, CANDIDATE, strategy="predicate")
    assert again["candidate_id"] != candidate_id
    crossed = await service.benchmark_candidate(
        session_id, again["candidate_id"], "appdb", BASELINE, CANDIDATE,
        phase="finalist", parameter_cases=CASES,
        cross_case={"compile_case": "rare", "run_case": "common"},
    )
    assert crossed["cross_case_result"]["compile_case"] == "rare"


def test_the_same_statement_cannot_be_registered_twice_while_the_first_is_open() -> None:
    service, _store, _plans, _executor = _service(binder=_typed_binder())
    session_id, (_candidate_id,) = _session(service, CANDIDATE)

    with pytest.raises(ValueError, match="already registered"):
        service.add_candidate(session_id, CANDIDATE, strategy="predicate")


@pytest.mark.parametrize(
    "option",
    [
        # Documented for Azure SQL Database and missing from the list until now.
        "OPTION (USE HINT('ASSUME_FIXED_MIN_SELECTIVITY_FOR_REGEXP'))",
        "OPTION (USE HINT('ASSUME_FIXED_MAX_SELECTIVITY_FOR_REGEXP'), MAXDOP 4)",
        "OPTION (USE HINT('DISABLE_OPTIMIZER_ROWGOAL', 'ASSUME_FIXED_MIN_SELECTIVITY_FOR_REGEXP'))",
    ],
)
def test_every_documented_plan_hint_of_azure_sql_database_is_admitted(option: str) -> None:
    validated = SafeSqlValidator().validate_read_only(f"{SELECT}\n{option}")

    assert validated.execution_sql == f"{SELECT}\n{option}"
    assert "USE HINT" in validated.statement_hints


def test_an_undocumented_use_hint_stays_refused_and_the_refusal_names_it() -> None:
    """O15: the refusal blamed the quotes of a statement whose quotes were intact."""

    with pytest.raises(ValueError) as refusal:
        SafeSqlValidator().validate_read_only(
            f"{SELECT}\nOPTION (USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE'))"
        )

    message = str(refusal.value)
    assert "USE HINT name 'ENABLE_PARALLEL_PLAN_PREFERENCE'" in message
    assert "documented" in message
    assert "Check that every string literal" not in message


@pytest.mark.parametrize(
    "option",
    [
        # A hint that blocks the statement, and one that exists in Fabric only.
        "OPTION (USE HINT('ABORT_QUERY_EXECUTION'))",
        "OPTION (USE HINT('DISABLE_RESULT_SET_CACHE'))",
        # Not a name: an expression, a subquery, a delimited or dotted name.
        "OPTION (USE HINT('A' + 'B'))",
        "OPTION (USE HINT((SELECT 1)))",
        "OPTION (USE HINT([ENABLE_PARALLEL_PLAN_PREFERENCE]))",
        "OPTION (USE HINT('DROP TABLE dbo.Users'))",
        "OPTION (USE HINT('x'); DROP TABLE dbo.Users; --'))",
        # A query hint outside the closed list.
        "OPTION (TABLE HINT(u, INDEX(IX_x)))",
    ],
)
def test_other_text_in_an_option_clause_is_still_refused(option: str) -> None:
    with pytest.raises(ValueError):
        SafeSqlValidator().validate_read_only(f"{SELECT}\n{option}")


def test_a_refused_option_clause_is_named_instead_of_blaming_the_quotes() -> None:
    with pytest.raises(ValueError) as refusal:
        SafeSqlValidator().validate_read_only(f"{SELECT}\nOPTION (TABLE HINT(u, FORCESEEK))")

    message = str(refusal.value)
    assert "OPTION clause" in message and "TABLE HINT" in message
    assert "Check that every string literal" not in message


def test_a_write_under_an_admitted_hint_is_still_refused() -> None:
    with pytest.raises(ValueError):
        SafeSqlValidator().validate_read_only(
            "DELETE FROM dbo.Users OPTION (USE HINT('ASSUME_FIXED_MIN_SELECTIVITY_FOR_REGEXP'))"
        )


def test_the_instructions_name_a_columnstore_tool_that_every_tuning_profile_has() -> None:
    """D12: get_columnstore_health was named for index work; the sandbox profile lacks it."""

    index_work = SERVER_INSTRUCTIONS.split("- Index design and review:", 1)[1].split("- One slow query", 1)[0]

    assert "get_columnstore_health" not in index_work
    assert "get_object_index_diagnostics" in index_work
    assert "get_object_index_diagnostics" in PROFILE_TOOL_ALLOWLISTS[McpProfile.SANDBOX]
    assert len(SERVER_INSTRUCTIONS) < 8200


def _guide(name: str) -> str:
    guide = get_tool_guide(name)
    assert guide["found"] is True and len(guide["guide"]) <= MAX_GUIDE_CHARS
    return " ".join(guide["guide"].split())


def test_the_hold_guide_says_to_drop_before_the_answer_and_to_check_each_object() -> None:
    text = _guide("session_held_objects")

    assert "Do not hold objects on a database other sessions measure" in text
    assert "before you deliver the answer" in text
    assert "dropped" in text and "cleanup_required" in text
    assert "finalize_tuning_session again" in text
    assert "remediation" in text


def test_the_tools_say_what_they_do_about_a_leftover(server_config_factory: Any) -> None:
    app = AzureSqlMcpApplication(server_config_factory())
    tools = {tool.name: tool for tool in app.mcp._tool_manager.list_tools()}

    finalize = tools["finalize_tuning_session"].description
    assert "cleanup_required" in finalize and "again" in finalize
    runs = tools["benchmark_tuning_candidate"].parameters["properties"]["runs"]
    assert "2 to 5" in runs["description"]
