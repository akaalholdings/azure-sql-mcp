from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.artifacts import ExplainPlanArtifact
from azure_sql_mcp.server import AzureSqlMcpApplication
from tests.unit.test_server import make_config

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "azure_live_acceptance.py"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "showplans"


def _load():
    spec = importlib.util.spec_from_file_location("azure_live_acceptance", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_live_acceptance_refuses_without_explicit_disposable_confirmation(capsys) -> None:
    # The run creates and drops objects, so it must never start by accident.
    assert _load().main(["--database", "appdb"]) == 2
    assert "--confirm-disposable-database" in capsys.readouterr().err


def test_live_acceptance_only_touches_its_own_schema() -> None:
    module = _load()

    assert module.SCHEMA == "mcp_accept"
    for statement in (module.CREATE_TABLES_SQL, module.LOAD_SQL):
        for line in statement.splitlines():
            if line.strip().upper().startswith(("CREATE TABLE", "INSERT ", "UPDATE STATISTICS")):
                assert "[mcp_accept]" in line, line


@pytest.mark.asyncio
async def test_plan_checks_match_real_tool_output(tmp_path: Path, showplan) -> None:
    # The live run cannot happen in CI, so prove its expectations against the
    # real tools with a faked database.
    module = _load()
    app = AzureSqlMcpApplication(make_config(tmp_path))
    rt = showplan.runtime
    timed = showplan.plan(
        showplan.relop(0, "Sort", None, showplan.scan(1, "Orders", runtime=rt((0, 40, 1, 30, 25))), runtime=rt((0, 40, 1, 45, 38))),
        plan_children='<QueryTimeStats ElapsedTime="46" CpuTime="39" />',
    )
    estimated = (FIXTURES / "seek_residual_lookup_sort.xml").read_text(encoding="utf-8")
    responses = [
        ("dm_exec_query_plan_stats", [{"plan_id": 7, "query_id": 3, "query_plan_hash": "0x01", "last_execution_time": None, "query_plan": timed}]),
        ("LAST_QUERY_PLAN_STATS", [{"value": "1"}]),
        ("AS avg_rowcount", [{"executions": 40, "avg_rowcount": 12.0}]),
        ("query_store_wait_stats", []),
        ("WHERE p.query_id = ?", [{"plan_id": 7, "query_id": 3, "is_forced_plan": 0, "query_plan": estimated}]),
    ]

    async def fetch(database_name: str, query: str, params: Any = None) -> list[dict[str, Any]]:
        return next(rows for fragment, rows in responses if fragment in query)

    app.executor.fetch_all = AsyncMock(side_effect=fetch)  # type: ignore[method-assign]
    app.plans.explain_query = AsyncMock(  # type: ignore[method-assign]
        return_value=ExplainPlanArtifact(database_name="appdb", analyze=True, summary={"statement_count": 1}, raw_xml=timed)
    )
    results: list[dict[str, Any]] = []

    await module.plan_checks(app, "appdb", results, 3)

    assert [item["check"] for item in results] == [
        "query store plan digest",
        "query store runtime attached",
        "last actual plan read",
        "actual plan ranked by self time",
        "self time never exceeds the statement",
    ]
    assert all(item["passed"] for item in results), results
