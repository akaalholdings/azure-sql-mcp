from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp.connection import QueryResult
from tests.azure_dmv_contract import documented_entries
from tests.azure_dmv_contract import documented_row

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "capture_dmv_columns.py"


def _load():
    spec = importlib.util.spec_from_file_location("capture_dmv_columns", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Table-valued functions per their Microsoft Learn syntax; kept here, not read
# from columns.json, so a fixture that marks one as a view still fails.
TABLE_VALUED_FUNCTIONS = frozenset(
    {"sys.dm_io_virtual_file_stats", "sys.dm_exec_query_plan_stats", "sys.dm_exec_query_statistics_xml"}
)


class CaptureExecutor:
    """Answers TOP (0) probes with column metadata only, like the driver."""

    def __init__(self, failing: dict[str, Exception] | None = None) -> None:
        self.failing = failing or {}
        self.statements: list[str] = []

    def _raise_if_failing(self, query: str) -> None:
        for needle, error in self.failing.items():
            if needle in query:
                raise error

    async def execute_batches(self, database_name: str, query: str, **_: Any) -> list[QueryResult]:
        self.statements.append(query)
        self._raise_if_failing(query)
        source = query.rsplit("APPLY ", 1)[1] if "APPLY " in query else query.split("FROM ", 1)[1]
        name = source.split("(", 1)[0].split()[0]
        if name in TABLE_VALUED_FUNCTIONS and (not source.startswith(f"{name}(") or f"{name}()" in source):
            raise RuntimeError(f"Parameters were not supplied for the function '{name}'.")
        return [QueryResult(columns=tuple(documented_entries()[name]["columns"]), rows=[])]

    async def fetch_all(self, database_name: str, query: str, *_: Any, **__: Any) -> list[dict[str, Any]]:
        self.statements.append(query)
        if "DATABASEPROPERTYEX" in query:
            return [{"edition": "GeneralPurpose", "service_objective": "ElasticPool", "elastic_pool_name": "pool-secret"}]
        self._raise_if_failing(query)
        return [
            documented_row(
                "sys.dm_user_db_resource_governance",
                database_id=7,
                server_name="server-secret",
                database_name="db-secret",
                slo_name="SQLDB_OP_GP_GEN5_4",
                logical_database_guid="0f0e0d0c-0000-0000-0000-000000000000",
                primary_max_log_rate=50331648,
                max_sessions=30000,
                primary_group_max_cpu=100.0,
                govern_background_io=True,
                user_data_directory_space_usage_mb=81234,
                replica_role=0,
                replica_type=0,
            )
        ]


@pytest.mark.asyncio
async def test_capture_writes_column_names_only(tmp_path: Path) -> None:
    module = _load()
    executor = CaptureExecutor()

    capture = await module.capture(executor, "db-secret")
    path = module.write_capture(capture, tmp_path)

    text = path.read_text()
    assert path.name == "generalpurpose_elasticpool.json"
    assert "secret" not in text
    payload = json.loads(text)
    assert payload["edition"] == "GeneralPurpose" and payload["pooled"] is True
    governance = payload["dmvs"]["sys.dm_user_db_resource_governance"]
    assert governance == {"columns": documented_entries()["sys.dm_user_db_resource_governance"]["columns"]}
    assert "governance_sample" not in payload
    assert all(statement.lstrip().startswith("SELECT TOP (0) ") for statement in executor.statements if "TOP (0)" in statement)
    assert "SELECT TOP (0) * FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL)" in executor.statements
    # Every table-valued function is probed with arguments, so a live capture can read it.
    assert all("columns" in payload["dmvs"][name] for name in TABLE_VALUED_FUNCTIONS)


@pytest.mark.asyncio
async def test_governance_sample_keeps_numeric_limits_only() -> None:
    capture = await _load().capture(CaptureExecutor(), "db-secret", sample_governance_row=True)

    sample = capture["governance_sample"]
    assert sample["primary_max_log_rate"] == 50331648
    assert sample["max_sessions"] == 30000
    # Names, GUIDs, flags and the database id never leave the database.
    assert not {"server_name", "database_name", "slo_name", "logical_database_guid", "govern_background_io", "database_id"} & set(sample)
    # Usage and role values are not limits.
    assert not {"user_data_directory_space_usage_mb", "replica_role", "replica_type"} & set(sample)


@pytest.mark.asyncio
async def test_unreadable_governance_sample_keeps_the_column_capture() -> None:
    # Basic, S0, S1 and pooled databases deny this DMV without ##MS_ServerStateReader##:
    # the tiers the sample is for.
    executor = CaptureExecutor(
        failing={
            "sys.dm_user_db_resource_governance": PermissionError(
                "The user does not have permission to perform this action on server 'server-secret'."
            )
        }
    )

    capture = await _load().capture(executor, "db-secret", sample_governance_row=True)

    assert capture["governance_sample"] == {"error": "permission_denied"}
    assert capture["dmvs"]["sys.dm_user_db_resource_governance"] == {"error": "permission_denied"}
    assert "columns" in capture["dmvs"]["sys.dm_db_resource_stats"]
    assert "secret" not in json.dumps(capture)


@pytest.mark.asyncio
async def test_unreadable_dmv_records_a_category_not_the_message() -> None:
    executor = CaptureExecutor(
        failing={
            "sys.dm_exec_query_stats": PermissionError("The user does not have permission on server 'server-secret'."),
            "sys.dm_os_out_of_memory_events": RuntimeError("Invalid object name 'sys.dm_os_out_of_memory_events'."),
        }
    )

    capture = await _load().capture(executor, "db-secret")

    assert capture["dmvs"]["sys.dm_exec_query_stats"] == {"error": "permission_denied"}
    assert capture["dmvs"]["sys.dm_os_out_of_memory_events"] == {"error": "invalid_object"}
    assert "secret" not in json.dumps(capture)


def test_capture_requires_a_database(capsys) -> None:
    with pytest.raises(SystemExit):
        _load().main([])
    assert "--database" in capsys.readouterr().err
