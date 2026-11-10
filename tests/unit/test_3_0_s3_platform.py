"""Platform label, compatibility gates and locking state in the capability envelope."""

from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.locking_state import read_locking_state
from azure_sql_mcp.locking_state import summarize_locking_state
from azure_sql_mcp.platform_capabilities import CAPABILITY_METADATA_SQL
from azure_sql_mcp.platform_capabilities import PlatformCapabilitiesService
from azure_sql_mcp.platform_capabilities import summarize_azure_sql_capabilities
from azure_sql_mcp.platform_capabilities import unsupported_platform_reason
from tests.azure_dmv_contract import StrictDmvExecutor


def metadata(**values: Any) -> dict[str, Any]:
    base: dict[str, Any] = {"database_name": "appdb", "compatibility_level": 160, "engine_edition": 5}
    base.update(values)
    return base


# --- finding 89: an engine that is not Azure SQL Database -----------------------


@pytest.mark.parametrize(
    ("engine_edition", "platform"),
    [
        (8, "azure_sql_managed_instance"),
        (3, "sql_server"),
        (2, "sql_server"),
        (4, "sql_server"),
        (6, "azure_synapse_analytics"),
    ],
)
def test_other_engines_are_labelled_and_not_supported(engine_edition: int, platform: str) -> None:
    # Per-database resource caps, ##MS_ServerStateReader## hints and "no instance
    # options" are Azure SQL Database facts. On a managed instance or a box they
    # are wrong advice, so the envelope must say so before any tool is trusted.
    summary = summarize_azure_sql_capabilities(metadata(engine_edition=engine_edition))

    assert summary["platform"] == platform
    assert summary["platform_supported"] is False
    assert summary["platform_verified"] is False
    assert summary["result_status"] == "not_supported"
    assert f"EngineEdition {engine_edition}" in summary["result_status_reason"]
    assert unsupported_platform_reason(summary) == summary["result_status_reason"]


def test_azure_sql_database_is_supported_with_no_status_noise() -> None:
    summary = summarize_azure_sql_capabilities(metadata())

    assert summary["platform"] == "azure_sql_database_paas"
    assert summary["platform_supported"] is True
    assert "result_status" not in summary
    assert unsupported_platform_reason(summary) is None


def test_unreadable_engine_edition_is_unknown_not_asserted() -> None:
    summary = summarize_azure_sql_capabilities({"database_name": "appdb", "compatibility_level": 160})

    assert summary["platform"] == "unknown"
    assert summary["platform_supported"] is None
    assert summary["platform_verified"] is None
    assert "could not be read" in (unsupported_platform_reason(summary) or "")


# --- finding 83: memory grant feedback gates -----------------------------------


def test_memory_grant_feedback_persistence_and_percentile_apply_from_level_140() -> None:
    # A level-140 database (common after a migration) with Query Store READ_WRITE
    # does persist grant feedback. Calling it "not applicable" makes an agent rule
    # out the feature that changed the grant between two runs.
    summary = summarize_azure_sql_capabilities(
        metadata(compatibility_level=140, query_store_actual_state="READ_WRITE")
    )

    for feature in ("memory_grant_feedback_persistence", "memory_grant_feedback_percentile"):
        assert summary["features"][feature]["minimum_compatibility_level"] == 140
        assert summary["features"][feature]["applicable"] is True
        assert summary["features"][feature]["prerequisites_met"] is True
    assert summary["features"]["row_mode_memory_grant_feedback"]["applicable"] is False


def test_persistence_still_needs_query_store_read_write() -> None:
    summary = summarize_azure_sql_capabilities(
        metadata(compatibility_level=140, query_store_actual_state="READ_ONLY")
    )

    assert summary["features"]["memory_grant_feedback_persistence"]["prerequisites_met"] is False


# --- item 4: optimized locking and accelerated database recovery ----------------


def test_locking_state_reads_each_flag() -> None:
    state = summarize_locking_state(
        {
            "optimized_locking_property": 1,
            "is_accelerated_database_recovery_on": True,
            "is_read_committed_snapshot_on": False,
            "snapshot_isolation_state_desc": "OFF",
        }
    )

    assert state["optimized_locking"] == "on"
    assert state["accelerated_database_recovery"] == "on"
    assert state["read_committed_snapshot"] == "off"
    assert state["snapshot_isolation"] == "off"
    # Lock after qualification needs RCSI; with it off only TID locking applies.
    assert state["lock_after_qualification"] == "inactive"
    assert any("TID" in note for note in state["notes"])


@pytest.mark.parametrize("row", [None, {}, {"optimized_locking_property": None, "is_accelerated_database_recovery_on": None}])
def test_unreadable_locking_flags_are_unknown_never_off(row: dict[str, Any] | None) -> None:
    # "off" would tell an agent that every row lock is held to end of transaction
    # and send it to tune lock escalation that no longer exists.
    state = summarize_locking_state(row)

    assert state["optimized_locking"] == "unknown"
    assert state["accelerated_database_recovery"] == "unknown"
    assert state["read_committed_snapshot"] == "unknown"
    assert state["lock_after_qualification"] == "unknown"
    assert "off" not in {value for value in state.values() if isinstance(value, str)}


def test_optimized_locking_off_is_reported_as_off_when_read() -> None:
    state = summarize_locking_state({"optimized_locking_property": 0, "is_read_committed_snapshot_on": 1})

    assert state["optimized_locking"] == "off"
    assert state["lock_after_qualification"] == "inactive"


@pytest.mark.asyncio
async def test_read_locking_state_is_self_describing_and_survives_a_failed_read() -> None:
    row = {
        "optimized_locking_property": 1,
        "is_accelerated_database_recovery_on": True,
        "is_read_committed_snapshot_on": True,
        "snapshot_isolation_state_desc": "ON",
    }
    ok = await read_locking_state(StrictDmvExecutor(responses=[("IsOptimizedLockingOn", [row])]), "appdb")  # type: ignore[arg-type]

    assert ok["result_status"] == "ok"
    assert ok["lock_after_qualification"] == "active"
    assert "DATABASEPROPERTYEX" in ok["source"] and "sys.databases" in ok["source"]
    assert ok["collection_window"] == {"mode": "point_in_time"}

    failed = await read_locking_state(
        StrictDmvExecutor(responses=[("IsOptimizedLockingOn", RuntimeError("connection reset"))]), "appdb"  # type: ignore[arg-type]
    )
    assert failed["result_status"] == "unavailable"
    assert failed["optimized_locking"] == "unknown"


@pytest.mark.asyncio
async def test_capability_summary_carries_the_locking_state() -> None:
    assert "'IsOptimizedLockingOn'" in CAPABILITY_METADATA_SQL
    assert "d.is_accelerated_database_recovery_on" in CAPABILITY_METADATA_SQL
    rows = [
        metadata(
            optimized_locking_property=1,
            is_accelerated_database_recovery_on=True,
            is_read_committed_snapshot_on=True,
            snapshot_isolation_state_desc="ON",
        )
    ]
    executor = StrictDmvExecutor(responses=[("FROM sys.databases AS d", rows)])

    summary = await PlatformCapabilitiesService(executor).get_summary("appdb")  # type: ignore[arg-type]

    assert executor.violations == []
    assert summary["locking"]["optimized_locking"] == "on"
    assert summary["locking"]["lock_after_qualification"] == "active"


def test_capability_summary_without_locking_columns_reports_unknown() -> None:
    assert summarize_azure_sql_capabilities(metadata())["locking"]["optimized_locking"] == "unknown"
