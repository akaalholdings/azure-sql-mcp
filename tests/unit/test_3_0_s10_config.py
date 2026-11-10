"""Server configuration added in 3.0: the data-detail level and the new tools."""

from __future__ import annotations

from dataclasses import replace

import pytest

from azure_sql_mcp.config import PROFILE_TOOL_ALLOWLISTS
from azure_sql_mcp.config import TOOL_GROUPS
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.config import load_server_config


@pytest.fixture
def base_env(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_SQL_SERVER", "server.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DEFAULT_DATABASE", "appdb")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "appdb")
    monkeypatch.delenv("AZURE_SQL_DATA_DETAIL", raising=False)


def test_data_detail_defaults_to_full_and_loads_from_environment_or_flag(
    base_env, monkeypatch
) -> None:
    assert load_server_config([]).data_detail == "full"

    monkeypatch.setenv("AZURE_SQL_DATA_DETAIL", " Mask_Literals ")
    assert load_server_config([]).data_detail == "mask_literals"
    assert (
        load_server_config(["--azure-sql-data-detail", "metadata_only"]).data_detail
        == "metadata_only"
    )


def test_an_unknown_data_detail_stops_startup(base_env, monkeypatch) -> None:
    # A typo must not silently fall back to "full" and return customer data.
    monkeypatch.setenv("AZURE_SQL_DATA_DETAIL", "masked")

    with pytest.raises(ValueError, match="AZURE_SQL_DATA_DETAIL"):
        load_server_config([])


def test_sanitized_config_reports_data_detail_and_no_secret(server_config_factory) -> None:
    config = replace(
        server_config_factory(username="reader", password="dummy-s3cret-value"),
        data_detail="metadata_only",
    )

    sanitized = config.sanitized_config()

    assert sanitized["data_detail"] == "metadata_only"
    assert "dummy-s3cret-value" not in repr(sanitized)
    assert sanitized["credentials_configured"]["password"] is True


def test_data_detail_does_not_rotate_the_config_fingerprint(server_config_factory) -> None:
    # Learning scopes are keyed on this fingerprint; a privacy setting must
    # not orphan them.
    config = server_config_factory()

    assert (
        replace(config, data_detail="mask_literals").sanitized_config_fingerprint()
        == config.sanitized_config_fingerprint()
    )


NEW_TOOL_PROFILES = {
    "get_tool_guide": set(McpProfile),
    "get_column_statistics": {McpProfile.OPTIMIZER, McpProfile.SANDBOX, McpProfile.TRIAGE},
    "get_memory_pressure": {McpProfile.TRIAGE},
    "get_elastic_pool_pressure": {McpProfile.TRIAGE},
    "get_failed_executions": {McpProfile.TRIAGE},
    "get_columnstore_health": {McpProfile.TRIAGE, McpProfile.INDEX_REVIEW},
}


@pytest.mark.parametrize("tool_name", sorted(NEW_TOOL_PROFILES))
def test_new_tool_is_registered_for_exactly_its_profiles(tool_name: str) -> None:
    assert tool_name in TOOL_GROUPS
    assert TOOL_GROUPS[tool_name] is not ToolGroup.ADMIN
    profiles = {
        profile for profile, allowed in PROFILE_TOOL_ALLOWLISTS.items() if tool_name in allowed
    }
    assert profiles == NEW_TOOL_PROFILES[tool_name]


def test_new_tools_follow_their_tool_group_without_a_profile(base_env) -> None:
    # An unprofiled server exposes a tool through its group. The deep
    # diagnostics belong to the performance group, like the other DMV tools.
    core_only = load_server_config(["--azure-sql-tool-groups", "core"])
    performance_only = load_server_config(["--azure-sql-tool-groups", "performance"])
    diagnostics = (
        "get_memory_pressure",
        "get_elastic_pool_pressure",
        "get_columnstore_health",
        "get_failed_executions",
    )

    assert core_only.profile is None
    assert all(core_only.is_tool_enabled(name) for name in ("get_tool_guide", "get_column_statistics"))
    assert not any(core_only.is_tool_enabled(name) for name in diagnostics)
    assert all(performance_only.is_tool_enabled(name) for name in diagnostics)
