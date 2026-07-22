from __future__ import annotations

import json
from pathlib import Path

import pytest

from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.database_policy import DatabasePolicyLoadError
from azure_sql_mcp.database_policy import DatabasePolicyValidationError
from azure_sql_mcp.database_policy import load_database_policy
from azure_sql_mcp.database_policy import load_database_policy_or_deny


def _document(**entry_overrides):
    entry = {
        "environment": "test",
        "allow_read": True,
        "allow_benchmark": True,
        "allow_test_indexes": True,
        "allow_plan_apply": True,
        "max_benchmark_executions": 4,
    }
    entry.update(entry_overrides)
    return {"version": 1, "databases": {"AppDb": entry}}


def _write_policy(tmp_path: Path, document: dict) -> Path:
    path = tmp_path / "database-policy.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_loads_version_one_policy_and_looks_up_database_case_insensitively(
    tmp_path: Path,
) -> None:
    policies = load_database_policy(_write_policy(tmp_path, _document()))

    policy = policies.require("appdb")

    assert policy.database_name == "AppDb"
    assert policy.environment == "test"
    assert policy.allow_read is True
    assert policy.allow_plan_apply is True
    assert policy.can_benchmark(4) is True
    assert policy.can_benchmark(5) is False
    assert policies.allows_test_indexes("APPDB") is True


def test_missing_database_is_denied_by_default(tmp_path: Path) -> None:
    policies = load_database_policy(_write_policy(tmp_path, _document()))

    denied = policies.policy_for("missing")

    assert denied.configured is False
    assert denied.allow_read is False
    assert denied.allow_benchmark is False
    assert denied.allow_test_indexes is False
    assert denied.allow_plan_apply is False
    assert denied.max_benchmark_executions == 0
    assert policies.allows_read("missing") is False
    assert policies.allows_benchmark("missing", 1) is False
    with pytest.raises(PermissionError, match="no configured database policy"):
        policies.require("missing")


def test_dangerous_capabilities_default_closed_when_explicitly_disabled(
    tmp_path: Path,
) -> None:
    document = _document(
        allow_benchmark=False,
        allow_test_indexes=False,
        allow_plan_apply=False,
        max_benchmark_executions=0,
    )
    policies = load_database_policy(_write_policy(tmp_path, document))

    assert policies.allows_read("appdb") is True
    assert policies.allows_benchmark("appdb", 1) is False
    assert policies.allows_test_indexes("appdb") is False
    assert policies.allows_plan_apply("appdb") is False


def test_omitted_dangerous_fields_default_closed(tmp_path: Path) -> None:
    document = {
        "version": 1,
        "databases": {
            "appdb": {
                "environment": "test",
                "allow_read": True,
            }
        },
    }

    policy = load_database_policy(_write_policy(tmp_path, document)).require("appdb")

    assert policy.allow_benchmark is False
    assert policy.allow_test_indexes is False
    assert policy.allow_plan_apply is False
    assert policy.max_benchmark_executions == 0


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"version": 2, "databases": {}}, "version"),
        ({"version": 1, "databases": []}, "databases"),
        ({"version": 1, "databases": {"appdb": {}}}, "missing required"),
        ({"version": 1, "databases": {"appdb": {**_document()["databases"]["AppDb"], "extra": 1}}}, "unknown field"),
        ({"version": 1, "databases": {"appdb": {**_document()["databases"]["AppDb"], "max_benchmark_executions": -1}}}, "non-negative"),
    ],
)
def test_rejects_invalid_schema(tmp_path: Path, document: dict, message: str) -> None:
    with pytest.raises(DatabasePolicyValidationError, match=message):
        load_database_policy(_write_policy(tmp_path, document))


def test_rejects_unknown_environment_and_non_boolean_capability(tmp_path: Path) -> None:
    unknown_environment = _document(environment="unknown")
    with pytest.raises(DatabasePolicyValidationError, match="known string"):
        load_database_policy(_write_policy(tmp_path, unknown_environment))

    non_boolean = _document(allow_plan_apply="true")
    with pytest.raises(DatabasePolicyValidationError, match="allow_plan_apply"):
        load_database_policy(_write_policy(tmp_path, non_boolean))


def test_load_errors_are_explicit_and_unconfigured_path_is_deny_all(tmp_path: Path) -> None:
    with pytest.raises(DatabasePolicyLoadError, match="could not read"):
        load_database_policy(tmp_path / "missing.json")

    policies = load_database_policy_or_deny(None)

    assert isinstance(policies, DatabasePolicySet)
    assert policies.allows_read("appdb") is False
    assert policies.allows_plan_apply("appdb") is False
