from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pytest

from azure_sql_mcp.config import parse_bool
from azure_sql_mcp.incident_cli import main
from azure_sql_mcp.incident_log import IncidentLog
from azure_sql_mcp.performance_store import PerformanceStore


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch):
    for name in (
        "AZURE_SQL_INCIDENT_DIR",
        "AZURE_SQL_PERFORMANCE_STATE_DIR",
        "AZURE_SQL_SERVER",
        "AZURE_SQL_ALLOWED_DATABASES",
        "AZURE_SQL_DEFAULT_DATABASE",
        "AZURE_SQL_USERNAME",
        "AZURE_CLIENT_ID",
        "AZURE_TENANT_ID",
    ):
        monkeypatch.delenv(name, raising=False)


def seed(directory, *errors: BaseException) -> None:
    log = IncidentLog(directory, server_version="2.6.0")
    for error in errors:
        log.end(log.begin("tune_query", {}), exc=error)
    log.close()


def package_value_error() -> ValueError:
    """A ValueError raised by package code: a caller error, P4."""

    try:
        parse_bool("not-a-bool")
    except ValueError as exc:
        return exc
    raise AssertionError


def test_export_markdown_to_stdout_exits_0(tmp_path, capsys) -> None:
    seed(tmp_path / "incidents", TypeError("boom"))
    assert main(["--incident-dir", str(tmp_path / "incidents"), "export"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("# azure-sql-mcp incident backlog\n")
    assert "### [P1] tune_query: TypeError" in out
    assert "Review before filing in a public repo." in out


def test_export_json_to_file_applies_min_priority(tmp_path) -> None:
    seed(tmp_path / "incidents", TypeError("boom"), package_value_error())
    output = tmp_path / "backlog.json"
    argv = ["--incident-dir", str(tmp_path / "incidents"), "export", "--format", "json"]
    assert main([*argv, "--min-priority", "P1", "--output", str(output)]) == 0
    backlog = json.loads(output.read_text(encoding="utf-8"))
    assert backlog["schema"] == "azure-sql-mcp-incident-backlog/1"
    assert [item["priority"] for item in backlog["items"]] == ["P1"]
    assert main([*argv, "--min-priority", "P4", "--output", str(output)]) == 0
    # An explicit incident dir never mixes in this host's workflow state.
    assert len(json.loads(output.read_text(encoding="utf-8"))["items"]) == 2


def test_state_dir_covers_incidents_and_stalled_workflows(tmp_path, capsys) -> None:
    seed(tmp_path / "incidents", TypeError("boom"))
    with PerformanceStore(tmp_path) as store:
        store.create_index_lease(
            lease_id="lease-failed",
            database_fingerprint="database-fingerprint",
            session_id="session-1",
            candidate_id="candidate-1",
            index_name="IX_Testing_synthetic",
            object_fingerprint="object-fingerprint",
            expires_at_utc="2026-01-01T00:00:00+00:00",
        )
        store.recover_index_lease("lease-failed", status="cleanup_required", expected_version=0)
    assert main(["--state-dir", str(tmp_path), "export", "--format", "json"]) == 0
    kinds = sorted(item["kind"] for item in json.loads(capsys.readouterr().out)["items"])
    assert kinds == ["stalled_workflow", "tool_error"]


def test_env_scrub_terms_withhold_leaking_items(tmp_path, capsys, monkeypatch) -> None:
    directory = tmp_path / "incidents"
    directory.mkdir()
    now = datetime.now(timezone.utc)
    legacy = {
        "schema": "azure-sql-mcp-incident/1",
        "ts_utc": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "kind": "tool_error",
        "category": "product_bug",
        "priority": "P1",
        "fingerprint": "0123456789abcdef",
        "tool": "sentineldb",
    }
    path = directory / f"incidents-{now:%Y-%m-%d}.jsonl"
    path.write_text(json.dumps(legacy) + "\n", encoding="utf-8")
    monkeypatch.setenv("AZURE_SQL_ALLOWED_DATABASES", "SentinelDb")
    assert main(["--incident-dir", str(directory), "export", "--format", "json"]) == 0
    backlog = json.loads(capsys.readouterr().out)
    assert backlog["totals"]["withheld"] == 1
    assert "sentinel" not in json.dumps(backlog).lower()


def test_missing_dir_exports_an_empty_backlog(tmp_path, capsys) -> None:
    assert main(["--incident-dir", str(tmp_path / "absent"), "export", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["items"] == []
    assert not (tmp_path / "absent").exists()


def test_summary_lists_priorities_counts_titles_and_fingerprints(tmp_path, capsys) -> None:
    seed(tmp_path / "incidents", TypeError("boom"), TypeError("boom"))
    assert main(["--incident-dir", str(tmp_path / "incidents"), "summary"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("Window: ")
    assert lines[1].startswith("[P1] 2x tune_query: TypeError")
    assert lines[1].endswith(")")


def test_bad_arguments_and_unusable_dirs_exit_2(tmp_path, capsys, monkeypatch) -> None:
    for argv in (
        ["export", "--min-priority", "P9"],
        ["export", "--since-days", "0"],
        ["export", "--max-items", "1000"],
        ["--incident-dir", "a", "--state-dir", "b", "export"],
    ):
        with pytest.raises(SystemExit) as raised:
            main(argv)
        assert raised.value.code == 2
    monkeypatch.setenv("AZURE_SQL_PERFORMANCE_STATE_DIR", ":memory:")
    assert main(["export"]) == 2
    not_a_dir = tmp_path / "file"
    not_a_dir.write_text("x", encoding="utf-8")
    assert main(["--incident-dir", str(not_a_dir), "export"]) == 2
    if os.name != "nt":
        locked = tmp_path / "locked"
        locked.mkdir(mode=0o000)
        try:
            assert main(["--incident-dir", str(locked), "export"]) == 2
        finally:
            locked.chmod(0o700)
    assert "azure-sql-mcp-incidents:" in capsys.readouterr().err


def test_agent_summaries_are_opt_in_for_the_shareable_export(tmp_path, capsys) -> None:
    # Agent free text can name people, tables, or the user's prompt.
    log = IncidentLog(tmp_path / "incidents", server_version="2.6.0")
    log.report_blocker(
        skill="sql-optimizer",
        blocker_kind="other",
        summary="User asked why Margaret Whitfield's visits load slowly",
    )
    log.close()
    argv = ["--incident-dir", str(tmp_path / "incidents"), "export", "--min-priority", "P4"]

    assert main(argv) == 0
    assert "Agent summary" not in capsys.readouterr().out
    assert main([*argv, "--include-summaries"]) == 0
    out = capsys.readouterr().out
    assert "- Agent summary: User asked why [ident] [ident] visits load slowly" in out
    assert "Whitfield" not in out
