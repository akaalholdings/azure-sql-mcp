from __future__ import annotations

import json

from azure_sql_mcp.learning_cli import build_export_pack, main
from azure_sql_mcp.learning_contracts import LessonV1
from azure_sql_mcp.learning_store import LearningStore


def make_lesson(lesson_id: str = "lesson-1", **overrides) -> LessonV1:
    values = {
        "lesson_id": lesson_id,
        "learning_key": "tactic-1",
        "trigger": {"kind": "candidate"},
        "action": {"kind": "review"},
        "preconditions": {"kind": "bounded"},
        "counterexamples": ({"kind": "bounded-risk"},),
        "required_evidence": ("evidence-1",),
        "applicable_skills": ("sql_optimizer",),
        "applicable_scopes": ({"database_fingerprint": "db-1", "runtime_compatibility_fingerprint": "runtime-compat-1"},),
        "query_fingerprints": ("query-1", "query-2"),
        "support_refs": ("review-1",),
        "reviewer": None,
        "support_session_ids": ("session-1", "session-2"),
        "support_query_fingerprints": ("query-1", "query-2"),
    }
    values.update(overrides)
    if values.get("status") in {"active", "quarantined", "superseded", "retired"}:
        if not values.get("reviewer"):
            values["reviewer"] = "maintainer"
        values.setdefault("reviewed_at_utc", "2026-01-01T00:00:00+00:00")
        values.setdefault("created_at_utc", "2026-01-01T00:00:00+00:00")
        values.setdefault("updated_at_utc", "2026-01-01T00:01:00+00:00")
    return LessonV1(**values)


def test_cli_named_reviewer_optimistic_lifecycle_and_deterministic_pack(tmp_path, capsys) -> None:
    with LearningStore(state_dir=tmp_path / "source") as store:
        store.create_lesson(make_lesson(status="eligible"))
        store.create_lesson(make_lesson("lesson-2", status="proposed"))
        store.create_lesson(
            make_lesson(
                "lesson-3",
                status="eligible",
                supersedes_lesson_id="lesson-1",
            )
        )

    activate = [
        "--state-dir",
        str(tmp_path / "source"),
        "activate",
        "lesson-1",
        "--reviewer",
        "alice",
        "--expected-version",
        "0",
    ]
    assert main(activate) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "active"
    assert main(activate) == 0
    assert json.loads(capsys.readouterr().out)["version"] == 1
    output_path = tmp_path / "pack.json"
    assert main(["--state-dir", str(tmp_path / "source"), "export", "--output", str(output_path)]) == 0
    first = output_path.read_text()
    assert main(["--state-dir", str(tmp_path / "source"), "export", "--output", str(output_path)]) == 0
    assert output_path.read_text() == first
    pack = json.loads(first)
    assert pack["pack_type"] == "azure-sql-mcp-learning-pack"
    assert pack["schema_version"] == 1
    assert pack["content_hash"].startswith("sha256:")
    assert [lesson["status"] for lesson in pack["lessons"]] == ["active"]

    assert main(
        [
            "--state-dir",
            str(tmp_path / "source"),
            "activate",
            "lesson-3",
            "--reviewer",
            "alice",
            "--expected-version",
            "0",
        ]
    ) == 0
    capsys.readouterr()
    supersede = [
        "--state-dir",
        str(tmp_path / "source"),
        "supersede",
        "lesson-1",
        "--replacement",
        "lesson-3",
        "--reviewer",
        "alice",
        "--expected-version",
        "1",
    ]
    assert main(supersede) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "superseded"
    assert main(supersede) == 0
    assert json.loads(capsys.readouterr().out)["version"] == 2
    retire = [
        "--state-dir",
        str(tmp_path / "source"),
        "retire",
        "lesson-3",
        "--reviewer",
        "carol",
        "--expected-version",
        "1",
    ]
    assert main(retire) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "retired"
    assert main(retire) == 0
    assert json.loads(capsys.readouterr().out)["version"] == 2
    reject = [
        "--state-dir",
        str(tmp_path / "source"),
        "reject",
        "lesson-2",
        "--reviewer",
        "bob",
        "--expected-version",
        "0",
        "--code",
        "not-supported",
    ]
    assert main(reject) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "rejected"
    assert main(reject) == 0
    assert json.loads(capsys.readouterr().out)["version"] == 1

    with LearningStore(state_dir=tmp_path / "source") as store:
        superseded_events = store.list_events(aggregate_id="lesson-1")
        assert superseded_events[-1]["payload"]["reviewer"] == "alice"
        assert (
            superseded_events[-1]["payload"]["superseded_by_lesson_id"]
            == "lesson-3"
        )

    assert main(["--state-dir", str(tmp_path / "imported"), "import", str(output_path)]) == 0
    imported_output = capsys.readouterr().out
    assert json.loads(imported_output)["imported"] == 1
    assert main(["--state-dir", str(tmp_path / "imported"), "import", str(output_path)]) == 0
    assert json.loads(capsys.readouterr().out)["imported"] == 0
    assert main(["--state-dir", str(tmp_path / "imported"), "list"]) == 0
    imported = json.loads(capsys.readouterr().out)["lessons"]
    assert {item["status"] for item in imported} == {"proposed"}
    assert {item["proposal_kind"] for item in imported} == {"imported"}
    assert imported[0]["source_provenance"]["content_hash"] == pack["content_hash"]


def test_cli_rejects_tampered_pack(tmp_path, capsys) -> None:
    lesson = make_lesson(status="active", reviewer="alice", reviewed_at_utc="2026-01-01T00:00:00+00:00")
    pack = build_export_pack([lesson])
    pack["lessons"][0]["action"]["kind"] = "tampered"
    path = tmp_path / "tampered.json"
    path.write_text(json.dumps(pack), encoding="utf-8")
    assert main(["--state-dir", str(tmp_path / "state"), "import", str(path)]) == 2
    assert "content hash" in capsys.readouterr().err
