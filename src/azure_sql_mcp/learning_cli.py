"""Local maintainer CLI for evidence-governed learning state."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

from .learning_contracts import CONTRACT_VERSION, LessonV1, utc_now
from .learning_store import (
    ContractNotFoundError,
    LearningStore,
    LearningStoreError,
)


PACK_TYPE = "azure-sql-mcp-learning-pack"
PACK_SCHEMA_VERSION = 1
_PACK_KEYS = frozenset({"pack_type", "schema_version", "provenance", "lessons", "content_hash"})


class LearningCliError(ValueError):
    """Raised for invalid maintainer CLI input or pack data."""


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _pack_without_hash(lessons: Sequence[LessonV1]) -> dict[str, Any]:
    active_lessons = [lesson for lesson in lessons if lesson.status == "active"]
    return {
        "pack_type": PACK_TYPE,
        "schema_version": PACK_SCHEMA_VERSION,
        "provenance": {
            "contract_version": CONTRACT_VERSION,
            "producer": "azure-sql-mcp-learning",
            "source": "local-owner-only-learning-store",
        },
        "lessons": [lesson.to_dict() for lesson in sorted(active_lessons, key=lambda item: item.lesson_id)],
    }


def _content_hash(payload: dict[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def build_export_pack(lessons: Sequence[LessonV1]) -> dict[str, Any]:
    payload = _pack_without_hash(lessons)
    return {**payload, "content_hash": _content_hash(payload)}


def _validate_pack(payload: Any) -> list[LessonV1]:
    if not isinstance(payload, dict):
        raise LearningCliError("Learning pack must be a JSON object.")
    unknown = set(payload) - _PACK_KEYS
    if unknown:
        raise LearningCliError(f"Learning pack has unknown field(s): {', '.join(sorted(unknown))}.")
    if payload.get("pack_type") != PACK_TYPE or payload.get("schema_version") != PACK_SCHEMA_VERSION:
        raise LearningCliError("Unsupported learning pack type or schema version.")
    provenance = payload.get("provenance")
    if not isinstance(provenance, dict) or set(provenance) != {"contract_version", "producer", "source"} or provenance.get("contract_version") != CONTRACT_VERSION or provenance.get("producer") != "azure-sql-mcp-learning" or provenance.get("source") != "local-owner-only-learning-store":
        raise LearningCliError("Learning pack provenance is invalid.")
    lessons_payload = payload.get("lessons")
    if not isinstance(lessons_payload, list):
        raise LearningCliError("Learning pack lessons must be a list.")
    content_hash = payload.get("content_hash")
    if not isinstance(content_hash, str) or content_hash != _content_hash({key: payload[key] for key in payload if key != "content_hash"}):
        raise LearningCliError("Learning pack content hash does not validate.")
    lessons = [LessonV1.from_dict(item) for item in lessons_payload]
    if any(lesson.status != "active" for lesson in lessons):
        raise LearningCliError("Learning packs may contain active lessons only.")
    if [lesson.lesson_id for lesson in lessons] != sorted(lesson.lesson_id for lesson in lessons):
        raise LearningCliError("Learning pack lessons must be sorted by lesson_id.")
    if len({lesson.lesson_id for lesson in lessons}) != len(lessons):
        raise LearningCliError("Learning pack contains duplicate lesson IDs.")
    return lessons


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="azure-sql-mcp-learning", description="Maintain local evidence-governed learning state.")
    parser.add_argument("--state-dir", default=None, help="Owner-only state directory (defaults to ~/.azure-sql-mcp/state).")
    parser.add_argument("--db-path", default=None, help="Explicit learning SQLite path, primarily for local tests.")
    commands = parser.add_subparsers(dest="command", required=True)

    list_parser = commands.add_parser("list", help="List lessons deterministically.")
    list_parser.add_argument("--status", choices=("proposed", "eligible", "active", "quarantined", "superseded", "retired", "rejected"))

    show_parser = commands.add_parser("show", help="Show one lesson.")
    show_parser.add_argument("lesson_id")

    activate_parser = commands.add_parser(
        "activate",
        aliases=["approve"],
        help="Activate an eligible, urgent, or imported lesson after maintainer review.",
    )
    activate_parser.add_argument("lesson_id")
    activate_parser.add_argument("--reviewer", required=True)
    activate_parser.add_argument("--expected-version", type=int, required=True)

    reject_parser = commands.add_parser("reject", help="Reject one proposal with an audit code.")
    reject_parser.add_argument("lesson_id")
    reject_parser.add_argument("--code", default="maintainer-rejected")
    reject_parser.add_argument("--reviewer", required=True)
    reject_parser.add_argument("--expected-version", type=int, required=True)

    retire_parser = commands.add_parser(
        "retire",
        aliases=["revoke"],
        help="Retire one active or quarantined lesson.",
    )
    retire_parser.add_argument("lesson_id")
    retire_parser.add_argument("--reviewer", required=True)
    retire_parser.add_argument("--expected-version", type=int, required=True)

    supersede_parser = commands.add_parser(
        "supersede",
        help="Supersede one active or quarantined lesson with an active replacement.",
    )
    supersede_parser.add_argument("lesson_id")
    supersede_parser.add_argument("--replacement", required=True)
    supersede_parser.add_argument("--reviewer", required=True)
    supersede_parser.add_argument("--expected-version", type=int, required=True)

    export_parser = commands.add_parser("export", help="Export a deterministic schema-versioned pack.")
    export_parser.add_argument("--output", default="-", help="Output path, or - for stdout.")

    import_parser = commands.add_parser("import", help="Validate and import a learning pack as inactive proposals.")
    import_parser.add_argument("input", help="Input path, or - for stdin.")
    return parser


def _json_output(value: Any) -> None:
    print(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False))


def _transition_key(command: str, lesson_id: str, expected_version: int) -> str:
    return f"cli:{command}:{lesson_id}:{expected_version}"


def _read_json(path: str) -> Any:
    if path == "-":
        return json.load(sys.stdin)
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise LearningCliError(f"Could not read learning pack {path}.") from exc
    except json.JSONDecodeError as exc:
        raise LearningCliError("Learning pack is not valid JSON.") from exc


def _write_json(path: str, payload: Any) -> None:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    if path == "-":
        sys.stdout.write(serialized)
        return
    try:
        Path(path).write_text(serialized, encoding="utf-8")
    except OSError as exc:
        raise LearningCliError(f"Could not write learning pack {path}.") from exc


def _import_lessons(store: LearningStore, lessons: Sequence[LessonV1], *, source_provenance: Mapping[str, Any]) -> int:
    imported = 0
    provenance = dict(source_provenance)
    for lesson in lessons:
        existing = None
        try:
            existing = store.get_lesson(lesson.lesson_id)
        except ContractNotFoundError:
            pass
        if existing is not None:
            if existing.to_json() == lesson.to_json() or (
                existing.status == "proposed"
                and existing.proposal_kind == "imported"
                and existing.source_provenance == provenance
            ):
                continue
            raise LearningCliError(f"Lesson {lesson.lesson_id} already exists with different content.")
        # Import cannot activate, revoke, or supersede state.  It preserves all
        # provenance and evidence references while requiring local approval.
        now = utc_now()
        imported_lesson = replace(lesson, status="proposed", proposal_kind="imported", reviewer=None, reviewed_at_utc=None, rejection_code=None, rejected_by=None, rejected_at_utc=None, superseded_by_lesson_id=None, source_provenance=provenance, updated_at_utc=now, status_changed_at_utc=now, version=0)
        store.create_lesson(imported_lesson, idempotency_key=f"import:{lesson.lesson_id}")
        imported += 1
    return imported


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        with LearningStore(state_dir=args.state_dir, db_path=args.db_path) as store:
            if args.command == "list":
                _json_output({"lessons": [lesson.to_dict() for lesson in store.list_lessons(status=args.status)]})
            elif args.command == "show":
                _json_output(store.get_lesson(args.lesson_id).to_dict())
            elif args.command in {"activate", "approve"}:
                _json_output(
                    store.transition_lesson(
                        args.lesson_id,
                        "active",
                        expected_version=args.expected_version,
                        actor="cli",
                        reviewer=args.reviewer,
                        idempotency_key=_transition_key(
                            "activate", args.lesson_id, args.expected_version
                        ),
                    ).to_dict()
                )
            elif args.command == "reject":
                _json_output(
                    store.transition_lesson(
                        args.lesson_id,
                        "rejected",
                        expected_version=args.expected_version,
                        actor="cli",
                        reviewer=args.reviewer,
                        rejection_code=args.code,
                        idempotency_key=_transition_key(
                            "reject", args.lesson_id, args.expected_version
                        ),
                    ).to_dict()
                )
            elif args.command in {"retire", "revoke"}:
                _json_output(
                    store.transition_lesson(
                        args.lesson_id,
                        "retired",
                        expected_version=args.expected_version,
                        actor="cli",
                        reviewer=args.reviewer,
                        idempotency_key=_transition_key(
                            "retire", args.lesson_id, args.expected_version
                        ),
                    ).to_dict()
                )
            elif args.command == "supersede":
                _json_output(
                    store.transition_lesson(
                        args.lesson_id,
                        "superseded",
                        expected_version=args.expected_version,
                        actor="cli",
                        reviewer=args.reviewer,
                        superseded_by_lesson_id=args.replacement,
                        idempotency_key=_transition_key(
                            "supersede", args.lesson_id, args.expected_version
                        ),
                    ).to_dict()
                )
            elif args.command == "export":
                lessons = store.list_lessons(status="active")
                _write_json(args.output, build_export_pack(lessons))
            elif args.command == "import":
                pack = _read_json(args.input)
                lessons = _validate_pack(pack)
                imported = _import_lessons(
                    store,
                    lessons,
                    source_provenance={
                        "pack_type": pack["pack_type"],
                        "schema_version": pack["schema_version"],
                        "content_hash": pack["content_hash"],
                        "source": pack["provenance"]["source"],
                    },
                )
                _json_output({"imported": imported})
            return 0
    except (LearningStoreError, LearningCliError, KeyError, ValueError) as exc:
        print(f"azure-sql-mcp-learning: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
