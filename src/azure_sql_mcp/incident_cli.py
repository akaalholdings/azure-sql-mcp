"""Local owner CLI that exports the redacted incident fix backlog."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .config import INCIDENT_ENV_DIR as ENV_DIR
from .incident_log import (
    DEFAULT_STATE_DIR,
    IncidentSettings,
    build_backlog,
    render_markdown,
    resolve_incident_dir,
    scrub_terms_from_env,
    summarize_backlog,
)


class IncidentCliError(ValueError):
    """Raised for an incident directory the CLI cannot read."""


def _bounded(low: int, high: int):
    def parse(raw: str) -> int:
        try:
            value = int(raw)
        except ValueError:
            raise argparse.ArgumentTypeError(f"must be an integer from {low} to {high}") from None
        if not low <= value <= high:
            raise argparse.ArgumentTypeError(f"must be an integer from {low} to {high}")
        return value

    return parse


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="azure-sql-mcp-incidents",
        description="Export the local, redacted incident backlog for review and filing.",
    )
    location = parser.add_mutually_exclusive_group()
    location.add_argument(
        "--incident-dir",
        default=None,
        help="Incident directory (defaults to AZURE_SQL_INCIDENT_DIR, else <state dir>/incidents).",
    )
    location.add_argument(
        "--state-dir",
        default=None,
        help="Performance state directory (defaults to AZURE_SQL_PERFORMANCE_STATE_DIR or ~/.azure-sql-mcp/state).",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    export_parser = commands.add_parser("export", help="Write the grouped backlog as Markdown or JSON.")
    summary_parser = commands.add_parser(
        "summary", help="Print priorities, counts, titles and fingerprints."
    )
    for command in (export_parser, summary_parser):
        command.add_argument("--since-days", type=_bounded(1, 90), default=14)
        command.add_argument("--min-priority", choices=("P1", "P2", "P3", "P4"), default="P3")
        command.add_argument("--max-items", type=_bounded(1, 200), default=50)
    export_parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    export_parser.add_argument("--output", default="-", help="Output path, or - for stdout.")
    export_parser.add_argument(
        "--include-summaries",
        action="store_true",
        help="Add agents' report_stuck summaries; they may name people or objects.",
    )
    summary_parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def _locations(args: argparse.Namespace) -> tuple[Path, Path | None]:
    state_dir = args.state_dir or os.environ.get("AZURE_SQL_PERFORMANCE_STATE_DIR") or DEFAULT_STATE_DIR
    if args.incident_dir:
        incident_dir: Path | None = Path(args.incident_dir).expanduser()
    elif args.state_dir:
        incident_dir = Path(args.state_dir).expanduser() / "incidents"
    else:
        incident_dir = resolve_incident_dir(
            IncidentSettings(directory=os.environ.get(ENV_DIR) or None),
            state_dir,
        )
    if incident_dir is None:
        raise IncidentCliError(
            "the state directory is :memory:; set --incident-dir or AZURE_SQL_INCIDENT_DIR."
        )
    if incident_dir.exists() and (
        not incident_dir.is_dir() or not os.access(incident_dir, os.R_OK | os.X_OK)
    ):
        raise IncidentCliError(f"cannot read incident directory {incident_dir}.")
    # An explicit incident dir may be a copy from another host: do not mix in
    # this host's workflow state.
    if args.incident_dir or state_dir == ":memory:":
        return incident_dir, None
    return incident_dir, Path(state_dir).expanduser() / "performance.sqlite3"


def _write(path: str, text: str) -> None:
    if path == "-":
        sys.stdout.write(text)
        return
    try:
        Path(path).write_text(text, encoding="utf-8")
    except OSError as exc:
        raise IncidentCliError(f"could not write {path}.") from exc


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        incident_dir, database = _locations(args)
        backlog = build_backlog(
            incident_dir,
            performance_db=database,
            since_days=args.since_days,
            min_priority=args.min_priority,
            max_items=args.max_items,
            scrub_terms=scrub_terms_from_env(os.environ),
            # Agent free text stays out of the shareable export unless asked for.
            include_summaries=getattr(args, "include_summaries", False),
        )
        if args.command == "export":
            if args.format == "json":
                _write(args.output, json.dumps(backlog, indent=2, sort_keys=True) + "\n")
            else:
                _write(args.output, render_markdown(backlog))
        elif args.format == "json":
            print(json.dumps(summarize_backlog(backlog), sort_keys=True, separators=(",", ":")))
        else:
            totals = backlog["totals"]
            print(
                f"Window: {backlog['window']['since_utc']} to {backlog['window']['until_utc']}. "
                f"Items: {totals['items']} shown, {totals['below_min_priority']} below "
                f"{args.min_priority}, {totals['withheld']} withheld."
            )
            for item in backlog["items"]:
                print(f"[{item['priority']}] {item['count']}x {item['title']} ({item['fingerprint']})")
        return 0
    except IncidentCliError as exc:
        print(f"azure-sql-mcp-incidents: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
