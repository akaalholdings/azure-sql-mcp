#!/usr/bin/env python3
"""Check local Markdown link targets without making network requests."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(r"(?<!!)\[[^\]]+\]\((?P<target>[^)]*)\)")
SKIP_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "build",
    "dist",
}


def _target(raw: str) -> str:
    value = raw.strip()
    if value.startswith("<") and ">" in value:
        return value[1 : value.index(">")]
    return value.split(maxsplit=1)[0] if value else value


def main() -> int:
    issues: list[tuple[Path, int]] = []
    checked = 0
    for path in sorted(ROOT.rglob("*.md")):
        relative = path.relative_to(ROOT)
        if any(part in SKIP_PARTS for part in relative.parts):
            continue
        if relative.as_posix() == "docs/plan-ultimate-sql-tuner.md":
            continue
        checked += 1
        in_fence = False
        for line_number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            if re.match(r"^\s*(?:```|~~~)", line):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            for match in LINK.finditer(line):
                target = _target(match.group("target"))
                parsed = urlsplit(target)
                if (
                    not target
                    or parsed.scheme
                    or target.startswith(("#", "//"))
                ):
                    continue
                candidate = (path.parent / unquote(parsed.path)).resolve()
                if not candidate.exists():
                    issues.append((relative, line_number))
    if issues:
        for path, line_number in issues:
            print(f"broken-local-markdown-link {path}:{line_number}")
        return 1
    print(f"local Markdown links passed ({checked} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
