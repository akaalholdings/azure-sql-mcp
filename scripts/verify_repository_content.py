#!/usr/bin/env python3
"""Fail on committed credentials, runtime knowledge links, or local user paths.

Findings contain only detector names and file locations. Secret values are
never printed.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
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


@dataclass(frozen=True)
class Finding:
    detector: str
    path: Path
    line: int


SECRET_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "private-key-block",
        re.compile(r"-----BEGIN [A-Z0-9 _-]{1,80}PRIVATE KEY-----"),
    ),
    ("aws-access-key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("github-token", re.compile(r"\b(?:gh[pousr]|github_pat)_[A-Za-z0-9_]{20,}\b")),
    (
        "openai-key",
        re.compile(
            r"(?<![A-Za-z0-9_-])sk-(?:[A-Za-z0-9]+-)?"
            r"[A-Za-z0-9_-]{20,}(?![A-Za-z0-9_-])"
        ),
    ),
    ("google-api-key", re.compile(r"\bAIza[0-9A-Za-z_-]{30,}\b")),
    ("slack-token", re.compile(r"\bxox[baprs]-[0-9A-Za-z-]{20,}\b")),
    ("azure-sas-signature", re.compile(r"(?:[?&])sig=[A-Za-z0-9%+/=]{16,}")),
)
SECRET_ASSIGNMENT = re.compile(
    r"(?i)(?:password|passwd|pwd|secret|api[_-]?key|access[_-]?token|bearer)"
    r"[A-Z0-9_-]*\s*[:=]\s*(['\"])(?P<value>[^'\"]{8,})\1"
)
ENV_SECRET_ASSIGNMENT = re.compile(
    r"^\s*(?:export\s+)?[A-Z][A-Z0-9_]*"
    r"(?:PASSWORD|PASSWD|PWD|SECRET|TOKEN|API[_-]?KEY)"
    r"\s*=\s*(?P<value>[^\s#;]+)"
)
KNOWLEDGE_LINK = re.compile(
    r"https?://(?:learn\.microsoft\.com|aka\.ms|techcommunity\.microsoft\.com|"
    r"stackoverflow\.com|sqlperformance\.com|brentozar\.com)(?:/|\b)",
    re.IGNORECASE,
)
LOCAL_USER_PATH = re.compile(r"(?:^|[\s`'\"])/(?:Users|home)/[^/\s`'\"]+/")


def _repository_files() -> list[Path]:
    output = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "-co", "--exclude-standard", "-z"],
        check=True,
        capture_output=True,
    ).stdout
    return sorted(
        ROOT / item.decode("utf-8")
        for item in output.split(b"\0")
        if item and (ROOT / item.decode("utf-8")).is_file()
    )


def _placeholder(value: str) -> bool:
    normalized = value.strip().strip("'\"").casefold()
    return (
        not normalized
        or normalized.startswith(
            (
                "${",
                "$(",
                "<",
                "...",
                "change-me",
                "change_me",
                "dummy",
                "example",
                "localtest-",
                "placeholder",
                "redacted",
                "replace-with",
                "replace_with",
                "test-",
                "your-",
                "your_",
            )
        )
        or "random-token" in normalized
        or normalized in {"sql-password", "testpass", "p@ssw0rd!", "test-password"}
    )


def _read_text_lines(path: Path) -> list[str] | None:
    try:
        content = path.read_bytes()
    except OSError:
        return None
    if b"\0" in content or any(
        byte < 32 and byte not in {9, 10, 13} for byte in content
    ):
        return None
    try:
        return content.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        return None


def scan() -> list[Finding]:
    findings: list[Finding] = []
    for path in _repository_files():
        relative = path.relative_to(ROOT)
        if any(part in SKIP_PARTS for part in relative.parts):
            continue
        if path.name == "verify_repository_content.py":
            continue
        if path.name == ".env" or (
            path.name.startswith(".env.") and path.name != ".env.example"
        ):
            findings.append(Finding("credential-file", relative, 1))
            continue
        lines = _read_text_lines(path)
        if lines is None:
            continue
        for line_number, line in enumerate(lines, start=1):
            for detector, pattern in SECRET_PATTERNS:
                if pattern.search(line):
                    findings.append(Finding(detector, relative, line_number))
            assignment = SECRET_ASSIGNMENT.search(line)
            env_assignment = ENV_SECRET_ASSIGNMENT.search(line)
            if (
                assignment
                and not _placeholder(assignment.group("value"))
                or env_assignment
                and not _placeholder(env_assignment.group("value"))
            ):
                findings.append(Finding("secret-assignment", relative, line_number))
            if KNOWLEDGE_LINK.search(line):
                findings.append(
                    Finding("external-knowledge-reference", relative, line_number)
                )
            if LOCAL_USER_PATH.search(line):
                findings.append(Finding("local-user-path", relative, line_number))
    return findings


def main() -> int:
    findings = scan()
    if findings:
        for finding in findings:
            print(f"{finding.detector} {finding.path}:{finding.line}")
        return 1
    print("repository content verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
