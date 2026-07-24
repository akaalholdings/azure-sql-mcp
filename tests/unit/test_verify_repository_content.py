from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


_SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "verify_repository_content.py"
_SPEC = importlib.util.spec_from_file_location(
    "verify_repository_content", _SCRIPT_PATH
)
assert _SPEC is not None and _SPEC.loader is not None
verifier = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = verifier
_SPEC.loader.exec_module(verifier)


def _synthetic_openai_key(prefix: str, suffix: str) -> str:
    stem = bytes((115, 107, 45)).decode("ascii")
    return f"{stem}{prefix}-{suffix}"


def _repository_files(monkeypatch, tmp_path, *names):
    paths = []
    for name in names:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")
        paths.append(path)
    monkeypatch.setattr(verifier, "ROOT", tmp_path)
    monkeypatch.setattr(verifier, "_repository_files", lambda: paths)
    return paths


def test_extensionless_text_files_are_scanned(monkeypatch, tmp_path) -> None:
    [path] = _repository_files(monkeypatch, tmp_path, "Dockerfile")
    sample_value = _synthetic_openai_key("proj", "abc_DEF-1234567890_xyz")
    path.write_text(f"ARG DEPLOY_TOKEN={sample_value}\n", encoding="utf-8")

    findings = verifier.scan()

    assert [
        (finding.detector, finding.path.name, finding.line) for finding in findings
    ] == [
        ("openai-key", "Dockerfile", 1),
    ]


def test_binary_files_are_skipped(monkeypatch, tmp_path) -> None:
    [path] = _repository_files(monkeypatch, tmp_path, "artifact")
    sample_value = _synthetic_openai_key("proj", "abc_DEF-1234567890_xyz")
    path.write_bytes(b"\x00" + sample_value.encode("ascii") + b"\x00")

    assert verifier.scan() == []


def test_prefixed_openai_key_formats_are_detected(monkeypatch, tmp_path) -> None:
    [path] = _repository_files(monkeypatch, tmp_path, "deploy")
    project_key = _synthetic_openai_key("proj", "abc_DEF-1234567890_xyz")
    service_key = _synthetic_openai_key("svcacct", "xyz_ABC-0987654321_def")
    path.write_text(
        f"{project_key}\n{service_key}\n",
        encoding="utf-8",
    )

    assert [(finding.detector, finding.line) for finding in verifier.scan()] == [
        ("openai-key", 1),
        ("openai-key", 2),
    ]


def test_secret_assignments_in_test_paths_are_scanned(
    monkeypatch, tmp_path
) -> None:
    [path] = _repository_files(monkeypatch, tmp_path, "tests/test_fixture.py")
    variable = "".join(("API_", "TOKEN"))
    path.write_text(
        f'{variable}="production-looking-fixture-value"\n',
        encoding="utf-8",
    )

    assert [(finding.detector, finding.line) for finding in verifier.scan()] == [
        ("secret-assignment", 1),
    ]


def test_cli_reports_detector_and_location_without_secret_value(
    monkeypatch, tmp_path, capsys
) -> None:
    [path] = _repository_files(monkeypatch, tmp_path, "Makefile")
    sample_value = _synthetic_openai_key("svcacct", "xyz_ABC-0987654321_def")
    path.write_text(f"DEPLOY_TOKEN={sample_value}\n", encoding="utf-8")

    assert verifier.main() == 1
    output = capsys.readouterr().out

    assert output.splitlines() == [
        "openai-key Makefile:1",
        "secret-assignment Makefile:1",
    ]
    assert sample_value not in output
