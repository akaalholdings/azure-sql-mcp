"""The bundled macOS ODBC driver must find an OpenSSL it can load.

Live (2026-11-04): Homebrew moved /opt/homebrew/opt/openssl to OpenSSL 4. The
driver in mssql-python 1.10 loads libssl from that fixed prefix and accepts only
OpenSSL 1.0, 1.1 or 3, so every connection failed with "OpenSSL library could
not be loaded" and the MCP could not reach any database from Claude Desktop or
Codex.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp import driver_openssl
from azure_sql_mcp.driver_openssl import REEXEC_MARKER
from azure_sql_mcp.driver_openssl import decide
from azure_sql_mcp.driver_openssl import ensure_driver_openssl


def _brew(root: Path, name: str, major: str) -> Path:
    """A Homebrew opt/<name>/lib with libssl.dylib -> libssl.<major>.dylib."""

    lib = root / "opt" / name / "lib"
    lib.mkdir(parents=True)
    (lib / f"libssl.{major}.dylib").write_bytes(b"")
    (lib / "libssl.dylib").symlink_to(f"libssl.{major}.dylib")
    return lib


def _decide(root: Path, **env: str) -> Any:
    return decide(platform="darwin", machine="arm64", environ=env, brew_prefixes={"arm64": root})


def test_openssl_4_default_link_reexecs_with_openssl_3(tmp_path: Path) -> None:
    _brew(tmp_path, "openssl", "4")
    good = _brew(tmp_path, "openssl@3", "3")

    decision = _decide(tmp_path)

    assert decision.action == "reexec"
    assert decision.library_dir == str(good)


@pytest.mark.parametrize("major", ["3", "1.1"])
def test_a_supported_default_link_needs_nothing(tmp_path: Path, major: str) -> None:
    _brew(tmp_path, "openssl", major)

    assert _decide(tmp_path).action == "none"


def test_openssl_4_without_openssl_3_names_the_fix(tmp_path: Path) -> None:
    _brew(tmp_path, "openssl", "4")

    decision = _decide(tmp_path)

    assert decision.action == "missing"
    assert "brew install openssl@3" in decision.message


def test_an_existing_dyld_path_with_openssl_3_is_left_alone(tmp_path: Path) -> None:
    _brew(tmp_path, "openssl", "4")
    good = _brew(tmp_path, "openssl@3", "3")

    assert _decide(tmp_path, DYLD_LIBRARY_PATH=f"/somewhere:{good}").action == "none"


def test_no_reexec_loop_when_the_marker_is_set(tmp_path: Path) -> None:
    _brew(tmp_path, "openssl", "4")
    _brew(tmp_path, "openssl@3", "3")

    decision = _decide(tmp_path, **{REEXEC_MARKER: "1"})

    assert decision.action == "missing"
    assert "did not take effect" in decision.message


@pytest.mark.parametrize("platform", ["linux", "win32"])
def test_other_platforms_are_untouched(tmp_path: Path, platform: str) -> None:
    _brew(tmp_path, "openssl", "4")

    decision = decide(platform=platform, machine="x86_64", environ={}, brew_prefixes={"x86_64": tmp_path})

    assert decision.action == "none"


def test_intel_uses_its_own_prefix(tmp_path: Path) -> None:
    _brew(tmp_path, "openssl", "4")
    good = _brew(tmp_path, "openssl@3", "3")

    decision = decide(platform="darwin", machine="x86_64", environ={}, brew_prefixes={"x86_64": tmp_path})

    assert decision.action == "reexec"
    assert decision.library_dir == str(good)


def test_ensure_reexecs_the_same_command_with_the_library_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _brew(tmp_path, "openssl", "4")
    good = _brew(tmp_path, "openssl@3", "3")
    calls: list[tuple[str, list[str], dict[str, str]]] = []

    def fake_execve(path: str, args: list[str], env: dict[str, str]) -> None:
        calls.append((path, args, env))

    monkeypatch.setattr(driver_openssl.os, "execve", fake_execve)
    monkeypatch.setattr(driver_openssl.sys, "executable", "/venv/bin/python")
    monkeypatch.setattr(driver_openssl.sys, "orig_argv", ["python", "/venv/bin/azure-sql-mcp", "--transport", "stdio"])

    ensure_driver_openssl(
        platform="darwin",
        machine="arm64",
        environ={"DYLD_LIBRARY_PATH": "/existing", "AZURE_SQL_SERVER": "s"},
        brew_prefixes={"arm64": tmp_path},
    )

    [(path, args, env)] = calls
    assert path == "/venv/bin/python"
    assert args == ["/venv/bin/python", "/venv/bin/azure-sql-mcp", "--transport", "stdio"]
    assert env["DYLD_LIBRARY_PATH"] == f"{good}:/existing"
    assert env[REEXEC_MARKER] == "1"
    assert env["AZURE_SQL_SERVER"] == "s"


def test_ensure_warns_and_continues_when_no_usable_openssl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _brew(tmp_path, "openssl", "4")
    monkeypatch.setattr(driver_openssl.os, "execve", lambda *a: pytest.fail("must not re-exec"))

    decision = ensure_driver_openssl(
        platform="darwin", machine="arm64", environ={}, brew_prefixes={"arm64": tmp_path}
    )

    assert decision.action == "missing"
    assert "brew install openssl@3" in capsys.readouterr().err


def test_the_server_entry_point_fixes_openssl_before_it_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    # Claude Desktop and Codex both launch "uv run azure-sql-mcp" -> azure_sql_mcp.main.
    import azure_sql_mcp
    import azure_sql_mcp.server as server

    order: list[str] = []
    monkeypatch.setattr(driver_openssl, "ensure_driver_openssl", lambda: order.append("openssl"))
    monkeypatch.setattr(server, "main", lambda argv=None: order.append("server"))

    azure_sql_mcp.main([])

    assert order == ["openssl", "server"]


@pytest.mark.parametrize("orig_argv", [["python", "-"], ["python"], ["python", "-I", "-"]])
def test_a_program_read_from_stdin_is_never_reexecuted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    orig_argv: list[str],
) -> None:
    # Live: "python - <<EOF" re-executed with stdin already consumed, ran an
    # empty program and exited 0 with no output. Warn instead.
    _brew(tmp_path, "openssl", "4")
    good = _brew(tmp_path, "openssl@3", "3")
    monkeypatch.setattr(driver_openssl.os, "execve", lambda *a: pytest.fail("must not re-exec"))
    monkeypatch.setattr(driver_openssl.sys, "orig_argv", orig_argv)

    decision = ensure_driver_openssl(
        platform="darwin", machine="arm64", environ={}, brew_prefixes={"arm64": tmp_path}
    )

    assert decision.action == "missing"
    err = capsys.readouterr().err
    assert f"DYLD_LIBRARY_PATH={good}" in err


def test_dyld_entries_count_in_order_and_an_earlier_openssl_4_still_wins(tmp_path: Path) -> None:
    # dyld uses the first DYLD_LIBRARY_PATH entry holding libssl.dylib.
    _brew(tmp_path, "openssl", "4")
    good = _brew(tmp_path, "openssl@3", "3")
    shadow = tmp_path / "lib"
    shadow.mkdir()
    (shadow / "libssl.4.dylib").write_bytes(b"")
    (shadow / "libssl.dylib").symlink_to("libssl.4.dylib")

    decision = _decide(tmp_path, DYLD_LIBRARY_PATH=f"{shadow}:{good}")

    assert decision.action == "reexec"


def test_the_reexec_marker_is_not_inherited_by_child_processes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A child launched through /bin/sh loses DYLD_LIBRARY_PATH (SIP) but would
    # keep the marker, then refuse to fix itself. The re-executed process drops it.
    _brew(tmp_path, "openssl", "4")
    good = _brew(tmp_path, "openssl@3", "3")
    monkeypatch.setenv("DYLD_LIBRARY_PATH", str(good))
    monkeypatch.setenv(REEXEC_MARKER, "1")

    decision = ensure_driver_openssl(platform="darwin", machine="arm64", brew_prefixes={"arm64": tmp_path})

    assert decision.action == "none"
    import os

    assert REEXEC_MARKER not in os.environ


def test_index_history_cli_fixes_openssl_before_it_connects(monkeypatch: pytest.MonkeyPatch) -> None:
    from azure_sql_mcp import index_history_cli

    order: list[str] = []
    monkeypatch.setattr(driver_openssl, "ensure_driver_openssl", lambda: order.append("openssl"))
    monkeypatch.setattr(index_history_cli, "build_parser", lambda: order.append("parse") or pytest.fail("stop"))

    with pytest.raises(BaseException):
        index_history_cli.main([])

    assert order[:2] == ["openssl", "parse"]


def test_python_m_server_goes_through_the_guarded_entry_point() -> None:
    # "python -m azure_sql_mcp.server" is a common MCP config form.
    import inspect

    from azure_sql_mcp import server

    source = inspect.getsource(server)
    main_block = source[source.rindex('if __name__ == "__main__":') :]
    assert "ensure_driver_openssl" in main_block or "azure_sql_mcp import main" in main_block
