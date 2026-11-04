"""Let the bundled macOS ODBC driver find an OpenSSL it can load.

The msodbcsql driver inside mssql-python loads ``libssl.dylib`` from a fixed
Homebrew prefix (``/opt/homebrew/opt/openssl/lib`` on Apple Silicon,
``/usr/local/opt/openssl/lib`` on Intel) and accepts only OpenSSL 1.0, 1.1 or 3.
When Homebrew points that link at OpenSSL 4, every connection fails with
"OpenSSL library could not be loaded". dyld reads ``DYLD_LIBRARY_PATH`` only at
process start, so the server re-executes itself once with Homebrew's
``openssl@3`` first on that path. It must run before any stdio is read.
"""

from __future__ import annotations

import os
import platform as platform_module
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

REEXEC_MARKER = "AZURE_SQL_MCP_OPENSSL_REEXEC"
_BREW_PREFIXES = {"arm64": Path("/opt/homebrew"), "x86_64": Path("/usr/local")}
_SUPPORTED_MAJORS = frozenset({"1.0.0", "1.1", "3"})
_LIBSSL_NAME = re.compile(r"^libssl\.(\d+(?:\.\d+)*)\.dylib$")


@dataclass(frozen=True)
class OpenSSLDecision:
    action: str  # "none", "reexec" or "missing"
    library_dir: str | None = None
    message: str = ""


def _libssl_major(library_dir: Path) -> str | None:
    link = library_dir / "libssl.dylib"
    if not link.exists():
        return None
    match = _LIBSSL_NAME.match(link.resolve().name)
    return match.group(1) if match else None


def decide(
    *,
    platform: str | None = None,
    machine: str | None = None,
    environ: Mapping[str, str] | None = None,
    brew_prefixes: Mapping[str, Path] | None = None,
) -> OpenSSLDecision:
    platform = sys.platform if platform is None else platform
    if platform != "darwin":
        return OpenSSLDecision("none")
    machine = platform_module.machine() if machine is None else machine
    environ = os.environ if environ is None else environ
    prefix = (brew_prefixes or _BREW_PREFIXES).get(machine)
    if prefix is None:
        return OpenSSLDecision("none")
    if _libssl_major(prefix / "opt" / "openssl" / "lib") in _SUPPORTED_MAJORS:
        return OpenSSLDecision("none")
    # dyld takes the first DYLD_LIBRARY_PATH entry that holds libssl.dylib.
    # os.pathsep is ":" on macOS; it keeps the logic testable on Windows paths.
    for entry in environ.get("DYLD_LIBRARY_PATH", "").split(os.pathsep):
        major = _libssl_major(Path(entry)) if entry else None
        if major is not None:
            if major in _SUPPORTED_MAJORS:
                return OpenSSLDecision("none")
            break
    if environ.get(REEXEC_MARKER):
        return OpenSSLDecision(
            "missing",
            message=(
                "azure-sql-mcp: the OpenSSL 3 library path did not take effect after "
                "re-exec; database connections will fail. Set DYLD_LIBRARY_PATH to "
                "Homebrew's openssl@3 lib directory in the MCP launch settings."
            ),
        )
    for name in ("openssl@3", "openssl@1.1"):
        candidate = prefix / "opt" / name / "lib"
        if _libssl_major(candidate) in _SUPPORTED_MAJORS:
            return OpenSSLDecision("reexec", library_dir=str(candidate))
    return OpenSSLDecision(
        "missing",
        message=(
            "azure-sql-mcp: Homebrew's default openssl is not OpenSSL 1.1 or 3, which the "
            "bundled SQL Server ODBC driver needs, and openssl@3 is not installed; "
            "database connections will fail. Fix: brew install openssl@3, then restart "
            "the MCP server."
        ),
    )


# Interpreter options that take a separate value (python -X dev, -W error).
_OPTIONS_WITH_VALUE = frozenset({"-X", "-W", "--check-hash-based-pycs"})


def _program_reads_stdin(orig_argv: list[str]) -> bool:
    """True when Python read the program itself from stdin (python -, or no script).

    A re-exec would then run an empty program: stdin is already consumed.
    """

    arguments = iter(orig_argv[1:])
    for argument in arguments:
        if argument in ("-c", "-m"):
            return False
        if argument in _OPTIONS_WITH_VALUE:
            next(arguments, None)
            continue
        if argument == "-":
            return True
        if argument.startswith("-") and argument != "--":
            continue
        return False
    return True


def ensure_driver_openssl(
    *,
    platform: str | None = None,
    machine: str | None = None,
    environ: Mapping[str, str] | None = None,
    brew_prefixes: Mapping[str, Path] | None = None,
) -> OpenSSLDecision:
    """Re-exec once with openssl@3 on DYLD_LIBRARY_PATH when the driver needs it."""

    own_environment = environ is None
    environ = os.environ if environ is None else environ
    decision = decide(platform=platform, machine=machine, environ=environ, brew_prefixes=brew_prefixes)
    if own_environment and decision.action == "none":
        # The re-exec worked. Children must not inherit the marker: one started
        # through /bin/sh loses DYLD_LIBRARY_PATH (SIP) and must fix itself.
        os.environ.pop(REEXEC_MARKER, None)
    if decision.action == "missing":
        print(decision.message, file=sys.stderr, flush=True)
    elif decision.action == "reexec" and decision.library_dir is not None:
        if _program_reads_stdin(list(sys.orig_argv)):
            decision = OpenSSLDecision(
                "missing",
                library_dir=decision.library_dir,
                message=(
                    "azure-sql-mcp: cannot re-exec a program read from stdin; database "
                    "connections will fail. Start it with "
                    f"DYLD_LIBRARY_PATH={decision.library_dir} set."
                ),
            )
            print(decision.message, file=sys.stderr, flush=True)
            return decision
        env = dict(environ)
        existing = env.get("DYLD_LIBRARY_PATH")
        env["DYLD_LIBRARY_PATH"] = (
            f"{decision.library_dir}{os.pathsep}{existing}" if existing else decision.library_dir
        )
        env[REEXEC_MARKER] = "1"
        os.execve(sys.executable, [sys.executable, *sys.orig_argv[1:]], env)
    return decision
