from __future__ import annotations

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "azure_live_acceptance.py"


def _load():
    spec = importlib.util.spec_from_file_location("azure_live_acceptance", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_live_acceptance_refuses_without_explicit_disposable_confirmation(capsys) -> None:
    # The run creates and drops objects, so it must never start by accident.
    assert _load().main(["--database", "appdb"]) == 2
    assert "--confirm-disposable-database" in capsys.readouterr().err


def test_live_acceptance_only_touches_its_own_schema() -> None:
    module = _load()

    assert module.SCHEMA == "mcp_accept"
    for statement in (module.CREATE_TABLES_SQL, module.LOAD_SQL):
        for line in statement.splitlines():
            if line.strip().upper().startswith(("CREATE TABLE", "INSERT ", "UPDATE STATISTICS")):
                assert "[mcp_accept]" in line, line
