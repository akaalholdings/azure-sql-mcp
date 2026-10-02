#!/usr/bin/env python3
"""Capture the DMV column sets of one Azure SQL Database tier (owner-run, read-only).

For every DMV in ``tests/fixtures/azure_dmv/columns.json`` it runs
``SELECT TOP (0) * FROM <dmv>`` (table-valued functions with their documented
arguments, or the entry's ``capture_sql`` when the argument comes from another
DMV) and keeps only the column names from the result metadata. It writes
``tests/fixtures/azure_dmv/live/<edition>_<service_objective>.json`` with the
edition, the service objective and the column names. No rows, server, database or
pool names are written. A DMV that cannot be read is recorded as
``permission_denied``, ``invalid_object`` or ``error``, never with message text.

``--sample-governance-row`` also keeps the numeric limits of this database's
``sys.dm_user_db_resource_governance`` row (log rate, IOPS, workers, sessions),
for capacity fixtures: only the columns in ``GOVERNANCE_LIMIT_COLUMNS``, so names,
GUIDs, flags, ids and usage values are dropped. A row that cannot be read is
recorded as a category, like a DMV, and the column capture still completes.

Connection settings come from the usual AZURE_SQL_* environment; the database
must be allowlisted. Run once per tier (GP, GP serverless, BC, Hyperscale, pool,
Basic/S0/S1) and commit the files: the DMV column contract test then checks the
code against each captured tier.

Usage:
    uv run python scripts/capture_dmv_columns.py --database appdb [--sample-governance-row]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from azure_sql_mcp.azure_tier import TIER_SQL
from azure_sql_mcp.azure_tier import classify_service_tier

ROOT = Path(__file__).resolve().parents[1]
COLUMNS_PATH = ROOT / "tests" / "fixtures" / "azure_dmv" / "columns.json"
LIVE_DIR = COLUMNS_PATH.parent / "live"

GOVERNANCE_ROW_SQL = """
SELECT TOP (1) *
FROM sys.dm_user_db_resource_governance
WHERE database_id = DB_ID()
"""
# Documented limit columns of sys.dm_user_db_resource_governance; everything else
# (names, GUIDs, ids, role and type flags, usage) stays in the database.
GOVERNANCE_LIMIT_COLUMNS = (
    "dtu_limit",
    "cpu_limit",
    "max_dop",
    "max_sessions",
    "max_memory_grant",
    "max_db_max_size_in_mb",
    "log_size_in_mb",
    "max_transaction_size",
    "instance_max_log_rate",
    "instance_max_worker_threads",
    "primary_max_log_rate",
    "pool_max_log_rate",
    "primary_group_max_io",
    "pool_max_io",
    "primary_group_max_cpu",
    "primary_group_max_workers",
    "primary_pool_max_workers",
    "user_data_directory_space_quota_mb",
)


def documented_dmvs() -> dict[str, dict[str, Any]]:
    return json.loads(COLUMNS_PATH.read_text())["dmvs"]


def probe_sql(name: str, entry: dict[str, Any]) -> str:
    if entry.get("capture_sql"):
        return entry["capture_sql"]
    source = f"{name}({entry['capture_args']})" if entry.get("kind") == "function" else name
    return f"SELECT TOP (0) * FROM {source}"


def _error_category(exc: Exception) -> str:
    text = str(exc).lower()
    if "permission" in text or isinstance(exc, PermissionError):
        return "permission_denied"
    if "invalid object name" in text:
        return "invalid_object"
    return "error"


def _numeric_limits(row: dict[str, Any]) -> dict[str, int | float]:
    sample: dict[str, int | float] = {}
    for column in GOVERNANCE_LIMIT_COLUMNS:
        value = row.get(column)
        if isinstance(value, bool):
            continue
        if isinstance(value, Decimal):
            value = float(value)
        if isinstance(value, (int, float)):
            sample[column] = value
    return sample


async def capture(executor: Any, database: str, *, sample_governance_row: bool = False) -> dict[str, Any]:
    rows = await executor.fetch_all(database, TIER_SQL)
    tier_row = rows[0] if rows else {}
    tier = classify_service_tier(
        tier_row.get("edition"), tier_row.get("service_objective"), tier_row.get("elastic_pool_name")
    )
    dmvs: dict[str, dict[str, Any]] = {}
    for name, entry in documented_dmvs().items():
        try:
            results = await executor.execute_batches(database, probe_sql(name, entry))
        except Exception as exc:  # recorded as a category; the message may name the server
            dmvs[name] = {"error": _error_category(exc)}
            continue
        columns = next((list(result.columns) for result in results if result.columns), None)
        dmvs[name] = {"columns": columns} if columns is not None else {"error": "no_result_set"}
    payload: dict[str, Any] = {
        "captured_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "edition": tier.edition,
        "service_objective": tier.service_objective,
        "pooled": tier.pooled,
        "dmvs": dmvs,
    }
    if sample_governance_row:
        try:
            governance = await executor.fetch_all(database, GOVERNANCE_ROW_SQL)
        except Exception as exc:  # gated on Basic, S0, S1 and pools; the message may name the server
            payload["governance_sample"] = {"error": _error_category(exc)}
        else:
            payload["governance_sample"] = _numeric_limits(governance[0]) if governance else {}
    return payload


def write_capture(payload: dict[str, Any], directory: Path = LIVE_DIR) -> Path:
    tier = f"{payload.get('edition') or 'unknown'}_{payload.get('service_objective') or 'unknown'}"
    path = directory / f"{re.sub(r'[^a-z0-9_]+', '_', tier.lower())}.json"
    directory.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


async def run(args: argparse.Namespace) -> int:
    from azure_sql_mcp.config import load_server_config
    from azure_sql_mcp.server import AzureSqlMcpApplication

    config = load_server_config([])
    database = config.validate_database_name(args.database)
    app = AzureSqlMcpApplication(config)
    payload = await capture(app.executor, database, sample_governance_row=args.sample_governance_row)
    path = write_capture(payload, Path(args.output_dir))
    unreadable = sorted(name for name, entry in payload["dmvs"].items() if "error" in entry)
    print(json.dumps({"written": str(path), "dmvs": len(payload["dmvs"]), "unreadable": unreadable}, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", required=True, help="Allowlisted database whose tier to capture.")
    parser.add_argument("--output-dir", default=str(LIVE_DIR), help="Directory for <tier>.json.")
    parser.add_argument(
        "--sample-governance-row",
        action="store_true",
        help="Also keep this database's numeric resource-governance limits (no names).",
    )
    args = parser.parse_args(argv)
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
