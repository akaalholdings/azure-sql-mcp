"""Non-interactive daily index-history capture CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from .auth import AzureSqlAuthenticator
from .config import load_server_config
from .connection import AzureSqlExecutor
from .connection_pool import ConnectionPool
from .database_policy import DatabasePolicyError
from .database_policy import load_database_policy_or_deny
from .index_review import IndexReviewCollectionError
from .index_review import IndexReviewIdempotencyConflictError
from .index_review import IndexReviewIntegrityError
from .index_review import IndexReviewOutcomeUnknownError
from .index_review import IndexReviewPolicyError
from .index_review import IndexReviewSchemaError
from .index_review import IndexReviewService
from .index_review import IndexReviewWriteError
from .index_review import SqlIndexHistoryRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="azure-sql-mcp-index-history",
        description="Capture redacted Azure SQL index lifecycle telemetry.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("capture", help="Capture one daily index snapshot.")
    capture.add_argument("--database", required=True, help="Allowlisted database name.")
    capture.add_argument("--idempotency-key", default=None, help="Optional capture key.")
    capture.add_argument(
        "--output",
        dest="output",
        choices=("json", "text"),
        default="json",
        help="Output format (default: json).",
    )
    return parser


async def _capture(args: argparse.Namespace) -> dict[str, Any]:
    config = load_server_config([])
    database_name = config.validate_database_name(args.database)
    authenticator = AzureSqlAuthenticator(config)
    pool = ConnectionPool(config, authenticator)
    executor = AzureSqlExecutor(config, authenticator, pool)
    try:
        policy = load_database_policy_or_deny(config.database_policy_file)
        repository = SqlIndexHistoryRepository(executor, policy)
        service = IndexReviewService(
            executor,
            repository,
            database_policy=policy,
        )
        result = await service.capture_snapshot(database_name, args.idempotency_key)
        return result.as_dict()
    finally:
        await pool.close_all()


def _print_result(payload: dict[str, Any], output: str) -> None:
    if output == "json":
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        return
    run = payload["run"]
    status = payload["status"]
    print(f"{status}: database={run['database_name']} run_id={run['run_id']}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        payload = asyncio.run(_capture(args))
        _print_result(payload, args.output)
        return 0
    except (
        DatabasePolicyError,
        ValueError,
        IndexReviewIdempotencyConflictError,
        IndexReviewIntegrityError,
        IndexReviewPolicyError,
        IndexReviewSchemaError,
    ) as exc:
        print(f"azure-sql-mcp-index-history: {exc}", file=sys.stderr)
        return 2
    except (
        IndexReviewCollectionError,
        IndexReviewOutcomeUnknownError,
        IndexReviewWriteError,
    ) as exc:
        print(f"azure-sql-mcp-index-history: {exc}", file=sys.stderr)
        return 3
    except Exception:
        print(
            "azure-sql-mcp-index-history: collection or transaction failure.",
            file=sys.stderr,
        )
        return 3


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
