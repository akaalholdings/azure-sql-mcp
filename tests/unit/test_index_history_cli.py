from __future__ import annotations

from types import SimpleNamespace

import pytest

from azure_sql_mcp import index_history_cli
from azure_sql_mcp.index_review import IndexReviewCollectionError
from azure_sql_mcp.index_review import IndexReviewIdempotencyConflictError
from azure_sql_mcp.index_review import IndexReviewOutcomeUnknownError
from azure_sql_mcp.index_review import IndexReviewPolicyError
from azure_sql_mcp.index_review import IndexReviewWriteError


def _args(output: str = "json") -> list[str]:
    return ["capture", "--database", "appdb", "--output", output]


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (IndexReviewPolicyError("denied"), 2),
        (IndexReviewIdempotencyConflictError("conflict"), 2),
        (IndexReviewCollectionError("collection"), 3),
        (IndexReviewWriteError("write"), 3),
        (IndexReviewOutcomeUnknownError("unknown"), 3),
    ],
)
def test_cli_maps_capture_outcomes_to_exact_exit_codes(
    monkeypatch, capsys, error, expected
) -> None:
    async def fail(_args):
        raise error

    monkeypatch.setattr(index_history_cli, "_capture", fail)
    assert index_history_cli.main(_args()) == expected
    assert "azure-sql-mcp-index-history:" in capsys.readouterr().err


def test_cli_parser_has_exact_capture_shape() -> None:
    args = index_history_cli.build_parser().parse_args(
        ["capture", "--database", "appdb", "--idempotency-key", "k", "--output", "text"]
    )
    assert args.command == "capture"
    assert args.database == "appdb"
    assert args.idempotency_key == "k"
    assert args.output == "text"


def test_cli_maps_cleanup_failure_to_three_without_leaking_details(
    monkeypatch,
    capsys,
) -> None:
    sensitive_detail = "password=must-not-appear"

    class FailingPool:
        async def close_all(self) -> None:
            raise RuntimeError(sensitive_detail)

    class SuccessfulService:
        async def capture_snapshot(self, _database_name, _idempotency_key):
            return SimpleNamespace(as_dict=lambda: {"status": "captured"})

    config = SimpleNamespace(
        database_policy_file=None,
        validate_database_name=lambda database_name: database_name,
    )
    pool = FailingPool()
    monkeypatch.setattr(index_history_cli, "load_server_config", lambda _argv: config)
    monkeypatch.setattr(index_history_cli, "AzureSqlAuthenticator", lambda _config: object())
    monkeypatch.setattr(index_history_cli, "ConnectionPool", lambda _config, _auth: pool)
    monkeypatch.setattr(
        index_history_cli,
        "AzureSqlExecutor",
        lambda _config, _auth, _pool: object(),
    )
    monkeypatch.setattr(
        index_history_cli,
        "load_database_policy_or_deny",
        lambda _path: object(),
    )
    monkeypatch.setattr(
        index_history_cli,
        "SqlIndexHistoryRepository",
        lambda _executor, _policy: object(),
    )
    monkeypatch.setattr(
        index_history_cli,
        "IndexReviewService",
        lambda *_args, **_kwargs: SuccessfulService(),
    )

    assert index_history_cli.main(_args()) == 3
    error = capsys.readouterr().err
    assert error == (
        "azure-sql-mcp-index-history: collection or transaction failure.\n"
    )
    assert sensitive_detail not in error
