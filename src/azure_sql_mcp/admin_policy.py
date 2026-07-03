from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

from sqlglot import exp
from sqlglot import parse
from sqlglot.errors import ParseError

from .config import ServerConfig
from .config import WritePolicy
from .connection import QueryResult


_HARD_DENY_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(
            r"\b(DROP|CREATE|ALTER)\s+"
            r"(DATABASE|TABLE|SCHEMA|LOGIN|USER|ROLE|INDEX|VIEW|PROCEDURE|FUNCTION|TRIGGER|ASSEMBLY)\b",
            re.IGNORECASE,
        ),
        "DDL or security principal change",
    ),
    (re.compile(r"\b(TRUNCATE\s+TABLE|SHUTDOWN|BACKUP|RESTORE|KILL|DBCC|WAITFOR)\b", re.IGNORECASE), "catastrophic or blocking command"),
    (re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE)\b", re.IGNORECASE), "raw data mutation"),
    (re.compile(r"\b(GRANT|DENY|REVOKE|ALTER\s+AUTHORIZATION)\b", re.IGNORECASE), "permission change"),
    (re.compile(r"\b(OPENROWSET|OPENQUERY|OPENDATASOURCE|BULK\s+INSERT)\b", re.IGNORECASE), "external data access"),
    (re.compile(r"\b(EXEC(?:UTE)?|sp_executesql)\b", re.IGNORECASE), "dynamic or stored procedure execution"),
    (re.compile(r"\b(xp_cmdshell|xp_dirtree|xp_fileexist|sp_oacreate|sp_oamethod)\b", re.IGNORECASE), "external procedure"),
    (re.compile(r"\bUSE\s+[\[\]\w]+", re.IGNORECASE), "database context switch"),
    (re.compile(r"\bSET\s+IDENTITY_INSERT\b", re.IGNORECASE), "identity insert override"),
)
_LINE_COMMENT_PATTERN = re.compile(r"--[^\r\n]*")
_BLOCK_COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)
_STRING_LITERAL_PATTERN = re.compile(r"N?'(?:''|[^'])*'", re.IGNORECASE)
_ALLOWED_RAW_ROOTS = (exp.Select, exp.Union, exp.Except, exp.Intersect)
_BANNED_RAW_NODES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.Command,
    exp.Transaction,
)


@dataclass(frozen=True)
class AdminAction:
    tool_name: str
    database_name: str
    action_type: str
    sql: str
    params: tuple[Any, ...] = ()
    rollback_sql: str | None = None
    trusted_generated: bool = False


class AdminPolicy:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.audit = AdminAuditLog(config.audit_dir, include_full_sql=config.audit_full_sql)

    def validate_sql(self, sql: str) -> None:
        scan_sql = _sql_for_policy_scan(sql)
        for pattern, reason in _HARD_DENY_PATTERNS:
            if pattern.search(scan_sql):
                raise PermissionError(f"SQL rejected by admin hard denylist: {reason}.")
        _validate_raw_sql_is_read_only(sql)

    def preview(self, action: AdminAction) -> dict[str, Any]:
        self._validate_or_audit_block(action)
        audit_id = self.audit.record(action, outcome="preview")
        return self._payload(action, status="dry_run", audit_id=audit_id)

    async def execute(
        self,
        action: AdminAction,
        executor,
        *,
        dry_run: bool,
        max_rows: int | None = None,
    ) -> dict[str, Any]:
        if dry_run:
            return self.preview(action)
        self._validate_or_audit_block(action)
        if self.config.write_policy != WritePolicy.APPLY:
            audit_id = self.audit.record(
                action,
                outcome="blocked",
                error="AZURE_SQL_WRITE_POLICY=apply is required for write execution.",
            )
            raise PermissionError(
                "Write execution requires AZURE_SQL_WRITE_POLICY=apply "
                f"(audit_id={audit_id})."
            )
        audit_id = self.audit.record(action, outcome="apply_started")
        try:
            if action.action_type == "query":
                result = await executor.execute_batches(
                    action.database_name,
                    action.sql,
                    params=action.params,
                    max_rows=max_rows,
                )
                self.audit.record(action, outcome="apply_completed", audit_id=audit_id)
                return self._payload(
                    action,
                    status="completed",
                    audit_id=audit_id,
                    result=_serialize_result_sets(result),
                )
            rowcount = await executor.execute_non_query(
                action.database_name,
                action.sql,
                params=action.params,
            )
            self.audit.record(action, outcome="apply_completed", audit_id=audit_id)
            return self._payload(action, status="completed", audit_id=audit_id, rowcount=rowcount)
        except Exception as exc:
            self.audit.record(action, outcome="apply_failed", audit_id=audit_id, error=str(exc))
            raise

    def _validate_or_audit_block(self, action: AdminAction) -> None:
        if action.trusted_generated:
            return
        try:
            self.validate_sql(action.sql)
        except PermissionError as exc:
            audit_id = self.audit.record(action, outcome="blocked", error=str(exc))
            raise PermissionError(f"{exc} (audit_id={audit_id})") from exc

    @staticmethod
    def _payload(
        action: AdminAction,
        *,
        status: str,
        audit_id: str,
        rowcount: int | None = None,
        result: Any | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "database_name": action.database_name,
            "tool_name": action.tool_name,
            "action_type": action.action_type,
            "status": status,
            "dry_run": status == "dry_run",
            "audit_id": audit_id,
            "sql_preview": _preview_sql(action.sql),
            "sql_hash": _hash_sql(action.sql),
        }
        if action.rollback_sql:
            payload["rollback_sql"] = action.rollback_sql
        if action.params:
            payload["param_count"] = len(action.params)
        if rowcount is not None:
            payload["rowcount"] = rowcount
        if result is not None:
            payload["result_sets"] = result
        return payload


class AdminAuditLog:
    def __init__(self, audit_dir: str, *, include_full_sql: bool):
        self.audit_dir = Path(audit_dir).expanduser()
        self.include_full_sql = include_full_sql

    def record(
        self,
        action: AdminAction,
        *,
        outcome: str,
        audit_id: str | None = None,
        error: str | None = None,
    ) -> str:
        event_id = audit_id or str(uuid.uuid4())
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        try:
            self.audit_dir.chmod(0o700)
        except OSError:
            pass
        event: dict[str, Any] = {
            "audit_id": event_id,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "tool_name": action.tool_name,
            "database_name": action.database_name,
            "action_type": action.action_type,
            "outcome": outcome,
            "sql_hash": _hash_sql(action.sql),
            "sql_preview": _preview_sql(action.sql),
            "param_count": len(action.params),
        }
        if action.rollback_sql:
            event["rollback_sql"] = action.rollback_sql
        if self.include_full_sql:
            event["sql"] = action.sql
        if error:
            event["error"] = error
        path = self.audit_dir / f"{datetime.now(timezone.utc):%Y-%m-%d}.jsonl"
        fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, default=str) + "\n")
        return event_id


def _hash_sql(sql: str) -> str:
    normalized = re.sub(r"\s+", " ", sql.strip())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def _preview_sql(sql: str, limit: int = 500) -> str:
    normalized = re.sub(r"\s+", " ", sql.strip())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3] + "..."


def _serialize_result_sets(result_sets: Any) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    if not isinstance(result_sets, list):
        return serialized
    for result in result_sets:
        if isinstance(result, QueryResult):
            serialized.append(
                {
                    "columns": list(result.columns),
                    "rows": result.rows,
                    "row_count": len(result.rows),
                }
            )
        elif isinstance(result, dict):
            serialized.append(result)
    return serialized


def _sql_for_policy_scan(sql: str) -> str:
    without_block_comments = _BLOCK_COMMENT_PATTERN.sub(" ", sql)
    without_comments = _LINE_COMMENT_PATTERN.sub(" ", without_block_comments)
    return _STRING_LITERAL_PATTERN.sub("?", without_comments)


def _validate_raw_sql_is_read_only(sql: str) -> None:
    try:
        statements = parse(sql, read="tsql")
    except ParseError as exc:
        raise PermissionError(
            "SQL rejected by admin hard denylist: raw arbitrary execution must parse as "
            "read-only SELECT statements. Use generated admin tools for writes."
        ) from exc
    if not statements:
        raise PermissionError("SQL rejected by admin hard denylist: SQL cannot be empty.")
    for statement in statements:
        if statement is None:
            raise PermissionError("SQL rejected by admin hard denylist: invalid statement.")
        if not isinstance(statement, _ALLOWED_RAW_ROOTS):
            raise PermissionError(
                "SQL rejected by admin hard denylist: raw arbitrary execution is limited "
                "to read-only SELECT statements. Use generated admin tools for writes."
            )
        for node in statement.walk():
            if isinstance(node, _BANNED_RAW_NODES):
                raise PermissionError(
                    "SQL rejected by admin hard denylist: raw arbitrary execution is limited "
                    "to read-only SELECT statements. Use generated admin tools for writes."
                )
            if isinstance(node, exp.Into):
                raise PermissionError(
                    "SQL rejected by admin hard denylist: SELECT INTO is not allowed."
                )
