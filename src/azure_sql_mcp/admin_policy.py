from __future__ import annotations

import asyncio
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

from .config import ServerConfig
from .config import WritePolicy
from .connection import AdminBatchOutcomeUnknownError
from .connection import BatchExecutionMode
from .connection import QueryResult
from .observability import redact_sql_literals
from .observability import sanitize_error_message


_DROP_DATABASE_ERROR = "SQL rejected by admin policy: DROP DATABASE is not allowed."
_TIMEOUT_MARKERS = ("HYT00", "HYT01", "QUERY TIMEOUT", "TIMED OUT")
_MAX_DYNAMIC_DEPTH = 8


@dataclass(frozen=True)
class _SqlToken:
    kind: str
    value: str


@dataclass(frozen=True)
class AdminAction:
    tool_name: str
    database_name: str
    action_type: str
    sql: str
    params: tuple[Any, ...] = ()
    rollback_sql: str | None = None
    rollback_params: tuple[Any, ...] = ()
    trusted_generated: bool = False
    reviewed_intent: bool = False
    idempotency_key: str | None = None
    exactly_once: bool = False
    policy_verified: bool = False
    non_production: bool = False
    verification_required: bool = False


class AdminPolicy:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.audit = AdminAuditLog(config.audit_dir, include_full_sql=config.audit_full_sql)

    def validate_sql(self, sql: str) -> None:
        if _contains_statically_recognizable_drop_database(_tokenize_sql(sql)):
            raise PermissionError(_DROP_DATABASE_ERROR)

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
        if action.exactly_once:
            return await self.execute_exactly_once(
                action,
                executor,
                dry_run=False,
                max_rows=max_rows,
            )
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
                execution_options: dict[str, Any] = {}
                if action.tool_name == "execute_tsql_unrestricted":
                    execution_options["execution_mode"] = BatchExecutionMode.ADMIN
                result = await executor.execute_batches(
                    action.database_name,
                    action.sql,
                    params=action.params,
                    max_rows=max_rows,
                    **execution_options,
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
        except asyncio.CancelledError as exc:
            self.audit.record(
                action,
                outcome="apply_outcome_unknown",
                audit_id=audit_id,
                error=str(exc) or type(exc).__name__,
            )
            raise
        except Exception as exc:
            self.audit.record(
                action,
                outcome=(
                    "apply_outcome_unknown"
                    if isinstance(exc, AdminBatchOutcomeUnknownError)
                    or _is_timeout_error(exc)
                    else "apply_failed"
                ),
                audit_id=audit_id,
                error=str(exc),
            )
            raise

    async def execute_exactly_once(
        self,
        action: AdminAction,
        executor,
        *,
        dry_run: bool,
        max_rows: int | None = None,
    ) -> dict[str, Any]:
        """Execute one reviewed generated batch through the no-retry admin lane.

        The executor's ADMIN batch mode deliberately disables automatic retry.
        This method is reserved for workflows that can reconcile or roll back
        a known statement and must never replay a dispatched DDL batch.
        """

        if not action.exactly_once:
            action = AdminAction(
                **{
                    **action.__dict__,
                    "exactly_once": True,
                }
            )
        if dry_run:
            return self.preview(action)
        try:
            self._validate_exactly_once_action(action)
            self._validate_or_audit_block(action)
        except PermissionError:
            raise
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
            result = await executor.execute_batches(
                action.database_name,
                action.sql,
                params=action.params,
                max_rows=max_rows,
                execution_mode=BatchExecutionMode.ADMIN,
            )
            self.audit.record(action, outcome="apply_completed", audit_id=audit_id)
            return self._payload(
                action,
                status="completed",
                audit_id=audit_id,
                result=_serialize_result_sets(result),
            )
        except asyncio.CancelledError as exc:
            self.audit.record(
                action,
                outcome="apply_outcome_unknown",
                audit_id=audit_id,
                error=str(exc) or type(exc).__name__,
            )
            raise
        except Exception as exc:
            self.audit.record(
                action,
                outcome=(
                    "apply_outcome_unknown"
                    if isinstance(exc, AdminBatchOutcomeUnknownError)
                    or _is_timeout_error(exc)
                    else "apply_failed"
                ),
                audit_id=audit_id,
                error=str(exc),
            )
            raise

    def _validate_exactly_once_action(self, action: AdminAction) -> None:
        if not action.reviewed_intent:
            raise PermissionError("exactly-once admin execution requires reviewed intent.")
        if not action.idempotency_key or not action.idempotency_key.strip():
            raise PermissionError(
                "exactly-once admin execution requires an idempotency key."
            )
        if not action.rollback_sql and action.tool_name != "drop_test_index":
            raise PermissionError(
                "exactly-once admin execution requires exact rollback SQL."
            )
        if not action.policy_verified:
            raise PermissionError(
                "exactly-once admin execution requires local policy verification."
            )
        if action.action_type != "query_store" and not action.non_production:
            raise PermissionError(
                "exactly-once admin execution is restricted to non-production targets."
            )
        if not action.verification_required:
            raise PermissionError(
                "exactly-once admin execution requires post-apply verification."
            )

    def _validate_or_audit_block(self, action: AdminAction) -> None:
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
        if action.exactly_once:
            payload["exactly_once"] = True
            payload["idempotency_key_hash"] = _hash_sql(action.idempotency_key or "")
        if action.rollback_sql:
            payload["rollback_sql"] = redact_sql_literals(action.rollback_sql)
            payload["rollback_param_count"] = len(action.rollback_params)
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
        if action.exactly_once:
            event["exactly_once"] = True
            event["idempotency_key_hash"] = _hash_sql(action.idempotency_key or "")
        if action.rollback_sql:
            event["rollback_sql"] = redact_sql_literals(action.rollback_sql)
            event["rollback_param_count"] = len(action.rollback_params)
        if self.include_full_sql:
            event["sql"] = action.sql
        if error:
            event["error"] = sanitize_error_message(error)
        path = self.audit_dir / f"{datetime.now(timezone.utc):%Y-%m-%d}.jsonl"
        fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, default=str) + "\n")
        return event_id


def _hash_sql(sql: str) -> str:
    normalized = re.sub(r"\s+", " ", sql.strip())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def _preview_sql(sql: str, limit: int = 500) -> str:
    normalized = re.sub(r"\s+", " ", redact_sql_literals(sql).strip())
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


def _is_timeout_error(exc: Exception) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, TimeoutError):
            return True
        for argument in getattr(current, "args", ()):
            text = str(argument).upper()
            if any(marker in text for marker in _TIMEOUT_MARKERS):
                return True
        current = current.__cause__ or current.__context__
    return False


def _contains_statically_recognizable_drop_database(
    tokens: list[_SqlToken],
    *,
    depth: int = 0,
) -> bool:
    if depth > _MAX_DYNAMIC_DEPTH:
        return False
    if any(
        token.kind == "word"
        and token.value == "DROP"
        and index + 1 < len(tokens)
        and tokens[index + 1].kind == "word"
        and tokens[index + 1].value == "DATABASE"
        for index, token in enumerate(tokens)
    ):
        return True

    known_variables: dict[str, str | None] = {}
    for index, token in enumerate(tokens):
        _update_known_sql_variable(tokens, index, known_variables)
        expression: list[_SqlToken] | None = None
        if token.kind == "word" and token.value in {"EXEC", "EXECUTE"}:
            expression = _executed_literal_expression(tokens, index)
        if expression is None:
            continue
        dynamic_sql = _literal_concatenation(expression, known_variables)
        static_prefix = _literal_prefix(expression, known_variables)
        candidates = [candidate for candidate in (dynamic_sql, static_prefix) if candidate]
        if any(
            _contains_statically_recognizable_drop_database(
                _tokenize_sql(candidate),
                depth=depth + 1,
            )
            for candidate in dict.fromkeys(candidates)
        ):
            return True
    return False


def _update_known_sql_variable(
    tokens: list[_SqlToken],
    index: int,
    known_variables: dict[str, str | None],
) -> None:
    token = tokens[index]
    if token.kind != "variable":
        return

    assignment_kind: str | None = None
    if index > 0 and tokens[index - 1].kind == "word":
        candidate = tokens[index - 1].value
        if candidate in {"SET", "SELECT"}:
            assignment_kind = candidate
    if assignment_kind is None:
        statement_start = index - 1
        while statement_start >= 0:
            candidate = tokens[statement_start]
            if candidate.kind == "symbol" and candidate.value == ";":
                break
            if candidate.kind == "word" and candidate.value == "DECLARE":
                assignment_kind = "DECLARE"
                break
            statement_start -= 1
    if assignment_kind is None:
        return

    equals_index = _find_assignment_equals(tokens, index + 1)
    if equals_index is None:
        return
    append = (
        equals_index > index + 1
        and tokens[equals_index - 1].kind == "symbol"
        and tokens[equals_index - 1].value == "+"
    )
    expression = _assignment_expression(tokens, equals_index + 1, assignment_kind)
    value = _literal_concatenation(expression, known_variables)
    if append and value is not None:
        previous = known_variables.get(token.value)
        value = previous + value if previous is not None else None
    known_variables[token.value] = value


def _find_assignment_equals(
    tokens: list[_SqlToken],
    start: int,
) -> int | None:
    depth = 0
    for index in range(start, len(tokens)):
        token = tokens[index]
        if token.kind != "symbol":
            continue
        if token.value == "(":
            depth += 1
        elif token.value == ")":
            depth = max(0, depth - 1)
        elif depth == 0 and token.value == "=":
            return index
        elif depth == 0 and token.value in {",", ";"}:
            return None
    return None


def _assignment_expression(
    tokens: list[_SqlToken],
    start: int,
    assignment_kind: str,
) -> list[_SqlToken]:
    depth = 0
    end = start
    while end < len(tokens):
        token = tokens[end]
        if token.kind == "symbol":
            if token.value == "(":
                depth += 1
            elif token.value == ")":
                if depth > 0:
                    depth -= 1
            elif depth == 0 and token.value == ";":
                break
            elif depth == 0 and assignment_kind in {"DECLARE", "SELECT"} and token.value == ",":
                break
        elif (
            depth == 0
            and token.kind == "word"
            and (
                end > start
                or (
                    assignment_kind == "SELECT"
                    and token.value in {"FROM", "WHERE", "OPTION"}
                )
            )
        ):
            break
        end += 1
    return tokens[start:end]


def _executed_literal_expression(
    tokens: list[_SqlToken], index: int,
) -> list[_SqlToken] | None:
    cursor = index + 1
    if (
        cursor + 1 < len(tokens)
        and tokens[cursor].kind == "variable"
        and tokens[cursor + 1].kind == "symbol"
        and tokens[cursor + 1].value == "="
    ):
        cursor += 2
    if cursor >= len(tokens):
        return None
    if tokens[cursor].kind == "symbol" and tokens[cursor].value == "(":
        close_index = _matching_closing_parenthesis(tokens, cursor)
        return tokens[cursor + 1 : close_index] if close_index is not None else None

    module_end, module_name = _qualified_name(tokens, cursor)
    if module_name != "SP_EXECUTESQL":
        return None
    return _first_argument_expression(tokens, module_end)


def _qualified_name(tokens: list[_SqlToken], index: int) -> tuple[int, str | None]:
    if index >= len(tokens) or tokens[index].kind not in {"word", "identifier"}:
        return index, None
    cursor = index + 1
    module_name = tokens[index].value
    while (
        cursor + 1 < len(tokens)
        and tokens[cursor].kind == "symbol"
        and tokens[cursor].value == "."
        and tokens[cursor + 1].kind in {"word", "identifier"}
    ):
        module_name = tokens[cursor + 1].value
        cursor += 2
    if (
        cursor + 1 < len(tokens)
        and tokens[cursor].kind == "symbol"
        and tokens[cursor].value == ";"
        and tokens[cursor + 1].kind == "number"
    ):
        cursor += 2
    return cursor, module_name


def _first_argument_expression(
    tokens: list[_SqlToken], start: int,
) -> list[_SqlToken] | None:
    if start >= len(tokens):
        return None
    cursor = start
    if (
        cursor + 1 < len(tokens)
        and tokens[cursor].kind == "variable"
        and tokens[cursor + 1].kind == "symbol"
        and tokens[cursor + 1].value == "="
    ):
        cursor += 2
    depth = 0
    end = cursor
    while end < len(tokens):
        token = tokens[end]
        if token.kind == "symbol":
            if token.value == "(":
                depth += 1
            elif token.value == ")":
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0 and token.value in {",", ";"}:
                break
        end += 1
    return tokens[cursor:end]


def _literal_concatenation(
    tokens: list[_SqlToken],
    known_variables: dict[str, str | None] | None = None,
) -> str | None:
    if not tokens:
        return None
    values: list[str] = []
    for token in tokens:
        if token.kind in {"string", "quoted"}:
            values.append(token.value)
        elif token.kind == "variable" and known_variables is not None:
            value = known_variables.get(token.value)
            if value is None:
                return None
            values.append(value)
        elif token.kind == "symbol" and token.value in {"+", "(", ")"}:
            continue
        else:
            return None
    return "".join(values) if values else None


def _literal_prefix(
    tokens: list[_SqlToken],
    known_variables: dict[str, str | None],
) -> str | None:
    values: list[str] = []
    for token in tokens:
        if token.kind in {"string", "quoted"}:
            values.append(token.value)
        elif token.kind == "variable":
            value = known_variables.get(token.value)
            if value is None:
                break
            values.append(value)
        elif token.kind == "symbol" and token.value in {"+", "(", ")"}:
            continue
        else:
            break
    return "".join(values) if values else None


def _matching_closing_parenthesis(
    tokens: list[_SqlToken], open_index: int,
) -> int | None:
    depth = 0
    for index in range(open_index, len(tokens)):
        token = tokens[index]
        if token.kind != "symbol":
            continue
        if token.value == "(":
            depth += 1
        elif token.value == ")":
            depth -= 1
            if depth == 0:
                return index
    return None


def _tokenize_sql(sql: str) -> list[_SqlToken]:
    tokens: list[_SqlToken] = []
    index = 0
    while index < len(sql):
        character = sql[index]
        if character.isspace():
            index += 1
            continue
        if sql.startswith("--", index):
            index = _skip_line_comment(sql, index + 2)
            continue
        if sql.startswith("/*", index):
            index = _skip_block_comment(sql, index + 2)
            continue
        if character in "Nn" and index + 1 < len(sql) and sql[index + 1] == "'":
            value, index = _read_quoted_content(sql, index + 1, "'")
            tokens.append(_SqlToken("string", value))
            continue
        if character == "'":
            value, index = _read_quoted_content(sql, index, "'")
            tokens.append(_SqlToken("string", value))
            continue
        if character == '"':
            value, index = _read_quoted_content(sql, index, '"')
            tokens.append(_SqlToken("quoted", value))
            continue
        if character == "[":
            value, index = _read_bracket_identifier(sql, index + 1)
            tokens.append(_SqlToken("identifier", value.upper()))
            continue
        if character == "@":
            start = index
            index += 1
            while index < len(sql) and (sql[index].isalnum() or sql[index] in "_@$#"):
                index += 1
            tokens.append(_SqlToken("variable", sql[start:index].upper()))
            continue
        if character.isdigit():
            start = index
            index += 1
            while index < len(sql) and sql[index].isdigit():
                index += 1
            tokens.append(_SqlToken("number", sql[start:index]))
            continue
        if character.isalpha() or character in "_#$":
            start = index
            index += 1
            while index < len(sql) and (sql[index].isalnum() or sql[index] in "_#$"):
                index += 1
            tokens.append(_SqlToken("word", sql[start:index].upper()))
            continue
        tokens.append(_SqlToken("symbol", character))
        index += 1
    return tokens


def _skip_line_comment(sql: str, index: int) -> int:
    while index < len(sql) and sql[index] not in "\r\n":
        index += 1
    return index


def _skip_block_comment(sql: str, index: int) -> int:
    depth = 1
    while index < len(sql) and depth:
        if sql.startswith("/*", index):
            depth += 1
            index += 2
        elif sql.startswith("*/", index):
            depth -= 1
            index += 2
        else:
            index += 1
    return index


def _read_quoted_content(sql: str, index: int, quote: str) -> tuple[str, int]:
    index += 1
    characters: list[str] = []
    while index < len(sql):
        if sql[index] != quote:
            characters.append(sql[index])
            index += 1
        elif index + 1 < len(sql) and sql[index + 1] == quote:
            characters.append(quote)
            index += 2
        else:
            return "".join(characters), index + 1
    return "".join(characters), index


def _read_bracket_identifier(sql: str, index: int) -> tuple[str, int]:
    characters: list[str] = []
    while index < len(sql):
        if sql[index] == "]":
            if index + 1 < len(sql) and sql[index + 1] == "]":
                characters.append("]")
                index += 2
            else:
                return "".join(characters), index + 1
        else:
            characters.append(sql[index])
            index += 1
    return "".join(characters), index
