from __future__ import annotations

import hashlib
import math
import re
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Mapping
from typing import Any

from .connection import AzureSqlExecutor
from .index_optimizer import INDEX_CANDIDATE_IMPACT_FLOOR_PCT
from .index_optimizer import score_index_candidate
from .plans import parse_showplan_index_evidence


QUERY_STORE_SHOWPLAN_NAMESPACE = {
    "sp": "http://schemas.microsoft.com/sqlserver/2004/07/showplan"
}


def _validate_query_hash(query_hash: str) -> str:
    normalized_hash = (query_hash or "").strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{16}", normalized_hash):
        raise ValueError("query_hash must be a 0x-prefixed hex string for binary(8).")
    return normalized_hash

def _escape_like_pattern(value: str) -> str:
    """Escape LIKE wildcards so query text matches as a literal substring.

    Query text routinely contains % and _ (LIKE clauses, identifiers); left
    unescaped they turn the fingerprint into a wildcard pattern.
    """
    return (
        value.replace("[", "[[]")
        .replace("%", "[%]")
        .replace("_", "[_]")
    )


def _weighted_runtime_average(metric: str) -> str:
    """Return a count-weighted Query Store runtime-stat average expression."""

    allowed_metrics = {
        "avg_cpu_time",
        "avg_duration",
        "avg_logical_io_reads",
        "avg_physical_io_reads",
        "avg_query_max_used_memory",
        "avg_rowcount",
    }
    if metric not in allowed_metrics:
        raise ValueError(f"Unsupported Query Store runtime metric: {metric}")
    return (
        f"SUM(rs.{metric} * rs.count_executions) "
        "/ NULLIF(SUM(rs.count_executions), 0)"
    )


_WEIGHTED_AVG_DURATION = _weighted_runtime_average("avg_duration")
_WEIGHTED_AVG_CPU = _weighted_runtime_average("avg_cpu_time")
_WEIGHTED_AVG_LOGICAL_READS = _weighted_runtime_average("avg_logical_io_reads")
_WEIGHTED_AVG_PHYSICAL_READS = _weighted_runtime_average("avg_physical_io_reads")
_WEIGHTED_AVG_MEMORY = _weighted_runtime_average("avg_query_max_used_memory")
_WEIGHTED_AVG_ROWS = _weighted_runtime_average("avg_rowcount")


SORT_BY_EXPRESSIONS = {
    "total_duration": "SUM(rs.avg_duration * rs.count_executions)",
    "avg_duration": _WEIGHTED_AVG_DURATION,
    "cpu": "SUM(rs.avg_cpu_time * rs.count_executions)",
    "executions": "SUM(rs.count_executions)",
    "logical_io": "SUM(rs.avg_logical_io_reads * rs.count_executions)",
    "physical_io": "SUM(rs.avg_physical_io_reads * rs.count_executions)",
    "memory": _WEIGHTED_AVG_MEMORY,
}


INDEX_EVIDENCE_QUERY = """
SELECT TOP (?)
    q.query_id,
    p.plan_id,
    p.query_plan_hash,
    p.is_forced_plan,
    CAST(p.query_plan AS nvarchar(max)) AS query_plan_xml,
    rs.runtime_stats_interval_id,
    COALESCE(SUM(rs.count_executions), 0) AS execution_count,
    COALESCE(SUM(rs.avg_cpu_time * rs.count_executions), 0) AS total_cpu_us,
    CAST(MAX(rs.end_time) AS datetime2(7)) AS last_seen_utc
FROM sys.query_store_query AS q
INNER JOIN sys.query_store_plan AS p
    ON p.query_id = q.query_id
LEFT JOIN
(
    SELECT
        runtime.plan_id,
        runtime.runtime_stats_interval_id,
        runtime.count_executions,
        runtime.avg_cpu_time,
        intervals.end_time
    FROM sys.query_store_runtime_stats AS runtime
    INNER JOIN sys.query_store_runtime_stats_interval AS intervals
        ON intervals.runtime_stats_interval_id = runtime.runtime_stats_interval_id
    WHERE intervals.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
) AS rs
    ON rs.plan_id = p.plan_id
GROUP BY
    q.query_id,
    p.plan_id,
    p.query_plan_hash,
    p.is_forced_plan,
    CAST(p.query_plan AS nvarchar(max)),
    rs.runtime_stats_interval_id
ORDER BY
    CASE WHEN p.is_forced_plan = 1 THEN 0 ELSE 1 END,
    SUM(rs.count_executions) DESC,
    p.plan_id,
    rs.runtime_stats_interval_id
"""


INDEX_CANDIDATE_ROW_COUNT_SQL = """
SELECT TOP (?)
    SUM(p.row_count) AS row_count
FROM sys.dm_db_partition_stats AS p
INNER JOIN sys.tables AS t
    ON p.object_id = t.object_id
INNER JOIN sys.schemas AS s
    ON t.schema_id = s.schema_id
WHERE s.name = ? AND t.name = ? AND p.index_id IN (0, 1)
"""


INDEX_CANDIDATE_WRITE_RATIO_SQL = """
SELECT TOP (?)
    CASE WHEN SUM(s.user_seeks + s.user_scans + s.user_lookups + s.user_updates) = 0
         THEN 0.5
         ELSE CAST(SUM(s.user_updates) AS FLOAT)
              / SUM(s.user_seeks + s.user_scans + s.user_lookups + s.user_updates)
    END AS write_ratio
FROM sys.indexes AS i
INNER JOIN sys.dm_db_index_usage_stats AS s
    ON i.object_id = s.object_id AND i.index_id = s.index_id
INNER JOIN sys.tables AS t
    ON i.object_id = t.object_id
INNER JOIN sys.schemas AS sch
    ON t.schema_id = sch.schema_id
WHERE sch.name = ? AND t.name = ? AND s.database_id = DB_ID()
"""


_INDEX_CANDIDATE_ROW_OVERHEAD = 9
_INDEX_CANDIDATE_SLOT_ARRAY_ENTRY = 2
_INDEX_CANDIDATE_USABLE_PAGE_BYTES = 8096
_INDEX_CANDIDATE_PAGE_SIZE_BYTES = 8192
_INDEX_CANDIDATE_NON_LEAF_MULTIPLIER = 1.1
_MAX_INDEX_CANDIDATE_COLUMNS = 256
_MAX_INDEX_EVIDENCE_ROWS = 10_000


QUERY_STORE_TEXT_HINTS_SQL = """
SELECT TOP (?)
    q.query_id,
    qt.query_sql_text AS retained_query_text
FROM sys.query_store_query AS q
INNER JOIN sys.query_store_query_text AS qt
    ON qt.query_text_id = q.query_text_id
ORDER BY q.last_execution_time DESC, q.query_id DESC
"""


QUERY_STORE_QUERY_HINTS_SQL = """
SELECT TOP (?)
    query_id,
    query_hint_text
FROM sys.query_store_query_hints
ORDER BY query_id DESC
"""


PLAN_GUIDE_HINTS_SQL = """
SELECT TOP (?)
    plan_guide_id,
    name AS plan_guide_name,
    hints AS plan_guide_hints
FROM sys.plan_guides
ORDER BY plan_guide_id DESC
"""


MODULE_HINTS_SQL = """
SELECT TOP (?)
    object_id,
    definition AS module_definition
FROM sys.sql_modules
WHERE definition IS NOT NULL
ORDER BY object_id
"""


def _strict_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value.strip()):
        return int(value.strip())
    return None


def _optional_float(
    value: Any,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    if minimum is not None and result < minimum:
        return None
    if maximum is not None and result > maximum:
        return None
    return result


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "y"}
    return bool(value)


def _merge_coverage(target: dict[str, Any], source: Mapping[str, Any]) -> None:
    for key in ("eligible", "scanned", "malformed"):
        target[key] = int(target.get(key, 0)) + int(source.get(key, 0) or 0)
    for key in ("truncated", "capped"):
        target[key] = bool(target.get(key)) or bool(source.get(key))
    target.setdefault("blockers", [])
    for blocker in source.get("blockers", []):
        if blocker not in target["blockers"]:
            target["blockers"].append(blocker)
    if (
        source.get("status") == "incomplete"
        or target.get("malformed", 0) > 0
        or target.get("capped")
        or target.get("truncated")
    ):
        target["status"] = "incomplete"


def _normalise_index_identity(value: Mapping[str, Any]) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        as_dict = getattr(value, "as_dict", None)
        if not callable(as_dict):
            return None
        value = as_dict()
    object_id = _strict_int(value.get("object_id"))
    index_id = _strict_int(value.get("index_id"))
    schema = value.get("schema", value.get("schema_name"))
    table = value.get("table", value.get("table_name", value.get("object_name")))
    name = value.get("name", value.get("index_name"))
    if (
        object_id is None
        or index_id is None
        or index_id <= 0
        or not isinstance(schema, str)
        or not isinstance(table, str)
        or not isinstance(name, str)
        or not schema
        or not table
        or not name
    ):
        return None
    return {
        "object_id": object_id,
        "index_id": index_id,
        "schema": schema,
        "table": table,
        "index_name": name,
    }


def _unquote_hint_identifier(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == "[" and text[-1] == "]":
        return text[1:-1].replace("]]", "]")
    return text


def _hint_table_matches(target: str | None, identity: Mapping[str, Any]) -> bool:
    if not target:
        return True
    pieces = [
        _unquote_hint_identifier(piece)
        for piece in target.strip().split(".")
        if piece.strip()
    ]
    if not pieces:
        return False
    if len(pieces) == 1:
        return pieces[0] == identity["table"]
    return pieces[-1] == identity["table"] and pieces[-2] == identity["schema"]


_HINT_IDENTIFIER = re.compile(
    r"\[(?:[^\]]|\]\])+\]|[A-Za-z_#$@][A-Za-z0-9_#$@]*|[0-9]+"
)


def _matching_hint_parenthesis(text: str, opening: int) -> int | None:
    depth = 0
    position = opening
    while position < len(text):
        character = text[position]
        if character == "[":
            position += 1
            while position < len(text):
                if text[position] != "]":
                    position += 1
                    continue
                if position + 1 < len(text) and text[position + 1] == "]":
                    position += 2
                    continue
                position += 1
                break
            continue
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return position
        position += 1
    return None


def _parse_hint_identifier_list(value: str) -> list[str] | None:
    identifiers: list[str] = []
    position = 0
    while position < len(value):
        while position < len(value) and value[position].isspace():
            position += 1
        match = _HINT_IDENTIFIER.match(value, position)
        if match is None:
            return None
        identifiers.append(match.group(0))
        position = match.end()
        while position < len(value) and value[position].isspace():
            position += 1
        if position == len(value):
            break
        if value[position] != ",":
            return None
        position += 1
    return identifiers or None


def _parse_index_hint_values(
    text: str,
    keyword_end: int,
) -> tuple[list[str] | None, int, bool]:
    position = keyword_end
    while position < len(text) and text[position].isspace():
        position += 1
    if position >= len(text) or text[position] not in {"=", "("}:
        return None, position, False
    if text[position] == "=":
        position += 1
        while position < len(text) and text[position].isspace():
            position += 1
        match = _HINT_IDENTIFIER.match(text, position)
        return (
            [match.group(0)] if match is not None else None,
            match.end() if match is not None else position,
            True,
        )
    closing = _matching_hint_parenthesis(text, position)
    if closing is None:
        return None, len(text), True
    return (
        _parse_hint_identifier_list(text[position + 1 : closing]),
        closing + 1,
        True,
    )


def _parse_forceseek_index(
    text: str,
    keyword_end: int,
) -> tuple[str | None, int]:
    position = keyword_end
    while position < len(text) and text[position].isspace():
        position += 1
    if position >= len(text) or text[position] != "(":
        return None, position
    closing = _matching_hint_parenthesis(text, position)
    if closing is None:
        return None, len(text)
    value_start = position + 1
    while value_start < closing and text[value_start].isspace():
        value_start += 1
    match = _HINT_IDENTIFIER.match(text, value_start)
    if match is None:
        return None, closing + 1
    remainder = match.end()
    while remainder < closing and text[remainder].isspace():
        remainder += 1
    if remainder < closing:
        if text[remainder] != "(":
            return None, closing + 1
        column_list_end = _matching_hint_parenthesis(text, remainder)
        if (
            column_list_end is None
            or text[column_list_end + 1 : closing].strip()
        ):
            return None, closing + 1
    return match.group(0), closing + 1


def _resolve_index_hints(
    text: str,
    identities: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    matches: dict[tuple[int, int, str], dict[str, Any]] = {}
    blockers: list[str] = []
    hints: list[tuple[int, str]] = []
    for match in re.finditer(r"\bINDEX\b", text, re.IGNORECASE):
        values, _end, recognized = _parse_index_hint_values(text, match.end())
        if not recognized:
            continue
        if not values:
            blockers.append("malformed_index_hint")
            continue
        hints.extend((match.start(), value) for value in values)
    for match in re.finditer(r"\bFORCESEEK\b", text, re.IGNORECASE):
        value, _end = _parse_forceseek_index(text, match.end())
        if value is None:
            blockers.append("unresolved_forceseek_index_hint")
            continue
        hints.append((match.start(), value))

    for hint_start, raw_value in hints:
        value = raw_value.strip()
        numeric = _strict_int(value)
        table_target: str | None = None
        prefix = text[max(0, hint_start - 180) : hint_start]
        table_hint = re.search(
            r"TABLE\s+HINT\s*\(\s*(?P<table>[^,()]+)\s*,\s*$",
            prefix,
            re.IGNORECASE,
        )
        if table_hint:
            table_target = table_hint.group("table").strip()
        if numeric is not None:
            candidates = [
                identity
                for identity in identities
                if identity["index_id"] == numeric
                and _hint_table_matches(table_target, identity)
            ]
            hint_kind = "numeric_index_id"
        else:
            name = _unquote_hint_identifier(value)
            candidates = [
                identity
                for identity in identities
                if identity["index_name"] == name
                and _hint_table_matches(table_target, identity)
            ]
            hint_kind = "index_name"
        if len(candidates) != 1:
            blockers.append(
                "unresolved_or_ambiguous_numeric_index_hint"
                if numeric is not None
                else "unresolved_or_ambiguous_index_hint"
            )
            continue
        identity = candidates[0]
        key = (identity["object_id"], identity["index_id"], hint_kind)
        matches[key] = {
            "hint_kind": hint_kind,
            "index": dict(identity),
            "object_id": identity["object_id"],
            "index_id": identity["index_id"],
            "schema": identity["schema"],
            "table": identity["table"],
            "index_name": identity["index_name"],
        }
    return list(matches.values()), list(dict.fromkeys(blockers))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()


def _hint_source_id(row: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in ("query_id", "plan_guide_id", "object_id"):
        value = _strict_int(row.get(key))
        if value is not None:
            result[key] = value
    if "plan_guide_name" in row and isinstance(row["plan_guide_name"], str):
        result["plan_guide_name_hash"] = _sha256_text(row["plan_guide_name"])
    return result


def _extract_statement_subtree_cost(plan_xml: str) -> float | None:
    try:
        root = ET.fromstring(plan_xml)
    except (ET.ParseError, TypeError):
        return None
    for statement in root.findall(
        ".//sp:StmtSimple", QUERY_STORE_SHOWPLAN_NAMESPACE
    ):
        cost = _optional_float(statement.attrib.get("StatementSubTreeCost"), minimum=0.0)
        if cost is not None:
            return cost
    return None


def _build_index_candidate_column_widths_sql(column_count: int) -> str:
    placeholders = ", ".join("?" for _ in range(column_count))
    return f"""
SELECT TOP (?)
    c.name AS column_name,
    c.max_length
FROM sys.columns AS c
INNER JOIN sys.tables AS t
    ON c.object_id = t.object_id
INNER JOIN sys.schemas AS s
    ON t.schema_id = s.schema_id
WHERE s.name = ? AND t.name = ? AND c.name IN ({placeholders})
"""


class QueryStoreService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_status(self, database_name: str) -> dict[str, Any]:
        query = """
        SELECT
            desired_state_desc,
            actual_state_desc,
            readonly_reason,
            current_storage_size_mb,
            max_storage_size_mb,
            query_capture_mode_desc,
            wait_stats_capture_mode_desc
        FROM sys.database_query_store_options
        """
        rows = await self.executor.fetch_all(database_name, query)
        if not rows:
            return {
                "enabled": False,
                "message": "Query Store options were not returned.",
            }
        row = rows[0]
        return {
            "enabled": row["actual_state_desc"] not in {"OFF", "ERROR"},
            "status": row,
        }

    async def resolve_query_identity(
        self,
        database_name: str,
        sql: str,
    ) -> dict[str, Any]:
        """Resolve exact Query Store text to one stable query identity.

        Similar-text matching is deliberately excluded. Multiple exact
        identities are returned as ambiguous rather than silently selecting
        one context/settings combination.
        """

        if not isinstance(sql, str) or not sql:
            raise ValueError("sql must not be empty.")
        rows = await self.executor.fetch_all(
            database_name,
            """
            SELECT TOP (20)
                q.query_id,
                CONVERT(varchar(18), q.query_hash, 1) AS query_hash,
                q.context_settings_id,
                q.object_id
            FROM sys.query_store_query_text AS qt
            INNER JOIN sys.query_store_query AS q
                ON q.query_text_id = qt.query_text_id
            WHERE DATALENGTH(qt.query_sql_text)
                    = DATALENGTH(CAST(? AS nvarchar(max)))
              AND qt.query_sql_text COLLATE Latin1_General_100_BIN2
                    = CAST(? AS nvarchar(max)) COLLATE Latin1_General_100_BIN2
            ORDER BY q.last_execution_time DESC, q.query_id DESC
            """,
            params=[sql, sql],
        )
        if not rows:
            return {
                "status": "not_found",
                "identity_kind": "exact_query_store_text",
                "matches": [],
            }
        identities = {
            (int(row["query_id"]), str(row["query_hash"])) for row in rows
        }
        if len(identities) != 1:
            return {
                "status": "ambiguous",
                "identity_kind": "exact_query_store_text",
                "matches": rows,
            }
        query_id, query_hash = next(iter(identities))
        return {
            "status": "resolved",
            "identity_kind": "query_id",
            "query_id": query_id,
            "query_hash": query_hash,
            "matches": rows,
        }

    async def get_top_queries(
        self,
        database_name: str,
        sort_by: str,
        window_minutes: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        query = self._build_top_queries_query(sort_by)
        params = [window_minutes, limit] if sort_by == "resource_blend" else [limit, window_minutes]
        return await self.executor.fetch_all(
            database_name,
            query,
            params=params,
        )

    async def get_query_history(
        self,
        database_name: str,
        *,
        query_id: int | None = None,
        query_hash: str | None = None,
        sql: str | None = None,
        window_minutes: int = 1440,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Resolve Query Store evidence by stable identity before fuzzy text.

        Query ID is preferred when present, then query hash, and only then the
        text matcher. This ordering prevents a nearby SQL string from silently
        becoming the evidence source for a different query identity.
        """
        if query_id is not None:
            return await self.get_query_history_by_id(
                database_name,
                query_id,
                window_minutes=window_minutes,
                limit=limit,
            )
        if query_hash:
            return await self.get_query_history_by_hash(
                database_name,
                query_hash,
                window_minutes=window_minutes,
                limit=limit,
            )
        if sql is not None:
            return await self.get_query_history_by_text(
                database_name,
                sql,
                window_minutes=window_minutes,
                limit=limit,
            )
        raise ValueError("query_id, query_hash, or sql is required.")

    async def get_query_history_by_id(
        self,
        database_name: str,
        query_id: int,
        window_minutes: int = 1440,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Return Query Store history for one stable ``query_id``."""
        self._validate_window_and_limit(window_minutes, limit)
        if query_id <= 0:
            raise ValueError("query_id must be greater than 0.")
        query = f"""
        SELECT TOP (?)
            q.query_id,
            q.query_hash,
            p.plan_id,
            p.query_plan_hash,
            q.query_parameterization_type_desc,
            p.plan_type_desc,
            p.is_forced_plan,
            p.force_failure_count,
            p.last_force_failure_reason_desc,
            qt.query_sql_text,
            SUM(rs.count_executions) AS executions,
            SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS total_duration_ms,
            {_WEIGHTED_AVG_DURATION} / 1000.0 AS avg_duration_ms,
            SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS total_cpu_ms,
            {_WEIGHTED_AVG_CPU} / 1000.0 AS avg_cpu_ms,
            SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
            SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_io_reads,
            {_WEIGHTED_AVG_LOGICAL_READS} AS avg_logical_io_reads,
            {_WEIGHTED_AVG_PHYSICAL_READS} AS avg_physical_io_reads,
            {_WEIGHTED_AVG_MEMORY} AS avg_query_max_used_memory,
            MIN(rs.avg_rowcount) AS min_avg_rowcount,
            MAX(rs.avg_rowcount) AS max_avg_rowcount,
            {_WEIGHTED_AVG_ROWS} AS avg_rowcount,
            MIN(rsi.start_time) AS first_seen_utc,
            MAX(rsi.end_time) AS last_seen_utc
        FROM sys.query_store_query AS q
        INNER JOIN sys.query_store_query_text AS qt
            ON qt.query_text_id = q.query_text_id
        INNER JOIN sys.query_store_plan AS p
            ON q.query_id = p.query_id
        INNER JOIN sys.query_store_runtime_stats AS rs
            ON p.plan_id = rs.plan_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
          AND q.query_id = ?
        GROUP BY
            q.query_id,
            q.query_hash,
            p.plan_id,
            p.query_plan_hash,
            q.query_parameterization_type_desc,
            p.plan_type_desc,
            p.is_forced_plan,
            p.force_failure_count,
            p.last_force_failure_reason_desc,
            qt.query_sql_text
        ORDER BY total_cpu_ms DESC, total_duration_ms DESC
        """
        rows = await self.executor.fetch_all(
            database_name,
            query,
            params=[limit, window_minutes, int(query_id)],
        )
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "query_id": query_id,
            "identity_kind": "query_id",
            "matches": rows,
        }

    async def get_query_history_by_hash(
        self,
        database_name: str,
        query_hash: str,
        window_minutes: int = 1440,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Query Store history matched by query_hash (from SHOWPLAN XML).

        Hash matching survives parameter renaming (@CustomerId vs @P1) and
        whitespace differences that defeat text matching.
        """
        normalized_hash = _validate_query_hash(query_hash)
        self._validate_window_and_limit(window_minutes, limit)

        query = f"""
        SELECT TOP (?)
            q.query_id,
            p.plan_id,
            p.is_forced_plan,
            p.force_failure_count,
            p.last_force_failure_reason_desc,
            qt.query_sql_text,
            SUM(rs.count_executions) AS executions,
            SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS total_duration_ms,
            {_WEIGHTED_AVG_DURATION} / 1000.0 AS avg_duration_ms,
            SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS total_cpu_ms,
            {_WEIGHTED_AVG_CPU} / 1000.0 AS avg_cpu_ms,
            SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
            SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_io_reads,
            {_WEIGHTED_AVG_LOGICAL_READS} AS avg_logical_io_reads,
            {_WEIGHTED_AVG_PHYSICAL_READS} AS avg_physical_io_reads,
            {_WEIGHTED_AVG_MEMORY} AS avg_query_max_used_memory,
            {_WEIGHTED_AVG_ROWS} AS avg_rowcount,
            MIN(rsi.start_time) AS first_seen_utc,
            MAX(rsi.end_time) AS last_seen_utc
        FROM sys.query_store_query AS q
        INNER JOIN sys.query_store_query_text AS qt
            ON qt.query_text_id = q.query_text_id
        INNER JOIN sys.query_store_plan AS p
            ON q.query_id = p.query_id
        INNER JOIN sys.query_store_runtime_stats AS rs
            ON p.plan_id = rs.plan_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
          AND q.query_hash = CONVERT(BINARY(8), ?, 1)
        GROUP BY
            q.query_id,
            p.plan_id,
            p.is_forced_plan,
            p.force_failure_count,
            p.last_force_failure_reason_desc,
            qt.query_sql_text
        ORDER BY total_cpu_ms DESC, total_duration_ms DESC
        """
        rows = await self.executor.fetch_all(
            database_name,
            query,
            params=[limit, window_minutes, normalized_hash],
        )
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "query_hash": normalized_hash,
            "identity_kind": "query_hash",
            "matches": rows,
        }

    async def get_query_history_by_text(
        self,
        database_name: str,
        sql: str,
        window_minutes: int = 1440,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Best-effort Query Store history for SQL text similar to the supplied query."""
        fingerprint = " ".join(sql.split())[:1000]
        if not fingerprint:
            raise ValueError("sql must not be empty.")
        self._validate_window_and_limit(window_minutes, limit)

        query = f"""
        SELECT TOP (?)
            q.query_id,
            p.plan_id,
            p.is_forced_plan,
            p.force_failure_count,
            p.last_force_failure_reason_desc,
            qt.query_sql_text,
            SUM(rs.count_executions) AS executions,
            SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS total_duration_ms,
            {_WEIGHTED_AVG_DURATION} / 1000.0 AS avg_duration_ms,
            SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS total_cpu_ms,
            {_WEIGHTED_AVG_CPU} / 1000.0 AS avg_cpu_ms,
            SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
            SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_io_reads,
            {_WEIGHTED_AVG_LOGICAL_READS} AS avg_logical_io_reads,
            {_WEIGHTED_AVG_PHYSICAL_READS} AS avg_physical_io_reads,
            {_WEIGHTED_AVG_MEMORY} AS avg_query_max_used_memory,
            {_WEIGHTED_AVG_ROWS} AS avg_rowcount,
            MIN(rsi.start_time) AS first_seen_utc,
            MAX(rsi.end_time) AS last_seen_utc
        FROM sys.query_store_query_text AS qt
        INNER JOIN sys.query_store_query AS q
            ON qt.query_text_id = q.query_text_id
        INNER JOIN sys.query_store_plan AS p
            ON q.query_id = p.query_id
        INNER JOIN sys.query_store_runtime_stats AS rs
            ON p.plan_id = rs.plan_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
          AND (
                qt.query_sql_text LIKE '%' + ? + '%'
             OR ? LIKE '%' + LEFT(qt.query_sql_text, 1000) + '%'
          )
        GROUP BY
            q.query_id,
            p.plan_id,
            p.is_forced_plan,
            p.force_failure_count,
            p.last_force_failure_reason_desc,
            qt.query_sql_text
        ORDER BY total_cpu_ms DESC, total_duration_ms DESC
        """
        rows = await self.executor.fetch_all(
            database_name,
            query,
            params=[limit, window_minutes, _escape_like_pattern(fingerprint), fingerprint],
        )
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "fingerprint_length": len(fingerprint),
            "identity_kind": "fuzzy_text",
            "matches": rows,
        }

    async def get_index_executed_plan_references(
        self,
        database_name: str,
        *,
        window_minutes: int = 1440,
        limit: int = 100,
        max_plan_xml_chars: int = 4_000_000,
    ) -> dict[str, Any]:
        """Collect index references at Query Store plan/interval grain."""

        self._validate_index_evidence_window_and_limit(window_minutes, limit)
        rows, coverage = await self._fetch_index_evidence_rows(
            database_name,
            window_minutes=window_minutes,
            limit=limit,
        )
        references_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in rows:
            evidence = self._parse_query_store_plan_evidence(
                row,
                max_plan_xml_chars=max_plan_xml_chars,
            )
            _merge_coverage(coverage, evidence.get("coverage", {}))
            for reference in evidence.get("index_references", []):
                key = (
                    reference.get("query_id"),
                    reference.get("plan_id"),
                    reference.get("database_name"),
                    reference.get("schema_name"),
                    reference.get("object_name"),
                    reference.get("index_name"),
                )
                existing = references_by_key.get(key)
                if existing is None:
                    references_by_key[key] = dict(reference)
                    continue
                existing["execution_count"] = (
                    existing.get("execution_count") or 0
                ) + (reference.get("execution_count") or 0)
                existing["runtime_interval_ids"] = sorted(
                    set(existing.get("runtime_interval_ids", []))
                    | set(reference.get("runtime_interval_ids", []))
                )
                existing["operator_kinds"] = sorted(
                    set(existing.get("operator_kinds", []))
                    | set(reference.get("operator_kinds", []))
                )
                existing["operator_kind"] = (
                    existing["operator_kinds"][0]
                    if len(existing["operator_kinds"]) == 1
                    else "Multiple"
                )
                if reference.get("last_seen") is not None and (
                    existing.get("last_seen") is None
                    or reference["last_seen"] > existing["last_seen"]
                ):
                    existing["last_seen"] = reference["last_seen"]
        references = list(references_by_key.values())
        coverage["eligible"] = len(rows)
        coverage["scanned"] = len(rows)
        references.sort(
            key=lambda item: (
                not bool(item.get("is_forced_plan")),
                -(item.get("execution_count") or 0),
                item.get("database_name", ""),
                item.get("schema_name", ""),
                item.get("object_name", ""),
                item.get("index_name", ""),
                item.get("plan_id", 0),
            )
        )
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "executed_plan_references": references,
            "index_references": references,
            "coverage": coverage,
        }

    async def collect_index_plan_references(
        self,
        database_name: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Alias for the per-index executed-plan collection API."""

        return await self.get_index_executed_plan_references(database_name, **kwargs)

    async def get_missing_index_candidate_requests(
        self,
        database_name: str,
        *,
        window_minutes: int = 1440,
        limit: int = 100,
        max_plan_xml_chars: int = 4_000_000,
    ) -> dict[str, Any]:
        """Return recurring missing-index requests without global Query Store totals."""

        self._validate_index_evidence_window_and_limit(window_minutes, limit)
        rows, coverage = await self._fetch_index_evidence_rows(
            database_name,
            window_minutes=window_minutes,
            limit=limit,
        )
        by_signature: dict[str, dict[str, Any]] = {}
        for row in rows:
            evidence = self._parse_query_store_plan_evidence(
                row,
                max_plan_xml_chars=max_plan_xml_chars,
            )
            _merge_coverage(coverage, evidence.get("coverage", {}))
            for candidate in evidence.get("missing_index_candidates", []):
                signature = str(candidate["candidate_signature"])
                item = by_signature.setdefault(
                    signature,
                    {
                        key: candidate[key]
                        for key in (
                            "database_name",
                            "schema_name",
                            "object_name",
                            "key_signature",
                            "include_signature",
                            "filter_signature",
                            "candidate_signature",
                            "equality_columns",
                            "inequality_columns",
                            "include_columns",
                            "impact_pct",
                        )
                    }
                )
                item.setdefault("query_ids", set()).add(candidate["query_id"])
                item.setdefault("plan_ids", set()).add(candidate["plan_id"])
                item.setdefault("runtime_interval_ids", set()).update(
                    candidate.get("runtime_interval_ids", [])
                )
                item["execution_count"] = (item.get("execution_count") or 0) + (
                    candidate.get("execution_count") or 0
                )
                item["request_count"] = item.get("request_count", 0) + 1
                item["is_forced_plan"] = bool(
                    item.get("is_forced_plan") or candidate.get("is_forced_plan")
                )
                candidate_impact = candidate.get("impact_pct")
                if candidate_impact is not None and (
                    item.get("impact_pct") is None
                    or candidate_impact > item["impact_pct"]
                ):
                    item["impact_pct"] = candidate_impact
                item.setdefault("_statement_costs_by_plan", {})
                statement_cost = candidate.get("statement_subtree_cost")
                plan_id = candidate.get("plan_id")
                if statement_cost is None:
                    item.setdefault("_scoring_blockers", []).append(
                        "statement_subtree_cost_unavailable"
                    )
                elif plan_id not in item["_statement_costs_by_plan"]:
                    item["_statement_costs_by_plan"][plan_id] = statement_cost
                for metric_name in ("estimated_size_mb", "write_ratio"):
                    metric = candidate.get(metric_name)
                    if metric is None:
                        continue
                    current_metric = item.get(metric_name)
                    if current_metric is None:
                        item[metric_name] = metric
                    elif current_metric != metric:
                        item.setdefault("_scoring_blockers", []).append(
                            f"{metric_name}_conflicting"
                        )
                last_seen = candidate.get("last_seen")
                if last_seen is not None and (
                    item.get("last_seen") is None or last_seen > item["last_seen"]
                ):
                    item["last_seen"] = last_seen

        coverage["eligible"] = len(rows)
        coverage["scanned"] = len(rows)

        candidates: list[dict[str, Any]] = []
        for item in by_signature.values():
            statement_costs = item.pop("_statement_costs_by_plan", {})
            item["statement_subtree_cost"] = (
                sum(statement_costs.values()) if statement_costs else None
            )
            scoring_blockers = list(
                dict.fromkeys(item.pop("_scoring_blockers", []))
            )
            interval_ids = sorted(item.pop("runtime_interval_ids", set()))
            item["runtime_interval_ids"] = interval_ids
            item["distinct_runtime_interval_count"] = len(interval_ids)
            item["recurring"] = len(interval_ids) >= 2
            item["query_ids"] = sorted(item.pop("query_ids", set()))
            item["plan_ids"] = sorted(item.pop("plan_ids", set()))
            item.setdefault("estimated_size_mb", None)
            item.setdefault("write_ratio", None)
            item["current_score"] = None
            impact_pct = _optional_float(
                item.get("impact_pct"), minimum=0.0, maximum=100.0
            )
            if impact_pct is None:
                scoring_blockers.append("impact_pct_unavailable")
            elif impact_pct < INDEX_CANDIDATE_IMPACT_FLOOR_PCT:
                scoring_blockers.append("impact_pct_below_existing_floor")
            else:
                estimated_size_mb = _optional_float(
                    item.get("estimated_size_mb"), minimum=0.0
                )
                if estimated_size_mb is None:
                    estimated_size_mb, blocker = await self._estimate_candidate_size(
                        database_name,
                        schema=str(item["schema_name"]),
                        table=str(item["object_name"]),
                        columns=[
                            *item.get("equality_columns", []),
                            *item.get("inequality_columns", []),
                            *item.get("include_columns", []),
                        ],
                    )
                    if blocker:
                        scoring_blockers.append(blocker)
                item["estimated_size_mb"] = estimated_size_mb

                write_ratio = _optional_float(
                    item.get("write_ratio"), minimum=0.0, maximum=1.0
                )
                if write_ratio is None:
                    write_ratio, blocker = await self._get_candidate_write_ratio(
                        database_name,
                        schema=str(item["schema_name"]),
                        table=str(item["object_name"]),
                    )
                    if blocker:
                        scoring_blockers.append(blocker)
                item["write_ratio"] = write_ratio

                score = score_index_candidate(
                    item.get("statement_subtree_cost"),
                    item.get("execution_count"),
                    impact_pct,
                    estimated_size_mb,
                    write_ratio,
                )
                if score is None:
                    scoring_blockers.append("index_candidate_scoring_inputs_incomplete")
                item["current_score"] = score
            if scoring_blockers:
                item["scoring_blockers"] = list(dict.fromkeys(scoring_blockers))
                if any(
                    blocker != "impact_pct_below_existing_floor"
                    for blocker in item["scoring_blockers"]
                ):
                    coverage["status"] = "incomplete"
                    for blocker in item["scoring_blockers"]:
                        if blocker not in coverage["blockers"]:
                            coverage["blockers"].append(blocker)
            candidates.append(item)
        candidates.sort(
            key=lambda item: (
                not bool(item["recurring"]),
                -(item.get("execution_count") or 0),
                item["candidate_signature"],
            )
        )
        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "missing_index_candidates": candidates,
            "candidates": candidates,
            "coverage": coverage,
        }

    async def collect_missing_index_candidates(
        self,
        database_name: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Alias for recurring missing-index request collection."""

        return await self.get_missing_index_candidate_requests(database_name, **kwargs)

    async def get_index_hint_coverage(
        self,
        database_name: str,
        *,
        index_identities: Iterable[Mapping[str, Any]],
        window_minutes: int = 1440,
        limit: int = 500,
    ) -> dict[str, Any]:
        """Scan retained hint sources and return hashed, resolved evidence only."""

        self._validate_index_evidence_window_and_limit(window_minutes, limit)
        raw_identities = list(index_identities)
        identities = [_normalise_index_identity(value) for value in raw_identities]
        identities = [value for value in identities if value is not None]
        coverage: dict[str, Any] = {
            "status": "complete",
            "eligible": 0,
            "scanned": 0,
            "malformed": 0,
            "truncated": False,
            "capped": False,
            "blockers": [],
            "sources": {},
        }
        if len(identities) != len(raw_identities):
            coverage["status"] = "incomplete"
            coverage["malformed"] = len(raw_identities) - len(identities)
            coverage["blockers"].append("index_identity_metadata_incomplete")
        if not identities:
            coverage["status"] = "incomplete"
            coverage["blockers"].append("index_identity_metadata_unavailable")
        evidence: list[dict[str, Any]] = []
        sources = (
            (
                "query_store_text",
                QUERY_STORE_TEXT_HINTS_SQL,
                [limit + 1],
                "retained_query_text",
            ),
            (
                "query_store_query_hints",
                QUERY_STORE_QUERY_HINTS_SQL,
                [limit + 1],
                "query_hint_text",
            ),
            (
                "plan_guides",
                PLAN_GUIDE_HINTS_SQL,
                [limit + 1],
                "plan_guide_hints",
            ),
            (
                "module_definitions",
                MODULE_HINTS_SQL,
                [limit + 1],
                "module_definition",
            ),
        )
        for source, query, params, text_key in sources:
            source_coverage = {
                "status": "complete",
                "eligible": 0,
                "scanned": 0,
                "malformed": 0,
                "truncated": False,
                "capped": False,
                "blockers": [],
            }
            try:
                rows = await self.executor.fetch_all(
                    database_name,
                    query,
                    params=params,
                )
            except Exception as exc:
                source_coverage.update(
                    {
                        "status": "incomplete",
                        "blockers": [f"{source}_permission_or_version_unavailable"],
                        "error_type": type(exc).__name__,
                    }
                )
                coverage["sources"][source] = source_coverage
                _merge_coverage(coverage, source_coverage)
                continue
            source_coverage["eligible"] = len(rows)
            if len(rows) > limit:
                source_coverage["capped"] = True
                source_coverage["blockers"].append(f"{source}_cap_reached")
                rows = rows[:limit]
            source_coverage["scanned"] = len(rows)
            for row in rows:
                text_value = row.get(text_key)
                if not isinstance(text_value, str) or not text_value.strip():
                    source_coverage["malformed"] += 1
                    source_coverage["blockers"].append(f"{source}_text_unavailable")
                    continue
                matches, parse_blockers = _resolve_index_hints(
                    text_value,
                    identities,
                )
                if parse_blockers:
                    source_coverage["malformed"] += 1
                    source_coverage["blockers"].extend(parse_blockers)
                if matches:
                    evidence.append(
                        {
                            "source": source,
                            "source_id": _hint_source_id(row),
                            "text_hash": _sha256_text(text_value),
                            "resolved_indexes": matches,
                            "hint_kinds": sorted(
                                {match["hint_kind"] for match in matches}
                            ),
                        }
                    )
            coverage["sources"][source] = source_coverage
            _merge_coverage(coverage, source_coverage)
        return {
            "database_name": database_name,
            "indexes": identities,
            "evidence": evidence,
            "coverage": coverage,
        }

    async def collect_index_hint_coverage(
        self,
        database_name: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Alias for conservative index-hint coverage."""

        return await self.get_index_hint_coverage(database_name, **kwargs)

    async def get_per_index_executed_plan_references(
        self,
        database_name: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Named alias used by index lifecycle callers."""

        return await self.get_index_executed_plan_references(database_name, **kwargs)

    async def get_per_candidate_missing_index_requests(
        self,
        database_name: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Named alias used by index lifecycle callers."""

        return await self.get_missing_index_candidate_requests(database_name, **kwargs)

    async def _fetch_index_evidence_rows(
        self,
        database_name: str,
        *,
        window_minutes: int,
        limit: int,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        coverage: dict[str, Any] = {
            "status": "complete",
            "eligible": 0,
            "scanned": 0,
            "malformed": 0,
            "truncated": False,
            "capped": False,
            "blockers": [],
        }
        try:
            rows = await self.executor.fetch_all(
                database_name,
                INDEX_EVIDENCE_QUERY,
                params=[limit + 1, window_minutes],
            )
        except Exception as exc:
            coverage.update(
                {
                    "status": "incomplete",
                    "blockers": ["query_store_permission_or_version_unavailable"],
                    "error_type": type(exc).__name__,
                }
            )
            return [], coverage
        if len(rows) > limit:
            coverage["capped"] = True
            coverage["blockers"].append("query_store_plan_cap_reached")
            rows = rows[:limit]
        return [dict(row) for row in rows], coverage

    async def _estimate_candidate_size(
        self,
        database_name: str,
        *,
        schema: str,
        table: str,
        columns: list[str],
    ) -> tuple[float | None, str | None]:
        unique_columns = list(dict.fromkeys(column for column in columns if column))
        if not unique_columns:
            return None, "index_candidate_columns_unavailable"
        if len(unique_columns) > _MAX_INDEX_CANDIDATE_COLUMNS:
            return None, "index_candidate_columns_capped"
        try:
            row_count_rows = await self.executor.fetch_all(
                database_name,
                INDEX_CANDIDATE_ROW_COUNT_SQL,
                params=[1, schema, table],
            )
        except Exception:
            return None, "index_candidate_row_count_unavailable"
        row_count = (
            _strict_int(row_count_rows[0].get("row_count"))
            if row_count_rows
            else None
        )
        if row_count is None:
            return None, "index_candidate_row_count_unavailable"
        if row_count == 0:
            return 0.0, None

        try:
            width_rows = await self.executor.fetch_all(
                database_name,
                _build_index_candidate_column_widths_sql(len(unique_columns)),
                params=[
                    _MAX_INDEX_CANDIDATE_COLUMNS + 1,
                    schema,
                    table,
                    *unique_columns,
                ],
            )
        except Exception:
            return None, "index_candidate_column_widths_unavailable"
        widths: dict[str, int] = {}
        for row in width_rows:
            column_name = row.get("column_name")
            width = _strict_int(row.get("max_length"))
            if isinstance(column_name, str) and width is not None:
                widths[column_name] = width
        if any(column not in widths for column in unique_columns):
            return None, "index_candidate_column_widths_unavailable"

        total_width = (
            sum(widths[column] for column in unique_columns)
            + _INDEX_CANDIDATE_ROW_OVERHEAD
        )
        rows_per_page = max(
            1,
            _INDEX_CANDIDATE_USABLE_PAGE_BYTES
            // (total_width + _INDEX_CANDIDATE_SLOT_ARRAY_ENTRY),
        )
        leaf_pages = math.ceil(row_count / rows_per_page)
        total_bytes = (
            leaf_pages
            * _INDEX_CANDIDATE_PAGE_SIZE_BYTES
            * _INDEX_CANDIDATE_NON_LEAF_MULTIPLIER
        )
        return total_bytes / (1024.0 * 1024.0), None

    async def _get_candidate_write_ratio(
        self,
        database_name: str,
        *,
        schema: str,
        table: str,
    ) -> tuple[float | None, str | None]:
        try:
            rows = await self.executor.fetch_all(
                database_name,
                INDEX_CANDIDATE_WRITE_RATIO_SQL,
                params=[1, schema, table],
            )
        except Exception:
            return None, "index_candidate_write_ratio_unavailable"
        value = (
            _optional_float(rows[0].get("write_ratio"), minimum=0.0, maximum=1.0)
            if rows
            else None
        )
        if value is None:
            return None, "index_candidate_write_ratio_unavailable"
        return value, None

    @staticmethod
    def _parse_query_store_plan_evidence(
        row: Mapping[str, Any],
        *,
        max_plan_xml_chars: int,
    ) -> dict[str, Any]:
        query_id = _strict_int(row.get("query_id"))
        plan_id = _strict_int(row.get("plan_id"))
        interval_id = _strict_int(
            row.get("runtime_stats_interval_id", row.get("runtime_interval_id"))
        )
        raw_execution_count = row.get("execution_count", row.get("executions"))
        execution_count = _strict_int(
            raw_execution_count
        )
        malformed_facts: list[str] = []
        if raw_execution_count is None:
            malformed_facts.append("execution_count_unavailable")
        elif execution_count is None:
            malformed_facts.append("execution_count_malformed")
        forced = _as_bool(row.get("is_forced_plan"))
        if query_id is None or plan_id is None or (
            interval_id is None and execution_count != 0
        ):
            malformed_facts.append("query_store_identity_or_interval_malformed")
        if malformed_facts:
            return {
                "index_references": [],
                "missing_index_candidates": [],
                "coverage": {
                    "status": "incomplete",
                    "eligible": 1,
                    "scanned": 1,
                    "malformed": 1,
                    "truncated": False,
                    "capped": False,
                    "blockers": list(dict.fromkeys(malformed_facts)),
                },
            }
        assert query_id is not None and plan_id is not None
        plan_xml = row.get("query_plan_xml") or row.get("plan_xml") or ""
        if not isinstance(plan_xml, str):
            plan_xml = ""
        interval_ids = [interval_id] if interval_id is not None else []
        parsed = parse_showplan_index_evidence(
            plan_xml,
            query_id=query_id,
            plan_id=plan_id,
            plan_hash=(
                str(row["query_plan_hash"])
                if row.get("query_plan_hash") is not None
                else None
            ),
            execution_count=execution_count,
            runtime_interval_ids=interval_ids,
            last_seen=row.get("last_seen_utc"),
            is_forced_plan=forced,
            input_truncated=len(plan_xml) >= max_plan_xml_chars,
            max_xml_chars=max_plan_xml_chars,
        )
        statement_subtree_cost = _optional_float(
            row.get("statement_subtree_cost"), minimum=0.0
        )
        if statement_subtree_cost is None:
            statement_subtree_cost = _extract_statement_subtree_cost(plan_xml)
        estimated_size_mb = _optional_float(
            row.get("estimated_index_size_mb", row.get("estimated_size_mb")),
            minimum=0.0,
        )
        write_ratio = _optional_float(
            row.get("table_write_ratio", row.get("write_ratio")),
            minimum=0.0,
            maximum=1.0,
        )
        for candidate in parsed.get("missing_index_candidates", []):
            candidate["statement_subtree_cost"] = statement_subtree_cost
            candidate["estimated_size_mb"] = estimated_size_mb
            candidate["write_ratio"] = write_ratio
        return parsed

    async def get_parameter_runtime_buckets(
        self,
        database_name: str,
        *,
        query_id: int | None = None,
        query_hash: str | None = None,
        window_minutes: int = 1440,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Expose compiled-parameter and runtime buckets for stable identity.

        Query Store does not record every runtime parameter value. The returned
        bucket therefore distinguishes compiled values from the runtime stats
        observed for the corresponding plan/interval; it never labels an
        inferred value as a runtime value.
        """
        self._validate_window_and_limit(window_minutes, limit)
        if query_id is None and query_hash is None:
            raise ValueError("query_id or query_hash is required.")
        if query_id is not None and query_id <= 0:
            raise ValueError("query_id must be greater than 0.")

        if query_id is not None:
            identity_clause = "q.query_id = ?"
            identity_params: list[Any] = [int(query_id)]
            identity_payload = {"query_id": int(query_id), "identity_kind": "query_id"}
        else:
            normalized_hash = _validate_query_hash(str(query_hash))
            identity_clause = "q.query_hash = CONVERT(BINARY(8), ?, 1)"
            identity_params = [normalized_hash]
            identity_payload = {
                "query_hash": normalized_hash,
                "identity_kind": "query_hash",
            }

        query = f"""
        SELECT TOP (?)
            q.query_id,
            q.query_hash,
            p.plan_id,
            p.query_plan_hash,
            q.query_parameterization_type_desc,
            p.plan_type_desc,
            CAST(p.query_plan AS nvarchar(max)) AS query_plan_xml,
            rs.execution_type_desc,
            SUM(rs.count_executions) AS executions,
            MIN(rs.avg_duration) / 1000.0 AS min_avg_duration_ms,
            MAX(rs.avg_duration) / 1000.0 AS max_avg_duration_ms,
            {_WEIGHTED_AVG_DURATION} / 1000.0 AS avg_duration_ms,
            {_WEIGHTED_AVG_CPU} / 1000.0 AS avg_cpu_ms,
            {_WEIGHTED_AVG_LOGICAL_READS} AS avg_logical_io_reads,
            {_WEIGHTED_AVG_PHYSICAL_READS} AS avg_physical_io_reads,
            {_WEIGHTED_AVG_ROWS} AS avg_rowcount,
            MIN(rsi.start_time) AS first_seen_utc,
            MAX(rsi.end_time) AS last_seen_utc
        FROM sys.query_store_query AS q
        INNER JOIN sys.query_store_plan AS p
            ON q.query_id = p.query_id
        INNER JOIN sys.query_store_runtime_stats AS rs
            ON p.plan_id = rs.plan_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
          AND {identity_clause}
        GROUP BY
            q.query_id,
            q.query_hash,
            p.plan_id,
            p.query_plan_hash,
            q.query_parameterization_type_desc,
            p.plan_type_desc,
            CAST(p.query_plan AS nvarchar(max)),
            rs.execution_type_desc
        ORDER BY executions DESC, avg_duration_ms DESC
        """
        rows = await self.executor.fetch_all(
            database_name,
            query,
            params=[limit, window_minutes, *identity_params],
        )
        buckets: list[dict[str, Any]] = []
        distinct_parameter_sets: set[tuple[tuple[str, Any], ...]] = set()
        for raw_row in rows:
            row = dict(raw_row)
            plan_xml = row.pop("query_plan_xml", None) or ""
            parameters = self._extract_plan_parameters(str(plan_xml))
            row["compiled_parameters"] = parameters
            runtime_parameters = [
                parameter
                for parameter in parameters
                if parameter.get("runtime_value") is not None
            ]
            row["runtime_parameters"] = runtime_parameters
            row["runtime_parameter_values_observed"] = bool(runtime_parameters)
            row["runtime_bucket_source"] = "query_store_runtime_stats_by_plan_and_interval"
            row["runtime_bucket_key"] = {
                "plan_id": row.get("plan_id"),
                "execution_type": row.get("execution_type_desc"),
            }
            buckets.append(row)
            key = tuple(
                (parameter["name"], parameter.get("compiled_value"))
                for parameter in parameters
            )
            if key:
                distinct_parameter_sets.add(key)

        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            **identity_payload,
            "buckets": buckets,
            "bucket_count": len(buckets),
            "plan_count": len({bucket.get("plan_id") for bucket in buckets}),
            "distinct_compiled_parameter_set_count": len(distinct_parameter_sets),
            "distinct_compiled_parameter_sets": [
                [{"name": name, "compiled_value": value} for name, value in key]
                for key in sorted(distinct_parameter_sets, key=repr)
            ],
            "runtime_values_note": (
                "Query Store runtime stats are grouped by plan and interval; "
                "compiled plan values are not runtime value samples."
            ),
        }

    async def get_query_parameter_buckets(
        self,
        database_name: str,
        query_id: int,
    ) -> dict[str, Any]:
        """Compatibility alias for the stable query-id bucket API."""
        return await self.get_parameter_runtime_buckets(
            database_name,
            query_id=query_id,
        )

    @staticmethod
    def _extract_plan_parameters(plan_xml: str) -> list[dict[str, Any]]:
        if not plan_xml.strip():
            return []
        try:
            root = ET.fromstring(plan_xml)
        except ET.ParseError:
            return []
        parameters: list[dict[str, Any]] = []
        for node in root.findall(
            ".//sp:ParameterList/sp:ColumnReference",
            QUERY_STORE_SHOWPLAN_NAMESPACE,
        ):
            name = node.attrib.get("Column")
            if not name:
                continue
            parameters.append(
                {
                    "name": name,
                    "data_type": node.attrib.get("ParameterDataType"),
                    "compiled_value": node.attrib.get("ParameterCompiledValue"),
                    "runtime_value": node.attrib.get("ParameterRuntimeValue"),
                }
            )
        return parameters

    @staticmethod
    def _validate_window_and_limit(window_minutes: int, limit: int) -> None:
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")
        if limit <= 0:
            raise ValueError("limit must be greater than 0.")

    @staticmethod
    def _validate_index_evidence_window_and_limit(
        window_minutes: int,
        limit: int,
    ) -> None:
        QueryStoreService._validate_window_and_limit(window_minutes, limit)
        if limit > _MAX_INDEX_EVIDENCE_ROWS:
            raise ValueError(
                f"limit must not exceed {_MAX_INDEX_EVIDENCE_ROWS} for index evidence."
            )

    def _build_top_queries_query(self, sort_by: str) -> str:
        if sort_by == "resource_blend":
            return f"""
            WITH QueryMetrics AS (
                SELECT
                    q.query_id,
                    p.plan_id,
                    qt.query_sql_text,
                    SUM(rs.count_executions) AS executions,
                    SUM(rs.avg_duration * rs.count_executions) AS total_duration_us,
                    {_WEIGHTED_AVG_DURATION} AS avg_duration_us,
                    SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu_us,
                    SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
                    SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_io_reads,
                    {_WEIGHTED_AVG_LOGICAL_READS} AS avg_logical_io_reads,
                    {_WEIGHTED_AVG_PHYSICAL_READS} AS avg_physical_io_reads,
                    {_WEIGHTED_AVG_MEMORY} AS avg_query_max_used_memory,
                    {_WEIGHTED_AVG_ROWS} AS avg_rowcount,
                    CAST(MAX(rsi.end_time) AS datetime2(7)) AS last_seen_utc
                FROM sys.query_store_query_text AS qt
                INNER JOIN sys.query_store_query AS q
                    ON qt.query_text_id = q.query_text_id
                INNER JOIN sys.query_store_plan AS p
                    ON q.query_id = p.query_id
                INNER JOIN sys.query_store_runtime_stats AS rs
                    ON p.plan_id = rs.plan_id
                INNER JOIN sys.query_store_runtime_stats_interval AS rsi
                    ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
                WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
                GROUP BY q.query_id, p.plan_id, qt.query_sql_text
            ),
            MaxMetrics AS (
                SELECT
                    MAX(total_cpu_us) AS max_cpu_us,
                    MAX(total_logical_io_reads) AS max_logical_io_reads,
                    MAX(total_duration_us) AS max_duration_us
                FROM QueryMetrics
            )
            SELECT TOP (?)
                qm.*,
                (
                    COALESCE(CAST(qm.total_cpu_us AS FLOAT) / NULLIF(mm.max_cpu_us, 0), 0.0)
                    + COALESCE(
                        CAST(qm.total_logical_io_reads AS FLOAT)
                        / NULLIF(mm.max_logical_io_reads, 0),
                        0.0
                    )
                    + COALESCE(
                        CAST(qm.total_duration_us AS FLOAT) / NULLIF(mm.max_duration_us, 0),
                        0.0
                    )
                ) / 3.0 AS resource_blend_score
            FROM QueryMetrics AS qm
            CROSS JOIN MaxMetrics AS mm
            ORDER BY resource_blend_score DESC, qm.total_cpu_us DESC
            """

        order_expression = SORT_BY_EXPRESSIONS.get(sort_by)
        if order_expression is None:
            supported = ", ".join(
                [
                    "total_duration",
                    "avg_duration",
                    "cpu",
                    "executions",
                    "logical_io",
                    "physical_io",
                    "memory",
                    "resource_blend",
                ]
            )
            raise ValueError(f"Unsupported sort_by. Use {supported}.")

        return f"""
        SELECT TOP (?)
            q.query_id,
            p.plan_id,
            qt.query_sql_text,
            SUM(rs.count_executions) AS executions,
            SUM(rs.avg_duration * rs.count_executions) AS total_duration_us,
            {_WEIGHTED_AVG_DURATION} AS avg_duration_us,
            SUM(rs.avg_cpu_time * rs.count_executions) AS total_cpu_us,
            SUM(rs.avg_logical_io_reads * rs.count_executions) AS total_logical_io_reads,
            SUM(rs.avg_physical_io_reads * rs.count_executions) AS total_physical_io_reads,
            {_WEIGHTED_AVG_LOGICAL_READS} AS avg_logical_io_reads,
            {_WEIGHTED_AVG_PHYSICAL_READS} AS avg_physical_io_reads,
            {_WEIGHTED_AVG_MEMORY} AS avg_query_max_used_memory,
            {_WEIGHTED_AVG_ROWS} AS avg_rowcount,
            CAST(MAX(rsi.end_time) AS datetime2(7)) AS last_seen_utc
        FROM sys.query_store_query_text AS qt
        INNER JOIN sys.query_store_query AS q
            ON qt.query_text_id = q.query_text_id
        INNER JOIN sys.query_store_plan AS p
            ON q.query_id = p.query_id
        INNER JOIN sys.query_store_runtime_stats AS rs
            ON p.plan_id = rs.plan_id
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi
            ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        WHERE rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
        GROUP BY q.query_id, p.plan_id, qt.query_sql_text
        ORDER BY {order_expression} DESC
        """
