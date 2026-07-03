from __future__ import annotations

import logging
import re
from typing import Any

from .connection import AzureSqlExecutor

logger = logging.getLogger(__name__)

PARAM_PATTERN = re.compile(r"@(\w+)")

# Type names come from sys.types and are embedded into DECLARE statements, so
# they must look like a plain (optionally parameterized) type. Anything else —
# e.g. a hostile user-defined type name — falls back to nvarchar(256).
SAFE_TYPE_PATTERN = re.compile(
    r"^[a-z_][a-z0-9_]*(?:\(\s*(?:max|\d{1,5}(?:\s*,\s*\d{1,5})?)\s*\))?$",
    re.IGNORECASE,
)
SAFE_FALLBACK_TYPE = "nvarchar(256)"

# Type-based fallback values (18.3) when stats are unavailable
TYPE_FALLBACKS: dict[str, str] = {
    "int": "1",
    "bigint": "1",
    "smallint": "1",
    "tinyint": "1",
    "bit": "1",
    "decimal": "1.0",
    "numeric": "1.0",
    "float": "1.0",
    "real": "1.0",
    "money": "1.00",
    "smallmoney": "1.00",
    "char": "'A'",
    "nchar": "N'A'",
    "varchar": "'test'",
    "nvarchar": "N'test'",
    "text": "'test'",
    "ntext": "N'test'",
    "date": "CAST(GETDATE() AS DATE)",
    "datetime": "GETDATE()",
    "datetime2": "SYSDATETIME()",
    "smalldatetime": "GETDATE()",
    "datetimeoffset": "SYSDATETIMEOFFSET()",
    "time": "CAST(GETDATE() AS TIME)",
    "uniqueidentifier": "NEWID()",
    "binary": "0x00",
    "varbinary": "0x00",
    "xml": "N'<root/>'",
    "sql_variant": "1",
}

DEFAULT_FALLBACK = "NULL"


def detect_parameters(sql: str) -> list[str]:
    """18.1: Detect @param style placeholders in T-SQL query text.

    Returns unique parameter names in order of first appearance.
    Excludes known system variables (@@ROWCOUNT, @@IDENTITY, etc.).
    """
    params: list[str] = []
    seen: set[str] = set()
    for match in PARAM_PATTERN.finditer(sql):
        name = match.group(1)
        # Skip system @@ variables captured as single @
        if match.start() > 0 and sql[match.start() - 1] == "@":
            continue
        if name not in seen:
            seen.add(name)
            params.append(name)
    return params


def get_type_fallback(data_type: str) -> str:
    """18.3: Get a sensible fallback value for a given SQL Server data type."""
    normalized = data_type.lower().strip()
    # Strip length/precision specifiers: varchar(50) -> varchar
    base_type = normalized.split("(")[0].strip()
    return TYPE_FALLBACKS.get(base_type, DEFAULT_FALLBACK)


class ParameterBindingService:
    """Binds parameter placeholders to realistic values using column statistics."""

    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def bind_parameters(
        self,
        database_name: str,
        sql: str,
    ) -> dict[str, Any]:
        """Detect parameters and bind them using stats or type fallbacks.

        Returns dict with:
        - original_sql: the input SQL
        - bound_sql: SQL with DECLARE/SET block prepended
        - parameters: list of {name, value, source, data_type}
        """
        param_names = detect_parameters(sql)
        if not param_names:
            return {
                "original_sql": sql,
                "bound_sql": sql,
                "parameters": [],
            }

        # Try to resolve parameter types and values from column stats
        param_info = await self._resolve_parameters(database_name, sql, param_names)

        # Build DECLARE/SET block
        declare_lines: list[str] = []
        set_lines: list[str] = []
        parameters: list[dict[str, Any]] = []

        for name in param_names:
            info = param_info.get(name, {})
            data_type = info.get("data_type", "nvarchar(256)")
            value = info.get("value")
            source = info.get("source", "type_fallback")

            if value is None:
                value = get_type_fallback(data_type)
                source = "type_fallback"

            declare_lines.append(f"DECLARE @{name} {data_type};")
            set_lines.append(f"SET @{name} = {value};")
            parameters.append({
                "name": f"@{name}",
                "data_type": data_type,
                "value": value,
                "source": source,
            })

        binding_block = "\n".join(declare_lines + set_lines)
        bound_sql = f"{binding_block}\n\n{sql}"

        return {
            "original_sql": sql,
            "bound_sql": bound_sql,
            "parameters": parameters,
        }

    async def _resolve_parameters(
        self,
        database_name: str,
        sql: str,
        param_names: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Attempt to resolve parameter types and values from column context.

        Uses a heuristic: look for patterns like 'column_name = @param' or
        '@param = column_name' in the SQL to identify which columns the
        parameters likely map to, then use stats histogram for values.
        """
        result: dict[str, dict[str, Any]] = {}

        # Extract column-to-param mappings from SQL patterns
        column_mappings = self._extract_column_mappings(sql, param_names)

        if not column_mappings:
            return result

        # Batch-resolve all mapped parameters
        for param_name, (table_hint, column_name) in column_mappings.items():
            info = await self._resolve_from_stats(
                database_name, table_hint, column_name,
            )
            if info:
                result[param_name] = info

        return result

    def _extract_column_mappings(
        self,
        sql: str,
        param_names: list[str],
    ) -> dict[str, tuple[str | None, str]]:
        """Extract column-to-param mappings from SQL patterns.

        Looks for: column = @param, @param = column, column > @param, etc.
        Returns {param_name: (table_hint, column_name)}.
        """
        mappings: dict[str, tuple[str | None, str]] = {}
        param_set = set(param_names)

        # Pattern: [alias.]column_name {=|>|<|>=|<=|<>|!=|LIKE|IN} @param
        column_then_param = re.compile(
            r"(?:(\w+)\.)?(\w+)\s*(?:=|>|<|>=|<=|<>|!=|LIKE)\s*@(\w+)",
            re.IGNORECASE,
        )
        for match in column_then_param.finditer(sql):
            table_hint = match.group(1)
            column_name = match.group(2)
            param_name = match.group(3)
            if param_name in param_set and param_name not in mappings:
                # Skip SQL keywords that look like column names
                if column_name.upper() not in {
                    "SET", "SELECT", "WHERE", "AND", "OR", "FROM",
                    "JOIN", "ON", "IN", "LIKE", "NOT", "IS", "NULL",
                    "BETWEEN", "EXISTS", "HAVING", "GROUP", "ORDER",
                    "CASE", "WHEN", "THEN", "ELSE", "END", "AS",
                }:
                    mappings[param_name] = (table_hint, column_name)

        return mappings

    async def _resolve_from_stats(
        self,
        database_name: str,
        table_hint: str | None,
        column_name: str,
    ) -> dict[str, Any] | None:
        """18.2: Use sys.dm_db_stats_histogram to get a representative value."""
        # First find the column's type and the stats_id
        type_query = """
        SELECT TOP 1
            t.name AS table_name,
            s.name AS schema_name,
            ty.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            st.stats_id
        FROM sys.columns AS c
        INNER JOIN sys.tables AS t ON c.object_id = t.object_id
        INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
        INNER JOIN sys.types AS ty ON c.user_type_id = ty.user_type_id
        INNER JOIN sys.stats_columns AS sc ON c.object_id = sc.object_id AND c.column_id = sc.column_id
        INNER JOIN sys.stats AS st ON sc.object_id = st.object_id AND sc.stats_id = st.stats_id
        WHERE c.name = ?
          AND OBJECTPROPERTY(c.object_id, 'IsUserTable') = 1
        """
        hinted_query = type_query + "\n          AND t.name = ?\n        ORDER BY st.stats_id"
        unhinted_query = type_query + "\n        ORDER BY st.stats_id"

        try:
            type_rows: list[dict[str, Any]] = []
            if table_hint:
                type_rows = await self.executor.fetch_all(
                    database_name, hinted_query, params=[column_name, table_hint],
                )
            if not type_rows:
                # The hint is often a table alias (o.CustomerId), not a real
                # table name — retry across all tables rather than silently
                # falling back to an nvarchar guess that breaks execution.
                type_rows = await self.executor.fetch_all(
                    database_name, unhinted_query, params=[column_name],
                )
        except Exception:
            logger.warning(
                "Failed to resolve column type for parameter '%s'",
                column_name,
                exc_info=True,
            )
            return None

        if not type_rows:
            return None

        row = type_rows[0]
        data_type = self._format_data_type(row)
        stats_id = row.get("stats_id")
        table_name = row.get("table_name")
        schema_name = row.get("schema_name", "dbo")

        if stats_id is None or table_name is None:
            return {"data_type": data_type, "value": None, "source": "type_fallback"}

        # Get histogram for representative value
        histogram_query = """
        SELECT TOP 1
            range_high_key,
            equal_rows
        FROM sys.dm_db_stats_histogram(
            OBJECT_ID(?),
            ?
        )
        WHERE equal_rows > 0
        ORDER BY equal_rows DESC
        """
        object_name = f"{schema_name}.{table_name}"
        try:
            hist_rows = await self.executor.fetch_all(
                database_name, histogram_query, params=[object_name, stats_id],
            )
        except Exception:
            logger.warning(
                "Failed to query histogram for %s (stats_id=%s), falling back to type default",
                object_name,
                stats_id,
                exc_info=True,
            )
            return {"data_type": data_type, "value": None, "source": "type_fallback"}

        if not hist_rows:
            return {"data_type": data_type, "value": None, "source": "type_fallback"}

        raw_value = hist_rows[0].get("range_high_key")
        if raw_value is None:
            return {"data_type": data_type, "value": None, "source": "type_fallback"}

        formatted_value = self._format_literal(data_type, raw_value)
        return {
            "data_type": data_type,
            "value": formatted_value,
            "source": "histogram",
        }

    def _format_data_type(self, row: dict[str, Any]) -> str:
        """Format a SQL Server data type from column metadata."""
        base_type = str(row.get("data_type", "nvarchar")).lower()
        if base_type in {"varchar", "nvarchar", "char", "nchar", "binary", "varbinary"}:
            max_length = int(row.get("max_length", 0))
            if max_length == -1:
                return f"{base_type}(max)"
            length = max_length
            if base_type.startswith("n"):
                length = max(length // 2, 1)
            return f"{base_type}({length})"
        if base_type in {"decimal", "numeric"}:
            precision = int(row.get("precision", 18))
            scale = int(row.get("scale", 0))
            return f"{base_type}({precision},{scale})"
        if base_type in {"datetime2", "datetimeoffset", "time"}:
            scale = int(row.get("scale", 7))
            return f"{base_type}({scale})"
        if not SAFE_TYPE_PATTERN.match(base_type):
            return SAFE_FALLBACK_TYPE
        return base_type

    def _format_literal(self, data_type: str, value: Any) -> str:
        """Format a raw value as a T-SQL literal."""
        base_type = data_type.lower().split("(")[0].strip()
        str_value = str(value)

        if base_type in {"int", "bigint", "smallint", "tinyint", "bit"}:
            return str_value
        if base_type in {"decimal", "numeric", "float", "real", "money", "smallmoney"}:
            return str_value
        if base_type in {"nvarchar", "nchar", "ntext"}:
            escaped = str_value.replace("'", "''")
            return f"N'{escaped}'"
        if base_type in {"varchar", "char", "text"}:
            escaped = str_value.replace("'", "''")
            return f"'{escaped}'"
        if base_type in {
            "date", "datetime", "datetime2", "smalldatetime",
            "datetimeoffset", "time",
        }:
            escaped = str_value.replace("'", "''")
            return f"'{escaped}'"
        if base_type == "uniqueidentifier":
            escaped = str_value.replace("'", "''")
            return f"'{escaped}'"
        if base_type in {"binary", "varbinary"}:
            return f"0x{str_value}" if not str_value.startswith("0x") else str_value

        escaped = str_value.replace("'", "''")
        return f"N'{escaped}'"
