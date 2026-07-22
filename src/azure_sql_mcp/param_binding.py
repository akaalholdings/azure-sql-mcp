from __future__ import annotations

import logging
import re
from typing import Any

from .connection import AzureSqlExecutor
from .query_text import strip_query_store_parameter_declarations
from .safe_sql import strip_literals_and_comments

logger = logging.getLogger(__name__)

PARAM_PATTERN = re.compile(r"@(\w+)")
DECLARE_STATEMENT_PATTERN = re.compile(
    r"\bDECLARE\b(?P<body>.*?)(?=;|\b(?:SELECT|WITH|INSERT|UPDATE|DELETE|MERGE|"
    r"EXEC(?:UTE)?|IF|BEGIN|SET)\b|$)",
    re.IGNORECASE | re.DOTALL,
)
DECLARED_PARAMETER_PATTERN = re.compile(
    r"(?:^|,)\s*@(?P<name>\w+)\s+"
    r"(?:\[[^\]]+\]|\w+)(?:\s*\.\s*(?:\[[^\]]+\]|\w+))?"
    r"(?:\s*\([^)]*\))?",
    re.IGNORECASE,
)

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
    code = strip_literals_and_comments(sql)
    declared = {
        parameter.group("name").casefold()
        for statement in DECLARE_STATEMENT_PATTERN.finditer(code)
        for parameter in DECLARED_PARAMETER_PATTERN.finditer(statement.group("body"))
    }
    params: list[str] = []
    seen: set[str] = set()
    for match in PARAM_PATTERN.finditer(code):
        name = match.group(1)
        # Skip system @@ variables captured as single @
        if match.start() > 0 and code[match.start() - 1] == "@":
            continue
        if name.casefold() in declared:
            continue
        key = name.casefold()
        if key not in seen:
            seen.add(key)
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
        parameter_values: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Detect parameters and bind them using stats or type fallbacks.

        Returns dict with:
        - original_sql: the input SQL
        - bound_sql: SQL with DECLARE/SET block prepended
        - parameters: list of {name, value, source, data_type}
        """
        parameter_values = parameter_values or {}
        normalized_values: dict[str, Any] = {}
        for raw_name, value in parameter_values.items():
            name = str(raw_name).lstrip("@").strip().casefold()
            if not name:
                raise ValueError("explicit parameter name must not be empty")
            if name in normalized_values:
                raise ValueError(f"duplicate explicit parameter name: {name}")
            normalized_values[name] = value

        param_names = detect_parameters(sql)
        detected_names = {name.casefold() for name in param_names}
        unknown_names = sorted(set(normalized_values) - detected_names)
        if unknown_names:
            raise ValueError(
                "explicit value supplied for unknown parameter(s): "
                + ", ".join(unknown_names)
            )
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
            info = next(
                (value for key, value in param_info.items() if key.casefold() == name.casefold()),
                {},
            )
            data_type = info.get("data_type", "nvarchar(256)")
            value = info.get("value")
            source = info.get("source", "type_fallback")

            if name.casefold() in normalized_values:
                value = self._format_literal(data_type, normalized_values[name.casefold()])
                source = "explicit"
            elif value is None:
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

    async def prepare_query_store_text(self, database_name: str, sql_text: str) -> str:
        """Turn Query Store text into executable SQL.

        Stored text often looks like ``(@P1 int)SELECT ... WHERE x = @P1`` —
        SHOWPLAN compilation fails on the bare body ("must declare the scalar
        variable"), so after stripping the declaration prefix any remaining
        parameters are bound to representative values.
        """
        stripped = strip_query_store_parameter_declarations(sql_text)
        if not detect_parameters(stripped):
            return stripped
        binding = await self.bind_parameters(database_name, stripped)
        if binding.get("parameters"):
            return str(binding["bound_sql"])
        return stripped

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
        param_lookup = {name.casefold(): name for name in param_names}
        code = strip_literals_and_comments(sql)
        identifier = r"(?:\[[^\]]+\]|\"[^\"]+\"|\w+)"
        operator = r"(?:>=|<=|<>|!=|=|>|<|LIKE|IN\s*\()"
        keywords = {
            "SET", "SELECT", "WHERE", "AND", "OR", "FROM", "JOIN", "ON",
            "IN", "LIKE", "NOT", "IS", "NULL", "BETWEEN", "EXISTS", "HAVING",
            "GROUP", "ORDER", "CASE", "WHEN", "THEN", "ELSE", "END", "AS",
        }

        def add_mapping(table_hint: str | None, column_name: str, param_name: str) -> None:
            canonical_param = param_lookup.get(param_name.casefold())
            if canonical_param is None or canonical_param in mappings:
                return
            clean_column = column_name.strip("[]\"")
            if clean_column.upper() not in keywords:
                mappings[canonical_param] = (
                    table_hint.strip("[]\"") if table_hint else None,
                    clean_column,
                )

        column_then_param = re.compile(
            rf"(?:(?P<table>{identifier})\s*\.)?(?P<column>{identifier})\s*"
            rf"{operator}\s*@(?P<param>\w+)",
            re.IGNORECASE,
        )
        for match in column_then_param.finditer(code):
            add_mapping(match.group("table"), match.group("column"), match.group("param"))

        param_then_column = re.compile(
            rf"@(?P<param>\w+)\s*{operator}\s*"
            rf"(?:(?P<table>{identifier})\s*\.)?(?P<column>{identifier})",
            re.IGNORECASE,
        )
        for match in param_then_column.finditer(code):
            add_mapping(match.group("table"), match.group("column"), match.group("param"))

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

        # Get histogram for representative value. range_high_key is sql_variant,
        # which the driver cannot fetch — CONVERT server-side (style 121 keeps
        # datetime values ISO-formatted).
        histogram_query = """
        SELECT TOP 1
            CONVERT(NVARCHAR(4000), range_high_key, 121) AS range_high_key,
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
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        str_value = str(value)

        if base_type == "bit":
            if str_value not in {"0", "1"}:
                raise ValueError(f"explicit value is not valid for {data_type}")
            return str_value
        if base_type in {"int", "bigint", "smallint", "tinyint"}:
            if not re.fullmatch(r"-?\d+", str_value):
                raise ValueError(f"explicit value is not valid for {data_type}")
            return str_value
        if base_type in {"decimal", "numeric", "float", "real", "money", "smallmoney"}:
            if not re.fullmatch(r"-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", str_value):
                raise ValueError(f"explicit value is not valid for {data_type}")
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
            hex_value = str_value[2:] if str_value.lower().startswith("0x") else str_value
            if not re.fullmatch(r"[0-9a-fA-F]*", hex_value):
                raise ValueError(f"explicit value is not valid for {data_type}")
            return f"0x{hex_value}"

        escaped = str_value.replace("'", "''")
        return f"N'{escaped}'"
