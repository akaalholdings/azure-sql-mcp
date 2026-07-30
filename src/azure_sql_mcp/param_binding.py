from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from datetime import datetime
from datetime import time
from decimal import Decimal
from decimal import InvalidOperation
from typing import Any
from typing import Mapping
from uuid import UUID

from mssql_python.constants import ConstantsDDBC

from .connection import AzureSqlExecutor
from .query_text import strip_query_store_parameter_declarations
from .safe_sql import strip_literals_and_comments

SP_EXECUTESQL_CONTROL_INPUT_SIZES = (
    (int(ConstantsDDBC.SQL_WVARCHAR.value), 0, 0),
    (int(ConstantsDDBC.SQL_WVARCHAR.value), 0, 0),
)

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
SUPPORTED_PARAMETER_TYPES = frozenset(
    {
        "bigint",
        "binary",
        "bit",
        "char",
        "date",
        "datetime",
        "datetime2",
        "datetimeoffset",
        "decimal",
        "float",
        "int",
        "money",
        "nchar",
        "numeric",
        "nvarchar",
        "real",
        "smalldatetime",
        "smallint",
        "smallmoney",
        "sql_variant",
        "time",
        "tinyint",
        "uniqueidentifier",
        "varbinary",
        "varchar",
        "xml",
    }
)
LENGTH_PARAMETER_TYPES = frozenset(
    {"varchar", "nvarchar", "char", "nchar", "binary", "varbinary"}
)
MAX_LENGTH_PARAMETER_TYPES = frozenset({"varchar", "nvarchar", "varbinary"})
SCALE_PARAMETER_TYPES = frozenset({"datetime2", "datetimeoffset", "time"})

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


def _parse_integral_value(value: Any) -> int:
    """Parse an integer without silently truncating fractional values."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        numeric_value = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("integer value is not valid") from exc
    if not numeric_value.is_finite() or numeric_value != numeric_value.to_integral_value():
        raise ValueError("integer value is not valid")
    return int(numeric_value)


def _parse_bit_value(value: Any) -> bool:
    """Parse only the recognized SQL bit representations."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"1", "true"}:
            return True
        if normalized in {"0", "false"}:
            return False
    raise ValueError("bit value is not valid")


@dataclass(frozen=True, slots=True)
class SqlParameterType:
    """The SQL type metadata required for faithful parameter compilation."""

    base_type: str
    length: int | str | None = None
    precision: int | None = None
    scale: int | None = None

    def __post_init__(self) -> None:
        base_type = self.base_type.strip().lower()
        if base_type not in SUPPORTED_PARAMETER_TYPES:
            raise ValueError(f"Unsupported Azure SQL parameter type: {self.base_type!r}")
        if not SAFE_TYPE_PATTERN.fullmatch(
            self.sql_declaration_for(
                base_type,
                self.length,
                self.precision,
                self.scale,
            )
        ):
            raise ValueError(f"Unsafe SQL parameter type: {self.base_type!r}")
        if self.length not in (None, "max") and (
            not isinstance(self.length, int)
            or isinstance(self.length, bool)
            or self.length < 0
        ):
            raise ValueError("SQL parameter length must be a non-negative integer or max.")
        if self.precision is not None and (
            not isinstance(self.precision, int)
            or isinstance(self.precision, bool)
            or not 1 <= self.precision <= 38
        ):
            raise ValueError("SQL decimal precision must be between 1 and 38.")
        if self.scale is not None and (
            not isinstance(self.scale, int)
            or isinstance(self.scale, bool)
            or not 0 <= self.scale <= 38
        ):
            raise ValueError("SQL decimal scale must be between 0 and 38.")
        if self.precision is not None and self.scale is not None and self.scale > self.precision:
            raise ValueError("SQL decimal scale must not exceed precision.")
        if self.length is not None and base_type not in LENGTH_PARAMETER_TYPES:
            raise ValueError(f"{base_type} does not accept length metadata.")
        if self.length == "max" and base_type not in MAX_LENGTH_PARAMETER_TYPES:
            raise ValueError(f"{base_type}(max) is not a supported parameter type.")
        if isinstance(self.length, int):
            maximum = 4000 if base_type in {"nvarchar", "nchar"} else 8000
            if not 1 <= self.length <= maximum:
                raise ValueError(
                    f"{base_type} parameter length must be between 1 and {maximum}."
                )
        if self.precision is not None and base_type not in {"decimal", "numeric"}:
            raise ValueError(f"{base_type} does not accept precision metadata.")
        if self.scale is not None and base_type not in {
            "decimal",
            "numeric",
            *SCALE_PARAMETER_TYPES,
        }:
            raise ValueError(f"{base_type} does not accept scale metadata.")
        if (
            base_type in SCALE_PARAMETER_TYPES
            and self.scale is not None
            and self.scale > 7
        ):
            raise ValueError(f"{base_type} scale must be between 0 and 7.")
        object.__setattr__(self, "base_type", base_type)

    @staticmethod
    def sql_declaration_for(
        base_type: str,
        length: int | str | None = None,
        precision: int | None = None,
        scale: int | None = None,
    ) -> str:
        if base_type in {"varchar", "nvarchar", "char", "nchar", "binary", "varbinary"}:
            return f"{base_type}({length if length is not None else 1})"
        if base_type in {"decimal", "numeric"}:
            return f"{base_type}({precision if precision is not None else 18},{scale or 0})"
        if base_type in {"datetime2", "datetimeoffset", "time"}:
            return f"{base_type}({scale if scale is not None else 7})"
        return base_type

    @classmethod
    def from_sql(cls, data_type: str) -> "SqlParameterType":
        match = re.fullmatch(
            r"\s*(?P<base>[a-z_][a-z0-9_]*)\s*"
            r"(?:\(\s*(?P<first>max|\d{1,5})\s*"
            r"(?:,\s*(?P<second>\d{1,5})\s*)?\))?\s*",
            data_type,
            re.IGNORECASE,
        )
        if not match:
            raise ValueError(f"Unsafe SQL parameter type: {data_type!r}")
        base_type = match.group("base").lower()
        first = match.group("first")
        second = match.group("second")
        if base_type not in SUPPORTED_PARAMETER_TYPES:
            raise ValueError(f"Unsupported Azure SQL parameter type: {data_type!r}")
        if first is not None and base_type not in {
            *LENGTH_PARAMETER_TYPES,
            "decimal",
            "numeric",
            *SCALE_PARAMETER_TYPES,
        }:
            raise ValueError(f"{base_type} does not accept parameter metadata.")
        if second is not None and base_type not in {"decimal", "numeric"}:
            raise ValueError(f"{base_type} does not accept precision and scale.")
        if base_type in {"varchar", "nvarchar", "char", "nchar", "binary", "varbinary"}:
            length: int | str | None = (
                first.lower() if first and first.lower() == "max" else int(first)
                if first
                else None
            )
            return cls(base_type=base_type, length=length)
        if base_type in {"decimal", "numeric"}:
            return cls(
                base_type=base_type,
                precision=int(first) if first else 18,
                scale=int(second) if second else 0,
            )
        if base_type in {"datetime2", "datetimeoffset", "time"}:
            return cls(base_type=base_type, scale=int(first) if first else 7)
        return cls(base_type=base_type)

    @property
    def sql_declaration(self) -> str:
        return self.sql_declaration_for(
            self.base_type,
            self.length,
            self.precision,
            self.scale,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "data_type": self.sql_declaration,
            "base_type": self.base_type,
            "length": self.length,
            "precision": self.precision,
            "scale": self.scale,
        }


@dataclass(frozen=True, slots=True)
class TypedParameter:
    """One value and its compilation-faithful SQL type/provenance."""

    name: str
    sql_type: SqlParameterType
    value: Any
    provenance: str
    provenance_detail: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name.startswith("@") or not re.fullmatch(r"@[A-Za-z_]\w*", self.name):
            raise ValueError(f"Invalid parameter name: {self.name!r}")
        if not self.provenance:
            raise ValueError("Parameter provenance must not be empty.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            **self.sql_type.to_dict(),
            "value": self.value,
            "provenance": self.provenance,
            "provenance_detail": dict(self.provenance_detail),
        }


@dataclass(frozen=True, slots=True)
class TypedParameterBucket:
    """Canonical typed values shared by baseline and candidate executions."""

    bucket_id: str
    parameters: tuple[TypedParameter, ...]
    provenance: str = "parameter_binding"
    label: str | None = None

    def __post_init__(self) -> None:
        if not self.bucket_id.strip():
            raise ValueError("bucket_id must not be empty.")
        names: set[str] = set()
        for parameter in self.parameters:
            key = parameter.name.casefold()
            if key in names:
                raise ValueError(f"Duplicate parameter in typed bucket: {parameter.name}")
            names.add(key)

    def for_sql(self, sql: str, *, provenance: str | None = None) -> "ParameterExecutionContract":
        names = detect_parameters(sql)
        by_name = {
            parameter.name.lstrip("@").casefold(): parameter
            for parameter in self.parameters
        }
        missing = [name for name in names if name.casefold() not in by_name]
        if missing:
            raise ValueError(
                "Typed parameter bucket does not cover SQL parameter(s): "
                + ", ".join(f"@{name}" for name in missing)
            )
        ordered = tuple(by_name[name.casefold()] for name in names)
        return ParameterExecutionContract(
            sql_text=sql,
            bucket_id=self.bucket_id,
            parameters=ordered,
            provenance=provenance or self.provenance,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "bucket_id": self.bucket_id,
            "label": self.label,
            "provenance": self.provenance,
            "parameters": [parameter.to_dict() for parameter in self.parameters],
        }


@dataclass(frozen=True, slots=True)
class ParameterExecutionContract:
    """Driver and ``sp_executesql`` forms for one typed parameter bucket."""

    sql_text: str
    bucket_id: str
    parameters: tuple[TypedParameter, ...]
    provenance: str

    def __post_init__(self) -> None:
        if not self.sql_text.strip():
            raise ValueError("parameter execution SQL must not be empty.")
        if not self.bucket_id.strip() or not self.provenance.strip():
            raise ValueError("parameter execution bucket and provenance are required.")
        expected = {name.casefold() for name in detect_parameters(self.sql_text)}
        supplied = {
            parameter.name.lstrip("@").casefold()
            for parameter in self.parameters
        }
        if len(supplied) != len(self.parameters):
            raise ValueError("parameter execution contract contains duplicate names.")
        if supplied != expected:
            raise ValueError(
                "parameter execution contract must cover every query parameter exactly."
            )

    @property
    def parameter_definition(self) -> str:
        return ", ".join(
            f"{parameter.name} {parameter.sql_type.sql_declaration}"
            for parameter in self.parameters
        )

    @property
    def driver_sql(self) -> str:
        return _replace_parameter_tokens(
            self.sql_text,
            {parameter.name.lstrip("@").casefold() for parameter in self.parameters},
        )

    @property
    def driver_values(self) -> tuple[Any, ...]:
        by_name = {
            parameter.name.lstrip("@").casefold(): parameter.value
            for parameter in self.parameters
        }
        return tuple(
            by_name[name.casefold()]
            for name in _scan_parameter_tokens(
                self.sql_text,
                set(by_name),
            )[1]
        )

    @property
    def sp_executesql_sql(self) -> str:
        # Keep every procedure argument positional. SQL Server rejects positional
        # arguments after named @stmt/@params arguments, while naming user
        # arguments can collide with a legitimate query parameter called @stmt
        # or @params. Parameter order is already fixed by parameter_definition.
        placeholders = ", ".join("?" for _ in range(2 + len(self.parameters)))
        return f"EXEC sys.sp_executesql {placeholders}"

    @property
    def sp_executesql_values(self) -> tuple[Any, ...]:
        return (
            self.sql_text,
            self.parameter_definition,
            *(parameter.value for parameter in self.parameters),
        )

    @property
    def sp_executesql_input_sizes(
        self,
    ) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
        """Bind only ``sp_executesql``'s Unicode control arguments.

        The user parameters intentionally remain absent from this list so
        mssql-python can infer their native types from their values.
        """

        return SP_EXECUTESQL_CONTROL_INPUT_SIZES

    def to_dict(self) -> dict[str, Any]:
        return {
            "bucket_id": self.bucket_id,
            "provenance": self.provenance,
            "sql_text": self.sql_text,
            "parameter_definition": self.parameter_definition,
            "parameters": [parameter.to_dict() for parameter in self.parameters],
            "driver_sql": self.driver_sql,
            "driver_values": list(self.driver_values),
            "sp_executesql_sql": self.sp_executesql_sql,
            "sp_executesql_values": list(self.sp_executesql_values),
        }


# Descriptive aliases kept intentionally small so integrations can adopt the
# contract vocabulary without coupling to the initial class names.
ParameterBucket = TypedParameterBucket
TypedParameterExecutionContract = ParameterExecutionContract


def _replace_parameter_tokens(sql: str, parameter_keys: set[str]) -> str:
    """Replace parameter tokens without touching strings, identifiers, or comments."""

    return _scan_parameter_tokens(sql, parameter_keys)[0]


def _bracketed_identifier_end(sql: str, start: int) -> int:
    index = start + 1
    while index < len(sql):
        if sql[index] == "]":
            index += 1
            if index < len(sql) and sql[index] == "]":
                index += 1
                continue
            return index
        index += 1
    return len(sql)


def _double_quoted_identifier_end(sql: str, start: int) -> int:
    index = start + 1
    while index < len(sql):
        if sql[index] == '"':
            index += 1
            if index < len(sql) and sql[index] == '"':
                index += 1
                continue
            return index
        index += 1
    return len(sql)


def _nested_block_comment_end(sql: str, start: int) -> int:
    index = start + 2
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


def _mask_parameter_non_code(sql: str) -> str:
    """Mask literals, comments, and delimited identifiers in one lexical pass."""

    output: list[str] = []
    index = 0
    while index < len(sql):
        start = index
        if sql[index] in {"N", "n"} and index + 1 < len(sql) and sql[index + 1] == "'":
            index += 1
        if sql[index] == "'":
            index += 1
            while index < len(sql):
                if sql[index] != "'":
                    index += 1
                    continue
                index += 1
                if index < len(sql) and sql[index] == "'":
                    index += 1
                    continue
                break
        elif sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            index = len(sql) if newline < 0 else newline
        elif sql.startswith("/*", index):
            index = _nested_block_comment_end(sql, index)
        elif sql[index] == "[":
            end = _bracketed_identifier_end(sql, index)
            index = end
        elif sql[index] == '"':
            index = _double_quoted_identifier_end(sql, index)
        else:
            output.append(sql[index])
            index += 1
            continue
        output.extend("\n" if character == "\n" else " " for character in sql[start:index])
    return "".join(output)


def _scan_parameter_tokens(
    sql: str,
    parameter_keys: set[str],
) -> tuple[str, tuple[str, ...]]:
    """Return qmark SQL and the exact user-parameter occurrence order."""

    output: list[str] = []
    occurrences: list[str] = []
    index = 0
    length = len(sql)
    while index < length:
        if sql[index] == "'":
            start = index
            index += 1
            while index < length:
                if sql[index] == "'":
                    index += 1
                    if index < length and sql[index] == "'":
                        index += 1
                        continue
                    break
                index += 1
            output.append(sql[start:index])
            continue
        if sql.startswith("--", index):
            end = sql.find("\n", index)
            end = length if end < 0 else end
            output.append(sql[index:end])
            index = end
            continue
        if sql.startswith("/*", index):
            end = _nested_block_comment_end(sql, index)
            output.append(sql[index:end])
            index = end
            continue
        if sql[index] == "[":
            end = _bracketed_identifier_end(sql, index)
            output.append(sql[index:end])
            index = end
            continue
        if sql[index] == '"':
            end = _double_quoted_identifier_end(sql, index)
            output.append(sql[index:end])
            index = end
            continue
        match = PARAM_PATTERN.match(sql, index)
        if match and not (index > 0 and sql[index - 1] == "@"):
            name = match.group(1)
            if name.casefold() in parameter_keys:
                output.append("?")
                occurrences.append(name)
                index = match.end()
                continue
        output.append(sql[index])
        index += 1
    return "".join(output), tuple(occurrences)


def _fallback_driver_value(sql_type: SqlParameterType) -> Any:
    base_type = sql_type.base_type
    if base_type in {"int", "bigint", "smallint", "tinyint"}:
        return 1
    if base_type == "bit":
        return True
    if base_type in {"decimal", "numeric", "money", "smallmoney"}:
        return Decimal("1.0")
    if base_type in {"float", "real"}:
        return 1.0
    if base_type in {"char", "nchar", "varchar", "nvarchar", "text", "ntext"}:
        return "test"
    if base_type == "date":
        return date(2000, 1, 1)
    if base_type in {"datetime", "datetime2", "smalldatetime", "datetimeoffset"}:
        return datetime(2000, 1, 1, 12, 0, 0)
    if base_type == "time":
        return time(12, 0, 0)
    if base_type == "uniqueidentifier":
        return UUID("00000000-0000-0000-0000-000000000001")
    if base_type in {"binary", "varbinary", "image"}:
        return b"\x00"
    if base_type in {"xml", "sql_variant"}:
        return "<root/>" if base_type == "xml" else 1
    return None


def detect_parameters(sql: str) -> list[str]:
    """18.1: Detect @param style placeholders in T-SQL query text.

    Returns unique parameter names in order of first appearance.
    Excludes known system variables (@@ROWCOUNT, @@IDENTITY, etc.).
    """
    code = _mask_parameter_non_code(sql)
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

    async def build_parameter_bucket(
        self,
        database_name: str,
        sql: str,
        parameter_values: dict[str, Any] | None = None,
        *,
        parameter_types: Mapping[str, str] | None = None,
        bucket_id: str = "auto",
        label: str | None = None,
        provenance: str = "parameter_binding",
    ) -> TypedParameterBucket:
        """Build one canonical typed bucket for repeatable query comparisons.

        The bucket contains raw driver values, not SQL literals. It can be
        passed unchanged to :meth:`build_execution_contract` for both a
        baseline and a candidate query.
        """
        normalized_values = self._normalize_explicit_values(parameter_values)
        normalized_types = {
            str(name).lstrip("@").casefold(): str(data_type)
            for name, data_type in (parameter_types or {}).items()
        }
        param_names = detect_parameters(sql)
        detected_names = {name.casefold() for name in param_names}
        unknown_names = sorted(set(normalized_values) - detected_names)
        if unknown_names:
            raise ValueError(
                "explicit value supplied for unknown parameter(s): "
                + ", ".join(unknown_names)
            )
        unknown_types = sorted(set(normalized_types) - detected_names)
        if unknown_types:
            raise ValueError(
                "explicit type supplied for unknown parameter(s): "
                + ", ".join(unknown_types)
            )

        resolve_from_database = any(
            name.casefold() not in normalized_types
            or name.casefold() not in normalized_values
            for name in param_names
        )
        param_info = (
            await self._resolve_parameters(database_name, sql, param_names)
            if resolve_from_database
            else {}
        )
        parameters: list[TypedParameter] = []
        for name in param_names:
            info = next(
                (
                    value
                    for key, value in param_info.items()
                    if key.casefold() == name.casefold()
                ),
                {},
            )
            key = name.casefold()
            sql_type = SqlParameterType.from_sql(
                normalized_types.get(
                    key,
                    str(info.get("data_type", SAFE_FALLBACK_TYPE)),
                )
            )
            if key in normalized_values:
                value = self._coerce_driver_value(sql_type, normalized_values[key])
                source = (
                    "explicit_value_and_type"
                    if key in normalized_types
                    else "explicit_value"
                )
            elif "raw_value" in info and info.get("raw_value") is not None:
                value = self._coerce_driver_value(sql_type, info["raw_value"])
                source = str(info.get("source", "histogram"))
            else:
                value = _fallback_driver_value(sql_type)
                source = str(info.get("source", "type_fallback"))
            self._validate_driver_value(sql_type, value)
            detail = {
                key: value
                for key, value in info.items()
                if key in {"table_name", "schema_name", "column_name", "stats_id", "equal_rows"}
            }
            parameters.append(
                TypedParameter(
                    name=f"@{name}",
                    sql_type=sql_type,
                    value=value,
                    provenance=source,
                    provenance_detail=detail,
                )
            )
        return TypedParameterBucket(
            bucket_id=bucket_id,
            parameters=tuple(parameters),
            provenance=provenance,
            label=label,
        )

    async def build_typed_parameter_bucket(
        self,
        database_name: str,
        sql: str,
        parameter_values: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> TypedParameterBucket:
        """Compatibility alias for callers that use the explicit type name."""
        return await self.build_parameter_bucket(
            database_name,
            sql,
            parameter_values,
            **kwargs,
        )

    def build_execution_contract(
        self,
        sql: str,
        bucket: TypedParameterBucket,
        *,
        provenance: str | None = None,
    ) -> ParameterExecutionContract:
        """Bind SQL to an existing typed bucket without changing its values."""
        return bucket.for_sql(sql, provenance=provenance)

    def build_parameter_execution_contract(
        self,
        sql: str,
        bucket: TypedParameterBucket,
        *,
        provenance: str | None = None,
    ) -> ParameterExecutionContract:
        """Compatibility alias for the explicit execution API."""
        return self.build_execution_contract(sql, bucket, provenance=provenance)

    def build_comparison_contracts(
        self,
        baseline_sql: str,
        candidate_sql: str,
        bucket: TypedParameterBucket,
    ) -> dict[str, ParameterExecutionContract]:
        """Return baseline/candidate contracts sharing one canonical bucket."""
        return {
            "baseline": bucket.for_sql(baseline_sql, provenance="baseline"),
            "candidate": bucket.for_sql(candidate_sql, provenance="candidate"),
        }

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
        bucket = await self.build_parameter_bucket(
            database_name,
            sql,
            parameter_values,
        )
        if not bucket.parameters:
            contract = bucket.for_sql(sql)
            return {
                "original_sql": sql,
                "bound_sql": sql,
                "parameters": [],
                "typed_bucket": bucket.to_dict(),
                "execution_contract": contract.to_dict(),
            }

        # Compatibility representation for existing callers. New measurement
        # code should use execution_contract, which retains raw typed values.
        declare_lines: list[str] = []
        set_lines: list[str] = []
        parameters: list[dict[str, Any]] = []
        contract = bucket.for_sql(sql)
        for parameter in bucket.parameters:
            literal = self._format_literal(
                parameter.sql_type.sql_declaration,
                parameter.value,
            )
            declare_lines.append(
                f"DECLARE {parameter.name} {parameter.sql_type.sql_declaration};"
            )
            set_lines.append(f"SET {parameter.name} = {literal};")
            parameters.append({
                "name": parameter.name,
                "data_type": parameter.sql_type.sql_declaration,
                "value": literal,
                "raw_value": parameter.value,
                "source": parameter.provenance,
                "provenance": parameter.provenance,
                "provenance_detail": dict(parameter.provenance_detail),
                "length": parameter.sql_type.length,
                "precision": parameter.sql_type.precision,
                "scale": parameter.sql_type.scale,
            })

        binding_block = "\n".join(declare_lines + set_lines)
        bound_sql = f"{binding_block}\n\n{sql}"

        return {
            "original_sql": sql,
            "bound_sql": bound_sql,
            "parameters": parameters,
            "typed_bucket": bucket.to_dict(),
            "execution_contract": contract.to_dict(),
            "compatibility_wrapper": "local_declaration_set",
        }

    @staticmethod
    def _normalize_explicit_values(
        parameter_values: dict[str, Any] | None,
    ) -> dict[str, Any]:
        normalized_values: dict[str, Any] = {}
        for raw_name, value in (parameter_values or {}).items():
            name = str(raw_name).lstrip("@").strip().casefold()
            if not name:
                raise ValueError("explicit parameter name must not be empty")
            if name in normalized_values:
                raise ValueError(f"duplicate explicit parameter name: {name}")
            normalized_values[name] = value
        return normalized_values

    @staticmethod
    def _validate_driver_value(sql_type: SqlParameterType, value: Any) -> None:
        if value is None:
            return
        base_type = sql_type.base_type
        if base_type in {"int", "bigint", "smallint", "tinyint"}:
            try:
                integer_value = _parse_integral_value(value)
            except ValueError as exc:
                raise ValueError(
                    f"explicit value is not valid for {sql_type.sql_declaration}"
                ) from exc
            limits = {
                "tinyint": (0, 255),
                "smallint": (-32768, 32767),
                "int": (-2147483648, 2147483647),
                "bigint": (-9223372036854775808, 9223372036854775807),
            }
            lower, upper = limits[base_type]
            if not lower <= integer_value <= upper:
                raise ValueError(f"value is outside {sql_type.sql_declaration} range")
        if base_type == "bit":
            try:
                _parse_bit_value(value)
            except ValueError as exc:
                raise ValueError(
                    f"explicit value is not valid for {sql_type.sql_declaration}"
                ) from exc
        if base_type in {"varchar", "nvarchar", "char", "nchar", "text", "ntext"}:
            if sql_type.length not in (None, "max") and len(str(value)) > int(sql_type.length):
                raise ValueError(
                    f"value exceeds {sql_type.sql_declaration} length: {len(str(value))}"
                )
        if base_type in {"binary", "varbinary", "image"}:
            byte_length = len(value) if isinstance(value, bytes) else len(str(value).removeprefix("0x")) // 2
            if sql_type.length not in (None, "max") and byte_length > int(sql_type.length):
                raise ValueError(
                    f"value exceeds {sql_type.sql_declaration} length: {byte_length}"
                )
        if base_type in {"decimal", "numeric"} and value is not None:
            try:
                decimal_value = Decimal(str(value))
            except (InvalidOperation, ValueError) as exc:
                raise ValueError(f"explicit value is not valid for {sql_type.sql_declaration}") from exc
            if sql_type.scale is not None:
                exponent = decimal_value.as_tuple().exponent
                actual_scale = max(0, -exponent) if isinstance(exponent, int) else 0
                if actual_scale > sql_type.scale:
                    raise ValueError(
                        f"value scale exceeds {sql_type.sql_declaration}: {actual_scale}"
                    )

    @staticmethod
    def _coerce_driver_value(sql_type: SqlParameterType, value: Any) -> Any:
        """Undo the catalog query's NVARCHAR histogram projection for drivers."""
        if value is None:
            return None
        base_type = sql_type.base_type
        if base_type in {"int", "bigint", "smallint", "tinyint"}:
            try:
                return _parse_integral_value(value)
            except ValueError as exc:
                raise ValueError(
                    f"explicit value is not valid for {sql_type.sql_declaration}"
                ) from exc
        if base_type == "bit":
            try:
                return _parse_bit_value(value)
            except ValueError as exc:
                raise ValueError(
                    f"explicit value is not valid for {sql_type.sql_declaration}"
                ) from exc
        text = str(value)
        try:
            if base_type in {"decimal", "numeric", "money", "smallmoney"}:
                return Decimal(text)
            if base_type in {"float", "real"}:
                return float(text)
            if base_type == "date":
                return date.fromisoformat(text[:10])
            if base_type in {"datetime", "datetime2", "datetimeoffset", "smalldatetime"}:
                return datetime.fromisoformat(text.replace("Z", "+00:00"))
            if base_type == "time":
                return time.fromisoformat(text)
            if base_type == "uniqueidentifier":
                return UUID(text)
            if base_type in {"binary", "varbinary", "image"}:
                return bytes.fromhex(text.removeprefix("0x"))
        except (TypeError, ValueError, InvalidOperation):
            # Keep the original catalog value so the caller can still inspect
            # provenance; explicit typed validation will reject unsafe values.
            return value
        return value

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
            return {
                "data_type": data_type,
                "value": None,
                "raw_value": None,
                "source": "type_fallback",
                "table_name": table_name,
                "schema_name": schema_name,
                "column_name": column_name,
                "stats_id": stats_id,
            }

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
            return {
                "data_type": data_type,
                "value": None,
                "raw_value": None,
                "source": "type_fallback",
                "table_name": table_name,
                "schema_name": schema_name,
                "column_name": column_name,
                "stats_id": stats_id,
            }

        if not hist_rows:
            return {
                "data_type": data_type,
                "value": None,
                "raw_value": None,
                "source": "type_fallback",
                "table_name": table_name,
                "schema_name": schema_name,
                "column_name": column_name,
                "stats_id": stats_id,
            }

        raw_value = hist_rows[0].get("range_high_key")
        if raw_value is None:
            return {
                "data_type": data_type,
                "value": None,
                "raw_value": None,
                "source": "type_fallback",
                "table_name": table_name,
                "schema_name": schema_name,
                "column_name": column_name,
                "stats_id": stats_id,
            }

        formatted_value = self._format_literal(data_type, raw_value)
        return {
            "data_type": data_type,
            "value": formatted_value,
            "raw_value": raw_value,
            "source": "histogram",
            "table_name": table_name,
            "schema_name": schema_name,
            "column_name": column_name,
            "stats_id": stats_id,
            "equal_rows": hist_rows[0].get("equal_rows"),
        }

    def _format_data_type(self, row: dict[str, Any]) -> str:
        """Format a SQL Server data type from column metadata."""
        base_type = str(row.get("data_type", "nvarchar")).lower()
        if base_type not in SUPPORTED_PARAMETER_TYPES:
            return SAFE_FALLBACK_TYPE
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
            if isinstance(value, bytes):
                return "0x" + value.hex()
            hex_value = str_value[2:] if str_value.lower().startswith("0x") else str_value
            if not re.fullmatch(r"[0-9a-fA-F]*", hex_value):
                raise ValueError(f"explicit value is not valid for {data_type}")
            return f"0x{hex_value}"

        escaped = str_value.replace("'", "''")
        return f"N'{escaped}'"
