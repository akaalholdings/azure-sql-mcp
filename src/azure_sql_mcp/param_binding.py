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
from typing import Sequence
from uuid import UUID

from mssql_python.constants import ConstantsDDBC

from .connection import AzureSqlExecutor
from .incident_log import note_exception
from .query_text import split_parameter_declarations
from .safe_sql import strip_literals_and_comments

SP_EXECUTESQL_CONTROL_INPUT_SIZES = (
    (int(ConstantsDDBC.SQL_WVARCHAR.value), 0, 0),
    (int(ConstantsDDBC.SQL_WVARCHAR.value), 0, 0),
)

logger = logging.getLogger(__name__)

PARAM_PATTERN = re.compile(r"@(\w+)")
_DECLARE_KEYWORD_PATTERN = re.compile(r"\bDECLARE\b", re.IGNORECASE)
_STATEMENT_KEYWORD_PATTERN = re.compile(
    r"(?:SELECT|WITH|INSERT|UPDATE|DELETE|MERGE|EXEC(?:UTE)?|IF|BEGIN|SET|"
    r"DECLARE|WHILE|RETURN|PRINT)\b",
    re.IGNORECASE,
)
_DECLARATOR_NAME_PATTERN = re.compile(r"\s*@(?P<name>\w+)\s+\S")

# Type names come from the catalog and are embedded into DECLARE statements, so
# they must look like a plain (optionally parameterized) type. Anything else,
# such as a hostile user-defined type name, leaves the parameter unbound.
SAFE_TYPE_PATTERN = re.compile(
    r"^[a-z_][a-z0-9_]*(?:\(\s*(?:max|\d{1,5}(?:\s*,\s*\d{1,5})?)\s*\))?$",
    re.IGNORECASE,
)
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

# Python datetime and time keep microseconds; a histogram key with a seventh
# fractional digit must stay text or the bound value is not the key.
_SUBMICROSECOND_PATTERN = re.compile(r"\.\d{6}[1-9]")

# Why a parameter has no faithful type or value. The text holds no values.
UNBOUND_REASONS: dict[str, str] = {
    "no_column_mapping": (
        "No comparison between this parameter and a column was found, so its "
        "type and a representative value cannot be read from the catalog."
    ),
    "no_table_reference": (
        "The statement names no base table that could hold the compared column."
    ),
    "column_not_found": (
        "The compared column was not found on the tables the statement names."
    ),
    "ambiguous_column": (
        "The compared column exists on more than one table the statement names."
    ),
    "unsupported_type": (
        "The column's type cannot be declared as a parameter type."
    ),
    "catalog_unreadable": "The column's type could not be read from the catalog.",
    "type_unknown": "A value was supplied but the SQL type is unknown.",
    "no_leading_statistics": (
        "No statistics object has this column as its leading column, so there is "
        "no representative value."
    ),
    "empty_histogram": (
        "The statistics object that leads with this column has no histogram step "
        "with rows."
    ),
    "histogram_unreadable": "The statistics histogram could not be read.",
    "histogram_value_unusable": (
        "The statistics value does not fit the parameter's declared type."
    ),
}


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
    # False: the type is exact but no value is known. Such a parameter can be
    # compiled for an estimated plan (as unknown) and never executed.
    value_known: bool = True

    def __post_init__(self) -> None:
        # Simple and forced parameterization name parameters @1, @2, ...
        if not re.fullmatch(r"@\w+", self.name):
            raise ValueError(f"Invalid parameter name: {self.name!r}")
        if not self.provenance:
            raise ValueError("Parameter provenance must not be empty.")

    def to_dict(self) -> dict[str, Any]:
        result = {
            "name": self.name,
            **self.sql_type.to_dict(),
            "value": self.value,
            "provenance": self.provenance,
            "provenance_detail": dict(self.provenance_detail),
        }
        if not self.value_known:
            result["value_known"] = False
        return result


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


def format_sql_literal(data_type: str, value: Any) -> str:
    """Format a raw value as a T-SQL literal."""
    base_type = data_type.lower().split("(")[0].strip()
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, datetime) and base_type in {"datetime", "smalldatetime"}:
        # datetime accepts at most 3 fractional digits from a string.
        str_value = value.replace(tzinfo=None).isoformat(timespec="milliseconds")
    elif isinstance(value, (date, time)):
        str_value = value.isoformat()
    else:
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

    def _require_known_values(self) -> None:
        unknown = [
            parameter.name for parameter in self.parameters if not parameter.value_known
        ]
        if unknown:
            raise ValueError(
                "Parameter(s) "
                + ", ".join(unknown)
                + " have a SQL type but no value: the statement can be compiled "
                "for an estimated plan, not executed. Supply parameter_values."
            )

    @property
    def driver_values(self) -> tuple[Any, ...]:
        self._require_known_values()
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
        self._require_known_values()
        return (
            self.sql_text,
            self.parameter_definition,
            *(parameter.value for parameter in self.parameters),
        )

    @property
    def estimated_showplan_sql(self) -> str:
        """The user statement compiled for the typed values, for SET SHOWPLAN_XML.

        SHOWPLAN_XML does not compile the statement inside ``EXEC sp_executesql``
        (it returns an EXECUTE PROC stub with no plan), and mssql-python returns
        no result set for parameter markers under it (live, Azure SQL Database).
        Typed local variables with no initializer plus ``OPTIMIZE FOR`` the typed
        constants compile the statement itself with the values sniffed. An
        initialized variable would compile a density estimate instead.

        This is an approximation, not the parameterized plan: a local variable
        never gets parameter-sensitive (PSP) or optional-parameter dispatcher
        plan variants, nor parameter embedding under RECOMPILE, so for a
        catch-all predicate such as ``(@p IS NULL OR col = @p)`` the shape can
        differ from the parameterized run (live: Index Scan here, Dispatcher
        plus Index Seek with analyze=true). ``estimated_showplan_basis`` states
        this. xml parameters stay out of ``OPTIMIZE FOR`` (SQL Server rejects an
        xml literal there) and compile as unknown.
        """

        if not self.parameters:
            return self.sql_text
        sql = self.sql_text
        code = _mask_parameter_non_code(sql)
        # Drop a leading or trailing statement terminator; any other one means
        # a second statement that the hint must not be attached to.
        for index in (len(code) - len(code.lstrip()), len(code.rstrip()) - 1):
            if 0 <= index < len(code) and code[index] == ";":
                sql = sql[:index] + " " + sql[index + 1:]
                code = code[:index] + " " + code[index + 1:]
        if ";" in code or _LEADING_VARIABLE_STATEMENT.match(code.lstrip()):
            raise ValueError(
                "An estimated plan for typed parameters needs a single SELECT "
                "statement; sql_text holds more than one statement. Remove the "
                "DECLARE or SET prefix, or use analyze=true."
            )
        hinted = self._optimize_for_parameters()
        declare = f"DECLARE {self.parameter_definition};\n"
        if not hinted:
            # OPTIMIZE FOR () is a syntax error; nothing to hint.
            return f"{declare}{sql}"
        hint = "OPTIMIZE FOR ({})".format(
            ", ".join(
                f"{parameter.name} = "
                + format_sql_literal(parameter.sql_type.sql_declaration, parameter.value)
                for parameter in hinted
            )
        )
        option_open = _top_level_option_open(code)
        if option_open is None:
            # A new line, so a trailing line comment cannot swallow the hint.
            return f"{declare}{sql}\nOPTION ({hint})"
        if _OPTIMIZE_FOR_PATTERN.search(code, option_open):
            raise ValueError(
                "sql_text already has an OPTIMIZE FOR hint; an estimated plan for "
                "typed parameters adds its own. Remove that hint or use analyze=true."
            )
        return f"{declare}{sql[:option_open]}{hint}, {sql[option_open:]}"

    def _optimize_for_parameters(self) -> tuple[TypedParameter, ...]:
        # OPTIMIZE FOR rejects an xml literal ("could not be implicitly
        # converted", live); an xml value does not drive cardinality anyway.
        # A parameter with no known value compiles as unknown.
        return tuple(
            parameter
            for parameter in self.parameters
            if parameter.value_known
            and parameter.sql_type.base_type.strip().lower() != "xml"
        )

    @property
    def estimated_showplan_basis(self) -> dict[str, Any]:
        """What the ``estimated_showplan_sql`` plan models and what it does not."""

        hinted = {parameter.name for parameter in self._optimize_for_parameters()}
        return {
            "basis": "local_variables_optimize_for",
            "optimize_for_parameters": [
                parameter.name for parameter in self.parameters if parameter.name in hinted
            ],
            "unknown_parameters": [
                parameter.name for parameter in self.parameters if parameter.name not in hinted
            ],
            "not_modeled": [
                "parameter_sensitive_plan",
                "optional_parameter_plan",
                "parameter_embedding",
            ],
            "note": (
                "Estimated from typed local variables with OPTIMIZE FOR the values. "
                "PSP and optional-parameter dispatcher plan variants and parameter "
                "embedding (RECOMPILE) are not modeled, so the shape can differ from "
                "the parameterized plan; use analyze=true for the parameterized plan."
            ),
        }

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
        executable = all(parameter.value_known for parameter in self.parameters)
        return {
            "bucket_id": self.bucket_id,
            "provenance": self.provenance,
            "sql_text": self.sql_text,
            "parameter_definition": self.parameter_definition,
            "parameters": [parameter.to_dict() for parameter in self.parameters],
            "driver_sql": self.driver_sql,
            "driver_values": list(self.driver_values) if executable else None,
            "sp_executesql_sql": self.sp_executesql_sql,
            "sp_executesql_values": (
                list(self.sp_executesql_values) if executable else None
            ),
        }


# Descriptive aliases kept intentionally small so integrations can adopt the
# contract vocabulary without coupling to the initial class names.
ParameterBucket = TypedParameterBucket
TypedParameterExecutionContract = ParameterExecutionContract


_LEADING_VARIABLE_STATEMENT = re.compile(r"(?:DECLARE|SET)\b", re.IGNORECASE)
_OPTION_CLAUSE_PATTERN = re.compile(r"\bOPTION\s*\(", re.IGNORECASE)
_OPTIMIZE_FOR_PATTERN = re.compile(r"\bOPTIMIZE\s+FOR\b", re.IGNORECASE)


def _top_level_option_open(code: str) -> int | None:
    """Index just after ``OPTION (`` of the statement's own query-hint clause.

    ``code`` is masked SQL, so literals, comments and delimited identifiers
    cannot match. Clauses inside parentheses (subqueries, CTE bodies) are skipped.
    """

    depth = 0
    position = 0
    for match in _OPTION_CLAUSE_PATTERN.finditer(code):
        depth += code.count("(", position, match.start()) - code.count(
            ")", position, match.start()
        )
        position = match.start()
        if depth == 0:
            return match.end()
    return None


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


def _declared_variable_names(code: str) -> set[str]:
    """Names declared by the DECLARE statements in masked code.

    A declarator ends at a top-level comma, and the statement at a top-level
    semicolon or statement keyword. Parentheses are tracked, so an initializer
    such as ``= (SELECT MAX(x) FROM t)`` does not end the statement early and
    hide the variables declared after it.
    """

    names: set[str] = set()
    length = len(code)
    for declare in _DECLARE_KEYWORD_PATTERN.finditer(code):
        depth = 0
        segment_start = index = declare.end()
        while True:
            char = code[index] if index < length else ";"
            ends_statement = depth == 0 and (
                char == ";"
                or (
                    char.isalpha()
                    and not _is_identifier_character(code[index - 1])
                    and _STATEMENT_KEYWORD_PATTERN.match(code, index) is not None
                )
            )
            if ends_statement or (depth == 0 and char == ","):
                declarator = _DECLARATOR_NAME_PATTERN.match(code, segment_start, index)
                if declarator is not None:
                    names.add(declarator.group("name").casefold())
                if ends_statement:
                    break
                segment_start = index + 1
            elif char == "(":
                depth += 1
            elif char == ")":
                depth = max(0, depth - 1)
            index += 1
    return names


def _is_identifier_character(character: str) -> bool:
    return character.isalnum() or character in "_@#$"


def detect_parameters(sql: str) -> list[str]:
    """18.1: Detect @param style placeholders in T-SQL query text.

    Returns unique parameter names in order of first appearance.
    Excludes known system variables (@@ROWCOUNT, @@IDENTITY, etc.) and the
    variables the text declares itself.
    """
    code = _mask_parameter_non_code(sql)
    declared = _declared_variable_names(code)
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


@dataclass(frozen=True, slots=True)
class UnboundParameter:
    """A parameter with no faithful SQL type or value, and why."""

    name: str
    reason_code: str
    # Set when only the value is missing.
    data_type: str | None = None

    @property
    def reason(self) -> str:
        return UNBOUND_REASONS[self.reason_code]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "data_type": self.data_type,
        }


class ParameterBindingError(ValueError):
    """Raised when a parameter has no faithful SQL type or value.

    The message names parameters and reason codes only. It holds no values.
    """

    def __init__(self, unbound: Sequence[UnboundParameter]) -> None:
        self.unbound = tuple(unbound)
        listed = ", ".join(f"{item.name} ({item.reason_code})" for item in self.unbound)
        super().__init__(
            f"Parameter(s) could not be bound: {listed}. "
            "Supply parameter_values and parameter_types for them."
        )


@dataclass(frozen=True, slots=True)
class ParameterBindingResult:
    """The parameters that were bound, and the ones that were not."""

    bucket: TypedParameterBucket
    unbound: tuple[UnboundParameter, ...] = ()

    @property
    def complete(self) -> bool:
        return not self.unbound

    def to_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "typed_bucket": self.bucket.to_dict(),
            "unbound_parameters": [item.to_dict() for item in self.unbound],
        }


@dataclass(frozen=True, slots=True)
class QueryStoreTextBinding:
    """Query Store text split into its statement and a typed contract.

    ``declared_types`` are the types Query Store stored, by parameter name.
    ``contract`` is None when the statement has no parameters.
    """

    statement: str
    declared_types: Mapping[str, str]
    contract: ParameterExecutionContract | None

    @property
    def showplan_sql(self) -> str:
        """The text to compile under SET SHOWPLAN_XML ON.

        The statement itself, with the declared types and the known values
        sniffed through OPTIMIZE FOR. A statement that has its own OPTIMIZE FOR
        hint keeps it: only the typed declarations are added.
        """

        if self.contract is None or not self.contract.parameters:
            return self.statement
        if _OPTIMIZE_FOR_PATTERN.search(_mask_parameter_non_code(self.statement)):
            return f"DECLARE {self.contract.parameter_definition};\n{self.statement}"
        return self.contract.estimated_showplan_sql


@dataclass(frozen=True, slots=True)
class _TableReference:
    schema: str | None
    table: str
    alias: str | None


_IDENTIFIER = r'(?:\[[^\]]+\]|"[^"]+"|[A-Za-z_][\w$#@]*)'
_TABLE_CLAUSE_PATTERN = re.compile(r"\b(?:FROM|JOIN|APPLY|UPDATE|INTO)\s+", re.IGNORECASE)
# The lookaheads stop a shorter match that would cut a name in two or take a
# function call (dbo.fn(...)) as a table.
_TABLE_REFERENCE_PATTERN = re.compile(
    rf"(?P<name>{_IDENTIFIER}(?:\s*\.\s*{_IDENTIFIER}){{0,2}})(?![\w$#@])(?!\s*[.(])"
    rf"(?:\s+(?:AS\s+)?(?P<alias>{_IDENTIFIER})(?![\w$#@]))?",
    re.IGNORECASE,
)
_TABLE_LIST_SEPARATOR = re.compile(r"\s*,\s*")
_NOT_AN_ALIAS = frozenset(
    {
        "AS", "CROSS", "EXCEPT", "FOR", "FULL", "GROUP", "HAVING", "INNER",
        "INTERSECT", "JOIN", "LEFT", "ON", "OPTION", "ORDER", "OUTER", "OUTPUT",
        "PIVOT", "RIGHT", "SELECT", "SET", "TABLESAMPLE", "UNION", "UNPIVOT",
        "USING", "VALUES", "WHEN", "WHERE", "WITH",
    }
)
_MAX_CANDIDATE_TABLES = 20


def _unquote_identifier(identifier: str) -> str:
    if identifier.startswith("["):
        return identifier[1:-1].replace("]]", "]")
    if identifier.startswith('"'):
        return identifier[1:-1].replace('""', '"')
    return identifier


def _table_references(code: str) -> list[_TableReference]:
    """Base tables named in FROM, JOIN, APPLY, UPDATE and INTO, with aliases.

    ``code`` has literals and comments removed. Temporary tables, table
    variables and table-valued functions are skipped: none has statistics this
    database can read.
    """

    references: list[_TableReference] = []
    for clause in _TABLE_CLAUSE_PATTERN.finditer(code):
        position = clause.end()
        while True:
            match = _TABLE_REFERENCE_PATTERN.match(code, position)
            if match is None:
                break
            parts = [
                _unquote_identifier(part)
                for part in re.findall(_IDENTIFIER, match.group("name"))
            ]
            alias = match.group("alias")
            end = match.end()
            if alias is not None and alias.upper() in _NOT_AN_ALIAS:
                alias = None
                end = match.end("name")
            references.append(
                _TableReference(
                    schema=parts[-2] if len(parts) >= 2 else None,
                    table=parts[-1],
                    alias=_unquote_identifier(alias) if alias else None,
                )
            )
            separator = _TABLE_LIST_SEPARATOR.match(code, end)
            if separator is None:
                break
            position = separator.end()
    return references


def _candidate_tables(
    table_hint: str | None,
    tables: Sequence[_TableReference],
) -> list[_TableReference]:
    """The tables that can own a column written as ``hint.column`` or ``column``."""

    if not table_hint:
        return list(tables)
    hint = table_hint.casefold()
    matched = [
        reference
        for reference in tables
        if hint in {(reference.alias or "").casefold(), reference.table.casefold()}
    ]
    if matched:
        return matched
    if tables:
        # The hint is a derived table or CTE name: the column still comes from
        # one of the statement's base tables.
        return list(tables)
    return [_TableReference(schema=None, table=table_hint, alias=None)]


# One row per statistics object that leads with the column, or one row with a
# NULL stats_id when none does. A histogram describes only the leading column.
_COLUMN_TYPE_SQL = """
SELECT TOP (200)
    s.name AS schema_name,
    t.name AS table_name,
    TYPE_NAME(c.system_type_id) AS data_type,
    c.max_length,
    c.precision,
    c.scale,
    st.stats_id,
    st.has_filter
FROM sys.columns AS c
INNER JOIN sys.tables AS t
    ON t.object_id = c.object_id
INNER JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
LEFT JOIN sys.stats_columns AS sc
    ON sc.object_id = c.object_id
   AND sc.column_id = c.column_id
   AND sc.stats_column_id = 1
LEFT JOIN sys.stats AS st
    ON st.object_id = sc.object_id
   AND st.stats_id = sc.stats_id
WHERE c.name = ?
  AND t.name IN ({tables})
ORDER BY s.name, t.name, st.has_filter, st.stats_id
"""

# range_high_key is sql_variant, which the driver cannot fetch: CONVERT it
# server-side (style 121 keeps date and time values ISO-formatted, with all
# seven fractional digits of a datetime2(7) key).
_HISTOGRAM_VALUE_SQL = """
SELECT TOP (1)
    CONVERT(NVARCHAR(4000), range_high_key, 121) AS range_high_key,
    equal_rows
FROM sys.dm_db_stats_histogram(
    OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?)),
    ?
)
WHERE equal_rows > 0
  AND range_high_key IS NOT NULL
ORDER BY equal_rows DESC
"""


def column_parameter_type(row: Mapping[str, Any]) -> str | None:
    """Format a sys.columns row as a parameter type; None when it cannot be one.

    ``row`` holds ``data_type`` (the system type name), ``max_length``,
    ``precision`` and ``scale``.
    """
    base_type = str(row.get("data_type") or "").lower()
    if base_type not in SUPPORTED_PARAMETER_TYPES:
        return None
    if base_type in LENGTH_PARAMETER_TYPES:
        max_length = int(row.get("max_length") or 0)
        if max_length == -1:
            return f"{base_type}(max)" if base_type in MAX_LENGTH_PARAMETER_TYPES else None
        length = max_length
        if base_type.startswith("n"):
            length = max(length // 2, 1)
        return f"{base_type}({length})"
    if base_type in {"decimal", "numeric"}:
        precision = int(row.get("precision") or 18)
        scale = int(row.get("scale") or 0)
        return f"{base_type}({precision},{scale})"
    if base_type in SCALE_PARAMETER_TYPES:
        scale = row.get("scale")
        return f"{base_type}({7 if scale is None else int(scale)})"
    return base_type


class ParameterBindingService:
    """Binds parameter placeholders to real types and values from statistics.

    A type comes from the caller, from the declarations Query Store stored, or
    from the compared column in the catalog. A value comes from the caller or
    from the histogram of a statistics object that leads with that column.
    A parameter with neither is reported as unbound; no value is made up.
    """

    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def resolve_parameter_bucket(
        self,
        database_name: str,
        sql: str,
        parameter_values: dict[str, Any] | None = None,
        *,
        parameter_types: Mapping[str, str] | None = None,
        bucket_id: str = "auto",
        label: str | None = None,
        provenance: str = "parameter_binding",
        allow_unknown_values: bool = False,
    ) -> ParameterBindingResult:
        """Bind what can be bound faithfully and report the rest as unbound.

        ``allow_unknown_values`` keeps a parameter whose type is known and whose
        value is not (``value_known`` False). Use it only to compile an
        estimated plan: such a contract refuses to execute.
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

        lookup_names = [
            name
            for name in param_names
            if name.casefold() not in normalized_types
            or name.casefold() not in normalized_values
        ]
        param_info = (
            await self._resolve_parameters(database_name, sql, lookup_names)
            if lookup_names
            else {}
        )
        parameters: list[TypedParameter] = []
        unbound: list[UnboundParameter] = []
        for name in param_names:
            key = name.casefold()
            info = param_info.get(name, {})
            reason = str(info.get("unbound_reason") or "no_column_mapping")
            declared = normalized_types.get(key) or info.get("data_type")
            if declared is None:
                unbound.append(
                    UnboundParameter(
                        name=f"@{name}",
                        reason_code="type_unknown" if key in normalized_values else reason,
                    )
                )
                continue
            try:
                sql_type = SqlParameterType.from_sql(str(declared))
            except ValueError:
                if key in normalized_types:
                    raise
                # A catalog type with metadata no parameter can carry.
                unbound.append(
                    UnboundParameter(name=f"@{name}", reason_code="unsupported_type")
                )
                continue
            value: Any = None
            value_known = True
            source = "type_only"
            if key in normalized_values:
                value = self._coerce_driver_value(sql_type, normalized_values[key])
                self._validate_driver_value(sql_type, value)
                source = (
                    "explicit_value_and_type"
                    if key in normalized_types
                    else "explicit_value"
                )
            elif info.get("raw_value") is not None:
                try:
                    value = self._coerce_driver_value(sql_type, info["raw_value"])
                    self._validate_driver_value(sql_type, value)
                    source = "histogram"
                except ValueError:
                    # The declared type is narrower than the column's.
                    value_known = False
                    reason = "histogram_value_unusable"
            else:
                value_known = False
            if not value_known:
                if not allow_unknown_values:
                    unbound.append(
                        UnboundParameter(
                            name=f"@{name}",
                            reason_code=reason,
                            data_type=sql_type.sql_declaration,
                        )
                    )
                    continue
                value = None
            detail = {
                field_name: info[field_name]
                for field_name in ("table_name", "schema_name", "column_name", "stats_id", "equal_rows")
                if field_name in info
            }
            if not value_known:
                detail["value_unknown_reason"] = reason
            parameters.append(
                TypedParameter(
                    name=f"@{name}",
                    sql_type=sql_type,
                    value=value,
                    provenance=source,
                    provenance_detail=detail,
                    value_known=value_known,
                )
            )
        return ParameterBindingResult(
            bucket=TypedParameterBucket(
                bucket_id=bucket_id,
                parameters=tuple(parameters),
                provenance=provenance,
                label=label,
            ),
            unbound=tuple(unbound),
        )

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
        allow_unknown_values: bool = False,
    ) -> TypedParameterBucket:
        """Build one canonical typed bucket for repeatable query comparisons.

        The bucket contains raw driver values, not SQL literals. It can be
        passed unchanged to :meth:`build_execution_contract` for both a
        baseline and a candidate query. Raises :class:`ParameterBindingError`
        when any parameter has no faithful type or value.
        """
        result = await self.resolve_parameter_bucket(
            database_name,
            sql,
            parameter_values,
            parameter_types=parameter_types,
            bucket_id=bucket_id,
            label=label,
            provenance=provenance,
            allow_unknown_values=allow_unknown_values,
        )
        if result.unbound:
            raise ParameterBindingError(result.unbound)
        return result.bucket

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
        """Detect parameters and bind them from explicit values or statistics.

        Returns dict with:
        - original_sql: the input SQL
        - bound_sql: SQL with a DECLARE/SET block prepended, or None when a
          parameter is unbound
        - parameters: list of {name, value, source, data_type}
        - unbound_parameters: list of {name, reason_code, reason, data_type}

        The DECLARE/SET text compiles local variables, which the optimizer
        does not sniff. Measurement code must use execution_contract.
        """
        result = await self.resolve_parameter_bucket(
            database_name,
            sql,
            parameter_values,
        )
        bucket = result.bucket
        unbound = [item.to_dict() for item in result.unbound]
        if not bucket.parameters and not unbound:
            contract = bucket.for_sql(sql)
            return {
                "original_sql": sql,
                "bound_sql": sql,
                "parameters": [],
                "unbound_parameters": [],
                "typed_bucket": bucket.to_dict(),
                "execution_contract": contract.to_dict(),
            }

        declare_lines: list[str] = []
        set_lines: list[str] = []
        parameters: list[dict[str, Any]] = []
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
        return {
            "original_sql": sql,
            "bound_sql": None if unbound else f"{binding_block}\n\n{sql}",
            "parameters": parameters,
            "unbound_parameters": unbound,
            "typed_bucket": bucket.to_dict(),
            "execution_contract": None if unbound else bucket.for_sql(sql).to_dict(),
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
        if base_type in SCALE_PARAMETER_TYPES and _SUBMICROSECOND_PATTERN.search(text):
            # sp_executesql converts the text to the declared type with all
            # seven digits; a Python value would drop the last one.
            return text
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

    async def bind_query_store_text(
        self,
        database_name: str,
        sql_text: str,
    ) -> QueryStoreTextBinding:
        """Bind Query Store text with the parameter types it was compiled with.

        Stored text often looks like ``(@P1 varchar(20),@P2 int)SELECT ...``.
        The declared types are kept exactly: re-inferring them from the column
        hides the implicit conversion production runs with. Values come from
        leading-column statistics; a parameter with no such value keeps its
        type and compiles as unknown. Raises :class:`ParameterBindingError`
        when a parameter has no declared type and none can be read.
        """
        declared, statement = split_parameter_declarations(sql_text)
        names = detect_parameters(statement)
        if not names:
            return QueryStoreTextBinding(statement, dict(declared), None)
        detected = {name.casefold() for name in names}
        bucket = await self.build_parameter_bucket(
            database_name,
            statement,
            parameter_types={
                name: data_type
                for name, data_type in declared
                if name.lstrip("@").casefold() in detected
            },
            bucket_id="query-store-text",
            provenance="query_store_declared_types",
            allow_unknown_values=True,
        )
        return QueryStoreTextBinding(
            statement,
            dict(declared),
            bucket.for_sql(statement, provenance="query_store_text"),
        )

    async def prepare_query_store_text(self, database_name: str, sql_text: str) -> str:
        """Turn Query Store text into SQL that compiles on its own.

        Stored text often looks like ``(@P1 int)SELECT ... WHERE x = @P1``.
        The bare body does not compile ("must declare the scalar variable"), so
        the parameters are declared with the exact types Query Store stored. No
        value is assigned: a local variable is never sniffed, so the plan uses
        density estimates either way. For a plan with sniffed values, compile
        ``bind_query_store_text(...).showplan_sql`` instead.
        """
        declared, statement = split_parameter_declarations(sql_text)
        names = detect_parameters(statement)
        if not names:
            return statement
        declared_types = {
            name.lstrip("@").casefold(): data_type for name, data_type in declared
        }
        if all(name.casefold() in declared_types for name in names):
            # No catalog read is needed when every type was stored.
            definition = ", ".join(
                f"@{name} "
                + SqlParameterType.from_sql(declared_types[name.casefold()]).sql_declaration
                for name in names
            )
        else:
            binding = await self.bind_query_store_text(database_name, sql_text)
            assert binding.contract is not None  # noqa: S101
            definition = binding.contract.parameter_definition
        return f"DECLARE {definition};\n{statement}"

    async def _resolve_parameters(
        self,
        database_name: str,
        sql: str,
        param_names: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Resolve each parameter's type and value from the column it is compared with.

        Looks for patterns such as 'column = @param', 'column BETWEEN @a AND @b'
        and 'column IN (@a, @b)'. Every name gets an entry; ``unbound_reason``
        says what is missing.
        """
        result: dict[str, dict[str, Any]] = {}
        column_mappings = self._extract_column_mappings(sql, param_names)
        tables = _table_references(strip_literals_and_comments(sql))
        # Parameters compared with the same column share one catalog read.
        resolved: dict[tuple[str, str], dict[str, Any]] = {}
        for param_name in param_names:
            mapping = column_mappings.get(param_name)
            if mapping is None:
                result[param_name] = {"unbound_reason": "no_column_mapping"}
                continue
            table_hint, column_name = mapping
            key = ((table_hint or "").casefold(), column_name.casefold())
            if key not in resolved:
                info = await self._resolve_from_stats(
                    database_name, table_hint, column_name, tables,
                )
                resolved[key] = (
                    info if info is not None else {"unbound_reason": "catalog_unreadable"}
                )
            result[param_name] = resolved[key]
        return result

    def _extract_column_mappings(
        self,
        sql: str,
        param_names: list[str],
    ) -> dict[str, tuple[str | None, str]]:
        """Extract column-to-param mappings from SQL patterns.

        Looks for: column = @param, @param = column, column > @param,
        column BETWEEN @a AND @b, column IN (@a, @b), etc.
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

        # Both bounds of a range and every item of an IN list compare with the
        # same column.
        column_then_list = re.compile(
            rf"(?:(?P<table>{identifier})\s*\.)?(?P<column>{identifier})\s+"
            r"(?:NOT\s+)?(?:BETWEEN\s+(?P<range>@\w+\s+AND\s+@\w+)"
            r"|IN\s*\(\s*(?P<list>@\w+(?:\s*,\s*@\w+)*))",
            re.IGNORECASE,
        )
        for match in column_then_list.finditer(code):
            for param in PARAM_PATTERN.findall(match.group("range") or match.group("list")):
                add_mapping(match.group("table"), match.group("column"), param)

        return mappings

    async def _resolve_from_stats(
        self,
        database_name: str,
        table_hint: str | None,
        column_name: str,
        tables: Sequence[_TableReference] = (),
    ) -> dict[str, Any] | None:
        """18.2: Read the column's type, and a value from statistics that lead with it.

        ``tables`` are the statement's base tables. A ``hint.column`` reference
        resolves through their aliases, so a column name shared by two tables
        is never read from the wrong one. Returns None when the catalog read
        fails; otherwise ``unbound_reason`` says what is missing.
        """
        candidates = _candidate_tables(table_hint, tables)
        if not candidates:
            return {"unbound_reason": "no_table_reference", "column_name": column_name}
        table_names = sorted({reference.table for reference in candidates}, key=str.casefold)
        table_names = table_names[:_MAX_CANDIDATE_TABLES]
        type_query = _COLUMN_TYPE_SQL.format(tables=", ".join("?" for _ in table_names))

        try:
            type_rows = await self.executor.fetch_all(
                database_name, type_query, params=[column_name, *table_names],
            )
        except Exception as exc:
            # WARNING is below the ERROR-level swallow handler: record it here.
            note_exception(exc, "param_binding.column_type")
            logger.warning(
                "Failed to resolve column type for parameter '%s'",
                column_name,
                exc_info=True,
            )
            return None

        written_schemas = {
            reference.table.casefold(): reference.schema.casefold()
            for reference in candidates
            if reference.schema
        }
        type_rows = [
            row
            for row in type_rows
            if written_schemas.get(str(row.get("table_name") or "").casefold())
            in (None, str(row.get("schema_name") or "").casefold())
        ]
        if not type_rows:
            return {"unbound_reason": "column_not_found", "column_name": column_name}
        owners = {(row.get("schema_name"), row.get("table_name")) for row in type_rows}
        if len(owners) > 1:
            return {"unbound_reason": "ambiguous_column", "column_name": column_name}

        # Unfiltered statistics first: a filtered histogram covers a subset.
        leading = sorted(
            (row for row in type_rows if row.get("stats_id") is not None),
            key=lambda row: (bool(row.get("has_filter")), int(row["stats_id"])),
        )
        row = leading[0] if leading else type_rows[0]
        data_type = self._format_data_type(row)
        stats_id = row.get("stats_id")
        table_name = row.get("table_name")
        schema_name = row.get("schema_name", "dbo")
        # "type_fallback" means: the type is known and no value was read.
        info: dict[str, Any] = {
            "data_type": data_type,
            "raw_value": None,
            "source": "type_fallback",
            "table_name": table_name,
            "schema_name": schema_name,
            "column_name": column_name,
            "stats_id": stats_id,
        }
        if data_type is None:
            return {**info, "unbound_reason": "unsupported_type"}
        if stats_id is None or table_name is None:
            return {**info, "unbound_reason": "no_leading_statistics"}

        try:
            hist_rows = await self.executor.fetch_all(
                database_name,
                _HISTOGRAM_VALUE_SQL,
                params=[schema_name, table_name, stats_id],
            )
        except Exception as exc:
            note_exception(exc, "param_binding.histogram")
            logger.warning(
                "Failed to query histogram for %s.%s (stats_id=%s); the parameter has no value",
                schema_name,
                table_name,
                stats_id,
                exc_info=True,
            )
            return {**info, "unbound_reason": "histogram_unreadable"}

        raw_value = hist_rows[0].get("range_high_key") if hist_rows else None
        if raw_value is None:
            return {**info, "unbound_reason": "empty_histogram"}
        return {
            **info,
            "raw_value": raw_value,
            "source": "histogram",
            "equal_rows": hist_rows[0].get("equal_rows"),
        }

    def _format_data_type(self, row: dict[str, Any]) -> str | None:
        """Format a column's type as a parameter type; None when it cannot be one."""
        return column_parameter_type(row)

    def _format_literal(self, data_type: str, value: Any) -> str:
        """Format a raw value as a T-SQL literal."""
        return format_sql_literal(data_type, value)
