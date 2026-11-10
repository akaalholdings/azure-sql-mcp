"""Parameter binding must not invent types or values (findings 42, 46, 85, 86).

A benchmark or an index recommendation is only as good as the parameter it was
compiled with. A value read from another column's histogram, an nvarchar(256)
guess against a varchar column, or the literal 'test' gives a plan production
never runs, and a measured win on that plan is not a win.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from azure_sql_mcp.param_binding import ParameterBindingError
from azure_sql_mcp.param_binding import ParameterBindingService
from azure_sql_mcp.param_binding import SqlParameterType
from azure_sql_mcp.param_binding import TypedParameter
from azure_sql_mcp.param_binding import TypedParameterBucket
from azure_sql_mcp.param_binding import detect_parameters
from azure_sql_mcp.safe_sql import SafeSqlValidator


class CatalogExecutor:
    """A small catalog: columns with a type and the statistics that LEAD with them.

    Like the engine, it returns the histogram of whichever statistics object is
    asked for, even when the column is not its leading column. A type query
    that does not ask for the leading column gets every statistics object that
    merely contains the column, lowest stats_id first.
    """

    def __init__(
        self,
        columns: dict[tuple[str, str, str], dict[str, Any]],
        histograms: dict[tuple[str, str, int], list[dict[str, Any]]] | None = None,
    ) -> None:
        self.columns = columns
        self.histograms = histograms or {}
        self.calls: list[tuple[str, list[Any]]] = []

    async def fetch_all(
        self,
        database_name: str,
        query: str,
        params: list[Any] | tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        arguments = list(params or [])
        self.calls.append((query, arguments))
        if "dm_db_stats_histogram" in query:
            schema_name, table_name, stats_id = arguments
            return self.histograms.get((schema_name, table_name, int(stats_id)), [])
        column_name, *table_names = arguments
        wanted = {name.casefold() for name in table_names}
        leading_only = "sc.stats_column_id = 1" in query
        rows: list[dict[str, Any]] = []
        for (schema_name, table_name, name), column in self.columns.items():
            if name.casefold() != column_name.casefold():
                continue
            if wanted and table_name.casefold() not in wanted:
                continue
            stats = column.get("leading_stats", [])
            if not leading_only:
                stats = sorted([*stats, *column.get("non_leading_stats", [])])
            for stats_id in stats or [None]:
                rows.append(
                    {
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "data_type": column["data_type"],
                        "max_length": column.get("max_length", 4),
                        "precision": column.get("precision", 10),
                        "scale": column.get("scale", 0),
                        "stats_id": stats_id,
                        "has_filter": False,
                    }
                )
        return rows

    @property
    def histogram_calls(self) -> list[list[Any]]:
        return [params for query, params in self.calls if "dm_db_stats_histogram" in query]


ORDERS_SQL = "SELECT o.OrderId FROM dbo.Orders AS o WHERE o.CustomerId = @CustomerId"


# --- finding 42: only statistics that lead with the column -------------------


@pytest.mark.asyncio
async def test_a_value_never_comes_from_statistics_that_lead_with_another_column() -> None:
    # CustomerId is the second key of IX_Orders(TenantId, CustomerId), stats 1.
    # Its histogram holds TenantId values; binding 7 as @CustomerId benchmarks
    # a customer that does not exist.
    executor = CatalogExecutor(
        {("dbo", "Orders", "CustomerId"): {"data_type": "int", "non_leading_stats": [1]}},
        {("dbo", "Orders", 1): [{"range_high_key": "7", "equal_rows": 90_000}]},
    )
    service = ParameterBindingService(executor)  # type: ignore[arg-type]

    result = await service.resolve_parameter_bucket("appdb", ORDERS_SQL)

    assert executor.histogram_calls == []
    assert result.bucket.parameters == ()
    assert [item.to_dict() for item in result.unbound] == [
        {
            "name": "@CustomerId",
            "reason_code": "no_leading_statistics",
            "reason": (
                "No statistics object has this column as its leading column, so "
                "there is no representative value."
            ),
            "data_type": "int",
        }
    ]


@pytest.mark.asyncio
async def test_the_statistics_that_lead_with_the_column_supply_the_value() -> None:
    executor = CatalogExecutor(
        {
            ("dbo", "Orders", "CustomerId"): {
                "data_type": "int",
                "non_leading_stats": [1],
                "leading_stats": [3],
            }
        },
        {
            ("dbo", "Orders", 1): [{"range_high_key": "7", "equal_rows": 90_000}],
            ("dbo", "Orders", 3): [{"range_high_key": "4242", "equal_rows": 500}],
        },
    )
    service = ParameterBindingService(executor)  # type: ignore[arg-type]

    bucket = await service.build_parameter_bucket("appdb", ORDERS_SQL)

    parameter = bucket.parameters[0]
    assert (parameter.value, parameter.provenance) == (4242, "histogram")
    assert parameter.provenance_detail["stats_id"] == 3
    # Schema and table go as two arguments, so a name with a dot or a bracket
    # still resolves through QUOTENAME.
    assert executor.histogram_calls == [["dbo", "Orders", 3]]


@pytest.mark.asyncio
async def test_an_alias_resolves_to_its_own_table() -> None:
    # CustomerId exists on both tables with different types. The alias decides.
    executor = CatalogExecutor(
        {
            ("Sales", "Orders", "CustomerId"): {"data_type": "bigint", "leading_stats": [2]},
            ("Sales", "Customers", "CustomerId"): {"data_type": "int", "leading_stats": [1]},
        },
        {
            ("Sales", "Orders", 2): [{"range_high_key": "11", "equal_rows": 9}],
            ("Sales", "Customers", 1): [{"range_high_key": "22", "equal_rows": 1}],
        },
    )
    service = ParameterBindingService(executor)  # type: ignore[arg-type]

    bucket = await service.build_parameter_bucket(
        "appdb",
        "SELECT c.Name FROM Sales.Customers AS c "
        "INNER JOIN [Sales].[Orders] o ON o.CustomerId = c.CustomerId "
        "WHERE o.CustomerId = @CustomerId",
    )

    parameter = bucket.parameters[0]
    assert parameter.sql_type.sql_declaration == "bigint"
    assert parameter.value == 11
    assert parameter.provenance_detail["table_name"] == "Orders"


@pytest.mark.asyncio
async def test_an_unqualified_column_on_two_tables_is_reported_not_guessed() -> None:
    executor = CatalogExecutor(
        {
            ("dbo", "Orders", "Status"): {"data_type": "varchar", "max_length": 20},
            ("dbo", "Shipments", "Status"): {"data_type": "int"},
        }
    )
    service = ParameterBindingService(executor)  # type: ignore[arg-type]

    result = await service.resolve_parameter_bucket(
        "appdb",
        "SELECT 1 FROM dbo.Orders AS o JOIN dbo.Shipments AS s ON s.OrderId = o.OrderId "
        "WHERE Status = @Status",
    )

    assert [(item.name, item.reason_code) for item in result.unbound] == [
        ("@Status", "ambiguous_column")
    ]


# --- backlog: no nvarchar(256) and no 'test' ----------------------------------


@pytest.mark.asyncio
async def test_a_parameter_with_no_column_is_unbound_with_the_reason() -> None:
    service = ParameterBindingService(CatalogExecutor({}))  # type: ignore[arg-type]
    sql = "SELECT TOP (@n) o.OrderId FROM dbo.Orders AS o WHERE ABS(@x) = 1"

    with pytest.raises(ParameterBindingError) as raised:
        await service.build_parameter_bucket("appdb", sql)

    assert [(item.name, item.reason_code) for item in raised.value.unbound] == [
        ("@n", "no_column_mapping"),
        ("@x", "no_column_mapping"),
    ]
    message = str(raised.value)
    assert "@n (no_column_mapping)" in message and "parameter_types" in message
    assert "nvarchar" not in message and "test" not in message

    compatibility = await service.bind_parameters("appdb", sql)
    assert compatibility["bound_sql"] is None
    assert compatibility["execution_contract"] is None
    assert [item["name"] for item in compatibility["unbound_parameters"]] == ["@n", "@x"]


@pytest.mark.asyncio
async def test_an_explicit_value_without_a_type_is_not_declared_nvarchar() -> None:
    # A string value bound as nvarchar against a varchar column converts the
    # column and scans: the type must be known, not guessed.
    service = ParameterBindingService(CatalogExecutor({}))  # type: ignore[arg-type]

    result = await service.resolve_parameter_bucket(
        "appdb",
        "SELECT 1 WHERE @Flag = 1",
        parameter_values={"Flag": "abc"},
    )

    assert [item.to_dict() for item in result.unbound] == [
        {
            "name": "@Flag",
            "reason_code": "type_unknown",
            "reason": "A value was supplied but the SQL type is unknown.",
            "data_type": None,
        }
    ]


@pytest.mark.asyncio
async def test_a_column_type_that_cannot_be_a_parameter_is_unbound() -> None:
    executor = CatalogExecutor({("dbo", "Places", "Shape"): {"data_type": "geography"}})
    service = ParameterBindingService(executor)  # type: ignore[arg-type]

    result = await service.resolve_parameter_bucket(
        "appdb", "SELECT 1 FROM dbo.Places WHERE Shape = @Shape"
    )

    assert [(item.name, item.reason_code) for item in result.unbound] == [
        ("@Shape", "unsupported_type")
    ]


@pytest.mark.asyncio
async def test_range_and_in_list_parameters_take_the_column_type() -> None:
    # Before: only the first IN item was mapped; the rest were nvarchar(256).
    executor = CatalogExecutor(
        {
            ("dbo", "Orders", "OrderDate"): {"data_type": "date", "leading_stats": [4]},
            ("dbo", "Orders", "Status"): {
                "data_type": "varchar",
                "max_length": 20,
                "leading_stats": [5],
            },
        },
        {
            ("dbo", "Orders", 4): [{"range_high_key": "2024-03-01", "equal_rows": 10}],
            ("dbo", "Orders", 5): [{"range_high_key": "Open", "equal_rows": 10}],
        },
    )
    service = ParameterBindingService(executor)  # type: ignore[arg-type]

    bucket = await service.build_parameter_bucket(
        "appdb",
        "SELECT 1 FROM dbo.Orders AS o WHERE o.OrderDate BETWEEN @From AND @To "
        "AND o.Status IN (@S1, @S2, @S3)",
    )

    assert [
        (parameter.name, parameter.sql_type.sql_declaration) for parameter in bucket.parameters
    ] == [
        ("@From", "date"),
        ("@To", "date"),
        ("@S1", "varchar(20)"),
        ("@S2", "varchar(20)"),
        ("@S3", "varchar(20)"),
    ]
    # One catalog read and one histogram read per column, not per parameter.
    assert len(executor.calls) == 4


# --- finding 46: Query Store text keeps its declared types --------------------

QUERY_STORE_TEXT = (
    "(@P1 varchar(20),@P2 int)SELECT o.OrderId FROM dbo.Orders AS o "
    "WHERE o.Status=@P1 AND o.CustomerID=@P2"
)


@pytest.mark.asyncio
async def test_query_store_text_compiles_with_the_types_it_was_stored_with() -> None:
    executor = CatalogExecutor({})
    service = ParameterBindingService(executor)  # type: ignore[arg-type]

    prepared = await service.prepare_query_store_text("appdb", QUERY_STORE_TEXT)

    assert prepared == (
        "DECLARE @P1 varchar(20), @P2 int;\n"
        "SELECT o.OrderId FROM dbo.Orders AS o WHERE o.Status=@P1 AND o.CustomerID=@P2"
    )
    # Every type was stored, so no catalog read is spent on this statement.
    assert executor.calls == []
    # The index tools pass this text through the read-only validator.
    assert SafeSqlValidator().validate_read_only(prepared).normalized_sql == prepared


@pytest.mark.asyncio
async def test_query_store_binding_keeps_declared_types_and_sniffs_real_values() -> None:
    # The application declared nvarchar(4000) against a varchar(20) column.
    # That mismatch is the production plan problem; the column type hides it.
    executor = CatalogExecutor(
        {
            ("dbo", "Orders", "Status"): {
                "data_type": "varchar",
                "max_length": 20,
                "leading_stats": [5],
            },
            ("dbo", "Orders", "CustomerID"): {"data_type": "int", "leading_stats": [3]},
        },
        {
            ("dbo", "Orders", 5): [{"range_high_key": "Open", "equal_rows": 800}],
            ("dbo", "Orders", 3): [{"range_high_key": "42", "equal_rows": 500}],
        },
    )
    service = ParameterBindingService(executor)  # type: ignore[arg-type]

    binding = await service.bind_query_store_text(
        "appdb",
        "(@P1 nvarchar(4000),@P2 int)SELECT o.OrderId FROM dbo.Orders AS o "
        "WHERE o.Status=@P1 AND o.CustomerID=@P2",
    )

    assert binding.declared_types == {"@P1": "nvarchar(4000)", "@P2": "int"}
    assert binding.contract is not None
    assert binding.contract.parameter_definition == "@P1 nvarchar(4000), @P2 int"
    # Executed through sp_executesql with typed parameters: values are sniffed.
    assert binding.contract.sp_executesql_values == (
        binding.statement,
        "@P1 nvarchar(4000), @P2 int",
        "Open",
        42,
    )
    assert binding.showplan_sql == (
        "DECLARE @P1 nvarchar(4000), @P2 int;\n"
        f"{binding.statement}\n"
        "OPTION (OPTIMIZE FOR (@P1 = N'Open', @P2 = 42))"
    )


@pytest.mark.asyncio
async def test_a_typed_parameter_with_no_value_compiles_as_unknown_and_never_runs() -> None:
    service = ParameterBindingService(CatalogExecutor({}))  # type: ignore[arg-type]

    binding = await service.bind_query_store_text("appdb", QUERY_STORE_TEXT)

    assert binding.contract is not None
    assert [parameter.value_known for parameter in binding.contract.parameters] == [False, False]
    assert binding.showplan_sql == f"DECLARE @P1 varchar(20), @P2 int;\n{binding.statement}"
    assert binding.contract.estimated_showplan_basis["unknown_parameters"] == ["@P1", "@P2"]
    with pytest.raises(ValueError, match="no value"):
        _ = binding.contract.sp_executesql_values
    with pytest.raises(ValueError, match="no value"):
        _ = binding.contract.driver_values
    assert binding.contract.to_dict()["sp_executesql_values"] is None


@pytest.mark.asyncio
async def test_auto_parameterized_text_binds_its_numbered_parameters() -> None:
    service = ParameterBindingService(CatalogExecutor({}))  # type: ignore[arg-type]

    prepared = await service.prepare_query_store_text(
        "appdb", "(@1 int,@2 varchar(8000))SELECT 1 FROM dbo.Orders WHERE Id = @1 AND Code = @2"
    )

    assert prepared.startswith("DECLARE @1 int, @2 varchar(8000);\nSELECT 1 FROM dbo.Orders")


@pytest.mark.asyncio
async def test_query_store_text_with_an_undeclared_unmapped_parameter_is_refused() -> None:
    # A statement from a procedure has no "(@p type)" prefix. A parameter that
    # is compared with no column has no type; compiling it as nvarchar(256)
    # would advise an index for a plan that never ran.
    service = ParameterBindingService(CatalogExecutor({}))  # type: ignore[arg-type]

    with pytest.raises(ParameterBindingError, match=r"@Cutoff \(no_column_mapping\)"):
        await service.prepare_query_store_text(
            "appdb", "SELECT 1 FROM dbo.Orders WHERE DATEADD(day, 1, OrderDate) > @Cutoff"
        )


@pytest.mark.asyncio
async def test_a_statement_with_its_own_optimize_for_hint_keeps_it() -> None:
    service = ParameterBindingService(CatalogExecutor({}))  # type: ignore[arg-type]

    binding = await service.bind_query_store_text(
        "appdb",
        "(@P1 int)SELECT 1 FROM dbo.Orders WHERE Id = @P1 OPTION (OPTIMIZE FOR (@P1 = 5))",
    )

    assert binding.showplan_sql == (
        "DECLARE @P1 int;\n"
        "SELECT 1 FROM dbo.Orders WHERE Id = @P1 OPTION (OPTIMIZE FOR (@P1 = 5))"
    )


# --- finding 85: declarations after a subquery initializer --------------------


def test_variables_declared_after_a_subquery_initializer_are_not_parameters() -> None:
    sql = (
        "DECLARE @a int = (SELECT MAX(OrderID) FROM dbo.Orders), @b int = 5; "
        "SELECT 1 FROM dbo.Orders WHERE OrderID BETWEEN @b AND @a"
    )

    assert detect_parameters(sql) == []


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            "DECLARE @a int = (SELECT COUNT(*) FROM t WHERE x IN (SELECT y FROM u)), "
            "@b varchar(10) = 'x' SELECT @a, @b, @c",
            ["c"],
        ),
        ("DECLARE @t TABLE (id int, v varchar(10)); SELECT 1 FROM @t WHERE id = @x", ["x"]),
        ("DECLARE @a int\nDECLARE @b int = @Outer\nSELECT @a + @b", ["Outer"]),
        ("DECLARE @a AS decimal(18, 2) = 1.5, @b AS int; SELECT @a, @b", []),
        ("DECLARE @set int = 1; SELECT @set, @p", ["p"]),
    ],
)
def test_declared_variable_scan_follows_parentheses(sql: str, expected: list[str]) -> None:
    assert detect_parameters(sql) == expected


# --- finding 86: seven fractional digits survive ------------------------------


@pytest.mark.parametrize(
    ("data_type", "key"),
    [
        ("datetime2(7)", "2024-01-01 10:00:00.1234567"),
        ("time(7)", "10:00:00.1234567"),
        ("datetimeoffset(7)", "2024-01-01 10:00:00.1234567 +01:00"),
    ],
)
def test_a_seven_digit_histogram_key_is_bound_unchanged(data_type: str, key: str) -> None:
    # The most frequent key .1234567 bound as .123456 matches no row: the plan
    # is then captured on an empty result.
    sql_type = SqlParameterType.from_sql(data_type)

    value = ParameterBindingService._coerce_driver_value(sql_type, key)

    assert value == key
    contract = TypedParameterBucket(
        bucket_id="hot-key",
        parameters=(TypedParameter("@p", sql_type, value, "histogram"),),
    ).for_sql("SELECT 1 FROM dbo.Events WHERE CreatedAt = @p")
    assert contract.sp_executesql_values[-1] == key
    assert f"OPTIMIZE FOR (@p = '{key}')" in contract.estimated_showplan_sql


def test_a_key_that_fits_microseconds_still_binds_as_a_datetime() -> None:
    value = ParameterBindingService._coerce_driver_value(
        SqlParameterType.from_sql("datetime2(7)"), "2024-01-01 10:00:00.1234560"
    )

    assert value == datetime(2024, 1, 1, 10, 0, 0, 123456)
