from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from azure_sql_mcp.param_binding import (
    ParameterBindingService,
    ParameterExecutionContract,
    SqlParameterType,
    TypedParameter,
    TypedParameterBucket,
    detect_parameters,
    get_type_fallback,
)


class FakeExecutor:
    def __init__(self, responses: list[tuple[str, list[dict[str, Any]]]] | None = None):
        self.responses = responses or []
        self.calls: list[tuple[str, str, Any]] = []

    async def fetch_all(
        self,
        database_name: str,
        query: str,
        params: list[Any] | tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((database_name, query, params))
        for needle, rows in self.responses:
            if needle in query:
                return rows
        return []


# --- 18.1: Parameter detection tests ---

def test_detect_parameters_finds_at_params() -> None:
    sql = "SELECT * FROM Orders WHERE CustomerId = @CustomerId AND Status = @Status"
    params = detect_parameters(sql)
    assert params == ["CustomerId", "Status"]


def test_detect_parameters_deduplicates() -> None:
    sql = "SELECT * FROM Orders WHERE Id = @Id OR ParentId = @Id"
    params = detect_parameters(sql)
    assert params == ["Id"]


def test_detect_parameters_skips_system_variables() -> None:
    sql = "SELECT @@ROWCOUNT, @@IDENTITY, @MyParam FROM T WHERE Id = @Id"
    params = detect_parameters(sql)
    assert "ROWCOUNT" not in params
    assert "IDENTITY" not in params
    assert "MyParam" in params
    assert "Id" in params


def test_detect_parameters_empty_for_no_params() -> None:
    sql = "SELECT * FROM Orders"
    params = detect_parameters(sql)
    assert params == []


def test_detect_parameters_ignores_literals_comments_and_existing_declarations() -> None:
    sql = """
    DECLARE @AlreadyDeclared int;
    SELECT '@fake', [o].[CustomerId] = @CustomerId -- @comment_param
    FROM dbo.Orders AS o
    WHERE @Status = o.Status AND @AlreadyDeclared = 1;
    """
    assert detect_parameters(sql) == ["CustomerId", "Status"]


def test_detect_parameters_ignores_every_variable_in_multi_declare() -> None:
    sql = """
    DECLARE @First int, @Second nvarchar(20);
    SELECT @First, @Second, @External;
    """
    assert detect_parameters(sql) == ["External"]


def test_detect_parameters_keeps_external_initializer_references() -> None:
    sql = """
    DECLARE @Local int = COALESCE(@External, 0), @Other int = 1;
    SELECT @Local + @Other;
    """
    assert detect_parameters(sql) == ["External"]


def test_detect_parameters_treats_names_case_insensitively() -> None:
    sql = "SELECT 1 FROM dbo.Orders WHERE Id = @CustomerId OR Id = @customerid"
    assert detect_parameters(sql) == ["CustomerId"]


def test_detect_parameters_ignores_escaped_bracketed_identifiers() -> None:
    sql = (
        "SELECT [not_a_param]]@Ignored] AS [@also_ignored] "
        "FROM dbo.Orders WHERE Id = @Id"
    )

    assert detect_parameters(sql) == ["Id"]


def test_detect_parameters_treats_comment_markers_inside_identifiers_as_data() -> None:
    sql = (
        "SELECT [not--a-comment], [not/*a-comment*/either] "
        "FROM dbo.Orders WHERE Id = @Id"
    )

    assert detect_parameters(sql) == ["Id"]


def test_detect_parameters_ignores_delimiters_inside_literals_and_identifiers() -> None:
    sql = (
        "SELECT N'not [an identifier] -- or @Ignored', "
        '"also @Ignored", [still]]@Ignored] '
        "FROM dbo.Orders WHERE Id = @Id"
    )

    assert detect_parameters(sql) == ["Id"]


def test_parameter_replacement_ignores_nested_block_comments() -> None:
    sql = (
        "SELECT @p /* outer @p /* nested @p */ outer @p */ "
        "FROM dbo.Orders"
    )
    contract = ParameterExecutionContract(
        sql_text=sql,
        bucket_id="nested-comments",
        parameters=(
            TypedParameter(
                name="@p",
                sql_type=SqlParameterType("int"),
                value=42,
                provenance="test",
            ),
        ),
        provenance="test",
    )

    assert contract.driver_sql == (
        "SELECT ? /* outer @p /* nested @p */ outer @p */ FROM dbo.Orders"
    )
    assert contract.driver_values == (42,)


def test_column_mapping_supports_brackets_and_reverse_comparison() -> None:
    service = ParameterBindingService(executor=FakeExecutor())
    mappings = service._extract_column_mappings(
        "SELECT 1 FROM [dbo].[Orders] AS o WHERE [o].[CustomerId] = @CustomerId AND @Status = o.Status",
        ["CustomerId", "Status"],
    )
    assert mappings == {
        "CustomerId": ("o", "CustomerId"),
        "Status": ("o", "Status"),
    }


# --- 18.3: Type fallback tests ---

def test_type_fallback_int() -> None:
    assert get_type_fallback("int") == "1"
    assert get_type_fallback("bigint") == "1"


def test_type_fallback_string_types() -> None:
    assert get_type_fallback("varchar") == "'test'"
    assert get_type_fallback("nvarchar") == "N'test'"
    assert get_type_fallback("varchar(50)") == "'test'"


def test_type_fallback_date_types() -> None:
    assert get_type_fallback("datetime") == "GETDATE()"
    assert get_type_fallback("date") == "CAST(GETDATE() AS DATE)"
    assert get_type_fallback("datetime2(7)") == "SYSDATETIME()"


def test_type_fallback_unknown_returns_null() -> None:
    assert get_type_fallback("geometry") == "NULL"


def test_format_data_type_rejects_hostile_type_names() -> None:
    """Catalog type names are embedded in DECLARE statements; a hostile UDT name
    must fall back to a safe type instead of being interpolated verbatim."""
    service = ParameterBindingService(executor=FakeExecutor())
    row = {"data_type": "int; DROP TABLE dbo.Users --", "max_length": 4}
    assert service._format_data_type(row) == "nvarchar(256)"


def test_format_data_type_allows_plain_types() -> None:
    service = ParameterBindingService(executor=FakeExecutor())
    assert service._format_data_type({"data_type": "int"}) == "int"
    assert service._format_data_type({"data_type": "uniqueidentifier"}) == "uniqueidentifier"


def test_binary_literal_rejects_non_hex_input() -> None:
    service = ParameterBindingService(executor=FakeExecutor())
    with pytest.raises(ValueError, match="not valid"):
        service._format_literal("varbinary(16)", "00; DROP TABLE dbo.Users")


def test_bit_literal_rejects_values_other_than_zero_or_one() -> None:
    service = ParameterBindingService(executor=FakeExecutor())
    with pytest.raises(ValueError, match="not valid"):
        service._format_literal("bit", 2)


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (True, True),
        (False, False),
        ("TRUE", True),
        ("false", False),
        ("1", True),
        ("0", False),
    ),
)
def test_bit_coercion_accepts_recognized_representations(
    value: Any,
    expected: bool,
) -> None:
    service = ParameterBindingService(executor=FakeExecutor())

    assert service._coerce_driver_value(SqlParameterType("bit"), value) is expected


@pytest.mark.parametrize("value", ("unknown", "yes", "2", 2))
def test_bit_coercion_rejects_unknown_representations(value: Any) -> None:
    service = ParameterBindingService(executor=FakeExecutor())

    with pytest.raises(ValueError, match="not valid"):
        service._coerce_driver_value(SqlParameterType("bit"), value)


# --- 18.2 + 18.4: Binding service tests ---

@pytest.mark.asyncio
async def test_bind_parameters_no_params_returns_original() -> None:
    executor = FakeExecutor()
    service = ParameterBindingService(executor)

    result = await service.bind_parameters("testdb", "SELECT * FROM Orders")

    assert result["bound_sql"] == "SELECT * FROM Orders"
    assert result["parameters"] == []


@pytest.mark.asyncio
async def test_bind_parameters_uses_explicit_values() -> None:
    service = ParameterBindingService(FakeExecutor())
    result = await service.bind_parameters(
        "testdb",
        "SELECT * FROM Orders WHERE Name = @Name AND IsActive = @IsActive",
        parameter_values={"Name": "O'Reilly", "@IsActive": True},
    )

    assert [p["source"] for p in result["parameters"]] == [
        "explicit_value",
        "explicit_value",
    ]
    assert "N'O''Reilly'" in result["bound_sql"]
    assert "SET @IsActive = 1;" in result["bound_sql"]


@pytest.mark.asyncio
async def test_bind_parameters_matches_explicit_names_case_insensitively() -> None:
    service = ParameterBindingService(FakeExecutor())
    result = await service.bind_parameters(
        "testdb",
        "SELECT * FROM Orders WHERE Name = @Name",
        parameter_values={"@name": "Alice"},
    )

    assert result["parameters"][0]["source"] == "explicit_value"
    assert "SET @Name = N'Alice';" in result["bound_sql"]


@pytest.mark.asyncio
async def test_bind_parameters_rejects_unknown_or_duplicate_explicit_names() -> None:
    service = ParameterBindingService(FakeExecutor())

    with pytest.raises(ValueError, match="unknown parameter"):
        await service.bind_parameters(
            "testdb",
            "SELECT * FROM Orders WHERE Name = @Name",
            parameter_values={"Nmae": "Alice"},
        )

    with pytest.raises(ValueError, match="duplicate explicit parameter"):
        await service.bind_parameters(
            "testdb",
            "SELECT * FROM Orders WHERE Name = @Name",
            parameter_values={"Name": "Alice", "@name": "Bob"},
        )


@pytest.mark.asyncio
async def test_bind_parameters_with_histogram_value() -> None:
    executor = FakeExecutor(responses=[
        (
            "sys.stats_columns",
            [
                {
                    "table_name": "Orders",
                    "schema_name": "dbo",
                    "data_type": "int",
                    "max_length": 4,
                    "precision": 10,
                    "scale": 0,
                    "stats_id": 1,
                }
            ],
        ),
        (
            "sys.dm_db_stats_histogram",
            [
                {
                    "range_high_key": 42,
                    "equal_rows": 500,
                }
            ],
        ),
    ])
    service = ParameterBindingService(executor)

    result = await service.bind_parameters(
        "testdb",
        "SELECT * FROM Orders WHERE CustomerId = @CustomerId",
    )

    assert len(result["parameters"]) == 1
    param = result["parameters"][0]
    assert param["name"] == "@CustomerId"
    assert param["source"] == "histogram"
    assert param["value"] == "42"
    assert "DECLARE @CustomerId int;" in result["bound_sql"]
    assert "SET @CustomerId = 42;" in result["bound_sql"]

    # range_high_key is sql_variant, which the driver cannot fetch; the
    # histogram query must CONVERT it server-side or binding never works live.
    histogram_queries = [q for _, q, _ in executor.calls if "dm_db_stats_histogram" in q]
    assert histogram_queries
    assert "CONVERT(NVARCHAR(4000), range_high_key, 121)" in histogram_queries[0]


@pytest.mark.asyncio
async def test_bind_parameters_falls_back_to_type_default() -> None:
    executor = FakeExecutor(responses=[
        (
            "sys.stats_columns",
            [
                {
                    "table_name": "Orders",
                    "schema_name": "dbo",
                    "data_type": "nvarchar",
                    "max_length": 100,
                    "precision": 0,
                    "scale": 0,
                    "stats_id": 1,
                }
            ],
        ),
        # No histogram rows — force fallback
        ("sys.dm_db_stats_histogram", []),
    ])
    service = ParameterBindingService(executor)

    result = await service.bind_parameters(
        "testdb",
        "SELECT * FROM Customers WHERE Name = @Name",
    )

    assert len(result["parameters"]) == 1
    param = result["parameters"][0]
    assert param["name"] == "@Name"
    assert param["source"] == "type_fallback"
    assert "DECLARE @Name nvarchar(50);" in result["bound_sql"]


@pytest.mark.asyncio
async def test_bind_parameters_no_column_match_uses_defaults() -> None:
    executor = FakeExecutor()
    service = ParameterBindingService(executor)

    # No column = @param pattern — just a bare @param in VALUES
    result = await service.bind_parameters(
        "testdb",
        "SELECT @SomeValue AS Val",
    )

    assert len(result["parameters"]) == 1
    param = result["parameters"][0]
    assert param["source"] == "type_fallback"
    assert param["data_type"] == "nvarchar(256)"


@pytest.mark.asyncio
async def test_bind_parameters_multiple_params() -> None:
    executor = FakeExecutor(responses=[
        (
            "sys.stats_columns",
            [
                {
                    "table_name": "Events",
                    "schema_name": "dbo",
                    "data_type": "datetime2",
                    "max_length": 8,
                    "precision": 27,
                    "scale": 7,
                    "stats_id": 2,
                }
            ],
        ),
        ("sys.dm_db_stats_histogram", []),
    ])
    service = ParameterBindingService(executor)

    result = await service.bind_parameters(
        "testdb",
        "SELECT * FROM Events WHERE EventDate > @StartDate AND Category = @Cat",
    )

    assert len(result["parameters"]) == 2
    names = [p["name"] for p in result["parameters"]]
    assert "@StartDate" in names
    assert "@Cat" in names


@pytest.mark.asyncio
async def test_alias_table_hint_retries_without_hint() -> None:
    """`o.CustomerId = @p` yields table hint 'o' (an alias, not a table). The
    resolver must retry across all tables instead of silently falling back to
    an nvarchar guess that breaks execution with a conversion error."""

    class AliasAwareExecutor:
        def __init__(self) -> None:
            self.calls: list[list[Any]] = []

        async def fetch_all(self, database_name, query, params=None):
            self.calls.append(list(params or []))
            if "AND t.name = ?" in query:
                return []  # alias never matches a real table
            if "sys.dm_db_stats_histogram" in query:
                return []
            return [{
                "table_name": "Orders",
                "schema_name": "Sales",
                "data_type": "int",
                "max_length": 4,
                "precision": 10,
                "scale": 0,
                "stats_id": 1,
            }]

    executor = AliasAwareExecutor()
    service = ParameterBindingService(executor)

    result = await service.bind_parameters(
        "appdb",
        "SELECT o.OrderID FROM Sales.Orders AS o WHERE o.CustomerID = @CustomerId",
    )

    parameter = result["parameters"][0]
    assert parameter["data_type"] == "int"
    assert parameter["value"] == "1"  # int type fallback, not N'test'
    assert ["CustomerID", "o"] in executor.calls  # hinted attempt happened
    assert ["CustomerID"] in executor.calls  # unhinted retry happened


def test_typed_bucket_binds_baseline_and_candidate_without_local_variables() -> None:
    bucket = TypedParameterBucket(
        bucket_id="common-rare-null-boundary",
        label="rare",
        provenance="query_store_compiled_parameter",
        parameters=(
            TypedParameter(
                name="@CustomerId",
                sql_type=SqlParameterType("int"),
                value=42,
                provenance="query_store_compiled_parameter",
            ),
            TypedParameter(
                name="@Name",
                sql_type=SqlParameterType("nvarchar", length=50),
                value=None,
                provenance="explicit_null_bucket",
            ),
            TypedParameter(
                name="@Amount",
                sql_type=SqlParameterType("decimal", precision=12, scale=4),
                value="999999.9999",
                provenance="boundary_bucket",
            ),
        ),
    )

    service = ParameterBindingService(FakeExecutor())
    contracts = service.build_comparison_contracts(
        "SELECT 1 WHERE CustomerId = @CustomerId AND Name = @Name",
        "SELECT 1 WHERE CustomerId = @CustomerId AND Name = @Name",
        bucket,
    )

    baseline = contracts["baseline"]
    assert baseline.parameter_definition == "@CustomerId int, @Name nvarchar(50)"
    assert baseline.driver_sql.endswith("CustomerId = ? AND Name = ?")
    assert baseline.driver_values == (42, None)
    assert baseline.sp_executesql_values[1] == baseline.parameter_definition
    assert baseline.parameters[1].provenance == "explicit_null_bucket"
    assert contracts["candidate"].bucket_id == baseline.bucket_id
    assert "DECLARE" not in baseline.driver_sql


def test_sql_parameter_type_preserves_length_precision_and_scale() -> None:
    assert SqlParameterType.from_sql("nvarchar(400)").to_dict() == {
        "data_type": "nvarchar(400)",
        "base_type": "nvarchar",
        "length": 400,
        "precision": None,
        "scale": None,
    }
    decimal_type = SqlParameterType.from_sql("decimal(19,4)")
    assert decimal_type.sql_declaration == "decimal(19,4)"
    assert decimal_type.precision == 19
    assert decimal_type.scale == 4


@pytest.mark.parametrize(
    "data_type",
    (
        "madeup_type",
        "text",
        "int(4)",
        "nvarchar(4001)",
        "varchar(8001)",
        "char(max)",
        "datetime2(8)",
    ),
)
def test_sql_parameter_type_rejects_unsupported_or_invalid_declarations(
    data_type: str,
) -> None:
    with pytest.raises(ValueError):
        SqlParameterType.from_sql(data_type)


@pytest.mark.parametrize(
    "factory",
    (
        lambda: SqlParameterType("varchar", length=True),
        lambda: SqlParameterType("decimal", precision=1.5),
        lambda: SqlParameterType("datetime2", scale=1.5),
    ),
)
def test_sql_parameter_type_rejects_non_integer_metadata(factory: Any) -> None:
    with pytest.raises(ValueError):
        factory()


@pytest.mark.parametrize("value", (1, 1.0, "1", "1.0", Decimal("1.0")))
def test_integer_validation_accepts_integral_representations(value: Any) -> None:
    service = ParameterBindingService(FakeExecutor())

    service._validate_driver_value(SqlParameterType("int"), value)


@pytest.mark.parametrize("value", (1.5, "1.5", Decimal("1.5")))
def test_integer_validation_rejects_fractional_values(value: Any) -> None:
    service = ParameterBindingService(FakeExecutor())

    with pytest.raises(ValueError, match="not valid"):
        service._validate_driver_value(SqlParameterType("int"), value)


def test_typed_bucket_rejects_length_and_decimal_scale_overflows() -> None:
    service = ParameterBindingService(FakeExecutor())
    with pytest.raises(ValueError, match="length"):
        service._validate_driver_value(SqlParameterType("varchar", length=3), "abcd")
    with pytest.raises(ValueError, match="scale"):
        service._validate_driver_value(
            SqlParameterType("decimal", precision=6, scale=2),
            "1.234",
        )


def test_typed_bucket_preserves_datetime_scale_and_raw_driver_value() -> None:
    value = datetime(2026, 7, 24, 10, 30, 12, 345678)
    bucket = TypedParameterBucket(
        bucket_id="boundary-datetime",
        parameters=(
            TypedParameter(
                name="@StartDate",
                sql_type=SqlParameterType("datetime2", scale=7),
                value=value,
                provenance="boundary_bucket",
            ),
        ),
    )
    contract = bucket.for_sql("SELECT 1 WHERE @StartDate IS NOT NULL")

    assert contract.parameter_definition == "@StartDate datetime2(7)"
    assert contract.driver_values == (value,)
    assert contract.parameters[0].provenance == "boundary_bucket"


def test_sp_executesql_uses_all_positional_arguments_in_definition_order() -> None:
    bucket = TypedParameterBucket(
        bucket_id="typed",
        parameters=(
            TypedParameter(
                name="@CustomerId",
                sql_type=SqlParameterType.from_sql("bigint"),
                value=42,
                provenance="synthetic",
            ),
        ),
    )
    contract = bucket.for_sql(
        "SELECT 1 WHERE @CustomerId = @CustomerId"
    )

    assert contract.sp_executesql_sql == "EXEC sys.sp_executesql ?, ?, ?"
    assert contract.sp_executesql_values == (
        "SELECT 1 WHERE @CustomerId = @CustomerId",
        "@CustomerId bigint",
        42,
    )


def test_sp_executesql_supports_query_parameters_named_stmt_and_params() -> None:
    bucket = TypedParameterBucket(
        bucket_id="reserved-names",
        parameters=(
            TypedParameter(
                name="@stmt",
                sql_type=SqlParameterType.from_sql("int"),
                value=1,
                provenance="synthetic",
            ),
            TypedParameter(
                name="@params",
                sql_type=SqlParameterType.from_sql("int"),
                value=2,
                provenance="synthetic",
            ),
        ),
    )
    contract = bucket.for_sql("SELECT @stmt + @params")

    assert contract.sp_executesql_sql == "EXEC sys.sp_executesql ?, ?, ?, ?"
    assert contract.parameter_definition == "@stmt int, @params int"


def test_driver_binding_ignores_escaped_bracketed_identifier_parameters() -> None:
    bucket = TypedParameterBucket(
        bucket_id="escaped-identifier",
        parameters=(
            TypedParameter(
                name="@Id",
                sql_type=SqlParameterType("int"),
                value=7,
                provenance="synthetic",
            ),
        ),
    )

    contract = bucket.for_sql(
        "SELECT [not_a_param]]@Ignored] FROM dbo.Orders WHERE Id = @Id"
    )

    assert contract.driver_sql == (
        "SELECT [not_a_param]]@Ignored] FROM dbo.Orders WHERE Id = ?"
    )
    assert contract.driver_values == (7,)


def test_parameter_execution_contract_rejects_missing_or_extra_parameters() -> None:
    parameter = TypedParameter(
        name="@Unexpected",
        sql_type=SqlParameterType.from_sql("int"),
        value=1,
        provenance="synthetic",
    )

    with pytest.raises(ValueError, match="cover every query parameter exactly"):
        ParameterExecutionContract(
            sql_text="SELECT 1 WHERE @Expected = 1",
            bucket_id="typed",
            parameters=(parameter,),
            provenance="synthetic",
        )


@pytest.mark.asyncio
async def test_bind_parameters_exposes_typed_driver_contract_and_provenance() -> None:
    result = await ParameterBindingService(FakeExecutor()).bind_parameters(
        "testdb",
        "SELECT * FROM Orders WHERE CustomerId = @CustomerId",
        parameter_values={"CustomerId": None},
    )

    assert result["parameters"][0]["value"] == "NULL"
    assert result["parameters"][0]["raw_value"] is None
    contract = result["execution_contract"]
    assert contract["driver_sql"].endswith("CustomerId = ?")
    assert contract["driver_values"] == [None]
    assert contract["parameters"][0]["provenance"] == "explicit_value"
