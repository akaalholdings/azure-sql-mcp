from __future__ import annotations

from typing import Any

import pytest

from azure_sql_mcp.param_binding import (
    ParameterBindingService,
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


# --- 18.2 + 18.4: Binding service tests ---

@pytest.mark.asyncio
async def test_bind_parameters_no_params_returns_original() -> None:
    executor = FakeExecutor()
    service = ParameterBindingService(executor)

    result = await service.bind_parameters("testdb", "SELECT * FROM Orders")

    assert result["bound_sql"] == "SELECT * FROM Orders"
    assert result["parameters"] == []


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
