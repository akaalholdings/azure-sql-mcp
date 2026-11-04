"""Table-valued function references in the database-aware equivalence preflight.

STRING_SPLIT and user table-valued functions are local, inspectable row
sources. Reporting them as cross-database dependencies blocks proof for the
common ID-list and MSTVF-to-iTVF rewrites. Inspecting them is also required
for soundness: a function body can hide GETDATE() as well as a view can.
"""

from __future__ import annotations

import json

import pytest

from azure_sql_mcp.equivalence_preflight import EquivalencePreflightService


def _resolver(objects, calls):
    async def resolve(_database_name: str, schema_name: str, object_name: str):
        calls.append((schema_name, object_name))
        return objects.get((schema_name, object_name))

    return resolve


_TABLES = {
    ("dbo", "Orders"): {"object_type": "USER_TABLE"},
    ("dbo", "Customers"): {"object_type": "USER_TABLE"},
}

_ITVF = {
    "object_type": "SQL_INLINE_TABLE_VALUED_FUNCTION",
    "schema_name": "dbo",
    "object_name": "fn_OrdersForCustomer",
    "definition": (
        "CREATE FUNCTION dbo.fn_OrdersForCustomer (@c int)\n"
        "RETURNS TABLE\nWITH SCHEMABINDING\nAS\nRETURN\n(\n"
        "    SELECT o.OrderId FROM dbo.Orders AS o WHERE o.CustomerId = @c\n);"
    ),
}

_MSTVF = {
    "object_type": "SQL_TABLE_VALUED_FUNCTION",
    "schema_name": "dbo",
    "object_name": "fn_OrdersForCustomer",
    "definition": (
        "CREATE FUNCTION dbo.fn_OrdersForCustomer (@c int)\n"
        "RETURNS @result TABLE (OrderId int NOT NULL)\nAS\nBEGIN\n"
        "    INSERT INTO @result (OrderId)\n"
        "    SELECT o.OrderId FROM dbo.Orders AS o WHERE o.CustomerId = @c;\n"
        "    RETURN;\nEND"
    ),
}


async def _analyze(sql: str, objects=None):
    calls: list[tuple[str, str]] = []
    result = await EquivalencePreflightService(
        _resolver({**_TABLES, **(objects or {})}, calls)
    ).analyze("appdb", sql)
    return result.as_dict(), calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT o.OrderId FROM dbo.Orders AS o WHERE o.CustomerId IN "
        "(SELECT CAST(s.value AS int) FROM STRING_SPLIT(@ids, ','))",
        "SELECT o.OrderId FROM dbo.Orders AS o INNER JOIN "
        "STRING_SPLIT(@ids, ',') AS s ON o.CustomerId = CAST(s.value AS int)",
        "SELECT o.OrderId FROM dbo.Orders AS o CROSS APPLY "
        "STRING_SPLIT(@ids, ',') AS s WHERE o.CustomerId = CAST(s.value AS int)",
    ],
)
async def test_string_split_is_a_deterministic_builtin_row_source(sql: str) -> None:
    payload, calls = await _analyze(sql)

    assert payload["classification"] == "direct_snapshot"
    assert payload["unresolved_dependencies"] == []
    assert calls == [("dbo", "Orders")]


@pytest.mark.asyncio
@pytest.mark.parametrize("definition", [_ITVF, _MSTVF])
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT f.OrderId FROM dbo.fn_OrdersForCustomer(@c) AS f",
        "SELECT f.OrderId FROM dbo.Customers AS c CROSS APPLY "
        "dbo.fn_OrdersForCustomer(c.CustomerId) AS f",
    ],
)
async def test_deterministic_table_valued_function_body_is_inspected(
    sql: str,
    definition,
) -> None:
    payload, calls = await _analyze(
        sql,
        {("dbo", "fn_OrdersForCustomer"): definition},
    )

    assert payload["classification"] == "direct_snapshot"
    assert payload["coverage_complete"] is True
    assert ("dbo", "fn_OrdersForCustomer") in calls
    # The body was read: its own table reference was resolved too.
    assert ("dbo", "Orders") in calls
    assert payload["resolved_dependencies"][0]["object_name"] == (
        "fn_OrdersForCustomer"
    )
    assert payload["resolved_dependencies"][0]["dependency_type"] == (
        "table_valued_function"
    )
    assert "CustomerId = @c" not in json.dumps(payload)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "definition",
    [
        "CREATE FUNCTION dbo.fn_Recent (@c int) RETURNS TABLE AS RETURN "
        "(SELECT o.OrderId FROM dbo.Orders AS o WHERE o.CreatedAt < GETDATE())",
        "CREATE FUNCTION dbo.fn_Recent (@c int) RETURNS @r TABLE (OrderId int) "
        "AS BEGIN IF @c > 0 INSERT @r SELECT o.OrderId FROM dbo.Orders AS o "
        "WHERE o.CreatedAt < SYSDATETIME(); RETURN; END",
    ],
)
async def test_applied_function_with_clock_body_is_flagged(definition: str) -> None:
    payload, _calls = await _analyze(
        "SELECT f.OrderId FROM dbo.Customers AS c "
        "CROSS APPLY dbo.fn_Recent(c.CustomerId) AS f",
        {
            ("dbo", "fn_Recent"): {
                "object_type": "SQL_TABLE_VALUED_FUNCTION",
                "schema_name": "dbo",
                "object_name": "fn_Recent",
                "definition": definition,
            }
        },
    )

    assert payload["direct_snapshot_supported"] is False
    assert "statement_stable_clock" in payload["risk_codes"]
    assert any(risk["source"] == "dbo.fn_Recent" for risk in payload["risks"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("resolution", "reason"),
    [
        (None, "unresolved_function_dependency"),
        ({"status": "encrypted"}, "encrypted_function_dependency"),
        (
            {
                "object_type": "SQL_INLINE_TABLE_VALUED_FUNCTION",
                "schema_name": "dbo",
                "object_name": "fn_Hidden",
                "definition": None,
            },
            "unresolved_function_dependency",
        ),
        # A CLR function has no readable T-SQL body.
        (
            {
                "object_type": "CLR_TABLE_VALUED_FUNCTION",
                "schema_name": "dbo",
                "object_name": "fn_Hidden",
            },
            "unresolved_function_dependency",
        ),
        (
            {"object_type": "FT", "schema_name": "dbo", "object_name": "fn_Hidden"},
            "unresolved_function_dependency",
        ),
    ],
)
async def test_unreadable_function_body_fails_closed(resolution, reason: str) -> None:
    payload, _calls = await _analyze(
        "SELECT f.OrderId FROM dbo.fn_Hidden(@c) AS f",
        {("dbo", "fn_Hidden"): resolution},
    )

    assert payload["direct_snapshot_supported"] is False
    assert payload["coverage_complete"] is False
    assert payload["unresolved_dependencies"][0]["object_name"] == "fn_Hidden"
    assert payload["unresolved_dependencies"][0]["reason"] == reason


@pytest.mark.asyncio
async def test_linked_server_rowset_function_remains_external_and_sanitized() -> None:
    payload, calls = await _analyze(
        "SELECT q.id FROM OPENQUERY(remote_server, 'SELECT id FROM t') AS q"
    )

    assert payload["direct_snapshot_supported"] is False
    assert payload["unresolved_dependencies"][0]["reason"] == (
        "cross_database_dependency"
    )
    assert payload["unresolved_dependencies"][0]["catalog_name"] == (
        "dynamic_external_source"
    )
    assert calls == []
    assert "remote_server" not in json.dumps(payload)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT f.id FROM OtherDb.dbo.fn_Remote(1) AS f",
        "SELECT f.id FROM dbo.Customers AS c "
        "CROSS APPLY OtherDb.dbo.fn_Remote(c.CustomerId) AS f",
    ],
)
async def test_cross_database_function_is_not_inspected(sql: str) -> None:
    payload, _calls = await _analyze(sql)

    assert payload["direct_snapshot_supported"] is False
    assert payload["unresolved_dependencies"][0]["reason"] == (
        "cross_database_dependency"
    )
    assert payload["unresolved_dependencies"][0]["object_name"] == "fn_Remote"


@pytest.mark.asyncio
async def test_table_variable_is_not_resolved_as_a_catalog_object() -> None:
    payload, calls = await _analyze(
        "DECLARE @ids TABLE (id int); "
        "SELECT o.OrderId FROM dbo.Orders AS o INNER JOIN @ids AS i "
        "ON i.id = o.CustomerId"
    )

    assert payload["classification"] == "direct_snapshot"
    assert calls == [("dbo", "Orders")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "definition",
    [
        # Not a table-function definition at all.
        "SELECT o.OrderId FROM dbo.Orders AS o",
        # The multi-statement body does not parse.
        "CREATE FUNCTION dbo.fn_Broken (@c int) RETURNS @r TABLE (OrderId int) "
        "AS BEGIN INSERT @r SELECT FROM WHERE; RETURN; END",
    ],
)
async def test_unparseable_function_body_fails_closed(definition: str) -> None:
    payload, _calls = await _analyze(
        "SELECT f.OrderId FROM dbo.fn_Broken(@c) AS f",
        {
            ("dbo", "fn_Broken"): {
                "object_type": "SQL_TABLE_VALUED_FUNCTION",
                "schema_name": "dbo",
                "object_name": "fn_Broken",
                "definition": definition,
            }
        },
    )

    assert payload["direct_snapshot_supported"] is False
    assert payload["unresolved_dependencies"][0]["reason"] == (
        "invalid_function_definition"
    )


# XML nodes() shreds a column or variable the query already reads. It is not a
# catalog function: treating `o.Payload.nodes(...)` as catalog.schema.function
# caps a common XML-shredding rewrite at performance_only with a false
# cross-database reason.
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT o.OrderId, n.c.value('@id', 'int') AS v FROM dbo.Orders AS o "
        "CROSS APPLY o.Payload.nodes('/r/i') AS n(c)",
        "SELECT o.OrderId FROM dbo.Orders AS o "
        "OUTER APPLY o.Payload.nodes('/r/i') AS n(c)",
        "SELECT Orders.OrderId FROM dbo.Orders "
        "CROSS APPLY Orders.Payload.nodes('/r/i') AS n(c)",
        "SELECT m.d.value('.', 'int') AS v FROM dbo.Orders AS o "
        "CROSS APPLY o.Payload.nodes('/r') AS n(c) "
        "CROSS APPLY n.c.nodes('i') AS m(d)",
        "SELECT o.OrderId FROM dbo.Orders AS o WHERE EXISTS "
        "(SELECT 1 FROM o.Payload.nodes('/r/i') AS n(c))",
        "DECLARE @x xml = N'<r/>'; "
        "SELECT n.c.value('@id', 'int') AS v FROM @x.nodes('/r/i') AS n(c)",
        "DECLARE @x xml = N'<r/>'; SELECT o.OrderId FROM dbo.Orders AS o "
        "CROSS APPLY @x.nodes('/r/i') AS n(c)",
    ],
)
async def test_xml_nodes_method_is_not_a_function_dependency(sql: str) -> None:
    payload, calls = await _analyze(sql)

    assert payload["classification"] == "direct_snapshot"
    assert payload["unresolved_dependencies"] == []
    assert all(name.casefold() != "nodes" for _schema, name in calls)


@pytest.mark.asyncio
async def test_column_nodes_method_without_alias_is_not_a_dependency() -> None:
    # `Payload.nodes()` reads like schema.function; no such catalog object
    # exists, so SQL binds it to the XML column.
    payload, calls = await _analyze(
        "SELECT n.c.value('@id', 'int') AS v FROM dbo.Orders "
        "CROSS APPLY Payload.nodes('/r/i') AS n(c)"
    )

    assert payload["classification"] == "direct_snapshot"
    assert payload["unresolved_dependencies"] == []
    assert ("Payload", "nodes") in calls


@pytest.mark.asyncio
async def test_schema_function_named_nodes_is_still_inspected() -> None:
    payload, calls = await _analyze(
        "SELECT f.OrderId FROM dbo.Customers AS c "
        "CROSS APPLY dbo.nodes(c.CustomerId) AS f",
        {
            ("dbo", "nodes"): {
                "object_type": "SQL_INLINE_TABLE_VALUED_FUNCTION",
                "schema_name": "dbo",
                "object_name": "nodes",
                "definition": (
                    "CREATE FUNCTION dbo.nodes (@c int) RETURNS TABLE AS RETURN "
                    "(SELECT o.OrderId FROM dbo.Orders AS o "
                    "WHERE o.CreatedAt < GETDATE())"
                ),
            }
        },
    )

    assert ("dbo", "nodes") in calls
    assert payload["direct_snapshot_supported"] is False
    assert "statement_stable_clock" in payload["risk_codes"]
    assert any(risk["source"] == "dbo.nodes" for risk in payload["risks"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sql", "resolution", "reason"),
    [
        (
            "SELECT f.OrderId FROM dbo.Customers AS c "
            "CROSS APPLY dbo.nodes(c.CustomerId) AS f",
            {"status": "encrypted"},
            "encrypted_function_dependency",
        ),
        (
            "SELECT f.OrderId FROM dbo.Customers AS c "
            "CROSS APPLY dbo.nodes(c.CustomerId) AS f",
            {
                "object_type": "SQL_INLINE_TABLE_VALUED_FUNCTION",
                "schema_name": "dbo",
                "object_name": "nodes",
                "definition": None,
            },
            "unresolved_function_dependency",
        ),
        (
            "SELECT f.OrderId FROM dbo.Customers AS c "
            "CROSS APPLY OtherDb.dbo.nodes(c.CustomerId) AS f",
            None,
            "cross_database_dependency",
        ),
    ],
)
async def test_unreadable_function_named_nodes_fails_closed(
    sql: str,
    resolution,
    reason: str,
) -> None:
    payload, _calls = await _analyze(sql, {("dbo", "nodes"): resolution})

    assert payload["direct_snapshot_supported"] is False
    assert payload["unresolved_dependencies"][0]["object_name"] == "nodes"
    assert payload["unresolved_dependencies"][0]["reason"] == reason
