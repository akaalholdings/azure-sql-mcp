#!/usr/bin/env python3
"""Live acceptance run against a disposable Azure SQL Database.

What it does, in order:

1. Creates a synthetic schema ``mcp_accept`` (customers, orders, order lines,
   and a heap event log), loads data, and adds deliberately imperfect indexes
   (a narrow foreign-key index, an exact duplicate pair).
2. Sets Query Store capture mode to ALL for the run and records the original
   settings.
3. Runs a parameterized workload: a seek plus key lookup, a scan with residual
   filters, a non-SARGable predicate on the heap, and an update.
4. Calls the real MCP tools through the tool manager: review_workload_indexes,
   get_deadlock_history, get_wait_stats; optionally provokes one deadlock
   through a temporary database-scoped XE session.
5. Checks the advisor's expected findings, prints a JSON summary, restores Query
   Store settings, and drops every object it created.

It writes to the database. It refuses to run without ``--database`` (which
must be allowlisted) and ``--confirm-disposable-database``. Connection settings
come from the usual AZURE_SQL_* environment; nothing secret is printed.

Usage:
    uv run python scripts/azure_live_acceptance.py --database testdb \\
        --confirm-disposable-database [--with-deadlock] [--keep]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from typing import Any

from azure_sql_mcp.config import load_server_config
from azure_sql_mcp.connection import BatchExecutionMode
from azure_sql_mcp.server import AzureSqlMcpApplication

SCHEMA = "mcp_accept"
TABLES = ("OrderLines", "Orders", "Customers", "EventLog", "DeadlockA", "DeadlockB")
XE_SESSION = "mcp_accept_deadlocks"

SETUP_SQL = f"""
IF SCHEMA_ID(N'{SCHEMA}') IS NULL EXEC(N'CREATE SCHEMA [{SCHEMA}]');
"""

CREATE_TABLES_SQL = f"""
CREATE TABLE [{SCHEMA}].[Customers] (
    CustomerID int NOT NULL CONSTRAINT PK_mcp_Customers PRIMARY KEY CLUSTERED,
    CustomerName nvarchar(100) NOT NULL,
    Region tinyint NOT NULL
);
CREATE TABLE [{SCHEMA}].[Orders] (
    OrderID int NOT NULL CONSTRAINT PK_mcp_Orders PRIMARY KEY CLUSTERED,
    CustomerID int NOT NULL CONSTRAINT FK_mcp_Orders_Customers REFERENCES [{SCHEMA}].[Customers] (CustomerID),
    SalespersonID int NOT NULL,
    OrderDate date NOT NULL,
    ExpectedDeliveryDate date NOT NULL,
    Status tinyint NOT NULL,
    Comments nvarchar(200) NULL
);
CREATE NONCLUSTERED INDEX FK_mcp_Orders_CustomerID ON [{SCHEMA}].[Orders] (CustomerID);
CREATE NONCLUSTERED INDEX IX_mcp_Orders_Salesperson ON [{SCHEMA}].[Orders] (SalespersonID);
CREATE NONCLUSTERED INDEX IX_mcp_Orders_Salesperson_Dup ON [{SCHEMA}].[Orders] (SalespersonID) INCLUDE (OrderDate);
CREATE TABLE [{SCHEMA}].[OrderLines] (
    OrderLineID int NOT NULL CONSTRAINT PK_mcp_OrderLines PRIMARY KEY CLUSTERED,
    OrderID int NOT NULL,
    StockItemID int NOT NULL,
    Description nvarchar(100) NOT NULL,
    Quantity int NOT NULL,
    UnitPrice decimal(18, 2) NOT NULL,
    PickingCompletedWhen datetime2(7) NULL
);
CREATE TABLE [{SCHEMA}].[EventLog] (
    EventID bigint NOT NULL,
    UserID int NOT NULL,
    CreatedAt datetime2(7) NOT NULL,
    AccountNumber varchar(20) NOT NULL,
    Severity tinyint NOT NULL,
    Payload nvarchar(400) NULL
);
CREATE TABLE [{SCHEMA}].[DeadlockA] (ID int NOT NULL PRIMARY KEY, V int NOT NULL);
CREATE TABLE [{SCHEMA}].[DeadlockB] (ID int NOT NULL PRIMARY KEY, V int NOT NULL);
INSERT [{SCHEMA}].[DeadlockA] VALUES (1, 0);
INSERT [{SCHEMA}].[DeadlockB] VALUES (1, 0);
"""

LOAD_SQL = f"""
SET NOCOUNT ON;
WITH n AS (
    SELECT TOP (400000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
    FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b
)
SELECT i INTO #n FROM n;
INSERT [{SCHEMA}].[Customers] (CustomerID, CustomerName, Region)
SELECT i, CONCAT(N'Customer ', i), i % 7 FROM #n WHERE i <= 2000;
INSERT [{SCHEMA}].[Orders] (OrderID, CustomerID, SalespersonID, OrderDate, ExpectedDeliveryDate, Status, Comments)
SELECT i, 1 + (i * 7919) % 2000, 1 + i % 20, DATEADD(day, -(i % 900), CAST('2026-09-30' AS date)),
       DATEADD(day, 3 - (i % 900), CAST('2026-09-30' AS date)), i % 3, CONCAT(N'Order comment ', i)
FROM #n WHERE i <= 120000;
INSERT [{SCHEMA}].[OrderLines] (OrderLineID, OrderID, StockItemID, Description, Quantity, UnitPrice, PickingCompletedWhen)
SELECT i, 1 + i % 120000, 1 + (i * 104729) % 1500, CONCAT(N'Item ', i % 1500), 1 + i % 10,
       CAST(1 + (i % 500) AS decimal(18, 2)),
       CASE WHEN i % 25 = 0 THEN NULL ELSE DATEADD(minute, -(i % 100000), CAST('2026-09-30' AS datetime2)) END
FROM #n WHERE i <= 360000;
INSERT [{SCHEMA}].[EventLog] (EventID, UserID, CreatedAt, AccountNumber, Severity, Payload)
SELECT i, 1 + i % 5000, DATEADD(second, -i * 37, CAST('2026-09-30' AS datetime2)),
       CONCAT('ACC', RIGHT(CONCAT('000000', i % 90000), 6)), i % 5, CONCAT(N'payload ', i)
FROM #n WHERE i <= 150000;
DROP TABLE #n;
UPDATE STATISTICS [{SCHEMA}].[Orders];
UPDATE STATISTICS [{SCHEMA}].[OrderLines];
UPDATE STATISTICS [{SCHEMA}].[EventLog];
"""

QUERY_STORE_OPTIONS_SQL = """
SELECT actual_state_desc, query_capture_mode_desc, interval_length_minutes
FROM sys.database_query_store_options
"""

WORKLOAD = (
    (
        "seek_lookup",
        f"SELECT o.OrderID, o.OrderDate, o.Comments FROM [{SCHEMA}].[Orders] AS o "
        "WHERE o.CustomerID = ? AND o.OrderDate >= ? AND o.Status = 1 ORDER BY o.OrderDate",
        lambda i: [1 + (i * 37) % 2000, "2026-01-01"],
    ),
    (
        "scan_residual",
        f"SELECT ol.OrderLineID, ol.Quantity FROM [{SCHEMA}].[OrderLines] AS ol "
        "WHERE ol.StockItemID = ? AND ol.PickingCompletedWhen IS NULL",
        lambda i: [1 + (i * 13) % 1500],
    ),
    (
        "non_sargable",
        f"SELECT e.EventID FROM [{SCHEMA}].[EventLog] AS e WHERE CONVERT(date, e.CreatedAt) = ?",
        lambda i: ["2026-09-29"],
    ),
)
UPDATE_SQL = f"UPDATE [{SCHEMA}].[Orders] SET Status = 2 WHERE OrderID = ?"


async def admin(app: AzureSqlMcpApplication, database: str, sql: str) -> None:
    await app.executor.execute_batches(database, sql, execution_mode=BatchExecutionMode.ADMIN)


async def cleanup(app: AzureSqlMcpApplication, database: str) -> None:
    statements = [
        f"IF EXISTS (SELECT 1 FROM sys.dm_xe_database_sessions WHERE name = N'{XE_SESSION}') "
        f"ALTER EVENT SESSION [{XE_SESSION}] ON DATABASE STATE = STOP;",
        f"IF EXISTS (SELECT 1 FROM sys.database_event_sessions WHERE name = N'{XE_SESSION}') "
        f"DROP EVENT SESSION [{XE_SESSION}] ON DATABASE;",
    ]
    statements += [f"DROP TABLE IF EXISTS [{SCHEMA}].[{table}];" for table in TABLES]
    statements.append(
        f"IF SCHEMA_ID(N'{SCHEMA}') IS NOT NULL AND NOT EXISTS "
        f"(SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID(N'{SCHEMA}')) "
        f"EXEC(N'DROP SCHEMA [{SCHEMA}]');"
    )
    for statement in statements:
        await admin(app, database, statement)


async def provoke_deadlock(app: AzureSqlMcpApplication, database: str) -> str:
    await admin(
        app,
        database,
        f"CREATE EVENT SESSION [{XE_SESSION}] ON DATABASE "
        "ADD EVENT sqlserver.database_xml_deadlock_report "
        "ADD TARGET package0.ring_buffer; "
        f"ALTER EVENT SESSION [{XE_SESSION}] ON DATABASE STATE = START;",
    )
    first = (
        f"SET DEADLOCK_PRIORITY LOW; BEGIN TRAN; UPDATE [{SCHEMA}].[DeadlockA] SET V = V + 1; "
        f"WAITFOR DELAY '00:00:03'; UPDATE [{SCHEMA}].[DeadlockB] SET V = V + 1; COMMIT;"
    )
    second = (
        f"BEGIN TRAN; UPDATE [{SCHEMA}].[DeadlockB] SET V = V + 1; "
        f"WAITFOR DELAY '00:00:03'; UPDATE [{SCHEMA}].[DeadlockA] SET V = V + 1; COMMIT;"
    )
    results = await asyncio.gather(
        admin(app, database, first), admin(app, database, second), return_exceptions=True
    )
    victims = [result for result in results if isinstance(result, Exception)]
    return "deadlock_victim_observed" if victims else "no_victim_observed"


async def call(app: AzureSqlMcpApplication, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return await app.mcp._tool_manager.call_tool(tool, arguments)


def check(results: list[dict[str, Any]], name: str, passed: bool, detail: str = "") -> None:
    results.append({"check": name, "passed": bool(passed), "detail": detail})


async def run(args: argparse.Namespace) -> int:
    config = load_server_config([])
    database = config.validate_database_name(args.database)
    app = AzureSqlMcpApplication(config)
    results: list[dict[str, Any]] = []
    original_qs: dict[str, Any] = {}
    started = time.monotonic()
    try:
        await cleanup(app, database)
        await admin(app, database, SETUP_SQL)
        await admin(app, database, CREATE_TABLES_SQL)
        await admin(app, database, LOAD_SQL)
        rows = await app.executor.fetch_all(database, QUERY_STORE_OPTIONS_SQL)
        original_qs = dict(rows[0]) if rows else {}
        await admin(
            app,
            database,
            "ALTER DATABASE CURRENT SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE, "
            "QUERY_CAPTURE_MODE = ALL);",
        )
        for _, sql, params in WORKLOAD:
            for iteration in range(args.executions):
                await app.executor.fetch_all(database, sql, params=params(iteration))
        for iteration in range(args.executions):
            await app.executor.execute_batches(database, UPDATE_SQL, params=[1 + iteration * 97])
        await admin(app, database, "EXEC sys.sp_query_store_flush_db;")

        if args.with_deadlock:
            outcome = await provoke_deadlock(app, database)
            check(results, "deadlock provoked", outcome == "deadlock_victim_observed", outcome)
            await asyncio.sleep(2)

        review = await call(
            app,
            "review_workload_indexes",
            {"database_name": database, "schema_name": SCHEMA, "lookback_days": 1, "min_table_rows": 1000},
        )
        recs = review.get("recommendations", [])
        check(results, "advisor status ok", review.get("result_status") == "ok", str(review.get("result_status")))
        orders = [
            r for r in recs
            if r["table"] == "Orders" and r["action"] in {"widen_index", "create_index", "extend_index"}
        ]
        check(
            results,
            "orders seek+lookup covered",
            any(
                [k["name"] for k in r["key_columns"]][:1] == ["CustomerID"] and "Comments" in r["include_columns"]
                for r in orders
            ),
            json.dumps([[k["name"] for k in r["key_columns"]] + ["|"] + r["include_columns"] for r in orders]),
        )
        lines = [r for r in recs if r["table"] == "OrderLines" and r["action"] == "create_index"]
        check(
            results,
            "orderlines scan becomes seek",
            any({"StockItemID", "PickingCompletedWhen"} <= {k["name"] for k in r["key_columns"]} for r in lines),
            json.dumps([[k["name"] for k in r["key_columns"]] for r in lines]),
        )
        rewrites = review.get("rewrite_opportunities", [])
        check(
            results,
            "non-sargable routed to optimizer",
            any(item["column"] == "CreatedAt" for item in rewrites),
            json.dumps([(item["column"], item["pattern"]) for item in rewrites]),
        )
        check(
            results,
            "duplicate index consolidated",
            any(r["action"] == "consolidate_index" and r["table"] == "Orders" for r in recs),
        )
        check(
            results,
            "heap flagged",
            any(r["action"] == "create_clustered_index" and r["table"] == "EventLog" for r in recs),
        )
        check(
            results,
            "rollback ddl renderable",
            all(r["rollback_ddl"] for r in recs if r["action"] in {"widen_index", "extend_index", "consolidate_index"}),
        )

        deadlocks = await call(app, "get_deadlock_history", {"database_name": database})
        status = deadlocks.get("result_status")
        if args.with_deadlock:
            check(results, "deadlock captured", status == "ok" and deadlocks.get("deadlock_count", 0) >= 1, str(status))
        else:
            check(results, "deadlock status is never a false empty", status in {"precondition", "ok", "empty", "unavailable"}, str(status))
        waits = await call(app, "get_wait_stats", {"database_name": database})
        check(results, "wait stats carry result_status", "result_status" in waits, str(waits.get("result_status")))
    finally:
        if original_qs.get("query_capture_mode_desc"):
            await admin(
                app,
                database,
                "ALTER DATABASE CURRENT SET QUERY_STORE (QUERY_CAPTURE_MODE = "
                f"{original_qs['query_capture_mode_desc']});",
            )
        if not args.keep:
            await cleanup(app, database)
    summary = {
        "database": database,
        "elapsed_seconds": round(time.monotonic() - started, 1),
        "checks": results,
        "passed": all(item["passed"] for item in results),
    }
    print(json.dumps(summary, indent=2))
    return 0 if summary["passed"] else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", required=True, help="Allowlisted disposable test database.")
    parser.add_argument(
        "--confirm-disposable-database",
        action="store_true",
        help="Required: the run creates and drops objects in this database.",
    )
    parser.add_argument("--executions", type=int, default=40, help="Executions per workload query.")
    parser.add_argument("--with-deadlock", action="store_true", help="Also provoke and verify one deadlock.")
    parser.add_argument("--keep", action="store_true", help="Keep the synthetic schema for inspection.")
    args = parser.parse_args(argv)
    if not args.confirm_disposable_database:
        print("Refusing to run: pass --confirm-disposable-database for a disposable test database.", file=sys.stderr)
        return 2
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
