from __future__ import annotations

from types import SimpleNamespace

import pytest

from azure_sql_mcp.index_recommendations import IndexRecommendationService
from azure_sql_mcp.query_index_analysis import QueryIndexAnalysisService
from azure_sql_mcp.safe_sql import SafeSqlValidator

SAMPLE_SHOWPLAN = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence>
    <Batch>
      <Statements>
        <StmtSimple StatementText="SELECT CustomerId FROM dbo.Orders" StatementType="SELECT">
          <QueryPlan>
            <MissingIndexes>
              <MissingIndexGroup Impact="85.5">
                <MissingIndex Database="[appdb]" Schema="[dbo]" Table="[Orders]">
                  <ColumnGroup Usage="EQUALITY">
                    <Column Name="[CustomerId]" />
                  </ColumnGroup>
                  <ColumnGroup Usage="INEQUALITY">
                    <Column Name="[CreatedAt]" />
                  </ColumnGroup>
                  <ColumnGroup Usage="INCLUDE">
                    <Column Name="[Status]" />
                  </ColumnGroup>
                </MissingIndex>
              </MissingIndexGroup>
            </MissingIndexes>
          </QueryPlan>
        </StmtSimple>
      </Statements>
    </Batch>
  </BatchSequence>
</ShowPlanXML>
"""


class FakeExecutor:
    def __init__(self, dmv_rows=None, existing_index_rows=None):
        self.dmv_rows = dmv_rows or []
        self.existing_index_rows = existing_index_rows or []
        self.config = SimpleNamespace(row_limit=200)
        self.fetch_history = []
        self.batch_history = []
        self.session_history = []

    async def fetch_all(self, database_name, query, params=None):
        self.fetch_history.append((database_name, query, params))
        if "sys.dm_db_missing_index_details" in query:
            return self.dmv_rows
        if "FROM sys.indexes AS i" in query:
            return self.existing_index_rows
        if "FROM sys.query_store_query_text AS qt" in query:
            return [
                {
                    "query_id": 11,
                    "plan_id": 101,
                    "query_sql_text": "SELECT CustomerId FROM dbo.Orders WHERE CustomerId = 1 AND CreatedAt > '2024-01-01'",
                    "executions": 12,
                    "total_cpu_us": 500000,
                    "total_duration_us": 800000,
                    "total_logical_io_reads": 9000,
                },
                {
                    "query_id": 12,
                    "plan_id": 102,
                    "query_sql_text": "SELECT Status FROM dbo.Orders WHERE CustomerId = 2 AND CreatedAt > '2024-01-01'",
                    "executions": 8,
                    "total_cpu_us": 300000,
                    "total_duration_us": 400000,
                    "total_logical_io_reads": 7000,
                },
            ]
        return []

    async def execute_batches(self, database_name, query, params=None):
        self.batch_history.append((database_name, query, params))

        class Result:
            rows = [{"Microsoft SQL Server 2005 XML Showplan": SAMPLE_SHOWPLAN}]

        return [Result()]

    async def execute_session(self, database_name, statements, max_rows=None):
        self.session_history.append((database_name, tuple(statements), max_rows))

        class Result:
            rows = [{"Microsoft SQL Server 2005 XML Showplan": SAMPLE_SHOWPLAN}]

        return [[], [Result()], []]


def test_build_create_index_statement():
    service = IndexRecommendationService(executor=None)  # type: ignore[arg-type]
    statement = service._build_create_index_statement(  # noqa: SLF001
        {
            "schema_name": "dbo",
            "table_name": "Orders",
            "equality_columns": "[CustomerId]",
            "inequality_columns": "[CreatedAt]",
            "included_columns": "[Status], [TotalAmount]",
        }
    )

    assert "CREATE INDEX [IX_Orders_CustomerId_CreatedAt]" in statement
    assert "ON [dbo].[Orders]" in statement
    assert "([CustomerId], [CreatedAt])" in statement
    assert "INCLUDE ([Status], [TotalAmount])" in statement


@pytest.mark.asyncio
async def test_analyze_workload_indexes_consolidates_and_ranks_recommendations():
    executor = FakeExecutor(
        dmv_rows=[
            {
                "schema_name": "dbo",
                "table_name": "Orders",
                "equality_columns": "[CustomerId]",
                "inequality_columns": "[CreatedAt]",
                "included_columns": "[Status]",
                "user_seeks": 41,
                "avg_user_impact": 92.0,
                "estimated_improvement": 1800.5,
                "estimated_size_kb": None,
            }
        ]
    )
    service = IndexRecommendationService(executor=executor, validator=SafeSqlValidator())

    payload = await service.analyze_workload_indexes("appdb", window_minutes=90, top_n=5)

    assert payload["database_name"] == "appdb"
    assert payload["window_minutes"] == 90
    assert payload["queries_analyzed"] == 2
    assert len(payload["recommendations"]) == 1

    recommendation = payload["recommendations"][0]
    assert recommendation["rank"] == 1
    assert recommendation["schema"] == "dbo"
    assert recommendation["table"] == "Orders"
    assert recommendation["key_columns"] == ["CustomerId", "CreatedAt"]
    assert recommendation["include_columns"] == ["Status"]
    assert recommendation["affected_queries"] == 2
    assert recommendation["impact_score"] > 0
    assert recommendation["dmv_match"]["user_seeks"] == 41
    assert "CREATE INDEX [IX_Orders_CustomerId_CreatedAt]" in recommendation["create_index_sql"]


@pytest.mark.asyncio
async def test_analyze_queries_filters_existing_matching_indexes():
    executor = FakeExecutor(
        existing_index_rows=[
            {
                "schema_name": "dbo",
                "table_name": "Orders",
                "index_id": 2,
                "index_name": "IX_Orders_CustomerId_CreatedAt",
                "is_included_column": 0,
                "key_ordinal": 1,
                "column_name": "CustomerId",
            },
            {
                "schema_name": "dbo",
                "table_name": "Orders",
                "index_id": 2,
                "index_name": "IX_Orders_CustomerId_CreatedAt",
                "is_included_column": 0,
                "key_ordinal": 2,
                "column_name": "CreatedAt",
            },
        ]
    )
    service = QueryIndexAnalysisService(executor=executor, validator=SafeSqlValidator())

    payload = await service.analyze_queries(
        "appdb",
        ["SELECT CustomerId FROM dbo.Orders WHERE CustomerId = 1 AND CreatedAt > '2024-01-01'"],
    )

    assert payload["queries_analyzed"] == 1
    assert payload["recommendations"] == []
