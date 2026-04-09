from __future__ import annotations

import pytest

from azure_sql_mcp.plans import PlansService
from azure_sql_mcp.safe_sql import SafeSqlValidator

SAMPLE_SHOWPLAN = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence>
    <Batch>
      <Statements>
        <StmtSimple StatementText="SELECT name FROM sys.objects" StatementType="SELECT" StatementSubTreeCost="0.013" StatementEstRows="10">
          <QueryPlan>
            <RelOp PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" EstimateRows="10" EstimateIO="0.01" EstimateCPU="0.003" EstimatedTotalSubtreeCost="0.013">
              <Warnings NoJoinPredicate="false" />
            </RelOp>
          </QueryPlan>
        </StmtSimple>
      </Statements>
    </Batch>
  </BatchSequence>
</ShowPlanXML>
"""


class FakeExecutor:
    def __init__(self, *, can_create_index=True, fail_create=False):
        self.can_create_index = can_create_index
        self.fail_create = fail_create
        self.fetch_history = []
        self.batch_history = []
        self.session_history = []
        self.non_query_history = []

    async def fetch_all(self, database_name, query, params=None):
        self.fetch_history.append((database_name, query, params))
        return [{"can_create_index": 1 if self.can_create_index else 0}]

    async def execute_batches(self, database_name, query, params=None):
        self.batch_history.append((database_name, query, params))

        class Result:
            rows = [{"plan_xml": SAMPLE_SHOWPLAN}]

        return [Result()]

    async def execute_session(self, database_name, statements, *, max_rows=None):
        self.session_history.append((database_name, list(statements)))

        class Result:
            rows = [{"plan_xml": SAMPLE_SHOWPLAN}]

        # Return one results-list per statement; the user query is at index 1.
        return [[], [Result()], []]

    async def execute_non_query(self, database_name, query, params=None):
        self.non_query_history.append((database_name, query, params))
        if self.fail_create and query.startswith("CREATE NONCLUSTERED INDEX"):
            raise RuntimeError("create index failed")
        return 0


def test_summarize_showplan_xml():
    service = PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]
    summary = service.summarize_showplan_xml(SAMPLE_SHOWPLAN)

    assert summary["statement_count"] == 1
    assert summary["operator_count"] == 1
    assert summary["statements"][0]["statement_type"] == "SELECT"
    assert summary["top_operators"][0]["physical_op"] == "Clustered Index Scan"
    assert summary["warnings"] == [{"NoJoinPredicate": "false"}]


@pytest.mark.asyncio
async def test_explain_query_with_hypothetical_indexes_creates_plan_and_cleans_up():
    executor = FakeExecutor()
    service = PlansService(executor=executor, validator=SafeSqlValidator())

    artifact = await service.explain_query(
        database_name="appdb",
        sql="SELECT name FROM sys.objects",
        analyze=False,
        hypothetical_indexes=[
            {
                "schema": "dbo",
                "table": "Orders",
                "columns": ["CustomerId", "CreatedAt"],
                "include_columns": ["Status"],
            }
        ],
    )

    assert artifact.summary["hypothetical_analysis"] is True
    assert len(artifact.summary["hypothetical_indexes"]) == 1
    assert executor.fetch_history
    assert any(
        query.startswith("CREATE NONCLUSTERED INDEX")
        for _, query, _ in executor.non_query_history
    )
    assert any(
        query.startswith("DROP INDEX")
        for _, query, _ in executor.non_query_history
    )
    assert executor.session_history, "expected execute_session to be invoked"
    statements = executor.session_history[0][1]
    assert any("SET SHOWPLAN_XML ON" in s for s in statements)
    assert any("SET SHOWPLAN_XML OFF" in s for s in statements)


@pytest.mark.asyncio
async def test_explain_query_with_hypothetical_indexes_requires_permission():
    executor = FakeExecutor(can_create_index=False)
    service = PlansService(executor=executor, validator=SafeSqlValidator())

    with pytest.raises(PermissionError, match="CREATE INDEX permission"):
        await service.explain_query(
            database_name="appdb",
            sql="SELECT name FROM sys.objects",
            analyze=False,
            hypothetical_indexes=[
                {"schema": "dbo", "table": "Orders", "columns": ["CustomerId"]}
            ],
        )


@pytest.mark.asyncio
async def test_explain_query_cleans_up_hypothetical_indexes_on_failure():
    executor = FakeExecutor(fail_create=True)
    service = PlansService(executor=executor, validator=SafeSqlValidator())

    with pytest.raises(RuntimeError, match="Hypothetical index analysis failed"):
        await service.explain_query(
            database_name="appdb",
            sql="SELECT name FROM sys.objects",
            analyze=False,
            hypothetical_indexes=[
                {"schema": "dbo", "table": "Orders", "columns": ["CustomerId"]}
            ],
        )

    # Failed CREATE should not prevent the drop path from being attempted for any completed setup.
    assert executor.non_query_history[0][1].startswith("CREATE NONCLUSTERED INDEX")
