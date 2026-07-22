from __future__ import annotations

import pytest

from azure_sql_mcp.query_regression import QueryRegressionService


class FakeExecutor:
    def __init__(self, results_by_call: list[list[dict]] | None = None):
        self._calls: list[list[dict]] = results_by_call or [[]]
        self._call_idx = 0
        self.queries: list[str] = []
        self.params: list[list[object] | None] = []

    async def fetch_all(
        self,
        database_name: str,
        query: str,
        params: list[object] | None = None,
    ) -> list[dict]:
        self.queries.append(query)
        self.params.append(params)
        if self._call_idx < len(self._calls):
            result = self._calls[self._call_idx]
            self._call_idx += 1
            return result
        return []


@pytest.mark.asyncio
async def test_detect_parameter_sniffing():
    rows = [
        {"query_id": 42, "query_sql_text": "SELECT * FROM Orders WHERE status = @p1", "plan_count": 3, "best_avg_duration_ms": 5.0, "worst_avg_duration_ms": 500.0, "duration_variance_ratio": 100.0, "best_avg_cpu_ms": 2.0, "worst_avg_cpu_ms": 200.0, "total_executions": 10000, "best_plan_id": 101, "worst_plan_id": 103},
    ]
    service = QueryRegressionService(FakeExecutor([rows]))
    result = await service.detect_parameter_sniffing("testdb", variance_threshold=10.0)

    assert result["affected_query_count"] == 1
    assert result["queries"][0]["duration_variance_ratio"] == 100.0
    assert result["queries"][0]["plan_count"] == 3


@pytest.mark.asyncio
async def test_detect_parameter_sniffing_none():
    service = QueryRegressionService(FakeExecutor([[]]))
    result = await service.detect_parameter_sniffing("testdb")
    assert result["affected_query_count"] == 0


@pytest.mark.asyncio
async def test_detect_regressed_queries():
    rows = [
        {"reason": "AverageQueryDurationChanged", "score": 85, "current_state": "Active", "state_reason": "SchemaChanged", "tuning_script": "exec sp_query_store_force_plan @query_id=42, @plan_id=101", "query_id": "42", "regressed_plan_id": "103", "recommended_plan_id": "101", "estimated_cpu_gain": "50", "estimated_duration_gain": "75", "details": '{"queryId": 42}'},
    ]
    executor = FakeExecutor([rows])
    service = QueryRegressionService(executor)
    result = await service.detect_regressed_queries("testdb", window_minutes=60)

    assert result["window_minutes"] == 60
    assert result["recommendation_count"] == 1
    assert result["recommendations"][0]["score"] == 85
    assert "RecentPlanActivity" in executor.queries[0]
    assert "execute_action_initiated_by" in executor.queries[0]
    assert executor.params == [[60]]
    # details should be parsed from JSON string
    assert result["recommendations"][0]["details"] == {"queryId": 42}


@pytest.mark.asyncio
async def test_compare_query_plans():
    rows = [
        {"plan_id": 101, "query_id": 42, "is_forced_plan": False, "force_failure_count": 0, "avg_duration_ms": 5.0, "avg_cpu_ms": 2.0, "avg_logical_io_reads": 100, "avg_physical_io_reads": 5, "count_executions": 5000, "first_execution_time": "2026-03-01", "last_execution_time": "2026-04-01", "query_plan_xml": "<ShowPlanXML/>", "best_rank": 1, "worst_rank": 2},
        {"plan_id": 103, "query_id": 42, "is_forced_plan": False, "force_failure_count": 0, "avg_duration_ms": 500.0, "avg_cpu_ms": 200.0, "avg_logical_io_reads": 50000, "avg_physical_io_reads": 1000, "count_executions": 100, "first_execution_time": "2026-03-15", "last_execution_time": "2026-04-01", "query_plan_xml": "<ShowPlanXML/>", "best_rank": 2, "worst_rank": 1},
    ]
    service = QueryRegressionService(FakeExecutor([rows]))
    result = await service.compare_query_plans("testdb", query_id=42)

    assert len(result["plans"]) == 2
    assert result["comparison"]["duration_ratio"] == 100.0  # 500/5
    assert result["comparison"]["cpu_ratio"] == 100.0  # 200/2
    assert result["comparison"]["io_ratio"] == 500.0  # 50000/100

    # plan_xml should be removed, replaced with length
    for plan in result["plans"]:
        assert "query_plan_xml" not in plan
        assert plan["plan_xml_length"] > 0


@pytest.mark.asyncio
async def test_compare_query_plans_single_plan():
    rows = [
        {"plan_id": 101, "query_id": 42, "is_forced_plan": False, "force_failure_count": 0, "avg_duration_ms": 5.0, "avg_cpu_ms": 2.0, "avg_logical_io_reads": 100, "avg_physical_io_reads": 5, "count_executions": 5000, "first_execution_time": "2026-03-01", "last_execution_time": "2026-04-01", "query_plan_xml": ""},
    ]
    service = QueryRegressionService(FakeExecutor([rows]))
    result = await service.compare_query_plans("testdb", query_id=42)
    assert len(result["plans"]) == 1
    assert result["comparison"] == {}  # Can't compare with only 1 plan


@pytest.mark.asyncio
async def test_get_forced_plans_warns_stale_and_failing():
    rows = [
        {"plan_id": 101, "query_id": 42, "query_sql_text": "SELECT ...", "is_forced_plan": True, "force_failure_count": 0, "last_force_failure_reason_desc": None, "avg_duration_ms": 5.0, "avg_cpu_ms": 2.0, "avg_logical_io_reads": 100, "count_executions": 5000, "last_execution_time": "2026-04-01", "days_since_last_exec": 0},
        {"plan_id": 201, "query_id": 99, "query_sql_text": "SELECT ...", "is_forced_plan": True, "force_failure_count": 3, "last_force_failure_reason_desc": "SCHEMA_CHANGE", "avg_duration_ms": 10.0, "avg_cpu_ms": 5.0, "avg_logical_io_reads": 200, "count_executions": 100, "last_execution_time": "2026-03-15", "days_since_last_exec": 17},
    ]
    executor = FakeExecutor([rows])
    service = QueryRegressionService(executor)
    result = await service.get_forced_plans("testdb", window_minutes=120)

    assert result["window_minutes"] == 120
    assert result["forced_plan_count"] == 2
    assert result["stale_count"] == 1  # plan 201, 17 days > 7
    assert result["failing_count"] == 1  # plan 201, force_failure_count > 0

    warning_types = [w["type"] for w in result["warnings"]]
    assert "stale_forced_plans" in warning_types
    assert "failing_forced_plans" in warning_types
    assert "recent_execution_count" in executor.queries[0]
    assert "plan_forcing_type_desc" in executor.queries[0]
    assert executor.params == [[120]]


@pytest.mark.asyncio
async def test_get_forced_plans_no_warnings():
    rows = [
        {"plan_id": 101, "query_id": 42, "query_sql_text": "SELECT ...", "is_forced_plan": True, "force_failure_count": 0, "last_force_failure_reason_desc": None, "avg_duration_ms": 5.0, "avg_cpu_ms": 2.0, "avg_logical_io_reads": 100, "count_executions": 5000, "last_execution_time": "2026-04-01", "days_since_last_exec": 0},
    ]
    service = QueryRegressionService(FakeExecutor([rows]))
    result = await service.get_forced_plans("testdb")
    assert result["warnings"] == []


class TestExtractTopOperators:
    def test_empty_xml(self):
        assert QueryRegressionService._extract_top_operators("") == []

    def test_invalid_xml(self):
        assert QueryRegressionService._extract_top_operators("<bad") == []

    def test_valid_showplan_xml(self):
        xml = """<?xml version="1.0"?>
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
          <BatchSequence>
            <Batch>
              <Statements>
                <StmtSimple>
                  <QueryPlan>
                    <RelOp PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan">
                      <RelOp PhysicalOp="Nested Loops" LogicalOp="Inner Join">
                        <RelOp PhysicalOp="Index Seek" LogicalOp="Index Seek"/>
                      </RelOp>
                    </RelOp>
                  </QueryPlan>
                </StmtSimple>
              </Statements>
            </Batch>
          </BatchSequence>
        </ShowPlanXML>
        """
        ops = QueryRegressionService._extract_top_operators(xml)
        assert "Clustered Index Scan" in ops
        assert "Nested Loops" in ops
        assert "Index Seek" in ops


_SHOWPLAN_NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"

_PLAN_XML_EU = f"""<ShowPlanXML xmlns="{_SHOWPLAN_NS}">
  <BatchSequence><Batch><Statements><StmtSimple>
    <QueryPlan>
      <ParameterList>
        <ColumnReference Column="@region" ParameterDataType="nvarchar(10)"
            ParameterCompiledValue="N'EU'" ParameterRuntimeValue="N'EU'" />
        <ColumnReference Column="@rows" ParameterDataType="int"
            ParameterCompiledValue="(100)" />
      </ParameterList>
    </QueryPlan>
  </StmtSimple></Statements></Batch></BatchSequence>
</ShowPlanXML>"""

_PLAN_XML_US = _PLAN_XML_EU.replace("N'EU'", "N'US'").replace("(100)", "(5000000)")


@pytest.mark.asyncio
async def test_get_query_parameter_buckets_extracts_compiled_values():
    rows = [
        {"plan_id": 7, "query_plan_xml": _PLAN_XML_EU, "is_forced_plan": False,
         "executions": 900, "avg_duration_ms": 4.0, "last_execution_time": "2026-07-01"},
        {"plan_id": 9, "query_plan_xml": _PLAN_XML_US, "is_forced_plan": False,
         "executions": 40, "avg_duration_ms": 900.0, "last_execution_time": "2026-07-02"},
    ]
    service = QueryRegressionService(FakeExecutor([rows]))
    result = await service.get_query_parameter_buckets("testdb", 42)

    assert result["query_id"] == 42
    assert result["plan_count"] == 2
    eu, us = result["buckets"]
    assert eu["plan_id"] == 7
    assert eu["parameters"][0] == {
        "name": "@region", "data_type": "nvarchar(10)",
        "compiled_value": "N'EU'", "runtime_value": "N'EU'",
    }
    assert eu["parameters"][1]["compiled_value"] == "(100)"
    assert us["parameters"][0]["compiled_value"] == "N'US'"
    # two plans compiled with different values -> two buckets to test
    assert len(result["distinct_parameter_sets"]) == 2
    assert "boundary" in result["note"]


@pytest.mark.asyncio
async def test_get_query_parameter_buckets_dedupes_identical_sets_and_tolerates_bad_xml():
    rows = [
        {"plan_id": 7, "query_plan_xml": _PLAN_XML_EU, "is_forced_plan": False,
         "executions": 900, "avg_duration_ms": 4.0, "last_execution_time": None},
        {"plan_id": 8, "query_plan_xml": _PLAN_XML_EU, "is_forced_plan": True,
         "executions": 10, "avg_duration_ms": 5.0, "last_execution_time": None},
        {"plan_id": 9, "query_plan_xml": "<not-xml", "is_forced_plan": False,
         "executions": 1, "avg_duration_ms": 1.0, "last_execution_time": None},
    ]
    service = QueryRegressionService(FakeExecutor([rows]))
    result = await service.get_query_parameter_buckets("testdb", 42)

    assert result["plan_count"] == 3
    assert len(result["distinct_parameter_sets"]) == 1     # identical sets deduped
    assert result["buckets"][2]["parameters"] == []        # bad XML -> empty, not an error


@pytest.mark.asyncio
async def test_get_query_parameter_buckets_rejects_bad_query_id():
    service = QueryRegressionService(FakeExecutor([[]]))
    with pytest.raises(ValueError, match="query_id"):
        await service.get_query_parameter_buckets("testdb", 0)
