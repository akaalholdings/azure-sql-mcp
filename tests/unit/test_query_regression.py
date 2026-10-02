from __future__ import annotations

import json

import pytest

from azure_sql_mcp.query_regression import QueryRegressionService
from azure_sql_mcp.query_regression import parse_tuning_recommendation


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


# sys.dm_db_tuning_recommendations.details as Microsoft Learn documents it: the
# ids and the pre-detection stats sit under $.planForceDetails (not the root),
# the script under $.implementationDetails, and CPU averages are microseconds.
_FORCE_DETAILS_42 = (
    '{"planForceDetails":{"queryId":42,"regressedPlanId":103,'
    '"regressedPlanExecutionCount":13,"regressedPlanErrorCount":0,'
    '"regressedPlanCpuTimeAverage":2.006833076923077e+006,'
    '"regressedPlanCpuTimeStddev":6.007066208918931e+005,'
    '"recommendedPlanId":101,"recommendedPlanExecutionCount":11,'
    '"recommendedPlanErrorCount":0,'
    '"recommendedPlanCpuTimeAverage":9.990909090909091e+002,'
    '"recommendedPlanCpuTimeStddev":9.428818454583705e+002},'
    '"implementationDetails":{"method":"TSql",'
    '"script":"exec sp_query_store_force_plan @query_id = 42, @plan_id = 101"}}'
)
_ACTIVE_STATE = '{"currentValue":"Active","reason":"AutomaticTuningOptionNotEnabled"}'


def _dmv_row(details: str, state: str = _ACTIVE_STATE, score: int = 85) -> dict:
    """One raw sys.dm_db_tuning_recommendations row (state/details are JSON text)."""
    return {
        "type": "FORCE_LAST_GOOD_PLAN",
        "reason": "Average query CPU time changed from 1ms to 2006.83ms",
        "score": score,
        "state": state,
        "details": details,
        "is_executable_action": True,
        "is_revertable_action": True,
        "execute_action_initiated_by": None,
        "revert_action_initiated_by": None,
        "valid_since": "2026-09-30T10:00:00",
        "last_refresh": "2026-10-01T10:00:00",
    }


@pytest.mark.asyncio
async def test_detect_regressed_queries_parses_plan_force_details():
    # The live regression: only the regressed plan 103 ran in the window. The
    # recommended last-good plan 101 exists in Query Store but did not run.
    activity = [
        {"plan_id": 103, "query_id": 42, "is_forced_plan": False,
         "last_seen_utc": "2026-10-01T11:00:00", "recent_execution_count": 250},
        {"plan_id": 101, "query_id": 42, "is_forced_plan": False,
         "last_seen_utc": None, "recent_execution_count": None},
    ]
    executor = FakeExecutor([[_dmv_row(_FORCE_DETAILS_42)], activity])
    service = QueryRegressionService(executor)
    result = await service.detect_regressed_queries("testdb", window_minutes=60)

    assert result["window_minutes"] == 60
    assert result["recommendation_count"] == 1
    rec = result["recommendations"][0]
    assert rec["query_id"] == 42
    assert rec["regressed_plan_id"] == 103
    assert rec["recommended_plan_id"] == 101
    assert rec["current_state"] == "Active"
    assert rec["state_reason"] == "AutomaticTuningOptionNotEnabled"
    assert rec["tuning_script"] == "exec sp_query_store_force_plan @query_id = 42, @plan_id = 101"
    assert rec["score"] == 85
    assert rec["live"] is True
    assert rec["regressed_plan_recent_execution_count"] == 250
    assert rec["recommended_plan_recent_execution_count"] == 0
    assert rec["recommended_plan_is_forced"] is False
    assert rec["last_seen_utc"] == "2026-10-01T11:00:00"
    # Microsoft's formula: (regExec + recExec) * (regCpuAvg - recCpuAvg) / 1e6.
    assert rec["estimated_cpu_gain"] == pytest.approx(
        (13 + 11) * (2006833.076923077 - 999.0909090909091) / 1_000_000
    )
    assert rec["estimated_cpu_gain_unit"] == "cpu_seconds"
    assert rec["estimated_duration_gain"] is None  # not in the DMV
    assert rec["error_prone"] is False
    assert rec["details"]["planForceDetails"]["queryId"] == 42

    # The DMV is read raw: no root-level JSON paths, no join that can drop rows.
    assert "sys.dm_db_tuning_recommendations" in executor.queries[0]
    assert "$.queryId" not in executor.queries[0]
    assert "JOIN" not in executor.queries[0].upper()
    # Activity is read for the plans the recommendations name, inside the window.
    assert executor.params[1] is not None
    assert executor.params[1][0] == 60
    assert sorted(executor.params[1][1:]) == [101, 103]


@pytest.mark.asyncio
async def test_detect_regressed_queries_drops_no_row_and_sorts_live_first():
    expired_details = _FORCE_DETAILS_42.replace('"queryId":42', '"queryId":77').replace(
        "103", "703"
    ).replace("101", "701")
    rows = [
        _dmv_row(
            expired_details,
            state='{"currentValue":"Expired","reason":"StatisticsChanged"}',
            score=99,
        ),
        _dmv_row(_FORCE_DETAILS_42, score=40),
    ]
    activity = [
        {"plan_id": 103, "query_id": 42, "is_forced_plan": False,
         "last_seen_utc": "2026-10-01T11:00:00", "recent_execution_count": 5},
    ]
    service = QueryRegressionService(FakeExecutor([rows, activity]))
    result = await service.detect_regressed_queries("testdb")

    # Same count as SELECT COUNT(*) FROM the DMV: nothing filtered away.
    assert result["recommendation_count"] == 2
    first, second = result["recommendations"]
    assert (first["query_id"], first["live"]) == (42, True)
    assert (second["query_id"], second["live"]) == (77, False)
    assert second["current_state"] == "Expired"
    # Plans that are no longer in Query Store are unknown, not "not forced".
    assert second["recommended_plan_is_forced"] is None


@pytest.mark.asyncio
async def test_detect_regressed_queries_keeps_rows_with_unreadable_details():
    executor = FakeExecutor([[_dmv_row("not json", state="also not json")]])
    service = QueryRegressionService(executor)
    result = await service.detect_regressed_queries("testdb")

    assert result["recommendation_count"] == 1
    rec = result["recommendations"][0]
    assert rec["query_id"] is None
    assert rec["current_state"] is None
    assert rec["live"] is False
    assert rec["estimated_cpu_gain"] is None
    assert rec["details"] == "not json"
    assert len(executor.queries) == 1  # no plan ids -> no activity query


def test_parse_tuning_recommendation_accepts_documented_aborted_count_names():
    # The DMV column reference names the error counters *AbortedCount while
    # Microsoft's sample queries read *ErrorCount; accept either.
    details = (
        '{"planForceDetails":{"queryId":5,"regressedPlanId":2,"recommendedPlanId":1,'
        '"regressedPlanAbortedCount":4,"recommendedPlanAbortedCount":0}}'
    )
    parsed = parse_tuning_recommendation(_dmv_row(details))

    assert parsed["regressed_plan_error_count"] == 4
    assert parsed["recommended_plan_error_count"] == 0
    assert parsed["error_prone"] is True


@pytest.mark.asyncio
async def test_detect_regressed_queries_is_not_live_when_only_the_recommended_plan_ran():
    # Live means the regressed plan still runs. If only the last-good plan ran,
    # the query already uses it and forcing it changes nothing.
    activity = [
        {"plan_id": 103, "query_id": 42, "is_forced_plan": False,
         "last_seen_utc": None, "recent_execution_count": None},
        {"plan_id": 101, "query_id": 42, "is_forced_plan": False,
         "last_seen_utc": "2026-10-01T11:00:00", "recent_execution_count": 30},
    ]
    service = QueryRegressionService(FakeExecutor([[_dmv_row(_FORCE_DETAILS_42)], activity]))
    result = await service.detect_regressed_queries("testdb")

    rec = result["recommendations"][0]
    assert rec["live"] is False
    assert rec["regressed_plan_recent_execution_count"] == 0
    assert rec["recommended_plan_recent_execution_count"] == 30


@pytest.mark.asyncio
async def test_plan_activity_sql_keeps_plans_that_did_not_run_in_the_window():
    """An idle plan must still return its row. Without it, the last-good plan of a
    live regression (only the regressed plan ran) reads as 'not in Query Store'
    and the force is never ranked. So the window filter sits inside the outer
    join, never in WHERE or an inner join that drops plan rows."""
    executor = FakeExecutor([[_dmv_row(_FORCE_DETAILS_42)], []])
    await QueryRegressionService(executor).detect_regressed_queries("testdb", window_minutes=60)

    sql = " ".join(executor.queries[1].split())
    params = executor.params[1]
    assert params is not None
    assert sql.count("?") == len(params)  # every bound value has its placeholder
    from_plans, _, joined = sql.partition("FROM sys.query_store_plan AS p ")
    assert "JOIN" not in from_plans
    assert joined.startswith("LEFT JOIN (")
    join_group, _, rest = joined.partition(") ON rs.plan_id = p.plan_id")
    assert "rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())" in join_group
    where_clause = rest.partition("GROUP BY")[0]
    assert "WHERE p.plan_id IN (" in where_clause
    assert "rs." not in where_clause and "rsi." not in where_clause


@pytest.mark.asyncio
async def test_detect_regressed_queries_stays_under_the_parameter_limit():
    # SQL Server rejects a request with more than 2100 parameters. 1100
    # recommendations name 2200 plan ids; every id is still read.
    rows = [
        _dmv_row(
            json.dumps(
                {"planForceDetails": {
                    "queryId": n, "regressedPlanId": 10_000 + n, "recommendedPlanId": 20_000 + n,
                }}
            )
        )
        for n in range(1, 1101)
    ]
    executor = FakeExecutor([rows])
    result = await QueryRegressionService(executor).detect_regressed_queries("testdb")

    assert result["recommendation_count"] == 1100
    activity_params = executor.params[1:]
    assert len(activity_params) > 1
    assert all(p is not None and len(p) <= 2100 for p in activity_params)
    read_ids = {plan_id for p in activity_params if p for plan_id in p[1:]}
    assert read_ids == {10_000 + n for n in range(1, 1101)} | {20_000 + n for n in range(1, 1101)}


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
    # Executions of ANY plan of the query in the window: with the forced plan's
    # own count, this shows whether forcing is failing now (query runs, forced
    # plan does not) rather than only failed at some point in the past.
    assert "query_recent_execution_count" in executor.queries[0]
    assert "p2.query_id" in executor.queries[0]
    assert executor.params == [[120, 120]]


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
