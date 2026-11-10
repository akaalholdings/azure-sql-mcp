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


# --- Query Store SQL run on SQLite -------------------------------------------
# The service's own SQL text runs here; only the T-SQL dialect is translated
# (TOP, NVARCHAR(MAX), DATEADD on the UTC clock). Joins, GROUP BY, windowing and
# aggregates are the exact text, so a grain or ranking bug shows as a wrong row.

_QS_SCHEMA = """
CREATE TABLE sys.query_store_query_text (query_text_id INT, query_sql_text TEXT);
CREATE TABLE sys.query_store_query (query_id INT, query_text_id INT,
    is_internal_query INT DEFAULT 0, query_hash TEXT, object_id INT,
    query_parameterization_type_desc TEXT);
CREATE TABLE sys.query_store_plan (plan_id INT, query_id INT, query_plan TEXT,
    is_forced_plan INT, force_failure_count INT, query_plan_hash TEXT,
    plan_type_desc TEXT, last_force_failure_reason_desc TEXT);
CREATE TABLE sys.query_store_runtime_stats_interval (runtime_stats_interval_id INT,
    start_time TEXT, end_time TEXT);
CREATE TABLE sys.query_store_runtime_stats (plan_id INT, runtime_stats_interval_id INT,
    execution_type INT, count_executions INT, avg_duration REAL, avg_cpu_time REAL,
    avg_logical_io_reads REAL, avg_physical_io_reads REAL,
    avg_query_max_used_memory REAL, avg_rowcount REAL,
    first_execution_time TEXT, last_execution_time TEXT);
CREATE TABLE sys.query_store_wait_stats (plan_id INT, runtime_stats_interval_id INT,
    execution_type INT, wait_category_desc TEXT, total_query_wait_time_ms REAL,
    avg_query_wait_time_ms REAL, max_query_wait_time_ms REAL);
"""

_SHOWPLAN = (
    "<ShowPlanXML xmlns='http://schemas.microsoft.com/sqlserver/2004/07/showplan'>"
    "<BatchSequence><Batch><Statements><StmtSimple><QueryPlan>"
    "<RelOp PhysicalOp='{op}'/></QueryPlan></StmtSimple></Statements></Batch>"
    "</BatchSequence></ShowPlanXML>"
)


def _qs_ts(value) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _translate_tsql(query: str, params: list) -> tuple[str, list]:
    import re

    params = list(params)
    q = re.sub(r"(?i)n?varchar\s*\(\s*max\s*\)", "TEXT", query)
    q = q.replace("TODATETIMEOFFSET(CAST(? AS datetime2), 0)", "replace(?, 'T', ' ')")
    q = q.replace("CONVERT(BINARY(8), ?, 1)", "?")
    q = q.replace(" COLLATE Latin1_General_CI_AS", "").replace("N'", "'")
    q = re.sub(r"CONVERT\(varchar\(18\), ([\w.]+), 1\)", r"\1", q)
    q = re.sub(r"'%' \+ (.+?) \+ '%'", r"'%' || \1 || '%'", q)
    q = re.sub(
        r"(?i)DATEADD\(MINUTE,\s*-\?,\s*(SYSUTCDATETIME|GETUTCDATE)\(\)\)",
        "datetime('now', '-' || ? || ' minutes')",
        q,
    )
    q = re.sub(
        r"(?i)DATEADD\(MINUTE,\s*-(\d+),\s*(SYSUTCDATETIME|GETUTCDATE)\(\)\)",
        r"datetime('now', '-\1 minutes')",
        q,
    )
    match = re.search(r"(?i)SELECT\s+TOP\s*\((\?|\d+)\)", q)
    if match:
        if match.group(1) == "?":
            limit = params.pop(q[: match.start()].count("?"))
        else:
            limit = int(match.group(1))
        q = q[: match.start()] + "SELECT " + q[match.end() :]
        q = q.rstrip().rstrip(";") + f"\nLIMIT {int(limit)}"
    return q, params


class QueryStoreSqlite:
    def __init__(self) -> None:
        import sqlite3

        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("ATTACH DATABASE ':memory:' AS sys")
        self.conn.executescript(_QS_SCHEMA)
        self.conn.row_factory = sqlite3.Row
        self.conn.create_function("LEFT", 2, lambda text, n: None if text is None else text[:n])
        self.conn.create_function("OBJECT_NAME", 1, lambda object_id: None)
        self.conn.create_function("OBJECT_SCHEMA_NAME", 1, lambda object_id: None)
        self.calls: list[tuple[str, list]] = []

    def insert(self, table: str, **values) -> None:
        columns = ", ".join(values)
        marks = ", ".join("?" for _ in values)
        self.conn.execute(
            f"INSERT INTO sys.{table} ({columns}) VALUES ({marks})", list(values.values())
        )

    def add_interval(self, interval_id: int, start, minutes: int = 60) -> None:
        from datetime import timedelta

        self.insert(
            "query_store_runtime_stats_interval",
            runtime_stats_interval_id=interval_id,
            start_time=_qs_ts(start),
            end_time=_qs_ts(start + timedelta(minutes=minutes)),
        )

    def add_query(self, query_id: int, plans: dict[int, str], *, is_internal: bool = False) -> None:
        self.insert("query_store_query_text", query_text_id=query_id, query_sql_text=f"q{query_id}")
        self.insert(
            "query_store_query",
            query_id=query_id,
            query_text_id=query_id,
            is_internal_query=int(is_internal),
            query_hash=f"0x{query_id:016X}",
        )
        for plan_id, op in plans.items():
            self.insert(
                "query_store_plan",
                plan_id=plan_id,
                query_id=query_id,
                query_plan=_SHOWPLAN.format(op=op),
                is_forced_plan=0,
                force_failure_count=0,
            )

    def add_runtime(
        self, plan_id: int, interval_id: int, count: int, avg_duration_us: float, execution_type: int = 0
    ) -> None:
        self.insert(
            "query_store_runtime_stats",
            plan_id=plan_id,
            runtime_stats_interval_id=interval_id,
            execution_type=execution_type,
            count_executions=count,
            avg_duration=avg_duration_us,
            avg_cpu_time=avg_duration_us,
            avg_logical_io_reads=10.0,
            avg_physical_io_reads=0.0,
            first_execution_time="2026-11-01 00:00:00",
            last_execution_time="2026-11-03 00:00:00",
        )

    async def fetch_all(self, database_name: str, query: str, params: list | None = None) -> list[dict]:
        sql, bound = _translate_tsql(query, params or [])
        self.calls.append((query, list(params or [])))
        return [dict(row) for row in self.conn.execute(sql, bound).fetchall()]


def _hourly_intervals(store: QueryStoreSqlite, hours: int):
    from datetime import UTC, datetime, timedelta

    top = datetime.now(UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    for index in range(hours):
        store.add_interval(index + 1, top - timedelta(hours=hours - 1 - index))
    return top


@pytest.mark.asyncio
async def test_parameter_sniffing_names_the_fast_plan_as_best() -> None:
    # An agent forces best_plan_id. Plan 10 is the slow plan (500 ms) and has the
    # lower id; naming it "best" would force the slow plan in production.
    store = QueryStoreSqlite()
    _hourly_intervals(store, 3)
    store.add_query(1, {10: "Clustered Index Scan", 20: "Index Seek"})
    for interval_id in (1, 2, 3):
        store.add_runtime(10, interval_id, 50, 500_000)
        store.add_runtime(20, interval_id, 400, 2_000)

    result = await QueryRegressionService(store).detect_parameter_sniffing("db", 10.0, 1440, 20)

    [row] = result["queries"]
    assert (row["best_plan_id"], row["worst_plan_id"]) == (20, 10)
    assert row["best_avg_duration_ms"] == pytest.approx(2.0)
    assert row["worst_avg_duration_ms"] == pytest.approx(500.0)
    assert row["duration_variance_ratio"] == pytest.approx(250.0)
    assert row["plan_count"] == 2
    assert row["total_executions"] == 1350
    assert row["query_sql_text"] == "q1"


@pytest.mark.asyncio
async def test_parameter_sniffing_ignores_timeouts_and_single_bad_intervals() -> None:
    # Two plans of equal speed are not a sniffing case. One Aborted (timeout) row
    # or one slow hour of one plan must not create a 6000x "variance".
    store = QueryStoreSqlite()
    _hourly_intervals(store, 3)
    store.add_query(2, {30: "Index Seek", 31: "Index Seek"})
    for interval_id in (1, 2, 3):
        store.add_runtime(30, interval_id, 1000, 5_000)
        store.add_runtime(31, interval_id, 1000, 5_000)
    store.add_runtime(31, 2, 1, 30_000_000, execution_type=3)  # Aborted
    store.add_runtime(30, 3, 1, 600_000)  # one slow execution among 3001

    result = await QueryRegressionService(store).detect_parameter_sniffing("db", 10.0, 1440, 20)

    assert result["queries"] == []


@pytest.mark.asyncio
async def test_parameter_sniffing_binds_its_inputs() -> None:
    store = QueryStoreSqlite()

    await QueryRegressionService(store).detect_parameter_sniffing("db", 10.0, 90, 5)

    query, params = store.calls[0]
    assert "rsi.end_time >= DATEADD(MINUTE, -?, SYSUTCDATETIME())" in query
    assert sorted(params, key=float) == [5, 10.0, 90]


def _two_plans_over_48_hours() -> QueryStoreSqlite:
    # Plan 5: 3 ms over 48 hourly intervals plus one Aborted 4 s row.
    # Plan 6: about 400 ms over the same intervals.
    store = QueryStoreSqlite()
    _hourly_intervals(store, 48)
    store.add_query(1, {5: "Index Seek", 6: "Clustered Index Scan"})
    for interval_id in range(1, 49):
        store.add_runtime(5, interval_id, 200, 3_000)
        store.add_runtime(6, interval_id, 50, 400_000)
    store.add_runtime(5, 10, 1, 4_000_000, execution_type=3)
    return store


@pytest.mark.asyncio
async def test_compare_query_plans_with_explicit_ids_compares_plans_not_intervals() -> None:
    # Each plan ran in 48 intervals. The answer is one row per plan with a ratio,
    # not 97 interval rows (each with its plan XML) and an empty comparison.
    store = _two_plans_over_48_hours()

    result = await QueryRegressionService(store).compare_query_plans("db", 1, 5, 6)

    assert [plan["plan_id"] for plan in result["plans"]] == [5, 6]
    fast, slow = result["plans"]
    assert fast["avg_duration_ms"] == pytest.approx(3.0)
    assert fast["count_executions"] == 9600
    assert fast["aborted_executions"] == 1
    assert slow["avg_duration_ms"] == pytest.approx(400.0)
    assert fast["top_operators"] == ["Index Seek"]
    assert result["comparison"]["duration_ratio"] == pytest.approx(133.33)


@pytest.mark.asyncio
async def test_compare_query_plans_default_picks_two_different_plans() -> None:
    # best and worst must be plans. Ranking interval rows compared plan 5 with its
    # own timeout row (a false 1333x regression) and hid the slow plan 6.
    store = _two_plans_over_48_hours()

    result = await QueryRegressionService(store).compare_query_plans("db", 1)

    assert [plan["plan_id"] for plan in result["plans"]] == [5, 6]
    assert result["comparison"]["duration_ratio"] == pytest.approx(133.33)


@pytest.mark.asyncio
async def test_compare_query_plans_never_names_a_plan_that_only_timed_out_as_best() -> None:
    # Plan 7 has only Aborted runs, so it has no Regular average. It must not be
    # the "best" plan; asked for by id, it still shows its aborted count.
    store = _two_plans_over_48_hours()
    store.insert("query_store_plan", plan_id=7, query_id=1, query_plan="", is_forced_plan=0, force_failure_count=0)
    store.add_runtime(7, 5, 3, 30_000_000, execution_type=3)
    service = QueryRegressionService(store)

    default = await service.compare_query_plans("db", 1)
    explicit = await service.compare_query_plans("db", 1, 5, 7)

    # Plan 7 is never "best"; it is the worst plan, because it always times out.
    assert [plan["plan_id"] for plan in default["plans"]] == [5, 7]
    timed_out = next(plan for plan in explicit["plans"] if plan["plan_id"] == 7)
    assert timed_out["avg_duration_ms"] is None
    assert timed_out["aborted_executions"] == 3
    # No Regular average means no ratio. Substituting 1 ms reported the plan
    # that always times out as about 3x faster than plan 5.
    comparison = explicit["comparison"]
    assert (comparison["duration_ratio"], comparison["cpu_ratio"], comparison["io_ratio"]) == (None, None, None)
    assert comparison["plans_without_regular_runs"] == [7]


@pytest.mark.asyncio
async def test_compare_query_plans_default_never_names_a_plan_that_mostly_times_out_as_best() -> None:
    # Plan 8 timed out 1000 times and finished twice at 1 ms. Its Regular-only
    # average is the lowest, but the default comparison must show it as the
    # worst plan, not the best: the documented default is "best vs worst".
    store = _two_plans_over_48_hours()
    store.insert("query_store_plan", plan_id=8, query_id=1, query_plan="", is_forced_plan=0, force_failure_count=0)
    store.add_runtime(8, 3, 1000, 30_000_000, execution_type=3)
    store.add_runtime(8, 4, 2, 1_000)

    result = await QueryRegressionService(store).compare_query_plans("db", 1)

    best, worst = result["plans"]
    assert (best["plan_id"], worst["plan_id"]) == (5, 8)
    assert (bool(best["material_failures"]), bool(worst["material_failures"])) == (False, True)
    # A Regular-only average of a failing plan omits its timeouts: no ratio.
    assert result["comparison"]["duration_ratio"] is None


@pytest.mark.asyncio
async def test_parameter_sniffing_never_names_a_plan_that_mostly_times_out_as_best() -> None:
    # Plan 10 timed out 1000 times (30 s) and finished twice at 1 ms; plan 20
    # runs at 50 ms. Averaging Regular runs only made plan 10 "best", and an
    # agent that forces best_plan_id would force the plan that times out.
    store = QueryStoreSqlite()
    _hourly_intervals(store, 3)
    store.add_query(1, {10: "Nested Loops", 20: "Hash Match"})
    store.add_runtime(10, 1, 1000, 30_000_000, execution_type=3)  # Aborted
    store.add_runtime(10, 2, 2, 1_000)
    store.add_runtime(20, 2, 1000, 50_000)

    result = await QueryRegressionService(store).detect_parameter_sniffing("db", 10.0, 1440, 20)

    [row] = result["queries"]
    assert (row["best_plan_id"], row["worst_plan_id"]) == (20, 10)
    assert row["best_avg_duration_ms"] == pytest.approx(50.0)
    # Its 1 ms Regular average omits 1000 timeouts, so it gives no ratio.
    assert row["duration_variance_ratio"] is None
    assert row["worst_plan_aborted_executions"] == 1000
    assert row["worst_plan_material_failures"] is True
    assert row["best_plan_aborted_executions"] == 0
    assert row["best_plan_material_failures"] is False


@pytest.mark.asyncio
async def test_parameter_sniffing_flags_a_plan_that_only_times_out() -> None:
    # Plan 11 has only Aborted runs, so it has no Regular average. It is the
    # worst plan of the query; dropping it hid the sniffing case completely.
    store = QueryStoreSqlite()
    _hourly_intervals(store, 3)
    store.add_query(2, {11: "Nested Loops", 21: "Hash Match"})
    store.add_runtime(11, 1, 500, 30_000_000, execution_type=3)
    store.add_runtime(11, 2, 4, 30_000_000, execution_type=4)  # Exception
    store.add_runtime(21, 2, 1000, 5_000)

    result = await QueryRegressionService(store).detect_parameter_sniffing("db", 10.0, 1440, 20)

    [row] = result["queries"]
    assert (row["best_plan_id"], row["worst_plan_id"]) == (21, 11)
    assert row["best_avg_duration_ms"] == pytest.approx(5.0)
    assert row["worst_avg_duration_ms"] is None
    assert row["duration_variance_ratio"] is None
    assert row["worst_plan_aborted_executions"] == 500
    assert row["worst_plan_exception_executions"] == 4
    assert row["worst_plan_material_failures"] is True


@pytest.mark.asyncio
async def test_parameter_sniffing_does_not_report_a_query_whose_plans_all_time_out() -> None:
    # No plan is safe to force, so there is no best plan to name.
    store = QueryStoreSqlite()
    _hourly_intervals(store, 3)
    store.add_query(3, {12: "Nested Loops", 22: "Hash Match"})
    store.add_runtime(12, 1, 500, 30_000_000, execution_type=3)
    store.add_runtime(22, 1, 100, 1_000)
    store.add_runtime(22, 2, 400, 30_000_000, execution_type=3)

    result = await QueryRegressionService(store).detect_parameter_sniffing("db", 10.0, 1440, 20)

    assert result["queries"] == []


@pytest.mark.asyncio
async def test_compare_query_plans_keeps_the_callers_plan_order() -> None:
    # A caller who asks for plan 5 relative to plan 6 must get 5/6, not the
    # higher plan id over the lower one. The ratio direction must also be named.
    store = _two_plans_over_48_hours()
    service = QueryRegressionService(store)

    fast_vs_slow = await service.compare_query_plans("db", 1, plan_id_a=6, plan_id_b=5)
    slow_vs_fast = await service.compare_query_plans("db", 1, plan_id_a=5, plan_id_b=6)

    assert [plan["plan_id"] for plan in fast_vs_slow["plans"]] == [6, 5]
    assert fast_vs_slow["comparison"]["duration_ratio"] == pytest.approx(0.01)  # 3 ms / 400 ms
    assert fast_vs_slow["comparison"]["baseline_plan_id"] == 6
    assert fast_vs_slow["comparison"]["compared_plan_id"] == 5
    assert [plan["plan_id"] for plan in slow_vs_fast["plans"]] == [5, 6]
    assert slow_vs_fast["comparison"]["duration_ratio"] == pytest.approx(133.33)
    assert slow_vs_fast["comparison"]["baseline_plan_id"] == 5


@pytest.mark.asyncio
async def test_compare_query_plans_default_names_best_plan_as_baseline() -> None:
    store = _two_plans_over_48_hours()

    result = await QueryRegressionService(store).compare_query_plans("db", 1)

    assert result["comparison"]["baseline_plan_id"] == 5
    assert result["comparison"]["compared_plan_id"] == 6


@pytest.mark.asyncio
@pytest.mark.parametrize("plan_ids", [(5, None), (None, 6)])
async def test_compare_query_plans_refuses_only_one_plan_id(plan_ids) -> None:
    # Silently answering best vs worst drops the plan the caller asked about.
    store = _two_plans_over_48_hours()

    with pytest.raises(ValueError, match="both plan_id_a and plan_id_b"):
        await QueryRegressionService(store).compare_query_plans("db", 1, *plan_ids)
    assert store.calls == []
