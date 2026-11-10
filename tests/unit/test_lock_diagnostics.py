from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from azure_sql_mcp.lock_diagnostics import LockDiagnosticsService
from tests.azure_dmv_contract import StrictDmvExecutor

# The per-resource-type aggregate that gives the true lock totals.
LOCK_TOTALS_NEEDLE = "GROUP BY tl.resource_type"


class FakeExecutor:
    def __init__(self, results: list[dict] | None = None, lock_totals: list[dict] | None = None):
        self.results = results or []
        self.lock_totals = lock_totals or []
        self.calls: list[tuple[str, object]] = []

    async def fetch_all(self, database_name: str, query: str, params=None) -> list[dict]:
        self.calls.append((query, params))
        if LOCK_TOTALS_NEEDLE in query:
            return self.lock_totals
        self.last_params = params
        return self.results

    def lock_rows_query(self) -> str:
        (query,) = [query for query, _ in self.calls if LOCK_TOTALS_NEEDLE not in query]
        return query


@pytest.mark.asyncio
async def test_get_lock_details_groups_by_type():
    rows = [
        {"resource_type": "KEY", "resource_subtype": "", "request_mode": "X", "request_status": "GRANT", "session_id": 55, "resource_description": "", "object_name": None, "login_name": "sa", "session_status": "running", "command": "UPDATE", "wait_type": None, "wait_time_ms": 0, "blocking_session_id": 0, "current_statement": "UPDATE t SET ..."},
        {"resource_type": "KEY", "resource_subtype": "", "request_mode": "S", "request_status": "WAIT", "session_id": 56, "resource_description": "", "object_name": None, "login_name": "app", "session_status": "suspended", "command": "SELECT", "wait_type": "LCK_M_S", "wait_time_ms": 5000, "blocking_session_id": 55, "current_statement": "SELECT * FROM t"},
        {"resource_type": "OBJECT", "resource_subtype": "", "request_mode": "IX", "request_status": "GRANT", "session_id": 55, "resource_description": "", "object_name": "Orders", "login_name": "sa", "session_status": "running", "command": "UPDATE", "wait_type": None, "wait_time_ms": 0, "blocking_session_id": 0, "current_statement": "UPDATE t SET ..."},
    ]
    totals = [
        {"resource_type": "KEY", "lock_count": 2, "waiting_count": 1},
        {"resource_type": "OBJECT", "lock_count": 1, "waiting_count": 0},
    ]
    service = LockDiagnosticsService(FakeExecutor(rows, lock_totals=totals))
    result = await service.get_lock_details("testdb")

    assert result["total_locks"] == 3
    assert result["returned_locks"] == 3
    assert result["waiting_locks"] == 1
    assert result["locks_by_resource_type"]["KEY"] == 2
    assert result["locks_by_resource_type"]["OBJECT"] == 1


@pytest.mark.asyncio
async def test_get_lock_details_empty():
    service = LockDiagnosticsService(FakeExecutor([]))
    result = await service.get_lock_details("testdb")
    assert result["total_locks"] == 0
    assert result["returned_locks"] == 0
    assert result["waiting_locks"] == 0
    assert result["locks_by_resource_type"] == {}


@pytest.mark.asyncio
async def test_get_open_transactions_flags_long_running():
    rows = [
        {"transaction_id": 1, "transaction_name": "user_transaction", "transaction_type": "Read/Write", "transaction_state": "Active", "transaction_begin_time": "2026-04-01 10:00:00", "duration_seconds": 600, "session_id": 55, "login_name": "sa", "session_status": "running", "host_name": "app01", "program_name": "MyApp", "log_bytes_used": 1024000, "log_bytes_reserved": 2048000},
        {"transaction_id": 2, "transaction_name": "user_transaction", "transaction_type": "Read-Only", "transaction_state": "Active", "transaction_begin_time": "2026-04-01 10:05:00", "duration_seconds": 30, "session_id": 56, "login_name": "app", "session_status": "running", "host_name": "app02", "program_name": "MyApp", "log_bytes_used": 0, "log_bytes_reserved": 0},
        {"transaction_id": 3, "transaction_name": "user_transaction", "transaction_type": "Read/Write", "transaction_state": "Active", "transaction_begin_time": "2026-04-01 09:50:00", "duration_seconds": 120, "session_id": 57, "login_name": "batch", "session_status": "sleeping", "host_name": "app03", "program_name": "BatchJob", "log_bytes_used": 500, "log_bytes_reserved": 1000},
    ]
    service = LockDiagnosticsService(FakeExecutor(rows))
    result = await service.get_open_transactions("testdb")

    assert result["open_transaction_count"] == 3
    assert result["long_running_count"] == 1  # only txn with 600s > 300s
    assert result["idle_with_open_txn_count"] == 1  # session 57 sleeping with 120s > 60s

    warning_types = [w["type"] for w in result["warnings"]]
    assert "long_running" in warning_types
    assert "idle_with_open_txn" in warning_types


@pytest.mark.asyncio
async def test_get_open_transactions_no_warnings():
    rows = [
        {"transaction_id": 1, "transaction_name": "t", "transaction_type": "Read/Write", "transaction_state": "Active", "transaction_begin_time": "2026-04-01 10:09:00", "duration_seconds": 10, "session_id": 55, "login_name": "sa", "session_status": "running", "host_name": "h", "program_name": "p", "log_bytes_used": 0, "log_bytes_reserved": 0},
    ]
    service = LockDiagnosticsService(FakeExecutor(rows))
    result = await service.get_open_transactions("testdb")
    assert result["warnings"] == []


class TestParseDeadlockXml:
    def test_empty_string(self):
        result = LockDiagnosticsService._parse_deadlock_xml("")
        assert result["participants"] == []
        assert result["victim_session_id"] is None

    def test_invalid_xml(self):
        result = LockDiagnosticsService._parse_deadlock_xml("<not>valid")
        assert result["participants"] == []

    def test_valid_deadlock_xml(self):
        xml = """
        <deadlock>
            <victim-list>
                <victimProcess id="process1"/>
            </victim-list>
            <process-list>
                <process id="process1" spid="55" waitresource="KEY: 5:12345" lockMode="X">
                    <inputbuf>SELECT * FROM t1</inputbuf>
                </process>
                <process id="process2" spid="56" waitresource="KEY: 5:67890" lockMode="S">
                    <inputbuf>UPDATE t2 SET x=1</inputbuf>
                </process>
            </process-list>
            <resource-list>
                <keylock hobtid="12345" dbid="5" objectname="dbo.t1"/>
            </resource-list>
        </deadlock>
        """
        result = LockDiagnosticsService._parse_deadlock_xml(xml)
        # The victim is named by its session id (spid); the process id stays apart.
        assert result["victim_session_id"] == "55"
        assert result["victim_process_ids"] == ["process1"]
        assert len(result["participants"]) == 2
        assert result["participants"][0]["is_victim"] is True
        assert result["participants"][0]["session_id"] == "55"
        assert result["participants"][1]["is_victim"] is False
        assert len(result["resources"]) == 1


@pytest.mark.asyncio
async def test_get_deadlock_history():
    service = LockDiagnosticsService(FakeExecutor([]))
    result = await service.get_deadlock_history("testdb", max_events=5)
    assert result["deadlock_count"] == 0
    assert result["deadlocks"] == []
    # No capture session exists, so zero deadlocks is not a clean bill of health.
    assert result["result_status"] == "precondition"


@pytest.mark.asyncio
async def test_get_lock_details_truncates_above_limit():
    """dm_tran_locks can hold hundreds of thousands of rows; the tool must
    bound what it returns and say so instead of flooding the context."""
    rows = [
        {"resource_type": "KEY", "request_status": "GRANT", "session_id": i}
        for i in range(3)
    ]
    # A blocking storm: the list is cut to the limit, but the totals must still
    # say 300,000 locks. A total equal to the limit reads as a quiet database.
    totals = [
        {"resource_type": "KEY", "lock_count": 299_950, "waiting_count": 40},
        {"resource_type": "OBJECT", "lock_count": 50, "waiting_count": 2},
    ]
    service = LockDiagnosticsService(FakeExecutor(rows, lock_totals=totals))
    result = await service.get_lock_details("testdb", limit=2)

    assert result["truncated"] is True
    assert result["limit"] == 2
    assert len(result["locks"]) == 2
    assert result["returned_locks"] == 2
    assert result["total_locks"] == 300_000
    assert result["waiting_locks"] == 42
    assert result["locks_by_resource_type"] == {"KEY": 299_950, "OBJECT": 50}


@pytest.mark.asyncio
async def test_lock_totals_come_from_one_aggregate_over_user_sessions():
    executor = FakeExecutor([])
    await LockDiagnosticsService(executor).get_lock_details("testdb")

    (totals_query,) = [query for query, _ in executor.calls if LOCK_TOTALS_NEEDLE in query]
    assert "COUNT_BIG(*) AS lock_count" in totals_query
    assert "AS waiting_count" in totals_query
    assert "tl.resource_database_id = DB_ID()" in totals_query
    assert "is_user_process = 1" in totals_query
    # The totals query must stay cheap: no SQL text, no per-row functions.
    assert "dm_exec_sql_text" not in totals_query
    assert "OBJECT_NAME" not in totals_query


def _lock_rows_statement(executor: FakeExecutor):
    return sqlglot.parse_one(executor.lock_rows_query(), read="tsql")


@pytest.mark.asyncio
async def test_lock_rows_are_cut_to_the_limit_before_the_sql_text_lookup():
    # dm_exec_sql_text ran once per lock row before TOP: up to 300,000 calls to
    # return 200 rows, during the incident the tool exists for.
    executor = FakeExecutor([])
    await LockDiagnosticsService(executor).get_lock_details("testdb", limit=200)

    statement = _lock_rows_statement(executor)
    (cte,) = statement.find_all(exp.CTE)
    limited = cte.this
    assert limited.args.get("limit") is not None, "TOP must sit inside the CTE"
    assert limited.find(exp.Lateral) is None
    assert "dm_exec_sql_text" not in limited.sql(dialect="tsql")
    assert "dm_exec_requests" not in limited.sql(dialect="tsql")
    # Waiting locks first, inside the CTE: the cut must keep the waiters.
    assert "WAIT" in limited.args["order"].sql(dialect="tsql")
    assert statement.args.get("limit") is None
    assert "dm_exec_sql_text" in statement.sql(dialect="tsql")
    assert executor.last_params == [201]


@pytest.mark.asyncio
async def test_lock_row_cut_keeps_the_user_process_filter():
    # Cutting to TOP before the session filter would fill the list with system
    # session locks and drop user locks that the old query returned.
    executor = FakeExecutor([])
    await LockDiagnosticsService(executor).get_lock_details("testdb")

    (cte,) = _lock_rows_statement(executor).find_all(exp.CTE)
    (exists,) = cte.this.find_all(exp.Exists)
    exists_sql = exists.sql(dialect="tsql")
    assert "sys.dm_exec_sessions" in exists_sql
    assert "is_user_process = 1" in exists_sql
    assert "request_session_id" in exists_sql
    assert "resource_database_id = DB_ID()" in cte.this.sql(dialect="tsql")


@pytest.mark.asyncio
async def test_object_name_is_resolved_for_row_and_page_locks():
    # KEY, PAGE, RID and HOBT locks carry a hobt_id, not an object_id. They are
    # the usual blocking resources, and without the name the agent cannot say
    # which table is blocked.
    executor = FakeExecutor([])
    await LockDiagnosticsService(executor).get_lock_details("testdb")

    query = " ".join(executor.lock_rows_query().split())
    assert (
        "LEFT JOIN sys.partitions AS p ON p.hobt_id = l.resource_associated_entity_id "
        "AND l.resource_type IN ('KEY', 'PAGE', 'RID', 'HOBT')"
    ) in query
    assert "WHEN l.resource_type = 'OBJECT' THEN OBJECT_NAME(l.resource_associated_entity_id)" in query
    assert (
        "WHEN l.resource_type IN ('KEY', 'PAGE', 'RID', 'HOBT') THEN OBJECT_NAME(p.object_id)"
    ) in query


@pytest.mark.asyncio
async def test_lock_queries_are_valid_tsql_on_documented_columns():
    executor = StrictDmvExecutor()

    await LockDiagnosticsService(executor).get_lock_details("testdb")  # type: ignore[arg-type]

    assert executor.violations == []
    assert len(executor.calls) == 2


@pytest.mark.asyncio
async def test_get_open_transactions_truncates_above_limit():
    rows = [
        {"transaction_id": i, "duration_seconds": 1, "session_status": "running", "session_id": i}
        for i in range(4)
    ]
    service = LockDiagnosticsService(FakeExecutor(rows))
    result = await service.get_open_transactions("testdb", limit=3)

    assert result["truncated"] is True
    assert len(result["transactions"]) == 3


@pytest.mark.asyncio
async def test_lock_limit_is_clamped_to_max():
    service = LockDiagnosticsService(FakeExecutor([]))
    result = await service.get_lock_details("testdb", limit=999999)
    assert result["limit"] == 1000
    assert service.executor.last_params == [1001]
