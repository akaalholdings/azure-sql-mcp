from __future__ import annotations

import pytest

from azure_sql_mcp.lock_diagnostics import LockDiagnosticsService


class FakeExecutor:
    def __init__(self, results: list[dict] | None = None):
        self.results = results or []

    async def fetch_all(self, database_name: str, query: str) -> list[dict]:
        return self.results


@pytest.mark.asyncio
async def test_get_lock_details_groups_by_type():
    rows = [
        {"resource_type": "KEY", "resource_subtype": "", "request_mode": "X", "request_status": "GRANT", "session_id": 55, "resource_description": "", "object_name": None, "login_name": "sa", "session_status": "running", "command": "UPDATE", "wait_type": None, "wait_time_ms": 0, "blocking_session_id": 0, "current_statement": "UPDATE t SET ..."},
        {"resource_type": "KEY", "resource_subtype": "", "request_mode": "S", "request_status": "WAIT", "session_id": 56, "resource_description": "", "object_name": None, "login_name": "app", "session_status": "suspended", "command": "SELECT", "wait_type": "LCK_M_S", "wait_time_ms": 5000, "blocking_session_id": 55, "current_statement": "SELECT * FROM t"},
        {"resource_type": "OBJECT", "resource_subtype": "", "request_mode": "IX", "request_status": "GRANT", "session_id": 55, "resource_description": "", "object_name": "Orders", "login_name": "sa", "session_status": "running", "command": "UPDATE", "wait_type": None, "wait_time_ms": 0, "blocking_session_id": 0, "current_statement": "UPDATE t SET ..."},
    ]
    service = LockDiagnosticsService(FakeExecutor(rows))
    result = await service.get_lock_details("testdb")

    assert result["total_locks"] == 3
    assert result["waiting_locks"] == 1
    assert result["locks_by_resource_type"]["KEY"] == 2
    assert result["locks_by_resource_type"]["OBJECT"] == 1


@pytest.mark.asyncio
async def test_get_lock_details_empty():
    service = LockDiagnosticsService(FakeExecutor([]))
    result = await service.get_lock_details("testdb")
    assert result["total_locks"] == 0
    assert result["waiting_locks"] == 0


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
        assert result["victim_session_id"] == "process1"
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
