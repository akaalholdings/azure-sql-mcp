"""The evidence response returns each section and the profile once (finding 70).

The response carried the sections and the profile beside the envelope and again
inside its metadata, so the agent paid for every lock row, statistics row and
plan summary twice.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

from tests.unit.test_performance_workflows import _service

SQL = "SELECT id FROM dbo.Items"
ROW_MARKER = "lock-row-marker"
# The fake plan service names the baseline query's only operator "baseline".
PLAN_MARKER = '"physical_op": "baseline"'
OPERATORS = [{"physical_op": "baseline"}]


def _locks_collector() -> AsyncMock:
    return AsyncMock(
        return_value={
            "status": "ok",
            "rows": [{"resource": f"{ROW_MARKER}-{index}"} for index in range(40)],
        }
    )


async def _collect(service: Any, case_id: str, collector: AsyncMock) -> dict[str, Any]:
    return await service.collect_case_evidence(
        case_id,
        "appdb",
        SQL,
        {"locks": collector},
        window_minutes=15,
        execute_query=True,
        idempotency_key="evidence-once",
    )


@pytest.mark.asyncio
async def test_evidence_response_returns_sections_and_profile_once() -> None:
    service, _store, _plans, _executor = _service()
    case = service.start_case("appdb", SQL)

    result = await _collect(service, case.case_id, _locks_collector())

    text = json.dumps(result, default=str)
    assert text.count(f"{ROW_MARKER}-7") == 1
    assert text.count(PLAN_MARKER) == 1
    # The response keeps the data, beside the envelope.
    assert len(result["sections"]["locks"]["data"]["rows"]) == 40
    assert result["profile"]["plan"]["summary"]["top_operators"] == OPERATORS
    # The envelope keeps its identity and its small metadata.
    metadata = result["evidence"]["metadata"]
    assert "sections" not in metadata
    assert "profile_summary" not in metadata
    assert metadata["outcome"] == result["outcome"]
    assert metadata["collection_window_minutes"] == 15
    assert result["evidence"]["evidence_id"].startswith("evidence")
    assert result["evidence"]["metrics"]["total_sections"] == 1


@pytest.mark.asyncio
async def test_stored_evidence_envelope_stays_complete() -> None:
    service, store, _plans, _executor = _service()
    case = service.start_case("appdb", SQL)

    result = await _collect(service, case.case_id, _locks_collector())

    # get_performance_case is where the full record is read back.
    stored = service.get_case(case.case_id)["evidence"]
    assert [envelope["evidence_id"] for envelope in stored] == [result["evidence"]["evidence_id"]]
    metadata = stored[0]["metadata"]
    assert metadata["sections"] == result["sections"]
    assert metadata["profile_summary"]["plan"]["summary"]["top_operators"] == OPERATORS
    assert store.get_evidence(result["evidence"]["evidence_id"]).to_dict() == stored[0]


@pytest.mark.asyncio
async def test_recovered_evidence_response_returns_sections_and_profile_once() -> None:
    service, _store, _plans, _executor = _service()
    case = service.start_case("appdb", SQL)
    collector = _locks_collector()

    first = await _collect(service, case.case_id, collector)
    replay = await _collect(service, case.case_id, collector)

    assert replay["recovered_from_durable_evidence"] is True
    assert collector.await_count == 1
    text = json.dumps(replay, default=str)
    assert text.count(f"{ROW_MARKER}-7") == 1
    assert text.count(PLAN_MARKER) == 1
    assert "sections" not in replay["evidence"]["metadata"]
    assert "profile_summary" not in replay["evidence"]["metadata"]
    # A replay answers with the same envelope and the same data as the first call.
    assert replay["evidence"] == first["evidence"]
    assert replay["sections"] == first["sections"]
    stored = service.get_case(case.case_id)["evidence"][0]["metadata"]
    assert replay["profile"] == stored["profile_summary"]
    assert replay["profile"]["plan"]["summary"]["top_operators"] == OPERATORS
