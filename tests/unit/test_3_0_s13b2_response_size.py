"""Response size (friction B01, B03; proposal P8).

Every task read a 10.5 KB capability report for two blocks of it, and a 10 to
17 KB benchmark summary for the verdict, the indexes and the lease.
"""

from __future__ import annotations

import json
from typing import Any

from azure_sql_mcp.performance_workflows import aggregate_samples
from azure_sql_mcp.performance_workflows import classify_benchmark
from azure_sql_mcp.server import _capabilities_summary
from azure_sql_mcp.server import _compact_benchmark_payload

PROBE_NAMES = (
    "connectivity", "query_store", "estimated_plan", "actual_plan", "dmv_exec",
    "dmv_index", "resource_stats", "deadlock_session", "master_access", "showplan",
)


def _capabilities() -> dict[str, Any]:
    return {
        "result_status": "ok",
        "checks": {
            name: {"available": True, "detail": {"note": "x" * 300, "enabled": True, "state": "READ_WRITE"}}
            for name in PROBE_NAMES
        },
        "azure_sql_database": {
            "ok": True,
            "data": {
                "platform_supported": True,
                "engine_edition": 5,
                "service_objective": "GP_S_Gen5_4",
                "compatibility_level": 160,
                "database_scoped_configurations": [{"name": f"option_{n}", "value": n} for n in range(40)],
            },
        },
        "query_store_replicas": {"ok": True, "data": {"replicas": ["primary"]}},
        "mcp_contract": {"contract_version": "3.0.0", "performance_tuning": 1},
        "indexed_view_experiment": {"allowed": True, "blockers": [], "test_view_name_prefixes": ["v_Testing_"]},
        "local_tuning_policy": {"configured": True, "allow_test_indexes": True},
    }


def test_the_capability_summary_keeps_what_a_tuning_session_needs_under_2_kb() -> None:
    full = _capabilities()

    summary = _capabilities_summary(full)

    assert summary["mcp_contract"] == full["mcp_contract"]
    assert summary["local_tuning_policy"] == full["local_tuning_policy"]
    assert summary["indexed_view_experiment"] == full["indexed_view_experiment"]
    assert summary["result_status"] == "ok"
    assert summary["platform"] == {
        "platform_supported": True,
        "engine_edition": 5,
        "service_objective": "GP_S_Gen5_4",
        "compatibility_level": 160,
    }
    assert summary["query_store"] == {"available": True, "enabled": True, "state": "READ_WRITE"}
    # Each probe is one word; a failed probe keeps its reason.
    assert summary["checks"] == {name: "available" for name in PROBE_NAMES}
    assert len(json.dumps(summary)) < 2048 < len(json.dumps(full))
    assert 'detail="full"' in summary["detail_note"]


def test_a_failed_probe_keeps_its_reason_in_the_summary() -> None:
    full = _capabilities()
    full["checks"]["actual_plan"] = {"available": False, "error": "SHOWPLAN permission denied"}
    full["result_status"] = "unavailable"
    full["result_status_reason"] = "a probe failed"

    summary = _capabilities_summary(full)

    assert summary["checks"]["actual_plan"] == {
        "available": False, "error": "SHOWPLAN permission denied",
    }
    assert summary["result_status_reason"] == "a probe failed"


def _case(name: str) -> dict[str, Any]:
    def side(elapsed: float) -> dict[str, Any]:
        return aggregate_samples(
            [
                {
                    "elapsed_ms": elapsed, "cpu_ms": elapsed / 2, "logical_reads": 1000,
                    "elapsed_source": "showplan_query_time_stats",
                    "cpu_source": "showplan_query_time_stats",
                    "read_source": "statistics_io_table_messages",
                    "plan_fingerprint": "f" * 64,
                }
                for _ in range(5)
            ]
        )

    plan = {"query_plan_hashes": ["0x1"], "plan_fingerprint": "f" * 64, "top_operators": [
        {"node_id": n, "operator": "Index Seek", "object": "[dbo].[Posts].[IX]", "self_elapsed_ms": 1.0}
        for n in range(3)
    ]}
    return {
        "parameter_case": name,
        "weight": 1.0,
        "baseline": side(400),
        "candidate": side(40),
        "plan_delta": {"plan_changed": True, "baseline": plan, "candidate": plan},
        "baseline_after_plan_delta": {"plan_changed": False, "baseline": plan, "candidate": plan},
        "plan_use": [{"used": True, "index_name": "IX_Testing_Posts_OwnerUserId_0a1b2c3d"}] * 5,
        "runs": {"baseline_before": 5, "candidate": 5, "baseline_after": 5},
        "plan_compiled_for": name,
    }


def _benchmark_payload() -> dict[str, Any]:
    cases = [_case(name) for name in ("selective", "common", "all")]
    equivalence = [
        {"parameter_case": case["parameter_case"], "status": "match", "same_sql": True,
         "plan_used_expected_index": True, "proven_for_parameter_case": True,
         "basis": "unchanged SQL plus complete A-B-A result stability",
         "result_evidence": [{"fingerprint": "a" * 64, "complete": True}] * 15}
        for case in cases
    ]
    classification, reason = classify_benchmark(cases, equivalence)
    return {
        "session_id": "session-1", "candidate_id": "candidate-1", "evidence_id": "evidence-1",
        "classification": classification, "performance_classification": classification,
        "reason": reason, "objective": "elapsed_time", "phase": "finalist", "executions": 45,
        "durable_state": classification, "session_continues": True,
        "metrics": cases[0], "parameter_results": cases,
        "parameter_case_receipts": [{"name": "selective"}] * 3,
        "equivalence": equivalence,
        "equivalence_preflight": {"classification": "direct_snapshot", "direct_snapshot_supported": True,
                                  "risk_codes": [], "details": [{"x": "y" * 400}]},
        "proof_scope": "aba_result_stability", "lineage": None,
        "lease": {"lease_id": "lease-1", "status": "cleaned", "index_name": "IX_Testing_x",
                  "expires_at_utc": "2026-11-10T10:00:00+00:00", "metadata": {"members": ["m" * 500]}},
        "indexes": [{"index_name": "IX_Testing_Posts_OwnerUserId_0a1b2c3d", "built": True,
                     "size_mb": 20.0, "index_depth": 3, "used_in_cases": ["selective", "common", "all"]}],
        "index_ddl": "CREATE NONCLUSTERED INDEX [IX_Testing_Posts_OwnerUserId_0a1b2c3d] ON [dbo].[Posts] ([OwnerUserId]);",
        "rollback_ddl": "DROP INDEX [IX_Testing_Posts_OwnerUserId_0a1b2c3d] ON [dbo].[Posts];",
        "plan_reuse": "per_case", "plan_controls_not_applied": True,
    }


def test_the_benchmark_summary_for_three_cases_keeps_the_decision_and_drops_the_bulk() -> None:
    payload = _benchmark_payload()

    summary = _compact_benchmark_payload(payload)

    # Measured 5.4 KB against 33 KB in full. The medians are in decision.cases and in
    # parameter_results, which the tool headline reads; that keeps it over 3 KB.
    assert len(json.dumps(summary)) < 6144 < len(json.dumps(payload)) / 5
    decision = summary["decision"]
    assert decision["classification"] == "improved"
    assert [row["parameter_case"] for row in decision["cases"]] == ["selective", "common", "all"]
    assert decision["cases"][0]["speedup"] == 10.0
    assert decision["cases"][0]["baseline"]["elapsed_ms"] == 400
    assert summary["indexes"] == payload["indexes"]
    assert summary["lease"] == {
        "lease_id": "lease-1", "status": "cleaned", "index_name": "IX_Testing_x",
        "expires_at_utc": "2026-11-10T10:00:00+00:00",
    }
    assert summary["index_ddl"] == payload["index_ddl"]
    assert summary["rollback_ddl"] == payload["rollback_ddl"]
    assert summary["session_id"] == "session-1" and summary["evidence_id"] == "evidence-1"
    # Per case: what ran, the medians, whether the plan changed and what it does now.
    case = summary["parameter_results"][0]
    assert case["runs"] == {"baseline_before": 5, "candidate": 5, "baseline_after": 5}
    assert case["baseline"]["elapsed_ms"] == 400 and "spread" not in case["baseline"]
    assert case["plan_delta"] == {
        "plan_changed": True,
        "candidate_operators": ["Index Seek [dbo].[Posts].[IX]"] * 3,
    }
    assert case["baseline_after_plan_delta"] == {"plan_changed": False}
    assert case["plan_use"] == {"runs": 5, "runs_using_index": 5}
    assert "comparison_margin" not in case
    assert summary["equivalence"][0] == {
        "parameter_case": "selective", "status": "match", "same_sql": True,
        "plan_used_expected_index": True, "proven_for_parameter_case": True,
    }
    assert "lineage" not in summary and "metrics" not in summary
    assert 'detail="full"' in summary["detail_note"]


def test_a_failed_benchmark_summary_keeps_the_diagnostic_and_the_mismatch_reason() -> None:
    payload = _benchmark_payload()
    payload["failure_diagnostic"] = {"failed_step": "create_index", "cause_class": "engine_error"}
    payload["equivalence"][1]["status"] = "mismatch"
    payload["equivalence"][1]["reason"] = "result shape, types, duplicates, values, or required order differ"
    payload["unused_indexes"] = ["IX_Testing_y"]

    summary = _compact_benchmark_payload(payload)

    assert summary["failure_diagnostic"] == payload["failure_diagnostic"]
    assert summary["equivalence"][1]["status"] == "mismatch"
    assert "differ" in summary["equivalence"][1]["reason"]
    assert summary["unused_indexes"] == ["IX_Testing_y"]
