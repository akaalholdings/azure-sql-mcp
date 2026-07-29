from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.config import ServerConfig
from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.config import TransportConfig
from azure_sql_mcp.config import TransportMode
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.learning_cli import main as learning_cli
from azure_sql_mcp.performance_contracts import EvidenceEnvelopeV1
from azure_sql_mcp.server import AzureSqlMcpApplication


def _config(tmp_path: Path) -> ServerConfig:
    return ServerConfig(
        server="server.database.windows.net",
        default_database="appdb",
        allowed_databases=("appdb",),
        auth_mode=AuthMode.ENTRA_DEFAULT,
        access_mode=AccessMode.RESTRICTED,
        query_timeout_seconds=30,
        row_limit=100,
        pool_size=5,
        max_retries=3,
        tool_timeout_seconds=45,
        log_format="text",
        username=None,
        password=None,
        trust_server_certificate=False,
        tenant_id=None,
        client_id=None,
        client_secret=None,
        transport=TransportConfig(
            mode=TransportMode.STDIO,
            host="127.0.0.1",
            port=8000,
        ),
        tool_groups=frozenset({ToolGroup.ALL}),
        log_level="WARNING",
        mcp_bearer_token=None,
        write_policy=WritePolicy.DISABLED,
        audit_dir=str(tmp_path / "audit"),
        audit_full_sql=False,
        remote_admin_enabled=False,
        performance_state_dir=str(tmp_path / "state"),
    )


def _close(app: AzureSqlMcpApplication) -> None:
    app.performance_store.close()
    assert app.learning_store is not None
    app.learning_store.close()


def _evidence(
    app: AzureSqlMcpApplication,
    *,
    database_fingerprint: str,
    suffix: str,
) -> EvidenceEnvelopeV1:
    return app.performance_store.create_evidence(
        EvidenceEnvelopeV1(
            database_fingerprint=database_fingerprint,
            query_fingerprint=f"query-{suffix}",
            observed_execution_count=1,
            metrics={"classification": "bounded-observation"},
        )
    )


async def _record_optimizer_decision(
    app: AzureSqlMcpApplication,
    *,
    runtime: dict,
    evidence: EvidenceEnvelopeV1,
    session_id: str,
    subject_fingerprint: str,
    based_on_review_ids: list[str],
    idempotency_key: str,
    applied_lesson_ids: list[str] | None = None,
) -> dict:
    return await app.mcp._tool_manager.call_tool(
        "record_decision",
        {
            "skill": "sql-optimizer",
            "skill_version": "2.3.0",
            "session_id": session_id,
            "learning_key": "bounded-candidate-selection",
            "consumed_evidence_refs": [evidence.evidence_id],
            "subject_kind": "query",
            "subject_fingerprint": subject_fingerprint,
            "query_fingerprint": evidence.query_fingerprint,
            "based_on_review_ids": based_on_review_ids,
            "tactic": "bounded-candidate",
            "expected_result": {"classification": "improved"},
            "confidence": 0.8,
            "uncertainty": {"classification": "bounded"},
            "applied_lesson_ids": applied_lesson_ids or [],
            "evaluator_fingerprint": "acceptance-evaluator",
            "runtime_fingerprint": runtime["runtime_fingerprint"],
            "runtime_compatibility_fingerprint": runtime[
                "runtime_compatibility_fingerprint"
            ],
            "tool_schema_fingerprint": runtime["tool_schema_fingerprint"],
            "sanitized_config_fingerprint": runtime[
                "sanitized_config_fingerprint"
            ],
            "idempotency_key": idempotency_key,
            "database_name": "appdb",
        },
    )


async def _terminal_and_review(
    app: AzureSqlMcpApplication,
    *,
    decision: dict,
    evidence: EvidenceEnvelopeV1,
    review_key: str,
    correction: bool = False,
) -> tuple[dict, dict]:
    terminal = await app._run_tool(
        "benchmark_tuning_candidate",
        "appdb",
        AsyncMock(
            return_value={
                "status": "complete",
                "classification": "improved",
                "evidence_id": evidence.evidence_id,
            }
        ),
        decision_id=decision["decision_id"],
    )
    assert terminal["learning_link_status"] == "linked"
    assert terminal["terminal_link_id"].startswith("terminal-link-")
    review = await app.mcp._tool_manager.call_tool(
        "review_decision",
        {
            "decision_id": decision["decision_id"],
            "terminal_evidence_refs": [terminal["terminal_link_id"]],
            "observed_result": {"classification": "improved"},
            "prediction_error": {"classification": "none"},
            "counterexamples": [{"classification": "bounded-noise"}],
            "next_observation": {"classification": "repeat-bounded-run"},
            "causal_strength": "strong",
            "alignment": "aligned",
            "safety_signal": "passed",
            "equivalence_signal": "passed",
            "cleanup_signal": "passed",
            "material_regression_signal": "passed",
            "correction": (
                {"classification": "narrower-threshold"} if correction else None
            ),
            "unknown_outcome": False,
            "idempotency_key": review_key,
            "database_name": "appdb",
        },
    )
    return terminal, review


@pytest.mark.asyncio
async def test_evidence_governed_learning_lifecycle_acceptance(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = _config(tmp_path)
    app = AzureSqlMcpApplication(config)
    assert app.learning_store is not None
    runtime = await app.mcp._tool_manager.call_tool("check_runtime_status", {})
    scope = app._current_learning_scope("appdb")
    recall_args = {
        "skill": "sql-optimizer",
        "skill_version": "2.3.0",
        "runtime_compatibility_fingerprint": runtime[
            "runtime_compatibility_fingerprint"
        ],
        "tool_schema_fingerprint": runtime["tool_schema_fingerprint"],
        "sanitized_config_fingerprint": runtime[
            "sanitized_config_fingerprint"
        ],
        "database_name": "appdb",
    }
    assert await app.mcp._tool_manager.call_tool("recall_lessons", recall_args) == {
        "lessons": [],
        "count": 0,
        "max_results": 3,
    }

    review_ids: list[str] = []
    terminal_links: list[str] = []
    prior_reviews: list[str] = []
    for index, (session_id, subject) in enumerate(
        (
            ("observation-session-1", "subject-a"),
            ("observation-session-1", "subject-b"),
            ("observation-session-2", "subject-a"),
        ),
        start=1,
    ):
        evidence = _evidence(
            app,
            database_fingerprint=scope["database_fingerprint"],
            suffix=str(index),
        )
        decision_args = {
            "app": app,
            "runtime": runtime,
            "evidence": evidence,
            "session_id": session_id,
            "subject_fingerprint": subject,
            "based_on_review_ids": prior_reviews[-1:],
            "idempotency_key": f"decision-{index}",
        }
        decision = await _record_optimizer_decision(**decision_args)
        if index == 1:
            replay = await _record_optimizer_decision(**decision_args)
            assert replay["decision_id"] == decision["decision_id"]
        terminal, review = await _terminal_and_review(
            app,
            decision=decision,
            evidence=evidence,
            review_key=f"review-{index}",
            correction=index == 1,
        )
        stored_terminal = app.learning_store.get_terminal_link(
            terminal["terminal_link_id"]
        )
        assert stored_terminal["evidence_refs"] == [evidence.evidence_id]
        review_ids.append(review["review_id"])
        terminal_links.append(terminal["terminal_link_id"])
        prior_reviews.append(review["review_id"])
        if index > 1:
            assert decision["based_on_review_ids"] == [review_ids[index - 2]]

    proposal = await app.mcp._tool_manager.call_tool(
        "propose_lesson",
        {
            "learning_key": "bounded-candidate-selection",
            "review_ids": review_ids,
            "trigger": {"classification": "bounded-candidate"},
            "action": {"classification": "repeat-bounded-evaluation"},
            "preconditions": {"classification": "same-scope"},
            "counterexamples": [{"classification": "bounded-noise"}],
            "next_observation": {"classification": "repeat-bounded-run"},
            "required_evidence": [stored_terminal["evidence_refs"][0]],
            "applicable_skills": ["sql-optimizer"],
            "idempotency_key": "normal-proposal",
            "database_name": "appdb",
        },
    )
    assert proposal["status"] == "eligible"

    handoff = await app.mcp._tool_manager.call_tool(
        "create_handoff",
        {
            "source_skill": "sql-optimizer",
            "target_skill": "sql-plan-enforcer",
            "objective": {"classification": "review-plan-control"},
            "evidence_refs": [terminal_links[-1]],
            "constraints": {"classification": "review-only"},
            "gaps": [],
            "acceptance_criteria": [{"classification": "terminal-verification"}],
            "idempotency_key": "handoff-1",
            "database_name": "appdb",
        },
    )
    fetched = await app.mcp._tool_manager.call_tool(
        "get_handoff",
        {"handoff_id": handoff["handoff_id"], "database_name": "appdb"},
    )
    assert fetched["target_skill"] == "sql-plan-enforcer"
    plan_decision = await app.mcp._tool_manager.call_tool(
        "record_decision",
        {
            "skill": "sql-plan-enforcer",
            "skill_version": "1.0.0",
            "session_id": "plan-session-1",
            "learning_key": "plan-control-review",
            "consumed_evidence_refs": [terminal_links[-1]],
            "subject_kind": "plan",
            "subject_fingerprint": "plan-subject-a",
            "based_on_review_ids": [],
            "tactic": "hold-for-verification",
            "expected_result": {"classification": "verified"},
            "confidence": 0.7,
            "uncertainty": {"classification": "bounded"},
            "evaluator_fingerprint": "acceptance-evaluator",
            "runtime_fingerprint": runtime["runtime_fingerprint"],
            "runtime_compatibility_fingerprint": runtime[
                "runtime_compatibility_fingerprint"
            ],
            "tool_schema_fingerprint": runtime["tool_schema_fingerprint"],
            "sanitized_config_fingerprint": runtime[
                "sanitized_config_fingerprint"
            ],
            "idempotency_key": "plan-decision",
            "database_name": "appdb",
        },
    )
    claimed = await app.mcp._tool_manager.call_tool(
        "resolve_handoff",
        {
            "handoff_id": handoff["handoff_id"],
            "action": "claim",
            "expected_version": 0,
            "owner": "sql-plan-enforcer",
            "database_name": "appdb",
        },
    )
    assert claimed["status"] == "claimed"
    resolved = await app.mcp._tool_manager.call_tool(
        "resolve_handoff",
        {
            "handoff_id": handoff["handoff_id"],
            "action": "resolve",
            "expected_version": 1,
            "resolution": {"classification": "verified"},
            "resolution_evidence_refs": [terminal_links[-1]],
            "decision_id": plan_decision["decision_id"],
            "database_name": "appdb",
        },
    )
    assert resolved["status"] == "resolved"
    assert resolved["learning_link_status"] == "linked"
    plan_review = await app.mcp._tool_manager.call_tool(
        "review_decision",
        {
            "decision_id": plan_decision["decision_id"],
            "terminal_evidence_refs": [resolved["terminal_link_id"]],
            "observed_result": {"classification": "verified"},
            "prediction_error": {"classification": "none"},
            "counterexamples": [{"classification": "ownership-change"}],
            "next_observation": {"classification": "bounded-verification"},
            "causal_strength": "moderate",
            "alignment": "aligned",
            "safety_signal": "passed",
            "equivalence_signal": "passed",
            "cleanup_signal": "passed",
            "material_regression_signal": "passed",
            "unknown_outcome": False,
            "idempotency_key": "plan-review",
            "database_name": "appdb",
        },
    )
    assert plan_review["decision_id"] == plan_decision["decision_id"]

    process_fingerprint = runtime["runtime_fingerprint"]
    _close(app)
    database_path = tmp_path / "state" / "performance.sqlite3"
    assert (
        learning_cli(
            [
                "--db-path",
                str(database_path),
                "activate",
                proposal["lesson_id"],
                "--reviewer",
                "acceptance-maintainer",
                "--expected-version",
                "0",
            ]
        )
        == 0
    )
    activated = json.loads(capsys.readouterr().out)
    assert activated["status"] == "active"
    pack_path = tmp_path / "learning-pack.json"
    assert (
        learning_cli(
            [
                "--db-path",
                str(database_path),
                "export",
                "--output",
                str(pack_path),
            ]
        )
        == 0
    )
    capsys.readouterr()
    exported = json.loads(pack_path.read_text(encoding="utf-8"))
    assert [lesson["lesson_id"] for lesson in exported["lessons"]] == [
        proposal["lesson_id"]
    ]
    tampered = pack_path.with_name("tampered-learning-pack.json")
    tampered_payload = json.loads(json.dumps(exported))
    tampered_payload["lessons"][0]["action"]["classification"] = "tampered"
    tampered.write_text(json.dumps(tampered_payload), encoding="utf-8")
    assert (
        learning_cli(
            [
                "--db-path",
                str(tmp_path / "tampered.sqlite3"),
                "import",
                str(tampered),
            ]
        )
        == 2
    )
    assert "content hash" in capsys.readouterr().err

    restarted = AzureSqlMcpApplication(config)
    assert restarted.learning_store is not None
    restarted_runtime = await restarted.mcp._tool_manager.call_tool(
        "check_runtime_status", {}
    )
    assert restarted_runtime["runtime_fingerprint"] != process_fingerprint
    assert (
        restarted_runtime["runtime_compatibility_fingerprint"]
        == runtime["runtime_compatibility_fingerprint"]
    )
    recalled = await restarted.mcp._tool_manager.call_tool(
        "recall_lessons",
        {
            **recall_args,
            "runtime_compatibility_fingerprint": restarted_runtime[
                "runtime_compatibility_fingerprint"
            ],
            "tool_schema_fingerprint": restarted_runtime[
                "tool_schema_fingerprint"
            ],
            "sanitized_config_fingerprint": restarted_runtime[
                "sanitized_config_fingerprint"
            ],
        },
    )
    assert [lesson["lesson_id"] for lesson in recalled["lessons"]] == [
        proposal["lesson_id"]
    ]

    privacy_evidence = _evidence(
        restarted,
        database_fingerprint=scope["database_fingerprint"],
        suffix="privacy",
    )
    invalid_args = {
        "app": restarted,
        "runtime": restarted_runtime,
        "evidence": privacy_evidence,
        "session_id": "privacy-session",
        "subject_fingerprint": "privacy-subject",
        "based_on_review_ids": [],
        "idempotency_key": "privacy-decision",
    }
    with pytest.raises(ToolError):
        await restarted.mcp._tool_manager.call_tool(
            "record_decision",
            {
                "skill": "sql-optimizer",
                "skill_version": "2.3.0",
                "session_id": invalid_args["session_id"],
                "learning_key": "privacy-check",
                "consumed_evidence_refs": [privacy_evidence.evidence_id],
                "subject_kind": "query",
                "subject_fingerprint": invalid_args["subject_fingerprint"],
                "based_on_review_ids": [],
                "tactic": "bounded-check",
                "expected_result": {"sql": "SELECT 1"},
                "confidence": 0.5,
                "uncertainty": {"classification": "bounded"},
                "evaluator_fingerprint": "acceptance-evaluator",
                "runtime_fingerprint": restarted_runtime[
                    "runtime_fingerprint"
                ],
                "runtime_compatibility_fingerprint": restarted_runtime[
                    "runtime_compatibility_fingerprint"
                ],
                "tool_schema_fingerprint": restarted_runtime[
                    "tool_schema_fingerprint"
                ],
                "sanitized_config_fingerprint": restarted_runtime[
                    "sanitized_config_fingerprint"
                ],
                "idempotency_key": invalid_args["idempotency_key"],
                "database_name": "appdb",
            },
        )

    contradiction_evidence = _evidence(
        restarted,
        database_fingerprint=scope["database_fingerprint"],
        suffix="contradiction",
    )
    contradicted_decision = await _record_optimizer_decision(
        restarted,
        runtime=restarted_runtime,
        evidence=contradiction_evidence,
        session_id="observation-session-3",
        subject_fingerprint="subject-c",
        based_on_review_ids=[review_ids[-1]],
        idempotency_key="contradiction-decision",
        applied_lesson_ids=[proposal["lesson_id"]],
    )
    contradicted_terminal = await restarted._run_tool(
        "benchmark_tuning_candidate",
        "appdb",
        AsyncMock(
            return_value={
                "status": "complete",
                "classification": "equivalence-failed",
                "evidence_id": contradiction_evidence.evidence_id,
            }
        ),
        decision_id=contradicted_decision["decision_id"],
    )
    contradiction_review = await restarted.mcp._tool_manager.call_tool(
        "review_decision",
        {
            "decision_id": contradicted_decision["decision_id"],
            "terminal_evidence_refs": [
                contradicted_terminal["terminal_link_id"]
            ],
            "observed_result": {"classification": "equivalence-failed"},
            "prediction_error": {"classification": "material"},
            "counterexamples": [{"classification": "equivalence-failure"}],
            "next_observation": {"classification": "hold"},
            "causal_strength": "strong",
            "alignment": "contradiction",
            "safety_signal": "passed",
            "equivalence_signal": "proven_failure",
            "cleanup_signal": "passed",
            "material_regression_signal": "passed",
            "correction": {"classification": "do-not-reuse"},
            "unknown_outcome": False,
            "idempotency_key": "contradiction-review",
            "database_name": "appdb",
        },
    )
    assert (
        restarted.learning_store.get_lesson(proposal["lesson_id"]).status
        == "quarantined"
    )
    urgent = await restarted.mcp._tool_manager.call_tool(
        "propose_lesson",
        {
            "learning_key": "bounded-candidate-selection",
            "review_ids": [contradiction_review["review_id"]],
            "trigger": {"classification": "equivalence-failure"},
            "action": {"classification": "avoid-reuse"},
            "preconditions": {"classification": "same-scope"},
            "counterexamples": [{"classification": "proven-failure"}],
            "next_observation": {"classification": "maintainer-review"},
            "required_evidence": [contradiction_evidence.evidence_id],
            "applicable_skills": ["sql-optimizer"],
            "idempotency_key": "urgent-proposal",
            "database_name": "appdb",
        },
    )
    assert urgent["proposal_kind"] == "urgent"
    assert urgent["status"] == "proposed"
    after_quarantine = await restarted.mcp._tool_manager.call_tool(
        "recall_lessons",
        {
            **recall_args,
            "runtime_compatibility_fingerprint": restarted_runtime[
                "runtime_compatibility_fingerprint"
            ],
            "tool_schema_fingerprint": restarted_runtime[
                "tool_schema_fingerprint"
            ],
            "sanitized_config_fingerprint": restarted_runtime[
                "sanitized_config_fingerprint"
            ],
        },
    )
    assert after_quarantine["lessons"] == []
    _close(restarted)
