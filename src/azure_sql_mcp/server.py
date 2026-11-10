from __future__ import annotations

import asyncio
import difflib
import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import time
import uuid
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from typing import Any
from typing import cast
from typing import Literal
from typing import NoReturn

from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import Context
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.fastmcp.tools import ToolManager
from mcp.types import ToolAnnotations
from pydantic import AnyHttpUrl
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import ValidationError

from .admin_policy import AdminAction
from .admin_policy import AdminPolicy
from .admin_policy import ConfirmationTokens
from .admin_policy import _is_timeout_error
from .application_patterns import ApplicationPatternService
from .artifact_store import ArtifactStore
from .artifacts import ErrorPayload
from .artifacts import ExplainPlanArtifact
from .auth import AzureSqlAuthenticator
from .capabilities import CapabilityService
from .column_statistics import ColumnStatisticsService
from .capacity_assessment import CapacityAssessmentService
from .catalog_constraints import TableConstraintFacts
from .catalog_constraints import UniqueKey
from .columnstore_health import OBJECT_FILTER
from .columnstore_health import ROW_GROUP_SQL
from .columnstore_health import ColumnstoreHealthService
from .columnstore_health import summarize_columnstore_indexes
from .candidate_lineage import combined_parent_id
from .candidate_lineage import validate_combined_parent
from .config import AccessMode
from .config import McpProfile
from .config import TOOL_GROUPS
from .config import ServerConfig
from .config import TransportMode
from .config import load_server_config
from .connection import AzureSqlExecutor
from .connection import statement_deadline
from .connection_pool import ConnectionPool
from .database_baseline import DatabaseBaselineService
from .database_diagnosis import DatabaseDiagnosisService
from .database_policy import DatabasePolicy
from .database_policy import WORKFLOW_TEST_INDEX_PREFIX
from .database_policy import extends_test_index_prefix
from .database_policy import load_database_policy_or_deny
from .diagnostics import DiagnosticQueryService
from .elastic_pool import ElasticPoolService
from .equivalence_contract import effective_compare_order
from .equivalence_contract import rewrite_requirements
from .equivalence_contract import statement_hint_profile
from .equivalence_contract import tables_read
from .equivalence_preflight import EquivalencePreflightService
from .health import HealthService
from .history_azure_monitor import ARM_SCOPE
from .history_azure_monitor import LOG_ANALYTICS_SCOPE
from .history_azure_monitor import AzureMonitorMetricsSource
from .history_azure_monitor import LogAnalyticsHistorySource
from .history_database_watcher import KUSTO_SCOPE
from .history_database_watcher import DatabaseWatcherHistorySource
from .history_extended_events import ExtendedEventsHistorySource
from .history_loki import LokiHistorySource
from .history_loki import parse_loki_settings
from .history_sources import EntraTokenProvider
from .history_sources import HistoryCategory
from .history_sources import HistoryResolver
from .history_sources import TimeWindow
from .history_sources import UrllibTransport
from .history_sources import credential_for
from .incident_log import IncidentLog
from .incident_log import native_codes
from .incident_log import note_exception
from .incident_log import record_startup_failure
from .incident_log import summarize_backlog
from .incident_log import tool_failure_code
from .index_advisor import NON_ALIGNED_INDEX_WARNING
from .index_advisor import build_index_change_plan
from .index_optimizer import INDEX_KIND_NONCLUSTERED_COLUMNSTORE
from .index_optimizer import INDEX_KIND_ROWSTORE
from .index_optimizer import TEST_INDEX_KINDS
from .index_optimizer import TEST_INDEX_LOW_PRIORITY_WAIT_MINUTES
from .index_optimizer import IndexCandidate
from .index_optimizer import IndexOptimizer
from .index_optimizer import ReplacementExperiment
from .index_optimizer import bounded_table_lock_probe_sql
from .index_optimizer import build_test_index_statement
from .index_optimizer import TRANSIENT_BUILD_ERRORS
from .index_optimizer import classify_create_index_failure
from .index_optimizer import columnstore_index_blockers
from .index_optimizer import expected_index_definition_matches
from .index_optimizer import hint_candidate_index
from .index_optimizer import plan_replacement_experiment
from .index_optimizer import verify_plan_uses_index
from .index_metadata import collect_existing_indexes
from .index_metadata import collect_table_column_facts
from .index_metadata import existing_index_covers_candidate
from .index_metadata import MAX_GENERATED_TEST_INDEX_NAME_LENGTH
from .index_metadata import is_test_index_name
from .index_metadata import parse_candidate_key
from .index_recommendations import IndexRecommendationService
from .index_review import IndexReviewService
from .index_review import IndexReviewSetupError
from .index_review import index_history_setup_precondition
from .index_review import CONTRACT_SCHEMA_FINGERPRINT
from .index_review import SqlIndexHistoryRepository
from .index_review import compact_index_review
from .index_review import index_review_subjects
from .index_review import render_index_review_artifacts
from .introspection import IntrospectionService
from .lock_diagnostics import LockDiagnosticsService
from .memory_pressure import MemoryPressureService
from .learning_contracts import DecisionRecordV1
from .learning_contracts import HandoffV1
from .learning_contracts import OutcomeReviewV1
from .learning_service import LearningService
from .learning_store import LearningStore
from .learning_store import LearningStoreError
from .logging_config import configure_logging
from .observability import apply_data_detail
from .observability import extract_failure_diagnostic
from .observability import not_found_details
from .observability import sanitize_error_message
from .param_binding import detect_parameters
from .param_binding import ParameterExecutionContract
from .param_binding import ParameterBindingService
from .performance_contracts import EvidenceEnvelopeV1
from .performance_contracts import PerformanceCaseV1
from .performance_contracts import STOPPING_REASON_MAX_LENGTH
from .performance_store import ContractNotFoundError
from .performance_store import IdempotencyConflictError
from .performance_store import PerformanceStore
from .performance_store import PerformanceStoreError
from .performance_store import ReservationError
from .performance_workflows import COMPARISON_DECISION_BASIS
from .performance_workflows import PerformanceWorkflowService
from .performance_workflows import aba_result_status
from .performance_workflows import aggregate_samples
from .performance_workflows import classify_benchmark
from .performance_workflows import compact_benchmark_result
from .performance_workflows import compact_evidence_result
from .performance_workflows import compare_plan_summaries_payload
from .performance_workflows import database_fingerprint
from .performance_workflows import database_fingerprint_matches
from .performance_workflows import extract_profile_metrics
from .performance_workflows import fingerprint_json
from .performance_workflows import fingerprint_text
from .performance_workflows import fingerprint_text_matches
from .performance_workflows import label_comparison
from .performance_workflows import order_contract_removed
from .performance_workflows import mark_benchmark_sql
from .performance_workflows import parameter_case_fingerprint
from .performance_workflows import parameter_case_mismatch
from .performance_workflows import parameter_case_receipt
from .performance_workflows import plan_delta_from_profiles
from .performance_workflows import preflight_gap_codes
from .performance_workflows import profile_result_fingerprint
from .performance_workflows import scope_case_regressions
from .performance_workflows import with_order_contract
from .view_experiments import LEASE_OBJECT_KIND
from .view_experiments import MAX_VIEW_EXTRA_INDEXES
from .view_experiments import READ_MODE_AUTOMATIC
from .view_experiments import READ_MODE_NOEXPAND
from .view_experiments import SESSION_SET_OPTIONS_SQL
from .view_experiments import ViewCatalogState
from .view_experiments import ViewIndexSpec
from .view_experiments import build_test_view_drop_batch
from .view_experiments import build_view_experiment_create_batch
from .view_experiments import name_in_use_blocker
from .view_experiments import plan_view_experiment
from .view_experiments import read_view_catalog_state
from .view_experiments import reconcile_view_experiment
from .view_experiments import require_view_experiment_policy
from .view_experiments import session_set_option_blockers
from .view_experiments import validate_test_view_definition
from .view_experiments import verify_view_experiment
from .view_experiments import view_build_failure
from .view_experiments import view_candidate_findings
from .view_experiments import view_experiment_batch_params
from .view_experiments import view_experiment_policy_blockers
from .view_experiments import view_index_size
from .view_experiments import view_name_prefixes
from .plan_action_service import PlanActionService
from .plan_cache import PlanCacheService
from .plan_enforcement import PlanEnforcementService
from .plan_digest import build_plan_digest  # noqa: F401  (kept for callers that patch it)
from .plan_digest import describe_plan_node
from .plan_rules import analyze_plan  # noqa: F401  (kept for callers that patch it)
from .plan_rules import read_plan
from .plan_sources import PlanSource
from .plan_sources import PlanSourceService
from .plan_tree import ParsedPlan
from .plan_tree import PlanParseError
from .plan_tree import parse_plan
from .plan_tree import partition_access
from .plans import PlansService
from .plans import compact_plan_summary
from .platform_capabilities import PlatformCapabilitiesService
from .platform_capabilities import unsupported_platform_reason
from .prompts import register_prompts
from .query_index_analysis import QueryIndexAnalysisService
from .query_hints import validate_query_hints
from .query_regression import QueryRegressionService
from .query_store import QueryStoreService
from .query_store_trends import QueryStoreTrendService
from .resource_governance import ResourceGovernanceService
from .resources import register_resources
from .result_status import ResultStatus
from .result_status import apply_result_status
from .result_status import status_payload
from .retry import _is_transient
from .safe_sql import SafeSqlValidator
from .schema_compat import portable_input_schema
from .showplan_access import parse_plan_access
from .server_instructions import SERVER_INSTRUCTIONS
from .schema_compare import SchemaCompareService
from .sessions import SessionsService
from .tempdb_memory import TempdbMemoryService
from .time_windows import AS_OF_DESCRIPTION
from .time_windows import iso_z
from .time_windows import parse_as_of
from .time_windows import window_bounds
from .tool_contracts import add_tool_headline
from .tool_guides import get_tool_guide as read_tool_guide
from .tool_guides import list_tool_guides
from .tool_contracts import annotate_next_tools
from .tool_contracts import BenchmarkPhase
from .tool_contracts import BenchmarkToolOutput
from .tool_contracts import CaseToolOutput
from .tool_contracts import ParameterCaseInput
from .tool_contracts import PlanToolOutput
from .tool_contracts import PreflightToolOutput
from .tool_contracts import SelectionScope
from .tool_contracts import SessionToolOutput
from .tool_contracts import TuningObjective
from .tool_contracts import TuningStrategy
from .tuning_sessions import FAST_STATEMENT_SECONDS
from .tuning_sessions import INDEX_ABA_PHASES
from .tuning_sessions import SLOW_STATEMENT_SECONDS
from .tuning_sessions import InvalidTransitionError
from .tuning_sessions import TuningSessionStateMachine
from .tuning_sessions import adaptive_run_count
from .tuning_sessions import decisive_first_runs
from .tuning_sessions import session_failure_diagnostic
from .transport_auth import StaticBearerTokenVerifier
from .version_store import VersionStoreService
from .wait_stats import WaitStatsService
from .workload_index_advisor import WorkloadIndexAdvisor
from .view_workflows import PreparedViewChange
from .view_workflows import ViewChangeRequest
from .view_workflows import ViewVerificationError
from .view_workflows import ViewWorkflowService
from .view_workflows import prepared_view_change_from_state
from .view_workflows import prepared_view_change_state
from .view_workflows import resolve_view_request_schema_bound
from .view_workflows import view_apply_receipt
from .view_workflows import view_snapshot_from_receipt

ResponseType = dict[str, Any]

logger = logging.getLogger(__name__)


def _parameter_case_payload(
    parameter_case: ParameterCaseInput | Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if parameter_case is None:
        return None
    if isinstance(parameter_case, ParameterCaseInput):
        return parameter_case.model_dump(mode="python", exclude_none=True)
    return dict(parameter_case)


def _parameter_case_payloads(
    parameter_cases: list[ParameterCaseInput] | None,
) -> list[dict[str, Any]] | None:
    if parameter_cases is None:
        return None
    return [
        parameter_case.model_dump(mode="python", exclude_none=True)
        for parameter_case in parameter_cases
    ]

# Disposable test indexes (create_test_index / drop_test_index) are namespaced by this
# prefix; the drop tool refuses anything outside it so real indexes are untouchable.
# A database policy may list more prefixes (test_index_name_prefixes).
TEST_INDEX_PREFIX = WORKFLOW_TEST_INDEX_PREFIX
MAX_TEST_INDEX_SET = 4
# One row per index: B-tree depth, pages in use and rows, read after the build.
# INDEXPROPERTY(..., 'IndexDepth') is NULL for a partitioned index (observed on
# Azure SQL Database). The depth is then the deepest partition, read in LIMITED
# mode (parent pages only). The function takes no correlated arguments, so the
# object and index ids are variables.
_BUILT_INDEX_SQL = """
DECLARE @object_id int = OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?));
DECLARE @index_name sysname = ?;
DECLARE @index_id int = (
    SELECT i.index_id
    FROM sys.indexes AS i
    WHERE i.object_id = @object_id AND i.name = @index_name
);
SELECT
    COALESCE(
        INDEXPROPERTY(@object_id, @index_name, 'IndexDepth'),
        (
            SELECT MAX(ips.index_depth)
            FROM sys.dm_db_index_physical_stats(
                DB_ID(), @object_id, @index_id, NULL, 'LIMITED'
            ) AS ips
            WHERE @index_id IS NOT NULL
              AND ips.alloc_unit_type_desc = N'IN_ROW_DATA'
        )
    ) AS index_depth,
    SUM(ps.used_page_count) AS used_pages,
    SUM(ps.row_count) AS row_count,
    MAX(p.data_compression_desc) AS data_compression
FROM sys.dm_db_partition_stats AS ps
JOIN sys.partitions AS p ON p.partition_id = ps.partition_id
WHERE ps.object_id = @object_id
  AND ps.index_id = @index_id
HAVING COUNT(*) > 0;
"""


_SLOW_BASELINE_DESCRIPTION = (
    "Parameter cases whose baseline is too slow to repeat: {case name: seconds one "
    "run takes}. Such a baseline runs once, without a warm-up, and later candidates "
    "of the session reuse that one sample, in both benchmark tools. With one sample "
    "only a gain of 50% or more counts as improved. A baseline whose first run takes "
    "10 seconds or more is treated this way without being named here."
)


class IndexDefinitionInput(BaseModel):
    """One index of the set that benchmark_index_candidate tests as one candidate."""

    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(min_length=1)
    key_columns: list[str] = Field(
        min_length=1,
        description=(
            "Ordered key columns, each optionally with ASC or DESC. For kind "
            "nonclustered_columnstore: the columns of the index, no ASC or DESC."
        ),
    )
    schema_name: str = Field(default="dbo", min_length=1)
    include_columns: list[str] | None = None
    filter_definition: str | None = None
    is_unique: bool = False
    data_compression: (
        Literal["NONE", "ROW", "PAGE", "COLUMNSTORE", "COLUMNSTORE_ARCHIVE"] | None
    ) = Field(
        default=None,
        description=(
            "rowstore: NONE, ROW or PAGE. nonclustered_columnstore: COLUMNSTORE "
            "(default) or COLUMNSTORE_ARCHIVE."
        ),
    )
    kind: Literal["rowstore", "nonclustered_columnstore"] = Field(
        default="rowstore",
        description=(
            "rowstore: a B-tree index. nonclustered_columnstore: for an aggregate "
            "scan of a rowstore table; no include_columns, not unique. Refused before "
            "any run when the table already has a columnstore index or a column is "
            "(max), computed, sparse or of a type a columnstore index cannot hold."
        ),
    )
    partition_aligned: bool = Field(
        default=True,
        description=(
            "On a partitioned table: true builds the index on the table's partition "
            "scheme (one B-tree per partition). false builds it ON [PRIMARY]: one "
            "ordered B-tree across all partitions, which blocks partition SWITCH "
            "(non_aligned_index_warning in the result). Refused on a partitioned "
            "table with a clustered columnstore index. Ignored with a note on a "
            "table that is not partitioned."
        ),
    )


class ViewIndexInput(BaseModel):
    """One more index on the view that benchmark_indexed_view_candidate builds."""

    model_config = ConfigDict(extra="forbid")

    key_columns: list[str] = Field(min_length=1)
    include_columns: list[str] | None = None
    is_unique: bool = False


_GENERATED_NAME_HASH_LENGTH = 8


# Caller text kept in a generated name when its prefix could not be used.
_MAX_TEST_INDEX_NAME_TAG_LENGTH = 24


def generated_test_index_name(prefix: str, candidate: IndexCandidate, tag: str = "") -> str:
    """The name of a workflow test index: readable, and one name per definition.

    ``<prefix><Table>_<KeyColumn1>_<KeyColumn2>..._<hash>``: the table name
    without its schema, the key columns in order (no ASC or DESC), every
    character outside letters, digits and underscore replaced by an
    underscore, then the first 8 hex digits of the definition fingerprint.
    A ``tag`` goes before the table name. The readable part is cut to keep
    the name within ``MAX_GENERATED_TEST_INDEX_NAME_LENGTH`` characters.
    """

    parts = [
        *([tag] if tag else []),
        candidate.table,
        *(parse_candidate_key(key).name for key in candidate.key_columns),
    ]
    readable = "_".join(re.sub(r"[^A-Za-z0-9_]", "_", part) for part in parts)
    room = (
        MAX_GENERATED_TEST_INDEX_NAME_LENGTH - len(prefix) - 1 - _GENERATED_NAME_HASH_LENGTH
    )
    readable = readable[: max(0, room)].rstrip("_")
    suffix = candidate.definition_fingerprint[:_GENERATED_NAME_HASH_LENGTH]
    return f"{prefix}{readable}_{suffix}"


def _usable_name_text(text: str | None) -> str:
    """The leading letters, digits and underscores of caller text, for a name."""

    usable = re.match(r"[A-Za-z0-9_]*", str(text or "").strip())
    return usable.group().strip("_") if usable is not None else ""


def _is_generated_test_index_name(name: str | None, prefixes: Sequence[str]) -> bool:
    """A name the index workflow generated, under a built-in or a policy prefix."""

    return is_test_index_name(name, prefixes)


def _indexes_used(plan_use: Mapping[str, Any]) -> list[str]:
    """The test indexes one measured plan read, for a one-index or a set record."""

    used = plan_use.get("indexes_used")
    if isinstance(used, list):
        return [str(name) for name in used]
    return [str(plan_use.get("expected_index_name"))] if plan_use.get("used") is True else []


def _index_set_plan_use(uses: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Plan use of an index set in one run: used when the plan reads at least one of them."""

    used = [str(use.get("expected_index_name")) for use in uses if use.get("used") is True]
    return {
        "expected_index_names": [str(use.get("expected_index_name")) for use in uses],
        "indexes_used": used,
        "used": bool(used),
        "verified": bool(used),
        "plan_fingerprint": uses[0].get("plan_fingerprint") if uses else None,
        "evidence": "showplan_xml",
    }


def _replacement_summary(experiment: ReplacementExperiment) -> dict[str, Any]:
    return {
        "replaces": list(experiment.replaces),
        "hide_method": experiment.hide_method,
        "candidate_hint": experiment.candidate_hint,
        "proof_scope": experiment.proof_scope,
        "notes": list(experiment.notes),
    }


NON_ALIGNED_DATA_SPACE = "PRIMARY"


@dataclass(frozen=True)
class _TestIndexTable:
    """Catalog facts about the table a test index is built on."""

    schema: str
    table: str
    # The clustered index is columnstore (sys.indexes type 5): the engine then
    # refuses an online index build with error 35352. A nonclustered columnstore
    # index (type 6) on a rowstore table does not: that build stays online.
    has_clustered_columnstore_index: bool = False
    partition_scheme: str | None = None
    partition_function: str | None = None
    partition_column: str | None = None
    # Set for a test index that is not partition-aligned: the filegroup it is
    # built on. Azure SQL Database has one filegroup, PRIMARY.
    data_space: str | None = None

    @property
    def partitioned(self) -> bool:
        return self.partition_scheme is not None and self.partition_column is not None

    def non_aligned(self) -> "_TestIndexTable":
        """The same table for an index built on PRIMARY, not on the partition scheme."""

        return replace(
            self,
            partition_scheme=None,
            partition_function=None,
            partition_column=None,
            data_space=NON_ALIGNED_DATA_SPACE,
        )

    def placement(self) -> dict[str, Any]:
        """IndexCandidate fields that place a test index on a partitioned table.

        CREATE INDEX without an ON clause builds the index on the table's
        partition scheme. The request names that placement, so the built index
        is compared with what the engine creates. A non-aligned index names
        its filegroup and no partition column.
        """

        if self.data_space is not None:
            return {"data_space_name": self.data_space}
        if self.partition_scheme is None or self.partition_column is None:
            return {}
        return {
            "data_space_name": self.partition_scheme,
            "partition_columns": (self.partition_column,),
            "partition_scheme_name": self.partition_scheme,
            "partition_function_name": self.partition_function,
        }


_COLUMNSTORE_OFFLINE_BUILD_NOTE = (
    "The table has a clustered columnstore index. Azure SQL Database cannot build an index "
    "online on such a table, so the test index is built offline. An offline build blocks all "
    "writes to the table while it runs."
)


def _test_index_build_online(
    policy: DatabasePolicy, online: bool, table: _TestIndexTable | None
) -> bool:
    """Whether a test index is built online on this table.

    A table with a clustered columnstore index takes an offline build only.
    Raises when the database policy does not allow that build.
    """

    if table is None or not table.has_clustered_columnstore_index:
        return online
    if not policy.allow_offline_test_index:
        raise PermissionError(
            f"{table.schema}.{table.table} has a clustered columnstore index. Azure SQL "
            "Database cannot build an index online on such a table (engine error 35352), so "
            "a test index on it must be built offline, and an offline build blocks all "
            "writes to the table while it runs. The database policy does not allow that "
            "build. A DBA must set allow_offline_test_index to true for this database "
            "before a test index can be built on this table. No query and no DDL was run."
        )
    return False


_INDEX_DDL_STEPS = frozenset({"create_index", "drop_index"})


def _not_compared(blocking_gaps: Sequence[str], failure: Any = None) -> dict[str, Any]:
    """The equivalence entry of a case whose results were not compared, with the reason."""

    if blocking_gaps:
        reason = (
            "a result comparison cannot settle "
            + ", ".join(blocking_gaps)
            + ": the rows or the order of a statement can change between two runs "
            "on the same data"
        )
    elif failure:
        reason = "the experiment stopped before the results were compared"
    else:
        reason = "the results of this case were not compared"
    return {
        "status": "not_compared",
        "proven_for_parameter_case": False,
        "same_snapshot": False,
        "reason": reason,
    }


class _CallTimeCap(Exception):
    """max_call_seconds was reached before the named step of a benchmark call."""


class _CallClock:
    """Wall time of one benchmark call: the seconds of each phase and the caller's cap."""

    PHASES = ("baseline_runs", "build", "candidate_runs", "comparison", "cleanup")

    def __init__(self, max_call_seconds: float | None) -> None:
        if max_call_seconds is not None and max_call_seconds <= 0:
            raise ValueError("max_call_seconds must be greater than zero.")
        self.max_call_seconds = max_call_seconds
        self.started = time.monotonic()
        self.seconds: dict[str, float] = dict.fromkeys(self.PHASES, 0.0)

    def start(self) -> float:
        return time.monotonic()

    def add(self, phase: str, started: float) -> None:
        self.seconds[phase] += time.monotonic() - started

    def elapsed(self) -> float:
        return time.monotonic() - self.started

    def left(self) -> float:
        """Seconds until the cap; infinite without one."""
        if self.max_call_seconds is None:
            return float("inf")
        return self.max_call_seconds - self.elapsed()

    def require(self, step: str) -> None:
        """Stop before ``step`` when the cap is reached. A running statement is never cut."""
        if self.left() <= 0:
            raise _CallTimeCap(step)

    def cap_diagnostic(self, step: str) -> dict[str, Any]:
        return {
            "diagnostic_code": "call_time_cap",
            "failed_step": step,
            "max_call_seconds": self.max_call_seconds,
            "elapsed_seconds": round(self.elapsed(), 1),
        }

    def report(self) -> dict[str, float]:
        """``time_spent``: seconds per phase, the rest of the call, and the total."""
        total = self.elapsed()
        spent = {phase: round(seconds, 1) for phase, seconds in self.seconds.items()}
        spent["other"] = round(max(0.0, total - sum(self.seconds.values())), 1)
        spent["total"] = round(total, 1)
        return spent


def _index_test_objects(
    members: Sequence[_IndexSetMember],
    lease: Mapping[str, Any],
    held_entries: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """``test_objects`` of an index benchmark: what was reused, built and is still held."""

    def report(member: _IndexSetMember) -> dict[str, Any]:
        return _held_object_report(
            {
                "target_schema": member.schema,
                "target_table": member.table,
                "index_name": member.index_name,
            }
        )

    still_held = [_held_object_report(entry, lease) for entry in held_entries]
    test_objects: dict[str, Any] = {
        "reused": [report(member) for member in members if member.reused],
        "built": [
            report(member)
            for member in members
            if member.ownership_recorded and not member.reused
        ],
        "still_held": still_held,
        "hold_expires_at_utc": lease["expires_at_utc"] if still_held else None,
    }
    if still_held:
        test_objects["note"] = (
            "These objects stay in place for this session. A later benchmark call "
            "reuses each one whose definition matches; finalize_tuning_session "
            "drops them, and so does the lease when it expires."
        )
    return test_objects


def _build_rate(members: Sequence[_IndexSetMember], build_seconds: float) -> float | None:
    """Rows per second of the builds of one call: the estimate of the next build in the session."""

    built_rows = sum(
        int(member.built["row_count"])
        for member in members
        if not member.reused
        and member.ownership_recorded
        and member.built
        and member.built.get("row_count")
    )
    return round(built_rows / build_seconds, 1) if built_rows and build_seconds > 0 else None


_DECISIVE_RUN_RULE = "one_run_each_large_gain"


def _decisive_run_note(baseline_seconds: float, candidate_seconds: float) -> str:
    return (
        f"the first candidate run took {candidate_seconds:g} s against {baseline_seconds:g} s "
        "for the baseline: at most one tenth of a statement that takes a second or more, "
        "so one run of each side decided. A smaller gain gets the repeated runs."
    )


def _call_cap_reason(clock: _CallClock, step: str, still_held: bool) -> str:
    return (
        f"the call reached max_call_seconds ({clock.max_call_seconds:g}) before step {step}: "
        "this is a partial result with no verdict. parameter_results holds what was "
        "measured and time_spent where the time went."
        + (
            " The test objects are still held: add a candidate for the same statement "
            "and call again; it reuses them and the baselines of this call."
            if still_held
            else ""
        )
    )


# Lease metadata key of a session hold: the test objects of the lease stay in
# place between the benchmark calls of its tuning session.
HOLD_FLAG = "held_for_session"
# Seconds to wait before the second and the third attempt to drop the objects
# of a lease; idle pooled connections are closed before each of them.
_LEASE_DROP_BACKOFF_SECONDS: tuple[float, ...] = (1.0, 3.0)
# Attempts to drop the objects of a lease: when a session ends, and before a
# call that must not run on a database with a leftover.
_FINALIZE_DROP_ATTEMPTS = 3
_PRE_CALL_DROP_ATTEMPTS = 2
# A drop that failed for one of these causes can succeed on a new connection.
_RETRYABLE_DROP_CAUSES = frozenset({"connection_lost", "timeout", "lock_wait_abort", "engine_error"})
# A lease whose create outcome is unknown: the build may still commit, so its
# object is dropped only after the lease has expired.
_OUTCOME_UNKNOWN_MARKS = frozenset(
    {"uncertain_after_create_dispatch", "outcome_unknown_after_transport_failure"}
)
_MAX_WARNED_TABLES = 8


class _LeaseDropError(Exception):
    """Some objects of a lease could not be dropped; ``failures`` is by index name."""

    def __init__(self, failures: Mapping[str, Exception]) -> None:
        super().__init__(f"{len(failures)} test object(s) of the lease were not dropped")
        self.failures = dict(failures)

    @property
    def first(self) -> Exception:
        return next(iter(self.failures.values()))


def _object_drop_statement(entry: Mapping[str, Any]) -> str:
    """The DDL a person runs to remove one leased test object."""

    schema = entry.get("target_schema")
    if entry.get("object_kind") == LEASE_OBJECT_KIND:
        return f"DROP VIEW [{schema}].[{entry.get('target_view')}];"
    return f"DROP INDEX [{entry.get('index_name')}] ON [{schema}].[{entry.get('target_table')}];"


def _drop_failure_diagnostic(
    exc: Exception,
    *,
    view: bool,
    query_timeout_seconds: int,
) -> dict[str, Any]:
    """Why the drop of one test object failed: the step, the class of the cause, the engine text.

    The DDL is built by the server, so the engine or driver message is
    returned through the same redaction as a failed test index build.
    """

    diagnostic = _index_experiment_failure(
        exc, step="drop_index", query_timeout_seconds=query_timeout_seconds
    )
    if view:
        diagnostic["failed_step"] = "drop_view"
    if (
        diagnostic.get("cause_class") in {"engine_error", "internal_error"}
        and classify_create_index_failure(exc)["reason"] == "connection_or_timeout"
    ):
        diagnostic["cause_class"] = "connection_lost"
    if diagnostic.get("cause_class") == "internal_error":
        # A refusal of the server itself (ownership not proven): its own words.
        diagnostic["detail"] = sanitize_error_message(str(exc))[:300]
    return diagnostic


def _held_object_report(entry: Mapping[str, Any], lease: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """What a caller is told about one test object of a lease: never the owner token."""

    view = entry.get("object_kind") == LEASE_OBJECT_KIND
    report: dict[str, Any] = {
        "kind": LEASE_OBJECT_KIND if view else "index",
        "name": f"{entry.get('target_schema')}."
        + str(entry.get("target_view") if view else entry.get("target_table")),
        "index_name": entry.get("index_name"),
    }
    if lease is None:
        return report
    held_since = entry.get("held_since_utc")
    report["held_since_utc"] = held_since
    try:
        since = datetime.fromisoformat(str(held_since))
        report["age_seconds"] = max(0, int((datetime.now(timezone.utc) - since).total_seconds()))
    except ValueError:
        report["age_seconds"] = None
    report["expires_at_utc"] = lease.get("expires_at_utc")
    report["lease_id"] = lease.get("lease_id")
    return report


def _tested_match_reason(subject: str, statuses: set[str], gaps: Sequence[str]) -> str:
    """Why a candidate whose results were tested under a preflight gap is performance-only."""

    return (
        f"{subject} improved the measured objective. Its results were tested against "
        "the original in one snapshot per parameter case and "
        + (
            "matched as multisets with duplicate counts"
            if statuses == {"match"}
            else "did not match for every case (" + ", ".join(sorted(statuses)) + ")"
        )
        + "; that is a test on today's data, and the static proof still has a gap ("
        + ", ".join(gaps)
        + "), so the result is performance-only."
    )
# compare_query_results stops after this many seconds unless the caller sets max_seconds.
_DEFAULT_COMPARISON_SECONDS = 120.0


def _view_recreate_sql(state: ViewCatalogState) -> str:
    """The statements that put back a view and its indexes as the catalog holds them.

    The inverse of a guarded DROP VIEW, for the admin action's rollback
    record. The clustered index comes first: the others need it.
    """

    statements = [str(state.definition or "").strip().rstrip(";") + ";"]
    for index in sorted(state.indexes, key=lambda item: item.index_id):
        keys = ", ".join(
            f"[{column.name}]" + (" DESC" if column.direction == "DESC" else "")
            for column in index.key_columns
        )
        include = (
            " INCLUDE (" + ", ".join(f"[{name}]" for name in index.include_columns) + ")"
            if index.include_columns
            else ""
        )
        kind = ("UNIQUE " if index.is_unique else "") + str(index.index_type).strip().upper()
        statements.append(
            f"CREATE {kind} INDEX [{index.name}] ON [{index.schema}].[{index.table}] "
            f"({keys}){include};"
        )
    return "\n".join(statements)


def _two_part_name(schema_name: str | None, object_name: str | None) -> tuple[str | None, str | None]:
    """Split ``schema.object`` (brackets allowed) when the schema was not given apart."""

    name = (object_name or "").strip()
    schema = (schema_name or "").strip() or None
    if not name:
        return schema, None
    parts = [part.strip().strip("[]") for part in name.split(".")]
    if len(parts) == 2 and all(parts):
        if schema is not None and schema.casefold() != parts[0].casefold():
            raise ValueError(
                f"schema_name is {schema} but the object name says {parts[0]}: pass one schema."
            )
        return parts[0], parts[1]
    return schema, name.strip("[]")


def _sample_seconds(sample: Mapping[str, Any]) -> float:
    """Wall seconds one measured run took: what a caller waits for."""

    values = [
        float(value)
        for value in (sample.get("client_elapsed_ms"), sample.get("elapsed_ms"))
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    ]
    return max(values) / 1000 if values else 0.0


def _index_experiment_failure(
    exc: Exception,
    *,
    step: str,
    query_timeout_seconds: int,
    create_failure: Mapping[str, Any] | None = None,
    definition_mismatch: bool = False,
    online_build: bool = False,
) -> dict[str, Any]:
    """The diagnostic of a failed index experiment: the step and the class of its cause.

    ``failed_step`` is baseline_before, create_index, verify_created_index,
    candidate_measure, baseline_after or drop_index. ``cause_class`` is
    timeout (with the limit and the setting that holds it), lock_wait_abort,
    connection_lost, definition_mismatch, engine_error or internal_error. The
    engine message is returned for the DDL steps only: that DDL is built here,
    the measured statement is the caller's.
    """

    diagnostic = extract_failure_diagnostic(exc, engine_text=step in _INDEX_DDL_STEPS)
    diagnostic["failed_step"] = step
    if diagnostic["diagnostic_code"] != "sql_execution_failed":
        return diagnostic
    failure = create_failure if step == "create_index" and create_failure else {}
    if "native_error_code" not in diagnostic and failure.get("error_number") is not None:
        diagnostic["native_error_code"] = failure["error_number"]
    if definition_mismatch:
        diagnostic["cause_class"] = "definition_mismatch"
    elif _is_timeout_error(exc):
        # Every statement of the experiment runs under the query timeout.
        diagnostic["cause_class"] = "timeout"
        diagnostic["timeout_seconds"] = query_timeout_seconds
        diagnostic["timeout_setting"] = "AZURE_SQL_QUERY_TIMEOUT_SECONDS"
    elif diagnostic.get("native_error_code") == 1222:
        diagnostic["cause_class"] = "lock_wait_abort"
        diagnostic["lock_timeout_seconds"] = query_timeout_seconds
        if step == "create_index" and online_build:
            # WAIT_AT_LOW_PRIORITY with ABORT_AFTER_WAIT = SELF ends the build.
            diagnostic["low_priority_wait_seconds"] = TEST_INDEX_LOW_PRIORITY_WAIT_MINUTES * 60
    elif failure.get("reason") == "connection_or_timeout":
        diagnostic["cause_class"] = "connection_lost"
    elif "engine_message" in diagnostic or _driver_exception(exc) is not None:
        diagnostic["cause_class"] = "engine_error"
    else:
        diagnostic["cause_class"] = "internal_error"
    return diagnostic


def _offline_build_report(tables: Sequence[_TestIndexTable]) -> dict[str, Any]:
    return {
        "reason": "clustered_columnstore_table",
        "tables": [f"{table.schema}.{table.table}" for table in tables],
        "note": _COLUMNSTORE_OFFLINE_BUILD_NOTE,
    }


@dataclass
class _IndexSetMember:
    """One index of a test-index set and where it is in the create and cleanup sequence."""

    schema: str
    table: str
    key_columns: list[str]
    include_columns: list[str] | None
    filter_definition: str | None
    is_unique: bool
    data_compression: str | None
    candidate: IndexCandidate
    index_ddl: str
    rollback_ddl: str
    create_key: str
    cleanup_key: str
    table_facts: _TestIndexTable
    # False on a clustered columnstore table: the engine builds that index offline only.
    online: bool
    # What the lease holds for this index. The lease of a one-index set keeps
    # these fields at its top level, as every lease before sets did.
    state: dict[str, Any] = field(default_factory=dict)
    create_attempted: bool = False
    ownership_recorded: bool = False
    # True once an unverified create was dropped or recorded through its owner
    # marker, or the engine reported a definite rollback.
    create_outcome_settled: bool = False
    # True when the index may exist and this run could not drop it.
    left_behind: bool = False
    built: dict[str, Any] | None = None
    # True when an earlier call of the session built this index and kept it.
    reused: bool = False
    # True once this run dropped the index of a lease that holds objects.
    released: bool = False

    @property
    def index_name(self) -> str:
        return str(self.candidate.index_name)

    @property
    def fingerprint(self) -> str:
        return self.candidate.definition_fingerprint
# Rows of a table: its heap or clustered index, all partitions.
_TABLE_ROW_COUNT_SQL = """
SELECT SUM(ps.row_count) AS row_count
FROM sys.dm_db_partition_stats AS ps
WHERE ps.object_id = OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?))
  AND ps.index_id IN (0, 1);
"""
# One row per table: its catalog spelling, whether its clustered index is
# columnstore (type 5), and its partition scheme and column.
_TEST_INDEX_TABLE_SQL = """
SELECT TOP (2)
    s.name AS schema_name,
    t.name AS table_name,
    CASE WHEN EXISTS (
        SELECT 1
        FROM sys.indexes AS cs
        WHERE cs.object_id = t.object_id
          AND cs.type = 5
    ) THEN 1 ELSE 0 END AS has_clustered_columnstore_index,
    ps.name AS partition_scheme_name,
    pf.name AS partition_function_name,
    pc.name AS partitioning_column
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
LEFT JOIN sys.indexes AS base
    ON base.object_id = t.object_id AND base.index_id IN (0, 1)
LEFT JOIN sys.partition_schemes AS ps ON ps.data_space_id = base.data_space_id
LEFT JOIN sys.partition_functions AS pf ON pf.function_id = ps.function_id
LEFT JOIN sys.index_columns AS ic
    ON ic.object_id = base.object_id
    AND ic.index_id = base.index_id
    AND ic.partition_ordinal = 1
    AND ps.data_space_id IS NOT NULL
LEFT JOIN sys.columns AS pc
    ON pc.object_id = ic.object_id AND pc.column_id = ic.column_id
WHERE LOWER(s.name) = LOWER(?)
  AND LOWER(t.name) = LOWER(?)
ORDER BY s.name, t.name
"""
TEST_INDEX_OWNER_PROPERTY = "AzureSqlMcpLeaseOwner"
TEST_INDEX_DEFINITION_PROPERTY = "AzureSqlMcpDefinitionFingerprint"
_PLAIN_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_INDEX_OWNER_PROOF = re.compile(r"^[A-Za-z0-9_.:-]{16,200}$")
_IDEMPOTENCY_DIGEST_PATTERN = re.compile(r"^idempotency-v1:[0-9a-f]{64}$")
_LEARNING_SKILL_VERSIONS = {
    "sql-health-triage": "1.1.0",
    "sql-optimizer": "3.0.0",
    "sql-plan-enforcer": "1.1.0",
    "sql-index-manager": "2.1.0",
}
_PENDING_INDEX_OWNERSHIP_SQL = """
SELECT
    i.index_id,
    CONVERT(nvarchar(4000), owner_marker.value) AS owner_marker,
    CONVERT(nvarchar(4000), definition_marker.value) AS definition_marker
FROM sys.indexes AS i WITH (UPDLOCK, HOLDLOCK)
INNER JOIN sys.tables AS t WITH (UPDLOCK, HOLDLOCK)
    ON t.object_id = i.object_id
INNER JOIN sys.schemas AS s WITH (UPDLOCK, HOLDLOCK)
    ON s.schema_id = t.schema_id
LEFT JOIN sys.extended_properties AS owner_marker WITH (UPDLOCK, HOLDLOCK)
    ON owner_marker.class = 7
    AND owner_marker.major_id = i.object_id
    AND owner_marker.minor_id = i.index_id
    AND owner_marker.name = ?
LEFT JOIN sys.extended_properties AS definition_marker WITH (UPDLOCK, HOLDLOCK)
    ON definition_marker.class = 7
    AND definition_marker.major_id = i.object_id
    AND definition_marker.minor_id = i.index_id
    AND definition_marker.name = ?
WHERE s.name = ?
  AND t.name = ?
  AND i.name = ?
  AND i.index_id > 0
"""
_SESSION_WORKFLOW_TOOLS = frozenset(
    {
        "benchmark_tuning_candidate",
        "benchmark_index_candidate",
        "benchmark_indexed_view_candidate",
        "benchmark_query_rewrite",
        "apply_prepared_view_change",
        "verify_view_change",
        "rollback_view_change",
    }
)
_EVIDENCE_WORKFLOW_TOOLS = frozenset(
    {"collect_performance_evidence", "tune_query"}
)
_CATALOG_READ_TOOLS = frozenset(
    {
        "execute_sql",
        "list_schemas",
        "list_objects",
        "search_objects",
        "get_object_details",
        "get_column_statistics",
        "get_dependencies",
        "get_table_stats",
        "capture_schema_snapshot",
        "capture_index_review_snapshot",
        "review_index_portfolio",
        "get_index_review",
    }
)
_CATALOG_PAIR_TOOLS = frozenset(
    {
        "compare_schemas",
        "generate_migration_script",
    }
)


def _driver_exception(exc: BaseException) -> BaseException | None:
    current: BaseException | None = exc
    while current is not None:
        if type(current).__module__.startswith(("mssql_python", "pyodbc")):
            return current
        current = current.__cause__ or current.__context__
    return None


def _label_unknown_parameters(digest: dict[str, Any], unknown: list[str]) -> None:
    """The typed estimate leaves some contract parameters out of OPTIMIZE FOR
    (xml), so its plan lists them as local variables; they are parameters."""

    names = {name.casefold() for name in unknown}
    for entry in digest.get("statements") or ():
        local = entry.get("local_variables") or []
        found = [name for name in local if name.casefold() in names]
        if not found:
            continue
        entry["unknown_parameters"] = found
        entry["unknown_parameters_note"] = (
            "Caller parameters this estimate compiled as unknown (not in OPTIMIZE FOR); "
            "see summary.estimated_plan_basis. Use analyze=true for their compiled values."
        )
        remaining = [name for name in local if name.casefold() not in names]
        if remaining:
            entry["local_variables"] = remaining
        else:
            entry.pop("local_variables", None)
            entry.pop("local_variables_note", None)


def _tool_failure_diagnostic(exc: Exception) -> dict[str, Any]:
    driver_exc = _driver_exception(exc)
    failure_diagnostic = extract_failure_diagnostic(exc)
    # The incident log's classification names failures no SQL error caused;
    # a recognised engine diagnostic (214) in the text still wins.
    code = tool_failure_code(exc)
    if code is not None and (
        code == "verification_failed"
        or failure_diagnostic["diagnostic_code"] == "sql_execution_failed"
    ):
        return {"diagnostic_code": code}
    if driver_exc is not None:
        # mssql-python text has no bracketed SQLSTATE; map its driver class.
        native, sqlstate = native_codes(driver_exc)
        if sqlstate is not None:
            failure_diagnostic.setdefault("sqlstate", sqlstate)
        if native is not None:
            failure_diagnostic.setdefault("native_error_code", native)
    if _is_transient(exc):
        # Skills retry these before they report a blocker.
        failure_diagnostic["transient"] = True
    return failure_diagnostic


class _SanitizingToolManager(ToolManager):
    """Return stable validation failures without echoing untrusted arguments."""

    def __init__(
        self,
        warn_on_duplicate_tools: bool = True,
        *,
        incidents: IncidentLog | None = None,
    ) -> None:
        super().__init__(warn_on_duplicate_tools)
        self.incidents = incidents or IncidentLog(None, disabled_reason="disabled_by_config")
        # Package tool names, including ones this profile or access mode hides.
        self.known_tool_names: frozenset[str] = frozenset()
        # Applied to every tool result: the one place no tool can bypass.
        self.finish_response: Callable[[Any], Any] = lambda payload: payload

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any],
        context=None,
        convert_result: bool = False,
    ) -> Any:
        # Any other name is agent text and may hold a database or table name.
        label = name if name in self.known_tool_names else "unregistered"
        call = self.incidents.begin(label, arguments, context)
        try:
            raw = await self._call_tool_checked(name, arguments, context)
            result = self._convert(name, raw) if convert_result else raw
        except BaseException as exc:
            self.incidents.end(call, exc=exc)
            raise
        self.incidents.end(call, result=raw)
        return result

    async def _call_tool_checked(
        self,
        name: str,
        arguments: dict[str, Any],
        context=None,
    ) -> Any:
        try:
            result = await super().call_tool(
                name,
                arguments,
                context=context,
                convert_result=False,
            )
        except ToolError as exc:
            validation_error = exc.__cause__
            if not isinstance(validation_error, ValidationError):
                raise
            issues = [
                {
                    "path": ".".join(str(part) for part in error.get("loc", ())),
                    "code": str(error.get("type") or "invalid"),
                    "message": _validation_issue_message(
                        str(error.get("type") or "invalid")
                    ),
                }
                for error in validation_error.errors(
                    include_url=False,
                    include_context=False,
                    include_input=False,
                )
            ]
            # When an argument name was the problem, name what the tool takes,
            # so the corrected call needs no second look-up.
            tool = self.get_tool(name)
            accepted = sorted(tool.parameters.get("properties", {})) if tool is not None else []
            details: dict[str, Any] = {"issues": issues}
            # A tool with one argument needs no list: its envelope stays as it was.
            if len(accepted) > 1 and any(
                issue["code"] in {"extra_forbidden", "missing"} for issue in issues
            ):
                details["accepted_arguments"] = accepted
                for issue in issues:
                    if issue["code"] == "extra_forbidden" and "." not in issue["path"]:
                        close = difflib.get_close_matches(
                            issue["path"], accepted, n=1, cutoff=0.5
                        )
                        if close:
                            issue["did_you_mean"] = close[0]
            payload = ErrorPayload(
                code="invalid_arguments",
                message="Tool arguments failed validation.",
                details=details,
            ).as_dict()
            raise ToolError(
                json.dumps(
                    payload,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            ) from None
        return self.finish_response(apply_result_status(name, result))

    def _convert(self, name: str, result: Any) -> Any:
        tool = self.get_tool(name)
        if tool is None:  # pragma: no cover - super().call_tool already resolved it
            raise ToolError(f"Unknown tool: {name}")
        try:
            return tool.fn_metadata.convert_result(result)
        except Exception as exc:
            raise ToolError(f"Error executing tool {name}: {exc}") from exc


def _validation_issue_message(code: str) -> str:
    messages = {
        "missing": "Field required.",
        "extra_forbidden": "Extra arguments are not permitted.",
        "literal_error": "Value is not one of the permitted choices.",
        "greater_than_equal": "Value is below the permitted minimum.",
        "less_than_equal": "Value is above the permitted maximum.",
        "int_parsing": "Value must be an integer.",
        "string_type": "Value must be a string.",
        "string_too_short": "Value is below the permitted minimum length.",
        "string_too_long": "Value is above the permitted maximum length.",
        "bool_parsing": "Value must be a boolean.",
        "list_type": "Value must be an array.",
        "dict_type": "Value must be an object.",
    }
    return messages.get(code, "Value failed validation.")


def _view_change_request_fingerprint(
    database_fingerprint_value: str,
    request: ViewChangeRequest,
) -> str:
    return fingerprint_json(
        {
            "database_fingerprint": database_fingerprint_value,
            "database_name": request.database_name,
            "schema_name": request.schema_name,
            "view_name": request.view_name,
            "definition": request.definition,
            "operation": request.operation.casefold(),
            "reviewed_intent": request.reviewed_intent,
            "idempotency_key": _view_idempotency_identity(request.idempotency_key),
            "indexed_view": request.indexed_view,
            "schema_bound": request.schema_bound,
        }
    )


def _view_idempotency_identity(value: str | None) -> str | None:
    if value is None:
        return None
    if _IDEMPOTENCY_DIGEST_PATTERN.fullmatch(value):
        return value
    digest = hashlib.sha256(
        f"idempotency-v1:view-change.intent:{value}".encode("utf-8")
    ).hexdigest()
    return f"idempotency-v1:{digest}"


def _auth_settings(config: ServerConfig) -> AuthSettings:
    host = config.transport.host
    if host in {"0.0.0.0", "::"}:
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    resource_url = AnyHttpUrl(f"http://{host}:{config.transport.port}")
    return AuthSettings(
        issuer_url=resource_url,
        resource_server_url=resource_url,
        required_scopes=["azure-sql-mcp"],
    )


MAX_ANALYZED_PLAN_XML_CHARS = 8_000_000
# Above this size the parse and the rules run in a worker thread, so one
# large plan does not stall every other tool call on the event loop.
LARGE_PLAN_XML_CHARS = 500_000
MAX_PARTITION_COLUMN_LOOKUPS = 5
MAX_UNIQUE_KEY_LOOKUPS = 5
_PARTITION_COLUMN_SQL = """
SELECT s.name AS schema_name, t.name AS table_name, c.name AS partitioning_column
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
JOIN sys.indexes AS i ON i.object_id = t.object_id AND i.index_id IN (0, 1)
JOIN sys.partition_schemes AS ps ON ps.data_space_id = i.data_space_id
JOIN sys.index_columns AS ic
  ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.partition_ordinal = 1
JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE t.object_id = OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?));
"""


def _compact_plan_findings(analysis: dict[str, Any], *, limit: int = 15) -> dict[str, Any]:
    findings = analysis.get("findings") or []
    compact = {
        "plan_kind": analysis.get("plan_kind"),
        "finding_counts": analysis.get("finding_counts", {}),
        "families": analysis.get("families", {}),
        "findings": findings[:limit],
        "truncated": bool(analysis.get("truncated")) or len(findings) > limit,
    }
    # How the list is ordered, and what a partial (running) plan cannot show.
    for key in ("ranking_basis", "note", "in_flight", "in_flight_note"):
        if analysis.get(key) is not None:
            compact[key] = analysis[key]
    return compact


def _branch_condition_text(conditions: Sequence[Mapping[str, Any]]) -> str:
    """The IF conditions that select a batch branch, as one predicate."""

    parts = [
        f"({item['condition']})" if item.get("is_true") else f"NOT ({item['condition']})"
        for item in conditions
    ]
    return " AND ".join(parts) if parts else "always"


def _one_parameter_shape(
    parameter_case: ParameterCaseInput | Mapping[str, Any] | None,
    parameter_values: dict[str, Any] | None,
    parameter_types: dict[str, str] | None,
) -> dict[str, Any] | None:
    """One parameter case from either spelling: parameter_case, or values and types."""

    case = _parameter_case_payload(parameter_case)
    if case is not None and (parameter_values or parameter_types):
        raise ValueError(
            "Pass parameter_case or parameter_values with parameter_types, not both."
        )
    if case is not None:
        return case
    if not parameter_values and not parameter_types:
        return None
    return {
        "name": "default",
        "values": dict(parameter_values or {}),
        "types": dict(parameter_types or {}),
        "weight": 1.0,
    }


_AGGREGATE_DECISION_KEYS = (
    "sample_count",
    "elapsed_ms",
    "cpu_ms",
    "logical_reads",
    "lob_logical_reads",
    "physical_reads",
    "actual_rows",
    "client_elapsed_ms",
    "noise_ratio",
    "plan_stable",
)
_PREFLIGHT_DECISION_KEYS = (
    "classification",
    "direct_snapshot_supported",
    "risk_codes",
    "total_order_gaps",
)
_LEASE_DECISION_KEYS = ("lease_id", "status", "index_name", "expires_at_utc")


def _compact_aggregate(aggregate: Any) -> Any:
    if not isinstance(aggregate, Mapping) or "sample_count" not in aggregate:
        return aggregate
    return {
        key: aggregate[key]
        for key in _AGGREGATE_DECISION_KEYS
        if aggregate.get(key) is not None
    }


def _compact_benchmark_payload(payload: Any) -> Any:
    """A benchmark result with the numbers a decision needs.

    Left out: per-run values, spread, metric coverage, result digests, the
    full preflight and the duplicate ``metrics`` block. ``detail="full"`` on
    the tool returns them; get_tuning_session(view="full") holds them too.
    """

    if not isinstance(payload, Mapping):
        return payload
    compact = {
        # The verdict, the equivalence status and, per case, the speedup and
        # the medians of each side: what a decision reads first.
        "decision": compact_benchmark_result(payload)["decision"],
        **{
            key: value
            for key, value in payload.items()
            if key not in {"metrics", "parameter_case_receipts"}
        },
    }
    # An index benchmark has one preflight, not one per side.
    one_preflight = payload.get("equivalence_preflight")
    if isinstance(one_preflight, Mapping) and isinstance(one_preflight.get("risk_codes"), list):
        compact["decision"]["equivalence"]["risk_codes"] = [
            str(code) for code in one_preflight["risk_codes"]
        ]
    results = payload.get("parameter_results")
    if isinstance(results, list):
        slim_results = []
        for item in results:
            if not isinstance(item, Mapping):
                slim_results.append(item)
                continue
            slim = {
                key: _compact_aggregate(value)
                for key, value in item.items()
                # The verdict and the margin are in decision.cases.
                if key not in {"comparison_margin", "decision_basis"}
            }
            for name in ("plan_delta", "baseline_after_plan_delta"):
                delta = slim.get(name)
                if isinstance(delta, Mapping):
                    slim[name] = _compact_plan_delta(delta, operators=name == "plan_delta")
            uses = slim.get("plan_use")
            if isinstance(uses, list):
                slim["plan_use"] = {
                    "runs": len(uses),
                    "runs_using_index": sum(
                        1 for use in uses if isinstance(use, Mapping) and use.get("used") is True
                    ),
                }
            slim_results.append(slim)
        compact["parameter_results"] = slim_results
    equivalence = payload.get("equivalence")
    if isinstance(equivalence, list):
        compact["equivalence"] = [
            {
                key: value
                for key, value in item.items()
                if key not in _EQUIVALENCE_DETAIL_KEYS
                # A reason is kept where the case did not simply match.
                and (key != "reason" or item.get("status") != "match")
            }
            if isinstance(item, Mapping)
            else item
            for item in equivalence
        ]
    preflight = payload.get("equivalence_preflight")
    if isinstance(preflight, Mapping):
        compact["equivalence_preflight"] = {
            key: preflight[key]
            for key in _PREFLIGHT_DECISION_KEYS
            if key in preflight and (preflight[key] or key != "total_order_gaps")
        }
    lease = payload.get("lease")
    if isinstance(lease, Mapping):
        compact["lease"] = {key: lease[key] for key in _LEASE_DECISION_KEYS if key in lease}
    for key in ("lineage", "cross_case_result"):
        if compact.get(key) is None:
            compact.pop(key, None)
    compact["detail_note"] = (
        'detail="full" adds per-run values, spread, the operators of each plan, '
        "result digests and the whole preflight."
    )
    return compact


# Per-case comparison fields that only detail="full" returns.
_EQUIVALENCE_DETAIL_KEYS = frozenset(
    {
        "result_evidence",
        "result_sets",
        "basis",
        "columns_match",
        "type_metadata_available",
        "positional_metadata_available",
        "rows_match",
        "truncated",
        "baseline_result_set_count",
        "candidate_result_set_count",
        "execution_count_is_conservative",
        "baseline_duplicate_rows",
        "candidate_duplicate_rows",
    }
)


def _compact_plan_delta(delta: Mapping[str, Any], *, operators: bool) -> dict[str, Any]:
    """Whether the plan changed and, for the candidate, its three costliest operators."""

    compact: dict[str, Any] = {"plan_changed": delta.get("plan_changed")}
    candidate = delta.get("candidate")
    if operators and isinstance(candidate, Mapping):
        compact["candidate_operators"] = [
            " ".join(
                str(operator[key]) for key in ("operator", "object") if operator.get(key)
            )
            for operator in candidate.get("top_operators") or ()
            if isinstance(operator, Mapping)
        ]
    return compact


def _capabilities_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    """What a tuning session needs from check_capabilities, in under 2 KB.

    The contract flags, the local policy and the view-experiment verdict are
    kept whole. The platform is four facts, Query Store its state, and each
    probe one word; a probe that failed keeps its whole entry.
    """

    # Every small top-level field is kept: the result status and its reason,
    # the contract flags, the local policy.
    summary: dict[str, Any] = {
        key: value
        for key, value in payload.items()
        if key not in {"checks", "azure_sql_database", "query_store_replicas"}
    }
    platform = payload.get("azure_sql_database")
    data = platform.get("data") if isinstance(platform, Mapping) else None
    if isinstance(platform, Mapping) and not isinstance(data, Mapping):
        # The platform read failed: its error is the fact to report.
        summary["azure_sql_database"] = platform
    if isinstance(data, Mapping):
        summary["platform"] = {
            key: data[key]
            for key in (
                "platform_supported",
                "engine_edition",
                "service_objective",
                "compatibility_level",
            )
            if key in data
        }
    probes = payload.get("checks")
    if isinstance(probes, Mapping):
        query_store = probes.get("query_store")
        if isinstance(query_store, Mapping):
            detail = query_store.get("detail")
            summary["query_store"] = {
                "available": query_store.get("available"),
                **(
                    {key: detail[key] for key in ("enabled", "state") if key in detail}
                    if isinstance(detail, Mapping)
                    else {}
                ),
            }
        summary["checks"] = {
            name: (
                "available"
                if isinstance(probe, Mapping) and probe.get("available") is True
                else probe
            )
            for name, probe in probes.items()
        }
    summary["detail_note"] = (
        'detail="full" adds the probe details, the platform report and the Query Store '
        "replica dimension."
    )
    return summary


def _lead_with_requested_queries(
    report: Mapping[str, Any],
    query_ids: Sequence[int],
    *,
    include_others: bool = False,
) -> dict[str, Any]:
    """Lead the report with the queries the caller named.

    The design for each named query alone and the count of recommendations
    that serve them come first. The recommendations of other queries are
    counted, and returned only with ``include_others``: they were most of a
    16 to 34 KB report that the caller did not ask for.
    """

    requested = {int(query_id) for query_id in query_ids}
    recommendations = report.get("recommendations")
    if not isinstance(recommendations, list):
        return dict(report)

    def serves(recommendation: Any) -> bool:
        supporting = (
            recommendation.get("supporting_queries") if isinstance(recommendation, Mapping) else None
        )
        return any(
            isinstance(item, Mapping) and item.get("query_id") in requested
            for item in supporting or ()
        )

    marked = [
        {**item, "serves_requested_query": serves(item)} if isinstance(item, Mapping) else item
        for item in recommendations
    ]
    for_them = [item for item in marked if isinstance(item, Mapping) and item["serves_requested_query"]]
    others = [item for item in marked if item not in for_them]
    ids = ", ".join(str(query_id) for query_id in sorted(requested))
    if for_them:
        message = (
            f"{len(for_them)} recommendation(s) serve query id {ids}; they come first."
        )
    else:
        message = (
            f"No recommendation serves query id {ids}: the review found no index change "
            "that helps."
        )
    requested_queries: dict[str, Any] = {
        "query_ids": sorted(requested),
        "recommendations_for_them": len(for_them),
    }
    if others and not include_others:
        requested_queries["other_recommendations_omitted"] = len(others)
        message += (
            f" {len(others)} recommendation(s) from other queries of the workload are "
            "left out; pass include_other_recommendations=true to see them."
        )
    elif others:
        message += f" The {len(others)} recommendation(s) after them come from other queries."
    requested_queries["message"] = message
    return {
        "requested_queries": requested_queries,
        "requested_query_design": report.get("requested_query_design", []),
        **{key: value for key, value in report.items() if key != "requested_query_design"},
        "recommendations": [*for_them, *(others if include_others else [])],
    }


def _summarize_evidence_section(section: Any) -> Any:
    """Status, gaps and row counts of one evidence section, without its rows."""

    if not isinstance(section, Mapping):
        return section
    summary: dict[str, Any] = {
        key: value
        for key, value in section.items()
        if not isinstance(value, (Mapping, list)) or key == "gaps"
    }
    nested = section.get("data")
    data: Mapping[str, Any] = nested if isinstance(nested, Mapping) else section
    row_counts = {
        key: len(value) for key, value in data.items() if isinstance(value, list) and key != "gaps"
    }
    if row_counts:
        summary["row_counts"] = row_counts
    identity = data.get("identity")
    if isinstance(identity, Mapping):
        summary["identity"] = {
            key: value for key, value in identity.items() if not isinstance(value, (Mapping, list))
        }
    return summary


def _compact_evidence_payload(payload: Any) -> Any:
    if not isinstance(payload, Mapping):
        return payload
    compact = dict(payload)
    evidence = payload.get("evidence")
    if isinstance(evidence, Mapping):
        compact["evidence"] = {
            key: value for key, value in evidence.items() if not isinstance(value, (Mapping, list))
        }
    sections = payload.get("sections")
    if isinstance(sections, Mapping):
        compact["sections"] = {
            name: _summarize_evidence_section(section) for name, section in sections.items()
        }
    compact["detail"] = "summary"
    compact["detail_note"] = (
        "Sections show status, gaps and row counts. The rows are stored with the "
        "evidence: call again with detail=full, or read get_performance_case."
    )
    return compact


_CHARACTER_TYPES = frozenset({"char", "varchar", "binary", "varbinary"})
_UNICODE_TYPES = frozenset({"nchar", "nvarchar"})
_PRECISION_SCALE_TYPES = frozenset({"decimal", "numeric"})
_SCALE_TYPES = frozenset({"datetime2", "datetimeoffset", "time"})


def _declared_column_type(column: Mapping[str, Any]) -> dict[str, Any]:
    """The type as DDL declares it. sys.columns.max_length is bytes: 80 for nvarchar(40)."""

    data_type = str(column.get("data_type") or "").casefold()
    max_length = column.get("max_length")
    if data_type in _CHARACTER_TYPES | _UNICODE_TYPES and isinstance(max_length, int):
        if max_length == -1:
            return {"declared_type": f"{data_type}(max)"}
        length = max_length // 2 if data_type in _UNICODE_TYPES else max_length
        return {"length_chars": length, "declared_type": f"{data_type}({length})"}
    if data_type in _PRECISION_SCALE_TYPES:
        return {"declared_type": f"{data_type}({column.get('precision')},{column.get('scale')})"}
    if data_type in _SCALE_TYPES:
        return {"declared_type": f"{data_type}({column.get('scale')})"}
    return {}


def _plan_logical_reads(parsed: ParsedPlan | None) -> float | None:
    """Logical reads the actual plan's operators report; None when it reports none."""

    if parsed is None:
        return None
    reads = [
        node.logical_reads
        for statement in parsed.statements
        for node in statement.nodes
        if node.logical_reads is not None
    ]
    return sum(reads) if reads else None


_DIGEST_HELD_SUMMARY_KEYS = frozenset({"statements", "parameters"})
_DIGEST_HELD_METRIC_KEYS = frozenset(
    {
        "statement_metrics",
        "root_operator_metrics",
        "runtime_counter_count",
        "statement_metric_count",
        "user_select_count",
        # Reported once, at the top level, as execution_ms with its provenance.
        "measured_wall_elapsed_ms",
        "measured_wall_elapsed_source",
    }
)


def _drop_zero_counters(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _drop_zero_counters(child)
            for key, child in value.items()
            if not (isinstance(child, (int, float)) and not isinstance(child, bool) and child == 0)
        }
    if isinstance(value, list):
        return [_drop_zero_counters(item) for item in value]
    return value


def _slim_explain_summary(summary: Mapping[str, Any], *, digest_present: bool) -> dict[str, Any]:
    """The default explain summary: nothing the digest already holds, nothing empty."""

    slim: dict[str, Any] = {}
    for key, value in summary.items():
        if digest_present and key in _DIGEST_HELD_SUMMARY_KEYS:
            continue
        if value is None or (isinstance(value, (list, dict)) and not value):
            continue
        if key == "actual_metrics" and isinstance(value, Mapping):
            value = {
                name: metric
                for name, metric in value.items()
                if metric is not None
                and not (digest_present and name in _DIGEST_HELD_METRIC_KEYS)
                # A read counter of zero says nothing; zero rows or zero ms does.
                and not (name.endswith("reads") and metric == 0)
            }
        elif key == "statistics_io":
            value = _drop_zero_counters(value)
        slim[key] = value
    return slim


class AzureSqlMcpApplication:
    def __init__(self, config: ServerConfig):
        self.config = config
        self._startup_timestamp = datetime.now(timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        try:
            self._package_version = package_version("azure-sql-mcp")
        except PackageNotFoundError:  # pragma: no cover - source-only execution
            self._package_version = "unknown"
        token_verifier = (
            StaticBearerTokenVerifier(config.mcp_bearer_token)
            if config.mcp_bearer_token
            else None
        )
        self.mcp = FastMCP(
            "azure-sql-mcp",
            instructions=SERVER_INSTRUCTIONS,
            token_verifier=token_verifier,
            auth=_auth_settings(config) if token_verifier else None,
        )
        # Budget and argument-name lookups may raise; the log treats that as unknown.
        self.incidents = IncidentLog.from_config(
            config,
            server_version=self._package_version,
            budget_for=lambda tool, arguments: self._timeout_for_tool(
                tool,
                self.config.validate_database_name(arguments.get("database_name")),
            ),
            declared_for=lambda tool: self.mcp._tool_manager._tools[tool].parameters[
                "properties"
            ],
            on_tick=lambda: self.pool.note_leaked_connections(),
        )
        tool_manager = _SanitizingToolManager(
            warn_on_duplicate_tools=self.mcp.settings.warn_on_duplicate_tools,
            incidents=self.incidents,
        )
        self.mcp._tool_manager = tool_manager
        tool_manager.finish_response = self._finish_response

        self.database_policy = load_database_policy_or_deny(
            config.database_policy_file
        )
        authenticator = AzureSqlAuthenticator(config)
        pool = ConnectionPool(
            config,
            authenticator,
            application_intents=self.database_policy.application_intents(),
        )
        executor = AzureSqlExecutor(config, authenticator, pool)
        validator = SafeSqlValidator()

        self.pool = pool

        self.executor = executor
        self.validator = validator
        self.equivalence_preflight = EquivalencePreflightService(executor=executor)
        self.artifacts = ArtifactStore()
        self.admin_policy = AdminPolicy(config)
        self.introspection = IntrospectionService(executor)
        self.query_store = QueryStoreService(executor)
        self.plans = PlansService(executor, validator)
        self.recommendations = IndexRecommendationService(executor)
        self.query_index_analysis = QueryIndexAnalysisService(executor, validator)
        self.health = HealthService(executor, self.query_store)
        self.sessions = SessionsService(executor)
        self.schema_compare = SchemaCompareService(executor)
        self.index_optimizer = IndexOptimizer(executor, validator)
        self.workload_index_advisor = WorkloadIndexAdvisor(executor)
        self.query_store_trends = QueryStoreTrendService(executor)
        self.version_store = VersionStoreService(executor)
        self.plan_sources = PlanSourceService(executor)
        self.wait_stats = WaitStatsService(executor)
        self.lock_diagnostics = LockDiagnosticsService(executor)
        self.tempdb_memory = TempdbMemoryService(executor)
        self.resource_governance = ResourceGovernanceService(executor)
        self.diagnostics = DiagnosticQueryService(executor)
        self.plan_cache = PlanCacheService(executor)
        self.platform_capabilities = PlatformCapabilitiesService(executor)
        self.query_regression = QueryRegressionService(executor)
        self.plan_enforcement = PlanEnforcementService(
            executor,
            self.query_regression,
            self.admin_policy,
        )
        self.param_binding = ParameterBindingService(executor)
        self.performance_store = (
            PerformanceStore(db_path=":memory:")
            if config.performance_state_dir == ":memory:"
            else PerformanceStore(config.performance_state_dir)
        )
        self.index_history_repository = SqlIndexHistoryRepository(
            executor,
            self.database_policy,
        )
        self.index_review = IndexReviewService(
            executor,
            self.index_history_repository,
            database_policy=self.database_policy,
            recommendations=self.recommendations,
            query_store=self.query_store,
        )
        self.learning_store: LearningStore | None = None
        self.learning_service: LearningService | None = None
        if config.transport.mode == TransportMode.STDIO:
            try:
                self.learning_store = (
                    LearningStore(db_path=":memory:")
                    if config.performance_state_dir == ":memory:"
                    else LearningStore(config.performance_state_dir)
                )
                self.learning_service = LearningService(
                    self.learning_store,
                    evidence_validator=self._learning_evidence_ref_is_valid,
                )
            except (LearningStoreError, OSError, sqlite3.Error) as exc:
                logger.warning(
                    "Learning state is unavailable; continuing with the static MCP surface.",
                    extra={
                        "error_type": type(exc).__name__,
                        "error": sanitize_error_message(str(exc)),
                    },
                )
                self.learning_store = None
                self.learning_service = None
        self.tuning_sessions = TuningSessionStateMachine(self.performance_store)
        self.performance_workflows = PerformanceWorkflowService(
            executor=executor,
            plans=self.plans,
            validator=validator,
            store=self.performance_store,
            sessions=self.tuning_sessions,
            database_policy=self.database_policy,
            row_limit=config.row_limit,
            parameter_binder=self._bind_performance_parameters,
            collector_timeout_seconds=config.query_timeout_seconds + 5,
            comparison_row_limit=config.comparison_row_limit,
            server_name=config.server,
            allow_legacy_state=bool(config.legacy_state_server_binding),
            equivalence_analyzer=self._check_equivalence_preflight,
        )
        self.plan_actions = PlanActionService(
            config=config,
            executor=executor,
            admin_policy=self.admin_policy,
            database_policy=self.database_policy,
            store=self.performance_store,
        )
        self.view_workflows = ViewWorkflowService(
            executor,
            self.database_policy,
            self.admin_policy,
        )
        self._prepared_view_changes: dict[str, PreparedViewChange] = {}
        self.capabilities = CapabilityService(
            executor,
            self.query_store,
            self.plans,
            self.recommendations,
        )

        self.column_statistics = ColumnStatisticsService(executor)
        self.memory_pressure = MemoryPressureService(executor)
        self.elastic_pool = ElasticPoolService(executor)
        self.columnstore_health = ColumnstoreHealthService(executor)
        self.database_baseline = DatabaseBaselineService(executor, self.query_store)
        self.capacity_assessment = CapacityAssessmentService(
            executor, memory_pressure=self.memory_pressure
        )
        self.application_patterns = ApplicationPatternService(
            query_store=self.query_store, sessions=self.sessions
        )
        self.database_diagnosis = DatabaseDiagnosisService(
            resource_governance=self.resource_governance,
            wait_stats=self.wait_stats,
            sessions=self.sessions,
            query_store=self.query_store,
            query_store_trends=self.query_store_trends,
            version_store=self.version_store,
            memory_pressure=self.memory_pressure,
            tempdb_memory=self.tempdb_memory,
            elastic_pool=self.elastic_pool,
            columnstore_health=self.columnstore_health,
            plan_cache=self.plan_cache,
            baseline=self.database_baseline,
            executor=executor,
            # One slow source must not hold the whole diagnosis.
            source_timeout_seconds=config.query_timeout_seconds,
        )
        # One per process: a restart cancels pending confirmations.
        self.confirmation_tokens = ConfirmationTokens()
        self._loki_settings = parse_loki_settings(os.environ)
        self._history_tokens: dict[str, EntraTokenProvider] = {}
        self._register_tools()
        if self.learning_service is not None:
            self._register_learning_tools()
        self._register_incident_tools()
        tool_manager.known_tool_names = frozenset(
            [tool.name for tool in tool_manager.list_tools()]
        ) | frozenset(TOOL_GROUPS)
        self._prune_disabled_tools()
        self._enforce_strict_tool_argument_models()
        register_resources(
            self.mcp,
            self.config,
            self.introspection,
            self.artifacts,
            database_policy=self.database_policy,
        )
        register_prompts(self.mcp, self.config)
        if self.config.schema_profile == "portable":
            self._install_portable_tool_listing()

    def _install_portable_tool_listing(self) -> None:
        """Serve strict-client input schemas on tools/list without touching validation."""

        list_raw_tools = self.mcp.list_tools

        async def list_portable_tools() -> list[Any]:
            return [
                tool.model_copy(update={"inputSchema": portable_input_schema(tool.inputSchema)})
                for tool in await list_raw_tools()
            ]

        self.mcp._mcp_server.list_tools()(list_portable_tools)

    def _prune_disabled_tools(self) -> None:
        """Remove tools that are not in the configured tool_groups."""
        registered = [t.name for t in self.mcp._tool_manager.list_tools()]
        for name in registered:
            if not self.config.is_tool_enabled(name):
                self.mcp.remove_tool(name)
                logger.debug("Pruned tool '%s' (not in active tool groups)", name)

    def _enforce_strict_tool_argument_models(self) -> None:
        """Make FastMCP's generated argument models reject unknown fields."""

        for tool in self.mcp._tool_manager.list_tools():
            argument_model = tool.fn_metadata.arg_model
            argument_model.model_config["extra"] = "forbid"
            argument_model.model_rebuild(force=True)
            tool.parameters = argument_model.model_json_schema()

    def _register_tools(self) -> None:
        @self.mcp.tool(
            description=(
                "Return DB-free runtime identity, registered-tool capabilities, and "
                "contract fingerprints for this MCP server process."
            ),
            annotations=ToolAnnotations(
                title="Check Runtime Status",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def check_runtime_status(
            include_tool_names: bool = Field(
                default=False,
                description=(
                    "Also list every registered tool name. Default false: the "
                    "counts per tool group are returned."
                ),
            ),
        ) -> ResponseType:
            return self._format_response(
                self._runtime_status(include_tool_names=include_tool_names)
            )

        @self.mcp.tool(
            description=(
                "How to use one tool or topic well: when to call it, the arguments that "
                "matter, how to read the result and what to call next. Without a name "
                "it lists the guides. Reads no database."
            ),
            annotations=ToolAnnotations(
                title="Get Tool Guide",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def get_tool_guide(
            name: str | None = Field(
                default=None,
                description="A tool name or a topic name. Omit it to list the guides.",
            ),
        ) -> ResponseType:
            if name is None or not name.strip():
                return self._format_response({"guides": list_tool_guides()})
            return self._format_response(read_tool_guide(name))

        @self.mcp.tool(
            description=(
                "Column statistics for tuning: the statistic that leads on each column, "
                "its age and sample, skew, heavy hitters, value picks for parameter "
                "cases (common, rare, NULL, boundary) and a bounded histogram. Values "
                "of masked or classified columns are withheld in the database; no table "
                "row is read."
            ),
            annotations=ToolAnnotations(
                title="Get Column Statistics",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_column_statistics(
            schema_name: str = Field(description="Schema of the table."),
            table_name: str = Field(description="Table name."),
            column_names: list[str] | None = Field(
                default=None, description="Another name for columns. Pass one of the three."
            ),
            column_name: str | None = Field(
                default=None, description="One column: another name for columns=[column]."
            ),
            columns: list[str] | None = Field(
                default=None,
                description="Up to 20 columns. Default: the columns that lead a statistic.",
            ),
            include_histogram: bool = Field(default=True),
            max_steps: int = Field(default=40, ge=1, le=200),
            top_values: int = Field(default=5, ge=1, le=20),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            given = [
                value
                for value in (columns, column_names, [column_name] if column_name else None)
                if value
            ]
            if len(given) > 1:
                raise ValueError("Pass one of columns, column_names, column_name.")
            requested_columns = given[0] if given else None
            return await self._run_tool(
                "get_column_statistics",
                database_name,
                lambda db: self.column_statistics.get_column_statistics(
                    db,
                    schema_name,
                    table_name,
                    requested_columns,
                    include_histogram=include_histogram,
                    max_steps=max_steps,
                    top_values=top_values,
                ),
            )

        @self.mcp.tool(
            description="List the configured Azure SQL databases available to this MCP server.",
            annotations=ToolAnnotations(
                title="List Databases",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def list_databases() -> ResponseType:
            return self._format_response(
                {
                    "server": self.config.server,
                    "default_database": self.config.default_database,
                    "allowed_databases": list(self.config.allowed_databases),
                }
            )

        @self.mcp.tool(
            description="Probe Azure SQL capabilities and permission-sensitive features for a database.",
            annotations=ToolAnnotations(
                title="Check Capabilities",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def check_capabilities(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
            detail: Literal["summary", "full"] = Field(
                default="summary",
                description=(
                    "summary: mcp_contract, local_tuning_policy, indexed_view_experiment, "
                    "the platform line, the Query Store state and one word per probe. "
                    "full: adds the probe details, the platform report and the replicas."
                ),
            ),
        ) -> ResponseType:
            async def check(db: str) -> dict[str, Any]:
                payload = await self._check_database_capabilities(db)
                return payload if detail == "full" else _capabilities_summary(payload)

            return await self._run_tool("check_capabilities", database_name, check)

        @self.mcp.tool(
            description="List schemas in the selected Azure SQL database.",
            annotations=ToolAnnotations(
                title="List Schemas",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def list_schemas(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "list_schemas",
                database_name,
                self.introspection.list_schemas,
            )

        @self.mcp.tool(
            description="List objects in a schema. Supports table, view, procedure, function, and index.",
            annotations=ToolAnnotations(
                title="List Objects",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def list_objects(
            schema_name: str = Field(description="Schema name."),
            object_type: str = Field(
                default="table",
                description="Object type: table, view, procedure, function, or index.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "list_objects",
                database_name,
                lambda resolved_database: self.introspection.list_objects(
                    resolved_database,
                    schema_name,
                    object_type,
                ),
            )

        @self.mcp.tool(
            description="Search for database objects by name pattern across all schemas.",
            annotations=ToolAnnotations(
                title="Search Objects",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def search_objects(
            pattern: str = Field(
                description="SQL LIKE pattern, for example '%User%' or 'Order%'.",
            ),
            object_type: str | None = Field(
                default=None,
                description="Optional filter: table, view, procedure, or function.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "search_objects",
                database_name,
                lambda resolved_database: self.introspection.search_objects(
                    resolved_database,
                    pattern,
                    object_type,
                ),
            )

        @self.mcp.tool(
            description="Show detailed information about a schema object.",
            annotations=ToolAnnotations(
                title="Get Object Details",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_object_details(
            schema_name: str | None = Field(
                default=None,
                description="Schema name. May be left out when object_name is schema.object.",
            ),
            object_name: str | None = Field(
                default=None,
                description="Object name, or a two-part name such as dw.FactPost.",
            ),
            table_name: str | None = Field(
                default=None, description="Another name for object_name. Pass one of the two."
            ),
            object_type: str = Field(
                default="table",
                description="Object type: table, view, procedure, function, or index.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            if object_name and table_name:
                raise ValueError("Pass object_name or table_name, not both.")
            schema, name = _two_part_name(schema_name, object_name or table_name)
            if not schema or not name:
                raise ValueError(
                    "get_object_details needs schema_name and object_name, or object_name "
                    "as schema.object (for example dw.FactPost)."
                )
            return await self._run_tool(
                "get_object_details",
                database_name,
                lambda resolved_database: self._get_object_details(
                    resolved_database,
                    schema,
                    name,
                    object_type,
                ),
            )

        @self.mcp.tool(
            description="Get dependency graph for a database object: what it references and what references it.",
            annotations=ToolAnnotations(
                title="Get Dependencies",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_dependencies(
            schema_name: str = Field(description="Schema name."),
            object_name: str = Field(description="Object name."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_dependencies",
                database_name,
                lambda resolved_database: self.introspection.get_dependencies(
                    resolved_database,
                    schema_name,
                    object_name,
                ),
            )

        @self.mcp.tool(
            description="Get approximate row counts and storage sizes for tables.",
            annotations=ToolAnnotations(
                title="Get Table Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_table_stats(
            schema_name: str | None = Field(
                default=None,
                description="Optional schema filter. Omit for all schemas.",
            ),
            table_name: str | None = Field(
                default=None,
                description=(
                    "Optional table filter: a table name, or schema.table. Without a "
                    "schema the table is returned from every schema that has it."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            schema, table = _two_part_name(schema_name, table_name)

            async def table_stats(resolved_database: str) -> list[dict[str, Any]]:
                rows = await self.introspection.get_table_stats(resolved_database, schema)
                if not table:
                    return rows
                return [
                    row
                    for row in rows
                    if str(row.get("table_name") or "").casefold() == table.casefold()
                ]

            return await self._run_tool("get_table_stats", database_name, table_stats)

        @self.mcp.tool(
            description="Capture a point-in-time schema snapshot for a database.",
            annotations=ToolAnnotations(
                title="Capture Schema Snapshot",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def capture_schema_snapshot(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
            schema_filter: str | None = Field(
                default=None,
                description="Comma-separated schema names to capture. Defaults to all user schemas.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "capture_schema_snapshot",
                database_name,
                lambda resolved_database: self.schema_compare.capture_schema_snapshot(
                    resolved_database,
                    schema_filter,
                ),
            )

        @self.mcp.tool(
            description="Compare schemas between two databases and return all differences.",
            annotations=ToolAnnotations(
                title="Compare Schemas",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def compare_schemas(
            source_database: str = Field(description="Source database name."),
            target_database: str = Field(description="Target database name."),
            schema_filter: str | None = Field(
                default=None,
                description="Comma-separated schema names to compare. Defaults to all user schemas.",
            ),
        ) -> ResponseType:
            return await self._run_database_pair_tool(
                "compare_schemas",
                source_database,
                target_database,
                lambda resolved_source, resolved_target: self.schema_compare.compare_schemas(
                    resolved_source,
                    resolved_target,
                    schema_filter,
                ),
            )

        @self.mcp.tool(
            description="Generate a T-SQL migration script to transform source schema to match target.",
            annotations=ToolAnnotations(
                title="Generate Migration Script",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def generate_migration_script(
            source_database: str = Field(description="Source database name."),
            target_database: str = Field(description="Target database name."),
            schema_filter: str | None = Field(
                default=None,
                description="Comma-separated schema names to compare. Defaults to all user schemas.",
            ),
        ) -> ResponseType:
            async def _generate_script_payload(
                resolved_source: str,
                resolved_target: str,
            ) -> dict[str, Any]:
                script = await self.schema_compare.generate_migration_script(
                    resolved_source,
                    resolved_target,
                    schema_filter,
                )
                return {
                    "source_database": resolved_source,
                    "target_database": resolved_target,
                    "schema_filter": schema_filter,
                    "migration_script": script,
                }

            return await self._run_database_pair_tool(
                "generate_migration_script",
                source_database,
                target_database,
                _generate_script_payload,
            )

        @self.mcp.tool(
            description=(
                "List active sessions and running queries with blocking chains "
                "(head blocker, blocked session ids, wait summary). Each request "
                "returns plan_handle and query_hash; the plan XML is returned only "
                "with include_plans=true. For one in-flight plan use "
                "analyze_query_plan(session_id=...)."
            ),
            annotations=ToolAnnotations(
                title="Get Active Sessions",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_active_sessions(
            limit: int = Field(
                default=200,
                description="Maximum sessions to return (longest-running first, max 1000).",
            ),
            include_plans: bool = Field(
                default=False,
                description=(
                    "When true, returns the cached plan XML of every request. This "
                    "builds each plan on the database and can return megabytes."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_active_sessions",
                database_name,
                lambda db: self.sessions.get_active_sessions(
                    db, limit, include_plans=include_plans
                ),
            )

        @self.mcp.tool(
            description=(
                "Execute statically validated, row-capped read-only SQL. The query may "
                "be preceded by DECLARE @var and SET @var = ... statements, followed "
                "by exactly one SELECT. DML, DDL, dynamic SQL, and stored procedure "
                "execution are always rejected."
            ),
            annotations=ToolAnnotations(
                title="Execute SQL",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def execute_sql(
            sql: str = Field(description="Read-only SQL to execute."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "execute_sql",
                database_name,
                lambda resolved_database: self._execute_safe_sql(resolved_database, sql),
            )

        @self.mcp.tool(
            description=(
                "Generate an estimated or actual execution plan for a read-only SQL query, "
                "with a plan digest and rule findings. A read-only IF / ELSE batch (one "
                "SELECT or sp_executesql of a literal per branch) is one candidate: "
                "analyze=false returns branches, one estimated plan per branch with its "
                "condition; analyze=true runs the batch once and names the branch the "
                "parameter values selected in summary.batch.executed_branch."
            ),
            annotations=ToolAnnotations(
                title="Explain Query",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def explain_query(
            sql: str = Field(description="Read-only SQL query to explain."),
            analyze: bool = Field(
                default=False,
                description="When true, executes the query and returns an actual plan.",
            ),
            hypothetical_indexes: list[dict[str, Any]] | None = Field(
                default=None,
                description=(
                    "Reserved for future use. Hypothetical index analysis is disabled "
                    "on this read-only tool for safety."
                ),
            ),
            auto_bind_params: bool = Field(
                default=False,
                description=(
                    "When true, detects @param placeholders and binds each from the "
                    "statistics of the column it is compared with. A parameter with "
                    "no such statistic is reported as unbound, never given an "
                    "invented value."
                ),
            ),
            parameter_case: ParameterCaseInput | None = Field(
                default=None,
                description=(
                    "Parameter values in the one shape every tool takes: {name, "
                    "values, types, weight}. Values are passed as driver parameters "
                    "and are not returned in the response."
                ),
            ),
            parameter_values: dict[str, Any] | None = Field(
                default=None,
                description=(
                    "Short form of parameter_case.values, for example "
                    "{'CustomerId': 42}. Do not pass both."
                ),
            ),
            parameter_types: dict[str, str] | None = Field(
                default=None,
                description=(
                    "Short form of parameter_case.types, for example "
                    "{'CustomerId': 'bigint'}. Supply exact types for faithful compilation."
                ),
            ),
            include_raw_xml: bool = Field(
                default=False,
                description=(
                    "When true, includes raw SHOWPLAN XML inline. Defaults to False; "
                    "use raw_xml_resource_uri for token-safe retrieval."
                ),
            ),
            include_operator_summary: bool = Field(
                default=False,
                description=(
                    "The full form. Default false: plan_digest and plan_findings carry "
                    "the plan, and summary holds only what they do not (actual_metrics "
                    "totals, compile metrics, warnings). True returns the whole "
                    "summary: statements, parameters, per-statement metrics, "
                    "summary.operators (about 1 KB each) and "
                    "summary.top_operators_by_estimated_cost."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> PlanToolOutput:
            return cast(
                PlanToolOutput,
                await self._run_tool(
                    "explain_query",
                    database_name,
                    lambda resolved_database: self._explain_query_one_shape(
                        resolved_database,
                        sql,
                        analyze,
                        hypothetical_indexes,
                        auto_bind_params,
                        include_raw_xml,
                        parameter_case,
                        parameter_values,
                        parameter_types,
                        include_operator_summary,
                    ),
                ),
            )

        @self.mcp.tool(
            description=(
                "Read a query plan the way an expert does. Returns a digest (plan kind and what "
                "it can show, statement time and UDF share, warnings by node, memory grant use, "
                "parameter and local-variable tells, top operators by self time on actual plans "
                "or estimated self cost otherwise, per-execution estimate errors, repeated table "
                "access, thread skew, waits, eager index spools, missing-index hints, predicates "
                "on cited nodes, and an operator tree) plus rule findings ranked by severity and "
                "self-time share. Pass node_id for one operator in full. Pass exactly one of "
                "plan_id (Query Store), query_id (its most expensive Query Store plan), plan_xml, "
                "or session_id (the in-flight plan of a running request); last_actual=true with "
                "plan_id or query_id reads the last actual plan from the plan cache instead."
            ),
            annotations=ToolAnnotations(
                title="Analyze Query Plan",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def analyze_query_plan(
            plan_id: int | None = Field(default=None, ge=1, description="Query Store plan_id."),
            query_id: int | None = Field(
                default=None,
                ge=1,
                description="Query Store query_id; its most expensive stored plan is analysed.",
            ),
            plan_xml: str | None = Field(
                default=None,
                description="Raw showplan XML (estimated or actual) to analyse directly.",
            ),
            session_id: int | None = Field(
                default=None,
                ge=1,
                description="A running session: analyse its in-flight plan (partial counts).",
            ),
            last_actual: bool = Field(
                default=False,
                description=(
                    "With plan_id or query_id: read the last actual plan (actual row counts) "
                    "from the plan cache. Needs LAST_QUERY_PLAN_STATS = ON."
                ),
            ),
            node_id: int | None = Field(
                default=None,
                ge=0,
                description="Return one operator in full instead of the digest and findings.",
            ),
            statement_index: int | None = Field(
                default=None,
                ge=0,
                description="With node_id: which statement of a multi-statement plan.",
            ),
            include_digest: bool = Field(
                default=True,
                description="Include the plan digest. Set false for findings only.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "analyze_query_plan",
                database_name,
                lambda db: self._analyze_query_plan(
                    db,
                    plan_id,
                    query_id,
                    plan_xml,
                    session_id=session_id,
                    last_actual=last_actual,
                    node_id=node_id,
                    statement_index=statement_index,
                    include_digest=include_digest,
                ),
            )

        @self.mcp.tool(
            description=(
                "Compatibility initializer only: open a performance case/session and "
                "collect evidence. It does not generate or benchmark a rewrite and must "
                "not be treated as a completed optimization. Named optimizer profiles "
                "use the explicit case/session workflow instead."
            ),
            annotations=ToolAnnotations(
                title="Tune Query",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def tune_query(
            sql: str = Field(description="Read-only SQL query to tune."),
            analyze: bool = Field(
                default=True,
                description="When true, execute the query to capture an actual plan.",
            ),
            auto_bind_params: bool = Field(
                default=True,
                description="Bind @param placeholders from column statistics where possible.",
            ),
            parameter_values: dict[str, Any] | None = Field(
                default=None,
                description=(
                    "Explicit parameter values for representative execution; prefer this "
                    "over heuristic statistics/type fallback binding."
                ),
            ),
            parameter_types: dict[str, str] | None = Field(
                default=None,
                description="Optional exact SQL types for the supplied parameter values.",
            ),
            include_raw_xml: bool = Field(
                default=False,
                description="Include raw SHOWPLAN XML inline. Defaults to token-safe artifact URI only.",
            ),
            window_minutes: int = Field(
                default=1440,
                description="Query Store lookback window for history evidence.",
            ),
            detail: Literal["summary", "full"] = Field(
                default="summary",
                description=(
                    "summary: the evidence id, the outcome, which sections were read "
                    "and the profile medians. full: every collected row (can exceed "
                    "200 KB); get_performance_case holds them too."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            async def tune(resolved_database: str) -> dict[str, Any]:
                payload = await self._tune_query(
                    resolved_database,
                    sql,
                    analyze,
                    auto_bind_params,
                    include_raw_xml,
                    window_minutes,
                    parameter_values,
                    parameter_types,
                )
                evidence = payload.get("evidence")
                # The rows are in the sections; evidence without them is small already.
                if (
                    detail == "summary"
                    and isinstance(evidence, Mapping)
                    and isinstance(evidence.get("sections"), Mapping)
                ):
                    payload = {
                        **payload,
                        "evidence": compact_evidence_result(evidence)["summary"],
                    }
                return {**payload, "detail": detail}

            return await self._run_tool("tune_query", database_name, tune)

        @self.mcp.tool(
            description=(
                "Benchmark a baseline query against a proposed rewrite using the same "
                "typed read-only execution path, interleaved actual-plan samples, and "
                "complete bounded snapshot equivalence. It opens its own performance "
                "case and tuning session and returns their ids; where the database "
                "policy permits no session it measures nothing and says why."
            ),
            annotations=ToolAnnotations(
                title="Benchmark Query Rewrite",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def benchmark_query_rewrite(
            baseline_sql: str = Field(description="Original read-only SQL query."),
            rewrite_sql: str = Field(description="Candidate semantically equivalent rewrite."),
            analyze: bool = Field(
                default=True,
                description="When true, execute both queries to capture actual plans.",
            ),
            auto_bind_params: bool = Field(
                default=True,
                description="Bind @param placeholders from column statistics where possible.",
            ),
            parameter_values: dict[str, Any] | None = Field(
                default=None,
                description="Explicit parameter values for both baseline and rewrite.",
            ),
            parameter_types: dict[str, str] | None = Field(
                default=None,
                description="Optional exact SQL types shared by baseline and rewrite.",
            ),
            compare_order: bool | None = Field(
                default=None,
                description=(
                    "Compare result rows in returned order. Default: resolved from "
                    "the SQL; true only when the baseline has an outer ORDER BY. An "
                    "outer ORDER BY is always order-compared."
                ),
            ),
            include_raw_xml: bool = Field(
                default=False,
                description="Include raw SHOWPLAN XML inline. Defaults to artifact URI only.",
            ),
            runs: int = Field(
                default=3,
                description=(
                    "Screening executions per side (2-3). Metrics "
                    "become per-run medians with min/max spread, so a single lucky run "
                    "cannot masquerade as a win. The workflow does not clear Azure SQL caches."
                ),
            ),
            detail: Literal["summary", "full"] = Field(
                default="summary",
                description=(
                    "summary: the verdict and, per parameter case, the medians a "
                    "decision needs. full: adds per-run values, spread, metric "
                    "coverage, result digests and the whole preflight."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> BenchmarkToolOutput:
            async def benchmark(resolved_database: str) -> Any:
                payload = await self._benchmark_query_rewrite(
                    resolved_database,
                    baseline_sql,
                    rewrite_sql,
                    analyze,
                    auto_bind_params,
                    include_raw_xml,
                    runs,
                    parameter_values,
                    compare_order,
                    parameter_types,
                )
                return payload if detail == "full" else _compact_benchmark_payload(payload)

            return cast(
                BenchmarkToolOutput,
                await self._run_tool("benchmark_query_rewrite", database_name, benchmark),
            )

        @self.mcp.tool(
            description=(
                "Analyze direct SQL and recursively resolved view definitions for "
                "semantic-equivalence proof risks without returning raw definitions."
            ),
            annotations=ToolAnnotations(
                title="Check Equivalence Preflight",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def check_equivalence_preflight(
            sql: str = Field(
                description=(
                    "Read-only SQL to inspect, including referenced view definitions."
                )
            ),
            database_name: str | None = Field(default=None),
        ) -> PreflightToolOutput:
            return cast(
                PreflightToolOutput,
                await self._run_tool(
                    "check_equivalence_preflight",
                    database_name,
                    lambda db: self._check_equivalence_preflight(db, sql),
                ),
            )

        @self.mcp.tool(
            description=(
                "Create a durable, redacted performance case. SQL is fingerprinted "
                "and is not written to the MCP state database."
            ),
            annotations=ToolAnnotations(
                title="Start Performance Case",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def start_performance_case(
            sql: str = Field(description="Baseline read-only SELECT-shaped SQL."),
            parameter_cases: list[ParameterCaseInput] | None = Field(
                default=None,
                description=(
                    "Up to four named parameter cases, for example common, rare, NULL, "
                    "and boundary. Each has name, values, types, and positive weight. "
                    "Values are fingerprinted, not persisted."
                ),
            ),
            query_store_query_id: int | None = Field(
                default=None,
                ge=1,
                strict=True,
                description=(
                    "Optional exact Query Store query_id. When supplied, evidence "
                    "collection uses only this identity and never fuzzy matching."
                ),
            ),
            objective: TuningObjective = Field(
                default="elapsed_time",
                description=(
                    "Primary tuning objective: elapsed_time, cpu, logical_reads, "
                    "or physical_reads."
                ),
            ),
            idempotency_key: str | None = Field(
                default=None,
                description="Optional caller-generated idempotency key.",
            ),
            database_name: str | None = Field(default=None),
        ) -> CaseToolOutput:
            return cast(
                CaseToolOutput,
                await self._run_tool(
                    "start_performance_case",
                    database_name,
                    lambda db: self._start_performance_case(
                        db,
                        sql,
                        _parameter_case_payloads(parameter_cases),
                        objective,
                        idempotency_key,
                        query_store_query_id,
                    ),
                ),
            )

        @self.mcp.tool(
            description=(
                "Collect Azure SQL resource, Query Store, wait, blocking, statistics, "
                "parameter-sensitivity, and regression evidence for a performance case."
            ),
            annotations=ToolAnnotations(
                title="Collect Performance Evidence",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def collect_performance_evidence(
            case_id: str = Field(description="Performance case identifier."),
            sql: str = Field(description="The same baseline SQL used to create the case."),
            window_minutes: int = Field(default=60, ge=1),
            execute_query: bool = Field(
                default=False,
                description=(
                    "Capture one actual-plan sample. Requires database benchmark policy."
                ),
            ),
            parameter_case: ParameterCaseInput | None = Field(
                default=None,
                description=(
                    "For active evidence on parameterized SQL, one named case containing "
                    "an exact values object and exact SQL types object."
                ),
            ),
            query_store_query_id: int | None = Field(
                default=None,
                ge=1,
                strict=True,
                description=(
                    "Optional exact Query Store query_id. It must match any identity "
                    "already stored on the performance case."
                ),
            ),
            detail: Literal["summary", "full"] = Field(
                default="summary",
                description=(
                    "summary: the evidence id, the outcome and, per section, status, "
                    "gaps and row counts. full: every collected row (can exceed 200 KB)."
                ),
            ),
            idempotency_key: str | None = Field(default=None),
            decision_id: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            async def collect(db: str) -> Any:
                payload = await self._collect_performance_evidence(
                    db,
                    case_id,
                    sql,
                    window_minutes,
                    execute_query,
                    idempotency_key,
                    _parameter_case_payload(parameter_case),
                    query_store_query_id,
                )
                return payload if detail == "full" else _compact_evidence_payload(payload)

            return await self._run_tool(
                "collect_performance_evidence",
                database_name,
                collect,
                decision_id=decision_id,
            )

        @self.mcp.tool(
            description="Get a redacted performance case, its evidence envelopes, and event history.",
            annotations=ToolAnnotations(
                title="Get Performance Case",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def get_performance_case(
            case_id: str = Field(description="Performance case identifier."),
            database_name: str | None = Field(default=None),
        ) -> CaseToolOutput:
            return cast(
                CaseToolOutput,
                await self._run_tool(
                    "get_performance_case",
                    database_name,
                    lambda db: self._get_performance_case(db, case_id),
                ),
            )

        @self.mcp.tool(
            description=(
                "Start a durable iterative tuning session. A limit you leave out takes "
                "its default (10 candidates, 120 executions or what the case's parameter "
                "cases need, 20 minutes), capped by the local database policy. A limit "
                "you state is checked as given: over the policy it is refused."
            ),
            annotations=ToolAnnotations(
                title="Start Tuning Session",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def start_tuning_session(
            case_id: str = Field(description="Performance case identifier."),
            max_candidates: int | None = Field(
                default=None,
                ge=1,
                description="Maximum candidate experiments. Default 10, capped by policy.",
            ),
            execution_limit: int | None = Field(
                default=None,
                ge=1,
                description=(
                    "Maximum measured query executions across the session. Default 120, "
                    "or more when the case's parameter cases need it; capped by policy."
                ),
            ),
            time_limit_minutes: int | None = Field(
                default=None,
                ge=1,
                description=(
                    "Wall-clock session budget in minutes; may span hours. Default 20, "
                    "capped by policy."
                ),
            ),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> SessionToolOutput:
            return cast(
                SessionToolOutput,
                await self._run_tool(
                    "start_tuning_session",
                    database_name,
                    lambda db: self._start_tuning_session(
                        db,
                        case_id,
                        max_candidates,
                        execution_limit,
                        time_limit_minutes,
                        idempotency_key,
                    ),
                ),
            )

        @self.mcp.tool(
            description=(
                "Read a durable tuning session: status, remaining budgets and one compact "
                "row per candidate (view=summary, the default). view=full adds every "
                "evidence record and event. Raw SQL is never persisted."
            ),
            annotations=ToolAnnotations(
                title="Get Tuning Session",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def get_tuning_session(
            session_id: str = Field(description="Tuning session identifier."),
            view: Literal["summary", "full"] = Field(
                default="summary",
                description=(
                    "summary: the latest result of each candidate. full: every "
                    "evidence record and event; use it once, not after each benchmark."
                ),
            ),
            database_name: str | None = Field(default=None),
        ) -> SessionToolOutput:
            return cast(
                SessionToolOutput,
                await self._run_tool(
                    "get_tuning_session",
                    database_name,
                    lambda db: self._get_tuning_session(db, session_id, view=view),
                ),
            )

        @self.mcp.tool(
            description=(
                "Add one concrete rewrite or index experiment to a tuning session. "
                "Only its fingerprint and optional artifact reference are persisted."
            ),
            annotations=ToolAnnotations(
                title="Add Tuning Candidate",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def add_tuning_candidate(
            session_id: str = Field(description="Tuning session identifier."),
            candidate_sql: str = Field(description="Concrete read-only candidate SQL."),
            strategy: TuningStrategy = Field(
                description=(
                    "Candidate family: predicate, join, aggregation, cardinality, "
                    "index, combined, or rewrite_plus_index."
                )
            ),
            artifact_ref: str | None = Field(
                default=None,
                description=(
                    "Parent of a rewrite_plus_index candidate: candidate:<parent-id>, "
                    "a rewrite finalist of this session whose results matched (improved, "
                    "neutral or regressed). Leave it out to measure the rewrite with its "
                    "indexes against the original in one call (candidate_sql of "
                    "benchmark_index_candidate)."
                ),
            ),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "add_tuning_candidate",
                database_name,
                lambda db: self._add_tuning_candidate(
                    db,
                    session_id,
                    candidate_sql,
                    strategy,
                    artifact_ref,
                    idempotency_key,
                ),
            )

        @self.mcp.tool(
            description=(
                "Benchmark one rewrite candidate with interleaved, exactly-once samples "
                "and snapshot-consistent duplicate-aware result comparison."
            ),
            annotations=ToolAnnotations(
                title="Benchmark Tuning Candidate",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def benchmark_tuning_candidate(
            session_id: str = Field(description="Tuning session identifier."),
            candidate_id: str = Field(description="Candidate identifier."),
            baseline_sql: str = Field(description="Baseline read-only SQL."),
            candidate_sql: str = Field(description="Candidate read-only SQL."),
            phase: BenchmarkPhase = Field(
                default="screening",
                description="Benchmark phase: screening or finalist.",
            ),
            parameter_cases: list[ParameterCaseInput] | None = Field(
                default=None,
                description=(
                    "Exact unchanged subset of the performance-case parameter inputs. "
                    "Omit it to use every case of the performance case, or name cases "
                    "with [{\"name\": ...}] when the case was registered in this process."
                ),
            ),
            cross_case: dict[str, str] | None = Field(
                default=None,
                description=(
                    "Parameter-sensitivity test: {\"compile_case\": A, \"run_case\": B} "
                    "compiles the plan with the values of case A and measures case B on "
                    "it. A candidate that regresses there is rejected."
                ),
            ),
            compare_order: bool | None = Field(
                default=None,
                description=(
                    "Compare rows in returned order. Default: resolved from the SQL; "
                    "true only when the baseline has an outer ORDER BY. An outer "
                    "ORDER BY is always order-compared."
                ),
            ),
            runs: int | None = Field(
                default=None,
                ge=2,
                le=5,
                description=(
                    "Optional paired runs per parameter case, 2 to 5 (a pair of runs "
                    "is the least that shows noise). Screening is capped by the "
                    "session screening limit; finalists by the finalist limit."
                ),
            ),
            prove_equivalence: bool | None = Field(
                default=None,
                description=(
                    "Defaults false for broad screening and true for finalists. "
                    "Finalists cannot disable full snapshot equivalence."
                ),
            ),
            slow_baseline_cases: dict[str, float] | None = Field(
                default=None,
                description=_SLOW_BASELINE_DESCRIPTION,
            ),
            detail: Literal["summary", "full"] = Field(
                default="summary",
                description=(
                    "summary: the verdict and, per parameter case, the medians a "
                    "decision needs. full: adds per-run values, spread, metric "
                    "coverage, result digests and the whole preflight."
                ),
            ),
            idempotency_key: str | None = Field(default=None),
            decision_id: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> BenchmarkToolOutput:
            async def benchmark(db: str) -> Any:
                payload = await self._benchmark_tuning_candidate(
                    db,
                    session_id,
                    candidate_id,
                    baseline_sql,
                    candidate_sql,
                    phase=phase,
                    parameter_cases=_parameter_case_payloads(parameter_cases),
                    compare_order=compare_order,
                    runs_override=runs,
                    prove_equivalence=prove_equivalence,
                    idempotency_key=idempotency_key,
                    cross_case=cross_case,
                    slow_baseline_cases=slow_baseline_cases,
                )
                return payload if detail == "full" else _compact_benchmark_payload(payload)

            return cast(
                BenchmarkToolOutput,
                await self._run_tool(
                    "benchmark_tuning_candidate",
                    database_name,
                    benchmark,
                    deadline_provider=lambda: self.tuning_sessions.get_session(
                        session_id
                    ).deadline_at_utc,
                    decision_id=decision_id,
                ),
            )

        @self.mcp.tool(
            description=(
                "Benchmark a leased set of 1 to 4 disposable indexes as one candidate, "
                "in a policy-allowlisted sandbox: the query is measured before, with "
                "and after the set. Pass one index with schema_name, table_name and "
                "key_columns, or several (also on different tables) with indexes. "
                "Every index is created or none is left, and every one is dropped on "
                "every path; cleanup failures are durable. The result lists per index "
                "what was built, its size in MB and its B-tree depth. Every parameter "
                "case of the call is measured under the one build: decision.cases is "
                "the per-case table and decision.worst_case the case that gains least. "
                "For a rewrite that needs its indexes, pass the rewrite as candidate_sql: "
                "one call measures the original against the rewrite with its indexes "
                "and compares their results."
            ),
            annotations=ToolAnnotations(
                title="Benchmark Index Candidate",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=False,
                openWorldHint=False,
            ),
        )
        async def benchmark_index_candidate(
            session_id: str = Field(description="Tuning session identifier."),
            candidate_id: str = Field(description="Index candidate identifier."),
            sql: str = Field(
                description=(
                    "Read-only query measured before and after the index. With "
                    "candidate_sql: the original statement of the performance case."
                )
            ),
            candidate_sql: str | None = Field(
                default=None,
                description=(
                    "A rewrite measured WITH the index set, against sql (the original) "
                    "without it: one call for a rewrite that needs its indexes. The "
                    "candidate is registered with add_tuning_candidate(strategy="
                    "'rewrite_plus_index') and no artifact_ref. A finalist compares the "
                    "results of both statements in one snapshot per parameter case "
                    "before the indexes are dropped."
                ),
            ),
            runs: int | None = Field(
                default=None,
                ge=1,
                description=(
                    "Measured runs per arm and parameter case, from 1 to the session "
                    "limit of the phase (default: that limit). The tool lowers it by "
                    "itself for a slow statement: at most 3 runs when the first run "
                    "takes 1 to 10 seconds, one run at 10 seconds or more."
                ),
            ),
            plan_reuse: Literal["per_case", "first_case"] = Field(
                default="per_case",
                description=(
                    "per_case: each parameter case compiles its own plan. first_case: "
                    "all cases share one statement text, so the first case compiles "
                    "and the later cases run on its plan, in the order given, as a "
                    "plan cache does. plan_compiled_for names the case per result."
                ),
            ),
            schema_name: str | None = Field(
                default=None, description="Target schema of a one-index candidate."
            ),
            table_name: str | None = Field(
                default=None, description="Target table of a one-index candidate."
            ),
            key_columns: list[str] | None = Field(
                default=None, description="Ordered key columns, optionally ASC/DESC."
            ),
            include_columns: list[str] | None = Field(default=None),
            filter_definition: str | None = Field(
                default=None,
                description="Optional filtered-index predicate; no SQL statements or comments.",
            ),
            is_unique: bool = Field(default=False),
            data_compression: (
                Literal["NONE", "ROW", "PAGE", "COLUMNSTORE", "COLUMNSTORE_ARCHIVE"] | None
            ) = Field(
                default=None,
                description=(
                    "Compression of a one-index candidate. ROW or PAGE can keep a wide "
                    "index one B-tree level shallower; compare index_depth in the result. "
                    "A nonclustered columnstore index takes COLUMNSTORE or COLUMNSTORE_ARCHIVE."
                ),
            ),
            index_kind: Literal["rowstore", "nonclustered_columnstore"] | None = Field(
                default=None,
                description=(
                    "Kind of a one-index candidate: rowstore (default) or "
                    "nonclustered_columnstore, where key_columns lists the columns of "
                    "the index. In indexes the same field is named kind."
                ),
            ),
            partition_aligned: bool | None = Field(
                default=None,
                description=(
                    "One-index candidate on a partitioned table: false builds the index "
                    "ON [PRIMARY], one ordered B-tree across all partitions (default: "
                    "aligned with the table's partition scheme). The result then carries "
                    "non_aligned_index_warning. The same field exists in each indexes entry."
                ),
            ),
            indexes: list[IndexDefinitionInput] | None = Field(
                default=None,
                description=(
                    "A set of 1 to 4 indexes tested together as one candidate, in place "
                    "of the single-index arguments. Each: {table_name, key_columns, "
                    "schema_name (default dbo), include_columns, filter_definition, "
                    "is_unique, data_compression, kind (rowstore or "
                    "nonclustered_columnstore)}. Use it when the design needs two "
                    "indexes, or one index on each of two tables."
                ),
            ),
            index_name_prefix: str | None = Field(
                default=None,
                description=(
                    "Prefix of the generated index names, when the engagement needs "
                    "its own. Use a prefix the database policy lists in "
                    "test_index_name_prefixes, or one that extends it (IX_bench_T7_ "
                    "for IX_bench_). Any other value is not refused: the policy prefix "
                    "is used and index_name_note says so. The tool generates the rest "
                    "of each name: <prefix><Table>_<KeyColumns>_<8 hex>."
                ),
            ),
            replaces: str | None = Field(
                default=None,
                description=(
                    "Name of an existing index that a one-index candidate would "
                    "replace. The candidate arm then runs with an index hint, so the "
                    "old index cannot be chosen. The result is hinted_evidence: what "
                    "the new index costs when used, never deployment-ready proof."
                ),
            ),
            slow_baseline_cases: dict[str, float] | None = Field(
                default=None,
                description=_SLOW_BASELINE_DESCRIPTION,
            ),
            parameter_cases: list[ParameterCaseInput] | None = Field(
                default=None,
                description="The same named parameter cases recorded on the performance case.",
            ),
            phase: BenchmarkPhase = Field(
                default="screening",
                description="Benchmark phase: screening or finalist.",
            ),
            online: bool = Field(
                default=True,
                description=(
                    "Build the test index online. A table with a clustered columnstore "
                    "index is built offline only (the engine refuses ONLINE there, error "
                    "35352; a nonclustered columnstore index does not force this): the build "
                    "then needs allow_offline_test_index in the database policy, blocks "
                    "writes to the table while it runs, and the result says so in "
                    "offline_build."
                ),
            ),
            compare_order: bool | None = Field(
                default=None,
                description=(
                    "Compare rows in returned order during A-B-A checks. Default: "
                    "resolved from the SQL; true only when it has an outer ORDER BY."
                ),
            ),
            lease_minutes: int = Field(default=30, ge=5, le=120),
            keep_for_session: bool = Field(
                default=False,
                description=(
                    "true leaves what this call built in place under the lease of the "
                    "session when it returns, for at most lease_minutes: a later "
                    "benchmark call of the session reuses each object whose definition "
                    "matches and builds only the others, and compare_query_results and "
                    "explain_query can read them meanwhile. The baseline of a later "
                    "call is the one measured before the hold, so keep with every "
                    "parameter case you will need. Default false: every object the "
                    "call used is dropped before it returns. finalize_tuning_session "
                    "drops whatever is held; test_objects in the result lists what was "
                    "reused, built and still held."
                ),
            ),
            max_call_seconds: float | None = Field(
                default=None,
                ge=30,
                description=(
                    "Cap of this call in seconds. No run and no build starts after it, "
                    "and a build whose estimate (table rows at the rate of the first "
                    "build of the session, in build_estimate) does not fit is not "
                    "started. A running statement is not cut. At the cap the result is "
                    "partial: true with no verdict, the measurements taken so far and "
                    "time_spent; with keep_for_session what was built stays held, so "
                    "the next call continues without a rebuild."
                ),
            ),
            idempotency_key: str = Field(
                description="Caller-generated key used to fence retries and cleanup."
            ),
            detail: Literal["summary", "full"] = Field(
                default="summary",
                description=(
                    "summary: the verdict and, per parameter case, the medians a "
                    "decision needs. full: adds per-run values, spread, metric "
                    "coverage, result digests and the whole preflight."
                ),
            ),
            decision_id: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> BenchmarkToolOutput:
            async def benchmark(db: str) -> Any:
                payload = await self._benchmark_index_candidate(
                    db,
                    session_id,
                    candidate_id,
                    sql,
                    schema_name,
                    table_name,
                    key_columns,
                    include_columns,
                    filter_definition,
                    is_unique,
                    phase,
                    online,
                    compare_order,
                    lease_minutes,
                    idempotency_key,
                    _parameter_case_payloads(parameter_cases),
                    indexes=(
                        [index.model_dump(mode="python") for index in indexes]
                        if indexes is not None
                        else None
                    ),
                    data_compression=data_compression,
                    index_name_prefix=index_name_prefix,
                    replaces=replaces,
                    slow_baseline_cases=slow_baseline_cases,
                    candidate_sql=candidate_sql,
                    runs=runs,
                    plan_reuse=plan_reuse,
                    index_kind=index_kind,
                    partition_aligned=partition_aligned,
                    keep_for_session=keep_for_session,
                    max_call_seconds=max_call_seconds,
                )
                return payload if detail == "full" else _compact_benchmark_payload(payload)

            return cast(
                BenchmarkToolOutput,
                await self._run_tool(
                    "benchmark_index_candidate",
                    database_name,
                    benchmark,
                    deadline_provider=lambda: self.tuning_sessions.get_session(
                        session_id
                    ).deadline_at_utc,
                    decision_id=decision_id,
                ),
            )

        @self.mcp.tool(
            description=(
                "Benchmark a leased indexed view as one candidate, in a policy-allowlisted "
                "sandbox: the original statement is measured before and after, and the "
                "statement that reads the view while the view exists. One lease builds "
                "the schema-bound view, its unique clustered index and up to 3 more "
                "indexes in one transaction, and drops the view on every path. Needs "
                "allow_test_indexes, allow_view_apply and allow_offline_test_index in the "
                "database policy (the first index of a view is built offline). "
                "preview=true returns the names, the findings and the setup statements "
                "and runs nothing. The result carries the write overhead warning: every "
                "write to a base table also maintains the view."
            ),
            annotations=ToolAnnotations(
                title="Benchmark Indexed View Candidate",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=False,
                openWorldHint=False,
            ),
        )
        async def benchmark_indexed_view_candidate(
            view_definition: str = Field(
                description=(
                    "The view: its SELECT body, or a whole CREATE VIEW ... WITH "
                    "SCHEMABINDING AS SELECT ... statement. Two-part table names, "
                    "COUNT_BIG(*) with GROUP BY, no outer joins, no subqueries."
                )
            ),
            session_id: str | None = Field(
                default=None, description="Tuning session identifier. Not needed for preview."
            ),
            candidate_id: str | None = Field(
                default=None,
                description=(
                    "Candidate identifier. read_mode noexpand: a rewrite_plus_index "
                    "candidate registered with candidate_sql and no artifact_ref. "
                    "read_mode automatic: an index candidate with the unchanged statement."
                ),
            ),
            sql: str | None = Field(
                default=None,
                description="The original statement of the performance case. Not needed for preview.",
            ),
            candidate_sql: str | None = Field(
                default=None,
                description=(
                    "read_mode noexpand: the statement that reads the view, with a "
                    "two-part name and WITH (NOEXPAND). Call with preview=true first "
                    "when you let the tool choose the view name."
                ),
            ),
            read_mode: Literal["noexpand", "automatic"] = Field(
                default="noexpand",
                description=(
                    "noexpand: candidate_sql reads the view WITH (NOEXPAND). automatic: "
                    "the original statement runs again and the result says whether the "
                    "optimizer matched the view."
                ),
            ),
            clustered_index_key: list[str] | None = Field(
                default=None,
                description=(
                    "Ordered key of the unique clustered index. May be left out for a "
                    "view with GROUP BY that returns every GROUP BY expression."
                ),
            ),
            extra_indexes: list[ViewIndexInput] | None = Field(
                default=None,
                description="0 to 3 more indexes on the view: {key_columns, include_columns, is_unique}.",
            ),
            view_name: str | None = Field(
                default=None,
                description=(
                    "Name under a test view prefix (v_Testing_, or the v_ twin of a "
                    "policy prefix: IX_bench_ gives v_bench_). Any other name, or none, "
                    "gives a generated name; the plan and the refusal name it."
                ),
            ),
            schema_name: str | None = Field(default=None, description="Schema of the view; default dbo."),
            index_name_prefix: str | None = Field(
                default=None, description="As for benchmark_index_candidate."
            ),
            parameter_cases: list[ParameterCaseInput] | None = Field(
                default=None,
                description="The same named parameter cases recorded on the performance case.",
            ),
            phase: BenchmarkPhase = Field(
                default="screening", description="Benchmark phase: screening or finalist."
            ),
            runs: int | None = Field(
                default=None,
                ge=1,
                description="Runs per arm and case, 1 to the session limit; lowered for a slow statement.",
            ),
            plan_reuse: Literal["per_case", "first_case"] = Field(
                default="per_case",
                description=(
                    "As for benchmark_index_candidate. per_case: each parameter case "
                    "compiles its own plan. first_case: all cases share one statement "
                    "text, so the first case compiles and the later cases run on its "
                    "plan, in the order given."
                ),
            ),
            slow_baseline_cases: dict[str, float] | None = Field(
                default=None,
                description=_SLOW_BASELINE_DESCRIPTION,
            ),
            compare_order: bool | None = Field(default=None),
            lease_minutes: int = Field(default=30, ge=5, le=120),
            keep_for_session: bool = Field(
                default=False,
                description=(
                    "true leaves what this call built in place under the lease of the "
                    "session when it returns, for at most lease_minutes: a later "
                    "benchmark call of the session reuses each object whose definition "
                    "matches and builds only the others, and compare_query_results and "
                    "explain_query can read them meanwhile. The baseline of a later "
                    "call is the one measured before the hold, so keep with every "
                    "parameter case you will need. Default false: every object the "
                    "call used is dropped before it returns. finalize_tuning_session "
                    "drops whatever is held; test_objects in the result lists what was "
                    "reused, built and still held."
                ),
            ),
            max_call_seconds: float | None = Field(
                default=None,
                ge=30,
                description=(
                    "Cap of this call in seconds. No run and no build starts after it, "
                    "and a build whose estimate (table rows at the rate of the first "
                    "build of the session, in build_estimate) does not fit is not "
                    "started. A running statement is not cut. At the cap the result is "
                    "partial: true with no verdict, the measurements taken so far and "
                    "time_spent; with keep_for_session what was built stays held, so "
                    "the next call continues without a rebuild."
                ),
            ),
            idempotency_key: str | None = Field(
                default=None,
                description="Caller-generated key that fences retries and cleanup. Not needed for preview.",
            ),
            preview: bool = Field(
                default=False,
                description="Plan only: names, findings and setup statements. No statement runs.",
            ),
            detail: Literal["summary", "full"] = Field(default="summary"),
            decision_id: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> BenchmarkToolOutput:
            async def benchmark(db: str) -> Any:
                payload = await self._benchmark_indexed_view_candidate(
                    db,
                    session_id,
                    candidate_id,
                    view_definition,
                    sql=sql,
                    candidate_sql=candidate_sql,
                    read_mode=read_mode,
                    clustered_index_key=clustered_index_key,
                    extra_indexes=(
                        [item.model_dump(mode="python") for item in extra_indexes]
                        if extra_indexes is not None
                        else None
                    ),
                    view_name=view_name,
                    schema_name=schema_name,
                    index_name_prefix=index_name_prefix,
                    parameter_cases=_parameter_case_payloads(parameter_cases),
                    phase=phase,
                    runs=runs,
                    compare_order=compare_order,
                    lease_minutes=lease_minutes,
                    idempotency_key=idempotency_key,
                    preview=preview,
                    keep_for_session=keep_for_session,
                    max_call_seconds=max_call_seconds,
                    plan_reuse=plan_reuse,
                    slow_baseline_cases=slow_baseline_cases,
                )
                if preview or detail == "full":
                    return payload
                return _compact_benchmark_payload(payload)

            return cast(
                BenchmarkToolOutput,
                await self._run_tool(
                    "benchmark_indexed_view_candidate",
                    database_name,
                    benchmark,
                    deadline_provider=(
                        (lambda: self.tuning_sessions.get_session(session_id).deadline_at_utc)
                        if session_id
                        else None
                    ),
                    decision_id=decision_id,
                ),
            )

        @self.mcp.tool(
            description=(
                "Finalize a tuning session with the winning candidate, complete leaderboard, "
                "rejected experiments, and explicit stopping reason. Test objects a "
                "benchmark call kept for the session (keep_for_session) are dropped "
                "and listed in held_test_objects_dropped, each with dropped true or "
                "false. A drop that fails is retried on a new connection; when an "
                "object is still in place the result has cleanup_required true, "
                "result_status precondition, the failure per object and the DDL in "
                "remediation: that is not a clean finish. Call this tool again on the "
                "finalized session to retry the drop."
            ),
            annotations=ToolAnnotations(
                title="Finalize Tuning Session",
                # It drops the session's own leased test objects when any are held.
                readOnlyHint=False,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def finalize_tuning_session(
            session_id: str = Field(description="Tuning session identifier."),
            selected_candidate_id: str | None = Field(default=None),
            selection_scope: SelectionScope = Field(
                default="proven",
                description=(
                    "Select proven finalists by default. performance_only is an "
                    "explicit opt-in and never asserts semantic equivalence."
                ),
            ),
            stopping_reason: str = Field(
                min_length=1,
                max_length=STOPPING_REASON_MAX_LENGTH,
                description="Why the optimizer stopped.",
            ),
            logical_preconditions: list[dict[str, Any]] | None = Field(
                default=None,
                description=(
                    "Data properties the selected rewrite depends on, each verified in "
                    "the catalog before the session completes. Every entry has kind: "
                    "not_null | unique | trusted_foreign_key | trusted_check, with "
                    "schema_name and table_name. not_null: {kind, schema_name, "
                    "table_name, columns: [..]}. unique: the same fields; an enabled, "
                    "unfiltered unique key must lie within columns. trusted_foreign_key: "
                    "{kind, schema_name, table_name, columns, referenced_schema, "
                    "referenced_table, referenced_columns}. trusted_check: {kind, "
                    "schema_name, table_name, constraint_name, expected_predicate}. "
                    "A rewrite is deployment_ready only when its preconditions are "
                    "catalog-verified. One the catalog refutes raises and leaves the "
                    "session open."
                ),
            ),
            idempotency_key: str | None = Field(default=None),
            decision_id: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> SessionToolOutput:
            return cast(
                SessionToolOutput,
                await self._run_tool(
                    "finalize_tuning_session",
                    database_name,
                    lambda db: self._finalize_tuning_session(
                        db,
                        session_id,
                        selected_candidate_id,
                        stopping_reason,
                        idempotency_key,
                        selection_scope=selection_scope,
                        logical_preconditions=logical_preconditions,
                    ),
                    decision_id=decision_id,
                ),
            )

        @self.mcp.tool(
            description=(
                "Compare two read-only query results in one snapshot. The result is proven "
                "only when the complete bounded results, duplicates, shape, and required order "
                "match. Statements with a proven total order are compared as ordered rows, the "
                "others as multisets with duplicate counts; comparison_basis says which. A "
                "preflight gap that leaves the rows stable (possible ORDER BY ties, a row "
                "limit with ties, a UNION ALL rewrite, a changed or removed ORDER BY) does "
                "not stop the run: the result is then a tested_match, proof_scope "
                "snapshot_test, a test on today's data with ordered_rows_match, "
                "multiset_match and the caveats. A volatile statement (clock, NEWID, "
                "TABLESAMPLE, unordered row limit) is refused. max_seconds caps the run. "
                "It can read test objects a benchmark call kept for the session."
            ),
            annotations=ToolAnnotations(
                title="Compare Query Results",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def compare_query_results(
            baseline_sql: str = Field(description="Baseline read-only SQL."),
            candidate_sql: str = Field(description="Candidate read-only SQL."),
            compare_order: bool | None = Field(default=None),
            parameter_case: ParameterCaseInput | None = Field(
                default=None,
                description=(
                    "Required for parameterized SQL: one named case {name, values, "
                    "types, weight} with exact values and declared SQL types for "
                    "every parameter."
                ),
            ),
            parameter_values: dict[str, Any] | None = Field(
                default=None,
                description="Short form of parameter_case.values. Do not pass both.",
            ),
            parameter_types: dict[str, str] | None = Field(
                default=None,
                description="Short form of parameter_case.types.",
            ),
            parameter_cases: list[ParameterCaseInput] | None = Field(
                default=None,
                description=(
                    "Up to 4 named cases compared in one call, in place of "
                    "parameter_case: [{name, values, types}]. The result has one entry "
                    "per case under cases."
                ),
            ),
            max_seconds: float | None = Field(
                default=None,
                gt=0,
                description=(
                    "Time cap for the whole comparison; default 120, or the server tool "
                    "timeout less 5 seconds when that is lower. When it is spent the running "
                    "statement is cancelled and the result is timed_out with the arm "
                    "that was running (and baseline_seconds when the baseline finished). "
                    "It cannot exceed the server tool timeout; a lower effective cap is "
                    "reported in max_seconds_note."
                ),
            ),
            estimated_seconds: float | None = Field(
                default=None,
                gt=0,
                description=(
                    "Seconds one execution of the slower statement takes, when known "
                    "(explain_query with analyze, or a benchmark). Both statements run "
                    "once: when twice this exceeds max_seconds nothing is started and "
                    "the result (not_run) says which max_seconds to pass."
                ),
            ),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            async def compare(db: str) -> dict[str, Any]:
                # The tool timeout ends the call without a result; the cap must end first.
                ceiling = max(1.0, float(self.config.tool_timeout_seconds) - 5.0)
                result = await self.performance_workflows.compare_query_results(
                    db,
                    baseline_sql,
                    candidate_sql,
                    compare_order=compare_order,
                    parameter_case=_one_parameter_shape(
                        parameter_case, parameter_values, parameter_types
                    ),
                    parameter_cases=_parameter_case_payloads(parameter_cases),
                    max_seconds=min(
                        _DEFAULT_COMPARISON_SECONDS if max_seconds is None else max_seconds,
                        ceiling,
                    ),
                    estimated_seconds=estimated_seconds,
                )
                if max_seconds is not None and max_seconds > ceiling and isinstance(result, dict):
                    result["max_seconds_note"] = (
                        f"max_seconds was lowered to {ceiling:g}: the server tool timeout "
                        f"is {self.config.tool_timeout_seconds} seconds "
                        "(AZURE_SQL_TOOL_TIMEOUT_SECONDS)."
                    )
                # A comparison has no session: every test index on a table it
                # reads is named, with the session that holds it when one does.
                foreign = await self._test_objects_on_read_tables(db, [baseline_sql, candidate_sql])
                if foreign is not None and isinstance(result, dict):
                    result["test_objects_on_read_tables"] = foreign
                return result

            return await self._run_tool("compare_query_results", database_name, compare)

        @self.mcp.tool(
            description="Compare arbitrary redacted execution-plan summaries and sourced metrics.",
            annotations=ToolAnnotations(
                title="Compare Plan Summaries",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def compare_plan_summaries(
            baseline_summary: dict[str, Any] = Field(description="Baseline plan summary."),
            candidate_summary: dict[str, Any] = Field(description="Candidate plan summary."),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "compare_plan_summaries",
                database_name,
                lambda db: self._compare_plan_summaries(
                    db,
                    baseline_summary,
                    candidate_summary,
                ),
            )

        @self.mcp.tool(
            description=(
                "Prepare and preview a reversible CREATE VIEW or ALTER VIEW change. "
                "ALTER keeps the column list and WITH options from the existing "
                "view; they cannot be changed through prepare_view_change. "
                "Optimizer preparations are process-local previews; sandbox "
                "preparations become restart-safe only with explicit raw-SQL state opt-in."
            ),
            annotations=ToolAnnotations(
                title="Prepare View Change",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def prepare_view_change(
            schema_name: str = Field(description="Target schema."),
            view_name: str = Field(description="Target view."),
            definition: str = Field(
                description="Complete SELECT-shaped view body, without CREATE/ALTER VIEW."
            ),
            operation: str = Field(default="auto", description="auto, create, or alter"),
            schema_bound: bool | None = Field(
                default=None,
                description=(
                    "Leave unset to keep the existing view's SCHEMABINDING on ALTER "
                    "(unbound on CREATE). True adds it; False removes it."
                ),
            ),
            indexed_view: bool = Field(default=False),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "prepare_view_change",
                database_name,
                lambda db: self._prepare_view_change(
                    db,
                    schema_name,
                    view_name,
                    definition,
                    operation,
                    schema_bound,
                    indexed_view,
                    idempotency_key,
                ),
            )

        @self.mcp.tool(
            description=(
                "Apply one durable prepared view change in the sandbox profile after "
                "explicit review, raw-SQL state opt-in, and non-production policy approval."
            ),
            annotations=ToolAnnotations(
                title="Apply Prepared View Change",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def apply_prepared_view_change(
            change_id: str = Field(description="Prepared view change identifier."),
            reviewed_intent: bool = Field(
                description="Must be true to confirm this exact prepared definition."
            ),
            idempotency_key: str = Field(
                description="Caller-stable key for this exact apply."
            ),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "apply_prepared_view_change",
                database_name,
                lambda db: self._apply_prepared_view_change(
                    db,
                    change_id,
                    reviewed_intent,
                    idempotency_key,
                ),
            )

        @self.mcp.tool(
            description=(
                "Verify that a prepared view change has the expected definition "
                "and dependency set."
            ),
            annotations=ToolAnnotations(
                title="Verify View Change",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def verify_view_change(
            change_id: str = Field(description="Prepared view change identifier."),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "verify_view_change",
                database_name,
                lambda db: self._verify_view_change(db, change_id),
            )

        @self.mcp.tool(
            description=(
                "Restore the exact prior definition for a workflow-owned view change. "
                "Rollback is fenced against unrelated current definitions."
            ),
            annotations=ToolAnnotations(
                title="Rollback View Change",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def rollback_view_change(
            change_id: str = Field(description="Prepared view change identifier."),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "rollback_view_change",
                database_name,
                lambda db: self._rollback_view_change(db, change_id),
            )

        @self.mcp.tool(
            name="get_top_queries",
            description=(
                "List top queries from Query Store, one row per plan, per query or per "
                "query_hash. Rows carry query_text_preview (300 characters), "
                "query_text_id and query_hash; read the full text of one query with "
                "analyze_query_plan(query_id=...)."
            ),
            annotations=ToolAnnotations(
                title="Get Top Queries",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_top_queries(
            sort_by: str = Field(
                default="total_duration",
                description=(
                    "Sort by total_duration, avg_duration, cpu, executions, "
                    "logical_io, physical_io, memory, or resource_blend."
                ),
            ),
            window_minutes: int = Field(
                default=60,
                description="How far back to look in Query Store, in minutes.",
            ),
            limit: int = Field(default=10, description="Maximum number of rows to return."),
            as_of_utc: str | None = Field(default=None, description=AS_OF_DESCRIPTION),
            group_by: Literal["plan", "query", "query_hash"] = Field(
                default="plan",
                description=(
                    "plan: one row per plan. query: one row per query_id. query_hash: "
                    "folds the literal variants of one statement into one row; use it "
                    "for ad hoc workloads, where each query_id is small and the family "
                    "is large."
                ),
            ),
            replica_role: Literal["primary", "secondary", "geo_secondary", "geo_ha_secondary"]
            | None = Field(
                default=None,
                description=(
                    "Only runs recorded on this replica role. Default: every replica "
                    "blended. Needs Query Store for secondary replicas "
                    "(check_capabilities.query_store_replicas)."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_top_queries",
                database_name,
                lambda resolved_database: self._get_top_queries(
                    resolved_database,
                    sort_by,
                    window_minutes,
                    limit,
                    as_of_utc=as_of_utc,
                    group_by=group_by,
                    replica_role=replica_role,
                ),
            )

        @self.mcp.tool(
            description=(
                "Query Store runs that ended Aborted (timeout or client cancel) or with "
                "an Exception, in the recent window against the baseline rate. rising "
                "lists queries failing at least 2x their baseline rate with at least 3 "
                "failed runs; top_failing_queries ranks by count. These runs are not in "
                "the averages get_top_queries shows."
            ),
            annotations=ToolAnnotations(
                title="Get Failed Executions",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_failed_executions(
            recent_minutes: int = Field(default=1440, ge=1, le=43200),
            baseline_minutes: int = Field(default=20160, ge=1, le=129600),
            as_of_utc: str | None = Field(default=None, description=AS_OF_DESCRIPTION),
            top: int = Field(default=20, ge=1, le=100),
            replica_role: Literal["primary", "secondary", "geo_secondary", "geo_ha_secondary"]
            | None = Field(default=None, description="Only runs recorded on this replica role."),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "get_failed_executions",
                database_name,
                lambda db: self.query_store_trends.failed_executions(
                    db,
                    recent_minutes=recent_minutes,
                    baseline_minutes=baseline_minutes,
                    as_of_utc=as_of_utc,
                    top=top,
                    replica_role=replica_role,
                ),
            )

        @self.mcp.tool(
            description=(
                "Query Store over time: a bucketed series of executions, CPU, duration, "
                "reads, and plan count for one query (query_id) or the whole workload, "
                "plus a per-plan breakdown for one query. Query Store survives failovers "
                "and plan-cache eviction, so this answers when a problem started and "
                "whether a plan change lines up with it."
            ),
            annotations=ToolAnnotations(
                title="Get Query Store Trend",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_query_store_trend(
            query_id: int | None = Field(
                default=None,
                ge=1,
                description="Query Store query_id; omit for the whole workload.",
            ),
            window_minutes: int = Field(
                default=1440,
                ge=5,
                le=43200,
                description="Window length in minutes (up to 30 days).",
            ),
            bucket_minutes: int = Field(
                default=60,
                ge=5,
                le=1440,
                description="Bucket width in minutes; raised automatically past 500 buckets.",
            ),
            as_of_utc: str | None = Field(default=None, description=AS_OF_DESCRIPTION),
            replica_role: Literal["primary", "secondary", "geo_secondary", "geo_ha_secondary"]
            | None = Field(
                default=None,
                description=(
                    "Only runs recorded on this replica role. Default: every replica "
                    "blended. Needs Query Store for secondary replicas "
                    "(check_capabilities.query_store_replicas)."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_query_store_trend",
                database_name,
                lambda db: self.query_store_trends.trend(
                    db,
                    query_id=query_id,
                    window_minutes=window_minutes,
                    bucket_minutes=bucket_minutes,
                    as_of_utc=as_of_utc,
                    replica_role=replica_role,
                ),
            )

        @self.mcp.tool(
            description=(
                "Find queries whose Query Store performance got worse: the recent window's "
                "per-execution average against the baseline window right before it, ranked "
                "by weighted extra cost (recent executions x per-execution increase), with "
                "new plan ids flagged. No baseline history returns unavailable, never a "
                "clean bill of health."
            ),
            annotations=ToolAnnotations(
                title="Get Query Store Regressions",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_query_store_regressions(
            recent_minutes: int = Field(
                default=60,
                ge=5,
                le=43200,
                description="Length of the recent window, ending at as_of_utc or now.",
            ),
            baseline_minutes: int = Field(
                default=10080,
                ge=15,
                le=43200,
                description="Length of the baseline window immediately before the recent one.",
            ),
            metric: Literal["duration", "cpu", "logical_reads"] = Field(
                default="duration",
                description="Per-execution measure to compare.",
            ),
            min_executions: int = Field(
                default=10,
                ge=1,
                description="Minimum executions in each window for a query to be compared.",
            ),
            min_regression_pct: float = Field(
                default=25.0,
                ge=0,
                description="Report only queries at least this much worse.",
            ),
            top: int = Field(default=20, ge=1, le=200, description="Maximum regressions returned."),
            as_of_utc: str | None = Field(default=None, description=AS_OF_DESCRIPTION),
            replica_role: Literal["primary", "secondary", "geo_secondary", "geo_ha_secondary"]
            | None = Field(
                default=None,
                description=(
                    "Only runs recorded on this replica role. Default: every replica "
                    "blended. Needs Query Store for secondary replicas "
                    "(check_capabilities.query_store_replicas)."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_query_store_regressions",
                database_name,
                lambda db: self.query_store_trends.regressions(
                    db,
                    recent_minutes=recent_minutes,
                    baseline_minutes=baseline_minutes,
                    as_of_utc=as_of_utc,
                    metric=metric,
                    min_executions=min_executions,
                    min_regression_pct=min_regression_pct,
                    top=top,
                    replica_role=replica_role,
                ),
            )

        @self.mcp.tool(
            description=(
                "Extract SQL Server missing-index hints from estimated plans for up to "
                "10 queries. A per-query missing_index_count of zero means no Showplan "
                "hint was emitted for that plan. The filtered recommendations can also "
                "be empty when an existing index already covers an emitted hint; neither "
                "condition proves that no index could improve the query."
            ),
            annotations=ToolAnnotations(
                title="Analyze Query Indexes",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def analyze_query_indexes(
            queries: list[str] = Field(
                min_length=1,
                max_length=10,
                description="List of SQL SELECT queries to analyze (max 10).",
            ),
            auto_bind_params: bool = Field(
                default=False,
                description=(
                    "When true, automatically binds @param placeholders in queries "
                    "using column statistics before analyzing."
                ),
            ),
            parameter_values: dict[str, Any] | None = Field(
                default=None,
                description="Explicit parameter values for the supplied queries.",
            ),
            parameter_types: dict[str, str] | None = Field(
                default=None,
                description=(
                    "Optional exact SQL types keyed by parameter name and shared "
                    "across the supplied queries."
                ),
            ),
            include_all_existing_indexes: bool = Field(
                default=False,
                description=(
                    "When true, returns the full metadata of every index in the "
                    "database. The default lists the indexes of the analysed tables."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "analyze_query_indexes",
                database_name,
                lambda resolved_database: self._analyze_query_indexes(
                    resolved_database,
                    queries,
                    auto_bind_params,
                    parameter_values,
                    parameter_types,
                    include_all_existing_indexes=include_all_existing_indexes,
                ),
            )

        @self.mcp.tool(
            description="Analyze the database workload to identify resource-intensive queries and recommend optimal indexes.",
            annotations=ToolAnnotations(
                title="Analyze Workload Indexes",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def analyze_workload_indexes(
            window_minutes: int = Field(
                default=60,
                description="How far back to look in Query Store, in minutes.",
            ),
            top_n: int = Field(
                default=20,
                description="Number of top queries to analyze.",
            ),
            include_all_existing_indexes: bool = Field(
                default=False,
                description=(
                    "When true, returns the full metadata of every index in the "
                    "database. The default lists the indexes of the analysed tables."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "analyze_workload_indexes",
                database_name,
                lambda resolved_database: self.recommendations.analyze_workload_indexes(
                    resolved_database,
                    window_minutes,
                    top_n,
                    include_all_existing_indexes=include_all_existing_indexes,
                ),
            )

        @self.mcp.tool(
            description="Analyze missing-index and automatic tuning recommendations.",
            annotations=ToolAnnotations(
                title="Analyze Index Recommendations",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def analyze_index_recommendations(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "analyze_index_recommendations",
                database_name,
                self.recommendations.analyze,
            )

        @self.mcp.tool(
            description=(
                "Run the index optimization engine: analyzes workload from Query Store, "
                "generates index candidates from execution plans and DMVs, estimates sizes, "
                "scores using Pareto optimization (read benefit vs size vs write overhead), "
                "and returns budget-constrained ranked recommendations."
            ),
            annotations=ToolAnnotations(
                title="Optimize Indexes",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def optimize_indexes(
            window_minutes: int = Field(
                default=60,
                description="Query Store lookback window in minutes.",
            ),
            top_n: int = Field(
                default=30,
                description="Number of top queries to analyze.",
            ),
            budget_mb: float | None = Field(
                default=None,
                description="Max total index size in MB. None = unlimited.",
            ),
            alpha: float = Field(
                default=1.5,
                description="Size penalty weight (higher = prefer smaller indexes).",
            ),
            beta: float = Field(
                default=0.5,
                description="Write penalty weight (higher = avoid indexes on write-heavy tables).",
            ),
            min_improvement_pct: float = Field(
                default=5.0,
                description="Minimum impact percentage to include a candidate.",
            ),
            include_all_existing_indexes: bool = Field(
                default=False,
                description=(
                    "When true, returns the full metadata of every index in the "
                    "database. The default lists the indexes of the analysed tables."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "optimize_indexes",
                database_name,
                lambda db: self.index_optimizer.optimize(
                    db,
                    window_minutes,
                    top_n,
                    budget_mb,
                    alpha,
                    beta,
                    min_improvement_pct,
                    include_all_existing_indexes=include_all_existing_indexes,
                ),
            )

        @self.mcp.tool(
            description=(
                "Review a database's indexes against its real workload. Reads Query Store "
                "runtime totals and each query's stored plan for a window, finds the queries "
                "that hit each table and how (seek, scan, lookup, residual filters, sort "
                "needs), and combines that with existing index definitions, usage counters, "
                "statistics selectivity, and write activity. Returns per-table "
                "recommendations to create, extend (add includes), widen, consolidate, or "
                "drop indexes, or cluster a heap, each with supporting query ids, confidence, "
                "inert DDL with exact rollback, and a validation path; plus predicates no "
                "index can fix (route to sql-optimizer). Recommend-only: it never executes "
                "DDL. Needs only VIEW DATABASE STATE and VIEW DEFINITION; no install step."
            ),
            annotations=ToolAnnotations(
                title="Review Workload Indexes",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def review_workload_indexes(
            schema_name: str | None = Field(
                default=None,
                description="Limit the review to one schema (required with table_names).",
            ),
            table_names: list[str] | None = Field(
                default=None,
                description="Limit the review to these tables in schema_name.",
            ),
            lookback_days: int = Field(
                default=7,
                ge=1,
                le=90,
                description="Query Store window length in days. Cover a full business cycle where possible.",
            ),
            as_of_utc: str | None = Field(
                default=None,
                description=(
                    "Optional ISO-8601 UTC end of the window, for example "
                    "2026-09-30T05:00:00Z. Defaults to now; future instants are refused."
                ),
            ),
            objective: Literal["cpu", "duration", "logical_reads", "executions"] = Field(
                default="cpu",
                description="Which Query Store measure ranks the workload and attributes cost.",
            ),
            top_queries: int = Field(
                default=100,
                ge=1,
                le=500,
                description="How many top queries by the objective to analyse.",
            ),
            plans_per_query: int = Field(
                default=1,
                ge=1,
                le=3,
                description="Stored plans to analyse per query, most expensive first.",
            ),
            min_table_rows: int = Field(
                default=10_000,
                ge=0,
                description="Tables with fewer rows get no create/extend advice.",
            ),
            max_recommendations_per_table: int = Field(
                default=5,
                ge=1,
                le=20,
                description="Cap on actionable recommendations per table.",
            ),
            include_existing_index_review: bool = Field(
                default=True,
                description=(
                    "Also review existing indexes: duplicates, left-prefix redundancy, "
                    "unused indexes, heaps, and unindexed foreign keys."
                ),
            ),
            query_ids: list[int] | None = Field(
                default=None,
                max_length=50,
                description=(
                    "Query Store query ids that must be analysed whatever their rank "
                    "or table scope. Pass the query being tuned so its access paths "
                    "shape the advice. The report then leads with requested_queries "
                    "and requested_query_design (the index each of these queries would "
                    "get alone: equality columns, then ORDER BY, then one range column), "
                    "and returns only the recommendations that serve them."
                ),
            ),
            include_other_recommendations: bool = Field(
                default=False,
                description=(
                    "With query_ids: also return the recommendations that come from "
                    "other queries of the workload. By default they are counted in "
                    "requested_queries.other_recommendations_omitted and left out."
                ),
            ),
            include_change_plan: bool = Field(
                default=False,
                description=(
                    "Also return change_plan: the recommendations ordered into steps a "
                    "client can schedule (create, change, observe, remove), with what "
                    "was not planned and why. Every statement is inert text."
                ),
            ),
            soak_days: int = Field(
                default=7,
                ge=1,
                le=90,
                description="Days to observe between the changes and the removals in change_plan.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            async def review(db: str) -> dict[str, Any]:
                # A leftover test index under a policy prefix is not part of
                # the design. The built-in prefixes need no argument.
                extra_prefixes = tuple(
                    prefix
                    for prefix in self.database_policy.policy_for(db).test_index_name_prefixes
                    if prefix != TEST_INDEX_PREFIX
                )
                policy_prefixes: dict[str, Any] = (
                    {"test_index_name_prefixes": extra_prefixes} if extra_prefixes else {}
                )
                report = await self.workload_index_advisor.review(
                    db,
                    schema_name=schema_name,
                    table_names=table_names,
                    lookback_days=lookback_days,
                    as_of_utc=as_of_utc,
                    objective=objective,
                    top_queries=top_queries,
                    plans_per_query=plans_per_query,
                    min_table_rows=min_table_rows,
                    max_recommendations_per_table=max_recommendations_per_table,
                    include_existing_index_review=include_existing_index_review,
                    query_ids=query_ids,
                    **policy_prefixes,
                )
                if query_ids:
                    report = _lead_with_requested_queries(
                        report, query_ids, include_others=include_other_recommendations
                    )
                if include_change_plan:
                    report = {
                        **report,
                        "change_plan": build_index_change_plan(report, soak_days=soak_days),
                    }
                return report

            return await self._run_tool("review_workload_indexes", database_name, review)

        # --- Phase 9: Wait Statistics ---

        @self.mcp.tool(
            description=(
                "Get top wait statistics from sys.dm_db_wait_stats with category mapping "
                "(CPU, I/O, Lock, Memory, Network, etc.), benign wait filtering, and root-cause recommendations."
            ),
            annotations=ToolAnnotations(
                title="Get Wait Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_wait_stats(
            top_n: int = Field(default=20, description="Number of top waits to return."),
            sample_seconds: int = Field(
                default=0,
                ge=0,
                le=30,
                description=(
                    "0 returns counters accumulated since the last reset (window.since_utc). "
                    "1-30 samples twice that many seconds apart and returns only the waits "
                    "that accrued in between: use it for a live incident."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_wait_stats",
                database_name,
                lambda db: self.wait_stats.get_wait_stats(
                    db, top_n, sample_seconds=sample_seconds
                ),
            )

        @self.mcp.tool(
            description=(
                "Get per-query wait breakdown from Query Store wait stats. "
                "Ties waits to specific queries: 'this query is slow because of X'."
            ),
            annotations=ToolAnnotations(
                title="Get Query Wait Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_query_wait_stats(
            window_minutes: int = Field(
                default=60, description="Query Store lookback window in minutes."
            ),
            top_n: int = Field(default=20, description="Number of top results."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_query_wait_stats",
                database_name,
                lambda db: self.wait_stats.get_query_wait_stats(db, window_minutes, top_n),
            )

        @self.mcp.tool(
            description=(
                "Get currently waiting tasks right now — real-time view of what is blocked "
                "from sys.dm_os_waiting_tasks with SQL text and wait category."
            ),
            annotations=ToolAnnotations(
                title="Get Currently Waiting Tasks",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_currently_waiting_tasks(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_currently_waiting_tasks",
                database_name,
                self.wait_stats.get_currently_waiting_tasks,
            )

        # --- Phase 10: Lock & Transaction Diagnostics ---

        @self.mcp.tool(
            description=(
                "Get current lock details from sys.dm_tran_locks with owning session, "
                "lock mode (S, X, IX, IS, U, Sch-M), resource type, and SQL text."
            ),
            annotations=ToolAnnotations(
                title="Get Lock Details",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_lock_details(
            limit: int = Field(
                default=200,
                description="Maximum lock rows to return (waiting locks first, max 1000).",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_lock_details",
                database_name,
                lambda db: self.lock_diagnostics.get_lock_details(db, limit),
            )

        @self.mcp.tool(
            description=(
                "Get open transactions with duration, type (read/write, read-only), "
                "log bytes used, and warnings for long-running or idle-in-transaction."
            ),
            annotations=ToolAnnotations(
                title="Get Open Transactions",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_open_transactions(
            limit: int = Field(
                default=100,
                description="Maximum transactions to return (oldest first, max 1000).",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_open_transactions",
                database_name,
                lambda db: self.lock_diagnostics.get_open_transactions(db, limit),
            )

        @self.mcp.tool(
            description=(
                "Get recent deadlocks for an Azure SQL database from database-scoped "
                "Extended Events ring buffers (database_xml_deadlock_report) and, when "
                "master is allowlisted, Azure's file-backed deadlock telemetry. Returns "
                "victims, participants, lock resources, objects, and SQL text. When no "
                "source can capture deadlocks, result_status is precondition and "
                "remediation holds the capture-session DDL; it never reports a false zero."
            ),
            annotations=ToolAnnotations(
                title="Get Deadlock History",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_deadlock_history(
            max_events: int = Field(
                default=10,
                ge=1,
                le=200,
                description="Maximum number of deadlock events to return, newest first.",
            ),
            include_graph_xml: bool = Field(
                default=False,
                description="Also return each raw deadlock graph XML (bounded). source=live only.",
            ),
            source: Literal[
                "live", "auto", "extended_events", "log_analytics", "database_watcher", "loki"
            ] = Field(
                default="live",
                description=(
                    "live: the database's own ring buffers and Azure telemetry (minutes "
                    "to hours). auto: every recorder mapped for this database, merged "
                    "(days to weeks); a recorder name reads that one only."
                ),
            ),
            window_minutes: int = Field(
                default=1440,
                ge=1,
                le=129600,
                description="How far back to read a recorder. Not used with source=live.",
            ),
            as_of_utc: str | None = Field(default=None, description=AS_OF_DESCRIPTION),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            if source != "live":
                return await self._run_tool(
                    "get_deadlock_history",
                    database_name,
                    lambda db: self._get_recorded_deadlocks(
                        db, source, as_of_utc, window_minutes, max_events
                    ),
                )
            return await self._run_tool(
                "get_deadlock_history",
                database_name,
                lambda db: self.lock_diagnostics.get_deadlock_history(
                    db,
                    max_events,
                    master_available=self._master_allowlisted(),
                    include_graph_xml=include_graph_xml,
                ),
            )

        @self.mcp.tool(
            description=(
                "Read past events from the recorders a database already has: Extended "
                "Events sessions, Log Analytics diagnostic settings, database watcher, "
                "or Loki. category picks the question (blocking, timeout, deadlock, "
                "error, wait_stats, resource_metric, long_statement, audit_statement); "
                "category=coverage lists the recorders and what each covers. A window "
                "no recorder covers is precondition or unavailable with remediation, "
                "never empty."
            ),
            annotations=ToolAnnotations(
                title="Get Diagnostic History",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_diagnostic_history(
            category: Literal[
                "coverage",
                "blocking",
                "timeout",
                "deadlock",
                "error",
                "wait_stats",
                "session_snapshot",
                "resource_metric",
                "long_statement",
                "audit_statement",
            ] = Field(description="The history question, or coverage."),
            as_of_utc: str | None = Field(default=None, description=AS_OF_DESCRIPTION),
            window_minutes: int = Field(default=60, ge=1, le=129600),
            max_events: int = Field(default=200, ge=1, le=1000),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "get_diagnostic_history",
                database_name,
                lambda db: self._get_diagnostic_history(
                    db, category, as_of_utc, window_minutes, max_events
                ),
            )

        @self.mcp.tool(
            description=(
                "Memory grant pressure now: grant waiters, timeouts and forced grants "
                "(cumulative since engine start), the largest memory clerks and the "
                "security token cache size."
            ),
            annotations=ToolAnnotations(
                title="Get Memory Pressure",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_memory_pressure(
            top_clerks: int = Field(default=10, ge=1, le=50),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "get_memory_pressure",
                database_name,
                lambda db: self.memory_pressure.get_memory_pressure(db, top_clerks=top_clerks),
            )

        @self.mcp.tool(
            description=(
                "Elastic pool resource use over a recent window, to tell a pool limit "
                "from a database limit. A single database returns not_supported."
            ),
            annotations=ToolAnnotations(
                title="Get Elastic Pool Pressure",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_elastic_pool_pressure(
            window_minutes: int = Field(default=30, ge=1, le=1440),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "get_elastic_pool_pressure",
                database_name,
                lambda db: self.elastic_pool.get_pool_pressure(db, window_minutes=window_minutes),
            )

        @self.mcp.tool(
            description=(
                "Columnstore row group health: open and closed delta stores, deleted "
                "rows, undersized compressed row groups and why they were cut short. "
                "Optionally one table."
            ),
            annotations=ToolAnnotations(
                title="Get Columnstore Health",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_columnstore_health(
            worst_n: int = Field(default=10, ge=1, le=100),
            schema_name: str | None = Field(default=None),
            table_name: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "get_columnstore_health",
                database_name,
                lambda db: self.columnstore_health.get_columnstore_health(
                    db, worst_n=worst_n, schema_name=schema_name, table_name=table_name
                ),
            )

        @self.mcp.tool(
            description=(
                "Is this window normal for this database? scope=resources compares the "
                "window's CPU, IO, log and memory with the band for the same hour "
                "(needs master allowlisted for more than the last hour of history). "
                "scope=queries compares queries with their own Query Store history. "
                "Too little history returns insufficient_history with what is missing."
            ),
            annotations=ToolAnnotations(
                title="Get Database Baseline",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_database_baseline(
            scope: Literal["resources", "queries"] = Field(default="resources"),
            window_minutes: int = Field(default=60, ge=1, le=10080),
            as_of_utc: str | None = Field(
                default=None, description=AS_OF_DESCRIPTION + " scope=resources only."
            ),
            query_ids: list[int] | None = Field(
                default=None,
                description="scope=queries: these query_ids. Default: the top queries by CPU.",
            ),
            top: int = Field(default=5, ge=1, le=20, description="scope=queries without query_ids."),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "get_database_baseline",
                database_name,
                lambda db: (
                    self.database_baseline.query_bands(
                        db, window_minutes=window_minutes, query_ids=query_ids, top=top
                    )
                    if scope == "queries"
                    else self.database_baseline.resource_bands(
                        db,
                        window_minutes=window_minutes,
                        as_of_utc=as_of_utc,
                        master_available=self._master_allowlisted(),
                    )
                ),
            )

        @self.mcp.tool(
            description=(
                "Capacity verdict for the current service objective from up to 14 days "
                "of resource history: under_provisioned, right_sized, over_provisioned "
                "or insufficient_history, with the measurements behind it. Needs master "
                "allowlisted. No price is read or stated."
            ),
            annotations=ToolAnnotations(
                title="Assess Capacity",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def assess_capacity(
            window_days: int = Field(default=14, ge=1, le=14),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "assess_capacity",
                database_name,
                lambda db: self.capacity_assessment.assess(
                    db,
                    window_days=window_days,
                    master_available=self._master_allowlisted(),
                ),
            )

        @self.mcp.tool(
            description=(
                "Find application patterns that no single query shows: row-by-row "
                "callers (one statement shape run thousands of times) and chatty "
                "callers, from Query Store and the running sessions."
            ),
            annotations=ToolAnnotations(
                title="Analyze Application Patterns",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def analyze_application_patterns(
            window_minutes: int = Field(default=60, ge=1, le=10080),
            top: int = Field(default=20, ge=1, le=100),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "analyze_application_patterns",
                database_name,
                lambda db: self.application_patterns.analyze(
                    db, window_minutes=window_minutes, top=top
                ),
            )

        # --- Phase 11: Tempdb & Memory Grant Diagnostics ---

        @self.mcp.tool(
            description=(
                "Get per-session tempdb consumption: user/internal object allocations "
                "and net usage in MB."
            ),
            annotations=ToolAnnotations(
                title="Get Tempdb Usage",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_tempdb_usage(
            limit: int = Field(
                default=200,
                description="Maximum sessions to return (largest tempdb consumers first, max 1000).",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_tempdb_usage",
                database_name,
                lambda db: self.tempdb_memory.get_tempdb_usage(db, limit),
            )

        @self.mcp.tool(
            description=(
                "Get tempdb space breakdown: version store, user objects, internal objects, "
                "free space. Useful for diagnosing version store bloat under snapshot isolation."
            ),
            annotations=ToolAnnotations(
                title="Get Tempdb Space Breakdown",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_tempdb_space_breakdown(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_tempdb_space_breakdown",
                database_name,
                self.tempdb_memory.get_tempdb_space_breakdown,
            )

        @self.mcp.tool(
            description=(
                "Get active and pending memory grants. Identifies queries waiting for memory "
                "(RESOURCE_SEMAPHORE) and queries likely spilling to tempdb."
            ),
            annotations=ToolAnnotations(
                title="Get Memory Grants",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_memory_grants(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_memory_grants",
                database_name,
                self.tempdb_memory.get_memory_grants,
            )

        @self.mcp.tool(
            description=(
                "Get MCP server connection pool statistics: per-database acquire/"
                "release/discard counts, peak utilization, and possible connection "
                "leaks. Diagnoses MCP-side slowness without touching the database."
            ),
            annotations=ToolAnnotations(
                title="Get Connection Pool Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=False,
            ),
        )
        async def get_connection_pool_stats() -> ResponseType:
            return self._format_response(
                {
                    "server": self.config.server,
                    "pool_size_per_database": self.config.pool_size,
                    "metrics": self.pool.get_metrics(),
                    "possible_leaks": self.pool.check_leaked_connections(),
                }
            )

        # --- Phase 12: I/O & Azure Resource Governance ---

        @self.mcp.tool(
            description=(
                "Get per-file I/O stats: read/write latency, throughput, stall times. "
                "Warns when average latency exceeds 20ms threshold."
            ),
            annotations=ToolAnnotations(
                title="Get I/O Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_io_stats(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_io_stats",
                database_name,
                self.resource_governance.get_io_stats,
            )

        @self.mcp.tool(
            description=(
                "Get Azure resource governance limits: max CPU%, IOPS, log rate, memory, "
                "workers, and current service tier/objective."
            ),
            annotations=ToolAnnotations(
                title="Get Resource Limits",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_resource_limits(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_resource_limits",
                database_name,
                self.resource_governance.get_resource_limits,
            )

        @self.mcp.tool(
            description=(
                "Get resource utilization history against the database's limits: CPU, data "
                "I/O, log write, memory, and workers. Windows up to 60 minutes use "
                "sys.dm_db_resource_stats (15-second samples); longer windows, up to 14 days, "
                "use sys.resource_stats from master as hourly average and maximum buckets. "
                "Includes sustained-pressure warnings."
            ),
            annotations=ToolAnnotations(
                title="Get Resource Stats History",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_resource_stats_history(
            window_minutes: int = Field(
                default=60,
                ge=1,
                le=20160,
                description="How far back to look, in minutes (up to 14 days).",
            ),
            source: Literal["auto", "recent", "long_term"] = Field(
                default="auto",
                description=(
                    "auto: recent for 60 minutes or less, long_term beyond. long_term "
                    "needs master in the database allowlist."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_resource_stats_history",
                database_name,
                lambda db: self.resource_governance.get_resource_stats_history(
                    db,
                    window_minutes,
                    source=source,
                    master_available=self._master_allowlisted(),
                ),
            )

        @self.mcp.tool(
            description=(
                "Diagnose an Azure SQL database in one call: resource use against its limits "
                "and its own baseline, waits (optionally a live interval sample), blocking, "
                "memory and pool pressure, failed executions, the top Query Store "
                "consumers, regressions and version store health. Returns verdict, a "
                "one-sentence conclusion, stories (a cause with its supporting facts, a "
                "confidence, a fix and next_tools with arguments), leads (a fact with "
                "nothing to corroborate it), facts (each value against its bar, marked "
                "verified or inferred) and findings (the actionable facts with "
                "next_tools). sources and gaps name what could not be read; an unread "
                "source is never treated as healthy. Start incident triage here."
            ),
            annotations=ToolAnnotations(
                title="Diagnose Database",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def diagnose_database(
            window_minutes: int = Field(
                default=60,
                ge=5,
                le=10080,
                description="Window for resource history, top queries, and the recent side of regressions.",
            ),
            sample_seconds: int = Field(
                default=0,
                ge=0,
                le=30,
                description="Seconds to sample waits for a live interval; 0 uses cumulative counters.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "diagnose_database",
                database_name,
                lambda db: self.database_diagnosis.diagnose(
                    db,
                    window_minutes=window_minutes,
                    sample_seconds=sample_seconds,
                    master_available=self._master_allowlisted(),
                ),
            )

        @self.mcp.tool(
            description=(
                "Get persistent version store (accelerated database recovery) health: PVS "
                "size and share of used data space, cleaner times, aborted transactions "
                "awaiting cleanup, and the oldest open and snapshot transactions that hold "
                "cleanup back, with findings. ADR is always on in Azure SQL Database, so a "
                "long open transaction can grow storage here."
            ),
            annotations=ToolAnnotations(
                title="Get Version Store Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_version_store_stats(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_version_store_stats",
                database_name,
                self.version_store.get_version_store_stats,
            )

        # --- Phase 22: Azure SQL Diagnostic Query Parity ---

        @self.mcp.tool(
            description=(
                "Get Azure SQL database configuration inventory: version, read-only "
                "instance settings, database properties, scoped configurations, Query Store, "
                "automatic tuning, geo-replication links, and Azure DB properties."
            ),
            annotations=ToolAnnotations(
                title="Get Database Configuration",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_database_configuration(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_database_configuration",
                database_name,
                self.diagnostics.get_database_configuration,
            )

        @self.mcp.tool(
            description=(
                "Get Azure SQL storage diagnostics: database/file size, log usage, "
                "VLF counts, last VLF status, and high-usage warnings."
            ),
            annotations=ToolAnnotations(
                title="Get Storage Diagnostics",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_storage_diagnostics(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_storage_diagnostics",
                database_name,
                self.diagnostics.get_storage_diagnostics,
            )

        @self.mcp.tool(
            description=(
                "Get connection diagnostics: connection counts by client IP, session "
                "summary, and optional bounded input-buffer text for current database sessions."
            ),
            annotations=ToolAnnotations(
                title="Get Connection Diagnostics",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )
        async def get_connection_diagnostics(
            limit: int = Field(default=50, description="Maximum rows per detail section."),
            include_input_buffer: bool = Field(
                default=False,
                description=(
                    "Include sys.dm_exec_input_buffer details when available. This can expose "
                    "sensitive SQL text and literals, so it is disabled by default."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_connection_diagnostics",
                database_name,
                lambda db: self.diagnostics.get_connection_diagnostics(
                    db,
                    limit=limit,
                    include_input_buffer=include_input_buffer,
                ),
            )

        @self.mcp.tool(
            description=(
                "Get top cached statements from sys.dm_exec_query_stats. Returns "
                "bounded text previews and plan-cache metrics without raw plan XML."
            ),
            annotations=ToolAnnotations(
                title="Get Top Cached Queries",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_top_cached_queries(
            sort_by: str = Field(
                default="total_worker_time",
                description=(
                    "Sort by execution_count, total_worker_time, avg_worker_time, "
                    "total_elapsed_time, avg_elapsed_time, total_logical_reads, "
                    "total_physical_reads, or total_logical_writes."
                ),
            ),
            limit: int = Field(default=25, description="Maximum cached queries to return."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_top_cached_queries",
                database_name,
                lambda db: self.diagnostics.get_top_cached_queries(db, sort_by, limit),
            )

        @self.mcp.tool(
            description=(
                "Get cached stored procedure and UDF execution statistics. Returns "
                "bounded routine metrics and missing-index flags without raw plan XML."
            ),
            annotations=ToolAnnotations(
                title="Get Cached Routine Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_cached_routine_stats(
            routine_type: str = Field(
                default="all",
                description="Routine type: all, procedure, or function.",
            ),
            sort_by: str = Field(
                default="total_worker_time",
                description=(
                    "Sort by execution_count, total_worker_time, avg_worker_time, "
                    "total_elapsed_time, avg_elapsed_time, total_logical_reads, "
                    "total_physical_reads, or total_logical_writes."
                ),
            ),
            limit: int = Field(default=25, description="Maximum routines per section."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_cached_routine_stats",
                database_name,
                lambda db: self.diagnostics.get_cached_routine_stats(
                    db,
                    routine_type=routine_type,
                    sort_by=sort_by,
                    limit=limit,
                ),
            )

        @self.mcp.tool(
            description=(
                "Get object and index diagnostics: write-heavy nonclustered indexes, "
                "read/write usage, volatile stats, columnstore row groups, "
                "index lock waits, and resumable index rebuilds. The buffer footprint "
                "is read only with include_buffer_usage=true."
            ),
            annotations=ToolAnnotations(
                title="Get Object Index Diagnostics",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_object_index_diagnostics(
            schema_name: str | None = Field(
                default=None,
                description="Optional schema filter.",
            ),
            table_name: str | None = Field(
                default=None,
                description="Optional table filter.",
            ),
            limit: int = Field(default=25, description="Maximum rows per detail section."),
            include_buffer_usage: bool = Field(
                default=False,
                description=(
                    "When true, reads sys.dm_os_buffer_descriptors. That walks the "
                    "whole buffer pool, also for one table; avoid it during an incident."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_object_index_diagnostics",
                database_name,
                lambda db: self.diagnostics.get_object_index_diagnostics(
                    db,
                    schema_name=schema_name,
                    table_name=table_name,
                    limit=limit,
                    include_buffer_usage=include_buffer_usage,
                ),
            )

        # --- Phase 13: Statistics & Plan Cache ---

        @self.mcp.tool(
            description=(
                "Check statistics health: stale stats, high modification counts, "
                "low sample rates. Flags stats needing UPDATE STATISTICS."
            ),
            annotations=ToolAnnotations(
                title="Check Statistics Health",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def check_statistics_health(
            stale_days: int = Field(
                default=7, description="Flag stats not updated in this many days."
            ),
            mod_pct_threshold: float = Field(
                default=20.0,
                description="Flag stats where modification_counter exceeds this % of rows.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "check_statistics_health",
                database_name,
                lambda db: self.plan_cache.check_statistics_health(
                    db, stale_days, mod_pct_threshold
                ),
            )

        @self.mcp.tool(
            description=(
                "Analyze plan cache: plan type distribution, single-use plan bloat, "
                "top plans by size. Detects ad-hoc query pollution."
            ),
            annotations=ToolAnnotations(
                title="Get Plan Cache Analysis",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_plan_cache_analysis(
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_plan_cache_analysis",
                database_name,
                self.plan_cache.get_plan_cache_analysis,
            )

        @self.mcp.tool(
            description=(
                "Rank cached statements by recompile_count (plan generations, from "
                "sys.dm_exec_query_stats) and flag recompiling_now: at least 10 plan "
                "generations and a compile in the last 60 minutes "
                "(recompiling_now_rule). Returns queries with minutes_since_last_compile, "
                "excessive_recompile_count and warnings. execution_count restarts at each "
                "compile, so no recompile ratio is given. OPTION (RECOMPILE) statements "
                "are never cached and are not listed."
            ),
            annotations=ToolAnnotations(
                title="Get Query Compilation Stats",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_query_compilation_stats(
            top_n: int = Field(default=20, description="Number of top recompiled queries."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_query_compilation_stats",
                database_name,
                lambda db: self.plan_cache.get_query_compilation_stats(db, top_n),
            )

        # --- Phase 14: Parameter Sniffing & Query Regression ---

        @self.mcp.tool(
            description=(
                "Detect parameter sniffing: queries with multiple plans where "
                "MAX(avg_duration) / MIN(avg_duration) exceeds threshold (default 10x)."
            ),
            annotations=ToolAnnotations(
                title="Detect Parameter Sniffing",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def detect_parameter_sniffing(
            variance_threshold: float = Field(
                default=10.0,
                description="Min ratio of worst/best plan duration to flag (e.g., 10 = 10x worse).",
            ),
            window_minutes: int = Field(
                default=1440,
                description="Query Store lookback window in minutes (default 24 hours).",
            ),
            top_n: int = Field(default=20, description="Max number of results."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "detect_parameter_sniffing",
                database_name,
                lambda db: self.query_regression.detect_parameter_sniffing(
                    db, variance_threshold, window_minutes, top_n
                ),
            )

        @self.mcp.tool(
            description=(
                "Surface automatic tuning regression recommendations from "
                "sys.dm_db_tuning_recommendations with plan forcing scripts."
            ),
            annotations=ToolAnnotations(
                title="Detect Regressed Queries",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def detect_regressed_queries(
            window_minutes: int = Field(
                default=1440,
                ge=1,
                description="Query Store lookback window in minutes (default 24 hours).",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "detect_regressed_queries",
                database_name,
                lambda db: self.query_regression.detect_regressed_queries(
                    db, window_minutes
                ),
            )

        @self.mcp.tool(
            description=(
                "Compare two query plans side-by-side: operators, duration, CPU, I/O. "
                "If plan IDs not specified, compares best vs worst plan by duration."
            ),
            annotations=ToolAnnotations(
                title="Compare Query Plans",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def compare_query_plans(
            query_id: int = Field(description="Query Store query_id."),
            plan_id_a: int | None = Field(
                default=None,
                description=(
                    "Baseline plan_id. Give both plan ids or neither (then best vs "
                    "worst by duration). Ratios are plan_id_b / plan_id_a "
                    "(comparison.compared_plan_id / baseline_plan_id)."
                ),
            ),
            plan_id_b: int | None = Field(
                default=None,
                description=(
                    "Compared plan_id. Give both plan ids or neither (then worst "
                    "by duration)."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "compare_query_plans",
                database_name,
                lambda db: self.query_regression.compare_query_plans(
                    db, query_id, plan_id_a, plan_id_b
                ),
            )

        @self.mcp.tool(
            description=(
                "List every forced plan (forced_plans) with its plan_forcing_type_desc, "
                "force_failure_count and last_force_failure_reason_desc, its Regular-run "
                "averages, aborted_executions and exception_executions, and "
                "recent_execution_count against query_recent_execution_count (the query "
                "runs while the forced plan does not when forcing fails now). warnings "
                "name the stale plans (no run in 7 days, or last_execution_unknown) and "
                "the failing ones; stale_count and failing_count total them."
            ),
            annotations=ToolAnnotations(
                title="Get Forced Plans",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def get_forced_plans(
            window_minutes: int = Field(
                default=1440,
                ge=1,
                description="Query Store lookback window in minutes (default 24 hours).",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_forced_plans",
                database_name,
                lambda db: self.query_regression.get_forced_plans(db, window_minutes),
            )

        @self.mcp.tool(
            description=(
                "Extract the compiled parameter values behind each Query Store plan for "
                "one query — the parameter buckets a tuning pass must test. Each distinct "
                "compiled set produced its own plan shape in production; pair with "
                "boundary/NULL/empty cases the history cannot show."
            ),
            annotations=ToolAnnotations(
                title="Get Query Parameter Buckets",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def get_query_parameter_buckets(
            query_id: int = Field(description="Query Store query_id."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_query_parameter_buckets",
                database_name,
                lambda db: self.query_regression.get_query_parameter_buckets(db, query_id),
            )

        @self.mcp.tool(
            description=(
                "Review Query Store health, parameter sensitivity, regressions, "
                "forced-plan failures, and ranked force/unforce candidates."
            ),
            annotations=ToolAnnotations(
                title="Plan Health Review",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def plan_health_review(
            window_minutes: int = Field(
                default=1440,
                description="Query Store lookback window in minutes.",
            ),
            top_n: int = Field(default=20, description="Maximum ranked findings to return."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "plan_health_review",
                database_name,
                lambda db: self._plan_health_review(
                    db,
                    window_minutes=window_minutes,
                    top_n=top_n,
                ),
            )

        @self.mcp.tool(
            description=(
                "Preview one plan-enforcement cycle. This tool is permanently read-only; "
                "use the prepared plan-action workflow for reviewed mutations."
            ),
            annotations=ToolAnnotations(
                title="Plan Enforcer Tick",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def plan_enforcer_tick(
            window_minutes: int = Field(
                default=1440,
                description="Query Store lookback window in minutes.",
            ),
            max_actions: int = Field(
                default=1,
                description="Maximum force/unforce actions to preview or apply.",
            ),
            dry_run: bool = Field(
                default=True,
                description="Compatibility flag. False is rejected; preview is permanent.",
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            if not dry_run:
                self._raise_tool_error(
                    "preview_only",
                    "plan_enforcer_tick is permanently preview-only; prepare a reviewed action.",
                )
            return await self._run_tool(
                "plan_enforcer_tick",
                database_name,
                lambda db: self.plan_enforcement.tick(
                    db,
                    window_minutes=window_minutes,
                    max_actions=max_actions,
                    dry_run=True,
                ),
            )

        @self.mcp.tool(
            description=(
                "Review Query Store regressions and forced-plan health, then rank "
                "read-only candidate plan force/unforce actions."
            ),
            annotations=ToolAnnotations(
                title="Review Plan Enforcement",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def review_plan_enforcement(
            window_minutes: int = Field(
                default=1440,
                description="Query Store lookback window in minutes.",
            ),
            top_n: int = Field(default=20, description="Maximum ranked actions to return."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "review_plan_enforcement",
                database_name,
                lambda db: self.plan_enforcement.review(
                    db,
                    window_minutes=window_minutes,
                    top_n=top_n,
                ),
            )

        @self.mcp.tool(
            description=(
                "Preview an exact reversible Query Store force/unforce action and "
                "record an audit entry without executing it."
            ),
            annotations=ToolAnnotations(
                title="Dry Run Plan Action",
                readOnlyHint=False,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def dry_run_plan_action(
            action: str = Field(description="'force' or 'unforce'."),
            query_id: int = Field(description="Query Store query_id."),
            plan_id: int = Field(description="Query Store plan_id."),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "dry_run_plan_action",
                database_name,
                lambda db: self.plan_enforcement.dry_run_action(
                    db,
                    action=action,
                    query_id=query_id,
                    plan_id=plan_id,
                ),
            )

        @self.mcp.tool(
            description=(
                "Capture exact Query Store control state and persist a reviewed, redacted "
                "plan-action intent. Automatic Tuning ownership is review-only."
            ),
            annotations=ToolAnnotations(
                title="Prepare Plan Action",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def prepare_plan_action(
            session_id: str = Field(description="Shared tuning session identifier."),
            operation: str = Field(
                description="force_plan, unforce_plan, set_hints, or clear_hints."
            ),
            query_id: int = Field(description="Query Store query_id."),
            evidence: dict[str, Any] = Field(description="Reviewed pre-change evidence window."),
            reviewed_by: str = Field(description="Human reviewer identifier."),
            reason: str = Field(description="Reviewed reason for the action."),
            idempotency_key: str = Field(description="Unique idempotency key."),
            candidate_id: str | None = Field(default=None),
            plan_id: int | None = Field(default=None),
            query_hints: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "prepare_plan_action",
                database_name,
                lambda db: self._prepare_plan_action(
                    db,
                    session_id=session_id,
                    candidate_id=candidate_id,
                    operation=operation,
                    query_id=query_id,
                    plan_id=plan_id,
                    query_hints=query_hints,
                    evidence=evidence,
                    reviewed_by=reviewed_by,
                    reason=reason,
                    idempotency_key=idempotency_key,
                ),
            )

        @self.mcp.tool(
            description=(
                "Apply one prepared intent after rechecking server policy, database policy, "
                "kill switch, explicit authorization, ownership, and exact prior state."
            ),
            annotations=ToolAnnotations(
                title="Apply Prepared Plan Action",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def apply_prepared_plan_action(
            intent_id: str = Field(description="Prepared plan-action intent identifier."),
            authorization_reference: str = Field(
                description="Explicit per-action authorization or change reference."
            ),
            confirmation_token: str | None = Field(
                default=None,
                description=(
                    "Required when the database policy environment is production or not "
                    "a known non-production label: the single-use token "
                    "prepare_plan_action returned as confirmation.confirmation_token. "
                    "A person must pass it back; it expires after 15 minutes."
                ),
            ),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "apply_prepared_plan_action",
                database_name,
                lambda db: self._apply_prepared_plan_action(
                    db,
                    intent_id,
                    authorization_reference,
                    confirmation_token,
                ),
            )

        @self.mcp.tool(
            description=(
                "Read pre/post-apply Query Store runtime stats for the query, check "
                "force failures and that the forced plan ran most executions, then "
                "keep, hold, or restore the exact prior force and hint state. "
                "Parameter buckets are reported as unavailable."
            ),
            annotations=ToolAnnotations(
                title="Verify Plan Action",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def verify_plan_action(
            intent_id: str = Field(description="Applied plan-action intent identifier."),
            candidate_evidence: dict[str, Any] | None = Field(
                default=None,
                description=(
                    "Optional caller context. The MCP reads post-apply Query Store "
                    "evidence itself; caller evidence can turn a keep into a hold "
                    "but never produces a keep."
                ),
            ),
            authorization_reference: str = Field(
                description="Authorization used if verification requires rollback."
            ),
            decision_id: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "verify_plan_action",
                database_name,
                lambda db: self.plan_actions.verify(
                    db,
                    intent_id,
                    candidate_evidence=candidate_evidence,
                    authorization_reference=authorization_reference,
                ),
                decision_id=decision_id,
            )

        @self.mcp.tool(
            description="Restore the exact force-plan and Query Store hint state captured at review.",
            annotations=ToolAnnotations(
                title="Rollback Plan Action",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def rollback_plan_action(
            intent_id: str = Field(description="Plan-action intent identifier."),
            authorization_reference: str = Field(
                description="Explicit rollback authorization or change reference."
            ),
            reason: str = Field(default="explicit rollback requested"),
            confirmation_token: str | None = Field(
                default=None,
                description=(
                    "For a production database: call once without it to receive a "
                    "single-use rollback token (nothing is changed), then call again "
                    "with that token."
                ),
            ),
            decision_id: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "rollback_plan_action",
                database_name,
                lambda db: self._rollback_plan_action(
                    db,
                    intent_id,
                    authorization_reference,
                    reason,
                    confirmation_token,
                ),
                decision_id=decision_id,
            )

        @self.mcp.tool(
            description=(
                "Analyze operational Azure SQL database health. Query-performance triage "
                "belongs to collect_performance_evidence, which uses resource, Query Store, "
                "wait, blocking, statistics, parameter-sensitivity, and regression evidence."
            ),
            annotations=ToolAnnotations(
                title="Analyze DB Health",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def analyze_db_health(
            health_type: str = Field(
                default="all",
                description=(
                    "Health type: connection, constraint, replication, identity, query_store, "
                    "tuning, resource, storage, statistics, or all."
                ),
            ),
            database_name: str | None = Field(
                default=None,
                description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
            ),
            decision_id: str | None = Field(default=None),
        ) -> ResponseType:
            return await self._run_tool(
                "analyze_db_health",
                database_name,
                lambda resolved_database: self.health.analyze(
                    resolved_database,
                    health_type,
                ),
                decision_id=decision_id,
            )

        if self.config.access_mode == AccessMode.UNRESTRICTED:
            @self.mcp.tool(
                description=(
                    "Preview a Query Store force/unforce action. Direct execution is "
                    "blocked; use prepare_plan_action and apply_prepared_plan_action."
                ),
                annotations=ToolAnnotations(
                    title="Apply Plan Action",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def apply_plan_action(
                action: str = Field(description="'force' or 'unforce'."),
                query_id: int = Field(description="Query Store query_id."),
                plan_id: int = Field(description="Query Store plan_id."),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Must remain true. Direct plan actions are permanently preview-only."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "apply_plan_action",
                    database_name,
                    lambda db: self.plan_enforcement.apply_action(
                        db,
                        action=action,
                        query_id=query_id,
                        plan_id=plan_id,
                        dry_run=dry_run,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Run DBA T-SQL against an allowlisted initial database. Direct or "
                    "statically recoverable DROP DATABASE statements are rejected, but SQL "
                    "assembled only at runtime cannot be proven or blocked. Each call makes "
                    "one submission with no retry, uses an isolated connection that is discarded, "
                    "and drains every result set. GO is a client batch separator, not T-SQL, "
                    "and is unsupported."
                ),
                annotations=ToolAnnotations(
                    title="Execute Unrestricted T-SQL",
                    readOnlyHint=False,
                    destructiveHint=True,
                    idempotentHint=False,
                    openWorldHint=True,
                ),
            )
            async def execute_tsql_unrestricted(
                sql: str = Field(
                    description=(
                        "One T-SQL batch to execute. Do not include the client-side GO separator. "
                        "DROP DATABASE is rejected when directly or statically recoverable."
                    )
                ),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Preview and audit without execution by default. Set False only "
                        "with AZURE_SQL_WRITE_POLICY=apply."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name. Defaults to AZURE_SQL_DEFAULT_DATABASE.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "execute_tsql_unrestricted",
                    database_name,
                    lambda resolved_database: self._execute_unrestricted_sql(
                        resolved_database,
                        sql,
                        dry_run,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Rebuild or reorganize a table index. "
                    "REBUILD recreates the index (fixes fragmentation, updates stats). "
                    "REORGANIZE is lighter-weight (online, defragments leaf level only). "
                    "Use ONLINE=True (default) to avoid blocking queries during rebuild."
                ),
                annotations=ToolAnnotations(
                    title="Rebuild Index",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def rebuild_index(
                schema_name: str = Field(description="Schema name (e.g. 'dbo')."),
                table_name: str = Field(description="Table name."),
                index_name: str = Field(description="Index name to rebuild/reorganize."),
                operation: str = Field(
                    default="REBUILD",
                    description="REBUILD or REORGANIZE.",
                ),
                online: bool = Field(
                    default=True,
                    description="Use ONLINE=ON for rebuild (avoids blocking). Ignored for REORGANIZE.",
                ),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Preview and audit without execution by default. Set False only "
                        "with AZURE_SQL_WRITE_POLICY=apply."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "rebuild_index",
                    database_name,
                    lambda db: self._rebuild_index(
                        db, schema_name, table_name, index_name, operation, online, dry_run,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Update statistics for a table or specific statistic. "
                    "Refreshes query optimizer cost estimates. Use after large data changes "
                    "or when stale statistics are detected by health checks."
                ),
                annotations=ToolAnnotations(
                    title="Update Statistics",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def update_statistics(
                schema_name: str = Field(description="Schema name (e.g. 'dbo')."),
                table_name: str = Field(description="Table name."),
                stat_name: str | None = Field(
                    default=None,
                    description="Optional statistic name. If omitted, updates all statistics on the table.",
                ),
                sample_percent: int | None = Field(
                    default=None,
                    description="Sample percentage (1-100). If omitted, SQL Server chooses automatically.",
                ),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Preview and audit without execution by default. Set False only "
                        "with AZURE_SQL_WRITE_POLICY=apply."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "update_statistics",
                    database_name,
                    lambda db: self._update_statistics(
                        db, schema_name, table_name, stat_name, sample_percent, dry_run,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Preview forcing or unforcing a Query Store plan. Direct execution "
                    "is blocked; use the reviewed prepared plan-action workflow."
                ),
                annotations=ToolAnnotations(
                    title="Force/Unforce Query Plan",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def force_query_plan(
                query_id: int = Field(description="Query Store query_id."),
                plan_id: int = Field(description="Query Store plan_id."),
                unforce: bool = Field(
                    default=False,
                    description="Set True to unforce (release) the plan instead of forcing it.",
                ),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Must remain true. Direct force/unforce is permanently preview-only."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "force_query_plan",
                    database_name,
                    lambda db: self._force_query_plan(
                        db, query_id, plan_id, unforce, dry_run,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Preview validated Query Store hints for a query. Direct execution is "
                    "blocked; use the reviewed prepared plan-action workflow."
                ),
                annotations=ToolAnnotations(
                    title="Set Query Store Hints",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def set_query_store_hints(
                query_id: int = Field(description="Query Store query_id."),
                query_hints: str = Field(
                    description=(
                        "Hints as a single OPTION(...) clause, e.g. OPTION(RECOMPILE) or "
                        "OPTION(OPTIMIZE FOR (@p = 42)). Validated against an allowlist."
                    ),
                ),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Must remain true. Direct Query Store hint changes are preview-only."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "set_query_store_hints",
                    database_name,
                    lambda db: self._set_query_store_hints(
                        db, query_id, query_hints, dry_run,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Preview clearing Query Store hints. Direct execution is blocked; "
                    "use the reviewed prepared plan-action workflow."
                ),
                annotations=ToolAnnotations(
                    title="Clear Query Store Hints",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def clear_query_store_hints(
                query_id: int = Field(description="Query Store query_id."),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Must remain true. Direct Query Store hint changes are preview-only."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "clear_query_store_hints",
                    database_name,
                    lambda db: self._clear_query_store_hints(db, query_id, dry_run),
                )

            @self.mcp.tool(
                description=(
                    "Preview a disposable test index definition. Direct creation is blocked; "
                    "use benchmark_index_candidate for a leased sandbox measurement."
                ),
                annotations=ToolAnnotations(
                    title="Create Test Index",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=False,
                    openWorldHint=False,
                ),
            )
            async def create_test_index(
                schema_name: str = Field(description="Schema name (e.g. 'dbo')."),
                table_name: str = Field(description="Table name."),
                index_name: str = Field(
                    description=(
                        "Index name, under IX_Testing_ or a prefix the database policy "
                        "lists in test_index_name_prefixes. Any other name is not "
                        "refused: the preview moves it under the policy prefix and "
                        "index_name_note gives the name used."
                    ),
                ),
                key_columns: list[str] = Field(
                    description=(
                        "Key columns in order; each may carry an ASC/DESC suffix, "
                        "e.g. ['ShipDate', 'StatusCode DESC']."
                    ),
                ),
                include_columns: list[str] | None = Field(
                    default=None,
                    description="Optional INCLUDE columns (plain column names).",
                ),
                online: bool = Field(
                    default=True,
                    description=(
                        "Use ONLINE=ON (avoids blocking during the build). A table with a "
                        "clustered columnstore index is built offline only: see offline_build."
                    ),
                ),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Must remain true. Direct test-index creation is preview-only."
                    ),
                ),
                index_kind: Literal["rowstore", "nonclustered_columnstore"] = Field(
                    default="rowstore",
                    description=(
                        "rowstore (default) or nonclustered_columnstore, where "
                        "key_columns lists the columns of the index."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                async def preview(db: str) -> dict[str, Any]:
                    # One catalog read, so the preview shows the DDL the
                    # engine accepts on this table.
                    table_facts = await self._resolve_canonical_table_identity(
                        db, schema_name, table_name
                    )
                    return await self._create_test_index(
                        db, schema_name, table_name, index_name,
                        key_columns, include_columns, online, dry_run,
                        table_facts=table_facts,
                        index_kind=index_kind,
                    )

                return await self._run_tool("create_test_index", database_name, preview)

            @self.mcp.tool(
                description=(
                    "Preview dropping a namespaced test index. Direct removal is blocked; "
                    "leased cleanup belongs to benchmark_index_candidate."
                ),
                annotations=ToolAnnotations(
                    title="Drop Test Index",
                    readOnlyHint=False,
                    destructiveHint=True,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            )
            async def drop_test_index(
                schema_name: str = Field(description="Schema name (e.g. 'dbo')."),
                table_name: str = Field(description="Table name."),
                index_name: str = Field(
                    description=(
                        "Index name. It must start with IX_Testing_ or a prefix the "
                        "database policy lists in test_index_name_prefixes. Any other "
                        "name is refused, so an application index cannot be dropped."
                    ),
                ),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Must remain true. Direct test-index removal is preview-only."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "drop_test_index",
                    database_name,
                    lambda db: self._drop_test_index(
                        db, schema_name, table_name, index_name, dry_run,
                    ),
                )

            @self.mcp.tool(
                description=(
                    "Terminate a user session (SPID). Use to resolve blocking chains "
                    "or kill runaway queries. Cannot kill system sessions."
                ),
                annotations=ToolAnnotations(
                    title="Kill Session",
                    readOnlyHint=False,
                    destructiveHint=True,
                    idempotentHint=False,
                    openWorldHint=False,
                ),
            )
            async def kill_session(
                session_id: int = Field(description="Session ID (SPID) to terminate."),
                dry_run: bool = Field(
                    default=True,
                    description=(
                        "Preview and audit without execution by default. Set False only "
                        "with AZURE_SQL_WRITE_POLICY=apply."
                    ),
                ),
                database_name: str | None = Field(
                    default=None,
                    description="Optional database name.",
                ),
            ) -> ResponseType:
                return await self._run_tool(
                    "kill_session",
                    database_name,
                    lambda db: self._kill_session(db, session_id, dry_run),
                )

        @self.mcp.tool(
            description=(
                "Capture a redacted index portfolio snapshot into the installed, "
                "append-only history contract. No index DDL is executed. Returns the "
                "run id, subject count and coverage; the stored snapshot is returned "
                "only with include_snapshot=true."
            ),
            annotations=ToolAnnotations(
                title="Capture Index Review Snapshot",
                readOnlyHint=False,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
        )
        async def capture_index_review_snapshot(
            database_name: str = Field(
                ...,
                min_length=1,
                max_length=128,
                description="Target database name.",
            ),
            idempotency_key: str | None = Field(
                default=None,
                min_length=1,
                max_length=512,
                description="Optional caller key. Omitted calls use the UTC-day key.",
            ),
            include_snapshot: bool = Field(
                default=False,
                description="When true, returns every stored subject (about 2.5 KB each).",
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "capture_index_review_snapshot",
                database_name,
                lambda db: self._capture_index_review_snapshot(
                    db, idempotency_key, include_snapshot=include_snapshot
                ),
            )

        @self.mcp.tool(
            description=(
                "Review captured index history using deterministic safety gates and "
                "recommend-only artifacts. Returns state counts, the create, consolidate "
                "and drop candidates with inert DDL and rollback, and blockers by code. "
                "For the full gate of a subject call get_index_review with subject_ids."
            ),
            annotations=ToolAnnotations(
                title="Review Index Portfolio",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def review_index_portfolio(
            database_name: str = Field(
                ...,
                min_length=1,
                max_length=128,
                description="Target database name.",
            ),
            as_of_run_id: str | None = Field(
                default=None,
                min_length=1,
                max_length=200,
                description="Optional captured run to review.",
            ),
            prior_review_id: str | None = Field(
                default=None,
                min_length=1,
                max_length=200,
                description="Optional prior review for deterministic recheck transitions.",
            ),
            include_full_review: bool = Field(
                default=False,
                description=(
                    "When true, returns every subject with its gate plus the JSON "
                    "artifact (about 14 KB per index)."
                ),
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "review_index_portfolio",
                database_name,
                lambda db: self._review_index_portfolio(
                    db, as_of_run_id, prior_review_id, include_full_review=include_full_review
                ),
            )

        @self.mcp.tool(
            description=(
                "Reconstruct one scoped index portfolio review from validated history. "
                "Returns the compact review by default. Pass subject_ids, or "
                "subject_offset to page, for the full gate, definition and protections "
                "of subjects."
            ),
            annotations=ToolAnnotations(
                title="Get Index Review",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def get_index_review(
            database_name: str = Field(
                ...,
                min_length=1,
                max_length=128,
                description="Target database name.",
            ),
            review_id: str = Field(min_length=1, max_length=200),
            subject_ids: list[str] | None = Field(
                default=None,
                max_length=100,
                description="Return the full entries of these subjects.",
            ),
            subject_offset: int | None = Field(
                default=None,
                ge=0,
                description="Return a page of full subject entries from this offset.",
            ),
            subject_limit: int = Field(
                default=10,
                ge=1,
                le=100,
                description="Page size for subject_ids or subject_offset.",
            ),
            include_full_review: bool = Field(
                default=False,
                description=(
                    "When true, returns every subject with its gate plus the JSON "
                    "artifact (about 14 KB per index)."
                ),
            ),
        ) -> ResponseType:
            return await self._run_tool(
                "get_index_review",
                database_name,
                lambda db: self._get_index_review(
                    db,
                    review_id,
                    subject_ids=subject_ids,
                    subject_offset=subject_offset,
                    subject_limit=subject_limit,
                    include_full_review=include_full_review,
                ),
            )

    def _register_incident_tools(self) -> None:
        """Register the local, DB-free incident tools; pruned off stdio."""

        @self.mcp.tool(
            description=(
                "Record that this agent session is blocked: skill text contradicts a "
                "tool, the same call fails the same way again, a call hangs, or a "
                "capability is missing. Writes one redacted local incident to the "
                "owner's fix backlog and never touches the database. Do not include "
                "SQL, literals, object, server, or database names, parameter values, "
                "or result data."
            ),
            annotations=ToolAnnotations(
                title="Report Stuck",
                readOnlyHint=False,
                destructiveHint=False,
                idempotentHint=False,
                openWorldHint=False,
            ),
        )
        async def report_stuck(
            skill: Literal[
                "sql-health-triage",
                "sql-optimizer",
                "sql-plan-enforcer",
                "sql-index-manager",
                "none",
            ] = Field(description='Maintained skill that is blocked, or "none".'),
            blocker_kind: Literal[
                "skill_tool_contradiction",
                "repeated_tool_failure",
                "tool_hang_or_timeout",
                "missing_capability",
                "unclear_next_step",
                "policy_blocks_required_step",
                "other",
            ] = Field(description="What kind of blocker this is."),
            summary: str = Field(
                min_length=10,
                max_length=500,
                description=(
                    "One sentence on what is blocked. No SQL, literals, names, "
                    "parameter values, or result data."
                ),
            ),
            skill_version: str | None = Field(
                default=None,
                pattern=r"^[0-9A-Za-z.+-]{1,32}$",
                description="The skill's metadata.version from its SKILL.md front matter.",
            ),
            last_tool: str | None = Field(
                default=None,
                pattern=r"^[a-z][a-z0-9_]{0,63}$",
                description="The last tool called before the blocker.",
            ),
            attempts: int = Field(
                default=1,
                ge=1,
                le=100,
                description="How many times the blocked step was tried.",
            ),
            related_ids: list[str] = Field(
                default_factory=list,
                max_length=5,
                description=(
                    "Case, session, candidate, decision, evidence, handoff, intent, "
                    "lesson, review, or incident ids this server returned; other "
                    "values are dropped."
                ),
            ),
            ctx: Context | None = None,
        ) -> ResponseType:
            expected_version = _LEARNING_SKILL_VERSIONS.get(skill)
            if last_tool is not None and self.mcp._tool_manager.get_tool(last_tool) is None:
                last_tool = "unregistered"  # agent text, possibly a name
            outcome = self.incidents.report_blocker(
                skill=skill,
                blocker_kind=blocker_kind,
                summary=summary,
                skill_version=skill_version,
                skill_version_expected=expected_version,
                last_tool=last_tool,
                attempts=attempts,
                related_ids=related_ids,
                context=ctx,
                tool_names=getattr(self.mcp._tool_manager, "known_tool_names", ()),
            )
            result: dict[str, Any] = {
                "recorded": bool(outcome.get("recorded")),
                "coalesced": bool(outcome.get("coalesced")),
                "incident_id": outcome.get("incident_id"),
                "fingerprint": outcome.get("fingerprint"),
                "summary": outcome.get("summary"),
                "skill_version_expected": expected_version,
                "skill_version_mismatch": bool(outcome.get("skill_version_mismatch")),
                "next_step": (
                    "Stop retrying that exact call. Tell the user what is blocked and "
                    "give the incident_id; continue only with independent work."
                ),
            }
            if not (result["recorded"] or result["coalesced"]):
                result["reason"] = outcome.get("reason")
                # A rate-limited report is a normal answer, not a degraded one.
                if result["reason"] != "rate_limited":
                    reason = (
                        f"The incident log is off: {self.incidents.status()['reason']}."
                        if not self.incidents.enabled
                        else f"The report was not recorded: {outcome.get('reason')}."
                    )
                    result.update(status_payload(ResultStatus.UNAVAILABLE, reason))
            return result

        @self.mcp.tool(
            description=(
                "Return the local incident fix backlog grouped by fingerprint: titles, "
                "counts, priorities, and fingerprints. include_details=true adds "
                "redacted examples and agent summaries. Never touches the database; "
                "the owner exports the full backlog with azure-sql-mcp-incidents."
            ),
            annotations=ToolAnnotations(
                title="Export Incident Backlog",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def export_incident_backlog(
            since_days: int = Field(
                default=14,
                ge=1,
                le=90,
                description="Days of incidents to include.",
            ),
            min_priority: Literal["P1", "P2", "P3", "P4"] = Field(
                default="P3",
                description=(
                    "Lowest priority to include. P4 adds caller errors, policy "
                    "rejections, degraded results, and abandoned work."
                ),
            ),
            max_items: int = Field(
                default=20,
                ge=1,
                le=50,
                description="Most items to return.",
            ),
            include_details: bool = Field(
                default=False,
                description="Add redacted examples and agent summaries from all sessions.",
            ),
        ) -> ResponseType:
            if not self.incidents.enabled:
                return {
                    **status_payload(
                        ResultStatus.UNAVAILABLE,
                        f"The incident log is off: {self.incidents.status()['reason']}.",
                    ),
                    "items": [],
                }
            backlog = await asyncio.to_thread(
                self.incidents.backlog,
                performance_db=self.performance_store.db_path,
                since_days=since_days,
                min_priority=min_priority,
                max_items=max_items,
                include_summaries=include_details,
            )
            return backlog if include_details else summarize_backlog(backlog)

    def _register_learning_tools(self) -> None:
        """Register the owner-only, advisory learning plane on local stdio."""

        learning = self.learning_service
        if learning is None:  # pragma: no cover - guarded by the caller
            return

        @self.mcp.tool(
            description=(
                "Record a redacted skill decision backed by scoped evidence. "
                "Learning is advisory and cannot change database activity."
            ),
            annotations=ToolAnnotations(
                title="Record Learning Decision",
                readOnlyHint=False,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def record_decision(
            skill: Literal[
                "sql-health-triage", "sql-optimizer", "sql-plan-enforcer", "sql-index-manager"
            ] = Field(description="Maintained skill that made the decision."),
            skill_version: str = Field(description="Skill contract version."),
            learning_key: str = Field(description="Stable decision-family key."),
            subject_kind: Literal["query", "plan", "incident", "database", "index"] = Field(
                description="Redacted decision subject kind.",
            ),
            subject_fingerprint: str = Field(
                min_length=1,
                max_length=512,
                description="Redacted fingerprint of the decision subject.",
            ),
            consumed_evidence_refs: list[str] = Field(
                description="Immutable evidence or terminal-link identifiers."
            ),
            based_on_review_ids: list[str] = Field(
                default_factory=list,
                description="Prior review identifiers supporting this decision.",
            ),
            tactic: str = Field(
                description=(
                    "What was done, in words (index, join, table and the like are "
                    "fine). No SQL statement: statement text carries literals."
                )
            ),
            expected_result: dict[str, Any] | str = Field(
                description="The prediction: one sentence, or a structured summary object."
            ),
            confidence: float = Field(ge=0.0, le=1.0),
            uncertainty: dict[str, Any] | str = Field(
                description="What could make it wrong: one sentence, or a structured summary object."
            ),
            evaluator_fingerprint: str = Field(
                description="Redacted evaluator/model fingerprint."
            ),
            runtime_fingerprint: str = Field(
                description="Process runtime fingerprint returned by check_runtime_status."
            ),
            runtime_compatibility_fingerprint: str = Field(
                description=(
                    "Stable compatibility fingerprint returned by check_runtime_status."
                )
            ),
            tool_schema_fingerprint: str = Field(
                description="Tool-schema fingerprint returned by check_runtime_status."
            ),
            sanitized_config_fingerprint: str = Field(
                description="Sanitized-config fingerprint returned by check_runtime_status."
            ),
            case_id: str | None = Field(default=None),
            session_id: str | None = Field(default=None),
            candidate_id: str | None = Field(default=None),
            query_fingerprint: str | None = Field(default=None),
            applied_lesson_ids: list[str] | None = Field(default=None),
            tags: list[str] | None = Field(default=None),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            self._validate_learning_skill_version(skill, skill_version)
            resolved_database = self.config.validate_database_name(database_name)
            scope = self._validated_learning_scope(
                resolved_database,
                runtime_fingerprint,
                runtime_compatibility_fingerprint,
                tool_schema_fingerprint,
                sanitized_config_fingerprint,
            )
            self._validate_learning_decision_references(
                resolved_database,
                case_id=case_id,
                session_id=session_id,
                candidate_id=candidate_id,
                query_fingerprint=query_fingerprint,
                evidence_refs=consumed_evidence_refs,
                applied_lesson_ids=tuple(applied_lesson_ids or ()),
                based_on_review_ids=tuple(based_on_review_ids),
            )
            decision = DecisionRecordV1(
                skill=skill,
                skill_version=skill_version,
                case_id=case_id,
                session_id=session_id,
                candidate_id=candidate_id,
                learning_key=learning_key,
                consumed_evidence_refs=tuple(consumed_evidence_refs),
                subject_kind=subject_kind,
                subject_fingerprint=subject_fingerprint,
                based_on_review_ids=tuple(based_on_review_ids),
                query_fingerprint=query_fingerprint,
                tactic=tactic,
                # A plain sentence is accepted and stored as {"summary": ...}.
                expected_result=(
                    {"summary": expected_result}
                    if isinstance(expected_result, str)
                    else expected_result
                ),
                confidence=confidence,
                uncertainty=(
                    {"summary": uncertainty} if isinstance(uncertainty, str) else uncertainty
                ),
                applied_lesson_ids=tuple(applied_lesson_ids or ()),
                evaluator_fingerprint=evaluator_fingerprint,
                runtime_fingerprint=runtime_fingerprint,
                runtime_compatibility_fingerprint=runtime_compatibility_fingerprint,
                tool_schema_fingerprint=tool_schema_fingerprint,
                sanitized_config_fingerprint=sanitized_config_fingerprint,
                scope=scope,
                tags=tuple(tags or ()),
            )
            request_fingerprint = fingerprint_json(
                {
                    key: value
                    for key, value in decision.to_dict().items()
                    if key
                    not in {"decision_id", "created_at_utc", "updated_at_utc", "version"}
                }
            )
            stored = learning.record_decision(
                decision,
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
            )
            return stored.to_dict()

        @self.mcp.tool(
            description=(
                "Review one decision using only MCP-created terminal-link evidence."
            ),
            annotations=ToolAnnotations(
                title="Review Learning Decision",
                readOnlyHint=False,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def review_decision(
            decision_id: str = Field(description="Recorded decision identifier."),
            terminal_evidence_refs: list[str] = Field(
                description="MCP-created terminal-link-* identifiers."
            ),
            observed_result: dict[str, Any] = Field(
                description="Structured redacted observed outcome."
            ),
            prediction_error: dict[str, Any] = Field(
                description="Structured redacted prediction error."
            ),
            counterexamples: list[dict[str, Any]] = Field(
                description="Structured redacted counterexamples or bounded risks."
            ),
            next_observation: dict[str, Any] = Field(
                description="Structured redacted next observation to collect."
            ),
            causal_strength: Literal[
                "strong", "moderate", "weak", "none", "unknown"
            ] = Field(default="unknown"),
            alignment: Literal["aligned", "contradiction", "unknown"] = Field(
                default="unknown"
            ),
            safety_signal: Literal[
                "passed", "proven_failure", "not_applicable", "unknown"
            ] = Field(default="unknown"),
            equivalence_signal: Literal[
                "passed", "proven_failure", "not_applicable", "unknown"
            ] = Field(default="unknown"),
            cleanup_signal: Literal[
                "passed", "proven_failure", "not_applicable", "unknown"
            ] = Field(default="unknown"),
            material_regression_signal: Literal[
                "passed", "proven_failure", "not_applicable", "unknown"
            ] = Field(default="unknown"),
            correction: dict[str, Any] | None = Field(default=None),
            unresolved_gaps: list[str] | None = Field(default=None),
            unknown_outcome: bool = Field(default=False),
            explicit_correction: bool = Field(default=False),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            resolved_database = self.config.validate_database_name(database_name)
            decision = learning.store.get_decision(decision_id)
            self._validate_learning_contract_scope(decision.scope, resolved_database)
            self._validate_learning_evidence_refs(
                terminal_evidence_refs,
                database_name=resolved_database,
                decision_id=decision_id,
                terminal_only=True,
            )
            completed_at = datetime.now(timezone.utc).isoformat()
            review_kwargs: dict[str, Any] = {
                "decision_id": decision_id,
                "terminal_evidence_refs": tuple(terminal_evidence_refs),
                "observed_result": observed_result,
                "prediction_error": prediction_error,
                "causal_strength": causal_strength,
                "unresolved_gaps": tuple(unresolved_gaps or ()),
                "created_at_utc": completed_at,
                "completed_at_utc": completed_at,
                "complete": True,
                "alignment": alignment,
                "safety_signal": safety_signal,
                "equivalence_signal": equivalence_signal,
                "cleanup_signal": cleanup_signal,
                "material_regression_signal": material_regression_signal,
                "unknown_outcome": unknown_outcome,
                "explicit_correction": explicit_correction,
            }
            review_kwargs.update(
                {
                    "counterexamples": tuple(counterexamples),
                    "next_observation": next_observation,
                    "correction": correction,
                }
            )
            review = OutcomeReviewV1(**review_kwargs)
            request_fingerprint = fingerprint_json(
                {
                    key: value
                    for key, value in review.to_dict().items()
                    if key
                    not in {
                        "review_id",
                        "created_at_utc",
                        "completed_at_utc",
                        "version",
                    }
                }
            )
            stored = learning.record_outcome_review(
                review,
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
            )
            return stored.to_dict()

        @self.mcp.tool(
            description="Propose an inactive lesson from reviewed decisions.",
            annotations=ToolAnnotations(
                title="Propose Learning Lesson",
                readOnlyHint=False,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def propose_lesson(
            learning_key: str = Field(description="Stable decision-family key."),
            review_ids: list[str] = Field(description="Supporting review identifiers."),
            trigger: dict[str, Any] = Field(description="Redacted trigger summary."),
            action: dict[str, Any] = Field(description="Redacted action summary."),
            preconditions: dict[str, Any] = Field(
                description="Bounded preconditions for applying the lesson."
            ),
            counterexamples: list[dict[str, Any]] = Field(
                description="Known failures, limits, or bounded risks."
            ),
            next_observation: dict[str, Any] | None = Field(
                default=None,
                description="Structured redacted next observation to collect.",
            ),
            required_evidence: list[str] = Field(
                description="Evidence labels required before using the lesson."
            ),
            applicable_skills: list[
                Literal[
                    "sql-health-triage",
                    "sql-optimizer",
                    "sql-plan-enforcer",
                    "sql-index-manager",
                ]
            ] = Field(
                description="Skills allowed to recall the lesson."
            ),
            tags: list[str] | None = Field(default=None),
            freshness_days: int | None = Field(default=None, ge=1),
            supersedes_lesson_id: str | None = Field(default=None),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            resolved_database = self.config.validate_database_name(database_name)
            for review_id in review_ids:
                review = learning.store.get_review(review_id)
                decision = learning.store.get_decision(review.decision_id)
                self._validate_learning_contract_scope(decision.scope, resolved_database)
            lesson = learning.propose_lesson(
                learning_key=learning_key,
                review_ids=review_ids,
                trigger=trigger,
                action=action,
                preconditions=preconditions,
                counterexamples=counterexamples,
                next_observation=next_observation,
                required_evidence=required_evidence,
                applicable_skills=applicable_skills,
                tags=tags or (),
                freshness_days=freshness_days,
                supersedes_lesson_id=supersedes_lesson_id,
                idempotency_key=idempotency_key,
            )
            return lesson.to_dict()

        @self.mcp.tool(
            description="Recall at most three fresh, active, scoped lessons.",
            annotations=ToolAnnotations(
                title="Recall Learning Lessons",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def recall_lessons(
            skill: Literal[
                "sql-health-triage", "sql-optimizer", "sql-plan-enforcer", "sql-index-manager"
            ] = Field(description="Maintained skill requesting advisory lessons."),
            skill_version: str = Field(description="Version of the requesting skill."),
            runtime_compatibility_fingerprint: str = Field(
                description="Stable compatibility fingerprint returned by check_runtime_status."
            ),
            tool_schema_fingerprint: str = Field(
                description="Tool-schema fingerprint returned by check_runtime_status."
            ),
            sanitized_config_fingerprint: str = Field(
                description="Sanitized-config fingerprint returned by check_runtime_status."
            ),
            query_fingerprint: str | None = Field(default=None),
            tags: list[str] | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            self._validate_learning_skill_version(skill, skill_version)
            resolved_database = self.config.validate_database_name(database_name)
            scope = self._validated_learning_scope(
                resolved_database,
                None,
                runtime_compatibility_fingerprint,
                tool_schema_fingerprint,
                sanitized_config_fingerprint,
            )
            lessons = learning.recall(
                skill=skill,
                skill_version=skill_version,
                query_fingerprint=query_fingerprint,
                scope=scope,
                runtime_compatibility_fingerprint=scope[
                    "runtime_compatibility_fingerprint"
                ],
                tool_schema_fingerprint=scope["tool_schema_fingerprint"],
                sanitized_config_fingerprint=scope["sanitized_config_fingerprint"],
                tags=tags or (),
                max_results=3,
            )
            return {
                "lessons": [lesson.to_dict() for lesson in lessons],
                "count": len(lessons),
                "max_results": 3,
            }

        @self.mcp.tool(
            description="List deterministic lesson candidates for maintainer review.",
            annotations=ToolAnnotations(
                title="List Learning Candidates",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def list_learning_candidates(
            skill: Literal[
                "sql-health-triage", "sql-optimizer", "sql-plan-enforcer", "sql-index-manager"
            ]
            | None = Field(default=None),
            learning_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            resolved_database = self.config.validate_database_name(database_name)
            candidates = learning.list_learning_candidates(
                skill=skill,
                learning_key=learning_key,
                scope=self._current_learning_scope(resolved_database),
            )
            return {"candidates": [dict(item) for item in candidates], "count": len(candidates)}

        @self.mcp.tool(
            description="Create a durable, redacted cross-skill learning handoff.",
            annotations=ToolAnnotations(
                title="Create Learning Handoff",
                readOnlyHint=False,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def create_handoff(
            source_skill: Literal[
                "sql-health-triage", "sql-optimizer", "sql-plan-enforcer", "sql-index-manager"
            ] = Field(description="Maintained skill creating the handoff."),
            target_skill: Literal[
                "sql-health-triage", "sql-optimizer", "sql-plan-enforcer", "sql-index-manager"
            ] = Field(description="Maintained skill expected to accept it."),
            objective: dict[str, Any] = Field(description="Redacted handoff objective."),
            evidence_refs: list[str] = Field(description="Immutable evidence identifiers."),
            constraints: dict[str, Any] = Field(description="Safety and scope constraints."),
            gaps: list[dict[str, Any]] = Field(description="Known unresolved gaps."),
            acceptance_criteria: list[dict[str, Any]] = Field(
                description="Evidence-backed completion criteria."
            ),
            case_id: str | None = Field(default=None),
            session_id: str | None = Field(default=None),
            idempotency_key: str | None = Field(default=None),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            resolved_database = self.config.validate_database_name(database_name)
            self._validate_learning_workflow_references(
                resolved_database,
                case_id=case_id,
                session_id=session_id,
            )
            self._validate_learning_evidence_refs(
                evidence_refs,
                database_name=resolved_database,
            )
            handoff = HandoffV1(
                source_skill=source_skill,
                target_skill=target_skill,
                case_id=case_id,
                session_id=session_id,
                scope=self._current_learning_scope(resolved_database),
                objective=objective,
                evidence_refs=tuple(evidence_refs),
                constraints=constraints,
                gaps=tuple(gaps),
                acceptance_criteria=tuple(acceptance_criteria),
            )
            return learning.create_handoff(
                handoff,
                idempotency_key=idempotency_key,
            ).to_dict()

        @self.mcp.tool(
            description="Read one scoped durable cross-skill learning handoff.",
            annotations=ToolAnnotations(
                title="Get Learning Handoff",
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def get_handoff(
            handoff_id: str = Field(description="Handoff identifier."),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            resolved_database = self.config.validate_database_name(database_name)
            handoff = learning.store.get_handoff(handoff_id)
            self._validate_learning_contract_scope(handoff.scope, resolved_database)
            return handoff.to_dict()

        @self.mcp.tool(
            description="Claim, resolve, reopen, or cancel a scoped learning handoff; successful resolutions can link a decision.",
            annotations=ToolAnnotations(
                title="Resolve Learning Handoff",
                readOnlyHint=False,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
        )
        async def resolve_handoff(
            handoff_id: str = Field(description="Handoff identifier."),
            action: Literal["claim", "resolve", "reopen", "cancel"] = Field(
                description="Lifecycle action."
            ),
            expected_version: int = Field(ge=0),
            owner: str | None = Field(default=None),
            resolution: dict[str, Any] | None = Field(default=None),
            resolution_evidence_refs: list[str] | None = Field(default=None),
            decision_id: str | None = Field(
                default=None,
                description="Optional decision to link after a successful resolution.",
            ),
            database_name: str | None = Field(default=None),
        ) -> ResponseType:
            resolved_database = self.config.validate_database_name(database_name)
            handoff = learning.store.get_handoff(handoff_id)
            self._validate_learning_contract_scope(handoff.scope, resolved_database)
            supplied_resolution_refs = tuple(resolution_evidence_refs or ())
            if supplied_resolution_refs and action != "resolve":
                raise ValueError(
                    "resolution_evidence_refs are only valid when resolving a handoff."
                )
            if action == "claim":
                if not owner:
                    raise ValueError("Claiming a handoff requires owner.")
                updated = learning.claim_handoff(
                    handoff_id, owner, expected_version=expected_version
                )
            elif action == "resolve":
                if resolution is None:
                    raise ValueError("Resolving a handoff requires resolution.")
                evidence_refs = supplied_resolution_refs
                self._validate_learning_evidence_refs(
                    evidence_refs,
                    database_name=resolved_database,
                    terminal_only=True,
                )
                updated = learning.resolve_handoff(
                    handoff_id,
                    resolution,
                    resolution_evidence_refs=evidence_refs,
                    expected_version=expected_version,
                )
            elif action == "reopen":
                updated = learning.reopen_handoff(
                    handoff_id, expected_version=expected_version
                )
            else:
                updated = learning.cancel_handoff(
                    handoff_id, expected_version=expected_version
                )
            result = updated.to_dict()
            if action == "resolve" and decision_id is not None:
                return await self._attach_learning_terminal_link(
                    "resolve_handoff",
                    resolved_database,
                    decision_id,
                    result,
                )
            return result

    @staticmethod
    def _validate_learning_skill_version(skill: str, skill_version: str) -> None:
        expected_version = _LEARNING_SKILL_VERSIONS.get(skill)
        if expected_version is None or skill_version != expected_version:
            raise ValueError(
                f"Learning skill {skill!r} requires version "
                f"{expected_version or 'unsupported'}."
            )

    def _current_learning_scope(self, database_name: str) -> dict[str, str]:
        runtime = self._runtime_status()
        return {
            "server_fingerprint": fingerprint_text(self.config.server),
            "database_fingerprint": database_fingerprint(
                database_name, self.config.server
            ),
            "runtime_compatibility_fingerprint": str(
                runtime["runtime_compatibility_fingerprint"]
            ),
            "tool_schema_fingerprint": str(runtime["tool_schema_fingerprint"]),
            "sanitized_config_fingerprint": str(
                runtime["sanitized_config_fingerprint"]
            ),
        }

    def _validated_learning_scope(
        self,
        database_name: str,
        runtime_fingerprint: str | None,
        runtime_compatibility_fingerprint: str,
        tool_schema_fingerprint: str,
        sanitized_config_fingerprint: str,
    ) -> dict[str, str]:
        current = self._current_learning_scope(database_name)
        supplied = {
            "runtime_compatibility_fingerprint": runtime_compatibility_fingerprint,
            "tool_schema_fingerprint": tool_schema_fingerprint,
            "sanitized_config_fingerprint": sanitized_config_fingerprint,
        }
        expected = {
            "runtime_compatibility_fingerprint": current[
                "runtime_compatibility_fingerprint"
            ],
            "tool_schema_fingerprint": current["tool_schema_fingerprint"],
            "sanitized_config_fingerprint": current[
                "sanitized_config_fingerprint"
            ],
        }
        if runtime_fingerprint is not None:
            supplied["runtime_fingerprint"] = runtime_fingerprint
            expected["runtime_fingerprint"] = self._runtime_status()[
                "runtime_fingerprint"
            ]
        mismatches = [
            name for name, value in supplied.items() if value != expected[name]
        ]
        if mismatches:
            raise ValueError(
                "Learning request runtime gate failed for: "
                + ", ".join(sorted(mismatches))
                + ". Call check_runtime_status and retry with its fingerprints."
            )
        return current

    def _validate_learning_contract_scope(
        self,
        scope: Mapping[str, Any],
        database_name: str,
    ) -> None:
        expected = self._current_learning_scope(database_name)
        if any(scope.get(name) != value for name, value in expected.items()):
            raise ValueError(
                "Learning contract belongs to a different server, database, or runtime scope."
            )

    def _validate_learning_workflow_references(
        self,
        database_name: str,
        *,
        case_id: str | None,
        session_id: str | None,
        candidate_id: str | None = None,
    ) -> None:
        case = (
            self.performance_store.get_performance_case(case_id)
            if case_id is not None
            else None
        )
        session = (
            self.performance_store.get_session(session_id)
            if session_id is not None and (case_id is not None or candidate_id is not None)
            else None
        )
        candidate = (
            self.performance_store.get_candidate(candidate_id)
            if candidate_id is not None
            else None
        )
        if case is not None and not database_fingerprint_matches(
            case.database_fingerprint or "",
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise ValueError("Learning workflow case belongs to another database scope.")
        if session is not None and case is not None and session.performance_case_id != case.case_id:
            raise ValueError("Learning session does not belong to the supplied case.")
        if candidate is not None and session_id is not None and candidate.session_id != session_id:
            raise ValueError("Learning candidate does not belong to the supplied session.")
        if candidate is not None and session is None:
            session = self.performance_store.get_session(candidate.session_id)
        if candidate is not None and case is not None and session is not None:
            if session.performance_case_id != case.case_id:
                raise ValueError("Learning candidate belongs to another performance case.")

    def _validate_learning_decision_references(
        self,
        database_name: str,
        *,
        case_id: str | None,
        session_id: str | None,
        candidate_id: str | None,
        query_fingerprint: str | None,
        evidence_refs: list[str],
        applied_lesson_ids: tuple[str, ...],
        based_on_review_ids: tuple[str, ...],
    ) -> None:
        self._validate_learning_workflow_references(
            database_name,
            case_id=case_id,
            session_id=session_id,
            candidate_id=candidate_id,
        )
        case = (
            self.performance_store.get_performance_case(case_id)
            if case_id is not None
            else None
        )
        if query_fingerprint is not None and case is not None and query_fingerprint != case.query_fingerprint:
            raise ValueError("Learning query fingerprint does not match the supplied case.")
        self._validate_learning_evidence_refs(
            evidence_refs,
            database_name=database_name,
        )
        if self.learning_store is None:  # pragma: no cover - local-only path
            raise ValueError("Learning store is unavailable.")
        current_scope = self._current_learning_scope(database_name)
        for review_id in based_on_review_ids:
            review = self.learning_store.get_review(review_id)
            decision = self.learning_store.get_decision(review.decision_id)
            self._validate_learning_contract_scope(decision.scope, database_name)
        for lesson_id in applied_lesson_ids:
            lesson = self.learning_store.get_lesson(lesson_id)
            if lesson.status != "active":
                raise ValueError(f"Applied lesson {lesson_id!r} is not active.")
            if not any(
                all(candidate_scope.get(key) == value for key, value in current_scope.items())
                for candidate_scope in lesson.applicable_scopes
            ):
                raise ValueError(f"Applied lesson {lesson_id!r} is outside this database scope.")

    def _validate_learning_evidence_refs(
        self,
        evidence_refs: tuple[str, ...] | list[str],
        *,
        database_name: str | None = None,
        decision_id: str | None = None,
        terminal_only: bool = False,
    ) -> None:
        for evidence_ref in evidence_refs:
            if evidence_ref.startswith("evidence-") and not terminal_only:
                evidence = self.performance_store.get_evidence(evidence_ref)
                if database_name is not None and not database_fingerprint_matches(
                    evidence.database_fingerprint or "",
                    database_name,
                    self.config.server,
                    allow_legacy=bool(self.config.legacy_state_server_binding),
                ):
                    raise ValueError("Learning evidence belongs to another database scope.")
                continue
            if evidence_ref.startswith("terminal-link-"):
                if not self._learning_terminal_evidence_exists(
                    evidence_ref,
                    decision_id=decision_id,
                ):
                    raise ValueError(
                        f"Terminal evidence reference {evidence_ref!r} was not found."
                    )
                if self.learning_store is not None:
                    link = self.learning_store.get_terminal_link(evidence_ref)
                    if decision_id is not None and link["decision_id"] != decision_id:
                        raise ValueError(
                            "Terminal evidence belongs to another decision scope."
                        )
                    if database_name is not None and any(
                        link["scope"].get(key) != value
                        for key, value in self._current_learning_scope(
                            database_name
                        ).items()
                    ):
                        raise ValueError(
                            "Terminal evidence belongs to another server, database, or runtime scope."
                        )
                continue
            raise ValueError(
                "Unknown learning evidence reference prefix; expected evidence- or terminal-link-."
            )

    def _learning_evidence_ref_is_valid(
        self,
        evidence_ref: str,
        decision: DecisionRecordV1,
    ) -> bool:
        try:
            if evidence_ref.startswith("evidence-"):
                evidence = self.performance_store.get_evidence(evidence_ref)
                return evidence.database_fingerprint == decision.scope.get(
                    "database_fingerprint"
                )
            if evidence_ref.startswith("terminal-link-"):
                if not self._learning_terminal_evidence_exists(
                    evidence_ref,
                ):
                    return False
                if self.learning_store is None:
                    return False
                link = self.learning_store.get_terminal_link(evidence_ref)
                return all(
                    link["scope"].get(key) == value
                    for key, value in decision.scope.items()
                    if key
                    in {
                        "server_fingerprint",
                        "database_fingerprint",
                        "runtime_compatibility_fingerprint",
                        "tool_schema_fingerprint",
                        "sanitized_config_fingerprint",
                    }
                )
        except (ContractNotFoundError, KeyError, ValueError):
            return False
        return False

    def _learning_terminal_evidence_exists(
        self,
        evidence_ref: str,
        *,
        decision_id: str | None = None,
    ) -> bool:
        if not evidence_ref.startswith("terminal-link-") or self.learning_service is None:
            return False
        return self.learning_service.terminal_link_exists(
            evidence_ref,
            decision_id=decision_id,
        )

    def _finish_response(self, payload: Any) -> Any:
        """Drop next_tools this server cannot call, then apply the data-detail level."""

        manager = self.mcp._tool_manager
        return apply_data_detail(
            annotate_next_tools(payload, lambda name: manager.get_tool(name) is not None),
            self.config.data_detail,
        )

    def _runtime_status(self, *, include_tool_names: bool = False) -> dict[str, Any]:
        tools = sorted(
            self.mcp._tool_manager.list_tools(),
            key=lambda tool: tool.name,
        )
        tool_names = [tool.name for tool in tools]
        counts_by_group: dict[str, int] = {}
        for name in tool_names:
            group = TOOL_GROUPS.get(name)
            label = group.value if group is not None else "ungrouped"
            counts_by_group[label] = counts_by_group.get(label, 0) + 1
        schema_material = [
            {
                "name": tool.name,
                "input_schema": tool.parameters,
                "output_schema": tool.output_schema,
            }
            for tool in tools
        ]
        schema_fingerprint = hashlib.sha256(
            json.dumps(
                schema_material,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        contracts = {
            "strict_arguments": True,
            "mcp_errors": True,
            "sanitized_validation_errors": True,
            "performance_only_selection": True,
        }
        config_fingerprint = self.config.sanitized_config_fingerprint()
        compatibility_fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "package_version": self._package_version,
                    "tool_schema_fingerprint": schema_fingerprint,
                    "sanitized_config_fingerprint": config_fingerprint,
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        runtime_fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "startup_timestamp": self._startup_timestamp,
                    "runtime_compatibility_fingerprint": compatibility_fingerprint,
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        default_policy = self.database_policy.policy_for(
            self.config.default_database
        )
        return {
            "startup_timestamp": self._startup_timestamp,
            "package_version": self._package_version,
            "profile": (
                self.config.profile.value if self.config.profile is not None else None
            ),
            "transport": self.config.transport.mode.value,
            "tool_groups": sorted(group.value for group in self.config.tool_groups),
            "tool_count": len(tool_names),
            "tool_counts_by_group": dict(sorted(counts_by_group.items())),
            **({"tool_names": tool_names} if include_tool_names else {}),
            "data_detail": self.config.data_detail,
            "query_cancellation": self.executor.cancellation_report(),
            "contracts": contracts,
            "strict_argument_models": contracts["strict_arguments"],
            "mcp_tool_errors": contracts["mcp_errors"],
            "timeouts": {
                "query_seconds": self.config.query_timeout_seconds,
                "default_tool_seconds": self.config.tool_timeout_seconds,
                "evidence_collection_stage_seconds": (
                    self.performance_workflows.collector_timeout_seconds
                ),
                "evidence_profile_stage_seconds": self.config.query_timeout_seconds,
                "evidence_workflow_seconds": self._evidence_workflow_timeout(),
                "session_workflow_floor_seconds": max(
                    self.config.tool_timeout_seconds,
                    21 * 60,
                ),
                "session_workflow_seconds": self._session_workflow_timeout(
                    self.config.default_database
                ),
                "session_workflow_max_benchmark_executions": (
                    default_policy.max_benchmark_executions
                ),
                "session_workflow_cleanup_headroom_seconds": 5 * 60,
                "client_timeout_managed_by_server": False,
            },
            "incident_log": self.incidents.status(),
            "runtime_fingerprint": runtime_fingerprint,
            "runtime_compatibility_fingerprint": compatibility_fingerprint,
            "tool_schema_fingerprint": schema_fingerprint,
            "sanitized_config_fingerprint": config_fingerprint,
        }

    async def _bind_performance_parameters(
        self,
        database_name: str,
        sql: str,
        parameter_case: dict[str, Any] | Any,
    ) -> ParameterExecutionContract:
        case = (
            parameter_case
            if isinstance(parameter_case, dict)
            else dict(parameter_case)
        )
        values = dict(case.get("values") or {})
        types = dict(case.get("types") or {})
        detected = {name.casefold() for name in detect_parameters(sql)}
        value_names = {str(name).lstrip("@").casefold() for name in values}
        type_names = {str(name).lstrip("@").casefold() for name in types}
        if value_names != detected or type_names != detected:
            raise ValueError(
                "Every query parameter requires one explicit value and SQL type "
                "in each benchmark case."
            )
        bucket = await self.param_binding.build_parameter_bucket(
            database_name,
            sql,
            parameter_values=values,
            parameter_types=types,
            bucket_id=str(case.get("name") or "default"),
            label=str(case.get("name") or "default"),
            provenance="explicit_performance_case",
        )
        return self.param_binding.build_execution_contract(
            sql,
            bucket,
            provenance="typed_sp_executesql",
        )

    async def _check_database_capabilities(
        self,
        database_name: str,
    ) -> dict[str, Any]:
        checks, platform = await asyncio.gather(
            self.capabilities.check(database_name),
            self._optional_payload(
                lambda: self.platform_capabilities.get_summary(database_name)
            ),
        )
        policy = self.database_policy.policy_for(database_name)
        probes = checks.get("checks")
        query_store_probe = probes.get("query_store") if isinstance(probes, dict) else None
        query_store_detail = (
            query_store_probe.get("detail") if isinstance(query_store_probe, dict) else None
        )
        if isinstance(query_store_detail, dict) and query_store_detail.get("enabled"):
            replicas = await self._optional_payload(
                lambda: self.query_store.get_replica_dimension(database_name)
            )
        else:
            # Query Store holds the replica dimension; with it off there is none to read.
            replicas = {"ok": False, "error": "Query Store is not readable or is off."}
        if isinstance(probes, dict):
            # The probe proves the SHOWPLAN permission; its operators describe
            # a catalog probe, not the caller's query.
            compact_probes = dict(probes)
            for name in ("estimated_plan", "actual_plan"):
                probe = probes.get(name)
                if isinstance(probe, dict) and isinstance(probe.get("detail"), dict):
                    compact_probes[name] = {
                        **probe,
                        "detail": {
                            "probe": "plan_returned",
                            "statement_count": probe["detail"].get("statement_count"),
                        },
                    }
            checks = {**checks, "checks": compact_probes}
        summary = platform.get("data")
        unsupported = (
            unsupported_platform_reason(summary)
            if isinstance(summary, dict) and "platform_supported" in summary
            else None
        )
        return {
            **checks,
            **(
                status_payload(ResultStatus.NOT_SUPPORTED, unsupported)
                if unsupported
                else {}
            ),
            "azure_sql_database": platform,
            "query_store_replicas": replicas,
            "mcp_contract": {
                # The package version: a literal here fell behind the package.
                "contract_version": self._package_version,
                "performance_tuning": 1,
                "branching_batch": 1,
                "index_set_experiment": 1,
                "slow_baseline_cases": 1,
                "durable_view_change": 1,
                "prepared_plan_action": 1,
                "index_portfolio_review": 1,
                "index_learning_mode": "recall_only",
                "index_history_schema_version": "index-history-v1",
                "index_history_schema_fingerprint": CONTRACT_SCHEMA_FINGERPRINT,
                "index_review_min_observation_days": 35,
                "index_review_snapshot_reuse_hours": 48,
                "workload_index_advisor": 1,
                "result_status": 1,
                "plan_digest": 1,
            },
            # Whether benchmark_indexed_view_candidate may run here, and the
            # prefixes a test view name may start with.
            "indexed_view_experiment": self._local_view_experiment_capability(policy),
            "local_tuning_policy": {
                "configured": policy.configured,
                "environment": policy.environment,
                "allow_read": policy.allow_read,
                "allow_benchmark": policy.allow_benchmark,
                "allow_test_indexes": policy.allow_test_indexes,
                "test_index_name_prefixes": list(
                    self._allowed_test_index_prefixes(policy)
                ),
                "test_index_maxdop": policy.test_index_maxdop,
                "allow_offline_test_index": policy.allow_offline_test_index,
                "allow_view_apply": policy.allow_view_apply,
                "allow_plan_apply": policy.allow_plan_apply,
                "allow_index_history_write": policy.allow_index_history_write,
                "business_cycle_extension_days": policy.business_cycle_extension_days,
                "max_benchmark_executions": policy.max_benchmark_executions,
                "max_tuning_candidates": policy.max_tuning_candidates,
                "max_tuning_session_executions": (
                    policy.max_tuning_session_executions
                ),
                "max_tuning_session_minutes": policy.max_tuning_session_minutes,
            },
        }

    async def _capture_index_review_snapshot(
        self,
        database_name: str,
        idempotency_key: str | None,
        *,
        include_snapshot: bool = False,
    ) -> dict[str, Any]:
        try:
            result = await self.index_review.capture_snapshot(
                database_name,
                idempotency_key=idempotency_key,
                include_snapshot=include_snapshot,
            )
        except IndexReviewSetupError as exc:
            return index_history_setup_precondition(exc, for_write=True)
        return result.as_dict() if include_snapshot else result.as_summary()

    async def _review_index_portfolio(
        self,
        database_name: str,
        as_of_run_id: str | None,
        prior_review_id: str | None,
        *,
        include_full_review: bool = False,
    ) -> dict[str, Any]:
        try:
            review = await self.index_review.review_portfolio(
                database_name,
                as_of_run_id=as_of_run_id,
                prior_review_id=prior_review_id,
            )
        except IndexReviewSetupError as exc:
            return index_history_setup_precondition(exc)
        return self._index_review_response(review, include_full_review=include_full_review)

    async def _get_index_review(
        self,
        database_name: str,
        review_id: str,
        *,
        subject_ids: list[str] | None = None,
        subject_offset: int | None = None,
        subject_limit: int = 10,
        include_full_review: bool = False,
    ) -> dict[str, Any]:
        try:
            review = await self.index_review.get_review(database_name, review_id)
        except IndexReviewSetupError as exc:
            return index_history_setup_precondition(exc)
        if subject_ids is not None or subject_offset is not None:
            return index_review_subjects(
                review,
                subject_ids=subject_ids,
                offset=subject_offset or 0,
                limit=subject_limit,
            )
        return self._index_review_response(review, include_full_review=include_full_review)

    @staticmethod
    def _index_review_response(review: Any, *, include_full_review: bool) -> dict[str, Any]:
        """Return the compact review, or the whole review with every artifact."""

        if include_full_review:
            payload = review.as_dict()
            payload["artifacts"] = render_index_review_artifacts(review)
            return payload
        payload = compact_index_review(review)
        payload["artifacts"] = render_index_review_artifacts(
            review, include_json=False, actionable_only=True
        )
        return payload

    def _plan_intent_fingerprint(self, intent_id: str) -> str:
        """What a confirmation token is bound to: the intent's evidence hash."""

        intent = self.performance_store.get_plan_action_intent(intent_id)
        evidence_hash = intent.metadata.get("evidence_hash")
        if not isinstance(evidence_hash, str) or not evidence_hash:
            raise PermissionError(
                "The prepared intent has no evidence hash; prepare it again."
            )
        return evidence_hash

    async def _prepare_plan_action(self, database_name: str, **request: Any) -> dict[str, Any]:
        result = await self.plan_actions.prepare(database_name, **request)
        policy = self.database_policy.policy_for(database_name)
        intent = result.get("intent")
        if (
            result.get("prepared")
            and isinstance(intent, Mapping)
            and self.confirmation_tokens.required(policy)
        ):
            metadata = intent.get("metadata")
            fingerprint = metadata.get("evidence_hash") if isinstance(metadata, Mapping) else None
            if isinstance(fingerprint, str) and fingerprint:
                result = {
                    **result,
                    "confirmation": {
                        **self.confirmation_tokens.issue(
                            intent_id=str(intent["intent_id"]),
                            fingerprint=fingerprint,
                            action="apply",
                        ),
                        "instruction": (
                            "Show this token to the person who approves the change. "
                            "apply_prepared_plan_action needs it as confirmation_token. "
                            "Do not pass it back without that person's approval."
                        ),
                    },
                }
        return result

    async def _apply_prepared_plan_action(
        self,
        database_name: str,
        intent_id: str,
        authorization_reference: str,
        confirmation_token: str | None,
    ) -> dict[str, Any]:
        policy = self.database_policy.policy_for(database_name)
        if self.confirmation_tokens.required(policy):
            # Before any gate that changes state: a refused token changes nothing.
            self.confirmation_tokens.verify(
                intent_id=intent_id,
                fingerprint=self._plan_intent_fingerprint(intent_id),
                action="apply",
                token=confirmation_token,
            )
        return await self.plan_actions.apply(
            database_name,
            intent_id,
            authorization_reference=authorization_reference,
        )

    async def _rollback_plan_action(
        self,
        database_name: str,
        intent_id: str,
        authorization_reference: str,
        reason: str,
        confirmation_token: str | None,
    ) -> dict[str, Any]:
        policy = self.database_policy.policy_for(database_name)
        if self.confirmation_tokens.required(policy):
            fingerprint = self._plan_intent_fingerprint(intent_id)
            if confirmation_token is None:
                return {
                    "confirmed": False,
                    "rolled_back": False,
                    "confirmation": {
                        **self.confirmation_tokens.issue(
                            intent_id=intent_id,
                            fingerprint=fingerprint,
                            action="rollback",
                        ),
                        "instruction": (
                            "Nothing was changed. Show this token to the person who "
                            "approves the rollback, then call rollback_plan_action "
                            "again with it as confirmation_token."
                        ),
                    },
                }
            self.confirmation_tokens.verify(
                intent_id=intent_id,
                fingerprint=fingerprint,
                action="rollback",
                token=confirmation_token,
            )
        return await self.plan_actions.rollback(
            database_name,
            intent_id,
            authorization_reference=authorization_reference,
            reason=reason,
        )

    def _history_resolver(self, database_name: str) -> HistoryResolver:
        """The recorders that can answer history questions for one database.

        Extended Events needs no configuration. The others read the binding in
        the policy's history_sources; without one they answer ``precondition``
        with the policy text, never ``empty``.
        """

        bindings = self.database_policy.policy_for(database_name).history_sources

        def tokens(scope: str) -> EntraTokenProvider:
            if scope not in self._history_tokens:
                self._history_tokens[scope] = EntraTokenProvider(
                    credential_for(self.config), scope
                )
            return self._history_tokens[scope]

        transport = UrllibTransport()
        watcher = bindings.get("database_watcher")
        settings = self._loki_settings
        return HistoryResolver(
            [
                ExtendedEventsHistorySource(database_name, self.executor),
                DatabaseWatcherHistorySource(
                    database_name,
                    watcher,
                    transport=transport,
                    tokens=tokens(watcher.token_scope if watcher is not None else KUSTO_SCOPE),
                ),
                LogAnalyticsHistorySource(
                    database_name,
                    bindings.get("log_analytics"),
                    transport=transport,
                    tokens=tokens(LOG_ANALYTICS_SCOPE),
                ),
                LokiHistorySource(
                    database_name,
                    settings,
                    bindings.get("loki"),
                    transport=UrllibTransport(ca_bundle=settings.ca_bundle),
                    tokens=tokens(settings.token_scope),
                ),
                AzureMonitorMetricsSource(
                    database_name,
                    bindings.get("azure_monitor_metrics"),
                    transport=transport,
                    tokens=tokens(ARM_SCOPE),
                ),
            ],
            database_name=database_name,
        )

    async def _get_recorded_deadlocks(
        self,
        database_name: str,
        source: str,
        as_of_utc: str | None,
        window_minutes: int,
        max_events: int,
    ) -> dict[str, Any]:
        resolver = self._history_resolver(database_name)
        if source != "auto":
            resolver = HistoryResolver(
                [item for item in resolver._sources if item.name == source],
                database_name=database_name,
            )
        start, end = window_bounds(window_minutes, as_of_utc)
        result = await resolver.read(
            HistoryCategory.DEADLOCK, TimeWindow(start, end), limit=max_events
        )
        return result.to_payload(
            "mask_literals" if self.config.data_detail == "full" else self.config.data_detail
        )

    async def _get_diagnostic_history(
        self,
        database_name: str,
        category: str,
        as_of_utc: str | None,
        window_minutes: int,
        max_events: int,
    ) -> dict[str, Any]:
        resolver = self._history_resolver(database_name)
        if category == "coverage":
            sources = [description.to_dict() for description in await resolver.describe()]
            return {
                "database_name": database_name,
                "sources": sources,
                **status_payload(
                    ResultStatus.OK,
                    "Recorders and what each one covers. configured=false means no "
                    "binding in the policy's history_sources (or, for Extended Events, "
                    "no capture session).",
                ),
            }
        start, end = window_bounds(window_minutes, as_of_utc)
        result = await resolver.read(
            HistoryCategory(category),
            TimeWindow(start, end),
            limit=max_events,
        )
        # At least mask_literals: history holds statements of other sessions.
        return result.to_payload(
            "mask_literals" if self.config.data_detail == "full" else self.config.data_detail
        )

    async def _get_object_details(
        self,
        database_name: str,
        schema_name: str,
        object_name: str,
        object_type: str,
    ) -> dict[str, Any]:
        """Object details with what index advice needs: includes, filters, lengths."""

        details = await self.introspection.get_object_details(
            database_name, schema_name, object_name, object_type
        )
        columns = details.get("columns")
        if isinstance(columns, list):
            details = {
                **details,
                "columns": [
                    {**column, **_declared_column_type(column)}
                    if isinstance(column, Mapping)
                    else column
                    for column in columns
                ],
            }
        indexes = details.get("indexes")
        if not isinstance(indexes, list) or not indexes:
            return details
        try:
            catalog = await collect_existing_indexes(
                self.executor,
                database_name,
                schema_name=schema_name,
                table_name=object_name,
            )
        except Exception as exc:
            note_exception(exc, "server.object_index_detail")
            return {
                **details,
                "index_detail_note": (
                    "Included columns, filters and key directions could not be read; "
                    "the index list shows key columns only."
                ),
            }
        by_name = {index.name.casefold(): index for index in catalog if index.name}
        enriched = []
        for entry in indexes:
            found = (
                by_name.get(str(entry.get("index_name") or "").casefold())
                if isinstance(entry, Mapping)
                else None
            )
            if found is None:
                enriched.append(entry)
                continue
            enriched.append(
                {
                    **entry,
                    "is_unique": found.is_unique,
                    "key_columns_ordered": [column.as_dict() for column in found.key_columns],
                    "include_columns": list(found.include_columns),
                    "filter_definition": found.filter_definition,
                    "is_unique_constraint": found.is_unique_constraint,
                    "is_disabled": found.is_disabled,
                }
            )
        return {**details, "indexes": enriched}

    async def _start_performance_case(
        self,
        database_name: str,
        sql: str,
        parameter_cases: list[dict[str, Any]] | None,
        objective: str,
        idempotency_key: str | None,
        query_store_query_id: int | None = None,
    ) -> dict[str, Any]:
        performance_case = await self._create_performance_case(
            database_name,
            sql,
            parameter_cases=parameter_cases,
            objective=objective,
            idempotency_key=idempotency_key,
            query_store_query_id=query_store_query_id,
        )
        return performance_case.to_dict()

    async def _create_performance_case(
        self,
        database_name: str,
        sql: str,
        *,
        parameter_cases: list[dict[str, Any]] | None,
        objective: str,
        metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        query_store_query_id: int | None = None,
    ) -> PerformanceCaseV1:
        normalized_sql = self.validator.validate_read_only_candidate(sql).execution_sql
        equivalence_preflight = await self._check_equivalence_preflight(
            database_name,
            normalized_sql,
        )
        case_metadata = dict(metadata or {})
        case_metadata.update(
            {
                "objective": objective,
                "raw_sql_persisted": False,
                "equivalence_preflight": equivalence_preflight,
            }
        )
        return self.performance_workflows.start_case(
            database_name,
            normalized_sql,
            parameter_cases=parameter_cases,
            metadata=case_metadata,
            idempotency_key=idempotency_key,
            query_store_query_id=query_store_query_id,
        )

    async def _check_equivalence_preflight(
        self,
        database_name: str,
        sql: str,
    ) -> dict[str, Any]:
        normalized_sql = self.validator.validate_read_only_candidate(sql).execution_sql
        result = await self.equivalence_preflight.analyze(
            database_name,
            normalized_sql,
        )
        return result.as_dict()

    async def _collect_performance_evidence(
        self,
        database_name: str,
        case_id: str,
        sql: str,
        window_minutes: int,
        execute_query: bool,
        idempotency_key: str | None,
        parameter_case: dict[str, Any] | None = None,
        query_store_query_id: int | None = None,
    ) -> dict[str, Any]:
        execution_contract: ParameterExecutionContract | None = None
        if execute_query and parameter_case is not None:
            execution_contract = await self._bind_performance_parameters(
                database_name,
                sql,
                parameter_case,
            )

        async def collect_regressions() -> Any:
            # The DMV holds every query's recommendations, Expired ones too;
            # the case gets only those that bear on it.
            case_query_id = (
                query_store_query_id
                if query_store_query_id is not None
                else self.performance_store.get_performance_case(case_id).query_store_query_id
            )
            return scope_case_regressions(
                await self.query_regression.detect_regressed_queries(
                    database_name,
                    window_minutes=window_minutes,
                ),
                case_query_id,
            )

        collectors: dict[str, Callable[..., Awaitable[Any]]] = {
            "resource_limits": lambda: self.resource_governance.get_resource_limits(
                database_name
            ),
            "resource_history": lambda: self.resource_governance.get_resource_stats_history(
                database_name,
                window_minutes,
                master_available=self._master_allowlisted(),
            ),
            "query_store_status": lambda: self.query_store.get_status(database_name),
            "query_store_history": lambda effective_query_id=None: self._collect_query_store_evidence(
                database_name,
                sql,
                window_minutes,
                query_store_query_id=effective_query_id,
                parameter_case=parameter_case,
            ),
            "waits": lambda: self.wait_stats.get_wait_stats(database_name, top_n=20),
            "blocking": lambda: self.lock_diagnostics.get_lock_details(
                database_name,
                limit=200,
            ),
            "open_transactions": lambda: self.lock_diagnostics.get_open_transactions(
                database_name,
                limit=100,
            ),
            "statistics": lambda: self.plan_cache.check_statistics_health(database_name),
            "parameter_sensitivity": lambda: self.query_regression.detect_parameter_sniffing(
                database_name,
                window_minutes=window_minutes,
                top_n=20,
            ),
            "regressions": collect_regressions,
        }
        return await self.performance_workflows.collect_case_evidence(
            case_id,
            database_name,
            sql,
            collectors,
            window_minutes=window_minutes,
            execute_query=execute_query,
            execution_contract=execution_contract,
            idempotency_key=idempotency_key,
            query_store_query_id=query_store_query_id,
        )

    async def _collect_query_store_evidence(
        self,
        database_name: str,
        sql: str,
        window_minutes: int,
        *,
        query_store_query_id: int | None = None,
        parameter_case: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if query_store_query_id is not None:
            if (
                isinstance(query_store_query_id, bool)
                or not isinstance(query_store_query_id, int)
                or query_store_query_id <= 0
            ):
                raise ValueError("query_store_query_id must be a positive integer.")
            query_id = query_store_query_id
            identity: dict[str, Any] = {
                "status": "resolved",
                "query_id": query_id,
                "identity_kind": "query_id",
                "identity_provenance": "caller_supplied",
                "identity_verified_against_sql": False,
                "fuzzy_match_used": False,
            }
        else:
            identity = await self.query_store.resolve_query_identity(
                database_name,
                sql,
                query_hash=await self._plan_query_hash(database_name, sql, parameter_case),
            )
            by_hash = str(identity.get("matched_by") or "").startswith("query_hash")
            if identity.get("status") != "resolved":
                return {
                    "available": False,
                    "complete": False,
                    "status": "inconclusive",
                    "reason": "exact Query Store identity was not uniquely resolved",
                    "identity": {
                        **identity,
                        "identity_provenance": "exact_sql_text",
                        "identity_verified_against_sql": False,
                    },
                    "fuzzy_match_used": False,
                }
            query_id = int(identity["query_id"])
            identity = {
                **identity,
                "identity_kind": "query_id",
                # The hash is of this SQL's own plan: it survives a change of
                # whitespace, casing or parameter names, which the text does not.
                "identity_provenance": "plan_query_hash" if by_hash else "exact_sql_text",
                "identity_verified_against_sql": True,
                "fuzzy_match_used": False,
            }
        history, parameter_buckets = await asyncio.gather(
            self.query_store.get_query_history_by_id(
                database_name,
                query_id,
                window_minutes=window_minutes,
                limit=20,
            ),
            self.query_store.get_parameter_runtime_buckets(
                database_name,
                query_id=query_id,
                window_minutes=window_minutes,
                limit=50,
            ),
        )

        def coverage(
            result: Any,
            rows_key: str,
            limit: int,
        ) -> dict[str, Any]:
            rows = result.get(rows_key) if isinstance(result, Mapping) else None
            row_count = len(rows) if isinstance(rows, list) else 0
            available = row_count > 0 and (
                not isinstance(result, Mapping) or result.get("available") is not False
            )
            complete = available and row_count < limit
            if isinstance(result, Mapping):
                complete = complete and result.get("complete") is not False
                complete = complete and result.get("truncated") is not True
            return {
                "available": available,
                "complete": complete,
                "row_count": row_count,
                "limit": limit,
            }

        history_coverage = coverage(history, "matches", 20)
        bucket_coverage = coverage(parameter_buckets, "buckets", 50)
        parameter_buckets_required = bool(detect_parameters(sql))
        bucket_coverage["required"] = parameter_buckets_required
        evidence_available = bool(
            history_coverage["available"]
            or (
                parameter_buckets_required
                and bucket_coverage["available"]
            )
        )
        evidence_complete = bool(
            history_coverage["complete"]
            and (
                not parameter_buckets_required
                or bucket_coverage["complete"]
            )
        )
        if evidence_complete:
            status = "resolved"
            reason = None
        elif evidence_available:
            status = "partial"
            reason = (
                "Query Store history or required parameter-bucket evidence is incomplete"
            )
        else:
            status = "inconclusive"
            reason = (
                "Query Store history is empty"
                if not parameter_buckets_required
                else "Query Store history and parameter-bucket evidence are empty"
            )
        return {
            "available": evidence_available,
            "complete": evidence_complete,
            "status": status,
            "reason": reason,
            "identity": {
                "query_id": query_id,
                "query_hash": identity.get("query_hash"),
                "identity_kind": "query_id",
                "identity_provenance": identity["identity_provenance"],
                "identity_verified_against_sql": identity[
                    "identity_verified_against_sql"
                ],
            },
            "history": history,
            "parameter_buckets": parameter_buckets,
            "coverage": {
                "history": history_coverage,
                "parameter_buckets": bucket_coverage,
            },
            "fuzzy_match_used": False,
        }

    async def _plan_query_hash(
        self,
        database_name: str,
        sql: str,
        parameter_case: Mapping[str, Any] | None,
    ) -> str | None:
        """QueryHash of the statement's estimated plan, or None when it has none to give.

        Nothing is executed. A batch has one hash per branch, and parameterized
        SQL without a parameter case cannot be compiled: both return None, and
        the identity is then resolved by text.
        """

        try:
            if self.validator.validate_read_only_candidate(sql).branches:
                return None
            if detect_parameters(sql):
                if parameter_case is None:
                    return None
                artifact = await self.plans.explain_parameterized_query(
                    database_name,
                    await self._bind_performance_parameters(
                        database_name, sql, dict(parameter_case)
                    ),
                    analyze=False,
                )
            else:
                artifact = await self.plans.explain_query(database_name, sql, False)
        except Exception as exc:
            note_exception(exc, "server.plan_query_hash")
            return None
        hashes = {
            str(statement["query_hash"])
            for statement in artifact.summary.get("statements") or ()
            if isinstance(statement, Mapping) and statement.get("query_hash")
        }
        if len(hashes) != 1:
            return None
        query_hash = hashes.pop()
        return query_hash if re.fullmatch(r"0x[0-9a-fA-F]{16}", query_hash) else None

    async def _get_performance_case(
        self,
        database_name: str,
        case_id: str,
    ) -> dict[str, Any]:
        payload = self.performance_workflows.get_case(case_id)
        case = payload["case"]
        if not database_fingerprint_matches(
            str(case.get("database_fingerprint") or ""),
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise PermissionError("Performance case belongs to another database.")
        return payload

    async def _start_tuning_session(
        self,
        database_name: str,
        case_id: str,
        max_candidates: int | None,
        execution_limit: int | None,
        time_limit_minutes: int | None,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        case = self.performance_store.get_performance_case(case_id)
        if not database_fingerprint_matches(
            case.database_fingerprint or "",
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise PermissionError("Performance case belongs to another database.")
        # A session must not start on a database that holds a leftover of an
        # earlier lease: its drop is retried here, and the start is refused
        # while the object is still in place.
        cleanup = await self._reconcile_leases(
            database_fingerprint=case.database_fingerprint,
            attempts=_PRE_CALL_DROP_ATTEMPTS,
        )
        if cleanup["cleanup_required"]:
            return self._leftover_refusal(cleanup, case_id=case_id)
        started = self.performance_workflows.start_session(
            case_id,
            database_name,
            max_candidates=max_candidates,
            execution_limit=execution_limit,
            time_limit_minutes=time_limit_minutes,
            idempotency_key=idempotency_key,
        )
        if cleanup["dropped"]:
            started = {**started, "cleanup_before_run": {"dropped": cleanup["dropped"]}}
        return started

    async def _get_tuning_session(
        self,
        database_name: str,
        session_id: str,
        *,
        view: str = "summary",
    ) -> dict[str, Any]:
        payload = self.performance_workflows.get_session(session_id, view=view)
        session = payload["session"]
        if not database_fingerprint_matches(
            str(
                session.get("metadata", {}).get("database_fingerprint")
                or ""
            ),
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise PermissionError("Tuning session belongs to another database.")
        held = self._held_test_objects(
            str(session.get("metadata", {}).get("database_fingerprint") or ""), session_id
        )
        if held:
            # Test objects a benchmark call kept for this session, with their age.
            payload = {**payload, "held_test_objects": held}
        return payload

    async def _add_tuning_candidate(
        self,
        database_name: str,
        session_id: str,
        candidate_sql: str,
        strategy: str,
        artifact_ref: str | None,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        session = self.tuning_sessions.get_session(session_id)
        case = self.performance_store.get_performance_case(session.performance_case_id)
        if not database_fingerprint_matches(
            case.database_fingerprint or "",
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise PermissionError("Tuning session belongs to another database.")
        return self.performance_workflows.add_candidate(
            session_id,
            candidate_sql,
            strategy=strategy,
            artifact_ref=artifact_ref,
            idempotency_key=idempotency_key,
        )

    async def _finalize_tuning_session(
        self,
        database_name: str,
        session_id: str,
        selected_candidate_id: str | None,
        stopping_reason: str,
        idempotency_key: str | None,
        selection_scope: SelectionScope = "proven",
        logical_preconditions: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        session = self.tuning_sessions.get_session(session_id)
        case = self.performance_store.get_performance_case(session.performance_case_id)
        if not database_fingerprint_matches(
            case.database_fingerprint or "",
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise PermissionError("Tuning session belongs to another database.")
        already_finalized = session.status not in {"created", "screening", "finalist_validation"}
        if already_finalized:
            # The session is closed and stays as it was finalized. The call is
            # the explicit retry of the drop of what it still has in place.
            result: dict[str, Any] = {
                **self.performance_workflows.get_session(session_id),
                "already_finalized": True,
                "already_finalized_note": (
                    "The session was finalized before; its selection and stopping "
                    "reason are unchanged. This call only retried the drop of test "
                    "objects the session still had in place."
                ),
            }
        else:
            # Read the catalog before the session completes: a refuted
            # precondition raises here and leaves the session open.
            verification = (
                await self.performance_workflows.verify_logical_preconditions(
                    database_name,
                    session_id,
                    selected_candidate_id,
                    logical_preconditions,
                )
                if selected_candidate_id is not None
                else None
            )
            result = self.performance_workflows.finalize_session(
                session_id,
                selected_candidate_id=selected_candidate_id,
                selection_scope=selection_scope,
                stopping_reason=stopping_reason,
                idempotency_key=idempotency_key,
                precondition_verification=verification,
            )
        # A finished session keeps no test object in the database.
        cleanup = await self._reconcile_leases(
            session_id=session_id,
            closed_by="released_by_finalize",
            attempts=_FINALIZE_DROP_ATTEMPTS,
        )
        objects = [*cleanup["dropped"], *cleanup["remaining"]]
        if objects or already_finalized:
            result = {**result, "held_test_objects_dropped": objects}
        if cleanup["remaining"]:
            names = ", ".join(
                str(item.get("index_name") or item.get("lease_id")) for item in cleanup["remaining"]
            )
            remediation = "\n".join(
                str(item["remediation"]) for item in cleanup["remaining"] if item.get("remediation")
            )
            reason = (
                f"The session is finalized, but a test object it held is still in place: {names}. "
                "This is not a clean finish: the object changes the plans of other sessions "
                "on its table until it is dropped."
            )
            result = {
                **result,
                "cleanup_required": True,
                **status_payload(ResultStatus.PRECONDITION, reason, remediation=remediation or None),
                "held_test_objects_note": (
                    f"Not dropped after {_FINALIZE_DROP_ATTEMPTS} attempts; failure says why "
                    "per object. Call finalize_tuning_session again with this session_id to "
                    "retry the drop. Every benchmark call and start_tuning_session in this "
                    "database also retries it first and is refused while the object is "
                    "still in place. If it stays, give the user remediation."
                ),
            }
        return result

    async def _compare_plan_summaries(
        self,
        _database_name: str,
        baseline_summary: dict[str, Any],
        candidate_summary: dict[str, Any],
    ) -> dict[str, Any]:
        return compare_plan_summaries_payload(baseline_summary, candidate_summary)

    async def _prepare_view_change(
        self,
        database_name: str,
        schema_name: str,
        view_name: str,
        definition: str,
        operation: str,
        schema_bound: bool | None,
        indexed_view: bool,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        request = ViewChangeRequest(
            database_name=database_name,
            schema_name=schema_name,
            view_name=view_name,
            definition=definition,
            operation=operation,
            idempotency_key=idempotency_key,
            indexed_view=indexed_view,
            schema_bound=schema_bound,
        )
        schema_name = request.schema_name
        view_name = request.view_name
        intent: dict[str, Any] | None = None
        durable = (
            self.config.profile == McpProfile.SANDBOX
            and self.config.persist_view_sql_state
            and self.config.performance_state_dir != ":memory:"
        )
        if durable:
            if not idempotency_key or not idempotency_key.strip():
                raise ValueError(
                    "Durable sandbox view preparation requires an idempotency key."
                )
            normalized_idempotency_key = _view_idempotency_identity(idempotency_key)
            assert normalized_idempotency_key is not None
            request = replace(
                request,
                idempotency_key=normalized_idempotency_key,
            )
            database_fp = database_fingerprint(database_name, self.config.server)
            intent = self.performance_store.get_idempotent_view_change_intent(
                database_fingerprint=database_fp,
                idempotency_key=normalized_idempotency_key,
            )
            if intent is not None:
                prepared = prepared_view_change_from_state(intent["payload"])
                # Stored requests hold schema_bound resolved against the prior
                # view; resolve the retry the same way before comparing.
                incoming = resolve_view_request_schema_bound(request, prepared.prior)
                if _view_change_request_fingerprint(database_fp, prepared.request) != (
                    _view_change_request_fingerprint(database_fp, incoming)
                ):
                    raise IdempotencyConflictError(
                        "View idempotency key was reused for a different request."
                    )
                change_id = str(intent["change_id"])
            else:
                prepared = await self.view_workflows.prepare_view_change(request)
                change_id = "view-" + fingerprint_json(
                    {
                        "database": database_fp,
                        "schema": schema_name,
                        "view": view_name,
                        "operation": prepared.operation,
                        "target": prepared.target_fingerprint,
                        "prior": prepared.prior.definition_fingerprint,
                        "idempotency_key": normalized_idempotency_key,
                    }
                )[:32]
                state = prepared_view_change_state(prepared)
                intent = self.performance_store.create_view_change_intent(
                    change_id=change_id,
                    database_fingerprint=database_fp,
                    request_fingerprint=fingerprint_json(state),
                    payload=state,
                    raw_sql_persistence_authorized=True,
                )
                prepared = prepared_view_change_from_state(intent["payload"])
        else:
            prepared = await self.view_workflows.prepare_view_change(request)
            change_id = "view-" + fingerprint_json(
                {
                    "database": database_fingerprint(
                        database_name,
                        self.config.server,
                    ),
                    "schema": schema_name,
                    "view": view_name,
                    "operation": prepared.operation,
                    "target": prepared.target_fingerprint,
                    "prior": prepared.prior.definition_fingerprint,
                    "idempotency_key": idempotency_key,
                }
            )[:32]
        existing = self._prepared_view_changes.get(change_id)
        if existing is not None and (
            existing.target_fingerprint != prepared.target_fingerprint
            or existing.prior.definition_fingerprint
            != prepared.prior.definition_fingerprint
        ):
            raise ValueError("Prepared view change identifier conflict.")
        self._prepared_view_changes[change_id] = prepared
        preview = await self.view_workflows.preview_view_change(prepared)
        return {
            "change_id": change_id,
            "process_local": not durable,
            "durable": durable,
            "restart_requires_reprepare": not durable,
            "raw_view_sql_persisted": durable,
            "intent_status": intent["status"] if intent is not None else None,
            **preview,
        }

    def _prepared_view_change(
        self,
        database_name: str,
        change_id: str,
    ) -> PreparedViewChange:
        if self.config.persist_view_sql_state:
            try:
                prepared, _intent = self._durable_view_change(
                    database_name,
                    change_id,
                )
            except ContractNotFoundError:
                pass
            else:
                return prepared
        prepared = self._prepared_view_changes.get(change_id)
        if prepared is None:
            raise ValueError(
                "Unknown view change; prepare it in this MCP process."
            )
        if prepared.request.database_name.casefold() != database_name.casefold():
            raise PermissionError("Prepared view change belongs to another database.")
        return prepared

    def _durable_view_change(
        self,
        database_name: str,
        change_id: str,
    ) -> tuple[PreparedViewChange, dict[str, Any]]:
        intent = self.performance_store.get_view_change_intent(change_id)
        if not database_fingerprint_matches(
            str(intent["database_fingerprint"]),
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise PermissionError("Prepared view change belongs to another database.")
        if fingerprint_json(intent["payload"]) != intent["request_fingerprint"]:
            raise ValueError("Durable view change payload fingerprint does not match.")
        prepared = prepared_view_change_from_state(intent["payload"])
        if prepared.request.database_name.casefold() != database_name.casefold():
            raise PermissionError("Prepared view change belongs to another database.")
        self._prepared_view_changes[change_id] = prepared
        return prepared, intent

    def _require_durable_view_change(
        self,
        database_name: str,
        change_id: str,
    ) -> tuple[PreparedViewChange, dict[str, Any]]:
        if (
            not self.config.persist_view_sql_state
            or self.config.performance_state_dir == ":memory:"
        ):
            raise PermissionError(
                "View apply and rollback require "
                "AZURE_SQL_PERSIST_VIEW_SQL_STATE=true so exact rollback survives restart."
            )
        return self._durable_view_change(database_name, change_id)

    async def _reconcile_durable_view_change(
        self,
        change_id: str,
        prepared: PreparedViewChange,
        intent: dict[str, Any],
    ) -> dict[str, Any]:
        verification = await self.view_workflows.verify_view_change(prepared)
        if intent.get("receipt") is None:
            if getattr(prepared, "operation", None) == "noop":
                prior_restored, current = await self.view_workflows.prior_state_restored(
                    prepared
                )
                if prior_restored:
                    updated = self.performance_store.update_view_change_intent(
                        change_id,
                        status="already_applied",
                        expected_version=int(intent["version"]),
                        raw_sql_persistence_authorized=True,
                    )
                    return {
                        "change_id": change_id,
                        "status": "already_applied",
                        "intent_status": updated["status"],
                        "workflow_applied": False,
                        "prior_state_restored": True,
                        "current": current.as_dict(),
                        "verification": verification.as_dict(),
                        "reason": (
                            "the unchanged prior snapshot proves this interrupted "
                            "no-op performed no mutation"
                        ),
                    }
            if verification.verified and verification.workflow_commit_proven:
                receipt = view_apply_receipt(verification.actual)
                self.view_workflows.register_apply_receipt(
                    prepared,
                    verification.actual,
                )
                updated = self.performance_store.update_view_change_intent(
                    change_id,
                    status="applied",
                    expected_version=int(intent["version"]),
                    raw_sql_persistence_authorized=True,
                    receipt=receipt,
                )
                return {
                    "change_id": change_id,
                    "status": "reconciled_applied",
                    "intent_status": updated["status"],
                    "workflow_applied": True,
                    "verification": verification.as_dict(),
                    "reason": (
                        "the exact workflow marker and target state prove that "
                        "the interrupted view change committed"
                    ),
                }
            updated = self.performance_store.update_view_change_intent(
                change_id,
                status="hold",
                expected_version=int(intent["version"]),
                raw_sql_persistence_authorized=True,
            )
            return {
                "change_id": change_id,
                "status": "hold",
                "intent_status": updated["status"],
                "workflow_applied": False,
                "verification": verification.as_dict(),
                "reason": (
                    "the target definition may have been applied externally, but "
                    "there is no durable dispatch/commit receipt for this workflow; "
                    "rollback ownership was not adopted"
                ),
            }
        if verification.verified:
            receipt_snapshot = view_snapshot_from_receipt(intent["receipt"])
            self.view_workflows.register_apply_receipt(prepared, receipt_snapshot)
            updated = self.performance_store.update_view_change_intent(
                change_id,
                status="applied",
                expected_version=int(intent["version"]),
                raw_sql_persistence_authorized=True,
            )
            return {
                "change_id": change_id,
                "status": "reconciled_applied",
                "intent_status": updated["status"],
                "workflow_applied": True,
                "verification": verification.as_dict(),
            }
        prior_restored, current = await self.view_workflows.prior_state_restored(
            prepared
        )
        updated = self.performance_store.update_view_change_intent(
            change_id,
            status="hold",
            expected_version=int(intent["version"]),
            raw_sql_persistence_authorized=True,
        )
        return {
            "change_id": change_id,
            "status": "hold",
            "intent_status": updated["status"],
            "workflow_applied": intent.get("receipt") is not None,
            "prior_state_restored": prior_restored,
            "current": current.as_dict(),
            "verification": verification.as_dict(),
            "reason": (
                "interrupted view apply is not at the prepared target; "
                "the DDL was not replayed"
            ),
        }

    async def _apply_prepared_view_change(
        self,
        database_name: str,
        change_id: str,
        reviewed_intent: bool,
        idempotency_key: str,
    ) -> dict[str, Any]:
        if self.config.profile != McpProfile.SANDBOX:
            raise PermissionError("View apply requires the sandbox MCP profile.")
        if not reviewed_intent:
            raise PermissionError("reviewed_intent must be true for view apply.")
        prepared, intent = self._require_durable_view_change(
            database_name,
            change_id,
        )
        if intent["status"] in {"applying", "hold"}:
            return await self._reconcile_durable_view_change(
                change_id,
                prepared,
                intent,
            )
        if intent["status"] == "applied":
            receipt = intent.get("receipt")
            if receipt is None:
                raise ValueError("Durable applied view intent has no receipt.")
            self.view_workflows.register_apply_receipt(
                prepared,
                view_snapshot_from_receipt(receipt),
            )
            verification = await self.view_workflows.verify_view_change(prepared)
            if not verification.verified:
                intent = self.performance_store.update_view_change_intent(
                    change_id,
                    status="hold",
                    expected_version=int(intent["version"]),
                    raw_sql_persistence_authorized=True,
                )
                return {
                    "change_id": change_id,
                    "status": "hold",
                    "intent_status": intent["status"],
                    "workflow_applied": True,
                    "verification": verification.as_dict(),
                    "reason": (
                        "the workflow-owned view no longer verifies; "
                        "the durable rollback receipt was retained"
                    ),
                }
            return {
                "change_id": change_id,
                "status": "already_applied",
                "intent_status": "applied",
                "workflow_applied": True,
                "verification": verification.as_dict(),
            }
        if intent["status"] == "already_applied":
            verification = await self.view_workflows.verify_view_change(prepared)
            if not verification.verified:
                intent = self.performance_store.update_view_change_intent(
                    change_id,
                    status="hold",
                    expected_version=int(intent["version"]),
                    raw_sql_persistence_authorized=True,
                )
                return {
                    "change_id": change_id,
                    "status": "hold",
                    "intent_status": intent["status"],
                    "workflow_applied": False,
                    "verification": verification.as_dict(),
                    "reason": "the externally applied view no longer verifies",
                }
            return {
                "change_id": change_id,
                "status": "already_applied",
                "intent_status": "already_applied",
                "workflow_applied": False,
                "verification": verification.as_dict(),
            }
        if intent["status"] == "rolled_back":
            raise ValueError("Rolled-back view intent cannot be applied again.")
        if intent["status"] != "prepared":
            raise ValueError(f"View intent is not applyable from status={intent['status']}.")
        normalized_idempotency_key = _view_idempotency_identity(idempotency_key)
        existing_key = prepared.request.idempotency_key
        if existing_key and existing_key != normalized_idempotency_key:
            raise ValueError(
                "Apply idempotency key does not match the prepared change."
            )
        prepared = replace(
            prepared,
            request=replace(
                prepared.request,
                reviewed_intent=True,
                idempotency_key=normalized_idempotency_key,
            ),
        )
        self._prepared_view_changes[change_id] = prepared
        intent = self.performance_store.update_view_change_intent(
            change_id,
            status="applying",
            expected_version=int(intent["version"]),
            raw_sql_persistence_authorized=True,
        )
        result = await self.view_workflows.apply_prepared_view_change(prepared)
        workflow_applied = result.get("workflow_applied") is True
        try:
            verification = await self.view_workflows.verify_view_change(prepared)
            if workflow_applied:
                receipt_payload = result.get("apply_receipt")
                receipt_snapshot = view_snapshot_from_receipt(receipt_payload)
                self.view_workflows.register_apply_receipt(
                    prepared,
                    receipt_snapshot,
                )
                receipt = view_apply_receipt(receipt_snapshot)
                status = "applied" if verification.verified else "hold"
            else:
                receipt = None
                status = "already_applied" if verification.verified else "hold"
            intent = self.performance_store.update_view_change_intent(
                change_id,
                status=status,
                expected_version=int(intent["version"]),
                raw_sql_persistence_authorized=True,
                receipt=receipt,
            )
        except (ValueError, LookupError, PerformanceStoreError) as exc:
            if not workflow_applied:
                raise
            # The workflow-owned DDL was dispatched: a refusal here does not
            # mean nothing changed, so it is not request_refused.
            raise ViewVerificationError(
                "the view change was dispatched, then its outcome could not be "
                "recorded; read the view with verify_view_change before any retry"
            ) from exc
        response = {
            "change_id": change_id,
            "intent_status": intent["status"],
            **result,
        }
        if status == "hold":
            response.update(
                {
                    "status": "hold",
                    "verification": verification.as_dict(),
                    "reason": (
                        "post-apply verification failed; durable ownership and "
                        "the exact rollback receipt were retained"
                        if workflow_applied
                        else "the target could not be verified"
                    ),
                }
            )
        return response

    async def _verify_view_change(
        self,
        database_name: str,
        change_id: str,
    ) -> dict[str, Any]:
        prepared = self._prepared_view_change(database_name, change_id)
        try:
            durable_prepared, intent = self._durable_view_change(
                database_name,
                change_id,
            )
        except ContractNotFoundError:
            intent = None
        else:
            prepared = durable_prepared
            if intent["status"] in {"applying", "hold"}:
                return await self._reconcile_durable_view_change(
                    change_id,
                    prepared,
                    intent,
                )
            if intent["status"] == "applied" and intent.get("receipt") is not None:
                self.view_workflows.register_apply_receipt(
                    prepared,
                    view_snapshot_from_receipt(intent["receipt"]),
                )
        verification = await self.view_workflows.verify_view_change(prepared)
        return {
            "change_id": change_id,
            "intent_status": intent["status"] if intent is not None else None,
            **verification.as_dict(),
        }

    async def _rollback_view_change(
        self,
        database_name: str,
        change_id: str,
    ) -> dict[str, Any]:
        if self.config.profile != McpProfile.SANDBOX:
            raise PermissionError("View rollback requires the sandbox MCP profile.")
        prepared, intent = self._require_durable_view_change(
            database_name,
            change_id,
        )
        if not prepared.request.idempotency_key:
            raise ValueError("Apply the reviewed view change before rollback.")
        if intent["status"] == "rolled_back":
            restored, current = await self.view_workflows.prior_state_restored(prepared)
            if not restored:
                raise ValueError("Rolled-back view intent no longer matches prior state.")
            return {
                "change_id": change_id,
                "status": "already_rolled_back",
                "intent_status": "rolled_back",
                "snapshot": current.as_dict(),
                **self.view_workflows.restore_exactness(prepared, current),
            }
        if intent["status"] == "applying" or (
            intent["status"] == "hold" and intent.get("receipt") is None
        ):
            reconciliation = await self._reconcile_durable_view_change(
                change_id,
                prepared,
                intent,
            )
            if reconciliation["intent_status"] != "applied":
                raise ValueError(
                    "Interrupted view apply is not at the prepared target; rollback is held."
                )
            prepared, intent = self._durable_view_change(database_name, change_id)
        if intent["status"] not in {"applied", "hold"}:
            raise ValueError(
                "Only a workflow-applied durable view intent can be rolled back."
            )
        receipt = intent.get("receipt")
        if receipt is None:
            raise ValueError("Durable applied view intent has no receipt.")
        self.view_workflows.register_apply_receipt(
            prepared,
            view_snapshot_from_receipt(receipt),
        )
        restored, current = await self.view_workflows.prior_state_restored(prepared)
        if restored:
            updated = self.performance_store.update_view_change_intent(
                change_id,
                status="rolled_back",
                expected_version=int(intent["version"]),
                raw_sql_persistence_authorized=True,
            )
            return {
                "change_id": change_id,
                "status": "already_rolled_back",
                "intent_status": updated["status"],
                "snapshot": current.as_dict(),
                **self.view_workflows.restore_exactness(prepared, current),
            }
        result = await self.view_workflows.rollback_view_change(prepared)
        try:
            updated = self.performance_store.update_view_change_intent(
                change_id,
                status="rolled_back",
                expected_version=int(intent["version"]),
                raw_sql_persistence_authorized=True,
            )
        except (ValueError, LookupError, PerformanceStoreError) as exc:
            # The restore DDL was dispatched: not request_refused.
            raise ViewVerificationError(
                "the view rollback was dispatched, then its outcome could not be "
                "recorded; read the view with verify_view_change before any retry"
            ) from exc
        return {
            "change_id": change_id,
            "intent_status": updated["status"],
            **result,
        }

    @staticmethod
    def _allowed_test_index_prefixes(policy: DatabasePolicy) -> tuple[str, ...]:
        """The workflow prefix, then the prefixes the database policy lists."""

        return tuple(dict.fromkeys((TEST_INDEX_PREFIX, *policy.test_index_name_prefixes)))

    def _resolve_test_index_prefix(
        self,
        policy: DatabasePolicy,
        index_name_prefix: str | None,
    ) -> tuple[str, str]:
        """The prefix of the generated test index names, and a tag for their readable part.

        A requested prefix that is, or extends, an allowed prefix is used as
        given. Any other value never refuses the call: the first prefix of the
        database policy is used, and the leading letters, digits and
        underscores of the request are kept as a tag in the name. Every
        generated name so starts with an allowed prefix, which is what the
        drop guard and lease cleanup check.
        """

        if index_name_prefix is None:
            return TEST_INDEX_PREFIX, ""
        if extends_test_index_prefix(
            index_name_prefix, self._allowed_test_index_prefixes(policy)
        ):
            return index_name_prefix, ""
        prefix = policy.test_index_name_prefixes[0]
        tag = _usable_name_text(index_name_prefix)
        if tag.casefold().startswith(prefix.casefold()):
            tag = tag[len(prefix) :]
        return prefix, tag.strip("_")[:_MAX_TEST_INDEX_NAME_TAG_LENGTH].rstrip("_")

    @staticmethod
    def _index_set_specs(
        schema_name: str | None,
        table_name: str | None,
        key_columns: list[str] | None,
        include_columns: list[str] | None,
        filter_definition: str | None,
        is_unique: bool,
        data_compression: str | None,
        indexes: list[dict[str, Any]] | None,
        index_kind: str | None = None,
        partition_aligned: bool | None = None,
    ) -> list[dict[str, Any]]:
        """The 1 to 4 index definitions of one candidate, from either argument form."""

        if indexes is not None:
            if (
                any(
                    value
                    for value in (
                        schema_name,
                        table_name,
                        key_columns,
                        include_columns,
                        filter_definition,
                        data_compression,
                        index_kind,
                    )
                )
                or is_unique
                or partition_aligned is not None
            ):
                raise ValueError(
                    "Pass indexes, or the single-index arguments (schema_name, "
                    "table_name, key_columns and the rest), not both."
                )
            if not 1 <= len(indexes) <= MAX_TEST_INDEX_SET:
                raise ValueError(
                    f"indexes must hold 1 to {MAX_TEST_INDEX_SET} index definitions."
                )
            raw = [dict(item) for item in indexes]
        else:
            if not schema_name or not table_name:
                raise ValueError("schema_name and table_name are required without indexes.")
            raw = [
                {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "key_columns": key_columns,
                    "include_columns": include_columns,
                    "filter_definition": filter_definition,
                    "is_unique": is_unique,
                    "data_compression": data_compression,
                    "kind": index_kind,
                    "partition_aligned": partition_aligned,
                }
            ]
        specs: list[dict[str, Any]] = []
        for item in raw:
            if not item.get("key_columns"):
                raise ValueError("key_columns must contain at least one column.")
            kind = str(item.get("kind") or INDEX_KIND_ROWSTORE).strip().casefold()
            if kind not in TEST_INDEX_KINDS:
                raise ValueError("kind must be " + " or ".join(TEST_INDEX_KINDS) + ".")
            columnstore = kind == INDEX_KIND_NONCLUSTERED_COLUMNSTORE
            # The default of the engine for the kind: the same index, the same name.
            engine_default = "COLUMNSTORE" if columnstore else "NONE"
            compression = str(item.get("data_compression") or engine_default).strip().upper()
            if columnstore and compression not in {"COLUMNSTORE", "COLUMNSTORE_ARCHIVE"}:
                raise ValueError(
                    "data_compression must be COLUMNSTORE or COLUMNSTORE_ARCHIVE for a "
                    "nonclustered_columnstore index."
                )
            if not columnstore and compression not in {"NONE", "ROW", "PAGE"}:
                raise ValueError("data_compression must be NONE, ROW or PAGE.")
            specs.append(
                {
                    "schema": item.get("schema_name") or "dbo",
                    "table": item.get("table_name") or "",
                    "key_columns": item["key_columns"],
                    "include_columns": item.get("include_columns"),
                    "filter_definition": item.get("filter_definition"),
                    "is_unique": bool(item.get("is_unique", False)),
                    "data_compression": None if compression == engine_default else compression,
                    "kind": kind,
                    "partition_aligned": item.get("partition_aligned") is not False,
                }
            )
        return specs

    @staticmethod
    def _slow_baseline_seconds(
        slow_baseline_cases: Mapping[str, float] | None,
        cases: Sequence[Mapping[str, Any]],
    ) -> dict[str, float]:
        names = {str(parameter_case["name"]) for parameter_case in cases}
        slow: dict[str, float] = {}
        for name, seconds in (slow_baseline_cases or {}).items():
            if (
                str(name) not in names
                or not isinstance(seconds, (int, float))
                or isinstance(seconds, bool)
                or seconds <= 0
            ):
                raise ValueError(
                    "slow_baseline_cases maps a parameter case of this benchmark call "
                    "to the seconds its baseline takes."
                )
            slow[str(name)] = float(seconds)
        return slow

    def _session_slow_index_baselines(
        self,
        session_id: str,
        compare_order: bool,
    ) -> dict[str, dict[str, Any]]:
        """The one-run baselines the session holds, from either benchmark tool.

        Each holds the sample, the plan brief and the result digest of that
        run, so a later index is still compared with the baseline's rows.
        """

        return {
            name: item
            for name, item in self.performance_workflows.session_slow_baselines(
                session_id
            ).items()
            if isinstance(item.get("result"), Mapping)
            and item.get("order_compared") is compare_order
        }

    async def _built_index_facts(
        self,
        database_name: str,
        member: _IndexSetMember,
    ) -> dict[str, Any]:
        """Size and B-tree depth of a test index after its build.

        A read that fails is a note, never a failed benchmark: the index is
        built and leased either way.
        """

        try:
            rows = await self.executor.fetch_all(
                database_name,
                _BUILT_INDEX_SQL,
                params=[member.schema, member.table, member.index_name],
                max_rows=1,
                sql_origin="server_built",
            )
        except Exception as exc:
            note_exception(exc, "server.built_index_facts")
            rows = []
        row = rows[0] if rows else {}

        def number(value: Any) -> float | None:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        pages = number(row.get("used_pages"))
        depth = number(row.get("index_depth"))
        row_count = number(row.get("row_count"))
        facts: dict[str, Any] = {
            "size_mb": round(pages * 8 / 1024, 2) if pages is not None else None,
            "index_depth": int(depth) if depth is not None else None,
            "row_count": int(row_count) if row_count is not None else None,
        }
        if member.candidate.index_kind == INDEX_KIND_NONCLUSTERED_COLUMNSTORE:
            # No B-tree, so no depth (the catalog read gives 0). What matters is
            # how the rows compressed.
            facts["index_depth"] = None
            facts.update(await self._columnstore_index_facts(database_name, member))
            if pages is None:
                facts["facts_note"] = "The size of the built index could not be read."
            return facts
        if pages is None and depth is None:
            facts["facts_note"] = "The size or depth of the built index could not be read."
        elif depth is None:
            facts["facts_note"] = "The B-tree depth of the built index could not be read."
        elif pages is None:
            facts["facts_note"] = "The size of the built index could not be read."
        return facts

    async def _columnstore_index_facts(
        self,
        database_name: str,
        member: _IndexSetMember,
    ) -> dict[str, Any]:
        """Row groups of a built columnstore test index; empty when they cannot be read."""

        try:
            rows = await self.executor.fetch_all(
                database_name,
                ROW_GROUP_SQL.format(object_filter=OBJECT_FILTER),
                params=[member.schema, member.table],
                sql_origin="server_built",
            )
        except Exception as exc:
            note_exception(exc, "server.columnstore_index_facts")
            return {}
        summary = next(
            (
                item
                for item in summarize_columnstore_indexes(rows)
                if item.get("index_name") == member.index_name
            ),
            None,
        )
        if summary is None:
            return {}
        return {
            "row_groups": {
                key: summary.get(key)
                for key in ("compressed_row_groups", "compressed_rows", "size_bytes", "trim_reasons")
                if summary.get(key) is not None
            }
        }

    @staticmethod
    def _index_lease_members(lease: Mapping[str, Any]) -> list[dict[str, Any]]:
        """One lease-shaped view per index of a lease.

        A lease of one index is its own view. A set keeps one entry per index
        under metadata.members; each view joins that entry with the fields the
        whole lease shares (owner fence, phase).
        """

        metadata = lease.get("metadata") or {}
        entries = metadata.get("members")
        if not isinstance(entries, list) or not entries:
            return [dict(lease)]
        shared = {key: value for key, value in metadata.items() if key != "members"}
        views: list[dict[str, Any]] = []
        for entry in entries:
            if (
                not isinstance(entry, Mapping)
                or not isinstance(entry.get("index_name"), str)
                or not entry.get("index_name")
                or not isinstance(entry.get("object_fingerprint"), str)
                or not entry.get("object_fingerprint")
            ):
                raise ValueError("index lease set has an invalid member")
            views.append(
                {
                    **lease,
                    "index_name": entry.get("index_name"),
                    "object_fingerprint": entry.get("object_fingerprint"),
                    "metadata": {**shared, **entry},
                    "set_member": True,
                }
            )
        return views

    def _session_hold(self, database_fingerprint: str, session_id: str) -> dict[str, Any] | None:
        """The lease that keeps test objects in place for this session, or None."""

        for lease in self.performance_store.list_open_index_leases(
            database_fingerprint=database_fingerprint
        ):
            metadata = lease.get("metadata") or {}
            if (
                lease["session_id"] == session_id
                and lease["status"] == "active"
                and metadata.get(HOLD_FLAG) is True
                and isinstance(metadata.get("members"), list)
            ):
                return lease
        return None

    def _held_test_objects(self, database_fingerprint: str, session_id: str) -> list[dict[str, Any]]:
        """The test objects this session holds, each with its age and the expiry of the hold."""

        hold = self._session_hold(database_fingerprint, session_id)
        if hold is None:
            return []
        return [
            _held_object_report(entry, hold)
            for entry in hold["metadata"]["members"]
            if isinstance(entry, Mapping)
        ]

    async def _table_row_count(self, database_name: str, schema: str, table: str) -> int | None:
        """Rows of a table from the partition statistics; None when it cannot be read."""

        try:
            rows = await self.executor.fetch_all(
                database_name,
                _TABLE_ROW_COUNT_SQL,
                params=[schema, table],
                max_rows=1,
                sql_origin="server_built",
            )
        except Exception as exc:
            note_exception(exc, "server.table_row_count")
            return None
        value = rows[0].get("row_count") if rows else None
        return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None

    async def _build_time_estimate(
        self,
        database_name: str,
        session_id: str,
        members: Sequence[_IndexSetMember],
    ) -> dict[str, Any]:
        """Seconds the builds of ``members`` should take, from the first build of the session.

        Each index reads its whole table once, so the estimate is the rows
        of the tables at the rows per second that the first build of this
        session reached. No build yet, or a row count that cannot be read:
        no estimate, and the cap of the call then applies between steps only.
        """

        rate = next(
            (
                float(evidence.metadata["build_rate_rows_per_second"])
                for evidence in self.performance_store.list_evidence_for_session(session_id)
                if isinstance(evidence.metadata.get("build_rate_rows_per_second"), (int, float))
            ),
            None,
        )
        counts = [
            await self._table_row_count(database_name, member.schema, member.table)
            for member in members
        ]
        rows = sum(count for count in counts if count is not None) if None not in counts else None
        return {
            "table_rows": rows,
            "rate_rows_per_second": rate,
            "estimated_seconds": round(rows / rate, 1) if rows is not None and rate else None,
            "basis": (
                "table rows at the rate of the first test index build of this session"
                if rows is not None and rate
                else "no estimate: no test index build was measured in this session yet"
                if rate is None
                else "no estimate: the row count of the table could not be read"
            ),
        }

    def _with_held_objects_present(
        self,
        database_name: str,
        session_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Say in a rewrite benchmark that held test objects existed while it ran."""

        held = self._held_test_objects(
            database_fingerprint(database_name, self.config.server), session_id
        )
        if not held:
            return payload
        return {
            **payload,
            "held_test_objects_present": {
                "objects": held,
                "note": (
                    "Both statements ran while these test objects of the session "
                    "existed, so this compares the two statements with them in place. "
                    "A baseline taken from the session (baseline_source "
                    "session_single_run) ran before they were built."
                ),
            },
        }

    def _baselines_before_hold(
        self,
        session_id: str,
        baseline_sql: str,
        compare_order: bool,
        plan_reuse: str,
        case_names: Sequence[str],
    ) -> dict[str, dict[str, Any]]:
        """The baselines of ``baseline_sql`` this session measured with no held object in place."""

        sql_fingerprint = fingerprint_json({"baseline_sql": baseline_sql})
        pool: dict[str, dict[str, Any]] = {}
        for name, items in self.performance_workflows.session_baselines_before_hold(
            session_id
        ).items():
            for item in items:
                if (
                    item.get("sql_fingerprint") == sql_fingerprint
                    and item.get("order_compared") is compare_order
                    and item.get("plan_reuse") == plan_reuse
                    and isinstance(item.get("result"), Mapping)
                    # On a shared plan the first case compiles: the same cases, the same order.
                    and (plan_reuse == "per_case" or item.get("case_order") == list(case_names))
                ):
                    pool[name] = item
        return pool

    def _return_hold_to_idle(self, lease_id: str, owner: str, lease_minutes: int) -> None:
        """Give a hold back its own bounded expiry when a call that took it did not start.

        While a call uses a hold, the open session protects the lease past its
        expiry. A call that stops before its first statement must not leave
        that protection behind.
        """

        try:
            current = self.performance_store.get_index_lease(lease_id)
            if current["status"] != "active" or (current.get("metadata") or {}).get(
                "hold_state"
            ) != "in_call":
                return
            self.performance_store.update_index_lease(
                lease_id,
                status="active",
                metadata={"hold_state": "idle"},
                owner_reference=owner,
                expected_version=current["version"],
                expires_at_utc=(
                    datetime.now(timezone.utc) + timedelta(minutes=lease_minutes)
                ).isoformat(),
            )
        except Exception as exc:
            note_exception(exc, "server.return_hold_to_idle")

    def _require_baselines_before_hold(
        self,
        session_id: str,
        baseline_sql: str,
        compare_order: bool,
        plan_reuse: str,
        case_names: Sequence[str],
        held_entries: Sequence[Mapping[str, Any]],
        session_pool: Mapping[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """The baseline of every case of a call that starts with held objects in place.

        A held object is in place, so the baseline statement cannot run clean
        now: each case takes the baseline this session measured before the
        hold, or its one-run baseline (``session_pool``). A case with neither
        is refused before any run.
        """

        pool = {
            **self._baselines_before_hold(
                session_id, baseline_sql, compare_order, plan_reuse, case_names
            ),
            **session_pool,
        }
        missing = [name for name in case_names if name not in pool]
        if missing:
            raise ValueError(
                "This session holds test objects ("
                + ", ".join(
                    str(_held_object_report(entry)["index_name"]) for entry in held_entries
                )
                + "), and parameter case(s) "
                + ", ".join(missing)
                + " have no baseline measured before they were built for this "
                "statement, plan_reuse and compare_order. The statement cannot give "
                "a clean baseline while they exist. Name only the cases that were "
                "measured in the call that kept the objects, with the same plan_reuse "
                "and compare_order, or call finalize_tuning_session to drop them. "
                "No query and no DDL was run."
            )
        return pool

    def _recover_index_benchmark_result(
        self,
        *,
        session_id: str,
        candidate_id: str,
        phase: str,
        runs: int,
        parameter_case_count: int,
        reservation: Mapping[str, Any],
        reservation_owner: str,
        request_fingerprint: str,
        evidence_operation_key: str,
        benchmark_operation_key: str,
        index_ddl: str,
        rollback_ddl: str,
        index_definition_fingerprint: str,
    ) -> dict[str, Any]:
        """Finish committed index evidence without repeating DDL or query work."""

        evidence = self.performance_store.get_idempotent_evidence(
            evidence_operation_key,
            request_fingerprint=request_fingerprint,
        )
        if evidence is None:
            finalized = reservation["status"] != "reserved"
            return {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "classification": "inconclusive",
                "reason": (
                    "This idempotent index benchmark has a durable reservation "
                    "but no committed result. Reconcile its lease; no DDL or query "
                    "was rerun."
                ),
                "failure_code": (
                    "index_benchmark_request_already_finalized"
                    if finalized
                    else "index_benchmark_request_reconciliation_required"
                ),
                "executions": 0,
                "execution_reservation_id": reservation["reservation_id"],
                "reservation_status": reservation["status"],
                "session_continues": True,
            }

        metadata = dict(evidence.metadata)
        if (
            metadata.get("session_id") != session_id
            or metadata.get("candidate_id") != candidate_id
            or metadata.get("phase") != phase
            or metadata.get("execution_reservation_id")
            != reservation["reservation_id"]
        ):
            raise RuntimeError(
                "Committed index evidence does not match its execution reservation."
            )
        metrics = dict(evidence.metrics)
        state = str(metrics.get("classification") or "")
        if not state:
            raise RuntimeError("Committed index evidence has no classification.")
        performance_classification = str(
            metrics.get("performance_classification") or state
        )
        candidate_state = (
            "inconclusive" if state == "proof_contract_required" else state
        )
        measured_executions = int(evidence.observed_execution_count)
        if phase == "screening" and candidate_state in {"promising", "improved"}:
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="screening",
                screen_runs=runs,
                parameter_cases=parameter_case_count,
                executions=measured_executions,
                evidence_ids=(evidence.evidence_id,),
                idempotency_key=benchmark_operation_key,
            )
            durable_state = updated.state
        else:
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state=candidate_state,
                screen_runs=runs if phase == "screening" else 0,
                finalist_runs=runs if phase == "finalist" else 0,
                parameter_cases=parameter_case_count,
                executions=measured_executions,
                evidence_ids=(evidence.evidence_id,),
                failure_code=(
                    state
                    if candidate_state
                    in {"inconclusive", "cleanup_required", "equivalence_failed"}
                    else None
                ),
                idempotency_key=benchmark_operation_key,
            )
            durable_state = updated.state

        reservation_status = str(reservation["status"])
        if reservation_status == "reserved":
            reservation_update = (
                self.performance_store.complete_execution_attempts
                if measured_executions
                else self.performance_store.release_execution_attempts
            )
            finalized_reservation = reservation_update(
                str(reservation["reservation_id"]),
                dispatched_attempt_count=measured_executions,
                owner_reference=reservation_owner,
                expected_version=int(reservation["version"]),
            )
            reservation_status = str(finalized_reservation["status"])

        lease_id = metadata.get("lease_id")
        if not isinstance(lease_id, str) or not lease_id:
            raise RuntimeError("Committed index evidence has no lease identity.")
        lease = self.performance_store.get_index_lease(lease_id)
        public_lease = self._public_index_lease(lease)
        parameter_results = metrics.get("parameter_results")
        result_rows = parameter_results if isinstance(parameter_results, list) else []
        parameter_case_receipts = metadata.get("parameter_case_receipts")
        result_receipts = (
            parameter_case_receipts
            if isinstance(parameter_case_receipts, list)
            else []
        )
        equivalence = metadata.get("equivalence")
        result = {
            "session_id": session_id,
            "candidate_id": candidate_id,
            "classification": state,
            "performance_classification": performance_classification,
            "objective": metrics.get("objective"),
            "decision_basis": metrics.get("decision_basis"),
            "durable_state": durable_state,
            "reason": metadata.get("reason")
            or "Recovered committed index evidence without rerunning work.",
            "phase": phase,
            "executions": measured_executions,
            "metrics": result_rows[0] if result_rows else {},
            "parameter_results": result_rows,
            "parameter_case_receipts": result_receipts,
            "equivalence": equivalence if isinstance(equivalence, list) else [],
            "equivalence_preflight": metadata.get("equivalence_preflight"),
            "proof_scope": metadata.get("proof_scope"),
            "lineage": metadata.get("lineage"),
            "evidence_id": evidence.evidence_id,
            "lease": public_lease,
            "index_definition_fingerprint": metadata.get(
                "index_definition_fingerprint",
                index_definition_fingerprint,
            ),
            "index_ddl": index_ddl,
            "rollback_ddl": rollback_ddl,
            "execution_reservation_id": reservation["reservation_id"],
            "reservation_status": reservation_status,
            "recovered_from_durable_evidence": True,
            "session_continues": True,
        }
        if isinstance(metadata.get("indexes"), list):
            result["indexes"] = metadata["indexes"]
        if isinstance(metadata.get("test_objects"), Mapping):
            result["test_objects"] = dict(metadata["test_objects"])
        if isinstance(metadata.get("time_spent"), Mapping):
            result["time_spent"] = dict(metadata["time_spent"])
        if isinstance(metadata.get("replacement_experiment"), Mapping):
            result["replacement_experiment"] = dict(metadata["replacement_experiment"])
            result["deployment_ready"] = False
        failure_diagnostic = metadata.get("failure_diagnostic")
        if isinstance(failure_diagnostic, Mapping):
            result["failure_diagnostic"] = dict(failure_diagnostic)
        return result

    async def _benchmark_index_candidate(
        self,
        database_name: str,
        session_id: str,
        candidate_id: str,
        sql: str,
        schema_name: str | None,
        table_name: str | None,
        key_columns: list[str] | None,
        include_columns: list[str] | None,
        filter_definition: str | None,
        is_unique: bool,
        phase: str,
        online: bool,
        compare_order: bool | None,
        lease_minutes: int,
        idempotency_key: str | None,
        parameter_cases: list[dict[str, Any]] | None = None,
        *,
        indexes: list[dict[str, Any]] | None = None,
        data_compression: str | None = None,
        index_name_prefix: str | None = None,
        replaces: str | None = None,
        slow_baseline_cases: Mapping[str, float] | None = None,
        candidate_sql: str | None = None,
        runs: int | None = None,
        plan_reuse: str = "per_case",
        index_kind: str | None = None,
        partition_aligned: bool | None = None,
        keep_for_session: bool = False,
        max_call_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Measure a query before, with and after a leased set of 1 to 4 test indexes.

        One lease covers the whole set: every index is created or none is
        left, and every created index is dropped on every path.

        ``keep_for_session`` leaves the set in place under the lease when the
        call returns, for at most ``lease_minutes``. A later call of the same
        session reuses each held index whose definition matches and builds
        only the others; its baseline is the one the session measured before
        the hold, because the statement cannot run clean while a held object
        exists. Without ``keep_for_session`` a call drops every index it
        used. finalize_tuning_session drops everything the session holds.

        ``max_call_seconds`` caps the call: no run and no build starts after
        it, and a build whose estimate (table rows at the rate of the first
        build of the session) does not fit is not started. The result is then
        ``partial`` with no verdict; with ``keep_for_session`` what was built
        stays held. A build that lost a deadlock or a lock wait is retried
        once. ``time_spent`` in the result is the seconds per phase.

        ``candidate_sql`` is the one-call path for a rewrite that needs its
        indexes: ``sql`` is the statement of the performance case and runs in
        the two baseline arms (no test index), ``candidate_sql`` is the
        rewrite and runs while the index set exists. A finalist compares the
        two statements in one snapshot per parameter case before the indexes
        are dropped, so the verdict is one measurement of the original
        against the rewrite with its indexes. The candidate is a
        rewrite_plus_index candidate; it needs no parent.

        Every parameter case of the call is measured under the one build.
        ``runs`` lowers the runs per arm and case (1 to the session limit of
        the phase). The first run of each arm then sets its count
        (``adaptive_run_count``): a statement that takes seconds gets at most
        3 runs, and one that takes 10 seconds or more runs once; such a
        baseline is kept for the session and not run again. ``plan_reuse``:
        ``per_case`` compiles a plan for each case; ``first_case`` gives all
        cases one statement text, so the later cases run on the plan the
        first case compiled, as a plan cache does.
        """

        if self.config.profile != McpProfile.SANDBOX:
            raise PermissionError("Index benchmarking requires the sandbox MCP profile.")
        clock = _CallClock(max_call_seconds)
        policy = self.database_policy.require(database_name)
        if not policy.is_non_production:
            raise PermissionError(
                "Temporary indexes need a policy environment that is a known "
                "non-production label (test, dev, sandbox, staging, bench, ...); "
                f"this entry is classed {policy.environment_class}."
            )
        if not policy.allow_test_indexes:
            raise PermissionError("Database policy does not permit temporary indexes.")
        if phase not in {"screening", "finalist"}:
            raise ValueError("phase must be screening or finalist.")
        if plan_reuse not in {"per_case", "first_case"}:
            raise ValueError("plan_reuse must be per_case or first_case.")
        name_prefix, name_tag = self._resolve_test_index_prefix(policy, index_name_prefix)
        allowed_session_statuses = {
            "created",
            "screening",
            "finalist_validation",
        }
        try:
            session = self.tuning_sessions.require_session_status(
                session_id,
                allowed=allowed_session_statuses,
            )
        except InvalidTransitionError as exc:
            raise InvalidTransitionError(
                f"{exc} Retrieve committed results with get_tuning_session; "
                "terminal sessions cannot benchmark or replay."
            ) from None
        # A read-only branching batch is one candidate here too: a rewrite into
        # branches often needs its indexes measured with it.
        normalized_sql = self.validator.validate_read_only_candidate(sql).execution_sql
        normalized_rewrite = (
            self.validator.validate_read_only_candidate(candidate_sql).execution_sql
            if candidate_sql is not None
            else None
        )
        compare_order = effective_compare_order(normalized_sql, compare_order)
        candidate = self.tuning_sessions.get_candidate(candidate_id)
        case = self.performance_store.get_performance_case(session.performance_case_id)
        # Only this database's leases gate its work: an unresolved cleanup in
        # another database must not stop index tests here.
        cleanup = await self._reconcile_leases(
            database_fingerprint=case.database_fingerprint,
            attempts=_PRE_CALL_DROP_ATTEMPTS,
        )
        if cleanup["cleanup_required"]:
            return self._leftover_refusal(cleanup, session_id=session_id, candidate_id=candidate_id)
        if candidate.session_id != session_id:
            raise ValueError("Candidate does not belong to the tuning session.")
        # Test objects an earlier call of this session left in place.
        hold = self._session_hold(case.database_fingerprint or "", session_id)
        held_entries: list[dict[str, Any]] = (
            [dict(entry) for entry in hold["metadata"]["members"]] if hold is not None else []
        )
        # A lease that holds objects lists each of them, also when it is one.
        hold_mode = keep_for_session or hold is not None
        legacy_combined_lineage = bool(
            candidate.strategy == "combined"
            and candidate.rewrite_artifact_ref
            and candidate.metadata.get("lineage")
        )
        if (
            candidate.strategy not in {"index", "rewrite_plus_index"}
            and not legacy_combined_lineage
        ):
            raise ValueError(
                "Index benchmarking requires index or rewrite_plus_index; "
                "lineage-backed combined is accepted only for legacy records."
            )
        allow_legacy_fingerprint = bool(self.config.legacy_state_server_binding)
        one_call_route = (
            "To measure a rewrite with its indexes against the original statement in "
            "one call, register the rewrite with add_tuning_candidate(strategy="
            "'rewrite_plus_index') and no artifact_ref, then pass sql = the statement "
            "of the performance case and candidate_sql = the rewrite."
        )
        if normalized_rewrite is not None:
            if candidate.strategy != "rewrite_plus_index":
                raise ValueError(
                    f"candidate_sql needs a rewrite_plus_index candidate; this candidate "
                    f"has strategy {candidate.strategy}. {one_call_route}"
                )
            if replaces is not None:
                raise ValueError("replaces tests an index on unchanged SQL; do not pass candidate_sql.")
            if not fingerprint_text_matches(
                candidate.rewrite_fingerprint,
                normalized_rewrite,
                allow_legacy=allow_legacy_fingerprint,
            ):
                raise ValueError(
                    "candidate_sql does not match the SQL this candidate was registered "
                    "with. Pass the registered rewrite unchanged, or add a new candidate."
                )
            if not fingerprint_text_matches(
                case.query_fingerprint,
                normalized_sql,
                allow_legacy=allow_legacy_fingerprint,
            ):
                raise ValueError(
                    "With candidate_sql, sql is the original statement: it must match the "
                    "performance case. The rewrite goes in candidate_sql."
                )
        elif not fingerprint_text_matches(
            candidate.rewrite_fingerprint,
            normalized_sql,
            allow_legacy=allow_legacy_fingerprint,
        ):
            raise ValueError("Index candidate SQL fingerprint does not match.")
        lineage: dict[str, Any] | None = None
        if normalized_rewrite is not None:
            # Measured against the original itself: no parent result is needed.
            pass
        elif candidate.strategy == "index":
            if not fingerprint_text_matches(
                case.query_fingerprint,
                normalized_sql,
                allow_legacy=allow_legacy_fingerprint,
            ):
                raise ValueError(
                    "Index benchmark SQL does not match the performance case: an index "
                    f"candidate measures the unchanged statement. {one_call_route}"
                )
        elif candidate.strategy == "rewrite_plus_index" and not candidate.rewrite_artifact_ref:
            raise ValueError(
                "This rewrite_plus_index candidate names no parent, so it is measured "
                "against the original statement: pass sql = the statement of the "
                "performance case and candidate_sql = the rewrite."
            )
        else:
            if phase != "finalist":
                raise ValueError(
                    "Lineage-backed candidates run only as finalist marginal "
                    "index experiments."
                )
            parent = self.tuning_sessions.get_candidate(combined_parent_id(candidate))
            parent_evidence = [
                self.performance_store.get_evidence(evidence_id)
                for evidence_id in parent.evidence_ids
            ]
            lineage = validate_combined_parent(
                candidate,
                parent,
                parent_evidence,
            )
            if dict(candidate.metadata.get("lineage") or {}) != lineage:
                raise ValueError(
                    "Lineage candidate durable metadata does not match its parent."
                )
        if not database_fingerprint_matches(
            case.database_fingerprint or "",
            database_name,
            self.config.server,
            allow_legacy=bool(self.config.legacy_state_server_binding),
        ):
            raise PermissionError("Tuning session belongs to another database.")
        index_specs = self._index_set_specs(
            schema_name,
            table_name,
            key_columns,
            include_columns,
            filter_definition,
            is_unique,
            data_compression,
            indexes,
            index_kind,
            partition_aligned,
        )
        if replaces is not None and len(index_specs) != 1:
            raise ValueError("replaces applies to a candidate of exactly one index.")
        if replaces is not None and index_specs[0]["kind"] != INDEX_KIND_ROWSTORE:
            raise ValueError(
                "replaces tests a rowstore index against the one it would replace; a "
                "nonclustered columnstore index replaces no B-tree index."
            )
        self._require_tuning_session_time(session_id)
        equivalence_preflight = await self._check_equivalence_preflight(
            database_name,
            normalized_sql,
        )
        direct_snapshot_supported = bool(
            equivalence_preflight["direct_snapshot_supported"]
        )
        if normalized_rewrite is not None:
            # Two statements are compared, so the preflight has one entry per side.
            equivalence_preflight = with_order_contract(
                {
                    "baseline": equivalence_preflight,
                    "candidate": await self._check_equivalence_preflight(
                        database_name, normalized_rewrite
                    ),
                },
                normalized_sql,
                normalized_rewrite,
            )
            direct_snapshot_supported = all(
                bool(side["direct_snapshot_supported"])
                for side in equivalence_preflight.values()
            )
        # A finalist of the one-call path compares the original with the rewrite
        # in one snapshot per case, while the index set exists.
        compare_in_call = (
            normalized_rewrite is not None
            and direct_snapshot_supported
            and phase == "finalist"
        )
        # The results of the two statements are compared in every call that
        # builds the indexes, a screening run too: the build is the cost. A gap
        # that a comparison can settle (a UNION ALL rewrite, possible ties) is
        # tested; that result is a test on today's data and the candidate
        # stays performance_only. Only a gap that makes a result unstable
        # (a clock, NEWID, an unordered row limit) leaves it uncompared.
        gap_codes: list[str] = []
        blocking_gaps: list[str] = []
        if normalized_rewrite is not None:
            gap_codes, blocking_gaps = preflight_gap_codes(equivalence_preflight)
        compare_results = normalized_rewrite is not None and not blocking_gaps
        tested_gaps = gap_codes if compare_results else []
        parent_equivalence_unproven = bool(
            lineage and lineage.get("parent_equivalence") == "unproven"
        )
        performance_only_scope = (
            not direct_snapshot_supported or parent_equivalence_unproven
        )
        # Cases may be left out or named: the values this process registered are used.
        cases = self.performance_workflows._resolve_parameter_cases(
            case.case_id,
            parameter_cases,
        )
        if len(cases) > session.parameter_case_limit:
            raise ValueError(
                f"parameter_cases exceeds the session limit of {session.parameter_case_limit}."
            )
        supplied_case_fingerprints = tuple(
            parameter_case_fingerprint(parameter_case) for parameter_case in cases
        )
        registered_case_fingerprints = set(case.parameter_case_fingerprints)
        registered_receipts = case.metadata.get("parameter_case_receipts")
        if isinstance(registered_receipts, (list, tuple)):
            for case_index, parameter_case in enumerate(cases):
                mismatch = parameter_case_mismatch(
                    parameter_case,
                    registered_receipts=registered_receipts,
                    case_index=case_index,
                )
                if mismatch is not None:
                    raise ValueError(mismatch)
        if (
            len(set(supplied_case_fingerprints)) != len(supplied_case_fingerprints)
            or any(
                fingerprint not in registered_case_fingerprints
                for fingerprint in supplied_case_fingerprints
            )
        ):
            raise ValueError(
                "Index benchmark parameter cases must be an unchanged fingerprint-v1 "
                "subset of the performance case. Matching is exact over name, "
                "values, types, and normalized weight; parameter-name and SQL-type "
                "spelling are exact, object key order is ignored, and values are "
                "not persisted. Received fingerprints: "
                f"{list(supplied_case_fingerprints)}; registered fingerprints: "
                f"{list(case.parameter_case_fingerprints)}."
            )
        if phase == "finalist" and set(supplied_case_fingerprints) != (
            registered_case_fingerprints
        ):
            raise ValueError(
                "Finalist index validation must cover all "
                f"{len(registered_case_fingerprints)} registered parameter cases; this call "
                f"names {len(supplied_case_fingerprints)}. Leave parameter_cases out to run "
                'them all, or use phase="screening" for a subset.'
            )

        def begin_candidate_phase() -> None:
            if phase == "screening":
                self.tuning_sessions.start_screening(session_id)
                return
            if self.tuning_sessions.get_session(session_id).status == "created":
                self.tuning_sessions.start_screening(session_id)
            self.tuning_sessions.mark_candidate_finalist(
                session_id,
                candidate_id,
            )

        default_runs = (
            session.screen_runs_per_candidate
            if phase == "screening"
            else session.finalist_runs_per_candidate
        )
        if runs is None:
            runs = default_runs
        elif not 1 <= runs <= default_runs:
            raise ValueError(
                f"runs must be between 1 and the {phase} limit of {default_runs}."
            )
        slow_cases = self._slow_baseline_seconds(slow_baseline_cases, cases)
        if slow_cases and candidate.strategy != "index" and normalized_rewrite is None:
            raise ValueError(
                "slow_baseline_cases applies to an index candidate on the SQL of the "
                "performance case; a lineage candidate measures a rewrite."
            )
        # A baseline too slow to repeat runs once in the session, whichever
        # benchmark tool ran it; its result digest is kept with the sample, so a
        # later index is still compared with it. Only the statement of the
        # performance case on its own plan is pooled.
        pool_allowed = (
            candidate.strategy == "index" or normalized_rewrite is not None
        ) and plan_reuse == "per_case"
        slow_pool = (
            self._session_slow_index_baselines(session_id, compare_order)
            if pool_allowed
            else {}
        )
        if held_entries:
            slow_pool = self._require_baselines_before_hold(
                session_id,
                normalized_sql,
                compare_order,
                plan_reuse,
                [str(parameter_case["name"]) for parameter_case in cases],
                held_entries,
                slow_pool,
            )
        requested_executions = sum(
            runs + (0 if str(parameter_case["name"]) in slow_pool else 1)
            if str(parameter_case["name"]) in slow_cases
            else runs
            if held_entries
            else runs * 3
            for parameter_case in cases
        ) + (2 * len(cases) if compare_results else 0)
        if not policy.can_benchmark(requested_executions):
            raise PermissionError("Database policy does not permit this benchmark count.")
        if not idempotency_key or not idempotency_key.strip():
            raise ValueError("Index benchmarking requires an idempotency key.")
        owner_fingerprint = fingerprint_json(
            {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "phase": phase,
                "idempotency_key": idempotency_key,
            }
        )
        execution_owner = f"index-execution-{owner_fingerprint[:32]}"
        index_operation_key = fingerprint_json(
            {"operation": "index-benchmark-result-key", "key": idempotency_key}
        )
        index_evidence_key = fingerprint_json(
            {"operation": "index-benchmark-evidence-key", "key": idempotency_key}
        )

        def member_operation_key(operation: str, ordinal: int) -> str:
            # The first index keeps the key a one-index request always had.
            identity: dict[str, Any] = {"operation": operation, "key": idempotency_key}
            if ordinal:
                identity["member"] = ordinal
            return fingerprint_json(identity)

        for spec in index_specs:
            spec["schema"] = self._validate_plain_identifier(spec["schema"], "schema_name")
            spec["table"] = self._validate_plain_identifier(spec["table"], "table_name")
        first_spec = index_specs[0]

        request_identity: dict[str, Any] = {
            "operation": "index-benchmark-v2",
            "session_id": session_id,
            "candidate_id": candidate_id,
            "database_name": database_name,
            "sql": normalized_sql,
            "schema": first_spec["schema"],
            "table": first_spec["table"],
            "key_columns": list(first_spec["key_columns"]),
            "include_columns": list(first_spec["include_columns"] or ()),
            "filter_definition": first_spec["filter_definition"],
            "is_unique": first_spec["is_unique"],
            "phase": phase,
            "parameter_cases": supplied_case_fingerprints,
            "runs": runs,
            "online": online,
            "compare_order": compare_order,
            "lease_minutes": lease_minutes,
            "equivalence_preflight": equivalence_preflight,
            "lineage": lineage,
        }
        # Added only when used, so a one-index request keeps its fingerprint.
        any_columnstore = any(spec["kind"] != INDEX_KIND_ROWSTORE for spec in index_specs)
        any_non_aligned = any(not spec["partition_aligned"] for spec in index_specs)
        if (
            len(index_specs) > 1
            or first_spec["data_compression"]
            or any_columnstore
            or any_non_aligned
        ):
            request_identity["index_set"] = [
                {
                    "schema": spec["schema"],
                    "table": spec["table"],
                    "key_columns": list(spec["key_columns"]),
                    "include_columns": list(spec["include_columns"] or ()),
                    "filter_definition": spec["filter_definition"],
                    "is_unique": spec["is_unique"],
                    "data_compression": spec["data_compression"],
                    **({"kind": spec["kind"]} if any_columnstore else {}),
                    **(
                        {"partition_aligned": spec["partition_aligned"]}
                        if any_non_aligned
                        else {}
                    ),
                }
                for spec in index_specs
            ]
        if name_prefix != TEST_INDEX_PREFIX:
            request_identity["index_name_prefix"] = name_prefix
        if name_tag:
            request_identity["index_name_tag"] = name_tag
        if replaces is not None:
            request_identity["replaces"] = replaces
        if slow_cases:
            request_identity["slow_baseline_cases"] = dict(slow_cases)
        if normalized_rewrite is not None:
            request_identity["candidate_sql"] = normalized_rewrite
        if plan_reuse != "per_case":
            request_identity["plan_reuse"] = plan_reuse
        if keep_for_session:
            request_identity["keep_for_session"] = True
        request_fingerprint = fingerprint_json(request_identity)

        def plan_identity(parameter_case: Mapping[str, Any]) -> str:
            # One text for all cases: the first case compiles, the others reuse.
            return str(parameter_case["name"]) if plan_reuse == "per_case" else "shared-plan"

        # Request-validation reads and refusals run before the request binds
        # the candidate; else the corrected request is refused as a replay.
        # The budget reservation runs after the bind, so its refusal releases
        # a fresh binding.
        self._require_tuning_session_time(session_id)
        bound_cases = [
            {
                "name": parameter_case["name"],
                "parameter_case_fingerprint": parameter_case_fingerprint(
                    parameter_case
                ),
                "weight": parameter_case["weight"],
                # The marker gives the benchmark its own plan cache and Query
                # Store entry: it never reuses or replaces the application's plan.
                "execution_contract": await self.performance_workflows._bind_case(
                    database_name,
                    mark_benchmark_sql(
                        normalized_sql,
                        session_id,
                        "index",
                        candidate_id,
                        plan_identity(parameter_case),
                    ),
                    parameter_case,
                ),
            }
            for parameter_case in cases
        ]
        self._require_tuning_session_time(session_id)
        canonical_tables: dict[tuple[str, str], _TestIndexTable] = {}
        not_partitioned: list[str] = []
        members: list[_IndexSetMember] = []
        held_indexes = {
            str(entry["object_fingerprint"]): entry
            for entry in held_entries
            if entry.get("object_kind") != LEASE_OBJECT_KIND
        }
        for ordinal, spec in enumerate(index_specs):
            spelled = (spec["schema"], spec["table"])
            if spelled not in canonical_tables:
                canonical_tables[spelled] = await self._resolve_canonical_table_identity(
                    database_name, *spelled
                )
            table_facts = canonical_tables[spelled]
            member_schema, member_table = table_facts.schema, table_facts.table
            if not spec["partition_aligned"]:
                if not table_facts.partitioned:
                    not_partitioned.append(f"{member_schema}.{member_table}")
                elif spec["kind"] == INDEX_KIND_NONCLUSTERED_COLUMNSTORE:
                    raise ValueError(
                        f"{member_schema}.{member_table} is partitioned. A columnstore index "
                        "on a partitioned table must be partition-aligned: the engine "
                        "refuses ON [PRIMARY] with error 35316. Leave partition_aligned "
                        "out. No query and no DDL was run."
                    )
                elif table_facts.has_clustered_columnstore_index:
                    raise ValueError(
                        f"{member_schema}.{member_table} is partitioned and has a clustered "
                        "columnstore index. A nonclustered index on such a table must be "
                        "partition-aligned: this is an expected engine refusal of "
                        "CREATE INDEX ... ON [PRIMARY]. Leave partition_aligned out to "
                        "test the aligned index. No query and no DDL was run."
                    )
                else:
                    # One B-tree on the only filegroup; the built index is
                    # verified against this placement, not the table's scheme.
                    table_facts = table_facts.non_aligned()
            # Refused here, before the request binds and before any baseline
            # run, when the engine needs an offline build the policy forbids.
            member_online = _test_index_build_online(policy, online, table_facts)
            provisional_index = IndexCandidate(
                schema=member_schema,
                table=member_table,
                key_columns=tuple(spec["key_columns"]),
                include_columns=tuple(spec["include_columns"] or ()),
                filter_definition=spec["filter_definition"],
                is_unique=spec["is_unique"],
                compression=spec["data_compression"],
                **table_facts.placement(),
                index_kind=spec["kind"],
            )
            held_index = held_indexes.get(provisional_index.definition_fingerprint)
            if spec["kind"] == INDEX_KIND_NONCLUSTERED_COLUMNSTORE and held_index is None:
                # Refused here, before the request binds and before any run: one
                # columnstore index per table, and only columns it can hold.
                blockers = columnstore_index_blockers(
                    provisional_index,
                    existing_indexes=await collect_existing_indexes(
                        self.executor,
                        database_name,
                        schema_name=member_schema,
                        table_name=member_table,
                    ),
                    table_columns=await collect_table_column_facts(
                        self.executor,
                        database_name,
                        schema_name=member_schema,
                        table_name=member_table,
                    ),
                )
                if blockers:
                    raise ValueError(" ".join(blockers) + " No query and no DDL was run.")
            # The caller chooses the prefix at most; the rest of the name is the
            # definition, so two requests can never fight over one name.
            member_candidate = replace(
                provisional_index,
                # A held index keeps the name it was built with.
                index_name=str(held_index["index_name"])
                if held_index is not None
                else generated_test_index_name(name_prefix, provisional_index, name_tag),
            )
            members.append(
                _IndexSetMember(
                    schema=member_schema,
                    table=member_table,
                    key_columns=spec["key_columns"],
                    include_columns=spec["include_columns"],
                    filter_definition=spec["filter_definition"],
                    is_unique=spec["is_unique"],
                    data_compression=spec["data_compression"],
                    candidate=member_candidate,
                    index_ddl=build_test_index_statement(
                        member_candidate,
                        online=member_online,
                        allow_offline=policy.allow_offline_test_index,
                        maxdop=policy.test_index_maxdop or None,
                    ),
                    rollback_ddl=(
                        f"DROP INDEX [{member_candidate.index_name}] "
                        f"ON [{member_schema}].[{member_table}];"
                    ),
                    create_key=member_operation_key("index-benchmark-create-key", ordinal),
                    cleanup_key=member_operation_key("index-benchmark-cleanup-key", ordinal),
                    table_facts=table_facts,
                    online=member_online,
                    reused=held_index is not None,
                )
            )
        if len({member.index_name for member in members}) != len(members):
            raise ValueError("indexes holds the same index definition twice.")
        # Said in every result from here on: the name is not the one asked for.
        name_note: dict[str, str] = {}
        if not_partitioned:
            name_note["partition_aligned_note"] = (
                ", ".join(dict.fromkeys(not_partitioned))
                + " is not partitioned, so partition_aligned was ignored: the index is "
                "built as on any table."
            )
        if index_name_prefix is not None and not extends_test_index_prefix(
            index_name_prefix, self._allowed_test_index_prefixes(policy)
        ):
            name_note["index_name_note"] = (
                "The requested index_name_prefix is not in test_index_name_prefixes ("
                + ", ".join(self._allowed_test_index_prefixes(policy))
                + f") and does not extend one of them. The prefix {name_prefix} was used: "
                + ", ".join(member.index_name for member in members)
                + "."
            )
        is_set = len(members) > 1
        lease_as_set = is_set or hold_mode
        reused_fingerprints = {member.fingerprint for member in members if member.reused}
        # Held objects this call does not use: they stay under the lease.
        carried_entries = [
            entry
            for entry in held_entries
            if entry.get("object_kind") == LEASE_OBJECT_KIND
            or entry["object_fingerprint"] not in reused_fingerprints
        ]
        held_index_names = {str(entry.get("index_name")) for entry in held_entries}
        tables = list(dict.fromkeys((member.schema, member.table) for member in members))
        # A one-index lease keeps the identity it always had.
        object_fingerprint = (
            fingerprint_json({"index_set": [member.fingerprint for member in members]})
            if is_set
            else members[0].fingerprint
        )
        index_ddl = "\n".join(member.index_ddl for member in members)
        rollback_ddl = "\n".join(member.rollback_ddl for member in reversed(members))
        # A replacement test hints the new index in the candidate arm, so the
        # index it replaces cannot be chosen there.
        replacement: ReplacementExperiment | None = None
        if replaces is not None:
            self._require_tuning_session_time(session_id)
            replaced_index = next(
                (
                    existing
                    for existing in await collect_existing_indexes(
                        self.executor,
                        database_name,
                        schema_name=members[0].schema,
                        table_name=members[0].table,
                    )
                    if existing.schema == members[0].schema
                    and existing.table == members[0].table
                    and existing.name.casefold() == replaces.casefold()
                ),
                None,
            )
            if replaced_index is None:
                raise ValueError(
                    "replaces must name an existing index of "
                    f"{members[0].schema}.{members[0].table}."
                )
            replacement = plan_replacement_experiment(
                members[0].candidate, [replaced_index], hide_method="table_hint"
            )
            hinted_sql = hint_candidate_index(normalized_sql, members[0].candidate)
            for bound_case, parameter_case in zip(bound_cases, cases):
                bound_case["candidate_contract"] = await self.performance_workflows._bind_case(
                    database_name,
                    mark_benchmark_sql(
                        hinted_sql,
                        session_id,
                        "index-hinted",
                        candidate_id,
                        plan_identity(parameter_case),
                    ),
                    parameter_case,
                )
        if normalized_rewrite is not None:
            for bound_case, parameter_case in zip(bound_cases, cases):
                bound_case["candidate_contract"] = await self.performance_workflows._bind_case(
                    database_name,
                    mark_benchmark_sql(
                        normalized_rewrite,
                        session_id,
                        "index-rewrite",
                        candidate_id,
                        plan_identity(parameter_case),
                    ),
                    parameter_case,
                )
        hinted = replacement is not None
        if hinted:
            # A hint shows what the index costs when used, not that it is chosen.
            performance_only_scope = True
        proof_scope_label = (
            "hinted_evidence"
            if hinted
            else "performance_only"
            if performance_only_scope
            else "direct_snapshot"
            if normalized_rewrite is not None
            else "aba_result_stability"
        )
        self._require_tuning_session_time(session_id)
        self.performance_store.bind_index_benchmark_request(
            session_id,
            candidate_id,
            phase,
            request_fingerprint,
            idempotency_key=idempotency_key,
        )
        existing_reservation = (
            self.performance_store.get_idempotent_execution_reservation(
                session_id,
                candidate_id,
                request_fingerprint,
                idempotency_key=idempotency_key,
                owner_reference=execution_owner,
            )
        )
        if existing_reservation is None and candidate.is_terminal:
            if candidate.executions == 0:
                return {
                    "session_id": session_id,
                    "candidate_id": candidate_id,
                    **name_note,
                    "classification": candidate.state,
                    "durable_state": candidate.state,
                    "reason": (
                        "Replayed the durable zero-execution candidate result; "
                        "no DDL or benchmark query was dispatched."
                    ),
                    "executions": 0,
                    "session_continues": candidate.state != "cleanup_required",
                    "replayed_zero_execution_result": True,
                }
            raise ValueError(
                f"Index candidate already has a terminal benchmark result ({candidate.state}); "
                "a candidate is measured once. For a further run (plan_reuse='first_case', "
                "other parameter cases), register it again with add_tuning_candidate (the "
                f"same SQL, strategy='{candidate.strategy}') and call benchmark_index_candidate "
                "on the new candidate_id. With keep_for_session=true on the first call the "
                "held index is reused, so nothing is built twice."
            )
        if (
            existing_reservation is None
            and not candidate.is_terminal
            and phase == "screening"
            and candidate.screen_runs
        ):
            raise ValueError("Index candidate screening has already been measured.")
        if (
            existing_reservation is None
            and not candidate.is_terminal
            and phase == "finalist"
            and candidate.finalist_runs
        ):
            raise ValueError("Index candidate finalist validation has already been measured.")

        if existing_reservation is not None:
            recovered = self._recover_index_benchmark_result(
                session_id=session_id,
                candidate_id=candidate_id,
                phase=phase,
                runs=runs,
                parameter_case_count=len(cases),
                reservation=existing_reservation,
                reservation_owner=execution_owner,
                request_fingerprint=request_fingerprint,
                evidence_operation_key=index_evidence_key,
                benchmark_operation_key=index_operation_key,
                index_ddl=index_ddl,
                rollback_ddl=rollback_ddl,
                index_definition_fingerprint=object_fingerprint,
            )
            return {**recovered, **name_note}

        def member_index(indexes: Sequence[Any], member: _IndexSetMember) -> Any:
            return next(
                (
                    existing
                    for existing in indexes
                    if existing.schema == member.schema
                    and existing.table == member.table
                    and existing.name == member.index_name
                ),
                None,
            )

        def covers_member(existing: Any, member: _IndexSetMember) -> bool:
            if replaces is not None and existing.name.casefold() == replaces.casefold():
                return False
            if member.candidate.index_kind == INDEX_KIND_NONCLUSTERED_COLUMNSTORE:
                # A B-tree on the same columns does not do what a columnstore index does.
                return False
            if not existing_index_covers_candidate(
                existing,
                schema=member.schema,
                table=member.table,
                key_columns=member.key_columns,
                include_columns=member.include_columns or (),
                filter_definition=member.filter_definition,
                is_unique=member.is_unique,
                test_index_name_prefixes=policy.test_index_name_prefixes,
            ):
                return False
            if member.data_compression is None:
                return True
            # The same columns under another compression are another test: the
            # depth and size of the index are what the candidate changes.
            return bool(existing.partition_compression) and all(
                str(compression).strip().upper() == member.data_compression
                for _partition, compression in existing.partition_compression
            )

        self._require_tuning_session_time(session_id)
        existing_indexes: list[Any] = []
        for table_schema, table_only in tables:
            existing_indexes.extend(
                await collect_existing_indexes(
                    self.executor,
                    database_name,
                    schema_name=table_schema,
                    table_name=table_only,
                )
            )
        # A test index that a closed lease of this server built and that is
        # still in the catalog is ours by its owner marker: it is dropped, not
        # reported as an index nobody owns.
        leftovers_dropped = await self._drop_owned_leftover_indexes(
            database_name,
            case.database_fingerprint or "",
            [
                existing
                for existing in existing_indexes
                if existing.name not in held_index_names
                and _is_generated_test_index_name(
                    existing.name, self._allowed_test_index_prefixes(policy)
                )
            ],
        )
        if leftovers_dropped:
            gone = {str(item["index_name"]) for item in leftovers_dropped}
            existing_indexes = [item for item in existing_indexes if item.name not in gone]
        name_conflict = next(
            (
                existing
                for member in members
                if not member.reused
                and (existing := member_index(existing_indexes, member)) is not None
            ),
            None,
        )
        if name_conflict is not None:
            if not candidate.is_terminal:
                begin_candidate_phase()
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="inconclusive",
                parameter_cases=len(cases),
                executions=0,
                failure_code="name_conflict",
                idempotency_key=index_operation_key,
            )
            return {
                "session_id": session_id,
                "candidate_id": candidate_id,
                **name_note,
                "classification": "inconclusive",
                "durable_state": updated.state,
                "reason": (
                    "The generated temporary index name already exists with an "
                    "unowned observed definition."
                ),
                "failure_code": "name_conflict",
                "existing_index": name_conflict.as_dict(),
                "executions": 0,
                "index_ddl": index_ddl,
                "rollback_ddl": rollback_ddl,
                "session_continues": True,
            }
        member_names = {member.index_name for member in members}
        generated_prefixes = self._allowed_test_index_prefixes(policy)
        leftovers = sorted(
            (existing.name, existing.schema, existing.table)
            for existing in existing_indexes
            if (existing.schema, existing.table) in tables
            and existing.name not in member_names
            and existing.name not in held_index_names
            and _is_generated_test_index_name(existing.name, generated_prefixes)
        )
        leftover_test_indexes = [name for name, _schema, _table in leftovers]
        if leftover_test_indexes:
            # No lease of this database is open here, so these belong to a lost
            # run. They change the baseline plan; measuring against them is wrong.
            # Nothing durable is recorded: the same request works after cleanup.
            return {
                "session_id": session_id,
                "candidate_id": candidate_id,
                **name_note,
                "classification": "cleanup_required",
                "reason": (
                    "The target table holds test indexes that no open lease owns; "
                    "no DDL or benchmark query was dispatched."
                ),
                "failure_code": "leftover_test_index",
                "leftover_test_indexes": leftover_test_indexes,
                "remediation": "\n".join(
                    f"DROP INDEX [{name}] ON [{leftover_schema}].[{leftover_table}];"
                    for name, leftover_schema, leftover_table in leftovers
                ),
                "executions": 0,
                "index_ddl": index_ddl,
                "rollback_ddl": rollback_ddl,
                "session_continues": True,
            }
        covered_members = [
            member
            for member in members
            if any(covers_member(existing, member) for existing in existing_indexes)
        ]
        covering_indexes = [
            existing
            for existing in existing_indexes
            if any(covers_member(existing, member) for member in members)
        ]
        if covered_members and len(covered_members) < len(members):
            # The set would measure an index the table already has. Dropping it
            # from the set is a new request; nothing ran for this one.
            if not candidate.is_terminal:
                begin_candidate_phase()
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="inconclusive",
                parameter_cases=len(cases),
                executions=0,
                failure_code="index_set_partly_covered",
                idempotency_key=index_operation_key,
            )
            return {
                "session_id": session_id,
                "candidate_id": candidate_id,
                **name_note,
                "classification": "inconclusive",
                "durable_state": updated.state,
                "reason": (
                    "An existing enabled index already covers part of the set: "
                    + ", ".join(
                        f"{member.schema}.{member.table} ({', '.join(member.key_columns)})"
                        for member in covered_members
                    )
                    + ". Add a candidate with only the indexes the table lacks."
                ),
                "failure_code": "index_set_partly_covered",
                "existing_indexes": [index.as_dict() for index in covering_indexes],
                "executions": 0,
                "index_ddl": index_ddl,
                "rollback_ddl": rollback_ddl,
                "session_continues": True,
            }
        if covering_indexes:
            if not candidate.is_terminal:
                begin_candidate_phase()
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="neutral",
                parameter_cases=len(cases),
                executions=0,
                failure_code=None,
                idempotency_key=index_operation_key,
            )
            return {
                "session_id": session_id,
                "candidate_id": candidate_id,
                **name_note,
                "classification": "neutral",
                "durable_state": updated.state,
                "reason": "an existing enabled index already covers this candidate",
                "existing_indexes": [index.as_dict() for index in covering_indexes],
                "executions": 0,
                "index_ddl": index_ddl,
                "rollback_ddl": rollback_ddl,
                "session_continues": True,
            }

        self._require_tuning_session_time(session_id)
        try:
            execution_reservation = self.performance_store.reserve_execution_attempts(
                session_id,
                candidate_id,
                requested_executions,
                request_fingerprint,
                idempotency_key=idempotency_key,
                owner_reference=execution_owner,
            )
        except ReservationError:
            # Also for a replayed binding: an earlier attempt may have bound the
            # request and failed before reserving. The store keeps any binding
            # that holds a reservation, so only an unreserved one is freed.
            self.performance_store.release_index_benchmark_request(
                session_id,
                candidate_id,
                phase,
                request_fingerprint,
                idempotency_key=idempotency_key,
            )
            raise
        if (
            execution_reservation["status"] != "reserved"
            or execution_reservation.get("replayed") is True
        ):
            recovered = self._recover_index_benchmark_result(
                session_id=session_id,
                candidate_id=candidate_id,
                phase=phase,
                runs=runs,
                parameter_case_count=len(cases),
                reservation=execution_reservation,
                reservation_owner=execution_owner,
                request_fingerprint=request_fingerprint,
                evidence_operation_key=index_evidence_key,
                benchmark_operation_key=index_operation_key,
                index_ddl=index_ddl,
                rollback_ddl=rollback_ddl,
                index_definition_fingerprint=object_fingerprint,
            )
            return {**recovered, **name_note}
        lease_id = (
            f"lease-{fingerprint_json({'session': session_id, 'candidate': candidate_id, 'key': idempotency_key})[:32]}"
        )
        lease_owner = f"index-lease-{owner_fingerprint[:32]}"
        if hold is not None:
            # The session has one lease; this call works under it and its owner token.
            lease_id = str(hold["lease_id"])
            lease_owner = str(hold["metadata"]["lease_owner_fence"])
        lease: dict[str, Any] | None = None
        pre_dispatch_reservation_finalized = False

        def finalize_pre_dispatch_reservation() -> None:
            nonlocal pre_dispatch_reservation_finalized
            if pre_dispatch_reservation_finalized:
                return
            self.performance_store.release_execution_attempts(
                execution_reservation["reservation_id"],
                owner_reference=execution_owner,
                expected_version=execution_reservation["version"],
            )
            pre_dispatch_reservation_finalized = True

        try:
            session_remaining_seconds = self._require_tuning_session_time(session_id)
            effective_lease_minutes = lease_minutes
            if math.isfinite(session_remaining_seconds):
                effective_lease_minutes = max(
                    lease_minutes,
                    int(session_remaining_seconds / 60) + 2,
                )
            expires_at = datetime.now(timezone.utc) + timedelta(
                minutes=effective_lease_minutes
            )
            for member in members:
                if member.reused:
                    member.state = dict(held_indexes[member.fingerprint])
                    member.ownership_recorded = True
                    member.create_outcome_settled = True
                    continue
                member.state = {
                    "target_schema": member.schema,
                    "target_table": member.table,
                    "key_columns": list(member.key_columns),
                    "include_columns": list(member.include_columns or ()),
                    "filter_definition": member.filter_definition,
                    "is_unique": member.is_unique,
                    "definition_fingerprint": member.fingerprint,
                    "marker_definition_fingerprint": member.fingerprint,
                    "fingerprint_provenance": "candidate_intent",
                    "create_dispatch_state": "pre_dispatch",
                }
                if member.data_compression:
                    member.state["data_compression"] = member.data_compression
                if member.candidate.index_kind != INDEX_KIND_ROWSTORE:
                    # Ownership recovery rebuilds the definition from these fields.
                    member.state["index_kind"] = member.candidate.index_kind
                if member.table_facts.data_space is not None:
                    # Not partition-aligned. Part of the definition fingerprint;
                    # ownership recovery rebuilds the request from these fields.
                    member.state["partition_aligned"] = False
                    member.state["data_space"] = member.table_facts.data_space
                elif member.table_facts.placement():
                    # Part of the definition fingerprint; ownership recovery
                    # rebuilds the request from these fields.
                    member.state["partition_scheme"] = member.table_facts.partition_scheme
                    member.state["partition_function"] = member.table_facts.partition_function
                    member.state["partition_column"] = member.table_facts.partition_column
                if lease_as_set:
                    member.state["index_name"] = member.index_name
                    member.state["object_fingerprint"] = member.fingerprint
            if hold is not None:
                lease = self.performance_store.update_index_lease(
                    lease_id,
                    status="active",
                    metadata={
                        "hold_state": "in_call",
                        "members": [*carried_entries, *(dict(member.state) for member in members)],
                    },
                    owner_reference=lease_owner,
                    expected_version=hold["version"],
                    # As for a new lease: the call is covered to the end of the session.
                    expires_at_utc=expires_at.isoformat(),
                )
            lease = lease or self.performance_store.create_index_lease(
                lease_id=lease_id,
                database_fingerprint=case.database_fingerprint or "",
                session_id=session_id,
                candidate_id=candidate_id,
                index_name=members[0].index_name,
                object_fingerprint=object_fingerprint,
                expires_at_utc=expires_at.isoformat(),
                metadata={
                    "phase": phase,
                    "requested_lease_minutes": lease_minutes,
                    "protected_lease_minutes": effective_lease_minutes,
                    "lease_owner_fence": lease_owner,
                    "sql_persisted": False,
                    # One lease holds the whole set. A lease of one index keeps
                    # its fields at the top level; a set lists them per index.
                    **(
                        {"members": [dict(member.state) for member in members]}
                        if lease_as_set
                        else members[0].state
                    ),
                    "equivalence_preflight": equivalence_preflight,
                    "lineage": lineage,
                },
                owner_reference=lease_owner,
                request_fingerprint=fingerprint_json(
                    {
                        "lease_id": lease_id,
                        "definition": object_fingerprint,
                        "key": idempotency_key,
                    }
                ),
            )
            if hold is None and lease["status"] != "pending_create":
                finalize_pre_dispatch_reservation()
                return {
                    "session_id": session_id,
                    "candidate_id": candidate_id,
                    **name_note,
                    "classification": (
                        "cleanup_required"
                        if lease["status"] == "cleanup_required"
                        else "inconclusive"
                    ),
                    "reason": (
                        "This idempotent index lease already has durable state. "
                        "Retrieve or reconcile that lease; no DDL or query was rerun."
                    ),
                    "failure_code": "index_lease_already_exists",
                    "executions": 0,
                    "lease": {
                        "lease_id": lease["lease_id"],
                        "status": lease["status"],
                        "version": lease["version"],
                    },
                    "session_continues": lease["status"] != "cleanup_required",
                }
            begin_candidate_phase()
        except BaseException:
            finalize_pre_dispatch_reservation()
            if lease is not None and hold is None:
                self.performance_store.update_index_lease(
                    lease_id,
                    status="create_failed",
                    metadata={"failure_type": "workflow_transition_failed"},
                    owner_reference=lease_owner,
                    expected_version=lease["version"],
                )
            elif hold is not None:
                self._return_hold_to_idle(lease_id, lease_owner, lease_minutes)
            raise

        measured_executions = 0
        measurements: list[dict[str, Any]] = [
            {
                "parameter_case": bound_case["name"],
                "weight": bound_case["weight"],
                "execution_contract": bound_case["execution_contract"],
                "candidate_contract": bound_case.get(
                    "candidate_contract", bound_case["execution_contract"]
                ),
                # Declared slow, or the session already holds its one run.
                "slow_seconds": slow_cases.get(
                    str(bound_case["name"]),
                    _sample_seconds(slow_pool[str(bound_case["name"])]["sample"])
                    if str(bound_case["name"]) in slow_pool
                    else None,
                ),
                "slow_auto": False,
                "pooled_baseline": slow_pool.get(str(bound_case["name"])),
                # Runs of a baseline of seconds that wait for the first candidate run.
                "baseline_runs_after": None,
                # True when one run of each side decided (decisive_first_runs).
                "decisive": False,
                "runs": {"baseline_before": 0, "candidate": 0, "baseline_after": 0},
                "baseline_before_samples": [],
                "baseline_after_samples": [],
                "candidate_samples": [],
                "baseline_before_results": [],
                "baseline_after_results": [],
                "candidate_results": [],
                "baseline_before_plan": {},
                "baseline_after_plan": {},
                "candidate_plan": {},
                "baseline_before_profile": None,
                "baseline_after_profile": None,
                "candidate_profile": None,
                "plan_use": [],
            }
            for bound_case in bound_cases
        ]
        cleanup_error: str | None = None
        cleanup_failure: dict[str, Any] | None = None
        create_failure: dict[str, Any] | None = None
        # The step a failure diagnostic names, and the index being built then.
        step = "baseline_before"
        building: _IndexSetMember | None = None
        definition_mismatch = False
        benchmark_error: str | None = None
        benchmark_failure_diagnostic: dict[str, Any] | None = None
        terminal_error: InvalidTransitionError | None = None
        cancelled = False
        equivalence: list[dict[str, Any]] = []
        build_estimate: dict[str, Any] | None = None
        build_retries: list[dict[str, Any]] = []

        def lease_entries() -> list[dict[str, Any]]:
            """What the lease lists: the carried objects, then the indexes of this call."""
            return [
                *carried_entries,
                *(dict(item.state) for item in members if not item.released),
            ]

        def update_lease(
            status: str,
            metadata: Mapping[str, Any] | None = None,
            member: _IndexSetMember | None = None,
            expires_at_utc: str | None = None,
        ) -> None:
            """Advance the one lease of the set; a fact about one index goes to its entry."""
            nonlocal lease
            if lease is None:
                raise RuntimeError("Temporary index lease was not created.")
            payload: dict[str, Any] | None = dict(metadata) if metadata is not None else None
            if member is not None and metadata is not None:
                member.state.update(metadata)
            if lease_as_set:
                if member is not None:
                    payload = None
                payload = {**(payload or {}), "members": lease_entries()}
            lease = self.performance_store.update_index_lease(
                lease_id,
                status=status,
                metadata=payload,
                owner_reference=lease_owner,
                expected_version=lease["version"],
                expires_at_utc=expires_at_utc,
            )

        def objects_in_place() -> bool:
            """True when the lease would still hold an object that exists."""
            return bool(carried_entries) or any(
                item.ownership_recorded and not item.released for item in members
            )

        def hold_lease() -> None:
            """Leave the lease open with the objects that exist, bounded from now."""
            held_at = datetime.now(timezone.utc)
            for item in members:
                if item.ownership_recorded and not item.released:
                    item.state.setdefault("held_since_utc", held_at.isoformat())
                else:
                    # Never built, or settled as not there: the hold does not list it.
                    item.released = True
            update_lease(
                "active",
                {HOLD_FLAG: True, "hold_state": "idle", "hold_minutes": lease_minutes},
                expires_at_utc=(held_at + timedelta(minutes=lease_minutes)).isoformat(),
            )

        async def record_observed_index(
            member: _IndexSetMember,
            observed_index: Any,
            *,
            observation: str,
        ) -> bool:
            if observed_index is None or not expected_index_definition_matches(
                observed_index,
                member.candidate,
            ):
                return False
            if lease is None:
                raise RuntimeError("Temporary index lease was not created.")
            await self._verify_pending_index_ownership(
                database_name,
                next(
                    view
                    for view in self._index_lease_members(lease)
                    if view["index_name"] == member.index_name
                ),
                observed_index,
                lease_owner,
            )
            # The lease is active only when every index of the set is owned.
            update_lease(
                "active"
                if all(other.ownership_recorded for other in members if other is not member)
                else "pending_create",
                {
                    "created_at_utc": datetime.now(timezone.utc).isoformat(),
                    "observed_definition_fingerprint": (
                        observed_index.definition_fingerprint
                    ),
                    "fingerprint_provenance": "observed_existing_index",
                    "creation_observation": observation,
                },
                member,
            )
            member.ownership_recorded = True
            return True

        async def drop_index_proven_by_owner_marker(
            member: _IndexSetMember,
            observed_index: Any,
        ) -> None:
            """Drop a created index whose catalog definition differs from the candidate."""

            if lease is None:
                return
            marker_matched = False
            try:
                marker_matched = await self._index_owner_marker_matches(
                    database_name,
                    member.schema,
                    member.table,
                    member.index_name,
                    ownership_proof=lease_owner,
                    definition_fingerprint=member.fingerprint,
                    index_id=observed_index.index_id,
                )
                if not marker_matched:
                    return
                # The guarded DROP rechecks both markers and the index_id
                # inside its own SERIALIZABLE transaction.
                await self._drop_test_index(
                    database_name,
                    member.schema,
                    member.table,
                    member.index_name,
                    dry_run=False,
                    workflow_managed=True,
                    idempotency_key=member.cleanup_key,
                    ownership_proof=lease_owner,
                    expected_definition_fingerprint=member.fingerprint,
                    expected_index_id=observed_index.index_id,
                )
                if member_index(
                    await collect_existing_indexes(
                        self.executor,
                        database_name,
                        schema_name=member.schema,
                        table_name=member.table,
                    ),
                    member,
                ) is not None:
                    raise RuntimeError("Temporary index removal was not verified.")
                # Indexes of the set that are still owned are dropped by the
                # cleanup below; the lease closes there.
                update_lease(
                    str(lease["status"])
                    if carried_entries or any(other.ownership_recorded for other in members)
                    else "cleaned",
                    {
                        "cleaned_at_utc": datetime.now(timezone.utc).isoformat(),
                        "creation_observation": "post_create_definition_mismatch",
                        "ownership_proof": "owner_marker",
                        "cleanup_policy": "dropped_by_owner_marker",
                    },
                    member,
                )
                member.create_outcome_settled = True
            except Exception:
                logger.exception(
                    "Unable to drop a created index proven by its owner marker",
                    extra={"lease_id": lease_id},
                )
                if not marker_matched:
                    return
                try:
                    update_lease(
                        "cleanup_required",
                        {
                            "ownership_status": "owner_marker_matched",
                            "creation_observation": "post_create_definition_mismatch",
                            "ownership_proof": "owner_marker",
                            "cleanup_policy": "retry_only_with_observed_ownership",
                        },
                        member,
                    )
                    member.create_outcome_settled = True
                    member.left_behind = True
                except Exception:
                    logger.exception(
                        "Unable to record temporary index ownership",
                        extra={"lease_id": lease_id},
                    )

        async def reconcile_create_outcome() -> bool:
            """Settle an uncertain CREATE; True when a cancel hit the probe."""
            # The create loop stops at its first failure, so at most one index
            # of the set has an unknown outcome.
            member = next(
                (
                    item
                    for item in members
                    if item.create_attempted
                    and not item.ownership_recorded
                    and not item.create_outcome_settled
                ),
                None,
            )
            if member is None or lease is None:
                return False
            # The CREATE batch commits the ownership markers with the index, so a
            # visible index whose markers match is ours even when the reply was lost.
            cancelled_during_probe = False
            # None: the catalog could not be read.
            index_in_catalog: bool | None = None
            try:
                observed_index = member_index(
                    await collect_existing_indexes(
                        self.executor,
                        database_name,
                        schema_name=member.schema,
                        table_name=member.table,
                    ),
                    member,
                )
                index_in_catalog = observed_index is not None
                if await record_observed_index(
                    member,
                    observed_index,
                    observation="reconciled_after_create_dispatch",
                ):
                    return False
            except asyncio.CancelledError:
                # Record the uncertain create first, so the lease stays open.
                cancelled_during_probe = True
            except Exception:
                logger.warning(
                    "Temporary index ownership could not be proven after CREATE",
                    extra={"lease_id": lease_id},
                    exc_info=True,
                )
            if create_failure is not None:
                create_failure["index_in_catalog"] = index_in_catalog
                if index_in_catalog is False and create_failure["reason"] != "connection_or_timeout":
                    # The engine reported the error, so the batch has ended, and
                    # the catalog holds no such index: nothing is left to clean.
                    # After a lost connection or a timeout the build may still
                    # run on the server, so that lease stays open.
                    member.create_outcome_settled = True
                    return False
            update_lease(
                "cleanup_required",
                {
                    "ownership_status": "uncertain_after_create_dispatch",
                    "ownership_proof": "none",
                    "cleanup_policy": "do_not_adopt_or_drop",
                },
                member,
            )
            member.left_behind = True
            # The caller sets `cancelled`; the durable tail re-raises it.
            return cancelled_during_probe

        async def drop_owned_member(member: _IndexSetMember) -> None:
            """Drop one index this run created and recorded, and verify it is gone."""
            current_index = member_index(
                await collect_existing_indexes(
                    self.executor,
                    database_name,
                    schema_name=member.schema,
                    table_name=member.table,
                ),
                member,
            )
            if current_index is not None:
                observed_fingerprint = member.state.get("observed_definition_fingerprint")
                verification_fingerprint = (
                    observed_fingerprint
                    if isinstance(observed_fingerprint, str) and observed_fingerprint
                    else member.fingerprint
                )
                owned_definition = (
                    current_index.definition_fingerprint == verification_fingerprint
                    and expected_index_definition_matches(
                        current_index,
                        member.candidate,
                    )
                )
                if not owned_definition:
                    raise RuntimeError(
                        "Temporary index name now identifies an unowned definition."
                    )
                await self._drop_test_index(
                    database_name,
                    member.schema,
                    member.table,
                    member.index_name,
                    dry_run=False,
                    workflow_managed=True,
                    idempotency_key=member.cleanup_key,
                    ownership_proof=lease_owner,
                    expected_definition_fingerprint=member.fingerprint,
                    expected_index_id=current_index.index_id,
                )
            if member_index(
                await collect_existing_indexes(
                    self.executor,
                    database_name,
                    schema_name=member.schema,
                    table_name=member.table,
                ),
                member,
            ) is not None:
                raise RuntimeError("Temporary index removal was not verified.")
            if lease_as_set:
                member.state["cleaned_at_utc"] = datetime.now(timezone.utc).isoformat()
            # A lease that holds objects lists only what still exists.
            member.released = hold_mode

        async def measure(measurement: dict[str, Any], phase_name: str) -> Any:
            """One charged execution of a case, kept under ``phase_name``."""
            nonlocal measured_executions
            self._require_tuning_session_time(session_id)
            clock.require("candidate_measure" if phase_name == "candidate" else phase_name)
            measured_executions += 1
            run_started = clock.start()
            try:
                profiled = await self.performance_workflows._profile_execution_contract(
                    database_name,
                    measurement[
                        "candidate_contract" if phase_name == "candidate" else "execution_contract"
                    ],
                    max_result_rows=self.config.comparison_row_limit,
                )
            finally:
                clock.add(
                    "candidate_runs" if phase_name == "candidate" else "baseline_runs",
                    run_started,
                )
            if profiled.user_query_executions != 1:
                raise RuntimeError(
                    "Profiled samples must execute the user query exactly once."
                )
            measurement[f"{phase_name}_samples"].append(extract_profile_metrics(profiled))
            measurement[f"{phase_name}_results"].append(
                profile_result_fingerprint(
                    profiled,
                    compare_order=compare_order,
                )
            )
            measurement[f"{phase_name}_plan"] = profiled.plan.summary
            measurement[f"{phase_name}_profile"] = profiled
            if phase_name == "candidate":
                uses = [
                    verify_plan_uses_index(profiled.plan.raw_xml, member.index_name).as_dict()
                    for member in members
                ]
                measurement["plan_use"].append(_index_set_plan_use(uses) if is_set else uses[0])
            return profiled

        arms_left = len(measurements) * INDEX_ABA_PHASES

        async def measure_arm(measurement: dict[str, Any], phase_name: str) -> None:
            """Measure one arm of one case; its first run sets how many runs it gets."""
            nonlocal arms_left
            await measure(measurement, phase_name)
            first_seconds = _sample_seconds(measurement[f"{phase_name}_samples"][0])
            # The session time and the cap of the call both bound the arm.
            remaining_seconds = min(self._require_tuning_session_time(session_id), clock.left())
            count = adaptive_run_count(
                first_seconds,
                runs,
                seconds_available=(
                    remaining_seconds / max(1, arms_left)
                    if math.isfinite(remaining_seconds)
                    else None
                ),
            )
            if (
                phase_name == "baseline_before"
                and count > 1
                and first_seconds >= FAST_STATEMENT_SECONDS
            ):
                # A statement of seconds runs once before the build. The first
                # candidate run decides whether its other runs are needed;
                # they then follow the drop.
                measurement["baseline_runs_after"] = count
                count = 1
            if phase_name == "candidate":
                baseline_seconds = (
                    _sample_seconds(measurement["baseline_before_samples"][0])
                    if measurement["baseline_before_samples"]
                    else measurement["slow_seconds"]
                )
                if baseline_seconds is not None and decisive_first_runs(
                    baseline_seconds, first_seconds
                ):
                    measurement["decisive"] = True
                    measurement["decisive_seconds"] = (baseline_seconds, first_seconds)
                    count = 1
            for _ in range(count - 1):
                await measure(measurement, phase_name)
            measurement["runs"][phase_name] = count
            arms_left -= 1

        async def build_member(member: _IndexSetMember, create_options: dict[str, Any]) -> None:
            """Build one index. A build that lost a deadlock or a lock wait gets one more try."""
            nonlocal create_failure
            build_started = clock.start()
            try:
                for attempt in (0, 1):
                    try:
                        await self._create_test_index(
                            database_name,
                            member.schema,
                            member.table,
                            member.index_name,
                            member.key_columns,
                            member.include_columns,
                            member.online,
                            dry_run=False,
                            workflow_managed=True,
                            filter_definition=member.filter_definition,
                            is_unique=member.is_unique,
                            idempotency_key=(
                                member_operation_key(
                                    "index-benchmark-create-retry-key", members.index(member)
                                )
                                if attempt
                                else member.create_key
                            ),
                            ownership_proof=lease_owner,
                            **create_options,
                        )
                        if attempt:
                            build_retries[-1]["succeeded"] = True
                        break
                    except Exception as create_error:
                        create_failure = classify_create_index_failure(create_error)
                        transient = (
                            create_failure["outcome"] == "definite_rollback"
                            and create_failure["error_number"] in TRANSIENT_BUILD_ERRORS
                        )
                        if transient:
                            create_failure["transient"] = True
                        if (
                            transient
                            and attempt == 0
                            and member_index(
                                await collect_existing_indexes(
                                    self.executor,
                                    database_name,
                                    schema_name=member.schema,
                                    table_name=member.table,
                                ),
                                member,
                            )
                            is None
                        ):
                            # The build lost a deadlock or a lock wait to
                            # another session and rolled back: one more try.
                            build_retries.append(
                                {
                                    "index_name": member.index_name,
                                    "error_number": create_failure["error_number"],
                                    "reason": create_failure["reason"],
                                    "retried": True,
                                    "succeeded": False,
                                }
                            )
                            create_failure = None
                            continue
                        if create_failure["outcome"] == "definite_rollback":
                            # The engine reported the error and XACT_ABORT rolled the
                            # CREATE transaction back: no index exists, nothing to clean.
                            member.create_outcome_settled = True
                        if is_set:
                            create_failure["index_name"] = member.index_name
                        raise
            finally:
                clock.add("build", build_started)

        def lease_status() -> str:
            if lease is None:
                raise RuntimeError("Temporary index lease was not created.")
            return str(lease["status"])

        async def settle_test_objects() -> None:
            """Drop what this call must not leave, or hold it, and record the lease state."""
            nonlocal cancelled, cleanup_error, cleanup_failure
            owned_members = [member for member in members if member.ownership_recorded]
            # An index whose create outcome is unknown keeps the lease open even
            # when every other index of the set was dropped.
            left_behind = any(member.left_behind for member in members)
            # keep_for_session leaves the whole set in place. A set that was not
            # built in full is not kept: what this call built of it is dropped.
            if not keep_for_session:
                drop_members = owned_members
            elif len(owned_members) == len(members) and not left_behind:
                drop_members = []
            else:
                drop_members = [member for member in owned_members if not member.reused]
            if drop_members:
                try:
                    if not left_behind:
                        update_lease("cleanup_pending")
                    drop_errors: list[Exception] = []
                    cleanup_started = clock.start()
                    try:
                        for member in reversed(drop_members):
                            try:
                                await drop_owned_member(member)
                            except Exception as drop_error:
                                # The other indexes of the set are still dropped.
                                drop_errors.append(drop_error)
                    finally:
                        clock.add("cleanup", cleanup_started)
                    if drop_errors:
                        raise drop_errors[0]
                    if left_behind:
                        update_lease("cleanup_required")
                    elif hold_mode and objects_in_place():
                        hold_lease()
                    else:
                        update_lease(
                            "cleaned",
                            {"cleaned_at_utc": datetime.now(timezone.utc).isoformat()},
                        )
                except asyncio.CancelledError:
                    cancelled = True
                    cleanup_error = "cancelled"
                    try:
                        update_lease(
                            "cleanup_required",
                            {
                                "cleanup_error_type": "cancelled",
                                "cleanup_policy": "retry_only_with_observed_ownership",
                            },
                        )
                    except Exception:
                        logger.exception(
                            "Unable to persist cancellation during temporary-index cleanup",
                            extra={"lease_id": lease_id},
                        )
                except Exception as exc:
                    cleanup_error = type(exc).__name__
                    cleanup_failure = _index_experiment_failure(
                        exc,
                        step="drop_index",
                        query_timeout_seconds=self.config.query_timeout_seconds,
                    )
                    update_lease("cleanup_required", {"cleanup_error_type": cleanup_error})
            elif hold_mode and objects_in_place() and lease_status() != "cleanup_required":
                try:
                    hold_lease()
                except Exception as exc:
                    # The lease stays open as it is; its expiry still bounds it.
                    note_exception(exc, "server.index_benchmark_hold")
                    cleanup_error = type(exc).__name__
            elif benchmark_error:
                if lease_status() not in {"cleanup_required", "cleaned"}:
                    update_lease("create_failed", {"failure_type": benchmark_error})

        try:
            for measurement in measurements:
                if measurement["slow_seconds"] is not None:
                    # Too slow to repeat: once in the session, never again.
                    if measurement["pooled_baseline"] is None:
                        await measure(measurement, "baseline_before")
                        measurement["runs"]["baseline_before"] = 1
                    arms_left -= 1
                    continue
                await measure_arm(measurement, "baseline_before")
                first_seconds = _sample_seconds(measurement["baseline_before_samples"][0])
                if first_seconds >= SLOW_STATEMENT_SECONDS:
                    # Found slow by its first run: treated as a declared slow case.
                    measurement["slow_seconds"] = round(first_seconds, 1)
                    measurement["slow_auto"] = True
            self._require_tuning_session_time(session_id)
            if lease is None:
                raise RuntimeError("Temporary index lease was not created.")
            to_build = [member for member in members if not member.reused]
            if to_build:
                clock.require("create_index")
            if to_build and max_call_seconds is not None:
                build_estimate = await self._build_time_estimate(
                    database_name, session_id, to_build
                )
                if (build_estimate["estimated_seconds"] or 0.0) > clock.left():
                    # Not started: a build cannot be stopped half way for free.
                    raise _CallTimeCap("create_index")
            for member in members:
                if member.reused:
                    continue
                if member is not members[0]:
                    self._require_tuning_session_time(session_id)
                update_lease(
                    "pending_create",
                    {
                        "create_dispatch_state": "dispatched",
                        "create_dispatched_at_utc": datetime.now(
                            timezone.utc
                        ).isoformat(),
                    },
                    member,
                )
                member.create_attempted = True
                step, building = "create_index", member
                # Passed only when set, as before these options existed.
                create_options: dict[str, Any] = {}
                if member.data_compression:
                    create_options["data_compression"] = member.data_compression
                if member.candidate.index_kind != INDEX_KIND_ROWSTORE:
                    create_options["index_kind"] = member.candidate.index_kind
                if member.table_facts.has_clustered_columnstore_index or member.table_facts.placement():
                    create_options["table_facts"] = member.table_facts
                await build_member(member, create_options)
                step = "verify_created_index"
                observed_index = member_index(
                    await collect_existing_indexes(
                        self.executor,
                        database_name,
                        schema_name=member.schema,
                        table_name=member.table,
                    ),
                    member,
                )
                if not await record_observed_index(
                    member,
                    observed_index,
                    observation="post_create",
                ):
                    if observed_index is not None:
                        await drop_index_proven_by_owner_marker(member, observed_index)
                    definition_mismatch = observed_index is not None
                    raise RuntimeError(
                        "Created index definition could not be verified exactly."
                    )
            step = "candidate_measure"
            for member in members:
                member.built = await self._built_index_facts(database_name, member)
            for measurement in measurements:
                await measure_arm(measurement, "candidate")
            if compare_results:
                step = "compare_results"
                for measurement in measurements:
                    clock.require("compare_results")
                    remaining_seconds = min(
                        self._require_tuning_session_time(session_id), clock.left()
                    )
                    # Charged before the await: a timeout can follow an execution.
                    measured_executions += 2
                    compare_started = clock.start()
                    try:
                        comparison = await self.performance_workflows._compare_execution_contracts(
                            database_name,
                            measurement["execution_contract"],
                            measurement["candidate_contract"],
                            compare_order=compare_order,
                            before_dispatch=lambda: self._require_tuning_session_time(session_id),
                            # The session time left, or the cap of the call, caps it.
                            max_seconds=(
                                remaining_seconds if math.isfinite(remaining_seconds) else None
                            ),
                        )
                    finally:
                        clock.add("comparison", compare_started)
                    measured_executions -= 2 - int(comparison.get("executions", 0))
                    if "rows_match" in comparison:
                        label_comparison(
                            comparison,
                            tested_gaps,
                            compare_order,
                            order_removed=order_contract_removed(equivalence_preflight),
                        )
                    measurement["snapshot_comparison"] = comparison
        except _CallTimeCap as cap:
            benchmark_error = "call_time_cap"
            benchmark_failure_diagnostic = clock.cap_diagnostic(str(cap))
        except asyncio.CancelledError:
            benchmark_error = "timeout"
            benchmark_failure_diagnostic = {
                "diagnostic_code": "benchmark_cancelled",
            }
            cancelled = True
            try:
                if await reconcile_create_outcome():
                    cancelled = True
                    benchmark_error = "timeout"
                    benchmark_failure_diagnostic = {
                        "diagnostic_code": "benchmark_cancelled",
                    }
            except Exception:
                logger.exception(
                    "Unable to durably reconcile temporary index ownership",
                    extra={"lease_id": lease_id},
                )
        except InvalidTransitionError as exc:
            benchmark_error = type(exc).__name__
            terminal_error = exc
            try:
                if await reconcile_create_outcome():
                    cancelled = True
                    benchmark_error = "timeout"
                    benchmark_failure_diagnostic = {
                        "diagnostic_code": "benchmark_cancelled",
                    }
            except Exception:
                logger.exception(
                    "Unable to durably reconcile temporary index ownership",
                    extra={"lease_id": lease_id},
                )
        except Exception as exc:
            note_exception(exc, "server.index_benchmark")
            benchmark_error = type(exc).__name__
            benchmark_failure_diagnostic = _index_experiment_failure(
                exc,
                step=step,
                query_timeout_seconds=self.config.query_timeout_seconds,
                create_failure=create_failure,
                definition_mismatch=definition_mismatch,
                online_build=building is not None and building.online,
            )
            try:
                if await reconcile_create_outcome():
                    cancelled = True
                    benchmark_error = "timeout"
                    benchmark_failure_diagnostic = {
                        "diagnostic_code": "benchmark_cancelled",
                    }
            except Exception:
                logger.exception(
                    "Unable to durably reconcile temporary index ownership",
                    extra={"lease_id": lease_id},
                )
        finally:
            await settle_test_objects()
        # True when test objects of the session are still in place: no
        # statement can then run as a clean baseline.
        objects_remain = hold_mode and lease["status"] == "active"

        if not cleanup_error and not benchmark_error and lease["status"] == "cleaned":
            try:
                for measurement in measurements:
                    if measurement["slow_seconds"] is not None or measurement["decisive"]:
                        continue
                    # As many runs as before the build: both halves of the
                    # baseline. A baseline of seconds ran once before it; the
                    # runs it still needs follow here.
                    after_runs = (
                        measurement["baseline_runs_after"]
                        or measurement["runs"]["baseline_before"]
                    )
                    for _ in range(after_runs):
                        await measure(measurement, "baseline_after")
                    measurement["runs"]["baseline_after"] = after_runs
            except _CallTimeCap as cap:
                benchmark_error = "call_time_cap"
                benchmark_failure_diagnostic = clock.cap_diagnostic(str(cap))
            except asyncio.CancelledError:
                benchmark_error = "timeout"
                benchmark_failure_diagnostic = {
                    "diagnostic_code": "benchmark_cancelled",
                }
                cancelled = True
            except InvalidTransitionError as exc:
                benchmark_error = type(exc).__name__
                terminal_error = exc
            except Exception as exc:
                note_exception(exc, "server.index_benchmark_after")
                benchmark_error = type(exc).__name__
                benchmark_failure_diagnostic = _index_experiment_failure(
                    exc,
                    step="baseline_after",
                    query_timeout_seconds=self.config.query_timeout_seconds,
                )

        if terminal_error is not None:
            reservation_update = (
                self.performance_store.complete_execution_attempts
                if measured_executions
                else self.performance_store.release_execution_attempts
            )
            reservation_update(
                execution_reservation["reservation_id"],
                dispatched_attempt_count=measured_executions,
                owner_reference=execution_owner,
                expected_version=execution_reservation["version"],
            )
            raise terminal_error

        for measurement in measurements:
            slow = measurement["slow_seconds"] is not None
            pooled = measurement["pooled_baseline"]
            baseline_results = (
                [dict(pooled["result"])]
                if pooled is not None
                else list(measurement["baseline_before_results"])
            )
            result_evidence = [
                *baseline_results,
                *measurement["candidate_results"],
                *measurement["baseline_after_results"],
            ]
            # A slow baseline has one result, and none after the index is dropped.
            # While held objects remain, no run follows a drop either.
            expected_counts = (
                (len(baseline_results) == 1, len(measurement["baseline_after_results"]) == 0)
                if slow
                else (
                    0 < len(baseline_results) == measurement["runs"]["baseline_before"],
                    len(measurement["baseline_after_results"]) == 0,
                )
                if objects_remain or measurement["decisive"]
                else (
                    0 < len(baseline_results) == measurement["runs"]["baseline_before"],
                    0
                    < len(measurement["baseline_after_results"])
                    == measurement["runs"]["baseline_after"],
                )
            )
            complete_results = (
                all(expected_counts)
                and 0 < len(measurement["candidate_results"]) == measurement["runs"]["candidate"]
                and all(item.get("complete") is True for item in result_evidence)
            )
            plan_used = bool(measurement["plan_use"]) and all(
                item.get("used") is True for item in measurement["plan_use"]
            )
            if normalized_rewrite is not None:
                # Two statements: only the snapshot comparison says they agree.
                snapshot_comparison = measurement.get(
                    "snapshot_comparison"
                ) or _not_compared(blocking_gaps, benchmark_error)
                equivalence.append(
                    {
                        **snapshot_comparison,
                        "parameter_case": measurement["parameter_case"],
                        "order_compared": compare_order,
                        "same_sql": False,
                        "basis": (
                            "the original and the rewrite compared in one snapshot "
                            "while the test indexes existed"
                        ),
                        "plan_used_expected_index": plan_used,
                    }
                )
                continue
            # An index often changes row access order; an order-only difference
            # is unproven, never a result mismatch.
            status = aba_result_status(result_evidence, complete=complete_results)
            stable = status == "match"
            equivalence.append(
                {
                    "parameter_case": measurement["parameter_case"],
                    "status": status,
                    "order_compared": compare_order,
                    "proven_for_parameter_case": stable,
                    "same_sql": not hinted,
                    "same_snapshot": False,
                    "basis": (
                        ("the SQL with only an index hint added" if hinted else "unchanged SQL")
                        + (
                            " plus result stability against the baseline this session "
                            "measured before the held test objects were built"
                            if pooled is not None and pooled.get("before_hold")
                            else " plus result stability against the one baseline run of the session"
                            if slow
                            else " plus result stability against the baseline runs before "
                            "the build (the test objects are kept, so no run follows a drop)"
                            if objects_remain
                            else " plus result stability against the one baseline run before "
                            "the build (a large gain is decided by one run of each side)"
                            if measurement["decisive"]
                            else " plus complete A-B-A result stability"
                        )
                    ),
                    "plan_used_expected_index": plan_used,
                    "result_evidence": result_evidence,
                }
            )

        parameter_results: list[dict[str, Any]] = []
        slow_baselines_kept: dict[str, Any] = {}
        baselines_before_hold_kept: dict[str, Any] = {}
        for measurement in measurements:
            pooled = measurement["pooled_baseline"]
            baseline_samples = (
                [dict(sample) for sample in pooled.get("samples") or [pooled["sample"]]]
                if pooled is not None
                else [
                    *measurement["baseline_before_samples"],
                    *measurement["baseline_after_samples"],
                ]
            )
            parameter_result: dict[str, Any] = {
                "parameter_case": measurement["parameter_case"],
                "weight": measurement["weight"],
                "baseline": aggregate_samples(baseline_samples),
                "candidate": aggregate_samples(measurement["candidate_samples"]),
                "plan_delta": plan_delta_from_profiles(
                    measurement["baseline_before_profile"],
                    measurement["candidate_profile"],
                ),
                "baseline_after_plan_delta": plan_delta_from_profiles(
                    measurement["baseline_before_profile"],
                    measurement["baseline_after_profile"],
                ),
                "plan_use": measurement["plan_use"],
                "runs": dict(measurement["runs"]),
                "plan_compiled_for": str(
                    measurement["parameter_case"]
                    if plan_reuse == "per_case"
                    else measurements[0]["parameter_case"]
                ),
            }
            if measurement["runs"]["candidate"] == 1:
                # Ran once by rule (slow, runs=1 or the time left): the 2x rule applies.
                parameter_result["candidate_single_sample"] = True
            if len(baseline_samples) == 1 and (measurement["decisive"] or objects_remain):
                # One baseline run by rule: the 2x rule applies to it too.
                parameter_result["baseline_single_sample"] = True
            if measurement["decisive"]:
                parameter_result["run_rule"] = _DECISIVE_RUN_RULE
                parameter_result["run_rule_note"] = _decisive_run_note(
                    *measurement["decisive_seconds"]
                )
            if (
                keep_for_session
                and pooled is None
                and measurement["baseline_before_samples"]
                and all(
                    item.get("complete") is True
                    for item in measurement["baseline_before_results"]
                )
            ):
                # Measured with no test object in place: the baseline of every
                # later call of the session while the objects of this one are held.
                baselines_before_hold_kept[str(measurement["parameter_case"])] = {
                    "sample": measurement["baseline_before_samples"][0],
                    "samples": list(measurement["baseline_before_samples"]),
                    "result": measurement["baseline_before_results"][0],
                    "plan": parameter_result["plan_delta"]["baseline"],
                    "order_compared": compare_order,
                    "plan_reuse": plan_reuse,
                    "case_order": [str(item["parameter_case"]) for item in measurements],
                    "sql_fingerprint": fingerprint_json({"baseline_sql": normalized_sql}),
                }
            if pooled is not None and pooled.get("before_hold"):
                parameter_result["baseline_single_sample"] = len(baseline_samples) == 1
                parameter_result["baseline_source"] = "session_before_hold"
                parameter_result["baseline_evidence_id"] = pooled["evidence_id"]
                parameter_result["baseline_note"] = (
                    "baseline measured earlier in this session, before the held test "
                    "objects were built. It was not run again: the statement cannot "
                    "run clean while they exist."
                )
                delta = parameter_result["plan_delta"]
                if delta["baseline"] is None and isinstance(pooled.get("plan"), Mapping):
                    delta["baseline"] = dict(pooled["plan"])
                    delta["plan_changed"] = (
                        delta["baseline"].get("plan_fingerprint")
                        != delta["candidate"]["plan_fingerprint"]
                        if delta["candidate"] is not None
                        else None
                    )
            elif measurement["slow_seconds"] is not None:
                parameter_result["baseline_single_sample"] = len(baseline_samples) == 1
                parameter_result["baseline_source"] = (
                    "session_single_run" if pooled is not None else "single_run"
                )
                parameter_result["baseline_note"] = (
                    (
                        f"baseline took {measurement['slow_seconds']:g} seconds on its first "
                        "run, so it was not repeated (10 seconds or more runs once). "
                    )
                    if measurement["slow_auto"]
                    else f"baseline too slow to repeat: {measurement['slow_seconds']:g} seconds. "
                ) + (
                    "It ran once in the session; the index was measured against that one "
                    "sample, and the baseline was not run again after the index was dropped."
                )
                delta = parameter_result["plan_delta"]
                if pooled is not None:
                    parameter_result["baseline_evidence_id"] = pooled["evidence_id"]
                    if delta["baseline"] is None and isinstance(pooled.get("plan"), Mapping):
                        delta["baseline"] = dict(pooled["plan"])
                        delta["plan_changed"] = (
                            delta["baseline"].get("plan_fingerprint")
                            != delta["candidate"]["plan_fingerprint"]
                            if delta["candidate"] is not None
                            else None
                        )
                elif (
                    pool_allowed
                    and measurement["baseline_before_samples"]
                    and measurement["baseline_before_results"][0].get("complete") is True
                ):
                    # Kept with the evidence, so the next index candidate of the
                    # session does not run this statement again.
                    slow_baselines_kept[str(measurement["parameter_case"])] = {
                        "sample": measurement["baseline_before_samples"][0],
                        "result": measurement["baseline_before_results"][0],
                        "plan": delta["baseline"],
                        "order_compared": compare_order,
                    }
            parameter_results.append(parameter_result)
        if cleanup_error:
            state, reason = (
                "cleanup_required",
                "benchmark completed but the temporary index could not be removed",
            )
        elif benchmark_error == "call_time_cap" and benchmark_failure_diagnostic is not None:
            state, reason = (
                "inconclusive",
                _call_cap_reason(
                    clock, str(benchmark_failure_diagnostic["failed_step"]), objects_remain
                ),
            )
        elif benchmark_error or not all(
            measurement["candidate_samples"] for measurement in measurements
        ):
            state, reason = (
                "inconclusive",
                "index candidate failed; reject this candidate and continue",
            )
        elif not all(
            comparison.get("plan_used_expected_index") is True
            for comparison in equivalence
        ):
            state, reason = (
                "inconclusive",
                "the expected temporary index was not used in every measured bucket",
            )
        else:
            objective = str(case.metadata.get("objective") or "elapsed_time")
            state, reason = classify_benchmark(
                parameter_results,
                equivalence,
                objective=objective,
                require_equivalence=(
                    compare_in_call
                    if normalized_rewrite is not None
                    else direct_snapshot_supported
                ),
                require_snapshot_attestation=compare_in_call,
            )

        tested_statuses = (
            {str(comparison.get("status")) for comparison in equivalence} if tested_gaps else set()
        )
        if (
            compare_results
            and state in {"promising", "improved", "neutral", "regressed"}
            and any(
                comparison.get("status") == "mismatch"
                and not comparison.get("mismatch_may_be_a_tie")
                for comparison in equivalence
            )
        ):
            # Different rows on today's data: no proof gap excuses that.
            state, reason = (
                "equivalence_failed",
                "at least one parameter case returned different results",
            )
        performance_classification = state
        performance_reason = reason
        candidate_state = state
        if (
            phase == "finalist"
            and performance_only_scope
            and state in {"promising", "improved"}
        ):
            state = "performance_only"
            candidate_state = "performance_only"
            performance_classification = "improved"
            reason = (
                (
                    "The hinted index improved the measured objective. This is hinted "
                    "evidence: it does not show that the optimizer will choose the "
                    "index, so the result is performance-only."
                )
                if hinted
                else _tested_match_reason("Candidate", tested_statuses, tested_gaps)
                if tested_gaps
                else (
                    "Candidate improved the measured objective, but semantic "
                    "equivalence remains unproven; the result is performance-only."
                )
            )

        index_report: list[dict[str, Any]] = []
        for member in members:
            index_report.append(
                {
                    "index_name": member.index_name,
                    "schema": member.schema,
                    "table": member.table,
                    "key_columns": list(member.key_columns),
                    "include_columns": list(member.include_columns or ()),
                    "filter_definition": member.filter_definition,
                    "is_unique": member.is_unique,
                    "index_kind": member.candidate.index_kind,
                    **(
                        {"partition_aligned": member.table_facts.data_space is None}
                        if member.table_facts.data_space is not None
                        or member.table_facts.partitioned
                        else {}
                    ),
                    "data_compression": member.data_compression
                    or (
                        "COLUMNSTORE"
                        if member.candidate.index_kind == INDEX_KIND_NONCLUSTERED_COLUMNSTORE
                        else "NONE"
                    ),
                    "online": member.online,
                    "built": member.ownership_recorded,
                    **(member.built or {}),
                    "used_in_cases": [
                        measurement["parameter_case"]
                        for measurement in measurements
                        if any(
                            member.index_name in _indexes_used(use)
                            for use in measurement["plan_use"]
                        )
                    ],
                }
            )
        unused_indexes = [
            item["index_name"]
            for item in index_report
            if item["built"] and not item["used_in_cases"]
        ]
        measured_with_set = not benchmark_error and all(
            measurement["candidate_samples"] for measurement in measurements
        )
        if is_set and unused_indexes and measured_with_set:
            reason = (
                f"{reason}. No measured case used {', '.join(unused_indexes)}: "
                "the gain does not come from it, so test the set without it"
            )

        candidate_plans = [
            measurement["candidate_plan"]
            for measurement in measurements
            if measurement["candidate_plan"]
        ]
        test_objects = _index_test_objects(
            members, lease, lease_entries() if objects_remain else []
        )
        time_spent = clock.report()
        build_rate = None if build_retries else _build_rate(members, clock.seconds["build"])
        # What finalize_tuning_session needs to state how far the rewrite is proven.
        rewrite_evidence: dict[str, Any] = {}
        if normalized_rewrite is not None:
            hint_profile = statement_hint_profile(normalized_rewrite)
            original_hints = statement_hint_profile(normalized_sql)["plan_forcing_hints"]
            hint_profile["added_plan_forcing_hints"] = [
                hint
                for hint in hint_profile["plan_forcing_hints"]
                if hint not in original_hints
            ]
            rewrite_evidence = {
                "experiment": "original_against_rewrite_with_indexes",
                "rewrite_requirements": rewrite_requirements(normalized_sql, normalized_rewrite),
                "hint_profile": hint_profile,
            }

        evidence = self.performance_store.create_evidence(
            EvidenceEnvelopeV1(
                source="azure-sql-mcp",
                kind=f"index_{phase}",
                query_fingerprint=case.query_fingerprint,
                database_fingerprint=case.database_fingerprint,
                parameters_fingerprint=fingerprint_json(
                    [
                        {
                            "name": bound_case["name"],
                            "parameter_case_fingerprint": bound_case[
                                "parameter_case_fingerprint"
                            ],
                        }
                        for bound_case in bound_cases
                    ]
                ),
                plan_fingerprint=(
                    fingerprint_json(candidate_plans) if candidate_plans else None
                ),
                observed_execution_count=measured_executions,
                metrics={
                    "classification": state,
                    "performance_classification": performance_classification,
                    "objective": str(case.metadata.get("objective") or "elapsed_time"),
                    "decision_basis": COMPARISON_DECISION_BASIS,
                    "parameter_results": parameter_results,
                    **({"slow_baselines": slow_baselines_kept} if slow_baselines_kept else {}),
                    **(
                        {"baselines_before_hold": baselines_before_hold_kept}
                        if baselines_before_hold_kept
                        else {}
                    ),
                },
                metadata={
                    "session_id": session_id,
                    "candidate_id": candidate_id,
                    "parameter_case_receipts": [
                        parameter_case_receipt(parameter_case)
                        for parameter_case in cases
                    ],
                    "test_objects": test_objects,
                    "time_spent": time_spent,
                    **(
                        {"build_rate_rows_per_second": build_rate}
                        if build_rate is not None
                        else {}
                    ),
                    "lease_id": lease_id,
                    "lease_status": lease["status"],
                    "index_definition_fingerprint": object_fingerprint,
                    "indexes": index_report,
                    **(
                        {"replacement_experiment": _replacement_summary(replacement)}
                        if replacement is not None
                        else {}
                    ),
                    "execution_reservation_id": execution_reservation[
                        "reservation_id"
                    ],
                    "equivalence": equivalence,
                    "equivalence_preflight": equivalence_preflight,
                    "proof_scope": proof_scope_label,
                    "equivalence_deferred": performance_only_scope,
                    "lineage": lineage,
                    "phase": phase,
                    "reason": reason,
                    "performance_reason": performance_reason,
                    "failure_diagnostic": benchmark_failure_diagnostic,
                    "execution_protocol": {
                        "measured_runs": runs,
                        "plan_reuse": plan_reuse,
                        "expected_executions": requested_executions,
                        # Adaptive run counts need fewer than were reserved.
                        **(
                            {"adaptive_expected_executions": measured_executions}
                            if not benchmark_error and not cleanup_error
                            else {}
                        ),
                    },
                    **rewrite_evidence,
                },
            ),
            idempotency_key=index_evidence_key,
            request_fingerprint=request_fingerprint,
        )
        durable_state = candidate_state
        if phase == "screening" and candidate_state in {"promising", "improved"}:
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state="screening",
                screen_runs=runs,
                parameter_cases=len(cases),
                executions=measured_executions,
                evidence_ids=(evidence.evidence_id,),
                idempotency_key=index_operation_key,
            )
            durable_state = updated.state
        else:
            _session, updated = self.tuning_sessions.record_candidate_result(
                session_id,
                candidate_id,
                state=candidate_state,
                screen_runs=runs if phase == "screening" else 0,
                finalist_runs=runs if phase == "finalist" else 0,
                parameter_cases=len(cases),
                executions=measured_executions,
                evidence_ids=(evidence.evidence_id,),
                failure_code=(
                    state
                    if candidate_state
                    in {"inconclusive", "cleanup_required", "equivalence_failed"}
                    else None
                ),
                idempotency_key=index_operation_key,
            )
        reservation_update = (
            self.performance_store.complete_execution_attempts
            if measured_executions
            else self.performance_store.release_execution_attempts
        )
        reservation_update(
            execution_reservation["reservation_id"],
            dispatched_attempt_count=measured_executions,
            owner_reference=execution_owner,
            expected_version=execution_reservation["version"],
        )
        public_lease = self._public_index_lease(lease)
        result = {
            "session_id": session_id,
            "candidate_id": candidate_id,
            **name_note,
            "classification": state,
            "performance_classification": performance_classification,
            "objective": str(case.metadata.get("objective") or "elapsed_time"),
            "decision_basis": COMPARISON_DECISION_BASIS,
            "durable_state": durable_state,
            "reason": reason,
            "phase": phase,
            "executions": measured_executions,
            "metrics": parameter_results[0],
            "parameter_results": parameter_results,
            "parameter_case_receipts": [
                parameter_case_receipt(parameter_case)
                for parameter_case in cases
            ],
            "equivalence": equivalence,
            "equivalence_preflight": equivalence_preflight,
            "equivalence_deferred": performance_only_scope,
            "proof_scope": proof_scope_label,
            "lineage": lineage,
            "evidence_id": evidence.evidence_id,
            "lease": public_lease,
            "index_definition_fingerprint": object_fingerprint,
            "indexes": index_report,
            "index_ddl": index_ddl,
            "rollback_ddl": rollback_ddl,
            "plan_reuse": plan_reuse,
            "test_objects": test_objects,
            "time_spent": time_spent,
            "session_continues": True,
        }
        if objects_remain and proof_scope_label == "aba_result_stability":
            result["proof_scope_note"] = (
                "The results matched before the build and with the index. No run followed "
                "a drop, because the index is held for the session."
            )
        if cleanup["dropped"]:
            result["cleanup_before_run"] = {"dropped": cleanup["dropped"]}
        if leftovers_dropped:
            result["leftovers_dropped"] = leftovers_dropped
        if benchmark_error == "call_time_cap":
            result["partial"] = True
        if build_estimate is not None:
            result["build_estimate"] = build_estimate
        if build_rate is not None:
            result["build_rate_rows_per_second"] = build_rate
        if build_retries:
            result["build_retries"] = build_retries
        if any(member.table_facts.data_space is not None for member in members):
            result["non_aligned_index_warning"] = NON_ALIGNED_INDEX_WARNING
        if normalized_rewrite is not None:
            result.update(rewrite_evidence)
            result["measured_arms"] = {
                "baseline": "the original statement, before and after, with no test index",
                "candidate": "the rewrite while the test indexes existed",
            }
        if replacement is not None:
            result["replacement_experiment"] = _replacement_summary(replacement)
            result["deployment_ready"] = False
        if is_set and unused_indexes and measured_with_set:
            result["unused_indexes"] = unused_indexes
        if benchmark_failure_diagnostic is not None:
            result["failure_diagnostic"] = benchmark_failure_diagnostic
        if create_failure is not None:
            result["create_failure"] = create_failure
        if cleanup_failure is not None:
            result["cleanup_failure"] = cleanup_failure
        offline_tables = list(
            dict.fromkeys(
                member.table_facts
                for member in members
                if member.table_facts.has_clustered_columnstore_index
            )
        )
        if offline_tables:
            result["offline_build"] = _offline_build_report(offline_tables)
        # A forced plan or Query Store hint on the application's query does
        # not apply to the marked benchmark text.
        result["plan_controls_not_applied"] = True
        if cancelled:
            raise asyncio.CancelledError
        return result

    def _local_view_experiment_capability(self, policy: DatabasePolicy) -> dict[str, Any]:
        """What check_capabilities says about indexed-view experiments on one database."""

        blockers = view_experiment_policy_blockers(policy)
        return {
            "allowed": not blockers,
            "blockers": list(blockers),
            "test_view_name_prefixes": list(
                view_name_prefixes(self._allowed_test_index_prefixes(policy))
            ),
        }

    async def _benchmark_indexed_view_candidate(
        self,
        database_name: str,
        session_id: str | None,
        candidate_id: str | None,
        view_definition: str,
        *,
        sql: str | None = None,
        candidate_sql: str | None = None,
        read_mode: str = READ_MODE_NOEXPAND,
        clustered_index_key: Sequence[str] | None = None,
        extra_indexes: Sequence[Mapping[str, Any]] | None = None,
        view_name: str | None = None,
        schema_name: str | None = None,
        index_name_prefix: str | None = None,
        parameter_cases: list[dict[str, Any]] | None = None,
        phase: str = "screening",
        runs: int | None = None,
        compare_order: bool | None = None,
        lease_minutes: int = 30,
        idempotency_key: str | None = None,
        preview: bool = False,
        keep_for_session: bool = False,
        max_call_seconds: float | None = None,
        plan_reuse: str = "per_case",
        slow_baseline_cases: Mapping[str, float] | None = None,
    ) -> dict[str, Any]:
        """Measure a statement against a leased indexed view built for it.

        ``plan_reuse`` and ``slow_baseline_cases`` are those of
        benchmark_index_candidate: ``first_case`` gives all parameter cases
        one statement text, so the later cases run on the plan of the first;
        a baseline declared slow runs once in the session, whichever
        benchmark tool measures it first. A replayed idempotency key returns
        the stored result and runs nothing.

        ``max_call_seconds`` caps the call: no run and no build starts after
        it, and the result is then ``partial`` with no verdict. A build that
        lost a deadlock or a lock wait is retried once. ``time_spent`` in
        the result is the seconds per phase.

        ``keep_for_session`` leaves the view in place under the lease of the
        session when the call returns, for at most ``lease_minutes``: a later
        call with the same view definition reuses it without a build, and
        compare_query_results and explain_query can read it meanwhile.
        finalize_tuning_session drops it.

        The original statement (``sql``, the statement of the performance
        case) runs before the build and after the drop. One lease then builds
        the schema-bound view, its unique clustered index and up to three more
        indexes in one transaction with the owner markers. ``read_mode``
        ``noexpand`` runs ``candidate_sql``, which reads the view ``WITH
        (NOEXPAND)`` (a rewrite_plus_index candidate, no parent); a finalist
        compares it with the original in one snapshot per parameter case
        while the view exists. ``automatic`` runs the original again (an
        index candidate) and reports whether the optimizer matched the view.
        The view is dropped on every path by a drop that checks the markers.
        ``preview`` returns the plan (names, findings, setup statements) and
        runs nothing.
        """

        self._require_test_index_database(database_name)
        clock = _CallClock(max_call_seconds)
        policy = self.database_policy.require(database_name)
        require_view_experiment_policy(policy)
        if phase not in {"screening", "finalist"}:
            raise ValueError("phase must be screening or finalist.")
        if read_mode not in {READ_MODE_NOEXPAND, READ_MODE_AUTOMATIC}:
            raise ValueError("read_mode must be noexpand or automatic.")
        if plan_reuse not in {"per_case", "first_case"}:
            raise ValueError("plan_reuse must be per_case or first_case.")
        name_prefix, name_tag = self._resolve_test_index_prefix(policy, index_name_prefix)
        allowed_prefixes = self._allowed_test_index_prefixes(policy)
        if len(extra_indexes or ()) > MAX_VIEW_EXTRA_INDEXES:
            raise ValueError(f"extra_indexes holds at most {MAX_VIEW_EXTRA_INDEXES} indexes.")
        first_check = validate_test_view_definition(view_definition)
        table_columns: dict[str, list[Any]] = {}
        for base_table in first_check.base_tables:
            base_schema, _, base_name = base_table.rpartition(".")
            table_columns[base_table] = await collect_table_column_facts(
                self.executor,
                database_name,
                schema_name=base_schema.strip("[]") or "dbo",
                table_name=base_name.strip("[]"),
            )
            if not table_columns[base_table]:
                raise ValueError(
                    f"Base table {base_table} does not exist or is not visible. "
                    "No query and no DDL was run."
                )
        plan = plan_view_experiment(
            view_definition,
            clustered_index_key=clustered_index_key,
            extra_indexes=[
                ViewIndexSpec(
                    key_columns=tuple(item.get("key_columns") or ()),
                    include_columns=tuple(item.get("include_columns") or ()),
                    is_unique=bool(item.get("is_unique", False)),
                )
                for item in extra_indexes or ()
            ],
            schema_name=schema_name,
            view_name=view_name,
            index_name_prefix=name_prefix,
            name_tag=name_tag,
            test_index_name_prefixes=allowed_prefixes,
            table_columns=table_columns,
            maxdop=policy.test_index_maxdop or None,
        )
        if preview:
            return {"preview": True, "executions": 0, **plan.as_dict()}

        def refuse(findings: Sequence[Any]) -> ValueError:
            return ValueError(
                "The indexed-view experiment was refused before any statement ran. "
                + " ".join(f"[{finding.code}] {finding.message}" for finding in findings)
                + f" The view name is {plan.schema}.{plan.view_name}; call with "
                "preview=true to see the plan."
            )

        if not plan.valid or plan.clustered_index is None:
            raise refuse([item for item in plan.findings if item.severity == "error"] or plan.findings)
        if not session_id or not candidate_id or not sql:
            raise ValueError("session_id, candidate_id and sql are required unless preview is true.")
        if not idempotency_key or not idempotency_key.strip():
            raise ValueError("An indexed-view experiment requires an idempotency key.")
        try:
            session = self.tuning_sessions.require_session_status(
                session_id, allowed={"created", "screening", "finalist_validation"}
            )
        except InvalidTransitionError as exc:
            raise InvalidTransitionError(
                f"{exc} Retrieve committed results with get_tuning_session; "
                "terminal sessions cannot benchmark or replay."
            ) from None
        candidate = self.tuning_sessions.get_candidate(candidate_id)
        if candidate.session_id != session_id:
            raise ValueError("Candidate does not belong to the tuning session.")
        case = self.performance_store.get_performance_case(session.performance_case_id)
        allow_legacy = bool(self.config.legacy_state_server_binding)
        if not database_fingerprint_matches(
            case.database_fingerprint or "", database_name, self.config.server, allow_legacy=allow_legacy
        ):
            raise PermissionError("Tuning session belongs to another database.")
        original = self.validator.validate_read_only_candidate(sql).execution_sql
        if not fingerprint_text_matches(case.query_fingerprint, original, allow_legacy=allow_legacy):
            raise ValueError("sql is the original statement: it must match the performance case.")
        noexpand = read_mode == READ_MODE_NOEXPAND
        if noexpand:
            if not candidate_sql:
                raise ValueError(
                    "read_mode noexpand needs candidate_sql: the statement that reads "
                    f"{plan.schema}.{plan.view_name} WITH (NOEXPAND)."
                )
            measured = self.validator.validate_read_only_candidate(candidate_sql).execution_sql
            if candidate.strategy != "rewrite_plus_index" or not fingerprint_text_matches(
                candidate.rewrite_fingerprint, measured, allow_legacy=allow_legacy
            ):
                raise ValueError(
                    "Register candidate_sql with add_tuning_candidate(strategy="
                    "'rewrite_plus_index') and no artifact_ref, and pass it unchanged."
                )
        else:
            if candidate_sql:
                raise ValueError("read_mode automatic runs the original statement; leave candidate_sql out.")
            measured = original
            if candidate.strategy != "index" or not fingerprint_text_matches(
                candidate.rewrite_fingerprint, original, allow_legacy=allow_legacy
            ):
                raise ValueError(
                    "read_mode automatic needs an index candidate registered with the "
                    "unchanged statement of the performance case."
                )
        findings = view_candidate_findings(measured, plan, read_mode=read_mode)
        if any(finding.severity == "error" for finding in findings) or (not noexpand and findings):
            raise refuse(findings)
        # Before the hold is read: an expired hold is dropped here, not reused.
        cleanup = await self._reconcile_leases(
            database_fingerprint=case.database_fingerprint,
            attempts=_PRE_CALL_DROP_ATTEMPTS,
        )
        if cleanup["cleanup_required"]:
            return self._leftover_refusal(cleanup, session_id=session_id, candidate_id=candidate_id)
        # Test objects an earlier call of this session left in place.
        hold = self._session_hold(case.database_fingerprint or "", session_id)
        held_entries: list[dict[str, Any]] = (
            [dict(entry) for entry in hold["metadata"]["members"]] if hold is not None else []
        )
        held_view = next(
            (
                entry
                for entry in held_entries
                if entry.get("object_kind") == LEASE_OBJECT_KIND
                and entry.get("object_fingerprint") == plan.definition_fingerprint
            ),
            None,
        )
        hold_mode = keep_for_session or hold is not None
        if held_view is None:
            blocker = name_in_use_blocker(
                plan,
                await read_view_catalog_state(
                    self.executor, database_name, schema_name=plan.schema, view_name=plan.view_name
                ),
            )
            if blocker is not None:
                raise ValueError(blocker)
        option_rows = await self.executor.fetch_all(
            database_name, SESSION_SET_OPTIONS_SQL, sql_origin="server_built"
        )
        option_blockers = session_set_option_blockers(option_rows[0] if option_rows else {})
        if option_blockers:
            raise ValueError(
                "The measurement connection cannot read an indexed view through its index: "
                + "; ".join(option_blockers)
                + ". No query and no DDL was run."
            )
        compare_order = effective_compare_order(original, compare_order)
        equivalence_preflight: dict[str, Any] = await self._check_equivalence_preflight(
            database_name, original
        )
        direct_snapshot_supported = bool(equivalence_preflight["direct_snapshot_supported"])
        if noexpand:
            # The view does not exist yet, so the catalog cannot resolve it. It
            # is declared to the order analysis with the key of its unique
            # clustered index: SQL Server treats NULLs as equal in a unique
            # index, so that key leaves no two rows of the view tied.
            view_key = tuple(
                parse_candidate_key(column).name for column in plan.clustered_index.key_columns
            )
            candidate_preflight = (
                await self.equivalence_preflight.analyze(
                    database_name,
                    measured,
                    declared_tables={
                        (plan.schema.casefold(), plan.view_name.casefold()): TableConstraintFacts(
                            schema_name=plan.schema,
                            table_name=plan.view_name,
                            exists=True,
                            object_type="USER_TABLE",
                            nullable={column.casefold(): False for column in view_key},
                            unique_keys=(
                                UniqueKey(
                                    name=str(plan.clustered_index.index_name),
                                    kind="unique_index",
                                    columns=view_key,
                                ),
                            ),
                        )
                    },
                )
            ).as_dict()
            # For the ORDER BY contract the view is read as its definition, so
            # a column of the view is the base column it copies.
            equivalence_preflight = with_order_contract(
                {"baseline": equivalence_preflight, "candidate": candidate_preflight},
                original,
                measured,
                candidate_relation=(
                    None
                    if plan.definition.column_list
                    else (plan.schema, plan.view_name, plan.definition.body)
                ),
            )
            direct_snapshot_supported = all(
                bool(side["direct_snapshot_supported"]) for side in equivalence_preflight.values()
            )
        compare_in_call = noexpand and direct_snapshot_supported and phase == "finalist"
        # The view statement is compared with the original in every call that
        # builds the view. A gap that a comparison can settle is tested; only a
        # gap that makes a result unstable leaves the case uncompared.
        gap_codes: list[str] = []
        blocking_gaps: list[str] = []
        if noexpand:
            gap_codes, blocking_gaps = preflight_gap_codes(equivalence_preflight)
        compare_results = noexpand and not blocking_gaps
        tested_gaps = gap_codes if compare_results else []
        cases = self.performance_workflows._resolve_parameter_cases(case.case_id, parameter_cases)
        supplied = tuple(parameter_case_fingerprint(item) for item in cases)
        registered = set(case.parameter_case_fingerprints)
        if (
            len(cases) > session.parameter_case_limit
            or len(set(supplied)) != len(supplied)
            or any(fingerprint not in registered for fingerprint in supplied)
        ):
            raise ValueError(
                "parameter_cases must be an unchanged subset of the cases registered on "
                f"the performance case, at most {session.parameter_case_limit}."
            )
        if phase == "finalist" and set(supplied) != registered:
            raise ValueError(
                f"A finalist must cover all {len(registered)} registered parameter cases; "
                f'this call names {len(supplied)}. Use phase="screening" for a subset.'
            )
        default_runs = (
            session.screen_runs_per_candidate
            if phase == "screening"
            else session.finalist_runs_per_candidate
        )
        if runs is None:
            runs = default_runs
        elif not 1 <= runs <= default_runs:
            raise ValueError(f"runs must be between 1 and the {phase} limit of {default_runs}.")
        case_names = [str(item["name"]) for item in cases]
        slow_cases = self._slow_baseline_seconds(slow_baseline_cases, cases)
        # A baseline too slow to repeat runs once in the session, whichever
        # benchmark tool ran it. Only a statement on its own plan is pooled.
        pooled_baselines = (
            self._session_slow_index_baselines(session_id, compare_order)
            if plan_reuse == "per_case"
            else {}
        )
        if held_entries:
            pooled_baselines = self._require_baselines_before_hold(
                session_id,
                original,
                compare_order,
                plan_reuse,
                case_names,
                held_entries,
                pooled_baselines,
            )
        requested_executions = sum(
            runs
            if name in pooled_baselines
            else runs + 1
            if name in slow_cases
            else runs * INDEX_ABA_PHASES
            for name in case_names
        ) + (2 * len(cases) if compare_results else 0)
        if not policy.can_benchmark(requested_executions):
            raise PermissionError("Database policy does not permit this benchmark count.")
        owner_fingerprint = fingerprint_json(
            {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "phase": phase,
                "idempotency_key": idempotency_key,
            }
        )
        execution_owner = f"view-execution-{owner_fingerprint[:32]}"
        lease_owner = f"index-lease-{owner_fingerprint[:32]}"
        lease_id = f"lease-{fingerprint_json({'view': owner_fingerprint})[:32]}"
        if hold is not None:
            # The session has one lease; this call works under it and its owner token.
            lease_id = str(hold["lease_id"])
            lease_owner = str(hold["metadata"]["lease_owner_fence"])
        # What the lease lists for the view, and the held objects this call leaves alone.
        view_entry: dict[str, Any] = (
            held_view
            if held_view is not None
            else {
                **plan.lease_metadata(),
                "index_name": str(plan.clustered_index.index_name),
                "object_fingerprint": plan.definition_fingerprint,
            }
        )
        carried_entries = [entry for entry in held_entries if entry is not held_view]
        view_listed = True

        def lease_entries() -> list[dict[str, Any]]:
            return [*carried_entries, *([dict(view_entry)] if view_listed else [])]

        request_fingerprint = fingerprint_json(
            {
                "operation": "indexed-view-benchmark-v1",
                "session_id": session_id,
                "candidate_id": candidate_id,
                "database_name": database_name,
                "sql": original,
                "candidate_sql": measured,
                "read_mode": read_mode,
                "view": plan.definition_fingerprint,
                "phase": phase,
                "parameter_cases": supplied,
                "runs": runs,
                "compare_order": compare_order,
                **({"keep_for_session": True} if keep_for_session else {}),
                **({"plan_reuse": plan_reuse} if plan_reuse != "per_case" else {}),
                **({"slow_baseline_cases": dict(slow_cases)} if slow_cases else {}),
            }
        )
        operation_key = fingerprint_json({"operation": "indexed-view-result", "key": idempotency_key})
        evidence_key = fingerprint_json(
            {"operation": "indexed-view-evidence", "key": idempotency_key}
        )

        def plan_identity(name: Any) -> str:
            # One text for all cases: the first case compiles, the others reuse.
            return str(name) if plan_reuse == "per_case" else "shared-plan"

        replay = {
            "session_id": session_id,
            "candidate_id": candidate_id,
            "classification": candidate.state,
            "durable_state": candidate.state,
            "reason": (
                "This idempotency key already ran for this candidate. No DDL and no "
                "query was dispatched again; read the result with get_tuning_session."
            ),
            "executions": 0,
            "replayed": True,
            "session_continues": True,
        }

        def stored_result() -> dict[str, Any]:
            """The committed result of this key; the notice when none was committed."""
            evidence = self.performance_store.get_idempotent_evidence(
                evidence_key, request_fingerprint=request_fingerprint
            )
            if evidence is None:
                return replay
            stored = dict(evidence.metadata)
            metrics = dict(evidence.metrics)
            stored_lease = self.performance_store.get_index_lease(str(stored.get("lease_id")))
            result: dict[str, Any] = {
                "session_id": session_id,
                "candidate_id": candidate_id,
                "experiment": "indexed_view",
                "read_mode": read_mode,
                "plan_reuse": plan_reuse,
                "classification": metrics.get("classification"),
                "performance_classification": metrics.get("performance_classification"),
                "objective": metrics.get("objective"),
                "decision_basis": metrics.get("decision_basis"),
                "durable_state": candidate.state,
                "reason": stored.get("reason"),
                "phase": phase,
                "executions": int(evidence.observed_execution_count),
                "parameter_results": metrics.get("parameter_results") or [],
                "equivalence": stored.get("equivalence") or [],
                "equivalence_preflight": stored.get("equivalence_preflight"),
                "equivalence_deferred": stored.get("equivalence_deferred"),
                "proof_scope": stored.get("proof_scope"),
                "evidence_id": evidence.evidence_id,
                "lease": self._public_index_lease(stored_lease),
                "view": {
                    key: value for key, value in plan.as_dict().items() if key != "findings" or value
                },
                "built": (stored.get("view") or {}).get("built"),
                "verification": stored.get("verification"),
                "test_objects": stored.get("test_objects"),
                "time_spent": stored.get("time_spent"),
                "session_continues": stored_lease["status"] != "cleanup_required",
                "plan_controls_not_applied": True,
                "replayed": True,
                "recovered_from_durable_evidence": True,
            }
            if isinstance(stored.get("failure_diagnostic"), Mapping):
                result["failure_diagnostic"] = dict(stored["failure_diagnostic"])
            return result

        if (
            self.performance_store.get_idempotent_execution_reservation(
                session_id,
                candidate_id,
                request_fingerprint,
                idempotency_key=idempotency_key,
                owner_reference=execution_owner,
            )
            is not None
        ):
            return stored_result()
        if candidate.is_terminal or (candidate.screen_runs if phase == "screening" else candidate.finalist_runs):
            raise ValueError(f"This candidate already has a {phase} result; add a new candidate.")
        bound_cases = [
            {
                "parameter_case": item["name"],
                "weight": item["weight"],
                "fingerprint": parameter_case_fingerprint(item),
                "execution_contract": await self.performance_workflows._bind_case(
                    database_name,
                    mark_benchmark_sql(
                        original, session_id, "view", candidate_id, plan_identity(item["name"])
                    ),
                    item,
                ),
                "candidate_contract": await self.performance_workflows._bind_case(
                    database_name,
                    mark_benchmark_sql(
                        measured,
                        session_id,
                        "view-candidate",
                        candidate_id,
                        plan_identity(item["name"]),
                    ),
                    item,
                ),
            }
            for item in cases
        ]
        self._require_tuning_session_time(session_id)
        reservation = self.performance_store.reserve_execution_attempts(
            session_id,
            candidate_id,
            requested_executions,
            request_fingerprint,
            idempotency_key=idempotency_key,
            owner_reference=execution_owner,
        )
        if reservation["status"] != "reserved" or reservation.get("replayed") is True:
            return stored_result()
        create_sql = build_view_experiment_create_batch(plan)
        drop_sql = build_test_view_drop_batch(
            plan.schema, plan.view_name, test_index_name_prefixes=allowed_prefixes
        )
        create_params, _drop_params = view_experiment_batch_params(plan, lease_owner)
        try:
            remaining = self._require_tuning_session_time(session_id)
            lease_expires_at = (
                datetime.now(timezone.utc)
                + timedelta(
                    minutes=max(lease_minutes, int(remaining / 60) + 2)
                    if math.isfinite(remaining)
                    else lease_minutes
                )
            ).isoformat()
            lease: dict[str, Any] = (
                self.performance_store.update_index_lease(
                    lease_id,
                    status="active",
                    metadata={"hold_state": "in_call", "members": lease_entries()},
                    owner_reference=lease_owner,
                    expected_version=hold["version"],
                    expires_at_utc=lease_expires_at,
                )
                if hold is not None
                else self.performance_store.create_index_lease(
                    lease_id=lease_id,
                    database_fingerprint=case.database_fingerprint or "",
                    session_id=session_id,
                    candidate_id=candidate_id,
                    index_name=str(plan.clustered_index.index_name),
                    object_fingerprint=plan.definition_fingerprint,
                    expires_at_utc=lease_expires_at,
                    metadata={
                        # A lease that keeps objects lists each of them.
                        **({"members": lease_entries()} if hold_mode else plan.lease_metadata()),
                        "phase": phase,
                        "lease_owner_fence": lease_owner,
                        "sql_persisted": False,
                    },
                    owner_reference=lease_owner,
                    request_fingerprint=fingerprint_json(
                        {"lease_id": lease_id, "key": idempotency_key}
                    ),
                )
            )
            if phase == "finalist":
                if self.tuning_sessions.get_session(session_id).status == "created":
                    self.tuning_sessions.start_screening(session_id)
                self.tuning_sessions.mark_candidate_finalist(session_id, candidate_id)
            else:
                self.tuning_sessions.start_screening(session_id)
        except BaseException:
            self.performance_store.release_execution_attempts(
                reservation["reservation_id"],
                owner_reference=execution_owner,
                expected_version=reservation["version"],
            )
            if hold is not None:
                self._return_hold_to_idle(lease_id, lease_owner, lease_minutes)
            raise

        def update_lease(
            status: str,
            metadata: Mapping[str, Any] | None = None,
            expires_at_utc: str | None = None,
        ) -> None:
            nonlocal lease
            payload: dict[str, Any] | None = dict(metadata) if metadata is not None else None
            if hold_mode:
                # A fact about the view goes to its entry; the lease lists what exists.
                facts = payload or {}
                payload = {
                    key: facts.pop(key)
                    for key in (HOLD_FLAG, "hold_state", "hold_minutes", "cleaned_at_utc")
                    if key in facts
                }
                view_entry.update(facts)
                payload["members"] = lease_entries()
            lease = self.performance_store.update_index_lease(
                lease_id,
                status=status,
                metadata=payload,
                owner_reference=lease_owner,
                expected_version=lease["version"],
                expires_at_utc=expires_at_utc,
            )

        def hold_lease() -> None:
            """Leave the lease open with the objects it lists, bounded from now."""
            held_at = datetime.now(timezone.utc)
            if view_listed:
                view_entry.setdefault("held_since_utc", held_at.isoformat())
            update_lease(
                "active",
                {HOLD_FLAG: True, "hold_state": "idle", "hold_minutes": lease_minutes},
                expires_at_utc=(held_at + timedelta(minutes=lease_minutes)).isoformat(),
            )

        view_index_names = [str(index.index_name) for index in plan.indexes]
        measurements: list[dict[str, Any]] = [
            {
                **bound,
                "pooled_baseline": pooled_baselines.get(str(bound["parameter_case"])),
                # True when the baseline is declared slow, or its first run
                # takes 10 seconds or more: it runs once in the session.
                "slow": str(bound["parameter_case"]) in slow_cases,
                # Runs of a baseline of seconds that wait for the first candidate run.
                "baseline_runs_after": None,
                # True when one run of each side decided (decisive_first_runs).
                "decisive": False,
                "runs": {"baseline_before": 0, "candidate": 0, "baseline_after": 0},
                "baseline_before_samples": [],
                "candidate_samples": [],
                "baseline_after_samples": [],
                "baseline_before_results": [],
                "candidate_results": [],
                "baseline_after_results": [],
                "baseline_before_profile": None,
                "candidate_profile": None,
                "plan_use": [],
            }
            for bound in bound_cases
        ]
        measured_executions = 0
        arms_left = len(measurements) * INDEX_ABA_PHASES

        async def measure_arm(measurement: dict[str, Any], arm: str, count: int | None = None) -> None:
            """One arm of one case; its first run sets the run count unless ``count`` is given."""
            nonlocal measured_executions, arms_left
            target = count
            run = 0
            while target is None or run < target:
                remaining_seconds = min(
                    self._require_tuning_session_time(session_id), clock.left()
                )
                clock.require("candidate_measure" if arm == "candidate" else arm)
                measured_executions += 1
                run_started = clock.start()
                try:
                    profiled = await self.performance_workflows._profile_execution_contract(
                        database_name,
                        measurement[
                            "candidate_contract" if arm == "candidate" else "execution_contract"
                        ],
                        max_result_rows=self.config.comparison_row_limit,
                    )
                finally:
                    clock.add(
                        "candidate_runs" if arm == "candidate" else "baseline_runs", run_started
                    )
                sample = extract_profile_metrics(profiled)
                measurement[f"{arm}_samples"].append(sample)
                measurement[f"{arm}_results"].append(
                    profile_result_fingerprint(profiled, compare_order=compare_order)
                )
                if arm != "baseline_after":
                    measurement[f"{arm}_profile"] = profiled
                if arm == "candidate":
                    measurement["plan_use"].append(
                        any(
                            verify_plan_uses_index(profiled.plan.raw_xml, name).used
                            for name in view_index_names
                        )
                    )
                run += 1
                if target is None:
                    first_seconds = _sample_seconds(sample)
                    target = adaptive_run_count(
                        first_seconds,
                        runs,
                        seconds_available=(
                            remaining_seconds / max(1, arms_left)
                            if math.isfinite(remaining_seconds)
                            else None
                        ),
                    )
                    if (
                        arm == "baseline_before"
                        and target > 1
                        and first_seconds >= FAST_STATEMENT_SECONDS
                    ):
                        # A statement of seconds runs once before the build. The
                        # first candidate run decides whether its other runs are
                        # needed; they then follow the drop.
                        measurement["baseline_runs_after"] = target
                        target = 1
                    if arm == "candidate":
                        pooled_sample = (measurement["pooled_baseline"] or {}).get("sample")
                        baseline_seconds = (
                            _sample_seconds(measurement["baseline_before_samples"][0])
                            if measurement["baseline_before_samples"]
                            else _sample_seconds(pooled_sample)
                            if isinstance(pooled_sample, Mapping)
                            else None
                        )
                        if baseline_seconds is not None and decisive_first_runs(
                            baseline_seconds, first_seconds
                        ):
                            measurement["decisive"] = True
                            measurement["decisive_seconds"] = (baseline_seconds, first_seconds)
                            target = 1
            measurement["runs"][arm] = run
            arms_left -= 1

        async def drop_view(state: ViewCatalogState) -> str:
            """Drop the view of this lease and return the lease status the catalog proves."""
            await self.admin_policy.execute(
                AdminAction(
                    tool_name="benchmark_indexed_view_candidate",
                    database_name=database_name,
                    action_type="test_view",
                    sql=drop_sql,
                    params=(lease_owner, plan.definition_fingerprint, state.object_id or None),
                    # The exact inverse of the drop: the build batch of this lease.
                    rollback_sql=create_sql,
                    rollback_params=create_params,
                    trusted_generated=True,
                    reviewed_intent=True,
                    idempotency_key=fingerprint_json({"view-drop": owner_fingerprint}),
                    exactly_once=True,
                    policy_verified=True,
                    non_production=True,
                    verification_required=True,
                ),
                self.executor,
                dry_run=False,
            )
            after = await read_view_catalog_state(
                self.executor, database_name, schema_name=plan.schema, view_name=plan.view_name
            )
            return reconcile_view_experiment(
                step="drop",
                state=after,
                owner_token=lease_owner,
                definition_fingerprint=plan.definition_fingerprint,
            ).lease_status

        step = "baseline_before"
        failure: dict[str, Any] | None = None
        built: dict[str, Any] | None = None
        verification: dict[str, Any] | None = None
        view_exists = False
        view_verified = False
        cancelled = False
        build_retries: list[dict[str, Any]] = []
        comparisons: dict[str, dict[str, Any]] = {}
        try:
            for measurement in measurements:
                if measurement["pooled_baseline"] is not None:
                    # Measured earlier in the session with no test object in place.
                    arms_left -= 1
                    continue
                await measure_arm(
                    measurement, "baseline_before", 1 if measurement["slow"] else None
                )
                measurement["slow"] = measurement["slow"] or (
                    _sample_seconds(measurement["baseline_before_samples"][0])
                    >= SLOW_STATEMENT_SECONDS
                )
            self._require_tuning_session_time(session_id)
            if held_view is None:
                step = "build_view"
                clock.require(step)
                update_lease("pending_create", {"create_dispatch_state": "dispatched"})
                build_started = clock.start()
                try:
                    for attempt in (0, 1):
                        try:
                            await self.admin_policy.execute(
                                AdminAction(
                                    tool_name="benchmark_indexed_view_candidate",
                                    database_name=database_name,
                                    action_type="test_view",
                                    sql=create_sql,
                                    params=create_params,
                                    rollback_sql=drop_sql,
                                    rollback_params=(lease_owner, plan.definition_fingerprint, None),
                                    trusted_generated=True,
                                    reviewed_intent=True,
                                    idempotency_key=fingerprint_json(
                                        {
                                            "view-build-retry" if attempt else "view-build": (
                                                owner_fingerprint
                                            )
                                        }
                                    ),
                                    exactly_once=True,
                                    policy_verified=True,
                                    non_production=True,
                                    verification_required=True,
                                ),
                                self.executor,
                                dry_run=False,
                            )
                            if attempt:
                                build_retries[-1]["succeeded"] = True
                            break
                        except Exception as build_error:
                            lost = view_build_failure(build_error)
                            if (
                                attempt
                                or lost["outcome"] != "definite_rollback"
                                or lost["engine_error_number"] not in TRANSIENT_BUILD_ERRORS
                            ):
                                raise
                            # The build lost a deadlock or a lock wait to another
                            # session and its batch rolled back: one more try.
                            build_retries.append(
                                {
                                    "view": f"{plan.schema}.{plan.view_name}",
                                    "error_number": lost["engine_error_number"],
                                    "retried": True,
                                    "succeeded": False,
                                }
                            )
                finally:
                    clock.add("build", build_started)
            # A held view is proven again before it is measured: the catalog,
            # not the lease, says what exists.
            step = "verify_built_view"
            state = await read_view_catalog_state(
                self.executor, database_name, schema_name=plan.schema, view_name=plan.view_name
            )
            view_exists = state.owned_by(lease_owner, plan.definition_fingerprint)
            check = verify_view_experiment(plan, state, owner_token=lease_owner)
            verification = check.as_dict()
            if not check.matches:
                raise RuntimeError(
                    "The held view no longer matches the request."
                    if held_view is not None
                    else "The built view does not match the request."
                )
            view_verified = True
            update_lease("active", {"view_object_id": state.object_id})
            built = view_index_size(plan, state)
            step = "candidate_measure"
            for measurement in measurements:
                await measure_arm(measurement, "candidate")
            if compare_results:
                step = "compare_results"
                for measurement in measurements:
                    clock.require(step)
                    remaining_seconds = min(
                        self._require_tuning_session_time(session_id), clock.left()
                    )
                    measured_executions += 2
                    compare_started = clock.start()
                    try:
                        comparison = await self.performance_workflows._compare_execution_contracts(
                            database_name,
                            measurement["execution_contract"],
                            measurement["candidate_contract"],
                            compare_order=compare_order,
                            before_dispatch=lambda: self._require_tuning_session_time(session_id),
                            # The session time left, or the cap of the call, caps it.
                            max_seconds=(
                                remaining_seconds if math.isfinite(remaining_seconds) else None
                            ),
                        )
                    finally:
                        clock.add("comparison", compare_started)
                    measured_executions -= 2 - int(comparison.get("executions", 0))
                    if "rows_match" in comparison:
                        label_comparison(
                            comparison,
                            tested_gaps,
                            compare_order,
                            order_removed=order_contract_removed(equivalence_preflight),
                        )
                    comparisons[str(measurement["parameter_case"])] = comparison
        except _CallTimeCap as cap:
            failure = clock.cap_diagnostic(str(cap))
        except asyncio.CancelledError:
            cancelled = True
            failure = {"diagnostic_code": "benchmark_cancelled", "failed_step": step}
        except Exception as exc:
            note_exception(exc, "server.indexed_view_benchmark")
            failure = extract_failure_diagnostic(exc, engine_text=step == "build_view")
            failure["failed_step"] = step
            if step == "build_view":
                build_failure = view_build_failure(exc)
                failure["build_outcome"] = build_failure["outcome"]
                failure["build_outcome_reason"] = build_failure["reason"]
                if build_failure["engine_error_number"] is not None:
                    failure["native_error_code"] = build_failure["engine_error_number"]
            elif step == "verify_built_view" and verification is not None:
                failure["cause_class"] = "definition_mismatch"
                failure["mismatches"] = verification["mismatches"]
        finally:
            # The view is dropped on every path; the lease closes only when
            # the catalog shows that nothing of it is left.
            try:
                if step == "baseline_before" and held_view is None:
                    # This call built nothing; held objects it did not use stay.
                    view_listed = False
                    if carried_entries:
                        hold_lease()
                    else:
                        update_lease("create_failed", {"failure_type": "not_built"})
                else:
                    try:
                        state = await read_view_catalog_state(
                            self.executor,
                            database_name,
                            schema_name=plan.schema,
                            view_name=plan.view_name,
                        )
                    except Exception as read_error:
                        note_exception(read_error, "server.indexed_view_catalog_read")
                        state = None
                    outcome = reconcile_view_experiment(
                        step="build" if step == "build_view" and not view_exists else "drop",
                        state=state,
                        owner_token=lease_owner,
                        definition_fingerprint=plan.definition_fingerprint,
                        # Only a lost connection, a timeout or a cancel can
                        # leave the build running after the catalog was read.
                        transport_failure=cancelled
                        or bool(
                            failure
                            and failure.get("build_outcome_reason") == "connection_or_timeout"
                        ),
                    )
                    status = outcome.lease_status
                    if keep_for_session and view_verified and outcome.drop_view:
                        # Built and proven: kept for the session, also when a
                        # later step of this call failed.
                        hold_lease()
                    else:
                        if outcome.drop_view and state is not None:
                            update_lease("cleanup_pending")
                            cleanup_started = clock.start()
                            try:
                                status = await drop_view(state)
                            finally:
                                clock.add("cleanup", cleanup_started)
                        if status in {"cleaned", "create_failed"}:
                            view_listed = False
                        if carried_entries and status in {"cleaned", "create_failed"}:
                            hold_lease()
                        else:
                            update_lease(
                                status,
                                {"cleaned_at_utc": datetime.now(timezone.utc).isoformat()}
                                if status == "cleaned"
                                else {"cleanup_reason": outcome.reason},
                            )
            except asyncio.CancelledError:
                cancelled = True
                update_lease("cleanup_required", {"cleanup_error_type": "cancelled"})
            except Exception as cleanup_error:
                note_exception(cleanup_error, "server.indexed_view_cleanup")
                update_lease("cleanup_required", {"cleanup_error_type": type(cleanup_error).__name__})

        # True when test objects of the session are still in place: no
        # statement can then run as a clean baseline.
        objects_remain = hold_mode and lease["status"] == "active"
        if failure is None and lease["status"] == "cleaned":
            try:
                step = "baseline_after"
                for measurement in measurements:
                    if (
                        measurement["pooled_baseline"] is not None
                        or measurement["slow"]
                        or measurement["decisive"]
                    ):
                        # One baseline run in the session; none after the drop.
                        continue
                    await measure_arm(
                        measurement,
                        "baseline_after",
                        measurement["baseline_runs_after"] or measurement["runs"]["baseline_before"],
                    )
            except _CallTimeCap as cap:
                failure = clock.cap_diagnostic(str(cap))
            except asyncio.CancelledError:
                cancelled = True
                failure = {"diagnostic_code": "benchmark_cancelled", "failed_step": step}
            except Exception as exc:
                note_exception(exc, "server.indexed_view_benchmark_after")
                failure = {**extract_failure_diagnostic(exc), "failed_step": step}

        equivalence: list[dict[str, Any]] = []
        parameter_results: list[dict[str, Any]] = []
        slow_baselines_kept: dict[str, Any] = {}
        baselines_before_hold_kept: dict[str, Any] = {}
        for measurement in measurements:
            name = str(measurement["parameter_case"])
            used = bool(measurement["plan_use"]) and all(measurement["plan_use"])
            pooled = measurement["pooled_baseline"]
            no_after_arm = (
                pooled is not None
                or measurement["slow"]
                or measurement["decisive"]
                or objects_remain
            )
            if noexpand:
                comparison = comparisons.get(name) or _not_compared(blocking_gaps, failure)
                equivalence.append(
                    {
                        **comparison,
                        "parameter_case": name,
                        "order_compared": compare_order,
                        "same_sql": False,
                        "basis": "the original and the view statement compared in one "
                        "snapshot while the view existed",
                        "plan_used_expected_index": used,
                    }
                )
            else:
                evidence = [
                    *([dict(pooled["result"])] if pooled is not None else ()),
                    *measurement["baseline_before_results"],
                    *measurement["candidate_results"],
                    *measurement["baseline_after_results"],
                ]
                complete = (
                    failure is None
                    and (no_after_arm or len(measurement["baseline_after_results"]) > 0)
                    and len(evidence) > len(measurement["candidate_results"]) > 0
                    and all(item.get("complete") is True for item in evidence)
                )
                status = aba_result_status(evidence, complete=complete)
                equivalence.append(
                    {
                        "parameter_case": name,
                        "status": status,
                        "order_compared": compare_order,
                        "proven_for_parameter_case": status == "match",
                        "same_sql": True,
                        "same_snapshot": False,
                        "basis": "unchanged SQL plus "
                        + (
                            "result stability against the baseline of the session "
                            "(no run followed a drop)"
                            if no_after_arm
                            else "complete A-B-A result stability"
                        ),
                        "plan_used_expected_index": used,
                    }
                )
            baseline_samples = (
                [dict(sample) for sample in pooled.get("samples") or [pooled["sample"]]]
                if pooled is not None
                else [
                    *measurement["baseline_before_samples"],
                    *measurement["baseline_after_samples"],
                ]
            )
            result_row: dict[str, Any] = {
                "parameter_case": name,
                "weight": measurement["weight"],
                "baseline": aggregate_samples(baseline_samples),
                "candidate": aggregate_samples(measurement["candidate_samples"]),
                "plan_delta": plan_delta_from_profiles(
                    measurement["baseline_before_profile"], measurement["candidate_profile"]
                ),
                "runs": dict(measurement["runs"]),
                "view_index_used": used,
                "plan_compiled_for": (
                    name if plan_reuse == "per_case" else str(measurements[0]["parameter_case"])
                ),
            }
            if measurement["runs"]["candidate"] == 1:
                result_row["candidate_single_sample"] = True
            if len(baseline_samples) == 1:
                result_row["baseline_single_sample"] = True
            if measurement["decisive"]:
                result_row["run_rule"] = _DECISIVE_RUN_RULE
                result_row["run_rule_note"] = _decisive_run_note(*measurement["decisive_seconds"])
            delta = result_row["plan_delta"]
            if pooled is not None:
                before_hold = bool(pooled.get("before_hold"))
                result_row["baseline_source"] = (
                    "session_before_hold" if before_hold else "session_single_run"
                )
                result_row["baseline_evidence_id"] = pooled["evidence_id"]
                result_row["baseline_note"] = (
                    "baseline measured earlier in this session, before the held test "
                    "objects were built. It was not run again: the statement cannot "
                    "run clean while they exist."
                    if before_hold
                    else "baseline too slow to repeat: it ran once in this session and "
                    "was not run again."
                )
                if delta["baseline"] is None and isinstance(pooled.get("plan"), Mapping):
                    delta["baseline"] = dict(pooled["plan"])
                    delta["plan_changed"] = (
                        delta["baseline"].get("plan_fingerprint")
                        != delta["candidate"]["plan_fingerprint"]
                        if delta["candidate"] is not None
                        else None
                    )
            elif measurement["baseline_before_samples"] and all(
                item.get("complete") is True for item in measurement["baseline_before_results"]
            ):
                kept_baseline = {
                    "sample": measurement["baseline_before_samples"][0],
                    "result": measurement["baseline_before_results"][0],
                    "plan": delta["baseline"],
                    "order_compared": compare_order,
                }
                if measurement["slow"]:
                    result_row["baseline_source"] = "single_run"
                    result_row["baseline_note"] = (
                        "baseline too slow to repeat (declared, or 10 seconds or more on "
                        "its first run): it ran once in the session and was not run "
                        "again after the drop."
                    )
                    if plan_reuse == "per_case":
                        slow_baselines_kept[name] = kept_baseline
                if keep_for_session:
                    # Measured with no test object in place: the baseline of every
                    # later call of the session while the view is held.
                    baselines_before_hold_kept[name] = {
                        **kept_baseline,
                        "samples": list(measurement["baseline_before_samples"]),
                        "plan_reuse": plan_reuse,
                        "case_order": case_names,
                        "sql_fingerprint": fingerprint_json({"baseline_sql": original}),
                    }
            parameter_results.append(result_row)

        objective = str(case.metadata.get("objective") or "elapsed_time")
        performance_only_scope = not direct_snapshot_supported
        if lease["status"] == "cleanup_required":
            state_name, reason = (
                "cleanup_required",
                "the test view could not be removed or its removal could not be verified",
            )
        elif failure is not None and failure.get("diagnostic_code") == "call_time_cap":
            state_name, reason = (
                "inconclusive",
                _call_cap_reason(clock, str(failure.get("failed_step")), objects_remain),
            )
        elif failure is not None:
            state_name, reason = (
                "inconclusive",
                f"the indexed-view experiment failed at step {failure.get('failed_step')}; "
                "reject this candidate and continue",
            )
        elif not all(item["plan_used_expected_index"] for item in equivalence):
            state_name, reason = (
                "inconclusive",
                (
                    "the optimizer did not match the view for every measured case; "
                    "read it WITH (NOEXPAND) (read_mode noexpand)"
                    if not noexpand
                    else "the plan did not read an index of the view in every measured case"
                ),
            )
        else:
            state_name, reason = classify_benchmark(
                parameter_results,
                equivalence,
                objective=objective,
                require_equivalence=compare_in_call if noexpand else direct_snapshot_supported,
                require_snapshot_attestation=compare_in_call,
            )
        if (
            compare_results
            and state_name in {"promising", "improved", "neutral", "regressed"}
            and any(
                item.get("status") == "mismatch" and not item.get("mismatch_may_be_a_tie")
                for item in equivalence
            )
        ):
            # Different rows on today's data: no phase and no proof gap excuses that.
            state_name, reason = (
                "equivalence_failed",
                "at least one parameter case returned different results",
            )
        performance_classification = state_name
        if phase == "finalist" and performance_only_scope and state_name in {"promising", "improved"}:
            state_name, performance_classification = "performance_only", "improved"
            reason = (
                _tested_match_reason(
                    "The view statement",
                    {str(item.get("status")) for item in equivalence},
                    tested_gaps,
                )
                if tested_gaps
                else (
                    "The view statement improved the measured objective, but its results "
                    "were not compared with the original ("
                    + str(equivalence[0].get("reason") if equivalence else "no case ran")
                    + "); the result is performance-only."
                )
            )
        proof_scope = (
            "performance_only"
            if performance_only_scope
            else "direct_snapshot"
            if noexpand
            else "aba_result_stability"
        )
        hint_profile = statement_hint_profile(measured)
        hint_profile["added_plan_forcing_hints"] = [
            hint
            for hint in hint_profile["plan_forcing_hints"]
            # NOEXPAND names the view: it is the design, not a forced plan shape.
            if hint not in statement_hint_profile(original)["plan_forcing_hints"]
            and hint.upper() != "NOEXPAND"
        ]
        view_report = {
            key: value for key, value in plan.as_dict().items() if key != "findings" or value
        }
        still_held = (
            [_held_object_report(entry, lease) for entry in lease_entries()]
            if objects_remain
            else []
        )
        view_object = _held_object_report(view_entry)
        time_spent = clock.report()
        test_objects: dict[str, Any] = {
            "reused": [view_object] if held_view is not None and view_verified else [],
            "built": [view_object] if held_view is None and view_verified else [],
            "still_held": still_held,
            "hold_expires_at_utc": lease["expires_at_utc"] if still_held else None,
        }
        if still_held:
            test_objects["note"] = (
                "These objects stay in place for this session. A later benchmark call "
                "reuses each one whose definition matches; compare_query_results and "
                "explain_query can read them; finalize_tuning_session drops them, and "
                "so does the lease when it expires."
            )
        evidence_envelope = self.performance_store.create_evidence(
            EvidenceEnvelopeV1(
                source="azure-sql-mcp",
                kind=f"index_{phase}",
                query_fingerprint=case.query_fingerprint,
                database_fingerprint=case.database_fingerprint,
                parameters_fingerprint=fingerprint_json(
                    [
                        {"name": bound["parameter_case"], "parameter_case_fingerprint": bound["fingerprint"]}
                        for bound in bound_cases
                    ]
                ),
                observed_execution_count=measured_executions,
                metrics={
                    "classification": state_name,
                    "performance_classification": performance_classification,
                    "objective": objective,
                    "decision_basis": COMPARISON_DECISION_BASIS,
                    "parameter_results": parameter_results,
                    **({"slow_baselines": slow_baselines_kept} if slow_baselines_kept else {}),
                    **(
                        {"baselines_before_hold": baselines_before_hold_kept}
                        if baselines_before_hold_kept
                        else {}
                    ),
                },
                metadata={
                    "session_id": session_id,
                    "candidate_id": candidate_id,
                    "experiment": "indexed_view",
                    "test_objects": test_objects,
                    "time_spent": time_spent,
                    "verification": verification,
                    "read_mode": read_mode,
                    "lease_id": lease_id,
                    "lease_status": lease["status"],
                    "index_definition_fingerprint": plan.definition_fingerprint,
                    "view": {"view": view_report["view"], "built": built},
                    "execution_reservation_id": reservation["reservation_id"],
                    "execution_protocol": {
                        "measured_runs": runs,
                        "expected_executions": requested_executions,
                        **(
                            {"adaptive_expected_executions": measured_executions}
                            if failure is None
                            else {}
                        ),
                    },
                    "equivalence": equivalence,
                    "equivalence_preflight": equivalence_preflight,
                    "proof_scope": proof_scope,
                    "equivalence_deferred": performance_only_scope,
                    "rewrite_requirements": rewrite_requirements(original, measured) if noexpand else [],
                    "hint_profile": hint_profile,
                    "phase": phase,
                    "reason": reason,
                    "failure_diagnostic": failure,
                },
            ),
            idempotency_key=evidence_key,
            request_fingerprint=request_fingerprint,
        )
        screening_passed = phase == "screening" and state_name in {"promising", "improved"}
        _session, updated = self.tuning_sessions.record_candidate_result(
            session_id,
            candidate_id,
            state="screening" if screening_passed else state_name,
            screen_runs=runs if phase == "screening" else 0,
            finalist_runs=0 if phase == "screening" else runs,
            parameter_cases=len(cases),
            executions=measured_executions,
            evidence_ids=(evidence_envelope.evidence_id,),
            failure_code=(
                state_name
                if state_name in {"inconclusive", "cleanup_required", "equivalence_failed"}
                else None
            ),
            idempotency_key=operation_key,
        )
        (
            self.performance_store.complete_execution_attempts
            if measured_executions
            else self.performance_store.release_execution_attempts
        )(
            reservation["reservation_id"],
            dispatched_attempt_count=measured_executions,
            owner_reference=execution_owner,
            expected_version=reservation["version"],
        )
        result: dict[str, Any] = {
            "session_id": session_id,
            "candidate_id": candidate_id,
            "experiment": "indexed_view",
            "read_mode": read_mode,
            "plan_reuse": plan_reuse,
            "classification": state_name,
            "performance_classification": performance_classification,
            "objective": objective,
            "decision_basis": COMPARISON_DECISION_BASIS,
            "durable_state": updated.state,
            "reason": reason,
            "phase": phase,
            "executions": measured_executions,
            "parameter_results": parameter_results,
            "equivalence": equivalence,
            "equivalence_preflight": equivalence_preflight,
            "equivalence_deferred": performance_only_scope,
            "proof_scope": proof_scope,
            "evidence_id": evidence_envelope.evidence_id,
            "lease": self._public_index_lease(lease),
            "view": view_report,
            "built": built,
            "verification": verification,
            "test_objects": test_objects,
            "time_spent": time_spent,
            "session_continues": lease["status"] != "cleanup_required",
            "plan_controls_not_applied": True,
        }
        if cleanup["dropped"]:
            result["cleanup_before_run"] = {"dropped": cleanup["dropped"]}
        if build_retries:
            result["build_retries"] = build_retries
        if failure is not None and failure.get("diagnostic_code") == "call_time_cap":
            result["partial"] = True
        if failure is not None:
            result["failure_diagnostic"] = failure
        if lease["status"] == "cleanup_required":
            result["remediation"] = "\n".join(plan.teardown_statements())
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def _attach_learning_terminal_link(
        self,
        tool_name: str,
        database_name: str,
        decision_id: str,
        payload: Any,
    ) -> Any:
        """Attach a redacted, idempotent terminal outcome without changing the operation result."""

        if self.learning_service is None:
            status = "unavailable"
            link_id = None
        else:
            try:
                decision = self.learning_service.store.get_decision(decision_id)
                self._validate_learning_contract_scope(decision.scope, database_name)
                runtime = self._runtime_status()
                if decision.runtime_fingerprint != runtime["runtime_fingerprint"]:
                    raise ValueError("Decision process runtime scope is stale.")
                if (
                    decision.runtime_compatibility_fingerprint
                    != runtime["runtime_compatibility_fingerprint"]
                ):
                    raise ValueError("Decision runtime compatibility scope is stale.")

                outcome: dict[str, Any] = {"outcome": "succeeded"}
                evidence_refs: list[str] = []
                if isinstance(payload, Mapping):
                    for key in (
                        "status",
                        "decision",
                        "classification",
                        "performance_classification",
                        "durable_state",
                        "phase",
                        "confirmed",
                        "idempotent",
                        "reconciliation_required",
                        "executions",
                        "case_id",
                        "session_id",
                        "candidate_id",
                        "intent_id",
                    ):
                        value = payload.get(key)
                        if isinstance(value, (str, int, float, bool)) or value is None:
                            outcome[key] = value
                    intent = payload.get("intent")
                    if isinstance(intent, Mapping) and isinstance(intent.get("status"), str):
                        outcome["intent_status"] = intent["status"]
                    for key in (
                        "evidence_id",
                        "evidence_ids",
                        "evidence_refs",
                        "resolution_evidence_refs",
                    ):
                        value = payload.get(key)
                        values = [value] if isinstance(value, str) else value
                        if isinstance(values, (list, tuple)):
                            evidence_refs.extend(
                                ref
                                for ref in values
                                if isinstance(ref, str) and ref.startswith("evidence-")
                            )
                evidence_refs = list(dict.fromkeys(evidence_refs))
                self._validate_learning_evidence_refs(
                    evidence_refs,
                    database_name=database_name,
                )
                response_fingerprint = fingerprint_json(
                    {
                        "tool": tool_name,
                        "decision_id": decision_id,
                        "outcome": outcome,
                        "evidence_refs": evidence_refs,
                    }
                )
                link = self.learning_service.record_terminal_link(
                    decision_id=decision_id,
                    source_tool=tool_name,
                    database_fingerprint=database_fingerprint(
                        database_name, self.config.server
                    ),
                    scope=self._current_learning_scope(database_name),
                    outcome_summary=outcome,
                    evidence_refs=tuple(evidence_refs),
                    response_fingerprint=response_fingerprint,
                    idempotency_key=(
                        f"terminal-link:{tool_name}:{decision_id}:{response_fingerprint}"
                    ),
                )
                status = "linked"
                link_id = link.get("link_id")
            except Exception as exc:
                note_exception(exc, "server.learning_terminal_link")
                logger.warning(
                    "Learning terminal linkage failed after successful tool operation.",
                    extra={
                        "tool_name": tool_name,
                        "decision_id": decision_id,
                        "error_type": type(exc).__name__,
                        "error": sanitize_error_message(str(exc)),
                    },
                )
                status = "failed"
                link_id = None
        if isinstance(payload, dict):
            result = dict(payload)
        else:
            result = {"result": payload}
        result["learning_link_status"] = status
        result["terminal_link_id"] = link_id
        return result

    async def _run_tool(
        self,
        tool_name: str,
        database_name: str | None,
        callback,
        *,
        deadline_provider: Callable[[], str | None] | None = None,
        decision_id: str | None = None,
    ) -> ResponseType:
        requested_database = (database_name or self.config.default_database).strip()
        correlation_id = str(uuid.uuid4())
        started_at = time.monotonic()
        timeout_seconds = self.config.tool_timeout_seconds
        try:
            resolved_database = self.config.validate_database_name(database_name)
            if tool_name in _CATALOG_READ_TOOLS:
                self.database_policy.require_read(resolved_database)
            timeout_seconds = self._timeout_for_tool(
                tool_name,
                resolved_database,
            )
            if deadline_provider is not None:
                deadline_at_utc = deadline_provider()
                if deadline_at_utc:
                    deadline = datetime.fromisoformat(
                        deadline_at_utc.replace("Z", "+00:00")
                    )
                    if deadline.tzinfo is None:
                        deadline = deadline.replace(tzinfo=timezone.utc)
                    remaining = (
                        deadline.astimezone(timezone.utc)
                        - datetime.now(timezone.utc)
                    ).total_seconds()
                    if remaining <= 0:
                        self._raise_tool_error(
                            "session_expired",
                            (
                                "The tuning session deadline has passed. Retrieve "
                                "durable results or finalize an already measured "
                                "candidate; no new SQL work was started."
                            ),
                        )
                    timeout_seconds = min(timeout_seconds, max(0.0, remaining))
            logger.info(
                "Running tool",
                extra={
                    "tool_name": tool_name,
                    "database_name": resolved_database,
                    "correlation_id": correlation_id,
                },
            )
            # The index benchmark drops its temporary index after a timeout;
            # a passed statement deadline would refuse that DROP.
            with statement_deadline(
                None
                if tool_name == "benchmark_index_candidate"
                else timeout_seconds
            ):
                payload = await asyncio.wait_for(
                    callback(resolved_database),
                    timeout=timeout_seconds,
                )
            if decision_id is not None:
                payload = await self._attach_learning_terminal_link(
                    tool_name,
                    resolved_database,
                    decision_id,
                    payload,
                )
            duration_ms = self._duration_ms(started_at)
            row_count = self._estimate_row_count(payload)
            log_extra: dict[str, Any] = {
                "tool_name": tool_name,
                "database_name": resolved_database,
                "correlation_id": correlation_id,
                "duration_ms": duration_ms,
                "row_count": row_count,
            }
            if row_count > 10_000:
                logger.warning(
                    "Tool returned large result set (%d rows)",
                    row_count,
                    extra=log_extra,
                )
            else:
                logger.info("Tool completed", extra=log_extra)
            payload = add_tool_headline(tool_name, payload)
            if isinstance(payload, dict) and "headline" in payload:
                # The verdict and its numbers first; the detail follows.
                payload = {"headline": payload["headline"], **payload}
            return self._format_response(payload)
        except ToolError:
            raise
        except asyncio.TimeoutError:
            duration_ms = self._duration_ms(started_at)
            logger.warning(
                "Tool timed out",
                extra={
                    "tool_name": tool_name,
                    "database_name": requested_database,
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                },
            )
            self._raise_tool_error(
                "timeout",
                f"Tool '{tool_name}' timed out after {timeout_seconds}s.",
            )
        except Exception as exc:
            duration_ms = self._duration_ms(started_at)
            sanitized_error = sanitize_error_message(str(exc))
            failure_diagnostic = _tool_failure_diagnostic(exc)
            logger.error(
                "Tool failed",
                extra={
                    "tool_name": tool_name,
                    "database_name": requested_database,
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                    "error_type": type(exc).__name__,
                    "error": sanitized_error,
                },
            )
            # The log stays fully redacted; the caller may see the identifier
            # an engine compile error names, never a row value.
            client_error = sanitize_error_message(str(exc), keep_identifiers=True)
            not_found = not_found_details(exc)
            session_failure = session_failure_diagnostic(exc)
            if not_found is not None:
                self._raise_tool_error(
                    "not_found",
                    not_found["message"],
                    details={
                        "failure_diagnostic": {
                            "diagnostic_code": "not_found",
                            "id_kind": not_found["id_kind"],
                            "retryable": False,
                        }
                    },
                )
            if session_failure is not None:
                self._raise_tool_error(
                    session_failure["diagnostic_code"],
                    client_error,
                    details={
                        "failure_diagnostic": {**failure_diagnostic, "retryable": False},
                        "next_action": session_failure["message"],
                    },
                )
            self._raise_tool_error(
                "tool_error",
                client_error,
                details={"failure_diagnostic": failure_diagnostic},
            )

    def _timeout_for_tool(
        self,
        tool_name: str,
        database_name: str | None = None,
    ) -> float:
        if tool_name in _SESSION_WORKFLOW_TOOLS:
            return self._session_workflow_timeout(database_name)
        if tool_name in _EVIDENCE_WORKFLOW_TOOLS:
            return self._evidence_workflow_timeout()
        if tool_name == "diagnose_database":
            # Sources run concurrently; allow a wait sample plus the slowest read.
            return max(
                float(self.config.tool_timeout_seconds),
                self.config.query_timeout_seconds * 3 + 45.0,
            )
        if tool_name == "get_wait_stats":
            # Two snapshots plus an optional sample of up to 30 seconds.
            return max(
                float(self.config.tool_timeout_seconds),
                self.config.query_timeout_seconds * 2 + 45.0,
            )
        if tool_name == "capture_index_review_snapshot":
            # Catalog, Query Store and DMV reads, per-candidate sizing, then the
            # chunked insert, all inside one history transaction.
            return max(
                float(self.config.tool_timeout_seconds),
                self.config.query_timeout_seconds * 6 + 60.0,
            )
        if tool_name == "review_workload_indexes":
            # About eight bounded catalog and Query Store reads run in sequence.
            return max(
                float(self.config.tool_timeout_seconds),
                self.config.query_timeout_seconds * 8 + 60.0,
            )
        return self.config.tool_timeout_seconds

    def _evidence_workflow_timeout(self) -> float:
        """Budget concurrent collectors followed by one optional profile query."""

        return max(
            self.config.tool_timeout_seconds,
            self.performance_workflows.collector_timeout_seconds
            + self.config.query_timeout_seconds
            + 60,
        )

    def _session_workflow_timeout(
        self,
        database_name: str | None,
    ) -> float:
        per_request_executions = (
            self.database_policy.policy_for(
                database_name
            ).max_benchmark_executions
            if database_name
            else 0
        )
        benchmark_timeout = (
            self.config.query_timeout_seconds
            * max(1, per_request_executions)
            + 5 * 60
        )
        return max(
            self.config.tool_timeout_seconds,
            21 * 60,
            benchmark_timeout,
        )

    def _require_tuning_session_time(self, session_id: str) -> float:
        """Fence benchmark work against terminal state and the durable deadline."""

        try:
            session = self.tuning_sessions.require_session_status(
                session_id,
                allowed={"created", "screening", "finalist_validation"},
            )
        except InvalidTransitionError as exc:
            raise InvalidTransitionError(
                f"{exc} Retrieve committed results with get_tuning_session; "
                "terminal sessions cannot benchmark or replay."
            ) from None
        if not session.deadline_at_utc:
            return float("inf")
        deadline = datetime.fromisoformat(
            session.deadline_at_utc.replace("Z", "+00:00")
        )
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        remaining = (
            deadline.astimezone(timezone.utc) - datetime.now(timezone.utc)
        ).total_seconds()
        if remaining <= 0:
            raise TimeoutError("The tuning session time budget has expired.")
        return remaining

    async def _resolve_canonical_table_identity(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
    ) -> _TestIndexTable:
        """Resolve catalog spelling and the facts a test index build depends on.

        One read, before any table DDL is generated: the canonical names,
        whether the table has a clustered columnstore index, and the partition
        scheme and partitioning column of a partitioned table.
        """

        rows = await self.executor.fetch_all(
            database_name,
            _TEST_INDEX_TABLE_SQL,
            params=[schema_name, table_name],
            max_rows=2,
            sql_origin="server_built",
        )
        if len(rows) != 1:
            raise ValueError(
                "The target table could not be resolved to one canonical Azure SQL "
                "catalog object; no index DDL was dispatched."
            )
        resolved_schema = self._validate_plain_identifier(
            str(rows[0].get("schema_name") or ""),
            "catalog schema_name",
        )
        resolved_table = self._validate_plain_identifier(
            str(rows[0].get("table_name") or ""),
            "catalog table_name",
        )
        partition_scheme = rows[0].get("partition_scheme_name")
        partition_column = rows[0].get("partitioning_column")
        if partition_scheme is not None and partition_column is None:
            raise ValueError(
                "The partitioning column of the target table could not be read; "
                "no index DDL was dispatched."
            )
        partition_function = rows[0].get("partition_function_name")
        return _TestIndexTable(
            schema=resolved_schema,
            table=resolved_table,
            has_clustered_columnstore_index=bool(rows[0].get("has_clustered_columnstore_index")),
            partition_scheme=str(partition_scheme) if partition_scheme is not None else None,
            partition_function=(
                str(partition_function) if partition_function is not None else None
            ),
            partition_column=str(partition_column) if partition_column is not None else None,
        )

    async def _run_database_pair_tool(
        self,
        tool_name: str,
        source_database: str,
        target_database: str,
        callback,
    ) -> ResponseType:
        requested_label = f"{source_database.strip()} -> {target_database.strip()}"
        correlation_id = str(uuid.uuid4())
        started_at = time.monotonic()
        try:
            resolved_source = self.config.validate_database_name(source_database)
            resolved_target = self.config.validate_database_name(target_database)
            if tool_name in _CATALOG_PAIR_TOOLS:
                self.database_policy.require_read(resolved_source)
                self.database_policy.require_read(resolved_target)
            database_label = f"{resolved_source} -> {resolved_target}"
            logger.info(
                "Running tool",
                extra={
                    "tool_name": tool_name,
                    "database_name": database_label,
                    "correlation_id": correlation_id,
                },
            )
            with statement_deadline(self.config.tool_timeout_seconds):
                payload = await asyncio.wait_for(
                    callback(resolved_source, resolved_target),
                    timeout=self.config.tool_timeout_seconds,
                )
            duration_ms = self._duration_ms(started_at)
            logger.info(
                "Tool completed",
                extra={
                    "tool_name": tool_name,
                    "database_name": database_label,
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                },
            )
            return self._format_response(payload)
        except ToolError:
            raise
        except asyncio.TimeoutError:
            duration_ms = self._duration_ms(started_at)
            logger.warning(
                "Tool timed out",
                extra={
                    "tool_name": tool_name,
                    "database_name": requested_label,
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                },
            )
            self._raise_tool_error(
                "timeout",
                f"Tool '{tool_name}' timed out after {self.config.tool_timeout_seconds}s.",
            )
        except Exception as exc:
            duration_ms = self._duration_ms(started_at)
            sanitized_error = sanitize_error_message(str(exc))
            logger.error(
                "Tool failed",
                extra={
                    "tool_name": tool_name,
                    "database_name": requested_label,
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                    "error_type": type(exc).__name__,
                    "error": sanitized_error,
                },
            )
            self._raise_tool_error(
                "tool_error",
                sanitize_error_message(str(exc), keep_identifiers=True),
                details={"failure_diagnostic": _tool_failure_diagnostic(exc)},
            )

    async def _execute_safe_sql(self, database_name: str, sql: str) -> dict[str, Any]:
        validated = self.validator.validate_read_only(sql)
        # Fetch at most row_limit + 1 rows to detect truncation without loading entire result
        fetch_limit = self.config.row_limit + 1
        rows = await self.executor.fetch_all(
            database_name,
            validated.execution_sql,
            max_rows=fetch_limit,
            sql_origin="caller_supplied",
            read_only_envelope=True,
        )
        return self._truncate_rows(
            {
                "database_name": database_name,
                "normalized_sql": validated.normalized_sql,
                "rows": rows,
            }
        )

    async def _execute_unrestricted_sql(
        self,
        database_name: str,
        sql: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        # Fetch at most row_limit + 1 rows per result set to prevent OOM
        fetch_limit = self.config.row_limit + 1
        action = AdminAction(
            tool_name="execute_tsql_unrestricted",
            database_name=database_name,
            action_type="query",
            sql=sql,
        )
        payload = await self.admin_policy.execute(
            action,
            self.executor,
            dry_run=dry_run,
            max_rows=fetch_limit,
        )
        for result_set in payload.get("result_sets", []):
            if isinstance(result_set, dict):
                self._truncate_rows(result_set)
        return payload

    async def _rebuild_index(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        index_name: str,
        operation: str,
        online: bool,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        op = operation.strip().upper()
        if op not in ("REBUILD", "REORGANIZE"):
            raise ValueError("operation must be REBUILD or REORGANIZE")
        quoted_schema = self._quote_identifier(schema_name)
        quoted_table = self._quote_identifier(table_name)
        quoted_index = self._quote_identifier(index_name)
        qualified_index = f"{quoted_schema}.{quoted_table}.{quoted_index}"
        if op == "REBUILD":
            online_clause = " WITH (ONLINE = ON)" if online else ""
            sql = f"ALTER INDEX {quoted_index} ON {quoted_schema}.{quoted_table} REBUILD{online_clause}"
        else:
            sql = f"ALTER INDEX {quoted_index} ON {quoted_schema}.{quoted_table} REORGANIZE"
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="rebuild_index",
                database_name=database_name,
                action_type="maintenance",
                sql=sql,
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "operation": op,
            "index": qualified_index,
            "online": online if op == "REBUILD" else None,
        })
        return payload

    async def _update_statistics(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        stat_name: str | None,
        sample_percent: int | None,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        quoted_schema = self._quote_identifier(schema_name)
        quoted_table_name = self._quote_identifier(table_name)
        qualified_table = f"{quoted_schema}.{quoted_table_name}"
        if stat_name:
            target = f"{qualified_table} {self._quote_identifier(stat_name)}"
        else:
            target = qualified_table
        sql = f"UPDATE STATISTICS {target}"
        if sample_percent is not None:
            if not 1 <= sample_percent <= 100:
                raise ValueError("sample_percent must be between 1 and 100")
            sql += f" WITH SAMPLE {sample_percent} PERCENT"
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="update_statistics",
                database_name=database_name,
                action_type="maintenance",
                sql=sql,
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "table": qualified_table,
            "statistic": stat_name or "(all)",
            "sample_percent": sample_percent,
        })
        return payload

    async def _force_query_plan(
        self,
        database_name: str,
        query_id: int,
        plan_id: int,
        unforce: bool,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if not dry_run:
            raise PermissionError(
                "Direct force/unforce is preview-only; use the prepared plan-action workflow."
            )
        if query_id <= 0:
            raise ValueError("query_id must be greater than 0.")
        if plan_id <= 0:
            raise ValueError("plan_id must be greater than 0.")
        if unforce:
            sql = "EXEC sp_query_store_unforce_plan @query_id = ?, @plan_id = ?"
            rollback_sql = (
                "EXEC sp_query_store_force_plan "
                f"@query_id = {int(query_id)}, @plan_id = {int(plan_id)}"
            )
        else:
            sql = "EXEC sp_query_store_force_plan @query_id = ?, @plan_id = ?"
            rollback_sql = (
                "EXEC sp_query_store_unforce_plan "
                f"@query_id = {int(query_id)}, @plan_id = {int(plan_id)}"
            )
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="force_query_plan",
                database_name=database_name,
                action_type="query_store",
                sql=sql,
                params=(int(query_id), int(plan_id)),
                rollback_sql=rollback_sql,
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "query_id": query_id,
            "plan_id": plan_id,
            "action": "unforced" if unforce else "forced",
        })
        return payload

    async def _set_query_store_hints(
        self,
        database_name: str,
        query_id: int,
        query_hints: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if not dry_run:
            raise PermissionError(
                "Direct Query Store hint changes are preview-only; use the prepared workflow."
            )
        if query_id <= 0:
            raise ValueError("query_id must be greater than 0.")
        validated_hints = validate_query_hints(query_hints)
        # The driver binds str parameters as varchar, but the proc requires
        # nvarchar — route the hints through an nvarchar variable.
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="set_query_store_hints",
                database_name=database_name,
                action_type="query_store",
                sql=(
                    "DECLARE @hints nvarchar(max) = ?;\n"
                    "EXEC sys.sp_query_store_set_hints @query_id = ?, @query_hints = @hints"
                ),
                params=(validated_hints, int(query_id)),
                rollback_sql=(
                    f"EXEC sys.sp_query_store_clear_hints @query_id = {int(query_id)}"
                ),
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "query_id": query_id,
            "query_hints": validated_hints,
            "action": "hints_set",
        })
        return payload

    async def _clear_query_store_hints(
        self,
        database_name: str,
        query_id: int,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if not dry_run:
            raise PermissionError(
                "Direct Query Store hint changes are preview-only; use the prepared workflow."
            )
        if query_id <= 0:
            raise ValueError("query_id must be greater than 0.")
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="clear_query_store_hints",
                database_name=database_name,
                action_type="query_store",
                sql="EXEC sys.sp_query_store_clear_hints @query_id = ?",
                params=(int(query_id),),
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "query_id": query_id,
            "action": "hints_cleared",
        })
        return payload

    async def _create_test_index(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        index_name: str,
        key_columns: list[str],
        include_columns: list[str] | None,
        online: bool,
        dry_run: bool = True,
        workflow_managed: bool = False,
        filter_definition: str | None = None,
        is_unique: bool = False,
        idempotency_key: str | None = None,
        ownership_proof: str | None = None,
        data_compression: str | None = None,
        table_facts: _TestIndexTable | None = None,
        index_kind: str = INDEX_KIND_ROWSTORE,
    ) -> dict[str, Any]:
        """Preview or, for the leased workflow, build one test index.

        ``table_facts`` (``_resolve_canonical_table_identity``) makes the DDL
        fit the table: no ONLINE option on a table with a clustered columnstore index,
        and the partition scheme of a partitioned table.
        """

        if not dry_run and not workflow_managed:
            raise PermissionError(
                "Direct test-index creation is preview-only; use benchmark_index_candidate."
            )
        if not dry_run:
            self._require_test_index_database(database_name)
        policy = self.database_policy.policy_for(database_name)
        allowed = self._allowed_test_index_prefixes(policy)
        requested_name = (index_name or "").strip()
        # A preview never fails on the name: a name outside the allowed
        # prefixes is moved under the policy prefix. The leased build keeps
        # the strict check, because its names are generated.
        renamed = not workflow_managed and not (
            _PLAIN_IDENTIFIER.match(requested_name)
            and requested_name.upper().startswith(tuple(prefix.upper() for prefix in allowed))
        )
        index = "" if renamed else self._validate_test_index_name(index_name, allowed)
        schema = self._validate_plain_identifier(schema_name, "schema_name")
        table = self._validate_plain_identifier(table_name, "table_name")
        if not key_columns:
            raise ValueError("key_columns must contain at least one column.")
        online = _test_index_build_online(policy, online, table_facts)

        candidate = IndexCandidate(
            schema=schema,
            table=table,
            key_columns=tuple(key_columns),
            include_columns=tuple(include_columns or ()),
            filter_definition=filter_definition,
            is_unique=is_unique,
            index_name=index,
            compression=data_compression,
            **(table_facts.placement() if table_facts is not None else {}),
            index_kind=index_kind,
        )
        if renamed:
            prefix = policy.test_index_name_prefixes[0]
            usable = _usable_name_text(requested_name)
            index = (
                f"{prefix}{usable}"[:MAX_GENERATED_TEST_INDEX_NAME_LENGTH].rstrip("_")
                if usable
                else generated_test_index_name(prefix, candidate)
            )
            candidate = replace(candidate, index_name=index)
        # Online builds wait at low priority and abort themselves. An offline
        # build is refused unless the database policy allows it.
        sql = build_test_index_statement(
            candidate,
            online=online,
            allow_offline=policy.allow_offline_test_index,
            maxdop=policy.test_index_maxdop or None,
        )
        rollback_sql = f"DROP INDEX [{index}] ON [{schema}].[{table}];"
        if workflow_managed and not idempotency_key:
            raise ValueError("Workflow-managed index creation requires an idempotency key.")
        if workflow_managed and (
            ownership_proof is None
            or not _INDEX_OWNER_PROOF.fullmatch(ownership_proof)
        ):
            raise ValueError(
                "Workflow-managed index creation requires a private lease-owner token."
            )
        params: tuple[Any, ...] = ()
        rollback_params: tuple[Any, ...] = ()
        if workflow_managed:
            definition_fingerprint = candidate.definition_fingerprint
            params = (ownership_proof, definition_fingerprint)
            rollback_params = (ownership_proof, definition_fingerprint, None)
            sql = (
                "SET XACT_ABORT ON;\n"
                "BEGIN TRANSACTION;\n"
                f"{sql.rstrip().rstrip(';')};\n"
                "EXEC sys.sp_addextendedproperty "
                f"@name = N'{TEST_INDEX_OWNER_PROPERTY}', "
                "@value = ?, "
                "@level0type = N'SCHEMA', "
                f"@level0name = N'{schema}', "
                "@level1type = N'TABLE', "
                f"@level1name = N'{table}', "
                "@level2type = N'INDEX', "
                f"@level2name = N'{index}';\n"
                "EXEC sys.sp_addextendedproperty "
                f"@name = N'{TEST_INDEX_DEFINITION_PROPERTY}', "
                "@value = ?, "
                "@level0type = N'SCHEMA', "
                f"@level0name = N'{schema}', "
                "@level1type = N'TABLE', "
                f"@level1name = N'{table}', "
                "@level2type = N'INDEX', "
                f"@level2name = N'{index}';\n"
                "COMMIT TRANSACTION;"
            )
            rollback_sql = self._build_guarded_index_drop_sql(schema, table, index)

        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="create_test_index",
                database_name=database_name,
                action_type="test_index",
                sql=sql,
                params=params,
                rollback_sql=rollback_sql,
                rollback_params=rollback_params,
                trusted_generated=True,
                reviewed_intent=workflow_managed,
                idempotency_key=idempotency_key,
                exactly_once=workflow_managed,
                policy_verified=workflow_managed,
                non_production=workflow_managed,
                verification_required=workflow_managed,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "index": f"[{schema}].[{table}].[{index}]",
            "index_name": index,
            "key_columns": key_columns,
            "include_columns": include_columns or [],
            "filter_definition": filter_definition,
            "is_unique": is_unique,
            "online": online,
            "action": "test_index_created",
            "note": "Disposable test index — drop it with drop_test_index after measuring.",
        })
        if table_facts is not None and table_facts.has_clustered_columnstore_index:
            payload["offline_build"] = _offline_build_report([table_facts])
        if renamed:
            payload["index_name_note"] = (
                "The requested index_name is not under an allowed test index prefix ("
                + ", ".join(allowed)
                + f"). The name {index} was used."
            )
        return payload

    async def _drop_test_index(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        index_name: str,
        dry_run: bool = True,
        workflow_managed: bool = False,
        idempotency_key: str | None = None,
        ownership_proof: str | None = None,
        expected_definition_fingerprint: str | None = None,
        expected_index_id: int | None = None,
    ) -> dict[str, Any]:
        if not dry_run and not workflow_managed:
            raise PermissionError(
                "Direct test-index removal is preview-only; use benchmark_index_candidate."
            )
        if not dry_run:
            self._require_test_index_database(database_name)
        index = self._validate_test_index_name(
            index_name,
            self._allowed_test_index_prefixes(self.database_policy.policy_for(database_name)),
        )
        schema = self._validate_plain_identifier(schema_name, "schema_name")
        table = self._validate_plain_identifier(table_name, "table_name")
        sql = f"DROP INDEX [{index}] ON [{schema}].[{table}];"
        if workflow_managed and not idempotency_key:
            raise ValueError("Workflow-managed index cleanup requires an idempotency key.")
        params: tuple[Any, ...] = ()
        if workflow_managed:
            if ownership_proof is None or not _INDEX_OWNER_PROOF.fullmatch(ownership_proof):
                raise ValueError(
                    "Workflow-managed index cleanup requires a private lease-owner token."
                )
            if expected_definition_fingerprint is not None and not re.fullmatch(
                r"[0-9a-fA-F]{64}", expected_definition_fingerprint
            ):
                raise ValueError("Expected index definition fingerprint is invalid.")
            if expected_index_id is not None and expected_index_id <= 0:
                raise ValueError("Expected index_id must be greater than 0.")
            params = (
                ownership_proof,
                expected_definition_fingerprint,
                expected_index_id,
            )
            sql = self._build_guarded_index_drop_sql(schema, table, index)
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="drop_test_index",
                database_name=database_name,
                action_type="test_index",
                sql=sql,
                params=params,
                trusted_generated=True,
                reviewed_intent=workflow_managed,
                idempotency_key=idempotency_key,
                exactly_once=workflow_managed,
                policy_verified=workflow_managed,
                non_production=workflow_managed,
                verification_required=workflow_managed,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "index": f"[{schema}].[{table}].[{index}]",
            "action": "test_index_dropped",
        })
        return payload

    def _require_test_index_database(self, database_name: str) -> None:
        """Require the named sandbox profile and local database policy."""
        if self.config.profile != McpProfile.SANDBOX:
            raise PermissionError("Temporary index DDL requires the sandbox MCP profile.")
        policy = self.database_policy.require(database_name)
        if not policy.allow_test_indexes:
            raise PermissionError("Database policy does not permit temporary indexes.")
        if not policy.is_non_production:
            raise PermissionError(
                "Temporary indexes need a policy environment that is a known "
                "non-production label; this entry is classed "
                f"{policy.environment_class}."
            )

    @staticmethod
    def _build_guarded_index_drop_sql(
        schema: str,
        table: str,
        index: str,
    ) -> str:
        return (
            "SET XACT_ABORT ON;\n"
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;\n"
            "BEGIN TRY\n"
            "    BEGIN TRANSACTION;\n"
            f"    DECLARE @object_id int = OBJECT_ID(N'{schema}.{table}', N'U');\n"
            "    IF @object_id IS NULL\n"
            "        THROW 50002, 'Temporary index target table is missing.', 1;\n"
            "    DECLARE @table_lock_probe bit;\n"
            + "".join(
                f"    {line}\n"
                for line in bounded_table_lock_probe_sql(schema, table).splitlines()
            )
            + ""
            "    DECLARE @owner_marker nvarchar(4000) = ?;\n"
            "    DECLARE @expected_definition nvarchar(4000) = ?;\n"
            "    DECLARE @expected_index_id int = ?;\n"
            "    DECLARE @index_id int;\n"
            "    SELECT @index_id = i.index_id\n"
            "    FROM sys.indexes AS i WITH (UPDLOCK, HOLDLOCK)\n"
            "    INNER JOIN sys.tables AS t WITH (UPDLOCK, HOLDLOCK)\n"
            "        ON t.object_id = i.object_id\n"
            "    INNER JOIN sys.schemas AS s WITH (UPDLOCK, HOLDLOCK)\n"
            "        ON s.schema_id = t.schema_id\n"
            f"    WHERE i.object_id = @object_id AND s.name = N'{schema}'\n"
            f"      AND t.name = N'{table}' AND i.name = N'{index}';\n"
            "    IF @index_id IS NULL\n"
            "        THROW 50003, 'Temporary index object is missing.', 1;\n"
            "    IF @expected_index_id IS NOT NULL AND @index_id <> @expected_index_id\n"
            "        THROW 50004, 'Temporary index object identity mismatch.', 1;\n"
            "    IF NOT EXISTS (\n"
            "        SELECT 1\n"
            "        FROM sys.extended_properties AS ep WITH (UPDLOCK, HOLDLOCK)\n"
            "        WHERE ep.class = 7\n"
            "          AND ep.major_id = @object_id\n"
            "          AND ep.minor_id = @index_id\n"
            f"          AND ep.name = N'{TEST_INDEX_OWNER_PROPERTY}'\n"
            "          AND CONVERT(nvarchar(4000), ep.value) COLLATE Latin1_General_100_BIN2\n"
            "              = @owner_marker COLLATE Latin1_General_100_BIN2\n"
            "    )\n"
            "        THROW 50001, 'Temporary index ownership marker mismatch.', 1;\n"
            "    IF @expected_definition IS NOT NULL AND NOT EXISTS (\n"
            "        SELECT 1\n"
            "        FROM sys.extended_properties AS ep WITH (UPDLOCK, HOLDLOCK)\n"
            "        WHERE ep.class = 7\n"
            "          AND ep.major_id = @object_id\n"
            "          AND ep.minor_id = @index_id\n"
            f"          AND ep.name = N'{TEST_INDEX_DEFINITION_PROPERTY}'\n"
            "          AND CONVERT(nvarchar(4000), ep.value) COLLATE Latin1_General_100_BIN2\n"
            "              = @expected_definition COLLATE Latin1_General_100_BIN2\n"
            "    )\n"
            "        THROW 50005, 'Temporary index definition mismatch.', 1;\n"
            f"    DROP INDEX [{index}] ON [{schema}].[{table}];\n"
            "    COMMIT TRANSACTION;\n"
            "    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\n"
            "END TRY\n"
            "BEGIN CATCH\n"
            "    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;\n"
            "    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\n"
            "    THROW;\n"
            "END CATCH;"
        )

    @staticmethod
    def _public_index_lease(lease: Mapping[str, Any]) -> dict[str, Any]:
        public = dict(lease)
        public.pop("owner_token", None)
        public.pop("fencing_token", None)
        metadata = public.get("metadata")
        if isinstance(metadata, Mapping):
            safe_metadata = dict(metadata)
            safe_metadata.pop("lease_owner_fence", None)
            public["metadata"] = safe_metadata
        return public

    @staticmethod
    def _validate_plain_identifier(identifier: str, label: str) -> str:
        value = (identifier or "").strip()
        if not _PLAIN_IDENTIFIER.match(value):
            raise ValueError(
                f"{label} must be a plain identifier (letters, digits, underscores); got {value!r}."
            )
        return value

    @classmethod
    def _validate_test_index_name(
        cls,
        index_name: str,
        prefixes: Sequence[str] = (TEST_INDEX_PREFIX,),
    ) -> str:
        value = cls._validate_plain_identifier(index_name, "index_name")
        if not value.upper().startswith(tuple(prefix.upper() for prefix in prefixes)):
            raise ValueError(
                f"index_name must start with the test prefix {' or '.join(prefixes)}. "
                "These tools create and drop only disposable test indexes under these "
                "prefixes, so they can never drop an application index."
            )
        return value

    async def _kill_session(
        self,
        database_name: str,
        session_id: int,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if session_id <= 50:
            raise ValueError(
                f"Cannot kill session {session_id}: system sessions (SPID <= 50) are protected."
            )
        # KILL cannot be parameterized — use string formatting with validated int
        sql = f"KILL {int(session_id)}"
        payload = await self.admin_policy.execute(
            AdminAction(
                tool_name="kill_session",
                database_name=database_name,
                action_type="session",
                sql=sql,
                trusted_generated=True,
            ),
            self.executor,
            dry_run=dry_run,
        )
        payload.update({
            "session_id": session_id,
            "note": "Session termination initiated. Rollback of active transactions may take time.",
        })
        if payload["status"] == "completed":
            payload["status"] = "kill_issued"
        return payload

    async def _tune_query(
        self,
        database_name: str,
        sql: str,
        analyze: bool,
        auto_bind_params: bool,
        include_raw_xml: bool,
        window_minutes: int,
        parameter_values: dict[str, Any] | None = None,
        parameter_types: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")
        # Refused text stops here, before any catalog read for its parameters.
        self.validator.validate_read_only_candidate(sql)
        parameter_case, binding_info = await self._compatibility_parameter_case(
            database_name,
            sql,
            auto_bind_params,
            parameter_values,
            parameter_types,
        )
        performance_case = await self._create_performance_case(
            database_name,
            sql,
            parameter_cases=[parameter_case] if parameter_case is not None else None,
            objective="elapsed_time",
            metadata={
                "compatibility_tool": "tune_query",
            },
        )
        evidence = await self._collect_performance_evidence(
            database_name,
            performance_case.case_id,
            sql,
            window_minutes,
            analyze,
            None,
            parameter_case,
        )
        # A read-only policy is an answer, not an error: the evidence stands.
        session = self.performance_workflows.start_session_if_permitted(
            performance_case.case_id,
            database_name,
        )
        return {
            "database_name": database_name,
            "performance_case_id": performance_case.case_id,
            "tuning_session_id": session["session_id"],
            "query_hash": fingerprint_text(sql),
            "analyze": analyze,
            "parameter_binding": binding_info,
            "evidence": evidence,
            "session": session,
            "workflow_complete": False,
            "candidate_required": True,
            "next_step": (
                "Produce concrete rewrites, add each with add_tuning_candidate, then "
                "benchmark them. Missing plan evidence does not block static rewrites."
                if session["session_id"] is not None
                else (
                    "No tuning session was opened: the local database policy does not "
                    "permit one here. Use the evidence for static advice, or benchmark "
                    "on a database whose policy allows it."
                )
            ),
            "raw_xml_included": False,
            "raw_xml_note": (
                "Durable tuning state stores plan fingerprints and summaries, not raw XML."
                if include_raw_xml
                else None
            ),
            "scripts": {
                "rollback": "-- No database changes were applied by tune_query.",
                "deploy": "-- Deploy only the finalist that passes equivalence and benchmark gates.",
            },
        }

    async def _benchmark_query_rewrite(
        self,
        database_name: str,
        baseline_sql: str,
        rewrite_sql: str,
        analyze: bool,
        auto_bind_params: bool,
        include_raw_xml: bool,
        runs: int = 3,
        parameter_values: dict[str, Any] | None = None,
        compare_order: bool | None = None,
        parameter_types: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if not analyze:
            raise ValueError("benchmark_query_rewrite requires analyze=true for measured results.")
        if not 2 <= runs <= 3:
            raise ValueError("Compatibility screening runs must be between 2 and 3.")
        # Refused text stops here, before any catalog read for its parameters.
        self.validator.validate_read_only_candidate(baseline_sql)
        self.validator.validate_read_only_candidate(rewrite_sql)
        parameter_case, binding_info = await self._compatibility_parameter_case(
            database_name,
            baseline_sql,
            auto_bind_params,
            parameter_values,
            parameter_types,
        )
        parameter_cases = [parameter_case] if parameter_case is not None else None
        if parameter_case is not None:
            await self._bind_performance_parameters(
                database_name,
                rewrite_sql,
                parameter_case,
            )
        performance_case = await self._create_performance_case(
            database_name,
            baseline_sql,
            parameter_cases=parameter_cases,
            objective="elapsed_time",
            metadata={
                "compatibility_tool": "benchmark_query_rewrite",
            },
        )
        session = self.performance_workflows.start_session_if_permitted(
            performance_case.case_id,
            database_name,
        )
        if session["session_id"] is None:
            raise PermissionError(session["policy_reason"])
        candidate = self.performance_workflows.add_candidate(
            session["session_id"],
            rewrite_sql,
            strategy="predicate",
        )
        benchmark = await self.performance_workflows.benchmark_candidate(
            session["session_id"],
            candidate["candidate_id"],
            database_name,
            baseline_sql,
            rewrite_sql,
            phase="screening",
            parameter_cases=parameter_cases,
            compare_order=compare_order,
            runs_override=runs,
            prove_equivalence=True,
        )
        benchmark.update({
            "database_name": database_name,
            "analyze": True,
            "performance_case_id": performance_case.case_id,
            "tuning_session_id": session["session_id"],
            "winning_sql": rewrite_sql if benchmark.get("classification") == "improved" else None,
            "parameter_binding": {
                "baseline": binding_info,
                "rewrite": binding_info,
            },
            "raw_xml_included": False,
            "raw_xml_note": (
                "Raw XML is omitted from durable workflow responses."
                if include_raw_xml
                else None
            ),
            "scripts": {
                "rollback": "-- No database changes were applied by benchmark_query_rewrite.",
                "deploy": "-- Deploy the accepted rewrite in application code after full equivalence proof.",
            },
        })
        return benchmark

    async def _plan_health_review(
        self,
        database_name: str,
        *,
        window_minutes: int,
        top_n: int,
    ) -> dict[str, Any]:
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")
        if top_n <= 0:
            raise ValueError("top_n must be greater than 0.")

        enforcement = await self.plan_enforcement.review(
            database_name,
            window_minutes=window_minutes,
            top_n=top_n,
        )
        sniffing = await self.query_regression.detect_parameter_sniffing(
            database_name,
            window_minutes=window_minutes,
            top_n=top_n,
        )
        return {
            "database_name": database_name,
            "mode": "review",
            "window_minutes": window_minutes,
            "top_n": top_n,
            "plan_enforcement": enforcement,
            "parameter_sniffing": {
                "affected_query_count": sniffing.get("affected_query_count", 0),
                "queries": sniffing.get("queries", [])[:top_n],
            },
        }

    async def _metadata_inventory(
        self,
        database_name: str,
        table_references: list[dict[str, str | None]],
    ) -> dict[str, Any]:
        details = []
        unresolved = []
        schema_name_set: set[str] = set()
        for ref in table_references:
            schema_name = ref.get("schema")
            if isinstance(schema_name, str) and schema_name:
                schema_name_set.add(schema_name)
        schema_names = sorted(schema_name_set)
        table_stats: dict[str, Any] = {}
        for schema_name in schema_names:
            table_stats[schema_name] = await self.introspection.get_table_stats(
                database_name,
                schema_name,
            )

        for ref in table_references:
            schema_name = ref.get("schema")
            table_name = ref.get("table")
            if not schema_name or not table_name:
                unresolved.append(
                    {
                        "reference": ref,
                        "reason": "schema is not explicit; use get_object_details after resolving it",
                    }
                )
                continue
            try:
                details.append(
                    await self.introspection.get_object_details(
                        database_name,
                        schema_name,
                        table_name,
                        "table",
                    )
                )
            except Exception as exc:
                note_exception(exc, "server.metadata_inventory")
                unresolved.append(
                    {
                        "reference": ref,
                        "reason": sanitize_error_message(str(exc)),
                    }
                )

        return {
            "table_references": table_references,
            "object_details": details,
            "table_stats_by_schema": table_stats,
            "unresolved": unresolved,
        }

    async def _query_store_history_for_plan(
        self,
        database_name: str,
        original_sql: str,
        plan: dict[str, Any],
        window_minutes: int,
    ) -> dict[str, Any]:
        """Return Query Store evidence only after stable identity resolution."""
        summary = plan.get("summary")
        statements = summary.get("statements") if isinstance(summary, dict) else None
        query_hash = None
        if isinstance(statements, list) and statements:
            first = statements[0]
            if isinstance(first, dict):
                query_hash = first.get("query_hash")

        # The plan's hash finds a parameterized statement that text alone misses.
        identity = await self.query_store.resolve_query_identity(
            database_name,
            original_sql,
            query_hash=query_hash if isinstance(query_hash, str) and query_hash else None,
        )
        if identity.get("status") != "resolved":
            return {
                "database_name": database_name,
                "window_minutes": window_minutes,
                "matches": [],
                "status": "inconclusive",
                "reason": "exact Query Store identity was not uniquely resolved",
                "identity": identity,
                "plan_query_hash": query_hash,
                "matched_by": "none",
                "fuzzy_match_used": False,
            }
        history = await self.query_store.get_query_history_by_id(
            database_name,
            int(identity["query_id"]),
            window_minutes=window_minutes,
            limit=10,
        )
        history["matched_by"] = "query_id"
        history["fuzzy_match_used"] = False
        history["plan_query_hash"] = query_hash
        history["identity_query_hash"] = identity.get("query_hash")
        history["query_hash_corroborated"] = (
            str(query_hash).casefold()
            == str(identity.get("query_hash")).casefold()
            if query_hash is not None and identity.get("query_hash") is not None
            else None
        )
        if history["query_hash_corroborated"] is False:
            history["status"] = "inconclusive"
            history["matches"] = []
            history["reason"] = (
                "the exact Query Store identity resolved to a different query hash "
                "than the captured plan"
            )
        return history

    async def _compatibility_parameter_case(
        self,
        database_name: str,
        sql: str,
        auto_bind_params: bool,
        parameter_values: dict[str, Any] | None,
        parameter_types: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        detected = detect_parameters(sql)
        detected_names = {name.casefold() for name in detected}
        supplied_names = {
            str(name).lstrip("@").casefold()
            for name in (parameter_values or {})
        }
        unreferenced = sorted(supplied_names - detected_names)
        if unreferenced:
            # Parameter names are identifiers, not data.
            raise ValueError(
                "parameter_values names parameters that are not referenced in the SQL: "
                + ", ".join(f"@{name}" for name in unreferenced)
                + ". Remove them, or check the spelling against the SQL."
            )
        if detected and not auto_bind_params and supplied_names != detected_names:
            missing = sorted(detected_names - supplied_names)
            raise ValueError(
                "Parameterized execution with auto_bind_params=false requires "
                "one explicit value for every query parameter. Missing: "
                + ", ".join(f"@{name}" for name in missing)
                + "."
            )
        if not detected and not parameter_values and not parameter_types:
            return None, None

        bucket = await self.param_binding.build_parameter_bucket(
            database_name,
            sql,
            parameter_values=parameter_values,
            parameter_types=parameter_types,
            bucket_id="compatibility",
            label="compatibility",
            provenance=(
                "compatibility_auto_binding"
                if auto_bind_params
                else "compatibility_explicit_binding"
            ),
        )
        if not bucket.parameters:
            return None, None

        parameter_case = {
            "name": "compatibility",
            "values": {
                parameter.name.lstrip("@"): parameter.value
                for parameter in bucket.parameters
            },
            "types": {
                parameter.name.lstrip("@"): parameter.sql_type.sql_declaration
                for parameter in bucket.parameters
            },
            "weight": 1.0,
        }
        binding_summary = {
            "bucket_id": bucket.bucket_id,
            "provenance": bucket.provenance,
            "values_redacted": True,
            "parameters": [
                {
                    "name": parameter.name,
                    "data_type": parameter.sql_type.sql_declaration,
                    "provenance": parameter.provenance,
                    "provenance_detail": dict(parameter.provenance_detail),
                }
                for parameter in bucket.parameters
            ],
        }
        return parameter_case, binding_summary

    @staticmethod
    async def _optional_payload(callback) -> dict[str, Any]:
        try:
            return {"ok": True, "data": await callback()}
        except Exception as exc:
            note_exception(exc, "server.optional_payload")
            return {"ok": False, "error": sanitize_error_message(str(exc))}

    async def _optional_evidence(
        self,
        callback: Callable[[], Awaitable[Any]],
    ) -> Any:
        try:
            return await callback()
        except Exception as exc:
            note_exception(exc, "server.optional_evidence")
            return {
                "available": False,
                "error": sanitize_error_message(str(exc)),
            }

    @staticmethod
    def _sql_hash(sql: str) -> str:
        normalized = " ".join(sql.split()).lower()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]

    async def _analyze_query_plan(
        self,
        database_name: str,
        plan_id: int | None,
        query_id: int | None,
        plan_xml: str | None,
        *,
        session_id: int | None = None,
        last_actual: bool = False,
        node_id: int | None = None,
        statement_index: int | None = None,
        include_digest: bool = True,
    ) -> dict[str, Any]:
        sources = [value is not None for value in (plan_id, query_id, plan_xml, session_id)]
        if sum(sources) != 1:
            raise ValueError("Pass exactly one of plan_id, query_id, plan_xml, or session_id.")
        if last_actual and plan_id is None and query_id is None:
            raise ValueError("last_actual applies only with plan_id or query_id.")
        if statement_index is not None and node_id is None:
            raise ValueError("statement_index applies only with node_id.")
        if plan_xml is not None:
            if len(plan_xml) > MAX_ANALYZED_PLAN_XML_CHARS:
                raise ValueError("plan_xml is too large to analyse.")
            found = PlanSource(plan_xml, {"source": "plan_xml"})
        elif session_id is not None:
            found = await self.plan_sources.live(database_name, session_id)
        elif last_actual:
            found = await self.plan_sources.last_actual(database_name, plan_id=plan_id, query_id=query_id)
        else:
            found = await self.plan_sources.query_store(database_name, plan_id=plan_id, query_id=query_id)
        base: dict[str, Any] = {"database_name": database_name, **found.source}
        if found.xml is None:
            return {**base, "findings": [], **found.status}
        if node_id is not None:
            try:
                node = describe_plan_node(found.xml, node_id, statement_index=statement_index)
            except PlanParseError as exc:
                return {**base, **status_payload(ResultStatus.UNAVAILABLE, f"The plan could not be parsed: {exc}.")}
            if node is None:
                return {
                    **base,
                    **status_payload(ResultStatus.EMPTY, f"No operator with node_id {node_id} in this plan."),
                }
            return {**base, "node": node, **status_payload(ResultStatus.OK, "Operator detail returned.")}
        pin = (
            found.source.get("query_plan_hash")
            if found.source.get("source") == "last_actual_plan"
            else None
        )
        reading = await self._read_plan(
            database_name,
            found.xml,
            include_digest=include_digest,
            # A running request's counts are partial.
            in_flight=found.source.get("source") == "live_session",
            pin_plan_hash=pin,
        )
        parsed = reading["parsed"]
        result = {**base, **reading["analysis"]}
        # A cached batch plan holds sibling statements; tag which findings belong
        # to the statement that was asked about.
        requested = found.source.get("requested_statement_indexes")
        if requested:
            for finding in result.get("findings", []):
                finding["is_requested_statement"] = (
                    finding.get("statement_index") in requested
                )
        if include_digest:
            result["digest"] = reading["digest"]
        if found.source.get("source") == "query_store" and found.source.get("plan_id"):
            result["query_store_runtime"] = await self.plan_sources.query_store_runtime(
                database_name,
                int(found.source["plan_id"]),
                parsed if parsed is not None else found.xml,
            )
        if result.get("parse_error"):
            result.update(
                status_payload(ResultStatus.UNAVAILABLE, f"The plan could not be analysed: {result['parse_error']}.")
            )
        elif parsed is not None and not any(statement.nodes for statement in parsed.statements):
            result.update(
                status_payload(
                    ResultStatus.EMPTY,
                    "The plan has no operators (a SET, DECLARE or other statement "
                    "without a query plan): there is nothing for a rule to judge. "
                    "This is not a clean bill for a query.",
                )
            )
        elif include_digest:
            result.update(
                status_payload(ResultStatus.OK, "Digest and findings returned; no findings means no rule fired.")
            )
        return result

    async def _partition_columns_for_plan(
        self,
        database_name: str,
        parsed: ParsedPlan,
    ) -> dict[str, str]:
        """The partitioning column of each partitioned table the plan reads.

        The plan does not name it; without it the partition rule can only say
        "if". A refused or failed catalog read leaves the rule at that lead.
        """

        tables: dict[tuple[str, str], None] = {}
        access_summary = parse_plan_access(parsed)
        for statement, access_statement in zip(parsed.statements, access_summary.statements):
            nodes = {node.node_id: node for node in statement.nodes}
            for access in access_statement.accesses:
                node = nodes.get(access.node_id)
                if node is not None and partition_access(node) is not None:
                    tables[(access.schema, access.table)] = None
        columns: dict[str, str] = {}
        for schema, table in list(tables)[:MAX_PARTITION_COLUMN_LOOKUPS]:
            try:
                rows = await self.executor.fetch_all(
                    database_name,
                    _PARTITION_COLUMN_SQL,
                    params=[schema, table],
                    max_rows=1,
                    sql_origin="server_built",
                )
            except Exception as exc:
                note_exception(exc, "server.partition_columns")
                continue
            if rows and rows[0].get("partitioning_column"):
                columns[f"{schema}.{table}"] = str(rows[0]["partitioning_column"])
        return columns

    async def _unique_keys_for_plan(
        self,
        database_name: str,
        plan_xml: str,
        parsed: ParsedPlan,
    ) -> dict[str, list[list[str]]]:
        """Usable unique keys of the tables a parameterized plan seeks by equality.

        The plan does not say an index is unique. Without the catalog the
        sniffed-parameter rule fires on a seek of a whole unique key, where no
        value could have given another plan. Read only for a plan that holds
        a compiled parameter value; an unreadable table leaves the finding.
        """

        if "ParameterCompiledValue" not in plan_xml:
            return {}
        tables: dict[tuple[str, str], None] = {}
        for access_statement in parse_plan_access(parsed).statements:
            for access in access_statement.accesses:
                if access.seek_eq_columns and access.schema and access.table:
                    tables[(access.schema, access.table)] = None
        unique_keys: dict[str, list[list[str]]] = {}
        for schema, table in list(tables)[:MAX_UNIQUE_KEY_LOOKUPS]:
            facts = await self.performance_workflows.catalog.table_facts(
                database_name, schema, table
            )
            usable = [list(key.columns) for key in facts.unique_keys if key.usable and key.columns]
            if usable:
                unique_keys[f"{schema}.{table}"] = usable
        return unique_keys

    async def _read_plan(
        self,
        database_name: str,
        plan_xml: str,
        **options: Any,
    ) -> dict[str, Any]:
        """Parse a showplan once, then run the rules and the digest on that parse."""

        def parse() -> ParsedPlan | None:
            try:
                return parse_plan(plan_xml)
            except PlanParseError:
                return None

        large = len(plan_xml) > LARGE_PLAN_XML_CHARS
        parsed = await asyncio.to_thread(parse) if large else parse()
        partition_columns = (
            await self._partition_columns_for_plan(database_name, parsed)
            if parsed is not None
            else {}
        )
        unique_keys = (
            await self._unique_keys_for_plan(database_name, plan_xml, parsed)
            if parsed is not None
            else {}
        )

        def read() -> dict[str, Any]:
            # read_plan takes the ParsedPlan as it is (parse_plan returns it unchanged).
            return read_plan(
                cast(str, parsed) if parsed is not None else plan_xml,
                partition_columns=partition_columns,
                unique_keys=unique_keys,
                **options,
            )

        return await asyncio.to_thread(read) if large else read()

    async def _explain_query_one_shape(
        self,
        database_name: str,
        sql: str,
        analyze: bool,
        hypothetical_indexes: list[dict[str, Any]] | None,
        auto_bind_params: bool,
        include_raw_xml: bool,
        parameter_case: ParameterCaseInput | None,
        parameter_values: dict[str, Any] | None,
        parameter_types: dict[str, str] | None,
        include_operator_summary: bool,
    ) -> dict[str, Any]:
        case = _one_parameter_shape(parameter_case, parameter_values, parameter_types)
        return await self._explain_query(
            database_name,
            sql,
            analyze,
            hypothetical_indexes,
            auto_bind_params,
            include_raw_xml,
            case["values"] if case is not None else None,
            (case["types"] or None) if case is not None else None,
            include_operator_summary,
        )

    async def _explain_query(
        self,
        database_name: str,
        sql: str,
        analyze: bool,
        hypothetical_indexes: list[dict[str, Any]] | None = None,
        auto_bind_params: bool = False,
        include_raw_xml: bool = False,
        parameter_values: dict[str, Any] | None = None,
        parameter_types: dict[str, str] | None = None,
        include_operator_summary: bool = False,
    ) -> dict[str, Any]:
        if hypothetical_indexes:
            raise ValueError(
                "Hypothetical index analysis is disabled on explain_query for safety. "
                "Use analyze_query_indexes/analyze_workload_indexes for read-only index insights."
            )
        # Refused text stops here, before any catalog read for its parameters.
        branches = self.validator.validate_read_only_candidate(sql).branches
        parameter_case, binding_info = await self._compatibility_parameter_case(
            database_name,
            sql,
            auto_bind_params,
            parameter_values,
            parameter_types,
        )
        contract: ParameterExecutionContract | None = None
        if parameter_case is not None:
            contract = await self._bind_performance_parameters(
                database_name,
                sql,
                parameter_case,
            )
        elif detect_parameters(sql):
            raise ValueError(
                "Parameterized SQL requires explicit values or auto_bind_params=true."
            )
        if branches and not analyze:
            return await self._explain_batch_branches(
                database_name, sql, contract, binding_info, include_raw_xml
            )
        if contract is not None:
            artifact = await self.plans.explain_parameterized_query(
                database_name,
                contract,
                analyze=analyze,
            )
        else:
            artifact = await self.plans.explain_query(
                database_name,
                sql,
                analyze,
                hypothetical_indexes=hypothetical_indexes,
            )
        result = self._artifact_to_dict(artifact, include_raw_xml=include_raw_xml)
        reading = (
            await self._read_plan(database_name, artifact.raw_xml, top_n=5, tree_budget=40)
            if artifact.raw_xml
            else None
        )
        # The response only: in-process readers use artifact.summary in full.
        summary = compact_plan_summary(
            result["summary"], include_operator_summary=include_operator_summary
        )
        actual_metrics = summary.get("actual_metrics")
        if artifact.analyze and isinstance(actual_metrics, Mapping):
            actual_metrics = dict(actual_metrics)
            if actual_metrics.get("actual_logical_reads") is None:
                statistics_io = summary.get("statistics_io")
                io_totals = (
                    statistics_io.get("query_totals")
                    if isinstance(statistics_io, Mapping)
                    else None
                )
                plan_reads = _plan_logical_reads(reading["parsed"]) if reading else None
                if isinstance(io_totals, Mapping) and io_totals.get("logical_reads") is not None:
                    actual_metrics["actual_logical_reads"] = io_totals["logical_reads"]
                    actual_metrics["read_metric_source"] = "statistics_io_table_messages"
                elif plan_reads is not None:
                    # The sum over the plan's operators; worktable reads are not in it.
                    actual_metrics["actual_logical_reads"] = plan_reads
                    actual_metrics["read_metric_source"] = "showplan_operator_runtime_counters"
            summary = {**summary, "actual_metrics": actual_metrics}
            server_elapsed = actual_metrics.get("actual_elapsed_ms")
            provenance = dict(result.get("metric_provenance") or {})
            if server_elapsed is not None:
                result["server_elapsed_ms"] = server_elapsed
                result["server_cpu_ms"] = actual_metrics.get("actual_cpu_ms")
                provenance["server_elapsed_ms"] = str(
                    actual_metrics.get("query_metric_source") or "showplan_query_time_stats"
                )
            if provenance.get("execution_ms") == "client_wall_clock":
                provenance["note"] = (
                    "execution_ms is the client wall clock: it includes the network "
                    "round trip and the row fetch. server_elapsed_ms is what the engine "
                    "measured; judge the query by it."
                )
            result["metric_provenance"] = provenance
        if not include_operator_summary:
            summary = _slim_explain_summary(summary, digest_present=reading is not None)
        result["summary"] = summary
        if binding_info is not None:
            result["parameter_binding"] = binding_info
        if reading is not None:
            result["plan_findings"] = _compact_plan_findings(reading["analysis"])
            result["plan_digest"] = reading["digest"]
            basis = artifact.summary.get("estimated_plan_basis")
            if basis:
                _label_unknown_parameters(result["plan_digest"], basis["unknown_parameters"])
        return result

    async def _explain_batch_branches(
        self,
        database_name: str,
        sql: str,
        contract: ParameterExecutionContract | None,
        binding_info: dict[str, Any] | None,
        include_raw_xml: bool,
    ) -> dict[str, Any]:
        """Estimated plan of every branch of a read-only branching batch.

        Nothing is executed. Per branch: the condition that selects it, the
        digest and the rule findings. Operator summaries are left out; the raw
        plan of each branch is a resource.
        """

        plans = await self.plans.explain_batch_branches(database_name, sql, contract=contract)
        branches: list[dict[str, Any]] = []
        operator_count = 0
        for item in plans:
            artifact: ExplainPlanArtifact = item["plan"]
            plan = self._artifact_to_dict(artifact, include_raw_xml=include_raw_xml)
            reading = await self._read_plan(
                database_name, artifact.raw_xml, top_n=3, tree_budget=20
            )
            operator_count += int(artifact.summary.get("operator_count") or 0)
            branch = dict(item["branch"])
            branch["condition"] = _branch_condition_text(branch.get("conditions") or [])
            branch["estimated_plan_basis"] = artifact.summary.get("estimated_plan_basis")
            branch["plan_findings"] = _compact_plan_findings(reading["analysis"], limit=5)
            branch["plan_digest"] = reading["digest"]
            for key in ("raw_xml", "raw_xml_resource_uri"):
                if plan.get(key) is not None:
                    branch[key] = plan[key]
            branches.append(branch)
        result: dict[str, Any] = {
            "database_name": database_name,
            "analyze": False,
            "plan_kind": "estimated",
            "query_executed": False,
            "summary": {
                "statement_count": len(branches),
                "operator_count": operator_count,
                "batch": {"branch_count": len(branches)},
            },
            "branches": branches,
            "note": (
                "One estimated plan per branch; nothing was executed. analyze=true "
                "runs the batch once and returns the plan of the branch the "
                "parameter values select (summary.batch.executed_branch)."
            ),
        }
        if binding_info is not None:
            result["parameter_binding"] = binding_info
        return result

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        value = identifier.strip()
        if not value:
            raise ValueError("Identifier cannot be empty.")
        return f"[{value.replace(']', ']]')}]"

    async def _analyze_query_indexes(
        self,
        database_name: str,
        queries: list[str],
        auto_bind_params: bool = False,
        parameter_values: dict[str, Any] | None = None,
        parameter_types: dict[str, str] | None = None,
        *,
        include_all_existing_indexes: bool = False,
    ) -> dict[str, Any]:
        normalized_values: dict[str, Any] = {}
        for raw_name, value in (parameter_values or {}).items():
            name = str(raw_name).lstrip("@").strip().casefold()
            if not name:
                raise ValueError("explicit parameter name must not be empty")
            if name in normalized_values:
                raise ValueError(f"duplicate explicit parameter name: {name}")
            normalized_values[name] = value
        normalized_types: dict[str, str] = {}
        for raw_name, value in (parameter_types or {}).items():
            name = str(raw_name).lstrip("@").strip().casefold()
            if not name:
                raise ValueError("explicit parameter name must not be empty")
            if name in normalized_types:
                raise ValueError(f"duplicate explicit parameter type: {name}")
            normalized_types[name] = str(value)
        detected_by_query = [
            {name.casefold() for name in detect_parameters(query)}
            for query in queries
        ]
        detected_any = set().union(*detected_by_query) if detected_by_query else set()
        unknown_names = sorted(
            (set(normalized_values) | set(normalized_types)) - detected_any
        )
        if unknown_names:
            raise ValueError(
                "explicit value or type supplied for unknown parameter(s): "
                + ", ".join(unknown_names)
            )

        contracts: list[ParameterExecutionContract] = []
        binding_summaries: list[dict[str, Any] | None] = []
        for ordinal, (query, detected_names) in enumerate(
            zip(queries, detected_by_query, strict=True),
            start=1,
        ):
            if not detected_names:
                contracts.append(
                    ParameterExecutionContract(
                        sql_text=query,
                        bucket_id=f"index-analysis-{ordinal}",
                        parameters=(),
                        provenance="unparameterized_index_analysis",
                    )
                )
                binding_summaries.append(None)
                continue
            query_values = {
                name: value
                for name, value in normalized_values.items()
                if name in detected_names
            }
            query_types = {
                name: value
                for name, value in normalized_types.items()
                if name in detected_names
            }
            if not auto_bind_params and set(query_values) != detected_names:
                raise ValueError(
                    "Parameterized index analysis with auto_bind_params=false "
                    "requires one explicit value for every query parameter."
                )
            bucket = await self.param_binding.build_parameter_bucket(
                database_name,
                query,
                parameter_values=query_values,
                parameter_types=query_types,
                bucket_id=f"index-analysis-{ordinal}",
                provenance=(
                    "index_analysis_auto_binding"
                    if auto_bind_params
                    else "index_analysis_explicit_binding"
                ),
            )
            contracts.append(
                self.param_binding.build_execution_contract(
                    query,
                    bucket,
                    provenance="typed_index_analysis",
                )
            )
            binding_summaries.append(
                {
                    "bucket_id": bucket.bucket_id,
                    "values_redacted": True,
                    "parameters": [
                        {
                            "name": parameter.name,
                            "data_type": parameter.sql_type.sql_declaration,
                            "provenance": parameter.provenance,
                        }
                        for parameter in bucket.parameters
                    ],
                }
            )
        result = await self.query_index_analysis.analyze_queries(
            database_name,
            queries,
            execution_contracts=contracts,
            include_all_existing_indexes=include_all_existing_indexes,
        )
        result["parameter_binding"] = binding_summaries
        return result

    async def _get_top_queries(
        self,
        database_name: str,
        sort_by: str,
        window_minutes: int,
        limit: int,
        *,
        as_of_utc: str | None = None,
        group_by: str = "plan",
        replica_role: str | None = None,
    ) -> dict[str, Any]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0.")
        if window_minutes <= 0:
            raise ValueError("window_minutes must be greater than 0.")
        window_end = parse_as_of(as_of_utc)

        status = await self.query_store.get_status(database_name)
        rows = await self.query_store.get_top_queries(
            database_name,
            sort_by,
            window_minutes,
            limit,
            window_end=window_end,
            group_by=group_by,
            replica_role=replica_role,
        )
        payload: dict[str, Any] = {
            "database_name": database_name,
            "query_store_status": status,
            "sort_by": sort_by,
            "group_by": group_by,
            "replica_role": replica_role or "all",
            "window_minutes": window_minutes,
            "rows": rows,
        }
        if window_end is not None:
            payload["window"] = {
                "start_utc": iso_z(window_end - timedelta(minutes=window_minutes)),
                "end_utc": iso_z(window_end),
            }
        return self._truncate_rows(payload)

    def _artifact_to_dict(
        self,
        artifact: ExplainPlanArtifact,
        *,
        include_raw_xml: bool = False,
    ) -> dict[str, Any]:
        raw_xml_resource = self.artifacts.put_text(
            kind="showplan-xml",
            text=artifact.raw_xml,
            mime_type="application/xml",
            metadata={
                "database_name": artifact.database_name,
                "analyze": artifact.analyze,
            },
        )
        return artifact.as_dict(
            include_raw_xml=include_raw_xml,
            raw_xml_resource=raw_xml_resource,
        )

    def _truncate_rows(self, payload: dict[str, Any]) -> dict[str, Any]:
        rows = payload.get("rows")
        if isinstance(rows, list):
            # Fetches use row_limit + 1 to detect truncation; row_count must
            # describe the rows actually returned, not the sentinel row.
            payload["truncated"] = len(rows) > self.config.row_limit
            payload["rows"] = rows[: self.config.row_limit]
            payload["row_count"] = len(payload["rows"])
        return payload

    def _master_allowlisted(self) -> bool:
        return any(name.casefold() == "master" for name in self.config.allowed_databases)

    def _format_response(self, payload: Any) -> ResponseType:
        if isinstance(payload, dict):
            return payload
        return {"result": payload}

    @staticmethod
    def _raise_tool_error(
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> NoReturn:
        payload = ErrorPayload(code=code, message=message, details=details).as_dict()
        raise ToolError(
            json.dumps(
                payload,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
        )

    @staticmethod
    def _duration_ms(started_at: float) -> float:
        return round((time.monotonic() - started_at) * 1000, 2)

    @staticmethod
    def _estimate_row_count(payload: Any) -> int:
        if isinstance(payload, dict):
            for value in payload.values():
                if isinstance(value, list):
                    return len(value)
        if isinstance(payload, list):
            return len(payload)
        return 0

    async def _verify_pending_index_ownership(
        self,
        database_name: str,
        lease: Mapping[str, Any],
        current_index: Any,
        ownership_proof: str,
    ) -> None:
        """Prove an uncertain CREATE belongs to this lease before cleanup."""

        metadata = lease.get("metadata") or {}
        key_columns = metadata.get("key_columns")
        include_columns = metadata.get("include_columns")
        filter_definition = metadata.get("filter_definition")
        is_unique = metadata.get("is_unique")

        schema_name = metadata.get("target_schema")
        table_name = metadata.get("target_table")
        index_name = lease.get("index_name")
        object_fingerprint = str(lease.get("object_fingerprint") or "")
        marker_fingerprint = str(
            metadata.get("marker_definition_fingerprint") or object_fingerprint
        )
        if not isinstance(schema_name, str) or not schema_name:
            raise ValueError("pending CREATE lease has no target schema")
        if not isinstance(table_name, str) or not table_name:
            raise ValueError("pending CREATE lease has no target table")
        if not isinstance(index_name, str) or not index_name:
            raise ValueError("pending CREATE lease has no resolvable target")
        if marker_fingerprint != object_fingerprint:
            raise ValueError("pending CREATE lease marker fingerprint is inconsistent")
        persisted_structure = any(
            value is not None
            for value in (key_columns, include_columns, filter_definition, is_unique)
        )
        if persisted_structure:
            if not isinstance(key_columns, (list, tuple)) or not key_columns:
                raise ValueError("pending CREATE lease key structure is invalid")
            if not all(isinstance(column, str) for column in key_columns):
                raise ValueError("pending CREATE lease key structure is invalid")
            if not isinstance(include_columns, (list, tuple)) or not all(
                isinstance(column, str) for column in include_columns
            ):
                raise ValueError("pending CREATE lease include structure is invalid")
            if filter_definition is not None and not isinstance(
                filter_definition, str
            ):
                raise ValueError("pending CREATE lease filter structure is invalid")
            if not isinstance(is_unique, bool):
                raise ValueError("pending CREATE lease uniqueness is invalid")
            data_compression = metadata.get("data_compression")
            if data_compression is not None and not isinstance(data_compression, str):
                raise ValueError("pending CREATE lease compression is invalid")
            partition = tuple(
                metadata.get(name)
                for name in ("partition_scheme", "partition_function", "partition_column")
            )
            if any(value is not None and not isinstance(value, str) for value in partition):
                raise ValueError("pending CREATE lease partition placement is invalid")
            data_space = metadata.get("data_space")
            if data_space is not None and data_space != NON_ALIGNED_DATA_SPACE:
                raise ValueError("pending CREATE lease data space is invalid")
            index_kind = metadata.get("index_kind", INDEX_KIND_ROWSTORE)
            if index_kind not in TEST_INDEX_KINDS:
                raise ValueError("pending CREATE lease index kind is invalid")
            candidate = IndexCandidate(
                schema=schema_name,
                table=table_name,
                key_columns=tuple(key_columns),
                include_columns=tuple(include_columns),
                filter_definition=filter_definition,
                is_unique=is_unique,
                index_name=index_name,
                compression=data_compression,
                index_kind=index_kind,
                **_TestIndexTable(
                    schema_name,
                    table_name,
                    partition_scheme=partition[0],
                    partition_function=partition[1],
                    partition_column=partition[2],
                    data_space=data_space,
                ).placement(),
            )
        else:
            candidate = IndexCandidate(
                schema=current_index.schema,
                table=current_index.table,
                key_columns=tuple(
                    f"[{column.name}] {column.direction}"
                    for column in current_index.key_columns
                ),
                include_columns=tuple(current_index.include_columns),
                filter_definition=current_index.filter_definition,
                is_unique=current_index.is_unique,
                index_name=current_index.name,
            )
        if candidate.definition_fingerprint != marker_fingerprint:
            raise ValueError("pending CREATE lease structure does not match its marker")
        if not expected_index_definition_matches(current_index, candidate):
            raise ValueError(
                "pending CREATE index structure does not match its persisted candidate"
            )

        if not await self._index_owner_marker_matches(
            database_name,
            schema_name,
            table_name,
            index_name,
            ownership_proof=ownership_proof,
            definition_fingerprint=marker_fingerprint,
            index_id=current_index.index_id,
        ):
            raise ValueError(
                "pending CREATE ownership or definition marker is missing or mismatched"
            )

    async def _index_owner_marker_matches(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        index_name: str,
        *,
        ownership_proof: str,
        definition_fingerprint: str,
        index_id: int,
    ) -> bool:
        """Check only the markers the CREATE batch wrote in its own transaction."""

        rows = await self.executor.fetch_all(
            database_name,
            _PENDING_INDEX_OWNERSHIP_SQL,
            params=(
                TEST_INDEX_OWNER_PROPERTY,
                TEST_INDEX_DEFINITION_PROPERTY,
                schema_name,
                table_name,
                index_name,
            ),
            sql_origin="server_built",
        )
        if len(rows) != 1:
            return False
        row = rows[0]
        try:
            marker_index_id = int(str(row.get("index_id")))
        except (TypeError, ValueError):
            return False
        return (
            marker_index_id > 0
            and marker_index_id == index_id
            and row.get("owner_marker") == ownership_proof
            and row.get("definition_marker") == definition_fingerprint
        )

    async def _cleanup_expired_index_leases(
        self,
        *,
        database_fingerprint: str | None = None,
    ) -> dict[str, int]:
        """Retry the cleanup of expired and cleanup_required leases; the counts only.

        ``database_fingerprint`` limits the pass to one database's leases.
        """

        report = await self._reconcile_leases(database_fingerprint=database_fingerprint)
        return {key: int(report[key]) for key in ("examined", "cleaned", "cleanup_required")}

    def _lease_database_name(self, lease: Mapping[str, Any]) -> str | None:
        return next(
            (
                name
                for name in self.config.allowed_databases
                if database_fingerprint_matches(
                    str(lease["database_fingerprint"]),
                    name,
                    self.config.server,
                    allow_legacy=bool(self.config.legacy_state_server_binding),
                )
            ),
            None,
        )

    def _lease_object_entries(self, lease: Mapping[str, Any]) -> list[dict[str, Any]]:
        """One entry per test object of a lease; empty when the lease cannot be read."""

        try:
            views = self._index_lease_members(lease)
        except ValueError:
            return []
        return [{**view["metadata"], "index_name": view["index_name"]} for view in views]

    def _lease_is_due(
        self,
        lease: Mapping[str, Any],
        now: datetime,
        session_id: str | None,
    ) -> bool:
        """True when the objects of a lease must be dropped now.

        Ending a session: every lease of it that still has objects. Otherwise:
        a lease whose cleanup already failed (cleanup_required) or was cut off
        (cleanup_pending), at once, and any lease that has expired. A lease
        whose create outcome is unknown waits for its expiry, because its
        build may still commit.
        """

        metadata = lease.get("metadata") or {}
        status = str(lease.get("status"))
        if session_id is not None:
            return lease.get("session_id") == session_id and (
                status in {"cleanup_required", "cleanup_pending"}
                or (status == "active" and metadata.get(HOLD_FLAG) is True)
            )
        try:
            expires_at = datetime.fromisoformat(str(lease["expires_at_utc"]).replace("Z", "+00:00"))
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            expires_at = now
        if expires_at.astimezone(timezone.utc) > now:
            if status not in {"cleanup_required", "cleanup_pending"}:
                return False
            marks = {
                str(entry.get(key))
                for entry in (metadata, *self._lease_object_entries(lease))
                for key in ("ownership_status", "cleanup_reason")
            }
            return not marks & _OUTCOME_UNKNOWN_MARKS
        # A hold between calls is bounded by its expiry alone: no call is
        # using the objects, so an open session does not extend it.
        idle_hold = metadata.get(HOLD_FLAG) is True and metadata.get("hold_state") == "idle"
        # A short/legacy lease must not let another MCP process drop an index
        # while its durable tuning session can still dispatch work.
        return idle_hold or not self._index_lease_has_live_session(lease, now)

    async def _reconcile_leases(
        self,
        *,
        database_fingerprint: str | None = None,
        database_name: str | None = None,
        session_id: str | None = None,
        closed_by: str = "recovered_at_startup",
        attempts: int = 1,
    ) -> dict[str, Any]:
        """Drop the test objects of every lease that is due, and say what happened.

        ``dropped`` lists the objects that are gone; ``remaining`` those still
        in place, each with ``failure`` (failed_step, cause_class, engine
        message) and ``remediation`` (the DDL that removes it). A drop that
        failed on the connection, a timeout or a lock wait is tried again, up
        to ``attempts`` times, after the idle pooled connections were closed.
        ``session_id`` limits the pass to the leases of one session and takes
        its held objects too (the session is ending). ``database_fingerprint``
        or ``database_name`` limits it to one database.
        """

        report: dict[str, Any] = {
            "examined": 0,
            "cleaned": 0,
            "cleanup_required": 0,
            "dropped": [],
            "remaining": [],
        }
        if self.config.profile != McpProfile.SANDBOX:
            return report
        now = datetime.now(timezone.utc)
        for lease in self.performance_store.list_open_index_leases(
            database_fingerprint=database_fingerprint
        ):
            if database_name is not None and self._lease_database_name(lease) != database_name:
                continue
            if not self._lease_is_due(lease, now, session_id):
                continue
            report["examined"] += 1
            entries = self._lease_object_entries(lease)
            error: Exception | None = None
            attempt = 0
            while True:
                attempt += 1
                current = self.performance_store.get_index_lease(lease["lease_id"])
                try:
                    await self._drop_lease_objects(current, now, closed_by=closed_by)
                    error = None
                    break
                except Exception as exc:
                    error = exc
                    note_exception(exc, "server.lease_cleanup")
                    self._record_lease_cleanup_failure(current, exc, now)
                    cause = exc.first if isinstance(exc, _LeaseDropError) else exc
                    retryable = (
                        _drop_failure_diagnostic(
                            cause,
                            view=False,
                            query_timeout_seconds=self.config.query_timeout_seconds,
                        ).get("cause_class")
                        in _RETRYABLE_DROP_CAUSES
                    )
                    if attempt >= attempts or not retryable:
                        break
                    # The next attempt must not reuse a connection that died
                    # with the one that just failed.
                    database_name = self._lease_database_name(current)
                    if database_name:
                        await self.pool.discard_idle(database_name)
                    await asyncio.sleep(
                        _LEASE_DROP_BACKOFF_SECONDS[
                            min(attempt - 1, len(_LEASE_DROP_BACKOFF_SECONDS) - 1)
                        ]
                    )
            failures = (
                error.failures
                if isinstance(error, _LeaseDropError)
                else {str(entry.get("index_name")): error for entry in entries}
                if error is not None
                else {}
            )
            report["cleaned" if error is None else "cleanup_required"] += 1
            for entry in entries:
                item = {**_held_object_report(entry), "lease_id": lease["lease_id"], "attempts": attempt}
                failure = failures.get(str(entry.get("index_name")))
                if failure is None:
                    report["dropped"].append({**item, "dropped": True})
                    continue
                report["remaining"].append(
                    {
                        **item,
                        "dropped": False,
                        "failure": _drop_failure_diagnostic(
                            failure,
                            view=entry.get("object_kind") == LEASE_OBJECT_KIND,
                            query_timeout_seconds=self.config.query_timeout_seconds,
                        ),
                        "remediation": _object_drop_statement(entry),
                    }
                )
            if error is not None and not entries:
                # A lease that cannot be read has no object to name; it still blocks.
                report["remaining"].append(
                    {
                        "lease_id": lease["lease_id"],
                        "dropped": False,
                        "failure": _drop_failure_diagnostic(
                            error,
                            view=False,
                            query_timeout_seconds=self.config.query_timeout_seconds,
                        ),
                    }
                )
        return report

    def _leftover_refusal(self, cleanup: Mapping[str, Any], **identity: Any) -> dict[str, Any]:
        """The result of a call that was not run because a leased test object is still in place."""

        remaining = [item for item in cleanup["remaining"] if isinstance(item, Mapping)]
        names = ", ".join(
            str(item.get("index_name") or item.get("lease_id")) for item in remaining
        )
        remediation = "\n".join(
            str(item["remediation"]) for item in remaining if item.get("remediation")
        )
        reason = (
            f"A test object of an earlier lease is still in place in this database ({names}) "
            "and its drop failed again. No DDL and no benchmark query was dispatched: a "
            "measurement on a database that holds a leftover is not valid. Show the user "
            "remediation, or call finalize_tuning_session on the session that held it to "
            "retry the drop."
        )
        return {
            **identity,
            "classification": "cleanup_required",
            "reason": reason,
            "failure_code": "expired_index_cleanup_required",
            "executions": 0,
            "cleanup": dict(cleanup),
            "cleanup_required": True,
            **status_payload(ResultStatus.PRECONDITION, reason, remediation=remediation or None),
            "session_continues": False,
        }

    def _record_lease_cleanup_failure(
        self,
        lease: Mapping[str, Any],
        exc: Exception,
        now: datetime,
    ) -> None:
        cause = exc.first if isinstance(exc, _LeaseDropError) else exc
        try:
            current_lease = self.performance_store.get_index_lease(lease["lease_id"])
            self.performance_store.recover_index_lease(
                lease["lease_id"],
                status="cleanup_required",
                metadata={
                    "recovery_error_type": type(cause).__name__,
                    "recovery_attempted_at_utc": now.isoformat(),
                    "recovery_failure": _drop_failure_diagnostic(
                        cause,
                        view=False,
                        query_timeout_seconds=self.config.query_timeout_seconds,
                    ),
                },
                expected_version=current_lease["version"],
            )
        except Exception:
            logger.exception(
                "Failed to record expired temporary-index cleanup failure",
                extra={"lease_id": lease["lease_id"]},
            )

    async def _drop_owned_leftover_indexes(
        self,
        database_name: str,
        database_fingerprint: str,
        candidates: Sequence[Any],
    ) -> list[dict[str, Any]]:
        """Drop test indexes that a closed lease of this server built and that still exist.

        The store says such a lease left nothing; the catalog disagrees. The
        owner marker the CREATE batch wrote proves which lease built the
        index, exactly as the drop guard checks it. An index whose markers do
        not match a lease of this store is not touched.
        """

        if not candidates:
            return []
        owners: dict[tuple[str, str, str], tuple[dict[str, Any], str]] = {}
        for lease in self.performance_store.list_closed_index_leases(
            database_fingerprint=database_fingerprint
        ):
            fence = (lease.get("metadata") or {}).get("lease_owner_fence")
            if not isinstance(fence, str) or not _INDEX_OWNER_PROOF.fullmatch(fence):
                continue
            for entry in self._lease_object_entries(lease):
                if entry.get("object_kind") == LEASE_OBJECT_KIND:
                    continue
                key = (
                    str(entry.get("target_schema")),
                    str(entry.get("target_table")),
                    str(entry.get("index_name")),
                )
                owners.setdefault(key, ({**entry, "lease_id": lease["lease_id"]}, fence))
        dropped: list[dict[str, Any]] = []
        for existing in candidates:
            owner = owners.get((existing.schema, existing.table, existing.name))
            if owner is None:
                continue
            entry, fence = owner
            fingerprint = str(entry.get("object_fingerprint") or "")
            try:
                if not await self._index_owner_marker_matches(
                    database_name,
                    existing.schema,
                    existing.table,
                    existing.name,
                    ownership_proof=fence,
                    definition_fingerprint=fingerprint,
                    index_id=existing.index_id,
                ):
                    continue
                # The guarded DROP checks both markers and the index_id again
                # inside its own transaction.
                await self._drop_test_index(
                    database_name,
                    existing.schema,
                    existing.table,
                    existing.name,
                    dry_run=False,
                    workflow_managed=True,
                    idempotency_key=(
                        f"leftover-recovery:{entry['lease_id']}:{existing.name}:"
                        f"{datetime.now(timezone.utc).isoformat()}"
                    ),
                    ownership_proof=fence,
                    expected_definition_fingerprint=fingerprint,
                    expected_index_id=existing.index_id,
                )
            except Exception as exc:
                # Still in place: the caller then reports the leftover as before.
                note_exception(exc, "server.owned_leftover_drop")
                continue
            dropped.append({**_held_object_report(entry), "lease_id": entry["lease_id"], "dropped": True})
        return dropped

    async def _test_objects_on_read_tables(
        self,
        database_name: str,
        statements: Sequence[str],
        *,
        own_session_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Test indexes on the tables the statements read that the caller's session does not hold.

        Such an index (held by another session, or left behind) can change the
        plan of both statements, so a measurement or a comparison made with it
        in place is not a measurement of the database as deployed. None when
        there is none, or when the check does not apply to this database.
        """

        try:
            policy = self.database_policy.require(database_name)
            if self.config.profile != McpProfile.SANDBOX and not policy.allow_test_indexes:
                return None
            prefixes = self._allowed_test_index_prefixes(policy)
            tables = list(
                dict.fromkeys(table for statement in statements for table in tables_read(statement))
            )[:_MAX_WARNED_TABLES]
            holders: dict[str, dict[str, Any]] = {}
            for lease in self.performance_store.list_open_index_leases():
                for entry in self._lease_object_entries(lease):
                    holders.setdefault(str(entry.get("index_name")), lease)
            objects: list[dict[str, Any]] = []
            for schema, table in tables:
                for existing in await collect_existing_indexes(
                    self.executor, database_name, schema_name=schema, table_name=table
                ):
                    if not _is_generated_test_index_name(existing.name, prefixes):
                        continue
                    holder = holders.get(existing.name)
                    if holder is not None and own_session_id and holder["session_id"] == own_session_id:
                        continue
                    objects.append(
                        {
                            "kind": "index",
                            "name": f"{existing.schema}.{existing.table}",
                            "index_name": existing.name,
                            "held_by_session": holder["session_id"] if holder else None,
                            "lease_status": holder["status"] if holder else None,
                        }
                    )
        except Exception as exc:
            # A warning that cannot be computed must not fail the call it annotates.
            note_exception(exc, "server.test_objects_on_read_tables")
            return None
        if not objects:
            return None
        return {
            "objects": objects,
            "warning": (
                "A test index that this call's session does not hold exists on a table "
                "the statements read. It can change the plans of both statements, so "
                "these numbers are not those of the database without it. An index with "
                "no lease (held_by_session null) is a leftover: its owner drops it with "
                "finalize_tuning_session, or a DBA with DROP INDEX."
            ),
        }

    async def _benchmark_tuning_candidate(
        self,
        database_name: str,
        session_id: str,
        candidate_id: str,
        baseline_sql: str,
        candidate_sql: str,
        **options: Any,
    ) -> dict[str, Any]:
        """Measure a rewrite, on a database with no leased leftover in place.

        A lease of this database whose cleanup failed or expired is retried
        first; when its object is still there the benchmark is refused. The
        result says what was dropped, which held objects were in place, and
        which test indexes of other sessions sit on the tables it read.
        """

        cleanup = await self._reconcile_leases(
            database_name=database_name, attempts=_PRE_CALL_DROP_ATTEMPTS
        )
        if cleanup["cleanup_required"]:
            return self._leftover_refusal(cleanup, session_id=session_id, candidate_id=candidate_id)
        payload = await self.performance_workflows.benchmark_candidate(
            session_id, candidate_id, database_name, baseline_sql, candidate_sql, **options
        )
        if cleanup["dropped"]:
            payload = {**payload, "cleanup_before_run": {"dropped": cleanup["dropped"]}}
        payload = self._with_held_objects_present(database_name, session_id, payload)
        foreign = await self._test_objects_on_read_tables(
            database_name, [baseline_sql, candidate_sql], own_session_id=session_id
        )
        if foreign is not None:
            payload = {**payload, "test_objects_on_read_tables": foreign}
        return payload

    async def _drop_lease_objects(
        self,
        lease: Mapping[str, Any],
        now: datetime,
        *,
        closed_by: str,
    ) -> None:
        """Prove and drop every test object of a lease, then close the lease.

        A lease holds one index, a set of indexes, one indexed view, or the
        mixed objects a session kept. Each object is dropped only when its
        ownership is proven; the lease closes only when none is left. Any
        other outcome raises, and the caller records cleanup_required.
        """

        database_name = self._lease_database_name(lease)
        metadata = lease.get("metadata") or {}
        ownership_proof = metadata.get("lease_owner_fence")
        views = self._index_lease_members(lease)
        for view in views:
            target = view["metadata"]
            if target.get("object_kind") == LEASE_OBJECT_KIND:
                # A leased test view: it has no target_table on purpose.
                if (
                    not database_name
                    or not isinstance(target.get("target_schema"), str)
                    or not isinstance(target.get("target_view"), str)
                ):
                    raise ValueError("expired view lease has no resolvable cleanup target")
            elif (
                not database_name
                or not isinstance(target.get("target_schema"), str)
                or not isinstance(target.get("target_table"), str)
            ):
                raise ValueError("expired lease has no resolvable cleanup target")
        if not database_name:
            raise ValueError("expired lease has no resolvable cleanup target")
        if not isinstance(ownership_proof, str) or not _INDEX_OWNER_PROOF.fullmatch(
            ownership_proof
        ):
            raise ValueError("expired lease has no private ownership marker token")
        self._require_test_index_database(database_name)
        updated_lease = self.performance_store.recover_index_lease(
            lease["lease_id"],
            status="cleanup_pending",
            metadata={"recovery_attempted_at_utc": now.isoformat()},
            expected_version=lease["version"],
        )
        member_failures: dict[str, Exception] = {}
        for ordinal, view in enumerate(views):
            drop_key = f"lease-recovery:{lease['lease_id']}:{updated_lease['version']}"
            if len(views) > 1:
                drop_key = f"{drop_key}:{ordinal}"
            try:
                if view["metadata"].get("object_kind") == LEASE_OBJECT_KIND:
                    await self._drop_expired_lease_view(
                        database_name, view, ownership_proof, drop_key=drop_key
                    )
                else:
                    await self._recover_expired_lease_index(
                        database_name, view, ownership_proof, drop_key=drop_key
                    )
            except Exception as member_error:
                if len(views) == 1:
                    raise
                # The other objects of the lease are still dropped.
                member_failures[str(view["index_name"])] = member_error
        if member_failures:
            raise _LeaseDropError(member_failures) from next(iter(member_failures.values()))
        self.performance_store.recover_index_lease(
            lease["lease_id"],
            status="cleaned",
            metadata={
                closed_by: True,
                "cleaned_at_utc": datetime.now(timezone.utc).isoformat(),
            },
            expected_version=updated_lease["version"],
        )

    async def _drop_expired_lease_view(
        self,
        database_name: str,
        lease: Mapping[str, Any],
        ownership_proof: str,
        *,
        drop_key: str,
    ) -> None:
        """Drop the view of an indexed-view lease, or raise when it is not proven gone.

        ``lease`` is a view lease, or the entry of a view in a lease that
        holds several objects. Only a view that carries both markers of the
        lease is dropped.
        """

        metadata = lease.get("metadata") or {}
        schema, view = str(metadata.get("target_schema")), str(metadata.get("target_view"))
        policy = self.database_policy.require(database_name)
        require_view_experiment_policy(policy)
        fingerprint = str(lease["object_fingerprint"])
        state = await read_view_catalog_state(
            self.executor, database_name, schema_name=schema, view_name=view
        )
        if state.owned_by(ownership_proof, fingerprint):
            await self.admin_policy.execute(
                AdminAction(
                    tool_name="benchmark_indexed_view_candidate",
                    database_name=database_name,
                    action_type="test_view",
                    sql=build_test_view_drop_batch(
                        schema,
                        view,
                        test_index_name_prefixes=self._allowed_test_index_prefixes(policy),
                    ),
                    params=(ownership_proof, fingerprint, state.object_id),
                    # The lease keeps no SQL text. The inverse of this drop is
                    # what the catalog holds now: the module text and its indexes.
                    rollback_sql=_view_recreate_sql(state),
                    trusted_generated=True,
                    reviewed_intent=True,
                    idempotency_key=drop_key,
                    exactly_once=True,
                    policy_verified=True,
                    non_production=True,
                    verification_required=True,
                ),
                self.executor,
                dry_run=False,
            )
            state = await read_view_catalog_state(
                self.executor, database_name, schema_name=schema, view_name=view
            )
        outcome = reconcile_view_experiment(
            step="drop",
            state=state,
            owner_token=ownership_proof,
            definition_fingerprint=fingerprint,
        )
        if outcome.lease_status != "cleaned":
            raise RuntimeError(f"expired view lease was not cleaned: {outcome.reason}")

    async def _recover_expired_lease_index(
        self,
        database_name: str,
        lease: Mapping[str, Any],
        ownership_proof: str,
        *,
        drop_key: str,
    ) -> None:
        """Prove and drop one index of an expired lease, or raise when it is uncertain.

        ``lease`` is a lease, or the view of one index of a set
        (``_index_lease_members``): the status is the lease's, the target and
        the create state are that index's.
        """

        metadata = lease.get("metadata") or {}
        schema_name = str(metadata.get("target_schema"))
        table_name = str(metadata.get("target_table"))
        current_indexes = await collect_existing_indexes(
            self.executor,
            database_name,
            schema_name=schema_name,
            table_name=table_name,
        )
        current_index = next(
            (
                index
                for index in current_indexes
                if index.schema == schema_name
                and index.table == table_name
                and index.name == str(lease["index_name"])
            ),
            None,
        )
        if current_index is not None:
            if lease["status"] == "pending_create" and metadata.get(
                "create_dispatch_state"
            ) != "dispatched":
                raise RuntimeError(
                    "pre-dispatch temporary-index lease has an unexpected "
                    "visible index"
                )
            if metadata.get("ownership_proof") == "owner_marker":
                # In-process cleanup already saw a catalog definition
                # that differs from the candidate; only the markers from
                # the CREATE transaction can prove ownership.
                if not await self._index_owner_marker_matches(
                    database_name,
                    schema_name,
                    table_name,
                    str(lease["index_name"]),
                    ownership_proof=ownership_proof,
                    definition_fingerprint=str(lease["object_fingerprint"]),
                    index_id=current_index.index_id,
                ):
                    raise RuntimeError(
                        "expired lease ownership marker no longer matches"
                    )
            elif lease["status"] == "pending_create" or (
                metadata.get("create_dispatch_state") == "dispatched"
                and metadata.get("fingerprint_provenance") == "candidate_intent"
            ):
                # An uncertain create (pending_create after a crash, or
                # cleanup_required after an in-process reconcile) is
                # proven by the markers the CREATE batch commits.
                await self._verify_pending_index_ownership(
                    database_name,
                    lease,
                    current_index,
                    ownership_proof,
                )
            else:
                observed_fingerprint = metadata.get(
                    "observed_definition_fingerprint"
                )
                verification_fingerprint = (
                    observed_fingerprint
                    if isinstance(observed_fingerprint, str)
                    and observed_fingerprint
                    else str(lease["object_fingerprint"])
                )
                if (
                    metadata.get("fingerprint_provenance")
                    != "observed_existing_index"
                    or current_index.definition_fingerprint
                    != verification_fingerprint
                    or current_index.is_disabled
                ):
                    raise RuntimeError(
                        "expired lease index definition no longer matches its "
                        "workflow-owned fingerprint"
                    )
            await self._drop_test_index(
                database_name,
                schema_name,
                table_name,
                lease["index_name"],
                dry_run=False,
                workflow_managed=True,
                idempotency_key=drop_key,
                ownership_proof=ownership_proof,
                expected_definition_fingerprint=str(
                    lease["object_fingerprint"]
                ),
                expected_index_id=current_index.index_id,
            )
        elif lease["status"] == "pending_create":
            # An index of a set that this run saw in the catalog and that is
            # gone now leaves nothing to clean.
            seen_and_gone = (
                lease.get("set_member") is True
                and metadata.get("fingerprint_provenance") == "observed_existing_index"
            )
            if metadata.get("create_dispatch_state") != "pre_dispatch" and not seen_and_gone:
                raise RuntimeError(
                    "temporary-index lease has an uncertain create outcome"
                )

    def _index_lease_has_live_session(
        self,
        lease: Mapping[str, Any],
        now: datetime,
    ) -> bool:
        if lease.get("status") not in {"pending_create", "active"}:
            return False
        try:
            session = self.tuning_sessions.get_session(str(lease["session_id"]))
            if session.status not in {"screening", "finalist_validation"}:
                return False
            if not session.deadline_at_utc:
                return True
            deadline = datetime.fromisoformat(
                session.deadline_at_utc.replace("Z", "+00:00")
            )
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            return deadline.astimezone(timezone.utc) > now
        except (ContractNotFoundError, KeyError, TypeError, ValueError):
            return False

    async def _temporary_index_exists(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        index_name: str,
    ) -> bool:
        rows = await self.executor.fetch_all(
            database_name,
            """
            SELECT TOP (1) 1 AS found
            FROM sys.indexes AS i
            INNER JOIN sys.tables AS t ON t.object_id = i.object_id
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE i.name = ? AND s.name = ? AND t.name = ?
            """,
            (index_name, schema_name, table_name),
            sql_origin="server_built",
        )
        return bool(rows)

    async def run(self) -> None:
        self.mcp.settings.host = self.config.transport.host
        self.mcp.settings.port = self.config.transport.port

        try:
            # Started first so lease-cleanup failures are captured too.
            self.incidents.start()
            cleanup = await self._cleanup_expired_index_leases()
            if cleanup["examined"]:
                logger.info("Reconciled expired temporary-index leases.", extra=cleanup)
            if self.config.transport.mode == TransportMode.STDIO:
                await self.mcp.run_stdio_async()
            elif self.config.transport.mode == TransportMode.SSE:
                await self.mcp.run_sse_async()
            else:
                await self.mcp.run_streamable_http_async()
            self.incidents.mark_shutting_down()
        except BaseException as exc:
            self.incidents.record_process_failure(exc, phase="run")
            raise
        finally:
            # First, so calls still open are recorded even if a later close hangs.
            self.incidents.close()
            try:
                self.performance_store.close()
            except Exception as exc:
                logger.error(
                    "Failed to close performance state store during shutdown.",
                    extra={
                        "error_type": type(exc).__name__,
                        "error": sanitize_error_message(str(exc)),
                    },
                )
            if self.learning_store is not None:
                try:
                    self.learning_store.close()
                except Exception as exc:
                    logger.error(
                        "Failed to close learning state store during shutdown.",
                        extra={
                            "error_type": type(exc).__name__,
                            "error": sanitize_error_message(str(exc)),
                        },
                    )
            try:
                await self.pool.close_all()
            except Exception as exc:
                logger.error(
                    "Failed to close connection pool during shutdown.",
                    extra={
                        "error_type": type(exc).__name__,
                        "error": sanitize_error_message(str(exc)),
                    },
                )


async def async_main(argv: list[str] | None = None) -> None:
    try:
        config = load_server_config(argv)
        configure_logging(config.log_level, config.log_format)
        app = AzureSqlMcpApplication(config)
    except BaseException as exc:
        # The server never started: record it from flags and env, then fail as before.
        record_startup_failure(exc, argv=argv)
        raise
    await app.run()


def main(argv: list[str] | None = None) -> None:
    asyncio.run(async_main(argv))


if __name__ == "__main__":
    # "python -m azure_sql_mcp.server": same guarded entry as the console script.
    from azure_sql_mcp import main as guarded_main

    guarded_main()
