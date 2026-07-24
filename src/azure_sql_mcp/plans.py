from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any

from .artifacts import ExplainPlanArtifact
from .connection import AzureSqlExecutor
from .connection import QueryResult
from .param_binding import ParameterExecutionContract
from .plan_diagnostics import parse_statistics_io_messages
from .plan_diagnostics import summarize_statistics_io_samples
from .safe_sql import SafeSqlValidator

SHOWPLAN_NAMESPACE = {"sp": "http://schemas.microsoft.com/sqlserver/2004/07/showplan"}
logger = logging.getLogger(__name__)
parse_statistics_io = parse_statistics_io_messages


@dataclass(frozen=True)
class ProfiledPlanResult:
    """Actual plan and bounded display rows from one user-query execution."""

    plan: ExplainPlanArtifact
    result_sets: list[QueryResult]
    elapsed_wall_ms: float
    user_query_executions: int
    truncated: bool
    metric_provenance: str


class PlansService:
    def __init__(
        self,
        executor: AzureSqlExecutor,
        validator: SafeSqlValidator,
    ):
        self.executor = executor
        self.validator = validator

    async def explain_query(
        self,
        database_name: str,
        sql: str,
        analyze: bool,
        hypothetical_indexes: list[dict[str, Any]] | None = None,
    ) -> ExplainPlanArtifact:
        validated_query = self.validator.validate_read_only(sql)
        if hypothetical_indexes:
            raise ValueError(
                "Hypothetical index analysis is disabled on explain_query for safety. "
                "Use analyze_query_indexes/analyze_workload_indexes for read-only index insights."
            )

        raw_xml = await self._get_plan_xml(
            database_name=database_name,
            sql=validated_query.normalized_sql,
            analyze=analyze,
        )
        summary = self.summarize_showplan_xml(raw_xml)
        return ExplainPlanArtifact(
            database_name=database_name,
            analyze=analyze,
            summary=summary,
            raw_xml=raw_xml,
        )

    async def profile_query(
        self,
        database_name: str,
        sql: str,
        *,
        max_result_rows: int | None = None,
    ) -> ProfiledPlanResult:
        """Capture rows and actual plan while executing the user SQL once."""

        validated_query = self.validator.validate_read_only(sql)
        return await self._profile_execution(
            database_name,
            execution_sql=validated_query.execution_sql,
            execution_params=(),
            max_result_rows=max_result_rows,
        )

    async def profile_parameterized_query(
        self,
        database_name: str,
        contract: ParameterExecutionContract,
        *,
        max_result_rows: int | None = None,
    ) -> ProfiledPlanResult:
        """Profile one typed ``sp_executesql`` execution without local variables."""

        self.validator.validate_read_only(contract.sql_text)
        return await self._profile_execution(
            database_name,
            execution_sql=contract.sp_executesql_sql,
            execution_params=contract.sp_executesql_values,
            max_result_rows=max_result_rows,
        )

    async def explain_parameterized_query(
        self,
        database_name: str,
        contract: ParameterExecutionContract,
        *,
        analyze: bool,
    ) -> ExplainPlanArtifact:
        """Explain one typed parameter execution without local-variable semantics."""

        self.validator.validate_read_only(contract.sql_text)
        if analyze:
            profiled = await self.profile_parameterized_query(
                database_name,
                contract,
            )
            return profiled.plan
        raw_xml = await self._get_plan_xml(
            database_name=database_name,
            sql=contract.sp_executesql_sql,
            analyze=False,
            params=contract.sp_executesql_values,
        )
        return ExplainPlanArtifact(
            database_name=database_name,
            analyze=False,
            summary=self.summarize_showplan_xml(raw_xml),
            raw_xml=raw_xml,
        )

    async def _profile_execution(
        self,
        database_name: str,
        *,
        execution_sql: str,
        execution_params: tuple[Any, ...],
        max_result_rows: int | None,
    ) -> ProfiledPlanResult:
        row_limit = (
            self.executor.config.row_limit
            if max_result_rows is None
            else max(1, int(max_result_rows))
        )
        execution = await self.executor.execute_profiled_read_only(
            database_name,
            execution_sql,
            params=execution_params,
            max_rows=row_limit + 1,
        )
        raw_xml = self._extract_plan_xml(execution.result_sets)
        summary = self.summarize_showplan_xml(raw_xml)
        statistics_io_messages = getattr(execution, "statistics_io_messages", None)
        if statistics_io_messages is None:
            statistics_io_messages = getattr(execution, "messages", None)
        if statistics_io_messages:
            summary["statistics_io"] = parse_statistics_io_messages(
                statistics_io_messages,
                sample_id="profiled-query",
            )
        actual_metrics = summary.setdefault("actual_metrics", {})
        actual_metrics["measured_wall_elapsed_ms"] = round(
            execution.elapsed_wall_ms,
            6,
        )
        actual_metrics["measured_wall_elapsed_source"] = "client_wall_clock"

        user_results: list[QueryResult] = []
        truncated = False
        for result in execution.result_sets:
            if self._result_contains_plan_xml(result):
                continue
            rows = result.rows
            positional_rows = result.comparison_rows()
            if len(rows) > row_limit:
                rows = rows[:row_limit]
                positional_rows = positional_rows[:row_limit]
                truncated = True
            user_results.append(
                QueryResult(
                    columns=result.columns,
                    rows=rows,
                    column_type_signatures=result.column_type_signatures,
                    positional_rows=positional_rows,
                    positional_rows_exact=result.positional_rows_exact,
                )
            )

        return ProfiledPlanResult(
            plan=ExplainPlanArtifact(
                database_name=database_name,
                analyze=True,
                summary=summary,
                raw_xml=raw_xml,
            ),
            result_sets=user_results,
            elapsed_wall_ms=execution.elapsed_wall_ms,
            user_query_executions=execution.user_query_executions,
            truncated=truncated,
            metric_provenance=execution.metric_provenance,
        )

    async def _get_plan_xml(
        self,
        database_name: str,
        sql: str,
        analyze: bool,
        params: tuple[Any, ...] = (),
    ) -> str:
        # SET SHOWPLAN_XML ON must be in its own batch — SQL Server rejects it
        # when combined with other statements — but the SET option is also
        # session-scoped, so the SET and the query must run on the SAME
        # connection.  execute_session keeps a single pooled connection for
        # the duration of the sequence.
        if analyze:
            set_on = "SET STATISTICS XML ON"
            set_off = "SET STATISTICS XML OFF"
        else:
            set_on = "SET SHOWPLAN_XML ON"
            set_off = "SET SHOWPLAN_XML OFF"

        # Bound each result set: with STATISTICS XML the user query actually
        # executes and returns its rows before the plan XML result set, so an
        # unbounded fetch here could pull an entire table into memory.
        if params:
            per_statement_results = await self.executor.execute_session(
                database_name,
                [set_on, sql, set_off],
                max_rows=self.executor.config.row_limit + 1,
                statement_params=[None, params, None],
            )
        else:
            per_statement_results = await self.executor.execute_session(
                database_name,
                [set_on, sql, set_off],
                max_rows=self.executor.config.row_limit + 1,
            )
        # The middle entry corresponds to the user query — that's where
        # the plan XML is returned by SQL Server.
        plan_results = per_statement_results[1] if len(per_statement_results) > 1 else []
        return self._extract_plan_xml(plan_results)

    def summarize_showplan_xml(self, raw_xml: str) -> dict[str, Any]:
        root = ET.fromstring(raw_xml)
        statement_nodes = root.findall(".//sp:StmtSimple", SHOWPLAN_NAMESPACE)
        operator_nodes = root.findall(".//sp:RelOp", SHOWPLAN_NAMESPACE)
        parent_map = {
            child: parent
            for parent in root.iter()
            for child in parent
        }

        statements = []
        for node in statement_nodes:
            statements.append(
                {
                    "statement_text": node.attrib.get("StatementText"),
                    "statement_type": node.attrib.get("StatementType"),
                    "statement_subtree_cost": node.attrib.get("StatementSubTreeCost"),
                    "statement_est_rows": node.attrib.get("StatementEstRows"),
                    "statement_optm_level": node.attrib.get("StatementOptmLevel"),
                    "statement_optm_early_abort_reason": node.attrib.get(
                        "StatementOptmEarlyAbortReason"
                    ),
                    "cardinality_estimation_model_version": node.attrib.get(
                        "CardinalityEstimationModelVersion"
                    ),
                    # Stable identifiers for Query Store correlation
                    "query_hash": node.attrib.get("QueryHash"),
                    "query_plan_hash": node.attrib.get("QueryPlanHash"),
                }
            )

        operators = [
            self._summarize_operator(node, parent_map)
            for node in operator_nodes
        ]
        expensive_operators = list(operators)

        expensive_operators.sort(
            key=lambda item: float(item["estimated_subtree_cost"] or 0.0),
            reverse=True,
        )

        warnings: list[dict[str, Any]] = []
        for warning_node in root.findall(".//sp:Warnings", SHOWPLAN_NAMESPACE):
            warning_payload: dict[str, Any] = {
                key: value for key, value in warning_node.attrib.items() if value not in {None, ""}
            }
            spills = [
                {key: value for key, value in spill.attrib.items() if value not in {None, ""}}
                for spill in warning_node.findall(".//sp:SpillToTempDb", SHOWPLAN_NAMESPACE)
            ]
            if spills:
                warning_payload["spills_to_tempdb"] = spills
            if warning_payload:
                warnings.append(warning_payload)

        spills = [
            {"node_id": operator["node_id"], **spill}
            for operator in operators
            for spill in operator["spills"]
        ]
        implicit_conversions = [
            {"node_id": operator["node_id"], **conversion}
            for operator in operators
            for conversion in operator["implicit_conversions"]
        ]
        feedback = self._extract_feedback(root)

        return {
            "statement_count": len(statements),
            "operator_count": len(operator_nodes),
            "statements": statements,
            "operators": operators,
            "top_operators": expensive_operators[:5],
            "warnings": warnings,
            "spills": spills,
            "implicit_conversions": implicit_conversions,
            "actual_metrics": self._extract_actual_metrics(root),
            "memory_grants": self._extract_memory_grants(root),
            "missing_indexes": self._extract_missing_indexes(root),
            "parameters": self._extract_parameters(root),
            "feedback": feedback,
            "feedback_state": "observed" if feedback else "not_observed",
        }

    def summarize_statistics_io(
        self,
        messages: list[Any],
        *,
        sample_id: str,
        provenance: str = "SET STATISTICS IO ON message",
    ) -> dict[str, Any]:
        """Expose the sourced IO parser through the plan service API."""
        return parse_statistics_io_messages(
            messages,
            sample_id=sample_id,
            provenance=provenance,
        )

    @staticmethod
    def summarize_statistics_io_samples(
        samples: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return summarize_statistics_io_samples(samples)

    def _summarize_operator(
        self,
        node: ET.Element,
        parent_map: dict[ET.Element, ET.Element],
    ) -> dict[str, Any]:
        estimated_rows = self._optional_float(node.attrib.get("EstimateRows"))
        counters = [
            self._summarize_thread_counter(counter)
            for counter in self._owned_descendants(
                node,
                parent_map,
                "RunTimeCountersPerThread",
            )
        ]
        actual_rows = (
            sum(counter["actual_rows"] or 0.0 for counter in counters)
            if counters
            else None
        )
        estimated_without_row_goal = self._optional_float(
            node.attrib.get("EstimateRowsWithoutRowGoal")
        )
        row_goal = (
            node.attrib.get("IsRowGoal", "").lower() == "true"
            or estimated_without_row_goal is not None
            and estimated_rows is not None
            and estimated_without_row_goal > estimated_rows
        )
        top_nodes = self._owned_descendants(node, parent_map, "Top")
        row_goal_details = {
            "detected": row_goal,
            "estimated_rows_without_row_goal": estimated_without_row_goal,
            "top_row_count": self._optional_float(top_nodes[0].attrib.get("RowCount"))
            if top_nodes
            else None,
            "source": "showplan_row_goal_attributes" if row_goal else None,
        }

        object_nodes = self._owned_descendants(node, parent_map, "Object")
        object_node = object_nodes[0] if object_nodes else None
        object_payload = self._object_payload(object_node)
        index_scan_nodes = self._owned_descendants(node, parent_map, "IndexScan")
        lookup = (
            node.attrib.get("PhysicalOp", "").lower() in {"key lookup", "rid lookup"}
            or any(scan.attrib.get("Lookup", "").lower() == "true" for scan in index_scan_nodes)
        )
        seek_predicates, residual_predicates = self._operator_predicates(node, parent_map)
        spills = self._owned_diagnostic_elements(node, parent_map, {"SpillToTempDb", "HashSpillDetails", "SortSpillDetails"})
        implicit_conversions = self._owned_diagnostic_elements(
            node,
            parent_map,
            {"PlanAffectingConvert", "ConvertIssue"},
        )
        operator_warnings = self._owned_diagnostic_elements(node, parent_map, {"Warnings"})
        execution_modes = sorted(
            {
                str(counter["execution_mode"])
                for counter in counters
                if counter.get("execution_mode")
            }
        )
        estimated_mode = node.attrib.get("EstimatedExecutionMode")
        parallel_exchange = self._parallel_exchange_payload(node, counters)

        return {
            "node_id": self._optional_int(node.attrib.get("NodeId")),
            "physical_op": node.attrib.get("PhysicalOp"),
            "logical_op": node.attrib.get("LogicalOp"),
            "object": object_payload,
            "object_name": object_payload.get("qualified_name") if object_payload else None,
            "index_name": object_payload.get("index") if object_payload else None,
            "seek_predicates": seek_predicates,
            "residual_predicates": residual_predicates,
            "estimated_rows": estimated_rows,
            "estimated_rows_without_row_goal": estimated_without_row_goal,
            "actual_rows": actual_rows,
            "actual_rows_ratio": (
                actual_rows / estimated_rows
                if actual_rows is not None and estimated_rows not in (None, 0)
                else None
            ),
            "estimate_error_ratio": (
                actual_rows / estimated_rows
                if actual_rows is not None and estimated_rows not in (None, 0)
                else None
            ),
            "estimated_io": self._optional_float(node.attrib.get("EstimateIO")),
            "estimated_cpu": self._optional_float(node.attrib.get("EstimateCPU")),
            "estimated_subtree_cost": self._optional_float(
                node.attrib.get("EstimatedTotalSubtreeCost")
            ),
            "estimated_execution_mode": estimated_mode,
            "execution_mode": execution_modes[0] if len(execution_modes) == 1 else execution_modes or estimated_mode,
            "thread_counters": counters,
            "row_goal": row_goal,
            "row_goal_details": row_goal_details,
            "lookup": lookup,
            "parallel_exchange": parallel_exchange,
            "parallelism": parallel_exchange,
            "spills": spills,
            "implicit_conversions": implicit_conversions,
            "warnings": operator_warnings,
        }

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]

    def _owned_descendants(
        self,
        node: ET.Element,
        parent_map: dict[ET.Element, ET.Element],
        tag_name: str,
    ) -> list[ET.Element]:
        result: list[ET.Element] = []
        for child in node.iter():
            if child is node or self._local_name(child.tag) != tag_name:
                continue
            parent = parent_map.get(child)
            while parent is not None and parent is not node:
                if self._local_name(parent.tag) == "RelOp":
                    break
                parent = parent_map.get(parent)
            if parent is node:
                result.append(child)
        return result

    def _owned_diagnostic_elements(
        self,
        node: ET.Element,
        parent_map: dict[ET.Element, ET.Element],
        tag_names: set[str],
    ) -> list[dict[str, Any]]:
        diagnostics: list[dict[str, Any]] = []
        for element in node.iter():
            if element is node or self._local_name(element.tag) not in tag_names:
                continue
            parent = parent_map.get(element)
            while parent is not None and parent is not node:
                if self._local_name(parent.tag) == "RelOp":
                    break
                parent = parent_map.get(parent)
            if parent is not node:
                continue
            diagnostics.append(
                {
                    "element": self._local_name(element.tag),
                    **{key: value for key, value in element.attrib.items() if value != ""},
                }
            )
        return diagnostics

    def _operator_predicates(
        self,
        node: ET.Element,
        parent_map: dict[ET.Element, ET.Element],
    ) -> tuple[list[str], list[str]]:
        seek: list[str] = []
        residual: list[str] = []
        for element in self._owned_descendants(node, parent_map, "ScalarOperator"):
            text = element.attrib.get("ScalarString")
            if not text:
                continue
            ancestors: list[str] = []
            parent = parent_map.get(element)
            while parent is not None and parent is not node:
                ancestors.append(self._local_name(parent.tag))
                parent = parent_map.get(parent)
            destination = seek if "SeekPredicates" in ancestors else residual
            if text not in destination:
                destination.append(text)
        return seek, residual

    def _object_payload(self, node: ET.Element | None) -> dict[str, Any] | None:
        if node is None:
            return None
        payload = {
            key.lower(): value.strip("[]")
            for key, value in node.attrib.items()
            if value not in {None, ""}
        }
        pieces = [payload.get("database"), payload.get("schema"), payload.get("table")]
        payload["qualified_name"] = ".".join(piece for piece in pieces if piece)
        return payload

    def _summarize_thread_counter(self, node: ET.Element) -> dict[str, Any]:
        return {
            "thread": self._optional_int(node.attrib.get("Thread")),
            "actual_rows": self._optional_float(node.attrib.get("ActualRows")),
            "actual_rows_read": self._optional_float(node.attrib.get("ActualRowsRead")),
            "actual_executions": self._optional_int(node.attrib.get("ActualExecutions")),
            "actual_cpu_ms": self._optional_int(node.attrib.get("ActualCPUms")),
            "actual_elapsed_ms": self._optional_int(node.attrib.get("ActualElapsedms")),
            "actual_logical_reads": self._optional_int(node.attrib.get("ActualLogicalReads")),
            "actual_physical_reads": self._optional_int(node.attrib.get("ActualPhysicalReads")),
            "execution_mode": node.attrib.get("ActualExecutionMode"),
        }

    def _parallel_exchange_payload(
        self,
        node: ET.Element,
        counters: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        physical_op = (node.attrib.get("PhysicalOp") or "").lower()
        parallel = physical_op == "parallelism" or node.attrib.get("Parallel", "").lower() == "true"
        if not parallel:
            return None
        rows = [counter["actual_rows"] for counter in counters if counter["actual_rows"] is not None]
        average = sum(rows) / len(rows) if rows else None
        return {
            "is_exchange": physical_op == "parallelism",
            "physical_op": node.attrib.get("PhysicalOp"),
            "logical_op": node.attrib.get("LogicalOp"),
            "partitioning_type": node.attrib.get("PartitioningType"),
            "thread_count": len(counters),
            "actual_rows_min": min(rows) if rows else None,
            "actual_rows_max": max(rows) if rows else None,
            "actual_rows_average": average,
            "row_skew_ratio": max(rows) / average if rows and average else None,
            "source": "showplan_parallel_operator_thread_counters",
        }

    def _extract_feedback(self, root: ET.Element) -> list[dict[str, Any]]:
        feedback: list[dict[str, Any]] = []
        for element in root.iter():
            local_name = self._local_name(element.tag)
            attributes = {
                key: value for key, value in element.attrib.items() if value not in {None, ""}
            }
            if local_name in {
                "ParameterSensitivePredicate",
                "QueryVariant",
                "PlanPerValue",
            } or "feedback" in local_name.casefold() or any(
                "feedback" in key.casefold() for key in attributes
            ):
                feedback.append({"element": local_name, **attributes})
        return feedback

    def _extract_actual_metrics(self, root: ET.Element) -> dict[str, Any]:
        """Return sourced query metrics without summing every operator/thread."""

        all_counters = root.findall(
            ".//sp:RunTimeCountersPerThread",
            SHOWPLAN_NAMESPACE,
        )
        statement_metrics: list[dict[str, Any]] = []
        root_operator_metrics: list[dict[str, Any]] = []

        for ordinal, statement in enumerate(
            root.findall(".//sp:StmtSimple", SHOWPLAN_NAMESPACE),
            start=1,
        ):
            query_time = statement.find("sp:QueryTimeStats", SHOWPLAN_NAMESPACE)
            if query_time is not None:
                statement_metrics.append(
                    {
                        "statement_ordinal": ordinal,
                        "statement_type": statement.attrib.get("StatementType"),
                        "cpu_ms": self._optional_int(query_time.attrib.get("CpuTime")),
                        "elapsed_ms": self._optional_int(
                            query_time.attrib.get("ElapsedTime")
                        ),
                        "source": "showplan_query_time_stats",
                    }
                )

            root_operator = statement.find(
                "sp:QueryPlan/sp:RelOp",
                SHOWPLAN_NAMESPACE,
            )
            if root_operator is None:
                continue
            counters = root_operator.findall(
                "sp:RunTimeInformation/sp:RunTimeCountersPerThread",
                SHOWPLAN_NAMESPACE,
            )
            root_operator_metrics.append(
                {
                    "statement_ordinal": ordinal,
                    "node_id": root_operator.attrib.get("NodeId"),
                    "physical_op": root_operator.attrib.get("PhysicalOp"),
                    "thread_counter_count": len(counters),
                    "actual_rows": sum(
                        self._to_float(counter.attrib.get("ActualRows"))
                        for counter in counters
                    ),
                    "actual_executions": sum(
                        self._to_int(counter.attrib.get("ActualExecutions"))
                        for counter in counters
                    ),
                    "source": "showplan_root_operator_thread_counters",
                }
            )

        single_statement = (
            statement_metrics[0] if len(statement_metrics) == 1 else None
        )
        single_root = (
            root_operator_metrics[0] if len(root_operator_metrics) == 1 else None
        )
        return {
            "runtime_counter_count": len(all_counters),
            "statement_metric_count": len(statement_metrics),
            "statement_metrics": statement_metrics,
            "root_operator_metrics": root_operator_metrics,
            "actual_cpu_ms": single_statement.get("cpu_ms")
            if single_statement
            else None,
            "actual_elapsed_ms": single_statement.get("elapsed_ms")
            if single_statement
            else None,
            "actual_rows": single_root.get("actual_rows") if single_root else None,
            "actual_executions": single_root.get("actual_executions")
            if single_root
            else None,
            "actual_logical_reads": None,
            "actual_physical_reads": None,
            "query_metric_source": "showplan_query_time_stats"
            if single_statement
            else "unavailable_for_multi_statement_plan",
            "read_metric_source": "not_available_as_reliable_query_total",
        }

    def _extract_memory_grants(self, root: ET.Element) -> list[dict[str, Any]]:
        grants = []
        for node in root.findall(".//sp:MemoryGrantInfo", SHOWPLAN_NAMESPACE):
            grants.append(
                {
                    "serial_required_memory_kb": self._to_int(
                        node.attrib.get("SerialRequiredMemory")
                    ),
                    "serial_desired_memory_kb": self._to_int(
                        node.attrib.get("SerialDesiredMemory")
                    ),
                    "required_memory_kb": self._to_int(node.attrib.get("RequiredMemory")),
                    "desired_memory_kb": self._to_int(node.attrib.get("DesiredMemory")),
                    "requested_memory_kb": self._to_int(node.attrib.get("RequestedMemory")),
                    "grant_wait_time_ms": self._to_int(node.attrib.get("GrantWaitTime")),
                    "granted_memory_kb": self._to_int(node.attrib.get("GrantedMemory")),
                    "max_used_memory_kb": self._to_int(node.attrib.get("MaxUsedMemory")),
                    "max_query_memory_kb": self._to_int(node.attrib.get("MaxQueryMemory")),
                }
            )
        return grants

    def _extract_missing_indexes(self, root: ET.Element) -> list[dict[str, Any]]:
        missing = []
        for group in root.findall(".//sp:MissingIndexGroup", SHOWPLAN_NAMESPACE):
            impact = self._to_float(group.attrib.get("Impact"))
            for index in group.findall("sp:MissingIndex", SHOWPLAN_NAMESPACE):
                missing.append(
                    {
                        "impact_pct": impact,
                        "database": index.attrib.get("Database"),
                        "schema": index.attrib.get("Schema"),
                        "table": index.attrib.get("Table"),
                        "equality_columns": self._extract_missing_index_columns(
                            index, "EQUALITY"
                        ),
                        "inequality_columns": self._extract_missing_index_columns(
                            index, "INEQUALITY"
                        ),
                        "include_columns": self._extract_missing_index_columns(
                            index, "INCLUDE"
                        ),
                    }
                )
        return missing

    def _extract_missing_index_columns(self, index_node: ET.Element, usage: str) -> list[str]:
        columns: list[str] = []
        for group in index_node.findall("sp:ColumnGroup", SHOWPLAN_NAMESPACE):
            if group.attrib.get("Usage") != usage:
                continue
            for column in group.findall("sp:Column", SHOWPLAN_NAMESPACE):
                name = column.attrib.get("Name")
                if name:
                    columns.append(name.strip("[]"))
        return columns

    def _extract_parameters(self, root: ET.Element) -> list[dict[str, Any]]:
        parameters = []
        for node in root.findall(".//sp:ParameterList/sp:ColumnReference", SHOWPLAN_NAMESPACE):
            name = node.attrib.get("Column")
            if not name:
                continue
            parameters.append(
                {
                    "name": name,
                    "data_type": node.attrib.get("ParameterDataType"),
                    "compiled_value": node.attrib.get("ParameterCompiledValue"),
                    "runtime_value": node.attrib.get("ParameterRuntimeValue"),
                    "metadata": {
                        key: value
                        for key, value in node.attrib.items()
                        if key not in {"Column", "ParameterCompiledValue", "ParameterRuntimeValue"}
                    },
                }
            )
        return parameters

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _to_int(value: Any) -> int:
        try:
            return int(float(value or 0))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        if value in {None, ""}:
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _optional_float(value: Any) -> float | None:
        if value in {None, ""}:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _result_contains_plan_xml(result: QueryResult) -> bool:
        return any(
            isinstance(value, str) and value.lstrip().startswith("<ShowPlanXML")
            for row in result.rows
            for value in row.values()
        )

    def _extract_plan_xml(self, results) -> str:
        xml_candidates: list[str] = []
        for result in results:
            for row in result.rows:
                for value in row.values():
                    if isinstance(value, str) and value.lstrip().startswith("<ShowPlanXML"):
                        xml_candidates.append(value)
        if not xml_candidates:
            raise RuntimeError(
                "No SHOWPLAN XML was returned. Confirm SHOWPLAN access and that the statement is supported."
            )
        return max(xml_candidates, key=len)

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        value = identifier.strip()
        if not value:
            raise ValueError("Identifier cannot be empty.")
        return f"[{value.replace(']', ']]')}]"
