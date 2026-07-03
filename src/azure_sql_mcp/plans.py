from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from typing import Any

from .artifacts import ExplainPlanArtifact
from .connection import AzureSqlExecutor
from .safe_sql import SafeSqlValidator

SHOWPLAN_NAMESPACE = {"sp": "http://schemas.microsoft.com/sqlserver/2004/07/showplan"}
logger = logging.getLogger(__name__)


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

    async def _get_plan_xml(
        self,
        database_name: str,
        sql: str,
        analyze: bool,
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
                }
            )

        expensive_operators = []
        for node in operator_nodes:
            expensive_operators.append(
                {
                    "physical_op": node.attrib.get("PhysicalOp"),
                    "logical_op": node.attrib.get("LogicalOp"),
                    "estimated_rows": node.attrib.get("EstimateRows"),
                    "estimated_io": node.attrib.get("EstimateIO"),
                    "estimated_cpu": node.attrib.get("EstimateCPU"),
                    "estimated_subtree_cost": node.attrib.get("EstimatedTotalSubtreeCost"),
                }
            )

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

        return {
            "statement_count": len(statements),
            "operator_count": len(operator_nodes),
            "statements": statements,
            "top_operators": expensive_operators[:5],
            "warnings": warnings,
            "actual_metrics": self._extract_actual_metrics(root),
            "memory_grants": self._extract_memory_grants(root),
            "missing_indexes": self._extract_missing_indexes(root),
            "parameters": self._extract_parameters(root),
        }

    def _extract_actual_metrics(self, root: ET.Element) -> dict[str, Any]:
        counters = root.findall(".//sp:RunTimeCountersPerThread", SHOWPLAN_NAMESPACE)
        totals = {
            "actual_rows": 0.0,
            "actual_executions": 0,
            "actual_logical_reads": 0,
            "actual_physical_reads": 0,
            "actual_cpu_ms": 0,
            "actual_elapsed_ms": 0,
        }
        for counter in counters:
            totals["actual_rows"] += self._to_float(counter.attrib.get("ActualRows"))
            totals["actual_executions"] += self._to_int(counter.attrib.get("ActualExecutions"))
            totals["actual_logical_reads"] += self._to_int(
                counter.attrib.get("ActualLogicalReads")
            )
            totals["actual_physical_reads"] += self._to_int(
                counter.attrib.get("ActualPhysicalReads")
            )
            totals["actual_cpu_ms"] += self._to_int(counter.attrib.get("ActualCPUms"))
            totals["actual_elapsed_ms"] += self._to_int(
                counter.attrib.get("ActualElapsedms")
            )

        return {
            "runtime_counter_count": len(counters),
            **totals,
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
                    "compiled_value": node.attrib.get("ParameterCompiledValue"),
                    "runtime_value": node.attrib.get("ParameterRuntimeValue"),
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
