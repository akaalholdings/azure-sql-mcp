from __future__ import annotations

import logging
import uuid
import xml.etree.ElementTree as ET
from typing import Any

from .artifacts import ExplainPlanArtifact
from .connection import AzureSqlExecutor
from .observability import sanitize_error_message
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
            return await self.explain_with_hypothetical(
                database_name=database_name,
                validated_sql=validated_query.normalized_sql,
                analyze=analyze,
                hypothetical_indexes=hypothetical_indexes,
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

    async def explain_with_hypothetical(
        self,
        database_name: str,
        validated_sql: str,
        analyze: bool,
        hypothetical_indexes: list[dict[str, Any]],
    ) -> ExplainPlanArtifact:
        if not hypothetical_indexes:
            raise ValueError("At least one hypothetical index definition is required.")

        if not await self._can_create_hypothetical_indexes(database_name):
            raise PermissionError(
                "Hypothetical index analysis requires CREATE INDEX permission on the target database."
            )

        cleanup_statements: list[str] = []
        created_indexes: list[dict[str, Any]] = []
        try:
            for definition in hypothetical_indexes:
                normalized = self._normalize_hypothetical_index(definition)
                create_sql, drop_sql, created_index = self._build_hypothetical_index_statements(
                    normalized
                )
                await self.executor.execute_non_query(database_name, create_sql)
                cleanup_statements.append(drop_sql)
                created_indexes.append(created_index)

            raw_xml = await self._get_plan_xml(
                database_name=database_name,
                sql=validated_sql,
                analyze=analyze,
            )
        except Exception as exc:
            raise RuntimeError(
                "Hypothetical index analysis failed. Statistics-only indexes may not be "
                f"supported in this environment or the current principal lacks the required permissions: {exc}"
            ) from exc
        finally:
            for statement in cleanup_statements:
                try:
                    await self.executor.execute_non_query(database_name, statement)
                except Exception as exc:
                    logger.warning(
                        "Failed to clean up hypothetical index",
                        extra={
                            "database_name": database_name,
                            "error": sanitize_error_message(str(exc)),
                        },
                    )

        summary = self.summarize_showplan_xml(raw_xml)
        summary["hypothetical_indexes"] = created_indexes
        summary["hypothetical_analysis"] = True
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

        per_statement_results = await self.executor.execute_session(
            database_name,
            [set_on, sql, set_off],
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

        warnings = []
        for warning_node in root.findall(".//sp:Warnings", SHOWPLAN_NAMESPACE):
            warning_payload = {
                key: value for key, value in warning_node.attrib.items() if value not in {None, ""}
            }
            if warning_payload:
                warnings.append(warning_payload)

        return {
            "statement_count": len(statements),
            "operator_count": len(operator_nodes),
            "statements": statements,
            "top_operators": expensive_operators[:5],
            "warnings": warnings,
        }

    async def _can_create_hypothetical_indexes(self, database_name: str) -> bool:
        rows = await self.executor.fetch_all(
            database_name,
            """
            SELECT
                HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE INDEX') AS can_create_index
            """,
        )
        if not rows:
            return False
        return bool(rows[0].get("can_create_index"))

    def _normalize_hypothetical_index(
        self,
        definition: dict[str, Any],
    ) -> dict[str, Any]:
        schema = str(definition.get("schema") or "dbo").strip()
        table = str(definition.get("table") or "").strip()
        columns = [
            str(column).strip().strip("[]")
            for column in definition.get("columns", [])
            if str(column).strip()
        ]
        include_columns = [
            str(column).strip().strip("[]")
            for column in definition.get("include_columns", [])
            if str(column).strip()
        ]

        if not table:
            raise ValueError("Hypothetical index definitions require a table name.")
        if not columns:
            raise ValueError("Hypothetical index definitions require at least one key column.")

        return {
            "schema": schema.strip("[]") or "dbo",
            "table": table.strip("[]"),
            "columns": columns,
            "include_columns": include_columns,
        }

    def _build_hypothetical_index_statements(
        self,
        definition: dict[str, Any],
    ) -> tuple[str, str, dict[str, Any]]:
        index_name = f"__hypo_{uuid.uuid4().hex[:8]}"
        quoted_schema = self._quote_identifier(definition["schema"])
        quoted_table = self._quote_identifier(definition["table"])
        include_clause = ""
        if definition["include_columns"]:
            include_clause = (
                " INCLUDE ("
                + ", ".join(self._quote_identifier(column) for column in definition["include_columns"])
                + ")"
            )

        create_sql = (
            f"CREATE NONCLUSTERED INDEX {self._quote_identifier(index_name)} "
            f"ON {quoted_schema}.{quoted_table} "
            f"({', '.join(self._quote_identifier(column) for column in definition['columns'])})"
            f"{include_clause} "
            "WITH (STATISTICS_ONLY = ON);"
        )
        drop_sql = (
            f"DROP INDEX {self._quote_identifier(index_name)} ON {quoted_schema}.{quoted_table};"
        )
        created_index = {
            "name": index_name,
            "schema": definition["schema"],
            "table": definition["table"],
            "columns": definition["columns"],
            "include_columns": definition["include_columns"],
        }
        return create_sql, drop_sql, created_index

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
