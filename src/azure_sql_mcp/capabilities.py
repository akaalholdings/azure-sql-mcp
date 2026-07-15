from __future__ import annotations

from typing import Any
from typing import Awaitable
from typing import Callable

from .connection import AzureSqlExecutor
from .index_recommendations import IndexRecommendationService
from .observability import sanitize_error_message
from .plans import PlansService
from .query_store import QueryStoreService

CheckCallable = Callable[[str], Awaitable[Any]]


class CapabilityService:
    def __init__(
        self,
        executor: AzureSqlExecutor,
        query_store_service: QueryStoreService,
        plans_service: PlansService,
        recommendation_service: IndexRecommendationService,
    ):
        self.executor = executor
        self.query_store_service = query_store_service
        self.plans_service = plans_service
        self.recommendation_service = recommendation_service

    async def check(self, database_name: str) -> dict[str, Any]:
        checks = {
            "connectivity": await self._run_check(
                database_name,
                self._check_connectivity,
            ),
            "schema_metadata": await self._run_check(
                database_name,
                self._check_schema_metadata,
            ),
            "query_store": await self._run_check(
                database_name,
                self._check_query_store,
            ),
            "estimated_plan": await self._run_check(
                database_name,
                self._check_estimated_plan,
            ),
            "actual_plan": await self._run_check(
                database_name,
                self._check_actual_plan,
            ),
            "missing_index_dmvs": await self._run_check(
                database_name,
                self._check_missing_indexes,
            ),
            "tuning_metadata": await self._run_check(
                database_name,
                self._check_tuning_metadata,
            ),
        }
        return {"database_name": database_name, "checks": checks}

    async def _run_check(self, database_name: str, callback: CheckCallable) -> dict[str, Any]:
        try:
            detail = await callback(database_name)
            return {"available": True, "detail": detail}
        except Exception as exc:
            return {"available": False, "message": sanitize_error_message(str(exc))}

    async def _check_connectivity(self, database_name: str) -> Any:
        rows = await self.executor.fetch_all(database_name, "SELECT DB_NAME() AS database_name")
        return rows[0] if rows else {"message": "No rows returned from connectivity probe."}

    async def _check_schema_metadata(self, database_name: str) -> Any:
        rows = await self.executor.fetch_all(
            database_name,
            "SELECT TOP (1) name AS schema_name FROM sys.schemas ORDER BY name",
        )
        return rows

    async def _check_query_store(self, database_name: str) -> Any:
        return await self.query_store_service.get_status(database_name)

    async def _plan_probe_query(self, database_name: str) -> str:
        """Probe plans against a user table when one exists.

        SQL Server exempts catalog-only statements from the SHOWPLAN
        permission check, so a sys.objects probe reports plans as available
        even when explain_query on real tables would be denied (found live
        with a db_datareader-only login).
        """
        rows = await self.executor.fetch_all(
            database_name,
            """
            SELECT TOP (1)
                QUOTENAME(s.name) + '.' + QUOTENAME(t.name) AS qualified_name
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
            WHERE t.is_ms_shipped = 0
            ORDER BY t.object_id
            """,
        )
        if rows and rows[0].get("qualified_name"):
            return f"SELECT TOP 1 * FROM {rows[0]['qualified_name']}"
        return "SELECT TOP 1 name FROM sys.objects ORDER BY name"

    async def _check_estimated_plan(self, database_name: str) -> Any:
        plan = await self.plans_service.explain_query(
            database_name,
            await self._plan_probe_query(database_name),
            analyze=False,
        )
        return plan.summary

    async def _check_actual_plan(self, database_name: str) -> Any:
        plan = await self.plans_service.explain_query(
            database_name,
            await self._plan_probe_query(database_name),
            analyze=True,
        )
        return plan.summary

    async def _check_missing_indexes(self, database_name: str) -> Any:
        rows = await self.executor.fetch_all(
            database_name,
            "SELECT TOP (1) index_handle FROM sys.dm_db_missing_index_details WHERE database_id = DB_ID()",
        )
        return {"row_count": len(rows)}

    async def _check_tuning_metadata(self, database_name: str) -> Any:
        analysis = await self.recommendation_service.analyze(database_name)
        return {
            "missing_indexes_count": len(analysis["missing_indexes"]),
            "automatic_tuning_recommendations_count": len(
                analysis["automatic_tuning_recommendations"]
            ),
        }
