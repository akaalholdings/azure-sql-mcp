from __future__ import annotations

from collections import Counter
from collections import defaultdict
from typing import Any
from typing import Sequence

from .connection import AzureSqlExecutor
from .ddl_generator import generate_migration_script as build_migration_script
from .schema_diff import DiffCategory
from .schema_diff import DiffType
from .schema_diff import SchemaDifference
from .schema_diff import compare_snapshots
from .schema_snapshot import capture_snapshot


class SchemaCompareService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def capture_schema_snapshot(
        self,
        database_name: str,
        schema_filter: str | Sequence[str] | None = None,
    ) -> dict[str, Any]:
        snapshot = await capture_snapshot(
            self.executor,
            database_name,
            _normalize_schema_filter(schema_filter),
        )
        return snapshot.to_dict()

    async def compare_schemas(
        self,
        source_database: str,
        target_database: str,
        schema_filter: str | Sequence[str] | None = None,
    ) -> dict[str, Any]:
        normalized_schema_filter = _normalize_schema_filter(schema_filter)
        source_snapshot, target_snapshot = await _capture_pair(
            self.executor,
            source_database,
            target_database,
            normalized_schema_filter,
        )
        differences = compare_snapshots(source_snapshot, target_snapshot)
        return {
            "source_database": source_database,
            "target_database": target_database,
            "schema_filter": normalized_schema_filter,
            "difference_count": len(differences),
            "summary": _summarize_differences(differences),
            "differences": _group_differences(differences),
        }

    async def generate_migration_script(
        self,
        source_database: str,
        target_database: str,
        schema_filter: str | Sequence[str] | None = None,
    ) -> str:
        normalized_schema_filter = _normalize_schema_filter(schema_filter)
        source_snapshot, target_snapshot = await _capture_pair(
            self.executor,
            source_database,
            target_database,
            normalized_schema_filter,
        )
        differences = compare_snapshots(source_snapshot, target_snapshot)
        return build_migration_script(differences, source_snapshot, target_snapshot)


async def _capture_pair(
    executor: AzureSqlExecutor,
    source_database: str,
    target_database: str,
    schema_filter: Sequence[str] | None,
) -> tuple[Any, Any]:
    import asyncio

    return await asyncio.gather(
        capture_snapshot(executor, source_database, schema_filter),
        capture_snapshot(executor, target_database, schema_filter),
    )


def _normalize_schema_filter(
    schema_filter: str | Sequence[str] | None,
) -> list[str] | None:
    if schema_filter is None:
        return None
    if isinstance(schema_filter, str):
        items = [item.strip() for item in schema_filter.split(",")]
    else:
        items = [item.strip() for item in schema_filter]
    normalized = [item for item in items if item]
    return normalized or None


def _group_differences(
    differences: Sequence[SchemaDifference],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for difference in differences:
        grouped[difference.category.value].append(difference.to_dict())
    return {
        category: grouped[category]
        for category in sorted(grouped.keys())
    }


def _summarize_differences(
    differences: Sequence[SchemaDifference],
) -> dict[str, Any]:
    counts_by_type = Counter(difference.diff_type.value for difference in differences)
    counts_by_category = Counter(difference.category.value for difference in differences)
    return {
        "by_type": {key: counts_by_type[key] for key in sorted(counts_by_type)},
        "by_category": {
            key: counts_by_category[key] for key in sorted(counts_by_category)
        },
    }

