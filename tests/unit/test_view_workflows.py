from __future__ import annotations

import json
import hashlib
import re
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from azure_sql_mcp.admin_policy import AdminPolicy
from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import McpProfile
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.server import AzureSqlMcpApplication
from azure_sql_mcp.view_workflows import ViewChangeRequest
from azure_sql_mcp.view_workflows import ViewDefinitionError
from azure_sql_mcp.view_workflows import ViewPolicyError
from azure_sql_mcp.view_workflows import ViewWorkflowError
from azure_sql_mcp.view_workflows import ViewWorkflowService
from azure_sql_mcp.view_workflows import build_view_execution_batch
from azure_sql_mcp.view_workflows import extract_view_dependencies
from azure_sql_mcp.view_workflows import prepared_view_change_from_state
from azure_sql_mcp.view_workflows import prepared_view_change_state
from azure_sql_mcp.view_workflows import validate_view_definition
from azure_sql_mcp.view_workflows import view_definition_fingerprint


def _fence_bit(value: str) -> bool | None:
    return None if value.casefold() == "null" else value == "1"


class FakeViewExecutor:
    def __init__(self, definition: str | None = None) -> None:
        self.views: dict[tuple[str, str], str] = {}
        self.catalog_schema_bound: dict[tuple[str, str], bool] = {}
        self.catalog_ansi_nulls: dict[tuple[str, str], bool] = {}
        self.legacy_ansi_nulls: dict[tuple[str, str], bool] = {}
        self.catalog_quoted_identifier: dict[tuple[str, str], bool] = {}
        self.object_ids: dict[tuple[str, str], int] = {}
        self.view_indexes: dict[tuple[str, str], list[dict[str, int]]] = {}
        self.extended_properties: dict[tuple[str, str, str], str | None] = {}
        self.catalog_dependencies: dict[tuple[str, str], tuple[str, ...]] = {}
        self._next_object_id = 18
        if definition is not None:
            key = ("dbo", "SalesView")
            self.views[key] = definition
            self.object_ids[key] = 17
        self.batch_history: list[tuple[str, str]] = []
        self.before_execute: Callable[[], None] | None = None
        self.at_lock_boundary: Callable[[], None] | None = None

    async def fetch_all(self, database_name, query, params=None):
        if "sys.extended_properties" in query:
            schema_name, view_name, marker_prefix_length, marker_prefix = params or (
                "dbo",
                "SalesView",
                0,
                "AzureSqlMcp_View_v1_",
            )
            return [
                {"name": property_name, "marker_value": value}
                for (property_schema, property_view, property_name), value in sorted(
                    self.extended_properties.items()
                )
                if (property_schema, property_view) == (schema_name, view_name)
                and property_name[:marker_prefix_length] == marker_prefix
            ]
        schema_name, view_name = params or ("dbo", "SalesView")
        key = (schema_name, view_name)
        if "sys.indexes" in query:
            return [{"index_count": len(self.view_indexes.get(key, []))}]
        if "sql_expression_dependencies" in query:
            dependencies = self.catalog_dependencies.get(key)
            if dependencies is None:
                definition = self.views.get(key)
                dependencies = extract_view_dependencies(definition or "")
            rows = []
            for dependency in dependencies:
                parts = dependency.split(".")
                rows.append(
                    {
                        "referenced_database_name": (
                            parts[-3] if len(parts) >= 3 else ""
                        ),
                        "referenced_schema_name": (
                            parts[-2] if len(parts) >= 2 else ""
                        ),
                        "referenced_entity_name": parts[-1],
                    }
                )
            return rows
        definition = self.views.get(key)
        if definition is None:
            return []
        row = {
            "schema_name": schema_name,
            "view_name": view_name,
            "object_id": self.object_ids[key],
            "definition": definition,
            # Azure SQL Database always reports ANSI_NULLS ON.  The explicit
            # legacy fixture is only for the fail-closed compatibility test.
            "uses_ansi_nulls": self.legacy_ansi_nulls.get(key, True),
            "uses_quoted_identifier": self.catalog_quoted_identifier.get(key, True),
        }
        if key in self.catalog_schema_bound:
            row["is_schema_bound"] = self.catalog_schema_bound[key]
        return [row]

    def _fence_state(
        self,
        key: tuple[str, str],
        marker_name: str,
    ) -> tuple[
        bool,
        int | None,
        int,
        str | None,
        bool | None,
        bool | None,
        bool | None,
        bool,
        str | None,
        int,
    ]:
        current_definition = self.views.get(key)
        actual_exists = current_definition is not None
        actual_object_id = self.object_ids.get(key) if actual_exists else None
        actual_definition_hash = (
            hashlib.sha256(current_definition.encode("utf-16le")).hexdigest()
            if current_definition is not None
            else None
        )
        actual_schema_bound = (
            self.catalog_schema_bound.get(
                key,
                bool(
                    re.search(
                        r"\bSCHEMABINDING\b",
                        current_definition or "",
                        flags=re.IGNORECASE,
                    )
                ),
            )
            if actual_exists
            else None
        )
        actual_ansi_nulls = (
            self.catalog_ansi_nulls.get(key, True) if actual_exists else None
        )
        actual_quoted_identifier = (
            self.catalog_quoted_identifier.get(key, True) if actual_exists else None
        )
        marker_key = (key[0], key[1], marker_name)
        actual_marker_present = marker_key in self.extended_properties
        actual_marker_value = self.extended_properties.get(marker_key)
        actual_reserved_marker_count = sum(
            1
            for property_schema, property_view, property_name in self.extended_properties
            if (property_schema, property_view) == key
            and property_name.startswith("AzureSqlMcp_View_v1_")
        )
        return (
            actual_exists,
            actual_object_id,
            len(self.view_indexes.get(key, [])),
            actual_definition_hash,
            actual_schema_bound,
            actual_ansi_nulls,
            actual_quoted_identifier,
            actual_marker_present,
            actual_marker_value,
            actual_reserved_marker_count,
        )

    async def execute_batches(self, database_name, sql, params=(), **kwargs):
        self.batch_history.append((database_name, sql))
        if self.before_execute is not None:
            self.before_execute()

        fence = re.search(
            r"-- AzureSqlMcp view fence "
            r"expected_exists=(?P<expected_exists>\d+) "
            r"expected_object_id=(?P<expected_object_id>NULL|\d+) "
            r"expected_index_count=(?P<expected_index_count>\d+) "
            r"expected_definition_sha256=(?P<expected_definition_sha256>[0-9a-f]+|NULL) "
            r"expected_schema_bound=(?P<expected_schema_bound>0|1|NULL) "
            r"expected_uses_ansi_nulls=(?P<expected_uses_ansi_nulls>0|1|NULL) "
            r"expected_uses_quoted_identifier=(?P<expected_uses_quoted_identifier>0|1|NULL) "
            r"expected_marker_present=(?P<expected_marker_present>\d+) "
            r"expected_marker_value=(?P<expected_marker_value>[^ ]+) "
            r"expected_reserved_marker_count=(?P<expected_reserved_marker_count>\d+) "
            r"marker_name=(?P<marker_name>[^ ]+) marker_action=(?P<marker_action>add|drop|drop_with_view)",
            sql,
            flags=re.IGNORECASE,
        )
        if fence:
            expected_exists = fence.group("expected_exists") == "1"
            expected_object_id = (
                None
                if fence.group("expected_object_id").casefold() == "null"
                else int(fence.group("expected_object_id"))
            )
            expected_index_count = int(fence.group("expected_index_count"))
            expected_definition_hash = (
                None
                if fence.group("expected_definition_sha256").casefold() == "null"
                else fence.group("expected_definition_sha256").casefold()
            )
            expected_schema_bound = _fence_bit(fence.group("expected_schema_bound"))
            expected_ansi_nulls = _fence_bit(
                fence.group("expected_uses_ansi_nulls")
            )
            expected_quoted_identifier = _fence_bit(
                fence.group("expected_uses_quoted_identifier")
            )
            expected_marker_present = fence.group("expected_marker_present") == "1"
            expected_marker_value = (
                None
                if fence.group("expected_marker_value").casefold() == "null"
                else fence.group("expected_marker_value")
            )
            expected_reserved_marker_count = int(
                fence.group("expected_reserved_marker_count")
            )
            marker_name = fence.group("marker_name")
            marker_action = fence.group("marker_action").casefold()
            view_match = re.search(
                r"WHERE s\.name = N'((?:''|[^'])*)' AND v\.name = N'((?:''|[^'])*)'",
                sql,
                flags=re.IGNORECASE,
            )
            if view_match is None:
                raise AssertionError("fenced view batch omitted its catalog target")
            schema_name, view_name = (
                value.replace("''", "'") for value in view_match.groups()
            )
            key = (schema_name, view_name)
            expected_state = (
                expected_exists,
                expected_object_id,
                expected_index_count,
                expected_definition_hash,
                expected_schema_bound,
                expected_ansi_nulls,
                expected_quoted_identifier,
                expected_marker_present,
                expected_marker_value,
                expected_reserved_marker_count,
            )
            if self._fence_state(key, marker_name) != expected_state:
                raise ViewWorkflowError("view mutation precondition no longer matches")

            marker_key = (key[0], key[1], marker_name)
            marker_before = self.extended_properties.get(marker_key)
            marker_was_present = marker_key in self.extended_properties
            if expected_exists:
                if self.at_lock_boundary is not None:
                    interleave = self.at_lock_boundary
                    self.at_lock_boundary = None
                    interleave()
                if marker_action == "add":
                    if marker_was_present or marker_key in self.extended_properties:
                        raise ViewWorkflowError(
                            "view mutation marker lock could not be acquired"
                        )
                    marker_value_match = re.search(
                        r"@value = N'((?:''|[^'])*)'",
                        sql,
                        flags=re.IGNORECASE,
                    )
                    if marker_value_match is None:
                        raise AssertionError("view apply omitted its marker value")
                    self.extended_properties[marker_key] = marker_value_match.group(
                        1
                    ).replace("''", "'")
                else:
                    if marker_key not in self.extended_properties:
                        raise ViewWorkflowError(
                            "view mutation marker lock could not be acquired"
                        )
                    self.extended_properties.pop(marker_key)
                locked_marker_present = marker_action == "add"
                locked_marker_value = (
                    self.extended_properties.get(marker_key)
                    if locked_marker_present
                    else None
                )
                locked_state = (
                    expected_exists,
                    expected_object_id,
                    expected_index_count,
                    expected_definition_hash,
                    expected_schema_bound,
                    expected_ansi_nulls,
                    expected_quoted_identifier,
                    locked_marker_present,
                    locked_marker_value,
                    expected_reserved_marker_count
                    + (1 if marker_action == "add" else -1),
                )
                if self._fence_state(key, marker_name) != locked_state:
                    if marker_was_present:
                        self.extended_properties[marker_key] = marker_before
                    else:
                        self.extended_properties.pop(marker_key, None)
                    raise ViewWorkflowError(
                        "view mutation precondition no longer matches"
                    )
            else:
                if self._fence_state(key, marker_name) != expected_state:
                    raise ViewWorkflowError(
                        "view mutation precondition no longer matches"
                    )
                if self.at_lock_boundary is not None:
                    interleave = self.at_lock_boundary
                    self.at_lock_boundary = None
                    interleave()

        module_sql = sql
        dynamic_batches = re.findall(
            r"EXEC\s+sys\.sp_executesql\s+N'((?:''|[^'])*)';",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )
        for dynamic_batch in dynamic_batches:
            decoded = dynamic_batch.replace("''", "'")
            if re.search(r"(?:CREATE|ALTER)\s+VIEW\b|DROP\s+VIEW\b", decoded, re.I):
                module_sql = decoded
                break
        match = re.search(
            r"(CREATE|ALTER) VIEW \[([^]]+)\]\.\[([^]]+)\]",
            module_sql,
            flags=re.IGNORECASE,
        )
        if match:
            operation, schema_name, view_name = match.groups()
            key = (schema_name, view_name)
            if operation.casefold() == "create" and key in self.views:
                raise ViewWorkflowError("CREATE VIEW target was created concurrently")
            if operation.casefold() == "create" or key not in self.object_ids:
                self.object_ids[key] = self._next_object_id
                self._next_object_id += 1
            self.views[key] = re.sub(
                r"\bALTER VIEW",
                "CREATE VIEW",
                module_sql,
                count=1,
                flags=re.IGNORECASE,
            )
            quoted_identifier = re.search(
                r"SET\s+QUOTED_IDENTIFIER\s+(ON|OFF)",
                sql,
                flags=re.IGNORECASE,
            )
            # Azure SQL Database exposes ANSI_NULLS as always ON.  A SET OFF
            # request cannot recreate legacy SQL Server module metadata.
            self.catalog_ansi_nulls[key] = True
            self.catalog_quoted_identifier[key] = (
                quoted_identifier is None
                or quoted_identifier.group(1).casefold() == "on"
            )
            if fence and not expected_exists and marker_action == "add":
                marker_key = (key[0], key[1], marker_name)
                marker_value_match = re.search(
                    r"@value = N'((?:''|[^'])*)'",
                    sql,
                    flags=re.IGNORECASE,
                )
                if marker_value_match is None:
                    raise AssertionError("view apply omitted its marker value")
                self.extended_properties[marker_key] = marker_value_match.group(
                    1
                ).replace("''", "'")
        elif module_sql.upper().startswith("DROP VIEW"):
            match = re.search(
                r"DROP VIEW \[([^]]+)\]\.\[([^]]+)\]",
                module_sql,
                flags=re.IGNORECASE,
            )
            if match:
                key = (match.group(1), match.group(2))
                self.views.pop(key, None)
                self.object_ids.pop(key, None)
                self.catalog_ansi_nulls.pop(key, None)
                self.catalog_quoted_identifier.pop(key, None)
                for property_key in tuple(self.extended_properties):
                    if property_key[:2] == key:
                        self.extended_properties.pop(property_key, None)
        return []


def _policy(*, allow_view_apply: bool = True) -> DatabasePolicySet:
    return DatabasePolicySet.from_mapping(
        {
            "version": 1,
            "databases": {
                "testdb": {
                    "environment": "test",
                    "allow_read": True,
                    "allow_view_apply": allow_view_apply,
                }
            },
        }
    )


def _admin(server_config_factory, tmp_path: Path, write_policy: WritePolicy) -> AdminPolicy:
    config = server_config_factory(
        access_mode=AccessMode.UNRESTRICTED,
        write_policy=write_policy,
        audit_dir=str(tmp_path),
        audit_full_sql=False,
    )
    return AdminPolicy(config)


@pytest.mark.asyncio
async def test_create_and_alter_preview_capture_exact_prior_definition(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor(
        "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[OldSales];"
    )
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    request = ViewChangeRequest(
        database_name="testdb",
        schema_name="dbo",
        view_name="SalesView",
        definition="SELECT [Id] FROM [dbo].[NewSales]",
        operation="alter",
        reviewed_intent=True,
        idempotency_key="view-case-1",
    )

    prepared = await service.prepare(request)
    preview = await service.preview(prepared)

    assert prepared.operation == "alter"
    assert "ALTER VIEW [dbo].[SalesView]" in (prepared.apply_sql or "")
    assert "OldSales" in prepared.rollback_sql
    assert preview["status"] == "dry_run"
    assert preview["apply_allowed"] is True
    assert len(executor.batch_history) == 0


@pytest.mark.asyncio
async def test_bracketed_view_identifiers_are_canonicalized_once_for_workflow_state(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor()
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    request = ViewChangeRequest(
        database_name="testdb",
        schema_name="[dbo]",
        view_name="[SalesView]",
        definition="SELECT [Id] FROM [dbo].[Sales]",
        operation="create",
        reviewed_intent=True,
        idempotency_key="bracketed-view-identifiers",
    )

    prepared = await service.prepare(request)

    assert prepared.request.schema_name == "dbo"
    assert prepared.request.view_name == "SalesView"
    assert prepared.prior.schema_name == "dbo"
    assert prepared.prior.view_name == "SalesView"
    assert "WHERE s.name = N'dbo' AND v.name = N'SalesView'" in (
        prepared.apply_sql or ""
    )
    assert "@level0name = N'dbo'" in (prepared.apply_sql or "")
    assert "[[dbo]]" not in (prepared.apply_sql or "")
    assert "[[SalesView]]" not in (prepared.apply_sql or "")
    assert "DROP VIEW [dbo].[SalesView]" in prepared.rollback_sql


@pytest.mark.asyncio
async def test_apply_is_denied_by_local_policy_before_dispatch(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor()
    service = ViewWorkflowService(
        executor,
        _policy(allow_view_apply=False),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[Sales]",
            reviewed_intent=True,
            idempotency_key="view-case-2",
        )
    )

    with pytest.raises(ViewPolicyError, match="does not allow"):
        await service.apply(prepared)

    assert executor.batch_history == []


@pytest.mark.asyncio
async def test_apply_batch_fences_catalog_state_inside_database_transaction(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor()
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[Sales]",
            reviewed_intent=True,
            idempotency_key="view-atomic-fence",
        )
    )

    batch = prepared.apply_sql or ""
    assert "BEGIN TRANSACTION;" in batch
    assert "sys.sp_getapplock" in batch
    assert "WITH (UPDLOCK, HOLDLOCK)" not in batch
    assert "sys.sp_addextendedproperty" in batch
    assert "expected_object_id=NULL" in batch
    assert "expected_index_count=0" in batch
    assert "expected_definition_sha256=NULL" in batch
    assert "expected_schema_bound=NULL" in batch
    assert "expected_uses_ansi_nulls=NULL" in batch
    assert "expected_uses_quoted_identifier=NULL" in batch
    assert "expected_reserved_marker_count=0" in batch


@pytest.mark.asyncio
async def test_existing_view_fence_locks_marker_before_target_ddl(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor(
        "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[OldSales];"
    )
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[NewSales]",
            operation="alter",
            reviewed_intent=True,
            idempotency_key="view-marker-lock-order",
        )
    )

    batch = prepared.apply_sql or ""
    marker_lock = "EXEC @marker_result = sys.sp_addextendedproperty"
    target_ddl = "EXEC sys.sp_executesql"
    assert marker_lock in batch
    assert batch.index(marker_lock) < batch.index(target_ddl)
    assert batch.count("FROM sys.views AS v") == 2


@pytest.mark.asyncio
async def test_apply_rejects_external_create_after_prepare_before_ddl(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor()
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[Sales]",
            reviewed_intent=True,
            idempotency_key="view-apply-race",
        )
    )

    def external_create() -> None:
        executor.views[("dbo", "SalesView")] = (
            "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[Sales];"
        )
        executor.object_ids[("dbo", "SalesView")] = 99

    executor.before_execute = external_create
    with pytest.raises(ViewWorkflowError, match="precondition"):
        await service.apply(prepared)

    assert executor.object_ids[("dbo", "SalesView")] == 99
    assert len(executor.batch_history) == 1


@pytest.mark.asyncio
async def test_rollback_rejects_external_change_after_fence_read(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor()
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[Sales]",
            reviewed_intent=True,
            idempotency_key="view-rollback-race",
        )
    )
    await service.apply(prepared)

    def external_alter() -> None:
        executor.views[("dbo", "SalesView")] = (
            "CREATE VIEW [dbo].[SalesView] WITH VIEW_METADATA AS "
            "SELECT [Id] FROM [dbo].[Sales];"
        )

    executor.before_execute = external_alter
    with pytest.raises(ViewWorkflowError, match="precondition"):
        await service.rollback(prepared)

    assert "VIEW_METADATA" in executor.views[("dbo", "SalesView")]
    assert len(executor.batch_history) == 2


@pytest.mark.asyncio
async def test_apply_aborts_after_external_alter_at_schema_lock_boundary(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor(
        "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[OldSales];"
    )
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[NewSales]",
            operation="alter",
            reviewed_intent=True,
            idempotency_key="view-lock-boundary-apply",
        )
    )

    def external_alter() -> None:
        executor.views[("dbo", "SalesView")] = (
            "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[ExternalSales];"
        )

    executor.at_lock_boundary = external_alter
    with pytest.raises(ViewWorkflowError, match="precondition"):
        await service.apply(prepared)

    assert executor.views[("dbo", "SalesView")].endswith("[ExternalSales];")
    assert executor.object_ids[("dbo", "SalesView")] == 17
    assert executor.extended_properties == {}
    assert len(executor.batch_history) == 1


@pytest.mark.asyncio
async def test_rollback_aborts_after_external_alter_at_schema_lock_boundary(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor()
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[Sales]",
            reviewed_intent=True,
            idempotency_key="view-lock-boundary-rollback",
        )
    )
    await service.apply(prepared)

    def external_alter() -> None:
        executor.views[("dbo", "SalesView")] = (
            "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[ExternalSales];"
        )

    executor.at_lock_boundary = external_alter
    with pytest.raises(ViewWorkflowError, match="precondition"):
        await service.rollback(prepared)

    assert executor.views[("dbo", "SalesView")].endswith("[ExternalSales];")
    assert executor.object_ids[("dbo", "SalesView")] == 18
    assert len(executor.batch_history) == 2
    assert len(executor.extended_properties) == 1


@pytest.mark.asyncio
async def test_apply_verify_and_exact_rollback_drops_only_new_view(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor()
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[Sales]",
            reviewed_intent=True,
            idempotency_key="view-case-3",
        )
    )

    applied = await service.apply(prepared)
    repeated = await service.apply(prepared)
    rolled_back = await service.rollback(prepared)

    assert applied["status"] == "completed"
    assert repeated["status"] == "already_applied"
    assert rolled_back["status"] == "rolled_back"
    assert prepared.rollback_sql.startswith("DROP VIEW")
    assert ("dbo", "SalesView") not in executor.views
    assert executor.extended_properties == {}
    assert [sql.split()[0] for _, sql in executor.batch_history] == ["SET", "SET"]
    assert "DROP VIEW [dbo].[SalesView]" in executor.batch_history[1][1]


@pytest.mark.asyncio
async def test_reserved_marker_collision_is_rejected_before_ddl(
    server_config_factory,
    tmp_path: Path,
) -> None:
    marker_name = "AzureSqlMcp_View_v1_" + "a" * 48
    second_marker_name = "AzureSqlMcp_View_v1_" + "b" * 48
    executor = FakeViewExecutor()
    executor.views[("dbo", "SalesView")] = (
        "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[OldSales];"
    )
    executor.object_ids[("dbo", "SalesView")] = 17
    executor.extended_properties[("dbo", "SalesView", marker_name)] = "external"
    executor.extended_properties[("dbo", "SalesView", second_marker_name)] = "older"
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )

    with pytest.raises(
        ViewWorkflowError,
        match="reserved Azure SQL MCP view marker already exists",
    ):
        await service.prepare(
            ViewChangeRequest(
                database_name="testdb",
                schema_name="dbo",
                view_name="SalesView",
                definition="SELECT [Id] FROM [dbo].[NewSales]",
                operation="alter",
                reviewed_intent=True,
                idempotency_key="marker-collision",
            )
        )

    assert executor.batch_history == []


@pytest.mark.asyncio
async def test_existing_view_rolls_back_with_alter_not_drop(
    server_config_factory,
    tmp_path: Path,
) -> None:
    prior_definition = "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[OldSales];"
    executor = FakeViewExecutor(prior_definition)
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[NewSales]",
            operation="alter",
            reviewed_intent=True,
            idempotency_key="view-case-4",
        )
    )

    await service.apply(prepared)
    result = await service.rollback(prepared)

    assert result["status"] == "rolled_back"
    assert executor.views[("dbo", "SalesView")] == prior_definition
    assert executor.extended_properties == {}
    assert all(not sql.upper().startswith("DROP VIEW") for _, sql in executor.batch_history)


@pytest.mark.asyncio
async def test_apply_rejects_drop_and_recreate_with_same_definition(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor(
        "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[OldSales];"
    )
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[NewSales]",
            operation="alter",
            reviewed_intent=True,
            idempotency_key="view-object-fence",
        )
    )
    executor.object_ids[("dbo", "SalesView")] = 99

    with pytest.raises(ViewWorkflowError, match="changed after preview"):
        await service.apply(prepared)

    assert executor.batch_history == []


@pytest.mark.asyncio
async def test_noop_apply_rejects_view_changed_after_prepare(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor(
        "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[Sales];"
    )
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[Sales]",
            operation="alter",
            reviewed_intent=True,
            idempotency_key="stale-noop",
        )
    )
    executor.views[("dbo", "SalesView")] = (
        "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[OtherSales];"
    )

    with pytest.raises(ViewWorkflowError, match="changed after preview"):
        await service.apply(prepared)

    assert executor.batch_history == []


@pytest.mark.asyncio
async def test_rollback_before_workflow_apply_is_fenced(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor()
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[Sales]",
            reviewed_intent=True,
            idempotency_key="view-never-applied",
        )
    )

    with pytest.raises(ViewWorkflowError, match="no confirmed apply receipt"):
        await service.rollback(prepared)

    assert executor.batch_history == []


@pytest.mark.asyncio
async def test_external_same_definition_is_not_owned_or_rolled_back(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor()
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[Sales]",
            reviewed_intent=True,
            idempotency_key="view-external-create",
        )
    )
    executor.views[("dbo", "SalesView")] = (
        "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[Sales];"
    )
    executor.object_ids[("dbo", "SalesView")] = 99

    applied = await service.apply(prepared)

    assert applied["status"] == "hold"
    assert applied["workflow_applied"] is False
    recovery_verification = await service.verify_view_change(prepared)
    assert recovery_verification.verified is False
    assert "marker" in recovery_verification.reason
    with pytest.raises(ViewWorkflowError, match="no confirmed apply receipt"):
        await service.rollback(prepared)
    assert executor.batch_history == []
    assert ("dbo", "SalesView") in executor.views


@pytest.mark.asyncio
async def test_marker_proves_commit_across_service_restart_without_audit_receipt(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor()
    first = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path / "first", WritePolicy.APPLY),
    )
    prepared = await first.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[Sales]",
            reviewed_intent=True,
            idempotency_key="marker-commit-proof",
        )
    )
    await first.apply(prepared)

    resumed = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path / "resumed", WritePolicy.APPLY),
    )
    resumed_prepared = prepared_view_change_from_state(
        prepared_view_change_state(prepared)
    )
    verification = await resumed.verify_view_change(resumed_prepared)

    assert verification.verified is True
    assert verification.marker_verified is True
    assert verification.workflow_commit_proven is True
    assert verification.actual.dispatch_proof is None

    resumed.register_apply_receipt(resumed_prepared, verification.actual)
    rolled_back = await resumed.rollback(resumed_prepared)
    assert rolled_back["status"] == "rolled_back"
    assert executor.extended_properties == {}


@pytest.mark.asyncio
async def test_external_header_change_after_apply_is_fenced_from_rollback(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor()
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[Sales]",
            reviewed_intent=True,
            idempotency_key="header-fence",
        )
    )

    await service.apply(prepared)
    executor.views[("dbo", "SalesView")] = (
        "CREATE VIEW [dbo].[SalesView] WITH VIEW_METADATA AS "
        "SELECT [Id] FROM [dbo].[Sales];"
    )

    with pytest.raises(ViewWorkflowError, match="fencing failed"):
        await service.rollback(prepared)

    assert len(executor.batch_history) == 1


@pytest.mark.asyncio
async def test_server_view_workflow_prepares_applies_verifies_and_rolls_back(
    server_config_factory,
    tmp_path: Path,
) -> None:
    app = AzureSqlMcpApplication(
        server_config_factory(
            access_mode=AccessMode.UNRESTRICTED,
            write_policy=WritePolicy.APPLY,
            profile=McpProfile.SANDBOX,
            performance_state_dir=str(tmp_path / "state"),
            persist_view_sql_state=True,
        )
    )
    executor = FakeViewExecutor()
    app.view_workflows = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )

    prepared = await app._prepare_view_change(
        "testdb",
        "dbo",
        "SalesView",
        "SELECT [Id] FROM [dbo].[Sales]",
        "create",
        False,
        False,
        "view-server-case",
    )
    applied = await app._apply_prepared_view_change(
        "testdb",
        prepared["change_id"],
        True,
        "view-server-case",
    )
    verified = await app._verify_view_change("testdb", prepared["change_id"])
    rolled_back = await app._rollback_view_change("testdb", prepared["change_id"])

    assert prepared["process_local"] is False
    assert prepared["durable"] is True
    assert prepared["restart_requires_reprepare"] is False
    assert prepared["raw_view_sql_persisted"] is True
    assert applied["status"] == "completed"
    assert verified["definition_verified"] is True
    assert verified["dependencies_verified"] is True
    assert rolled_back["status"] == "rolled_back"
    assert ("dbo", "SalesView") not in executor.views
    app.performance_store.close()


@pytest.mark.asyncio
async def test_view_apply_requires_durable_raw_sql_opt_in(
    server_config_factory,
    tmp_path: Path,
) -> None:
    app = AzureSqlMcpApplication(
        server_config_factory(
            access_mode=AccessMode.UNRESTRICTED,
            write_policy=WritePolicy.APPLY,
            profile=McpProfile.SANDBOX,
            performance_state_dir=":memory:",
            persist_view_sql_state=False,
        )
    )
    executor = FakeViewExecutor()
    app.view_workflows = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await app._prepare_view_change(
        "testdb",
        "dbo",
        "SalesView",
        "SELECT [Id] FROM [dbo].[Sales]",
        "create",
        False,
        False,
        "view-without-durable-state",
    )

    assert prepared["process_local"] is True
    with pytest.raises(PermissionError, match="PERSIST_VIEW_SQL_STATE"):
        await app._apply_prepared_view_change(
            "testdb",
            prepared["change_id"],
            True,
            "view-without-durable-state",
        )
    assert executor.batch_history == []
    app.performance_store.close()


@pytest.mark.asyncio
async def test_optimizer_view_preview_cannot_cross_into_sandbox_process(
    server_config_factory,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "cross-profile-state"
    executor = FakeViewExecutor()
    optimizer = AzureSqlMcpApplication(
        server_config_factory(
            profile=McpProfile.OPTIMIZER,
            performance_state_dir=str(state_dir),
            persist_view_sql_state=True,
        )
    )
    optimizer.view_workflows = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path / "optimizer-audit", WritePolicy.DISABLED),
    )
    preview = await optimizer._prepare_view_change(
        "testdb",
        "dbo",
        "SalesView",
        "SELECT [Id] FROM [dbo].[Sales]",
        "create",
        False,
        False,
        "cross-profile-view",
    )
    assert preview["process_local"] is True
    optimizer.performance_store.close()

    sandbox = AzureSqlMcpApplication(
        server_config_factory(
            access_mode=AccessMode.UNRESTRICTED,
            write_policy=WritePolicy.APPLY,
            profile=McpProfile.SANDBOX,
            performance_state_dir=str(state_dir),
            persist_view_sql_state=True,
        )
    )
    sandbox.view_workflows = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path / "sandbox-audit", WritePolicy.APPLY),
    )
    with pytest.raises(KeyError, match="Unknown view change intent"):
        await sandbox._apply_prepared_view_change(
            "testdb",
            preview["change_id"],
            True,
            "cross-profile-view",
        )

    durable = await sandbox._prepare_view_change(
        "testdb",
        "dbo",
        "SalesView",
        "SELECT [Id] FROM [dbo].[Sales]",
        "create",
        False,
        False,
        "cross-profile-view",
    )
    applied = await sandbox._apply_prepared_view_change(
        "testdb",
        durable["change_id"],
        True,
        "cross-profile-view",
    )

    assert durable["durable"] is True
    assert applied["status"] == "completed"
    sandbox.performance_store.close()


@pytest.mark.asyncio
async def test_durable_view_intent_rolls_back_after_server_restart(
    server_config_factory,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "restart-state"
    config = server_config_factory(
        access_mode=AccessMode.UNRESTRICTED,
        write_policy=WritePolicy.APPLY,
        profile=McpProfile.SANDBOX,
        performance_state_dir=str(state_dir),
        persist_view_sql_state=True,
    )
    executor = FakeViewExecutor(
        "CREATE VIEW [dbo].[SalesView] WITH VIEW_METADATA AS "
        "SELECT [Id] FROM [dbo].[OldSales];"
    )
    first = AzureSqlMcpApplication(config)
    first.view_workflows = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path / "first-audit", WritePolicy.APPLY),
    )
    prepared = await first._prepare_view_change(
        "testdb",
        "dbo",
        "SalesView",
        "SELECT [Id] FROM [dbo].[NewSales]",
        "alter",
        False,
        False,
        "restart-safe-view",
    )
    await first._apply_prepared_view_change(
        "testdb",
        prepared["change_id"],
        True,
        "restart-safe-view",
    )
    first.performance_store.close()

    restarted = AzureSqlMcpApplication(config)
    restarted.view_workflows = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path / "second-audit", WritePolicy.APPLY),
    )
    verified = await restarted._verify_view_change("testdb", prepared["change_id"])
    rolled_back = await restarted._rollback_view_change(
        "testdb",
        prepared["change_id"],
    )

    assert verified["definition_verified"] is True
    assert verified["intent_status"] == "applied"
    assert rolled_back["status"] == "rolled_back"
    assert (
        executor.views[("dbo", "SalesView")]
        == "CREATE VIEW [dbo].[SalesView] WITH VIEW_METADATA AS "
        "SELECT [Id] FROM [dbo].[OldSales];"
    )
    restarted.performance_store.close()


@pytest.mark.asyncio
async def test_failed_post_apply_verification_retains_durable_rollback_ownership(
    server_config_factory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "post-apply-hold-state"
    config = server_config_factory(
        access_mode=AccessMode.UNRESTRICTED,
        write_policy=WritePolicy.APPLY,
        profile=McpProfile.SANDBOX,
        performance_state_dir=str(state_dir),
        persist_view_sql_state=True,
    )
    executor = FakeViewExecutor()
    first = AzureSqlMcpApplication(config)
    first.view_workflows = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path / "first-audit", WritePolicy.APPLY),
    )
    prepared = await first._prepare_view_change(
        "testdb",
        "dbo",
        "SalesView",
        "SELECT [Id] FROM [dbo].[Sales]",
        "create",
        False,
        False,
        "post-apply-hold",
    )

    async def fail_post_apply_verification(change):
        verification = await first.view_workflows.verify(change)
        return replace(
            verification,
            verified=False,
            dependencies_verified=False,
            reason="synthetic post-apply dependency mismatch",
        )

    monkeypatch.setattr(
        first.view_workflows,
        "verify_view_change",
        fail_post_apply_verification,
    )
    held = await first._apply_prepared_view_change(
        "testdb",
        prepared["change_id"],
        True,
        "post-apply-hold",
    )
    durable = first.performance_store.get_view_change_intent(
        prepared["change_id"]
    )

    assert held["status"] == "hold"
    assert held["workflow_applied"] is True
    assert durable["status"] == "hold"
    assert durable["receipt"] is not None
    first.performance_store.close()

    restarted = AzureSqlMcpApplication(config)
    restarted.view_workflows = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path / "second-audit", WritePolicy.APPLY),
    )
    rolled_back = await restarted._rollback_view_change(
        "testdb",
        prepared["change_id"],
    )

    assert rolled_back["status"] == "rolled_back"
    assert ("dbo", "SalesView") not in executor.views
    restarted.performance_store.close()


@pytest.mark.asyncio
async def test_durable_view_intent_rejects_tampered_payload(
    server_config_factory,
    tmp_path: Path,
) -> None:
    config = server_config_factory(
        access_mode=AccessMode.UNRESTRICTED,
        write_policy=WritePolicy.APPLY,
        profile=McpProfile.SANDBOX,
        performance_state_dir=str(tmp_path / "tamper-state"),
        persist_view_sql_state=True,
    )
    executor = FakeViewExecutor()
    app = AzureSqlMcpApplication(config)
    app.view_workflows = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path / "audit", WritePolicy.APPLY),
    )
    prepared = await app._prepare_view_change(
        "testdb",
        "dbo",
        "SalesView",
        "SELECT [Id] FROM [dbo].[Sales]",
        "create",
        False,
        False,
        "tamper-view",
    )
    app.performance_store._connection.execute(
        "UPDATE view_change_intents SET payload = ? WHERE change_id = ?",
        (
            json.dumps({"database_name": "testdb", "definition": "SELECT 1"}),
            prepared["change_id"],
        ),
    )
    app.performance_store._connection.commit()

    with pytest.raises(ValueError, match="payload fingerprint"):
        await app._verify_view_change("testdb", prepared["change_id"])

    assert executor.batch_history == []
    app.performance_store.close()


@pytest.mark.asyncio
async def test_interrupted_view_apply_recovers_from_marker_without_replaying_ddl(
    server_config_factory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "interrupted-state"
    config = server_config_factory(
        access_mode=AccessMode.UNRESTRICTED,
        write_policy=WritePolicy.APPLY,
        profile=McpProfile.SANDBOX,
        performance_state_dir=str(state_dir),
        persist_view_sql_state=True,
    )
    executor = FakeViewExecutor()
    first = AzureSqlMcpApplication(config)
    first.view_workflows = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path / "first-audit", WritePolicy.APPLY),
    )
    prepared = await first._prepare_view_change(
        "testdb",
        "dbo",
        "SalesView",
        "SELECT [Id] FROM [dbo].[Sales]",
        "create",
        False,
        False,
        "interrupted-view",
    )
    original_update = first.performance_store.update_view_change_intent

    def interrupt_final_state(change_id, **kwargs):
        if kwargs["status"] == "applied":
            raise RuntimeError("synthetic process interruption")
        return original_update(change_id, **kwargs)

    monkeypatch.setattr(
        first.performance_store,
        "update_view_change_intent",
        interrupt_final_state,
    )
    with pytest.raises(RuntimeError, match="synthetic process interruption"):
        await first._apply_prepared_view_change(
            "testdb",
            prepared["change_id"],
            True,
            "interrupted-view",
        )
    assert len(executor.batch_history) == 1
    first.performance_store.close()

    restarted = AzureSqlMcpApplication(config)
    restarted.view_workflows = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path / "second-audit", WritePolicy.APPLY),
    )
    reconciled = await restarted._verify_view_change(
        "testdb",
        prepared["change_id"],
    )
    assert reconciled["status"] == "reconciled_applied"
    assert reconciled["intent_status"] == "applied"
    assert reconciled["workflow_applied"] is True
    assert "workflow marker" in reconciled["reason"]
    assert len(executor.batch_history) == 1
    rolled_back = await restarted._rollback_view_change(
        "testdb",
        prepared["change_id"],
    )
    assert rolled_back["status"] == "rolled_back"
    assert ("dbo", "SalesView") not in executor.views
    assert executor.extended_properties == {}
    restarted.performance_store.close()


@pytest.mark.asyncio
async def test_prepared_view_state_round_trip_revalidates_exact_rollback(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor(
        "-- retained\n"
        "CREATE VIEW [dbo].[SalesView] ([Id]) WITH VIEW_METADATA AS "
        "SELECT [Id] FROM [dbo].[OldSales];"
    )
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[NewSales]",
            operation="alter",
        )
    )

    restored = prepared_view_change_from_state(prepared_view_change_state(prepared))

    assert restored.target_fingerprint == prepared.target_fingerprint
    assert restored.apply_sql == prepared.apply_sql
    assert restored.rollback_sql == prepared.rollback_sql


def test_view_legality_reports_indexed_view_restrictions_without_schema_lookup() -> None:
    report = validate_view_definition(
        "SELECT DISTINCT [Id] FROM [dbo].[Sales]",
        indexed_view=True,
        schema_bound=False,
    )

    assert report.valid is False
    assert any("SCHEMABINDING" in error for error in report.errors)
    assert any("DISTINCT" in error for error in report.errors)


def test_view_body_fingerprint_does_not_strip_cast_alias_or_string_content() -> None:
    body = "SELECT CAST([Amount] AS decimal(19,4)) AS [Amount], 'GO; AS' AS [Note] FROM [dbo].[Sales]"
    module_definition = f"CREATE VIEW [dbo].[SalesView] AS {body};"

    assert view_definition_fingerprint(body) == view_definition_fingerprint(
        module_definition
    )
    assert validate_view_definition(body).valid is True


def test_view_fingerprint_includes_normalized_operation_independent_header() -> None:
    create = (
        "CREATE VIEW [dbo].[SalesView] ([Id], [Amount]) "
        "WITH VIEW_METADATA, SCHEMABINDING AS "
        "SELECT [Id], [Amount] FROM [dbo].[Sales];"
    )
    alter = create.replace("CREATE VIEW", "ALTER VIEW").replace(
        "WITH VIEW_METADATA, SCHEMABINDING",
        "WITH SCHEMABINDING, VIEW_METADATA",
    )
    no_header_options = "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[Sales];"

    assert view_definition_fingerprint(create) == view_definition_fingerprint(alter)
    assert view_definition_fingerprint(create) != view_definition_fingerprint(
        no_header_options
    )


@pytest.mark.parametrize(
    ("definition", "message"),
    [
        ("SELECT * FROM [dbo].[Sales]", "SELECT *"),
        ("SELECT [Id] FROM [Sales]", "two-part"),
        ("SELECT [Id] FROM [archive].[dbo].[Sales]", "two-part"),
        (
            "WITH SalesCte AS (SELECT [Id] FROM [dbo].[Sales]) "
            "SELECT [Id] FROM SalesCte",
            "CTEs",
        ),
        (
            "SELECT [Id] FROM [dbo].[Sales] "
            "WHERE [Id] IN (SELECT [Id] FROM [dbo].[Other])",
            "subqueries",
        ),
        (
            "SELECT [Id] FROM [dbo].[Sales] UNION SELECT [Id] FROM [dbo].[Other]",
            "set operators",
        ),
        (
            "SELECT s.[Id] FROM [dbo].[Sales] AS s "
            "LEFT JOIN [dbo].[Other] AS o ON o.[Id] = s.[Id]",
            "outer joins",
        ),
        ("SELECT DISTINCT [Id] FROM [dbo].[Sales]", "DISTINCT"),
        ("SELECT TOP 1 [Id] FROM [dbo].[Sales]", "TOP"),
        ("SELECT [Id] FROM [dbo].[Sales] ORDER BY [Id] OFFSET 1 ROWS", "OFFSET"),
        (
            "SELECT s.[Id] FROM [dbo].[Sales] AS s CROSS APPLY [dbo].[Expand](s.[Id])",
            "APPLY",
        ),
        (
            "SELECT a.[Id] FROM [dbo].[Sales] AS a "
            "JOIN [dbo].[Sales] AS b ON b.[Id] = a.[Id]",
            "self-joins",
        ),
        ("SELECT [Id] FROM [dbo].[Sales] WITH (NOLOCK)", "table hints"),
        ("SELECT GETDATE() FROM [dbo].[Sales]", "nondeterministic"),
        ("SELECT AVG([Amount]) FROM [dbo].[Sales]", "AVG"),
        ("SELECT SUM([Amount]) FROM [dbo].[Sales]", "SUM"),
        (
            "SELECT ROW_NUMBER() OVER (ORDER BY [Id]) FROM [dbo].[Sales]",
            "window",
        ),
        (
            "SELECT [Id], COUNT(*) FROM [dbo].[Sales] GROUP BY [Id]",
            "COUNT_BIG",
        ),
    ],
)
def test_indexed_and_schema_bound_views_reject_uncertain_definitions(
    definition: str,
    message: str,
) -> None:
    report = validate_view_definition(
        definition,
        indexed_view=True,
        schema_bound=True,
    )

    assert report.valid is False
    assert any(message.casefold() in error.casefold() for error in report.errors)


def test_indexed_view_execution_batch_emits_all_required_set_options() -> None:
    batch = build_view_execution_batch(
        "CREATE VIEW [dbo].[SalesView] WITH SCHEMABINDING AS "
        "SELECT [Id] FROM [dbo].[Sales];",
        uses_ansi_nulls=False,
        uses_quoted_identifier=False,
        indexed_view=True,
    )

    for name, value in (
        ("ANSI_NULLS", "ON"),
        ("QUOTED_IDENTIFIER", "ON"),
        ("ANSI_PADDING", "ON"),
        ("ANSI_WARNINGS", "ON"),
        ("ARITHABORT", "ON"),
        ("CONCAT_NULL_YIELDS_NULL", "ON"),
        ("NUMERIC_ROUNDABORT", "OFF"),
    ):
        assert f"SET {name} {value};" in batch


@pytest.mark.asyncio
async def test_invalid_indexed_definition_fails_before_ddl(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor()
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )

    with pytest.raises(ViewDefinitionError, match=r"SELECT \*"):
        await service.prepare(
            ViewChangeRequest(
                database_name="testdb",
                schema_name="dbo",
                view_name="SalesView",
                definition="SELECT * FROM [dbo].[Sales]",
                indexed_view=True,
                schema_bound=True,
            )
        )

    assert executor.batch_history == []


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_bound,target_bound", [(False, True), (True, False)])
async def test_schema_binding_state_is_part_of_target_and_verification(
    server_config_factory,
    tmp_path: Path,
    prior_bound: bool,
    target_bound: bool,
) -> None:
    binding = " WITH SCHEMABINDING" if prior_bound else ""
    executor = FakeViewExecutor(
        f"CREATE VIEW [dbo].[SalesView]{binding} AS SELECT [Id] FROM [dbo].[Sales];"
    )
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[Sales]",
            operation="alter",
            reviewed_intent=True,
            idempotency_key=f"binding-{prior_bound}-{target_bound}",
            schema_bound=target_bound,
        )
    )

    assert prepared.prior.schema_bound is prior_bound
    assert prepared.prior.definition_fingerprint != prepared.target_fingerprint
    applied = await service.apply(prepared)

    assert applied["status"] == "completed"
    assert applied["verification"]["definition_verified"] is True
    assert prepared.prior.schema_bound is not target_bound
    assert ("WITH SCHEMABINDING" in (prepared.apply_sql or "")) is target_bound


@pytest.mark.asyncio
async def test_catalog_binding_metadata_is_preferred_to_module_body_text(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor(
        "CREATE VIEW [dbo].[SalesView] AS "
        "SELECT 'WITH SCHEMABINDING' AS [Note] FROM [dbo].[Sales];"
    )
    executor.catalog_schema_bound[("dbo", "SalesView")] = True
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )

    snapshot = await service.capture_view("testdb", "dbo", "SalesView")

    assert snapshot.schema_bound is True


@pytest.mark.asyncio
async def test_fallback_binding_parse_ignores_comments_and_literals_in_body(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor(
        "CREATE VIEW [dbo].[SalesView] AS "
        "SELECT 'WITH SCHEMABINDING' AS [Note], /* VIEW_METADATA */ [Id] "
        "FROM [dbo].[Sales];"
    )
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )

    snapshot = await service.capture_view("testdb", "dbo", "SalesView")

    assert snapshot.schema_bound is False


@pytest.mark.asyncio
async def test_exact_rollback_preserves_column_list_and_multiple_view_attributes(
    server_config_factory,
    tmp_path: Path,
) -> None:
    prior_definition = (
        "CREATE VIEW [dbo].[SalesView] ([Id], [Amount]) "
        "WITH VIEW_METADATA, SCHEMABINDING AS "
        "SELECT [Id], [Amount] FROM [dbo].[OldSales];"
    )
    executor = FakeViewExecutor(prior_definition)
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id], [Amount] FROM [dbo].[NewSales]",
            operation="alter",
            reviewed_intent=True,
            idempotency_key="exact-header-rollback",
        )
    )

    assert (
        "ALTER VIEW [dbo].[SalesView] ([Id], [Amount]) "
        "WITH VIEW_METADATA, SCHEMABINDING AS "
        "SELECT [Id], [Amount] FROM [dbo].[OldSales];"
    ) in prepared.rollback_sql

    await service.apply(prepared)
    result = await service.rollback(prepared)

    assert result["status"] == "rolled_back"
    assert executor.views[("dbo", "SalesView")] == prior_definition


@pytest.mark.asyncio
async def test_exact_rollback_preserves_leading_comments(
    server_config_factory,
    tmp_path: Path,
) -> None:
    prior_definition = (
        "-- view owner note\n"
        "/* retained during rollback */\n"
        "CREATE VIEW [dbo].[SalesView] WITH VIEW_METADATA AS "
        "SELECT [Id] FROM [dbo].[OldSales];"
    )
    executor = FakeViewExecutor(prior_definition)
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[NewSales]",
            operation="alter",
            reviewed_intent=True,
            idempotency_key="leading-comment-rollback",
        )
    )

    assert (
        "-- view owner note\n/* retained during rollback */\nALTER VIEW"
    ) in prepared.rollback_sql
    await service.apply(prepared)
    result = await service.rollback(prepared)

    assert result["status"] == "rolled_back"
    assert executor.views[("dbo", "SalesView")] == prior_definition


@pytest.mark.asyncio
async def test_prepare_fails_closed_for_unsupported_view_header(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor(
        "CREATE VIEW [dbo].[SalesView] WITH (CUSTOM_ATTRIBUTE) AS "
        "SELECT [Id] FROM [dbo].[Sales];"
    )
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )

    with pytest.raises(ViewWorkflowError):
        await service.prepare(
            ViewChangeRequest(
                database_name="testdb",
                schema_name="dbo",
                view_name="SalesView",
                definition="SELECT [Id] FROM [dbo].[NewSales]",
                operation="alter",
                reviewed_intent=True,
                idempotency_key="unsupported-header",
            )
        )


@pytest.mark.asyncio
async def test_prepare_rejects_indexed_existing_view_before_any_ddl_dispatch(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor(
        "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[OldSales];"
    )
    executor.view_indexes[("dbo", "SalesView")] = [{"index_id": 1}]
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )

    with pytest.raises(ViewWorkflowError, match="existing indexes"):
        await service.prepare(
            ViewChangeRequest(
                database_name="testdb",
                schema_name="dbo",
                view_name="SalesView",
                definition="SELECT [Id] FROM [dbo].[NewSales]",
                operation="alter",
                reviewed_intent=True,
                idempotency_key="indexed-view-rejected",
            )
        )

    assert executor.batch_history == []


@pytest.mark.asyncio
async def test_apply_rejects_index_added_after_prepare_before_any_ddl_dispatch(
    server_config_factory,
    tmp_path: Path,
) -> None:
    executor = FakeViewExecutor(
        "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[OldSales];"
    )
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT [Id] FROM [dbo].[NewSales]",
            operation="alter",
            reviewed_intent=True,
            idempotency_key="indexed-view-race",
        )
    )
    executor.view_indexes[("dbo", "SalesView")] = [{"index_id": 1}]

    with pytest.raises(ViewWorkflowError, match="existing indexes"):
        await service.apply(prepared)

    assert executor.batch_history == []


def test_view_fingerprint_preserves_literal_and_quoted_identifier_spelling() -> None:
    assert view_definition_fingerprint("SELECT 'A  B' AS [Note]") != (
        view_definition_fingerprint("SELECT 'a b' AS [Note]")
    )
    assert view_definition_fingerprint("SELECT 'A B' AS [Note]") != (
        view_definition_fingerprint("SELECT 'A  B' AS [Note]")
    )
    assert view_definition_fingerprint("SELECT [Customer Name]") != (
        view_definition_fingerprint("SELECT [customer name]")
    )
    assert view_definition_fingerprint("SELECT [Customer  Name]") != (
        view_definition_fingerprint("SELECT [Customer Name]")
    )
    assert view_definition_fingerprint("SELECT CustomerId FROM dbo.Sales") != (
        view_definition_fingerprint("SELECT customerid FROM dbo.sales")
    )
    assert view_definition_fingerprint(
        "SELECT [Id] FROM [dbo].[Sales]",
        uses_ansi_nulls=False,
    ) != view_definition_fingerprint(
        "SELECT [Id] FROM [dbo].[Sales]",
        uses_ansi_nulls=True,
    )
    assert view_definition_fingerprint(
        "SELECT [Id] FROM [dbo].[Sales]",
        uses_quoted_identifier=False,
    ) != view_definition_fingerprint(
        "SELECT [Id] FROM [dbo].[Sales]",
        uses_quoted_identifier=True,
    )


@pytest.mark.asyncio
async def test_prepare_rejects_legacy_ansi_nulls_off_view(
    server_config_factory,
    tmp_path: Path,
) -> None:
    key = ("dbo", "SalesView")
    executor = FakeViewExecutor(
        "CREATE VIEW [dbo].[SalesView] AS SELECT [Id] FROM [dbo].[OldSales];"
    )
    executor.legacy_ansi_nulls[key] = False
    executor.catalog_quoted_identifier[key] = False
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    with pytest.raises(ViewWorkflowError, match=r"ANSI_NULLS.*always ON"):
        await service.prepare(
            ViewChangeRequest(
                database_name="testdb",
                schema_name="dbo",
                view_name="SalesView",
                definition="SELECT [Id] FROM [dbo].[NewSales]",
                operation="alter",
                reviewed_intent=True,
                idempotency_key="preserve-view-set-options",
            )
        )

    assert executor.batch_history == []


def test_unqualified_functions_are_not_reported_as_view_dependencies() -> None:
    dependencies = extract_view_dependencies(
        "SELECT NormalizeSku([Sku]), HASHBYTES('SHA2_256', [Sku]) "
        "FROM [dbo].[Sales]"
    )

    assert dependencies == ("dbo.sales",)


def test_direct_cross_database_view_dependencies_are_rejected() -> None:
    report = validate_view_definition(
        "SELECT [Id] FROM [archive].[dbo].[Sales]"
    )

    assert report.valid is False
    assert any(
        "cannot use direct three- or four-part cross-database" in error
        for error in report.errors
    )


@pytest.mark.asyncio
async def test_function_dependencies_are_included_in_view_verification(
    server_config_factory,
    tmp_path: Path,
) -> None:
    definition = (
        "CREATE VIEW [dbo].[SalesView] AS "
        "SELECT dbo.NormalizeSku([Sku]) FROM [dbo].[Sales];"
    )
    executor = FakeViewExecutor(definition)
    executor.catalog_dependencies[("dbo", "SalesView")] = (
        "dbo.NormalizeSku",
        "dbo.Sales",
    )
    service = ViewWorkflowService(
        executor,
        _policy(),
        _admin(server_config_factory, tmp_path, WritePolicy.APPLY),
    )
    prepared = await service.prepare(
        ViewChangeRequest(
            database_name="testdb",
            schema_name="dbo",
            view_name="SalesView",
            definition="SELECT dbo.NormalizeSku([Sku]) FROM [dbo].[Sales]",
            operation="alter",
            reviewed_intent=True,
            idempotency_key="function-dependency-verification",
        )
    )

    assert "dbo.normalizesku" in prepared.target_dependencies
    verification = await service.verify(prepared)

    assert verification.verified is True
    assert verification.dependencies_verified is True
