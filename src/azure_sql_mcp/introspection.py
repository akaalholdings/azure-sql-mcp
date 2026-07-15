from __future__ import annotations

import asyncio
from typing import Any

from .connection import AzureSqlExecutor

OBJECT_TYPE_MAP = {
    "table": ("U",),
    "view": ("V",),
    "procedure": ("P",),
    "function": ("FN", "IF", "TF", "FS", "FT", "AF"),
}


class IntrospectionService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def list_schemas(self, database_name: str) -> list[dict[str, Any]]:
        query = """
        SELECT
            s.name AS schema_name,
            dp.name AS schema_owner,
            CASE
                WHEN s.name IN ('sys', 'INFORMATION_SCHEMA') THEN 'system'
                ELSE 'user'
            END AS schema_type
        FROM sys.schemas AS s
        LEFT JOIN sys.database_principals AS dp
            ON s.principal_id = dp.principal_id
        ORDER BY
            CASE WHEN s.name IN ('sys', 'INFORMATION_SCHEMA') THEN 0 ELSE 1 END,
            s.name
        """
        return await self.executor.fetch_all(database_name, query)

    async def list_objects(
        self,
        database_name: str,
        schema_name: str,
        object_type: str,
    ) -> list[dict[str, Any]]:
        object_type = object_type.lower()
        if object_type == "index":
            return await self._list_indexes(database_name, schema_name)
        if object_type not in OBJECT_TYPE_MAP:
            raise ValueError(
                "Unsupported object_type. Use table, view, procedure, function, or index."
            )

        type_codes = OBJECT_TYPE_MAP[object_type]
        placeholders = ",".join("?" for _ in type_codes)
        query = f"""
        SELECT
            s.name AS schema_name,
            o.name AS object_name,
            o.type AS object_type_code,
            o.type_desc AS object_type,
            o.create_date,
            o.modify_date
        FROM sys.objects AS o
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_id
        WHERE s.name = ?
          AND o.type IN ({placeholders})
        ORDER BY o.name
        """
        params = [schema_name, *type_codes]
        return await self.executor.fetch_all(database_name, query, params=params)

    async def search_objects(
        self,
        database_name: str,
        pattern: str,
        object_type: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_type = object_type.lower() if object_type else None
        type_codes: tuple[str, ...] = ()
        if normalized_type:
            if normalized_type not in OBJECT_TYPE_MAP:
                raise ValueError(
                    "Unsupported object_type. Use table, view, procedure, or function."
                )
            type_codes = OBJECT_TYPE_MAP[normalized_type]

        query = """
        SELECT
            s.name AS schema_name,
            o.name AS object_name,
            o.type AS object_type_code,
            o.type_desc AS object_type,
            o.create_date,
            o.modify_date
        FROM sys.objects AS o
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_id
        WHERE o.name LIKE ?
          AND o.is_ms_shipped = 0
        """
        params: list[Any] = [pattern]
        if type_codes:
            placeholders = ",".join("?" for _ in type_codes)
            query += f"\n  AND o.type IN ({placeholders})"
            params.extend(type_codes)

        query += "\nORDER BY s.name, o.name"
        return await self.executor.fetch_all(database_name, query, params=params)

    async def get_dependencies(
        self,
        database_name: str,
        schema_name: str,
        object_name: str,
    ) -> dict[str, Any]:
        depends_on_query = """
        SELECT
            COALESCE(sed.referenced_schema_name, 'dbo') AS referenced_schema,
            sed.referenced_entity_name AS referenced_object,
            c.name AS referenced_column,
            sed.referencing_class_desc
        FROM sys.sql_expression_dependencies AS sed
        LEFT JOIN sys.columns AS c
            ON sed.referenced_id = c.object_id
           AND sed.referenced_minor_id = c.column_id
        WHERE sed.referencing_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
        ORDER BY referenced_schema, referenced_object, referenced_column
        """
        depended_on_by_query = """
        SELECT
            OBJECT_SCHEMA_NAME(sed.referencing_id) AS referencing_schema,
            OBJECT_NAME(sed.referencing_id) AS referencing_object,
            o.type_desc AS referencing_type
        FROM sys.sql_expression_dependencies AS sed
        INNER JOIN sys.objects AS o
            ON sed.referencing_id = o.object_id
        WHERE sed.referenced_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
        ORDER BY referencing_schema, referencing_object
        """
        params = [schema_name, object_name]
        depends_on = await self.executor.fetch_all(
            database_name,
            depends_on_query,
            params=params,
        )
        depended_on_by = await self.executor.fetch_all(
            database_name,
            depended_on_by_query,
            params=params,
        )
        return {
            "schema_name": schema_name,
            "object_name": object_name,
            "depends_on": [
                {
                    "schema": row.get("referenced_schema"),
                    "object": row.get("referenced_object"),
                    "column": row.get("referenced_column"),
                    "type": row.get("referencing_class_desc"),
                }
                for row in depends_on
            ],
            "depended_on_by": [
                {
                    "schema": row.get("referencing_schema"),
                    "object": row.get("referencing_object"),
                    "type": row.get("referencing_type"),
                }
                for row in depended_on_by
            ],
        }

    async def get_table_stats(
        self,
        database_name: str,
        schema_name: str | None = None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            SUM(p.rows) AS approximate_row_count,
            CAST(SUM(au.total_pages) * 8.0 / 1024 AS DECIMAL(18, 2)) AS total_size_mb,
            CAST(SUM(au.used_pages) * 8.0 / 1024 AS DECIMAL(18, 2)) AS used_size_mb,
            CAST(
                SUM(CASE WHEN au.type = 1 THEN au.used_pages ELSE 0 END) * 8.0 / 1024
                AS DECIMAL(18, 2)
            ) AS data_size_mb,
            CAST(
                SUM(CASE WHEN au.type = 2 THEN au.used_pages ELSE 0 END) * 8.0 / 1024
                AS DECIMAL(18, 2)
            ) AS index_size_mb,
            COUNT(DISTINCT i.index_id) - 1 AS index_count
        FROM sys.tables AS t
        INNER JOIN sys.schemas AS s
            ON t.schema_id = s.schema_id
        INNER JOIN sys.partitions AS p
            ON t.object_id = p.object_id
           AND p.index_id IN (0, 1)
        INNER JOIN sys.allocation_units AS au
            ON p.partition_id = au.container_id
        LEFT JOIN sys.indexes AS i
            ON t.object_id = i.object_id
           AND i.index_id > 0
        WHERE (? IS NULL OR s.name = ?)
        GROUP BY s.name, t.name
        ORDER BY SUM(p.rows) DESC, s.name, t.name
        """
        params = [schema_name, schema_name]
        return await self.executor.fetch_all(database_name, query, params=params)

    async def get_object_details(
        self,
        database_name: str,
        schema_name: str,
        object_name: str,
        object_type: str,
    ) -> dict[str, Any]:
        object_type = object_type.lower()
        if object_type in {"table", "view"}:
            return await self._get_table_or_view_details(
                database_name,
                schema_name,
                object_name,
                object_type,
            )
        if object_type in {"procedure", "function"}:
            return await self._get_programmable_object_details(
                database_name,
                schema_name,
                object_name,
                object_type,
            )
        if object_type == "index":
            return await self._get_index_details(database_name, schema_name, object_name)
        raise ValueError(
            "Unsupported object_type. Use table, view, procedure, function, or index."
        )

    async def _list_indexes(
        self,
        database_name: str,
        schema_name: str,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT
            schema_name(t.schema_id) AS schema_name,
            t.name AS table_name,
            i.name AS index_name,
            i.type_desc AS index_type,
            i.is_unique,
            i.is_primary_key
        FROM sys.indexes AS i
        INNER JOIN sys.tables AS t
            ON i.object_id = t.object_id
        WHERE schema_name(t.schema_id) = ?
          AND i.name IS NOT NULL
        ORDER BY t.name, i.name
        """
        return await self.executor.fetch_all(database_name, query, params=[schema_name])

    async def _get_table_or_view_details(
        self,
        database_name: str,
        schema_name: str,
        object_name: str,
        object_type: str,
    ) -> dict[str, Any]:
        columns_query = """
        SELECT
            c.column_id,
            c.name AS column_name,
            t.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            dc.definition AS default_definition
        FROM sys.columns AS c
        INNER JOIN sys.types AS t
            ON c.user_type_id = t.user_type_id
        LEFT JOIN sys.default_constraints AS dc
            ON c.default_object_id = dc.object_id
        WHERE c.object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
        ORDER BY c.column_id
        """
        constraints_query = """
        SELECT
            kc.name AS constraint_name,
            kc.type_desc AS constraint_type,
            c.name AS column_name
        FROM sys.key_constraints AS kc
        LEFT JOIN sys.index_columns AS ic
            ON kc.parent_object_id = ic.object_id
           AND kc.unique_index_id = ic.index_id
        LEFT JOIN sys.columns AS c
            ON ic.object_id = c.object_id
           AND ic.column_id = c.column_id
        WHERE kc.parent_object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
        UNION ALL
        SELECT
            fk.name AS constraint_name,
            fk.type_desc AS constraint_type,
            pc.name AS column_name
        FROM sys.foreign_keys AS fk
        INNER JOIN sys.foreign_key_columns AS fkc
            ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.columns AS pc
            ON fkc.parent_object_id = pc.object_id
           AND fkc.parent_column_id = pc.column_id
        WHERE fk.parent_object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
        ORDER BY constraint_name, column_name
        """
        indexes_query = """
        SELECT
            i.name AS index_name,
            i.type_desc AS index_type,
            i.is_unique,
            i.is_primary_key,
            STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns
        FROM sys.indexes AS i
        LEFT JOIN sys.index_columns AS ic
            ON i.object_id = ic.object_id
           AND i.index_id = ic.index_id
           AND ic.is_included_column = 0
        LEFT JOIN sys.columns AS c
            ON ic.object_id = c.object_id
           AND ic.column_id = c.column_id
        WHERE i.object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
          AND i.name IS NOT NULL
        GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key
        ORDER BY i.name
        """
        definition_query = """
        SELECT m.definition
        FROM sys.sql_modules AS m
        INNER JOIN sys.objects AS o
            ON m.object_id = o.object_id
        WHERE o.object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
        """

        params = [schema_name, object_name]
        columns, constraints, indexes, definition_rows = await asyncio.gather(
            self.executor.fetch_all(database_name, columns_query, params=params),
            self.executor.fetch_all(
                database_name,
                constraints_query,
                params=[schema_name, object_name, schema_name, object_name],
            ),
            self.executor.fetch_all(database_name, indexes_query, params=params),
            self.executor.fetch_all(
                database_name,
                definition_query,
                params=params,
            ),
        )

        return {
            "schema_name": schema_name,
            "object_name": object_name,
            "object_type": object_type,
            "columns": columns,
            "constraints": constraints,
            "indexes": indexes,
            "definition": definition_rows[0]["definition"] if definition_rows else None,
        }

    async def _get_programmable_object_details(
        self,
        database_name: str,
        schema_name: str,
        object_name: str,
        object_type: str,
    ) -> dict[str, Any]:
        query = """
        SELECT
            s.name AS schema_name,
            o.name AS object_name,
            o.type_desc AS object_type,
            o.create_date,
            o.modify_date,
            m.definition
        FROM sys.objects AS o
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_id
        LEFT JOIN sys.sql_modules AS m
            ON o.object_id = m.object_id
        WHERE s.name = ?
          AND o.name = ?
        """
        rows = await self.executor.fetch_all(database_name, query, params=[schema_name, object_name])
        if not rows:
            raise ValueError(f"Object '{schema_name}.{object_name}' was not found.")
        row = rows[0]
        row["requested_type"] = object_type
        return row

    async def _get_index_details(
        self,
        database_name: str,
        schema_name: str,
        index_name: str,
    ) -> dict[str, Any]:
        query = """
        SELECT
            schema_name(t.schema_id) AS schema_name,
            t.name AS table_name,
            i.name AS index_name,
            i.type_desc AS index_type,
            i.is_unique,
            i.is_primary_key,
            i.is_disabled,
            STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns
        FROM sys.indexes AS i
        INNER JOIN sys.tables AS t
            ON i.object_id = t.object_id
        LEFT JOIN sys.index_columns AS ic
            ON i.object_id = ic.object_id
           AND i.index_id = ic.index_id
           AND ic.is_included_column = 0
        LEFT JOIN sys.columns AS c
            ON ic.object_id = c.object_id
           AND ic.column_id = c.column_id
        WHERE schema_name(t.schema_id) = ?
          AND i.name = ?
        GROUP BY
            t.schema_id,
            t.name,
            i.name,
            i.type_desc,
            i.is_unique,
            i.is_primary_key,
            i.is_disabled
        """
        rows = await self.executor.fetch_all(database_name, query, params=[schema_name, index_name])
        if not rows:
            raise ValueError(f"Index '{schema_name}.{index_name}' was not found.")
        return rows[0]
