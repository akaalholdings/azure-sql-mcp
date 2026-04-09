from __future__ import annotations

from typing import Any

from .connection import AzureSqlExecutor


class ResourceGovernanceService:
    def __init__(self, executor: AzureSqlExecutor):
        self.executor = executor

    async def get_io_stats(
        self,
        database_name: str,
    ) -> dict[str, Any]:
        """Per-file I/O stats: latency, throughput, pending I/O."""
        # Azure SQL DB user databases cannot read sys.master_files (server-scoped
        # catalog only visible from master). Use sys.database_files which is
        # available from each user database and joins to the per-file IO DMV.
        query = """
        SELECT
            df.name AS file_name,
            df.type_desc AS file_type,
            df.physical_name,
            CAST(df.size * 8.0 / 1024 AS DECIMAL(18, 2)) AS file_size_mb,
            fs.num_of_reads,
            fs.num_of_writes,
            fs.num_of_bytes_read / (1024.0 * 1024.0) AS read_mb,
            fs.num_of_bytes_written / (1024.0 * 1024.0) AS write_mb,
            CASE WHEN fs.num_of_reads > 0
                 THEN CAST(fs.io_stall_read_ms * 1.0 / fs.num_of_reads AS DECIMAL(18, 2))
                 ELSE 0
            END AS avg_read_latency_ms,
            CASE WHEN fs.num_of_writes > 0
                 THEN CAST(fs.io_stall_write_ms * 1.0 / fs.num_of_writes AS DECIMAL(18, 2))
                 ELSE 0
            END AS avg_write_latency_ms,
            fs.io_stall_read_ms,
            fs.io_stall_write_ms,
            fs.io_stall AS total_io_stall_ms
        FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) AS fs
        INNER JOIN sys.database_files AS df
            ON fs.file_id = df.file_id
        ORDER BY fs.io_stall DESC
        """
        rows = await self.executor.fetch_all(database_name, query)

        warnings: list[dict[str, Any]] = []
        for row in rows:
            avg_read = row.get("avg_read_latency_ms", 0) or 0
            avg_write = row.get("avg_write_latency_ms", 0) or 0
            if avg_read > 20:
                warnings.append(
                    {
                        "type": "high_read_latency",
                        "file": row.get("file_name"),
                        "latency_ms": avg_read,
                        "message": f"Average read latency {avg_read}ms exceeds 20ms threshold",
                    }
                )
            if avg_write > 20:
                warnings.append(
                    {
                        "type": "high_write_latency",
                        "file": row.get("file_name"),
                        "latency_ms": avg_write,
                        "message": f"Average write latency {avg_write}ms exceeds 20ms threshold",
                    }
                )

        return {
            "database_name": database_name,
            "files": rows,
            "warnings": warnings,
        }

    async def get_resource_limits(
        self,
        database_name: str,
    ) -> dict[str, Any]:
        """Azure resource governance limits and current service objective."""
        # Column availability in sys.dm_user_db_resource_governance varies
        # significantly by service tier (GeneralPurpose vs BusinessCritical
        # vs Hyperscale) and across Azure SQL versions.  SELECT * returns
        # whatever the current tier exposes, and we expose it as-is.
        governance_query = """
        SELECT TOP 1 *
        FROM sys.dm_user_db_resource_governance
        """
        slo_query = """
        SELECT
            edition,
            service_objective,
            elastic_pool_name
        FROM sys.database_service_objectives
        WHERE database_id = DB_ID()
        """

        governance_rows = await self.executor.fetch_all(database_name, governance_query)
        try:
            slo_rows = await self.executor.fetch_all(database_name, slo_query)
        except Exception:
            slo_rows = []

        governance = governance_rows[0] if governance_rows else {}
        slo = slo_rows[0] if slo_rows else {}

        # Convert log rate to MB/s for readability when the column is present
        # (column name varies by tier).
        log_rate_bytes = governance.get(
            "primary_max_log_rate_per_db_in_bytes_per_second"
        ) or governance.get("max_log_rate") or 0
        if log_rate_bytes:
            governance["max_log_rate_mb_per_sec"] = round(
                log_rate_bytes / (1024 * 1024), 2
            )

        return {
            "database_name": database_name,
            "service_objective": slo,
            "governance_limits": governance,
        }

    async def get_resource_stats_history(
        self,
        database_name: str,
        window_minutes: int = 60,
    ) -> dict[str, Any]:
        """Resource utilization history from sys.dm_db_resource_stats (15-sec granularity)."""
        query = """
        SELECT
            end_time,
            CAST(avg_cpu_percent AS DECIMAL(5, 2)) AS avg_cpu_percent,
            CAST(avg_data_io_percent AS DECIMAL(5, 2)) AS avg_data_io_percent,
            CAST(avg_log_write_percent AS DECIMAL(5, 2)) AS avg_log_write_percent,
            CAST(avg_memory_usage_percent AS DECIMAL(5, 2)) AS avg_memory_usage_percent,
            CAST(max_worker_percent AS DECIMAL(5, 2)) AS max_worker_percent,
            CAST(max_session_percent AS DECIMAL(5, 2)) AS max_session_percent,
            CAST(avg_instance_cpu_percent AS DECIMAL(5, 2)) AS avg_instance_cpu_percent
        FROM sys.dm_db_resource_stats
        WHERE end_time >= DATEADD(MINUTE, -{window_minutes}, GETUTCDATE())
        ORDER BY end_time DESC
        """.format(window_minutes=int(window_minutes))

        rows = await self.executor.fetch_all(database_name, query)

        # Compute summary stats
        summary: dict[str, Any] = {}
        if rows:
            for metric in (
                "avg_cpu_percent",
                "avg_data_io_percent",
                "avg_log_write_percent",
                "avg_memory_usage_percent",
            ):
                values = [float(r.get(metric, 0) or 0) for r in rows]
                above_80 = sum(1 for v in values if v > 80)
                summary[metric] = {
                    "avg": round(sum(values) / len(values), 2),
                    "max": round(max(values), 2),
                    "min": round(min(values), 2),
                    "samples_above_80_pct": above_80,
                    "total_samples": len(values),
                }

        # Generate warnings for sustained pressure
        warnings: list[dict[str, Any]] = []
        for metric_name, info in summary.items():
            if info.get("samples_above_80_pct", 0) > len(rows) * 0.3:
                friendly = metric_name.replace("avg_", "").replace("_", " ").title()
                warnings.append(
                    {
                        "type": f"sustained_{metric_name}",
                        "message": (
                            f"{friendly} was above 80% for "
                            f"{info['samples_above_80_pct']} of {info['total_samples']} samples "
                            f"(>{30}% of window)"
                        ),
                        "max": info["max"],
                        "avg": info["avg"],
                    }
                )

        return {
            "database_name": database_name,
            "window_minutes": window_minutes,
            "sample_count": len(rows),
            "summary": summary,
            "warnings": warnings,
            "history": rows,
        }
