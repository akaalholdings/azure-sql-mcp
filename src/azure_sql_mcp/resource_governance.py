from __future__ import annotations

from typing import Any

from .connection import AzureSqlExecutor
from .observability import sanitize_error_message
from .result_status import ResultStatus
from .result_status import status_payload


MAX_RESOURCE_HISTORY_MINUTES = 14 * 24 * 60
RECENT_RETENTION_MINUTES = 60

RECENT_RESOURCE_SQL = """
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
"""

LONG_TERM_RESOURCE_SQL = """
SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, end_time), 0) AS hour_start_utc,
    COUNT(*) AS samples,
    CAST(AVG(avg_cpu_percent) AS DECIMAL(5, 2)) AS avg_cpu_percent,
    CAST(MAX(avg_cpu_percent) AS DECIMAL(5, 2)) AS max_cpu_percent,
    CAST(AVG(avg_data_io_percent) AS DECIMAL(5, 2)) AS avg_data_io_percent,
    CAST(MAX(avg_data_io_percent) AS DECIMAL(5, 2)) AS max_data_io_percent,
    CAST(AVG(avg_log_write_percent) AS DECIMAL(5, 2)) AS avg_log_write_percent,
    CAST(MAX(avg_log_write_percent) AS DECIMAL(5, 2)) AS max_log_write_percent,
    CAST(MAX(max_worker_percent) AS DECIMAL(5, 2)) AS max_worker_percent,
    CAST(MAX(max_session_percent) AS DECIMAL(5, 2)) AS max_session_percent,
    MAX(storage_in_megabytes) AS storage_in_megabytes,
    MAX(sku) AS sku
FROM sys.resource_stats
WHERE database_name = ?
  AND end_time >= DATEADD(MINUTE, -?, GETUTCDATE())
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, end_time), 0)
ORDER BY hour_start_utc DESC
"""


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
        # In an elastic pool the view returns a row per pool database, so the
        # current database must be selected explicitly.
        governance_query = """
        SELECT TOP 1 *
        FROM sys.dm_user_db_resource_governance
        WHERE database_id = DB_ID()
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
        warnings: list[dict[str, Any]] = []
        try:
            slo_rows = await self.executor.fetch_all(database_name, slo_query)
        except Exception as exc:
            slo_rows = []
            warnings.append(
                {
                    "type": "service_objective_unavailable",
                    "message": "sys.database_service_objectives could not be read: "
                    + sanitize_error_message(str(exc)),
                }
            )
        if not governance_rows:
            warnings.append(
                {
                    "type": "governance_row_missing",
                    "message": "sys.dm_user_db_resource_governance returned no row for this database.",
                }
            )

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
            "warnings": warnings,
        }

    async def get_resource_stats_history(
        self,
        database_name: str,
        window_minutes: int = 60,
        *,
        source: str = "auto",
        master_available: bool = False,
    ) -> dict[str, Any]:
        """Resource utilization history against the database's governance limits.

        ``recent`` reads sys.dm_db_resource_stats (15-second samples, about one
        hour retained). ``long_term`` reads sys.resource_stats in master (5-minute
        samples, about 14 days retained) and returns hourly average and maximum
        buckets so long windows stay compact. ``auto`` picks recent for windows of
        60 minutes or less and long_term beyond that.
        """
        window_minutes = max(1, min(int(window_minutes), MAX_RESOURCE_HISTORY_MINUTES))
        if source not in {"auto", "recent", "long_term"}:
            raise ValueError("source must be auto, recent, or long_term")
        resolved = source
        if source == "auto":
            resolved = "recent" if window_minutes <= RECENT_RETENTION_MINUTES else "long_term"
        status: dict[str, Any] = {}
        if resolved == "long_term" and not master_available:
            status = status_payload(
                ResultStatus.PRECONDITION,
                f"A {window_minutes}-minute window needs sys.resource_stats, which is read "
                "from master. Returned the most recent hour from sys.dm_db_resource_stats "
                "instead.",
                remediation=(
                    "Add master to AZURE_SQL_ALLOWED_DATABASES; the login needs access to master."
                ),
            )
            resolved = "recent"
        if resolved == "long_term":
            rows = await self.executor.fetch_all(
                "master", LONG_TERM_RESOURCE_SQL, params=[database_name, window_minutes]
            )
            granularity = "hourly_buckets_from_5_minute_samples"
        else:
            query = RECENT_RESOURCE_SQL.format(window_minutes=window_minutes)
            rows = await self.executor.fetch_all(database_name, query)
            granularity = "15_second_samples"

        # Compute summary stats
        summary: dict[str, Any] = {}
        if rows:
            for metric in (
                "avg_cpu_percent",
                "avg_data_io_percent",
                "avg_log_write_percent",
                "avg_memory_usage_percent",
                "max_worker_percent",
            ):
                if not any(metric in r for r in rows):
                    continue
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
            "source": resolved,
            "granularity": granularity,
            "sample_count": len(rows),
            "summary": summary,
            "warnings": warnings,
            "history": rows,
            **status,
        }
