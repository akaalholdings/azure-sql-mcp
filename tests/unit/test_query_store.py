from __future__ import annotations

import pytest

from azure_sql_mcp.index_optimizer import score_index_candidate
from azure_sql_mcp.query_store import INDEX_EVIDENCE_QUERY
from azure_sql_mcp.query_store import QUERY_STORE_TEXT_HINTS_SQL
from azure_sql_mcp.query_store import QueryStoreService
from azure_sql_mcp.query_store import _resolve_index_hints


INDEX_EVIDENCE_XML = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence><Batch><Statements>
    <StmtSimple QueryPlanHash="0x0102030405060708" StatementText="SELECT secret">
      <QueryPlan><RelOp PhysicalOp="Index Seek" LogicalOp="Index Seek">
        <IndexScan><Object Database="[appdb]" Schema="[dbo]" Table="[Orders]" Index="[IX Orders]" /></IndexScan>
        <MissingIndexes><MissingIndexGroup Impact="90"><MissingIndex Database="[appdb]" Schema="[dbo]" Table="[Orders]">
          <ColumnGroup Usage="EQUALITY"><Column Name="[CustomerId]" /></ColumnGroup>
          <ColumnGroup Usage="INCLUDE"><Column Name="[Status]" /></ColumnGroup>
        </MissingIndex></MissingIndexGroup></MissingIndexes>
      </RelOp></QueryPlan>
    </StmtSimple>
  </Statements></Batch></BatchSequence>
</ShowPlanXML>
"""


def test_index_evidence_query_retains_stored_plans_without_window_runtime() -> None:
    normalized = " ".join(INDEX_EVIDENCE_QUERY.split()).casefold()

    assert "left join ( select runtime.plan_id" in normalized
    assert "where intervals.end_time >= dateadd" in normalized
    assert "where rsi.runtime_stats_interval_id is not null" not in normalized
    assert "or (p.is_forced_plan = 1" not in normalized


def test_query_store_text_hints_scan_all_retained_text() -> None:
    normalized = " ".join(QUERY_STORE_TEXT_HINTS_SQL.split()).casefold()

    assert "dateadd" not in normalized
    assert "where q.last_execution_time" not in normalized


def test_index_hint_parser_resolves_multi_index_and_forceseek_targets() -> None:
    identities = [
        {
            "object_id": 101,
            "index_id": 2,
            "schema": "dbo",
            "table": "Orders",
            "index_name": "IX_A",
        },
        {
            "object_id": 101,
            "index_id": 3,
            "schema": "dbo",
            "table": "Orders",
            "index_name": "IX_B",
        },
    ]

    matches, blockers = _resolve_index_hints(
        "SELECT * FROM dbo.Orders WITH (INDEX(IX_A, [IX_B]), "
        "FORCESEEK([IX_B]([CustomerId])))",
        identities,
    )

    assert blockers == []
    assert {item["index_name"] for item in matches} == {"IX_A", "IX_B"}


def test_index_hint_parser_blocks_unresolved_or_unscoped_hints() -> None:
    identities = [
        {
            "object_id": 101,
            "index_id": 2,
            "schema": "dbo",
            "table": "Orders",
            "index_name": "IX_A",
        }
    ]

    matches, blockers = _resolve_index_hints(
        "SELECT * FROM dbo.Orders WITH (INDEX(IX_A, IX_Missing), FORCESEEK)",
        identities,
    )

    assert {item["index_name"] for item in matches} == {"IX_A"}
    assert "unresolved_or_ambiguous_index_hint" in blockers
    assert "unresolved_forceseek_index_hint" in blockers


def _evidence_rows() -> list[dict[str, object]]:
    return [
        {
            "query_id": 42,
            "plan_id": 7,
            "query_plan_hash": "0x0102030405060708",
            "is_forced_plan": True,
            "query_plan_xml": INDEX_EVIDENCE_XML,
            "runtime_stats_interval_id": 10,
            "execution_count": 2,
            "last_seen_utc": "2026-08-27T10:00:00Z",
        },
        {
            "query_id": 42,
            "plan_id": 7,
            "query_plan_hash": "0x0102030405060708",
            "is_forced_plan": True,
            "query_plan_xml": INDEX_EVIDENCE_XML,
            "runtime_stats_interval_id": 11,
            "execution_count": 3,
            "last_seen_utc": "2026-08-28T10:00:00Z",
        },
        {
            "query_id": 99,
            "plan_id": 9,
            "query_plan_hash": "0x1111111111111111",
            "is_forced_plan": False,
            "query_plan_xml": INDEX_EVIDENCE_XML.replace(
                "IX Orders", "Other Index"
            ).replace("CustomerId", "UnrelatedColumn"),
            "runtime_stats_interval_id": 12,
            "execution_count": 1000,
            "last_seen_utc": "2026-08-28T11:00:00Z",
        },
    ]


class FakeExecutor:
    def __init__(self, rows=None):
        self.query = None
        self.params = None
        self.rows = rows or []

    async def fetch_all(self, database_name, query, params=None):
        self.query = query
        self.params = params
        return self.rows


@pytest.mark.asyncio
async def test_top_queries_uses_requested_sort_expression():
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_top_queries("appdb", "cpu", 30, 5)

    assert "ORDER BY SUM(rs.avg_cpu_time * rs.count_executions) DESC" in executor.query
    assert executor.params == [5, 30]


@pytest.mark.asyncio
async def test_top_queries_supports_extended_sort_metrics():
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_top_queries("appdb", "logical_io", 45, 10)

    assert "SUM(rs.avg_logical_io_reads * rs.count_executions)" in executor.query
    assert "avg_query_max_used_memory" in executor.query
    assert "CAST(MAX(rsi.end_time) AS datetime2(7)) AS last_seen_utc" in executor.query
    assert executor.params == [10, 45]


@pytest.mark.asyncio
async def test_top_queries_uses_resource_blend_query_shape():
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_top_queries("appdb", "resource_blend", 60, 7)

    assert "WITH QueryMetrics AS" in executor.query
    assert "resource_blend_score" in executor.query
    assert "MAX(total_logical_io_reads) AS max_logical_io_reads" in executor.query
    assert "CAST(MAX(rsi.end_time) AS datetime2(7)) AS last_seen_utc" in executor.query
    assert executor.params == [60, 7]


@pytest.mark.asyncio
async def test_top_queries_rejects_unknown_sort():
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    with pytest.raises(ValueError, match="resource_blend"):
        await service.get_top_queries("appdb", "bad-sort", 30, 5)


@pytest.mark.asyncio
async def test_exact_query_identity_uses_binary_length_aware_comparison() -> None:
    executor = FakeExecutor()
    service = QueryStoreService(executor)
    sql = "SELECT N'MiXeD  value' "

    await service.resolve_query_identity("appdb", sql)

    assert "DATALENGTH(qt.query_sql_text)" in executor.query
    assert "Latin1_General_100_BIN2" in executor.query
    assert executor.params == [sql, sql]


@pytest.mark.asyncio
async def test_query_history_escapes_like_wildcards():
    """Query text routinely contains % and _; the fingerprint must match as a
    literal substring, not as a wildcard pattern."""
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_query_history_by_text(
        "appdb",
        "SELECT * FROM t WHERE name LIKE '%foo_bar%'",
    )

    escaped_fingerprint = executor.params[2]
    assert "[%]" in escaped_fingerprint
    assert "[_]" in escaped_fingerprint
    # The reversed containment clause still receives the raw fingerprint value.
    assert executor.params[3] == "SELECT * FROM t WHERE name LIKE '%foo_bar%'"


@pytest.mark.asyncio
async def test_query_history_by_hash_converts_hex_string():
    """Hash matching survives parameter renaming (@CustomerId vs @P1) that
    defeats text matching; the hex string must be converted to BINARY(8)."""
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    result = await service.get_query_history_by_hash(
        "appdb", "0x90FC7E5399EA52A5", window_minutes=60, limit=5,
    )

    assert "CONVERT(BINARY(8), ?, 1)" in executor.query
    assert executor.params == [5, 60, "0x90FC7E5399EA52A5"]
    assert result["matches"] == []
    assert result["query_hash"] == "0x90FC7E5399EA52A5"


@pytest.mark.asyncio
async def test_query_history_by_hash_rejects_non_hex():
    service = QueryStoreService(FakeExecutor())
    with pytest.raises(ValueError, match="0x-prefixed"):
        await service.get_query_history_by_hash("appdb", "DROP TABLE x")


@pytest.mark.asyncio
async def test_query_history_prefers_query_id_over_hash_and_fuzzy_text() -> None:
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    result = await service.get_query_history(
        "appdb",
        query_id=42,
        query_hash="0x1111111111111111",
        sql="SELECT * FROM Orders",
        window_minutes=60,
        limit=5,
    )

    assert result["identity_kind"] == "query_id"
    assert "q.query_id = ?" in executor.query
    assert "LIKE '%'" not in executor.query
    assert executor.params == [5, 60, 42]


@pytest.mark.asyncio
async def test_query_history_by_id_omits_variant_column_from_plan_catalog() -> None:
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_query_history_by_id("appdb", 42, window_minutes=60, limit=5)

    query = executor.query.lower()
    assert "p.query_variant_query_id" not in query
    assert "query_variant_query_id" not in query
    assert query.count("q.query_parameterization_type_desc") == 2
    assert "p.query_parameterization_type_desc" not in query
    assert "and q.query_id = ?" in query
    assert executor.params == [5, 60, 42]


@pytest.mark.asyncio
async def test_query_history_averages_are_execution_weighted_and_zero_safe() -> None:
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_query_history_by_id("appdb", 42, window_minutes=60, limit=5)

    assert (
        "SUM(rs.avg_duration * rs.count_executions) "
        "/ NULLIF(SUM(rs.count_executions), 0)"
    ) in executor.query
    assert (
        "SUM(rs.avg_cpu_time * rs.count_executions) "
        "/ NULLIF(SUM(rs.count_executions), 0)"
    ) in executor.query
    assert "SUM(rs.avg_logical_io_reads * rs.count_executions)" in executor.query
    assert "SUM(rs.avg_rowcount * rs.count_executions)" in executor.query
    assert "AVG(rs.avg_duration)" not in executor.query
    assert "AVG(rs.avg_cpu_time)" not in executor.query


@pytest.mark.asyncio
async def test_parameter_runtime_buckets_preserve_compiled_values_and_provenance() -> None:
    plan_xml = """\
    <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
      <BatchSequence><Batch><Statements><StmtSimple><QueryPlan><ParameterList>
        <ColumnReference Column="@CustomerId" ParameterDataType="int"
                         ParameterCompiledValue="(1)" ParameterRuntimeValue="(42)" />
      </ParameterList></QueryPlan></StmtSimple></Statements></Batch></BatchSequence>
    </ShowPlanXML>
    """
    executor = FakeExecutor(
        rows=[
            {
                "query_id": 42,
                "plan_id": 7,
                "query_plan_xml": plan_xml,
                "executions": 10,
                "avg_duration_ms": 12.5,
            }
        ]
    )
    service = QueryStoreService(executor)

    result = await service.get_parameter_runtime_buckets(
        "appdb",
        query_id=42,
        window_minutes=60,
        limit=5,
    )

    bucket = result["buckets"][0]
    assert bucket["compiled_parameters"][0]["compiled_value"] == "(1)"
    assert bucket["compiled_parameters"][0]["runtime_value"] == "(42)"
    assert bucket["runtime_parameter_values_observed"] is True
    assert bucket["runtime_bucket_source"] == (
        "query_store_runtime_stats_by_plan_and_interval"
    )
    assert result["distinct_compiled_parameter_sets"] == [
        [{"name": "@CustomerId", "compiled_value": "(1)"}]
    ]
    assert result["distinct_compiled_parameter_set_count"] == 1


@pytest.mark.asyncio
async def test_parameter_runtime_buckets_omits_variant_column_from_plan_catalog() -> None:
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    result = await service.get_parameter_runtime_buckets(
        "appdb",
        query_id=42,
        window_minutes=60,
        limit=5,
    )

    query = executor.query.lower()
    assert "p.query_variant_query_id" not in query
    assert "query_variant_query_id" not in query
    assert query.count("q.query_parameterization_type_desc") == 2
    assert "p.query_parameterization_type_desc" not in query
    assert "and q.query_id = ?" in query
    assert result["query_id"] == 42
    assert executor.params == [5, 60, 42]


@pytest.mark.asyncio
async def test_parameter_buckets_weight_runtime_averages_by_execution_count() -> None:
    executor = FakeExecutor()
    service = QueryStoreService(executor)

    await service.get_parameter_runtime_buckets(
        "appdb", query_id=42, window_minutes=60, limit=5
    )

    assert (
        "SUM(rs.avg_duration * rs.count_executions) "
        "/ NULLIF(SUM(rs.count_executions), 0)"
    ) in executor.query
    assert "SUM(rs.avg_logical_io_reads * rs.count_executions)" in executor.query
    assert "SUM(rs.avg_rowcount * rs.count_executions)" in executor.query
    assert "AVG(rs.avg_duration)" not in executor.query
    assert "AVG(rs.avg_logical_io_reads)" not in executor.query


class IndexEvidenceExecutor:
    def __init__(self, rows: list[dict[str, object]], *, deny_source: str | None = None):
        self.rows = rows
        self.deny_source = deny_source
        self.queries: list[str] = []

    async def fetch_all(self, database_name, query, params=None):
        self.queries.append(query)
        if self.deny_source and self.deny_source in query:
            raise PermissionError("permission denied")
        if "query_plan_xml" in query:
            return self.rows
        return []


class ScoringEvidenceExecutor(IndexEvidenceExecutor):
    async def fetch_all(self, database_name, query, params=None):
        if "sys.dm_db_partition_stats" in query:
            return [{"row_count": 100_000}]
        if "sys.columns" in query:
            return [
                {"column_name": "CustomerId", "max_length": 4},
                {"column_name": "Status", "max_length": 20},
            ]
        if "sys.dm_db_index_usage_stats" in query:
            return [{"write_ratio": 0.25}]
        return await super().fetch_all(database_name, query, params)


def _scoring_evidence_rows() -> list[dict[str, object]]:
    return [
        {
            **row,
            "query_plan_xml": row["query_plan_xml"].replace(
                '<StmtSimple QueryPlanHash=',
                '<StmtSimple StatementSubTreeCost="12.5" QueryPlanHash=',
            ),
        }
        for row in _evidence_rows()
        if row["query_id"] == 42
    ]


@pytest.mark.parametrize(
    "missing_input",
    ["statement_subtree_cost", "execution_count", "impact_pct", "estimated_size_mb", "write_ratio"],
)
def test_index_candidate_score_fails_closed_for_absent_required_input(
    missing_input: str,
) -> None:
    inputs: dict[str, float | None] = {
        "statement_subtree_cost": 12.5,
        "execution_count": 5.0,
        "impact_pct": 90.0,
        "estimated_size_mb": 3.0,
        "write_ratio": 0.25,
    }
    inputs[missing_input] = None

    assert score_index_candidate(**inputs) is None


@pytest.mark.asyncio
async def test_missing_candidate_score_reuses_index_optimizer_model() -> None:
    executor = ScoringEvidenceExecutor(_scoring_evidence_rows())
    service = QueryStoreService(executor)

    result = await service.get_missing_index_candidate_requests(
        "appdb", window_minutes=60, limit=10
    )

    [candidate] = result["missing_index_candidates"]
    assert candidate["statement_subtree_cost"] == 12.5
    assert candidate["execution_count"] == 5
    assert candidate["impact_pct"] == 90.0
    assert candidate["estimated_size_mb"] > 0
    assert candidate["write_ratio"] == 0.25
    expected = score_index_candidate(
        candidate["statement_subtree_cost"],
        candidate["execution_count"],
        candidate["impact_pct"],
        candidate["estimated_size_mb"],
        candidate["write_ratio"],
    )
    assert expected is not None
    assert candidate["current_score"] == pytest.approx(expected)


@pytest.mark.asyncio
async def test_missing_candidate_score_is_absent_when_required_evidence_is_missing() -> None:
    service = QueryStoreService(IndexEvidenceExecutor(_evidence_rows()))

    result = await service.get_missing_index_candidate_requests("appdb")

    assert result["coverage"]["status"] == "incomplete"
    for candidate in result["missing_index_candidates"]:
        assert candidate["current_score"] is None
        assert candidate["estimated_size_mb"] is None
        assert candidate["write_ratio"] is None
    assert "statement_subtree_cost_unavailable" in result["coverage"]["blockers"]


@pytest.mark.asyncio
async def test_missing_candidate_score_requires_existing_impact_floor() -> None:
    low_impact_rows = [
        {
            **row,
            "query_plan_xml": row["query_plan_xml"].replace(
                "Impact=\"90\"", "Impact=\"4\""
            ),
        }
        for row in _scoring_evidence_rows()
    ]
    service = QueryStoreService(ScoringEvidenceExecutor(low_impact_rows))

    result = await service.get_missing_index_candidate_requests("appdb")

    [candidate] = result["missing_index_candidates"]
    assert candidate["current_score"] is None
    assert candidate["scoring_blockers"] == ["impact_pct_below_existing_floor"]
    assert result["coverage"]["status"] == "complete"


@pytest.mark.asyncio
async def test_index_plan_references_keep_forced_and_interval_grain() -> None:
    executor = IndexEvidenceExecutor(_evidence_rows())
    service = QueryStoreService(executor)

    result = await service.get_index_executed_plan_references(
        "appdb", window_minutes=60, limit=10
    )

    assert result["coverage"]["status"] == "complete"
    assert result["coverage"]["eligible"] == 3
    assert result["executed_plan_references"][0]["is_forced_plan"] is True
    assert {
        interval_id
        for item in result["index_references"]
        for interval_id in item["runtime_interval_ids"]
    } == {10, 11, 12}
    assert "query_sql_text" not in repr(result)
    assert "SELECT secret" not in repr(result)
    assert "Parameter" not in repr(result)
    assert "ShowPlanXML" not in repr(result)


@pytest.mark.asyncio
async def test_stored_unforced_zero_execution_plan_is_complete_reference() -> None:
    stored_plan = {
        **_evidence_rows()[2],
        "runtime_stats_interval_id": None,
        "execution_count": 0,
        "last_seen_utc": None,
    }
    service = QueryStoreService(IndexEvidenceExecutor([stored_plan]))

    result = await service.get_index_executed_plan_references(
        "appdb", window_minutes=60, limit=10
    )

    assert result["coverage"]["status"] == "complete"
    [reference] = result["index_references"]
    assert reference["is_forced_plan"] is False
    assert reference["execution_count"] == 0
    assert reference["runtime_interval_ids"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("forced", [False, True])
async def test_positive_execution_plan_without_interval_is_incomplete(
    forced: bool,
) -> None:
    malformed_plan = {
        **_evidence_rows()[2],
        "runtime_stats_interval_id": None,
        "execution_count": 1,
        "is_forced_plan": forced,
    }
    service = QueryStoreService(IndexEvidenceExecutor([malformed_plan]))

    result = await service.get_index_executed_plan_references(
        "appdb", window_minutes=60, limit=10
    )

    assert result["index_references"] == []
    assert result["coverage"]["status"] == "incomplete"
    assert (
        "query_store_identity_or_interval_malformed"
        in result["coverage"]["blockers"]
    )


@pytest.mark.asyncio
async def test_missing_candidates_require_distinct_intervals_and_do_not_smear_totals() -> None:
    executor = IndexEvidenceExecutor(_evidence_rows())
    service = QueryStoreService(executor)

    result = await service.get_missing_index_candidate_requests(
        "appdb", window_minutes=60, limit=10
    )

    candidates = result["missing_index_candidates"]
    matching = [item for item in candidates if item["object_name"] == "Orders"]
    assert len(matching) == 2
    recurring = next(item for item in matching if item["include_signature"] == "Status")
    assert recurring["recurring"] is True
    assert recurring["runtime_interval_ids"] == [10, 11]
    assert recurring["execution_count"] == 5
    assert recurring["execution_count"] != 1005
    assert recurring["query_ids"] == [42]


@pytest.mark.asyncio
async def test_query_store_candidate_cap_is_incomplete() -> None:
    executor = IndexEvidenceExecutor(_evidence_rows())
    service = QueryStoreService(executor)

    result = await service.get_missing_index_candidate_requests("appdb", limit=2)

    assert result["coverage"]["capped"] is True
    assert result["coverage"]["status"] == "incomplete"
    assert result["coverage"]["scanned"] == 2


@pytest.mark.asyncio
async def test_query_store_plan_permission_gap_is_incomplete() -> None:
    executor = IndexEvidenceExecutor(
        _evidence_rows(), deny_source="sys.query_store_runtime_stats"
    )
    service = QueryStoreService(executor)

    result = await service.get_index_executed_plan_references("appdb")

    assert result["executed_plan_references"] == []
    assert result["coverage"]["status"] == "incomplete"
    assert result["coverage"]["blockers"]


class HintCoverageExecutor:
    def __init__(self, *, deny_source: str | None = None):
        self.deny_source = deny_source

    async def fetch_all(self, database_name, query, params=None):
        if self.deny_source and self.deny_source in query:
            raise PermissionError("permission denied")
        if "retained_query_text" in query:
            return [
                {
                    "query_id": 42,
                    "retained_query_text": (
                        "SELECT * FROM dbo.Orders WITH (INDEX = [IX Orders]) "
                        "OPTION (TABLE HINT([dbo].[Orders], INDEX(2)))"
                    ),
                }
            ]
        if "sys.query_store_query_hints" in query:
            return [{"query_id": 42, "query_hint_text": "OPTION (TABLE HINT([dbo].[Orders], INDEX(2)))"}]
        if "sys.plan_guides" in query:
            return [{"plan_guide_id": 8, "plan_guide_name": "guide", "plan_guide_hints": "INDEX([IX Orders])"}]
        return [
            {
                "object_id": 900,
                "module_definition": "SELECT * FROM dbo.Orders WITH (INDEX(999))",
            }
        ]


class RetainedQueryStoreHintExecutor:
    def __init__(self) -> None:
        self.query: str | None = None
        self.params: list[int] | None = None

    async def fetch_all(self, database_name, query, params=None):
        if "retained_query_text" in query:
            self.query = query
            self.params = params
            return [
                {
                    "query_id": 7,
                    "last_execution_time_utc": "2025-01-01T00:00:00Z",
                    "retained_query_text": (
                        "SELECT * FROM dbo.Orders WITH (INDEX([IX Orders]))"
                    ),
                }
            ]
        return []


def _hint_index() -> dict[str, object]:
    return {
        "object_id": 101,
        "index_id": 2,
        "schema": "dbo",
        "table": "Orders",
        "index_name": "IX Orders",
    }


@pytest.mark.asyncio
async def test_index_hint_coverage_resolves_names_and_numeric_table_hints_redacted() -> None:
    service = QueryStoreService(HintCoverageExecutor())

    result = await service.get_index_hint_coverage(
        "appdb", index_identities=[_hint_index()], limit=10
    )

    assert result["coverage"]["status"] == "incomplete"
    assert any(
        item["index"]["index_id"] == 2
        for item in result["evidence"]
        for item in item["resolved_indexes"]
    )
    serialized = repr(result)
    assert "SELECT * FROM dbo.Orders" not in serialized
    assert "INDEX(999)" not in serialized
    assert "CREATE * FROM" not in serialized
    assert "query_hint_text" not in serialized
    assert all("text_hash" in item for item in result["evidence"])


@pytest.mark.asyncio
async def test_index_hint_coverage_keeps_retained_hint_outside_runtime_window() -> None:
    executor = RetainedQueryStoreHintExecutor()
    service = QueryStoreService(executor)

    result = await service.get_index_hint_coverage(
        "appdb",
        index_identities=[_hint_index()],
        window_minutes=1,
        limit=10,
    )

    assert executor.query is not None
    assert "DATEADD" not in executor.query.upper()
    assert executor.params == [11]
    [evidence] = result["evidence"]
    assert evidence["source"] == "query_store_text"
    assert evidence["resolved_indexes"][0]["index"]["index_id"] == 2


@pytest.mark.asyncio
async def test_index_hint_permission_gap_is_incomplete() -> None:
    service = QueryStoreService(
        HintCoverageExecutor(deny_source="sys.plan_guides")
    )

    result = await service.get_index_hint_coverage(
        "appdb", index_identities=[_hint_index()]
    )

    assert result["coverage"]["status"] == "incomplete"
    assert result["coverage"]["sources"]["plan_guides"]["status"] == "incomplete"
