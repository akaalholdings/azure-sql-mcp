from __future__ import annotations

import math

import pytest

from azure_sql_mcp.index_optimizer import (
    IndexCandidate,
    IndexOptimizer,
    OptimizationResult,
    ScoredCandidate,
    _RawCandidate,
    _is_prefix,
    _merge_unique,
    _to_float,
)

SAMPLE_SHOWPLAN = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence>
    <Batch>
      <Statements>
        <StmtSimple StatementText="SELECT * FROM dbo.Orders WHERE CustomerId = 1"
                    StatementType="SELECT"
                    StatementSubTreeCost="12.5"
                    StatementEstRows="100">
          <QueryPlan>
            <MissingIndexes>
              <MissingIndexGroup Impact="78.5">
                <MissingIndex Database="[testdb]" Schema="[dbo]" Table="[Orders]">
                  <ColumnGroup Usage="EQUALITY">
                    <Column Name="[CustomerId]" />
                  </ColumnGroup>
                  <ColumnGroup Usage="INCLUDE">
                    <Column Name="[Status]" />
                  </ColumnGroup>
                </MissingIndex>
              </MissingIndexGroup>
            </MissingIndexes>
          </QueryPlan>
        </StmtSimple>
      </Statements>
    </Batch>
  </BatchSequence>
</ShowPlanXML>
"""

SAMPLE_SHOWPLAN_TWO_MISSING = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence>
    <Batch>
      <Statements>
        <StmtSimple StatementText="SELECT * FROM dbo.Orders WHERE CustomerId = 1 AND CreatedAt > '2024-01-01'"
                    StatementType="SELECT"
                    StatementSubTreeCost="25.0"
                    StatementEstRows="500">
          <QueryPlan>
            <MissingIndexes>
              <MissingIndexGroup Impact="90.0">
                <MissingIndex Database="[testdb]" Schema="[dbo]" Table="[Orders]">
                  <ColumnGroup Usage="EQUALITY">
                    <Column Name="[CustomerId]" />
                  </ColumnGroup>
                  <ColumnGroup Usage="INEQUALITY">
                    <Column Name="[CreatedAt]" />
                  </ColumnGroup>
                  <ColumnGroup Usage="INCLUDE">
                    <Column Name="[Status]" />
                  </ColumnGroup>
                </MissingIndex>
              </MissingIndexGroup>
            </MissingIndexes>
          </QueryPlan>
        </StmtSimple>
      </Statements>
    </Batch>
  </BatchSequence>
</ShowPlanXML>
"""

EMPTY_SHOWPLAN = """\
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence>
    <Batch>
      <Statements>
        <StmtSimple StatementText="SELECT 1"
                    StatementType="SELECT"
                    StatementSubTreeCost="0.001"
                    StatementEstRows="1">
          <QueryPlan />
        </StmtSimple>
      </Statements>
    </Batch>
  </BatchSequence>
</ShowPlanXML>
"""


class FakeExecutor:
    """Mock executor that returns configurable results based on query content."""

    def __init__(
        self,
        workload_rows=None,
        dmv_rows=None,
        existing_index_rows=None,
        row_count=10000,
        column_widths=None,
        write_ratio=0.3,
        plan_xml=SAMPLE_SHOWPLAN,
    ):
        self.workload_rows = workload_rows or []
        self.dmv_rows = dmv_rows or []
        self.existing_index_rows = existing_index_rows or []
        self.row_count = row_count
        self.column_widths = column_widths or [
            {"column_name": "CustomerId", "max_length": 4},
            {"column_name": "CreatedAt", "max_length": 8},
            {"column_name": "Status", "max_length": 20},
        ]
        self.write_ratio_val = write_ratio
        self.plan_xml = plan_xml
        self.fetch_history: list = []
        self.batch_history: list = []
        self.session_history: list = []

    async def fetch_all(self, database_name, query, params=None):
        self.fetch_history.append((database_name, query, params))

        if "sys.query_store_query_text" in query:
            return self.workload_rows

        if "sys.dm_db_missing_index_details" in query:
            return self.dmv_rows

        if "FROM sys.indexes" in query:
            return self.existing_index_rows

        if "sys.dm_db_partition_stats" in query:
            return [{"row_count": self.row_count}]

        if "sys.columns" in query and "max_length" in query:
            return self.column_widths

        if "dm_db_index_usage_stats" in query:
            return [{"write_ratio": self.write_ratio_val}]

        return []

    async def execute_batches(self, database_name, query, params=None):
        self.batch_history.append((database_name, query, params))

        class Result:
            rows = [{"col1": self.plan_xml}]

        r = Result()
        r.rows = [{"col1": self.plan_xml}]
        return [r]

    async def execute_session(self, database_name, statements, max_rows=None):
        self.session_history.append((database_name, tuple(statements), max_rows))

        class Result:
            rows = [{"col1": self.plan_xml}]

        r = Result()
        r.rows = [{"col1": self.plan_xml}]
        return [[], [r], []]


# ---------------------------------------------------------------------------
# Module-level helper tests
# ---------------------------------------------------------------------------


class TestIsPrefix:
    def test_equal_tuples(self):
        assert _is_prefix(("A", "B"), ("A", "B")) is True

    def test_shorter_prefix(self):
        assert _is_prefix(("A",), ("A", "B", "C")) is True

    def test_empty_is_prefix(self):
        assert _is_prefix((), ("A", "B")) is True

    def test_not_a_prefix(self):
        assert _is_prefix(("A", "C"), ("A", "B", "C")) is False

    def test_longer_not_prefix(self):
        assert _is_prefix(("A", "B", "C"), ("A", "B")) is False


class TestMergeUnique:
    def test_merges_without_duplicates(self):
        assert _merge_unique(["A", "B"], ["B", "C"]) == ["A", "B", "C"]

    def test_empty_lists(self):
        assert _merge_unique([], []) == []

    def test_skips_empty_strings(self):
        assert _merge_unique(["A"], ["", "B"]) == ["A", "B"]


class TestToFloat:
    def test_none_returns_zero(self):
        assert _to_float(None) == 0.0

    def test_string_number(self):
        assert _to_float("3.14") == 3.14

    def test_invalid_returns_zero(self):
        assert _to_float("abc") == 0.0


# ---------------------------------------------------------------------------
# Candidate generation
# ---------------------------------------------------------------------------


class TestGenerateCandidates:
    def setup_method(self):
        from azure_sql_mcp.safe_sql import SafeSqlValidator

        self.optimizer = IndexOptimizer(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]

    def test_generate_from_plan_and_dmv(self):
        workload = [
            {
                "query_id": 1,
                "plan_id": 10,
                "sql": "SELECT * FROM dbo.Orders WHERE CustomerId = 1",
                "statement_subtree_cost": 12.5,
                "missing_indexes": [
                    {
                        "schema": "dbo",
                        "table": "Orders",
                        "equality_columns": ["CustomerId"],
                        "inequality_columns": [],
                        "include_columns": ["Status"],
                        "impact_pct": 78.5,
                    }
                ],
                "executions": 100.0,
                "total_cpu_us": 500000.0,
            },
        ]
        dmv_recs = [
            {
                "schema": "dbo",
                "table": "Orders",
                "equality_columns": ["CustomerId"],
                "inequality_columns": [],
                "include_columns": ["Status"],
                "user_seeks": 50,
                "avg_user_impact": 80.0,
                "avg_total_user_cost": 10.0,
                "estimated_improvement": 400.0,
            },
        ]

        candidates = self.optimizer._generate_candidates(workload, dmv_recs)
        assert len(candidates) == 1

        rc = candidates[0]
        assert rc.schema == "dbo"
        assert rc.table == "Orders"
        assert rc.key_columns == ["CustomerId"]
        assert rc.from_plan_xml is True
        assert rc.from_dmv is True
        assert rc.source == "merged"
        assert rc.confidence == "high"

    def test_dmv_only_candidate(self):
        dmv_recs = [
            {
                "schema": "dbo",
                "table": "Products",
                "equality_columns": ["CategoryId"],
                "inequality_columns": [],
                "include_columns": [],
                "user_seeks": 200,
                "avg_user_impact": 60.0,
                "avg_total_user_cost": 5.0,
                "estimated_improvement": 600.0,
            },
        ]
        candidates = self.optimizer._generate_candidates([], dmv_recs)
        assert len(candidates) == 1
        assert candidates[0].source == "dmv"
        assert candidates[0].confidence == "medium"

    def test_plan_only_candidate(self):
        workload = [
            {
                "query_id": 2,
                "statement_subtree_cost": 5.0,
                "missing_indexes": [
                    {
                        "schema": "dbo",
                        "table": "Items",
                        "equality_columns": ["ItemId"],
                        "inequality_columns": [],
                        "include_columns": [],
                        "impact_pct": 50.0,
                    }
                ],
                "executions": 10.0,
                "total_cpu_us": 1000.0,
            },
        ]
        candidates = self.optimizer._generate_candidates(workload, [])
        assert len(candidates) == 1
        assert candidates[0].source == "plan_xml"
        assert candidates[0].confidence == "medium"

    def test_empty_workload_and_dmv(self):
        candidates = self.optimizer._generate_candidates([], [])
        assert candidates == []


# ---------------------------------------------------------------------------
# Prefix subsumption
# ---------------------------------------------------------------------------


class TestPrefixSubsumption:
    def setup_method(self):
        from azure_sql_mcp.safe_sql import SafeSqlValidator

        self.optimizer = IndexOptimizer(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]

    def test_merges_overlapping(self):
        narrow = _RawCandidate(
            schema="dbo", table="Orders",
            key_columns=["A", "B"],
            include_columns=["X"],
            impact_pct=50.0, total_executions=10.0,
            from_plan_xml=True,
        )
        wide = _RawCandidate(
            schema="dbo", table="Orders",
            key_columns=["A", "B", "C"],
            include_columns=["Y"],
            impact_pct=70.0, total_executions=5.0,
            from_dmv=True,
        )
        result = self.optimizer._detect_prefix_subsumption([narrow, wide])
        assert len(result) == 1
        merged = result[0]
        assert merged.key_columns == ["A", "B", "C"]
        assert merged.impact_pct == 70.0
        assert merged.total_executions == 15.0
        assert "X" in merged.include_columns
        assert "Y" in merged.include_columns
        assert merged.from_plan_xml is True
        assert merged.from_dmv is True

    def test_keeps_non_overlapping(self):
        a = _RawCandidate(
            schema="dbo", table="Orders",
            key_columns=["A", "B"],
            include_columns=[],
            impact_pct=50.0,
        )
        b = _RawCandidate(
            schema="dbo", table="Orders",
            key_columns=["C", "D"],
            include_columns=[],
            impact_pct=60.0,
        )
        result = self.optimizer._detect_prefix_subsumption([a, b])
        assert len(result) == 2

    def test_different_tables_not_merged(self):
        a = _RawCandidate(
            schema="dbo", table="Orders",
            key_columns=["A"],
            include_columns=[],
            impact_pct=50.0,
        )
        b = _RawCandidate(
            schema="dbo", table="Products",
            key_columns=["A", "B"],
            include_columns=[],
            impact_pct=60.0,
        )
        result = self.optimizer._detect_prefix_subsumption([a, b])
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Existing index filtering
# ---------------------------------------------------------------------------


class TestFilterExistingIndexes:
    def setup_method(self):
        from azure_sql_mcp.safe_sql import SafeSqlValidator

        self.optimizer = IndexOptimizer(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]

    def test_exact_match_filtered(self):
        candidate = _RawCandidate(
            schema="dbo", table="Orders",
            key_columns=["CustomerId", "CreatedAt"],
            include_columns=[],
        )
        existing = {("dbo", "Orders", ("CustomerId", "CreatedAt"))}
        result = self.optimizer._filter_existing_indexes([candidate], existing)
        assert result == []

    def test_prefix_of_existing_filtered(self):
        candidate = _RawCandidate(
            schema="dbo", table="Orders",
            key_columns=["CustomerId"],
            include_columns=[],
        )
        existing = {("dbo", "Orders", ("CustomerId", "CreatedAt", "Status"))}
        result = self.optimizer._filter_existing_indexes([candidate], existing)
        assert result == []

    def test_non_matching_kept(self):
        candidate = _RawCandidate(
            schema="dbo", table="Orders",
            key_columns=["ProductId"],
            include_columns=[],
        )
        existing = {("dbo", "Orders", ("CustomerId",))}
        result = self.optimizer._filter_existing_indexes([candidate], existing)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Size estimation
# ---------------------------------------------------------------------------


class TestSizeEstimation:
    @pytest.mark.asyncio
    async def test_size_formula(self):
        from azure_sql_mcp.safe_sql import SafeSqlValidator

        executor = FakeExecutor(row_count=100000, column_widths=[
            {"column_name": "CustomerId", "max_length": 4},
            {"column_name": "CreatedAt", "max_length": 8},
        ])
        optimizer = IndexOptimizer(executor=executor, validator=SafeSqlValidator())

        size_mb = await optimizer._estimate_index_size(
            "testdb", "dbo", "Orders", ["CustomerId", "CreatedAt"],
        )

        # key_width = 4 + 8 + 9 (overhead) = 21
        # rows_per_page = 8096 // (21 + 2) = 352
        # leaf_pages = ceil(100000 / 352) = 285
        # total = 285 * 8192 * 1.1 / (1024*1024) ≈ 2.45 MB
        assert 2.0 < size_mb < 3.0

    @pytest.mark.asyncio
    async def test_zero_rows_returns_zero(self):
        from azure_sql_mcp.safe_sql import SafeSqlValidator

        executor = FakeExecutor(row_count=0)
        optimizer = IndexOptimizer(executor=executor, validator=SafeSqlValidator())
        size_mb = await optimizer._estimate_index_size(
            "testdb", "dbo", "Orders", ["CustomerId"],
        )
        assert size_mb == 0.0


# ---------------------------------------------------------------------------
# Pareto scoring
# ---------------------------------------------------------------------------


class TestParetoScoring:
    def setup_method(self):
        from azure_sql_mcp.safe_sql import SafeSqlValidator

        self.optimizer = IndexOptimizer(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]

    def test_higher_benefit_scores_higher(self):
        high = _RawCandidate(
            schema="dbo", table="Orders",
            key_columns=["A"],
            include_columns=[],
            impact_pct=90.0,
            statement_subtree_cost=10.0,
            total_executions=100.0,
            from_plan_xml=True,
        )
        high.estimated_size_mb = 5.0  # type: ignore[attr-defined]
        high.write_ratio = 0.3  # type: ignore[attr-defined]

        low = _RawCandidate(
            schema="dbo", table="Orders",
            key_columns=["B"],
            include_columns=[],
            impact_pct=10.0,
            statement_subtree_cost=1.0,
            total_executions=5.0,
            from_dmv=True,
        )
        low.estimated_size_mb = 5.0  # type: ignore[attr-defined]
        low.write_ratio = 0.3  # type: ignore[attr-defined]

        scored = self.optimizer._score_candidates([high, low], alpha=1.5, beta=0.5)
        assert scored[0].candidate.key_columns == ("A",)
        assert scored[0].score > scored[1].score

    def test_larger_size_penalized(self):
        small = _RawCandidate(
            schema="dbo", table="T",
            key_columns=["A"],
            include_columns=[],
            impact_pct=50.0,
            statement_subtree_cost=5.0,
            total_executions=50.0,
            from_plan_xml=True,
        )
        small.estimated_size_mb = 1.0  # type: ignore[attr-defined]
        small.write_ratio = 0.3  # type: ignore[attr-defined]

        large = _RawCandidate(
            schema="dbo", table="T",
            key_columns=["B"],
            include_columns=[],
            impact_pct=50.0,
            statement_subtree_cost=5.0,
            total_executions=50.0,
            from_plan_xml=True,
        )
        large.estimated_size_mb = 500.0  # type: ignore[attr-defined]
        large.write_ratio = 0.3  # type: ignore[attr-defined]

        scored = self.optimizer._score_candidates([small, large], alpha=1.5, beta=0.5)
        assert scored[0].candidate.key_columns == ("A",)

    def test_high_write_ratio_penalized(self):
        read_heavy = _RawCandidate(
            schema="dbo", table="T",
            key_columns=["A"],
            include_columns=[],
            impact_pct=50.0,
            statement_subtree_cost=5.0,
            total_executions=50.0,
            from_plan_xml=True,
        )
        read_heavy.estimated_size_mb = 5.0  # type: ignore[attr-defined]
        read_heavy.write_ratio = 0.1  # type: ignore[attr-defined]

        write_heavy = _RawCandidate(
            schema="dbo", table="T",
            key_columns=["B"],
            include_columns=[],
            impact_pct=50.0,
            statement_subtree_cost=5.0,
            total_executions=50.0,
            from_plan_xml=True,
        )
        write_heavy.estimated_size_mb = 5.0  # type: ignore[attr-defined]
        write_heavy.write_ratio = 0.95  # type: ignore[attr-defined]

        scored = self.optimizer._score_candidates(
            [read_heavy, write_heavy], alpha=1.5, beta=0.5,
        )
        assert scored[0].candidate.key_columns == ("A",)

    def test_include_columns_cleaned(self):
        rc = _RawCandidate(
            schema="dbo", table="T",
            key_columns=["A"],
            include_columns=["A", "B"],  # A is a key col — should be stripped
            impact_pct=50.0,
            statement_subtree_cost=1.0,
            total_executions=1.0,
            from_plan_xml=True,
        )
        rc.estimated_size_mb = 1.0  # type: ignore[attr-defined]
        rc.write_ratio = 0.3  # type: ignore[attr-defined]

        scored = self.optimizer._score_candidates([rc], alpha=1.5, beta=0.5)
        assert scored[0].candidate.include_columns == ("B",)


# ---------------------------------------------------------------------------
# Greedy selection with budget
# ---------------------------------------------------------------------------


class TestGreedySelect:
    def setup_method(self):
        from azure_sql_mcp.safe_sql import SafeSqlValidator

        self.optimizer = IndexOptimizer(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]

    def _make_scored(self, key, size_mb, impact_pct, score, schema="dbo", table="T"):
        return ScoredCandidate(
            candidate=IndexCandidate(
                schema=schema, table=table,
                key_columns=tuple(key),
                include_columns=(),
            ),
            estimated_size_mb=size_mb,
            read_benefit=score,
            write_ratio=0.3,
            impact_pct=impact_pct,
            affected_query_count=1,
            affected_query_ids=[],
            source="plan_xml",
            confidence="medium",
            score=score,
            create_index_sql=f"CREATE INDEX ... ({', '.join(key)})",
        )

    def test_respects_budget(self):
        a = self._make_scored(["A"], size_mb=100, impact_pct=80.0, score=10.0)
        b = self._make_scored(["B"], size_mb=100, impact_pct=70.0, score=9.0)
        c = self._make_scored(["C"], size_mb=100, impact_pct=60.0, score=8.0)
        # Budget only fits 2
        selected = self.optimizer._greedy_select([a, b, c], budget_mb=200.0, min_improvement_pct=5.0)
        assert len(selected) == 2
        keys = {s.candidate.key_columns for s in selected}
        assert ("A",) in keys
        assert ("B",) in keys

    def test_unlimited_budget(self):
        a = self._make_scored(["A"], size_mb=100, impact_pct=80.0, score=10.0)
        b = self._make_scored(["B"], size_mb=100, impact_pct=70.0, score=9.0)
        selected = self.optimizer._greedy_select([a, b], budget_mb=None, min_improvement_pct=5.0)
        assert len(selected) == 2

    def test_stops_below_min_improvement(self):
        good = self._make_scored(["A"], size_mb=10, impact_pct=50.0, score=10.0)
        bad = self._make_scored(["B"], size_mb=10, impact_pct=2.0, score=1.0)
        selected = self.optimizer._greedy_select([good, bad], budget_mb=None, min_improvement_pct=5.0)
        assert len(selected) == 1
        assert selected[0].candidate.key_columns == ("A",)

    def test_merges_overlapping_with_selected(self):
        narrow = self._make_scored(["A", "B"], size_mb=50, impact_pct=80.0, score=10.0)
        wide = self._make_scored(["A", "B", "C"], size_mb=80, impact_pct=70.0, score=9.0)
        # narrow selected first, then wide should replace it (wider covers narrow)
        selected = self.optimizer._greedy_select([narrow, wide], budget_mb=None, min_improvement_pct=5.0)
        assert len(selected) == 1
        assert selected[0].candidate.key_columns == ("A", "B", "C")

    def test_skips_already_covered_by_wider(self):
        wide = self._make_scored(["A", "B", "C"], size_mb=80, impact_pct=90.0, score=12.0)
        narrow = self._make_scored(["A", "B"], size_mb=50, impact_pct=70.0, score=9.0)
        selected = self.optimizer._greedy_select([wide, narrow], budget_mb=None, min_improvement_pct=5.0)
        assert len(selected) == 1
        assert selected[0].candidate.key_columns == ("A", "B", "C")


# ---------------------------------------------------------------------------
# Plan XML helpers
# ---------------------------------------------------------------------------


class TestPlanXmlHelpers:
    def setup_method(self):
        from azure_sql_mcp.safe_sql import SafeSqlValidator

        self.optimizer = IndexOptimizer(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]

    def test_extract_statement_cost(self):
        cost = self.optimizer._extract_statement_cost(SAMPLE_SHOWPLAN)
        assert cost == 12.5

    def test_extract_statement_cost_empty_plan(self):
        cost = self.optimizer._extract_statement_cost(EMPTY_SHOWPLAN)
        assert cost == 0.001

    def test_extract_missing_indexes(self):
        indexes = self.optimizer._extract_missing_indexes(SAMPLE_SHOWPLAN)
        assert len(indexes) == 1
        assert indexes[0]["schema"] == "dbo"
        assert indexes[0]["table"] == "Orders"
        assert indexes[0]["equality_columns"] == ["CustomerId"]
        assert indexes[0]["include_columns"] == ["Status"]
        assert indexes[0]["impact_pct"] == 78.5

    def test_extract_missing_indexes_empty_plan(self):
        indexes = self.optimizer._extract_missing_indexes(EMPTY_SHOWPLAN)
        assert indexes == []

    def test_extract_missing_indexes_two(self):
        indexes = self.optimizer._extract_missing_indexes(SAMPLE_SHOWPLAN_TWO_MISSING)
        assert len(indexes) == 1
        idx = indexes[0]
        assert idx["equality_columns"] == ["CustomerId"]
        assert idx["inequality_columns"] == ["CreatedAt"]
        assert idx["include_columns"] == ["Status"]
        assert idx["impact_pct"] == 90.0


# ---------------------------------------------------------------------------
# Full pipeline (end-to-end with mocked executor)
# ---------------------------------------------------------------------------


class TestFullOptimizePipeline:
    @pytest.mark.asyncio
    async def test_full_pipeline(self):
        from azure_sql_mcp.safe_sql import SafeSqlValidator

        executor = FakeExecutor(
            workload_rows=[
                {
                    "query_id": 1,
                    "plan_id": 10,
                    "query_sql_text": (
                        "SELECT CustomerId FROM dbo.Orders "
                        "WHERE CustomerId = 1"
                    ),
                    "executions": 100,
                    "total_cpu_us": 500000,
                    "total_duration_us": 800000,
                    "total_logical_io_reads": 9000,
                },
            ],
            dmv_rows=[
                {
                    "schema_name": "dbo",
                    "table_name": "Orders",
                    "equality_columns": "[CustomerId]",
                    "inequality_columns": None,
                    "included_columns": "[Status]",
                    "user_seeks": 50,
                    "user_scans": 0,
                    "avg_total_user_cost": 10.0,
                    "avg_user_impact": 80.0,
                    "estimated_improvement": 400.0,
                },
            ],
            row_count=100000,
            write_ratio=0.25,
        )
        optimizer = IndexOptimizer(executor=executor, validator=SafeSqlValidator())

        result = await optimizer.optimize(
            "testdb",
            window_minutes=60,
            top_n=10,
            budget_mb=500.0,
            alpha=1.5,
            beta=0.5,
            min_improvement_pct=5.0,
        )

        assert isinstance(result, OptimizationResult)
        assert result.database_name == "testdb"
        assert result.queries_analyzed >= 0
        assert result.candidates_generated >= 0
        assert result.existing_indexes_count >= 0
        assert result.budget_mb == 500.0

        # Should produce at least one recommendation
        if result.selected_indexes:
            idx = result.selected_indexes[0]
            assert idx.candidate.schema == "dbo"
            assert idx.candidate.table == "Orders"
            assert "CustomerId" in idx.candidate.key_columns
            assert idx.estimated_size_mb > 0
            assert idx.score != 0.0
            assert "CREATE INDEX" in idx.create_index_sql

        # as_dict should serialize
        d = result.as_dict()
        assert d["database_name"] == "testdb"
        assert d["budget_mb"] == 500.0

    @pytest.mark.asyncio
    async def test_empty_workload(self):
        from azure_sql_mcp.safe_sql import SafeSqlValidator

        executor = FakeExecutor(workload_rows=[], dmv_rows=[])
        optimizer = IndexOptimizer(executor=executor, validator=SafeSqlValidator())

        result = await optimizer.optimize("testdb")
        assert isinstance(result, OptimizationResult)
        assert result.queries_analyzed == 0
        assert result.candidates_generated == 0
        assert result.selected_indexes == []
