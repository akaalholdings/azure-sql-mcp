"""Query Store identity and top-query roll-up (review findings 52, 53, 93).

The service's own SQL runs on SQLite here, so a wrong match rule or a wrong
grouping grain shows as a wrong row, not as a changed string.
"""

from __future__ import annotations

import re
import pytest

from azure_sql_mcp.query_store import QueryStoreService
from tests.unit.test_query_regression import QueryStoreSqlite
from tests.unit.test_query_regression import _hourly_intervals


class IdentitySqlite(QueryStoreSqlite):
    """QueryStoreSqlite plus the T-SQL the identity and roll-up reads use."""

    def __init__(self) -> None:
        super().__init__()
        self.conn.execute("ALTER TABLE sys.query_store_query ADD COLUMN context_settings_id INT")
        self.conn.execute("ALTER TABLE sys.query_store_query ADD COLUMN last_execution_time TEXT")
        self.conn.create_function("RIGHT", 2, lambda text, n: text[len(text) - n :] if n else "")
        self.conn.create_function("DATALENGTH", 1, lambda text: 2 * len(text))

    def add_statement(
        self,
        query_id: int,
        text: str,
        query_hash: str,
        *,
        plan_id: int | None = None,
        parameterization: str = "None",
    ) -> None:
        self.insert("query_store_query_text", query_text_id=query_id, query_sql_text=text)
        self.insert(
            "query_store_query",
            query_id=query_id,
            query_text_id=query_id,
            query_hash=query_hash,
            context_settings_id=1,
            query_parameterization_type_desc=parameterization,
            last_execution_time=f"2026-11-01 00:00:{query_id % 60:02d}",
        )
        if plan_id is not None:
            self.insert(
                "query_store_plan",
                plan_id=plan_id,
                query_id=query_id,
                query_plan="",
                is_forced_plan=0,
                force_failure_count=0,
                query_plan_hash="0x00000000000000AA",
            )

    async def fetch_all(self, database_name: str, query: str, params: list | None = None) -> list[dict]:
        bound = list(params or [])
        query = query.replace(" COLLATE Latin1_General_100_BIN2", "")
        declared = re.match(r"\s*DECLARE @sql nvarchar\(max\) = \?;", query)
        if declared:
            # SQLite has no variables: bind the declared value at each use.
            sql_value, rest = bound[0], bound[1:]
            body = query[declared.end() :]
            bound = [
                sql_value if token == "@sql" else rest.pop(0)
                for token in re.findall(r"@sql|\?", body)
            ]
            query = body.replace("@sql", "?")
        return await super().fetch_all(database_name, query, bound)


APP_SQL = "SELECT o.Id FROM dbo.Orders AS o WHERE o.CustomerID = @c"
APP_HASH = "0x00000000000000C1"


def test_split_parameter_prefix_accepts_only_one_whole_declaration_block() -> None:
    from azure_sql_mcp.query_store import split_parameter_prefix

    assert split_parameter_prefix("(@c int)") == "(@c int)"
    assert split_parameter_prefix("(@p1 nvarchar(4000),@p2 decimal(10,2))") is not None
    # The block closes before the end: the stored text is another statement.
    assert split_parameter_prefix("(@c int)SELECT x FROM (") is None
    assert split_parameter_prefix("(SELECT 1)") is None
    assert split_parameter_prefix("") is None


@pytest.mark.asyncio
async def test_identity_matches_a_parameterized_statement_by_text() -> None:
    # The application sends the statement through sp_executesql, so Query Store
    # holds '(@c nvarchar(4000))' + the statement. Byte-exact equality never
    # matched it, and the history and parameter evidence were lost.
    store = IdentitySqlite()
    store.add_statement(42, "(@c nvarchar(4000))" + APP_SQL, APP_HASH)
    store.add_statement(43, "(@c int)SELECT o.Id FROM dbo.Other AS o WHERE o.CustomerID = @c", "0x0D")

    result = await QueryStoreService(store).resolve_query_identity("db", APP_SQL)

    assert result["status"] == "resolved"
    assert result["query_id"] == 42
    assert result["matched_by"] == "parameterized_text"
    # The application's declared type is the evidence for an implicit conversion.
    assert result["declared_parameters"] == "(@c nvarchar(4000))"


@pytest.mark.asyncio
async def test_identity_does_not_match_a_statement_that_only_ends_with_the_sql() -> None:
    store = IdentitySqlite()
    store.add_statement(50, "(@c int)SELECT 1 FROM (" + APP_SQL, "0x0E")

    result = await QueryStoreService(store).resolve_query_identity("db", APP_SQL)

    assert result["status"] == "not_found"


@pytest.mark.asyncio
async def test_identity_uses_the_plan_query_hash_before_text() -> None:
    # The agent reformatted the statement, so no text rule can match; the
    # QueryHash of its plan still names the Query Store query.
    store = IdentitySqlite()
    store.add_statement(42, "(@c int)" + APP_SQL, APP_HASH)
    reformatted = "select o.Id\nfrom dbo.Orders o\nwhere o.CustomerID = @c"

    service = QueryStoreService(store)
    by_text = await service.resolve_query_identity("db", reformatted)
    by_hash = await service.resolve_query_identity("db", reformatted, query_hash=APP_HASH)

    assert by_text["status"] == "not_found"
    assert by_hash["status"] == "resolved"
    assert by_hash["query_id"] == 42
    assert by_hash["matched_by"] == "query_hash"


@pytest.mark.asyncio
async def test_identity_reports_every_query_under_a_shared_hash() -> None:
    # Ad-hoc literal variants share one query_hash. No single query_id is the
    # statement, so none is picked silently.
    store = IdentitySqlite()
    for query_id in (61, 62, 63):
        store.add_statement(
            query_id, f"SELECT o.Id FROM dbo.Orders AS o WHERE o.CustomerID = {query_id}", APP_HASH
        )

    result = await QueryStoreService(store).resolve_query_identity(
        "db", "SELECT o.Id FROM dbo.Orders AS o WHERE o.CustomerID = 7", query_hash=APP_HASH
    )

    assert result["status"] == "ambiguous"
    assert result["matched_by"] == "query_hash"
    assert sorted(result["query_ids"]) == [61, 62, 63]
    assert result["query_count_under_hash"] == 3


@pytest.mark.asyncio
async def test_identity_text_narrows_a_shared_hash_to_one_query() -> None:
    store = IdentitySqlite()
    store.add_statement(61, "SELECT o.Id FROM dbo.Orders AS o WHERE o.CustomerID = 61", APP_HASH)
    store.add_statement(62, "SELECT o.Id FROM dbo.Orders AS o WHERE o.CustomerID = 62", APP_HASH)

    result = await QueryStoreService(store).resolve_query_identity(
        "db", "SELECT o.Id FROM dbo.Orders AS o WHERE o.CustomerID = 62", query_hash=APP_HASH
    )

    assert result["status"] == "resolved"
    assert result["query_id"] == 62
    assert result["matched_by"] == "query_hash_and_text"


def _literal_workload() -> IdentitySqlite:
    """Six literal variants of one statement (1 s CPU each) and one 3 s query."""

    store = IdentitySqlite()
    _hourly_intervals(store, 2)
    for offset in range(6):
        query_id = 100 + offset
        store.add_statement(
            query_id,
            f"SELECT * FROM dbo.Orders WHERE CustomerID = {offset}",
            "0x00000000000000F1",
            plan_id=200 + offset,
        )
        store.add_runtime(200 + offset, 1, 10, 100_000)
    store.add_statement(
        300, "(@c int)SELECT * FROM dbo.Lines WHERE c = @c", "0x00000000000000F2",
        plan_id=400, parameterization="User",
    )
    store.add_runtime(400, 1, 10, 300_000)
    return store


@pytest.mark.asyncio
async def test_query_hash_roll_up_ranks_the_literal_family_first() -> None:
    # Per query_id the 3 s query leads and each 1 s variant looks small. The
    # family burns 6 s: it is the real top consumer and a parameterization fix.
    store = _literal_workload()
    service = QueryStoreService(store)

    by_plan = await service.get_top_queries("db", "cpu", 1440, 5)
    by_hash = await service.get_top_queries("db", "cpu", 1440, 5, group_by="query_hash")

    assert by_plan[0]["query_id"] == 300
    family, single = by_hash
    assert family["query_hash"] == "0x00000000000000F1"
    assert family["total_cpu_us"] == pytest.approx(6_000_000)
    assert family["distinct_query_count"] == 6
    assert family["distinct_plan_count"] == 6
    assert family["unparameterized_query_count"] == 6
    assert 100 <= family["sample_query_id"] <= 105
    assert single["query_hash"] == "0x00000000000000F2"
    assert single["unparameterized_query_count"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("sort_by", ["cpu", "resource_blend"])
@pytest.mark.parametrize("group_by", ["plan", "query", "query_hash"])
async def test_top_queries_return_a_bounded_preview_and_ids(sort_by: str, group_by: str) -> None:
    # ORM statements run to tens of kilobytes. The ranking must not sort or
    # return that text; the ids let the caller fetch it when it is needed.
    store = IdentitySqlite()
    _hourly_intervals(store, 2)
    store.add_statement(7, "SELECT " + "x, " * 3000 + "1", "0x00000000000000A7", plan_id=70)
    store.add_runtime(70, 1, 5, 1000)

    [row] = await QueryStoreService(store).get_top_queries(
        "db", sort_by, 1440, 5, group_by=group_by
    )

    query = store.calls[-1][0]
    assert "query_sql_text" not in row
    assert len(row["query_text_preview"]) == 300
    assert row["query_text_id"] == 7
    assert row["query_hash"] == "0x00000000000000A7"
    assert not re.search(r"GROUP BY[^)]*query_sql_text", query)


@pytest.mark.asyncio
async def test_query_grouping_folds_the_plans_of_one_query() -> None:
    store = IdentitySqlite()
    _hourly_intervals(store, 2)
    store.add_statement(9, "SELECT 9", "0x0000000000000009", plan_id=90)
    store.insert(
        "query_store_plan", plan_id=91, query_id=9, query_plan="", is_forced_plan=0,
        force_failure_count=0,
    )
    store.add_runtime(90, 1, 4, 1000)
    store.add_runtime(91, 2, 6, 2000)

    [row] = await QueryStoreService(store).get_top_queries("db", "cpu", 1440, 5, group_by="query")

    assert (row["query_id"], row["plan_count"], row["executions"]) == (9, 2, 10)
    assert "plan_id" not in row


@pytest.mark.asyncio
async def test_top_queries_reject_an_unknown_grouping() -> None:
    with pytest.raises(ValueError, match="query_hash"):
        await QueryStoreService(IdentitySqlite()).get_top_queries(
            "db", "cpu", 60, 5, group_by="text"
        )
