from __future__ import annotations

import json

import pytest

from azure_sql_mcp.equivalence_contract import analyze_equivalence_preflight
from azure_sql_mcp.equivalence_preflight import EquivalencePreflightService


def _view_resolver(definitions):
    async def resolve(_database_name: str, schema_name: str, object_name: str):
        return definitions.get((schema_name, object_name))

    return resolve


def test_deterministic_ordered_query_supports_direct_snapshot() -> None:
    result = analyze_equivalence_preflight("SELECT id FROM dbo.Orders ORDER BY id")

    assert result.as_dict() == {
        "contract_version": 1,
        "classification": "direct_snapshot",
        "direct_snapshot_supported": True,
        "risk_codes": [],
        "volatile_functions": [],
        "statement_stable_clock_functions": [],
        "row_volatile_functions": [],
        "nonrepeatable_table_sample_count": 0,
        "unordered_row_limit_count": 0,
        "ordered_row_limit_without_total_order_count": 0,
        "window_order_without_total_order_count": 0,
        "deterministic_proof_contract_available": False,
        "details": [],
        "performance_sql_must_remain_unchanged": True,
        "proxy_scope": "not_applicable",
    }


def test_clock_and_unordered_top_require_explicit_proof_contract() -> None:
    result = analyze_equivalence_preflight(
        "SELECT TOP (30) id FROM dbo.Orders WHERE created_at < GETDATE()"
    ).as_dict()

    assert result["classification"] == "proof_contract_required"
    assert result["direct_snapshot_supported"] is False
    assert result["risk_codes"] == [
        "statement_stable_clock",
        "unordered_row_limit",
    ]
    assert result["statement_stable_clock_functions"] == ["CURRENT_TIMESTAMP"]
    assert result["unordered_row_limit_count"] == 1


def test_ordered_top_requires_verified_unique_total_order() -> None:
    result = analyze_equivalence_preflight(
        "SELECT TOP (1) id FROM dbo.Orders ORDER BY status"
    ).as_dict()

    assert result["risk_codes"] == ["row_limit_total_order_unproven"]
    assert result["ordered_row_limit_without_total_order_count"] == 1


def test_order_sensitive_window_requires_verified_unique_total_order() -> None:
    result = analyze_equivalence_preflight(
        """
        SELECT id, ROW_NUMBER() OVER (ORDER BY status) AS row_number
        FROM dbo.Orders
        """
    ).as_dict()

    assert result["risk_codes"] == ["window_order_total_order_unproven"]
    assert result["window_order_without_total_order_count"] == 1


def test_partition_only_window_remains_directly_comparable() -> None:
    result = analyze_equivalence_preflight(
        """
        SELECT id, SUM(amount) OVER (PARTITION BY status) AS status_total
        FROM dbo.Orders
        """
    ).as_dict()

    assert result["classification"] == "direct_snapshot"
    assert result["window_order_without_total_order_count"] == 0


def test_row_volatile_functions_are_not_presented_as_freezable_clock_values() -> None:
    result = analyze_equivalence_preflight(
        "SELECT NEWID() AS token, RAND() AS sample FROM dbo.Orders"
    ).as_dict()

    assert result["classification"] == "proof_contract_required"
    assert result["risk_codes"] == ["row_volatile_function"]
    assert result["row_volatile_functions"] == ["NEWID", "RAND"]
    assert result["statement_stable_clock_functions"] == []


def test_sysdatetimeoffset_requires_an_explicit_clock_proof() -> None:
    result = analyze_equivalence_preflight(
        "SELECT SYSDATETIMEOFFSET() AS captured_at"
    ).as_dict()

    assert result["risk_codes"] == ["statement_stable_clock"]
    assert result["statement_stable_clock_functions"] == ["SYSDATETIMEOFFSET"]


def test_nested_unordered_limit_is_detected_even_when_outer_query_is_ordered() -> None:
    result = analyze_equivalence_preflight(
        """
        SELECT picked.id
        FROM (SELECT TOP (1) id FROM dbo.Orders) AS picked
        ORDER BY picked.id
        """
    ).as_dict()

    assert result["risk_codes"] == ["unordered_row_limit"]
    assert result["unordered_row_limit_count"] == 1


def test_nonrepeatable_table_sample_requires_a_proof_contract() -> None:
    result = analyze_equivalence_preflight(
        "SELECT id FROM dbo.Orders TABLESAMPLE (10 PERCENT)"
    ).as_dict()

    assert result["risk_codes"] == ["nonrepeatable_table_sample"]
    assert result["nonrepeatable_table_sample_count"] == 1


def test_repeatable_table_sample_and_seeded_rand_are_directly_comparable() -> None:
    result = analyze_equivalence_preflight(
        """
        SELECT RAND(42) AS sample, id
        FROM dbo.Orders TABLESAMPLE (10 PERCENT) REPEATABLE (42)
        """
    ).as_dict()

    assert result["classification"] == "direct_snapshot"
    assert result["risk_codes"] == []


def test_nondeterministic_rand_seed_is_still_rejected() -> None:
    result = analyze_equivalence_preflight(
        "SELECT RAND(CHECKSUM(NEWID())) AS sample"
    ).as_dict()

    assert result["risk_codes"] == ["row_volatile_function"]
    assert result["row_volatile_functions"] == ["NEWID"]


def test_parse_failure_is_a_safe_preflight_error() -> None:
    with pytest.raises(ValueError, match="equivalence preflight"):
        analyze_equivalence_preflight("SELECT FROM")


@pytest.mark.asyncio
async def test_database_preflight_lists_direct_function_verdicts() -> None:
    result = await EquivalencePreflightService().analyze(
        "appdb",
        "SELECT GETDATE(), RAND(42), NEWID()",
    )

    verdicts = {
        (item["function"], item["source"]): item
        for item in result.as_dict()["function_verdicts"]
    }
    assert verdicts[("GETDATE", "query")]["count"] == 1
    assert verdicts[("GETDATE", "query")]["verdict"] == "proof_required"
    assert verdicts[("RAND", "query")]["verdict"] == "safe"
    assert verdicts[("NEWID", "query")]["verdict"] == "proof_required"
    assert result.as_dict()["risk_codes"] == [
        "statement_stable_clock",
        "row_volatile_function",
    ]


@pytest.mark.asyncio
async def test_database_preflight_enumerates_every_supported_function() -> None:
    result = await EquivalencePreflightService().analyze(
        "appdb",
        """
        SELECT
            CURRENT_TIMESTAMP,
            GETDATE(),
            GETDATE(),
            GETUTCDATE(),
            SYSDATETIME(),
            SYSDATETIMEOFFSET(),
            SYSUTCDATETIME(),
            NEWID(),
            RAND(),
            RAND(42),
            RAND(-1),
            CRYPT_GEN_RANDOM(4)
        """,
    )

    verdicts = {
        (item["function"], item["category"]): item
        for item in result.as_dict()["functions"]
    }
    assert set(verdicts) == {
        ("CURRENT_TIMESTAMP", "clock"),
        ("GETDATE", "clock"),
        ("GETUTCDATE", "clock"),
        ("SYSDATETIME", "clock"),
        ("SYSDATETIMEOFFSET", "clock"),
        ("SYSUTCDATETIME", "clock"),
        ("NEWID", "volatile"),
        ("RAND", "volatile"),
        ("RAND", "safely_seeded"),
        ("CRYPT_GEN_RANDOM", "volatile"),
    }
    assert verdicts[("GETDATE", "clock")]["count"] == 2
    assert verdicts[("RAND", "safely_seeded")]["count"] == 2
    assert verdicts[("RAND", "safely_seeded")]["verdict"] == "safe"
    assert all(
        item["source"] == "query" and item["reason"] for item in verdicts.values()
    )


@pytest.mark.asyncio
async def test_database_preflight_recurses_nested_views_and_detects_getdate() -> None:
    resolver = _view_resolver(
        {
            ("dbo", "Orders"): {"object_type": "USER_TABLE"},
            (
                "dbo",
                "v_outer",
            ): {
                "object_type": "VIEW",
                "definition": ("SELECT id, GETDATE() AS captured_at FROM dbo.v_inner"),
            },
            (
                "dbo",
                "v_inner",
            ): {
                "object_type": "VIEW",
                "definition": "SELECT id FROM dbo.Orders",
            },
        }
    )

    payload = await EquivalencePreflightService(resolver).analyze(
        "appdb",
        "SELECT * FROM dbo.v_outer",
    )
    result = payload.as_dict()

    assert result["direct_snapshot_supported"] is False
    assert result["coverage_complete"] is True
    assert result["analysis_scope"] == "query_and_recursive_view_definitions"
    assert result["analysis_coverage"]["status"] == "complete"
    assert [
        (item["schema_name"], item["object_name"], item["depth"])
        for item in result["resolved_view_dependencies"]
    ] == [("dbo", "v_outer", 1), ("dbo", "v_inner", 2)]
    assert {
        (item["function"], item["source"], item["count"])
        for item in result["functions"]
    } == {("GETDATE", "dbo.v_outer", 1)}
    assert result["functions"] == result["function_verdicts"]
    assert result["resolved_dependencies"] == result["resolved_view_dependencies"]
    assert result["unresolved_dependencies"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("resolution", "reason"),
    [
        ({"status": "encrypted"}, "encrypted_view_dependency"),
        ({"accessible": False}, "inaccessible_view_dependency"),
        (None, "unresolved_view_dependency"),
    ],
)
async def test_database_preflight_fails_closed_for_unavailable_views(
    resolution,
    reason: str,
) -> None:
    resolver = _view_resolver({("dbo", "v_hidden"): resolution})

    result = await EquivalencePreflightService(resolver).analyze(
        "appdb",
        "SELECT * FROM dbo.v_hidden",
    )
    payload = result.as_dict()

    assert payload["direct_snapshot_supported"] is False
    assert payload["analysis_coverage"]["status"] == "incomplete"
    assert payload["unresolved_dependencies"][0]["reason"] == reason
    assert reason in payload["risk_codes"]


@pytest.mark.asyncio
async def test_database_preflight_fails_closed_for_synonyms() -> None:
    resolver = _view_resolver(
        {
            ("dbo", "CurrentOrders"): {
                "object_type": "SYNONYM",
                "schema_name": "dbo",
                "object_name": "CurrentOrders",
            }
        }
    )

    result = await EquivalencePreflightService(resolver).analyze(
        "appdb",
        "SELECT * FROM dbo.CurrentOrders",
    )

    payload = result.as_dict()
    assert payload["direct_snapshot_supported"] is False
    assert payload["unresolved_dependencies"][0]["reason"] == (
        "unresolved_view_dependency"
    )


@pytest.mark.asyncio
async def test_database_preflight_does_not_assume_dbo_for_unqualified_names() -> None:
    calls = []

    async def resolve(_database_name: str, schema_name: str, object_name: str):
        calls.append((schema_name, object_name))
        return None

    result = await EquivalencePreflightService(resolve).analyze(
        "appdb",
        "SELECT * FROM Orders",
    )

    payload = result.as_dict()
    assert calls == [("", "Orders")]
    assert payload["direct_snapshot_supported"] is False
    assert payload["unresolved_dependencies"][0]["schema_name"] == ""


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "definition",
    [
        "SELECT * FROM OtherDb.dbo.RemoteView",
        "SELECT * FROM LinkedServer.OtherDb.dbo.RemoteView",
    ],
)
async def test_database_preflight_fails_closed_for_external_dependencies(
    definition: str,
) -> None:
    resolver = _view_resolver(
        {
            ("dbo", "v_outer"): {
                "object_type": "VIEW",
                "definition": definition,
            },
            ("dbo", "RemoteView"): {
                "object_type": "USER_TABLE",
            },
            ("OtherDb", "RemoteView"): {
                "object_type": "USER_TABLE",
            },
        }
    )

    result = await EquivalencePreflightService(resolver).analyze(
        "appdb",
        "SELECT * FROM dbo.v_outer",
    )

    payload = result.as_dict()
    assert payload["direct_snapshot_supported"] is False
    assert payload["coverage_complete"] is False
    assert "cross_database_dependency" in payload["risk_codes"]
    assert payload["unresolved_dependencies"][0]["reason"] == (
        "cross_database_dependency"
    )


@pytest.mark.asyncio
async def test_database_preflight_sanitizes_dynamic_external_sources() -> None:
    resolver = _view_resolver(
        {
            ("dbo", "v_outer"): {
                "object_type": "VIEW",
                "definition": (
                    "SELECT * FROM "
                    "OPENDATASOURCE('SQLNCLI', 'Data Source=remote').db.dbo.t"
                ),
            },
            ("db", "t"): {
                "object_type": "USER_TABLE",
            },
        }
    )

    result = await EquivalencePreflightService(resolver).analyze(
        "appdb",
        "SELECT * FROM dbo.v_outer",
    )

    payload = result.as_dict()
    serialized = json.dumps(payload)
    assert payload["direct_snapshot_supported"] is False
    assert payload["coverage_complete"] is False
    assert payload["unresolved_dependencies"][0]["catalog_name"] == (
        "dynamic_external_source"
    )
    assert payload["unresolved_dependencies"][0]["reason"] == (
        "cross_database_dependency"
    )
    assert "OPENDATASOURCE" not in serialized
    assert "Data Source" not in serialized


@pytest.mark.asyncio
async def test_database_preflight_preserves_case_sensitive_object_names() -> None:
    resolver = _view_resolver(
        {
            ("dbo", "Safe"): {
                "object_type": "USER_TABLE",
            },
            ("dbo", "safe"): {
                "object_type": "VIEW",
                "definition": "SELECT GETDATE() AS captured_at",
            },
        }
    )

    result = await EquivalencePreflightService(resolver).analyze(
        "appdb",
        (
            "SELECT id FROM dbo.Safe "
            "UNION ALL "
            "SELECT id FROM dbo.safe"
        ),
    )

    payload = result.as_dict()
    assert payload["direct_snapshot_supported"] is False
    assert payload["coverage_complete"] is True
    assert payload["resolved_dependencies"][0]["object_name"] == "safe"
    assert payload["functions"][0]["function"] == "GETDATE"


@pytest.mark.asyncio
async def test_executor_resolves_unqualified_name_to_canonical_non_dbo_view() -> None:
    async def fetch(_database_name: str, _query: str, params):
        assert params == ["", "Orders", "", "Orders"]
        return [
            {
                "schema_name": "sales",
                "object_name": "Orders",
                "object_type": "VIEW",
                "definition": "SELECT GETDATE() AS captured_at",
            }
        ]

    result = await EquivalencePreflightService(executor=fetch).analyze(
        "appdb",
        "SELECT * FROM Orders",
    )

    payload = result.as_dict()
    assert payload["direct_snapshot_supported"] is False
    assert payload["resolved_dependencies"][0]["schema_name"] == "sales"
    assert payload["functions"][0]["source"] == "sales.Orders"


@pytest.mark.asyncio
async def test_database_preflight_fails_closed_for_cycles_and_depth() -> None:
    cyclic = _view_resolver(
        {
            ("dbo", "v_one"): {
                "object_type": "VIEW",
                "definition": "SELECT * FROM dbo.v_two",
            },
            ("dbo", "v_two"): {
                "object_type": "VIEW",
                "definition": "SELECT * FROM dbo.v_one",
            },
        }
    )
    cyclic_result = await EquivalencePreflightService(cyclic).analyze(
        "appdb",
        "SELECT * FROM dbo.v_one",
    )
    assert cyclic_result.as_dict()["unresolved_dependencies"][0]["reason"] == (
        "cyclic_view_dependency"
    )

    definitions = {}
    for number in range(1, 10):
        definitions[("dbo", f"v_{number}")] = {
            "object_type": "VIEW",
            "definition": (
                f"SELECT * FROM dbo.v_{number + 1}"
                if number < 9
                else "SELECT * FROM dbo.Orders"
            ),
        }
    definitions[("dbo", "Orders")] = {"object_type": "USER_TABLE"}
    depth_result = await EquivalencePreflightService(
        _view_resolver(definitions)
    ).analyze("appdb", "SELECT * FROM dbo.v_1")
    depth_payload = depth_result.as_dict()

    assert "view_dependency_depth_exceeded" in depth_payload["risk_codes"]
    assert any(
        item["reason"] == "view_dependency_depth_exceeded"
        for item in depth_payload["unresolved_dependencies"]
    )


@pytest.mark.asyncio
async def test_database_preflight_allows_eight_views_ending_in_a_table() -> None:
    definitions = {
        ("dbo", f"v_{number}"): {
            "object_type": "VIEW",
            "definition": (
                f"SELECT * FROM dbo.v_{number + 1}"
                if number < 8
                else "SELECT * FROM dbo.Orders"
            ),
        }
        for number in range(1, 9)
    }
    definitions[("dbo", "Orders")] = {"object_type": "USER_TABLE"}

    result = await EquivalencePreflightService(_view_resolver(definitions)).analyze(
        "appdb", "SELECT * FROM dbo.v_1"
    )
    payload = result.as_dict()

    assert payload["coverage_complete"] is True
    assert payload["risk_codes"] == []
    assert len(payload["resolved_dependencies"]) == 8


@pytest.mark.asyncio
async def test_database_preflight_returns_summaries_without_raw_view_definitions() -> (
    None
):
    definition = "SELECT GETDATE() AS secret_definition FROM dbo.Orders"
    resolver = _view_resolver(
        {
            ("dbo", "Orders"): {"object_type": "USER_TABLE"},
            ("dbo", "v_secret"): {
                "object_type": "VIEW",
                "definition": definition,
            },
        }
    )

    result = await EquivalencePreflightService(resolver).analyze(
        "appdb",
        "SELECT * FROM dbo.v_secret",
    )

    serialized = json.dumps(result.as_dict(), sort_keys=True)
    assert "secret_definition" not in serialized
    assert '"definition"' not in serialized


@pytest.mark.asyncio
async def test_database_preflight_accepts_executor_compatible_fetcher() -> None:
    async def fetch(_database_name: str, _query: str, params):
        schema_name, object_name = params[:2]
        if (schema_name, object_name) == ("dbo", "Orders"):
            return [{"object_type": "USER_TABLE"}]
        return [
            {
                "object_type": "VIEW",
                "definition": "SELECT id FROM dbo.Orders",
            }
        ]

    result = await EquivalencePreflightService(executor=fetch).analyze(
        "appdb",
        "SELECT * FROM dbo.v_orders",
    )

    assert result.as_dict()["analysis_coverage"]["complete"] is True
    assert result.as_dict()["resolved_view_dependencies"][0]["object_name"] == (
        "v_orders"
    )
