from __future__ import annotations

import pytest

from azure_sql_mcp.equivalence_contract import analyze_equivalence_preflight


def test_deterministic_ordered_query_supports_direct_snapshot() -> None:
    result = analyze_equivalence_preflight(
        "SELECT id FROM dbo.Orders ORDER BY id"
    )

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
