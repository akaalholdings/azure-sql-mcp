from __future__ import annotations

from azure_sql_mcp.platform_capabilities import summarize_azure_sql_capabilities


def test_capability_summary_is_metadata_driven_and_does_not_prescribe_hints() -> None:
    summary = summarize_azure_sql_capabilities(
        [
            {
                "database_name": "appdb",
                "compatibility_level": 160,
                "engine_edition": 5,
                "product_version": "16.0",
                "query_store_actual_state": "READ_WRITE",
                "configuration_name": "PARAMETER_SENSITIVE_PLAN_OPTIMIZATION",
                "configuration_value": 1,
                "psp_observed": True,
            },
            {
                "database_name": "appdb",
                "compatibility_level": 160,
                "configuration_name": "DOP_FEEDBACK",
                "configuration_value": "ON",
            },
        ]
    )

    assert summary["platform"] == "azure_sql_database_paas"
    assert summary["compatibility_level"] == 160
    psp = summary["features"]["parameter_sensitive_plan_optimization"]
    assert psp["applicable"] is True
    assert psp["enabled"] is True
    assert psp["observed"] is True
    assert psp["minimum_compatibility_level"] == 160
    assert psp["prerequisites_met"] is True
    assert summary["features"]["optional_parameter_optimization"]["applicable"] is False
    assert summary["features"]["degree_of_parallelism_feedback"]["enabled"] is True
    assert summary["features"]["degree_of_parallelism_feedback"][
        "requires_query_store_read_write"
    ] is True
    assert summary["query_store"]["read_write"] is True
    assert summary["platform_verified"] is True
    assert summary["hint_policy"]["compatibility_level_is_not_a_hint_recommendation"] is True


def test_capability_thresholds_do_not_claim_observed_feedback_without_evidence() -> None:
    summary = summarize_azure_sql_capabilities(
        {"database_name": "appdb", "compatibility_level": 170}
    )

    assert summary["features"]["optional_parameter_optimization"]["applicable"] is True
    assert summary["features"]["optional_parameter_optimization"]["enabled"] is None
    assert summary["features"]["optional_parameter_optimization"]["observed"] is None
    assert summary["features"]["memory_grant_feedback"]["applicable"] is True
    assert summary["query_store"]["read_write"] is None
    assert summary["features"]["memory_grant_feedback_persistence"][
        "prerequisites_met"
    ] is None


def test_missing_compatibility_level_keeps_capability_applicability_unknown() -> None:
    summary = summarize_azure_sql_capabilities({"database_name": "appdb"})

    assert summary["features"]["optional_parameter_optimization"]["applicable"] is None
    assert summary["features"]["optional_parameter_optimization"][
        "prerequisites_met"
    ] is None


def test_capability_summary_exposes_optimizer_relevant_azure_sql_features() -> None:
    summary = summarize_azure_sql_capabilities(
        [
            {
                "database_name": "appdb",
                "compatibility_level": 170,
                "engine_edition": 5,
                "is_auto_create_stats_on": 1,
                "is_auto_update_stats_on": 1,
                "is_auto_update_stats_async_on": 0,
                "is_parameterization_forced": 0,
                "query_store_actual_state": "READ_WRITE",
                "query_store_capture_mode": "AUTO",
                "configuration_name": "ROW_MODE_MEMORY_GRANT_FEEDBACK",
                "configuration_value": 1,
            },
            {
                "database_name": "appdb",
                "compatibility_level": 170,
                "configuration_name": "DEFERRED_COMPILATION_TV",
                "configuration_value": 1,
            },
            {
                "database_name": "appdb",
                "compatibility_level": 170,
                "configuration_name": "TSQL_SCALAR_UDF_INLINING",
                "configuration_value": 0,
            },
        ]
    )

    assert summary["features"]["row_mode_memory_grant_feedback"]["enabled"] is True
    assert summary["features"]["table_variable_deferred_compilation"]["enabled"] is True
    assert summary["features"]["scalar_udf_inlining"]["enabled"] is False
    assert summary["features"]["optional_parameter_optimization"][
        "minimum_compatibility_level"
    ] == 170
    assert summary["statistics_options"] == {
        "auto_create_statistics": True,
        "auto_update_statistics": True,
        "auto_update_statistics_async": False,
        "forced_parameterization": False,
    }
