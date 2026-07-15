from __future__ import annotations

import os

import pytest


@pytest.mark.skipif(
    not os.getenv("AZURE_SQL_SERVER") or not os.getenv("AZURE_SQL_DEFAULT_DATABASE"),
    reason="Azure SQL environment variables are not configured.",
)
def test_azure_sql_integration_environment_is_present():
    assert os.getenv("AZURE_SQL_SERVER")
    assert os.getenv("AZURE_SQL_DEFAULT_DATABASE")
