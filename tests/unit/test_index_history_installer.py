from __future__ import annotations

import re
from pathlib import Path


INSTALLER = (
    Path(__file__).parents[2] / "sql" / "Install-IndexReviewHistory-v1.sql"
).read_text(encoding="utf-8")


def test_index_history_installer_manages_only_the_two_history_tables() -> None:
    assert INSTALLER.count("CREATE TABLE [dbatools].[IndexReviewRun]") == 1
    assert INSTALLER.count("CREATE TABLE [dbatools].[IndexReviewSnapshot]") == 1
    assert len(re.findall(r"\bCREATE\s+TABLE\b", INSTALLER, re.IGNORECASE)) == 2
    assert len(re.findall(r"\bBEGIN\s+TRANSACTION\b", INSTALLER, re.IGNORECASE)) == 1
    assert len(re.findall(r"\bCOMMIT\s+TRANSACTION\b", INSTALLER, re.IGNORECASE)) == 1


def test_index_history_installer_does_not_manage_principals_or_permissions() -> None:
    assert not re.search(r"\bCREATE\s+SCHEMA\b", INSTALLER, re.IGNORECASE)
    assert not re.search(r"\b(?:INSERT|UPDATE|DELETE)\b", INSTALLER, re.IGNORECASE)
    assert "DATABASE_PRINCIPAL_ID" not in INSTALLER.upper()
    assert not re.search(
        r"\b(?:CREATE|ALTER|DROP)\s+(?:USER|ROLE)\b", INSTALLER, re.IGNORECASE
    )
    assert not re.search(r"\bALTER\s+ROLE\b", INSTALLER, re.IGNORECASE)
    assert not re.search(r"\b(?:GRANT|DENY|REVOKE)\b", INSTALLER, re.IGNORECASE)
