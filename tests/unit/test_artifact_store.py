from __future__ import annotations

import hashlib

from azure_sql_mcp.artifact_store import ArtifactStore


def test_artifact_store_returns_metadata_and_text_by_id() -> None:
    store = ArtifactStore()

    metadata = store.put_text(
        kind="showplan-xml",
        text="<ShowPlanXML />",
        mime_type="application/xml",
        metadata={"database_name": "appdb"},
    )

    artifact = store.get(metadata["artifact_id"])
    assert metadata["resource_uri"] == f"azuresql-artifact://{metadata['artifact_id']}"
    assert metadata["length"] == len("<ShowPlanXML />")
    assert metadata["size_bytes"] == len(b"<ShowPlanXML />")
    assert metadata["sha256"] == hashlib.sha256(b"<ShowPlanXML />").hexdigest()
    assert metadata["expires_at_utc"]
    assert artifact.text == "<ShowPlanXML />"
    assert artifact.size_bytes == len(b"<ShowPlanXML />")
    assert artifact.metadata == {"database_name": "appdb"}


def test_artifact_store_evicts_oldest_artifacts_by_count() -> None:
    store = ArtifactStore(max_artifacts=2)

    first = store.put_text(kind="showplan-xml", text="one", mime_type="text/plain")
    second = store.put_text(kind="showplan-xml", text="two", mime_type="text/plain")
    third = store.put_text(kind="showplan-xml", text="three", mime_type="text/plain")

    try:
        store.get(first["artifact_id"])
    except KeyError:
        pass
    else:
        raise AssertionError("Expected oldest artifact to be evicted.")
    assert store.get(second["artifact_id"]).text == "two"
    assert store.get(third["artifact_id"]).text == "three"


def test_artifact_store_rejects_single_artifact_above_byte_limit() -> None:
    store = ArtifactStore(max_bytes=4)

    try:
        store.put_text(kind="showplan-xml", text="12345", mime_type="text/plain")
    except ValueError as exc:
        assert "above configured max_bytes" in str(exc)
    else:
        raise AssertionError("Expected oversized artifact to be rejected.")
