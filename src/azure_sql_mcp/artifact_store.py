from __future__ import annotations

import hashlib
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from typing import Any

DEFAULT_MAX_ARTIFACTS = 128
DEFAULT_MAX_BYTES = 16 * 1024 * 1024
DEFAULT_TTL_SECONDS = 60 * 60


@dataclass(frozen=True)
class Artifact:
    artifact_id: str
    kind: str
    mime_type: str
    text: str
    size_bytes: int
    created_at_utc: str
    expires_at_utc: str
    metadata: dict[str, Any]


class ArtifactStore:
    def __init__(
        self,
        *,
        max_artifacts: int = DEFAULT_MAX_ARTIFACTS,
        max_bytes: int = DEFAULT_MAX_BYTES,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ):
        if max_artifacts <= 0:
            raise ValueError("max_artifacts must be greater than 0.")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be greater than 0.")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be greater than 0.")
        self.max_artifacts = max_artifacts
        self.max_bytes = max_bytes
        self.ttl_seconds = ttl_seconds
        self._artifacts: OrderedDict[str, Artifact] = OrderedDict()
        self._total_bytes = 0

    def put_text(
        self,
        *,
        kind: str,
        text: str,
        mime_type: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._evict_expired()
        artifact_id = f"{kind}-{uuid.uuid4().hex}"
        encoded = text.encode("utf-8")
        size_bytes = len(encoded)
        if size_bytes > self.max_bytes:
            raise ValueError(
                f"Artifact is {size_bytes} bytes, above configured max_bytes={self.max_bytes}."
            )
        digest = hashlib.sha256(encoded).hexdigest()
        created_at = datetime.now(timezone.utc)
        expires_at = created_at + timedelta(seconds=self.ttl_seconds)
        artifact = Artifact(
            artifact_id=artifact_id,
            kind=kind,
            mime_type=mime_type,
            text=text,
            size_bytes=size_bytes,
            created_at_utc=created_at.isoformat(),
            expires_at_utc=expires_at.isoformat(),
            metadata=metadata or {},
        )
        self._artifacts[artifact_id] = artifact
        self._total_bytes += size_bytes
        self._evict_to_bounds()
        return {
            "artifact_id": artifact_id,
            "resource_uri": f"azuresql-artifact://{artifact_id}",
            "kind": kind,
            "mime_type": mime_type,
            "length": len(text),
            "size_bytes": size_bytes,
            "sha256": digest,
            "created_at_utc": artifact.created_at_utc,
            "expires_at_utc": artifact.expires_at_utc,
            "metadata": artifact.metadata,
        }

    def get(self, artifact_id: str) -> Artifact:
        self._evict_expired()
        try:
            artifact = self._artifacts.pop(artifact_id)
        except KeyError as exc:
            raise KeyError(f"Unknown artifact_id: {artifact_id}") from exc
        self._artifacts[artifact_id] = artifact
        return artifact

    def _evict_expired(self) -> None:
        now = datetime.now(timezone.utc)
        expired = [
            artifact_id
            for artifact_id, artifact in self._artifacts.items()
            if datetime.fromisoformat(artifact.expires_at_utc) <= now
        ]
        for artifact_id in expired:
            self._remove(artifact_id)

    def _evict_to_bounds(self) -> None:
        while len(self._artifacts) > self.max_artifacts or self._total_bytes > self.max_bytes:
            artifact_id, _ = next(iter(self._artifacts.items()))
            self._remove(artifact_id)

    def _remove(self, artifact_id: str) -> None:
        artifact = self._artifacts.pop(artifact_id)
        self._total_bytes = max(0, self._total_bytes - artifact.size_bytes)
