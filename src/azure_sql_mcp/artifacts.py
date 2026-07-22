from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from typing import Any


def json_text(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, indent=2, default=str)


@dataclass(frozen=True)
class ErrorPayload:
    code: str
    message: str
    details: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = {"ok": False, "code": self.code, "message": self.message}
        if self.details:
            payload["details"] = self.details
        return payload


@dataclass(frozen=True)
class ExplainPlanArtifact:
    database_name: str
    analyze: bool
    summary: dict[str, Any]
    raw_xml: str

    def as_dict(
        self,
        *,
        include_raw_xml: bool = True,
        raw_xml_resource: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "database_name": self.database_name,
            "analyze": self.analyze,
            "summary": self.summary,
            "raw_xml_length": len(self.raw_xml),
            "raw_xml_hash": hashlib.sha256(self.raw_xml.encode("utf-8")).hexdigest(),
        }
        if raw_xml_resource:
            payload["raw_xml_resource"] = raw_xml_resource
            payload["raw_xml_resource_uri"] = raw_xml_resource["resource_uri"]
        if include_raw_xml:
            payload["raw_xml"] = self.raw_xml
        return payload
