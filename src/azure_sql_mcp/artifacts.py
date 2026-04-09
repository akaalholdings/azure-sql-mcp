from __future__ import annotations

import json
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

    def as_dict(self) -> dict[str, Any]:
        return {
            "database_name": self.database_name,
            "analyze": self.analyze,
            "summary": self.summary,
            "raw_xml": self.raw_xml,
        }

