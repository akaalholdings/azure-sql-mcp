from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Structured JSON log formatter.

    Emits one JSON object per line with standard fields plus optional extras
    commonly attached by the MCP server (tool_name, database_name,
    correlation_id, duration_ms).
    """

    _EXTRA_KEYS = (
        "tool_name",
        "database_name",
        "correlation_id",
        "duration_ms",
        "error",
        "attempt",
        "server",
    )

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Pull well-known extras if present.
        for key in self._EXTRA_KEYS:
            value = getattr(record, key, None)
            if value is not None:
                log_entry[key] = value

        # Include exception info when present.
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def configure_logging(log_level: str, log_format: str) -> None:
    """Set up root logging with the chosen format (``'text'`` or ``'json'``).

    Must be called early in application startup (before any log messages are
    emitted).
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    root = logging.getLogger()
    # Remove any existing handlers to avoid duplicate output.
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stderr)

    if log_format.lower() == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-8s %(name)s - %(message)s")
        )

    root.addHandler(handler)
    root.setLevel(level)
