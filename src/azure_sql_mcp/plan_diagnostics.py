"""Pure parsers for sourced query-tuning diagnostics.

The SQL executor owns transport and cursor message collection. This module only
parses messages that were actually emitted by ``SET STATISTICS IO ON`` and keeps
their sample identity attached to every measurement.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any
from typing import Mapping


_TABLE_MESSAGE = re.compile(
    r"\bTable\s+'(?P<table>(?:''|[^'])*)'\.\s*(?P<body>.*?)(?=\bTable\s+'|$)",
    re.IGNORECASE | re.DOTALL,
)
_IO_METRIC = re.compile(
    r"(?P<name>[A-Za-z][A-Za-z -]*?)\s*(?:=\s*)?(?P<value>-?\d+)",
    re.IGNORECASE,
)
_METRIC_NAMES = {
    "scan count": "scan_count",
    "logical reads": "logical_reads",
    "physical reads": "physical_reads",
    "page server reads": "page_server_reads",
    "read-ahead reads": "read_ahead_reads",
    "page server read-ahead reads": "page_server_read_ahead_reads",
    "lob logical reads": "lob_logical_reads",
    "lob physical reads": "lob_physical_reads",
    "lob page server reads": "lob_page_server_reads",
    "lob read-ahead reads": "lob_read_ahead_reads",
    "lob page server read-ahead reads": "lob_page_server_read_ahead_reads",
}


def _message_text(message: Any) -> str:
    if isinstance(message, str):
        return message
    if isinstance(message, Mapping):
        for key in ("message", "text", "value"):
            if key in message:
                value = _message_text(message[key])
                if value:
                    return value
    if isinstance(message, (tuple, list)):
        string_values = [value for value in message if isinstance(value, str)]
        for value in reversed(string_values):
            if _TABLE_MESSAGE.search(value):
                return value
        if string_values:
            return string_values[-1]
        for value in message:
            text = _message_text(value)
            if text:
                return text
    value = getattr(message, "message", None) or getattr(message, "text", None)
    if isinstance(value, str):
        return value
    return str(message)


def parse_statistics_io_messages(
    messages: Iterable[Any],
    *,
    sample_id: str,
    provenance: str = "SET STATISTICS IO ON message",
) -> dict[str, Any]:
    """Parse one execution sample's SQL Server STATISTICS IO messages.

    ``query_totals`` is aggregated only from the table messages in this sample.
    It is explicitly not inferred from SHOWPLAN operator or thread counters.
    """
    if not sample_id.strip():
        raise ValueError("sample_id must not be empty")

    tables: list[dict[str, Any]] = []
    ignored_messages: list[int] = []
    for message_index, message in enumerate(messages):
        text = _message_text(message)
        matches = list(_TABLE_MESSAGE.finditer(text))
        if not matches:
            ignored_messages.append(message_index)
            continue
        for match in matches:
            metrics: dict[str, int] = {}
            for metric in _IO_METRIC.finditer(match.group("body")):
                name = _METRIC_NAMES.get(metric.group("name").strip().lower())
                if name:
                    metrics[name] = int(metric.group("value"))
            if not metrics:
                ignored_messages.append(message_index)
                continue
            tables.append(
                {
                    "sample_id": sample_id,
                    "message_index": message_index,
                    "table": match.group("table").replace("''", "'"),
                    "metrics": metrics,
                    "provenance": provenance,
                }
            )

    totals: dict[str, int] = {}
    for table in tables:
        for name, value in table["metrics"].items():
            totals[name] = totals.get(name, 0) + value

    return {
        "sample_id": sample_id,
        "provenance": provenance,
        "tables": tables,
        "query_totals": totals,
        "query_totals_source": "statistics_io_table_messages",
        "operator_thread_counters_not_used": True,
        "ignored_message_indexes": ignored_messages,
    }


def parse_statistics_io(
    messages: Iterable[Any],
    *,
    sample_id: str,
    provenance: str = "SET STATISTICS IO ON message",
) -> dict[str, Any]:
    """Short alias for integrations that call the feature STATISTICS IO."""
    return parse_statistics_io_messages(
        messages,
        sample_id=sample_id,
        provenance=provenance,
    )


def summarize_statistics_io_samples(samples: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Keep multiple samples separate while returning coverage metadata."""
    normalized = [dict(sample) for sample in samples]
    return {
        "sample_count": len(normalized),
        "samples": normalized,
        "metric_provenance": "statistics_io_per_sample",
        "cross_sample_totals": None,
        "cross_sample_totals_reason": "samples are not collapsed into a fake query total",
    }
