"""Shared time-window handling for Query Store and history reads.

A window has a length and an end. ``as_of_utc`` moves the end into the past so a
past incident is read as itself instead of through a wider window that changes
every aggregate. Future instants are refused, never silently read as "now".
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone

AS_OF_DESCRIPTION = (
    "Optional ISO-8601 UTC end of the window, for example 2026-09-30T05:00:00Z or "
    "2026-09-30. Defaults to now. Future instants are refused. Use it to read a past "
    "incident instead of widening the window."
)


def parse_as_of(as_of_utc: str | None, *, now: datetime | None = None) -> datetime | None:
    if as_of_utc is None or not str(as_of_utc).strip():
        return None
    current = now or datetime.now(timezone.utc)
    text = str(as_of_utc).strip()
    if len(text) == 10:
        text += "T00:00:00"
    try:
        value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            "as_of_utc must be an ISO-8601 UTC instant such as 2026-09-30T05:00:00Z"
        ) from exc
    value = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if value > current + timedelta(minutes=1):
        raise ValueError("as_of_utc must not be in the future")
    return value


def window_bounds(
    minutes: int, as_of_utc: str | None, *, now: datetime | None = None
) -> tuple[datetime, datetime]:
    end = parse_as_of(as_of_utc, now=now) or (now or datetime.now(timezone.utc))
    return end - timedelta(minutes=max(1, int(minutes))), end


def sql_utc(value: datetime) -> str:
    """Naive UTC ISO text for ``TODATETIMEOFFSET(CAST(? AS datetime2), 0)``."""

    return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


def iso_z(value: datetime) -> str:
    return sql_utc(value) + "Z"
