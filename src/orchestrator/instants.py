"""One way to read a timestamp as an instant.

restic writes snapshot and lock times in the box's local offset ("...T12:00:00-06:00"), sometimes
as 'Z', and with nanoseconds. Our own receipts are aware UTC, but an older or hand-edited one may
have no zone. Compared as strings, or mixed naive and aware, these give a wrong order or a
TypeError at the moment it matters (a freshness check, a stale verdict, a drill that is due).
Every reader of such a time goes through `parse_time`: the result is always an aware UTC datetime
or None, so two times can always be compared and subtracted."""
from __future__ import annotations

import re
from datetime import UTC, datetime


def parse_time(text: object) -> datetime | None:
    """An aware UTC datetime from an ISO-8601 time, or None when it cannot be read. Accepts a
    'Z' or an offset, nanosecond fractions (truncated to microseconds), and a time with no zone
    (read as UTC)."""
    raw = str(text or "").strip()
    if not raw:
        return None
    raw = re.sub(r"(\.\d{6})\d+", r"\1", raw)
    try:
        when = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return when.astimezone(UTC) if when.tzinfo else when.replace(tzinfo=UTC)
