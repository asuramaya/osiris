"""An offload target that has gone quiet: enabled, and no successful offload for a number of days.

A target that is skipped quietly (the laptop is away from home, the drive is not docked) is
correct behaviour, but a skip that lasts a week means nothing is being backed up off the box and
nobody is told. This module decides, from the offload receipts alone (no database, no network),
which enabled targets have gone `stale_days` without a successful offload and why, so the
readiness stepper can show it and the offload tick can tell the desk once.

The clock for a target is its last successful offload, or when the tick first saw it enabled
(`tracked_since`) if it has never succeeded. A target with neither is not judged yet."""
from __future__ import annotations

from datetime import datetime
from typing import Any

DEFAULT_STALE_DAYS = 7


def _parse(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _date(value: object) -> str:
    when = _parse(value)
    return when.date().isoformat() if when else "an unknown date"


def stale_reason(receipt: dict[str, Any]) -> str:
    """Why this target has not been backed up to, in words an operator can act on."""
    skip = receipt.get("last_skip_reason")
    if skip:
        since = _date(receipt.get("skipped_since") or receipt.get("last_skip_at"))
        text = f"{skip} since {since}"
        if "reachable only over" in str(skip):
            return f"{text}: bring the laptop home"
        if "mountpoint absent" in str(skip):
            return f"{text}: dock the drive"
        return text
    if receipt.get("last_error"):
        return f"the last attempt failed: {receipt['last_error']}"
    if receipt.get("last_successful_offload"):
        return "no attempt has run since the last successful offload"
    return "no offload has run yet"


def stale_offload_targets(
    targets: list[dict[str, Any]], receipts: dict[str, dict[str, Any]], *,
    now: datetime, stale_days: int = DEFAULT_STALE_DAYS,
) -> list[dict[str, Any]]:
    """Every ENABLED target whose clock is `stale_days` or more old. Each entry carries the
    `name`, the `clock` (the ISO time the days are counted from), `days` and a `reason`."""
    stale: list[dict[str, Any]] = []
    for target in targets:
        if not isinstance(target, dict) or not target.get("enabled"):
            continue
        name = str(target.get("name", "<unnamed>"))
        receipt = receipts.get(name) or {}
        clock = receipt.get("last_successful_offload") or receipt.get("tracked_since")
        started = _parse(clock)
        if started is None:
            continue
        if started.tzinfo is None:
            started = started.replace(tzinfo=now.tzinfo)
        days = (now - started).days
        if days >= stale_days:
            stale.append({"name": name, "clock": str(clock), "days": days,
                          "reason": stale_reason(receipt)})
    return stale


def stale_sentence(entry: dict[str, Any], stale_days: int) -> str:
    """One line for the readiness step and the desk."""
    return (f"{entry['name']}: no successful offload for {entry['days']} days "
            f"(limit {stale_days}): {entry['reason']}")
