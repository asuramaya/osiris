"""Waiting helpers for tests that depend on something happening, so they wait for the
condition instead of racing a fixed real-time window.

A test that sleeps 0.15 s and then asserts two log records exist passes on an idle machine
and fails when sixteen workers compete for the CPUs: the event loop is starved, fewer ticks
land in the window, and the assertion sees one record. The behaviour under test did not
change; the window did. Waiting for the condition under a generous deadline keeps the
intent (the event DOES happen) and drops the race. The deadline is a ceiling for a real
hang, never the expected time, so a healthy run returns as soon as the condition holds.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

# generous on purpose: a hang costs one test this long, a loaded machine costs it nothing
DEFAULT_DEADLINE_S = 30.0


async def wait_until(
    condition: Callable[[], bool], *, deadline_s: float = DEFAULT_DEADLINE_S,
    interval_s: float = 0.01,
) -> bool:
    """Poll `condition` until it is true or `deadline_s` passes. Returns whether it held,
    so the caller's own assert states what was expected."""
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        if condition():
            return True
        await asyncio.sleep(interval_s)
    return bool(condition())


def wait_until_sync(
    condition: Callable[[], bool], *, deadline_s: float = DEFAULT_DEADLINE_S,
    interval_s: float = 0.01,
) -> bool:
    """The same for a plain (non-async) test."""
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        if condition():
            return True
        time.sleep(interval_s)
    return bool(condition())
