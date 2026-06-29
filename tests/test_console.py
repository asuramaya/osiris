"""The shared console cursor — the substrate of real-time Claude↔front sync.

Both Claude (MCP) and the browser write here; `rev` + `updated_by` are what let each side
follow the other without echoing its own move. These prove the write semantics the UI relies
on: a partial move keeps the rest, every write bumps rev monotonically, and the writer learns
who-moved-last.
"""
from __future__ import annotations

import pytest
from src.actions.core import Actions
from src.orchestrator.console import get_console, set_console


async def test_set_console_is_partial_and_bumps_rev(actions: Actions) -> None:
    p = actions.pool
    s0 = await get_console(p)
    # claude focuses an object
    oid = await actions.create_or_find_object("Thread", "thread:x", "git-memory")
    s1 = await set_console(p, by="claude", focused_object_id=oid)
    assert s1["updated_by"] == "claude"
    assert s1["focused_object_id"] == str(oid)
    assert s1["rev"] > s0["rev"]                         # monotonic
    # a later move sets the composition WITHOUT clearing the focus (partial update)
    s2 = await set_console(p, by="human", composition="briefing")
    assert s2["composition"] == "briefing"
    assert s2["focused_object_id"] == str(oid)           # untouched
    assert s2["updated_by"] == "human"
    assert s2["rev"] > s1["rev"]
    # get reflects the last write
    assert await get_console(p) == s2


async def test_set_console_rejects_unknown_field(actions: Actions) -> None:
    with pytest.raises(ValueError, match="unknown console field"):
        await set_console(actions.pool, by="claude", secret="oops")


async def test_console_survives_a_wiped_singleton(actions: Actions) -> None:
    """The actions fixture TRUNCATE…CASCADE wipes the singleton (console_state references
    objects); set_console upserts, so the cursor still works — get returns a coherent default
    until the first write."""
    p = actions.pool
    base = await get_console(p)
    assert base["rev"] == 0 and base["composition"] is None      # coherent default
    s = await set_console(p, by="claude", composition="who-is-this")
    assert s["composition"] == "who-is-this"
