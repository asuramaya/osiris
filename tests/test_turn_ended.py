"""A DM that arrives after the addressee's turn ENDED is not 'mid-turn' (mail 15848).

The live specimen: a seat's last message was at 16:57:52, the harness wrote the
`stop_hook_summary` and `turn_duration` lines at 16:57:56, and a graded ask arrived at
16:58:41. The transcript's newest turn line was 49 s old, inside `osiris_dm_active_secs`,
so dispatch answered 'mid-turn' ("its own turn's end surfaces the DM") for a seat whose turn
had already ended and whose Stop hook had already run. Nothing ever surfaced the ask; it sat
89 minutes until an unrelated re-send woke the seat."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator import trigger as trigger_module
from src.orchestrator.trigger import dispatch_dm

from tests.test_trigger import (
    FULL_SID,
    _dm_to_owner,
    _no_job,
    _no_windows,
    _settings,
    _stale_resumable_owner,
)


def _line(kind: str, at: datetime, **extra: Any) -> str:
    return json.dumps({"type": kind, "timestamp": at.isoformat(), **extra}) + "\n"


def _ended_turn(at: datetime) -> str:
    """What a finished turn leaves: the last message, the Stop hook's summary, the marker."""
    return (_line("assistant", at)
            + _line("system", at + timedelta(seconds=4), subtype="stop_hook_summary")
            + _line("system", at + timedelta(seconds=4), subtype="turn_duration"))


def test_an_ended_turn_is_not_in_flight_however_recent(tmp_path: Path) -> None:
    sid = "abcd1234-0000-4000-8000-000000000000"
    root = tmp_path / "projects"
    proj = root / "-repo-demo"
    proj.mkdir(parents=True)
    t = proj / f"{sid}.jsonl"
    now = datetime.now(UTC)

    t.write_text(_line("user", now - timedelta(seconds=60)) + _line("assistant", now))
    assert trigger_module._turn_fresh_sync(root, sid, 120) is True  # no marker: moving

    t.write_text(_ended_turn(now - timedelta(seconds=49)))
    assert trigger_module._turn_fresh_sync(root, sid, 120) is False  # marker: over

    # housekeeping after the marker (an away summary) does not reopen the turn
    with t.open("a") as fh:
        fh.write(_line("system", now, subtype="away_summary"))
    assert trigger_module._turn_fresh_sync(root, sid, 120) is False

    # a new prompt after the marker starts a new turn, which IS in flight
    with t.open("a") as fh:
        fh.write(_line("user", now))
    assert trigger_module._turn_fresh_sync(root, sid, 120) is True

    # the age is still reported for an ended turn (the idle-holder checks read it)
    t.write_text(_ended_turn(now - timedelta(seconds=49)))
    age = trigger_module._last_turn_age_sync(root, sid)
    assert age is not None and 40 < age < 70


async def test_a_dm_after_the_turn_ended_is_not_told_mid_turn(
    actions: Actions, tmp_path: Path,
) -> None:
    sense = await _stale_resumable_owner(actions, tmp_path)
    transcript = sense / "-repo-demo" / f"{FULL_SID}.jsonl"
    msg_id = await _dm_to_owner(actions)
    transcript.write_text(_ended_turn(datetime.now(UTC) - timedelta(seconds=49)))
    calls: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(repo)

    async def _nudge(*a: Any, **kw: Any) -> None:
        calls.append("nudge")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, jobs=_no_job, nudge=_nudge)
    assert d["mode"] != "mid-turn"
    assert calls, "an idle addressee is woken, not left to a turn end that already happened"


async def test_a_dm_to_a_seat_still_working_keeps_mid_turn(
    actions: Actions, tmp_path: Path,
) -> None:
    sense = await _stale_resumable_owner(actions, tmp_path)
    transcript = sense / "-repo-demo" / f"{FULL_SID}.jsonl"
    msg_id = await _dm_to_owner(actions)
    now = datetime.now(UTC)
    transcript.write_text(_ended_turn(now - timedelta(seconds=90))
                          + _line("user", now - timedelta(seconds=49))
                          + _line("assistant", now))

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a seat with a turn in flight is neither woken nor forked")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "mid-turn"
