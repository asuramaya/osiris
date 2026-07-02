"""The heartbeat DIGEST — the dead-man's-switch computed on READ.

`_fn_pulse` (the `pulse-digest` lens) used to `LIMIT 1`: it read only the single most recent
pulse, so a quiet last tick reported "no pulse yet" even with a full log, and a heartbeat that
DIED with its daemon looked identical to one that never ran. These tests pin the fixed
semantics: findings aggregated across a real window, and a liveness verdict re-derived on every
look (so the lens outlives the loop — that is the dead-man's-switch)."""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest_asyncio
from src.actions.core import Actions
from src.orchestrator.compositions import run_spec

NOW = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)


@pytest_asyncio.fixture(autouse=True)
async def _clean_pulses(actions: Actions) -> AsyncIterator[None]:
    # dev_pulses is operational telemetry, not in conftest's per-test TRUNCATE set — clear it
    # both before AND after each digest test, so this module starts empty and never leaks pulse
    # rows into a sibling module (e.g. test_pulse.py, which relies on an empty log for baseline).
    await actions.pool.execute("TRUNCATE dev_pulses RESTART IDENTITY")
    yield
    await actions.pool.execute("TRUNCATE dev_pulses RESTART IDENTITY")


async def _pulse(actions: Actions, ran_at: datetime, findings: list[str],
                 synced: list[str] | None = None) -> None:
    await actions.pool.execute(
        "INSERT INTO dev_pulses (ran_at, synced, snapshot, findings) VALUES ($1,$2,$3,$4)",
        ran_at, synced or [], {}, findings)


async def _digest(actions: Actions, **args: Any) -> list[dict[str, str]]:
    res = await run_spec(actions.pool, {"op": "function", "name": "pulse", "args": args}, None)
    assert res["kind"] == "data"
    return list(next(iter(res["items"].values())))


async def test_digest_aggregates_findings_across_the_window(actions: Actions) -> None:
    """The core fix: findings are aggregated across a WINDOW, not just the last tick. A finding
    4 pulses back still surfaces (the old LIMIT-1 would have shown only the empty newest tick and
    lied 'no pulse yet')."""
    await _pulse(actions, NOW - timedelta(hours=4), ["3 new commits in kast"])   # 4 pulses back
    await _pulse(actions, NOW - timedelta(hours=3), [])
    await _pulse(actions, NOW - timedelta(hours=2), [])
    await _pulse(actions, NOW - timedelta(hours=1), [])
    await _pulse(actions, NOW, [], synced=["osiris"])              # newest, fresh, quiet

    feed = await _digest(actions, now=NOW)
    assert "heartbeat alive" in feed[0]["finding"]                 # status row leads; loop is fresh
    assert any("3 new commits in kast" in r["finding"] for r in feed)   # the 4-back finding shows


async def test_pulsed_but_quiet_is_not_no_pulse_yet(actions: Actions) -> None:
    """Pulses ran but nothing changed → a 'quiet since <t>' row, NEVER the 'no pulse yet'
    instruction (the specific lie the old lens told with a non-empty but findingless log)."""
    await _pulse(actions, NOW - timedelta(minutes=20), [])
    await _pulse(actions, NOW - timedelta(minutes=10), [])
    await _pulse(actions, NOW, [])                                  # fresh, all quiet

    feed = await _digest(actions, now=NOW)
    text = " | ".join(r["finding"] for r in feed)
    assert "no pulse yet" not in text                              # the instruction is gone
    assert "heartbeat alive" in feed[0]["finding"]
    assert any("quiet" in r["finding"] for r in feed[1:])          # says quiet-since instead


async def test_stale_heartbeat_leads_with_the_dead_since_row(actions: Actions) -> None:
    """The dead-man's-switch: the newest pulse is 2h old (> ~45 min), so the lens — computed on
    READ — leads with a DEAD-since-<t> row even though the daemon is gone. The findings from
    before it died still show (the window is anchored at the last pulse)."""
    last = NOW - timedelta(hours=2)
    await _pulse(actions, last - timedelta(hours=1), ["1 new decision recorded"])
    await _pulse(actions, last, [])

    feed = await _digest(actions, now=NOW)
    assert "DEAD" in feed[0]["finding"]
    assert str(last)[:19] in feed[0]["finding"]                    # dead SINCE that time
    assert any("1 new decision recorded" in r["finding"] for r in feed)   # pre-death finding kept


async def test_no_pulses_ever_keeps_the_run_instruction(actions: Actions) -> None:
    """An empty log is the ONLY case that keeps the how-to-run row (distinct from quiet-pulsed)."""
    feed = await _digest(actions, now=NOW)
    assert len(feed) == 1
    assert "no pulse yet" in feed[0]["finding"]
    assert "python -m src.orchestrator.pulse" in feed[0]["finding"]


async def test_last_override_bounds_the_window(actions: Actions) -> None:
    """`args.last` overrides the time window to exactly the last N pulses (time-agnostic)."""
    for i in range(6):
        await _pulse(actions, NOW - timedelta(hours=6 - i), [f"finding {i}"])

    feed = await _digest(actions, now=NOW, last=2)
    text = " | ".join(r["finding"] for r in feed)
    assert "finding 5" in text and "finding 4" in text            # the two newest pulses
    assert "finding 3" not in text                                # bounded by last=2
