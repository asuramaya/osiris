"""The fleet trigger-hook — the mailbox's alarm clock, bounded against recursion.

The mailbox is pull-based; this lets the WORKER wake an agent when a project has unread mail. The
named danger is the A↔B ping-pong. These tests prove the safety story: OFF by default, and a
per-project RATE CAP that halts a loop even under persistent unread mail.
"""
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from src.actions.core import Actions
from src.orchestrator.mailbox import send_message
from src.orchestrator.trigger import should_wake, trigger_mail_tick

NOW = datetime(2026, 7, 6, tzinfo=UTC)


def _settings(*, enabled: bool, rate_cap: int = 5, window: int = 3600) -> SimpleNamespace:
    return SimpleNamespace(osiris_trigger_enabled=enabled,
                           osiris_trigger_rate_cap=rate_cap, osiris_trigger_window_secs=window)


def test_should_wake_is_off_by_default_and_rate_capped() -> None:
    assert should_wake(enabled=False, recent_wakes=0, rate_cap=5) == "disabled"    # kill switch
    assert should_wake(enabled=True, recent_wakes=5, rate_cap=5) == "rate-capped"  # the bound
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5) is None           # → WAKE


async def _agent_with_mail(actions: Actions) -> None:
    a = await actions.create_or_find_object("Agent", "agent:demo", "session")
    await actions.assert_property(a, "project", "demo", "session", NOW, 0.9)
    await actions.assert_property(a, "cwd", "/repo/demo", "session", NOW, 0.9)
    await send_message(actions.pool, from_agent="agent:other", from_project="other",
                       to_project="demo", body="please look at X")


async def test_trigger_is_dormant_when_disabled(actions: Actions) -> None:
    await _agent_with_mail(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str) -> None:
        spawned.append(repo)

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=False), spawn=_spawn)
    assert spawned == [] and rep["woke"] == 0  # OFF by default — nothing woken


async def test_rate_cap_bounds_the_recursive_pingpong(actions: Actions) -> None:
    """Even with mail that never clears (a stuck loop), the wakes stop at the per-project cap."""
    await _agent_with_mail(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str) -> None:
        spawned.append(repo)

    st = _settings(enabled=True, rate_cap=2)
    for _ in range(5):  # the unread message persists across ticks (the agent hasn't read it)
        await trigger_mail_tick(actions, settings=st, spawn=_spawn)
    assert len(spawned) == 2  # bounded at the rate cap — the ping-pong halts
    assert "/repo/demo" in spawned[0]  # woke in the recipient's repo
    # the wakes are recorded — the visible, auditable chain
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE to_project='demo'") == 2
