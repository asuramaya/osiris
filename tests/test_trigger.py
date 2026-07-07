"""The fleet trigger-hook — the mailbox's alarm clock, bounded against recursion.

The mailbox is pull-based; this lets the WORKER wake an agent when a project has deliverable
mail. The named danger is the A↔B ping-pong. These tests prove the safety story: OFF by
default, a per-project RATE CAP that halts a loop even under persistent unread mail, no wake
while a live lease says the mail is already being processed, and the operator's desk is never
woken (it has no repo — the human reads it, membrane #6's upward lane).
"""
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from src.actions.core import Actions
from src.orchestrator.mailbox import OPERATOR_ADDR, read_inbox, send_message
from src.orchestrator.trigger import _WAKE_PROMPT, should_wake, trigger_mail_tick, wake_status

NOW = datetime(2026, 7, 6, tzinfo=UTC)


def _settings(*, enabled: bool, rate_cap: int = 5, window: int = 3600,
              lease: int = 900) -> SimpleNamespace:
    return SimpleNamespace(osiris_trigger_enabled=enabled, osiris_trigger_rate_cap=rate_cap,
                           osiris_trigger_window_secs=window, osiris_mail_lease_secs=lease)


def test_should_wake_is_off_by_default_and_rate_capped() -> None:
    assert should_wake(enabled=False, recent_wakes=0, rate_cap=5) == "disabled"    # kill switch
    assert should_wake(enabled=True, recent_wakes=5, rate_cap=5) == "rate-capped"  # the bound
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5) is None           # → WAKE


def test_wake_prompt_carries_the_upward_duty() -> None:
    # the woken agent's contract: settle what it handles, and REPORT UP when the loop closes —
    # the operator must see it (membrane #6); acks-only replies stay forbidden (ping-pong).
    p = _WAKE_PROMPT.format(repo="/repo/demo")
    assert "send(reply_to=" in p and "ack" in p          # the settle ritual
    assert "send(to='operator')" in p and "record_decision" in p  # the report-up duty
    assert "never an acknowledgement-only" in p


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

    st = _settings(enabled=True, rate_cap=2, lease=0)  # lease=0: the mail stays deliverable
    for _ in range(5):  # the unread message persists across ticks (the agent hasn't read it)
        await trigger_mail_tick(actions, settings=st, spawn=_spawn)
    assert len(spawned) == 2  # bounded at the rate cap — the ping-pong halts
    assert "/repo/demo" in spawned[0]  # woke in the recipient's repo
    # the wakes are recorded — the visible, auditable chain
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE to_project='demo'") == 2


async def test_leased_mail_does_not_rewake(actions: Actions) -> None:
    """Mail under a live lease is being processed RIGHT NOW — re-waking would double-spawn.
    Lease expiry re-arms the wake (the processing died; someone should look again)."""
    await _agent_with_mail(actions)
    await read_inbox(actions.pool, "demo")  # the woken agent leased its inbox
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str) -> None:
        spawned.append(repo)

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn)
    assert spawned == [] and rep["woke"] == 0  # lease live → no double-spawn
    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True, lease=0),
                                  spawn=_spawn)
    assert rep["woke"] == 1  # lease expired, still unsettled → re-armed


async def test_operator_desk_is_never_woken(actions: Actions) -> None:
    await send_message(actions.pool, from_agent="agent:x", from_project="demo",
                       to_project=OPERATOR_ADDR, body="finding for the human")
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str) -> None:
        spawned.append(repo)

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn)
    assert spawned == [] and rep["woke"] == 0 and rep["skipped"] == 0  # not even a candidate


async def test_wake_status_is_the_sender_visible_signal(actions: Actions) -> None:
    p = actions.pool
    assert await wake_status(p, "demo", _settings(enabled=False)) == "disabled"
    assert await wake_status(p, "demo", _settings(enabled=True)) == "armed"
    assert "never woken" in await wake_status(p, OPERATOR_ADDR, _settings(enabled=True))
    await p.execute("INSERT INTO agent_wakes (to_project, from_agent, message_id) "
                    "VALUES ('demo','agent:x',NULL)")
    assert await wake_status(p, "demo", _settings(enabled=True, rate_cap=1)) == "rate-capped"