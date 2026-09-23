"""The fleet trigger-hook: the mailbox's alarm clock, bounded against recursion.

The mailbox is pull-based; this lets the WORKER wake an agent when a project has deliverable
mail. The named danger is the A↔B ping-pong. These tests prove the safety story: OFF by
default, a per-project RATE CAP that halts a loop even under persistent unread mail, no wake
while a live lease says the mail is already being processed, and the operator's desk is never
woken (it has no repo: the human reads it directly, via membrane #6's upward lane).
"""
from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import trigger as trigger_module
from src.orchestrator.mailbox import OPERATOR_ADDR, read_inbox, send_message
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import bind_holder, ensure_seat, set_seat_attended
from src.orchestrator.trigger import (
    _WAKE_PROMPT,
    _marker_landed_sync,
    _stuck_wake_in_flight_asks,
    _wake_marker,
    dispatch_broadcast,
    dispatch_dm,
    should_wake,
    trigger_mail_tick,
    wake_status,
    wake_worker,
)

NOW = datetime(2026, 7, 6, tzinfo=UTC)


def _settings(*, enabled: bool, rate_cap: int = 5, window: int = 3600,
              lease: int = 900, grace: int = 0, live: int = 900,
              ceiling: int = 8_000_000, min_tail_bytes: int = 0, sense: str = "",
              wake_model: str = "", attempts: int = 0,
              daily_usd: float = -1.0, projects: str = "",
              poke_only: bool = False, dm_resume: bool = True,
              dm_active: int = 120, seat_cap: int = 0,
              dm_resume_model: str = "",
              extract_provider: str = "claude-cli", api_key: str = "",
              wake_enabled: bool = True) -> SimpleNamespace:
    # grace defaults to 0 (disabled) so the rate-cap / lease tests exercise those bounds in
    # isolation; the wake-grace tests set it explicitly. sense="" → resume resolution looks at
    # ~/.claude/projects (no anchored transcript for the test ids there → mint), so the legacy
    # mint-path tests stay exactly as they were.
    # daily_usd defaults to -1 (NO CEILING) so these tests keep exercising the DISPATCH decisions
    # rate caps, leases, alternation, in isolation. The ceiling has its own suite
    # (test_ceiling.py) and its own trigger test below; a spend gate silently swallowing every
    # other test's wake would hide the very behaviour they exist to pin.
    # seat_cap defaults to 0 (unbraked) for the same isolation reason; the brake has its own test.
    return SimpleNamespace(osiris_trigger_enabled=enabled, osiris_trigger_rate_cap=rate_cap,
                           osiris_trigger_window_secs=window, osiris_mail_lease_secs=lease,
                           osiris_trigger_grace_secs=grace, osiris_owner_live_secs=live,
                           osiris_resume_ceiling_bytes=ceiling,
                           osiris_resume_min_tail_bytes=min_tail_bytes,
                           osiris_sense_sessions=sense,
                           osiris_wake_model=wake_model,
                           osiris_wake_hourly_budget=0,  # unmetered: economics has its own tests
                           osiris_wake_message_attempts=attempts,
                           osiris_wake_allowed_tools="mcp__osiris",
                           osiris_daily_usd=daily_usd,
                           osiris_trigger_projects=projects,
                           osiris_poke_min_idle_secs=600,
                           osiris_trigger_poke_only=poke_only,
                           osiris_dm_resume=dm_resume,
                           osiris_dm_active_secs=dm_active,
                           osiris_seat_wake_hourly_cap=seat_cap,
                           osiris_dm_resume_model=dm_resume_model,
                           # spend_is_metered(st) reads these: default is the local Claude CLI
                           # (a subscription) → the dollar ceiling is INERT, which is why the
                           # daily_usd=-1 dispatch tests never trip it. The ceiling test below
                           # flips to the keyed API backend, the only world where it bites.
                           osiris_extract_provider=extract_provider,
                           osiris_claude_binary="claude", anthropic_api_key=api_key,
                           # wake defaults ON in tests so the delivery/authorization suite
                           # exercises the real send path; production ships it FROZEN (False).
                           osiris_wake_enabled=wake_enabled)


async def _no_windows() -> list[dict[str, Any]]:
    """The poke lane's OFF position for tests that exercise the pre-poke sequence, a dark
    manager (the production default until windows exist) is an empty roster."""
    return []


@pytest.fixture(autouse=True)
def _dark_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    """HERMETIC by default: no test in this module may consult a live daemon socket on the
    dev box: the roster default resolves late, so darkening the module attribute covers
    every tick that doesn't inject its own windows. The CLAUDE harness daemon is darkened
    the same way (its real socket exists on the dev box and would answer)."""
    monkeypatch.setattr(trigger_module, "_manager_windows", _no_windows)

    async def _no_job(ids: set) -> None:
        return None

    from src.ingest.harness import claude_daemon
    monkeypatch.setattr(claude_daemon, "job_for", _no_job)


def test_should_wake_is_off_by_default_and_rate_capped() -> None:
    assert should_wake(enabled=False, recent_wakes=0, rate_cap=5) == "disabled"    # kill switch
    assert should_wake(enabled=True, recent_wakes=5, rate_cap=5) == "rate-capped"  # the bound
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5) is None           # → WAKE


def test_should_wake_reads_the_hourly_budget(_: None = None) -> None:
    """Wake economics: past the soft ceiling only URGENT mail wakes;
    at the hard ceiling nothing does; budget 0 = unmetered (the old behavior)."""
    # unmetered: the budget params change nothing
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5,
                       hourly_wakes=999, hourly_budget=0) is None
    # under the soft ceiling: wakes flow
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5,
                       hourly_wakes=10, hourly_budget=30) is None
    # past the soft ceiling (80%): non-urgent defers, urgent rides through
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5,
                       hourly_wakes=24, hourly_budget=30) == "budget-deferred"
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5,
                       hourly_wakes=24, hourly_budget=30, urgent=True) is None
    # the hard ceiling blocks even urgent mail until the window slides
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5,
                       hourly_wakes=30, hourly_budget=30, urgent=True) == "budget-exhausted"
    # ranking: the project rate cap is the harder, more specific signal
    assert should_wake(enabled=True, recent_wakes=5, rate_cap=5,
                       hourly_wakes=30, hourly_budget=30) == "rate-capped"


def test_should_wake_grace_is_distinct_and_ranked_below_the_cap() -> None:
    # recently woken but under the cap → 'wake-grace' (processing), a DISTINCT skip from the bound
    assert should_wake(enabled=True, recent_wakes=1, rate_cap=5, within_grace=True) == "wake-grace"
    # the cap outranks grace: the harder safety signal wins when both apply
    assert should_wake(enabled=True, recent_wakes=5, rate_cap=5, within_grace=True) == "rate-capped"
    # neither → WAKE
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5, within_grace=False) is None


def test_wake_prompt_carries_the_upward_duty() -> None:
    # the woken agent's contract: settle what it handles, and REPORT UP when the loop closes,
    # the operator must see it (membrane #6); acks-only replies stay forbidden (ping-pong).
    p = _WAKE_PROMPT.format(repo="/repo/demo", job_dir="/tmp/osiris-wakes/jobs/wake-demo")
    assert "send(reply_to=" in p and "ack" in p          # the settle ritual
    assert "send(to='operator'" in p and "record_decision" in p  # the report-up duty
    assert "desk=" in p and "'fyi'" in p                 # the desk bands ride the brief
    assert "never an acknowledgement-only" in p


async def _agent_with_mail(actions: Actions) -> None:
    a = await actions.create_or_find_object("Agent", "agent:demo", "session")
    await actions.assert_property(a, "project", "demo", "session", NOW, 0.9)
    await actions.assert_property(a, "cwd", "/repo/demo", "session", NOW, 0.9)
    # send_message now refuses a to_project nobody has ever mounted under (shape 3 of
    # #117): alive=False so this registers 'demo' as existing
    # without stamping a live last_seen, which would trip _owner_live() and make the
    # trigger deliver instead of mint/resume/poke (the file's own idiom, see the
    # alive=False seeds elsewhere in this file, e.g. "pulseless: not owner_live").
    await save_mount(actions.pool, job_dir="/test/seed/demo", agent_id="agent:seed-demo",
                     project="demo", cwd="/test", model=None, session_key=None, alive=False)
    await send_message(actions.pool, from_agent="agent:other", from_project="other",
                       to_project="demo", body="please look at X")


async def test_trigger_is_dormant_when_disabled(actions: Actions) -> None:
    await _agent_with_mail(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=False), spawn=_spawn)
    assert spawned == [] and rep["woke"] == 0  # OFF by default, nothing woken


async def test_a_scoped_rearm_touches_ONLY_its_named_subjects(actions: Actions) -> None:
    """THE RE-ARM SCOPE (2026-07-14, the pokex pile-drain experiment): every handoff since
    XXVII said 'turn it on for ONE project, watched', and until tonight that was a promise,
    not a setting. Armed with an allowlist, unread mail OUTSIDE the scope is scoped_out, never
    woken; the named project wakes normally. An empty allowlist keeps the old behavior."""
    await _agent_with_mail(actions)  # project 'demo' has unread mail
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    # armed, but the re-arm names a DIFFERENT project: demo's mail waits, no wake
    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, projects="pokex"), spawn=_spawn)
    assert spawned == [] and rep["woke"] == 0 and rep["scoped_out"] == 1
    # the same tick with demo IN scope wakes it
    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, projects="pokex, demo"), spawn=_spawn)
    assert len(spawned) == 1 and rep["woke"] == 1 and rep["scoped_out"] == 0
    # ...and the sender-visible signal tells the scoped truth, never a false 'armed'
    st = _settings(enabled=True, projects="pokex")
    assert "scoped-out" in await wake_status(actions.pool, "demo", st)  # type: ignore[arg-type]
    assert await wake_status(actions.pool, "pokex", st) == "armed"  # type: ignore[arg-type]


async def test_rate_cap_bounds_the_recursive_pingpong(actions: Actions) -> None:
    """Even with mail that never clears (a stuck loop), the wakes stop at the per-project cap."""
    await _agent_with_mail(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    st = _settings(enabled=True, rate_cap=2, lease=0)  # lease=0: the mail stays deliverable
    for _ in range(5):  # the unread message persists across ticks (the agent hasn't read it)
        await trigger_mail_tick(actions, settings=st, spawn=_spawn)
    assert len(spawned) == 2  # bounded at the rate cap, the ping-pong halts
    assert "/repo/demo" in spawned[0]  # woke in the recipient's repo
    # the wakes are recorded: the visible, auditable chain
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE to_project='demo'") == 2


async def test_leased_mail_does_not_rewake(actions: Actions) -> None:
    """Mail under a live lease is being processed RIGHT NOW: re-waking would double-spawn.
    Lease expiry re-arms the wake (the processing died; someone should look again)."""
    await _agent_with_mail(actions)
    await read_inbox(actions.pool, "demo", reader_agent="agent:demo")  # the woken agent leased
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
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

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
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
    # RECEIPT HONESTY: a skip reason now names its own retry cadence,
    # 'the sweep retries' told a sender nothing about WHEN; '~60s' is measured,
    # not a guess
    rate_capped = await wake_status(p, "demo", _settings(enabled=True, rate_cap=1))
    assert rate_capped.startswith("rate-capped") and "~60s" in rate_capped
    # a recent wake under the cap → 'wake-grace', so a sender sees 'processing' not 'off'/'capped'
    grace_status = await wake_status(
        p, "demo", _settings(enabled=True, rate_cap=5, grace=300))
    assert grace_status.startswith("wake-grace") and "~60s" in grace_status


async def test_wake_status_poke_only_names_the_real_limit_instead_of_a_blanket_armed(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE LYING-RECEIPT FIX: a broadcast has NO
    daemon-reply lane (DM-only), under poke-only mode with no open manager window for the
    project, dispatch is GUARANTEED to terminate at 'held' (no resume, no mint, ever), so
    the old blanket 'armed' was a lie a sender had no way to see through. With a window
    present, poke really would succeed, so 'armed' stays honest."""
    p = actions.pool
    st = _settings(enabled=True, poke_only=True)
    # _dark_manager (autouse) darkens _manager_windows to [] for this whole module: the
    # no-window case is the default here, nothing extra to arrange
    status = await wake_status(p, "demo", st)
    assert "poke-only" in status and "will NOT be pushed" in status
    assert "no daemon-reply lane" in status.lower()
    # a real manager window for THIS project flips the verdict back to armed: poke
    # genuinely would deliver it. wake_status's window check reads agent_mounts (the
    # durable mount row), not the graph: mounts.save_mount is the real write path
    # every other test in this module uses for the same table.
    from src.orchestrator import mounts
    await mounts.save_mount(actions.pool, job_dir="/tmp/jobs/windowed-sess",
                            agent_id="agent:windowed", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)

    async def _windowed() -> list[dict[str, Any]]:
        return [{"name": "demo-window", "alive": True, "job_dir": "/tmp/jobs/windowed-sess"}]

    monkeypatch.setattr(trigger_module, "_manager_windows", _windowed)
    assert await wake_status(p, "demo", st) == "armed"


async def test_wake_grace_prevents_the_double_wake(actions: Actions) -> None:
    """The fix: the cron ticks (60s) faster than a woken agent spawns,
    mounts, and leases its inbox (~100s+), so the next tick re-wakes the SAME still-deliverable
    message. Within the grace window that re-tick is skipped: one message, one wake."""
    await _agent_with_mail(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    st = _settings(enabled=True, grace=300, lease=0)  # lease=0: mail stays deliverable (unread)
    await trigger_mail_tick(actions, settings=st, spawn=_spawn)  # first tick wakes
    await trigger_mail_tick(actions, settings=st, spawn=_spawn)  # ~100s later: within grace → skip
    assert len(spawned) == 1  # NOT double-woken on one message
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE to_project='demo'") == 1  # one ledger entry


async def test_wake_grace_expiry_rearms(actions: Actions) -> None:
    """Grace is a window, not a latch: once it expires and the mail is STILL deliverable (the
    woken agent died before reading), the wake re-arms: the mail is not stranded."""
    await _agent_with_mail(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    st = _settings(enabled=True, grace=300, lease=0)
    await trigger_mail_tick(actions, settings=st, spawn=_spawn)  # wakes
    await trigger_mail_tick(actions, settings=st, spawn=_spawn)  # within grace → skip
    assert len(spawned) == 1
    # age the wake past the grace window (deterministic, no sleep) → grace lapses
    await actions.pool.execute(
        "UPDATE agent_wakes SET woke_at = now() - make_interval(secs => 400)")
    await trigger_mail_tick(actions, settings=st, spawn=_spawn)  # grace expired → re-armed
    assert len(spawned) == 2


async def test_spawned_wake_carries_a_durable_job_dir_anchor(actions: Actions) -> None:
    """Part 1: a triggered `claude -p` gets no CLAUDE_JOB_DIR from any harness,
    so the woken agent used to mount by guessing its identity off a co-tenant's transcript. The
    trigger synthesizes a durable anchor with a 'jobs/wake-<x>' shape _job_id parses.

    AMENDED 2026-07-12: <x> was the WAKE ROW ID, so every wake became a new agent:wake-<id>, 463
    mints, 463 strangers on the roster, 48 of them in a project the operator had not opened in two
    days. A wake is not a new MIND; it is the same errand run again. It is now keyed on the
    PROJECT: one ghost per house, re-worn, and instantly recognisable as a machine.

    The anchor ALSO now rides in the PROMPT as a literal path. It used to be the text
    `$CLAUDE_JOB_DIR`, which a woken agent (tools: mcp__osiris only, no shell) cannot expand, so
    the mount hook refused the '$' and derived a fresh session-based identity every single time,
    and this anchor was never used ONCE in 463 mints."""
    from src.ingest.sessions import _job_id

    await _agent_with_mail(actions)
    captured: list[tuple[str, str]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        captured.append((kw["job_dir"], prompt))

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn)
    assert rep["woke"] == 1 and len(captured) == 1
    jd, prompt = captured[0]
    assert jd.endswith("jobs/wake-demo")           # the token is the PROJECT, not the row id
    assert _job_id(jd) == "wake-demo"              # the parser resolves it to a stable handle
    assert Path(jd).is_dir()                       # a REAL created dir, not just a string
    # and the agent is TOLD the literal path: it has no shell to expand a variable with
    assert f'job_dir="{jd}"' in prompt and "$CLAUDE_JOB_DIR" not in prompt


async def test_spawn_claude_injects_claude_job_dir_into_child_env(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    """_spawn_claude passes the synthesized job_dir as CLAUDE_JOB_DIR in the child's environment
    (inheriting ours), so the woken `claude -p` sees $CLAUDE_JOB_DIR and mounts with it."""
    from src.orchestrator import trigger

    captured: dict[str, Any] = {}

    class _Proc:
        pid = 4242

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        captured["args"] = args
        captured["env"] = kwargs.get("env")
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    # RECEIPTS must be patched on every direct _spawn_claude rehearsal: the spawner opens its
    # receipt before the (mocked) exec, so an unpatched run drops a 0-byte envelope in the
    # OPERATOR'S REAL HOME: that is where ~/.osiris/wake-receipts/wake-7.json came from, and a
    # priced test envelope there would bill phantom dollars into llm_usage via meter_receipts.
    monkeypatch.setattr(trigger, "RECEIPTS", tmp_path / "receipts")
    # this test's own concern is env/cmd construction, not the existence backstop
    # (that guard gets its own dedicated test below).
    monkeypatch.setattr(trigger, "_tree_exists", lambda p: True)
    await trigger._spawn_claude("/repo/demo", "wake up", job_dir="/tmp/x/jobs/wake-7")
    assert (tmp_path / "receipts" / "wake-7.json").exists()  # the rehearsal's receipt stayed home
    # by POSITION only where position is load-bearing: `claude -p` leads, and the PROMPT is last
    # (flags are appended between them). Pinning the prompt at index 2 broke the moment the wake
    # learned to keep its receipt, a test asserting an ARRANGEMENT rather than a REQUIREMENT.
    assert captured["args"][:2] == ("claude", "-p")
    assert captured["args"][-1] == "wake up"
    assert captured["env"]["CLAUDE_JOB_DIR"] == "/tmp/x/jobs/wake-7"
    assert "PATH" in captured["env"]  # inherited the parent environment, not a bare dict


async def test_spawn_claude_refuses_a_nonexistent_repo_without_raising(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    """THE BACKSTOP: a real crash from an `osiris launch` command reached
    `create_subprocess_exec`'s `cwd=` with no existence
    check at all, a raw, uncaught FileNotFoundError out of a fire-and-forget wake. Same
    degrade-don't-die discipline the unwritable-receipt path just above already follows:
    refuses as a logged no-op, subprocess never even attempted."""
    from src.orchestrator import trigger

    async def _unreachable(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("create_subprocess_exec must never be reached, the "
                             "existence backstop refuses first")

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _unreachable)
    ghost = str(tmp_path / "never-created")
    await trigger._spawn_claude(ghost, "wake up", job_dir="/tmp/x/jobs/wake-ghost")
    # fire-and-forget: no exception escapes, no subprocess spawned (proven by _unreachable)


async def test_spawn_claude_bg_refuses_a_nonexistent_repo_without_raising(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    """Same backstop as `_spawn_claude`'s own."""
    from src.orchestrator import trigger

    async def _unreachable(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("create_subprocess_exec must never be reached, the "
                             "existence backstop refuses first")

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _unreachable)
    ghost = str(tmp_path / "never-created-bg")
    await trigger._spawn_claude_bg(ghost, name="ghost")


async def test_spawn_claude_authorizes_the_graph_hands(monkeypatch: Any) -> None:
    """The wake permission storm: headless `claude -p` cannot answer a
    permission prompt, so in a repo with no stored approval every mcp__osiris__* call is
    silently denied: the wake dies blind and its mail redelivers forever. The spawner must
    pre-authorize the hands it asks for: --allowedTools rides in the command."""
    from src.orchestrator import trigger

    captured: dict[str, Any] = {}

    class _Proc:
        pid = 4242

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        captured["args"] = args
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    monkeypatch.setattr(trigger, "_tree_exists", lambda p: True)
    await trigger._spawn_claude("/repo/demo", "wake up", allowed_tools="mcp__osiris")
    assert ("--allowedTools", "mcp__osiris") in _pairs(captured["args"])
    # empty/None = the old behavior: rely on the repo's stored approvals, no flag at all
    await trigger._spawn_claude("/repo/demo", "wake up", allowed_tools=None)
    assert "--allowedTools" not in captured["args"]


def _pairs(args: tuple[Any, ...]) -> list[tuple[Any, Any]]:
    return [(args[i], args[i + 1]) for i in range(len(args) - 1)]


async def test_the_wake_KEEPS_ITS_RECEIPT(monkeypatch: Any, tmp_path: Path) -> None:
    """OSIRIS'S MOST EXPENSIVE ACT THREW AWAY THE VENDOR'S OWN PRICE FOR IT, 463 TIMES.

    A wake is a whole Claude session, with tools, in a repo, on the operator's card. It was
    spawned with `stdout=DEVNULL`, so the CLI's output envelope went in the bin. That envelope
    carries `total_cost_usd`: authoritative, free, volunteered on every call. It is EXACTLY where
    the miner's $40.49-to-the-cent comes from. Nobody ever read it, and so the single question the
    operator actually cares about (what does this cost per day?) had no answer for eight days.

        A HAND YOU CANNOT COST IS A HAND YOU CANNOT GOVERN.

    And it stays FIRE-AND-FORGET. The receipt goes to a FILE and nothing awaits the process:
    that is not laziness, it is B1's scar: an arq timeout that abandoned a live billing
    `claude -p` is how the worker wedged itself with ten 290MB children against a 2G cap.
    """
    from src.orchestrator import trigger

    captured: dict[str, Any] = {}

    class _Proc:
        pid = 4242

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        captured["args"] = args
        captured["stdout"] = kwargs.get("stdout")
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    monkeypatch.setattr(trigger, "RECEIPTS", tmp_path / "receipts")
    monkeypatch.setattr(trigger, "_tree_exists", lambda p: True)
    await trigger._spawn_claude("/repo/demo", "wake up", job_dir="/home/x/.claude/jobs/abcd1234")

    assert ("--output-format", "json") in _pairs(captured["args"]), "the CLI was not asked to price"
    assert captured["stdout"] is not trigger.asyncio.subprocess.DEVNULL, "the receipt was binned"
    assert (tmp_path / "receipts" / "abcd1234.json").exists()


async def test_every_wake_lane_passes_the_allowed_tools(actions: Actions) -> None:
    """The mint lane (and by the same call shape, both resume lanes) forwards the setting:
    a wake is born with its graph hands authorized, not hoping for a stored approval."""
    await _agent_with_mail(actions)
    captured: list[Any] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        captured.append(kw.get("allowed_tools"))

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn)
    assert rep["woke"] == 1 and captured == ["mcp__osiris"]

# --- the dispatch order: DELIVER → RESUME → MINT ---

FULL_SID = "abcd1234-0000-4000-8000-000000000000"


async def _stale_resumable_owner(actions: Actions, tmp_path: Path,
                                 transcript_bytes: int = 16, *,
                                 bind_seat: bool = True) -> Path:
    """An owner for project demo: a durable mount (made STALE so it isn't 'live') whose job_dir
    anchors a real transcript under the sense root; the transcript AGED too, because under
    the adapter's law mid-turn means the TRANSCRIPT is moving (a fresh mtime would read as a
    working mind and correctly refuse to resume). Returns the sense root.

    ALSO seats agent:abcd1234 with an office at the SAME cwd and asserts the graph's own
    `session`/`seat_generation` properties (task #178: dispatch_dm's resume selection now
    reads `_lineage_resume_candidate`, graph truth via `succession_chain`, never
    `agent_mounts` alone; a fixture that only wrote the mount row is invisible to it).
    `bind_seat=False` skips the seat-binding half only: a caller that ALSO calls
    `_managed_pair` for agent:abcd1234 must bind its own seat there instead (`bind_holder`
    never invalidates an agent's holds on a DIFFERENT seat, only a different agent's hold
    on the SAME seat: two calls would leave abcd1234 holding two seats at once, breaking
    seat-scoped lookups like the pair rate cap)."""
    import os
    import time as _time

    from src.orchestrator import mounts

    job = tmp_path / "jobs" / "abcd1234"
    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    # the resident's SIGNATURE (the leak fix): a session is addressable only when its own
    # transcript testifies who lives there: here, a signed send receipt as the harness
    # encodes it (JSON-escaped inside the line)
    signed = ('{"type":"user","toolUseResult":'
              '"{\\"sent\\":1,\\"from\\":\\"agent:abcd1234\\"}"}\n')
    t.write_bytes(signed.encode() + b"x" * transcript_bytes)
    old = _time.time() - 3600
    os.utime(t, (old, old))
    await mounts.save_mount(actions.pool, job_dir=str(job), agent_id="agent:abcd1234",
                            project="demo", cwd="/repo/demo", model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    if bind_seat:
        from src.orchestrator.seats import bind_holder, ensure_seat
        seat = (await ensure_seat(actions, house="demo", handle="StaleOwner",
                                  source="test"))["seat_id"]
        await bind_holder(actions, seat_id=seat, agent_id="agent:abcd1234")
        await _office(actions, seat, "/repo/demo")
    obj = await actions.create_or_find_object("Agent", "agent:abcd1234", "test")
    await actions.assert_property(obj, "seat_generation", "1", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(obj, "session", FULL_SID, "test", NOW, 0.9,
                                  evidence_class="self_declared")
    return sense


async def _seat_and_graph_session(
    actions: Actions, *, agent_id: str = "agent:abcd1234", cwd: str = "/repo/demo",
    session: str = FULL_SID, handle: str = "RawFixtureOwner",
) -> str:
    """The graph-truth counterpart a raw `mounts.save_mount`-only fixture never had (task
    #178): a Seat with an office at `cwd`, bound to `agent_id`, plus the graph's own
    `session`/`seat_generation` properties `_lineage_resume_candidate` reads. Returns the
    seat_id."""
    from src.orchestrator.seats import bind_holder, ensure_seat
    seat = (await ensure_seat(actions, house="demo", handle=handle,
                              source="test"))["seat_id"]
    await bind_holder(actions, seat_id=seat, agent_id=agent_id)
    await _office(actions, seat, cwd)
    obj = await actions.create_or_find_object("Agent", agent_id, "test")
    await actions.assert_property(obj, "seat_generation", "1", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(obj, "session", session, "test", NOW, 0.9,
                                  evidence_class="self_declared")
    return str(seat)


async def test_live_owner_gets_delivery_not_a_twin(actions: Actions, tmp_path: Path) -> None:
    """An awake owner (fresh mount) means DELIVER: the mail sits in its box, nothing spawns;
    waking a twin beside a live owner is the fragmentation sibling-one reported."""
    from src.orchestrator import mounts

    await _agent_with_mail(actions)
    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "own00001"),
                            agent_id="agent:own00001", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)  # last_seen = now → live
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn)
    assert spawned == [] and rep["woke"] == 0 and rep["owner_live"] == 1


async def test_resumable_owner_is_resumed_not_minted(actions: Actions, tmp_path: Path) -> None:
    """A stale-but-resumable owner is CONTINUED via its own session: the wake carries
    --resume <its session id> and the ledger records mode='resume'."""
    await _agent_with_mail(actions)
    sense = await _stale_resumable_owner(actions, tmp_path)
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, kw))

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense)), spawn=_spawn)
    assert rep["resumed"] == 1 and rep["woke"] == 1
    repo, kw = calls[0]
    assert repo == "/repo/demo"
    assert kw.get("resume_session") == FULL_SID       # the owner's OWN session, not a twin
    assert "job_dir" not in kw or kw["job_dir"] is None
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes ORDER BY id DESC LIMIT 1") == "resume"


async def test_retired_owner_is_never_reanimated(actions: Actions, tmp_path: Path) -> None:
    """retired=true is a deliberate close: the dispatch skips resume and MINTS a successor."""
    await _agent_with_mail(actions)
    sense = await _stale_resumable_owner(actions, tmp_path)
    a = await actions.create_or_find_object("Agent", "agent:abcd1234", "agent:abcd1234")
    await actions.assert_property(a, "retired", True, "agent:abcd1234", NOW, 0.9,
                                  evidence_class="self_declared")
    calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense)), spawn=_spawn)
    assert rep["resumed"] == 0 and rep["woke"] == 1   # minted, not reanimated
    assert calls[0].get("resume_session") is None or "resume_session" not in calls[0]
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes ORDER BY id DESC LIMIT 1") == "mint"


# --- the ceiling checks the RESUMABLE TAIL, not raw file size (task #135/
# #136): a transcript's cumulative lifetime size is a poor proxy for what a resume actually
# has to hydrate; Claude Code auto-compacts, and only the content since the LAST compaction is
# live. Verified against two real specimens: one 72MB transcript (17 boundaries, 2.29MB tail)
# and one 103MB transcript (20 boundaries, 2.23MB tail), both under 3% of their own file size.
# `resumable_tail_bytes` itself lives in src/ingest/sessions.py (tests: test_sessions.py),
# these tests cover only `_pick_resumable_sync`'s own use of it. ----------------------------

_COMPACT_LINE = (
    b'{"type":"system","subtype":"compact_boundary","summary":"compacted"}\n'
)


def _write_transcript(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)


def _user_line(text: str) -> bytes:
    obj = {"type": "user", "message": {"role": "user", "content": text}}
    return json.dumps(obj).encode() + b"\n"


def test_marker_landed_sync_finds_an_anchored_match(tmp_path: Path) -> None:
    root = tmp_path / "projects"
    _write_transcript(root / "-repo" / "aaaaaaaa-0000.jsonl", _user_line("hello ##MARK## world"))
    assert _marker_landed_sync(root, "aaaaaaaa", "##MARK##") is True


def test_marker_landed_sync_ignores_a_substring_look_alike(tmp_path: Path) -> None:
    """Re-derived rather than inherited: the old
    `glob(f"*/{sid_prefix}*.jsonl")` matched `sid_prefix` ANYWHERE in the filename. A
    look-alike whose stem merely CONTAINS the prefix (never at the very start) carries
    the SAME marker text by construction here (to prove a real look-alike, not just an
    absent one) and must still be ignored, or a coincidental filename could report a
    marker landed in a session that never received it."""
    root = tmp_path / "projects"
    _write_transcript(root / "-repo" / "zzzz-aaaaaaaa-0000.jsonl", _user_line("##MARK##"))
    assert _marker_landed_sync(root, "aaaaaaaa", "##MARK##") is False


def test_marker_landed_sync_still_scans_every_genuine_match_not_just_the_first(
    tmp_path: Path,
) -> None:
    """THE SAFETY PROPERTY RE-DERIVED, NOT REGRESSED: two genuine physical copies of the
    same session (the materializer's own duplicate shape) both anchor on `sid_prefix`:
    the marker landing in the SECOND one alone (the first is a stale/empty copy) must
    still be found. Collapsing to a single newest-anchored file would miss this."""
    root = tmp_path / "projects"
    _write_transcript(root / "-repo" / "aaaaaaaa-0000.jsonl", _user_line("no marker here"))
    _write_transcript(root / "-repo-materialized" / "aaaaaaaa-0001.jsonl",
                      _user_line("##MARK##"))
    assert _marker_landed_sync(root, "aaaaaaaa", "##MARK##") is True


def test_marker_landed_sync_false_when_nothing_matches(tmp_path: Path) -> None:
    root = tmp_path / "projects"
    _write_transcript(root / "-repo" / "bbbbbbbb-0000.jsonl", _user_line("##MARK##"))
    assert _marker_landed_sync(root, "aaaaaaaa", "##MARK##") is False


def test_pick_resumable_sync_rescues_a_large_transcript_with_a_small_tail(
    tmp_path: Path,
) -> None:
    """THE SIZE FIX, isolated: raw size is well over the ceiling, but everything since the
    last compaction fits comfortably under it: now correctly resumable BY SIZE, where the
    old raw-size check would have refused it (exactly two real repro cases,
    one 72MB and one 103MB transcript). min_tail_bytes=1 widens the OTHER gate out of the
    way on purpose: this test is about the size fix alone; the compaction gate has its
    own tests below, since a 2026-08-03 ruling made the two independent."""
    root = tmp_path / "projects"
    proj = root / "-repo-demo"
    body = (b'{"type":"assistant","message":"old"}\n' * 100_000 + _COMPACT_LINE
            + b'{"type":"user","message":"new"}\n' * 3)
    assert len(body) > 1_000_000  # comfortably over the small ceiling used below
    _write_transcript(proj / f"{FULL_SID}.jsonl", body)
    out = trigger_module._pick_resumable_sync(
        [("jobs/abcd1234", "/repo/demo")], root, ceiling_bytes=1000, min_tail_bytes=1)
    assert out is not None and out[0] == FULL_SID


def test_pick_resumable_sync_still_refuses_when_the_tail_itself_is_over_ceiling(
    tmp_path: Path,
) -> None:
    """The size fix narrows the false-refusal, it does not remove the ceiling: a transcript
    whose LIVE tail is genuinely large stays refused. min_tail_bytes=1 isolates this from
    the separate compaction gate, same reasoning as the test above."""
    root = tmp_path / "projects"
    proj = root / "-repo-demo"
    body = (b'{"type":"assistant","message":"old"}\n' * 100_000 + _COMPACT_LINE
            + b'{"type":"user","message":"new"}\n' * 100_000)
    _write_transcript(proj / f"{FULL_SID}.jsonl", body)
    out = trigger_module._pick_resumable_sync(
        [("jobs/abcd1234", "/repo/demo")], root, ceiling_bytes=1000, min_tail_bytes=1)
    assert out is None


# --- the minimum-tail floor, INDEPENDENT of the ceiling (#156's rebuild, 2026-08-09,
# replacing the old compaction-COUNT gate): closed at exactly
# the compaction seam is a rare special case. A tail with real work after the last
# boundary is resumable REGARDLESS of how many times it compacted; only a tail at or near
# zero (the session closed AT the seam) refuses. Measured live on one real specimen: 12 compactions,
# 4.07MB of real work after the last one, the old gate refused it anyway, factually
# wrong about its own transcript (see resume_diagnostics's own docstring). ----------------


def test_pick_resumable_sync_allows_a_compacted_transcript_with_real_tail_work(
    tmp_path: Path,
) -> None:
    """THE EXACT CASE THE OLD COMPACTION-COUNT GATE GOT WRONG: a small tail (well under the
    ceiling) that carries real work since the last compaction boundary IS resumable now,
    however many times it compacted, the same live specimen in miniature."""
    root = tmp_path / "projects"
    proj = root / "-repo-demo"
    body = (b'{"type":"assistant","message":"old"}\n' * 100_000 + _COMPACT_LINE
            + b'{"type":"user","message":"new"}\n' * 3)
    _write_transcript(proj / f"{FULL_SID}.jsonl", body)
    out = trigger_module._pick_resumable_sync(
        [("jobs/abcd1234", "/repo/demo")], root, ceiling_bytes=1_000_000_000,
        min_tail_bytes=1)
    assert out is not None and out[0] == FULL_SID


def test_pick_resumable_sync_allows_a_never_compacted_transcript_at_default_threshold(
    tmp_path: Path,
) -> None:
    """The common case (85.4% of real transcripts, measured): a never-compacted transcript
    passes the floor cleanly: this gate must not misfire on ordinary short sessions."""
    root = tmp_path / "projects"
    proj = root / "-repo-demo"
    _write_transcript(proj / f"{FULL_SID}.jsonl", b'{"type":"user","message":"hi"}\n' * 5)
    out = trigger_module._pick_resumable_sync(
        [("jobs/abcd1234", "/repo/demo")], root, ceiling_bytes=1_000_000_000,
        min_tail_bytes=1)
    assert out is not None and out[0] == FULL_SID


def test_pick_resumable_sync_refuses_a_tail_closed_at_the_seam_itself(
    tmp_path: Path,
) -> None:
    """The operator's own "rare special case": nothing at all after the last compaction
    boundary: the session closed AT the seam, genuinely nothing to resume into. The ONE
    shape the new floor still refuses, however cheap the file is to scan (superseding the
    old size-only optimization, which used to skip the scan entirely for a file already
    under the ceiling by raw size)."""
    root = tmp_path / "projects"
    proj = root / "-repo-demo"
    _write_transcript(proj / f"{FULL_SID}.jsonl", _COMPACT_LINE)
    out = trigger_module._pick_resumable_sync(
        [("jobs/abcd1234", "/repo/demo")], root, ceiling_bytes=1_000_000_000,
        min_tail_bytes=len(_COMPACT_LINE) + 1)
    assert out is None


async def test_trigger_resumes_a_large_transcript_with_a_small_tail_end_to_end(
    actions: Actions, tmp_path: Path,
) -> None:
    """End-to-end through trigger_mail_tick, not just the pure/unit layer: a transcript over
    the raw ceiling but with a small post-compaction tail is now RESUMED, not minted.
    min_tail_bytes=1 isolates the size gate: the compaction gate has its own end-to-end
    coverage via test_ceiling_transcript_mints_instead's sibling below."""
    await _agent_with_mail(actions)
    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    body = (b'{"type":"assistant","message":"old"}\n' * 100_000 + _COMPACT_LINE
            + b'{"type":"user","message":"new"}\n' * 3)
    _write_transcript(proj / f"{FULL_SID}.jsonl", body)
    import os
    import time as _time

    from src.orchestrator import mounts

    job = tmp_path / "jobs" / "abcd1234"
    await mounts.save_mount(actions.pool, job_dir=str(job), agent_id="agent:abcd1234",
                            project="demo", cwd="/repo/demo", model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    old = _time.time() - 3600
    os.utime(proj / f"{FULL_SID}.jsonl", (old, old))
    calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense), ceiling=1000,
                                    min_tail_bytes=1),
        spawn=_spawn)
    assert rep["resumed"] == 1   # "woke" is a superset counter incremented alongside resumed
    assert calls[0].get("resume_session") == FULL_SID


async def test_trigger_resumes_a_once_compacted_small_transcript_with_real_tail_work(
    actions: Actions, tmp_path: Path,
) -> None:
    """End-to-end sibling of the size-fix test above, for the minimum-tail floor (#156's
    rebuild, 2026-08-09, the operator's own correction): a transcript with a tail
    comfortably under the ceiling, carrying real work since its one compaction, is RESUMED,
    the old gate minted a fresh duplicate agent here purely for having compacted at all, which was
    the bug (the same live specimen)."""
    await _agent_with_mail(actions)
    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    body = (b'{"type":"assistant","message":"old"}\n' * 100_000 + _COMPACT_LINE
            + b'{"type":"user","message":"new"}\n' * 3)
    _write_transcript(proj / f"{FULL_SID}.jsonl", body)
    import os
    import time as _time

    from src.orchestrator import mounts

    job = tmp_path / "jobs" / "abcd1234"
    await mounts.save_mount(actions.pool, job_dir=str(job), agent_id="agent:abcd1234",
                            project="demo", cwd="/repo/demo", model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    old = _time.time() - 3600
    os.utime(proj / f"{FULL_SID}.jsonl", (old, old))
    calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense), ceiling=1_000_000_000,
                                    min_tail_bytes=1),
        spawn=_spawn)
    assert rep["resumed"] == 1 and rep["woke"] == 1
    assert calls[0].get("resume_session") == FULL_SID


async def test_ceiling_transcript_mints_instead(actions: Actions, tmp_path: Path) -> None:
    """A transcript at the context ceiling is retirement-by-compaction territory: resuming it
    would replay a legitimate succession; the dispatch mints."""
    await _agent_with_mail(actions)
    sense = await _stale_resumable_owner(actions, tmp_path, transcript_bytes=64)
    calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense), ceiling=32), spawn=_spawn)
    assert rep["resumed"] == 0 and rep["woke"] == 1
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes ORDER BY id DESC LIMIT 1") == "mint"


async def test_resume_is_not_retried_on_the_same_message(
    actions: Actions, tmp_path: Path
) -> None:
    """The alternation guard: a resume that never leased its mail (still deliverable) is not
    tried twice: the next wake for that message MINTS."""
    await _agent_with_mail(actions)
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await actions.pool.fetchval("SELECT id FROM fleet_messages LIMIT 1")
    await actions.pool.execute(
        "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
        "VALUES ('demo','agent:other',$1,'resume')", msg_id)
    calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense)), spawn=_spawn)
    assert rep["resumed"] == 0 and rep["woke"] == 1   # alternated to mint
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes ORDER BY id DESC LIMIT 1") == "mint"


async def test_wake_model_pins_the_triage_lane(actions: Actions, tmp_path: Path) -> None:
    """Wake economics: when osiris_wake_model is set, BOTH lanes spawn with it (the prompt
    escalates real work back to a full session); empty setting passes no model at all."""
    await _agent_with_mail(actions)
    calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    await trigger_mail_tick(
        actions, settings=_settings(enabled=True, wake_model="claude-haiku-4-5-20251001"),
        spawn=_spawn)
    assert calls[0].get("model") == "claude-haiku-4-5-20251001"
    # the prompt carries the escalation contract
    assert "TRIAGE" in _WAKE_PROMPT and "open_thread(kind='obligation')" in _WAKE_PROMPT


# --- the DM lane (fleet mail phase 3, #61): DELIVER → RESUME → nothing, never a mint ---

async def _dm_to_owner(actions: Actions) -> int:
    """A DM to agent:abcd1234 (the resumable-owner fixture's agent)."""
    out = await send_message(actions.pool, from_agent="agent:sender", from_project="other",
                             to_agent="agent:abcd1234", body="for your eyes only")
    return int(out["id"])


async def _no_job(ids: set[str]) -> dict[str, Any] | None:
    """No daemon job matches: a clean, hermetic 'the daemon lane has nothing' for tests
    that want to exercise the resume lane specifically. NEVER pass a bare `None` for
    `jobs`/`nudge` in these tests: dispatch_dm treats either as 'unset' and falls back to
    the REAL claude_daemon functions, which would reach for a live daemon socket."""
    return None


async def test_dispatch_dm_persists_its_own_verdict_onto_the_message(
    actions: Actions,
) -> None:
    """BUG 3: dispatch_dm's own mode never used
    to leave a durable trace on the message it dispatched: only the cron log line saw it.
    `dispatch_mode`/`dispatch_mode_at` on fleet_messages now do, regardless of which branch
    returns (here: the cheapest one, trigger-dark), audit-only, nothing reads it back to
    gate anything."""
    msg_id = await _dm_to_owner(actions)
    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender", settings=_settings(enabled=False))
    assert d["mode"] == "trigger-dark"
    row = await actions.pool.fetchrow(
        "SELECT dispatch_mode, dispatch_mode_at FROM fleet_messages WHERE id=$1", msg_id)
    assert row["dispatch_mode"] == "trigger-dark"
    assert row["dispatch_mode_at"] is not None


async def test_dispatch_dm_persists_mid_turn_and_then_the_later_resumed_verdict(
    actions: Actions, tmp_path: Path,
) -> None:
    """The shape BUG 3 actually cares about: the SAME message's dispatch_mode moves from
    'mid-turn' to whatever the backstop sweep's next tick finds once the addressee goes
    idle: a durable trail of the classification changing, not just the final word. This
    is the existing re-dispatch machinery (trigger_mail_tick calling dispatch_dm fresh
    every ~60s, already correct, no new sweep needed) exercised twice by hand."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    transcript = sense / "-repo-demo" / f"{FULL_SID}.jsonl"
    msg_id = await _dm_to_owner(actions)
    signed = ('{"type":"user","toolUseResult":'
             '"{\\"sent\\":1,\\"from\\":\\"agent:abcd1234\\"}"}\n')

    # a real conversational line, freshly timestamped = a turn genuinely in flight = mid-turn
    fresh_line = json.dumps({"type": "assistant", "timestamp": datetime.now(UTC).isoformat()})
    transcript.write_text(signed + fresh_line + "\n")

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("mid-turn must never reach the wake lane")

    d1 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender",
                           settings=_settings(enabled=True, sense=str(sense)),
                           windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d1["mode"] == "mid-turn"
    assert await actions.pool.fetchval(
        "SELECT dispatch_mode FROM fleet_messages WHERE id=$1", msg_id) == "mid-turn"

    # the transcript's last real line ages past active_secs (its own turn ended): the
    # identical call, re-run exactly as the ~60s backstop sweep would, now falls through
    # to a real dispatch
    stale_line = json.dumps({"type": "assistant", "timestamp": "2020-01-01T00:00:00+00:00"})
    transcript.write_text(signed + stale_line + "\n")
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, kw))

    d2 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender",
                           settings=_settings(enabled=True, sense=str(sense)),
                           spawn=_spawn, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d2["mode"] == "resumed"
    assert calls
    assert await actions.pool.fetchval(
        "SELECT dispatch_mode FROM fleet_messages WHERE id=$1", msg_id) == "resumed"


async def test_a_dm_resumes_the_addressee_itself(actions: Actions, tmp_path: Path) -> None:
    """The payoff: a DM to a stale-but-resumable agent wakes THAT agent via its own session
    (mode 'dm-resume' in the ledger, the private prompt), never a duplicate agent."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    await _dm_to_owner(actions)
    calls: list[tuple[str, str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, prompt, kw))

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense)), spawn=_spawn)
    assert rep["resumed"] == 1 and rep["woke"] == 1
    repo, prompt, kw = calls[0]
    assert kw.get("resume_session") == FULL_SID       # the ADDRESSEE's own session
    assert "private" in prompt and "seat" in prompt   # the DM prompt, not the broadcast one
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes ORDER BY id DESC LIMIT 1") == "dm-resume"


async def test_a_dm_never_mints_a_stranger(actions: Actions, tmp_path: Path) -> None:
    """No mint lane for DMs: an addressee with no resumable session (transcript missing)
    leaves the DM pull-only: a private message is never handed to a fresh duplicate agent."""
    from src.orchestrator import mounts

    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "abcd1234"),
                            agent_id="agent:abcd1234", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    await _dm_to_owner(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(tmp_path / "nowhere")),
        spawn=_spawn)
    assert spawned == [] and rep["woke"] == 0         # nothing woken, nothing minted


async def test_dispatch_dm_refusal_names_no_anchored_transcript_when_nothing_was_ever_mounted(
    actions: Actions, tmp_path: Path,
) -> None:
    """A 2026-08-03 ruling (#135/#136): dispatch_dm's own refusal used to collapse
    two opposite situations into one identical sentence. This pins the 'genuinely nothing
    to resume' shape: a mount exists, but its job_dir anchors no transcript at all under
    the sense root."""
    from src.orchestrator import mounts

    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "abcd1234"),
                            agent_id="agent:abcd1234", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    await _seat_and_graph_session(actions)
    msg_id = await _dm_to_owner(actions)

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("no resumable session means no spawn at all")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(tmp_path / "nowhere")),
                          spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "resume-refused-no-anchor"
    assert "no transcript found on disk" in d["detail"]


async def test_dispatch_dm_refusal_names_the_ceiling_when_a_real_session_is_too_large(
    actions: Actions, tmp_path: Path,
) -> None:
    """The OPPOSITE shape of the sibling test above: a genuine, signed session exists but
    every candidate is over osiris_resume_ceiling_bytes: the refusal must say so, not
    collapse into the same 'no anchored transcript' sentence a truly-missing session gets.
    No mint lane for DMs (unlike the project ladder's own ceiling test): a private message
    stays pull-only, never handed to a fresh twin."""
    sense = await _stale_resumable_owner(actions, tmp_path, transcript_bytes=64)
    msg_id = await _dm_to_owner(actions)

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a DM never mints or resumes past the ceiling")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense), ceiling=32),
                          spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "resume-refused-ceiling"
    # CORRECTED 2026-09-08: `ceiling_bytes` no
    # longer names the primary resumability ceiling: a tiny ceiling=32 now trips the
    # catastrophic-corruption sanity bound in `_verdict_from_diagnostics`, not the (now
    # occupancy-based, and unreached here since the corruption bound fires first) real
    # ceiling. The refusal still classifies as the "ceiling" gate (`_gate_name`).
    assert "catastrophic-corruption sanity bound" in d["detail"]


# --- wake_gate_preflight (#156.4): the same four gates, answerable before an attempt ------

async def test_wake_gate_preflight_reports_resumable_with_no_side_effects(
    actions: Actions, tmp_path: Path,
) -> None:
    """The happy path: every gate clears, and (unlike dispatch_dm) nothing is spawned,
    resumed, or sent. It only answers the question."""
    sense = await _stale_resumable_owner(actions, tmp_path, transcript_bytes=16)
    d = await trigger_module.wake_gate_preflight(
        actions.pool, "agent:abcd1234", settings=_settings(enabled=True, sense=str(sense)))
    assert d["mode"] == "resumable"
    assert d["status"] == "resumable"
    assert "abcd1234" in d["detail"]


async def test_wake_gate_preflight_names_the_ceiling_before_any_attempt(
    actions: Actions, tmp_path: Path,
) -> None:
    """The same specimen dispatch_dm's own ceiling test pins, read through the read-only
    surface: must agree exactly, since both call the same underlying gate."""
    sense = await _stale_resumable_owner(actions, tmp_path, transcript_bytes=64)
    d = await trigger_module.wake_gate_preflight(
        actions.pool, "agent:abcd1234",
        settings=_settings(enabled=True, sense=str(sense), ceiling=32))
    assert d["mode"] == "resume-refused-ceiling"
    assert d["status"] == "refused-ceiling"
    # CORRECTED 2026-09-08: see the sibling
    # dispatch_dm ceiling test's own comment: ceiling=32 now trips the catastrophic-
    # corruption sanity bound, not the (occupancy-based) real ceiling.
    assert "catastrophic-corruption sanity bound" in d["detail"]


async def test_wake_gate_preflight_reports_fresh_heir_available_past_the_seam(
    actions: Actions, tmp_path: Path,
) -> None:
    """RESUME / NUDGE / FRESH-HEIR: the read-only
    preflight must agree with what a real dispatch_dm would do: a holder past its own
    compaction seam is no longer a bare 'refused-compaction' wall, it is a real, actionable
    outcome (a real wake would boot a fresh successor here)."""
    sense = await _lineage_holder_with_session(
        actions, tmp_path, agent_id="agent:wg0001", compacted=True)
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:wg0001", manager_agent="agent:hm-wg0001",
        worker_handle="Wg-Seam-Test", house="osiris")
    await _office(actions, worker_seat, "/repo/demo")
    # THE JOB_DIR ANCHOR (`_job_id`/`locate_current_transcript`): the transcript-locating
    # gate matches on job_dir's OWN last path segment as a PREFIX of the session id, never
    # on cwd: `_lineage_holder_with_session` always names its file FULL_SID.jsonl
    # ("abcd1234-…"), so the job_dir here must end in "abcd1234" to anchor onto it.
    await save_mount(actions.pool, job_dir="/x/jobs/abcd1234", agent_id="agent:wg0001",
                     project="osiris", cwd="/repo/demo", model=None,
                     session_key=None)
    d = await trigger_module.wake_gate_preflight(
        actions.pool, "agent:wg0001", seat_id=worker_seat,
        settings=_settings(enabled=True, sense=str(sense), min_tail_bytes=1000))
    assert d["mode"] == "fresh-heir-available"
    assert d["status"] == "fresh-heir-available"
    assert "/repo/demo" in d["detail"]


async def test_wake_gate_preflight_finds_a_bg_anchor_resume_the_mount_row_misses(
    actions: Actions, tmp_path: Path,
) -> None:
    """A live-fire specimen: a `--bg`-launched seat
    with NO `agent_mounts` row at all for the addressee (the shared-anchor collapse
    `_lineage_resume_candidate`'s own docstring documents): `_agent_resumable` reads
    this as "no anchored transcript at all" (gate='no-anchor'), and the OLD preflight
    code only ever tried the lineage walk when the mount-keyed gate said 'compaction',
    so it reported a confident false `resume-refused-no-anchor` here. The graph's own
    `session` property (succession_chain's shape) still points at a real, uncompacted,
    resumable transcript: exactly what `launch_seat` (`test_launch_harness_lane_
    resumes_a_stale_but_resumable_holder`, same fixture) actually resumes. preflight
    must now agree with launch, not merely echo the narrower mount-keyed miss."""
    sense = await _lineage_holder_with_session(
        actions, tmp_path, agent_id="agent:fulcrum1")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:fulcrum1", manager_agent="agent:hm-fulcrum1",
        worker_handle="Fulcrum-Test", house="osiris")
    await _office(actions, worker_seat, "/repo/demo")
    # THE SHARED `--bg` ANCHOR ITSELF: a mount row exists (so wakeable_identity resolves
    # a living head at all (exactly how a real specimen still shows up), but
    # its job_dir anchors to NOTHING on disk (a stale/overwritten anchor, or a later
    # session-less generation's own mount): `_agent_resumable` genuinely reports
    # 'no anchored transcript at all' for THIS row, while the graph's own `session`
    # property (set by `_lineage_holder_with_session` above) still points at a real file.
    await save_mount(actions.pool, job_dir="/x/jobs/stale-anchor-no-file",
                     agent_id="agent:fulcrum1", project="osiris", cwd="/repo/demo",
                     model=None, session_key=None)

    d = await trigger_module.wake_gate_preflight(
        actions.pool, "agent:fulcrum1", seat_id=worker_seat,
        settings=_settings(enabled=True, sense=str(sense)))
    assert d["mode"] == "resumable"
    assert d["status"] == "resumable"
    assert "lineage walk" in d["detail"]
    assert FULL_SID[:8] in d["detail"]


async def test_wake_gate_preflight_reports_never_mounted(actions: Actions) -> None:
    """No agent_mounts row at all: nothing to wait for, ever."""
    d = await trigger_module.wake_gate_preflight(
        actions.pool, "agent:totally-unknown", settings=_settings(enabled=True))
    assert d["mode"] == "never-mounted"
    assert d["status"] == "no-live-body"


async def test_wake_gate_preflight_reports_queued_live_when_only_transcript_fresh(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A specimen re-pinned after a later fix:
    a mind can be live, writing to its own transcript right now, with NO agent_mounts
    row at all; wakeable_identity (agent_mounts-only) finds nothing, but that lookup miss
    is not evidence the mind never mounted. `current_assertions.last_active` used to cover
    this, but that property goes stale forever after one miner pass
    the live fallback is now a real transcript stat against this
    lineage's own durable `anchor_sid` ledger. Must report the honest 'live, but no
    session resolved' outcome, never the false-absence 'never-mounted'."""
    monkeypatch.setenv("OSIRIS_TRANSCRIPTS", str(tmp_path))
    a = await actions.create_or_find_object("Agent", "agent:liveonly01", "fleet-observer")
    sid = "aaaa1111-bbbb-2222-cccc-333344445555"
    (tmp_path / "-repo").mkdir()
    (tmp_path / "-repo" / f"{sid}.jsonl").write_text('{"type":"user"}\n')
    await actions.assert_property(a, f"anchor_sid:{sid[:8]}", sid, "test", NOW, 0.9,
                                  evidence_class="direct_observation")
    d = await trigger_module.wake_gate_preflight(
        actions.pool, "agent:liveonly01", settings=_settings(enabled=True))
    assert d["mode"] == "queued-live-unresolved"
    assert d["status"] == "queued"
    assert "has never mounted" not in d["detail"]


async def test_wake_gate_preflight_reports_cold_mounted_before_not_never_mounted(
    actions: Actions,
) -> None:
    """A post-reboot finding: agent:fb47aea8-xiv had
    mounted and sent DMs that same morning, but its agent_mounts row was gone by the time
    send() ran (the sweep, or a reboot leaving the cache empty, the registry went
    stale), send() answered 'never-mounted' anyway, a false claim of zero history. NO
    agent_mounts row at all (so wakeable_identity misses, exactly like the real
    specimen), but a durable anchor_sid assertion (record_session_anchor, stamped once
    per real session and never swept) proves a real session bound to this identity
    before: must report 'cold-mounted-before', never the false-absence 'never-mounted'."""
    from datetime import UTC, datetime

    a = await actions.create_or_find_object(
        "Agent", "agent:coldmount1", "agent:coldmount1")
    await actions.assert_property(a, "anchor_sid:deadbeef", "deadbeef12345678",
                                  "agent:coldmount1", datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")
    d = await trigger_module.wake_gate_preflight(
        actions.pool, "agent:coldmount1", settings=_settings(enabled=True))
    assert d["mode"] == "cold-mounted-before"
    assert d["status"] == "no-live-body"
    assert "has never mounted" not in d["detail"]
    assert "mounted before" in d["detail"]


async def test_wake_preflight_mcp_tool_resolves_a_seat_and_never_touches_dispatch(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The MCP surface: resolves target the same way wake()/dispatch_dm do, then answers
    read-only: dispatch_dm itself must never be called."""
    import src.mcp_server as srv

    sense = await _stale_resumable_owner(actions, tmp_path, transcript_bytes=16)
    monkeypatch.setattr(trigger_module, "get_settings",
                        lambda: _settings(enabled=True, sense=str(sense)))
    saved_pool = srv._pool
    srv._pool = actions.pool

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("preflight must never dispatch a real wake")

    monkeypatch.setattr(trigger_module, "dispatch_dm", _boom)
    try:
        d = await srv.wake_preflight("agent:abcd1234")
    finally:
        srv._pool = saved_pool
    assert d["mode"] == "resumable"


async def test_wake_preflight_mcp_tool_resolves_a_bare_claimed_handle(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live-fire regression (2026-08-08): this tool's own first real run against a bare
    handle answered 'never-mounted' for a seat that had actually
    mounted many times: _resolve_wake_address only ever understood 'seat:'/'agent:'
    prefixes; dispatch_dm's own addressee always arrives PRE-RESOLVED via wake_worker's
    _seat_for_target call, so this gap was invisible until something called the MCP tool
    directly with a plain name, the same way a human or a fleet agent actually would."""
    import src.mcp_server as srv

    sense = await _stale_resumable_owner(actions, tmp_path, transcript_bytes=16)
    seat = (await ensure_seat(actions, house="demo", handle="Nefertari",
                              source="test"))["seat_id"]
    await bind_holder(actions, seat_id=seat, agent_id="agent:abcd1234")
    monkeypatch.setattr(trigger_module, "get_settings",
                        lambda: _settings(enabled=True, sense=str(sense)))
    saved_pool = srv._pool
    srv._pool = actions.pool

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("preflight must never dispatch a real wake")

    monkeypatch.setattr(trigger_module, "dispatch_dm", _boom)
    try:
        d = await srv.wake_preflight("Nefertari")
    finally:
        srv._pool = saved_pool
    assert d["mode"] == "resumable", d


async def test_mid_turn_means_the_transcript_is_moving_NOT_the_heartbeat(
    actions: Actions, tmp_path: Path
) -> None:
    """THE STATUSLINE-HEARTBEAT SUPERSTITION, killed at the operator's first live
    round-trip ask (2026-07-20): the chrome bumps agent_mounts.last_seen every few seconds
    FOR BACKGROUNDED SESSIONS TOO, so by that field every seated idle agent read as
    permanently mid-turn and the resume gate could never open. A turn WRITES the
    transcript; a statusline render does not. AND THE INODE IS NOT THE TRANSCRIPT (the
    same phantom finding, 2026-07-21): something in the chrome/daemon touches mtime on a session
    that is OFF: awake and asleep must never be confounded.
    (a) fresh heartbeat + quiet transcript → RESUMED; (b) a touched inode with no fresh
    TURN is ASLEEP → RESUMED, never 'delivered' to a corpse; (c) a genuinely moving
    transcript (timestamped turn in the tail) → delivered, no second process."""
    import json as _json
    import os
    import time as _time
    from datetime import UTC as _UTC
    from datetime import datetime as _dt

    sense = await _stale_resumable_owner(actions, tmp_path)
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now()")  # the pump
    m1 = await _dm_to_owner(actions)
    spawned: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(kw)

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense)), spawn=_spawn)
    assert rep["resumed"] == 1 and spawned[0].get("resume_session") == FULL_SID

    # (b) THE PHANTOM: mtime bumped, size unchanged, no timestamped turn: the addressee
    # is ASLEEP and the mail wakes it; 'delivered' to a dead session strands the letter
    await actions.pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, read_at) "
        "VALUES ($1,$2,now())", m1, "agent:abcd1234")
    await actions.pool.execute("DELETE FROM agent_wakes")  # clear the once-per-message row
    m2 = await send_message(actions.pool, from_agent="agent:sender", from_project="other",
                            to_agent="agent:abcd1234", body="while you were touched")
    t = sense / "-repo-demo" / f"{FULL_SID}.jsonl"
    now = _time.time()
    os.utime(t, (now, now))
    rep2 = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense)), spawn=_spawn)
    assert rep2["resumed"] == 1 and len(spawned) == 2 and rep2["owner_live"] == 0

    # (c) a REAL turn in flight (timestamped line in the tail): delivered, no new process
    await actions.pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, read_at) "
        "VALUES ($1,$2,now())", m2["id"], "agent:abcd1234")
    await actions.pool.execute("DELETE FROM agent_wakes")
    await send_message(actions.pool, from_agent="agent:sender", from_project="other",
                       to_agent="agent:abcd1234", body="while you were typing")
    with t.open("a") as fh:  # the fixture's pad bytes end without a newline, start fresh
        fh.write("\n" + _json.dumps({"type": "assistant",
                                     "timestamp": _dt.now(_UTC).isoformat()}) + "\n")
    rep3 = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense)), spawn=_spawn)
    assert rep3["resumed"] == 0 and len(spawned) == 2 and rep3["owner_live"] == 1


def test_turn_fresh_sync_a_daemon_resume_line_is_not_a_turn(tmp_path: Path) -> None:
    """LIVE SPECIMEN (2026-09-13 17:26 CDT reboot): the
    daemon resumed every backgrounded body within five minutes, and each resume appears to
    append a fresh-timestamped housekeeping line (a `type: system` entry, no different in
    shape from a turn_duration/stop_hook_summary line a real turn also leaves behind) to
    the transcript: NOT a conversational turn. `_turn_fresh_sync` used to check only that
    SOME line in the tail carried a recent timestamp, with no regard for what kind of line
    it was, so this read as 'genuinely mid-turn' and held the addressee's mail for as long
    as nothing else touched the file (five DMs sat 13 minutes after the live reboot before
    they were woken by hand). A `type: system` line with a fresh timestamp must NOT count
    as a turn in flight; a real `type: assistant`/`user` line must, the same mtime-
    toucher protection (reading content, never the inode) stays intact either way."""
    sid = "abcd1234-0000-4000-8000-000000000000"
    root = tmp_path / "projects"
    proj = root / "-repo-demo"
    proj.mkdir(parents=True)
    t = proj / f"{sid}.jsonl"

    # only a daemon-authored system/resume line in the tail, freshly timestamped
    t.write_text(json.dumps({
        "type": "system", "subtype": "turn_duration",
        "timestamp": datetime.now(UTC).isoformat(),
    }) + "\n")
    assert trigger_module._turn_fresh_sync(root, sid, 900) is False

    # a genuine assistant turn, freshly timestamped, in the same file
    with t.open("a") as fh:
        fh.write(json.dumps({
            "type": "assistant", "timestamp": datetime.now(UTC).isoformat(),
        }) + "\n")
    assert trigger_module._turn_fresh_sync(root, sid, 900) is True


async def test_a_dm_resume_is_never_looped(actions: Actions, tmp_path: Path) -> None:
    """One attempt per message: a dm-resume that didn't settle its mail is not retried,
    the DM falls back to pull (and the estate carries it across the next mint)."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    await _dm_to_owner(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    st = _settings(enabled=True, sense=str(sense))
    await trigger_mail_tick(actions, settings=st, spawn=_spawn)
    await trigger_mail_tick(actions, settings=st, spawn=_spawn)
    assert len(spawned) == 1                          # the second tick declines


async def test_the_mint_prompt_retires_its_face() -> None:
    """Wake hygiene: a triage wake is one-shot, the prompt itself carries
    the retire() duty so no zombie card survives it."""
    assert "retire()" in _WAKE_PROMPT and "ONE-SHOT" in _WAKE_PROMPT


def test_a_rate_is_not_a_bound() -> None:
    """THE 2026-07-12 GHOST FARM, in one assertion.

    Every guard in should_wake measured wakes over a SLIDING WINDOW: the per-project cap, the
    hourly budget, the grace. Every one of them RESETS. So one unread letter ("to whoever mounts
    sibling-three next") spawned 79 `claude -p` sessions over 18 hours on a project the operator
    had not opened in two days, minting a fresh agent every ~32 minutes: AT EXACTLY THE CAP.
    The cap was working perfectly, and that was the bug: it bounded the RATE while nothing bounded
    the TOTAL, so a message that could never be settled became a permanent alarm clock ticking at
    the legal limit.
    """
    # the old world: the window has rolled over, so the rate cap happily says WAKE, forever
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5) is None
    # the new bound is a TOTAL and it does not reset
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5,
                       attempts=3, attempt_limit=3) == "unsettleable"
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5,
                       attempts=2, attempt_limit=3) is None      # still trying: fine
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5,
                       attempts=79, attempt_limit=3) == "unsettleable"


def test_urgency_cannot_override_the_total() -> None:
    """A message that has failed three times is still failing, and urgency is not a reason to
    keep failing louder. `urgent` rides through the budget guards: it must NOT ride through
    this one, or the loop simply returns wearing a hat."""
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5, urgent=True,
                       hourly_wakes=0, hourly_budget=30,
                       attempts=3, attempt_limit=3) == "unsettleable"


def test_the_limit_is_checked_before_every_other_guard() -> None:
    """'unsettleable' is the hardest signal here: it is a fact about the MESSAGE, not about our
    current appetite. It must win over rate-capped/grace, or the true reason gets masked and the
    escalation to the human never fires."""
    assert should_wake(enabled=True, recent_wakes=99, rate_cap=5, within_grace=True,
                       attempts=3, attempt_limit=3) == "unsettleable"
    # ...but the kill switch still wins over everything. Off means off.
    assert should_wake(enabled=False, recent_wakes=0, rate_cap=5,
                       attempts=3, attempt_limit=3) == "disabled"


def test_attempt_limit_off_by_default_leaves_the_old_behaviour_intact() -> None:
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5,
                       attempts=999, attempt_limit=0) is None


async def test_an_unsettleable_letter_stops_forever_and_tells_the_human(
    actions: Actions,
) -> None:
    """THE GHOST FARM, killed end to end.

    The rate-cap test above proves the loop halts WITHIN A WINDOW. It does not halt ACROSS
    windows: the cap resets, and the wake fires again, forever. That is exactly what happened:
    one letter spawned 79 sessions over 18 hours on an abandoned project, each wake obediently
    reading it, correctly judging it was not theirs to ack, leaving it politely alone, and
    thereby summoning its replacement. The letter's own politeness was the fuel.

    Now the total bounds it: after `attempts` tries the trigger STOPS on that message forever and
    hands it to the only reader who can act: the human. Nothing is deleted; the letter stays in
    the graph. It simply stops ringing.
    """
    await _agent_with_mail(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    # rate_cap high + window rolling is the old world; the TOTAL is what must bite.
    st = _settings(enabled=True, rate_cap=99, lease=0, attempts=3)
    for _ in range(20):                      # twenty ticks, mail never settled, the storm
        await trigger_mail_tick(actions, settings=st, spawn=_spawn)

    assert len(spawned) == 3, "the TOTAL must bound it, a rate would have spawned 20"

    # the tombstone: recorded once, and excluded from the attempt count so it cannot re-arm
    tombs = await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE mode='abandoned'")
    assert tombs == 1

    # AND THE HUMAN IS TOLD: the loop may close, but never silently
    desk = await read_inbox(actions.pool, OPERATOR_ADDR, reader_agent="operator",
                            mark_read=False)
    briefs = [m for m in desk if "UNSETTLEABLE MAIL" in m["body"]]
    assert len(briefs) == 1
    assert "STOPPED waking on it" in briefs[0]["body"]
    assert "it is a leak" in briefs[0]["body"]

    # a further twenty ticks change nothing: no new spawns, no second brief, no re-arm
    for _ in range(20):
        await trigger_mail_tick(actions, settings=st, spawn=_spawn)
    assert len(spawned) == 3
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE mode='abandoned'") == 1


async def test_the_wake_prompt_forbids_leaving_mail_unsettled() -> None:
    """The prompt gave a triage wake two exits (ack it, or leave it), and 'leave it' silently
    re-armed the wake. It needs the THIRD DOOR named, or a well-behaved agent keeps the loop
    alive by doing exactly what it was told."""
    assert "MUST NOT LEAVE MAIL UNSETTLED" in _WAKE_PROMPT
    assert "THIRD OPTION" in _WAKE_PROMPT
    assert "obligation" in _WAKE_PROMPT


def test_a_wake_gets_one_stable_ghost_per_project_not_one_per_wake() -> None:
    """463 MINTS, 463 IDENTITIES, AND NOT ONE OF THEM THE INTENDED ONE.

    The anchor was keyed on the WAKE ROW ID, so every wake resolved to a fresh agent:wake-<id>
    and the roster filled with strangers the operator never started: 48 in sibling-three alone,
    a project he had not opened in two days. A wake is not a new MIND; it is the same errand run
    again. One name per house, re-worn.
    """
    from src.orchestrator.trigger import _wake_job_dir

    a = _wake_job_dir("sibling-three")
    b = _wake_job_dir("sibling-three")
    assert a == b and a.endswith("/jobs/wake-sibling-three")   # same errand, same face
    assert _wake_job_dir("tony") != a                          # different house, different face
    # a hostile project name cannot escape the jobs dir
    assert "/jobs/wake-" in _wake_job_dir("../../etc/passwd")
    assert ".." not in _wake_job_dir("../../etc/passwd").split("/jobs/")[1]


def test_the_wake_prompt_carries_a_literal_anchor_never_a_shell_variable() -> None:
    """A woken agent has NO SHELL (its tools are mcp__osiris only), so `$CLAUDE_JOB_DIR` in a
    prompt is just text it hands over verbatim. The mount-anchor hook rightly refuses a '$'-bearing
    path and derives one from the SESSION id instead, fresh for every `claude -p`. That is how all
    463 mints got a new identity while the stable anchor sat unused.

    An earlier ruling had already fixed exactly this for the SessionStart whisper ("tell the agent
    the literal path, never $CLAUDE_JOB_DIR") and nobody carried the fix here.
    """
    assert "$CLAUDE_JOB_DIR" not in _WAKE_PROMPT
    rendered = _WAKE_PROMPT.format(repo="/repo/demo", job_dir="/tmp/osiris-wakes/jobs/wake-demo")
    assert 'job_dir="/tmp/osiris-wakes/jobs/wake-demo"' in rendered
    assert "$" not in rendered.split("mount(")[1].split(")")[0]


async def test_the_DAILY_CEILING_stops_the_wake(actions: Actions) -> None:
    """THE PRODUCER THIS CEILING WAS BUILT FOR.

    A wake is not a token. It is an entire Claude session, with tools, in a repo, on the
    operator's card. 463 of them were minted on projects he had not opened in days, and NOT ONE
    was ever in the ledger: because the spawner threw the vendor's own receipt at /dev/null.

    Every other guard on this path is a RATE (wakes per hour, attempts per message). AND A RATE
    IS NOT A BOUND: the wake storm ran for days at a perfectly legal 5/hr, and every guard was
    working exactly as designed while it happened. A rate limits how FAST you burn. Only a
    ceiling limits how MUCH.

    BUT ONLY WHEN THE DOLLARS ARE REAL (a live specimen, 2026-07-21): the ceiling bites on the keyed
    API backend, where total_cost_usd is a true debit. On a subscription the figure is notional
    and the gate is inert: so this test runs the billed world explicitly (extract_provider=
    'anthropic' + a key → spend_is_metered True).
    """
    from src.ingest.providers import Usage
    from src.ingest.usage import record_usage

    spawned: list[Any] = []

    async def _spawn(*a: Any, **kw: Any) -> None:
        spawned.append(a)

    await _agent_with_mail(actions)
    for _ in range(12):                                  # $12 spent against a $10 ceiling
        await record_usage(actions.pool, purpose="wake", usage=Usage(
            model="claude-haiku-4-5-20251001", input_tokens=1, output_tokens=1,
            cache_read_tokens=0, cache_creation_tokens=0, cost_usd=1.00))

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, daily_usd=10.0,
                                    extract_provider="anthropic", api_key="k"), spawn=_spawn)

    assert spawned == [], "the ceiling was reached and the trigger spawned anyway"
    assert rep.get("refused") == 1
    assert "CEILING REACHED" in str(rep.get("why", ""))


async def test_a_SUBSCRIPTION_does_not_false_stop_the_wake(actions: Actions) -> None:
    """THE FALSE STOP, REMOVED (a live specimen, 2026-07-21). Identical $12-over-$10 ledger to the
    test above, but on a SUBSCRIPTION (the local Claude CLI, the helper's default), where
    total_cost_usd is a notional number the vendor prints, not a debit against a card. The
    ceiling must NOT refuse here: it was halting real work on imaginary money. Every other guard
    (rate caps, the window, the licence) still stands; only the phantom dollar wall is gone."""
    from src.ingest.providers import Usage
    from src.ingest.usage import record_usage

    spawned: list[Any] = []

    async def _spawn(*a: Any, **kw: Any) -> None:
        spawned.append(a)

    await _agent_with_mail(actions)
    for _ in range(12):                                  # $12 of NOTIONAL cost, not a real charge
        await record_usage(actions.pool, purpose="wake", usage=Usage(
            model="claude-haiku-4-5-20251001", input_tokens=1, output_tokens=1,
            cache_read_tokens=0, cache_creation_tokens=0, cost_usd=1.00))

    # extract_provider defaults to 'claude-cli' → spend_is_metered False → the gate is inert
    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, daily_usd=10.0), spawn=_spawn)

    assert rep.get("refused") != 1, "the ceiling false-stopped on notional subscription dollars"


async def test_poke_lane_types_into_the_open_window_before_any_resume(
    actions: Actions,
) -> None:
    """THE WAKE LAW (Phase 2, Stage C): mail for a project whose session lives in a manager
    window becomes a TURN in that window: typed, recorded mode='poke', no process spawned.
    A poke already typed for the same cause (deduped) and still unsettled ESCALATES past
    the window to the old resume/mint ladder."""
    from src.orchestrator.mounts import save_mount

    await _agent_with_mail(actions)
    await save_mount(actions.pool, job_dir="/x/jobs/beefcafe", agent_id="agent:demo",
                     project="demo", cwd="/repo/demo", model=None,
                     session_key="whisper:beefcafe", alive=False)  # pulseless: not owner_live
    wins = [{"name": "w-demo", "alive": True, "idle_seconds": 999.0,
             "job_dir": "/x/jobs/beefcafe"}]
    pokes: list[tuple[str, str, int]] = []
    spawned: list[str] = []

    async def _windows() -> list[dict[str, Any]]:
        return wins

    async def _poke(name: str, text: str, *, dedup: str, min_idle: int) -> dict[str, Any]:
        pokes.append((name, dedup, min_idle))
        return {"poked": name, "idle_seconds": 999.0}

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn,
                                  windows=_windows, poke=_poke)
    assert rep["poked"] == 1 and rep["woke"] == 1
    assert spawned == []                                  # a turn, never a second process
    assert pokes[0][0] == "w-demo" and pokes[0][1].startswith("msg:")
    assert pokes[0][2] == 600                             # the idle gate rides the call
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes ORDER BY id DESC LIMIT 1") == "poke"

    # the SAME cause again: the daemon answers deduped; the ledger already knows the poke
    # (mode='poke'), so the lane escalates PAST the window: sense="" makes that a mint
    async def _poke_deduped(name: str, text: str, *, dedup: str,
                            min_idle: int) -> dict[str, Any]:
        return {"poked": name, "deduped": True}

    rep2 = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn,
                                   windows=_windows, poke=_poke_deduped)
    assert rep2["poked"] == 0 and spawned == ["/repo/demo"]  # escalated to the mint rung


async def test_poke_only_arms_the_window_and_nothing_else(actions: Actions) -> None:
    """THE POKE-ONLY ARM (2026-07-19): the poke lane is armed, without turning on the
    miners or critter background agents yet. With the lane switch on, the ladder ends at
    the poke. (1) mail with NO window is HELD: never minted; (2) a poke already typed for
    the same cause and still unsettled is HELD: never escalated to resume/mint; (3) an
    open window still gets its turn, exactly as before."""
    from src.orchestrator.mounts import save_mount

    await _agent_with_mail(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    # (1) no window anywhere → held, not minted (the dark-manager autouse fixture rules)
    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True, poke_only=True),
                                  spawn=_spawn)
    assert spawned == [] and rep["woke"] == 0
    assert rep["poke_only_held"] == 1
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0

    # (3) an open window gets its turn: the poke lane itself is untouched by the switch
    await save_mount(actions.pool, job_dir="/x/jobs/beefcafe", agent_id="agent:demo",
                     project="demo", cwd="/repo/demo", model=None,
                     session_key="whisper:beefcafe", alive=False)
    wins = [{"name": "w-demo", "alive": True, "idle_seconds": 999.0,
             "job_dir": "/x/jobs/beefcafe"}]

    async def _windows() -> list[dict[str, Any]]:
        return wins

    async def _poke(name: str, text: str, *, dedup: str, min_idle: int) -> dict[str, Any]:
        return {"poked": name}

    rep2 = await trigger_mail_tick(actions, settings=_settings(enabled=True, poke_only=True),
                                   spawn=_spawn, windows=_windows, poke=_poke)
    assert rep2["poked"] == 1 and spawned == []

    # (2) the same cause, deduped and still unsettled: the pre-poke ladder would escalate
    # to a mint, poke-only HOLDS instead. No process, ever.
    async def _poke_deduped(name: str, text: str, *, dedup: str,
                            min_idle: int) -> dict[str, Any]:
        return {"poked": name, "deduped": True}

    rep3 = await trigger_mail_tick(actions, settings=_settings(enabled=True, poke_only=True),
                                   spawn=_spawn, windows=_windows, poke=_poke_deduped)
    assert spawned == [], "poke-only escalated to a spawn, the forbidden rung fired"
    assert rep3["poke_only_held"] == 1 and rep3["resumed"] == 0


async def test_the_dm_lane_rides_its_own_arm_not_poke_only(
    actions: Actions, tmp_path: Path
) -> None:
    """SUPERSESSION ON THE RECORD: the poke-only arm (2026-07-19)
    used to hold the DM resume rung too, then a later adapter ruling (2026-07-20)
    made resume the DM lane's PRIMARY push. poke_only still holds the BROADCAST spawn
    rungs (that word stands: no critter background agents); the DM lane rides its OWN arm,
    osiris_dm_resume: poke_only=True no longer touches it, and dm_resume=False is the
    switch that darkens it."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    m1 = await _dm_to_owner(actions)
    calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense), poke_only=True),
        spawn=_spawn)
    assert rep["resumed"] == 1 and calls[0].get("resume_session") == FULL_SID

    # the resumed session settles its mail; the DM lane's own dark arm then holds a FRESH
    # DM as 'held', counted where the operator's chrome already looks
    await actions.pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, read_at) "
        "VALUES ($1,$2,now())", m1, "agent:abcd1234")
    await send_message(actions.pool, from_agent="agent:sender", from_project="other",
                       to_agent="agent:abcd1234", body="a second word")
    rep2 = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense), dm_resume=False),
        spawn=_spawn)
    assert len(calls) == 1 and rep2["resumed"] == 0
    assert rep2["poke_only_held"] == 1


async def test_a_busy_window_defers_and_spends_nothing(actions: Actions) -> None:
    """The idle gate's refusal is a DEFERRAL, not a wake: nothing recorded, nothing
    spawned: the mail waits for the next tick or the window's own next osiris call."""
    from src.orchestrator.mounts import save_mount

    await _agent_with_mail(actions)
    await save_mount(actions.pool, job_dir="/x/jobs/beefcafe", agent_id="agent:demo",
                     project="demo", cwd="/repo/demo", model=None,
                     session_key="whisper:beefcafe", alive=False)
    spawned: list[str] = []

    async def _windows() -> list[dict[str, Any]]:
        return [{"name": "w-demo", "alive": True, "idle_seconds": 3.0,
                 "job_dir": "/x/jobs/beefcafe"}]

    async def _poke(name: str, text: str, *, dedup: str, min_idle: int) -> dict[str, Any]:
        return {"error": "window busy", "busy": True}

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn,
                                  windows=_windows, poke=_poke)
    assert rep["window_busy"] == 1 and rep["woke"] == 0 and spawned == []
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


async def test_a_dm_pokes_the_addressees_own_window_lineage_wide(actions: Actions) -> None:
    """The DM half: the addressee's own window (matched by ANY generation's anchor, the
    lineage rollup's discipline): gets the private prompt, recorded mode='dm-poke'."""
    from src.orchestrator.mounts import save_mount

    a = await actions.create_or_find_object("Agent", "agent:demo-ii", "session")
    await actions.assert_property(a, "project", "demo", "session", NOW, 0.9)
    await send_message(actions.pool, from_agent="agent:other", from_project="other",
                       to_agent="agent:demo-ii", body="for your eyes")
    # the lineage's LIVE window anchors on the BASE generation's old job_dir: still its own
    await save_mount(actions.pool, job_dir="/x/jobs/cafe0001", agent_id="agent:demo-ii",
                     project="demo", cwd="/repo/demo", model=None,
                     session_key="whisper:cafe0001", alive=False)
    pokes: list[str] = []

    async def _windows() -> list[dict[str, Any]]:
        return [{"name": "w-demo-own", "alive": True, "idle_seconds": 999.0,
                 "job_dir": "/x/jobs/cafe0001"}]

    async def _poke(name: str, text: str, *, dedup: str, min_idle: int) -> dict[str, Any]:
        pokes.append(dedup)
        return {"poked": name}

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        raise AssertionError("a DM with a live window must never spawn")

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn,
                                  windows=_windows, poke=_poke)
    assert rep["poked"] == 1 and pokes and pokes[0].startswith("dm:")
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes ORDER BY id DESC LIMIT 1") == "dm-poke"


async def test_a_mint_declares_its_parent_when_the_room_has_a_seat(
    actions: Actions,
) -> None:
    """THE WAKE-ORPHAN CURE, the trigger's half: a mint into a room with a NAMED seat
    carries spawn_parent (the seat's living head): the child is born declared, never an
    anonymous stranger. A seatless room's mint carries None (the visitor class)."""
    from src.orchestrator.mounts import save_mount

    await _agent_with_mail(actions)
    a = await actions.create_or_find_object("Agent", "agent:demo", "session")
    await actions.assert_property(a, "handle", "Demo", "session", NOW, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/x/jobs/demodoor", agent_id="agent:demo",
                     project="demo", cwd="/repo/demo", model=None,
                     session_key="whisper:demodoor", alive=False)
    seen: list[Any] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        seen.append(kw.get("spawn_parent"))

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn)
    assert rep["woke"] == 1 and seen == ["agent:demo"]   # born declared, the seat's head

# ═══ the background-session adapter: dispatch_dm, the per-hop grammar ═══
# The fleet is harness-backgrounded sessions under one spawner pty, no pty fd, no turn in
# flight: so RESUME is the DM lane's primary push, dispatched PER MESSAGE on arrival
# (send()'s immediate leg) with the worker tick as the queue-draining backstop. These tests
# pin the four walls: immediate, gated (needs-input / pause), flat, braked.


async def test_dispatch_is_immediate_and_carries_the_receipt(
    actions: Actions, tmp_path: Path
) -> None:
    """The immediate leg: ONE dispatch_dm call (the thing send() fires on arrival), pushes
    the DM as the addressee's next turn and returns the per-hop receipt. No tick, no clock:
    natural mail arrival IS the spacing (a schedule halts-then-floods into rate limits)."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await _dm_to_owner(actions)
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, kw))

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows)
    assert d["mode"] == "resumed" and FULL_SID[:8] in d["detail"]
    assert calls[0][1].get("resume_session") == FULL_SID
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes ORDER BY id DESC LIMIT 1") == "dm-resume"


async def test_a_dm_queued_during_an_outage_dispatches_on_the_backstops_next_tick(
    actions: Actions, tmp_path: Path,
) -> None:
    """REBOOT SURVIVAL's fleet half, piece 4: a
    DM filed while the trigger's own immediate leg never ran (the exact outage shape:
    send() itself either never attempted dispatch, or its attempt raised and was caught,
    per cli.py's/mcp_server.py's own "the send already committed; confess" handling) must
    still leave the message genuinely QUEUED (read_at IS NULL, no delivered_at) rather
    than silently dropped, and `_dms_with_unread`'s own population query is unconditional
    on how the row got there: this pins that the worker's own backstop sweep, ticking
    with the trigger now armed, picks it up and dispatches on its very first pass, no
    special-casing needed. Send WITHOUT ever calling dispatch_dm (simulating an outage
    where the immediate leg never ran at all, the harshest case), the message sits
    exactly as `_dms_with_unread` will find it."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await _dm_to_owner(actions)  # send_message alone, no dispatch_dm call yet
    row = await actions.pool.fetchrow(
        "SELECT read_at FROM fleet_messages WHERE id=$1", msg_id)
    assert row is not None and row["read_at"] is None  # genuinely still queued

    calls: list[tuple[str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, kw))

    report = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense)), spawn=_spawn,
        windows=_no_windows)
    assert report["woke"] == 1
    assert report["resumed"] == 1
    assert calls and calls[0][1].get("resume_session") == FULL_SID


async def test_an_fyi_dm_never_wakes(actions: Actions, tmp_path: Path) -> None:
    """The grammar's loop terminator: grade='fyi' + ack settles WITHOUT minting a turn, so
    an fyi never resumes anybody. It waits, readable, for the addressee's own next turn;
    this is what ends an A<->B exchange instead of ping-ponging it to the ceiling."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    out = await send_message(actions.pool, from_agent="agent:sender", from_project="other",
                             to_agent="agent:abcd1234", body="done, for the record",
                             grade="fyi")

    async def _boom(repo: str, prompt: str, **kw: Any) -> None:
        raise AssertionError("an fyi minted a turn, the terminator failed")

    st = _settings(enabled=True, sense=str(sense))
    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=int(out["id"]),
                          sender="agent:sender", settings=st, spawn=_boom,
                          windows=_no_windows)
    assert d["mode"] == "queued-fyi"
    # and the backstop sweep holds the same line, two callers, one grammar
    rep = await trigger_mail_tick(actions, settings=st, spawn=_boom)
    assert rep.get("dm_queued") == 1 and rep["woke"] == 0
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


# ═══ BROADCAST DISPATCH (task #151): the same grammar as a DM's, applied to the surface it
# was missing from. Before this, send(to=<project>) filed a message and computed wake_status
# (a status STRING, no dispatch): the only push was the worker sweep, up to ~60s later, and
# NONE at all under poke-only with no open window. One live specimen's broadcast once
# reached nobody the night of the second history rewrite for exactly this reason.

async def test_an_fyi_broadcast_never_wakes(actions: Actions) -> None:
    """The DM lane's own loop terminator, extended to broadcasts: grade='fyi' never wakes
    anyone. Before this, grade had ZERO effect on broadcast dispatch, confirmed empirically
    (not assumed) before building on it; it only ever reached mount()/orient()'s unread
    count."""
    a = await actions.create_or_find_object("Agent", "agent:demo", "session")
    await actions.assert_property(a, "project", "demo", "session", NOW, 0.9)
    await save_mount(actions.pool, job_dir="/test/seed/demo", agent_id="agent:seed-demo",
                     project="demo", cwd="/test", model=None, session_key=None, alive=False)
    out = await send_message(actions.pool, from_agent="agent:other", from_project="other",
                             to_project="demo", body="fyi: filed for the record",
                             grade="fyi")

    async def _boom(repo: str, prompt: str, **kw: Any) -> None:
        raise AssertionError("an fyi broadcast minted a turn, the terminator failed")

    st = _settings(enabled=True)
    d = await dispatch_broadcast(actions.pool, project="demo", msg_id=int(out["id"]),
                                 sender="agent:other", settings=st, spawn=_boom,
                                 windows=_no_windows)
    assert d["mode"] == "queued-fyi"
    # and the backstop sweep holds the same line, two callers, one rule, no drift
    rep = await trigger_mail_tick(actions, settings=st, spawn=_boom)
    assert rep["fyi_queued"] == 1 and rep["woke"] == 0
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


async def test_dispatch_broadcast_pokes_an_open_window_immediately(actions: Actions) -> None:
    """The IMMEDIATE LEG (send()'s own call, simulated here directly): an 'ask'-graded
    broadcast pokes a manager-hosted open window on arrival, not up to ~60s later at the
    sweep's own pace: the exact latency gap task #151 exists to close."""
    await _agent_with_mail(actions)
    await save_mount(actions.pool, job_dir="/x/jobs/beefcafe", agent_id="agent:demo",
                     project="demo", cwd="/repo/demo", model=None,
                     session_key="whisper:beefcafe", alive=False)
    wins = [{"name": "w-demo", "alive": True, "idle_seconds": 999.0,
             "job_dir": "/x/jobs/beefcafe"}]
    pokes: list[tuple[str, str]] = []

    async def _windows() -> list[dict[str, Any]]:
        return wins

    async def _poke(name: str, text: str, *, dedup: str, min_idle: int) -> dict[str, Any]:
        pokes.append((name, dedup))
        return {"poked": name}

    msg_id = await actions.pool.fetchval(
        "SELECT id FROM fleet_messages WHERE to_project='demo' ORDER BY id DESC LIMIT 1")
    d = await dispatch_broadcast(actions.pool, project="demo", msg_id=msg_id,
                                 sender="agent:other", settings=_settings(enabled=True),
                                 windows=_windows, poke=_poke)
    assert d["mode"] == "poked"
    assert pokes[0][0] == "w-demo" and pokes[0][1] == f"msg:{msg_id}"
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes ORDER BY id DESC LIMIT 1") == "poke"
    # the sweep, arriving after, finds the SAME cause already poked (shared dedup key,
    # msg:<id>, between the immediate leg and the backstop): it does not double-poke, but
    # the message is STILL unsettled, so the pre-poke ladder escalates past the window to
    # mint, exactly as it already did before this refactor (unchanged behavior, preserved)
    async def _poke_deduped(name: str, text: str, *, dedup: str,
                            min_idle: int) -> dict[str, Any]:
        return {"poked": name, "deduped": True}

    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True, sense=""),
                                  spawn=_spawn, windows=_windows, poke=_poke_deduped)
    assert rep["poked"] == 0 and rep["woke"] == 1  # escalated to the mint rung
    assert spawned == ["/repo/demo"]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE mode='poke'") == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE mode='mint'") == 1


async def test_a_paused_seat_queues_and_release_drains(
    actions: Actions, tmp_path: Path
) -> None:
    """Wall #2, the explicit arm: paused=true holds the push lane (mail queues, nothing
    lost); the newest paused assertion wins, so a release is just the next word, and the
    queued DM rides the very next dispatch."""
    from datetime import timedelta

    from src.orchestrator.offices import _default_office_root

    sense = await _stale_resumable_owner(actions, tmp_path)
    real_office = str(_default_office_root() / "staleowner")
    msg_id = await _dm_to_owner(actions)
    a = await actions.create_or_find_object("Agent", "agent:abcd1234", "agent:abcd1234")
    await actions.assert_property(a, "paused", True, "agent:abcd1234", NOW, 0.9,
                                  evidence_class="self_declared")
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    st = _settings(enabled=True, sense=str(sense))
    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender", settings=st, spawn=_spawn,
                          windows=_no_windows)
    assert d["mode"] == "queued-paused" and spawned == []
    # the release: a NEWER paused=false, latest word wins, the queue drains
    await actions.assert_property(a, "paused", False, "agent:abcd1234",
                                  NOW + timedelta(hours=1), 0.9,
                                  evidence_class="self_declared")
    d2 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows)
    assert d2["mode"] == "resumed" and spawned == [real_office]


async def test_needs_input_gates_until_the_operators_word(
    actions: Actions, tmp_path: Path
) -> None:
    """Wall #2, the implicit arm: a seat whose last act was asking the human (an undismissed
    decision/hands brief, quiet since) is not peer-resumable: its mail queues; the human's
    word is the release. Peer mail must never preempt the operator's judgment."""
    from src.orchestrator.offices import _default_office_root

    sense = await _stale_resumable_owner(actions, tmp_path)  # mount last_seen: 1h ago
    real_office = str(_default_office_root() / "staleowner")
    brief = await send_message(actions.pool, from_agent="agent:abcd1234",
                               from_project="demo", to_project=OPERATOR_ADDR,
                               body="which retraction tier?", desk_kind="decision")
    msg_id = await _dm_to_owner(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    st = _settings(enabled=True, sense=str(sense))
    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender", settings=st, spawn=_spawn,
                          windows=_no_windows)
    assert d["mode"] == "queued-needs-input" and "decision" in d["detail"]
    assert spawned == []
    # the human answers (the brief is dismissed): the gate lifts, the queue drains
    await actions.pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, read_at) "
        "VALUES ($1,$2,now())", int(brief["id"]), OPERATOR_ADDR)
    d2 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows)
    assert d2["mode"] == "resumed" and spawned == [real_office]


async def test_a_47_day_old_desk_brief_no_longer_gates(
    actions: Actions, tmp_path: Path
) -> None:
    """A post-reboot finding: a 47-day-old
    undismissed desk brief braked a September peer DM as 'queued-needs-input': the
    'kept working since' check is itself reboot-fragile (agent_mounts can read no fresh
    row for a lineage that hasn't remounted yet this boot), so a brief old enough is
    dropped as a gate outright, 7 days per a proposed default,
    whatever the mount-freshness check alone could or couldn't prove."""
    from src.orchestrator.offices import _default_office_root

    sense = await _stale_resumable_owner(actions, tmp_path)  # mount last_seen: 1h ago
    real_office = str(_default_office_root() / "staleowner")
    await send_message(actions.pool, from_agent="agent:abcd1234", from_project="demo",
                       to_project=OPERATOR_ADDR, body="which retraction tier?",
                       desk_kind="decision")
    await actions.pool.execute(
        "UPDATE fleet_messages SET created_at = now() - interval '47 days' "
        "WHERE to_project=$1 AND desk_kind='decision'", OPERATOR_ADDR)
    msg_id = await _dm_to_owner(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    st = _settings(enabled=True, sense=str(sense))
    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender", settings=st, spawn=_spawn,
                          windows=_no_windows)
    assert d["mode"] != "queued-needs-input"
    assert d["mode"] == "resumed" and spawned == [real_office]


async def test_an_fyi_brief_never_gates(actions: Actions, tmp_path: Path) -> None:
    """Only decision/hands briefs mean 'awaiting the word': a loop-closed fyi on the desk
    must not freeze its sender's inbound lane."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    await send_message(actions.pool, from_agent="agent:abcd1234", from_project="demo",
                       to_project=OPERATOR_ADDR, body="shipped, fyi", desk_kind="fyi")
    msg_id = await _dm_to_owner(actions)

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        pass

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows)
    assert d["mode"] == "resumed"


async def test_a_seat_addressed_dm_reaches_the_holder(
    actions: Actions, tmp_path: Path
) -> None:
    """THE SEAT GAP, closed: name-addressed mail stores the SEAT id (B2), and the old DM
    lane matched it against agent_mounts verbatim: so every seat-BOUND addressee (the
    whole charter pattern, one real case) was silently pull-only. The dispatch now
    resolves seat → holder → living head before looking for a session."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    seat = await actions.create_or_find_object("Seat", "seat:demo-charter", "session")
    holder = await actions.create_or_find_object("Agent", "agent:abcd1234", "session")
    await actions.create_link(holder, seat, "holds", "session", NOW, 0.9)
    await _office(actions, "seat:demo-charter", "/repo/demo")
    out = await send_message(actions.pool, from_agent="agent:sender", from_project="other",
                             to_agent="seat:demo-charter", body="for the seat")
    calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    d = await dispatch_dm(actions.pool, addressee="seat:demo-charter",
                          msg_id=int(out["id"]), sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows)
    assert d["mode"] == "resumed"
    assert calls[0].get("resume_session") == FULL_SID  # the HOLDER's session, via the seat


async def test_the_per_seat_brake_holds_the_spiral(
    actions: Actions, tmp_path: Path
) -> None:
    """Wall #4: an A<->B ping-pong is legal work until a brake says otherwise, and the
    per-SEAT hourly cap is the brake that says it (the per-project cap can't see a spiral
    burning one seat inside a busy project)."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    m1 = await _dm_to_owner(actions)
    out2 = await send_message(actions.pool, from_agent="agent:other", from_project="other",
                              to_agent="agent:abcd1234", body="a different word")
    calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    st = _settings(enabled=True, sense=str(sense), seat_cap=1)
    d1 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=m1,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows)
    d2 = await dispatch_dm(actions.pool, addressee="agent:abcd1234",
                           msg_id=int(out2["id"]), sender="agent:other", settings=st,
                           spawn=_spawn, windows=_no_windows)
    assert d1["mode"] == "resumed" and d2["mode"] == "braked"
    assert len(calls) == 1


async def test_the_grace_collapses_a_burst(actions: Actions, tmp_path: Path) -> None:
    """Three DMs land in one minute: the FIRST resumes; the rest see the wake in flight and
    ride along: the resumed session reads its WHOLE box, so nothing needs a second spawn."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    m1 = await _dm_to_owner(actions)
    out2 = await send_message(actions.pool, from_agent="agent:third", from_project="other",
                              to_agent="agent:abcd1234", body="me too")
    calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    st = _settings(enabled=True, sense=str(sense), grace=300)
    d1 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=m1,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows)
    d2 = await dispatch_dm(actions.pool, addressee="agent:abcd1234",
                           msg_id=int(out2["id"]), sender="agent:third", settings=st,
                           spawn=_spawn, windows=_no_windows)
    assert d1["mode"] == "resumed" and d2["mode"] == "queued-wake-in-flight"
    assert len(calls) == 1


async def test_stuck_wake_in_flight_asks_finds_what_the_dm_lane_cannot_reach(
    actions: Actions,
) -> None:
    """A live specimen:
    `_dms_with_unread`'s DISTINCT ON returns only the SINGLE oldest unread DM per
    addressee: a message that already spent its one-time wake (an EARLIER, unrelated
    DM to the same addressee, dispatched 'resumed'/'nudged'/'poked', still reads
    `read_at IS NULL` forever, since a push delivery never marks it or rows
    message_recipients) permanently occupies that slot, so a genuinely-still-pending
    'queued-wake-in-flight' ask queued BEHIND it is never reachable by the ordinary DM
    lane again. This query goes at `dispatch_mode` directly and finds it anyway, once
    its own grace window has elapsed."""
    m_blocker = await send_message(actions.pool, from_agent="agent:sender",
                                   from_project="other", to_agent="agent:abcd1234",
                                   body="already spent its one wake")
    await actions.pool.execute(
        "UPDATE fleet_messages SET dispatch_mode='resumed', dispatch_mode_at=now() "
        "WHERE id=$1", int(m_blocker["id"]))
    m_stuck = await send_message(actions.pool, from_agent="agent:third",
                                 from_project="other", to_agent="agent:abcd1234",
                                 grade="ask", body="rode a wake that never woke it")
    stuck_id = int(m_stuck["id"])
    await actions.pool.execute(
        "UPDATE fleet_messages SET dispatch_mode='queued-wake-in-flight', "
        "dispatch_mode_at=now() - interval '10 minutes' WHERE id=$1", stuck_id)

    # too soon (grace hasn't elapsed): not yet a candidate
    assert await _stuck_wake_in_flight_asks(actions.pool, grace_secs=3600) == []
    # grace elapsed: found, by exact id: the blocker (dispatch_mode='resumed', not
    # 'queued-wake-in-flight') is never a candidate at all, whatever its own age
    found = await _stuck_wake_in_flight_asks(actions.pool, grace_secs=300)
    assert found == [("agent:abcd1234", stuck_id, "agent:third")]


async def test_trigger_mail_tick_escalates_a_stuck_wake_in_flight_ask(
    actions: Actions, tmp_path: Path,
) -> None:
    """End to end, with a fake daemon state: a resumable owner
    whose wake-in-flight ask has aged past grace gets escalated to a real wake by the
    sweep's new lane, and the escalation is reported and the new verdict persisted."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    m1 = await _dm_to_owner(actions)
    m2 = await send_message(actions.pool, from_agent="agent:third", from_project="other",
                            to_agent="agent:abcd1234", grade="ask",
                            body="rode m1's wake, never got its own turn")
    m2_id = int(m2["id"])
    calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    st = _settings(enabled=True, sense=str(sense), grace=300)
    d1 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=m1,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows)
    d2 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=m2_id,
                           sender="agent:third", settings=st, spawn=_spawn,
                           windows=_no_windows)
    assert d1["mode"] == "resumed" and d2["mode"] == "queued-wake-in-flight"
    assert len(calls) == 1  # only m1 actually spawned so far

    # simulate time passing: m1's own wake (the thing m2 was riding) and m2's own
    # verdict both age past the 300s grace window: nothing else changes
    await actions.pool.execute(
        "UPDATE agent_wakes SET woke_at = now() - interval '10 minutes'")
    await actions.pool.execute(
        "UPDATE fleet_messages SET dispatch_mode_at = now() - interval '10 minutes' "
        "WHERE id=$1", m2_id)

    report = await trigger_mail_tick(actions, settings=st, spawn=_spawn,
                                     windows=_no_windows)
    assert report["wake_in_flight_reevaluated"] == 1
    assert report["wake_in_flight_escalated"] == 1
    assert len(calls) == 2  # m2 now genuinely spawned its own turn
    persisted = await actions.pool.fetchval(
        "SELECT dispatch_mode FROM fleet_messages WHERE id=$1", m2_id)
    assert persisted != "queued-wake-in-flight"


async def test_a_dm_resume_never_pins_the_triage_model(
    actions: Actions, tmp_path: Path
) -> None:
    """A DM resume continues a REAL seat's own session: osiris_wake_model (the haiku
    triage pin) must NOT ride it: that would be a silent model downgrade of a working seat
    (the rug-pull class). The DM lane has its own knob, empty by default."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    await _dm_to_owner(actions)
    calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense),
                                    wake_model="claude-haiku-4-5-20251001"),
        spawn=_spawn)
    assert rep["resumed"] == 1
    assert calls[0].get("model") is None  # the seat's own default, never the triage pin

# ═══ the daemon-reply rung: the VISIBLE hop leads the sequence ═══


async def test_the_daemon_reply_rung_leads_and_wears_the_envelope(
    actions: Actions, tmp_path: Path
) -> None:
    """The ghost problem's fix, operator-confirmed 2026-07-20: a daemon-held addressee is
    NUDGED through the harness daemon (the front renders daemon-owned turns) instead of
    resumed by a second process, and the injected turn is a CUTE LITTLE MAIL: full
    attribution (who, to whom, which message, what grade) readable at the transcript
    level, plus a body preview."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    out = await send_message(actions.pool, from_agent="agent:sender", from_project="other",
                             to_agent="agent:abcd1234", body="the colorbar shipped, verify",
                             grade="ask")
    msg_id = int(out["id"])
    nudges: list[tuple[dict[str, Any], str]] = []

    async def _jobs(ids: set) -> dict[str, Any] | None:
        assert "abcd1234" in ids            # matched via the door name AND the sid prefix
        return {"short": "abcd1234", "sessionId": FULL_SID, "name": "[D] Demo",
                "_sock": "/nowhere"}

    async def _nudge(job: dict[str, Any], text: str) -> bool:
        nudges.append((job, text))
        return True

    async def _agents_json() -> list[dict[str, Any]]:
        return [{"id": "abcd1234", "sessionId": FULL_SID, "cwd": "/wherever"}]

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        raise AssertionError("a daemon-held addressee must be nudged, never resumed")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, jobs=_jobs, nudge=_nudge,
                          agents_json=_agents_json)
    assert d["mode"] == "nudged" and "[D] Demo" in d["detail"]
    job, text = nudges[0]
    assert f"DM #{msg_id}" in text                      # which message
    assert "agent:sender" in text                        # who
    assert "agent:abcd1234" in text                      # to whom
    assert "ask: needs your reply or act" in text         # what grade
    assert "the colorbar shipped" in text                # the preview
    assert f"send(reply_to={msg_id})" in text            # how to settle
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes ORDER BY id DESC LIMIT 1") == "dm-reply"


async def test_daemon_rung_excludes_a_stale_mount_row_from_its_candidate_ids(
    actions: Actions, tmp_path: Path
) -> None:
    """An independent measurement found the daemon rung used to
    pull EVERY job_dir this agent_id has ever mounted at into `jobs(ids)`'s own candidate
    set with no freshness check: a long-dead mount row could still hand its job_dir to
    the daemon's out-of-band registry, risking wrong-body delivery if it separately still
    listed a session under that id. A mount row older than the shared liveness window
    (mounts.LIVENESS_WINDOW_MINUTES) must never reach the daemon rung's own `ids`."""
    from src.orchestrator import mounts

    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "deadbeef1"),
                            agent_id="agent:staledoor", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour' "
        "WHERE agent_id='agent:staledoor'")
    msg_id = int((await send_message(
        actions.pool, from_agent="agent:sender", from_project="other",
        to_agent="agent:staledoor", body="is anybody home", grade="ask"))["id"])

    seen_ids: set[str] = set()

    async def _jobs(ids: set) -> None:
        seen_ids.update(ids)
        return None

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        return None

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("no job was found, nudge must never be called")

    await dispatch_dm(actions.pool, addressee="agent:staledoor", msg_id=msg_id,
                      sender="agent:sender", settings=_settings(enabled=True, sense=""),
                      spawn=_spawn, windows=_no_windows, jobs=_jobs, nudge=_boom)
    assert "deadbeef1" not in seen_ids


async def test_daemon_rung_includes_a_fresh_mount_row_in_its_candidate_ids(
    actions: Actions, tmp_path: Path
) -> None:
    """The control case for the freshness filter above: a mount row within the shared
    liveness window still reaches the daemon rung's own candidate ids, unchanged."""
    from src.orchestrator import mounts

    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "freshdoor1"),
                            agent_id="agent:freshdoor", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)
    msg_id = int((await send_message(
        actions.pool, from_agent="agent:sender", from_project="other",
        to_agent="agent:freshdoor", body="is anybody home", grade="ask"))["id"])

    seen_ids: set[str] = set()

    async def _jobs(ids: set) -> None:
        seen_ids.update(ids)
        return None

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        return None

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("no job was found, nudge must never be called")

    await dispatch_dm(actions.pool, addressee="agent:freshdoor", msg_id=msg_id,
                      sender="agent:sender", settings=_settings(enabled=True, sense=""),
                      spawn=_spawn, windows=_no_windows, jobs=_jobs, nudge=_boom)
    assert "freshdoor1" in seen_ids


async def test_a_dark_daemon_falls_open_to_the_resume_lane(
    actions: Actions, tmp_path: Path
) -> None:
    """Undocumented internals never strand a message: a daemon that refuses the nudge
    (version seam, dead socket, missing key) writes NO ledger row and the dispatch falls
    straight through to the resume fallback."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await _dm_to_owner(actions)
    calls: list[dict[str, Any]] = []

    async def _jobs(ids: set) -> dict[str, Any]:
        return {"short": "abcd1234", "sessionId": FULL_SID, "_sock": "/nowhere"}

    async def _nudge(job: dict[str, Any], text: str) -> bool:
        return False                                     # EAUTH / EPROTO / dead socket

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append(kw)

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, jobs=_jobs, nudge=_nudge)
    assert d["mode"] == "resumed" and calls[0].get("resume_session") == FULL_SID
    modes = [r["mode"] for r in await actions.pool.fetch(
        "SELECT mode FROM agent_wakes ORDER BY id")]
    assert modes == ["dm-resume"]                        # no dm-reply row for the failure


async def test_a_nudged_message_is_never_renudged(
    actions: Actions, tmp_path: Path
) -> None:
    """Once per message covers the nudge lane too: a second dispatch (the sweep after the
    send leg, or a redelivery) skips: the envelope already knocked once."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await _dm_to_owner(actions)

    async def _jobs(ids: set) -> dict[str, Any]:
        return {"short": "abcd1234", "sessionId": FULL_SID, "_sock": "/nowhere"}

    async def _nudge(job: dict[str, Any], text: str) -> bool:
        return True

    async def _agents_json() -> list[dict[str, Any]]:
        return [{"id": "abcd1234", "sessionId": FULL_SID, "cwd": "/wherever"}]

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        raise AssertionError("nothing may spawn in this test")

    st = _settings(enabled=True, sense=str(sense))
    d1 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows, jobs=_jobs, nudge=_nudge,
                           agents_json=_agents_json)
    d2 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows, jobs=_jobs, nudge=_nudge,
                           agents_json=_agents_json)
    assert d1["mode"] == "nudged" and d2["mode"] == "skipped-once-per-message"
    # RECEIPT HONESTY: 'nudged' means the daemon
    # ACCEPTED the injection, never that the turn already ran, 'landed as X's next turn'
    # overclaimed a confirmed outcome from a bare queue success (a related
    # distinction)
    assert "ACCEPTED" in d1["detail"] and "landed as" not in d1["detail"]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE message_id=$1", msg_id) == 1


# ═══ the third state (task #176, 2026-08-18): the daemon accepted, but nobody confirmed
# home: it must be able to say I don't know, applied to dispatch_dm's
# own strongest-looking receipt ═══

async def test_a_nudge_with_no_confirmed_listener_is_queued_not_nudged(
    actions: Actions, tmp_path: Path
) -> None:
    """The daemon's {ok:true} means it ACCEPTED the envelope into its own queue, never that
    a live reader is there: a job it still lists after the body exited, or one that
    outlived the daemon's own generation, both accept with nobody home. When `claude agents
    --json` shows no matching session-shaped body, the receipt must say UNKNOWN
    (queued-no-listener), never the confident 'nudged' a caller would read as delivered."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await _dm_to_owner(actions)

    async def _jobs(ids: set) -> dict[str, Any]:
        return {"short": "abcd1234", "sessionId": FULL_SID, "_sock": "/nowhere"}

    async def _nudge(job: dict[str, Any], text: str) -> bool:
        return True

    async def _agents_json() -> list[dict[str, Any]]:
        return []  # the daemon's job list disagrees with the harness's own live roster

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        raise AssertionError("nothing may spawn in this test")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, jobs=_jobs, nudge=_nudge,
                          agents_json=_agents_json)
    assert d["mode"] == "queued-no-listener"
    assert "UNKNOWN" in d["detail"] and "claude agents --json" in d["detail"]


async def test_queue_semantics_are_unchanged_by_the_listener_check(
    actions: Actions, tmp_path: Path
) -> None:
    """The explicit guardrail (task #176): the third state is a RECEIPT change only. The
    agent_wakes ledger row still lands on a bare {ok:true} regardless of the listener
    check, so the once-per-message brake still fires on a second dispatch: at-least-once
    across successions stays correct, unchanged by this fix."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await _dm_to_owner(actions)

    async def _jobs(ids: set) -> dict[str, Any]:
        return {"short": "abcd1234", "sessionId": FULL_SID, "_sock": "/nowhere"}

    async def _nudge(job: dict[str, Any], text: str) -> bool:
        return True

    async def _agents_json() -> list[dict[str, Any]]:
        return []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        raise AssertionError("nothing may spawn in this test")

    st = _settings(enabled=True, sense=str(sense))
    d1 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows, jobs=_jobs, nudge=_nudge,
                           agents_json=_agents_json)
    d2 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows, jobs=_jobs, nudge=_nudge,
                           agents_json=_agents_json)
    assert d1["mode"] == "queued-no-listener" and d2["mode"] == "skipped-once-per-message"
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes WHERE message_id=$1", msg_id) == "dm-reply"
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE message_id=$1", msg_id) == 1


async def test_confirm_listener_matches_by_short_full_or_prefix_session_id() -> None:
    """`_confirm_listener` matches a job the SAME way `claude_daemon.job_for` does: short
    id, full session id, or its 8-char prefix, so the two can never disagree about which
    identity means the same body."""
    from src.orchestrator.trigger import _confirm_listener

    async def _rows_by_short() -> list[dict[str, Any]]:
        return [{"id": "abcd1234", "cwd": "/x"}]

    async def _rows_by_full_sid() -> list[dict[str, Any]]:
        return [{"sessionId": FULL_SID, "cwd": "/x"}]

    async def _rows_no_match() -> list[dict[str, Any]]:
        return [{"id": "ffffffff", "sessionId": "ffffffff-0000-0000-0000-000000000000"}]

    job = {"short": "abcd1234", "sessionId": FULL_SID}
    assert await _confirm_listener(job, _rows_by_short) is True
    assert await _confirm_listener(job, _rows_by_full_sid) is True
    assert await _confirm_listener(job, _rows_no_match) is False


async def test_confirm_listener_fails_open_on_a_read_error() -> None:
    """An `agents_json` read failure (harness version seam, transient error) reads as
    'cannot confirm': False, never a raised exception that would strand the caller."""
    from src.orchestrator.trigger import _confirm_listener

    async def _broken() -> list[dict[str, Any]]:
        raise TimeoutError("harness CLI hung")

    assert await _confirm_listener({"short": "abcd1234"}, _broken) is False


async def test_mail_settled_by_a_successor_is_never_phantom_nudged(
    actions: Actions, tmp_path: Path
) -> None:
    """The per-agent-id read-state class, third bite: the deliverable query
    keys settlement on the EXACT addressed id, so a DM to an old generation that the LIVING
    HEAD already settled reads deliverable forever: caught live when the lane's first
    unsolicited delivery knocked on its own builder's window with mail settled days
    earlier. The dispatch now checks settlement lineage-wide and answers 'settled'."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    # a DM addressed to the OLD generation of the lineage the fixture's head belongs to
    out = await send_message(actions.pool, from_agent="agent:sender", from_project="other",
                             to_agent="agent:abcd1234-ii", body="old ask, long since done")
    msg_id = int(out["id"])
    # ...settled by a DIFFERENT generation of the same lineage (the living head)
    await actions.pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, read_at) "
        "VALUES ($1,$2,now())", msg_id, "agent:abcd1234-iv")

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("settled mail must never wake anything")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234-ii", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_boom, windows=_no_windows, jobs=_boom, nudge=_boom)
    assert d["mode"] == "settled"
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0

async def test_a_crossed_registry_never_leaks_the_envelope_or_the_resume(
    actions: Actions, tmp_path: Path
) -> None:
    """THE LEAK FIX (2026-07-20): the registry needed to resolve
    the actual agent currently connected, not by registry timestamp: the statusline pump
    keeps a stale claimant row eternally fresh. The RESIDENT'S SIGNATURE decides: the
    newest signed osiris act in the session's own append-only transcript. When the
    registry's own record leads to a session whose signatures name a DIFFERENT mind (a
    real misdelivery), BOTH the nudge and the resume refuse: the mail stays
    pull-only, and not one preview character reaches the foreign window."""
    import os
    import time as _time

    from src.orchestrator import mounts

    # the registry claims agent:abcd1234 lives at this door...
    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    # ...but the session's own signed testimony names a STRANGER
    signed = ('{"type":"user","toolUseResult":'
              '"{\\"sent\\":9,\\"from\\":\\"agent:zzstranger-ix\\"}"}\n')
    t.write_bytes(signed.encode())
    old = _time.time() - 3600
    os.utime(t, (old, old))
    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "abcd1234"),
                            agent_id="agent:abcd1234", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    await _seat_and_graph_session(actions)
    msg_id = await _dm_to_owner(actions)

    async def _jobs(ids: set) -> dict[str, Any]:
        return {"short": "abcd1234", "sessionId": FULL_SID, "_sock": "/nowhere"}

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a crossed door must never be nudged or resumed")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_boom, windows=_no_windows, jobs=_jobs, nudge=_boom)
    assert d["mode"] == "resume-refused-crossed-registry" and "crossed" in d["detail"]
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


async def test_an_absent_transcript_refuses_as_unknown_never_as_a_found_mismatch(
    actions: Actions, tmp_path: Path
) -> None:
    """The 18th specimen of a related fix: an EMPTY lookup (a transcript
    exists and resumes fine, but nothing SIGNED appears anywhere in it: no whisper, no
    mount, no send) must never be rendered with the same words as a POSITIVE identity
    mismatch. Same registry shape as the crossed-registry test above (a mounted door, a
    resumable candidate) but the transcript's content is unsigned harness noise, not a
    signature naming a stranger: so this must refuse as `resident-unknown`, never
    `crossed-registry`, and the detail text must never claim a different mind was found.

    ONE HOP BACK, deliberately (task #178's own zero-hop graph door would otherwise
    legitimately RESUME this exact unsigned-but-graph-corroborated shape at hop 0, see
    `test_launch_harness_lane_resumes_zero_hop_unsigned_via_the_graph_door_not_a_refusal`
    for that composed case; this test's own point is unrelated to hop count, so it moves
    one hop back to stay clear of that door and keep testing what it always tested)."""
    import os
    import time as _time

    from src.orchestrator import mounts

    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    # a real, resumable transcript, just nothing signed anywhere in it
    t.write_bytes(b'{"type":"assistant","text":"just harness chrome, nothing signed"}\n')
    old = _time.time() - 3600
    os.utime(t, (old, old))
    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "abcd1234"),
                            agent_id="agent:abcd1234", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    await _seat_and_graph_session(actions)
    # a fresher-mounted successor with NO graph session of its own: wakeable_identity
    # picks it as wake_target (freshest mount), pushing the unsigned transcript above to
    # hop 1 in `_lineage_resume_candidate`'s own walk from there
    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "abcd1234ii"),
                            agent_id="agent:abcd1234-ii", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)
    succ = await actions.create_or_find_object("Agent", "agent:abcd1234-ii", "test")
    await actions.assert_property(succ, "succeeded_from", "agent:abcd1234", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    msg_id = await _dm_to_owner(actions)

    async def _jobs(ids: set) -> dict[str, Any]:
        return {"short": "abcd1234", "sessionId": FULL_SID, "_sock": "/nowhere"}

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("an unresolved identity must never be nudged or resumed")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_boom, windows=_no_windows, jobs=_jobs, nudge=_boom)
    assert d["mode"] == "resume-refused-resident-unknown"
    assert "signed testimony names a different mind" not in d["detail"]
    assert "crossed-registry" not in d["detail"]
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


async def test_resume_guard_returns_gate_token_directly_not_prose_to_reparse(
    actions: Actions, tmp_path: Path
) -> None:
    """The tri-state fix itself, at the unit level: `_resume_guard` must hand back
    "crossed-registry"/"resident-unknown" as a structured token, never leave the caller
    to string-match its detail text to recover the distinction (that reinvents the exact
    bug this fix closes)."""
    from src.orchestrator import mounts

    sense = tmp_path / "projects"
    sense.mkdir(parents=True, exist_ok=True)
    st = _settings(enabled=True, sense=str(sense))
    resume = (FULL_SID, "/repo/demo", 0.0, "abcd1234")
    # no transcript anywhere under `sense` for FULL_SID: the unknown arm
    gate, detail = await trigger_module._resume_guard(
        actions.pool, resume, "agent:abcd1234", seat_id=None, st=st)
    assert gate == "resident-unknown"
    assert detail is not None
    assert "signed testimony names a different mind" not in detail

    # now a transcript that positively names a stranger: the mismatch arm
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    t.write_bytes(b'{"type":"user","toolUseResult":'
                  b'"{\\"sent\\":1,\\"from\\":\\"agent:zzstranger-ix\\"}"}\n')
    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "abcd1234-ii"),
                            agent_id="agent:abcd1234", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)
    gate2, detail2 = await trigger_module._resume_guard(
        actions.pool, resume, "agent:abcd1234", seat_id=None, st=st)
    assert gate2 == "crossed-registry"
    assert detail2 is not None
    assert "signed testimony names a different agent" in detail2


# ═══ THE CORROBORATION FALLBACK ══════════════════════
# one parked session was provably its own: every signature in the whole transcript
# was its own lineage, but the LAST 400KB was all unsigned harness noise (away summaries,
# chrome), so the old tail-only check read it as a stranger and refused both nudge and
# resume. These tests build a transcript whose signed act sits DEEPER than the tail, prove
# the fallback finds and corroborates it, and prove the different-mind arm and the
# registry re-check still refuse when either leg fails.

def _deep_transcript_bytes(*, signed_line: bytes, total_size: int) -> bytes:
    """`signed_line` at offset 0, padded with unsigned filler out to `total_size` bytes:
    the shape of that transcript: real signed history, then a long unsigned tail."""
    assert len(signed_line) < total_size
    return signed_line + b"x" * (total_size - len(signed_line))


def test_resident_of_deeper_sync_finds_a_signature_beyond_the_tail(tmp_path: Path) -> None:
    proj = tmp_path / "-repo-demo"
    proj.mkdir(parents=True)
    signed = b'{"type":"user","toolUseResult":"{\\"sent\\":1,\\"from\\":\\"agent:abcd1234\\"}"}\n'
    t = proj / f"{FULL_SID}.jsonl"
    # total size > one tail window, so the signed line at offset 0 sits OUTSIDE the tail
    # the plain _resident_of_sync already checked, but inside the first deeper window
    t.write_bytes(_deep_transcript_bytes(signed_line=signed, total_size=500_000))
    assert trigger_module._resident_of_sync(tmp_path, FULL_SID) is None  # the OLD check misses it
    resident, path = trigger_module._resident_of_deeper_sync(tmp_path, FULL_SID)
    assert resident == "agent:abcd1234" and path == t


def test_resident_of_deeper_sync_respects_its_own_cap(tmp_path: Path) -> None:
    """Beyond `_RESIDENT_DEEP_WINDOWS` windows back, the signature is unreachable: bounded
    cost, not an unbounded scan of an arbitrarily large transcript."""
    proj = tmp_path / "-repo-demo"
    proj.mkdir(parents=True)
    signed = b'{"type":"user","toolUseResult":"{\\"sent\\":1,\\"from\\":\\"agent:abcd1234\\"}"}\n'
    t = proj / f"{FULL_SID}.jsonl"
    # tail (400KB) + 4 extra windows (1.6MB) = 2MB reachable; push the signature well past it
    t.write_bytes(_deep_transcript_bytes(signed_line=signed, total_size=2_500_000))
    resident, path = trigger_module._resident_of_deeper_sync(tmp_path, FULL_SID)
    assert resident is None and path == t  # unreachable, a clean miss, not a wrong guess


async def _mounted_deep_agent(
    actions: Actions, tmp_path: Path, *, agent_id: str = "agent:abcd1234",
    cwd: str = "/repo/demo", seat_id: str | None = None, graph_identity: bool = True,
) -> tuple[Path, Path]:
    """A registry row + a transcript whose signed act sits beyond the tail, the same deep
    shape. Returns (sense_root, transcript_path).

    `graph_identity=True` (default) ALSO seats `agent_id` with an office at `cwd` and
    asserts the graph's own `session`/`seat_generation` properties (task #178:
    dispatch_dm's resume selection reads `_lineage_resume_candidate` (graph truth),
    never `agent_mounts` alone). Callers that test `_registry_corroborates`/`_resident_*`
    DIRECTLY (never through `dispatch_dm`) pass `graph_identity=False`: those unit tests
    exercise their own `job_dir`/`seat_id` plumbing and don't need a Seat at all."""
    from src.orchestrator import mounts
    from src.orchestrator.mounts import _harness_slug

    sense = tmp_path / "projects"
    proj = sense / _harness_slug(cwd)
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    signed = ('{"type":"user","toolUseResult":'
             f'"{{\\"sent\\":1,\\"from\\":\\"{agent_id}\\"}}"}}\n').encode()
    t.write_bytes(_deep_transcript_bytes(signed_line=signed, total_size=500_000))
    job_dir = tmp_path / "jobs" / "abcd1234"
    await mounts.save_mount(actions.pool, job_dir=str(job_dir), agent_id=agent_id,
                            project="demo", cwd=cwd, model=None, session_key=None)
    if seat_id is not None:
        await actions.pool.execute(
            "UPDATE agent_mounts SET seat_id=$1 WHERE job_dir=$2", seat_id, str(job_dir))
    if graph_identity:
        from src.orchestrator.seats import bind_holder, ensure_seat
        seat = (await ensure_seat(actions, house="demo", handle=f"Deep{agent_id[-6:]}",
                                  source="test"))["seat_id"]
        await bind_holder(actions, seat_id=seat, agent_id=agent_id)
        await _office(actions, seat, cwd)
        obj = await actions.create_or_find_object("Agent", agent_id, "test")
        await actions.assert_property(obj, "seat_generation", "1", "test", NOW, 0.9,
                                      evidence_class="self_declared")
        await actions.assert_property(obj, "session", FULL_SID, "test", NOW, 0.9,
                                      evidence_class="self_declared")
    return sense, t


async def test_registry_corroborates_a_genuine_lineage_correct_deep_match(
    actions: Actions, tmp_path: Path,
) -> None:
    _sense, t = await _mounted_deep_agent(actions, tmp_path)
    job_dir = str(tmp_path / "jobs" / "abcd1234")
    assert await trigger_module._registry_corroborates(
        actions.pool, job_dir, t, "agent:abcd1234", seat_id=None)


async def test_registry_corroborates_refuses_a_reassigned_door(
    actions: Actions, tmp_path: Path,
) -> None:
    """The exact failure mode named in the design brief: stale deep history names the
    original addressee, but the CURRENT registry row for this job_dir now names someone
    else (the door was legitimately reassigned): corroboration must fail, not trust the
    stale transcript content alone."""
    _sense, t = await _mounted_deep_agent(
        actions, tmp_path, agent_id="agent:newowner")  # registry NOW says newowner
    job_dir = str(tmp_path / "jobs" / "abcd1234")
    # the deep-scan signature (baked into the transcript by the helper) still names
    # abcd1234, the registry disagrees, so corroboration must refuse FOR abcd1234
    assert not await trigger_module._registry_corroborates(
        actions.pool, job_dir, t, "agent:abcd1234", seat_id=None)


async def test_registry_corroborates_refuses_a_slug_collision(
    actions: Actions, tmp_path: Path,
) -> None:
    """An earlier instruction: dashes AND dots both fold to '-' under
    _harness_slug, so two real, different cwds can collide. A collision is corroboration
    FAILURE, never a pass on a coincidental string match."""
    from src.orchestrator import mounts

    # the addressee's own door, cwd with a DOT
    _sense, t = await _mounted_deep_agent(actions, tmp_path, cwd="/repo/demo.x")
    job_dir = str(tmp_path / "jobs" / "abcd1234")
    # a second, unrelated door whose cwd (a DASH instead of the dot) slugifies IDENTICALLY
    await mounts.save_mount(
        actions.pool, job_dir=str(tmp_path / "jobs" / "other0001"),
        agent_id="agent:someoneelse", project="demo", cwd="/repo/demo-x",
        model=None, session_key=None)
    assert not await trigger_module._registry_corroborates(
        actions.pool, job_dir, t, "agent:abcd1234", seat_id=None)


async def test_registry_corroborates_refuses_a_seat_mismatch(
    actions: Actions, tmp_path: Path,
) -> None:
    _sense, t = await _mounted_deep_agent(actions, tmp_path, seat_id="seat:wrong-one")
    job_dir = str(tmp_path / "jobs" / "abcd1234")
    assert not await trigger_module._registry_corroborates(
        actions.pool, job_dir, t, "agent:abcd1234", seat_id="seat:the-real-one")


async def test_registry_corroborates_accepts_a_null_seat_as_unsuspicious(
    actions: Actions, tmp_path: Path,
) -> None:
    """Not every agent holds a seat: a null seat_id on the registry row is not itself
    evidence against corroboration."""
    _sense, t = await _mounted_deep_agent(actions, tmp_path, seat_id=None)
    job_dir = str(tmp_path / "jobs" / "abcd1234")
    assert await trigger_module._registry_corroborates(
        actions.pool, job_dir, t, "agent:abcd1234", seat_id="seat:the-real-one")


async def test_registry_corroborates_a_same_lineage_stale_row_is_not_a_collision(
    actions: Actions, tmp_path: Path,
) -> None:
    """#184 Leg 1 follow-on, live-reproduced: a durable per-seat anchor mounts
    the SAME cwd on every generation of one lineage by construction: an earlier
    generation's own stale, never-swept row sharing that cwd is corroborating evidence,
    not ambiguity. Only a DIFFERENT lineage landing on the same slug (the existing
    `test_registry_corroborates_refuses_a_slug_collision` above) is a real collision."""
    from src.orchestrator import mounts

    _sense, t = await _mounted_deep_agent(actions, tmp_path, agent_id="agent:abcd1234-ii")
    job_dir = str(tmp_path / "jobs" / "abcd1234")
    # an EARLIER generation of the SAME lineage, same cwd (the durable-anchor shape):
    # stale, never swept, but not a stranger
    await mounts.save_mount(
        actions.pool, job_dir=str(tmp_path / "jobs" / "abcd1234-old"),
        agent_id="agent:abcd1234", project="demo", cwd="/repo/demo",
        model=None, session_key=None)
    assert await trigger_module._registry_corroborates(
        actions.pool, job_dir, t, "agent:abcd1234", seat_id=None)


async def test_resident_verdict_total_miss_corroborates_via_the_freshest_registry_row(
    actions: Actions, tmp_path: Path,
) -> None:
    """#184 Leg 1 follow-on, live-reproduced: a genuinely live seat whose
    transcript simply hasn't called an osiris tool recently (nothing signed anywhere in
    the scan, tail or deep) must not read as a stranger when the registry's own freshest
    row for this exact lineage corroborates it: the same authority the deeper-signed-act
    branch already trusts, extended to the total-miss case."""
    from src.orchestrator import mounts

    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    t.write_bytes(b'{"type":"assistant","text":"just harness chrome, nothing signed"}\n')
    job_dir = tmp_path / "jobs" / "abcd1234"
    await mounts.save_mount(actions.pool, job_dir=str(job_dir), agent_id="agent:abcd1234",
                            project="demo", cwd="/repo/demo", model=None, session_key=None)
    verdict = await trigger_module._resident_verdict(
        actions.pool, sense, FULL_SID, "agent:abcd1234", job_dir_hint=str(job_dir))
    assert verdict == "match"


async def test_resident_verdict_total_miss_refuses_when_a_fresher_successor_exists(
    actions: Actions, tmp_path: Path,
) -> None:
    """The exact case the freshness check exists for: a declared successor has ALREADY
    mounted its own, later row for this lineage: the ancestor's own registry row still
    corroborates on its own terms, but must not win over a fresher body."""
    from src.orchestrator import mounts

    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    t.write_bytes(b'{"type":"assistant","text":"just harness chrome, nothing signed"}\n')
    job_dir = tmp_path / "jobs" / "abcd1234"
    await mounts.save_mount(actions.pool, job_dir=str(job_dir), agent_id="agent:abcd1234",
                            project="demo", cwd="/repo/demo", model=None, session_key=None)
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    # a fresher successor's own mount, same lineage
    await mounts.save_mount(
        actions.pool, job_dir=str(tmp_path / "jobs" / "abcd1234ii"),
        agent_id="agent:abcd1234-ii", project="demo", cwd="/repo/demo",
        model=None, session_key=None)
    verdict = await trigger_module._resident_verdict(
        actions.pool, sense, FULL_SID, "agent:abcd1234", job_dir_hint=str(job_dir))
    assert verdict == "unknown"


async def test_dispatch_dm_resumes_the_halcyon_shaped_unsigned_tail(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE PAYOFF: a lineage-correct body whose transcript's last 400KB is all unsigned
    harness noise is no longer stranded: the deeper scan finds its own real signature,
    the registry corroborates, and the resume lane proceeds exactly as if the tail itself
    had been signed."""
    sense, _t = await _mounted_deep_agent(actions, tmp_path)
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    msg_id = await _dm_to_owner(actions)
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, kw))

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("no daemon job was found, nudge must never be reached")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "resumed"
    assert calls and calls[0][1].get("resume_session") == FULL_SID


async def test_a_dm_wakes_the_live_body_even_when_its_declared_successor_never_mounted(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE PAYOFF: reproduces the exact shape, the seat's recorded
    holder was a newer generation that had never mounted, while the bare id was the one
    actually live, mounted, and explicitly addressed. Before this fix, dispatch_dm reused
    living_head's DELIVERY answer (the declared successor) for wake eligibility too, and
    reported 'has never mounted' beside a receipt naming that same live body's fresh
    last_seen. Now the wake path resolves through wakeable_identity and reaches it."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    a = await actions.create_or_find_object("Agent", "agent:abcd1234", "agent:abcd1234")
    # the successor is MINTED (a real Agent object, exactly what mint_heir does), just
    # never mounted an OS session; lineage_head only advances past a succeeded_by pointer
    # that resolves to a real, active Agent object, so this is that specimen's actual shape
    await actions.create_or_find_object("Agent", "agent:abcd1234-ii", "agent:abcd1234")
    await actions.assert_property(a, "succeeded_by", "agent:abcd1234-ii", "agent:abcd1234",
                                  NOW, 0.9, evidence_class="self_declared")
    msg_id = await _dm_to_owner(actions)          # addressed to the raw live id, explicitly
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, kw))

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("no daemon job was found, nudge must never be reached")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "resumed"                  # not 'pull-only' beside a live mount
    assert calls and calls[0][1].get("resume_session") == FULL_SID


async def test_a_dm_still_wakes_the_true_successor_after_a_real_completed_succession(
    actions: Actions, tmp_path: Path,
) -> None:
    """Negative control at the dispatch_dm level: a NORMAL, healthy succession, the
    declared successor has itself also mounted, with its own resumable session: must
    still resolve and resume THAT successor, unchanged from before this fix. This guards
    against an overcorrection that always prefers the original body."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    a = await actions.create_or_find_object("Agent", "agent:abcd1234", "agent:abcd1234")
    await actions.create_or_find_object("Agent", "agent:abcd1234-ii", "agent:abcd1234")
    await actions.assert_property(a, "succeeded_by", "agent:abcd1234-ii", "agent:abcd1234",
                                  NOW, 0.9, evidence_class="self_declared")
    from src.orchestrator import mounts

    succ_sid = "eeee2222-0000-4000-8000-000000000000"
    succ_dir = sense / "-repo-demo"
    succ_t = succ_dir / f"{succ_sid}.jsonl"
    signed = ('{"type":"user","toolUseResult":'
              '"{\\"sent\\":1,\\"from\\":\\"agent:abcd1234-ii\\"}"}\n')
    succ_t.write_bytes(signed.encode() + b"x" * 16)
    import os
    import time as _time
    old = _time.time() - 1800  # newer than the original's -1h staleness, still not mid-turn
    os.utime(succ_t, (old, old))
    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "eeee2222"),
                            agent_id="agent:abcd1234-ii", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)
    succ_obj = await actions.create_or_find_object("Agent", "agent:abcd1234-ii", "test")
    await actions.assert_property(succ_obj, "seat_generation", "2", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(succ_obj, "session", succ_sid, "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(succ_obj, "succeeded_from", "agent:abcd1234", "test", NOW,
                                  0.9, evidence_class="self_declared")
    msg_id = await _dm_to_owner(actions)
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, kw))

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("no daemon job was found, nudge must never be reached")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "resumed"
    assert calls and calls[0][1].get("resume_session") == succ_sid  # the SUCCESSOR's session


async def test_dispatch_dm_still_refuses_when_deep_history_is_a_different_mind(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE DIFFERENT-MIND ARM STAYS UNCONDITIONAL: an unsigned tail whose deeper history
    belongs to someone else must refuse exactly like today's tail-signed crossed-registry
    case: the fallback never overrides a found disagreement, wherever it's found."""
    from src.orchestrator import mounts

    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    stranger_signed = (b'{"type":"user","toolUseResult":'
                       b'"{\\"sent\\":1,\\"from\\":\\"agent:zzstranger-ix\\"}"}\n')
    t.write_bytes(_deep_transcript_bytes(signed_line=stranger_signed, total_size=500_000))
    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "abcd1234"),
                            agent_id="agent:abcd1234", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    await _seat_and_graph_session(actions)
    msg_id = await _dm_to_owner(actions)

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a different-mind deep match must never be nudged or resumed")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "resume-refused-crossed-registry"
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


async def test_dispatch_dm_still_refuses_a_reassigned_door_end_to_end(
    actions: Actions, tmp_path: Path,
) -> None:
    """A named case: the transcript's stale deep history still says abcd1234, but
    the door has been legitimately reassigned: the CURRENT registry says otherwise. The
    fallback must not resurrect an addressee's access to a door that moved on."""
    sense, _t = await _mounted_deep_agent(
        actions, tmp_path, agent_id="agent:newowner")
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    msg_id = await _dm_to_owner(actions)

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a reassigned door must never be nudged or resumed for the "
                             "old addressee")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom)
    # the old identity's OWN mounts are gone (the door reassigned to newowner): dispatch_dm
    # never reaches the crossed-registry guard at all here, it stops one step earlier at
    # wakeable_identity finding nothing for abcd1234 specifically (#156.2 clarified the
    # label; the underlying refusal was already this, unchanged by that fix).
    assert d["mode"] == "never-mounted"
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


# ═══ IDENTITY vs OCCUPANCY (task #178) ═══════════════════════
# `_lineage_resume_candidate`/`_resume_guard` answer IDENTITY (whose session, graph-truth).
# `_resume_occupancy_gate` answers the OTHER question: is a body ALREADY SITTING there right
# now. A `-p --resume` fired beside a live body forks the mind: these pin that it never does.


async def test_dispatch_dm_refuses_to_fork_a_body_confirmed_via_agents_json(
    actions: Actions, tmp_path: Path,
) -> None:
    """`_confirm_listener` (task #176's own primitive, reused not reimplemented) matches BY
    SESSION ID: a live `claude agents --json` row naming the exact candidate session
    refuses the resume outright, before the spend, never after."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await _dm_to_owner(actions)

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a session already listed in claude agents --json must never "
                             "be forked by a second -p --resume")

    async def _agents_json(**kw: Any) -> list[dict[str, Any]]:
        return [{"sessionId": FULL_SID, "id": FULL_SID[:8], "cwd": "/somewhere/else"}]

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom,
                          agents_json=_agents_json)
    # SPLIT 2026-08-28: the addressee's OWN session being live is a DELIVERY OUTCOME
    # (it reads at its next turn), never a refusal: the collapse that made a reachable
    # seat read as unreachable. No resume is spent either way; that invariant is below.
    assert d["mode"] == "queued-live-holder"
    assert "own session" in d["detail"].lower() and FULL_SID[:8] in d["detail"]
    assert "nothing is owed" in d["detail"].lower()
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


# ═══ a live-but-IDLE self-holder gets nudged, not left to wait on an ═══
# unscheduled next turn: a `claude --bg` body with no turn coming has no way to ever read
# mail sitting in a box it never opens. The daemon reply lane (no fork) closes that gap;
# a genuinely busy addressee stays exactly as before (its own turn's end surfaces the DM).


async def test_self_branch_idle_holder_is_nudged_through_the_daemon_reply_lane(
    actions: Actions, tmp_path: Path,
) -> None:
    """A specimen: the daemon's OWN live job list can hold a session that the
    door-registry rung (agent_mounts.job_dir, possibly stale) never matches: exactly the
    shape of a session that outlived its own mount row. `jobs` is stateful here: no match
    on the door-registry rung's own first call (falls through to the resume-candidate
    path, same as `test_dispatch_dm_refuses_to_fork_a_body_confirmed_via_agents_json`
    above), a real match on the self-branch's own second, session-id-keyed call."""
    sense = await _stale_resumable_owner(actions, tmp_path)   # aged transcript == idle
    msg_id = await _dm_to_owner(actions)
    calls: list[set] = []
    nudges: list[tuple[dict[str, Any], str]] = []

    async def _jobs(ids: set) -> dict[str, Any] | None:
        calls.append(ids)
        if len(calls) < 2:
            return None                      # the door-registry rung finds nothing
        assert FULL_SID[:8] in ids            # the self-branch's own session-id lookup
        return {"short": "abcd1234", "sessionId": FULL_SID, "_sock": "/nowhere"}

    async def _nudge(job: dict[str, Any], text: str) -> bool:
        nudges.append((job, text))
        return True

    async def _agents_json() -> list[dict[str, Any]]:
        return [{"id": "abcd1234", "sessionId": FULL_SID, "cwd": "/wherever"}]

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("an idle self-holder must be nudged, never resumed or forked")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_boom, windows=_no_windows, jobs=_jobs, nudge=_nudge,
                          agents_json=_agents_json)
    assert d["mode"] == "nudged-live-holder"
    assert len(nudges) == 1
    _job, text = nudges[0]
    assert f"DM #{msg_id}" in text and "agent:abcd1234" in text
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes ORDER BY id DESC LIMIT 1") == "dm-reply"


async def test_self_branch_idle_holder_nudge_is_never_renudged(
    actions: Actions, tmp_path: Path,
) -> None:
    """Once per message covers this lane too: a redelivery or a concurrent backstop tick
    must skip the second attempt, exactly like the daemon-nudge rung's own idempotency."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await _dm_to_owner(actions)
    calls: list[set] = []

    async def _jobs(ids: set) -> dict[str, Any] | None:
        calls.append(ids)
        if len(calls) % 2 == 1:
            return None
        return {"short": "abcd1234", "sessionId": FULL_SID, "_sock": "/nowhere"}

    async def _nudge(job: dict[str, Any], text: str) -> bool:
        return True

    async def _agents_json() -> list[dict[str, Any]]:
        return [{"id": "abcd1234", "sessionId": FULL_SID, "cwd": "/wherever"}]

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("nothing may spawn in this test")

    st = _settings(enabled=True, sense=str(sense))
    d1 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender", settings=st, spawn=_boom,
                           windows=_no_windows, jobs=_jobs, nudge=_nudge,
                           agents_json=_agents_json)
    d2 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender", settings=st, spawn=_boom,
                           windows=_no_windows, jobs=_jobs, nudge=_nudge,
                           agents_json=_agents_json)
    assert d1["mode"] == "nudged-live-holder"
    assert d2["mode"] == "skipped-once-per-message"
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 1


async def test_self_branch_busy_holder_is_never_nudged_or_resumed(
    actions: Actions, tmp_path: Path,
) -> None:
    """A GENUINELY mid-turn addressee (a transcript with a fresh, moving timestamp) keeps
    its own existing outcome: its own turn's end surfaces the DM. No fork (spawn), and no
    poke either: a busy mind gets no unsolicited injection competing with its live turn."""
    import time as _time

    sense = await _stale_resumable_owner(actions, tmp_path)
    # age the mount row (still "not live" by the occupancy signals this fixture already
    # relies on) but make the TRANSCRIPT itself fresh and moving: busy, not idle.
    t = sense / "-repo-demo" / f"{FULL_SID}.jsonl"
    signed = ('{"type":"user","toolUseResult":'
              '"{\\"sent\\":1,\\"from\\":\\"agent:abcd1234\\"}"}\n')
    now_iso = datetime.now(UTC).isoformat()
    t.write_text(signed + f'{{"type":"assistant","timestamp":"{now_iso}"}}\n')
    import os
    os.utime(t, (_time.time(), _time.time()))
    msg_id = await _dm_to_owner(actions)

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a busy addressee must never be nudged, resumed, or forked")

    async def _agents_json() -> list[dict[str, Any]]:
        return [{"id": "abcd1234", "sessionId": FULL_SID, "cwd": "/wherever"}]

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom,
                          agents_json=_agents_json)
    # caught by the EARLIER mid-turn gate ("mid-turn") in this specimen, before ever
    # reaching the self-branch at all: confirmed live rather than assumed. The self-
    # branch's own busy check (this fix) is the second line of defense for the case where
    # `wake_target`'s own mount-based candidate diverges from graph_resume's own, never
    # the only one; either way the observable contract holds: no nudge, no fork.
    assert d["mode"] == "mid-turn"
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


async def test_dispatch_dm_refuses_to_fork_a_body_found_via_proc(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`census.live_bodies_by_cwd` matches BY OFFICE DIRECTORY via /proc: catches a live
    claude process sitting in the exact office this resume would land in, whatever session
    id it thinks it has (invisible to the session-id signal above, on purpose).

    Checked against the DERIVED office, never the seat's own
    `/repo/demo` anchor_cwd: THE PEN RULE means occupancy is re-verified at the ACTUAL
    materialize/spawn target, not a stale launch_cwd the materializer no longer spawns
    into (see `_resume_occupancy_gate`'s own docstring)."""
    from src.orchestrator.offices import _default_office_root

    sense = await _stale_resumable_owner(actions, tmp_path)
    real_office = str(_default_office_root() / "staleowner")
    msg_id = await _dm_to_owner(actions)

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a live process sitting at the office must never be forked "
                             "by a second -p --resume")

    from src.orchestrator import census
    monkeypatch.setattr(census, "live_bodies_by_cwd",
                        lambda **kw: {real_office: [999999]})

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom)
    # ...whereas an unidentified body in the office is the real refusal, and says the
    # reader is UNKNOWN rather than claiming a delivery nobody observed.
    assert d["mode"] == "resume-refused-occupied-foreign"
    assert "999999" in d["detail"] and real_office in d["detail"]
    assert "unknown" in d["detail"].lower()
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


async def test_dispatch_dm_resumes_when_neither_occupancy_signal_fires(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The negative control: neither signal finds anybody home; the resume proceeds
    exactly as before this gate existed."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await _dm_to_owner(actions)
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, kw))

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("no daemon job was found, nudge must never be reached")

    from src.orchestrator import census
    monkeypatch.setattr(census, "live_bodies_by_cwd", lambda **kw: {})

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "resumed"
    assert calls and calls[0][1].get("resume_session") == FULL_SID


async def test_178_acceptance_replays_the_incident_shape_neither_door_mints_a_stranger(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE ACCEPTANCE #178 NAMED: nobody had actually run it end to end
    until now. Replays the exact incident shape live: a body the HARNESS still lists
    (`claude agents --json`), whose `agent_mounts` row is SWEPT (the #178a suspend-never-
    delete sentinel, `mounts.SUSPENDED_AT`, not merely aged stale), sitting on a
    RESUMABLE transcript. Both doors that can spawn a body for this identity are exercised
    against the IDENTICAL fixture, in one test, so neither can silently disagree with the
    other:

    `osiris launch` (launch_seat): `_launch_twin_check` reads the harness roster BY CWD
    and refuses outright ("already-live"); it never even reaches the resume/identity lane,
    exactly as it must when a live body genuinely already holds the office.

    dispatch_dm's mail-wake lane: `_lineage_resume_candidate` SELECTS THE GRAPH HEAD
    first (the correct session, graph-truth, proven by the session id appearing in the
    receipt's own detail) and only THEN `_resume_occupancy_gate` (matching by session id
    this time, not cwd) refuses the fork ("resume-refused-occupied"): proving identity
    resolution and occupancy refusal are properly sequenced, not accidentally correct.

    Both doors' own spawn/resume_spawn hooks are wired to raise if ever called: the
    strongest assertion available that NO NEW GENERATION is minted by either path."""
    from src.orchestrator.mounts import SUSPENDED_AT

    sense = await _stale_resumable_owner(actions, tmp_path, bind_seat=False)
    # THE SWEEP (#178a, not just "aged stale"): the row survives (findable, never deleted)
    # but reads as dead to is_live() via the epoch sentinel: the real post-OOM-kill shape.
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen=$1 WHERE agent_id='agent:abcd1234'", SUSPENDED_AT)

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:hm-incident178",
        worker_handle="Incident178", house="osiris")
    await _office(actions, worker_seat, "/repo/demo")

    # THE HARNESS'S OWN LISTING: one row satisfies BOTH doors' distinct matchers: launch's
    # twin_check matches by `cwd`, dispatch's occupancy gate matches by `sessionId`.
    listing = _fake_agents_json([[{"sessionId": FULL_SID, "id": FULL_SID[:8],
                                  "cwd": "/repo/demo"}]])

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("the incident-shaped fixture must never mint a new generation")

    launch_receipt = await trigger_module.launch_seat(
        actions, caller="agent:hm-incident178", target=worker_seat, substrate="harness",
        settings=_settings(enabled=True, sense=str(sense)),
        spawn=_boom, agents_json=listing)
    assert launch_receipt["status"] == "already-live"
    assert launch_receipt["body_exists"] is True

    msg_id = await _dm_to_owner(actions)
    dispatch_receipt = await dispatch_dm(
        actions.pool, addressee="agent:abcd1234", msg_id=msg_id, sender="agent:sender",
        settings=_settings(enabled=True, sense=str(sense)),
        spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom, agents_json=listing)
    assert dispatch_receipt["mode"] == "queued-live-holder"
    # THE GRAPH HEAD WAS ACTUALLY SELECTED (identity), not just refused blind (occupancy):
    # the correct session id names itself in the very detail that then refuses it.
    assert FULL_SID[:8] in dispatch_receipt["detail"]
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0

    # NO NEW GENERATION: the only Agent object for this identity is still gen 1, no
    # abcd1234-ii or any other successor was ever minted by either door.
    agents = await actions.pool.fetch(
        "SELECT canonical FROM objects WHERE type='Agent' AND canonical LIKE 'agent:abcd1234%'")
    assert [r["canonical"] for r in agents] == ["agent:abcd1234"]


# ═══ THE KNOCK: wake() ═════════════════════════════
# wake() adds ONE thing dispatch_dm doesn't have: the managed_by authority gate. These tests
# pin the gate (both directions authorize, peers/unbound/seatless refuse, nothing is sent on
# a refusal) and the honest vocabulary (dispatch_dm's own "mid-turn": genuinely unread,
# renamed from its old misleading "delivered": must never surface as wake()'s "delivered").


async def _managed_pair(actions: Actions, *, worker_agent: str, manager_agent: str,
                        worker_handle: str = "Worker", manager_handle: str = "Manager",
                        house: str = "demo") -> tuple[str, str]:
    """Two seats, bound to two live agents, with an active managed_by edge worker→manager:
    the ordinary shape wake()'s gate is built for. Returns (worker_seat_id, manager_seat_id)."""
    worker_seat = (await ensure_seat(actions, house=house, handle=worker_handle,
                                     source="test"))["seat_id"]
    manager_seat = (await ensure_seat(actions, house=house, handle=manager_handle,
                                      source="test"))["seat_id"]
    await bind_holder(actions, seat_id=worker_seat, agent_id=worker_agent)
    await bind_holder(actions, seat_id=manager_seat, agent_id=manager_agent)
    w_oid = await actions.create_or_find_object("Seat", worker_seat, "test")
    m_oid = await actions.create_or_find_object("Seat", manager_seat, "test")
    await actions.create_link(w_oid, m_oid, "managed_by", "test", NOW, 0.9)
    return str(worker_seat), str(manager_seat)


# ═══ THE RATE CAP'S UNIT (2026-07-21): project vs pair ═══════════════════════
# The DM lane's rate cap used to count wakes for the whole PROJECT; a managed pair's own
# ping-pong bound is what the cap actually guards against (should_wake's own docstring),
# and a project-wide count starves ordinary supervision the moment a house holds more than
# one pair. dispatch_dm now scopes the cap to the pair when sender and target share an
# active managed_by edge, and falls back to the old project-wide count otherwise.

async def test_recent_wakes_for_pair_counts_both_directions_and_ignores_others(
    actions: Actions,
) -> None:
    from src.orchestrator.trigger import _recent_wakes_for_pair

    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:pairw01", manager_agent="agent:pairm01")
    m1 = await send_message(actions.pool, from_agent="agent:pairm01", from_project="demo",
                            to_agent=worker_seat, body="assignment")
    await actions.pool.execute(
        "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
        "VALUES ('demo','agent:pairm01',$1,'resume')", int(m1["id"]))
    m2 = await send_message(actions.pool, from_agent="agent:pairw01", from_project="demo",
                            to_agent=manager_seat, body="reply")
    await actions.pool.execute(
        "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
        "VALUES ('demo','agent:pairw01',$1,'resume')", int(m2["id"]))
    # an unrelated wake, same project, a different pair entirely: must not count
    stray = await send_message(actions.pool, from_agent="agent:stranger", from_project="demo",
                               to_agent="agent:someoneelse", body="unrelated")
    await actions.pool.execute(
        "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
        "VALUES ('demo','agent:stranger',$1,'mint')", int(stray["id"]))

    n = await _recent_wakes_for_pair(
        actions.pool, base_a="agent:pairw01", seat_a=worker_seat,
        base_b="agent:pairm01", seat_b=manager_seat, window_secs=3600)
    assert n == 2


async def test_dispatch_dm_pair_scoped_cap_ignores_unrelated_project_wakes(
    actions: Actions, tmp_path: Path,
) -> None:
    """The actual fix, wired: a managed pair's own cap check must not be starved by OTHER
    traffic sharing the same project (a measured incident): only wakes between
    THIS pair count against it."""
    sense = await _stale_resumable_owner(actions, tmp_path, bind_seat=False)
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:sender")
    await _office(actions, worker_seat, "/repo/demo")
    # flood the project-wide wake count: enough to blow a project-scoped cap of 1, none of
    # it involving this pair
    for i in range(3):
        stray = await send_message(
            actions.pool, from_agent="agent:unrelated-stranger", from_project="demo",
            to_agent=f"agent:target{i}", body="unrelated traffic")
        await actions.pool.execute(
            "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
            "VALUES ('demo','agent:unrelated-stranger',$1,'mint')", int(stray["id"]))
    msg_id = await _dm_to_owner(actions)

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        pass

    st = _settings(enabled=True, sense=str(sense), rate_cap=1)
    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender", settings=st, spawn=_spawn,
                          windows=_no_windows)
    # the pair's OWN count is 0: the unrelated flood never touches it
    assert d["mode"] == "resumed"


async def test_dispatch_dm_pair_scoped_cap_still_bounds_the_pingpong(
    actions: Actions, tmp_path: Path,
) -> None:
    """The unit changed; the bound itself must not have. A wake already recorded for THIS
    pair, within the window, still caps the next one: the ping-pong halts exactly as
    before, just scoped correctly now."""
    sense = await _stale_resumable_owner(actions, tmp_path, bind_seat=False)
    await _managed_pair(actions, worker_agent="agent:abcd1234", manager_agent="agent:sender")
    prior = await send_message(actions.pool, from_agent="agent:sender", from_project="demo",
                               to_agent="agent:abcd1234", body="earlier assignment")
    await actions.pool.execute(
        "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
        "VALUES ('demo','agent:sender',$1,'resume')", int(prior["id"]))
    msg_id = await _dm_to_owner(actions)

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        pass

    st = _settings(enabled=True, sense=str(sense), rate_cap=1)
    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender", settings=st, spawn=_spawn,
                          windows=_no_windows)
    assert d["mode"] == "skipped-rate-capped"


async def test_dispatch_dm_falls_back_to_project_scope_without_a_managed_edge(
    actions: Actions, tmp_path: Path,
) -> None:
    """No managed_by edge between sender and target (a peer DM, or an unseated party) keeps
    the ORIGINAL project-wide brake: the fix narrows the cap only where the pair concept
    actually applies; everything else is exactly as protected as it was before."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    # no _managed_pair call: "agent:sender" and "agent:abcd1234" share no managed_by edge
    stray = await send_message(actions.pool, from_agent="agent:whoever", from_project="demo",
                               to_agent="agent:whoever-else", body="project noise")
    await actions.pool.execute(
        "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
        "VALUES ('demo','agent:whoever',$1,'mint')", int(stray["id"]))
    msg_id = await _dm_to_owner(actions)

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        pass

    st = _settings(enabled=True, sense=str(sense), rate_cap=1)
    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender", settings=st, spawn=_spawn,
                          windows=_no_windows)
    assert d["mode"] == "skipped-rate-capped"  # the old project-wide count still applies


async def test_manages_someone_is_the_MANAGER_side(actions: Actions) -> None:
    """Pin the direction the human-attended guard turns on (invert it and you inject the operator
    while starving the worker): managed_by is minted worker→manager, so the MANAGER is the
    to-side; _manages_someone is True for the manager, False for the worker."""
    from src.orchestrator.trigger import _manages_someone

    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:w0001", manager_agent="agent:m0001")
    assert await _manages_someone(actions.pool, manager_seat) is True
    assert await _manages_someone(actions.pool, worker_seat) is False


async def test_dispatch_dm_never_injects_an_explicitly_attended_seat(actions: Actions) -> None:
    """THE HUMAN-ATTENDED GUARD'S REAL SIGNAL (replacing an older
    managed_by proxy). agent:abcd1234's seat is stamped attended='human' via set_seat_attended:
    mail to it waits in the box and is perceived by PULL (mailbox + stop-hook), never a
    forged-human daemon injection into the operator's live turn. Merely MANAGING someone is no
    longer sufficient on its own (see the regression test right below this one): the explicit
    stamp is what gates it now."""
    _, manager_seat = await _managed_pair(
        actions, worker_agent="agent:sender", manager_agent="agent:abcd1234")
    await set_seat_attended(actions, seat_id=manager_seat, attended="human", actor="operator",
                            because="test: this seat IS the operator-fronted one")

    async def _boom(*a: Any, **kw: Any) -> Any:
        raise AssertionError("a human-attended seat must never be injected / spawned / poked")

    msg_id = await _dm_to_owner(actions)  # a DM to agent:abcd1234 (the manager)
    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender", settings=_settings(enabled=True),
                          spawn=_boom, nudge=_boom, poke=_boom, windows=_no_windows)
    assert d["mode"] == "queued-human" and "human-attended" in d["detail"]


async def test_dispatch_dm_no_longer_infers_attendance_from_managing_someone(
    actions: Actions,
) -> None:
    """THE REGRESSION THIS THREAD FIXES: a seat that merely
    a test seat (one flip-test's mints, another's pilot workers) must NOT be
    silently reclassified as human-attended just because managed_by points at it. With no
    `attended` stamp and a non-matching handle, dispatch proceeds normally (never
    queued-human): the old proxy would have wrongly queued this and starved the push lane."""
    await _managed_pair(actions, worker_agent="agent:sender", manager_agent="agent:abcd1234",
                        manager_handle="Imhotep")

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        return None

    msg_id = await _dm_to_owner(actions)
    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender", settings=_settings(enabled=True),
                          spawn=_spawn, windows=_no_windows)
    assert d["mode"] != "queued-human"


async def test_dispatch_dm_falls_back_closed_for_thoths_own_unstamped_seat(
    actions: Actions,
) -> None:
    """The rollout belt: a seat with NO explicit attended stamp yet still falls
    back to human-attended if, and only if, its handle is that specific one's, so the one seat that
    actually IS operator-driven never starts being injected just because nobody has stamped
    it yet. Everyone else defaults OPEN (the prior test)."""
    await _managed_pair(actions, worker_agent="agent:sender", manager_agent="agent:abcd1234",
                        manager_handle="Thoth")

    async def _boom(*a: Any, **kw: Any) -> Any:
        raise AssertionError("thoth's own seat must never be injected while unstamped")

    msg_id = await _dm_to_owner(actions)
    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender", settings=_settings(enabled=True),
                          spawn=_boom, nudge=_boom, poke=_boom, windows=_no_windows)
    assert d["mode"] == "queued-human"


async def test_wake_refuses_an_unseated_caller(actions: Actions) -> None:
    """No held seat, no knock: managed_by is seat-to-seat and an unseated mind has no
    relationship it could invoke it with."""
    d = await wake_worker(actions, caller="agent:nobody", target="agent:alsonobody",
                          message="hey")
    assert d["mode"] == "refused-not-your-worker" and "holds no seat" in d["detail"]


async def test_wake_refuses_a_seatless_target(actions: Actions) -> None:
    """The caller holds a seat but the target names nobody living: refused, nothing sent."""
    worker_seat = (await ensure_seat(actions, house="demo", handle="Solo",
                                     source="test"))["seat_id"]
    await bind_holder(actions, seat_id=worker_seat, agent_id="agent:solo01")
    d = await wake_worker(actions, caller="agent:solo01", target="agent:ghost99",
                          message="hey")
    assert d["mode"] == "refused-not-your-worker" and "no living Seat" in d["detail"]
    assert await actions.pool.fetchval("SELECT count(*) FROM fleet_messages") == 0


async def test_wake_refuses_a_peer_with_no_managed_by_edge(actions: Actions) -> None:
    """Two seated minds, no managed_by edge between them: a peer knock, refused. This is
    the exact case wake() exists to distinguish from send(): mail between peers is normal;
    a wake between peers is not."""
    a_seat = (await ensure_seat(actions, house="demo", handle="Alpha",
                                source="test"))["seat_id"]
    b_seat = (await ensure_seat(actions, house="demo", handle="Beta",
                                source="test"))["seat_id"]
    await bind_holder(actions, seat_id=a_seat, agent_id="agent:alpha01")
    await bind_holder(actions, seat_id=b_seat, agent_id="agent:beta01")
    d = await wake_worker(actions, caller="agent:alpha01", target="agent:beta01",
                          message="hey")
    assert d["mode"] == "refused-not-your-worker" and "no active managed_by edge" in d["detail"]
    assert await actions.pool.fetchval("SELECT count(*) FROM fleet_messages") == 0


def _land_marker(sense: Path, marker: str) -> None:
    """Simulate the outcome-read's happy path: append a genuine "type":"user" line carrying
    `marker` to the fixture transcript `_stale_resumable_owner` already created: the mocked
    spawn/nudge in these tests never write anything real, so the landing has to be staged."""
    import json

    t = sense / "-repo-demo" / f"{FULL_SID}.jsonl"
    with t.open("a") as f:
        # a leading newline guarantees our own line, whatever the fixture's own trailing
        # bytes look like (the base fixture pads with un-terminated "x" filler bytes)
        f.write("\n" + json.dumps({"type": "user",
                                   "message": {"content": f"{marker}\n\nbody"}}) + "\n")


async def test_wake_authorizes_worker_to_manager(actions: Actions, tmp_path: Path) -> None:
    """The direction the org chart actually stores (worker --managed_by--> manager): a
    worker knocking on ITS OWN manager is authorized, and the mail reaches the manager's
    seat via the SAME dispatch path send() uses: CONFIRMED landed via the outcome-read,
    not merely queued."""
    sense = await _stale_resumable_owner(actions, tmp_path, bind_seat=False)
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:sender", manager_agent="agent:abcd1234")
    # attendance is an explicit stamp now, not inferred from managed_by:
    # this manager IS the operator-fronted one for this test's purpose, so it's stamped.
    await set_seat_attended(actions, seat_id=manager_seat, attended="human", actor="operator",
                            because="test: this seat is the operator-fronted one")
    _land_marker(sense, _wake_marker("agent:sender", worker_seat, "Worker"))
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, kw))

    d = await wake_worker(actions, caller="agent:sender", target=manager_seat,
                          message="blocked, need your word",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows)
    # the manager is HUMAN-ATTENDED (the real signal now): the knock is
    # authorized but delivered by PULL: it waits in the manager's box and surfaces on its
    # next turn / the stop-hook, never a forged injection into the operator's live turn.
    # "the worker can reach up" is preserved; only the delivery mechanism changes.
    assert d["status"] == "queued-human-attended" and d["raw_mode"] == "queued-human"
    assert d["seat"] == manager_seat
    assert not calls  # NOTHING injected or spawned, the human perceives it via the mailbox
    row = await actions.pool.fetchrow(
        "SELECT to_agent, grade FROM fleet_messages WHERE id=$1", d["message_id"])
    assert row["to_agent"] == manager_seat and row["grade"] == "ask"  # the ask waits in the box


async def test_wake_authorizes_manager_to_worker_too(actions: Actions, tmp_path: Path) -> None:
    """The gate is bidirectional: a manager knocking DOWN on its own
    worker is just as authorized as the reverse: only peers and strangers refuse."""
    sense = await _stale_resumable_owner(actions, tmp_path, bind_seat=False)
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:sender")
    await _office(actions, worker_seat, "/repo/demo")
    _land_marker(sense, _wake_marker("agent:sender", manager_seat, "Manager"))

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        pass

    d = await wake_worker(actions, caller="agent:sender", target=worker_seat,
                          message="status?",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows)
    assert d["status"] == "delivered" and d["seat"] == worker_seat and d["observed"] is True


async def test_wake_polls_and_reports_observed_once_the_marker_lands_mid_window(
    actions: Actions, tmp_path: Path,
) -> None:
    """DOOR 2, the happy path: unlike the test above (the marker
    already landed BEFORE the call, so `_poll_landed_then_renudge`'s own first check
    succeeds with no sleep at all), this stages the landing to happen DURING the second
    poll: proof the bounded poll itself, not just its zero-wait edge case, is what
    reports `observed: True`."""
    sense = await _stale_resumable_owner(actions, tmp_path, bind_seat=False)
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:sender")
    await _office(actions, worker_seat, "/repo/demo")
    marker = _wake_marker("agent:sender", manager_seat, "Manager")

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        pass  # the mocked spawn writes nothing real, staged below instead

    sleep_calls: list[float] = []

    async def _fake_sleep(secs: float) -> None:
        sleep_calls.append(secs)
        _land_marker(sense, marker)  # lands between the first miss and the second check

    d = await wake_worker(actions, caller="agent:sender", target=worker_seat,
                          message="status?",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, sleep=_fake_sleep,
                          landed_poll_attempts=3, landed_poll_delay_secs=0.0)
    assert d["status"] == "delivered" and d["observed"] is True
    assert len(sleep_calls) == 1  # returned on the SECOND check, never waited out the rest
    assert "renudge" not in d  # landed inside the window, no re-nudge was ever needed


async def test_wake_is_FROZEN_when_the_flag_is_off(actions: Actions, tmp_path: Path) -> None:
    """THE HANDOFF'S DEPLOY BIND, CLOSED (2026-07-21). wake() rides the daemon reply
    lane (a confirmed RCE), so it ships FROZEN (osiris_wake_enabled=False). An AUTHORIZED pair,
    the exact case the gate would otherwise let through, is refused with 'refused-wake-frozen'
    and NOTHING is sent: no marker, no DM, no spawn. The flag flips only once a sanctioned
    inter-agent API replaces the lane."""
    sense = await _stale_resumable_owner(actions, tmp_path, bind_seat=False)
    _worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:sender", manager_agent="agent:abcd1234")

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        raise AssertionError("a frozen wake must never spawn")

    d = await wake_worker(actions, caller="agent:sender", target=manager_seat,
                          message="blocked, need your word",
                          settings=_settings(enabled=True, sense=str(sense), wake_enabled=False),
                          spawn=_spawn, windows=_no_windows)
    assert d["mode"] == "refused-wake-frozen"
    assert await actions.pool.fetchval("SELECT count(*) FROM fleet_messages") == 0


async def test_wake_reports_queued_when_the_marker_never_lands(
    actions: Actions, tmp_path: Path
) -> None:
    """THE OUTCOME-READ'S WHOLE POINT: a daemon/resume success is a QUEUE
    success, not a SEEN one. When the marker never appears in the target's transcript (the
    ordinary case in these tests, since the mocked spawn/nudge writes nothing real), wake()
    must NOT claim "delivered": it downgrades honestly to "queued", unconfirmed.

    DOOR 2: this now exercises `_poll_landed_then_renudge`'s own
    BOUNDED poll (a fake `sleep`, never real time, same discipline launch()'s own
    poll tests already use) rather than a single immediate check; the re-nudge at the end
    of the window reaches the SAME injected `spawn`/`windows`, so it stays fully hermetic
    even though it fires a second real `dispatch_dm` resolution."""
    sense = await _stale_resumable_owner(actions, tmp_path, bind_seat=False)
    # knock DOWN on a worker (abcd1234), the injectable direction: a manager target would be
    # pull-only by the human-attended guard and never reach the marker-downgrade path this pins.
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:sender")
    await _office(actions, worker_seat, "/repo/demo")

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        pass  # never writes the marker anywhere, nothing "lands"

    sleep_calls: list[float] = []

    async def _fake_sleep(secs: float) -> None:
        sleep_calls.append(secs)

    d = await wake_worker(actions, caller="agent:sender", target=worker_seat,
                          message="status?",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, sleep=_fake_sleep,
                          landed_poll_attempts=2, landed_poll_delay_secs=0.0)
    assert d["raw_mode"] == "resumed"  # dispatch_dm itself still reports success...
    assert d["status"] == "queued" and d["observed"] is False  # ...but wake() won't inherit it
    assert "not yet confirmed" in d["detail"].lower()
    assert len(sleep_calls) == 2  # the full bounded window, never fewer, never more
    # the re-nudge itself is HONESTLY reported even though dispatch_dm's own once-per-
    # message brake refuses it (the first call already recorded a dm-resume ledger row
    # for this exact msg_id), a genuinely useful finding, not a broken test: the system
    # correctly never double-wakes the identical message, and the receipt says so rather
    # than pretending a second push happened.
    assert d["renudge"]["mode"] == "skipped-once-per-message"


async def test_wake_never_calls_mid_turn_delivered(
    actions: Actions, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE WHOLE POINT: dispatch_dm's own mode used to be
    literally named "delivered" though it means the addressee is mid-turn and has NOT
    read the message: renamed at the source to "mid-turn" so the receipt is honest from
    the first hop, never a translation patching over a lying word. wake() still surfaces
    both the raw mode and the (now identical) external status."""
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:sender", manager_agent="agent:abcd1234")

    async def _fake_dispatch(*a: Any, **kw: Any) -> dict[str, Any]:
        return {"mode": "mid-turn", "detail": "genuinely mid-turn, unread"}

    monkeypatch.setattr(trigger_module, "dispatch_dm", _fake_dispatch)
    d = await wake_worker(actions, caller="agent:sender", target=manager_seat,
                          message="hey", settings=_settings(enabled=True))
    assert d["status"] == "mid-turn"
    assert d["raw_mode"] == "mid-turn"  # the raw truth was never a lie to begin with now


async def test_wake_translates_pull_only_and_refused_budget(
    actions: Actions, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other two named buckets: nobody home, and the dollar wall, each its own honest
    word, neither one a bare boolean."""
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:sender", manager_agent="agent:abcd1234")

    async def _pull_only(*a: Any, **kw: Any) -> dict[str, Any]:
        return {"mode": "never-mounted", "detail": "never mounted"}

    monkeypatch.setattr(trigger_module, "dispatch_dm", _pull_only)
    d = await wake_worker(actions, caller="agent:sender", target=manager_seat, message="hey",
                          settings=_settings(enabled=True))
    assert d["status"] == "no-live-body"

    async def _refused(*a: Any, **kw: Any) -> dict[str, Any]:
        return {"mode": "refused", "detail": "daily ceiling reached"}

    monkeypatch.setattr(trigger_module, "dispatch_dm", _refused)
    d2 = await wake_worker(actions, caller="agent:sender", target=manager_seat, message="hey",
                           settings=_settings(enabled=True))
    assert d2["status"] == "refused-budget"


async def test_wake_worker_forwards_its_own_resolved_settings_not_the_raw_param(
    actions: Actions, monkeypatch: pytest.MonkeyPatch
) -> None:
    """wake_worker's own `st = settings or
    get_settings()` resolves ONE settings snapshot for its osiris_wake_enabled check, but
    the dispatch_dm call downstream used to forward the RAW `settings` param instead of
    that resolved `st`, harmless only because every real caller today passes
    settings=None either way, so both sides degenerate to the same bare get_settings()
    read. get_settings() is not memoized (config/settings.py: `Settings()` constructed
    fresh every call), so a future caller wired to `settings_with_overlay()`'s own live
    override would have that override silently dropped the instant dispatch_dm
    re-resolved bare instead of reusing wake_worker's own `st`. Proven with the only real
    shape today (settings=None passed IN): dispatch_dm must receive a concrete resolved
    Settings object, never None re-passed straight through."""
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:sender", manager_agent="agent:abcd1234")

    captured: dict[str, Any] = {}

    async def _capture_dispatch(*a: Any, **kw: Any) -> dict[str, Any]:
        captured["settings"] = kw.get("settings")
        return {"mode": "queued-no-listener", "detail": "nobody home"}

    monkeypatch.setattr(trigger_module, "dispatch_dm", _capture_dispatch)
    await wake_worker(actions, caller="agent:sender", target=manager_seat, message="hey",
                      settings=None)
    assert captured["settings"] is not None
    assert hasattr(captured["settings"], "osiris_wake_enabled")  # a real resolved Settings


async def test_wake_translates_the_named_gate_refusals_and_not_injectable(
    actions: Actions, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#156.2: the third bucket, a body/session DOES exist but a NAMED gate refused it,
    must say WHICH gate, never a bare 'refused' or the old 'no-live-body' lie (a mind
    exists here; resuming it just isn't safe). And a system-config reason (the trigger
    switched off) must never read as 'nobody home' either: the mail IS queued and WILL
    be pulled, same as any other queued mode."""
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:sender", manager_agent="agent:abcd1234")

    for raw_mode, want_status in (
        ("resume-refused-compaction", "refused-compaction"),
        ("resume-refused-ceiling", "refused-ceiling"),
        ("resume-refused-no-anchor", "refused-no-anchor"),
        ("resume-refused-crossed-registry", "refused-crossed-registry"),
        ("resume-refused-unknown", "refused-unknown"),
        ("trigger-dark", "not-injectable"),
        ("held", "not-injectable"),
    ):
        async def _mode(*a: Any, _m: str = raw_mode, **kw: Any) -> dict[str, Any]:
            return {"mode": _m, "detail": f"detail for {_m}"}

        monkeypatch.setattr(trigger_module, "dispatch_dm", _mode)
        d = await wake_worker(actions, caller="agent:sender", target=manager_seat,
                              message="hey", settings=_settings(enabled=True))
        assert d["status"] == want_status, (raw_mode, d)
        assert d["raw_mode"] == raw_mode


async def test_dispatch_dm_never_mounted_is_distinct_from_no_anchor(
    actions: Actions,
) -> None:
    """#156.2's own live specimen: an addressee with NO agent_mounts row at all ('nothing
    to wait for, ever') must report 'never-mounted', never the same 'pull-only' bucket a
    mounted-but-transcript-missing addressee gets (resume-refused-no-anchor, pinned
    above): the old shared mode string could not tell these apart."""
    out = await send_message(actions.pool, from_agent="agent:sender", from_project="other",
                             to_agent="agent:totally-unknown", body="hello?")
    msg_id = int(out["id"])

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("never-mounted means nothing to wake at all")

    d = await dispatch_dm(actions.pool, addressee="agent:totally-unknown", msg_id=msg_id,
                          sender="agent:sender", settings=_settings(enabled=True),
                          spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "never-mounted"
    assert "never mounted" in d["detail"]


async def test_dispatch_dm_reports_queued_live_not_never_mounted_when_only_transcript_fresh(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dispatch_dm sibling of an earlier specimen, re-pinned after a later fix (the stale
    `last_active` fallback is gone; a real transcript stat against the lineage's own
    `anchor_sid` ledger replaces it): a live-by-transcript addressee with no agent_mounts
    row must never earn the manager's 'escalate, worker is gone' receipt (an earlier fix's
    exact shape), it queues, an outcome, not a failure."""
    monkeypatch.setenv("OSIRIS_TRANSCRIPTS", str(tmp_path))
    a = await actions.create_or_find_object("Agent", "agent:liveonly02", "fleet-observer")
    sid = "bbbb2222-cccc-3333-dddd-444455556666"
    (tmp_path / "-repo").mkdir()
    (tmp_path / "-repo" / f"{sid}.jsonl").write_text('{"type":"user"}\n')
    await actions.assert_property(a, f"anchor_sid:{sid[:8]}", sid, "test", NOW, 0.9,
                                  evidence_class="direct_observation")
    out = await send_message(actions.pool, from_agent="agent:sender", from_project="other",
                             to_agent="agent:liveonly02", body="hello?")
    msg_id = int(out["id"])

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a live-but-unresolved addressee is never spawned/nudged")

    d = await dispatch_dm(actions.pool, addressee="agent:liveonly02", msg_id=msg_id,
                          sender="agent:sender", settings=_settings(enabled=True),
                          spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "queued-live-unresolved"
    assert "has never mounted" not in d["detail"]


# ═══ THE FRESH-HEIR FALLBACK ═════════════════════════════════════
# the zero-tolerance compaction gate was the bug, not the seats: a mail-triggered wake
# whose addressee's own transcript sits past its resumable seam now boots a successor at
# the seat's own office instead of refusing outright. Scoped to the compaction gate only;
# the other gates stay genuine refusals (see the sibling test after this one).

async def test_dispatch_dm_boots_a_fresh_heir_when_the_holder_sits_past_the_seam(
    actions: Actions, tmp_path: Path,
) -> None:
    """A deep-tail-class defect, closed: a seat holder whose tail since its own last
    compaction boundary is genuinely tiny (min_tail_bytes refuses it) is not a dead end:
    dispatch_dm boots a fresh successor at the seat's own office. BOUND BEFORE SPAWN
    (closing the hole above an earlier claim_name backstop): the
    boot prompt is a STATEMENT, not an instruction: identity was written server-side
    by `_bind_before_spawn` before the process exists, the same fix an earlier Piece 1
    already gave launch_seat's own fresh-mint fallthrough."""
    sense = await _lineage_holder_with_session(
        actions, tmp_path, agent_id="agent:fh0001", compacted=True)
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:fh0001", manager_agent="agent:hm-fresh-heir",
        worker_handle="Seam-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/seam-test-office")
    # dispatch_dm's own wake_target resolution is agent_mounts-keyed (wakeable_identity),
    # distinct from the graph `session` property the lineage walk reads below it: both
    # must be present, the same shape a real launched-then-compacted seat leaves behind.
    await save_mount(actions.pool, job_dir="/x/jobs/fh0001", agent_id="agent:fh0001",
                     project="osiris", cwd="/tmp/seam-test-office", model=None,
                     session_key=None)
    out = await send_message(actions.pool, from_agent="agent:hm-fresh-heir",
                             from_project="osiris", to_agent=worker_seat,
                             body="please pick this up")
    msg_id = int(out["id"])
    from src.orchestrator.seats import seat_receipt as _seat_receipt

    before = await _seat_receipt(actions.pool, worker_seat)
    booted: list[dict[str, Any]] = []

    async def _fresh_spawn(repo: str, **kw: Any) -> None:
        booted.append({"repo": repo, **kw})

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a tail closed at the seam itself must never be resumed")

    d = await dispatch_dm(
        actions.pool, addressee=worker_seat, msg_id=msg_id, sender="agent:hm-fresh-heir",
        settings=_settings(enabled=True, dm_resume=True, min_tail_bytes=1000,
                          sense=str(sense)),
        spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom,
        fresh_spawn=_fresh_spawn)

    assert d["mode"] == "fresh-heir"
    assert "seam" in d["detail"] and "/tmp/seam-test-office" in d["detail"]
    assert len(booted) == 1
    assert booted[0]["repo"] == "/tmp/seam-test-office"
    assert "Seam-Test" in booted[0]["prompt"]
    assert "/tmp/seam-test-office" in booted[0]["prompt"]
    assert "claim_name" not in booted[0]["prompt"]
    row = await actions.pool.fetchrow(
        "SELECT mode FROM agent_wakes WHERE message_id=$1", msg_id)
    assert row is not None and row["mode"] == "dm-fresh-heir"
    # THE BIND ITSELF LANDED (not just the prompt's own wording): a real Agent identity
    # is minted and pre-registered server-side before the fresh body ever calls mount()
    # but per THE HOLDS-SANDWICH FIX, the seat's own `holds` edge never moves onto
    # that fresh bookkeeping heir (it has done no work yet and may never do any); it
    # stays on the ancestor it already correctly named, unbroken, never left to a fresh
    # session's own claim_name to re-establish.
    after = await _seat_receipt(actions.pool, worker_seat)
    assert after is not None and after.get("holder") is not None
    assert after["holder"] == (before or {}).get("holder")


async def test_dispatch_dm_never_mints_fresh_for_a_non_compaction_gate(
    actions: Actions, tmp_path: Path,
) -> None:
    """SCOPED TO COMPACTION ONLY: a no-anchor/ceiling/crossed-registry/resident-unknown
    refusal is a real identity uncertainty (here: a declared `session` with NO transcript
    on disk at all (nothing to corroborate against, the no-anchor class), minting on top
    of it would repeat the exact stranger-over-a-live-head class Leg 3 already fixed (
    item 1) just closed. Only the compaction gate (a KNOWN identity, just an unresumable
    tail) gets the fresh-heir door."""
    sense = tmp_path / "projects"
    sense.mkdir(parents=True, exist_ok=True)  # no transcript file written under it at all
    obj = await actions.create_or_find_object("Agent", "agent:fh0002", "test")
    await actions.assert_property(obj, "seat_generation", "1", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(obj, "session", FULL_SID, "test", NOW, 0.9,
                                  evidence_class="self_declared")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:fh0002", manager_agent="agent:hm-fresh-heir-2",
        worker_handle="Unknown-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/unknown-test-office")
    await save_mount(actions.pool, job_dir="/x/jobs/fh0002", agent_id="agent:fh0002",
                     project="osiris", cwd="/tmp/unknown-test-office", model=None,
                     session_key=None)
    out = await send_message(actions.pool, from_agent="agent:hm-fresh-heir-2",
                             from_project="osiris", to_agent=worker_seat,
                             body="please pick this up")
    msg_id = int(out["id"])

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a no-anchor refusal must never spawn anything")

    d = await dispatch_dm(
        actions.pool, addressee=worker_seat, msg_id=msg_id, sender="agent:hm-fresh-heir-2",
        settings=_settings(enabled=True, dm_resume=True, min_tail_bytes=1,
                          sense=str(sense)),
        spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom, fresh_spawn=_boom)

    assert d["mode"] == "resume-refused-no-anchor"


def test_gate_name_reads_the_same_prose_the_gates_already_produce() -> None:
    """#156.2: a stable short token per named gate, read from the SAME sentences
    `_resume_candidate_verdict`/`_resume_miss_reason` already produce for humans, never
    a second source of truth, and never a guess on unrecognized text.

    NEVER FED `_resume_guard`'s own prose: that gate returns its token
    ("crossed-registry" / "resident-unknown") directly now, precisely so this function
    never has to re-derive a mismatch/unknown distinction by string-matching rendered
    text: former crossed-registry prose now falls through to "unknown" here, same as
    any other text this function was never meant to parse."""
    gate_name = trigger_module._gate_name
    assert gate_name("found a candidate, but its tail after the last compaction boundary "
                     "is only 12 byte(s) (1 line(s)), closed at or near the compaction "
                     "boundary itself, with nothing real to resume into") == "compaction"
    # CORRECTED 2026-09-08: the ceiling refusal's
    # own prose changed: occupancy-shaped (the primary ceiling now) or
    # catastrophic-corruption-shaped (`_verdict_from_diagnostics`'s narrowed ceiling check),
    # never the old "over the context ceiling" wording. Both still classify as "ceiling".
    assert gate_name("found a candidate, but its last recorded context occupancy "
                     "(1,050,000 tokens, 105% of the 1000k window) is at or over the "
                     "window — a resume would have no room left to even receive the "
                     "resumed state before needing to compact again") == "ceiling"
    assert gate_name("found a candidate, but its tail after the last compaction boundary "
                     "is 70.0MB — over the 64MB catastrophic-corruption sanity bound, a "
                     "shape that suggests something is actually broken rather than merely "
                     "large; refused regardless of what its last recorded context "
                     "occupancy reads") == "ceiling"
    assert gate_name("no anchored transcript at all") == "no-anchor"
    assert gate_name("retired, a deliberate close, never reanimated") == "retired"
    assert gate_name("something nobody wrote yet") == "unknown"


async def test_wake_buckets_every_unnamed_mode_as_queued(
    actions: Actions, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rate brakes, pauses, and in-flight wakes are all genuinely "queued": the catch-all
    default, and the raw mode/detail survive so nothing is lost to the bucket."""
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:sender", manager_agent="agent:abcd1234")

    async def _braked(*a: Any, **kw: Any) -> dict[str, Any]:
        return {"mode": "braked", "detail": "the per-seat rate brake: 3 wakes/h already landed"}

    monkeypatch.setattr(trigger_module, "dispatch_dm", _braked)
    d = await wake_worker(actions, caller="agent:sender", target=manager_seat, message="hey",
                          settings=_settings(enabled=True))
    assert d["status"] == "queued" and d["raw_mode"] == "braked"
    assert "rate brake" in d["detail"]


# ═══ launch(): THE BODY VERB ══════════════════════════
# launch() is the CREATE twin of wake()'s speak: it summons a fresh claude into a managed seat's
# own office via the manager daemon's pty_spawn. These tests pin the authority gate (DOWNWARD-
# ONLY, unlike wake's either-direction knock), idempotency (never a twin), the honest body_exists-
# vs-can_receive receipt, and graceful failure on a dark daemon. The
# manager socket is INJECTED, so nothing here spawns a real body.


def _fake_manager(record: list[dict[str, Any]], *, ret: dict[str, Any] | None = None,
                  raises: BaseException | None = None) -> Any:
    async def _m(req: dict[str, Any]) -> dict[str, Any]:
        record.append(req)
        if raises is not None:
            raise raises
        return ret if ret is not None else {"spawned": req["name"]}
    return _m


def _fake_windows(rows: list[dict[str, Any]]) -> Any:
    async def _w() -> list[dict[str, Any]]:
        return list(rows)
    return _w


async def _office(actions: Actions, seat_id: str, cwd: str) -> None:
    """Give a seat an anchor_cwd (its office): _managed_pair leaves it unset."""
    oid = await actions.create_or_find_object("Seat", seat_id, "test")
    await actions.assert_property(oid, "anchor_cwd", cwd, "test", NOW, 0.9,
                                  evidence_class="self_declared")


async def test_launch_refuses_an_unseated_caller(actions: Actions) -> None:
    """No held seat, no birth: launch is a seat-to-seat act. Returns before any daemon touch."""
    d = await trigger_module.launch_seat(actions, caller="agent:nobody01", target="seat:whatever")
    assert d["status"] == "refused-not-your-worker" and "holds no seat" in d["detail"]


async def test_launch_refuses_a_seatless_target(actions: Actions) -> None:
    await _managed_pair(actions, worker_agent="agent:lw01", manager_agent="agent:lm01")
    record: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:lm01", target="agent:has-no-seat",
        manager=_fake_manager(record), windows=_fake_windows([]))
    assert d["status"] == "refused-not-your-worker" and "no living Seat" in d["detail"]
    assert record == []  # nothing spawned


async def test_launch_is_downward_only_a_worker_cannot_body_its_manager(actions: Actions) -> None:
    """THE distinction from wake(): a worker may WAKE its manager but may never
    LAUNCH it a body. The stored edge is worker→manager, so the worker does not
    MANAGE the manager: refused, nothing spawned."""
    _worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:dw01", manager_agent="agent:dm01")
    record: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:dw01", target=manager_seat,
        manager=_fake_manager(record), windows=_fake_windows([]))
    assert d["status"] == "refused-not-your-worker" and "DOWNWARD-ONLY" in d["detail"]
    assert record == []


async def test_launch_refuses_a_seat_with_no_office(actions: Actions) -> None:
    """A seat with no anchor_cwd has no room to be born in: refused, nothing spawned."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:ow01", manager_agent="agent:om01")
    record: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:om01", target=worker_seat,
        manager=_fake_manager(record), windows=_fake_windows([]))
    assert d["status"] == "refused-no-office" and record == []


async def test_launch_bodies_a_managed_seat_with_an_honest_receipt(actions: Actions) -> None:
    """The core: a manager bodies a seat it manages. body_exists is true (the window was made);
    can_receive is FALSE at the spawn instant (the fresh claude has not booted), reported
    SEPARATELY. The manager op is a pty_spawn naming the seat, into its office,
    carrying the seat's own anchor (never the launcher's)."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:sw01", manager_agent="agent:sm01",
        worker_handle="Tefnut", house="osiris")
    await _office(actions, worker_seat, "/home/asuramaya/.osiris/seats/tefnut")
    record: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:sm01", target=worker_seat, substrate="pty",
        manager=_fake_manager(record), windows=_fake_windows([]))
    assert d["status"] == "launched"
    assert d["body_exists"] is True and d["can_receive"] is False
    assert "NOT yet confirmed" in d["detail"] and "self-bind" in d["detail"]
    assert len(record) == 1
    req = record[0]
    assert req["op"] == "pty_spawn"
    assert req["name"] == "[OS] Tefnut"
    assert req["argv"][0] == "claude"
    assert req["cwd"] == "/home/asuramaya/.osiris/seats/tefnut"
    assert req["seat"] == {"handle": "Tefnut", "house": "osiris"}
    assert req["env"]["CLAUDE_JOB_DIR"] == req["job_dir"]  # the body's anchor, not inherited
    # THE ATTACH LINE: office dir + session anchor + a
    # command that works TODAY, independent of where the spawn's cwd happens to register in
    # the harness's own per-project session list.
    assert d["attach"]["office"] == "/home/asuramaya/.osiris/seats/tefnut"
    assert d["attach"]["session_anchor"] == req["job_dir"]
    assert d["attach"]["command"] == 'python -m src.manager.attach "[OS] Tefnut"'


async def test_launch_hands_the_attach_line_on_the_idempotent_path_too(
    actions: Actions,
) -> None:
    """A caller who launches into an already-live seat still needs to reach it: the
    already-live receipt must carry the same attach line, not just the fresh-spawn one."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:alw01", manager_agent="agent:alm01",
        worker_handle="Anhur", house="osiris")
    await _office(actions, worker_seat, "/tmp/anhur")
    record: list[dict[str, Any]] = []
    existing = _fake_windows([{"name": "[OS] Anhur", "alive": True, "seat_id": worker_seat}])
    d = await trigger_module.launch_seat(
        actions, caller="agent:alm01", target=worker_seat, substrate="pty",
        manager=_fake_manager(record), windows=existing)

    assert d["status"] == "already-live"
    assert d["attach"]["office"] == "/tmp/anhur"
    assert d["attach"]["command"] == 'python -m src.manager.attach "[OS] Anhur"'


async def test_launch_refuses_a_second_body_on_a_seat_a_live_body_already_occupies(
    actions: Actions,
) -> None:
    """ONE SEAT, ONE LIVE LINEAGE HEAD: the
    same seat's live specimen: two generations' jobs, both heartbeating on the same
    seat, because launch_seat's own idempotency checks never consulted the single occupancy
    authority the fold/reanimation/send() doors already share. A launch onto a seat whose
    CURRENT HOLDER that authority confirms is a live body must refuse outright, before
    either spawn lane, regardless of substrate.

    THE LIVENESS CONVERGENCE FIX:
    was `is_occupied_by_a_live_body` (registry_census's harness+/proc check); now
    `mounts.agent_liveness`, the SAME single source `team()`/`vacate_dead_seat` share:
    a real, fresh `agent_mounts` row (never a monkeypatched occupancy authority) is what
    makes this holder read live. Status stays `already-live` for LAUNCH specifically
    (cli.py's own contract: a body already existing is the GOAL STATE, exit 0, never a
    refusal): `_launch_target_setup`'s `occupied_status` param is what lets the SAME
    detection logic report itself differently to launch vs. resume."""
    from src.orchestrator.mounts import save_mount

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:occ01", manager_agent="agent:occm01",
        worker_handle="Halcyon-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/halcyon-test")
    await save_mount(actions.pool, job_dir="/jobs/occ01", agent_id="agent:occ01",
                     project="osiris", cwd="/tmp/halcyon-test", model="claude-sonnet-5",
                     session_key=None)

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a refused launch must spawn nothing")

    d = await trigger_module.launch_seat(
        actions, caller="agent:occm01", target=worker_seat,
        spawn=_boom, agents_json=_fake_agents_json([[]]))

    assert d["status"] == "already-live"
    assert d["holder"] == "agent:occ01"
    assert d["body_exists"] is True and d["can_receive"] is True


async def test_launch_delivers_the_message_when_already_live_and_the_nudge_lands(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DOOR 1: the duplicate-body
    refusal above is correct, but a caller-supplied `message` used to just vanish here:
    same already-live setup (a real `agent_mounts` row, `agent_liveness` says live), with
    a `message` this time. `dispatch_dm` mocked to confirm a genuine live injection."""
    from src.orchestrator.mounts import save_mount

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:occ02", manager_agent="agent:occm02",
        worker_handle="Halcyon-Msg", house="osiris")
    await _office(actions, worker_seat, "/tmp/halcyon-msg")
    await save_mount(actions.pool, job_dir="/jobs/occ02", agent_id="agent:occ02",
                     project="osiris", cwd="/tmp/halcyon-msg", model="claude-sonnet-5",
                     session_key=None)

    nudge_calls: list[dict[str, Any]] = []

    async def _fake_dispatch_dm(pool: Any, **kw: Any) -> dict[str, Any]:
        nudge_calls.append(kw)
        return {"mode": "nudged"}

    monkeypatch.setattr(trigger_module, "dispatch_dm", _fake_dispatch_dm)

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a refused launch must spawn nothing")

    d = await trigger_module.launch_seat(
        actions, caller="agent:occm02", target=worker_seat, message="status update?",
        spawn=_boom, agents_json=_fake_agents_json([[]]))

    assert d["status"] == "already-live"
    assert d["brief_delivery"] == {"delivered_via": "dm", "message_id": nudge_calls[0]["msg_id"]}
    assert nudge_calls[0]["addressee"] == worker_seat
    row = await actions.pool.fetchrow(
        "SELECT to_agent, grade, body FROM fleet_messages WHERE id=$1",
        nudge_calls[0]["msg_id"])
    assert row["to_agent"] == worker_seat and row["grade"] == "ask"
    assert row["body"] == "status update?"


async def test_launch_reports_brief_dropped_when_already_live_and_the_nudge_fails(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DOOR 1, the honest-failure shape: never a success-shaped receipt over an
    undelivered brief: a nudge outcome that does NOT confirm a live injection (here,
    the daemon accepted the envelope but no session-shaped body confirms it) is reported
    as `brief_dropped`, naming why, never smoothed into a false `delivered_via`."""
    from src.orchestrator.mounts import save_mount

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:occ03", manager_agent="agent:occm03",
        worker_handle="Halcyon-Drop", house="osiris")
    await _office(actions, worker_seat, "/tmp/halcyon-drop")
    await save_mount(actions.pool, job_dir="/jobs/occ03", agent_id="agent:occ03",
                     project="osiris", cwd="/tmp/halcyon-drop", model="claude-sonnet-5",
                     session_key=None)

    async def _fake_dispatch_dm(pool: Any, **kw: Any) -> dict[str, Any]:
        return {"mode": "queued-no-listener", "detail": "nobody home"}

    monkeypatch.setattr(trigger_module, "dispatch_dm", _fake_dispatch_dm)

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a refused launch must spawn nothing")

    d = await trigger_module.launch_seat(
        actions, caller="agent:occm03", target=worker_seat, message="status update?",
        spawn=_boom, agents_json=_fake_agents_json([[]]))

    assert d["status"] == "already-live"
    assert d["brief_delivery"]["brief_dropped"] is True
    assert d["brief_delivery"]["why"] == "nobody home"
    assert "message_id" in d["brief_delivery"]


async def test_launch_already_live_with_no_message_never_touches_mail(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No caller-supplied `message` at all: the already-live path must stay the pure,
    side-effect-free idempotent return it always was; `brief_delivery` is absent
    entirely (never a spurious empty-body DM)."""
    from src.orchestrator.mounts import save_mount

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:occ04", manager_agent="agent:occm04",
        worker_handle="Halcyon-Quiet", house="osiris")
    await _office(actions, worker_seat, "/tmp/halcyon-quiet")
    await save_mount(actions.pool, job_dir="/jobs/occ04", agent_id="agent:occ04",
                     project="osiris", cwd="/tmp/halcyon-quiet", model="claude-sonnet-5",
                     session_key=None)

    async def _boom_dispatch(*a: Any, **kw: Any) -> None:
        raise AssertionError("no message means nothing to dispatch")

    monkeypatch.setattr(trigger_module, "dispatch_dm", _boom_dispatch)

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a refused launch must spawn nothing")

    d = await trigger_module.launch_seat(
        actions, caller="agent:occm04", target=worker_seat,
        spawn=_boom, agents_json=_fake_agents_json([[]]))

    assert d["status"] == "already-live"
    assert "brief_delivery" not in d
    assert await actions.pool.fetchval("SELECT count(*) FROM fleet_messages") == 0


async def test_resume_refuses_occupied_when_agent_liveness_says_live(
    actions: Actions,
) -> None:
    """THE LIVENESS CONVERGENCE FIX (continued from
    11760): resume's own occupancy gate is the SAME shared `_launch_target_setup` check
    as launch's, but keeps its default `occupied_status="refused-occupied"`: a genuine
    conflict for resume (there is nothing to idempotently return, unlike launch's own
    goal-state framing), never `already-live`."""
    from src.orchestrator.mounts import save_mount

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:rsoc01", manager_agent="agent:rsocm01",
        worker_handle="Resume-Occupied", house="osiris")
    await _office(actions, worker_seat, "/tmp/resume-occupied")
    await save_mount(actions.pool, job_dir="/jobs/rsoc01", agent_id="agent:rsoc01",
                     project="osiris", cwd="/tmp/resume-occupied", model="claude-sonnet-5",
                     session_key=None)

    d = await trigger_module.resume_seat(
        actions, caller="agent:rsocm01", target=worker_seat,
        agents_json=_fake_agents_json([[]]))

    assert d["status"] == "refused-occupied"
    assert d["holder"] == "agent:rsoc01"


async def test_launch_resolves_a_vacant_seat_by_handle(actions: Actions) -> None:
    """task #68 (finding b): a freshly minted, never-launched seat has NO Agent bound to it
    at all: the old Agent-centric resolve_seat fallback returns no seat_id for such a seat
    (it only walks Agent objects that claimed a handle), so launch(target=<handle>) could
    never body a seat mint_seat had just made. The worker here is deliberately vacant (no
    bind_holder call) and still resolves by its bare handle."""
    manager_seat = (await ensure_seat(actions, house="demo", handle="Manager",
                                      source="test"))["seat_id"]
    await bind_holder(actions, seat_id=manager_seat, agent_id="agent:vm01")
    worker_seat = (await ensure_seat(actions, house="demo", handle="Nefer",
                                     source="test"))["seat_id"]  # never bound, VACANT
    await _office(actions, worker_seat, "/tmp/nefer")
    w_oid = await actions.create_or_find_object("Seat", worker_seat, "test")
    m_oid = await actions.create_or_find_object("Seat", manager_seat, "test")
    await actions.create_link(w_oid, m_oid, "managed_by", "test", NOW, 0.9)

    record: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:vm01", target="Nefer", substrate="pty",
        manager=_fake_manager(record), windows=_fake_windows([]))

    assert d["status"] == "launched"
    assert len(record) == 1 and record[0]["seat"]["handle"] == "Nefer"


async def test_launch_prefers_the_seats_stamped_model_over_the_global_default(
    actions: Actions,
) -> None:
    """task #68 (finding #7): the old precedence never read the seat's own
    stamped intended_model at all, only an explicit param or the trigger's global default,
    which is why a seat pinned to sonnet-5 could spawn on whatever osiris_wake_model happened
    to be configured, silently. The stamp must win over the global default."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:mw01", manager_agent="agent:mm01",
        worker_handle="Ptah", house="osiris")
    await _office(actions, worker_seat, "/tmp/ptah")
    oid = await actions.create_or_find_object("Seat", worker_seat, "test")
    await actions.assert_property(oid, "intended_model", "claude-sonnet-5", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    record: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:mm01", target=worker_seat, substrate="pty",
        manager=_fake_manager(record), windows=_fake_windows([]),
        settings=_settings(enabled=True, wake_model="claude-haiku-4-5"))

    assert d["status"] == "launched"
    assert d["spawned_model"] == "claude-sonnet-5"
    assert "model_mismatch" not in d
    assert record[0]["argv"][-2:] == ["--model", "claude-sonnet-5"]


async def test_launch_flags_a_model_mismatch_loudly(actions: Actions) -> None:
    """An explicit caller-supplied model still wins over the stamp (an intentional override),
    but the receipt must NAME the mismatch rather than silently spawning off-pin."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:xw01", manager_agent="agent:xm01",
        worker_handle="Sekhmet", house="osiris")
    await _office(actions, worker_seat, "/tmp/sekhmet")
    oid = await actions.create_or_find_object("Seat", worker_seat, "test")
    await actions.assert_property(oid, "intended_model", "claude-sonnet-5", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    record: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:xm01", target=worker_seat, model="claude-haiku-4-5",
        substrate="pty", manager=_fake_manager(record), windows=_fake_windows([]))

    assert d["spawned_model"] == "claude-haiku-4-5"
    assert "model_mismatch" in d and "claude-sonnet-5" in d["model_mismatch"]


async def test_launch_can_receive_true_when_the_window_comes_up_live(actions: Actions) -> None:
    """When the post-spawn READ shows the window ALIVE, can_receive is true: the separate read
    is real, not a hard-coded false."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:cw01", manager_agent="agent:cm01",
        worker_handle="Nut", house="osiris")
    await _office(actions, worker_seat, "/tmp/nut")
    name = "[OS] Nut"
    calls = {"n": 0}

    async def _w() -> list[dict[str, Any]]:
        calls["n"] += 1
        if calls["n"] == 1:  # the idempotency check, nothing live yet
            return []
        return [{"name": name, "alive": True, "seat_id": worker_seat}]  # the liveness read

    record: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:cm01", target=worker_seat, substrate="pty",
        manager=_fake_manager(record, ret={"spawned": name}), windows=_w)
    assert d["status"] == "launched"
    assert d["body_exists"] is True and d["can_receive"] is True
    assert d["detail"] == "body created and live"


async def test_launch_is_idempotent_returns_the_live_window_not_a_twin(actions: Actions) -> None:
    """A live body already holds the seat → RETURN it, never spawn a duplicate (a stale-liveness
    collision). The manager op is never called."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:iw01", manager_agent="agent:im01",
        worker_handle="Geb", house="osiris")
    await _office(actions, worker_seat, "/tmp/geb")
    record: list[dict[str, Any]] = []
    existing = _fake_windows([{"name": "[OS] Geb", "alive": True, "seat_id": worker_seat}])
    d = await trigger_module.launch_seat(
        actions, caller="agent:im01", target=worker_seat, substrate="pty",
        manager=_fake_manager(record), windows=existing)
    assert d["status"] == "already-live"
    assert d["body_exists"] is True and d["can_receive"] is True
    assert record == []  # NO twin spawned


async def test_launch_reports_manager_cold_when_the_daemon_is_dark(actions: Actions) -> None:
    """A dark manager daemon → an honest 'manager-cold', nothing claimed spawned."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:kw01", manager_agent="agent:km01",
        worker_handle="Shu", house="osiris")
    await _office(actions, worker_seat, "/tmp/shu")
    record: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:km01", target=worker_seat, substrate="pty",
        manager=_fake_manager(record, raises=OSError("no such socket")),
        windows=_fake_windows([]))
    assert d["status"] == "manager-cold" and "osiris-manager" in d["detail"]


async def test_launch_delivers_the_opening_brief_over_the_mail_lane(actions: Actions) -> None:
    """A message rides the ordinary mail lane as a graded ask to the new seat: never a
    hand-forged turn. It lands as a fleet_messages row addressed to the seat."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:bw01", manager_agent="agent:bm01",
        worker_handle="Isis", house="osiris")
    await _office(actions, worker_seat, "/tmp/isis")
    manc = await actions.create_or_find_object("Agent", "agent:bm01", "test")
    await actions.assert_property(manc, "project", "osiris", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    record: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:bm01", target=worker_seat, message="Welcome, mount and orient.",
        substrate="pty",
        manager=_fake_manager(record, ret={"spawned": "[OS] Isis"}), windows=_fake_windows([]))
    assert d["status"] == "launched" and d.get("brief_message_id")
    row = await actions.pool.fetchrow(
        "SELECT body FROM fleet_messages WHERE id=$1", int(d["brief_message_id"]))
    assert row is not None and "mount and orient" in row["body"]


# ═══ THE DEFAULT FLIP: launch_seat's harness-native lane (task #68,
# rulings clause 3) ═════════════════════════════════════════════════════════════════════════════
# 'harness' is now launch_seat's DEFAULT substrate (no `substrate` argument needed); the PTY
# lane above only runs when a test (or a caller) asks for it by name. `spawn`/`agents_json`/
# `cost_reader` are injected fakes: never a real `claude` process or subprocess.


def _fake_agents_json(rows_by_call: list[list[dict[str, Any]]]) -> Any:
    """One list of rows PER CALL, consumed in order; the last list repeats once exhausted (a
    caller that reads the roster more times than scripted gets the steady state, not an
    IndexError)."""
    calls = {"n": 0}

    async def _read(*, cwd: str | None = None,
                    include_completed: bool = False) -> list[dict[str, Any]]:
        i = min(calls["n"], len(rows_by_call) - 1)
        calls["n"] += 1
        return list(rows_by_call[i])
    return _read


def _fake_spawn(record: list[dict[str, Any]], *, raises: Exception | None = None) -> Any:
    async def _spawn(repo: str, **kwargs: Any) -> None:
        if raises is not None:
            raise raises
        record.append({"repo": repo, **kwargs})
    return _spawn


def _fake_cost_reader(result: dict[str, Any]) -> Any:
    async def _cost(session_id: str, *, cwd: str | None = None) -> dict[str, Any]:
        return dict(result)
    return _cost


async def _launch_usage_rows(actions: Actions) -> list[Any]:
    return await actions.pool.fetch(
        "SELECT purpose, model, cost_usd FROM llm_usage WHERE purpose='launch' "
        "ORDER BY id DESC LIMIT 1")


async def test_launch_defaults_to_the_harness_native_lane_with_an_honest_receipt(
    actions: Actions,
) -> None:
    """No `substrate` argument at all, the flip's whole point: launch_seat now bodies a
    seat as a `claude --bg` session by default, with an honest body_exists/can_receive split
    exactly like the old PTY lane."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw01", manager_agent="agent:hm01",
        worker_handle="Sobek", house="osiris")
    await _office(actions, worker_seat, "/tmp/sobek")
    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm01", target=worker_seat,
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]),
        cost_reader=_fake_cost_reader({"priced": False, "reason": "no cost field"}))

    assert d["status"] == "launched"
    assert d["body_exists"] is True and d["can_receive"] is False
    assert d["window"] == "[OS] Sobek"
    assert d["attach"]["office"] == "/tmp/sobek"
    assert d["attach"]["command"] == 'python -m src.manager.attach "[OS] Sobek"'
    assert len(spawned) == 1
    call = spawned[0]
    assert call["repo"] == "/tmp/sobek" and call["name"] == "[OS] Sobek"
    assert "session_id" not in call  # --bg ignores it; never passed (live finding 2026-07-27)
    assert "job_dir" not in call  # env vars never reach a --bg spare either (same finding)
    # THE BOOT PROMPT IS A STATEMENT, NOT AN INSTRUCTION (Piece 1,
    # one layer up): identity is bound SERVER-SIDE, before the body exists,
    # `_bind_before_spawn` minted the seat's own next generation and pre-registered its
    # anchor row, so the fresh session's own mount() call re-attaches directly. claim_name
    # never appears, an earlier specimen showed a fresh session asked to re-derive its
    # own binding through that fallible call can refuse and mint a phantom instead. (The
    # exact heir id here is `_managed_pair`'s own fixture shortcut's business, see
    # test_bind_before_spawn_mints_an_heir_of_the_seats_own_lineage for the precise id.)
    assert "/tmp/sobek" in call["prompt"]
    assert "Sobek" in call["prompt"]
    assert "claim_name" not in call["prompt"]


# ═══ PIECE 1's OWN UNIT: _bind_before_spawn writes the identity
# before the body exists: mint_heir's mechanics for a seat with a recorded ancestor, a
# bare first generation for one that has never been claimed, and a pre-registered
# (alive=False) anchor row either way so the spawned session's own mount() re-attaches
# directly instead of needing claim_name. ═══════════════════════════════════════════════

async def test_bind_before_spawn_mints_an_heir_of_the_seats_own_lineage(
    actions: Actions,
) -> None:
    """The ordinary case: the seat's handle-assertion source AND its `holds` edge agree on
    the same lineage. Reuses mint_heir outright (bind_seat=False, the fresh heir has done
    no work yet and never opens a holds window of its own, THE HOLDS-SANDWICH FIX): next
    generation, succeeds_seat edge, `out["agent"]` names the heir for the spawn prompt. The
    seat's own holds link stays on the ANCESTOR, unbroken (via the explicit bind_holder call
    below, never left to mint_heir's own follow_binding alone, see the disagreement test
    below for why)."""
    seat_id = (await ensure_seat(actions, house="dealer-to-fb", handle="Marquee",
                                 source="agent:38cf08a9"))["seat_id"]
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:38cf08a9")

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Marquee", house="dealer-to-fb",
        current_holder="agent:38cf08a9", office="/tmp/marquee",
        anchor="/tmp/anchors/marquee", source="agent:thoth01")

    assert out["agent"] == "agent:38cf08a9-ii"
    assert out["generation"] == 2
    row = await actions.pool.fetchrow(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_id)
    assert row["canonical"] == "agent:38cf08a9"  # the ANCESTOR, never the fresh heir


async def test_bind_before_spawn_never_breaks_the_real_holders_holds_row(
    actions: Actions,
) -> None:
    """THE HOLDS-SANDWICH FIX, THE DIRECT ASSERTION:
    a launch through the bind-before-spawn path must never open a second, near-instant
    holds window for the fresh bookkeeping heir: the real holder's own row (first_seen,
    and staying valid_until IS NULL) is untouched, not invalidated-then-recreated, proving
    bind_holder below is a true no-op here rather than a same-agent rebind that still
    breaks the row in two."""
    seat_id = (await ensure_seat(actions, house="dealer-to-fb", handle="Sandwich",
                                 source="agent:sandwichline"))["seat_id"]
    bound = await bind_holder(actions, seat_id=seat_id, agent_id="agent:sandwichline")
    before = await actions.pool.fetchrow(
        "SELECT l.id, l.first_seen FROM links l "
        "JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE t.canonical=$1 AND f.canonical='agent:sandwichline' AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_id)
    assert before is not None
    assert bound["new_holder"] == "agent:sandwichline"

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Sandwich", house="dealer-to-fb",
        current_holder="agent:sandwichline", office="/tmp/sandwich",
        anchor="/tmp/anchors/sandwich", source="agent:thoth01")

    assert out["agent"] == "agent:sandwichline-ii"  # the fresh bookkeeping heir, minted
    rows = await actions.pool.fetch(
        "SELECT f.canonical, l.id, l.first_seen, l.valid_until FROM links l "
        "JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE t.canonical=$1 AND l.type='holds'", seat_id)
    # exactly ONE holds row exists, the original, byte-identical, never invalidated,
    # never a second row for the fresh heir sandwiched in front of or behind it
    assert len(rows) == 1
    assert rows[0]["id"] == before["id"]
    assert rows[0]["first_seen"] == before["first_seen"]
    assert rows[0]["canonical"] == "agent:sandwichline"
    assert rows[0]["valid_until"] is None


async def test_bind_before_spawn_confesses_a_legacy_lineage_source_without_changing_it(
    actions: Actions,
) -> None:
    """AN EARLIER RULING: the 17-seat mint_seat landmine (a manager's own id shared
    as the `handle` assertion source across every worker it minted) gets NO behavior
    change for seats that already exist: this specimen's ancestor resolution is
    UNCHANGED from test_bind_before_spawn_mints_an_heir_of_the_seats_own_lineage right
    above it, same inputs, same output agent/generation. The only difference: the
    receipt now carries a one-line `legacy_lineage_warning` naming the pre-fix source,
    so a human watching launches can catch the exposure without this function ever
    refusing or rewriting anything about the mint itself."""
    seat_id = (await ensure_seat(actions, house="dealer-to-fb", handle="Marquee",
                                 source="agent:38cf08a9"))["seat_id"]
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:38cf08a9")

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Marquee", house="dealer-to-fb",
        current_holder="agent:38cf08a9", office="/tmp/marquee",
        anchor="/tmp/anchors/marquee", source="agent:thoth01")

    assert out["agent"] == "agent:38cf08a9-ii"  # UNCHANGED from the plain-lineage case
    assert "legacy_lineage_warning" in out
    assert "agent:38cf08a9" in out["legacy_lineage_warning"]
    assert seat_id in out["legacy_lineage_warning"]


async def test_bind_before_spawn_never_confesses_a_post_fix_founder_source(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE NEGATIVE CONTROL: a seat founded AFTER this fix (a real _FOUNDER_SOURCE_PREFIX
    source, same as the console/no-handle-assertion cases) never carries the confession:
    there is nothing to confess, since _seat_lineage_ancestor already excludes it and
    this mints a genuinely fresh root, not an inherited one."""
    from src.orchestrator.mintseat import found_seat

    seat_id = (await found_seat(actions, handle="Postfixa", path=str(tmp_path / "ws"),
                                actor="khnum", office_root=tmp_path / "seats"))["seat_id"]

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Postfixa", house="Postfixa",
        current_holder=None, office="/tmp/postfixa", anchor="/tmp/anchors/postfixa",
        source="agent:thoth01")

    assert "legacy_lineage_warning" not in out
    assert out["agent"] == f"agent:seat-{seat_id.removeprefix('seat:')}"


async def test_bind_before_spawn_never_adopts_another_agents_live_job_dir_as_an_ancestor(
    actions: Actions,
) -> None:
    """A specimen, reproduced directly: a seat had no lineage-side handle assertion to
    resolve (this
    test's own `_seat_lineage_ancestor` returns None, same as the no-handle-assertion
    case above), so `current_holder`, the seat's own `holds` edge, whatever it names,
    became the ancestor fallback. Live, that edge named `agent:9c9a534f`: not a real
    identity at all, but a borrowed harness session id. The real
    agent (a different canonical, `agent:dustinreal-xv` here) is genuinely mounted
    under job_dir `.../jobs/9c9a534f` at the moment this fires: the exact collision
    this door must catch and refuse, falling through to a genuinely fresh, seat-
    derived root instead of inheriting a lineage that was never actually its own,
    or anyone's."""
    from src.orchestrator import mounts as mounts_module

    seat_id = (await ensure_seat(actions, house="monsterhouse", handle="Jennytest",
                                 source="console"))["seat_id"]  # no lineage-side source
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:9c9a534f")
    # A genuinely different agent, live under the exact job_dir slug
    # this seat's holds edge happens to name.
    await actions.create_or_find_object("Agent", "agent:dustinreal-xv", "test")
    await mounts_module.save_mount(
        actions.pool, job_dir="/home/asuramaya/.claude/jobs/9c9a534f",
        agent_id="agent:dustinreal-xv", project="monsterhouse",
        cwd="/home/asuramaya/code/monsterhouse", model=None, session_key=None)

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Jennytest", house="monsterhouse",
        current_holder="agent:9c9a534f", office="/tmp/jennytest",
        anchor="/tmp/anchors/jennytest", source="agent:thoth01")

    # a genuinely fresh, seat-derived root: never an heir of the borrowed session id
    assert out["agent"] == f"agent:seat-{seat_id.removeprefix('seat:')}"
    assert "9c9a534f" not in out["agent"]
    row = await actions.pool.fetchrow(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_id)
    # the seat's own holds edge is corrected too: never left pointing at the borrowed id
    assert row["canonical"] == out["agent"]
    # the other agent's own live mount is entirely untouched by this
    dustin_row = await mounts_module.find_mount(
        actions.pool, job_dir="/home/asuramaya/.claude/jobs/9c9a534f")
    assert dustin_row is not None and dustin_row.agent_id == "agent:dustinreal-xv"


async def test_bind_before_spawn_resolves_from_the_lineage_never_the_stale_holds_edge(
    actions: Actions,
) -> None:
    """A specimen, reproduced directly (an earlier correction,
    measured against the real seat): a seat's `handle` assertion is sourced from ONE
    lineage (the mind that actually built the seat) while its `holds` edge names an
    UNRELATED agent that contributed nothing else (a corrupted/stale binding, cold means
    'held, nobody live', not 'held by the right mind'). Resolving generation math from
    `holds` would mint the wrong lineage's next generation while LOOKING like success (no
    new objects minted, just the wrong ancestor): the exact bug an earlier dispatch named
    as the load-bearing requirement. The lineage wins; the stale holds edge gets corrected
    as a byproduct of the SAME act, through the same named verb, never a hand write."""
    seat_id = (await ensure_seat(actions, house="dealer-to-fb", handle="Marquee",
                                 source="agent:realmind"))["seat_id"]
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:staleholder")

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Marquee", house="dealer-to-fb",
        current_holder="agent:staleholder", office="/tmp/marquee",
        anchor="/tmp/anchors/marquee", source="agent:thoth01")

    assert out["agent"] == "agent:realmind-ii"  # the LINEAGE's next gen, not staleholder's
    row = await actions.pool.fetchrow(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_id)
    # the stale edge is corrected to the REAL ancestor, not left on staleholder, and never
    # handed to the fresh bookkeeping heir either (the holds-sandwich fix)
    assert row["canonical"] == "agent:realmind"


async def test_bind_before_spawn_never_treats_a_console_established_handle_as_an_ancestor(
    actions: Actions,
) -> None:
    """A specimen, reproduced directly (2026-09-03): a
    seat established DIRECTLY by a human at the CLI carries `source_id='console'`
    (`seats.py`'s own `_OPERATOR_ACTORS` sentinel) on its `handle` assertion from birth:
    never an agent lineage at all, unlike the OTHER `_bind_before_spawn` specimens above
    where the handle's source genuinely IS the seat's own founding mind. Before this fix,
    `_seat_lineage_ancestor` fed 'console' straight into `lineage_head`, which found no
    succeeded_by chain to walk and handed 'console' back unchanged: minting a PHANTOM
    console/console-ii lineage and rebinding the seat to it, discarding the real holder
    entirely. `_OPERATOR_ACTORS` must fall through to `current_holder`, the SAME safe path
    a seat with no handle assertion at all already takes."""
    seat_id = (await ensure_seat(actions, house="Chad", handle="Chad",
                                 source="console"))["seat_id"]
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:7451509a")

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Chad", house="Chad",
        current_holder="agent:7451509a", office="/tmp/chad",
        anchor="/tmp/anchors/chad", source="agent:thoth01")

    assert out["agent"] == "agent:7451509a-ii"  # the REAL holder's next gen
    assert "console" not in out["agent"]
    row = await actions.pool.fetchrow(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_id)
    assert row["canonical"] == "agent:7451509a"  # the real holder, never console, never
    # the fresh bookkeeping heir either (the holds-sandwich fix)
    phantom = await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE canonical IN ('console', 'console-ii')")
    assert phantom is None  # nothing minted from the CLI actor label at all


async def test_bind_before_spawn_never_chains_two_self_managed_seats_under_the_same_actor(
    actions: Actions, tmp_path: Path,
) -> None:
    """A REGRESSION, REPRODUCED AT THE MECHANISM THAT
    ACTUALLY CAUSES IT: two seats founded via found_seat under the SAME --actor used
    to carry that actor's own id as the source on BOTH seats' `handle` assertions:
    `_seat_lineage_ancestor` treated it as each seat's own founding lineage, so seat
    B's FIRST launch walked seat A's own live lineage forward (via lineage_head) and
    chained B's fresh mint onto A's chain: the corruption `_lineage_resume_candidate`
    then surfaces AT RESUME TIME (walking B's own holder backward through
    succeeded_from lands on A's real session; measured live in the resume_seat
    acceptance test: a synthetic seat 3 inherited seat 2's own dormant session, both
    founded `--actor khnum`). found_seat's own per-seat founder source (seats.py's
    `_FOUNDER_SOURCE_PREFIX`) fixes this at the root: both seats always mint a bare,
    independent `agent:seat-<id>` root, never an heir of anyone else's lineage: so
    resume(B) can never continue A, because nothing ever chained them together."""
    from src.orchestrator.mintseat import found_seat

    seat_a = (await found_seat(actions, handle="Lineage-A", path=str(tmp_path / "wsa"),
                               actor="khnum", office_root=tmp_path / "seatsa"))["seat_id"]
    seat_b = (await found_seat(actions, handle="Lineage-B", path=str(tmp_path / "wsb"),
                               actor="khnum", office_root=tmp_path / "seatsb"))["seat_id"]

    out_a = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_a, handle="Lineage-A", house="Lineage-A",
        current_holder=None, office="/tmp/lineage-a", anchor="/tmp/anchors/lineage-a",
        source="agent:thoth01")
    out_b = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_b, handle="Lineage-B", house="Lineage-B",
        current_holder=None, office="/tmp/lineage-b", anchor="/tmp/anchors/lineage-b",
        source="agent:thoth01")

    # THE ROOT FIX: each seat mints its OWN bare first generation, never an heir of
    # the other (or of the shared actor): the exact "agent:seat-<id>" shape the
    # NO-ANCESTOR case already uses for a seat truly never described.
    assert out_a["agent"] == f"agent:seat-{seat_a.removeprefix('seat:')}"
    assert out_b["agent"] == f"agent:seat-{seat_b.removeprefix('seat:')}"
    assert out_a["agent"] != out_b["agent"]

    succeeded_from_b = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o "
        "ON o.id=a.object_id WHERE o.canonical=$1 AND a.name='succeeded_from'",
        out_b["agent"])
    assert succeeded_from_b is None  # B is a fresh root, never an heir of A's lineage

    row_a = await actions.pool.fetchrow(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_a)
    row_b = await actions.pool.fetchrow(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_b)
    assert row_a["canonical"] == out_a["agent"]
    assert row_b["canonical"] == out_b["agent"]
    assert row_a["canonical"] != row_b["canonical"]  # resume(B) can never continue A


async def test_bind_before_spawn_still_chains_a_relaunch_of_the_same_self_managed_seat(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE NEGATIVE CONTROL: the fix above must not turn EVERY relaunch into a fresh
    root: a seat's own SECOND generation still chains from its OWN first, exactly as
    before, via the same `current_holder` fallback a seat with no handle assertion at
    all already relies on (the founder-prefixed source is excluded unconditionally,
    same as `_OPERATOR_ACTORS`, so this path was already proven safe there)."""
    from src.orchestrator.mintseat import found_seat

    seat_id = (await found_seat(actions, handle="Lineage-C", path=str(tmp_path / "wsc"),
                                actor="khnum", office_root=tmp_path / "seatsc"))["seat_id"]

    first = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Lineage-C", house="Lineage-C",
        current_holder=None, office="/tmp/lineage-c", anchor="/tmp/anchors/lineage-c",
        source="agent:thoth01")
    second = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Lineage-C", house="Lineage-C",
        current_holder=first["agent"], office="/tmp/lineage-c",
        anchor="/tmp/anchors/lineage-c", source="agent:thoth01")

    assert second["agent"] == f"{first['agent']}-ii"


async def test_bind_before_spawn_pre_registers_the_anchor_row_so_mount_reattaches(
    actions: Actions,
) -> None:
    """The whole reason claim_name becomes unnecessary: the spawned session's own
    mount(job_dir=anchor) finds THIS row waiting, agent_id already set: the same
    'seated the moment you exist' discipline save_mount's own docstring describes,
    fired before the process exists rather than at its first call. `alive=False`: a
    pre-registration is not a heartbeat, and must not read as a false liveness signal."""
    from src.orchestrator.mounts import find_mount

    seat_id = (await ensure_seat(actions, house="dealer-to-fb", handle="Marquee",
                                 source="agent:38cf08a9"))["seat_id"]
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:38cf08a9")

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Marquee", house="dealer-to-fb",
        current_holder="agent:38cf08a9", office="/tmp/marquee",
        anchor="/tmp/anchors/marquee", source="agent:thoth01")

    rec = await find_mount(actions.pool, job_dir="/tmp/anchors/marquee")
    assert rec is not None
    assert rec.agent_id == out["agent"]
    assert rec.cwd == "/tmp/marquee" and rec.project == "dealer-to-fb"
    pulse = await actions.pool.fetchval(
        "SELECT last_seen FROM agent_mounts WHERE job_dir=$1", "/tmp/anchors/marquee")
    assert pulse is None  # alive=False: no heartbeat granted by the pre-registration itself


async def test_bind_before_spawn_mints_a_first_generation_when_the_seat_was_never_held(
    actions: Actions,
) -> None:
    """No ancestor exists: the seat has no `handle` assertion at all (nothing lineage-side
    to read) and no `holds` edge, so this mints a bare first generation directly, under the
    same `agent:seat-<id>` root every pure-seat-office lineage already carries."""
    seat_id = "seat:freshling01"
    await actions.create_or_find_object("Seat", seat_id, "test")  # bare: no handle asserted

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Freshling", house="freshhouse",
        current_holder=None, office="/tmp/freshling",
        anchor="/tmp/anchors/freshling", source="agent:thoth01")

    assert out["agent"] == f"agent:seat-{seat_id.removeprefix('seat:')}"
    assert out["generation"] == 1
    row = await actions.pool.fetchrow(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_id)
    assert row["canonical"] == out["agent"]


async def test_launch_harness_lane_is_idempotent_returns_the_live_body_not_a_twin(
    actions: Actions,
) -> None:
    """A live `claude agents --json` row already sitting at this seat's own office cwd →
    RETURN it, never spawn a duplicate (the same one-body law as the PTY lane). Matched
    on cwd, not session id: `--bg` assigns its own id and ignores any we present."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw02", manager_agent="agent:hm02",
        worker_handle="Anubis", house="osiris")
    await _office(actions, worker_seat, "/tmp/anubis")
    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm02", target=worker_seat,
        spawn=_fake_spawn(spawned),
        agents_json=_fake_agents_json([[{"cwd": "/tmp/anubis", "name": "[OS] Anubis"}]]))

    assert d["status"] == "already-live"
    assert d["body_exists"] is True and d["can_receive"] is True
    assert d["attach"]["office"] == "/tmp/anubis"
    assert spawned == []  # NO twin spawned


async def test_launch_harness_lane_catches_a_resumed_body_the_harness_roster_cannot_see(
    actions: Actions,
) -> None:
    """Task #148's contested seam 4: `claude agents --json` is invisible to a resumed
    (`-p --resume`) body BY CONSTRUCTION: an EMPTY harness roster used to mean "safe to
    mint," even when a resumed session is genuinely live at this exact cwd, reachable only
    through agent_mounts (its own mid-turn mount() call lands there, never in the harness's
    `--bg`-only roster).

    THE LIVENESS CONVERGENCE FIX:
    `_launch_target_setup`'s own shared occupancy gate (both launch and resume) now
    reads `mounts.agent_liveness` too, so it catches this exact agent_mounts-only-visible
    body BEFORE `_launch_twin_check`'s own richer, cwd-scoped evidence-gathering ever
    runs -- still never twins the body (task #148's real safety property), still under
    `already-live` for LAUNCH (`occupied_status="already-live"`, matching cli.py's own
    exit-0 goal-state contract) even though the DETECTION now comes from the shared
    gate rather than `_launch_twin_check`'s own cwd-scoped harness+mounts probe."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw03", manager_agent="agent:hm03",
        worker_handle="Sobek-Resumed", house="osiris")
    await _office(actions, worker_seat, "/tmp/sobek-resumed")
    await save_mount(actions.pool, job_dir="/tmp/jobs/sobek-resumed-job",
                     agent_id="agent:hw03", project="p", cwd="/tmp/sobek-resumed",
                     model=None, session_key=None)
    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm03", target=worker_seat,
        spawn=_fake_spawn(spawned),
        agents_json=_fake_agents_json([[]]))  # the harness roster sees NOTHING here

    assert d["status"] == "already-live"
    assert spawned == []  # NO twin spawned, even though the harness roster was empty
    assert d["holder"] == "agent:hw03"
    assert d["body_exists"] is True and d["can_receive"] is True


async def test_launch_never_treats_a_different_seats_live_mount_as_its_own_twin(
    actions: Actions, tmp_path: Path,
) -> None:
    """TWO SEATS, ONE TREE, ONE LIVE MOUNT:
    sweep-seat-trees legitimately binds more than one seat to
    the same tree_cwd. `_launch_twin_check`'s own `agent_mounts WHERE cwd=$1 LIMIT 1`
    query used to return the single freshest LIVE row at that cwd from ANY agent, no
    matter whose lineage it belonged to: one seat's own holder was stopped, but a
    sibling seat's still-live mount at the SAME shared tree read as "already-live" for
    that seat too, blocking its launch entirely. A cwd is not an identity: a mount only
    counts as THIS seat's own body when `held_seat` confirms its agent actually holds
    this seat (the same lineage-wide holds-link authority `identify_agent` trusts)."""
    from src.orchestrator.seats import bind_seat_tree

    chowder_seat, _chowder_mgr = await _managed_pair(
        actions, worker_agent="agent:cho01", manager_agent="agent:chom01",
        worker_handle="Chowder", manager_handle="ChowderMgr", house="monsterhouse")
    dustin_seat, _dustin_mgr = await _managed_pair(
        actions, worker_agent="agent:dus01", manager_agent="agent:dusm01",
        worker_handle="Dustin", manager_handle="DustinMgr", house="monsterhouse")
    shared_tree = tmp_path / "monsterhouse"
    shared_tree.mkdir()
    await _office(actions, chowder_seat, "/home/asuramaya/.osiris/seats/chowder")
    await _office(actions, dustin_seat, "/home/asuramaya/.osiris/seats/dustin")
    await bind_seat_tree(actions, seat_id=chowder_seat, tree_cwd=str(shared_tree),
                         actor="operator", because="test: shared tree, chowder")
    await bind_seat_tree(actions, seat_id=dustin_seat, tree_cwd=str(shared_tree),
                         actor="operator", because="test: shared tree, dustin")
    # this seat's own holder (agent:cho01) was stopped, no mount row for it at all, so
    # `_launch_target_setup`'s own liveness gate finds nobody live and falls through to
    # `_launch_twin_check`. The sibling seat's own holder is genuinely live, at the SAME cwd.
    await save_mount(actions.pool, job_dir="/tmp/jobs/dustin-job", agent_id="agent:dus01",
                     project="monsterhouse", cwd=str(shared_tree), model=None,
                     session_key=None)
    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:chom01", target=chowder_seat,
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched"  # never "already-live", the sibling mount is not a duplicate
    assert spawned and spawned[0]["repo"] == str(shared_tree)


async def test_launch_ignores_a_failed_dead_harness_row_and_reports_it_as_stale(
    actions: Actions, tmp_path: Path,
) -> None:
    """The follow-up to the
    mounts-side fix above: `claude agents --json` kept listing one seat's OLD session
    (state='failed', a long-dead pid) forever: the harness never reaps a failed row on
    its own, so `_launch_twin_check`'s cwd match kept refusing every relaunch as
    already-live off a process that no longer existed. A failed/pid-dead row must never
    be trusted as a twin, and must be named on the receipt rather than silently
    swallowed."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:deadrow01", manager_agent="agent:deadrowm01",
        worker_handle="Deadrow", house="monsterhouse")
    launch_cwd = tmp_path / "deadrow"
    launch_cwd.mkdir()
    await _office(actions, worker_seat, str(launch_cwd))
    # the seat's own holder never mounted at all: `_launch_target_setup`'s liveness
    # gate finds nobody live and falls through to `_launch_twin_check`, whose ONLY
    # signal here is the harness's own stale roster row.
    stale_row = {"cwd": str(launch_cwd), "name": "[MO] Deadrow", "sessionId": "aa64c831",
                 "status": "idle", "state": "failed", "pid": 2025736}
    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:deadrowm01", target=worker_seat,
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[stale_row]]))

    assert d["status"] == "launched"  # never "already-live", a dead row is not a twin
    assert spawned and spawned[0]["repo"] == str(launch_cwd)
    assert d["stale_harness_row"] == stale_row


async def test_launch_ignores_a_bg_spare_wearing_the_seats_own_window_name(
    actions: Actions, tmp_path: Path,
) -> None:
    """The exact shape behind one incident's own
    incident: the "failed" row's pid turned out to be `claude bg-spare --bg-spare
    <sock>`, a warm-spare claim process, real and alive on `/proc`, but never a
    conversational body. A `state: "running"` row whose cmdline names it a spare must
    be ignored exactly like a failed/dead one: `state` alone is not the whole story."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:spare01", manager_agent="agent:sparem01",
        worker_handle="Sparerow", house="monsterhouse")
    launch_cwd = tmp_path / "sparerow"
    launch_cwd.mkdir()
    await _office(actions, worker_seat, str(launch_cwd))
    real_pid = os.getpid()
    spare_row = {"cwd": str(launch_cwd), "name": "[MO] Sparerow",
                "sessionId": "faf79699", "status": "idle", "state": "running",
                "pid": real_pid}

    def _spare_cmdline(pid: int) -> bytes:
        return b"claude\x00bg-spare\x00--bg-spare\x00/tmp/spare.sock"

    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:sparem01", target=worker_seat,
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[spare_row]]),
        read_cmdline=_spare_cmdline)

    assert d["status"] == "launched"  # never "already-live", a spare is not a twin
    assert spawned and spawned[0]["repo"] == str(launch_cwd)
    assert d["stale_harness_row"] == spare_row


async def test_harness_row_is_live_checks_state_then_a_real_proc_pid() -> None:
    """The predicate in isolation: `state` in failed/completed refuses outright (never
    even reaching the pid check); a live-looking state with a pid that does not exist
    on THIS box (checked against /proc, never assumed) also refuses; a row with no pid
    at all is trusted on `state` alone: a harness version that doesn't report one is
    not treated as evidence of death it never actually carried. A live pid whose own
    argv (`read_cmdline`, injected, never a real `/proc` read here) is a
    `claude bg-spare` warm-spare refuses too, regardless of `state` (an earlier
    addendum): a genuine `claude` body's cmdline is what makes it True."""
    from src.orchestrator.trigger import _harness_row_is_live

    def _body_cmdline(pid: int) -> bytes:
        return b"claude\x00--bg\x00-n\x00[MO] chowder"

    def _bg_spare_cmdline(pid: int) -> bytes:
        return b"claude\x00bg-spare\x00--bg-spare\x00/tmp/cc-daemon-1000/spare/faf79699.claim.sock"

    assert _harness_row_is_live({"state": "failed", "pid": os.getpid()}) is False
    assert _harness_row_is_live({"state": "completed"}) is False
    assert _harness_row_is_live({"state": "running", "pid": 999999999}) is False
    assert _harness_row_is_live({"state": "running"}) is True
    assert _harness_row_is_live({}) is True
    assert _harness_row_is_live(
        {"state": "running", "pid": os.getpid()}, read_cmdline=_body_cmdline) is True
    # THE ADDENDUM: "running" state, a genuinely alive pid, still refused,
    # because the process behind it is a warm spare, never a conversational body.
    assert _harness_row_is_live(
        {"state": "running", "pid": os.getpid()}, read_cmdline=_bg_spare_cmdline) is False


async def test_launch_binds_holds_to_the_fresh_heir_at_spawn_return_not_the_stale_ancestor(
    actions: Actions,
) -> None:
    """LAW (a):
    one seat's own holder was stopped, launch minted and bound a fresh heir's IDENTITY
    (`_bind_before_spawn`) but left the seat's own `holds` edge on the pre-stop
    generation (THE HOLDS-SANDWICH FIX's own deliberate ancestor-case behavior), so
    the live spawned body sat unaddressable while mail to the seat routed into the old
    (soon retired) generation. Once the OS-level spawn is accepted, that gap must
    close: the seat's own `holds` edge names the fresh heir immediately, before the
    body's first turn."""
    from src.orchestrator.seats import bind_holder, ensure_seat, held_seat

    # NOT `_managed_pair` (its own `ensure_seat(..., source="test")` makes the literal
    # string "test" the seat's own handle-assertion source, which `_seat_lineage_
    # ancestor` would then trust as "the ancestor" instead of agent:anc01, a test-
    # fixture artifact no production caller ever hits, since a real launch's source is
    # always a real agent id). Built by hand so agent:anc01 is genuinely the lineage
    # `_bind_before_spawn` resolves and binds ancestor-side, the actual shape this law
    # fixes.
    worker = await ensure_seat(actions, house="monsterhouse", handle="Ancestral2",
                               source="agent:anc01")
    worker_seat = worker["seat_id"]
    await bind_holder(actions, seat_id=worker_seat, agent_id="agent:anc01",
                      source="agent:anc01")
    manager = await ensure_seat(actions, house="monsterhouse", handle="AncMgr2",
                                source="agent:ancm01")
    manager_seat = manager["seat_id"]
    await bind_holder(actions, seat_id=manager_seat, agent_id="agent:ancm01",
                      source="agent:ancm01")
    w_oid = await actions.create_or_find_object("Seat", worker_seat, "test")
    m_oid = await actions.create_or_find_object("Seat", manager_seat, "test")
    await actions.create_link(w_oid, m_oid, "managed_by", "test", NOW, 0.9)
    await _office(actions, worker_seat, "/tmp/ancestral")
    # the ancestor's own holder is long dead: no live mount row for it at all, so
    # `_launch_target_setup`'s liveness gate falls through to a fresh mint.
    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:ancm01", target=worker_seat,
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched"
    held = await held_seat(actions.pool, "agent:anc01")
    # the ANCESTOR no longer holds: held_seat resolves lineage-wide, so this also
    # proves the fresh heir (not agent:anc01 itself) is the seat's own current holder.
    assert held is not None and held["seat_id"] == worker_seat
    fresh_holder = await actions.pool.fetchval(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", worker_seat)
    assert fresh_holder != "agent:anc01"  # promoted past the stale ancestor generation


async def test_launch_reports_the_brief_delivered_once_inbox_reads_it(
    actions: Actions,
) -> None:
    """LAW (b), the happy path: the fresh body's own `inbox()` call sets the brief's
    read state: `_verify_brief_reached_a_turn` polls for exactly that and reports
    `delivered: True` the moment it lands, never waiting out the full window once it
    has its answer."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:brief01", manager_agent="agent:briefm01",
        worker_handle="Briefed", house="monsterhouse")
    await _office(actions, worker_seat, "/tmp/briefed")
    sleep_calls: list[float] = []

    async def _fake_sleep(secs: float) -> None:
        sleep_calls.append(secs)
        # simulate the fresh body's own inbox() call landing between polls
        await actions.pool.execute(
            "UPDATE fleet_messages SET read_at=now() WHERE to_agent=$1 "
            "AND read_at IS NULL", worker_seat)

    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:briefm01", target=worker_seat, message="welcome aboard",
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]),
        sleep=_fake_sleep, brief_poll_attempts=3, brief_poll_delay_secs=0.0)

    assert d["status"] == "launched"
    assert d["brief_delivery"] == {"delivered": True}
    assert len(sleep_calls) == 1  # returned on the SECOND check, never waited out the rest


async def test_launch_nudges_and_reports_when_the_brief_never_reaches_a_turn(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LAW (b), the incident's own shape: one seat's opening brief
    never became the body's first turn: its transcript held only the boot
    ritual for 20+ minutes. Exhausting the bounded poll with nothing read must nudge
    through the SAME mail ladder every other DM escalation uses (`dispatch_dm`), never
    a second nudge mechanism, and report exactly what happened rather than silently
    calling this "launched" with no further signal."""
    from src.orchestrator import trigger as trigger_mod

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:stall01", manager_agent="agent:stallm01",
        worker_handle="Stalled", house="monsterhouse")
    await _office(actions, worker_seat, "/tmp/stalled")
    sleep_calls: list[float] = []

    async def _fake_sleep(secs: float) -> None:
        sleep_calls.append(secs)  # never marks the message read, it just never arrives

    nudge_calls: list[dict[str, Any]] = []

    async def _fake_dispatch_dm(pool: Any, **kw: Any) -> dict[str, Any]:
        nudge_calls.append(kw)
        return {"mode": "nudged"}

    monkeypatch.setattr(trigger_mod, "dispatch_dm", _fake_dispatch_dm)

    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:stallm01", target=worker_seat, message="welcome aboard",
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]),
        sleep=_fake_sleep, brief_poll_attempts=2, brief_poll_delay_secs=0.0)

    assert d["status"] == "launched"
    assert len(sleep_calls) == 2  # the full bounded window, never fewer, never more
    assert d["brief_delivery"]["delivered"] is False
    assert d["brief_delivery"]["nudge"] == {"mode": "nudged"}
    # "nudged" injects mail into an ALREADY-LIVE session directly: no lineage walk.
    assert d["brief_delivery"]["delivered_via"] == "direct"
    assert len(nudge_calls) == 1
    assert nudge_calls[0]["msg_id"] == d["brief_message_id"]


async def test_launch_names_lineage_delivery_when_the_nudge_resumes_a_dormant_session(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One live specimen:
    the nudge genuinely delivered the brief (session 7806f810's real transcript grew
    69MB two seconds later), but by RESUMING a dormant session in the seat's own
    lineage, not the literal harness session launch just spawned. THE DESIGN DECISION:
    "mint a body, deliver through the lineage" is the accepted shape, named honestly
    on the receipt (`delivered_via: "lineage"`) rather than pretending session-exact
    precision this door cannot promise."""
    from src.orchestrator import trigger as trigger_mod

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:lineage01", manager_agent="agent:lineagem01",
        worker_handle="Lineagebody", house="monsterhouse")
    await _office(actions, worker_seat, "/tmp/lineagebody")

    async def _fake_sleep(secs: float) -> None:
        pass  # never marks the message read, the fresh spawn never picks it up

    async def _fake_dispatch_dm(pool: Any, **kw: Any) -> dict[str, Any]:
        return {"mode": "resumed", "session_id": "7806f810-057e-4380-b479-d7ad812d1a83",
               "detail": "the addressee's own session (7806f810) is continued with this "
                         "mail as its next turn, watch the hop in the agents view"}

    monkeypatch.setattr(trigger_mod, "dispatch_dm", _fake_dispatch_dm)

    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:lineagem01", target=worker_seat, message="welcome aboard",
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]),
        sleep=_fake_sleep, brief_poll_attempts=1, brief_poll_delay_secs=0.0)

    assert d["status"] == "launched"
    assert d["brief_delivery"]["delivered"] is False
    assert d["brief_delivery"]["delivered_via"] == "lineage"
    assert d["brief_delivery"]["nudge"]["session_id"] == "7806f810-057e-4380-b479-d7ad812d1a83"


async def test_launch_names_no_delivery_path_when_the_nudge_only_queues(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A nudge that neither resumed nor injected directly (queued, refused, skipped)
    did not deliver through either concrete path this house can characterize:
    `delivered_via` stays absent rather than guessing one."""
    from src.orchestrator import trigger as trigger_mod

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:queued01", manager_agent="agent:queuedm01",
        worker_handle="Queuedbody", house="monsterhouse")
    await _office(actions, worker_seat, "/tmp/queuedbody")

    async def _fake_sleep(secs: float) -> None:
        pass

    async def _fake_dispatch_dm(pool: Any, **kw: Any) -> dict[str, Any]:
        return {"mode": "queued-live-holder", "detail": "it reads this at its next turn"}

    monkeypatch.setattr(trigger_mod, "dispatch_dm", _fake_dispatch_dm)

    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:queuedm01", target=worker_seat, message="welcome aboard",
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]),
        sleep=_fake_sleep, brief_poll_attempts=1, brief_poll_delay_secs=0.0)

    assert d["status"] == "launched"
    assert d["brief_delivery"]["delivered"] is False
    assert "delivered_via" not in d["brief_delivery"]


async def test_launch_harness_lane_refuses_an_over_budget_mint(
    actions: Actions,
) -> None:
    """THE SPEND GAP: a new
    body is a real turn, same dollar wall dispatch_dm/wake_worker already stand behind:
    but launch_seat never checked it at all. Same $12-over-$10 ledger and billed-backend
    settings as test_the_DAILY_CEILING_stops_the_wake, aimed at launch_seat instead of the
    mail-driven wake path."""
    from src.ingest.providers import Usage
    from src.ingest.usage import record_usage

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw10", manager_agent="agent:hm10",
        worker_handle="Overbudget-Harness", house="osiris")
    await _office(actions, worker_seat, "/tmp/overbudget-harness")
    for _ in range(12):
        await record_usage(actions.pool, purpose="wake", usage=Usage(
            model="claude-haiku-4-5-20251001", input_tokens=1, output_tokens=1,
            cache_read_tokens=0, cache_creation_tokens=0, cost_usd=1.00))

    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm10", target=worker_seat, substrate="harness",
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]),
        settings=_settings(enabled=True, daily_usd=10.0,
                           extract_provider="anthropic", api_key="k"))

    assert d["status"] == "refused-budget"
    assert "CEILING REACHED" in d["detail"] or "ceiling" in d["detail"].lower()
    assert spawned == []


async def test_launch_pty_lane_refuses_an_over_budget_mint(
    actions: Actions,
) -> None:
    """Same gate, the other substrate: a real gap independently, per launch_seat's own
    two-lanes-two-spawn-sites shape."""
    from src.ingest.providers import Usage
    from src.ingest.usage import record_usage

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:pw10", manager_agent="agent:pm10",
        worker_handle="Overbudget-Pty", house="osiris")
    await _office(actions, worker_seat, "/tmp/overbudget-pty")
    for _ in range(12):
        await record_usage(actions.pool, purpose="wake", usage=Usage(
            model="claude-haiku-4-5-20251001", input_tokens=1, output_tokens=1,
            cache_read_tokens=0, cache_creation_tokens=0, cost_usd=1.00))

    async def _boom(*a: Any, **kw: Any) -> Any:
        raise AssertionError("a refused launch must spawn nothing")

    d = await trigger_module.launch_seat(
        actions, caller="agent:pm10", target=worker_seat, substrate="pty",
        manager=_boom, windows=_fake_windows([]),
        settings=_settings(enabled=True, daily_usd=10.0,
                           extract_provider="anthropic", api_key="k"))

    assert d["status"] == "refused-budget"
    assert "CEILING REACHED" in d["detail"] or "ceiling" in d["detail"].lower()


async def test_launch_harness_lane_confesses_dormant_history_before_spawn(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When launch_cwd already holds a substantial
    transcript, the receipt names it ({"path", "size_bytes", "last_touched"}) rather than
    silently spawning into it. Disclosure, not a gate: the spawn still fires."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw09", manager_agent="agent:hm09",
        worker_handle="Ooblek-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/ooblek-test")

    fake_info = {"path": "/tmp/ooblek-test.jsonl", "size_bytes": 20_300_000,
                 "last_touched": "2026-08-02T17:57:18+00:00"}
    monkeypatch.setattr(
        "src.ingest.sessions.dormant_history_confession",
        lambda cwd, **k: fake_info if cwd == "/tmp/ooblek-test" else None,
    )
    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm09", target=worker_seat,
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched"
    assert len(spawned) == 1  # the confession never blocks the spawn
    assert d["dormant_history"] == fake_info


async def test_launch_harness_lane_omits_dormant_history_key_when_absent(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw10", manager_agent="agent:hm10",
        worker_handle="Clean-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/clean-test")
    monkeypatch.setattr("src.ingest.sessions.dormant_history_confession", lambda cwd, **k: None)
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm10", target=worker_seat,
        spawn=_fake_spawn([]), agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched"
    assert "dormant_history" not in d


async def test_launch_harness_lane_can_receive_true_when_the_session_comes_up_live(
    actions: Actions,
) -> None:
    """The post-spawn READ is real, not hard-coded false: when the fresh session already
    shows up in `claude agents --json` at the seat's own office cwd, can_receive reports it."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw03", manager_agent="agent:hm03",
        worker_handle="Bastet", house="osiris")
    await _office(actions, worker_seat, "/tmp/bastet")
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm03", target=worker_seat,
        spawn=_fake_spawn([]),
        agents_json=_fake_agents_json(
            # LEG 3: launch_seat now runs an occupancy check
            # (is_occupied_by_a_live_body) BEFORE the twin check, one extra leading
            # agents_json() call, empty because this seat's holder is unoccupied.
            [[], [], [{"cwd": "/tmp/bastet", "sessionId": "real-abc", "name": "[OS] Bastet"}]]))

    assert d["status"] == "launched"
    assert d["body_exists"] is True and d["can_receive"] is True
    # NO RESUME DECISION TO NAME ANYMORE: launch never walks the
    # lineage now, so the receipt is the plain "body created and live", no resume_check.
    assert d["detail"] == "body created and live"
    assert "resume_check" not in d


async def test_launch_harness_lane_reports_refused_spawn_when_claude_bg_fails(
    actions: Actions,
) -> None:
    """A `claude --bg` that fails to start (OSError, e.g. no such binary) is an honest
    refusal, same taxonomy as the PTY lane's manager-cold/refused-spawn: never a false
    'launched'."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw04", manager_agent="agent:hm04",
        worker_handle="Thoth-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/thoth-test")
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm04", target=worker_seat,
        spawn=_fake_spawn([], raises=OSError("no such file or directory: claude")),
        agents_json=_fake_agents_json([[]]))

    assert d["status"] == "refused-spawn"
    assert "claude --bg" in d["detail"]


async def test_launch_harness_lane_refuses_a_fabricated_project_when_the_charter_disagrees(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE MCP DOOR'S OWN COPY OF AN EARLIER BUG (task #204,
    trigger.launch_seat's harness lane is a SEPARATE call site from
    cli.py's own `_cmd_launch_harness` (independently vulnerable, independently fixed,
    same as the fabricated-tree-cwd specimen before it): same refusal, same shared
    _resolve_launch_project, proven here too rather than assumed from the CLI-door test
    alone."""
    from src.orchestrator.charter import set_charter

    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "trigfabseat"\n')

    await actions.create_or_find_object("SoftwareProject", "repo:trigrealrepo", "test")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw20", manager_agent="agent:hm20",
        worker_handle="trigfabseat", house="osiris")
    await _office(actions, worker_seat, str(office))
    charter = await set_charter(actions, worker_seat, ["trigrealrepo"], actor="operator")
    assert not charter.get("rejected"), charter

    async def _boom(*a: Any, **k: Any) -> Any:
        raise AssertionError("should never be called, the project check refuses first")

    d = await trigger_module.launch_seat(
        actions, caller="agent:hm20", target=worker_seat,
        spawn=_boom, agents_json=_fake_agents_json([[]]))
    assert d["status"] == "refused-fabricated-project"
    assert d["charter_project"] == "trigrealrepo"
    assert d["resolved_project"] == "trigfabseat"
    assert "transition_seat_project" in d["detail"]

    # NEGATIVE CONTROL: pin corrected to agree with the charter, launch proceeds, and
    # the boot prompt names the resolved project explicitly.
    (office / ".osiris").write_text('project = "trigrealrepo"\n')
    spawned: list[dict[str, Any]] = []
    d2 = await trigger_module.launch_seat(
        actions, caller="agent:hm20", target=worker_seat,
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]))
    assert d2["status"] == "launched"
    assert len(spawned) == 1
    assert "working trigrealrepo" in spawned[0]["prompt"]


async def test_launch_harness_lane_records_the_unpriced_cost_honestly(actions: Actions) -> None:
    """THE CEILING'S READ PATH (task #8): a --bg body is a real billed session, same as any
    other: its spend must land in llm_usage even when it is UNPRICED, or the ceiling never
    learns it happened at all (the ghost-farm disease). Never fabricated: cost_usd stays
    NULL, not folded into 0."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw05", manager_agent="agent:hm05",
        worker_handle="Khepri", house="osiris")
    await _office(actions, worker_seat, "/tmp/khepri")
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm05", target=worker_seat,
        spawn=_fake_spawn([]),
        agents_json=_fake_agents_json(
            # LEG 3's leading occupancy-check call (see Bastet test above for why).
            [[], [], [{"cwd": "/tmp/khepri", "sessionId": "khepri-sess", "name": "[OS] Khepri"}]]),
        cost_reader=_fake_cost_reader(
            {"priced": False, "reason": "claude agents --json carries no cost field"}))

    assert d["status"] == "launched"
    rows = await _launch_usage_rows(actions)
    assert len(rows) == 1
    assert rows[0]["purpose"] == "launch" and rows[0]["cost_usd"] is None


async def test_launch_harness_lane_skips_metering_when_not_yet_visible(actions: Actions) -> None:
    """A launch not yet showing up in `claude agents --json` has no REAL session id to look
    up cost for: it is simply not metered THIS cycle, never metered on a guessed id (the
    cost_reader must not even be called)."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw08", manager_agent="agent:hm08",
        worker_handle="Tefnut-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/tefnut-test")
    calls: list[str] = []

    async def _cost(session_id: str, *, cwd: str | None = None) -> dict[str, Any]:
        calls.append(session_id)
        return {"priced": False, "reason": "should never be called"}

    d = await trigger_module.launch_seat(
        actions, caller="agent:hm08", target=worker_seat,
        spawn=_fake_spawn([]), agents_json=_fake_agents_json([[], []]), cost_reader=_cost)

    assert d["status"] == "launched" and d["can_receive"] is False
    assert calls == []
    rows = await _launch_usage_rows(actions)
    assert len(rows) == 0


async def test_launch_harness_lane_records_a_real_price_if_the_reader_has_one(
    actions: Actions,
) -> None:
    """Forward-compatible (mirrors _bg_session_cost's own forward-compat test): if the cost
    reader ever reports a real number, it is RECORDED, not discarded in favor of blindness."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw06", manager_agent="agent:hm06",
        worker_handle="Wadjet", house="osiris")
    await _office(actions, worker_seat, "/tmp/wadjet")
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm06", target=worker_seat,
        spawn=_fake_spawn([]),
        agents_json=_fake_agents_json(
            # LEG 3's leading occupancy-check call (see Bastet test above for why).
            [[], [], [{"cwd": "/tmp/wadjet", "sessionId": "wadjet-sess", "name": "[OS] Wadjet"}]]),
        cost_reader=_fake_cost_reader({"priced": True, "cost_usd": 0.17}))

    assert d["status"] == "launched"
    rows = await _launch_usage_rows(actions)
    assert len(rows) == 1
    assert rows[0]["cost_usd"] == pytest.approx(0.17)


async def test_launch_harness_lane_delivers_the_opening_brief_over_the_mail_lane(
    actions: Actions,
) -> None:
    """Same law as the PTY lane: the message rides the ordinary mail lane as a graded ask,
    never a hand-forged turn: substrate must not change that contract."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw07", manager_agent="agent:hm07",
        worker_handle="Serqet", house="osiris")
    await _office(actions, worker_seat, "/tmp/serqet")
    manc = await actions.create_or_find_object("Agent", "agent:hm07", "test")
    await actions.assert_property(manc, "project", "osiris", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm07", target=worker_seat, message="Welcome, mount and orient.",
        spawn=_fake_spawn([]), agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched" and d.get("brief_message_id")
    row = await actions.pool.fetchrow(
        "SELECT body FROM fleet_messages WHERE id=$1", int(d["brief_message_id"]))
    assert row is not None and "mount and orient" in row["body"]


# ═══ THE RESUME LANE (2026-08-04):
# reversing an earlier "not built, deliberately": launch_seat gains dispatch_dm's own
# resume branch, REUSED via `_resume_guard`/`_agent_resumable`/`_DM_RESUME_PROMPT`, never
# reimplemented. ONE-SHOT: the resumed body runs its turn and exits, re-summonable via the
# next mail wake, not a standing window (an earlier unresolved tradeoff, now settled).
#
# LIVE-FIRE CORRECTION (2026-08-04): the lookup moved from
# agent_mounts (wakeable_identity/_agent_resumable, dispatch_dm's own DM-lane shape) to a
# succession_chain WALK (_lineage_resume_candidate): a `--bg`-launched seat's every
# generation shares ONE durable per-seat mount anchor, so agent_mounts can never encode a
# real per-generation session id; only the graph's own `session` property assertion
# survives. The fixtures below assert `session` directly (succession_chain's own shape),
# never `mounts.save_mount`, the old agent_mounts-only setup silently stopped exercising
# the resume path at all once this fix landed (it still "passed" by falling through to
# mint, for the wrong reason (caught rewriting these tests, not left in).


async def _lineage_holder_with_session(
    actions: Actions, tmp_path: Path, *, agent_id: str, transcript_bytes: int = 16,
    compacted: bool = False,
) -> Path:
    """A seat holder whose resumable session lives ONLY as a graph `session` property
    (succession_chain's own shape) plus a real transcript on disk anchored to that same
    session id, NOT an agent_mounts row, which is exactly the record this fixture proves
    the new lookup no longer needs. Returns the sense root."""
    import os
    import time as _time

    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    signed = ('{"type":"user","toolUseResult":'
              '"{\\"sent\\":1,\\"from\\":\\"' + agent_id + '\\"}"}\n').encode()
    body = signed + (_COMPACT_LINE if compacted else b"") + b"x" * transcript_bytes
    t.write_bytes(body)
    old = _time.time() - 3600
    os.utime(t, (old, old))
    obj = await actions.create_or_find_object("Agent", agent_id, "test")
    await actions.assert_property(obj, "seat_generation", "1", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(obj, "session", FULL_SID, "test", NOW, 0.9,
                                  evidence_class="self_declared")
    return sense


async def _lineage_holder_with_unsigned_session(
    actions: Actions, tmp_path: Path, *, agent_id: str, transcript_bytes: int = 16,
) -> Path:
    """Same shape as `_lineage_holder_with_session`, a real, uncompacted transcript the
    seat's own `session` property points at, but with NO signed testimony anywhere in
    it: a related specimen, the `resident-unknown` class (an absence of
    evidence, never a positive finding of a different mind)."""
    import os
    import time as _time

    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    t.write_bytes(b'{"type":"assistant","text":"just harness chrome, nothing signed"}\n'
                  + b"x" * transcript_bytes)
    old = _time.time() - 3600
    os.utime(t, (old, old))
    obj = await actions.create_or_find_object("Agent", agent_id, "test")
    await actions.assert_property(obj, "seat_generation", "1", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(obj, "session", FULL_SID, "test", NOW, 0.9,
                                  evidence_class="self_declared")
    return sense

async def test_launch_harness_lane_resumes_a_stale_but_resumable_holder(
    actions: Actions, tmp_path: Path,
) -> None:
    """The payoff: a seat whose holder left a resumable session (a graph `session`
    property, the ONLY record that survives the shared-anchor collapse, see this
    section's own header comment) is CONTINUED via its own `-p --resume`, never minted
    fresh: mode='resumed' in the receipt, the shared `_DM_RESUME_PROMPT` (never a
    launch-specific copy), and the spawn call carries resume_session, never job_dir (a
    resume is not a birth). The resumed body's own repo is the SEAT's own launch_cwd,
    deliberately, not a per-generation agent_mounts.cwd, which is exactly the record this
    lookup no longer trusts (see _lineage_resume_candidate's own docstring)."""
    from src.orchestrator.offices import _default_office_root

    sense = await _lineage_holder_with_session(
        actions, tmp_path, agent_id="agent:abcd1234")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:hm-resume",
        worker_handle="Stale-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/stale-test-office")
    # the ACTUAL spawn cwd, post-inversion: the materializer
    # emits to the seat's own DERIVED office (offices.seat_office_target), never the
    # (possibly stale) anchor_cwd `_office` set above: the anchor invariant's own
    # self-healing working as designed.
    real_office = str(_default_office_root() / "stale-test")
    manc = await actions.create_or_find_object("Agent", "agent:hm-resume", "test")
    await actions.assert_property(manc, "project", "osiris", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    resumed: list[dict[str, Any]] = []

    async def _resume_spawn(repo: str, prompt: str, **kw: Any) -> None:
        resumed.append({"repo": repo, "prompt": prompt, **kw})

    async def _clear_stale_record(job_dir_key: str) -> bool:
        return False

    d = await trigger_module.resume_seat(
        actions, caller="agent:hm-resume", target=worker_seat,
        message="pick up where you left off",
        settings=_settings(enabled=True, sense=str(sense)),
        resume_spawn=_resume_spawn, agents_json=_fake_agents_json([[]]),
        clear_stale_record=_clear_stale_record)

    assert d["status"] == "launched" and d["mode"] == "resumed"
    assert d["session"] == FULL_SID
    assert d["body_exists"] is True and d["can_receive"] is True
    assert d.get("brief_message_id")
    # THE RECEIPT NAMES THE DECISION: which generation, how far back.
    assert d["resume_check"] == [
        f"gen 1 (session {FULL_SID[:8]}, 0.00MB store tail): resumable, 0 hop(s) back"]
    assert "gen 1" in d["detail"] and "1 generation(s) back" in d["detail"]
    assert len(resumed) == 1
    call = resumed[0]
    assert call["repo"] == real_office  # the seat's own DERIVED office
    assert call.get("resume_session") == FULL_SID
    assert "job_dir" not in call  # a resume is not a birth (mirrors dispatch_dm's own call)
    assert "private" in call["prompt"] and "seat" in call["prompt"]  # _DM_RESUME_PROMPT itself
    # THE ORDERING GUARANTEE: the brief landed in mail BEFORE the spawn was even issued:
    # a one-shot resumed body's first turn IS its inbox() check, no boot lag to hide behind.
    row = await actions.pool.fetchrow(
        "SELECT body FROM fleet_messages WHERE id=$1", int(d["brief_message_id"]))
    assert row is not None and "pick up where you left off" in row["body"]


# ═══ THE ZERO-HOP GRAPH DOOR (#173a, a real incident, 2026-08-18 00:41Z) ════════════
# `osiris launch` found a resumable candidate (gen 8, 0 hops) and the resident-unknown guard
# refused it: no signed osiris act had been written into that transcript yet, so the
# testimony arm honestly answered "unknown": an absence of evidence, not a stranger. The
# operator resumed it by hand anyway, correctly. `_zero_hop_graph_corroborates` is the
# named, narrow door that now lets exactly this shape through: never for an ancestor hop,
# never when the repo lands anywhere but the seat's own launch location.

async def test_launch_harness_lane_resumes_a_zero_hop_candidate_with_no_signed_testimony(
    actions: Actions, tmp_path: Path,
) -> None:
    """The exact incident shape: a fresh, anchored, resumable transcript for the seat's
    CURRENT holder (0 hops back) that never wrote a single signed osiris act: the
    testimony arm alone would refuse this as resident-unknown. The zero-hop graph door
    corroborates it a different way (the graph's own `session` stamp + the seat's own
    launch location) and the resume proceeds anyway."""
    sense = tmp_path / "projects"
    proj = sense / "-repo-ferryman"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    t.write_bytes(b'{"type":"assistant","text":"booting"}\n' + b"x" * 16)  # no signed act
    obj = await actions.create_or_find_object("Agent", "agent:ferry1234", "test")
    await actions.assert_property(obj, "seat_generation", "1", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(obj, "session", FULL_SID, "test", NOW, 0.9,
                                  evidence_class="self_declared")
    from src.orchestrator.offices import _default_office_root

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:ferry1234", manager_agent="agent:hm-ferry",
        worker_handle="Ferryman-Test", house="osiris")
    await _office(actions, worker_seat, "/repo/ferryman")
    # see test_launch_harness_lane_resumes_a_stale_but_resumable_holder's own comment:
    # post-inversion the spawn cwd is the DERIVED office, never the anchor_cwd set above.
    real_office = str(_default_office_root() / "ferryman-test")
    manc = await actions.create_or_find_object("Agent", "agent:hm-ferry", "test")
    await actions.assert_property(manc, "project", "osiris", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    resumed: list[dict[str, Any]] = []

    async def _resume_spawn(repo: str, prompt: str, **kw: Any) -> None:
        resumed.append({"repo": repo, "prompt": prompt, **kw})

    async def _clear_stale_record(job_dir_key: str) -> bool:
        return False

    d = await trigger_module.resume_seat(
        actions, caller="agent:hm-ferry", target=worker_seat,
        message="pick it back up",
        settings=_settings(enabled=True, sense=str(sense)),
        resume_spawn=_resume_spawn, agents_json=_fake_agents_json([[]]),
        clear_stale_record=_clear_stale_record)

    assert d["status"] == "launched" and d["mode"] == "resumed"
    assert d["session"] == FULL_SID
    assert len(resumed) == 1 and resumed[0]["repo"] == real_office
    assert resumed[0].get("resume_session") == FULL_SID


async def test_zero_hop_graph_door_never_fires_one_hop_back(
    actions: Actions, tmp_path: Path,
) -> None:
    """The door is named and narrow: an otherwise-identical unsigned, resumable transcript
    ONE hop back (the predecessor's session, not the current holder's own) must still
    refuse resident-unknown: the graph door only ever opens for hop 0. Composed with
    an earlier fix: a resident-unknown refusal now REFUSES THE WHOLE LAUNCH
    rather than falling through to a fresh mint: this is the exact case that fix exists
    for, one hop back is simply not eligible for the zero-hop door's own exception."""
    sense = tmp_path / "projects"
    proj = sense / "-repo-ferryman2"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    t.write_bytes(b'{"type":"assistant","text":"booting"}\n' + b"x" * 16)  # no signed act
    pred = await actions.create_or_find_object("Agent", "agent:ferry2", "test")
    await actions.assert_property(pred, "seat_generation", "1", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(pred, "session", FULL_SID, "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(
        (await actions.create_or_find_object("Agent", "agent:ferry2-ii", "test")),
        "succeeded_from", "agent:ferry2", "test", NOW, 0.9,
        evidence_class="self_declared")  # minted, never mounted, no session of its own
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:ferry2-ii", manager_agent="agent:hm-ferry2",
        worker_handle="Ferryman2-Test", house="osiris")
    await _office(actions, worker_seat, "/repo/ferryman2")
    manc = await actions.create_or_find_object("Agent", "agent:hm-ferry2", "test")
    await actions.assert_property(manc, "project", "osiris", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    async def _boom_resume(*a: Any, **kw: Any) -> None:
        raise AssertionError("one hop back must never clear the graph door")

    d = await trigger_module.resume_seat(
        actions, caller="agent:hm-ferry2", target=worker_seat,
        settings=_settings(enabled=True, sense=str(sense)),
        resume_spawn=_boom_resume,
        agents_json=_fake_agents_json([[]]))

    assert d["status"] == "refused-resume-unknown"
    assert d["session"] == FULL_SID
    assert d["body_exists"] is False and d["can_receive"] is False


async def test_launch_harness_lane_walks_past_a_zero_turn_generation(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE LIVE-FIRE CASE ITSELF: the seat's CURRENT holder is a
    generation minted at a compaction seam whose body never ran (no `session` asserted at
    all): the walk must go ONE HOP BACK to the predecessor's own resumable session,
    exactly as succession_chain's own generation-2 test fixture shapes it, rather than
    reporting the whole seat unresumable the instant the newest generation turns out
    stillborn."""
    # NAMING NOTE: "agent:seat-zt" (no suffix) is generation 1 by _generation()'s own
    # convention (agent:x = gen 1; agent:x-ii = gen 2), a "-i" suffix on the ROOT would
    # parse as its OWN base (roman "i" = 1, which _generation only splits at >= 2), landing
    # the signed-tail check on a different base than the successor's and refusing as a
    # false crossed-registry mismatch. Matched to house convention, not worked around.
    sense = await _lineage_holder_with_session(
        actions, tmp_path, agent_id="agent:seat-zt")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:seat-zt-ii", manager_agent="agent:hm-zt",
        worker_handle="ZeroTurn-Test", house="osiris")
    await actions.assert_property(
        (await actions.create_or_find_object("Agent", "agent:seat-zt-ii", "test")),
        "succeeded_from", "agent:seat-zt", "test", NOW, 0.9,
        evidence_class="self_declared")  # minted, never mounted, no seat_generation/session
    await _office(actions, worker_seat, "/tmp/zeroturn-test")
    resumed: list[dict[str, Any]] = []

    async def _resume_spawn(repo: str, prompt: str, **kw: Any) -> None:
        resumed.append({"repo": repo, "prompt": prompt, **kw})

    async def _clear_stale_record(job_dir_key: str) -> bool:
        return False

    d = await trigger_module.resume_seat(
        actions, caller="agent:hm-zt", target=worker_seat,
        settings=_settings(enabled=True, sense=str(sense)),
        resume_spawn=_resume_spawn,
        agents_json=_fake_agents_json([[]]), clear_stale_record=_clear_stale_record)

    assert d["status"] == "launched" and d["mode"] == "resumed"
    assert d["session"] == FULL_SID
    assert len(resumed) == 1 and resumed[0].get("resume_session") == FULL_SID
    assert d["resume_check"][0] == "gen None: minted but never mounted, no session to check"
    assert "resumable, 1 hop(s) back" in d["resume_check"][1]


async def test_launch_harness_lane_always_mints_fresh_even_when_nothing_was_ever_mounted(
    actions: Actions,
) -> None:
    """The ordinary case (no prior mount at all) mints fresh, unconditionally now (task
    #199 lane 3C): launch no longer runs a resume check of its own at all,
    so there is nothing left to "fall through" from. The sibling refusal case (resume_seat
    refuses rather than falling through to a mint when nothing is resumable) lives right
    after this one, on the function that now owns that decision."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw-fresh", manager_agent="agent:hm-fresh",
        worker_handle="Fresh-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/fresh-test")
    spawned: list[dict[str, Any]] = []

    d = await trigger_module.launch_seat(
        actions, caller="agent:hm-fresh", target=worker_seat, substrate="harness",
        settings=_settings(enabled=True, sense=""),
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched" and "mode" not in d
    assert len(spawned) == 1
    assert "resume_check" not in d  # no resume decision was ever made to name


async def test_resume_seat_refuses_rather_than_falling_through_when_nothing_is_resumable(
    actions: Actions,
) -> None:
    """THE ONE DELIBERATE BEHAVIOR CHANGE from launch_seat's former combined lane:
    resume answers exactly one question, and "nothing to resume" is a real
    answer to it: never an invitation to mint fresh instead, mirroring the CLI's own
    _cmd_resume_harness precedent exactly ("never falls through to a fresh mint")."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw-fresh2", manager_agent="agent:hm-fresh2",
        worker_handle="Fresh-Test-2", house="osiris")
    await _office(actions, worker_seat, "/tmp/fresh-test-2")

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("nothing resumable exists, resume_spawn must never be called")

    d = await trigger_module.resume_seat(
        actions, caller="agent:hm-fresh2", target=worker_seat,
        settings=_settings(enabled=True, sense=""),
        resume_spawn=_boom, agents_json=_fake_agents_json([[]]))

    assert d["status"] == "refused-nothing-to-resume"
    assert d["body_exists"] is False and d["can_receive"] is False
    assert "resume_check" in d


async def test_launch_harness_lane_resumes_zero_hop_unsigned_via_the_graph_door_not_a_refusal(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE COMPOSITION OF an earlier fix AND #173a: a resumable session with
    NO signed testimony, for the seat's OWN CURRENT holder (hop 0, no predecessor at all)
    : the exact shape this test used to expect a hard refusal for, back when the earlier fix
    shipped alone. Once #173a's zero-hop graph door lands beside it, THIS fixture is
    precisely the case that door exists to let through: `_zero_hop_graph_corroborates`
    clears the gate before the earlier fix's `resident-unknown` refusal branch is ever reached,
    so the resume proceeds. `test_zero_hop_graph_door_never_fires_one_hop_back` is the
    sibling proof that a NON-zero-hop resident-unknown case still hits the earlier fix's refusal
    (refused-resume-unknown, nothing spawned): the two tests together are the real
    contract: hop 0 resumes, hop >=1 refuses, never a fresh mint either way."""
    sense = await _lineage_holder_with_unsigned_session(
        actions, tmp_path, agent_id="agent:unkhold")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:unkhold", manager_agent="agent:hm-unk",
        worker_handle="Unknown-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/unknown-test")

    resumed: list[dict[str, Any]] = []

    async def _resume_spawn(repo: str, prompt: str, **kw: Any) -> None:
        resumed.append({"repo": repo, "prompt": prompt, **kw})

    async def _clear_stale_record(job_dir_key: str) -> bool:
        return False

    d = await trigger_module.resume_seat(
        actions, caller="agent:hm-unk", target=worker_seat,
        settings=_settings(enabled=True, sense=str(sense)),
        resume_spawn=_resume_spawn, agents_json=_fake_agents_json([[]]),
        clear_stale_record=_clear_stale_record)

    assert d["status"] == "launched" and d["mode"] == "resumed"
    assert d["session"] == FULL_SID
    assert len(resumed) == 1 and resumed[0].get("resume_session") == FULL_SID


async def test_lineage_resume_candidate_returns_hop_explicitly_not_via_log_length(
    actions: Actions, tmp_path: Path,
) -> None:
    """Regression for the #200 residual (a live specimen).
    A materialize refusal appends a SECOND log
    line for the SAME winning hop ("materialize refused: a LIVE transcript exists at
    the target"): a caller that derived hop via `len(log) - 1` on the documented
    one-line-per-hop assumption would miscount a genuine hop-0 candidate as hop 1,
    silently denying it `_zero_hop_graph_corroborates`'s own zero-hop fallback.
    `_lineage_resume_candidate` must return the hop NUMBER directly (the tuple's 6th
    field) so no caller ever has to re-derive it from log shape again."""
    import os
    import time as _time

    sense = await _lineage_holder_with_unsigned_session(
        actions, tmp_path, agent_id="agent:hopcount1")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hopcount1", manager_agent="agent:hm-hopcount",
        worker_handle="HopCount-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/hopcount-test")

    from src.orchestrator.mounts import _harness_slug
    from src.orchestrator.offices import _default_office_root

    office = str(_default_office_root() / "hopcount-test")
    dest_dir = sense / _harness_slug(office)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{FULL_SID}.jsonl"
    # SAME CONTENT as the fixture's real transcript (sense/-repo-demo/{FULL_SID}.jsonl):
    # discovery's own glob (locate_current_transcript, `*/*.jsonl` root-wide, newest-mtime-
    # wins among same-stem anchors) will find WHICHEVER of the two this test's own future
    # mtime makes newest; garbage content there would break ingestion itself rather than
    # exercising the materialize-refusal path this test is actually after. Byte-identical
    # to that seat's own live specimen too: the stray file blocking rematerialize there was
    # a genuine full copy, not a stub.
    dest.write_bytes((sense / "-repo-demo" / f"{FULL_SID}.jsonl").read_bytes())
    future = _time.time() + 3600  # newer than any last_ingested_at the store will record
    os.utime(dest, (future, future))

    st = _settings(enabled=True, sense=str(sense))
    outcome = await trigger_module._lineage_resume_candidate(
        actions.pool, "agent:hopcount1", st, repo="/tmp/hopcount-test",
        seat_id=worker_seat)

    assert isinstance(outcome, tuple)
    candidate, log = outcome
    assert len(log) == 2  # the resumable line, THEN the materialize-refusal line
    assert "resumable, 0 hop(s) back" in log[0]
    assert "materialize refused" in log[1]
    assert candidate[5] == 0  # THE FIX: read directly, never len(log) - 1 (which is 1, wrong)
    assert candidate[4] is None  # materialized_at, refused, so no office to spawn into


async def test_lineage_resume_candidate_hop_still_zero_for_an_ordinary_single_line_hop(
    actions: Actions, tmp_path: Path,
) -> None:
    """Negative control for the fix above: the ordinary case (materialize succeeds, one
    log line per hop) must still report hop=0 correctly: proving the explicit field is
    right for the common shape too, not only the two-line specimen."""
    sense = await _lineage_holder_with_session(
        actions, tmp_path, agent_id="agent:hopcount2")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hopcount2", manager_agent="agent:hm-hopcount2",
        worker_handle="HopCount2-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/hopcount2-test")

    st = _settings(enabled=True, sense=str(sense))
    outcome = await trigger_module._lineage_resume_candidate(
        actions.pool, "agent:hopcount2", st, repo="/tmp/hopcount2-test",
        seat_id=worker_seat)

    assert isinstance(outcome, tuple)
    candidate, log = outcome
    assert len(log) == 1
    assert candidate[5] == 0
    assert candidate[4] is not None  # materialized_at, succeeded this time


async def test_lineage_resume_candidate_degrades_gracefully_when_soul_key_missing(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE KEY DOOR, defect 2: a missing
    soul-store key used to surface as a raw `SoulKeyMissing` traceback straight out of
    `osiris resume`, a READ path, which must degrade to a one-line log entry and keep
    walking the rest of the chain, never abort the whole resume. Boot refusal for the
    worker/MCP itself stays exactly as ruled elsewhere; only this read path changed."""
    from src.ingest.soul_crypto import SoulKeyMissing
    from src.ingest.soul_store import SoulStore

    sense = await _lineage_holder_with_session(actions, tmp_path, agent_id="agent:keymiss1")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:keymiss1", manager_agent="agent:hm-keymiss1",
        worker_handle="KeyMiss-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/keymiss-test")

    async def _raise_missing(self: SoulStore, anchor_sid: str, harness: str = "claude-code",
                             ) -> tuple[int, int, int] | None:
        raise SoulKeyMissing(
            "no soul-store encryption key found at /fake/soul.key, run "
            "`osiris soul-key init` ONCE")

    monkeypatch.setattr(SoulStore, "resume_diagnostics", _raise_missing)

    st = _settings(enabled=True, sense=str(sense))
    outcome = await trigger_module._lineage_resume_candidate(
        actions.pool, "agent:keymiss1", st, repo="/tmp/keymiss-test", seat_id=worker_seat)

    # no candidate cleared both gates (the one hop's own diagnostics were unreadable),
    # but the walk finished: never an uncaught SoulKeyMissing propagating out
    assert isinstance(outcome, list)
    assert any(
        "soul store locked" in line and "run osiris soul-key init" in line
        for line in outcome), outcome


async def _lineage_holder_with_truncated_session(
    actions: Actions, tmp_path: Path, *, agent_id: str, full_sid: str = FULL_SID,
) -> tuple[Path, str]:
    """PRODUCTION SHAPE, not the other fixtures' shape (operator's own live catch,
    2026-09-03): the graph's `session` property is the 8-char
    TRUNCATED anchor (`sid[:8]`, agents.py's own convention): every OTHER fixture in
    this file stamps the property with the SAME full uuid the transcript file is named
    by, which never exercises the truncation bug at all (the whole reason it went
    unnoticed by this suite). Here the property and the filename genuinely diverge, the
    same divergence a real graph carries. Returns (sense_root,
    truncated_session)."""
    import os
    import time as _time

    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{full_sid}.jsonl"
    signed = ('{"type":"user","toolUseResult":'
              '"{\\"sent\\":1,\\"from\\":\\"' + agent_id + '\\"}"}\n').encode()
    t.write_bytes(signed + b"x" * 16)
    old = _time.time() - 3600
    os.utime(t, (old, old))
    truncated = full_sid.split("-")[0]
    obj = await actions.create_or_find_object("Agent", agent_id, "test")
    await actions.assert_property(obj, "seat_generation", "1", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(obj, "session", truncated, "test", NOW, 0.9,
                                  evidence_class="self_declared")
    return sense, truncated


async def test_lineage_resume_candidate_returns_the_full_session_id_not_the_graphs_own_truncation(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE FIX ITSELF: `claude --bg
    --resume <8-char-anchor>` matches nothing in the harness's own index and silently
    mints a fresh, disposable session instead of erroring: the exact failure the
    operator caught with the harness's own resume picker after this suite's own
    (pre-fix) tests all reported a clean pass, because every other fixture's `session`
    property already held the FULL id and never exercised the truncation. The candidate
    returned must be the file's own full stem, read off `soul_sessions.source_path`,
    never the graph's shorter key."""
    sense, truncated = await _lineage_holder_with_truncated_session(
        actions, tmp_path, agent_id="agent:fullsid1")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:fullsid1", manager_agent="agent:hm-fullsid1",
        worker_handle="FullSid-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/fullsid-test")

    st = _settings(enabled=True, sense=str(sense))
    outcome = await trigger_module._lineage_resume_candidate(
        actions.pool, "agent:fullsid1", st, repo="/tmp/fullsid-test",
        seat_id=worker_seat, materialize=False)

    assert isinstance(outcome, tuple)
    candidate, _log = outcome
    assert candidate[0] == FULL_SID
    assert candidate[0] != truncated


async def test_lineage_resume_candidate_refuses_a_hop_with_only_a_truncated_stub_on_disk(
    actions: Actions, tmp_path: Path,
) -> None:
    """Negative control / the safety half of the fix: when the ONLY discoverable file
    for this hop is itself named by the bare anchor (e.g. an earlier materialize wrote
    its own destination the same truncated way, before that was fixed too), there is no
    genuine full id to hand back: this hop must refuse honestly rather than return a
    value already known to be unusable against the harness's own `--resume`."""
    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    truncated = FULL_SID.split("-")[0]
    t = proj / f"{truncated}.jsonl"  # named by the BARE anchor, no fuller id at all
    signed = (b'{"type":"user","toolUseResult":'
              b'"{\\"sent\\":1,\\"from\\":\\"agent:stubonly\\"}"}\n')
    t.write_bytes(signed + b"x" * 16)
    obj = await actions.create_or_find_object("Agent", "agent:stubonly", "test")
    await actions.assert_property(obj, "seat_generation", "1", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(obj, "session", truncated, "test", NOW, 0.9,
                                  evidence_class="self_declared")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:stubonly", manager_agent="agent:hm-stubonly",
        worker_handle="StubOnly-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/stubonly-test")

    st = _settings(enabled=True, sense=str(sense))
    outcome = await trigger_module._lineage_resume_candidate(
        actions.pool, "agent:stubonly", st, repo="/tmp/stubonly-test",
        seat_id=worker_seat, materialize=False)

    assert not isinstance(outcome, tuple)  # no candidate, refused, never a broken id
    assert any("truncated" in line or "synthetic" in line for line in outcome)


async def test_resume_seat_refuses_a_tail_closed_at_the_seam(
    actions: Actions, tmp_path: Path,
) -> None:
    """#156's rebuild (2026-08-09, the operator's own correction), re-homed to resume_seat
    (task #199 lane 3C, the one deliberate behavior change: this used to
    fall through to a fresh mint on launch_seat's old combined lane; resume now REFUSES
    instead, never falls through, matching resume_seat's own new contract): the floor
    still refuses a transcript whose tail since its last compaction is genuinely tiny:
    the seam-itself case, the operator's own "rare special case." The receipt still NAMES
    the refusal with real numbers, not a silent one. (Compacting once and then doing real
    work IS resumed, see the sibling test right after this one; the fixture here
    deliberately leaves only 16 raw bytes after the boundary, well under the floor set
    below.)"""
    sense = await _lineage_holder_with_session(
        actions, tmp_path, agent_id="agent:abcd1234", compacted=True)
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:hm-compact",
        worker_handle="Compacted-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/compacted-test")

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a tail closed at the seam itself must never be resumed")

    d = await trigger_module.resume_seat(
        actions, caller="agent:hm-compact", target=worker_seat,
        settings=_settings(enabled=True, sense=str(sense), min_tail_bytes=1000),
        resume_spawn=_boom, agents_json=_fake_agents_json([[]]))

    assert d["status"] == "refused-nothing-to-resume"
    assert d["body_exists"] is False and d["can_receive"] is False
    # THE REFUSAL IS NAMED, NOT SILENT: the receipt carries which generation, its size, and
    # the specific gate that fired: a human reads one line instead of re-deriving it.
    assert len(d["resume_check"]) == 1
    reason = d["resume_check"][0]
    assert "gen 1" in reason and f"session {FULL_SID[:8]}" in reason
    assert "compaction boundary itself" in reason
    assert "min_tail_bytes=1000" in d["detail"]


async def test_resume_seat_resumes_a_compacted_transcript_with_real_tail_work(
    actions: Actions, tmp_path: Path,
) -> None:
    """The sibling and the whole point of #156's rebuild: a holder whose transcript
    compacted once but carries real work since that boundary IS resumed, not refused
    (a related live specimen, 12 compactions, 4.07MB of real work after the last
    one), in miniature."""
    sense = await _lineage_holder_with_session(
        actions, tmp_path, agent_id="agent:abcd1234", compacted=True)
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:hm-compact-2",
        worker_handle="Compacted-Test-2", house="osiris")
    await _office(actions, worker_seat, "/tmp/compacted-test-2")
    resumed: list[dict[str, Any]] = []

    async def _resume_spawn(repo: str, prompt: str, **kw: Any) -> None:
        resumed.append(kw)

    async def _clear_stale_record(job_dir_key: str) -> bool:
        return False

    d = await trigger_module.resume_seat(
        actions, caller="agent:hm-compact-2", target=worker_seat,
        settings=_settings(enabled=True, sense=str(sense), min_tail_bytes=1),
        resume_spawn=_resume_spawn, agents_json=_fake_agents_json([[]]),
        clear_stale_record=_clear_stale_record)

    assert d["status"] == "launched" and d.get("mode") == "resumed"
    assert resumed and resumed[0].get("resume_session") == FULL_SID


# ═══ tree_cwd (task #103's re-scope): the office/code split. ═══

async def test_launch_refuses_a_tree_cwd_that_does_not_exist_on_disk(
    actions: Actions, tmp_path: Path,
) -> None:
    """OSIRIS NEVER PROVISIONS THE TREE (harness owns isolation): a seat naming a
    tree_cwd the harness never actually created is refused, cleanly, before anything spawns."""
    from src.orchestrator.seats import bind_seat_tree

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:tw01", manager_agent="agent:tm01",
        worker_handle="Notree", house="osiris")
    await _office(actions, worker_seat, str(tmp_path / "office"))
    ghost_tree = str(tmp_path / "never-created")
    bind = await bind_seat_tree(actions, seat_id=worker_seat, tree_cwd=ghost_tree,
                                actor="operator", because="test: refusal proof")
    assert bind.get("error") is None
    d = await trigger_module.launch_seat(
        actions, caller="agent:tm01", target=worker_seat,
        spawn=_fake_spawn([]), agents_json=_fake_agents_json([[]]))
    assert d["status"] == "refused-no-tree"
    assert ghost_tree in d["detail"] and "never provisions" in d["detail"]


async def test_launch_spawns_into_tree_cwd_not_office_pty_lane(
    actions: Actions, tmp_path: Path,
) -> None:
    """The office (identity) and the tree (code) are DISTINCT: a bound, real tree_cwd is
    where the body actually spawns; office stays only the identity anchor in the receipt."""
    from src.orchestrator.seats import bind_seat_tree

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:tw02", manager_agent="agent:tm02",
        worker_handle="Treewalker", house="osiris")
    office = tmp_path / "office"
    office.mkdir()
    tree = tmp_path / "worktree"
    tree.mkdir()
    await _office(actions, worker_seat, str(office))
    await bind_seat_tree(actions, seat_id=worker_seat, tree_cwd=str(tree), actor="operator",
                         because="test: spawn location proof")
    record: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:tm02", target=worker_seat, substrate="pty",
        manager=_fake_manager(record), windows=_fake_windows([]))
    assert d["status"] == "launched"
    assert record[0]["cwd"] == str(tree)               # spawned INTO the tree, not the office
    assert d["attach"]["office"] == str(office)          # identity anchor unchanged
    assert d["attach"]["tree_cwd"] == str(tree)          # and named in the receipt


async def test_launch_spawns_into_tree_cwd_not_office_harness_lane(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import bind_seat_tree

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:tw03", manager_agent="agent:tm03",
        worker_handle="Treewalker2", house="osiris")
    office = tmp_path / "office2"
    office.mkdir()
    tree = tmp_path / "worktree2"
    tree.mkdir()
    await _office(actions, worker_seat, str(office))
    await bind_seat_tree(actions, seat_id=worker_seat, tree_cwd=str(tree), actor="operator",
                         because="test: harness spawn location proof")
    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:tm03", target=worker_seat,
        spawn=_fake_spawn(spawned), agents_json=_fake_agents_json([[]]))
    assert d["status"] == "launched"
    assert spawned[0]["repo"] == str(tree)
    # the boot prompt still anchors mount() AT THE OFFICE: identity never follows the tree
    assert str(office) in spawned[0]["prompt"]
    assert str(tree) not in spawned[0]["prompt"]


async def test_launch_harness_lane_idempotency_matches_on_tree_cwd_not_office(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE CORRECTNESS PROOF: a tree-bound seat's live process sits at tree_cwd. Matching
    idempotency on `office` alone (the pre-fix shape) would never find it and would twin on
    every relaunch: this proves the fix reads the actual launch location, not the office."""
    from src.orchestrator.seats import bind_seat_tree

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:tw04", manager_agent="agent:tm04",
        worker_handle="Treewalker3", house="osiris")
    office = tmp_path / "office3"
    office.mkdir()
    tree = tmp_path / "worktree3"
    tree.mkdir()
    await _office(actions, worker_seat, str(office))
    await bind_seat_tree(actions, seat_id=worker_seat, tree_cwd=str(tree), actor="operator",
                         because="test: idempotency proof")
    spawned: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:tm04", target=worker_seat,
        spawn=_fake_spawn(spawned),
        agents_json=_fake_agents_json([[{"cwd": str(tree), "name": "[OS] Treewalker3"}]]))
    assert d["status"] == "already-live"
    assert spawned == []                                # no twin


# ═══ vacate_dead_seat: THE LIVENESS
# CONVERGENCE FIX, the evidence-
# gathering complement to seats.vacate_holder / seats.retire_seat's stale-holder
# refusal, never its bypass. `liveness_fn` is injected so tests assert the DECISION
# without depending on the real `mounts.agent_liveness` cache state.

def _fake_liveness(live: bool) -> Any:
    async def _fn(pool: Any, agent_id: str) -> dict[str, Any]:
        return {"live": live, "last_seen": "2026-01-01T00:00:00+00:00",
               "ever_mounted": True}
    return _fn


async def test_vacate_dead_seat_refuses_a_vacant_seat(actions: Actions) -> None:
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:vd1holdr", manager_agent="agent:vd1mgr0",
        worker_handle="Ptah-Vacant", house="osiris")
    await _office(actions, worker_seat, "/tmp/ptah-vacant")
    # unbind: no holder at all: the fixture above binds one, so start from a fresh seat
    from src.orchestrator import trigger as tm
    seat2 = "seat:vd1empty"
    await actions.create_or_find_object("Seat", seat2, "test")

    d = await tm.vacate_dead_seat(actions, seat_id=seat2, actor="test", because="dead")
    assert d["status"] == "refused-vacant"


async def test_vacate_dead_seat_refuses_no_office(actions: Actions) -> None:
    from src.orchestrator import trigger as tm
    from src.orchestrator.seats import bind_holder

    seat_id = "seat:vd2noofc"
    await actions.create_or_find_object("Seat", seat_id, "test")
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:vd2holdr", source="test")

    d = await tm.vacate_dead_seat(actions, seat_id=seat_id, actor="test", because="dead")
    assert d["status"] == "refused-no-office"


async def test_vacate_dead_seat_refuses_when_agent_liveness_says_live(
    actions: Actions,
) -> None:
    """THE SINGLE SOURCE: `agent_liveness` saying live is
    enough to refuse on its own: the same instrument `team()`'s `live` column and
    resume's occupancy gate already answer with, never a second, disagreeing check."""
    from src.orchestrator import trigger as tm

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:vd3holdr", manager_agent="agent:vd3mgr0",
        worker_handle="Sekhmet-Alive", house="osiris")
    await _office(actions, worker_seat, "/tmp/sekhmet-alive")

    d = await tm.vacate_dead_seat(
        actions, seat_id=worker_seat, actor="test", because="dead",
        liveness_fn=_fake_liveness(live=True))
    assert d["status"] == "refused-live"
    assert "agent:vd3holdr" in d["detail"]


async def test_vacate_dead_seat_vacates_when_agent_liveness_says_dead(
    actions: Actions,
) -> None:
    """The core: `agent_liveness` says dead → vacated: proof this reaches
    seats.vacate_holder's own write, not just a receipt shape."""
    from src.orchestrator import trigger as tm

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:vd6corps", manager_agent="agent:vd6mgr0",
        worker_handle="Khepri-Dead", house="osiris")
    await _office(actions, worker_seat, "/tmp/khepri-dead")

    d = await tm.vacate_dead_seat(
        actions, seat_id=worker_seat, actor="test", because="process confirmed dead",
        liveness_fn=_fake_liveness(live=False))
    assert d["status"] == "vacated"
    assert d["was_held_by"] == ["agent:vd6corps"]
    assert d["evidence"]["liveness"]["live"] is False
    holder = await actions.pool.fetchval(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", worker_seat)
    assert holder is None


async def test_vacate_dead_seat_uses_the_real_agent_liveness_by_default(
    actions: Actions,
) -> None:
    """No injected `liveness_fn` → the real `mounts.agent_liveness` runs, reading the
    freshly-bound holder's own agent_mounts row as live (never vacated): proof the
    default wiring is the shared instrument, not a silent no-op."""
    from src.orchestrator import trigger as tm
    from src.orchestrator.mounts import save_mount

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:vd7fresh", manager_agent="agent:vd7mgr0",
        worker_handle="Anubis-Fresh", house="osiris")
    await _office(actions, worker_seat, "/tmp/anubis-fresh")
    await save_mount(actions.pool, job_dir="/jobs/anubis-fresh", agent_id="agent:vd7fresh",
                     project="osiris", cwd="/tmp/anubis-fresh", model="claude-sonnet-5",
                     session_key=None)

    d = await tm.vacate_dead_seat(actions, seat_id=worker_seat, actor="test", because="dead")
    assert d["status"] == "refused-live"


# ═══ THE HARNESS-NATIVE SUBSTRATE (task #68 item 9) ══════
# `claude --bg` + `claude agents --json` instead of the manager daemon's PTY broker. Same
# hermetic discipline as _spawn_claude's own tests: `trigger.asyncio.create_subprocess_exec` is
# monkeypatched, never a real `claude` process.


async def test_spawn_claude_bg_issues_the_documented_bg_flags(monkeypatch: Any) -> None:
    """`--bg` + `-n` + `--model` + a trailing prompt: the sanctioned flags the spike
    verified, never the undocumented daemon claim-socket, and NEVER `--session-id` (live
    finding, 2026-07-27: `--bg` manages its own session id and silently ignores an explicit
    one, see _spawn_claude_bg's own docstring). Fire-and-forget: NOTHING here awaits the
    process (same B1 scar _spawn_claude's tests guard), so a fake proc with just a pid
    satisfies the call."""
    from src.orchestrator import trigger

    captured: dict[str, Any] = {}

    class _Proc:
        pid = 4242

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        captured["args"] = args
        captured["env"] = kwargs.get("env")
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    monkeypatch.setattr(trigger, "_tree_exists", lambda p: True)
    await trigger._spawn_claude_bg(
        "/home/asuramaya/.osiris/seats/nefer", name="[OS] Nefer",
        model="claude-sonnet-5", prompt="mount and claim_name")

    assert captured["args"][:2] == ("claude", "--bg")
    pairs = _pairs(captured["args"])
    assert ("-n", "[OS] Nefer") in pairs
    assert ("--model", "claude-sonnet-5") in pairs
    assert not any(a == "--session-id" for a in captured["args"])
    assert captured["args"][-1] == "mount and claim_name"  # the trailing positional prompt


async def test_spawn_claude_bg_issues_resume_before_the_other_flags(monkeypatch: Any) -> None:
    """`--resume <session-id>` genuinely continues a `--bg`
    session under the SAME background id (verified live against harness 2.1.258, a real
    disposable probe, see this function's own docstring): #173's "silently ignores"
    premise was true of an older harness and is false now. `osiris resume` rides this."""
    from src.orchestrator import trigger

    captured: dict[str, Any] = {}

    class _Proc:
        pid = 99

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        captured["args"] = args
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    monkeypatch.setattr(trigger, "_tree_exists", lambda p: True)
    await trigger._spawn_claude_bg(
        "/home/asuramaya/code/jesus", name="[JE] Jesus", model="claude-sonnet-5",
        prompt="a private DM is waiting", resume_session="b51dab8b-aa3c-4428-87a9")

    pairs = _pairs(captured["args"])
    assert ("--resume", "b51dab8b-aa3c-4428-87a9") in pairs
    assert captured["args"][:2] == ("claude", "--bg")
    assert not any(a == "--session-id" for a in captured["args"])


async def test_spawn_claude_bg_starts_its_own_process_group(monkeypatch: Any) -> None:
    """#156 (independent of the kill verb, this is already a bug
    today): without its own session/group, a body's Bash-tool children share OSIRIS'S OWN
    process group and can outlive their parent with nobody owning them: the exact shape
    of the leaked spares #156.5 found and killed by hand. Every new spawn must be its own
    group leader, the same discipline pty_broker.py's own children already carry."""
    from src.orchestrator import trigger

    captured: dict[str, Any] = {}

    class _Proc:
        pid = 7

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        captured["kwargs"] = kwargs
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    monkeypatch.setattr(trigger, "_tree_exists", lambda p: True)
    await trigger._spawn_claude_bg("/repo/demo")
    assert captured["kwargs"].get("start_new_session") is True


async def test_spawn_claude_bg_never_leaks_the_spawners_own_anchor(monkeypatch: Any) -> None:
    """Same anchor discipline as _spawn_claude (the collision class): the spawner's
    own CLAUDE_JOB_DIR must never reach the child: inert for --bg today (no env var reaches
    a claimed spare either way, live finding 2026-07-27) but cheap and harmless to scrub."""
    from src.orchestrator import trigger

    captured: dict[str, Any] = {}

    class _Proc:
        pid = 1

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        captured["env"] = kwargs.get("env")
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    monkeypatch.setattr(trigger, "_tree_exists", lambda p: True)
    monkeypatch.setenv("CLAUDE_JOB_DIR", "/tmp/jobs/spawner-own-anchor")
    await trigger._spawn_claude_bg("/repo/demo")
    assert "CLAUDE_JOB_DIR" not in captured["env"]


async def test_spawn_claude_bg_omits_the_prompt_argument_when_none_given(
    monkeypatch: Any,
) -> None:
    """No prompt → no trailing positional arg at all, never an empty string (the CLI would
    treat "" as a real, if useless, prompt)."""
    from src.orchestrator import trigger

    captured: dict[str, Any] = {}

    class _Proc:
        pid = 9

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        captured["args"] = args
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    monkeypatch.setattr(trigger, "_tree_exists", lambda p: True)
    await trigger._spawn_claude_bg("/repo/demo", name="bare")
    assert captured["args"] == ("claude", "--bg", "-n", "bare")


async def test_claude_agents_json_parses_a_real_shaped_sample(monkeypatch: Any) -> None:
    """The exact shape sampled live from a running fleet (2026-07-27): background AND
    interactive rows, with and without a `state`/`id` field."""
    from src.orchestrator import trigger

    sample = json.dumps([
        {"pid": 1, "id": "e08c3850", "cwd": "/home/asuramaya/.osiris/seats/imhotep",
         "kind": "background", "sessionId": "e08c3850-4180-4876-b313-fafef21d368a",
         "name": "[OS] Imhotep", "status": "busy", "state": "working"},
        {"pid": 2, "cwd": "/home/asuramaya/.osiris/seats/imhotep", "kind": "interactive",
         "sessionId": "5198945f-d468-4a2d-b794-b9f3a2d364ad", "name": "imhotep-0e",
         "status": "idle"},
    ]).encode()

    class _Proc:
        async def communicate(self) -> tuple[bytes, bytes]:
            return sample, b""

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        captured_argv.append(args)
        return _Proc()

    captured_argv: list[Any] = []
    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    rows = await trigger._claude_agents_json(cwd="/home/asuramaya/.osiris/seats/imhotep")

    assert captured_argv[0] == ("claude", "agents", "--json",
                                "--cwd", "/home/asuramaya/.osiris/seats/imhotep")
    assert len(rows) == 2
    assert rows[0]["id"] == "e08c3850" and rows[0]["status"] == "busy"


async def test_claude_agents_json_fails_open_to_empty_on_error(monkeypatch: Any) -> None:
    """A status read must never break a caller that only wants a roster (same discipline as
    _manager_windows): a dark/missing `claude` binary answers [], not an exception."""
    from src.orchestrator import trigger

    async def _boom(*args: Any, **kwargs: Any) -> Any:
        raise OSError("no such file or directory: claude")

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _boom)
    assert await trigger._claude_agents_json() == []


async def test_bg_session_cost_is_honestly_unpriced_not_fabricated(monkeypatch: Any) -> None:
    """The spike's own open question: `claude agents --json` carries no cost field at all
    (confirmed live against the real fleet): a --bg session's spend must be reported as
    UNPRICED, never a made-up number."""
    from src.orchestrator import trigger

    sample = json.dumps([
        {"id": "e08c3850", "sessionId": "e08c3850-4180-4876-b313-fafef21d368a",
         "status": "idle", "state": "done"},
    ]).encode()

    class _Proc:
        async def communicate(self) -> tuple[bytes, bytes]:
            return sample, b""

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    out = await trigger._bg_session_cost("e08c3850-4180-4876-b313-fafef21d368a")
    assert out == {"priced": False,
                   "reason": "claude agents --json carries no cost field for this session",
                   "session_row": json.loads(sample)[0]}


async def test_bg_session_cost_reports_a_real_number_if_the_harness_ever_adds_one(
    monkeypatch: Any,
) -> None:
    """Forward-compatible: if a future harness version DOES carry a cost field, this reports
    it as priced rather than staying stuck in the unpriced branch forever."""
    from src.orchestrator import trigger

    sample = json.dumps([{"id": "abc", "sessionId": "abc-full", "total_cost_usd": 0.42}]).encode()

    class _Proc:
        async def communicate(self) -> tuple[bytes, bytes]:
            return sample, b""

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    out = await trigger._bg_session_cost("abc-full")
    assert out == {"priced": True, "cost_usd": 0.42}


async def test_bg_session_cost_session_not_found(monkeypatch: Any) -> None:
    from src.orchestrator import trigger

    async def _fake_exec(*args: Any, **kwargs: Any) -> Any:
        class _Proc:
            async def communicate(self) -> tuple[bytes, bytes]:
                return b"[]", b""
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    out = await trigger._bg_session_cost("nonexistent")
    assert out == {"priced": False, "reason": "session not found in claude agents --json"}


# ═══ stop(): the process-lifecycle inverse of launch() (#156's held half)
# ═════════════════════════════════════════════════════════════════════════════

def _fake_census_agents_json(pid: int, *, cwd: str = "/repo/demo",
                             session_id: str = "wg-9999-0000-4000-8000-000000000000",
                             name: str = "[OS] StopTest") -> Any:
    async def _read(**kw: Any) -> list[dict[str, Any]]:
        return [{"sessionId": session_id, "pid": pid, "cwd": cwd, "name": name}]
    return _read


def _fake_claude_exe(pid: int) -> str:
    return "/home/x/.local/share/claude/versions/2.1.210"


async def test_stop_seat_sends_sigterm_to_the_confirmed_live_holder(
    actions: Actions,
) -> None:
    """The happy path: a seat's current holder has a real, harness+/proc-confirmed live
    body: stop() signals its exact pid and records the event on the seat (survives
    succession: it names an event, not a gate on the chair)."""
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:stop01", manager_agent="agent:stopm01",
        worker_handle="Stop-Test", house="osiris")
    await save_mount(actions.pool, job_dir="/x/jobs/wg9999ii", agent_id="agent:stop01",
                            project="osiris", cwd="/repo/demo", model=None, session_key=None)
    killed: list[int] = []

    async def _kill(pid: int, job_dir_key: str | None) -> None:
        killed.append(pid)

    d = await trigger_module.stop_seat(
        actions, caller="agent:stopm01", target=worker_seat,
        agents_json=_fake_census_agents_json(4242, session_id="wg9999ii-0000-4000-8000-"
                                             "000000000000"),
        read_exe=_fake_claude_exe, read_cwd=lambda pid: "/repo/demo", kill=_kill)

    assert d["status"] == "stopped"
    assert d["pid"] == 4242 and d["holder"] == "agent:stop01"
    assert killed == [4242]
    row = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o "
        "ON o.id=a.object_id WHERE o.canonical=$1 AND a.name='stopped_at' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", worker_seat)
    assert row  # a real timestamp landed, not silence
    _ = manager_seat


async def test_stop_seat_records_the_session_anchor_for_a_clean_lineage_before_the_kill(
    actions: Actions,
) -> None:
    """An acceptance finding, closed: a seat
    mounted ONCE and never compacted never had a heartbeat/live_succession call to
    stamp `session` (register_agent's own birth-time write deliberately skips it on a
    `--bg` seat's FIRST mount): a plain SIGTERM used to kill that body with NO graph
    session property anywhere on its lineage, so `_lineage_resume_candidate`'s own
    succession_chain walk (which reads exactly this property, confirmed by reading
    succession.py's query directly) found nothing and reported
    `refused-nothing-to-resume` even though the body just died with a real, resumable
    transcript on disk.

    PROVEN HERE at the graph level (the part stop_seat actually controls; a real
    transcript file matching the session id is a harness-truth fact independent of
    this fix): BEFORE stop, the holder carries no `session` property at all. AFTER,
    it carries the census-confirmed session id (truncated to 8 chars, same shape
    register_agent's own write uses), and succession_chain, read fresh, now finds it
    on hop 0. The sid->soul ledger (`anchor_sid:`, handshake.record_session_anchor)
    lands too, for the SIBLING gap: the session resume() eventually spawns needs its
    own identity resolved from the sid alone once it mounts."""
    from src.orchestrator.succession import succession_chain

    full_sid = "abcd1234-0000-4000-8000-000000000000"
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:stopanchor01", manager_agent="agent:stopanchorm01",
        worker_handle="Stop-Anchor-Test", house="osiris")
    await save_mount(actions.pool, job_dir="/x/jobs/abcd1234", agent_id="agent:stopanchor01",
                            project="osiris", cwd="/repo/demo", model=None, session_key=None)

    before = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o "
        "ON o.id=a.object_id WHERE o.canonical=$1 AND a.name='session' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", "agent:stopanchor01")
    assert before is None  # the exact clean-lineage gap: nothing written yet

    async def _kill(pid: int, job_dir_key: str | None) -> None:
        pass

    d = await trigger_module.stop_seat(
        actions, caller="agent:stopanchorm01", target=worker_seat,
        agents_json=_fake_census_agents_json(7373, session_id=full_sid),
        read_exe=_fake_claude_exe, read_cwd=lambda pid: "/repo/demo", kill=_kill)

    assert d["status"] == "stopped"
    assert d["session_anchor_recorded"] is True

    after = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o "
        "ON o.id=a.object_id WHERE o.canonical=$1 AND a.name='session' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", "agent:stopanchor01")
    assert after == full_sid[:8]

    chain = await succession_chain(actions.pool, "agent:stopanchor01")
    assert chain[0]["session"] == full_sid[:8]  # _lineage_resume_candidate's own hop-0 read

    ledger = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o "
        "ON o.id=a.object_id WHERE o.canonical=$1 AND a.name=$2",
        "agent:stopanchor01", f"anchor_sid:{full_sid[:8]}")
    assert ledger == full_sid  # the sibling ledger, for the SPAWNED resumed session's own id


async def test_stop_seat_releases_both_the_anchor_and_session_derived_mount_rows(
    actions: Actions,
) -> None:
    """THE FIX (a live finding, and its own
    live-fire correction). A killed process's own agent_mounts row used to survive the
    kill untouched, reading as LIVE to _launch_twin_check's is_live() for a full
    LIVENESS_WINDOW_MINUTES: an immediate resume()/launch() right after a genuine stop
    saw a body that was already gone.

    THE REAL SHAPE, confirmed by querying a live `--bg`-launched seat's own agent_mounts
    rows directly (a second acceptance run) rather than assumed: TWO
    rows exist for the same live session, not one: the boot prompt's own explicit
    `mount(job_dir=_launch_anchor(seat_id))` call (the STABLE per-seat anchor) AND a
    SECOND row the harness's own SessionStart/automount path writes at the session-id-
    derived job_dir (`handshake._derive_job_dir`, `sid[:8]`). A first fix attempt here
    released only the session-derived row (the one census's own exact-agent_id match
    actually needs to find the PID): stop still reported success, but a live re-run
    against it STILL saw 'already-live' on resume, because releasing the newer row simply
    unmasked the older, still-stale anchor row as `_launch_twin_check`'s own
    `ORDER BY last_seen DESC LIMIT 1` new "most recent" live signal for that cwd. Both
    rows must be released; this test proves both are, not just the one matching found."""
    from src.orchestrator.handshake import _derive_job_dir
    from src.orchestrator.mounts import is_live

    session_id = "e1e1e1e1-0000-4000-8000-000000000000"
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:stoprel01", manager_agent="agent:stoprelm01",
        worker_handle="Stop-Release-Test", house="osiris")
    anchor_job_dir = trigger_module._launch_anchor(worker_seat)
    session_job_dir = _derive_job_dir(session_id)
    assert session_job_dir is not None
    # THE STABLE ANCHOR ROW: written first, by the boot prompt's own explicit mount().
    await save_mount(actions.pool, job_dir=anchor_job_dir, agent_id="agent:stoprel01",
                            project="osiris", cwd="/repo/demo", model=None, session_key=None)
    # THE SESSION-DERIVED ROW: written second (later last_seen), by the harness's own
    # SessionStart path; census's exact-agent_id match resolves the live PID through
    # THIS row (its job_dir basename equals sid[:8], the shape registry_census matches).
    await save_mount(actions.pool, job_dir=session_job_dir, agent_id="agent:stoprel01",
                            project="osiris", cwd="/repo/demo", model=None, session_key=None)

    async def _kill(pid: int, job_dir_key: str | None) -> None:
        pass

    d = await trigger_module.stop_seat(
        actions, caller="agent:stoprelm01", target=worker_seat,
        agents_json=_fake_census_agents_json(5252, session_id=session_id),
        read_exe=_fake_claude_exe, read_cwd=lambda pid: "/repo/demo", kill=_kill)

    assert d["status"] == "stopped"
    assert d.get("released_mounts", 0) >= 2
    for jd in (anchor_job_dir, session_job_dir):
        row = await actions.pool.fetchrow(
            "SELECT last_seen FROM agent_mounts WHERE job_dir=$1", jd)
        assert row is not None
        assert not is_live(row["last_seen"]), f"{jd} still reads live after stop"


async def test_stop_seat_also_releases_a_fork_sessions_own_mount_row(
    actions: Actions,
) -> None:
    """THE UNDER-REPORT: a `--fork-session` child
    (`_fork_child`) mounts its OWN row, under its OWN agent_id,
    at its OWN job_dir: a genuinely separate live process. Neither release pass reached
    it: it is `spawned_by`-linked to its parent generation, never a `succeeded_from`
    member of `succession_chain`, so it matched no anchor and no chain-derived job_dir.
    A live fork process under a stopped seat stayed falsely 'live' indefinitely. This
    proves it is now suspended too, and counted in the same `released_mounts` receipt."""
    from src.orchestrator.handshake import _derive_job_dir
    from src.orchestrator.lineage import register_spawn
    from src.orchestrator.mounts import is_live

    session_id = "f0f0f0f0-0000-4000-8000-000000000000"
    fork_session_id = "fc1fc1fc-0000-4000-8000-000000000000"
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:stopfork01", manager_agent="agent:stopforkm01",
        worker_handle="Stop-Fork-Test", house="osiris")
    session_job_dir = _derive_job_dir(session_id)
    assert session_job_dir is not None
    await save_mount(actions.pool, job_dir=session_job_dir, agent_id="agent:stopfork01",
                            project="osiris", cwd="/repo/demo", model=None, session_key=None)

    # THE FORK CHILD: a whole second live process, spawned_by the seat's own holder,
    # never a succession_chain member, mounted at its own job_dir.
    fork_child = await register_spawn(
        actions, fork_session_id[:8], agent_type="fork",
        parent_agent="agent:stopfork01", project="osiris",
        session=fork_session_id, witnessed=True)
    assert fork_child is not None
    fork_job_dir = _derive_job_dir(fork_session_id)
    assert fork_job_dir is not None
    await save_mount(actions.pool, job_dir=fork_job_dir, agent_id=fork_child,
                            project="osiris", cwd="/repo/demo", model=None, session_key=None)

    async def _kill(pid: int, job_dir_key: str | None) -> None:
        pass

    d = await trigger_module.stop_seat(
        actions, caller="agent:stopforkm01", target=worker_seat,
        agents_json=_fake_census_agents_json(5353, session_id=session_id),
        read_exe=_fake_claude_exe, read_cwd=lambda pid: "/repo/demo", kill=_kill)

    assert d["status"] == "stopped"
    assert d.get("released_mounts", 0) >= 2  # the worker's own row AND the fork's
    fork_row = await actions.pool.fetchrow(
        "SELECT last_seen FROM agent_mounts WHERE job_dir=$1", fork_job_dir)
    assert fork_row is not None
    assert not is_live(fork_row["last_seen"]), "the fork's own row still reads live after stop"


async def test_stop_seat_reports_no_live_body_when_registry_census_finds_nothing(
    actions: Actions,
) -> None:
    """A stale mount row is not a live body: the SAME harness+/proc-confirmed occupancy
    authority every other door in the fold/reanimation/send()/launch lane already
    consults, never a second notion of 'live'."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:stop02", manager_agent="agent:stopm02",
        worker_handle="Stop-Test-2", house="osiris")
    await save_mount(actions.pool, job_dir="/x/jobs/nowhere", agent_id="agent:stop02",
                            project="osiris", cwd="/repo/demo", model=None, session_key=None)

    async def _boom(pid: int, job_dir_key: str | None) -> None:
        raise AssertionError("nothing confirmed live, no signal should ever be sent")

    async def _empty_agents_json(**kw: Any) -> list[dict[str, Any]]:
        return []

    d = await trigger_module.stop_seat(
        actions, caller="agent:stopm02", target=worker_seat,
        agents_json=_empty_agents_json, kill=_boom)

    assert d["status"] == "no-live-body"


async def test_stop_seat_is_downward_only_a_worker_cannot_stop_its_manager(
    actions: Actions,
) -> None:
    """Mirrors launch_seat's own authority exactly: the worker→manager
    managed_by edge does not run the other way; nothing is ever signaled."""
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:stop03w", manager_agent="agent:stop03m")

    async def _boom(pid: int, job_dir_key: str | None) -> None:
        raise AssertionError("downward-only: a worker stopping its manager must refuse first")

    d = await trigger_module.stop_seat(
        actions, caller="agent:stop03w", target=manager_seat, kill=_boom)

    assert d["status"] == "refused-not-your-worker" and "DOWNWARD-ONLY" in d["detail"]


async def test_stop_seat_self_target_needs_no_managed_by_edge(actions: Actions) -> None:
    """`target=None` always stops the CALLER's own seat: pause_seat's own precedent, the
    one case authority never has to arbitrate (no peer/edge check at all)."""
    seat = (await ensure_seat(actions, house="demo", handle="SelfStopper",
                              source="test"))["seat_id"]
    await bind_holder(actions, seat_id=seat, agent_id="agent:self01")
    await save_mount(actions.pool, job_dir="/x/jobs/self9999", agent_id="agent:self01",
                            project="demo", cwd="/repo/demo", model=None, session_key=None)
    killed: list[int] = []

    async def _kill(pid: int, job_dir_key: str | None) -> None:
        killed.append(pid)

    d = await trigger_module.stop_seat(
        actions, caller="agent:self01", target=None,
        agents_json=_fake_census_agents_json(
            777, session_id="self9999-0000-4000-8000-000000000000"),
        read_exe=_fake_claude_exe, read_cwd=lambda pid: "/repo/demo", kill=_kill)

    assert d["status"] == "stopped" and killed == [777]


async def test_stop_seat_reports_process_lookup_error_honestly(actions: Actions) -> None:
    """A race (the body exits between the census read and the signal) is a real,
    honestly-named outcome: never mistaken for a signal failure."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:stop04", manager_agent="agent:stopm04",
        worker_handle="Stop-Test-4", house="osiris")
    await save_mount(actions.pool, job_dir="/x/jobs/wg8888ii", agent_id="agent:stop04",
                            project="osiris", cwd="/repo/demo", model=None, session_key=None)

    async def _kill(pid: int, job_dir_key: str | None) -> None:
        raise ProcessLookupError()

    d = await trigger_module.stop_seat(
        actions, caller="agent:stopm04", target=worker_seat,
        agents_json=_fake_census_agents_json(
            5555, session_id="wg8888ii-0000-4000-8000-000000000000"),
        read_exe=_fake_claude_exe, read_cwd=lambda pid: "/repo/demo", kill=_kill)

    assert d["status"] == "no-live-body"
    assert "already gone" in d["detail"]


async def test_stop_seat_passes_the_census_job_dir_key_to_kill(actions: Actions) -> None:
    """The wiring proof: stop_seat's own census match already carries `job_dir_key`
    (identical to `claude agents --json`'s own `id`): this must reach `kill` unchanged,
    with no separate lookup, so `_real_kill_pid` can prefer `claude stop <id>`."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:stop05", manager_agent="agent:stopm05",
        worker_handle="Stop-Test-5", house="osiris")
    await save_mount(actions.pool, job_dir="/x/jobs/wgjobkey", agent_id="agent:stop05",
                            project="osiris", cwd="/repo/demo", model=None, session_key=None)
    seen: list[tuple[int, str | None]] = []

    async def _kill(pid: int, job_dir_key: str | None) -> None:
        seen.append((pid, job_dir_key))

    d = await trigger_module.stop_seat(
        actions, caller="agent:stopm05", target=worker_seat,
        agents_json=_fake_census_agents_json(
            6161, session_id="wgjobkey-0000-4000-8000-000000000000"),
        read_exe=_fake_claude_exe, read_cwd=lambda pid: "/repo/demo", kill=_kill)

    assert d["status"] == "stopped"
    assert seen == [(6161, "wgjobkey")]


# --- THE ORPHAN PATH: a body
# the graph never bound a holder for, still stoppable via the harness's own roster. -------

async def test_stop_seat_kills_an_orphan_harness_body_the_graph_never_bound(
    actions: Actions,
) -> None:
    """One seat's own graph holder was never bound at all, but a real body still ran
    under its own window name at its own office cwd: stop_seat must be able to reach
    it, never refuse outright just because there's no `holds` edge to walk."""
    from src.orchestrator.trigger import _window_name

    seat = await ensure_seat(actions, house="monsterhouse", handle="Orphan",
                             source="agent:orphm01")
    orphan_seat = seat["seat_id"]
    office = "/tmp/orphan-office"
    await _office(actions, orphan_seat, office)
    manager = await ensure_seat(actions, house="monsterhouse", handle="OrphanMgr",
                                source="agent:orphm01")
    manager_seat = manager["seat_id"]
    await bind_holder(actions, seat_id=manager_seat, agent_id="agent:orphm01",
                      source="agent:orphm01")
    w_oid = await actions.create_or_find_object("Seat", orphan_seat, "test")
    m_oid = await actions.create_or_find_object("Seat", manager_seat, "test")
    await actions.create_link(w_oid, m_oid, "managed_by", "test", NOW, 0.9)

    expected_name = await _window_name(actions.pool, "monsterhouse", "Orphan", None)
    real_pid = os.getpid()  # a genuinely alive pid, /proc's own real state, never faked
    row = {"name": expected_name, "cwd": office, "pid": real_pid, "id": "abc12345",
          "state": "running"}

    async def _agents_json(**kw: Any) -> list[dict[str, Any]]:
        return [row]

    def _real_body_cmdline(pid: int) -> bytes:
        return b"claude\x00--bg\x00-n\x00" + expected_name.encode()

    killed: list[tuple[int, str | None]] = []

    async def _kill(pid: int, job_dir_key: str | None) -> None:
        killed.append((pid, job_dir_key))

    d = await trigger_module.stop_seat(
        actions, caller="agent:orphm01", target=orphan_seat,
        agents_json=_agents_json, kill=_kill, read_cmdline=_real_body_cmdline)

    assert d["status"] == "stopped"
    assert d["orphan"] is True
    assert d["pid"] == real_pid
    assert killed == [(real_pid, "abc12345")]


async def test_stop_seat_refuses_an_orphan_row_whose_cwd_does_not_match_the_seat(
    actions: Actions,
) -> None:
    """A NAME COLLISION IS NOT ENOUGH EVIDENCE TO KILL SOMEONE ELSE'S PROCESS: two seats
    can share a handle across different houses/projects. A harness row that matches by
    NAME but sits at a DIFFERENT cwd than this seat's own tree_cwd/office must never be
    killed: the exact 'pattern match, not identity' hazard this test explicitly guards against."""
    from src.orchestrator.trigger import _window_name

    seat = await ensure_seat(actions, house="monsterhouse", handle="Orphan2",
                             source="agent:orphm02")
    orphan_seat = seat["seat_id"]
    await _office(actions, orphan_seat, "/tmp/orphan2-office")
    manager = await ensure_seat(actions, house="monsterhouse", handle="Orphan2Mgr",
                                source="agent:orphm02")
    manager_seat = manager["seat_id"]
    await bind_holder(actions, seat_id=manager_seat, agent_id="agent:orphm02",
                      source="agent:orphm02")
    w_oid = await actions.create_or_find_object("Seat", orphan_seat, "test")
    m_oid = await actions.create_or_find_object("Seat", manager_seat, "test")
    await actions.create_link(w_oid, m_oid, "managed_by", "test", NOW, 0.9)

    expected_name = await _window_name(actions.pool, "monsterhouse", "Orphan2", None)
    # SAME name, WRONG cwd: a different seat's own body, wearing a colliding label.
    row = {"name": expected_name, "cwd": "/somewhere/unrelated", "pid": 4242,
          "id": "def67890", "state": "running"}

    async def _agents_json(**kw: Any) -> list[dict[str, Any]]:
        return [row]

    async def _boom(pid: int, job_dir_key: str | None) -> None:
        raise AssertionError("a cwd mismatch must never reach kill()")

    d = await trigger_module.stop_seat(
        actions, caller="agent:orphm02", target=orphan_seat,
        agents_json=_agents_json, kill=_boom)

    assert d["status"] == "no-live-body"


async def test_stop_seat_never_kills_a_bg_spare_wearing_the_seats_own_window_name(
    actions: Actions,
) -> None:
    """An earlier addendum: a `claude bg-spare` warm-spare that
    FAILED to claim can still be labelled with a seat's own window name (one earlier
    incident): matching by name and cwd is not enough on its own; a spare is never a
    body, and the orphan path must refuse it exactly as `_harness_row_is_live` now
    does everywhere else."""
    from src.orchestrator.trigger import _window_name

    seat = await ensure_seat(actions, house="monsterhouse", handle="Orphan3",
                             source="agent:orphm03")
    orphan_seat = seat["seat_id"]
    office = "/tmp/orphan3-office"
    await _office(actions, orphan_seat, office)
    manager = await ensure_seat(actions, house="monsterhouse", handle="Orphan3Mgr",
                                source="agent:orphm03")
    manager_seat = manager["seat_id"]
    await bind_holder(actions, seat_id=manager_seat, agent_id="agent:orphm03",
                      source="agent:orphm03")
    w_oid = await actions.create_or_find_object("Seat", orphan_seat, "test")
    m_oid = await actions.create_or_find_object("Seat", manager_seat, "test")
    await actions.create_link(w_oid, m_oid, "managed_by", "test", NOW, 0.9)

    expected_name = await _window_name(actions.pool, "monsterhouse", "Orphan3", None)
    real_pid = os.getpid()  # genuinely alive, the spare-ness comes from cmdline alone
    row = {"name": expected_name, "cwd": office, "pid": real_pid, "id": "spare123",
          "state": "running"}

    async def _agents_json(**kw: Any) -> list[dict[str, Any]]:
        return [row]

    def _spare_cmdline(pid: int) -> bytes:
        return b"claude\x00bg-spare\x00--bg-spare\x00/tmp/spare.sock"

    async def _boom(pid: int, job_dir_key: str | None) -> None:
        raise AssertionError("a bg-spare must never reach kill()")

    d = await trigger_module.stop_seat(
        actions, caller="agent:orphm03", target=orphan_seat,
        agents_json=_agents_json, kill=_boom, read_cmdline=_spare_cmdline)

    assert d["status"] == "no-live-body"


# --- _real_kill_pid: prefer the harness's own
# `claude stop <id>`: a raw SIGTERM to a --bg-substrate body's inner process gets
# silently respawned by the harness's own background-agent daemon, which reads an
# unexpected exit as a crash to heal rather than a stop request. -------------------------

async def test_real_kill_pid_prefers_claude_stop_when_a_harness_id_is_known(
    monkeypatch: Any,
) -> None:
    calls: list[list[str]] = []

    class _FakeProc:
        async def wait(self) -> int:
            return 0

    async def _fake_exec(*argv: str, **kw: Any) -> _FakeProc:
        calls.append(list(argv))
        return _FakeProc()

    killed: list[int] = []

    def _fake_os_kill(pid: int, sig: int) -> None:
        killed.append(pid)

    monkeypatch.setattr("asyncio.create_subprocess_exec", _fake_exec)
    monkeypatch.setattr("os.kill", _fake_os_kill)

    await trigger_module._real_kill_pid(4242, "wgjobkey5")

    # stop, THEN rm the stopped record (2026-09-03: a record left on file makes the next
    # `--bg --resume` start a copy instead of continuing the session)
    assert calls == [["claude", "stop", "wgjobkey5"], ["claude", "rm", "wgjobkey5"]]
    assert killed == []  # claude stop succeeded, SIGTERM must never also fire


async def test_real_kill_pid_falls_back_to_sigterm_when_claude_stop_fails(
    monkeypatch: Any,
) -> None:
    class _FakeProc:
        async def wait(self) -> int:
            return 1  # unknown id, or a dark daemon, claude stop itself refused

    async def _fake_exec(*argv: str, **kw: Any) -> _FakeProc:
        return _FakeProc()

    killed: list[tuple[int, int]] = []

    def _fake_os_kill(pid: int, sig: int) -> None:
        killed.append((pid, sig))

    monkeypatch.setattr("asyncio.create_subprocess_exec", _fake_exec)
    monkeypatch.setattr("os.kill", _fake_os_kill)

    await trigger_module._real_kill_pid(4242, "some-unknown-id")

    import signal
    assert killed == [(4242, signal.SIGTERM)]


async def test_real_kill_pid_falls_back_to_sigterm_when_the_binary_is_missing(
    monkeypatch: Any,
) -> None:
    """The same graceful-degrade fix as _clear_stale_stopped_record's own
    OSError guard: a harness that genuinely isn't installed is exactly the "claude stop
    itself refused" case this fallback already exists for: not a crash."""
    async def _fake_exec(*argv: str, **kw: Any) -> Any:
        raise FileNotFoundError(2, "No such file or directory", "claude")

    killed: list[tuple[int, int]] = []

    def _fake_os_kill(pid: int, sig: int) -> None:
        killed.append((pid, sig))

    monkeypatch.setattr("asyncio.create_subprocess_exec", _fake_exec)
    monkeypatch.setattr("os.kill", _fake_os_kill)

    await trigger_module._real_kill_pid(4242, "wgjobkey5")

    import signal
    assert killed == [(4242, signal.SIGTERM)]


async def test_real_kill_pid_uses_sigterm_directly_with_no_harness_id(
    monkeypatch: Any,
) -> None:
    """The PTY-broker fallback lane: no harness-tracked id exists at all, so no
    subprocess is even attempted: straight to the ordinary graceful SIGTERM."""
    exec_calls: list[Any] = []

    async def _fake_exec(*argv: str, **kw: Any) -> Any:
        exec_calls.append(argv)
        raise AssertionError("no harness id, claude stop must never be attempted")

    killed: list[tuple[int, int]] = []

    def _fake_os_kill(pid: int, sig: int) -> None:
        killed.append((pid, sig))

    monkeypatch.setattr("asyncio.create_subprocess_exec", _fake_exec)
    monkeypatch.setattr("os.kill", _fake_os_kill)

    await trigger_module._real_kill_pid(4242, None)

    import signal
    assert exec_calls == []
    assert killed == [(4242, signal.SIGTERM)]


# --- _clear_stale_stopped_record: the copy-quirk's own
# pre-emption: the SAME `claude rm <id>` removal _real_kill_pid does after a stop, now
# factored out so resume can run it BEFORE spawning too. ---------------------------------

async def test_clear_stale_stopped_record_runs_claude_rm_and_reports_success(
    monkeypatch: Any,
) -> None:
    calls: list[list[str]] = []

    class _FakeProc:
        async def wait(self) -> int:
            return 0

    async def _fake_exec(*argv: str, **kw: Any) -> _FakeProc:
        calls.append(list(argv))
        return _FakeProc()

    monkeypatch.setattr("asyncio.create_subprocess_exec", _fake_exec)

    cleared = await trigger_module._clear_stale_stopped_record("wgjobkey5")

    assert calls == [["claude", "rm", "wgjobkey5"]]
    assert cleared is True


async def test_clear_stale_stopped_record_reports_false_on_nothing_to_clear(
    monkeypatch: Any,
) -> None:
    class _FakeProc:
        async def wait(self) -> int:
            return 1  # no record existed for this id, rm found nothing

    async def _fake_exec(*argv: str, **kw: Any) -> _FakeProc:
        return _FakeProc()

    monkeypatch.setattr("asyncio.create_subprocess_exec", _fake_exec)

    cleared = await trigger_module._clear_stale_stopped_record("no-such-id")

    assert cleared is False


async def test_clear_stale_stopped_record_tolerates_a_missing_binary(
    monkeypatch: Any,
) -> None:
    """Found live by the stranger_test proof: retire_agent's own "best-
    effort, never blocks" claim was false on a box with no `claude` installed at all:
    create_subprocess_exec's own FileNotFoundError crashed every caller uncaught. Now
    degrades to the same False every other no-op path already returns."""
    async def _fake_exec(*argv: str, **kw: Any) -> Any:
        raise FileNotFoundError(2, "No such file or directory", "claude")

    monkeypatch.setattr("asyncio.create_subprocess_exec", _fake_exec)

    cleared = await trigger_module._clear_stale_stopped_record("some-id")

    assert cleared is False  # never raises


async def test_resume_seat_clears_a_stale_stopped_record_before_spawning(
    actions: Actions, tmp_path: Path,
) -> None:
    """The copy-quirk's own pre-emption: resume_seat now
    clears any stale harness "stopped" record BEFORE calling resume_spawn, instead of
    only detecting-and-adopting a copy after the harness has already minted one. Order
    matters, asserted directly, not inferred from call counts."""
    sense = await _lineage_holder_with_session(
        actions, tmp_path, agent_id="agent:clearme01")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:clearme01", manager_agent="agent:hm-clear",
        worker_handle="Clear-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/clear-test-office")
    manc = await actions.create_or_find_object("Agent", "agent:hm-clear", "test")
    await actions.assert_property(manc, "project", "osiris", "test", NOW, 0.9,
                                  evidence_class="self_declared")

    order: list[str] = []
    cleared_keys: list[str] = []

    async def _clear_stale_record(job_dir_key: str) -> bool:
        order.append("clear")
        cleared_keys.append(job_dir_key)
        return True

    async def _resume_spawn(repo: str, prompt: str, **kw: Any) -> None:
        order.append("spawn")

    d = await trigger_module.resume_seat(
        actions, caller="agent:hm-clear", target=worker_seat,
        settings=_settings(enabled=True, sense=str(sense)),
        resume_spawn=_resume_spawn, agents_json=_fake_agents_json([[]]),
        clear_stale_record=_clear_stale_record)

    assert order == ["clear", "spawn"]  # clear runs BEFORE the spawn, never after
    assert cleared_keys == [FULL_SID[:8]]
    assert any("cleared a stale stopped record" in line for line in d["resume_check"])


async def test_resume_seat_stays_silent_when_no_stale_record_existed(
    actions: Actions, tmp_path: Path,
) -> None:
    """A no-op clear (nothing was there) must never be reported as if it did something:
    the resume_check log stays exactly as it was before this fix for the common case."""
    sense = await _lineage_holder_with_session(
        actions, tmp_path, agent_id="agent:clearme02")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:clearme02", manager_agent="agent:hm-clear2",
        worker_handle="Clear-Test2", house="osiris")
    await _office(actions, worker_seat, "/tmp/clear-test-office-2")
    manc = await actions.create_or_find_object("Agent", "agent:hm-clear2", "test")
    await actions.assert_property(manc, "project", "osiris", "test", NOW, 0.9,
                                  evidence_class="self_declared")

    async def _clear_stale_record(job_dir_key: str) -> bool:
        return False

    async def _resume_spawn(repo: str, prompt: str, **kw: Any) -> None:
        pass

    d = await trigger_module.resume_seat(
        actions, caller="agent:hm-clear2", target=worker_seat,
        settings=_settings(enabled=True, sense=str(sense)),
        resume_spawn=_resume_spawn, agents_json=_fake_agents_json([[]]),
        clear_stale_record=_clear_stale_record)

    assert not any("cleared a stale stopped record" in line for line in d["resume_check"])


# ═══ THE COLLAPSE ITSELF (2026-08-28) ══════════════════════════════════════════════════
# The occupancy gate answered two structurally different findings with one word, and the
# mode it emitted was never added to _WAKE_STATUS at all: so it rode an unnamed default
# written for rate-brakes and pauses. Both halves are pinned here because both are how a
# reader ends up believing a reachable seat is unreachable (a related incident, hit
# again by a coordinator on 2026-08-28 before this fix).

def test_occupancy_gate_arms_are_distinguishable_and_both_named_in_wake_status() -> None:
    from src.orchestrator.trigger import _WAKE_STATUS

    # NEITHER ARM MAY RIDE A DEFAULT. Every other resume-refused-* is named explicitly;
    # the occupancy pair was the one sibling missing from the family.
    assert "queued-live-holder" in _WAKE_STATUS
    assert "resume-refused-occupied-foreign" in _WAKE_STATUS
    # the pre-split mode stays mapped so stored receipts never fall through either.
    assert "resume-refused-occupied" in _WAKE_STATUS

    # AND THEY MUST NOT AGREE. The addressee's own live session is a delivery outcome;
    # an unidentified body in its office is a refusal with an unknown reader. Collapsing
    # them is the defect, one rung further down the sequence.
    assert _WAKE_STATUS["queued-live-holder"] != (
        _WAKE_STATUS["resume-refused-occupied-foreign"])
    assert _WAKE_STATUS["queued-live-holder"] == "queued"


async def test_resident_verdict_ranks_the_minds_own_act_above_a_later_whisper_greeting(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE WHISPER IS HEARSAY (2026-09-03): a transcript whose every ACT (mount/send
    receipts) is the addressee's own, but whose tail carries SessionStart greetings naming
    another lineage (an anchor-leaked hand resume) with no act by that
    lineage after them, is "unknown": never a found different mind. Negative control:
    an act by the other lineage AFTER the greeting is a real "mismatch"; a greeting naming
    the addressee itself stays "match"."""
    from src.orchestrator.trigger import _resident_verdict

    root = tmp_path / "projects"
    slug = root / "-home-x--osiris-seats-chad"
    slug.mkdir(parents=True)
    sid = "7451509a-f711-48ea-b67e-d2877d721ca3"
    t = slug / f"{sid}.jsonl"
    mount_by_chad = ('{"type":"user","text":"{\\"agent\\":\\"agent:7451509a\\",'
                     '\\"project\\":\\"cdking\\"}"}\n')
    greeting_khnum = '{"type":"attachment","text":"osiris knows you as agent:aad6603a-g40-vii"}\n'
    send_by_khnum = ('{"type":"user","text":"{\\"sent\\":9,'
                     '\\"from\\":\\"agent:aad6603a-g40-vii\\"}"}\n')
    greeting_chad = '{"type":"attachment","text":"osiris knows you as agent:7451509a"}\n'

    t.write_text(mount_by_chad + greeting_khnum + greeting_khnum)
    assert await _resident_verdict(actions.pool, root, sid, "agent:7451509a") == "unknown"

    t.write_text(mount_by_chad + greeting_khnum + send_by_khnum)
    assert await _resident_verdict(actions.pool, root, sid, "agent:7451509a") == "mismatch"

    t.write_text(mount_by_chad + greeting_chad)
    assert await _resident_verdict(actions.pool, root, sid, "agent:7451509a") == "match"

    t.write_text(greeting_khnum)                     # nothing but a stranger's greeting
    assert await _resident_verdict(actions.pool, root, sid, "agent:7451509a") == "mismatch"


async def test_adopt_resumed_body_adopts_a_harness_copy_and_leaves_a_true_resume_alone(
    actions: Actions,
) -> None:
    """WHAT THE HARNESS ACTUALLY STARTED (2026-09-03, harness 2.1.259): with a stopped
    background record still on file, `--bg --resume <id>` starts a COPY under a new id.
    The copy is adopted as the seat's own continuation: ledgered under the holder,
    registry row rebound, never left as a stranger; a body that came back under the
    requested id is left alone; no body at all is confessed, never assumed."""
    from src.orchestrator.trigger import _adopt_resumed_body

    office = "/home/x/.osiris/seats/adopt"
    requested = "7451509a-f711-48ea-b67e-d2877d721ca3"
    copy = "5f54e1fc-be7b-40c7-b941-ee3881b44775"
    await actions.create_or_find_object("Agent", "agent:ad0p7ee1", "test")

    async def _copy(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return [{"id": copy[:8], "sessionId": copy, "cwd": office, "name": "[AD] Adopt"}]

    out = await _adopt_resumed_body(actions.pool, agents_json=_copy, office=office,
                                    requested_sid=requested, holder="agent:ad0p7ee1",
                                    project="adopthouse", attempts=1, delay=0)
    assert out["copied"] is True and out["adopted"] is True and out["session_id"] == copy
    ledgered = await actions.pool.fetchval(
        "SELECT o.canonical FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE a.name = 'anchor_sid:' || $1", copy[:8])
    assert ledgered == "agent:ad0p7ee1"
    bound = await actions.pool.fetchrow(
        "SELECT agent_id, cwd FROM agent_mounts WHERE job_dir LIKE '%/jobs/' || $1", copy[:8])
    assert bound is not None and bound["agent_id"] == "agent:ad0p7ee1" and bound["cwd"] == office

    async def _same(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return [{"id": requested[:8], "sessionId": requested, "cwd": office}]

    out = await _adopt_resumed_body(actions.pool, agents_json=_same, office=office,
                                    requested_sid=requested, holder="agent:ad0p7ee1",
                                    project="adopthouse", attempts=1, delay=0)
    assert out == {"session_id": requested, "copied": False, "adopted": False}

    async def _none(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return []

    out = await _adopt_resumed_body(actions.pool, agents_json=_none, office=office,
                                    requested_sid=requested, holder="agent:ad0p7ee1",
                                    project="adopthouse", attempts=2, delay=0)
    assert out == {"session_id": None, "copied": False, "adopted": False}


async def test_stop_seat_falls_back_to_the_lineages_own_graph_sessions(
    actions: Actions,
) -> None:
    """THE LINEAGE FALLBACK: the seat's holder just succeeded
    (agent:stop06 -> agent:stop06-ii), but the live body's own agent_mounts row hasn't
    caught up yet (job_dir still filed under the OLD generation, a real live shape)
    : the exact-agent_id match against `matched` misses it, so stop_seat must fall back to
    the lineage's own succession_chain sessions against the /proc-verified census."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:stop06", manager_agent="agent:stopm06",
        worker_handle="Stop-Lineage-Test", house="osiris")
    # the succession: a new generation now holds the seat...
    await bind_holder(actions, seat_id=worker_seat, agent_id="agent:stop06-ii")
    a = await actions.create_or_find_object("Agent", "agent:stop06-ii", "test")
    await actions.assert_property(a, "succeeded_from", "agent:stop06", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(a, "session", "ii906060", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    # ...but the live body's own registry row is still filed under the OLD generation
    # (agent_mounts hasn't been re-written since the succession landed).
    await save_mount(actions.pool, job_dir="/x/jobs/ii906060", agent_id="agent:stop06",
                            project="osiris", cwd="/repo/lineage-demo", model=None,
                            session_key=None)
    killed: list[int] = []

    async def _kill(pid: int, job_dir_key: str | None) -> None:
        killed.append(pid)

    d = await trigger_module.stop_seat(
        actions, caller="agent:stopm06", target=worker_seat,
        agents_json=_fake_census_agents_json(
            5252, cwd="/repo/lineage-demo",
            session_id="ii906060-0000-4000-8000-000000000000"),
        read_exe=_fake_claude_exe, read_cwd=lambda pid: "/repo/lineage-demo", kill=_kill)

    assert d["status"] == "stopped"
    assert d["pid"] == 5252 and d["holder"] == "agent:stop06-ii"
    assert killed == [5252]


def test_job_id_anchors_on_the_innermost_jobs_component():
    """A job dir nested under another job's tree names the INNER job."""
    from src.ingest.sessions import _job_id

    assert _job_id("/home/u/.claude/jobs/outer111/tmp/x/jobs/inner222") == "inner222"
    assert _job_id("/home/u/.claude/jobs/outer111") == "outer111"
    assert _job_id(None) is None


async def test_bind_before_spawn_trusts_tenure_over_the_handle_assertions_author(
    actions: Actions,
) -> None:
    """THE WERNER/TILL SPECIMEN (2026-09-05): both seats were FOUNDED
    by a founder in July, so their `handle` assertions are sourced from that founder's lineage,
    while their own lineages held them across eleven generations
    each. Post-reboot launches minted both bodies as that founder's own generations 37
    and 38: an earlier "confess, never change" ruling let the founder's lineage win. A
    lineage that has held the seat across 2+ generations IS the seat's own; one stale
    edge cannot fake tenure, but a founder's authorship of the handle proves nothing."""
    seat_id = (await ensure_seat(actions, house="alfred", handle="Werner",
                                 source="agent:thothfounder-xiii"))["seat_id"]
    from src.orchestrator.agents import mint_heir
    root_oid = await actions.create_or_find_object("Agent", "agent:wernerline", "test")
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:wernerline")
    heir, _ = await mint_heir(actions, "agent:wernerline", root_oid, because="test",
                              succession=None)
    await bind_holder(actions, seat_id=seat_id, agent_id=heir)

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Werner", house="alfred",
        current_holder=heir, office="/tmp/werner", anchor="/tmp/anchors/werner",
        source="agent:alfred01")

    assert out["agent"] == "agent:wernerline-iii"  # werner's own lineage, never Thoth's
    assert "thothfounder" not in out["agent"]
    row = await actions.pool.fetchrow(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_id)
    # the ANCESTOR (heir, wernerline-ii), never the fresh bookkeeping heir (wernerline-iii),
    # the holds-sandwich fix
    assert row["canonical"] == heir


async def test_bind_before_spawn_a_single_stale_edge_is_not_tenure(actions: Actions) -> None:
    """The tenure guard survives: ONE holds edge from an unrelated agent is not tenure, so
    the handle-source lineage still wins there (the test above this file already pins)."""
    seat_id = (await ensure_seat(actions, house="dealer-to-fb", handle="Marquee2",
                                 source="agent:realmind2"))["seat_id"]
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:staleholder2")
    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Marquee2", house="dealer-to-fb",
        current_holder="agent:staleholder2", office="/tmp/m2", anchor="/tmp/anchors/m2",
        source="agent:thoth01")
    assert out["agent"] == "agent:realmind2-ii"


async def test_bind_before_spawn_a_single_holder_with_a_mount_in_the_office_is_tenure(
    actions: Actions,
) -> None:
    """An earlier fix: "prefer resume" alone still left
    several real holders exposed to a founder-side launch. A single real
    holder now earns tenure too, IF it left occupancy evidence behind: here, a durable
    `agent_mounts` row whose cwd is the seat's own office (never live at read time, the
    "durable" half of "live or durable")."""
    seat_id = (await ensure_seat(actions, house="alfred", handle="Cassandra2",
                                 anchor_cwd="/home/x/.osiris/seats/cassandra2",
                                 source="agent:thothfounder-xiii"))["seat_id"]
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:cassline")
    await actions.pool.execute(
        "INSERT INTO agent_mounts (job_dir, agent_id, project, cwd, last_seen) "
        "VALUES ($1, $2, $3, $4, now() - interval '10 days')",
        "/home/x/.claude/jobs/cassline-job", "agent:cassline", "osiris",
        "/home/x/.osiris/seats/cassandra2")

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Cassandra2", house="alfred",
        current_holder="agent:cassline", office="/home/x/.osiris/seats/cassandra2",
        anchor="/tmp/anchors/cass2", source="agent:alfred01")

    assert out["agent"] == "agent:cassline-ii"  # cassandra's own lineage, never Thoth's
    assert "thothfounder" not in out["agent"]


async def test_bind_before_spawn_a_single_holder_with_no_occupancy_evidence_is_not_tenure(
    actions: Actions,
) -> None:
    """The tenure guard survives the new leg too: one holds edge with NO mount trace
    behind it (never mounted into the office, never claimed the seat) still falls through
    to the handle-source lineage: lowering the threshold to admit it would readmit the
    exact specimen the 2+-generation design was built to exclude."""
    seat_id = (await ensure_seat(actions, house="dealer-to-fb", handle="Marquee3",
                                 anchor_cwd="/home/x/.osiris/seats/marquee3",
                                 source="agent:realmind3"))["seat_id"]
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:staleholder3")
    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Marquee3", house="dealer-to-fb",
        current_holder="agent:staleholder3", office="/home/x/.osiris/seats/marquee3",
        anchor="/tmp/anchors/m3", source="agent:thoth01")
    assert out["agent"] == "agent:realmind3-ii"


async def test_bind_before_spawn_a_seat_id_stamped_mount_is_also_occupancy_evidence(
    actions: Actions,
) -> None:
    """The second evidence leg: `agent_mounts.seat_id` stamped directly onto this seat
    (claim_name's own binding act): counts even when the mount's `cwd` doesn't match the
    seat's recorded office (a seat resumed from a worktree, say)."""
    seat_id = (await ensure_seat(actions, house="alfred", handle="Jenny2",
                                 anchor_cwd="/home/x/.osiris/seats/jenny2",
                                 source="agent:thothfounder-xiii"))["seat_id"]
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:jennyline")
    await actions.pool.execute(
        "INSERT INTO agent_mounts (job_dir, agent_id, project, cwd, seat_id, last_seen) "
        "VALUES ($1, $2, $3, $4, $5, now() - interval '3 days')",
        "/home/x/.claude/jobs/jenny-job", "agent:jennyline", "osiris",
        "/some/other/worktree", seat_id)

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Jenny2", house="alfred",
        current_holder="agent:jennyline", office="/home/x/.osiris/seats/jenny2",
        anchor="/tmp/anchors/jenny2", source="agent:alfred01")

    assert out["agent"] == "agent:jennyline-ii"
    assert "thothfounder" not in out["agent"]


async def test_resolve_launch_model_is_sticky_to_the_last_holder(actions: Actions) -> None:
    """A finding: a seat with no `intended_model` stamp came back on the
    global haiku default after the reboot although its whole lineage ran Sonnet 5. The
    last holder's `source_model` now sits between the stamp and the default, and the
    receipt names the leg."""
    from datetime import UTC, datetime
    from types import SimpleNamespace

    seat_id = (await ensure_seat(actions, house="alfred", handle="Till2",
                                 source="agent:founder"))["seat_id"]
    oid = await actions.create_or_find_object("Agent", "agent:tillline-xiv", "test")
    await actions.assert_property(oid, "source_model", "claude-sonnet-5", "test",
                                  datetime.now(UTC), 0.6, evidence_class="direct_observation")
    await bind_holder(actions, seat_id=seat_id, agent_id="agent:tillline-xiv")
    st = SimpleNamespace(osiris_wake_model="claude-haiku-4-5-20251001")

    assert await trigger_module._resolve_launch_model(
        actions.pool, seat_id, model=None, facts={}, settings=st,
    ) == ("claude-sonnet-5", "last_holder")
    assert await trigger_module._resolve_launch_model(
        actions.pool, seat_id, model=None, facts={"intended_model": "claude-opus-5"},
        settings=st) == ("claude-opus-5", "intended_model")
    assert await trigger_module._resolve_launch_model(
        actions.pool, seat_id, model="x", facts={}, settings=st) == ("x", "explicit")
    bare = (await ensure_seat(actions, house="alfred", handle="Nobody2",
                              source="console"))["seat_id"]
    assert await trigger_module._resolve_launch_model(
        actions.pool, bare, model=None, facts={}, settings=st,
    ) == ("claude-haiku-4-5-20251001", "wake_default")


async def test_bind_before_spawn_single_holder_named_in_the_graph_is_tenure(
    actions: Actions,
) -> None:
    """A SPECIMEN (2026-09-05 16:52Z, minutes after the cache-only
    occupancy leg deployed): the seat's one real holder had NO agent_mounts row left (the
    cache is swept) but its own graph assertions name the seat: cwd == the office, handle
    == the seat's handle. That is tenure; the founder who wrote the seat's handle is not."""
    from datetime import UTC, datetime

    seat_id = (await ensure_seat(actions, house="monsterhouse", handle="Jenny",
                                 anchor_cwd="/tmp/offices/jenny",
                                 source="agent:thothfounder-xiii"))["seat_id"]
    from src.orchestrator.agents import mint_heir
    root_oid = await actions.create_or_find_object("Agent", "agent:jennyline", "test")
    heir, heir_oid = await mint_heir(actions, "agent:jennyline", root_oid, because="test",
                                     succession=None)
    assert heir == "agent:jennyline-ii"
    now = datetime.now(UTC)
    await actions.assert_property(heir_oid, "handle", "Jenny", heir, now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat_id, agent_id=heir)  # the ONE real hold

    out = await trigger_module._bind_before_spawn(
        actions, target_seat=seat_id, handle="Jenny", house="monsterhouse",
        current_holder="agent:jennyline-ii", office="/tmp/offices/jenny",
        anchor="/tmp/anchors/jenny", source="operator")

    assert out["agent"] == "agent:jennyline-iii"
    assert "thothfounder" not in out["agent"]
