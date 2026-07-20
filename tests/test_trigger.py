"""The fleet trigger-hook — the mailbox's alarm clock, bounded against recursion.

The mailbox is pull-based; this lets the WORKER wake an agent when a project has deliverable
mail. The named danger is the A↔B ping-pong. These tests prove the safety story: OFF by
default, a per-project RATE CAP that halts a loop even under persistent unread mail, no wake
while a live lease says the mail is already being processed, and the operator's desk is never
woken (it has no repo — the human reads it, membrane #6's upward lane).
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import trigger as trigger_module
from src.orchestrator.mailbox import OPERATOR_ADDR, read_inbox, send_message
from src.orchestrator.trigger import (
    _WAKE_PROMPT,
    dispatch_dm,
    should_wake,
    trigger_mail_tick,
    wake_status,
)

NOW = datetime(2026, 7, 6, tzinfo=UTC)


def _settings(*, enabled: bool, rate_cap: int = 5, window: int = 3600,
              lease: int = 900, grace: int = 0, live: int = 900,
              ceiling: int = 8_000_000, sense: str = "",
              wake_model: str = "", attempts: int = 0,
              daily_usd: float = -1.0, projects: str = "",
              poke_only: bool = False, dm_resume: bool = True,
              dm_active: int = 120, seat_cap: int = 0,
              dm_resume_model: str = "") -> SimpleNamespace:
    # grace defaults to 0 (disabled) so the rate-cap / lease tests exercise those bounds in
    # isolation; the wake-grace tests set it explicitly. sense="" → resume resolution looks at
    # ~/.claude/projects (no anchored transcript for the test ids there → mint), so the legacy
    # mint-path tests stay exactly as they were.
    # daily_usd defaults to -1 (NO CEILING) so these tests keep exercising the DISPATCH decisions
    # — rate caps, leases, alternation — in isolation. The ceiling has its own suite
    # (test_ceiling.py) and its own trigger test below; a spend gate silently swallowing every
    # other test's wake would hide the very behaviour they exist to pin.
    # seat_cap defaults to 0 (unbraked) for the same isolation reason; the brake has its own test.
    return SimpleNamespace(osiris_trigger_enabled=enabled, osiris_trigger_rate_cap=rate_cap,
                           osiris_trigger_window_secs=window, osiris_mail_lease_secs=lease,
                           osiris_trigger_grace_secs=grace, osiris_owner_live_secs=live,
                           osiris_resume_ceiling_bytes=ceiling, osiris_sense_sessions=sense,
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
                           osiris_dm_resume_model=dm_resume_model)


async def _no_windows() -> list[dict[str, Any]]:
    """The poke lane's OFF position for tests that exercise the pre-poke ladder — a dark
    manager (the production default until windows exist) is an empty roster."""
    return []


@pytest.fixture(autouse=True)
def _dark_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    """HERMETIC by default: no test in this module may consult a live daemon socket on the
    dev box — the roster default resolves late, so darkening the module attribute covers
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
    """Wake economics (obligation 4e52af7e): past the soft ceiling only URGENT mail wakes;
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
    # the cap outranks grace — the harder safety signal wins when both apply
    assert should_wake(enabled=True, recent_wakes=5, rate_cap=5, within_grace=True) == "rate-capped"
    # neither → WAKE
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5, within_grace=False) is None


def test_wake_prompt_carries_the_upward_duty() -> None:
    # the woken agent's contract: settle what it handles, and REPORT UP when the loop closes —
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
    await send_message(actions.pool, from_agent="agent:other", from_project="other",
                       to_project="demo", body="please look at X")


async def test_trigger_is_dormant_when_disabled(actions: Actions) -> None:
    await _agent_with_mail(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=False), spawn=_spawn)
    assert spawned == [] and rep["woke"] == 0  # OFF by default — nothing woken


async def test_a_scoped_rearm_touches_ONLY_its_named_subjects(actions: Actions) -> None:
    """THE RE-ARM SCOPE (2026-07-14, the pokex pile-drain experiment): every handoff since
    XXVII said 'turn it on for ONE project, watched' — and until tonight that was a promise,
    not a setting. Armed with an allowlist, unread mail OUTSIDE the scope is scoped_out, never
    woken; the named project wakes normally. An empty allowlist keeps the old behavior."""
    await _agent_with_mail(actions)  # project 'demo' has unread mail
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    # armed, but the re-arm names a DIFFERENT project — demo's mail waits, no wake
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
    assert len(spawned) == 2  # bounded at the rate cap — the ping-pong halts
    assert "/repo/demo" in spawned[0]  # woke in the recipient's repo
    # the wakes are recorded — the visible, auditable chain
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE to_project='demo'") == 2


async def test_leased_mail_does_not_rewake(actions: Actions) -> None:
    """Mail under a live lease is being processed RIGHT NOW — re-waking would double-spawn.
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
    assert await wake_status(p, "demo", _settings(enabled=True, rate_cap=1)) == "rate-capped"
    # a recent wake under the cap → 'wake-grace', so a sender sees 'processing' not 'off'/'capped'
    assert await wake_status(
        p, "demo", _settings(enabled=True, rate_cap=5, grace=300)) == "wake-grace"


async def test_wake_grace_prevents_the_double_wake(actions: Actions) -> None:
    """The fix (obligation c45bb2e3): the cron ticks (60s) faster than a woken agent spawns,
    mounts, and leases its inbox (~100s+), so the next tick re-wakes the SAME still-deliverable
    message. Within the grace window that re-tick is skipped — one message, one wake."""
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
    woken agent died before reading), the wake re-arms — the mail is not stranded."""
    await _agent_with_mail(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    st = _settings(enabled=True, grace=300, lease=0)
    await trigger_mail_tick(actions, settings=st, spawn=_spawn)  # wakes
    await trigger_mail_tick(actions, settings=st, spawn=_spawn)  # within grace → skip
    assert len(spawned) == 1
    # age the wake past the grace window (deterministic — no sleep) → grace lapses
    await actions.pool.execute(
        "UPDATE agent_wakes SET woke_at = now() - make_interval(secs => 400)")
    await trigger_mail_tick(actions, settings=st, spawn=_spawn)  # grace expired → re-armed
    assert len(spawned) == 2


async def test_spawned_wake_carries_a_durable_job_dir_anchor(actions: Actions) -> None:
    """Obligation e1ed13fb part 1: a triggered `claude -p` gets no CLAUDE_JOB_DIR from any harness,
    so the woken agent used to mount by guessing its identity off a co-tenant's transcript. The
    trigger synthesizes a durable anchor with a 'jobs/wake-<x>' shape _job_id parses.

    AMENDED 2026-07-12: <x> was the WAKE ROW ID, so every wake became a new agent:wake-<id> — 463
    mints, 463 strangers on the roster, 48 of them in a project the operator had not opened in two
    days. A wake is not a new MIND; it is the same errand run again. It is now keyed on the
    PROJECT: one ghost per house, re-worn, and instantly recognisable as a machine.

    The anchor ALSO now rides in the PROMPT as a literal path. It used to be the text
    `$CLAUDE_JOB_DIR`, which a woken agent (tools: mcp__osiris only, no shell) cannot expand — so
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
    # and the agent is TOLD the literal path — it has no shell to expand a variable with
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
    # OPERATOR'S REAL HOME — that is where ~/.osiris/wake-receipts/wake-7.json came from, and a
    # priced test envelope there would bill phantom dollars into llm_usage via meter_receipts.
    monkeypatch.setattr(trigger, "RECEIPTS", tmp_path / "receipts")
    await trigger._spawn_claude("/repo/demo", "wake up", job_dir="/tmp/x/jobs/wake-7")
    assert (tmp_path / "receipts" / "wake-7.json").exists()  # the rehearsal's receipt stayed home
    # by POSITION only where position is load-bearing: `claude -p` leads, and the PROMPT is last
    # (flags are appended between them). Pinning the prompt at index 2 broke the moment the wake
    # learned to keep its receipt — a test asserting an ARRANGEMENT rather than a REQUIREMENT.
    assert captured["args"][:2] == ("claude", "-p")
    assert captured["args"][-1] == "wake up"
    assert captured["env"]["CLAUDE_JOB_DIR"] == "/tmp/x/jobs/wake-7"
    assert "PATH" in captured["env"]  # inherited the parent environment, not a bare dict


async def test_spawn_claude_authorizes_the_graph_hands(monkeypatch: Any) -> None:
    """The wake permission storm (thread ba73c0c8): headless `claude -p` cannot answer a
    permission prompt, so in a repo with no stored approval every mcp__osiris__* call is
    silently denied — the wake dies blind and its mail redelivers forever. The spawner must
    pre-authorize the hands it asks for: --allowedTools rides in the command."""
    from src.orchestrator import trigger

    captured: dict[str, Any] = {}

    class _Proc:
        pid = 4242

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        captured["args"] = args
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
    await trigger._spawn_claude("/repo/demo", "wake up", allowed_tools="mcp__osiris")
    assert ("--allowedTools", "mcp__osiris") in _pairs(captured["args"])
    # empty/None = the old behavior: rely on the repo's stored approvals, no flag at all
    await trigger._spawn_claude("/repo/demo", "wake up", allowed_tools=None)
    assert "--allowedTools" not in captured["args"]


def _pairs(args: tuple[Any, ...]) -> list[tuple[Any, Any]]:
    return [(args[i], args[i + 1]) for i in range(len(args) - 1)]


async def test_the_wake_KEEPS_ITS_RECEIPT(monkeypatch: Any, tmp_path: Path) -> None:
    """OSIRIS'S MOST EXPENSIVE ACT THREW AWAY THE VENDOR'S OWN PRICE FOR IT, 463 TIMES.

    A wake is a whole Claude session — with tools, in a repo, on the operator's card. It was
    spawned with `stdout=DEVNULL`, so the CLI's output envelope went in the bin. That envelope
    carries `total_cost_usd`: authoritative, free, volunteered on every call. It is EXACTLY where
    the miner's $40.49-to-the-cent comes from. Nobody ever read it, and so the single question the
    operator actually cares about — what does this cost per day? — had no answer for eight days.

        A HAND YOU CANNOT COST IS A HAND YOU CANNOT GOVERN.

    And it stays FIRE-AND-FORGET. The receipt goes to a FILE and nothing awaits the process —
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
    await trigger._spawn_claude("/repo/demo", "wake up", job_dir="/home/x/.claude/jobs/abcd1234")

    assert ("--output-format", "json") in _pairs(captured["args"]), "the CLI was not asked to price"
    assert captured["stdout"] is not trigger.asyncio.subprocess.DEVNULL, "the receipt was binned"
    assert (tmp_path / "receipts" / "abcd1234.json").exists()


async def test_every_wake_lane_passes_the_allowed_tools(actions: Actions) -> None:
    """The mint lane (and by the same call shape, both resume lanes) forwards the setting —
    a wake is born with its graph hands authorized, not hoping for a stored approval."""
    await _agent_with_mail(actions)
    captured: list[Any] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        captured.append(kw.get("allowed_tools"))

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn)
    assert rep["woke"] == 1 and captured == ["mcp__osiris"]

# --- the dispatch order: DELIVER → RESUME → MINT (thread 9f2ddb44) ---

FULL_SID = "abcd1234-0000-4000-8000-000000000000"


async def _stale_resumable_owner(actions: Actions, tmp_path: Path,
                                 transcript_bytes: int = 16) -> Path:
    """An owner for project demo: a durable mount (made STALE so it isn't 'live') whose job_dir
    anchors a real transcript under the sense root — the transcript AGED too, because under
    the adapter's law mid-turn means the TRANSCRIPT is moving (a fresh mtime would read as a
    working mind and correctly refuse to resume). Returns the sense root."""
    import os
    import time as _time

    from src.orchestrator import mounts

    job = tmp_path / "jobs" / "abcd1234"
    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{FULL_SID}.jsonl"
    t.write_bytes(b"x" * transcript_bytes)
    old = _time.time() - 3600
    os.utime(t, (old, old))
    await mounts.save_mount(actions.pool, job_dir=str(job), agent_id="agent:abcd1234",
                            project="demo", cwd="/repo/demo", model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    return sense


async def test_live_owner_gets_delivery_not_a_twin(actions: Actions, tmp_path: Path) -> None:
    """An awake owner (fresh mount) means DELIVER: the mail sits in its box, nothing spawns —
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
    """A stale-but-resumable owner is CONTINUED via its own session — the wake carries
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
    """retired=true is a deliberate close — the dispatch skips resume and MINTS a successor."""
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


async def test_ceiling_transcript_mints_instead(actions: Actions, tmp_path: Path) -> None:
    """A transcript at the context ceiling is retirement-by-compaction territory — resuming it
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
    tried twice — the next wake for that message MINTS."""
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


async def test_a_dm_resumes_the_addressee_itself(actions: Actions, tmp_path: Path) -> None:
    """The payoff: a DM to a stale-but-resumable agent wakes THAT agent via its own session
    — mode 'dm-resume' in the ledger, the private prompt, never a twin."""
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
    leaves the DM pull-only — a private message is never handed to a fresh twin."""
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


async def test_mid_turn_means_the_transcript_is_moving_NOT_the_heartbeat(
    actions: Actions, tmp_path: Path
) -> None:
    """THE STATUSLINE-HEARTBEAT SUPERSTITION, killed at the operator's first live
    round-trip ask (2026-07-20): the chrome bumps agent_mounts.last_seen every few seconds
    FOR BACKGROUNDED SESSIONS TOO, so by that field every seated idle agent read as
    permanently mid-turn and the resume gate could never open. A turn WRITES the
    transcript; a statusline render does not. (a) fresh heartbeat + quiet transcript →
    RESUMED; (b) a genuinely moving transcript → delivered, no second process."""
    import os
    import time as _time

    sense = await _stale_resumable_owner(actions, tmp_path)
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now()")  # the pump
    m1 = await _dm_to_owner(actions)
    spawned: list[dict[str, Any]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(kw)

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense)), spawn=_spawn)
    assert rep["resumed"] == 1 and spawned[0].get("resume_session") == FULL_SID

    # (b) the transcript MOVES (a real turn in flight): a fresh DM is delivered, not woken
    await actions.pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, read_at) "
        "VALUES ($1,$2,now())", m1, "agent:abcd1234")
    await send_message(actions.pool, from_agent="agent:sender", from_project="other",
                       to_agent="agent:abcd1234", body="while you were typing")
    t = sense / "-repo-demo" / f"{FULL_SID}.jsonl"
    now = _time.time()
    os.utime(t, (now, now))
    rep2 = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense)), spawn=_spawn)
    assert rep2["resumed"] == 0 and len(spawned) == 1 and rep2["owner_live"] == 1


async def test_a_dm_resume_is_never_looped(actions: Actions, tmp_path: Path) -> None:
    """One attempt per message: a dm-resume that didn't settle its mail is not retried —
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
    """Wake hygiene (thread fc2071f8): a triage wake is one-shot — the prompt itself carries
    the retire() duty so no zombie card survives it."""
    assert "retire()" in _WAKE_PROMPT and "ONE-SHOT" in _WAKE_PROMPT


def test_a_rate_is_not_a_bound() -> None:
    """THE 2026-07-12 GHOST FARM, in one assertion.

    Every guard in should_wake measured wakes over a SLIDING WINDOW — the per-project cap, the
    hourly budget, the grace. Every one of them RESETS. So one unread letter ("to whoever mounts
    sibling-three next") spawned 79 `claude -p` sessions over 18 hours on a project the operator
    had not opened in two days, minting a fresh agent every ~32 minutes — AT EXACTLY THE CAP.
    The cap was working perfectly, and that was the bug: it bounded the RATE while nothing bounded
    the TOTAL, so a message that could never be settled became a permanent alarm clock ticking at
    the legal limit.
    """
    # the old world: the window has rolled over, so the rate cap happily says WAKE — forever
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
    keep failing louder. `urgent` rides through the budget guards — it must NOT ride through
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
    windows — the cap resets, and the wake fires again, forever. That is exactly what happened:
    one letter spawned 79 sessions over 18 hours on an abandoned project, each wake obediently
    reading it, correctly judging it was not theirs to ack, leaving it politely alone, and
    thereby summoning its replacement. The letter's own politeness was the fuel.

    Now the total bounds it: after `attempts` tries the trigger STOPS on that message forever and
    hands it to the only reader who can act — the human. Nothing is deleted; the letter stays in
    the graph. It simply stops ringing.
    """
    await _agent_with_mail(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    # rate_cap high + window rolling is the old world; the TOTAL is what must bite.
    st = _settings(enabled=True, rate_cap=99, lease=0, attempts=3)
    for _ in range(20):                      # twenty ticks, mail never settled — the storm
        await trigger_mail_tick(actions, settings=st, spawn=_spawn)

    assert len(spawned) == 3, "the TOTAL must bound it — a rate would have spawned 20"

    # the tombstone: recorded once, and excluded from the attempt count so it cannot re-arm
    tombs = await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE mode='abandoned'")
    assert tombs == 1

    # AND THE HUMAN IS TOLD — the loop may close, but never silently
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
    """The prompt gave a triage wake two exits — ack it, or leave it — and 'leave it' silently
    re-armed the wake. It needs the THIRD DOOR named, or a well-behaved agent keeps the loop
    alive by doing exactly what it was told."""
    assert "MUST NOT LEAVE MAIL UNSETTLED" in _WAKE_PROMPT
    assert "THIRD DOOR" in _WAKE_PROMPT
    assert "obligation" in _WAKE_PROMPT


def test_a_wake_gets_one_stable_ghost_per_project_not_one_per_wake() -> None:
    """463 MINTS, 463 IDENTITIES, AND NOT ONE OF THEM THE INTENDED ONE.

    The anchor was keyed on the WAKE ROW ID, so every wake resolved to a fresh agent:wake-<id>
    and the roster filled with strangers the operator never started — 48 in sibling-three alone,
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
    """A woken agent has NO SHELL — its tools are mcp__osiris only — so `$CLAUDE_JOB_DIR` in a
    prompt is just text it hands over verbatim. The mount-anchor hook rightly refuses a '$'-bearing
    path and derives one from the SESSION id instead, fresh for every `claude -p`. That is how all
    463 mints got a new identity while the stable anchor sat unused.

    Ruling 40faa5e6 had already fixed exactly this for the SessionStart whisper ("tell the agent
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
    was ever in the ledger — because the spawner threw the vendor's own receipt at /dev/null.

    Every other guard on this path is a RATE (wakes per hour, attempts per message). AND A RATE
    IS NOT A BOUND: the wake storm ran for days at a perfectly legal 5/hr, and every guard was
    working exactly as designed while it happened. A rate limits how FAST you burn. Only a
    ceiling limits how MUCH.
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
        actions, settings=_settings(enabled=True, daily_usd=10.0), spawn=_spawn)

    assert spawned == [], "the ceiling was reached and the trigger spawned anyway"
    assert rep.get("refused") == 1
    assert "CEILING REACHED" in str(rep.get("why", ""))


async def test_poke_lane_types_into_the_open_window_before_any_resume(
    actions: Actions,
) -> None:
    """THE WAKE LAW (Phase 2, Stage C): mail for a project whose session lives in a manager
    window becomes a TURN in that window — typed, recorded mode='poke', no process spawned.
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
    # (mode='poke'), so the lane escalates PAST the window — sense="" makes that a mint
    async def _poke_deduped(name: str, text: str, *, dedup: str,
                            min_idle: int) -> dict[str, Any]:
        return {"poked": name, "deduped": True}

    rep2 = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn,
                                   windows=_windows, poke=_poke_deduped)
    assert rep2["poked"] == 0 and spawned == ["/repo/demo"]  # escalated to the mint rung


async def test_poke_only_arms_the_window_and_nothing_else(actions: Actions) -> None:
    """THE POKE-ONLY ARM (operator, 2026-07-19: 'arm the poke ... but dont turn on the
    miners or critter background agents yet'): with the lane switch on, the ladder ends at
    the poke. (1) mail with NO window is HELD — never minted; (2) a poke already typed for
    the same cause and still unsettled is HELD — never escalated to resume/mint; (3) an
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

    # (3) an open window gets its turn — the poke lane itself is untouched by the switch
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
    # to a mint — poke-only HOLDS instead. No process, ever.
    async def _poke_deduped(name: str, text: str, *, dedup: str,
                            min_idle: int) -> dict[str, Any]:
        return {"poked": name, "deduped": True}

    rep3 = await trigger_mail_tick(actions, settings=_settings(enabled=True, poke_only=True),
                                   spawn=_spawn, windows=_windows, poke=_poke_deduped)
    assert spawned == [], "poke-only escalated to a spawn — the forbidden rung fired"
    assert rep3["poke_only_held"] == 1 and rep3["resumed"] == 0


async def test_the_dm_lane_rides_its_own_arm_not_poke_only(
    actions: Actions, tmp_path: Path
) -> None:
    """SUPERSESSION ON THE RECORD (ruling 6c4d0b62): the poke-only arm (operator,
    2026-07-19) used to hold the DM resume rung too — then the adapter ruling (2026-07-20)
    made resume the DM lane's PRIMARY push. poke_only still holds the BROADCAST spawn
    rungs (that word stands: no critter background agents); the DM lane rides its OWN arm,
    osiris_dm_resume — poke_only=True no longer touches it, and dm_resume=False is the
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
    spawned — the mail waits for the next tick or the window's own next osiris call."""
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
    """The DM half: the addressee's own window — matched by ANY generation's anchor (the
    lineage rollup's discipline) — gets the private prompt, recorded mode='dm-poke'."""
    from src.orchestrator.mounts import save_mount

    a = await actions.create_or_find_object("Agent", "agent:demo-ii", "session")
    await actions.assert_property(a, "project", "demo", "session", NOW, 0.9)
    await send_message(actions.pool, from_agent="agent:other", from_project="other",
                       to_agent="agent:demo-ii", body="for your eyes")
    # the lineage's LIVE window anchors on the BASE generation's old job_dir — still his
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
    carries spawn_parent (the seat's living head) — the child is born declared, never an
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

# ═══ the background-session adapter (ruling 6c4d0b62): dispatch_dm, the per-hop grammar ═══
# The fleet is harness-backgrounded sessions under one spawner pty — no pty fd, no turn in
# flight — so RESUME is the DM lane's primary push, dispatched PER MESSAGE on arrival
# (send()'s immediate leg) with the worker tick as the queue-draining backstop. These tests
# pin the four walls: immediate, gated (needs-input / pause), flat, braked.


async def test_dispatch_is_immediate_and_carries_the_receipt(
    actions: Actions, tmp_path: Path
) -> None:
    """The immediate leg: ONE dispatch_dm call — the thing send() fires on arrival — pushes
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


async def test_an_fyi_dm_never_wakes(actions: Actions, tmp_path: Path) -> None:
    """The grammar's loop terminator: grade='fyi' + ack settles WITHOUT minting a turn — so
    an fyi never resumes anybody. It waits, readable, for the addressee's own next turn;
    this is what ends an A<->B exchange instead of ping-ponging it to the ceiling."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    out = await send_message(actions.pool, from_agent="agent:sender", from_project="other",
                             to_agent="agent:abcd1234", body="done, for the record",
                             grade="fyi")

    async def _boom(repo: str, prompt: str, **kw: Any) -> None:
        raise AssertionError("an fyi minted a turn — the terminator failed")

    st = _settings(enabled=True, sense=str(sense))
    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=int(out["id"]),
                          sender="agent:sender", settings=st, spawn=_boom,
                          windows=_no_windows)
    assert d["mode"] == "queued-fyi"
    # and the backstop sweep holds the same line — two callers, one grammar
    rep = await trigger_mail_tick(actions, settings=st, spawn=_boom)
    assert rep.get("dm_queued") == 1 and rep["woke"] == 0
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


async def test_a_paused_seat_queues_and_release_drains(
    actions: Actions, tmp_path: Path
) -> None:
    """Wall #2, the explicit arm: paused=true holds the push lane (mail queues, nothing
    lost); the newest paused assertion wins, so a release is just the next word — and the
    queued DM rides the very next dispatch."""
    from datetime import timedelta

    sense = await _stale_resumable_owner(actions, tmp_path)
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
    # the release: a NEWER paused=false — latest word wins, the queue drains
    await actions.assert_property(a, "paused", False, "agent:abcd1234",
                                  NOW + timedelta(hours=1), 0.9,
                                  evidence_class="self_declared")
    d2 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows)
    assert d2["mode"] == "resumed" and spawned == ["/repo/demo"]


async def test_needs_input_gates_until_the_operators_word(
    actions: Actions, tmp_path: Path
) -> None:
    """Wall #2, the implicit arm: a seat whose last act was asking the human (an undismissed
    decision/hands brief, quiet since) is not peer-resumable — its mail queues; the human's
    word is the release. Peer mail must never preempt the operator's judgment."""
    sense = await _stale_resumable_owner(actions, tmp_path)  # mount last_seen: 1h ago
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
    # the human answers (the brief is dismissed) — the gate lifts, the queue drains
    await actions.pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, read_at) "
        "VALUES ($1,$2,now())", int(brief["id"]), OPERATOR_ADDR)
    d2 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows)
    assert d2["mode"] == "resumed" and spawned == ["/repo/demo"]


async def test_an_fyi_brief_never_gates(actions: Actions, tmp_path: Path) -> None:
    """Only decision/hands briefs mean 'awaiting the word' — a loop-closed fyi on the desk
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
    lane matched it against agent_mounts verbatim — so every seat-BOUND addressee (the
    whole charter pattern, alfred's exact case) was silently pull-only. The dispatch now
    resolves seat → holder → living head before looking for a session."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    seat = await actions.create_or_find_object("Seat", "seat:demo-charter", "session")
    holder = await actions.create_or_find_object("Agent", "agent:abcd1234", "session")
    await actions.create_link(holder, seat, "holds", "session", NOW, 0.9)
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
    """Wall #4: an A<->B ping-pong is legal work until a brake says otherwise — and the
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
    ride along — the resumed session reads its WHOLE box, so nothing needs a second spawn."""
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


async def test_a_dm_resume_never_pins_the_triage_model(
    actions: Actions, tmp_path: Path
) -> None:
    """A DM resume continues a REAL seat's own session — osiris_wake_model (the haiku
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

# ═══ the daemon-reply rung (thread 4261a0d8): the VISIBLE hop leads the ladder ═══


async def test_the_daemon_reply_rung_leads_and_wears_the_envelope(
    actions: Actions, tmp_path: Path
) -> None:
    """The ghost problem's fix, operator-confirmed 2026-07-20: a daemon-held addressee is
    NUDGED through the harness daemon (the front renders daemon-owned turns) instead of
    resumed by a second process — and the injected turn is a CUTE LITTLE MAIL: full
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
        return {"short": "abcd1234", "name": "[D] Demo", "_sock": "/nowhere"}

    async def _nudge(job: dict[str, Any], text: str) -> bool:
        nudges.append((job, text))
        return True

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        raise AssertionError("a daemon-held addressee must be nudged, never resumed")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, jobs=_jobs, nudge=_nudge)
    assert d["mode"] == "nudged" and "[D] Demo" in d["detail"]
    job, text = nudges[0]
    assert f"DM #{msg_id}" in text                      # which message
    assert "agent:sender" in text                        # who
    assert "agent:abcd1234" in text                      # to whom
    assert "ask — needs your reply or act" in text       # what grade
    assert "the colorbar shipped" in text                # the preview
    assert f"send(reply_to={msg_id})" in text            # how to settle
    assert await actions.pool.fetchval(
        "SELECT mode FROM agent_wakes ORDER BY id DESC LIMIT 1") == "dm-reply"


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
        return {"short": "abcd1234", "_sock": "/nowhere"}

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
    send leg, or a redelivery) skips — the envelope already knocked once."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await _dm_to_owner(actions)

    async def _jobs(ids: set) -> dict[str, Any]:
        return {"short": "abcd1234", "_sock": "/nowhere"}

    async def _nudge(job: dict[str, Any], text: str) -> bool:
        return True

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        raise AssertionError("nothing may spawn in this test")

    st = _settings(enabled=True, sense=str(sense))
    d1 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows, jobs=_jobs, nudge=_nudge)
    d2 = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                           sender="agent:sender", settings=st, spawn=_spawn,
                           windows=_no_windows, jobs=_jobs, nudge=_nudge)
    assert d1["mode"] == "nudged" and d2["mode"] == "skipped-once-per-message"
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE message_id=$1", msg_id) == 1
