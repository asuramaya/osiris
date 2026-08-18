"""The fleet trigger-hook — the mailbox's alarm clock, bounded against recursion.

The mailbox is pull-based; this lets the WORKER wake an agent when a project has deliverable
mail. The named danger is the A↔B ping-pong. These tests prove the safety story: OFF by
default, a per-project RATE CAP that halts a loop even under persistent unread mail, no wake
while a live lease says the mail is already being processed, and the operator's desk is never
woken (it has no repo — the human reads it, membrane #6's upward lane).
"""
from __future__ import annotations

import json
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
    # — rate caps, leases, alternation — in isolation. The ceiling has its own suite
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
    # send_message now refuses a to_project nobody has ever mounted under (shape 3 of
    # #117, obligation 45e52530) — alive=False so this registers 'demo' as existing
    # without stamping a live last_seen, which would trip _owner_live() and make the
    # trigger deliver instead of mint/resume/poke (the file's own idiom — see the
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
    # RECEIPT HONESTY (thread aa58c1e4): a skip reason now names its own retry cadence —
    # 'the sweep retries' told a sender nothing about WHEN; '~60s' is measured (decision
    # 636c8abd), not a guess
    rate_capped = await wake_status(p, "demo", _settings(enabled=True, rate_cap=1))
    assert rate_capped.startswith("rate-capped") and "~60s" in rate_capped
    # a recent wake under the cap → 'wake-grace', so a sender sees 'processing' not 'off'/'capped'
    grace_status = await wake_status(
        p, "demo", _settings(enabled=True, rate_cap=5, grace=300))
    assert grace_status.startswith("wake-grace") and "~60s" in grace_status


async def test_wake_status_poke_only_names_the_real_limit_instead_of_a_blanket_armed(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE LYING-RECEIPT FIX (thread aa58c1e4, decision 636c8abd): a broadcast has NO
    daemon-reply lane (DM-only) — under poke-only mode with no open manager window for the
    project, the ladder is GUARANTEED to terminate at 'held' (no resume, no mint, ever), so
    the old blanket 'armed' was a lie a sender had no way to see through. With a window
    present, poke really would succeed, so 'armed' stays honest."""
    p = actions.pool
    st = _settings(enabled=True, poke_only=True)
    # _dark_manager (autouse) darkens _manager_windows to [] for this whole module — the
    # no-window case is the default here, nothing extra to arrange
    status = await wake_status(p, "demo", st)
    assert "poke-only" in status and "will NOT be pushed" in status
    assert "no daemon-reply lane" in status.lower()
    # a real manager window for THIS project flips the verdict back to armed — poke
    # genuinely would deliver it. wake_status's window check reads agent_mounts (the
    # durable mount row), not the graph — mounts.save_mount is the real write path
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
                                 transcript_bytes: int = 16, *,
                                 bind_seat: bool = True) -> Path:
    """An owner for project demo: a durable mount (made STALE so it isn't 'live') whose job_dir
    anchors a real transcript under the sense root — the transcript AGED too, because under
    the adapter's law mid-turn means the TRANSCRIPT is moving (a fresh mtime would read as a
    working mind and correctly refuse to resume). Returns the sense root.

    ALSO seats agent:abcd1234 with an office at the SAME cwd and asserts the graph's own
    `session`/`seat_generation` properties (task #178: dispatch_dm's resume selection now
    reads `_lineage_resume_candidate` — graph truth, via `succession_chain` — never
    `agent_mounts` alone; a fixture that only wrote the mount row is invisible to it).
    `bind_seat=False` skips the seat-binding half only — a caller that ALSO calls
    `_managed_pair` for agent:abcd1234 must bind its own seat there instead (`bind_holder`
    never invalidates an agent's holds on a DIFFERENT seat, only a different agent's hold
    on the SAME seat — two calls would leave abcd1234 holding two seats at once, breaking
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
    # transcript testifies who lives there — here, a signed send receipt as the harness
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


# --- the ceiling checks the RESUMABLE TAIL, not raw file size (thread 771366d1, task #135/
# #136) — a transcript's cumulative lifetime size is a poor proxy for what a resume actually
# has to hydrate; Claude Code auto-compacts, and only the content since the LAST compaction is
# live. Verified against two real specimens: imhotep XVIII (72MB, 17 boundaries, 2.29MB tail)
# and seshat XXIII (103MB, 20 boundaries, 2.23MB tail) — both under 3% of their own file size.
# `resumable_tail_bytes` itself lives in src/ingest/sessions.py (tests: test_sessions.py) —
# these tests cover only `_pick_resumable_sync`'s own use of it. ----------------------------

_COMPACT_LINE = (
    b'{"type":"system","subtype":"compact_boundary","summary":"compacted"}\n'
)


def _write_transcript(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)


def test_pick_resumable_sync_rescues_a_large_transcript_with_a_small_tail(
    tmp_path: Path,
) -> None:
    """THE SIZE FIX, isolated: raw size is well over the ceiling, but everything since the
    last compaction fits comfortably under it — now correctly resumable BY SIZE, where the
    old raw-size check would have refused it (exactly Sekhmet's two real repro cases,
    imhotep XVIII and seshat XXIII). min_tail_bytes=1 widens the OTHER gate out of the
    way on purpose — this test is about the size fix alone; the compaction gate has its
    own tests below, since Thoth's ruling (2026-08-03) made the two independent."""
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


# --- the minimum-tail floor, INDEPENDENT of the ceiling (#156's rebuild, 2026-08-09, the
# operator's own correction, replacing the old compaction-COUNT gate): "closed at exactly
# the compaction seam is a rare special case." A tail with real work after the last
# boundary is resumable REGARDLESS of how many times it compacted; only a tail at or near
# zero (the session closed AT the seam) refuses. Measured live on sekhmet: 12 compactions,
# 4.07MB of real work after the last one — the old gate refused her anyway, factually
# wrong about her own transcript (see resume_diagnostics's own docstring). ----------------


def test_pick_resumable_sync_allows_a_compacted_transcript_with_real_tail_work(
    tmp_path: Path,
) -> None:
    """THE EXACT CASE THE OLD COMPACTION-COUNT GATE GOT WRONG: a small tail (well under the
    ceiling) that carries real work since the last compaction boundary IS resumable now,
    however many times it compacted — sekhmet's own live specimen, in miniature."""
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
    passes the floor cleanly — this gate must not misfire on ordinary short sessions."""
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
    boundary — the session closed AT the seam, genuinely nothing to resume into. The ONE
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
    min_tail_bytes=1 isolates the size gate — the compaction gate has its own end-to-end
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
    comfortably under the ceiling, carrying real work since its one compaction, is RESUMED
    — the old gate minted a fresh twin here purely for having compacted at all, which was
    the bug (sekhmet's own live specimen)."""
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


async def _no_job(ids: set[str]) -> dict[str, Any] | None:
    """No daemon job matches — a clean, hermetic 'the daemon lane has nothing' for tests
    that want to exercise the resume lane specifically. NEVER pass a bare `None` for
    `jobs`/`nudge` in these tests: dispatch_dm treats either as 'unset' and falls back to
    the REAL claude_daemon functions, which would reach for a live daemon socket."""
    return None


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


async def test_dispatch_dm_refusal_names_no_anchored_transcript_when_nothing_was_ever_mounted(
    actions: Actions, tmp_path: Path,
) -> None:
    """Thoth's ruling (2026-08-03, #135/#136): dispatch_dm's own refusal used to collapse
    two opposite situations into one identical sentence. This pins the 'genuinely nothing
    to resume' shape — a mount exists, but its job_dir anchors no transcript at all under
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
    every candidate is over osiris_resume_ceiling_bytes — the refusal must say so, not
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
    assert "over the context ceiling" in d["detail"]


# --- wake_gate_preflight (#156.4): the same four gates, answerable before an attempt ------

async def test_wake_gate_preflight_reports_resumable_with_no_side_effects(
    actions: Actions, tmp_path: Path,
) -> None:
    """The happy path: every gate clears, and — unlike dispatch_dm — nothing is spawned,
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
    surface — must agree exactly, since both call the same underlying gate."""
    sense = await _stale_resumable_owner(actions, tmp_path, transcript_bytes=64)
    d = await trigger_module.wake_gate_preflight(
        actions.pool, "agent:abcd1234",
        settings=_settings(enabled=True, sense=str(sense), ceiling=32))
    assert d["mode"] == "resume-refused-ceiling"
    assert d["status"] == "refused-ceiling"
    assert "over the context ceiling" in d["detail"]


async def test_wake_gate_preflight_reports_never_mounted(actions: Actions) -> None:
    """No agent_mounts row at all — nothing to wait for, ever."""
    d = await trigger_module.wake_gate_preflight(
        actions.pool, "agent:totally-unknown", settings=_settings(enabled=True))
    assert d["mode"] == "never-mounted"
    assert d["status"] == "no-live-body"


async def test_wake_gate_preflight_reports_queued_live_when_only_last_active_fresh(
    actions: Actions,
) -> None:
    """Ra XXXV's specimen (msg 4901, threads 94dc4aae + 27917f1f): a mind can be live by
    the SAME registry fleet() trusts (fresh last_active testimony, ruling 70493925) while
    carrying NO agent_mounts row at all — wakeable_identity (agent_mounts-only) finds
    nothing, but that lookup miss is not evidence the mind never mounted. Must report the
    honest 'live, but no session resolved' outcome, never the false-absence 'never-mounted'."""
    a = await actions.create_or_find_object("Agent", "agent:liveonly01", "fleet-observer")
    fresh = datetime.now(UTC).isoformat()
    await actions.assert_property(a, "last_active", fresh, "fleet-observer", NOW, 0.9,
                                  evidence_class="self_declared")
    d = await trigger_module.wake_gate_preflight(
        actions.pool, "agent:liveonly01", settings=_settings(enabled=True))
    assert d["mode"] == "queued-live-unresolved"
    assert d["status"] == "queued"
    assert "has never mounted" not in d["detail"]


async def test_wake_preflight_mcp_tool_resolves_a_seat_and_never_touches_dispatch(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The MCP surface: resolves target the same way wake()/dispatch_dm do, then answers
    read-only — dispatch_dm itself must never be called."""
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
    handle ('metron') silently answered 'never-mounted' for a seat that had actually
    mounted many times — _resolve_wake_address only ever understood 'seat:'/'agent:'
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
    Aegis phantom, 2026-07-21): something in the chrome/daemon touches mtime on a session
    that is OFF — awake and asleep must never be confounded (the operator's ruling).
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

    # (b) THE PHANTOM: mtime bumped, size unchanged, no timestamped turn — the addressee
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
    with t.open("a") as fh:  # the fixture's pad bytes end without a newline — start fresh
        fh.write("\n" + _json.dumps({"type": "assistant",
                                     "timestamp": _dt.now(_UTC).isoformat()}) + "\n")
    rep3 = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense)), spawn=_spawn)
    assert rep3["resumed"] == 0 and len(spawned) == 2 and rep3["owner_live"] == 1


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

    BUT ONLY WHEN THE DOLLARS ARE REAL (Thoth LIII 2026-07-21): the ceiling bites on the keyed
    API backend, where total_cost_usd is a true debit. On a subscription the figure is notional
    and the gate is inert — so this test runs the billed world explicitly (extract_provider=
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
    """THE FALSE STOP, REMOVED (Thoth LIII 2026-07-21). Identical $12-over-$10 ledger to the
    test above — but on a SUBSCRIPTION (the local Claude CLI, the helper's default), where
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


# ═══ BROADCAST DISPATCH (task #151) — the same grammar as a DM's, applied to the surface it
# was missing from. Before this, send(to=<project>) filed a message and computed wake_status
# (a status STRING, no dispatch) — the only push was the worker sweep, up to ~60s later, and
# NONE at all under poke-only with no open window. Thoth LXXII's commit-freeze broadcast
# reached nobody the night of the second history rewrite for exactly this reason.

async def test_an_fyi_broadcast_never_wakes(actions: Actions) -> None:
    """The DM lane's own loop terminator, extended to broadcasts: grade='fyi' never wakes
    anyone. Before this, grade had ZERO effect on broadcast dispatch — confirmed empirically
    (not assumed) before building on it — it only ever reached mount()/orient()'s unread
    count."""
    a = await actions.create_or_find_object("Agent", "agent:demo", "session")
    await actions.assert_property(a, "project", "demo", "session", NOW, 0.9)
    await save_mount(actions.pool, job_dir="/test/seed/demo", agent_id="agent:seed-demo",
                     project="demo", cwd="/test", model=None, session_key=None, alive=False)
    out = await send_message(actions.pool, from_agent="agent:other", from_project="other",
                             to_project="demo", body="fyi: filed for the record",
                             grade="fyi")

    async def _boom(repo: str, prompt: str, **kw: Any) -> None:
        raise AssertionError("an fyi broadcast minted a turn — the terminator failed")

    st = _settings(enabled=True)
    d = await dispatch_broadcast(actions.pool, project="demo", msg_id=int(out["id"]),
                                 sender="agent:other", settings=st, spawn=_boom,
                                 windows=_no_windows)
    assert d["mode"] == "queued-fyi"
    # and the backstop sweep holds the same line — two callers, one law, no drift
    rep = await trigger_mail_tick(actions, settings=st, spawn=_boom)
    assert rep["fyi_queued"] == 1 and rep["woke"] == 0
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


async def test_dispatch_broadcast_pokes_an_open_window_immediately(actions: Actions) -> None:
    """The IMMEDIATE LEG (send()'s own call, simulated here directly): an 'ask'-graded
    broadcast pokes a manager-hosted open window on arrival, not up to ~60s later at the
    sweep's own pace — the exact latency gap task #151 exists to close."""
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
    # msg:<id>, between the immediate leg and the backstop) — it does not double-poke, but
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
    send leg, or a redelivery) skips — the envelope already knocked once."""
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
    # RECEIPT HONESTY (thread aa58c1e4, decision 636c8abd): 'nudged' means the daemon
    # ACCEPTED the injection, never that the turn already ran — 'landed as X's next turn'
    # overclaimed a confirmed outcome from a bare queue success (ruling 986b12f0's own
    # distinction)
    assert "ACCEPTED" in d1["detail"] and "landed as" not in d1["detail"]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE message_id=$1", msg_id) == 1


# ═══ the third state (task #176, 2026-08-18): the daemon accepted, but nobody confirmed
# home — practice 2c45d78e's "must be able to say I don't know" applied to dispatch_dm's
# own strongest-looking receipt ═══

async def test_a_nudge_with_no_confirmed_listener_is_queued_not_nudged(
    actions: Actions, tmp_path: Path
) -> None:
    """The daemon's {ok:true} means it ACCEPTED the envelope into its own queue, never that
    a live reader is there — a job it still lists after the body exited, or one that
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
    check, so the once-per-message brake still fires on a second dispatch — at-least-once
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
    """`_confirm_listener` matches a job the SAME way `claude_daemon.job_for` does — short
    id, full session id, or its 8-char prefix — so the two can never disagree about which
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
    'cannot confirm' — False, never a raised exception that would strand the caller."""
    from src.orchestrator.trigger import _confirm_listener

    async def _broken() -> list[dict[str, Any]]:
        raise TimeoutError("harness CLI hung")

    assert await _confirm_listener({"short": "abcd1234"}, _broken) is False


async def test_mail_settled_by_a_successor_is_never_phantom_nudged(
    actions: Actions, tmp_path: Path
) -> None:
    """The per-agent-id read-state class (bug 00378259), third bite: the deliverable query
    keys settlement on the EXACT addressed id, so a DM to an old generation that the LIVING
    HEAD already settled reads deliverable forever — caught live when the lane's first
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
    """THE LEAK FIX (operator, 2026-07-20: 'the leak has to be fixed — how can it resolve
    the current agent I'm speaking with?'): not by registry timestamp — the statusline pump
    keeps a stale claimant row eternally fresh. The RESIDENT'S SIGNATURE decides: the
    newest signed osiris act in the session's own append-only transcript. When the
    registry's door leads to a session whose signatures name a DIFFERENT mind (the Ra
    misdelivery, thread 0100a35e), BOTH the nudge and the resume refuse — the mail stays
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
    """Ruling f624d114 — the 18th specimen of 60bc15db: an EMPTY lookup (a transcript
    exists and resumes fine, but nothing SIGNED appears anywhere in it — no whisper, no
    mount, no send) must never be rendered with the same words as a POSITIVE identity
    mismatch. Same registry shape as the crossed-registry test above (a mounted door, a
    resumable candidate) but the transcript's content is unsigned harness noise, not a
    signature naming a stranger — so this must refuse as `resident-unknown`, never
    `crossed-registry`, and the detail text must never claim a different mind was found.

    ONE HOP BACK, deliberately (task #178's own zero-hop graph door would otherwise
    legitimately RESUME this exact unsigned-but-graph-corroborated shape at hop 0 — see
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
    # a real, resumable transcript — just nothing signed anywhere in it
    t.write_bytes(b'{"type":"assistant","text":"just harness chrome, nothing signed"}\n')
    old = _time.time() - 3600
    os.utime(t, (old, old))
    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "abcd1234"),
                            agent_id="agent:abcd1234", project="demo", cwd="/repo/demo",
                            model=None, session_key=None)
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    await _seat_and_graph_session(actions)
    # a fresher-mounted successor with NO graph session of its own — wakeable_identity
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
    bug this fix closes, ruling f624d114)."""
    from src.orchestrator import mounts

    sense = tmp_path / "projects"
    sense.mkdir(parents=True, exist_ok=True)
    st = _settings(enabled=True, sense=str(sense))
    resume = (FULL_SID, "/repo/demo", 0.0, "abcd1234")
    # no transcript anywhere under `sense` for FULL_SID — the unknown arm
    gate, detail = await trigger_module._resume_guard(
        actions.pool, resume, "agent:abcd1234", seat_id=None, st=st)
    assert gate == "resident-unknown"
    assert detail is not None
    assert "signed testimony names a different mind" not in detail

    # now a transcript that positively names a stranger — the mismatch arm
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
    assert "signed testimony names a different mind" in detail2


# ═══ THE CORROBORATION FALLBACK (thread 25943031, halcyon's own stranding) ═══════════════
# halcyon's parked session was provably its own — every signature in the whole transcript
# was its own lineage — but the LAST 400KB was all unsigned harness noise (away summaries,
# chrome), so the old tail-only check read it as a stranger and refused both nudge and
# resume. These tests build a transcript whose signed act sits DEEPER than the tail, prove
# the fallback finds and corroborates it, and prove the different-mind arm and the
# registry re-check still refuse when either leg fails (Thoth's design approval, DM 1825).

def _deep_transcript_bytes(*, signed_line: bytes, total_size: int) -> bytes:
    """`signed_line` at offset 0, padded with unsigned filler out to `total_size` bytes —
    the shape of halcyon's own transcript: real signed history, then a long unsigned tail."""
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
    """Beyond `_RESIDENT_DEEP_WINDOWS` windows back, the signature is unreachable — bounded
    cost, not an unbounded scan of an arbitrarily large transcript."""
    proj = tmp_path / "-repo-demo"
    proj.mkdir(parents=True)
    signed = b'{"type":"user","toolUseResult":"{\\"sent\\":1,\\"from\\":\\"agent:abcd1234\\"}"}\n'
    t = proj / f"{FULL_SID}.jsonl"
    # tail (400KB) + 4 extra windows (1.6MB) = 2MB reachable; push the signature well past it
    t.write_bytes(_deep_transcript_bytes(signed_line=signed, total_size=2_500_000))
    resident, path = trigger_module._resident_of_deeper_sync(tmp_path, FULL_SID)
    assert resident is None and path == t  # unreachable — a clean miss, not a wrong guess


async def _mounted_deep_agent(
    actions: Actions, tmp_path: Path, *, agent_id: str = "agent:abcd1234",
    cwd: str = "/repo/demo", seat_id: str | None = None, graph_identity: bool = True,
) -> tuple[Path, Path]:
    """A registry row + a transcript whose signed act sits beyond the tail — the halcyon
    shape. Returns (sense_root, transcript_path).

    `graph_identity=True` (default) ALSO seats `agent_id` with an office at `cwd` and
    asserts the graph's own `session`/`seat_generation` properties (task #178:
    dispatch_dm's resume selection reads `_lineage_resume_candidate` — graph truth —
    never `agent_mounts` alone). Callers that test `_registry_corroborates`/`_resident_*`
    DIRECTLY (never through `dispatch_dm`) pass `graph_identity=False` — those unit tests
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
    else (the door was legitimately reassigned) — corroboration must fail, not trust the
    stale transcript content alone."""
    _sense, t = await _mounted_deep_agent(
        actions, tmp_path, agent_id="agent:newowner")  # registry NOW says newowner
    job_dir = str(tmp_path / "jobs" / "abcd1234")
    # the deep-scan signature (baked into the transcript by the helper) still names
    # abcd1234 — the registry disagrees, so corroboration must refuse FOR abcd1234
    assert not await trigger_module._registry_corroborates(
        actions.pool, job_dir, t, "agent:abcd1234", seat_id=None)


async def test_registry_corroborates_refuses_a_slug_collision(
    actions: Actions, tmp_path: Path,
) -> None:
    """Thoth's own instruction (DM 1825): dashes AND dots both fold to '-' under
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
    """Not every agent holds a seat — a null seat_id on the registry row is not itself
    evidence against corroboration."""
    _sense, t = await _mounted_deep_agent(actions, tmp_path, seat_id=None)
    job_dir = str(tmp_path / "jobs" / "abcd1234")
    assert await trigger_module._registry_corroborates(
        actions.pool, job_dir, t, "agent:abcd1234", seat_id="seat:the-real-one")


async def test_dispatch_dm_resumes_the_halcyon_shaped_unsigned_tail(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE PAYOFF: a lineage-correct body whose transcript's last 400KB is all unsigned
    harness noise is no longer stranded — the deeper scan finds its own real signature,
    the registry corroborates, and the resume lane proceeds exactly as if the tail itself
    had been signed."""
    sense, _t = await _mounted_deep_agent(actions, tmp_path)
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    msg_id = await _dm_to_owner(actions)
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, kw))

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("no daemon job was found — nudge must never be reached")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "resumed"
    assert calls and calls[0][1].get("resume_session") == FULL_SID


async def test_a_dm_wakes_the_live_body_even_when_its_declared_successor_never_mounted(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE PAYOFF (thread 28842543): reproduces Imhotep's exact shape — his seat's recorded
    holder was a newer generation that had never mounted, while the bare id was the one
    actually live, mounted, and explicitly addressed. Before this fix, dispatch_dm reused
    living_head's DELIVERY answer (the declared successor) for wake eligibility too, and
    reported 'has never mounted' beside a receipt naming that same live body's fresh
    last_seen. Now the wake path resolves through wakeable_identity and reaches it."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    a = await actions.create_or_find_object("Agent", "agent:abcd1234", "agent:abcd1234")
    # the successor is MINTED (a real Agent object, exactly what mint_heir does) — just
    # never mounted an OS session; lineage_head only advances past a succeeded_by pointer
    # that resolves to a real, active Agent object, so this is Imhotep's actual shape
    await actions.create_or_find_object("Agent", "agent:abcd1234-ii", "agent:abcd1234")
    await actions.assert_property(a, "succeeded_by", "agent:abcd1234-ii", "agent:abcd1234",
                                  NOW, 0.9, evidence_class="self_declared")
    msg_id = await _dm_to_owner(actions)          # addressed to the raw live id, explicitly
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, kw))

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("no daemon job was found — nudge must never be reached")

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "resumed"                  # not 'pull-only' beside a live mount
    assert calls and calls[0][1].get("resume_session") == FULL_SID


async def test_a_dm_still_wakes_the_true_successor_after_a_real_completed_succession(
    actions: Actions, tmp_path: Path,
) -> None:
    """Negative control at the dispatch_dm level: a NORMAL, healthy succession — the
    declared successor has itself also mounted, with its own resumable session — must
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
        raise AssertionError("no daemon job was found — nudge must never be reached")

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
    case — the fallback never overrides a found disagreement, wherever it's found."""
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
    """Thoth's own named case: the transcript's stale deep history still says abcd1234, but
    the door has been legitimately reassigned — the CURRENT registry says otherwise. The
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
    # the old identity's OWN mounts are gone (the door reassigned to newowner) — dispatch_dm
    # never reaches the crossed-registry guard at all here, it stops one step earlier at
    # wakeable_identity finding nothing for abcd1234 specifically (#156.2 clarified the
    # label; the underlying refusal was already this, unchanged by that fix).
    assert d["mode"] == "never-mounted"
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


# ═══ IDENTITY vs OCCUPANCY (task #178, the ferryman/sekhmet wave) ═══════════════════════
# `_lineage_resume_candidate`/`_resume_guard` answer IDENTITY (whose session, graph-truth).
# `_resume_occupancy_gate` answers the OTHER question: is a body ALREADY SITTING there right
# now. A `-p --resume` fired beside a live body forks the mind — these pin that it never does.


async def test_dispatch_dm_refuses_to_fork_a_body_confirmed_via_agents_json(
    actions: Actions, tmp_path: Path,
) -> None:
    """`_confirm_listener` (task #176's own primitive, reused not reimplemented) matches BY
    SESSION ID — a live `claude agents --json` row naming the exact candidate session
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
    assert d["mode"] == "resume-refused-occupied"
    assert "claude agents --json" in d["detail"] and FULL_SID[:8] in d["detail"]
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


async def test_dispatch_dm_refuses_to_fork_a_body_found_via_proc(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`census.live_bodies_by_cwd` matches BY OFFICE DIRECTORY via /proc — catches a live
    claude process sitting in the exact office this resume would land in, whatever session
    id it thinks it has (invisible to the session-id signal above, on purpose)."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await _dm_to_owner(actions)

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a live process sitting at the office must never be forked "
                             "by a second -p --resume")

    from src.orchestrator import census
    monkeypatch.setattr(census, "live_bodies_by_cwd",
                        lambda **kw: {"/repo/demo": [999999]})

    d = await dispatch_dm(actions.pool, addressee="agent:abcd1234", msg_id=msg_id,
                          sender="agent:sender",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom)
    assert d["mode"] == "resume-refused-occupied"
    assert "999999" in d["detail"] and "/repo/demo" in d["detail"]
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0


async def test_dispatch_dm_resumes_when_neither_occupancy_signal_fires(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The negative control: neither signal finds anybody home — the resume proceeds
    exactly as before this gate existed."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    msg_id = await _dm_to_owner(actions)
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        calls.append((repo, kw))

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("no daemon job was found — nudge must never be reached")

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
    """THE ACCEPTANCE #178 NAMED (Thoth msg 5254) — nobody had actually run it end to end
    until now. Replays the exact incident shape live: a body the HARNESS still lists
    (`claude agents --json`), whose `agent_mounts` row is SWEPT (the #178a suspend-never-
    delete sentinel, `mounts.SUSPENDED_AT` — not merely aged stale), sitting on a
    RESUMABLE transcript. Both doors that can spawn a body for this identity are exercised
    against the IDENTICAL fixture, in one test, so neither can silently disagree with the
    other:

    `osiris launch` (launch_seat) — `_launch_twin_check` reads the harness roster BY CWD
    and refuses outright ("already-live"); it never even reaches the resume/identity lane,
    exactly as it must when a live body genuinely already holds the office.

    dispatch_dm's mail-wake lane — `_lineage_resume_candidate` SELECTS THE GRAPH HEAD
    first (the correct session, graph-truth, proven by the session id appearing in the
    receipt's own detail) and only THEN `_resume_occupancy_gate` (matching by session id
    this time, not cwd) refuses the fork ("resume-refused-occupied") — proving identity
    resolution and occupancy refusal are properly sequenced, not accidentally correct.

    Both doors' own spawn/resume_spawn hooks are wired to raise if ever called — the
    strongest assertion available that NO NEW GENERATION is minted by either path."""
    from src.orchestrator.mounts import SUSPENDED_AT

    sense = await _stale_resumable_owner(actions, tmp_path, bind_seat=False)
    # THE SWEEP (#178a, not just "aged stale"): the row survives (findable, never deleted)
    # but reads as dead to is_live() via the epoch sentinel — the real post-OOM-kill shape.
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen=$1 WHERE agent_id='agent:abcd1234'", SUSPENDED_AT)

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:hm-incident178",
        worker_handle="Incident178", house="osiris")
    await _office(actions, worker_seat, "/repo/demo")

    # THE HARNESS'S OWN LISTING — one row satisfies BOTH doors' distinct matchers: launch's
    # twin_check matches by `cwd`, dispatch's occupancy gate matches by `sessionId`.
    listing = _fake_agents_json([[{"sessionId": FULL_SID, "id": FULL_SID[:8],
                                  "cwd": "/repo/demo"}]])

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("the incident-shaped fixture must never mint a new generation")

    launch_receipt = await trigger_module.launch_seat(
        actions, caller="agent:hm-incident178", target=worker_seat, substrate="harness",
        settings=_settings(enabled=True, sense=str(sense)),
        spawn=_boom, resume_spawn=_boom, agents_json=listing)
    assert launch_receipt["status"] == "already-live"
    assert launch_receipt["body_exists"] is True

    msg_id = await _dm_to_owner(actions)
    dispatch_receipt = await dispatch_dm(
        actions.pool, addressee="agent:abcd1234", msg_id=msg_id, sender="agent:sender",
        settings=_settings(enabled=True, sense=str(sense)),
        spawn=_boom, windows=_no_windows, jobs=_no_job, nudge=_boom, agents_json=listing)
    assert dispatch_receipt["mode"] == "resume-refused-occupied"
    # THE GRAPH HEAD WAS ACTUALLY SELECTED (identity), not just refused blind (occupancy):
    # the correct session id names itself in the very detail that then refuses it.
    assert FULL_SID[:8] in dispatch_receipt["detail"]
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_wakes") == 0

    # NO NEW GENERATION: the only Agent object for this identity is still gen 1 — no
    # abcd1234-ii or any other successor was ever minted by either door.
    agents = await actions.pool.fetch(
        "SELECT canonical FROM objects WHERE type='Agent' AND canonical LIKE 'agent:abcd1234%'")
    assert [r["canonical"] for r in agents] == ["agent:abcd1234"]


# ═══ THE KNOCK — wake(), thread 9f566244 piece D, ruling 16722273 ═══════════════════════
# wake() adds ONE thing dispatch_dm doesn't have: the managed_by authority gate. These tests
# pin the gate (both directions authorize, peers/unbound/seatless refuse, nothing is sent on
# a refusal) and the honest vocabulary (dispatch_dm's own "delivered" — genuinely mid-turn,
# unread — must never surface as wake()'s "delivered").


async def _managed_pair(actions: Actions, *, worker_agent: str, manager_agent: str,
                        worker_handle: str = "Worker", manager_handle: str = "Manager",
                        house: str = "demo") -> tuple[str, str]:
    """Two seats, bound to two live agents, with an active managed_by edge worker→manager —
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


# ═══ THE RATE CAP'S UNIT (msg 984, 2026-07-21) — project vs pair ═══════════════════════
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
    # an unrelated wake, same project, a different pair entirely — must not count
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
    traffic sharing the same project (msg 984's measured incident) — only wakes between
    THIS pair count against it."""
    sense = await _stale_resumable_owner(actions, tmp_path, bind_seat=False)
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:sender")
    await _office(actions, worker_seat, "/repo/demo")
    # flood the project-wide wake count — enough to blow a project-scoped cap of 1, none of
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
    # the pair's OWN count is 0 — the unrelated flood never touches it
    assert d["mode"] == "resumed"


async def test_dispatch_dm_pair_scoped_cap_still_bounds_the_pingpong(
    actions: Actions, tmp_path: Path,
) -> None:
    """The unit changed; the bound itself must not have. A wake already recorded for THIS
    pair, within the window, still caps the next one — the ping-pong halts exactly as
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
    the ORIGINAL project-wide brake — the fix narrows the cap only where the pair concept
    actually applies; everything else is exactly as protected as it was before."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    # no _managed_pair call — "agent:sender" and "agent:abcd1234" share no managed_by edge
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
    """THE HUMAN-ATTENDED GUARD'S REAL SIGNAL (thread 96f62338, replacing ruling d8a77f80's
    managed_by proxy). agent:abcd1234's seat is stamped attended='human' via set_seat_attended
    — mail to it waits in the box and is perceived by PULL (mailbox + stop-hook), never a
    forged-human daemon injection into the operator's live turn. Merely MANAGING someone is no
    longer sufficient on its own (see the regression test right below this one) — the explicit
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
    """THE REGRESSION THIS THREAD FIXES (96f62338): a seat that merely manages a sub-worker or
    a test seat — Imhotep's own flip-test mints, alfred's #50-pilot workers — must NOT be
    silently reclassified as human-attended just because managed_by points at it. With no
    `attended` stamp and a handle that isn't thoth's, dispatch proceeds normally (never
    queued-human) — the old proxy would have wrongly queued this and starved the push lane."""
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
    """The rollout belt (96f62338): a seat with NO explicit attended stamp yet still falls
    back to human-attended if — and only if — its handle is thoth's, so the one seat that
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
    """No held seat, no knock — managed_by is seat-to-seat and an unseated mind has no
    relationship it could invoke it with."""
    d = await wake_worker(actions, caller="agent:nobody", target="agent:alsonobody",
                          message="hey")
    assert d["mode"] == "refused-not-your-worker" and "holds no seat" in d["detail"]


async def test_wake_refuses_a_seatless_target(actions: Actions) -> None:
    """The caller holds a seat but the target names nobody living — refused, nothing sent."""
    worker_seat = (await ensure_seat(actions, house="demo", handle="Solo",
                                     source="test"))["seat_id"]
    await bind_holder(actions, seat_id=worker_seat, agent_id="agent:solo01")
    d = await wake_worker(actions, caller="agent:solo01", target="agent:ghost99",
                          message="hey")
    assert d["mode"] == "refused-not-your-worker" and "no living Seat" in d["detail"]
    assert await actions.pool.fetchval("SELECT count(*) FROM fleet_messages") == 0


async def test_wake_refuses_a_peer_with_no_managed_by_edge(actions: Actions) -> None:
    """Two seated minds, no managed_by edge between them — a peer knock, refused. This is
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
    `marker` to the fixture transcript `_stale_resumable_owner` already created — the mocked
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
    seat via the SAME dispatch path send() uses — CONFIRMED landed via the outcome-read
    (ruling 986b12f0), not merely queued."""
    sense = await _stale_resumable_owner(actions, tmp_path, bind_seat=False)
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:sender", manager_agent="agent:abcd1234")
    # thread 96f62338: attendance is an explicit stamp now, not inferred from managed_by —
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
    # the manager is HUMAN-ATTENDED (the real signal now, thread 96f62338): the knock is
    # authorized but delivered by PULL — it waits in the manager's box and surfaces on its
    # next turn / the stop-hook, never a forged injection into the operator's live turn.
    # 16722273's "the worker can reach up" is preserved; only the delivery mechanism changes.
    assert d["status"] == "queued-human-attended" and d["raw_mode"] == "queued-human"
    assert d["seat"] == manager_seat
    assert not calls  # NOTHING injected or spawned — the human perceives it via the mailbox
    row = await actions.pool.fetchrow(
        "SELECT to_agent, grade FROM fleet_messages WHERE id=$1", d["message_id"])
    assert row["to_agent"] == manager_seat and row["grade"] == "ask"  # the ask waits in the box


async def test_wake_authorizes_manager_to_worker_too(actions: Actions, tmp_path: Path) -> None:
    """Ruling 16722273: the gate is bidirectional. A manager knocking DOWN on its own
    worker is just as authorized as the reverse — only peers and strangers refuse."""
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


async def test_wake_is_FROZEN_when_the_flag_is_off(actions: Actions, tmp_path: Path) -> None:
    """THE HANDOFF'S DEPLOY BIND, CLOSED (Thoth LIII 2026-07-21). wake() rides the daemon reply
    lane — a confirmed RCE — so it ships FROZEN (osiris_wake_enabled=False). An AUTHORIZED pair,
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
    """THE OUTCOME-READ'S WHOLE POINT (ruling 986b12f0): a daemon/resume success is a QUEUE
    success, not a SEEN one. When the marker never appears in the target's transcript (the
    ordinary case in these tests, since the mocked spawn/nudge writes nothing real), wake()
    must NOT claim "delivered" — it downgrades honestly to "queued", unconfirmed."""
    sense = await _stale_resumable_owner(actions, tmp_path, bind_seat=False)
    # knock DOWN on a worker (abcd1234), the injectable direction — a manager target would be
    # pull-only by the human-attended guard and never reach the marker-downgrade path this pins.
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:sender")
    await _office(actions, worker_seat, "/repo/demo")

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        pass  # never writes the marker anywhere — nothing "lands"

    d = await wake_worker(actions, caller="agent:sender", target=worker_seat,
                          message="status?",
                          settings=_settings(enabled=True, sense=str(sense)),
                          spawn=_spawn, windows=_no_windows)
    assert d["raw_mode"] == "resumed"  # dispatch_dm itself still reports success...
    assert d["status"] == "queued" and d["observed"] is False  # ...but wake() won't inherit it
    assert "not yet confirmed" in d["detail"].lower()


async def test_wake_never_calls_mid_turn_delivered(
    actions: Actions, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE WHOLE POINT: dispatch_dm's own mode literally named "delivered" means the
    addressee is mid-turn and has NOT read the message. wake() must translate it to
    "mid-turn", never let the lying word reach the caller."""
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:sender", manager_agent="agent:abcd1234")

    async def _fake_dispatch(*a: Any, **kw: Any) -> dict[str, Any]:
        return {"mode": "delivered", "detail": "genuinely mid-turn, unread"}

    monkeypatch.setattr(trigger_module, "dispatch_dm", _fake_dispatch)
    d = await wake_worker(actions, caller="agent:sender", target=manager_seat,
                          message="hey", settings=_settings(enabled=True))
    assert d["status"] == "mid-turn"
    assert d["raw_mode"] == "delivered"  # the raw truth stays visible for anyone who reads it


async def test_wake_translates_pull_only_and_refused_budget(
    actions: Actions, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other two named buckets: nobody home, and the dollar wall — each its own honest
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


async def test_wake_translates_the_named_gate_refusals_and_not_injectable(
    actions: Actions, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#156.2: the third bucket — a body/session DOES exist but a NAMED gate refused it —
    must say WHICH gate, never a bare 'refused' or the old 'no-live-body' lie (a mind
    exists here; resuming it just isn't safe). And a system-config reason (the trigger
    switched off) must never read as 'nobody home' either — the mail IS queued and WILL
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
    above) — the old shared mode string could not tell these apart."""
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


async def test_dispatch_dm_reports_queued_live_not_never_mounted_when_only_last_active_fresh(
    actions: Actions,
) -> None:
    """The dispatch_dm sibling of Ra's specimen: a live-by-last_active addressee with no
    agent_mounts row must never earn the manager's 'escalate, worker is gone' receipt
    (60bc15db's exact shape) — it queues, an outcome, not a failure."""
    a = await actions.create_or_find_object("Agent", "agent:liveonly02", "fleet-observer")
    fresh = datetime.now(UTC).isoformat()
    await actions.assert_property(a, "last_active", fresh, "fleet-observer", NOW, 0.9,
                                  evidence_class="self_declared")
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


def test_gate_name_reads_the_same_prose_the_gates_already_produce() -> None:
    """#156.2: a stable short token per named gate, read from the SAME sentences
    `_resume_candidate_verdict`/`_resume_miss_reason` already produce for humans — never
    a second source of truth, and never a guess on unrecognized text.

    NEVER FED `_resume_guard`'s own prose (ruling f624d114): that gate returns its token
    ("crossed-registry" / "resident-unknown") directly now, precisely so this function
    never has to re-derive a mismatch/unknown distinction by string-matching rendered
    text — former crossed-registry prose now falls through to "unknown" here, same as
    any other text this function was never meant to parse."""
    gate_name = trigger_module._gate_name
    assert gate_name("found a candidate, but its tail after the last compaction boundary "
                     "is only 12 byte(s) (1 line(s)) — it closed at or near the seam "
                     "itself, with nothing real to resume into") == "compaction"
    assert gate_name("found a candidate, but its resumable content is over the context "
                     "ceiling") == "ceiling"
    assert gate_name("no anchored transcript at all") == "no-anchor"
    assert gate_name("retired — a deliberate close, never reanimated") == "retired"
    assert gate_name("something nobody wrote yet") == "unknown"


async def test_wake_buckets_every_unnamed_mode_as_queued(
    actions: Actions, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rate brakes, pauses, and in-flight wakes are all genuinely "queued" — the catch-all
    default — and the raw mode/detail survive so nothing is lost to the bucket."""
    worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:sender", manager_agent="agent:abcd1234")

    async def _braked(*a: Any, **kw: Any) -> dict[str, Any]:
        return {"mode": "braked", "detail": "the per-seat rate brake: 3 wakes/h already landed"}

    monkeypatch.setattr(trigger_module, "dispatch_dm", _braked)
    d = await wake_worker(actions, caller="agent:sender", target=manager_seat, message="hey",
                          settings=_settings(enabled=True))
    assert d["status"] == "queued" and d["raw_mode"] == "braked"
    assert "rate brake" in d["detail"]


# ═══ launch() — THE BODY VERB (thread 9f566244 piece D; ruling 43b84c5e) ══════════════════════
# launch() is the CREATE twin of wake()'s speak: it summons a fresh claude into a managed seat's
# own office via the manager daemon's pty_spawn. These tests pin the authority gate (DOWNWARD-
# ONLY, unlike wake's either-direction knock), idempotency (never a twin), the honest body_exists-
# vs-can_receive receipt (Ra's requirement, 53ae1a87), and graceful failure on a dark daemon. The
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
    """Give a seat an anchor_cwd (its office) — _managed_pair leaves it unset."""
    oid = await actions.create_or_find_object("Seat", seat_id, "test")
    await actions.assert_property(oid, "anchor_cwd", cwd, "test", NOW, 0.9,
                                  evidence_class="self_declared")


async def test_launch_refuses_an_unseated_caller(actions: Actions) -> None:
    """No held seat, no birth — launch is a seat-to-seat act. Returns before any daemon touch."""
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
    """THE distinction from wake(): a worker may WAKE its manager (16722273) but may never
    LAUNCH it a body (78e3734e). The stored edge is worker→manager, so the worker does not
    MANAGE the manager — refused, nothing spawned."""
    _worker_seat, manager_seat = await _managed_pair(
        actions, worker_agent="agent:dw01", manager_agent="agent:dm01")
    record: list[dict[str, Any]] = []
    d = await trigger_module.launch_seat(
        actions, caller="agent:dw01", target=manager_seat,
        manager=_fake_manager(record), windows=_fake_windows([]))
    assert d["status"] == "refused-not-your-worker" and "DOWNWARD-ONLY" in d["detail"]
    assert record == []


async def test_launch_refuses_a_seat_with_no_office(actions: Actions) -> None:
    """A seat with no anchor_cwd has no room to be born in — refused, nothing spawned."""
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
    SEPARATELY (Ra, 53ae1a87). The manager op is a pty_spawn naming the seat, into its office,
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
    # THE ATTACH LINE (ruling 0fe36e59, thread c171a3de): office dir + session anchor + a
    # command that works TODAY, independent of where the spawn's cwd happens to register in
    # the harness's own per-project session list.
    assert d["attach"]["office"] == "/home/asuramaya/.osiris/seats/tefnut"
    assert d["attach"]["session_anchor"] == req["job_dir"]
    assert d["attach"]["command"] == 'python -m src.manager.attach "[OS] Tefnut"'


async def test_launch_hands_the_attach_line_on_the_idempotent_path_too(
    actions: Actions,
) -> None:
    """A caller who launches into an already-live seat still needs to reach it — the
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


async def test_launch_resolves_a_vacant_seat_by_handle(actions: Actions) -> None:
    """task #68 (finding b): a freshly minted, never-launched seat has NO Agent bound to it
    at all — the old Agent-centric resolve_seat fallback returns no seat_id for such a seat
    (it only walks Agent objects that claimed a handle), so launch(target=<handle>) could
    never body a seat mint_seat had just made. The worker here is deliberately vacant (no
    bind_holder call) and still resolves by its bare handle."""
    manager_seat = (await ensure_seat(actions, house="demo", handle="Manager",
                                      source="test"))["seat_id"]
    await bind_holder(actions, seat_id=manager_seat, agent_id="agent:vm01")
    worker_seat = (await ensure_seat(actions, house="demo", handle="Nefer",
                                     source="test"))["seat_id"]  # never bound — VACANT
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
    """task #68 (finding #7, thread 20e4feb6): the old precedence never read the seat's own
    stamped intended_model at all — only an explicit param or the trigger's global default —
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
    """When the post-spawn READ shows the window ALIVE, can_receive is true — the separate read
    is real, not a hard-coded false."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:cw01", manager_agent="agent:cm01",
        worker_handle="Nut", house="osiris")
    await _office(actions, worker_seat, "/tmp/nut")
    name = "[OS] Nut"
    calls = {"n": 0}

    async def _w() -> list[dict[str, Any]]:
        calls["n"] += 1
        if calls["n"] == 1:  # the idempotency check — nothing live yet
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
    """A live body already holds the seat → RETURN it, never spawn a twin (Ra's stale-liveness
    collision, b3a86a7d). The manager op is never called."""
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
    """A message rides the ordinary mail lane as a graded ask to the new seat — never a
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
        actions, caller="agent:bm01", target=worker_seat, message="Welcome — mount and orient.",
        substrate="pty",
        manager=_fake_manager(record, ret={"spawned": "[OS] Isis"}), windows=_fake_windows([]))
    assert d["status"] == "launched" and d.get("brief_message_id")
    row = await actions.pool.fetchrow(
        "SELECT body FROM fleet_messages WHERE id=$1", int(d["brief_message_id"]))
    assert row is not None and "mount and orient" in row["body"]


# ═══ THE DEFAULT FLIP — launch_seat's harness-native lane (task #68 wave, rulings 0fe36e59 +
# 33d6a2eb clause 3) ═══════════════════════════════════════════════════════════════════════════
# 'harness' is now launch_seat's DEFAULT substrate (no `substrate` argument needed); the PTY
# lane above only runs when a test (or a caller) asks for it by name. `spawn`/`agents_json`/
# `cost_reader` are injected fakes — never a real `claude` process or subprocess.


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
    """No `substrate` argument at all — the flip's whole point: launch_seat now bodies a
    seat as a `claude --bg` session by default, with an honest body_exists/can_receive split
    (Ra, 53ae1a87) exactly like the old PTY lane."""
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
    # THE BOOT PROMPT (live finding, 2026-07-27): identity rides the session's own first
    # turn, not env stamping — it must tell the session to mount at this exact office and
    # claim this exact handle, or a fresh launch mounts anonymous.
    assert "/tmp/sobek" in call["prompt"]
    assert 'claim_name("Sobek")' in call["prompt"]


async def test_launch_harness_lane_is_idempotent_returns_the_live_body_not_a_twin(
    actions: Actions,
) -> None:
    """A live `claude agents --json` row already sitting at this seat's own office cwd →
    RETURN it, never spawn a twin (the same one-body law as the PTY lane, b3a86a7d). Matched
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
    (`-p --resume`) body BY CONSTRUCTION — an EMPTY harness roster used to mean "safe to
    mint," even when a resumed session is genuinely live at this exact cwd, reachable only
    through agent_mounts (its own mid-turn mount() call lands there, never in the harness's
    `--bg`-only roster). The shared twin guard now reads BOTH and refuses on either,
    naming agent_mounts by name in the receipt so the refusal is never mistaken for the
    harness-roster case."""
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
    assert any("agent_mounts" in s for s in d["seen_via"])
    assert d["window"] is None  # nothing to name from the (empty) harness roster


async def test_launch_harness_lane_confesses_dormant_history_before_spawn(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Thread fc69b9b4 (Ooblek specimen): when launch_cwd already holds a substantial
    transcript, the receipt names it — {"path", "size_bytes", "last_touched"} — rather than
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
    """The post-spawn READ is real, not hard-coded false — when the fresh session already
    shows up in `claude agents --json` at the seat's own office cwd, can_receive reports it."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw03", manager_agent="agent:hm03",
        worker_handle="Bastet", house="osiris")
    await _office(actions, worker_seat, "/tmp/bastet")
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm03", target=worker_seat,
        spawn=_fake_spawn([]),
        agents_json=_fake_agents_json(
            [[], [{"cwd": "/tmp/bastet", "sessionId": "real-abc", "name": "[OS] Bastet"}]]))

    assert d["status"] == "launched"
    assert d["body_exists"] is True and d["can_receive"] is True
    # THE RECEIPT NAMES THE RESUME DECISION EVERY TIME (Thoth msg 3691) — no bare "body
    # created and live" anymore; a holder with no session on record says so, then "; live".
    assert d["detail"].startswith("booted fresh") and d["detail"].endswith("; live")
    assert d["resume_check"] == ["gen None: minted but never mounted, no session to check"]


async def test_launch_harness_lane_reports_refused_spawn_when_claude_bg_fails(
    actions: Actions,
) -> None:
    """A `claude --bg` that fails to start (OSError — e.g. no such binary) is an honest
    refusal, same taxonomy as the PTY lane's manager-cold/refused-spawn — never a false
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


async def test_launch_harness_lane_records_the_unpriced_cost_honestly(actions: Actions) -> None:
    """THE CEILING'S READ PATH (task #8): a --bg body is a real billed session, same as any
    other — its spend must land in llm_usage even when it is UNPRICED, or the ceiling never
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
            [[], [{"cwd": "/tmp/khepri", "sessionId": "khepri-sess", "name": "[OS] Khepri"}]]),
        cost_reader=_fake_cost_reader(
            {"priced": False, "reason": "claude agents --json carries no cost field"}))

    assert d["status"] == "launched"
    rows = await _launch_usage_rows(actions)
    assert len(rows) == 1
    assert rows[0]["purpose"] == "launch" and rows[0]["cost_usd"] is None


async def test_launch_harness_lane_skips_metering_when_not_yet_visible(actions: Actions) -> None:
    """A launch not yet showing up in `claude agents --json` has no REAL session id to look
    up cost for — it is simply not metered THIS cycle, never metered on a guessed id (the
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
            [[], [{"cwd": "/tmp/wadjet", "sessionId": "wadjet-sess", "name": "[OS] Wadjet"}]]),
        cost_reader=_fake_cost_reader({"priced": True, "cost_usd": 0.17}))

    assert d["status"] == "launched"
    rows = await _launch_usage_rows(actions)
    assert len(rows) == 1
    assert rows[0]["cost_usd"] == pytest.approx(0.17)


async def test_launch_harness_lane_delivers_the_opening_brief_over_the_mail_lane(
    actions: Actions,
) -> None:
    """Same law as the PTY lane: the message rides the ordinary mail lane as a graded ask,
    never a hand-forged turn — substrate must not change that contract."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw07", manager_agent="agent:hm07",
        worker_handle="Serqet", house="osiris")
    await _office(actions, worker_seat, "/tmp/serqet")
    manc = await actions.create_or_find_object("Agent", "agent:hm07", "test")
    await actions.assert_property(manc, "project", "osiris", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm07", target=worker_seat, message="Welcome — mount and orient.",
        spawn=_fake_spawn([]), agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched" and d.get("brief_message_id")
    row = await actions.pool.fetchrow(
        "SELECT body FROM fleet_messages WHERE id=$1", int(d["brief_message_id"]))
    assert row is not None and "mount and orient" in row["body"]


# ═══ THE RESUME LANE (operator's own order, 2026-08-04, decision a829a15d + msg 3639,
# reversing 315c3181's "not built, deliberately") — launch_seat gains dispatch_dm's own
# resume branch, REUSED via `_resume_guard`/`_agent_resumable`/`_DM_RESUME_PROMPT`, never
# reimplemented. ONE-SHOT: the resumed body runs its turn and exits — re-summonable via the
# next mail wake, not a standing window (315c3181's own unresolved tradeoff, now settled).
#
# LIVE-FIRE CORRECTION (Thoth msg 3691, 2026-08-04, Sekhmet): the lookup moved from
# agent_mounts (wakeable_identity/_agent_resumable, dispatch_dm's own DM-lane shape) to a
# succession_chain WALK (_lineage_resume_candidate) — a `--bg`-launched seat's every
# generation shares ONE durable per-seat mount anchor, so agent_mounts can never encode a
# real per-generation session id; only the graph's own `session` property assertion
# survives. The fixtures below assert `session` directly (succession_chain's own shape),
# never `mounts.save_mount` — the old agent_mounts-only setup silently stopped exercising
# the resume path at all once this fix landed (it still "passed" by falling through to
# mint, for the wrong reason — caught rewriting these tests, not left in).


async def _lineage_holder_with_session(
    actions: Actions, tmp_path: Path, *, agent_id: str, transcript_bytes: int = 16,
    compacted: bool = False,
) -> Path:
    """A seat holder whose resumable session lives ONLY as a graph `session` property
    (succession_chain's own shape) plus a real transcript on disk anchored to that same
    session id — NOT an agent_mounts row, which is exactly the record this fixture proves
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
    """Same shape as `_lineage_holder_with_session` — a real, uncompacted transcript the
    seat's own `session` property points at — but with NO signed testimony anywhere in
    it: thread ef88e2bb's own specimen, the `resident-unknown` class (an absence of
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
    property — the ONLY record that survives the shared-anchor collapse, see this
    section's own header comment) is CONTINUED via its own `-p --resume`, never minted
    fresh — mode='resumed' in the receipt, the shared `_DM_RESUME_PROMPT` (never a
    launch-specific copy), and the spawn call carries resume_session, never job_dir (a
    resume is not a birth). The resumed body's own repo is the SEAT's own launch_cwd —
    deliberately, not a per-generation agent_mounts.cwd, which is exactly the record this
    lookup no longer trusts (see _lineage_resume_candidate's own docstring)."""
    sense = await _lineage_holder_with_session(
        actions, tmp_path, agent_id="agent:abcd1234")
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:hm-resume",
        worker_handle="Stale-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/stale-test-office")
    manc = await actions.create_or_find_object("Agent", "agent:hm-resume", "test")
    await actions.assert_property(manc, "project", "osiris", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    resumed: list[dict[str, Any]] = []

    async def _resume_spawn(repo: str, prompt: str, **kw: Any) -> None:
        resumed.append({"repo": repo, "prompt": prompt, **kw})

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a resumable holder must never be minted fresh")

    d = await trigger_module.launch_seat(
        actions, caller="agent:hm-resume", target=worker_seat,
        message="pick up where you left off", substrate="harness",
        settings=_settings(enabled=True, sense=str(sense)),
        spawn=_boom, resume_spawn=_resume_spawn, agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched" and d["mode"] == "resumed"
    assert d["session"] == FULL_SID
    assert d["body_exists"] is True and d["can_receive"] is True
    assert d.get("brief_message_id")
    # THE RECEIPT NAMES THE DECISION (Thoth msg 3691): which generation, how far back.
    assert d["resume_check"] == [f"gen 1 (session {FULL_SID[:8]}, 0MB): resumable, 0 hop(s) back"]
    assert "gen 1" in d["detail"] and "1 generation(s) back" in d["detail"]
    assert len(resumed) == 1
    call = resumed[0]
    assert call["repo"] == "/tmp/stale-test-office"  # the SEAT's own launch_cwd
    assert call.get("resume_session") == FULL_SID
    assert "job_dir" not in call  # a resume is not a birth (mirrors dispatch_dm's own call)
    assert "private" in call["prompt"] and "seat" in call["prompt"]  # _DM_RESUME_PROMPT itself
    # THE ORDERING GUARANTEE: the brief landed in mail BEFORE the spawn was even issued —
    # a one-shot resumed body's first turn IS its inbox() check, no boot lag to hide behind.
    row = await actions.pool.fetchrow(
        "SELECT body FROM fleet_messages WHERE id=$1", int(d["brief_message_id"]))
    assert row is not None and "pick up where you left off" in row["body"]


# ═══ THE ZERO-HOP GRAPH DOOR (#173a, the ferryman incident 2026-08-18 00:41Z) ════════════
# `osiris launch` found d8727352 (gen 8, 0 hops, resumable) and the resident-unknown guard
# refused it: no signed osiris act had been written into that transcript yet, so the
# testimony arm honestly answered "unknown" — an absence of evidence, not a stranger. The
# operator resumed it by hand anyway, correctly. `_zero_hop_graph_corroborates` is the
# named, narrow door that now lets exactly this shape through — never for an ancestor hop,
# never when the repo lands anywhere but the seat's own launch location.

async def test_launch_harness_lane_resumes_a_zero_hop_candidate_with_no_signed_testimony(
    actions: Actions, tmp_path: Path,
) -> None:
    """The exact ferryman shape: a fresh, anchored, resumable transcript for the seat's
    CURRENT holder (0 hops back) that never wrote a single signed osiris act — the
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
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:ferry1234", manager_agent="agent:hm-ferry",
        worker_handle="Ferryman-Test", house="osiris")
    await _office(actions, worker_seat, "/repo/ferryman")
    manc = await actions.create_or_find_object("Agent", "agent:hm-ferry", "test")
    await actions.assert_property(manc, "project", "osiris", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    resumed: list[dict[str, Any]] = []

    async def _resume_spawn(repo: str, prompt: str, **kw: Any) -> None:
        resumed.append({"repo": repo, "prompt": prompt, **kw})

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a graph-corroborated zero-hop candidate must never be minted")

    d = await trigger_module.launch_seat(
        actions, caller="agent:hm-ferry", target=worker_seat,
        message="pick it back up", substrate="harness",
        settings=_settings(enabled=True, sense=str(sense)),
        spawn=_boom, resume_spawn=_resume_spawn, agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched" and d["mode"] == "resumed"
    assert d["session"] == FULL_SID
    assert len(resumed) == 1 and resumed[0]["repo"] == "/repo/ferryman"
    assert resumed[0].get("resume_session") == FULL_SID


async def test_zero_hop_graph_door_never_fires_one_hop_back(
    actions: Actions, tmp_path: Path,
) -> None:
    """The door is named and narrow: an otherwise-identical unsigned, resumable transcript
    ONE hop back (the predecessor's session, not the current holder's own) must still
    refuse resident-unknown — the graph door only ever opens for hop 0. Composed with
    ef88e2bb (954c591/9421c81): a resident-unknown refusal now REFUSES THE WHOLE LAUNCH
    rather than falling through to a fresh mint — this is the exact case that fix exists
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
        evidence_class="self_declared")  # minted, never mounted — no session of its own
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:ferry2-ii", manager_agent="agent:hm-ferry2",
        worker_handle="Ferryman2-Test", house="osiris")
    await _office(actions, worker_seat, "/repo/ferryman2")
    manc = await actions.create_or_find_object("Agent", "agent:hm-ferry2", "test")
    await actions.assert_property(manc, "project", "osiris", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    async def _boom_resume(*a: Any, **kw: Any) -> None:
        raise AssertionError("one hop back must never clear the graph door")

    async def _boom_spawn(*a: Any, **kw: Any) -> None:
        raise AssertionError("resident-unknown must refuse the whole launch (ef88e2bb), "
                             "never fall through to a fresh mint")

    d = await trigger_module.launch_seat(
        actions, caller="agent:hm-ferry2", target=worker_seat, substrate="harness",
        settings=_settings(enabled=True, sense=str(sense)),
        spawn=_boom_spawn, resume_spawn=_boom_resume,
        agents_json=_fake_agents_json([[]]))

    assert d["status"] == "refused-resume-unknown"
    assert d["session"] == FULL_SID
    assert d["body_exists"] is False and d["can_receive"] is False


async def test_launch_harness_lane_walks_past_a_zero_turn_generation(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE LIVE-FIRE CASE ITSELF (Thoth msg 3691, Sekhmet): the seat's CURRENT holder is a
    generation minted at a compaction seam whose body never ran (no `session` asserted at
    all) — the walk must go ONE HOP BACK to the predecessor's own resumable session,
    exactly as succession_chain's own generation-2 test fixture shapes it, rather than
    reporting the whole seat unresumable the instant the newest generation turns out
    stillborn."""
    # NAMING NOTE: "agent:seat-zt" (no suffix) is generation 1 by _generation()'s own
    # convention (agent:x = gen 1; agent:x-ii = gen 2) — a "-i" suffix on the ROOT would
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
        evidence_class="self_declared")  # minted, never mounted — no seat_generation/session
    await _office(actions, worker_seat, "/tmp/zeroturn-test")
    spawned: list[dict[str, Any]] = []
    resumed: list[dict[str, Any]] = []

    async def _resume_spawn(repo: str, prompt: str, **kw: Any) -> None:
        resumed.append({"repo": repo, "prompt": prompt, **kw})

    d = await trigger_module.launch_seat(
        actions, caller="agent:hm-zt", target=worker_seat, substrate="harness",
        settings=_settings(enabled=True, sense=str(sense)),
        spawn=_fake_spawn(spawned), resume_spawn=_resume_spawn,
        agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched" and d["mode"] == "resumed"
    assert d["session"] == FULL_SID
    assert spawned == []  # never minted fresh
    assert len(resumed) == 1 and resumed[0].get("resume_session") == FULL_SID
    assert d["resume_check"][0] == "gen None: minted but never mounted, no session to check"
    assert "resumable, 1 hop(s) back" in d["resume_check"][1]


async def test_launch_harness_lane_falls_through_to_mint_when_nothing_is_resumable(
    actions: Actions,
) -> None:
    """The ordinary case (no prior mount at all) must still mint fresh exactly as before this
    lane existed — the resume check is a first look, never a hard gate that could strand a
    launch when nothing is there to continue."""
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:hw-fresh", manager_agent="agent:hm-fresh",
        worker_handle="Fresh-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/fresh-test")
    spawned: list[dict[str, Any]] = []

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("nothing resumable exists — resume_spawn must never be called")

    d = await trigger_module.launch_seat(
        actions, caller="agent:hm-fresh", target=worker_seat, substrate="harness",
        settings=_settings(enabled=True, sense=""),
        spawn=_fake_spawn(spawned), resume_spawn=_boom, agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched" and "mode" not in d
    assert len(spawned) == 1


async def test_launch_harness_lane_resumes_zero_hop_unsigned_via_the_graph_door_not_a_refusal(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE COMPOSITION OF ef88e2bb (954c591) AND #173a (0db0659): a resumable session with
    NO signed testimony, for the seat's OWN CURRENT holder (hop 0, no predecessor at all)
    — the exact shape this test used to expect a hard refusal for, back when ef88e2bb
    shipped alone. Once #173a's zero-hop graph door lands beside it, THIS fixture is
    precisely the case that door exists to let through: `_zero_hop_graph_corroborates`
    clears the gate before ef88e2bb's `resident-unknown` refusal branch is ever reached,
    so the resume proceeds. `test_zero_hop_graph_door_never_fires_one_hop_back` is the
    sibling proof that a NON-zero-hop resident-unknown case still hits ef88e2bb's refusal
    (refused-resume-unknown, nothing spawned) — the two tests together are the real
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

    async def _boom_spawn(*a: Any, **kw: Any) -> None:
        raise AssertionError("a zero-hop graph-corroborated candidate must never be minted")

    d = await trigger_module.launch_seat(
        actions, caller="agent:hm-unk", target=worker_seat, substrate="harness",
        settings=_settings(enabled=True, sense=str(sense)),
        spawn=_boom_spawn, resume_spawn=_resume_spawn, agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched" and d["mode"] == "resumed"
    assert d["session"] == FULL_SID
    assert len(resumed) == 1 and resumed[0].get("resume_session") == FULL_SID


async def test_launch_harness_lane_never_resumes_a_tail_closed_at_the_seam(
    actions: Actions, tmp_path: Path,
) -> None:
    """#156's rebuild (2026-08-09, the operator's own correction): the floor still refuses
    a transcript whose tail since its last compaction is genuinely tiny — the seam-itself
    case, the operator's own "rare special case" — falling through to mint fresh exactly
    like the no-history case. The receipt still NAMES the refusal with real numbers, not a
    silent fall-through. (Compacting once and then doing real work is now RESUMED, not
    refused — see the sibling test right after this one; the fixture here deliberately
    leaves only 16 raw bytes after the boundary, well under the floor set below.)"""
    sense = await _lineage_holder_with_session(
        actions, tmp_path, agent_id="agent:abcd1234", compacted=True)
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:abcd1234", manager_agent="agent:hm-compact",
        worker_handle="Compacted-Test", house="osiris")
    await _office(actions, worker_seat, "/tmp/compacted-test")
    spawned: list[dict[str, Any]] = []

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a tail closed at the seam itself must never be resumed")

    d = await trigger_module.launch_seat(
        actions, caller="agent:hm-compact", target=worker_seat, substrate="harness",
        settings=_settings(enabled=True, sense=str(sense), min_tail_bytes=1000),
        spawn=_fake_spawn(spawned), resume_spawn=_boom, agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched" and "mode" not in d
    assert len(spawned) == 1
    # THE REFUSAL IS NAMED, NOT SILENT: the receipt carries which generation, its size, and
    # the specific gate that fired — a human reads one line instead of re-deriving it.
    assert len(d["resume_check"]) == 1
    reason = d["resume_check"][0]
    assert "gen 1" in reason and f"session {FULL_SID[:8]}" in reason
    assert "seam itself" in reason
    assert "min_tail_bytes=1000" in d["detail"]


async def test_launch_harness_lane_resumes_a_compacted_transcript_with_real_tail_work(
    actions: Actions, tmp_path: Path,
) -> None:
    """The sibling and the whole point of #156's rebuild: a holder whose transcript
    compacted once but carries real work since that boundary IS resumed, not minted fresh
    — sekhmet's own live specimen (12 compactions, 4.07MB of real work after the last
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

    async def _boom(*a: Any, **kw: Any) -> None:
        raise AssertionError("a tail with real post-compaction work must never mint fresh")

    d = await trigger_module.launch_seat(
        actions, caller="agent:hm-compact-2", target=worker_seat, substrate="harness",
        settings=_settings(enabled=True, sense=str(sense), min_tail_bytes=1),
        spawn=_boom, resume_spawn=_resume_spawn, agents_json=_fake_agents_json([[]]))

    assert d["status"] == "launched" and d.get("mode") == "resumed"
    assert resumed and resumed[0].get("resume_session") == FULL_SID


# ═══ tree_cwd (task #103's re-scope, ff3bdc37, Thoth DM 2794) — the office/code split. ═══

async def test_launch_refuses_a_tree_cwd_that_does_not_exist_on_disk(
    actions: Actions, tmp_path: Path,
) -> None:
    """OSIRIS NEVER PROVISIONS THE TREE (ff3bdc37: harness owns isolation) — a seat naming a
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
    """The office (identity) and the tree (code) are DISTINCT — a bound, real tree_cwd is
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
    # the boot prompt still anchors mount() AT THE OFFICE — identity never follows the tree
    assert str(office) in spawned[0]["prompt"]
    assert str(tree) not in spawned[0]["prompt"]


async def test_launch_harness_lane_idempotency_matches_on_tree_cwd_not_office(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE CORRECTNESS PROOF: a tree-bound seat's live process sits at tree_cwd. Matching
    idempotency on `office` alone (the pre-fix shape) would never find it and would twin on
    every relaunch — this proves the fix reads the actual launch location, not the office."""
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


# ═══ vacate_dead_seat (thread 445a7356, Thoth's ruling msg 1611) — the evidence-gathering
# complement to seats.vacate_holder / seats.retire_seat's stale-holder refusal, never its
# bypass. `agents_json`/`transcript_activity` are injected so tests assert the DECISION
# without a real `claude` binary or real transcript files.

def _fake_transcript_activity(checked: bool, fresh: bool) -> Any:
    async def _t(pool: Any, holder: str, st: Any) -> tuple[bool, bool]:
        return checked, fresh
    return _t


async def test_vacate_dead_seat_refuses_a_vacant_seat(actions: Actions) -> None:
    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:vd1holdr", manager_agent="agent:vd1mgr0",
        worker_handle="Ptah-Vacant", house="osiris")
    await _office(actions, worker_seat, "/tmp/ptah-vacant")
    # unbind: no holder at all — the fixture above binds one, so start from a fresh seat
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


async def test_vacate_dead_seat_refuses_when_the_roster_shows_a_live_session(
    actions: Actions,
) -> None:
    """Signal 1 alone showing life is enough to refuse — the transcript check never even
    runs (the fake would raise if called, proving short-circuit)."""
    from src.orchestrator import trigger as tm

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:vd3holdr", manager_agent="agent:vd3mgr0",
        worker_handle="Sekhmet-Alive", house="osiris")
    await _office(actions, worker_seat, "/tmp/sekhmet-alive")

    async def _boom(pool: Any, holder: str, st: Any) -> tuple[bool, bool]:
        raise AssertionError("transcript check must not run when the roster shows life")

    d = await tm.vacate_dead_seat(
        actions, seat_id=worker_seat, actor="test", because="dead",
        agents_json=_fake_agents_json(
            [[{"cwd": "/tmp/sekhmet-alive", "name": "[OS] Sekhmet-Alive"}]]),
        transcript_activity=_boom)
    assert d["status"] == "refused-live"
    assert "Sekhmet-Alive" in d["detail"]


async def test_vacate_dead_seat_refuses_when_the_transcript_is_fresh(
    actions: Actions,
) -> None:
    """Signal 1 (roster) is silent, but signal 2 (the transcript's own timestamped
    content) disagrees — refused, the Aegis-phantom case (mtime alone would have lied
    the other way)."""
    from src.orchestrator import trigger as tm

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:vd4holdr", manager_agent="agent:vd4mgr0",
        worker_handle="Bastet-Working", house="osiris")
    await _office(actions, worker_seat, "/tmp/bastet-working")

    d = await tm.vacate_dead_seat(
        actions, seat_id=worker_seat, actor="test", because="dead",
        agents_json=_fake_agents_json([[]]),
        transcript_activity=_fake_transcript_activity(checked=True, fresh=True))
    assert d["status"] == "refused-live"
    assert "agent:vd4holdr" in d["detail"]


async def test_vacate_dead_seat_refuses_ambiguous_on_an_unreadable_roster(
    actions: Actions,
) -> None:
    from src.orchestrator import trigger as tm

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:vd5holdr", manager_agent="agent:vd5mgr0",
        worker_handle="Nut-Unreadable", house="osiris")
    await _office(actions, worker_seat, "/tmp/nut-unreadable")

    async def _boom(*, cwd: str | None = None,
                    include_completed: bool = False) -> list[dict[str, Any]]:
        raise OSError("no such file or directory: claude")

    d = await tm.vacate_dead_seat(actions, seat_id=worker_seat, actor="test",
                                  because="dead", agents_json=_boom)
    assert d["status"] == "refused-ambiguous"


async def test_vacate_dead_seat_vacates_when_both_signals_confirm_death(
    actions: Actions,
) -> None:
    """The core: no live roster entry AND a stale (or absent) transcript → vacated —
    proof this reaches seats.vacate_holder's own write, not just a receipt shape."""
    from src.orchestrator import trigger as tm

    worker_seat, _manager_seat = await _managed_pair(
        actions, worker_agent="agent:vd6corps", manager_agent="agent:vd6mgr0",
        worker_handle="Khepri-Dead", house="osiris")
    await _office(actions, worker_seat, "/tmp/khepri-dead")

    d = await tm.vacate_dead_seat(
        actions, seat_id=worker_seat, actor="test", because="process confirmed dead",
        agents_json=_fake_agents_json([[]]),
        transcript_activity=_fake_transcript_activity(checked=False, fresh=False))
    assert d["status"] == "vacated"
    assert d["was_held_by"] == ["agent:vd6corps"]
    assert d["evidence"] == {"roster_checked": True, "transcript_checked": False}
    holder = await actions.pool.fetchval(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", worker_seat)
    assert holder is None


# ═══ THE HARNESS-NATIVE SUBSTRATE (task #68 item 9, ruling 33d6a2eb; spike f2dc98549521) ══════
# `claude --bg` + `claude agents --json` instead of the manager daemon's PTY broker. Same
# hermetic discipline as _spawn_claude's own tests: `trigger.asyncio.create_subprocess_exec` is
# monkeypatched, never a real `claude` process.


async def test_spawn_claude_bg_issues_the_documented_bg_flags(monkeypatch: Any) -> None:
    """`--bg` + `-n` + `--model` + a trailing prompt — the sanctioned flags the spike
    verified, never the undocumented daemon claim-socket, and NEVER `--session-id` (live
    finding, 2026-07-27: `--bg` manages its own session id and silently ignores an explicit
    one — see _spawn_claude_bg's own docstring). Fire-and-forget: NOTHING here awaits the
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
    await trigger._spawn_claude_bg(
        "/home/asuramaya/.osiris/seats/nefer", name="[OS] Nefer",
        model="claude-sonnet-5", prompt="mount and claim_name")

    assert captured["args"][:2] == ("claude", "--bg")
    pairs = _pairs(captured["args"])
    assert ("-n", "[OS] Nefer") in pairs
    assert ("--model", "claude-sonnet-5") in pairs
    assert not any(a == "--session-id" for a in captured["args"])
    assert captured["args"][-1] == "mount and claim_name"  # the trailing positional prompt


async def test_spawn_claude_bg_starts_its_own_process_group(monkeypatch: Any) -> None:
    """#156 (Thoth's own ruling, independent of the kill verb — this is already a bug
    today): without its own session/group, a body's Bash-tool children share OSIRIS'S OWN
    process group and can outlive their parent with nobody owning them — the exact shape
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
    await trigger._spawn_claude_bg("/repo/demo")
    assert captured["kwargs"].get("start_new_session") is True


async def test_spawn_claude_bg_never_leaks_the_spawners_own_anchor(monkeypatch: Any) -> None:
    """Same anchor discipline as _spawn_claude (the collision class, 2294e95d): the spawner's
    own CLAUDE_JOB_DIR must never reach the child — inert for --bg today (no env var reaches
    a claimed spare either way, live finding 2026-07-27) but cheap and harmless to scrub."""
    from src.orchestrator import trigger

    captured: dict[str, Any] = {}

    class _Proc:
        pid = 1

    async def _fake_exec(*args: Any, **kwargs: Any) -> _Proc:
        captured["env"] = kwargs.get("env")
        return _Proc()

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _fake_exec)
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
    await trigger._spawn_claude_bg("/repo/demo", name="bare")
    assert captured["args"] == ("claude", "--bg", "-n", "bare")


async def test_claude_agents_json_parses_a_real_shaped_sample(monkeypatch: Any) -> None:
    """The exact shape sampled live from a running fleet (2026-07-27) — background AND
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
    _manager_windows) — a dark/missing `claude` binary answers [], not an exception."""
    from src.orchestrator import trigger

    async def _boom(*args: Any, **kwargs: Any) -> Any:
        raise OSError("no such file or directory: claude")

    monkeypatch.setattr(trigger.asyncio, "create_subprocess_exec", _boom)
    assert await trigger._claude_agents_json() == []


async def test_bg_session_cost_is_honestly_unpriced_not_fabricated(monkeypatch: Any) -> None:
    """The spike's own open question: `claude agents --json` carries no cost field at all
    (confirmed live against the real fleet) — a --bg session's spend must be reported as
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
