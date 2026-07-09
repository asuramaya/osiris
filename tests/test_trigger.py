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

from src.actions.core import Actions
from src.orchestrator.mailbox import OPERATOR_ADDR, read_inbox, send_message
from src.orchestrator.trigger import _WAKE_PROMPT, should_wake, trigger_mail_tick, wake_status

NOW = datetime(2026, 7, 6, tzinfo=UTC)


def _settings(*, enabled: bool, rate_cap: int = 5, window: int = 3600,
              lease: int = 900, grace: int = 0, live: int = 900,
              ceiling: int = 8_000_000, sense: str = "",
              wake_model: str = "") -> SimpleNamespace:
    # grace defaults to 0 (disabled) so the rate-cap / lease tests exercise those bounds in
    # isolation; the wake-grace tests set it explicitly. sense="" → resume resolution looks at
    # ~/.claude/projects (no anchored transcript for the test ids there → mint), so the legacy
    # mint-path tests stay exactly as they were.
    return SimpleNamespace(osiris_trigger_enabled=enabled, osiris_trigger_rate_cap=rate_cap,
                           osiris_trigger_window_secs=window, osiris_mail_lease_secs=lease,
                           osiris_trigger_grace_secs=grace, osiris_owner_live_secs=live,
                           osiris_resume_ceiling_bytes=ceiling, osiris_sense_sessions=sense,
                           osiris_wake_model=wake_model)


def test_should_wake_is_off_by_default_and_rate_capped() -> None:
    assert should_wake(enabled=False, recent_wakes=0, rate_cap=5) == "disabled"    # kill switch
    assert should_wake(enabled=True, recent_wakes=5, rate_cap=5) == "rate-capped"  # the bound
    assert should_wake(enabled=True, recent_wakes=0, rate_cap=5) is None           # → WAKE


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

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=False), spawn=_spawn)
    assert spawned == [] and rep["woke"] == 0  # OFF by default — nothing woken


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
    """Obligation e1ed13fb part 1: a triggered `claude -p` gets no CLAUDE_JOB_DIR from any
    harness, so the woken agent used to mount by guessing its identity off a co-tenant's
    transcript. The trigger now synthesizes a durable, deterministic per-wake anchor with a
    'jobs/wake-<row id>' shape _job_id parses — so the agent mounts as a stable agent:wake-<id>."""
    from src.ingest.sessions import _job_id

    await _agent_with_mail(actions)
    captured: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        captured.append(kw["job_dir"])

    rep = await trigger_mail_tick(actions, settings=_settings(enabled=True), spawn=_spawn)
    assert rep["woke"] == 1 and len(captured) == 1
    jd = captured[0]
    wake_id = await actions.pool.fetchval("SELECT id FROM agent_wakes WHERE to_project='demo'")
    assert jd.endswith(f"jobs/wake-{wake_id}")     # the token is the ledger row id (deterministic)
    assert _job_id(jd) == f"wake-{wake_id}"        # the parser resolves it to the session handle
    assert Path(jd).is_dir()                       # a REAL created dir, not just a string


async def test_spawn_claude_injects_claude_job_dir_into_child_env(monkeypatch: Any) -> None:
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
    await trigger._spawn_claude("/repo/demo", "wake up", job_dir="/tmp/x/jobs/wake-7")
    assert captured["args"][:3] == ("claude", "-p", "wake up")
    assert captured["env"]["CLAUDE_JOB_DIR"] == "/tmp/x/jobs/wake-7"
    assert "PATH" in captured["env"]  # inherited the parent environment, not a bare dict

# --- the dispatch order: DELIVER → RESUME → MINT (thread 9f2ddb44) ---

FULL_SID = "abcd1234-0000-4000-8000-000000000000"


async def _stale_resumable_owner(actions: Actions, tmp_path: Path,
                                 transcript_bytes: int = 16) -> Path:
    """An owner for project demo: a durable mount (made STALE so it isn't 'live') whose job_dir
    anchors a real transcript under the sense root. Returns the sense root."""
    from src.orchestrator import mounts

    job = tmp_path / "jobs" / "abcd1234"
    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    (proj / f"{FULL_SID}.jsonl").write_bytes(b"x" * transcript_bytes)
    await mounts.save_mount(actions.pool, job_dir=str(job), agent_id="agent:abcd1234",
                            project="demo", cwd="/repo/demo", model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour'")
    return sense


async def test_live_owner_gets_delivery_not_a_twin(actions: Actions, tmp_path: Path) -> None:
    """An awake owner (fresh mount) means DELIVER: the mail sits in its box, nothing spawns —
    waking a twin beside a live owner is the fragmentation heinrich reported."""
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


async def test_a_live_addressee_is_not_rewoken(actions: Actions, tmp_path: Path) -> None:
    """The addressee's own tab is awake — its chrome/stop-hook surface the DM; a wake beside
    it would be noise."""
    sense = await _stale_resumable_owner(actions, tmp_path)
    await actions.pool.execute("UPDATE agent_mounts SET last_seen = now()")  # awake NOW
    await _dm_to_owner(actions)
    spawned: list[str] = []

    async def _spawn(repo: str, prompt: str, **kw: Any) -> None:
        spawned.append(repo)

    rep = await trigger_mail_tick(
        actions, settings=_settings(enabled=True, sense=str(sense)), spawn=_spawn)
    assert spawned == [] and rep["owner_live"] == 1


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
