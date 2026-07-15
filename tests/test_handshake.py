"""The whisper's server half — automount: every session wakes up already mounted.

Drives the same tested mount path the tool uses, so these tests focus on what the whisper
ADDS: the derived job_dir anchor (durable + resolved, visible to the liveness probe), the
payload the hook prints (mail/desk/away), and idempotence on hook re-fire.
"""
from __future__ import annotations

import json
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.handshake import automount
from src.orchestrator.mailbox import send_message

SID = "39fb22a2-0000-4000-8000-000000000000"


def _transcript(root: Path, cwd: str, model: str = "claude-fable-5") -> None:
    proj = root / cwd.replace("/", "-")
    proj.mkdir(parents=True, exist_ok=True)
    (proj / f"{SID}.jsonl").write_text(json.dumps(
        {"type": "assistant", "cwd": cwd,
         "message": {"model": model, "content": [{"type": "text", "text": "hi"}]}}) + "\n")


async def test_automount_is_a_durable_anchored_mount(actions: Actions, tmp_path: Path) -> None:
    root = tmp_path / "projects"
    _transcript(root, "/w/sibling-eight")
    await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                       to_project="sibling-eight", body="mail waiting at birth")

    out = await automount(actions, session_id=SID, cwd="/w/sibling-eight",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")

    assert out["agent"] == "agent:39fb22a2"       # anchored on the derived job id
    assert out["resolved"] is True                # never the cwd-guess
    assert out["model"] == "claude-fable-5"       # read off its OWN transcript
    assert out["mail"] == 1                       # the whisper says so at birth
    # the DURABLE half: registered in agent_mounts → the liveness probe sees the tab,
    # mail takes the deliver lane, a bounce re-attaches
    row = await actions.pool.fetchrow(
        "SELECT agent_id, project FROM agent_mounts WHERE agent_id='agent:39fb22a2'")
    assert row is not None and row["project"] == "sibling-eight"
    # hook re-fire (session resume) is idempotent — same identity, no dup Agent
    again = await automount(actions, session_id=SID, cwd="/w/sibling-eight",
                            actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    assert again["agent"] == out["agent"]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical='agent:39fb22a2'") == 1


async def test_whisper_hands_back_the_durable_anchor_that_prevents_the_twin(
    actions: Actions, tmp_path: Path
) -> None:
    """The reconnect-twin fix (thread 883a24f4): the whisper returns the derived job_dir, and a
    later mount carrying THAT anchor re-attaches to the same identity instead of minting a twin —
    even though $CLAUDE_JOB_DIR is empty. Co-located sessions stay distinct (own session → own
    anchor), so sibling-three cloud+engine on one dir never collide."""
    from src.orchestrator.agents import register_agent, resolve_identity

    root = tmp_path / "projects"
    _transcript(root, "/w/sibling-three")
    out = await automount(actions, session_id=SID, cwd="/w/sibling-three",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    anchor = out["job_dir"]
    assert anchor and anchor.endswith(SID[:8])            # the real derived anchor, handed back
    assert out["agent"] == "agent:39fb22a2"

    # a RECONNECT re-mount carrying the whisper's anchor (not $CLAUDE_JOB_DIR) re-attaches
    reident = resolve_identity(cwd="/w/sibling-three", job_dir=anchor, root=root)
    await register_agent(actions, reident, actor="analyst:operator")
    assert reident.agent_id == out["agent"]               # SAME identity — no twin
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical='agent:39fb22a2'") == 1

    # a DIFFERENT co-located session (engine on the same dir) derives its OWN distinct anchor
    other_sid = "beef1234-0000-4000-8000-000000000000"
    (root / "-w-sibling-three" / f"{other_sid}.jsonl").write_text(
        __import__("json").dumps({"type": "assistant", "cwd": "/w/sibling-three",
                                  "message": {"model": "claude-fable-5",
                                              "content": [{"type": "text", "text": "hi"}]}}) + "\n")
    out2 = await automount(actions, session_id=other_sid, cwd="/w/sibling-three",
                           actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    assert out2["agent"] == "agent:beef1234" and out2["job_dir"] != anchor  # distinct, no collision


async def test_compaction_mints_the_next_mind(actions: Actions, tmp_path: Path) -> None:
    """Ruling a882b334, fork 2 (operator overruled the same-weights argument): a compaction is
    a DEATH — the weights survive but the memory the operator was talking to does not. The
    SessionStart source='compact' whisper mints the lineage's next generation, the seat passes,
    and the durable row follows the heir."""
    from src.orchestrator.agents import claim_name

    root = tmp_path / "projects"
    _transcript(root, "/w/osiris")
    out = await automount(actions, session_id=SID, cwd="/w/osiris", actor="analyst:operator",
                          root=root, jobs_home=tmp_path / "jobs", source="startup")
    assert out["agent"] == "agent:39fb22a2"
    await claim_name(actions, "agent:39fb22a2", "Thoth", source="agent:39fb22a2")

    # the harness compacts the session — same sid, same transcript, same model: still a death
    reborn = await automount(actions, session_id=SID, cwd="/w/osiris", actor="analyst:operator",
                             root=root, jobs_home=tmp_path / "jobs", source="compact")
    assert reborn["agent"] == "agent:39fb22a2-ii"
    assert reborn["minted"] == "agent:39fb22a2"       # the whisper confesses the succession
    assert reborn["seat"] == "Thoth II"               # the seat passed, the numeral ticked
    assert await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='agent:39fb22a2-ii' AND a.name='minted_because'") == "compaction"
    # the durable row moved with the seat — per-render reads resolve to the new mind
    assert await actions.pool.fetchval(
        "SELECT agent_id FROM agent_mounts WHERE job_dir LIKE '%/jobs/' || $1",
        SID[:8]) == "agent:39fb22a2-ii"
    # a SECOND compaction is a second death — the numeral keeps counting
    third = await automount(actions, session_id=SID, cwd="/w/osiris", actor="analyst:operator",
                            root=root, jobs_home=tmp_path / "jobs", source="compact")
    assert third["agent"] == "agent:39fb22a2-iii" and third["seat"] == "Thoth III"
    # ...but a plain resume between deaths is NOT one — same mind, same numeral
    resumed = await automount(actions, session_id=SID, cwd="/w/osiris", actor="analyst:operator",
                              root=root, jobs_home=tmp_path / "jobs", source="resume")
    assert resumed["agent"] == "agent:39fb22a2-iii"


async def test_the_whisper_honors_a_bound_seat(actions: Actions, tmp_path: Path) -> None:
    """Soundwave V's complaint (thread 33838160): a new tab deliberately wearing a seat (its
    session row bound to a foreign lineage) must NEVER be re-asserted as its session hash by
    the whisper — and a compaction seam mints on the SEAT's lineage, not a phantom twin's."""
    from src.orchestrator import mounts
    from src.orchestrator.agents import claim_name

    root = tmp_path / "projects"
    _transcript(root, "/w/sibling-two")
    # the seat: an established lineage with a name
    seat = await actions.create_or_find_object("Agent", "agent:0806072e", "agent:0806072e")
    from src.parsers.base import EvidenceClass
    await actions.assert_property(seat, "source_model", "claude-fable-5", "agent:0806072e",
                                  __import__("datetime").datetime.now(
                                      __import__("datetime").UTC), 0.85,
                                  evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    await claim_name(actions, "agent:0806072e", "Soundwave", source="agent:0806072e")
    # the new tab's session row is BOUND to the seat (what mount(session_anchor=...) writes)
    session_row = str(tmp_path / "jobs" / SID[:8])
    await mounts.save_mount(actions.pool, job_dir=session_row, agent_id="agent:0806072e",
                            project="sibling-two", cwd="/w/sibling-two",
                            model="claude-fable-5", session_key="k")
    # the whisper re-fires on RESUME: it must assert the seat, not agent:39fb22a2
    out = await automount(actions, session_id=SID, cwd="/w/sibling-two",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                          source="resume")
    assert out["agent"] == "agent:0806072e"
    assert out["seat"] == "Soundwave"
    # ...and a COMPACTION is a death of the SEAT's mind: the heir is Soundwave II, no twin
    reborn = await automount(actions, session_id=SID, cwd="/w/sibling-two",
                             actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                             source="compact")
    assert reborn["agent"] == "agent:0806072e-ii" and reborn["seat"] == "Soundwave II"
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical LIKE 'agent:39fb22a2%'"
    ) == 0  # the hash twin was never born


async def test_a_stranger_compacting_at_birth_mints_nothing(actions: Actions,
                                                            tmp_path: Path) -> None:
    """You can only die if you lived: a session whose FIRST whisper arrives at a compact
    boundary (hook installed mid-session, server was down at startup) mounts fresh — no
    phantom ancestor, no -ii for a lineage with no I."""
    root = tmp_path / "projects"
    _transcript(root, "/w/fresh")
    out = await automount(actions, session_id=SID, cwd="/w/fresh", actor="analyst:operator",
                          root=root, jobs_home=tmp_path / "jobs", source="compact")
    assert out["agent"] == "agent:39fb22a2"           # generation 1 — nothing preceded it
    assert out["minted"] is None


async def test_a_joiner_inherits_the_rooms_settle_state(actions: Actions,
                                                        tmp_path: Path) -> None:
    """The zombie-count fix (operator-observed, 2026-07-09): a wake-minted second session
    counted 5 broadcasts its sibling had already settled. Joining the group chat does not
    make handled history your unread — but mail NOBODY settled still greets you (the wake
    pipeline's cause survives), and members who already joined keep their own unread."""
    from src.orchestrator.mailbox import ack_messages, read_inbox, send_message, unread_count

    root = tmp_path / "projects"
    _transcript(root, "/w/osiris")
    first = await automount(actions, session_id=SID, cwd="/w/osiris",
                            actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    # two broadcasts land; the incumbent settles ONE and leaves one open
    a = await send_message(actions.pool, from_agent="agent:x", from_project="q",
                           to_project="osiris", body="handled by the incumbent")
    await send_message(actions.pool, from_agent="agent:x", from_project="q",
                       to_project="osiris", body="still open work")
    await read_inbox(actions.pool, "osiris", reader_agent=first["agent"])
    await ack_messages(actions.pool, "osiris", [a["id"]], reader_agent=first["agent"])
    # a SECOND session joins the project fresh
    sid2 = "0abc1234-0000-4000-8000-000000000000"
    (root / "-w-osiris" / f"{sid2}.jsonl").write_text(json.dumps(
        {"type": "assistant", "cwd": "/w/osiris",
         "message": {"model": "claude-fable-5", "content": []}}) + "\n")
    joined = await automount(actions, session_id=sid2, cwd="/w/osiris",
                             actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    assert joined["mail"] == 1                       # the open one greets it; history doesn't
    # the incumbent's own state is untouched: its unsettled message is still its unread
    assert await unread_count(actions.pool, "osiris", reader_agent=first["agent"],
                              lease_secs=0) == 1
    # re-fire (resume) does not re-stamp anything — join-settle is a first-mount event
    again = await automount(actions, session_id=sid2, cwd="/w/osiris",
                            actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    assert again["mail"] == 1


async def test_automount_survives_a_sessionless_stranger(actions: Actions,
                                                         tmp_path: Path) -> None:
    # no transcript, junk session id → still a valid (unresolved) mount, never a crash:
    # the whisper is fail-open end to end
    out = await automount(actions, session_id="x", cwd="/w/mystery",
                          actor="analyst:operator", root=tmp_path / "empty",
                          jobs_home=tmp_path / "jobs")
    assert out["agent"].startswith("agent:") and out["resolved"] is False
    assert out["mail"] == 0 and "desk" in out


async def test_session_end_releases_the_seat_the_same_way_retire_does(
    actions: Actions, tmp_path: Path
) -> None:
    """The ghost-seat fix (heinrich's filing, thread 1fe6811c): SessionEnd is the harness's
    REAL close signal (Stop fires per-turn and cannot mean this) — releasing the durable mount
    row the instant the tab closes, instead of leaving it to age out of `last_seen`'s 15-minute
    window."""
    from src.orchestrator import mounts
    from src.orchestrator.handshake import session_end

    root = tmp_path / "projects"
    _transcript(root, "/w/sibling-nine")
    mounted = await automount(actions, session_id=SID, cwd="/w/sibling-nine",
                              actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    assert await mounts.find_mount(
        actions.pool, job_dir=str(tmp_path / "jobs" / SID[:8])) is not None

    out = await session_end(actions, session_id=SID, jobs_home=tmp_path / "jobs")

    assert out["agent"] == mounted["agent"] and out["released"] == 1
    assert await mounts.find_mount(actions.pool, job_dir=str(tmp_path / "jobs" / SID[:8])) is None
    # NOT a retire(): no permanent certificate — the Agent object is untouched
    assert await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='retired'", mounted["agent"]) is None


async def test_session_end_lets_the_same_session_id_resume_clean(
    actions: Actions, tmp_path: Path
) -> None:
    """Deliberately not a farewell: the SAME session id resuming later (`claude --resume`) fires
    SessionStart again, and its automount re-earns the row from scratch — no reanimation
    warning, no succession seam, exactly as if session_end() had never fired."""
    from src.orchestrator.handshake import session_end

    root = tmp_path / "projects"
    _transcript(root, "/w/sibling-nine")
    first = await automount(actions, session_id=SID, cwd="/w/sibling-nine",
                            actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    await session_end(actions, session_id=SID, jobs_home=tmp_path / "jobs")

    resumed = await automount(actions, session_id=SID, cwd="/w/sibling-nine",
                              actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                              source="resume")

    assert resumed["agent"] == first["agent"]         # same identity, no twin minted
    assert resumed["minted"] is None and resumed["swap"] is None
    # never retired, so the Agent carries no certificate a resume could reanimate
    assert await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='retired'", first["agent"]) is None


async def test_session_end_on_a_never_mounted_session_is_a_quiet_no_op(
    actions: Actions, tmp_path: Path
) -> None:
    """A session id nobody ever automounted (a phantom that never earned a row, or one this
    server never saw) must not error — SessionEnd fires unconditionally at every close."""
    from src.orchestrator.handshake import session_end

    out = await session_end(actions, session_id="deadbeef0000", jobs_home=tmp_path / "jobs")
    assert out["released"] == 0

    out2 = await session_end(actions, session_id="short")  # too short to derive an anchor
    assert out2["released"] == 0
