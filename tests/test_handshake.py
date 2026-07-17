"""The whisper's server half — automount: every session wakes up already mounted.

Drives the same tested mount path the tool uses, so these tests focus on what the whisper
ADDS: the derived job_dir anchor (durable + resolved, visible to the liveness probe), the
payload the hook prints (mail/desk/away), and idempotence on hook re-fire.
"""
from __future__ import annotations

import json
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.handshake import automount, office_claim, record_session_anchor
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
    assert out["seat"] == "Soundwave I"
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


async def test_automount_adopts_a_tab_view_instead_of_minting_a_clone(
    actions: Actions, tmp_path: Path
) -> None:
    """THE ALIAS-CLONE CURE (2026-07-16, the operator's '2 clones just spawned'): a tab
    attached to a live session fires a whisper under the TAB's own sid — no state.json
    receipt (the daemon's artifact), no transcript of its own — so every archaeology in
    fork_seat found nobody and a fresh anonymous agent was minted beside the living
    original. The hook's transcript_path names the conversation the tab continues:
    adopt that session's soul; the window registers as the mind it shows."""
    root = tmp_path / "projects"
    _transcript(root, "/w/viewed-repo")   # the REAL session's transcript (sid SID)
    first = await automount(actions, session_id=SID, cwd="/w/viewed-repo",
                            actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    tab_sid = "ab12cd34-0000-4000-8000-000000000000"
    out = await automount(actions, session_id=tab_sid, cwd="/w/viewed-repo",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                          transcript_path=str(root / "-w-viewed-repo" / f"{SID}.jsonl"))
    assert out["agent"] == first["agent"]           # the window IS the soul it shows
    assert out["view_of"] == SID[:8]                # ...and the whisper confesses it
    row = await actions.pool.fetchrow(
        "SELECT agent_id, session_key FROM agent_mounts WHERE job_dir=$1",
        str(tmp_path / "jobs" / "ab12cd34"))
    assert row is not None and row["agent_id"] == first["agent"]   # alias row born bound
    assert row["session_key"] == f"view-of:{SID[:8]}"   # ...and marked as the window it is
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical='agent:ab12cd34'") == 0


async def test_automount_never_adopts_a_sessions_own_transcript(
    actions: Actions, tmp_path: Path
) -> None:
    """A fresh session appending its OWN transcript is genuinely new — a transcript_path
    naming the session's own sid must not suppress the mint."""
    root = tmp_path / "projects"
    _transcript(root, "/w/fresh-repo")
    out = await automount(actions, session_id=SID, cwd="/w/fresh-repo",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                          transcript_path=str(root / "-w-fresh-repo" / f"{SID}.jsonl"))
    assert out["agent"] == "agent:39fb22a2"         # minted as itself, exactly as before
    assert "view_of" not in out


async def test_the_office_crowns_at_the_first_act_never_the_greeting(
    actions: Actions, tmp_path: Path
) -> None:
    """THE FOURTH DOOR, re-cut (16e3cee9, the title-generator incident): the whisper
    fires for plumbing exactly as for minds, so the GREETING mints nobody — it only
    HINTS whose office this is. The session's first authenticated ACT (office_claim at
    mount/re-attach) seats it as the seat's next life; and once the new life is minted,
    a second claimant is refused (succession is never parallel)."""
    from src.orchestrator.agents import register_agent, resolve_identity
    from src.orchestrator.mounts import save_mount

    root = tmp_path / "projects"
    offices = tmp_path / "seats"
    office = offices / "offa"
    office.mkdir(parents=True)
    o = await actions.create_or_find_object("Agent", "agent:0ffab001", "agent:0ffab001")
    from datetime import UTC, datetime
    now = datetime.now(UTC)
    await actions.assert_property(o, "handle", "Offa", "agent:0ffab001", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(o, "project", "offahouse", "agent:0ffab001", now, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/jobs/0ffab001", agent_id="agent:0ffab001",
                     project="offahouse", cwd=str(office), model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour' "
        "WHERE agent_id='agent:0ffab001'")   # the prior life is COLD — ended, not parallel

    fresh_sid = "0f45b117-0000-4000-8000-000000000000"
    out = await automount(actions, session_id=fresh_sid, cwd=str(office),
                          actor="analyst:operator", root=root,
                          jobs_home=tmp_path / "jobs", office_root=offices,
                          source="startup")
    assert out["agent"] == "agent:0f45b117"          # the greeting crowns NOBODY
    assert out["office_of"] == "agent:0ffab001"      # ...but names whose office this is

    # THE ACT: the session's first authenticated call claims the seat
    ident = resolve_identity(cwd=str(office),
                             job_dir=str(tmp_path / "jobs" / "0f45b117"))
    claimed = await office_claim(actions, cwd=str(office), agent_id=ident.agent_id,
                                 office_root=offices)
    assert claimed == "agent:0ffab001"
    ident.agent_id = claimed
    await register_agent(actions, ident, actor="analyst:operator",
                         mint_reason="office-birth")
    assert ident.agent_id == "agent:0ffab001-ii"     # the numeral ticked AT THE ACT

    # a SECOND claimant while the new life is just-minted: refused, stays anonymous
    guest_sid = "9ce57000-0000-4000-8000-000000000000"
    guest = await automount(actions, session_id=guest_sid, cwd=str(office),
                            actor="analyst:operator", root=root,
                            jobs_home=tmp_path / "jobs", office_root=offices,
                            source="startup")
    assert guest["agent"] == "agent:9ce57000"        # never a parallel life of the seat
    assert "office_of" not in guest                  # and no hint dangled at a taken seat
    assert await office_claim(actions, cwd=str(office), agent_id="agent:9ce57000",
                              office_root=offices) is None


async def test_a_stub_at_the_office_is_never_crowned(
    actions: Actions, tmp_path: Path
) -> None:
    """The title-generator replay (16e3cee9): harness plumbing whispers at a dead seat's
    office and never acts — however long it sits there, the lineage NEVER ticks."""
    from datetime import UTC, datetime

    root = tmp_path / "projects"
    offices = tmp_path / "seats"
    office = offices / "deeda"
    office.mkdir(parents=True)
    o = await actions.create_or_find_object("Agent", "agent:deed0b01", "agent:deed0b01")
    now = datetime.now(UTC)
    await actions.assert_property(o, "handle", "Deeda", "agent:deed0b01", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(o, "project", "deedhouse", "agent:deed0b01", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(o, "office", str(office), "agent:deed0b01", now, 0.9,
                                  evidence_class="direct_observation")
    # NO mount row for this lineage — the seat is dead; the deed alone names the office

    stub_sid = "cb083341-0000-4000-8000-000000000000"
    out = await automount(actions, session_id=stub_sid, cwd=str(office),
                          actor="analyst:operator", root=root,
                          jobs_home=tmp_path / "jobs", office_root=offices,
                          source="startup")
    assert out["agent"] == "agent:cb083341"          # its own hash, nothing more
    assert out["office_of"] == "agent:deed0b01"      # the hint fired (deed path, no rows)
    # the stub never acts — and the lineage never ticked
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' "
        "AND canonical LIKE 'agent:deed0b01-%'") == 0
    # a real mind acting from the same office still claims it (the deed alone suffices)
    claimed = await office_claim(actions, cwd=str(office), agent_id="agent:cb083341",
                                 office_root=offices)
    assert claimed == "agent:deed0b01"


async def test_a_known_sid_rebinds_from_the_ledger_never_minting(
    actions: Actions, tmp_path: Path
) -> None:
    """THE SESSION LEDGER (the g40-vi replay, 16e3cee9): a named session's registry row
    is wiped by an accident; its next whisper REBINDS to its lineage from the graph —
    it neither mints a hash twin nor crowns itself a false successor."""
    from datetime import UTC, datetime

    root = tmp_path / "projects"
    o = await actions.create_or_find_object("Agent", "agent:1edece01", "agent:1edece01")
    now = datetime.now(UTC)
    await actions.assert_property(o, "handle", "Ledda", "agent:1edece01", now, 0.9,
                                  evidence_class="self_declared")
    sid = "77aa88bb-0000-4000-8000-000000000000"
    wrote = await record_session_anchor(actions, agent_id="agent:1edece01",
                                        session_id=sid, actor="agent:1edece01")
    assert wrote is True
    # the accident: NO mount row anywhere for this sid or this lineage

    _transcript(root, "/w/ledda-repo")
    out = await automount(actions, session_id=sid, cwd="/w/ledda-repo",
                          actor="analyst:operator", root=root,
                          jobs_home=tmp_path / "jobs", source="startup")
    assert out["agent"] == "agent:1edece01"          # rebound from the ledger
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' "
        "AND canonical='agent:77aa88bb'") == 0       # no twin was ever minted


async def test_the_whisper_files_a_named_binding_on_the_ledger(
    actions: Actions, tmp_path: Path
) -> None:
    """THE SESSION LEDGER, write side: a session bound to a NAMED identity files its
    sid→soul fact at the whisper, so no future registry accident can orphan it. The
    self-evident anonymous case (canonical IS the sid hash) files nothing."""
    from src.orchestrator.mounts import save_mount

    root = tmp_path / "projects"
    _transcript(root, "/w/bind-repo")
    await save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / SID[:8]),
                     agent_id="agent:0ffab001", project="offahouse",
                     cwd="/w/bind-repo", model=None, session_key=None)
    await automount(actions, session_id=SID, cwd="/w/bind-repo",
                    actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                    source="startup")
    filed = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='anchor_sid' AND o.canonical='agent:0ffab001'")
    assert filed == SID                              # the binding is graph memory now


async def test_a_seat_walking_home_files_its_own_deed(
    actions: Actions, tmp_path: Path
) -> None:
    """A LIVE migration needs no ceremony (the Thoth pattern, 6886ca2a): a claimed seat
    whose running session breathes at its own office self-files the deed at the whisper
    door — so its NEXT death cannot orphan the office the way Ra's did."""
    from datetime import UTC, datetime

    from src.orchestrator.mounts import save_mount

    root = tmp_path / "projects"
    offices = tmp_path / "seats"
    office = offices / "walka"
    office.mkdir(parents=True)
    o = await actions.create_or_find_object("Agent", "agent:0a1cafe1", "agent:0a1cafe1")
    now = datetime.now(UTC)
    await actions.assert_property(o, "handle", "Walka", "agent:0a1cafe1", now, 0.9,
                                  evidence_class="self_declared")
    sid = "0a1cafe1-0000-4000-8000-000000000000"
    await save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "0a1cafe1"),
                     agent_id="agent:0a1cafe1", project="walkahouse",
                     cwd=str(office), model=None, session_key=None)

    await automount(actions, session_id=sid, cwd=str(office),
                    actor="analyst:operator", root=root,
                    jobs_home=tmp_path / "jobs", office_root=offices,
                    source="startup")
    deed = await actions.pool.fetchval(
        "SELECT d.value #>> '{}' FROM current_assertions d "
        "JOIN objects o2 ON o2.id=d.object_id "
        "WHERE d.name='office' AND o2.canonical='agent:0a1cafe1'")
    assert deed == str(office)                       # the seat walked home and deeded it
