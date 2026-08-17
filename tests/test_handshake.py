"""The whisper's server half — automount: every session wakes up already mounted.

Drives the same tested mount path the tool uses, so these tests focus on what the whisper
ADDS: the derived job_dir anchor (durable + resolved, visible to the liveness probe), the
payload the hook prints (mail/desk/away), and idempotence on hook re-fire.
"""
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.orchestrator import mounts as mounts_mod
from src.orchestrator.handshake import (
    BridgeAmbiguity,
    automount,
    bridged_seat,
    office_claim,
    record_bridge_anchor,
    record_session_anchor,
)
from src.orchestrator.mailbox import send_message
from src.orchestrator.seats import bind_holder, ensure_seat

SID = "39fb22a2-0000-4000-8000-000000000000"


@pytest.fixture(autouse=True)
def _fresh_greet_ledger() -> object:
    """The greet ledger is process-global monotonic state; a stamp left by one test's
    automount must not make another test's session_end yield (the resume-race grace)."""
    mounts_mod._GREETS.clear()
    yield
    mounts_mod._GREETS.clear()


def _transcript(root: Path, cwd: str, model: str = "claude-fable-5") -> None:
    proj = root / cwd.replace("/", "-")
    proj.mkdir(parents=True, exist_ok=True)
    (proj / f"{SID}.jsonl").write_text(json.dumps(
        {"type": "assistant", "cwd": cwd,
         "message": {"model": model, "content": [{"type": "text", "text": "hi"}]}}) + "\n")


async def test_automount_is_a_durable_anchored_mount(actions: Actions, tmp_path: Path) -> None:
    root = tmp_path / "projects"
    _transcript(root, "/w/sibling-eight")
    # send_message refuses a to_project nobody has ever mounted under (f6f3e43e, shape 3 of
    # #117) -- a throwaway, differently-named seed row (never agent:39fb22a2, the identity
    # this test's own automount() call mints below) registers existence without a live pulse.
    await mounts_mod.save_mount(
        actions.pool, job_dir="/test/seed/sibling-eight", agent_id="agent:seed-sibling-eight",
        project="sibling-eight", cwd="/test", model=None, session_key=None, alive=False)
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
    # hook re-fire (session resume) is idempotent — same identity, and under the full
    # visitor gate NO greeting mints an object: the graph stays clean until the first act
    again = await automount(actions, session_id=SID, cwd="/w/sibling-eight",
                            actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    assert again["agent"] == out["agent"]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical='agent:39fb22a2'") == 0


async def test_automount_from_a_bare_seats_root_writes_the_seated_house(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """thread 6a00e942: THE ACTUAL SPECIMEN — a background job's FIRST whisper fires from
    the bare seats CONTAINER root (~/.osiris/seats), not the interactive mount() tool.
    mount()'s own project write already overrides a seated session's project with its
    seat's derived house (mcp_server._resolve_project_seat_first) — automount() never
    did, so a session whisper-mounted here permanently filed its registry row under no
    project at all, while orient()/mount() (re-deriving live via the same seat binding
    on every later call) told the correct story. Same seat, same session, two answers —
    the 60bc15db shape."""
    fake_root = tmp_path / ".osiris" / "seats"
    fake_root.mkdir(parents=True)
    monkeypatch.setattr("src.orchestrator.offices._DEFAULT_OFFICE_ROOT", fake_root)

    seat = await ensure_seat(actions, house="osiris", handle="Barewhisper", source="test")
    assert seat.get("error") is None
    # the job_dir a whisper for THIS session_id will derive — bind the seat to the SAME
    # agent id BEFORE the first breath, exactly as a returning/pre-charted seat would be
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:39fb22a2",
                      source="test")

    out = await automount(actions, session_id=SID, cwd=str(fake_root),
                          actor="analyst:operator", jobs_home=tmp_path / "jobs")
    assert out["agent"] == "agent:39fb22a2"
    row = await actions.pool.fetchrow(
        "SELECT project FROM agent_mounts WHERE agent_id='agent:39fb22a2'")
    assert row is not None
    assert row["project"] == "osiris", (
        "the whisper's own registry write must carry the seated house too — a background "
        "job's first breath must never file a seated agent under no project at all")
    # THE ACTUAL REPORTED SYMPTOM: fleet() reads the AGENT's own current_assertions
    # 'project', not agent_mounts.project — this is register_agent's own stamp,
    # written BEFORE the seat-first correction, so it must be checked separately.
    fleet_row = await actions.pool.fetchrow(
        "SELECT value #>> '{}' AS project FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='agent:39fb22a2' AND a.name='project' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1")
    assert fleet_row is not None
    assert fleet_row["project"] == "osiris", (
        "fleet() must file the seated agent under its house, never '?'")


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
    # the visitor gate holds at the greeting; the first ACT (mount's register) mints,
    # then the mind claims its name — the production order
    from src.orchestrator.agents import register_agent, resolve_identity
    ident = resolve_identity(cwd="/w/osiris", job_dir=str(tmp_path / "jobs" / SID[:8]),
                             root=root)
    await register_agent(actions, ident, actor="analyst:operator")
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
    # a SECOND compaction is a second death — the numeral keeps counting, PROVIDED "-ii"
    # actually lived (ruling d3531cd8): give it a witnessed act first, or this is exactly
    # the zero-turn-fold's own shape (two compactions, no acts between) and "-ii" folds
    # instead of "-iii" ever minting — see test_two_zero_turn_compactions_fold below.
    from src.orchestrator.capture import record_decision
    await record_decision(actions, "39fb22a2-ii did real work", source="agent:39fb22a2-ii")
    third = await automount(actions, session_id=SID, cwd="/w/osiris", actor="analyst:operator",
                            root=root, jobs_home=tmp_path / "jobs", source="compact")
    assert third["agent"] == "agent:39fb22a2-iii" and third["seat"] == "Thoth III"
    # ...but a plain resume between deaths is NOT one — same mind, same numeral
    resumed = await automount(actions, session_id=SID, cwd="/w/osiris", actor="analyst:operator",
                              root=root, jobs_home=tmp_path / "jobs", source="resume")
    assert resumed["agent"] == "agent:39fb22a2-iii"


async def test_two_zero_turn_compactions_fold(actions: Actions, tmp_path: Path) -> None:
    """SUCCESSION FOLLOWS TURNS, NOT HARNESS EVENTS (ruling d3531cd8) — the canonical repro,
    through the REAL production whisper path: two SessionStart source='compact' events
    back-to-back with no witnessed act between them (the exact shape of /compact then
    /model, zero turns) must not chain a third generation onto a mind that never lived.
    The second compaction's heir lands on the ORIGINAL agent, reusing '-ii', not '-iii'."""
    from src.orchestrator.agents import claim_name, register_agent, resolve_identity

    root = tmp_path / "projects"
    _transcript(root, "/w/osiris")
    await automount(actions, session_id=SID, cwd="/w/osiris", actor="analyst:operator",
                    root=root, jobs_home=tmp_path / "jobs", source="startup")
    ident = resolve_identity(cwd="/w/osiris", job_dir=str(tmp_path / "jobs" / SID[:8]),
                             root=root)
    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, "agent:39fb22a2", "Thoth", source="agent:39fb22a2")

    reborn = await automount(actions, session_id=SID, cwd="/w/osiris", actor="analyst:operator",
                             root=root, jobs_home=tmp_path / "jobs", source="compact")
    assert reborn["agent"] == "agent:39fb22a2-ii"

    # NO witnessed act here — the phantom's whole life is this one silent mint
    third = await automount(actions, session_id=SID, cwd="/w/osiris", actor="analyst:operator",
                            root=root, jobs_home=tmp_path / "jobs", source="compact")
    assert third["agent"] == "agent:39fb22a2-ii", "the silent '-ii' folds; the numeral is reused"
    assert await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='agent:39fb22a2-ii' AND a.name='false_mint' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1") == "true"


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


async def test_the_whisper_reads_the_current_seat_generation_not_a_text_max(
    actions: Actions, tmp_path: Path,
) -> None:
    """thread 43c84fa9 (stale generation label): _seat_of used to pick seat_generation via
    a bare MAX(value) TEXT aggregate across EVERY current_assertions row for the property,
    instead of the one CURRENT (highest-confidence, most-recent) value that seat_bearings
    already reads correctly — current_assertions can carry more than one non-superseded
    row per (object, name) under multiple writers (a real, live-confirmed condition), and a
    bare text MAX picks whichever sorts highest as a STRING, not the right one: '9' text-
    sorts above '12' even though 12 is the true, higher-confidence generation. This is
    exactly why the resume-hook whisper (automount -> _seat_of) could announce a stale
    numeral while orient() (-> seat_bearings), moments later in the same session, read
    correctly."""
    from src.orchestrator.agents import claim_name

    root = tmp_path / "projects"
    _transcript(root, "/w/text-max")
    seat = await actions.create_or_find_object("Agent", "agent:7e57c0de", "agent:7e57c0de")
    await claim_name(actions, "agent:7e57c0de", "Deckard", source="agent:7e57c0de")
    now = datetime.now(UTC)
    # the correct, current generation — high confidence, from the normal minting writer
    await actions.assert_property(seat, "seat_generation", "12", "agent:7e57c0de", now, 0.9,
                                  evidence_class="self_declared")
    # a conflicting stray from another writer — LOWER confidence, but text-sorts ABOVE "12"
    await actions.assert_property(seat, "seat_generation", "9", "agent:stray-writer", now, 0.4,
                                  evidence_class="self_declared")
    session_row = str(tmp_path / "jobs" / SID[:8])
    await mounts_mod.save_mount(actions.pool, job_dir=session_row, agent_id="agent:7e57c0de",
                                project="text-max", cwd="/w/text-max",
                                model="claude-fable-5", session_key="k")
    out = await automount(actions, session_id=SID, cwd="/w/text-max",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                          source="resume")
    assert out["seat"] == "Deckard XII"


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


async def test_no_greeting_mints_an_object_anywhere(actions: Actions,
                                                    tmp_path: Path) -> None:
    """THE FULL VISITOR GATE (Phase 1b of ruling 120fcc81): the office rule extends to
    every threshold — an unknown fresh session at an ORDINARY cwd gets a row and nothing
    else, however many times its greeting re-fires; the object is earned at the first
    authenticated act (mount()/_reattach's register)."""
    from src.orchestrator import mounts
    from src.orchestrator.agents import register_agent, resolve_identity

    root = tmp_path / "projects"
    _transcript(root, "/w/anywhere")
    out = await automount(actions, session_id=SID, cwd="/w/anywhere",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    assert out["agent"] == "agent:39fb22a2"
    # the greeting left NO object...
    assert await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='Agent' AND canonical=$1", out["agent"]) is None
    # ...but the ROW (the address) is there, so mail and liveness have a door
    assert await mounts.find_mount(
        actions.pool, job_dir=str(tmp_path / "jobs" / SID[:8])) is not None
    # a RE-FIRED greeting (hook refire, resume) still mints nothing — the row is the
    # gate's own artifact, never evidence of a life
    again = await automount(actions, session_id=SID, cwd="/w/anywhere",
                            actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    assert again["agent"] == out["agent"]
    assert await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='Agent' AND canonical=$1", out["agent"]) is None
    # ...and a compact re-fire for a row-only stranger mints NO phantom heir either:
    # you can only die if you lived, and a whisper row alone is not a life
    compacted = await automount(actions, session_id=SID, cwd="/w/anywhere",
                                actor="analyst:operator", root=root,
                                jobs_home=tmp_path / "jobs", source="compact")
    assert compacted["minted"] is None
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical LIKE 'agent:39fb22a2%'",
    ) == 0
    # the first authenticated ACT mints it as itself (mount()/_reattach's register path)
    ident = resolve_identity(cwd="/w/anywhere", job_dir=str(tmp_path / "jobs" / SID[:8]),
                             root=root)
    await register_agent(actions, ident, actor="analyst:operator")
    assert await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='Agent' AND canonical=$1", out["agent"]) == 1


async def test_a_declared_child_is_born_denominated(actions: Actions,
                                                    tmp_path: Path) -> None:
    """THE WAKE-ORPHAN CURE (operator ruling 2026-07-17: 'orphans like that are
    structurally impossible going forward'): a spawner that declared parentage before the
    child's first breath (OSIRIS_SPAWNED_BY → the whisper's spawned_by) gets a REGISTERED
    CHILD — spawned_by edge, roman.arabic patronym — never an anonymous stranger."""
    from datetime import UTC, datetime

    root = tmp_path / "projects"
    _transcript(root, "/w/wakehouse")
    parent = await actions.create_or_find_object("Agent", "agent:dad0beef", "agent:dad0beef")
    await actions.assert_property(parent, "handle", "Sower", "agent:dad0beef",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")

    out = await automount(actions, session_id=SID, cwd="/w/wakehouse",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                          spawned_by="agent:dad0beef", spawn_type="wake-triage")

    assert out["agent"] == "agent:39fb22a2"          # the child IS the session's canonical
    assert out["child_of"] == "agent:dad0beef"       # the birth receipt names the parent
    row = await actions.pool.fetchrow(
        "SELECT (SELECT 1 FROM links l JOIN objects p ON p.id=l.to_id "
        "        JOIN objects c ON c.id=l.from_id "
        "        WHERE l.type='spawned_by' AND c.canonical='agent:39fb22a2' "
        "        AND p.canonical='agent:dad0beef') AS parented, "
        "       (SELECT value #>> '{}' FROM current_assertions a "
        "        JOIN objects o ON o.id=a.object_id "
        "        WHERE o.canonical='agent:39fb22a2' AND a.name='patronym') AS patronym")
    assert row["parented"] == 1                      # the edge exists from breath one
    assert row["patronym"] and row["patronym"].startswith("Sower")  # roman.arabic
    # a re-fired greeting converges on the SAME child — no twin, no stranger
    again = await automount(actions, session_id=SID, cwd="/w/wakehouse",
                            actor="analyst:operator", root=root,
                            jobs_home=tmp_path / "jobs",
                            spawned_by="agent:dad0beef")
    assert again["agent"] == "agent:39fb22a2"
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' "
        "AND canonical='agent:39fb22a2'") == 1


async def test_a_failed_child_registration_confesses_rather_than_omitting(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """60bc15db specimen 2 (decision 01e0c69a): a spawner DID declare parentage, but
    register_spawn itself failed — this must never render identically to "no spawner
    declared parentage at all" (the genuine, correctly-silent case). The mount still
    degrades to a plain visitor (identity binding stays conservative), but the receipt
    must confess, matching attach/transcripts_healed's own "...FAILED..." shape two
    blocks away in the same function."""
    import src.orchestrator.lineage as lineage_mod

    async def _raise(*args: object, **kwargs: object) -> str:
        raise RuntimeError("simulated registration failure")

    monkeypatch.setattr(lineage_mod, "register_spawn", _raise)

    root = tmp_path / "projects"
    _transcript(root, "/w/wakehouse2")
    parent = await actions.create_or_find_object("Agent", "agent:dad0beef2", "agent:dad0beef2")
    await actions.assert_property(parent, "handle", "Sower2", "agent:dad0beef2",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")

    out = await automount(actions, session_id=SID, cwd="/w/wakehouse2",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                          spawned_by="agent:dad0beef2", spawn_type="wake-triage")

    assert out["child_of"] == "agent:dad0beef2"       # confessed, not omitted
    assert out["child_note"] is not None and "FAILED" in out["child_note"]
    # identity binding stayed conservative — never rebound to a phantom child
    assert out["agent"] != "agent:dad0beef2"


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

    mounts._GREETS.clear()  # a TRUE close: long after any greeting, not the resume race
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
    mounts_mod._GREETS.clear()  # a TRUE close, not the resume race
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


async def test_whisper_names_live_co_agents_at_first_breath(
    actions: Actions, tmp_path: Path
) -> None:
    """Task #40 (thread 2b784653): the shared-tree collision warning used to arrive at
    mount() — AFTER a session may already have staged or pushed. The whisper payload now
    carries live co-agents on the same project from breath one; a lone session carries
    none (no wolf-crying on an uncontended tree)."""
    from src.orchestrator.mounts import save_mount

    root = tmp_path / "projects"
    _transcript(root, "/w/sibling-nine")
    # a live sibling on the same project, at a different door
    await save_mount(actions.pool, job_dir="/x/jobs/c0a9e9e1",
                     agent_id="agent:c0a9e9e1", project="sibling-nine",
                     cwd="/w/sibling-nine", model=None, session_key=None)
    out = await automount(actions, session_id=SID, cwd="/w/sibling-nine",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    assert out.get("co_agents") == ["agent:c0a9e9e1"]
    # ...and the sibling's own whisper sees us back; a project with no OTHER live door
    # gets no key at all — absence, not an empty list
    lone = await automount(actions, session_id="feed0002-0000-4000-8000-000000000000",
                           cwd="/w/lonely", actor="analyst:operator",
                           root=tmp_path / "empty", jobs_home=tmp_path / "jobs")
    assert "co_agents" not in lone


async def test_session_end_yields_to_a_fresh_greeting_the_resume_race(
    actions: Actions, tmp_path: Path
) -> None:
    """THE RESUME RACE (Alfred's field report msgs 717/718, 2026-07-19): a window resume
    fires the predecessor's SessionEnd beside the successor's SessionStart, and when the
    end lands second (automount 20:03:03, session-end 20:03:04, observed live) it deleted
    the door the greeting had just seated — Maat's window then answered every probe
    {live:false, last_seen:NULL} and the poke lane skipped it while a build order sat
    unread. An end arriving within the greeting's grace is the OLD incarnation's obituary,
    not the new one's: it must yield and leave the door standing. A true close (long after
    any greeting) still releases — the previous tests pin that half."""
    from src.orchestrator.handshake import session_end

    root = tmp_path / "projects"
    _transcript(root, "/w/sibling-nine")
    await automount(actions, session_id=SID, cwd="/w/sibling-nine",
                    actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")

    # the end lands one breath after the greeting — the observed race, exactly
    out = await session_end(actions, session_id=SID, jobs_home=tmp_path / "jobs")

    assert out["released"] == 0 and out.get("yielded") is True
    assert await mounts_mod.find_mount(
        actions.pool, job_dir=str(tmp_path / "jobs" / SID[:8])) is not None  # door stands


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
    # ...and the guest's GREETING minted no object either (the fa4462d5 orphan class)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' "
        "AND canonical='agent:9ce57000'") == 0


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
    # the stub never acts — the lineage never ticked, AND no object was minted for a
    # GREETING at an office (the fa4462d5 orphan class: identity is earned by the act)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' "
        "AND canonical LIKE 'agent:deed0b01-%'") == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' "
        "AND canonical='agent:cb083341'") == 0
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
        "WHERE a.name = 'anchor_sid:' || $1 AND o.canonical='agent:0ffab001'", SID[:8])
    assert filed == SID                              # the binding is graph memory now


async def test_the_ledger_remembers_every_sid_of_a_lineage(actions: Actions) -> None:
    """The first backfill's catch: current_assertions keeps ONE winner per (object,
    name), so a shared 'anchor_sid' name gave a many-sid lineage amnesia — only the
    last-filed sid survived. Namespaced names (anchor_sid:<sid8>) make each sid its own
    fact: every door of a long life must stay resolvable."""
    from datetime import UTC, datetime

    o = await actions.create_or_find_object("Agent", "agent:9a9a0001", "agent:9a9a0001")
    now = datetime.now(UTC)
    await actions.assert_property(o, "handle", "Nine", "agent:9a9a0001", now, 0.9,
                                  evidence_class="self_declared")
    sid_a = "aaaa1111-0000-4000-8000-000000000000"
    sid_b = "bbbb2222-0000-4000-8000-000000000000"
    assert await record_session_anchor(actions, agent_id="agent:9a9a0001",
                                       session_id=sid_a, actor="t") is True
    assert await record_session_anchor(actions, agent_id="agent:9a9a0001",
                                       session_id=sid_b, actor="t") is True
    from src.orchestrator.handshake import ledger_seat
    assert await ledger_seat(actions, sid_prefix=sid_a) == "agent:9a9a0001"
    assert await ledger_seat(actions, sid_prefix=sid_b) == "agent:9a9a0001"
    # idempotent per sid, and the 8-char anchor form resolves too
    assert await record_session_anchor(actions, agent_id="agent:9a9a0001",
                                       session_id=sid_a, actor="t") is False
    assert await ledger_seat(actions, sid_prefix="bbbb2222") == "agent:9a9a0001"


async def test_bridged_seat_rebinds_a_known_bridge_id_never_minting(
    actions: Actions, tmp_path: Path,
) -> None:
    """task #68 binding leg: a background-job fork's CLAUDE_CODE_BRIDGE_SESSION_ID rebinds
    to its lineage's living head — the class fork_seat cannot see, because this kind of
    fork's transcript starts fresh (sessionKind='bg') and shares no record uuid with its
    ancestor's at all."""
    root = tmp_path / "projects"
    o = await actions.create_or_find_object("Agent", "agent:5198945f", "agent:5198945f")
    now = datetime.now(UTC)
    await actions.assert_property(o, "handle", "Imhotep", "agent:5198945f", now, 0.9,
                                  evidence_class="self_declared")
    bridge = "session_01Ckh2yDH5pi945rm4iLBsfa"
    wrote = await record_bridge_anchor(actions, agent_id="agent:5198945f",
                                       bridge_session_id=bridge, actor="agent:5198945f")
    assert wrote is True

    fork_sid = "e08c3850-4180-4876-b313-fafef21d368a"
    _transcript(root, "/w/imhotep-repo")
    out = await automount(actions, session_id=fork_sid, cwd="/w/imhotep-repo",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                          source="startup", bridge_session_id=bridge)
    assert out["agent"] == "agent:5198945f"           # rebound via the bridge
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' "
        "AND canonical='agent:e08c3850'") == 0         # no sixth identity minted


async def test_no_bridge_id_never_touches_the_bridge_door(
    actions: Actions, tmp_path: Path,
) -> None:
    """A session with no CLAUDE_CODE_BRIDGE_SESSION_ID (the ordinary case, still the vast
    majority) must resolve exactly as before — the bridge door is additive, never a
    default-path regression."""
    root = tmp_path / "projects"
    _transcript(root, "/w/no-bridge-repo")
    out = await automount(actions, session_id=SID, cwd="/w/no-bridge-repo",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                          source="startup")
    assert out["agent"] == f"agent:{SID[:8]}"


async def test_the_whisper_files_a_bridge_binding(actions: Actions, tmp_path: Path) -> None:
    """THE BRIDGE, write side: a session bound to a NAMED identity files its bridge id at the
    whisper (same gate record_session_anchor's own write-side test uses — a genuine stranger
    mints no object at all, per the full visitor gate, so there is nothing yet to stamp)."""
    from src.orchestrator.mounts import save_mount

    root = tmp_path / "projects"
    _transcript(root, "/w/bridge-repo")
    await save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / SID[:8]),
                     agent_id="agent:0ffab001", project="offahouse",
                     cwd="/w/bridge-repo", model=None, session_key=None)
    await automount(actions, session_id=SID, cwd="/w/bridge-repo",
                    actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                    source="startup", bridge_session_id="session_bridgetest01")
    filed = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='bridge_session_id' AND o.canonical='agent:0ffab001'")
    assert filed == "session_bridgetest01"


async def test_automount_rebinds_via_the_bridge_even_while_the_ancestor_is_alive(
    actions: Actions, tmp_path: Path,
) -> None:
    """The whole point of the bridge door: office_seat REFUSES to crown a new session when
    the office's own lineage still has a live pulse ('two parallel fresh contexts must
    never both be the seat' — office_seat's own law, correct for a cwd-GUESS). The bridge is
    not a guess — the harness said so directly — so it must succeed exactly where office_hint
    would decline and mint a guest. This is the exact shape of Imhotep's own six-id
    biography: a fork landing while its ancestor is still live and running."""
    from src.orchestrator.mounts import save_mount

    root = tmp_path / "projects"
    offices = tmp_path / "seats"
    office = offices / "nefer"
    office.mkdir(parents=True)
    o = await actions.create_or_find_object("Agent", "agent:aaaa1111", "agent:aaaa1111")
    now = datetime.now(UTC)
    await actions.assert_property(o, "handle", "Nefer", "agent:aaaa1111", now, 0.9,
                                  evidence_class="self_declared")
    # a LIVE pulse for the ancestor (save_mount's own alive=True default) — exactly what
    # makes office_seat refuse and mint a guest instead, the bug this door fixes.
    await save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "aaaa1111"),
                     agent_id="agent:aaaa1111", project="neferhouse",
                     cwd=str(office), model=None, session_key=None)
    bridge = "session_livefork01"
    assert await record_bridge_anchor(actions, agent_id="agent:aaaa1111",
                                      bridge_session_id=bridge, actor="t") is True

    fork_sid = "bbbb2222-0000-4000-8000-000000000000"
    out = await automount(actions, session_id=fork_sid, cwd=str(office),
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                          office_root=offices, source="startup", bridge_session_id=bridge)
    assert out["agent"] == "agent:aaaa1111"           # rebound, not a guest
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' "
        "AND canonical='agent:bbbb2222'") == 0


async def test_record_bridge_anchor_serializes_concurrent_writers(actions: Actions) -> None:
    """The write-side half of ruling 61e00f25 (thread dc9d1eed, Khnum VII's review finding
    d224f186): two concurrent automounts carrying the IDENTICAL bridge id — the exact shape
    of several background forks of one conversation booting near-simultaneously — must not
    both land the anchor. Before the mint_lock wrap, both racers could pass the exists-check
    before either committed; the lock makes the loser's own exists-check see the winner's
    committed row and no-op."""
    now = datetime.now(UTC)
    for cid in ("agent:c0000001", "agent:c0000002"):
        o = await actions.create_or_find_object("Agent", cid, cid)
        await actions.assert_property(o, "handle", "racer", cid, now, 0.9,
                                       evidence_class="self_declared")
    bridge = "session_racecondition01"
    results = await asyncio.gather(
        record_bridge_anchor(actions, agent_id="agent:c0000001",
                             bridge_session_id=bridge, actor="agent:c0000001"),
        record_bridge_anchor(actions, agent_id="agent:c0000002",
                             bridge_session_id=bridge, actor="agent:c0000002"),
    )
    assert sorted(results) == [False, True]  # exactly one writer wins, never both
    owners = await actions.pool.fetch(
        "SELECT o.canonical FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='bridge_session_id' AND a.value #>> '{}' = $1", bridge)
    assert len(owners) == 1  # never landed on two different lineages


async def test_bridged_seat_refuses_on_multi_owner_ambiguity(actions: Actions) -> None:
    """The read-side half: ambiguity already minted (a race the lock doesn't cover, or one
    that pre-dates this fix) must be refused loudly, never guessed away by picking whichever
    lineage happened to write last — the exact silent-mis-resolve d224f186 found."""
    now = datetime.now(UTC)
    for cid in ("agent:d1111111", "agent:d2222222"):
        o = await actions.create_or_find_object("Agent", cid, cid)
        await actions.assert_property(o, "handle", "racer", cid, now, 0.9,
                                      evidence_class="self_declared")
        await actions.assert_property(o, "bridge_session_id", "session_ambiguous01", cid, now,
                                      0.9, evidence_class="direct_observation")
    with pytest.raises(BridgeAmbiguity):
        await bridged_seat(actions, bridge_session_id="session_ambiguous01")


async def test_bridged_seat_same_lineage_repeat_write_is_not_ambiguous(
    actions: Actions,
) -> None:
    """One lineage bridging more than once (ordinary repeated forking of the same
    conversation) must never be mistaken for the multi-lineage race — both rows agree on
    the SAME base, so this resolves to the lineage's living head exactly as before."""
    now = datetime.now(UTC)
    base = await actions.create_or_find_object("Agent", "agent:e1111111", "agent:e1111111")
    heir = await actions.create_or_find_object(
        "Agent", "agent:e1111111-ii", "agent:e1111111-ii")
    for cid, obj in (("agent:e1111111", base), ("agent:e1111111-ii", heir)):
        await actions.assert_property(obj, "handle", "racer", cid, now, 0.9,
                                      evidence_class="self_declared")
        await actions.assert_property(obj, "bridge_session_id", "session_samelineage01", cid,
                                      now, 0.9, evidence_class="direct_observation")
    result = await bridged_seat(actions, bridge_session_id="session_samelineage01")
    assert result == "agent:e1111111-ii"  # the lineage's head, no ambiguity raised


async def test_automount_falls_through_to_office_hint_on_bridge_ambiguity(
    actions: Actions, tmp_path: Path,
) -> None:
    """The whisper must never die of an ambiguous bridge (ruling 61e00f25's 'refuse loudly,
    but the whisper itself never dies of one' — same law as the attach ceremony and the
    resume heal): it falls through to the ordinary stranger resolution exactly as a bare
    'no known bridge id' would, and confesses the ambiguity in the payload instead of
    silently swallowing or crashing on it."""
    now = datetime.now(UTC)
    for cid in ("agent:f1111111", "agent:f2222222"):
        o = await actions.create_or_find_object("Agent", cid, cid)
        await actions.assert_property(o, "handle", "racer", cid, now, 0.9,
                                      evidence_class="self_declared")
        await actions.assert_property(o, "bridge_session_id", "session_ambiguous02", cid, now,
                                      0.9, evidence_class="direct_observation")
    root = tmp_path / "projects"
    sid = "ffff3333-0000-4000-8000-000000000000"
    _transcript(root, "/w/ambiguous-bridge-repo")
    out = await automount(actions, session_id=sid, cwd="/w/ambiguous-bridge-repo",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                          source="startup", bridge_session_id="session_ambiguous02")
    assert out["agent"] == f"agent:{sid[:8]}"  # falls through, exactly as no-bridge-id would
    assert "session_ambiguous02" in out["bridge_ambiguity"]
    assert "agent:f1111111" in out["bridge_ambiguity"]
    assert "agent:f2222222" in out["bridge_ambiguity"]


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


async def test_automount_inlines_the_top_of_the_obligations_wall(
    actions: Actions, tmp_path: Path
) -> None:
    """INLINE THE FOLD (thread a3a3d512): the thing a resumed mind re-derives every time
    is its project's live obligation set — automount's payload now carries the top of the
    SAME ranked wall orient() renders (obligations-first, echoes excluded), so the whisper
    can show it without a full orient() round-trip."""
    from src.orchestrator.capture import open_thread

    root = tmp_path / "projects"
    _transcript(root, "/w/obligated-project")
    await open_thread(actions, "fix the thing before it breaks again",
                      repo="obligated-project", kind="obligation", owner="obligated-project")
    await open_thread(actions, "just a question, never a duty",
                      repo="obligated-project", kind="question", owner="obligated-project")

    out = await automount(actions, session_id=SID, cwd="/w/obligated-project",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")

    assert out.get("obligations")
    summaries = [o["summary"] for o in out["obligations"]]
    assert any("fix the thing" in s for s in summaries)
    assert not any("just a question" in s for s in summaries)  # an echo, never inlined


async def test_automount_points_a_fresh_heir_at_charter_and_newest_succession_thread(
    actions: Actions, tmp_path: Path
) -> None:
    """SUCCESSION STEERING (d80621a7 piece 4): a freshly minted heir's summary-steering
    anchor is resolved BY QUERY — the office's charter file (self-filed at a live seat's
    own office) plus the newest kind='obligation' thread this project owns — never an id
    copied into the whisper script once and left to rot."""
    from src.orchestrator.agents import claim_name, register_agent, resolve_identity
    from src.orchestrator.capture import open_thread

    root = tmp_path / "projects"
    offices = tmp_path / "seats"
    office = offices / "steward"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "testhouse"\n')
    _transcript(root, str(office))

    ident = resolve_identity(cwd=str(office), job_dir=str(tmp_path / "jobs" / SID[:8]), root=root)
    assert ident.project == "testhouse"  # decoupled from the office dir name (the handle)
    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "Steward", source=ident.agent_id)
    # the live seat breathes at its own office — automount self-files the deed (the
    # existing 'a seat walking home files its own deed' path), here just setup
    await automount(actions, session_id=SID, cwd=str(office), actor="analyst:operator",
                    root=root, jobs_home=tmp_path / "jobs", office_root=offices,
                    source="startup")
    deed = await actions.pool.fetchval(
        "SELECT d.value #>> '{}' FROM current_assertions d "
        "JOIN objects o2 ON o2.id=d.object_id "
        "WHERE d.name='office' AND o2.canonical=$1", ident.agent_id)
    assert deed == str(office)  # setup sanity: the deed really did self-file

    await open_thread(actions, "an old obligation nobody touched", repo="testhouse",
                      kind="obligation", owner="testhouse")
    await open_thread(actions, "the actual handoff, freshest of the pile", repo="testhouse",
                      kind="obligation", owner="testhouse")

    reborn = await automount(actions, session_id=SID, cwd=str(office),
                             actor="analyst:operator", root=root,
                             jobs_home=tmp_path / "jobs", office_root=offices,
                             source="compact")
    assert reborn["minted"] == ident.agent_id
    succ = reborn.get("succession")
    assert succ is not None
    assert "the actual handoff" in succ["thread_summary"]
    # #155: charter_file now rides identity_anchor, unconditional — not succession
    anchor = reborn.get("identity_anchor")
    assert anchor is not None
    assert anchor["charter_file"] == f"{office}/CLAUDE.md"


async def test_succession_pointer_prefers_charter_md_over_claude_md(
    actions: Actions, tmp_path: Path
) -> None:
    """Assignment 3, piece (iii): when the office has a charter.md (the seat's own
    hand-maintained live state, assignment 3's scaffold), the identity anchor (#155)
    prefers it over CLAUDE.md (the standing orders, which never change). automount runs
    on the same host as the office it names, so a disk check is a legitimate witness. An
    office with no charter.md (the case above) keeps falling back to CLAUDE.md."""
    from src.orchestrator.agents import claim_name, register_agent, resolve_identity

    root = tmp_path / "projects"
    offices = tmp_path / "seats"
    office = offices / "curator"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "curatorhouse"\n')
    (office / "charter.md").write_text("# Curator's charter\n\nlive state.\n")
    _transcript(root, str(office))

    ident = resolve_identity(cwd=str(office), job_dir=str(tmp_path / "jobs" / SID[:8]), root=root)
    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "Curator", source=ident.agent_id)
    await automount(actions, session_id=SID, cwd=str(office), actor="analyst:operator",
                    root=root, jobs_home=tmp_path / "jobs", office_root=offices,
                    source="startup")

    reborn = await automount(actions, session_id=SID, cwd=str(office),
                             actor="analyst:operator", root=root,
                             jobs_home=tmp_path / "jobs", office_root=offices,
                             source="compact")
    # #155: charter_file now rides identity_anchor, unconditional — not succession
    anchor = reborn.get("identity_anchor")
    assert anchor is not None
    assert anchor["charter_file"] == f"{office}/charter.md"


async def test_identity_anchor_fires_on_an_ordinary_mount_never_gated_on_succession(
    actions: Actions, tmp_path: Path,
) -> None:
    """#155's whole point, live: gating identity delivery on succession WAS the bug
    (Thoth's own ruling, msg 3853 — she had generalized from her own most-compacted-seat-
    in-the-fleet experience). A single ordinary mount (source='startup', nothing minted)
    must still carry the identity anchor, including the compiled office pointer, which is
    exactly what a tree-bound seat's harness never auto-loads (#141)."""
    from src.orchestrator.agents import claim_name, register_agent, resolve_identity
    from src.orchestrator.boot_compiler import wrap_managed

    root = tmp_path / "projects"
    offices = tmp_path / "seats"
    office = offices / "sentinel"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "sentinelhouse"\n')
    (office / "CLAUDE.md").write_text(
        "# sentinel — seat office\n\n" + wrap_managed("role: WORKER", "abc123"))
    _transcript(root, str(office))

    ident = resolve_identity(cwd=str(office), job_dir=str(tmp_path / "jobs" / SID[:8]), root=root)
    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "Sentinel", source=ident.agent_id)

    out = await automount(actions, session_id=SID, cwd=str(office), actor="analyst:operator",
                          root=root, jobs_home=tmp_path / "jobs", office_root=offices,
                          source="startup")
    assert not out.get("minted")  # gen 1, first breath, nothing succeeded
    anchor = out.get("identity_anchor")
    assert anchor is not None, "identity_anchor must fire on an ordinary mount too"
    assert anchor["charter_file"] == f"{office}/CLAUDE.md"
    assert anchor["compiled_office"] == f"{office}/CLAUDE.md"


async def test_identity_anchor_compiled_office_fails_open_when_never_compiled(
    actions: Actions, tmp_path: Path,
) -> None:
    """A scaffolded office whose CLAUDE.md has no osiris:compiled markers at all (never
    reissue_office'd) must still carry charter_file — compiled_office is simply absent,
    never an exception that breaks the whisper (577988ed)."""
    from src.orchestrator.agents import claim_name, register_agent, resolve_identity

    root = tmp_path / "projects"
    offices = tmp_path / "seats"
    office = offices / "outrider"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "outriderhouse"\n')
    (office / "CLAUDE.md").write_text("# outrider — seat office\n\nplain, never compiled.\n")
    _transcript(root, str(office))

    ident = resolve_identity(cwd=str(office), job_dir=str(tmp_path / "jobs" / SID[:8]), root=root)
    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "Outrider", source=ident.agent_id)

    out = await automount(actions, session_id=SID, cwd=str(office), actor="analyst:operator",
                          root=root, jobs_home=tmp_path / "jobs", office_root=offices,
                          source="startup")
    anchor = out.get("identity_anchor")
    assert anchor is not None
    assert anchor["charter_file"] == f"{office}/CLAUDE.md"
    assert "compiled_office" not in anchor


async def test_succession_owner_match_finds_a_specific_incarnation_never_a_rebase_decoy(
    actions: Actions, tmp_path: Path
) -> None:
    """Thoth LI's optional tightening (msg 861): 'owned by this project' must also match a
    SPECIFIC incarnation's id ('agent:base-iii'), not only the bare project name or bare
    lineage base — real obligations are filed that way. But the wide SQL prefilter
    (LIKE base||'%') is unsafe alone — the deckard-rebase trap: an UNRELATED lineage that
    merely shares the hash prefix ('agent:base-extra-ii') must never be picked over (or
    instead of) the genuine match, however recent it is."""
    from src.orchestrator.agents import claim_name, register_agent, resolve_identity
    from src.orchestrator.capture import open_thread

    root = tmp_path / "projects"
    offices = tmp_path / "seats"
    office = offices / "warden"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "wardenhouse"\n')
    _transcript(root, str(office))

    ident = resolve_identity(cwd=str(office), job_dir=str(tmp_path / "jobs" / SID[:8]), root=root)
    assert ident.project == "wardenhouse"
    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "Warden", source=ident.agent_id)
    base = ident.agent_id  # generation 1, no numeral suffix
    await automount(actions, session_id=SID, cwd=str(office), actor="analyst:operator",
                    root=root, jobs_home=tmp_path / "jobs", office_root=offices,
                    source="startup")

    # older, but owned by a SPECIFIC past incarnation of this exact lineage — must match
    await open_thread(actions, "owned by a specific past incarnation, not the bare base",
                      repo="wardenhouse", kind="obligation", owner=f"{base}-iii")
    # newer, but an UNRELATED lineage that only shares the hash prefix — must NOT match
    await open_thread(actions, "a decoy from an unrelated rebased lineage",
                      repo="wardenhouse", kind="obligation", owner=f"{base}extra-ii")

    reborn = await automount(actions, session_id=SID, cwd=str(office),
                             actor="analyst:operator", root=root,
                             jobs_home=tmp_path / "jobs", office_root=offices,
                             source="compact")
    succ = reborn.get("succession")
    assert succ is not None
    assert "owned by a specific past incarnation" in succ["thread_summary"]
    assert "decoy" not in succ["thread_summary"]


async def test_the_whisper_carries_the_handoff_specifically(
    actions: Actions, tmp_path: Path,
) -> None:
    """Thread e749036e (Thoth LX): the whisper's succession steering finds the newest OPEN
    obligation — deliberately never claimed to be a handoff (Thoth LI's amend, msg 861).
    This is the OTHER half: a REAL handoff (is_handoff='true') rides alongside it under its
    own `handoff` key, found via the same bounded chain-walk orient() uses — never
    confused with an ordinary fresh-but-unrelated open thread."""
    from src.orchestrator.agents import claim_name, register_agent, resolve_identity
    from src.orchestrator.capture import open_thread

    root = tmp_path / "projects"
    offices = tmp_path / "seats"
    office = offices / "envoy"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "envoyhouse"\n')
    _transcript(root, str(office))

    ident = resolve_identity(cwd=str(office), job_dir=str(tmp_path / "jobs" / SID[:8]), root=root)
    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "Envoy", source=ident.agent_id)
    await automount(actions, session_id=SID, cwd=str(office), actor="analyst:operator",
                    root=root, jobs_home=tmp_path / "jobs", office_root=offices,
                    source="startup")

    # an ordinary open obligation — NOT a handoff, must never be reported as one
    await open_thread(actions, "just an ordinary open obligation, unrelated to any handoff",
                      repo="envoyhouse", kind="obligation", owner="envoyhouse")
    # the REAL handoff, structurally marked, sourced by the mind that's about to be superseded
    now = datetime.now(UTC)
    handoff_obj = await actions.create_or_find_object("Decision", "decision:envoy-marker",
                                                       ident.agent_id)
    await actions.assert_property(handoff_obj, "summary", "the estate is settled structurally",
                                  ident.agent_id, now, 0.9, evidence_class="self_declared")
    await actions.assert_property(handoff_obj, "is_handoff", "true", ident.agent_id, now, 0.9,
                                  evidence_class="self_declared")

    reborn = await automount(actions, session_id=SID, cwd=str(office),
                             actor="analyst:operator", root=root,
                             jobs_home=tmp_path / "jobs", office_root=offices,
                             source="compact")
    succ = reborn.get("succession")
    assert succ is not None
    assert "ordinary open obligation" in succ.get("thread_summary", "")
    handoff = succ.get("handoff")
    assert handoff is not None, "the structurally-marked handoff must ride along, separately"
    assert handoff["from"] == ident.agent_id
    assert "estate is settled structurally" in " ".join(n["text"] for n in handoff["notes"])
