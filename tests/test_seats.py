"""The identity core, Phase A (ruling 5cef856b) — the Seat object + the attach ceremony.

Every test here demonstrates a rule of the ceremony against the bug that demanded it:
identity keyed on ephemeral facts (session, path) and RECONSTRUCTED by inference at every
door was the collision class (2294e95d: a mind mounted into a SIBLING's seat; this session's
own triple-mint boot). A Seat is minted once, exists before its first session, and a session
ATTACHES with a one-time token — refusals loud, nothing written.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.agents import mint_heir
from src.orchestrator.handshake import automount
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import (
    attach_session,
    bind_holder,
    ensure_seat,
    find_seat,
    held_seat,
    mint_attach_token,
    seat_of_mount,
)


async def _seated_agent(actions: Actions, agent_id: str, job_dir: str,
                        project: str = "osiris") -> None:
    """A registered agent with a durable mount row — the state automount leaves behind
    before the ceremony runs (the binding rides the mount row)."""
    await actions.create_or_find_object("Agent", agent_id, agent_id)
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent_id, project=project,
                     cwd="/w/osiris", model="claude-fable-5", session_key=None)


async def _active_holds(actions: Actions, agent_id: str, seat_id: str) -> bool:
    return bool(await actions.pool.fetchval(
        "SELECT 1 FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id "
        "WHERE f.canonical=$1 AND t.canonical=$2 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent_id, seat_id))


async def test_ensure_seat_mints_once_and_is_idempotent(actions: Actions) -> None:
    first = await ensure_seat(actions, house="osiris", handle="Thoth", source="test")
    assert first["minted"] is True
    assert first["seat_id"].startswith("seat:")
    again = await ensure_seat(actions, house="osiris", handle="thoth", source="test")
    assert again["minted"] is False                      # case-insensitive find
    assert again["seat_id"] == first["seat_id"]
    # a seat is OF a house: the same handle elsewhere is a different seat
    other = await ensure_seat(actions, house="bytebye", handle="Thoth", source="test")
    assert other["minted"] is True
    assert other["seat_id"] != first["seat_id"]
    assert await find_seat(actions.pool, house="osiris", handle="Thoth") == first["seat_id"]


async def test_attach_binds_the_session_and_links_the_holder(actions: Actions) -> None:
    seat = await ensure_seat(actions, house="osiris", handle="Anna", source="test")
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"],
                                    minted_by="osiris-manager")
    await _seated_agent(actions, "agent:aaaa0001", "/jobs/aaaa0001")

    out = await attach_session(actions, seat_id=seat["seat_id"], token=token,
                               job_dir="/jobs/aaaa0001", agent_id="agent:aaaa0001")

    assert out["attached"] == seat["seat_id"]
    assert out["handle"] == "Anna"
    assert out["resumed"] is False
    assert await seat_of_mount(actions.pool, job_dir="/jobs/aaaa0001") == seat["seat_id"]
    assert await _active_holds(actions, "agent:aaaa0001", seat["seat_id"])
    used = await actions.pool.fetchrow(
        "SELECT used_by, used_at FROM seat_tokens WHERE token=$1", token)
    assert used is not None and used["used_by"] == "/jobs/aaaa0001"
    assert used["used_at"] is not None


async def test_attach_refuses_a_used_token_from_another_session_loudly(
    actions: Actions,
) -> None:
    """The env-inheritance leak (0344e536) and the collision (2294e95d ask #1) in one rule:
    a one-time token binds to its FIRST presenter; a second session is refused LOUDLY and
    nothing is written."""
    seat = await ensure_seat(actions, house="osiris", handle="Maat", source="test")
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    await _seated_agent(actions, "agent:bbbb0001", "/jobs/bbbb0001")
    await _seated_agent(actions, "agent:bbbb0002", "/jobs/bbbb0002")
    first = await attach_session(actions, seat_id=seat["seat_id"], token=token,
                                 job_dir="/jobs/bbbb0001", agent_id="agent:bbbb0001")
    assert first["attached"] == seat["seat_id"]

    second = await attach_session(actions, seat_id=seat["seat_id"], token=token,
                                  job_dir="/jobs/bbbb0002", agent_id="agent:bbbb0002")

    assert "REFUSED" in second["error"] and "another session" in second["error"]
    # nothing was written: the intruder's row stays unbound, the holder's binding stands
    assert await seat_of_mount(actions.pool, job_dir="/jobs/bbbb0002") is None
    assert await seat_of_mount(actions.pool, job_dir="/jobs/bbbb0001") == seat["seat_id"]
    assert await _active_holds(actions, "agent:bbbb0001", seat["seat_id"])
    assert not await _active_holds(actions, "agent:bbbb0002", seat["seat_id"])


async def test_attach_same_presenter_resumes_idempotently(actions: Actions) -> None:
    """Same presenter re-presenting is a RESUME (claude --resume re-fires the whisper with
    the same session id → same job_dir), never an intruder."""
    seat = await ensure_seat(actions, house="osiris", handle="Ra", source="test")
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    await _seated_agent(actions, "agent:cccc0001", "/jobs/cccc0001")
    await attach_session(actions, seat_id=seat["seat_id"], token=token,
                         job_dir="/jobs/cccc0001", agent_id="agent:cccc0001")

    again = await attach_session(actions, seat_id=seat["seat_id"], token=token,
                                 job_dir="/jobs/cccc0001", agent_id="agent:cccc0001")

    assert again["attached"] == seat["seat_id"]
    assert again["resumed"] is True
    # one active holds link, not a stack
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id "
        "WHERE f.canonical='agent:cccc0001' AND t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat["seat_id"])
    assert n == 1


async def test_attach_refuses_unknown_token_and_mismatched_seat(actions: Actions) -> None:
    seat_a = await ensure_seat(actions, house="osiris", handle="Aegis", source="test")
    seat_b = await ensure_seat(actions, house="osiris", handle="Atlas", source="test")
    await _seated_agent(actions, "agent:dddd0001", "/jobs/dddd0001")

    unknown = await attach_session(actions, seat_id=seat_a["seat_id"], token="not-a-token",
                                   job_dir="/jobs/dddd0001", agent_id="agent:dddd0001")
    assert "REFUSED" in unknown["error"] and "unknown" in unknown["error"]

    token_a = await mint_attach_token(actions.pool, seat_id=seat_a["seat_id"])
    crossed = await attach_session(actions, seat_id=seat_b["seat_id"], token=token_a,
                                   job_dir="/jobs/dddd0001", agent_id="agent:dddd0001")
    assert "REFUSED" in crossed["error"] and "mismatch" in crossed["error"]
    assert await seat_of_mount(actions.pool, job_dir="/jobs/dddd0001") is None


async def test_attach_refuses_a_dead_seat(actions: Actions) -> None:
    """A token for a seat the graph does not hold as a living object binds nothing —
    a forged or rotted seat id is refused before any write."""
    await _seated_agent(actions, "agent:eeee0001", "/jobs/eeee0001")
    token = await mint_attach_token(actions.pool, seat_id="seat:00000000")
    out = await attach_session(actions, seat_id="seat:00000000", token=token,
                               job_dir="/jobs/eeee0001", agent_id="agent:eeee0001")
    assert "REFUSED" in out["error"] and "living Seat" in out["error"]


async def test_attach_refuses_a_live_held_seat(actions: Actions) -> None:
    """Two minds in one seat is the collision class itself — a seat bound to a session with
    a fresh pulse is not vacant, even to a VALID fresh token."""
    seat = await ensure_seat(actions, house="osiris", handle="Deckard", source="test")
    t1 = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    await _seated_agent(actions, "agent:ffff0001", "/jobs/ffff0001")
    await attach_session(actions, seat_id=seat["seat_id"], token=t1,
                         job_dir="/jobs/ffff0001", agent_id="agent:ffff0001")

    t2 = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    await _seated_agent(actions, "agent:ffff0002", "/jobs/ffff0002")
    out = await attach_session(actions, seat_id=seat["seat_id"], token=t2,
                               job_dir="/jobs/ffff0002", agent_id="agent:ffff0002")

    assert "REFUSED" in out["error"] and "held LIVE" in out["error"]
    assert await seat_of_mount(actions.pool, job_dir="/jobs/ffff0002") is None
    # the unused token survives, unclaimed — refusal wrote nothing
    row = await actions.pool.fetchrow(
        "SELECT used_at FROM seat_tokens WHERE token=$1", t2)
    assert row is not None and row["used_at"] is None


async def test_attach_without_a_mount_row_refuses(actions: Actions) -> None:
    """The binding rides the durable mount row — a session that never mounted has nothing
    to bind to, and the ceremony says so instead of inventing a row."""
    seat = await ensure_seat(actions, house="osiris", handle="Khepri", source="test")
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    await actions.create_or_find_object("Agent", "agent:gggg0001", "agent:gggg0001")
    out = await attach_session(actions, seat_id=seat["seat_id"], token=token,
                               job_dir="/jobs/gggg0001", agent_id="agent:gggg0001")
    assert "REFUSED" in out["error"] and "mount" in out["error"]


async def test_the_binding_follows_the_lineage_head(actions: Actions) -> None:
    """The mind layer keeps its seams (a882b334: a swap/compaction mints an heir) — but the
    SEAT must keep pointing at whoever the mind is NOW, or the first compaction after an
    attach strands the seat on a corpse. mint_heir re-links; the old link heals, walkable."""
    seat = await ensure_seat(actions, house="osiris", handle="Anubis", source="test")
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    await _seated_agent(actions, "agent:hhhh0001", "/jobs/hhhh0001")
    await attach_session(actions, seat_id=seat["seat_id"], token=token,
                         job_dir="/jobs/hhhh0001", agent_id="agent:hhhh0001")
    ancestor_oid = await actions.create_or_find_object(
        "Agent", "agent:hhhh0001", "agent:hhhh0001")

    heir, _heir_oid = await mint_heir(actions, "agent:hhhh0001", ancestor_oid,
                                      because="compaction", succession=None,
                                      now=datetime.now(UTC))

    assert await _active_holds(actions, heir, seat["seat_id"])
    assert not await _active_holds(actions, "agent:hhhh0001", seat["seat_id"])
    # ...and the healed link is still THERE (valid_until stamped, never deleted)
    healed = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id "
        "WHERE f.canonical='agent:hhhh0001' AND t.canonical=$1 AND l.type='holds' "
        "AND l.valid_until IS NOT NULL", seat["seat_id"])
    assert healed == 1


async def test_held_seat_is_lineage_aware(actions: Actions) -> None:
    """THE THOTH SEAT-BINDING GAP (2026-07-21, two independent witnesses in one hour — wake()'s
    authorization gate and the mail envelope's handle lookup): a holds link minted for an
    ANCESTOR generation (agent:iiii0001) must still answer for a SUCCESSOR presenting its own
    id (agent:iiii0001-iii) that the ordinary succession path never re-bound — held_seat now
    matches anywhere in the lineage, not just the exact id presented."""
    seat = await ensure_seat(actions, house="osiris", handle="Ptah", source="test")
    await actions.create_or_find_object("Agent", "agent:iiii0001", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:iiii0001")

    # a successor exists in the graph (e.g. a mint_heir path that, for whatever reason, never
    # re-ran follow_binding) but the active link is still sitting on the ancestor
    successor = "agent:iiii0001-iii"
    await actions.create_or_find_object("Agent", successor, "test")

    bound = await held_seat(actions.pool, successor)
    assert bound is not None
    assert bound["seat_id"] == seat["seat_id"] and bound["handle"] == "Ptah"

    # NEWEST GENERATION WINS when more than one survives un-healed (the same tiebreak
    # follow_binding uses): bind a later generation too, DIRECTLY (create_link, not
    # bind_holder) so the ancestor's link is never healed — simulating exactly the
    # un-healed state this fix exists to read past.
    later = "agent:iiii0001-v"
    other_seat = await ensure_seat(actions, house="osiris", handle="Ptah2", source="test")
    later_oid = await actions.create_or_find_object("Agent", later, "test")
    other_seat_oid = await actions.create_or_find_object("Seat", other_seat["seat_id"], "test")
    await actions.create_link(later_oid, other_seat_oid, "holds", "test", datetime.now(UTC), 0.9)
    newest = await held_seat(actions.pool, "agent:iiii0001-vii")  # asks about a THIRD generation
    assert newest is not None and newest["seat_id"] == other_seat["seat_id"]  # -v outranks -i


SID = "af00c0de-0000-4000-8000-000000000000"


def _transcript(root: Path, cwd: str, model: str = "claude-fable-5") -> None:
    proj = root / cwd.replace("/", "-")
    proj.mkdir(parents=True, exist_ok=True)
    (proj / f"{SID}.jsonl").write_text(json.dumps(
        {"type": "assistant", "cwd": cwd,
         "message": {"model": model, "content": [{"type": "text", "text": "hi"}]}}) + "\n")


async def test_automount_carries_the_ceremony(actions: Actions, tmp_path: Path) -> None:
    """The whisper's server half runs the full ceremony: env-exported seat + token arrive
    with the hook payload, the session mounts AND binds in one breath — identity at birth."""
    root = tmp_path / "projects"
    _transcript(root, "/w/osiris")
    seat = await ensure_seat(actions, house="osiris", handle="Horus", source="test")
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"],
                                    minted_by="osiris-manager")

    out = await automount(actions, session_id=SID, cwd="/w/osiris",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                          seat_id=seat["seat_id"], attach_token=token)

    assert out["attach"]["attached"] == seat["seat_id"]
    assert out["attach"]["handle"] == "Horus"
    job_dir = out["job_dir"]
    assert await seat_of_mount(actions.pool, job_dir=job_dir) == seat["seat_id"]
    assert await _active_holds(actions, out["agent"], seat["seat_id"])


async def test_automount_attach_refusal_is_loud_but_the_mount_stands(
    actions: Actions, tmp_path: Path,
) -> None:
    """A refused attach must never cost the session its mount: the ceremony degrades to
    exactly the inferred path, plus a confession the whisper prints (never silence)."""
    root = tmp_path / "projects"
    _transcript(root, "/w/osiris")
    seat = await ensure_seat(actions, house="osiris", handle="Sobek", source="test")

    out = await automount(actions, session_id=SID, cwd="/w/osiris",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                          seat_id=seat["seat_id"], attach_token="forged-or-stale")

    assert "REFUSED" in out["attach"]["error"]
    assert out["agent"] == f"agent:{SID[:8]}"     # mounted anyway, on the inferred path
    assert await seat_of_mount(actions.pool, job_dir=out["job_dir"]) is None


# --- Phase B1: the binding outranks the inference ---


async def test_resolve_seat_prefers_the_holds_binding_over_a_hotter_grave(
    actions: Actions,
) -> None:
    """The grave-delivery shape, killed at the root: an old generation with a HOT mount row
    outranked the true holder under the assertion path's liveness ranking. A declared binding
    is not a guess — the bound holder wins outright."""
    from src.orchestrator.agents import resolve_seat

    seat = await ensure_seat(actions, house="osiris", handle="Payne", source="test")
    # the impostor: carries the handle ASSERTION and a live pulse (the hotter grave)
    from datetime import datetime as _dt
    now = _dt.now(UTC)
    ghost = await actions.create_or_find_object("Agent", "agent:iiii0001", "agent:iiii0001")
    await actions.assert_property(ghost, "handle", "Payne", "agent:iiii0001", now, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/jobs/iiii0001", agent_id="agent:iiii0001",
                     project="osiris", cwd="/w", model=None, session_key=None)
    # the true holder: bound via the ceremony, mount released (not live)
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    await _seated_agent(actions, "agent:iiii0002", "/jobs/iiii0002")
    await attach_session(actions, seat_id=seat["seat_id"], token=token,
                         job_dir="/jobs/iiii0002", agent_id="agent:iiii0002")

    out = await resolve_seat(actions, "Payne")

    assert out["agent"] == "agent:iiii0002"       # the binding, not the hotter assertion
    assert out["seat_id"] == seat["seat_id"]
    assert out["live"] is True                    # its mount row is fresh (just saved)


async def test_resolve_seat_falls_back_to_the_assertion_path(actions: Actions) -> None:
    """No Seat object, no binding — the assertion path answers exactly as before (every
    un-seated lineage keeps resolving; Phase B changes nothing for them)."""
    from src.orchestrator.agents import resolve_seat

    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:jjjj0001", "agent:jjjj0001")
    await actions.assert_property(a, "handle", "Unbound", "agent:jjjj0001", now, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/jobs/jjjj0001", agent_id="agent:jjjj0001",
                     project="osiris", cwd="/w", model=None, session_key=None)
    out = await resolve_seat(actions, "Unbound")
    assert out["agent"] == "agent:jjjj0001"
    assert "seat_id" not in out


async def test_resolve_seat_ambiguous_handle_falls_back(actions: Actions) -> None:
    """Two houses, one handle: the seat world has no unique answer — the assertion path's
    liveness ranking arbitrates instead of a coin-flip between seats."""
    from src.orchestrator.agents import resolve_seat

    await ensure_seat(actions, house="alpha", handle="Twin", source="test")
    await ensure_seat(actions, house="beta", handle="Twin", source="test")
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:kkkk0001", "agent:kkkk0001")
    await actions.assert_property(a, "handle", "Twin", "agent:kkkk0001", now, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/jobs/kkkk0001", agent_id="agent:kkkk0001",
                     project="alpha", cwd="/w", model=None, session_key=None)
    out = await resolve_seat(actions, "Twin")
    assert out["agent"] == "agent:kkkk0001"       # assertion path answered
    assert "seat_id" not in out


async def test_a_vacant_seat_never_blocks_resolution(actions: Actions) -> None:
    """A Seat with no active holder contributes nothing — the assertion path answers."""
    from src.orchestrator.agents import resolve_seat

    await ensure_seat(actions, house="osiris", handle="Vacant", source="test")
    out = await resolve_seat(actions, "Vacant")
    assert out["agent"] is None                   # nobody anywhere — honest empty


# --- Phase B4: the hand-resume follows the seat ---


async def test_reseed_binding_restores_a_session_ended_binding(actions: Actions) -> None:
    """session_end deletes the mount row (the binding's hot half); the holds link is the
    durable half. A fresh row for the same mind re-earns seat_id from the link."""
    from src.orchestrator.mounts import release_mounts
    from src.orchestrator.seats import reseed_binding

    seat = await ensure_seat(actions, house="osiris", handle="Osar", source="test")
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    await _seated_agent(actions, "agent:llll0001", "/jobs/llll0001")
    await attach_session(actions, seat_id=seat["seat_id"], token=token,
                         job_dir="/jobs/llll0001", agent_id="agent:llll0001")
    await release_mounts(actions.pool, "agent:llll0001")          # the tab closed
    assert await seat_of_mount(actions.pool, job_dir="/jobs/llll0001") is None

    # the hand-resume: a fresh row, no token anywhere
    await save_mount(actions.pool, job_dir="/jobs/llll0001", agent_id="agent:llll0001",
                     project="osiris", cwd="/w/osiris", model=None, session_key=None)
    got = await reseed_binding(actions.pool, agent_id="agent:llll0001",
                               job_dir="/jobs/llll0001")

    assert got == seat["seat_id"]
    assert await seat_of_mount(actions.pool, job_dir="/jobs/llll0001") == seat["seat_id"]
    # timid: a row that already carries a binding is never overwritten
    other = await ensure_seat(actions, house="osiris", handle="Osar2", source="test")
    assert other["seat_id"] != seat["seat_id"]
    again = await reseed_binding(actions.pool, agent_id="agent:llll0001",
                                 job_dir="/jobs/llll0001")
    assert again == seat["seat_id"]               # still the held seat, row untouched


async def test_automount_reseeds_on_hand_resume(actions: Actions, tmp_path: Path) -> None:
    """End to end: attach at birth, session ends, the SAME session hand-resumes with no env —
    the whisper payload carries the re-earned binding."""
    from src.orchestrator.handshake import session_end

    root = tmp_path / "projects"
    _transcript(root, "/w/osiris")
    seat = await ensure_seat(actions, house="osiris", handle="Ptah", source="test")
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    born = await automount(actions, session_id=SID, cwd="/w/osiris",
                           actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs",
                           seat_id=seat["seat_id"], attach_token=token)
    assert born["attach"]["attached"] == seat["seat_id"]
    await session_end(actions, session_id=SID, jobs_home=tmp_path / "jobs")

    resumed = await automount(actions, session_id=SID, cwd="/w/osiris",
                              actor="analyst:operator", root=root,
                              jobs_home=tmp_path / "jobs", source="resume")

    assert resumed.get("attach") is None                      # no token this time
    assert resumed["seat_binding"] == seat["seat_id"]         # re-earned from the link
    assert await seat_of_mount(actions.pool, job_dir=resumed["job_dir"]) == seat["seat_id"]


async def test_session_end_releases_only_the_ending_door(
    actions: Actions, tmp_path: Path
) -> None:
    """THE DOOR-SCOPED RELEASE (the g40-v/g40-vi false-succession incident, 2026-07-17):
    SessionEnd released EVERY row of the ending session's agent — so one closing
    tab-view deleted the LIVING session's anchor, the emptied registry read as the
    seat's death, and the office door minted false successors at thoth's own office.
    Only the ending door's own rows may go; the seat-wide release is retire()'s."""
    from src.orchestrator.handshake import session_end
    from src.orchestrator.mounts import save_mount

    await save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "11ab5001"),
                     agent_id="agent:11ab5001", project="p", cwd="/w/p",
                     model=None, session_key="whisper:11ab5001")
    tab_sid = "beef7777-0000-4000-8000-000000000000"
    await save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "beef7777"),
                     agent_id="agent:11ab5001", project="p", cwd="/w/p",
                     model=None, session_key="view-of:11ab5001")

    out = await session_end(actions, session_id=tab_sid, jobs_home=tmp_path / "jobs")

    assert out["released"] == 1                       # the tab's own row and nothing else
    assert await actions.pool.fetchval(
        "SELECT agent_id FROM agent_mounts WHERE job_dir=$1",
        str(tmp_path / "jobs" / "11ab5001")) == "agent:11ab5001"   # the living anchor STANDS

    # a resume's binding rides its ANCESTOR's job_dir, keyed sid:<its own id> — the
    # resumed session's own end must find and release that row too
    res_sid = "c0de9999-0000-4000-8000-000000000000"
    await save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "0e1d0b99"),
                     agent_id="agent:11ab5001", project="p", cwd="/w/p",
                     model=None, session_key="sid:" + res_sid.replace("-", ""))
    out2 = await session_end(actions, session_id=res_sid, jobs_home=tmp_path / "jobs")
    assert out2["released"] == 1


# --- Phase B3: the binding is part of who you are ---


async def test_seat_bearings_carries_the_binding(actions: Actions) -> None:
    """orient/mount tell a bound mind WHICH ROLE it sits in — even one that never
    claim_named itself in the assertion world (attached at birth, still anonymous)."""
    from src.orchestrator.agents import seat_bearings

    seat = await ensure_seat(actions, house="osiris", handle="Nefer", source="test")
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    await _seated_agent(actions, "agent:mmmm0001", "/jobs/mmmm0001")
    await attach_session(actions, seat_id=seat["seat_id"], token=token,
                         job_dir="/jobs/mmmm0001", agent_id="agent:mmmm0001")

    out = await seat_bearings(actions.pool, "agent:mmmm0001")

    assert out["seat_binding"]["seat_id"] == seat["seat_id"]
    assert out["seat_binding"]["handle"] == "Nefer"
    assert out["seat_binding"]["house"] == "osiris"

    # an unbound mind sees no phantom binding
    await _seated_agent(actions, "agent:mmmm0002", "/jobs/mmmm0002")
    bare = await seat_bearings(actions.pool, "agent:mmmm0002")
    assert "seat_binding" not in bare


# --- Phase B2: the seat is the address; the holder is the reader ---


async def _bound(actions: Actions, handle: str, agent: str, jobs: str) -> dict:
    seat = await ensure_seat(actions, house="osiris", handle=handle, source="test")
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    await _seated_agent(actions, agent, jobs)
    await attach_session(actions, seat_id=seat["seat_id"], token=token,
                         job_dir=jobs, agent_id=agent)
    return seat


async def test_dm_by_name_to_a_bound_seat_stores_the_seat_address(actions: Actions) -> None:
    """A name that resolves through a BINDING stores the SEAT as the address — it survives
    every succession, so the mail reaches whoever holds the seat at READ time. The receipt
    names the seat, its current holder, and the holder's lineage head."""
    from src.orchestrator.mailbox import send_message

    seat = await _bound(actions, "Iris", "agent:nnnn0001", "/jobs/nnnn0001")
    out = await send_message(actions.pool, from_agent="agent:nnnn0099",
                             from_project="elsewhere", to_agent="Iris",
                             body="to the role, not the mind", require_seat=True)

    assert out["to_agent"] == seat["seat_id"]
    assert out["seat"] == "Iris"
    assert out["holder"] == "agent:nnnn0001"
    stored = await actions.pool.fetchval(
        "SELECT to_agent FROM fleet_messages WHERE id=$1", out["id"])
    assert stored == seat["seat_id"]


async def test_seat_mail_reaches_the_holder_and_only_the_holder(actions: Actions) -> None:
    from src.orchestrator.mailbox import ack_messages, read_inbox, send_message, unread_count

    await _bound(actions, "Osec", "agent:oooo0001", "/jobs/oooo0001")
    await _seated_agent(actions, "agent:oooo0002", "/jobs/oooo0002")  # same project, unbound
    out = await send_message(actions.pool, from_agent="agent:oooo0099",
                             from_project="elsewhere", to_agent="Osec", body="for the seat")

    assert await unread_count(actions.pool, "osiris", reader_agent="agent:oooo0001") == 1
    assert await unread_count(actions.pool, "osiris", reader_agent="agent:oooo0002") == 0
    got = await read_inbox(actions.pool, "osiris", reader_agent="agent:oooo0001")
    assert [m["id"] for m in got] == [out["id"]]
    assert got[0]["dm"] is True
    assert (await ack_messages(actions.pool, "osiris", [out["id"]],
                               reader_agent="agent:oooo0001"))["settled"] == [out["id"]]
    assert await unread_count(actions.pool, "osiris", reader_agent="agent:oooo0001") == 0


async def test_seat_mail_survives_succession_without_estate_transfer(
    actions: Actions,
) -> None:
    """THE POINT OF B2: a seat DM unread at the holder's death reaches the heir with NO
    re-addressing — the row never changes; the heir holds the seat, therefore the heir
    matches. mint_heir's estate-transfer UPDATE keeps covering agent-id mail only."""
    from src.orchestrator.mailbox import read_inbox, send_message, unread_count

    seat = await _bound(actions, "Sekhmet", "agent:pppp0001", "/jobs/pppp0001")
    out = await send_message(actions.pool, from_agent="agent:pppp0099",
                             from_project="elsewhere", to_agent="Sekhmet", body="in flight")
    ancestor_oid = await actions.create_or_find_object(
        "Agent", "agent:pppp0001", "agent:pppp0001")

    heir, _ = await mint_heir(actions, "agent:pppp0001", ancestor_oid,
                              because="compaction", succession=None,
                              now=datetime.now(UTC))
    await save_mount(actions.pool, job_dir="/jobs/pppp0001", agent_id=heir,
                     project="osiris", cwd="/w/osiris", model=None, session_key=None)

    assert await unread_count(actions.pool, "osiris", reader_agent=heir) == 1
    got = await read_inbox(actions.pool, "osiris", reader_agent=heir)
    assert [m["id"] for m in got] == [out["id"]]
    stored = await actions.pool.fetchval(
        "SELECT to_agent FROM fleet_messages WHERE id=$1", out["id"])
    assert stored == seat["seat_id"]      # the row NEVER changed — no estate transfer ran


async def test_raw_seat_address_is_an_act_of_intent(actions: Actions) -> None:
    from src.orchestrator.mailbox import send_message

    seat = await _bound(actions, "Wadjet", "agent:qqqq0001", "/jobs/qqqq0001")
    out = await send_message(actions.pool, from_agent="agent:qqqq0099",
                             from_project="elsewhere", to_agent=seat["seat_id"],
                             body="raw seat address")
    assert out["to_agent"] == seat["seat_id"]
    assert out["holder"] == "agent:qqqq0001"

    import pytest
    with pytest.raises(ValueError, match="no such seat"):
        await send_message(actions.pool, from_agent="agent:qqqq0099",
                           from_project="elsewhere", to_agent="seat:deadbeef",
                           body="into the void")


async def test_reply_to_a_seat_dm_routes_back_and_settles(actions: Actions) -> None:
    """The holder replying to seat mail: routes back to the sender as a DM (a DM 'to me'
    includes a seat I hold) and settles the referenced message for the replier."""
    from src.orchestrator.mailbox import send_message, unread_count

    await _bound(actions, "Bastet", "agent:rrrr0001", "/jobs/rrrr0001")
    dm = await send_message(actions.pool, from_agent="agent:rrrr0099",
                            from_project="elsewhere", to_agent="Bastet", body="question")
    assert await unread_count(actions.pool, "osiris", reader_agent="agent:rrrr0001") == 1

    reply = await send_message(actions.pool, from_agent="agent:rrrr0001",
                               from_project="osiris", reply_to=dm["id"], body="answer")

    assert reply["to_agent"] == "agent:rrrr0099"          # routed back to the sender
    assert reply["thread_id"] == dm["id"]                 # joined the thread
    assert await unread_count(actions.pool, "osiris",
                              reader_agent="agent:rrrr0001") == 0  # the reply IS the ack


async def test_an_unbound_name_keeps_snapshot_addressing(actions: Actions) -> None:
    """No Seat object → the live holder's AGENT id is stored, exactly as before (ruling
    1e02e069's snapshot semantics; the estate transfer still covers these)."""
    from src.orchestrator.mailbox import send_message

    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:ssss0001", "agent:ssss0001")
    await actions.assert_property(a, "handle", "Plain", "agent:ssss0001", now, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/jobs/ssss0001", agent_id="agent:ssss0001",
                     project="osiris", cwd="/w", model=None, session_key=None)
    out = await send_message(actions.pool, from_agent="agent:ssss0099",
                             from_project="elsewhere", to_agent="Plain", body="old path")
    assert out["to_agent"] == "agent:ssss0001"
    assert "holder" not in out


# --- Phase C: a visitor may never claim, hold, count as, or resolve as a seat (§4.3) ---


async def _visitor(actions: Actions, child: str, parent: str) -> None:
    """The fixture shape alfred donated (ce348dc5/42bf712d, dead builder-orphans): a spawn
    in the house — but WITH its spawned_by edge, the shape the stamped path now records."""
    from datetime import datetime as _dt
    now = _dt.now(UTC)
    p = await actions.create_or_find_object("Agent", parent, parent)
    c = await actions.create_or_find_object("Agent", child, child)
    await actions.create_link(c, p, "spawned_by", child, now, 0.9,
                              evidence_class="self_declared")
    await actions.assert_property(c, "project", "osiris", child, now, 0.9,
                                  evidence_class="self_declared")


async def test_a_visitor_may_not_claim_a_seat(actions: Actions) -> None:
    from src.orchestrator.agents import claim_name

    await _visitor(actions, "agent:tttt0002", "agent:tttt0001")
    out = await claim_name(actions, "agent:tttt0002", "Builder", source="agent:tttt0002")
    assert "VISITOR" in out["error"] and "agent:tttt0001" in out["error"]


async def test_a_visitor_never_resolves_or_counts_as_a_holder(actions: Actions) -> None:
    """The old leak, replayed against the guards: a spawn wearing a handle assertion and a
    hot mount row must not answer to the name, and must not renumber the seat's history."""
    from src.orchestrator.agents import resolve_seat, seat_holders

    await _visitor(actions, "agent:uuuu0002", "agent:uuuu0001")
    now = datetime.now(UTC)
    c = await actions.create_or_find_object("Agent", "agent:uuuu0002", "agent:uuuu0002")
    await actions.assert_property(c, "handle", "Leaked", "agent:uuuu0002", now, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/jobs/uuuu0002", agent_id="agent:uuuu0002",
                     project="osiris", cwd="/w", model=None, session_key=None)

    resolved = await resolve_seat(actions, "Leaked")
    assert resolved["agent"] is None                 # a leak, not a holder — honest empty
    holders = await seat_holders(actions.pool, "osiris", "Leaked")
    assert holders == []                             # and it never renumbers history


async def test_attach_refuses_a_visitor(actions: Actions) -> None:
    """A sidechain inherits its parent's environment — if it somehow presents a fresh valid
    token first, the ceremony still refuses: a sub-agent never holds a seat."""
    await _visitor(actions, "agent:vvvv0002", "agent:vvvv0001")
    seat = await ensure_seat(actions, house="osiris", handle="Sphinx", source="test")
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    await save_mount(actions.pool, job_dir="/jobs/vvvv0002", agent_id="agent:vvvv0002",
                     project="osiris", cwd="/w", model=None, session_key=None)

    out = await attach_session(actions, seat_id=seat["seat_id"], token=token,
                               job_dir="/jobs/vvvv0002", agent_id="agent:vvvv0002")

    assert "REFUSED" in out["error"] and "VISITOR" in out["error"]
    assert await seat_of_mount(actions.pool, job_dir="/jobs/vvvv0002") is None
    unused = await actions.pool.fetchrow(
        "SELECT used_at FROM seat_tokens WHERE token=$1", token)
    assert unused is not None and unused["used_at"] is None   # refusal wrote nothing


# --- Phase D: a seated sid is never guessable ---


async def test_a_seated_sid_is_claimed_even_when_stale(actions: Actions) -> None:
    """The cwd-guess refuses sids held by LIVE mounts; Phase D extends the claim to SEATED
    rows regardless of pulse — a holder dying must never make its identity guessable by an
    anchorless stranger reading the hottest transcript. session_end releases the row, so a
    deliberately-closed seat frees its sid the honest way."""
    from src.orchestrator.mounts import live_claimed_sids, release_mounts

    seat = await _bound(actions, "Duat", "agent:aead0001", "/jobs/aead0001")
    assert seat["seat_id"]
    # kill the pulse: the holder has been silent for an hour
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour' "
        "WHERE job_dir='/jobs/aead0001'")

    claimed = await live_claimed_sids(actions.pool, exclude_session_key=None,
                                      within_secs=900)
    assert "aead0001" in claimed            # stale, but SEATED — still claimed

    # an unseated stale row frees its sid exactly as before
    await _seated_agent(actions, "agent:aead0002", "/jobs/aead0002")
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour' "
        "WHERE job_dir='/jobs/aead0002'")
    claimed = await live_claimed_sids(actions.pool, exclude_session_key=None,
                                      within_secs=900)
    assert "aead0002" not in claimed

    # ...and the honest release frees even a seated sid
    await release_mounts(actions.pool, "agent:aead0001")
    claimed = await live_claimed_sids(actions.pool, exclude_session_key=None,
                                      within_secs=900)
    assert "aead0001" not in claimed


# --- the claim_name on-ramp (the designed-but-unshipped half, caught in the pilot) ---


async def test_claim_name_mints_and_binds_the_seat_object(actions: Actions) -> None:
    """A successful claim is the assertion world's own deliberate binding act: the Seat
    OBJECT mints (or is found) and the claimer becomes its active holder — legacy seats
    enter the Seat world the moment they are next claimed."""
    from src.orchestrator.agents import claim_name

    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:xxxx0001", "agent:xxxx0001")
    await actions.assert_property(a, "project", "osiris", "agent:xxxx0001", now, 0.9,
                                  evidence_class="self_declared")
    out = await claim_name(actions, "agent:xxxx0001", "Nut", source="agent:xxxx0001")

    assert out["claimed"] == "Nut"
    assert out["seat_id"].startswith("seat:")
    assert await _active_holds(actions, "agent:xxxx0001", out["seat_id"])
    from src.orchestrator.seats import binding_of_handle
    bound = await binding_of_handle(actions.pool, "Nut")
    assert bound == {"seat_id": out["seat_id"], "holder": "agent:xxxx0001"}

    # a LATER lineage inheriting the vacant seat re-binds: one active holder, healed history
    b = await actions.create_or_find_object("Agent", "agent:xxxx0002", "agent:xxxx0002")
    await actions.assert_property(b, "project", "osiris", "agent:xxxx0002", now, 0.9,
                                  evidence_class="self_declared")
    again = await claim_name(actions, "agent:xxxx0002", "Nut", source="agent:xxxx0002")
    assert again["seat_id"] == out["seat_id"]          # the SAME durable seat, found not minted
    assert await _active_holds(actions, "agent:xxxx0002", out["seat_id"])
    assert not await _active_holds(actions, "agent:xxxx0001", out["seat_id"])


# ═══ THE ORPHAN-SEAT BACKFILL (thread 749bf530 / occupancy piece C, 9f566244) ═══════════════
# A seat whose original claim predates the Seat-object binding never got a holds link, and
# mint_heir's automatic succession only ever MOVES an existing one — never creates one from
# nothing. These tests simulate exactly that: a Seat object + a handle-asserted Agent, with
# NO holds link between them at all (the pre-binding-era shape), never routed through
# claim_name/bind_holder.


async def _legacy_unbound_seat(
    actions: Actions, *, handle: str, house: str = "osiris", agent_id: str = "agent:zzzz0001",
) -> tuple[str, str]:
    """A Seat object + a live, handle-matching Agent — the pre-binding-era shape backfill
    exists to heal. Returns (seat_id, agent_id)."""
    seat = await ensure_seat(actions, house=house, handle=handle, source="test")
    a = await actions.create_or_find_object("Agent", agent_id, agent_id)
    now = datetime.now(UTC)
    await actions.assert_property(a, "handle", handle, agent_id, now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(a, "project", house, agent_id, now, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir=f"/jobs/{agent_id.split(':')[1]}", agent_id=agent_id,
                     project=house, cwd="/w/osiris", model="claude-sonnet-5", session_key=None)
    return str(seat["seat_id"]), agent_id


async def test_backfill_dry_run_reports_without_writing(actions: Actions) -> None:
    """DRY-RUN IS THE DEFAULT AND WRITES NOTHING (a2cf8405): the plan names the seat and its
    resolvable holder, but no holds link exists afterward — a mutation is never hand-run
    without surfacing the plan first."""
    from src.orchestrator.seats import backfill_unbound_seats

    seat_id, agent_id = await _legacy_unbound_seat(actions, handle="Sekhmet")
    out = await backfill_unbound_seats(actions)  # dry_run=True by default

    assert out["dry_run"] is True and out["bound"] == 0
    row = next(p for p in out["plan"] if p["seat_id"] == seat_id)
    assert row["holder"] == agent_id and row["live"] is True
    assert await held_seat(actions.pool, agent_id) is None  # still unbound — nothing written


async def test_backfill_apply_binds_the_resolved_holder(actions: Actions) -> None:
    """dry_run=False actually binds — the same act claim_name's own tail performs, run in
    bulk for seats whose current holder will never call it unprompted."""
    from src.orchestrator.seats import backfill_unbound_seats

    seat_id, agent_id = await _legacy_unbound_seat(actions, handle="Anubis")
    out = await backfill_unbound_seats(actions, dry_run=False)

    assert out["bound"] == 1
    bound = await held_seat(actions.pool, agent_id)
    assert bound is not None and bound["seat_id"] == seat_id and bound["handle"] == "Anubis"


async def test_backfill_is_idempotent(actions: Actions) -> None:
    """A second pass — whether re-run deliberately or because a seat someone fixed by hand
    meanwhile is now bound — finds nothing left to do and changes nothing."""
    from src.orchestrator.seats import backfill_unbound_seats

    await _legacy_unbound_seat(actions, handle="Bastet")
    first = await backfill_unbound_seats(actions, dry_run=False)
    assert first["bound"] == 1

    second = await backfill_unbound_seats(actions, dry_run=False)
    assert second["total_unbound"] == 0 and second["bound"] == 0


async def test_backfill_never_guesses_an_unresolvable_seat(actions: Actions) -> None:
    """A seat with no live (or any) handle-matching Agent is reported, not guessed — apply
    must not crash or bind a stranger to it."""
    from src.orchestrator.seats import backfill_unbound_seats

    seat = await ensure_seat(actions, house="osiris", handle="Wepwawet", source="test")
    out = await backfill_unbound_seats(actions, dry_run=False)

    row = next(p for p in out["plan"] if p["seat_id"] == seat["seat_id"])
    assert row["holder"] is None and "note" in row
    assert out["bound"] == 0


async def test_backfill_only_seats_scopes_the_write(actions: Actions) -> None:
    """THE OPERATOR'S STAGED ROLLOUT (2026-07-21: Thoth-first, fleet-wide only after that
    lands clean) — only_seats restricts both the plan and the write to exactly the named
    seats; every other unbound seat is counted in total_unbound but never appears in `plan`
    and is never touched, so a scoped apply cannot spill onto seats nobody signed off on."""
    from src.orchestrator.seats import backfill_unbound_seats

    scoped_seat, scoped_agent = await _legacy_unbound_seat(
        actions, handle="Osiris1", agent_id="agent:yyyy0001")
    other_seat, other_agent = await _legacy_unbound_seat(
        actions, handle="Osiris2", agent_id="agent:yyyy0002")

    out = await backfill_unbound_seats(actions, dry_run=False, only_seats={scoped_seat})

    assert out["total_unbound"] == 2 and out["scoped_out"] == 1
    assert [p["seat_id"] for p in out["plan"]] == [scoped_seat]
    assert out["bound"] == 1
    assert (await held_seat(actions.pool, scoped_agent) or {}).get("seat_id") == scoped_seat
    assert await held_seat(actions.pool, other_agent) is None  # untouched — out of scope


# ═══ THE HOLE STOPS REGENERATING (Khnum's tail of 9f566244/749bf530) ═══════════════════════
# The backfill above cures every orphan seat that exists TODAY. But mint_heir's automatic
# succession is the one path that fires on every compaction/model-swap/session-death without
# anyone asking — and until now it only ever MOVED an existing holds link (follow_binding),
# never created one from nothing. Left alone, the very next mint of an already-orphaned
# lineage would re-open the identical hole the backfill just closed. These tests exercise
# mint_heir directly against the same pre-binding-era shape _legacy_unbound_seat builds.


async def test_mint_heir_binds_a_never_claimed_orphan_seat(actions: Actions) -> None:
    """An heir minted for a handle that names an EXISTING, still-unbound Seat binds to it as
    part of the mint itself — the same self-heal claim_name performs explicitly, now running
    at the one moment nobody has to think to call it."""
    seat_id, agent_id = await _legacy_unbound_seat(actions, handle="Tefnut")
    assert await held_seat(actions.pool, agent_id) is None       # confirm the orphan shape
    anc = await actions.create_or_find_object("Agent", agent_id, agent_id)

    heir, _heir_oid = await mint_heir(actions, agent_id, anc, because="compaction",
                                      succession=None)

    bound = await held_seat(actions.pool, heir)
    assert bound is not None
    assert bound["seat_id"] == seat_id and bound["handle"] == "Tefnut"


async def test_mint_heir_leaves_an_actively_held_seat_alone(actions: Actions) -> None:
    """The bind-if-unbound path only fires when the seat is TRULY unbound — a lineage whose
    seat is already properly bound (the common case, via claim_name) keeps riding
    follow_binding exactly as before; the new code never double-writes or races it."""
    from src.orchestrator.agents import claim_name

    anc = await actions.create_or_find_object("Agent", "agent:xxww0001", "test")
    await actions.assert_property(anc, "project", "osiris", "test", datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")
    claimed = await claim_name(actions, "agent:xxww0001", "Serqet", source="test")
    seat_id = claimed["seat_id"]

    heir, _heir_oid = await mint_heir(actions, "agent:xxww0001", anc, because="compaction",
                                      succession=None)

    bound = await held_seat(actions.pool, heir)
    assert bound is not None and bound["seat_id"] == seat_id
    assert not await _active_holds(actions, "agent:xxww0001", seat_id)  # healed, not doubled


async def test_mint_heir_never_mints_a_seat_that_never_existed(actions: Actions) -> None:
    """Bind-if-unbound closes a hole that already has a NAME — it must never MINT a new Seat
    object (ensure_seat's own law: minting is deliberate, only at a claim or an attach, never
    an automatic sweep). A handle with no Seat object stays exactly that plain."""
    anc = await actions.create_or_find_object("Agent", "agent:noseat01", "test")
    now = datetime.now(UTC)
    await actions.assert_property(anc, "project", "osiris", "test", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(anc, "handle", "Sobek", "test", now, 0.9,
                                  evidence_class="self_declared")

    heir, _heir_oid = await mint_heir(actions, "agent:noseat01", anc, because="compaction",
                                      succession=None)

    assert await find_seat(actions.pool, house="osiris", handle="Sobek") is None
    assert await held_seat(actions.pool, heir) is None


async def test_mint_heir_orphan_bind_is_idempotent_across_successions(actions: Actions) -> None:
    """A SECOND mint of the now-bound lineage rides follow_binding forward exactly as it
    always has — the orphan-bind fires once, at the mint that actually closes the hole, and
    every later succession is the ordinary case."""
    seat_id, agent_id = await _legacy_unbound_seat(actions, handle="Neith")
    anc = await actions.create_or_find_object("Agent", agent_id, agent_id)
    heir, heir_oid = await mint_heir(actions, agent_id, anc, because="compaction",
                                     succession=None)
    assert (await held_seat(actions.pool, heir) or {}).get("seat_id") == seat_id

    heir2, _heir2_oid = await mint_heir(actions, heir, heir_oid, because="compaction",
                                        succession=None)

    bound2 = await held_seat(actions.pool, heir2)
    assert bound2 is not None and bound2["seat_id"] == seat_id
    assert not await _active_holds(actions, heir, seat_id)  # healed forward, one active link


# ═══ OCCUPANCY — VACANT / OCCUPIED / COLD (occupancy piece B, 9f566244) ═══════════════════
# The acceptance case: Ptah's office once showed four bodies where one lived — a seat with
# no holder at all must read VACANT on its own, distinct from a seat that HAS a holder who
# simply isn't live right now (COLD, the ordinary in-between state, never an alarm).


async def test_occupancy_reads_vacant_for_a_seat_never_held(actions: Actions) -> None:
    """A minted seat nobody has ever attached to — furniture, not yet a body."""
    from src.orchestrator.seats import seat_occupancy

    seat = await ensure_seat(actions, house="osiris", handle="Ptah", source="test")

    occ = await seat_occupancy(actions.pool, seat["seat_id"])
    assert occ == {"state": "vacant", "holder": None, "live": False}


async def test_occupancy_reads_occupied_for_a_live_holder(actions: Actions) -> None:
    """A bound holder with a fresh mount row reads OCCUPIED."""
    from src.orchestrator.seats import seat_occupancy

    seat = await ensure_seat(actions, house="osiris", handle="Anhur", source="test")
    await actions.create_or_find_object("Agent", "agent:live00001", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:live00001")
    await save_mount(actions.pool, job_dir="/jobs/live00001", agent_id="agent:live00001",
                     project="osiris", cwd="/w/osiris", model="claude-sonnet-5",
                     session_key=None)

    occ = await seat_occupancy(actions.pool, seat["seat_id"])
    assert occ == {"state": "occupied", "holder": "agent:live00001", "live": True}


async def test_occupancy_reads_cold_for_a_holder_who_is_not_live(actions: Actions) -> None:
    """A bound holder with NO recent pulse (no mount row at all) reads COLD, not VACANT —
    the seat is not furniture, it is simply between sessions."""
    from src.orchestrator.seats import seat_occupancy

    seat = await ensure_seat(actions, house="osiris", handle="Wadjet", source="test")
    await actions.create_or_find_object("Agent", "agent:cold0001", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:cold0001")

    occ = await seat_occupancy(actions.pool, seat["seat_id"])
    assert occ == {"state": "cold", "holder": "agent:cold0001", "live": False}


async def test_occupancy_reads_cold_after_the_holder_moves_on(actions: Actions) -> None:
    """A seat HELD HISTORICALLY but with no active holder at all (the link healed by
    valid_until and nothing replaced it) is COLD, not VACANT — it has a past, just no
    present holder."""
    from src.orchestrator.seats import seat_occupancy

    seat = await ensure_seat(actions, house="osiris", handle="Nekhbet", source="test")
    await actions.create_or_find_object("Agent", "agent:moved001", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:moved001")
    # heal the only active link, leaving the seat with history but no current holder
    seat_oid = await actions.create_or_find_object("Seat", seat["seat_id"], "test")
    agent_oid = await actions.create_or_find_object("Agent", "agent:moved001", "test")
    await actions.invalidate_link(agent_oid, seat_oid, "holds", "test", datetime.now(UTC))

    occ = await seat_occupancy(actions.pool, seat["seat_id"])
    assert occ == {"state": "cold", "holder": None, "live": False}


async def test_occupancy_is_lineage_aware_like_held_seat(actions: Actions) -> None:
    """A holder bound under an ancestor generation label but LIVE under its successor's
    label still reads OCCUPIED — the same lineage-wide liveness held_seat already uses."""
    from src.orchestrator.seats import seat_occupancy

    seat = await ensure_seat(actions, house="osiris", handle="Serqet", source="test")
    await actions.create_or_find_object("Agent", "agent:gen00001", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:gen00001")
    await save_mount(actions.pool, job_dir="/jobs/gen00001-ii", agent_id="agent:gen00001-ii",
                     project="osiris", cwd="/w/osiris", model="claude-sonnet-5",
                     session_key=None)

    occ = await seat_occupancy(actions.pool, seat["seat_id"])
    assert occ["state"] == "occupied" and occ["live"] is True


async def test_fleet_occupancy_lists_every_active_seat_including_vacant(
    actions: Actions,
) -> None:
    """The batch read fleet() renders from — every active Seat gets a row, vacant ones
    included, so a seat with no body at all is as visible as one with a live holder."""
    from src.orchestrator.seats import fleet_occupancy

    vacant = await ensure_seat(actions, house="osiris", handle="Sopdet", source="test")
    occupied = await ensure_seat(actions, house="osiris", handle="Tefnut2", source="test")
    await actions.create_or_find_object("Agent", "agent:fo000001", "test")
    await bind_holder(actions, seat_id=occupied["seat_id"], agent_id="agent:fo000001")
    await save_mount(actions.pool, job_dir="/jobs/fo000001", agent_id="agent:fo000001",
                     project="osiris", cwd="/w/osiris", model="claude-sonnet-5",
                     session_key=None)

    rows = {r["seat_id"]: r for r in await fleet_occupancy(actions.pool)}

    assert rows[vacant["seat_id"]]["state"] == "vacant"
    assert rows[vacant["seat_id"]]["handle"] == "Sopdet"
    assert rows[occupied["seat_id"]]["state"] == "occupied"
    assert rows[occupied["seat_id"]]["handle"] == "Tefnut2"
    assert rows[occupied["seat_id"]]["holder"] == "agent:fo000001"


# ═══ REACHABILITY (ruling d739d486) — a TRUTHFUL "can I reach this lineage right now?" ═════
# Ra's clean repro: a mail send-receipt refused a fresh successor ("no resumable session —
# never handed to a fresh twin") while the daemon's own job_for on the same lineage held a
# live, resumable job the whole time. A stale disk/DB snapshot infers; job_for reads the
# one place that cannot lag the seam. These tests fake claude_daemon.job_for directly (the
# same monkeypatch shape test_trigger.py's own hermetic fixture uses) — never a live socket.


async def test_reachability_confirms_a_live_daemon_job(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.ingest.harness import claude_daemon
    from src.orchestrator.seats import reachability

    await save_mount(actions.pool, job_dir="/home/t/.claude/jobs/reach001",
                     agent_id="agent:reach0001", project="osiris", cwd="/t",
                     model="claude-sonnet-5", session_key=None)
    job = {"short": "reach001", "sessionId": "reach001-full", "state": "resumable"}

    async def _fake(ids: set[str]) -> dict[str, Any] | None:
        return job if "reach001" in ids else None

    monkeypatch.setattr(claude_daemon, "job_for", _fake)

    out = await reachability(actions.pool, "agent:reach0001")

    assert out == {"reachable": True, "via": "daemon-job", "job": job,
                   "detail": "the daemon's own job state confirms reach001 is live right now"}


async def test_reachability_is_honest_when_the_daemon_is_dark(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dark daemon (no job found) reads unreachable — but the detail says so plainly as
    'couldn't confirm', never as proof the lineage is dead."""
    from src.ingest.harness import claude_daemon
    from src.orchestrator.seats import reachability

    await save_mount(actions.pool, job_dir="/home/t/.claude/jobs/reach002",
                     agent_id="agent:reach0002", project="osiris", cwd="/t",
                     model="claude-sonnet-5", session_key=None)

    async def _dark(ids: set[str]) -> None:
        return None

    monkeypatch.setattr(claude_daemon, "job_for", _dark)

    out = await reachability(actions.pool, "agent:reach0002")

    assert out["reachable"] is False and out["via"] == "none" and out["job"] is None
    assert "never treated as proof of death" in out["detail"]


async def test_reachability_has_nothing_to_ask_about_an_unmounted_lineage(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No agent_mounts row at all — never even asks the daemon, since there is no job_dir
    to ask about."""
    from src.ingest.harness import claude_daemon
    from src.orchestrator.seats import reachability

    async def _boom(ids: set[str]) -> None:
        raise AssertionError("job_for must not be called with nothing to ask about")

    monkeypatch.setattr(claude_daemon, "job_for", _boom)

    out = await reachability(actions.pool, "agent:reach0003")

    assert out == {"reachable": False, "via": "none", "job": None,
                   "detail": "no known job_dir for this lineage — nothing to ask the "
                             "daemon about"}


async def test_reachability_is_lineage_wide_like_held_seat(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ra's exact shape: the mount row still names the PRE-compaction generation, but the
    caller asks about the POST-compaction successor — the daemon check must still find it,
    the same base-or-'-suffix' match held_seat/seat_occupancy already use."""
    from src.ingest.harness import claude_daemon
    from src.orchestrator.seats import reachability

    await save_mount(actions.pool, job_dir="/home/t/.claude/jobs/reach004",
                     agent_id="agent:reach0004", project="osiris", cwd="/t",
                     model="claude-sonnet-5", session_key=None)
    job = {"short": "reach004", "sessionId": "reach004-full"}

    async def _fake(ids: set[str]) -> dict[str, Any] | None:
        return job if "reach004" in ids else None

    monkeypatch.setattr(claude_daemon, "job_for", _fake)

    out = await reachability(actions.pool, "agent:reach0004-ii")  # the fresh successor

    assert out["reachable"] is True and out["job"] == job


# ═══ MANAGER_OF_SEAT (notify-at-seam, thread aeae9977) — the single-pair managed_by read,
# mirroring osiris_stophook.py's own local `_manager_seat` query so mint_heir's compaction
# path doesn't hand-roll a third copy of the same SQL. ═══════════════════════════════════


async def test_manager_of_seat_resolves_the_managed_by_link(actions: Actions) -> None:
    from src.orchestrator.seats import manager_of_seat

    worker = await actions.create_or_find_object("Seat", "seat:mos1aaaa", "test")
    manager = await actions.create_or_find_object("Seat", "seat:mos1bbbb", "test")
    await actions.create_link(worker, manager, "managed_by", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")

    assert await manager_of_seat(actions.pool, "seat:mos1aaaa") == "seat:mos1bbbb"


async def test_manager_of_seat_none_when_unmanaged(actions: Actions) -> None:
    from src.orchestrator.seats import manager_of_seat

    await actions.create_or_find_object("Seat", "seat:mos2cccc", "test")

    assert await manager_of_seat(actions.pool, "seat:mos2cccc") is None


# ═══ DERIVE_HOUSE (ruling ff6148b0, decision 4c9e4bd7) — house is DERIVED off the managed_by
# chain to the head, never a stored snapshot that drifts (Alfred's legacy bytebye, Vajra's
# twin house=vajra). The head's own stored house is the one legitimate anchor. ═══════════


async def _link_managed_by(actions: Actions, worker: Any, manager: Any) -> None:
    await actions.create_link(worker, manager, "managed_by", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")


async def test_derive_house_of_a_head_reads_its_own_stored_house(actions: Actions) -> None:
    """No managed_by edge out = the head; its own stamped house is authoritative — the one
    legitimate place house is still stored (a deliberate anchor, e.g. Alfred's 'alfred')."""
    from src.orchestrator.seats import derive_house

    head = await actions.create_or_find_object("Seat", "seat:dh1head0", "test")
    await actions.assert_property(head, "house", "alfred", "test", datetime.now(UTC), 0.9)

    assert await derive_house(actions.pool, "seat:dh1head0") == "alfred"


async def test_derive_house_walks_the_chain_ignoring_a_worker_s_own_stale_stamp(
    actions: Actions,
) -> None:
    """THE ACTUAL BUG: a worker's own stored house ('bytebye', a legacy mint-time snapshot)
    is WRONG — but it's never read. Only the chain up to the head matters, however many
    hops. Two levels here (worker -> manager -> head) to prove it isn't just one hop."""
    from src.orchestrator.seats import derive_house

    head = await actions.create_or_find_object("Seat", "seat:dh2head0", "test")
    await actions.assert_property(head, "house", "alfred", "test", datetime.now(UTC), 0.9)
    manager = await actions.create_or_find_object("Seat", "seat:dh2mgr00", "test")
    worker = await actions.create_or_find_object("Seat", "seat:dh2wrk00", "test")
    await actions.assert_property(worker, "house", "bytebye", "test", datetime.now(UTC), 0.9)
    await _link_managed_by(actions, manager, head)
    await _link_managed_by(actions, worker, manager)

    assert await derive_house(actions.pool, "seat:dh2wrk00") == "alfred"
    assert await derive_house(actions.pool, "seat:dh2mgr00") == "alfred"


async def test_derive_house_detects_a_managed_by_cycle_without_hanging(
    actions: Actions,
) -> None:
    """A seat reappearing in its own chain is a graph BUG, not a deep hierarchy — this must
    terminate (never infinite-loop) and read as None, not crash or hang."""
    from src.orchestrator.seats import derive_house

    a = await actions.create_or_find_object("Seat", "seat:dh3aaaa0", "test")
    b = await actions.create_or_find_object("Seat", "seat:dh3bbbb0", "test")
    await _link_managed_by(actions, a, b)
    await _link_managed_by(actions, b, a)

    assert await derive_house(actions.pool, "seat:dh3aaaa0") is None


async def test_derive_house_none_for_an_unknown_seat(actions: Actions) -> None:
    from src.orchestrator.seats import derive_house

    assert await derive_house(actions.pool, "seat:dh4ghost") is None


async def test_held_seat_reports_the_derived_house_not_the_stale_stamp(
    actions: Actions, tmp_path: Path,
) -> None:
    """The integration point orient()/mount() actually read: a worker seat's OWN stored
    house is stale, but held_seat() reports the head's house, matching derive_house()."""
    from src.orchestrator.agents import claim_name
    from src.orchestrator.seats import bind_holder, held_seat

    head = await actions.create_or_find_object("Seat", "seat:dh5head0", "test")
    await actions.assert_property(head, "house", "alfred", "test", datetime.now(UTC), 0.9)
    a = await actions.create_or_find_object("Agent", "agent:dh5wrk01", "test")
    await actions.assert_property(a, "project", "osiris", "test", datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")
    claimed = await claim_name(actions, "agent:dh5wrk01", "Vajra", source="test")
    worker = await actions.create_or_find_object("Seat", claimed["seat_id"], "test")
    # a legacy stray stamp on the worker's OWN seat — must never be read now
    await actions.assert_property(worker, "house", "vajra", "test", datetime.now(UTC), 0.9)
    await _link_managed_by(actions, worker, head)
    await bind_holder(actions, seat_id=claimed["seat_id"], agent_id="agent:dh5wrk01")

    bound = await held_seat(actions.pool, "agent:dh5wrk01")
    assert bound is not None and bound["house"] == "alfred"


async def test_fleet_occupancy_reports_derived_house_per_seat(actions: Actions) -> None:
    from src.orchestrator.seats import bind_holder, ensure_seat, fleet_occupancy

    head = await actions.create_or_find_object("Seat", "seat:dh6head0", "test")
    await actions.assert_property(head, "house", "alfred", "test", datetime.now(UTC), 0.9)
    worker = await ensure_seat(actions, house="stale-nonsense", handle="Tefnut6",
                               source="test")
    await _link_managed_by(actions, await actions.create_or_find_object(
        "Seat", worker["seat_id"], "test"), head)
    await bind_holder(actions, seat_id=worker["seat_id"], agent_id="agent:dh6live1")

    rows = await fleet_occupancy(actions.pool)
    row = next(r for r in rows if r["seat_id"] == worker["seat_id"])
    assert row["house"] == "alfred"


async def test_seat_facts_returns_all_four_keys_with_derived_house(actions: Actions) -> None:
    """The shared resolver (mintseat.py + trigger.py's own duplicate _seat_facts,
    consolidated here) — always all four keys present, `house` derived not stored."""
    from src.orchestrator.seats import seat_facts

    head = await actions.create_or_find_object("Seat", "seat:sf1head0", "test")
    await actions.assert_property(head, "house", "alfred", "test", datetime.now(UTC), 0.9)
    worker = await actions.create_or_find_object("Seat", "seat:sf1wrk00", "test")
    await actions.assert_property(worker, "handle", "Vajra", "test", datetime.now(UTC), 0.9)
    await actions.assert_property(worker, "anchor_cwd", "/home/vajra", "test",
                                  datetime.now(UTC), 0.9)
    await _link_managed_by(actions, worker, head)

    facts = await seat_facts(actions.pool, "seat:sf1wrk00")
    assert facts == {"handle": "Vajra", "house": "alfred", "intended_model": None,
                     "anchor_cwd": "/home/vajra"}


async def test_seat_facts_all_none_for_an_unknown_seat(actions: Actions) -> None:
    from src.orchestrator.seats import seat_facts

    assert await seat_facts(actions.pool, "seat:sf2ghost") == {
        "handle": None, "house": None, "intended_model": None, "anchor_cwd": None}


# ═══ ATTENDANCE (thread 96f62338, replacing ruling d8a77f80's broken managed_by proxy) ═══

async def _attended_value(actions: Actions, seat_id: str) -> str | None:
    return await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND o.type='Seat' AND a.name='attended' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", seat_id)


async def test_set_seat_attended_stamps_and_is_read_back(actions: Actions) -> None:
    from src.orchestrator.seats import set_seat_attended

    seat = (await ensure_seat(actions, house="demo", handle="Attendee",
                              source="test"))["seat_id"]
    out = await set_seat_attended(actions, seat_id=seat, attended="human", actor="test",
                                  because="this seat is operator-fronted")
    assert out == {"seat": seat, "attended": "human", "because": "this seat is operator-fronted"}
    assert await _attended_value(actions, seat) == "human"

    # a later stamp reversing it supersedes cleanly — one current value, not a pile-up
    await set_seat_attended(actions, seat_id=seat, attended="worker", actor="test",
                            because="handed off to full automation")
    assert await _attended_value(actions, seat) == "worker"
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='attended'", seat) == 1


async def test_set_seat_attended_refuses_a_value_outside_the_closed_set(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import set_seat_attended

    seat = (await ensure_seat(actions, house="demo", handle="Typo",
                              source="test"))["seat_id"]
    out = await set_seat_attended(actions, seat_id=seat, attended="humann", actor="test",
                                  because="a typo must never silently land as 'not human'")
    assert "error" in out and "human" in out["error"] and "worker" in out["error"]
    assert await _attended_value(actions, seat) is None


async def test_set_seat_attended_refuses_a_blank_because(actions: Actions) -> None:
    from src.orchestrator.seats import set_seat_attended

    seat = (await ensure_seat(actions, house="demo", handle="Blank",
                              source="test"))["seat_id"]
    out = await set_seat_attended(actions, seat_id=seat, attended="human", actor="test",
                                  because="   ")
    assert "error" in out
    assert await _attended_value(actions, seat) is None


async def test_set_seat_attended_refuses_an_unknown_seat(actions: Actions) -> None:
    from src.orchestrator.seats import set_seat_attended

    out = await set_seat_attended(actions, seat_id="seat:nosuchsea", attended="human",
                                  actor="test", because="no such seat exists")
    assert out == {"error": "no such seat: 'seat:nosuchsea'"}


async def test_set_seat_attended_refuses_a_retired_seat(actions: Actions) -> None:
    from src.orchestrator.seats import retire_seat, set_seat_attended

    seat = (await ensure_seat(actions, house="demo", handle="Gone", source="test"))["seat_id"]
    await retire_seat(actions, seat, reason="role is over", actor="test")
    out = await set_seat_attended(actions, seat_id=seat, attended="human", actor="test",
                                  because="attempting to stamp a dead seat")
    assert "error" in out and "retired" in out["error"]


# ═══ RENAME_SEAT (operator-ordered, 2026-07-28 — the casing-drift build) ═══

async def _handle_of(actions: Actions, canonical: str) -> str | None:
    return await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='handle' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", canonical)


async def test_rename_seat_stamps_seat_and_its_current_holder(actions: Actions) -> None:
    from src.orchestrator.seats import bind_holder, rename_seat

    seat = (await ensure_seat(actions, house="demo", handle="tjmax", source="test"))["seat_id"]
    await bind_holder(actions, seat_id=seat, agent_id="agent:tjholder", source="test")
    await actions.assert_property(
        await actions.create_or_find_object("Agent", "agent:tjholder", "test"),
        "handle", "TJMAX", "agent:tjholder", datetime.now(UTC), 0.9, evidence_class="self_declared")

    out = await rename_seat(actions, seat_id=seat, new_handle="William", actor="test",
                            because="after William Shockley, replacing a linux-function name")
    assert out["seat"] == seat and out["old_handle"] == "tjmax" and out["new_handle"] == "William"
    assert out["holder_stamped"] == "agent:tjholder"
    assert "harness" in out["note"] and "next spawn" in out["note"]
    assert await _handle_of(actions, seat) == "William"
    assert await _handle_of(actions, "agent:tjholder") == "William"
    # old handle stays in history — never deleted, just superseded
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='handle' AND a.value #>> '{}' = 'tjmax'", seat) == 1


async def test_rename_seat_on_a_vacant_seat_only_stamps_the_seat(actions: Actions) -> None:
    from src.orchestrator.seats import rename_seat

    seat = (await ensure_seat(actions, house="demo", handle="Empty", source="test"))["seat_id"]
    out = await rename_seat(actions, seat_id=seat, new_handle="StillEmpty", actor="test",
                            because="renaming a seat nobody sits in yet")
    assert out["holder_stamped"] is None
    assert await _handle_of(actions, seat) == "StillEmpty"


async def test_rename_seat_refuses_a_name_another_seat_already_carries(
    actions: Actions,
) -> None:
    """The exact drift lesson: 'vajra' and 'Vajra' must never both be claimable."""
    from src.orchestrator.seats import rename_seat

    await ensure_seat(actions, house="demo", handle="Vajra", source="test")
    other = (await ensure_seat(actions, house="demo", handle="Renameme",
                               source="test"))["seat_id"]
    out = await rename_seat(actions, seat_id=other, new_handle="vajra", actor="test",
                            because="attempting a casing-only collision")
    assert "error" in out and "already claimed" in out["error"]
    assert await _handle_of(actions, other) == "Renameme"


async def test_rename_seat_refuses_a_blank_because(actions: Actions) -> None:
    from src.orchestrator.seats import rename_seat

    seat = (await ensure_seat(actions, house="demo", handle="Blank2",
                              source="test"))["seat_id"]
    out = await rename_seat(actions, seat_id=seat, new_handle="Something", actor="test",
                            because="   ")
    assert "error" in out
    assert await _handle_of(actions, seat) == "Blank2"


async def test_rename_seat_refuses_a_blank_or_overlong_handle(actions: Actions) -> None:
    from src.orchestrator.seats import rename_seat

    seat = (await ensure_seat(actions, house="demo", handle="Sized",
                              source="test"))["seat_id"]
    blank = await rename_seat(actions, seat_id=seat, new_handle="   ", actor="test",
                              because="an empty name is not a name")
    assert "error" in blank
    toolong = await rename_seat(actions, seat_id=seat, new_handle="x" * 41, actor="test",
                                because="past the 40-char handle cap")
    assert "error" in toolong
    assert await _handle_of(actions, seat) == "Sized"


async def test_rename_seat_refuses_an_unknown_seat(actions: Actions) -> None:
    from src.orchestrator.seats import rename_seat

    out = await rename_seat(actions, seat_id="seat:nosuchsea", new_handle="Anyone",
                            actor="test", because="no such seat exists")
    assert out == {"error": "no such seat: 'seat:nosuchsea'"}


# ═══ SEAT LIFECYCLE (ruling ff6148b0's completion, decision 87953278, thread cb374585) ═══


async def test_correct_house_lets_a_head_fix_its_own_anchor(actions: Actions) -> None:
    """The motivating case: Alfred (a head, no managed_by out) corrects bytebye -> alfred
    on himself. A delegate re-derives the new value immediately."""
    from src.orchestrator.agents import claim_name
    from src.orchestrator.seats import correct_house, derive_house

    head = await actions.create_or_find_object("Agent", "agent:ch1alfrd", "test")
    await actions.assert_property(head, "project", "bytebye", "test", datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")
    claimed = await claim_name(actions, "agent:ch1alfrd", "Alfred", source="test")
    assert claimed.get("error") is None

    out = await correct_house(actions, "agent:ch1alfrd", "alfred", source="test")
    assert out == {"seat_id": claimed["seat_id"], "house": "alfred", "was": "bytebye"}
    assert await derive_house(actions.pool, claimed["seat_id"]) == "alfred"


async def test_correct_house_refuses_a_non_head(actions: Actions) -> None:
    from src.orchestrator.seats import correct_house

    head = await actions.create_or_find_object("Seat", "seat:ch2head0", "test")
    await actions.assert_property(head, "house", "alfred", "test", datetime.now(UTC), 0.9)
    worker = await actions.create_or_find_object("Seat", "seat:ch2wrk00", "test")
    await _link_managed_by(actions, worker, head)
    await actions.create_or_find_object("Agent", "agent:ch2wrker", "test")
    from src.orchestrator.seats import bind_holder
    await bind_holder(actions, seat_id="seat:ch2wrk00", agent_id="agent:ch2wrker",
                      source="test")

    out = await correct_house(actions, "agent:ch2wrker", "bogus", source="test")
    assert "not a head" in out.get("error", "")


async def test_correct_house_refuses_an_empty_value(actions: Actions) -> None:
    from src.orchestrator.seats import correct_house

    assert "needs a name" in (
        await correct_house(actions, "agent:ch3nobody", "  ", source="test"))["error"]


async def test_correct_house_refuses_a_caller_with_no_seat(actions: Actions) -> None:
    from src.orchestrator.seats import correct_house

    out = await correct_house(actions, "agent:ch4unseated", "somehouse", source="test")
    assert "holds no seat" in out["error"]


async def test_fold_seat_moves_active_holders_and_estate_the_vajra_shape(
    actions: Actions,
) -> None:
    """Reproduces the exact live shape: a twin with TWO concurrent holders (the anomaly
    itself — two sessions bound to the wrong seat) folds into the real, org-anchored seat.
    Both are re-pointed (named in holders_moved); they converge to ONE active holder on
    the survivor — the NEWEST — matching bind_holder's own one-seat-one-holder law rather
    than preserving the twin's anomaly on the far side of the fold."""
    from src.orchestrator.seats import fold_seat, held_seat, manager_of_seat

    alfred = await actions.create_or_find_object("Seat", "seat:fs1alfrd", "test")
    await actions.assert_property(alfred, "house", "alfred", "test", datetime.now(UTC), 0.9)
    real = await actions.create_or_find_object("Seat", "seat:fs1real0", "test")
    await _link_managed_by(actions, real, alfred)
    twin = await actions.create_or_find_object("Seat", "seat:fs1twin0", "test")
    h1 = await actions.create_or_find_object("Agent", "agent:fs1hld01", "test")
    h2 = await actions.create_or_find_object("Agent", "agent:fs1hld02", "test")
    await actions.create_link(h1, twin, "holds", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    await actions.create_link(h2, twin, "holds", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    await mailbox_send(actions, to_agent="seat:fs1twin0")

    out = await fold_seat(actions, dupe="seat:fs1twin0", into="seat:fs1real0",
                          evidence="test: the Vajra twin shape", actor="test")
    assert set(out["holders_moved"]) == {"agent:fs1hld01", "agent:fs1hld02"}
    assert out["mail_moved"] == 1

    b1 = await held_seat(actions.pool, "agent:fs1hld01")
    b2 = await held_seat(actions.pool, "agent:fs1hld02")
    assert b1 is None, "the OLDER concurrent holder heals away, converging to one soul"
    assert b2 is not None and b2["seat_id"] == "seat:fs1real0", (
       "the NEWER concurrent holder survives as the seat's one active holder")
    assert await manager_of_seat(actions.pool, "seat:fs1real0") == "seat:fs1alfrd"


async def mailbox_send(actions: Actions, *, to_agent: str) -> None:
    from src.orchestrator.mailbox import send_message
    await send_message(actions.pool, from_agent="agent:someone", from_project="test",
                       to_agent=to_agent, body="a stray message to the twin")


async def test_fold_seat_refuses_thin_evidence(actions: Actions) -> None:
    from src.orchestrator.seats import fold_seat

    a = await actions.create_or_find_object("Seat", "seat:fs2aaaa0", "test")
    b = await actions.create_or_find_object("Seat", "seat:fs2bbbb0", "test")
    assert a and b
    out = await fold_seat(actions, dupe="seat:fs2aaaa0", into="seat:fs2bbbb0", evidence="",
                          actor="test")
    assert "auto-merge wearing a signature" in out["error"]


async def test_fold_seat_refuses_dupe_equals_into(actions: Actions) -> None:
    from src.orchestrator.seats import fold_seat

    out = await fold_seat(actions, dupe="seat:fs3same0", into="seat:fs3same0",
                          evidence="test", actor="test")
    assert "same seat" in out["error"]


async def test_fold_seat_refuses_an_unknown_seat(actions: Actions) -> None:
    from src.orchestrator.seats import fold_seat

    a = await actions.create_or_find_object("Seat", "seat:fs4real0", "test")
    assert a
    out = await fold_seat(actions, dupe="seat:fs4ghost", into="seat:fs4real0",
                          evidence="test", actor="test")
    assert "unknown seat" in out["error"] and "seat:fs4ghost" in out["error"]


async def test_retire_seat_closes_a_vacant_seat(actions: Actions) -> None:
    from src.orchestrator.seats import retire_seat

    await actions.create_or_find_object("Seat", "seat:rs1dead0", "test")
    out = await retire_seat(actions, "seat:rs1dead0", reason="role discontinued",
                            actor="test")
    assert out == {"retired": "seat:rs1dead0"}
    val = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "AND a.name='retired' WHERE o.canonical=$1", "seat:rs1dead0")
    assert val == "true"


async def test_retire_seat_flips_object_status_too(actions: Actions) -> None:
    """THE STATUS GAP (Seshat msg 1686, operator-caught msg 1713): the property alone left
    objects.status readable as 'active' forever — a fresh claim could still bind to a seat
    that LOOKED retired. Both layers must flip together."""
    from src.orchestrator.seats import retire_seat

    await actions.create_or_find_object("Seat", "seat:rs4status", "test")
    out = await retire_seat(actions, "seat:rs4status", reason="status gap coverage",
                            actor="test")
    assert out == {"retired": "seat:rs4status"}
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='seat:rs4status'")
    assert row["status"] == "retired"
    # a second retire_seat call must refuse on the now-non-active status, not re-fire
    again = await retire_seat(actions, "seat:rs4status", actor="test")
    assert "already retired" in again["error"]


async def test_retire_seat_refuses_an_active_holder(actions: Actions) -> None:
    from src.orchestrator.seats import bind_holder, retire_seat

    await actions.create_or_find_object("Seat", "seat:rs2live0", "test")
    await bind_holder(actions, seat_id="seat:rs2live0", agent_id="agent:rs2holdr",
                      source="test")

    out = await retire_seat(actions, "seat:rs2live0", actor="test")
    assert "actively held" in out["error"] and "agent:rs2holdr" in out["error"]


async def test_retire_seat_refuses_an_unknown_seat(actions: Actions) -> None:
    from src.orchestrator.seats import retire_seat

    out = await retire_seat(actions, "seat:rs3ghost", actor="test")
    assert "no such seat" in out["error"]


# ═══ vacate_holder (thread 445a7356) — retire_seat's stale-holder refusal is correct;
# this is its complement, releasing a holder WITHOUT closing the seat. It trusts its
# caller (trigger.vacate_dead_seat gathers the liveness evidence) and does the write.

async def test_vacate_holder_releases_the_active_holder(actions: Actions) -> None:
    from src.orchestrator.seats import bind_holder, vacate_holder

    await actions.create_or_find_object("Seat", "seat:vh1dead0", "test")
    await bind_holder(actions, seat_id="seat:vh1dead0", agent_id="agent:vh1corps",
                      source="test")

    out = await vacate_holder(actions, seat_id="seat:vh1dead0", actor="test",
                              because="process confirmed dead")
    assert out == {"vacated": "seat:vh1dead0", "was_held_by": ["agent:vh1corps"]}
    holder = await actions.pool.fetchval(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", "seat:vh1dead0")
    assert holder is None
    because = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "AND a.name='vacated_because' WHERE o.canonical=$1", "seat:vh1dead0")
    assert because == "process confirmed dead"
    # the seat itself is untouched — never retired, still ready for a fresh claim
    status = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical=$1", "seat:vh1dead0")
    assert status == "active"


async def test_vacate_holder_refuses_a_blank_because(actions: Actions) -> None:
    from src.orchestrator.seats import bind_holder, vacate_holder

    await actions.create_or_find_object("Seat", "seat:vh2blank", "test")
    await bind_holder(actions, seat_id="seat:vh2blank", agent_id="agent:vh2holdr",
                      source="test")
    out = await vacate_holder(actions, seat_id="seat:vh2blank", actor="test", because="  ")
    assert "because is required" in out["error"]
    # nothing written — the holder is still bound
    holder = await actions.pool.fetchval(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", "seat:vh2blank")
    assert holder == "agent:vh2holdr"


async def test_vacate_holder_refuses_an_unknown_seat(actions: Actions) -> None:
    from src.orchestrator.seats import vacate_holder

    out = await vacate_holder(actions, seat_id="seat:vh3ghost", actor="test",
                              because="dead")
    assert "no such seat" in out["error"]


async def test_vacate_holder_refuses_an_already_vacant_seat(actions: Actions) -> None:
    from src.orchestrator.seats import vacate_holder

    await actions.create_or_find_object("Seat", "seat:vh4empty", "test")
    out = await vacate_holder(actions, seat_id="seat:vh4empty", actor="test",
                              because="dead")
    assert "nothing to vacate" in out["error"]
