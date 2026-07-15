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

from src.actions.core import Actions
from src.orchestrator.agents import mint_heir
from src.orchestrator.handshake import automount
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import (
    attach_session,
    ensure_seat,
    find_seat,
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
    assert await ack_messages(actions.pool, "osiris", [out["id"]],
                              reader_agent="agent:oooo0001") == 1
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
