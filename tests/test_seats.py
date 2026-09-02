"""The identity core, Phase A (ruling 5cef856b) — the Seat object + the attach ceremony.

Every test here demonstrates a rule of the ceremony against the bug that demanded it:
identity keyed on ephemeral facts (session, path) and RECONSTRUCTED by inference at every
door was the collision class (2294e95d: a mind mounted into a SIBLING's seat; this session's
own triple-mint boot). A Seat is minted once, exists before its first session, and a session
ATTACHES with a one-time token — refusals loud, nothing written.
"""
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import agents as agents_mod
from src.orchestrator import seats as seats_mod
from src.orchestrator.agents import mint_heir, mint_lock
from src.orchestrator.handshake import automount
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import (
    LockWedged,
    _peer_lock,
    _seat_lock,
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


async def test_bind_holder_invalidates_the_agents_own_other_active_holds(
    actions: Actions,
) -> None:
    """Guard-symmetry inventory (decision efd97c13, thread 6068/6088): bind_holder always
    invalidated a SEAT's prior holders before binding a new one; now it also invalidates
    the AGENT's own other active `holds` edges elsewhere — a Seat is a specific identity
    (no charter-like concept sanctions one agent holding two), unlike works_in's
    additive-only fix, which stayed additive because a declared multi-project charter is a
    real, legitimate multi-value state that `holds` has no counterpart for."""
    seat_a = await ensure_seat(actions, house="osiris", handle="Bastet", source="test")
    seat_b = await ensure_seat(actions, house="osiris", handle="Bastet2", source="test")
    await actions.create_or_find_object("Agent", "agent:dual00001", "test")

    await bind_holder(actions, seat_id=seat_a["seat_id"], agent_id="agent:dual00001")
    assert await _active_holds(actions, "agent:dual00001", seat_a["seat_id"])

    await bind_holder(actions, seat_id=seat_b["seat_id"], agent_id="agent:dual00001")

    assert await _active_holds(actions, "agent:dual00001", seat_b["seat_id"])
    assert not await _active_holds(actions, "agent:dual00001", seat_a["seat_id"])
    # ...and seat_a's own healed link is still there, never deleted
    healed = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id "
        "WHERE f.canonical='agent:dual00001' AND t.canonical=$1 AND l.type='holds' "
        "AND l.valid_until IS NOT NULL", seat_a["seat_id"])
    assert healed == 1


async def test_bind_holder_rebinding_the_same_seat_is_a_no_op_on_other_seats(
    actions: Actions,
) -> None:
    """The new agent-side invalidation must not fire on an ordinary re-bind (the same
    agent, the same seat, called twice — e.g. a repeat claim_name) — only on a GENUINELY
    different seat, mirroring the seat-side `f.canonical <> $2` exclusion already there."""
    seat = await ensure_seat(actions, house="osiris", handle="Sobek", source="test")
    await actions.create_or_find_object("Agent", "agent:same0001", "test")

    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:same0001")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:same0001")

    assert await _active_holds(actions, "agent:same0001", seat["seat_id"])
    # exactly one link row, never healed — a same-seat rebind touched nothing
    rows = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id "
        "WHERE f.canonical='agent:same0001' AND t.canonical=$1 AND l.type='holds'",
        seat["seat_id"])
    assert rows == 1


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

    # ONE LIVENESS AUTHORITY, FOURTH DOOR: resolve_seat's own "live" cross-checks
    # is_occupied_by_a_live_body — confirm the TRUE holder (not the impostor) as the
    # harness-verified body.
    async def _agents_json(**kw: Any) -> list[dict[str, Any]]:
        return [{"sessionId": "iiii0002-0000-4000-8000-000000000000", "pid": 888,
                 "cwd": "/jobs/iiii0002", "name": "[OS] Payne"}]

    out = await resolve_seat(
        actions, "Payne", agents_json=_agents_json,
        read_exe=lambda pid: "/home/x/.local/share/claude/versions/2.1.210",
        read_cwd=lambda pid: "/jobs/iiii0002")

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


# --- THE ADDRESSING REFUSAL (rulings 1a64ae9a/aee67e6d, DM 2360 — John XV/XVI) ---
# binding_of_handle collapses "no seat" / "ambiguous" / "genuinely vacant" / "a seat WITH a
# marked-ineligible holder" into one bare None — resolve_seat then treats all four alike as
# license to fall back to the assertion path, which found John's DEAD PREDECESSOR the one
# time it mattered. seat_holder_ineligible names the fourth shape distinctly so a caller
# (send_message) can refuse BEFORE that fallback ever runs.


async def test_seat_holder_ineligible_none_for_no_such_seat(actions: Actions) -> None:
    from src.orchestrator.seats import seat_holder_ineligible

    assert await seat_holder_ineligible(actions.pool, "NoSuchHandleAtAll") is None


async def test_seat_holder_ineligible_none_for_an_ambiguous_handle(actions: Actions) -> None:
    from src.orchestrator.seats import seat_holder_ineligible

    await ensure_seat(actions, house="alpha", handle="AmbigTwin", source="test")
    await ensure_seat(actions, house="beta", handle="AmbigTwin", source="test")
    assert await seat_holder_ineligible(actions.pool, "AmbigTwin") is None


async def test_seat_holder_ineligible_none_for_a_genuinely_vacant_seat(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import seat_holder_ineligible

    await ensure_seat(actions, house="osiris", handle="VacantIneligible", source="test")
    assert await seat_holder_ineligible(actions.pool, "VacantIneligible") is None


async def test_seat_holder_ineligible_none_for_an_eligible_holder(actions: Actions) -> None:
    from src.orchestrator.seats import bind_holder, seat_holder_ineligible

    seat = await ensure_seat(actions, house="osiris", handle="Eligible", source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:elig0001")
    assert await seat_holder_ineligible(actions.pool, "Eligible") is None


async def test_seat_holder_ineligible_names_the_seat_when_the_holder_is_false_mint(
    actions: Actions,
) -> None:
    """John's exact live shape: a unique seat, ONE active holder, and that holder is a
    healed phantom mint. binding_of_handle's own NOT EXISTS guard excludes it silently;
    this function is the only thing that says why."""
    from src.orchestrator.seats import bind_holder, seat_holder_ineligible

    seat = await ensure_seat(actions, house="osiris", handle="Ghost", source="test")
    now = datetime.now(UTC)
    old = await actions.create_or_find_object("Agent", "agent:ghost-old", "agent:ghost-old")
    await actions.assert_property(old, "handle", "Ghost", "agent:ghost-old", now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:ghost-old")
    new = await actions.create_or_find_object("Agent", "agent:ghost-new", "agent:ghost-new")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:ghost-new")  # succession
    await actions.assert_property(new, "false_mint", "true", "agent:ghost-new", now, 0.9,
                                  evidence_class="self_declared")

    reason = await seat_holder_ineligible(actions.pool, "Ghost")

    assert reason is not None
    assert seat["seat_id"] in reason
    assert "agent:ghost-new" in reason


async def test_seat_holder_ineligible_names_the_seat_when_the_holder_is_retired(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import bind_holder, seat_holder_ineligible

    seat = await ensure_seat(actions, house="osiris", handle="Retiree", source="test")
    now = datetime.now(UTC)
    holder = await actions.create_or_find_object("Agent", "agent:ret0001", "agent:ret0001")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:ret0001")
    await actions.assert_property(holder, "retired", "true", "agent:ret0001", now, 0.9,
                                  evidence_class="self_declared")

    reason = await seat_holder_ineligible(actions.pool, "Retiree")
    assert reason is not None and "agent:ret0001" in reason


async def test_seat_holder_ineligible_ignores_a_superseded_holders_own_marks(
    actions: Actions,
) -> None:
    """A mark on a PRIOR holder whose `holds` edge has already healed (bind_holder's own
    succession, valid_until in the past) must never surface — only the CURRENT active
    holder's marks matter, same predicate binding_of_handle itself runs."""
    from src.orchestrator.seats import bind_holder, seat_holder_ineligible

    seat = await ensure_seat(actions, house="osiris", handle="Healed", source="test")
    now = datetime.now(UTC)
    old = await actions.create_or_find_object("Agent", "agent:heal-old", "agent:heal-old")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:heal-old")
    await actions.assert_property(old, "false_mint", "true", "agent:heal-old", now, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:heal-new")  # supersedes

    assert await seat_holder_ineligible(actions.pool, "Healed") is None


async def test_seat_holder_ineligible_none_when_an_older_active_holder_is_eligible(
    actions: Actions,
) -> None:
    """THOTH'S CORRECTION (DM 2377, caught in review — held the deploy of ddb8104): the
    first build of this function checked whether the NEWEST active holds edge was marked,
    not whether ANY eligible holder existed among them. `bind_holder` always heals a prior
    edge before creating a new one, so it can never itself produce two simultaneously
    active `holds` edges — but nothing else in the schema enforces single-holder (decision
    6ce4ac5f), and seat:c476e7a2 carries exactly this shape live: two active edges, the
    newer one marked, the older one still eligible. Constructed here with create_link
    directly (bypassing bind_holder's own invalidation) to reproduce that shape, not
    theorize it. binding_of_handle filters marked holders out before ranking by recency, so
    it resolves the older, eligible one fine — this function must agree, not refuse."""
    from src.orchestrator.seats import seat_holder_ineligible

    seat = await ensure_seat(actions, house="osiris", handle="TwoHolders", source="test")
    seat_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", seat["seat_id"])
    now = datetime.now(UTC)
    older = await actions.create_or_find_object("Agent", "agent:two-older", "agent:two-older")
    await actions.create_link(older, seat_oid, "holds", "test", now - timedelta(minutes=5), 0.9,
                              evidence_class="self_declared")
    newer = await actions.create_or_find_object("Agent", "agent:two-newer", "agent:two-newer")
    await actions.create_link(newer, seat_oid, "holds", "test", now, 0.9,
                              evidence_class="self_declared")
    await actions.assert_property(newer, "false_mint", "true", "agent:two-newer", now, 0.9,
                                  evidence_class="self_declared")

    assert await seat_holder_ineligible(actions.pool, "TwoHolders") is None


async def test_seat_holder_ineligible_fires_when_every_active_holder_is_ineligible(
    actions: Actions,
) -> None:
    """The genuine positive case with TWO active holders: both marked. Must still refuse —
    the fix narrows the false-positive, it must not swallow the real one."""
    from src.orchestrator.seats import seat_holder_ineligible

    seat = await ensure_seat(actions, house="osiris", handle="BothMarked", source="test")
    seat_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", seat["seat_id"])
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:both-a", "agent:both-a")
    await actions.create_link(a, seat_oid, "holds", "test", now - timedelta(minutes=5), 0.9,
                              evidence_class="self_declared")
    await actions.assert_property(a, "retired", "true", "agent:both-a", now, 0.9,
                                  evidence_class="self_declared")
    b = await actions.create_or_find_object("Agent", "agent:both-b", "agent:both-b")
    await actions.create_link(b, seat_oid, "holds", "test", now, 0.9,
                              evidence_class="self_declared")
    await actions.assert_property(b, "false_mint", "true", "agent:both-b", now, 0.9,
                                  evidence_class="self_declared")

    reason = await seat_holder_ineligible(actions.pool, "BothMarked")
    assert reason is not None
    assert "agent:both-a" in reason and "agent:both-b" in reason


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

    # ONE LIVENESS AUTHORITY, FOURTH DOOR (Thoth msg 5719, 2026-08-26): resolve_seat's own
    # "live" now cross-checks is_occupied_by_a_live_body, so a harness-confirming fake is
    # needed here too — _legacy_unbound_seat's own job_dir ("/jobs/zzzz0001") is exactly
    # 8 chars on purpose (registry_census keys agent_mounts.job_dir's basename against
    # sessionId[:8]).
    async def _agents_json(**kw: Any) -> list[dict[str, Any]]:
        return [{"sessionId": "zzzz0001-0000-4000-8000-000000000000", "pid": 777,
                 "cwd": "/w/osiris", "name": "[OS] Sekhmet"}]

    out = await backfill_unbound_seats(
        actions, agents_json=_agents_json,
        read_exe=lambda pid: "/home/x/.local/share/claude/versions/2.1.210",
        read_cwd=lambda pid: "/w/osiris")  # dry_run=True by default

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


async def test_occupancy_reads_occupied_off_last_active_alone_the_tenth_instance(
    actions: Actions,
) -> None:
    """Alfred's question via Thoth (msg 4394/4405, decision 59b3092c): seat_occupancy()
    used to run its own inline agent_mounts-only query — the exact single-source shape
    the dispatch-listener probe had BEFORE ruling 70493925 gave it a current_assertions
    fallback (test_agent_liveness_falls_back_to_last_active_like_fleet_always_has, this
    same fix, one reader over). A holder with NO mount row at all but a FRESH last_active
    assertion — the graph's own self-testimony, fleet()'s signal the whole time — now
    reads OCCUPIED here too, not COLD. Before this fix this returned COLD; the assertion
    below is the actual regression, not a restatement of already-passing behavior."""
    from src.orchestrator.seats import seat_occupancy

    seat = await ensure_seat(actions, house="osiris", handle="Sobek", source="test")
    await actions.create_or_find_object("Agent", "agent:lastactive1", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:lastactive1")
    agent_oid = await actions.create_or_find_object("Agent", "agent:lastactive1", "test")
    fresh = datetime.now(UTC).isoformat()
    await actions.assert_property(agent_oid, "last_active", fresh, "fleet-observer",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")

    occ = await seat_occupancy(actions.pool, seat["seat_id"])
    assert occ == {"state": "occupied", "holder": "agent:lastactive1", "live": True}


async def test_occupancy_and_fleet_occupancy_no_longer_disagree_with_themselves(
    actions: Actions,
) -> None:
    """Thoth's own regression proof (msg 4405): fleet() used to compute agent-level
    liveness via the fixed dual-source path and seat-level occupancy via this function's
    OWN unfixed single-source query — one payload, two disagreeing authorities over the
    same table. Both now delegate to the same mounts.agent_liveness(), so a holder live
    only by last_active reads occupied through BOTH doors, not just one."""
    from src.orchestrator import mounts
    from src.orchestrator.seats import fleet_occupancy, seat_occupancy

    seat = await ensure_seat(actions, house="osiris", handle="Bastet", source="test")
    await actions.create_or_find_object("Agent", "agent:selfconsist1", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:selfconsist1")
    agent_oid = await actions.create_or_find_object("Agent", "agent:selfconsist1", "test")
    fresh = datetime.now(UTC).isoformat()
    await actions.assert_property(agent_oid, "last_active", fresh, "fleet-observer",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")

    via_agent_liveness = await mounts.agent_liveness(actions.pool, "agent:selfconsist1")
    via_seat_occupancy = await seat_occupancy(actions.pool, seat["seat_id"])
    rows = await fleet_occupancy(actions.pool)
    via_fleet_occupancy = next(r for r in rows if r["seat_id"] == seat["seat_id"])

    assert via_agent_liveness["live"] is True
    assert via_seat_occupancy["live"] is True
    assert via_fleet_occupancy["live"] is True  # all three doors, one answer


async def test_occupancy_refuses_a_custom_live_secs_rather_than_silently_ignoring_it(
    actions: Actions,
) -> None:
    """`live_secs` is no longer a real knob — agent_liveness owns the one shared window.
    Nothing in this codebase ever passed a non-default value (confirmed by grep before
    this fix), so a refusal here costs nothing today; the alternative (silently dropping
    the parameter) would let a future caller believe it changed behavior when it hadn't,
    exactly the class of silent divergence this whole fix exists to close."""
    from src.orchestrator.seats import seat_occupancy

    seat = await ensure_seat(actions, house="osiris", handle="Khepri", source="test")
    with pytest.raises(ValueError, match="no longer supports a custom live_secs"):
        await seat_occupancy(actions.pool, seat["seat_id"], live_secs=60)


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


# ═══ ROSTER (task #140, Alfred's 2813da48) — cold is not vacant, and a pin is not certified ═
# canonical. Alfred read mount()'s live-agent list as the roster, found his own house cold,
# read COLD AS VACANT, and misrouted a repo's work to another seat's lineage while the seat
# offices on disk held the right answer the whole time. These tests prove roster() answers
# "who owns this repo, and is anybody home" from the graph, without collapsing any of the
# axes that bug depended on collapsing.

async def _repo(actions: Actions, name: str) -> None:
    """Pre-mint a SoftwareProject, matching test_charter.py's own helper — simulates the
    graph already having independent evidence this repo is real."""
    await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "test")


async def test_roster_tells_apart_vacant_cold_and_occupied(actions: Actions) -> None:
    from src.orchestrator.seats import roster

    vacant = await ensure_seat(actions, house="osiris", handle="Rvacant", source="test")
    cold = await ensure_seat(actions, house="osiris", handle="Rcold", source="test")
    await actions.create_or_find_object("Agent", "agent:rcold001", "test")
    await bind_holder(actions, seat_id=cold["seat_id"], agent_id="agent:rcold001")
    occupied = await ensure_seat(actions, house="osiris", handle="Roccup", source="test")
    await actions.create_or_find_object("Agent", "agent:rlive001", "test")
    await bind_holder(actions, seat_id=occupied["seat_id"], agent_id="agent:rlive001")
    await save_mount(actions.pool, job_dir="/jobs/rlive001", agent_id="agent:rlive001",
                     project="osiris", cwd="/w/osiris", model="claude-sonnet-5",
                     session_key=None)

    rows = {r["seat"]: r for r in (await roster(actions.pool))["seats"]}
    assert rows[vacant["seat_id"]]["occupancy"] == "vacant"
    assert rows[cold["seat_id"]]["occupancy"] == "cold"
    assert rows[occupied["seat_id"]]["occupancy"] == "occupied"


async def test_roster_pin_declared_from_a_real_osiris_file(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import roster

    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "coldspot"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Rpin1", source="test",
                             anchor_cwd=str(office))

    rows = {r["seat"]: r for r in (await roster(actions.pool))["seats"]}
    row = rows[seat["seat_id"]]
    assert row["pin"] == {"declared": "coldspot", "state": "declared",
                          "path": None, "error": None,
                          "triage_bucket": "no-such-project"}
    assert row["office_exists"] is True


async def test_roster_pin_unset_when_osiris_file_never_declares_project(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import roster

    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('model = "claude-fable-5"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Rpin2", source="test",
                             anchor_cwd=str(office))

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin"]["state"] == "unset"
    assert row["pin"]["declared"] is None


async def test_roster_pin_unreadable_on_malformed_toml(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import roster

    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project: "brokentoml"\n')  # colon, not `=` — bad TOML
    seat = await ensure_seat(actions, house="osiris", handle="Rpin3", source="test",
                             anchor_cwd=str(office))

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin"]["state"] == "unreadable"
    assert row["pin"]["error"] is not None


async def test_roster_pin_is_unknown_office_with_no_anchor_cwd_and_no_conventional_office(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Alfred's third live-reproduced defect (thread 3806, msg 4066): no `anchor_cwd`
    recorded is not the same claim as no office. This is the genuine miss — the probe of
    the conventional path (`~/.osiris/seats/<handle>/`, faked here so the test never
    touches the real filesystem) ALSO finds nothing — so the honest state is
    `unknown-office`, never the old `no-office`."""
    from src.orchestrator.seats import roster

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    seat = await ensure_seat(actions, house="osiris", handle="Rpin4", source="test")

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin"]["state"] == "unknown-office"
    assert row["probed_anchor_cwd"] is None
    assert row["office_exists"] is None


async def test_roster_pin_probes_the_conventional_office_when_no_anchor_cwd_is_recorded(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half of the same defect: a real, furnished office DOES exist at the
    conventional path, just never recorded as this seat's anchor_cwd — exactly Alfred's 7
    seats (Soundwave/vajra/Tantra/Ptah/Loupe/Ra/Marquee), invisible to roster() and, through
    it, to Imhotep's plan_pin_migration undercount. The probe finds it; `anchor_cwd` stays
    honestly null (the graph never recorded it) while `probed_anchor_cwd` shows what
    convention found."""
    from src.orchestrator.seats import roster

    fake_root = tmp_path / "seats"
    fake_root.mkdir()
    office = fake_root / "rpin5"
    office.mkdir()
    (office / ".osiris").write_text('project = "kast"\n')
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(fake_root))
    seat = await ensure_seat(actions, house="osiris", handle="Rpin5", source="test")

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["anchor_cwd"] is None
    assert row["probed_anchor_cwd"] == str(office)
    assert row["office_exists"] is True
    assert row["pin"] == {"declared": "kast", "state": "declared", "path": None,
                          "error": None, "triage_bucket": "no-such-project"}


async def test_roster_office_exists_is_false_for_a_vanished_path(actions: Actions) -> None:
    from src.orchestrator.seats import roster

    seat = await ensure_seat(actions, house="osiris", handle="Rpin5", source="test",
                             anchor_cwd="/nonexistent/office/path/for/this/test")

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["office_exists"] is False


async def test_roster_reports_chartered_repos_from_charter_of(actions: Actions) -> None:
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import roster

    await _repo(actions, "gestalt")
    seat = await ensure_seat(actions, house="osiris", handle="Rchart1", source="test")
    await set_charter(actions, seat["seat_id"], ["gestalt"], actor="test")

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["chartered_repos"] == ["gestalt"]


# --- pin_charter_agreement: the jesus/chad/marquee detector (Thoth DM 6279/6287) ---------

async def test_roster_pin_charter_agreement_agree_when_pin_is_among_charter(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import roster

    await _repo(actions, "godel")
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "godel"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Ragree1", source="test",
                             anchor_cwd=str(office))
    await set_charter(actions, seat["seat_id"], ["godel"], actor="test")

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin_charter_agreement"] == "agree"


async def test_roster_pin_charter_agreement_disagrees_the_jesus_chad_shape(
    actions: Actions, tmp_path: Path,
) -> None:
    """The live specimen (Thoth DM 6279): jesus's pin says 'Jesus', its real charter is
    'godel'. A mechanism that already carries both facts in one row must mark them as
    disagreeing rather than leave a reader to notice by eye."""
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import roster

    await _repo(actions, "godel")
    await _repo(actions, "jesus")
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "jesus"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Rdisagree1", source="test",
                             anchor_cwd=str(office))
    await set_charter(actions, seat["seat_id"], ["godel"], actor="test")

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin_charter_agreement"] == "disagree"


async def test_roster_pin_charter_agreement_na_when_pin_unset(
    actions: Actions, tmp_path: Path,
) -> None:
    """An unset pin is a valid state (ruling fe8ec7ff), never a defect — must read 'n/a',
    never 'disagree', even when a real charter exists to compare against."""
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import roster

    await _repo(actions, "godel")
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('model = "claude-fable-5"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Rna1", source="test",
                             anchor_cwd=str(office))
    await set_charter(actions, seat["seat_id"], ["godel"], actor="test")

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin_charter_agreement"] == "n/a"


async def test_roster_pin_charter_agreement_na_when_uncharted(
    actions: Actions, tmp_path: Path,
) -> None:
    """A declared pin with no charter at all has nothing to disagree WITH — 'n/a', not
    'disagree'. An uncharted worker is an already-visible, ordinary state."""
    from src.orchestrator.seats import roster

    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "coldspot"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Rna2", source="test",
                             anchor_cwd=str(office))

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin_charter_agreement"] == "n/a"


async def test_roster_pin_charter_agreement_agrees_when_pin_is_one_of_several_charters(
    actions: Actions, tmp_path: Path,
) -> None:
    """A seat legitimately governing several repos while pinned to just one of them is
    NOT a disagreement — the mark only fires when the pin's own value is absent from the
    whole charter set, never merely because the charter has more than one entry."""
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import roster

    await _repo(actions, "godel")
    await _repo(actions, "osiris-console")
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "godel"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Ragree2", source="test",
                             anchor_cwd=str(office))
    await set_charter(actions, seat["seat_id"], ["godel", "osiris-console"], actor="test")

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin_charter_agreement"] == "agree"


async def test_roster_live_cwd_only_populated_when_occupied(actions: Actions) -> None:
    from src.orchestrator.seats import roster

    cold = await ensure_seat(actions, house="osiris", handle="Rlive1", source="test")
    await actions.create_or_find_object("Agent", "agent:rlv-cold", "test")
    await bind_holder(actions, seat_id=cold["seat_id"], agent_id="agent:rlv-cold")
    occupied = await ensure_seat(actions, house="osiris", handle="Rlive2", source="test")
    await actions.create_or_find_object("Agent", "agent:rlv-live", "test")
    await bind_holder(actions, seat_id=occupied["seat_id"], agent_id="agent:rlv-live")
    await save_mount(actions.pool, job_dir="/jobs/rlv-live", agent_id="agent:rlv-live",
                     project="osiris", cwd="/actual/live/cwd", model="claude-sonnet-5",
                     session_key=None)

    rows = {r["seat"]: r for r in (await roster(actions.pool))["seats"]}
    assert rows[cold["seat_id"]]["live_cwd"] is None
    assert rows[occupied["seat_id"]]["live_cwd"] == "/actual/live/cwd"


async def test_roster_repo_lookup_single_match_via_charter(actions: Actions) -> None:
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import roster

    await _repo(actions, "sutra")
    seat = await ensure_seat(actions, house="alfred", handle="Rlk1", source="test")
    await set_charter(actions, seat["seat_id"], ["sutra"], actor="test")

    out = await roster(actions.pool, repo="sutra")
    assert out["agreement"] == "single-match"
    assert out["matches"] == [{"seat": seat["seat_id"], "handle": "Rlk1",
                               "occupancy": "vacant", "holder": None, "via": ["charter"]}]


async def test_roster_repo_lookup_single_match_via_pin(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import roster

    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "phanspeed"\n')
    await ensure_seat(actions, house="alfred", handle="Rlk2", source="test",
                      anchor_cwd=str(office))

    out = await roster(actions.pool, repo="phanspeed")
    assert out["agreement"] == "single-match"
    assert out["matches"][0]["via"] == ["pin"]


async def test_roster_repo_lookup_conflict_when_charter_and_pin_disagree(
    actions: Actions, tmp_path: Path,
) -> None:
    """The exact shape of Alfred's incident, generalized: two independent signals naming
    different seats for the same repo must come back as BOTH, never silently one."""
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import roster

    await _repo(actions, "mudra")
    charter_seat = await ensure_seat(actions, house="alfred", handle="Rlk3a", source="test")
    await set_charter(actions, charter_seat["seat_id"], ["mudra"], actor="test")
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "mudra"\n')
    pin_seat = await ensure_seat(actions, house="alfred", handle="Rlk3b", source="test",
                                 anchor_cwd=str(office))

    out = await roster(actions.pool, repo="mudra")
    assert out["agreement"] == "conflict"
    seats_found = {m["seat"] for m in out["matches"]}
    assert seats_found == {charter_seat["seat_id"], pin_seat["seat_id"]}


async def test_roster_repo_lookup_governed_when_charter_seat_manages_pin_seat(
    actions: Actions, tmp_path: Path,
) -> None:
    """Alfred's live review (thread 3806, finding 3): a coordinator's charter and a worker
    it manages pinning the SAME repo is the normal, correctly-configured shape (his own
    house: 8 repos, each governed by him and pinned by a different worker) — not a
    `conflict`, which trained readers to skip the word."""
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import roster

    await _repo(actions, "kast")
    coordinator = await ensure_seat(actions, house="alfred", handle="Rlk4a", source="test")
    await set_charter(actions, coordinator["seat_id"], ["kast"], actor="test")
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "kast"\n')
    worker = await ensure_seat(actions, house="alfred", handle="Rlk4b", source="test",
                               anchor_cwd=str(office))
    worker_oid = await actions.create_or_find_object("Seat", worker["seat_id"], "test")
    coordinator_oid = await actions.create_or_find_object(
        "Seat", coordinator["seat_id"], "test")
    await actions.create_link(worker_oid, coordinator_oid, "managed_by", "test",
                              datetime.now(UTC), 0.9, evidence_class="self_declared")

    out = await roster(actions.pool, repo="kast")
    assert out["agreement"] == "governed"
    seats_found = {m["seat"] for m in out["matches"]}
    assert seats_found == {coordinator["seat_id"], worker["seat_id"]}


async def test_roster_repo_lookup_stays_conflict_when_the_manager_edge_points_the_other_way(
    actions: Actions, tmp_path: Path,
) -> None:
    """`governed` requires the CHARTER-seat to manage the PIN-seat specifically — the
    reverse (pin-seat manages charter-seat) is not the shape Alfred named and stays a real
    `conflict`, same as two genuinely unrelated seats."""
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import roster

    await _repo(actions, "sutra2")
    charter_seat = await ensure_seat(actions, house="alfred", handle="Rlk5a", source="test")
    await set_charter(actions, charter_seat["seat_id"], ["sutra2"], actor="test")
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "sutra2"\n')
    pin_seat = await ensure_seat(actions, house="alfred", handle="Rlk5b", source="test",
                                 anchor_cwd=str(office))
    charter_oid = await actions.create_or_find_object("Seat", charter_seat["seat_id"], "test")
    pin_oid = await actions.create_or_find_object("Seat", pin_seat["seat_id"], "test")
    await actions.create_link(charter_oid, pin_oid, "managed_by", "test",
                              datetime.now(UTC), 0.9, evidence_class="self_declared")

    out = await roster(actions.pool, repo="sutra2")
    assert out["agreement"] == "conflict"


async def test_roster_repo_lookup_near_misses_on_case_and_separator_mismatch(
    actions: Actions, tmp_path: Path,
) -> None:
    """Alfred live-reproduced this exact shape (thread 3806, finding 2): the operator
    renamed a repo family-wide (`RAMstein` -> `ramstein`) while a seat's pin still carried
    the old spelling. A bare `no-match` gave no evidence; near_misses does."""
    from src.orchestrator.seats import roster

    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "RAMstein"\n')
    seat = await ensure_seat(actions, house="alfred", handle="Rlk6", source="test",
                             anchor_cwd=str(office))

    out = await roster(actions.pool, repo="ramstein")
    assert out["agreement"] == "no-match"
    assert out["matches"] == []
    assert out["near_misses"] == [
        {"repo": "RAMstein", "seat": seat["seat_id"], "via": ["pin"], "differs_by": "case"}]


async def test_roster_repo_lookup_near_misses_empty_when_agreement_is_not_no_match(
    actions: Actions,
) -> None:
    """The extra scan only ever runs on a bare no-match (Alfred's own scoping: "costs one
    extra pass on the miss path only") — a real match never pays for it."""
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import roster

    await _repo(actions, "gestalt")
    seat = await ensure_seat(actions, house="alfred", handle="Rlk7", source="test")
    await set_charter(actions, seat["seat_id"], ["gestalt"], actor="test")

    out = await roster(actions.pool, repo="gestalt")
    assert out["agreement"] == "single-match"
    assert out["near_misses"] == []


async def test_roster_repo_lookup_no_match_is_not_no_owner(actions: Actions) -> None:
    from src.orchestrator.seats import roster

    out = await roster(actions.pool, repo="never-declared-anywhere")
    assert out["agreement"] == "no-match"
    assert out["matches"] == []
    assert any("not that the repo has no owner" in c for c in out["caveats"])


async def test_roster_pin_triage_bucket_none_when_nothing_declared(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator.seats import roster

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    seat = await ensure_seat(actions, house="osiris", handle="Rtri1", source="test")

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin"]["state"] == "unknown-office"
    assert row["pin"]["triage_bucket"] is None


async def test_roster_pin_triage_bucket_reuses_triages_own_verdict(
    actions: Actions, tmp_path: Path,
) -> None:
    """Thread 251443ff's whole point: roster does not invent a second project-health
    notion, it asks triage's own buckets mode and reports the answer verbatim — a fresh
    zero-link SoftwareProject reads 'orphan' by triage's own priority order, same as it
    would from triage(mode='buckets') directly."""
    from src.orchestrator.seats import roster

    await _repo(actions, "orphanproj")
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "orphanproj"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Rtri2", source="test",
                             anchor_cwd=str(office))

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin"]["state"] == "declared"
    assert row["pin"]["triage_bucket"] == "orphan"


async def test_roster_duplicate_suspect_names_its_siblings_never_a_winner(
    actions: Actions, tmp_path: Path,
) -> None:
    """Task #152 Build 2 (Thoth's dispatch, msg 4215): a seat's own pin can resolve to a
    real, populated object that STILL reads duplicate_suspect because an unrelated,
    unpopulated near-duplicate exists elsewhere — the exact werner/maat/till/aegis shape
    (maat/till/aegis confirmed live). `duplicate_siblings` must name the OTHER object(s) and
    their own agent_count so a reader can judge the collision themselves; it must NEVER pick
    a winner (#102, ruling 8cdf905) — the seat's own bucket stays exactly whatever triage
    already computed, unchanged."""
    from src.orchestrator.seats import roster

    real = await actions.create_or_find_object("SoftwareProject", "repo:RAMstein", "test")
    await actions.create_or_find_object("SoftwareProject", "repo:ramstein", "test")
    agent = await actions.create_or_find_object("Agent", "agent:rdup0001", "test")
    await actions.create_link(agent, real, "works_in", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")

    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "RAMstein"\n')
    seat = await ensure_seat(actions, house="alfred", handle="Rdup1", source="test",
                             anchor_cwd=str(office))

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin"]["triage_bucket"] == "duplicate_suspect"   # untouched, no winner picked
    assert row["pin"]["duplicate_siblings"] == [
        {"canonical": "repo:ramstein", "agent_count": 0}]


async def test_roster_duplicate_siblings_absent_outside_duplicate_suspect(
    actions: Actions, tmp_path: Path,
) -> None:
    """The field must not appear at all (not even as an empty list) for any other bucket —
    computing it costs a real query, so it is never paid on the common, unflagged path."""
    from src.orchestrator.seats import roster

    await _repo(actions, "orphanproj")
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "orphanproj"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Rdup2", source="test",
                             anchor_cwd=str(office))

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin"]["triage_bucket"] == "orphan"
    assert "duplicate_siblings" not in row["pin"]


async def test_roster_names_a_pin_that_matches_a_name_not_a_canonical(
    actions: Actions, tmp_path: Path,
) -> None:
    """Task #152/#157 arc: the exact khepri mistake shape (decisions 126210f0/23b667d0) —
    a pin holding a project's post-rename DISPLAY NAME instead of its stable CANONICAL
    SUFFIX. roster()'s canonical-only lookup correctly reports no-such-project (nothing has
    THIS canonical); `pin.name_resolution` names what it actually found, WITHOUT ever
    implying the pin should be silently trusted or auto-corrected."""
    from src.orchestrator.seats import roster

    proj = await actions.create_or_find_object("SoftwareProject", "repo:tony", "test")
    await actions.assert_property(proj, "name", "cultural-infrastructure", "test",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "cultural-infrastructure"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Rname1", source="test",
                             anchor_cwd=str(office))

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin"]["triage_bucket"] == "no-such-project"   # canonical lookup, unchanged
    assert row["pin"]["name_resolution"] == {
        "resolved_by": "name", "canonical": "repo:tony",
        "note": ("this pin's value matches an existing object's current NAME, not its "
                 "canonical suffix — a pin should hold the canonical (stable across a "
                 "rename), never a display name; correct the pin to the canonical shown "
                 "here, never to the name it currently holds"),
    }


async def test_roster_names_resolution_stays_none_when_the_pin_genuinely_matches_nothing(
    actions: Actions, tmp_path: Path,
) -> None:
    """A pin that resolves via neither canonical nor name gets NO name_resolution field at
    all — the field's presence itself is the signal that something was found by name."""
    from src.orchestrator.seats import roster

    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "nothing-answers-to-this"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Rname2", source="test",
                             anchor_cwd=str(office))

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin"]["triage_bucket"] == "no-such-project"
    assert "name_resolution" not in row["pin"]


async def test_roster_names_resolution_reports_ambiguity_never_picks(
    actions: Actions, tmp_path: Path,
) -> None:
    """Two live objects answering to the same name (#110's ballgem/sutra shape) must report
    ambiguous:true and every candidate — never silently prefer one (#102's law)."""
    from src.orchestrator.seats import roster

    p1 = await actions.create_or_find_object("SoftwareProject", "repo:twin-a", "test")
    p2 = await actions.create_or_find_object("SoftwareProject", "repo:twin-b", "test")
    for p in (p1, p2):
        await actions.assert_property(p, "name", "sharedname", "test", datetime.now(UTC),
                                      0.9, evidence_class="self_declared")
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "sharedname"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Rname3", source="test",
                             anchor_cwd=str(office))

    row = next(r for r in (await roster(actions.pool))["seats"]
              if r["seat"] == seat["seat_id"])
    assert row["pin"]["triage_bucket"] == "no-such-project"
    assert row["pin"]["name_resolution"]["ambiguous"] is True
    assert row["pin"]["name_resolution"]["candidates"] == ["repo:twin-a", "repo:twin-b"]


async def test_roster_always_returns_caveats(actions: Actions) -> None:
    from src.orchestrator.seats import roster

    out = await roster(actions.pool)
    assert out["caveats"], (
        "a roster with no stated blind spots is the exact failure mode it exists to avoid")
    assert any("canonical" in c for c in out["caveats"])


# ═══ TREE LEDGER (task #158, dispatch msg 3900) — the pin-vs-graph disagreement report. ═══
# Off Sekhmet's live repo:seats/repo:code phantom catch (rulings 719ed5b1/13af22fc): every
# active SoftwareProject judged against the fleet's own declared-name index (`declared_by`),
# and every cwd agent_mounts holds right now cross-checked against what the graph currently
# believes. Live-measured against the real dev graph before these were written: the
# instrument independently re-found both known phantoms (repo:seats, repo:code) as
# phantom-suspect with zero prior knowledge of them baked in.

async def _agent_works_in(actions: Actions, agent_id: str, project: str) -> None:
    """Mint an Agent and a live `works_in` edge to `project` — same pattern
    test_agents.py's own mint_heir tests use for wiring an agent's graph attribution."""
    aoid = await actions.create_or_find_object("Agent", agent_id, "test")
    poid = await actions.create_or_find_object("SoftwareProject", f"repo:{project}", "test")
    await actions.create_link(aoid, poid, "works_in", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")


async def test_phantom_verdict_names_a_known_test_fixture(actions: Actions) -> None:
    from src.orchestrator.seats import project_ledger

    await _repo(actions, "smoketest")

    out = await project_ledger(actions.pool)
    row = next(p for p in out["projects"] if p["project"] == "repo:smoketest")
    assert row["phantom_verdict"] == "test-fixture"


async def test_phantom_verdict_is_declared_when_a_seat_pins_it(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import project_ledger

    await _repo(actions, "arealproject")
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "arealproject"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Tled1", source="test",
                             anchor_cwd=str(office))

    out = await project_ledger(actions.pool)
    row = next(p for p in out["projects"] if p["project"] == "repo:arealproject")
    assert row["phantom_verdict"] == "declared"
    assert row["declared_by"] == [seat["seat_id"]]


async def test_phantom_verdict_is_declared_via_seat_origin_charter(actions: Actions) -> None:
    """Sekhmet's own calibration (msg 3906): only a Seat-origin governs edge counts as a
    real charter. set_charter always mints Seat-origin edges (charter_of: l.from_id=seat),
    so this proves the declared path fires through it without any extra code of its own —
    an Agent-origin edge (the class that legitimized repo:code's own bogus edge) is never
    reachable through charter_of at all."""
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import project_ledger

    await _repo(actions, "charteredproj")
    seat = await ensure_seat(actions, house="osiris", handle="Tled2", source="test")
    await set_charter(actions, seat["seat_id"], ["charteredproj"], actor="test")

    out = await project_ledger(actions.pool)
    row = next(p for p in out["projects"] if p["project"] == "repo:charteredproj")
    assert row["phantom_verdict"] == "declared"


async def test_phantom_verdict_suspects_a_generic_basename_with_no_declaration(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import project_ledger

    await _repo(actions, "tmp")

    out = await project_ledger(actions.pool)
    row = next(p for p in out["projects"] if p["project"] == "repo:tmp")
    assert row["phantom_verdict"] == "phantom-suspect"
    assert row["declared_by"] == []


async def test_phantom_verdict_is_undetermined_when_neither_test_fires(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import project_ledger

    await _repo(actions, "myrealproject")

    out = await project_ledger(actions.pool)
    row = next(p for p in out["projects"] if p["project"] == "repo:myrealproject")
    assert row["phantom_verdict"] == "undetermined"


async def test_project_ledger_reuses_triage_bucket_verbatim(actions: Actions) -> None:
    from src.orchestrator.seats import project_ledger

    await _repo(actions, "orphanledger")

    out = await project_ledger(actions.pool)
    row = next(p for p in out["projects"] if p["project"] == "repo:orphanledger")
    assert row["triage_bucket"] == "orphan"


async def test_project_ledger_pagination_is_honest(actions: Actions) -> None:
    from src.orchestrator.seats import project_ledger

    await _repo(actions, "pageone")
    await _repo(actions, "pagetwo")

    out = await project_ledger(actions.pool, limit=1, offset=0)
    assert len(out["projects"]) == 1
    assert out["total"] >= 2
    assert out["limit"] == 1


async def test_live_cwd_no_graph_yet_when_nothing_works_in_it(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import live_cwd_ledger

    office = tmp_path / "office"
    office.mkdir()
    await actions.create_or_find_object("Agent", "agent:ledgerbare1", "test")
    await save_mount(actions.pool, job_dir="/jobs/ledgerbare1", agent_id="agent:ledgerbare1",
                     project=None, cwd=str(office), model="claude-sonnet-5",
                     session_key=None)

    out = await live_cwd_ledger(actions.pool)
    row = next(c for c in out["cwds"] if c["cwd"] == str(office))
    assert row["agreement"] == "no-graph-yet"


async def test_live_cwd_matches_when_pin_and_graph_agree(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import live_cwd_ledger

    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "ledgermatch"\n')
    await _agent_works_in(actions, "agent:ledgermatch1", "ledgermatch")
    await save_mount(actions.pool, job_dir="/jobs/ledgermatch1",
                     agent_id="agent:ledgermatch1", project="ledgermatch",
                     cwd=str(office), model="claude-sonnet-5", session_key=None)

    out = await live_cwd_ledger(actions.pool)
    row = next(c for c in out["cwds"] if c["cwd"] == str(office))
    assert row["resolved_today"] == "ledgermatch"
    assert row["graph_believes"] == ["ledgermatch"]
    assert row["agreement"] == "match"


async def test_live_cwd_partial_match_when_graph_carries_extra_stale_belief(
    actions: Actions, tmp_path: Path,
) -> None:
    """The residue class, measured live tonight on osiris's own worktrees: today's
    resolution is correct, but the graph ALSO carries an older belief for the same cwd —
    worth a look, not urgent, and must not read the same as a genuine mismatch."""
    from src.orchestrator.seats import live_cwd_ledger

    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "ledgerpartial"\n')
    await _agent_works_in(actions, "agent:ledgerpartial1", "ledgerpartial")
    await _agent_works_in(actions, "agent:ledgerpartial2", "stalebelief")
    await save_mount(actions.pool, job_dir="/jobs/ledgerpartial1",
                     agent_id="agent:ledgerpartial1", project="ledgerpartial",
                     cwd=str(office), model="claude-sonnet-5", session_key=None)
    await save_mount(actions.pool, job_dir="/jobs/ledgerpartial2",
                     agent_id="agent:ledgerpartial2", project="stalebelief",
                     cwd=str(office), model="claude-sonnet-5", session_key=None)

    out = await live_cwd_ledger(actions.pool)
    row = next(c for c in out["cwds"] if c["cwd"] == str(office))
    assert row["agreement"] == "partial-match"
    assert set(row["graph_believes"]) == {"ledgerpartial", "stalebelief"}


async def test_live_cwd_mismatch_when_resolution_and_graph_fully_disagree(
    actions: Actions, tmp_path: Path,
) -> None:
    """The live risk this instrument exists to catch: a currently-unpinned tree that would
    basename-fallback to a NEW name today, while the graph believes something else
    entirely — the exact shape measured live on two real seats tonight (flip68real,
    resumelanecheck)."""
    from src.orchestrator.seats import live_cwd_ledger

    office = tmp_path / "mismatchtree"
    office.mkdir()
    await _agent_works_in(actions, "agent:ledgermismatch1", "elsewhere")
    await save_mount(actions.pool, job_dir="/jobs/ledgermismatch1",
                     agent_id="agent:ledgermismatch1", project="elsewhere",
                     cwd=str(office), model="claude-sonnet-5", session_key=None)

    out = await live_cwd_ledger(actions.pool)
    row = next(c for c in out["cwds"] if c["cwd"] == str(office))
    assert row["resolved_today"] == "mismatchtree"
    assert row["graph_believes"] == ["elsewhere"]
    assert row["agreement"] == "mismatch"


async def test_live_cwd_graph_only_at_the_bare_office_root(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """13af22fc's own historical signature (repo:seats), reproduced live: today's code
    correctly REFUSES to resolve anything at the bare seats container
    (resolved_today=None), while the graph still carries a belief there from before the
    fix — exactly what should stay visible, not silently absent."""
    from src.orchestrator.seats import live_cwd_ledger

    fake_root = tmp_path / "seats"
    fake_root.mkdir()
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(fake_root))
    await _agent_works_in(actions, "agent:ledgerbareroot1", "seats")
    await save_mount(actions.pool, job_dir="/jobs/ledgerbareroot1",
                     agent_id="agent:ledgerbareroot1", project="seats",
                     cwd=str(fake_root), model="claude-sonnet-5", session_key=None)

    out = await live_cwd_ledger(actions.pool)
    row = next(c for c in out["cwds"] if c["cwd"] == str(fake_root))
    assert row["resolved_today"] is None
    assert row["graph_believes"] == ["seats"]
    assert row["agreement"] == "graph-only"


async def test_live_cwd_ghost_when_the_office_directory_is_gone(
    actions: Actions, tmp_path: Path,
) -> None:
    """Thoth's own catch (msg 3928), reproduced exactly: flip68real/resumelanecheck's
    directories were DELETED, not merely unpinned. The canonical `.osiris` reader climbs
    past a missing cwd to its PARENT's own pin without ever checking `cwd` itself exists —
    collapsing 'office is gone' into 'office exists, pin unset', two conditions with
    OPPOSITE dispositions (one wants a pin written, the other wants the graph's belief
    reaped). This proves the fix: a nonexistent cwd with a real parent pin must read
    directory_exists=False, pin_state='missing-directory', resolved_today=None,
    agreement='ghost' — never 'unset'/'mismatch', this instrument's own first-draft bug."""
    from src.orchestrator.seats import live_cwd_ledger

    container = tmp_path / "seats"
    container.mkdir()
    (container / ".osiris").write_text('kind = "container"\n')
    ghost_office = container / "deletedseat"  # never created — the office is GONE

    await _agent_works_in(actions, "agent:ledgerghost1", "osiris")
    await save_mount(actions.pool, job_dir="/jobs/ledgerghost1",
                     agent_id="agent:ledgerghost1", project="osiris",
                     cwd=str(ghost_office), model="claude-sonnet-5", session_key=None)

    out = await live_cwd_ledger(actions.pool)
    row = next(c for c in out["cwds"] if c["cwd"] == str(ghost_office))
    assert row["directory_exists"] is False
    assert row["pin"]["state"] == "missing-directory"
    assert row["resolved_today"] is None
    assert row["graph_believes"] == ["osiris"]
    assert row["agreement"] == "ghost"


async def test_tree_ledger_combines_both_sections_and_caveats(actions: Actions) -> None:
    from src.orchestrator.seats import tree_ledger

    out = await tree_ledger(actions.pool)
    assert "projects" in out["project_ledger"]
    assert "cwds" in out["live_cwd_ledger"]
    assert out["caveats"], (
        "an instrument with no stated blind spots is the exact failure mode it exists to "
        "prevent")
    assert any("agent_mounts" in c for c in out["caveats"])


async def test_project_ledger_surfaces_the_generic_basename_list_in_the_output(
    actions: Actions,
) -> None:
    """Thoth's own instruction (msg 3920): a hidden deny-list is an unfalsifiable claim —
    the editable phantom-suspect basis must be VISIBLE in the data a caller actually reads,
    not just documented in a docstring or a private module constant."""
    from src.orchestrator.seats import _GENERIC_PATH_BASENAMES, project_ledger

    out = await project_ledger(actions.pool)
    assert out["phantom_verdict_basis"]["phantom-suspect"] == sorted(_GENERIC_PATH_BASENAMES)
    assert "mechanical" in out["note"].lower()


async def test_live_cwd_ledger_states_its_own_non_durability_in_section(
    actions: Actions,
) -> None:
    """Thoth's own instruction (msg 3920): 'nobody reads the bottom' — the caveat belongs
    where a reader hits it, inside this section's own output, not only in the report-level
    caveats list."""
    from src.orchestrator.seats import live_cwd_ledger

    out = await live_cwd_ledger(actions.pool)
    assert "agent_mounts" in out["note"]
    assert "evict" in out["note"].lower()


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


# ═══ SEATS_MANAGED_BY (msg 4761/4798, obligation f6a40441 — the reverse of manager_of_seat:
# "does this seat manage anyone", the one non-inferred signal a manager-authored-code flag
# could ever use) ═══════════════════════════════════════════════════════════════════════

async def test_seats_managed_by_lists_every_active_worker(actions: Actions) -> None:
    from src.orchestrator.seats import seats_managed_by

    manager = await actions.create_or_find_object("Seat", "seat:smb1mgr0", "test")
    w1 = await actions.create_or_find_object("Seat", "seat:smb1wk01", "test")
    w2 = await actions.create_or_find_object("Seat", "seat:smb1wk02", "test")
    await actions.create_link(w1, manager, "managed_by", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    await actions.create_link(w2, manager, "managed_by", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")

    out = await seats_managed_by(actions.pool, "seat:smb1mgr0")
    assert out == ["seat:smb1wk01", "seat:smb1wk02"]


async def test_seats_managed_by_empty_for_a_worker_seat(actions: Actions) -> None:
    """The correct read for "not a manager": genuinely empty, not merely un-checked."""
    from src.orchestrator.seats import seats_managed_by

    manager = await actions.create_or_find_object("Seat", "seat:smb2mgr0", "test")
    worker = await actions.create_or_find_object("Seat", "seat:smb2wk00", "test")
    await actions.create_link(worker, manager, "managed_by", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")

    assert await seats_managed_by(actions.pool, "seat:smb2wk00") == []


async def test_seats_managed_by_ignores_a_detached_worker(actions: Actions) -> None:
    """An INVALIDATED managed_by edge (detach_seat's own act) must not still count — this
    reads live edges only, matching manager_of_seat's own valid_until guard."""
    from src.orchestrator.seats import detach_seat, seats_managed_by

    manager = await actions.create_or_find_object("Seat", "seat:smb3mgr0", "test")
    worker = await actions.create_or_find_object("Seat", "seat:smb3wk00", "test")
    await actions.create_link(worker, manager, "managed_by", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    assert await seats_managed_by(actions.pool, "seat:smb3mgr0") == ["seat:smb3wk00"]

    await detach_seat(actions, "seat:smb3wk00", because="promoted", actor="test")
    assert await seats_managed_by(actions.pool, "seat:smb3mgr0") == []


# ═══ DETACH_SEAT (thread fad0dc14) — the toolkit hole: unpeer heals peer_of, nothing healed
# managed_by before this. A coordinator is DEFINED by having no manager, so this is a
# REMOVAL of the edge, never a repoint. ══════════════════════════════════════════════════


async def test_detach_seat_removes_the_managed_by_edge(actions: Actions) -> None:
    from src.orchestrator.boot_compiler import derive_role
    from src.orchestrator.seats import detach_seat, manager_of_seat

    worker = await actions.create_or_find_object("Seat", "seat:det1aaaa", "test")
    manager = await actions.create_or_find_object("Seat", "seat:det1bbbb", "test")
    await actions.create_link(worker, manager, "managed_by", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    assert await derive_role(actions.pool, "seat:det1aaaa") == "worker"

    out = await detach_seat(actions, "seat:det1aaaa", because="promoted to coordinator",
                            actor="test")

    assert out == {"detached": "seat:det1aaaa", "was_managed_by": "seat:det1bbbb",
                   "because": "promoted to coordinator"}
    assert await manager_of_seat(actions.pool, "seat:det1aaaa") is None
    assert await derive_role(actions.pool, "seat:det1aaaa") == "coordinator"
    reason = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='detached_because' WHERE o.canonical=$1",
        "seat:det1aaaa")
    assert reason == "promoted to coordinator"


async def test_detach_seat_refuses_blank_because(actions: Actions) -> None:
    from src.orchestrator.seats import detach_seat

    worker = await actions.create_or_find_object("Seat", "seat:det2aaaa", "test")
    manager = await actions.create_or_find_object("Seat", "seat:det2bbbb", "test")
    await actions.create_link(worker, manager, "managed_by", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")

    out = await detach_seat(actions, "seat:det2aaaa", because=" ", actor="test")
    assert "because is required" in out["error"]


async def test_detach_seat_refuses_an_unmanaged_seat(actions: Actions) -> None:
    from src.orchestrator.seats import detach_seat

    await actions.create_or_find_object("Seat", "seat:det3cccc", "test")

    out = await detach_seat(actions, "seat:det3cccc", because="test", actor="test")
    assert "no active manager" in out["error"]


async def test_detach_seat_refuses_an_unknown_seat(actions: Actions) -> None:
    from src.orchestrator.seats import detach_seat

    out = await detach_seat(actions, "seat:no-such-seat", because="test", actor="test")
    assert "no such active seat" in out["error"]


# ═══ ATTACH_SEAT (thread fad0dc14, the other half) — managed_by is created in exactly two
# places in the whole codebase (mint_seat's birth edge, fold_seat's re-point); this is the
# third, deliberate one, the mirror of detach_seat above. ═══════════════════════════════

async def test_attach_seat_creates_the_managed_by_edge(actions: Actions) -> None:
    from src.orchestrator.boot_compiler import derive_role
    from src.orchestrator.seats import attach_seat, manager_of_seat

    await actions.create_or_find_object("Seat", "seat:att1aaaa", "test")
    await actions.create_or_find_object("Seat", "seat:att1bbbb", "test")
    assert await derive_role(actions.pool, "seat:att1aaaa") == "coordinator"

    out = await attach_seat(actions, "seat:att1aaaa", "seat:att1bbbb",
                            evidence="operator: alfred manages this seat", actor="test")

    assert out == {"attached": "seat:att1aaaa", "now_managed_by": "seat:att1bbbb",
                   "evidence": "operator: alfred manages this seat"}
    assert await manager_of_seat(actions.pool, "seat:att1aaaa") == "seat:att1bbbb"
    assert await derive_role(actions.pool, "seat:att1aaaa") == "worker"
    reason = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='attached_evidence' WHERE o.canonical=$1",
        "seat:att1aaaa")
    assert reason == "operator: alfred manages this seat"


async def test_attach_seat_refuses_blank_evidence(actions: Actions) -> None:
    from src.orchestrator.seats import attach_seat

    await actions.create_or_find_object("Seat", "seat:att2aaaa", "test")
    await actions.create_or_find_object("Seat", "seat:att2bbbb", "test")

    out = await attach_seat(actions, "seat:att2aaaa", "seat:att2bbbb", evidence=" ",
                            actor="test")
    assert "evidence is required" in out["error"]


async def test_attach_seat_refuses_an_unknown_worker(actions: Actions) -> None:
    from src.orchestrator.seats import attach_seat

    await actions.create_or_find_object("Seat", "seat:att3bbbb", "test")

    out = await attach_seat(actions, "seat:no-such-worker", "seat:att3bbbb",
                            evidence="test", actor="test")
    assert "no such active seat" in out["error"]


async def test_attach_seat_refuses_an_unknown_manager(actions: Actions) -> None:
    from src.orchestrator.seats import attach_seat

    await actions.create_or_find_object("Seat", "seat:att4aaaa", "test")

    out = await attach_seat(actions, "seat:att4aaaa", "seat:no-such-manager",
                            evidence="test", actor="test")
    assert "no such active seat" in out["error"]


async def test_attach_seat_refuses_self_management(actions: Actions) -> None:
    from src.orchestrator.seats import attach_seat

    await actions.create_or_find_object("Seat", "seat:att5aaaa", "test")

    out = await attach_seat(actions, "seat:att5aaaa", "seat:att5aaaa", evidence="test",
                            actor="test")
    assert "cannot manage itself" in out["error"]


async def test_attach_seat_refuses_a_silent_repoint(actions: Actions) -> None:
    from src.orchestrator.seats import attach_seat

    worker = await actions.create_or_find_object("Seat", "seat:att6aaaa", "test")
    original = await actions.create_or_find_object("Seat", "seat:att6bbbb", "test")
    await actions.create_or_find_object("Seat", "seat:att6cccc", "test")
    await actions.create_link(worker, original, "managed_by", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")

    out = await attach_seat(actions, "seat:att6aaaa", "seat:att6cccc", evidence="test",
                            actor="test")
    assert "already has an active manager" in out["error"]
    assert "seat:att6bbbb" in out["error"]


async def test_attach_seat_after_detach_seat_succeeds(actions: Actions) -> None:
    """The documented path for a real repoint: detach, then attach — not a single call
    silently swapping the manager out from under the worker."""
    from src.orchestrator.seats import attach_seat, detach_seat, manager_of_seat

    worker = await actions.create_or_find_object("Seat", "seat:att7aaaa", "test")
    old_manager = await actions.create_or_find_object("Seat", "seat:att7bbbb", "test")
    await actions.create_or_find_object("Seat", "seat:att7cccc", "test")
    await actions.create_link(worker, old_manager, "managed_by", "test", datetime.now(UTC),
                              0.9, evidence_class="self_declared")

    await detach_seat(actions, "seat:att7aaaa", because="reorg", actor="test")
    out = await attach_seat(actions, "seat:att7aaaa", "seat:att7cccc",
                            evidence="reorg: reassigned", actor="test")

    assert out["attached"] == "seat:att7aaaa" and out["now_managed_by"] == "seat:att7cccc"
    assert await manager_of_seat(actions.pool, "seat:att7aaaa") == "seat:att7cccc"


# --- the MCP tool wrapper (same srv._pool monkey-patch pattern test_doors.py's own wrapper
# test uses) -------------------------------------------------------------------------------

async def test_attach_seat_mcp_wrapper_delegates_and_stamps_the_caller(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.seats import manager_of_seat

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    await actions.create_or_find_object("Seat", "seat:attw1aaa", "test")
    await actions.create_or_find_object("Seat", "seat:attw1bbb", "test")
    ident = AgentIdentity(agent_id="agent:attachwrap1", session="attachwrap1",
                          project="p", model="claude-sonnet-5", cwd=None,
                          model_method="job_dir", model_history=("claude-sonnet-5",))
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = ident
    try:
        out = await srv.attach_seat("seat:attw1aaa", "seat:attw1bbb",
                                    "operator: real report line", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["attached"] == "seat:attw1aaa" and out["now_managed_by"] == "seat:attw1bbb"
    assert await manager_of_seat(actions.pool, "seat:attw1aaa") == "seat:attw1bbb"


async def test_attach_seat_mcp_wrapper_refuses_before_mount(actions: Actions) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.attach_seat("seat:no-mount-a", "seat:no-mount-b", "test")
    finally:
        srv._pool = saved_pool
    assert "mount first" in out["error"]


# ═══ RESOLVE_PROJECT (ruling 577988ed, hoisted msg 1888) — the ONE project resolver every
# reader (mount, the stop hook, census) now funnels through, replacing four hand-rolled
# `Path(cwd).name` copies that could mint a phantom "seats" project. ═══════════

async def test_resolve_project_a_seated_agent_gets_its_house_not_the_cwd(
    actions: Actions,
) -> None:
    """The core contract: a seated agent's project is its seat's derived house,
    unconditionally — cwd is irrelevant when a real seat backs the agent."""
    from src.orchestrator.seats import bind_holder, resolve_project

    head = await actions.create_or_find_object("Seat", "seat:rp1head0", "test")
    await actions.assert_property(head, "house", "osiris", "test", datetime.now(UTC), 0.9)
    worker = await actions.create_or_find_object("Seat", "seat:rp1wrk00", "test")
    await _link_managed_by(actions, worker, head)
    await actions.create_or_find_object("Agent", "agent:rp1aaaa0", "test")
    await bind_holder(actions, seat_id="seat:rp1wrk00", agent_id="agent:rp1aaaa0")

    assert await resolve_project(
        actions.pool, "agent:rp1aaaa0", "/some/unrelated/path") == "osiris"


async def test_resolve_project_a_seated_agent_in_its_own_office_gets_the_house_not_the_handle(
    actions: Actions,
) -> None:
    """THE LITERAL GUARD (msg 1888): `~/.osiris/seats/<handle>` must resolve to the seat's
    HOUSE, not the handle basename — exactly the shape a caller sees when cwd IS the
    agent's own office directory."""
    from src.orchestrator.seats import bind_holder, resolve_project

    head = await actions.create_or_find_object("Seat", "seat:rp2head0", "test")
    await actions.assert_property(head, "house", "osiris", "test", datetime.now(UTC), 0.9)
    worker = await actions.create_or_find_object("Seat", "seat:rp2wrk00", "test")
    await _link_managed_by(actions, worker, head)
    await actions.create_or_find_object("Agent", "agent:rp2aaaa0", "test")
    await bind_holder(actions, seat_id="seat:rp2wrk00", agent_id="agent:rp2aaaa0")

    office_cwd = str(Path.home() / ".osiris" / "seats" / "somehandle")
    assert await resolve_project(actions.pool, "agent:rp2aaaa0", office_cwd) == "osiris"


async def test_resolve_project_an_unseated_agent_at_the_bare_office_root_refuses(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE LIVE SPECIMEN (Thoth, msg 1888): an agent with no seat at all, sitting at the
    bare container root, must never mint the phantom "seats" — refuse (None), same honesty
    resolve_identity already keeps for the identical cwd."""
    from src.orchestrator.seats import resolve_project

    fake_root = tmp_path / ".osiris" / "seats"
    fake_root.mkdir(parents=True)
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(fake_root))
    await actions.create_or_find_object("Agent", "agent:rp3aaaa0", "test")

    assert await resolve_project(actions.pool, "agent:rp3aaaa0", str(fake_root)) is None


async def test_resolve_project_an_unseated_agent_falls_back_to_an_ordinary_cwd_guess(
    actions: Actions, tmp_path: Path,
) -> None:
    """No seat, no office-root hazard — an ordinary repo cwd guesses its basename exactly
    like resolve_identity's own cwd fold, the fallback this function exists to preserve."""
    from src.orchestrator.seats import resolve_project

    repo = tmp_path / "some-repo"
    repo.mkdir()
    await actions.create_or_find_object("Agent", "agent:rp4aaaa0", "test")

    assert await resolve_project(actions.pool, "agent:rp4aaaa0", str(repo)) == "some-repo"


async def test_resolve_project_an_unseated_agent_honors_the_osiris_pin_over_the_basename(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import resolve_project

    repo = tmp_path / "renamed-folder"
    repo.mkdir()
    (repo / ".osiris").write_text('project = "pinnedname"\n')
    await actions.create_or_find_object("Agent", "agent:rp5aaaa0", "test")

    assert await resolve_project(actions.pool, "agent:rp5aaaa0", str(repo)) == "pinnedname"


async def test_resolve_project_an_unseated_agent_with_no_cwd_at_all_is_none(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import resolve_project

    await actions.create_or_find_object("Agent", "agent:rp6aaaa0", "test")

    assert await resolve_project(actions.pool, "agent:rp6aaaa0", None) is None


# ═══ DERIVE_HOUSE (ruling ff6148b0, decision 4c9e4bd7) — house is DERIVED off the managed_by
# chain to the head, never a stored snapshot that drifts (Alfred's legacy bytebye, Vajra's
# twin house=vajra). The head's own stored house is the one legitimate anchor. ═══════════


async def _link_managed_by(
    actions: Actions, worker: Any, manager: Any, *, source: str = "test",
) -> None:
    await actions.create_link(worker, manager, "managed_by", source, datetime.now(UTC), 0.9,
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


# --- THE HOUSE ANCHOR (ruling b4208fa3, thread 105f3425/bec2e4af — cross-house adoption
# silently annexed Ferryman/halcyon into osiris and, escalated, leaked 50 of Thoth's own
# messages into a hector-vector seat's mailbox) ---------------------------------------------

async def test_derive_house_anchors_on_an_operator_sourced_managed_by_link(
    actions: Actions,
) -> None:
    """THE LIVE REPRO'S ACTUAL SHAPE (halcyon): an ALREADY-EXISTING seat's own `house`
    property keeps its ORIGINAL, unrelated source (an old agent id) — only the managed_by
    LINK itself carries today's operator-sourced adoption. The anchor must fire off the
    LINK's source, not just the property's, or this exact seat is silently missed."""
    from src.orchestrator.seats import derive_house

    worker = await actions.create_or_find_object("Seat", "seat:ha1worker", "test")
    await actions.assert_property(worker, "house", "hector-vector", "agent:old-mint",
                                  datetime.now(UTC), 0.9)
    manager = await actions.create_or_find_object("Seat", "seat:ha1mgr00", "test")
    await actions.assert_property(manager, "house", "osiris", "test", datetime.now(UTC), 0.9)
    await _link_managed_by(actions, worker, manager, source="operator")

    assert await derive_house(actions.pool, "seat:ha1worker") == "hector-vector"
    # the manager itself is untouched
    assert await derive_house(actions.pool, "seat:ha1mgr00") == "osiris"


async def test_derive_house_anchors_on_an_operator_sourced_house_property(
    actions: Actions,
) -> None:
    """THE LITERAL TEXT OF THE RULING: a seat whose own `house` property was freshly
    operator-stamped (Ferryman's own shape — minted fresh at the operator's word) anchors
    even if the managed_by link itself was asserted by an ordinary agent."""
    from src.orchestrator.seats import derive_house

    worker = await actions.create_or_find_object("Seat", "seat:ha2worker", "test")
    await actions.assert_property(worker, "house", "hector-vector", "operator",
                                  datetime.now(UTC), 0.9)
    manager = await actions.create_or_find_object("Seat", "seat:ha2mgr00", "test")
    await actions.assert_property(manager, "house", "osiris", "test", datetime.now(UTC), 0.9)
    await _link_managed_by(actions, worker, manager, source="agent:ordinary-worker")

    assert await derive_house(actions.pool, "seat:ha2worker") == "hector-vector"


async def test_derive_house_an_ordinary_managed_seat_still_derives_its_manager_s(
    actions: Actions,
) -> None:
    """NEITHER signal present (an ordinary worker, an ordinary manager, an ordinary link) —
    derivation is UNCHANGED: the worker still walks to its manager's house exactly as
    before this fix. The regression this fix must never introduce."""
    from src.orchestrator.seats import derive_house

    manager = await actions.create_or_find_object("Seat", "seat:ha3mgr00", "test")
    await actions.assert_property(manager, "house", "osiris", "test", datetime.now(UTC), 0.9)
    worker = await actions.create_or_find_object("Seat", "seat:ha3worker", "test")
    await actions.assert_property(worker, "house", "hector-vector", "agent:old-mint",
                                  datetime.now(UTC), 0.9)
    await _link_managed_by(actions, worker, manager, source="agent:ordinary-worker")

    assert await derive_house(actions.pool, "seat:ha3worker") == "osiris"


async def test_derive_house_anchor_chains_correctly_past_a_third_generation(
    actions: Actions,
) -> None:
    """An anchor's OWN manager still derives normally past it — the anchor stops the
    WALK for the anchored seat's own lookup, it doesn't turn its manager into an anchor
    too, and a seat BEYOND the anchor (if any) is unaffected by this specific edge."""
    from src.orchestrator.seats import derive_house

    grandhead = await actions.create_or_find_object("Seat", "seat:ha4head0", "test")
    await actions.assert_property(grandhead, "house", "osiris", "test", datetime.now(UTC), 0.9)
    anchor = await actions.create_or_find_object("Seat", "seat:ha4anchr", "test")
    await actions.assert_property(anchor, "house", "hector-vector", "agent:old-mint",
                                  datetime.now(UTC), 0.9)
    await _link_managed_by(actions, anchor, grandhead, source="operator")

    assert await derive_house(actions.pool, "seat:ha4anchr") == "hector-vector"
    assert await derive_house(actions.pool, "seat:ha4head0") == "osiris"


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
                     "anchor_cwd": "/home/vajra", "tree_cwd": None}


async def test_seat_facts_all_none_for_an_unknown_seat(actions: Actions) -> None:
    from src.orchestrator.seats import seat_facts

    assert await seat_facts(actions.pool, "seat:sf2ghost") == {
        "handle": None, "house": None, "intended_model": None, "anchor_cwd": None,
        "tree_cwd": None}


# ═══ ANCHOR_CWD BACKFILL (task #141 shape 3, decision eda71c32) ═══════════════════════════
# A seat OCCUPIED right now with no anchor_cwd ever captured has no durable trace of its
# office — the moment it goes cold, the location is gone from the graph entirely. This
# stamps anchor_cwd from a live, first-hand observation while one exists, additive-only.


async def test_backfill_stamps_anchor_cwd_for_an_occupied_seat_with_none_on_file(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import (
        backfill_anchor_cwd_from_live_observation,
        seat_facts,
    )

    seat = await ensure_seat(actions, house="rotten-apple", handle="Ra", source="test")
    await actions.create_or_find_object("Agent", "agent:baraobs1", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:baraobs1")
    await save_mount(actions.pool, job_dir="/jobs/baraobs1", agent_id="agent:baraobs1",
                     project="rotten-apple", cwd="/home/asuramaya/.osiris/seats/ra",
                     model="claude-sonnet-5", session_key=None)

    out = await backfill_anchor_cwd_from_live_observation(actions, actor="test")
    assert out["stamped"].get(seat["seat_id"]) == "/home/asuramaya/.osiris/seats/ra"
    assert seat["seat_id"] not in out["skipped_has_anchor"]
    assert seat["seat_id"] not in out["skipped_not_occupied"]
    assert seat["seat_id"] not in out["skipped_ambiguous"]

    facts = await seat_facts(actions.pool, seat["seat_id"])
    assert facts["anchor_cwd"] == "/home/asuramaya/.osiris/seats/ra"
    row = await actions.pool.fetchrow(
        "SELECT a.evidence_class FROM current_assertions a JOIN objects o "
        "ON o.id=a.object_id WHERE o.canonical=$1 AND a.name='anchor_cwd'", seat["seat_id"])
    assert row["evidence_class"] == "direct_observation"  # an OBSERVATION, never a declaration

    # idempotent-safe: a second run must not re-stamp or error — the seat now HAS an anchor
    out2 = await backfill_anchor_cwd_from_live_observation(actions, actor="test")
    assert seat["seat_id"] not in out2["stamped"]
    assert seat["seat_id"] in out2["skipped_has_anchor"]


async def test_backfill_never_overwrites_an_existing_anchor_cwd(actions: Actions) -> None:
    """The one absolute law: a seat that already has an answer, even a stale one, is this
    function's business only to skip — never to arbitrate or correct."""
    from src.orchestrator.seats import (
        backfill_anchor_cwd_from_live_observation,
        seat_facts,
    )

    seat = await ensure_seat(actions, house="alfred", handle="William", source="test")
    await actions.assert_property((await actions.create_or_find_object(
        "Seat", seat["seat_id"], "test")), "anchor_cwd",
        "/home/asuramaya/.osiris/seats/tjmax", "test", datetime.now(UTC), 0.9)
    await actions.create_or_find_object("Agent", "agent:barwm0001", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:barwm0001")
    await save_mount(actions.pool, job_dir="/jobs/barwm0001", agent_id="agent:barwm0001",
                     project="alfred", cwd="/somewhere/else/entirely",
                     model="claude-sonnet-5", session_key=None)

    out = await backfill_anchor_cwd_from_live_observation(actions, actor="test")
    assert seat["seat_id"] not in out["stamped"]
    assert seat["seat_id"] in out["skipped_has_anchor"]
    facts = await seat_facts(actions.pool, seat["seat_id"])
    assert facts["anchor_cwd"] == "/home/asuramaya/.osiris/seats/tjmax"  # untouched


async def test_backfill_skips_a_seat_that_is_not_occupied(actions: Actions) -> None:
    from src.orchestrator.seats import (
        backfill_anchor_cwd_from_live_observation,
        seat_facts,
    )

    seat = await ensure_seat(actions, house="osiris", handle="Vacant1", source="test")

    out = await backfill_anchor_cwd_from_live_observation(actions, actor="test")
    assert seat["seat_id"] not in out["stamped"]
    assert seat["seat_id"] in out["skipped_not_occupied"]
    facts = await seat_facts(actions.pool, seat["seat_id"])
    assert facts["anchor_cwd"] is None


async def test_backfill_writes_nothing_when_the_live_holder_is_ambiguous(
    actions: Actions,
) -> None:
    """More than one DISTINCT fresh cwd for the same holder — two concurrent sessions
    disagreeing — is not this function's call to arbitrate: a missing value is
    recoverable, a wrong one is not."""
    from src.orchestrator.seats import (
        backfill_anchor_cwd_from_live_observation,
        seat_facts,
    )

    seat = await ensure_seat(actions, house="dealer-to-fb", handle="Marquee", source="test")
    await actions.create_or_find_object("Agent", "agent:bamqamb1", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:bamqamb1")
    await save_mount(actions.pool, job_dir="/jobs/bamqamb1", agent_id="agent:bamqamb1",
                     project="dealer-to-fb", cwd="/path/one",
                     model="claude-sonnet-5", session_key=None)
    await save_mount(actions.pool, job_dir="/jobs/bamqamb2", agent_id="agent:bamqamb1",
                     project="dealer-to-fb", cwd="/path/two",
                     model="claude-sonnet-5", session_key=None)

    out = await backfill_anchor_cwd_from_live_observation(actions, actor="test")
    assert seat["seat_id"] not in out["stamped"]
    assert seat["seat_id"] in out["skipped_ambiguous"]
    facts = await seat_facts(actions.pool, seat["seat_id"])
    assert facts["anchor_cwd"] is None


async def test_the_window_tag_renders_an_anchored_seat_s_true_house(actions: Actions) -> None:
    """THE OPERATOR'S OWN SIGHTING (decision 105f3425): 'Ferryman said [OS] instead of
    [HE]'. seat_facts (the shared resolver launch()'s window tag is built from) must
    report the anchored seat's OWN house, not its osiris manager's, and _house_tag must
    render it correctly — the full path the operator actually looks at, proven together
    rather than trusting derive_house's own fix in isolation."""
    from src.orchestrator.seats import seat_facts
    from src.orchestrator.trigger import _house_tag

    manager = await actions.create_or_find_object("Seat", "seat:wt1mgr00", "test")
    await actions.assert_property(manager, "house", "osiris", "test", datetime.now(UTC), 0.9)
    worker = await actions.create_or_find_object("Seat", "seat:wt1worker", "test")
    await actions.assert_property(worker, "handle", "Ferryman", "test", datetime.now(UTC), 0.9)
    await actions.assert_property(worker, "house", "hector-vector", "agent:old-mint",
                                  datetime.now(UTC), 0.9)
    await _link_managed_by(actions, worker, manager, source="operator")

    facts = await seat_facts(actions.pool, "seat:wt1worker")
    assert facts["house"] == "hector-vector"
    assert _house_tag(facts["house"]) == "HE"  # not "OS" — the bug the operator caught live


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
    out = await set_seat_attended(actions, seat_id=seat, attended="human", actor="operator",
                                  because="this seat is operator-fronted")
    assert out == {"seat": seat, "attended": "human", "because": "this seat is operator-fronted"}
    assert await _attended_value(actions, seat) == "human"

    # a later stamp reversing it supersedes cleanly — one current value, not a pile-up
    await set_seat_attended(actions, seat_id=seat, attended="worker", actor="operator",
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
                                  actor="operator", because="no such seat exists")
    assert out == {"error": "no such seat: 'seat:nosuchsea'"}


async def test_set_seat_attended_refuses_a_retired_seat(actions: Actions) -> None:
    from src.orchestrator.seats import retire_seat, set_seat_attended

    seat = (await ensure_seat(actions, house="demo", handle="Gone", source="test"))["seat_id"]
    await retire_seat(actions, seat, reason="role is over", actor="test")
    out = await set_seat_attended(actions, seat_id=seat, attended="human", actor="operator",
                                  because="attempting to stamp a dead seat")
    assert "error" in out and "retired" in out["error"]


async def test_set_seat_attended_refuses_a_non_manager_non_operator(actions: Actions) -> None:
    """NEGATIVE CONTROL (census a5e53ed8/3f97f9c7, fixed 2026-08-02): this docstring
    claimed "OPERATOR-APPROVED TO CHANGE" for weeks while any mounted caller could stamp
    any seat's attendance signal — confirmed against pre-fix code (see every OTHER test
    in this section, all of which had to be updated to actor="operator" to keep passing,
    since none of them were testing authority before — there was none to test)."""
    from src.orchestrator.seats import attach_seat, bind_holder, set_seat_attended

    manager = (await ensure_seat(actions, house="demo", handle="AttendMgr",
                                 source="test"))["seat_id"]
    worker = (await ensure_seat(actions, house="demo", handle="AttendWkr",
                                source="test"))["seat_id"]
    await attach_seat(actions, worker, manager, evidence="org chart", actor="test")
    await bind_holder(actions, seat_id=worker, agent_id="agent:attend-stranger", source="test")

    out = await set_seat_attended(actions, seat_id=worker, attended="human",
                                  actor="agent:attend-stranger", because="unauthorized try")
    assert "not authorized" in out["error"] and manager in out["error"]
    assert await _attended_value(actions, worker) is None


async def test_set_seat_attended_allows_the_target_seats_own_manager(actions: Actions) -> None:
    from src.orchestrator.seats import attach_seat, bind_holder, set_seat_attended

    manager = (await ensure_seat(actions, house="demo", handle="AttendMgr2",
                                 source="test"))["seat_id"]
    worker = (await ensure_seat(actions, house="demo", handle="AttendWkr2",
                                source="test"))["seat_id"]
    await attach_seat(actions, worker, manager, evidence="org chart", actor="test")
    await bind_holder(actions, seat_id=manager, agent_id="agent:attend-manager", source="test")

    out = await set_seat_attended(actions, seat_id=worker, attended="human",
                                  actor="agent:attend-manager",
                                  because="the manager stamps its own worker")
    assert out["attended"] == "human"
    assert await _attended_value(actions, worker) == "human"


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

    out = await rename_seat(actions, seat_id=seat, new_handle="William", actor="operator",
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
    out = await rename_seat(actions, seat_id=seat, new_handle="StillEmpty", actor="operator",
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
    out = await rename_seat(actions, seat_id=other, new_handle="vajra", actor="operator",
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
                            actor="operator", because="no such seat exists")
    assert out == {"error": "no such seat: 'seat:nosuchsea'"}


async def test_rename_seat_refuses_a_non_manager_non_operator(actions: Actions) -> None:
    """NEGATIVE CONTROL (census a5e53ed8/3f97f9c7, fixed 2026-08-02): this docstring
    claimed "manager/operator-invoked, no self-service" for weeks while any mounted
    caller could rename any active seat — confirmed against pre-fix code (a stranger's
    actor="test" call renamed the seat cleanly before this gate existed, see every OTHER
    test in this section, all of which had to be updated to actor="operator" to keep
    passing). Mirrors charter_for's own refusal shape exactly."""
    from src.orchestrator.seats import attach_seat, bind_holder, rename_seat

    manager = (await ensure_seat(actions, house="demo", handle="RenameMgr",
                                 source="test"))["seat_id"]
    worker = (await ensure_seat(actions, house="demo", handle="RenameWkr",
                                source="test"))["seat_id"]
    await attach_seat(actions, worker, manager, evidence="org chart", actor="test")
    await bind_holder(actions, seat_id=worker, agent_id="agent:rename-stranger", source="test")

    out = await rename_seat(actions, seat_id=worker, new_handle="Stolen",
                            actor="agent:rename-stranger", because="unauthorized try")
    assert "not authorized" in out["error"] and manager in out["error"]
    assert await _handle_of(actions, worker) == "RenameWkr"


async def test_rename_seat_allows_the_target_seats_own_manager(actions: Actions) -> None:
    from src.orchestrator.seats import attach_seat, bind_holder, rename_seat

    manager = (await ensure_seat(actions, house="demo", handle="RenameMgr2",
                                 source="test"))["seat_id"]
    worker = (await ensure_seat(actions, house="demo", handle="RenameWkr2",
                                source="test"))["seat_id"]
    await attach_seat(actions, worker, manager, evidence="org chart", actor="test")
    await bind_holder(actions, seat_id=manager, agent_id="agent:rename-manager", source="test")

    out = await rename_seat(actions, seat_id=worker, new_handle="Renamed2",
                            actor="agent:rename-manager",
                            because="the manager renames its own worker")
    assert out["new_handle"] == "Renamed2"
    assert await _handle_of(actions, worker) == "Renamed2"


# ═══ bind_seat_tree (task #103's re-scope, ff3bdc37, Thoth DM 2794 sign-off) ═══

async def test_bind_seat_tree_records_a_distinct_property_from_anchor_cwd(
    actions: Actions,
) -> None:
    """The office (identity) and the tree (code) are TWO properties, not one — binding a
    tree must never touch anchor_cwd."""
    from src.orchestrator.seats import bind_seat_tree, seat_facts

    seat = await actions.create_or_find_object("Seat", "seat:bt1work0", "test")
    await actions.assert_property(seat, "handle", "Treebind", "test", datetime.now(UTC), 0.9)
    await actions.assert_property(seat, "anchor_cwd", "/home/treebind", "test",
                                  datetime.now(UTC), 0.9)

    out = await bind_seat_tree(actions, seat_id="seat:bt1work0", tree_cwd="/repo/treebind",
                               actor="operator", because="test: new checkout for a task")
    assert out == {"seat": "seat:bt1work0", "old_tree_cwd": None, "tree_cwd": "/repo/treebind",
                   "because": "test: new checkout for a task",
                   "note": out["note"]}
    facts = await seat_facts(actions.pool, "seat:bt1work0")
    assert facts["tree_cwd"] == "/repo/treebind"
    assert facts["anchor_cwd"] == "/home/treebind"       # untouched


async def test_bind_seat_tree_rebinding_reports_the_old_value(actions: Actions) -> None:
    from src.orchestrator.seats import bind_seat_tree

    seat = await actions.create_or_find_object("Seat", "seat:bt2work0", "test")
    await actions.assert_property(seat, "handle", "Rebound", "test", datetime.now(UTC), 0.9)
    await bind_seat_tree(actions, seat_id="seat:bt2work0", tree_cwd="/repo/first",
                         actor="operator", because="test: first tree")
    out = await bind_seat_tree(actions, seat_id="seat:bt2work0", tree_cwd="/repo/second",
                               actor="operator", because="test: re-pointed for a new task")
    assert out["old_tree_cwd"] == "/repo/first" and out["tree_cwd"] == "/repo/second"


async def test_bind_seat_tree_refuses_a_blank_tree_cwd(actions: Actions) -> None:
    from src.orchestrator.seats import bind_seat_tree

    seat = await actions.create_or_find_object("Seat", "seat:bt3work0", "test")
    await actions.assert_property(seat, "handle", "Blanktree", "test", datetime.now(UTC), 0.9)
    out = await bind_seat_tree(actions, seat_id="seat:bt3work0", tree_cwd="  ",
                               actor="test", because="test: blank")
    assert out == {"error": "bind_seat_tree needs a tree_cwd"}


async def test_bind_seat_tree_refuses_a_blank_because(actions: Actions) -> None:
    from src.orchestrator.seats import bind_seat_tree

    seat = await actions.create_or_find_object("Seat", "seat:bt4work0", "test")
    await actions.assert_property(seat, "handle", "Blankwhy", "test", datetime.now(UTC), 0.9)
    out = await bind_seat_tree(actions, seat_id="seat:bt4work0", tree_cwd="/repo/x",
                               actor="test", because="  ")
    assert "testimony" in out["error"]


async def test_bind_seat_tree_refuses_an_unknown_seat(actions: Actions) -> None:
    from src.orchestrator.seats import bind_seat_tree

    out = await bind_seat_tree(actions, seat_id="seat:nosuchtre", tree_cwd="/repo/x",
                               actor="operator", because="test: no such seat")
    assert out == {"error": "no such seat: 'seat:nosuchtre'"}


async def test_bind_seat_tree_refuses_a_non_manager_non_operator(actions: Actions) -> None:
    """THE NEGATIVE CONTROL (2026-08-02): before this gate existed, ANY mounted caller
    could rebind ANY seat's tree_cwd — a code-execution vector at the seat's next launch,
    not a metadata drift. This is the specimen that proves the gate now actually fires."""
    from src.orchestrator.seats import bind_seat_tree

    seat = await actions.create_or_find_object("Seat", "seat:bt5work0", "test")
    await actions.assert_property(seat, "handle", "Guardedtree", "test", datetime.now(UTC), 0.9)
    out = await bind_seat_tree(actions, seat_id="seat:bt5work0", tree_cwd="/repo/hostile",
                               actor="agent:some-random-mind", because="test: unauthorized")
    assert "not authorized to bind" in out["error"]
    assert "seat:bt5work0" in out["error"]


async def test_bind_seat_tree_allows_the_target_seats_own_manager(actions: Actions) -> None:
    from src.orchestrator.seats import attach_seat, bind_holder, bind_seat_tree, ensure_seat

    manager = (await ensure_seat(actions, house="demo", handle="TreeMgr",
                                 source="test"))["seat_id"]
    worker = (await ensure_seat(actions, house="demo", handle="TreeWkr",
                                source="test"))["seat_id"]
    await attach_seat(actions, worker, manager, evidence="org chart", actor="test")
    await bind_holder(actions, seat_id=manager, agent_id="agent:tree-manager", source="test")

    out = await bind_seat_tree(actions, seat_id=worker, tree_cwd="/repo/legit",
                               actor="agent:tree-manager", because="the manager rebinds its worker")
    assert out.get("error") is None
    assert out["tree_cwd"] == "/repo/legit"


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
    assert out == {"seat_id": claimed["seat_id"], "house": "alfred", "was": "bytebye",
                   "already_correct": False, "still_contradicted": []}
    assert await derive_house(actions.pool, claimed["seat_id"]) == "alfred"


async def test_correct_house_names_when_it_corrected_nothing(actions: Actions) -> None:
    """The fourth 42176e16 specimen, Alfred's own catch: a call whose `new_house` already
    matches `was` still writes (harmless, append-only) but corrects nothing — `was`
    alone reads identically to a real correction. `already_correct` says so plainly.

    UPDATED FOR fe8ec7ff mechanism 3a (self-heal at write time, ruling df646654): a stale,
    outvoted contradicting value from a different source no longer survives a correction —
    correct_house now heals it in the same call, so `still_contradicted` reads empty right
    after the write instead of naming a row a human would once have had to invalidate by
    hand (decision 7a46db36's own gap, closed here)."""
    from src.orchestrator.agents import claim_name
    from src.orchestrator.seats import correct_house

    claimed = await claim_name(actions, "agent:ch5alrdy", "Alfred5", source="test")
    assert claimed.get("error") is None
    # a stale, OUTVOTED contradicting value from a different source
    seat_obj = await actions.create_or_find_object("Seat", claimed["seat_id"], "test")
    await actions.assert_property(seat_obj, "house", "stalehouse", "some-other-agent",
                                  datetime.now(UTC) - timedelta(days=21), 0.9,
                                  evidence_class="self_declared")
    first = await correct_house(actions, "agent:ch5alrdy", "alfred5", source="test")
    assert first["already_correct"] is False
    assert first["still_contradicted"] == []  # healed, not merely reported

    again = await correct_house(actions, "agent:ch5alrdy", "alfred5", source="test")
    assert again["was"] == "alfred5" and again["house"] == "alfred5"
    assert again["already_correct"] is True  # the real catch — was == house, nothing to fix
    assert again["still_contradicted"] == []  # stays healed


async def test_correct_house_surfaces_prior_art_never_refuses_on_it(
    actions: Actions, monkeypatch,
) -> None:
    """obligation e4612853's sibling (Thoth DM 3169/3185) — same guard as rename_project,
    generalized: never blocks the write, just makes sure it isn't silently unread."""
    from src.orchestrator.agents import claim_name
    from src.orchestrator.seats import correct_house

    await actions.create_or_find_object("Agent", "agent:ch2alfrd", "test")
    claimed = await claim_name(actions, "agent:ch2alfrd", "Alfred2", source="test")
    assert claimed.get("error") is None

    async def _fake_prior_art(pool, *, subject_canonical, field, new_value, actor, because=""):
        return {"prior_art": [{"id": "abcdef01", "type": "Decision"}],
               "prior_art_flag": f"a standing ruling (abcdef01) may already cover "
                                  f"{subject_canonical}'s {field!r}"}

    monkeypatch.setattr("src.orchestrator.capture.property_prior_art", _fake_prior_art)
    out = await correct_house(actions, "agent:ch2alfrd", "somewhere-else", source="test")
    assert out["house"] == "somewhere-else"  # the write still happened
    assert out["prior_art_flag"] == (
        f"a standing ruling (abcdef01) may already cover {claimed['seat_id']}'s 'house'")


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


# ═══ resync_seat_house_third_party (task #152's Seat.house repair, the third-party sibling
# of correct_house — a rename_project that never propagates to the seat's own house) ═══

async def test_resync_seat_house_third_party_writes_a_new_value(actions: Actions) -> None:
    from src.orchestrator.seats import resync_seat_house_third_party, seat_facts

    seat = await actions.create_or_find_object("Seat", "seat:rs1khepr", "test")
    await actions.assert_property(seat, "house", "tony", "test", datetime.now(UTC), 0.9)

    out = await resync_seat_house_third_party(
        actions, "seat:rs1khepr", "cultural-infrastructure", source="test",
        reason="task #152: repo:tony was renamed, khepri's Seat.house never followed")
    assert out == {"written": True, "seat_id": "seat:rs1khepr",
                   "house": "cultural-infrastructure", "was": "tony",
                   "reason": "task #152: repo:tony was renamed, khepri's Seat.house never "
                             "followed",
                   "still_contradicted": []}  # same source: the old value is superseded
    facts = await seat_facts(actions.pool, "seat:rs1khepr")
    assert facts["house"] == "cultural-infrastructure"


async def test_resync_seat_house_third_party_is_a_noop_when_already_correct(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import resync_seat_house_third_party

    seat = await actions.create_or_find_object("Seat", "seat:rs2match0", "test")
    await actions.assert_property(seat, "house", "xxit", "test", datetime.now(UTC), 0.9)

    out = await resync_seat_house_third_party(
        actions, "seat:rs2match0", "xxit", source="test", reason="x")
    assert out == {"written": False, "seat_id": "seat:rs2match0", "house": "xxit",
                   "still_contradicted": []}


async def test_resync_seat_house_third_party_heals_a_lingering_different_source(
    actions: Actions,
) -> None:
    """UPDATED FOR fe8ec7ff mechanism 3a (self-heal at write time, ruling df646654): a
    different source's contradicting value used to survive a real write forever, reported
    but never invalidated — a real WRITE branch now heals it in the same call, so
    `still_contradicted` reads empty right after."""
    from src.orchestrator.seats import resync_seat_house_third_party

    seat = await actions.create_or_find_object("Seat", "seat:rs5linger", "test")
    await actions.assert_property(seat, "house", "xxit", "some-other-agent",
                                  datetime.now(UTC) - timedelta(days=10), 0.9)

    out = await resync_seat_house_third_party(
        actions, "seat:rs5linger", "handlingtheloop", source="test",
        reason="rename never propagated")
    assert out["written"] is True
    assert out["still_contradicted"] == []  # healed, not merely reported

async def test_resync_seat_house_third_party_refuses_an_empty_reason(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import resync_seat_house_third_party

    seat = await actions.create_or_find_object("Seat", "seat:rs3noreas", "test")
    await actions.assert_property(seat, "house", "tony", "test", datetime.now(UTC), 0.9)

    out = await resync_seat_house_third_party(
        actions, "seat:rs3noreas", "cultural-infrastructure", source="test", reason="  ")
    assert "silent overwrite" in out["error"]
    from src.orchestrator.seats import seat_facts
    facts = await seat_facts(actions.pool, "seat:rs3noreas")
    assert facts["house"] == "tony"                   # nothing written


async def test_resync_seat_house_third_party_refuses_an_empty_house(actions: Actions) -> None:
    from src.orchestrator.seats import resync_seat_house_third_party

    out = await resync_seat_house_third_party(
        actions, "seat:rs4empty0", "   ", source="test", reason="x")
    assert "needs a name" in out["error"]


async def test_correct_house_mcp_wrapper_moves_orient_without_reconnecting(
    actions: Actions,
) -> None:
    """THE SAME ACCEPTANCE TEST invalidate_works_in's own wrapper carries (Thoth's ruling,
    thread 8640a625 / decision 4001f6d1 — the gap that made John's own fix appear to take
    effect three steps late): a live session correcting its OWN house must see orient()'s
    resolution move WITHOUT reconnecting, not just the DB row."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity, claim_name

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    claimed = await claim_name(actions, "agent:chwrap1aa", "ChwrapHead", source="test")
    assert claimed.get("error") is None

    ident = AgentIdentity(agent_id="agent:chwrap1aa", session="chwrap1", project="oldhouse",
                          model="claude-sonnet-5", cwd=None, model_method="job_dir",
                          model_history=("claude-sonnet-5",))
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    key = srv._conn_key(ctx)
    srv._agents[key] = ident
    try:
        before = await srv.orient(ctx=ctx)
        assert before["project"] == "oldhouse"               # the stale cache, before anything

        out = await srv.correct_house("newhouse", ctx=ctx)
        assert out["house"] == "newhouse"

        after = await srv.orient(ctx=ctx)                    # SAME ctx — no reconnect
    finally:
        srv._pool = saved_pool
        srv._agents.pop(key, None)
    assert after["project"] == "newhouse"                    # RESOLUTION moved, not just the row


async def test_correct_pin_value_mcp_wrapper_targets_the_callers_own_office(
    actions: Actions, tmp_path, monkeypatch,
) -> None:
    """msg 4761, obligation 114f7ac9 — the MCP tool that gives `offices.correct_pin_value`
    a reachable surface, always self-scoped to the CALLER's own office (never a path the
    caller supplies)."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity, claim_name

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path))

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    claimed = await claim_name(actions, "agent:cpvwrap1a", "CpvwrapHead", source="test")
    assert claimed.get("error") is None
    office = tmp_path / "cpvwraphead"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\n')

    ident = AgentIdentity(agent_id="agent:cpvwrap1a", session="cpvwrap1", project="tony",
                          model="claude-sonnet-5", cwd=None, model_method="job_dir",
                          model_history=("claude-sonnet-5",))
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    key = srv._conn_key(ctx)
    srv._agents[key] = ident
    try:
        out = await srv.correct_pin_value(
            "project", "cultural-infrastructure", "task #152", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(key, None)
    assert out["written"] is True
    assert out["seat_id"] == claimed["seat_id"]
    assert (office / ".osiris").read_text() == 'project = "cultural-infrastructure"\n'


async def test_correct_pin_value_mcp_wrapper_refuses_an_unmounted_caller(actions: Actions) -> None:
    from src import mcp_server as srv

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    out = await srv.correct_pin_value("project", "x", "x", ctx=ctx)
    assert "mount first" in out["error"]


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


async def test_fold_seat_survives_a_crash_between_mail_move_and_merge(
    actions: Actions,
) -> None:
    """Task #59's own precondition fix: fold_seat's mail leg now moves BEFORE
    Actions.merge_objects (holders/managed_by already did) — a process death in that
    window leaves dupe.status=='active', so a retry continues rather than hitting
    fold_seat's own "already folded — nothing to do" refusal with mail stranded on the
    dupe seat forever. Simulated by hand (mirroring fold_seat's own mail-move query) with
    merge_objects never called, then the real verb — proving it does not refuse and that
    re-moving already-moved mail is a true no-op."""
    from src.orchestrator.seats import fold_seat

    a = await actions.create_or_find_object("Seat", "seat:fs5crsh0", "test")
    b = await actions.create_or_find_object("Seat", "seat:fs5real0", "test")
    assert a and b
    await mailbox_send(actions, to_agent="seat:fs5crsh0")

    # THE SIMULATED CRASH: fold_seat's own mail-move query, run by hand, with
    # merge_objects never called — the state a real crash in that window would leave.
    await actions.pool.execute(
        "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
        "seat:fs5real0", "seat:fs5crsh0")
    still_active = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='seat:fs5crsh0'")
    assert still_active == "active"          # confirms the crash left it retryable

    out = await fold_seat(actions, dupe="seat:fs5crsh0", into="seat:fs5real0",
                          evidence="retry after a simulated mid-fold crash", actor="test")

    assert "error" not in out                # the retry completed, it did not refuse
    assert out["mail_moved"] == 0             # already moved by the "crash" — a true no-op


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


# ═══ unfold_seat (ruling 31c02dca's PARITY requirement: fold_seat had NO reversal before
# this — a Seat fold was permanent, task #127's own named case) ═══


async def test_unfold_seat_dry_run_plans_the_holder_and_managed_by_restore(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import fold_seat, unfold_seat

    dupe = await actions.create_or_find_object("Seat", "seat:us1dupe0", "test")
    await actions.create_or_find_object("Seat", "seat:us1into0", "test")
    mgr = await actions.create_or_find_object("Seat", "seat:us1mgr00", "test")
    holder = await actions.create_or_find_object("Agent", "agent:us1hld00", "test")
    await actions.create_link(holder, dupe, "holds", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    await _link_managed_by(actions, dupe, mgr)

    await fold_seat(actions, dupe="seat:us1dupe0", into="seat:us1into0",
                    evidence="test: a twin", actor="test")

    out = await unfold_seat(actions, dupe="seat:us1dupe0",
                            because="wrongful fold — a real second seat",
                            actor="agent:judge")
    assert out["execute"] is False
    assert out["was_merged_into"] == "seat:us1into0"
    ops = {p["op"] for p in out["plan"]}
    assert "unmerge_objects" in ops and "move_link" in ops
    st = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='seat:us1dupe0'")
    assert st == "merged"  # dry run never writes


async def test_unfold_seat_executed_restores_holder_and_managed_by(actions: Actions) -> None:
    from src.orchestrator.seats import fold_seat, held_seat, manager_of_seat, unfold_seat

    dupe = await actions.create_or_find_object("Seat", "seat:us2dupe0", "test")
    into = await actions.create_or_find_object("Seat", "seat:us2into0", "test")
    mgr = await actions.create_or_find_object("Seat", "seat:us2mgr00", "test")
    assert dupe and into and mgr
    holder = await actions.create_or_find_object("Agent", "agent:us2hld00", "test")
    await actions.create_link(holder, dupe, "holds", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    await _link_managed_by(actions, dupe, mgr)

    await fold_seat(actions, dupe="seat:us2dupe0", into="seat:us2into0",
                    evidence="test: a twin", actor="test")

    out = await unfold_seat(actions, dupe="seat:us2dupe0",
                            because="a real second seat, wrongly folded",
                            actor="agent:judge", execute=True)

    assert out["unmerged"] is True
    assert out["holders_restored"] == 1
    assert out["managed_by_restored"] == 1
    row = await actions.pool.fetchrow(
        "SELECT status, merged_into FROM objects WHERE canonical='seat:us2dupe0'")
    assert row["status"] == "active" and row["merged_into"] is None
    bound = await held_seat(actions.pool, "agent:us2hld00")
    assert bound is not None and bound["seat_id"] == "seat:us2dupe0"
    assert await manager_of_seat(actions.pool, "seat:us2dupe0") == "seat:us2mgr00"
    # the merge event and same_as link stay as witnesses (unmerge_objects' own contract)
    same_as = await actions.pool.fetchval(
        "SELECT 1 FROM links l JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE f.canonical='seat:us2dupe0' AND t.canonical='seat:us2into0' "
        "AND l.type='same_as'")
    assert same_as == 1


async def test_unfold_seat_refuses_a_never_folded_dupe(actions: Actions) -> None:
    from src.orchestrator.seats import unfold_seat

    await actions.create_or_find_object("Seat", "seat:us3free0", "test")
    out = await unfold_seat(actions, dupe="seat:us3free0", because="x", actor="agent:judge")
    assert "not folded" in out["error"]


async def test_unfold_seat_refuses_a_blank_because(actions: Actions) -> None:
    from src.orchestrator.seats import fold_seat, unfold_seat

    await actions.create_or_find_object("Seat", "seat:us4dupe0", "test")
    await actions.create_or_find_object("Seat", "seat:us4into0", "test")
    await fold_seat(actions, dupe="seat:us4dupe0", into="seat:us4into0", evidence="x",
                    actor="test")
    out = await unfold_seat(actions, dupe="seat:us4dupe0", because="   ", actor="agent:judge")
    assert "because" in out["error"]


async def test_unfold_seat_refuses_an_unknown_dupe(actions: Actions) -> None:
    from src.orchestrator.seats import unfold_seat

    out = await unfold_seat(actions, dupe="seat:nobody99", because="x", actor="agent:judge")
    assert "unknown" in out["error"]


async def test_unfold_seat_refuses_an_operator_blessed_fold_without_fresh_operator_word(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import fold_seat, unfold_seat

    await actions.create_or_find_object("Seat", "seat:us5dupe0", "test")
    await actions.create_or_find_object("Seat", "seat:us5into0", "test")
    await fold_seat(actions, dupe="seat:us5dupe0", into="seat:us5into0",
                    evidence="the operator confirmed these are one seat, 2026-07-01",
                    actor="operator")

    out = await unfold_seat(actions, dupe="seat:us5dupe0",
                            because="I think this was wrong", actor="agent:judge")
    assert "operator" in out["error"]
    st = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='seat:us5dupe0'")
    assert st == "merged"  # refused, nothing written

    out2 = await unfold_seat(
        actions, dupe="seat:us5dupe0",
        because="the operator's fresh word, 2026-07-28: this fold was wrong",
        actor="agent:judge", execute=True)
    assert out2["unmerged"] is True


async def test_unfold_seat_does_not_restore_a_holder_who_moved_on_since(
    actions: Actions,
) -> None:
    """The unfold_agent honesty model, generalized to links: a holder only gets restored
    to dupe when their CURRENT active seat is still exactly what the fold left them on. A
    holder who has since moved to a THIRD seat is never guessed back — reported as simply
    not among the reversible items, the same discipline unfold_agent holds for mail it
    cannot prove was ever the dupe's own."""
    from src.orchestrator.seats import fold_seat, held_seat, unfold_seat

    dupe = await actions.create_or_find_object("Seat", "seat:us6dupe0", "test")
    into = await actions.create_or_find_object("Seat", "seat:us6into0", "test")
    third = await actions.create_or_find_object("Seat", "seat:us6thrd0", "test")
    assert dupe and into and third
    holder = await actions.create_or_find_object("Agent", "agent:us6hld00", "test")
    await actions.create_link(holder, dupe, "holds", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")

    await fold_seat(actions, dupe="seat:us6dupe0", into="seat:us6into0",
                    evidence="test: a twin", actor="test")
    # the holder moves on to a THIRD seat, unrelated to the fold — a real transfer vacates
    # the old seat first (bind_holder alone only heals a SEAT's prior holder, never an
    # AGENT's own other seat) — nothing should stitch this back onto dupe when the fold is
    # later reversed
    now = datetime.now(UTC)
    await actions.invalidate_link(holder, into, "holds", "test", now)
    await actions.create_link(holder, third, "holds", "test", now, 0.9,
                              evidence_class="self_declared")

    out = await unfold_seat(actions, dupe="seat:us6dupe0", because="wrongly folded",
                            actor="agent:judge", execute=True)
    assert out["holders_restored"] == 0
    bound = await held_seat(actions.pool, "agent:us6hld00")
    assert bound is not None and bound["seat_id"] == "seat:us6thrd0", (
        "a holder who moved on since the fold is never pulled back onto the reversed seat")


async def test_unfold_seat_reports_unreturnable_mail(actions: Actions) -> None:
    from src.orchestrator.mailbox import send_message
    from src.orchestrator.seats import fold_seat, unfold_seat

    await actions.create_or_find_object("Seat", "seat:us7dupe0", "test")
    await actions.create_or_find_object("Seat", "seat:us7into0", "test")
    await send_message(actions.pool, from_agent="agent:sender", from_project="osiris",
                       to_agent="seat:us7dupe0", body="a question for the twin")
    await fold_seat(actions, dupe="seat:us7dupe0", into="seat:us7into0", evidence="x",
                    actor="test")

    out = await unfold_seat(actions, dupe="seat:us7dupe0", because="wrongly folded",
                            actor="agent:judge")  # dry run
    assert len(out["estate_unreturnable"]["mail"]) == 1


# ═══ reconcile_seat_fold (#127, the repair path fold_seat never had — mirrors
# reconcile_project_fold's exact design, sharing the SAME _move_seat_estate fold_seat
# itself calls) ═══


async def test_reconcile_seat_fold_repairs_an_orphaned_holder_from_a_partial_fold(
    actions: Actions,
) -> None:
    """An OLD-style merge (a raw merge_objects call with no estate-move at all) leaves an
    active holder stranded on the now-merged dupe seat. reconcile repairs it without
    re-performing the merge."""
    from src.orchestrator.seats import held_seat, reconcile_seat_fold

    dupe = await actions.create_or_find_object("Seat", "seat:rs1dupe0", "test")
    into = await actions.create_or_find_object("Seat", "seat:rs1into0", "test")
    holder = await actions.create_or_find_object("Agent", "agent:rs1hld00", "test")
    await actions.create_link(holder, dupe, "holds", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    # simulate the OLD, estate-blind merge path directly — no fold_seat involved
    await actions.merge_objects(into, dupe, justification="old-style merge", actor="test")
    events_before = await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='merge'")

    out = await reconcile_seat_fold(actions, dupe="seat:rs1dupe0", into="seat:rs1into0",
                                    actor="test")

    assert out["reconciled"] == "seat:rs1dupe0" and out["into"] == "seat:rs1into0"
    assert out["holders_moved"] == ["agent:rs1hld00"]
    bound = await held_seat(actions.pool, "agent:rs1hld00")
    assert bound is not None and bound["seat_id"] == "seat:rs1into0"
    events_after = await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='merge'")
    assert events_after == events_before
    status = await actions.pool.fetchval("SELECT status FROM objects WHERE id=$1", dupe)
    assert status == "merged"  # unchanged — still exactly one merge, ever


async def test_reconcile_seat_fold_is_a_true_noop_on_a_healthy_fold(actions: Actions) -> None:
    """NEGATIVE CONTROL, by construction: a fold_seat run that already moved everything
    must come out UNCHANGED when reconcile runs on it."""
    from src.orchestrator.seats import fold_seat, reconcile_seat_fold

    await actions.create_or_find_object("Seat", "seat:rs2dupe0", "test")
    await actions.create_or_find_object("Seat", "seat:rs2into0", "test")
    holder = await actions.create_or_find_object("Agent", "agent:rs2hld00", "test")
    dupe_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='seat:rs2dupe0'")
    await actions.create_link(holder, dupe_oid, "holds", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    fold_out = await fold_seat(actions, dupe="seat:rs2dupe0", into="seat:rs2into0",
                               evidence="x", actor="test")
    assert fold_out["holders_moved"] == ["agent:rs2hld00"]

    out = await reconcile_seat_fold(actions, dupe="seat:rs2dupe0", into="seat:rs2into0",
                                    actor="test")

    assert out["holders_moved"] == []
    assert out["managed_by_moved"] == 0
    assert out["mail_moved"] == 0


async def test_reconcile_seat_fold_refuses_a_still_active_dupe(actions: Actions) -> None:
    """REFUSAL CONTROL: reconcile must never become a side door into performing a fold —
    an active (never-folded) dupe is fold_seat's job, not this one's."""
    from src.orchestrator.seats import reconcile_seat_fold

    await actions.create_or_find_object("Seat", "seat:rs3active", "test")
    await actions.create_or_find_object("Seat", "seat:rs3into00", "test")

    out = await reconcile_seat_fold(actions, dupe="seat:rs3active", into="seat:rs3into00",
                                    actor="test")

    assert "not merged" in out["error"] and "merge" in out["error"]
    status = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='seat:rs3active'")
    assert status == "active"


async def test_reconcile_seat_fold_refuses_to_redirect_a_merge(actions: Actions) -> None:
    """A dupe already merged into A is not this pair's business if the caller names B —
    reconcile never guesses or redirects which merge a repair applies to."""
    from src.orchestrator.seats import fold_seat, reconcile_seat_fold

    await actions.create_or_find_object("Seat", "seat:rs4dupe0", "test")
    await actions.create_or_find_object("Seat", "seat:rs4real0", "test")
    await actions.create_or_find_object("Seat", "seat:rs4wrong0", "test")
    await fold_seat(actions, dupe="seat:rs4dupe0", into="seat:rs4real0", evidence="x",
                    actor="test")

    out = await reconcile_seat_fold(actions, dupe="seat:rs4dupe0", into="seat:rs4wrong0",
                                    actor="test")

    assert "not" in out["error"] and "seat:rs4real0" in out["error"]


async def test_reconcile_seat_fold_refuses_unknown_refs(actions: Actions) -> None:
    from src.orchestrator.seats import fold_seat, reconcile_seat_fold

    await actions.create_or_find_object("Seat", "seat:rs5dupe0", "test")
    await actions.create_or_find_object("Seat", "seat:rs5into0", "test")
    await fold_seat(actions, dupe="seat:rs5dupe0", into="seat:rs5into0", evidence="x",
                    actor="test")

    missing_into = await reconcile_seat_fold(actions, dupe="seat:rs5dupe0",
                                             into="seat:nope-at-all", actor="test")
    assert "no such seat" in missing_into["error"]

    missing_dupe = await reconcile_seat_fold(actions, dupe="seat:nope-either",
                                             into="seat:rs5into0", actor="test")
    assert "no such seat" in missing_dupe["error"]


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


async def test_retire_seat_refuses_a_peered_seat(actions: Actions) -> None:
    """THE PEER GUARD (Khnum IX's review, msg 1774): unpeer requires BOTH seats active to
    resolve them — nothing stopped retiring a peered seat first, stranding the bond
    pointing at a dead seat forever with no sanctioned verb able to heal it. unpeer first,
    same discipline as the active-holder guard."""
    from src.orchestrator.seats import peer_seats, retire_seat

    await actions.create_or_find_object("Seat", "seat:rs5peer0", "test")
    await actions.create_or_find_object("Seat", "seat:rs5peer1", "test")
    await peer_seats(actions, "seat:rs5peer0", "seat:rs5peer1", because="test bond",
                     actor="test")

    out = await retire_seat(actions, "seat:rs5peer0", actor="test")
    assert "peered with seat:rs5peer1" in out["error"]
    # refused loudly — nothing written; the seat is still active and still peered
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='seat:rs5peer0'")
    assert row["status"] == "active"


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


# #172 (2026-08-18 00:08-00:23Z): pg_advisory_lock on a borrowed pool connection wedged the
# fleet — a cancelled/wedged holder returned its connection to the pool still owning the
# lock (advisory locks are session-scoped, untouched by asyncpg's connection.reset()), and
# the next unrelated borrower inherited it. _seat_lock/_peer_lock/mint_lock are now
# XACT-scoped: the lock dies with the transaction, no finally required.


async def test_seat_lock_releases_even_when_the_holder_is_cancelled(
    actions: Actions,
) -> None:
    holder_ready = asyncio.Event()

    async def _hold_forever() -> None:
        async with _seat_lock(actions.pool, "osiris", "wedge-cancel"):
            holder_ready.set()
            await asyncio.sleep(30)

    task = asyncio.create_task(_hold_forever())
    await holder_ready.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # the cancelled holder's xact rolled back with it — the key is free, not still owned
    async with actions.pool.acquire() as conn:
        key = "seat:osiris/wedge-cancel"
        got_it = await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", key)
        assert got_it is True
        await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", key)

    # a fresh holder proceeds without waiting on a ghost
    async with asyncio.timeout(2):
        async with _seat_lock(actions.pool, "osiris", "wedge-cancel"):
            pass


async def test_peer_lock_releases_even_when_the_holder_is_cancelled(
    actions: Actions,
) -> None:
    holder_ready = asyncio.Event()

    async def _hold_forever() -> None:
        async with _peer_lock(actions.pool, "seat:pw1", "seat:pw2"):
            holder_ready.set()
            await asyncio.sleep(30)

    task = asyncio.create_task(_hold_forever())
    await holder_ready.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    async with actions.pool.acquire() as conn:
        for key in ("peer:seat:pw1", "peer:seat:pw2"):
            got_it = await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", key)
            assert got_it is True
        for key in ("peer:seat:pw1", "peer:seat:pw2"):
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", key)

    async with asyncio.timeout(2):
        async with _peer_lock(actions.pool, "seat:pw1", "seat:pw2"):
            pass


async def test_mint_lock_releases_even_when_the_holder_is_cancelled(
    actions: Actions,
) -> None:
    holder_ready = asyncio.Event()

    async def _hold_forever() -> None:
        async with mint_lock(actions.pool, "agent:wedge-cancel"):
            holder_ready.set()
            await asyncio.sleep(30)

    task = asyncio.create_task(_hold_forever())
    await holder_ready.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    async with actions.pool.acquire() as conn:
        key = "mint:agent:wedge-cancel"
        got_it = await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", key)
        assert got_it is True
        await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", key)

    async with asyncio.timeout(2):
        async with mint_lock(actions.pool, "agent:wedge-cancel"):
            pass


async def test_seat_lock_wedged_waiter_fails_loud_named(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A holder that is genuinely still working the key (not cancelled, not dead) makes a
    waiter fail LOUD past lock_timeout with a named error — never park silently."""
    monkeypatch.setattr(seats_mod, "_LOCK_TIMEOUT", "200ms")
    holder_ready = asyncio.Event()
    release = asyncio.Event()

    async def _hold() -> None:
        async with _seat_lock(actions.pool, "osiris", "wedge-timeout"):
            holder_ready.set()
            await release.wait()

    task = asyncio.create_task(_hold())
    await holder_ready.wait()
    try:
        with pytest.raises(LockWedged):
            async with _seat_lock(actions.pool, "osiris", "wedge-timeout"):
                pass
    finally:
        release.set()
        await task


async def test_mint_lock_wedged_waiter_fails_loud_named(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(agents_mod, "_LOCK_TIMEOUT", "200ms")
    holder_ready = asyncio.Event()
    release = asyncio.Event()

    async def _hold() -> None:
        async with mint_lock(actions.pool, "agent:wedge-timeout"):
            holder_ready.set()
            await release.wait()

    task = asyncio.create_task(_hold())
    await holder_ready.wait()
    try:
        with pytest.raises((LockWedged, TimeoutError)):
            async with mint_lock(actions.pool, "agent:wedge-timeout"):
                pass
    finally:
        release.set()
        await task
