"""THE SEAT — identity that exists before its first session (the identity core, 5cef856b).

The bug class this cures is structural: the durable identity key was `agent:<session-id>` —
a fact about a CONVERSATION, the most ephemeral object in the system — and everything the
operator cares about (seat, house, charter, mail) was layered on it as assertions,
RECONSTRUCTED BY INFERENCE at every door (whisper, mount, re-attach, heartbeat). Every
inference door can mint; seven patches (mint-lock, debounce, null-seam gate, dating gate,
fork archaeology, claimed-sid guard, the binding rule) each defend the same wound. The cure
is to stop inferring: a Seat is minted ONCE as `seat:<uuid8>` and never re-keyed; its handle,
house, and anchor are mutable ASSERTIONS (never the key — keying identity on a mutable fact
is the whole arc's lesson); a session ATTACHES to it with a one-time token its spawner
exported at birth, before the harness's first breath.

THE ATTACH CEREMONY, and why each rule exists:
  * a token is ONE-TIME and binds to its FIRST presenter — a subagent inherits its parent's
    environment (the CLAUDE_JOB_DIR lesson, 0344e536), so a second session presenting a used
    token is refused LOUDLY (thread 2294e95d ask #1: mount must refuse and say so when the
    anchor contradicts the claim — silence was the whole bug);
  * same presenter re-presenting is a RESUME, not an intruder — idempotent re-affirm;
  * a seat a LIVE session is bound to is not vacant — two minds in one seat is the collision
    class this exists to kill, refused before anything is written;
  * tokens live in a plain table (hot secrets, revocable), never the append-only kernel.

The MIND layer keeps its seams (a882b334: swaps/compactions mint minds — the numeral tracks
the mind); the binding follows the lineage head at every mint (`follow_binding`, called from
mint_heir), so the SEAT is the address that stops churning precisely because minds die.
"""

from __future__ import annotations

import logging
import secrets
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

logger = logging.getLogger("osiris.seats")

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)

# how fresh a bound session's pulse must be to count as LIVE contention (mirrors the fleet's
# 15-minute liveness window everywhere else — resolve_seat, agent_liveness, the roster)
_LIVE_SECS = 900


@asynccontextmanager
async def _seat_lock(pool: asyncpg.Pool, house: str, handle: str) -> AsyncIterator[None]:
    """Serialize ensure_seat per (house, handle) — the same advisory-lock discipline as
    mint_lock (two concurrent ensures would otherwise both find nothing and mint twins)."""
    key = f"seat:{house or ''}/{handle.lower()}"
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock(hashtext($1))", key)
        try:
            yield
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", key)


async def find_seat(pool: asyncpg.Pool, *, house: str | None, handle: str) -> str | None:
    """The existing Seat object for (house, handle), by WINNING assertions — the same
    predicate style seat_holders uses, so the roster and the mint see one truth."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1) "
        "AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='house' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '') = COALESCE($2, '') "
        "ORDER BY o.created_at LIMIT 1", handle, house)


async def seats_by_handle(pool: asyncpg.Pool, handle: str) -> list[str]:
    """Every ACTIVE Seat carrying this handle, case-insensitive, GLOBALLY — house-agnostic
    on purpose (thread cb374585, the Vajra twin's root cause): find_seat's (house, handle)
    lookup misses a real, unambiguous seat whenever the caller's own computed house doesn't
    match what's actually stored on it (a vacant seat has no live session to disagree with
    a stale/CWD-derived house guess) — exactly the shape that let claim_name mint a SECOND
    seat for 'Vajra' while the real one (managed_by Alfred, house 'bytebye') sat untouched.
    Zero results means genuinely new (mint fresh, house-scoped is fine — nothing to
    conflict with); exactly one is the seat any claim to this handle must bind; two or more
    is an ambiguity — a twin — that a caller must name and refuse, never silently
    arbitrate (fold_seat resolves it deliberately, this function only reports it)."""
    return [r["canonical"] for r in await pool.fetch(
        "SELECT o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1) "
        "ORDER BY o.created_at", handle)]


async def ensure_seat(
    actions: Actions, *, house: str | None, handle: str, anchor_cwd: str | None = None,
    source: str,
) -> dict[str, Any]:
    """Find-or-mint the Seat for a (house, handle) — idempotent, advisory-locked. Minting is
    DELIBERATE (called at a claim or a daemon spawn), never a bulk sweep: the graph heals
    forward, one seat at a time, as roles are actually exercised."""
    handle = (handle or "").strip()
    if not handle:
        return {"error": "a seat needs a handle"}
    async with _seat_lock(actions.pool, house or "", handle):
        existing = await find_seat(actions.pool, house=house, handle=handle)
        if existing is not None:
            return {"seat_id": existing, "handle": handle, "house": house, "minted": False}
        canonical = f"seat:{uuid.uuid4().hex[:8]}"
        # an 8-hex collision is vanishingly rare but a re-key is forbidden — check, retry
        for _ in range(3):
            taken = await actions.pool.fetchval(
                "SELECT 1 FROM objects WHERE canonical=$1", canonical)
            if not taken:
                break
            canonical = f"seat:{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)
        oid = await actions.create_or_find_object("Seat", canonical, source)
        await actions.assert_property(oid, "name", handle, source, now, _CONF,
                                      evidence_class=_EC)
        await actions.assert_property(oid, "handle", handle, source, now, _CONF,
                                      evidence_class=_EC)
        if house:
            await actions.assert_property(oid, "house", house, source, now, _CONF,
                                          evidence_class=_EC)
        if anchor_cwd:
            await actions.assert_property(oid, "anchor_cwd", anchor_cwd, source, now, _CONF,
                                          evidence_class=_EC)
        return {"seat_id": canonical, "handle": handle, "house": house, "minted": True}


async def mint_attach_token(
    pool: asyncpg.Pool, *, seat_id: str, minted_by: str | None = None,
) -> str:
    """A one-time attach token for a seat — minted by the spawner, exported into the child's
    environment before the harness's first breath (identity at birth, spec §4.2)."""
    token = secrets.token_urlsafe(24)
    await pool.execute(
        "INSERT INTO seat_tokens (token, seat_id, minted_by) VALUES ($1, $2, $3)",
        token, seat_id, minted_by)
    return token


async def seat_of_mount(pool: asyncpg.Pool, *, job_dir: str) -> str | None:
    """The seat a session's durable mount is bound to, or None (never attached)."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT seat_id FROM agent_mounts WHERE job_dir=$1", job_dir)


async def reseed_binding(pool: asyncpg.Pool, *, agent_id: str, job_dir: str) -> str | None:
    """THE HAND-RESUME FOLLOWS THE SEAT (Phase B4, the honest tail Phase A named): the holds
    link is the binding's DURABLE half and survives session_end; the mount row does not. A
    fresh row minted for a mind that actively holds a seat re-earns its `seat_id` from the
    link — no token needed, because the graph already knows who holds what. Idempotent and
    deliberately timid: only a row with NO binding is ever touched (an explicit attach, or a
    surviving row, always outranks a re-derivation)."""
    seat = await pool.fetchval(
        "SELECT t.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id "
        "WHERE f.canonical=$1 AND l.type='holds' AND t.type='Seat' AND t.status='active' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen DESC LIMIT 1", agent_id)
    if seat is None:
        return None
    await pool.execute(
        "UPDATE agent_mounts SET seat_id=$2 WHERE job_dir=$1 AND seat_id IS NULL",
        job_dir, seat)
    return str(seat)


async def held_seat(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any] | None:
    """The Seat this mind actively holds, with its display facts — the 'who am I' half of
    the binding (Phase B3): orient/mount tell a bound mind WHICH ROLE it sits in, whether
    or not it ever claim_named itself in the assertion world. None when unbound.

    LINEAGE-AWARE (the Thoth seat-binding gap, 2026-07-21 — two independent witnesses caught
    it from opposite ends the same hour: wake()'s authorization gate and the mail envelope's
    handle lookup): mint_heir's automatic succession never called claim_name, so a holds link
    minted for an ancestor generation (say, -xvii) was invisible to a caller asking about its
    successor (-xviii) — the old query exact-matched the presented id. Now any active link
    ANYWHERE in the lineage (the presented id, the bare root, or any `-<suffix>` generation —
    the same LIKE-prefix shape the trigger.py rate caps already use) answers; when more than
    one survives un-healed, the NEWEST generation wins, the same tiebreak follow_binding uses
    when it moves a link forward.

    `house` is DERIVED (ruling ff6148b0, decision 4c9e4bd7), never the seat's own stored
    property — see derive_house()."""
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    rows = await pool.fetch(
        "SELECT f.canonical AS holder, t.canonical AS seat_id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=t.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle "
        "FROM links l JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE (f.canonical=$1 OR f.canonical=$2 OR f.canonical LIKE $2 || '-%') "
        "AND l.type='holds' AND t.type='Seat' AND t.status='active' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent_id, base)
    if not rows:
        return None
    best = max(rows, key=lambda r: _generation(r["holder"])[1])
    house = await derive_house(pool, best["seat_id"])
    return {"seat_id": best["seat_id"], "handle": best["handle"], "house": house}


SeatState = Literal["vacant", "occupied", "cold"]


async def seat_occupancy(
    pool: asyncpg.Pool, seat_id: str, *, live_secs: int = _LIVE_SECS,
) -> dict[str, Any]:
    """VACANT / OCCUPIED / COLD (occupancy piece B, 9f566244) — one authority for whether a
    Seat has a living body in it, the vitals.py way: computed at READ time from
    links(holds) + agent_mounts, no schema change, no new table. THE ACCEPTANCE CASE: Ptah's
    office once showed four bodies where one lived — a seat with no holder at all must read
    VACANT on its own, never silently absent from a query that only ever asks about agents.

    VACANT — no `holds` link has EVER existed for this seat (mint_seat's own law: a seat is
    furniture until a body sits in it). OCCUPIED — an active holder exists AND is live right
    now (agent_mounts, the same live_secs window every other liveness read in this codebase
    shares — live_souls, resolve_seat, held_seat's own contention check, the fleet roster).
    COLD — held, now or in the past, but nobody live this instant: the ordinary in-between
    state of a seat between sessions, never an alarm by itself.

    LINEAGE-AWARE the same way held_seat is: the active holder's liveness is read across its
    whole lineage (base id or any `-<suffix>` generation), because bind_holder/follow_binding
    keep the holds link on the freshest generation but a mount row can still be tagged to an
    ancestor label mid-succession.

    This IS the read launch() needs for its own idempotency (detect-existing-window before
    spawning, never mint a twin body) and its honest liveness receipt (Ra's requirement,
    53ae1a87: body-exists and can-receive are separate states, each independently
    verifiable) — call it with the seat about to be launched into, before launching."""
    ever_held = await pool.fetchval(
        "SELECT 1 FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE t.canonical=$1 AND l.type='holds' LIMIT 1", seat_id)
    if not ever_held:
        return {"state": "vacant", "holder": None, "live": False}
    holder = await pool.fetchval(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id "
        "WHERE t.canonical=$1 AND l.type='holds' AND f.type='Agent' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen DESC LIMIT 1", seat_id)
    if holder is None:
        return {"state": "cold", "holder": None, "live": False}
    from src.orchestrator.agents import _generation

    base = _generation(holder)[0]
    live = bool(await pool.fetchval(
        "SELECT max(last_seen) > now() - make_interval(secs => $2) "
        "FROM agent_mounts WHERE agent_id=$1 OR agent_id LIKE $1 || '-%'",
        base, float(live_secs)))
    return {"state": "occupied" if live else "cold", "holder": holder, "live": live}


async def fleet_occupancy(
    pool: asyncpg.Pool, *, live_secs: int = _LIVE_SECS,
) -> list[dict[str, Any]]:
    """Every active Seat's occupancy, one row each — the batch read fleet() renders beside
    the agent tree, so a seat with no holder at all (Ptah's shape) is as visible as one with
    three. Same authority as seat_occupancy(), run once per seat instead of asked one at a
    time; small fleet, small N, and matches backfill_unbound_seats' own list-then-resolve
    shape rather than a fused query the seat count doesn't yet justify.

    `house` is DERIVED per seat (ruling ff6148b0, decision 4c9e4bd7) — see derive_house()."""
    seats = await pool.fetch(
        "SELECT o.canonical AS seat_id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle "
        "FROM objects o WHERE o.type='Seat' AND o.status='active' ORDER BY o.canonical")
    out: list[dict[str, Any]] = []
    for row in seats:
        occ = await seat_occupancy(pool, row["seat_id"], live_secs=live_secs)
        house = await derive_house(pool, row["seat_id"])
        out.append({"seat_id": row["seat_id"], "handle": row["handle"], "house": house,
                    **occ})
    return out


async def reachability(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any]:
    """Can this lineage be reached RIGHT NOW — the TRUTHFUL answer, consulted from the
    Claude harness daemon's own job state (ruling d739d486, Ra's clean repro): a mail
    SEND-RECEIPT can lie during a compaction seam ('no resumable session — never handed to
    a fresh twin') while the daemon's job_for on the SAME lineage has held a live,
    resumable job the whole time. A stale disk/DB snapshot INFERS liveness; job_for READS
    it from the one place that cannot lag the seam — the daemon owns the job, so it knows
    the instant a successor exists, before any mount row or transcript file catches up.

    Composes with seat_occupancy: occupancy answers 'is a live body here' (holds link +
    agent_mounts); this answers 'and can it RECEIVE a turn, this instant' (the daemon's own
    job table) — two different authorities for two different questions, not a duplicate.

    LINEAGE-WIDE, matching every other liveness read in this codebase (held_seat,
    seat_occupancy, trigger.py's own `doors`): checks every job_dir this base OR any of its
    generations has ever mounted, because a fresh successor's OWN mount row is exactly the
    evidence a seam can lag — the daemon may already hold its job before agent_mounts
    reflects it at all.

    THE READ ONLY, deliberately: this does not retry a refused send, notify anyone, or
    change what dispatch_dm does — it is the truthful primitive Ra's fix and any future
    notify-at-seam work CONSULT, not a rewrite of either. `job_for` is a pure READ of
    daemon job state (claude_daemon.job_for), never the RCE-flagged `reply` injection lane
    (ruling 482c3d0f) — legal and used as-is under the flagged-abstraction policy. Fails
    OPEN like claude_daemon's own convention: a dark daemon or an unknown lineage reads
    unreachable-by-this-check, never treated as proof of death — only as 'this read
    couldn't confirm it'."""
    from src.ingest.harness.claude_daemon import job_for
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    rows = await pool.fetch(
        "SELECT job_dir FROM agent_mounts WHERE (agent_id=$1 OR agent_id LIKE $1 || '-%') "
        "AND job_dir IS NOT NULL", base)
    doors = {Path(r["job_dir"]).name for r in rows if r["job_dir"]}
    if not doors:
        return {"reachable": False, "via": "none", "job": None,
                "detail": "no known job_dir for this lineage — nothing to ask the daemon "
                          "about"}
    ids = doors | {d[:8] for d in doors}
    job = await job_for(ids)
    if job is None:
        return {"reachable": False, "via": "none", "job": None,
                "detail": "the daemon holds no job for this lineage right now — dark or "
                          "genuinely not running; never treated as proof of death"}
    shown = job.get("short") or job.get("sessionId") or "its job"
    return {"reachable": True, "via": "daemon-job", "job": job,
            "detail": f"the daemon's own job state confirms {shown} is live right now"}


async def manager_of_seat(pool: asyncpg.Pool, seat_id: str) -> str | None:
    """The manager Seat of a worker Seat, or None when unmanaged — the single-pair read
    mirroring osiris_stophook.py's own local `_manager_seat` query (same table, same shape),
    promoted here so notify-at-seam (mint_heir's compaction path, thread aeae9977) doesn't
    hand-roll a third copy. The stop-hook script keeps its own copy for now (it cannot import
    across the script/package boundary without a heavier refactor) — a known duplicate, not a
    disagreement: both read `managed_by` the same way."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT t.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE f.canonical=$1 AND l.type='managed_by' "
        "AND t.type='Seat' AND t.status='active' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen DESC LIMIT 1", seat_id)


_MAX_HOUSE_HOPS = 32  # generous for any real org depth (mirrors mint_heir's own bounded-
                      # walk convention, range(64)) — exists to catch a managed_by CYCLE,
                      # a data bug, not a legitimately deep chain


async def derive_house(pool: asyncpg.Pool, seat_id: str, *, max_hops: int = _MAX_HOUSE_HOPS,
                       ) -> str | None:
    """House must be DERIVED, not stored (operator ruling ff6148b0, decision 4c9e4bd7):
    house(seat) = house(manager(seat)), walked up the managed_by chain to the HEAD (a seat
    with no active managed_by edge out) — the head's own STORED house is the one legitimate
    anchor left, a deliberate declaration (Alfred's 'alfred', Thoth's 'osiris') never a
    spawn-time snapshot. Every seat below derives through the chain; the stored `house`
    property on a non-head seat is legacy noise this never reads (Alfred's old bytebye,
    Vajra's twin house=vajra simply stop being consulted, not corrected).

    READ-TIME ONLY, same discipline as reachability(): computed fresh every call, nothing
    written back. LOUD on a managed_by CYCLE — a seat reappearing in its own chain is a
    graph bug, not a deep hierarchy, so this logs and returns None rather than silently
    truncating; a legitimately unbounded chain (should never happen — max_hops is generous)
    reads the same way, also logged."""
    seen: set[str] = set()
    current = seat_id
    for _ in range(max_hops):
        if current in seen:
            logger.warning("managed_by cycle deriving house for %s: reached %s twice",
                           seat_id, current)
            return None
        seen.add(current)
        manager = await manager_of_seat(pool, current)
        if manager is None:  # current is the HEAD — its own stamped house is authoritative
            return await pool.fetchval(  # type: ignore[no-any-return]
                "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
                "ON a.object_id=o.id AND a.name='house' WHERE o.canonical=$1 "
                "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", current)
        current = manager
    logger.warning("house derivation for %s exceeded %d hops without reaching a head",
                   seat_id, max_hops)
    return None


async def seat_facts(pool: asyncpg.Pool, seat_id: str) -> dict[str, Any]:
    """A Seat's own handle/house/intended_model/anchor_cwd, one read — the shared resolver
    mintseat.py and trigger.py each independently hand-rolled as a private `_seat_facts`
    (identical name, near-identical shape, the exact 'two resolvers disagree' class this
    house keeps re-learning). `house` is DERIVED (ruling ff6148b0, decision 4c9e4bd7), the
    other three are the seat's own stored assertions — always all four keys present (None
    when absent or the seat doesn't exist), matching trigger.py's stricter contract so a
    caller can index `facts["handle"]` directly rather than every caller re-deriving its
    own tolerant `.get()`."""
    row = await pool.fetchrow(
        "SELECT "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='intended_model' ORDER BY a.confidence DESC, a.observed_at DESC "
        "   LIMIT 1) AS intended_model, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='anchor_cwd' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS anchor_cwd "
        "FROM objects o WHERE o.canonical=$1 AND o.type='Seat' AND o.status='active'", seat_id)
    house = await derive_house(pool, seat_id)
    if row is None:
        return {"handle": None, "house": house, "intended_model": None, "anchor_cwd": None}
    return {"handle": row["handle"], "house": house,
            "intended_model": row["intended_model"], "anchor_cwd": row["anchor_cwd"]}


_ATTENDED_VALUES = {"human", "worker"}


async def set_seat_attended(
    actions: Actions, *, seat_id: str, attended: str, actor: str, because: str,
) -> dict[str, Any]:
    """Stamp a seat's REAL attendance signal (thread 96f62338) — replaces ruling `d8a77f80`'s
    broken proxy in `dispatch_dm` ('a seat that manages someone is human-attended'), true only
    while Thoth was the sole manager and false the day workers started minting sub-workers and
    test seats of their own (Imhotep's flip-test mints reclassified him; alfred's #50-pilot
    workers did too — both silently lost their push lane forever). `attended='human'` marks a
    seat the operator actually fronts; `attended='worker'` marks the ordinary case explicitly,
    for reversing a prior stamp. The human-attended guard reads this directly and no longer
    infers anything from managed_by.

    OPERATOR-APPROVED TO CHANGE, deliberately: this verb only builds the write path — the
    actual stamp on thoth's own seat waits for the operator's explicit word, not this build.

    Refuses LOUDLY on: a value outside {'human','worker'} (no silent typo landing as
    'not human'); a blank `because` (a safety guard reads this property — the reason it
    changed belongs on the record); an unknown or retired seat (a Seat's `status` column
    stays 'active' forever — retirement is the `retired` property `retire_seat` stamps, the
    same signal checked here)."""
    if attended not in _ATTENDED_VALUES:
        return {"error": f"attended must be one of {sorted(_ATTENDED_VALUES)}, not "
                         f"{attended!r}"}
    if not because.strip():
        return {"error": "because is required — a seat's attendance signal gates a safety "
                         "guard (dispatch_dm's human-attended check); the reason it changed "
                         "must be on the record"}
    row = await actions.pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
        seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    retired = await actions.pool.fetchval(
        "SELECT 1 FROM current_assertions a WHERE a.object_id=$1 AND a.name='retired' "
        "AND a.value #>> '{}' = 'true'", row["id"])
    if retired:
        return {"error": f"{seat_id} is retired — cannot stamp attendance on a retired seat"}
    now = datetime.now(UTC)
    await actions.assert_property(row["id"], "attended", attended, actor, now, _CONF,
                                  evidence_class=_EC)
    return {"seat": seat_id, "attended": attended, "because": because}


async def rename_seat(
    actions: Actions, *, seat_id: str, new_handle: str, actor: str, because: str,
) -> dict[str, Any]:
    """RENAME_SEAT (operator-ordered, 2026-07-28) — no rename verb existed; claim_name is
    self-claiming only (a mind picks its OWN name), and this house's handles have drifted
    in casing (vajra lowercase, TJMAX all-caps at the agent level vs tjmax at the seat) with
    nothing to correct it deliberately. A manager or the operator renames a seat by hand,
    always with a reason on the record.

    SCOPE, both compensating assertions (old handle stays in history, never deleted):
    (1) the seat's own `handle` property; (2) the CURRENT holder's `handle` stamp too, if
    the seat is occupied — a rename that only touched the seat would leave the live mind
    still answering to its old name in every seat_label() render. Mirrors claim_name's own
    40-char cap on a handle — same class of field, same discipline.

    OUT OF SCOPE, deliberately: the harness-session display name (a terminal/window title
    like "[P] [PS] Tjmax") is NOT touched — it belongs to a running process this verb has
    no reach into. The honest receipt is "graph renamed; the harness name follows at next
    spawn," never a claim of something this call didn't do.

    Refuses LOUDLY on: a blank `new_handle` or one over 40 chars; a blank `because` (a
    rename is testimony — the reason must be on the record, the same discipline
    set_seat_attended holds); an unknown seat; `new_handle` already claimed by a DIFFERENT
    active seat, case-insensitive (seats_by_handle — the exact drift lesson that named this
    build: 'vajra' and 'Vajra' must never both be claimable)."""
    new_handle = (new_handle or "").strip()
    if not new_handle or len(new_handle) > 40:
        return {"error": "pick a short handle (1-40 chars)"}
    if not because.strip():
        return {"error": "because is required — a rename is testimony; the reason it "
                         "changed must be on the record"}
    row = await actions.pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
        seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    collisions = [s for s in await seats_by_handle(actions.pool, new_handle) if s != seat_id]
    if collisions:
        return {"error": f"'{new_handle}' is already claimed by {collisions[0]} "
                         "(case-insensitive) — a name belongs to one seat forever"}
    old_handle = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        row["id"])
    now = datetime.now(UTC)
    await actions.assert_property(row["id"], "handle", new_handle, actor, now, _CONF,
                                  evidence_class=_EC)
    occ = await seat_occupancy(actions.pool, seat_id)
    holder_stamped: str | None = None
    if occ["holder"]:
        holder_oid = await actions.create_or_find_object("Agent", occ["holder"], actor)
        await actions.assert_property(holder_oid, "handle", new_handle, actor, now, _CONF,
                                      evidence_class=_EC)
        holder_stamped = occ["holder"]
    return {"seat": seat_id, "old_handle": old_handle, "new_handle": new_handle,
            "holder_stamped": holder_stamped, "because": because,
            "note": "graph renamed; the harness window/session display name follows at "
                    "next spawn, not retroactively"}


async def bind_holder(
    actions: Actions, *, seat_id: str, agent_id: str, source: str | None = None,
) -> None:
    """Make `agent_id` the seat's ACTIVE holder — prior holders' `holds` links heal by
    valid_until (never deleted, history walkable), one active link remains. The shared tail
    of the two deliberate binding acts: the attach ceremony (token-gated, spawner-driven)
    and a `claim_name` (guard-gated, the live mind's own act). Callers run their refusals
    FIRST; this only writes."""
    now = datetime.now(UTC)
    src = source or agent_id
    seat_oid = await actions.create_or_find_object("Seat", seat_id, src)
    agent_oid = await actions.create_or_find_object("Agent", agent_id, src)
    prior = [r["from_id"] for r in await actions.pool.fetch(
        "SELECT DISTINCT l.from_id FROM links l JOIN objects f ON f.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='holds' AND f.canonical <> $2 "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_oid, agent_id)]
    for old in prior:
        await actions.invalidate_link(old, seat_oid, "holds", src, now)
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='holds' "
        "AND (valid_until IS NULL OR valid_until > now()) LIMIT 1", agent_oid, seat_oid)
    if not exists:
        await actions.create_link(agent_oid, seat_oid, "holds", src, now, _CONF,
                                  evidence_class=_EC)


async def backfill_unbound_seats(
    actions: Actions, *, dry_run: bool = True, only_seats: set[str] | None = None,
) -> dict[str, Any]:
    """THE ORPHAN-SEAT BACKFILL (thread 749bf530 / occupancy piece C, 9f566244) — the batch
    cure for the Thoth seat-binding gap. mint_heir's automatic succession only ever MOVES an
    existing `holds` link (follow_binding, lineage-wide) — it never CREATES one from nothing.
    A seat whose original claim predates the Seat-object binding (5cef856b) has therefore sat
    unbound through every generation since, however many times it has changed hands; its
    CURRENT holder calling claim_name again would fix it in one act, but nothing prompts that
    call. This finds every such seat and (dry-run by default) proposes binding it to whoever
    the assertion world already calls its live holder — the exact fallback resolve_seat uses
    for an un-seated lineage, asked in bulk rather than one name at a time.

    `only_seats` scopes BOTH the plan and the write to exactly those seat ids (operator's
    ruling, 2026-07-21: Thoth-first, fleet-wide only after that lands clean) — every OTHER
    unbound seat is still counted in `total_unbound` so the caller can see what a scoped run
    deliberately left untouched, but never appears in `plan` and is never written.

    DRY-RUN REPORTS THE PLAN AND WRITES NOTHING (a2cf8405: a graph mutation is never hand-run
    without surfacing it first). Idempotent either way: bind_holder no-ops on an already-active
    link, so a repeat run — or a seat someone fixed by hand in between — changes nothing on its
    second pass. A seat with no resolvable live holder (no handle asserted, or the assertion
    world itself has no living candidate) is reported, never guessed."""
    from src.orchestrator.agents import resolve_seat

    pool = actions.pool
    unbound = await pool.fetch(
        "SELECT o.canonical AS seat_id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='house' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS house "
        "FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.to_id=o.id AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()))")
    scoped = [row for row in unbound if only_seats is None or row["seat_id"] in only_seats]
    plan: list[dict[str, Any]] = []
    for row in scoped:
        seat_id, handle, house = row["seat_id"], row["handle"], row["house"]
        if not handle:
            plan.append({"seat_id": seat_id, "handle": None, "house": house, "holder": None,
                        "note": "no handle asserted on this seat — nothing to resolve by"})
            continue
        resolved = await resolve_seat(actions, handle)
        holder = resolved.get("agent")
        item: dict[str, Any] = {"seat_id": seat_id, "handle": handle, "house": house,
                                "holder": holder, "live": resolved.get("live", False)}
        if not holder:
            item["note"] = "no resolvable holder in the assertion world — skipped"
        elif resolved.get("warning"):
            item["note"] = resolved["warning"]
        plan.append(item)
    bound = 0
    if not dry_run:
        for item in plan:
            if item.get("holder"):
                await bind_holder(actions, seat_id=item["seat_id"], agent_id=item["holder"])
                bound += 1
    return {"dry_run": dry_run, "total_unbound": len(unbound),
            "scoped_out": len(unbound) - len(scoped), "plan": plan,
            "resolvable": sum(1 for p in plan if p.get("holder")), "bound": bound}


async def holds(pool: asyncpg.Pool, agent_id: str, seat_id: str) -> bool:
    """Does this mind actively hold this seat? The read side of seat-addressed mail
    (Phase B2): a message to `seat:<id>` is deliverable to whoever this returns True for."""
    return bool(await pool.fetchval(
        "SELECT 1 FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id "
        "WHERE f.canonical=$1 AND t.canonical=$2 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent_id, seat_id))


async def seat_receipt(pool: asyncpg.Pool, seat_id: str) -> dict[str, Any] | None:
    """The DM-receipt facts for a seat ADDRESS (Phase B2): its display handle/house and the
    mind currently holding it (None while vacant — the mail waits; a seat address is never
    a grave, its next holder reads it). None when no such living Seat exists."""
    display = await _seat_display(pool, seat_id)
    if not display:
        return None
    holder = await pool.fetchval(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id "
        "WHERE t.canonical=$1 AND l.type='holds' AND f.type='Agent' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen DESC LIMIT 1", seat_id)
    return {**display, "holder": str(holder) if holder else None}


async def binding_of_handle(pool: asyncpg.Pool, name: str) -> dict[str, Any] | None:
    """The Seat-object world's answer to 'who is <name>?' (Phase B1): the UNIQUE living Seat
    carrying this handle, and its current holder via the ACTIVE holds link. None when the
    seat world has no authoritative answer — no such seat, an ambiguous handle (two houses,
    same name: the assertion path's liveness ranking arbitrates instead), a vacant seat, or
    a holder that is retired/false-minted (a binding must never resolve into a grave)."""
    seats = [r["canonical"] for r in await pool.fetch(
        "SELECT o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1)", name)]
    if len(seats) != 1:
        return None
    holder = await pool.fetchval(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id "
        "WHERE t.canonical=$1 AND l.type='holds' AND f.type='Agent' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions r WHERE r.object_id=f.id "
        "  AND r.name IN ('retired','false_mint') AND r.value #>> '{}' = 'true') "
        # a VISITOR never resolves as a holder (Phase C — the attach guard makes this link
        # shape impossible going forward; this is defense in depth for healed history)
        "AND NOT EXISTS (SELECT 1 FROM links sl WHERE sl.from_id=f.id "
        "  AND sl.type='spawned_by') "
        "ORDER BY l.first_seen DESC LIMIT 1", seats[0])
    if holder is None:
        return None
    return {"seat_id": seats[0], "holder": str(holder)}


async def _seat_display(pool: asyncpg.Pool, seat_id: str) -> dict[str, Any]:
    row = await pool.fetchrow(
        "SELECT "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='house' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS house "
        "FROM objects o WHERE o.canonical=$1 AND o.type='Seat' AND o.status='active'", seat_id)
    if row is None:
        return {}
    return {"handle": row["handle"], "house": row["house"]}


async def attach_session(
    actions: Actions, *, seat_id: str, token: str, job_dir: str, agent_id: str,
    live_secs: int = _LIVE_SECS,
) -> dict[str, Any]:
    """THE CEREMONY — verify the token, bind the session to its Seat. Refusals are LOUD
    (an error dict the whisper prints) and write NOTHING; only a verified fresh use — or the
    same presenter resuming — touches the tables."""
    pool = actions.pool
    row = await pool.fetchrow(
        "SELECT seat_id, used_by, used_at FROM seat_tokens WHERE token=$1", token)
    if row is None:
        return {"error": "ATTACH REFUSED — unknown attach token: nothing was bound. "
                         "The token in this environment matches no mint on record."}
    if row["seat_id"] != seat_id:
        return {"error": f"ATTACH REFUSED — token/seat mismatch: this token was minted for "
                         f"{row['seat_id']}, not {seat_id}. A stale or foreign environment; "
                         "nothing was bound."}
    display = await _seat_display(pool, seat_id)
    if not display:
        return {"error": f"ATTACH REFUSED — {seat_id} is not a living Seat in the graph; "
                         "nothing was bound."}
    if row["used_at"] is not None and row["used_by"] != job_dir:
        # the env-inheritance leak (0344e536) and the collision class (2294e95d), refused
        # in one breath: the FIRST presenter owns the token, forever.
        return {"error": f"ATTACH REFUSED — this token was already used by another session "
                         f"({row['used_by']}). A one-time token binds to its first "
                         "presenter; a second presentation is an inherited environment or a "
                         "collision, never a resume. Nothing was bound."}
    # A VISITOR NEVER HOLDS A SEAT (Phase C, §4.3): a sub-agent works in its parent's name —
    # binding one would seat a sidechain that dies with its task and never resumes.
    spawner = await pool.fetchval(
        "SELECT p.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects p ON p.id=l.to_id "
        "WHERE f.canonical=$1 AND l.type='spawned_by' LIMIT 1", agent_id)
    if spawner:
        return {"error": f"ATTACH REFUSED — {agent_id} is a VISITOR (spawned_by {spawner}); "
                         "a sub-agent never holds a seat. Nothing was bound."}
    fresh_use = row["used_at"] is None
    if fresh_use:
        holder = await pool.fetchrow(
            "SELECT job_dir, agent_id FROM agent_mounts WHERE seat_id=$1 AND job_dir<>$2 "
            "AND last_seen > now() - make_interval(secs => $3) "
            "ORDER BY last_seen DESC LIMIT 1", seat_id, job_dir, live_secs)
        if holder is not None:
            return {"error": f"ATTACH REFUSED — {seat_id} ({display.get('handle')}) is held "
                             f"LIVE by {holder['agent_id']}. Two minds in one seat is the "
                             "collision class the ceremony exists to kill; nothing was bound."}
        claimed = await pool.execute(
            "UPDATE seat_tokens SET used_by=$2, used_at=now() "
            "WHERE token=$1 AND used_at IS NULL", token, job_dir)
        if claimed.rsplit(" ", 1)[-1] == "0":
            # raced by a concurrent presenter — re-read; only the same job_dir may proceed
            again = await pool.fetchrow(
                "SELECT used_by FROM seat_tokens WHERE token=$1", token)
            if again is None or again["used_by"] != job_dir:
                return {"error": "ATTACH REFUSED — token claimed concurrently by another "
                                 "session; nothing was bound."}
    bound = await pool.execute(
        "UPDATE agent_mounts SET seat_id=$2 WHERE job_dir=$1", job_dir, seat_id)
    if bound.rsplit(" ", 1)[-1] == "0":
        return {"error": "ATTACH REFUSED — no durable mount row for this session yet; the "
                         "binding needs the mount to exist first (automount runs it in "
                         "order). Nothing was bound."}
    # the holds link — the previous holder's heals (valid_until), never deleted
    await bind_holder(actions, seat_id=seat_id, agent_id=agent_id)
    return {"attached": seat_id, "handle": display.get("handle"),
            "house": display.get("house"), "agent": agent_id,
            "resumed": not fresh_use}


async def follow_binding(
    actions: Actions, *, ancestor_oid: uuid.UUID, heir: str, heir_oid: uuid.UUID,
    now: datetime,
) -> None:
    """The binding follows the lineage head (mint_heir's hook): every Seat the LINEAGE
    actively holds re-links to the heir — the old link heals by valid_until, the seat's
    holder history stays walkable, and seat-addressed anything keeps reaching whoever the
    mind is NOW. No seat, no-op.

    LINEAGE-WIDE (Ra's stranded seat, 2026-07-17): the churn can leave the active holds
    link on a FOLDED SIBLING rather than the direct ancestor — the mint from the living
    head then found nothing to move, and Atlas's DM to the seat rotted on a grave. Any
    active holds link anywhere in the heir's lineage (or on the explicit ancestor, which
    a cross-base succession may place outside it) re-links to the heir."""
    from src.orchestrator.agents import _generation

    base = _generation(heir)[0]
    seats = await actions.pool.fetch(
        "SELECT l.from_id, l.to_id FROM links l JOIN objects hf ON hf.id=l.from_id "
        "WHERE l.type='holds' AND l.from_id <> $3 "
        "AND (l.from_id=$1 OR hf.canonical=$2 OR hf.canonical LIKE $2 || '-%') "
        "AND (l.valid_until IS NULL OR l.valid_until > now())",
        ancestor_oid, base, heir_oid)
    for r in seats:
        await actions.invalidate_link(r["from_id"], r["to_id"], "holds", heir, now)
        await actions.create_link(heir_oid, r["to_id"], "holds", heir, now, _CONF,
                                  evidence_class=_EC)


# ═══ SEAT LIFECYCLE (ruling ff6148b0's completion, decision 87953278, thread cb374585) —
# self-organizing seats: a head corrects its own anchor, a twin folds deliberately, a
# genuinely dead role retires. None of this is fenced to the operator's hand — each is an
# identity act within its own caller's authority, the same law claim_name already runs on.


async def correct_house(actions: Actions, agent_id: str, new_house: str, *, source: str,
                        ) -> dict[str, Any]:
    """A HEAD corrects its OWN stored house — the one legitimate write left after
    derive_house (ruling ff6148b0, decision 4c9e4bd7): a head's anchor is a deliberate
    identity declaration, exactly like claim_name, so this is SELF-scoped and NOT
    operator-fenced (decision 87953278: 'an identity act within its own authority').

    Refuses LOUDLY on: an empty house; a caller holding no seat; a caller whose seat is
    NOT a head (an active managed_by edge out means this seat derives its house through
    its manager now — stamping its own house property would be inert data nobody reads,
    the exact 'legacy write stays inert' shape derive_house already documents)."""
    new_house = (new_house or "").strip()
    if not new_house:
        return {"error": "a house needs a name"}
    bound = await held_seat(actions.pool, agent_id)
    if bound is None:
        return {"error": f"{agent_id} holds no seat — house-correct is a seat's own act, "
                         "never done on another's behalf"}
    seat_id = bound["seat_id"]
    manager = await manager_of_seat(actions.pool, seat_id)
    if manager is not None:
        return {"error": f"{seat_id} is managed_by {manager} — not a head. A non-head "
                         f"derives its house through the chain (currently {bound['house']!r}); "
                         "only a head's own stamp is ever read, so only a head may correct "
                         "one. Nothing to do here."}
    was = bound.get("house")
    seat_obj = await actions.create_or_find_object("Seat", seat_id, source)
    await actions.assert_property(seat_obj, "house", new_house, source, datetime.now(UTC),
                                  _CONF, evidence_class=_EC)
    return {"seat_id": seat_id, "house": new_house, "was": was}


async def fold_seat(
    actions: Actions, *, dupe: str, into: str, evidence: str, actor: str,
) -> dict[str, Any]:
    """Fold seat `dupe` into seat `into` — the deliberate, evidence-gated cure for a TWIN
    (two Seat objects that should have been one; the Vajra twin, thread cb374585, is the
    concrete case this exists for: claim_name's own resolution-order bug minted a second
    seat while the real one, managed_by Alfred, sat vacant).

    UNLIKE fold_agent (folds.py — refuses outright when `dupe` holds a seat, because a
    seat transfer is a deliberate act there, never a fold's side effect): fold_seat's WHOLE
    JOB is moving active holders. Every live `holds` link on `dupe` re-points to `into`. If
    `dupe` had MORE THAN ONE concurrent holder (the twin's own anomaly — the thing this
    verb exists to close, not preserve), they converge to ONE: bind_holder's own succession
    law means whichever is re-pointed LAST survives as `into`'s active holder, so this
    processes oldest-first — the NEWEST holder wins, matching every recency-wins
    convention elsewhere in this codebase. All are still named in `holders_moved`. `managed_by`
    edges move too, in either direction, so a folded seat's own org-chart position (who it
    managed, who managed it) survives the merge. Mail addressed to `dupe` follows to `into`.

    Refuses LOUDLY, nothing written, on: empty evidence (an auto-merge wearing a
    signature); either label unknown or not a Seat; dupe==into; dupe already folded."""
    dupe, into = (dupe or "").strip(), (into or "").strip()
    if not (evidence or "").strip():
        return {"error": "a fold without evidence is an auto-merge wearing a signature — "
                         "cite what proves these are one seat"}
    if not dupe or not into:
        return {"error": "fold_seat needs both labels: dupe and into"}
    if dupe == into:
        return {"error": "dupe and into name the same seat — nothing to fold"}
    rows = await actions.pool.fetch(
        "SELECT id, canonical, status FROM objects WHERE canonical = ANY($1::text[]) "
        "AND type='Seat'", [dupe, into])
    by_label = {r["canonical"]: r for r in rows}
    if dupe not in by_label or into not in by_label:
        missing = [x for x in (dupe, into) if x not in by_label]
        return {"error": f"unknown seat(s): {', '.join(missing)} — a fold never invents "
                         "either side"}
    if by_label[dupe]["status"] == "merged":
        return {"error": f"{dupe} is already folded — nothing to do"}
    if by_label[into]["status"] == "merged":
        return {"error": f"{into} is itself folded — fold into the living seat instead"}
    now = datetime.now(UTC)
    dupe_oid, into_oid = by_label[dupe]["id"], by_label[into]["id"]
    into_obj = await actions.create_or_find_object("Seat", into, actor)
    # THE ESTATE, seat-shaped: active holders move first — the point of this verb, where
    # fold_agent refuses instead. OLDEST FIRST, DELIBERATELY: bind_holder's own succession
    # law (one active holder per seat, prior heals by valid_until) means whichever holder
    # is bound LAST survives as the seat's single active holder — a twin's multiple
    # concurrent holders (a data anomaly the fold exists to close, not preserve) converge
    # to the NEWEST one, the same recency-wins convention this codebase already uses
    # everywhere else. Still reported in `holders_moved`, whether or not they end up
    # active — every one was RE-POINTED off the dupe seat, which is what "moved" means.
    holders = await actions.pool.fetch(
        "SELECT f.id AS fid, f.canonical AS holder FROM links l JOIN objects f ON f.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen ASC", dupe_oid)
    for row in holders:
        await actions.invalidate_link(row["fid"], dupe_oid, "holds", actor, now)
        await bind_holder(actions, seat_id=into, agent_id=row["holder"], source=actor)
    # managed_by, either direction — a folded seat's org-chart position survives with it
    managing = await actions.pool.fetch(
        "SELECT to_id AS tid, t.canonical AS mgr FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='managed_by' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", dupe_oid)
    for row in managing:
        await actions.invalidate_link(dupe_oid, row["tid"], "managed_by", actor, now)
        await actions.create_link(into_obj, row["tid"], "managed_by", actor, now, _CONF,
                                  evidence_class=_EC)
    managed = await actions.pool.fetch(
        "SELECT from_id AS fid, f.canonical AS worker FROM links l "
        "JOIN objects f ON f.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='managed_by' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", dupe_oid)
    for row in managed:
        await actions.invalidate_link(row["fid"], dupe_oid, "managed_by", actor, now)
        await actions.create_link(row["fid"], into_obj, "managed_by", actor, now, _CONF,
                                  evidence_class=_EC)
    # the kernel merge: event, projection, resolve-on-read — the same primitive fold_agent
    # itself calls, type-agnostic, no Agent-only check inside it
    await actions.merge_objects(into_oid, dupe_oid, justification=evidence, actor=actor)
    mail_tag = await actions.pool.execute(
        "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
        into, dupe)
    mail_moved = int(mail_tag.rsplit(" ", 1)[-1])
    return {"folded": dupe, "into": into, "holders_moved": [r["holder"] for r in holders],
           "managed_by_moved": len(managing) + len(managed), "mail_moved": mail_moved}


async def retire_seat(actions: Actions, seat_id: str, *, reason: str = "", actor: str,
                      ) -> dict[str, Any]:
    """Mark a Seat permanently CLOSED — a genuinely dead role, no successor, no merge
    target (fold_seat is for a twin; this is for a role that's simply over). DISTINCT from
    the session-level retire() (mcp_server.py) — that one retires a live AGENT's own
    turn; this retires the ROLE ITSELF, for every mind that ever might hold it.

    Refuses LOUDLY on: unknown or already-inactive seat; an ACTIVE holder (a live mind
    sitting in a seat is not this verb's business to evict — transfer or let it vacate
    first, the same discipline fold_agent already holds for agents)."""
    seat_id = (seat_id or "").strip()
    row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Seat'", seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    if row["status"] != "active":
        return {"error": f"{seat_id} is already {row['status']} — nothing to retire"}
    holder = await actions.pool.fetchval(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) LIMIT 1", row["id"])
    if holder:
        return {"error": f"{seat_id} is actively held by {holder} — retire_seat never "
                         "evicts a live mind; transfer or vacate the seat first"}
    await actions.assert_property(row["id"], "retired", "true", actor, datetime.now(UTC),
                                  _CONF, evidence_class=_EC)
    if (reason or "").strip():
        await actions.assert_property(row["id"], "retired_because", reason.strip(), actor,
                                      datetime.now(UTC), _CONF, evidence_class=_EC)
    return {"retired": seat_id}


async def vacate_holder(
    actions: Actions, *, seat_id: str, actor: str, because: str,
) -> dict[str, Any]:
    """Release a seat's ACTIVE holder(s) WITHOUT binding a new one (thread 445a7356,
    Thoth's ruling msg 1611) — the deliberate-hand COMPLEMENT to bind_holder (which only
    ever MOVES the link onto a new holder) and to retire_seat's own stale-holder refusal
    (which is right to refuse — retire_seat closes the ROLE, and evicting a live mind is
    not its business). This is for the one case that refusal correctly can't resolve on
    its own: a holder whose PROCESS is confirmed dead without ever calling retire() on
    itself (found live during task #68's acceptance demo — a `claude stop`ped body leaves
    its `holds` link stale forever, with nothing to release it).

    THIS VERB TRUSTS ITS CALLER. It does no liveness check of its own — that evidence is
    trigger.py's job (vacate_dead_seat, the only sanctioned caller), which reads the real
    process roster and the transcript's own timestamped content before ever reaching
    here, exactly as retire_seat's docstring already distinguishes "the graph's word"
    from "an actual eviction." Calling this directly on a genuinely live holder is a
    caller error, not a refusal this function can catch.

    Refuses LOUDLY on: an unknown/inactive seat, a blank `because` (the same law
    set_seat_attended already holds — a seat's occupancy changing this way belongs on
    the record), or a seat with no active holder (nothing to vacate)."""
    if not because.strip():
        return {"error": "because is required — vacating a seat's holder is a deliberate "
                         "act on the record"}
    seat_id = (seat_id or "").strip()
    row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Seat'", seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    if row["status"] != "active":
        return {"error": f"{seat_id} is already {row['status']} — nothing to vacate"}
    now = datetime.now(UTC)
    holders = await actions.pool.fetch(
        "SELECT f.id AS fid, f.canonical AS holder FROM links l "
        "JOIN objects f ON f.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='holds' AND f.type='Agent' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", row["id"])
    if not holders:
        return {"error": f"{seat_id} has no active holder — nothing to vacate"}
    for h in holders:
        await actions.invalidate_link(h["fid"], row["id"], "holds", actor, now)
    await actions.assert_property(row["id"], "vacated_because", because.strip(), actor, now,
                                  _CONF, evidence_class=_EC)
    return {"vacated": seat_id, "was_held_by": [str(h["holder"]) for h in holders]}
